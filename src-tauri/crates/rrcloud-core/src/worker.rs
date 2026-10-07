//! The headless worker (ARCHITECTURE.md §6): full-reconcile foreign
//! adoption, proxy/thumb backfill, stale-multipart hygiene, and the §2.10
//! GC / compaction / retirement pass, all as a single idempotent,
//! crash-safe [`run_cycle`] driven over the existing modules.
//!
//! # Design refinement vs ARCHITECTURE.md §6 (documented)
//!
//! §6 prose says the worker is a bin in the host app crate that links
//! `rapidraw_lib` (and therefore tauri/gtk/webkit) "to guarantee exact
//! color parity (same rawler rev, same `proxy.rs`)". That rationale no
//! longer requires linking the app: `proxy.rs` lives **here** in
//! `rrcloud-core` and generates proxies against the same pinned rawler rev
//! the host app resolves, so color parity is already a property of this
//! crate. **The P4 worker therefore depends ONLY on `rrcloud-core`** — it
//! is a `[[bin]]` in this crate (`src/bin/rrcloud-worker.rs`) over
//! [`worker::run_cycle`](run_cycle), with no tauri/gtk/webkit anywhere.
//! This is a deliberate, documented refinement of §6 (see
//! `docs/ARCHITECTURE.md` §6 and `docs/UPSTREAM_TOUCHES.md`); the duties,
//! durability contract, and idempotence are unchanged.
//!
//! # Durable state is mandatory (§6, fixing B5)
//!
//! The worker is a full protocol participant: it journals under one stable
//! device identity with a monotonic seq across runs. That requires a
//! persistent redb state directory ([`WorkerConfig::state_dir`]). A worker
//! constructed without one **refuses to journal** — [`Worker::open`]
//! returns [`WorkerError::StatelessRefusal`] *before any journal write*
//! (not a warning). A stateless invocation may at most do read-only
//! reporting ([`Worker::open_readonly`]). One journal prefix, one minted
//! identity persisted in the state dir, no phantom devices and no seq
//! reuse — all by construction of [`crate::state::SyncDb`].
//!
//! # The cycle is pure orchestration
//!
//! [`run_cycle`] reimplements **no** protocol logic. It composes the
//! existing entry points:
//!
//! - reconcile/adoption: [`crate::reader::poll`] to learn journal-known
//!   state, [`crate::s3::S3Client::list_objects_v2`] over `library/` to
//!   find foreign originals, [`crate::proxy::generate_proxy_with`] for the
//!   smart preview + thumbs, [`crate::publisher::enqueue_entry`] +
//!   [`crate::publisher::publish_pending`] to journal the `attest` + `put`
//!   entries, and the S3 client to PUT the preview/thumb objects;
//! - hygiene: [`crate::transfer::abort_stale_uploads`];
//! - §2.10: [`crate::compact::tombstone_gc`],
//!   [`crate::compact::compact_own_segments`],
//!   [`crate::compact::auto_retire_sweep`],
//!   [`crate::compact::gc_retired_prefixes`], folding into and rewriting
//!   the worker's own manifest.
//!
//! Safe to run concurrently with clients and with a second backfiller:
//! each journals under its own identity and writes its own manifest
//! (§6) — just wasteful, never unsafe; the worker never assumes
//! exclusivity.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;

use crate::clock::{DeviceId, VersionVector};
use crate::compact::{
    auto_retire_sweep, compact_own_segments, gc_retired_prefixes, tombstone_gc, CompactConfig,
    CompactError, CompactionSummary, GcSummary, ServerClock,
};
use crate::engine::{reconcile_wholeness, EngineConsumer, EngineError};
use crate::journal::{JournalEntry, JournalError, Kind, Op, JOURNAL_VERSION};
use crate::keys::{
    classify_key, library_key, preview_key, thumb_key, KeyClass, KeyError, RelKey, ThumbSize,
    CONTROL_PREFIX, LIBRARY_PREFIX,
};
use crate::manifest::{get_manifest, merge as merge_manifests, Manifest, ManifestError};
use crate::proxy::{generate_proxy_with, ProxyError, ProxyParams};
use crate::publisher::{
    enqueue_entry_in, get_device_entry, publish_pending, put_device_entry, DeviceProfile,
    PublisherError,
};
use crate::reader::{poll, ReaderError};
use crate::s3::{ListObjectsV2Request, PutObjectOptions, S3Client, S3Config, S3Error};
use crate::semhash::{Blake3Hex, ContentId};
use crate::state::{ItemRecord, ItemState, StateError, SyncDb};
use crate::transfer::{
    abort_stale_uploads, probe_backend, server_ts_estimate, stored_backend_profile, TransferConfig,
    TransferError,
};

/// Default S3 region when `RRCLOUD_REGION` is unset (§6 deployment).
pub const DEFAULT_REGION: &str = "garage";

/// The redb state file the worker opens inside [`WorkerConfig::state_dir`].
pub const STATE_DB_FILENAME: &str = "worker-state.redb";

/// `RRCLOUD_STATE_DIR` — the mandatory persistent redb directory (§6).
pub const ENV_STATE_DIR: &str = "RRCLOUD_STATE_DIR";
/// `RRCLOUD_ENDPOINT` — the S3 endpoint base.
pub const ENV_ENDPOINT: &str = "RRCLOUD_ENDPOINT";
/// `RRCLOUD_BUCKET` — the library bucket.
pub const ENV_BUCKET: &str = "RRCLOUD_BUCKET";
/// `RRCLOUD_REGION` — the S3 region (default [`DEFAULT_REGION`]).
pub const ENV_REGION: &str = "RRCLOUD_REGION";
/// `RRCLOUD_ACCESS_KEY` — the S3 access key id.
pub const ENV_ACCESS_KEY: &str = "RRCLOUD_ACCESS_KEY";
/// `RRCLOUD_SECRET_KEY` — the S3 secret access key.
pub const ENV_SECRET_KEY: &str = "RRCLOUD_SECRET_KEY";

/// Cap on an adopted original's buffered size (the adoption GET buffers the
/// whole object to blake3 it and feed `proxy.rs`). Generous for any real
/// RAW; a larger object is refused as a hostile/foreign drop rather than
/// buffered unbounded — the same posture every other fetch lane takes.
const MAX_ADOPT_ORIGINAL_BYTES: usize = 2 * 1024 * 1024 * 1024;

/// §2.4 stale-multipart threshold: a multipart upload older than this (by
/// its creation time) is abandoned and aborted. 24 h is well past any
/// legitimate single transfer, mobile networks included.
const STALE_UPLOAD_MAX_AGE_SECS: i64 = 24 * 3600;

/// The default `--daemon` interval (§6: "also `--daemon --interval 15m`").
pub const DEFAULT_DAEMON_INTERVAL: Duration = Duration::from_secs(15 * 60);

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Any failure of worker configuration, startup, or a cycle.
#[derive(Debug, thiserror::Error)]
pub enum WorkerError {
    /// The journaling role was requested without a persistent state
    /// directory. Returned by [`Worker::open`] **before any journal
    /// write** — the §6 hard startup error, not a warning. The only
    /// stateless path is [`Worker::open_readonly`].
    #[error(
        "RRCLOUD_STATE_DIR is mandatory: the worker refuses to journal without a persistent \
         state directory (§6); a stateless invocation may only do read-only reporting"
    )]
    StatelessRefusal,

    /// A mandatory configuration value was absent from the environment
    /// (names the missing variable).
    #[error("missing required configuration: {0}")]
    MissingConfig(&'static str),

    /// The state directory was unexpectedly fresh on reopen: the redb file
    /// (and its monotonic seq counter) was lost, so continuing under the
    /// persisted identity would risk §2.2 (device, seq) reuse. A hard
    /// error — the operator must investigate, not silently re-mint.
    #[error(
        "state directory {0} was unexpectedly fresh on reopen: refusing to risk (device, seq) \
         reuse (§2.2); investigate state-volume loss"
    )]
    StateReset(PathBuf),

    /// Minting a fresh worker device identity produced a value the
    /// [`DeviceId`] validator rejected. Unreachable in practice — the mint
    /// constructs a canonical UUIDv4 — but surfaced as a typed error rather
    /// than a panic (library paths do not unwrap).
    #[error("failed to mint worker device identity: {0}")]
    Mint(String),

    /// State-store failure.
    #[error(transparent)]
    State(#[from] StateError),
    /// S3 failure.
    #[error(transparent)]
    S3(#[from] S3Error),
    /// Inbound journal lane failure.
    #[error(transparent)]
    Reader(#[from] ReaderError),
    /// Transfer-engine failure.
    #[error(transparent)]
    Transfer(#[from] TransferError),
    /// Compaction / GC / lifecycle failure (§2.10).
    #[error(transparent)]
    Compact(#[from] CompactError),
    /// Proxy / thumb generation failure (§4.2).
    #[error(transparent)]
    Proxy(#[from] ProxyError),
    /// Outbound journal / device-registry failure.
    #[error(transparent)]
    Publisher(#[from] PublisherError),
    /// Engine apply / staging failure.
    #[error(transparent)]
    Engine(#[from] EngineError),
    /// Manifest build/encode/transfer failure.
    #[error(transparent)]
    Manifest(#[from] ManifestError),
    /// Journal encode/decode failure.
    #[error(transparent)]
    Journal(#[from] JournalError),
    /// Key classification/construction failure.
    #[error(transparent)]
    Key(#[from] KeyError),
    /// A local filesystem operation failed.
    #[error("{context}: {source}")]
    Io {
        /// What was being attempted.
        context: String,
        /// The underlying I/O failure.
        #[source]
        source: std::io::Error,
    },

    /// An adoption GET returned fewer bytes than its `Content-Length`
    /// declared — a short read not surfaced as a transport error (a proxy
    /// cleanly terminating a chunked body, a future HTTP/2 path). A foreign
    /// original has no journal head to verify against, so a silent short
    /// read would be attested + content-addressed + proxied as if whole,
    /// yielding a wrong `content_id`/proxy and a later false `corrupt_remote`
    /// on peers that fetch the full object. The length is asserted explicitly
    /// and the mismatch skips that one object this cycle (never aborts it).
    #[error(
        "short read adopting {key}: collected {got} bytes but Content-Length declared {declared}"
    )]
    ShortRead {
        /// The object key that short-read.
        key: String,
        /// The `Content-Length` the response declared.
        declared: u64,
        /// The number of bytes actually collected.
        got: u64,
    },

    /// A test-only injected fault (only constructible when the `test-util`
    /// feature is on — the integration suite's crash-injection point for the
    /// atomic-adoption contract). Never constructed in a release build: the
    /// variant and its only construction site are both `test-util`-gated, so
    /// the shipped `rrcloud-worker` binary carries neither.
    #[cfg(feature = "test-util")]
    #[error("injected test fault: {0}")]
    InjectedFault(&'static str),
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// The worker's resolved configuration (§6). `state_dir` is `None` exactly
/// when `RRCLOUD_STATE_DIR` was unset — the stateless case
/// [`Worker::open`] refuses.
#[derive(Clone, Debug)]
pub struct WorkerConfig {
    /// The persistent redb state directory (`RRCLOUD_STATE_DIR`), or `None`
    /// when unset — a stateless invocation [`Worker::open`] refuses.
    pub state_dir: Option<PathBuf>,
    /// S3 endpoint base (`RRCLOUD_ENDPOINT`).
    pub endpoint: String,
    /// Library bucket (`RRCLOUD_BUCKET`).
    pub bucket: String,
    /// S3 region (`RRCLOUD_REGION`, default [`DEFAULT_REGION`]).
    pub region: String,
    /// Access key id (`RRCLOUD_ACCESS_KEY`).
    pub access_key_id: String,
    /// Secret access key (`RRCLOUD_SECRET_KEY`).
    pub secret_access_key: String,
}

impl WorkerConfig {
    /// Resolve the configuration from the process environment. The S3
    /// coordinates and credentials are mandatory (their absence is
    /// [`WorkerError::MissingConfig`]); `RRCLOUD_REGION` defaults to
    /// [`DEFAULT_REGION`]; `RRCLOUD_STATE_DIR` is read into
    /// [`WorkerConfig::state_dir`] as `None` when unset — the mandatory
    /// check is enforced at [`Worker::open`] so the typed refusal happens
    /// before any journal write.
    pub fn from_env() -> Result<WorkerConfig, WorkerError> {
        fn req(name: &'static str) -> Result<String, WorkerError> {
            std::env::var(name)
                .ok()
                .filter(|s| !s.is_empty())
                .ok_or(WorkerError::MissingConfig(name))
        }
        let state_dir = std::env::var(ENV_STATE_DIR)
            .ok()
            .filter(|s| !s.is_empty())
            .map(PathBuf::from);
        let region = std::env::var(ENV_REGION)
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| DEFAULT_REGION.to_string());
        Ok(WorkerConfig {
            state_dir,
            endpoint: req(ENV_ENDPOINT)?,
            bucket: req(ENV_BUCKET)?,
            region,
            access_key_id: req(ENV_ACCESS_KEY)?,
            secret_access_key: req(ENV_SECRET_KEY)?,
        })
    }

    /// The [`S3Config`] this configuration addresses. Addressing is always
    /// path-style (the [`S3Client`] forces it), as §6 requires.
    pub fn s3_config(&self) -> S3Config {
        S3Config {
            endpoint: self.endpoint.clone(),
            region: self.region.clone(),
            access_key_id: self.access_key_id.clone(),
            secret_access_key: self.secret_access_key.clone(),
            // Generous transfer timeouts (the public phone endpoint runs a
            // 900 s read timeout); the idle-read guard unsticks a dead TCP
            // connection without capping a large adoption GET.
            connect_timeout: Some(Duration::from_secs(30)),
            read_timeout: Some(Duration::from_secs(300)),
            request_timeout: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Run mode / per-cycle options / report
// ---------------------------------------------------------------------------

/// How [`run`] drives [`run_cycle`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunMode {
    /// Run exactly one cycle, then return (`--once`).
    Once,
    /// Loop, sleeping `interval` between cycles, until interrupted
    /// (`--daemon --interval <dur>`).
    Daemon {
        /// Delay between the end of one cycle and the start of the next.
        interval: Duration,
    },
}

/// Per-cycle knobs. The defaults are production: the server clock is
/// derived from the state db (`None` → [`ServerClock::from_db`] after a
/// heartbeat records the offset), with the §2.10 and §4.2 defaults. Tests
/// pin a [`ServerClock`] and plant history relative to it.
#[derive(Clone, Copy, Debug, Default)]
pub struct CycleOptions {
    /// The §2.10 server clock, or `None` to derive it from the db this
    /// cycle (production). Tests pass `Some(ServerClock::pinned(..))`.
    pub clock: Option<ServerClock>,
    /// §2.10 horizons / grace / retention configuration.
    pub compact: CompactConfig,
    /// §4.2 proxy / thumb sizes and JPEG quality.
    pub proxy: ProxyParams,
    /// Cap on a single adopted original's buffered size, or `None` for the
    /// production default [`MAX_ADOPT_ORIGINAL_BYTES`]. The adoption GET
    /// buffers the whole object to blake3 it and feed `proxy.rs`; an object
    /// past this cap (or whose body overruns it against a lying
    /// `Content-Length`) is a [`crate::s3::S3Error::BodyCapExceeded`] that
    /// **skips that one object** without aborting the cycle (§6 forward
    /// progress — see [`adopt_foreign_originals`]). Tunable so an operator can
    /// bound the adoption buffer below the 2 GiB default, and so tests can
    /// pin a small cap to exercise the per-item isolation lane deterministically.
    pub max_adopt_original_bytes: Option<usize>,
    /// Test-only crash-injection point for the atomic-adoption contract.
    /// Carried **per call** (not a process-global env var) so a fault in one
    /// test can never race another test's adoption in the shared test
    /// process. `Some("after_stage_before_record")` makes
    /// [`adopt_foreign_originals`] abort its stage+record transaction after
    /// staging the journal entries but before recording the item — modelling
    /// a SIGKILL at exactly the window the single transaction closes. Gated
    /// to the `test-util` feature: the shipped binary's `CycleOptions` has no
    /// such field and no construction site can set it.
    #[cfg(feature = "test-util")]
    pub fault: Option<&'static str>,
}

/// What one [`run_cycle`] did — the structured log a cycle emits (§6 point
/// 3). Every count is observable by tests driving `run_cycle` directly.
#[derive(Debug, Default)]
pub struct CycleReport {
    /// Foreign originals adopted this cycle (journaled `attest` + `put`),
    /// by item relkey. Includes [`CycleReport::adopted_without_proxy`].
    pub adopted: Vec<RelKey>,
    /// Foreign originals adopted this cycle whose smart preview could **not**
    /// be generated (a non-RAW/undecodable `library/` object — still a valid
    /// §1.2 original, so it is journaled `attest` + `put` so phones learn it,
    /// but with no preview/thumbs). Per-item isolation: one such object never
    /// aborts the cycle (§6 forward progress). A subset of
    /// [`CycleReport::adopted`].
    pub adopted_without_proxy: Vec<RelKey>,
    /// §2.7/§6 whole-item resurrections re-advertised this cycle by
    /// [`crate::engine::reconcile_wholeness`] (a live sidecar's tombstoned
    /// original, or the converse), by item relkey — the GC worker's
    /// unconditional whole-item backstop.
    pub resurrected: Vec<RelKey>,
    /// §4.3's second generation duty: journal-known originals (uploaded and
    /// journaled by a client that skipped preview generation) whose smart
    /// preview + thumbs this cycle generated and PUT, by item relkey. No
    /// original version is published for these — only preview/thumb put
    /// entries — so the uploading client's head is never contended.
    pub previews_backfilled: Vec<RelKey>,
    /// Smart previews generated via [`crate::proxy::generate_proxy_with`].
    pub proxies_generated: usize,
    /// Preview objects PUT under `previews/<content_id>`.
    pub previews_put: usize,
    /// Thumb objects PUT under `thumbs/<content_id>_{small,medium}`.
    pub thumbs_put: usize,
    /// Journal entries published under the worker's own prefix this cycle.
    pub journal_entries_published: u64,
    /// Stale multipart uploads aborted (`ListMultipartUploads` +
    /// `AbortMultipartUpload`), by key.
    pub aborted_multipart_uploads: Vec<String>,
    /// §2.10 own-segment compaction outcome.
    pub compaction: CompactionSummary,
    /// §2.10 tombstone-GC outcome.
    pub gc: GcSummary,
    /// Devices retired (explicit or auto) this cycle.
    pub retired: Vec<DeviceId>,
    /// Read-only reporting only ([`Worker::open_readonly`]): the count of
    /// `library/` original objects observed, written nowhere. Always `0` for
    /// the journaling role.
    pub foreign_originals_seen: usize,
}

// ---------------------------------------------------------------------------
// The worker handle
// ---------------------------------------------------------------------------

/// A constructed worker: its durable state db, S3 client, and bucket. Built
/// by [`Worker::open`] (journaling) or [`Worker::open_readonly`]
/// (stateless, reporting only).
pub struct Worker {
    db: SyncDb,
    s3: S3Client,
    bucket: String,
    /// `true` for a [`Worker::open_readonly`] handle: [`run_cycle`] does
    /// **read-only reporting only** — it never heartbeats, journals,
    /// publishes, or GCs, so the ephemeral identity never pollutes the
    /// device registry / §2.10 horizons with a phantom device (§6).
    readonly: bool,
    /// Set only for a [`Worker::open_readonly`] handle: the throwaway redb
    /// directory minted under the system temp dir for its ephemeral
    /// reporting identity. [`Drop`] removes it so a recurring `--report`
    /// cron cannot leak a redb dir per invocation into `/tmp` without bound
    /// (P4 review round 2). `None` for the journaling role, whose state dir
    /// is operator-owned and persistent and must never be removed here.
    ephemeral_dir: Option<PathBuf>,
}

impl Drop for Worker {
    fn drop(&mut self) {
        // Remove the ephemeral read-only reporting dir (never the persistent
        // journaling state dir, which is `None` here). Best-effort: a cleanup
        // failure on a throwaway temp dir is not worth a panic in a `Drop`,
        // and on Linux `remove_dir_all` unlinks the still-open redb file
        // cleanly (the fd stays valid until `db` drops immediately after).
        if let Some(dir) = self.ephemeral_dir.take() {
            let _ = std::fs::remove_dir_all(&dir);
        }
    }
}

impl Worker {
    /// Open the worker in its **journaling** role. Requires a persistent
    /// state directory: a `None` [`WorkerConfig::state_dir`] is
    /// [`WorkerError::StatelessRefusal`], returned before any journal
    /// write. On first open the state db mints and persists one stable
    /// device identity; on reopen the identity (and its monotonic seq) are
    /// read back — an unexpectedly fresh file is
    /// [`WorkerError::StateReset`] (no silent re-mint, no seq reuse).
    pub fn open(cfg: &WorkerConfig) -> Result<Worker, WorkerError> {
        // The mandatory-state check is FIRST, before any S3 client build or
        // network contact (§6 hard startup error).
        let state_dir = cfg
            .state_dir
            .as_deref()
            .ok_or(WorkerError::StatelessRefusal)?;
        std::fs::create_dir_all(state_dir).map_err(|e| WorkerError::Io {
            context: format!("create state directory {}", state_dir.display()),
            source: e,
        })?;
        let db_path = state_dir.join(STATE_DB_FILENAME);
        // File presence is the first-open vs. reopen signal: reopen reads
        // the stored identity (`None` never mints), first open mints a fresh
        // stable id. A "reopen" whose file turns out fresh (identity lost)
        // is the typed StateReset, never a silent re-mint.
        let db = if db_path.exists() {
            match SyncDb::open(&db_path, None) {
                Ok(db) => db,
                Err(StateError::DeviceIdRequired) => {
                    return Err(WorkerError::StateReset(state_dir.to_path_buf()))
                }
                Err(e) => return Err(e.into()),
            }
        } else {
            let id = mint_worker_device_id(state_dir)?;
            SyncDb::open(&db_path, Some(id))?
        };
        let s3 = S3Client::new(cfg.s3_config())?;
        Ok(Worker {
            db,
            s3,
            bucket: cfg.bucket.clone(),
            readonly: false,
            // The journaling role's state dir is operator-owned and
            // persistent — never removed on drop.
            ephemeral_dir: None,
        })
    }

    /// Open the worker in a **read-only reporting** role, with no persistent
    /// state directory. It cannot journal, adopt, or GC — it is the only
    /// path a stateless invocation may take (§6). The ephemeral identity
    /// lives in a throwaway temp directory that is removed when the handle is
    /// dropped (see the `Drop` impl), so a recurring `--report` cron leaks no
    /// redb dirs into `/tmp`; it is never used to publish.
    pub fn open_readonly(cfg: &WorkerConfig) -> Result<Worker, WorkerError> {
        let base = std::env::temp_dir();
        let id = mint_worker_device_id(&base)?;
        let dir = base.join(format!("rrcloud-worker-ro-{}", id.as_str()));
        std::fs::create_dir_all(&dir).map_err(|e| WorkerError::Io {
            context: format!("create ephemeral reporting dir {}", dir.display()),
            source: e,
        })?;
        let db = SyncDb::open(dir.join(STATE_DB_FILENAME), Some(id))?;
        let s3 = S3Client::new(cfg.s3_config())?;
        Ok(Worker {
            db,
            s3,
            bucket: cfg.bucket.clone(),
            readonly: true,
            // The ephemeral reporting dir is removed on drop (no /tmp leak on
            // a recurring --report, P4 review round 2).
            ephemeral_dir: Some(dir),
        })
    }

    /// The worker's stable device identity (minted on first
    /// [`Worker::open`], persisted in the state dir).
    pub fn device_id(&self) -> &DeviceId {
        self.db.device_id()
    }

    /// The durable state db (test/inspection access).
    pub fn db(&self) -> &SyncDb {
        &self.db
    }

    /// The S3 client.
    pub fn client(&self) -> &S3Client {
        &self.s3
    }

    /// The library bucket.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// The directory the state db lives in — the worker's scratch root for
    /// the inbound consumer (loser materialization) and the transfer
    /// config. The worker keeps no local originals; this is a persistent
    /// writable dir, never a sync tree.
    fn work_root(&self) -> PathBuf {
        self.db
            .path()
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."))
    }
}

// ---------------------------------------------------------------------------
// The cycle
// ---------------------------------------------------------------------------

/// Run exactly one worker cycle (§6 duties a–c), idempotent and crash-safe.
///
/// 1. **Catch up + reconcile (§2.3).** Heartbeat the device registry, poll
///    every foreign journal prefix into the state db, then `ListObjectsV2`
///    over `library/`. For each **foreign** original object (present in the
///    listing with no journal-known state): GET it, blake3 it, journal an
///    `attest`, generate a proxy + thumbs
///    ([`crate::proxy::generate_proxy_with`]), PUT the preview/thumb objects
///    under `content_id` keys, and journal `put` entries for the original +
///    preview + thumb under the worker's device id — so phones materialize
///    stubs and seeded thumbs on their next poll, with no bucket
///    notifications.
/// 2. **Hygiene.** Abort stale multipart uploads via
///    [`crate::transfer::abort_stale_uploads`].
/// 3. **§2.10.** Auto-retire long-dead devices, tombstone GC, orphaned-prefix
///    fold, and own-segment compaction (last, so the final manifest covers
///    everything this cycle changed) — reusing `compact.rs`, honoring the
///    grace / horizon / server-time rules.
///
/// `opts.clock` pins the §2.10 "now" when set (tests); production derives
/// it from the db after the heartbeat has recorded the server-time offset.
pub async fn run_cycle(worker: &Worker, opts: &CycleOptions) -> Result<CycleReport, WorkerError> {
    let s3 = &worker.s3;
    let bucket = worker.bucket.as_str();
    let db = &worker.db;
    let mut report = CycleReport::default();

    // A read-only handle ([`Worker::open_readonly`]) does reporting only: it
    // never heartbeats, journals, publishes, or GCs, so its ephemeral
    // identity never pollutes the device registry / §2.10 horizons (§6). It
    // may still READ: it polls every foreign prefix into its throwaway db (a
    // pure bucket read — journal GETs applied to local state, no S3 writes)
    // and merges gapped peers' manifests, so `foreign_originals_seen` counts
    // only the `library/` originals with **no journal-known state** — the
    // genuinely foreign drops, not every original already advertised by a
    // peer or the worker.
    if worker.readonly {
        catch_up(worker).await?;
        let mut seen = 0usize;
        for (key, _) in list_library_keys(s3, bucket).await? {
            if let KeyClass::Original { relkey } = classify_key(&key) {
                if db.get_item(&relkey)?.is_none() && db.get_deleted(&relkey)?.is_none() {
                    seen += 1;
                }
            }
        }
        report.foreign_originals_seen = seen;
        return Ok(report);
    }

    // (0) Heartbeat: register/refresh this worker's registry entry so peers'
    // §2.10 horizons account for it and the server-time offset is recorded.
    heartbeat(worker).await?;

    // The §2.10 "now": pinned by tests, else derived from the just-recorded
    // server-time offset.
    let clock = match opts.clock {
        Some(c) => c,
        None => ServerClock::from_db(db)?,
    };

    // (1a) Catch up (§2.2 poll + §2.3 gap-routed manifest merge): poll every
    // FOREIGN prefix into our state db, and for any peer the poll reports a
    // gap on (its early segments §2.10-compacted), merge that peer's manifest
    // so we learn its advertised items instead of mistaking them for foreign
    // drops. Own prefix is skipped by `poll`.
    catch_up(worker).await?;

    // (1a′) Flush any LEFTOVER staged entries from a prior crashed cycle
    // BEFORE adopting again. Adoption (1c) now stages its journal entries and
    // records the item in ONE transaction, so a crash can no longer leave
    // staged-but-unrecorded adoption entries; this leading drain is the
    // belt-and-braces cleanup for entries staged by an OLDER (pre-atomic)
    // binary or any other lane, so they are published under our prefix as
    // their own entries instead of commingling with (and being
    // double-counted against) this cycle's fresh staging at (1d). A no-op on
    // the common path (nothing pending).
    let leftover = publish_pending(db, s3, bucket).await?;
    report.journal_entries_published += leftover.entries;

    // (1b) §2.7/§6 whole-item backstop at quiescence: re-advertise any
    // original a live sidecar still references that a delete-vs-edit race
    // left tombstoned (and the converse). This is the GC worker's
    // *unconditional* whole-item backstop — the catch-all the author-only
    // inline `apply_del` lanes miss when the surviving editor went offline
    // before applying the delete, so §2.10 GC below can never destroy a RAW a
    // live sidecar still references. Runs at quiescence (post-poll/merge), so
    // its decision is a pure function of converged local state (§2.11). Its
    // staged resurrection puts are published by (1d).
    {
        let mut events = ();
        report.resurrected = reconcile_wholeness(db, &mut events)?;
    }

    // (1c) Reconcile: adopt every foreign original that has no journal-known
    // state (staging only — per-item isolated, so one undecodable object
    // never aborts the cycle).
    adopt_foreign_originals(worker, opts, &clock, &mut report).await?;

    // (1c′) §4.3's second generation duty: backfill previews for
    // journal-known originals whose uploading client skipped generation
    // (staging only, same single publish point as adoption).
    backfill_known_previews(worker, opts, &clock, &mut report).await?;

    // (1d) Publish everything staged this cycle (adoptions + resurrections)
    // under our own prefix in one drain — a single publish point, so a poison
    // item earlier in (1c) can never strand already-staged entries unpublished.
    let published = publish_pending(db, s3, bucket).await?;
    report.journal_entries_published += published.entries;

    // (2) Hygiene: abort stale multipart uploads (the portable mechanism —
    // ListMultipartUploads + AbortMultipartUpload, no lifecycle-rule
    // dependency, §2.4). The age gate uses §2.10 **server** time (the
    // multipart Initiated timestamps are server-provided), not the local
    // wall clock.
    {
        let backend = match stored_backend_profile(db)? {
            Some(b) => b,
            None => probe_backend(db, s3, bucket).await?,
        };
        let tcfg = TransferConfig::new(bucket.to_string(), worker.work_root(), backend);
        let stale =
            abort_stale_uploads(db, s3, &tcfg, STALE_UPLOAD_MAX_AGE_SECS, clock.now_server())
                .await?;
        for (rel, _id) in stale.aborted_own {
            report.aborted_multipart_uploads.push(library_key(&rel));
        }
        for (key, _id) in stale.aborted_orphans {
            report.aborted_multipart_uploads.push(key);
        }
        for (rel, _id) in stale.cleared_recordless {
            report.aborted_multipart_uploads.push(library_key(&rel));
        }
    }

    // (3) §2.10: retire dead devices first (so horizons stop counting them),
    // GC tombstones, fold retired devices' orphaned prefixes, then compact
    // our own segments LAST — each builds from the same durable state, and
    // compaction's own-manifest coverage check reflects everything above
    // (including this cycle's resurrections, so a resurrected original is in
    // the manifest before any peer's horizon could GC it).
    report.retired = auto_retire_sweep(s3, bucket, &clock, &opts.compact).await?;
    report.gc = tombstone_gc(db, s3, bucket, &clock, &opts.compact).await?;
    // Orphaned-prefix reclaim: folded into our manifest losslessly; the
    // per-device summaries are not part of the §6 cycle report.
    let _orphans = gc_retired_prefixes(db, s3, bucket, &clock, &opts.compact).await?;
    report.compaction = compact_own_segments(db, s3, bucket, &clock, &opts.compact).await?;

    Ok(report)
}

/// §2.2 steady-state poll + §2.3 gap-routed catch-up.
///
/// Poll every foreign prefix; then for every device the poll reports a
/// **bootstrap or mid-stream gap** on — its journal starts past where we can
/// apply, because its early segments were §2.10-compacted — GET that peer's
/// **manifest** and merge it. The §2.3 catch-up attestation seeds the cursor
/// past the gap and folds the compacted entries' effects (live rows + the
/// deleted set); a re-poll then applies anything past the now-seeded cursors.
///
/// Without this a peer original advertised **only** in a manifest (its
/// journal segment compacted) would be mistaken for a foreign drop and
/// re-adopted — a spurious `{worker:1}` version concurrent with the peer's,
/// plus a duplicate proxy — and a peer's compacted soft-`del` whose bytes are
/// still in the grace window would be resurrected (reopening A3). The merge
/// routes the gap to the manifest so neither happens.
async fn catch_up(worker: &Worker) -> Result<(), WorkerError> {
    let db = &worker.db;
    let s3 = &worker.s3;
    let bucket = worker.bucket.as_str();

    let report = {
        let mut events = ();
        let mut consumer = EngineConsumer::new(db, worker.work_root(), &mut events)?;
        poll(db, s3, bucket, &mut consumer).await?
    };

    // The peers whose journals are gapped (bootstrap or mid-stream).
    let mut gapped: BTreeSet<DeviceId> = BTreeSet::new();
    for gap in &report.gaps {
        gapped.insert(gap.device.clone());
    }
    for gap in &report.mid_stream_gaps {
        gapped.insert(gap.device.clone());
    }
    if gapped.is_empty() {
        return Ok(());
    }

    // GET each gapped peer's manifest (the §2.3 catch-up attestation).
    let mut manifests: Vec<(DeviceId, Manifest)> = Vec::new();
    for device in gapped {
        match get_manifest(s3, bucket, &device).await {
            Ok(m) => manifests.push((device, m)),
            // A gapped peer with no manifest yet: nothing to merge this pass;
            // the gap re-reports next cycle until the peer (or its retiring GC
            // worker) writes/folds one. Never a cycle-aborting error.
            Err(ManifestError::S3(e)) if e.is_no_such_key() => {}
            Err(e) => return Err(e.into()),
        }
    }
    if manifests.is_empty() {
        return Ok(());
    }

    // Merge through the same engine consumer as journal replay (§2.3), then
    // re-poll to apply entries past the now-seeded cursors.
    {
        let mut events = ();
        let mut consumer = EngineConsumer::new(db, worker.work_root(), &mut events)?;
        merge_manifests(&manifests, db, &mut consumer)?;
    }
    {
        let mut events = ();
        let mut consumer = EngineConsumer::new(db, worker.work_root(), &mut events)?;
        poll(db, s3, bucket, &mut consumer).await?;
    }
    Ok(())
}

/// The §2.2/§2.10 heartbeat: PUT this worker's device-registry entry with
/// the applied cursors snapshotted from the state db. `created` is read back
/// from the existing registry entry so it stays stable across runs; a
/// first-ever heartbeat seeds it from the best server-time estimate.
async fn heartbeat(worker: &Worker) -> Result<(), WorkerError> {
    let db = &worker.db;
    let s3 = &worker.s3;
    let bucket = worker.bucket.as_str();
    let created = match get_device_entry(s3, bucket, db.device_id()).await {
        Ok(entry) => entry.created,
        Err(PublisherError::S3(e)) if e.is_no_such_key() => server_ts_estimate(db)?,
        Err(e) => return Err(e.into()),
    };
    let short = &db.device_id().as_str()[..8];
    let profile = DeviceProfile {
        name: format!("rrcloud-worker-{short}"),
        platform: std::env::consts::OS.to_string(),
        created,
    };
    put_device_entry(db, s3, bucket, &profile).await?;
    Ok(())
}

/// §2.3 reconcile + §2.1 foreign adoption: list `library/`, and for each
/// original with no journal-known state (no local item record after the
/// catch-up poll + merge) GET → blake3 → proxy → PUT preview/thumbs →
/// **stage** `attest` + `put` (original/preview/thumb), recording the
/// adopted original in our own state so a re-run is a no-op and the manifest
/// advertises it. Staging only — [`run_cycle`] publishes once, afterward.
///
/// Per-item isolation (§6 forward progress): a non-RAW/undecodable
/// `library/` object is still a valid §1.2 original, so its proxy failure
/// does **not** abort the cycle — the original is adopted `attest` + `put`
/// (so phones learn it), with no preview/thumbs, and recorded in
/// [`CycleReport::adopted_without_proxy`]. Because it is still recorded in
/// local state, a later cycle skips it (no re-poison), and hygiene + §2.10
/// GC always run.
async fn adopt_foreign_originals(
    worker: &Worker,
    opts: &CycleOptions,
    clock: &ServerClock,
    report: &mut CycleReport,
) -> Result<(), WorkerError> {
    let db = &worker.db;
    let s3 = &worker.s3;
    let bucket = worker.bucket.as_str();
    let device = db.device_id().clone();
    let now_ts = clock.now_server();
    let cap = opts
        .max_adopt_original_bytes
        .unwrap_or(MAX_ADOPT_ORIGINAL_BYTES);

    for (key, last_modified) in list_library_keys(s3, bucket).await? {
        let relkey = match classify_key(&key) {
            KeyClass::Original { relkey } => relkey,
            // Sidecars/xmp/foreign keys are not adopted as originals here.
            _ => continue,
        };
        // Journal-known state (an item record OR a deleted-set row, learned
        // from the journal or a §2.3 manifest merge) → already adopted/known
        // by us or a peer, or soft-deleted with bytes still in grace. Skip:
        // re-adopting would spuriously conflict, and re-adopting a deleted
        // key would resurrect it (A3).
        if db.get_item(&relkey)?.is_some() || db.get_deleted(&relkey)?.is_some() {
            continue;
        }

        // Foreign original: GET the bytes and derive identity. PER-ITEM
        // ISOLATED (§6 forward progress): the fetch is the ONLY network step
        // whose failure is per-item — a LIST-vs-GET `NoSuchKey` race, a
        // transient 5xx, a `BodyCapExceeded` on an object past `cap`, or a
        // short read (bytes != declared Content-Length) all LOG + SKIP this
        // one object rather than abort the cycle. A persistent poison object
        // (oversized/corrupt) is skipped every cycle without ever wedging the
        // adoption of other originals, the resurrection backstop, hygiene, or
        // §2.10 GC. A transient failure simply retries next cycle (the object
        // was never recorded).
        let bytes = match fetch_foreign_original(s3, bucket, &key, cap).await {
            Ok(b) => b,
            Err(e) => {
                eprintln!(
                    "rrcloud-worker: skipping {key} this cycle (adoption fetch failed, not \
                     aborting the cycle): {e}"
                );
                continue;
            }
        };
        let blake3 = Blake3Hex::from_bytes(&bytes);
        let size = bytes.len() as u64;
        // §2.2/§3.5: carry the object's server mtime so phones materialize
        // the stub at the real mtime, not epoch-0. The LIST `LastModified` is
        // the server-provided ISO-8601 timestamp (NOT an RFC-1123 HTTP-date),
        // so it is parsed as RFC 3339 first (see `parse_object_last_modified`).
        let mtime_unix = last_modified
            .as_deref()
            .and_then(parse_object_last_modified)
            .filter(|s| *s > 0)
            .unwrap_or(0);
        let mtime_unix_ns = mtime_unix.saturating_mul(1_000_000_000);

        // Smart preview + thumbs (§4.2), via the same pinned rawler the app
        // resolves — color parity by construction. Per-item isolated: a
        // non-RAW/undecodable original (a valid §1.2 original the proxy
        // pipeline cannot represent) is adopted WITHOUT a proxy rather than
        // aborting the cycle.
        let content_id = ContentId::from_bytes(&bytes);
        let proxy = match generate_proxy_with(&bytes, &opts.proxy) {
            Ok(p) => Some(p),
            Err(e) => {
                eprintln!(
                    "rrcloud-worker: adopting {} without a proxy (preview generation failed: {e})",
                    key
                );
                None
            }
        };

        // The worker is the first to version this foreign original, so the
        // §2.6 version vector is {worker: 1}.
        let vv: VersionVector = [(device.clone(), 1u32)].into_iter().collect();

        // A staging template; each entry clones it and sets its per-kind
        // fields. `attest` snapshots the version whose bytes we verified
        // (§2.2/§3.5 eviction gate for every original the worker touches).
        let tmpl = JournalEntry {
            v: JOURNAL_VERSION,
            seq: 0,
            ts: now_ts,
            device: device.clone(),
            op: Op::Put,
            kind: Kind::Original,
            key: String::new(),
            vv: vv.clone(),
            size: None,
            blake3: None,
            sem_hash: None,
            rating: None,
            color_label: None,
            content_id: None,
            w: None,
            h: None,
            mtime: None,
            from_key: None,
        };

        let mut attest = tmpl.clone();
        attest.op = Op::Attest;
        attest.kind = Kind::Original;
        attest.key = library_key(&relkey);
        attest.blake3 = Some(blake3.clone());
        attest.size = Some(size);

        let mut put_original = tmpl.clone();
        put_original.op = Op::Put;
        put_original.kind = Kind::Original;
        put_original.key = library_key(&relkey);
        put_original.blake3 = Some(blake3.clone());
        put_original.size = Some(size);
        put_original.mtime = (mtime_unix > 0).then_some(mtime_unix);

        // The preview/thumb lane only when the proxy was generated; a
        // no-proxy original carries no `content_id` (there is no preview to
        // address) and no dimensions. The preview/thumb OBJECTS are PUT here
        // (network, content-addressed → idempotent on a re-run), and the
        // matching journal entries are collected into `extra` to be staged
        // atomically below.
        let mut extra: Vec<JournalEntry> = Vec::new();
        let (record_content_id, record_w, record_h) = if let Some(proxy) = proxy {
            report.proxies_generated += 1;
            let orig_w = proxy.orig_width;
            let orig_h = proxy.orig_height;
            let preview_hash = Blake3Hex::from_bytes(&proxy.dng);
            let preview_len = proxy.dng.len() as u64;
            let small_hash = Blake3Hex::from_bytes(&proxy.small_jpeg);
            let small_len = proxy.small_jpeg.len() as u64;
            let medium_hash = Blake3Hex::from_bytes(&proxy.medium_jpeg);
            let medium_len = proxy.medium_jpeg.len() as u64;

            // PUT the content-addressed preview + thumb objects.
            s3.put_object(
                bucket,
                &preview_key(&content_id),
                Bytes::from(proxy.dng),
                &PutObjectOptions::default(),
            )
            .await?;
            report.previews_put += 1;
            s3.put_object(
                bucket,
                &thumb_key(&content_id, ThumbSize::Small),
                Bytes::from(proxy.small_jpeg),
                &PutObjectOptions::default(),
            )
            .await?;
            s3.put_object(
                bucket,
                &thumb_key(&content_id, ThumbSize::Medium),
                Bytes::from(proxy.medium_jpeg),
                &PutObjectOptions::default(),
            )
            .await?;
            report.thumbs_put += 2;

            put_original.content_id = Some(content_id.clone());
            put_original.w = Some(orig_w);
            put_original.h = Some(orig_h);

            let mut put_preview = tmpl.clone();
            put_preview.op = Op::Put;
            put_preview.kind = Kind::Preview;
            put_preview.key = preview_key(&content_id);
            put_preview.blake3 = Some(preview_hash);
            put_preview.size = Some(preview_len);
            put_preview.content_id = Some(content_id.clone());
            extra.push(put_preview);

            let mut put_thumb_small = tmpl.clone();
            put_thumb_small.op = Op::Put;
            put_thumb_small.kind = Kind::Thumb;
            put_thumb_small.key = thumb_key(&content_id, ThumbSize::Small);
            put_thumb_small.blake3 = Some(small_hash);
            put_thumb_small.size = Some(small_len);
            put_thumb_small.content_id = Some(content_id.clone());
            extra.push(put_thumb_small);

            let mut put_thumb_medium = tmpl.clone();
            put_thumb_medium.op = Op::Put;
            put_thumb_medium.kind = Kind::Thumb;
            put_thumb_medium.key = thumb_key(&content_id, ThumbSize::Medium);
            put_thumb_medium.blake3 = Some(medium_hash);
            put_thumb_medium.size = Some(medium_len);
            put_thumb_medium.content_id = Some(content_id.clone());
            extra.push(put_thumb_medium);

            (Some(content_id.clone()), Some(orig_w), Some(orig_h))
        } else {
            report.adopted_without_proxy.push(relkey.clone());
            (None, None, None)
        };

        // Record the adopted original in our own state: a re-run sees it as
        // journal-known (idempotence), and the manifest advertises it as a
        // live row. The worker keeps no local bytes, but it GET-and-blake3'd
        // the object and attested it, so `verified_remote`/`attested` hold.
        let record = ItemRecord {
            kind: Kind::Original,
            state: ItemState::Synced,
            size,
            mtime_unix_ns,
            blake3: Some(blake3),
            sem_hash: None,
            vv,
            content_id: record_content_id,
            w: record_w,
            h: record_h,
            pinned: false,
            last_access_unix: 0,
            verified_remote: true,
            attested: true,
            base_unknown: false,
            rating: None,
            color_label: None,
            device: Some(device.clone()),
            head_ts: Some(now_ts),
            admitted_vv: None,
            admitted_ts: None,
            deleted: false,
        };

        // ATOMIC stage + record (crash-safe idempotence, the §6 contract):
        // stage the attest, the preview/thumb puts, and the original put AND
        // record the item in ONE transaction. Either all of them commit
        // together, or none do — so a crash (SIGKILL/OOM/pod-eviction) can
        // never leave staged-but-unrecorded adoption entries that a re-run
        // would double-publish. The item record is the idempotence guard: if
        // this commits, the re-run's skip-check (`get_item`) short-circuits;
        // if it does not, nothing was staged, so the re-run re-adopts cleanly
        // (exactly once). The preview/thumb OBJECTS were PUT above
        // content-addressed, so re-PUTting them on a clean re-adopt is a
        // byte-identical overwrite, never a duplicate.
        db.with_txn_err::<(), WorkerError>(|t| {
            enqueue_entry_in(t, &device, &attest)?;
            for e in &extra {
                enqueue_entry_in(t, &device, e)?;
            }
            enqueue_entry_in(t, &device, &put_original)?;
            // Test-only crash-injection point: a fault raised HERE — after
            // staging, before the record write — aborts the whole
            // transaction (nothing staged, nothing recorded), which is the
            // very property this single-transaction structure guarantees.
            #[cfg(feature = "test-util")]
            if opts.fault == Some("after_stage_before_record") {
                return Err(WorkerError::InjectedFault("after_stage_before_record"));
            }
            t.replay_put_item(&relkey, &record)?;
            Ok(())
        })?;
        report.adopted.push(relkey);
    }

    Ok(())
}

/// §4.3's second generation duty: for every journal-known live original
/// whose `content_id` has no preview object in the bucket (the uploading
/// client skipped generation — today's app journals the original put but
/// never uploads preview/thumbs), GET the original, generate the smart
/// preview + thumbs, PUT them under the content-addressed keys, and stage
/// matching put entries (published by the caller's single publish point).
///
/// Deliberately does NOT publish any original version: the preview/thumb
/// lane is content-addressed and versionless, so the uploading device's
/// head is never contended (unlike adoption, which versions a previously
/// unknown key). Idempotence is structural: the next cycle's preview
/// listing contains the content id, so the item is skipped — no per-item
/// marker state, safe with a second concurrent backfiller (byte-identical
/// content-addressed overwrites).
///
/// Per-item isolation, same contract as adoption: a failed GET, a
/// blake3-vs-record mismatch (a stale or replaced object), or an
/// undecodable original logs and skips that one item without aborting the
/// cycle; a transient failure retries next cycle.
async fn backfill_known_previews(
    worker: &Worker,
    opts: &CycleOptions,
    clock: &ServerClock,
    report: &mut CycleReport,
) -> Result<(), WorkerError> {
    let db = &worker.db;
    let s3 = &worker.s3;
    let bucket = worker.bucket.as_str();
    let device = db.device_id().clone();
    let now_ts = clock.now_server();
    let cap = opts
        .max_adopt_original_bytes
        .unwrap_or(MAX_ADOPT_ORIGINAL_BYTES);

    let have = list_preview_content_ids(s3, bucket).await?;

    for (relkey, record) in db.iter_items()? {
        if record.deleted || record.kind != Kind::Original {
            continue;
        }
        let Some(content_id) = record.content_id.clone() else {
            continue;
        };
        if have.contains(&content_id) {
            continue;
        }

        let key = library_key(&relkey);
        let bytes = match fetch_foreign_original(s3, bucket, &key, cap).await {
            Ok(b) => b,
            Err(e) => {
                eprintln!(
                    "rrcloud-worker: skipping preview backfill of {key} this cycle (fetch \
                     failed, not aborting the cycle): {e}"
                );
                continue;
            }
        };
        // The journal head names the bytes this preview must render. A
        // mismatch means the object was replaced/corrupted relative to the
        // merged head — backfilling from it would journal a preview for
        // content the head does not describe.
        let fetched_blake3 = Blake3Hex::from_bytes(&bytes);
        if record.blake3.as_ref() != Some(&fetched_blake3) {
            eprintln!(
                "rrcloud-worker: skipping preview backfill of {key} (object blake3 does not \
                 match the journal-known head)"
            );
            continue;
        }

        let proxy = match generate_proxy_with(&bytes, &opts.proxy) {
            Ok(p) => p,
            Err(e) => {
                eprintln!(
                    "rrcloud-worker: skipping preview backfill of {key} (preview generation \
                     failed): {e}"
                );
                continue;
            }
        };
        report.proxies_generated += 1;
        let orig_w = proxy.orig_width;
        let orig_h = proxy.orig_height;
        let preview_hash = Blake3Hex::from_bytes(&proxy.dng);
        let preview_len = proxy.dng.len() as u64;
        let small_hash = Blake3Hex::from_bytes(&proxy.small_jpeg);
        let small_len = proxy.small_jpeg.len() as u64;
        let medium_hash = Blake3Hex::from_bytes(&proxy.medium_jpeg);
        let medium_len = proxy.medium_jpeg.len() as u64;

        s3.put_object(
            bucket,
            &preview_key(&content_id),
            Bytes::from(proxy.dng),
            &PutObjectOptions::default(),
        )
        .await?;
        report.previews_put += 1;
        s3.put_object(
            bucket,
            &thumb_key(&content_id, ThumbSize::Small),
            Bytes::from(proxy.small_jpeg),
            &PutObjectOptions::default(),
        )
        .await?;
        s3.put_object(
            bucket,
            &thumb_key(&content_id, ThumbSize::Medium),
            Bytes::from(proxy.medium_jpeg),
            &PutObjectOptions::default(),
        )
        .await?;
        report.thumbs_put += 2;

        // Journal the preview/thumb puts under the head's version vector
        // (the vector names which original version these renditions render;
        // the worker is not authoring a new original version).
        let tmpl = JournalEntry {
            v: JOURNAL_VERSION,
            seq: 0,
            ts: now_ts,
            device: device.clone(),
            op: Op::Put,
            kind: Kind::Preview,
            key: String::new(),
            vv: record.vv.clone(),
            size: None,
            blake3: None,
            sem_hash: None,
            rating: None,
            color_label: None,
            content_id: Some(content_id.clone()),
            w: None,
            h: None,
            mtime: None,
            from_key: None,
        };
        let mut put_preview = tmpl.clone();
        put_preview.key = preview_key(&content_id);
        put_preview.blake3 = Some(preview_hash);
        put_preview.size = Some(preview_len);
        let mut put_thumb_small = tmpl.clone();
        put_thumb_small.kind = Kind::Thumb;
        put_thumb_small.key = thumb_key(&content_id, ThumbSize::Small);
        put_thumb_small.blake3 = Some(small_hash);
        put_thumb_small.size = Some(small_len);
        let mut put_thumb_medium = tmpl.clone();
        put_thumb_medium.kind = Kind::Thumb;
        put_thumb_medium.key = thumb_key(&content_id, ThumbSize::Medium);
        put_thumb_medium.blake3 = Some(medium_hash);
        put_thumb_medium.size = Some(medium_len);

        // Stage the entries and record the measured dims on our own copy of
        // the item in ONE transaction (same crash-safety shape as adoption:
        // either the entries are staged and the dims recorded, or neither).
        let mut updated = record.clone();
        if updated.w.is_none() {
            updated.w = Some(orig_w);
        }
        if updated.h.is_none() {
            updated.h = Some(orig_h);
        }
        db.with_txn_err::<(), WorkerError>(|t| {
            enqueue_entry_in(t, &device, &put_preview)?;
            enqueue_entry_in(t, &device, &put_thumb_small)?;
            enqueue_entry_in(t, &device, &put_thumb_medium)?;
            t.replay_put_item(&relkey, &updated)?;
            Ok(())
        })?;
        report.previews_backfilled.push(relkey);
    }

    Ok(())
}

/// The set of `content_id`s that already have a preview object under
/// `.rrcloud/v1/previews/` — one paginated LIST per cycle, the structural
/// idempotence guard for [`backfill_known_previews`]. Unparseable keys
/// under the prefix are ignored (future formats are not this cycle's
/// concern).
async fn list_preview_content_ids(
    s3: &S3Client,
    bucket: &str,
) -> Result<std::collections::HashSet<ContentId>, WorkerError> {
    let mut have = std::collections::HashSet::new();
    let mut continuation_token: Option<String> = None;
    loop {
        let page = s3
            .list_objects_v2(
                bucket,
                &ListObjectsV2Request {
                    prefix: Some(format!("{CONTROL_PREFIX}previews/")),
                    continuation_token: continuation_token.take(),
                    ..Default::default()
                },
            )
            .await?;
        for o in page.objects {
            if let KeyClass::Preview { content_id } = classify_key(&o.key) {
                have.insert(content_id);
            }
        }
        if !page.is_truncated {
            return Ok(have);
        }
        match page.next_continuation_token {
            Some(token) => continuation_token = Some(token),
            None => return Ok(have),
        }
    }
}

/// GET a foreign original with per-item isolation (§6 forward progress): the
/// whole body is buffered to `cap` and its length is asserted against the
/// declared `Content-Length`. A body over `cap` (an oversized/over-length
/// drop) is a [`S3Error::BodyCapExceeded`]; a body SHORTER than its declared
/// length — a short read not surfaced as a transport error — is the typed
/// [`WorkerError::ShortRead`]. A foreign original has no journal head to
/// verify against, so the explicit length check is the only guard against
/// attesting + content-addressing + proxying a truncated object as if whole.
/// Every error here is caught by the caller and skips that one object.
async fn fetch_foreign_original(
    s3: &S3Client,
    bucket: &str,
    key: &str,
    cap: usize,
) -> Result<Bytes, WorkerError> {
    let got = s3.get_object(bucket, key, None).await?;
    let declared = got.content_length;
    let bytes = got.body.collect_capped(cap).await?;
    if bytes.len() as u64 != declared {
        return Err(WorkerError::ShortRead {
            key: key.to_string(),
            declared,
            got: bytes.len() as u64,
        });
    }
    Ok(bytes)
}

/// Parse an object's server timestamp to unix seconds. A `ListObjectsV2`
/// `LastModified` is an ISO-8601 / RFC 3339 value (e.g.
/// `2026-10-03T13:47:23.060Z` on Garage), so RFC 3339 is tried FIRST; a
/// HEAD/GET `Last-Modified` header (RFC-1123 IMF-fixdate) is accepted as a
/// fallback via [`crate::publisher::parse_http_date`]. Returns `None` for an
/// unparsable value, so the caller falls back to no mtime rather than
/// epoch-0 masquerading as a real time.
fn parse_object_last_modified(s: &str) -> Option<i64> {
    time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
        .ok()
        .map(|dt| dt.unix_timestamp())
        .or_else(|| crate::publisher::parse_http_date(s))
}

/// One paged `ListObjectsV2` over `library/`, following continuation tokens.
/// Each entry pairs the object key with its `LastModified` (the §2.2 mtime
/// source for adopted originals).
async fn list_library_keys(
    s3: &S3Client,
    bucket: &str,
) -> Result<Vec<(String, Option<String>)>, WorkerError> {
    let mut keys = Vec::new();
    let mut continuation_token: Option<String> = None;
    loop {
        let page = s3
            .list_objects_v2(
                bucket,
                &ListObjectsV2Request {
                    prefix: Some(LIBRARY_PREFIX.to_string()),
                    continuation_token: continuation_token.take(),
                    ..Default::default()
                },
            )
            .await?;
        keys.extend(page.objects.into_iter().map(|o| (o.key, o.last_modified)));
        if !page.is_truncated {
            return Ok(keys);
        }
        match page.next_continuation_token {
            Some(token) => continuation_token = Some(token),
            None => return Ok(keys),
        }
    }
}

/// Drive [`run_cycle`] per [`RunMode`]: once (then return) or as a daemon
/// looping on `interval`. Structured per-cycle logs go to stdout. In daemon
/// mode a transient cycle error is logged and the next cycle still runs
/// (cycles are idempotent and crash-safe), so one bad poll never kills the
/// loop; a clean shutdown is simply a signal between cycles — each cycle
/// commits atomically.
pub async fn run(worker: &Worker, mode: RunMode, opts: &CycleOptions) -> Result<(), WorkerError> {
    match mode {
        RunMode::Once => {
            let report = run_cycle(worker, opts).await?;
            log_cycle(&report);
            Ok(())
        }
        RunMode::Daemon { interval } => loop {
            match run_cycle(worker, opts).await {
                Ok(report) => log_cycle(&report),
                Err(e) => eprintln!("rrcloud-worker: cycle error (continuing): {e}"),
            }
            tokio::time::sleep(interval).await;
        },
    }
}

/// Structured per-cycle log line to stdout (§6 point 3).
fn log_cycle(report: &CycleReport) {
    println!(
        "rrcloud-worker cycle: adopted={} adopted_no_proxy={} resurrected={} proxies={} \
         previews_put={} thumbs_put={} journaled={} aborted_uploads={} gc_destroyed={} \
         gc_retained={} compacted_segments={} retired={}",
        report.adopted.len(),
        report.adopted_without_proxy.len(),
        report.resurrected.len(),
        report.proxies_generated,
        report.previews_put,
        report.thumbs_put,
        report.journal_entries_published,
        report.aborted_multipart_uploads.len(),
        report.gc.destroyed.len(),
        report.gc.retained.len(),
        report.compaction.deleted_seqs.len(),
        report.retired.len(),
    );
    for relkey in &report.adopted {
        println!("  adopted {}", relkey.as_str());
    }
    for relkey in &report.resurrected {
        println!("  resurrected {}", relkey.as_str());
    }
    for device in &report.retired {
        println!("  retired {device}");
    }
}

/// Mint a fresh, stable worker device identity: a canonical UUIDv4 derived
/// from process entropy (wall-clock nanos, pid, a per-process counter) mixed
/// with the state-dir path so two deployments on one host never collide. The
/// result is persisted by [`crate::state::SyncDb`] on first open and read
/// back on every reopen, so it is minted exactly once per state volume.
pub(crate) fn mint_worker_device_id(salt: &Path) -> Result<DeviceId, WorkerError> {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let mut hasher = blake3::Hasher::new();
    hasher.update(&nanos.to_le_bytes());
    hasher.update(&(std::process::id() as u64).to_le_bytes());
    hasher.update(&COUNTER.fetch_add(1, Ordering::Relaxed).to_le_bytes());
    hasher.update(salt.as_os_str().as_encoded_bytes());
    let digest = hasher.finalize();
    let b = digest.as_bytes();
    let mut u = [0u8; 16];
    u.copy_from_slice(&b[..16]);
    // Set the UUIDv4 version (4) and variant (10xx) bits.
    u[6] = (u[6] & 0x0f) | 0x40;
    u[8] = (u[8] & 0x3f) | 0x80;
    let s = format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-\
         {:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        u[0],
        u[1],
        u[2],
        u[3],
        u[4],
        u[5],
        u[6],
        u[7],
        u[8],
        u[9],
        u[10],
        u[11],
        u[12],
        u[13],
        u[14],
        u[15],
    );
    DeviceId::new(s).map_err(|e| WorkerError::Mint(e.to_string()))
}
