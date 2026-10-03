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
//! [`worker::run_cycle`], with no tauri/gtk/webkit anywhere. This is a
//! deliberate, documented refinement of §6 (see
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
//!   find foreign originals, [`crate::proxy::generate_proxy`] for the
//!   smart preview + thumbs, [`crate::engine::EnginePut`] +
//!   [`crate::publisher`] to journal the `attest` + `put` entries, and
//!   [`crate::transfer`] to upload the preview/thumb objects;
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

use std::path::PathBuf;
use std::time::Duration;

use crate::clock::DeviceId;
use crate::compact::{CompactConfig, CompactError, CompactionSummary, GcSummary, ServerClock};
use crate::engine::EngineError;
use crate::journal::JournalError;
use crate::keys::{KeyError, RelKey};
use crate::manifest::ManifestError;
use crate::proxy::{ProxyError, ProxyParams};
use crate::publisher::PublisherError;
use crate::reader::ReaderError;
use crate::s3::{S3Client, S3Config, S3Error};
use crate::state::{StateError, SyncDb};
use crate::transfer::TransferError;

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
        todo!("P4 green: resolve WorkerConfig from the RRCLOUD_* environment")
    }

    /// The [`S3Config`] this configuration addresses. Addressing is always
    /// path-style (the [`S3Client`] forces it), as §6 requires.
    pub fn s3_config(&self) -> S3Config {
        todo!("P4 green: build the path-style S3Config from this WorkerConfig")
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
}

/// What one [`run_cycle`] did — the structured log a cycle emits (§6 point
/// 3). Every count is observable by tests driving `run_cycle` directly.
#[derive(Debug, Default)]
pub struct CycleReport {
    /// Foreign originals adopted this cycle (journaled `attest` + `put`),
    /// by item relkey.
    pub adopted: Vec<RelKey>,
    /// Smart previews generated via [`crate::proxy::generate_proxy`].
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
        todo!("P4 green: enforce the mandatory state dir, open/mint the durable SyncDb, build the S3 client")
    }

    /// Open the worker in a **read-only reporting** role, with no state
    /// directory. It cannot journal, adopt, or GC — it is the only path a
    /// stateless invocation may take (§6).
    pub fn open_readonly(cfg: &WorkerConfig) -> Result<Worker, WorkerError> {
        todo!("P4 green: build a stateless read-only reporting worker")
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
}

// ---------------------------------------------------------------------------
// The cycle
// ---------------------------------------------------------------------------

/// Run exactly one worker cycle (§6 duties a–c), idempotent and crash-safe.
///
/// 1. **Catch up + reconcile (§2.3).** Poll every foreign journal prefix
///    into the state db, then `ListObjectsV2` over `library/`. For each
///    **foreign** original object (present in the listing with no
///    journal-known state): GET it, blake3 it, journal an `attest`,
///    generate a proxy + thumbs ([`crate::proxy::generate_proxy`]), PUT the
///    preview/thumb objects under `content_id` keys, and journal `put`
///    entries for the original + preview + thumb under the worker's device
///    id — so phones materialize stubs and seeded thumbs on their next
///    poll, with no bucket notifications.
/// 2. **Hygiene.** Validate `missing`/`corrupt_remote` flags; abort stale
///    multipart uploads via [`crate::transfer::abort_stale_uploads`].
/// 3. **§2.10.** Tombstone GC, own-segment compaction, manifest
///    fold/rewrite, and retire long-dead devices (incl. folding orphaned
///    prefixes) — reusing `compact.rs`, honoring the grace / horizon /
///    server-time rules.
///
/// `opts.clock` pins the §2.10 "now" when set (tests); production derives
/// it from the db.
pub async fn run_cycle(worker: &Worker, opts: &CycleOptions) -> Result<CycleReport, WorkerError> {
    let _ = (worker, opts);
    todo!(
        "P4 green: orchestrate the §6 reconcile + hygiene + §2.10 cycle over the existing modules"
    )
}

/// Drive [`run_cycle`] per [`RunMode`]: once (then return) or as a daemon
/// looping on `interval` with a clean shutdown. Structured per-cycle logs
/// go to stdout.
pub async fn run(worker: &Worker, mode: RunMode, opts: &CycleOptions) -> Result<(), WorkerError> {
    let _ = (worker, mode, opts);
    todo!("P4 green: --once / --daemon loop with clean shutdown and structured logging")
}
