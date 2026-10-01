//! The per-item transfer engine (architecture §2.4 upload state machine,
//! §3.5 download/hydration mechanics, §2.1.5 durability order), plus the
//! bounded-concurrency queue pump.
//!
//! Scope boundary: this module is the machinery **one item** goes through
//! — probe, upload, verify, journal-enqueue, download, verify, install —
//! and the pump that runs N of them concurrently. The SyncManager
//! supervisor loop, debounce/quiescence admission (§3.7), conflict
//! resolution (§2.6), and LRU eviction (§3.5) are later units.
//!
//! # Upload (§2.4, driven through the state-machine CAS)
//!
//! `queued → uploading`: under [`TransferConfig::multipart_threshold`]
//! bytes the object goes up as a **single buffered PUT with `Content-MD5`**
//! (signed payload), and the returned ETag must equal the body's hex MD5.
//! At or above the threshold, `CreateMultipartUpload` runs first and
//! `{upload_id, part_size}` is persisted ([`crate::state::SyncDb::set_upload`])
//! **before the first part is sent**; each part is read through the
//! [`ChunkSource`] seam with a **concurrent running blake3 over exactly
//! the bytes that are sent** plus a per-part MD5 (sent as `Content-MD5`,
//! the §2.4 integrity backbone), and `{part_no, etag, md5}` is persisted
//! **after** each part completes. `CompleteMultipartUpload` uses the
//! recorded list.
//!
//! **Resume**: on re-entry with a persisted `upload_id`, `ListParts` is
//! called opportunistically to reconcile, and only parts lacking a durable
//! record are re-uploaded. Because the journal `blake3` must hash exactly
//! the sent bytes, resume **re-hashes the already-sent byte ranges from
//! the source** — legal only if the file is unchanged (size + mtime
//! recheck). If the source changed mid-resume, the upload is **aborted on
//! the backend** and the item re-queued `dirty`
//! ([`TransferError::AbortedSourceChanged`]). Note the deliberate
//! distinction from §2.4's "complete the old version, then queue the new":
//! that rule applies when the change is detected **at completion** (the
//! already-streamed bytes are a complete, correctly-hashed old version);
//! a change detected **mid-resume** means the old version's bytes are no
//! longer obtainable for the un-sent parts, so the only honest outcome is
//! abort + restart.
//!
//! `uploading → verifying`: HEAD, size check; single-part ETag == our MD5;
//! multipart parts were server-verified at receipt. When the backend
//! **failed the setup probe** ([`BackendProfile::digest_rejection_works`]
//! `== false`, i.e. `requires_readback_verify`), verification additionally
//! performs a **full ranged-GET re-hash**. `verifying → synced` sets
//! `verified_remote` and stages the journal `put` entry **in the same
//! state transaction as the transition** (§2.1.5; see [`commit_verified`]).
//! A verify mismatch transitions to `corrupt_remote`, never `synced`, and
//! stages nothing.
//!
//! # Download (§3.5)
//!
//! Downloads stream to `<dir>/.rr.part-<name>` ([`partial_path`]). A
//! surviving partial is resumed: the existing bytes are **re-hashed from
//! disk** and the transfer continues with a ranged GET (`bytes=N-`) from
//! exactly the partial's length. The final blake3 must equal the expected
//! hash — a mismatch is a typed [`TransferError::IntegrityMismatch`], the
//! partial is deleted, and the item moves to the `corrupt_remote` lane. A
//! sidecar is additionally **parse-validated** (through the §2.5 semantic
//! parser) and is *never installed* when invalid. Install is an atomic
//! rename over the destination followed by an mtime restore (`filetime`)
//! to the provided remote mtime. The engine does **not** hard-depend on
//! conditional requests to detect a mid-download remote replacement: the
//! final-hash check is the backstop (a replaced object can never install).
//!
//! # Pump
//!
//! [`pump_uploads`]/[`pump_downloads`] pop the durable queues by priority
//! class and run up to `concurrency` items at once (`tokio::sync::Semaphore`
//! — §2.4 names 2 on Android, 4 on desktop; the number is a parameter
//! here). One item's failure is recorded in the summary — with its state
//! left resumable — and never stops the pump. A [`CancelFlag`] stops
//! *admission* deterministically and waits for in-flight items to finish.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use futures::stream::BoxStream;

use crate::journal::{JournalEntry, Kind};
use crate::keys::{library_key, sidecar_key, RelKey};
use crate::publisher::PublisherError;
use crate::s3::{S3Error, S3TransferApi};
use crate::semhash::{Blake3Hex, SemHashError};
use crate::state::{ItemRecord, StateError, SyncDb};

/// Default part size for multipart uploads: 16 MiB (§2.4; ≥ the 5 MiB
/// S3 minimum, a 60 MB RAW is 4 parts).
pub const DEFAULT_PART_SIZE: u64 = 16 * 1024 * 1024;

/// Default single-PUT/multipart threshold: 16 MiB (§2.4 "<16 MiB single
/// PUT with `Content-MD5` ... ≥16 MiB multipart").
pub const DEFAULT_MULTIPART_THRESHOLD: u64 = 16 * 1024 * 1024;

/// The smallest part size the engine will accept (the S3 minimum for
/// non-final parts). [`TransferConfig`]s below this are rejected at use.
pub const MIN_PART_SIZE: u64 = 5 * 1024 * 1024;

/// §2.4 stale-upload hygiene age: our own `upload_id`s older than this
/// are aborted (7 days, in seconds).
pub const DEFAULT_STALE_UPLOAD_MAX_AGE_SECS: i64 = 7 * 24 * 60 * 60;

/// Bucket key of the small throwaway object the §2.4 setup probe PUTs
/// with a deliberately wrong `Content-MD5`. Lives under the control
/// prefix so a FUSE-mounted library never sees it; deleted by the probe
/// on either outcome.
pub const PROBE_KEY: &str = ".rrcloud/v1/probe/digest-check";

/// Errors from the transfer engine. Every failure that leaves an item
/// mid-pipeline also leaves its durable state **resumable** (the §2.4
/// tables say which state each edge lands in); variants below note the
/// state they leave behind where it is load-bearing.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TransferError {
    /// An S3 operation failed (transport or API). Upload: the item is
    /// re-queued (`uploading → queued`) with its multipart record and
    /// part records intact. Download: the partial is kept and the item
    /// returns to `pending_down`.
    #[error("S3 operation failed: {0}")]
    S3(#[from] S3Error),

    /// The durable state store refused an operation (including the
    /// transition CAS losing to a concurrent writer).
    #[error("state store: {0}")]
    State(#[from] StateError),

    /// Staging the journal entry failed (the whole verify-commit
    /// transaction rolls back; the item stays `verifying`-resumable —
    /// in practice re-queued by the caller).
    #[error("journal staging: {0}")]
    Publisher(#[from] PublisherError),

    /// Local file I/O failed.
    #[error("I/O error on {path}: {source}")]
    Io {
        /// The path the operation was on.
        path: PathBuf,
        /// The underlying error.
        source: std::io::Error,
    },

    /// No item record exists for the relkey.
    #[error("no item record for {relkey}")]
    MissingItem {
        /// The item.
        relkey: RelKey,
    },

    /// The item's kind has no transfer mapping in this unit (previews,
    /// thumbs, and meta documents ride other lanes).
    #[error("kind {kind:?} is not transferable by this engine ({relkey})")]
    UnsupportedKind {
        /// The item.
        relkey: RelKey,
        /// Its kind.
        kind: Kind,
    },

    /// The server rejected a part's `Content-MD5` twice (one retry per
    /// §2.4). The upload record and all completed-part records are kept:
    /// the item is re-queued and the next attempt resumes.
    #[error("part {part_number} of {relkey} was digest-rejected twice")]
    DigestRejected {
        /// The item.
        relkey: RelKey,
        /// The 1-based part that kept failing.
        part_number: u32,
    },

    /// The source file changed (size/mtime) while a multipart upload was
    /// being resumed: the backend upload was aborted, the multipart
    /// record cleared, and the item re-queued `dirty` with **nothing
    /// journaled** (see the module docs for why mid-resume differs from
    /// §2.4's detected-at-completion rule).
    #[error("source for {relkey} changed mid-resume; upload aborted and item re-marked dirty")]
    AbortedSourceChanged {
        /// The item.
        relkey: RelKey,
    },

    /// A single PUT's returned ETag did not equal the body's hex MD5
    /// (upload verify, §2.4). The item is re-queued.
    #[error("ETag mismatch on {relkey}: expected {expected}, got {actual}")]
    EtagMismatch {
        /// The item.
        relkey: RelKey,
        /// Our hex MD5 of the sent bytes.
        expected: String,
        /// The server's ETag.
        actual: String,
    },

    /// Upload verification failed against the stored object (HEAD size
    /// mismatch, or the `requires_readback_verify` re-hash disagreed
    /// with the streamed hash): the item transitioned to
    /// `corrupt_remote` and **no journal entry was staged**.
    #[error("remote object for {relkey} is corrupt: {detail}")]
    CorruptRemote {
        /// The item.
        relkey: RelKey,
        /// What disagreed.
        detail: String,
    },

    /// A downloaded object's final blake3 did not equal the expected
    /// hash (§3.5): the partial was deleted, nothing was installed, and
    /// the item transitioned to `corrupt_remote`.
    #[error("download integrity mismatch for {relkey}: expected {expected}, got {actual}")]
    IntegrityMismatch {
        /// The item.
        relkey: RelKey,
        /// The expected content hash.
        expected: Blake3Hex,
        /// The hash of what actually arrived.
        actual: Blake3Hex,
    },

    /// A downloaded sidecar failed parse-validation (§3.5): it was
    /// **not** installed, the destination (including any pre-existing
    /// file) is untouched, the partial was deleted, and the item
    /// transitioned to `corrupt_remote`.
    #[error("downloaded sidecar for {relkey} failed parse-validation: {source}")]
    SidecarInvalid {
        /// The item.
        relkey: RelKey,
        /// The parse failure.
        source: SemHashError,
    },

    /// The [`TransferConfig`] is unusable (e.g. `part_size` below
    /// [`MIN_PART_SIZE`]).
    #[error("invalid transfer config: {0}")]
    InvalidConfig(String),
}

/// What the §2.4 setup probe learned about the backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackendProfile {
    /// `true`: the backend rejected a deliberately wrong `Content-MD5`
    /// (`BadDigest`/`InvalidDigest` — [`S3Error::is_digest_rejection`]),
    /// so per-part `Content-MD5` is a real integrity backbone. `false`:
    /// the backend accepted it, so every upload verification must
    /// perform a full ranged-GET re-hash (`requires_readback_verify`).
    pub digest_rejection_works: bool,
}

impl BackendProfile {
    /// §2.4 `requires_readback_verify`: the inverse of
    /// [`BackendProfile::digest_rejection_works`].
    pub fn requires_readback_verify(&self) -> bool {
        !self.digest_rejection_works
    }
}

/// Configuration for one transfer engine instance.
#[derive(Debug, Clone)]
pub struct TransferConfig {
    /// Bucket all transfers target.
    pub bucket: String,
    /// The local sync root ([`crate::keys::local_path`] base). The pump
    /// derives each item's source/destination path from it.
    pub sync_root: PathBuf,
    /// Multipart part size in bytes ([`DEFAULT_PART_SIZE`]; must be ≥
    /// [`MIN_PART_SIZE`]).
    pub part_size: u64,
    /// Objects of at least this many bytes go multipart
    /// ([`DEFAULT_MULTIPART_THRESHOLD`]).
    pub multipart_threshold: u64,
    /// The probed backend profile (persist/load via [`probe_backend`] /
    /// [`stored_backend_profile`]).
    pub backend: BackendProfile,
}

impl TransferConfig {
    /// A config with the §2.4 defaults (16 MiB parts and threshold).
    pub fn new(
        bucket: impl Into<String>,
        sync_root: impl Into<PathBuf>,
        backend: BackendProfile,
    ) -> Self {
        TransferConfig {
            bucket: bucket.into(),
            sync_root: sync_root.into(),
            part_size: DEFAULT_PART_SIZE,
            multipart_threshold: DEFAULT_MULTIPART_THRESHOLD,
            backend,
        }
    }
}

/// Runs the §2.4 setup probe: PUT a small object at [`PROBE_KEY`] with a
/// deliberately wrong `Content-MD5`.
///
/// - Digest rejection observed ([`S3Error::is_digest_rejection`]) →
///   `digest_rejection_works: true`.
/// - Accepted (2xx) → `digest_rejection_works: false`
///   (`requires_readback_verify`), and the accepted probe object is
///   deleted.
///
/// Any *other* failure (transport, auth, missing bucket) is an error —
/// never silently classified as either outcome. The outcome is persisted
/// in the state store's meta table
/// ([`crate::state::SyncDb::set_backend_digest_rejection`]) before
/// returning, and the probe object does not survive the call on either
/// path.
pub async fn probe_backend(
    db: &SyncDb,
    s3: &impl S3TransferApi,
    bucket: &str,
) -> Result<BackendProfile, TransferError> {
    let _ = (db, s3, bucket);
    todo!("P1-U4: §2.4 backend digest probe")
}

/// The persisted probe outcome, when one exists
/// ([`crate::state::SyncDb::backend_digest_rejection`]).
pub fn stored_backend_profile(db: &SyncDb) -> Result<Option<BackendProfile>, TransferError> {
    Ok(db
        .backend_digest_rejection()?
        .map(|digest_rejection_works| BackendProfile {
            digest_rejection_works,
        }))
}

/// A chunk stream over source-file bytes.
pub type SourceStream = BoxStream<'static, Result<Bytes, std::io::Error>>;

/// The seam between the upload engine and the bytes it sends — the §2.4
/// "hash of what was actually sent" requirement is *by construction*:
/// the engine computes its running blake3 and the per-part MD5s over the
/// chunks this source yields, and sends exactly those chunks. Production
/// code uses [`FsChunkSource`]; tests interpose wrappers that yield bytes
/// differing from the file to pin that the journal records the sent
/// bytes, not the file.
#[allow(async_fn_in_trait)] // engine-internal seam; no dyn dispatch, same contract as S3Api
pub trait ChunkSource {
    /// Opens `path` positioned at `offset` bytes, yielding the remainder
    /// as a chunk stream. Resume re-hashing opens at 0 and reads the
    /// already-sent ranges through the same seam.
    async fn open(&self, path: &Path, offset: u64) -> Result<SourceStream, std::io::Error>;
}

/// The production [`ChunkSource`]: reads the file at `path` from
/// `offset` via `tokio::fs`.
#[derive(Debug, Clone, Copy, Default)]
pub struct FsChunkSource;

impl ChunkSource for FsChunkSource {
    async fn open(&self, path: &Path, offset: u64) -> Result<SourceStream, std::io::Error> {
        let _ = (path, offset);
        todo!("P1-U4: chunked file reader")
    }
}

/// What a successful [`upload_item`] did.
#[derive(Debug, Clone)]
pub struct UploadOutcome {
    /// The item.
    pub relkey: RelKey,
    /// blake3 of exactly the bytes that were sent (== the staged journal
    /// entry's `blake3`).
    pub blake3: Blake3Hex,
    /// Total bytes sent.
    pub size: u64,
    /// The stored object's ETag (single PUT: hex MD5; multipart:
    /// `<md5-of-md5s>-<n>`).
    pub e_tag: String,
    /// Whether the upload went multipart.
    pub multipart: bool,
    /// Staging id of the journal `put` entry committed with the
    /// `verifying → synced` transition.
    pub outbound_id: u64,
}

/// Uploads one item per §2.4, reading the source file through
/// [`FsChunkSource`]. See [`upload_item_from`] for the full contract.
pub async fn upload_item(
    db: &SyncDb,
    s3: &impl S3TransferApi,
    cfg: &TransferConfig,
    relkey: &RelKey,
    source: &Path,
) -> Result<UploadOutcome, TransferError> {
    upload_item_from(db, s3, cfg, relkey, source, &FsChunkSource).await
}

/// Uploads one item per §2.4 (see the module docs for the full state
/// walk), reading bytes through `chunks`.
///
/// Entry states: `queued` (fresh admission; CAS `queued → uploading`) or
/// `uploading` with a persisted multipart record (crash-recovery
/// re-entry; resumed in place). Anything else is a typed
/// [`StateError::StaleState`] via [`TransferError::State`].
///
/// On success the item is `synced` with `verified_remote` set, its
/// multipart bookkeeping is cleared, and the journal `put` entry is
/// staged (same transaction as the final transition — [`commit_verified`]).
/// The entry carries `blake3`/`size` of the sent bytes and, per kind:
/// `sem_hash` for sidecars (parsed from the sent bytes), `content_id` +
/// `mtime` for originals (`content_id` == the sent bytes' blake3, §1.2).
pub async fn upload_item_from(
    db: &SyncDb,
    s3: &impl S3TransferApi,
    cfg: &TransferConfig,
    relkey: &RelKey,
    source: &Path,
    chunks: &impl ChunkSource,
) -> Result<UploadOutcome, TransferError> {
    let _ = (db, s3, cfg, relkey, source, chunks);
    todo!("P1-U4: §2.4 upload state walk")
}

/// The atomic tail of the §2.4 upload: in **one** committed state
/// transaction, CAS the item `verifying → synced` (setting
/// `verified_remote`, then applying `mutate` — e.g. recording the sent
/// `blake3`/`size` on the record), build the journal entry from the
/// about-to-be-committed record, and stage it via
/// [`crate::publisher::enqueue_entry_in`]. Returns the staging id.
///
/// Atomicity contract (§2.1.5 "verify → journal entry → local commit",
/// pinned by the panic/error-injection tests): if `build_entry` returns
/// `Err` **or panics**, or staging fails (foreign device, oversized
/// entry), the transaction aborts and *neither* the transition nor the
/// staged entry survives — a crash between verify and journal-enqueue
/// can never produce a `synced` item whose entry was lost.
pub fn commit_verified(
    db: &SyncDb,
    relkey: &RelKey,
    mutate: impl FnOnce(&mut ItemRecord),
    build_entry: impl FnOnce(&ItemRecord) -> Result<JournalEntry, TransferError>,
) -> Result<u64, TransferError> {
    let _ = (db, relkey, mutate, build_entry);
    todo!("P1-U4: single-txn verifying→synced transition + journal staging")
}

/// What [`abort_stale_uploads`] cleaned up.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StaleUploadReport {
    /// Our own multipart records older than `max_age` that were aborted
    /// on the backend and cleared locally: `(relkey, upload_id)`.
    pub aborted_own: Vec<(RelKey, String)>,
    /// Backend-listed uploads targeting a key **we hold a multipart
    /// record for** but under a *different* `upload_id` (orphans of a
    /// restart that re-created the upload): `(bucket_key, upload_id)`.
    pub aborted_orphans: Vec<(String, String)>,
}

/// §2.4 stale-upload hygiene.
///
/// 1. Every `uploads`-table record with `started_unix + max_age_secs <=
///    now_unix` is aborted on the backend (`AbortMultipartUpload`), its
///    local record and part records cleared, and — when the item sits in
///    `uploading` — the item re-marked `dirty` (`uploading → dirty`, the
///    §2.4 "upload abandoned" edge). A record still fresh is untouched.
/// 2. `ListMultipartUploads` (paged) is scanned for **own-key orphans**:
///    uploads whose key corresponds to one of our `uploads`-table
///    relkeys but whose `upload_id` differs from the recorded one. These
///    are aborted regardless of age (nothing can ever complete them).
///    Uploads on keys we hold no record for are **left alone** — another
///    device may legitimately be mid-upload (§1.2 multi-device bucket);
///    the cross-device sweep is the worker's job (later unit).
pub async fn abort_stale_uploads(
    db: &SyncDb,
    s3: &impl S3TransferApi,
    cfg: &TransferConfig,
    max_age_secs: i64,
    now_unix: i64,
) -> Result<StaleUploadReport, TransferError> {
    let _ = (db, s3, cfg, max_age_secs, now_unix);
    todo!("P1-U4: §2.4 stale-upload hygiene")
}

/// What the caller knows the remote object must contain (from the
/// journal/manifest head) — the download's acceptance criteria.
#[derive(Debug, Clone)]
pub struct ExpectedDownload {
    /// Required blake3 of the full object bytes. The backstop for every
    /// failure mode including mid-download remote replacement.
    pub blake3: Blake3Hex,
    /// Required object size in bytes.
    pub size: u64,
    /// Remote mtime (unix seconds) restored onto the installed file
    /// (§3.5 — keeps the thumbnail cache hash stable across hydration).
    pub mtime_unix: i64,
}

/// What a successful [`download_item`] did.
#[derive(Debug, Clone)]
pub struct DownloadOutcome {
    /// The item.
    pub relkey: RelKey,
    /// Where the verified file was installed.
    pub path: PathBuf,
    /// Byte offset the transfer resumed from (0 for a fresh download).
    pub resumed_from: u64,
    /// Bytes fetched over the network by this call.
    pub bytes_fetched: u64,
}

/// The local file a download of `relkey`/`kind` installs under
/// `dest_root`: [`crate::keys::local_path`] for originals and `.xmp`
/// projections, plus the `.rrdata` suffix for the primary sidecar
/// (mirroring [`crate::keys::sidecar_key`]).
pub fn local_target_path(dest_root: &Path, relkey: &RelKey, kind: Kind) -> PathBuf {
    let base = crate::keys::local_path(relkey, dest_root);
    match kind {
        Kind::Sidecar => {
            let mut s = base.into_os_string();
            s.push(".rrdata");
            PathBuf::from(s)
        }
        _ => base,
    }
}

/// The §3.5 temp file a download streams into: `.rr.part-<name>` next to
/// the final path (same directory, so the final rename is atomic).
pub fn partial_path(final_path: &Path) -> PathBuf {
    let name = final_path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    final_path.with_file_name(format!(".rr.part-{name}"))
}

/// Downloads one item per §3.5 (see the module docs): temp file, ranged
/// resume with partial re-hash, blake3 verify against `expected`,
/// sidecar parse-validation, atomic rename, mtime restore.
///
/// Entry states: `pending_down` or `stub` (CAS to `downloading`), or
/// `downloading` (crash-recovery re-entry with a surviving partial).
/// Terminal states: originals land `hydrated`, everything else `synced`;
/// integrity/parse failures land `corrupt_remote` (partial deleted,
/// destination untouched). A transport failure keeps the partial and
/// returns the item to `pending_down` (resumable).
pub async fn download_item(
    db: &SyncDb,
    s3: &impl S3TransferApi,
    cfg: &TransferConfig,
    relkey: &RelKey,
    dest_root: &Path,
    expected: &ExpectedDownload,
) -> Result<DownloadOutcome, TransferError> {
    let _ = (db, s3, cfg, relkey, dest_root, expected);
    todo!("P1-U4: §3.5 download/hydration walk")
}

/// Cooperative cancellation for the pump, with no new dependency (an
/// `Arc<AtomicBool>`; `tokio_util`'s CancellationToken deliberately not
/// pulled in). Cloning shares the flag.
#[derive(Debug, Clone, Default)]
pub struct CancelFlag(Arc<AtomicBool>);

impl CancelFlag {
    /// A fresh, uncancelled flag.
    pub fn new() -> Self {
        Self::default()
    }

    /// Requests cancellation: the pump stops admitting new items (items
    /// already running finish; items popped but not yet started are
    /// re-queued).
    pub fn cancel(&self) {
        self.0.store(true, Ordering::SeqCst);
    }

    /// Whether cancellation was requested.
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}

/// What one pump pass did.
#[derive(Debug, Default)]
pub struct PumpSummary {
    /// Items that completed their transfer, in completion order.
    pub completed: Vec<RelKey>,
    /// Items whose transfer failed, with the failure rendered — each was
    /// left in a resumable state (and re-queued where the state machine
    /// re-queues), and **did not stop the pump**.
    pub failed: Vec<(RelKey, String)>,
    /// `true` when the pass stopped early because the [`CancelFlag`]
    /// fired (queued items remain queued for a later pass).
    pub cancelled: bool,
}

/// Drains the upload queue: pops by priority class (lowest class first,
/// FIFO within a class — [`crate::state::SyncDb::queue_pop`] order), runs
/// up to `concurrency` [`upload_item`]s at once, sourcing each item's
/// bytes from its path under `cfg.sync_root`.
///
/// Contract (pinned by tests): admission strictly follows queue order; at
/// most `concurrency` transfers are in flight at any moment; a failing
/// item is recorded in the summary (state resumable, re-queued per the
/// state machine) without stopping the others; one pass pops each queued
/// item at most once (a failure re-queues for a *later* pass, never a
/// retry loop within this one); `cancel` stops admission — in-flight
/// items are awaited, never abandoned mid-transition, and nothing new
/// starts after the flag is observed.
pub async fn pump_uploads(
    db: &SyncDb,
    s3: &impl S3TransferApi,
    cfg: &TransferConfig,
    concurrency: usize,
    cancel: &CancelFlag,
) -> Result<PumpSummary, TransferError> {
    let _ = (db, s3, cfg, concurrency, cancel);
    todo!("P1-U4: upload queue pump")
}

/// Drains the download queue with the same admission/concurrency/
/// failure-isolation/cancel contract as [`pump_uploads`]; each item's
/// [`ExpectedDownload`] comes from its own record (`blake3`, `size`,
/// `mtime_unix_ns / 1e9`) and its destination from `cfg.sync_root`. An
/// item whose record lacks a `blake3` cannot be verified and is recorded
/// as failed (never installed unverified).
pub async fn pump_downloads(
    db: &SyncDb,
    s3: &impl S3TransferApi,
    cfg: &TransferConfig,
    concurrency: usize,
    cancel: &CancelFlag,
) -> Result<PumpSummary, TransferError> {
    let _ = (db, s3, cfg, concurrency, cancel);
    todo!("P1-U4: download queue pump")
}

/// The bucket key an item of `kind` at `relkey` transfers to/from:
/// [`library_key`] for originals and `.xmp` projections (both are plain
/// library files), [`sidecar_key`] for the primary sidecar. Other kinds
/// ride other lanes ([`TransferError::UnsupportedKind`]).
pub fn bucket_key_for(relkey: &RelKey, kind: Kind) -> Result<String, TransferError> {
    match kind {
        Kind::Original | Kind::Xmp => Ok(library_key(relkey)),
        Kind::Sidecar => Ok(sidecar_key(relkey)),
        other => Err(TransferError::UnsupportedKind {
            relkey: relkey.clone(),
            kind: other,
        }),
    }
}
