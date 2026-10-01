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
//! the source** — legal only if the file is unchanged. Three guards pin
//! that, in order of strength: (1) size + mtime are compared against the
//! facts **captured when the upload was created**
//! ([`crate::state::MultipartUploadState::size`]/`mtime_unix_ns` — not the
//! item record's, which per the §2.6 coordination note keep naming the
//! last *published* version); (2) every already-sent range that is
//! re-read is **MD5-compared against the part's persisted `md5_b64`**, so
//! a rewrite that preserves size and mtime (coarse-mtime filesystems,
//! mtime-restoring tools) still cannot smuggle a journal hash that
//! matches no stored object; (3) the §2.4 completion recheck below. If
//! the source changed mid-resume, the upload is **aborted on the
//! backend** and the item re-marked `dirty`
//! ([`TransferError::AbortedSourceChanged`]). Note the deliberate
//! distinction from §2.4's "complete the old version, then queue the new":
//! that rule applies when the change is detected **at completion** (the
//! already-streamed bytes are a complete, correctly-hashed old version);
//! a change detected **mid-resume** means the old version's bytes are no
//! longer obtainable for the un-sent parts, so the only honest outcome is
//! abort + restart.
//!
//! **Completion recheck (§2.4)**: after the verify-commit lands, the
//! source's size/mtime are re-checked against what the transfer captured
//! at start. A change means the file was rewritten *mid-upload*: the old
//! version was completed, verified and journaled honestly (streamed-hash
//! truth), and the item is immediately re-marked `dirty`
//! ([`UploadOutcome::source_changed_at_completion`]) so the new version
//! uploads next — §2.4's "complete the old version, journal it, queue the
//! new". The dirty→queued admission itself (with its §3.7 vv bump) is the
//! SyncManager's, a later unit.
//!
//! **Upload gone (`NoSuchUpload`)**: a persisted upload id the backend no
//! longer knows (swept elsewhere, or completed just before a crash) never
//! wedges the item. `Complete` failing `NoSuchUpload` on a resume that
//! re-uploaded nothing is the crash-after-complete signature: the stored
//! object is HEAD-checked and adopted **only when it is provably our
//! completed upload** — `content_length` must equal the re-read source
//! size *and* the stored ETag must equal the multipart ETag derivable
//! from the recorded part MD5s (`hex(md5(concat(part md5 bytes)))-<n>`,
//! the S3 multipart ETag convention the §8 harness validates on
//! Garage/MinIO). Size alone is not proof: relkeys map to *shared*
//! bucket keys (§1.2), so after our id was swept a sibling device may
//! have stored a same-size different-content version at the key —
//! adopting it would journal a blake3 no stored object has and poison
//! every downloader (§2.1.5). On any mismatch the record is cleared and
//! the next pass restarts cleanly, like every other `NoSuchUpload`
//! (mid-part, mismatched HEAD, abort of an already-gone upload).
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
//! exactly the partial's length. A resume requires the response to
//! actually be partial (`Content-Range` starting at the resume offset); a
//! backend or intermediary that ignored the `Range` header and returned
//! the full object makes the engine **discard the partial and take the
//! full body from offset 0** instead of splicing garbage. The final
//! blake3 must equal the expected hash — but a mismatch after a *resumed*
//! attempt first **discards the partial and retries once from scratch**,
//! because a surviving partial may belong to a superseded object version
//! (journal head advanced between attempts) and must not condemn an
//! intact remote. Only a from-scratch mismatch is the typed
//! [`TransferError::IntegrityMismatch`]: the partial is deleted and the
//! item moves to the `corrupt_remote` lane. A sidecar is additionally
//! **parse-validated** (through the §2.5 semantic parser) and is *never
//! installed* when invalid. Install is an atomic rename over the
//! destination followed by an mtime restore (`filetime`) to the provided
//! remote mtime. The engine does **not** hard-depend on conditional
//! requests to detect a mid-download remote replacement: the final-hash
//! check is the backstop (a replaced object can never install). File
//! *data* I/O on this path goes through `tokio::fs` (reads/writes land on
//! tokio's blocking pool in bounded chunks), so one item's bulk disk work
//! never stalls the pump's other in-flight transfers. Two classes of
//! small, bounded calls remain synchronous on the caller's task and are a
//! deliberate trade-off, not an oversight: the post-install mtime restore
//! (one `utimensat`-class syscall via `filetime`) and every state-machine
//! commit (a synchronous redb fsync — one per part / transition). Both
//! are micro-scale next to a multi-MiB part transfer; batching or
//! off-loading them would need a tokio runtime feature this crate
//! deliberately does not depend on.
//!
//! # Pump
//!
//! [`pump_uploads`]/[`pump_downloads`] pop the durable queues by priority
//! class and run up to `concurrency` items at once (§2.4 names 2 on
//! Android, 4 on desktop; the number is a parameter here). The cap is
//! structural: the pump admits into a [`FuturesUnordered`] on the
//! caller's task and never lets its length exceed `concurrency`, so no
//! semaphore (and no task spawning) is needed. One item's failure is
//! recorded in the summary — with its state left resumable — and never
//! stops the pump. A [`CancelFlag`] stops *admission* deterministically
//! and waits for in-flight items to finish.
//!
//! # Crash recovery ([`recover_interrupted`]) and single-driver entry
//!
//! A crash can strand an item in a pipeline-interior state whose durable
//! queue row is already gone: `uploading` (with or without a multipart
//! record), `verifying` (object stored, nothing journaled), or
//! `downloading` (queue row popped before the crash).
//! [`recover_interrupted`] is the startup sweep that re-drives them: it
//! demotes each stranded item along its legal §2.4 edge back to its
//! queueable state (`uploading`/`verifying` → `queued`, `downloading` →
//! `pending_down` — a surviving multipart record or `.rr.part` partial
//! is what makes the next attempt a *resume*, not the state) and
//! re-pushes it onto its queue. The supervisor (§3.3, a later unit) must
//! run it once before its first pump pass; it assumes the
//! single-supervisor exclusion (§5.1).
//!
//! Because recovery normalizes every stranded state, the engine entry
//! points accept **only** the queueable states and admit via transition
//! CAS: [`upload_item`] requires `queued`, [`download_item`] requires
//! `pending_down` or `stub`. A record already sitting at
//! `uploading`/`downloading` therefore means exactly one thing — a
//! *live* concurrent transfer owns the item right now — and the second
//! caller gets a typed [`StateError::StaleState`] instead of silently
//! double-driving the same multipart bookkeeping or `.rr.part` partial
//! (which could install a file the live writer keeps appending to).
//! Callers that can legitimately race on one item (the §3.5
//! `ensure_local` guard sites vs. the background pump) must treat that
//! error as "already in flight" — wait for the live transfer or
//! single-flight per relkey — never as a retry-now signal.

use std::collections::BTreeMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use base64::Engine as _;
use bytes::Bytes;
use futures::stream::{BoxStream, FuturesUnordered};
use futures::{StreamExt as _, TryStreamExt as _};
use md5::{Digest as _, Md5};

use crate::journal::{JournalEntry, Kind, Op, JOURNAL_VERSION};
use crate::keys::{library_key, sidecar_key, RelKey};
use crate::publisher::{enqueue_entry_in, PublisherError};
use crate::s3::{
    ByteRange, CompletedPart, ListMultipartUploadsRequest, ListPartsRequest, PartBody,
    PutObjectOptions, S3Error, S3ErrorCode, S3TransferApi,
};
use crate::semhash::{sem_hash, sidecar_badges, Blake3Hex, ContentId, SemHash, SemHashError};
use crate::state::{
    ItemRecord, ItemState, MultipartUploadState, Queue, StateError, SyncDb, UploadPart,
};

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

    /// The source file changed while a multipart upload was being resumed
    /// — caught by the size/mtime recheck against the facts captured at
    /// upload creation, **or** by an already-sent range re-reading with an
    /// MD5 that no longer matches the part's persisted `md5_b64` (the
    /// same-size/same-mtime rewrite case). The backend upload was aborted,
    /// the multipart record cleared, and the item re-marked `dirty` with
    /// **nothing journaled** (see the module docs for why mid-resume
    /// differs from §2.4's detected-at-completion rule).
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

    /// A sidecar failed parse-validation (§2.5 semantic parser). Two
    /// sites, two contracts:
    ///
    /// - **Download** (§3.5): the invalid object was **not** installed,
    ///   the destination (including any pre-existing file) is untouched,
    ///   the partial was deleted, and the item transitioned to
    ///   `corrupt_remote`.
    /// - **Upload**: the sidecar is validated **before** it is stored —
    ///   below the multipart threshold nothing is PUT at all; at or above
    ///   it the already-created multipart upload is aborted before
    ///   `Complete`, so no unjournaled garbage object lands in the
    ///   bucket. The item is parked `dirty` (not re-queued — retrying an
    ///   unchanged invalid sidecar can never succeed; the next local
    ///   change re-admits it) with nothing journaled.
    #[error("sidecar for {relkey} failed parse-validation: {source}")]
    SidecarInvalid {
        /// The item.
        relkey: RelKey,
        /// The parse failure.
        source: SemHashError,
    },

    /// A pump-admitted download whose item record carries no `blake3`:
    /// nothing could ever verify the fetched bytes, so the engine refuses
    /// to install them (§3.5 — the hash is the backstop for *every*
    /// failure mode, so an unverifiable download is never attempted).
    /// Because the engine can never supply the missing hash itself, the
    /// pump **parks** such an item off the queue (state left
    /// `pending_down`, row not re-pushed) — see [`pump_downloads`].
    #[error("item record for {relkey} has no blake3; refusing an unverifiable download")]
    MissingExpectedHash {
        /// The item.
        relkey: RelKey,
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
    const PROBE_BODY: &[u8] = b"rrcloud digest probe";
    // A well-formed base64 MD5 of *different* bytes: syntactically valid,
    // so a verifying backend must answer a digest rejection, not a
    // malformed-header error.
    let wrong_md5 = b64(Md5::digest(b"deliberately not the probe body"));
    let opts = PutObjectOptions {
        content_md5: Some(wrong_md5),
        ..PutObjectOptions::default()
    };
    let digest_rejection_works = match s3
        .put_object(bucket, PROBE_KEY, Bytes::from_static(PROBE_BODY), &opts)
        .await
    {
        Err(e) if e.is_digest_rejection() => true,
        Ok(_) => {
            // The backend stored an object whose digest never verified:
            // clean it up before reporting `requires_readback_verify`.
            s3.delete_object(bucket, PROBE_KEY).await?;
            false
        }
        // Transport/auth/bucket failures are errors, never a probe outcome.
        Err(e) => return Err(e.into()),
    };
    db.set_backend_digest_rejection(digest_rejection_works)?;
    Ok(BackendProfile {
        digest_rejection_works,
    })
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
        use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _};
        let mut file = tokio::fs::File::open(path).await?;
        if offset > 0 {
            file.seek(std::io::SeekFrom::Start(offset)).await?;
        }
        let stream = futures::stream::unfold(Some(file), |file| async move {
            let mut file = file?;
            let mut buf = vec![0u8; 64 * 1024];
            match file.read(&mut buf).await {
                Ok(0) => None,
                Ok(n) => {
                    buf.truncate(n);
                    Some((Ok(Bytes::from(buf)), Some(file)))
                }
                Err(e) => Some((Err(e), None)),
            }
        });
        Ok(stream.boxed())
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
    /// The §2.4 completion recheck found the source rewritten while the
    /// upload was in flight: the *old* version was completed, verified
    /// and journaled honestly, and the item was re-marked `dirty` so the
    /// new version uploads next (module docs, "Completion recheck").
    pub source_changed_at_completion: bool,
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
/// Entry state: `queued`, admitted via the `queued → uploading`
/// transition CAS — the single-driver gate (module docs, "Crash recovery
/// and single-driver entry"). A crash-stranded `uploading` item is
/// re-admitted by [`recover_interrupted`] demoting it back to `queued`
/// first (its surviving multipart record is what makes the next pass a
/// resume); a record *still* at `uploading` here means a live concurrent
/// transfer owns the item, and this caller gets the CAS's typed
/// [`StateError::StaleState`] via [`TransferError::State`] without
/// touching the shared bookkeeping.
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
    if cfg.part_size < MIN_PART_SIZE {
        return Err(TransferError::InvalidConfig(format!(
            "part_size {} is below the {MIN_PART_SIZE}-byte S3 minimum for non-final parts",
            cfg.part_size
        )));
    }
    let record = db
        .get_item(relkey)?
        .ok_or_else(|| TransferError::MissingItem {
            relkey: relkey.clone(),
        })?;
    let key = bucket_key_for(relkey, record.kind)?;
    let resume = db.get_upload(relkey)?;

    // Entry: the `queued → uploading` CAS is the single-driver gate.
    // `uploading` is NOT accepted here: recover_interrupted demotes every
    // crash-stranded `uploading` item back to `queued` (keeping its
    // multipart record for the resume), so a record still at `uploading`
    // means a live concurrent transfer owns this item — the CAS fails
    // typed (StaleState) before any shared bookkeeping is touched.
    db.transition(relkey, ItemState::Queued, ItemState::Uploading, |_| {})?;

    // ---- uploading: send the bytes (single PUT or multipart) ----
    let sent = match transfer_object(db, s3, cfg, relkey, &record, &key, source, chunks, resume)
        .await
    {
        Ok(sent) => sent,
        // `AbortedSourceChanged` and the upload-side `SidecarInvalid`
        // already parked the item dirty; every other failure re-queues
        // (`uploading → queued`) with the multipart bookkeeping
        // intact, so the next attempt resumes.
        Err(
            e @ (TransferError::AbortedSourceChanged { .. } | TransferError::SidecarInvalid { .. }),
        ) => return Err(e),
        Err(e) => {
            // The transfer failure stays primary (demote doc).
            let _ = demote(db, relkey, ItemState::Uploading, ItemState::Queued);
            return Err(e);
        }
    };

    // ---- uploading → verifying; the completed upload's bookkeeping is
    // cleared in the same transaction (nothing on the backend can resume
    // it any more) ----
    db.with_txn(|t| {
        t.transition(relkey, ItemState::Uploading, ItemState::Verifying, |_| {})?;
        t.clear_upload(relkey)?;
        Ok(())
    })?;

    // ---- verifying: HEAD (+ read-back re-hash when the backend failed
    // the digest probe) ----
    if let Err(e) = verify_remote(s3, cfg, relkey, &key, &sent).await {
        return Err(match e {
            corrupt @ TransferError::CorruptRemote { .. } => {
                // The verify outcome stays primary (demote doc).
                let _ = demote(db, relkey, ItemState::Verifying, ItemState::CorruptRemote);
                corrupt
            }
            other => {
                let _ = demote(db, relkey, ItemState::Verifying, ItemState::Queued);
                other
            }
        });
    }

    // ---- verifying → synced + journal staging, one transaction ----
    let ts = server_ts_estimate(db)?;
    let (sem, badges) = match sent.sidecar {
        Some((sem, badges)) => (Some(sem), badges),
        None => (None, crate::semhash::SidecarBadges::default()),
    };
    let kind = record.kind;
    let blake3 = sent.blake3.clone();
    let size = sent.size;
    let content_id = (kind == Kind::Original).then(|| ContentId::from_blake3(&blake3));
    let mtime = (kind == Kind::Original).then_some(sent.mtime_unix);
    let device = db.device_id().clone();
    let outbound_id = {
        let mutate_blake3 = blake3.clone();
        let mutate_sem = sem.clone();
        let mutate_cid = content_id.clone();
        let entry_blake3 = blake3.clone();
        let entry_key = key.clone();
        match commit_verified(
            db,
            relkey,
            move |r| {
                r.blake3 = Some(mutate_blake3);
                r.size = size;
                if mutate_sem.is_some() {
                    r.sem_hash = mutate_sem;
                }
                if mutate_cid.is_some() {
                    r.content_id = mutate_cid;
                }
            },
            move |r| {
                Ok(JournalEntry {
                    v: JOURNAL_VERSION,
                    seq: 0,
                    ts,
                    device,
                    op: Op::Put,
                    kind,
                    key: entry_key,
                    vv: r.vv.clone(),
                    size: Some(size),
                    blake3: Some(entry_blake3),
                    sem_hash: sem,
                    // §2.2: sidecar entries carry the badge fields so the
                    // grid can render before the sidecar bytes download
                    // (§3.5) — extracted from exactly the sent bytes.
                    rating: badges.rating,
                    color_label: badges.color_label,
                    content_id,
                    // Measured dimensions travel on original entries when
                    // the import unit has recorded them (§2.2).
                    w: (kind == Kind::Original).then_some(r.w).flatten(),
                    h: (kind == Kind::Original).then_some(r.h).flatten(),
                    mtime,
                    from_key: None,
                })
            },
        ) {
            Ok(id) => id,
            Err(e) => {
                // The whole commit rolled back: the item is still
                // `verifying`; re-queue it so the next pass retries. The
                // commit failure stays primary (demote doc).
                let _ = demote(db, relkey, ItemState::Verifying, ItemState::Queued);
                return Err(e);
            }
        }
    };

    // ---- §2.4 completion recheck (module docs): a source rewritten
    // mid-upload means the *old* version was just journaled honestly and
    // the new one is un-backed-up — re-mark dirty so it uploads next. A
    // stat failure reads as "cannot prove unchanged" and also re-marks
    // (the upload path will surface a real I/O problem loudly). Best
    // effort CAS: a concurrent writer that already moved the item off
    // `synced` owns its state now.
    let source_changed_at_completion = match tokio::fs::metadata(source).await {
        Ok(meta) => meta.len() != sent.size || file_mtime_unix_ns(&meta) != sent.mtime_unix_ns,
        Err(_) => true,
    };
    if source_changed_at_completion {
        // No primary error exists on this success path, so a real state
        // store failure here (anything but the tolerated CAS loss) must
        // surface rather than silently leave the rewritten item `synced`.
        demote(db, relkey, ItemState::Synced, ItemState::Dirty)?;
    }

    Ok(UploadOutcome {
        relkey: relkey.clone(),
        blake3,
        size,
        e_tag: sent.e_tag,
        multipart: sent.multipart,
        outbound_id,
        source_changed_at_completion,
    })
}

/// What the `uploading` phase produced (the §2.4 facts the verify and
/// commit phases run on).
struct SentObject {
    /// Running blake3 over exactly the bytes that were sent.
    blake3: Blake3Hex,
    /// Total bytes sent.
    size: u64,
    /// The stored object's ETag.
    e_tag: String,
    /// Whether the object went multipart.
    multipart: bool,
    /// Hex MD5 of a single-PUT body (`verifying` compares it to the HEAD
    /// ETag on a digest-verifying backend); `None` for multipart.
    md5_hex: Option<String>,
    /// Sidecars only: the journal `sem_hash` and §2.2 badge fields,
    /// parse-validated from exactly the sent bytes **before** the object
    /// was stored (see [`TransferError::SidecarInvalid`]).
    sidecar: Option<(SemHash, crate::semhash::SidecarBadges)>,
    /// Source mtime (unix seconds) captured when the transfer began.
    mtime_unix: i64,
    /// Source mtime (unix nanoseconds) captured when the transfer began —
    /// the §2.4 completion-recheck baseline.
    mtime_unix_ns: i64,
}

/// The `uploading` phase: single buffered PUT below the threshold,
/// multipart (fresh or resumed) otherwise. State edges are the caller's
/// job except the §2.4 mid-resume source-change abort, which re-marks the
/// item `dirty` itself (its terminal state differs from every other
/// failure).
#[allow(clippy::too_many_arguments)] // internal seam of one state walk
async fn transfer_object(
    db: &SyncDb,
    s3: &impl S3TransferApi,
    cfg: &TransferConfig,
    relkey: &RelKey,
    record: &ItemRecord,
    key: &str,
    source: &Path,
    chunks: &impl ChunkSource,
    resume: Option<MultipartUploadState>,
) -> Result<SentObject, TransferError> {
    let meta = tokio::fs::metadata(source)
        .await
        .map_err(|e| io_err(source, e))?;
    let size = meta.len();
    let mtime_unix_ns = file_mtime_unix_ns(&meta);
    let mtime_unix = mtime_unix_ns.div_euclid(1_000_000_000);
    let keep_bytes = record.kind == Kind::Sidecar;

    if resume.is_none() && size < cfg.multipart_threshold {
        // --- single buffered PUT with Content-MD5 (signed payload) ---
        let body = read_range(chunks, source, 0, size).await?;
        // A sidecar is parse-validated BEFORE anything is stored: an
        // invalid one is never uploaded at all (see the variant doc).
        let sidecar = if keep_bytes {
            match parse_sidecar(&body) {
                Ok(sidecar) => Some(sidecar),
                Err(source) => return Err(park_sidecar_invalid(db, relkey, source)?),
            }
        } else {
            None
        };
        let digest = Md5::digest(&body);
        let md5_hex = hex::encode(digest);
        let blake3 = Blake3Hex::from_bytes(&body);
        let opts = PutObjectOptions {
            content_md5: Some(b64(digest)),
            ..PutObjectOptions::default()
        };
        let bytes = Bytes::from(body);
        let out = s3
            .put_object(&cfg.bucket, key, bytes.clone(), &opts)
            .await?;
        // ETag == md5hex (§2.4) — meaningful only on a backend whose ETag
        // convention the digest probe validated; a non-verifying backend
        // is caught by the read-back re-hash instead.
        if cfg.backend.digest_rejection_works && out.e_tag != md5_hex {
            return Err(TransferError::EtagMismatch {
                relkey: relkey.clone(),
                expected: md5_hex,
                actual: out.e_tag,
            });
        }
        return Ok(SentObject {
            blake3,
            size,
            e_tag: out.e_tag,
            multipart: false,
            md5_hex: Some(md5_hex),
            sidecar,
            mtime_unix,
            mtime_unix_ns,
        });
    }

    // --- multipart ---
    let resuming = resume.is_some();
    let upload = match resume {
        Some(up) => {
            // §2.4 mid-resume source-change recheck: size + mtime against
            // the facts captured when the upload was CREATED (never the
            // item record's `size`/`mtime_unix_ns`, which per the §2.6
            // coordination note keep naming the last *published* version
            // while a newer one is in flight). A change here means the
            // un-sent parts of the *old* version are no longer obtainable:
            // abort + re-mark dirty (see the module docs for why this
            // differs from the detected-at-completion rule).
            if size != up.size || mtime_unix_ns != up.mtime_unix_ns {
                return Err(abort_source_changed(db, s3, cfg, relkey, key, &up.upload_id).await?);
            }
            up
        }
        None => {
            let created = s3
                .create_multipart_upload(&cfg.bucket, key, &PutObjectOptions::default())
                .await?;
            let up = MultipartUploadState {
                upload_id: created.upload_id,
                part_size: cfg.part_size,
                started_unix: now_unix(),
                // The resume source-change baseline (see above).
                size,
                mtime_unix_ns,
            };
            // Persisted BEFORE the first part (§2.4): whatever survives a
            // crash from here on is resumable.
            db.set_upload(relkey, &up)?;
            up
        }
    };

    let part_size = upload.part_size.max(1);
    let part_count = size.div_ceil(part_size).max(1);
    if part_count > 10_000 {
        return Err(TransferError::InvalidConfig(format!(
            "{size}-byte object needs {part_count} parts of {part_size}; S3 allows at most 10000"
        )));
    }
    let recorded: BTreeMap<u32, UploadPart> = db.upload_parts(relkey)?.into_iter().collect();
    // ListParts, opportunistically (resume only): a part is trusted as
    // done only when its durable record exists AND the backend lists it
    // with the recorded ETag. A failed listing falls back to the records
    // (a NoSuchUpload here is NOT proof the upload is gone — transport
    // failures land here too; the Complete/part paths below handle a
    // genuinely gone upload typed).
    let listed: Option<BTreeMap<u32, String>> = if resuming {
        list_all_parts(s3, &cfg.bucket, key, &upload.upload_id)
            .await
            .ok()
    } else {
        None
    };

    let mut hasher = blake3::Hasher::new();
    let mut sidecar_bytes: Option<Vec<u8>> = keep_bytes.then(Vec::new);
    let mut completed: Vec<CompletedPart> = Vec::with_capacity(part_count as usize);
    let mut uploaded_this_pass = 0u32;
    for part_no in 1..=part_count as u32 {
        let offset = u64::from(part_no - 1) * part_size;
        let len = part_size.min(size - offset);
        let done = recorded.get(&part_no).filter(|rec| match &listed {
            Some(parts) => parts.get(&part_no).is_some_and(|etag| *etag == rec.etag),
            None => true,
        });
        // The journal blake3 hashes exactly the sent bytes, in order —
        // already-sent ranges are re-read from the (unchanged) source
        // through the same chunk seam, un-sent ranges are read once and
        // sent as exactly the hashed bytes.
        let buf = read_range(chunks, source, offset, len).await?;
        if let Some(rec) = done {
            // The re-read range must still be the bytes the backend holds:
            // its MD5 is compared against the part's persisted `md5_b64`.
            // This is what catches a rewrite that preserved size AND mtime
            // (coarse-mtime filesystems, mtime-restoring tools) — without
            // it, the journal would advertise a blake3 matching no stored
            // object anywhere (§2.4 "hash of what was actually sent").
            if b64(Md5::digest(&buf)) != rec.md5_b64 {
                return Err(
                    abort_source_changed(db, s3, cfg, relkey, key, &upload.upload_id).await?,
                );
            }
            hasher.update(&buf);
            if let Some(acc) = sidecar_bytes.as_mut() {
                acc.extend_from_slice(&buf);
            }
            completed.push(CompletedPart {
                part_number: part_no,
                e_tag: rec.etag.clone(),
            });
            continue;
        }
        hasher.update(&buf);
        if let Some(acc) = sidecar_bytes.as_mut() {
            acc.extend_from_slice(&buf);
        }
        let digest = Md5::digest(&buf);
        let md5_b64 = b64(digest);
        let body = Bytes::from(buf);
        let out = match s3
            .upload_part(
                &cfg.bucket,
                key,
                &upload.upload_id,
                part_no,
                PartBody::from(body.clone()),
                Some(&md5_b64),
            )
            .await
        {
            Ok(out) => out,
            Err(first) if first.is_digest_rejection() => {
                // §2.4: the rejected part is retried exactly once (same
                // buffered bytes, same MD5 — the hash is NOT re-fed).
                match s3
                    .upload_part(
                        &cfg.bucket,
                        key,
                        &upload.upload_id,
                        part_no,
                        PartBody::from(body.clone()),
                        Some(&md5_b64),
                    )
                    .await
                {
                    Ok(out) => out,
                    Err(second) if second.is_digest_rejection() => {
                        return Err(TransferError::DigestRejected {
                            relkey: relkey.clone(),
                            part_number: part_no,
                        });
                    }
                    Err(second) => return Err(second.into()),
                }
            }
            Err(first) if is_no_such_upload(&first) => {
                // The backend no longer knows this upload id (swept by
                // another party): retrying it next pass can never succeed.
                // Clear the record so the requeued item restarts cleanly.
                db.with_txn(|t| t.clear_upload(relkey))?;
                return Err(first.into());
            }
            Err(first) => return Err(first.into()),
        };
        uploaded_this_pass += 1;
        // {part_no, etag, md5} persisted AFTER the part completed and
        // BEFORE the next part starts (§2.4; the crash suite SIGKILLs
        // between exactly these points).
        db.record_upload_part(
            relkey,
            part_no,
            &UploadPart {
                etag: out.e_tag.clone(),
                md5_b64,
            },
        )?;
        completed.push(CompletedPart {
            part_number: part_no,
            e_tag: out.e_tag,
        });
    }
    // A multipart sidecar is parse-validated BEFORE Complete: an invalid
    // one aborts the upload, so no unjournaled garbage object ever lands
    // (the parts alone are not an object until Complete).
    let sidecar = match sidecar_bytes {
        Some(bytes) => match parse_sidecar(&bytes) {
            Ok(sidecar) => Some(sidecar),
            Err(source) => {
                abort_upload_tolerant(s3, cfg, key, &upload.upload_id).await?;
                db.with_txn(|t| t.clear_upload(relkey))?;
                return Err(park_sidecar_invalid(db, relkey, source)?);
            }
        },
        None => None,
    };
    let blake3 = Blake3Hex::from_hash(&hasher.finalize());
    let e_tag = match s3
        .complete_multipart_upload(&cfg.bucket, key, &upload.upload_id, &completed)
        .await
    {
        Ok(done) => done.e_tag,
        Err(e) if is_no_such_upload(&e) => {
            // The upload id is definitively dead. Two cases:
            //
            // The crash-after-Complete signature — a resume in which every
            // part was already recorded (nothing re-uploaded this pass) —
            // means a previous run *may* have completed the upload and
            // died before clearing the bookkeeping. The stored object is
            // adopted only when it is provably OUR completed upload:
            // HEAD's `content_length` must equal the size whose ranges
            // were just re-read and MD5-verified against the recorded
            // parts (so `hasher` honestly names its bytes), AND the
            // stored ETag must equal the multipart ETag derivable from
            // exactly those recorded part MD5s. Size alone is not proof —
            // relkeys map to shared bucket keys (§1.2), so after our id
            // was swept (the §2.4 hygiene is cross-device by design) a
            // sibling may have stored a same-size different-content
            // version at this key, and adopting it would journal a blake3
            // no stored object has (§2.1.5 violation; every downloader
            // would condemn the intact remote as corrupt).
            //
            // Anything else (the id was swept out from under us — some
            // backends accept the parts and only fail at Complete; or the
            // stored object is not ours): the upload is unfinishable, so
            // the record is cleared and the requeued item restarts
            // cleanly instead of retrying a dead id forever.
            if resuming && uploaded_this_pass == 0 {
                if let Ok(head) = s3.head_object(&cfg.bucket, key).await {
                    let ours = head.content_length == size
                        && multipart_etag_from_parts(&recorded, part_count as u32)
                            .is_some_and(|etag| etag == head.e_tag);
                    if ours {
                        return Ok(SentObject {
                            blake3,
                            size,
                            e_tag: head.e_tag,
                            multipart: true,
                            md5_hex: None,
                            sidecar,
                            mtime_unix,
                            mtime_unix_ns,
                        });
                    }
                }
            }
            db.with_txn(|t| t.clear_upload(relkey))?;
            return Err(e.into());
        }
        Err(e) => return Err(e.into()),
    };
    Ok(SentObject {
        blake3,
        size,
        e_tag,
        multipart: true,
        md5_hex: None,
        sidecar,
        mtime_unix,
        mtime_unix_ns,
    })
}

/// Parse-validates sidecar bytes (§2.5 parser), returning the journal
/// facts they carry.
fn parse_sidecar(bytes: &[u8]) -> Result<(SemHash, crate::semhash::SidecarBadges), SemHashError> {
    Ok((sem_hash(bytes)?, sidecar_badges(bytes)?))
}

/// The upload-side invalid-sidecar exit: park the item
/// `uploading → dirty` (retrying an unchanged invalid sidecar can never
/// succeed; the next local change re-admits it) and hand back the typed
/// [`TransferError::SidecarInvalid`].
fn park_sidecar_invalid(
    db: &SyncDb,
    relkey: &RelKey,
    source: SemHashError,
) -> Result<TransferError, TransferError> {
    db.with_txn(|t| {
        t.transition(relkey, ItemState::Uploading, ItemState::Dirty, |_| {})?;
        Ok(())
    })?;
    Ok(TransferError::SidecarInvalid {
        relkey: relkey.clone(),
        source,
    })
}

/// The S3 multipart ETag the backend must report for an object completed
/// from exactly these recorded parts: `hex(md5(concat(part md5 bytes)))-
/// <n>` (the AWS/Garage/MinIO convention the §8 harness validates, like
/// the single-PUT `ETag == md5hex` convention). `None` when any of parts
/// `1..=part_count` lacks a record or its persisted `md5_b64` does not
/// decode — the caller must then treat the stored object as not provably
/// ours.
fn multipart_etag_from_parts(
    recorded: &BTreeMap<u32, UploadPart>,
    part_count: u32,
) -> Option<String> {
    let mut concat = Vec::with_capacity(16 * part_count as usize);
    for part_no in 1..=part_count {
        let md5_b64 = &recorded.get(&part_no)?.md5_b64;
        let digest = base64::engine::general_purpose::STANDARD
            .decode(md5_b64)
            .ok()?;
        if digest.len() != 16 {
            return None;
        }
        concat.extend_from_slice(&digest);
    }
    Some(format!(
        "{}-{part_count}",
        hex::encode(Md5::digest(&concat))
    ))
}

/// `true` when `e` is the backend saying the multipart upload id no
/// longer exists (`NoSuchUpload`).
fn is_no_such_upload(e: &S3Error) -> bool {
    e.code() == Some(&S3ErrorCode::NoSuchUpload)
}

/// Aborts `upload_id`, treating a backend that no longer knows it
/// (already aborted/completed elsewhere) as success.
async fn abort_upload_tolerant(
    s3: &impl S3TransferApi,
    cfg: &TransferConfig,
    key: &str,
    upload_id: &str,
) -> Result<(), TransferError> {
    match s3.abort_multipart_upload(&cfg.bucket, key, upload_id).await {
        Ok(()) => Ok(()),
        Err(e) if is_no_such_upload(&e) || e.is_no_such_key() => Ok(()),
        Err(e) => Err(e.into()),
    }
}

/// The mid-resume source-change exit: abort the backend upload
/// (tolerating one already gone), clear the multipart bookkeeping and
/// re-mark the item `dirty` in one transaction, and hand back the typed
/// [`TransferError::AbortedSourceChanged`].
async fn abort_source_changed(
    db: &SyncDb,
    s3: &impl S3TransferApi,
    cfg: &TransferConfig,
    relkey: &RelKey,
    key: &str,
    upload_id: &str,
) -> Result<TransferError, TransferError> {
    abort_upload_tolerant(s3, cfg, key, upload_id).await?;
    db.with_txn(|t| {
        t.clear_upload(relkey)?;
        t.transition(relkey, ItemState::Uploading, ItemState::Dirty, |_| {})?;
        Ok(())
    })?;
    Ok(TransferError::AbortedSourceChanged {
        relkey: relkey.clone(),
    })
}

/// The `verifying` step (§2.4): HEAD + size check; single-PUT ETag == MD5
/// on a digest-verifying backend; full GET re-hash when the backend
/// failed the probe (`requires_readback_verify`). A content disagreement
/// is [`TransferError::CorruptRemote`]; a transport failure is the
/// underlying error (retryable).
async fn verify_remote(
    s3: &impl S3TransferApi,
    cfg: &TransferConfig,
    relkey: &RelKey,
    key: &str,
    sent: &SentObject,
) -> Result<(), TransferError> {
    let head = s3.head_object(&cfg.bucket, key).await?;
    if head.content_length != sent.size {
        return Err(TransferError::CorruptRemote {
            relkey: relkey.clone(),
            detail: format!(
                "stored size {} != sent size {}",
                head.content_length, sent.size
            ),
        });
    }
    if cfg.backend.digest_rejection_works {
        // Parts (multipart) were server-MD5-verified at receipt; a single
        // PUT's stored ETag must still equal our MD5, and a multipart
        // object's stored ETag must still equal what Complete returned —
        // a different ETag means the key no longer holds the object we
        // verified part-by-part (e.g. a sibling device replaced the
        // shared key between Complete and here), and journaling our
        // blake3 against it would poison every downloader (§2.1.5).
        if let Some(md5_hex) = &sent.md5_hex {
            if head.e_tag != *md5_hex {
                return Err(TransferError::CorruptRemote {
                    relkey: relkey.clone(),
                    detail: format!("stored ETag {} != sent MD5 {md5_hex}", head.e_tag),
                });
            }
        } else if head.e_tag != sent.e_tag {
            return Err(TransferError::CorruptRemote {
                relkey: relkey.clone(),
                detail: format!(
                    "stored ETag {} != completed upload ETag {}",
                    head.e_tag, sent.e_tag
                ),
            });
        }
    } else {
        // requires_readback_verify: full GET re-hash against the streamed
        // hash of what was sent.
        let out = s3.get_object(&cfg.bucket, key, None).await?;
        let mut hasher = blake3::Hasher::new();
        let mut body = out.body;
        while let Some(chunk) = body.try_next().await? {
            hasher.update(&chunk);
        }
        let actual = Blake3Hex::from_hash(&hasher.finalize());
        if actual != sent.blake3 {
            return Err(TransferError::CorruptRemote {
                relkey: relkey.clone(),
                detail: format!("read-back blake3 {actual} != sent blake3 {}", sent.blake3),
            });
        }
    }
    Ok(())
}

/// Every part of `upload_id`, across all `ListParts` pages.
async fn list_all_parts(
    s3: &impl S3TransferApi,
    bucket: &str,
    key: &str,
    upload_id: &str,
) -> Result<BTreeMap<u32, String>, S3Error> {
    let mut out = BTreeMap::new();
    let mut request = ListPartsRequest::default();
    loop {
        let page = s3.list_parts(bucket, key, upload_id, &request).await?;
        for part in page.parts {
            out.insert(part.part_number, part.e_tag);
        }
        if !page.is_truncated || page.next_part_number_marker.is_none() {
            break;
        }
        request.part_number_marker = page.next_part_number_marker;
    }
    Ok(out)
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
    db.with_txn_err::<u64, TransferError>(|t| {
        let record = t.transition(relkey, ItemState::Verifying, ItemState::Synced, |r| {
            r.verified_remote = true;
            mutate(r);
        })?;
        let entry = build_entry(&record)?;
        Ok(enqueue_entry_in(t, db.device_id(), &entry)?)
    })
}

/// What [`abort_stale_uploads`] cleaned up.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StaleUploadReport {
    /// Our own multipart records older than `max_age` that were aborted
    /// on the backend and cleared locally: `(relkey, upload_id)`.
    pub aborted_own: Vec<(RelKey, String)>,
    /// Backend-listed uploads targeting a key **we hold a multipart
    /// record for**, under a *different* `upload_id`, whose `Initiated`
    /// timestamp proves them older than `max_age` (abandoned orphans of a
    /// restart that re-created the upload): `(bucket_key, upload_id)`.
    pub aborted_orphans: Vec<(String, String)>,
    /// Aged-out `uploads`-table rows whose item record no longer exists:
    /// the backend upload was aborted best-effort under both candidate
    /// bucket keys and the local row cleared, so neither the row nor the
    /// backend upload leaks forever: `(relkey, upload_id)`.
    pub cleared_recordless: Vec<(RelKey, String)>,
}

/// §2.4 stale-upload hygiene.
///
/// 1. Every `uploads`-table record with `started_unix + max_age_secs <=
///    now_unix` is aborted on the backend (`AbortMultipartUpload`), its
///    local record and part records cleared, and — when the item sits in
///    `uploading` — the item re-marked `dirty` (`uploading → dirty`, the
///    §2.4 "upload abandoned" edge). A record still fresh is untouched.
///    An aged row whose **item record is missing** (a legitimate delete
///    raced the upload, or corruption recovery removed the item) cannot
///    derive its one bucket key, so the abort is issued best-effort under
///    both candidate keys (library/sidecar; abort is addressed by
///    `(key, upload_id)`, so a wrong-key attempt is a tolerated
///    `NoSuchUpload`, never someone else's upload) and the row cleared —
///    reported in [`StaleUploadReport::cleared_recordless`].
/// 2. `ListMultipartUploads` (paged) is scanned for **own-key orphans**:
///    uploads whose key corresponds to one of our `uploads`-table relkeys
///    but whose `upload_id` differs from the recorded one. An orphan is
///    aborted only when its `Initiated` timestamp proves it older than
///    `max_age_secs` — age-gated exactly like the own-record path,
///    because "a different id on our key" does **not** mean abandoned:
///    relkeys map to *shared* bucket keys (§1.2), so a sibling device may
///    be live-uploading the same key right now (duplicate import, the
///    §2.4 corrupt-remote repair), and this engine's own pump may have
///    re-created the upload between the table snapshot and this page. An
///    orphan whose age cannot be proven (missing/unparseable
///    `Initiated`) is left alone. Uploads on keys we hold no record for
///    are another device's business and are also left alone; the
///    cross-device sweep is the worker's job (later unit).
pub async fn abort_stale_uploads(
    db: &SyncDb,
    s3: &impl S3TransferApi,
    cfg: &TransferConfig,
    max_age_secs: i64,
    now_unix: i64,
) -> Result<StaleUploadReport, TransferError> {
    let aged = |start: i64| start.saturating_add(max_age_secs) <= now_unix;
    let mut report = StaleUploadReport::default();
    // Our uploads table is the orphan-matching key set regardless of age
    // (the age gate below is per-orphan, on the backend's Initiated).
    let mut own_keys: BTreeMap<String, String> = BTreeMap::new();
    for (relkey, upload) in db.iter_uploads()? {
        let Some(record) = db.get_item(&relkey)? else {
            // No item record: the one true bucket key is underivable. An
            // aged row is still cleaned up (doc item 1); a fresh one is
            // left for the item/delete machinery to settle first.
            if aged(upload.started_unix) {
                for key in [library_key(&relkey), sidecar_key(&relkey)] {
                    abort_upload_tolerant(s3, cfg, &key, &upload.upload_id).await?;
                }
                db.with_txn(|t| t.clear_upload(&relkey))?;
                report.cleared_recordless.push((relkey, upload.upload_id));
            }
            continue;
        };
        let key = bucket_key_for(&relkey, record.kind)?;
        own_keys.insert(key.clone(), upload.upload_id.clone());
        if !aged(upload.started_unix) {
            continue; // still fresh
        }
        // Aged out: abort on the backend, clear locally, and — when the
        // item still sits in `uploading` — re-mark it dirty (§2.4 "upload
        // abandoned"). A backend that no longer knows the upload id
        // (already aborted/completed elsewhere) is treated as done.
        abort_upload_tolerant(s3, cfg, &key, &upload.upload_id).await?;
        db.with_txn(|t| {
            t.clear_upload(&relkey)?;
            Ok(())
        })?;
        if record.state == ItemState::Uploading {
            demote(db, &relkey, ItemState::Uploading, ItemState::Dirty)?;
        }
        report.aborted_own.push((relkey, upload.upload_id));
    }

    // Backend scan for own-key orphans (paged), age-gated on Initiated
    // (doc item 2). Uploads on keys we hold no record for are another
    // device's business (§1.2) and are left alone.
    let mut request = ListMultipartUploadsRequest::default();
    loop {
        let page = s3.list_multipart_uploads(&cfg.bucket, &request).await?;
        for upload in page.uploads {
            let orphan = own_keys
                .get(&upload.key)
                .is_some_and(|ours| *ours != upload.upload_id);
            let provably_aged = upload
                .initiated
                .as_deref()
                .and_then(initiated_unix)
                .is_some_and(aged);
            if orphan && provably_aged {
                abort_upload_tolerant(s3, cfg, &upload.key, &upload.upload_id).await?;
                report.aborted_orphans.push((upload.key, upload.upload_id));
            }
        }
        if !page.is_truncated || page.next_key_marker.is_none() {
            break;
        }
        request.key_marker = page.next_key_marker;
        request.upload_id_marker = page.next_upload_id_marker;
    }
    Ok(report)
}

/// Parses a `ListMultipartUploads` `Initiated` timestamp (RFC 3339) to
/// unix seconds. `None` when absent/unparseable — the caller treats that
/// as "age unprovable" and leaves the upload alone.
fn initiated_unix(initiated: &str) -> Option<i64> {
    time::OffsetDateTime::parse(initiated, &time::format_description::well_known::Rfc3339)
        .ok()
        .map(|t| t.unix_timestamp())
}

/// What [`recover_interrupted`] re-drove.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RecoveryReport {
    /// Items found stranded in `uploading`/`verifying`, demoted back to
    /// `queued` and pushed onto the upload queue. A surviving multipart
    /// record is kept — it (not the state) is what makes the next
    /// [`upload_item`] pass a resume.
    pub requeued_uploads: Vec<RelKey>,
    /// Items found stranded in `downloading`, demoted to `pending_down`
    /// and pushed back onto the download queue (their partial survives
    /// for a ranged resume).
    pub requeued_downloads: Vec<RelKey>,
}

/// The crash-recovery startup sweep (module docs, "Crash recovery"): every
/// item a crash stranded in a pipeline-interior state whose queue row is
/// already popped — `uploading`, `verifying`, `downloading` — is demoted
/// along its legal §2.4 edge where needed and pushed back onto its queue
/// at priority `class`, so the next pump pass re-drives it. Idempotent
/// (`queue_push` is membership-checked). Run once at startup, before the
/// first pump pass, under the §5.1 single-instance exclusion; it is a
/// check-then-act scan and must not race a live pump.
pub fn recover_interrupted(db: &SyncDb, class: u8) -> Result<RecoveryReport, TransferError> {
    let mut report = RecoveryReport::default();
    // Crash between upload-complete bookkeeping and the verify-commit:
    // the object may be stored but nothing was journaled — re-run the
    // whole upload (idempotent: a re-PUT of identical bytes re-verifies).
    for (relkey, _) in db.items_in_state(ItemState::Verifying)? {
        db.with_txn(|t| {
            t.transition(&relkey, ItemState::Verifying, ItemState::Queued, |_| {})?;
            t.queue_push(Queue::Up, &relkey, class)?;
            Ok(())
        })?;
        report.requeued_uploads.push(relkey);
    }
    // Crash mid-upload: demoted back to `queued` so re-admission is the
    // ordinary queued → uploading CAS (the single-driver gate — a record
    // still at `uploading` when upload_item runs means a LIVE transfer,
    // never a crash leftover). A surviving multipart record is kept: it,
    // not the state, is what makes the next pass a resume; without one
    // (mid-single-PUT, or before `set_upload`) the pass restarts.
    for (relkey, _) in db.items_in_state(ItemState::Uploading)? {
        db.with_txn(|t| {
            t.transition(&relkey, ItemState::Uploading, ItemState::Queued, |_| {})?;
            t.queue_push(Queue::Up, &relkey, class)?;
            Ok(())
        })?;
        report.requeued_uploads.push(relkey);
    }
    // Crash mid-download: the queue row was popped before the crash.
    for (relkey, _) in db.items_in_state(ItemState::Downloading)? {
        db.with_txn(|t| {
            t.transition(
                &relkey,
                ItemState::Downloading,
                ItemState::PendingDown,
                |_| {},
            )?;
            t.queue_push(Queue::Down, &relkey, class)?;
            Ok(())
        })?;
        report.requeued_downloads.push(relkey);
    }
    Ok(report)
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
/// Entry states: `pending_down` or `stub`, admitted via the transition
/// CAS — the single-driver gate (module docs, "Crash recovery and
/// single-driver entry"). `downloading` is **not** an entry state: a
/// crash-stranded `downloading` item is demoted to `pending_down` by
/// [`recover_interrupted`] before any pump pass (its surviving `.rr.part`
/// partial is what makes the next attempt a resume), so a record already
/// at `downloading` means a live concurrent transfer owns the item and
/// this caller gets a typed [`StateError::StaleState`] — never a second
/// writer appending to the live transfer's partial. Callers that can
/// legitimately race (the §3.5 `ensure_local` guard sites vs. the pump)
/// must treat that error as "already in flight" (wait / single-flight
/// per relkey), not retry immediately.
///
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
    let record = db
        .get_item(relkey)?
        .ok_or_else(|| TransferError::MissingItem {
            relkey: relkey.clone(),
        })?;
    let kind = record.kind;
    let key = bucket_key_for(relkey, kind)?;
    let final_path = local_target_path(dest_root, relkey, kind);
    let partial = partial_path(&final_path);

    // Entry: `pending_down`/`stub` → `downloading` via the transition CAS
    // — the single-driver gate. `downloading` is NOT accepted:
    // recover_interrupted demotes crash-stranded items before the first
    // pump pass, so a record already at `downloading` means a live
    // concurrent transfer owns this item (and its partial). Refuse typed
    // instead of racing a second writer onto the shared `.rr.part`.
    if record.state == ItemState::Downloading {
        return Err(StateError::StaleState {
            relkey: relkey.clone(),
            expected: ItemState::PendingDown,
            found: Some(ItemState::Downloading),
        }
        .into());
    }
    db.transition(relkey, record.state, ItemState::Downloading, |_| {})?;

    let had_partial = tokio::fs::metadata(&partial)
        .await
        .map(|m| m.len() > 0)
        .unwrap_or(false);
    let mut result = run_download(
        db,
        s3,
        cfg,
        relkey,
        kind,
        &key,
        &final_path,
        &partial,
        expected,
    )
    .await;
    // A hash mismatch after a RESUMED attempt does not prove the remote
    // wrong: the surviving partial may belong to a superseded object
    // version (the journal head advanced between attempts), and splicing
    // old-prefix + new-tail can never verify even over an intact object.
    // Discard the partial and retry once from scratch (module docs); only
    // a from-scratch mismatch condemns the remote below.
    if had_partial && matches!(result, Err(TransferError::IntegrityMismatch { .. })) {
        let _ = tokio::fs::remove_file(&partial).await;
        result = run_download(
            db,
            s3,
            cfg,
            relkey,
            kind,
            &key,
            &final_path,
            &partial,
            expected,
        )
        .await;
    }
    match result {
        Ok(outcome) => Ok(outcome),
        // Integrity/parse failures are properties of the remote object:
        // the partial is deleted (it can never verify) and the item moves
        // to the corrupt_remote lane.
        Err(
            e @ (TransferError::IntegrityMismatch { .. } | TransferError::SidecarInvalid { .. }),
        ) => {
            let _ = tokio::fs::remove_file(&partial).await;
            // The integrity/parse failure stays primary (demote doc).
            let _ = demote(db, relkey, ItemState::Downloading, ItemState::CorruptRemote);
            Err(e)
        }
        // Everything else (transport, local I/O) keeps the partial for a
        // ranged resume and returns the item to `pending_down`.
        Err(e) => {
            let _ = demote(db, relkey, ItemState::Downloading, ItemState::PendingDown);
            Err(e)
        }
    }
}

/// The streaming middle of [`download_item`]: partial re-hash, (ranged)
/// GET, append, final verify, parse-validate, atomic install, mtime
/// restore, terminal transition. Failure state edges live in the caller.
#[allow(clippy::too_many_arguments)] // internal seam of one state walk
async fn run_download(
    db: &SyncDb,
    s3: &impl S3TransferApi,
    cfg: &TransferConfig,
    relkey: &RelKey,
    kind: Kind,
    key: &str,
    final_path: &Path,
    partial: &Path,
    expected: &ExpectedDownload,
) -> Result<DownloadOutcome, TransferError> {
    if let Some(parent) = final_path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|e| io_err(parent, e))?;
    }
    let mut hasher = blake3::Hasher::new();
    let mut resumed_from = 0u64;
    match tokio::fs::metadata(partial).await {
        Ok(meta) if meta.len() > expected.size => {
            // An over-long partial can never verify: start fresh.
            tokio::fs::remove_file(partial)
                .await
                .map_err(|e| io_err(partial, e))?;
        }
        Ok(meta) => {
            // §3.5 resume: re-hash the surviving bytes from disk, then
            // continue with a ranged GET from exactly this offset.
            resumed_from = meta.len();
            hash_file_into(partial, &mut hasher).await?;
        }
        Err(_) => {}
    }

    // The partial exists from here on even when nothing needs fetching (a
    // zero-byte object downloads as create + verify-empty + rename).
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(partial)
        .await
        .map_err(|e| io_err(partial, e))?;
    let mut bytes_fetched = 0u64;
    if resumed_from < expected.size {
        use tokio::io::AsyncWriteExt as _;
        let range = (resumed_from > 0).then_some(ByteRange::From(resumed_from));
        let out = s3.get_object(&cfg.bucket, key, range).await?;
        // A resume is only a resume when the response is actually partial
        // and starts at our offset. A backend/intermediary that ignored
        // the Range header returned the FULL object — appending it after
        // the partial would splice garbage and mis-file an intact remote
        // as corrupt. Discard the partial and take the full body from 0.
        if resumed_from > 0
            && out
                .content_range
                .as_deref()
                .and_then(content_range_start)
                .is_none_or(|start| start != resumed_from)
        {
            file.set_len(0).await.map_err(|e| io_err(partial, e))?;
            hasher.reset();
            resumed_from = 0;
        }
        let mut body = out.body;
        loop {
            match body.next().await {
                Some(Ok(chunk)) => {
                    file.write_all(&chunk)
                        .await
                        .map_err(|e| io_err(partial, e))?;
                    hasher.update(&chunk);
                    bytes_fetched += chunk.len() as u64;
                }
                Some(Err(e)) => {
                    // Every received byte is already appended: the partial
                    // survives for the next ranged resume.
                    let _ = file.sync_all().await;
                    return Err(e.into());
                }
                None => break,
            }
        }
        file.sync_all().await.map_err(|e| io_err(partial, e))?;
    }
    drop(file);

    // The §3.5 backstop for every failure mode, including a mid-download
    // remote replacement: nothing installs unless the full content hashes
    // to exactly what the journal advertised.
    let actual = Blake3Hex::from_hash(&hasher.finalize());
    if actual != expected.blake3 {
        return Err(TransferError::IntegrityMismatch {
            relkey: relkey.clone(),
            expected: expected.blake3.clone(),
            actual,
        });
    }

    // Sidecars are additionally parse-validated (§2.5 semantic parser):
    // an invalid sidecar is never installed, whatever its hash says.
    if kind == Kind::Sidecar {
        let bytes = tokio::fs::read(partial)
            .await
            .map_err(|e| io_err(partial, e))?;
        if let Err(source) = sem_hash(&bytes) {
            return Err(TransferError::SidecarInvalid {
                relkey: relkey.clone(),
                source,
            });
        }
    }

    // Atomic install (same directory) + mtime restore (§3.5: keeps the
    // thumbnail cache hash stable across hydration).
    tokio::fs::rename(partial, final_path)
        .await
        .map_err(|e| io_err(final_path, e))?;
    filetime::set_file_mtime(
        final_path,
        filetime::FileTime::from_unix_time(expected.mtime_unix, 0),
    )
    .map_err(|e| io_err(final_path, e))?;

    let terminal = if kind == Kind::Original {
        ItemState::Hydrated
    } else {
        ItemState::Synced
    };
    db.transition(relkey, ItemState::Downloading, terminal, |r| {
        r.blake3 = Some(expected.blake3.clone());
        r.size = expected.size;
        r.mtime_unix_ns = expected.mtime_unix.saturating_mul(1_000_000_000);
    })?;
    Ok(DownloadOutcome {
        relkey: relkey.clone(),
        path: final_path.to_path_buf(),
        resumed_from,
        bytes_fetched,
    })
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

    /// Requests cancellation: the pump stops admitting new items, and
    /// items already admitted run to completion (a popped item is in
    /// flight immediately — there is no popped-but-not-started window).
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
    pump(
        db,
        Queue::Up,
        ItemState::Queued,
        concurrency,
        cancel,
        |relkey| {
            Box::pin(async move {
                let record = db
                    .get_item(&relkey)?
                    .ok_or_else(|| TransferError::MissingItem {
                        relkey: relkey.clone(),
                    })?;
                let source = local_target_path(&cfg.sync_root, &relkey, record.kind);
                upload_item(db, s3, cfg, &relkey, &source).await.map(|_| ())
            })
        },
    )
    .await
}

/// Drains the download queue with the same admission/concurrency/
/// failure-isolation/cancel contract as [`pump_uploads`]; each item's
/// [`ExpectedDownload`] comes from its own record (`blake3`, `size`,
/// `mtime_unix_ns / 1e9`) and its destination from `cfg.sync_root`. An
/// item whose record lacks a `blake3` cannot be verified and is recorded
/// as failed (never installed unverified) — and because nothing inside
/// the engine can ever supply the missing hash, it is **parked off the
/// queue** (state left `pending_down`, queue row not re-pushed) instead
/// of being re-popped and re-failed on every pass forever. Whatever
/// later supplies the hash (a journal apply advancing the head) re-queues
/// it through its own admission.
pub async fn pump_downloads(
    db: &SyncDb,
    s3: &impl S3TransferApi,
    cfg: &TransferConfig,
    concurrency: usize,
    cancel: &CancelFlag,
) -> Result<PumpSummary, TransferError> {
    pump(
        db,
        Queue::Down,
        ItemState::PendingDown,
        concurrency,
        cancel,
        |relkey| {
            Box::pin(async move {
                let record = db
                    .get_item(&relkey)?
                    .ok_or_else(|| TransferError::MissingItem {
                        relkey: relkey.clone(),
                    })?;
                let Some(blake3) = record.blake3.clone() else {
                    return Err(TransferError::MissingExpectedHash {
                        relkey: relkey.clone(),
                    });
                };
                let expected = ExpectedDownload {
                    blake3,
                    size: record.size,
                    mtime_unix: record.mtime_unix_ns.div_euclid(1_000_000_000),
                };
                download_item(db, s3, cfg, &relkey, &cfg.sync_root, &expected)
                    .await
                    .map(|_| ())
            })
        },
    )
    .await
}

/// One transfer future as the pump holds it: boxed so an empty pass needs
/// no type inference, `!Send` is fine (the pump never spawns — N futures
/// are polled concurrently on the caller's task).
type PumpFuture<'a> = Pin<Box<dyn Future<Output = Result<(), TransferError>> + 'a>>;

/// A [`PumpFuture`] tagged with the queue row it came from, as the pump's
/// in-flight set holds it.
type TaggedPumpFuture<'a> =
    Pin<Box<dyn Future<Output = (RelKey, u8, Result<(), TransferError>)> + 'a>>;

/// The shared pump loop: pops `queue` strictly in (class, arrival) order,
/// keeps up to `concurrency` transfer futures in flight at once (the
/// [`FuturesUnordered`] length *is* the §2.4 concurrency cap), records
/// each item's outcome without stopping the others, and re-queues a
/// failed item for a **later** pass — only when its state still says it
/// belongs in the queue (`requeue_state`), so a terminal failure
/// (`corrupt_remote`, re-marked `dirty`) never loops, and never when the
/// failure can never heal inside the engine
/// ([`TransferError::MissingExpectedHash`] — such an item is parked off
/// the queue in its queueable state). A fired [`CancelFlag`] stops
/// admission; in-flight items are always awaited.
async fn pump<'a, F>(
    db: &SyncDb,
    queue: Queue,
    requeue_state: ItemState,
    concurrency: usize,
    cancel: &CancelFlag,
    run: F,
) -> Result<PumpSummary, TransferError>
where
    F: Fn(RelKey) -> PumpFuture<'a>,
{
    let concurrency = concurrency.max(1);
    let mut summary = PumpSummary::default();
    let mut requeue: Vec<(RelKey, u8)> = Vec::new();
    let mut in_flight: FuturesUnordered<TaggedPumpFuture<'a>> = FuturesUnordered::new();
    loop {
        // Admission: strictly queue order, never past the concurrency cap,
        // and nothing new once the cancel flag is observed.
        while in_flight.len() < concurrency {
            if cancel.is_cancelled() {
                summary.cancelled = true;
                break;
            }
            let Some((relkey, class)) = db.queue_pop(queue)? else {
                break;
            };
            let fut = run(relkey.clone());
            in_flight.push(Box::pin(async move {
                let result = fut.await;
                (relkey, class, result)
            }));
        }
        let Some((relkey, class, result)) = in_flight.next().await else {
            break;
        };
        match result {
            Ok(()) => summary.completed.push(relkey),
            Err(e) => {
                // A failure the engine can never heal on its own — a
                // record with no expected hash stays hashless however
                // often it is popped — is parked: recorded in the
                // summary, left in its queueable state, but NOT re-pushed
                // (whatever supplies the hash later re-queues it). Every
                // other failure goes back for a later pass.
                let never_heals = matches!(e, TransferError::MissingExpectedHash { .. });
                summary.failed.push((relkey.clone(), e.to_string()));
                if !never_heals {
                    requeue.push((relkey, class));
                }
            }
        }
    }
    // Failed items go back for a later pass (never re-popped in this one),
    // keeping their class — unless their state left the queueable lane.
    for (relkey, class) in requeue {
        let still_queueable = matches!(db.get_item(&relkey)?, Some(r) if r.state == requeue_state);
        if still_queueable {
            db.queue_push(queue, &relkey, class)?;
        }
    }
    Ok(summary)
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

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Standard base64 of an MD5 digest — the `Content-MD5` wire form.
fn b64(digest: impl AsRef<[u8]>) -> String {
    base64::engine::general_purpose::STANDARD.encode(digest)
}

fn io_err(path: &Path, source: std::io::Error) -> TransferError {
    TransferError::Io {
        path: path.to_path_buf(),
        source,
    }
}

/// Best-effort state demotion. A CAS loss ([`StateError::StaleState`]) is
/// tolerated as `Ok` — it means a concurrent writer already moved the
/// item, whose state is *its* responsibility now. Any **other** state
/// error (redb I/O, corruption) is returned: call sites with no primary
/// error in hand propagate it, while failure-path call sites keep their
/// primary error primary and discard this result explicitly (`let _ =`)
/// — the item is then stranded in a pipeline-interior state that the
/// next [`recover_interrupted`] sweep re-drives, which is the only
/// recovery a failing state store allows anyway.
fn demote(db: &SyncDb, relkey: &RelKey, from: ItemState, to: ItemState) -> Result<(), StateError> {
    match db.transition(relkey, from, to, |_| {}) {
        Ok(_) | Err(StateError::StaleState { .. }) => Ok(()),
        Err(e) => Err(e),
    }
}

/// Local wall clock, unix seconds (saturating; library paths never panic
/// on a badly set clock).
fn now_unix() -> i64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => i64::try_from(d.as_secs()).unwrap_or(i64::MAX),
        Err(e) => i64::try_from(e.duration().as_secs())
            .map(i64::wrapping_neg)
            .unwrap_or(i64::MIN),
    }
}

/// Best server-time estimate in unix seconds (§2.10): the persisted
/// heartbeat offset applied to the local clock, or the raw local clock
/// before any measurement exists.
fn server_ts_estimate(db: &SyncDb) -> Result<i64, TransferError> {
    let offset_ms = db.server_time_offset_ms()?.unwrap_or(0);
    Ok(now_unix().saturating_add(offset_ms.div_euclid(1000)))
}

/// A file's mtime as unix nanoseconds (negative before the epoch).
fn file_mtime_unix_ns(meta: &std::fs::Metadata) -> i64 {
    let Ok(mtime) = meta.modified() else {
        return 0; // platform without mtime support
    };
    match mtime.duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => i64::try_from(d.as_nanos()).unwrap_or(i64::MAX),
        Err(e) => i64::try_from(e.duration().as_nanos())
            .map(i64::wrapping_neg)
            .unwrap_or(i64::MIN),
    }
}

/// Reads exactly `len` bytes of `path` starting at `offset` through the
/// chunk seam, buffering them (one part at a time — bounded by the part
/// size / single-PUT threshold, never the whole object). A source that
/// ends early is an I/O error (the §2.4 size recheck makes it a race).
async fn read_range(
    chunks: &impl ChunkSource,
    path: &Path,
    offset: u64,
    len: u64,
) -> Result<Vec<u8>, TransferError> {
    let mut stream = chunks
        .open(path, offset)
        .await
        .map_err(|e| io_err(path, e))?;
    let mut buf: Vec<u8> = Vec::with_capacity(usize::try_from(len).unwrap_or(0));
    while (buf.len() as u64) < len {
        match stream.next().await {
            Some(Ok(chunk)) => {
                let need = len - buf.len() as u64;
                let take = usize::try_from(need)
                    .map(|n| n.min(chunk.len()))
                    .unwrap_or(chunk.len());
                buf.extend_from_slice(&chunk[..take]);
            }
            Some(Err(e)) => return Err(io_err(path, e)),
            None => {
                return Err(io_err(
                    path,
                    std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        format!(
                            "source ended after {} of {len} bytes at offset {offset}",
                            buf.len()
                        ),
                    ),
                ));
            }
        }
    }
    Ok(buf)
}

/// Streams `path` into `hasher` (the §3.5 partial re-hash; never buffers
/// the whole file). Reads go through `tokio::fs` in bounded chunks, so a
/// multi-gigabyte partial's re-hash yields between chunks instead of
/// pinning the executor thread for the whole file.
async fn hash_file_into(path: &Path, hasher: &mut blake3::Hasher) -> Result<(), TransferError> {
    use tokio::io::AsyncReadExt as _;
    let mut file = tokio::fs::File::open(path)
        .await
        .map_err(|e| io_err(path, e))?;
    let mut buf = vec![0u8; 256 * 1024];
    loop {
        let n = file.read(&mut buf).await.map_err(|e| io_err(path, e))?;
        if n == 0 {
            return Ok(());
        }
        hasher.update(&buf[..n]);
    }
}

/// Parses the start offset out of a `Content-Range` header
/// (`bytes <start>-<end>/<total>`); `None` when it does not parse.
fn content_range_start(content_range: &str) -> Option<u64> {
    content_range
        .trim()
        .strip_prefix("bytes")?
        .trim_start()
        .split('-')
        .next()?
        .parse()
        .ok()
}
