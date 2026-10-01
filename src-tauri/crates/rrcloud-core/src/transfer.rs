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
//! class and run up to `concurrency` items at once (§2.4 names 2 on
//! Android, 4 on desktop; the number is a parameter here). The cap is
//! structural: the pump admits into a [`FuturesUnordered`] on the
//! caller's task and never lets its length exceed `concurrency`, so no
//! semaphore (and no task spawning) is needed. One item's failure is
//! recorded in the summary — with its state left resumable — and never
//! stops the pump. A [`CancelFlag`] stops *admission* deterministically
//! and waits for in-flight items to finish.

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
    PutObjectOptions, S3Error, S3TransferApi,
};
use crate::semhash::{sem_hash, Blake3Hex, ContentId, SemHashError};
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

    /// A pump-admitted download whose item record carries no `blake3`:
    /// nothing could ever verify the fetched bytes, so the engine refuses
    /// to install them (§3.5 — the hash is the backstop for *every*
    /// failure mode, so an unverifiable download is never attempted).
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

    // Entry: `queued → uploading` CAS, or crash-recovery re-entry already
    // sitting at `uploading` with a persisted multipart record. Any other
    // state fails the CAS typed (StaleState / IllegalTransition).
    if !(record.state == ItemState::Uploading && resume.is_some()) {
        db.transition(relkey, ItemState::Queued, ItemState::Uploading, |_| {})?;
    }

    // ---- uploading: send the bytes (single PUT or multipart) ----
    let sent =
        match transfer_object(db, s3, cfg, relkey, &record, &key, source, chunks, resume).await {
            Ok(sent) => sent,
            // `AbortedSourceChanged` already re-marked the item dirty; every
            // other failure re-queues (`uploading → queued`) with the
            // multipart bookkeeping intact, so the next attempt resumes.
            Err(e @ TransferError::AbortedSourceChanged { .. }) => return Err(e),
            Err(e) => {
                demote(db, relkey, ItemState::Uploading, ItemState::Queued);
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
                demote(db, relkey, ItemState::Verifying, ItemState::CorruptRemote);
                corrupt
            }
            other => {
                demote(db, relkey, ItemState::Verifying, ItemState::Queued);
                other
            }
        });
    }

    // ---- verifying → synced + journal staging, one transaction ----
    let ts = server_ts_estimate(db)?;
    let sem = match &sent.sidecar_bytes {
        Some(bytes) => match sem_hash(bytes) {
            Ok(sem) => Some(sem),
            Err(source) => {
                demote(db, relkey, ItemState::Verifying, ItemState::Queued);
                return Err(TransferError::SidecarInvalid {
                    relkey: relkey.clone(),
                    source,
                });
            }
        },
        None => None,
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
                    rating: None,
                    color_label: None,
                    content_id,
                    w: None,
                    h: None,
                    mtime,
                    from_key: None,
                })
            },
        ) {
            Ok(id) => id,
            Err(e) => {
                // The whole commit rolled back: the item is still
                // `verifying`; re-queue it so the next pass retries.
                demote(db, relkey, ItemState::Verifying, ItemState::Queued);
                return Err(e);
            }
        }
    };

    Ok(UploadOutcome {
        relkey: relkey.clone(),
        blake3,
        size,
        e_tag: sent.e_tag,
        multipart: sent.multipart,
        outbound_id,
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
    /// The sent bytes, retained only for sidecars (the journal entry's
    /// `sem_hash` is parsed from exactly what was sent).
    sidecar_bytes: Option<Vec<u8>>,
    /// Source mtime (unix seconds) captured when the transfer began.
    mtime_unix: i64,
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
    let meta = std::fs::metadata(source).map_err(|e| io_err(source, e))?;
    let size = meta.len();
    let mtime_unix_ns = file_mtime_unix_ns(&meta);
    let mtime_unix = mtime_unix_ns.div_euclid(1_000_000_000);
    let keep_bytes = record.kind == Kind::Sidecar;

    if resume.is_none() && size < cfg.multipart_threshold {
        // --- single buffered PUT with Content-MD5 (signed payload) ---
        let body = read_range(chunks, source, 0, size).await?;
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
            sidecar_bytes: keep_bytes.then(|| bytes.to_vec()),
            mtime_unix,
        });
    }

    // --- multipart ---
    let resuming = resume.is_some();
    let upload = match resume {
        Some(up) => {
            // §2.4 mid-resume source-change recheck (size + mtime against
            // the record the upload describes). A change here means the
            // un-sent parts of the *old* version are no longer obtainable:
            // abort + re-mark dirty (see the module docs for why this
            // differs from the detected-at-completion rule).
            if size != record.size || mtime_unix_ns != record.mtime_unix_ns {
                s3.abort_multipart_upload(&cfg.bucket, key, &up.upload_id)
                    .await?;
                db.with_txn(|t| {
                    t.clear_upload(relkey)?;
                    t.transition(relkey, ItemState::Uploading, ItemState::Dirty, |_| {})?;
                    Ok(())
                })?;
                return Err(TransferError::AbortedSourceChanged {
                    relkey: relkey.clone(),
                });
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
    // with the recorded ETag. A failed listing falls back to the records.
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
        hasher.update(&buf);
        if let Some(acc) = sidecar_bytes.as_mut() {
            acc.extend_from_slice(&buf);
        }
        if let Some(rec) = done {
            completed.push(CompletedPart {
                part_number: part_no,
                e_tag: rec.etag.clone(),
            });
            continue;
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
            Err(first) => return Err(first.into()),
        };
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
    let done = s3
        .complete_multipart_upload(&cfg.bucket, key, &upload.upload_id, &completed)
        .await?;
    Ok(SentObject {
        blake3: Blake3Hex::from_hash(&hasher.finalize()),
        size,
        e_tag: done.e_tag,
        multipart: true,
        md5_hex: None,
        sidecar_bytes,
        mtime_unix,
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
        // PUT's stored ETag must still equal our MD5.
        if let Some(md5_hex) = &sent.md5_hex {
            if head.e_tag != *md5_hex {
                return Err(TransferError::CorruptRemote {
                    relkey: relkey.clone(),
                    detail: format!("stored ETag {} != sent MD5 {md5_hex}", head.e_tag),
                });
            }
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
    let mut report = StaleUploadReport::default();
    // Our uploads table is the orphan-matching key set regardless of age:
    // a backend upload on one of OUR keys under a different id can never
    // be completed by anyone.
    let mut own_keys: BTreeMap<String, String> = BTreeMap::new();
    for (relkey, upload) in db.iter_uploads()? {
        let Some(record) = db.get_item(&relkey)? else {
            // An uploads row without an item record cannot derive a bucket
            // key; leave it for corruption recovery.
            continue;
        };
        let key = bucket_key_for(&relkey, record.kind)?;
        own_keys.insert(key.clone(), upload.upload_id.clone());
        if upload.started_unix.saturating_add(max_age_secs) > now_unix {
            continue; // still fresh
        }
        // Aged out: abort on the backend, clear locally, and — when the
        // item still sits in `uploading` — re-mark it dirty (§2.4 "upload
        // abandoned"). A backend that no longer knows the upload id
        // (already aborted/completed elsewhere) is treated as done.
        match s3
            .abort_multipart_upload(&cfg.bucket, &key, &upload.upload_id)
            .await
        {
            Ok(()) => {}
            Err(e) if e.code() == Some(&crate::s3::S3ErrorCode::NoSuchUpload) => {}
            Err(e) => return Err(e.into()),
        }
        db.with_txn(|t| {
            t.clear_upload(&relkey)?;
            Ok(())
        })?;
        if record.state == ItemState::Uploading {
            demote(db, &relkey, ItemState::Uploading, ItemState::Dirty);
        }
        report.aborted_own.push((relkey, upload.upload_id));
    }

    // Backend scan for own-key orphans (paged). Uploads on keys we hold
    // no record for are another device's business (§1.2) and are left
    // alone.
    let mut request = ListMultipartUploadsRequest::default();
    loop {
        let page = s3.list_multipart_uploads(&cfg.bucket, &request).await?;
        for upload in page.uploads {
            let orphan = own_keys
                .get(&upload.key)
                .is_some_and(|ours| *ours != upload.upload_id);
            if orphan {
                s3.abort_multipart_upload(&cfg.bucket, &upload.key, &upload.upload_id)
                    .await?;
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
    let record = db
        .get_item(relkey)?
        .ok_or_else(|| TransferError::MissingItem {
            relkey: relkey.clone(),
        })?;
    let kind = record.kind;
    let key = bucket_key_for(relkey, kind)?;
    let final_path = local_target_path(dest_root, relkey, kind);
    let partial = partial_path(&final_path);

    // Entry: `pending_down`/`stub` → `downloading`, or crash-recovery
    // re-entry already at `downloading` (with a surviving partial). Any
    // other state fails the CAS/legality check typed.
    if record.state != ItemState::Downloading {
        db.transition(relkey, record.state, ItemState::Downloading, |_| {})?;
    }

    match run_download(
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
    .await
    {
        Ok(outcome) => Ok(outcome),
        // Integrity/parse failures are properties of the remote object:
        // the partial is deleted (it can never verify) and the item moves
        // to the corrupt_remote lane.
        Err(
            e @ (TransferError::IntegrityMismatch { .. } | TransferError::SidecarInvalid { .. }),
        ) => {
            let _ = std::fs::remove_file(&partial);
            demote(db, relkey, ItemState::Downloading, ItemState::CorruptRemote);
            Err(e)
        }
        // Everything else (transport, local I/O) keeps the partial for a
        // ranged resume and returns the item to `pending_down`.
        Err(e) => {
            demote(db, relkey, ItemState::Downloading, ItemState::PendingDown);
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
        std::fs::create_dir_all(parent).map_err(|e| io_err(parent, e))?;
    }
    let mut hasher = blake3::Hasher::new();
    let mut resumed_from = 0u64;
    match std::fs::metadata(partial) {
        Ok(meta) if meta.len() > expected.size => {
            // An over-long partial can never verify: start fresh.
            std::fs::remove_file(partial).map_err(|e| io_err(partial, e))?;
        }
        Ok(meta) => {
            // §3.5 resume: re-hash the surviving bytes from disk, then
            // continue with a ranged GET from exactly this offset.
            resumed_from = meta.len();
            hash_file_into(partial, &mut hasher)?;
        }
        Err(_) => {}
    }

    let mut bytes_fetched = 0u64;
    if resumed_from < expected.size {
        let range = (resumed_from > 0).then_some(ByteRange::From(resumed_from));
        let out = s3.get_object(&cfg.bucket, key, range).await?;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(partial)
            .map_err(|e| io_err(partial, e))?;
        let mut body = out.body;
        loop {
            match body.next().await {
                Some(Ok(chunk)) => {
                    use std::io::Write as _;
                    file.write_all(&chunk).map_err(|e| io_err(partial, e))?;
                    hasher.update(&chunk);
                    bytes_fetched += chunk.len() as u64;
                }
                Some(Err(e)) => {
                    // Every received byte is already appended: the partial
                    // survives for the next ranged resume.
                    let _ = file.sync_all();
                    return Err(e.into());
                }
                None => break,
            }
        }
        file.sync_all().map_err(|e| io_err(partial, e))?;
    }

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
        let bytes = std::fs::read(partial).map_err(|e| io_err(partial, e))?;
        if let Err(source) = sem_hash(&bytes) {
            return Err(TransferError::SidecarInvalid {
                relkey: relkey.clone(),
                source,
            });
        }
    }

    // Atomic install (same directory) + mtime restore (§3.5: keeps the
    // thumbnail cache hash stable across hydration).
    std::fs::rename(partial, final_path).map_err(|e| io_err(final_path, e))?;
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
/// as failed (never installed unverified).
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
/// (`corrupt_remote`, re-marked `dirty`) never loops. A fired
/// [`CancelFlag`] stops admission; in-flight items are always awaited.
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
                summary.failed.push((relkey.clone(), e.to_string()));
                requeue.push((relkey, class));
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

/// Best-effort state demotion on a failure path. The primary error stays
/// primary: a CAS loss here means a concurrent writer already moved the
/// item (its state is *its* responsibility now) and must not mask what
/// actually failed.
fn demote(db: &SyncDb, relkey: &RelKey, from: ItemState, to: ItemState) {
    let _ = db.transition(relkey, from, to, |_| {});
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
/// the whole file).
fn hash_file_into(path: &Path, hasher: &mut blake3::Hasher) -> Result<(), TransferError> {
    use std::io::Read as _;
    let mut file = std::fs::File::open(path).map_err(|e| io_err(path, e))?;
    let mut buf = vec![0u8; 256 * 1024];
    loop {
        let n = file.read(&mut buf).map_err(|e| io_err(path, e))?;
        if n == 0 {
            return Ok(());
        }
        hasher.update(&buf[..n]);
    }
}
