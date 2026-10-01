//! The durable local state store: a single redb database file per device
//! (architecture §3.2), holding item sync state (§2.4), journal publication
//! state (§2.1.5, §2.2), apply/cursor bookkeeping, upload resume state,
//! transfer queues, and change-detection caches.
//!
//! # Locking (§5.1) — empirically verified against redb 2.6
//!
//! The app/worker single-instance exclusion rides directly on redb's own
//! database file lock. Measured behavior (Linux, redb 2.6.3):
//!
//! - A second `Database::create`/`open` — from **another process or the
//!   same process** — fails in microseconds with
//!   `DatabaseError::DatabaseAlreadyOpen`. It does **not** block, so no
//!   sidecar try-lock file is needed; [`SyncDb::open`] surfaces this as
//!   [`StateError::AlreadyLocked`].
//! - The lock is a kernel file lock: SIGKILL of the holder releases it
//!   immediately, and the next open runs recovery (~ms) and succeeds.
//! - Dropping the [`SyncDb`] releases the lock promptly (drop + reopen in
//!   one process works).
//!
//! # Durability
//!
//! Every write API here is one write transaction committed with
//! [`redb::Durability::Immediate`] — pinned explicitly on every write
//! transaction (not inherited from redb's default, so a future redb default
//! change cannot silently weaken §3.2). `commit()` fsyncs before returning;
//! redb's commit is single-phase with a non-cryptographic (xxh3) checksum
//! that validates the commit slot on recovery. Strict power-loss write
//! ordering via `WriteTransaction::set_two_phase_commit` exists upstream and
//! is deliberately not enabled in v1.
//!
//! Empirically verified **for process death**: SIGKILL loops against a
//! committing writer showed zero torn or lost committed records (the db may
//! be *ahead* of what the writer observed returning, never behind). Scope
//! honestly stated: SIGKILL cannot distinguish `Immediate` from `Eventual`
//! durability — the page cache is kernel-owned and survives process death —
//! so the experiment proves the process-death story only. The power-loss
//! posture rests on the pinned `Immediate` fsync plus the checksummed
//! commit slot, not on the experiment.
//!
//! `begin_write` from a second thread blocks until the open write
//! transaction commits — writers are serialized, which is what makes
//! [`SyncDb::transition`]'s compare-and-set semantics race-free.
//!
//! # Composite steps and recovery scans
//!
//! Cross-table steps that must be atomic (§2.4 "transition + enqueue",
//! "freeze journal entry + transition to synced") run inside one scoped
//! write transaction via [`SyncDb::with_txn`], which exposes the same typed
//! operations on a [`StateTxn`]; either everything in the closure commits or
//! nothing does. Crash recovery and hygiene scans (§2.4 stale uploads, §3.5
//! LRU eviction, §2.3 reconcile) are served by the enumeration accessors
//! ([`SyncDb::iter_items`], [`SyncDb::items_in_state`],
//! [`SyncDb::count_in_state`], [`SyncDb::iter_uploads`],
//! [`SyncDb::queue_peek`]).
//!
//! # Encoding
//!
//! Record values (items, upload state, parts) are serialized as JSON inside
//! redb values — deliberately, for v1 debuggability (`redb` dump + `jq`
//! works). The encoding is an internal detail behind the typed accessors and
//! can be swapped (e.g. for a binary codec) without touching callers or the
//! public schema version. Raw redb table names are likewise internal.

use std::path::{Path, PathBuf};

use redb::{ReadableTable, ReadableTableMetadata, TableDefinition};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::clock::{DeviceId, VersionVector};
use crate::journal::Kind;
use crate::keys::RelKey;
use crate::semhash::{Blake3Hex, ContentId, SemHash};

/// The state-store schema version this build reads and writes.
///
/// Written into `meta` on first open. v1 performs **no migrations**: a db
/// whose stored version is higher *or* lower/missing fails closed with a
/// typed error ([`StateError::SchemaTooNew`] /
/// [`StateError::SchemaUnsupported`]).
pub const SCHEMA_VERSION: u32 = 1;

/// Error from the state store.
#[derive(Debug, thiserror::Error)]
pub enum StateError {
    /// Another process (or another handle in this process) holds the
    /// database. This is the §5.1 single-instance refusal: it is raised
    /// immediately (redb's lock does not block) so a worker can cleanly
    /// defer to a running app and vice versa.
    #[error("state db is already open in another process: {path}")]
    AlreadyLocked {
        /// The database path that is locked.
        path: PathBuf,
    },
    /// First open of a fresh database with no device id supplied. The store
    /// never generates ids itself (no RNG dependency; determinism in tests)
    /// — the caller mints the UUIDv4 and passes it.
    #[error("fresh state db needs a device id to mint")]
    DeviceIdRequired,
    /// Reopen supplied `given` but the database already minted `stored`.
    /// Device identity is owned by the database once minted; a mismatch is
    /// a caller bug (e.g. pointing two device identities at one db file)
    /// and must fail loudly, not silently keep either id.
    #[error("state db already has device id {stored}, caller supplied {given}")]
    DeviceIdMismatch {
        /// The id persisted in the database.
        stored: DeviceId,
        /// The conflicting id the caller passed.
        given: DeviceId,
    },
    /// The database carries a schema stamp but no device identity record.
    /// Identity is written in the same transaction as the stamp, so no
    /// legal history produces this — it is meta corruption (or a future
    /// layout that moved identity). Never treated as fresh: minting here
    /// would silently fork identity on — and overwrite the stamp of — a
    /// file that already had one.
    #[error("state db has a schema stamp but no device identity (corrupt meta)")]
    DeviceIdMissing,
    /// The database was written by a newer schema than this build supports.
    /// Fail closed (§2.2 min-reader spirit): never guess at future tables.
    /// This gate runs **before** any identity logic, so a newer-format file
    /// can never be mistaken for fresh and re-stamped (see
    /// [`SyncDb::open`]).
    #[error("state db schema version {found} is newer than supported {supported}")]
    SchemaTooNew {
        /// The version found in `meta`.
        found: u32,
        /// This build's [`SCHEMA_VERSION`].
        supported: u32,
    },
    /// The database's schema version is missing, unreadable, or lower than
    /// [`SCHEMA_VERSION`]. v1 ships no migrations, so anything but an exact
    /// match is refused (a missing version on a non-fresh file reads as
    /// corruption, not as "old version").
    #[error("state db schema version {found:?} is not supported (want exactly {SCHEMA_VERSION})")]
    SchemaUnsupported {
        /// The version found, or `None` when absent/unreadable.
        found: Option<u32>,
    },
    /// `transition` was asked for a `(from, to)` pair the §2.4 state machine
    /// does not allow. Checked statically against [`legal`] **before** the
    /// stored state is consulted, so an illegal request is reported as
    /// illegal even when the stored state also happens to differ.
    #[error("illegal item state transition {from:?} -> {to:?} for {relkey}")]
    IllegalTransition {
        /// The item.
        relkey: RelKey,
        /// The `expected_from` the caller passed.
        from: ItemState,
        /// The target state the caller passed.
        to: ItemState,
    },
    /// `transition`/`update_item`'s expected state did not match the stored
    /// state (a concurrent transition won, or the item does not exist).
    /// Nothing was mutated.
    #[error("stale item state for {relkey}: expected {expected:?}, found {found:?}")]
    StaleState {
        /// The item.
        relkey: RelKey,
        /// What the caller expected.
        expected: ItemState,
        /// What is actually stored (`None`: no record at all).
        found: Option<ItemState>,
    },
    /// `freeze_segment` for a seq that [`SyncDb::allocate_seq`] never
    /// returned. Freezing is only meaningful for allocated seqs; this is an
    /// engine bug surfaced loudly.
    #[error("segment seq {seq} was never allocated")]
    SeqNotAllocated {
        /// The offending seq.
        seq: u64,
    },
    /// `freeze_segment` for a seq that already has frozen bytes. A segment
    /// is frozen exactly once (§2.1.5: the frozen bytes *are* the segment's
    /// identity); crash replay re-reads them, it never re-freezes.
    #[error("segment seq {seq} is already frozen")]
    AlreadyFrozen {
        /// The offending seq.
        seq: u64,
    },
    /// `mark_published` for a seq that was never frozen. Publishing an
    /// unfrozen segment would break the §2.1.5 durability order (bytes
    /// frozen before any network).
    #[error("segment seq {seq} is not frozen, cannot mark published")]
    NotFrozen {
        /// The offending seq.
        seq: u64,
    },
    /// A persisted monotonic counter sits at `u64::MAX` and the next
    /// increment would wrap. Unreachable by honest operation (2^64
    /// commits); hitting it means the stored counter was corrupted or
    /// tampered with, so the store refuses with a typed error instead of a
    /// debug-build panic or a release-mode silent wrap — a wrapped
    /// `last_seq` would hand out regressed seqs, the exact §2.2 invariant
    /// this store exists to protect.
    #[error("state db counter {key:?} is saturated at u64::MAX (corrupt meta?)")]
    CounterSaturated {
        /// The internal meta key of the saturated counter.
        key: &'static str,
    },
    /// Underlying redb failure (I/O, corruption, poisoned txn). Boxed:
    /// `redb::Error` is ~160 bytes and would dominate every `Result` in
    /// this module (clippy `result_large_err`).
    #[error("state db storage error: {0}")]
    Db(#[from] Box<redb::Error>),
    /// A stored record failed to encode/decode. On the read side this means
    /// the value bytes do not parse as the record schema — surfaced as a
    /// typed error, never a panic (library paths do not panic).
    #[error("state db record encoding error: {0}")]
    Codec(#[from] serde_json::Error),
}

impl From<redb::Error> for StateError {
    fn from(e: redb::Error) -> Self {
        StateError::Db(Box::new(e))
    }
}

/// Wraps any specific redb error (transaction, table, storage, commit, …)
/// into the boxed [`StateError::Db`] umbrella.
fn db_err(e: impl Into<redb::Error>) -> StateError {
    StateError::Db(Box::new(e.into()))
}

// ---------------------------------------------------------------------------
// Table definitions (internal; the typed accessors are the public surface)
// ---------------------------------------------------------------------------

/// `items`: relkey -> JSON [`ItemRecord`].
const T_ITEMS: TableDefinition<&str, &[u8]> = TableDefinition::new("items");
/// `applied`: (device id, seq) -> () — §2.2 idempotent-apply dedup set.
const T_APPLIED: TableDefinition<(&str, u64), ()> = TableDefinition::new("applied");
/// `cursors`: device id -> highest contiguously-applied seq.
const T_CURSORS: TableDefinition<&str, u64> = TableDefinition::new("cursors");
/// `pending_segments`: seq -> frozen segment bytes (§2.1.5).
const T_SEGMENTS: TableDefinition<u64, &[u8]> = TableDefinition::new("pending_segments");
/// `published_segments`: seq -> () — per-seq published flag (out-of-order
/// publish support; the max also lives in meta as the published cursor).
const T_PUBLISHED: TableDefinition<u64, ()> = TableDefinition::new("published_segments");
/// `uploads`: relkey -> JSON [`MultipartUploadState`].
const T_UPLOADS: TableDefinition<&str, &[u8]> = TableDefinition::new("uploads");
/// `upload_parts`: (relkey, part number) -> JSON [`UploadPart`].
const T_UPLOAD_PARTS: TableDefinition<(&str, u32), &[u8]> = TableDefinition::new("upload_parts");
/// `queue_up` entries: (class, arrival counter) -> relkey. The key encoding
/// makes a plain ascending range scan yield priority-then-FIFO order.
const T_QUEUE_UP: TableDefinition<(u8, u64), &str> = TableDefinition::new("queue_up");
/// `queue_up` index: relkey -> (class, arrival counter) — idempotent-push
/// membership + O(log n) removal.
const T_QUEUE_UP_IDX: TableDefinition<&str, (u8, u64)> = TableDefinition::new("queue_up_idx");
/// `queue_down` entries (same encoding as `queue_up`).
const T_QUEUE_DOWN: TableDefinition<(u8, u64), &str> = TableDefinition::new("queue_down");
/// `queue_down` index.
const T_QUEUE_DOWN_IDX: TableDefinition<&str, (u8, u64)> = TableDefinition::new("queue_down_idx");
/// `xmp_seen`: relkey -> blake3 hex of the last-imported sidecar bytes.
const T_XMP_SEEN: TableDefinition<&str, &str> = TableDefinition::new("xmp_seen");
/// `dcim_seen`: (source path, size, mtime) -> content id hex.
const T_DCIM_SEEN: TableDefinition<(&str, u64, i64), &str> = TableDefinition::new("dcim_seen");
/// `meta`: string key -> JSON value (device id, schema version, cursors,
/// counters).
const T_META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");

/// Meta keys.
const K_DEVICE_ID: &str = "device_id";
const K_SCHEMA_VERSION: &str = "schema_version";
const K_LAST_SEQ: &str = "last_seq";
const K_PUBLISHED_CURSOR: &str = "published_cursor";
/// Highest seq `F` such that every seq in `1..=F` is frozen **and**
/// published — the contiguous published prefix. `unpublished_segments`
/// starts its scan at `F + 1`, so a long-lived device's publish pass costs
/// O(pending), not O(all segments ever frozen). Maintained by
/// `mark_published`; an allocated-never-frozen hole (possible only via the
/// legacy `allocate_seq` + `freeze_segment` pair, never via
/// `freeze_next_segment`) blocks the floor but not correctness.
const K_PUBLISHED_FLOOR: &str = "published_floor";
const K_QUEUE_ARRIVAL: &str = "queue_arrival";
const K_SERVER_TIME_OFFSET_MS: &str = "server_time_offset_ms";

// ---------------------------------------------------------------------------
// Encoding helpers
// ---------------------------------------------------------------------------

fn to_json<T: Serialize>(value: &T) -> Result<Vec<u8>, StateError> {
    Ok(serde_json::to_vec(value)?)
}

fn from_json<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, StateError> {
    Ok(serde_json::from_slice(bytes)?)
}

/// Rehydrates a validating string newtype ([`RelKey`], [`Blake3Hex`],
/// [`ContentId`], [`DeviceId`]) from its stored string form through its own
/// `Deserialize` impl, so the type's validation runs and a corrupt stored
/// value surfaces as [`StateError::Codec`], never a panic.
fn from_stored_str<T: DeserializeOwned>(s: &str) -> Result<T, StateError> {
    Ok(serde_json::from_value(serde_json::Value::String(
        s.to_owned(),
    ))?)
}

/// Reads a JSON-encoded meta value (`None` when the key is absent).
fn meta_get<T: DeserializeOwned>(
    meta: &impl ReadableTable<&'static str, &'static [u8]>,
    key: &str,
) -> Result<Option<T>, StateError> {
    match meta.get(key).map_err(db_err)? {
        Some(guard) => Ok(Some(from_json(guard.value())?)),
        None => Ok(None),
    }
}

/// Per-item sync state (§2.4 upload rows + §3.5 download/hydration rows).
///
/// Mapping to the design doc:
///
/// | Variant | Doc row |
/// |---|---|
/// | [`Dirty`](ItemState::Dirty) | §2.4 `dirty` — local change detected |
/// | [`Queued`](ItemState::Queued) | §2.4 `queued` — in upload queue |
/// | [`Uploading`](ItemState::Uploading) | §2.4 `uploading` |
/// | [`Verifying`](ItemState::Verifying) | §2.4 `verifying` |
/// | [`Synced`](ItemState::Synced) | §2.4 `synced` |
/// | [`CorruptRemote`](ItemState::CorruptRemote) | §2.4 `corrupt_remote` |
/// | [`Conflict`](ItemState::Conflict) | §2.4 `conflict` (§2.6) |
/// | [`PendingDown`](ItemState::PendingDown) | §3.5 `pending_down` — advertised remotely, bytes not yet local |
/// | [`Downloading`](ItemState::Downloading) | §3.5 — a download slot holds it |
/// | [`Stub`](ItemState::Stub) | §3.5 — evicted original, cloud placeholder |
/// | [`Hydrated`](ItemState::Hydrated) | §3.5 — original fully present and verified locally |
///
/// Serialized in `snake_case` to match the doc's spelling
/// (`corrupt_remote`, `pending_down`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ItemState {
    /// Local change detected, not yet queued.
    Dirty,
    /// In the upload queue.
    Queued,
    /// A transfer slot holds it (single PUT or multipart).
    Uploading,
    /// Upload finished; integrity check against the remote in progress.
    Verifying,
    /// Local and remote agree; `verified_remote` set by the verify step.
    Synced,
    /// Remote object exists but its content is provably wrong. Never
    /// evicted, never served; repair flow re-uploads or re-downloads.
    CorruptRemote,
    /// Provably concurrent versions exist (§2.6); resolution pending.
    Conflict,
    /// A remote version is advertised that we do not have locally.
    PendingDown,
    /// A download slot holds it.
    Downloading,
    /// Evicted original: placeholder on disk, verified copy in the cloud.
    Stub,
    /// Original fully downloaded and blake3-verified locally.
    Hydrated,
}

impl ItemState {
    /// Every state, for exhaustive table tests.
    pub const ALL: [ItemState; 11] = [
        ItemState::Dirty,
        ItemState::Queued,
        ItemState::Uploading,
        ItemState::Verifying,
        ItemState::Synced,
        ItemState::CorruptRemote,
        ItemState::Conflict,
        ItemState::PendingDown,
        ItemState::Downloading,
        ItemState::Stub,
        ItemState::Hydrated,
    ];
}

/// The §2.4 legal-transition predicate — the single authority consulted by
/// [`SyncDb::transition`]; the table exists exactly once, here.
///
/// The legal edges (everything else, including every self-loop, is
/// illegal; state-preserving durable mutations go through
/// [`SyncDb::update_item`], not a self-transition):
///
/// | From | To | Why |
/// |---|---|---|
/// | `Dirty` | `Queued` | debouncer enqueues (§2.4) |
/// | `Dirty` | `Conflict` | concurrent remote version met local dirt (§2.6) |
/// | `Queued` | `Uploading` | transfer slot acquired |
/// | `Queued` | `Dirty` | dequeued (e.g. shutdown drain re-marks) |
/// | `Queued` | `Conflict` | concurrent remote entry vs the committed queued version (§2.6 case 4; vv bumps at queue admission, §3.7, so a queued version is a committed concurrent version) |
/// | `Uploading` | `Verifying` | PUT/CompleteMultipartUpload done |
/// | `Uploading` | `Queued` | transfer failed, requeue with backoff |
/// | `Uploading` | `Dirty` | upload abandoned (e.g. stale upload hygiene) |
/// | `Uploading` | `Conflict` | concurrent remote entry arrived mid-transfer (§2.6 case 4) |
/// | `Verifying` | `Synced` | integrity confirmed; journal entry written |
/// | `Verifying` | `Queued` | verify failed on our own upload → retry |
/// | `Verifying` | `CorruptRemote` | backend-accepted-but-wrong (readback mismatch) |
/// | `Verifying` | `Conflict` | concurrent remote entry arrived during verify (§2.6 case 4) |
/// | `Synced` | `Dirty` | new local change |
/// | `Synced` | `Conflict` | concurrent versions discovered |
/// | `Synced` | `PendingDown` | remote advertised a newer version |
/// | `Synced` | `CorruptRemote` | reconcile found remote content wrong |
/// | `Synced` | `Stub` | eviction (attestation-gated, §3.5) |
/// | `CorruptRemote` | `Queued` | repair: we hold verified bytes, re-upload |
/// | `CorruptRemote` | `PendingDown` | repair landed elsewhere, fetch it |
/// | `Conflict` | `Dirty` | resolved toward local → re-upload |
/// | `Conflict` | `PendingDown` | resolved toward remote → fetch |
/// | `PendingDown` | `Downloading` | download slot acquired |
/// | `PendingDown` | `Dirty` | local edit on a not-yet-downloaded head (§3.4 offline path, flagged `base_unknown`) |
/// | `PendingDown` | `Conflict` | the §3.4 chokepoint committed a local version concurrent with the advertised head (§2.6 case 4) |
/// | `Downloading` | `Synced` | sidecar/meta download complete, agree |
/// | `Downloading` | `Hydrated` | original fully fetched + verified |
/// | `Downloading` | `PendingDown` | transfer failed, retry later |
/// | `Downloading` | `CorruptRemote` | hash mismatch on download (§2.4) |
/// | `Stub` | `PendingDown` | hydration requested |
/// | `Stub` | `Downloading` | hydration slot acquired directly |
/// | `Hydrated` | `Stub` | evicted again |
/// | `Hydrated` | `Dirty` | local change to the hydrated item |
/// | `Hydrated` | `PendingDown` | remote advertised a newer version |
/// | `Hydrated` | `CorruptRemote` | local verify against journal failed remotely |
///
/// Note on `Downloading`: a conflict discovered against an item mid-download
/// is recorded after the transfer resolves (`Downloading` →
/// `Synced`/`Hydrated`/`PendingDown` first) — the download slot owns the
/// item until it releases it, mirroring how `Uploading` → `Conflict` is
/// taken by the apply loop, not the transfer task.
pub fn legal(from: ItemState, to: ItemState) -> bool {
    use ItemState::*;
    matches!(
        (from, to),
        (Dirty, Queued)
            | (Dirty, Conflict)
            | (Queued, Uploading)
            | (Queued, Dirty)
            | (Queued, Conflict)
            | (Uploading, Verifying)
            | (Uploading, Queued)
            | (Uploading, Dirty)
            | (Uploading, Conflict)
            | (Verifying, Synced)
            | (Verifying, Queued)
            | (Verifying, CorruptRemote)
            | (Verifying, Conflict)
            | (Synced, Dirty)
            | (Synced, Conflict)
            | (Synced, PendingDown)
            | (Synced, CorruptRemote)
            | (Synced, Stub)
            | (CorruptRemote, Queued)
            | (CorruptRemote, PendingDown)
            | (Conflict, Dirty)
            | (Conflict, PendingDown)
            | (PendingDown, Downloading)
            | (PendingDown, Dirty)
            | (PendingDown, Conflict)
            | (Downloading, Synced)
            | (Downloading, Hydrated)
            | (Downloading, PendingDown)
            | (Downloading, CorruptRemote)
            | (Stub, PendingDown)
            | (Stub, Downloading)
            | (Hydrated, Stub)
            | (Hydrated, Dirty)
            | (Hydrated, PendingDown)
            | (Hydrated, CorruptRemote)
    )
}

/// One item's durable sync record (the `items` table value, §3.2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ItemRecord {
    /// What kind of object this relkey is (original, sidecar, …).
    pub kind: Kind,
    /// Current §2.4 state. Changed **only** through [`SyncDb::transition`]
    /// after the record exists; state-preserving mutations go through
    /// [`SyncDb::update_item`]. The wholesale
    /// [`SyncDb::replay_put_item`] bypass is reserved for ingest/replay
    /// paths and is named to be greppable.
    pub state: ItemState,
    /// Size in bytes of the local file (0 when unknown/stub).
    pub size: u64,
    /// Local file mtime, unix **nanoseconds** (change pre-check, §2.5).
    pub mtime_unix_ns: i64,
    /// blake3 of the last uploaded/verified bytes (journal `blake3`, §2.2).
    pub blake3: Option<Blake3Hex>,
    /// Semantic hash of the sidecar (§2.5); sidecars only.
    pub sem_hash: Option<SemHash>,
    /// Per-relkey version vector (§2.6).
    pub vv: VersionVector,
    /// Content identity of the original's bytes (§1.2); originals only.
    pub content_id: Option<ContentId>,
    /// Final displayed width (measured, never EXIF — §2.2).
    pub w: Option<u32>,
    /// Final displayed height.
    pub h: Option<u32>,
    /// Pinned: never evicted (§3.5).
    pub pinned: bool,
    /// Last local access, unix seconds — LRU eviction order (§3.5).
    pub last_access_unix: u64,
    /// Upload-side integrity held (§2.4 `verifying` passed).
    pub verified_remote: bool,
    /// An `attest` journal entry covers the current version (§3.5 eviction
    /// gate).
    pub attested: bool,
    /// The local edit's remote base version is unknown (device cursor
    /// predates retention; §2.3 pre-upload quarantine input).
    pub base_unknown: bool,
}

/// Multipart upload resume state (the `uploads` table value, §2.4).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MultipartUploadState {
    /// The backend's `UploadId`.
    pub upload_id: String,
    /// Part size in bytes chosen at creation (16 MiB default, §2.4).
    pub part_size: u64,
    /// When the upload started, unix seconds (stale-upload hygiene: abort
    /// after 7 days, §2.4).
    pub started_unix: i64,
}

/// One completed part of a multipart upload (the `upload_parts` value).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadPart {
    /// The ETag the backend returned for the part.
    pub etag: String,
    /// The base64 `Content-MD5` we sent (resume + Complete validation).
    pub md5_b64: String,
}

/// Which transfer queue (§3.2 `queue_up` / `queue_down`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Queue {
    /// Upload queue.
    Up,
    /// Download queue.
    Down,
}

/// A queue's entry table: (class, arrival counter) -> relkey.
type QueueEntriesDef = TableDefinition<'static, (u8, u64), &'static str>;
/// A queue's index table: relkey -> (class, arrival counter).
type QueueIndexDef = TableDefinition<'static, &'static str, (u8, u64)>;

impl Queue {
    /// This queue's (entries, index) table pair.
    fn tables(self) -> (QueueEntriesDef, QueueIndexDef) {
        match self {
            Queue::Up => (T_QUEUE_UP, T_QUEUE_UP_IDX),
            Queue::Down => (T_QUEUE_DOWN, T_QUEUE_DOWN_IDX),
        }
    }
}

/// The head of a queue — lowest class, then earliest arrival — without
/// removing it. Shared by peek (read txn) and pop (write txn).
fn queue_head(
    entries: &impl ReadableTable<(u8, u64), &'static str>,
) -> Result<Option<(u8, u64, String)>, StateError> {
    match entries.first().map_err(db_err)? {
        None => Ok(None),
        Some((key, value)) => {
            let (class, arrival) = key.value();
            Ok(Some((class, arrival, value.value().to_owned())))
        }
    }
}

/// Reads an item record from any readable `items` table.
fn read_item(
    items: &impl ReadableTable<&'static str, &'static [u8]>,
    relkey: &RelKey,
) -> Result<Option<ItemRecord>, StateError> {
    match items.get(relkey.as_str()).map_err(db_err)? {
        Some(guard) => Ok(Some(from_json(guard.value())?)),
        None => Ok(None),
    }
}

/// The device's durable sync state database (§3.2).
///
/// One per device, at `app_data_dir/rrcloud/state.redb`. Opening takes the
/// redb file lock for the life of the value; a second open anywhere fails
/// fast with [`StateError::AlreadyLocked`] (§5.1). All methods take `&self`
/// and are safe to call from multiple threads: redb serializes write
/// transactions internally (empirical note in the module docs).
///
/// Every mutating method is **one committed write transaction**: when it
/// returns `Ok`, the change is durable (survives SIGKILL). Steps spanning
/// several operations that must commit together go through
/// [`SyncDb::with_txn`].
#[derive(Debug)]
pub struct SyncDb {
    db: redb::Database,
    device_id: DeviceId,
    path: PathBuf,
}

/// A scoped write transaction over the state store (see
/// [`SyncDb::with_txn`]). Exposes the same typed operations as [`SyncDb`];
/// everything performed through one `StateTxn` commits atomically, or — when
/// the closure returns an error — nothing does.
pub struct StateTxn<'a> {
    txn: &'a redb::WriteTransaction,
}

impl SyncDb {
    /// Opens (or creates) the state database at `path`.
    ///
    /// The open gate is keyed on the **schema stamp first** (fail closed —
    /// a v1 binary must never mint into, or re-stamp, a file written by any
    /// other format):
    ///
    /// 1. No schema stamp and no device identity → fresh file: stores
    ///    `schema_version =` [`SCHEMA_VERSION`] and mints the identity from
    ///    `mint_device_id` — which must be `Some` then
    ///    ([`StateError::DeviceIdRequired`] otherwise; the store never
    ///    generates randomness).
    /// 2. A schema stamp other than exactly [`SCHEMA_VERSION`] (newer,
    ///    older, or unreadable) → [`StateError::SchemaTooNew`] /
    ///    [`StateError::SchemaUnsupported`], **before any identity logic
    ///    runs** — so a newer-format file whose identity lives elsewhere is
    ///    refused, never treated as fresh.
    /// 3. A schema stamp without a device identity →
    ///    [`StateError::DeviceIdMissing`] (meta corruption; never a mint).
    /// 4. No schema stamp but an identity present →
    ///    [`StateError::SchemaUnsupported`] (corruption, not "old
    ///    version").
    ///
    /// Reopening an initialized db ignores a `None` and verifies a
    /// `Some(id)` against the stored identity
    /// ([`StateError::DeviceIdMismatch`] on conflict).
    ///
    /// Fails fast and typed when the db is held elsewhere
    /// ([`StateError::AlreadyLocked`]).
    pub fn open(
        path: impl AsRef<Path>,
        mint_device_id: Option<DeviceId>,
    ) -> Result<Self, StateError> {
        let path = path.as_ref().to_path_buf();
        let db = match redb::Database::create(&path) {
            Ok(db) => db,
            Err(redb::DatabaseError::DatabaseAlreadyOpen) => {
                return Err(StateError::AlreadyLocked { path })
            }
            Err(e) => return Err(db_err(e)),
        };

        // One write transaction: inspect/initialize meta, then make sure
        // every table exists so read paths never see a missing table. A
        // refusal below drops the txn un-committed, leaving a fresh file
        // untouched (a later open with an id can still mint) and an
        // initialized file byte-for-byte as found (never re-stamped).
        let mut txn = db.begin_write().map_err(db_err)?;
        txn.set_durability(redb::Durability::Immediate);
        let device_id = {
            let mut meta = txn.open_table(T_META).map_err(db_err)?;
            // Schema stamp first. `Some(None)` = present but unreadable.
            let schema: Option<Option<u32>> = meta
                .get(K_SCHEMA_VERSION)
                .map_err(db_err)?
                .map(|guard| serde_json::from_slice(guard.value()).ok());
            let stored: Option<DeviceId> = match meta.get(K_DEVICE_ID).map_err(db_err)? {
                Some(guard) => Some(from_json(guard.value())?),
                None => None,
            };
            match schema {
                // No stamp at all.
                None => match stored {
                    // Identity without a stamp: corruption, not fresh.
                    Some(_) => return Err(StateError::SchemaUnsupported { found: None }),
                    // Fresh database: stamp the schema + mint identity.
                    None => {
                        let minted = mint_device_id.ok_or(StateError::DeviceIdRequired)?;
                        meta.insert(K_SCHEMA_VERSION, to_json(&SCHEMA_VERSION)?.as_slice())
                            .map_err(db_err)?;
                        meta.insert(K_DEVICE_ID, to_json(&minted)?.as_slice())
                            .map_err(db_err)?;
                        minted
                    }
                },
                // Stamp present but unreadable.
                Some(None) => return Err(StateError::SchemaUnsupported { found: None }),
                Some(Some(v)) if v > SCHEMA_VERSION => {
                    return Err(StateError::SchemaTooNew {
                        found: v,
                        supported: SCHEMA_VERSION,
                    });
                }
                Some(Some(v)) if v < SCHEMA_VERSION => {
                    return Err(StateError::SchemaUnsupported { found: Some(v) });
                }
                // Exact match: identity must exist; verify a supplied id.
                Some(Some(_)) => {
                    let stored = stored.ok_or(StateError::DeviceIdMissing)?;
                    if let Some(given) = mint_device_id {
                        if given != stored {
                            return Err(StateError::DeviceIdMismatch { stored, given });
                        }
                    }
                    stored
                }
            }
        };
        // Create every other table (no-ops when they already exist).
        txn.open_table(T_ITEMS).map_err(db_err)?;
        txn.open_table(T_APPLIED).map_err(db_err)?;
        txn.open_table(T_CURSORS).map_err(db_err)?;
        txn.open_table(T_SEGMENTS).map_err(db_err)?;
        txn.open_table(T_PUBLISHED).map_err(db_err)?;
        txn.open_table(T_UPLOADS).map_err(db_err)?;
        txn.open_table(T_UPLOAD_PARTS).map_err(db_err)?;
        txn.open_table(T_QUEUE_UP).map_err(db_err)?;
        txn.open_table(T_QUEUE_UP_IDX).map_err(db_err)?;
        txn.open_table(T_QUEUE_DOWN).map_err(db_err)?;
        txn.open_table(T_QUEUE_DOWN_IDX).map_err(db_err)?;
        txn.open_table(T_XMP_SEEN).map_err(db_err)?;
        txn.open_table(T_DCIM_SEEN).map_err(db_err)?;
        txn.commit().map_err(db_err)?;

        Ok(SyncDb {
            db,
            device_id,
            path,
        })
    }

    /// This database's device identity (minted on first open).
    pub fn device_id(&self) -> &DeviceId {
        &self.device_id
    }

    /// The database file path this store was opened at.
    pub fn path(&self) -> &Path {
        &self.path
    }

    fn begin_read(&self) -> Result<redb::ReadTransaction, StateError> {
        self.db.begin_read().map_err(db_err)
    }

    /// Begins a write transaction with durability pinned to
    /// [`redb::Durability::Immediate`] (module docs: pinned explicitly so a
    /// future redb default change cannot weaken §3.2).
    fn begin_write(&self) -> Result<redb::WriteTransaction, StateError> {
        let mut txn = self.db.begin_write().map_err(db_err)?;
        txn.set_durability(redb::Durability::Immediate);
        Ok(txn)
    }

    /// Runs `f` inside **one** write transaction and commits iff it returns
    /// `Ok`. On `Err`, the transaction is dropped and **nothing** `f` did is
    /// visible — this is the §2.4 composite-step primitive ("transition +
    /// enqueue", "freeze journal entry + transition", "pop + transition")
    /// that closes the torn-cross-table crash windows a sequence of
    /// single-operation commits would leave.
    ///
    /// The closure must not call back into `self`'s own single-operation
    /// methods (they would block on the open transaction); it uses the
    /// [`StateTxn`] it is handed.
    pub fn with_txn<T>(
        &self,
        f: impl FnOnce(&StateTxn<'_>) -> Result<T, StateError>,
    ) -> Result<T, StateError> {
        let txn = self.begin_write()?;
        let out = f(&StateTxn { txn: &txn })?;
        txn.commit().map_err(db_err)?;
        Ok(out)
    }

    // -- items ------------------------------------------------------------

    /// Inserts an item record **only if none exists** for `relkey`.
    /// Returns `Ok(true)` when inserted, `Ok(false)` (nothing changed) when
    /// a record already exists. This is the creation path for the state
    /// machine; existing records change only through
    /// [`SyncDb::transition`] / [`SyncDb::update_item`].
    pub fn insert_item(&self, relkey: &RelKey, record: &ItemRecord) -> Result<bool, StateError> {
        self.with_txn(|t| t.insert_item(relkey, record))
    }

    /// Wholesale-replaces (or inserts) an item record **including its
    /// state, bypassing the §2.4 legality table**. Reserved for
    /// ingest/replay paths (manifest bootstrap, journal replay) — named to
    /// be greppable, so every bypass of the state machine is visible and
    /// intentional. State-machine steps go through [`SyncDb::transition`];
    /// creation goes through [`SyncDb::insert_item`].
    pub fn replay_put_item(&self, relkey: &RelKey, record: &ItemRecord) -> Result<(), StateError> {
        self.with_txn(|t| t.replay_put_item(relkey, record))
    }

    /// Reads an item record (`None` when absent). A stored value that does
    /// not parse is [`StateError::Codec`], never a panic.
    pub fn get_item(&self, relkey: &RelKey) -> Result<Option<ItemRecord>, StateError> {
        let txn = self.begin_read()?;
        let items = txn.open_table(T_ITEMS).map_err(db_err)?;
        read_item(&items, relkey)
    }

    /// Deletes an item record; `Ok(true)` when it existed.
    pub fn delete_item(&self, relkey: &RelKey) -> Result<bool, StateError> {
        self.with_txn(|t| t.delete_item(relkey))
    }

    /// Every item record, ascending by relkey. One consistent snapshot.
    ///
    /// This (with [`SyncDb::items_in_state`]) is the crash-recovery and
    /// hygiene scan surface: after a crash, items that were popped from a
    /// queue and transitioned (e.g. to `Uploading`) sit in no queue and are
    /// findable only by scanning. v1 materializes the result (libraries are
    /// bounded; records are small); a streaming iterator can replace this
    /// without changing callers' logic.
    pub fn iter_items(&self) -> Result<Vec<(RelKey, ItemRecord)>, StateError> {
        let txn = self.begin_read()?;
        let items = txn.open_table(T_ITEMS).map_err(db_err)?;
        let mut out = Vec::new();
        for entry in items.iter().map_err(db_err)? {
            let (key, value) = entry.map_err(db_err)?;
            out.push((from_stored_str(key.value())?, from_json(value.value())?));
        }
        Ok(out)
    }

    /// Every item currently in `state`, ascending by relkey (e.g. the §3.5
    /// LRU evictor's candidate scan, or post-crash "find everything stuck
    /// in `Uploading`").
    pub fn items_in_state(
        &self,
        state: ItemState,
    ) -> Result<Vec<(RelKey, ItemRecord)>, StateError> {
        let txn = self.begin_read()?;
        let items = txn.open_table(T_ITEMS).map_err(db_err)?;
        let mut out = Vec::new();
        for entry in items.iter().map_err(db_err)? {
            let (key, value) = entry.map_err(db_err)?;
            let record: ItemRecord = from_json(value.value())?;
            if record.state == state {
                out.push((from_stored_str(key.value())?, record));
            }
        }
        Ok(out)
    }

    /// Number of items currently in `state` (§3.3/§3.8 "N edits not backed
    /// up" / `dirty_unbacked` feed).
    pub fn count_in_state(&self, state: ItemState) -> Result<u64, StateError> {
        let txn = self.begin_read()?;
        let items = txn.open_table(T_ITEMS).map_err(db_err)?;
        let mut count = 0u64;
        for entry in items.iter().map_err(db_err)? {
            let (_, value) = entry.map_err(db_err)?;
            let record: ItemRecord = from_json(value.value())?;
            if record.state == state {
                count += 1;
            }
        }
        Ok(count)
    }

    /// Performs one §2.4 state transition as a single committed write
    /// transaction (compare-and-set on the stored state).
    ///
    /// Check order (pinned by tests):
    ///
    /// 1. `legal(expected_from, to)` — else
    ///    [`StateError::IllegalTransition`] (static check, reported even
    ///    when the stored state also differs).
    /// 2. Record exists — else [`StateError::StaleState`] with
    ///    `found: None`.
    /// 3. Stored state `== expected_from` — else
    ///    [`StateError::StaleState`] with the actual state.
    ///
    /// On success `mutate` runs on the record, then `state` is set to `to`
    /// (overriding anything `mutate` wrote to it), the record is committed,
    /// and the committed record is returned. On any error **nothing is
    /// mutated**.
    ///
    /// Concurrency: redb serializes write transactions, so two racing
    /// transitions execute in some order; the second re-reads the state the
    /// first committed. Two threads attempting the same `expected_from`:
    /// exactly one succeeds, the loser gets [`StateError::StaleState`].
    /// Racing transitions that are *both* legal in sequence (A→B then B→C)
    /// both succeed.
    pub fn transition(
        &self,
        relkey: &RelKey,
        expected_from: ItemState,
        to: ItemState,
        mutate: impl FnOnce(&mut ItemRecord),
    ) -> Result<ItemRecord, StateError> {
        self.with_txn(|t| t.transition(relkey, expected_from, to, mutate))
    }

    /// Durably mutates an item record **without changing its state**, with
    /// the same compare-and-set protection as [`SyncDb::transition`]: the
    /// stored state must equal `expected_state` or
    /// [`StateError::StaleState`] is returned and nothing is mutated.
    ///
    /// This is the §2.6/§3.5 state-preserving write path — converged-apply
    /// vv max + metadata adoption on a `Synced` item, setting `attested`
    /// when an attest entry arrives, bumping `last_access_unix`, setting
    /// `base_unknown` — which a bare `get_item` + `replay_put_item` pair
    /// would expose to lost updates (a concurrent legal transition landing
    /// between the read and the write would be silently stomped; with this
    /// method the stale writer gets [`StateError::StaleState`] instead and
    /// re-reads).
    ///
    /// `mutate` may touch **any field except `state`** — a `state` written
    /// by `mutate` is overridden back to `expected_state` (state changes go
    /// through [`SyncDb::transition`], the single legality authority).
    /// Returns the committed record.
    pub fn update_item(
        &self,
        relkey: &RelKey,
        expected_state: ItemState,
        mutate: impl FnOnce(&mut ItemRecord),
    ) -> Result<ItemRecord, StateError> {
        self.with_txn(|t| t.update_item(relkey, expected_state, mutate))
    }

    // -- applied / cursors (§2.2 idempotent apply) -------------------------

    /// `true` when `(device, seq)` was already applied.
    pub fn has_applied(&self, device: &DeviceId, seq: u64) -> Result<bool, StateError> {
        let txn = self.begin_read()?;
        let applied = txn.open_table(T_APPLIED).map_err(db_err)?;
        Ok(applied
            .get((device.as_str(), seq))
            .map_err(db_err)?
            .is_some())
    }

    /// Records `(device, seq)` as applied. Idempotent: re-marking is a
    /// committed no-op, not an error.
    pub fn mark_applied(&self, device: &DeviceId, seq: u64) -> Result<(), StateError> {
        self.with_txn(|t| t.mark_applied(device, seq))
    }

    /// The highest contiguously-applied seq recorded for `device`'s journal
    /// prefix (0 when none).
    pub fn cursor(&self, device: &DeviceId) -> Result<u64, StateError> {
        let txn = self.begin_read()?;
        let cursors = txn.open_table(T_CURSORS).map_err(db_err)?;
        Ok(cursors
            .get(device.as_str())
            .map_err(db_err)?
            .map(|guard| guard.value())
            .unwrap_or(0))
    }

    /// Sets `device`'s cursor.
    pub fn set_cursor(&self, device: &DeviceId, seq: u64) -> Result<(), StateError> {
        self.with_txn(|t| t.set_cursor(device, seq))
    }

    // -- journal publication (§2.1.5) --------------------------------------

    /// Allocates the next seq and freezes `build(seq)` as its segment bytes
    /// **in one committed transaction** — the §2.1.5 "persisted
    /// transactionally with the serialized segment bytes" primitive, and
    /// the engine's publication path. Because the counter increment and the
    /// bytes commit together, a crash can never leave an
    /// allocated-but-never-frozen seq: **holes are impossible by
    /// construction**, so a remote reader's contiguity cursor (§2.2/§3.2)
    /// can always eventually advance past every seq this device publishes.
    ///
    /// The builder receives the seq (entries embed it, §2.2) and must be
    /// infallible and side-effect-free — it runs inside the open
    /// transaction. Returns the allocated seq. Seqs are strictly
    /// increasing, starting at 1, across threads **and** across
    /// crash/reopen.
    pub fn freeze_next_segment(
        &self,
        build: impl FnOnce(u64) -> Vec<u8>,
    ) -> Result<u64, StateError> {
        self.with_txn(|t| t.freeze_next_segment(build))
    }

    /// Allocates and durably commits the next journal seq for this device,
    /// **without** freezing bytes.
    ///
    /// Strictly increasing, starting at 1; the allocation itself is a
    /// committed transaction, so a seq this method returned is never handed
    /// out again — across threads **and** across crash/reopen (§2.2
    /// monotonicity).
    ///
    /// **Prefer [`SyncDb::freeze_next_segment`]**: a crash between this
    /// allocation and the matching [`SyncDb::freeze_segment`] leaves the
    /// seq a permanent hole, which stalls remote contiguity cursors. This
    /// two-step pair exists for tests and for callers that genuinely need a
    /// seq with no segment; the engine's publication path must use the
    /// single-transaction form.
    pub fn allocate_seq(&self) -> Result<u64, StateError> {
        self.with_txn(|t| t.allocate_seq())
    }

    /// The highest seq allocated so far (0 when none).
    pub fn last_allocated_seq(&self) -> Result<u64, StateError> {
        let txn = self.begin_read()?;
        let meta = txn.open_table(T_META).map_err(db_err)?;
        Ok(meta_get(&meta, K_LAST_SEQ)?.unwrap_or(0))
    }

    /// Durably freezes `bytes` as the segment for an already-allocated
    /// `seq` — the second half of the legacy two-step pair (see
    /// [`SyncDb::allocate_seq`]; new code uses
    /// [`SyncDb::freeze_next_segment`]). The stored bytes are the segment's
    /// identity: a crash-replayed publish re-reads them via
    /// [`SyncDb::unpublished_segments`] byte-identically.
    ///
    /// `seq` must have been allocated ([`StateError::SeqNotAllocated`]) and
    /// not already frozen ([`StateError::AlreadyFrozen`] — a segment is
    /// frozen exactly once, replay never re-freezes).
    pub fn freeze_segment(&self, seq: u64, bytes: &[u8]) -> Result<(), StateError> {
        self.with_txn(|t| t.freeze_segment(seq, bytes))
    }

    /// All frozen-but-unpublished segments, in ascending seq order, each
    /// with the exact bytes passed to [`SyncDb::freeze_segment`] /
    /// [`SyncDb::freeze_next_segment`] (byte identity is the §2.1.5
    /// republish guarantee).
    ///
    /// The scan starts at the contiguous published prefix (the "published
    /// floor" maintained by [`SyncDb::mark_published`]), not at seq 1, so a
    /// publish pass costs O(pending), not O(every segment ever frozen) —
    /// published bytes are retained in v1 (pruning is a later unit's GC
    /// concern) but are not rescanned.
    pub fn unpublished_segments(&self) -> Result<Vec<(u64, Vec<u8>)>, StateError> {
        let txn = self.begin_read()?;
        let meta = txn.open_table(T_META).map_err(db_err)?;
        let floor: u64 = meta_get(&meta, K_PUBLISHED_FLOOR)?.unwrap_or(0);
        let segments = txn.open_table(T_SEGMENTS).map_err(db_err)?;
        let published = txn.open_table(T_PUBLISHED).map_err(db_err)?;
        let mut out = Vec::new();
        for entry in segments.range(floor.saturating_add(1)..).map_err(db_err)? {
            let (key, value) = entry.map_err(db_err)?;
            let seq = key.value();
            if published.get(seq).map_err(db_err)?.is_none() {
                out.push((seq, value.value().to_vec()));
            }
        }
        Ok(out)
    }

    /// Marks a frozen segment as published (the post-PUT §2.1.5 step) and
    /// advances the published cursor to `max(cursor, seq)`.
    ///
    /// Idempotent: re-marking a published seq is a no-op `Ok`. Marking a
    /// never-frozen seq is [`StateError::NotFrozen`]. Out-of-order marking
    /// is allowed (crash replay publishes in order, but the store does not
    /// enforce it); an earlier still-unpublished segment remains in
    /// [`SyncDb::unpublished_segments`]. Frozen bytes are retained after
    /// publish in v1 (pruning is a later unit's GC concern).
    pub fn mark_published(&self, seq: u64) -> Result<(), StateError> {
        self.with_txn(|t| t.mark_published(seq))
    }

    /// The highest published seq (0 when nothing published yet).
    pub fn published_cursor(&self) -> Result<u64, StateError> {
        let txn = self.begin_read()?;
        let meta = txn.open_table(T_META).map_err(db_err)?;
        Ok(meta_get(&meta, K_PUBLISHED_CURSOR)?.unwrap_or(0))
    }

    // -- multipart upload resume (§2.4) ------------------------------------

    /// Stores (or replaces) the multipart state for `relkey`.
    pub fn set_upload(
        &self,
        relkey: &RelKey,
        upload: &MultipartUploadState,
    ) -> Result<(), StateError> {
        self.with_txn(|t| t.set_upload(relkey, upload))
    }

    /// Reads the multipart state for `relkey`.
    pub fn get_upload(&self, relkey: &RelKey) -> Result<Option<MultipartUploadState>, StateError> {
        let txn = self.begin_read()?;
        let uploads = txn.open_table(T_UPLOADS).map_err(db_err)?;
        match uploads.get(relkey.as_str()).map_err(db_err)? {
            Some(guard) => Ok(Some(from_json(guard.value())?)),
            None => Ok(None),
        }
    }

    /// Every in-flight multipart upload, ascending by relkey — the §2.4
    /// stale-upload hygiene scan ("engine aborts its own `upload_id`s older
    /// than 7 days") and the post-crash resume scan.
    pub fn iter_uploads(&self) -> Result<Vec<(RelKey, MultipartUploadState)>, StateError> {
        let txn = self.begin_read()?;
        let uploads = txn.open_table(T_UPLOADS).map_err(db_err)?;
        let mut out = Vec::new();
        for entry in uploads.iter().map_err(db_err)? {
            let (key, value) = entry.map_err(db_err)?;
            out.push((from_stored_str(key.value())?, from_json(value.value())?));
        }
        Ok(out)
    }

    /// Records a completed part. Re-recording a part number replaces it
    /// (a part re-uploaded after a resume has a new ETag).
    pub fn record_upload_part(
        &self,
        relkey: &RelKey,
        part_no: u32,
        part: &UploadPart,
    ) -> Result<(), StateError> {
        self.with_txn(|t| t.record_upload_part(relkey, part_no, part))
    }

    /// All recorded parts for `relkey`, ascending by part number.
    pub fn upload_parts(&self, relkey: &RelKey) -> Result<Vec<(u32, UploadPart)>, StateError> {
        let txn = self.begin_read()?;
        let parts = txn.open_table(T_UPLOAD_PARTS).map_err(db_err)?;
        let rel = relkey.as_str();
        let mut out = Vec::new();
        for entry in parts.range((rel, 0u32)..=(rel, u32::MAX)).map_err(db_err)? {
            let (key, value) = entry.map_err(db_err)?;
            let (_, part_no) = key.value();
            out.push((part_no, from_json(value.value())?));
        }
        Ok(out)
    }

    /// Removes the multipart state **and all recorded parts** for `relkey`
    /// in one transaction (upload completed or aborted). Idempotent.
    pub fn clear_upload(&self, relkey: &RelKey) -> Result<(), StateError> {
        self.with_txn(|t| t.clear_upload(relkey))
    }

    // -- transfer queues ---------------------------------------------------

    /// Enqueues `relkey` in queue `q` with priority `class`.
    ///
    /// Ordering: lower `class` byte pops first (0 = most urgent, matching
    /// §3.5: thumbs-visible > sidecars > small thumbs > proxies); FIFO
    /// within a class, stable across reopen (the key encodes the class byte
    /// followed by a persisted monotonic arrival counter, so a redb range
    /// scan pops in order).
    ///
    /// Idempotent per relkey: re-pushing an already-queued relkey is a
    /// no-op returning `Ok(false)` — it keeps its original class **and**
    /// position even when a different `class` is passed; use
    /// [`SyncDb::queue_reprioritize`] to change the class of a queued
    /// entry atomically. Returns `Ok(true)` when newly enqueued.
    pub fn queue_push(&self, q: Queue, relkey: &RelKey, class: u8) -> Result<bool, StateError> {
        self.with_txn(|t| t.queue_push(q, relkey, class))
    }

    /// The head of queue `q` — lowest class, then earliest arrival —
    /// **without removing it** (`None` when empty). The §3.3 exit-flush
    /// inspection path: peeking costs no commit and loses no position.
    pub fn queue_peek(&self, q: Queue) -> Result<Option<(RelKey, u8)>, StateError> {
        let (entries_def, _) = q.tables();
        let txn = self.begin_read()?;
        let entries = txn.open_table(entries_def).map_err(db_err)?;
        match queue_head(&entries)? {
            None => Ok(None),
            Some((class, _, rel)) => Ok(Some((from_stored_str::<RelKey>(&rel)?, class))),
        }
    }

    /// Removes and returns the head of queue `q` — lowest class, then
    /// earliest arrival — or `None` when empty.
    pub fn queue_pop(&self, q: Queue) -> Result<Option<(RelKey, u8)>, StateError> {
        self.with_txn(|t| t.queue_pop(q))
    }

    /// Atomically moves an already-queued `relkey` to priority `class` (at
    /// the **back** of that class), in one committed transaction — the §3.5
    /// "thumbs became visible" bump, without the crash window a
    /// `queue_remove` + `queue_push` pair would leave (durably absent from
    /// the queue while the item's state still says queued).
    ///
    /// Returns `Ok(true)` when the relkey is queued afterwards (including
    /// the no-op case where it already had `class` — its position is then
    /// kept), `Ok(false)` when it was not queued at all (nothing changed;
    /// the caller decides whether to push).
    pub fn queue_reprioritize(
        &self,
        q: Queue,
        relkey: &RelKey,
        class: u8,
    ) -> Result<bool, StateError> {
        self.with_txn(|t| t.queue_reprioritize(q, relkey, class))
    }

    /// Removes `relkey` from queue `q` wherever it sits; `Ok(true)` when it
    /// was queued.
    pub fn queue_remove(&self, q: Queue, relkey: &RelKey) -> Result<bool, StateError> {
        self.with_txn(|t| t.queue_remove(q, relkey))
    }

    /// Number of entries in queue `q`.
    pub fn queue_len(&self, q: Queue) -> Result<u64, StateError> {
        let (entries_def, _) = q.tables();
        let txn = self.begin_read()?;
        let entries = txn.open_table(entries_def).map_err(db_err)?;
        entries.len().map_err(db_err)
    }

    // -- change-detection caches (§2.5) ------------------------------------

    /// The last-imported XMP content hash for `relkey` (gates
    /// `sync_metadata_from_xmp` on actual change, §2.5). This is a digest
    /// of arbitrary sidecar bytes, hence [`Blake3Hex`], not [`ContentId`]
    /// (which names an *original's* identity) — the §3.2 table list writes
    /// "blake3" for this column.
    pub fn xmp_seen(&self, relkey: &RelKey) -> Result<Option<Blake3Hex>, StateError> {
        let txn = self.begin_read()?;
        let seen = txn.open_table(T_XMP_SEEN).map_err(db_err)?;
        match seen.get(relkey.as_str()).map_err(db_err)? {
            Some(guard) => Ok(Some(from_stored_str(guard.value())?)),
            None => Ok(None),
        }
    }

    /// Records the imported XMP hash for `relkey`.
    pub fn set_xmp_seen(&self, relkey: &RelKey, hash: &Blake3Hex) -> Result<(), StateError> {
        self.with_txn(|t| t.set_xmp_seen(relkey, hash))
    }

    /// Removes the XMP-seen row for `relkey` (item deleted; keeps the
    /// cache from growing beyond the live library). `Ok(true)` when a row
    /// existed.
    pub fn remove_xmp_seen(&self, relkey: &RelKey) -> Result<bool, StateError> {
        self.with_txn(|t| t.remove_xmp_seen(relkey))
    }

    /// DCIM import cache: the content id previously computed for a source
    /// file at `(path, size, mtime_unix)` (§5.3 camera-roll scan skips
    /// unchanged files).
    pub fn dcim_seen(
        &self,
        path: &str,
        size: u64,
        mtime_unix: i64,
    ) -> Result<Option<ContentId>, StateError> {
        let txn = self.begin_read()?;
        let seen = txn.open_table(T_DCIM_SEEN).map_err(db_err)?;
        match seen.get((path, size, mtime_unix)).map_err(db_err)? {
            Some(guard) => Ok(Some(from_stored_str(guard.value())?)),
            None => Ok(None),
        }
    }

    /// Records a DCIM scan result. Any previous rows for the **same source
    /// path** (older `(size, mtime)` observations) are pruned in the same
    /// transaction — a modified watched file replaces its cache row rather
    /// than stranding the old one, so the table stays bounded by the number
    /// of watched files, not by their modification history.
    pub fn set_dcim_seen(
        &self,
        path: &str,
        size: u64,
        mtime_unix: i64,
        content_id: &ContentId,
    ) -> Result<(), StateError> {
        self.with_txn(|t| t.set_dcim_seen(path, size, mtime_unix, content_id))
    }

    /// Removes every DCIM-seen row for `path` (source file gone). Returns
    /// the number of rows removed.
    pub fn remove_dcim_seen(&self, path: &str) -> Result<u64, StateError> {
        self.with_txn(|t| t.remove_dcim_seen(path))
    }

    // -- meta --------------------------------------------------------------

    /// The persisted server-time offset in milliseconds (server minus
    /// local; §2.10 GC age rules run on server time), or `None` before the
    /// first measurement.
    pub fn server_time_offset_ms(&self) -> Result<Option<i64>, StateError> {
        let txn = self.begin_read()?;
        let meta = txn.open_table(T_META).map_err(db_err)?;
        meta_get(&meta, K_SERVER_TIME_OFFSET_MS)
    }

    /// Stores the server-time offset.
    pub fn set_server_time_offset_ms(&self, offset_ms: i64) -> Result<(), StateError> {
        self.with_txn(|t| {
            let mut meta = t.txn.open_table(T_META).map_err(db_err)?;
            meta.insert(K_SERVER_TIME_OFFSET_MS, to_json(&offset_ms)?.as_slice())
                .map_err(db_err)?;
            Ok(())
        })
    }

    // -- test support (not part of the supported API) ----------------------

    /// Test support only: overwrite (or remove, with `None`) the stored
    /// schema version so the open-time gates can be exercised. Not part of
    /// the supported API.
    #[doc(hidden)]
    pub fn force_schema_version(&self, version: Option<u32>) -> Result<(), StateError> {
        self.with_txn(|t| {
            let mut meta = t.txn.open_table(T_META).map_err(db_err)?;
            match version {
                Some(v) => {
                    meta.insert(K_SCHEMA_VERSION, to_json(&v)?.as_slice())
                        .map_err(db_err)?;
                }
                None => {
                    meta.remove(K_SCHEMA_VERSION).map_err(db_err)?;
                }
            }
            Ok(())
        })
    }

    /// Test support only: remove the stored device identity, modeling meta
    /// corruption / a future layout that moved identity, so the open gate's
    /// schema-first ordering can be exercised.
    #[doc(hidden)]
    pub fn force_remove_device_id(&self) -> Result<(), StateError> {
        self.with_txn(|t| {
            let mut meta = t.txn.open_table(T_META).map_err(db_err)?;
            meta.remove(K_DEVICE_ID).map_err(db_err)?;
            Ok(())
        })
    }

    /// Test support only: overwrite the persisted last-allocated seq
    /// counter (corruption/tamper modeling for the overflow guard).
    #[doc(hidden)]
    pub fn force_last_seq(&self, value: u64) -> Result<(), StateError> {
        self.with_txn(|t| {
            let mut meta = t.txn.open_table(T_META).map_err(db_err)?;
            meta.insert(K_LAST_SEQ, to_json(&value)?.as_slice())
                .map_err(db_err)?;
            Ok(())
        })
    }

    /// Test support only: overwrite the persisted queue arrival counter
    /// (corruption/tamper modeling for the overflow guard).
    #[doc(hidden)]
    pub fn force_queue_arrival(&self, value: u64) -> Result<(), StateError> {
        self.with_txn(|t| {
            let mut meta = t.txn.open_table(T_META).map_err(db_err)?;
            meta.insert(K_QUEUE_ARRIVAL, to_json(&value)?.as_slice())
                .map_err(db_err)?;
            Ok(())
        })
    }

    /// Test support only: store raw (typically unparseable) bytes as the
    /// item record for `relkey`, so the documented
    /// [`StateError::Codec`]-not-panic read behavior can be pinned.
    #[doc(hidden)]
    pub fn force_corrupt_item(&self, relkey: &RelKey, bytes: &[u8]) -> Result<(), StateError> {
        self.with_txn(|t| {
            let mut items = t.txn.open_table(T_ITEMS).map_err(db_err)?;
            items.insert(relkey.as_str(), bytes).map_err(db_err)?;
            Ok(())
        })
    }
}

/// Increments a persisted u64 meta counter, refusing to wrap
/// ([`StateError::CounterSaturated`]).
fn bump_counter(
    meta: &mut redb::Table<'_, &'static str, &'static [u8]>,
    key: &'static str,
) -> Result<u64, StateError> {
    let last: u64 = meta_get(meta, key)?.unwrap_or(0);
    let next = last
        .checked_add(1)
        .ok_or(StateError::CounterSaturated { key })?;
    meta.insert(key, to_json(&next)?.as_slice())
        .map_err(db_err)?;
    Ok(next)
}

impl StateTxn<'_> {
    /// [`SyncDb::get_item`] within this transaction (sees the
    /// transaction's own uncommitted writes).
    pub fn get_item(&self, relkey: &RelKey) -> Result<Option<ItemRecord>, StateError> {
        let items = self.txn.open_table(T_ITEMS).map_err(db_err)?;
        read_item(&items, relkey)
    }

    /// [`SyncDb::insert_item`] within this transaction.
    pub fn insert_item(&self, relkey: &RelKey, record: &ItemRecord) -> Result<bool, StateError> {
        let mut items = self.txn.open_table(T_ITEMS).map_err(db_err)?;
        if items.get(relkey.as_str()).map_err(db_err)?.is_some() {
            return Ok(false);
        }
        let value = to_json(record)?;
        items
            .insert(relkey.as_str(), value.as_slice())
            .map_err(db_err)?;
        Ok(true)
    }

    /// [`SyncDb::replay_put_item`] within this transaction.
    pub fn replay_put_item(&self, relkey: &RelKey, record: &ItemRecord) -> Result<(), StateError> {
        let value = to_json(record)?;
        let mut items = self.txn.open_table(T_ITEMS).map_err(db_err)?;
        items
            .insert(relkey.as_str(), value.as_slice())
            .map_err(db_err)?;
        Ok(())
    }

    /// [`SyncDb::delete_item`] within this transaction.
    pub fn delete_item(&self, relkey: &RelKey) -> Result<bool, StateError> {
        let mut items = self.txn.open_table(T_ITEMS).map_err(db_err)?;
        let previous = items.remove(relkey.as_str()).map_err(db_err)?;
        Ok(previous.is_some())
    }

    /// [`SyncDb::transition`] within this transaction — the composite
    /// building block ("transition + enqueue" etc.). Semantics, check
    /// order, and errors are identical; an `Err` from here aborts the whole
    /// transaction when propagated out of the closure.
    pub fn transition(
        &self,
        relkey: &RelKey,
        expected_from: ItemState,
        to: ItemState,
        mutate: impl FnOnce(&mut ItemRecord),
    ) -> Result<ItemRecord, StateError> {
        // (1) Static legality — before any storage is consulted.
        if !legal(expected_from, to) {
            return Err(StateError::IllegalTransition {
                relkey: relkey.clone(),
                from: expected_from,
                to,
            });
        }
        let mut items = self.txn.open_table(T_ITEMS).map_err(db_err)?;
        // (2) + (3) Compare-and-set against the stored state.
        let stored = match read_item(&items, relkey)? {
            None => {
                return Err(StateError::StaleState {
                    relkey: relkey.clone(),
                    expected: expected_from,
                    found: None,
                });
            }
            Some(record) => record,
        };
        if stored.state != expected_from {
            return Err(StateError::StaleState {
                relkey: relkey.clone(),
                expected: expected_from,
                found: Some(stored.state),
            });
        }
        let mut record = stored;
        mutate(&mut record);
        record.state = to; // the transition owns the state field
        let value = to_json(&record)?;
        items
            .insert(relkey.as_str(), value.as_slice())
            .map_err(db_err)?;
        Ok(record)
    }

    /// [`SyncDb::update_item`] within this transaction.
    pub fn update_item(
        &self,
        relkey: &RelKey,
        expected_state: ItemState,
        mutate: impl FnOnce(&mut ItemRecord),
    ) -> Result<ItemRecord, StateError> {
        let mut items = self.txn.open_table(T_ITEMS).map_err(db_err)?;
        let stored = match read_item(&items, relkey)? {
            None => {
                return Err(StateError::StaleState {
                    relkey: relkey.clone(),
                    expected: expected_state,
                    found: None,
                });
            }
            Some(record) => record,
        };
        if stored.state != expected_state {
            return Err(StateError::StaleState {
                relkey: relkey.clone(),
                expected: expected_state,
                found: Some(stored.state),
            });
        }
        let mut record = stored;
        mutate(&mut record);
        record.state = expected_state; // state-preserving by contract
        let value = to_json(&record)?;
        items
            .insert(relkey.as_str(), value.as_slice())
            .map_err(db_err)?;
        Ok(record)
    }

    /// [`SyncDb::mark_applied`] within this transaction.
    pub fn mark_applied(&self, device: &DeviceId, seq: u64) -> Result<(), StateError> {
        let mut applied = self.txn.open_table(T_APPLIED).map_err(db_err)?;
        applied.insert((device.as_str(), seq), ()).map_err(db_err)?;
        Ok(())
    }

    /// [`SyncDb::set_cursor`] within this transaction.
    pub fn set_cursor(&self, device: &DeviceId, seq: u64) -> Result<(), StateError> {
        let mut cursors = self.txn.open_table(T_CURSORS).map_err(db_err)?;
        cursors.insert(device.as_str(), seq).map_err(db_err)?;
        Ok(())
    }

    /// [`SyncDb::freeze_next_segment`] within this transaction — e.g. the
    /// §2.4 `verifying → synced` step, which freezes the journal entry and
    /// transitions the item in one commit.
    pub fn freeze_next_segment(
        &self,
        build: impl FnOnce(u64) -> Vec<u8>,
    ) -> Result<u64, StateError> {
        let seq = {
            let mut meta = self.txn.open_table(T_META).map_err(db_err)?;
            bump_counter(&mut meta, K_LAST_SEQ)?
        };
        let bytes = build(seq);
        let mut segments = self.txn.open_table(T_SEGMENTS).map_err(db_err)?;
        // Defensive: a freshly allocated seq cannot be frozen unless the
        // counter was tampered backwards; refuse rather than overwrite.
        if segments.get(seq).map_err(db_err)?.is_some() {
            return Err(StateError::AlreadyFrozen { seq });
        }
        segments.insert(seq, bytes.as_slice()).map_err(db_err)?;
        Ok(seq)
    }

    /// [`SyncDb::allocate_seq`] within this transaction.
    pub fn allocate_seq(&self) -> Result<u64, StateError> {
        let mut meta = self.txn.open_table(T_META).map_err(db_err)?;
        bump_counter(&mut meta, K_LAST_SEQ)
    }

    /// [`SyncDb::freeze_segment`] within this transaction.
    pub fn freeze_segment(&self, seq: u64, bytes: &[u8]) -> Result<(), StateError> {
        let meta = self.txn.open_table(T_META).map_err(db_err)?;
        let last: u64 = meta_get(&meta, K_LAST_SEQ)?.unwrap_or(0);
        if seq == 0 || seq > last {
            return Err(StateError::SeqNotAllocated { seq });
        }
        let mut segments = self.txn.open_table(T_SEGMENTS).map_err(db_err)?;
        if segments.get(seq).map_err(db_err)?.is_some() {
            return Err(StateError::AlreadyFrozen { seq });
        }
        segments.insert(seq, bytes).map_err(db_err)?;
        Ok(())
    }

    /// [`SyncDb::mark_published`] within this transaction.
    pub fn mark_published(&self, seq: u64) -> Result<(), StateError> {
        let segments = self.txn.open_table(T_SEGMENTS).map_err(db_err)?;
        if segments.get(seq).map_err(db_err)?.is_none() {
            return Err(StateError::NotFrozen { seq });
        }
        let mut published = self.txn.open_table(T_PUBLISHED).map_err(db_err)?;
        published.insert(seq, ()).map_err(db_err)?;
        let mut meta = self.txn.open_table(T_META).map_err(db_err)?;
        let cursor: u64 = meta_get(&meta, K_PUBLISHED_CURSOR)?.unwrap_or(0);
        if seq > cursor {
            meta.insert(K_PUBLISHED_CURSOR, to_json(&seq)?.as_slice())
                .map_err(db_err)?;
        }
        // Advance the contiguous published floor (the unpublished-scan
        // start). It only ever crosses published seqs, so a frozen or
        // allocated-but-unfrozen seq below it is impossible by induction.
        let mut floor: u64 = meta_get(&meta, K_PUBLISHED_FLOOR)?.unwrap_or(0);
        let start = floor;
        while let Some(next) = floor.checked_add(1) {
            if published.get(next).map_err(db_err)?.is_some() {
                floor = next;
            } else {
                break;
            }
        }
        if floor != start {
            meta.insert(K_PUBLISHED_FLOOR, to_json(&floor)?.as_slice())
                .map_err(db_err)?;
        }
        Ok(())
    }

    /// [`SyncDb::set_upload`] within this transaction.
    pub fn set_upload(
        &self,
        relkey: &RelKey,
        upload: &MultipartUploadState,
    ) -> Result<(), StateError> {
        let value = to_json(upload)?;
        let mut uploads = self.txn.open_table(T_UPLOADS).map_err(db_err)?;
        uploads
            .insert(relkey.as_str(), value.as_slice())
            .map_err(db_err)?;
        Ok(())
    }

    /// [`SyncDb::record_upload_part`] within this transaction.
    pub fn record_upload_part(
        &self,
        relkey: &RelKey,
        part_no: u32,
        part: &UploadPart,
    ) -> Result<(), StateError> {
        let value = to_json(part)?;
        let mut parts = self.txn.open_table(T_UPLOAD_PARTS).map_err(db_err)?;
        parts
            .insert((relkey.as_str(), part_no), value.as_slice())
            .map_err(db_err)?;
        Ok(())
    }

    /// [`SyncDb::clear_upload`] within this transaction.
    pub fn clear_upload(&self, relkey: &RelKey) -> Result<(), StateError> {
        let rel = relkey.as_str();
        let mut uploads = self.txn.open_table(T_UPLOADS).map_err(db_err)?;
        uploads.remove(rel).map_err(db_err)?;
        let mut parts = self.txn.open_table(T_UPLOAD_PARTS).map_err(db_err)?;
        let part_nos: Vec<u32> = parts
            .range((rel, 0u32)..=(rel, u32::MAX))
            .map_err(db_err)?
            .map(|entry| entry.map(|(key, _)| key.value().1))
            .collect::<Result<_, _>>()
            .map_err(db_err)?;
        for part_no in part_nos {
            parts.remove((rel, part_no)).map_err(db_err)?;
        }
        Ok(())
    }

    /// [`SyncDb::queue_push`] within this transaction — the other half of
    /// the "transition to `Queued` + enqueue" composite.
    pub fn queue_push(&self, q: Queue, relkey: &RelKey, class: u8) -> Result<bool, StateError> {
        let (entries_def, idx_def) = q.tables();
        let rel = relkey.as_str();
        let mut idx = self.txn.open_table(idx_def).map_err(db_err)?;
        if idx.get(rel).map_err(db_err)?.is_some() {
            return Ok(false);
        }
        let arrival = {
            let mut meta = self.txn.open_table(T_META).map_err(db_err)?;
            bump_counter(&mut meta, K_QUEUE_ARRIVAL)?
        };
        let mut entries = self.txn.open_table(entries_def).map_err(db_err)?;
        entries.insert((class, arrival), rel).map_err(db_err)?;
        idx.insert(rel, (class, arrival)).map_err(db_err)?;
        Ok(true)
    }

    /// [`SyncDb::queue_pop`] within this transaction — enables the atomic
    /// "pop + transition to `Uploading`/`Downloading`" step.
    pub fn queue_pop(&self, q: Queue) -> Result<Option<(RelKey, u8)>, StateError> {
        let (entries_def, idx_def) = q.tables();
        let mut entries = self.txn.open_table(entries_def).map_err(db_err)?;
        match queue_head(&entries)? {
            None => Ok(None),
            Some((class, arrival, rel)) => {
                entries.remove((class, arrival)).map_err(db_err)?;
                let mut idx = self.txn.open_table(idx_def).map_err(db_err)?;
                idx.remove(rel.as_str()).map_err(db_err)?;
                Ok(Some((from_stored_str::<RelKey>(&rel)?, class)))
            }
        }
    }

    /// [`SyncDb::queue_reprioritize`] within this transaction.
    pub fn queue_reprioritize(
        &self,
        q: Queue,
        relkey: &RelKey,
        class: u8,
    ) -> Result<bool, StateError> {
        let (entries_def, idx_def) = q.tables();
        let rel = relkey.as_str();
        let mut idx = self.txn.open_table(idx_def).map_err(db_err)?;
        let slot = idx.get(rel).map_err(db_err)?.map(|guard| guard.value());
        match slot {
            None => Ok(false),
            Some((old_class, _)) if old_class == class => Ok(true),
            Some((old_class, old_arrival)) => {
                let mut entries = self.txn.open_table(entries_def).map_err(db_err)?;
                entries.remove((old_class, old_arrival)).map_err(db_err)?;
                let arrival = {
                    let mut meta = self.txn.open_table(T_META).map_err(db_err)?;
                    bump_counter(&mut meta, K_QUEUE_ARRIVAL)?
                };
                entries.insert((class, arrival), rel).map_err(db_err)?;
                idx.insert(rel, (class, arrival)).map_err(db_err)?;
                Ok(true)
            }
        }
    }

    /// [`SyncDb::queue_remove`] within this transaction.
    pub fn queue_remove(&self, q: Queue, relkey: &RelKey) -> Result<bool, StateError> {
        let (entries_def, idx_def) = q.tables();
        let rel = relkey.as_str();
        let mut idx = self.txn.open_table(idx_def).map_err(db_err)?;
        let slot = idx.remove(rel).map_err(db_err)?.map(|guard| guard.value());
        match slot {
            None => Ok(false),
            Some((class, arrival)) => {
                let mut entries = self.txn.open_table(entries_def).map_err(db_err)?;
                entries.remove((class, arrival)).map_err(db_err)?;
                Ok(true)
            }
        }
    }

    /// [`SyncDb::set_xmp_seen`] within this transaction.
    pub fn set_xmp_seen(&self, relkey: &RelKey, hash: &Blake3Hex) -> Result<(), StateError> {
        let mut seen = self.txn.open_table(T_XMP_SEEN).map_err(db_err)?;
        seen.insert(relkey.as_str(), hash.as_str())
            .map_err(db_err)?;
        Ok(())
    }

    /// [`SyncDb::remove_xmp_seen`] within this transaction.
    pub fn remove_xmp_seen(&self, relkey: &RelKey) -> Result<bool, StateError> {
        let mut seen = self.txn.open_table(T_XMP_SEEN).map_err(db_err)?;
        let removed = seen.remove(relkey.as_str()).map_err(db_err)?.is_some();
        Ok(removed)
    }

    /// [`SyncDb::set_dcim_seen`] within this transaction.
    pub fn set_dcim_seen(
        &self,
        path: &str,
        size: u64,
        mtime_unix: i64,
        content_id: &ContentId,
    ) -> Result<(), StateError> {
        let mut seen = self.txn.open_table(T_DCIM_SEEN).map_err(db_err)?;
        // Prune stale observations of the same source path (doc on
        // `SyncDb::set_dcim_seen`).
        let stale: Vec<(u64, i64)> = seen
            .range((path, 0u64, i64::MIN)..=(path, u64::MAX, i64::MAX))
            .map_err(db_err)?
            .map(|entry| {
                entry.map(|(key, _)| {
                    let (_, s, m) = key.value();
                    (s, m)
                })
            })
            .collect::<Result<_, _>>()
            .map_err(db_err)?;
        for (s, m) in stale {
            seen.remove((path, s, m)).map_err(db_err)?;
        }
        seen.insert((path, size, mtime_unix), content_id.as_str())
            .map_err(db_err)?;
        Ok(())
    }

    /// [`SyncDb::remove_dcim_seen`] within this transaction.
    pub fn remove_dcim_seen(&self, path: &str) -> Result<u64, StateError> {
        let mut seen = self.txn.open_table(T_DCIM_SEEN).map_err(db_err)?;
        let rows: Vec<(u64, i64)> = seen
            .range((path, 0u64, i64::MIN)..=(path, u64::MAX, i64::MAX))
            .map_err(db_err)?
            .map(|entry| {
                entry.map(|(key, _)| {
                    let (_, s, m) = key.value();
                    (s, m)
                })
            })
            .collect::<Result<_, _>>()
            .map_err(db_err)?;
        let removed = rows.len() as u64;
        for (s, m) in rows {
            seen.remove((path, s, m)).map_err(db_err)?;
        }
        Ok(removed)
    }
}
