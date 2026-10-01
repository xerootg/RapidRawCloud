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
//! # Durability — empirically verified
//!
//! redb's default durability fsyncs at `commit()`; every write API here is
//! one committed write transaction, so once a call returns, the state
//! survives process death. SIGKILL loops against a committing writer showed
//! zero torn or lost committed records (the db may be *ahead* of what the
//! writer observed returning, never behind). `begin_write` from a second
//! thread blocks until the open write transaction commits — writers are
//! serialized, which is what makes [`SyncDb::transition`]'s compare-and-set
//! semantics race-free.
//!
//! # Encoding
//!
//! Record values (items, upload state, parts) are serialized as JSON inside
//! redb values — deliberately, for v1 debuggability (`redb` dump + `jq`
//! works). The encoding is an internal detail behind the typed accessors and
//! can be swapped (e.g. for a binary codec) without touching callers or the
//! public schema version. Raw redb table names are likewise internal.

use std::path::{Path, PathBuf};

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
    /// The database was written by a newer schema than this build supports.
    /// Fail closed (§2.2 min-reader spirit): never guess at future tables.
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
    /// `transition`'s `expected_from` did not match the stored state (a
    /// concurrent transition won, or the item does not exist). Nothing was
    /// mutated.
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
/// illegal):
///
/// | From | To | Why |
/// |---|---|---|
/// | `Dirty` | `Queued` | debouncer enqueues (§2.4) |
/// | `Dirty` | `Conflict` | concurrent remote version met local dirt (§2.6) |
/// | `Queued` | `Uploading` | transfer slot acquired |
/// | `Queued` | `Dirty` | dequeued (e.g. shutdown drain re-marks) |
/// | `Uploading` | `Verifying` | PUT/CompleteMultipartUpload done |
/// | `Uploading` | `Queued` | transfer failed, requeue with backoff |
/// | `Uploading` | `Dirty` | upload abandoned (e.g. stale upload hygiene) |
/// | `Verifying` | `Synced` | integrity confirmed; journal entry written |
/// | `Verifying` | `Queued` | verify failed on our own upload → retry |
/// | `Verifying` | `CorruptRemote` | backend-accepted-but-wrong (readback mismatch) |
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
pub fn legal(from: ItemState, to: ItemState) -> bool {
    let _ = (from, to);
    todo!("P1-U2 green: §2.4 transition table")
}

/// One item's durable sync record (the `items` table value, §3.2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ItemRecord {
    /// What kind of object this relkey is (original, sidecar, …).
    pub kind: Kind,
    /// Current §2.4 state. Changed **only** through [`SyncDb::transition`]
    /// after the record exists (a wholesale [`SyncDb::put_item`] of a
    /// different state is for ingest/replay paths, not the state machine).
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

/// The device's durable sync state database (§3.2).
///
/// One per device, at `app_data_dir/rrcloud/state.redb`. Opening takes the
/// redb file lock for the life of the value; a second open anywhere fails
/// fast with [`StateError::AlreadyLocked`] (§5.1). All methods take `&self`
/// and are safe to call from multiple threads: redb serializes write
/// transactions internally (empirical note in the module docs).
///
/// Every mutating method is **one committed write transaction**: when it
/// returns `Ok`, the change is durable (survives SIGKILL).
#[derive(Debug)]
pub struct SyncDb {
    #[allow(dead_code)] // consumed in the green phase
    db: redb::Database,
    device_id: DeviceId,
    path: PathBuf,
}

impl SyncDb {
    /// Opens (or creates) the state database at `path`.
    ///
    /// First open of a fresh file initializes `meta`: it stores
    /// `schema_version =` [`SCHEMA_VERSION`] and mints the device identity
    /// from `mint_device_id` — which must be `Some` then
    /// ([`StateError::DeviceIdRequired`] otherwise; the store never
    /// generates randomness).
    ///
    /// Reopening an initialized db ignores a `None` and verifies a
    /// `Some(id)` against the stored identity
    /// ([`StateError::DeviceIdMismatch`] on conflict).
    ///
    /// Fails fast and typed when the db is held elsewhere
    /// ([`StateError::AlreadyLocked`]) or carries a schema version other
    /// than exactly [`SCHEMA_VERSION`] ([`StateError::SchemaTooNew`] /
    /// [`StateError::SchemaUnsupported`]).
    pub fn open(
        path: impl AsRef<Path>,
        mint_device_id: Option<DeviceId>,
    ) -> Result<Self, StateError> {
        let _ = (path.as_ref(), mint_device_id);
        todo!("P1-U2 green: open/create + meta init + lock/schema gates")
    }

    /// This database's device identity (minted on first open).
    pub fn device_id(&self) -> &DeviceId {
        &self.device_id
    }

    /// The database file path this store was opened at.
    pub fn path(&self) -> &Path {
        &self.path
    }

    // -- items ------------------------------------------------------------

    /// Inserts or wholesale-replaces an item record. For ingest/replay
    /// paths; state-machine steps go through [`SyncDb::transition`].
    pub fn put_item(&self, relkey: &RelKey, record: &ItemRecord) -> Result<(), StateError> {
        let _ = (relkey, record);
        todo!("P1-U2 green")
    }

    /// Reads an item record (`None` when absent). A stored value that does
    /// not parse is [`StateError::Codec`], never a panic.
    pub fn get_item(&self, relkey: &RelKey) -> Result<Option<ItemRecord>, StateError> {
        let _ = relkey;
        todo!("P1-U2 green")
    }

    /// Deletes an item record; `Ok(true)` when it existed.
    pub fn delete_item(&self, relkey: &RelKey) -> Result<bool, StateError> {
        let _ = relkey;
        todo!("P1-U2 green")
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
        let _ = (relkey, expected_from, to, &mutate);
        todo!("P1-U2 green: single-txn CAS transition")
    }

    // -- applied / cursors (§2.2 idempotent apply) -------------------------

    /// `true` when `(device, seq)` was already applied.
    pub fn has_applied(&self, device: &DeviceId, seq: u64) -> Result<bool, StateError> {
        let _ = (device, seq);
        todo!("P1-U2 green")
    }

    /// Records `(device, seq)` as applied. Idempotent: re-marking is a
    /// committed no-op, not an error.
    pub fn mark_applied(&self, device: &DeviceId, seq: u64) -> Result<(), StateError> {
        let _ = (device, seq);
        todo!("P1-U2 green")
    }

    /// The highest contiguously-applied seq recorded for `device`'s journal
    /// prefix (0 when none).
    pub fn cursor(&self, device: &DeviceId) -> Result<u64, StateError> {
        let _ = device;
        todo!("P1-U2 green")
    }

    /// Sets `device`'s cursor.
    pub fn set_cursor(&self, device: &DeviceId, seq: u64) -> Result<(), StateError> {
        let _ = (device, seq);
        todo!("P1-U2 green")
    }

    // -- journal publication (§2.1.5) --------------------------------------

    /// Allocates and durably commits the next journal seq for this device.
    ///
    /// Strictly increasing, starting at 1; the allocation itself is a
    /// committed transaction, so a seq this method returned is never handed
    /// out again — across threads **and** across crash/reopen (§2.2
    /// monotonicity). Gaps are allowed (an allocated seq whose segment was
    /// never frozen stays a hole; readers dedup by `(device, seq)`).
    pub fn allocate_seq(&self) -> Result<u64, StateError> {
        todo!("P1-U2 green")
    }

    /// The highest seq [`SyncDb::allocate_seq`] has returned (0 when none).
    pub fn last_allocated_seq(&self) -> Result<u64, StateError> {
        todo!("P1-U2 green")
    }

    /// Durably freezes `bytes` as the segment for `seq` — the §2.1.5 step
    /// that must commit **before** any network PUT. The stored bytes are
    /// the segment's identity: a crash-replayed publish re-reads them via
    /// [`SyncDb::unpublished_segments`] byte-identically.
    ///
    /// `seq` must have been allocated ([`StateError::SeqNotAllocated`]) and
    /// not already frozen ([`StateError::AlreadyFrozen`] — a segment is
    /// frozen exactly once, replay never re-freezes).
    pub fn freeze_segment(&self, seq: u64, bytes: &[u8]) -> Result<(), StateError> {
        let _ = (seq, bytes);
        todo!("P1-U2 green")
    }

    /// All frozen-but-unpublished segments, in ascending seq order, each
    /// with the exact bytes passed to [`SyncDb::freeze_segment`] (byte
    /// identity is the §2.1.5 republish guarantee).
    pub fn unpublished_segments(&self) -> Result<Vec<(u64, Vec<u8>)>, StateError> {
        todo!("P1-U2 green")
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
        let _ = seq;
        todo!("P1-U2 green")
    }

    /// The highest published seq (0 when nothing published yet).
    pub fn published_cursor(&self) -> Result<u64, StateError> {
        todo!("P1-U2 green")
    }

    // -- multipart upload resume (§2.4) ------------------------------------

    /// Stores (or replaces) the multipart state for `relkey`.
    pub fn set_upload(
        &self,
        relkey: &RelKey,
        upload: &MultipartUploadState,
    ) -> Result<(), StateError> {
        let _ = (relkey, upload);
        todo!("P1-U2 green")
    }

    /// Reads the multipart state for `relkey`.
    pub fn get_upload(&self, relkey: &RelKey) -> Result<Option<MultipartUploadState>, StateError> {
        let _ = relkey;
        todo!("P1-U2 green")
    }

    /// Records a completed part. Re-recording a part number replaces it
    /// (a part re-uploaded after a resume has a new ETag).
    pub fn record_upload_part(
        &self,
        relkey: &RelKey,
        part_no: u32,
        part: &UploadPart,
    ) -> Result<(), StateError> {
        let _ = (relkey, part_no, part);
        todo!("P1-U2 green")
    }

    /// All recorded parts for `relkey`, ascending by part number.
    pub fn upload_parts(&self, relkey: &RelKey) -> Result<Vec<(u32, UploadPart)>, StateError> {
        let _ = relkey;
        todo!("P1-U2 green")
    }

    /// Removes the multipart state **and all recorded parts** for `relkey`
    /// in one transaction (upload completed or aborted). Idempotent.
    pub fn clear_upload(&self, relkey: &RelKey) -> Result<(), StateError> {
        let _ = relkey;
        todo!("P1-U2 green")
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
    /// complete no-op returning `Ok(false)` — it keeps its original class
    /// **and** position even when a different `class` is passed (callers
    /// that want to re-prioritize must [`SyncDb::queue_remove`] first).
    /// Returns `Ok(true)` when newly enqueued.
    pub fn queue_push(&self, q: Queue, relkey: &RelKey, class: u8) -> Result<bool, StateError> {
        let _ = (q, relkey, class);
        todo!("P1-U2 green")
    }

    /// Removes and returns the head of queue `q` — lowest class, then
    /// earliest arrival — or `None` when empty.
    pub fn queue_pop(&self, q: Queue) -> Result<Option<(RelKey, u8)>, StateError> {
        let _ = q;
        todo!("P1-U2 green")
    }

    /// Removes `relkey` from queue `q` wherever it sits; `Ok(true)` when it
    /// was queued.
    pub fn queue_remove(&self, q: Queue, relkey: &RelKey) -> Result<bool, StateError> {
        let _ = (q, relkey);
        todo!("P1-U2 green")
    }

    /// Number of entries in queue `q`.
    pub fn queue_len(&self, q: Queue) -> Result<u64, StateError> {
        let _ = q;
        todo!("P1-U2 green")
    }

    // -- change-detection caches (§2.5) ------------------------------------

    /// The last-imported XMP content hash for `relkey` (gates
    /// `sync_metadata_from_xmp` on actual change, §2.5). This is a digest
    /// of arbitrary sidecar bytes, hence [`Blake3Hex`], not [`ContentId`]
    /// (which names an *original's* identity) — the §3.2 table list writes
    /// "blake3" for this column.
    pub fn xmp_seen(&self, relkey: &RelKey) -> Result<Option<Blake3Hex>, StateError> {
        let _ = relkey;
        todo!("P1-U2 green")
    }

    /// Records the imported XMP hash for `relkey`.
    pub fn set_xmp_seen(&self, relkey: &RelKey, hash: &Blake3Hex) -> Result<(), StateError> {
        let _ = (relkey, hash);
        todo!("P1-U2 green")
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
        let _ = (path, size, mtime_unix);
        todo!("P1-U2 green")
    }

    /// Records a DCIM scan result.
    pub fn set_dcim_seen(
        &self,
        path: &str,
        size: u64,
        mtime_unix: i64,
        content_id: &ContentId,
    ) -> Result<(), StateError> {
        let _ = (path, size, mtime_unix, content_id);
        todo!("P1-U2 green")
    }

    // -- meta --------------------------------------------------------------

    /// The persisted server-time offset in milliseconds (server minus
    /// local; §2.10 GC age rules run on server time), or `None` before the
    /// first measurement.
    pub fn server_time_offset_ms(&self) -> Result<Option<i64>, StateError> {
        todo!("P1-U2 green")
    }

    /// Stores the server-time offset.
    pub fn set_server_time_offset_ms(&self, offset_ms: i64) -> Result<(), StateError> {
        let _ = offset_ms;
        todo!("P1-U2 green")
    }

    /// Test support only: overwrite (or remove, with `None`) the stored
    /// schema version so the open-time gates can be exercised. Not part of
    /// the supported API.
    #[doc(hidden)]
    pub fn force_schema_version(&self, version: Option<u32>) -> Result<(), StateError> {
        let _ = version;
        todo!("P1-U2 green")
    }
}
