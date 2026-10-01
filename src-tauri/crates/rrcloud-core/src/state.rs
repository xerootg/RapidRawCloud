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
//! Scope: the lock is redb's **advisory** `flock(LOCK_EX | LOCK_NB)` on
//! the db file (verified in redb 2.6.3's unix file backend). Advisory
//! means filesystem-dependent — on mounts where flock is a no-op or not
//! propagated (some FUSE filesystems, old NFS, SMB shares) two processes
//! can both open the db and interleave commits with no error anywhere.
//! §5.1's exclusion is therefore guaranteed only on local filesystems with
//! working flock, which is the normal `app_data_dir` case; exotic network
//! mounts are out of scope for v1.
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
//! Platform scope (macOS): redb's `Immediate` durability issues
//! `File::sync_data` — plain `fsync(2)` (verified in redb 2.6.3's unix
//! file backend; `F_BARRIERFSYNC` is used only for `Eventual`, and
//! `F_FULLFSYNC` never). Apple documents that `fsync` does **not** force
//! the drive's write cache to stable media (`F_FULLFSYNC` is reserved for
//! that), so on macOS a commit acknowledged `Ok` can still be lost on
//! power failure, beyond the checksummed-commit-slot caveat above.
//! Process-death durability is unaffected; this is redb backend behavior
//! and not changeable at this layer.
//!
//! First creation additionally fsyncs the parent **directory** (unix)
//! after the init commit: POSIX does not promise that a new file's dirent
//! is durable just because the file itself was fsynced, and redb's
//! `Builder::create` does not sync the parent — without this, a power
//! loss shortly after first open could lose the entire db even though
//! every commit returned `Ok`.
//!
//! # Identity, reopen, and database loss (§2.2 scope)
//!
//! The device identity and the seq counter live **only** in this file, so
//! §2.2's "seq never regresses" claim holds exactly as long as the db
//! file is the identity's single home. Reopen of an existing db should
//! pass `None` to [`SyncDb::open`]: an unexpectedly fresh file (user
//! wipe, restore-from-backup, the file lost some other way) then fails
//! typed with [`StateError::DeviceIdRequired`] instead of silently
//! re-minting. A caller that caches the device id outside the db and
//! passes `Some(id)` on every open converts whole-file loss into silent
//! seq reuse under the same identity — a §2.2 protocol violation if the
//! lost seqs were ever published. The store cannot distinguish "freshly
//! minted" from "reused" ids by itself; [`SyncDb::minted_identity`]
//! reports whether an open minted, so such a caller can detect unexpected
//! freshness and refuse to publish.
//!
//! `begin_write` from a second thread blocks until the open write
//! transaction commits — writers are serialized, which is what makes
//! [`SyncDb::transition`]'s compare-and-set semantics race-free.
//!
//! # Composite steps and recovery scans
//!
//! Cross-table steps that must be atomic (§2.4 "transition + enqueue",
//! "freeze journal entry + transition to synced", §2.2 "dedup-check +
//! apply + mark applied") run inside one scoped write transaction via
//! [`SyncDb::with_txn`], which exposes the typed mutators **and the point
//! reads those composites need** on a [`StateTxn`] (its doc lists them);
//! either everything in the closure commits or nothing does. Crash recovery and hygiene scans (§2.4 stale uploads, §3.5
//! LRU eviction, §2.3 reconcile) are served by the enumeration accessors
//! ([`SyncDb::iter_items`], [`SyncDb::items_in_state`],
//! [`SyncDb::count_in_state`], [`SyncDb::iter_uploads`],
//! [`SyncDb::iter_cursors`], [`SyncDb::queue_peek`]). An enumeration that
//! hits an undecodable row fails typed with [`StateError::CodecAt`]
//! naming the row's key, so the engine can remove the one bad record and
//! rescan instead of losing the subsystem.
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
use crate::journal::{JournalError, Kind};
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
    /// `insert_item` was asked to create a record in a pipeline-interior
    /// state. Items are born only in the §2.4 entry states ([`legal_entry`]:
    /// `Dirty`, `PendingDown`, `Stub`, `Hydrated`); everything else is
    /// reachable only through [`SyncDb::transition`], so a creation
    /// elsewhere would bypass the state machine at birth with no greppable
    /// marker (ingest/replay uses [`SyncDb::replay_put_item`], which is
    /// named for exactly that visibility).
    #[error("item {relkey} cannot be created in state {state:?} (not a §2.4 entry state)")]
    IllegalCreationState {
        /// The item.
        relkey: RelKey,
        /// The non-entry state the caller passed.
        state: ItemState,
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
    /// `freeze_segment` for a seq that already has frozen bytes — either a
    /// segment frozen under exactly that seq, or a multi-entry segment
    /// whose seq **span** covers it. A segment is frozen exactly once
    /// (§2.1.5: the frozen bytes *are* the segment's identity); crash
    /// replay re-reads them, it never re-freezes.
    #[error("segment seq {seq} is already frozen (or covered by a frozen segment's span)")]
    AlreadyFrozen {
        /// The offending seq.
        seq: u64,
    },
    /// `freeze_next_segment` was asked to freeze a segment with zero
    /// entries. §2.2 seqs are per-entry, so an empty segment would consume
    /// no seq and store unreachable bytes; the journal format has no empty
    /// segments either.
    #[error("segment must contain at least one entry")]
    EmptySegment,
    /// The builder handed to [`SyncDb::freeze_next_segment`] failed —
    /// typically [`crate::journal::encode_segment`] refusing the batch
    /// because a cap was crossed only once the real seq digits were
    /// stamped (entries embed their seq as JSON digits, so the exact
    /// encoded size is not knowable before the first seq is). **No seqs
    /// were consumed and nothing was stored**, at either API level: from
    /// [`SyncDb::freeze_next_segment`] the whole transaction was aborted,
    /// and from [`StateTxn::freeze_next_segment`] the seq counter was
    /// restored inside the still-open transaction before the error
    /// returned — so the caller can shrink the batch and retry, even by
    /// catching this error inside the same [`SyncDb::with_txn`] closure,
    /// without leaving an allocated-never-frozen hole.
    #[error("segment build failed: {0}")]
    SegmentBuild(#[from] JournalError),
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
    /// typed error, never a panic (library paths do not panic). Point
    /// reads use this bare form (the caller already holds the key);
    /// enumeration scans use [`StateError::CodecAt`].
    #[error("state db record encoding error: {0}")]
    Codec(#[from] serde_json::Error),
    /// A stored row failed to decode during an **enumeration** scan, at
    /// the named key. The key is attached because scans are the only way
    /// to discover it (raw table names are internal, and e.g.
    /// [`SyncDb::delete_item`] needs a known key): it is what lets the
    /// engine surface-and-delete the one bad record —
    /// [`SyncDb::delete_item`] for an item, [`SyncDb::clear_upload`] for
    /// an upload, [`SyncDb::queue_clear`] for a queue row (whose *key*
    /// is the corrupt string itself) — instead of losing all enumeration.
    /// This matters most for the `items` scans, the crash-recovery source
    /// of truth, where clearing everything is not a recovery option.
    #[error("state db record encoding error at key {key:?}: {source}")]
    CodecAt {
        /// The stored key, in raw string form: a valid relkey/device id
        /// when the value side of the row is corrupt (the common case),
        /// or the corrupt raw key itself when the key side is.
        key: String,
        /// The decode failure.
        source: serde_json::Error,
    },
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

/// Fsyncs the directory containing `path`, making `path`'s directory
/// entry durable (first-create power-loss window; module docs).
#[cfg(unix)]
fn fsync_parent_dir(path: &Path) -> Result<(), StateError> {
    let parent = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };
    std::fs::File::open(parent)
        .and_then(|dir| dir.sync_all())
        .map_err(|e| db_err(redb::StorageError::Io(e)))
}

// ---------------------------------------------------------------------------
// Table definitions (internal; the typed accessors are the public surface)
// ---------------------------------------------------------------------------

/// `items`: relkey -> JSON [`ItemRecord`].
const T_ITEMS: TableDefinition<&str, &[u8]> = TableDefinition::new("items");
/// `applied`: (device id, seq) -> () — §2.2 idempotent-apply dedup set.
///
/// Grows by one row per journal entry per peer for the life of the device,
/// unboundedly — **deliberate for v1** (like `pending_segments` retention,
/// pruning is the §2.10 compaction/GC unit's concern). The invariant that
/// makes pruning safe when that unit lands: rows at or below
/// `cursors[device]` (the highest **contiguously**-applied seq) are
/// redundant with the cursor itself — `has_applied` for them can answer
/// from `seq <= cursor` — so a `prune_applied_below(device, seq <= cursor)`
/// range delete loses nothing.
const T_APPLIED: TableDefinition<(&str, u64), ()> = TableDefinition::new("applied");
/// `cursors`: device id -> highest contiguously-applied seq.
const T_CURSORS: TableDefinition<&str, u64> = TableDefinition::new("cursors");
/// `segment_spans`: (device id, segment first seq) -> last entry seq — the
/// reader's cache of fully-applied foreign segments' spans. A published
/// segment is immutable (§2.2), so a recorded span lets the §2.2
/// steady-state poll prove "this segment is covered by the cursor" without
/// re-GETting it; losing a row costs one redundant GET, never correctness.
const T_SEGMENT_SPANS: TableDefinition<(&str, u64), u64> = TableDefinition::new("segment_spans");
/// `pending_segments`: first entry seq -> (entry count, frozen segment
/// bytes) (§2.1.5). §2.2 seqs are per-**entry**, so a segment with `n`
/// entries covers the seq span `first..first + n`; the stored count is
/// what lets [`StateTxn::mark_published`] advance the published cursor
/// and floor over the whole span without parsing segment bytes.
const T_SEGMENTS: TableDefinition<u64, (u64, &[u8])> = TableDefinition::new("pending_segments");
/// `published_segments`: segment first seq -> () — per-segment published
/// flag (out-of-order publish support; the covered span's max also lives
/// in meta as the published cursor).
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
/// `outbound_entries`: staging arrival counter -> opaque staged outbound
/// journal-entry bytes (the publisher's durable outbound lane, §2.1.5).
/// The key is a persisted monotonic counter ([`K_OUTBOUND_ARRIVAL`]), so a
/// plain ascending range scan yields strict FIFO staging order; the value
/// bytes are opaque to this store (the publisher owns the entry encoding),
/// which keeps the staging table schema-stable across journal versions.
const T_OUTBOUND: TableDefinition<u64, &[u8]> = TableDefinition::new("outbound_entries");
/// `deleted_set`: relkey -> JSON [`DeletedRecord`] — the durable §2.3
/// deleted set this device's manifest publishes (`{del, vv, server_ts}`
/// rows), retained for 12 months after deletion (pruning is the §2.10 GC
/// unit's concern, like `applied`).
const T_DELETED: TableDefinition<&str, &[u8]> = TableDefinition::new("deleted_set");
/// `meta`: string key -> JSON value (device id, schema version, cursors,
/// counters).
const T_META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");

/// Meta keys.
const K_DEVICE_ID: &str = "device_id";
const K_SCHEMA_VERSION: &str = "schema_version";
const K_LAST_SEQ: &str = "last_seq";
const K_PUBLISHED_CURSOR: &str = "published_cursor";
/// Highest seq `F` such that every seq in `1..=F` is covered by a frozen
/// **and** published segment's span — the contiguous published prefix.
/// `unpublished_segments` starts its scan at `F + 1`, so a long-lived
/// device's publish pass costs O(pending), not O(all segments ever
/// frozen). Maintained by `mark_published`, which walks whole segment
/// spans (first seq -> stored entry count), so a multi-entry segment's
/// interior seqs never stall it; an allocated-never-frozen hole (possible
/// only via the legacy `allocate_seq` + `freeze_segment` pair, never via
/// `freeze_next_segment`) blocks the floor but not correctness.
const K_PUBLISHED_FLOOR: &str = "published_floor";
const K_QUEUE_ARRIVAL: &str = "queue_arrival";
const K_OUTBOUND_ARRIVAL: &str = "outbound_arrival";
const K_SERVER_TIME_OFFSET_MS: &str = "server_time_offset_ms";
/// §2.4 setup-probe outcome: does the backend reject a wrong `Content-MD5`?
const K_BACKEND_DIGEST_REJECTION: &str = "backend_digest_rejection";
/// §2.3/§2.10 pre-upload quarantine input: the server time (unix seconds)
/// through which this device has provably applied every deletion — the
/// newest point its deleted-set knowledge is complete to. When
/// `now_server - this > 12 months` (the deleted-set retention window), the
/// device can no longer prove a local-only key was not deleted, so it must
/// quarantine rather than auto-re-upload (`crate::compact`).
const K_APPLIED_PROOF_SERVER_TS: &str = "applied_proof_server_ts";
/// Per-segment server-time publish stamp key prefix (§2.10 14-day cap).
const K_SEGMENT_PUB_TS_PREFIX: &str = "seg_pub_ts:";

/// The meta key holding the server-time publish stamp of the own-prefix
/// segment starting at `first_seq` (zero-padded hex, so keys sort in seq
/// order under a prefix scan should one ever be needed).
fn segment_pub_ts_key(first_seq: u64) -> String {
    format!("{K_SEGMENT_PUB_TS_PREFIX}{first_seq:016x}")
}

// ---------------------------------------------------------------------------
// Encoding helpers
// ---------------------------------------------------------------------------

fn to_json<T: Serialize>(value: &T) -> Result<Vec<u8>, StateError> {
    Ok(serde_json::to_vec(value)?)
}

fn from_json<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, StateError> {
    Ok(serde_json::from_slice(bytes)?)
}

/// [`from_json`] with the row's key attached ([`StateError::CodecAt`]) —
/// for enumeration scans, whose caller cannot otherwise learn which row
/// is corrupt.
fn from_json_at<T: DeserializeOwned>(key: &str, bytes: &[u8]) -> Result<T, StateError> {
    serde_json::from_slice(bytes).map_err(|source| StateError::CodecAt {
        key: key.to_owned(),
        source,
    })
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

/// [`from_stored_str`] with key context attached
/// ([`StateError::CodecAt`]) — for enumeration scans. When the corrupt
/// string *is* the row's key, pass it as both arguments.
fn from_stored_str_at<T: DeserializeOwned>(key: &str, s: &str) -> Result<T, StateError> {
    serde_json::from_value(serde_json::Value::String(s.to_owned())).map_err(|source| {
        StateError::CodecAt {
            key: key.to_owned(),
            source,
        }
    })
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

/// The §2.4 **entry** states — the only states an item record may be
/// *created* in via [`SyncDb::insert_item`]:
///
/// | State | Why creation is legal here |
/// |---|---|
/// | [`Dirty`](ItemState::Dirty) | local change detected / fresh import (§2.4) |
/// | [`PendingDown`](ItemState::PendingDown) | remote advertised an item we have no record for (§2.2 apply) |
/// | [`Synced`](ItemState::Synced) | adopting an existing mirrored library whose local copy matches the advertised head (§3.5 bootstrap over a pre-mirrored tree — the sidecar/meta analogue of `Hydrated`; engine-unit additive entry) |
/// | [`Stub`](ItemState::Stub) | adopting an existing library where the original is a cloud placeholder (§3.5) |
/// | [`Hydrated`](ItemState::Hydrated) | adopting an existing library with the original verified locally (§3.5) |
///
/// Pipeline-interior states (`Queued`, `Uploading`, `Verifying`, …) are
/// reachable only via [`SyncDb::transition`]; birth there would bypass
/// the state machine invisibly. Ingest/replay paths that must materialize
/// a record in an arbitrary state use [`SyncDb::replay_put_item`], whose
/// name makes the bypass greppable.
pub fn legal_entry(state: ItemState) -> bool {
    matches!(
        state,
        ItemState::Dirty
            | ItemState::PendingDown
            | ItemState::Synced
            | ItemState::Stub
            | ItemState::Hydrated
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
    /// Size in bytes of the file (0 when unknown/stub).
    ///
    /// **§2.6 coordination note**: the manifest advertise gate
    /// ([`crate::manifest`] `build_manifest`) emits in-flight rows whose
    /// `blake3`/`size` pair must keep describing the last **published**
    /// version — a peer's §2.3 reconcile flags a `blake3`/`size`
    /// mismatch as `corrupt_remote`. So until a last-published snapshot
    /// mechanism lands, the chokepoint/§3.4 unit must NOT refresh `size`
    /// to current-local facts while marking an item `Dirty` with a
    /// published `blake3` still advertised; it stays naming the
    /// published bytes.
    pub size: u64,
    /// File mtime, unix **nanoseconds** (change pre-check, §2.5). Same
    /// §2.6 coordination note as `size`: not refreshed in flight until a
    /// last-published snapshot exists (mtime itself is not advertised
    /// with reconcile weight, but it travels in the manifest row and
    /// must not imply a version the `blake3` does not name).
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
    /// Star rating of the current head version (§2.2: sidecar entries and
    /// manifest rows advertise it so the grid can badge before the sidecar
    /// bytes download, §3.5). Engine-unit additive field: absent in stored
    /// v1 records, which decode as `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rating: Option<u8>,
    /// Color label of the current head version (same §2.2 badge contract
    /// as `rating`). Engine-unit additive field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub color_label: Option<String>,
    /// Authoring device of the current head version — one §2.6 case-4
    /// tiebreak input (the manifest row's `device` provenance, which
    /// [`crate::manifest`] documents as owed by the vv-engine unit).
    /// Engine-unit additive field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device: Option<DeviceId>,
    /// Wall-clock `ts` of the current head version's journal entry — the
    /// other §2.6 case-4 tiebreak input. Snapshotted at admission for a
    /// locally authored version (one admitted upload = one `(vv, ts)`
    /// identity) and adopted from the entry for a remote one, so every
    /// device orders the same candidate pair identically. Engine-unit
    /// additive field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head_ts: Option<i64>,
    /// §2.6 admission snapshot: the version vector the in-flight upload
    /// will journal (`vv[self]` bumped at queue admission, §3.7). Kept
    /// separate from `vv` so the record's advertised fields keep naming
    /// the last **published** version while an upload is in flight (the
    /// §2.6 coordination note on `size`/`mtime_unix_ns` and in
    /// [`crate::manifest::build_manifest`]); the §2.4 verify-commit
    /// promotes it (entry carries exactly this snapshot, record `vv`
    /// becomes the elementwise max, the snapshot clears). Engine-unit
    /// additive field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admitted_vv: Option<VersionVector>,
    /// §2.6 admission snapshot, `ts` half: the wall-clock entry timestamp
    /// frozen when the in-flight upload was admitted — the `(vv, ts)`
    /// identity "one admitted upload = one version" promises. The §2.4
    /// verify-commit stamps the staged entry with exactly this value, so
    /// the published `ts` stays the admission freeze even when a
    /// converged twin applied mid-flight moved the *record's* `head_ts`
    /// (which tracks the resolved head identity, not the intent). Set and
    /// cleared together with `admitted_vv`; a record written before this
    /// field decodes it as `None`, for which the commit seam falls back
    /// to `head_ts` (the pre-field behavior). Engine-unit additive field
    /// (review round 0).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admitted_ts: Option<i64>,
    /// §2.7 soft-delete marker: a dominating `del` was applied. The item
    /// keeps its record (hidden from the UI, listed under "Recently
    /// Deleted"); a deliberate flag, not an [`ItemState`], because
    /// deletion composes with every pipeline state (a deleted item can
    /// still be `Stub` or `Synced`) and restore must reproduce the exact
    /// pre-delete record. Engine-unit additive field: absent in stored v1
    /// records, which decode as `false`.
    #[serde(default)]
    pub deleted: bool,
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
    /// Source file size in bytes **observed when the upload was created**
    /// — the baseline for the §2.4 mid-resume source-change recheck. The
    /// [`ItemRecord`]'s own `size`/`mtime_unix_ns` cannot serve here: per
    /// the §2.6 coordination note they keep naming the last *published*
    /// version while a newer version is in flight, so a resume comparing
    /// against them would spuriously abort every re-upload of a changed
    /// item. `#[serde(default)]`: a row written before this field existed
    /// decodes as `0`, which reads as "source changed" — the safe
    /// direction (abort + restart, never a wrong hash).
    #[serde(default)]
    pub size: u64,
    /// Source file mtime (unix nanoseconds) observed when the upload was
    /// created — same purpose and same default posture as `size`.
    #[serde(default)]
    pub mtime_unix_ns: i64,
}

/// One completed part of a multipart upload (the `upload_parts` value).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadPart {
    /// The ETag the backend returned for the part.
    pub etag: String,
    /// The base64 `Content-MD5` we sent (resume + Complete validation).
    pub md5_b64: String,
}

/// One entry of the durable §2.3 deleted set (the `deleted_set` table
/// value): what this device knows about a relkey's deletion, published as
/// a manifest `{del, vv, server_ts}` row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeletedRecord {
    /// The deletion's version vector (bumped past the deleted version, so
    /// delete-vs-edit resolves through the §2.6 machinery).
    pub vv: VersionVector,
    /// Server time of the deletion, unix seconds (§2.10 GC age rules run
    /// on server time).
    pub server_ts: i64,
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

/// The frozen segment whose seq **span** covers `seq`, as
/// `(first_seq, entry_count)` — `None` when no span covers it. A segment
/// frozen at first seq `f` with `n` entries covers `f..f + n` (§2.2
/// per-entry seqs), so this is the "is this seq already frozen" predicate
/// both freeze paths guard with.
fn span_covering(
    segments: &impl ReadableTable<u64, (u64, &'static [u8])>,
    seq: u64,
) -> Result<Option<(u64, u64)>, StateError> {
    if let Some(entry) = segments.range(..=seq).map_err(db_err)?.next_back() {
        let (key, value) = entry.map_err(db_err)?;
        let first = key.value();
        let (count, _) = value.value();
        // `first <= seq` by the range bound, so the subtraction is safe.
        if seq - first < count {
            return Ok(Some((first, count)));
        }
    }
    Ok(None)
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

/// Reads the multipart state from any readable `uploads` table.
fn read_upload(
    uploads: &impl ReadableTable<&'static str, &'static [u8]>,
    relkey: &RelKey,
) -> Result<Option<MultipartUploadState>, StateError> {
    match uploads.get(relkey.as_str()).map_err(db_err)? {
        Some(guard) => Ok(Some(from_json(guard.value())?)),
        None => Ok(None),
    }
}

/// Reads all recorded parts for `relkey` from any readable `upload_parts`
/// table, ascending by part number.
///
/// This per-relkey range scan is an enumeration like any other: a corrupt
/// part row fails typed with [`StateError::CodecAt`] naming its row as
/// `<relkey>:<part number>` (`:` is illegal in relkeys, so the spelling is
/// unambiguous), never a bare [`StateError::Codec`] that hides which row
/// is bad.
fn read_upload_parts(
    parts: &impl ReadableTable<(&'static str, u32), &'static [u8]>,
    relkey: &RelKey,
) -> Result<Vec<(u32, UploadPart)>, StateError> {
    let rel = relkey.as_str();
    let mut out = Vec::new();
    for entry in parts.range((rel, 0u32)..=(rel, u32::MAX)).map_err(db_err)? {
        let (key, value) = entry.map_err(db_err)?;
        let (_, part_no) = key.value();
        out.push((
            part_no,
            from_json_at(&format!("{rel}:{part_no}"), value.value())?,
        ));
    }
    Ok(out)
}

/// Reads the deleted-set record for `relkey` from any readable
/// `deleted_set` table.
fn read_deleted(
    deleted: &impl ReadableTable<&'static str, &'static [u8]>,
    relkey: &RelKey,
) -> Result<Option<DeletedRecord>, StateError> {
    match deleted.get(relkey.as_str()).map_err(db_err)? {
        Some(guard) => Ok(Some(from_json(guard.value())?)),
        None => Ok(None),
    }
}

/// Reads every staged outbound record from any readable `outbound_entries`
/// table, in FIFO (ascending-id) order.
fn read_outbound(
    outbound: &impl ReadableTable<u64, &'static [u8]>,
) -> Result<Vec<(u64, Vec<u8>)>, StateError> {
    let mut out = Vec::new();
    for entry in outbound.iter().map_err(db_err)? {
        let (key, value) = entry.map_err(db_err)?;
        out.push((key.value(), value.value().to_vec()));
    }
    Ok(out)
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
    minted: bool,
}

/// A scoped write transaction over the state store (see
/// [`SyncDb::with_txn`]). Exposes every typed **mutator** [`SyncDb`] has,
/// plus the point **reads** composite steps need — [`StateTxn::get_item`],
/// [`StateTxn::has_applied`], [`StateTxn::cursor`],
/// [`StateTxn::get_upload`], [`StateTxn::upload_parts`],
/// [`StateTxn::queue_peek`], [`StateTxn::last_allocated_seq`],
/// [`StateTxn::published_cursor`] — which see the transaction's own
/// uncommitted writes. The §2.2 apply composite (`if !has_applied { mutate;
/// mark_applied; set_cursor }`) and the transfer engine's resume check
/// therefore run **entirely inside one commit**; checking through
/// [`SyncDb`]'s read methods first and then mutating in a transaction is
/// check-then-act and is correct only under the §3.3 single-supervisor
/// assumption. The enumeration scans ([`SyncDb::iter_items`],
/// [`SyncDb::items_in_state`], [`SyncDb::iter_uploads`],
/// [`SyncDb::queue_len`], …) and the seen-caches remain [`SyncDb`]-only;
/// calling back into [`SyncDb`]'s own methods from inside the closure
/// deadlocks on the open transaction.
///
/// Everything performed through one `StateTxn` commits atomically, or —
/// when the closure returns an error — nothing does.
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
    /// **Reopen should pass `None`.** Passing the cached id on every open
    /// makes an unexpectedly fresh file (whole-file loss, restore from
    /// backup) silently re-mint the same identity with the seq counter
    /// reset to zero — §2.2 (device, seq) reuse if the lost seqs were
    /// ever published. With `None`, that situation fails typed with
    /// [`StateError::DeviceIdRequired`]; a caller that must pass
    /// `Some(id)` checks [`SyncDb::minted_identity`] afterwards to detect
    /// unexpected freshness (module docs, "Identity, reopen, and database
    /// loss").
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
        let (device_id, minted) = {
            let mut meta = txn.open_table(T_META).map_err(db_err)?;
            // Schema stamp first. `Some(None)` = present but unreadable.
            let schema: Option<Option<u32>> = meta
                .get(K_SCHEMA_VERSION)
                .map_err(db_err)?
                .map(|guard| serde_json::from_slice(guard.value()).ok());
            // Only PRESENCE of the identity is checked before the schema
            // gate; the value is parsed inside the exact-match arm, so a
            // refused stamp (newer/older/unreadable) is reported as the
            // schema error even when the identity bytes are also garbage —
            // the doc'd "schema gate before any identity logic" ordering.
            let has_identity = meta.get(K_DEVICE_ID).map_err(db_err)?.is_some();
            match schema {
                // No stamp at all.
                None => {
                    // Identity without a stamp: corruption, not fresh.
                    if has_identity {
                        return Err(StateError::SchemaUnsupported { found: None });
                    }
                    // Fresh database: stamp the schema + mint identity.
                    let minted = mint_device_id.ok_or(StateError::DeviceIdRequired)?;
                    meta.insert(K_SCHEMA_VERSION, to_json(&SCHEMA_VERSION)?.as_slice())
                        .map_err(db_err)?;
                    meta.insert(K_DEVICE_ID, to_json(&minted)?.as_slice())
                        .map_err(db_err)?;
                    (minted, true)
                }
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
                // Exact match: identity must exist and parse; verify a
                // supplied id.
                Some(Some(_)) => {
                    let stored: DeviceId = match meta.get(K_DEVICE_ID).map_err(db_err)? {
                        Some(guard) => from_json(guard.value())?,
                        None => return Err(StateError::DeviceIdMissing),
                    };
                    if let Some(given) = mint_device_id {
                        if given != stored {
                            return Err(StateError::DeviceIdMismatch { stored, given });
                        }
                    }
                    (stored, false)
                }
            }
        };
        // Create every other table (no-ops when they already exist).
        txn.open_table(T_ITEMS).map_err(db_err)?;
        txn.open_table(T_APPLIED).map_err(db_err)?;
        txn.open_table(T_CURSORS).map_err(db_err)?;
        txn.open_table(T_SEGMENT_SPANS).map_err(db_err)?;
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
        txn.open_table(T_OUTBOUND).map_err(db_err)?;
        txn.open_table(T_DELETED).map_err(db_err)?;
        txn.commit().map_err(db_err)?;

        // First creation: make the new file's directory entry itself
        // durable (module docs, "Durability" — redb fsyncs the file, not
        // the parent directory, and POSIX does not promise the dirent
        // survives power loss without this).
        #[cfg(unix)]
        if minted {
            fsync_parent_dir(&path)?;
        }

        Ok(SyncDb {
            db,
            device_id,
            path,
            minted,
        })
    }

    /// This database's device identity (minted on first open).
    pub fn device_id(&self) -> &DeviceId {
        &self.device_id
    }

    /// `true` when **this** open minted the device identity — i.e. the
    /// file was fresh and [`SyncDb::open`] stamped it. A caller that
    /// passes `Some(id)` on reopen (instead of the first-class `None`
    /// pattern) must check this to detect an unexpectedly fresh file: a
    /// mint where the caller expected an existing db means the previous
    /// db — and its seq counter — was lost, and publishing under the
    /// cached identity would reuse (device, seq) pairs (§2.2; module
    /// docs, "Identity, reopen, and database loss").
    pub fn minted_identity(&self) -> bool {
        self.minted
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
        self.with_txn_err::<T, StateError>(f)
    }

    /// [`SyncDb::with_txn`] generalized over the closure's error type: `f`
    /// may fail with any `E: From<StateError>`, and an `Err` of **either**
    /// origin — a storage/codec failure from the [`StateTxn`] mutators or
    /// the caller's own domain error — aborts the transaction so nothing
    /// `f` did is visible.
    ///
    /// This is what lets the §2.2 apply composite put a *consumer's* work
    /// (whose failures are not [`StateError`]s) inside the same commit as
    /// `mark_applied` + `set_cursor`: the journal reader runs
    /// `consumer.apply(txn, entry)` through this method, so a consumer
    /// error — or a consumer panic, which drops the transaction
    /// un-committed during unwind — leaves neither the consumer's
    /// mutations nor the applied mark behind, by construction.
    ///
    /// The same closure rules as [`SyncDb::with_txn`] apply (no calling
    /// back into `self`).
    pub fn with_txn_err<T, E>(&self, f: impl FnOnce(&StateTxn<'_>) -> Result<T, E>) -> Result<T, E>
    where
        E: From<StateError>,
    {
        // An early `return Err(..)` — and a panic unwinding out of `f` —
        // drops `txn` before `commit()`, which aborts it: nothing the
        // closure did is visible. Only the success path commits.
        let txn = self.begin_write()?;
        let out = f(&StateTxn { txn: &txn })?;
        txn.commit().map_err(db_err).map_err(E::from)?;
        Ok(out)
    }

    // -- items ------------------------------------------------------------

    /// Inserts an item record **only if none exists** for `relkey`.
    /// Returns `Ok(true)` when inserted, `Ok(false)` (nothing changed) when
    /// a record already exists. This is the creation path for the state
    /// machine; existing records change only through
    /// [`SyncDb::transition`] / [`SyncDb::update_item`].
    ///
    /// Creation is restricted to the §2.4 **entry states** ([`legal_entry`]:
    /// `Dirty`, `PendingDown`, `Stub`, `Hydrated`) — a record cannot be
    /// born in a pipeline-interior state like `Uploading`
    /// ([`StateError::IllegalCreationState`]); ingest/replay paths that
    /// need an arbitrary state use the greppable
    /// [`SyncDb::replay_put_item`] bypass.
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

    /// Wholesale-replaces an **existing** item record — including its
    /// state, bypassing the §2.4 legality table like
    /// [`SyncDb::replay_put_item`] — but **guarded by compare-and-set** on
    /// the stored state: the record must exist and currently be in
    /// `expected_state`, or [`StateError::StaleState`] is returned and
    /// nothing changes.
    ///
    /// This is the apply loop's path for the semantic replacements the
    /// §2.4 table deliberately has no edge for, where the CAS-free
    /// [`SyncDb::replay_put_item`] would reopen the exact lost-update
    /// hazard [`SyncDb::update_item`] was added to close (a racing
    /// transfer-engine transition silently stomped between read and
    /// write). The two known cases:
    ///
    /// | Replace | Why there is no [`legal`] edge |
    /// |---|---|
    /// | `Stub` record → `Dirty` record (new `content_id`) | §2.8 out-of-band overwrite of an evicted original: the bytes were *replaced* on disk, not downloaded, so the `Stub → Downloading → …` download rows do not apply |
    /// | `Dirty` record → `Synced`/`Hydrated` record (merged vv) | §2.6 apply rule case 1: remote `sem_hash` (sidecars) / `content_id` (originals) equals local — converged, adopt metadata and drop the dirt with **no upload**, so the `Dirty → Queued → …` upload rows do not apply |
    /// | `Synced` original → `Hydrated` record (merged vv) | §2.6 case-1 axis normalization: for an original, the upload pipeline's `Synced` terminal and `Hydrated` describe the same physical fact (bytes present + verified locally); the converged adoption settles the record onto the §3.5 hydration axis — not a pipeline step, so no [`legal`] edge |
    ///
    /// State-machine steps stay on [`SyncDb::transition`];
    /// state-preserving mutations on [`SyncDb::update_item`]; unguarded
    /// ingest/replay on [`SyncDb::replay_put_item`]. Named `replay_*` so
    /// every legality bypass stays greppable.
    pub fn replay_put_item_cas(
        &self,
        relkey: &RelKey,
        expected_state: ItemState,
        record: &ItemRecord,
    ) -> Result<(), StateError> {
        self.with_txn(|t| t.replay_put_item_cas(relkey, expected_state, record))
    }

    /// Reads an item record (`None` when absent). A stored value that does
    /// not parse is [`StateError::Codec`], never a panic.
    pub fn get_item(&self, relkey: &RelKey) -> Result<Option<ItemRecord>, StateError> {
        let txn = self.begin_read()?;
        let items = txn.open_table(T_ITEMS).map_err(db_err)?;
        read_item(&items, relkey)
    }

    /// Deletes an item record; `Ok(true)` when it existed.
    ///
    /// This removes **only the `items` row** — an item may also own rows
    /// in the queues, the upload tables and `xmp_seen`, and deleting the
    /// record while a queue entry survives leaves an orphan the engine's
    /// documented "pop + transition" composite wedges on forever: the pop
    /// aborts with [`StateError::StaleState`]`{found: None}`, restoring
    /// the orphan to the queue head, and a blind retry loops. A full
    /// deletion (e.g. a §2.7 tombstone apply) composes, in **one**
    /// [`SyncDb::with_txn`]: [`StateTxn::delete_item`] +
    /// [`StateTxn::queue_remove`] (both queues) +
    /// [`StateTxn::clear_upload`] + [`StateTxn::remove_xmp_seen`].
    pub fn delete_item(&self, relkey: &RelKey) -> Result<bool, StateError> {
        self.with_txn(|t| t.delete_item(relkey))
    }

    /// **Corruption-recovery only**: deletes the `items` row stored under
    /// the raw key string `raw`, which need **not** be a valid relkey.
    /// `Ok(true)` when a row existed.
    ///
    /// This closes the surface-and-delete loop for **key-side** corruption:
    /// the item scans' [`StateError::CodecAt`] can name a corrupt raw key,
    /// but [`SyncDb::delete_item`] takes a validated [`RelKey`], which can
    /// never be constructed from invalid stored text — so without this
    /// method a key-side-corrupt row would wedge every enumeration forever
    /// (the uploads twin is [`SyncDb::clear_upload_raw`]; queues recover
    /// through [`SyncDb::queue_clear`]). Pass exactly the string
    /// [`StateError::CodecAt`] reported. Not a general deletion path: for
    /// valid keys use [`SyncDb::delete_item`], whose doc covers companion
    /// rows.
    pub fn delete_item_raw(&self, raw: &str) -> Result<bool, StateError> {
        self.with_txn(|t| {
            let mut items = t.txn.open_table(T_ITEMS).map_err(db_err)?;
            let previous = items.remove(raw).map_err(db_err)?;
            Ok(previous.is_some())
        })
    }

    /// Every item record, ascending by relkey. One consistent snapshot.
    ///
    /// This (with [`SyncDb::items_in_state`]) is the crash-recovery and
    /// hygiene scan surface: after a crash, items that were popped from a
    /// queue and transitioned (e.g. to `Uploading`) sit in no queue and are
    /// findable only by scanning. v1 materializes the result (libraries are
    /// bounded; records are small); a streaming iterator can replace this
    /// without changing callers' logic.
    ///
    /// A row that fails to decode aborts the scan with
    /// [`StateError::CodecAt`] naming its key, so the one bad record can
    /// be removed via [`SyncDb::delete_item`] and the scan retried —
    /// enumeration is never lost wholesale to a single corrupt row.
    pub fn iter_items(&self) -> Result<Vec<(RelKey, ItemRecord)>, StateError> {
        let txn = self.begin_read()?;
        let items = txn.open_table(T_ITEMS).map_err(db_err)?;
        let mut out = Vec::new();
        for entry in items.iter().map_err(db_err)? {
            let (key, value) = entry.map_err(db_err)?;
            let raw = key.value();
            out.push((
                from_stored_str_at(raw, raw)?,
                from_json_at(raw, value.value())?,
            ));
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
            let raw = key.value();
            let record: ItemRecord = from_json_at(raw, value.value())?;
            if record.state == state {
                out.push((from_stored_str_at(raw, raw)?, record));
            }
        }
        Ok(out)
    }

    /// Number of items currently in `state` (§3.3/§3.8 "N edits not backed
    /// up" / `dirty_unbacked` feed).
    ///
    /// Cost: like the other enumeration accessors, this decodes **every**
    /// item record (a full-table JSON scan) — fine for recovery,
    /// verification and seeding, wrong for a hot path. The §3.3/§3.8
    /// status feed ticks at up to 1 Hz: the engine must seed in-memory
    /// per-state counters from one scan at startup and maintain them on
    /// its own transition/insert/delete calls, reaching for this accessor
    /// only to (re-)verify — e.g. after crash recovery — never per tick.
    pub fn count_in_state(&self, state: ItemState) -> Result<u64, StateError> {
        let txn = self.begin_read()?;
        let items = txn.open_table(T_ITEMS).map_err(db_err)?;
        let mut count = 0u64;
        for entry in items.iter().map_err(db_err)? {
            let (key, value) = entry.map_err(db_err)?;
            let record: ItemRecord = from_json_at(key.value(), value.value())?;
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

    /// Advances `device`'s cursor to `max(stored, seq)`.
    ///
    /// The cursor is §2.2's "highest contiguously-applied seq": §2.3
    /// bootstrap legitimately jumps it forward, but nothing legitimately
    /// moves it back, so — like [`SyncDb::mark_published`]'s cursor — a
    /// lower value is a committed no-op rather than a regression. A
    /// stale or racing apply-loop write-back therefore cannot rewind the
    /// invariant; the applied-set dedup already makes any re-apply after
    /// such a stale write idempotent.
    pub fn set_cursor(&self, device: &DeviceId, seq: u64) -> Result<(), StateError> {
        self.with_txn(|t| t.set_cursor(device, seq))
    }

    /// Every known peer cursor, as `(device, highest contiguously-applied
    /// seq)` ascending by device id — the full `cursors` map, where
    /// [`SyncDb::cursor`] is the point read.
    ///
    /// The engine needs the whole map in two places: the §1.2
    /// device-registry heartbeat publishes `applied: {device: seq}` for
    /// every known peer, and §2.3 bootstrap/catch-up compares the merged
    /// cursors against the journal's segments. Without this accessor the
    /// engine would have to keep a shadow map of peers outside the durable
    /// store. A device appears once a cursor has been set for it
    /// ([`SyncDb::set_cursor`]); peers this device has never applied from
    /// read as absent here (their cursor is implicitly 0), and enumerating
    /// *those* is the device registry's concern, not this table's.
    pub fn iter_cursors(&self) -> Result<Vec<(DeviceId, u64)>, StateError> {
        let txn = self.begin_read()?;
        let cursors = txn.open_table(T_CURSORS).map_err(db_err)?;
        let mut out = Vec::new();
        for entry in cursors.iter().map_err(db_err)? {
            let (key, value) = entry.map_err(db_err)?;
            let raw = key.value();
            out.push((from_stored_str_at(raw, raw)?, value.value()));
        }
        Ok(out)
    }

    /// Records that `device`'s segment at filename seq `first_seq` spans
    /// entries `first_seq..=last_seq`, as observed from a fully-applied
    /// decode. Published segments are immutable (§2.2), so a recorded span
    /// is a permanent fact; re-recording overwrites (the values are equal
    /// for a conforming journal). The §2.2 steady-state poll uses it to
    /// skip cursor-covered segments without a GET.
    pub fn set_segment_span(
        &self,
        device: &DeviceId,
        first_seq: u64,
        last_seq: u64,
    ) -> Result<(), StateError> {
        self.with_txn(|t| t.set_segment_span(device, first_seq, last_seq))
    }

    /// The recorded last entry seq of `device`'s segment at `first_seq`
    /// (`None` when the segment was never fully applied by this device).
    pub fn segment_span(
        &self,
        device: &DeviceId,
        first_seq: u64,
    ) -> Result<Option<u64>, StateError> {
        let txn = self.begin_read()?;
        let spans = txn.open_table(T_SEGMENT_SPANS).map_err(db_err)?;
        Ok(spans
            .get((device.as_str(), first_seq))
            .map_err(db_err)?
            .map(|guard| guard.value()))
    }

    // -- journal publication (§2.1.5) --------------------------------------

    /// Allocates the next `entry_count` seqs and freezes
    /// `build(first_seq)` as the segment covering them, **in one committed
    /// transaction** — the §2.1.5 "persisted transactionally with the
    /// serialized segment bytes" primitive, and the engine's publication
    /// path. Because the counter advance and the bytes commit together, a
    /// crash can never leave allocated-but-never-frozen seqs: **holes are
    /// impossible by construction** (the frozen spans tile the allocated
    /// range exactly), so a remote reader's contiguity cursor (§2.2/§3.2)
    /// can always eventually advance past every seq this device publishes.
    ///
    /// §2.2 seqs are per-**entry**: a segment holds up to
    /// [`crate::journal::SEGMENT_MAX_ENTRIES`] entries, each embedding its
    /// own seq, and the segment's filename is the seq of the *first*
    /// entry. The builder receives that first seq and must stamp its
    /// `entry_count` entries with `first_seq..first_seq + entry_count`
    /// (its own argument), in order. It runs inside the open transaction,
    /// so it must be side-effect-free — but it **may fail**: the intended
    /// builder is [`crate::journal::encode_segment`] over the stamped
    /// entries (`|first| Ok(encode_segment(&stamp(first))?)` — a
    /// [`JournalError`] converts into [`StateError::SegmentBuild`]), and
    /// its byte cap can be crossed only once the real seq digits are
    /// stamped. A builder `Err` aborts the whole transaction as a typed
    /// error: **no seqs are consumed, nothing is stored**, and the caller
    /// can shrink the batch and retry without leaving a hole.
    ///
    /// Returns the first seq. `entry_count` of zero is
    /// [`StateError::EmptySegment`]. Spans are strictly increasing and
    /// never overlap, starting at 1, across threads **and** across
    /// crash/reopen — so a remote device's per-entry `(device, seq)`
    /// apply-dedup can never mistake a later segment's entries for
    /// already-applied ones.
    ///
    /// (The store cannot verify the builder's stamping; the journal
    /// encoder and this method sharing one `entry_count` argument is the
    /// contract.)
    pub fn freeze_next_segment(
        &self,
        entry_count: u64,
        build: impl FnOnce(u64) -> Result<Vec<u8>, StateError>,
    ) -> Result<u64, StateError> {
        self.with_txn(|t| t.freeze_next_segment(entry_count, build))
    }

    /// Allocates and durably commits the next journal seq for this device,
    /// **without** freezing bytes.
    ///
    /// Strictly increasing, starting at 1; the allocation itself is a
    /// committed transaction, so a seq this method returned is never handed
    /// out again — across threads **and** across crash/reopen (§2.2
    /// monotonicity).
    ///
    /// **Production code must not use this pair** — the engine's
    /// publication path uses the single-transaction
    /// [`SyncDb::freeze_next_segment`]. A crash between this allocation
    /// and the matching [`SyncDb::freeze_segment`] leaves the seq a
    /// permanent hole, which (a) stalls remote contiguity cursors and
    /// (b) pins the published floor below it for the life of the device,
    /// degrading **every** future publish pass to a rescan of all
    /// published segments above the hole
    /// ([`SyncDb::unpublished_segments`]). The two-step pair survives for
    /// tests (hole/out-of-order coverage) and is a candidate for gating
    /// behind the `test-util` feature in a later unit.
    pub fn allocate_seq(&self) -> Result<u64, StateError> {
        self.with_txn(|t| t.allocate_seq())
    }

    /// The highest seq allocated so far (0 when none).
    pub fn last_allocated_seq(&self) -> Result<u64, StateError> {
        let txn = self.begin_read()?;
        let meta = txn.open_table(T_META).map_err(db_err)?;
        Ok(meta_get(&meta, K_LAST_SEQ)?.unwrap_or(0))
    }

    /// Durably freezes `bytes` as a **single-entry** segment for an
    /// already-allocated `seq` — the second half of the legacy two-step
    /// pair (see [`SyncDb::allocate_seq`]; new code uses
    /// [`SyncDb::freeze_next_segment`]). The stored bytes are the segment's
    /// identity: a crash-replayed publish re-reads them via
    /// [`SyncDb::unpublished_segments`] byte-identically.
    ///
    /// `seq` must have been allocated ([`StateError::SeqNotAllocated`]) and
    /// not already frozen — neither under exactly `seq` nor covered by a
    /// multi-entry segment's span ([`StateError::AlreadyFrozen`] — a
    /// segment is frozen exactly once, replay never re-freezes, and spans
    /// never overlap).
    pub fn freeze_segment(&self, seq: u64, bytes: &[u8]) -> Result<(), StateError> {
        self.with_txn(|t| t.freeze_segment(seq, bytes))
    }

    /// All frozen-but-unpublished segments as `(first_seq, bytes)`, in
    /// ascending seq order, each with the exact bytes passed to
    /// [`SyncDb::freeze_segment`] / [`SyncDb::freeze_next_segment`] (byte
    /// identity is the §2.1.5 republish guarantee; the first seq is the
    /// §2.2 segment filename).
    ///
    /// The scan starts at the contiguous published prefix (the "published
    /// floor" maintained by [`SyncDb::mark_published`]), not at seq 1, so a
    /// publish pass costs O(pending), not O(every segment ever frozen) —
    /// published bytes are retained in v1 (pruning is a later unit's GC
    /// concern) but are not rescanned.
    ///
    /// That cost claim has one exception: an allocated-but-never-frozen
    /// seq — reachable **only** through the legacy pair of
    /// [`SyncDb::allocate_seq`] and [`SyncDb::freeze_segment`], never
    /// through [`SyncDb::freeze_next_segment`] — pins the floor below it until the
    /// hole is frozen. Correctness is unaffected (nothing is lost or
    /// duplicated), but every pass then re-iterates and per-seq-probes all
    /// published segments above the hole, permanently. This is why the
    /// legacy pair is tests-only (its doc).
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
                let (_, bytes) = value.value();
                out.push((seq, bytes.to_vec()));
            }
        }
        Ok(out)
    }

    /// Marks a frozen segment as published (the post-PUT §2.1.5 step) and
    /// advances the published cursor over the segment's whole seq span —
    /// `max(cursor, first_seq + entry_count - 1)`.
    ///
    /// `seq` is the segment's **first** seq, the value
    /// [`SyncDb::freeze_next_segment`] returned (and the §2.2 filename
    /// seq). Idempotent: re-marking a published segment is a no-op `Ok`.
    /// Marking a seq that is not a frozen segment's first seq is
    /// [`StateError::NotFrozen`] — including a multi-entry segment's
    /// interior seqs, which are published with their segment, never
    /// individually. Out-of-order marking is allowed (crash replay
    /// publishes in order, but the store does not enforce it); an earlier
    /// still-unpublished segment remains in
    /// [`SyncDb::unpublished_segments`]. Frozen bytes are retained after
    /// publish in v1 (pruning is a later unit's GC concern).
    pub fn mark_published(&self, seq: u64) -> Result<(), StateError> {
        self.with_txn(|t| t.mark_published(seq))
    }

    /// The highest seq covered by any published segment's span (0 when
    /// nothing published yet).
    pub fn published_cursor(&self) -> Result<u64, StateError> {
        let txn = self.begin_read()?;
        let meta = txn.open_table(T_META).map_err(db_err)?;
        Ok(meta_get(&meta, K_PUBLISHED_CURSOR)?.unwrap_or(0))
    }

    // -- outbound staging (§2.1.5 publisher lane) --------------------------

    /// Durably stages one outbound journal-entry record (opaque `bytes`;
    /// the publisher owns the encoding) at the back of the FIFO staging
    /// lane, returning its staging id. Ids are strictly increasing across
    /// threads and crash/reopen (persisted counter, same overflow refusal
    /// as the other counters: [`StateError::CounterSaturated`]).
    ///
    /// Staged records survive until [`StateTxn::remove_outbound`] — which
    /// the publisher calls **in the same transaction** as
    /// [`StateTxn::freeze_next_segment`], so a staged entry is either
    /// still staged or covered by frozen segment bytes, never neither
    /// (§2.1.5: no outbound record is ever lost to a crash between
    /// staging and freezing).
    pub fn stage_outbound(&self, bytes: &[u8]) -> Result<u64, StateError> {
        self.with_txn(|t| t.stage_outbound(bytes))
    }

    /// Every staged outbound record as `(staging id, bytes)`, in FIFO
    /// (ascending-id) order — the publisher's drain scan.
    pub fn iter_outbound(&self) -> Result<Vec<(u64, Vec<u8>)>, StateError> {
        let txn = self.begin_read()?;
        let outbound = txn.open_table(T_OUTBOUND).map_err(db_err)?;
        read_outbound(&outbound)
    }

    /// Number of staged outbound records.
    pub fn outbound_len(&self) -> Result<u64, StateError> {
        let txn = self.begin_read()?;
        let outbound = txn.open_table(T_OUTBOUND).map_err(db_err)?;
        outbound.len().map_err(db_err)
    }

    // -- deleted set (§2.3) ------------------------------------------------

    /// Durably records (or replaces) this device's knowledge of `relkey`'s
    /// deletion — the row [`crate::manifest`] publishes as
    /// `{del, vv, server_ts}`. Retention/pruning is the §2.10 GC unit's
    /// concern.
    pub fn record_deleted(
        &self,
        relkey: &RelKey,
        record: &DeletedRecord,
    ) -> Result<(), StateError> {
        self.with_txn(|t| t.record_deleted(relkey, record))
    }

    /// Reads the deleted-set record for `relkey` (`None` when this device
    /// knows of no deletion).
    pub fn get_deleted(&self, relkey: &RelKey) -> Result<Option<DeletedRecord>, StateError> {
        let txn = self.begin_read()?;
        let deleted = txn.open_table(T_DELETED).map_err(db_err)?;
        read_deleted(&deleted, relkey)
    }

    /// Every deleted-set record, ascending by relkey (the manifest
    /// builder's scan). A row that fails to decode aborts with
    /// [`StateError::CodecAt`] naming its key, like the other
    /// enumerations.
    pub fn iter_deleted(&self) -> Result<Vec<(RelKey, DeletedRecord)>, StateError> {
        let txn = self.begin_read()?;
        let deleted = txn.open_table(T_DELETED).map_err(db_err)?;
        let mut out = Vec::new();
        for entry in deleted.iter().map_err(db_err)? {
            let (key, value) = entry.map_err(db_err)?;
            let raw = key.value();
            out.push((
                from_stored_str_at(raw, raw)?,
                from_json_at(raw, value.value())?,
            ));
        }
        Ok(out)
    }

    /// Removes the deleted-set record for `relkey` (§2.10 retention
    /// expiry, and the §2.7 deletion-superseded withdrawal). `Ok(true)`
    /// when a record existed.
    pub fn remove_deleted(&self, relkey: &RelKey) -> Result<bool, StateError> {
        self.with_txn(|t| t.remove_deleted(relkey))
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
        read_upload(&uploads, relkey)
    }

    /// Every in-flight multipart upload, ascending by relkey — the §2.4
    /// stale-upload hygiene scan ("engine aborts its own `upload_id`s older
    /// than 7 days") and the post-crash resume scan.
    ///
    /// A row that fails to decode aborts the scan with
    /// [`StateError::CodecAt`] naming its key (recovery:
    /// [`SyncDb::clear_upload`] that one key, then rescan).
    pub fn iter_uploads(&self) -> Result<Vec<(RelKey, MultipartUploadState)>, StateError> {
        let txn = self.begin_read()?;
        let uploads = txn.open_table(T_UPLOADS).map_err(db_err)?;
        let mut out = Vec::new();
        for entry in uploads.iter().map_err(db_err)? {
            let (key, value) = entry.map_err(db_err)?;
            let raw = key.value();
            out.push((
                from_stored_str_at(raw, raw)?,
                from_json_at(raw, value.value())?,
            ));
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
        read_upload_parts(&parts, relkey)
    }

    /// Removes the multipart state **and all recorded parts** for `relkey`
    /// in one transaction (upload completed or aborted). Idempotent.
    pub fn clear_upload(&self, relkey: &RelKey) -> Result<(), StateError> {
        self.with_txn(|t| t.clear_upload(relkey))
    }

    /// **Corruption-recovery only**: removes the `uploads` row **and every
    /// `upload_parts` row** stored under the raw key string `raw`, which
    /// need not be a valid relkey — the key-side-corruption twin of
    /// [`SyncDb::delete_item_raw`] (see its doc for the rationale;
    /// [`SyncDb::clear_upload`] takes a validated [`RelKey`] and cannot
    /// name a corrupt stored key). Idempotent. Pass exactly the string
    /// [`StateError::CodecAt`] reported.
    pub fn clear_upload_raw(&self, raw: &str) -> Result<(), StateError> {
        self.with_txn(|t| {
            let mut uploads = t.txn.open_table(T_UPLOADS).map_err(db_err)?;
            uploads.remove(raw).map_err(db_err)?;
            let mut parts = t.txn.open_table(T_UPLOAD_PARTS).map_err(db_err)?;
            let part_nos: Vec<u32> = parts
                .range((raw, 0u32)..=(raw, u32::MAX))
                .map_err(db_err)?
                .map(|entry| entry.map(|(key, _)| key.value().1))
                .collect::<Result<_, _>>()
                .map_err(db_err)?;
            for part_no in part_nos {
                parts.remove((raw, part_no)).map_err(db_err)?;
            }
            Ok(())
        })
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
            Some((class, _, rel)) => Ok(Some((from_stored_str_at::<RelKey>(&rel, &rel)?, class))),
        }
    }

    /// Removes and returns the head of queue `q` — lowest class, then
    /// earliest arrival — or `None` when empty.
    ///
    /// A stored relkey that fails validation on rehydration (on-disk
    /// corruption) is [`StateError::Codec`] and the row **stays**: every
    /// subsequent pop fails on the same head, and [`SyncDb::queue_remove`]
    /// cannot name it (it takes a validated [`RelKey`]). The recovery path
    /// is [`SyncDb::queue_clear`] + re-enqueue from
    /// [`SyncDb::items_in_state`] (`Queued` / `PendingDown`), the same
    /// source of truth post-crash recovery already rebuilds from.
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

    /// Removes **every** entry from queue `q` without decoding any stored
    /// relkey, returning how many were removed. Idempotent (`Ok(0)` when
    /// already empty).
    ///
    /// This is the queue's degradation escape hatch: a corrupt stored row
    /// wedges [`SyncDb::queue_pop`] permanently (its doc) and is not
    /// addressable by [`SyncDb::queue_remove`], so without a raw drain the
    /// queue would be the one subsystem with no recovery path (a corrupt
    /// *item* is still deletable via [`SyncDb::delete_item`] — the item
    /// scans' [`StateError::CodecAt`] names its key). After
    /// clearing, rebuild from [`SyncDb::items_in_state`] — item state, not
    /// queue membership, is the crash-recovery source of truth
    /// ([`SyncDb::iter_items`] docs) — accepting re-derived priorities and
    /// FIFO order for the drained entries.
    pub fn queue_clear(&self, q: Queue) -> Result<u64, StateError> {
        self.with_txn(|t| t.queue_clear(q))
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

    /// The persisted outcome of the §2.4 setup probe ("does this backend
    /// reject a deliberately wrong `Content-MD5`?"), or `None` before the
    /// probe has ever run against this backend. `Some(false)` is the
    /// `requires_readback_verify` condition: the `verifying` step must
    /// perform a full ranged-GET re-hash because the backend performs no
    /// digest verification of its own.
    pub fn backend_digest_rejection(&self) -> Result<Option<bool>, StateError> {
        let txn = self.begin_read()?;
        let meta = txn.open_table(T_META).map_err(db_err)?;
        meta_get(&meta, K_BACKEND_DIGEST_REJECTION)
    }

    /// Persists the §2.4 setup-probe outcome (see
    /// [`SyncDb::backend_digest_rejection`]). Overwrites any previous
    /// probe result — re-probing against a reconfigured backend must win.
    pub fn set_backend_digest_rejection(&self, works: bool) -> Result<(), StateError> {
        self.with_txn(|t| {
            let mut meta = t.txn.open_table(T_META).map_err(db_err)?;
            meta.insert(K_BACKEND_DIGEST_REJECTION, to_json(&works)?.as_slice())
                .map_err(db_err)?;
            Ok(())
        })
    }

    /// The server time (unix seconds) through which this device has provably
    /// applied every deletion (§2.3/§2.10 pre-upload quarantine input), or
    /// `None` before any catch-up has recorded one. See
    /// [`K_APPLIED_PROOF_SERVER_TS`].
    pub fn applied_proof_server_ts(&self) -> Result<Option<i64>, StateError> {
        let txn = self.begin_read()?;
        let meta = txn.open_table(T_META).map_err(db_err)?;
        meta_get(&meta, K_APPLIED_PROOF_SERVER_TS)
    }

    /// Records the deletion-knowledge proof horizon (see
    /// [`SyncDb::applied_proof_server_ts`]). Monotonic in intent but not
    /// enforced here — the engine advances it only forward.
    pub fn set_applied_proof_server_ts(&self, server_ts: i64) -> Result<(), StateError> {
        self.with_txn(|t| {
            let mut meta = t.txn.open_table(T_META).map_err(db_err)?;
            meta.insert(K_APPLIED_PROOF_SERVER_TS, to_json(&server_ts)?.as_slice())
                .map_err(db_err)?;
            Ok(())
        })
    }

    /// The **server** time (unix seconds) at which this device published the
    /// own-prefix segment whose first entry is `first_seq`, or `None` if
    /// unstamped. §2.10 segment compaction's 14-day cap is measured against
    /// this — a server-time fact, never the local clock, so two differently
    /// skewed devices age the same segment identically. Stored under a
    /// per-segment meta key.
    pub fn segment_published_server_ts(&self, first_seq: u64) -> Result<Option<i64>, StateError> {
        let txn = self.begin_read()?;
        let meta = txn.open_table(T_META).map_err(db_err)?;
        meta_get(&meta, &segment_pub_ts_key(first_seq))
    }

    /// Stamps the server-time publication instant of the own-prefix segment
    /// at `first_seq` (see [`SyncDb::segment_published_server_ts`]). The
    /// publisher stamps this as it marks a segment published; compaction
    /// reads it for the 14-day cap.
    pub fn set_segment_published_server_ts(
        &self,
        first_seq: u64,
        server_ts: i64,
    ) -> Result<(), StateError> {
        self.with_txn(|t| {
            let mut meta = t.txn.open_table(T_META).map_err(db_err)?;
            meta.insert(
                segment_pub_ts_key(first_seq).as_str(),
                to_json(&server_ts)?.as_slice(),
            )
            .map_err(db_err)?;
            Ok(())
        })
    }

    // -- test support (not part of the supported API) ----------------------
    //
    // The force_* tamper/corruption helpers are compiled only under the
    // `test-util` cargo feature (enabled for this crate's own tests via a
    // self-dev-dependency), so release builds of the library do not ship
    // methods that can corrupt the invariants this store exists to
    // protect (regress last_seq, strip identity, store unparseable
    // records).

    /// Test support only: overwrite (or remove, with `None`) the stored
    /// schema version so the open-time gates can be exercised. Not part of
    /// the supported API.
    #[cfg(feature = "test-util")]
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
    #[cfg(feature = "test-util")]
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
    #[cfg(feature = "test-util")]
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
    #[cfg(feature = "test-util")]
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
    #[cfg(feature = "test-util")]
    #[doc(hidden)]
    pub fn force_corrupt_item(&self, relkey: &RelKey, bytes: &[u8]) -> Result<(), StateError> {
        self.with_txn(|t| {
            let mut items = t.txn.open_table(T_ITEMS).map_err(db_err)?;
            items.insert(relkey.as_str(), bytes).map_err(db_err)?;
            Ok(())
        })
    }

    /// Test support only: store raw (typically unparseable) bytes as the
    /// multipart upload state for `relkey`, so the keyed enumeration
    /// error ([`StateError::CodecAt`]) can be pinned on the uploads scan.
    #[cfg(feature = "test-util")]
    #[doc(hidden)]
    pub fn force_corrupt_upload(&self, relkey: &RelKey, bytes: &[u8]) -> Result<(), StateError> {
        self.with_txn(|t| {
            let mut uploads = t.txn.open_table(T_UPLOADS).map_err(db_err)?;
            uploads.insert(relkey.as_str(), bytes).map_err(db_err)?;
            Ok(())
        })
    }

    /// Test support only: enqueue a raw string that is NOT a valid relkey
    /// (modeling on-disk corruption of a queue row), so the documented
    /// wedged-pop behavior and the [`SyncDb::queue_clear`] recovery path
    /// can be pinned.
    #[cfg(feature = "test-util")]
    #[doc(hidden)]
    pub fn force_corrupt_queue_row(
        &self,
        q: Queue,
        class: u8,
        raw: &str,
    ) -> Result<(), StateError> {
        self.with_txn(|t| {
            let (entries_def, idx_def) = q.tables();
            let arrival = {
                let mut meta = t.txn.open_table(T_META).map_err(db_err)?;
                bump_counter_by(&mut meta, K_QUEUE_ARRIVAL, 1)?
            };
            let mut entries = t.txn.open_table(entries_def).map_err(db_err)?;
            entries.insert((class, arrival), raw).map_err(db_err)?;
            let mut idx = t.txn.open_table(idx_def).map_err(db_err)?;
            idx.insert(raw, (class, arrival)).map_err(db_err)?;
            Ok(())
        })
    }

    /// Test support only: store raw (typically unparseable) bytes as the
    /// item record under a raw **key** string that need not be a valid
    /// relkey (modeling key-side on-disk corruption), so the
    /// [`StateError::CodecAt`]-surface-then-[`SyncDb::delete_item_raw`]
    /// recovery loop can be pinned.
    #[cfg(feature = "test-util")]
    #[doc(hidden)]
    pub fn force_corrupt_item_key(&self, raw: &str, bytes: &[u8]) -> Result<(), StateError> {
        self.with_txn(|t| {
            let mut items = t.txn.open_table(T_ITEMS).map_err(db_err)?;
            items.insert(raw, bytes).map_err(db_err)?;
            Ok(())
        })
    }

    /// Test support only: store raw bytes as the multipart upload state
    /// under a raw key string that need not be a valid relkey, plus one
    /// part row under the same raw key — the uploads twin of
    /// [`SyncDb::force_corrupt_item_key`], pinning the
    /// [`SyncDb::clear_upload_raw`] recovery loop.
    #[cfg(feature = "test-util")]
    #[doc(hidden)]
    pub fn force_corrupt_upload_key(&self, raw: &str, bytes: &[u8]) -> Result<(), StateError> {
        self.with_txn(|t| {
            let mut uploads = t.txn.open_table(T_UPLOADS).map_err(db_err)?;
            uploads.insert(raw, bytes).map_err(db_err)?;
            let mut parts = t.txn.open_table(T_UPLOAD_PARTS).map_err(db_err)?;
            parts.insert((raw, 1u32), bytes).map_err(db_err)?;
            Ok(())
        })
    }

    /// Test support only: store raw (typically unparseable) bytes as one
    /// recorded part of `raw_rel`'s multipart upload, so the per-part
    /// enumeration's keyed error contract can be pinned.
    #[cfg(feature = "test-util")]
    #[doc(hidden)]
    pub fn force_corrupt_upload_part(
        &self,
        raw_rel: &str,
        part_no: u32,
        bytes: &[u8],
    ) -> Result<(), StateError> {
        self.with_txn(|t| {
            let mut parts = t.txn.open_table(T_UPLOAD_PARTS).map_err(db_err)?;
            parts.insert((raw_rel, part_no), bytes).map_err(db_err)?;
            Ok(())
        })
    }

    /// Test support only: store raw (typically unparseable) bytes as the
    /// device identity, so the open gate's schema-before-identity ordering
    /// can be pinned (a refused schema stamp must win over garbage
    /// identity bytes).
    #[cfg(feature = "test-util")]
    #[doc(hidden)]
    pub fn force_corrupt_device_id(&self, bytes: &[u8]) -> Result<(), StateError> {
        self.with_txn(|t| {
            let mut meta = t.txn.open_table(T_META).map_err(db_err)?;
            meta.insert(K_DEVICE_ID, bytes).map_err(db_err)?;
            Ok(())
        })
    }
}

/// Advances a persisted u64 meta counter by `n` (≥ 1), refusing to wrap
/// ([`StateError::CounterSaturated`]). Returns the **first** of the `n`
/// newly-allocated values (`stored + 1`); the counter is left at
/// `stored + n`.
fn bump_counter_by(
    meta: &mut redb::Table<'_, &'static str, &'static [u8]>,
    key: &'static str,
    n: u64,
) -> Result<u64, StateError> {
    debug_assert!(n >= 1, "bump_counter_by requires n >= 1");
    let last: u64 = meta_get(meta, key)?.unwrap_or(0);
    let new_last = last
        .checked_add(n)
        .ok_or(StateError::CounterSaturated { key })?;
    meta.insert(key, to_json(&new_last)?.as_slice())
        .map_err(db_err)?;
    // `last < new_last`, so `last + 1` cannot overflow.
    Ok(last + 1)
}

impl StateTxn<'_> {
    /// [`SyncDb::get_item`] within this transaction (sees the
    /// transaction's own uncommitted writes).
    pub fn get_item(&self, relkey: &RelKey) -> Result<Option<ItemRecord>, StateError> {
        let items = self.txn.open_table(T_ITEMS).map_err(db_err)?;
        read_item(&items, relkey)
    }

    /// [`SyncDb::has_applied`] within this transaction — the §2.2 apply
    /// loop's dedup check, inside the same commit as the apply itself.
    pub fn has_applied(&self, device: &DeviceId, seq: u64) -> Result<bool, StateError> {
        let applied = self.txn.open_table(T_APPLIED).map_err(db_err)?;
        let found = applied
            .get((device.as_str(), seq))
            .map_err(db_err)?
            .is_some();
        Ok(found)
    }

    /// [`SyncDb::cursor`] within this transaction.
    pub fn cursor(&self, device: &DeviceId) -> Result<u64, StateError> {
        let cursors = self.txn.open_table(T_CURSORS).map_err(db_err)?;
        let stored = cursors
            .get(device.as_str())
            .map_err(db_err)?
            .map(|guard| guard.value())
            .unwrap_or(0);
        Ok(stored)
    }

    /// [`SyncDb::get_upload`] within this transaction — the transfer
    /// engine's resume check, inside the same commit as the decision it
    /// gates.
    pub fn get_upload(&self, relkey: &RelKey) -> Result<Option<MultipartUploadState>, StateError> {
        let uploads = self.txn.open_table(T_UPLOADS).map_err(db_err)?;
        read_upload(&uploads, relkey)
    }

    /// [`SyncDb::upload_parts`] within this transaction.
    pub fn upload_parts(&self, relkey: &RelKey) -> Result<Vec<(u32, UploadPart)>, StateError> {
        let parts = self.txn.open_table(T_UPLOAD_PARTS).map_err(db_err)?;
        read_upload_parts(&parts, relkey)
    }

    /// [`SyncDb::queue_peek`] within this transaction.
    pub fn queue_peek(&self, q: Queue) -> Result<Option<(RelKey, u8)>, StateError> {
        let (entries_def, _) = q.tables();
        let entries = self.txn.open_table(entries_def).map_err(db_err)?;
        match queue_head(&entries)? {
            None => Ok(None),
            Some((class, _, rel)) => Ok(Some((from_stored_str_at::<RelKey>(&rel, &rel)?, class))),
        }
    }

    /// [`SyncDb::last_allocated_seq`] within this transaction.
    pub fn last_allocated_seq(&self) -> Result<u64, StateError> {
        let meta = self.txn.open_table(T_META).map_err(db_err)?;
        Ok(meta_get(&meta, K_LAST_SEQ)?.unwrap_or(0))
    }

    /// [`SyncDb::published_cursor`] within this transaction.
    pub fn published_cursor(&self) -> Result<u64, StateError> {
        let meta = self.txn.open_table(T_META).map_err(db_err)?;
        Ok(meta_get(&meta, K_PUBLISHED_CURSOR)?.unwrap_or(0))
    }

    /// [`SyncDb::insert_item`] within this transaction.
    pub fn insert_item(&self, relkey: &RelKey, record: &ItemRecord) -> Result<bool, StateError> {
        if !legal_entry(record.state) {
            return Err(StateError::IllegalCreationState {
                relkey: relkey.clone(),
                state: record.state,
            });
        }
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

    /// [`SyncDb::replay_put_item_cas`] within this transaction — the §2.6
    /// case-1 converged adoption composes with [`StateTxn::mark_applied`]
    /// / [`StateTxn::set_cursor`] in one commit here.
    pub fn replay_put_item_cas(
        &self,
        relkey: &RelKey,
        expected_state: ItemState,
        record: &ItemRecord,
    ) -> Result<(), StateError> {
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
        let value = to_json(record)?;
        items
            .insert(relkey.as_str(), value.as_slice())
            .map_err(db_err)?;
        Ok(())
    }

    /// [`SyncDb::delete_item`] within this transaction — see that doc's
    /// companion-removal warning: a full deletion composes this with
    /// [`StateTxn::queue_remove`] (both queues), [`StateTxn::clear_upload`]
    /// and [`StateTxn::remove_xmp_seen`] in the same closure.
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

    /// [`SyncDb::set_cursor`] within this transaction (advance-only:
    /// `max(stored, seq)` wins, a lower value is a committed no-op).
    pub fn set_cursor(&self, device: &DeviceId, seq: u64) -> Result<(), StateError> {
        let mut cursors = self.txn.open_table(T_CURSORS).map_err(db_err)?;
        let stored = cursors
            .get(device.as_str())
            .map_err(db_err)?
            .map(|guard| guard.value())
            .unwrap_or(0);
        if seq > stored {
            cursors.insert(device.as_str(), seq).map_err(db_err)?;
        }
        Ok(())
    }

    /// [`SyncDb::set_segment_span`] within this transaction.
    pub fn set_segment_span(
        &self,
        device: &DeviceId,
        first_seq: u64,
        last_seq: u64,
    ) -> Result<(), StateError> {
        let mut spans = self.txn.open_table(T_SEGMENT_SPANS).map_err(db_err)?;
        spans
            .insert((device.as_str(), first_seq), last_seq)
            .map_err(db_err)?;
        Ok(())
    }

    /// [`SyncDb::freeze_next_segment`] within this transaction — e.g. the
    /// §2.4 `verifying → synced` step, which freezes the journal entry and
    /// transitions the item in one commit. A builder `Err` propagated out
    /// of the closure aborts the whole composite; it may instead be
    /// **caught inside the closure** (shrink the batch and retry in the
    /// same commit): the seq counter is restored before the error
    /// returns, so the failed attempt consumes no seqs either way
    /// ([`StateError::SegmentBuild`]).
    pub fn freeze_next_segment(
        &self,
        entry_count: u64,
        build: impl FnOnce(u64) -> Result<Vec<u8>, StateError>,
    ) -> Result<u64, StateError> {
        if entry_count == 0 {
            return Err(StateError::EmptySegment);
        }
        let first = {
            let mut meta = self.txn.open_table(T_META).map_err(db_err)?;
            bump_counter_by(&mut meta, K_LAST_SEQ, entry_count)?
        };
        // Every typed refusal below restores the counter to its prior
        // value before returning. Within this still-open write transaction
        // the bump is not yet observable to anyone, so the restore is
        // race-free — and it is what makes the SegmentBuild "no seqs were
        // consumed" contract true at THIS level too: a caller that catches
        // the error inside a `with_txn` closure and shrink-retries in the
        // same commit (the pattern the error doc invites) would otherwise
        // commit the failed attempt's seqs as a permanent
        // allocated-never-frozen hole — stalling remote §2.2 contiguity
        // cursors and pinning the published floor for the life of the
        // device. (The meta table is reopened here, not held across
        // `build`, so a builder that captures the enclosing [`StateTxn`]
        // can still read meta-backed values.)
        let restore = || {
            let mut meta = self.txn.open_table(T_META).map_err(db_err)?;
            meta.insert(K_LAST_SEQ, to_json(&(first - 1))?.as_slice())
                .map_err(db_err)?;
            Ok::<(), StateError>(())
        };
        let mut segments = self.txn.open_table(T_SEGMENTS).map_err(db_err)?;
        // Defensive: freshly allocated seqs cannot already be covered
        // unless the counter was tampered backwards; refuse rather than
        // overwrite (checks both an earlier span reaching into ours and
        // any segment keyed at or above our first seq).
        if span_covering(&segments, first)?.is_some()
            || segments.range(first..).map_err(db_err)?.next().is_some()
        {
            restore()?;
            return Err(StateError::AlreadyFrozen { seq: first });
        }
        let bytes = match build(first) {
            Ok(bytes) => bytes,
            Err(e) => {
                restore()?;
                return Err(e);
            }
        };
        segments
            .insert(first, (entry_count, bytes.as_slice()))
            .map_err(db_err)?;
        Ok(first)
    }

    /// [`SyncDb::allocate_seq`] within this transaction.
    pub fn allocate_seq(&self) -> Result<u64, StateError> {
        let mut meta = self.txn.open_table(T_META).map_err(db_err)?;
        bump_counter_by(&mut meta, K_LAST_SEQ, 1)
    }

    /// [`SyncDb::freeze_segment`] within this transaction.
    pub fn freeze_segment(&self, seq: u64, bytes: &[u8]) -> Result<(), StateError> {
        let meta = self.txn.open_table(T_META).map_err(db_err)?;
        let last: u64 = meta_get(&meta, K_LAST_SEQ)?.unwrap_or(0);
        if seq == 0 || seq > last {
            return Err(StateError::SeqNotAllocated { seq });
        }
        let mut segments = self.txn.open_table(T_SEGMENTS).map_err(db_err)?;
        // Covered by an existing segment — exactly, or inside a
        // multi-entry span — means already frozen. (A later span cannot
        // overlap a single-entry segment at `seq`: spans start above the
        // counter as of their freeze, so any span keyed above `seq` is
        // disjoint from it.)
        if span_covering(&segments, seq)?.is_some() {
            return Err(StateError::AlreadyFrozen { seq });
        }
        segments.insert(seq, (1u64, bytes)).map_err(db_err)?;
        Ok(())
    }

    /// [`SyncDb::mark_published`] within this transaction.
    pub fn mark_published(&self, seq: u64) -> Result<(), StateError> {
        let segments = self.txn.open_table(T_SEGMENTS).map_err(db_err)?;
        // `seq` must be a frozen segment's FIRST seq; interior seqs of a
        // span publish with their segment, never individually.
        let count = match segments.get(seq).map_err(db_err)? {
            Some(guard) => guard.value().0,
            None => return Err(StateError::NotFrozen { seq }),
        };
        let mut published = self.txn.open_table(T_PUBLISHED).map_err(db_err)?;
        published.insert(seq, ()).map_err(db_err)?;
        let mut meta = self.txn.open_table(T_META).map_err(db_err)?;
        // The cursor covers the whole span: its last entry seq, not its
        // filename seq. (`count >= 1` by construction of both freeze
        // paths; saturating arithmetic keeps a tampered count from
        // wrapping.)
        let span_end = seq.saturating_add(count.saturating_sub(1));
        let cursor: u64 = meta_get(&meta, K_PUBLISHED_CURSOR)?.unwrap_or(0);
        if span_end > cursor {
            meta.insert(K_PUBLISHED_CURSOR, to_json(&span_end)?.as_slice())
                .map_err(db_err)?;
        }
        // Advance the contiguous published floor (the unpublished-scan
        // start) over whole published spans. It only ever crosses
        // published segments, so a frozen or allocated-but-unfrozen seq
        // below it is impossible by induction.
        let mut floor: u64 = meta_get(&meta, K_PUBLISHED_FLOOR)?.unwrap_or(0);
        let start = floor;
        while let Some(next) = floor.checked_add(1) {
            match segments.get(next).map_err(db_err)? {
                Some(guard) if published.get(next).map_err(db_err)?.is_some() => {
                    let (next_count, _) = guard.value();
                    floor = next.saturating_add(next_count.saturating_sub(1));
                }
                _ => break,
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

    /// [`SyncDb::stage_outbound`] within this transaction (e.g. the §3.4
    /// chokepoint's "commit the local version + stage its journal entry"
    /// composite).
    pub fn stage_outbound(&self, bytes: &[u8]) -> Result<u64, StateError> {
        let id = {
            let mut meta = self.txn.open_table(T_META).map_err(db_err)?;
            bump_counter_by(&mut meta, K_OUTBOUND_ARRIVAL, 1)?
        };
        let mut outbound = self.txn.open_table(T_OUTBOUND).map_err(db_err)?;
        outbound.insert(id, bytes).map_err(db_err)?;
        Ok(id)
    }

    /// Removes one staged outbound record by id; `Ok(true)` when it was
    /// staged. The publisher's drain calls this **in the same
    /// transaction** as [`StateTxn::freeze_next_segment`], so staged
    /// records convert to frozen segment bytes atomically (§2.1.5) — a
    /// crash leaves each entry staged or frozen, never both, never
    /// neither.
    pub fn remove_outbound(&self, id: u64) -> Result<bool, StateError> {
        let mut outbound = self.txn.open_table(T_OUTBOUND).map_err(db_err)?;
        let previous = outbound.remove(id).map_err(db_err)?;
        Ok(previous.is_some())
    }

    /// [`SyncDb::iter_outbound`] within this transaction — the publisher's
    /// drain reads the staged lane inside the same transaction that
    /// freezes it into segments (§2.1.5), so it sees its own removals.
    pub fn iter_outbound(&self) -> Result<Vec<(u64, Vec<u8>)>, StateError> {
        let outbound = self.txn.open_table(T_OUTBOUND).map_err(db_err)?;
        read_outbound(&outbound)
    }

    /// [`SyncDb::record_deleted`] within this transaction — the §2.7
    /// tombstone-apply composite records the deletion in the same commit
    /// that deletes the item row.
    pub fn record_deleted(
        &self,
        relkey: &RelKey,
        record: &DeletedRecord,
    ) -> Result<(), StateError> {
        let value = to_json(record)?;
        let mut deleted = self.txn.open_table(T_DELETED).map_err(db_err)?;
        deleted
            .insert(relkey.as_str(), value.as_slice())
            .map_err(db_err)?;
        Ok(())
    }

    /// [`SyncDb::get_deleted`] within this transaction.
    pub fn get_deleted(&self, relkey: &RelKey) -> Result<Option<DeletedRecord>, StateError> {
        let deleted = self.txn.open_table(T_DELETED).map_err(db_err)?;
        read_deleted(&deleted, relkey)
    }

    /// [`SyncDb::remove_deleted`] within this transaction — the §2.6/§2.7
    /// apply composite withdraws a superseded deletion row in the same
    /// commit that adopts the dominating `put` (a restore/resurrection
    /// reaching this device must clear the row atomically with the
    /// un-hiding, or a crash between the two would advertise a deletion
    /// the item record no longer carries). `Ok(true)` when a row existed.
    pub fn remove_deleted(&self, relkey: &RelKey) -> Result<bool, StateError> {
        let mut deleted = self.txn.open_table(T_DELETED).map_err(db_err)?;
        let previous = deleted.remove(relkey.as_str()).map_err(db_err)?;
        Ok(previous.is_some())
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
            bump_counter_by(&mut meta, K_QUEUE_ARRIVAL, 1)?
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
                Ok(Some((from_stored_str_at::<RelKey>(&rel, &rel)?, class)))
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
                    bump_counter_by(&mut meta, K_QUEUE_ARRIVAL, 1)?
                };
                entries.insert((class, arrival), rel).map_err(db_err)?;
                idx.insert(rel, (class, arrival)).map_err(db_err)?;
                Ok(true)
            }
        }
    }

    /// [`SyncDb::queue_clear`] within this transaction. Implemented as a
    /// raw table drop + recreate, so no stored relkey is ever decoded — a
    /// corrupt row cannot fail the clear.
    pub fn queue_clear(&self, q: Queue) -> Result<u64, StateError> {
        let (entries_def, idx_def) = q.tables();
        let removed = {
            let entries = self.txn.open_table(entries_def).map_err(db_err)?;
            entries.len().map_err(db_err)?
        };
        self.txn.delete_table(entries_def).map_err(db_err)?;
        self.txn.delete_table(idx_def).map_err(db_err)?;
        // Recreate immediately: `SyncDb::open` guarantees every table
        // exists, and read paths rely on it.
        self.txn.open_table(entries_def).map_err(db_err)?;
        self.txn.open_table(idx_def).map_err(db_err)?;
        Ok(removed)
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
