//! The outbound journal lane for one device (architecture §2.1.5, §2.2),
//! plus the §1.2 device-registry heartbeat.
//!
//! # Durability order (§2.1.5, journal plane)
//!
//! serialize segment → **commit the exact segment bytes + seq to redb**
//! ([`crate::state::StateTxn::freeze_next_segment`], in the same
//! transaction that consumes the staged outbound records) → PUT → commit
//! published cursor ([`crate::state::SyncDb::mark_published`]). Freezing
//! happens strictly **before any network I/O**, and crash replay re-PUTs
//! the byte-identical frozen bytes, so a reader that saw the first PUT and
//! dedups by `(device, seq)` can never diverge.
//!
//! # Publish ordering
//!
//! [`publish_pending`] publishes in strict ascending seq order: every
//! frozen-but-unpublished segment — including segments left over from a
//! crash or an earlier failed pass — is PUT **before** any later segment.
//! A PUT failure stops the drain (the failed segment and everything after
//! it stay frozen and retryable); a later segment is never attempted while
//! an earlier one is unpublished.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::clock::DeviceId;
use crate::journal::{JournalEntry, JournalError};
use crate::s3::{S3Api, S3Error};
use crate::state::{StateError, SyncDb};

/// The protocol versions this build can read, advertised in the device
/// registry entry (§2.2 min-reader rule).
pub const PROTO_READ: &[u32] = &[1];

/// The protocol version this build writes.
pub const PROTO_WRITE: u32 = 1;

/// Error from the outbound journal lane.
#[derive(Debug, thiserror::Error)]
pub enum PublisherError {
    /// State-store failure.
    #[error(transparent)]
    State(#[from] StateError),
    /// S3 failure. For a segment PUT this is retryable: the segment (and
    /// every later one) stays frozen, and the next [`publish_pending`]
    /// resumes in order.
    #[error(transparent)]
    S3(#[from] S3Error),
    /// Journal encoding failure (an entry that cannot be serialized).
    #[error(transparent)]
    Journal(#[from] JournalError),
    /// [`enqueue_entry`] was handed an entry authored by a different
    /// device than the state db's own identity. Each device writes only
    /// its own journal prefix (§2.2 single-writer); staging a foreign
    /// entry would publish under the wrong identity.
    #[error("outbound entry authored by {entry_device}, but this db's device is {ours}")]
    ForeignDevice {
        /// The entry's `device` field.
        entry_device: DeviceId,
        /// The state db's own identity.
        ours: DeviceId,
    },
    /// A staged outbound record no longer decodes as a journal entry
    /// (on-disk corruption). The staging id is attached so the engine can
    /// surface-and-drop the one bad record
    /// ([`crate::state::StateTxn::remove_outbound`]) and retry.
    #[error("staged outbound record {outbound_id} does not decode: {source}")]
    CorruptStaged {
        /// The staging id ([`crate::state::SyncDb::stage_outbound`]).
        outbound_id: u64,
        /// The decode failure.
        source: JournalError,
    },
    /// The S3 response carried no usable `Date` header, so no server-time
    /// measurement could be taken (§2.10: server time is the only clock
    /// the registry/GC lanes trust).
    #[error("S3 response has no usable Date header: {reason}")]
    NoServerDate {
        /// What was wrong (absent, or unparsable spelling).
        reason: String,
    },
}

/// What one [`publish_pending`] pass did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PublishReport {
    /// First seq of every segment PUT **and** marked published by this
    /// pass, in the (ascending) order they were published. Includes
    /// crash-replayed re-PUTs of previously frozen segments.
    pub segments: Vec<u64>,
    /// Total entries covered by those segments.
    pub entries: u64,
}

/// Durably stages one outbound journal entry for later publication,
/// returning its staging id (FIFO order; §2.1.5 outbound lane).
///
/// The entry must be **complete except for `seq`**: `vv`, `ts`, `op`,
/// `kind`, `key` and the per-kind fields are the caller's responsibility
/// and are published exactly as staged. The staged `seq` field is
/// **ignored and overwritten**: seqs are allocated at freeze time by
/// [`publish_pending`] (via the state store's single-transaction
/// allocate+freeze), because a pre-assigned seq could not survive the
/// §2.2 "never regresses, never duplicates" contract across crashes.
/// `entry.device` must equal the db's own identity
/// ([`PublisherError::ForeignDevice`] otherwise); `entry.v` is likewise
/// stamped to [`crate::journal::JOURNAL_VERSION`] at publication.
pub fn enqueue_entry(db: &SyncDb, entry: &JournalEntry) -> Result<u64, PublisherError> {
    let _ = (db, entry);
    todo!("P1-U3: enqueue_entry")
}

/// Drains the outbound lane: freezes every staged entry into segments
/// (respecting the §2.2 caps — at most
/// [`crate::journal::SEGMENT_MAX_ENTRIES`] entries and
/// [`crate::journal::SEGMENT_MAX_BYTES`] encoded bytes per segment, via
/// the freeze primitive's shrink-retry contract), then PUTs every
/// frozen-but-unpublished segment to its journal key
/// ([`crate::keys::journal_segment_key`]) in strict ascending seq order,
/// marking each published after its PUT succeeds.
///
/// Contracts (all pinned by tests):
///
/// - **Freeze before network** (§2.1.5): every staged entry is committed
///   as frozen segment bytes — in the same transaction that consumes its
///   staged record — before any PUT is attempted.
/// - **Crash replay**: unpublished frozen segments from any earlier pass
///   or process life are re-PUT **byte-identical** (the frozen bytes are
///   the segment's identity) and **before** any newer segment.
/// - **Ordering under failure**: a PUT failure stops the drain with a
///   typed error; the failed segment and all later ones stay frozen and
///   unpublished, and no later segment's PUT is attempted. A retry with a
///   working client completes in order.
/// - Publishing nothing (no staged entries, nothing unpublished) is
///   `Ok` with an empty report and performs no network I/O.
pub async fn publish_pending(
    db: &SyncDb,
    s3: &impl S3Api,
    bucket: &str,
) -> Result<PublishReport, PublisherError> {
    let _ = (db, s3, bucket);
    todo!("P1-U3: publish_pending")
}

/// The caller-owned identity facts of the device registry entry (§1.2):
/// everything in `devices/<id>.json` that the engine does not derive from
/// the state db or the server clock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceProfile {
    /// Human-readable device name.
    pub name: String,
    /// Platform string (e.g. `"linux"`, `"android"`).
    pub platform: String,
    /// Unix seconds the device joined the library (stable across
    /// heartbeats; the caller persists it).
    pub created: i64,
}

/// Protocol support advertisement (§2.2 min-reader rule): which journal
/// format versions this device reads, and which it writes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtoSupport {
    /// Versions this device can read.
    pub read: Vec<u32>,
    /// The version this device writes.
    pub write: u32,
}

/// The device registry entry, `devices/<device_id>.json` (§1.2):
/// `{name, platform, created, last_seen_server_ts, applied: {device: seq},
/// proto: {read, write}}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceEntry {
    /// Human-readable device name.
    pub name: String,
    /// Platform string.
    pub platform: String,
    /// Unix seconds the device joined the library.
    pub created: i64,
    /// **Server** time (unix seconds, from the S3 `Date` header) of the
    /// last heartbeat — never the device clock (§2.10).
    pub last_seen_server_ts: i64,
    /// Highest contiguously-applied seq per peer device
    /// ([`crate::state::SyncDb::iter_cursors`]) — what compaction's
    /// min-cursor rule reads (§2.10).
    pub applied: BTreeMap<DeviceId, u64>,
    /// Protocol support advertisement.
    pub proto: ProtoSupport,
}

/// The §2.2 heartbeat: PUTs this device's registry entry to
/// [`crate::keys::device_registry_key`], with `applied` snapshotted from
/// [`crate::state::SyncDb::iter_cursors`], `proto` =
/// `{read: [`[`PROTO_READ`]`], write: `[`PROTO_WRITE`]`}`, and
/// `last_seen_server_ts` in **server** time.
///
/// Server-time handling: the PUT response's `Date` header is parsed and
/// the measured offset (server minus local, milliseconds) is durably
/// recorded via
/// [`crate::state::SyncDb::set_server_time_offset_ms`]; a response
/// without a parsable `Date` is [`PublisherError::NoServerDate`] (the
/// entry may have been stored, but no time measurement was taken).
/// `last_seen_server_ts` itself is derived from the best server-time
/// estimate available (the previously stored offset applied to the local
/// clock, or the local clock on a first-ever heartbeat), then corrected
/// by this PUT's own measurement for the *next* heartbeat — so the stored
/// field is always within one round-trip + one heartbeat interval of true
/// server time.
///
/// Returns the entry exactly as written.
pub async fn put_device_entry(
    db: &SyncDb,
    s3: &impl S3Api,
    bucket: &str,
    profile: &DeviceProfile,
) -> Result<DeviceEntry, PublisherError> {
    let _ = (db, s3, bucket, profile);
    todo!("P1-U3: put_device_entry")
}

/// GETs and decodes `device`'s registry entry (§1.2). Unknown JSON fields
/// are ignored (min-reader rule for a v1 document); a missing key
/// surfaces as the underlying typed [`S3Error`].
pub async fn get_device_entry(
    s3: &impl S3Api,
    bucket: &str,
    device: &DeviceId,
) -> Result<DeviceEntry, PublisherError> {
    let _ = (s3, bucket, device);
    todo!("P1-U3: get_device_entry")
}
