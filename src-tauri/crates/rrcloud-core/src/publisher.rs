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
use crate::journal::{
    encode_segment, JournalEntry, JournalError, SEGMENT_MAX_BYTES, SEGMENT_MAX_ENTRIES,
};
use crate::keys::{device_registry_key, journal_segment_key};
use crate::s3::{PutObjectOptions, S3Api, S3Error};
use crate::state::{StateError, StateTxn, SyncDb};

/// The protocol versions this build can read, advertised in the device
/// registry entry (§2.2 min-reader rule).
pub const PROTO_READ: &[u32] = &[1];

/// The protocol version this build writes.
pub const PROTO_WRITE: u32 = 1;

/// Cap on a fetched `devices/<id>.json` object, bounding the
/// [`get_device_entry`] network-lane buffer (fail-closed allocation
/// stance, like the journal segment and manifest fetch caps). A
/// conforming registry entry is a few hundred bytes even with many peers
/// in `applied`; 64 KiB is orders of magnitude of headroom.
pub const DEVICE_ENTRY_MAX_BYTES: usize = 64 * 1024;

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
    /// A **staged** record is authored by a different device than this
    /// db's own identity, caught at freeze time. The §2.2 single-writer
    /// gate lives in [`enqueue_entry`], but [`crate::state::StateTxn::stage_outbound`]
    /// takes opaque bytes (it is the §3.4 composite's designed staging
    /// path), so freezing re-checks authorship: publishing a foreign-
    /// authored entry under this device's prefix would make every
    /// conforming reader refuse the segment forever (their entry-device
    /// validation), permanently wedging this journal prefix — and the
    /// frozen bytes are the segment's identity, so no replay could ever
    /// heal it. Like [`PublisherError::CorruptStaged`], the staging id is
    /// attached so the engine can surface-and-drop the one bad record
    /// ([`crate::state::StateTxn::remove_outbound`]) and retry.
    #[error(
        "staged outbound record {outbound_id} is authored by {entry_device}, \
         but this db's device is {ours} (§2.2 single-writer)"
    )]
    ForeignStaged {
        /// The staging id of the foreign-authored record.
        outbound_id: u64,
        /// The staged entry's `device` field.
        entry_device: DeviceId,
        /// The state db's own identity.
        ours: DeviceId,
    },
    /// [`enqueue_entry`] was handed an entry whose encoded line — with
    /// its newline and the maximal seq-digit headroom freeze-time
    /// stamping could add — exceeds [`SEGMENT_MAX_BYTES`]: no conforming
    /// segment could ever carry it, so staging it would permanently wedge
    /// the outbound lane. Refused synchronously, where the caller still
    /// has the entry in hand.
    #[error(
        "journal entry encodes to {size} bytes (incl. newline and maximal seq \
         stamping); no segment can carry a line over {SEGMENT_MAX_BYTES} bytes"
    )]
    OversizedEntry {
        /// The encoded line length including its newline and the reserved
        /// maximal seq-digit headroom.
        size: usize,
    },
    /// A **staged** record's single encoded line exceeds
    /// [`SEGMENT_MAX_BYTES`], so freezing cannot proceed (reachable only
    /// for records staged around [`enqueue_entry`]'s gate, e.g. by an
    /// older build). Like [`PublisherError::CorruptStaged`], the staging
    /// id is attached so the engine can surface-and-drop the one bad
    /// record ([`crate::state::StateTxn::remove_outbound`]) and retry —
    /// never an unactionable head-of-line wedge.
    #[error(
        "staged outbound record {outbound_id} encodes to a {size}-byte segment, \
         over the {SEGMENT_MAX_BYTES}-byte cap"
    )]
    OversizedStaged {
        /// The staging id of the unfreezable record.
        outbound_id: u64,
        /// The single-entry segment size that broke the cap.
        size: usize,
    },
    /// The S3 response carried no usable `Date` header, so no server-time
    /// measurement could be taken (§2.10: server time is the only clock
    /// the registry/GC lanes trust).
    #[error("S3 response has no usable Date header: {reason}")]
    NoServerDate {
        /// What was wrong (absent, or unparsable spelling).
        reason: String,
    },
    /// The stored `devices/<id>.json` object exceeds
    /// [`DEVICE_ENTRY_MAX_BYTES`] (fail-closed network-lane allocation
    /// bound, same stance as the journal segment and manifest fetch caps:
    /// a registry entry is a few hundred bytes, so a bigger object is
    /// corrupt or hostile and is refused before — and while — buffering).
    #[error(
        "device registry entry exceeds the {DEVICE_ENTRY_MAX_BYTES}-byte cap \
         (declared Content-Length {declared})"
    )]
    OversizedRegistryEntry {
        /// The response's declared `Content-Length` (which may understate
        /// the true size when the refusal came from the capped collect).
        declared: u64,
    },
    /// A fetched `devices/<id>.json` does not decode as a §1.2 registry
    /// entry: a persistent per-**object** condition of the stored
    /// document (the [`crate::reader::CorruptSegment`] analogue on the
    /// registry lane), never a local state-store failure
    /// — surfacing it as `State(Codec)` would read as "my redb is
    /// broken" (abort-the-pass territory) when the remedy is to distrust
    /// or rewrite the one stored object.
    #[error("stored device registry entry is malformed: {source}")]
    MalformedRegistryEntry {
        /// The decode failure.
        #[source]
        source: serde_json::Error,
    },
    /// This build's own registry entry failed to **serialize** (should be
    /// unreachable for the fixed §1.2 schema; typed so a wire-document
    /// codec failure is never mislabeled as a state-db failure).
    #[error("device registry entry could not be encoded: {0}")]
    EncodeRegistryEntry(#[source] serde_json::Error),
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
    // Delegating to the txn entry point keeps the two staging paths
    // byte-identical by construction (one normalization + oversize gate).
    db.with_txn_err::<u64, PublisherError>(|t| enqueue_entry_in(t, db.device_id(), entry))
}

/// [`enqueue_entry`], but **inside a caller-held transaction** — the §2.4
/// `verifying → synced` commit needs the journal `put` entry staged in the
/// *same* committed transaction as the state transition (§2.1.5: a crash
/// between verify and journal-enqueue must not lose the entry), and
/// [`enqueue_entry`]'s own `stage_outbound` commit cannot compose with
/// another transaction.
///
/// Identical contract to [`enqueue_entry`] in every other respect: the
/// staged bytes for a given entry are byte-identical between the two
/// entry points (same normalization of `v`/`seq`, same oversize refusal
/// with the same seq-digit headroom), and `entry.device` must equal
/// `own_device` — the caller passes the db's own identity
/// ([`crate::state::SyncDb::device_id`]), since the transaction handle
/// does not carry it. On `Err`, nothing was staged *by this call*;
/// whether the surrounding transaction commits remains the caller's
/// decision (the transfer engine propagates the error, which aborts the
/// whole transaction).
pub fn enqueue_entry_in(
    txn: &StateTxn<'_>,
    own_device: &DeviceId,
    entry: &JournalEntry,
) -> Result<u64, PublisherError> {
    if entry.device != *own_device {
        return Err(PublisherError::ForeignDevice {
            entry_device: entry.device.clone(),
            ours: own_device.clone(),
        });
    }
    // Normalize the stamped-at-publication fields before staging, so the
    // staged bytes always decode as a v1 entry on drain regardless of what
    // junk the caller left in `seq`/`v`.
    let mut staged = entry.clone();
    staged.v = crate::journal::JOURNAL_VERSION;
    staged.seq = 0;
    let line = staged.to_json_line()?;
    // One encoded line (plus its newline) over the §2.2 segment byte cap
    // could never be frozen: refuse it here — synchronously, with the
    // caller in context — rather than let it poison-pill every later
    // publish pass. The seq stamped at freeze time replaces the staged
    // single-digit `0` with up to u64::MAX's 20 digits, so reserve that
    // headroom now: without it, an entry within 19 bytes of the cap would
    // pass this gate but could never freeze, and the documented
    // OversizedStaged recovery (drop the staged record) would discard a
    // legitimately staged entry — a journaled-state/local-state
    // divergence for its key. With the reserve, OversizedStaged is
    // reachable only for records staged around this gate (a foreign or
    // older writer).
    const SEQ_DIGIT_HEADROOM: usize = 19; // "0" (1 digit) -> u64::MAX (20 digits)
    let reserved = line.len() + 1 + SEQ_DIGIT_HEADROOM;
    if reserved > SEGMENT_MAX_BYTES {
        return Err(PublisherError::OversizedEntry { size: reserved });
    }
    Ok(txn.stage_outbound(line.as_bytes())?)
}

/// Decodes one staged outbound record back into a [`JournalEntry`],
/// attaching its staging id on failure ([`PublisherError::CorruptStaged`]).
fn decode_staged(id: u64, bytes: &[u8]) -> Result<JournalEntry, PublisherError> {
    let line = std::str::from_utf8(bytes).map_err(|e| PublisherError::CorruptStaged {
        outbound_id: id,
        source: JournalError::Json(serde_json::Error::io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            e,
        ))),
    })?;
    JournalEntry::from_json_line(line).map_err(|source| PublisherError::CorruptStaged {
        outbound_id: id,
        source,
    })
}

/// Freezes every staged outbound entry into capped segments, consuming the
/// staged records **in the same transaction** (§2.1.5: staged or frozen,
/// never neither). No network I/O. The byte cap is enforced through
/// [`crate::state::StateTxn::freeze_next_segment`]'s shrink-retry contract:
/// a [`StateError::SegmentBuild`] caught inside the open transaction
/// consumed no seqs, so the batch shrinks and retries in the same commit.
fn freeze_staged(db: &SyncDb) -> Result<(), PublisherError> {
    db.with_txn_err::<_, PublisherError>(|t| {
        let staged = t.iter_outbound()?;
        let mut pending = Vec::with_capacity(staged.len());
        for (id, bytes) in &staged {
            let entry = decode_staged(*id, bytes)?;
            // The §2.2 single-writer authorship gate, re-checked at freeze
            // time: enqueue_entry refuses foreign entries up front, but
            // stage_outbound takes opaque bytes, so a record staged around
            // that gate must be caught HERE — once frozen, the bytes are
            // the segment's identity and every reader would refuse the
            // whole prefix forever (see PublisherError::ForeignStaged).
            if entry.device != *db.device_id() {
                return Err(PublisherError::ForeignStaged {
                    outbound_id: *id,
                    entry_device: entry.device,
                    ours: db.device_id().clone(),
                });
            }
            pending.push((*id, entry));
        }
        let mut rest = pending.as_slice();
        while !rest.is_empty() {
            let mut take = rest.len().min(SEGMENT_MAX_ENTRIES);
            loop {
                let batch = &rest[..take];
                let build = |first: u64| {
                    let stamped: Vec<JournalEntry> = batch
                        .iter()
                        .enumerate()
                        .map(|(i, (_, entry))| {
                            let mut e = entry.clone();
                            e.seq = first + i as u64;
                            e
                        })
                        .collect();
                    Ok(encode_segment(&stamped)?)
                };
                match t.freeze_next_segment(take as u64, build) {
                    Ok(_first) => {
                        for (id, _) in batch {
                            t.remove_outbound(*id)?;
                        }
                        rest = &rest[take..];
                        break;
                    }
                    // Byte cap crossed (only knowable once real seq digits
                    // are stamped): no seqs were consumed — shrink and
                    // retry inside the same transaction.
                    Err(StateError::SegmentBuild(JournalError::SegmentTooLarge { size }))
                        if take > 1 =>
                    {
                        // Proportional estimate from the failed attempt's
                        // actual size, clamped to a strict decrease so the
                        // retry loop always terminates.
                        let estimated = take * SEGMENT_MAX_BYTES / size.max(1);
                        take = estimated.clamp(1, take - 1);
                    }
                    // A single staged record over the cap can never
                    // freeze: fail typed WITH its staging id, so the
                    // engine can surface-and-drop it (like CorruptStaged)
                    // instead of wedging the lane on an unactionable
                    // error. (enqueue_entry refuses these up front; this
                    // arm covers records staged around that gate.)
                    Err(StateError::SegmentBuild(JournalError::SegmentTooLarge { size })) => {
                        return Err(PublisherError::OversizedStaged {
                            outbound_id: batch[0].0,
                            size,
                        })
                    }
                    Err(e) => return Err(e.into()),
                }
            }
        }
        Ok(())
    })
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
    // Phase 1 — freeze before any network I/O (§2.1.5): every staged
    // entry becomes frozen segment bytes in one committed transaction.
    freeze_staged(db)?;

    // Phase 2 — drain every frozen-but-unpublished segment (including
    // crash-replayed ones from earlier passes/process lives) in strict
    // ascending seq order. The frozen bytes ARE the segment: a re-PUT is
    // byte-identical by construction.
    let mut report = PublishReport::default();
    let pending = db.unpublished_segments()?;
    if pending.is_empty() {
        return Ok(report);
    }
    let device = db.device_id().clone();
    for (first_seq, bytes) in pending {
        // NDJSON: exactly one newline-terminated line per entry (JSON
        // strings cannot carry a raw newline), so the count is cheap.
        let entries = bytes.iter().filter(|&&b| b == b'\n').count() as u64;
        let key = journal_segment_key(&device, first_seq);
        // A failure here stops the drain: this segment and every later one
        // stay frozen and unpublished, and no later PUT is attempted.
        s3.put_object(
            bucket,
            &key,
            bytes::Bytes::from(bytes),
            &PutObjectOptions::default(),
        )
        .await?;
        db.mark_published(first_seq)?;
        report.segments.push(first_seq);
        report.entries += entries;
    }
    Ok(report)
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
/// by this PUT's own measurement for the *next* heartbeat.
///
/// **First-heartbeat caveat**: with no stored offset yet, the first-ever
/// entry's `last_seen_server_ts` is the raw device clock and carries its
/// full skew until the second heartbeat (~one interval later) corrects
/// it; every later heartbeat is within one round-trip + one heartbeat
/// interval of true server time. §2.10/B8 consumers of the registry (GC
/// horizons, auto-retirement) must tolerate one arbitrarily-skewed
/// initial value per device — e.g. by never acting on a device whose
/// registry entry has only ever been written once.
///
/// Returns the entry exactly as written.
pub async fn put_device_entry(
    db: &SyncDb,
    s3: &impl S3Api,
    bucket: &str,
    profile: &DeviceProfile,
) -> Result<DeviceEntry, PublisherError> {
    // Best server-time estimate BEFORE this PUT: the previously stored
    // offset applied to the local clock, or the local clock on a
    // first-ever heartbeat. This PUT's own measurement corrects the
    // stored offset for the NEXT heartbeat.
    let prior_offset_ms = db.server_time_offset_ms()?.unwrap_or(0);
    let local_before_ms = local_unix_ms();
    let last_seen_server_ts = (local_before_ms + prior_offset_ms).div_euclid(1000);

    let applied: BTreeMap<DeviceId, u64> = db.iter_cursors()?.into_iter().collect();
    let entry = DeviceEntry {
        name: profile.name.clone(),
        platform: profile.platform.clone(),
        created: profile.created,
        last_seen_server_ts,
        applied,
        proto: ProtoSupport {
            read: PROTO_READ.to_vec(),
            write: PROTO_WRITE,
        },
    };
    let body = serde_json::to_vec(&entry).map_err(PublisherError::EncodeRegistryEntry)?;
    let output = s3
        .put_object(
            bucket,
            &device_registry_key(db.device_id()),
            bytes::Bytes::from(body),
            &PutObjectOptions::default(),
        )
        .await?;

    // Measure and durably record the server-time offset (server minus
    // local, ms). The HTTP Date has 1 s granularity; the midpoint of the
    // request is approximated by the local clock right after the response.
    let date = output.date.as_deref().ok_or(PublisherError::NoServerDate {
        reason: "header absent".to_string(),
    })?;
    let server_secs = parse_http_date(date).ok_or_else(|| PublisherError::NoServerDate {
        reason: format!("unparsable Date header {date:?}"),
    })?;
    let offset_ms = server_secs
        .saturating_mul(1000)
        .saturating_sub(local_unix_ms());
    db.set_server_time_offset_ms(offset_ms)?;
    Ok(entry)
}

/// The local wall clock as unix milliseconds (negative before the epoch —
/// no panic on a badly set clock; library paths do not panic).
fn local_unix_ms() -> i64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => i64::try_from(d.as_millis()).unwrap_or(i64::MAX),
        Err(e) => i64::try_from(e.duration().as_millis())
            .map(i64::wrapping_neg)
            .unwrap_or(i64::MIN),
    }
}

/// Parses an RFC 9110 IMF-fixdate (`Sun, 06 Nov 1994 08:49:37 GMT`) into
/// unix seconds. `None` for anything else — including the two obsolete
/// HTTP-date forms, which no S3 backend this engine targets emits; the
/// caller surfaces `None` as the typed [`PublisherError::NoServerDate`].
fn parse_http_date(s: &str) -> Option<i64> {
    // "<day-name>, " is exactly 5 bytes; the rest is fixed-width fields.
    let rest = s.get(5..)?;
    let mut fields = rest.split(' ');
    let (day, mon, year, hms, zone) = (
        fields.next()?,
        fields.next()?,
        fields.next()?,
        fields.next()?,
        fields.next()?,
    );
    if fields.next().is_some() || zone != "GMT" || !s[..5].ends_with(", ") {
        return None;
    }
    let month = match mon {
        "Jan" => time::Month::January,
        "Feb" => time::Month::February,
        "Mar" => time::Month::March,
        "Apr" => time::Month::April,
        "May" => time::Month::May,
        "Jun" => time::Month::June,
        "Jul" => time::Month::July,
        "Aug" => time::Month::August,
        "Sep" => time::Month::September,
        "Oct" => time::Month::October,
        "Nov" => time::Month::November,
        "Dec" => time::Month::December,
        _ => return None,
    };
    let day: u8 = day.parse().ok()?;
    let year: i32 = year.parse().ok()?;
    let mut hms_fields = hms.split(':');
    let (h, m, sec) = (hms_fields.next()?, hms_fields.next()?, hms_fields.next()?);
    if hms_fields.next().is_some() {
        return None;
    }
    let date = time::Date::from_calendar_date(year, month, day).ok()?;
    let time = time::Time::from_hms(h.parse().ok()?, m.parse().ok()?, sec.parse().ok()?).ok()?;
    Some(date.with_time(time).assume_utc().unix_timestamp())
}

/// GETs and decodes `device`'s registry entry (§1.2). Unknown JSON fields
/// are ignored (min-reader rule for a v1 document); a missing key
/// surfaces as the underlying typed [`S3Error`], and a stored object that
/// does not decode as the typed
/// [`PublisherError::MalformedRegistryEntry`]. Network-lane allocation
/// is bounded ([`DEVICE_ENTRY_MAX_BYTES`]) like every other fetch lane in
/// this unit: an oversized object is the typed
/// [`PublisherError::OversizedRegistryEntry`] before (and, against a
/// lying `Content-Length`, while) buffering — never an unbounded buffer.
pub async fn get_device_entry(
    s3: &impl S3Api,
    bucket: &str,
    device: &DeviceId,
) -> Result<DeviceEntry, PublisherError> {
    let output = s3
        .get_object(bucket, &device_registry_key(device), None)
        .await?;
    let declared = output.content_length;
    if declared > DEVICE_ENTRY_MAX_BYTES as u64 {
        return Err(PublisherError::OversizedRegistryEntry { declared });
    }
    let bytes = match output.body.collect_capped(DEVICE_ENTRY_MAX_BYTES).await {
        Ok(bytes) => bytes,
        Err(S3Error::BodyCapExceeded { .. }) => {
            return Err(PublisherError::OversizedRegistryEntry { declared })
        }
        Err(e) => return Err(e.into()),
    };
    serde_json::from_slice(&bytes)
        .map_err(|source| PublisherError::MalformedRegistryEntry { source })
}

#[cfg(test)]
mod tests {
    use super::parse_http_date;

    #[test]
    fn imf_fixdate_parses_to_unix_seconds() {
        // The RFC 9110 example date.
        assert_eq!(
            parse_http_date("Sun, 06 Nov 1994 08:49:37 GMT"),
            Some(784_111_777)
        );
        assert_eq!(
            parse_http_date("Mon, 01 Jan 2024 00:00:00 GMT"),
            Some(1_704_067_200)
        );
        assert_eq!(
            parse_http_date("Tue, 29 Feb 2000 23:59:59 GMT"),
            Some(951_868_799),
            "leap day round-trips"
        );
    }

    #[test]
    fn obsolete_http_date_forms_are_rejected() {
        // The two obsolete forms RFC 9110 readers may accept but this
        // parser's doc promises to refuse (no S3 backend emits them; the
        // caller surfaces None as the typed NoServerDate).
        assert_eq!(parse_http_date("Sunday, 06-Nov-94 08:49:37 GMT"), None);
        assert_eq!(parse_http_date("Sun Nov  6 08:49:37 1994"), None);
    }

    #[test]
    fn malformed_dates_are_rejected_not_misread() {
        for s in [
            "",
            "Sun",
            "Sun, ",
            "Sun; 06 Nov 1994 08:49:37 GMT", // no ", " separator
            "Sun, 06 Nov 1994 08:49:37 UTC", // non-GMT zone
            "Sun, 06 Nov 1994 08:49:37 GMT extra", // trailing field
            "Sun, 06 Nov 1994 08:49:37",     // zone missing
            "Sun, 32 Nov 1994 08:49:37 GMT", // no such day
            "Sun, 06 Foo 1994 08:49:37 GMT", // no such month
            "Sun, 06 Nov 1994 24:00:00 GMT", // no such hour
            "Sun, 06 Nov 1994 08:49 GMT",    // seconds missing
            "Sun, 06 Nov 1994 08:49:37:00 GMT", // extra hms field
            "Sun, xx Nov 1994 08:49:37 GMT", // non-numeric day
            "Sun, 06 Nov year 08:49:37 GMT", // non-numeric year
        ] {
            assert_eq!(parse_http_date(s), None, "must reject {s:?}");
        }
    }
}
