//! The inbound journal lane (architecture §2.2): one `ListObjectsV2` poll
//! over the journal prefix, strict-ordered per-device application through
//! a [`JournalConsumer`], and the fail-closed min-reader gate.
//!
//! # Atomicity (§2.2 apply composite)
//!
//! Every entry is applied inside **one** state-store write transaction:
//! the dedup check ([`crate::state::StateTxn::has_applied`]), the
//! consumer's own mutations (which the consumer performs through the
//! [`crate::state::StateTxn`] it is handed), the applied mark, and the
//! cursor advance all commit together via
//! [`crate::state::SyncDb::with_txn_err`]. A consumer error — or a
//! consumer panic, which unwinds through the open transaction and drops
//! it un-committed — therefore leaves **neither** the consumer's
//! side-effects **nor** the applied mark behind: the crash window between
//! "consumer effect" and "mark applied" is closed by construction, not by
//! luck.
//!
//! # Fail-closed (§2.2 min-reader rule)
//!
//! A segment whose filename version, or any entry whose `"v"`, this
//! reader does not support **halts that device's prefix** at the last
//! good seq ([`PrefixHalted`]): nothing from the unreadable segment — or
//! any later segment of that device — is applied, the cursor does not
//! advance past the last good entry, and other devices' prefixes continue
//! unaffected. Entries applied before the halt stay applied; once the bad
//! segment is replaced by a readable one at the same key, the next poll
//! resumes exactly where it halted.
//!
//! # Gap semantics (pinned)
//!
//! Segments present for a device are applied strictly in seq order.
//! A gap **below the lowest present segment** is accepted only when the
//! device's cursor is 0 (bootstrap against a compacted journal): the
//! present segments are applied in order, the cursor ends at the highest
//! applied seq, and a typed [`GapDetected`] outcome is reported so the
//! engine can route the gap to §2.3 manifest catch-up (bootstrap = merge
//! then poll). A gap **anywhere else** — between the cursor and the next
//! present segment, or between two present segments — is the typed
//! [`MidStreamGap`] outcome: application for that device stops at the
//! gap, its cursor never jumps, and nothing past the gap is applied.

use std::collections::BTreeMap;

use crate::clock::DeviceId;
use crate::journal::{decode_segment, JournalEntry, JournalError, JOURNAL_VERSION};
use crate::keys::{classify_key, journal_segment_key, KeyClass, CONTROL_PREFIX};
use crate::s3::{ListObjectsV2Request, S3Api, S3Error};
use crate::state::{StateError, StateTxn, SyncDb};

/// A consumer failure: opaque to the reader, which only needs to abort
/// the entry's transaction and surface it
/// ([`ReaderError::Consumer`]).
pub type ConsumerError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// What the apply loop feeds decoded journal entries into (§2.2). The
/// engine's §2.6 version-vector resolution plugs in here in a later unit;
/// this unit ships a recording test double in the integration suite.
///
/// # Contract
///
/// `apply` is invoked **at most once per `(device, seq)` ever applied**
/// (the applied-set dedup runs first, inside the same transaction), in
/// strict seq order per device. The consumer performs any state mutations
/// **through the [`StateTxn`] it is handed** — never through its own
/// [`SyncDb`] handle, which would deadlock — so its work commits
/// atomically with the applied mark and cursor advance. Returning `Err`
/// (or panicking) aborts the whole transaction: the entry stays
/// unapplied and is retried by the next poll.
///
/// Manifest merge ([`crate::manifest::merge`]) drives the **same** trait
/// with synthetic entries (`seq` 0, which merge never marks applied), so
/// §2.3's "merging manifests is the same idempotent apply operation as
/// replaying the journal" holds at the type level.
pub trait JournalConsumer {
    /// Applies one entry's effects inside `txn`.
    fn apply(&mut self, txn: &StateTxn<'_>, entry: &JournalEntry) -> Result<(), ConsumerError>;
}

/// A device's journal prefix is halted on an unreadable format version
/// (§2.2 min-reader rule: "app update required", never skip-and-diverge).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("journal prefix of device {device} halted: unsupported format version {version}")]
pub struct PrefixHalted {
    /// The device whose prefix is halted.
    pub device: DeviceId,
    /// The version this reader could not read (from the segment filename
    /// or an entry's `"v"` field).
    pub version: u64,
}

/// A device's journal starts past the cursor while the cursor is 0: the
/// below-horizon prefix was compacted away before this device ever
/// polled. Bootstrap placeholder: the engine routes this to §2.3
/// manifest catch-up.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GapDetected {
    /// The device whose journal is gapped at the bottom.
    pub device: DeviceId,
    /// The lowest present segment seq (> 1).
    pub lowest_seq: u64,
}

/// A missing seq range **mid-stream**: the next present segment starts
/// past `cursor + 1` while `cursor > 0`, or past the end of the previous
/// segment. Never silently skipped — application for the device stops at
/// the gap.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("mid-stream journal gap for device {device}: cursor {cursor}, next present seq {next_seq}")]
pub struct MidStreamGap {
    /// The device whose journal is gapped.
    pub device: DeviceId,
    /// The device's cursor (highest contiguously-applied seq) at the gap.
    pub cursor: u64,
    /// The first present seq past the gap.
    pub next_seq: u64,
}

/// What one [`poll`] pass did and found.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PollReport {
    /// Entries newly applied (consumer invoked + marked applied).
    pub entries_applied: u64,
    /// Prefixes halted on an unreadable version this pass (§2.2
    /// min-reader rule), one per affected device.
    pub halted: Vec<PrefixHalted>,
    /// Bootstrap gaps (cursor 0, journal starts past seq 1); the gapped
    /// devices' present segments **were** applied.
    pub gaps: Vec<GapDetected>,
    /// Mid-stream gaps; the gapped devices' application stopped at the
    /// gap.
    pub mid_stream_gaps: Vec<MidStreamGap>,
}

/// Error from the inbound journal lane. Per-device conditions
/// ([`PrefixHalted`], gaps) are **outcomes** in the [`PollReport`], not
/// errors — they must not stop other devices' prefixes; these variants
/// are the failures that abort the whole pass (everything already applied
/// stays applied; re-polling resumes).
#[derive(Debug, thiserror::Error)]
pub enum ReaderError {
    /// State-store failure.
    #[error(transparent)]
    State(#[from] StateError),
    /// S3 failure (LIST or segment GET).
    #[error(transparent)]
    S3(#[from] S3Error),
    /// A segment's bytes do not decode for a reason **other than** an
    /// unsupported version (which is a [`PrefixHalted`] outcome):
    /// malformed JSON/NDJSON — corruption, surfaced loudly.
    #[error("segment {seq:016x} of device {device} does not decode: {source}")]
    Segment {
        /// The owning device.
        device: DeviceId,
        /// The segment's first seq (its filename seq).
        seq: u64,
        /// The decode failure.
        source: JournalError,
    },
    /// The consumer refused entry `(device, seq)`. The entry's
    /// transaction was aborted: the consumer's mutations and the applied
    /// mark both rolled back, the cursor stayed at `seq - 1`'s position,
    /// and the next poll retries exactly this entry.
    #[error("consumer failed on entry ({device}, {seq}): {source}")]
    Consumer {
        /// The entry's device.
        device: DeviceId,
        /// The entry's seq.
        seq: u64,
        /// The consumer's error.
        #[source]
        source: ConsumerError,
    },
}

/// One inbound poll (§2.2 steady-state loop):
///
/// 1. **One** paged `ListObjectsV2` over the journal prefix
///    (`.rrcloud/v1/journal/`, no delimiter) — exactly one page request
///    when the listing fits in a page, which is the steady state (pinned
///    by the integration suite's counting wrapper).
/// 2. Keys are classified ([`crate::keys::classify_key`]); non-journal
///    and own-device keys are ignored (a device never applies its own
///    prefix).
/// 3. For each **foreign** device, in order: segments beyond the cursor
///    are GET-ed in ascending seq order, decoded fail-closed, and each
///    not-yet-applied entry is applied through `consumer` — dedup check,
///    consumer mutations, applied mark and cursor advance in **one**
///    committed transaction (module docs).
///
/// Version halts and gaps are per-device **outcomes** in the returned
/// [`PollReport`]; see the module docs for their pinned semantics.
pub async fn poll(
    db: &SyncDb,
    s3: &impl S3Api,
    bucket: &str,
    consumer: &mut impl JournalConsumer,
) -> Result<PollReport, ReaderError> {
    // (1) One paged LIST over the journal prefix: exactly one request in
    // the single-page steady state.
    let keys = list_journal_keys(s3, bucket).await?;

    // (2) Classify: foreign devices' segments only, grouped per device in
    // ascending seq order. Non-journal keys and our own prefix are
    // ignored. (A seq that appears under several filename versions keeps
    // the lowest — prefer the readable spelling.)
    let own = db.device_id();
    let mut per_device: BTreeMap<DeviceId, BTreeMap<u64, u32>> = BTreeMap::new();
    for key in &keys {
        if let KeyClass::Journal {
            device,
            seq,
            version,
        } = classify_key(key)
        {
            if device == *own {
                continue;
            }
            let versions = per_device.entry(device).or_default();
            let slot = versions.entry(seq).or_insert(version);
            *slot = (*slot).min(version);
        }
    }

    // (3) Per foreign device: strict-ordered application beyond the
    // cursor, with the pinned gap and min-reader-halt semantics.
    let mut report = PollReport::default();
    for (device, segments) in per_device {
        apply_device_prefix(db, s3, bucket, consumer, &device, &segments, &mut report).await?;
    }
    Ok(report)
}

/// One paged `ListObjectsV2` over `.rrcloud/v1/journal/` (no delimiter),
/// following continuation tokens. Pages are capped against a backend that
/// keeps answering truncated pages without advancing.
async fn list_journal_keys(s3: &impl S3Api, bucket: &str) -> Result<Vec<String>, ReaderError> {
    const MAX_PAGES: u32 = 10_000;
    let prefix = format!("{CONTROL_PREFIX}journal/");
    let mut keys = Vec::new();
    let mut continuation_token: Option<String> = None;
    let mut pages = 0u32;
    loop {
        if pages >= MAX_PAGES {
            return Err(ReaderError::S3(S3Error::InvalidResponse(format!(
                "ListObjectsV2 still truncated after {MAX_PAGES} pages; \
                 refusing a runaway paging loop"
            ))));
        }
        pages += 1;
        let page = s3
            .list_objects_v2(
                bucket,
                &ListObjectsV2Request {
                    prefix: Some(prefix.clone()),
                    continuation_token: continuation_token.take(),
                    ..Default::default()
                },
            )
            .await?;
        keys.extend(page.objects.into_iter().map(|o| o.key));
        if !page.is_truncated {
            return Ok(keys);
        }
        match page.next_continuation_token {
            Some(token) => continuation_token = Some(token),
            None => {
                return Err(ReaderError::S3(S3Error::InvalidResponse(
                    "truncated ListObjectsV2 page without a NextContinuationToken".to_string(),
                )))
            }
        }
    }
}

/// The §2.2 apply composite's error: either a state-store failure or the
/// consumer's refusal, so both abort the one shared transaction.
enum ApplyError {
    State(StateError),
    Consumer(ConsumerError),
}

impl From<StateError> for ApplyError {
    fn from(e: StateError) -> Self {
        ApplyError::State(e)
    }
}

/// Applies one foreign device's present segments beyond its cursor, in
/// strict seq order, updating `report` with per-device outcomes (module
/// docs: gap semantics, min-reader halts). Returns `Err` only for
/// whole-pass failures (S3, state store, consumer refusal).
async fn apply_device_prefix(
    db: &SyncDb,
    s3: &impl S3Api,
    bucket: &str,
    consumer: &mut impl JournalConsumer,
    device: &DeviceId,
    segments: &BTreeMap<u64, u32>,
    report: &mut PollReport,
) -> Result<(), ReaderError> {
    let mut cursor = db.cursor(device)?;
    let firsts: Vec<u64> = segments.keys().copied().collect();
    for (i, &first_seq) in firsts.iter().enumerate() {
        let next_first = firsts.get(i + 1).copied();
        // A segment is provably fully covered when the NEXT present
        // segment starts at or below cursor + 1 (its span ends at the next
        // segment's first seq minus one). The last segment's span length is
        // unknown without a GET, so it is fetched even when it starts at or
        // below the cursor — its tail may be unapplied.
        if next_first.is_some_and(|nf| nf <= cursor.saturating_add(1)) {
            continue;
        }
        if first_seq > cursor.saturating_add(1) {
            if cursor == 0 && i == 0 {
                // Bootstrap gap (§2.3 placeholder): the below-horizon
                // prefix was compacted away before we ever polled. The
                // present segments ARE applied; the engine routes this
                // outcome to manifest catch-up.
                report.gaps.push(GapDetected {
                    device: device.clone(),
                    lowest_seq: first_seq,
                });
            } else {
                // Mid-stream gap: typed, never skipped — application for
                // this device stops at the gap and the cursor stays put.
                report.mid_stream_gaps.push(MidStreamGap {
                    device: device.clone(),
                    cursor,
                    next_seq: first_seq,
                });
                return Ok(());
            }
        }
        // Min-reader gate on the FILENAME version, before any GET.
        let version = segments[&first_seq];
        if version != JOURNAL_VERSION {
            report.halted.push(PrefixHalted {
                device: device.clone(),
                version: u64::from(version),
            });
            return Ok(());
        }
        let key = journal_segment_key(device, first_seq);
        let bytes = s3
            .get_object(bucket, &key, None)
            .await?
            .body
            .collect()
            .await?;
        let entries = match decode_segment(&bytes) {
            Ok(entries) => entries,
            // Fail closed: an unsupported entry version anywhere in the
            // segment halts the prefix — nothing from the segment applies,
            // not even its valid earlier lines.
            Err(JournalError::UnsupportedVersion { version }) => {
                report.halted.push(PrefixHalted {
                    device: device.clone(),
                    version,
                });
                return Ok(());
            }
            Err(source) => {
                return Err(ReaderError::Segment {
                    device: device.clone(),
                    seq: first_seq,
                    source,
                })
            }
        };
        for entry in &entries {
            if entry.seq <= cursor {
                // Covered by the cursor (applied earlier, or attested by a
                // merged manifest header): never re-applied.
                continue;
            }
            let outcome = db.with_txn_err::<bool, ApplyError>(|txn| {
                if txn.has_applied(device, entry.seq)? {
                    return Ok(false);
                }
                consumer.apply(txn, entry).map_err(ApplyError::Consumer)?;
                txn.mark_applied(device, entry.seq)?;
                txn.set_cursor(device, entry.seq)?;
                Ok(true)
            });
            match outcome {
                Ok(applied) => {
                    if applied {
                        report.entries_applied += 1;
                    }
                    cursor = cursor.max(entry.seq);
                }
                Err(ApplyError::State(e)) => return Err(e.into()),
                Err(ApplyError::Consumer(source)) => {
                    return Err(ReaderError::Consumer {
                        device: device.clone(),
                        seq: entry.seq,
                        source,
                    })
                }
            }
        }
    }
    Ok(())
}
