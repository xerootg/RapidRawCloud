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
//! Segments present for a device are applied strictly in seq order, and
//! **no gap is ever applied past**. A gap **below the lowest present
//! segment** while the device's cursor is 0 is the typed [`GapDetected`]
//! outcome (bootstrap against a compacted journal): nothing is applied,
//! the cursor stays at 0, and the engine routes the outcome to §2.3
//! manifest catch-up (bootstrap = merge then poll — the merged header
//! cursors seed past the gap, after which the re-poll applies the present
//! segments normally). A gap **anywhere else** — between the cursor and
//! the next present segment, or between two present segments — is the
//! typed [`MidStreamGap`] outcome: application for that device stops at
//! the gap, its cursor never jumps, and nothing past the gap is applied.
//!
//! Both outcomes are deliberately **report-without-apply**, so they are
//! crash-safe by re-derivation: the cursor does not move, which means the
//! very evidence that produced the outcome is still there on the next
//! poll, and the gap keeps re-reporting until the §2.3 merge (or a healed
//! segment) actually clears it. The rejected alternative — apply the
//! present segments, jump the cursor, and report once from memory — would
//! make acting on the one-shot report load-bearing for correctness: a
//! crash (or a dropped report) between that durable cursor jump and the
//! completed manifest merge would silently and permanently skip every
//! item and deleted-set effect folded into the owner's compacted seqs —
//! exactly the skip-and-diverge §2.1 principle 2 forbids.
//!
//! **Routing a [`MidStreamGap`]**: a mid-stream gap is not only an
//! anomaly — it is also what the *designed* §2.10 laggard catch-up looks
//! like (a device returns after the owner compacted segments this device
//! had not yet applied; its cursor > 0 and the lowest surviving segment
//! starts past it). The two cases are indistinguishable from the journal
//! alone, so the engine should route a [`MidStreamGap`] exactly like
//! [`GapDetected`]: merge the owner's manifest (§2.3) — whose header
//! attests the owner's own published cursor over the compacted seqs and
//! whose rows fold their effects — then re-poll. Only a gap that
//! **survives** the merge is a genuine anomaly (a lost segment).
//!
//! # Entry-granularity validation (fail-closed)
//!
//! A decoded segment's body must agree with its filename and prefix: the
//! first entry's seq equals the filename seq, every following entry's seq
//! is exactly +1 contiguous, and every entry's `device` equals the prefix
//! owner (§2.2 single-writer). No conforming publisher violates any of
//! these, so a violation is corruption (or a forged/buggy writer) and the
//! segment is refused **whole** as the typed [`CorruptSegment`] outcome —
//! applying a misstamped body would silently cover never-applied seqs
//! with the cursor, and the inflated cursor would then propagate
//! fleet-wide through the device registry and manifest headers.
//!
//! # Per-device isolation
//!
//! Version halts, gaps, segment corruption (undecodable or misstamped
//! bodies, oversized objects) and per-segment GET failures are all
//! **per-device outcomes** in the [`PollReport`], not pass errors: a
//! permanently corrupt object in one device's prefix must not starve
//! every other device's application for as long as the corruption
//! persists. Only state-store failures, LIST failures and consumer
//! refusals abort the whole pass.

use std::collections::BTreeMap;

use crate::clock::DeviceId;
use crate::journal::{
    decode_segment, JournalEntry, JournalError, JOURNAL_VERSION, SEGMENT_MAX_BYTES,
};
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
///
/// # Error contract (pinned)
///
/// `Err` is reserved for **retryable local failures** — state-store
/// trouble, resource exhaustion, anything where retrying the same entry
/// later can genuinely succeed. It aborts the whole pass, and the same
/// entry is retried identically on every later poll (and, from
/// [`crate::manifest::merge`], aborts the whole merge). A consumer must
/// therefore treat **content-level rejection** of an entry — an
/// unclassifiable or foreign `key` (entry content is attacker/buggy-
/// writer-controlled and deliberately unvalidated at decode; route it
/// through [`crate::keys::classify_key`]), an op or kind it does not
/// handle, semantics it chooses not to apply — as a successful **skip**
/// (`Ok(())` with no effects, logged by the consumer as it sees fit),
/// never as `Err`: the reader cannot tell "my state store is broken"
/// from "this entry is poison", so a refusal over one hostile entry in an
/// otherwise valid segment would permanently starve that device's whole
/// prefix, re-failing on every poll forever.
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
/// manifest catch-up (merge, then re-poll).
///
/// Nothing was applied and the cursor stayed at 0 (module docs, gap
/// semantics): the outcome re-reports on **every** poll until the merged
/// manifest header seeds the cursor past the gap, so a crash — or a
/// failed manifest GET — between this report and the completed merge
/// loses nothing. The signal is durable because it is re-derived, never
/// because anyone remembered it.
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
///
/// This is also the §2.10 laggard catch-up case (the owner compacted
/// segments this device had not applied), so the engine routes it to
/// §2.3 manifest merge before treating it as an anomaly — see the module
/// docs ("Routing a MidStreamGap"); the carried fields are what the
/// merge-then-re-poll decision needs.
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

/// A segment that is present but unusable for a reason **other than** an
/// unsupported version (which is [`PrefixHalted`]): an undecodable body
/// (malformed JSON/NDJSON), an object over the §2.2 size cap, or a body
/// that disagrees with its filename or prefix (first entry seq ≠ filename
/// seq, non-contiguous entry seqs, `entry.device` ≠ prefix owner — the
/// module docs' entry-granularity validation). Fail-closed: nothing from
/// the segment applies, the device's application stops here with its
/// cursor intact, and other devices' prefixes continue. Clears when the
/// owner (or an operator) replaces the object at the same key with a
/// conforming segment.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("segment {seq:016x} of device {device} is corrupt: {detail}")]
pub struct CorruptSegment {
    /// The owning device (whose prefix application stopped here).
    pub device: DeviceId,
    /// The segment's filename seq.
    pub seq: u64,
    /// What was wrong (decode failure or validation violation).
    pub detail: String,
}

/// A segment GET that failed (transport or backend error). The device's
/// application stops at its current cursor for this pass; other devices
/// continue, and the next poll retries. Carried as an outcome rather than
/// a pass error so one persistently unreadable object cannot starve every
/// later-sorted device's prefix.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("segment {seq:016x} of device {device} could not be fetched: {error}")]
pub struct FetchFailed {
    /// The owning device.
    pub device: DeviceId,
    /// The segment's filename seq.
    pub seq: u64,
    /// The S3 error, rendered.
    pub error: String,
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
    /// devices' application stopped at the gap — nothing applied, cursor
    /// untouched — and the outcome re-reports every poll until a §2.3
    /// merge seeds the cursor past it.
    pub gaps: Vec<GapDetected>,
    /// Mid-stream gaps; the gapped devices' application stopped at the
    /// gap.
    pub mid_stream_gaps: Vec<MidStreamGap>,
    /// Corrupt segments (undecodable, oversized, or misstamped bodies);
    /// the affected devices' application stopped at the corrupt segment.
    pub corrupt: Vec<CorruptSegment>,
    /// Segment GETs that failed this pass; the affected devices'
    /// application stopped at the unfetchable segment and will retry.
    pub fetch_failed: Vec<FetchFailed>,
}

/// Error from the inbound journal lane. Per-device conditions
/// ([`PrefixHalted`], gaps, [`CorruptSegment`], [`FetchFailed`]) are
/// **outcomes** in the [`PollReport`], not errors — they must not stop
/// other devices' prefixes; these variants are the failures that abort
/// the whole pass (everything already applied stays applied; re-polling
/// resumes).
#[derive(Debug, thiserror::Error)]
pub enum ReaderError {
    /// State-store failure.
    #[error(transparent)]
    State(#[from] StateError),
    /// S3 failure on the LIST (a failed segment GET is the per-device
    /// [`FetchFailed`] outcome instead).
    #[error(transparent)]
    S3(#[from] S3Error),
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
        // segment's first seq minus one), or when this device previously
        // applied it fully and recorded its span (§2.2 steady-state cost:
        // published segments are immutable, so the recorded span makes the
        // skip provable without a GET — the newest segment of a caught-up
        // device must not be re-fetched on every idle poll).
        if next_first.is_some_and(|nf| nf <= cursor.saturating_add(1)) {
            continue;
        }
        if db
            .segment_span(device, first_seq)?
            .is_some_and(|last_seq| last_seq <= cursor)
        {
            continue;
        }
        if first_seq > cursor.saturating_add(1) {
            if cursor == 0 && i == 0 {
                // Bootstrap gap (§2.3 placeholder): the below-horizon
                // prefix was compacted away before we ever polled.
                // Report WITHOUT applying (module docs): the cursor stays
                // at 0, so the outcome re-derives on every poll until the
                // engine's manifest merge seeds the cursor past the gap —
                // a crash or a dropped report between this poll and the
                // completed merge can never silently skip the compacted
                // seqs' effects.
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
            }
            return Ok(());
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
        // A failed GET is a per-device outcome: application for this
        // device stops at the cursor and retries next poll; other devices
        // are unaffected (module docs, per-device isolation).
        let output = match s3.get_object(bucket, &key, None).await {
            Ok(output) => output,
            Err(error) => {
                report.fetch_failed.push(FetchFailed {
                    device: device.clone(),
                    seq: first_seq,
                    error: error.to_string(),
                });
                return Ok(());
            }
        };
        // Bound reader-side allocation BEFORE buffering: no conforming
        // writer emits a segment over the §2.2 byte cap, so a bigger
        // object at a segment key is corruption, refused without being
        // collected (the capped collect also guards a lying
        // Content-Length).
        if output.content_length > SEGMENT_MAX_BYTES as u64 {
            report.corrupt.push(CorruptSegment {
                device: device.clone(),
                seq: first_seq,
                detail: format!(
                    "object is {} bytes, segment cap is {SEGMENT_MAX_BYTES}",
                    output.content_length
                ),
            });
            return Ok(());
        }
        let declared_length = output.content_length;
        let bytes = match output.body.collect_capped(SEGMENT_MAX_BYTES).await {
            Ok(bytes) => bytes,
            // The capped collect tripping means the body is over the
            // segment cap even though the declared Content-Length passed
            // the pre-check above — an oversized object behind a lying
            // header. That is a persistent property of the stored object
            // (the module docs classify oversized objects as corruption),
            // not a fetch problem: reporting it FetchFailed would promise
            // "will retry" about a condition that never clears and
            // re-download up to the cap on every poll forever.
            Err(S3Error::BodyCapExceeded { cap }) => {
                report.corrupt.push(CorruptSegment {
                    device: device.clone(),
                    seq: first_seq,
                    detail: format!(
                        "object body exceeds the {cap}-byte segment cap behind a \
                         declared Content-Length of {declared_length}"
                    ),
                });
                return Ok(());
            }
            Err(error) => {
                report.fetch_failed.push(FetchFailed {
                    device: device.clone(),
                    seq: first_seq,
                    error: error.to_string(),
                });
                return Ok(());
            }
        };
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
            // Any other decode failure is corruption: a per-device
            // outcome, so one bit-rotted object cannot starve every
            // later-sorted device's prefix for as long as it persists.
            Err(source) => {
                report.corrupt.push(CorruptSegment {
                    device: device.clone(),
                    seq: first_seq,
                    detail: source.to_string(),
                });
                return Ok(());
            }
        };
        // Entry-granularity validation (module docs): the body must agree
        // with its filename and prefix, or applying it would silently
        // cover never-applied seqs with the cursor / forge attribution.
        if let Err(detail) = validate_segment_body(device, first_seq, &entries) {
            report.corrupt.push(CorruptSegment {
                device: device.clone(),
                seq: first_seq,
                detail,
            });
            return Ok(());
        }
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
        // The whole segment is applied (or cursor-covered): record its
        // span so future polls can prove coverage without a GET (§2.2
        // steady-state cost; published segments are immutable). A crash
        // before this write costs one redundant GET next poll, never
        // correctness — which is why it need not share the entries'
        // transactions.
        if let Some(last) = entries.last() {
            db.set_segment_span(device, first_seq, last.seq)?;
        }
    }
    Ok(())
}

/// The module docs' entry-granularity validation: a decoded segment body
/// must be non-empty, belong to a filename seq ≥ 1, start at its filename
/// seq, carry strictly `+1` contiguous entry seqs, and be authored
/// entirely by the prefix owner. `Err` is the human-readable violation
/// for [`CorruptSegment::detail`].
fn validate_segment_body(
    device: &DeviceId,
    first_seq: u64,
    entries: &[JournalEntry],
) -> Result<(), String> {
    // Conforming writers allocate seqs from 1 (the freeze primitive), so
    // a segment at filename seq 0 is always a forged or buggy writer.
    // Refuse it whole: the apply loop skips entries with seq <= cursor,
    // and a fresh cursor is 0, so accepting it would silently drop its
    // seq-0 entry while applying the rest — skip-and-continue instead of
    // fail-closed.
    if first_seq == 0 {
        return Err(
            "segment filename seq is 0 (conforming writers allocate seqs from 1)".to_string(),
        );
    }
    let Some(first) = entries.first() else {
        return Err("segment body holds no entries".to_string());
    };
    if first.seq != first_seq {
        return Err(format!(
            "first entry seq {} does not match the filename seq {first_seq}",
            first.seq
        ));
    }
    for (i, entry) in entries.iter().enumerate() {
        let expected = first_seq.saturating_add(i as u64);
        if entry.seq != expected {
            return Err(format!(
                "entry seq {} where {expected} was required (seqs must be +1 contiguous \
                 from the filename seq; a skipped seq would be covered by the cursor \
                 without ever applying)",
                entry.seq
            ));
        }
        if entry.device != *device {
            return Err(format!(
                "entry seq {} is authored by {}, but the prefix owner is {device} \
                 (§2.2 single-writer)",
                entry.seq, entry.device
            ));
        }
    }
    Ok(())
}
