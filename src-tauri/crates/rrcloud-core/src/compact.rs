//! Compaction, horizons, GC, device lifecycle, and pre-upload quarantine
//! (architecture §2.10, with §2.3/§2.1/§1.2 as the surrounding contract).
//!
//! This unit is the **pure decisions + their S3 effects**, driven by
//! explicit entry points a caller (the SyncManager timer loop, or the
//! headless worker) invokes. It does *not* own the timer loop, the worker
//! binary, or §2.3 reconcile/foreign-adoption (the worker unit).
//!
//! # Server time is the only clock (§2.10)
//!
//! Every age threshold in this module is evaluated in **server time** —
//! the `Date` header of S3 responses, recorded as an offset in the state
//! db's meta (`crate::state::SyncDb::server_time_offset_ms`) and applied to
//! the local clock by [`ServerClock`]. A device whose wall clock runs fast
//! or slow records a correspondingly larger/smaller offset, so
//! `now_server()` is the same instant on every device, and two devices with
//! +2 h / −2 h local clocks reach **identical** compaction/GC/retirement
//! decisions (fixing review B8). No decision here ever reads a raw device
//! clock.
//!
//! The entry points take a [`ServerClock`] and consult only
//! [`ServerClock::now_server`] for "now"; historical timestamps come from
//! durable server-time records (manifest `written_server_ts`, tombstone
//! `server_ts`, device entry `last_seen_server_ts`, the per-segment publish
//! stamp). [`record_server_time`] is the recording hook any S3 op can call
//! to refresh the stored offset from a response `Date` header — a GC-only
//! worker that never heartbeats still keeps its clock fresh through the
//! read-back HEADs it performs every pass.
//!
//! # What this module never does
//!
//! It never destroys a data key, a journal segment, or a tombstone except
//! by the durable-record rules of §2.10 (§2.1 principle 3): a stale LIST can
//! delay discovery or delay GC, never cause destruction or resurrection.

use std::collections::BTreeSet;
use std::path::Path;

use bytes::Bytes;

use crate::clock::{compare, pick_winner, Candidate, DeviceId, VersionVector, VvOrder};
use crate::journal::{decode_segment, JournalEntry, Kind, Op, Tombstone, SEGMENT_MAX_BYTES};
use crate::keys::{
    classify_key, device_retired_key, journal_segment_key, library_key, local_path, manifest_key,
    preview_key, sidecar_key, thumb_key, tombstone_key, KeyClass, RelKey, ThumbSize,
    CONTROL_PREFIX,
};
use crate::manifest::{build_manifest, get_manifest, put_manifest, ManifestError};
use crate::publisher::{get_device_entry, DeviceEntry, PublisherError};
use crate::s3::{
    ListObjectsV2Request, ObjectSummary, PutObjectOptions, S3Api, S3Error, S3TransferApi,
};
use crate::semhash::ContentId;
use crate::state::{DeletedRecord, ItemRecord, ItemState, StateError, StateTxn, SyncDb};

/// Seconds in a day (server-time arithmetic is all in unix seconds).
const DAY: i64 = 86_400;

/// Default: a device is **active** only if its last heartbeat is within
/// this window (§2.10). Past it, the device stops gating horizons even
/// without an explicit retirement.
pub const ACTIVE_WINDOW_SECS: i64 = rrcloud_proto::ACTIVE_WINDOW_SECS;

/// Default: a segment's covering manifest must have been durably present at
/// least this long — and be re-confirmed present — before any DELETE
/// (§2.10 segment-compaction rule 2, the 24 h insurance against a transient
/// PUT anomaly).
pub const COMPACTION_RECONFIRM_SECS: i64 = rrcloud_proto::COMPACTION_RECONFIRM_SECS;

/// Default: the 14-day pressure-valve cap (§2.10 rule 3 / D3). A laggard
/// that has not applied a segment still lets it be compacted once the
/// segment is older than this, because the laggard catches up losslessly by
/// manifest merge (rows carry vv + the deleted set).
pub const LAGGARD_CAP_SECS: i64 = rrcloud_proto::LAGGARD_CAP_SECS;

/// Default: the user-facing "Recently Deleted" grace window (§2.7/§2.10
/// (b)). A tombstone younger than this is never GC'd, even when every
/// active device has applied past it.
pub const RECENTLY_DELETED_GRACE_SECS: i64 = rrcloud_proto::RECENTLY_DELETED_GRACE_SECS;

/// Default: a device with no heartbeat for this long is auto-retired by the
/// GC sweep (§2.10 device lifecycle), server time.
pub const AUTO_RETIRE_SECS: i64 = rrcloud_proto::AUTO_RETIRE_SECS;

/// Default: the deleted-set retention window (§2.3): a GC runner's manifest
/// retains a destroyed tombstone's `del` row this long so a bootstrapping or
/// long-dormant device still learns the deletion after the tombstone object
/// is gone. Also the §2.3 pre-upload quarantine horizon: a device whose
/// deletion-knowledge predates this window cannot prove a local-only key was
/// not deleted.
pub const DELETED_SET_RETENTION_SECS: i64 = rrcloud_proto::DELETED_SET_RETENTION_SECS;

/// The server-time thresholds this unit evaluates, all defaulting to the
/// §2.10 constants. A caller may shrink them (tests, aggressive homelab
/// policy); the entry points read them so no threshold is a buried literal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactConfig {
    /// [`ACTIVE_WINDOW_SECS`].
    pub active_window_secs: i64,
    /// [`COMPACTION_RECONFIRM_SECS`].
    pub reconfirm_secs: i64,
    /// [`LAGGARD_CAP_SECS`].
    pub laggard_cap_secs: i64,
    /// [`RECENTLY_DELETED_GRACE_SECS`].
    pub grace_secs: i64,
    /// [`AUTO_RETIRE_SECS`].
    pub auto_retire_secs: i64,
    /// [`DELETED_SET_RETENTION_SECS`].
    pub retention_secs: i64,
}

impl Default for CompactConfig {
    fn default() -> Self {
        Self {
            active_window_secs: ACTIVE_WINDOW_SECS,
            reconfirm_secs: COMPACTION_RECONFIRM_SECS,
            laggard_cap_secs: LAGGARD_CAP_SECS,
            grace_secs: RECENTLY_DELETED_GRACE_SECS,
            auto_retire_secs: AUTO_RETIRE_SECS,
            retention_secs: DELETED_SET_RETENTION_SECS,
        }
    }
}

/// Error from a compaction/GC/lifecycle entry point.
#[derive(Debug, thiserror::Error)]
pub enum CompactError {
    /// State-store failure.
    #[error(transparent)]
    State(#[from] StateError),
    /// S3 failure.
    #[error(transparent)]
    S3(#[from] S3Error),
    /// Manifest build/encode/transfer/merge failure.
    #[error(transparent)]
    Manifest(#[from] ManifestError),
    /// A journal segment of a retired device's orphaned prefix did not decode
    /// (corrupt or oversized) while folding it into the runner's manifest.
    #[error(transparent)]
    Journal(#[from] crate::journal::JournalError),
    /// Journal publish/registry failure (device entry decode, etc.).
    #[error(transparent)]
    Publisher(#[from] PublisherError),
    /// The read-back HEAD of the owner's own manifest did not confirm the
    /// fold is durably present (object absent, or ETag ≠ the PUT's). §2.10
    /// rule 1/2: a failed or not-present manifest read-back **aborts ALL
    /// deletes** this pass — nothing destroyed without its covering record
    /// proven durable (§2.1 principle 3).
    #[error("own manifest read-back verification failed before any segment delete: {detail}")]
    ManifestNotVerified {
        /// What the read-back saw (absent / etag mismatch).
        detail: String,
    },
    /// The GC runner could not prove a tombstone's `del` row is durably
    /// folded into its own manifest before destroying data keys (§2.10 (c),
    /// the crash-safety ordering): destruction is refused so no key is ever
    /// destroyed without its deleted-set record already durable.
    #[error("deleted-set fold not verified before tombstone destruction: {detail}")]
    DeletedSetNotVerified {
        /// What the read-back saw.
        detail: String,
    },
    /// A local filesystem operation failed while resolving a §2.3 quarantine
    /// decision (`Discard` removing the local file). Carries the relkey so
    /// the caller can surface which item could not be dropped.
    #[error("local filesystem error for {relkey}: {source}")]
    Io {
        /// The relkey whose local file operation failed.
        relkey: RelKey,
        /// The underlying I/O failure.
        #[source]
        source: std::io::Error,
    },
}

// ---------------------------------------------------------------------------
// Server clock (§2.10)
// ---------------------------------------------------------------------------

/// The server-time clock: `now_server() = local + offset`, where `offset`
/// (server minus local, seconds) was measured from an S3 response `Date`
/// header (§2.10). This is the single "now" every threshold in this module
/// consults.
///
/// Construction:
/// - [`ServerClock::from_db`] — production: reads the offset the heartbeat
///   (or [`record_server_time`]) stored in meta, and `now_server()` tracks
///   the real local clock.
/// - [`ServerClock::observe`] — the offset exactly as a heartbeat derives it
///   (`server - local`), with `now_server()` **pinned** to the observed
///   instant. The skew-immunity contract is checkable with it: a fast and a
///   slow device that observe the *same* server `Date` compute the *same*
///   `now_server()`, because each one's offset absorbs its own skew.
/// - [`ServerClock::pinned`] — a fixed server instant (decision tests plant
///   the historical timestamps relative to it, rather than sleeping).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerClock {
    offset_secs: i64,
    /// When `Some`, `now_server()` uses this frozen local instant instead of
    /// the real wall clock — the determinism hook that makes skew immunity
    /// and age thresholds testable without sleeping or touching the system
    /// clock. Production (`from_db`) leaves it `None`.
    pinned_local: Option<i64>,
}

impl ServerClock {
    /// A clock with an explicit `server - local` offset, tracking the real
    /// local wall clock.
    pub fn with_offset_secs(offset_secs: i64) -> Self {
        Self {
            offset_secs,
            pinned_local: None,
        }
    }

    /// Production constructor: the offset persisted by the heartbeat /
    /// [`record_server_time`] (`server_time_offset_ms`, truncated to
    /// seconds), tracking the real local clock. An unmeasured offset reads
    /// as `0` (the §2.2 first-heartbeat caveat; the caller gates on having a
    /// measured offset where that matters).
    pub fn from_db(db: &SyncDb) -> Result<Self, CompactError> {
        let offset_secs = db.server_time_offset_ms()?.unwrap_or(0).div_euclid(1_000);
        Ok(Self::with_offset_secs(offset_secs))
    }

    /// The offset exactly as a heartbeat derives it from one response —
    /// `server_unix - local_unix` — with `now_server()` pinned to the
    /// observed `server_unix`.
    pub fn observe(local_unix: i64, server_unix: i64) -> Self {
        Self {
            offset_secs: server_unix.saturating_sub(local_unix),
            pinned_local: Some(local_unix),
        }
    }

    /// A clock pinned so `now_server()` returns exactly `server_unix`
    /// (offset `0`, frozen local). For decision tests that plant history
    /// relative to a fixed "now".
    pub fn pinned(server_unix: i64) -> Self {
        Self {
            offset_secs: 0,
            pinned_local: Some(server_unix),
        }
    }

    /// The measured offset (`server - local`), seconds.
    pub fn offset_secs(&self) -> i64 {
        self.offset_secs
    }

    /// Server time now, unix seconds — `local + offset` (local pinned in
    /// tests, the real clock in production).
    pub fn now_server(&self) -> i64 {
        self.pinned_local
            .unwrap_or_else(now_unix)
            .saturating_add(self.offset_secs)
    }

    /// Server time corresponding to an explicit local instant — the pure
    /// `local + offset` map, exposed so the skew-immunity proof can show two
    /// differently-skewed locals mapping to one server instant.
    pub fn server_at(&self, local_unix: i64) -> i64 {
        local_unix.saturating_add(self.offset_secs)
    }
}

/// The local wall clock as unix seconds (negative before the epoch — no
/// panic on a badly set clock; library paths do not panic).
fn now_unix() -> i64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => i64::try_from(d.as_secs()).unwrap_or(i64::MAX),
        Err(e) => i64::try_from(e.duration().as_secs())
            .map(i64::wrapping_neg)
            .unwrap_or(i64::MIN),
    }
}

/// The local wall clock as unix milliseconds.
fn now_unix_ms() -> i64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => i64::try_from(d.as_millis()).unwrap_or(i64::MAX),
        Err(e) => i64::try_from(e.duration().as_millis())
            .map(i64::wrapping_neg)
            .unwrap_or(i64::MIN),
    }
}

/// The §2.10 server-time recording hook for **any** S3 op: parse a response
/// `Date` header (RFC 9110 IMF-fixdate) and durably record the measured
/// offset (`server - local`, ms) in meta, so the next [`ServerClock`] built
/// here is fresh. Returns `true` when an offset was recorded, `false` when
/// the header was absent or unparsable (the caller keeps the previously
/// stored offset; a GC runner's other read-backs will catch the next one).
///
/// Publisher (heartbeat PUT) and transfer (object PUTs) already record on
/// their writes; this hook lets the compaction/GC lane record from the
/// read-back HEADs and manifest GETs it does every pass, so a worker that
/// never journals between runs still keeps server time fresh
/// ([`crate::s3::HeadObjectOutput::date`] / `GetObjectOutput` carry the
/// header).
pub fn record_server_time(db: &SyncDb, date_header: Option<&str>) -> Result<bool, CompactError> {
    let Some(date) = date_header else {
        return Ok(false);
    };
    let Some(server_secs) = crate::publisher::parse_http_date(date) else {
        return Ok(false);
    };
    let offset_ms = server_secs
        .saturating_mul(1_000)
        .saturating_sub(now_unix_ms());
    db.set_server_time_offset_ms(offset_ms)?;
    Ok(true)
}

// ---------------------------------------------------------------------------
// Active set + horizons (§2.10)
// ---------------------------------------------------------------------------

/// One active device and the registry entry it was read from. The entry's
/// `applied` map is what horizon arithmetic reads; its `last_seen_server_ts`
/// is what the active-set filter read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveDevice {
    /// The device id (owner of the registry entry).
    pub device: DeviceId,
    /// The decoded registry entry.
    pub entry: DeviceEntry,
}

/// The §2.10 active set: list `.rrcloud/v1/devices/`, GET each `<id>.json`
/// entry, and keep a device **iff** (1) it has no `<id>.retired` marker and
/// (2) its `last_seen_server_ts` is within `cfg.active_window_secs` of
/// `clock.now_server()`. Retirement wins immediately — a retired device
/// drops from the set the moment its marker exists, so it stops gating
/// horizons (fixing the orphan-blocks-horizons hole, B5/C3).
///
/// Returned ascending by device id. The result is **data about the fleet**,
/// never trusted as instructions; a malformed or oversized entry object is a
/// typed error from the registry decode lane, not a silently dropped device.
pub async fn active_devices(
    s3: &impl S3TransferApi,
    bucket: &str,
    clock: &ServerClock,
    cfg: &CompactConfig,
) -> Result<Vec<ActiveDevice>, CompactError> {
    let prefix = format!("{CONTROL_PREFIX}devices/");
    let keys = list_keys_under(s3, bucket, &prefix).await?;
    let mut retired: BTreeSet<DeviceId> = BTreeSet::new();
    let mut registered: BTreeSet<DeviceId> = BTreeSet::new();
    for key in &keys {
        match classify_key(key) {
            KeyClass::DeviceRetired { device } => {
                retired.insert(device);
            }
            KeyClass::DeviceRegistry { device } => {
                registered.insert(device);
            }
            _ => {}
        }
    }
    let now = clock.now_server();
    // `registered` is a BTreeSet, so iteration — and the output — is
    // ascending by device id (the documented order).
    let mut out = Vec::new();
    for device in registered {
        // Retirement wins immediately: a retired device drops from the set
        // the moment its marker exists, so it stops gating horizons (B5/C3).
        if retired.contains(&device) {
            continue;
        }
        let entry = get_device_entry(s3, bucket, &device).await?;
        if now.saturating_sub(entry.last_seen_server_ts) <= cfg.active_window_secs {
            out.push(ActiveDevice { device, entry });
        }
    }
    Ok(out)
}

/// The §2.10 segment-compaction horizon for `target`'s prefix: the minimum,
/// over every active device **other than `target` itself**, of that
/// device's `applied[target]` (the registry entry's per-device cursor).
///
/// Precedence (pinned): the **device-registry `applied` map** is
/// authoritative here — not manifest `cursors` — because the heartbeat
/// refreshes it on every poll and it is exactly what "every active device's
/// applied[A] ≥ s" (§2.10 rule 3) names. `target` never gates its own
/// prefix: it authored those entries, so including its (absent, peers-only)
/// `applied[target]` would read `0` and wedge the fast path forever. With no
/// *other* active device, the horizon is unbounded ([`u64::MAX`]) — nothing
/// gates the fast path.
pub fn horizon_applied(active: &[ActiveDevice], target: &DeviceId) -> u64 {
    active
        .iter()
        .filter(|ad| ad.device != *target)
        .map(|ad| ad.entry.applied.get(target).copied().unwrap_or(0))
        .min()
        .unwrap_or(u64::MAX)
}

// ---------------------------------------------------------------------------
// Segment compaction (own prefix only, §2.10)
// ---------------------------------------------------------------------------

/// Why one own segment was not deleted this pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SkipReason {
    /// Rule 1: the owner's own manifest does not yet cover the segment
    /// (`cursors[owner] < segment.max_seq`). A segment whose entries are not
    /// provably in the manifest is **never** deleted (§2.10 safety).
    ManifestCoverageBelow {
        /// The manifest's `cursors[owner]`.
        cursor: u64,
    },
    /// Rule 2: the covering manifest has been durably present less than
    /// `reconfirm_secs` (24 h) — the re-confirm window is still open.
    ReconfirmWindowOpen {
        /// How long (server-time seconds) the coverage has been durable.
        manifest_age_secs: i64,
    },
    /// Rule 3: neither the fast path (`horizon ≥ max_seq`) nor the 14-day
    /// cap holds for this segment.
    HorizonBlocked {
        /// The computed horizon over the active set.
        horizon: u64,
    },
}

/// One own segment that was not deleted, with the reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkippedSegment {
    /// The segment's first entry seq (its filename seq).
    pub first_seq: u64,
    /// The segment's highest entry seq (`first_seq + entries - 1`).
    pub max_seq: u64,
    /// Why it was kept.
    pub reason: SkipReason,
}

/// What one [`compact_own_segments`] pass did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompactionSummary {
    /// First-seqs of the own segments whose S3 objects were DELETEd this
    /// pass, ascending. Idempotent: a re-run finds them already gone and
    /// reports them neither deleted nor skipped.
    pub deleted_seqs: Vec<u64>,
    /// Own segments kept, with the per-segment reason.
    pub skipped: Vec<SkippedSegment>,
}

/// §2.10 segment compaction of the caller's **own** journal prefix. Deletes
/// only the owner's own segments, idempotently, and never a segment whose
/// entries are not provably folded into the owner's own manifest.
///
/// Per pass, for the owner `A` = `db.device_id()`:
///
/// 1. **Cover + verify (rule 1).** Ensure `A`'s manifest
///    (`manifests/A.json.gz`) durably covers `A`'s published cursor: GET the
///    existing manifest; if absent or its `cursors[A]` is below the current
///    published cursor, build and PUT a fresh one. Then **read-back HEAD**
///    the manifest and require it present with the expected ETag — this is
///    the rule-1/2 read-back that must precede any DELETE. A failed or
///    not-present read-back is [`CompactError::ManifestNotVerified`] and
///    **aborts the whole pass** (no segment is deleted). Record server time
///    from the read-back's `Date` while here.
/// 2. **Age (rule 2).** The deletable ceiling is the manifest's
///    `cursors[A]` **only when that coverage has been durably present for at
///    least `reconfirm_secs` (24 h)** — measured from the covering
///    manifest's `written_server_ts`. A manifest freshly written this pass
///    (because coverage advanced) resets that clock; a manifest whose
///    coverage was unchanged is *not* rewritten, so its `written_server_ts`
///    keeps aging and the next pass can delete. Segments above the ceiling
///    are skipped [`SkipReason::ReconfirmWindowOpen`] /
///    [`SkipReason::ManifestCoverageBelow`].
/// 3. **Horizon or cap (rule 3).** A covered, aged segment is deleted iff
///    **either** every active device has applied past it
///    ([`horizon_applied`] `≥ max_seq`, the fast path) **or** the segment is
///    older than `laggard_cap_secs` (14 days, server time — from its
///    per-segment publish stamp,
///    `crate::state::SyncDb::segment_published_server_ts`). Otherwise it is
///    kept [`SkipReason::HorizonBlocked`]. The cap is the pressure valve: a
///    laggard catches up losslessly by manifest merge (§2.3).
///
/// Own segment extents are derived from the LIST of `A`'s own journal
/// prefix: because `freeze_next_segment` tiles seqs with no holes, each
/// segment's `max_seq` is one below the next segment's `first_seq` (the last
/// one's is the published cursor). No segment GET is needed to compute
/// coverage.
pub async fn compact_own_segments(
    db: &SyncDb,
    s3: &impl S3TransferApi,
    bucket: &str,
    clock: &ServerClock,
    cfg: &CompactConfig,
) -> Result<CompactionSummary, CompactError> {
    let owner = db.device_id().clone();
    let now = clock.now_server();

    // Own segment extents from the LIST of the owner's journal prefix — no
    // segment GET needed (freeze_next_segment tiles seqs with no holes).
    let segments = own_segment_extents(db, s3, bucket, &owner).await?;
    if segments.is_empty() {
        // Nothing to compact — idempotent no-op (a re-run after a prior pass
        // deleted every segment lands here).
        return Ok(CompactionSummary::default());
    }

    // Rule 1: the owner's own manifest must durably cover the segments, and
    // its presence is re-confirmed by a read-back HEAD before any DELETE.
    // The deletable ceiling never exceeds the **published** cursor — the
    // db-folded extent build_manifest can prove (its own-cursor attestation
    // + rows/deleted-set fold every published entry's effect), so a segment
    // the db has not folded (`published < max_seq`) is always skipped, never
    // retroactively covered.
    let published = db.published_cursor()?;
    let (ceiling, manifest_written_ts) = match get_manifest(s3, bucket, &owner).await {
        Ok(manifest) => {
            let stored = manifest.header.cursors.get(&owner).copied().unwrap_or(0);
            if stored < published {
                // The on-wire manifest predates segments published since it
                // was written (review round 0 major: without this the frozen
                // cursor wedged every later segment at ManifestCoverageBelow
                // forever — unbounded journal growth, defeating §2.10's ~2
                // week bound). Rebuild to cover the published cursor, PUT, and
                // read-back verify the ETag. Freshly written, so rule 2 holds
                // every segment back this pass (age 0); the reconfirm clock
                // resets on a coverage advance exactly as this function's
                // docstring promises, and deletion resumes ≥ 24 h later.
                let manifest = build_manifest(db, now)?;
                let etag = put_manifest(s3, bucket, &owner, &manifest).await?;
                verify_manifest_present(s3, bucket, &owner, Some(&etag), db).await?;
                (
                    manifest.header.cursors.get(&owner).copied().unwrap_or(0),
                    now,
                )
            } else {
                // Coverage already current: use it as-is, re-confirmed by a
                // read-back HEAD (a transient anomaly aborts the whole pass).
                // The unchanged manifest keeps aging in place, so rule 2 can
                // fire on this pass.
                verify_manifest_present(s3, bucket, &owner, None, db).await?;
                (stored, manifest.header.written_server_ts)
            }
        }
        Err(ManifestError::S3(e)) if e.is_no_such_key() => {
            // Absent: build + PUT a fresh covering manifest and read-back
            // verify its ETag. Freshly written, so its written_server_ts is
            // `now` and rule 2 holds every segment back this pass (age 0) —
            // deletion begins only on a later pass, ≥ 24 h on.
            let manifest = build_manifest(db, now)?;
            let etag = put_manifest(s3, bucket, &owner, &manifest).await?;
            verify_manifest_present(s3, bucket, &owner, Some(&etag), db).await?;
            (
                manifest.header.cursors.get(&owner).copied().unwrap_or(0),
                now,
            )
        }
        Err(e) => return Err(e.into()),
    };

    let active = active_devices(s3, bucket, clock, cfg).await?;
    let horizon = horizon_applied(&active, &owner);
    let manifest_age = now.saturating_sub(manifest_written_ts);

    let mut summary = CompactionSummary::default();
    for seg in &segments {
        match segment_delete_decision(seg, ceiling, manifest_age, horizon, db, cfg, now)? {
            Decision::Delete => {
                s3.delete_object(bucket, &journal_segment_key(&owner, seg.first_seq))
                    .await?;
                summary.deleted_seqs.push(seg.first_seq);
            }
            Decision::Skip(reason) => summary.skipped.push(SkippedSegment {
                first_seq: seg.first_seq,
                max_seq: seg.max_seq,
                reason,
            }),
        }
    }
    summary.deleted_seqs.sort_unstable();
    Ok(summary)
}

// ---------------------------------------------------------------------------
// Tombstone GC (worker / backfill role, §2.10)
// ---------------------------------------------------------------------------

/// Why a tombstone was retained (not GC'd) this pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GcSkipReason {
    /// (b) Younger than the 30-day Recently-Deleted grace window.
    WithinGrace {
        /// Its server-asserted age in seconds (see `tombstone_age`): `0`
        /// when the listing carried no parsable `LastModified`.
        age_secs: i64,
    },
    /// (a) Not every active device has applied past it and the 14-day cap
    /// has not elapsed.
    HorizonBlocked,
    /// (d) A final journal re-read found a resurrecting `put` with a vv that
    /// dominates the tombstone's — edits beat deletes (§2.7); the data keys
    /// are preserved.
    Superseded,
}

/// One tombstone destroyed this pass, with exactly what was removed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DestroyedTombstone {
    /// The deleted image's relkey.
    pub relkey: RelKey,
    /// Data keys (`library/…` original/sidecar(s)/xmp) DELETEd for it.
    pub data_keys: Vec<String>,
    /// Content-addressed preview/thumb objects DELETEd because this was the
    /// last reference to their `content_id` (none when the content is still
    /// live under another relkey or another in-grace tombstone).
    pub content_destroyed: Vec<ContentId>,
}

/// One tombstone retained this pass, with the reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetainedTombstone {
    /// The tombstone's relkey.
    pub relkey: RelKey,
    /// Why it was kept.
    pub reason: GcSkipReason,
}

/// What one [`tombstone_gc`] pass did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GcSummary {
    /// Tombstones destroyed (data keys + tombstone object deleted; content
    /// objects deleted when unreferenced).
    pub destroyed: Vec<DestroyedTombstone>,
    /// Tombstones retained, with reasons.
    pub retained: Vec<RetainedTombstone>,
}

/// §2.10 tombstone GC (the worker, or a desktop with `worker_backfill`).
/// Destroys a tombstone's data keys and the tombstone object only when
/// **all** hold, and only after the deletion is durable in the runner's
/// deleted set:
///
/// - **(a) horizon/cap** — every active device has applied past it, or the
///   14-day cap has elapsed. (Once (b)'s 30-day grace holds, the 14-day cap
///   trivially holds too, so a laggard never blocks a 30-day-old tombstone.)
/// - **(b) grace** — it is ≥ `grace_secs` (30 days) old in server time
///   ([`GcSkipReason::WithinGrace`] otherwise). Age (for (a) too) is the
///   **server-asserted** `min(now - server_ts, now - LastModified)` — the
///   body's `server_ts` can only make a tombstone look younger, never older.
/// - **(c) deleted-set fold, first** — its `{del, vv, server_ts}` row is
///   folded into the **runner's** manifest and that manifest is PUT and
///   **read-back verified present** *before* any data key is destroyed
///   ([`CompactError::DeletedSetNotVerified`] otherwise). This is the
///   §2.3/A3 resurrection fix: a bootstrapping or long-dormant device learns
///   the deletion from the retained deleted set, **not** from the vanished
///   tombstone. The deleted set is retained 12 months.
/// - **(d) not superseded** — a final journal re-read (all devices'
///   prefixes) finds no resurrecting `put` whose vv dominates the
///   tombstone's ([`GcSkipReason::Superseded`] otherwise). Edits beat
///   deletes survive GC (§2.7/§2.6).
///
/// Destruction = DELETE the original + sidecar(s) + xmp data keys **and**
/// the content-addressed `preview`/`thumb` objects — the latter **only when
/// no live relkey and no retained tombstone references that `content_id`**
/// (content-id liveness: two images sharing byte-identical originals keep
/// the shared preview/thumb until *both* are gone; a tombstone retained this
/// pass for *any* reason — in-grace or horizon-blocked — still keeps its
/// data keys, so it protects the shared content too). The order is strict and
/// pinned by the crash-safety test: fold-and-verify (c) → DELETE data/content
/// keys → DELETE the tombstone object. A crash between fold and DELETE leaves
/// a recoverable state — the deleted-set record is already durable, so a
/// re-run completes and no key is ever destroyed without its record.
/// Idempotent: a re-run finds the data keys already 404 and completes.
pub async fn tombstone_gc(
    db: &SyncDb,
    s3: &impl S3TransferApi,
    bucket: &str,
    clock: &ServerClock,
    cfg: &CompactConfig,
) -> Result<GcSummary, CompactError> {
    let now = clock.now_server();
    let runner = db.device_id().clone();

    // Gather every tombstone object + the content id of the item it covers
    // (read from the runner's local item record, which carries the original's
    // content_id — needed for the content-id liveness check).
    let prefix = format!("{CONTROL_PREFIX}tombstones/");
    let objects = list_objects_under(s3, bucket, &prefix).await?;
    struct TombCandidate {
        tomb: Tombstone,
        content_id: Option<ContentId>,
        /// The tombstone object's server-asserted write instant (the
        /// listing's `LastModified`, unix seconds); `None` when the listing
        /// carried none or it did not parse.
        last_modified_unix: Option<i64>,
    }
    let mut candidates: Vec<TombCandidate> = Vec::new();
    let mut unstamped = 0usize;
    for obj in &objects {
        let key = &obj.key;
        if !matches!(classify_key(key), KeyClass::Tombstone { .. }) {
            continue;
        }
        let last_modified_unix = obj.last_modified.as_deref().and_then(parse_last_modified);
        if last_modified_unix.is_none() {
            unstamped += 1;
        }
        let body = match s3.get_object(bucket, key, None).await {
            Ok(output) => output.body.collect_capped(TOMBSTONE_MAX_BYTES).await?,
            // Vanished between LIST and GET (another runner raced us): a
            // stale LIST never forces an action, so skip it.
            Err(e) if e.is_no_such_key() => continue,
            Err(e) => return Err(e.into()),
        };
        // A malformed tombstone object is a foreign/corrupt document — never
        // destroy on its say-so; skip it (§2.1 principle 3).
        let Ok(tomb) = serde_json::from_slice::<Tombstone>(&body) else {
            continue;
        };
        let content_id = db.get_item(&tomb.relkey)?.and_then(|r| r.content_id);
        candidates.push(TombCandidate {
            tomb,
            content_id,
            last_modified_unix,
        });
    }
    if unstamped > 0 {
        // Once per pass, not per key: a listing without a usable
        // `LastModified` only ever delays GC (those tombstones are retained
        // as age 0 below), so this is a diagnostic, never an error.
        eprintln!(
            "rrcloud-worker: warning: {unstamped} tombstone listing entr{} under {prefix:?} \
             carried no parsable LastModified; retaining them this pass (age treated as 0)",
            if unstamped == 1 { "y" } else { "ies" }
        );
    }

    let active = active_devices(s3, bucket, clock, cfg).await?;

    // §2.10 (d) "a final journal re-read (all devices' prefixes)": the
    // on-wire truth, consulted in UNION with the runner's local converged
    // state for both the supersession and the content-id liveness checks.
    // This closes the catch-up→GC race — a resurrecting put (or a live
    // sibling sharing a content_id) that is durably published but not yet
    // folded into the runner's local state would otherwise be invisible to
    // a possibly-stale local snapshot, and GC would destroy data a live
    // dominating version still references (review round 0 blocker + major,
    // Garage-probe-verified). The local state still covers a put whose
    // segment was already compacted away, so neither source alone is enough.
    let fleet = read_fleet_entries(s3, bucket).await?;

    // Classify each tombstone against the 4-condition gate.
    enum Class {
        Eligible,
        Retained(GcSkipReason),
    }
    let mut classes: Vec<Class> = Vec::with_capacity(candidates.len());
    for c in &candidates {
        // Server-asserted age for BOTH (b) grace and (a) cap: the body's
        // `server_ts` can only make a tombstone look younger, never older.
        // No usable `LastModified` -> retained within grace as age 0 (a
        // stale/missing listing field delays GC, never causes destruction).
        let Some(age) = tombstone_age(now, c.tomb.server_ts, c.last_modified_unix) else {
            classes.push(Class::Retained(GcSkipReason::WithinGrace { age_secs: 0 }));
            continue;
        };
        // (b) grace.
        if age < cfg.grace_secs {
            classes.push(Class::Retained(GcSkipReason::WithinGrace { age_secs: age }));
            continue;
        }
        // (d) not superseded by a resurrecting put (edits beat deletes) —
        // local converged state OR the final journal re-read.
        if is_superseded(db, &c.tomb)? || fleet_supersedes(&fleet, &c.tomb) {
            classes.push(Class::Retained(GcSkipReason::Superseded));
            continue;
        }
        // (a) every active device applied past it, or the 14-day cap elapsed.
        // The "applied past it" fast path compares the del entry's own
        // journal **seq** (from the re-read — seq space) against each active
        // device's applied cursor (also seq space); a tombstone carries no
        // seq of its own, so when its del has been compacted off the wire
        // the 14-day cap is the sole (a) gate (review round 0 minor: the old
        // code compared a seq-space horizon against a vv-space component —
        // different counters — which passed prematurely under any
        // grace<cap config; past the default 30-day grace the cap holds
        // anyway, so defaults are unchanged).
        let cap_elapsed = age > cfg.laggard_cap_secs;
        let applied_past = match fleet_del_seq(&fleet, &c.tomb) {
            Some(del_seq) => horizon_applied(&active, &c.tomb.device) >= del_seq,
            None => false,
        };
        if applied_past || cap_elapsed {
            classes.push(Class::Eligible);
        } else {
            classes.push(Class::Retained(GcSkipReason::HorizonBlocked));
        }
    }

    let mut summary = GcSummary::default();
    let any_eligible = classes.iter().any(|c| matches!(c, Class::Eligible));
    if any_eligible {
        // Content ids a *retained* tombstone still references: its data keys
        // survive this pass, so the shared content must too (content-id
        // liveness). This is EVERY `Retained(_)` tombstone, not only
        // `WithinGrace` ones — a `HorizonBlocked` tombstone keeps its
        // original/sidecar this pass just the same, so destroying a shared
        // content-addressed preview/thumb it still points at would strand it
        // (review round 1 minor: previously only `WithinGrace` siblings were
        // counted, so an eligible sibling could destroy content a
        // HorizonBlocked one still referenced under an aggressive grace<cap
        // policy).
        let mut retained_content: BTreeSet<ContentId> = BTreeSet::new();
        for (i, c) in candidates.iter().enumerate() {
            if matches!(classes[i], Class::Retained(_)) {
                if let Some(cid) = &c.content_id {
                    retained_content.insert(cid.clone());
                }
            }
        }

        // Content ids referenced by a live relkey, computed ONCE for the whole
        // pass (review round 1 minor efficiency): a single `iter_items` scan
        // for local live references, unioned with one `fleet_content_live`
        // sweep over the distinct candidate content ids for on-wire live
        // siblings not yet folded locally. Previously both were recomputed
        // inside the destroy loop for every eligible tombstone — N full
        // item-table scans plus N quadratic fleet scans on the hot path.
        let mut live_content: BTreeSet<ContentId> = BTreeSet::new();
        for (_, r) in db.iter_items()? {
            if !r.deleted {
                if let Some(cid) = r.content_id {
                    live_content.insert(cid);
                }
            }
        }
        let candidate_cids: BTreeSet<ContentId> = candidates
            .iter()
            .filter_map(|c| c.content_id.clone())
            .collect();
        for cid in &candidate_cids {
            if !live_content.contains(cid) && fleet_content_live(&fleet, cid) {
                live_content.insert(cid.clone());
            }
        }

        // (c) deleted-set fold, FIRST. Ensure each eligible tombstone's
        // {del, vv, server_ts} row is durable in the runner's deleted set,
        // then PUT the runner's manifest and read-back verify it present
        // BEFORE any data key is destroyed. A crash after this point leaves
        // the deletion already learnable from the retained deleted set
        // (§2.3/A3), never a key destroyed without its record.
        for (i, c) in candidates.iter().enumerate() {
            if matches!(classes[i], Class::Eligible) {
                db.record_deleted(
                    &c.tomb.relkey,
                    &DeletedRecord {
                        vv: c.tomb.vv.clone(),
                        server_ts: c.tomb.server_ts,
                    },
                )?;
            }
        }
        let manifest = build_manifest(db, now)?;
        let etag = put_manifest(s3, bucket, &runner, &manifest).await?;
        verify_deleted_set_folded(s3, bucket, &runner, &etag, db).await?;

        for (i, c) in candidates.iter().enumerate() {
            if !matches!(classes[i], Class::Eligible) {
                continue;
            }
            let relkey = &c.tomb.relkey;
            // Destroy order (pinned by the crash test): data keys → content
            // keys → the tombstone object.
            let mut data_keys = Vec::new();
            for kind in &c.tomb.kinds {
                let key = match kind {
                    Kind::Original | Kind::Xmp => library_key(relkey),
                    Kind::Sidecar => sidecar_key(relkey),
                    // Preview/thumb/pack/meta are content-addressed or not
                    // per-relkey data keys; they are handled by the content
                    // liveness step (or not owned by this tombstone).
                    _ => continue,
                };
                s3.delete_object(bucket, &key).await?;
                data_keys.push(key);
            }

            // Content-id liveness: destroy the content-addressed
            // preview/thumb objects ONLY when no live relkey and no retained
            // tombstone still references the content_id. Both sets are
            // precomputed above: `live_content` from local state unioned with
            // the final journal re-read (a byte-identical sibling on another
            // device not yet folded locally still protects the shared
            // content — review round 0 major, Garage-probe-verified), and
            // `retained_content` from every tombstone kept this pass.
            let mut content_destroyed = Vec::new();
            if let Some(content) = &c.content_id {
                let live_ref = live_content.contains(content);
                let grace_ref = retained_content.contains(content);
                if !live_ref && !grace_ref {
                    s3.delete_object(bucket, &preview_key(content)).await?;
                    s3.delete_object(bucket, &thumb_key(content, ThumbSize::Small))
                        .await?;
                    s3.delete_object(bucket, &thumb_key(content, ThumbSize::Medium))
                        .await?;
                    content_destroyed.push(content.clone());
                }
            }

            s3.delete_object(bucket, &tombstone_key(relkey)).await?;
            summary.destroyed.push(DestroyedTombstone {
                relkey: relkey.clone(),
                data_keys,
                content_destroyed,
            });
        }
    }

    for (i, c) in candidates.iter().enumerate() {
        if let Class::Retained(reason) = &classes[i] {
            summary.retained.push(RetainedTombstone {
                relkey: c.tomb.relkey.clone(),
                reason: reason.clone(),
            });
        }
    }
    Ok(summary)
}

// ---------------------------------------------------------------------------
// Device lifecycle (§2.10)
// ---------------------------------------------------------------------------

/// Retire `device` explicitly (§2.10 device lifecycle): PUT its
/// `devices/<id>.retired` marker. Idempotent (re-retiring is a no-op PUT).
/// Retirement removes the device from the active set immediately (so a dead
/// phone or CronJob identity stops gating horizons at once); a retired
/// device that returns must re-register as a **new** device and bootstrap
/// from manifests (§2.3 quarantine prevents it resurrecting anything).
pub async fn retire_device(
    s3: &impl S3TransferApi,
    bucket: &str,
    device: &DeviceId,
) -> Result<(), CompactError> {
    s3.put_object(
        bucket,
        &device_retired_key(device),
        Bytes::new(),
        &PutObjectOptions::default(),
    )
    .await?;
    Ok(())
}

/// The §2.10 auto-retire sweep: list `devices/`, GET each entry, and PUT a
/// `.retired` marker for every registered, not-already-retired device whose
/// `last_seen_server_ts` is older than `cfg.auto_retire_secs` (90 days,
/// server time). Returns the devices newly retired this sweep, ascending.
/// Idempotent: a device already carrying a `.retired` marker is skipped, so
/// a re-run returns an empty list.
///
/// §2.2 first-heartbeat caveat: a device whose registry entry has only ever
/// been written once carries a possibly-skewed `last_seen_server_ts`; the
/// sweep must not auto-retire on that single arbitrarily-skewed value (the
/// implementation gates accordingly).
pub async fn auto_retire_sweep(
    s3: &impl S3TransferApi,
    bucket: &str,
    clock: &ServerClock,
    cfg: &CompactConfig,
) -> Result<Vec<DeviceId>, CompactError> {
    let now = clock.now_server();
    let prefix = format!("{CONTROL_PREFIX}devices/");
    let keys = list_keys_under(s3, bucket, &prefix).await?;
    let mut retired: BTreeSet<DeviceId> = BTreeSet::new();
    let mut registered: BTreeSet<DeviceId> = BTreeSet::new();
    for key in &keys {
        match classify_key(key) {
            KeyClass::DeviceRetired { device } => {
                retired.insert(device);
            }
            KeyClass::DeviceRegistry { device } => {
                registered.insert(device);
            }
            _ => {}
        }
    }
    let mut newly = Vec::new();
    for device in registered {
        // Idempotent: a device already carrying a `.retired` marker is
        // skipped, so a re-sweep returns an empty list.
        if retired.contains(&device) {
            continue;
        }
        let entry = get_device_entry(s3, bucket, &device).await?;
        let idle = now.saturating_sub(entry.last_seen_server_ts) > cfg.auto_retire_secs;
        // §2.2 first-heartbeat caveat: never auto-retire on a single,
        // possibly-skewed `last_seen`. A settled entry has heartbeated more
        // than once, so its `last_seen` sits well past its `created`; the
        // span is skew-immune (both are the device's own clock, so the error
        // cancels). A never-re-heartbeated entry has span ≈ 0 and is left
        // alone until another device or the UI retires it.
        let settled =
            entry.last_seen_server_ts.saturating_sub(entry.created) >= AUTO_RETIRE_MIN_SETTLED_SECS;
        if idle && settled {
            retire_device(s3, bucket, &device).await?;
            newly.push(device);
        }
    }
    // `registered` iterated ascending, so `newly` is ascending.
    Ok(newly)
}

/// §2.10 orphaned-prefix GC: the journal segments of **retired** devices,
/// folded into the GC **runner's** manifest (through the §2.6/§2.7 ordered
/// apply, [`fold_entry`]) and then deleted. Returns one [`CompactionSummary`]
/// per retired device whose prefix was touched, keyed by that device id.
/// Idempotent.
///
/// Deletion rules (the orphan form of the own-prefix rules): **coverage** —
/// the runner's freshly rebuilt manifest must cover the orphaned segment
/// (`cursors[device] >= max_seq`), and that manifest is PUT and **read-back
/// HEAD verified present before any DELETE** (§2.1 principle 3); **horizon**
/// — every other active device must have applied past the segment. The
/// own-prefix 24 h re-confirm does not translate: that insurance watches a
/// manifest that ages in place across passes, whereas the runner rewrites
/// and read-back-verifies its manifest *this* pass, which is a stronger
/// presence proof than waiting to re-HEAD it. The own-prefix 14-day cap does
/// not apply either — the runner holds no per-(device,seq) publish stamp for
/// a foreign prefix (see the loop) — so a laggard that has not caught up to a
/// retired device's prefix simply defers the reclaim to a later pass; the
/// fold is lossless and already durable, so nothing is lost by waiting.
///
/// This is how a dead device's journal growth is reclaimed without the dead
/// device ever running again: the live runner owns the fold (its manifest's
/// deleted/live rows carry the retired device's effects losslessly), so a
/// later bootstrapper still learns everything the orphaned prefix held.
pub async fn gc_retired_prefixes(
    db: &SyncDb,
    s3: &impl S3TransferApi,
    bucket: &str,
    clock: &ServerClock,
    cfg: &CompactConfig,
) -> Result<Vec<(DeviceId, CompactionSummary)>, CompactError> {
    let now = clock.now_server();
    let runner = db.device_id().clone();

    let prefix = format!("{CONTROL_PREFIX}devices/");
    let keys = list_keys_under(s3, bucket, &prefix).await?;
    let mut retired: BTreeSet<DeviceId> = BTreeSet::new();
    for key in &keys {
        if let KeyClass::DeviceRetired { device } = classify_key(key) {
            retired.insert(device);
        }
    }

    let active = active_devices(s3, bucket, clock, cfg).await?;

    // Fold each retired device's orphaned prefix into the runner's state
    // (live rows + deleted rows + an attested cursor), so a later
    // bootstrapper still learns every effect from the runner's manifest.
    struct Orphan {
        device: DeviceId,
        segments: Vec<SegExtent>,
    }
    let mut orphans: Vec<Orphan> = Vec::new();
    for device in retired {
        if device == runner {
            continue;
        }
        let jprefix = format!("{CONTROL_PREFIX}journal/{device}/");
        let jkeys = list_keys_under(s3, bucket, &jprefix).await?;
        let mut firsts: Vec<u64> = jkeys
            .iter()
            .filter_map(|k| match classify_key(k) {
                KeyClass::Journal { device: d, seq, .. } if d == device => Some(seq),
                _ => None,
            })
            .collect();
        firsts.sort_unstable();
        firsts.dedup();
        if firsts.is_empty() {
            continue;
        }
        // GET + decode each segment: an orphaned prefix carries no local
        // span cache, so the exact per-segment extent comes from the entries.
        let mut all_entries: Vec<JournalEntry> = Vec::new();
        let mut segments: Vec<SegExtent> = Vec::new();
        for &first in &firsts {
            let body = s3
                .get_object(bucket, &journal_segment_key(&device, first), None)
                .await?
                .body
                .collect_capped(SEGMENT_MAX_BYTES)
                .await?;
            let entries = decode_segment(&body)?;
            let count = entries.len() as u64;
            let max_seq = first + count.saturating_sub(1);
            segments.push(SegExtent {
                first_seq: first,
                max_seq,
            });
            all_entries.extend(entries);
        }
        let overall_max = all_entries.iter().map(|e| e.seq).max().unwrap_or(0);
        db.with_txn_err::<_, CompactError>(|txn| {
            for entry in &all_entries {
                fold_entry(txn, entry)?;
            }
            // Attest how far the runner has folded the orphaned prefix, so
            // the rebuilt runner manifest covers it (cursors[device]).
            txn.set_cursor(&device, overall_max)?;
            Ok(())
        })?;
        orphans.push(Orphan { device, segments });
    }

    if orphans.is_empty() {
        return Ok(Vec::new());
    }

    // Fold durable, then verify: PUT the runner manifest (now carrying the
    // orphans' effects + cursors) and read-back HEAD it present before any
    // orphaned segment is deleted (§2.1 principle 3).
    let manifest = build_manifest(db, now)?;
    let etag = put_manifest(s3, bucket, &runner, &manifest).await?;
    verify_manifest_present(s3, bucket, &runner, Some(&etag), db).await?;

    let mut out = Vec::new();
    for orphan in orphans {
        let horizon = horizon_applied(&active, &orphan.device);
        let ceiling = manifest
            .header
            .cursors
            .get(&orphan.device)
            .copied()
            .unwrap_or(0);
        let mut summary = CompactionSummary::default();
        for seg in &orphan.segments {
            if seg.max_seq > ceiling {
                summary.skipped.push(SkippedSegment {
                    first_seq: seg.first_seq,
                    max_seq: seg.max_seq,
                    reason: SkipReason::ManifestCoverageBelow { cursor: ceiling },
                });
                continue;
            }
            // The 14-day cap does NOT apply to a foreign (orphaned) prefix:
            // the runner's per-segment publish stamps are keyed by seq with
            // no device component (state.rs `segment_pub_ts_key`), so a
            // retired device's segment at first_seq=1 would read the
            // runner's OWN stamp for its own first_seq=1 — a cross-device
            // collision feeding the cap a wrong publish time (review round 0
            // minor). The fast path (every active peer applied past the
            // orphaned prefix) is the correct and sufficient route; the fold
            // is already read-back verified above, so an un-cleared horizon
            // simply retries next pass.
            if horizon >= seg.max_seq {
                s3.delete_object(bucket, &journal_segment_key(&orphan.device, seg.first_seq))
                    .await?;
                summary.deleted_seqs.push(seg.first_seq);
            } else {
                summary.skipped.push(SkippedSegment {
                    first_seq: seg.first_seq,
                    max_seq: seg.max_seq,
                    reason: SkipReason::HorizonBlocked { horizon },
                });
            }
        }
        summary.deleted_seqs.sort_unstable();
        out.push((orphan.device, summary));
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Pre-upload quarantine (§2.3)
// ---------------------------------------------------------------------------

/// The §2.3 pre-upload quarantine decision for local-only files.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuarantineDecision {
    /// The device's deletion-knowledge horizon is within the retention
    /// window (or there is nothing local-only): it can prove a key was not
    /// deleted, so local-only files may auto-re-upload through the ordinary
    /// lanes. No user decision needed.
    Clear,
    /// The device's applied-proof horizon predates the 12-month deleted-set
    /// retention window, so it **cannot** prove these local-only keys were
    /// not deleted. They must **not** auto-re-upload; the UI prompts the
    /// user, whose choice routes through [`resolve_quarantine`]. This
    /// converts the mass-resurrection failure of a long-dormant device into
    /// an explicit decision (§2.3).
    QuarantineRequired {
        /// The affected local-only relkeys, ascending.
        relkeys: Vec<RelKey>,
    },
}

/// Detects the §2.3 pre-upload quarantine condition: when
/// `now_server - db.applied_proof_server_ts()` exceeds `cfg.retention_secs`
/// (12 months) **and** there are local-only unprovable items (records
/// flagged [`crate::state::ItemRecord::base_unknown`], not soft-deleted),
/// returns [`QuarantineDecision::QuarantineRequired`] listing them — never
/// an auto re-upload. Otherwise [`QuarantineDecision::Clear`].
///
/// Pure decision (no network): the proof horizon and the candidate items are
/// both local state the engine's catch-up maintains.
pub fn detect_local_only_unprovable(
    db: &SyncDb,
    clock: &ServerClock,
    cfg: &CompactConfig,
) -> Result<QuarantineDecision, CompactError> {
    let now = clock.now_server();
    // The device can prove a key was not deleted only while its deletion
    // knowledge is complete to within the retention window. No measured
    // proof horizon at all is treated as predating it (it cannot prove
    // anything).
    let predates = match db.applied_proof_server_ts()? {
        Some(ts) => now.saturating_sub(ts) > cfg.retention_secs,
        None => true,
    };
    if !predates {
        return Ok(QuarantineDecision::Clear);
    }
    let mut relkeys: Vec<RelKey> = db
        .iter_items()?
        .into_iter()
        .filter(|(_, record)| record.base_unknown && !record.deleted)
        .map(|(key, _)| key)
        .collect();
    if relkeys.is_empty() {
        return Ok(QuarantineDecision::Clear);
    }
    relkeys.sort();
    Ok(QuarantineDecision::QuarantineRequired { relkeys })
}

/// The user's resolution of one quarantined relkey.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuarantineResolution {
    /// Restore: re-advertise the local bytes as a `put` (re-enter the upload
    /// lanes).
    Keep,
    /// Discard: delete the local file and drop the local-only record.
    Discard,
}

/// What [`resolve_quarantine`] did for one relkey.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuarantineOutcome {
    /// `Keep`: a re-advertise `put` was staged (the item re-enters the
    /// upload lanes; `base_unknown` cleared).
    Restored {
        /// The restored relkey.
        relkey: RelKey,
    },
    /// `Discard`: the local file was removed and the local-only record
    /// dropped.
    Discarded {
        /// The discarded relkey.
        relkey: RelKey,
    },
    /// `Keep` was **refused** because a cloud tombstone or deleted-set row
    /// still covers this relkey: re-advertising would resurrect a
    /// legitimately-deleted key. The user must restore through the §2.7
    /// Recently-Deleted flow instead (which mints a vv dominating the
    /// tombstone), not through quarantine. Nothing was staged.
    RefusedResurrection {
        /// The relkey left untouched.
        relkey: RelKey,
    },
}

/// Routes one quarantined relkey per the user's [`QuarantineResolution`]
/// (§2.3/§2.10):
///
/// - [`QuarantineResolution::Keep`] stages a re-advertise `put` and clears
///   the item's `base_unknown` — **unless** a cloud tombstone or
///   deleted-set row still covers the relkey, in which case it is refused
///   ([`QuarantineOutcome::RefusedResurrection`]); quarantine must never be
///   the lane that resurrects a legitimately-deleted key.
/// - [`QuarantineResolution::Discard`] deletes the local file under
///   `sync_root` and drops the local-only record.
///
/// Neither path resurrects a deleted key: `Keep` is gated on the absence of
/// a covering deletion record, `Discard` only removes local state.
pub async fn resolve_quarantine(
    db: &SyncDb,
    s3: &impl S3TransferApi,
    bucket: &str,
    sync_root: &Path,
    relkey: &RelKey,
    resolution: QuarantineResolution,
) -> Result<QuarantineOutcome, CompactError> {
    match resolution {
        QuarantineResolution::Keep => {
            // Gate: a cloud tombstone OR a local deleted-set row still
            // covering this relkey means re-advertising would resurrect a
            // legitimately-deleted key. Refuse — the §2.7 Recently-Deleted
            // flow (which mints a vv dominating the tombstone) is the only
            // lane that may bring it back.
            let tombstoned = match s3.head_object(bucket, &tombstone_key(relkey)).await {
                Ok(_) => true,
                Err(e) if e.is_no_such_key() => false,
                Err(e) => return Err(e.into()),
            };
            if tombstoned || db.get_deleted(relkey)?.is_some() {
                return Ok(QuarantineOutcome::RefusedResurrection {
                    relkey: relkey.clone(),
                });
            }
            // Re-advertise: clear `base_unknown` and re-enter the upload lane
            // (Dirty). No journal entry is published here — that is the
            // ordinary upload lane's job.
            if let Some(mut item) = db.get_item(relkey)? {
                item.base_unknown = false;
                item.state = ItemState::Dirty;
                db.replay_put_item(relkey, &item)?;
            }
            Ok(QuarantineOutcome::Restored {
                relkey: relkey.clone(),
            })
        }
        QuarantineResolution::Discard => {
            let path = local_path(relkey, sync_root);
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(source) => {
                    return Err(CompactError::Io {
                        relkey: relkey.clone(),
                        source,
                    })
                }
            }
            db.delete_item(relkey)?;
            Ok(QuarantineOutcome::Discarded {
                relkey: relkey.clone(),
            })
        }
    }
}

/// Crate-internal: does a `put` at vv `pv` keep an item alive against a
/// deletion at vv `dv` (§2.7 "edits beat deletes")? A put that **strictly
/// dominates** the deletion is a restore/resurrection; a put **concurrent**
/// with it is a genuine concurrent edit that also survives (the same
/// un-hide predicate the engine's `apply_put` deleted-record arm uses).
/// Only a put the deletion dominates-or-equals (`Less`/`Equal`) is
/// superseded by it. Used by **both** the local-state and the on-wire
/// journal supersession checks so one spelling of "resurrects" lives here.
pub(crate) fn put_supersedes_del(
    pv: &crate::clock::VersionVector,
    dv: &crate::clock::VersionVector,
) -> bool {
    matches!(compare(pv, dv), VvOrder::Greater | VvOrder::Concurrent)
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Fail-closed allocation bound on a fetched tombstone document (a few
/// hundred bytes in practice; a larger object is corrupt or hostile).
const TOMBSTONE_MAX_BYTES: usize = 64 * 1024;

/// The minimum join-to-last-seen span a device entry must show before the
/// auto-retire sweep trusts its `last_seen_server_ts` (§2.2 first-heartbeat
/// caveat). A never-re-heartbeated entry has a ~zero span and is left alone;
/// any settled device clears this by orders of magnitude. The span is
/// skew-immune — `created` and `last_seen` share the device's clock — so a
/// skewed first heartbeat cannot fake a settled entry.
const AUTO_RETIRE_MIN_SETTLED_SECS: i64 = DAY;

/// One own/orphaned journal segment's seq extent.
struct SegExtent {
    /// The segment's first entry seq (its filename seq).
    first_seq: u64,
    /// The segment's highest entry seq.
    max_seq: u64,
}

/// A per-segment compaction decision.
enum Decision {
    /// Delete the segment object.
    Delete,
    /// Keep it, with the reason.
    Skip(SkipReason),
}

/// A tombstone's age in server time, for both §2.10 gates — (b) the
/// Recently-Deleted grace and (a) the laggard cap.
///
/// §2.10 evaluates every age threshold in **server time** so that a device's
/// clock cannot age a tombstone out early — and the tombstone body's
/// `server_ts` is written by whichever peer created it, so a hostile or
/// buggy peer could backdate it and have the next GC pass destroy a live
/// item's data keys, bypassing the grace window that §2.7/§2.10 describe as
/// the safety net against exactly that device. The bucket's own
/// `LastModified` for the tombstone object is asserted by the server and
/// cannot be backdated by the writer, so the age is
/// `min(now - server_ts, now - last_modified)`, saturated at 0: the body can
/// only ever make a tombstone look *younger* (a re-PUT by a late writer
/// resets `LastModified` and merely delays GC — §2.1 principle 3), never
/// older. A negative server-asserted age (runner clock behind the server)
/// is age 0, not a reason to fall back to the body.
///
/// Returns `None` when the listing carried no parsable `LastModified`: the
/// caller must then **retain** the tombstone (treat as age 0), never trust
/// `server_ts` alone.
fn tombstone_age(now: i64, server_ts: i64, last_modified_unix: Option<i64>) -> Option<i64> {
    let asserted = now.saturating_sub(last_modified_unix?);
    let claimed = now.saturating_sub(server_ts);
    Some(claimed.min(asserted).max(0))
}

/// Parses a `ListObjectsV2` `LastModified` (RFC 3339 / ISO 8601, e.g.
/// `2026-10-10T12:34:56.000Z` on Garage and AWS) to unix seconds; `None`
/// when it does not parse.
fn parse_last_modified(s: &str) -> Option<i64> {
    time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
        .ok()
        .map(|dt| dt.unix_timestamp())
}

/// Lists every object key under `prefix` (paginated), bounded like the
/// reader's journal-listing lane so a hostile bucket cannot force unbounded
/// key buffering on phone-class targets.
async fn list_keys_under(
    s3: &impl S3Api,
    bucket: &str,
    prefix: &str,
) -> Result<Vec<String>, CompactError> {
    Ok(list_objects_under(s3, bucket, prefix)
        .await?
        .into_iter()
        .map(|o| o.key)
        .collect())
}

/// Lists every object under `prefix` (paginated) with its listing metadata
/// — notably the server-asserted `LastModified` that [`tombstone_gc`] ages
/// tombstones by — bounded like [`list_keys_under`].
async fn list_objects_under(
    s3: &impl S3Api,
    bucket: &str,
    prefix: &str,
) -> Result<Vec<ObjectSummary>, CompactError> {
    const MAX_PAGES: u32 = 10_000;
    const MAX_KEYS: usize = 1_000_000;
    let mut keys = Vec::new();
    let mut continuation_token: Option<String> = None;
    let mut pages = 0u32;
    loop {
        if pages >= MAX_PAGES {
            return Err(S3Error::InvalidResponse(format!(
                "ListObjectsV2 still truncated after {MAX_PAGES} pages under {prefix:?}"
            ))
            .into());
        }
        pages += 1;
        let page = s3
            .list_objects_v2(
                bucket,
                &ListObjectsV2Request {
                    prefix: Some(prefix.to_string()),
                    continuation_token: continuation_token.take(),
                    ..Default::default()
                },
            )
            .await?;
        keys.extend(page.objects);
        if keys.len() > MAX_KEYS {
            return Err(S3Error::InvalidResponse(format!(
                "ListObjectsV2 returned more than {MAX_KEYS} keys under {prefix:?}"
            ))
            .into());
        }
        if !page.is_truncated {
            return Ok(keys);
        }
        match page.next_continuation_token {
            Some(token) => continuation_token = Some(token),
            None => {
                return Err(S3Error::InvalidResponse(
                    "truncated ListObjectsV2 page without a NextContinuationToken".to_string(),
                )
                .into())
            }
        }
    }
}

/// The owner's own journal segments and their seq extents, derived from the
/// LIST of its prefix: because `freeze_next_segment` tiles seqs with no
/// holes, each segment's `max_seq` is one below the next segment's
/// `first_seq`, and the last segment's `max_seq` is the published cursor —
/// so no segment GET is needed.
async fn own_segment_extents(
    db: &SyncDb,
    s3: &impl S3Api,
    bucket: &str,
    owner: &DeviceId,
) -> Result<Vec<SegExtent>, CompactError> {
    let prefix = format!("{CONTROL_PREFIX}journal/{owner}/");
    let keys = list_keys_under(s3, bucket, &prefix).await?;
    let mut firsts: Vec<u64> = keys
        .iter()
        .filter_map(|k| match classify_key(k) {
            KeyClass::Journal { device, seq, .. } if device == *owner => Some(seq),
            _ => None,
        })
        .collect();
    firsts.sort_unstable();
    firsts.dedup();
    if firsts.is_empty() {
        return Ok(Vec::new());
    }
    let published = db.published_cursor()?;
    let mut out = Vec::with_capacity(firsts.len());
    for (i, &first) in firsts.iter().enumerate() {
        let max_seq = if i + 1 < firsts.len() {
            firsts[i + 1].saturating_sub(1)
        } else {
            published.max(first)
        };
        out.push(SegExtent {
            first_seq: first,
            max_seq,
        });
    }
    Ok(out)
}

/// The §2.10 3-rule gate for one segment: coverage (rule 1) → 24 h reconfirm
/// (rule 2) → fast path or 14-day cap (rule 3). A segment is deleted only
/// when all three clear.
fn segment_delete_decision(
    seg: &SegExtent,
    ceiling: u64,
    manifest_age: i64,
    horizon: u64,
    db: &SyncDb,
    cfg: &CompactConfig,
    now: i64,
) -> Result<Decision, CompactError> {
    // Rule 1: the manifest must provably fold the segment's entries.
    if seg.max_seq > ceiling {
        return Ok(Decision::Skip(SkipReason::ManifestCoverageBelow {
            cursor: ceiling,
        }));
    }
    // Rule 2: the covering manifest must have aged ≥ reconfirm_secs.
    if manifest_age < cfg.reconfirm_secs {
        return Ok(Decision::Skip(SkipReason::ReconfirmWindowOpen {
            manifest_age_secs: manifest_age,
        }));
    }
    // Rule 3: fast path (every active peer applied past it) OR the 14-day
    // cap (server time, from the per-segment publish stamp; an absent stamp
    // is conservative — never cap-deletes an unaged segment).
    let fast = horizon >= seg.max_seq;
    let capped = match db.segment_published_server_ts(seg.first_seq)? {
        Some(ts) => now.saturating_sub(ts) > cfg.laggard_cap_secs,
        None => false,
    };
    if fast || capped {
        Ok(Decision::Delete)
    } else {
        Ok(Decision::Skip(SkipReason::HorizonBlocked { horizon }))
    }
}

/// Read-back HEAD of `device`'s own manifest (§2.10 rule 1/2): requires it
/// present, and — when `expected_etag` names a PUT this pass — byte-identical
/// to that PUT. A failed or not-present read-back is
/// [`CompactError::ManifestNotVerified`] and aborts the whole compaction
/// pass (no segment deleted). Refreshes the stored server-time offset from
/// the HEAD's `Date` while here.
async fn verify_manifest_present(
    s3: &impl S3Api,
    bucket: &str,
    device: &DeviceId,
    expected_etag: Option<&str>,
    db: &SyncDb,
) -> Result<(), CompactError> {
    match s3.head_object(bucket, &manifest_key(device)).await {
        Ok(head) => {
            let _ = record_server_time(db, head.date.as_deref());
            if let Some(expected) = expected_etag {
                if head.e_tag != expected {
                    return Err(CompactError::ManifestNotVerified {
                        detail: format!(
                            "manifest ETag after PUT is {:?}, expected {:?}",
                            head.e_tag, expected
                        ),
                    });
                }
            }
            Ok(())
        }
        Err(e) => Err(CompactError::ManifestNotVerified {
            detail: format!("read-back HEAD of {} failed: {e}", manifest_key(device)),
        }),
    }
}

/// Read-back HEAD of the GC runner's manifest after the deleted-set fold PUT
/// (§2.10 (c)): requires it present and byte-identical to the fold PUT before
/// any data key is destroyed. A failed read-back is
/// [`CompactError::DeletedSetNotVerified`].
async fn verify_deleted_set_folded(
    s3: &impl S3Api,
    bucket: &str,
    device: &DeviceId,
    expected_etag: &str,
    db: &SyncDb,
) -> Result<(), CompactError> {
    match s3.head_object(bucket, &manifest_key(device)).await {
        Ok(head) => {
            let _ = record_server_time(db, head.date.as_deref());
            if head.e_tag != expected_etag {
                return Err(CompactError::DeletedSetNotVerified {
                    detail: format!(
                        "runner manifest ETag after fold PUT is {:?}, expected {:?}",
                        head.e_tag, expected_etag
                    ),
                });
            }
            Ok(())
        }
        Err(e) => Err(CompactError::DeletedSetNotVerified {
            detail: format!("read-back HEAD of {} failed: {e}", manifest_key(device)),
        }),
    }
}

/// The §2.10 (d) superseded check against the runner's **local converged
/// state**: a resurrecting `put` the runner has already applied shows up as
/// a **live** item record whose vv supersedes the tombstone's (edits beat
/// deletes, §2.7/§2.6). This is only one half of (d): a put durably
/// published but **not yet folded locally** would not appear here, so
/// [`tombstone_gc`] takes the UNION of this with [`fleet_supersedes`] (the
/// §2.10-mandated final journal re-read) before destroying anything — the
/// local check alone consulted a possibly-stale snapshot and destroyed a
/// restore's data keys in the catch-up→GC race (review round 0 blocker).
fn is_superseded(db: &SyncDb, tomb: &Tombstone) -> Result<bool, CompactError> {
    match db.get_item(&tomb.relkey)? {
        Some(item) if !item.deleted => Ok(put_supersedes_del(&item.vv, &tomb.vv)),
        _ => Ok(false),
    }
}

/// The item relkey an entry's bucket key addresses (originals/sidecars/xmp),
/// or `None` for a key outside the per-item namespace.
fn entry_item_relkey(entry: &JournalEntry) -> Option<RelKey> {
    match classify_key(&entry.key) {
        KeyClass::Original { relkey }
        | KeyClass::Sidecar { relkey, .. }
        | KeyClass::Xmp { relkey } => Some(relkey),
        _ => None,
    }
}

/// The §2.10 (d) "final journal re-read (all devices' prefixes)": LISTs the
/// whole journal namespace, GETs + decodes every segment, and returns all
/// entries. Consulted in union with the runner's local converged state so a
/// resurrecting put (or a live content sibling) durably on the wire but not
/// yet folded locally cannot be destroyed. A segment that vanished between
/// the LIST and the GET (a racing compaction) is skipped — a stale LIST
/// never forces an action (§2.1 principle 3).
async fn read_fleet_entries(
    s3: &impl S3TransferApi,
    bucket: &str,
) -> Result<Vec<JournalEntry>, CompactError> {
    let prefix = format!("{CONTROL_PREFIX}journal/");
    let keys = list_keys_under(s3, bucket, &prefix).await?;
    let mut entries = Vec::new();
    for key in &keys {
        if !matches!(classify_key(key), KeyClass::Journal { .. }) {
            continue;
        }
        let body = match s3.get_object(bucket, key, None).await {
            Ok(output) => output.body.collect_capped(SEGMENT_MAX_BYTES).await?,
            Err(e) if e.is_no_such_key() => continue,
            Err(e) => return Err(e.into()),
        };
        entries.extend(decode_segment(&body)?);
    }
    Ok(entries)
}

/// On-wire supersession: a `put` on the tombstone's relkey that the
/// deletion does not dominate ([`put_supersedes_del`] — §2.7 edits beat
/// deletes). The resurrecting-put half of §2.10 (d).
fn fleet_supersedes(entries: &[JournalEntry], tomb: &Tombstone) -> bool {
    entries.iter().any(|e| {
        e.op == Op::Put
            && entry_item_relkey(e).as_ref() == Some(&tomb.relkey)
            && put_supersedes_del(&e.vv, &tomb.vv)
    })
}

/// The seq of the tombstone's own `del` entry on the wire (its deleting
/// device's prefix, matching relkey + vv) — the seq-space value §2.10 (a)'s
/// "every active device applied past it" fast path compares against each
/// active device's applied cursor (both seqs). `None` when the del's
/// segment has been compacted away, in which case the 14-day cap is the
/// sole (a) gate.
fn fleet_del_seq(entries: &[JournalEntry], tomb: &Tombstone) -> Option<u64> {
    entries
        .iter()
        .filter(|e| {
            e.op == Op::Del
                && e.device == tomb.device
                && entry_item_relkey(e).as_ref() == Some(&tomb.relkey)
                && compare(&e.vv, &tomb.vv) == VvOrder::Equal
        })
        .map(|e| e.seq)
        .min()
}

/// On-wire content-id liveness: some relkey carries a live `put` referencing
/// `content` — a put that no `del` on the same relkey dominates-or-equals.
/// The fleet half of the content-destruction guard: a shared preview/thumb
/// stays while any live reference to its byte-identical original exists.
fn fleet_content_live(entries: &[JournalEntry], content: &ContentId) -> bool {
    entries.iter().any(|put| {
        if put.op != Op::Put || put.content_id.as_ref() != Some(content) {
            return false;
        }
        let Some(rel) = entry_item_relkey(put) else {
            return false;
        };
        // Live iff no del on this relkey dominates-or-equals the put's vv.
        !entries.iter().any(|d| {
            d.op == Op::Del
                && entry_item_relkey(d).as_ref() == Some(&rel)
                && matches!(compare(&put.vv, &d.vv), VvOrder::Less | VvOrder::Equal)
        })
    })
}

/// Folds one orphaned-prefix journal entry into the runner's **converged**
/// state inside a caller-held transaction — the §2.6/§2.7 *ordered* apply,
/// **not** a blind replay. Every entry is ordered (by version vector)
/// against the runner's current converged head for its relkey, so an entry
/// the head already supersedes is a no-op — never a resurrection, never a
/// deletion-loss. This is the same vv-aware rule `merge()` and the engine
/// [`crate::reader::JournalConsumer`] run; the download/loser side effects a
/// live device would take are deliberately **not** taken here (the fold's
/// only output is the runner's manifest rows + deleted set).
///
/// Review round 0 blocker (two directions, Garage-probe-verified): the prior
/// implementation applied a `put` through an unconditional
/// [`StateTxn::replay_put_item`] and a `del` through
/// [`StateTxn::delete_item`] + [`StateTxn::record_deleted`] with **no vv
/// comparison at all**. Folding a stale orphan `put` over a dominating
/// deletion the runner had already converged flipped the item live *and*
/// left the deleted-set row standing — the rebuilt manifest then carried
/// both a live row and a deleted row for one relkey (the §2.3 live/deleted
/// contradiction `build_manifest`'s docs forbid), the A3 mass-resurrection.
/// Folding a stale orphan `del` over a live dominating edit destroyed the
/// live version and advertised the deletion — a real deletion-loss in the
/// runner's authoritative manifest. The ordered apply below closes both.
///
/// Content-level unclassifiable keys are skipped (the [`JournalConsumer`]
/// error contract); `Move`/`Attest` ride later units.
fn fold_entry(txn: &StateTxn<'_>, entry: &JournalEntry) -> Result<(), CompactError> {
    let relkey = match classify_key(&entry.key) {
        KeyClass::Original { relkey }
        | KeyClass::Sidecar { relkey, .. }
        | KeyClass::Xmp { relkey } => relkey,
        _ => return Ok(()),
    };
    match entry.op {
        Op::Put => fold_put(txn, entry, &relkey),
        Op::Del => fold_del(txn, entry, &relkey),
        Op::Move | Op::Attest => Ok(()),
    }
}

/// The §2.6/§2.7 ordered apply of an orphaned `put` against the runner's
/// converged head (its live/soft-deleted item record and any deleted-set
/// row). Adopts the put only when the head does not already dominate it;
/// edits beat deletes (a put a deletion does not dominate un-hides).
fn fold_put(txn: &StateTxn<'_>, entry: &JournalEntry, relkey: &RelKey) -> Result<(), CompactError> {
    let Some(local) = txn.get_item(relkey)? else {
        // Unknown item: order against any recorded deletion (§2.7). A put
        // the deletion dominates (or equals) stays hidden behind the row;
        // otherwise it is live and clears the row.
        let row = txn.get_deleted(relkey)?;
        let hidden = row
            .as_ref()
            .is_some_and(|r| matches!(compare(&entry.vv, &r.vv), VvOrder::Less | VvOrder::Equal));
        let mut record = fold_item_record(entry);
        if let Some(row) = &row {
            record.vv.merge(&row.vv);
        }
        record.deleted = hidden;
        txn.replay_put_item(relkey, &record)?;
        if !hidden && row.is_some() {
            txn.remove_deleted(relkey)?;
        }
        return Ok(());
    };
    match compare(&entry.vv, &local.vv) {
        // The runner's converged head already dominates (or equals) the
        // folded put: it is already reflected, so applying it would
        // resurrect a deletion the head folds (the round-0 resurrection
        // blocker). No-op.
        VvOrder::Less | VvOrder::Equal => Ok(()),
        // A strictly newer version the runner had not folded: adopt it live.
        VvOrder::Greater => fold_adopt_put(txn, entry, relkey, &local),
        // §2.6 case 4 over the converged head. A soft-deleted head has no
        // live content to keep, so a concurrent put un-hides and adopts
        // outright (edits beat deletes); a live head keeps its primary when
        // it wins the deterministic (ts, device) pick, folding both vvs.
        VvOrder::Concurrent => {
            if local.deleted || fold_put_wins(entry, &local) {
                fold_adopt_put(txn, entry, relkey, &local)
            } else {
                let mut record = local.clone();
                record.vv.merge(&entry.vv);
                record.deleted = false;
                txn.replay_put_item(relkey, &record)?;
                txn.remove_deleted(relkey)?;
                Ok(())
            }
        }
    }
}

/// Adopt the folded `put` as the item's live head, folding the prior head's
/// lineage (including any deletion the head carried) into the vv and
/// clearing any deleted-set row — so the manifest advertises one truth.
fn fold_adopt_put(
    txn: &StateTxn<'_>,
    entry: &JournalEntry,
    relkey: &RelKey,
    local: &ItemRecord,
) -> Result<(), CompactError> {
    let mut record = fold_item_record(entry);
    record.vv.merge(&local.vv);
    record.pinned = local.pinned;
    record.last_access_unix = local.last_access_unix;
    record.deleted = false;
    txn.replay_put_item(relkey, &record)?;
    txn.remove_deleted(relkey)?;
    Ok(())
}

/// The §2.7 ordered apply of an orphaned `del` against the runner's
/// converged head. A `del` a live head strictly dominates (or that is
/// concurrent with a live edit) is dropped — edits beat deletes; only a
/// `del` that dominates (or equals a hidden head) hides the item and records
/// the deleted-set row.
fn fold_del(txn: &StateTxn<'_>, entry: &JournalEntry, relkey: &RelKey) -> Result<(), CompactError> {
    let Some(local) = txn.get_item(relkey)? else {
        // Unknown item: record the deletion anchor so a slower put cannot
        // resurrect it out of order (§2.7), merging prior knowledge.
        return fold_record_deletion(txn, relkey, &entry.vv, entry.ts);
    };
    if local.deleted {
        // Already hidden: two deletes of one item both stand — fold the vv
        // into the record and the row.
        let mut record = local.clone();
        record.vv.merge(&entry.vv);
        txn.replay_put_item(relkey, &record)?;
        return fold_record_deletion(txn, relkey, &entry.vv, entry.ts);
    }
    match compare(&entry.vv, &local.vv) {
        // The live head supersedes (Less) the del, or re-delivers an
        // already-applied one (Equal): nothing to delete — a stale orphan
        // del must not destroy a live dominating edit (the round-0
        // deletion-loss blocker).
        VvOrder::Less | VvOrder::Equal => Ok(()),
        // Dominating del: hide the item and record the deleted-set row.
        VvOrder::Greater => {
            let mut record = local.clone();
            record.vv.merge(&entry.vv);
            record.deleted = true;
            txn.replay_put_item(relkey, &record)?;
            fold_record_deletion(txn, relkey, &entry.vv, entry.ts)
        }
        // Concurrent del vs a live edit: EDITS BEAT DELETES (§2.7) — the
        // item stays live and absorbs the del's vv (no row; the deletion
        // lost). The fold needs only the runner's converged metadata to be
        // right for its manifest, so no side-effecting resurrection fires.
        VvOrder::Concurrent => {
            let mut record = local.clone();
            record.vv.merge(&entry.vv);
            txn.replay_put_item(relkey, &record)?;
            Ok(())
        }
    }
}

/// Records (or merges into) the per-item deleted-set row — vv max-merge,
/// `server_ts` max — matching the engine's `record_deletion_row` so the
/// fold's deleted set is identical to the one journal replay would produce.
fn fold_record_deletion(
    txn: &StateTxn<'_>,
    relkey: &RelKey,
    vv: &VersionVector,
    server_ts: i64,
) -> Result<(), CompactError> {
    let merged = match txn.get_deleted(relkey)? {
        Some(mut row) => {
            row.vv.merge(vv);
            DeletedRecord {
                vv: row.vv,
                server_ts: row.server_ts.max(server_ts),
            }
        }
        None => DeletedRecord {
            vv: vv.clone(),
            server_ts,
        },
    };
    txn.record_deleted(relkey, &merged)?;
    Ok(())
}

/// The §2.6 case-4 (ts, device) pick between a folded `put` and the local
/// head (same rule as the engine's `remote_wins_identity`): an
/// identity-less head always loses to the arriving entry.
fn fold_put_wins(entry: &JournalEntry, local: &ItemRecord) -> bool {
    match (local.head_ts, &local.device) {
        (Some(ts), Some(device)) => {
            let winner = pick_winner(
                Candidate {
                    ts: entry.ts,
                    device: &entry.device,
                },
                Candidate { ts, device },
            );
            winner.ts == entry.ts && *winner.device == entry.device
        }
        _ => true,
    }
}

/// Builds the runner's item record for a folded `put` entry (a `Synced`
/// snapshot of the advertised version — the manifest row's source).
fn fold_item_record(entry: &JournalEntry) -> ItemRecord {
    ItemRecord {
        kind: entry.kind,
        state: ItemState::Synced,
        size: entry.size.unwrap_or(0),
        mtime_unix_ns: entry.mtime.unwrap_or(0).saturating_mul(1_000_000_000),
        blake3: entry.blake3.clone(),
        sem_hash: entry.sem_hash.clone(),
        vv: entry.vv.clone(),
        content_id: entry.content_id.clone(),
        w: entry.w,
        h: entry.h,
        pinned: false,
        last_access_unix: 0,
        verified_remote: false,
        attested: false,
        base_unknown: false,
        rating: entry.rating,
        color_label: entry.color_label.clone(),
        device: Some(entry.device.clone()),
        head_ts: Some(entry.ts),
        admitted_vv: None,
        admitted_ts: None,
        deleted: false,
    }
}
