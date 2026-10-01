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

use std::path::Path;

use crate::clock::{compare, DeviceId, VvOrder};
use crate::keys::RelKey;
use crate::manifest::ManifestError;
use crate::publisher::{DeviceEntry, PublisherError};
use crate::s3::{S3Error, S3TransferApi};
use crate::semhash::ContentId;
use crate::state::{StateError, SyncDb};

/// Seconds in a day (server-time arithmetic is all in unix seconds).
const DAY: i64 = 86_400;

/// Default: a device is **active** only if its last heartbeat is within
/// this window (§2.10). Past it, the device stops gating horizons even
/// without an explicit retirement.
pub const ACTIVE_WINDOW_SECS: i64 = 30 * DAY;

/// Default: a segment's covering manifest must have been durably present at
/// least this long — and be re-confirmed present — before any DELETE
/// (§2.10 segment-compaction rule 2, the 24 h insurance against a transient
/// PUT anomaly).
pub const COMPACTION_RECONFIRM_SECS: i64 = DAY;

/// Default: the 14-day pressure-valve cap (§2.10 rule 3 / D3). A laggard
/// that has not applied a segment still lets it be compacted once the
/// segment is older than this, because the laggard catches up losslessly by
/// manifest merge (rows carry vv + the deleted set).
pub const LAGGARD_CAP_SECS: i64 = 14 * DAY;

/// Default: the user-facing "Recently Deleted" grace window (§2.7/§2.10
/// (b)). A tombstone younger than this is never GC'd, even when every
/// active device has applied past it.
pub const RECENTLY_DELETED_GRACE_SECS: i64 = 30 * DAY;

/// Default: a device with no heartbeat for this long is auto-retired by the
/// GC sweep (§2.10 device lifecycle), server time.
pub const AUTO_RETIRE_SECS: i64 = 90 * DAY;

/// Default: the deleted-set retention window (§2.3): a GC runner's manifest
/// retains a destroyed tombstone's `del` row this long so a bootstrapping or
/// long-dormant device still learns the deletion after the tombstone object
/// is gone. Also the §2.3 pre-upload quarantine horizon: a device whose
/// deletion-knowledge predates this window cannot prove a local-only key was
/// not deleted.
pub const DELETED_SET_RETENTION_SECS: i64 = 365 * DAY;

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
    /// folded into its own manifest before destroying data keys (§2.10 (c)
    /// + the crash-safety ordering): destruction is refused so no key is
    /// ever destroyed without its deleted-set record already durable.
    #[error("deleted-set fold not verified before tombstone destruction: {detail}")]
    DeletedSetNotVerified {
        /// What the read-back saw.
        detail: String,
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
    let _ = (s3, bucket, clock, cfg);
    todo!("P1-U6 green: list devices/, GET entries, filter by retired-marker + 30d last_seen")
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
    let _ = (db, s3, bucket, clock, cfg);
    todo!("P1-U6 green: the 3-rule gate, read-back verify, own-prefix DELETE, idempotent summary")
}

// ---------------------------------------------------------------------------
// Tombstone GC (worker / backfill role, §2.10)
// ---------------------------------------------------------------------------

/// Why a tombstone was retained (not GC'd) this pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GcSkipReason {
    /// (b) Younger than the 30-day Recently-Deleted grace window.
    WithinGrace {
        /// Its server-time age in seconds.
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
///   ([`GcSkipReason::WithinGrace`] otherwise).
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
/// no live relkey and no in-grace tombstone references that `content_id`**
/// (content-id liveness: two images sharing byte-identical originals keep
/// the shared preview/thumb until *both* are gone). The order is strict and
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
    let _ = (db, s3, bucket, clock, cfg);
    todo!("P1-U6 green: the 4-condition gate, fold-before-destroy ordering, content-id liveness")
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
    let _ = (s3, bucket, device);
    todo!("P1-U6 green: PUT devices/<id>.retired (idempotent)")
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
    let _ = (s3, bucket, clock, cfg);
    todo!("P1-U6 green: 90-day server-time auto-retire, idempotent, first-heartbeat-safe")
}

/// §2.10 orphaned-prefix GC: the journal segments of **retired** devices,
/// folded into the GC **runner's** manifest and then deleted under the same
/// rules as the runner's own prefix (coverage + 24 h re-confirm + horizon or
/// 14-day cap). Returns one [`CompactionSummary`] per retired device whose
/// prefix was touched, keyed by that device id. Idempotent.
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
    let _ = (db, s3, bucket, clock, cfg);
    todo!("P1-U6 green: fold retired prefixes into runner manifest, delete under compaction rules")
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
    let _ = (db, clock, cfg);
    todo!("P1-U6 green: compare applied-proof horizon to retention, collect base_unknown items")
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
    let _ = (db, s3, bucket, sync_root, relkey, resolution);
    todo!("P1-U6 green: Keep -> gated re-advertise put; Discard -> local delete + record drop")
}

/// Crate-internal: does `put` (vv `pv`) dominate a tombstone/del at vv `dv`?
/// A resurrecting put is one that strictly dominates the deletion's vv
/// (§2.6/§2.7). Exposed to the GC superseded-check; a thin wrapper over
/// [`crate::clock::compare`] so the one spelling of "resurrects" lives here.
#[allow(dead_code)]
pub(crate) fn put_supersedes_del(
    pv: &crate::clock::VersionVector,
    dv: &crate::clock::VersionVector,
) -> bool {
    matches!(compare(pv, dv), VvOrder::Greater)
}
