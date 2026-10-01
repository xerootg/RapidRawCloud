//! Shared test support for the sync-engine suites (`tests/engine.rs`,
//! `tests/engine_scenarios.rs`): a recording [`EngineEvents`] sink,
//! sidecar document builders (semantic edits vs. §2.5 churn rewrites),
//! the cross-device state-equivalence projection, journal decoding from
//! the bucket, and the manual drive helpers (admit → pump → publish,
//! poll-apply) the scenario suite composes.

use std::collections::BTreeMap;
use std::path::Path;

use rrcloud_core::clock::{DeviceId, VersionVector};
use rrcloud_core::engine::{
    admit_pending, ConflictEvent, EngineConsumer, EngineEvents, LoserPreservationSkippedEvent,
    OriginalConflictEvent, ResurrectionIncompleteEvent,
};
use rrcloud_core::journal::{decode_segment, JournalEntry, Kind};
use rrcloud_core::keys::{classify_key, KeyClass, RelKey, CONTROL_PREFIX, LIBRARY_PREFIX};
use rrcloud_core::publisher::publish_pending;
use rrcloud_core::reader::{poll, PollReport};
use rrcloud_core::s3::{ListObjectsV2Request, S3Client};
use rrcloud_core::semhash::{Blake3Hex, ContentId, SemHash};
use rrcloud_core::state::{ItemRecord, SyncDb};
use rrcloud_core::transfer::{pump_downloads, pump_uploads, CancelFlag, TransferConfig};

use super::transfer::CountingS3;

// ---------------------------------------------------------------------------
// Event recording
// ---------------------------------------------------------------------------

/// Records every engine event, in emission order.
#[derive(Default)]
pub struct RecordedEvents {
    pub conflicts: Vec<ConflictEvent>,
    pub resurrection_incomplete: Vec<ResurrectionIncompleteEvent>,
    pub original_conflicts: Vec<OriginalConflictEvent>,
    pub loser_skipped: Vec<LoserPreservationSkippedEvent>,
}

impl EngineEvents for RecordedEvents {
    fn conflict(&mut self, event: ConflictEvent) {
        self.conflicts.push(event);
    }

    fn resurrection_incomplete(&mut self, event: ResurrectionIncompleteEvent) {
        self.resurrection_incomplete.push(event);
    }

    fn original_conflict(&mut self, event: OriginalConflictEvent) {
        self.original_conflicts.push(event);
    }

    fn loser_preservation_skipped(&mut self, event: LoserPreservationSkippedEvent) {
        self.loser_skipped.push(event);
    }
}

// ---------------------------------------------------------------------------
// Sidecar documents
// ---------------------------------------------------------------------------

/// A realistic sidecar document: `rating`, a `color:<label>` tag (when
/// given), one adjustment, plus `exif`/`version` noise the §2.5
/// projection excludes.
pub fn doc(rating: u64, label: Option<&str>, exposure: f64) -> Vec<u8> {
    let tags: Vec<String> = label
        .map(|l| vec![format!("color:{l}")])
        .unwrap_or_default();
    serde_json::to_vec(&serde_json::json!({
        "version": 1,
        "rating": rating,
        "tags": tags,
        "adjustments": { "exposure": exposure, "contrast": 0 },
        "exif": { "camera": "TestCam", "iso": 100 },
    }))
    .expect("doc encodes")
}

/// The §2.6 canonical loser document the engine materializes as a vc
/// (the §2.5 semantic canonical form — churn-stable across holders).
pub fn semantic(bytes: &[u8]) -> Vec<u8> {
    rrcloud_core::semhash::semantic_document(bytes)
        .expect("semantic document")
        .into_bytes()
}

/// A §2.5 churn rewrite of `original`: same semantic content (rating,
/// tags, adjustments), different bytes — the `exif` cache is rewritten
/// and the whole document re-serialized pretty-printed. `sem_hash` of
/// the result equals the original's; the raw bytes do not.
pub fn churned(original: &[u8]) -> Vec<u8> {
    let mut value: serde_json::Value = serde_json::from_slice(original).expect("doc parses");
    value["exif"] = serde_json::json!({
        "camera": "TestCam",
        "iso": 100,
        "cached_at": 1_769_912_345,
        "lens": "50mm",
    });
    let mut pretty = serde_json::to_vec_pretty(&value).expect("doc re-encodes");
    pretty.push(b'\n');
    pretty
}

// ---------------------------------------------------------------------------
// State equivalence
// ---------------------------------------------------------------------------

/// The cross-device projection of one item record: everything two
/// converged devices must agree on exactly. Local-only facts (pipeline
/// state, pinning, access clock, verification flags) are deliberately
/// outside it; full-record equality is reserved for same-role
/// comparisons (e.g. one device applying the same entries in two
/// orders).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncView {
    pub kind: Kind,
    pub vv: VersionVector,
    pub blake3: Option<Blake3Hex>,
    pub sem_hash: Option<SemHash>,
    pub content_id: Option<ContentId>,
    pub rating: Option<u8>,
    pub color_label: Option<String>,
    pub device: Option<DeviceId>,
    pub head_ts: Option<i64>,
    pub size: u64,
    // §2.2 original-metadata fleet facts: the journal carries `w`/`h`
    // (final displayed dimensions, used for §4.4 proxy_scale), so two
    // converged devices must agree on them exactly — unlike the
    // local-only facts above the `deleted` line (review round 3, minor).
    // `mtime` is deliberately NOT here: originals carry it only at
    // whole-second precision on the wire while a holder keeps its local
    // file's nanosecond mtime, and sidecar puts carry none at all, so it
    // is a local fact, not a cross-device one.
    pub w: Option<u32>,
    pub h: Option<u32>,
    pub deleted: bool,
}

impl From<&ItemRecord> for SyncView {
    fn from(r: &ItemRecord) -> Self {
        SyncView {
            kind: r.kind,
            vv: r.vv.clone(),
            blake3: r.blake3.clone(),
            sem_hash: r.sem_hash.clone(),
            content_id: r.content_id.clone(),
            rating: r.rating,
            color_label: r.color_label.clone(),
            device: r.device.clone(),
            head_ts: r.head_ts,
            size: r.size,
            w: r.w,
            h: r.h,
            deleted: r.deleted,
        }
    }
}

/// The device's full item map projected to [`SyncView`], keyed by the
/// item relkey string.
pub fn sync_view(db: &SyncDb) -> BTreeMap<String, SyncView> {
    db.iter_items()
        .expect("iter_items")
        .into_iter()
        .map(|(k, r)| (k.as_str().to_string(), SyncView::from(&r)))
        .collect()
}

/// The device's full item map, keyed by relkey string — for exact
/// same-role equality (identical records, every field).
pub fn full_items(db: &SyncDb) -> BTreeMap<String, ItemRecord> {
    db.iter_items()
        .expect("iter_items")
        .into_iter()
        .map(|(k, r)| (k.as_str().to_string(), r))
        .collect()
}

// ---------------------------------------------------------------------------
// Bucket inspection
// ---------------------------------------------------------------------------

/// Every key currently in the bucket under `prefix`, sorted.
pub async fn keys_under(client: &S3Client, bucket: &str, prefix: &str) -> Vec<String> {
    let mut keys = Vec::new();
    let mut continuation_token: Option<String> = None;
    loop {
        let page = client
            .list_objects_v2(
                bucket,
                &ListObjectsV2Request {
                    prefix: Some(prefix.to_string()),
                    continuation_token: continuation_token.take(),
                    ..Default::default()
                },
            )
            .await
            .expect("list_objects_v2");
        keys.extend(page.objects.into_iter().map(|o| o.key));
        if !page.is_truncated {
            break;
        }
        continuation_token = page.next_continuation_token;
    }
    keys.sort();
    keys
}

/// Every key under `library/`.
pub async fn library_keys(client: &S3Client, bucket: &str) -> Vec<String> {
    keys_under(client, bucket, LIBRARY_PREFIX).await
}

/// Every published journal entry of `device`, decoded, ascending by seq.
pub async fn journal_entries_of(
    client: &S3Client,
    bucket: &str,
    device: &DeviceId,
) -> Vec<JournalEntry> {
    let keys = keys_under(client, bucket, &format!("{CONTROL_PREFIX}journal/")).await;
    let mut segs: Vec<(u64, String)> = keys
        .into_iter()
        .filter_map(|key| match classify_key(&key) {
            KeyClass::Journal { device: d, seq, .. } if d == *device => Some((seq, key)),
            _ => None,
        })
        .collect();
    segs.sort();
    let mut entries = Vec::new();
    for (_, key) in segs {
        let bytes = super::transfer::get_bytes(client, bucket, &key).await;
        entries.extend(decode_segment(&bytes).expect("segment decodes"));
    }
    entries
}

// ---------------------------------------------------------------------------
// Drive helpers (the scenario suite's manual publish/poll/pump)
// ---------------------------------------------------------------------------

/// Admits every quiesced-dirty item WITHOUT pumping or publishing —
/// the in-flight-intent window the review-round-0 scenarios hold open.
pub fn sync_up_admit_only(db: &SyncDb) -> Vec<RelKey> {
    admit_pending(db, |_, _| true).expect("admit_pending")
}

/// Admits every quiesced-dirty item, pumps the upload queue to
/// completion, and publishes the staged journal entries. Returns the
/// admitted item relkeys.
pub async fn sync_up(db: &SyncDb, s3: &CountingS3, cfg: &TransferConfig) -> Vec<RelKey> {
    let admitted = admit_pending(db, |_, _| true).expect("admit_pending");
    let summary = pump_uploads(db, s3, cfg, 2, &CancelFlag::new())
        .await
        .expect("pump_uploads");
    assert!(
        summary.failed.is_empty(),
        "sync_up: uploads failed: {:?}",
        summary.failed
    );
    publish_pending(db, s3, &cfg.bucket)
        .await
        .expect("publish_pending");
    admitted
}

/// One inbound poll through a fresh [`EngineConsumer`] recording into
/// `events`, with no byte transfers (pump separately).
pub async fn poll_apply(
    db: &SyncDb,
    s3: &CountingS3,
    cfg: &TransferConfig,
    events: &mut RecordedEvents,
) -> PollReport {
    let mut consumer =
        EngineConsumer::new(db, cfg.sync_root.clone(), events).expect("consumer construction");
    let report = poll(db, s3, &cfg.bucket, &mut consumer)
        .await
        .expect("poll");
    assert!(
        report.halted.is_empty()
            && report.mid_stream_gaps.is_empty()
            && report.corrupt.is_empty()
            && report.fetch_failed.is_empty(),
        "poll_apply: unexpected per-device outcomes: {report:?}"
    );
    report
}

/// Pumps the download queue to completion.
pub async fn pump_down(db: &SyncDb, s3: &CountingS3, cfg: &TransferConfig) {
    let summary = pump_downloads(db, s3, cfg, 2, &CancelFlag::new())
        .await
        .expect("pump_downloads");
    assert!(
        summary.failed.is_empty(),
        "pump_down: downloads failed: {:?}",
        summary.failed
    );
}

/// Reads a local file.
pub fn read_file(path: &Path) -> Vec<u8> {
    std::fs::read(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}
