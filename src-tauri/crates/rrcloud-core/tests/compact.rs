//! Failing tests for `rrcloud_core::compact` (architecture §2.10, with
//! §2.3/§2.1/§1.2 as the surrounding contract): the server-time clock and
//! its skew immunity, active-set computation and horizons, own-prefix
//! segment compaction (3-rule gate, read-back verify, safety), tombstone GC
//! (4-condition gate, fold-before-destroy ordering, content-id liveness,
//! resurrection guard, grace window), device retirement + auto-retire +
//! orphaned-prefix GC, and the §2.3 pre-upload quarantine decision +
//! resolution.
//!
//! All age thresholds are exercised by **planting server-time offsets and
//! historical timestamps** (manifest `written_server_ts`, tombstone
//! `server_ts`, device `last_seen_server_ts`, per-segment publish stamps),
//! never by sleeping, and every decision is pinned against a [`ServerClock`]
//! whose "now" is fixed — so the assertions pin the *decision*, not wall
//! behavior.

mod common;

use bytes::Bytes;
use common::garage;
use common::sync::{dev, open_db, rel, DEV_A, DEV_B, DEV_C, DEV_X};

use rrcloud_core::clock::{DeviceId, VersionVector};
use rrcloud_core::compact::{
    active_devices, auto_retire_sweep, compact_own_segments, detect_local_only_unprovable,
    gc_retired_prefixes, horizon_applied, record_server_time, resolve_quarantine, retire_device,
    tombstone_gc, ActiveDevice, CompactConfig, GcSkipReason, QuarantineDecision, QuarantineOutcome,
    QuarantineResolution, ServerClock, SkipReason, ACTIVE_WINDOW_SECS, AUTO_RETIRE_SECS,
    DELETED_SET_RETENTION_SECS, LAGGARD_CAP_SECS, RECENTLY_DELETED_GRACE_SECS,
};
use rrcloud_core::journal::{Kind, Op, Tombstone};
use rrcloud_core::keys::{
    device_registry_key, device_retired_key, journal_segment_key, library_key, preview_key,
    sidecar_key, thumb_key, tombstone_key, RelKey, ThumbSize,
};
use rrcloud_core::manifest::{build_manifest, get_manifest, merge, put_manifest};
use rrcloud_core::publisher::{
    enqueue_entry, publish_pending, DeviceEntry, ProtoSupport, PROTO_READ, PROTO_WRITE,
};
use rrcloud_core::s3::{
    ByteRange, CompleteMultipartUploadOutput, CompletedPart, CreateMultipartUploadOutput,
    GetObjectOutput, HeadObjectOutput, ListMultipartUploadsOutput, ListMultipartUploadsRequest,
    ListObjectsV2Output, ListObjectsV2Request, ListPartsOutput, ListPartsRequest, PartBody,
    PutObjectOptions, PutObjectOutput, S3Api, S3Client, S3Error, S3TransferApi,
};
use rrcloud_core::state::{DeletedRecord, ItemRecord, ItemState, SyncDb};

use std::collections::HashSet;

// A fixed "server now" (2026-02-01T00:00:00Z-ish) so every planted history
// is deterministic relative to it.
const NOW: i64 = 1_769_904_000;

const BLAKE3_HEX: &str = "4878ca0425c739fa427f7eda20fe845f6b2e46ba5fe2a14df5b1e32f50603215";

// ---------------------------------------------------------------------------
// Builders
// ---------------------------------------------------------------------------

fn vv(pairs: &[(&DeviceId, u32)]) -> VersionVector {
    pairs.iter().map(|(d, c)| ((*d).clone(), *c)).collect()
}

fn device_entry(created: i64, last_seen: i64, applied: &[(&DeviceId, u64)]) -> DeviceEntry {
    DeviceEntry {
        name: "dev".to_string(),
        platform: "linux".to_string(),
        created,
        last_seen_server_ts: last_seen,
        applied: applied.iter().map(|(d, s)| ((*d).clone(), *s)).collect(),
        proto: ProtoSupport {
            read: PROTO_READ.to_vec(),
            write: PROTO_WRITE,
        },
    }
}

fn synced_sidecar(vv_: VersionVector) -> ItemRecord {
    ItemRecord {
        kind: Kind::Sidecar,
        state: ItemState::Synced,
        size: 64,
        mtime_unix_ns: 0,
        blake3: Some(rrcloud_core::semhash::Blake3Hex::parse(BLAKE3_HEX).unwrap()),
        sem_hash: None,
        vv: vv_,
        content_id: None,
        w: None,
        h: None,
        pinned: false,
        last_access_unix: 0,
        verified_remote: true,
        attested: false,
        base_unknown: false,
        rating: None,
        color_label: None,
        device: None,
        head_ts: None,
        admitted_vv: None,
        admitted_ts: None,
        deleted: false,
    }
}

fn original_item(
    content: &rrcloud_core::semhash::ContentId,
    vv_: VersionVector,
    deleted: bool,
) -> ItemRecord {
    ItemRecord {
        kind: Kind::Original,
        state: ItemState::Synced,
        size: 1024,
        mtime_unix_ns: 0,
        blake3: Some(rrcloud_core::semhash::Blake3Hex::parse(BLAKE3_HEX).unwrap()),
        sem_hash: None,
        vv: vv_,
        content_id: Some(content.clone()),
        w: Some(100),
        h: Some(100),
        pinned: false,
        last_access_unix: 0,
        verified_remote: true,
        attested: false,
        base_unknown: false,
        rating: None,
        color_label: None,
        device: None,
        head_ts: None,
        admitted_vv: None,
        admitted_ts: None,
        deleted,
    }
}

// ---------------------------------------------------------------------------
// Raw S3 helpers
// ---------------------------------------------------------------------------

async fn put_device(client: &S3Client, bucket: &str, device: &DeviceId, entry: &DeviceEntry) {
    let body = serde_json::to_vec(entry).expect("encode device entry");
    client
        .put_object(
            bucket,
            &device_registry_key(device),
            Bytes::from(body),
            &PutObjectOptions::default(),
        )
        .await
        .expect("put device entry");
}

async fn put_retired(client: &S3Client, bucket: &str, device: &DeviceId) {
    client
        .put_object(
            bucket,
            &device_retired_key(device),
            Bytes::from_static(b""),
            &PutObjectOptions::default(),
        )
        .await
        .expect("put retired marker");
}

async fn put_object_raw(client: &S3Client, bucket: &str, key: &str, body: &[u8]) {
    client
        .put_object(
            bucket,
            key,
            Bytes::copy_from_slice(body),
            &PutObjectOptions::default(),
        )
        .await
        .expect("put object");
}

async fn exists(client: &S3Client, bucket: &str, key: &str) -> bool {
    client.head_object(bucket, key).await.is_ok()
}

/// Publishes `n` single-entry own sidecar segments (seqs 1..=n), stamping
/// each with a planted server-time publish instant so the 14-day cap is
/// exercised deterministically. Returns the segments' first-seqs.
async fn publish_segments(
    db: &SyncDb,
    client: &S3Client,
    bucket: &str,
    n: usize,
    seg_server_ts: i64,
) -> Vec<u64> {
    let mut segs = Vec::new();
    for i in 0..n {
        let e = common::sync::entry(
            db.device_id(),
            Op::Put,
            Kind::Sidecar,
            sidecar_key(&rel(&format!("img/i{i:04}.NEF"))),
        );
        enqueue_entry(db, &e).expect("enqueue");
        let report = publish_pending(db, client, bucket).await.expect("publish");
        for first in report.segments {
            db.set_segment_published_server_ts(first, seg_server_ts)
                .expect("stamp seg ts");
            segs.push(first);
        }
    }
    segs
}

// ---------------------------------------------------------------------------
// A fault-injecting S3 wrapper (fail selected HEAD / DELETE / PUT keys)
// ---------------------------------------------------------------------------

struct FaultS3 {
    inner: S3Client,
    fail_heads: HashSet<String>,
    fail_deletes: HashSet<String>,
    fail_puts: HashSet<String>,
}

impl FaultS3 {
    fn new(inner: S3Client) -> Self {
        Self {
            inner,
            fail_heads: HashSet::new(),
            fail_deletes: HashSet::new(),
            fail_puts: HashSet::new(),
        }
    }
}

impl S3Api for FaultS3 {
    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: Bytes,
        opts: &PutObjectOptions,
    ) -> Result<PutObjectOutput, S3Error> {
        if self.fail_puts.contains(key) {
            return Err(S3Error::InvalidRequest(format!(
                "injected PUT failure {key}"
            )));
        }
        self.inner.put_object(bucket, key, body, opts).await
    }

    async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<GetObjectOutput, S3Error> {
        self.inner.get_object(bucket, key, range).await
    }

    async fn head_object(&self, bucket: &str, key: &str) -> Result<HeadObjectOutput, S3Error> {
        if self.fail_heads.contains(key) {
            return Err(S3Error::InvalidRequest(format!(
                "injected HEAD failure {key}"
            )));
        }
        self.inner.head_object(bucket, key).await
    }

    async fn list_objects_v2(
        &self,
        bucket: &str,
        request: &ListObjectsV2Request,
    ) -> Result<ListObjectsV2Output, S3Error> {
        self.inner.list_objects_v2(bucket, request).await
    }
}

impl S3TransferApi for FaultS3 {
    async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), S3Error> {
        if self.fail_deletes.contains(key) {
            return Err(S3Error::InvalidRequest(format!(
                "injected DELETE failure {key}"
            )));
        }
        self.inner.delete_object(bucket, key).await
    }

    async fn create_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        opts: &PutObjectOptions,
    ) -> Result<CreateMultipartUploadOutput, S3Error> {
        self.inner.create_multipart_upload(bucket, key, opts).await
    }

    async fn upload_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: u32,
        body: PartBody,
        content_md5: Option<&str>,
    ) -> Result<rrcloud_core::s3::UploadPartOutput, S3Error> {
        self.inner
            .upload_part(bucket, key, upload_id, part_number, body, content_md5)
            .await
    }

    async fn complete_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: &[CompletedPart],
    ) -> Result<CompleteMultipartUploadOutput, S3Error> {
        self.inner
            .complete_multipart_upload(bucket, key, upload_id, parts)
            .await
    }

    async fn abort_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<(), S3Error> {
        self.inner
            .abort_multipart_upload(bucket, key, upload_id)
            .await
    }

    async fn list_multipart_uploads(
        &self,
        bucket: &str,
        request: &ListMultipartUploadsRequest,
    ) -> Result<ListMultipartUploadsOutput, S3Error> {
        self.inner.list_multipart_uploads(bucket, request).await
    }

    async fn list_parts(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        request: &ListPartsRequest,
    ) -> Result<ListPartsOutput, S3Error> {
        self.inner.list_parts(bucket, key, upload_id, request).await
    }
}

// ===========================================================================
// Server-time clock (§2.10) — pure, no network
// ===========================================================================

#[test]
fn server_clock_now_is_local_plus_offset() {
    let clock = ServerClock::with_offset_secs(500);
    assert_eq!(clock.offset_secs(), 500);
    assert_eq!(clock.server_at(1_000), 1_500);
    assert_eq!(clock.server_at(-200), 300);
}

#[test]
fn server_clock_pinned_reports_the_fixed_instant() {
    let clock = ServerClock::pinned(NOW);
    assert_eq!(clock.now_server(), NOW);
}

#[test]
fn server_clock_is_skew_immune() {
    // Two devices observe the SAME server Date; one local clock is +2h, the
    // other -2h. Each offset absorbs its own skew, so now_server() is the
    // identical server instant on both — the §2.10/B8 invariant.
    let server = NOW;
    let fast = ServerClock::observe(server + 2 * 3600, server); // local runs 2h fast
    let slow = ServerClock::observe(server - 2 * 3600, server); // local runs 2h slow

    assert_eq!(fast.offset_secs(), -2 * 3600);
    assert_eq!(slow.offset_secs(), 2 * 3600);
    assert_eq!(fast.now_server(), server);
    assert_eq!(slow.now_server(), server);
    assert_eq!(fast.now_server(), slow.now_server());
}

#[test]
fn record_server_time_records_an_offset_from_a_date_header() {
    let (_dir, _path, db) = open_db(&dev(DEV_A));
    // Absent / unparsable headers record nothing and report false.
    assert!(!record_server_time(&db, None).unwrap());
    assert!(!record_server_time(&db, Some("not a date")).unwrap());
    assert!(db.server_time_offset_ms().unwrap().is_none());

    // A real IMF-fixdate records an offset; from_db then reads it back.
    assert!(record_server_time(&db, Some("Sun, 01 Feb 2026 00:00:00 GMT")).unwrap());
    let offset = db
        .server_time_offset_ms()
        .unwrap()
        .expect("offset recorded");
    let clock = ServerClock::from_db(&db).unwrap();
    assert_eq!(clock.offset_secs(), offset.div_euclid(1000));
}

// ===========================================================================
// Active set + horizons (§2.10) — pure
// ===========================================================================

#[test]
fn horizon_is_min_over_other_active_devices_and_excludes_self() {
    let a = dev(DEV_A);
    let b = dev(DEV_B);
    let c = dev(DEV_C);
    let active = vec![
        ActiveDevice {
            device: b.clone(),
            entry: device_entry(0, NOW, &[(&a, 7)]),
        },
        ActiveDevice {
            device: c.clone(),
            entry: device_entry(0, NOW, &[(&a, 4)]),
        },
        // A's own entry must never gate A's prefix (peers-only applied -> 0).
        ActiveDevice {
            device: a.clone(),
            entry: device_entry(0, NOW, &[]),
        },
    ];
    assert_eq!(horizon_applied(&active, &a), 4);

    // No other active device -> unbounded (fast path unconstrained).
    let only_self = vec![ActiveDevice {
        device: a.clone(),
        entry: device_entry(0, NOW, &[]),
    }];
    assert_eq!(horizon_applied(&only_self, &a), u64::MAX);
}

#[tokio::test]
async fn active_set_excludes_retired_and_stale_devices() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("cmp-active");
    let client = g.client();
    let (a, b, c) = (dev(DEV_A), dev(DEV_B), dev(DEV_C));

    // B recent, C stale (>30d), A recent but retired.
    put_device(&client, &bucket, &a, &device_entry(0, NOW - 60, &[])).await;
    put_device(&client, &bucket, &b, &device_entry(0, NOW - 60, &[])).await;
    put_device(
        &client,
        &bucket,
        &c,
        &device_entry(0, NOW - (ACTIVE_WINDOW_SECS + 60), &[]),
    )
    .await;
    put_retired(&client, &bucket, &a).await;

    let clock = ServerClock::pinned(NOW);
    let active = active_devices(&client, &bucket, &clock, &CompactConfig::default())
        .await
        .expect("active devices");
    let ids: Vec<DeviceId> = active.iter().map(|ad| ad.device.clone()).collect();
    assert_eq!(ids, vec![b.clone()], "only the recent, non-retired device");
}

// ===========================================================================
// Segment compaction (own prefix, §2.10)
// ===========================================================================

#[tokio::test]
async fn compaction_fast_path_deletes_covered_aged_segments() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("cmp-fast");
    let client = g.client();
    let (a, b, c) = (dev(DEV_A), dev(DEV_B), dev(DEV_C));
    let (_dir, _path, db) = open_db(&a);

    // A publishes 3 own segments (seqs 1,2,3), published recently.
    let segs = publish_segments(&db, &client, &bucket, 3, NOW - 3600).await;
    assert_eq!(segs, vec![1, 2, 3]);

    // A's own manifest covers published cursor (3) and is 25h old (rule 2
    // satisfied); planted by writing it with an aged written_server_ts.
    let manifest = build_manifest(&db, NOW - 25 * 3600).expect("build manifest");
    assert_eq!(manifest.header.cursors.get(&a), Some(&3));
    put_manifest(&client, &bucket, &a, &manifest)
        .await
        .expect("plant manifest");

    // Two active devices, both applied past seq 3 (fast path).
    put_device(&client, &bucket, &b, &device_entry(0, NOW - 60, &[(&a, 3)])).await;
    put_device(&client, &bucket, &c, &device_entry(0, NOW - 60, &[(&a, 3)])).await;

    let clock = ServerClock::pinned(NOW);
    let summary = compact_own_segments(&db, &client, &bucket, &clock, &CompactConfig::default())
        .await
        .expect("compact");

    let mut deleted = summary.deleted_seqs.clone();
    deleted.sort_unstable();
    assert_eq!(
        deleted,
        vec![1, 2, 3],
        "all covered+aged+horizon-cleared segments"
    );
    for seq in [1u64, 2, 3] {
        assert!(
            !exists(&client, &bucket, &journal_segment_key(&a, seq)).await,
            "segment {seq} object must be gone"
        );
    }

    // Idempotent: a re-run deletes nothing new.
    let again = compact_own_segments(&db, &client, &bucket, &clock, &CompactConfig::default())
        .await
        .expect("compact again");
    assert!(again.deleted_seqs.is_empty(), "re-run is a no-op");
}

#[tokio::test]
async fn compaction_laggard_caught_up_reader_learns_via_manifest_merge() {
    // Fast-path compaction ran; a reader that never applied A's journal
    // bootstraps from A's manifest and reaches A's state including a
    // deletion that A had folded into the manifest's deleted set.
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("cmp-reader");
    let client = g.client();
    let (a, b, c) = (dev(DEV_A), dev(DEV_B), dev(DEV_C));
    let (_dir, _path, db) = open_db(&a);

    let live = rel("img/live.NEF");
    let gone = rel("img/gone.NEF");
    db.replay_put_item(&live, &synced_sidecar(vv(&[(&a, 1)])))
        .expect("live item");
    db.record_deleted(
        &gone,
        &DeletedRecord {
            vv: vv(&[(&a, 2)]),
            server_ts: NOW - 26 * 3600,
        },
    )
    .expect("deleted row");

    let segs = publish_segments(&db, &client, &bucket, 2, NOW - 3600).await;
    let manifest = build_manifest(&db, NOW - 25 * 3600).expect("manifest");
    put_manifest(&client, &bucket, &a, &manifest)
        .await
        .expect("plant");
    put_device(&client, &bucket, &b, &device_entry(0, NOW - 60, &[(&a, 2)])).await;
    put_device(&client, &bucket, &c, &device_entry(0, NOW - 60, &[(&a, 2)])).await;

    let clock = ServerClock::pinned(NOW);
    let summary = compact_own_segments(&db, &client, &bucket, &clock, &CompactConfig::default())
        .await
        .expect("compact");
    assert_eq!(summary.deleted_seqs, segs);

    // Reader R never applied A's prefix: those segments are gone, so R must
    // learn the whole state (live item + the deletion) from A's manifest.
    let (_rdir, _rpath, rdb) = open_db(&b);
    let a_manifest = get_manifest(&client, &bucket, &a)
        .await
        .expect("get A manifest");
    let mut consumer = common::sync::ReplayConsumer;
    merge(&[(a.clone(), a_manifest)], &rdb, &mut consumer).expect("merge");
    assert!(
        rdb.get_item(&live).expect("live").is_some(),
        "learned the live item"
    );
    assert!(
        rdb.get_deleted(&gone).expect("deleted").is_some(),
        "learned the deletion from the deleted set, not the journal"
    );
}

#[tokio::test]
async fn compaction_14_day_cap_deletes_despite_a_laggard() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("cmp-cap");
    let client = g.client();
    let (a, b) = (dev(DEV_A), dev(DEV_B));
    let (_dir, _path, db) = open_db(&a);

    // Segments published 15 days ago (older than the 14-day cap).
    let old_ts = NOW - 15 * 86_400;
    let segs = publish_segments(&db, &client, &bucket, 2, old_ts).await;
    let manifest = build_manifest(&db, NOW - 25 * 3600).expect("manifest");
    put_manifest(&client, &bucket, &a, &manifest)
        .await
        .expect("plant");

    // B is a laggard: applied[A] = 0 (far behind the published cursor).
    put_device(&client, &bucket, &b, &device_entry(0, NOW - 60, &[(&a, 0)])).await;

    let clock = ServerClock::pinned(NOW);
    const {
        assert!(
            LAGGARD_CAP_SECS < 15 * 86_400,
            "the planted age is past the cap"
        )
    };
    let summary = compact_own_segments(&db, &client, &bucket, &clock, &CompactConfig::default())
        .await
        .expect("compact");
    let mut deleted = summary.deleted_seqs.clone();
    deleted.sort_unstable();
    assert_eq!(deleted, segs, "cap fires even though the laggard is behind");
}

#[tokio::test]
async fn compaction_skips_segment_not_covered_by_manifest() {
    // §2.10 safety: a segment whose entries the db has NOT folded
    // (published_cursor < max_seq, so build_manifest cannot prove coverage)
    // is NEVER deleted. The covering manifest's own cursor rides the
    // published cursor (review round 0: the present-branch rebuilds the
    // manifest to track the published cursor, so the genuine under-coverage
    // case is published < max_seq — not a manifest artificially frozen below
    // what the db has already folded).
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("cmp-safety");
    let client = g.client();
    let (a, b, c) = (dev(DEV_A), dev(DEV_B), dev(DEV_C));
    let (_dir, _path, db) = open_db(&a);

    // Two segments are genuinely published+folded (published_cursor = 2);
    // a third segment OBJECT exists on the wire but its entries were never
    // folded (published_cursor stays 2), so the manifest cannot cover it.
    let segs = publish_segments(&db, &client, &bucket, 2, NOW - 3600).await;
    assert_eq!(segs, vec![1, 2]);
    let orphan_seg3 = {
        let mut e =
            common::sync::entry(&a, Op::Put, Kind::Sidecar, sidecar_key(&rel("img/s3.NEF")));
        e.seq = 3;
        e
    };
    common::sync::put_raw_segment(&client, &bucket, &a, 3, &[orphan_seg3]).await;

    let manifest = build_manifest(&db, NOW - 25 * 3600).expect("manifest");
    assert_eq!(
        manifest.header.cursors.get(&a),
        Some(&2),
        "manifest covers only the published cursor (2)"
    );
    put_manifest(&client, &bucket, &a, &manifest)
        .await
        .expect("plant");
    put_device(&client, &bucket, &b, &device_entry(0, NOW - 60, &[(&a, 3)])).await;
    put_device(&client, &bucket, &c, &device_entry(0, NOW - 60, &[(&a, 3)])).await;

    let clock = ServerClock::pinned(NOW);
    let summary = compact_own_segments(&db, &client, &bucket, &clock, &CompactConfig::default())
        .await
        .expect("compact");

    assert!(
        !summary.deleted_seqs.contains(&3),
        "segment 3 (entries not folded into the manifest) must survive"
    );
    assert!(
        exists(&client, &bucket, &journal_segment_key(&a, 3)).await,
        "segment 3 object still present"
    );
    let skipped3 = summary
        .skipped
        .iter()
        .find(|s| s.first_seq == 3)
        .expect("segment 3 skipped with a reason");
    assert!(
        matches!(
            skipped3.reason,
            SkipReason::ManifestCoverageBelow { cursor: 2 }
        ),
        "exact skip reason, got {:?}",
        skipped3.reason
    );
    // The covered+folded segments still compact normally.
    let mut deleted = summary.deleted_seqs.clone();
    deleted.sort_unstable();
    assert_eq!(deleted, vec![1, 2], "the folded segments compact");
}

#[tokio::test]
async fn compaction_advances_manifest_coverage_for_later_segments() {
    // Review round 0 major: once a manifest exists, segments published AFTER
    // it used to be wedged at ManifestCoverageBelow forever (the frozen
    // cursor was never refreshed) -> unbounded journal growth. The present
    // branch must rebuild the manifest to cover the published cursor.
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("cmp-advance");
    let client = g.client();
    let (a, b, c) = (dev(DEV_A), dev(DEV_B), dev(DEV_C));
    let (_dir, _path, db) = open_db(&a);

    // First manifest covers cursor 2 (aged 25h).
    publish_segments(&db, &client, &bucket, 2, NOW - 3600).await;
    let first = build_manifest(&db, NOW - 25 * 3600).expect("manifest");
    assert_eq!(first.header.cursors.get(&a), Some(&2));
    put_manifest(&client, &bucket, &a, &first)
        .await
        .expect("plant first manifest");

    // Two more segments (3,4) published and folded; the on-wire manifest
    // still says cursor 2.
    publish_segments(&db, &client, &bucket, 2, NOW - 3600).await;
    assert_eq!(db.published_cursor().unwrap(), 4);

    // Both active peers applied past 4 (fast path clears once covered).
    put_device(&client, &bucket, &b, &device_entry(0, NOW - 60, &[(&a, 4)])).await;
    put_device(&client, &bucket, &c, &device_entry(0, NOW - 60, &[(&a, 4)])).await;

    // Pass 1: coverage advances. The rebuilt manifest is fresh, so rule 2
    // holds every segment back this pass; no segment is stuck below coverage.
    let clock = ServerClock::pinned(NOW);
    let pass1 = compact_own_segments(&db, &client, &bucket, &clock, &CompactConfig::default())
        .await
        .expect("compact pass 1");
    assert!(
        !pass1
            .skipped
            .iter()
            .any(|s| matches!(s.reason, SkipReason::ManifestCoverageBelow { .. })),
        "no segment is wedged below coverage after the rebuild, got {:?}",
        pass1.skipped
    );
    let on_wire = get_manifest(&client, &bucket, &a)
        .await
        .expect("manifest after advance");
    assert_eq!(
        on_wire.header.cursors.get(&a),
        Some(&4),
        "on-wire manifest coverage advanced to the published cursor"
    );

    // Pass 2, ≥24h later: the now-aged manifest lets the newly-covered
    // segments compact (the growth bound is honored, not stalled).
    let later = ServerClock::pinned(NOW + 25 * 3600);
    let pass2 = compact_own_segments(&db, &client, &bucket, &later, &CompactConfig::default())
        .await
        .expect("compact pass 2");
    let mut deleted = pass2.deleted_seqs.clone();
    deleted.sort_unstable();
    assert_eq!(
        deleted,
        vec![1, 2, 3, 4],
        "every covered+aged segment compacts once coverage advanced"
    );
}

#[tokio::test]
async fn compaction_aborts_all_deletes_when_manifest_readback_fails() {
    // A failed/not-present manifest read-back aborts ALL deletes this pass.
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("cmp-readback");
    let client = g.client();
    let (a, b, c) = (dev(DEV_A), dev(DEV_B), dev(DEV_C));
    let (_dir, _path, db) = open_db(&a);

    let segs = publish_segments(&db, &client, &bucket, 2, NOW - 3600).await;
    let manifest = build_manifest(&db, NOW - 25 * 3600).expect("manifest");
    put_manifest(&client, &bucket, &a, &manifest)
        .await
        .expect("plant");
    put_device(&client, &bucket, &b, &device_entry(0, NOW - 60, &[(&a, 2)])).await;
    put_device(&client, &bucket, &c, &device_entry(0, NOW - 60, &[(&a, 2)])).await;

    // Fault: the read-back HEAD of A's own manifest fails.
    let mut fault = FaultS3::new(g.client());
    fault
        .fail_heads
        .insert(rrcloud_core::keys::manifest_key(&a));

    let clock = ServerClock::pinned(NOW);
    let result =
        compact_own_segments(&db, &fault, &bucket, &clock, &CompactConfig::default()).await;
    assert!(
        matches!(
            result,
            Err(rrcloud_core::compact::CompactError::ManifestNotVerified { .. })
        ),
        "a failed read-back is ManifestNotVerified, got {result:?}"
    );
    for seq in segs {
        assert!(
            exists(&client, &bucket, &journal_segment_key(&a, seq)).await,
            "no segment deleted when read-back failed"
        );
    }
}

#[tokio::test]
async fn compaction_decisions_are_skew_immune() {
    // Identical planted state in two buckets; two clocks built from +2h and
    // -2h local clocks observing the same server Date. The compaction
    // decision (which seqs delete) must be identical.
    let Some(g) = garage::shared() else { return };
    let client = g.client();
    let (a, b, c) = (dev(DEV_A), dev(DEV_B), dev(DEV_C));

    async fn run(
        g: &garage::Garage,
        client: &S3Client,
        a: &DeviceId,
        b: &DeviceId,
        c: &DeviceId,
        clock: &ServerClock,
        tag: &str,
    ) -> Vec<u64> {
        let bucket = g.create_unique_bucket(tag);
        let (_dir, _path, db) = open_db(a);
        publish_segments(&db, client, &bucket, 3, NOW - 3600).await;
        let manifest = build_manifest(&db, NOW - 25 * 3600).expect("manifest");
        put_manifest(client, &bucket, a, &manifest)
            .await
            .expect("plant");
        put_device(client, &bucket, b, &device_entry(0, NOW - 60, &[(a, 3)])).await;
        put_device(client, &bucket, c, &device_entry(0, NOW - 60, &[(a, 3)])).await;
        let mut s = compact_own_segments(&db, client, &bucket, clock, &CompactConfig::default())
            .await
            .expect("compact")
            .deleted_seqs;
        s.sort_unstable();
        s
    }

    let fast = ServerClock::observe(NOW + 2 * 3600, NOW);
    let slow = ServerClock::observe(NOW - 2 * 3600, NOW);
    let d_fast = run(g, &client, &a, &b, &c, &fast, "cmp-skew-fast").await;
    let d_slow = run(g, &client, &a, &b, &c, &slow, "cmp-skew-slow").await;
    assert_eq!(
        d_fast, d_slow,
        "skewed clocks reach identical compaction decisions"
    );
    assert_eq!(d_fast, vec![1, 2, 3]);
}

// ===========================================================================
// Tombstone GC (§2.10)
// ===========================================================================

/// Plants a tombstone object + the GC runner's view of a deleted image: the
/// deleted-set record, the deleted item record(s), and the data keys on S3.
async fn plant_deleted_image(
    db: &SyncDb,
    client: &S3Client,
    bucket: &str,
    image: &RelKey,
    content: &rrcloud_core::semhash::ContentId,
    del_vv: VersionVector,
    server_ts: i64,
) {
    // Data keys present in the bucket.
    put_object_raw(client, bucket, &library_key(image), b"original-bytes").await;
    put_object_raw(client, bucket, &sidecar_key(image), b"{\"sidecar\":true}").await;
    put_object_raw(client, bucket, &preview_key(content), b"preview").await;
    put_object_raw(
        client,
        bucket,
        &thumb_key(content, ThumbSize::Small),
        b"thumb",
    )
    .await;

    // Tombstone object.
    let tomb = Tombstone {
        relkey: image.clone(),
        vv: del_vv.clone(),
        device: db.device_id().clone(),
        server_ts,
        kinds: vec![Kind::Original, Kind::Sidecar],
    };
    put_object_raw(
        client,
        bucket,
        &tombstone_key(image),
        &serde_json::to_vec(&tomb).unwrap(),
    )
    .await;

    // Runner's local view: a soft-deleted item + the deleted-set row.
    let mut rec = original_item(content, del_vv.clone(), true);
    rec.state = ItemState::Synced;
    db.replay_put_item(image, &rec).expect("deleted item");
    db.record_deleted(
        image,
        &DeletedRecord {
            vv: del_vv,
            server_ts,
        },
    )
    .expect("deleted row");
}

#[tokio::test]
async fn tombstone_gc_happy_path_destroys_and_bootstrapper_learns_from_deleted_set() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("gc-happy");
    let client = g.client();
    let (a, b) = (dev(DEV_A), dev(DEV_B));
    let (_dir, _path, db) = open_db(&a);

    let image = rel("img/old.NEF");
    let content = rrcloud_core::semhash::ContentId::from_bytes(b"old-image-bytes");
    // 31 days old: past the 30-day grace (and so past the 14-day cap).
    plant_deleted_image(
        &db,
        &client,
        &bucket,
        &image,
        &content,
        vv(&[(&a, 5)]),
        NOW - 31 * 86_400,
    )
    .await;

    // One active device that applied past the deletion.
    put_device(&client, &bucket, &b, &device_entry(0, NOW - 60, &[(&a, 9)])).await;

    let clock = ServerClock::pinned(NOW);
    let summary = tombstone_gc(&db, &client, &bucket, &clock, &CompactConfig::default())
        .await
        .expect("gc");

    let destroyed = summary
        .destroyed
        .iter()
        .find(|d| d.relkey == image)
        .expect("image destroyed");
    assert!(
        destroyed.content_destroyed.contains(&content),
        "preview/thumb content destroyed"
    );

    // Exact bucket object presence/absence.
    assert!(
        !exists(&client, &bucket, &library_key(&image)).await,
        "original gone"
    );
    assert!(
        !exists(&client, &bucket, &sidecar_key(&image)).await,
        "sidecar gone"
    );
    assert!(
        !exists(&client, &bucket, &preview_key(&content)).await,
        "preview gone"
    );
    assert!(
        !exists(&client, &bucket, &thumb_key(&content, ThumbSize::Small)).await,
        "thumb gone"
    );
    assert!(
        !exists(&client, &bucket, &tombstone_key(&image)).await,
        "tombstone object gone"
    );

    // The deletion is now carried only by the runner's manifest deleted set.
    let runner_manifest = get_manifest(&client, &bucket, &a)
        .await
        .expect("runner manifest");
    assert!(
        runner_manifest.deleted.iter().any(|d| d.del == image),
        "deleted-set row retained for bootstrappers"
    );
    // A fresh device merges the manifest and learns the deletion without any
    // tombstone object (the §2.3/A3 resurrection fix).
    let (_rdir, _rpath, rdb) = open_db(&b);
    let mut consumer = common::sync::ReplayConsumer;
    merge(&[(a.clone(), runner_manifest)], &rdb, &mut consumer).expect("merge");
    assert!(
        rdb.get_deleted(&image).expect("deleted").is_some(),
        "bootstrapper learned the deletion from the deleted set"
    );
}

#[tokio::test]
async fn tombstone_gc_resurrection_guard_preserves_superseded_tombstone() {
    // A resurrecting put (dominating vv) supersedes the del: NOT GC'd, data
    // keys preserved (edits beat deletes survives GC, §2.7/§2.6).
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("gc-resurrect");
    let client = g.client();
    let (a, b) = (dev(DEV_A), dev(DEV_B));
    let (_dir, _path, db) = open_db(&a);

    let image = rel("img/edited.NEF");
    let content = rrcloud_core::semhash::ContentId::from_bytes(b"edited-image");
    // del at vv {A:5}, 31 days old.
    plant_deleted_image(
        &db,
        &client,
        &bucket,
        &image,
        &content,
        vv(&[(&a, 5)]),
        NOW - 31 * 86_400,
    )
    .await;

    // A resurrecting put: B published a sidecar put on the image with a vv
    // dominating the del ({A:5, B:1}). The runner has applied it (so the
    // item is live again, not deleted) — the final journal re-read must find
    // the dominating put and refuse GC.
    let put = {
        let mut e = common::sync::entry(&b, Op::Put, Kind::Sidecar, sidecar_key(&image));
        e.vv = vv(&[(&a, 5), (&b, 1)]);
        e
    };
    common::sync::put_raw_segment(
        &client,
        &bucket,
        &b,
        1,
        &[{
            let mut s = put.clone();
            s.seq = 1;
            s
        }],
    )
    .await;
    // Runner's local state reflects the resurrection: the item is live.
    db.replay_put_item(&image, &synced_sidecar(vv(&[(&a, 5), (&b, 1)])))
        .expect("resurrected item");
    db.remove_deleted(&image)
        .expect("clear deleted row on resurrect");

    put_device(&client, &bucket, &b, &device_entry(0, NOW - 60, &[(&a, 9)])).await;

    let clock = ServerClock::pinned(NOW);
    let summary = tombstone_gc(&db, &client, &bucket, &clock, &CompactConfig::default())
        .await
        .expect("gc");

    assert!(
        summary.destroyed.iter().all(|d| d.relkey != image),
        "superseded tombstone is NOT destroyed"
    );
    assert!(
        summary
            .retained
            .iter()
            .any(|r| r.relkey == image && matches!(r.reason, GcSkipReason::Superseded)),
        "retained with the Superseded reason"
    );
    assert!(
        exists(&client, &bucket, &library_key(&image)).await,
        "original preserved"
    );
    assert!(
        exists(&client, &bucket, &sidecar_key(&image)).await,
        "sidecar preserved"
    );
}

#[tokio::test]
async fn tombstone_gc_content_id_liveness_keeps_shared_preview_until_both_gone() {
    // Two images share one content_id (byte-identical originals). One is
    // deleted + GC-eligible, the other is live -> the shared
    // preview/thumb/original-content is NOT destroyed. Only when BOTH are
    // gone does the content go.
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("gc-dedupe");
    let client = g.client();
    let (a, b) = (dev(DEV_A), dev(DEV_B));
    let (_dir, _path, db) = open_db(&a);

    let content = rrcloud_core::semhash::ContentId::from_bytes(b"shared-bytes");
    let dead = rel("img/dup-a.NEF");
    let live = rel("img/dup-b.NEF");

    // Live sibling references the same content_id.
    put_object_raw(&client, &bucket, &library_key(&live), b"original-bytes").await;
    db.replay_put_item(&live, &original_item(&content, vv(&[(&a, 2)]), false))
        .expect("live sibling");

    // Dead image, GC-eligible (31d).
    plant_deleted_image(
        &db,
        &client,
        &bucket,
        &dead,
        &content,
        vv(&[(&a, 5)]),
        NOW - 31 * 86_400,
    )
    .await;
    put_device(&client, &bucket, &b, &device_entry(0, NOW - 60, &[(&a, 9)])).await;

    let clock = ServerClock::pinned(NOW);
    let first = tombstone_gc(&db, &client, &bucket, &clock, &CompactConfig::default())
        .await
        .expect("gc 1");

    // The dead image's own data keys go, but the shared content stays.
    assert!(
        !exists(&client, &bucket, &library_key(&dead)).await,
        "dead original gone"
    );
    assert!(
        exists(&client, &bucket, &preview_key(&content)).await,
        "shared preview kept while a live sibling references the content_id"
    );
    let dead_destroyed = first
        .destroyed
        .iter()
        .find(|d| d.relkey == dead)
        .expect("dead destroyed");
    assert!(
        dead_destroyed.content_destroyed.is_empty(),
        "content NOT destroyed while still referenced"
    );

    // Now delete the sibling too and GC again -> content finally goes.
    db.replay_put_item(&live, &original_item(&content, vv(&[(&a, 6)]), true))
        .expect("delete sibling");
    db.record_deleted(
        &live,
        &DeletedRecord {
            vv: vv(&[(&a, 6)]),
            server_ts: NOW - 31 * 86_400,
        },
    )
    .expect("sibling deleted row");
    let tomb = Tombstone {
        relkey: live.clone(),
        vv: vv(&[(&a, 6)]),
        device: a.clone(),
        server_ts: NOW - 31 * 86_400,
        kinds: vec![Kind::Original],
    };
    put_object_raw(
        &client,
        &bucket,
        &tombstone_key(&live),
        &serde_json::to_vec(&tomb).unwrap(),
    )
    .await;

    let second = tombstone_gc(&db, &client, &bucket, &clock, &CompactConfig::default())
        .await
        .expect("gc 2");
    assert!(
        !exists(&client, &bucket, &preview_key(&content)).await,
        "shared content destroyed once BOTH references are gone"
    );
    let live_destroyed = second
        .destroyed
        .iter()
        .find(|d| d.relkey == live)
        .expect("sibling destroyed");
    assert!(live_destroyed.content_destroyed.contains(&content));
}

#[tokio::test]
async fn tombstone_gc_grace_window_keeps_a_young_tombstone() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("gc-grace");
    let client = g.client();
    let (a, b) = (dev(DEV_A), dev(DEV_B));
    let (_dir, _path, db) = open_db(&a);

    let image = rel("img/fresh.NEF");
    let content = rrcloud_core::semhash::ContentId::from_bytes(b"fresh-image");
    // Only 5 days old: inside the 30-day grace window.
    plant_deleted_image(
        &db,
        &client,
        &bucket,
        &image,
        &content,
        vv(&[(&a, 5)]),
        NOW - 5 * 86_400,
    )
    .await;
    // Even though every active device applied well past it.
    put_device(
        &client,
        &bucket,
        &b,
        &device_entry(0, NOW - 60, &[(&a, 99)]),
    )
    .await;

    const { assert!(5 * 86_400 < RECENTLY_DELETED_GRACE_SECS) };
    let clock = ServerClock::pinned(NOW);
    let summary = tombstone_gc(&db, &client, &bucket, &clock, &CompactConfig::default())
        .await
        .expect("gc");

    assert!(summary.destroyed.is_empty(), "nothing GC'd inside grace");
    assert!(
        summary
            .retained
            .iter()
            .any(|r| r.relkey == image && matches!(r.reason, GcSkipReason::WithinGrace { .. })),
        "retained WithinGrace"
    );
    assert!(
        exists(&client, &bucket, &library_key(&image)).await,
        "data keys preserved in grace"
    );
    assert!(
        exists(&client, &bucket, &tombstone_key(&image)).await,
        "tombstone preserved in grace"
    );
}

#[tokio::test]
async fn tombstone_gc_crash_between_fold_and_delete_is_recoverable() {
    // Ordering invariant: the deleted-set row is folded into the runner's
    // manifest (durable) BEFORE any data key is destroyed. A crash modeled
    // as a DELETE failure must leave the deleted row already in the manifest
    // and no data key destroyed-without-record; a re-run completes.
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("gc-crash");
    let client = g.client();
    let (a, b) = (dev(DEV_A), dev(DEV_B));
    let (_dir, _path, db) = open_db(&a);

    let image = rel("img/crash.NEF");
    let content = rrcloud_core::semhash::ContentId::from_bytes(b"crash-image");
    plant_deleted_image(
        &db,
        &client,
        &bucket,
        &image,
        &content,
        vv(&[(&a, 5)]),
        NOW - 31 * 86_400,
    )
    .await;
    put_device(&client, &bucket, &b, &device_entry(0, NOW - 60, &[(&a, 9)])).await;

    // Fault: the data-key DELETE fails (crash right after the fold PUT).
    let mut fault = FaultS3::new(g.client());
    fault.fail_deletes.insert(library_key(&image));
    let clock = ServerClock::pinned(NOW);
    let crashed = tombstone_gc(&db, &fault, &bucket, &clock, &CompactConfig::default()).await;
    assert!(crashed.is_err(), "the DELETE failure surfaces");

    // The deleted-set row is already durable in the runner's manifest, and
    // nothing was destroyed without its record.
    let manifest = get_manifest(&client, &bucket, &a)
        .await
        .expect("manifest after crash");
    assert!(
        manifest.deleted.iter().any(|d| d.del == image),
        "deleted-set row folded BEFORE any destroy"
    );

    // A re-run with a healthy client completes the destruction.
    let summary = tombstone_gc(&db, &client, &bucket, &clock, &CompactConfig::default())
        .await
        .expect("gc re-run");
    assert!(
        summary.destroyed.iter().any(|d| d.relkey == image),
        "re-run completes"
    );
    assert!(
        !exists(&client, &bucket, &library_key(&image)).await,
        "original finally gone"
    );
    assert!(
        !exists(&client, &bucket, &tombstone_key(&image)).await,
        "tombstone finally gone"
    );
}

#[tokio::test]
async fn tombstone_gc_resurrection_guard_from_on_wire_journal_only() {
    // Review round 0 blocker: a resurrecting put durably PUBLISHED to a
    // device's journal segment but NOT yet folded into the runner's local
    // state must still block GC — the §2.10(d) "final journal re-read (all
    // devices' prefixes)" closes the catch-up->GC race. The old local-only
    // superseded-check consulted a stale snapshot and destroyed the restore's
    // data keys (Garage-probe-verified). The runner's local item here is
    // STILL deleted; the put lives only on the wire.
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("gc-resurrect-wire");
    let client = g.client();
    let (a, b) = (dev(DEV_A), dev(DEV_B));
    let (_dir, _path, db) = open_db(&a);

    let image = rel("img/wire-edited.NEF");
    let content = rrcloud_core::semhash::ContentId::from_bytes(b"wire-edited");
    // del {a:5}, 31 days old; runner's LOCAL state shows it deleted (NOT
    // caught up to B's resurrecting put).
    plant_deleted_image(
        &db,
        &client,
        &bucket,
        &image,
        &content,
        vv(&[(&a, 5)]),
        NOW - 31 * 86_400,
    )
    .await;

    // Resurrecting put {a:5,b:1} published ONLY to B's journal segment.
    let put = {
        let mut e = common::sync::entry(&b, Op::Put, Kind::Sidecar, sidecar_key(&image));
        e.seq = 1;
        e.vv = vv(&[(&a, 5), (&b, 1)]);
        e
    };
    common::sync::put_raw_segment(&client, &bucket, &b, 1, &[put]).await;
    put_device(&client, &bucket, &b, &device_entry(0, NOW - 60, &[(&a, 9)])).await;

    let clock = ServerClock::pinned(NOW);
    let summary = tombstone_gc(&db, &client, &bucket, &clock, &CompactConfig::default())
        .await
        .expect("gc");

    assert!(
        summary.destroyed.iter().all(|d| d.relkey != image),
        "an on-wire resurrecting put (not yet applied locally) blocks GC"
    );
    assert!(
        summary
            .retained
            .iter()
            .any(|r| r.relkey == image && matches!(r.reason, GcSkipReason::Superseded)),
        "retained with the Superseded reason from the journal re-read"
    );
    assert!(
        exists(&client, &bucket, &library_key(&image)).await,
        "original preserved"
    );
    assert!(
        exists(&client, &bucket, &sidecar_key(&image)).await,
        "sidecar preserved"
    );
    assert!(
        exists(&client, &bucket, &tombstone_key(&image)).await,
        "tombstone preserved"
    );
}

#[tokio::test]
async fn tombstone_gc_content_liveness_from_on_wire_sibling() {
    // Review round 0 major: a live sibling sharing a content_id, present on
    // the wire (another device's journal) but NOT folded into the runner's
    // local state, must keep the shared preview/thumb from being destroyed.
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("gc-dedupe-wire");
    let client = g.client();
    let (a, b) = (dev(DEV_A), dev(DEV_B));
    let (_dir, _path, db) = open_db(&a);

    let content = rrcloud_core::semhash::ContentId::from_bytes(b"shared-on-wire");
    let dead = rel("img/dup-dead.NEF");
    let live = rel("img/dup-live.NEF");

    // Dead image, GC-eligible (31d); its local record references the content.
    plant_deleted_image(
        &db,
        &client,
        &bucket,
        &dead,
        &content,
        vv(&[(&a, 5)]),
        NOW - 31 * 86_400,
    )
    .await;

    // Live sibling referencing the SAME content_id, present ONLY on B's
    // journal (never folded into the runner's local state).
    let put = {
        let mut e = common::sync::entry(&b, Op::Put, Kind::Original, library_key(&live));
        e.seq = 1;
        e.vv = vv(&[(&b, 1)]);
        e.content_id = Some(content.clone());
        e
    };
    common::sync::put_raw_segment(&client, &bucket, &b, 1, &[put]).await;
    put_device(&client, &bucket, &b, &device_entry(0, NOW - 60, &[(&a, 9)])).await;

    let clock = ServerClock::pinned(NOW);
    let summary = tombstone_gc(&db, &client, &bucket, &clock, &CompactConfig::default())
        .await
        .expect("gc");

    // The dead image's own data keys go, but the shared content survives.
    assert!(
        !exists(&client, &bucket, &library_key(&dead)).await,
        "dead original gone"
    );
    assert!(
        exists(&client, &bucket, &preview_key(&content)).await,
        "shared preview kept while an on-wire sibling references the content_id"
    );
    let dead_destroyed = summary
        .destroyed
        .iter()
        .find(|d| d.relkey == dead)
        .expect("dead destroyed");
    assert!(
        dead_destroyed.content_destroyed.is_empty(),
        "content NOT destroyed while an on-wire live sibling references it"
    );
}

#[tokio::test]
async fn tombstone_gc_content_kept_by_horizon_blocked_sibling() {
    // Review round 1 minor: a HorizonBlocked tombstone keeps its data keys
    // this pass exactly like a WithinGrace one, so its content_id must be
    // protected from the content-destruction guard too. Two tombstones share
    // one content_id (byte-identical originals): X is Eligible (past the
    // 14-day cap) and Y is HorizonBlocked (past grace, inside the cap, no del
    // on the wire). GC destroys X's own data keys but must NOT destroy the
    // shared preview/thumb — Y's original still references the content.
    //
    // Reachable only under an aggressive homelab policy (grace < cap); under
    // the default config a past-grace tombstone is always past the cap too,
    // so HorizonBlocked never co-occurs with an Eligible sibling.
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("gc-dedupe-horizon");
    let client = g.client();
    let (a, b) = (dev(DEV_A), dev(DEV_B));
    let (_dir, _path, db) = open_db(&a);

    // Aggressive policy: grace (5d) below the cap (14d).
    let cfg = CompactConfig {
        grace_secs: 5 * 86_400,
        laggard_cap_secs: 14 * 86_400,
        ..CompactConfig::default()
    };

    let content = rrcloud_core::semhash::ContentId::from_bytes(b"shared-horizon");
    let x = rel("img/dup-x.NEF");
    let y = rel("img/dup-y.NEF");

    // X: 20 days old -> past the 14-day cap -> Eligible.
    plant_deleted_image(
        &db,
        &client,
        &bucket,
        &x,
        &content,
        vv(&[(&a, 5)]),
        NOW - 20 * 86_400,
    )
    .await;
    // Y: 8 days old -> past the 5-day grace, inside the 14-day cap, and with
    // no del on the wire the fast path cannot fire -> HorizonBlocked.
    plant_deleted_image(
        &db,
        &client,
        &bucket,
        &y,
        &content,
        vv(&[(&a, 7)]),
        NOW - 8 * 86_400,
    )
    .await;
    put_device(&client, &bucket, &b, &device_entry(0, NOW - 60, &[(&a, 9)])).await;

    let clock = ServerClock::pinned(NOW);
    let summary = tombstone_gc(&db, &client, &bucket, &clock, &cfg)
        .await
        .expect("gc");

    // X destroyed, Y retained HorizonBlocked.
    let x_destroyed = summary
        .destroyed
        .iter()
        .find(|d| d.relkey == x)
        .expect("X destroyed");
    assert!(
        summary
            .retained
            .iter()
            .any(|r| r.relkey == y && matches!(r.reason, GcSkipReason::HorizonBlocked)),
        "Y retained HorizonBlocked, got {:?}",
        summary.retained
    );

    // Y's own data keys and tombstone are untouched.
    assert!(
        exists(&client, &bucket, &library_key(&y)).await,
        "HorizonBlocked sibling's original preserved"
    );
    assert!(
        exists(&client, &bucket, &tombstone_key(&y)).await,
        "HorizonBlocked sibling's tombstone preserved"
    );
    // The shared content survives because Y still references it.
    assert!(
        exists(&client, &bucket, &preview_key(&content)).await,
        "shared preview kept while a HorizonBlocked sibling references the content_id"
    );
    assert!(
        exists(&client, &bucket, &thumb_key(&content, ThumbSize::Small)).await,
        "shared thumb kept while a HorizonBlocked sibling references the content_id"
    );
    assert!(
        x_destroyed.content_destroyed.is_empty(),
        "content NOT destroyed while a HorizonBlocked sibling references it"
    );
}

#[tokio::test]
async fn tombstone_gc_horizon_fast_path_uses_seq_not_vv_component() {
    // Review round 0 minor: §2.10(a)'s "every active device applied past it"
    // fast path must compare the del's journal SEQ (seq space) against each
    // active device's applied cursor (seq space), NOT the del's vv component
    // (vv space) — the two counters diverge (attestations/moves/re-puts
    // advance seq without bumping vv). Under a grace<cap config the fast path
    // is live, and the old vv-component compare passed prematurely. Here the
    // del sits at seq 10 with vv {A:1}; the only active peer applied only to
    // seq 5, and the tombstone is past grace but under the cap -> it must be
    // RETAINED (HorizonBlocked), where the vv-component compare (1) would
    // have wrongly declared it eligible.
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("gc-seq-horizon");
    let client = g.client();
    let (a, b) = (dev(DEV_A), dev(DEV_B));
    let (_dir, _path, db) = open_db(&a);

    // grace 5d < cap 14d, so the fast path (not the cap) gates (a).
    let cfg = CompactConfig {
        grace_secs: 5 * 86_400,
        laggard_cap_secs: 14 * 86_400,
        ..CompactConfig::default()
    };

    let image = rel("img/seq-horizon.NEF");
    let content = rrcloud_core::semhash::ContentId::from_bytes(b"seq-horizon");
    // Tombstone 8 days old: past the 5-day grace, under the 14-day cap. The
    // runner (A) is the deleting device; del vv {A:1}.
    plant_deleted_image(
        &db,
        &client,
        &bucket,
        &image,
        &content,
        vv(&[(&a, 1)]),
        NOW - 8 * 86_400,
    )
    .await;

    // The del entry on the wire sits at seq 10 (seq != vv component).
    let del = {
        let mut e = common::sync::entry(&a, Op::Del, Kind::Sidecar, sidecar_key(&image));
        e.seq = 10;
        e.vv = vv(&[(&a, 1)]);
        e
    };
    common::sync::put_raw_segment(&client, &bucket, &a, 10, &[del]).await;

    // The only active peer applied A's prefix only to seq 5 (< the del's
    // seq 10): it has NOT learned the deletion.
    put_device(&client, &bucket, &b, &device_entry(0, NOW - 60, &[(&a, 5)])).await;

    let clock = ServerClock::pinned(NOW);
    let summary = tombstone_gc(&db, &client, &bucket, &clock, &cfg)
        .await
        .expect("gc");

    assert!(
        summary.destroyed.iter().all(|d| d.relkey != image),
        "a laggard behind the del's real seq blocks GC (seq-space horizon)"
    );
    assert!(
        summary
            .retained
            .iter()
            .any(|r| r.relkey == image && matches!(r.reason, GcSkipReason::HorizonBlocked)),
        "retained HorizonBlocked, got {:?}",
        summary.retained
    );
    assert!(
        exists(&client, &bucket, &library_key(&image)).await,
        "data keys preserved while the horizon is blocked"
    );
}

// ===========================================================================
// Device lifecycle (§2.10)
// ===========================================================================

#[tokio::test]
async fn retirement_drops_device_from_active_set_and_unblocks_compaction() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("cmp-retire");
    let client = g.client();
    let (a, b) = (dev(DEV_A), dev(DEV_B));
    let (_dir, _path, db) = open_db(&a);

    // A has 2 segments, covered + aged. Published recently (cap not in play).
    let segs = publish_segments(&db, &client, &bucket, 2, NOW - 3600).await;
    let manifest = build_manifest(&db, NOW - 25 * 3600).expect("manifest");
    put_manifest(&client, &bucket, &a, &manifest)
        .await
        .expect("plant");
    // B is the ONLY active device and is a laggard (applied[A]=0): it blocks
    // the fast path, and the segments are not past the 14-day cap.
    put_device(&client, &bucket, &b, &device_entry(0, NOW - 60, &[(&a, 0)])).await;

    let clock = ServerClock::pinned(NOW);
    let blocked = compact_own_segments(&db, &client, &bucket, &clock, &CompactConfig::default())
        .await
        .expect("compact blocked");
    assert!(
        blocked.deleted_seqs.is_empty(),
        "laggard B blocks the fast path"
    );
    assert!(blocked
        .skipped
        .iter()
        .any(|s| matches!(s.reason, SkipReason::HorizonBlocked { .. })));

    // Retire B -> it drops from the active set immediately -> horizon
    // unbounded -> compaction proceeds.
    retire_device(&client, &bucket, &b).await.expect("retire B");
    let unblocked = compact_own_segments(&db, &client, &bucket, &clock, &CompactConfig::default())
        .await
        .expect("compact after retire");
    let mut deleted = unblocked.deleted_seqs.clone();
    deleted.sort_unstable();
    assert_eq!(
        deleted, segs,
        "retiring the only blocker unblocks compaction"
    );
}

#[tokio::test]
async fn auto_retire_sweep_retires_90_day_idle_device() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("cmp-autoretire");
    let client = g.client();
    let (a, b, c) = (dev(DEV_A), dev(DEV_B), dev(DEV_C));

    // A active, B idle 91 days (auto-retire), C idle 10 days (kept). Each
    // entry written more than once so the first-heartbeat caveat does not
    // suppress retirement (modeled by a distinct created time well in the
    // past — the entry has a settled last_seen).
    put_device(
        &client,
        &bucket,
        &a,
        &device_entry(NOW - 200 * 86_400, NOW - 60, &[]),
    )
    .await;
    put_device(
        &client,
        &bucket,
        &b,
        &device_entry(NOW - 200 * 86_400, NOW - 91 * 86_400, &[]),
    )
    .await;
    put_device(
        &client,
        &bucket,
        &c,
        &device_entry(NOW - 200 * 86_400, NOW - 10 * 86_400, &[]),
    )
    .await;

    const { assert!(91 * 86_400 > AUTO_RETIRE_SECS) };
    let clock = ServerClock::pinned(NOW);
    let retired = auto_retire_sweep(&client, &bucket, &clock, &CompactConfig::default())
        .await
        .expect("sweep");
    assert_eq!(retired, vec![b.clone()], "only the 90-day-idle device");
    assert!(
        exists(&client, &bucket, &device_retired_key(&b)).await,
        "B marker written"
    );
    assert!(
        !exists(&client, &bucket, &device_retired_key(&c)).await,
        "C not retired"
    );

    // Idempotent: a second sweep retires nothing new.
    let again = auto_retire_sweep(&client, &bucket, &clock, &CompactConfig::default())
        .await
        .expect("sweep again");
    assert!(again.is_empty(), "re-sweep is a no-op");
}

#[tokio::test]
async fn gc_retired_prefix_folds_and_deletes_orphaned_segments() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("cmp-orphan");
    let client = g.client();
    // Runner A; retired device X with an orphaned journal prefix.
    let (a, x, b) = (dev(DEV_A), dev(DEV_X), dev(DEV_B));
    let (_dir, _path, db) = open_db(&a);

    // X published 2 segments long ago (older than the 14-day cap).
    let x_entries: Vec<_> = (0..2)
        .map(|i| {
            let mut e = common::sync::entry(
                &x,
                Op::Put,
                Kind::Sidecar,
                sidecar_key(&rel(&format!("x/i{i}.NEF"))),
            );
            e.seq = (i + 1) as u64;
            e
        })
        .collect();
    common::sync::put_raw_segment(&client, &bucket, &x, 1, &x_entries[..1]).await;
    common::sync::put_raw_segment(&client, &bucket, &x, 2, &x_entries[1..]).await;
    // X is retired.
    put_retired(&client, &bucket, &x).await;
    // One other active device, applied past X's prefix (so horizon clears).
    put_device(&client, &bucket, &b, &device_entry(0, NOW - 60, &[(&x, 2)])).await;
    put_device(&client, &bucket, &a, &device_entry(0, NOW - 60, &[(&x, 2)])).await;

    let clock = ServerClock::pinned(NOW);
    let summaries = gc_retired_prefixes(&db, &client, &bucket, &clock, &CompactConfig::default())
        .await
        .expect("orphan gc");

    let (_dev, summary) = summaries
        .iter()
        .find(|(d, _)| *d == x)
        .expect("X's orphaned prefix processed");
    let mut deleted = summary.deleted_seqs.clone();
    deleted.sort_unstable();
    assert_eq!(deleted, vec![1, 2], "orphaned segments deleted");
    assert!(!exists(&client, &bucket, &journal_segment_key(&x, 1)).await);
    assert!(!exists(&client, &bucket, &journal_segment_key(&x, 2)).await);
    // Folded into the runner's manifest so a bootstrapper still learns X's
    // effects.
    let runner = get_manifest(&client, &bucket, &a)
        .await
        .expect("runner manifest");
    assert!(
        runner.rows.iter().any(|r| r.key == rel("x/i0.NEF"))
            || runner.rows.iter().any(|r| r.key == rel("x/i1.NEF")),
        "X's live effects folded into the runner manifest before deletion"
    );
}

#[tokio::test]
async fn gc_retired_prefix_fold_does_not_resurrect_a_dominated_deletion() {
    // Review round 0 blocker (PROBE2): the runner converged X DELETED at a
    // dominating vv {X:1,A:1}; retired device X's orphaned journal still
    // holds the stale put {X:1}. The vv-aware fold must leave X deleted, and
    // the rebuilt manifest must NOT carry both a live row and a deleted row
    // for the same relkey (the §2.3 A3 mass-resurrection contradiction).
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("cmp-orphan-resurrect");
    let client = g.client();
    let (a, x, b) = (dev(DEV_A), dev(DEV_X), dev(DEV_B));
    let (_dir, _path, db) = open_db(&a);

    let img = rel("img/orphan-x.NEF");
    // Runner's converged head: deleted at {X:1,A:1} + the deleted-set row.
    let mut head = synced_sidecar(vv(&[(&x, 1), (&a, 1)]));
    head.deleted = true;
    db.replay_put_item(&img, &head).expect("deleted head");
    db.record_deleted(
        &img,
        &DeletedRecord {
            vv: vv(&[(&x, 1), (&a, 1)]),
            server_ts: NOW - 40 * 86_400,
        },
    )
    .expect("deleted row");

    // X's orphaned journal: the stale put {X:1} the deletion dominates.
    let stale_put = {
        let mut e = common::sync::entry(&x, Op::Put, Kind::Sidecar, sidecar_key(&img));
        e.seq = 1;
        e.vv = vv(&[(&x, 1)]);
        e
    };
    common::sync::put_raw_segment(&client, &bucket, &x, 1, &[stale_put]).await;
    put_retired(&client, &bucket, &x).await;
    put_device(&client, &bucket, &a, &device_entry(0, NOW - 60, &[(&x, 1)])).await;
    put_device(&client, &bucket, &b, &device_entry(0, NOW - 60, &[(&x, 1)])).await;

    let clock = ServerClock::pinned(NOW);
    gc_retired_prefixes(&db, &client, &bucket, &clock, &CompactConfig::default())
        .await
        .expect("orphan gc");

    // X stays deleted locally (not resurrected by the stale orphan put).
    let item = db.get_item(&img).expect("item").expect("present");
    assert!(
        item.deleted,
        "the fold must not resurrect a dominated deletion"
    );

    // The rebuilt runner manifest advertises one truth: a deleted row, no
    // live row for the same key.
    let manifest = get_manifest(&client, &bucket, &a)
        .await
        .expect("runner manifest");
    assert!(
        !manifest.rows.iter().any(|r| r.key == img),
        "no live row for a key the runner knows is deleted"
    );
    assert!(
        manifest.deleted.iter().any(|d| d.del == img),
        "deleted-set row retained"
    );
}

#[tokio::test]
async fn gc_retired_prefix_fold_does_not_destroy_a_live_dominating_edit() {
    // Review round 0 blocker (PROBE3, the converse): the runner converged X
    // LIVE at a dominating vv {X:1,A:1}; retired device X's orphaned journal
    // holds the stale del {X:1}. The fold must leave X live and NOT advertise
    // a deletion (deletion-loss of a live version).
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("cmp-orphan-delloss");
    let client = g.client();
    let (a, x, b) = (dev(DEV_A), dev(DEV_X), dev(DEV_B));
    let (_dir, _path, db) = open_db(&a);

    let img = rel("img/orphan-live.NEF");
    // Runner's converged head: LIVE at {X:1,A:1}.
    db.replay_put_item(&img, &synced_sidecar(vv(&[(&x, 1), (&a, 1)])))
        .expect("live head");

    // X's orphaned journal: the stale del {X:1} the live edit dominates.
    let stale_del = {
        let mut e = common::sync::entry(&x, Op::Del, Kind::Sidecar, sidecar_key(&img));
        e.seq = 1;
        e.vv = vv(&[(&x, 1)]);
        e
    };
    common::sync::put_raw_segment(&client, &bucket, &x, 1, &[stale_del]).await;
    put_retired(&client, &bucket, &x).await;
    put_device(&client, &bucket, &a, &device_entry(0, NOW - 60, &[(&x, 1)])).await;
    put_device(&client, &bucket, &b, &device_entry(0, NOW - 60, &[(&x, 1)])).await;

    let clock = ServerClock::pinned(NOW);
    gc_retired_prefixes(&db, &client, &bucket, &clock, &CompactConfig::default())
        .await
        .expect("orphan gc");

    // X stays live locally (the stale orphan del did not destroy it).
    let item = db.get_item(&img).expect("item").expect("present");
    assert!(
        !item.deleted,
        "the fold must not destroy a live dominating edit"
    );

    // The rebuilt runner manifest carries the live row and NO deleted row.
    let manifest = get_manifest(&client, &bucket, &a)
        .await
        .expect("runner manifest");
    assert!(
        manifest.rows.iter().any(|r| r.key == img),
        "live row retained"
    );
    assert!(
        !manifest.deleted.iter().any(|d| d.del == img),
        "no spurious deleted row for a live key"
    );
}

// ===========================================================================
// Pre-upload quarantine (§2.3)
// ===========================================================================

#[tokio::test]
async fn quarantine_required_lists_local_only_and_does_not_auto_reupload() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("q-detect");
    let client = g.client();
    let a = dev(DEV_A);
    let (_dir, _path, db) = open_db(&a);

    // Two local-only unprovable items (base_unknown) + one proven item.
    let lonely1 = rel("local/only-1.NEF");
    let lonely2 = rel("local/only-2.NEF");
    let proven = rel("local/proven.NEF");
    let mut r1 = synced_sidecar(vv(&[(&a, 1)]));
    r1.base_unknown = true;
    r1.state = ItemState::Dirty;
    r1.blake3 = None;
    let r2 = r1.clone();
    db.replay_put_item(&lonely1, &r1).unwrap();
    db.replay_put_item(&lonely2, &r2).unwrap();
    db.replay_put_item(&proven, &synced_sidecar(vv(&[(&a, 1)])))
        .unwrap();

    // Applied-proof horizon predates the 12-month retention window.
    db.set_applied_proof_server_ts(NOW - (DELETED_SET_RETENTION_SECS + 86_400))
        .unwrap();

    let clock = ServerClock::pinned(NOW);
    let decision =
        detect_local_only_unprovable(&db, &clock, &CompactConfig::default()).expect("detect");
    match decision {
        QuarantineDecision::QuarantineRequired { mut relkeys } => {
            relkeys.sort();
            assert_eq!(
                relkeys,
                vec![lonely1.clone(), lonely2.clone()],
                "both unprovable local-only keys"
            );
            assert!(
                !relkeys.contains(&proven),
                "a proven item is not quarantined"
            );
        }
        other => panic!("expected QuarantineRequired, got {other:?}"),
    }

    // Nothing was auto-uploaded (no own segment published).
    assert!(
        !exists(&client, &bucket, &journal_segment_key(&a, 1)).await,
        "detection never auto-re-uploads"
    );

    // Within the retention window the decision clears.
    db.set_applied_proof_server_ts(NOW - 86_400).unwrap();
    assert!(matches!(
        detect_local_only_unprovable(&db, &clock, &CompactConfig::default()).unwrap(),
        QuarantineDecision::Clear
    ));
}

#[tokio::test]
async fn resolve_quarantine_keep_restores_discard_deletes_and_neither_resurrects() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("q-resolve");
    let client = g.client();
    let a = dev(DEV_A);
    let (_dir, _path, db) = open_db(&a);
    let sync_root = tempfile::tempdir().expect("sync root");

    let keep = rel("local/keep.NEF");
    let discard = rel("local/discard.NEF");
    let deleted_key = rel("local/was-deleted.NEF");
    for k in [&keep, &discard, &deleted_key] {
        let mut r = synced_sidecar(vv(&[(&a, 1)]));
        r.base_unknown = true;
        r.state = ItemState::Dirty;
        r.blake3 = None;
        db.replay_put_item(k, &r).unwrap();
        // A local file exists under the sync root for each.
        let path = rrcloud_core::keys::local_path(k, sync_root.path());
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"bytes").unwrap();
    }
    // A cloud tombstone legitimately covers `deleted_key`.
    let tomb = Tombstone {
        relkey: deleted_key.clone(),
        vv: vv(&[(&a, 9)]),
        device: a.clone(),
        server_ts: NOW - 40 * 86_400,
        kinds: vec![Kind::Sidecar],
    };
    put_object_raw(
        &client,
        &bucket,
        &tombstone_key(&deleted_key),
        &serde_json::to_vec(&tomb).unwrap(),
    )
    .await;

    // Keep: re-advertises (stages a put), clears base_unknown.
    let kept = resolve_quarantine(
        &db,
        &client,
        &bucket,
        sync_root.path(),
        &keep,
        QuarantineResolution::Keep,
    )
    .await
    .expect("keep");
    assert!(matches!(kept, QuarantineOutcome::Restored { .. }));
    assert!(
        !db.get_item(&keep).unwrap().unwrap().base_unknown,
        "restored item is no longer base_unknown"
    );

    // Discard: local file removed + record dropped.
    let discarded = resolve_quarantine(
        &db,
        &client,
        &bucket,
        sync_root.path(),
        &discard,
        QuarantineResolution::Discard,
    )
    .await
    .expect("discard");
    assert!(matches!(discarded, QuarantineOutcome::Discarded { .. }));
    assert!(db.get_item(&discard).unwrap().is_none(), "record dropped");
    assert!(
        !rrcloud_core::keys::local_path(&discard, sync_root.path()).exists(),
        "local file deleted"
    );

    // Keep on a legitimately-deleted key is REFUSED (never resurrects).
    let refused = resolve_quarantine(
        &db,
        &client,
        &bucket,
        sync_root.path(),
        &deleted_key,
        QuarantineResolution::Keep,
    )
    .await
    .expect("refuse");
    assert!(
        matches!(refused, QuarantineOutcome::RefusedResurrection { .. }),
        "Keep must not resurrect a tombstoned key, got {refused:?}"
    );
    assert!(
        !exists(&client, &bucket, &journal_segment_key(&a, 1)).await,
        "no resurrecting put published for the deleted key"
    );
}
