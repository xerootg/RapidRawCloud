//! Failing tests for `rrcloud_core::publisher` (architecture §2.1.5, §2.2,
//! §1.2): durable outbound staging, freeze-before-network segment
//! publication with strict ordering and crash replay, segment caps via the
//! shrink-retry contract, and the device-registry heartbeat with
//! server-time offset measurement.
//!
//! Garage-backed where the network is involved (shared instance,
//! per-test buckets); staging-only tests run without Garage.

mod common;

use bytes::Bytes;
use common::garage;
use common::sync::{dev, entry, open_db, sidecar_entries, FakeS3, RecordingConsumer, DEV_A, DEV_B};
use md5::{Digest as _, Md5};
use rrcloud_core::journal::{
    decode_segment, Kind, Op, JOURNAL_VERSION, SEGMENT_MAX_BYTES, SEGMENT_MAX_ENTRIES,
};
use rrcloud_core::keys::{device_registry_key, journal_segment_key, sidecar_key, CONTROL_PREFIX};
use rrcloud_core::publisher::{
    enqueue_entry, get_device_entry, publish_pending, put_device_entry, DeviceProfile,
    PublisherError, DEVICE_ENTRY_MAX_BYTES, PROTO_READ, PROTO_WRITE,
};
use rrcloud_core::reader::poll;
use rrcloud_core::s3::{
    ByteRange, GetObjectOutput, HeadObjectOutput, ListObjectsV2Output, ListObjectsV2Request,
    PutObjectOptions, PutObjectOutput, S3Api, S3Client, S3Config, S3Error,
};

fn md5_hex(data: &[u8]) -> String {
    hex::encode(Md5::digest(data))
}

/// A client whose endpoint accepts no connections (never reached by tests
/// that must not touch the network).
fn offline_client() -> S3Client {
    S3Client::new(S3Config {
        endpoint: "http://127.0.0.1:9".to_string(),
        region: "garage".to_string(),
        access_key_id: "GKnone".to_string(),
        secret_access_key: "none".to_string(),
        connect_timeout: Some(std::time::Duration::from_millis(250)),
        read_timeout: None,
        request_timeout: None,
    })
    .expect("client")
}

// ---------------------------------------------------------------------------
// Staging (no network)
// ---------------------------------------------------------------------------

#[test]
fn an_entry_too_large_for_any_segment_is_refused_at_enqueue() {
    let a = dev(DEV_A);
    let (_dir, _path, db) = open_db(&a);
    // A single encoded line over SEGMENT_MAX_BYTES could never be frozen
    // into a conforming segment; staging it would poison-pill the whole
    // outbound lane. Refuse it synchronously, where the caller has
    // context.
    let mut oversized = entry(
        &a,
        Op::Put,
        Kind::Sidecar,
        sidecar_key(&common::sync::rel("big.NEF")),
    );
    oversized.color_label = Some("x".repeat(2 * SEGMENT_MAX_BYTES));
    let err = enqueue_entry(&db, &oversized).expect_err("oversized entry must be refused");
    match err {
        PublisherError::OversizedEntry { size } => assert!(size > SEGMENT_MAX_BYTES),
        other => panic!("expected OversizedEntry, got {other:?}"),
    }
    assert_eq!(db.outbound_len().expect("len"), 0, "nothing staged");
}

#[tokio::test]
async fn an_oversized_staged_record_fails_naming_its_id_and_dropping_it_recovers() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("pub-oversized-staged");
    let client = g.client();
    let a = dev(DEV_A);
    let (_dir, _path, db) = open_db(&a);

    // Bypass enqueue's gate (modeling a record staged by an older build):
    // a decodable v1 entry whose single line exceeds the segment byte cap.
    let mut giant = entry(
        &a,
        Op::Put,
        Kind::Sidecar,
        sidecar_key(&common::sync::rel("giant.NEF")),
    );
    giant.color_label = Some("x".repeat(2 * SEGMENT_MAX_BYTES));
    let line = giant.to_json_line().expect("encode");
    let bad_id = db.stage_outbound(line.as_bytes()).expect("stage raw");
    enqueue_entry(
        &db,
        &entry(
            &a,
            Op::Put,
            Kind::Sidecar,
            sidecar_key(&common::sync::rel("after.NEF")),
        ),
    )
    .expect("enqueue good entry behind it");

    // The failure must carry the staging id so the engine can
    // surface-and-drop it — like CorruptStaged, not an unactionable wedge.
    let err = publish_pending(&db, &client, &bucket)
        .await
        .expect_err("an unfreezable staged record must fail typed");
    match err {
        PublisherError::OversizedStaged { outbound_id, size } => {
            assert_eq!(outbound_id, bad_id);
            assert!(size > SEGMENT_MAX_BYTES);
        }
        other => panic!("expected OversizedStaged, got {other:?}"),
    }
    assert_eq!(db.outbound_len().expect("len"), 2, "nothing consumed");

    // The documented recovery loop closes: drop the named record, retry.
    assert!(db
        .with_txn(|t| t.remove_outbound(bad_id))
        .expect("remove outbound"));
    let report = publish_pending(&db, &client, &bucket)
        .await
        .expect("retry publishes the healthy entry");
    assert_eq!(report.entries, 1);
    assert_eq!(db.outbound_len().expect("len"), 0);
}

#[tokio::test]
async fn a_foreign_authored_staged_record_fails_at_freeze_naming_its_id_and_dropping_it_recovers() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("pub-foreign-staged");
    let client = g.client();
    let a = dev(DEV_A);
    let b = dev(DEV_B);
    let (_dir, _path, db) = open_db(&a);

    // Bypass enqueue_entry's ForeignDevice gate the way the §3.4 composite
    // path can: stage_outbound takes opaque bytes. A decodable v1 entry
    // authored by B must NOT freeze into A's lane — published, every
    // reader would refuse the segment forever (entry.device != prefix
    // owner), and the frozen bytes being the segment's identity means no
    // replay could ever heal the wedge.
    let foreign = entry(
        &b,
        Op::Put,
        Kind::Sidecar,
        sidecar_key(&common::sync::rel("foreign.NEF")),
    );
    let line = foreign.to_json_line().expect("encode");
    let bad_id = db.stage_outbound(line.as_bytes()).expect("stage raw");
    enqueue_entry(
        &db,
        &entry(
            &a,
            Op::Put,
            Kind::Sidecar,
            sidecar_key(&common::sync::rel("own.NEF")),
        ),
    )
    .expect("enqueue own entry behind it");

    // Freezing happens before any network I/O, so the gate must fire even
    // offline — and carry the staging id for surface-and-drop recovery.
    let err = publish_pending(&db, &offline_client(), &bucket)
        .await
        .expect_err("a foreign-authored staged record must fail typed at freeze");
    match err {
        PublisherError::ForeignStaged {
            outbound_id,
            entry_device,
            ours,
        } => {
            assert_eq!(outbound_id, bad_id);
            assert_eq!(entry_device, b);
            assert_eq!(ours, a.clone());
        }
        other => panic!("expected ForeignStaged, got {other:?}"),
    }
    assert_eq!(db.outbound_len().expect("len"), 2, "nothing consumed");
    assert!(
        db.unpublished_segments().expect("unpublished").is_empty(),
        "nothing frozen"
    );

    // The documented recovery loop closes: drop the named record, retry.
    assert!(db
        .with_txn(|t| t.remove_outbound(bad_id))
        .expect("remove outbound"));
    let report = publish_pending(&db, &client, &bucket)
        .await
        .expect("retry publishes the healthy entry");
    assert_eq!(report.entries, 1);
    assert_eq!(db.outbound_len().expect("len"), 0);

    // What landed is readable by a conforming reader (no wedge).
    let decoded = decode_segment(
        &client
            .get_object(&bucket, &journal_segment_key(&a, 1), None)
            .await
            .expect("get")
            .body
            .collect()
            .await
            .expect("body"),
    )
    .expect("decode");
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].device, a);
}

#[test]
fn enqueue_reserves_seq_digit_headroom_so_a_staged_entry_can_always_freeze() {
    let a = dev(DEV_A);
    let (_dir, _path, db) = open_db(&a);

    // Freeze-time stamping replaces the staged seq "0" (1 digit) with up
    // to u64::MAX's 20 digits. An entry whose line passes a naive cap
    // check but sits within those 19 bytes of the cap could be staged yet
    // never freeze — and the documented OversizedStaged recovery would
    // discard a legitimately staged entry. Enqueue must reserve the
    // headroom instead.
    let line_len_for = |pad: usize| {
        let mut e = entry(
            &a,
            Op::Put,
            Kind::Sidecar,
            sidecar_key(&common::sync::rel("pad.NEF")),
        );
        e.color_label = Some("x".repeat(pad));
        let len = e_line_len(&e);
        (e, len)
    };
    fn e_line_len(e: &rrcloud_core::journal::JournalEntry) -> usize {
        let mut staged = e.clone();
        staged.v = JOURNAL_VERSION;
        staged.seq = 0;
        staged.to_json_line().expect("encode").len()
    }

    // Tune padding so the staged line is exactly SEGMENT_MAX_BYTES - 19:
    // within the raw cap (line + newline <= cap), but refused because max
    // seq stamping could push it over.
    let (probe, probe_len) = line_len_for(0);
    let _ = probe;
    let target_refused = SEGMENT_MAX_BYTES - 19;
    let (refused, refused_len) = line_len_for(target_refused - probe_len);
    assert_eq!(refused_len, target_refused, "test calibration");
    assert!(refused_len < SEGMENT_MAX_BYTES, "within the raw cap");
    let err = enqueue_entry(&db, &refused).expect_err("blind-spot entry must be refused");
    match err {
        PublisherError::OversizedEntry { size } => assert!(size > SEGMENT_MAX_BYTES),
        other => panic!("expected OversizedEntry, got {other:?}"),
    }
    assert_eq!(db.outbound_len().expect("len"), 0, "nothing staged");

    // One byte smaller is accepted — and can always freeze, whatever seq
    // it is stamped with.
    let (accepted, accepted_len) = line_len_for(target_refused - probe_len - 1);
    assert_eq!(accepted_len, target_refused - 1, "test calibration");
    enqueue_entry(&db, &accepted).expect("an entry outside the reserve is staged");
    assert_eq!(db.outbound_len().expect("len"), 1);
}

#[test]
fn enqueue_rejects_foreign_device_entry_and_stages_nothing() {
    let a = dev(DEV_A);
    let (_dir, _path, db) = open_db(&a);
    let foreign = entry(
        &dev(DEV_B),
        Op::Put,
        Kind::Sidecar,
        sidecar_key(&common::sync::rel("x.NEF")),
    );
    let err = enqueue_entry(&db, &foreign).expect_err("foreign-device entry must be refused");
    match err {
        PublisherError::ForeignDevice { entry_device, ours } => {
            assert_eq!(entry_device, dev(DEV_B));
            assert_eq!(ours, a);
        }
        other => panic!("expected ForeignDevice, got {other:?}"),
    }
    assert_eq!(db.outbound_len().expect("len"), 0);
}

#[test]
fn enqueue_stages_durably_in_fifo_order() {
    let a = dev(DEV_A);
    let (_dir, path, db) = open_db(&a);
    let entries = sidecar_entries(&a, "stage", 3);
    let mut ids = Vec::new();
    for e in &entries {
        ids.push(enqueue_entry(&db, e).expect("enqueue"));
    }
    assert!(
        ids.windows(2).all(|w| w[0] < w[1]),
        "staging ids must be strictly increasing (FIFO): {ids:?}"
    );
    assert_eq!(db.outbound_len().expect("len"), 3);

    // Staged entries survive a reopen (durable, not an in-memory queue).
    drop(db);
    let db = rrcloud_core::state::SyncDb::open(&path, None).expect("reopen");
    assert_eq!(db.outbound_len().expect("len"), 3);
    let staged = db.iter_outbound().expect("iter");
    let staged_ids: Vec<u64> = staged.iter().map(|(id, _)| *id).collect();
    assert_eq!(staged_ids, ids, "iteration must be FIFO (ascending id)");
}

#[tokio::test]
async fn publish_with_nothing_staged_is_ok_and_touches_no_network() {
    let a = dev(DEV_A);
    let (_dir, _path, db) = open_db(&a);
    // The endpoint accepts no connections: publishing nothing must not
    // perform any network I/O at all, or this errors on the transport.
    let report = publish_pending(&db, &offline_client(), "bucket")
        .await
        .expect("empty publish must succeed offline");
    assert_eq!(report.segments, Vec::<u64>::new());
    assert_eq!(report.entries, 0);
}

// ---------------------------------------------------------------------------
// Publication (Garage)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn publish_drains_fifo_stamps_seqs_and_marks_published() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("pub-drain");
    let client = g.client();
    let a = dev(DEV_A);
    let (_dir, _path, db) = open_db(&a);

    let mut entries = sidecar_entries(&a, "drain", 3);
    // Staged seq values are junk on purpose: publication must stamp them.
    entries[0].seq = 999;
    entries[2].seq = 7;
    for e in &entries {
        enqueue_entry(&db, e).expect("enqueue");
    }

    let report = publish_pending(&db, &client, &bucket)
        .await
        .expect("publish");
    assert_eq!(report.segments, vec![1], "one segment, first seq 1");
    assert_eq!(report.entries, 3);
    assert_eq!(db.outbound_len().expect("len"), 0, "staged lane drained");
    assert_eq!(db.published_cursor().expect("cursor"), 3);
    assert!(db.unpublished_segments().expect("unpublished").is_empty());

    // The published segment decodes to exactly the staged entries, in
    // enqueue order, with contiguous stamped seqs and v = 1.
    let key = journal_segment_key(&a, 1);
    let got = client
        .get_object(&bucket, &key, None)
        .await
        .expect("get segment")
        .body
        .collect()
        .await
        .expect("body");
    let decoded = decode_segment(&got).expect("decode");
    assert_eq!(decoded.len(), 3);
    for (i, (published, staged)) in decoded.iter().zip(&entries).enumerate() {
        assert_eq!(published.seq, 1 + i as u64, "stamped contiguous seqs");
        assert_eq!(published.v, JOURNAL_VERSION);
        let mut expected = staged.clone();
        expected.seq = published.seq;
        expected.v = JOURNAL_VERSION;
        assert_eq!(
            published, &expected,
            "entry {i} published exactly as staged"
        );
    }
}

#[tokio::test]
async fn publish_splits_on_the_entry_count_cap() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("pub-count-cap");
    let client = g.client();
    let a = dev(DEV_A);
    let (_dir, _path, db) = open_db(&a);

    let n = SEGMENT_MAX_ENTRIES + 1; // 1001
    for e in &sidecar_entries(&a, "cap", n) {
        enqueue_entry(&db, e).expect("enqueue");
    }
    let report = publish_pending(&db, &client, &bucket)
        .await
        .expect("publish");
    assert_eq!(
        report.segments,
        vec![1, SEGMENT_MAX_ENTRIES as u64 + 1],
        "1001 entries split as 1000 + 1"
    );
    assert_eq!(report.entries, n as u64);
    assert_eq!(db.published_cursor().expect("cursor"), n as u64);

    let seg1 = client
        .get_object(&bucket, &journal_segment_key(&a, 1), None)
        .await
        .expect("get seg1")
        .body
        .collect()
        .await
        .expect("body");
    let seg2 = client
        .get_object(
            &bucket,
            &journal_segment_key(&a, SEGMENT_MAX_ENTRIES as u64 + 1),
            None,
        )
        .await
        .expect("get seg2")
        .body
        .collect()
        .await
        .expect("body");
    let d1 = decode_segment(&seg1).expect("decode seg1");
    let d2 = decode_segment(&seg2).expect("decode seg2");
    assert_eq!(d1.len(), SEGMENT_MAX_ENTRIES);
    assert_eq!(d2.len(), 1);
    assert_eq!(d1.first().expect("first").seq, 1);
    assert_eq!(d1.last().expect("last").seq, SEGMENT_MAX_ENTRIES as u64);
    assert_eq!(d2[0].seq, SEGMENT_MAX_ENTRIES as u64 + 1);
}

#[tokio::test]
async fn publish_splits_on_the_byte_cap_via_shrink_retry() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("pub-byte-cap");
    let client = g.client();
    let a = dev(DEV_A);
    let (_dir, _path, db) = open_db(&a);

    // 150 entries of ~9KB each (~1.35 MiB total): more than one 1 MiB
    // segment, but comfortably within per-entry limits — forcing the
    // byte-cap shrink-retry, which is only detectable once real seq
    // digits are stamped (freeze_next_segment's SegmentBuild contract).
    let total = 150usize;
    let mut entries = sidecar_entries(&a, "bytes", total);
    for e in &mut entries {
        e.color_label = Some("x".repeat(9_000));
    }
    for e in &entries {
        enqueue_entry(&db, e).expect("enqueue");
    }
    let report = publish_pending(&db, &client, &bucket)
        .await
        .expect("publish");
    assert!(
        report.segments.len() >= 2,
        "~1.35 MiB of entries cannot fit one 1 MiB segment: {:?}",
        report.segments
    );
    assert_eq!(report.segments[0], 1);
    assert_eq!(report.entries, total as u64);
    assert_eq!(db.outbound_len().expect("len"), 0);

    // Every published segment obeys the byte cap; entries are contiguous
    // and in order across segment boundaries.
    let mut next_seq = 1u64;
    for &first in &report.segments {
        assert_eq!(first, next_seq, "segments tile the seq space exactly");
        let bytes = client
            .get_object(&bucket, &journal_segment_key(&a, first), None)
            .await
            .expect("get segment")
            .body
            .collect()
            .await
            .expect("body");
        assert!(
            bytes.len() <= SEGMENT_MAX_BYTES,
            "segment {first} is {} bytes, cap is {SEGMENT_MAX_BYTES}",
            bytes.len()
        );
        let decoded = decode_segment(&bytes).expect("decode");
        assert!(!decoded.is_empty());
        for e in &decoded {
            assert_eq!(e.seq, next_seq);
            next_seq += 1;
        }
    }
    assert_eq!(next_seq, total as u64 + 1, "all {total} entries published");
}

#[tokio::test]
async fn put_failure_stops_the_drain_and_retry_completes_in_order() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("pub-ordering");
    let client = g.client();
    let a = dev(DEV_A);
    let (_dir, _path, db) = open_db(&a);

    // Segment 1 (seq 1): published cleanly.
    enqueue_entry(&db, &sidecar_entries(&a, "ord1", 1)[0]).expect("enqueue");
    let report = publish_pending(&db, &client, &bucket)
        .await
        .expect("publish seg1");
    assert_eq!(report.segments, vec![1]);

    let seg2_key = journal_segment_key(&a, 2);
    let seg3_key = journal_segment_key(&a, 3);

    // Segment 2 (seq 2): freeze succeeds, PUT fails.
    enqueue_entry(&db, &sidecar_entries(&a, "ord2", 1)[0]).expect("enqueue");
    let mut fake = FakeS3::new(g.client());
    fake.fail_puts.insert(seg2_key.clone());
    let err = publish_pending(&db, &fake, &bucket)
        .await
        .expect_err("seg2 PUT failure must surface");
    assert!(matches!(err, PublisherError::S3(_)), "got {err:?}");
    assert_eq!(fake.attempted_puts(), vec![seg2_key.clone()]);

    // Segment 3 (seq 3): staged + frozen while seg2 is still unpublished.
    // The failed pass must stop at seg2: seg3's PUT is NEVER attempted.
    enqueue_entry(&db, &sidecar_entries(&a, "ord3", 1)[0]).expect("enqueue");
    let mut fake = FakeS3::new(g.client());
    fake.fail_puts.insert(seg2_key.clone());
    let err = publish_pending(&db, &fake, &bucket)
        .await
        .expect_err("seg2 still fails");
    assert!(matches!(err, PublisherError::S3(_)), "got {err:?}");
    assert_eq!(
        fake.attempted_puts(),
        vec![seg2_key.clone()],
        "a later segment must not be attempted while an earlier one is unpublished"
    );

    // Both stay frozen and retryable, in order.
    let unpublished: Vec<u64> = db
        .unpublished_segments()
        .expect("unpublished")
        .iter()
        .map(|(seq, _)| *seq)
        .collect();
    assert_eq!(unpublished, vec![2, 3]);

    // Retry with a working client completes strictly in order.
    let fake = FakeS3::new(g.client());
    let report = publish_pending(&db, &fake, &bucket).await.expect("retry");
    assert_eq!(report.segments, vec![2, 3]);
    assert_eq!(
        fake.attempted_puts(),
        vec![seg2_key.clone(), seg3_key.clone()]
    );
    assert_eq!(db.published_cursor().expect("cursor"), 3);
    assert!(db.unpublished_segments().expect("unpublished").is_empty());

    // The bucket holds exactly segments 1, 2, 3.
    let listed = client
        .list_all_objects(&bucket, Some(&format!("{CONTROL_PREFIX}journal/")))
        .await
        .expect("list");
    let mut keys: Vec<String> = listed.into_iter().map(|o| o.key).collect();
    keys.sort();
    assert_eq!(
        keys,
        vec![journal_segment_key(&a, 1), seg2_key, seg3_key],
        "exactly segments 1..=3 published"
    );
}

#[tokio::test]
async fn crash_replay_reputs_unpublished_segments_byte_identical_before_new_ones() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("pub-replay");
    let client = g.client();
    let a = dev(DEV_A);
    let b = dev(DEV_B);
    let (_dir, path, db) = open_db(&a);
    let seg1_key = journal_segment_key(&a, 1);

    // Freeze + PUT segment 1 (3 entries), but the success report is lost
    // (connection died after the backend committed the object): the exact
    // §2.1.5 crash window — the segment is in the bucket but NOT marked
    // published locally.
    for e in &sidecar_entries(&a, "replay1", 3) {
        enqueue_entry(&db, e).expect("enqueue");
    }
    let mut fake = FakeS3::new(g.client());
    fake.put_then_fail.insert(seg1_key.clone());
    publish_pending(&db, &fake, &bucket)
        .await
        .expect_err("lost PUT ack surfaces as an error");
    let frozen = db.unpublished_segments().expect("unpublished");
    assert_eq!(frozen.len(), 1, "segment 1 still frozen+unpublished");
    assert_eq!(frozen[0].0, 1);
    let seg1_bytes = frozen[0].1.clone();

    // Freeze segment 2 as well (seqs 4-5); its publish pass stops at the
    // still-unpublished segment 1.
    for e in &sidecar_entries(&a, "replay2", 2) {
        enqueue_entry(&db, e).expect("enqueue");
    }
    let mut fake = FakeS3::new(g.client());
    fake.fail_puts.insert(seg1_key.clone());
    publish_pending(&db, &fake, &bucket)
        .await
        .expect_err("seg1 re-PUT fails, pass stops");
    assert_eq!(fake.attempted_puts(), vec![seg1_key.clone()]);
    let unpublished: Vec<u64> = db
        .unpublished_segments()
        .expect("unpublished")
        .iter()
        .map(|(seq, _)| *seq)
        .collect();
    assert_eq!(
        unpublished,
        vec![1, 4],
        "two frozen segments, only the first PUT landed"
    );

    // A reader polls BETWEEN the crash and the replay: it sees segment 1's
    // first PUT and applies (dedup keyed by (device, seq)).
    let (_bdir, _bpath, db_b) = open_db(&b);
    let mut consumer = RecordingConsumer::default();
    poll(&db_b, &client, &bucket, &mut consumer)
        .await
        .expect("B poll");
    let seen: Vec<u64> = consumer
        .transcript
        .iter()
        .map(|(_, seq, _, _)| *seq)
        .collect();
    assert_eq!(seen, vec![1, 2, 3]);
    assert_eq!(db_b.cursor(&a).expect("cursor"), 3);
    let polled_bytes = client
        .get_object(&bucket, &seg1_key, None)
        .await
        .expect("get seg1")
        .body
        .collect()
        .await
        .expect("body");
    assert_eq!(
        &polled_bytes[..],
        &seg1_bytes[..],
        "frozen bytes ARE the published bytes"
    );

    // Tamper with the remote object so the replay's re-PUT is observable:
    // after replay the key must hold the ORIGINAL frozen bytes again.
    let garbage = Bytes::from_static(b"{\"v\":1,\"tampered\":true}\n");
    client
        .put_object(
            &bucket,
            &seg1_key,
            garbage.clone(),
            &PutObjectOptions::default(),
        )
        .await
        .expect("tamper");

    // Crash: drop everything; a NEW publisher instance replays from the
    // frozen bytes — re-PUT of segment 1 byte-identical BEFORE segment 2.
    drop(db);
    let db = rrcloud_core::state::SyncDb::open(&path, None).expect("reopen after crash");
    let report = publish_pending(&db, &client, &bucket)
        .await
        .expect("replay publish");
    assert_eq!(
        report.segments,
        vec![1, 4],
        "replayed segment first, then the new one"
    );
    assert_eq!(db.published_cursor().expect("cursor"), 5);

    let replayed = client
        .get_object(&bucket, &seg1_key, None)
        .await
        .expect("get seg1 after replay")
        .body
        .collect()
        .await
        .expect("body");
    assert_eq!(
        &replayed[..],
        &seg1_bytes[..],
        "crash replay must re-PUT the segment byte-identical"
    );
    let head = client.head_object(&bucket, &seg1_key).await.expect("head");
    assert_eq!(
        head.e_tag,
        md5_hex(&seg1_bytes),
        "re-PUT happened (ETag = md5 of frozen bytes)"
    );
    assert_ne!(
        head.e_tag,
        md5_hex(&garbage),
        "the tampered object was overwritten"
    );

    // The reader that saw the first PUT does not diverge: re-polling
    // applies only the new entries, exactly once.
    poll(&db_b, &client, &bucket, &mut consumer)
        .await
        .expect("B re-poll");
    let seen: Vec<u64> = consumer
        .transcript
        .iter()
        .map(|(_, seq, _, _)| *seq)
        .collect();
    assert_eq!(
        seen,
        vec![1, 2, 3, 4, 5],
        "no re-application, no divergence"
    );
    assert_eq!(db_b.cursor(&a).expect("cursor"), 5);
}

// ---------------------------------------------------------------------------
// Device registry heartbeat (§1.2, server time §2.10)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn device_registry_entry_roundtrips_and_records_server_time_offset() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("dev-registry");
    let client = g.client();
    let a = dev(DEV_A);
    let b = dev(DEV_B);
    let (_dir, _path, db) = open_db(&a);
    db.set_cursor(&b, 7).expect("cursor");
    assert_eq!(
        db.server_time_offset_ms().expect("meta"),
        None,
        "no offset before first heartbeat"
    );

    // The new S3 surface the heartbeat rides on: Garage answers PUTs with
    // a Date header.
    let probe = client
        .put_object(
            &bucket,
            "probe",
            Bytes::from_static(b"x"),
            &PutObjectOptions::default(),
        )
        .await
        .expect("probe put");
    assert!(
        probe.date.is_some(),
        "Garage must answer with a Date header"
    );

    let profile = DeviceProfile {
        name: "desk".to_string(),
        platform: "linux".to_string(),
        created: 1_700_000_000,
    };
    let written = put_device_entry(&db, &client, &bucket, &profile)
        .await
        .expect("heartbeat");
    assert_eq!(written.name, "desk");
    assert_eq!(written.platform, "linux");
    assert_eq!(written.created, 1_700_000_000);
    assert_eq!(written.applied, [(b.clone(), 7u64)].into_iter().collect());
    assert_eq!(written.proto.read, PROTO_READ.to_vec());
    assert_eq!(written.proto.write, PROTO_WRITE);

    // Server-time offset recorded in meta, and sane for a same-host
    // server (HTTP Date has 1 s granularity; allow generous CI slack).
    let offset = db
        .server_time_offset_ms()
        .expect("meta")
        .expect("offset recorded by the heartbeat");
    assert!(
        offset.abs() < 60_000,
        "same-host offset out of sanity bounds: {offset} ms"
    );
    let local_now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_secs() as i64;
    assert!(
        (written.last_seen_server_ts - local_now).abs() < 60,
        "last_seen_server_ts {} vs local {local_now}",
        written.last_seen_server_ts
    );

    // Round-trips through GET, exactly.
    let fetched = get_device_entry(&client, &bucket, &a)
        .await
        .expect("get entry");
    assert_eq!(fetched, written);

    // Wire shape pinned against §1.2: key and field spellings.
    let raw = client
        .get_object(&bucket, &device_registry_key(&a), None)
        .await
        .expect("raw get")
        .body
        .collect()
        .await
        .expect("body");
    let json: serde_json::Value = serde_json::from_slice(&raw).expect("valid JSON");
    assert_eq!(json["name"], "desk");
    assert_eq!(json["platform"], "linux");
    assert_eq!(json["created"], 1_700_000_000);
    assert!(json["last_seen_server_ts"].is_i64());
    assert_eq!(json["applied"][DEV_B], 7);
    assert_eq!(json["proto"]["read"], serde_json::json!([1]));
    assert_eq!(json["proto"]["write"], 1);
}

/// An [`S3Api`] wrapper that rewrites the `Date` header of PUT responses
/// (`None` strips it), for pinning the [`PublisherError::NoServerDate`]
/// surface without a backend that misbehaves.
struct DateOverrideS3 {
    inner: S3Client,
    date: Option<String>,
}

impl S3Api for DateOverrideS3 {
    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: Bytes,
        opts: &PutObjectOptions,
    ) -> Result<PutObjectOutput, S3Error> {
        let mut out = self.inner.put_object(bucket, key, body, opts).await?;
        out.date = self.date.clone();
        Ok(out)
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

#[tokio::test]
async fn heartbeat_without_a_usable_date_header_is_a_typed_error_and_records_no_offset() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("dev-no-date");
    let a = dev(DEV_A);
    let profile = DeviceProfile {
        name: "desk".to_string(),
        platform: "linux".to_string(),
        created: 1_700_000_000,
    };

    // Absent Date header.
    let (_dir, _path, db) = open_db(&a);
    let stripped = DateOverrideS3 {
        inner: g.client(),
        date: None,
    };
    let err = put_device_entry(&db, &stripped, &bucket, &profile)
        .await
        .expect_err("no Date header must surface typed");
    match &err {
        PublisherError::NoServerDate { reason } => {
            assert!(reason.contains("absent"), "reason: {reason}")
        }
        other => panic!("expected NoServerDate, got {other:?}"),
    }
    assert_eq!(
        db.server_time_offset_ms().expect("meta"),
        None,
        "no bogus offset recorded"
    );

    // Unparsable Date header (an obsolete rfc850 spelling).
    let garbled = DateOverrideS3 {
        inner: g.client(),
        date: Some("Sunday, 06-Nov-94 08:49:37 GMT".to_string()),
    };
    let err = put_device_entry(&db, &garbled, &bucket, &profile)
        .await
        .expect_err("unparsable Date header must surface typed");
    match &err {
        PublisherError::NoServerDate { reason } => {
            assert!(reason.contains("unparsable"), "reason: {reason}")
        }
        other => panic!("expected NoServerDate, got {other:?}"),
    }
    assert_eq!(db.server_time_offset_ms().expect("meta"), None);
}

#[tokio::test]
async fn an_oversized_registry_entry_is_refused_before_buffering() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("dev-oversized");
    let client = g.client();
    let a = dev(DEV_A);

    // A corrupt or hostile devices/<id>.json far over the cap: the only
    // other network lanes in the unit (segments, manifests) already bound
    // their buffers, and this one must too — typed, before (and while)
    // buffering, never collected wholesale.
    let oversized = vec![b'x'; DEVICE_ENTRY_MAX_BYTES + 10];
    client
        .put_object(
            &bucket,
            &device_registry_key(&a),
            Bytes::from(oversized),
            &PutObjectOptions::default(),
        )
        .await
        .expect("oversized put");

    let err = get_device_entry(&client, &bucket, &a)
        .await
        .expect_err("an oversized registry entry must be refused");
    match err {
        PublisherError::OversizedRegistryEntry { declared } => {
            assert_eq!(declared, DEVICE_ENTRY_MAX_BYTES as u64 + 10);
        }
        other => panic!("expected OversizedRegistryEntry, got {other:?}"),
    }
}

#[tokio::test]
async fn a_malformed_registry_entry_is_a_typed_wire_error_not_a_state_db_error() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("dev-malformed");
    let client = g.client();
    let a = dev(DEV_A);

    // A corrupt stored devices/<id>.json is a persistent per-OBJECT
    // condition (the CorruptSegment analogue on the registry lane), not a
    // local state-store failure: surfacing it as PublisherError::State
    // (Codec) read as "my redb is broken" — abort-the-pass territory —
    // when the true condition is "this stored object is malformed".
    client
        .put_object(
            &bucket,
            &device_registry_key(&a),
            Bytes::from_static(b"{\"name\": not-json"),
            &PutObjectOptions::default(),
        )
        .await
        .expect("garbage put");

    let err = get_device_entry(&client, &bucket, &a)
        .await
        .expect_err("a malformed registry entry must be refused, typed");
    match err {
        PublisherError::MalformedRegistryEntry { .. } => {}
        other => panic!("expected MalformedRegistryEntry, got {other:?}"),
    }
}
