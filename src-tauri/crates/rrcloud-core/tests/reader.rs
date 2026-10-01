//! Failing tests for `rrcloud_core::reader` (architecture §2.2): the
//! inbound poll — exactly-once in-order application through a consumer,
//! fail-closed version halts per device prefix, pinned gap semantics,
//! apply atomicity (consumer work + applied mark + cursor in one state
//! transaction), and the steady-state polling cost.
//!
//! All Garage-backed (the lane under test is the network lane); device A
//! publishes through the real publisher, while "foreign" devices X and Y
//! are crafted byte-level through the raw client.

mod common;

use bytes::Bytes;
use common::garage;
use common::sync::{
    dev, open_db, probe_relkey, put_raw_segment, sidecar_entries, stamped, FakeS3,
    RecordingConsumer, DEV_A, DEV_B, DEV_C, DEV_X, DEV_Y,
};
use futures::FutureExt as _;
use rrcloud_core::clock::DeviceId;
use rrcloud_core::keys::{journal_segment_key, CONTROL_PREFIX};
use rrcloud_core::publisher::{enqueue_entry, publish_pending};
use rrcloud_core::reader::{
    poll, CorruptSegment, FetchFailed, GapDetected, MidStreamGap, PrefixHalted, ReaderError,
};
use rrcloud_core::s3::PutObjectOptions;
use rrcloud_core::state::SyncDb;

/// Seqs applied for `device`, in transcript order.
fn seqs_for(consumer: &RecordingConsumer, device: &DeviceId) -> Vec<u64> {
    consumer
        .transcript
        .iter()
        .filter(|(d, _, _, _)| d == device)
        .map(|(_, seq, _, _)| *seq)
        .collect()
}

/// Publishes `count` entries from `db` (device A pattern) as one segment.
async fn publish_batch(
    db: &SyncDb,
    client: &rrcloud_core::s3::S3Client,
    bucket: &str,
    device: &DeviceId,
    tag: &str,
    count: usize,
) {
    for e in &sidecar_entries(device, tag, count) {
        enqueue_entry(db, e).expect("enqueue");
    }
    publish_pending(db, client, bucket).await.expect("publish");
}

// ---------------------------------------------------------------------------
// Two-device happy path
// ---------------------------------------------------------------------------

#[tokio::test]
async fn two_devices_apply_in_order_exactly_once_and_catch_up_incrementally() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("rdr-two-device");
    let client = g.client();
    let a = dev(DEV_A);
    let b = dev(DEV_B);
    let (_adir, _apath, db_a) = open_db(&a);
    let (_bdir, _bpath, db_b) = open_db(&b);

    // A publishes 3 segments (2 entries each: seqs 1-2, 3-4, 5-6).
    publish_batch(&db_a, &client, &bucket, &a, "t1", 2).await;
    publish_batch(&db_a, &client, &bucket, &a, "t2", 2).await;
    publish_batch(&db_a, &client, &bucket, &a, "t3", 2).await;

    // B applies all entries, in order, exactly once.
    let mut consumer = RecordingConsumer::default();
    let report = poll(&db_b, &client, &bucket, &mut consumer)
        .await
        .expect("poll");
    assert_eq!(report.entries_applied, 6);
    assert!(
        report.halted.is_empty() && report.gaps.is_empty() && report.mid_stream_gaps.is_empty()
    );
    assert_eq!(seqs_for(&consumer, &a), vec![1, 2, 3, 4, 5, 6]);
    assert_eq!(db_b.cursor(&a).expect("cursor"), 6);
    for seq in 1..=6 {
        assert!(db_b.has_applied(&a, seq).expect("applied"), "seq {seq}");
    }

    // Second poll: nothing new, nothing re-applied.
    let report = poll(&db_b, &client, &bucket, &mut consumer)
        .await
        .expect("re-poll");
    assert_eq!(report.entries_applied, 0);
    assert_eq!(consumer.transcript.len(), 6, "no re-application");

    // A publishes more; B catches up incrementally.
    publish_batch(&db_a, &client, &bucket, &a, "t4", 2).await;
    let report = poll(&db_b, &client, &bucket, &mut consumer)
        .await
        .expect("catch-up");
    assert_eq!(report.entries_applied, 2);
    assert_eq!(seqs_for(&consumer, &a), vec![1, 2, 3, 4, 5, 6, 7, 8]);
    assert_eq!(db_b.cursor(&a).expect("cursor"), 8);
}

#[tokio::test]
async fn own_segments_and_non_journal_keys_are_ignored() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("rdr-own");
    let client = g.client();
    let a = dev(DEV_A);
    let (_adir, _apath, db_a) = open_db(&a);
    publish_batch(&db_a, &client, &bucket, &a, "own", 2).await;

    // Junk under the journal prefix (classifies Foreign) must be ignored,
    // not errored on.
    client
        .put_object(
            &bucket,
            &format!("{CONTROL_PREFIX}journal/README.txt"),
            Bytes::from_static(b"not a segment"),
            &PutObjectOptions::default(),
        )
        .await
        .expect("junk put");

    // A device never applies its own prefix.
    let mut consumer = RecordingConsumer::default();
    let report = poll(&db_a, &client, &bucket, &mut consumer)
        .await
        .expect("self poll");
    assert_eq!(report.entries_applied, 0);
    assert!(consumer.transcript.is_empty());
    assert_eq!(db_a.cursor(&a).expect("cursor"), 0, "own cursor untouched");
}

// ---------------------------------------------------------------------------
// Fail-closed version halts (§2.2 min-reader rule)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn unsupported_segment_filename_version_halts_that_prefix_only() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("rdr-v2-name");
    let client = g.client();
    let b = dev(DEV_B);
    let x = dev(DEV_X);
    let y = dev(DEV_Y);
    let (_bdir, _bpath, db_b) = open_db(&b);

    // X: good seqs 1-2, then a v2-NAMED segment at seq 3, then a v1
    // segment at seq 5 that must NOT be applied (the prefix is halted).
    put_raw_segment(
        &client,
        &bucket,
        &x,
        1,
        &stamped(sidecar_entries(&x, "x1", 2), 1),
    )
    .await;
    let v2_key = format!("{CONTROL_PREFIX}journal/{x}/{:016x}.v2.ndjson", 3);
    client
        .put_object(
            &bucket,
            &v2_key,
            Bytes::from_static(b"future format\n"),
            &PutObjectOptions::default(),
        )
        .await
        .expect("v2 segment put");
    put_raw_segment(
        &client,
        &bucket,
        &x,
        5,
        &stamped(sidecar_entries(&x, "x2", 1), 5),
    )
    .await;
    // Y: unaffected good segment.
    put_raw_segment(
        &client,
        &bucket,
        &y,
        1,
        &stamped(sidecar_entries(&y, "y1", 2), 1),
    )
    .await;

    let fake = FakeS3::new(g.client());
    let mut consumer = RecordingConsumer::default();
    let report = poll(&db_b, &fake, &bucket, &mut consumer)
        .await
        .expect("poll");
    assert_eq!(
        report.halted,
        vec![PrefixHalted {
            device: x.clone(),
            version: 2
        }],
        "X's prefix halts with the typed error"
    );
    assert_eq!(
        seqs_for(&consumer, &x),
        vec![1, 2],
        "X stops at the last good seq"
    );
    assert_eq!(seqs_for(&consumer, &y), vec![1, 2], "Y's prefix continues");
    assert_eq!(
        db_b.cursor(&x).expect("cursor"),
        2,
        "X's cursor never passes the halt"
    );
    assert_eq!(db_b.cursor(&y).expect("cursor"), 2);
    assert!(
        !fake.get_keys().contains(&v2_key),
        "the min-reader gate runs on the filename, BEFORE any GET of the v2 segment"
    );

    // Re-polling stays halted (and still applies nothing new).
    let report = poll(&db_b, &client, &bucket, &mut consumer)
        .await
        .expect("re-poll");
    assert_eq!(
        report.halted,
        vec![PrefixHalted {
            device: x.clone(),
            version: 2
        }]
    );
    assert_eq!(consumer.transcript.len(), 4);
}

#[tokio::test]
async fn unsupported_entry_version_inside_v1_segment_halts_then_resumes_after_replacement() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("rdr-v2-entry");
    let client = g.client();
    let b = dev(DEV_B);
    let x = dev(DEV_X);
    let (_bdir, _bpath, db_b) = open_db(&b);

    put_raw_segment(
        &client,
        &bucket,
        &x,
        1,
        &stamped(sidecar_entries(&x, "e1", 2), 1),
    )
    .await;

    // A v1-NAMED segment at seq 3 whose second line declares v:2: the
    // fail-closed decode refuses the whole segment, so even its valid
    // first line must not be applied.
    let good_line = stamped(sidecar_entries(&x, "e2", 1), 3)[0]
        .to_json_line()
        .expect("encode line");
    let poisoned = format!("{good_line}\n{{\"v\":2,\"future\":true}}\n");
    let seg3_key = journal_segment_key(&x, 3);
    client
        .put_object(
            &bucket,
            &seg3_key,
            Bytes::from(poisoned),
            &PutObjectOptions::default(),
        )
        .await
        .expect("poisoned segment put");

    let mut consumer = RecordingConsumer::default();
    let report = poll(&db_b, &client, &bucket, &mut consumer)
        .await
        .expect("poll");
    assert_eq!(
        report.halted,
        vec![PrefixHalted {
            device: x.clone(),
            version: 2
        }]
    );
    assert_eq!(
        seqs_for(&consumer, &x),
        vec![1, 2],
        "nothing from the poisoned segment applies — not even its valid first entry"
    );
    assert_eq!(db_b.cursor(&x).expect("cursor"), 2);
    assert!(!db_b.has_applied(&x, 3).expect("applied"));

    // The bad segment is REPLACED by a valid one at the SAME key (§2.2:
    // the writer republishes after fixing itself); polling resumes
    // exactly where it halted.
    let fixed = stamped(sidecar_entries(&x, "e2fixed", 2), 3);
    put_raw_segment(&client, &bucket, &x, 3, &fixed).await;
    let report = poll(&db_b, &client, &bucket, &mut consumer)
        .await
        .expect("re-poll");
    assert!(
        report.halted.is_empty(),
        "halt clears once the segment is readable"
    );
    assert_eq!(report.entries_applied, 2);
    assert_eq!(seqs_for(&consumer, &x), vec![1, 2, 3, 4]);
    assert_eq!(db_b.cursor(&x).expect("cursor"), 4);
}

// ---------------------------------------------------------------------------
// Apply atomicity (§2.2: consumer work + applied mark share one txn)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn consumer_error_aborts_entry_txn_and_repoll_retries_exactly_that_entry() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("rdr-atomic-err");
    let client = g.client();
    let a = dev(DEV_A);
    let b = dev(DEV_B);
    let (_adir, _apath, db_a) = open_db(&a);
    let (_bdir, _bpath, db_b) = open_db(&b);
    publish_batch(&db_a, &client, &bucket, &a, "atomic", 3).await;

    // The consumer writes a durable probe item for every entry, then
    // fails on seq 2: the probe and the applied mark must BOTH roll back.
    let mut consumer = RecordingConsumer {
        fail_on: Some((a.clone(), 2)),
        probe_items: true,
        ..Default::default()
    };
    let err = poll(&db_b, &client, &bucket, &mut consumer)
        .await
        .expect_err("consumer failure must surface");
    match &err {
        ReaderError::Consumer { device, seq, .. } => {
            assert_eq!(device, &a);
            assert_eq!(*seq, 2);
        }
        other => panic!("expected ReaderError::Consumer, got {other:?}"),
    }
    assert_eq!(
        seqs_for(&consumer, &a),
        vec![1],
        "entries below the failure stay applied"
    );
    assert!(db_b.has_applied(&a, 1).expect("applied"));
    assert!(
        !db_b.has_applied(&a, 2).expect("applied"),
        "failed entry left unapplied"
    );
    assert_eq!(
        db_b.cursor(&a).expect("cursor"),
        1,
        "cursor reflects the k-1 position"
    );
    assert!(
        db_b.get_item(&probe_relkey(&a, 1)).expect("get").is_some(),
        "entry 1's consumer mutation committed"
    );
    assert!(
        db_b.get_item(&probe_relkey(&a, 2)).expect("get").is_none(),
        "entry 2's consumer mutation rolled back with the applied mark (one txn)"
    );

    // Re-poll with a healthy consumer: retries entry 2 exactly, then 3.
    let mut consumer = RecordingConsumer {
        probe_items: true,
        ..Default::default()
    };
    let report = poll(&db_b, &client, &bucket, &mut consumer)
        .await
        .expect("re-poll");
    assert_eq!(report.entries_applied, 2);
    assert_eq!(
        seqs_for(&consumer, &a),
        vec![2, 3],
        "entry 1 is not re-applied"
    );
    assert_eq!(db_b.cursor(&a).expect("cursor"), 3);
    assert!(db_b.get_item(&probe_relkey(&a, 2)).expect("get").is_some());
}

#[tokio::test]
async fn consumer_panic_aborts_entry_txn_leaving_neither_mutation_nor_applied_mark() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("rdr-atomic-panic");
    let client = g.client();
    let a = dev(DEV_A);
    let b = dev(DEV_B);
    let (_adir, _apath, db_a) = open_db(&a);
    let (_bdir, _bpath, db_b) = open_db(&b);
    publish_batch(&db_a, &client, &bucket, &a, "panic", 3).await;

    // Crash simulation: the consumer performs its durable mutation and
    // then panics mid-apply. The unwound transaction must leave NEITHER
    // the item mutation NOR the applied mark.
    let mut consumer = RecordingConsumer {
        panic_on: Some((a.clone(), 2)),
        probe_items: true,
        ..Default::default()
    };
    let outcome = std::panic::AssertUnwindSafe(poll(&db_b, &client, &bucket, &mut consumer))
        .catch_unwind()
        .await;
    assert!(outcome.is_err(), "the injected panic must propagate");

    assert!(db_b.has_applied(&a, 1).expect("applied"));
    assert!(
        !db_b.has_applied(&a, 2).expect("applied"),
        "no applied mark for the panicked entry"
    );
    assert_eq!(db_b.cursor(&a).expect("cursor"), 1);
    assert!(db_b.get_item(&probe_relkey(&a, 1)).expect("get").is_some());
    assert!(
        db_b.get_item(&probe_relkey(&a, 2)).expect("get").is_none(),
        "the panicked entry's consumer mutation must not survive"
    );

    // The db is not wedged: a healthy re-poll finishes the prefix.
    let mut consumer = RecordingConsumer::default();
    let report = poll(&db_b, &client, &bucket, &mut consumer)
        .await
        .expect("re-poll");
    assert_eq!(report.entries_applied, 2);
    assert_eq!(seqs_for(&consumer, &a), vec![2, 3]);
    assert_eq!(db_b.cursor(&a).expect("cursor"), 3);
}

// ---------------------------------------------------------------------------
// Gap semantics (pinned)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bootstrap_gap_below_lowest_segment_is_accepted_and_reported() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("rdr-gap-boot");
    let client = g.client();
    let b = dev(DEV_B);
    let x = dev(DEV_X);
    let (_bdir, _bpath, db_b) = open_db(&b);

    // X's journal was compacted: it starts at seq 5.
    put_raw_segment(
        &client,
        &bucket,
        &x,
        5,
        &stamped(sidecar_entries(&x, "boot", 2), 5),
    )
    .await;

    let mut consumer = RecordingConsumer::default();
    let report = poll(&db_b, &client, &bucket, &mut consumer)
        .await
        .expect("poll");
    assert_eq!(
        report.gaps,
        vec![GapDetected {
            device: x.clone(),
            lowest_seq: 5
        }],
        "cursor 0 + journal starting past 1 is the typed bootstrap outcome"
    );
    assert!(report.mid_stream_gaps.is_empty());
    assert_eq!(
        seqs_for(&consumer, &x),
        vec![5, 6],
        "present segments ARE applied in order"
    );
    assert_eq!(
        db_b.cursor(&x).expect("cursor"),
        6,
        "cursor jumps via contiguous application"
    );

    // Settled: a re-poll reports no gap and applies nothing new.
    let report = poll(&db_b, &client, &bucket, &mut consumer)
        .await
        .expect("re-poll");
    assert!(report.gaps.is_empty());
    assert_eq!(report.entries_applied, 0);
}

#[tokio::test]
async fn mid_stream_gap_is_typed_never_skipped_and_heals_when_filled() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("rdr-gap-mid");
    let client = g.client();
    let b = dev(DEV_B);
    let y = dev(DEV_Y);
    let (_bdir, _bpath, db_b) = open_db(&b);

    put_raw_segment(
        &client,
        &bucket,
        &y,
        1,
        &stamped(sidecar_entries(&y, "m1", 2), 1),
    )
    .await;
    let mut consumer = RecordingConsumer::default();
    poll(&db_b, &client, &bucket, &mut consumer)
        .await
        .expect("poll");
    assert_eq!(db_b.cursor(&y).expect("cursor"), 2);

    // Seqs 3-4 are missing; a segment appears at 5. The reader must NOT
    // skip: typed outcome, no application, cursor pinned.
    put_raw_segment(
        &client,
        &bucket,
        &y,
        5,
        &stamped(sidecar_entries(&y, "m3", 2), 5),
    )
    .await;
    let report = poll(&db_b, &client, &bucket, &mut consumer)
        .await
        .expect("poll");
    assert_eq!(
        report.mid_stream_gaps,
        vec![MidStreamGap {
            device: y.clone(),
            cursor: 2,
            next_seq: 5
        }]
    );
    assert!(
        report.gaps.is_empty(),
        "a mid-stream gap is never the bootstrap outcome"
    );
    assert_eq!(
        seqs_for(&consumer, &y),
        vec![1, 2],
        "nothing past the gap applies"
    );
    assert_eq!(
        db_b.cursor(&y).expect("cursor"),
        2,
        "the cursor never jumps a gap"
    );

    // The missing segment appears (slow upload landed): contiguous
    // application resumes across both segments in order.
    put_raw_segment(
        &client,
        &bucket,
        &y,
        3,
        &stamped(sidecar_entries(&y, "m2", 2), 3),
    )
    .await;
    let report = poll(&db_b, &client, &bucket, &mut consumer)
        .await
        .expect("re-poll");
    assert!(report.mid_stream_gaps.is_empty());
    assert_eq!(report.entries_applied, 4);
    assert_eq!(seqs_for(&consumer, &y), vec![1, 2, 3, 4, 5, 6]);
    assert_eq!(db_b.cursor(&y).expect("cursor"), 6);
}

// ---------------------------------------------------------------------------
// Segment-body validation (fail-closed at entry granularity)
// ---------------------------------------------------------------------------

/// `pred` holds for exactly one corrupt-segment outcome, which names
/// `(device, seq)`.
fn assert_one_corrupt(report: &[CorruptSegment], device: &DeviceId, seq: u64) {
    assert_eq!(report.len(), 1, "exactly one corrupt outcome: {report:?}");
    assert_eq!(report[0].device, *device);
    assert_eq!(report[0].seq, seq);
    assert!(!report[0].detail.is_empty());
}

#[tokio::test]
async fn entry_seq_gaps_inside_a_segment_are_corrupt_never_silently_skipped() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("rdr-seq-gap");
    let client = g.client();
    let b = dev(DEV_B);
    let x = dev(DEV_X);
    let (_bdir, _bpath, db_b) = open_db(&b);

    // A segment at filename seq 1 whose entries are stamped 1 and 5: seqs
    // 2-4 exist nowhere, and applying entry 5 would cover them with the
    // cursor forever. Fail closed instead.
    let mut entries = sidecar_entries(&x, "gap", 2);
    entries[0].seq = 1;
    entries[1].seq = 5;
    put_raw_segment(&client, &bucket, &x, 1, &entries).await;

    let mut consumer = RecordingConsumer::default();
    let report = poll(&db_b, &client, &bucket, &mut consumer)
        .await
        .expect("poll");
    assert_one_corrupt(&report.corrupt, &x, 1);
    assert_eq!(report.entries_applied, 0);
    assert!(report.gaps.is_empty() && report.mid_stream_gaps.is_empty());
    assert!(
        consumer.transcript.is_empty(),
        "nothing from a seq-discontiguous segment applies"
    );
    assert_eq!(
        db_b.cursor(&x).expect("cursor"),
        0,
        "the cursor must never advance over never-applied seqs"
    );
    assert!(!db_b.has_applied(&x, 1).expect("applied"));
    assert!(!db_b.has_applied(&x, 5).expect("applied"));
}

#[tokio::test]
async fn first_entry_seq_mismatching_the_filename_seq_is_corrupt() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("rdr-seq-name");
    let client = g.client();
    let b = dev(DEV_B);
    let x = dev(DEV_X);
    let (_bdir, _bpath, db_b) = open_db(&b);

    // Filename says seq 1; the entries claim 100,101. Applying them would
    // stamp the cursor at 101 with seqs 1-99 silently skipped.
    put_raw_segment(
        &client,
        &bucket,
        &x,
        1,
        &stamped(sidecar_entries(&x, "mis", 2), 100),
    )
    .await;

    let mut consumer = RecordingConsumer::default();
    let report = poll(&db_b, &client, &bucket, &mut consumer)
        .await
        .expect("poll");
    assert_one_corrupt(&report.corrupt, &x, 1);
    assert!(consumer.transcript.is_empty());
    assert_eq!(db_b.cursor(&x).expect("cursor"), 0);
}

#[tokio::test]
async fn entry_device_mismatching_the_prefix_owner_is_corrupt() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("rdr-dev-mis");
    let client = g.client();
    let b = dev(DEV_B);
    let c = dev(DEV_C);
    let x = dev(DEV_X);
    let (_bdir, _bpath, db_b) = open_db(&b);

    // A segment under X's prefix whose entries claim device C (§2.2
    // single-writer: entry.device must equal the prefix owner). Applying
    // it would feed the consumer forged attribution while dedup/cursor
    // bookkeeping ran under X.
    put_raw_segment(
        &client,
        &bucket,
        &x,
        1,
        &stamped(sidecar_entries(&c, "forged", 2), 1),
    )
    .await;

    let mut consumer = RecordingConsumer::default();
    let report = poll(&db_b, &client, &bucket, &mut consumer)
        .await
        .expect("poll");
    assert_one_corrupt(&report.corrupt, &x, 1);
    assert!(consumer.transcript.is_empty(), "the consumer never sees it");
    assert_eq!(db_b.cursor(&x).expect("cursor"), 0);
    assert_eq!(db_b.cursor(&c).expect("cursor"), 0);
}

#[tokio::test]
async fn corrupt_and_unfetchable_segments_isolate_to_their_device() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("rdr-isolate");
    let client = g.client();
    let b = dev(DEV_B);
    let c = dev(DEV_C); // c0ffee… sorts before deadbeef…
    let x = dev(DEV_X);
    let (_bdir, _bpath, db_b) = open_db(&b);

    // C's segment is bit-rotted (malformed JSON, not a version halt); X's
    // is healthy and sorts after C. The pass must complete X.
    client
        .put_object(
            &bucket,
            &journal_segment_key(&c, 1),
            Bytes::from_static(b"this is not NDJSON {\n"),
            &PutObjectOptions::default(),
        )
        .await
        .expect("corrupt put");
    put_raw_segment(
        &client,
        &bucket,
        &x,
        1,
        &stamped(sidecar_entries(&x, "ok", 2), 1),
    )
    .await;

    let mut consumer = RecordingConsumer::default();
    let report = poll(&db_b, &client, &bucket, &mut consumer)
        .await
        .expect("a corrupt segment must not abort the pass");
    assert_one_corrupt(&report.corrupt, &c, 1);
    assert_eq!(
        seqs_for(&consumer, &x),
        vec![1, 2],
        "the later-sorted healthy device still applies"
    );
    assert_eq!(db_b.cursor(&x).expect("cursor"), 2);
    assert_eq!(db_b.cursor(&c).expect("cursor"), 0);

    // The condition persists across polls without starving X.
    let report = poll(&db_b, &client, &bucket, &mut consumer)
        .await
        .expect("re-poll");
    assert_one_corrupt(&report.corrupt, &c, 1);
    assert_eq!(report.entries_applied, 0);
}

#[tokio::test]
async fn failing_get_isolates_to_its_device_and_recovers() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("rdr-get-fail");
    let client = g.client();
    let b = dev(DEV_B);
    let c = dev(DEV_C);
    let x = dev(DEV_X);
    let (_bdir, _bpath, db_b) = open_db(&b);

    let c_key = put_raw_segment(
        &client,
        &bucket,
        &c,
        1,
        &stamped(sidecar_entries(&c, "c", 2), 1),
    )
    .await;
    put_raw_segment(
        &client,
        &bucket,
        &x,
        1,
        &stamped(sidecar_entries(&x, "x", 2), 1),
    )
    .await;

    let mut fake = FakeS3::new(g.client());
    fake.fail_gets.insert(c_key);
    let mut consumer = RecordingConsumer::default();
    let report = poll(&db_b, &fake, &bucket, &mut consumer)
        .await
        .expect("a per-segment GET failure must not abort the pass");
    assert_eq!(report.fetch_failed.len(), 1, "{:?}", report.fetch_failed);
    assert_eq!(
        report.fetch_failed[0],
        FetchFailed {
            device: c.clone(),
            seq: 1,
            error: report.fetch_failed[0].error.clone(),
        }
    );
    assert!(!report.fetch_failed[0].error.is_empty());
    assert_eq!(seqs_for(&consumer, &x), vec![1, 2], "X is unaffected");
    assert_eq!(db_b.cursor(&c).expect("cursor"), 0);

    // The blight clears (here: a healthy client): C catches up.
    let report = poll(&db_b, &client, &bucket, &mut consumer)
        .await
        .expect("re-poll");
    assert!(report.fetch_failed.is_empty());
    assert_eq!(seqs_for(&consumer, &c), vec![1, 2]);
    assert_eq!(db_b.cursor(&c).expect("cursor"), 2);
}

#[tokio::test]
async fn oversized_segment_object_is_corrupt_without_unbounded_buffering() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("rdr-oversize");
    let client = g.client();
    let b = dev(DEV_B);
    let y = dev(DEV_Y);
    let (_bdir, _bpath, db_b) = open_db(&b);

    // No conforming writer emits a segment over SEGMENT_MAX_BYTES; a
    // bigger object at a segment key is corruption and must be refused —
    // typed, without the reader buffering it wholesale.
    let oversized = vec![b'\n'; rrcloud_core::journal::SEGMENT_MAX_BYTES + 10];
    client
        .put_object(
            &bucket,
            &journal_segment_key(&y, 1),
            Bytes::from(oversized),
            &PutObjectOptions::default(),
        )
        .await
        .expect("oversized put");

    let mut consumer = RecordingConsumer::default();
    let report = poll(&db_b, &client, &bucket, &mut consumer)
        .await
        .expect("poll");
    assert_one_corrupt(&report.corrupt, &y, 1);
    assert!(consumer.transcript.is_empty());
    assert_eq!(db_b.cursor(&y).expect("cursor"), 0);
}

// ---------------------------------------------------------------------------
// Steady-state polling cost (§2.2)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn steady_state_poll_issues_exactly_one_list_page_request() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("rdr-poll-cost");
    let client = g.client();
    let a = dev(DEV_A);
    let b = dev(DEV_B);
    let (_adir, _apath, db_a) = open_db(&a);
    let (_bdir, _bpath, db_b) = open_db(&b);
    publish_batch(&db_a, &client, &bucket, &a, "cost", 2).await;

    // First poll applies; counted separately.
    let fake = FakeS3::new(g.client());
    let mut consumer = RecordingConsumer::default();
    let report = poll(&db_b, &fake, &bucket, &mut consumer)
        .await
        .expect("first poll");
    assert_eq!(report.entries_applied, 2);
    assert_eq!(fake.list_count(), 1, "a single-page listing is one request");

    // Steady state: nothing changed — exactly ONE ListObjectsV2 request
    // and NO segment GETs (§2.2 prices the idle poll at one near-empty
    // page, not O(#devices) calls; a fully-applied segment's span is
    // recorded, so skipping it needs no re-fetch).
    let fake = FakeS3::new(g.client());
    let report = poll(&db_b, &fake, &bucket, &mut consumer)
        .await
        .expect("idle poll");
    assert_eq!(report.entries_applied, 0);
    assert_eq!(
        fake.list_count(),
        1,
        "the idle steady-state poll must cost exactly one ListObjectsV2 page request"
    );
    assert_eq!(
        fake.get_keys(),
        Vec::<String>::new(),
        "an idle poll must not re-GET already-applied segments"
    );
}

#[tokio::test]
async fn idle_polls_cost_no_gets_per_caught_up_device() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("rdr-idle-gets");
    let client = g.client();
    let b = dev(DEV_B);
    let x = dev(DEV_X);
    let y = dev(DEV_Y);
    let (_bdir, _bpath, db_b) = open_db(&b);

    // Two foreign devices, two segments each — the newest segment of each
    // device is the one the span-less skip rule could never prove covered.
    for d in [&x, &y] {
        put_raw_segment(
            &client,
            &bucket,
            d,
            1,
            &stamped(sidecar_entries(d, "s1", 2), 1),
        )
        .await;
        put_raw_segment(
            &client,
            &bucket,
            d,
            3,
            &stamped(sidecar_entries(d, "s2", 2), 3),
        )
        .await;
    }
    let mut consumer = RecordingConsumer::default();
    let report = poll(&db_b, &client, &bucket, &mut consumer)
        .await
        .expect("first poll");
    assert_eq!(report.entries_applied, 8);

    // Fully caught up: repeated idle polls must issue zero GETs — not one
    // per device's newest segment, every poll, forever.
    for _ in 0..2 {
        let fake = FakeS3::new(g.client());
        let report = poll(&db_b, &fake, &bucket, &mut consumer)
            .await
            .expect("idle poll");
        assert_eq!(report.entries_applied, 0);
        assert_eq!(fake.list_count(), 1);
        assert_eq!(
            fake.get_keys(),
            Vec::<String>::new(),
            "idle polls must not hide O(#devices) segment GETs"
        );
    }
}
