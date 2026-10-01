//! Failing tests for `rrcloud_core::manifest` (architecture §2.3): the
//! gzip NDJSON wire format (header-first, fail-closed, strict-wire
//! relkeys), build-from-db, S3 put/get + read-back HEAD verification, and
//! merge-through-the-consumer — including the §2.3 equivalence proof that
//! merging a manifest produces the same state as replaying the journal.

mod common;

use std::collections::BTreeMap;
use std::io::{Read as _, Write as _};

use common::garage;
use common::sync::{
    apply_entries_locally, dev, entry, open_db, rel, RecordingConsumer, ReplayConsumer, DEV_A,
    DEV_B, DEV_C, DEV_Y,
};
use rrcloud_core::clock::{DeviceId, VersionVector};
use rrcloud_core::journal::{Kind, Op};
use rrcloud_core::keys::{library_key, manifest_key, sidecar_key};
use rrcloud_core::manifest::{
    build_manifest, decode_manifest, encode_manifest, get_manifest, head_manifest_etag, merge,
    put_manifest, DeletedRow, Manifest, ManifestError, ManifestHeader, ManifestRow, MANIFEST_PROTO,
};
use rrcloud_core::publisher::{enqueue_entry, publish_pending};
use rrcloud_core::reader::poll;
use rrcloud_core::semhash::{Blake3Hex, ContentId, SemHash};
use rrcloud_core::state::{DeletedRecord, ItemRecord, ItemState};

const BLAKE3_HEX: &str = "4878ca0425c739fa427f7eda20fe845f6b2e46ba5fe2a14df5b1e32f50603215";
const SEMHASH_HEX: &str = "af1349b9f5f9a1a6a0404dee36dcc9499bcb25c9adc112b7cc9a93cae41f3262";
const CONTENT_HEX: &str = "0000ca0425c739fa427f7eda20fe845f6b2e46ba5fe2a14df5b1e32f50603215";

fn gzip(text: &str) -> Vec<u8> {
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    enc.write_all(text.as_bytes()).expect("gzip write");
    enc.finish().expect("gzip finish")
}

fn gunzip(bytes: &[u8]) -> String {
    let mut out = String::new();
    flate2::read::GzDecoder::new(bytes)
        .read_to_string(&mut out)
        .expect("gunzip");
    out
}

fn header(cursors: &[(&DeviceId, u64)]) -> ManifestHeader {
    ManifestHeader {
        written_server_ts: 1_769_950_000,
        cursors: cursors.iter().map(|(d, s)| ((*d).clone(), *s)).collect(),
        proto: MANIFEST_PROTO,
    }
}

fn live_row(key: &str, kind: Kind, device: &DeviceId) -> ManifestRow {
    ManifestRow {
        key: rel(key),
        kind,
        size: 48_213,
        blake3: Some(Blake3Hex::parse(BLAKE3_HEX).expect("blake3")),
        sem_hash: Some(SemHash::parse(SEMHASH_HEX).expect("semhash")),
        vv: [(device.clone(), 2u32)].into_iter().collect(),
        device: Some(device.clone()),
        content_id: None,
        w: None,
        h: None,
        mtime: Some(1_769_899_000),
        rating: Some(3),
        color_label: Some("red".to_string()),
    }
}

fn deleted_row(key: &str, device: &DeviceId) -> DeletedRow {
    DeletedRow {
        del: rel(key),
        vv: [(device.clone(), 3u32)].into_iter().collect(),
        server_ts: 1_769_940_000,
    }
}

// ---------------------------------------------------------------------------
// Wire format (no network)
// ---------------------------------------------------------------------------

#[test]
fn encode_is_gzip_ndjson_header_first_and_round_trips() {
    let a = dev(DEV_A);
    let b = dev(DEV_B);
    let manifest = Manifest {
        header: header(&[(&b, 4)]),
        rows: vec![
            live_row("2026/10/IMG_0042.NEF", Kind::Sidecar, &a),
            live_row("2026/10/IMG_0043.NEF", Kind::Original, &a),
        ],
        deleted: vec![deleted_row("2026/09/old.NEF", &a)],
    };
    let bytes = encode_manifest(&manifest).expect("encode");
    assert_eq!(
        &bytes[..2],
        &[0x1f, 0x8b],
        "gzip magic (§2.3: gzip NDJSON from v1)"
    );

    // Header-first NDJSON with the §2.3 spellings.
    let text = gunzip(&bytes);
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 4, "header + 2 live rows + 1 deleted row");
    let head: serde_json::Value = serde_json::from_str(lines[0]).expect("header json");
    assert_eq!(head["proto"], 1);
    assert_eq!(head["written_server_ts"], 1_769_950_000);
    assert_eq!(head["cursors"][DEV_B], 4);
    let row: serde_json::Value = serde_json::from_str(lines[1]).expect("row json");
    assert_eq!(row["key"], "2026/10/IMG_0042.NEF");
    assert_eq!(row["kind"], "sidecar");
    assert_eq!(row["rating"], 3);
    assert_eq!(row["color_label"], "red");
    let del: serde_json::Value = serde_json::from_str(lines[3]).expect("del json");
    assert_eq!(del["del"], "2026/09/old.NEF");
    assert_eq!(del["server_ts"], 1_769_940_000);
    assert!(
        del.get("key").is_none(),
        "deleted rows are keyed by 'del', not 'key'"
    );

    // Exact round trip.
    let decoded = decode_manifest(&bytes).expect("decode");
    assert_eq!(decoded, manifest);

    // Deterministic encoding (byte identity matters for read-back
    // verification by ETag).
    assert_eq!(encode_manifest(&manifest).expect("encode again"), bytes);
}

#[test]
fn decode_ignores_unknown_fields_within_proto_1() {
    // Min-reader rule inside a supported proto: a v1 writer with extra
    // optional fields must still decode.
    let text = concat!(
        "{\"written_server_ts\":1,\"cursors\":{},\"proto\":1,\"extra\":true}\n",
        "{\"key\":\"a.NEF\",\"kind\":\"original\",\"size\":5,\"vv\":{},\"novel_field\":1}\n",
        "{\"del\":\"b.NEF\",\"vv\":{},\"server_ts\":2,\"novel\":\"x\"}\n",
    );
    let m = decode_manifest(&gzip(text)).expect("decode with unknown optional fields");
    assert_eq!(m.rows.len(), 1);
    assert_eq!(m.rows[0].key, rel("a.NEF"));
    assert_eq!(m.deleted.len(), 1);
    assert_eq!(m.deleted[0].del, rel("b.NEF"));
}

#[test]
fn unknown_proto_fails_closed_before_any_row_decodes() {
    // A proto-2 manifest whose rows are garbage: the proto gate must fire
    // first — no partial result, no row decode error.
    let text = "{\"written_server_ts\":1,\"cursors\":{},\"proto\":2}\nTOTAL GARBAGE\n";
    let err = decode_manifest(&gzip(text)).expect_err("proto 2 must be refused");
    match err {
        ManifestError::UnsupportedProto { proto } => assert_eq!(proto, Some(2)),
        other => panic!("expected UnsupportedProto, got {other:?}"),
    }

    // A stringified proto is equally unreadable: fail closed, truthfully.
    let text = "{\"written_server_ts\":1,\"cursors\":{},\"proto\":\"1\"}\n";
    let err = decode_manifest(&gzip(text)).expect_err("non-integer proto must be refused");
    match err {
        ManifestError::UnsupportedProto { proto } => assert_eq!(proto, None),
        other => panic!("expected UnsupportedProto, got {other:?}"),
    }
}

#[test]
fn missing_or_misplaced_header_fails_closed() {
    // Empty document.
    let err = decode_manifest(&gzip("")).expect_err("empty manifest");
    assert!(matches!(err, ManifestError::MissingHeader), "got {err:?}");

    // A live row where the header belongs.
    let text = "{\"key\":\"a.NEF\",\"kind\":\"original\",\"size\":5,\"vv\":{}}\n";
    let err = decode_manifest(&gzip(text)).expect_err("row-first manifest");
    assert!(matches!(err, ManifestError::MissingHeader), "got {err:?}");

    // Not gzip at all.
    let err = decode_manifest(b"not gzip").expect_err("raw bytes");
    assert!(matches!(err, ManifestError::Gzip(_)), "got {err:?}");
}

#[test]
fn malformed_rows_abort_naming_their_line() {
    let text = concat!(
        "{\"written_server_ts\":1,\"cursors\":{},\"proto\":1}\n",
        "{\"key\":\"a.NEF\",\"kind\":\"original\",\"size\":5,\"vv\":{}}\n",
        "{\"key\":\"b.NEF\",\"kind\":\"not-a-kind\",\"size\":5,\"vv\":{}}\n",
    );
    let err = decode_manifest(&gzip(text)).expect_err("bad kind must abort");
    match err {
        ManifestError::Line { line, .. } => assert_eq!(line, 3),
        other => panic!("expected Line {{3}}, got {other:?}"),
    }
}

#[test]
fn non_nfc_relkeys_are_rejected_on_decode_never_normalized() {
    // NFD "café.NEF" — the strict wire lane (§1.1): a normalized decode
    // would re-aim the record at the distinct NFC object.
    let nfd = "cafe\u{301}.NEF";

    let text = format!(
        "{{\"written_server_ts\":1,\"cursors\":{{}},\"proto\":1}}\n{{\"del\":\"{nfd}\",\"vv\":{{}},\"server_ts\":2}}\n"
    );
    let err = decode_manifest(&gzip(&text)).expect_err("NFD deleted-set relkey must be refused");
    match err {
        ManifestError::Line { line, .. } => assert_eq!(line, 2),
        other => panic!("expected Line {{2}}, got {other:?}"),
    }

    let text = format!(
        "{{\"written_server_ts\":1,\"cursors\":{{}},\"proto\":1}}\n{{\"key\":\"{nfd}\",\"kind\":\"original\",\"size\":5,\"vv\":{{}}}}\n"
    );
    let err = decode_manifest(&gzip(&text)).expect_err("NFD live-row relkey must be refused");
    assert!(
        matches!(err, ManifestError::Line { line: 2, .. }),
        "got {err:?}"
    );
}

// ---------------------------------------------------------------------------
// Build from db (no network)
// ---------------------------------------------------------------------------

#[test]
fn build_manifest_snapshots_items_deleted_set_and_cursors() {
    let a = dev(DEV_A);
    let b = dev(DEV_B);
    let (_dir, _path, db) = open_db(&a);
    db.set_cursor(&b, 9).expect("cursor");

    let record = ItemRecord {
        kind: Kind::Original,
        state: ItemState::Synced,
        size: 31_457_280,
        mtime_unix_ns: 1_769_899_000_123_456_789, // truncates to 1_769_899_000 s
        blake3: Some(Blake3Hex::parse(BLAKE3_HEX).expect("blake3")),
        sem_hash: Some(SemHash::parse(SEMHASH_HEX).expect("semhash")),
        vv: [(a.clone(), 9u32), (b.clone(), 4u32)].into_iter().collect(),
        content_id: Some(ContentId::parse(CONTENT_HEX).expect("content id")),
        w: Some(6000),
        h: Some(4000),
        pinned: true,
        last_access_unix: 1,
        verified_remote: true,
        attested: true,
        base_unknown: false,
    };
    db.replay_put_item(&rel("z/last.NEF"), &record)
        .expect("put item");
    db.replay_put_item(&rel("a/first.NEF"), &record)
        .expect("put item");
    let deleted = DeletedRecord {
        vv: [(a.clone(), 3u32)].into_iter().collect(),
        server_ts: 1_769_940_000,
    };
    db.record_deleted(&rel("gone.NEF"), &deleted)
        .expect("record deleted");

    let manifest = build_manifest(&db, 1_769_950_111).expect("build");
    assert_eq!(manifest.header.proto, MANIFEST_PROTO);
    assert_eq!(manifest.header.written_server_ts, 1_769_950_111);
    assert_eq!(
        manifest.header.cursors,
        [(b.clone(), 9u64)].into_iter().collect::<BTreeMap<_, _>>()
    );

    assert_eq!(manifest.rows.len(), 2);
    let keys: Vec<&str> = manifest.rows.iter().map(|r| r.key.as_str()).collect();
    assert_eq!(
        keys,
        vec!["a/first.NEF", "z/last.NEF"],
        "live rows ascend by relkey"
    );
    let row = &manifest.rows[0];
    assert_eq!(row.kind, Kind::Original);
    assert_eq!(row.size, 31_457_280);
    assert_eq!(
        row.mtime,
        Some(1_769_899_000),
        "nanosecond mtime truncated to seconds"
    );
    assert_eq!(row.blake3, record.blake3);
    assert_eq!(row.sem_hash, record.sem_hash);
    assert_eq!(row.vv, record.vv);
    assert_eq!(row.content_id, record.content_id);
    assert_eq!(row.w, Some(6000));
    assert_eq!(row.h, Some(4000));
    // Fields ItemRecord v1 does not carry are honestly absent.
    assert_eq!(row.device, None);
    assert_eq!(row.rating, None);
    assert_eq!(row.color_label, None);

    assert_eq!(
        manifest.deleted,
        vec![DeletedRow {
            del: rel("gone.NEF"),
            vv: deleted.vv.clone(),
            server_ts: deleted.server_ts,
        }]
    );
}

// ---------------------------------------------------------------------------
// Merge through the consumer (no network)
// ---------------------------------------------------------------------------

#[test]
fn merge_propagates_deletions_the_receiver_never_saw_a_journal_entry_for() {
    let a = dev(DEV_A);
    let b = dev(DEV_B);
    let (_dir, _path, db_b) = open_db(&b);

    // A's manifest records a deletion; B has no journal history for it.
    let manifest = Manifest {
        header: header(&[]),
        rows: vec![],
        deleted: vec![deleted_row("2026/09/old.NEF", &a)],
    };
    let mut consumer = ReplayConsumer;
    let report = merge(&[(a.clone(), manifest)], &db_b, &mut consumer).expect("merge");
    assert_eq!(report.deleted_rows, 1);
    assert_eq!(report.live_rows, 0);

    let learned = db_b
        .get_deleted(&rel("2026/09/old.NEF"))
        .expect("get")
        .expect("B learned the deletion from the manifest alone");
    assert_eq!(
        learned.vv,
        [(a.clone(), 3u32)].into_iter().collect::<VersionVector>()
    );
    assert_eq!(learned.server_ts, 1_769_940_000);
}

#[test]
fn merge_applies_rows_in_relkey_order_as_synthetic_ops_and_seeds_max_cursors() {
    let a = dev(DEV_A);
    let b = dev(DEV_B);
    let c = dev(DEV_C);
    let y = dev(DEV_Y);
    let (_dir, _path, db) = open_db(&y);

    // Two manifests with interleaved relkeys and overlapping cursor
    // claims: rows must reach the consumer in ascending relkey order
    // across manifests, and cursors seed at the per-device max.
    let m1 = Manifest {
        header: header(&[(&b, 2), (&c, 7)]),
        rows: vec![
            live_row("a.NEF", Kind::Sidecar, &a),
            live_row("m.NEF", Kind::Original, &a),
        ],
        deleted: vec![deleted_row("zz-gone.NEF", &a)],
    };
    let m2 = Manifest {
        header: header(&[(&b, 5)]),
        rows: vec![live_row("g.NEF", Kind::Sidecar, &b)],
        deleted: vec![],
    };

    let mut consumer = RecordingConsumer::default();
    let report = merge(&[(a.clone(), m1), (b.clone(), m2)], &db, &mut consumer).expect("merge");
    assert_eq!(report.live_rows, 3);
    assert_eq!(report.deleted_rows, 1);
    assert_eq!(
        report.cursors,
        [(b.clone(), 5u64), (c.clone(), 7u64)]
            .into_iter()
            .collect::<BTreeMap<_, _>>()
    );
    assert_eq!(db.cursor(&b).expect("cursor"), 5, "max across headers wins");
    assert_eq!(db.cursor(&c).expect("cursor"), 7);

    // Synthetic entries: live rows ascending by relkey (across manifests)
    // then deleted rows; bucket keys rebuilt from (kind, relkey); ops
    // put/del; seq 0 and never marked applied.
    let seen: Vec<(String, Op, u64)> = consumer
        .transcript
        .iter()
        .map(|(_, seq, key, op)| (key.clone(), *op, *seq))
        .collect();
    assert_eq!(
        seen,
        vec![
            (sidecar_key(&rel("a.NEF")), Op::Put, 0),
            (sidecar_key(&rel("g.NEF")), Op::Put, 0),
            (library_key(&rel("m.NEF")), Op::Put, 0),
            (sidecar_key(&rel("zz-gone.NEF")), Op::Del, 0),
        ]
    );
    assert!(
        !db.has_applied(&a, 0).expect("applied"),
        "synthetic manifest entries are never marked applied"
    );
}

#[test]
fn merge_consumer_failure_aborts_the_whole_merge() {
    let a = dev(DEV_A);
    let b = dev(DEV_B);
    let (_dir, _path, db) = open_db(&b);
    let manifest = Manifest {
        header: header(&[(&a, 3)]),
        rows: vec![live_row("ok.NEF", Kind::Sidecar, &a)],
        deleted: vec![deleted_row("gone.NEF", &a)],
    };
    // A consumer that performs a durable mutation per row and then fails
    // on the deleted row: NOTHING of the merge may survive — rows,
    // deleted-set writes, or cursor seeding (one transaction).
    let mut consumer = RecordingConsumer {
        fail_on: Some((a.clone(), 0)),
        probe_items: true,
        ..Default::default()
    };
    // (seq 0 + fail_on device A hits the FIRST synthetic entry.)
    let err = merge(&[(a.clone(), manifest)], &db, &mut consumer)
        .expect_err("consumer failure must abort the merge");
    assert!(matches!(err, ManifestError::Consumer { .. }), "got {err:?}");
    assert_eq!(
        db.cursor(&a).expect("cursor"),
        0,
        "no cursor seeding on a failed merge"
    );
    assert!(
        db.iter_items().expect("items").is_empty(),
        "no probe mutation survives"
    );
    assert!(db.iter_deleted().expect("deleted").is_empty());
}

// ---------------------------------------------------------------------------
// S3 transfer (Garage)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn put_get_roundtrip_with_readback_head_etag_verification() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("man-roundtrip");
    let client = g.client();
    let a = dev(DEV_A);
    let b = dev(DEV_B);
    let manifest = Manifest {
        header: header(&[(&b, 4)]),
        rows: vec![live_row("2026/10/IMG_0042.NEF", Kind::Sidecar, &a)],
        deleted: vec![deleted_row("2026/09/old.NEF", &a)],
    };

    let put_etag = put_manifest(&client, &bucket, &a, &manifest)
        .await
        .expect("put");
    assert!(!put_etag.is_empty());

    // Read-back HEAD verify: the stored object's ETag matches the PUT's.
    let head_etag = head_manifest_etag(&client, &bucket, &a)
        .await
        .expect("head");
    assert_eq!(head_etag, put_etag);

    // Stored at the §1.2 key, as gzip.
    let raw = client
        .get_object(&bucket, &manifest_key(&a), None)
        .await
        .expect("raw get")
        .body
        .collect()
        .await
        .expect("body");
    assert_eq!(&raw[..2], &[0x1f, 0x8b]);

    let fetched = get_manifest(&client, &bucket, &a).await.expect("get");
    assert_eq!(fetched, manifest);
}

/// The §2.3 claim, proven: "rows are keyed by relkey and ordered by vv
/// exactly like journal entries — merging manifests is the same
/// idempotent apply operation as replaying the journal".
#[tokio::test]
async fn merging_a_manifest_equals_replaying_the_journal() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("man-equiv");
    let client = g.client();
    let a = dev(DEV_A);
    let b = dev(DEV_B);
    let c = dev(DEV_C);
    let y = dev(DEV_Y);

    // A's history: two live keys, one key put-then-deleted.
    let mut e1 = entry(&a, Op::Put, Kind::Original, library_key(&rel("p/one.NEF")));
    e1.size = Some(31_457_280);
    e1.blake3 = Some(Blake3Hex::parse(BLAKE3_HEX).expect("blake3"));
    e1.content_id = Some(ContentId::parse(CONTENT_HEX).expect("content id"));
    e1.w = Some(6000);
    e1.h = Some(4000);
    let mut e2 = entry(&a, Op::Put, Kind::Sidecar, sidecar_key(&rel("p/two.NEF")));
    e2.sem_hash = Some(SemHash::parse(SEMHASH_HEX).expect("semhash"));
    let e3 = entry(&a, Op::Put, Kind::Sidecar, sidecar_key(&rel("p/gone.NEF")));
    let mut e4 = entry(&a, Op::Del, Kind::Sidecar, sidecar_key(&rel("p/gone.NEF")));
    e4.vv = [(a.clone(), 2u32)].into_iter().collect();
    e4.ts = 1_769_941_000;
    let entries = vec![e1, e2, e3, e4];

    // A's own db state is these entries applied; A also knows C's cursor.
    let (_adir, _apath, db_a) = open_db(&a);
    apply_entries_locally(&db_a, &entries);
    db_a.set_cursor(&c, 7).expect("cursor");

    // A publishes the same history as journal segments...
    for e in &entries {
        enqueue_entry(&db_a, e).expect("enqueue");
    }
    publish_pending(&db_a, &client, &bucket)
        .await
        .expect("publish");
    // ...and as a manifest, through the full wire path.
    let manifest = build_manifest(&db_a, 1_769_950_000).expect("build");
    put_manifest(&client, &bucket, &a, &manifest)
        .await
        .expect("put manifest");
    let fetched = get_manifest(&client, &bucket, &a)
        .await
        .expect("get manifest");
    assert_eq!(fetched, manifest, "wire round trip");

    // Path 1 (B): journal replay.
    let (_bdir, _bpath, db_b) = open_db(&b);
    let mut replay = ReplayConsumer;
    poll(&db_b, &client, &bucket, &mut replay)
        .await
        .expect("B poll");

    // Path 2 (Y): manifest merge.
    let (_ydir, _ypath, db_y) = open_db(&y);
    let mut replay = ReplayConsumer;
    merge(&[(a.clone(), fetched)], &db_y, &mut replay).expect("merge");

    // Same idempotent apply ⇒ same state.
    assert_eq!(
        db_b.iter_items().expect("items"),
        db_y.iter_items().expect("items"),
        "items from merge == items from journal replay"
    );
    assert_eq!(
        db_b.iter_deleted().expect("deleted"),
        db_y.iter_deleted().expect("deleted"),
        "the deleted set propagates identically (B and Y both learn p/gone.NEF)"
    );
    assert!(db_y.get_deleted(&rel("p/gone.NEF")).expect("get").is_some());
    assert_eq!(
        db_b.cursor(&a).expect("cursor"),
        4,
        "journal replay advanced A's cursor"
    );
    assert_eq!(
        db_y.cursor(&c).expect("cursor"),
        7,
        "merge seeded A's knowledge of C"
    );

    // Bootstrap = merge then poll: polling after the merge re-applies
    // idempotently and converges the cursors too.
    let mut replay = ReplayConsumer;
    poll(&db_y, &client, &bucket, &mut replay)
        .await
        .expect("Y poll");
    assert_eq!(
        db_y.iter_items().expect("items"),
        db_b.iter_items().expect("items"),
        "merge-then-poll is idempotent over the same apply"
    );
    assert_eq!(db_y.cursor(&a).expect("cursor"), 4);
}

#[tokio::test]
async fn merged_header_cursors_make_the_next_poll_skip_covered_segments() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("man-seed");
    let client = g.client();
    let a = dev(DEV_A);
    let b = dev(DEV_B);
    let c = dev(DEV_C);

    // B publishes seqs 1-2.
    let (_bdir, _bpath, db_b) = open_db(&b);
    for e in &common::sync::sidecar_entries(&b, "seed", 2) {
        enqueue_entry(&db_b, e).expect("enqueue");
    }
    publish_pending(&db_b, &client, &bucket)
        .await
        .expect("publish");

    // A's manifest attests it applied B up to 2.
    let (_adir, _apath, db_a) = open_db(&a);
    db_a.set_cursor(&b, 2).expect("cursor");
    let manifest = build_manifest(&db_a, 1_769_950_000).expect("build");

    // Fresh C merges A's manifest, then polls: B's segments are covered
    // by the seeded cursor — nothing is applied or re-applied.
    let (_cdir, _cpath, db_c) = open_db(&c);
    let mut consumer = RecordingConsumer::default();
    merge(&[(a.clone(), manifest)], &db_c, &mut consumer).expect("merge");
    assert_eq!(
        db_c.cursor(&b).expect("cursor"),
        2,
        "header cursors seed the receiver"
    );

    let report = poll(&db_c, &client, &bucket, &mut consumer)
        .await
        .expect("poll");
    assert_eq!(report.entries_applied, 0, "covered segments are skipped");
    assert!(consumer.transcript.is_empty());
    assert_eq!(db_c.cursor(&b).expect("cursor"), 2);
}
