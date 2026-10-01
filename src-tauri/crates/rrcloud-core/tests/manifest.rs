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
use rrcloud_core::keys::{journal_segment_key, library_key, manifest_key, sidecar_key};
use rrcloud_core::manifest::{
    build_manifest, decode_manifest, encode_manifest, get_manifest, head_manifest_etag, merge,
    put_manifest, DeletedRow, Manifest, ManifestError, ManifestHeader, ManifestRow,
    MANIFEST_MAX_DECODED_BYTES, MANIFEST_PROTO,
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

#[test]
fn decode_refuses_a_decompression_bomb_with_a_typed_error() {
    // A small gzip object expanding past the decode cap: the §2.2-style
    // allocation bound for the manifest lane. Without it a single corrupt
    // or hostile object OOMs a phone before the first row parses.
    let chunk = vec![b'\n'; 1024 * 1024];
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    enc.write_all(b"{\"written_server_ts\":1,\"cursors\":{},\"proto\":1}\n")
        .expect("header");
    let mut written = 0usize;
    while written <= MANIFEST_MAX_DECODED_BYTES {
        enc.write_all(&chunk).expect("bomb body");
        written += chunk.len();
    }
    let bytes = enc.finish().expect("finish");
    assert!(
        bytes.len() < 8 * 1024 * 1024,
        "the bomb must be small on the wire for the test to mean anything"
    );
    let err = decode_manifest(&bytes).expect_err("over-cap decompression must be refused");
    match err {
        ManifestError::DecodedTooLarge { limit } => assert_eq!(limit, MANIFEST_MAX_DECODED_BYTES),
        other => panic!("expected DecodedTooLarge, got {other:?}"),
    }
}

#[test]
fn concatenated_gzip_members_never_silently_truncate_the_deleted_set() {
    // RFC 1952 allows multi-member streams (plain `cat` produces them). A
    // decoder that stops at the first member would accept a PARTIAL
    // deleted set — the §2.3 resurrection hazard. Every member must
    // decode, or the input must be refused; never a silent prefix.
    let member1 = gzip(concat!(
        "{\"written_server_ts\":1,\"cursors\":{},\"proto\":1}\n",
        "{\"del\":\"a.NEF\",\"vv\":{},\"server_ts\":2}\n",
    ));
    let member2 = gzip("{\"del\":\"b.NEF\",\"vv\":{},\"server_ts\":3}\n");
    let mut cat = member1.clone();
    cat.extend_from_slice(&member2);
    let m = decode_manifest(&cat).expect("multi-member manifest decodes fully");
    let dels: Vec<&str> = m.deleted.iter().map(|d| d.del.as_str()).collect();
    assert_eq!(
        dels,
        vec!["a.NEF", "b.NEF"],
        "the second member's deletion row must not be dropped"
    );

    // Trailing non-gzip bytes are refused, not ignored.
    let mut garbage = member1.clone();
    garbage.extend_from_slice(b"TRAILING GARBAGE, NOT GZIP");
    let err = decode_manifest(&garbage).expect_err("unconsumed trailing bytes must be refused");
    assert!(matches!(err, ManifestError::Gzip(_)), "got {err:?}");

    // Two whole manifests concatenated: the second header lands
    // mid-stream where only rows belong — fail closed, never a partial.
    let mut two = member1.clone();
    two.extend_from_slice(&member1);
    decode_manifest(&two).expect_err("a mid-stream header line is not a row");
}

// ---------------------------------------------------------------------------
// Merge through the consumer (no network)
// ---------------------------------------------------------------------------

#[test]
fn build_manifest_withholds_rows_its_own_merge_cannot_convert() {
    let a = dev(DEV_A);
    let (_dir, _path, db) = open_db(&a);
    let base = ItemRecord {
        kind: Kind::Sidecar,
        state: ItemState::Synced,
        size: 64,
        mtime_unix_ns: 0,
        blake3: None,
        sem_hash: None,
        vv: [(a.clone(), 1u32)].into_iter().collect(),
        content_id: None,
        w: None,
        h: None,
        pinned: false,
        last_access_unix: 0,
        verified_remote: false,
        attested: false,
        base_unknown: false,
    };
    db.replay_put_item(&rel("good.NEF"), &base).expect("put");
    // A thumb item and a preview item without a content id are legal
    // ItemRecord state, but no manifest row can round-trip them into a
    // synthetic apply op. Publishing them would poison every peer's §2.3
    // bootstrap.
    let thumb = ItemRecord {
        kind: Kind::Thumb,
        ..base.clone()
    };
    db.replay_put_item(&rel("thumbed.NEF"), &thumb)
        .expect("put");
    let preview = ItemRecord {
        kind: Kind::Preview,
        content_id: None,
        ..base.clone()
    };
    db.replay_put_item(&rel("previewed.NEF"), &preview)
        .expect("put");

    let manifest = build_manifest(&db, 1_769_950_000).expect("build");
    let keys: Vec<&str> = manifest.rows.iter().map(|r| r.key.as_str()).collect();
    assert_eq!(
        keys,
        vec!["good.NEF"],
        "unconvertible kinds are withheld at build time"
    );

    // The build's own merge accepts what it wrote — the wedge regression.
    let b = dev(DEV_B);
    let (_bdir, _bpath, db_b) = open_db(&b);
    let mut consumer = ReplayConsumer;
    let report = merge(&[(a.clone(), manifest)], &db_b, &mut consumer).expect("merge");
    assert_eq!(report.live_rows, 1);
    assert!(report.unconvertible.is_empty());
    assert!(db_b.get_item(&rel("good.NEF")).expect("get").is_some());
}

#[test]
fn build_manifest_withholds_items_whose_version_never_finished_an_upload() {
    let a = dev(DEV_A);
    let (_dir, _path, db) = open_db(&a);
    // blake3 None throughout: by the ItemRecord contract blake3 names the
    // last uploaded/verified bytes, so None proves NO version of the key
    // was ever uploaded — withholding such rows cannot drop a published
    // entry's effect (no entry was ever published for the key).
    let base = ItemRecord {
        kind: Kind::Sidecar,
        state: ItemState::Synced,
        size: 64,
        mtime_unix_ns: 0,
        blake3: None,
        sem_hash: None,
        vv: [(a.clone(), 1u32)].into_iter().collect(),
        content_id: None,
        w: None,
        h: None,
        pinned: false,
        last_access_unix: 0,
        verified_remote: false,
        attested: false,
        base_unknown: false,
    };
    // A §2.3 live row advertises that the key's version IS in the bucket.
    // States before the first completed upload cannot prove that: a
    // freshly imported item in Dirty/Queued/Uploading has no remote
    // object yet, and advertising it would 404 every bootstrapping peer's
    // hydrate. States at or past upload completion stay advertised.
    for (name, state, advertised) in [
        ("dirty.NEF", ItemState::Dirty, false),
        ("queued.NEF", ItemState::Queued, false),
        ("uploading.NEF", ItemState::Uploading, false),
        ("verifying.NEF", ItemState::Verifying, true),
        ("synced.NEF", ItemState::Synced, true),
        ("stub.NEF", ItemState::Stub, true),
        ("hydrated.NEF", ItemState::Hydrated, true),
    ] {
        let record = ItemRecord {
            state,
            ..base.clone()
        };
        db.replay_put_item(&rel(name), &record).expect("put");
        let manifest = build_manifest(&db, 1_769_950_000).expect("build");
        assert_eq!(
            manifest.rows.iter().any(|r| r.key == rel(name)),
            advertised,
            "{name} in state {state:?}: advertised should be {advertised}"
        );
    }

    // The final manifest advertises exactly the remotely-visible rows.
    let manifest = build_manifest(&db, 1_769_950_000).expect("build");
    let keys: Vec<&str> = manifest.rows.iter().map(|r| r.key.as_str()).collect();
    assert_eq!(
        keys,
        vec!["hydrated.NEF", "stub.NEF", "synced.NEF", "verifying.NEF"],
        "never-uploaded versions are withheld; uploaded ones ascend by relkey"
    );
}

#[test]
fn in_flight_items_with_a_published_version_stay_advertised() {
    // The round-2 blocker's local half: the §2.3 row gate is
    // evidence-based, not purely state-based. An item whose PREVIOUS
    // version completed an upload (blake3 names bytes that ARE in the
    // bucket) must keep advertising that version through the §2.4
    // Dirty/Queued/Uploading pipeline — its journal `put` was published
    // and is covered by the header's own-cursor attestation, so the row
    // is the manifest's ONLY carrier of that entry's effect. Withholding
    // it would make a §2.3 bootstrap merge silently drop the item (the
    // skip-and-diverge §2.1 principle 2 forbids).
    let a = dev(DEV_A);
    let (_dir, _path, db) = open_db(&a);
    let record = ItemRecord {
        kind: Kind::Sidecar,
        state: ItemState::Dirty,
        size: 64,
        mtime_unix_ns: 0,
        blake3: Some(Blake3Hex::parse(BLAKE3_HEX).expect("blake3")),
        sem_hash: Some(SemHash::parse(SEMHASH_HEX).expect("semhash")),
        vv: [(a.clone(), 1u32)].into_iter().collect(),
        content_id: None,
        w: None,
        h: None,
        pinned: false,
        last_access_unix: 0,
        verified_remote: true,
        attested: false,
        base_unknown: false,
    };
    for state in [ItemState::Dirty, ItemState::Queued, ItemState::Uploading] {
        let record = ItemRecord {
            state,
            ..record.clone()
        };
        db.replay_put_item(&rel("re-edited.NEF"), &record)
            .expect("put");
        let manifest = build_manifest(&db, 1_769_950_000).expect("build");
        let row = manifest
            .rows
            .iter()
            .find(|r| r.key == rel("re-edited.NEF"))
            .unwrap_or_else(|| panic!("row must stay advertised in {state:?}"));
        // The row describes the published version: in ItemRecord v1 the
        // vv and blake3 still name it (the §2.6 queue-admission bump is a
        // later unit, which inherits the snapshot requirement documented
        // on build_manifest).
        assert_eq!(row.vv, record.vv);
        assert_eq!(row.blake3, record.blake3);
        assert_eq!(row.size, record.size);
    }
}

#[test]
fn merge_skips_unconvertible_rows_instead_of_refusing_the_whole_merge() {
    let a = dev(DEV_A);
    let b = dev(DEV_B);
    let (_dir, _path, db) = open_db(&b);

    // A (non-conforming or future) writer shipped a thumb row and a
    // preview row without a content id. These are additive advisory rows —
    // skipping one cannot destroy data — so one bad row must not wedge the
    // whole §2.3 bootstrap lane.
    let mut manifest = Manifest {
        header: header(&[(&a, 4)]),
        rows: vec![
            live_row("ok.NEF", Kind::Sidecar, &a),
            live_row("pv.NEF", Kind::Preview, &a),
            live_row("th.NEF", Kind::Thumb, &a),
        ],
        deleted: vec![deleted_row("gone.NEF", &a)],
    };
    manifest.rows[1].content_id = None;

    let mut consumer = ReplayConsumer;
    let report = merge(&[(a.clone(), manifest)], &db, &mut consumer)
        .expect("unconvertible rows must not refuse the merge");
    assert_eq!(report.live_rows, 1, "only the convertible row applies");
    assert_eq!(report.deleted_rows, 1);
    assert_eq!(
        report.unconvertible,
        vec![(rel("pv.NEF"), Kind::Preview), (rel("th.NEF"), Kind::Thumb)],
        "skipped rows are reported, not silently dropped"
    );
    assert!(db.get_item(&rel("ok.NEF")).expect("get").is_some());
    assert!(db.get_deleted(&rel("gone.NEF")).expect("get").is_some());
    assert_eq!(db.cursor(&a).expect("cursor"), 4, "cursors still seed");
}

#[test]
fn merge_never_seeds_a_cursor_for_the_merging_devices_own_prefix() {
    let a = dev(DEV_A);
    let y = dev(DEV_Y);
    let (_dir, _path, db_y) = open_db(&y);

    // A's manifest attests Y's seqs; Y itself knows better (its own
    // published cursor), and its cursors table holds peers only.
    let manifest = Manifest {
        header: header(&[(&y, 7), (&a, 2)]),
        rows: vec![],
        deleted: vec![],
    };
    let mut consumer = RecordingConsumer::default();
    let report = merge(&[(a.clone(), manifest)], &db_y, &mut consumer).expect("merge");
    assert_eq!(db_y.cursor(&a).expect("cursor"), 2);
    assert_eq!(db_y.cursor(&y).expect("cursor"), 0, "own prefix untouched");
    assert!(!report.cursors.contains_key(&y));
}

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
fn merge_applies_rows_in_relkey_order_as_synthetic_ops_and_seeds_owner_attested_cursors() {
    let a = dev(DEV_A);
    let b = dev(DEV_B);
    let c = dev(DEV_C);
    let y = dev(DEV_Y);
    let (_dir, _path, db) = open_db(&y);

    // Two manifests with interleaved relkeys and overlapping cursor
    // claims: rows must reach the consumer in ascending relkey order
    // across manifests, and each device's cursor seeds ONLY from its own
    // manifest's self-attestation (`cursors[owner]`, backed by that
    // manifest's rows). A peer's header claim about a third device's
    // prefix (A's `c: 7` and `b: 2`, B's inflated `a: 9`) is subject to
    // the claimant's own live-row withholding, so trusting it could skip
    // journal entries whose effects no merged row folds — the round-2
    // blocker's third-device lane.
    let m1 = Manifest {
        header: header(&[(&a, 4), (&b, 2), (&c, 7)]),
        rows: vec![
            live_row("a.NEF", Kind::Sidecar, &a),
            live_row("m.NEF", Kind::Original, &a),
        ],
        deleted: vec![deleted_row("zz-gone.NEF", &a)],
    };
    let m2 = Manifest {
        header: header(&[(&a, 9), (&b, 5)]),
        rows: vec![live_row("g.NEF", Kind::Sidecar, &b)],
        deleted: vec![],
    };

    let mut consumer = RecordingConsumer::default();
    let report = merge(&[(a.clone(), m1), (b.clone(), m2)], &db, &mut consumer).expect("merge");
    assert_eq!(report.live_rows, 3);
    assert_eq!(report.deleted_rows, 1);
    assert_eq!(
        report.cursors,
        [(a.clone(), 4u64), (b.clone(), 5u64)]
            .into_iter()
            .collect::<BTreeMap<_, _>>()
    );
    assert_eq!(
        db.cursor(&a).expect("cursor"),
        4,
        "A's own attestation wins over B's unbacked claim of 9"
    );
    assert_eq!(db.cursor(&b).expect("cursor"), 5);
    assert_eq!(
        db.cursor(&c).expect("cursor"),
        0,
        "no manifest of C's own was merged, so C's prefix stays unseeded \
         and the next poll applies it from the journal"
    );

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
        0,
        "A's header claim about C's prefix is unbacked by A's rows and \
         never seeds a cursor — Y will apply C's journal itself"
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

/// §2.10 compaction rule 1 ("A's own manifest has cursors[A] >= s")
/// requires the writer to attest its OWN published cursor; without it a
/// bootstrapping device that merges A's manifest after A compacted its
/// journal prefix wedges in MidStreamGap forever.
#[tokio::test]
async fn build_manifest_attests_the_writers_own_published_cursor() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("man-own-cursor");
    let client = g.client();
    let a = dev(DEV_A);
    let b = dev(DEV_B);
    let x = dev(DEV_C);

    // A publishes seqs 1-3 (one segment) and knows B's cursor.
    let (_adir, _apath, db_a) = open_db(&a);
    let entries = common::sync::sidecar_entries(&a, "own", 3);
    apply_entries_locally(&db_a, &entries);
    for e in &entries {
        enqueue_entry(&db_a, e).expect("enqueue");
    }
    publish_pending(&db_a, &client, &bucket)
        .await
        .expect("publish");
    db_a.set_cursor(&b, 9).expect("cursor");

    let manifest = build_manifest(&db_a, 1_769_950_000).expect("build");
    assert_eq!(
        manifest.header.cursors,
        [(a.clone(), 3u64), (b.clone(), 9u64)]
            .into_iter()
            .collect::<BTreeMap<_, _>>(),
        "the writer's own published cursor is attested alongside its peers'"
    );

    // Simulate §2.10 compaction of A's whole journal prefix, then the
    // designed catch-up: X merges A's manifest (rows fold the compacted
    // entries' effects; cursors[A] covers their seqs), then polls.
    client
        .delete_object(&bucket, &journal_segment_key(&a, 1))
        .await
        .expect("compact away A's segment");
    let (_xdir, _xpath, db_x) = open_db(&x);
    let mut consumer = ReplayConsumer;
    merge(&[(a.clone(), manifest)], &db_x, &mut consumer).expect("merge");
    assert_eq!(db_x.cursor(&a).expect("cursor"), 3);

    // A publishes more; X continues contiguously past the compacted
    // prefix — no gap, no wedge.
    let more = common::sync::sidecar_entries(&a, "more", 2);
    apply_entries_locally(&db_a, &more);
    for e in &more {
        enqueue_entry(&db_a, e).expect("enqueue");
    }
    publish_pending(&db_a, &client, &bucket)
        .await
        .expect("publish more");
    let mut consumer = ReplayConsumer;
    let report = poll(&db_x, &client, &bucket, &mut consumer)
        .await
        .expect("poll");
    assert!(
        report.mid_stream_gaps.is_empty() && report.gaps.is_empty(),
        "the attested cursor covers the compacted prefix: {report:?}"
    );
    assert_eq!(report.entries_applied, 2);
    assert_eq!(db_x.cursor(&a).expect("cursor"), 5);
}

#[tokio::test]
async fn own_manifest_cursors_make_the_next_poll_skip_covered_segments_losslessly() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("man-seed");
    let client = g.client();
    let b = dev(DEV_B);
    let c = dev(DEV_C);

    // B applies and publishes seqs 1-2, then writes its OWN manifest: the
    // header attests cursors[B] = 2 (its published cursor) and the rows
    // fold both entries' effects.
    let (_bdir, _bpath, db_b) = open_db(&b);
    let entries = common::sync::sidecar_entries(&b, "seed", 2);
    apply_entries_locally(&db_b, &entries);
    for e in &entries {
        enqueue_entry(&db_b, e).expect("enqueue");
    }
    publish_pending(&db_b, &client, &bucket)
        .await
        .expect("publish");
    let manifest = build_manifest(&db_b, 1_769_950_000).expect("build");
    assert_eq!(manifest.rows.len(), 2, "the attestation is backed by rows");

    // Fresh C merges B's manifest, then polls: B's segments are covered
    // by the owner-attested cursor and nothing is re-applied — and C
    // holds both items, because the rows carried their effects. Skipping
    // is an optimization, never a loss.
    let (_cdir, _cpath, db_c) = open_db(&c);
    let mut replay = ReplayConsumer;
    merge(&[(b.clone(), manifest)], &db_c, &mut replay).expect("merge");
    assert_eq!(
        db_c.cursor(&b).expect("cursor"),
        2,
        "the owner's self-attestation seeds the receiver"
    );
    assert_eq!(db_c.iter_items().expect("items").len(), 2);

    let mut consumer = RecordingConsumer::default();
    let report = poll(&db_c, &client, &bucket, &mut consumer)
        .await
        .expect("poll");
    assert_eq!(report.entries_applied, 0, "covered segments are skipped");
    assert!(consumer.transcript.is_empty());
    assert_eq!(db_c.cursor(&b).expect("cursor"), 2);
}

#[tokio::test]
async fn a_peers_unbacked_claim_about_a_third_device_never_skips_its_journal() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("man-peer-claim");
    let client = g.client();
    let a = dev(DEV_A);
    let b = dev(DEV_B);
    let c = dev(DEV_C);

    // B publishes seqs 1-2. A's manifest claims it applied B up to 2 —
    // but A's rows fold NONE of those entries' effects (A's live-row
    // withholding may legitimately hide them, e.g. an item A re-dirtied
    // from a base it never uploaded). Round-2 blocker: trusting this
    // claim made a fresh device skip B's segments and silently lose both
    // entries.
    let (_bdir, _bpath, db_b) = open_db(&b);
    for e in &common::sync::sidecar_entries(&b, "seed", 2) {
        enqueue_entry(&db_b, e).expect("enqueue");
    }
    publish_pending(&db_b, &client, &bucket)
        .await
        .expect("publish");
    let (_adir, _apath, db_a) = open_db(&a);
    db_a.set_cursor(&b, 2).expect("cursor");
    let manifest = build_manifest(&db_a, 1_769_950_000).expect("build");
    assert_eq!(
        manifest.header.cursors,
        [(b.clone(), 2u64)].into_iter().collect::<BTreeMap<_, _>>(),
        "A still publishes its applied cursors (compaction input)"
    );

    // Fresh C merges A's manifest: the peer claim must NOT seed B's
    // cursor, so the next poll applies B's journal itself.
    let (_cdir, _cpath, db_c) = open_db(&c);
    let mut replay = ReplayConsumer;
    merge(&[(a.clone(), manifest)], &db_c, &mut replay).expect("merge");
    assert_eq!(
        db_c.cursor(&b).expect("cursor"),
        0,
        "an unbacked peer claim never seeds a cursor"
    );
    let mut consumer = RecordingConsumer::default();
    let report = poll(&db_c, &client, &bucket, &mut consumer)
        .await
        .expect("poll");
    assert_eq!(
        report.entries_applied, 2,
        "B's entries are applied, not lost"
    );
    assert_eq!(db_c.cursor(&b).expect("cursor"), 2);
}

/// The round-2 blocker's confirmed probe, pinned as a regression test:
/// device A publishes `put` seq 1 for a key, the item reaches `Synced`,
/// the user re-edits (`Synced` → `Dirty`, a legal §2.4 edge). A's
/// manifest attests `cursors[A] = 1`, so a fresh device bootstrapping per
/// §2.3 (merge then poll) seeds past seq 1 — the manifest row is the only
/// carrier of the published entry's effect, and withholding it silently
/// and permanently dropped the item (no gap, no halt, no outcome).
#[tokio::test]
async fn bootstrap_merge_cannot_drop_an_item_whose_re_edit_is_in_flight() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("man-inflight");
    let client = g.client();
    let a = dev(DEV_A);
    let c = dev(DEV_C);
    let k = rel("2026/10/IMG_0042.NEF");

    // A publishes put seq 1 for K; the upload completed, so the record
    // carries the uploaded bytes' blake3 (the ItemRecord contract).
    let (_adir, _apath, db_a) = open_db(&a);
    let mut e = entry(&a, Op::Put, Kind::Sidecar, sidecar_key(&k));
    e.blake3 = Some(Blake3Hex::parse(BLAKE3_HEX).expect("blake3"));
    apply_entries_locally(&db_a, &[e.clone()]);
    enqueue_entry(&db_a, &e).expect("enqueue");
    publish_pending(&db_a, &client, &bucket)
        .await
        .expect("publish");

    // The user re-edits: Synced → Dirty, then on through the §2.4
    // pipeline. At every in-flight state the manifest must keep carrying
    // the published version's row next to the own-cursor attestation.
    for (from, to) in [
        (ItemState::Synced, ItemState::Dirty),
        (ItemState::Dirty, ItemState::Queued),
        (ItemState::Queued, ItemState::Uploading),
    ] {
        db_a.transition(&k, from, to, |_| {}).expect("transition");
        let manifest = build_manifest(&db_a, 1_769_950_000).expect("build");
        assert_eq!(
            manifest.header.cursors.get(&a),
            Some(&1),
            "the own published cursor stays attested"
        );
        assert!(
            manifest.rows.iter().any(|r| r.key == k),
            "the published version's row must not be withheld in {to:?}"
        );
    }

    // Bootstrap per §2.3 (merge then poll) on a fresh device, through the
    // full wire path, with A's re-edit still in flight (A may stay
    // offline indefinitely).
    let manifest = build_manifest(&db_a, 1_769_950_000).expect("build");
    put_manifest(&client, &bucket, &a, &manifest)
        .await
        .expect("put manifest");
    let fetched = get_manifest(&client, &bucket, &a)
        .await
        .expect("get manifest");
    let (_cdir, _cpath, db_c) = open_db(&c);
    let mut replay = ReplayConsumer;
    merge(&[(a.clone(), fetched)], &db_c, &mut replay).expect("merge");
    let report = poll(&db_c, &client, &bucket, &mut replay)
        .await
        .expect("poll");

    // Before the fix: entries_applied == 0, no gaps, and get_item == None
    // — the published item silently vanished from the bootstrapped fleet.
    assert!(report.gaps.is_empty() && report.mid_stream_gaps.is_empty());
    assert_eq!(report.entries_applied, 0, "seq 1 is covered by the row");
    let item = db_c
        .get_item(&k)
        .expect("get")
        .expect("the published item must survive a bootstrap merge");
    assert_eq!(item.vv, e.vv, "the row carried the published version");
    assert_eq!(item.blake3, e.blake3);
    assert_eq!(db_c.cursor(&a).expect("cursor"), 1);
}
