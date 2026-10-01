//! Failing tests for `rrcloud_core::journal` (architecture §2.2, §2.7).
//!
//! Pins the v1 entry schema against the design doc's example, round-trip
//! stability and deterministic encoding, the segment caps, the fail-closed
//! min-reader rule for unknown versions, segment filename format/ordering,
//! and the tombstone document.

use rrcloud_core::clock::{DeviceId, VersionVector};
use rrcloud_core::journal::{
    decode_segment, encode_segment, format_segment_filename, parse_segment_filename, JournalEntry,
    JournalError, Kind, Op, SegmentFilename, Tombstone, JOURNAL_VERSION, SEGMENT_MAX_BYTES,
    SEGMENT_MAX_ENTRIES,
};
use rrcloud_core::keys::{journal_segment_key, RelKey};
use rrcloud_core::semhash::SemHash;

const DEV1: &str = "d1f0c2aa-9d2b-4a6e-8f1c-3b7d5e9a0c42";
const DEV2: &str = "a3b2e1d0-5c4f-4b3a-9e2d-1f0a9b8c7d6e";
const BLAKE3_HEX: &str = "4878ca0425c739fa427f7eda20fe845f6b2e46ba5fe2a14df5b1e32f50603215";
const SEMHASH_HEX: &str = "af1349b9f5f9a1a6a0404dee36dcc9499bcb25c9adc112b7cc9a93cae41f3262";

fn dev(s: &str) -> DeviceId {
    DeviceId::new(s).expect("valid device id")
}

fn vv(pairs: &[(&str, u32)]) -> VersionVector {
    pairs.iter().map(|&(d, c)| (dev(d), c)).collect()
}

/// A minimal valid sidecar `put` entry for segment tests.
fn entry(seq: u64, key: &str) -> JournalEntry {
    JournalEntry {
        v: JOURNAL_VERSION,
        seq,
        ts: 1_769_900_000,
        device: dev(DEV1),
        op: Op::Put,
        kind: Kind::Sidecar,
        key: key.to_string(),
        vv: vv(&[(DEV1, 9), (DEV2, 4)]),
        size: Some(48_213),
        blake3: Some(BLAKE3_HEX.to_string()),
        sem_hash: Some(SemHash::parse(SEMHASH_HEX).expect("valid hash")),
        rating: Some(3),
        color_label: Some("red".to_string()),
        content_id: None,
        w: None,
        h: None,
        mtime: Some(1_769_899_000),
        from_key: None,
    }
}

// ---------------------------------------------------------------------------
// Entry decode: the design doc's §2.2 example
// ---------------------------------------------------------------------------

fn doc_example_line() -> String {
    format!(
        concat!(
            r#"{{"v":1, "seq":412, "ts":1769900000, "device":"{d1}", "op":"put","#,
            r#" "kind":"sidecar", "key":"library/2026/10/IMG_0042.NEF.rrdata","#,
            r#" "size":48213, "blake3":"{b3}", "sem_hash":"{sh}","#,
            r#" "vv":{{"{d1}":9,"{d2}":4}}, "rating":3, "color_label":"red","#,
            r#" "content_id":null, "w":null, "h":null, "mtime":1769899000}}"#
        ),
        d1 = DEV1,
        d2 = DEV2,
        b3 = BLAKE3_HEX,
        sh = SEMHASH_HEX,
    )
}

#[test]
fn doc_example_entry_parses() {
    let e = JournalEntry::from_json_line(&doc_example_line()).expect("doc example parses");
    assert_eq!(e.v, 1);
    assert_eq!(e.seq, 412);
    assert_eq!(e.ts, 1_769_900_000);
    assert_eq!(e.device, dev(DEV1));
    assert_eq!(e.op, Op::Put);
    assert_eq!(e.kind, Kind::Sidecar);
    assert_eq!(e.key, "library/2026/10/IMG_0042.NEF.rrdata");
    assert_eq!(e.size, Some(48_213));
    assert_eq!(e.blake3.as_deref(), Some(BLAKE3_HEX));
    assert_eq!(
        e.sem_hash,
        Some(SemHash::parse(SEMHASH_HEX).expect("valid"))
    );
    assert_eq!(e.vv, vv(&[(DEV1, 9), (DEV2, 4)]));
    assert_eq!(e.rating, Some(3));
    assert_eq!(e.color_label.as_deref(), Some("red"));
    // Explicit nulls and absent fields are both None.
    assert_eq!(e.content_id, None);
    assert_eq!(e.w, None);
    assert_eq!(e.h, None);
    assert_eq!(e.mtime, Some(1_769_899_000));
    assert_eq!(e.from_key, None);
}

#[test]
fn move_and_attest_entries_parse() {
    let mv = format!(
        concat!(
            r#"{{"v":1,"seq":5,"ts":1769900100,"device":"{d1}","op":"move","kind":"original","#,
            r#""key":"library/new/IMG_0001.NEF","from_key":"library/old/IMG_0001.NEF","#,
            r#""vv":{{"{d1}":2}},"content_id":"{sh}","w":6048,"h":4032,"mtime":1769899000}}"#
        ),
        d1 = DEV1,
        sh = SEMHASH_HEX,
    );
    let e = JournalEntry::from_json_line(&mv).expect("move entry parses");
    assert_eq!(e.op, Op::Move);
    assert_eq!(e.from_key.as_deref(), Some("library/old/IMG_0001.NEF"));
    assert_eq!(e.w, Some(6048));
    assert_eq!(e.h, Some(4032));

    let attest = format!(
        concat!(
            r#"{{"v":1,"seq":6,"ts":1769900200,"device":"{d1}","op":"attest","kind":"original","#,
            r#""key":"library/new/IMG_0001.NEF","blake3":"{b3}","vv":{{"{d1}":2}}}}"#
        ),
        d1 = DEV1,
        b3 = BLAKE3_HEX,
    );
    let e = JournalEntry::from_json_line(&attest).expect("attest entry parses");
    assert_eq!(e.op, Op::Attest);
    assert_eq!(e.blake3.as_deref(), Some(BLAKE3_HEX));
    assert_eq!(e.size, None);
}

#[test]
fn all_ops_and_kinds_round_trip_their_wire_names() {
    // op: put | del | move | attest; kind: original | sidecar | xmp |
    // preview | thumb | thumbpack | albums | presets (§2.2).
    for (op, name) in [
        (Op::Put, "\"put\""),
        (Op::Del, "\"del\""),
        (Op::Move, "\"move\""),
        (Op::Attest, "\"attest\""),
    ] {
        assert_eq!(serde_json::to_string(&op).expect("op"), name);
        assert_eq!(serde_json::from_str::<Op>(name).expect("op back"), op);
    }
    for (kind, name) in [
        (Kind::Original, "\"original\""),
        (Kind::Sidecar, "\"sidecar\""),
        (Kind::Xmp, "\"xmp\""),
        (Kind::Preview, "\"preview\""),
        (Kind::Thumb, "\"thumb\""),
        (Kind::Thumbpack, "\"thumbpack\""),
        (Kind::Albums, "\"albums\""),
        (Kind::Presets, "\"presets\""),
    ] {
        assert_eq!(serde_json::to_string(&kind).expect("kind"), name);
        assert_eq!(serde_json::from_str::<Kind>(name).expect("kind back"), kind);
    }
}

// ---------------------------------------------------------------------------
// Min-reader rule: unknown versions are typed errors; unknown fields are not
// ---------------------------------------------------------------------------

#[test]
fn unknown_optional_field_is_ignored_within_v1() {
    let line = doc_example_line().replacen("\"v\":1,", "\"v\":1, \"flux_capacitor\":true,", 1);
    assert!(line.contains("flux_capacitor"), "test premise");
    let e = JournalEntry::from_json_line(&line).expect("unknown optional field ignored");
    assert_eq!(e.seq, 412);
}

#[test]
fn unknown_version_is_a_typed_error_carrying_the_version() {
    let line = doc_example_line().replacen("\"v\":1,", "\"v\":2,", 1);
    match JournalEntry::from_json_line(&line) {
        Err(JournalError::UnsupportedVersion { version }) => assert_eq!(version, 2),
        other => panic!("v:2 entry must be UnsupportedVersion, got {other:?}"),
    }
    let line99 = doc_example_line().replacen("\"v\":1,", "\"v\":99,", 1);
    match JournalEntry::from_json_line(&line99) {
        Err(JournalError::UnsupportedVersion { version }) => assert_eq!(version, 99),
        other => panic!("v:99 entry must be UnsupportedVersion, got {other:?}"),
    }
}

#[test]
fn missing_version_is_an_error_never_a_default() {
    let line = doc_example_line().replacen("\"v\":1,", "", 1);
    assert!(JournalEntry::from_json_line(&line).is_err());
}

#[test]
fn garbage_lines_are_errors() {
    for bad in ["", "not json", "[1,2]", "{\"v\":1}"] {
        assert!(JournalEntry::from_json_line(bad).is_err(), "{bad:?}");
    }
}

// ---------------------------------------------------------------------------
// Round-trip stability and deterministic encoding
// ---------------------------------------------------------------------------

#[test]
fn entry_round_trips_and_encodes_deterministically() {
    let e = entry(412, "library/2026/10/IMG_0042.NEF.rrdata");
    let line = e.to_json_line().expect("encode");
    assert!(!line.contains('\n'), "one NDJSON line");
    let back = JournalEntry::from_json_line(&line).expect("decode own encoding");
    assert_eq!(back, e);
    // Deterministic: same entry, same bytes — and stable through a cycle.
    assert_eq!(line, e.to_json_line().expect("encode again"));
    assert_eq!(line, back.to_json_line().expect("encode decoded"));
}

#[test]
fn doc_example_round_trips_through_reencoding() {
    let e = JournalEntry::from_json_line(&doc_example_line()).expect("parse");
    let line = e.to_json_line().expect("encode");
    let back = JournalEntry::from_json_line(&line).expect("reparse");
    assert_eq!(back, e);
}

#[test]
fn segment_round_trips() {
    let entries = vec![
        entry(412, "library/2026/10/IMG_0042.NEF.rrdata"),
        entry(413, "library/2026/10/IMG_0043.NEF.rrdata"),
        entry(414, "library/2026/10/IMG_0044.NEF.rrdata"),
    ];
    let bytes = encode_segment(&entries).expect("encode");
    assert_eq!(
        bytes,
        encode_segment(&entries).expect("re-encode"),
        "deterministic"
    );
    let back = decode_segment(&bytes).expect("decode");
    assert_eq!(back, entries);
}

#[test]
fn segment_decode_is_fail_closed_on_unknown_versions() {
    let good = entry(1, "library/a.NEF.rrdata")
        .to_json_line()
        .expect("encode");
    let future = doc_example_line().replacen("\"v\":1,", "\"v\":2,", 1);
    let segment = format!("{good}\n{future}\n");
    match decode_segment(segment.as_bytes()) {
        Err(JournalError::UnsupportedVersion { version }) => assert_eq!(version, 2),
        other => panic!("mixed-version segment must fail closed, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Segment caps (§2.2: ≤1000 entries, ≤1 MiB)
// ---------------------------------------------------------------------------

#[test]
fn segment_entry_cap_enforced_on_encode() {
    assert_eq!(SEGMENT_MAX_ENTRIES, 1000);
    let at_cap: Vec<JournalEntry> = (0..1000)
        .map(|i| entry(i, "library/a.NEF.rrdata"))
        .collect();
    assert!(
        encode_segment(&at_cap).is_ok(),
        "exactly 1000 entries is fine"
    );
    let over: Vec<JournalEntry> = (0..1001)
        .map(|i| entry(i, "library/a.NEF.rrdata"))
        .collect();
    match encode_segment(&over) {
        Err(JournalError::TooManyEntries { count }) => assert_eq!(count, 1001),
        other => panic!("1001 entries must be TooManyEntries, got {other:?}"),
    }
}

#[test]
fn segment_byte_cap_enforced_on_encode() {
    assert_eq!(SEGMENT_MAX_BYTES, 1024 * 1024);
    // 700 entries × ~1.7 KiB key ≈ 1.2 MiB: under the entry cap, over the
    // byte cap.
    let long_dir = "d".repeat(1700);
    let entries: Vec<JournalEntry> = (0..700)
        .map(|i| entry(i, &format!("library/{long_dir}/{i}.NEF.rrdata")))
        .collect();
    match encode_segment(&entries) {
        Err(JournalError::SegmentTooLarge { size }) => assert!(size > SEGMENT_MAX_BYTES),
        other => panic!("oversized segment must be SegmentTooLarge, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Segment filenames
// ---------------------------------------------------------------------------

#[test]
fn segment_filename_format_is_16_hex_zero_padded() {
    assert_eq!(format_segment_filename(0), "0000000000000000.v1.ndjson");
    assert_eq!(format_segment_filename(0x19a), "000000000000019a.v1.ndjson");
    assert_eq!(
        format_segment_filename(u64::MAX),
        "ffffffffffffffff.v1.ndjson"
    );
}

#[test]
fn segment_filename_parse_round_trips() {
    for seq in [0u64, 1, 0x19a, 1 << 32, u64::MAX] {
        let name = format_segment_filename(seq);
        let parsed = parse_segment_filename(&name).expect("parses own format");
        assert_eq!(parsed, SegmentFilename { seq, version: 1 });
    }
    // A future segment version parses (the apply loop gates on it).
    let v2 = parse_segment_filename("000000000000019a.v2.ndjson").expect("v2 name parses");
    assert_eq!(
        v2,
        SegmentFilename {
            seq: 0x19a,
            version: 2
        }
    );
}

#[test]
fn segment_filename_parse_is_strict() {
    let bad = [
        "",
        "19a.v1.ndjson",
        "000000000000019.v1.ndjson",
        "000000000000019A.v1.ndjson",
        "000000000000019a.ndjson",
        "000000000000019a.v1.json",
        "000000000000019a.v1.ndjson.bak",
        "x000000000000019a.v1.ndjson",
        "000000000000019a.v.ndjson",
        "000000000000019a.vx.ndjson",
    ];
    for name in bad {
        assert!(
            parse_segment_filename(name).is_err(),
            "{name:?} must be rejected"
        );
    }
}

#[test]
fn segment_filename_lexicographic_order_equals_numeric_order() {
    let mut seqs = vec![
        0u64,
        1,
        2,
        9,
        10,
        15,
        16,
        255,
        256,
        4095,
        4096,
        65535,
        65536,
        0xdead_beef,
        1 << 32,
        (1 << 40) + 7,
        u64::MAX,
    ];
    let mut names: Vec<String> = seqs.iter().map(|&s| format_segment_filename(s)).collect();
    names.sort();
    seqs.sort_unstable();
    let expected: Vec<String> = seqs.iter().map(|&s| format_segment_filename(s)).collect();
    assert_eq!(names, expected);
}

#[test]
fn journal_key_embeds_the_segment_filename() {
    let d = dev(DEV1);
    for seq in [0u64, 0x19a, u64::MAX] {
        let key = journal_segment_key(&d, seq);
        let name = format_segment_filename(seq);
        assert!(
            key.ends_with(&format!("/{name}")),
            "{key:?} must end with /{name}"
        );
        let parsed = parse_segment_filename(&name).expect("parses");
        assert_eq!(parsed.seq, seq);
    }
}

// ---------------------------------------------------------------------------
// Tombstone document (§2.7)
// ---------------------------------------------------------------------------

#[test]
fn tombstone_round_trips() {
    let t = Tombstone {
        relkey: RelKey::new("2026/10/IMG_0042.NEF").expect("valid relkey"),
        vv: vv(&[(DEV1, 10), (DEV2, 4)]),
        device: dev(DEV1),
        server_ts: 1_769_900_123,
        kinds: vec![Kind::Original, Kind::Sidecar, Kind::Xmp],
    };
    let json = serde_json::to_string(&t).expect("serialize");
    let back: Tombstone = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(back, t);
    assert_eq!(json, serde_json::to_string(&back).expect("re-serialize"));
}

#[test]
fn tombstone_wire_document_parses() {
    let doc = format!(
        concat!(
            r#"{{"relkey":"2026/10/IMG_0042.NEF","vv":{{"{d1}":10,"{d2}":4}},"#,
            r#""device":"{d1}","server_ts":1769900123,"kinds":["original","sidecar"]}}"#
        ),
        d1 = DEV1,
        d2 = DEV2,
    );
    let t: Tombstone = serde_json::from_str(&doc).expect("wire tombstone parses");
    assert_eq!(t.relkey.as_str(), "2026/10/IMG_0042.NEF");
    assert_eq!(t.vv, vv(&[(DEV1, 10), (DEV2, 4)]));
    assert_eq!(t.device, dev(DEV1));
    assert_eq!(t.server_ts, 1_769_900_123);
    assert_eq!(t.kinds, vec![Kind::Original, Kind::Sidecar]);
}

#[test]
fn tombstone_rejects_invalid_relkey() {
    let doc = format!(
        r#"{{"relkey":"../escape","vv":{{"{DEV1}":1}},"device":"{DEV1}","server_ts":1,"kinds":["original"]}}"#
    );
    assert!(serde_json::from_str::<Tombstone>(&doc).is_err());
}
