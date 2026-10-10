//! The engine's wire types *are* the generated `rrcloud-proto` SDK's (one definition,
//! `protocol/rrcloud.protocol.toml`), but the engine keeps its own codecs with a richer
//! error taxonomy (`JournalError`, `ManifestError`) and its own key constructors. On
//! every shared fixture both codec lanes must (a) decode, (b) re-encode to identical
//! bytes, and (c) agree on the key schema and the relkey rules. A drift fails here,
//! not in a bucket.

use std::path::PathBuf;

use rrcloud_core::journal::{decode_segment, JournalEntryExt as _, JournalError, Tombstone};
use rrcloud_core::keys;
use rrcloud_core::manifest::{decode_manifest, encode_manifest, ManifestError};
use rrcloud_core::publisher::DeviceEntry;
use rrcloud_proto as proto;

fn fixture(name: &str) -> String {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../protocol/fixtures")
        .join(name);
    std::fs::read_to_string(&p).unwrap_or_else(|e| panic!("{}: {e}", p.display()))
}

#[test]
fn journal_entries_encode_identically_in_engine_and_sdk() {
    let text = fixture("journal_segment.v1.ndjson");
    let engine = decode_segment(text.as_bytes()).expect("engine decodes");
    let sdk = proto::decode_journal_segment(text.as_bytes()).expect("sdk decodes");
    assert_eq!(engine, sdk);
    for (line, e) in text.lines().zip(engine.iter()) {
        assert_eq!(e.to_json_line().unwrap(), line, "engine re-encode");
        assert_eq!(e.to_json().unwrap(), line, "sdk re-encode");
    }
    // The engine's error taxonomy is a faithful view of the SDK's version gate.
    let future = text.replacen("\"v\":1", "\"v\":7", 1);
    assert!(matches!(
        decode_segment(future.as_bytes()),
        Err(JournalError::UnsupportedVersion { version: 7 })
    ));
    assert!(matches!(
        proto::decode_journal_segment(future.as_bytes()),
        Err(proto::ProtoError::UnsupportedVersion { version: 7 })
    ));
}

#[test]
fn tombstone_and_device_entry_agree() {
    let t = fixture("tombstone.json");
    let e: Tombstone = serde_json::from_str(t.trim()).unwrap();
    let s = Tombstone::from_json(t.trim()).unwrap();
    assert_eq!(e, s);
    assert_eq!(serde_json::to_string(&e).unwrap(), t.trim());
    let d = fixture("device_entry.json");
    let e: DeviceEntry = serde_json::from_str(d.trim()).unwrap();
    assert_eq!(serde_json::to_string(&e).unwrap(), d.trim());
    assert_eq!(e.to_json().unwrap(), d.trim());
}

#[test]
fn manifests_gzip_encode_and_decode_across_implementations() {
    let nd = fixture("manifest.ndjson");
    let mut lines = nd.lines();
    let header = proto::ManifestHeader::from_json(lines.next().unwrap()).unwrap();
    let rows: Vec<proto::ManifestRow> = lines
        .clone()
        .filter(|l| l.starts_with("{\"key\""))
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    let deleted: Vec<proto::DeletedRow> = lines
        .filter(|l| l.starts_with("{\"del\""))
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    let doc = proto::Manifest {
        header,
        rows,
        deleted,
    };
    let gz_engine = encode_manifest(&doc).unwrap();
    let gz_sdk = proto::encode_manifest(&doc).unwrap();
    assert_eq!(gz_engine, gz_sdk, "both lanes gzip the same NDJSON");
    assert_eq!(proto::decode_manifest(&gz_engine).unwrap(), doc);
    assert_eq!(decode_manifest(&gz_sdk).unwrap(), doc);
    // Header-first, fail-closed on both lanes.
    let bad = nd.replacen("\"proto\":1", "\"proto\":9", 1);
    let gz = {
        use std::io::Write as _;
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        enc.write_all(bad.as_bytes()).unwrap();
        enc.finish().unwrap()
    };
    assert!(matches!(
        decode_manifest(&gz),
        Err(ManifestError::UnsupportedProto { proto: Some(9) })
    ));
    assert!(matches!(
        proto::decode_manifest(&gz),
        Err(proto::ProtoError::UnsupportedVersion { version: 9 })
    ));
}

#[test]
fn key_schema_agrees() {
    let d = proto::DeviceId::new("0f6b2a1e-1111-4222-8333-944444444444").unwrap();
    let rk = proto::RelKey::new("2026/10/IMG_0001.NEF").unwrap();
    let cid =
        proto::ContentId::parse("6a0f8e2d3c4b5a69788796a5b4c3d2e1f0f1e2d3c4b5a69788796a5b4c3d2e1f")
            .unwrap();
    assert_eq!(keys::library_key(&rk), proto::key_library_original(&rk));
    assert_eq!(keys::sidecar_key(&rk), proto::key_sidecar(&rk));
    assert_eq!(
        keys::vc_sidecar_key(&rk, "abc123").unwrap(),
        proto::key_vc_sidecar(&rk, "abc123")
    );
    assert!(keys::vc_sidecar_key(&rk, "ABC123").is_err());
    assert_eq!(
        keys::journal_segment_key(&d, 412),
        proto::key_journal_segment(&d, 412)
    );
    assert_eq!(
        keys::journal_segment_key(&d, 412),
        format!(
            "{}journal/{d}/{}",
            keys::CONTROL_PREFIX,
            rrcloud_core::journal::format_segment_filename(412)
        )
    );
    assert_eq!(keys::manifest_key(&d), proto::key_manifest(&d));
    assert_eq!(
        keys::device_registry_key(&d),
        proto::key_device_registry(&d)
    );
    assert_eq!(keys::device_retired_key(&d), proto::key_device_retired(&d));
    assert_eq!(keys::tombstone_key(&rk), proto::key_tombstone(&rk));
    assert_eq!(keys::preview_key(&cid), proto::key_preview(&cid));
    assert_eq!(
        keys::thumb_key(&cid, keys::ThumbSize::Medium),
        proto::key_thumb(&cid, proto::ThumbSize::Medium)
    );
    assert_eq!(keys::thumbpack_key(&rk), proto::key_thumbpack(&rk));
    // Every constructor still inverts through the engine's classifier.
    assert!(matches!(
        keys::classify_key(&keys::thumb_key(&cid, keys::ThumbSize::Small)),
        keys::KeyClass::Thumb {
            size: keys::ThumbSize::Small,
            ..
        }
    ));
}

#[test]
fn relkey_lanes_behave_as_documented() {
    for s in [
        "2026/10/IMG_0042.NEF",
        "caf\u{e9}/x.jpg",
        "x/com0",
        "x/console.log",
        "a/x.rr.y",
    ] {
        assert!(proto::RelKey::parse_wire(s).is_ok(), "{s}");
        assert!(proto::validate_relkey(s).is_ok(), "{s}");
    }
    for s in [
        "",
        "/a",
        "a\\b",
        "C:/x",
        "a/../b",
        "a//b",
        "a./b",
        "a /b",
        "x/CON",
        "x/Com1.txt",
        "x/COM\u{b9}.jpg",
        "a/.rr.part-foo",
        "a\tb",
        "cafe\u{301}",
        "x/lpt9",
    ] {
        assert!(
            proto::RelKey::parse_wire(s).is_err(),
            "wire lane rejects {s:?}"
        );
        assert!(
            proto::validate_relkey(s).is_err(),
            "validator rejects {s:?}"
        );
    }
    // The local lane normalizes NFD → NFC (macOS filenames); the wire lane never rewrites.
    assert_eq!(
        proto::RelKey::new("cafe\u{301}").unwrap().as_str(),
        "caf\u{e9}"
    );
    assert!(matches!(
        proto::RelKey::parse_wire("cafe\u{301}"),
        Err(proto::RelKeyError::NotNfc(_))
    ));
    // `relkey()` from a path is the local lane, surfaced through the engine's KeyError.
    let root = std::path::Path::new("/lib");
    assert!(matches!(
        keys::relkey(std::path::Path::new("/lib/x/CON.txt"), root),
        Err(keys::KeyError::RelKey(proto::RelKeyError::WindowsReserved(
            _
        )))
    ));
    assert!(matches!(
        keys::relkey(std::path::Path::new("/elsewhere/a.NEF"), root),
        Err(keys::KeyError::OutsideRoot(_))
    ));
}
