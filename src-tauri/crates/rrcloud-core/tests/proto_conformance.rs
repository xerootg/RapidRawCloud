//! The engine's hand-written wire types and the generated `rrcloud-proto` SDK are two
//! implementations of one definition (`protocol/rrcloud.protocol.toml`). On every
//! shared fixture both must (a) decode, (b) re-encode to identical bytes, and (c) agree
//! on the key schema and the relkey rules. A drift in either side fails here, not in a bucket.

use std::path::PathBuf;

use rrcloud_core::journal::{decode_segment, Kind, Op, Tombstone};
use rrcloud_core::keys;
use rrcloud_core::manifest::{
    decode_manifest, encode_manifest, DeletedRow, Manifest, ManifestHeader, ManifestRow,
};
use rrcloud_core::publisher::DeviceEntry;
use rrcloud_proto as proto;

fn fixture(name: &str) -> String {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../protocol/fixtures")
        .join(name);
    std::fs::read_to_string(&p).unwrap_or_else(|e| panic!("{}: {e}", p.display()))
}

fn op_wire(op: Op) -> &'static str {
    match op {
        Op::Put => "put",
        Op::Del => "del",
        Op::Move => "move",
        Op::Attest => "attest",
    }
}

fn kind_wire(kind: Kind) -> &'static str {
    match kind {
        Kind::Original => "original",
        Kind::Sidecar => "sidecar",
        Kind::Xmp => "xmp",
        Kind::Preview => "preview",
        Kind::Thumb => "thumb",
        Kind::Thumbpack => "thumbpack",
        Kind::Albums => "albums",
        Kind::Presets => "presets",
    }
}

#[test]
fn journal_entries_encode_identically_in_engine_and_sdk() {
    let text = fixture("journal_segment.v1.ndjson");
    let engine = decode_segment(text.as_bytes()).expect("engine decodes");
    let sdk = proto::decode_journal_segment(text.as_bytes()).expect("sdk decodes");
    assert_eq!(engine.len(), sdk.len());
    for (line, (e, s)) in text.lines().zip(engine.iter().zip(sdk.iter())) {
        assert_eq!(e.to_json_line().unwrap(), line, "engine re-encode");
        assert_eq!(s.to_json().unwrap(), line, "sdk re-encode");
        assert_eq!(e.seq, s.seq);
        assert_eq!(op_wire(e.op), s.op.as_wire());
        assert_eq!(kind_wire(e.kind), s.kind.as_wire());
        assert_eq!(e.device.as_str(), s.device.as_str());
        assert_eq!(e.vv.len(), s.vv.len());
        for (d, c) in s.vv.iter() {
            assert_eq!(
                e.vv.get(&rrcloud_core::clock::DeviceId::new(d.as_str()).unwrap()),
                *c
            );
        }
    }
}

#[test]
fn tombstone_and_device_entry_agree() {
    let t = fixture("tombstone.json");
    let e: Tombstone = serde_json::from_str(t.trim()).unwrap();
    let s = proto::Tombstone::from_json(t.trim()).unwrap();
    assert_eq!(serde_json::to_string(&e).unwrap(), s.to_json().unwrap());
    assert_eq!(serde_json::to_string(&e).unwrap(), t.trim());
    let d = fixture("device_entry.json");
    let e: DeviceEntry = serde_json::from_str(d.trim()).unwrap();
    let s = proto::DeviceEntry::from_json(d.trim()).unwrap();
    assert_eq!(serde_json::to_string(&e).unwrap(), s.to_json().unwrap());
    assert_eq!(e.proto.read, s.proto.read);
}

#[test]
fn manifests_gzip_encode_and_decode_across_implementations() {
    let nd = fixture("manifest.ndjson");
    let mut lines = nd.lines();
    let header: ManifestHeader = serde_json::from_str(lines.next().unwrap()).unwrap();
    let rows: Vec<ManifestRow> = lines
        .clone()
        .filter(|l| l.starts_with("{\"key\""))
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    let deleted: Vec<DeletedRow> = lines
        .filter(|l| l.starts_with("{\"del\""))
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    let engine_doc = Manifest {
        header: header.clone(),
        rows: rows.clone(),
        deleted: deleted.clone(),
    };
    let gz_engine = encode_manifest(&engine_doc).unwrap();
    // The SDK decodes what the engine wrote …
    let sdk_doc = proto::decode_manifest(&gz_engine).unwrap();
    assert_eq!(sdk_doc.manifest_rows.len(), rows.len());
    assert_eq!(sdk_doc.deleted_rows.len(), deleted.len());
    assert_eq!(sdk_doc.header.as_ref().unwrap().proto, header.proto);
    // … and the engine decodes what the SDK wrote.
    let gz_sdk = proto::encode_manifest(&sdk_doc).unwrap();
    let back = decode_manifest(&gz_sdk).unwrap();
    assert_eq!(back, engine_doc);
}

#[test]
fn key_schema_agrees() {
    let d = rrcloud_core::clock::DeviceId::new("0f6b2a1e-1111-4222-8333-944444444444").unwrap();
    let pd = proto::DeviceId::new("0f6b2a1e-1111-4222-8333-944444444444").unwrap();
    let rk = keys::RelKey::new("2026/10/IMG_0001.NEF").unwrap();
    let prk = proto::RelKey::new("2026/10/IMG_0001.NEF").unwrap();
    let cid = rrcloud_core::semhash::ContentId::parse(
        "6a0f8e2d3c4b5a69788796a5b4c3d2e1f0f1e2d3c4b5a69788796a5b4c3d2e1f",
    )
    .unwrap();
    let pcid =
        proto::ContentId::new("6a0f8e2d3c4b5a69788796a5b4c3d2e1f0f1e2d3c4b5a69788796a5b4c3d2e1f")
            .unwrap();
    assert_eq!(keys::library_key(&rk), proto::key_library_original(&prk));
    assert_eq!(keys::sidecar_key(&rk), proto::key_sidecar(&prk));
    assert_eq!(
        keys::vc_sidecar_key(&rk, "abc123").unwrap(),
        proto::key_vc_sidecar(&prk, "abc123")
    );
    assert_eq!(
        keys::journal_segment_key(&d, 412),
        proto::key_journal_segment(&pd, 412)
    );
    assert_eq!(keys::manifest_key(&d), proto::key_manifest(&pd));
    assert_eq!(
        keys::device_registry_key(&d),
        proto::key_device_registry(&pd)
    );
    assert_eq!(keys::device_retired_key(&d), proto::key_device_retired(&pd));
    assert_eq!(keys::tombstone_key(&rk), proto::key_tombstone(&prk));
    assert_eq!(keys::preview_key(&cid), proto::key_preview(&pcid));
    assert_eq!(
        keys::thumb_key(&cid, keys::ThumbSize::Medium),
        proto::key_thumb(&pcid, proto::ThumbSize::Medium)
    );
    assert_eq!(keys::thumbpack_key(&rk), proto::key_thumbpack(&prk));
}

#[test]
fn relkey_rules_agree_on_the_edge_cases() {
    for s in [
        "2026/10/IMG_0042.NEF",
        "caf\u{e9}/x.jpg",
        "x/com0",
        "x/console.log",
        "a/x.rr.y",
    ] {
        assert!(keys::RelKey::parse_wire(s).is_ok(), "{s}");
        assert!(proto::RelKey::new(s).is_ok(), "{s}");
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
            keys::RelKey::parse_wire(s).is_err(),
            "engine should reject {s:?}"
        );
        assert!(proto::RelKey::new(s).is_err(), "sdk should reject {s:?}");
    }
}
