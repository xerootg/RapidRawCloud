//! The generated Rust SDK must round-trip every fixture byte-for-byte (fixtures are
//! in canonical encoding order) and enforce the fail-closed rules.

use std::path::PathBuf;

use rrcloud_proto::*;

fn fixture(name: &str) -> String {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures").join(name);
    std::fs::read_to_string(&p).unwrap_or_else(|e| panic!("{}: {e}", p.display()))
}

#[test]
fn journal_segment_round_trips_byte_for_byte() {
    let text = fixture("journal_segment.v1.ndjson");
    let entries = decode_journal_segment(text.as_bytes()).expect("decode");
    assert_eq!(entries.len(), 5);
    assert_eq!(entries[1].op, Op::Put);
    assert_eq!(entries[1].kind, Kind::Sidecar);
    assert_eq!(entries[1].rating, Some(3));
    assert_eq!(entries[1].vv.len(), 2);
    assert_eq!(entries[3].op, Op::Move);
    assert_eq!(entries[3].from_key.as_ref().map(|k| k.as_str()), Some("library/2026/10/IMG_0042.NEF"));
    assert_eq!(entries[4].op, Op::Attest);
    let encoded = encode_journal_segment(&entries).expect("encode");
    assert_eq!(String::from_utf8(encoded).unwrap(), text);
}

#[test]
fn journal_version_gate_fails_closed() {
    let bad = r#"{"v":2,"seq":1,"ts":0,"device":"0f6b2a1e-1111-4222-8333-944444444444","op":"put","kind":"original","key":"library/x","vv":{}}"#;
    assert!(matches!(JournalEntry::from_json(bad), Err(ProtoError::UnsupportedVersion { version: 2 })));
    let none = r#"{"seq":1,"ts":0,"device":"0f6b2a1e-1111-4222-8333-944444444444","op":"put","kind":"original","key":"library/x","vv":{}}"#;
    assert!(matches!(JournalEntry::from_json(none), Err(ProtoError::MissingVersion)));
    let malformed = r#"{"v":"1","seq":1,"ts":0,"device":"0f6b2a1e-1111-4222-8333-944444444444","op":"put","kind":"original","key":"library/x","vv":{}}"#;
    assert!(matches!(JournalEntry::from_json(malformed), Err(ProtoError::MalformedVersion { .. })));
    // Unknown optional fields are ignored (min-reader rule).
    let extra = r#"{"v":1,"seq":1,"ts":0,"device":"0f6b2a1e-1111-4222-8333-944444444444","op":"put","kind":"original","key":"library/x","vv":{},"future_field":[1,2,3]}"#;
    assert!(JournalEntry::from_json(extra).is_ok());
}

#[test]
fn version_vector_drops_zero_components() {
    let e = JournalEntry::from_json(r#"{"v":1,"seq":1,"ts":0,"device":"0f6b2a1e-1111-4222-8333-944444444444","op":"put","kind":"original","key":"library/x","vv":{"0f6b2a1e-1111-4222-8333-944444444444":1,"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa":0}}"#).unwrap();
    assert_eq!(e.vv.len(), 1);
    let mut a = VersionVector::new();
    a.set(DeviceId::new("0f6b2a1e-1111-4222-8333-944444444444").unwrap(), 2);
    let mut b = a.clone();
    b.bump(&DeviceId::new("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa").unwrap());
    assert_eq!(a.compare(&b), VvOrder::Less);
    assert_eq!(b.compare(&a), VvOrder::Greater);
    assert_eq!(a.compare(&a), VvOrder::Equal);
    let mut c = VersionVector::new();
    c.set(DeviceId::new("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa").unwrap(), 1);
    assert_eq!(a.compare(&c), VvOrder::Concurrent);
}

#[test]
fn tombstone_and_device_entry_round_trip() {
    let t = fixture("tombstone.json");
    let ts = Tombstone::from_json(t.trim()).unwrap();
    assert_eq!(ts.kinds, vec![Kind::Original, Kind::Sidecar, Kind::Xmp]);
    assert_eq!(ts.to_json().unwrap(), t.trim());
    let d = fixture("device_entry.json");
    let de = DeviceEntry::from_json(d.trim()).unwrap();
    assert_eq!(de.proto.read, vec![1]);
    assert_eq!(de.to_json().unwrap(), d.trim());
}

#[test]
fn manifest_round_trips_through_gzip() {
    let nd = fixture("manifest.ndjson");
    // Build the document from the plain lines, gzip it with the SDK, decode it back.
    let mut lines = nd.lines();
    let header = ManifestHeader::from_json(lines.next().unwrap()).unwrap();
    let doc = Manifest {
        header,
        rows: lines.clone().filter(|l| l.starts_with("{\"key\"")).map(|l| ManifestRow::from_json(l).unwrap()).collect(),
        deleted: lines.filter(|l| l.starts_with("{\"del\"")).map(|l| DeletedRow::from_json(l).unwrap()).collect(),
    };
    let gz = encode_manifest(&doc).unwrap();
    let back = decode_manifest(&gz).unwrap();
    assert_eq!(back, doc);
    assert_eq!(back.rows.len(), 2);
    assert_eq!(back.deleted.len(), 1);
    // The re-encoded NDJSON equals the fixture text.
    let mut again = String::new();
    again.push_str(&back.header.to_json().unwrap());
    again.push('\n');
    for r in &back.rows {
        again.push_str(&r.to_json().unwrap());
        again.push('\n');
    }
    for r in &back.deleted {
        again.push_str(&r.to_json().unwrap());
        again.push('\n');
    }
    assert_eq!(again, nd);
}

#[test]
fn pairing_documents_round_trip_in_camel_case() {
    let pi = fixture("pairing_info.json");
    let info = PairingInfo::from_json(pi.trim()).unwrap();
    assert_eq!(info.config_endpoint, "/api/config");
    assert_eq!(info.to_json().unwrap(), pi.trim());
    // Missing default-able field → default applied.
    assert_eq!(info.redirect_uris, vec![PAIRING_REDIRECT_URI, PAIRING_REDIRECT_URI_RAW2DNG]);
    let min = PairingInfo::from_json(r#"{"version":1,"issuer":"https://i","clientId":"c"}"#).unwrap();
    assert_eq!(min.config_endpoint, "/api/config");
    assert!(min.redirect_uris.is_empty(), "older services omit the list");
    let pc = fixture("pairing_config.json");
    let doc = PairingConfigDoc::from_json(pc.trim()).unwrap();
    assert_eq!(doc.sync.bucket, "my-photos");
    assert!(doc.sync.worker_backfill);
    assert_eq!(doc.to_json().unwrap(), pc.trim());
    let minimal = PairingConfigDoc::from_json(r#"{"sync":{"endpoint":"https://e","bucket":"b"},"credentials":{"accessKeyId":"k","secretAccessKey":"s"}}"#).unwrap();
    assert!(!minimal.sync.worker_backfill);
    assert_eq!(minimal.sync.cache_size_gb, 8);
    assert!(minimal.sync.force_path_style);
    assert!(!minimal.sync.upload_requires_unmetered);
    assert!(!minimal.sync.upload_requires_charging);
}

#[test]
fn scalars_validate_like_the_engine() {
    assert!(DeviceId::new("0f6b2a1e-1111-4222-8333-944444444444").is_ok());
    assert!(DeviceId::new("0F6B2A1E-1111-4222-8333-944444444444").is_err());
    assert!(DeviceId::new("0f6b2a1e-1111-1222-8333-944444444444").is_err());
    assert!(Blake3Hex::new("6a0f8e2d3c4b5a69788796a5b4c3d2e1f0f1e2d3c4b5a69788796a5b4c3d2e1f").is_ok());
    assert!(Blake3Hex::new("6A0F").is_err());
    assert!(RelKey::new("2026/10/IMG_0042.NEF").is_ok());
    for bad in ["", "/a", "a\\b", "C:/x", "a/../b", "a//b", "a./b", "a /b", "x/CON", "x/Com1.txt", "x/COM\u{b9}.jpg", "a/.rr.part-foo", "a\tb", "cafe\u{301}"] {
        assert!(RelKey::parse_wire(bad).is_err(), "{bad:?} should be rejected on the wire");
    }
    assert!(RelKey::new("x/com0").is_ok());
    assert!(RelKey::new("caf\u{e9}/x.jpg").is_ok());
    // The local lane normalizes NFD → NFC; the wire lane (what decoders use) rejects it.
    assert_eq!(RelKey::new("cafe\u{301}").unwrap().as_str(), "caf\u{e9}");
    assert!(matches!(RelKey::parse_wire("cafe\u{301}"), Err(RelKeyError::NotNfc(_))));
    assert!(matches!(RelKey::new("x/CON.txt"), Err(RelKeyError::WindowsReserved(_))));
    assert!(matches!(RelKey::new("a/b:c"), Err(RelKeyError::Colon(_))));
    assert!(matches!(RelKey::new("a".repeat(RELKEY_MAX_BYTES + 1)), Err(RelKeyError::TooLong { .. })));
    // A manifest without a header line is a fail-closed decode error.
    let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    std::io::Write::write_all(&mut gz, b"").unwrap();
    assert!(matches!(decode_manifest(&gz.finish().unwrap()), Err(ProtoError::MissingHeader)));
    // … and so is a header with an unsupported proto.
    assert!(matches!(
        ManifestHeader::from_json(r#"{"written_server_ts":1,"cursors":{},"proto":2}"#),
        Err(ProtoError::UnsupportedVersion { version: 2 })
    ));
    // Deserializing a non-NFC relkey inside a document fails closed.
    assert!(Tombstone::from_json(r#"{"relkey":"cafe\u0301","vv":{},"device":"0f6b2a1e-1111-4222-8333-944444444444","server_ts":1,"kinds":[]}"#).is_err());
}

#[test]
fn keys_match_the_reference_table() {
    let d = DeviceId::new("0f6b2a1e-1111-4222-8333-944444444444").unwrap();
    let rk = RelKey::new("2026/10/IMG_0001.NEF").unwrap();
    let folder = RelKey::new("2026/10").unwrap();
    let cid = ContentId::new("6a0f8e2d3c4b5a69788796a5b4c3d2e1f0f1e2d3c4b5a69788796a5b4c3d2e1f").unwrap();
    for line in fixture("keys.expected").lines() {
        let mut it = line.split('\t');
        let (name, _input, expected) = (it.next().unwrap(), it.next().unwrap(), it.next().unwrap());
        let got = match name {
            "library_original" => key_library_original(&rk),
            "sidecar" => key_sidecar(&rk),
            "vc_sidecar" => key_vc_sidecar(&rk, "abc123"),
            "journal_segment" => key_journal_segment(&d, 412),
            "manifest" => key_manifest(&d),
            "device_registry" => key_device_registry(&d),
            "device_retired" => key_device_retired(&d),
            "tombstone" => key_tombstone(&rk),
            "preview" => key_preview(&cid),
            "thumb" => key_thumb(&cid, ThumbSize::Medium),
            "thumbpack" => key_thumbpack(&folder),
            "pairing_user_config" => key_pairing_user_config("alice"),
            other => panic!("unknown key {other}"),
        };
        assert_eq!(got, expected, "key {name}");
    }
}

#[test]
fn constants_are_the_documented_values() {
    assert_eq!(JOURNAL_VERSION, 1);
    assert_eq!(MANIFEST_PROTO, 1);
    assert_eq!(SEGMENT_MAX_ENTRIES, 1000);
    assert_eq!(SEGMENT_MAX_BYTES, 1024 * 1024);
    assert_eq!(LIBRARY_PREFIX, "library/");
    assert_eq!(CONTROL_PREFIX, ".rrcloud/v1/");
    assert_eq!(PROTO_READ, &[1]);
    assert_eq!(LAGGARD_CAP_SECS, 14 * 86400);
}
