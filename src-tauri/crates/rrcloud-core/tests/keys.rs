//! Failing tests for `rrcloud_core::keys` (architecture §1.1–§1.2).
//!
//! Pins the relkey mapping rules (rejections, NFC normalization, round
//! trips), every typed key constructor's exact output shape, and
//! `classify_key` as the exact inverse of every constructor.

use std::path::{Path, PathBuf};

use rrcloud_core::clock::DeviceId;
use rrcloud_core::keys::{
    classify_key, device_registry_key, device_retired_key, journal_segment_key, library_key,
    local_path, manifest_key, preview_key, relkey, sidecar_key, thumb_key, thumbpack_key,
    tombstone_key, vc_sidecar_key, KeyClass, RelKey, ThumbSize, ALBUMS_META_KEY, PRESETS_META_KEY,
};
use rrcloud_core::semhash::ContentId;

const DEV1: &str = "d1f0c2aa-9d2b-4a6e-8f1c-3b7d5e9a0c42";
const DEV2: &str = "a3b2e1d0-5c4f-4b3a-9e2d-1f0a9b8c7d6e";
const DEV3: &str = "0a1b2c3d-4e5f-4a6b-8c9d-0e1f2a3b4c5d";

fn dev(s: &str) -> DeviceId {
    DeviceId::new(s).expect("valid device id")
}

fn rk(s: &str) -> RelKey {
    RelKey::new(s).expect("valid relkey")
}

// ---------------------------------------------------------------------------
// RelKey validation
// ---------------------------------------------------------------------------

#[test]
fn relkey_accepts_plain_paths() {
    for ok in [
        "IMG_0001.NEF",
        "2026/10/IMG_0042.NEF",
        "folder with space/file name.jpg",
        "a/b/c/d/e.tif",
        "no_extension",
        "dots.in.name.v2.dng",
        ".hidden/file.jpg",
        "x",
    ] {
        let k = RelKey::new(ok).unwrap_or_else(|e| panic!("{ok:?} rejected: {e}"));
        assert_eq!(k.as_str(), ok, "valid ASCII relkey must be unchanged");
    }
}

#[test]
fn relkey_rejects_traversal_and_junk() {
    let bad = [
        "../x",
        "a/../b",
        "a/..",
        "..",
        ".\u{0}",
        "C:\\x",
        "a\\b",
        "/abs/path.NEF",
        "",
        ".",
        "./a",
        "a/./b",
        "a//b",
        "a/",
        "a\u{7}b",
        "nul\u{0}byte",
        "tab\tseparated",
        "new\nline",
    ];
    for s in bad {
        assert!(RelKey::new(s).is_err(), "{s:?} must be rejected");
    }
}

#[test]
fn relkey_rejects_colon_segments() {
    // Review finding (round 0, blocker): on Windows, PathBuf::push of a
    // "C:"-style drive-relative segment REPLACES the accumulated path, so a
    // remote-controlled key like `library/C:/Users/victim/evil` would make
    // local_path() discard the sync root and write anywhere on C:. ':' is
    // illegal in Windows filenames anyway, so it is rejected outright.
    let bad = [
        "C:/Windows/evil.dll",
        "C:evil",
        "a/C:/b",
        "C:",
        "x/C:",
        "a:b",
        "z:/x",
    ];
    for s in bad {
        assert!(RelKey::new(s).is_err(), "{s:?} must be rejected");
    }
}

#[test]
fn relkey_rejects_windows_reserved_names_and_trailing_dot_or_space() {
    // Review finding (round 1, major): Win32 resolves reserved device
    // names (CON/PRN/AUX/NUL/COM1-9/LPT1-9, with or without extension) in
    // any directory to the device, and strips trailing dots/spaces at
    // create time — so "a.jpg" and "a.jpg." are distinct bucket keys that
    // collide onto one Windows file (silent cross-key clobber), and a
    // benign Linux-created "aux.jpg" would hydrate into a device write.
    // These must be rejected at the mapping layer, not left to bite the
    // hydration unit.
    let bad = [
        "NUL",
        "nul",
        "con.jpg",
        "AUX.NEF",
        "2026/com1.raw",
        "LPT9.txt",
        "prn.tar.gz",
        "Con.jpg",
        "a.jpg ",
        "a.jpg.",
        "dir./x.jpg",
        "dir /x.jpg",
        "...",
    ];
    for s in bad {
        assert!(RelKey::new(s).is_err(), "{s:?} must be rejected");
    }
    // Near-misses stay valid: reservation is base-name-exact.
    let ok = [
        "console.jpg",
        "nullable.NEF",
        "com0.raw",
        "com10.raw",
        "lpt.txt",
        "aux1/file.jpg",
        "prnter.txt",
        "a. jpg",
    ];
    for s in ok {
        let k = RelKey::new(s).unwrap_or_else(|e| panic!("{s:?} rejected: {e}"));
        assert_eq!(k.as_str(), s);
    }
}

#[test]
fn relkey_rejects_superscript_com_lpt_variants() {
    // Review finding (round 2, minor): Win32's reserved-name parser treats
    // the Latin-1 superscript digits as digits, so COM¹/COM²/COM³ and
    // LPT¹/LPT²/LPT³ (U+00B9/U+00B2/U+00B3) are reserved alongside
    // COM1–COM9 per Microsoft's current file-naming documentation. NFC
    // does not decompose them (that is NFKC), so they survive relkey
    // normalization and must be rejected explicitly.
    let bad = [
        "com\u{b9}.jpg",
        "COM\u{b2}",
        "Com\u{b3}.NEF",
        "lpt\u{b9}",
        "LPT\u{b2}.txt",
        "2026/lpt\u{b3}.raw",
    ];
    for s in bad {
        assert!(RelKey::new(s).is_err(), "{s:?} must be rejected");
    }
    // Near-misses stay valid: only ¹ ² ³ exist in Latin-1 (U+2074 ⁴ is
    // not in Win32's reserved set), and the digit must be the whole rest
    // of the base name.
    let ok = [
        "com\u{2074}.jpg",
        "com\u{b9}0.raw",
        "co\u{b9}.jpg",
        "lpt\u{b2}x.txt",
    ];
    for s in ok {
        assert!(RelKey::new(s).is_ok(), "{s:?} must stay valid");
    }
}

#[test]
fn relkey_nfc_normalizes_composed_and_decomposed_to_same_key() {
    // "Käch.jpg": composed U+00E4 vs decomposed 'a' + U+0308.
    let composed = "K\u{e4}ch.jpg";
    let decomposed = "Ka\u{308}ch.jpg";
    assert_ne!(composed, decomposed, "test premise: distinct byte forms");
    let a = rk(composed);
    let b = rk(decomposed);
    assert_eq!(a, b, "both spellings must map to the same RelKey");
    assert_eq!(
        a.as_str(),
        composed,
        "the normalized form is NFC (composed)"
    );

    // Same inside a directory segment.
    let c = rk("2026/K\u{e4}ch/e\u{301}te\u{301}.NEF");
    let d = rk("2026/K\u{e4}ch/\u{e9}t\u{e9}.NEF");
    assert_eq!(c, d);
}

#[test]
fn relkey_is_ordered_and_hashable() {
    use std::collections::{BTreeSet, HashSet};
    let mut bt = BTreeSet::new();
    bt.insert(rk("b.NEF"));
    bt.insert(rk("a.NEF"));
    bt.insert(rk("a/b.NEF"));
    let ordered: Vec<&str> = bt.iter().map(|k| k.as_str()).collect();
    assert_eq!(ordered, vec!["a.NEF", "a/b.NEF", "b.NEF"]);

    let mut hs = HashSet::new();
    hs.insert(rk("a.NEF"));
    hs.insert(rk("a.NEF"));
    assert_eq!(hs.len(), 1);
}

#[test]
fn relkey_serde_round_trip_validates() {
    let k = rk("2026/10/IMG_0042.NEF");
    let json = serde_json::to_string(&k).expect("serialize");
    assert_eq!(json, "\"2026/10/IMG_0042.NEF\"");
    let back: RelKey = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(back, k);
    // Deserialization must run the same validation.
    assert!(serde_json::from_str::<RelKey>("\"../etc/passwd\"").is_err());
    assert!(serde_json::from_str::<RelKey>("\"/abs\"").is_err());
}

#[test]
fn relkey_wire_decode_rejects_non_nfc() {
    // Review finding (round 2, major): the wire lane (serde /
    // TryFrom<String>) must REJECT non-NFC input, never normalize it. A
    // relkey arriving inside a decoded document (Tombstone §2.7, manifest
    // deleted-set rows §2.3) names a bucket object; silently rewriting NFD
    // to NFC re-aims the record at the user's *distinct* NFC object — the
    // exact conflation classify_key's Foreign lane exists to prevent.
    // Validation-by-rewriting is not validation.
    let nfd = "\"Ka\\u0308ch.jpg\""; // JSON spelling of NFD "Käch.jpg"
    assert!(
        serde_json::from_str::<RelKey>(nfd).is_err(),
        "NFD relkey on the wire must fail to decode"
    );
    // The NFC spelling decodes fine and matches the constructor's output.
    let nfc: RelKey = serde_json::from_str("\"K\u{e4}ch.jpg\"").expect("NFC decodes");
    assert_eq!(nfc, rk("K\u{e4}ch.jpg"));

    // The strict wire parser is the same lane, callable directly.
    assert!(RelKey::parse_wire("Ka\u{308}ch.jpg").is_err());
    assert_eq!(
        RelKey::parse_wire("K\u{e4}ch.jpg").expect("NFC parses"),
        rk("K\u{e4}ch.jpg")
    );
    // ...and it still enforces every other relkey rule.
    assert!(RelKey::parse_wire("../x").is_err());
    assert!(RelKey::parse_wire("/abs").is_err());
    assert!(RelKey::parse_wire("a\\b").is_err());

    // The local path-mapping constructor keeps normalizing (macOS NFD
    // filenames legitimately need it) — the two lanes are deliberately
    // asymmetric.
    assert_eq!(rk("Ka\u{308}ch.jpg").as_str(), "K\u{e4}ch.jpg");
}

// ---------------------------------------------------------------------------
// relkey() / local_path() path mapping
// ---------------------------------------------------------------------------

#[test]
fn relkey_maps_paths_under_sync_root() {
    let root = Path::new("/data/library");
    let k = relkey(Path::new("/data/library/2026/10/IMG_0042.NEF"), root).expect("maps");
    assert_eq!(k.as_str(), "2026/10/IMG_0042.NEF");
}

#[test]
fn relkey_rejects_root_itself_and_escapes() {
    let root = Path::new("/data/library");
    assert!(relkey(Path::new("/data/library"), root).is_err());
    assert!(relkey(Path::new("/data/library/"), root).is_err());
    assert!(relkey(Path::new("/other/place/x.NEF"), root).is_err());
    assert!(relkey(Path::new("/data/library/../etc/passwd"), root).is_err());
    assert!(relkey(Path::new("/data/library/a/../../b"), root).is_err());
    // Sibling directory sharing the root as a string prefix is NOT inside.
    assert!(relkey(Path::new("/data/library2/x.NEF"), root).is_err());
}

#[test]
fn local_path_joins_relkey_onto_root() {
    let root = Path::new("/data/library");
    let p = local_path(&rk("2026/10/IMG_0042.NEF"), root);
    assert_eq!(p, PathBuf::from("/data/library/2026/10/IMG_0042.NEF"));
}

#[test]
fn relkey_local_path_round_trip() {
    let root = Path::new("/data/library");
    for rel in [
        "IMG_0001.NEF",
        "2026/10/IMG_0042.NEF",
        "K\u{e4}ch/\u{e9}t\u{e9}.jpg",
        "folder with space/file.dng",
    ] {
        let p = root.join(rel);
        let k = relkey(&p, root).expect("maps");
        assert_eq!(local_path(&k, root), p, "round trip for {rel:?}");
        // Idempotence: mapping the joined path again yields the same key.
        assert_eq!(relkey(&local_path(&k, root), root).expect("re-maps"), k);
    }
}

// ---------------------------------------------------------------------------
// Key constructors: exact shapes
// ---------------------------------------------------------------------------

#[test]
fn library_and_sidecar_key_shapes() {
    let k = rk("2026/10/IMG_0042.NEF");
    assert_eq!(library_key(&k), "library/2026/10/IMG_0042.NEF");
    assert_eq!(sidecar_key(&k), "library/2026/10/IMG_0042.NEF.rrdata");
    assert_eq!(
        vc_sidecar_key(&k, "ab12cd").expect("valid vc suffix"),
        "library/2026/10/IMG_0042.NEF.ab12cd.rrdata"
    );
}

#[test]
fn vc_sidecar_key_requires_6_lowercase_hex() {
    let k = rk("a.NEF");
    for bad in [
        "AB12CD",
        "ab12c",
        "ab12cde",
        "ab12cg",
        "",
        "ab 12c",
        "ab12c\u{e9}",
    ] {
        assert!(vc_sidecar_key(&k, bad).is_err(), "{bad:?} must be rejected");
    }
}

#[test]
fn control_plane_key_shapes() {
    let d = dev(DEV1);
    assert_eq!(
        journal_segment_key(&d, 0x19a),
        format!(".rrcloud/v1/journal/{DEV1}/000000000000019a.v1.ndjson")
    );
    assert_eq!(
        journal_segment_key(&d, 0),
        format!(".rrcloud/v1/journal/{DEV1}/0000000000000000.v1.ndjson")
    );
    assert_eq!(
        manifest_key(&d),
        format!(".rrcloud/v1/manifests/{DEV1}.json.gz")
    );
    assert_eq!(
        device_registry_key(&d),
        format!(".rrcloud/v1/devices/{DEV1}.json")
    );
    assert_eq!(
        device_retired_key(&d),
        format!(".rrcloud/v1/devices/{DEV1}.retired")
    );
    assert_eq!(ALBUMS_META_KEY, ".rrcloud/v1/meta/albums.json");
    assert_eq!(PRESETS_META_KEY, ".rrcloud/v1/meta/presets.json");
}

#[test]
fn tombstone_key_is_blake3_prefix32_of_relkey() {
    let k = rk("2026/10/IMG_0042.NEF");
    let expected_hex = blake3::hash(k.as_str().as_bytes()).to_hex();
    let expected32 = &expected_hex.as_str()[..32];
    assert_eq!(
        tombstone_key(&k),
        format!(".rrcloud/v1/tombstones/{expected32}.json")
    );
    // Deterministic and distinct per relkey.
    assert_eq!(tombstone_key(&k), tombstone_key(&k));
    assert_ne!(tombstone_key(&k), tombstone_key(&rk("other.NEF")));
}

#[test]
fn preview_and_thumb_key_shapes() {
    let cid = ContentId::from_bytes(b"original raw bytes");
    let hexid = cid.as_str().to_string();
    assert_eq!(hexid.len(), 64);
    assert_eq!(
        preview_key(&cid),
        format!(".rrcloud/v1/previews/{hexid}.pxy.dng")
    );
    assert_eq!(
        thumb_key(&cid, ThumbSize::Small),
        format!(".rrcloud/v1/thumbs/{hexid}_small.jpg")
    );
    assert_eq!(
        thumb_key(&cid, ThumbSize::Medium),
        format!(".rrcloud/v1/thumbs/{hexid}_medium.jpg")
    );
}

#[test]
fn thumbpack_key_is_blake3_prefix32_of_folder_relkey() {
    // Review finding (round 1): 128-bit prefix, same as tombstones — the
    // previous 64-bit truncation was adversarially collidable at ~2^32
    // work and birthday-weak around 2^32 folders.
    let folder = rk("2026/10");
    let expected_hex = blake3::hash(folder.as_str().as_bytes()).to_hex();
    let expected32 = &expected_hex.as_str()[..32];
    assert_eq!(
        thumbpack_key(&folder),
        format!(".rrcloud/v1/thumbpacks/{expected32}.tar")
    );
}

// ---------------------------------------------------------------------------
// classify_key: exact inverse of every constructor (property-style)
// ---------------------------------------------------------------------------

/// A generated spread of valid relkeys. Stems deliberately avoid ending in
/// `.<6hex>` (the documented upstream virtual-copy ambiguity) and avoid the
/// reserved `.rrdata` / `.xmp` suffixes (those are covered separately).
fn gen_relkeys() -> Vec<RelKey> {
    let dirs = ["", "2026/10", "deep/n e s t/K\u{e4}ch"];
    let stems = [
        "IMG_0001",
        "r\u{e4}w photo",
        "\u{65e5}\u{672c}\u{8a9e}",
        "a.b.c",
        "x",
    ];
    let exts = ["NEF", "jpg", "dng", "tif"];
    let mut out = Vec::new();
    for d in dirs {
        for s in stems {
            for e in exts {
                let p = if d.is_empty() {
                    format!("{s}.{e}")
                } else {
                    format!("{d}/{s}.{e}")
                };
                out.push(rk(&p));
            }
        }
    }
    out
}

#[test]
fn classify_inverts_library_keys() {
    for rel in gen_relkeys() {
        match classify_key(&library_key(&rel)) {
            KeyClass::Original { relkey } => assert_eq!(relkey, rel),
            other => panic!("library_key({rel}) classified as {other:?}"),
        }
    }
}

#[test]
fn classify_inverts_sidecar_keys() {
    for rel in gen_relkeys() {
        match classify_key(&sidecar_key(&rel)) {
            KeyClass::Sidecar { relkey, vc } => {
                assert_eq!(relkey, rel);
                assert_eq!(vc, None);
            }
            other => panic!("sidecar_key({rel}) classified as {other:?}"),
        }
        for suffix in ["000000", "ab12cd", "ffffff"] {
            match classify_key(&vc_sidecar_key(&rel, suffix).expect("valid suffix")) {
                KeyClass::Sidecar { relkey, vc } => {
                    assert_eq!(relkey, rel);
                    assert_eq!(vc.as_deref(), Some(suffix));
                }
                other => panic!("vc_sidecar_key({rel}, {suffix}) classified as {other:?}"),
            }
        }
    }
}

#[test]
fn classify_separates_xmp_from_original() {
    let rel = rk("2026/10/IMG_0042.xmp");
    match classify_key(&library_key(&rel)) {
        KeyClass::Xmp { relkey } => assert_eq!(relkey, rel),
        other => panic!("xmp library key classified as {other:?}"),
    }
    // But the .xmp's own sidecar is a sidecar of the .xmp relkey.
    match classify_key(&sidecar_key(&rel)) {
        KeyClass::Sidecar { relkey, vc } => {
            assert_eq!(relkey, rel);
            assert_eq!(vc, None);
        }
        other => panic!("sidecar of xmp classified as {other:?}"),
    }
}

#[test]
fn classify_inverts_journal_keys() {
    for ds in [DEV1, DEV2, DEV3] {
        let d = dev(ds);
        for seq in [0u64, 1, 0x19a, 1_000_000, u64::MAX] {
            match classify_key(&journal_segment_key(&d, seq)) {
                KeyClass::Journal {
                    device,
                    seq: s,
                    version,
                } => {
                    assert_eq!(device, d);
                    assert_eq!(s, seq);
                    assert_eq!(version, 1, "constructor emits v1 segments");
                }
                other => panic!("journal key ({ds}, {seq}) classified as {other:?}"),
            }
        }
    }
}

#[test]
fn classify_journal_carries_the_segment_format_version() {
    // Review finding (round 0): the filename carries the format version
    // precisely so the apply loop can halt-and-surface "app update
    // required" (§2.2 min-reader rule) BEFORE GET+decode. Dropping it from
    // KeyClass::Journal would route a future-version segment to an opaque
    // JSON/corruption error instead.
    let key = format!(".rrcloud/v1/journal/{DEV1}/000000000000019a.v2.ndjson");
    match classify_key(&key) {
        KeyClass::Journal {
            device,
            seq,
            version,
        } => {
            assert_eq!(device, dev(DEV1));
            assert_eq!(seq, 0x19a);
            assert_eq!(version, 2);
        }
        other => panic!("v2 journal key classified as {other:?}"),
    }
}

#[test]
fn classify_inverts_device_scoped_keys() {
    for ds in [DEV1, DEV2, DEV3] {
        let d = dev(ds);
        assert_eq!(
            classify_key(&manifest_key(&d)),
            KeyClass::Manifest { device: d.clone() }
        );
        assert_eq!(
            classify_key(&device_registry_key(&d)),
            KeyClass::DeviceRegistry { device: d.clone() }
        );
        assert_eq!(
            classify_key(&device_retired_key(&d)),
            KeyClass::DeviceRetired { device: d.clone() }
        );
    }
}

#[test]
fn classify_inverts_content_and_hash_keys() {
    for rel in [rk("a.NEF"), rk("2026/10/IMG_0042.NEF")] {
        match classify_key(&tombstone_key(&rel)) {
            KeyClass::Tombstone { hash32 } => {
                let expected = blake3::hash(rel.as_str().as_bytes());
                assert_eq!(hash32, expected.to_hex().as_str()[..32]);
            }
            other => panic!("tombstone key classified as {other:?}"),
        }
    }
    for data in [b"one".as_slice(), b"two".as_slice()] {
        let cid = ContentId::from_bytes(data);
        assert_eq!(
            classify_key(&preview_key(&cid)),
            KeyClass::Preview {
                content_id: cid.clone()
            }
        );
        for size in [ThumbSize::Small, ThumbSize::Medium] {
            assert_eq!(
                classify_key(&thumb_key(&cid, size)),
                KeyClass::Thumb {
                    content_id: cid.clone(),
                    size
                }
            );
        }
    }
    let folder = rk("2026/10");
    match classify_key(&thumbpack_key(&folder)) {
        KeyClass::Thumbpack { hash32 } => {
            let expected = blake3::hash(folder.as_str().as_bytes());
            assert_eq!(hash32, expected.to_hex().as_str()[..32]);
        }
        other => panic!("thumbpack key classified as {other:?}"),
    }
}

#[test]
fn classify_meta_keys() {
    assert_eq!(classify_key(ALBUMS_META_KEY), KeyClass::MetaAlbums);
    assert_eq!(classify_key(PRESETS_META_KEY), KeyClass::MetaPresets);
}

#[test]
fn classify_rejects_foreign_and_malformed_keys() {
    let foreign = [
        "",
        "settings.json",
        "library/",
        "library",
        "library/../etc/passwd",
        "library/a\\b.NEF",
        "library/a//b.NEF",
        "librarium/a.NEF",
        ".rrcloud/v2/journal/d1f0c2aa-9d2b-4a6e-8f1c-3b7d5e9a0c42/0000000000000000.v1.ndjson",
        ".rrcloud/v1/unknown/x",
        ".rrcloud/v1/journal/not-a-uuid/0000000000000000.v1.ndjson",
        // 15 hex digits instead of 16
        ".rrcloud/v1/journal/d1f0c2aa-9d2b-4a6e-8f1c-3b7d5e9a0c42/000000000000019.v1.ndjson",
        // uppercase seq hex
        ".rrcloud/v1/journal/d1f0c2aa-9d2b-4a6e-8f1c-3b7d5e9a0c42/000000000000019A.v1.ndjson",
        ".rrcloud/v1/manifests/not-a-uuid.json.gz",
        ".rrcloud/v1/manifests/d1f0c2aa-9d2b-4a6e-8f1c-3b7d5e9a0c42.json",
        ".rrcloud/v1/devices/d1f0c2aa-9d2b-4a6e-8f1c-3b7d5e9a0c42.toml",
        // 31-char tombstone hash
        ".rrcloud/v1/tombstones/0123456789abcdef0123456789abcde.json",
        // 16-hex thumbpack hash (pre-round-1 width; schema is now 32)
        ".rrcloud/v1/thumbpacks/0123456789abcdef.tar",
        // non-canonical segment version spellings (v01 aliases v1; v0 is
        // never emitted) — review finding (round 1)
        ".rrcloud/v1/journal/d1f0c2aa-9d2b-4a6e-8f1c-3b7d5e9a0c42/000000000000019a.v01.ndjson",
        ".rrcloud/v1/journal/d1f0c2aa-9d2b-4a6e-8f1c-3b7d5e9a0c42/000000000000019a.v0.ndjson",
        // 63-char content id
        ".rrcloud/v1/previews/af1349b9f5f9a1a6a0404dee36dcc9499bcb25c9adc112b7cc9a93cae41f326.pxy.dng",
        ".rrcloud/v1/thumbs/af1349b9f5f9a1a6a0404dee36dcc9499bcb25c9adc112b7cc9a93cae41f3262_large.jpg",
        ".rrcloud/v1/meta/other.json",
    ];
    for k in foreign {
        assert_eq!(classify_key(k), KeyClass::Foreign, "{k:?} must be Foreign");
    }
}

#[test]
fn classify_treats_non_nfc_library_keys_as_foreign() {
    // Review finding (round 0): rclone-synced buckets can hold NFD keys
    // (macOS stores NFD filenames, and §1.2 advertises rclone interop).
    // Silently NFC-normalizing inside classify_key would conflate two
    // coexisting NFD/NFC bucket objects into one relkey and break the
    // classify→constructor round trip (GET/PUT/tombstone would target the
    // NFC spelling while the object lives at the NFD key). A key whose
    // relpath is not already NFC is therefore adopted as Foreign — the
    // §2.3 safe lane.
    //
    // (This replaces the red-stage `classify_normalizes_relkey_text` test,
    // which pinned the conflating behavior and was objectively wrong.)
    let nfd = [
        "library/Ka\u{308}ch.jpg",
        "library/Ka\u{308}ch.NEF.rrdata",
        "library/Ka\u{308}ch.NEF.ab12cd.rrdata",
        "library/Ka\u{308}ch.xmp",
        "library/2026/Mu\u{308}nchen/IMG.NEF",
    ];
    for k in nfd {
        assert_eq!(classify_key(k), KeyClass::Foreign, "{k:?} must be Foreign");
    }
    // The NFC spelling still round-trips exactly through the constructors.
    match classify_key("library/K\u{e4}ch.jpg") {
        KeyClass::Original { relkey } => {
            assert_eq!(relkey, rk("K\u{e4}ch.jpg"));
            assert_eq!(library_key(&relkey), "library/K\u{e4}ch.jpg");
        }
        other => panic!("NFC library key classified as {other:?}"),
    }
}

#[test]
fn classify_treats_drive_relative_library_keys_as_foreign() {
    // Companion to `relkey_rejects_colon_segments`: no remote-controlled
    // bucket key with a colon segment may reach local_path().
    let keys = [
        "library/C:/Users/victim/evil",
        "library/C:evil.dll",
        "library/a/C:/b.NEF",
        "library/C:/Users/victim/evil.rrdata",
        "library/C:/x.xmp",
    ];
    for k in keys {
        assert_eq!(classify_key(k), KeyClass::Foreign, "{k:?} must be Foreign");
    }
}

#[test]
fn classify_treats_windows_hazard_library_keys_as_foreign() {
    // Companion to relkey_rejects_windows_reserved_names_and_trailing_dot_
    // or_space: no such remote-controlled bucket key may reach local_path()
    // — including via the sidecar-stem path.
    let keys = [
        "library/NUL",
        "library/con.jpg",
        "library/AUX.NEF",
        "library/2026/com1.raw",
        "library/com\u{b9}.jpg",
        "library/LPT\u{b2}.txt",
        "library/a.jpg ",
        "library/a.jpg.",
        "library/aux.NEF.rrdata",
        "library/con.xmp",
    ];
    for k in keys {
        assert_eq!(classify_key(k), KeyClass::Foreign, "{k:?} must be Foreign");
    }
}

#[test]
fn classify_shadows_rrdata_named_originals_as_sidecars() {
    // Review finding (round 1): the second documented ambiguity of the
    // suffix-based schema. A library file literally named "foo.rrdata" has
    // the same bucket key bytes as the sidecar of "foo", so classify_key
    // reports Sidecar — consistent with upstream, which already treats
    // every *.rrdata as a sidecar. Pinned so the reconcile loop (§2.3)
    // inherits an accurate inverse contract.
    match classify_key(&library_key(&rk("foo.rrdata"))) {
        KeyClass::Sidecar { relkey, vc } => {
            assert_eq!(relkey, rk("foo"));
            assert_eq!(vc, None);
        }
        other => panic!("library foo.rrdata classified as {other:?}, want Sidecar"),
    }
}

#[test]
fn classify_recognizes_uppercase_xmp() {
    // Review finding (round 0): upstream probes both with_extension("xmp")
    // and with_extension("XMP") (file_management.rs), so uppercase .XMP
    // interop files exist in real libraries and must get the §2.8
    // projection semantics, not Original treatment.
    for k in [
        "library/IMG_0042.XMP",
        "library/2026/10/IMG_0042.Xmp",
        "library/a.xMp",
    ] {
        match classify_key(k) {
            KeyClass::Xmp { relkey } => assert_eq!(library_key(&relkey), k),
            other => panic!("{k:?} classified as {other:?}, want Xmp"),
        }
    }
}
