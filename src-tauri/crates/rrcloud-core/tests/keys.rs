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
fn thumbpack_key_is_blake3_prefix16_of_folder_relkey() {
    let folder = rk("2026/10");
    let expected_hex = blake3::hash(folder.as_str().as_bytes()).to_hex();
    let expected16 = &expected_hex.as_str()[..16];
    assert_eq!(
        thumbpack_key(&folder),
        format!(".rrcloud/v1/thumbpacks/{expected16}.tar")
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
                KeyClass::Journal { device, seq: s } => {
                    assert_eq!(device, d);
                    assert_eq!(s, seq);
                }
                other => panic!("journal key ({ds}, {seq}) classified as {other:?}"),
            }
        }
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
        KeyClass::Thumbpack { hash16 } => {
            let expected = blake3::hash(folder.as_str().as_bytes());
            assert_eq!(hash16, expected.to_hex().as_str()[..16]);
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
fn classify_normalizes_relkey_text() {
    // A bucket key written with decomposed Unicode classifies to the same
    // RelKey as the composed spelling (same NFC normalization as RelKey::new).
    let composed = rk("K\u{e4}ch.jpg");
    let decomposed_key = "library/Ka\u{308}ch.jpg";
    match classify_key(decomposed_key) {
        KeyClass::Original { relkey } => assert_eq!(relkey, composed),
        other => panic!("decomposed library key classified as {other:?}"),
    }
}
