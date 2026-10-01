//! Failing tests for `rrcloud_core::semhash` (architecture §2.5).
//!
//! Pins the invariants that kill sidecar rewrite-churn: formatting and key
//! order never matter, `exif`/`version` never matter, `lutPath` never
//! matters, `null` adjustments equal absent adjustments — while every real
//! user-visible change (rating, tags, adjustment values, AI patch blobs)
//! changes the hash. Invalid JSON is an error, never a default.

use rrcloud_core::semhash::{canonical_json, sem_hash, ContentId, SemHash};
use serde_json::{json, Value};

const FULL: &str = include_str!("fixtures/sidecar_full.json");
const NULL_ADJ: &str = include_str!("fixtures/sidecar_null_adjustments.json");
const AIPATCHES: &str = include_str!("fixtures/sidecar_aipatches.json");

fn parse(s: &str) -> Value {
    serde_json::from_str(s).expect("fixture parses")
}

fn hash_bytes(b: &[u8]) -> SemHash {
    sem_hash(b).expect("sem_hash of valid sidecar")
}

fn hash_value(v: &Value) -> SemHash {
    hash_bytes(serde_json::to_string(v).expect("serialize").as_bytes())
}

fn is_lower_hex(s: &str) -> bool {
    s.chars()
        .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
}

// ---------------------------------------------------------------------------
// canonical_json
// ---------------------------------------------------------------------------

#[test]
fn canonical_json_sorts_keys_recursively_without_whitespace() {
    let v: Value =
        serde_json::from_str(r#"{"b":{"d":2,"c":1},"a":[3,{"z":1,"y":2}],"s":"x"}"#).unwrap();
    assert_eq!(
        canonical_json(&v),
        r#"{"a":[3,{"y":2,"z":1}],"b":{"c":1,"d":2},"s":"x"}"#
    );
}

#[test]
fn canonical_json_is_formatting_independent() {
    let compact: Value = serde_json::from_str(r#"{"b":1,"a":[true,null]}"#).unwrap();
    let spaced: Value =
        serde_json::from_str("{\n  \"a\" : [ true , null ],\n  \"b\" : 1\n}").unwrap();
    assert_eq!(canonical_json(&compact), canonical_json(&spaced));
}

#[test]
fn canonical_json_preserves_array_order() {
    let v: Value = serde_json::from_str(r#"{"t":["b","a"]}"#).unwrap();
    assert_eq!(canonical_json(&v), r#"{"t":["b","a"]}"#);
}

// ---------------------------------------------------------------------------
// sem_hash shape
// ---------------------------------------------------------------------------

#[test]
fn sem_hash_is_64_lowercase_hex() {
    let h = hash_bytes(FULL.as_bytes());
    assert_eq!(h.as_str().len(), 64);
    assert!(is_lower_hex(h.as_str()), "got {:?}", h.as_str());
}

#[test]
fn sem_hash_is_deterministic() {
    assert_eq!(hash_bytes(FULL.as_bytes()), hash_bytes(FULL.as_bytes()));
}

#[test]
fn fixtures_hash_distinctly() {
    let a = hash_bytes(FULL.as_bytes());
    let b = hash_bytes(NULL_ADJ.as_bytes());
    let c = hash_bytes(AIPATCHES.as_bytes());
    assert_ne!(a, b);
    assert_ne!(a, c);
    assert_ne!(b, c);
}

// ---------------------------------------------------------------------------
// Invariance: formatting, key order, exif, version, lutPath
// ---------------------------------------------------------------------------

#[test]
fn reformatting_same_document_same_hash() {
    for fixture in [FULL, NULL_ADJ, AIPATCHES] {
        let v = parse(fixture);
        let pretty = serde_json::to_string_pretty(&v).unwrap();
        let compact = serde_json::to_string(&v).unwrap();
        let h0 = hash_bytes(fixture.as_bytes());
        assert_eq!(h0, hash_bytes(pretty.as_bytes()), "pretty reprint");
        assert_eq!(h0, hash_bytes(compact.as_bytes()), "compact reprint");
    }
}

#[test]
fn key_order_does_not_matter() {
    let a = r#"{"rating":1,"tags":["a","b"],"adjustments":null,"version":1}"#;
    let b = r#" { "version" : 1 , "adjustments" : null , "tags" : [ "a", "b" ], "rating" : 1 } "#;
    assert_eq!(hash_bytes(a.as_bytes()), hash_bytes(b.as_bytes()));
}

#[test]
fn exif_added_removed_or_changed_same_hash() {
    let base = parse(FULL);
    let h0 = hash_value(&base);

    let mut no_exif = base.clone();
    no_exif.as_object_mut().unwrap().remove("exif");
    assert_eq!(h0, hash_value(&no_exif), "exif removed");

    let mut changed_exif = base.clone();
    changed_exif["exif"]["Make"] = json!("CANON");
    changed_exif["exif"]["NewField"] = json!("surprise");
    assert_eq!(h0, hash_value(&changed_exif), "exif changed");
}

#[test]
fn version_field_is_excluded() {
    let base = parse(FULL);
    let h0 = hash_value(&base);
    let mut v7 = base.clone();
    v7["version"] = json!(7);
    assert_eq!(h0, hash_value(&v7));
    let mut no_version = base;
    no_version.as_object_mut().unwrap().remove("version");
    assert_eq!(h0, hash_value(&no_version));
}

#[test]
fn lut_path_changed_or_removed_same_hash() {
    let base = parse(FULL);
    let h0 = hash_value(&base);

    let mut moved = base.clone();
    moved["adjustments"]["lutPath"] = json!("/mnt/other/machine/path.cube");
    assert_eq!(h0, hash_value(&moved), "lutPath changed");

    let mut removed = base.clone();
    removed["adjustments"]
        .as_object_mut()
        .unwrap()
        .remove("lutPath");
    assert_eq!(h0, hash_value(&removed), "lutPath removed");

    // But the other LUT fields are real content: intensity is user intent.
    let mut intensity = base;
    intensity["adjustments"]["lutIntensity"] = json!(10);
    assert_ne!(h0, hash_value(&intensity), "lutIntensity is content");
}

// ---------------------------------------------------------------------------
// Null normalization
// ---------------------------------------------------------------------------

#[test]
fn null_adjustments_equals_absent_adjustments() {
    let with_null = parse(NULL_ADJ);
    let mut absent = with_null.clone();
    absent.as_object_mut().unwrap().remove("adjustments");
    assert_eq!(hash_value(&with_null), hash_value(&absent));
}

#[test]
fn null_tags_equals_absent_tags() {
    let with_null = parse(NULL_ADJ);
    let mut absent = with_null.clone();
    absent.as_object_mut().unwrap().remove("tags");
    assert_eq!(hash_value(&with_null), hash_value(&absent));
}

#[test]
fn null_member_inside_adjustments_equals_absent_member() {
    // sidecar_aipatches.json carries "crop": null.
    let with_null = parse(AIPATCHES);
    assert!(
        with_null["adjustments"]["crop"].is_null(),
        "fixture premise"
    );
    let mut absent = with_null.clone();
    absent["adjustments"]
        .as_object_mut()
        .unwrap()
        .remove("crop");
    assert_eq!(hash_value(&with_null), hash_value(&absent));
}

#[test]
fn semantically_empty_adjustments_equals_absent_adjustments() {
    // Review finding (round 0): the §2.5 equivalence classes must be
    // closed. A writer that emits `adjustments:{}`, or whose only
    // adjustment member is machine-local `lutPath` or a null, is making no
    // user-visible statement — hashing it differently from an absent
    // `adjustments` produces spurious dirties, spurious version bumps, and
    // junk conflict virtual-copies: exactly the churn class §2.5 exists to
    // kill.
    let h0 = hash_bytes(br#"{"rating":3}"#);
    for spelled in [
        r#"{"rating":3,"adjustments":null}"#,
        r#"{"rating":3,"adjustments":{}}"#,
        r#"{"rating":3,"adjustments":{"foo":null}}"#,
        r#"{"rating":3,"adjustments":{"lutPath":"/x.cube"}}"#,
        r#"{"rating":3,"adjustments":{"lutPath":"/x.cube","foo":null}}"#,
    ] {
        assert_eq!(
            h0,
            hash_bytes(spelled.as_bytes()),
            "{spelled} must hash like absent adjustments"
        );
    }
}

#[test]
fn empty_tags_equals_absent_tags() {
    // Same closure requirement for tags: `[]`, `null`, and absent are one
    // equivalence class.
    let h0 = hash_bytes(br#"{"rating":3}"#);
    assert_eq!(h0, hash_bytes(br#"{"rating":3,"tags":[]}"#));
    assert_eq!(h0, hash_bytes(br#"{"rating":3,"tags":null}"#));
}

// ---------------------------------------------------------------------------
// Real changes must change the hash
// ---------------------------------------------------------------------------

#[test]
fn rating_change_changes_hash() {
    let base = parse(FULL);
    let h0 = hash_value(&base);
    let mut r4 = base;
    r4["rating"] = json!(4);
    assert_ne!(h0, hash_value(&r4));
}

#[test]
fn tags_are_order_insensitive_but_content_sensitive() {
    let base = parse(FULL);
    let h0 = hash_value(&base);

    let mut reordered = base.clone();
    reordered["tags"] = json!(["keeper", "travel", "alps"]);
    assert_eq!(h0, hash_value(&reordered), "reordered tags");

    let mut different = base;
    different["tags"] = json!(["travel", "alps"]);
    assert_ne!(h0, hash_value(&different), "removed tag");
}

#[test]
fn adjustment_value_change_changes_hash() {
    let base = parse(FULL);
    let h0 = hash_value(&base);
    let mut tweaked = base;
    tweaked["adjustments"]["exposure"] = json!(0.36);
    assert_ne!(h0, hash_value(&tweaked));
}

#[test]
fn multi_mb_ai_patch_blob_is_content() {
    // A multi-megabyte base64 blob inside adjustments is user content: two
    // documents differing only in the blob must hash differently.
    let base = parse(AIPATCHES);
    let blob_a: String = "iVBORw0KGgoAAAANSUhEUg".repeat(150_000); // ~3.3 MB
    let mut blob_b = blob_a.clone();
    blob_b.push_str("deadbeef");

    let mut doc_a = base.clone();
    doc_a["adjustments"]["aiPatches"][0]["patchData"]["dataBase64"] = json!(blob_a);
    let mut doc_b = base.clone();
    doc_b["adjustments"]["aiPatches"][0]["patchData"]["dataBase64"] = json!(blob_b);

    let ha = hash_value(&doc_a);
    let hb = hash_value(&doc_b);
    assert_ne!(ha, hb, "blob content must affect the hash");
    assert_ne!(
        ha,
        hash_value(&base),
        "adding the blob must affect the hash"
    );
}

// ---------------------------------------------------------------------------
// Failure modes: never a silent default
// ---------------------------------------------------------------------------

#[test]
fn invalid_json_is_an_error() {
    for bad in [
        &b"not json at all"[..],
        &b"{\"rating\": 3,"[..],
        &b""[..],
        &b"\xff\xfe"[..],
    ] {
        assert!(sem_hash(bad).is_err(), "{bad:?} must be an error");
    }
}

#[test]
fn non_object_root_is_an_error() {
    for bad in ["[1,2,3]", "\"a string\"", "42", "null", "true"] {
        assert!(
            sem_hash(bad.as_bytes()).is_err(),
            "{bad:?} must be an error"
        );
    }
}

// ---------------------------------------------------------------------------
// ContentId
// ---------------------------------------------------------------------------

#[test]
fn content_id_is_full_blake3_lowercase_hex() {
    // Known blake3 vector: empty input (official BLAKE3 test_vectors.json,
    // input_len 0). The red commit had a one-character typo here
    // ("…0404dee36…" for "…0404dea36…"), caught against the upstream
    // vectors and the crate's output during the green stage.
    assert_eq!(
        ContentId::from_bytes(b"").as_str(),
        "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"
    );
    let c = ContentId::from_bytes(b"hello world");
    assert_eq!(c.as_str(), blake3::hash(b"hello world").to_hex().as_str());
    assert_eq!(c.as_str().len(), 64);
    assert!(is_lower_hex(c.as_str()));
    assert_ne!(c, ContentId::from_bytes(b"hello worle"));
}

#[test]
fn content_id_parse_validates() {
    let good = "af1349b9f5f9a1a6a0404dee36dcc9499bcb25c9adc112b7cc9a93cae41f3262";
    assert_eq!(ContentId::parse(good).expect("valid").as_str(), good);
    for bad in [
        "",
        "af1349",
        // uppercase
        "AF1349B9F5F9A1A6A0404DEE36DCC9499BCB25C9ADC112B7CC9A93CAE41F3262",
        // 63 chars
        "af1349b9f5f9a1a6a0404dee36dcc9499bcb25c9adc112b7cc9a93cae41f326",
        // non-hex
        "zf1349b9f5f9a1a6a0404dee36dcc9499bcb25c9adc112b7cc9a93cae41f3262",
    ] {
        assert!(ContentId::parse(bad).is_err(), "{bad:?} must be rejected");
    }
}

#[test]
fn sem_hash_parse_validates() {
    let good = "af1349b9f5f9a1a6a0404dee36dcc9499bcb25c9adc112b7cc9a93cae41f3262";
    assert_eq!(SemHash::parse(good).expect("valid").as_str(), good);
    assert!(SemHash::parse("nope").is_err());
}
