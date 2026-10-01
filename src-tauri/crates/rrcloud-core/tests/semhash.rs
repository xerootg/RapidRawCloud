//! Failing tests for `rrcloud_core::semhash` (architecture §2.5).
//!
//! Pins the invariants that kill sidecar rewrite-churn: formatting and key
//! order never matter, `exif`/`version` never matter, `lutPath` never
//! matters, `null` adjustments equal absent adjustments — while every real
//! user-visible change (rating, tags, adjustment values, AI patch blobs)
//! changes the hash. Invalid JSON is an error, never a default.

use rrcloud_core::semhash::{canonical_json, sem_hash, Blake3Hex, ContentId, SemHash};
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

#[test]
fn canonical_json_normalizes_number_spellings() {
    // Review finding (round 1, major): JS writers (JSON.stringify / Tauri
    // IPC) spell integral numbers "100" (parsed as u64) while Rust f64
    // writers spell them "100.0"; the canonical form must collapse every
    // spelling of one value to one string.
    let v: Value = serde_json::from_str(r#"{"a":1.0,"b":-0.0,"c":1e2,"d":1,"e":-5.0}"#).unwrap();
    assert_eq!(canonical_json(&v), r#"{"a":1,"b":0,"c":100,"d":1,"e":-5}"#);
    // Non-integral doubles keep serde_json's shortest-round-trip spelling,
    // which is a pure function of the f64 value.
    let f: Value = serde_json::from_str(r#"{"x":0.25,"y":2.5e-1}"#).unwrap();
    assert_eq!(canonical_json(&f), r#"{"x":0.25,"y":0.25}"#);
}

#[test]
fn canonical_json_pins_the_integral_range_boundaries() {
    // Review finding (round 2, minor): the delicate boundary region of
    // write_canonical_number was untested — exactly where a misleading
    // bounds comment invited a regression. Pinned:
    //
    // i64::MIN (-2^63) IS representable as i64 and is deliberately
    // INCLUDED in the integer-spelling range: its f64 spelling and its
    // integer-literal spelling collapse to one canonical form.
    let v: Value =
        serde_json::from_str(r#"{"a":-9223372036854775808,"b":-9.223372036854776e18}"#).unwrap();
    assert_eq!(
        canonical_json(&v),
        r#"{"a":-9223372036854775808,"b":-9223372036854775808}"#
    );
    // u64::MAX is not f64-representable and keeps its exact integer
    // spelling.
    let v: Value = serde_json::from_str(r#"{"m":18446744073709551615}"#).unwrap();
    assert_eq!(canonical_json(&v), r#"{"m":18446744073709551615}"#);
    // 2^64 is the EXCLUSIVE upper bound (not representable as u64): both
    // parse paths yield the same f64 and stay in ryu spelling.
    let a: Value = serde_json::from_str(r#"{"x":18446744073709551616}"#).unwrap();
    let b: Value = serde_json::from_str(r#"{"x":1.8446744073709552e19}"#).unwrap();
    assert_eq!(canonical_json(&a), canonical_json(&b));
    assert_eq!(canonical_json(&a), r#"{"x":1.8446744073709552e+19}"#);
    // Just below i64::MIN: integral f64 with no i64 alias stays in ryu
    // spelling.
    let v: Value = serde_json::from_str(r#"{"y":-9.223372036854778e18}"#).unwrap();
    assert_eq!(canonical_json(&v), r#"{"y":-9.223372036854778e+18}"#);
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
fn number_respelling_does_not_change_hash() {
    // Review finding (round 1, major): a no-edit load/save round trip
    // through the other runtime (JS integral spelling vs Rust f64 spelling)
    // must not change sem_hash — otherwise it produces exactly the spurious
    // dirties, spurious vv bumps, and junk conflict virtual-copies §2.5
    // exists to kill, and defeats the §2.6 (key, sem_hash) loser dedup.
    let equal_pairs = [
        (
            r#"{"adjustments":{"exposure":1}}"#,
            r#"{"adjustments":{"exposure":1.0}}"#,
        ),
        (
            r#"{"adjustments":{"exposure":1}}"#,
            r#"{"adjustments":{"exposure":1e0}}"#,
        ),
        (
            r#"{"adjustments":{"exposure":0}}"#,
            r#"{"adjustments":{"exposure":-0.0}}"#,
        ),
        (
            r#"{"adjustments":{"exposure":100}}"#,
            r#"{"adjustments":{"exposure":1e2}}"#,
        ),
        (
            r#"{"adjustments":{"exposure":-100}}"#,
            r#"{"adjustments":{"exposure":-1.0e2}}"#,
        ),
        (
            r#"{"adjustments":{"exposure":0.25}}"#,
            r#"{"adjustments":{"exposure":2.5e-1}}"#,
        ),
        (r#"{"rating":3}"#, r#"{"rating":3.0}"#),
        // Integral f64 beyond 2^53 but inside u64 range still matches the
        // u64 spelling exactly (integral f64 are exact).
        (
            r#"{"adjustments":{"big":10000000000000000000}}"#,
            r#"{"adjustments":{"big":1e19}}"#,
        ),
        // Boundary pins (review finding, round 2): i64::MIN spelled as
        // f64 vs integer literal, and 2^64 on both parse paths.
        (
            r#"{"adjustments":{"x":-9223372036854775808}}"#,
            r#"{"adjustments":{"x":-9.223372036854776e18}}"#,
        ),
        (
            r#"{"adjustments":{"x":18446744073709551616}}"#,
            r#"{"adjustments":{"x":1.8446744073709552e19}}"#,
        ),
    ];
    for (a, b) in equal_pairs {
        assert_eq!(
            hash_bytes(a.as_bytes()),
            hash_bytes(b.as_bytes()),
            "{a} and {b} must hash identically"
        );
    }
    // But distinct values stay distinct.
    let distinct_pairs = [
        (
            r#"{"adjustments":{"exposure":1}}"#,
            r#"{"adjustments":{"exposure":2}}"#,
        ),
        (
            r#"{"adjustments":{"exposure":0}}"#,
            r#"{"adjustments":{"exposure":0.5}}"#,
        ),
        (
            r#"{"adjustments":{"exposure":1}}"#,
            r#"{"adjustments":{"exposure":-1}}"#,
        ),
        // u64::MAX and 2^64 are distinct values (u64::MAX is not
        // f64-representable; it must keep its integer spelling, never be
        // rounded into the 2^64 ryu spelling).
        (
            r#"{"adjustments":{"x":18446744073709551615}}"#,
            r#"{"adjustments":{"x":1.8446744073709552e19}}"#,
        ),
    ];
    for (a, b) in distinct_pairs {
        assert_ne!(
            hash_bytes(a.as_bytes()),
            hash_bytes(b.as_bytes()),
            "{a} and {b} must hash differently"
        );
    }
}

#[test]
fn nested_residue_collapses_recursively() {
    // Review finding (round 1): the equivalence closure must hold at every
    // depth. Upstream's frontend default adjustments spell out sections
    // (masks: [], aiPatches: []) while Rust writers use json!({}) — "no
    // edits" must hash identically across writers, and a stripped null must
    // not leave an {}-residue behind.
    let h0 = hash_bytes(br#"{"rating":3}"#);
    for spelled in [
        r#"{"rating":3,"adjustments":{"foo":{"bar":null}}}"#,
        r#"{"rating":3,"adjustments":{"masks":[],"aiPatches":[]}}"#,
        r#"{"rating":3,"adjustments":{"a":{"b":{"c":null}},"d":[],"e":{}}}"#,
        r#"{"rating":3,"adjustments":{"foo":{"lutPath":null}}}"#,
    ] {
        assert_eq!(
            h0,
            hash_bytes(spelled.as_bytes()),
            "{spelled} must hash like absent adjustments"
        );
    }
    // The closure also holds inside a non-empty adjustments.
    assert_eq!(
        hash_bytes(br#"{"adjustments":{"exposure":1,"masks":[]}}"#),
        hash_bytes(br#"{"adjustments":{"exposure":1}}"#),
    );
    // Non-empty nested content is kept...
    assert_ne!(
        h0,
        hash_bytes(br#"{"rating":3,"adjustments":{"masks":[{"id":1}]}}"#)
    );
    // ...and empty array/object *elements* are positional content.
    assert_ne!(
        hash_bytes(br#"{"adjustments":{"masks":[{},{"id":1}]}}"#),
        hash_bytes(br#"{"adjustments":{"masks":[{"id":1}]}}"#),
    );
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
fn blake3_hex_parse_validates() {
    // Review finding (round 1): the journal's blake3 field is typed like
    // its sibling hashes, so a malformed digest fails at decode.
    let good = "4878ca0425c739fa427f7eda20fe845f6b2e46ba5fe2a14df5b1e32f50603215";
    assert_eq!(Blake3Hex::parse(good).expect("valid").as_str(), good);
    assert_eq!(
        Blake3Hex::from_bytes(b"hello world").as_str(),
        blake3::hash(b"hello world").to_hex().as_str()
    );
    for bad in [
        "",
        "4878CA0425C739FA427F7EDA20FE845F6B2E46BA5FE2A14DF5B1E32F50603215",
        "4878ca",
        "z878ca0425c739fa427f7eda20fe845f6b2e46ba5fe2a14df5b1e32f50603215",
    ] {
        assert!(Blake3Hex::parse(bad).is_err(), "{bad:?} must be rejected");
    }
}

#[test]
fn sem_hash_parse_validates() {
    let good = "af1349b9f5f9a1a6a0404dee36dcc9499bcb25c9adc112b7cc9a93cae41f3262";
    assert_eq!(SemHash::parse(good).expect("valid").as_str(), good);
    assert!(SemHash::parse("nope").is_err());
}

#[test]
fn streaming_hash_constructors_match_the_buffered_forms() {
    // Additive P1-U4 extensions: the transfer engine's running blake3 is
    // finalized from a streaming hasher (no contiguous buffer ever
    // exists), and an original's content id is by definition the same
    // digest relabeled (§1.2). Both constructions must equal what the
    // buffered entry points produce for the same bytes.
    let bytes = b"streamed in several chunks";
    let mut hasher = blake3::Hasher::new();
    hasher.update(&bytes[..9]);
    hasher.update(&bytes[9..]);
    let streamed = Blake3Hex::from_hash(&hasher.finalize());
    assert_eq!(streamed, Blake3Hex::from_bytes(bytes));
    assert_eq!(
        ContentId::from_blake3(&streamed),
        ContentId::from_bytes(bytes),
        "a content id IS the full-file blake3 (§1.2)"
    );
}
