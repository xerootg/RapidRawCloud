//! §2.9 albums & presets meta documents: the pure relativization,
//! whole-document resolution, and loser-key contracts. No Garage, no I/O —
//! these pin the byte-level behavior the app glue and the two-device
//! Garage suite build on.

use std::path::Path;

use rrcloud_core::clock::{DeviceId, VersionVector};
use rrcloud_core::journal::Kind;
use rrcloud_core::meta::{
    canonical, decide_meta, localize, merge_out_of_root, relativize, MetaDecision, MetaHead,
    MetaKind, RR_SCHEME,
};
use rrcloud_core::semhash::Blake3Hex;

const DEV_A: &str = "d1f0c2aa-9d2b-4a6e-8f1c-3b7d5e9a0c42";
const DEV_B: &str = "a3b2e1d0-5c4f-4b3a-9e2d-1f0a9b8c7d6e";

fn dev(s: &str) -> DeviceId {
    DeviceId::new(s).expect("device id")
}

fn vv(pairs: &[(&str, u32)]) -> VersionVector {
    pairs.iter().map(|(d, n)| (dev(d), *n)).collect()
}

fn head(vv_pairs: &[(&str, u32)], ts: i64, device: &str, doc: &[u8]) -> MetaHead {
    MetaHead {
        vv: vv(vv_pairs),
        blake3: Blake3Hex::from_bytes(doc),
        ts,
        device: dev(device),
    }
}

/// An albums document with two in-root images, one out-of-root image, and a
/// nested group album — exactly the §2.9 round-trip fixture.
fn albums_doc() -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!([
        {
            "type": "album",
            "id": "a1",
            "name": "Trip",
            "icon": null,
            "images": [
                "/rootA/trip/a.jpg",
                "/rootA/trip/b.jpg",
                "/somewhere/else/c.jpg"
            ]
        },
        {
            "type": "group",
            "id": "g1",
            "name": "Grp",
            "icon": null,
            "children": [
                {
                    "type": "album",
                    "id": "a2",
                    "name": "Sub",
                    "icon": null,
                    "images": ["/rootA/x/y.jpg"]
                }
            ]
        }
    ]))
    .expect("albums doc encodes")
}

/// A presets document whose one preset carries an in-root `lutPath` and a
/// second whose `lutPath` points outside the sync root.
fn presets_doc() -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!([
        {
            "preset": {
                "id": "p1",
                "name": "Warm",
                "adjustments": { "exposure": 0.2, "lutPath": "/rootA/luts/warm.cube" }
            }
        },
        {
            "preset": {
                "id": "p2",
                "name": "ExternalLut",
                "adjustments": { "exposure": 0.0, "lutPath": "/outside/luts/x.cube" }
            }
        }
    ]))
    .expect("presets doc encodes")
}

/// Pulls every `images` array (recursively) out of a relativized/localized
/// albums document, flattened in document order.
fn all_album_images(doc: &[u8]) -> Vec<String> {
    fn walk(v: &serde_json::Value, out: &mut Vec<String>) {
        match v {
            serde_json::Value::Array(items) => items.iter().for_each(|i| walk(i, out)),
            serde_json::Value::Object(map) => {
                if let Some(serde_json::Value::Array(imgs)) = map.get("images") {
                    for i in imgs {
                        if let Some(s) = i.as_str() {
                            out.push(s.to_string());
                        }
                    }
                }
                if let Some(children) = map.get("children") {
                    walk(children, out);
                }
            }
            _ => {}
        }
    }
    let v: serde_json::Value = serde_json::from_slice(doc).expect("doc parses");
    let mut out = Vec::new();
    walk(&v, &mut out);
    out
}

/// Collects every `lutPath` string value anywhere in a presets document.
fn all_lut_paths(doc: &[u8]) -> Vec<String> {
    fn walk(v: &serde_json::Value, out: &mut Vec<String>) {
        match v {
            serde_json::Value::Array(items) => items.iter().for_each(|i| walk(i, out)),
            serde_json::Value::Object(map) => {
                if let Some(serde_json::Value::String(s)) = map.get("lutPath") {
                    out.push(s.clone());
                }
                for (_, val) in map {
                    walk(val, out);
                }
            }
            _ => {}
        }
    }
    let v: serde_json::Value = serde_json::from_slice(doc).expect("doc parses");
    let mut out = Vec::new();
    walk(&v, &mut out);
    out
}

// ---------------------------------------------------------------------------
// MetaKind schema (these are real helpers — they pin the key/kind schema)
// ---------------------------------------------------------------------------

#[test]
fn meta_kind_keys_and_journal_kinds() {
    assert_eq!(MetaKind::Albums.meta_key(), ".rrcloud/v1/meta/albums.json");
    assert_eq!(
        MetaKind::Presets.meta_key(),
        ".rrcloud/v1/meta/presets.json"
    );
    assert_eq!(MetaKind::Albums.journal_kind(), Kind::Albums);
    assert_eq!(MetaKind::Presets.journal_kind(), Kind::Presets);
}

#[test]
fn conflict_key_is_deterministic_and_shaped() {
    let loser = br#"{"canonical":"loser"}"#;
    let k1 = MetaKind::Albums.conflict_key(loser);
    let k2 = MetaKind::Albums.conflict_key(loser);
    assert_eq!(
        k1, k2,
        "same canonical bytes ⇒ same conflict key on every device"
    );
    assert!(
        k1.starts_with(".rrcloud/v1/meta/albums.conflict-") && k1.ends_with(".json"),
        "unexpected conflict key shape: {k1}"
    );
    // `albums.conflict-<6hex>.json`: six lowercase hex after the dash.
    let hex = k1
        .strip_prefix(".rrcloud/v1/meta/albums.conflict-")
        .and_then(|s| s.strip_suffix(".json"))
        .expect("suffix");
    assert_eq!(hex.len(), 6, "conflict suffix must be 6 hex, got {hex:?}");
    assert!(hex
        .bytes()
        .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()));
    // A different document must (overwhelmingly) key differently.
    assert_ne!(
        k1,
        MetaKind::Albums.conflict_key(br#"{"canonical":"other"}"#)
    );
    assert_ne!(
        k1,
        MetaKind::Presets.conflict_key(loser),
        "stem distinguishes kinds"
    );
}

// ---------------------------------------------------------------------------
// Relativization (these FAIL in red on the todo!() bodies)
// ---------------------------------------------------------------------------

#[test]
fn relativize_albums_rewrites_in_root_and_drops_out_of_root() {
    let out = relativize(MetaKind::Albums, &albums_doc(), Path::new("/rootA")).expect("relativize");
    let imgs = all_album_images(&out);
    assert_eq!(
        imgs,
        vec![
            format!("{RR_SCHEME}trip/a.jpg"),
            format!("{RR_SCHEME}trip/b.jpg"),
            format!("{RR_SCHEME}x/y.jpg"),
        ],
        "in-root images relativize to rr://; the out-of-root entry is dropped from the uploaded copy"
    );
    // Non-path fields survive untouched.
    let v: serde_json::Value = serde_json::from_slice(&out).expect("parse");
    assert_eq!(v[0]["name"], "Trip");
    assert_eq!(v[1]["children"][0]["id"], "a2");
}

#[test]
fn localize_albums_maps_rr_back_to_this_device_root() {
    let uploaded =
        relativize(MetaKind::Albums, &albums_doc(), Path::new("/rootA")).expect("relativize");
    let local =
        localize(MetaKind::Albums, &uploaded, Path::new("/dev2/library")).expect("localize");
    let imgs = all_album_images(&local);
    assert_eq!(
        imgs,
        vec![
            "/dev2/library/trip/a.jpg".to_string(),
            "/dev2/library/trip/b.jpg".to_string(),
            "/dev2/library/x/y.jpg".to_string(),
        ],
        "rr:// paths rebase onto the second device's sync root; the dropped out-of-root entry stays absent"
    );
    assert!(
        !local
            .windows(RR_SCHEME.len())
            .any(|w| w == RR_SCHEME.as_bytes()),
        "a localized document must contain no rr:// URIs"
    );
}

#[test]
fn merge_out_of_root_restores_local_only_album_membership_after_adopt() {
    // The device's own prior local file: album "a1" has one in-root image
    // and one out-of-root image the uploaded copy never carried.
    let old_local = serde_json::to_vec(&serde_json::json!([
        {
            "type": "album", "id": "a1", "name": "Trip", "icon": null,
            "images": ["/rootA/trip/a.jpg", "/somewhere/else/c.jpg"]
        }
    ]))
    .unwrap();
    // A peer's adopted-and-localized remote: same album, gained an extra
    // in-root image, but (correctly) has never seen the out-of-root one.
    let new_localized = serde_json::to_vec(&serde_json::json!([
        {
            "type": "album", "id": "a1", "name": "Trip", "icon": null,
            "images": ["/rootA/trip/a.jpg", "/rootA/trip/b.jpg"]
        }
    ]))
    .unwrap();
    let merged = merge_out_of_root(
        MetaKind::Albums,
        Some(&old_local),
        &new_localized,
        Path::new("/rootA"),
    )
    .expect("merge");
    assert_eq!(
        all_album_images(&merged),
        vec![
            "/rootA/trip/a.jpg".to_string(),
            "/rootA/trip/b.jpg".to_string(),
            "/somewhere/else/c.jpg".to_string(),
        ],
        "the remote's new in-root image is kept AND the device's own \
         out-of-root image survives the adopt"
    );

    // Idempotent: merging again (e.g. a second consecutive AdoptRemote)
    // does not duplicate the restored entry.
    let merged_again = merge_out_of_root(
        MetaKind::Albums,
        Some(&old_local),
        &merged,
        Path::new("/rootA"),
    )
    .expect("merge again");
    assert_eq!(all_album_images(&merged_again), all_album_images(&merged));
}

#[test]
fn merge_out_of_root_restores_local_only_preset_lutpath_after_adopt() {
    // The device's own prior local file: a preset's lutPath points outside
    // the sync root (never crossed the wire — relativize dropped it).
    let old_local = serde_json::to_vec(&serde_json::json!([
        {
            "preset": {
                "id": "p1",
                "name": "Warm",
                "adjustments": { "exposure": 0.2, "lutPath": "/outside/luts/warm.cube" }
            }
        }
    ]))
    .unwrap();
    // A peer's adopted-and-localized remote: same preset id, a descendant
    // edit that never touched lutPath (exposure changed) — but the
    // uploaded copy this device is adopting never carried the out-of-root
    // lutPath at all (it was dropped at THIS device's own earlier upload).
    let new_localized = serde_json::to_vec(&serde_json::json!([
        {
            "preset": {
                "id": "p1",
                "name": "Warm",
                "adjustments": { "exposure": 0.5 }
            }
        }
    ]))
    .unwrap();
    let merged = merge_out_of_root(
        MetaKind::Presets,
        Some(&old_local),
        &new_localized,
        Path::new("/rootA"),
    )
    .expect("merge");
    let v: serde_json::Value = serde_json::from_slice(&merged).expect("merged parses");
    assert_eq!(
        v[0]["preset"]["adjustments"]["lutPath"], "/outside/luts/warm.cube",
        "the device's own out-of-root lutPath must survive an unrelated remote adopt \
         (round-trip data loss otherwise — the exact hazard §2.9's merge-back exists \
         to prevent, here left unhandled for presets)"
    );
    assert_eq!(
        v[0]["preset"]["adjustments"]["exposure"], 0.5,
        "the peer's descendant edit is still applied"
    );

    // Idempotent: merging again (e.g. a second consecutive AdoptRemote)
    // does not change the result.
    let merged_again = merge_out_of_root(
        MetaKind::Presets,
        Some(&old_local),
        &merged,
        Path::new("/rootA"),
    )
    .expect("merge again");
    assert_eq!(merged_again, merged);
}

#[test]
fn merge_out_of_root_is_noop_with_no_prior_local_copy() {
    let new_localized = relativize(MetaKind::Albums, &albums_doc(), Path::new("/rootA"))
        .and_then(|rel| localize(MetaKind::Albums, &rel, Path::new("/dev2")))
        .expect("prep localized doc");
    let merged = merge_out_of_root(MetaKind::Albums, None, &new_localized, Path::new("/dev2"))
        .expect("merge with no prior local");
    assert_eq!(merged, new_localized);
}

#[test]
fn presets_lutpath_relativizes_in_root_and_drops_out_of_root() {
    let out =
        relativize(MetaKind::Presets, &presets_doc(), Path::new("/rootA")).expect("relativize");
    let luts = all_lut_paths(&out);
    assert_eq!(
        luts,
        vec![format!("{RR_SCHEME}luts/warm.cube")],
        "in-root lutPath relativizes; the out-of-root lutPath is dropped (LUTs are not synced v1)"
    );
    let back = localize(MetaKind::Presets, &out, Path::new("/dev2/lib")).expect("localize");
    assert_eq!(
        all_lut_paths(&back),
        vec!["/dev2/lib/luts/warm.cube".to_string()]
    );
}

// ---------------------------------------------------------------------------
// Canonicalization (FAILS in red on todo!())
// ---------------------------------------------------------------------------

#[test]
fn canonical_is_stable_across_key_order_and_whitespace() {
    let a = br#"{"b":1,"a":2}"#;
    let b = b"{\n  \"a\": 2,\n  \"b\": 1\n}\n";
    assert_eq!(
        canonical(a).expect("canonical a"),
        canonical(b).expect("canonical b"),
        "canonical form must ignore key order and insignificant whitespace"
    );
    // And it must therefore produce the same conflict key on either spelling.
    assert_eq!(
        MetaKind::Albums.conflict_key(&canonical(a).unwrap()),
        MetaKind::Albums.conflict_key(&canonical(b).unwrap()),
    );
}

// ---------------------------------------------------------------------------
// Whole-document §2.6 resolution (FAILS in red on todo!())
// ---------------------------------------------------------------------------

#[test]
fn decide_meta_converged_on_equal_vv() {
    let doc = albums_doc();
    let local = head(
        &[("d1f0c2aa-9d2b-4a6e-8f1c-3b7d5e9a0c42", 1)],
        10,
        DEV_A,
        &doc,
    );
    let remote = head(
        &[("d1f0c2aa-9d2b-4a6e-8f1c-3b7d5e9a0c42", 1)],
        10,
        DEV_A,
        &doc,
    );
    assert_eq!(decide_meta(&local, &remote), MetaDecision::Converged);
}

#[test]
fn decide_meta_adopts_dominating_remote_and_keeps_dominating_local() {
    // Distinct content on each side (the real-world shape of a dominating
    // version: a later edit actually changed something) — §2.6 case 1's
    // content-equality fast path must not mask the vv-order outcome this
    // test isolates.
    let lo = head(&[(DEV_A, 1)], 10, DEV_A, &albums_doc());
    let hi = head(&[(DEV_A, 2)], 20, DEV_A, &presets_doc());
    assert_eq!(decide_meta(&lo, &hi), MetaDecision::AdoptRemote);
    assert_eq!(decide_meta(&hi, &lo), MetaDecision::KeepLocal);
}

#[test]
fn decide_meta_concurrent_picks_deterministic_winner() {
    let doc_a = albums_doc();
    let doc_b = presets_doc();
    // Concurrent: neither vv dominates. Higher ts wins (§2.6).
    let local = head(&[(DEV_A, 1)], 100, DEV_A, &doc_a);
    let remote = head(&[(DEV_B, 1)], 200, DEV_B, &doc_b);
    assert_eq!(
        decide_meta(&local, &remote),
        MetaDecision::Conflict { remote_wins: true },
        "the higher-ts remote is the deterministic primary"
    );
    // Symmetric: swapping the sides flips `remote_wins` but agrees on who wins.
    assert_eq!(
        decide_meta(&remote, &local),
        MetaDecision::Conflict { remote_wins: false },
    );
}

#[test]
fn decide_meta_concurrent_vv_with_identical_content_converges() {
    // §2.6 case 1: "remote.vv == local.vv OR remote.sem_hash == local.sem_hash
    // → converged" applies regardless of vv order. Two devices that
    // independently produce the SAME bytes (e.g. both create the same
    // default/empty document on first run) must never be flagged as a
    // conflict just because their vvs happen to be concurrent — doing so
    // writes a spurious, permanent conflict-loser object for content that
    // never actually diverged.
    let doc = albums_doc();
    let local = head(&[(DEV_A, 1)], 100, DEV_A, &doc);
    let remote = head(&[(DEV_B, 1)], 200, DEV_B, &doc);
    assert_eq!(
        decide_meta(&local, &remote),
        MetaDecision::Converged,
        "identical content under a concurrent vv must converge, not conflict"
    );
    assert_eq!(
        decide_meta(&remote, &local),
        MetaDecision::Converged,
        "symmetric: the same holds with sides swapped"
    );
}
