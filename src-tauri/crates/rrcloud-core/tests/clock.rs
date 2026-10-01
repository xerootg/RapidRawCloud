//! Failing tests for `rrcloud_core::clock` (architecture §2.6).
//!
//! Pins version-vector ordering (reflexive, antisymmetric, concurrency
//! detection), elementwise-max merge, monotonic bump, device-id validation,
//! and the totally deterministic concurrent-winner rule.

use rrcloud_core::clock::{compare, pick_winner, Candidate, DeviceId, VersionVector, VvOrder};

const DEV1: &str = "d1f0c2aa-9d2b-4a6e-8f1c-3b7d5e9a0c42";
const DEV2: &str = "a3b2e1d0-5c4f-4b3a-9e2d-1f0a9b8c7d6e";
const DEV3: &str = "0a1b2c3d-4e5f-4a6b-8c9d-0e1f2a3b4c5d";

fn dev(s: &str) -> DeviceId {
    DeviceId::new(s).expect("valid device id")
}

fn vv(pairs: &[(&str, u32)]) -> VersionVector {
    pairs.iter().map(|&(d, c)| (dev(d), c)).collect()
}

// ---------------------------------------------------------------------------
// DeviceId
// ---------------------------------------------------------------------------

#[test]
fn device_id_accepts_canonical_lowercase_uuidv4() {
    for s in [DEV1, DEV2, DEV3] {
        let d = DeviceId::new(s).unwrap_or_else(|e| panic!("{s:?} rejected: {e}"));
        assert_eq!(d.as_str(), s);
        assert_eq!(d.to_string(), s);
    }
}

#[test]
fn device_id_rejects_non_canonical_forms() {
    let bad = [
        "",
        "not-a-uuid",
        // uppercase
        "D1F0C2AA-9D2B-4A6E-8F1C-3B7D5E9A0C42",
        // missing dashes
        "d1f0c2aa9d2b4a6e8f1c3b7d5e9a0c42",
        // version nibble is 1, not 4
        "d1f0c2aa-9d2b-1a6e-8f1c-3b7d5e9a0c42",
        // too short / too long
        "d1f0c2aa-9d2b-4a6e-8f1c-3b7d5e9a0c4",
        "d1f0c2aa-9d2b-4a6e-8f1c-3b7d5e9a0c421",
        // non-hex character
        "g1f0c2aa-9d2b-4a6e-8f1c-3b7d5e9a0c42",
        // dash in the wrong place
        "d1f0c2a-a9d2b-4a6e-8f1c-3b7d5e9a0c42",
        // surrounding whitespace
        " d1f0c2aa-9d2b-4a6e-8f1c-3b7d5e9a0c42",
    ];
    for s in bad {
        assert!(DeviceId::new(s).is_err(), "{s:?} must be rejected");
    }
}

#[test]
fn device_id_serde_round_trip_validates() {
    let d = dev(DEV1);
    let json = serde_json::to_string(&d).expect("serialize");
    assert_eq!(json, format!("\"{DEV1}\""));
    let back: DeviceId = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(back, d);
    assert!(serde_json::from_str::<DeviceId>("\"nope\"").is_err());
}

// ---------------------------------------------------------------------------
// compare(): ordering semantics
// ---------------------------------------------------------------------------

#[test]
fn compare_is_reflexive() {
    let cases = [
        VersionVector::new(),
        vv(&[(DEV1, 1)]),
        vv(&[(DEV1, 2), (DEV2, 4)]),
        vv(&[(DEV1, 2), (DEV2, 4), (DEV3, 1)]),
    ];
    for a in &cases {
        assert_eq!(compare(a, a), VvOrder::Equal, "compare({a:?}, itself)");
        assert_eq!(compare(a, &a.clone()), VvOrder::Equal);
    }
}

#[test]
fn compare_detects_dominance() {
    let small = vv(&[(DEV1, 2), (DEV2, 3)]);
    let big = vv(&[(DEV1, 2), (DEV2, 4)]);
    assert_eq!(compare(&big, &small), VvOrder::Greater);
    assert_eq!(compare(&small, &big), VvOrder::Less);
}

#[test]
fn missing_components_read_as_zero() {
    let empty = VersionVector::new();
    let one = vv(&[(DEV1, 1)]);
    assert_eq!(compare(&one, &empty), VvOrder::Greater);
    assert_eq!(compare(&empty, &one), VvOrder::Less);
    assert_eq!(compare(&empty, &empty), VvOrder::Equal);
    // Explicit zero equals absent.
    let explicit_zero = vv(&[(DEV1, 1), (DEV2, 0)]);
    assert_eq!(compare(&one, &explicit_zero), VvOrder::Equal);
}

#[test]
fn compare_detects_concurrency() {
    // The spec's canonical example: {a:2} vs {b:1}.
    let a = vv(&[(DEV1, 2)]);
    let b = vv(&[(DEV2, 1)]);
    assert_eq!(compare(&a, &b), VvOrder::Concurrent);
    assert_eq!(compare(&b, &a), VvOrder::Concurrent);

    let c = vv(&[(DEV1, 2), (DEV2, 1)]);
    let d = vv(&[(DEV1, 1), (DEV2, 2)]);
    assert_eq!(compare(&c, &d), VvOrder::Concurrent);
}

#[test]
fn compare_is_antisymmetric_over_a_generated_set() {
    let mut cases = vec![VersionVector::new()];
    for x in 0..3u32 {
        for y in 0..3u32 {
            for z in 0..2u32 {
                cases.push(vv(&[(DEV1, x), (DEV2, y), (DEV3, z)]));
            }
        }
    }
    for a in &cases {
        for b in &cases {
            let fwd = compare(a, b);
            let rev = compare(b, a);
            let expected_rev = match fwd {
                VvOrder::Equal => VvOrder::Equal,
                VvOrder::Greater => VvOrder::Less,
                VvOrder::Less => VvOrder::Greater,
                VvOrder::Concurrent => VvOrder::Concurrent,
            };
            assert_eq!(rev, expected_rev, "a={a:?} b={b:?}");
            if fwd == VvOrder::Equal {
                assert_eq!(a, b, "Equal must mean identical effective vectors");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// merge(): elementwise max
// ---------------------------------------------------------------------------

#[test]
fn merge_is_elementwise_max() {
    let mut a = vv(&[(DEV1, 2), (DEV2, 1)]);
    let b = vv(&[(DEV1, 1), (DEV2, 3), (DEV3, 4)]);
    a.merge(&b);
    assert_eq!(a.get(&dev(DEV1)), 2);
    assert_eq!(a.get(&dev(DEV2)), 3);
    assert_eq!(a.get(&dev(DEV3)), 4);
}

#[test]
fn merge_result_dominates_both_inputs() {
    let a = vv(&[(DEV1, 5)]);
    let b = vv(&[(DEV2, 3)]);
    let mut m = a.clone();
    m.merge(&b);
    assert!(matches!(compare(&m, &a), VvOrder::Greater | VvOrder::Equal));
    assert!(matches!(compare(&m, &b), VvOrder::Greater | VvOrder::Equal));
    // Merging concurrent branches closes the conflict: the next bump
    // dominates both.
    let mut next = m.clone();
    next.bump(&dev(DEV1));
    assert_eq!(compare(&next, &a), VvOrder::Greater);
    assert_eq!(compare(&next, &b), VvOrder::Greater);
}

#[test]
fn merge_is_commutative_and_idempotent() {
    let a = vv(&[(DEV1, 2), (DEV3, 7)]);
    let b = vv(&[(DEV2, 4), (DEV3, 5)]);
    let mut ab = a.clone();
    ab.merge(&b);
    let mut ba = b.clone();
    ba.merge(&a);
    assert_eq!(ab, ba);
    let mut again = ab.clone();
    again.merge(&b);
    assert_eq!(again, ab);
}

// ---------------------------------------------------------------------------
// bump(): monotonic
// ---------------------------------------------------------------------------

#[test]
fn bump_is_monotonic() {
    let d1 = dev(DEV1);
    let mut v = VersionVector::new();
    assert_eq!(v.get(&d1), 0);
    let mut prev = v.clone();
    for expected in 1..=5u32 {
        v.bump(&d1);
        assert_eq!(v.get(&d1), expected);
        assert_eq!(compare(&v, &prev), VvOrder::Greater, "bump #{expected}");
        prev = v.clone();
    }
    // Bumping one device never touches another's component.
    assert_eq!(v.get(&dev(DEV2)), 0);
}

// ---------------------------------------------------------------------------
// VersionVector serde
// ---------------------------------------------------------------------------

#[test]
fn version_vector_serializes_as_plain_map() {
    let v = vv(&[(DEV1, 9), (DEV2, 4)]);
    let json = serde_json::to_string(&v).expect("serialize");
    let back: VersionVector = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(back, v);
    // Matches the journal `vv` wire shape exactly.
    let wire = format!("{{\"{DEV2}\":4,\"{DEV1}\":9}}");
    let from_wire: VersionVector = serde_json::from_str(&wire).expect("wire shape");
    assert_eq!(from_wire, v);
}

// ---------------------------------------------------------------------------
// pick_winner(): total determinism
// ---------------------------------------------------------------------------

#[test]
fn higher_ts_wins() {
    let d1 = dev(DEV1);
    let d2 = dev(DEV2);
    let newer = Candidate {
        ts: 200,
        device: &d2,
    };
    let older = Candidate {
        ts: 100,
        device: &d1,
    };
    assert_eq!(pick_winner(newer, older), newer);
    assert_eq!(pick_winner(older, newer), newer);
}

#[test]
fn ts_tie_breaks_on_lexicographically_greater_device() {
    let d1 = dev(DEV1); // "d1f0…"
    let d2 = dev(DEV2); // "a3b2…" — lexicographically smaller
    assert!(DEV1 > DEV2, "test premise");
    let a = Candidate {
        ts: 100,
        device: &d1,
    };
    let b = Candidate {
        ts: 100,
        device: &d2,
    };
    assert_eq!(pick_winner(a, b), a);
    assert_eq!(pick_winner(b, a), a);
}

#[test]
fn pick_winner_is_symmetric_and_deterministic_over_a_matrix() {
    let devs = [dev(DEV1), dev(DEV2), dev(DEV3)];
    let tss = [0i64, 1, 100, 1_769_900_000, i64::MAX];
    for da in &devs {
        for db in &devs {
            for &ta in &tss {
                for &tb in &tss {
                    let a = Candidate { ts: ta, device: da };
                    let b = Candidate { ts: tb, device: db };
                    let w1 = pick_winner(a, b);
                    let w2 = pick_winner(b, a);
                    assert_eq!(w1, w2, "symmetry for {a:?} vs {b:?}");
                    assert_eq!(w1, pick_winner(a, b), "determinism");
                    assert!(w1 == a || w1 == b, "winner must be one of the pair");
                    // The winner rule itself, restated independently.
                    if ta != tb {
                        let expect_a = ta > tb;
                        assert_eq!(w1 == a, expect_a || (a == b));
                    } else if da != db {
                        assert_eq!(w1.device, da.max(db));
                    }
                }
            }
        }
    }
}

#[test]
fn identical_candidates_pick_themselves() {
    let d1 = dev(DEV1);
    let a = Candidate {
        ts: 42,
        device: &d1,
    };
    assert_eq!(pick_winner(a, a), a);
}
