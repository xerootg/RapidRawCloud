//! Device identity, per-key version vectors, and the deterministic
//! concurrent-edit winner rule (architecture §2.6).
//!
//! Causality in the sync protocol is tracked with **per-relkey version
//! vectors**, never wall-clock time. Wall-clock `ts` appears only in
//! [`pick_winner`], which chooses the *primary* of two provably concurrent
//! versions — the loser is always preserved (§2.6), so clock skew can pick a
//! surprising primary but can never destroy data.
//!
//! Version-vector ordering is deliberately exposed as [`compare`] returning
//! [`VvOrder`] instead of a `PartialOrd` impl: two vectors can be
//! *concurrent* (neither dominates), and hiding that fourth outcome behind
//! `PartialOrd::partial_cmp == None` invites subtle misuse in comparison
//! chains.

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

/// Error returned when a string is not a valid [`DeviceId`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DeviceIdError {
    /// The string is not a canonical lowercase hyphenated UUIDv4.
    #[error("not a canonical lowercase UUIDv4 device id: {0:?}")]
    Invalid(String),
}

/// A sync participant's identity: a UUIDv4 minted on first sync setup and
/// persisted in the device's sync state DB (§1.2).
///
/// Only the canonical string form is accepted: 36 characters, lowercase hex,
/// hyphens at positions 8/13/18/23, version nibble `4`. Device ids appear in
/// bucket keys and are compared lexicographically as the conflict tiebreak
/// (§2.6), so a single canonical spelling is load-bearing.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct DeviceId(String);

impl DeviceId {
    /// Validates `s` as a canonical lowercase UUIDv4 and wraps it.
    pub fn new(s: impl Into<String>) -> Result<Self, DeviceIdError> {
        let s = s.into();
        if is_canonical_uuidv4(&s) {
            Ok(DeviceId(s))
        } else {
            Err(DeviceIdError::Invalid(s))
        }
    }

    /// The canonical string form.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// `true` when `s` is a canonical lowercase hyphenated UUIDv4: 36 bytes,
/// hyphens at 8/13/18/23, lowercase hex elsewhere, version nibble `4`.
fn is_canonical_uuidv4(s: &str) -> bool {
    let b = s.as_bytes();
    if b.len() != 36 {
        return false;
    }
    for (i, &c) in b.iter().enumerate() {
        match i {
            8 | 13 | 18 | 23 => {
                if c != b'-' {
                    return false;
                }
            }
            14 => {
                if c != b'4' {
                    return false;
                }
            }
            _ => {
                if !(c.is_ascii_digit() || (b'a'..=b'f').contains(&c)) {
                    return false;
                }
            }
        }
    }
    true
}

impl TryFrom<String> for DeviceId {
    type Error = DeviceIdError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::new(s)
    }
}

impl From<DeviceId> for String {
    fn from(d: DeviceId) -> String {
        d.0
    }
}

impl fmt::Display for DeviceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A per-relkey version vector: `{device_id: counter}` (§2.6).
///
/// Missing components read as `0`, and zero components are **never stored**
/// (construction normalizes them away), so structural equality coincides
/// with [`compare`] returning [`VvOrder::Equal`]. Serializes as a plain
/// JSON map, matching the `vv` field of journal entries, manifests, and
/// tombstones.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct VersionVector(BTreeMap<DeviceId, u32>);

impl VersionVector {
    /// The empty vector (every component `0`).
    pub fn new() -> Self {
        Self::default()
    }

    /// The counter for `device` (`0` when absent).
    pub fn get(&self, device: &DeviceId) -> u32 {
        self.0.get(device).copied().unwrap_or(0)
    }

    /// Increments `device`'s component by one (one admitted upload = one
    /// version, §2.6).
    pub fn bump(&mut self, device: &DeviceId) {
        let slot = self.0.entry(device.clone()).or_insert(0);
        *slot = slot.saturating_add(1);
    }

    /// Elementwise-max merge of `other` into `self` (§2.6: after conflict
    /// resolution the key's vv becomes the max of both branches).
    pub fn merge(&mut self, other: &VersionVector) {
        for (device, &count) in &other.0 {
            if count == 0 {
                continue;
            }
            let slot = self.0.entry(device.clone()).or_insert(0);
            *slot = (*slot).max(count);
        }
    }

    /// Number of non-zero components present.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// `true` when no component is present.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Iterates `(device, counter)` pairs in device order.
    pub fn iter(&self) -> std::collections::btree_map::Iter<'_, DeviceId, u32> {
        self.0.iter()
    }
}

impl FromIterator<(DeviceId, u32)> for VersionVector {
    /// Collects `(device, counter)` pairs, dropping zero counters (see the
    /// type docs: zero components are never stored).
    fn from_iter<T: IntoIterator<Item = (DeviceId, u32)>>(iter: T) -> Self {
        let mut map = BTreeMap::new();
        for (device, count) in iter {
            if count == 0 {
                continue;
            }
            let slot = map.entry(device).or_insert(0);
            *slot = (*slot).max(count);
        }
        VersionVector(map)
    }
}

/// Outcome of ordering two version vectors (§2.6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VvOrder {
    /// Identical vectors.
    Equal,
    /// `a` dominates `b` (`∀d: a[d] ≥ b[d]`, and `a ≠ b`).
    Greater,
    /// `b` dominates `a`.
    Less,
    /// Neither dominates: the versions are concurrent — a conflict.
    Concurrent,
}

/// Orders `a` against `b`: `a ≥ b` iff every component of `a` is ≥ the
/// matching component of `b` (missing = 0). Neither dominating means the
/// versions are [`VvOrder::Concurrent`].
pub fn compare(a: &VersionVector, b: &VersionVector) -> VvOrder {
    let mut a_ge_b = true;
    let mut b_ge_a = true;
    for device in a.0.keys().chain(b.0.keys()) {
        let ca = a.get(device);
        let cb = b.get(device);
        if ca < cb {
            a_ge_b = false;
        }
        if cb < ca {
            b_ge_a = false;
        }
    }
    match (a_ge_b, b_ge_a) {
        (true, true) => VvOrder::Equal,
        (true, false) => VvOrder::Greater,
        (false, true) => VvOrder::Less,
        (false, false) => VvOrder::Concurrent,
    }
}

/// One side of a concurrent pair handed to [`pick_winner`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Candidate<'a> {
    /// Wall-clock timestamp of the version (unix seconds, from the journal
    /// entry). Used **only** to pick the primary of a concurrent pair.
    pub ts: i64,
    /// Authoring device — lexicographic tiebreak when `ts` is equal.
    pub device: &'a DeviceId,
}

/// The deterministic winner rule for a provably concurrent pair (§2.6 case
/// 4): higher `ts` wins; on a tie the lexicographically greater `device` id
/// wins. Totally deterministic and symmetric:
/// `pick_winner(a, b) == pick_winner(b, a)` for all inputs.
pub fn pick_winner<'a>(a: Candidate<'a>, b: Candidate<'a>) -> Candidate<'a> {
    if (a.ts, a.device) >= (b.ts, b.device) {
        a
    } else {
        b
    }
}
