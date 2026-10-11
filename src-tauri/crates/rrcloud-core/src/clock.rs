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

/// The wire types themselves are the generated `rrcloud-proto` SDK's
/// (`protocol/rrcloud.protocol.toml` is the single definition): the
/// canonical-UUIDv4 device id and the drop-zero version vector, whose
/// `Deserialize` normalizes explicit zero components away so structural
/// equality coincides with [`compare`] returning [`VvOrder::Equal`].
pub use rrcloud_proto::{DeviceId, VersionVector, VvOrder};

/// Error returned when a string is not a valid [`DeviceId`]: the SDK's
/// decode error (`ProtoError::Invalid { ty: "DeviceId", .. }`).
pub type DeviceIdError = rrcloud_proto::ProtoError;

/// Orders `a` against `b`: `a ≥ b` iff every component of `a` is ≥ the
/// matching component of `b` (missing = 0). Neither dominating means the
/// versions are [`VvOrder::Concurrent`]. (The free-function spelling of
/// [`VersionVector::compare`], kept because the engine reads better with
/// the symmetric form.)
pub fn compare(a: &VersionVector, b: &VersionVector) -> VvOrder {
    a.compare(b)
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

/// The converged/concurrent-pair identity tiebreak (§2.6 case 1 / case 4):
/// a strict [`VvOrder`] decides the adopted `(ts, device)` identity outright
/// (`Greater` → `remote` wins, `Less`/`Equal` → `local` wins, matching the
/// causal order itself rather than the wall clock); only a genuinely
/// [`VvOrder::Concurrent`] pair falls to the deterministic [`pick_winner`]
/// over the same two candidates. Factored out so every call site performing
/// this exact adoption decision — the sidecar engine's
/// `remote_wins_identity`/`converged_identity_is_remote` and the meta-sync
/// manager's converged-head arm — shares one rule instead of maintaining
/// independent copies that could silently diverge if the tiebreak ever
/// changes.
pub fn identity_order_wins_remote<'a>(
    ord: VvOrder,
    remote: Candidate<'a>,
    local: Candidate<'a>,
) -> bool {
    match ord {
        VvOrder::Greater => true,
        VvOrder::Less | VvOrder::Equal => false,
        VvOrder::Concurrent => {
            let winner = pick_winner(remote, local);
            winner.ts == remote.ts && *winner.device == *remote.device
        }
    }
}
