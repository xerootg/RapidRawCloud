//! Albums & presets as synced **meta documents** (architecture §2.9).
//!
//! Albums and presets are the last library data to sync. Unlike sidecars
//! and originals they are not per-image library files: each is a single
//! whole document (`albums.json`, `presets.json`) that lives in the app
//! data dir, references images by **absolute local path**, and must survive
//! a round-trip through a bucket shared by devices whose sync roots differ.
//!
//! Two concerns live here, both pure (bytes in, typed values out — no I/O,
//! no network):
//!
//! 1. **Relativization** ([`relativize`] / [`localize`]). On upload every
//!    absolute image path *under the sync root* is rewritten to
//!    `rr://<relkey>` (a [`crate::keys::RelKey`] carried in the
//!    [`RR_SCHEME`] URI form); a path *outside* the sync root is dropped
//!    from the uploaded copy (the album keeps it in the device-local file,
//!    so an album that references a non-synced folder stays device-local).
//!    On download `rr://<relkey>` is rewritten back to an absolute path
//!    under *this* device's sync root. Presets carry no image paths except
//!    LUT references (`lutPath`), relativized by the same rule (LUTs are not
//!    synced in v1, exactly like the sidecar `lutPath`). Frontend pseudo-
//!    paths such as `"Album: <name>"` never appear in `albums.json`
//!    (they live only in `useLibraryStore`), so they need no mapping.
//!
//! 2. **Whole-document version-vector resolution** ([`decide_meta`]). A meta
//!    document is just another whole-document kind: it carries a version
//!    vector and is resolved by the **same §2.6 apply rule** as a sidecar —
//!    converged / remote-dominates / local-dominates / concurrent — reusing
//!    [`crate::clock::compare`] and [`crate::clock::pick_winner`] verbatim
//!    (this module never reimplements conflict resolution). On a concurrent
//!    conflict the deterministic loser is preserved as
//!    `albums.conflict-<blake3(canonical loser)[..6]>.json` under
//!    `.rrcloud/v1/meta/` ([`MetaKind::conflict_key`]), the same suffix on
//!    every device, so a `sync-conflict` surfaces one recoverable copy and
//!    no album/preset data is ever lost.

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::clock::{DeviceId, VersionVector};
use crate::journal::Kind;
use crate::keys::{KeyError, ALBUMS_META_KEY, CONTROL_PREFIX, PRESETS_META_KEY};
use crate::semhash::Blake3Hex;

/// URI scheme marking a relativized, library-relative image/LUT path inside
/// a synced meta document (`rr://<relkey>`). Chosen so a relativized path is
/// never mistaken for a local absolute path on any platform (no absolute
/// path begins `rr://`).
pub const RR_SCHEME: &str = "rr://";

/// Error from meta-document relativization or resolution.
#[derive(Debug, thiserror::Error)]
pub enum MetaError {
    /// The document is not valid JSON, or not the shape its [`MetaKind`]
    /// expects (an albums array / a presets array).
    #[error("invalid meta document JSON: {0}")]
    Json(#[from] serde_json::Error),
    /// An `rr://` URI inside a downloaded document carried a relkey that
    /// fails wire validation (non-NFC, traversal, reserved, …) — fail
    /// closed rather than map it to an unexpected local path.
    #[error("invalid rr:// relkey in meta document: {0}")]
    Key(#[from] KeyError),
}

/// Which meta document (§2.9). Determines the bucket key, the journal
/// [`Kind`], and the conflict-copy key prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MetaKind {
    /// `albums.json` — the album tree (`AlbumItem` forest on the app side).
    Albums,
    /// `presets.json` — the preset tree (`PresetItem` forest on the app
    /// side).
    Presets,
}

impl MetaKind {
    /// The fixed bucket key this document syncs to (§1.2 / §2.9):
    /// `.rrcloud/v1/meta/albums.json` or `.rrcloud/v1/meta/presets.json`.
    pub fn meta_key(self) -> &'static str {
        match self {
            MetaKind::Albums => ALBUMS_META_KEY,
            MetaKind::Presets => PRESETS_META_KEY,
        }
    }

    /// The journal [`Kind`] carried by this document's `put` entries.
    pub fn journal_kind(self) -> Kind {
        match self {
            MetaKind::Albums => Kind::Albums,
            MetaKind::Presets => Kind::Presets,
        }
    }

    /// The document's basename stem under `.rrcloud/v1/meta/` (`albums` /
    /// `presets`), used to build both [`MetaKind::meta_key`] and
    /// [`MetaKind::conflict_key`].
    fn stem(self) -> &'static str {
        match self {
            MetaKind::Albums => "albums",
            MetaKind::Presets => "presets",
        }
    }

    /// The deterministic conflict-loser key for a §2.6-concurrent meta
    /// conflict (§2.9): `.rrcloud/v1/meta/<stem>.conflict-<6hex>.json`,
    /// where `6hex = blake3(canonical_loser)[..6]`.
    ///
    /// The caller passes the loser's **canonical** bytes ([`canonical`]) so
    /// the suffix is byte-identical on every device that materializes the
    /// loser — exactly the single-copy guarantee §2.6 gives sidecar losers.
    pub fn conflict_key(self, canonical_loser: &[u8]) -> String {
        let hex = Blake3Hex::from_bytes(canonical_loser);
        format!(
            "{CONTROL_PREFIX}meta/{}.conflict-{}.json",
            self.stem(),
            &hex.as_str()[..6]
        )
    }
}

/// A meta document's resolvable head: the version vector and content hash
/// of one side, plus the `(ts, device)` the §2.6 concurrent tiebreak reads.
/// Deliberately the same five facts [`crate::clock::pick_winner`] and
/// [`crate::clock::compare`] already consume, so [`decide_meta`] is pure
/// reuse of the engine's version-vector machinery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaHead {
    /// The document version's version vector (§2.6).
    pub vv: VersionVector,
    /// blake3 of the relativized document bytes as stored in the bucket.
    pub blake3: Blake3Hex,
    /// Wall-clock unix seconds of the version (concurrent tiebreak only).
    pub ts: i64,
    /// Authoring device (lexicographic tiebreak when `ts` ties).
    pub device: DeviceId,
}

/// The §2.6 apply decision for a meta document, ordering an arriving remote
/// head against the local head — the same four outcomes as a sidecar.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetaDecision {
    /// Equal vv (or identical content): adopt metadata, no write.
    Converged,
    /// Remote dominates: adopt the remote document (download + localize +
    /// atomic replace of the local file).
    AdoptRemote,
    /// Local dominates: ignore (our version reaches the peer via our
    /// journal).
    KeepLocal,
    /// Concurrent: resolve by [`crate::clock::pick_winner`]. `remote_wins`
    /// is true when the remote head is the deterministic primary; the other
    /// side is materialized as the conflict loser. After resolution the vv
    /// is the elementwise max of both so the conflict cannot reopen.
    Conflict {
        /// Whether the remote head won the deterministic pick.
        remote_wins: bool,
    },
}

/// Orders an arriving `remote` meta head against the `local` head under the
/// §2.6 unified apply rule (§2.9), reusing [`crate::clock::compare`] and —
/// for the concurrent case — [`crate::clock::pick_winner`]. Never
/// reimplements the comparison or the tiebreak.
///
/// Totally deterministic: every device that sees the same pair reaches the
/// same [`MetaDecision`], and the concurrent winner is stable regardless of
/// arrival order (`pick_winner` is symmetric).
pub fn decide_meta(local: &MetaHead, remote: &MetaHead) -> MetaDecision {
    let _ = (local, remote);
    todo!("P6 green: reuse clock::compare + pick_winner to order the two meta heads")
}

/// Rewrites a meta document for **upload** (§2.9): every absolute image path
/// (albums) or `lutPath` (presets) that is under `sync_root` becomes
/// `rr://<relkey>`; any path outside `sync_root` is dropped from the
/// returned bytes (the caller keeps the local file untouched, so the album
/// stays device-local). Pretty-printed JSON, stable field order, so a
/// semantically unchanged document relativizes to byte-identical output
/// (feeds [`MetaKind::conflict_key`] determinism).
pub fn relativize(kind: MetaKind, doc: &[u8], sync_root: &Path) -> Result<Vec<u8>, MetaError> {
    let _ = (kind, doc, sync_root);
    todo!("P6 green: relativize in-root paths to rr://, drop out-of-root entries")
}

/// Rewrites a downloaded meta document for **this device** (§2.9): every
/// `rr://<relkey>` is mapped back to an absolute path under `sync_root`.
/// Entries with no `rr://` form (there are none in a well-formed uploaded
/// copy — out-of-root paths were dropped at upload) pass through unchanged.
/// A malformed `rr://` relkey is a typed [`MetaError::Key`] (fail closed).
pub fn localize(kind: MetaKind, doc: &[u8], sync_root: &Path) -> Result<Vec<u8>, MetaError> {
    let _ = (kind, doc, sync_root);
    todo!("P6 green: map rr://<relkey> back to this device's absolute paths")
}

/// The canonical byte form of a meta document used for loser hashing
/// (§2.9): parse, re-serialize with sorted object keys and no insignificant
/// whitespace, so two devices holding the same logical loser produce the
/// same bytes — and therefore the same [`MetaKind::conflict_key`] suffix —
/// regardless of how either serialized it locally.
pub fn canonical(doc: &[u8]) -> Result<Vec<u8>, MetaError> {
    let _ = doc;
    todo!("P6 green: parse + re-serialize with sorted keys for a stable loser hash")
}
