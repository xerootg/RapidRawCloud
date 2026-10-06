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
use serde_json::Value;

use crate::clock::{compare, pick_winner, Candidate, DeviceId, VersionVector, VvOrder};
use crate::journal::Kind;
use crate::keys::{
    local_path, relkey, KeyError, RelKey, ALBUMS_META_KEY, CONTROL_PREFIX, PRESETS_META_KEY,
};
use crate::semhash::{canonical_json, Blake3Hex};

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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    // §2.6 case 1: `remote.vv == local.vv` OR `remote.sem_hash ==
    // local.sem_hash` → converged, checked before the vv order at all —
    // identical content never conflicts, even under a concurrent vv (two
    // devices independently producing the same bytes, e.g. both creating
    // the same default/empty document on first run).
    if local.blake3 == remote.blake3 {
        return MetaDecision::Converged;
    }
    match compare(&remote.vv, &local.vv) {
        VvOrder::Equal => MetaDecision::Converged,
        VvOrder::Greater => MetaDecision::AdoptRemote,
        VvOrder::Less => MetaDecision::KeepLocal,
        VvOrder::Concurrent => {
            let winner = pick_winner(
                Candidate {
                    ts: remote.ts,
                    device: &remote.device,
                },
                Candidate {
                    ts: local.ts,
                    device: &local.device,
                },
            );
            let remote_wins = winner.ts == remote.ts && *winner.device == remote.device;
            MetaDecision::Conflict { remote_wins }
        }
    }
}

/// Rewrites a meta document for **upload** (§2.9): every absolute image path
/// (albums) or `lutPath` (presets) that is under `sync_root` becomes
/// `rr://<relkey>`; any path outside `sync_root` is dropped from the
/// returned bytes (the caller keeps the local file untouched, so the album
/// stays device-local). Pretty-printed JSON, stable field order, so a
/// semantically unchanged document relativizes to byte-identical output
/// (feeds [`MetaKind::conflict_key`] determinism).
pub fn relativize(kind: MetaKind, doc: &[u8], sync_root: &Path) -> Result<Vec<u8>, MetaError> {
    let mut value: Value = serde_json::from_slice(doc)?;
    match kind {
        MetaKind::Albums => relativize_albums(&mut value, sync_root),
        MetaKind::Presets => relativize_presets(&mut value, sync_root),
    }
    Ok(serde_json::to_vec_pretty(&value)?)
}

/// Rewrites a downloaded meta document for **this device** (§2.9): every
/// `rr://<relkey>` is mapped back to an absolute path under `sync_root`.
/// Entries with no `rr://` form (there are none in a well-formed uploaded
/// copy — out-of-root paths were dropped at upload) pass through unchanged.
/// A malformed `rr://` relkey is a typed [`MetaError::Key`] (fail closed).
pub fn localize(kind: MetaKind, doc: &[u8], sync_root: &Path) -> Result<Vec<u8>, MetaError> {
    let mut value: Value = serde_json::from_slice(doc)?;
    match kind {
        MetaKind::Albums => localize_albums(&mut value, sync_root)?,
        MetaKind::Presets => localize_presets(&mut value, sync_root)?,
    }
    Ok(serde_json::to_vec_pretty(&value)?)
}

/// The canonical byte form of a meta document used for loser hashing
/// (§2.9): parse, re-serialize with sorted object keys and no insignificant
/// whitespace, so two devices holding the same logical loser produce the
/// same bytes — and therefore the same [`MetaKind::conflict_key`] suffix —
/// regardless of how either serialized it locally.
pub fn canonical(doc: &[u8]) -> Result<Vec<u8>, MetaError> {
    let value: Value = serde_json::from_slice(doc)?;
    Ok(canonical_json(&value).into_bytes())
}

/// Maps one path string through [`relkey`] into its `rr://<relkey>` wire
/// form; `None` when `s` is not a path under `sync_root` (or not a mappable
/// path at all — see [`relativize`]'s doc for the drop rule this feeds).
fn relativize_path_str(s: &str, sync_root: &Path) -> Option<String> {
    relkey(Path::new(s), sync_root)
        .ok()
        .map(|rk| format!("{RR_SCHEME}{}", rk.as_str()))
}

/// The reverse of [`relativize_path_str`]: an `rr://<relkey>` form maps back
/// to an absolute path under `sync_root`; anything else (there is no
/// `rr://` form in a well-formed uploaded copy) passes through unchanged.
fn localize_path_str(s: &str, sync_root: &Path) -> Result<String, MetaError> {
    match s.strip_prefix(RR_SCHEME) {
        Some(rest) => {
            let rk = RelKey::parse_wire(rest.to_string())?;
            Ok(local_path(&rk, sync_root).to_string_lossy().into_owned())
        }
        None => Ok(s.to_string()),
    }
}

/// Relativizes every `images` array (recursing through `children`, §2.9):
/// in-root entries become `rr://<relkey>`, out-of-root entries are dropped.
fn relativize_albums(value: &mut Value, sync_root: &Path) {
    match value {
        Value::Array(items) => {
            for item in items.iter_mut() {
                relativize_albums(item, sync_root);
            }
        }
        Value::Object(map) => {
            if let Some(Value::Array(images)) = map.get_mut("images") {
                let mapped: Vec<Value> = images
                    .iter()
                    .filter_map(|img| {
                        img.as_str()
                            .and_then(|s| relativize_path_str(s, sync_root))
                            .map(Value::String)
                    })
                    .collect();
                *images = mapped;
            }
            if let Some(children) = map.get_mut("children") {
                relativize_albums(children, sync_root);
            }
        }
        _ => {}
    }
}

/// Merges this device's out-of-root album membership from `old_local`
/// (the bytes currently on disk, if any) into `new_localized` (a document
/// just adopted from the remote via [`localize`], about to replace the
/// local file) before the caller overwrites it (§2.9).
///
/// A meta document's shared/remote form never carries an out-of-root path
/// at all — it is dropped at whichever device's upload first relativized
/// it ([`relativize_albums`] / [`relativize_presets`]'s drop rule, "kept
/// locally"). So a plain atomic replace of the local file with every
/// adopted/won remote document — not just the first one — would silently
/// destroy that out-of-root data on every subsequent apply, since the
/// remote the device is adopting was never in a position to carry it
/// forward. This restores it for **both** kinds: albums are matched by
/// `"id"`, and an out-of-root image present on the old local album but
/// absent from the new one is appended; presets are matched by `"id"`, and
/// an out-of-root `lutPath` present on the old local preset but absent from
/// the new one is restored at the same nested location it was stripped
/// from (normally `adjustments.lutPath`, but the path is recorded rather
/// than assumed so an unexpected shape still round-trips). Both are
/// skipped if already present, so a repeated apply is idempotent.
///
/// `old_local` absent (this device has never held a local copy before) is
/// a no-op — there is no prior local-only data to preserve. Malformed
/// `old_local` bytes are likewise treated as "nothing to merge" rather than
/// failing the whole adopt (the remote is still a valid document on its
/// own); `new_localized` is assumed well-formed (it just round-tripped
/// through [`localize`]) and its errors propagate.
pub fn merge_out_of_root(
    kind: MetaKind,
    old_local: Option<&[u8]>,
    new_localized: &[u8],
    sync_root: &Path,
) -> Result<Vec<u8>, MetaError> {
    let Some(old_local) = old_local else {
        return Ok(new_localized.to_vec());
    };
    let Ok(old) = serde_json::from_slice::<Value>(old_local) else {
        return Ok(new_localized.to_vec());
    };
    let mut new_value: Value = serde_json::from_slice(new_localized)?;
    match kind {
        MetaKind::Albums => {
            let out_of_root = collect_album_out_of_root(&old, sync_root);
            if !out_of_root.is_empty() {
                merge_album_out_of_root(&mut new_value, &out_of_root);
            }
        }
        MetaKind::Presets => {
            let out_of_root = collect_preset_out_of_root(&old, sync_root);
            if !out_of_root.is_empty() {
                merge_preset_out_of_root(&mut new_value, &out_of_root);
            }
        }
    }
    Ok(serde_json::to_vec_pretty(&new_value)?)
}

/// Walks an albums document (local, absolute-path form) and collects, per
/// album `id`, the images that are *not* under `sync_root` — exactly the
/// set [`relativize_albums`] would drop from an uploaded copy.
fn collect_album_out_of_root(
    value: &Value,
    sync_root: &Path,
) -> std::collections::HashMap<String, Vec<String>> {
    fn walk(
        value: &Value,
        sync_root: &Path,
        out: &mut std::collections::HashMap<String, Vec<String>>,
    ) {
        match value {
            Value::Array(items) => {
                for item in items {
                    walk(item, sync_root, out);
                }
            }
            Value::Object(map) => {
                if let (Some(Value::String(id)), Some(Value::Array(images))) =
                    (map.get("id"), map.get("images"))
                {
                    let oor: Vec<String> = images
                        .iter()
                        .filter_map(Value::as_str)
                        .filter(|s| relativize_path_str(s, sync_root).is_none())
                        .map(str::to_string)
                        .collect();
                    if !oor.is_empty() {
                        out.insert(id.clone(), oor);
                    }
                }
                if let Some(children) = map.get("children") {
                    walk(children, sync_root, out);
                }
            }
            _ => {}
        }
    }
    let mut out = std::collections::HashMap::new();
    walk(value, sync_root, &mut out);
    out
}

/// The reverse direction of [`collect_album_out_of_root`]: for every album
/// in `value` whose `id` has a recorded out-of-root set, appends any entry
/// not already present in that album's `images` array (order-preserving,
/// idempotent).
fn merge_album_out_of_root(
    value: &mut Value,
    out_of_root: &std::collections::HashMap<String, Vec<String>>,
) {
    match value {
        Value::Array(items) => {
            for item in items.iter_mut() {
                merge_album_out_of_root(item, out_of_root);
            }
        }
        Value::Object(map) => {
            let id = map.get("id").and_then(Value::as_str).map(str::to_string);
            if let Some(extra) = id.as_deref().and_then(|id| out_of_root.get(id)) {
                let images = map
                    .entry("images".to_string())
                    .or_insert_with(|| Value::Array(Vec::new()));
                if let Value::Array(images) = images {
                    for path in extra {
                        let already_present =
                            images.iter().any(|v| v.as_str() == Some(path.as_str()));
                        if !already_present {
                            images.push(Value::String(path.clone()));
                        }
                    }
                }
            }
            if let Some(children) = map.get_mut("children") {
                merge_album_out_of_root(children, out_of_root);
            }
        }
        _ => {}
    }
}

/// The reverse of [`relativize_albums`]: every `rr://` image entry maps
/// back onto `sync_root`. A malformed relkey fails closed.
fn localize_albums(value: &mut Value, sync_root: &Path) -> Result<(), MetaError> {
    match value {
        Value::Array(items) => {
            for item in items.iter_mut() {
                localize_albums(item, sync_root)?;
            }
        }
        Value::Object(map) => {
            if let Some(Value::Array(images)) = map.get_mut("images") {
                for img in images.iter_mut() {
                    if let Value::String(s) = img {
                        *s = localize_path_str(s, sync_root)?;
                    }
                }
            }
            if let Some(children) = map.get_mut("children") {
                localize_albums(children, sync_root)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Relativizes every `lutPath` field anywhere in the document (§2.9): an
/// in-root value becomes `rr://<relkey>`; an out-of-root value drops the
/// field entirely (LUTs are not synced in v1, the same rule as a sidecar's
/// `lutPath`).
fn relativize_presets(value: &mut Value, sync_root: &Path) {
    if let Value::Object(map) = value {
        let replacement = map
            .get("lutPath")
            .and_then(Value::as_str)
            .map(|s| relativize_path_str(s, sync_root));
        match replacement {
            Some(Some(rr)) => {
                map.insert("lutPath".to_string(), Value::String(rr));
            }
            Some(None) => {
                map.remove("lutPath");
            }
            None => {}
        }
    }
    match value {
        Value::Array(items) => {
            for item in items.iter_mut() {
                relativize_presets(item, sync_root);
            }
        }
        Value::Object(map) => {
            for v in map.values_mut() {
                relativize_presets(v, sync_root);
            }
        }
        _ => {}
    }
}

/// Walks a presets document (local, absolute-path form) and collects, per
/// preset `id`, the out-of-root `lutPath` values [`relativize_presets`]
/// would drop from an uploaded copy — together with the key path from the
/// preset object down to the field (normally `["adjustments"]`, since
/// `lutPath` lives inside `adjustments`), so [`merge_preset_out_of_root`]
/// can restore it at the exact nested location it was taken from rather
/// than assuming a fixed shape.
///
/// An `id` found on an object applies to that object's whole subtree (its
/// nested `lutPath`, wherever it lives) until a *nested* object introduces
/// its own `id` (e.g. a folder's preset children), at which point the path
/// tracked for matches resets to be relative to that nearer id — the same
/// "closest enclosing id" rule a folder/preset tree needs regardless of
/// nesting depth.
fn collect_preset_out_of_root(
    value: &Value,
    sync_root: &Path,
) -> std::collections::HashMap<String, Vec<(Vec<String>, String)>> {
    fn walk(
        value: &Value,
        sync_root: &Path,
        id_ctx: Option<&str>,
        rel_path: &[String],
        out: &mut std::collections::HashMap<String, Vec<(Vec<String>, String)>>,
    ) {
        match value {
            Value::Array(items) => {
                for item in items {
                    walk(item, sync_root, id_ctx, rel_path, out);
                }
            }
            Value::Object(map) => {
                let own_id = map.get("id").and_then(Value::as_str);
                let (effective_id, base_path): (Option<&str>, &[String]) = match own_id {
                    Some(id) => (Some(id), &[][..]),
                    None => (id_ctx, rel_path),
                };
                for (k, v) in map {
                    if k == "lutPath" {
                        if let (Some(id), Value::String(s)) = (effective_id, v) {
                            if relativize_path_str(s, sync_root).is_none() {
                                out.entry(id.to_string())
                                    .or_default()
                                    .push((base_path.to_vec(), s.clone()));
                            }
                        }
                        continue;
                    }
                    let mut child_path = base_path.to_vec();
                    child_path.push(k.clone());
                    walk(v, sync_root, effective_id, &child_path, out);
                }
            }
            _ => {}
        }
    }
    let mut out = std::collections::HashMap::new();
    walk(value, sync_root, None, &[], &mut out);
    out
}

/// The reverse direction of [`collect_preset_out_of_root`]: for every
/// preset in `value` whose `id` has a recorded out-of-root `lutPath`,
/// restores it at the recorded key path — unless a `lutPath` is already
/// present there (an in-root value just localized, or a previous idempotent
/// restore), so a repeated apply never clobbers real data or duplicates.
fn merge_preset_out_of_root(
    value: &mut Value,
    out_of_root: &std::collections::HashMap<String, Vec<(Vec<String>, String)>>,
) {
    fn set_at_path(map: &mut serde_json::Map<String, Value>, path: &[String], lut: &str) {
        match path.split_first() {
            None => {
                map.entry("lutPath".to_string())
                    .or_insert_with(|| Value::String(lut.to_string()));
            }
            Some((head, rest)) => {
                let entry = map
                    .entry(head.clone())
                    .or_insert_with(|| Value::Object(serde_json::Map::new()));
                if let Value::Object(inner) = entry {
                    set_at_path(inner, rest, lut);
                }
            }
        }
    }
    match value {
        Value::Array(items) => {
            for item in items.iter_mut() {
                merge_preset_out_of_root(item, out_of_root);
            }
        }
        Value::Object(map) => {
            let id = map.get("id").and_then(Value::as_str).map(str::to_string);
            if let Some(entries) = id.as_deref().and_then(|id| out_of_root.get(id)) {
                for (path, lut) in entries {
                    set_at_path(map, path, lut);
                }
            }
            for v in map.values_mut() {
                merge_preset_out_of_root(v, out_of_root);
            }
        }
        _ => {}
    }
}

/// The reverse of [`relativize_presets`]: every `rr://` `lutPath` maps back
/// onto `sync_root`. A malformed relkey fails closed.
fn localize_presets(value: &mut Value, sync_root: &Path) -> Result<(), MetaError> {
    if let Value::Object(map) = value {
        if let Some(Value::String(s)) = map.get_mut("lutPath") {
            *s = localize_path_str(s, sync_root)?;
        }
    }
    match value {
        Value::Array(items) => {
            for item in items.iter_mut() {
                localize_presets(item, sync_root)?;
            }
        }
        Value::Object(map) => {
            for v in map.values_mut() {
                localize_presets(v, sync_root)?;
            }
        }
        _ => {}
    }
    Ok(())
}
