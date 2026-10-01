//! Library-relative key mapping and the bucket key schema
//! (architecture §1.1–§1.2).
//!
//! The cloud namespace is **library-relative**: a [`RelKey`] is a path
//! relative to the sync root, `/`-separated, Unicode NFC-normalized, with no
//! leading slash and no `.`/`..` segments, backslashes, or control
//! characters. All bucket keys are built from typed constructors in this
//! module, and [`classify_key`] parses any bucket key back into its schema
//! role — the reconcile loop (§2.3) depends on that being an exact inverse.

use std::fmt;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::clock::DeviceId;
use crate::semhash::ContentId;

/// Prefix of the byte-faithful mirror of the on-disk library tree (§1.2).
pub const LIBRARY_PREFIX: &str = "library/";

/// Prefix of the control plane (§1.2). Dot-prefixed so `scan_dir_lazy`
/// ignores it if the bucket is ever FUSE-mounted into a library.
pub const CONTROL_PREFIX: &str = ".rrcloud/v1/";

/// Key of the relativized albums document (§2.9).
pub const ALBUMS_META_KEY: &str = ".rrcloud/v1/meta/albums.json";

/// Key of the relativized presets document (§2.9).
pub const PRESETS_META_KEY: &str = ".rrcloud/v1/meta/presets.json";

/// Error from relkey mapping or key construction.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KeyError {
    /// Empty path, or a path that resolves to the sync root itself.
    #[error("empty relkey")]
    Empty,
    /// A backslash — never a valid separator in the cloud namespace.
    #[error("backslash in relkey: {0:?}")]
    Backslash(String),
    /// An ASCII control character (including NUL).
    #[error("control character in relkey: {0:?}")]
    ControlChar(String),
    /// A `.` or `..` segment, or an empty segment (`//`, trailing `/`).
    #[error("dot, dot-dot, or empty segment in relkey: {0:?}")]
    BadSegment(String),
    /// A leading slash (relkeys are always relative).
    #[error("leading slash in relkey: {0:?}")]
    LeadingSlash(String),
    /// The local path does not live under the sync root.
    #[error("path escapes the sync root: {0:?}")]
    OutsideRoot(String),
    /// The local path is not valid Unicode.
    #[error("non-Unicode path")]
    NonUnicode,
    /// A virtual-copy suffix that is not exactly 6 lowercase hex chars.
    #[error("invalid virtual-copy suffix: {0:?}")]
    BadVcSuffix(String),
}

/// A validated library-relative key (§1.1): `/`-separated, NFC-normalized,
/// no leading slash.
///
/// Ordered and hashable so it can key `BTreeMap`/`HashMap` state tables;
/// the ordering is the byte order of the normalized string.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct RelKey(String);

impl RelKey {
    /// Validates and NFC-normalizes a relative path string into a [`RelKey`].
    ///
    /// Rejects empty strings, leading `/`, backslashes, control characters,
    /// and `.`/`..`/empty segments. Composed and decomposed spellings of the
    /// same Unicode text normalize to the same [`RelKey`].
    pub fn new(s: impl Into<String>) -> Result<Self, KeyError> {
        let s = s.into();
        let _ = s;
        todo!("P1-U1 green: RelKey validation + NFC")
    }

    /// The normalized relative path, `/`-separated.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for RelKey {
    type Error = KeyError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::new(s)
    }
}

impl From<RelKey> for String {
    fn from(k: RelKey) -> String {
        k.0
    }
}

impl fmt::Display for RelKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Maps a local absolute path under `sync_root` to its [`RelKey`] (§1.1).
///
/// Pure path arithmetic: nothing is touched on disk. The path must be
/// lexically inside `sync_root` (no `..` escapes), and the relative part is
/// validated and NFC-normalized exactly as [`RelKey::new`] does.
pub fn relkey(path: &Path, sync_root: &Path) -> Result<RelKey, KeyError> {
    let _ = (path, sync_root);
    todo!("P1-U1 green: local path -> relkey mapping")
}

/// Joins a [`RelKey`] back onto the local `sync_root` (§1.1 reverse
/// mapping). Round-trips with [`relkey`] for valid, NFC-normalized paths.
pub fn local_path(rel: &RelKey, sync_root: &Path) -> PathBuf {
    let _ = (rel, sync_root);
    todo!("P1-U1 green: relkey -> local path mapping")
}

/// `library/<relpath>` — an original (or `.xmp`) byte-identical to local.
pub fn library_key(rel: &RelKey) -> String {
    let _ = rel;
    todo!("P1-U1 green: library key")
}

/// `library/<relpath>.rrdata` — the primary sidecar.
pub fn sidecar_key(rel: &RelKey) -> String {
    let _ = rel;
    todo!("P1-U1 green: sidecar key")
}

/// `library/<relpath>.<6hex>.rrdata` — a virtual-copy sidecar (including
/// deterministic conflict losers, §2.6). `vc6` must be exactly 6 lowercase
/// hex characters.
pub fn vc_sidecar_key(rel: &RelKey, vc6: &str) -> Result<String, KeyError> {
    let _ = (rel, vc6);
    todo!("P1-U1 green: virtual-copy sidecar key")
}

/// `.rrcloud/v1/journal/<device>/<seq:016x>.v1.ndjson` — a journal segment
/// (§2.2). The filename part is [`crate::journal::format_segment_filename`].
pub fn journal_segment_key(device: &DeviceId, seq: u64) -> String {
    let _ = (device, seq);
    todo!("P1-U1 green: journal segment key")
}

/// `.rrcloud/v1/manifests/<device>.json.gz` — the per-writer manifest (§2.3).
pub fn manifest_key(device: &DeviceId) -> String {
    let _ = device;
    todo!("P1-U1 green: manifest key")
}

/// `.rrcloud/v1/devices/<device>.json` — the device registry entry (§1.2).
pub fn device_registry_key(device: &DeviceId) -> String {
    let _ = device;
    todo!("P1-U1 green: device registry key")
}

/// `.rrcloud/v1/devices/<device>.retired` — the retirement marker (§2.10).
pub fn device_retired_key(device: &DeviceId) -> String {
    let _ = device;
    todo!("P1-U1 green: device retired key")
}

/// `.rrcloud/v1/tombstones/<blake3(relkey)[..32]>.json` — a deletion marker
/// (§2.7). The hash prefix is the first 32 lowercase hex chars of
/// `blake3(relkey bytes)`.
pub fn tombstone_key(rel: &RelKey) -> String {
    let _ = rel;
    todo!("P1-U1 green: tombstone key")
}

/// `.rrcloud/v1/previews/<content_id>.pxy.dng` — a smart preview (§4).
pub fn preview_key(content_id: &ContentId) -> String {
    let _ = content_id;
    todo!("P1-U1 green: preview key")
}

/// Thumb flavor for [`thumb_key`] (§1.2: 480px `_small`, 1280px `_medium`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ThumbSize {
    /// 480px, `<content_id>_small.jpg`.
    Small,
    /// 1280px, `<content_id>_medium.jpg`.
    Medium,
}

/// `.rrcloud/v1/thumbs/<content_id>_small.jpg` / `_medium.jpg`.
pub fn thumb_key(content_id: &ContentId, size: ThumbSize) -> String {
    let _ = (content_id, size);
    todo!("P1-U1 green: thumb key")
}

/// `.rrcloud/v1/thumbpacks/<blake3(folder relkey)[..16]>.tar` — a per-folder
/// pack of `_small` thumbs (§4.3). `folder` is the folder's relkey.
pub fn thumbpack_key(folder: &RelKey) -> String {
    let _ = folder;
    todo!("P1-U1 green: thumbpack key")
}

/// The schema role of a bucket key, as parsed back by [`classify_key`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyClass {
    /// `library/<relpath>` — an original (not `.xmp`, not `.rrdata`).
    Original {
        /// The library-relative path.
        relkey: RelKey,
    },
    /// `library/<relpath>.rrdata` (primary, `vc: None`) or
    /// `library/<relpath>.<6hex>.rrdata` (virtual copy, `vc: Some`).
    Sidecar {
        /// The relkey of the image the sidecar belongs to.
        relkey: RelKey,
        /// The 6-lowercase-hex virtual-copy suffix, if any.
        vc: Option<String>,
    },
    /// `library/<relpath>.xmp` — an interop XMP projection (§2.8). The
    /// relkey includes the `.xmp` extension (it is a real library file).
    Xmp {
        /// The library-relative path of the `.xmp` file itself.
        relkey: RelKey,
    },
    /// `.rrcloud/v1/journal/<device>/<seq:016x>.v<N>.ndjson`.
    Journal {
        /// The owning (single-writer) device.
        device: DeviceId,
        /// The segment's starting sequence number.
        seq: u64,
    },
    /// `.rrcloud/v1/manifests/<device>.json.gz`.
    Manifest {
        /// The owning device.
        device: DeviceId,
    },
    /// `.rrcloud/v1/devices/<device>.json`.
    DeviceRegistry {
        /// The registered device.
        device: DeviceId,
    },
    /// `.rrcloud/v1/devices/<device>.retired`.
    DeviceRetired {
        /// The retired device.
        device: DeviceId,
    },
    /// `.rrcloud/v1/tombstones/<hash32>.json`.
    Tombstone {
        /// First 32 lowercase hex chars of `blake3(relkey)`.
        hash32: String,
    },
    /// `.rrcloud/v1/previews/<content_id>.pxy.dng`.
    Preview {
        /// The content identity the preview renders.
        content_id: ContentId,
    },
    /// `.rrcloud/v1/thumbs/<content_id>_small.jpg` / `_medium.jpg`.
    Thumb {
        /// The content identity the thumb renders.
        content_id: ContentId,
        /// Which flavor.
        size: ThumbSize,
    },
    /// `.rrcloud/v1/thumbpacks/<hash16>.tar`.
    Thumbpack {
        /// First 16 lowercase hex chars of `blake3(folder relkey)`.
        hash16: String,
    },
    /// `.rrcloud/v1/meta/albums.json`.
    MetaAlbums,
    /// `.rrcloud/v1/meta/presets.json`.
    MetaPresets,
    /// Anything the schema does not recognize (including malformed device
    /// ids, bad hex widths, or invalid relkeys). Reconcile adopts these per
    /// §2.3; they are never destroyed.
    Foreign,
}

/// Parses a bucket key back into its schema role (§1.2). The exact inverse
/// of every key constructor in this module; anything unrecognized — or
/// recognized in shape but malformed in content — is [`KeyClass::Foreign`].
///
/// One deliberate ambiguity, inherited from upstream's 6-hex virtual-copy
/// recognizer: a file literally named `<stem>.<6hex>` would have its primary
/// sidecar classified as a virtual copy of `<stem>`. Matches
/// `list_images_in_dir` behavior.
pub fn classify_key(bucket_key: &str) -> KeyClass {
    let _ = bucket_key;
    todo!("P1-U1 green: bucket key classification")
}
