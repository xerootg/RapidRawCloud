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
        use unicode_normalization::UnicodeNormalization;
        let raw = s.into();
        if raw.contains('\\') {
            return Err(KeyError::Backslash(raw));
        }
        if raw.chars().any(|c| c.is_control()) {
            return Err(KeyError::ControlChar(raw));
        }
        let s: String = raw.nfc().collect();
        if s.is_empty() {
            return Err(KeyError::Empty);
        }
        if s.starts_with('/') {
            return Err(KeyError::LeadingSlash(s));
        }
        if s.split('/')
            .any(|seg| seg.is_empty() || seg == "." || seg == "..")
        {
            return Err(KeyError::BadSegment(s));
        }
        Ok(RelKey(s))
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
    let rel = path
        .strip_prefix(sync_root)
        .map_err(|_| KeyError::OutsideRoot(path.to_string_lossy().into_owned()))?;
    let mut joined = String::new();
    for comp in rel.components() {
        let seg = match comp {
            std::path::Component::Normal(os) => os.to_str().ok_or(KeyError::NonUnicode)?,
            std::path::Component::CurDir => ".",
            std::path::Component::ParentDir => "..",
            // Root/prefix components cannot appear in a stripped relative
            // path, but map them to a rejected spelling rather than panic.
            _ => "/",
        };
        if !joined.is_empty() {
            joined.push('/');
        }
        joined.push_str(seg);
    }
    RelKey::new(joined)
}

/// Joins a [`RelKey`] back onto the local `sync_root` (§1.1 reverse
/// mapping). Round-trips with [`relkey`] for valid, NFC-normalized paths.
pub fn local_path(rel: &RelKey, sync_root: &Path) -> PathBuf {
    let mut p = sync_root.to_path_buf();
    for seg in rel.as_str().split('/') {
        p.push(seg);
    }
    p
}

/// `library/<relpath>` — an original (or `.xmp`) byte-identical to local.
pub fn library_key(rel: &RelKey) -> String {
    format!("{LIBRARY_PREFIX}{rel}")
}

/// `library/<relpath>.rrdata` — the primary sidecar.
pub fn sidecar_key(rel: &RelKey) -> String {
    format!("{LIBRARY_PREFIX}{rel}.rrdata")
}

/// `library/<relpath>.<6hex>.rrdata` — a virtual-copy sidecar (including
/// deterministic conflict losers, §2.6). `vc6` must be exactly 6 lowercase
/// hex characters.
pub fn vc_sidecar_key(rel: &RelKey, vc6: &str) -> Result<String, KeyError> {
    if !is_lower_hex(vc6, 6) {
        return Err(KeyError::BadVcSuffix(vc6.to_string()));
    }
    Ok(format!("{LIBRARY_PREFIX}{rel}.{vc6}.rrdata"))
}

/// `true` when `s` is exactly `len` lowercase hex characters.
fn is_lower_hex(s: &str, len: usize) -> bool {
    s.len() == len
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// `.rrcloud/v1/journal/<device>/<seq:016x>.v1.ndjson` — a journal segment
/// (§2.2). The filename part is [`crate::journal::format_segment_filename`].
pub fn journal_segment_key(device: &DeviceId, seq: u64) -> String {
    format!(
        "{CONTROL_PREFIX}journal/{device}/{}",
        crate::journal::format_segment_filename(seq)
    )
}

/// `.rrcloud/v1/manifests/<device>.json.gz` — the per-writer manifest (§2.3).
pub fn manifest_key(device: &DeviceId) -> String {
    format!("{CONTROL_PREFIX}manifests/{device}.json.gz")
}

/// `.rrcloud/v1/devices/<device>.json` — the device registry entry (§1.2).
pub fn device_registry_key(device: &DeviceId) -> String {
    format!("{CONTROL_PREFIX}devices/{device}.json")
}

/// `.rrcloud/v1/devices/<device>.retired` — the retirement marker (§2.10).
pub fn device_retired_key(device: &DeviceId) -> String {
    format!("{CONTROL_PREFIX}devices/{device}.retired")
}

/// `.rrcloud/v1/tombstones/<blake3(relkey)[..32]>.json` — a deletion marker
/// (§2.7). The hash prefix is the first 32 lowercase hex chars of
/// `blake3(relkey bytes)`.
pub fn tombstone_key(rel: &RelKey) -> String {
    let hex = blake3::hash(rel.as_str().as_bytes()).to_hex();
    format!("{CONTROL_PREFIX}tombstones/{}.json", &hex.as_str()[..32])
}

/// `.rrcloud/v1/previews/<content_id>.pxy.dng` — a smart preview (§4).
pub fn preview_key(content_id: &ContentId) -> String {
    format!("{CONTROL_PREFIX}previews/{content_id}.pxy.dng")
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
    let suffix = match size {
        ThumbSize::Small => "small",
        ThumbSize::Medium => "medium",
    };
    format!("{CONTROL_PREFIX}thumbs/{content_id}_{suffix}.jpg")
}

/// `.rrcloud/v1/thumbpacks/<blake3(folder relkey)[..16]>.tar` — a per-folder
/// pack of `_small` thumbs (§4.3). `folder` is the folder's relkey.
pub fn thumbpack_key(folder: &RelKey) -> String {
    let hex = blake3::hash(folder.as_str().as_bytes()).to_hex();
    format!("{CONTROL_PREFIX}thumbpacks/{}.tar", &hex.as_str()[..16])
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
    if let Some(rest) = bucket_key.strip_prefix(LIBRARY_PREFIX) {
        return classify_library_key(rest);
    }
    if let Some(rest) = bucket_key.strip_prefix(CONTROL_PREFIX) {
        return classify_control_key(rest);
    }
    KeyClass::Foreign
}

/// Classifies the part of a bucket key after `library/`.
fn classify_library_key(rest: &str) -> KeyClass {
    if let Some(stem) = rest.strip_suffix(".rrdata") {
        // Virtual copy: `<relpath>.<6hex>.rrdata` (the documented upstream
        // 6-hex ambiguity: checked before the primary interpretation).
        if let Some((base, suffix)) = stem.rsplit_once('.') {
            if is_lower_hex(suffix, 6) {
                if let Ok(relkey) = RelKey::new(base) {
                    return KeyClass::Sidecar {
                        relkey,
                        vc: Some(suffix.to_string()),
                    };
                }
            }
        }
        return match RelKey::new(stem) {
            Ok(relkey) => KeyClass::Sidecar { relkey, vc: None },
            Err(_) => KeyClass::Foreign,
        };
    }
    match RelKey::new(rest) {
        Ok(relkey) if relkey.as_str().ends_with(".xmp") => KeyClass::Xmp { relkey },
        Ok(relkey) => KeyClass::Original { relkey },
        Err(_) => KeyClass::Foreign,
    }
}

/// Classifies the part of a bucket key after `.rrcloud/v1/`.
fn classify_control_key(rest: &str) -> KeyClass {
    let Some((area, tail)) = rest.split_once('/') else {
        return KeyClass::Foreign;
    };
    match area {
        "journal" => {
            let Some((dev, name)) = tail.split_once('/') else {
                return KeyClass::Foreign;
            };
            let (Ok(device), Ok(parsed)) = (
                DeviceId::new(dev),
                crate::journal::parse_segment_filename(name),
            ) else {
                return KeyClass::Foreign;
            };
            KeyClass::Journal {
                device,
                seq: parsed.seq,
            }
        }
        "manifests" => match tail
            .strip_suffix(".json.gz")
            .and_then(|d| DeviceId::new(d).ok())
        {
            Some(device) => KeyClass::Manifest { device },
            None => KeyClass::Foreign,
        },
        "devices" => {
            if let Some(device) = tail
                .strip_suffix(".json")
                .and_then(|d| DeviceId::new(d).ok())
            {
                KeyClass::DeviceRegistry { device }
            } else if let Some(device) = tail
                .strip_suffix(".retired")
                .and_then(|d| DeviceId::new(d).ok())
            {
                KeyClass::DeviceRetired { device }
            } else {
                KeyClass::Foreign
            }
        }
        "tombstones" => match tail.strip_suffix(".json") {
            Some(h) if is_lower_hex(h, 32) => KeyClass::Tombstone {
                hash32: h.to_string(),
            },
            _ => KeyClass::Foreign,
        },
        "previews" => match tail
            .strip_suffix(".pxy.dng")
            .and_then(|h| ContentId::parse(h).ok())
        {
            Some(content_id) => KeyClass::Preview { content_id },
            None => KeyClass::Foreign,
        },
        "thumbs" => {
            let (stem, size) = if let Some(s) = tail.strip_suffix("_small.jpg") {
                (s, ThumbSize::Small)
            } else if let Some(s) = tail.strip_suffix("_medium.jpg") {
                (s, ThumbSize::Medium)
            } else {
                return KeyClass::Foreign;
            };
            match ContentId::parse(stem) {
                Ok(content_id) => KeyClass::Thumb { content_id, size },
                Err(_) => KeyClass::Foreign,
            }
        }
        "thumbpacks" => match tail.strip_suffix(".tar") {
            Some(h) if is_lower_hex(h, 16) => KeyClass::Thumbpack {
                hash16: h.to_string(),
            },
            _ => KeyClass::Foreign,
        },
        "meta" => match tail {
            "albums.json" => KeyClass::MetaAlbums,
            "presets.json" => KeyClass::MetaPresets,
            _ => KeyClass::Foreign,
        },
        _ => KeyClass::Foreign,
    }
}
