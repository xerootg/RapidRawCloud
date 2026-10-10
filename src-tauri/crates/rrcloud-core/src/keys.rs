//! Library-relative key mapping and the bucket key schema
//! (architecture §1.1–§1.2).
//!
//! The cloud namespace is **library-relative**: a [`RelKey`] is a path
//! relative to the sync root, `/`-separated, Unicode NFC-normalized, with no
//! leading slash and no `.`/`..` segments, backslashes, colons, or control
//! characters. All bucket keys are built from typed constructors in this
//! module, and [`classify_key`] parses any bucket key back into its schema
//! role — the reconcile loop (§2.3) depends on that being an exact inverse,
//! which is why a library key whose relpath is not already NFC classifies
//! as [`KeyClass::Foreign`] rather than being silently normalized.

use std::fmt;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::clock::DeviceId;
use crate::hexutil::is_lower_hex;
use crate::semhash::ContentId;

/// Prefix of the byte-faithful mirror of the on-disk library tree (§1.2).
pub const LIBRARY_PREFIX: &str = rrcloud_proto::LIBRARY_PREFIX;

/// Prefix of the control plane (§1.2). Dot-prefixed so `scan_dir_lazy`
/// ignores it if the bucket is ever FUSE-mounted into a library.
pub const CONTROL_PREFIX: &str = rrcloud_proto::CONTROL_PREFIX;

/// Key of the relativized albums document (§2.9).
pub const ALBUMS_META_KEY: &str = rrcloud_proto::ALBUMS_META_KEY;

/// Key of the relativized presets document (§2.9).
pub const PRESETS_META_KEY: &str = rrcloud_proto::PRESETS_META_KEY;

/// Segment prefix of the engine's reserved local temp namespace. The
/// §3.5 download engine streams into `.rr.part-<name>` next to the final
/// file (`crate::transfer::partial_path`), so any relkey segment starting
/// with `.rr.` is rejected ([`KeyError::EngineReserved`]) — the whole
/// prefix, not just `.rr.part-`, so future engine temp names never
/// re-open the hole. This is what makes an engine temp path
/// non-expressible as a relkey (see the variant doc for the collision it
/// prevents).
pub const ENGINE_TEMP_PREFIX: &str = rrcloud_proto::ENGINE_TEMP_PREFIX;

/// Error from relkey mapping or key construction.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KeyError {
    /// Empty path, or a path that resolves to the sync root itself.
    #[error("empty relkey")]
    Empty,
    /// A backslash — never a valid separator in the cloud namespace.
    #[error("backslash in relkey: {0:?}")]
    Backslash(String),
    /// A colon — illegal in Windows filenames, and a `C:`-style segment
    /// pushed onto a `PathBuf` on Windows *replaces* the accumulated path
    /// (drive-relative), which would let a remote-controlled key escape
    /// the sync root in [`local_path`].
    #[error("colon in relkey: {0:?}")]
    Colon(String),
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
    /// A segment ending in `.` or ` `. Win32 strips trailing dots and
    /// spaces at file-create time, so the distinct bucket keys
    /// `library/a.jpg` and `library/a.jpg.` would collide onto one local
    /// file on a Windows receiver — a silent cross-key clobber that the
    /// engine's blake3 verification would then misreport as corruption.
    #[error("segment ends with dot or space: {0:?}")]
    TrailingDotOrSpace(String),
    /// A segment whose base name is a Win32 reserved device name
    /// (`CON`, `PRN`, `AUX`, `NUL`, `COM1`–`COM9`, `LPT1`–`LPT9`, plus
    /// the superscript variants `COM¹`–`COM³`/`LPT¹`–`LPT³`, any ASCII
    /// case, with or without an extension). Win32 resolves these in
    /// *any* directory to the device itself, so hydrating such a key on a
    /// Windows receiver would write to a device or fail the item.
    #[error("Windows-reserved device name segment: {0:?}")]
    WindowsReserved(String),
    /// A segment beginning with [`ENGINE_TEMP_PREFIX`] (`.rr.`) — the
    /// engine's own reserved temp namespace. The §3.5 download engine
    /// streams into `<dir>/.rr.part-<name>` next to the final file, so a
    /// library file literally named `.rr.part-foo.NEF` would make one
    /// relkey's *final* path another relkey's *partial* path: downloading
    /// `dir/foo.NEF` would adopt `dir/.rr.part-foo.NEF`'s installed,
    /// verified bytes as its own surviving partial, fail the blake3
    /// backstop, and the scratch retry would delete the sibling's file
    /// while its record still read `hydrated` (review finding, round 3).
    /// Rejected by the same standard as [`KeyError::WindowsReserved`]:
    /// distinct bucket keys must never silently collide onto one local
    /// file.
    #[error("engine-reserved temp-namespace segment (`.rr.` prefix): {0:?}")]
    EngineReserved(String),
    /// A virtual-copy suffix that is not exactly 6 lowercase hex chars.
    #[error("invalid virtual-copy suffix: {0:?}")]
    BadVcSuffix(String),
    /// Wire input ([`RelKey::parse_wire`], serde) that is not already NFC.
    /// The wire lane never normalizes: a non-NFC relkey inside a decoded
    /// document (tombstone §2.7, manifest deleted-set row §2.3) names a
    /// *different* bucket object than its NFC spelling, and silently
    /// rewriting it would re-aim the record — e.g. a deletion — at the
    /// user's distinct NFC object. Fail closed instead, the same stance
    /// [`classify_key`] takes for non-NFC library keys.
    #[error("relkey is not NFC-normalized: {0:?}")]
    NotNfc(String),
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
    /// This is the **local path-mapping lane** ([`relkey`] from on-disk
    /// paths), where macOS NFD filenames legitimately need normalization.
    /// Text arriving off the wire goes through [`RelKey::parse_wire`]
    /// instead, which rejects non-NFC input rather than rewriting it.
    ///
    /// Rejects empty strings, leading `/`, backslashes, colons, control
    /// characters, `.`/`..`/empty segments, segments ending in a dot or
    /// space, Win32 reserved device names — the full set of segment
    /// shapes that are ambiguous or hazardous on a Windows receiver — and
    /// segments in the engine's own reserved temp namespace
    /// ([`ENGINE_TEMP_PREFIX`], which would collide a relkey's final path
    /// with a sibling's `.rr.part` partial) (§1.1; each rejection's
    /// rationale is on its [`KeyError`] variant). Composed
    /// and decomposed spellings of the same Unicode text normalize to the
    /// same [`RelKey`].
    ///
    /// Interop consequence (documented §1.1 limitation): files whose names
    /// hit any of these rules are creatable on Linux/macOS libraries but
    /// can never sync — the mapping layer errors, and the corresponding
    /// bucket keys classify as [`KeyClass::Foreign`] rather than reaching
    /// [`local_path`] on any platform.
    pub fn new(s: impl Into<String>) -> Result<Self, KeyError> {
        use unicode_normalization::UnicodeNormalization;
        let raw = s.into();
        if raw.contains('\\') {
            return Err(KeyError::Backslash(raw));
        }
        if raw.contains(':') {
            return Err(KeyError::Colon(raw));
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
        for seg in s.split('/') {
            if seg.is_empty() || seg == "." || seg == ".." {
                return Err(KeyError::BadSegment(s));
            }
            if seg.ends_with('.') || seg.ends_with(' ') {
                return Err(KeyError::TrailingDotOrSpace(s));
            }
            if seg.starts_with(ENGINE_TEMP_PREFIX) {
                return Err(KeyError::EngineReserved(s));
            }
            if is_windows_reserved(seg) {
                return Err(KeyError::WindowsReserved(s));
            }
        }
        Ok(RelKey(s))
    }

    /// Validates a **wire-format** relkey without normalizing: input that
    /// is not already NFC is rejected with [`KeyError::NotNfc`], then the
    /// full [`RelKey::new`] rule set applies (on already-NFC input the
    /// normalization inside is the identity).
    ///
    /// This is the decode lane for relkeys arriving inside documents —
    /// [`crate::journal::Tombstone::relkey`] (§2.7) and the manifest
    /// deleted-set rows (§2.3) — and is what `Deserialize` /
    /// `TryFrom<String>` use (review finding, round 2). Normalizing here
    /// would be validation-by-rewriting: an NFD relkey names a distinct
    /// bucket object, and a deletion record silently re-aimed at the NFC
    /// spelling would hide/GC the wrong object. [`RelKey::new`] remains
    /// the normalizing constructor for the local path-mapping lane.
    pub fn parse_wire(s: impl Into<String>) -> Result<Self, KeyError> {
        let raw = s.into();
        if !unicode_normalization::is_nfc(&raw) {
            return Err(KeyError::NotNfc(raw));
        }
        Self::new(raw)
    }

    /// The normalized relative path, `/`-separated.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// `true` when `seg`'s base name — the part before the first `.`, with any
/// trailing spaces stripped, matching Win32's own name parsing — is a
/// reserved device name: `CON`, `PRN`, `AUX`, `NUL`, `COM1`–`COM9`,
/// `LPT1`–`LPT9`, in any ASCII case, plus the Latin-1 superscript variants
/// `COM¹`/`COM²`/`COM³` and `LPT¹`/`LPT²`/`LPT³` (U+00B9/U+00B2/U+00B3) —
/// Win32's reserved-name parser treats the superscript digits as digits,
/// and Microsoft's file-naming documentation lists them alongside
/// `COM1`–`COM9` (review finding, round 2; NFC does not decompose them,
/// so they survive relkey normalization). Win32 resolves these, with or
/// without an extension, in any directory, to the device itself.
fn is_windows_reserved(seg: &str) -> bool {
    let base = seg.split('.').next().unwrap_or(seg).trim_end_matches(' ');
    let bytes = base.as_bytes();
    if bytes.len() == 3 {
        return [&b"con"[..], b"prn", b"aux", b"nul"]
            .iter()
            .any(|r| bytes.eq_ignore_ascii_case(r));
    }
    // `com`/`lpt` followed by exactly one digit character: ASCII `1`–`9`
    // or superscript `¹`/`²`/`³`. (`COM0`/`LPT0` are not reserved, nor is
    // U+2074 ⁴ — only ¹ ² ³ exist in Latin-1.) The prefix match is pure
    // ASCII, so index 3 is always a char boundary.
    if bytes.len() > 3
        && (bytes[..3].eq_ignore_ascii_case(b"com") || bytes[..3].eq_ignore_ascii_case(b"lpt"))
    {
        let mut rest = base[3..].chars();
        if let (Some(c), None) = (rest.next(), rest.next()) {
            return matches!(c, '1'..='9' | '\u{b9}' | '\u{b2}' | '\u{b3}');
        }
    }
    false
}

impl TryFrom<String> for RelKey {
    type Error = KeyError;

    /// The serde decode path: strict wire parsing via
    /// [`RelKey::parse_wire`] — non-NFC input is an error, never
    /// normalized.
    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::parse_wire(s)
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
///
/// Every pushed segment is a plain relative path component on both Unix and
/// Windows: [`RelKey`] validation rejects `/`-in-segment (by construction),
/// backslashes, colons (so no `C:`-style drive-relative segment can make
/// `PathBuf::push` discard the root), and dot segments.
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

/// `.rrcloud/v1/thumbpacks/<blake3(folder relkey)[..32]>.tar` — a per-folder
/// pack of `_small` thumbs (§4.3). `folder` is the folder's relkey.
///
/// The prefix is 128 bits, same as [`tombstone_key`] (review finding, round
/// 1: the original 64-bit truncation was adversarially collidable at ~2^32
/// work and birthday-weak; a collision only degrades to per-thumb GETs with
/// wrong-thumb transients, but the wider prefix costs nothing while the
/// schema is still open).
pub fn thumbpack_key(folder: &RelKey) -> String {
    let hex = blake3::hash(folder.as_str().as_bytes()).to_hex();
    format!("{CONTROL_PREFIX}thumbpacks/{}.tar", &hex.as_str()[..32])
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
    /// `library/<relpath>.xmp` (any ASCII case of the extension; upstream
    /// writes and probes both `.xmp` and `.XMP`) — an interop XMP
    /// projection (§2.8). The relkey includes the extension as spelled
    /// (it is a real library file).
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
        /// The segment format version from the filename. The apply loop's
        /// min-reader gate (§2.2) runs on this *before* GET+decode: a
        /// version the reader does not support must halt feed application
        /// and surface "app update required", never read as corruption.
        version: u32,
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
    /// `.rrcloud/v1/thumbpacks/<hash32>.tar`.
    Thumbpack {
        /// First 32 lowercase hex chars of `blake3(folder relkey)`.
        hash32: String,
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
/// of every key constructor in this module — up to the two documented
/// ambiguities below; anything unrecognized — or recognized in shape but
/// malformed in content — is [`KeyClass::Foreign`].
///
/// Two deliberate ambiguities, both inherent to the suffix-based schema and
/// consistent with upstream's recognizers, which the reconcile loop (§2.3)
/// must inherit as part of the contract:
///
/// 1. Upstream's 6-hex virtual-copy recognizer: a file literally named
///    `<stem>.<6hex>` would have its primary sidecar classified as a
///    virtual copy of `<stem>`. Matches `list_images_in_dir` behavior.
/// 2. `.rrdata`-named originals are shadowed: a library file literally
///    named `foo.rrdata` has the same bucket key as the sidecar of `foo`,
///    so `classify_key(library_key("foo.rrdata"))` is
///    `Sidecar { relkey: "foo", vc: None }`, not `Original`. Upstream
///    already treats every `*.rrdata` as a sidecar, so this matches local
///    behavior; such a file simply cannot be synced *as an original*.
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
///
/// A relpath that is not already NFC is [`KeyClass::Foreign`]: rclone-synced
/// buckets can legitimately hold NFD keys (macOS filenames, §1.2 interop),
/// and silently normalizing here would conflate distinct bucket objects into
/// one relkey and break the classify→constructor round trip — the engine
/// would GET/PUT/tombstone the NFC spelling while the object lives at the
/// NFD key. Adopt-as-foreign is the safe lane (§2.3).
fn classify_library_key(rest: &str) -> KeyClass {
    if !unicode_normalization::is_nfc(rest) {
        return KeyClass::Foreign;
    }
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
        Ok(relkey) if has_xmp_extension(relkey.as_str()) => KeyClass::Xmp { relkey },
        Ok(relkey) => KeyClass::Original { relkey },
        Err(_) => KeyClass::Foreign,
    }
}

/// `true` when the path's final extension is `xmp` in any ASCII case.
/// Upstream explicitly probes both `with_extension("xmp")` and
/// `with_extension("XMP")` (`file_management.rs`), so uppercase `.XMP`
/// interop files exist in real libraries and must get the §2.8 projection
/// semantics.
fn has_xmp_extension(path: &str) -> bool {
    path.rsplit_once('.')
        .is_some_and(|(_, ext)| ext.eq_ignore_ascii_case("xmp"))
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
                version: parsed.version,
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
            Some(h) if is_lower_hex(h, 32) => KeyClass::Thumbpack {
                hash32: h.to_string(),
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
