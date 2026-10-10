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

use std::path::{Path, PathBuf};

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
    /// The text is not a relkey (§1.1): every segment-shape rule and its
    /// rationale is on the SDK's [`RelKeyError`] variants.
    #[error(transparent)]
    RelKey(#[from] RelKeyError),
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

/// The validated library-relative key (§1.1) and the thumb flavour are the
/// generated `rrcloud-proto` SDK's types.
///
/// [`RelKey`] has two constructors with deliberately different lanes:
///
/// - [`RelKey::new`] is the **local path-mapping lane** ([`relkey`] from
///   on-disk paths): it NFC-normalizes first (macOS NFD filenames
///   legitimately need it), then validates.
/// - [`RelKey::parse_wire`] is the **strict wire lane** — what
///   `Deserialize` uses for [`crate::journal::Tombstone::relkey`] and the
///   manifest rows: non-NFC input is [`RelKeyError::NotNfc`], never
///   rewritten, because an NFD relkey names a *distinct* bucket object and
///   a deletion record silently re-aimed at the NFC spelling would
///   hide/GC the wrong object.
///
/// Interop consequence (documented §1.1 limitation): files whose names hit
/// any rule are creatable on Linux/macOS libraries but can never sync —
/// the mapping layer errors, and the corresponding bucket keys classify as
/// [`KeyClass::Foreign`] rather than reaching [`local_path`].
pub use rrcloud_proto::{RelKey, RelKeyError, ThumbSize};

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
    Ok(RelKey::new(joined)?)
}

/// Joins a [`RelKey`] back onto the local `sync_root` (§1.1 reverse
/// mapping). Round-trips with [`relkey`] for valid, NFC-normalized paths.
///
/// Every pushed segment is a plain relative path component on both Unix and
/// Windows: [`RelKey`] validation rejects `/`-in-segment (by construction),
/// backslashes, colons (so no `C:`-style drive-relative segment can make
/// `PathBuf::push` discard the root), dot segments, and trailing dots or
/// spaces and Win32 device names (which would collide distinct keys onto
/// one file on a Windows receiver).
pub fn local_path(rel: &RelKey, sync_root: &Path) -> PathBuf {
    let mut p = sync_root.to_path_buf();
    for seg in rel.as_str().split('/') {
        p.push(seg);
    }
    p
}

/// `library/<relpath>` — an original (or `.xmp`) byte-identical to local.
pub fn library_key(rel: &RelKey) -> String {
    rrcloud_proto::key_library_original(rel)
}

/// `library/<relpath>.rrdata` — the primary sidecar.
pub fn sidecar_key(rel: &RelKey) -> String {
    rrcloud_proto::key_sidecar(rel)
}

/// `library/<relpath>.<6hex>.rrdata` — a virtual-copy sidecar (including
/// deterministic conflict losers, §2.6). `vc6` must be exactly 6 lowercase
/// hex characters.
pub fn vc_sidecar_key(rel: &RelKey, vc6: &str) -> Result<String, KeyError> {
    if !is_lower_hex(vc6, 6) {
        return Err(KeyError::BadVcSuffix(vc6.to_string()));
    }
    Ok(rrcloud_proto::key_vc_sidecar(rel, vc6))
}

/// `.rrcloud/v1/journal/<device>/<seq:016x>.v1.ndjson` — a journal segment
/// (§2.2). The filename part is [`crate::journal::format_segment_filename`].
pub fn journal_segment_key(device: &DeviceId, seq: u64) -> String {
    rrcloud_proto::key_journal_segment(device, seq)
}

/// `.rrcloud/v1/manifests/<device>.json.gz` — the per-writer manifest (§2.3).
pub fn manifest_key(device: &DeviceId) -> String {
    rrcloud_proto::key_manifest(device)
}

/// `.rrcloud/v1/devices/<device>.json` — the device registry entry (§1.2).
pub fn device_registry_key(device: &DeviceId) -> String {
    rrcloud_proto::key_device_registry(device)
}

/// `.rrcloud/v1/devices/<device>.retired` — the retirement marker (§2.10).
pub fn device_retired_key(device: &DeviceId) -> String {
    rrcloud_proto::key_device_retired(device)
}

/// `.rrcloud/v1/tombstones/<blake3(relkey)[..32]>.json` — a deletion marker
/// (§2.7). The hash prefix is the first 32 lowercase hex chars of
/// `blake3(relkey bytes)`.
pub fn tombstone_key(rel: &RelKey) -> String {
    rrcloud_proto::key_tombstone(rel)
}

/// `.rrcloud/v1/previews/<content_id>.pxy.dng` — a smart preview (§4).
pub fn preview_key(content_id: &ContentId) -> String {
    rrcloud_proto::key_preview(content_id)
}

/// `.rrcloud/v1/thumbs/<content_id>_small.jpg` / `_medium.jpg`.
pub fn thumb_key(content_id: &ContentId, size: ThumbSize) -> String {
    rrcloud_proto::key_thumb(content_id, size)
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
    rrcloud_proto::key_thumbpack(folder)
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
