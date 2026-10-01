//! Journal entry schema (v1), NDJSON segment encode/decode, segment
//! filenames, and the tombstone document (architecture §2.2, §2.7).
//!
//! **Types only** in this unit: publishing, cursors, and replay land with
//! the engine. Everything here is pure — bytes in, typed values out.
//!
//! Versioning follows the **min-reader rule** (§2.2): a v1 reader may
//! ignore unknown *optional fields* inside a v1 entry (plain serde
//! behavior), but an entry or segment whose `"v"` it does not support is a
//! typed [`JournalError::UnsupportedVersion`] — fail closed, never
//! skip-and-diverge.

use serde::{Deserialize, Serialize};

use crate::clock::{DeviceId, VersionVector};
use crate::keys::RelKey;
use crate::semhash::{ContentId, SemHash};

/// The journal entry/segment format version this reader+writer supports.
pub const JOURNAL_VERSION: u32 = 1;

/// Maximum entries per segment (§2.2).
pub const SEGMENT_MAX_ENTRIES: usize = 1000;

/// Maximum encoded segment size in bytes (§2.2: 1 MiB).
pub const SEGMENT_MAX_BYTES: usize = 1024 * 1024;

/// Error from journal entry/segment encoding or decoding.
#[derive(Debug, thiserror::Error)]
pub enum JournalError {
    /// An entry or segment carries a format version this reader does not
    /// support. The caller must halt applying the prefix and surface
    /// "app update required" (§2.2) — never skip.
    #[error("unsupported journal format version {version}")]
    UnsupportedVersion {
        /// The version the entry/segment declared.
        version: u64,
    },
    /// An entry has no `"v"` field at all (equally unreadable: fail closed).
    #[error("journal entry has no \"v\" field")]
    MissingVersion,
    /// An entry's `"v"` field is present but not an unsigned integer (e.g.
    /// `"2"`, `2.5`, `-1`). Distinct from [`JournalError::MissingVersion`]
    /// so a sloppy writer's stringified version surfaces truthfully rather
    /// than as "no v field" (which reads as corruption). Fail closed.
    #[error("journal entry \"v\" field is not an unsigned integer: {value}")]
    MalformedVersion {
        /// The JSON spelling of the offending `"v"` value.
        value: String,
    },
    /// Malformed JSON, or JSON that does not match the v1 entry schema.
    #[error("invalid journal JSON: {0}")]
    Json(#[from] serde_json::Error),
    /// More than [`SEGMENT_MAX_ENTRIES`] entries handed to the encoder.
    #[error("segment has {count} entries, cap is {SEGMENT_MAX_ENTRIES}")]
    TooManyEntries {
        /// How many entries were handed in.
        count: usize,
    },
    /// The encoded segment would exceed [`SEGMENT_MAX_BYTES`].
    #[error("encoded segment is {size} bytes, cap is {SEGMENT_MAX_BYTES}")]
    SegmentTooLarge {
        /// The encoded size that broke the cap.
        size: usize,
    },
    /// A segment filename that does not match `<seq:016x>.v<N>.ndjson`.
    #[error("invalid journal segment filename: {name:?}")]
    BadSegmentFilename {
        /// The offending filename.
        name: String,
    },
}

/// Journal operation (§2.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Op {
    /// A new version of a key was uploaded.
    Put,
    /// The key was deleted (paired with a tombstone, §2.7).
    Del,
    /// The key was moved/renamed (`from_key` carries the old key).
    Move,
    /// A device fully downloaded an advertised object and verified its
    /// blake3 (gates LRU eviction, §3.5).
    Attest,
}

/// What kind of object an entry describes (§2.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    /// A RAW/JPEG/derived original under `library/`.
    Original,
    /// A `.rrdata` sidecar.
    Sidecar,
    /// An interop `.xmp` projection.
    Xmp,
    /// A smart preview.
    Preview,
    /// A thumb.
    Thumb,
    /// A per-folder thumb pack.
    Thumbpack,
    /// The albums meta document.
    Albums,
    /// The presets meta document.
    Presets,
}

/// One journal entry, v1 schema (§2.2).
///
/// Required fields are those present on every entry; everything the schema
/// marks per-kind is `Option`. Unknown *optional* fields in incoming v1
/// JSON are ignored (min-reader rule); an unknown `"v"` is a typed error
/// from [`JournalEntry::from_json_line`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalEntry {
    /// Format version; always [`JOURNAL_VERSION`] for entries this writer
    /// produces.
    pub v: u32,
    /// Per-device monotonic sequence number.
    pub seq: u64,
    /// Wall-clock unix seconds at entry creation (used only by the §2.6
    /// concurrent-winner rule and GC age heuristics).
    pub ts: i64,
    /// Authoring device.
    pub device: DeviceId,
    /// Operation.
    pub op: Op,
    /// Object kind.
    pub kind: Kind,
    /// Full bucket key the entry is about.
    pub key: String,
    /// Per-relkey version vector snapshot (§2.6). Required on **every**
    /// entry, including `attest` (where it snapshots the version whose
    /// bytes the attesting device verified) — §2.2 defines a single v1
    /// envelope, never a reduced per-op shape.
    pub vv: VersionVector,
    /// Uploaded object size in bytes (`put` entries).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    /// Lowercase hex blake3 of the uploaded/verified bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blake3: Option<String>,
    /// Semantic hash (`sidecar` entries, §2.5).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sem_hash: Option<SemHash>,
    /// Star rating (`sidecar` entries; lets the grid badge before the
    /// sidecar bytes download, §3.5).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rating: Option<u8>,
    /// Color label (`sidecar` entries, same purpose as `rating`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub color_label: Option<String>,
    /// Content identity (`original` entries).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_id: Option<ContentId>,
    /// Final displayed width (`original` entries; measured, never
    /// EXIF-derived — §2.2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub w: Option<u32>,
    /// Final displayed height (`original` entries).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub h: Option<u32>,
    /// Local file mtime, unix seconds (`original` entries).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mtime: Option<i64>,
    /// Previous bucket key (`move` entries).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_key: Option<String>,
}

impl JournalEntry {
    /// Parses one NDJSON line into an entry.
    ///
    /// Checks `"v"` **before** schema decoding so an unreadable future
    /// entry is [`JournalError::UnsupportedVersion`] (carrying the version)
    /// rather than an opaque schema error. Unknown optional fields within a
    /// supported version are ignored.
    pub fn from_json_line(line: &str) -> Result<Self, JournalError> {
        Self::from_json_slice(line.as_bytes())
    }

    /// [`JournalEntry::from_json_line`] over raw bytes (what
    /// [`decode_segment`] feeds it; invalid UTF-8 surfaces as a JSON error).
    fn from_json_slice(bytes: &[u8]) -> Result<Self, JournalError> {
        let value: serde_json::Value = serde_json::from_slice(bytes)?;
        let version = match value.get("v") {
            None => return Err(JournalError::MissingVersion),
            Some(v) => v.as_u64().ok_or_else(|| JournalError::MalformedVersion {
                value: v.to_string(),
            })?,
        };
        if version != u64::from(JOURNAL_VERSION) {
            return Err(JournalError::UnsupportedVersion { version });
        }
        Ok(serde_json::from_value(value)?)
    }

    /// Serializes the entry as one JSON line (no trailing newline).
    ///
    /// Deterministic: the same entry always encodes to the same bytes
    /// (fixed field order, sorted `vv` map, `None` fields omitted) —
    /// required so a crash-replayed segment is byte-identical (§2.1.5).
    pub fn to_json_line(&self) -> Result<String, JournalError> {
        Ok(serde_json::to_string(self)?)
    }
}

/// Encodes entries as an NDJSON segment (one [`JournalEntry::to_json_line`]
/// line per entry, each newline-terminated), enforcing the §2.2 caps:
/// at most [`SEGMENT_MAX_ENTRIES`] entries and [`SEGMENT_MAX_BYTES`] bytes.
/// Deterministic for the same entries.
pub fn encode_segment(entries: &[JournalEntry]) -> Result<Vec<u8>, JournalError> {
    if entries.len() > SEGMENT_MAX_ENTRIES {
        return Err(JournalError::TooManyEntries {
            count: entries.len(),
        });
    }
    let mut out = Vec::new();
    for entry in entries {
        out.extend_from_slice(entry.to_json_line()?.as_bytes());
        out.push(b'\n');
    }
    if out.len() > SEGMENT_MAX_BYTES {
        return Err(JournalError::SegmentTooLarge { size: out.len() });
    }
    Ok(out)
}

/// Decodes an NDJSON segment. Fail-closed: the first entry with an
/// unsupported `"v"` aborts the whole decode with
/// [`JournalError::UnsupportedVersion`] — no partial results, so a reader
/// can never apply half a segment it only partly understands.
///
/// The §2.2 caps are enforced on the read side too: no conforming v1
/// writer produces a segment over [`SEGMENT_MAX_BYTES`] or
/// [`SEGMENT_MAX_ENTRIES`], so a larger one is malformed, and rejecting it
/// up front bounds reader-side allocation against corrupt or hostile
/// segments.
pub fn decode_segment(bytes: &[u8]) -> Result<Vec<JournalEntry>, JournalError> {
    if bytes.len() > SEGMENT_MAX_BYTES {
        return Err(JournalError::SegmentTooLarge { size: bytes.len() });
    }
    let mut entries = Vec::new();
    for line in bytes.split(|&b| b == b'\n') {
        if line.is_empty() {
            continue;
        }
        if entries.len() == SEGMENT_MAX_ENTRIES {
            return Err(JournalError::TooManyEntries {
                count: entries.len() + 1,
            });
        }
        entries.push(JournalEntry::from_json_slice(line)?);
    }
    Ok(entries)
}

/// A parsed segment filename: `<seq:016x>.v<version>.ndjson` (§2.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentFilename {
    /// Sequence number of the segment's first entry.
    pub seq: u64,
    /// Format version carried in the filename. Parsing does not gate on it
    /// — the min-reader gate belongs to the apply loop, which must *halt*
    /// on a version it cannot read, not misfile the segment.
    pub version: u32,
}

/// Formats the filename for a v1 segment starting at `seq`:
/// `<seq:016x>.v1.ndjson`. Zero-padded 16 lowercase hex, so lexicographic
/// filename order equals numeric `seq` order.
pub fn format_segment_filename(seq: u64) -> String {
    format!("{seq:016x}.v{JOURNAL_VERSION}.ndjson")
}

/// Parses a segment filename (the final path component, not the full key).
/// Strict: exactly 16 lowercase hex chars, `.v<digits>.ndjson`, nothing
/// else.
pub fn parse_segment_filename(name: &str) -> Result<SegmentFilename, JournalError> {
    let bad = || JournalError::BadSegmentFilename {
        name: name.to_string(),
    };
    let stem = name.strip_suffix(".ndjson").ok_or_else(bad)?;
    let (hex, ver) = stem.split_once('.').ok_or_else(bad)?;
    if !crate::hexutil::is_lower_hex(hex, 16) {
        return Err(bad());
    }
    let seq = u64::from_str_radix(hex, 16).map_err(|_| bad())?;
    let digits = ver.strip_prefix('v').ok_or_else(bad)?;
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(bad());
    }
    let version: u32 = digits.parse().map_err(|_| bad())?;
    Ok(SegmentFilename { seq, version })
}

/// A tombstone document (§2.7), stored at
/// `.rrcloud/v1/tombstones/<blake3(relkey)[..32]>.json`.
///
/// Idempotent/commutative under concurrent write: any device deleting the
/// same relkey at the same version writes an equivalent document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tombstone {
    /// The deleted library-relative key.
    pub relkey: RelKey,
    /// The deletion's version vector (bumped past the deleted version, so
    /// delete-vs-edit resolves through the ordinary §2.6 machinery).
    pub vv: VersionVector,
    /// The deleting device.
    pub device: DeviceId,
    /// Server time of the deletion (from the S3 `Date` header) — all GC
    /// age rules run on server time (§2.10).
    pub server_ts: i64,
    /// Which kinds existed for the relkey and are covered by this
    /// tombstone (e.g. `original`, `sidecar`, `xmp`).
    pub kinds: Vec<Kind>,
}
