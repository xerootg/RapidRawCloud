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

/// The entry, enums and tombstone are the generated `rrcloud-proto` SDK's
/// types (`protocol/rrcloud.protocol.toml`). [`JournalEntry::key`] is a
/// plain `String`, **unvalidated on decode** — deliberately, unlike
/// [`Tombstone::relkey`]: a journal feed carries keys for every schema role
/// and the engine may need to read entries about keys this build does not
/// recognize, so it **must** route `key` through
/// [`crate::keys::classify_key`] (which rejects `library/../x`, NFD text
/// and garbage as `Foreign`) before acting on it.
pub use rrcloud_proto::{JournalEntry, Kind, Op, Tombstone};

/// The journal entry/segment format version this reader+writer supports.
pub const JOURNAL_VERSION: u32 = rrcloud_proto::JOURNAL_VERSION;

/// Maximum entries per segment (§2.2).
pub const SEGMENT_MAX_ENTRIES: usize = rrcloud_proto::SEGMENT_MAX_ENTRIES;

/// Maximum encoded segment size in bytes (§2.2: 1 MiB).
pub const SEGMENT_MAX_BYTES: usize = rrcloud_proto::SEGMENT_MAX_BYTES;

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

impl From<rrcloud_proto::ProtoError> for JournalError {
    /// The SDK codec's failure, in the engine's taxonomy. The segment
    /// codec can only raise the version-gate, JSON and cap variants; the
    /// rest of `ProtoError` (gzip, manifest, scalar) is unreachable from
    /// it and folds into `Json` so the mapping stays total.
    fn from(e: rrcloud_proto::ProtoError) -> Self {
        use rrcloud_proto::ProtoError as P;
        match e {
            P::UnsupportedVersion { version } => JournalError::UnsupportedVersion { version },
            P::MissingVersion => JournalError::MissingVersion,
            P::MalformedVersion { value } => JournalError::MalformedVersion { value },
            P::Json(e) => JournalError::Json(e),
            P::TooManyEntries { count, .. } => JournalError::TooManyEntries { count },
            P::TooLarge { size, .. } => JournalError::SegmentTooLarge { size },
            other => JournalError::Json(serde::de::Error::custom(other.to_string())),
        }
    }
}

/// The engine's line codec over [`JournalEntry`] (an extension trait,
/// since the record type is the SDK's). Import it to call
/// `JournalEntry::from_json_line` / `entry.to_json_line()`.
pub trait JournalEntryExt: Sized {
    /// Parses one NDJSON line into an entry.
    ///
    /// Checks `"v"` **before** schema decoding so an unreadable future
    /// entry is [`JournalError::UnsupportedVersion`] (carrying the version)
    /// rather than an opaque schema error. Unknown optional fields within a
    /// supported version are ignored (min-reader rule).
    fn from_json_line(line: &str) -> Result<Self, JournalError>;

    /// Serializes the entry as one JSON line (no trailing newline).
    ///
    /// Deterministic: the same entry always encodes to the same bytes
    /// (fixed field order, sorted `vv` map, `None` fields omitted) —
    /// required so a crash-replayed segment is byte-identical (§2.1.5).
    fn to_json_line(&self) -> Result<String, JournalError>;
}

impl JournalEntryExt for JournalEntry {
    fn from_json_line(line: &str) -> Result<Self, JournalError> {
        Ok(JournalEntry::from_json(line)?)
    }

    fn to_json_line(&self) -> Result<String, JournalError> {
        Ok(self.to_json()?)
    }
}

/// Encodes entries as an NDJSON segment (one [`JournalEntry::to_json_line`]
/// line per entry, each newline-terminated), enforcing the §2.2 caps:
/// at most [`SEGMENT_MAX_ENTRIES`] entries and [`SEGMENT_MAX_BYTES`] bytes.
/// Deterministic for the same entries.
pub fn encode_segment(entries: &[JournalEntry]) -> Result<Vec<u8>, JournalError> {
    Ok(rrcloud_proto::encode_journal_segment(entries)?)
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
    Ok(rrcloud_proto::decode_journal_segment(bytes)?)
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
/// Strict and **canonical**: exactly 16 lowercase hex chars,
/// `.v<version>.ndjson` where the version is a decimal with no leading
/// zeros (so `v0` and `v01` are rejected — no conforming writer emits
/// either, versions start at 1, and accepting them would let two distinct
/// bucket keys alias one segment identity and break
/// `format(parse(name)) == name`, weakening [`crate::keys::classify_key`]'s
/// exact-inverse contract).
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
    // starts_with('0') rejects both leading zeros and version 0 itself: a
    // "v0" segment is a malformed name (corruption), not a parseable
    // version for the apply loop's min-reader gate to misread as "app
    // update required".
    if digits.is_empty() || digits.starts_with('0') || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(bad());
    }
    let version: u32 = digits.parse().map_err(|_| bad())?;
    Ok(SegmentFilename { seq, version })
}
