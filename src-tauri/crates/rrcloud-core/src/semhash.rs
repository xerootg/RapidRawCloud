//! Semantic hashing of sidecar documents and content hashing of file bytes
//! (architecture §2.5).
//!
//! RapidRAW rewrites sidecars for reasons that are not user edits (EXIF
//! caching, auto-heal, XMP import bookkeeping). Byte-level change detection
//! would upload on every such rewrite; instead, change detection hashes a
//! **canonical projection** of the document:
//!
//! `sem_hash = blake3(canonical_json({rating, tags: sorted, adjustments'}))`
//!
//! where `adjustments'` is `adjustments` with `lutPath` removed (LUT files
//! are not synced, §1.2) and `null` normalized, and the `exif` and `version`
//! fields are excluded entirely.
//!
//! Invalid sidecar JSON is an **error**, never a default document: the
//! upstream `load_sidecar` silently substitutes `ImageMetadata::default()`
//! on parse failure, and hashing that default would make a corrupt file look
//! like a deliberate "reset everything" edit.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Error from [`sem_hash`] or the hash-string parsers.
#[derive(Debug, thiserror::Error)]
pub enum SemHashError {
    /// The sidecar bytes are not valid JSON. Fail closed — never substitute
    /// a default document (§2.5).
    #[error("invalid sidecar JSON: {0}")]
    InvalidJson(#[from] serde_json::Error),
    /// The sidecar parsed, but its root is not a JSON object.
    #[error("sidecar root is not a JSON object")]
    NotAnObject,
    /// A hash string is not 64 lowercase hex characters.
    #[error("not a 64-char lowercase hex hash: {0:?}")]
    InvalidHash(String),
}

/// A semantic hash of a sidecar document: lowercase hex blake3 (64 chars) of
/// the canonical projection described in the module docs.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct SemHash(String);

impl SemHash {
    /// Validates `s` as 64 lowercase hex characters and wraps it.
    pub fn parse(s: impl Into<String>) -> Result<Self, SemHashError> {
        let s = s.into();
        let _ = s;
        todo!("P1-U1 green: SemHash::parse")
    }

    /// The lowercase hex form.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for SemHash {
    type Error = SemHashError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::parse(s)
    }
}

impl From<SemHash> for String {
    fn from(h: SemHash) -> String {
        h.0
    }
}

impl fmt::Display for SemHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A content identity: full lowercase hex blake3 of a file's bytes
/// (64 chars, prefix-free; §1.2). Previews and thumbs are keyed by it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ContentId(String);

impl ContentId {
    /// Hashes `bytes` with blake3 into a content id.
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let _ = bytes;
        todo!("P1-U1 green: ContentId::from_bytes")
    }

    /// Validates `s` as 64 lowercase hex characters and wraps it.
    pub fn parse(s: impl Into<String>) -> Result<Self, SemHashError> {
        let s = s.into();
        let _ = s;
        todo!("P1-U1 green: ContentId::parse")
    }

    /// The lowercase hex form.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for ContentId {
    type Error = SemHashError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::parse(s)
    }
}

impl From<ContentId> for String {
    fn from(c: ContentId) -> String {
        c.0
    }
}

impl fmt::Display for ContentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Re-serializes `value` as canonical JSON: object keys sorted (recursively,
/// byte-wise on the UTF-8 key), no whitespace, array order preserved.
///
/// The result is independent of the input's formatting and key order, and —
/// deliberately — of whether serde_json's `preserve_order` feature happens
/// to be enabled anywhere in the build (§2.5: canonicalization sorts via
/// `BTreeMap`, it does not rely on `Map`'s backing store).
pub fn canonical_json(value: &serde_json::Value) -> String {
    let _ = value;
    todo!("P1-U1 green: canonical JSON serialization")
}

/// Computes the semantic hash of a sidecar document (§2.5).
///
/// Parses `sidecar_json_bytes`, projects `{rating, tags: sorted,
/// adjustments'}` (where `adjustments'` strips `lutPath` and normalizes
/// `null` — a `null` adjustments value, a missing one, and `null`-valued
/// members inside it are all equivalent; `exif` and `version` are excluded),
/// canonicalizes, and hashes with blake3.
///
/// # Errors
///
/// Invalid JSON or a non-object root is an error — never a default.
pub fn sem_hash(sidecar_json_bytes: &[u8]) -> Result<SemHash, SemHashError> {
    let _ = sidecar_json_bytes;
    todo!("P1-U1 green: semantic hashing")
}
