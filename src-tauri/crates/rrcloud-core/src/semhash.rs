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
        if is_lower_hex64(&s) {
            Ok(SemHash(s))
        } else {
            Err(SemHashError::InvalidHash(s))
        }
    }

    /// The lowercase hex form.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// `true` when `s` is exactly 64 lowercase hex characters.
fn is_lower_hex64(s: &str) -> bool {
    crate::hexutil::is_lower_hex(s, 64)
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
        ContentId(blake3::hash(bytes).to_hex().to_string())
    }

    /// Validates `s` as 64 lowercase hex characters and wraps it.
    pub fn parse(s: impl Into<String>) -> Result<Self, SemHashError> {
        let s = s.into();
        if is_lower_hex64(&s) {
            Ok(ContentId(s))
        } else {
            Err(SemHashError::InvalidHash(s))
        }
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
    let mut out = String::new();
    write_canonical(value, &mut out);
    out
}

/// Recursive canonical writer: sorted object keys (via `BTreeMap`), no
/// whitespace, arrays in order, serde_json's own number formatting.
fn write_canonical(value: &serde_json::Value, out: &mut String) {
    use serde_json::Value;
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(true) => out.push_str("true"),
        Value::Bool(false) => out.push_str("false"),
        // serde_json::Number's Display is exactly its JSON representation.
        Value::Number(n) => out.push_str(&n.to_string()),
        Value::String(s) => write_json_string(s, out),
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_canonical(item, out);
            }
            out.push(']');
        }
        Value::Object(map) => {
            // Re-sort through a BTreeMap so the output never depends on the
            // backing store of serde_json::Map (preserve_order or not).
            let sorted: std::collections::BTreeMap<&String, &Value> = map.iter().collect();
            out.push('{');
            for (i, (k, v)) in sorted.into_iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_json_string(k, out);
                out.push(':');
                write_canonical(v, out);
            }
            out.push('}');
        }
    }
}

/// Writes `s` as a JSON string literal (serde_json-compatible escaping:
/// only `"`, `\`, and control characters are escaped).
fn write_json_string(s: &str, out: &mut String) {
    use std::fmt::Write as _;
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                // write! into a String cannot fail; ignore the Ok(()).
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// Computes the semantic hash of a sidecar document (§2.5).
///
/// Parses `sidecar_json_bytes`, projects `{rating, tags: sorted,
/// adjustments'}` (where `adjustments'` strips `lutPath` and normalizes
/// `null`; `exif` and `version` are excluded), canonicalizes, and hashes
/// with blake3.
///
/// The equivalence classes are **closed**: a `null` adjustments value, a
/// missing one, `null`-valued members inside it, an empty `{}` — including
/// one left empty after stripping `lutPath` and `null` members — are all
/// equivalent to an absent `adjustments`; likewise `tags: []`, `tags: null`
/// and a missing `tags`. A writer that spells "no edits" any of these ways
/// must produce the same hash, or the spurious-dirty churn §2.5 kills comes
/// back as spurious version bumps and junk conflict copies.
///
/// # Errors
///
/// Invalid JSON or a non-object root is an error — never a default.
pub fn sem_hash(sidecar_json_bytes: &[u8]) -> Result<SemHash, SemHashError> {
    use serde_json::{Map, Value};

    let doc: Value = serde_json::from_slice(sidecar_json_bytes)?;
    let Value::Object(root) = doc else {
        return Err(SemHashError::NotAnObject);
    };

    let mut projection = Map::new();

    if let Some(rating) = root.get("rating") {
        if !rating.is_null() {
            projection.insert("rating".to_string(), rating.clone());
        }
    }

    if let Some(tags) = root.get("tags") {
        match tags {
            Value::Null => {}
            // An empty tags array is the same statement as no tags field.
            Value::Array(items) if items.is_empty() => {}
            Value::Array(items) => {
                // Order-insensitive: sort elements by their canonical form.
                let mut sorted = items.clone();
                sorted.sort_by_cached_key(canonical_json);
                projection.insert("tags".to_string(), Value::Array(sorted));
            }
            other => {
                projection.insert("tags".to_string(), other.clone());
            }
        }
    }

    if let Some(adjustments) = root.get("adjustments") {
        if !adjustments.is_null() {
            let mut adj = adjustments.clone();
            if let Value::Object(map) = &mut adj {
                // lutPath is machine-local and never synced (§1.2).
                map.remove("lutPath");
            }
            strip_null_members(&mut adj);
            // A semantically empty residue ({} — possibly left over after
            // stripping lutPath/null members) equals absent adjustments.
            let empty_object = adj.as_object().is_some_and(serde_json::Map::is_empty);
            if !empty_object {
                projection.insert("adjustments".to_string(), adj);
            }
        }
    }

    let canonical = canonical_json(&serde_json::Value::Object(projection));
    let hex = blake3::hash(canonical.as_bytes()).to_hex().to_string();
    Ok(SemHash(hex))
}

/// Removes `null`-valued object members, recursively (a `null` member and an
/// absent member are the same edit). `null` *array elements* are positional
/// content and are kept.
fn strip_null_members(value: &mut serde_json::Value) {
    use serde_json::Value;
    match value {
        Value::Object(map) => {
            map.retain(|_, v| !v.is_null());
            for v in map.values_mut() {
                strip_null_members(v);
            }
        }
        Value::Array(items) => {
            for v in items.iter_mut() {
                strip_null_members(v);
            }
        }
        _ => {}
    }
}
