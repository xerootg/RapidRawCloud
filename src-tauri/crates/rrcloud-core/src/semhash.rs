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

    /// A content id naming the same bytes as `digest` — a §1.2 content id
    /// *is* the full-file blake3, so this is a relabeling, not a rehash.
    /// Used by the §2.4 upload path, whose running hash is computed
    /// streaming (no contiguous buffer ever exists to pass to
    /// [`ContentId::from_bytes`]).
    pub fn from_blake3(digest: &Blake3Hex) -> Self {
        ContentId(digest.as_str().to_owned())
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

/// A full blake3 digest of transferred bytes: 64 lowercase hex characters.
///
/// This is the type of the journal's `blake3` field (§2.2). It is kept
/// distinct from [`ContentId`] (the identity of an *original's* bytes, used
/// to key previews/thumbs) because a `blake3` can cover any uploaded object
/// — a sidecar, a thumbpack, a manifest read-back — not just originals.
///
/// Validated on decode exactly like [`SemHash`]/[`ContentId`]: the
/// attestation gate for LRU eviction (§3.5), verify-state checks (§2.4) and
/// manifest-row merge (§2.3) all compare this field, and a malformed or
/// uppercase digest from a foreign/buggy writer must surface at decode time,
/// not as a silently never-equal string.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Blake3Hex(String);

impl Blake3Hex {
    /// Hashes `bytes` with blake3 into a digest.
    pub fn from_bytes(bytes: &[u8]) -> Self {
        Blake3Hex(blake3::hash(bytes).to_hex().to_string())
    }

    /// Wraps an already-finalized blake3 hash (infallible by construction:
    /// `to_hex` is always 64 lowercase hex characters). This is the
    /// streaming-hash path of the §2.4 transfer engine, whose running
    /// digest covers bytes that never exist in one buffer.
    pub fn from_hash(hash: &blake3::Hash) -> Self {
        Blake3Hex(hash.to_hex().to_string())
    }

    /// Validates `s` as 64 lowercase hex characters and wraps it.
    pub fn parse(s: impl Into<String>) -> Result<Self, SemHashError> {
        let s = s.into();
        if is_lower_hex64(&s) {
            Ok(Blake3Hex(s))
        } else {
            Err(SemHashError::InvalidHash(s))
        }
    }

    /// The lowercase hex form.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for Blake3Hex {
    type Error = SemHashError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::parse(s)
    }
}

impl From<Blake3Hex> for String {
    fn from(b: Blake3Hex) -> String {
        b.0
    }
}

impl fmt::Display for Blake3Hex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Re-serializes `value` as canonical JSON: object keys sorted (recursively,
/// byte-wise on the UTF-8 key), no whitespace, array order preserved,
/// numbers in canonical spelling (see [`write_canonical_number`]).
///
/// The result is independent of the input's formatting, key order, and
/// number spelling (`1` / `1.0` / `1e0` / `-0.0` all canonicalize to `1`
/// resp. `0`), and — deliberately — of whether serde_json's
/// `preserve_order` feature happens to be enabled anywhere in the build
/// (§2.5: canonicalization sorts via `BTreeMap`, it does not rely on
/// `Map`'s backing store).
pub fn canonical_json(value: &serde_json::Value) -> String {
    let mut out = String::new();
    write_canonical(value, &mut out);
    out
}

/// Recursive canonical writer: sorted object keys (via `BTreeMap`), no
/// whitespace, arrays in order, canonical number spelling.
fn write_canonical(value: &serde_json::Value, out: &mut String) {
    use serde_json::Value;
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(true) => out.push_str("true"),
        Value::Bool(false) => out.push_str("false"),
        Value::Number(n) => write_canonical_number(n, out),
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

/// Writes a JSON number in canonical spelling, RFC 8785-style for the
/// integral range (review finding, round 1): JSON has one number *value*
/// space but many spellings, and semantically identical sidecars cross the
/// JS↔Rust boundary with different ones — `JSON.stringify`/Tauri IPC spells
/// integral numbers `100` (parsed by serde_json as `u64`), while Rust `f64`
/// writers (e.g. `preset_converter.rs`'s clamped `json!` inserts) spell
/// them `100.0`. Hashing the raw spelling would make a no-edit load/save
/// round trip through the other runtime change `sem_hash` — exactly the
/// spurious-dirty/spurious-vv-bump churn §2.5 exists to kill.
///
/// Rules:
/// - `i64`/`u64` values print in their integer spelling.
/// - A finite integral `f64` that equals an `i64`/`u64` value prints in
///   that same integer spelling (`1.0`, `1e2`, `-0.0` → `1`, `100`, `0`;
///   every integral `f64` in that range is exact, so the cast is lossless).
/// - Any other `f64` keeps serde_json's shortest-round-trip (ryu)
///   spelling, which is already a pure function of the `f64` value
///   (`0.36` and `3.6e-1` parse to the same `f64` and print identically).
fn write_canonical_number(n: &serde_json::Number, out: &mut String) {
    use std::fmt::Write as _;
    if let (None, None, Some(f)) = (n.as_i64(), n.as_u64(), n.as_f64()) {
        // Bounds (review finding, round 2 — the old comment wrongly
        // claimed both ends were exclusive): 2^64 is EXCLUDED (U64_END
        // itself is not representable as u64), but -2^63 IS exactly
        // i64::MIN, so the I64_START..0.0 range below deliberately
        // INCLUDES it (start-inclusive) and the cast is exact. "Fixing"
        // the range to exclude I64_START would split that one value into
        // two spellings — the integer "-9223372036854775808" vs ryu's
        // "-9.223372036854776e18" — desynchronizing sem_hash across the
        // JS/Rust boundary at exactly i64::MIN.
        const U64_END: f64 = 18_446_744_073_709_551_616.0; // 2^64
        const I64_START: f64 = -9_223_372_036_854_775_808.0; // -2^63
        if f == 0.0 {
            // Covers -0.0: IEEE 754 negative zero compares equal to zero
            // but prints "-0.0", and it must hash like the integer 0.
            out.push('0');
            return;
        }
        if f.fract() == 0.0 {
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            if (0.0..U64_END).contains(&f) {
                // In-range integral f64 → exact u64 (write! into a String
                // cannot fail).
                let _ = write!(out, "{}", f as u64);
                return;
            } else if (I64_START..0.0).contains(&f) {
                let _ = write!(out, "{}", f as i64);
                return;
            }
        }
    }
    // Integer Numbers, and f64 values with no integer alias: serde_json's
    // Display is exactly its canonical JSON representation.
    out.push_str(&n.to_string());
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
/// The equivalence classes are **closed, recursively**: a `null`
/// adjustments value, a missing one, `null`-valued members at any depth
/// inside it, and members whose value collapses to an empty `{}` or `[]` —
/// including residue left over after stripping `lutPath` and `null` members
/// — are all equivalent to the member being absent, and an `adjustments`
/// that collapses to nothing is equivalent to an absent `adjustments`;
/// likewise `tags: []`, `tags: null` and a missing `tags`. This matters at
/// depth because writers spell "nothing here" differently: upstream's
/// frontend default adjustments spell out sections (`masks: []`,
/// `aiPatches: []`) while Rust writers use `json!({})`
/// (`file_management.rs`). A writer that spells "no edits" any of these
/// ways must produce the same hash, or the spurious-dirty churn §2.5 kills
/// comes back as spurious version bumps and junk conflict copies. Number
/// spelling is likewise normalized (see [`canonical_json`]).
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
            strip_residue(&mut adj);
            // A semantically empty residue ({} or [] — possibly left over
            // after stripping lutPath/null/empty members) equals absent
            // adjustments.
            if !is_residue(&adj) {
                projection.insert("adjustments".to_string(), adj);
            }
        }
    }

    let canonical = canonical_json(&serde_json::Value::Object(projection));
    let hex = blake3::hash(canonical.as_bytes()).to_hex().to_string();
    Ok(SemHash(hex))
}

/// Removes semantic residue from an adjustments subtree, **bottom-up**: an
/// object member whose value is `null`, or collapses (after its own
/// stripping) to an empty `{}` or `[]`, is removed — a writer spelling
/// "nothing here" as `null`, `{}`, `[]`, or by omitting the member is
/// making the same statement, and the closure must hold at every depth
/// (review finding, round 1: `{"foo":{"bar":null}}` used to leave a
/// `{"foo":{}}` residue because the emptiness check ran only at the top
/// level). `null`/empty *array elements* are positional content and are
/// kept.
fn strip_residue(value: &mut serde_json::Value) {
    use serde_json::Value;
    match value {
        Value::Object(map) => {
            for v in map.values_mut() {
                strip_residue(v);
            }
            map.retain(|_, v| !is_residue(v));
        }
        Value::Array(items) => {
            for v in items.iter_mut() {
                strip_residue(v);
            }
        }
        _ => {}
    }
}

/// `true` when `value` carries no semantic content as an object member:
/// `null`, an empty object, or an empty array.
fn is_residue(value: &serde_json::Value) -> bool {
    use serde_json::Value;
    match value {
        Value::Null => true,
        Value::Object(map) => map.is_empty(),
        Value::Array(items) => items.is_empty(),
        _ => false,
    }
}
