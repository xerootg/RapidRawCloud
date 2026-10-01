//! The per-writer manifest (architecture §2.3): gzip NDJSON snapshots of
//! one device's live rows, deleted set, and cursors, written only by the
//! owning device (single-writer, same invariant as journal prefixes).
//!
//! # Wire format
//!
//! gzip-compressed NDJSON. The **first** line is the header
//! `{"written_server_ts": .., "cursors": {device: seq}, "proto": 1}`;
//! every following line is either a live row (`"key"` field) or a
//! deleted-set row (`"del"` field). Decoding is **fail-closed and
//! header-first**: the header is parsed and its `proto` checked before
//! any row is decoded, a `proto` this build does not support is the typed
//! [`ManifestError::UnsupportedProto`] (no partial result), a first line
//! that is not a header is [`ManifestError::MissingHeader`], and any
//! malformed row aborts the decode naming its line. Relkeys inside rows
//! (`key`, `del`) take the **strict wire lane**
//! ([`crate::keys::RelKey::parse_wire`], via [`RelKey`]'s `Deserialize`):
//! a non-NFC spelling is rejected, never normalized — §1.1 wire
//! strictness, because a rewritten `del` row would re-aim a deletion
//! record at a different object.
//!
//! # Merge = the same idempotent apply (§2.3)
//!
//! [`merge`] converts every row into a synthetic journal entry and drives
//! the **same** [`crate::reader::JournalConsumer`] the journal apply loop
//! uses — live rows become `put` ops, deleted rows become `del` ops — and
//! seeds cursors from the merged headers (max per device), so
//! "bootstrap = merge then poll" and a subsequent poll skips the covered
//! segments.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::clock::{DeviceId, VersionVector};
use crate::journal::Kind;
use crate::keys::RelKey;
use crate::reader::{ConsumerError, JournalConsumer};
use crate::s3::{S3Api, S3Error};
use crate::semhash::{Blake3Hex, ContentId, SemHash};
use crate::state::{StateError, SyncDb};

/// The manifest format version this build reads and writes (the header's
/// `proto` field).
pub const MANIFEST_PROTO: u32 = 1;

/// Error from manifest build/encode/decode/transfer/merge.
#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    /// State-store failure.
    #[error(transparent)]
    State(#[from] StateError),
    /// S3 failure.
    #[error(transparent)]
    S3(#[from] S3Error),
    /// gzip compression/decompression failure (truncated or corrupt
    /// stream).
    #[error("manifest gzip stream error: {0}")]
    Gzip(std::io::Error),
    /// The manifest has no header line (empty document, or a row where
    /// the header belongs). Fail closed: without a verified `proto`
    /// nothing is decoded.
    #[error("manifest has no header line")]
    MissingHeader,
    /// The header's `proto` is missing, not an unsigned integer, or a
    /// version this build does not support. Fail closed, min-reader rule
    /// (§2.2/§2.3): never guess at future row schemas.
    #[error("unsupported manifest proto {proto:?}")]
    UnsupportedProto {
        /// The declared proto (`None` when absent/malformed).
        proto: Option<u64>,
    },
    /// A line (header or row) failed to decode. 1-based line number in
    /// the uncompressed NDJSON; includes strict-wire relkey rejections
    /// (non-NFC `key`/`del`).
    #[error("manifest line {line} is malformed: {source}")]
    Line {
        /// 1-based NDJSON line number.
        line: usize,
        /// The decode failure.
        source: serde_json::Error,
    },
    /// A row cannot be converted into a synthetic journal apply op — e.g.
    /// a `preview` row with no `content_id`, or a `thumb` row (whose
    /// bucket key needs a size this schema does not carry).
    #[error("manifest row for {relkey} ({kind:?}) cannot be converted to an apply op")]
    Unconvertible {
        /// The row's relkey.
        relkey: RelKey,
        /// The row's kind.
        kind: Kind,
    },
    /// The consumer refused a synthetic entry; the merge transaction was
    /// aborted (nothing merged).
    #[error("consumer failed applying manifest row for {key:?}: {source}")]
    Consumer {
        /// The synthetic entry's bucket key.
        key: String,
        /// The consumer's error.
        #[source]
        source: ConsumerError,
    },
}

/// The manifest header (first NDJSON line, §2.3):
/// `{written_server_ts, cursors, proto}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestHeader {
    /// **Server** time (unix seconds) the manifest was written (§2.10
    /// horizons run on server time).
    pub written_server_ts: i64,
    /// The writer's applied cursors ([`crate::state::SyncDb::iter_cursors`])
    /// at write time: highest contiguously-applied seq per peer device.
    pub cursors: BTreeMap<DeviceId, u64>,
    /// Format version; [`MANIFEST_PROTO`] for manifests this build
    /// writes.
    pub proto: u32,
}

/// One live row (§2.3): the writer's knowledge of one live relkey.
/// Fields absent from [`crate::state::ItemRecord`] v1 (`device`,
/// `rating`, `color_label`) are `Option` and omitted when `None`, like
/// journal-entry optionals; unknown fields in incoming rows are ignored
/// (min-reader rule within a supported proto).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestRow {
    /// The library-relative key (strict wire decode).
    pub key: RelKey,
    /// Object kind.
    pub kind: Kind,
    /// Size in bytes.
    pub size: u64,
    /// blake3 of the current version's bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blake3: Option<Blake3Hex>,
    /// Semantic hash (sidecars).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sem_hash: Option<SemHash>,
    /// Per-relkey version vector (§2.6).
    pub vv: VersionVector,
    /// Authoring device of the current version, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device: Option<DeviceId>,
    /// Content identity (originals).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_id: Option<ContentId>,
    /// Final displayed width (originals).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub w: Option<u32>,
    /// Final displayed height (originals).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub h: Option<u32>,
    /// File mtime, unix **seconds** (journal-entry provenance, §2.2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mtime: Option<i64>,
    /// Star rating (sidecars), when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rating: Option<u8>,
    /// Color label (sidecars), when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub color_label: Option<String>,
}

/// One deleted-set row (§2.3): `{del, vv, server_ts}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeletedRow {
    /// The deleted relkey (strict wire decode — [`RelKey::parse_wire`]
    /// lane; a non-NFC spelling fails the decode rather than re-aiming
    /// the deletion).
    pub del: RelKey,
    /// The deletion's version vector.
    pub vv: VersionVector,
    /// Server time of the deletion (unix seconds).
    pub server_ts: i64,
}

/// A decoded (or to-be-encoded) per-writer manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest {
    /// The header line.
    pub header: ManifestHeader,
    /// Live rows, ascending by relkey (the order [`build_manifest`]
    /// produces and [`encode_manifest`] preserves).
    pub rows: Vec<ManifestRow>,
    /// Deleted-set rows, ascending by relkey.
    pub deleted: Vec<DeletedRow>,
}

/// Builds this device's manifest from the state db: header
/// `{written_server_ts, cursors:` [`crate::state::SyncDb::iter_cursors`]`,
/// proto:` [`MANIFEST_PROTO`]`}`, one live row per
/// [`crate::state::SyncDb::iter_items`] record (field mapping on
/// [`ManifestRow`]; `mtime` is the record's nanosecond mtime truncated to
/// seconds), one deleted row per
/// [`crate::state::SyncDb::iter_deleted`] record. Rows come out ascending
/// by relkey (the scans' order).
pub fn build_manifest(db: &SyncDb, written_server_ts: i64) -> Result<Manifest, ManifestError> {
    let _ = (db, written_server_ts);
    todo!("P1-U3: build_manifest")
}

/// Encodes a manifest as gzip NDJSON (module docs): header line first,
/// then live rows, then deleted rows, one JSON document per line.
/// Deterministic for the same manifest.
pub fn encode_manifest(manifest: &Manifest) -> Result<Vec<u8>, ManifestError> {
    let _ = manifest;
    todo!("P1-U3: encode_manifest")
}

/// Decodes gzip NDJSON manifest bytes, fail-closed and header-first
/// (module docs): unsupported `proto` is refused before any row decodes,
/// malformed rows abort naming their line, relkeys take the strict wire
/// lane.
pub fn decode_manifest(bytes: &[u8]) -> Result<Manifest, ManifestError> {
    let _ = bytes;
    todo!("P1-U3: decode_manifest")
}

/// Encodes and PUTs `manifest` to `device`'s manifest key
/// ([`crate::keys::manifest_key`]), returning the PUT's ETag. Only the
/// owning device may call this for its own id (single-writer invariant —
/// not enforceable here, but the engine only wires its own id through).
pub async fn put_manifest(
    s3: &impl S3Api,
    bucket: &str,
    device: &DeviceId,
    manifest: &Manifest,
) -> Result<String, ManifestError> {
    let _ = (s3, bucket, device, manifest);
    todo!("P1-U3: put_manifest")
}

/// GETs and decodes `device`'s manifest.
pub async fn get_manifest(
    s3: &impl S3Api,
    bucket: &str,
    device: &DeviceId,
) -> Result<Manifest, ManifestError> {
    let _ = (s3, bucket, device);
    todo!("P1-U3: get_manifest")
}

/// Read-back verification helper: HEADs `device`'s manifest key and
/// returns the stored object's ETag (compare against
/// [`put_manifest`]'s return to confirm the write landed intact).
pub async fn head_manifest_etag(
    s3: &impl S3Api,
    bucket: &str,
    device: &DeviceId,
) -> Result<String, ManifestError> {
    let _ = (s3, bucket, device);
    todo!("P1-U3: head_manifest_etag")
}

/// What one [`merge`] did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MergeReport {
    /// Live rows applied through the consumer.
    pub live_rows: u64,
    /// Deleted rows applied through the consumer.
    pub deleted_rows: u64,
    /// The merged cursor seed (max per device across all headers), as
    /// committed via [`crate::state::StateTxn::set_cursor`].
    pub cursors: BTreeMap<DeviceId, u64>,
}

/// §2.3 bootstrap/catch-up merge: applies every manifest's rows through
/// the **same** [`JournalConsumer`] as journal replay, then seeds the
/// db's cursors from the merged headers — all in **one** committed state
/// transaction (a failed merge leaves nothing behind).
///
/// Each element pairs a manifest with its **owning device** (known from
/// the key it was fetched from); the owner stamps synthetic `del`
/// entries, whose [`DeletedRow`] carries no device of its own.
///
/// Conversion (pinned): live rows become synthetic `put` entries carrying
/// the row's `kind`, `vv`, hashes, dimensions and `mtime`, with the
/// bucket key rebuilt from `(kind, key)` via the [`crate::keys`]
/// constructors and `device` = the row's `device` (falling back to the
/// manifest owner); deleted rows become synthetic `del` entries with
/// `ts` = the row's `server_ts`. Synthetic entries have `seq` 0 and are
/// **never** marked applied — idempotency across merge-then-poll comes
/// from the consumer's own apply semantics (§2.3: merging manifests *is*
/// the idempotent apply operation), not the applied set. Application
/// order (pinned): all live rows ascending by relkey (across manifests),
/// then all deleted rows ascending by relkey.
pub fn merge(
    manifests: &[(DeviceId, Manifest)],
    db: &SyncDb,
    consumer: &mut impl JournalConsumer,
) -> Result<MergeReport, ManifestError> {
    let _ = (manifests, db, consumer);
    todo!("P1-U3: manifest::merge")
}
