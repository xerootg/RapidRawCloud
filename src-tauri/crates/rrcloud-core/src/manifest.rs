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
//! seeds each device's cursor from its **own** manifest's
//! self-attestation (`cursors[owner]`; a peer's header claim about a
//! third device's prefix never seeds — see [`merge`]), so "bootstrap =
//! merge then poll" and a subsequent poll skips exactly the segments
//! whose effects the merged rows are guaranteed to fold.

use std::collections::BTreeMap;
use std::io::{Read as _, Write as _};

use crate::clock::DeviceId;
use crate::journal::{JournalEntry, Kind, Op, JOURNAL_VERSION};
use crate::keys::{
    library_key, manifest_key, preview_key, sidecar_key, thumbpack_key, RelKey, ALBUMS_META_KEY,
    PRESETS_META_KEY,
};
use crate::reader::{ConsumerError, JournalConsumer};
use crate::s3::{PutObjectOptions, S3Api, S3Error};
use crate::state::{ItemRecord, ItemState, StateError, SyncDb};

/// The manifest format version this build reads and writes (the header's
/// `proto` field).
pub const MANIFEST_PROTO: u32 = rrcloud_proto::MANIFEST_PROTO;

/// Cap on a manifest's **decompressed** NDJSON size. Generous —
/// proportional to the largest plausible library (at the §2.3 ballpark of
/// a few hundred bytes per row, this covers several hundred thousand
/// rows) — but a hard bound: gzip expands up to ~1000×, so without it a
/// single ~1 MiB corrupt or hostile object decompresses to a phone-OOMing
/// gigabyte before the first row parses. Same fail-closed stance as
/// [`crate::journal::decode_segment`]'s read-side cap.
pub const MANIFEST_MAX_DECODED_BYTES: usize = rrcloud_proto::MANIFEST_MAX_DECODED_BYTES;

/// Cap on a manifest's **compressed** (on-the-wire) size, bounding the
/// [`get_manifest`] network-lane buffer. NDJSON compresses well, so this
/// comfortably carries [`MANIFEST_MAX_DECODED_BYTES`] of real rows.
pub const MANIFEST_MAX_FETCH_BYTES: usize = rrcloud_proto::MANIFEST_MAX_FETCH_BYTES;

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
    /// stream, or unconsumed trailing bytes after the last gzip member).
    #[error("manifest gzip stream error: {0}")]
    Gzip(std::io::Error),
    /// The manifest decompresses past [`MANIFEST_MAX_DECODED_BYTES`]
    /// (fail-closed allocation bound — a decompression bomb, or a
    /// manifest far beyond any plausible library).
    #[error("manifest decompresses past the {limit}-byte cap")]
    DecodedTooLarge {
        /// The cap that was exceeded ([`MANIFEST_MAX_DECODED_BYTES`]).
        limit: usize,
    },
    /// The stored manifest object exceeds [`MANIFEST_MAX_FETCH_BYTES`]
    /// (fail-closed network-lane allocation bound).
    #[error("manifest object is {size} bytes, over the {limit}-byte fetch cap")]
    ObjectTooLarge {
        /// The object's declared size (which may understate the true size
        /// when the refusal came from the capped collect catching a lying
        /// `Content-Length`).
        size: u64,
        /// The cap that was exceeded ([`MANIFEST_MAX_FETCH_BYTES`]).
        limit: usize,
    },
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
    /// A header or row failed to **serialize** while encoding a manifest
    /// (should be unreachable for this build's own types; typed so a
    /// wire-document codec failure is never mislabeled as a state-db
    /// failure).
    #[error("manifest line could not be encoded: {0}")]
    Encode(#[source] serde_json::Error),
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
    /// [`build_manifest`] withholds such rows and [`merge`] skips them
    /// per-row ([`MergeReport::unconvertible`]) rather than refusing the
    /// merge, so this variant signals conversion failure internally and
    /// does not surface from those paths.
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

/// The header, live row, deleted row and the decoded document are the
/// generated `rrcloud-proto` SDK's types. Relkeys inside rows (`key`,
/// `del`) decode through the strict wire lane ([`RelKey::parse_wire`]); the
/// additive provenance fields (`device`, `ts`, `rating`, `color_label`)
/// are `Option` and omitted when `None`; unknown fields in incoming rows
/// are ignored (min-reader rule within a supported proto). `ManifestRow.ts`
/// is the advertised head version's journal `ts` — the §2.6 case-4
/// tiebreak input (review round 0: without it, [`merge`] stamped the
/// synthetic put with `written_server_ts`, so a device learning a version
/// via manifest could pick a different conflict primary than every journal
/// replayer); absent in rows from older writers, for which
/// [`live_row_entry`] falls back to the header ts, and withheld while an
/// upload intent is in flight (the record's `head_ts` then names the
/// in-flight version, not the advertised published one).
///
/// [`Manifest::rows`] and [`Manifest::deleted`] are ascending by relkey —
/// the order [`build_manifest`] produces and [`encode_manifest`] preserves.
pub use rrcloud_proto::{DeletedRow, Manifest, ManifestHeader, ManifestRow};

/// Builds this device's manifest from the state db: header
/// `{written_server_ts, cursors, proto:` [`MANIFEST_PROTO`]`}`, one live
/// row per [`crate::state::SyncDb::iter_items`] record (field mapping on
/// [`ManifestRow`]; `mtime` is the record's nanosecond mtime truncated to
/// seconds), one deleted row per
/// [`crate::state::SyncDb::iter_deleted`] record. Rows come out ascending
/// by relkey (the scans' order).
///
/// Header cursors are the peer cursors
/// ([`crate::state::SyncDb::iter_cursors`]) **plus the writer's own
/// published cursor** ([`crate::state::SyncDb::published_cursor`], when
/// nonzero): §2.10 compaction rule 1 ("the owner's own manifest has
/// `cursors[owner] >= s`") makes this entry the one guaranteed §2.3
/// catch-up attestation over the owner's compacted seqs — the manifest's
/// rows fold those entries' effects (deleted rows are always included,
/// and every live effect of a published entry is advertised, including
/// through the in-flight states: [`record_is_advertisable`]), so the
/// snapshot provably covers them. Without it, a device bootstrapping
/// after compaction could wedge in a permanent
/// [`crate::reader::MidStreamGap`].
///
/// Rows this build's own [`merge`] could not convert — `thumb` rows
/// (advertised via thumbpacks per §2.2; the row schema carries no thumb
/// size), and `preview` rows missing their `content_id` — are
/// **withheld**: publishing a row no reader can apply would at best be
/// dead weight and at worst wedge a less lenient peer's bootstrap.
///
/// Rows in the in-flight states `Dirty`, `Queued`, `Uploading` (not
/// [`state_is_remotely_visible`]) are withheld **only when the record
/// carries no uploaded version at all** (`blake3` `None` —
/// [`record_is_advertisable`]): a §2.3 live row is an advertisement that
/// the key's version is in the bucket, and a freshly imported item that
/// has never finished an upload would point every bootstrapping peer at
/// an object that is not there (404 on hydrate, phantom "missing" noise
/// in reconcile). But an in-flight item **with** a `blake3` has a
/// previously-published version (the record's `blake3` names the last
/// uploaded/verified bytes, which ARE in the bucket), and its row is the
/// manifest's only carrier of that published journal entry's effect —
/// withholding it while the header attests the own published cursor over
/// the entry's seq would make a §2.3 bootstrap merge silently drop the
/// item (the skip-and-diverge §2.1 principle 2 forbids; pinned by the
/// bootstrap regression tests). The emitted row truthfully describes the
/// published version: in [`crate::state::ItemRecord`] v1 its `vv`,
/// `blake3`, `size` and hashes still name it at these states — `size`
/// included, because a peer's §2.3 reconcile pairs the row's `blake3`
/// with its `size` and flags a mismatch as `corrupt_remote` (which is
/// why [`crate::state::ItemRecord`]'s `size`/`mtime_unix_ns` field
/// contracts pin last-published semantics until a snapshot mechanism
/// lands).
///
/// Two invariants this gate leans on, owed by the engine units around it:
///
/// - A completed upload records its `blake3`, and `blake3` is never set
///   before the first completed upload (or remote adoption) — so
///   `blake3: None` proves no version of the key was ever published, and
///   withholding such a row cannot un-fold a published entry.
/// - **§2.6 coordination note**: the queue-admission vv bump is a later
///   unit. Once it lands, the record — or a last-published snapshot
///   carried for this purpose — must keep the advertised fields (`vv`,
///   `blake3`, `size`, hashes) describing the last **published** version
///   while an upload is in flight; otherwise this row would pair the
///   bumped vv with the previous version's bytes and poison peer
///   idempotency.
///
/// The engine unit's additive record fields (`device`, `rating`,
/// `color_label` — §2.2/§2.6 provenance) travel on the row when the
/// record carries them; records written before that unit decode them as
/// `None` and stay honestly absent. §2.7 soft-deleted records are
/// withheld from the live rows (their deletion rides the deleted set
/// instead; see the filter below).
pub fn build_manifest(db: &SyncDb, written_server_ts: i64) -> Result<Manifest, ManifestError> {
    let mut cursors: BTreeMap<DeviceId, u64> = db.iter_cursors()?.into_iter().collect();
    // The own entry comes from the published cursor, never the cursors
    // table (which holds peers): see the doc comment.
    let own = db.device_id().clone();
    cursors.remove(&own);
    let published = db.published_cursor()?;
    if published > 0 {
        cursors.insert(own, published);
    }
    let header = ManifestHeader {
        written_server_ts,
        cursors,
        proto: MANIFEST_PROTO,
    };
    let rows = db
        .iter_items()?
        .into_iter()
        // Never advertise a key that provably has no remote object; but an
        // in-flight item with a published previous version MUST stay
        // advertised, or the own-cursor attestation above would claim
        // coverage the rows don't deliver (doc comment). A §2.7
        // soft-deleted record is withheld as a LIVE row — its deletion is
        // what the deleted set below advertises; emitting both would hand
        // every bootstrapping peer a live/deleted contradiction for the
        // same key (engine unit; pinned by the engine suite).
        .filter(|(_, record)| !record.deleted && record_is_advertisable(record))
        .map(|(key, record)| {
            // §2.6 coordination note: while an upload intent is in
            // flight the row advertises the last PUBLISHED version
            // (vv/blake3/size), but the record's head identity
            // (`head_ts`/`device`) names the IN-FLIGHT version — pairing
            // them would advertise a (version, identity) no journal
            // entry ever carried, so both are withheld for in-flight
            // rows (merge falls back to owner/header ts; review round 0).
            let in_flight = record.admitted_vv.is_some();
            ManifestRow {
                key,
                kind: record.kind,
                size: record.size,
                blake3: record.blake3,
                sem_hash: record.sem_hash,
                vv: record.vv,
                // §2.2/§2.6 provenance, carried since the engine unit
                // landed the additive record fields; absent (older
                // records, non-head kinds, in-flight rows) stays
                // honestly absent.
                device: if in_flight { None } else { record.device },
                content_id: record.content_id,
                w: record.w,
                h: record.h,
                mtime: Some(record.mtime_unix_ns / 1_000_000_000),
                ts: if in_flight { None } else { record.head_ts },
                rating: record.rating,
                color_label: record.color_label,
            }
        })
        // Withhold rows merge cannot convert (doc comment): emitting them
        // would hand every peer an unusable row.
        .filter(|row| bucket_key_for(row).is_ok())
        .collect();
    let deleted = db
        .iter_deleted()?
        .into_iter()
        .map(|(del, record)| DeletedRow {
            del,
            vv: record.vv,
            server_ts: record.server_ts,
        })
        .collect();
    Ok(Manifest {
        header,
        rows,
        deleted,
    })
}

/// Encodes a manifest as gzip NDJSON (module docs): header line first,
/// then live rows, then deleted rows, one JSON document per line.
/// Deterministic for the same manifest.
pub fn encode_manifest(manifest: &Manifest) -> Result<Vec<u8>, ManifestError> {
    let mut ndjson = Vec::new();
    let mut push_line = |line: Result<String, serde_json::Error>| -> Result<(), ManifestError> {
        // Encode-side serialization failures carry no useful line number,
        // but they are wire-document codec failures, not state-db ones:
        // the dedicated Encode variant keeps the error taxonomy truthful.
        let line = line.map_err(ManifestError::Encode)?;
        ndjson.extend_from_slice(line.as_bytes());
        ndjson.push(b'\n');
        Ok(())
    };
    push_line(serde_json::to_string(&manifest.header))?;
    for row in &manifest.rows {
        push_line(serde_json::to_string(row))?;
    }
    for row in &manifest.deleted {
        push_line(serde_json::to_string(row))?;
    }
    // flate2's plain gzip header carries no mtime or filename, so the
    // encoding is deterministic for the same manifest (ETag read-back
    // verification depends on byte identity).
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(&ndjson).map_err(ManifestError::Gzip)?;
    encoder.finish().map_err(ManifestError::Gzip)
}

/// Decodes gzip NDJSON manifest bytes, fail-closed and header-first
/// (module docs): unsupported `proto` is refused before any row decodes,
/// malformed rows abort naming their line, relkeys take the strict wire
/// lane.
pub fn decode_manifest(bytes: &[u8]) -> Result<Manifest, ManifestError> {
    // MultiGzDecoder, not GzDecoder: RFC 1952 streams may hold several
    // members (plain `cat` produces them), and a decoder that stops at
    // the first member would silently accept a PARTIAL manifest — for the
    // deleted set, the §2.3 resurrection hazard. MultiGzDecoder decodes
    // every member and errors on trailing non-gzip bytes, so the result
    // is always the whole input or a refusal. The `take` bounds
    // decompressed allocation against bombs (one byte of headroom turns
    // "hit the cap exactly" into a detectable overflow).
    let mut ndjson = Vec::new();
    flate2::read::MultiGzDecoder::new(bytes)
        .take(MANIFEST_MAX_DECODED_BYTES as u64 + 1)
        .read_to_end(&mut ndjson)
        .map_err(ManifestError::Gzip)?;
    if ndjson.len() > MANIFEST_MAX_DECODED_BYTES {
        return Err(ManifestError::DecodedTooLarge {
            limit: MANIFEST_MAX_DECODED_BYTES,
        });
    }

    let mut lines = ndjson.split(|&b| b == b'\n').enumerate();

    // Header first, fail closed: the proto gate fires before any row —
    // including the header's own full schema — decodes.
    let header = loop {
        let Some((idx, line)) = lines.next() else {
            return Err(ManifestError::MissingHeader);
        };
        if line.is_empty() {
            continue;
        }
        let line_no = idx + 1;
        let value: serde_json::Value =
            serde_json::from_slice(line).map_err(|source| ManifestError::Line {
                line: line_no,
                source,
            })?;
        // A row where the header belongs is a missing header, not a
        // malformed one.
        if value.get("key").is_some() || value.get("del").is_some() {
            return Err(ManifestError::MissingHeader);
        }
        let proto = value.get("proto").and_then(serde_json::Value::as_u64);
        if proto != Some(u64::from(MANIFEST_PROTO)) {
            return Err(ManifestError::UnsupportedProto { proto });
        }
        break serde_json::from_value::<ManifestHeader>(value).map_err(|source| {
            ManifestError::Line {
                line: line_no,
                source,
            }
        })?;
    };

    let mut rows = Vec::new();
    let mut deleted = Vec::new();
    for (idx, line) in lines {
        if line.is_empty() {
            continue;
        }
        let line_no = idx + 1;
        let at_line = |source: serde_json::Error| ManifestError::Line {
            line: line_no,
            source,
        };
        let value: serde_json::Value = serde_json::from_slice(line).map_err(at_line)?;
        if value.get("del").is_some() {
            deleted.push(serde_json::from_value::<DeletedRow>(value).map_err(at_line)?);
        } else {
            rows.push(serde_json::from_value::<ManifestRow>(value).map_err(at_line)?);
        }
    }
    Ok(Manifest {
        header,
        rows,
        deleted,
    })
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
    let bytes = encode_manifest(manifest)?;
    let output = s3
        .put_object(
            bucket,
            &manifest_key(device),
            bytes::Bytes::from(bytes),
            &PutObjectOptions::default(),
        )
        .await?;
    Ok(output.e_tag)
}

/// GETs and decodes `device`'s manifest. Network-lane allocation is
/// bounded ([`MANIFEST_MAX_FETCH_BYTES`]): an oversized object is the
/// typed [`ManifestError::ObjectTooLarge`] before (and while) buffering,
/// never an unbounded buffer.
pub async fn get_manifest(
    s3: &impl S3Api,
    bucket: &str,
    device: &DeviceId,
) -> Result<Manifest, ManifestError> {
    let output = s3.get_object(bucket, &manifest_key(device), None).await?;
    let declared = output.content_length;
    if declared > MANIFEST_MAX_FETCH_BYTES as u64 {
        return Err(ManifestError::ObjectTooLarge {
            size: declared,
            limit: MANIFEST_MAX_FETCH_BYTES,
        });
    }
    let bytes = match output.body.collect_capped(MANIFEST_MAX_FETCH_BYTES).await {
        Ok(bytes) => bytes,
        // The body overran the cap behind a lying Content-Length: the same
        // persistent oversized-object condition as the pre-check catches,
        // not a transport failure.
        Err(S3Error::BodyCapExceeded { .. }) => {
            return Err(ManifestError::ObjectTooLarge {
                size: declared,
                limit: MANIFEST_MAX_FETCH_BYTES,
            })
        }
        Err(e) => return Err(e.into()),
    };
    decode_manifest(&bytes)
}

/// Read-back verification helper: HEADs `device`'s manifest key and
/// returns the stored object's ETag (compare against
/// [`put_manifest`]'s return to confirm the write landed intact).
pub async fn head_manifest_etag(
    s3: &impl S3Api,
    bucket: &str,
    device: &DeviceId,
) -> Result<String, ManifestError> {
    let output = s3.head_object(bucket, &manifest_key(device)).await?;
    Ok(output.e_tag)
}

/// What one [`merge`] did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MergeReport {
    /// Live rows applied through the consumer.
    pub live_rows: u64,
    /// Deleted rows applied through the consumer.
    pub deleted_rows: u64,
    /// The merged cursor seed, as committed via
    /// [`crate::state::StateTxn::set_cursor`]: per device, the max of
    /// that device's **own** manifests' self-attestations
    /// (`cursors[owner]`), never a peer's claim about it, and excluding
    /// the merging device's own id — its own prefix is authoritative
    /// locally. See [`merge`] for why peer claims are not trusted.
    pub cursors: BTreeMap<DeviceId, u64>,
    /// Live rows no synthetic apply op could be built for (`thumb` rows,
    /// `preview` rows without a `content_id`), as `(relkey, kind)`
    /// ascending by relkey. Skipped, not applied — they are additive
    /// advisory rows from a non-conforming or future writer, so skipping
    /// one cannot destroy data, while refusing the whole merge over one
    /// would wedge the §2.3 bootstrap lane fleet-wide. Surfaced here so
    /// the engine can log them loudly.
    pub unconvertible: Vec<(RelKey, Kind)>,
}

/// §2.3 bootstrap/catch-up merge: applies every manifest's rows through
/// the **same** [`JournalConsumer`] as journal replay, then seeds the
/// db's cursors from the merged headers' **owner self-attestations** —
/// all in **one** committed state transaction (a failed merge leaves
/// nothing behind).
///
/// Cursor seeding (pinned): device `D`'s cursor seeds only from
/// `cursors[D]` of a manifest **owned by `D`** — the §2.10 rule-1
/// attestation whose coverage `D`'s own rows are guaranteed to fold
/// ([`build_manifest`]). A peer's header claim about a third device's
/// prefix is never trusted: the claimant's rows are subject to its own
/// live-row withholding, so seeding from the claim could skip journal
/// entries whose effects no merged row folds (silent loss, §2.1
/// principle 2). An unseeded prefix is simply applied from the journal
/// on the next poll.
///
/// Each element pairs a manifest with its **owning device** (known from
/// the key it was fetched from); the owner stamps synthetic `del`
/// entries, whose [`DeletedRow`] carries no device of its own.
///
/// Conversion (pinned): live rows become synthetic `put` entries carrying
/// the row's `kind`, `vv`, hashes, dimensions and `mtime`, with the
/// bucket key rebuilt from `(kind, key)` via the [`crate::keys`]
/// constructors, `device` = the row's `device` (falling back to the
/// manifest owner), and `ts` = the header's `written_server_ts` (a
/// provenance rewrite — see [`live_row_entry`]'s caveat); deleted rows
/// become synthetic `del` entries with `ts` = the row's `server_ts`.
/// Synthetic entries have `seq` 0 and are
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
    // Gather and order BEFORE opening the transaction: all live rows
    // ascending by relkey across manifests, then all deleted rows
    // ascending by relkey. (Stable sort: same-relkey rows keep manifest
    // order.)
    let mut live: Vec<(&DeviceId, &ManifestHeader, &ManifestRow)> = Vec::new();
    let mut deleted: Vec<(&DeviceId, &DeletedRow)> = Vec::new();
    let mut cursors: BTreeMap<DeviceId, u64> = BTreeMap::new();
    for (owner, manifest) in manifests {
        for row in &manifest.rows {
            live.push((owner, &manifest.header, row));
        }
        for row in &manifest.deleted {
            deleted.push((owner, row));
        }
        // Cursor seeding trusts only each OWNER's self-attestation
        // (`cursors[owner]`): the §2.10 rule-1 entry whose coverage this
        // manifest's own rows are guaranteed to fold. A peer's header
        // claim about a THIRD device's prefix is "I applied these", but
        // the claimant's rows are subject to its own live-row withholding
        // (e.g. an item it re-dirtied from a base it never uploaded, the
        // PendingDown→Dirty edge), so trusting the claim could skip
        // journal entries whose effects no merged row folds — silent loss.
        // Unseeded prefixes simply get applied from the journal (or hit
        // the typed gap outcomes) on the next poll.
        //
        // Never seed a cursor for the merging device's own prefix either:
        // a device never applies its own journal, its cursors table holds
        // peers only, and its own published cursor is the authoritative
        // local fact.
        if *owner != *db.device_id() {
            if let Some(&seq) = manifest.header.cursors.get(owner) {
                let slot = cursors.entry(owner.clone()).or_insert(0);
                *slot = (*slot).max(seq);
            }
        }
    }
    live.sort_by(|a, b| a.2.key.cmp(&b.2.key));
    deleted.sort_by(|a, b| a.1.del.cmp(&b.1.del));

    // Convert eagerly too, so conversion failures surface before any
    // consumer side-effect could need rolling back. An unconvertible row
    // (thumb / content-id-less preview, from a non-conforming or future
    // writer) is skipped and reported, not a refusal: these are additive
    // advisory rows, and one of them must not wedge the whole §2.3
    // bootstrap lane ([`MergeReport::unconvertible`]).
    let mut entries: Vec<JournalEntry> = Vec::with_capacity(live.len() + deleted.len());
    let mut unconvertible: Vec<(RelKey, Kind)> = Vec::new();
    for (owner, header, row) in &live {
        match live_row_entry(owner, header, row) {
            Ok(entry) => entries.push(entry),
            Err(ManifestError::Unconvertible { relkey, kind }) => {
                unconvertible.push((relkey, kind));
            }
            Err(e) => return Err(e),
        }
    }
    for (owner, row) in &deleted {
        entries.push(deleted_row_entry(owner, row));
    }

    // One committed transaction for every row apply AND the cursor
    // seeding: a failed merge leaves nothing behind.
    let report = MergeReport {
        live_rows: (live.len() - unconvertible.len()) as u64,
        deleted_rows: deleted.len() as u64,
        cursors,
        unconvertible,
    };
    db.with_txn_err::<_, ManifestError>(|txn| {
        for entry in &entries {
            consumer
                .apply(txn, entry)
                .map_err(|source| ManifestError::Consumer {
                    key: entry.key.clone(),
                    source,
                })?;
        }
        for (device, &seq) in &report.cursors {
            txn.set_cursor(device, seq)?;
        }
        Ok(())
    })?;
    Ok(report)
}

/// Whether an item record may be advertised as a §2.3 live row: `true`
/// iff a remote object for some version of the key provably exists —
/// either the state itself proves a completed upload
/// ([`state_is_remotely_visible`]), or the record's `blake3` does (it
/// names the last **uploaded/verified** bytes by the
/// [`crate::state::ItemRecord`] contract, so an in-flight re-edit of a
/// previously-published item keeps advertising the published version).
/// Only a record that is both in-flight and `blake3`-less — a key no
/// version of which was ever uploaded, hence no published journal entry
/// covers — is withheld. See [`build_manifest`]'s docs for why anything
/// weaker breaks the own-cursor attestation.
fn record_is_advertisable(record: &ItemRecord) -> bool {
    state_is_remotely_visible(record.state) || record.blake3.is_some()
}

/// Whether an item's **state alone** proves a remote object exists for
/// the recorded version. `Dirty`/`Queued`/`Uploading` versions have never
/// finished an upload; `Verifying` and later states follow a completed
/// upload (or an advertisement by another writer, for the download-side
/// states); even `CorruptRemote` names an object that exists — content
/// verification is every reader's own job, keyed by the row's `blake3`.
/// This is one input to [`record_is_advertisable`], never the whole gate:
/// an in-flight record whose `blake3` evidences a published previous
/// version is still advertised.
fn state_is_remotely_visible(state: ItemState) -> bool {
    // Exhaustive on purpose: a future ItemState must decide its
    // manifest visibility explicitly.
    match state {
        ItemState::Dirty | ItemState::Queued | ItemState::Uploading => false,
        ItemState::Verifying
        | ItemState::Synced
        | ItemState::CorruptRemote
        | ItemState::Conflict
        | ItemState::PendingDown
        | ItemState::Downloading
        | ItemState::Stub
        | ItemState::Hydrated => true,
    }
}

/// Rebuilds the bucket key a manifest row's `(kind, relkey)` addresses,
/// through the [`crate::keys`] constructors.
///
/// **§2.6 coordination note (virtual-copy keys), resolved by the engine
/// unit's file-relkey convention**: the engine keys sidecar items —
/// primaries and conflict losers alike — by the sidecar **file's own**
/// relkey (`<image>.rrdata`, `<image>.<6hex>.rrdata`;
/// [`crate::engine`] module docs), so a vc row's `key` carries the full
/// file path and needs no extra field. This function mirrors the
/// transfer seam's suffix-aware mapping: a `Sidecar` row whose key
/// already ends in `.rrdata` rebuilds through plain [`library_key`]
/// (appending a second suffix would aim the row at a key no journal
/// entry ever advertised), while image-relkey rows — everything written
/// before the engine unit — keep the landed [`sidecar_key`] mapping.
/// Residual caveat for **pre-engine readers only**: they lack the
/// suffix-aware branch and would double-suffix a file-relkey row; such
/// a key classifies as a plain (nonexistent) sidecar and creates a
/// stray `pending_down` record, never a clobber of the primary's
/// vv/hashes — and no pre-engine build ever shipped outside this repo.
fn bucket_key_for(row: &ManifestRow) -> Result<String, ManifestError> {
    let unconvertible = || ManifestError::Unconvertible {
        relkey: row.key.clone(),
        kind: row.kind,
    };
    Ok(match row.kind {
        Kind::Original => library_key(&row.key),
        Kind::Sidecar if row.key.as_str().ends_with(".rrdata") => library_key(&row.key),
        Kind::Sidecar => sidecar_key(&row.key),
        // An .xmp projection is a real library file; its relkey carries
        // the extension.
        Kind::Xmp => library_key(&row.key),
        Kind::Preview => preview_key(row.content_id.as_ref().ok_or_else(unconvertible)?),
        // A thumb's bucket key needs a size this schema does not carry.
        Kind::Thumb => return Err(unconvertible()),
        Kind::Thumbpack => thumbpack_key(&row.key),
        Kind::Albums => ALBUMS_META_KEY.to_string(),
        Kind::Presets => PRESETS_META_KEY.to_string(),
    })
}

/// Converts one live row into its synthetic `put` entry (`seq` 0, never
/// marked applied; conversion pinned by the merge tests).
///
/// `ts` provenance: the row's own `ts` — the advertised version's real
/// journal timestamp — so a device learning a version via manifest merge
/// resolves §2.6 case 4 with the SAME candidate the journal replayers
/// compare, picking the same primary (review round 0: the old
/// `written_server_ts` stamp made merge-learned heads win ties they
/// lost everywhere else — fleet-divergent primaries with equal folded
/// vvs plus a wrong-blake3 download wedge). **Residual fallback**: a
/// row without `ts` (an older writer, or a row advertised while its
/// writer had an upload in flight) still stamps `written_server_ts` —
/// always-newer, so such a head can win a pick it would lose with its
/// true ts; the loser is preserved either way, and the next real journal
/// entry for the key re-heads it.
fn live_row_entry(
    owner: &DeviceId,
    header: &ManifestHeader,
    row: &ManifestRow,
) -> Result<JournalEntry, ManifestError> {
    Ok(JournalEntry {
        v: JOURNAL_VERSION,
        seq: 0,
        ts: row.ts.unwrap_or(header.written_server_ts),
        device: row.device.clone().unwrap_or_else(|| owner.clone()),
        op: Op::Put,
        kind: row.kind,
        key: bucket_key_for(row)?,
        vv: row.vv.clone(),
        size: Some(row.size),
        blake3: row.blake3.clone(),
        sem_hash: row.sem_hash.clone(),
        rating: row.rating,
        color_label: row.color_label.clone(),
        content_id: row.content_id.clone(),
        w: row.w,
        h: row.h,
        mtime: row.mtime,
        from_key: None,
    })
}

/// Converts one deleted-set row into its synthetic `del` entry. The
/// deleted set holds **one row per deleted item**, keyed by the item's
/// own file relkey (§2.3 "one row per deleted key"; [`crate::engine`]
/// records them this way — review round 0: a single image-keyed row
/// synthesized only a primary-sidecar del, so a laggard catching a
/// deletion up via manifest merge hid the sidecar but left the original
/// live, breaking "merge is the same idempotent apply as replay"). The
/// del therefore aims at the item's own bucket key, with the kind that
/// key classifies to; a [`DeletedRow`] carries no device, so the
/// manifest's owner stamps the entry.
fn deleted_row_entry(owner: &DeviceId, row: &DeletedRow) -> JournalEntry {
    let key = library_key(&row.del);
    let kind = match crate::keys::classify_key(&key) {
        crate::keys::KeyClass::Original { .. } => Kind::Original,
        crate::keys::KeyClass::Xmp { .. } => Kind::Xmp,
        // Sidecar spellings (primary or vc) — and, unreachably for a
        // library key built from a valid relkey, anything else — aim at
        // the sidecar kind, matching the engine's classification.
        _ => Kind::Sidecar,
    };
    JournalEntry {
        v: JOURNAL_VERSION,
        seq: 0,
        ts: row.server_ts,
        device: owner.clone(),
        op: Op::Del,
        kind,
        key,
        vv: row.vv.clone(),
        size: None,
        blake3: None,
        sem_hash: None,
        rating: None,
        color_label: None,
        content_id: None,
        w: None,
        h: None,
        mtime: None,
        from_key: None,
    }
}
