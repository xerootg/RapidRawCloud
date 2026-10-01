//! The sync engine for **one device** (architecture §2.5–§2.8): local
//! change intake with the semantic churn gate, quiescence-gated admission
//! (the §2.6 "one admitted upload = one version" vv bump), the §2.6
//! unified apply rule as a [`JournalConsumer`] (which therefore also
//! drives §2.3 manifest merge), §2.7 soft delete / restore /
//! edits-beat-deletes resurrection, and the §2.8 original-overwrite
//! conflict copies.
//!
//! No supervisor loop and no timers live here: every entry point is a
//! synchronous-async call a caller drives (the §3.3 SyncManager wiring is
//! an app-side unit), which is also what makes the whole protocol
//! deterministic enough to pin in the scenario suite. Out of scope for
//! this unit: compaction/GC/horizons (§2.10), reconcile/foreign adoption
//! (worker unit), hydration/eviction policy (P2).
//!
//! # Item keying (the file-relkey convention)
//!
//! One image owns **several** independently versioned sync items — its
//! original, its primary sidecar, virtual-copy sidecars, an interop
//! `.xmp` — and §3.5 requires them in different states at once (original
//! `Stub` while the sidecar is `Dirty`). The state store keys items by
//! one [`RelKey`], so the engine keys every item by **the file's own
//! library-relative path**:
//!
//! | Item | Items-table key | Bucket key |
//! |---|---|---|
//! | original | `p/img.NEF` | `library/p/img.NEF` |
//! | primary sidecar | `p/img.NEF.rrdata` | `library/p/img.NEF.rrdata` |
//! | virtual copy / conflict loser | `p/img.NEF.<6hex>.rrdata` | `library/p/img.NEF.<6hex>.rrdata` |
//! | xmp projection | `p/img.xmp` | `library/p/img.xmp` |
//! | displaced original (§2.8) | `p/img.conflict-<6hex>.NEF` | `library/p/img.conflict-<6hex>.NEF` |
//!
//! Every bucket key is uniformly `library_key(item relkey)` (for a
//! sidecar item the `.rrdata` suffix is part of the relkey, so this
//! equals the canonical [`crate::keys::sidecar_key`] spelling of the
//! image relkey), and [`crate::keys::classify_key`] inverts it;
//! [`item_relkey_for`] is the engine's single spelling of that inverse.
//! The transfer engine's `(relkey, kind)` mapping is extended additively
//! for this convention: a `Kind::Sidecar` relkey that already ends in
//! `.rrdata` maps through [`crate::keys::library_key`] / plain
//! [`crate::keys::local_path`] instead of having a second suffix
//! appended (pinned by the engine suite; relkeys without the suffix keep
//! the landed behavior).
//!
//! # The §2.6 unified apply rule (what [`EngineConsumer`] implements)
//!
//! Every arriving `put` is ordered against the local item head by
//! [`crate::clock::compare`]; there is no resolution-bypassing
//! fast-forward path:
//!
//! 1. **Converged** (`vv` equal, or same content: equal `sem_hash` for
//!    sidecars, equal `blake3` otherwise) → adopt metadata (vv
//!    elementwise max), no download.
//! 2. **Remote dominates** → adopt: record takes the entry's metadata
//!    and the item queues a download (`PendingDown`; the byte transfer
//!    is pump-driven, and the §3.5 sidecar install path parse-validates
//!    before replacing). If the local copy has **uncommitted dirty
//!    edits** (`Dirty`, never admitted), it is first committed as a
//!    local version through the admission path (vv[self] bump, `ts`
//!    frozen), which makes the comparison fall into case 4.
//! 3. **Local dominates** → ignore (our version supersedes; it reaches
//!    the other side through our journal).
//! 4. **Concurrent** → deterministic winner via
//!    [`crate::clock::pick_winner`] over `(ts, device)` of the two head
//!    versions — which is why [`crate::state::ItemRecord`] carries
//!    `head_ts`/`device`. The winner becomes the path's primary
//!    (download queued when remote won); the **loser materializes** as
//!    the deterministic virtual-copy sidecar
//!    `<file>.<blake3(canonical loser doc)[..6]>.rrdata` on every device
//!    that holds the loser bytes (its author always does; a device
//!    holding only the winner skips materialization), with apply-side
//!    dedup by `(key, sem_hash)`; the loser vc is journaled as its own
//!    new key with a fresh single-component vv (through the ordinary
//!    admission + upload lane). Afterwards the path's vv becomes the
//!    elementwise max of both, so the conflict cannot reopen, and a
//!    [`ConflictEvent`] fires through the caller's [`EngineEvents`].
//!
//! `del` entries order through the same machinery (§2.7): a dominating
//! `del` marks the item deleted (hidden; the record survives with the
//! [`crate::state::ItemRecord::deleted`] flag); a `del` concurrent with
//! local dirty-or-newer **resurrects** — put entries with dominating vv
//! are emitted for the sidecar **and** the image's original (the
//! original's put re-advertises the known `blake3`/`content_id`; the
//! bytes are still in the bucket during the grace window, so nothing
//! re-uploads). When the original was never known locally (no record /
//! no `blake3`), only the sidecar resurrects and a
//! [`ResurrectionIncompleteEvent`] surfaces the §2.7 edge.
//!
//! `preview` / `thumb` / `thumbpack` / `albums` / `presets` entries are
//! recorded as applied with **no state effects** in this unit (download
//! policy is P2); `xmp` entries record metadata only — per §2.8 the xmp
//! is a projection of the sidecar, never a conflict domain of its own,
//! so no loser copies and no transfers originate from them here.
//!
//! # Always-blake3 puts
//!
//! Every engine-emitted `put` carries a `blake3` **by construction**:
//! upload-journaled puts hash exactly the sent bytes (§2.4), and every
//! metadata-only put (restore, resurrection, loser/conflict-copy
//! advertisements) is built through [`EnginePut`], whose `blake3` field
//! is not optional — the U3-noted blake3-less-put corner is
//! unconstructible at the type level (pinned by the S8 scenario).

use std::path::{Path, PathBuf};

use crate::clock::{compare, pick_winner, Candidate, DeviceId, VersionVector, VvOrder};
use crate::journal::{JournalEntry, JournalError, Kind, Op, Tombstone, JOURNAL_VERSION};
use crate::keys::{classify_key, library_key, tombstone_key, KeyClass, KeyError, RelKey};
use crate::publisher::{enqueue_entry_in, PublisherError};
use crate::reader::{ConsumerError, JournalConsumer};
use crate::s3::{PutObjectOptions, S3Api, S3Error};
use crate::semhash::{
    canonical_json, sem_hash, sidecar_badges, Blake3Hex, ContentId, SemHash, SemHashError,
};
use crate::state::{DeletedRecord, ItemRecord, ItemState, Queue, StateError, StateTxn, SyncDb};
use crate::transfer::TransferError;

/// Upload/download priority class for sidecar items (§3.5:
/// thumbs-visible > sidecars > small thumbs > proxies; within this
/// unit's transfer lanes, sidecars outrank originals).
pub const CLASS_SIDECAR: u8 = 1;

/// Priority class for original (and conflict-copy original) items.
pub const CLASS_ORIGINAL: u8 = 2;

/// Error from the sync engine.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum EngineError {
    /// State-store failure.
    #[error(transparent)]
    State(#[from] StateError),
    /// S3 failure (tombstone PUT).
    #[error(transparent)]
    S3(#[from] S3Error),
    /// Journal staging failure.
    #[error(transparent)]
    Publisher(#[from] PublisherError),
    /// Transfer-engine failure surfaced through an engine entry point.
    #[error(transparent)]
    Transfer(#[from] TransferError),
    /// Journal encoding failure.
    #[error(transparent)]
    Journal(#[from] JournalError),
    /// Relkey construction failure (derived item keys).
    #[error(transparent)]
    Key(#[from] KeyError),
    /// Sidecar parse failure (§2.5 fail-closed: an unparsable sidecar is
    /// an error, never a default document).
    #[error(transparent)]
    SemHash(#[from] SemHashError),
    /// Local file I/O failed (loser materialization, conflict-copy
    /// staging).
    #[error("I/O error on {path}: {source}")]
    Io {
        /// The path the operation was on.
        path: PathBuf,
        /// The underlying error.
        source: std::io::Error,
    },
    /// An engine entry point was called for an item the store has no
    /// record of.
    #[error("no item record for {relkey}")]
    UnknownItem {
        /// The item (file relkey).
        relkey: RelKey,
    },
    /// The caller-supplied kind disagrees with the item's stored kind.
    #[error("kind {given:?} does not match stored kind {stored:?} for {relkey}")]
    KindMismatch {
        /// The item (file relkey).
        relkey: RelKey,
        /// The kind the caller passed.
        given: Kind,
        /// The kind the record holds.
        stored: Kind,
    },
    /// [`restore_item`] was called for an image with nothing deleted.
    #[error("nothing is deleted at {relkey}; nothing to restore")]
    NotDeleted {
        /// The image relkey.
        relkey: RelKey,
    },
}

// ---------------------------------------------------------------------------
// Item keying helpers (module docs, "Item keying")
// ---------------------------------------------------------------------------

/// The items-table key of `image`'s **primary sidecar**:
/// `<image>.rrdata`. Appending the suffix to a valid relkey always
/// yields a valid relkey (the final segment grows and still ends in a
/// letter), so this is total in practice; the `Result` keeps the
/// no-panic contract honest.
pub fn sidecar_item_relkey(image: &RelKey) -> Result<RelKey, KeyError> {
    RelKey::new(format!("{image}.rrdata"))
}

/// The items-table key of `image`'s **virtual-copy sidecar** with
/// 6-lowercase-hex suffix `vc6`: `<image>.<vc6>.rrdata`. Rejects a
/// malformed suffix ([`KeyError::BadVcSuffix`]).
pub fn vc_item_relkey(image: &RelKey, vc6: &str) -> Result<RelKey, KeyError> {
    if !crate::hexutil::is_lower_hex(vc6, 6) {
        return Err(KeyError::BadVcSuffix(vc6.to_string()));
    }
    RelKey::new(format!("{image}.{vc6}.rrdata"))
}

/// The deterministic §2.6 loser virtual-copy suffix:
/// `blake3(canonical loser document)[..6]` — six lowercase hex chars of
/// the blake3 of [`crate::semhash::canonical_json`] over the parsed
/// loser sidecar document. Canonicalization makes the suffix a function
/// of the document's **content**, not its spelling, so every holder of
/// the loser materializes the **same** key (§2.6 / review B2). Invalid
/// JSON fails closed like [`crate::semhash::sem_hash`].
pub fn loser_vc_suffix(loser_doc: &[u8]) -> Result<String, EngineError> {
    let value: serde_json::Value = serde_json::from_slice(loser_doc).map_err(SemHashError::from)?;
    let hex = blake3::hash(canonical_json(&value).as_bytes()).to_hex();
    Ok(hex.as_str()[..6].to_string())
}

/// The deterministic §2.8 displaced-original relkey for `image` whose
/// displaced bytes hash to `displaced`:
/// `<stem>.conflict-<displaced[..6]>.<ext>` in the image's directory
/// (`<stem>.conflict-<6hex>` when the image has no extension). The same
/// `(image, displaced)` pair yields the same relkey on every device, so
/// concurrent uploads are byte-identical PUTs to one key.
pub fn original_conflict_relkey(
    image: &RelKey,
    displaced: &ContentId,
) -> Result<RelKey, EngineError> {
    let h6 = &displaced.as_str()[..6];
    let full = image.as_str();
    let (dir, name) = match full.rfind('/') {
        Some(i) => full.split_at(i + 1),
        None => ("", full),
    };
    let spelled = match name.rsplit_once('.') {
        Some((stem, ext)) => format!("{dir}{stem}.conflict-{h6}.{ext}"),
        None => format!("{dir}{name}.conflict-{h6}"),
    };
    Ok(RelKey::new(spelled)?)
}

/// The items-table key a classified bucket key addresses (`None` for
/// control-plane and foreign keys): the single inverse of the module's
/// file-relkey convention — `Original`/`Xmp` keep their relkey,
/// `Sidecar { relkey, vc: None }` maps to `<relkey>.rrdata`, and
/// `Sidecar { relkey, vc: Some(h) }` to `<relkey>.<h>.rrdata`.
pub fn item_relkey_for(class: &KeyClass) -> Option<RelKey> {
    match class {
        KeyClass::Original { relkey } | KeyClass::Xmp { relkey } => Some(relkey.clone()),
        KeyClass::Sidecar { relkey, vc: None } => sidecar_item_relkey(relkey).ok(),
        KeyClass::Sidecar {
            relkey,
            vc: Some(h),
        } => vc_item_relkey(relkey, h).ok(),
        _ => None,
    }
}

/// The transfer priority class of `kind`'s lane
/// ([`CLASS_SIDECAR`]/[`CLASS_ORIGINAL`]).
fn transfer_class(kind: Kind) -> u8 {
    match kind {
        Kind::Sidecar => CLASS_SIDECAR,
        _ => CLASS_ORIGINAL,
    }
}

/// `true` when the state says the local file at the item's path holds
/// the item's **head version** — the §2.6 "device that holds the loser
/// bytes" predicate: for a committed-but-unpublished head the §3.4
/// chokepoint wrote the file; for a published head the upload/download
/// lanes verified it. `PendingDown`/`Downloading`/`Stub`/
/// `CorruptRemote`/`Conflict` states do not prove the file matches the
/// head.
fn state_holds_local_bytes(state: ItemState) -> bool {
    matches!(
        state,
        ItemState::Dirty
            | ItemState::Queued
            | ItemState::Uploading
            | ItemState::Verifying
            | ItemState::Synced
            | ItemState::Hydrated
    )
}

/// The §2.6 case-4/convergence identity pick over the local head and the
/// arriving entry: `true` when the entry's `(ts, device)` wins
/// ([`pick_winner`]; a local head without recorded identity always
/// loses, deterministically — the entry carries a complete candidate).
/// Both devices of any exchange compare the same candidate pair, so the
/// outcome is fleet-deterministic whatever the clocks said.
fn remote_wins_identity(local: &ItemRecord, entry: &JournalEntry) -> bool {
    match (local.head_ts, &local.device) {
        (Some(ts), Some(device)) => {
            let winner = pick_winner(
                Candidate {
                    ts: entry.ts,
                    device: &entry.device,
                },
                Candidate { ts, device },
            );
            winner.ts == entry.ts && *winner.device == entry.device
        }
        _ => true,
    }
}

/// The converged-adoption (§2.6 case 1) head-identity rule: a version
/// ordering between the two converged spellings decides outright — a
/// strictly dominating entry IS the newer version, so its `(ts, device)`
/// is adopted whatever the clocks said (a tie-break here would let the
/// superseded identity win a same-second tie on the device that held it,
/// diverging from the author's own record); only genuinely concurrent
/// twins fall to the deterministic [`pick_winner`] over the shared
/// candidate pair.
fn converged_identity_is_remote(local: &ItemRecord, entry: &JournalEntry, ord: VvOrder) -> bool {
    match ord {
        VvOrder::Greater => true,
        VvOrder::Less => false,
        VvOrder::Equal | VvOrder::Concurrent => remote_wins_identity(local, entry),
    }
}

/// Streamed blake3 of a local file (the §2.8 displaced-copy identity;
/// never buffers the whole original).
fn hash_file(path: &Path) -> Result<Blake3Hex, std::io::Error> {
    use std::io::Read as _;
    let mut file = std::fs::File::open(path)?;
    let mut hasher = blake3::Hasher::new();
    let mut buf = vec![0u8; 256 * 1024];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            return Ok(Blake3Hex::from_hash(&hasher.finalize()));
        }
        hasher.update(&buf[..n]);
    }
}

/// [`EngineError::Io`] constructor.
fn io_err(path: &Path, source: std::io::Error) -> EngineError {
    EngineError::Io {
        path: path.to_path_buf(),
        source,
    }
}

// ---------------------------------------------------------------------------
// §2.5 local change intake
// ---------------------------------------------------------------------------

/// What a local scan observed about one file — the §2.5 change-detection
/// inputs (`size`, `mtime`, and byte access for the semantic/content
/// hash). Callers hand the engine the bytes they already hold (the §3.4
/// chokepoint has the sidecar document in hand; import has the original).
#[derive(Debug, Clone, Copy)]
pub struct LocalScan<'a> {
    /// File size in bytes.
    pub size: u64,
    /// File mtime, unix nanoseconds.
    pub mtime_unix_ns: i64,
    /// The file's bytes (sidecar document / original content).
    pub bytes: &'a [u8],
}

/// What [`notify_local_change`] decided.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeOutcome {
    /// No semantic change (§2.5 churn gate): byte rewrites with an
    /// unchanged `sem_hash` (sidecars) or unchanged content hash
    /// (originals) mark nothing, queue nothing, and bump nothing.
    Unchanged,
    /// A semantic change was recorded: the item is `Dirty` (created
    /// `Dirty` when no record existed) and awaits [`admit_pending`].
    MarkedDirty,
}

/// The scanned §2.5 change identity of one file.
enum ScanIdentity {
    /// Sidecar: semantic hash + the §2.2 badge projection.
    Sidecar(SemHash, crate::semhash::SidecarBadges),
    /// Original / xmp: content hash.
    Content(Blake3Hex, ContentId),
}

/// The items-table key `notify_local_change`/`delete_item` address for
/// `image`'s item of `kind` (module docs: the sidecar's `.rrdata` suffix
/// is part of the key; every other kind keys by the file's own relkey,
/// which for originals and the xmp projection the caller already names).
fn item_key_for_kind(image: &RelKey, kind: Kind) -> Result<RelKey, KeyError> {
    match kind {
        Kind::Sidecar => sidecar_item_relkey(image),
        _ => Ok(image.clone()),
    }
}

/// §2.5 local change intake for `image`'s item of `kind` (the §3.4
/// chokepoint's engine half).
///
/// Computes the change identity from `scan.bytes` — [`crate::semhash::sem_hash`]
/// for sidecars (an unparsable document is a typed error, never a
/// default), blake3/[`ContentId`] for originals — and compares it with
/// the item record:
///
/// - **No record**: create the item `Dirty` with the scanned facts, the
///   §2.2 badge fields ([`crate::semhash::sidecar_badges`]) for
///   sidecars, and an empty vv (the version is minted at admission).
/// - **Same identity** ([`ChangeOutcome::Unchanged`]): nothing is
///   written — no dirty mark, no queue, no vv movement; this is the
///   churn gate that makes EXIF-cache/auto-heal rewrites free.
/// - **Changed identity**: mark `Dirty` along the legal §2.4 edge from
///   the current state (`Synced`/`Hydrated` → `Dirty`;
///   `PendingDown → Dirty` records the §3.4 offline-edit lane, flagged
///   `base_unknown`; a `Stub` original whose bytes were replaced
///   out-of-band takes the §2.8 guarded
///   [`crate::state::SyncDb::replay_put_item_cas`] bypass, `Stub` →
///   `Dirty` with the new `content_id`), refresh the §2.2 badges
///   (sidecars) or the scanned `content_id` (originals), and leave the
///   record's `vv`/`blake3`/`sem_hash`/`size` advertising the last
///   published version per the §2.6 coordination note — admission owns
///   the version mint, and the upload lane re-derives the new version's
///   hashes from exactly the sent bytes. An already-`Dirty` item just
///   refreshes the scanned identity.
///
/// The relkey/kind pair addresses the item via the module's keying
/// convention (`kind == Sidecar` resolves to `<image>.rrdata`); a kind
/// disagreeing with an existing record is a typed
/// [`EngineError::KindMismatch`].
pub fn notify_local_change(
    db: &SyncDb,
    image: &RelKey,
    kind: Kind,
    scan: &LocalScan<'_>,
) -> Result<ChangeOutcome, EngineError> {
    let item = item_key_for_kind(image, kind)?;
    let identity = match kind {
        Kind::Sidecar => ScanIdentity::Sidecar(sem_hash(scan.bytes)?, sidecar_badges(scan.bytes)?),
        _ => {
            let b3 = Blake3Hex::from_bytes(scan.bytes);
            let cid = ContentId::from_blake3(&b3);
            ScanIdentity::Content(b3, cid)
        }
    };

    let Some(record) = db.get_item(&item)? else {
        // Fresh item: born Dirty with the scanned facts; the version is
        // minted at admission (§2.6), so the vv starts empty and blake3
        // stays None until the first verified upload records it.
        let (sem, badges, content_id) = match &identity {
            ScanIdentity::Sidecar(sem, badges) => (Some(sem.clone()), badges.clone(), None),
            ScanIdentity::Content(_, cid) => (None, Default::default(), Some(cid.clone())),
        };
        let fresh = ItemRecord {
            kind,
            state: ItemState::Dirty,
            size: scan.size,
            mtime_unix_ns: scan.mtime_unix_ns,
            blake3: None,
            sem_hash: sem,
            vv: VersionVector::new(),
            content_id,
            w: None,
            h: None,
            pinned: false,
            last_access_unix: 0,
            verified_remote: false,
            attested: false,
            base_unknown: false,
            rating: badges.rating,
            color_label: badges.color_label,
            device: None,
            head_ts: None,
            admitted_vv: None,
            deleted: false,
        };
        db.insert_item(&item, &fresh)?;
        return Ok(ChangeOutcome::MarkedDirty);
    };

    if record.kind != kind {
        return Err(EngineError::KindMismatch {
            relkey: item,
            given: kind,
            stored: record.kind,
        });
    }

    // §2.5 churn gate: identical identity writes nothing at all.
    let unchanged = match &identity {
        ScanIdentity::Sidecar(sem, _) => record.sem_hash.as_ref() == Some(sem),
        ScanIdentity::Content(b3, cid) => match (&record.content_id, &record.blake3) {
            (Some(stored), _) => stored == cid,
            (None, Some(stored)) => stored == b3,
            (None, None) => false,
        },
    };
    if unchanged {
        return Ok(ChangeOutcome::Unchanged);
    }

    // The scanned-identity refresh every dirty-marking lane applies: the
    // §2.2 badges follow the document immediately (advisory display
    // facts), the original's content_id follows the bytes (§2.8); the
    // integrity fields (vv/blake3/sem_hash/size) keep naming the last
    // published version (§2.6 coordination note).
    let refresh = |r: &mut ItemRecord| match &identity {
        ScanIdentity::Sidecar(_, badges) => {
            r.rating = badges.rating;
            r.color_label = badges.color_label.clone();
        }
        ScanIdentity::Content(_, cid) => {
            r.content_id = Some(cid.clone());
        }
    };
    match record.state {
        ItemState::Dirty => {
            db.update_item(&item, ItemState::Dirty, refresh)?;
        }
        state @ (ItemState::Synced | ItemState::Hydrated) => {
            db.transition(&item, state, ItemState::Dirty, refresh)?;
        }
        ItemState::PendingDown => {
            // §3.4 offline-edit lane: editing a not-yet-downloaded head.
            db.transition(&item, ItemState::PendingDown, ItemState::Dirty, |r| {
                refresh(r);
                r.base_unknown = true;
            })?;
        }
        ItemState::Stub => {
            // §2.8 out-of-band overwrite of an evicted original: the bytes
            // were replaced on disk, not downloaded, so there is no legal()
            // edge — the guarded replay CAS is the sanctioned bypass.
            let mut replaced = record.clone();
            replaced.state = ItemState::Dirty;
            refresh(&mut replaced);
            db.replay_put_item_cas(&item, ItemState::Stub, &replaced)?;
        }
        state => {
            // Pipeline-interior states (Queued/Uploading/Verifying/
            // Downloading/Conflict/CorruptRemote): the pipeline owns the
            // state — refresh the scanned identity only; the §2.4
            // completion recheck (or the next intake after the pipeline
            // settles) re-marks the item.
            db.update_item(&item, state, refresh)?;
        }
    }
    Ok(ChangeOutcome::MarkedDirty)
}

// ---------------------------------------------------------------------------
// §2.6 / §3.7 admission
// ---------------------------------------------------------------------------

/// §2.6 "one admitted upload = one version": admits every `Dirty` item
/// that passes the caller-supplied `quiesced` check (the §3.7
/// quiescence/debounce policy lives with the caller; a test passes
/// `|_, _| true`).
///
/// Per admitted item, in one committed transaction: `Dirty → Queued`,
/// `vv[self]` bumped and **snapshotted into the upload intent**
/// ([`crate::state::ItemRecord::admitted_vv`]; the record's own `vv`
/// keeps naming the last published version until the §2.4 verify-commit
/// promotes the snapshot), `head_ts` frozen to the engine's server-time
/// estimate and `device` to this db's identity (the §2.6 case-4
/// candidate the entry will advertise), and the item pushed onto the
/// upload queue ([`CLASS_SIDECAR`]/[`CLASS_ORIGINAL`]). The subsequent
/// [`crate::transfer::upload_item`] flows through
/// [`crate::transfer::commit_verified`], whose staged entry carries
/// exactly the admitted `(vv, ts)` plus the record's `rating`/
/// `color_label` — and always a `blake3` (module docs).
///
/// Returns the admitted item relkeys in admission order.
pub fn admit_pending(
    db: &SyncDb,
    mut quiesced: impl FnMut(&RelKey, &ItemRecord) -> bool,
) -> Result<Vec<RelKey>, EngineError> {
    let own = db.device_id().clone();
    let now = crate::transfer::server_ts_estimate(db)?;
    let mut admitted = Vec::new();
    for (relkey, record) in db.items_in_state(ItemState::Dirty)? {
        // A hidden (§2.7 soft-deleted) item never uploads; restore first.
        if record.deleted || !quiesced(&relkey, &record) {
            continue;
        }
        let class = transfer_class(record.kind);
        db.with_txn(|t| {
            t.transition(&relkey, ItemState::Dirty, ItemState::Queued, |r| {
                let mut vv = r.vv.clone();
                vv.bump(&own);
                r.admitted_vv = Some(vv);
                r.head_ts = Some(now);
                r.device = Some(own.clone());
            })?;
            t.queue_push(Queue::Up, &relkey, class)?;
            Ok(())
        })?;
        admitted.push(relkey);
    }
    Ok(admitted)
}

// ---------------------------------------------------------------------------
// Events (§3.8, engine subset — a trait, no channel dependency)
// ---------------------------------------------------------------------------

/// §2.6 case-4 resolution event (`sync-conflict`): `relkey` is the
/// conflicted **image**, `winner_device` authored the version that
/// became the primary, and `copy_relkey` is the loser's deterministic
/// virtual-copy item — `Some` on a device that holds (and therefore
/// materialized, or can name) the loser document, `None` on a device
/// that holds only the winner (§2.6: it skips materialization and
/// learns the vc key from its author's journal entry).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConflictEvent {
    /// The conflicted image relkey.
    pub relkey: RelKey,
    /// The device whose version became the primary.
    pub winner_device: DeviceId,
    /// The loser's vc item relkey, when this device can name it.
    pub copy_relkey: Option<RelKey>,
}

/// §2.7 edge event: a concurrent `del` resurrected the sidecar, but the
/// original could not be re-advertised because this device never knew
/// it (no record / no `blake3`) — the original stays deleted-side until
/// a holder re-advertises it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResurrectionIncompleteEvent {
    /// The image whose original resurrection was skipped.
    pub relkey: RelKey,
}

/// §2.8 original-overwrite event: a remote `put original` concurrent
/// with our known different content won the library key; our displaced
/// bytes were staged at the deterministic conflict relkey.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OriginalConflictEvent {
    /// The overwritten image relkey.
    pub relkey: RelKey,
    /// The deterministic conflict-copy item holding the displaced bytes.
    pub conflict_relkey: RelKey,
    /// Content identity of the displaced bytes.
    pub displaced_content_id: ContentId,
}

/// The engine's event sink (§3.8's `sync-conflict`/`sync-error` family,
/// as a small trait so the core carries no channel dependency; the app
/// side adapts it onto `app_handle.emit`).
pub trait EngineEvents {
    /// A §2.6 case-4 conflict was resolved.
    fn conflict(&mut self, event: ConflictEvent);
    /// A §2.7 resurrection could not cover the original.
    fn resurrection_incomplete(&mut self, event: ResurrectionIncompleteEvent);
    /// A §2.8 original-overwrite conflict was resolved.
    fn original_conflict(&mut self, event: OriginalConflictEvent);
}

/// The no-op sink (callers that do not care).
impl EngineEvents for () {
    fn conflict(&mut self, _event: ConflictEvent) {}
    fn resurrection_incomplete(&mut self, _event: ResurrectionIncompleteEvent) {}
    fn original_conflict(&mut self, _event: OriginalConflictEvent) {}
}

// ---------------------------------------------------------------------------
// Always-blake3 engine puts (module docs; §2.6/§2.7 metadata-only entries)
// ---------------------------------------------------------------------------

/// A metadata-only `put` the engine stages directly (restore,
/// resurrection, re-advertisement) — **constructible only with a
/// [`Blake3Hex`]**: the `blake3` field is not optional, so an engine
/// put without a content hash is a compile error, closing the U3-noted
/// blake3-less-put corner at the type level (pinned by scenario S8).
/// Upload-journaled puts get the same guarantee from the §2.4 streamed
/// hash.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnginePut {
    /// Authoring device (must be the staging db's own identity).
    pub device: DeviceId,
    /// Object kind.
    pub kind: Kind,
    /// The item's file relkey (module docs; the entry key is its
    /// [`crate::keys::library_key`]).
    pub item: RelKey,
    /// The advertised version vector.
    pub vv: VersionVector,
    /// blake3 of the advertised bytes. **Required** — see the type docs.
    pub blake3: Blake3Hex,
    /// Advertised object size.
    pub size: u64,
    /// Entry timestamp (server-time estimate; the §2.6 tiebreak input).
    pub ts: i64,
    /// Semantic hash (sidecar puts).
    pub sem_hash: Option<SemHash>,
    /// §2.2 badge: star rating (sidecar puts).
    pub rating: Option<u8>,
    /// §2.2 badge: color label (sidecar puts).
    pub color_label: Option<String>,
    /// Content identity (original puts).
    pub content_id: Option<ContentId>,
    /// Measured width (original puts, when known).
    pub w: Option<u32>,
    /// Measured height (original puts, when known).
    pub h: Option<u32>,
    /// File mtime, unix seconds (original puts, when known).
    pub mtime: Option<i64>,
}

impl EnginePut {
    /// The v1 journal entry this put stages: `op: put`, `key` =
    /// [`crate::keys::library_key`] of `item`, `seq` 0 (stamped at
    /// publication), and `blake3` **always** `Some` — by construction
    /// from the required field.
    pub fn entry(&self) -> JournalEntry {
        JournalEntry {
            v: JOURNAL_VERSION,
            seq: 0,
            ts: self.ts,
            device: self.device.clone(),
            op: Op::Put,
            kind: self.kind,
            key: library_key(&self.item),
            vv: self.vv.clone(),
            size: Some(self.size),
            blake3: Some(self.blake3.clone()),
            sem_hash: self.sem_hash.clone(),
            rating: self.rating,
            color_label: self.color_label.clone(),
            content_id: self.content_id.clone(),
            w: self.w,
            h: self.h,
            mtime: self.mtime,
            from_key: None,
        }
    }
}

// ---------------------------------------------------------------------------
// §2.6/§2.7 remote apply: the EngineConsumer
// ---------------------------------------------------------------------------

/// The §2.6 unified apply rule as a [`JournalConsumer`] (module docs) —
/// the consumer the caller hands to [`crate::reader::poll`] and
/// [`crate::manifest::merge`], so journal replay and manifest merge
/// resolve through one rule.
///
/// Mutations run through the [`StateTxn`] the reader hands over, so a
/// consumer effect commits atomically with the §2.2 applied mark; local
/// side files (loser materialization) are written idempotently before
/// the commit, so a crash replays them byte-identically. Entries whose
/// `key` does not classify into the library namespace, and ops/kinds
/// this unit does not handle, are **skips**, never errors (the
/// [`JournalConsumer`] error contract). Events fire as the resolution
/// happens, before the surrounding transaction commits: a crash between
/// the two re-fires them on replay (at-least-once), which is the §3.8
/// family's contract anyway.
pub struct EngineConsumer<'a, E: EngineEvents> {
    own_device: DeviceId,
    sync_root: PathBuf,
    events: &'a mut E,
    now_unix: i64,
}

impl<'a, E: EngineEvents> EngineConsumer<'a, E> {
    /// A consumer applying into `db`'s state under the module's rules:
    /// snapshots the db's identity and server-time estimate (local clock
    /// plus the persisted §2.10 offset) for the pass; `sync_root` is
    /// where loser bytes are materialized from/to.
    pub fn new(
        db: &SyncDb,
        sync_root: impl Into<PathBuf>,
        events: &'a mut E,
    ) -> Result<Self, EngineError> {
        let now_unix = crate::transfer::server_ts_estimate(db)?;
        Ok(EngineConsumer {
            own_device: db.device_id().clone(),
            sync_root: sync_root.into(),
            events,
            now_unix,
        })
    }

    /// Test hook: pins the pass's "now" (the `ts`/`head_ts` stamped on
    /// in-consumer admissions and resurrection puts) instead of the
    /// constructor's estimate, making §2.6 tiebreaks deterministic in
    /// unit tests.
    pub fn with_now(mut self, now_unix: i64) -> Self {
        self.now_unix = now_unix;
        self
    }

    /// The typed-error body of [`JournalConsumer::apply`]; the trait impl
    /// boxes the result.
    fn apply_inner(&mut self, txn: &StateTxn<'_>, entry: &JournalEntry) -> Result<(), EngineError> {
        if !matches!(entry.op, Op::Put | Op::Del) {
            // move/attest ride later units; skip (never Err).
            return Ok(());
        }
        let class = classify_key(&entry.key);
        let (kind, image) = match &class {
            KeyClass::Original { relkey } => (Kind::Original, relkey.clone()),
            KeyClass::Sidecar { relkey, .. } => (Kind::Sidecar, relkey.clone()),
            KeyClass::Xmp { relkey } => (Kind::Xmp, relkey.clone()),
            // preview/thumb/thumbpack/albums/presets keys (download policy
            // is P2), control-plane keys and foreign/unclassifiable keys:
            // recorded as applied with no state effects.
            _ => return Ok(()),
        };
        if entry.kind != kind {
            // A key whose schema role disagrees with the entry's declared
            // kind is a buggy/hostile writer: content-level skip.
            return Ok(());
        }
        let Some(item) = item_relkey_for(&class) else {
            return Ok(());
        };
        match entry.op {
            Op::Put => self.apply_put(txn, entry, kind, &image, &item),
            Op::Del => self.apply_del(txn, entry, kind, &image, &item),
            _ => Ok(()),
        }
    }

    // -- put ----------------------------------------------------------------

    /// The §2.6 unified apply rule for one `put` (module docs).
    fn apply_put(
        &mut self,
        txn: &StateTxn<'_>,
        entry: &JournalEntry,
        kind: Kind,
        image: &RelKey,
        item: &RelKey,
    ) -> Result<(), EngineError> {
        let Some(mut local) = txn.get_item(item)? else {
            return self.create_from_put(txn, entry, kind, image, item);
        };
        if local.kind != kind {
            return Ok(());
        }

        // Local-commit-before-compare (§2.6 case 2's dirty clause):
        // uncommitted dirty edits are first committed as a local version —
        // unless the local FILE already holds the entry's content (the
        // §2.5 "same test applied to downloaded sidecars": converged, the
        // dirt collapses with no upload and no version).
        if local.state == ItemState::Dirty && local.admitted_vv.is_none() {
            if compare(&entry.vv, &local.vv) == VvOrder::Less {
                return Ok(()); // an ancestor of our base: our dirt supersedes it
            }
            let file_converged = match kind {
                Kind::Sidecar => {
                    entry.sem_hash.is_some() && entry.sem_hash == self.local_file_sem(item)
                }
                _ => entry.content_id.is_some() && entry.content_id == local.content_id,
            };
            if file_converged {
                return self.converge_dirty(txn, entry, image, item, &local);
            }
            if compare(&entry.vv, &local.vv) == VvOrder::Equal {
                return Ok(()); // the remote re-advertised our base; dirt stays
            }
            local = self.commit_dirty(txn, item, &local)?;
        }

        // The local head: the committed-but-unpublished version when one
        // is in flight (its admission snapshot), else the published vv.
        let head_vv = local
            .admitted_vv
            .clone()
            .unwrap_or_else(|| local.vv.clone());
        if compare(&entry.vv, &head_vv) == VvOrder::Equal {
            return Ok(()); // the identical version: nothing moves
        }
        let content_equal = match kind {
            Kind::Sidecar => entry.sem_hash.is_some() && entry.sem_hash == local.sem_hash,
            _ => {
                (entry.blake3.is_some() && entry.blake3 == local.blake3)
                    || (entry.content_id.is_some() && entry.content_id == local.content_id)
            }
        };
        if content_equal {
            let ord = compare(&entry.vv, &head_vv);
            return self.converge(txn, entry, image, item, &local, ord);
        }
        match compare(&entry.vv, &head_vv) {
            VvOrder::Less | VvOrder::Equal => Ok(()), // case 3 (Equal handled above)
            VvOrder::Greater => self.adopt_remote(txn, entry, image, item, &local, kind),
            VvOrder::Concurrent => self.resolve_concurrent(txn, entry, image, item, &local, kind),
        }
    }

    /// Case 2 for an unknown item: create it `PendingDown` with the
    /// entry's facts — hidden instead when a recorded deletion still
    /// dominates it (§2.7 ordering holds in every arrival order).
    fn create_from_put(
        &mut self,
        txn: &StateTxn<'_>,
        entry: &JournalEntry,
        kind: Kind,
        image: &RelKey,
        item: &RelKey,
    ) -> Result<(), EngineError> {
        let row = txn.get_deleted(image)?;
        let dominates_row = row
            .as_ref()
            .is_none_or(|r| compare(&entry.vv, &r.vv) == VvOrder::Greater);
        let record = ItemRecord {
            kind,
            state: ItemState::PendingDown,
            size: entry.size.unwrap_or(0),
            mtime_unix_ns: entry.mtime.unwrap_or(0).saturating_mul(1_000_000_000),
            blake3: entry.blake3.clone(),
            sem_hash: entry.sem_hash.clone(),
            vv: entry.vv.clone(),
            content_id: entry.content_id.clone(),
            w: entry.w,
            h: entry.h,
            pinned: false,
            last_access_unix: 0,
            verified_remote: false,
            attested: false,
            base_unknown: false,
            rating: entry.rating,
            color_label: entry.color_label.clone(),
            device: Some(entry.device.clone()),
            head_ts: Some(entry.ts),
            admitted_vv: None,
            deleted: !dominates_row,
        };
        txn.insert_item(item, &record)?;
        if dominates_row {
            if row.is_some() {
                txn.remove_deleted(image)?;
            }
            // xmp download policy is P2: metadata recording only.
            if kind != Kind::Xmp {
                txn.queue_push(Queue::Down, item, transfer_class(kind))?;
            }
        }
        Ok(())
    }

    /// Case 1 over uncommitted dirt: the local file already holds the
    /// entry's content, so the dirt collapses — adopt the entry's
    /// metadata, no upload, no version mint (the documented
    /// `Dirty → Synced` replay-CAS row).
    fn converge_dirty(
        &mut self,
        txn: &StateTxn<'_>,
        entry: &JournalEntry,
        image: &RelKey,
        item: &RelKey,
        local: &ItemRecord,
    ) -> Result<(), EngineError> {
        let mut record = local.clone();
        // Originals settle onto the §3.5 hydration axis (the local file
        // holds the converged bytes — see `converge`'s normalization
        // note); sidecars/meta land `Synced`.
        record.state = if local.kind == Kind::Original {
            ItemState::Hydrated
        } else {
            ItemState::Synced
        };
        record.vv.merge(&entry.vv);
        record.blake3 = entry.blake3.clone();
        record.sem_hash = entry.sem_hash.clone();
        record.size = entry.size.unwrap_or(local.size);
        record.rating = entry.rating;
        record.color_label = entry.color_label.clone();
        if entry.content_id.is_some() {
            record.content_id = entry.content_id.clone();
        }
        let ord = compare(&entry.vv, &local.vv);
        if converged_identity_is_remote(local, entry, ord) {
            record.head_ts = Some(entry.ts);
            record.device = Some(entry.device.clone());
        }
        record.admitted_vv = None;
        if local.deleted && compare(&entry.vv, &local.vv) == VvOrder::Greater {
            record.deleted = false;
        }
        txn.replay_put_item_cas(item, ItemState::Dirty, &record)?;
        self.clear_superseded_row(txn, &entry.vv, image)
    }

    /// Case 1 for a committed/clean local head: same content under a
    /// different vv — vv max-merge, deterministic head-identity
    /// convergence ([`converged_identity_is_remote`]), local bytes stay
    /// authoritative, no transfer.
    fn converge(
        &mut self,
        txn: &StateTxn<'_>,
        entry: &JournalEntry,
        image: &RelKey,
        item: &RelKey,
        local: &ItemRecord,
        ord: VvOrder,
    ) -> Result<(), EngineError> {
        let undelete = local.deleted && compare(&entry.vv, &local.vv) == VvOrder::Greater;
        let adopt_identity = converged_identity_is_remote(local, entry, ord);
        let mutate = |r: &mut ItemRecord| {
            r.vv.merge(&entry.vv);
            if adopt_identity {
                r.head_ts = Some(entry.ts);
                r.device = Some(entry.device.clone());
            }
            if undelete {
                r.deleted = false;
            }
        };
        if local.kind == Kind::Original && local.state == ItemState::Synced {
            // §3.5 axis normalization: for an original, the upload
            // pipeline's `Synced` terminal and `Hydrated` name the same
            // physical fact (bytes present + verified locally). The
            // converged adoption settles the record onto the hydration
            // axis, so eviction/hydration policy (P2) has one state to
            // reason about; `legal()` deliberately carries no
            // `Synced → Hydrated` edge (it is not a pipeline step), so
            // this takes the documented replay-CAS lane.
            let mut record = local.clone();
            mutate(&mut record);
            record.state = ItemState::Hydrated;
            txn.replay_put_item_cas(item, ItemState::Synced, &record)?;
        } else {
            txn.update_item(item, local.state, mutate)?;
        }
        self.clear_superseded_row(txn, &entry.vv, image)
    }

    /// Case 2 (and the case-4 remote-winner half): the record adopts the
    /// entry's facts and the item heads for the download lane —
    /// `PendingDown` plus a queue row for sidecars and held originals;
    /// `Stub` originals adopt metadata only (hydration is on-demand,
    /// P2), and xmp items are metadata-only at this unit.
    fn adopt_remote(
        &mut self,
        txn: &StateTxn<'_>,
        entry: &JournalEntry,
        image: &RelKey,
        item: &RelKey,
        local: &ItemRecord,
        kind: Kind,
    ) -> Result<(), EngineError> {
        let undelete = local.deleted && compare(&entry.vv, &local.vv) == VvOrder::Greater;
        let had_intent = local.admitted_vv.is_some();
        let adopt = |r: &mut ItemRecord| {
            // §2.6: the path's vv becomes the elementwise max of BOTH
            // heads. A withdrawn committed-local head (its admitted
            // snapshot) folds in too: that version lives on as the vc,
            // and the next edit on this path must dominate it.
            if let Some(admitted) = &local.admitted_vv {
                r.vv.merge(admitted);
            }
            r.vv.merge(&entry.vv);
            r.blake3 = entry.blake3.clone();
            r.sem_hash = entry.sem_hash.clone();
            r.size = entry.size.unwrap_or(0);
            r.mtime_unix_ns = entry.mtime.unwrap_or(0).saturating_mul(1_000_000_000);
            r.rating = entry.rating;
            r.color_label = entry.color_label.clone();
            r.content_id = entry.content_id.clone();
            r.w = entry.w;
            r.h = entry.h;
            r.device = Some(entry.device.clone());
            r.head_ts = Some(entry.ts);
            // The in-flight upload intent (if any) is withdrawn: the
            // version it named lost its claim to the primary key.
            r.admitted_vv = None;
            // Integrity facts described the superseded version.
            r.verified_remote = false;
            r.attested = false;
            r.base_unknown = false;
            if undelete {
                r.deleted = false;
            }
        };
        use ItemState::*;
        let mut fetch = false;
        match (kind, local.state) {
            // xmp: §2.8 projection — metadata recording only at this unit.
            (Kind::Xmp, state) => {
                txn.update_item(item, state, adopt)?;
            }
            // Evicted original: adopt metadata, stay a stub (P2 hydrates).
            (_, Stub) => {
                txn.update_item(item, Stub, adopt)?;
            }
            // A live download slot owns the state; it re-verifies against
            // the adopted facts when it settles.
            (_, Downloading) => {
                txn.update_item(item, Downloading, adopt)?;
            }
            (_, PendingDown) => {
                txn.update_item(item, PendingDown, adopt)?;
                fetch = true;
            }
            (_, Synced) => {
                txn.transition(item, Synced, PendingDown, adopt)?;
                fetch = true;
            }
            (_, Hydrated) => {
                txn.transition(item, Hydrated, PendingDown, adopt)?;
                fetch = true;
            }
            (_, CorruptRemote) => {
                txn.transition(item, CorruptRemote, PendingDown, adopt)?;
                fetch = true;
            }
            (_, Conflict) => {
                txn.transition(item, Conflict, PendingDown, adopt)?;
                fetch = true;
            }
            // Committed upload lanes: the §2.4 table routes them through
            // Conflict (a concurrent remote version met the committed
            // one) and out toward the fetch.
            (_, state @ (Dirty | Queued | Uploading | Verifying)) => {
                txn.transition(item, state, Conflict, |_| {})?;
                txn.transition(item, Conflict, PendingDown, adopt)?;
                fetch = true;
            }
        }
        if had_intent {
            txn.queue_remove(Queue::Up, item)?;
        }
        if fetch {
            txn.queue_push(Queue::Down, item, transfer_class(kind))?;
        }
        self.clear_superseded_row(txn, &entry.vv, image)
    }

    /// Case 4: deterministic winner, loser preservation (module docs).
    fn resolve_concurrent(
        &mut self,
        txn: &StateTxn<'_>,
        entry: &JournalEntry,
        image: &RelKey,
        item: &RelKey,
        local: &ItemRecord,
        kind: Kind,
    ) -> Result<(), EngineError> {
        if kind == Kind::Xmp {
            // §2.8: the xmp is a projection of the sidecar, never a
            // conflict domain — the §2.6 pick decides which metadata the
            // record carries; no vc, no event, no transfer.
            if remote_wins_identity(local, entry) {
                return self.adopt_remote(txn, entry, image, item, local, kind);
            }
            txn.update_item(item, local.state, |r| r.vv.merge(&entry.vv))?;
            return Ok(());
        }
        if remote_wins_identity(local, entry) {
            // The LOCAL version is the loser: preserve it first (we are a
            // holder of its bytes whenever our state proves the file is
            // the head), then adopt the winner at the primary.
            let copy = match kind {
                Kind::Sidecar => self.materialize_sidecar_loser(txn, image, item, local)?,
                _ => self.stage_displaced_original(txn, image, item, local)?,
            };
            self.adopt_remote(txn, entry, image, item, local, kind)?;
            if kind == Kind::Sidecar {
                self.events.conflict(ConflictEvent {
                    relkey: image.clone(),
                    winner_device: entry.device.clone(),
                    copy_relkey: copy,
                });
            }
        } else {
            // The REMOTE version is the loser: its holders materialize it
            // (we hold only the winner) — the path still folds both
            // branches so the conflict cannot reopen.
            txn.update_item(item, local.state, |r| r.vv.merge(&entry.vv))?;
            if kind == Kind::Sidecar {
                let winner_device = local
                    .device
                    .clone()
                    .unwrap_or_else(|| self.own_device.clone());
                self.events.conflict(ConflictEvent {
                    relkey: image.clone(),
                    winner_device,
                    copy_relkey: None,
                });
            }
        }
        Ok(())
    }

    // -- del ----------------------------------------------------------------

    /// §2.7 del ordering: dominate → hide; ancestor → ignore; concurrent
    /// (or any non-ancestor meeting uncommitted dirt) → edits beat
    /// deletes.
    fn apply_del(
        &mut self,
        txn: &StateTxn<'_>,
        entry: &JournalEntry,
        kind: Kind,
        image: &RelKey,
        item: &RelKey,
    ) -> Result<(), EngineError> {
        let Some(local) = txn.get_item(item)? else {
            // Unknown item: remember the deletion so a slower put cannot
            // resurrect it out of order (§2.7; the row merges with any
            // prior knowledge so the §2.7 anchor survives arrival order).
            self.record_deletion_row(txn, image, &entry.vv, entry.ts)?;
            return Ok(());
        };
        if local.kind != kind {
            return Ok(());
        }

        // §2.7 edits-beat-deletes: a del meeting UNCOMMITTED dirty edits
        // resurrects — the dirt commits as a local version whose vv
        // dominates the del, and (for the sidecar) the image's original
        // is re-advertised so the whole item survives.
        if local.state == ItemState::Dirty && local.admitted_vv.is_none() {
            if compare(&entry.vv, &local.vv) == VvOrder::Less {
                return Ok(()); // the del predates our base; superseded
            }
            let own = self.own_device.clone();
            let now = self.now_unix;
            let mut admitted = local.vv.clone();
            admitted.merge(&entry.vv);
            admitted.bump(&own);
            let admitted_snapshot = admitted.clone();
            txn.transition(item, ItemState::Dirty, ItemState::Queued, |r| {
                r.admitted_vv = Some(admitted_snapshot);
                r.head_ts = Some(now);
                r.device = Some(own.clone());
                r.deleted = false;
            })?;
            txn.queue_push(Queue::Up, item, transfer_class(local.kind))?;
            if kind == Kind::Sidecar {
                self.resurrect_original(txn, entry, image)?;
            }
            return self.clear_superseded_row(txn, &admitted, image);
        }

        let head_vv = local
            .admitted_vv
            .clone()
            .unwrap_or_else(|| local.vv.clone());
        match compare(&entry.vv, &head_vv) {
            // Our version supersedes (or equals) the deletion: a
            // superseded del changes nothing; an equal one is the
            // already-applied deletion re-delivered (manifest merge).
            VvOrder::Less | VvOrder::Equal => Ok(()),
            VvOrder::Greater => {
                // Dominating del: hide — the record survives whole (§2.7),
                // the transfer lanes let go of it, and the deletion is
                // recorded for the §2.3 deleted set.
                txn.update_item(item, local.state, |r| {
                    r.vv.merge(&entry.vv);
                    r.deleted = true;
                    r.device = Some(entry.device.clone());
                    r.head_ts = Some(entry.ts);
                    r.admitted_vv = None;
                })?;
                txn.queue_remove(Queue::Up, item)?;
                txn.queue_remove(Queue::Down, item)?;
                self.record_deletion_row(txn, image, &entry.vv, entry.ts)
            }
            // Concurrent with our committed/published head: edits beat
            // deletes (§2.7) — the item stays live; our version reaches
            // the deleter through our journal, and only a del dominating
            // it could hide the item again.
            VvOrder::Concurrent => Ok(()),
        }
    }

    /// The §2.7 whole-item resurrection half: re-advertise the image's
    /// original with a vv dominating the del — metadata-only, the bytes
    /// are still in the bucket during grace — or surface
    /// [`ResurrectionIncompleteEvent`] when this device never knew the
    /// original (no record / no `blake3`).
    fn resurrect_original(
        &mut self,
        txn: &StateTxn<'_>,
        del_entry: &JournalEntry,
        image: &RelKey,
    ) -> Result<(), EngineError> {
        let known = txn.get_item(image)?;
        let Some(original) = known.filter(|r| r.kind == Kind::Original) else {
            self.events
                .resurrection_incomplete(ResurrectionIncompleteEvent {
                    relkey: image.clone(),
                });
            return Ok(());
        };
        let Some(blake3) = original.blake3.clone() else {
            self.events
                .resurrection_incomplete(ResurrectionIncompleteEvent {
                    relkey: image.clone(),
                });
            return Ok(());
        };
        let own = self.own_device.clone();
        let now = self.now_unix;
        let mut vv = original.vv.clone();
        vv.merge(&del_entry.vv);
        vv.bump(&own);
        let put = EnginePut {
            device: own.clone(),
            kind: Kind::Original,
            item: image.clone(),
            vv: vv.clone(),
            blake3,
            size: original.size,
            ts: now,
            sem_hash: None,
            rating: None,
            color_label: None,
            content_id: original.content_id.clone(),
            w: original.w,
            h: original.h,
            mtime: Some(original.mtime_unix_ns.div_euclid(1_000_000_000)),
        };
        enqueue_entry_in(txn, &own, &put.entry())?;
        txn.update_item(image, original.state, |r| {
            r.vv = vv.clone();
            r.deleted = false;
            r.head_ts = Some(now);
            r.device = Some(own.clone());
        })?;
        Ok(())
    }

    // -- shared pieces -------------------------------------------------------

    /// Semantic hash of the local file backing `item`, when it exists
    /// and parses (the §2.6 dirty-convergence probe).
    fn local_file_sem(&self, item: &RelKey) -> Option<SemHash> {
        let path = item_local_path(&self.sync_root, item);
        let bytes = std::fs::read(path).ok()?;
        sem_hash(&bytes).ok()
    }

    /// §2.6 loser materialization: when this device's state proves the
    /// local file holds the losing head, the loser document is written
    /// to its deterministic vc path (idempotent — byte-identical on
    /// every holder) and, unless the vc key is already known (`(key,
    /// sem_hash)` apply dedup), staged for upload with a fresh
    /// single-component vv through the ordinary admission lane. Returns
    /// the vc item relkey when this device can name it.
    fn materialize_sidecar_loser(
        &mut self,
        txn: &StateTxn<'_>,
        image: &RelKey,
        item: &RelKey,
        local: &ItemRecord,
    ) -> Result<Option<RelKey>, EngineError> {
        if !state_holds_local_bytes(local.state) {
            return Ok(None);
        }
        let path = item_local_path(&self.sync_root, item);
        let Ok(bytes) = std::fs::read(&path) else {
            return Ok(None); // not actually held
        };
        // An unparsable local document cannot name a deterministic vc
        // key: content-level skip (the §3.4 corruption guard owns local
        // corruption; the loser's author still materializes its copy).
        let Ok(suffix) = loser_vc_suffix(&bytes) else {
            return Ok(None);
        };
        let Ok(sem) = sem_hash(&bytes) else {
            return Ok(None);
        };
        let vc_rel = vc_item_relkey(image, &suffix)?;
        let vc_path = item_local_path(&self.sync_root, &vc_rel);
        if let Some(parent) = vc_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| io_err(&vc_path, e))?;
        }
        std::fs::write(&vc_path, &bytes).map_err(|e| io_err(&vc_path, e))?;
        if txn.get_item(&vc_rel)?.is_some() {
            // Apply dedup by (key, sem_hash): the vc is already known —
            // our own earlier materialization, or the peer's advertisement
            // (which the ordinary put apply converges with ours).
            return Ok(Some(vc_rel));
        }
        let badges = sidecar_badges(&bytes).unwrap_or_default();
        let own = self.own_device.clone();
        let record = ItemRecord {
            kind: Kind::Sidecar,
            state: ItemState::Dirty,
            size: bytes.len() as u64,
            mtime_unix_ns: 0,
            blake3: None,
            sem_hash: Some(sem),
            vv: VersionVector::new(),
            content_id: None,
            w: None,
            h: None,
            pinned: false,
            last_access_unix: 0,
            verified_remote: false,
            attested: false,
            base_unknown: false,
            rating: badges.rating,
            color_label: badges.color_label,
            device: Some(own.clone()),
            head_ts: Some(self.now_unix),
            admitted_vv: None,
            deleted: false,
        };
        txn.insert_item(&vc_rel, &record)?;
        txn.transition(&vc_rel, ItemState::Dirty, ItemState::Queued, |r| {
            let mut fresh = VersionVector::new();
            fresh.bump(&own);
            r.admitted_vv = Some(fresh);
        })?;
        txn.queue_push(Queue::Up, &vc_rel, CLASS_SIDECAR)?;
        Ok(Some(vc_rel))
    }

    /// §2.8 displaced-bytes staging: when this device holds the losing
    /// original's bytes, they are copied to the deterministic conflict
    /// relkey (idempotent), staged for upload with a fresh
    /// single-component vv, and the [`OriginalConflictEvent`] fires.
    fn stage_displaced_original(
        &mut self,
        txn: &StateTxn<'_>,
        image: &RelKey,
        item: &RelKey,
        local: &ItemRecord,
    ) -> Result<Option<RelKey>, EngineError> {
        if !state_holds_local_bytes(local.state) {
            return Ok(None);
        }
        let path = item_local_path(&self.sync_root, item);
        if !path.is_file() {
            return Ok(None); // not actually held
        }
        let blake3 = hash_file(&path).map_err(|e| io_err(&path, e))?;
        let size = std::fs::metadata(&path)
            .map_err(|e| io_err(&path, e))?
            .len();
        let displaced = ContentId::from_blake3(&blake3);
        let conflict_rel = original_conflict_relkey(image, &displaced)?;
        let conflict_path = item_local_path(&self.sync_root, &conflict_rel);
        if let Some(parent) = conflict_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| io_err(&conflict_path, e))?;
        }
        std::fs::copy(&path, &conflict_path).map_err(|e| io_err(&conflict_path, e))?;
        if txn.get_item(&conflict_rel)?.is_none() {
            let own = self.own_device.clone();
            let record = ItemRecord {
                kind: Kind::Original,
                state: ItemState::Dirty,
                size,
                mtime_unix_ns: local.mtime_unix_ns,
                blake3: None,
                sem_hash: None,
                vv: VersionVector::new(),
                content_id: Some(displaced.clone()),
                w: local.w,
                h: local.h,
                pinned: false,
                last_access_unix: 0,
                verified_remote: false,
                attested: false,
                base_unknown: false,
                rating: None,
                color_label: None,
                device: Some(own.clone()),
                head_ts: Some(self.now_unix),
                admitted_vv: None,
                deleted: false,
            };
            txn.insert_item(&conflict_rel, &record)?;
            txn.transition(&conflict_rel, ItemState::Dirty, ItemState::Queued, |r| {
                let mut fresh = VersionVector::new();
                fresh.bump(&own);
                r.admitted_vv = Some(fresh);
            })?;
            txn.queue_push(Queue::Up, &conflict_rel, CLASS_ORIGINAL)?;
        }
        self.events.original_conflict(OriginalConflictEvent {
            relkey: image.clone(),
            conflict_relkey: conflict_rel.clone(),
            displaced_content_id: displaced,
        });
        Ok(Some(conflict_rel))
    }

    /// Records (or vv-max-merges into) the §2.3 deleted-set row for
    /// `image` — merging keeps the §2.7 tombstone anchor (the sidecar
    /// del's vv) regardless of the per-kind dels' arrival order.
    fn record_deletion_row(
        &self,
        txn: &StateTxn<'_>,
        image: &RelKey,
        vv: &VersionVector,
        server_ts: i64,
    ) -> Result<(), EngineError> {
        let merged = match txn.get_deleted(image)? {
            Some(mut row) => {
                row.vv.merge(vv);
                DeletedRecord {
                    vv: row.vv,
                    server_ts: row.server_ts.max(server_ts),
                }
            }
            None => DeletedRecord {
                vv: vv.clone(),
                server_ts,
            },
        };
        txn.record_deleted(image, &merged)?;
        Ok(())
    }

    /// Withdraws this device's deleted-set row for `image` when `vv`
    /// strictly dominates it (the deletion was superseded by a restore
    /// or resurrection, §2.7).
    fn clear_superseded_row(
        &self,
        txn: &StateTxn<'_>,
        vv: &VersionVector,
        image: &RelKey,
    ) -> Result<(), EngineError> {
        if let Some(row) = txn.get_deleted(image)? {
            if compare(vv, &row.vv) == VvOrder::Greater {
                txn.remove_deleted(image)?;
            }
        }
        Ok(())
    }

    /// The §2.6 local-commit path shared by the case-2 dirty clause:
    /// `Dirty → Queued` with `admitted_vv` = local vv + `vv[self]` bump,
    /// `head_ts` frozen at the pass's now, `device` = self.
    fn commit_dirty(
        &mut self,
        txn: &StateTxn<'_>,
        item: &RelKey,
        local: &ItemRecord,
    ) -> Result<ItemRecord, EngineError> {
        let own = self.own_device.clone();
        let now = self.now_unix;
        let record = txn.transition(item, ItemState::Dirty, ItemState::Queued, |r| {
            let mut vv = r.vv.clone();
            vv.bump(&own);
            r.admitted_vv = Some(vv);
            r.head_ts = Some(now);
            r.device = Some(own.clone());
        })?;
        txn.queue_push(Queue::Up, item, transfer_class(local.kind))?;
        Ok(record)
    }
}

impl<E: EngineEvents> JournalConsumer for EngineConsumer<'_, E> {
    fn apply(&mut self, txn: &StateTxn<'_>, entry: &JournalEntry) -> Result<(), ConsumerError> {
        self.apply_inner(txn, entry).map_err(ConsumerError::from)
    }
}

// ---------------------------------------------------------------------------
// §2.7 local delete / restore
// ---------------------------------------------------------------------------

/// What [`delete_item`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteOutcome {
    /// The tombstone document as PUT to
    /// [`crate::keys::tombstone_key`] (`relkey` strict-wire NFC by
    /// [`RelKey`] construction; `vv` = the sidecar `del`'s bumped vv,
    /// the §2.7 deletion anchor [`crate::manifest::DeletedRow`]
    /// advertises).
    pub tombstone: Tombstone,
    /// The item relkeys whose `del` entries were staged, in staging
    /// order.
    pub staged: Vec<RelKey>,
}

/// §2.7 soft delete of `image`'s items of `kinds`:
///
/// 1. PUT the tombstone `{relkey, vv (bumped), device, server_ts,
///    kinds}` — **data keys untouched**, now and here: destruction is
///    GC's (§2.10, a later unit).
/// 2. In **one** committed transaction: per existing item of a named
///    kind, bump `vv[self]` and stage the `del` journal entry carrying
///    it; mark every such record
///    [`crate::state::ItemRecord::deleted`] (hidden; local trash is
///    app-side); record the §2.3 deleted-set row
///    ([`crate::state::StateTxn::record_deleted`]) under the image
///    relkey with the sidecar del's vv.
///
/// A kind with no item record is skipped (deleting a never-synced kind
/// is a no-op, not an error); naming no existing item at all is
/// [`EngineError::UnknownItem`].
pub async fn delete_item(
    db: &SyncDb,
    s3: &impl S3Api,
    bucket: &str,
    image: &RelKey,
    kinds: &[Kind],
) -> Result<DeleteOutcome, EngineError> {
    let own = db.device_id().clone();
    let server_ts = crate::transfer::server_ts_estimate(db)?;

    // The targets, in the caller's kind order, each with its bumped vv.
    let mut targets: Vec<(Kind, RelKey, ItemRecord, VersionVector)> = Vec::new();
    for &kind in kinds {
        // Only the kinds whose item key is derivable from the image
        // relkey live in this unit's delete lane.
        if !matches!(kind, Kind::Sidecar | Kind::Original) {
            continue;
        }
        let item = item_key_for_kind(image, kind)?;
        if let Some(record) = db.get_item(&item)? {
            if record.kind == kind {
                let mut vv = record.vv.clone();
                vv.bump(&own);
                targets.push((kind, item, record, vv));
            }
        }
    }
    if targets.is_empty() {
        return Err(EngineError::UnknownItem {
            relkey: image.clone(),
        });
    }

    // The §2.7 anchor: the sidecar del's bumped vv (the first target's
    // when no sidecar is among them).
    let anchor = targets
        .iter()
        .find(|(kind, ..)| *kind == Kind::Sidecar)
        .map(|(.., vv)| vv.clone())
        .unwrap_or_else(|| targets[0].3.clone());
    let tombstone = Tombstone {
        relkey: image.clone(),
        vv: anchor.clone(),
        device: own.clone(),
        server_ts,
        kinds: targets.iter().map(|(kind, ..)| *kind).collect(),
    };

    // 1. Tombstone PUT — idempotent/commutative (§2.1 principle 1); data
    //    keys are not touched.
    let body = serde_json::to_vec(&tombstone).map_err(JournalError::Json)?;
    s3.put_object(
        bucket,
        &tombstone_key(image),
        bytes::Bytes::from(body),
        &PutObjectOptions::default(),
    )
    .await?;

    // 2. One transaction: del entries staged + records hidden + the
    //    deleted-set row recorded.
    let mut staged = Vec::new();
    db.with_txn_err::<_, EngineError>(|t| {
        for (kind, item, record, vv) in &targets {
            let del = JournalEntry {
                v: JOURNAL_VERSION,
                seq: 0,
                ts: server_ts,
                device: own.clone(),
                op: Op::Del,
                kind: *kind,
                key: library_key(item),
                vv: vv.clone(),
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
            };
            enqueue_entry_in(t, &own, &del)?;
            t.update_item(item, record.state, |r| {
                r.vv = vv.clone();
                r.deleted = true;
                r.device = Some(own.clone());
                r.head_ts = Some(server_ts);
                r.admitted_vv = None;
            })?;
            t.queue_remove(Queue::Up, item)?;
            t.queue_remove(Queue::Down, item)?;
            staged.push(item.clone());
        }
        t.record_deleted(
            image,
            &DeletedRecord {
                vv: anchor.clone(),
                server_ts,
            },
        )?;
        Ok(())
    })?;

    Ok(DeleteOutcome { tombstone, staged })
}

/// §2.7 restore from "Recently Deleted": for every deleted item of
/// `image`, stage a metadata-only [`EnginePut`] whose vv dominates the
/// deletion (elementwise max of the record's vv and the known deletion
/// vv, plus a `vv[self]` bump), re-advertising the record's known
/// `blake3`/`sem_hash`/`content_id` — the data keys are still in the
/// bucket during grace, so restore moves **no bytes**. Clears the
/// `deleted` flags and this device's deleted-set row in the same
/// transaction. Returns the restored item relkeys.
///
/// An image with nothing deleted is [`EngineError::NotDeleted`].
pub fn restore_item(db: &SyncDb, image: &RelKey) -> Result<Vec<RelKey>, EngineError> {
    let own = db.device_id().clone();
    let now = crate::transfer::server_ts_estimate(db)?;
    let prefix = format!("{image}.");
    let targets: Vec<(RelKey, ItemRecord)> = db
        .iter_items()?
        .into_iter()
        .filter(|(key, record)| {
            record.deleted && (key == image || key.as_str().starts_with(&prefix))
        })
        .collect();
    if targets.is_empty() {
        return Err(EngineError::NotDeleted {
            relkey: image.clone(),
        });
    }
    let row = db.get_deleted(image)?;
    let mut restored = Vec::new();
    db.with_txn_err::<_, EngineError>(|t| {
        for (relkey, record) in &targets {
            let mut vv = record.vv.clone();
            if let Some(row) = &row {
                vv.merge(&row.vv);
            }
            vv.bump(&own);
            // A record without a published blake3 has nothing to
            // re-advertise (an EnginePut is unconstructible without one —
            // module docs); it is un-hidden locally only.
            if let Some(blake3) = record.blake3.clone() {
                let put = EnginePut {
                    device: own.clone(),
                    kind: record.kind,
                    item: relkey.clone(),
                    vv: vv.clone(),
                    blake3,
                    size: record.size,
                    ts: now,
                    sem_hash: record.sem_hash.clone(),
                    rating: record.rating,
                    color_label: record.color_label.clone(),
                    content_id: record.content_id.clone(),
                    w: record.w,
                    h: record.h,
                    mtime: (record.kind == Kind::Original)
                        .then(|| record.mtime_unix_ns.div_euclid(1_000_000_000)),
                };
                enqueue_entry_in(t, &own, &put.entry())?;
            }
            t.update_item(relkey, record.state, |r| {
                r.vv = vv.clone();
                r.deleted = false;
                r.head_ts = Some(now);
                r.device = Some(own.clone());
            })?;
            restored.push(relkey.clone());
        }
        t.remove_deleted(image)?;
        Ok(())
    })?;
    Ok(restored)
}

/// The §2.7 "Recently Deleted" listing: every item record carrying the
/// `deleted` flag, ascending by relkey.
pub fn recently_deleted(db: &SyncDb) -> Result<Vec<(RelKey, ItemRecord)>, EngineError> {
    Ok(db
        .iter_items()?
        .into_iter()
        .filter(|(_, record)| record.deleted)
        .collect())
}

// ---------------------------------------------------------------------------
// Local file paths (engine side of the file-relkey convention)
// ---------------------------------------------------------------------------

/// The local file of an engine-keyed item: plain
/// [`crate::keys::local_path`] of the **file relkey** (the suffix is
/// part of the key, so no kind-dependent suffixing happens here —
/// unlike [`crate::transfer::local_target_path`], whose landed base-
/// relkey convention this module's keying makes suffix-aware; module
/// docs).
pub fn item_local_path(sync_root: &Path, item: &RelKey) -> PathBuf {
    crate::keys::local_path(item, sync_root)
}
