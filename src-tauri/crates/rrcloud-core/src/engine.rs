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
//!    elementwise max), no download. A strictly dominating converged
//!    entry additionally hands its `blake3`/`size` to a record whose
//!    state holds no local bytes, and supersedes a `CorruptRemote`
//!    condemnation back into the fetch lane (review round 1; see
//!    [`EngineConsumer::converge`]). Over **uncommitted dirt**, a
//!    file-equal entry collapses the dirt only when it is a descendant
//!    of the local base or wins the §2.6 pick against it — a file-equal
//!    twin that LOSES the pick commits the dirt instead (review round
//!    1: collapsing parked the device on the fleet's losing branch
//!    under the identical folded vv).
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
//!    When the REMOTE branch loses, the device that authored the
//!    winning sidecar head and quiescently holds its bytes re-marks it
//!    `Dirty`, so the next admission re-publishes the winner as a fresh
//!    dominating version — the §1.2 shared bucket key is last-writer,
//!    so a losing upload landing after the winner's would otherwise
//!    leave the key holding loser bytes under the winner's advertised
//!    blake3 forever (review round 1; see
//!    [`EngineConsumer::resolve_concurrent`]).
//!
//! `del` entries order through the same machinery (§2.7): a dominating
//! `del` marks the item deleted (hidden; the record survives with the
//! [`crate::state::ItemRecord::deleted`] flag); a `del` concurrent with
//! local dirty-or-newer **resurrects** — put entries with dominating vv
//! are emitted for the sidecar **and** the image's original (the
//! original's put re-advertises the known `blake3`/`content_id`; the
//! bytes are still in the bucket during the grace window, so nothing
//! re-uploads). An original whose own upload intent is **in flight** is
//! NOT re-advertised — the in-flight put is concurrent with the del and
//! is itself the resurrection (review round 1; see
//! [`EngineConsumer::resurrect_original`]). When the original was never
//! known locally (no record / no `blake3`), only the sidecar resurrects
//! and a [`ResurrectionIncompleteEvent`] surfaces the §2.7 edge.
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

use crate::clock::{
    compare, identity_order_wins_remote, Candidate, DeviceId, VersionVector, VvOrder,
};
use crate::journal::{JournalEntry, JournalError, Kind, Op, Tombstone, JOURNAL_VERSION};
use crate::keys::{classify_key, library_key, tombstone_key, KeyClass, KeyError, RelKey};
use crate::publisher::{enqueue_entry_in, PublisherError};
use crate::reader::{ConsumerError, JournalConsumer};
use crate::s3::{PutObjectOptions, S3Api, S3Error};
use crate::semhash::{
    sem_hash, semantic_document, sidecar_badges, Blake3Hex, ContentId, SemHash, SemHashError,
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
/// `blake3(canonical loser document)[..6]`, where the canonical loser
/// document is the §2.5 **semantic** canonical form
/// ([`crate::semhash::semantic_document`]) — so the suffix is the first
/// six hex chars of the loser's [`crate::semhash::sem_hash`]. Deriving
/// from the semantic form (not the raw bytes) makes the suffix
/// **churn-stable** (review round 0): two holders of one loser version
/// whose local files diverged only by a §2.5 churn rewrite (EXIF
/// caching, auto-heal) still name the **same** key, preserving §2.6's
/// single-copy materialization. Invalid JSON fails closed like
/// [`crate::semhash::sem_hash`].
pub fn loser_vc_suffix(loser_doc: &[u8]) -> Result<String, EngineError> {
    let sem = sem_hash(loser_doc)?;
    Ok(sem.as_str()[..6].to_string())
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
/// ([`pick_winner`]).
///
/// A local head **without recorded identity** always loses to the
/// arriving entry. That fallback is locally deterministic but not
/// fleet-symmetric in the abstract (the entry's author compares the real
/// candidate pair) — it is fleet-safe here because identity-less heads
/// with a nonempty vv are unreachable through engine-written state:
/// every path that commits or adopts a head records `(head_ts, device)`
/// (admission, [`EngineConsumer`] apply, delete/restore), and the only
/// records predating those fields were written by pre-engine builds that
/// had **no production lane minting vvs at all** (their vv decodes
/// empty, so any entry compares `Greater` and case 4 never fires). A
/// record built outside the engine (test doubles, hand migration) that
/// pairs a nonempty vv with no identity accepts the remote-always-wins
/// pick as its contract (review round 0: rejected backfill — there is no
/// data to backfill from, and no shipped lineage produces the shape).
/// Both devices of a real exchange therefore compare the same candidate
/// pair, so the outcome is fleet-deterministic whatever the clocks said.
fn remote_wins_identity(local: &ItemRecord, entry: &JournalEntry) -> bool {
    match (local.head_ts, &local.device) {
        (Some(ts), Some(device)) => identity_order_wins_remote(
            VvOrder::Concurrent,
            Candidate {
                ts: entry.ts,
                device: &entry.device,
            },
            Candidate { ts, device },
        ),
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
/// candidate pair. An `Equal` vv is the **same version** re-delivered
/// (a manifest row, a replayed segment): its identity is already
/// recorded locally, so the local identity is kept — adopting here would
/// let a provenance-rewritten redelivery (e.g. a pre-`ts` manifest row
/// falling back to `written_server_ts`) corrupt the version's recorded
/// `(ts, device)` (review round 0).
fn converged_identity_is_remote(local: &ItemRecord, entry: &JournalEntry, ord: VvOrder) -> bool {
    match (local.head_ts, &local.device) {
        (Some(ts), Some(device)) => identity_order_wins_remote(
            ord,
            Candidate {
                ts: entry.ts,
                device: &entry.device,
            },
            Candidate { ts, device },
        ),
        // No local identity to arbitrate against: `Greater`/`Concurrent`
        // both resolve to the remote entry (nothing locally to prefer, same
        // as `remote_wins_identity`'s no-identity fallback); `Less`/`Equal`
        // still keep the (identity-less) local side, matching this
        // function's own dominance rule above.
        _ => matches!(ord, VvOrder::Greater | VvOrder::Concurrent),
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
    /// A semantic change was observed while a pipeline-interior state
    /// (`Queued`/`Uploading`/`Verifying`/`Downloading`/`Conflict`/
    /// `CorruptRemote`) owns the item: only the scanned identity was
    /// refreshed — **nothing was marked dirty** — and the caller still
    /// owes a re-scan once the pipeline settles (the §2.4 completion
    /// recheck, or the next intake). Honest third outcome (review round
    /// 0): the old `MarkedDirty` answer here misled callers into
    /// believing the re-admission was already recorded.
    DeferredToPipeline,
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
///
/// Suffix-aware for `Sidecar` (review round 0): a relkey that already
/// ends in `.rrdata` — a virtual-copy or primary sidecar named by its
/// own file path — **is** the item key; appending a second suffix would
/// derive a key no item ever had, making vc items unaddressable by
/// `delete_item`/`notify_local_change`.
fn item_key_for_kind(image: &RelKey, kind: Kind) -> Result<RelKey, KeyError> {
    match kind {
        Kind::Sidecar if image.as_str().ends_with(".rrdata") => Ok(image.clone()),
        Kind::Sidecar => sidecar_item_relkey(image),
        _ => Ok(image.clone()),
    }
}

/// Whether `item` is one of `image`'s items under the module's keying
/// convention: the original (`item == image`), the primary sidecar
/// (`<image>.rrdata`), or a virtual-copy sidecar
/// (`<image>.<6hex>.rrdata`). Structural, not a string prefix match — a
/// sibling image whose relkey merely extends `<image>.` (e.g.
/// `p/img.NEF.bak`) is **not** matched (review round 0:
/// `restore_item`'s old prefix filter over-matched exactly that).
fn item_belongs_to_image(item: &RelKey, image: &RelKey) -> bool {
    if item == image {
        return true;
    }
    let Some(rest) = item.as_str().strip_prefix(image.as_str()) else {
        return false;
    };
    match rest.strip_prefix('.') {
        Some("rrdata") => true,
        Some(tail) => tail
            .strip_suffix(".rrdata")
            .is_some_and(|hex| crate::hexutil::is_lower_hex(hex, 6)),
        None => false,
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
            admitted_ts: None,
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
            // settles) re-marks the item. The outcome says so honestly.
            db.update_item(&item, state, refresh)?;
            return Ok(ChangeOutcome::DeferredToPipeline);
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
                r.admitted_ts = Some(now);
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

/// §2.7/§2.11 edge event: one half of an item survived a concurrent
/// `del` but its counterpart could not be re-advertised on this device,
/// so the counterpart stays deleted-side until a holder re-advertises
/// it. Two symmetric lanes raise it:
///
/// - **forward** (§2.7): a resurrected sidecar's original could not be
///   re-advertised — this device never knew it (no record) or holds no
///   `blake3` for it. `relkey` is the image.
/// - **converse** (§2.11, round 5): a live original's tombstoned base
///   sidecar could not be re-advertised — this device holds a
///   `blake3`-less deleted sidecar record. `relkey` is the sidecar item.
///
/// Either way the sink treats it as advisory: re-derive state and let
/// any holder (or the §2.10 GC worker) close the gap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResurrectionIncompleteEvent {
    /// The item (image for the forward lane, sidecar for the converse)
    /// whose counterpart's resurrection was skipped on this device.
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

/// Why a §2.6/§2.8 loser-preservation step was skipped on this device
/// (review round 0: the skip must never be silent).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoserSkipReason {
    /// The state said this device holds the losing head, but the local
    /// file is gone (removed out of band). Not retried: the file will
    /// not come back on its own, and another holder (the loser's author)
    /// still materializes its copy.
    FileMissing,
    /// The local file exists but does not parse as a sidecar document
    /// (the §3.4 corruption case) — no deterministic vc key can be named
    /// from it.
    Unparsable,
    /// A different document already occupies the loser's deterministic
    /// vc key (a 24-bit suffix-prefix collision). The existing copy is
    /// never clobbered; the colliding loser stays unmaterialized here.
    KeyCollision,
}

/// §2.6 invariant-violation event: this device's state said it holds a
/// losing head, but the loser could **not** be preserved (the file is
/// missing, unparsable, or its vc key is occupied by a different
/// document). If this device was the loser's only holder, that version
/// is now unreachable — surfaced so the app can tell the user instead
/// of failing silently (review round 0; transient I/O failures are NOT
/// this event: they abort the apply transaction and retry next poll).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoserPreservationSkippedEvent {
    /// The conflicted image relkey.
    pub relkey: RelKey,
    /// The losing item whose preservation was skipped.
    pub item: RelKey,
    /// Why it was skipped.
    pub reason: LoserSkipReason,
}

/// The engine's event sink (§3.8's `sync-conflict`/`sync-error` family,
/// as a small trait so the core carries no channel dependency; the app
/// side adapts it onto `app_handle.emit`).
///
/// # Delivery contract (review round 0)
///
/// Events fire **inside the open apply transaction**, before it commits:
///
/// - **At-least-once, possibly for a rolled-back apply**: if the
///   surrounding transaction later aborts (a store failure, a consumer
///   error on a later mutation), the event was already delivered for a
///   resolution that never committed — and the retried apply re-fires
///   it. Sinks must treat events as advisory notifications to
///   re-derive state from, never as the state itself.
/// - **No reentrancy into the state store**: the sink runs on the thread
///   holding the single redb write transaction. A sink that
///   synchronously calls any `SyncDb` write path deadlocks the apply
///   (same single-writer hazard the [`crate::reader::JournalConsumer`]
///   docs pin). Queue the event and return; do the work after the poll.
pub trait EngineEvents {
    /// A §2.6 case-4 conflict was resolved.
    fn conflict(&mut self, event: ConflictEvent);
    /// A §2.7 resurrection could not cover the original.
    fn resurrection_incomplete(&mut self, event: ResurrectionIncompleteEvent);
    /// A §2.8 original-overwrite conflict was resolved.
    fn original_conflict(&mut self, event: OriginalConflictEvent);
    /// A losing head this device supposedly holds could not be preserved
    /// (see [`LoserPreservationSkippedEvent`]). Default: ignored, so the
    /// method is additive for existing sinks.
    fn loser_preservation_skipped(&mut self, event: LoserPreservationSkippedEvent) {
        let _ = event;
    }
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
            //
            // DELIBERATE v1 DEBT (review round 0, pinned by the suite):
            // a skipped entry is still marked applied, so a future-build
            // op published through this build is permanently invisible to
            // a device that polled it here — the applied-set/cursor never
            // re-applies it. This bites BOTH deferred ops the same way:
            //   - `Op::Move` (§2.7 rename): a lost move leaves a stale
            //     local path / orphaned stub.
            //   - `Op::Attest` (§2.2): attestations are the §2.4/§3.5
            //     LRU-eviction gate and the §6 worker is their writer
            //     (one attest per original it GETs), so a device that
            //     polls the worker's attests through this build drops them
            //     from its applied set and silently degrades the eviction
            //     gate — load-bearing, not cosmetic.
            // Acceptable only while no build publishes either op (none
            // does — verified: the only writers in-repo are `Op::Put`
            // in engine/manifest/transfer and `Op::Del` in engine),
            // exactly like manifest.rs's same-shaped argument. The move
            // unit AND the attest/eviction unit MUST each ship their own
            // migration (e.g. a one-time applied-set re-scan/reconcile)
            // before any writer exists. Fail-closed alternatives were
            // rejected: a consumer `Err` here is the reader's WHOLE-PASS
            // abort (it would let one future-op entry starve every
            // later-sorted device's prefix), and per-device halts are the
            // reader's own version gate, unreachable from a consumer.
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
            return self.create_from_put(txn, entry, kind, item);
        };
        if local.kind != kind {
            return Ok(());
        }

        // §2.7: a hidden record resolves put-vs-del FIRST — its vv
        // already folds the deletion, so a put the deletion does not
        // dominate un-hides the item (a restore/resurrection put, or an
        // edit genuinely concurrent with the delete: EDITS BEAT DELETES,
        // review round 0 — previously only a strictly dominating put
        // undeleted, so the deleter kept a winning concurrent edit
        // hidden forever). No conflict pick and no loser materialization
        // run against deleted content: the delete superseded it, so
        // un-hiding adopts the put as the head outright.
        if local.deleted {
            let ord = compare(&entry.vv, &local.vv);
            return match ord {
                VvOrder::Less | VvOrder::Equal => Ok(()),
                VvOrder::Greater | VvOrder::Concurrent => {
                    if self.content_equal(entry, kind, item, &local) {
                        self.converge(txn, entry, item, &local, ord)
                    } else {
                        self.adopt_remote(txn, entry, item, &local, kind)
                    }
                }
            };
        }

        // Local-commit-before-compare (§2.6 case 2's dirty clause):
        // uncommitted dirty edits are first committed as a local version —
        // unless the local FILE already holds the entry's content (the
        // §2.5 "same test applied to downloaded sidecars": converged, the
        // dirt collapses with no upload and no version). The gate is on
        // the Dirty STATE alone: an `admitted_vv` on a Dirty record is by
        // invariant a stale leftover (every demotion into Dirty withdraws
        // the intent — review round 0), and `commit_dirty` re-mints it.
        if local.state == ItemState::Dirty {
            let ord = compare(&entry.vv, &local.vv);
            if ord == VvOrder::Less {
                return Ok(()); // an ancestor of our base: our dirt supersedes it
            }
            let file_converged = match kind {
                Kind::Sidecar => {
                    entry.sem_hash.is_some() && entry.sem_hash == self.local_file_sem(item)
                }
                _ => entry.content_id.is_some() && entry.content_id == local.content_id,
            };
            // A file-equal entry may only COLLAPSE the dirt when adopting
            // it cannot contradict the §2.6 resolution the rest of the
            // fleet runs on the same pair (review round 1, probe-verified
            // permanent divergence): a Greater/Equal entry descends from
            // our committed base (there is no pick), and a Concurrent one
            // only when it WINS the (ts, device) pick against that base —
            // the exact candidate pair every other device resolves. A
            // file-equal entry that LOSES the pick must not become the
            // local primary (it is the fleet's losing branch; collapsing
            // parked this device on it under the identical folded vv,
            // unhealable and event-less): the dirt commits as a local
            // version instead — it IS a genuinely newer edit authored
            // over our base — and the in-flight content-equal lane below
            // folds the entry as a twin of the committed head.
            if file_converged
                && (matches!(ord, VvOrder::Greater | VvOrder::Equal)
                    || remote_wins_identity(&local, entry))
            {
                return self.converge_dirty(txn, entry, item, &local);
            }
            if ord == VvOrder::Equal {
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
        if self.content_equal(entry, kind, item, &local) {
            let ord = compare(&entry.vv, &head_vv);
            return self.converge(txn, entry, item, &local, ord);
        }
        match compare(&entry.vv, &head_vv) {
            VvOrder::Less | VvOrder::Equal => Ok(()), // case 3 (Equal handled above)
            VvOrder::Greater => self.adopt_remote(txn, entry, item, &local, kind),
            VvOrder::Concurrent => self.resolve_concurrent(txn, entry, image, item, &local, kind),
        }
    }

    /// The §2.6 case-1 content test against the local **head**. With an
    /// upload intent in flight (`admitted_vv` set), the record's
    /// `sem_hash`/`blake3` still name the last *published* version (the
    /// §2.6 coordination note), while `head_vv` is the admitted snapshot
    /// — comparing the entry's content against the published fields
    /// would converge an entry that matches the OLD version with the
    /// in-flight head and silently skip case 4 (review round 0, verified
    /// divergence). The in-flight head's content identity is derivable
    /// only from the local file (sidecars: [`Self::local_file_sem`], as
    /// the dirty lane already does) or the intake-refreshed `content_id`
    /// (originals; [`notify_local_change`] keeps it tracking the local
    /// bytes). An unreadable/unparsable local file reads as "not equal",
    /// which conservatively falls through to the vv comparison.
    fn content_equal(
        &self,
        entry: &JournalEntry,
        kind: Kind,
        item: &RelKey,
        local: &ItemRecord,
    ) -> bool {
        let in_flight = local.admitted_vv.is_some();
        match kind {
            Kind::Sidecar => {
                entry.sem_hash.is_some()
                    && if in_flight {
                        entry.sem_hash == self.local_file_sem(item)
                    } else {
                        entry.sem_hash == local.sem_hash
                    }
            }
            _ => {
                if in_flight {
                    entry.content_id.is_some() && entry.content_id == local.content_id
                } else {
                    (entry.blake3.is_some() && entry.blake3 == local.blake3)
                        || (entry.content_id.is_some() && entry.content_id == local.content_id)
                }
            }
        }
    }

    /// Case 2 for an unknown item: create it with the entry's facts —
    /// hidden instead when a recorded deletion for the **item** still
    /// dominates the put (per-item deleted-set rows, review round 0; the
    /// record's vv folds the known deletion lineage either way, so both
    /// arrival orders of `{put, del}` produce the identical record).
    fn create_from_put(
        &mut self,
        txn: &StateTxn<'_>,
        entry: &JournalEntry,
        kind: Kind,
        item: &RelKey,
    ) -> Result<(), EngineError> {
        let row = txn.get_deleted(item)?;
        // §2.7: a put the deletion does not dominate is live (restore,
        // resurrection, or edits-beat-deletes); only a put the deletion
        // strictly supersedes (or equals) stays hidden behind the row.
        let hidden = row
            .as_ref()
            .is_some_and(|r| matches!(compare(&entry.vv, &r.vv), VvOrder::Less | VvOrder::Equal));
        let mut vv = entry.vv.clone();
        if let Some(row) = &row {
            vv.merge(&row.vv);
        }
        let record = ItemRecord {
            kind,
            state: ItemState::PendingDown,
            size: entry.size.unwrap_or(0),
            mtime_unix_ns: entry.mtime.unwrap_or(0).saturating_mul(1_000_000_000),
            blake3: entry.blake3.clone(),
            sem_hash: entry.sem_hash.clone(),
            vv,
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
            admitted_ts: None,
            deleted: hidden,
        };
        txn.insert_item(item, &record)?;
        if !hidden {
            if row.is_some() {
                txn.remove_deleted(item)?;
            }
            // xmp download policy is P2: metadata recording only. Note: ORIGINALS
            // are enqueued here too, but the app-layer cycle's stub policy
            // (`sync::manager::Configured::stub_pending_originals`, §3.5) turns
            // unpinned PendingDown originals into cloud stubs and dequeues them
            // before `pump_downloads` runs, so they hydrate on demand rather than
            // downloading eagerly. The engine itself stays download-everything;
            // the stub-vs-download *policy* lives in the app.
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
        // The record now names the entry's blake3: integrity facts earned
        // for the superseded published version are stale — cleared exactly
        // as `adopt_remote` and `commit_verified` do on their re-points
        // (review round 1; the §3.5 eviction gate reads both flags).
        record.verified_remote = false;
        record.attested = false;
        let ord = compare(&entry.vv, &local.vv);
        if converged_identity_is_remote(local, entry, ord) {
            record.head_ts = Some(entry.ts);
            record.device = Some(entry.device.clone());
        }
        record.admitted_vv = None;
        record.admitted_ts = None;
        txn.replay_put_item_cas(item, ItemState::Dirty, &record)?;
        self.clear_superseded_row(txn, &entry.vv, item)
    }

    /// Case 1 for a committed/clean local head: same content under a
    /// different vv — vv max-merge, deterministic head-identity
    /// convergence ([`converged_identity_is_remote`]), and for a device
    /// whose state proves it **holds** the head's bytes, local bytes stay
    /// authoritative with no transfer. A hidden record un-hides when the
    /// entry is `Greater` **or `Concurrent`** with the folded deletion
    /// (§2.7 edits beat deletes / content-equal resurrection — review
    /// round 0: the strictly-Greater predicate kept a re-advertised
    /// original hidden on the deleter when the resurrection vv was
    /// concurrent with the original's own del).
    ///
    /// A **strictly dominating** entry IS the newer version, so a record
    /// whose state holds no local bytes (`PendingDown`/`Downloading`/
    /// `Stub`/`CorruptRemote`/`Conflict`) adopts the entry's
    /// `blake3`/`size`/`content_id` (review round 1, probe-verified: a
    /// churned same-sem rewrite otherwise left a pending fetch verifying
    /// the key's new bytes against the superseded hash — condemning a
    /// healthy remote to `CorruptRemote` with the vv already advanced),
    /// clearing the integrity flags that described the superseded blake3.
    /// And a `CorruptRemote` record meeting a Greater entry takes the
    /// §2.4 `CorruptRemote → PendingDown` repair edge ("repair landed
    /// elsewhere, fetch it") — the condemned advertisement is superseded,
    /// so the item re-enters the fetch lane instead of staying wedged
    /// off-queue (review round 1: this is also what heals the receivers
    /// of the shared-key repair re-advertisement).
    fn converge(
        &mut self,
        txn: &StateTxn<'_>,
        entry: &JournalEntry,
        item: &RelKey,
        local: &ItemRecord,
        ord: VvOrder,
    ) -> Result<(), EngineError> {
        let undelete = local.deleted
            && matches!(
                compare(&entry.vv, &local.vv),
                VvOrder::Greater | VvOrder::Concurrent
            );
        // Un-hiding adopts the put's identity unconditionally: the
        // deletion superseded the local content's head claim, so the
        // arriving put is the item's only live head — and its author
        // stamped itself (restore/resurrection), so every device
        // converges on that identity whatever its local vv relation to
        // the folded deletion was (review round 0, pinned by the
        // asymmetric-resurrection scenario's state-equivalence check).
        let adopt_identity = undelete || converged_identity_is_remote(local, entry, ord);
        // Content-identity adoption (doc comment): only for a strictly
        // dominating entry, only when this device has no local bytes to
        // keep authoritative, and only when the blake3 actually moves
        // (a re-advertisement of the same hash — restore/resurrection —
        // keeps the flags it legitimately still describes).
        let adopt_content = ord == VvOrder::Greater
            && !state_holds_local_bytes(local.state)
            && entry.blake3.is_some()
            && entry.blake3 != local.blake3;
        let mutate = |r: &mut ItemRecord| {
            r.vv.merge(&entry.vv);
            if adopt_content {
                r.blake3 = entry.blake3.clone();
                r.size = entry.size.unwrap_or(local.size);
                if entry.content_id.is_some() {
                    r.content_id = entry.content_id.clone();
                }
                // The §2.2 `w`/`h` fleet facts travel with the content the
                // device is adopting (review round 3, minor:
                // `adopt_remote`/`create_from_put` already carry them, so
                // the converge path must not leave them stale — §4.4
                // proxy_scale reads them). `mtime` is left as-is: a holder
                // keeps its local file's nanosecond mtime, which the
                // whole-second wire value would only coarsen.
                r.w = entry.w;
                r.h = entry.h;
                // Integrity facts described the superseded blake3.
                r.verified_remote = false;
                r.attested = false;
            }
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
        } else if local.state == ItemState::CorruptRemote && ord == VvOrder::Greater {
            // The §2.4 repair edge (doc comment): supersede the condemned
            // advertisement and fetch the newer version.
            txn.transition(
                item,
                ItemState::CorruptRemote,
                ItemState::PendingDown,
                mutate,
            )?;
            txn.queue_push(Queue::Down, item, transfer_class(local.kind))?;
        } else {
            txn.update_item(item, local.state, mutate)?;
        }
        self.clear_superseded_row(txn, &entry.vv, item)
    }

    /// Case 2 (and the case-4 remote-winner half, and the §2.7
    /// un-hide-and-adopt half): the record adopts the entry's facts and
    /// the item heads for the download lane — `PendingDown` plus a queue
    /// row for sidecars and held originals; `Stub` originals adopt
    /// metadata only (hydration is on-demand, P2), and xmp items are
    /// metadata-only at this unit.
    fn adopt_remote(
        &mut self,
        txn: &StateTxn<'_>,
        entry: &JournalEntry,
        item: &RelKey,
        local: &ItemRecord,
        kind: Kind,
    ) -> Result<(), EngineError> {
        // §2.7 un-hide rule, same as `converge`'s: Greater (restore) or
        // Concurrent (edits beat deletes) vs the deletion-folded vv.
        let undelete = local.deleted
            && matches!(
                compare(&entry.vv, &local.vv),
                VvOrder::Greater | VvOrder::Concurrent
            );
        let had_intent = local.admitted_vv.is_some();
        let adopt = |r: &mut ItemRecord| {
            // §2.6: the path's vv becomes the elementwise max of both
            // heads' PUBLISHED histories. A withdrawn in-flight intent
            // (its admitted snapshot) deliberately does NOT fold in
            // (review round 0): the snapshot was never published on this
            // key — the losing version lives on as the vc under its own
            // key and vv — so folding it would give this device a vv
            // component no other device can ever learn, making every
            // FUTURE honest descendant of the converged state read as
            // concurrent here (a permanent spurious-conflict wedge). The
            // unpublished component is simply re-mintable: the next
            // admission bumps from the merged record vv.
            r.vv.merge(&entry.vv);
            r.blake3 = entry.blake3.clone();
            r.sem_hash = entry.sem_hash.clone();
            r.size = entry.size.unwrap_or(local.size);
            r.mtime_unix_ns = entry
                .mtime
                .map(|m| m.saturating_mul(1_000_000_000))
                .unwrap_or(local.mtime_unix_ns);
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
            r.admitted_ts = None;
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
        self.clear_superseded_row(txn, &entry.vv, item)
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
                return self.adopt_remote(txn, entry, item, local, kind);
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
            self.adopt_remote(txn, entry, item, local, kind)?;
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
            // §1.2 shared-key repair (review round 1, probe-verified):
            // the loser's entry exists, so its bytes LANDED on the shared
            // bucket key at some point — and landing order is independent
            // of the §2.6 pick, so when the loser's PUT landed last the
            // key holds loser bytes while every record adopts the
            // winner's blake3: every fetcher wedges CorruptRemote and
            // nothing in this unit would ever re-upload the winner. The
            // device that AUTHORED the winning head and quiescently holds
            // its bytes re-marks it Dirty here: the next admission
            // (§3.7-gated like any edit) re-publishes the winner as a
            // fresh dominating version, whose upload re-PUTs the bytes
            // over the key and whose Greater entry rescues wedged
            // receivers through `converge`'s repair edge. Author-only so
            // one conflict yields one repair (N holders would mint N
            // versions); an in-flight committed head (`admitted_vv`
            // standing) re-PUTs by itself, and non-holding states have
            // nothing to upload. Scoped to sidecars: original overwrites
            // adopt the §2.8 displaced-copy lane, and auto re-uploading
            // multi-GB RAWs is a policy choice this unit does not make.
            //
            // Cost note (review round 2, accepted): this re-mark is
            // UNCONDITIONAL on the winning author because the engine
            // cannot cheaply know the bucket's last-PUT order, so it also
            // fires when the winner's bytes already occupy the key (no
            // repair needed) — one spurious version + one re-upload per
            // conflict on the author, never divergence (receivers
            // converge either way). A future cheap landing-order probe
            // (HEAD/ETag vs the winner's blake3 before re-marking) could
            // drop the redundant re-publish; not worth a round trip here.
            if kind == Kind::Sidecar
                && local.admitted_vv.is_none()
                && local.device.as_ref() == Some(&self.own_device)
                && matches!(local.state, ItemState::Synced | ItemState::Hydrated)
            {
                txn.transition(item, local.state, ItemState::Dirty, |_| {})?;
            }
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

    /// §2.7 del ordering, arrival-order independent (review round 0):
    ///
    /// - **dominating del** → hide, fold the vv, record the per-item
    ///   deleted-set row;
    /// - **ancestor/equal del** → ignore (our version supersedes it);
    /// - **concurrent del vs uncommitted dirt** → resurrect (the single
    ///   dirt holder commits + re-advertises, §2.7's explicit lane);
    /// - **concurrent del vs a committed/in-flight live head** → EDITS
    ///   BEAT DELETES: the item stays live and the del's vv **folds into
    ///   the record** so the state is absorbing — the deleter's side
    ///   un-hides when it applies our head's put (the `apply_put`
    ///   deleted-record arm), and both arrival orders of `{put, del}`
    ///   land identical (previously this arm dropped the del entirely:
    ///   no fold, order-divergent third devices, half-deleted items).
    ///   For a surviving *sidecar* head this arm also resurrects the
    ///   image's original (author-only — review round 2), so a committed
    ///   edit raced by a whole-photo delete keeps the whole item, not
    ///   just the edited key;
    /// - **concurrent del vs an already-hidden record** → two deletes of
    ///   one item: fold vv into the record **and** the row, stay hidden.
    ///
    /// Deletion ordering is strictly **per item key** (§2.6 applied to
    /// dels): a del of the image's original is resolved against the
    /// original's own vv. Item-wholeness across kinds — re-advertising
    /// the original when its sidecar's edit beats a concurrent delete —
    /// is carried by the resurrection lanes, each keyed to a *single*
    /// re-advertiser so a delete mints one resurrection put, not N: the
    /// uncommitted-dirty lane (the lone dirt holder) and the
    /// committed/in-flight Concurrent arm (the surviving head's author,
    /// `local.device == own_device`). A clean whole-photo delete with no
    /// concurrent edit never reaches either lane: its sidecar del
    /// *dominates* (the Greater arm hides the sidecar), so no
    /// resurrection fires and the item deletes whole.
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
            self.record_deletion_row(txn, item, &entry.vv, entry.ts)?;
            return Ok(());
        };
        if local.kind != kind {
            return Ok(());
        }

        // §2.7 edits-beat-deletes: a del meeting UNCOMMITTED dirty edits
        // resurrects — the dirt commits as a local version whose vv
        // dominates the del, and (for the sidecar) the image's original
        // is re-advertised so the whole item survives. A hidden record is
        // never dirt (deletion withdraws local intents), so the deleted
        // case below is not shadowed.
        if !local.deleted && local.state == ItemState::Dirty {
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
                r.admitted_ts = Some(now);
                r.head_ts = Some(now);
                r.device = Some(own.clone());
                r.deleted = false;
            })?;
            txn.queue_push(Queue::Up, item, transfer_class(local.kind))?;
            if kind == Kind::Sidecar {
                self.resurrect_original(txn, entry, image)?;
            }
            return self.clear_superseded_row(txn, &admitted, item);
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
                // Dominating del: hide — the record survives whole (§2.7)
                // and keeps the deleted CONTENT version's head identity
                // (what restore re-advertises; also what a device that
                // learns `{put, del}` del-first records, so arrival
                // orders agree bit-for-bit — review round 0), the
                // transfer lanes let go of it, and the deletion is
                // recorded for the §2.3 deleted set under the item key.
                txn.update_item(item, local.state, |r| {
                    r.vv.merge(&entry.vv);
                    r.deleted = true;
                    r.admitted_vv = None;
                    r.admitted_ts = None;
                })?;
                txn.queue_remove(Queue::Up, item)?;
                txn.queue_remove(Queue::Down, item)?;
                self.record_deletion_row(txn, item, &entry.vv, entry.ts)
            }
            VvOrder::Concurrent => {
                if local.deleted {
                    // Concurrent deletes of one item: both stand — fold
                    // the vv into the record and the row.
                    txn.update_item(item, local.state, |r| r.vv.merge(&entry.vv))?;
                    self.record_deletion_row(txn, item, &entry.vv, entry.ts)
                } else {
                    // Edits beat deletes (§2.7): our committed (or
                    // in-flight) version survives and ABSORBS the del —
                    // the record's folded vv is what reaches the deleter
                    // (its apply_put un-hides on Concurrent) and what any
                    // later-bootstrapping device folds, so every arrival
                    // order converges live. No row: the deletion lost.
                    txn.update_item(item, local.state, |r| r.vv.merge(&entry.vv))?;
                    // Whole-item survival for a COMMITTED/in-flight sidecar
                    // edit (review round 2 blocker): a surviving sidecar
                    // edit concurrent with a whole-photo delete used to
                    // leave the image's ORIGINAL orphaned — its own del
                    // strictly dominates (the editor never bumped the
                    // original's component), so apply_del hid it fleet-wide
                    // and its deleted-set row stood, handing §2.10 GC a
                    // live-sidecar-but-dead-RAW item to destroy. No event
                    // fired and the fleet CONVERGED to the broken item, so
                    // §2.11's "resurrection restores a whole item" was
                    // silently violated for the ordinary two-devices-online
                    // case (only the uncommitted-dirty lane above healed
                    // it). The surviving head's deterministic single author
                    // re-advertises the original here, exactly as the dirty
                    // lane and resolve_concurrent's author-only repair do.
                    //
                    // Author-only (`local.device == own_device`) is what
                    // makes this safe where the per-item comment above
                    // declined committed-holder resurrection: one delete
                    // yields ONE resurrection put (the lone author's), not
                    // one per committed holder — the N-version fan-out is
                    // defeated by the gate, not by dropping the
                    // resurrection. Receivers (including a third device
                    // holding the same head) adopt the published put; the
                    // author alone mints it. Scoped to Sidecar: an
                    // original's own concurrent del is the §2.8 overwrite
                    // lane, never a whole-item race. resurrect_original
                    // itself skips an original whose upload is in flight
                    // (that put IS its survival) and fires
                    // ResurrectionIncompleteEvent when the original is
                    // unknown here — the orphaning is never again silent.
                    if kind == Kind::Sidecar && local.device.as_ref() == Some(&self.own_device) {
                        self.resurrect_original(txn, entry, image)?;
                    }
                    Ok(())
                }
            }
        }
    }

    /// The §2.7 whole-item resurrection half: re-advertise the image's
    /// original with a vv dominating the del — metadata-only, the bytes
    /// are still in the bucket during grace — or surface
    /// [`ResurrectionIncompleteEvent`] when this device never knew the
    /// original (no record / no `blake3`).
    ///
    /// An original with an **admitted upload intent in flight** is
    /// skipped outright (review round 1, two probe-verified blockers):
    /// the in-flight put IS the survival — its admitted vv carries this
    /// device's unpublished self component, so it is concurrent with
    /// every del this device had not folded at admission, and
    /// edits-beat-deletes keeps the item live here (apply_del's
    /// Concurrent arm) and un-hides it on the deleter (apply_put's
    /// deleted-record arm). A metadata re-advertisement of the OLD
    /// published blake3 here would either re-mint the exact self
    /// component the upload is about to publish (two different versions
    /// under one vv — permanent fleet divergence on the content of that
    /// vv) or, folding the snapshot, mint a vv strictly dominating the
    /// in-flight NEW version — shadowing it fleet-wide while the bucket
    /// key's bytes stop matching the winning head's hash (every fetcher
    /// then wedges `CorruptRemote`). No event fires: the resurrection is
    /// complete, just carried by the upload's own entry.
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
        if original.admitted_vv.is_some()
            && matches!(
                original.state,
                ItemState::Queued | ItemState::Uploading | ItemState::Verifying
            )
        {
            // In-flight intent: the upload is the resurrection (doc
            // comment). The Dirty state is deliberately NOT in the gate:
            // a Dirty record's leftover intent is withdrawn by invariant,
            // and its own del (if any) commits the dirt through the
            // ordinary Dirty lane.
            return Ok(());
        }
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
        // §1.2: content_id IS the full-file blake3 of the bytes this put
        // advertises. Derive it from the `blake3` being re-advertised (the
        // PUBLISHED bytes), never from `original.content_id` —
        // notify_local_change moves that to track an UNCOMMITTED
        // out-of-band overwrite (§2.6 coordination note), so copying it
        // here would publish a put whose content_id names bytes other than
        // the ones it carries (round 4 major: previews/thumbs keyed by the
        // mislabel are GC'd, and apply_put case-1 content-id equality
        // mis-converges the record with a genuinely different same-id
        // overwrite).
        let content_id = Some(ContentId::from_blake3(&blake3));
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
            content_id,
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
        // Transfer-lane normalization, mirroring restore_item (review
        // round 1, minor): when the dels applied [Original, Sidecar],
        // the original's dominating del removed its Down queue row while
        // hiding it — un-hiding a `PendingDown` record must re-push the
        // row or the item is pump-invisible until the next startup
        // sweep. (The upload lane needs no analogue: a hidden record can
        // never sit there — a remote del never dominates an admitted
        // intent, whose snapshot holds an unpublished self component,
        // and a LOCAL delete also hides the sidecar, whose hidden record
        // never enters this resurrection lane.)
        if original.deleted && original.state == ItemState::PendingDown {
            txn.queue_push(Queue::Down, image, CLASS_ORIGINAL)?;
        }
        // The original's own deleted-set row (if its del already applied
        // here) is superseded by the resurrection put.
        self.clear_superseded_row(txn, &vv, image)
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
    /// local file holds the losing head, the loser's **canonical
    /// semantic document** ([`crate::semhash::semantic_document`]) is
    /// written to its deterministic vc path and, unless the vc key is
    /// already known **live** (`(key, sem_hash)` apply dedup; a
    /// soft-DELETED record with the matching sem is first restored —
    /// review round 1, [`Self::restore_vc_in_txn`]), staged for upload
    /// with a fresh single-component vv through the ordinary admission
    /// lane. Returns the vc item relkey when this device can name it.
    ///
    /// Materializing the canonical form — not the holder's raw bytes —
    /// is what makes the §2.6 "byte-identical PUTs to the same key"
    /// claim TRUE for holders whose local files diverged only by §2.5
    /// churn (review round 0): every holder derives the same suffix
    /// ([`loser_vc_suffix`], sem-based) AND writes the same bytes, so
    /// records, bucket bytes and blake3s all converge. The semantic form
    /// is the §2.5 definition of the document's content; everything it
    /// drops (`exif` cache, `version`, `lutPath`, formatting) is churn
    /// the protocol never syncs. Idempotent: the projection is a fixed
    /// point, so `sem_hash(vc file) == sem_hash(loser)`.
    ///
    /// Failure posture (review round 0 — preservation must never be
    /// skipped silently): a missing file or an unparsable document fires
    /// [`LoserPreservationSkippedEvent`] (plus
    /// [`loser_vc_suffix`]-collision with a DIFFERENT resident document,
    /// which is never clobbered); any other I/O failure is `Err`, which
    /// aborts the apply transaction so the next poll retries.
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
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // Removed out of band: not transient (never retried); the
                // loser's author still materializes its copy elsewhere.
                self.events
                    .loser_preservation_skipped(LoserPreservationSkippedEvent {
                        relkey: image.clone(),
                        item: item.clone(),
                        reason: LoserSkipReason::FileMissing,
                    });
                return Ok(None);
            }
            Err(e) => return Err(io_err(&path, e)),
        };
        // An unparsable local document cannot name a deterministic vc
        // key (the §3.4 corruption case): skipped WITH an event.
        let Ok(canonical) = semantic_document(&bytes) else {
            self.events
                .loser_preservation_skipped(LoserPreservationSkippedEvent {
                    relkey: image.clone(),
                    item: item.clone(),
                    reason: LoserSkipReason::Unparsable,
                });
            return Ok(None);
        };
        let canonical = canonical.into_bytes();
        let sem = sem_hash(&canonical)?;
        let suffix = sem.as_str()[..6].to_string();
        let vc_rel = vc_item_relkey(image, &suffix)?;
        if let Some(existing) = txn.get_item(&vc_rel)? {
            if existing.sem_hash.as_ref() != Some(&sem) {
                // 24-bit suffix-prefix collision with a genuinely
                // different document (review round 0): never clobber the
                // resident vc's file or record, never stage anything.
                self.events
                    .loser_preservation_skipped(LoserPreservationSkippedEvent {
                        relkey: image.clone(),
                        item: item.clone(),
                        reason: LoserSkipReason::KeyCollision,
                    });
                return Ok(None);
            }
            if existing.deleted {
                // A DELETED record with this sem is not a live
                // preservation (review round 1, probe-verified): the vc
                // was materialized by an earlier conflict and then
                // soft-deleted — "a tombstoned item happens to have this
                // sem" is a different event from "this loser is already
                // preserved live", and conflating them left the new
                // loser reachable through NO live state anywhere while
                // the ConflictEvent claimed a copy. The new conflict
                // supersedes the vc's deletion: restore it (the
                // restore_item body, inside this apply transaction) — a
                // dominating metadata-only put re-advertising its
                // published identity, un-hide, transfer-lane
                // normalization, deletion row cleared.
                self.restore_vc_in_txn(txn, &vc_rel, &existing)?;
            }
            // Apply dedup by (key, sem_hash): the vc is already known —
            // our own earlier materialization, or the peer's
            // advertisement (which the ordinary put apply converges with
            // ours). Re-write the (byte-identical) file idempotently so
            // a missing local copy heals.
            self.write_vc_file(&vc_rel, &canonical)?;
            return Ok(Some(vc_rel));
        }
        self.write_vc_file(&vc_rel, &canonical)?;
        let badges = sidecar_badges(&canonical).unwrap_or_default();
        let own = self.own_device.clone();
        let record = ItemRecord {
            kind: Kind::Sidecar,
            state: ItemState::Dirty,
            size: canonical.len() as u64,
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
            admitted_ts: None,
            deleted: false,
        };
        txn.insert_item(&vc_rel, &record)?;
        let now = self.now_unix;
        txn.transition(&vc_rel, ItemState::Dirty, ItemState::Queued, |r| {
            let mut fresh = VersionVector::new();
            fresh.bump(&own);
            r.admitted_vv = Some(fresh);
            r.admitted_ts = Some(now);
        })?;
        txn.queue_push(Queue::Up, &vc_rel, CLASS_SIDECAR)?;
        Ok(Some(vc_rel))
    }

    /// [`restore_item`]'s single-item body, run inside the apply
    /// transaction for a soft-deleted vc whose deterministic key a new
    /// conflict's loser needs again (review round 1; see the dedup
    /// branch of [`Self::materialize_sidecar_loser`]): stages a
    /// metadata-only [`EnginePut`] whose vv dominates the deletion
    /// (record vv ∪ deleted-set row, `vv[self]` bumped) when the record
    /// has a published `blake3` to re-advertise, un-hides the record
    /// with the fresh head identity, normalizes the transfer lanes the
    /// deletion emptied (a `PendingDown` vc re-queues its fetch; a
    /// stranded upload-lane vc demotes to `Dirty` so the next admission
    /// re-publishes the file this materialization just rewrote), and
    /// clears this device's deletion row.
    fn restore_vc_in_txn(
        &mut self,
        txn: &StateTxn<'_>,
        vc_rel: &RelKey,
        existing: &ItemRecord,
    ) -> Result<(), EngineError> {
        let own = self.own_device.clone();
        let now = self.now_unix;
        let mut vv = existing.vv.clone();
        if let Some(row) = txn.get_deleted(vc_rel)? {
            vv.merge(&row.vv);
        }
        vv.bump(&own);
        if let Some(blake3) = existing.blake3.clone() {
            let put = EnginePut {
                device: own.clone(),
                kind: Kind::Sidecar,
                item: vc_rel.clone(),
                vv: vv.clone(),
                blake3,
                size: existing.size,
                ts: now,
                sem_hash: existing.sem_hash.clone(),
                rating: existing.rating,
                color_label: existing.color_label.clone(),
                content_id: None,
                w: None,
                h: None,
                mtime: None,
            };
            enqueue_entry_in(txn, &own, &put.entry())?;
        }
        txn.update_item(vc_rel, existing.state, |r| {
            r.vv = vv.clone();
            r.deleted = false;
            r.head_ts = Some(now);
            r.device = Some(own.clone());
        })?;
        match existing.state {
            ItemState::PendingDown => {
                if existing.blake3.is_some() {
                    txn.queue_push(Queue::Down, vc_rel, CLASS_SIDECAR)?;
                }
            }
            state @ (ItemState::Queued | ItemState::Uploading) => {
                txn.transition(vc_rel, state, ItemState::Dirty, |_| {})?;
            }
            ItemState::Verifying => {
                // No direct Verifying → Dirty edge: via the legal retry
                // demotion first (mirroring restore_item).
                txn.transition(vc_rel, ItemState::Verifying, ItemState::Queued, |_| {})?;
                txn.transition(vc_rel, ItemState::Queued, ItemState::Dirty, |_| {})?;
            }
            _ => {}
        }
        txn.remove_deleted(vc_rel)?;
        Ok(())
    }

    /// Writes a materialized vc document to its local path (parents
    /// created as needed).
    fn write_vc_file(&self, vc_rel: &RelKey, canonical: &[u8]) -> Result<(), EngineError> {
        let vc_path = item_local_path(&self.sync_root, vc_rel);
        if let Some(parent) = vc_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| io_err(&vc_path, e))?;
        }
        std::fs::write(&vc_path, canonical).map_err(|e| io_err(&vc_path, e))
    }

    /// §2.8 displaced-bytes staging: when this device holds the losing
    /// original's bytes, they are copied to the deterministic conflict
    /// relkey (idempotent), staged for upload with a fresh
    /// single-component vv, and the [`OriginalConflictEvent`] fires.
    ///
    /// **Displacement is detected from vv concurrency, not a
    /// replaced-content_id field** — and this now MATCHES the design:
    /// review round 2 rewrote ARCHITECTURE.md §2.8 to specify the
    /// vv-concurrency mechanism and to state explicitly that "the v1
    /// `JournalEntry` wire carries only the *new* `content_id`." (The
    /// earlier spec wording about an entry "recording the content_id it
    /// replaced" was the one corrected; this is no longer a deviation.)
    /// The two concurrent overwrite puts meet in §2.6 case 4
    /// ([`Self::resolve_concurrent`]), which routes the losing holder
    /// here — reaching §2.8's outcome for its two-concurrent-overwrites
    /// scenario (pinned by S6 and the `original_overwrite_*` unit tests)
    /// while correctly NOT preserving superseded *ancestors*, which §2.8
    /// does not ask for. A future reconcile/worker unit must not go
    /// looking for a replaced-content_id field on entries: none exists,
    /// by design.
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
        let blake3 = match hash_file(&path) {
            Ok(b3) => b3,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // The state said we hold the displaced bytes but the file
                // is gone out of band (review round 0: no silent skip).
                self.events
                    .loser_preservation_skipped(LoserPreservationSkippedEvent {
                        relkey: image.clone(),
                        item: item.clone(),
                        reason: LoserSkipReason::FileMissing,
                    });
                return Ok(None);
            }
            Err(e) => return Err(io_err(&path, e)),
        };
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
                admitted_ts: None,
                deleted: false,
            };
            txn.insert_item(&conflict_rel, &record)?;
            let now = self.now_unix;
            txn.transition(&conflict_rel, ItemState::Dirty, ItemState::Queued, |r| {
                let mut fresh = VersionVector::new();
                fresh.bump(&own);
                r.admitted_vv = Some(fresh);
                r.admitted_ts = Some(now);
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

    /// Records (or vv-max-merges into) the §2.3 deleted-set row for one
    /// deleted **item** — keyed by the item's own file relkey (§2.3 "one
    /// row per deleted key"; review round 0: an image-keyed row mixed
    /// the sidecar's, original's and vcs' independent vv lineages, so a
    /// manifest-merging laggard hid only the sidecar and bootstrap
    /// compared cross-lineage vvs). Merging keeps the anchor regardless
    /// of the dels' arrival order.
    fn record_deletion_row(
        &self,
        txn: &StateTxn<'_>,
        item: &RelKey,
        vv: &VersionVector,
        server_ts: i64,
    ) -> Result<(), EngineError> {
        let merged = match txn.get_deleted(item)? {
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
        txn.record_deleted(item, &merged)?;
        Ok(())
    }

    /// Withdraws this device's deleted-set row for `item` when an
    /// applied live put's `vv` supersedes the deletion — strictly
    /// dominating (restore/resurrection) **or concurrent** (§2.7 edits
    /// beat deletes, review round 0): a put that wins against the
    /// deletion leaves no standing row in any arrival order, so
    /// manifests advertise one truth fleet-wide. A put the row dominates
    /// compares `Less`/`Equal` and leaves the row standing.
    fn clear_superseded_row(
        &self,
        txn: &StateTxn<'_>,
        vv: &VersionVector,
        item: &RelKey,
    ) -> Result<(), EngineError> {
        if let Some(row) = txn.get_deleted(item)? {
            if matches!(compare(vv, &row.vv), VvOrder::Greater | VvOrder::Concurrent) {
                txn.remove_deleted(item)?;
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
            r.admitted_ts = Some(now);
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
///    kind, bump `vv[self]` **past the withdrawn upload intent too**
///    (the del's vv strictly supersedes every version this device ever
///    minted, so it can never collide with an admitted-but-unpublished
///    put's vv — review round 0) and stage the `del` journal entry
///    carrying it; mark every such record
///    [`crate::state::ItemRecord::deleted`] (hidden; local trash is
///    app-side; the record keeps the deleted *content* version's head
///    identity, which is what restore re-advertises); record one §2.3
///    deleted-set row **per deleted item**, keyed by the item's file
///    relkey with that item's del vv (§2.3 "one row per deleted key").
///
/// Addressing (review round 0): `Sidecar` resolves suffix-aware — an
/// `image` already ending in `.rrdata` (a virtual copy, or the primary
/// named by its own file path) IS the item; `Original` and `Xmp` items
/// are self-keyed by the passed relkey (the xmp's own file relkey, which
/// the caller names — it is not derivable from the image's). Kinds with
/// no engine delete lane (previews/thumbs/meta) are skipped.
///
/// A kind with no item record is skipped (deleting a never-synced kind
/// is a no-op, not an error); naming no existing item at all is
/// [`EngineError::UnknownItem`].
///
/// # Whole-photo only (round 5)
///
/// §2.7/§3.4 model deletion as a WHOLE-PHOTO action — all of an image's
/// kinds together. Deleting only one half of an otherwise-live image
/// (e.g. just `Kind::Original` while its base sidecar stays live, or vice
/// versa) is **not supported**: it leaves the forbidden half-deleted
/// shape that [`reconcile_wholeness`] exists to heal, so the next
/// quiescent reconciliation re-advertises the deleted half and the
/// partial delete is silently undone. (That re-advertisement is the
/// INTENDED behavior for the shape when it arises from an
/// overwrite-vs-delete or edit-beats-delete race — reconcile cannot tell
/// a deliberate partial delete from a half-deleted-by-race state.) Pass
/// every kind the image holds to delete the photo; a single-kind slice is
/// honored on the wire but will not persist against a live counterpart.
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
        // Only kinds with an engine item lane; the item key resolves
        // per the module's file-relkey convention (doc: Addressing).
        if !matches!(kind, Kind::Sidecar | Kind::Original | Kind::Xmp) {
            continue;
        }
        let item = item_key_for_kind(image, kind)?;
        if let Some(record) = db.get_item(&item)? {
            if record.kind == kind {
                let mut vv = record.vv.clone();
                if let Some(admitted) = &record.admitted_vv {
                    // The del supersedes the withdrawn in-flight intent
                    // too, so no put and del can ever share a vv.
                    vv.merge(admitted);
                }
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
                r.admitted_vv = None;
                r.admitted_ts = None;
            })?;
            t.queue_remove(Queue::Up, item)?;
            t.queue_remove(Queue::Down, item)?;
            // §2.3: one deleted-set row per deleted item key, carrying
            // that item's own del vv (per-lineage; review round 0).
            t.record_deleted(
                item,
                &DeletedRecord {
                    vv: vv.clone(),
                    server_ts,
                },
            )?;
            staged.push(item.clone());
        }
        Ok(())
    })?;

    Ok(DeleteOutcome { tombstone, staged })
}

/// §2.7 restore from "Recently Deleted": for every deleted item of
/// `image`, stage a metadata-only [`EnginePut`] whose vv dominates the
/// deletion (elementwise max of the record's vv and the item's known
/// deleted-set row, plus a `vv[self]` bump), re-advertising the record's
/// known `blake3`/`sem_hash`/`content_id` — the data keys are still in
/// the bucket during grace, so restore moves **no bytes**. Clears the
/// `deleted` flags and this device's per-item deleted-set rows in the
/// same transaction. Returns the restored item relkeys.
///
/// Targets are matched **structurally** ([`item_belongs_to_image`]): the
/// original, the primary sidecar, and the image's 6-hex virtual copies —
/// never a sibling image whose relkey merely extends `<image>.` (review
/// round 0). An xmp or vc item can also be restored by passing its own
/// file relkey as `image` (the exact-match arm), mirroring
/// [`delete_item`]'s addressing.
///
/// Transfer-lane normalization (review round 0 — restore used to wedge
/// items off the queues): a restored `PendingDown` item with a known
/// `blake3` is re-pushed onto the download queue (the delete dequeued
/// it, and nothing else ever would have); a restored item stranded in
/// the upload lane (`Queued`/`Uploading`/`Verifying` — its intent was
/// withdrawn by the delete) is demoted to `Dirty`, so the next
/// [`admit_pending`] re-admits the **local file's** content as a new
/// version. (When that file still matches the restored version this
/// re-uploads one semantically identical version, which every receiver
/// converges case-1; when it holds a newer edit — the probe's lost-v2
/// shape — this is exactly what publishes it.)
///
/// An image with nothing deleted is [`EngineError::NotDeleted`].
pub fn restore_item(db: &SyncDb, image: &RelKey) -> Result<Vec<RelKey>, EngineError> {
    let own = db.device_id().clone();
    let now = crate::transfer::server_ts_estimate(db)?;
    let targets: Vec<(RelKey, ItemRecord)> = db
        .iter_items()?
        .into_iter()
        .filter(|(key, record)| record.deleted && item_belongs_to_image(key, image))
        .collect();
    if targets.is_empty() {
        return Err(EngineError::NotDeleted {
            relkey: image.clone(),
        });
    }
    let mut restored = Vec::new();
    db.with_txn_err::<_, EngineError>(|t| {
        for (relkey, record) in &targets {
            let mut vv = record.vv.clone();
            if let Some(row) = t.get_deleted(relkey)? {
                vv.merge(&row.vv);
            }
            vv.bump(&own);
            // A record without a published blake3 has nothing to
            // re-advertise (an EnginePut is unconstructible without one —
            // module docs); it is un-hidden locally only.
            if let Some(blake3) = record.blake3.clone() {
                // §1.2: derive from the advertised blake3, never copy
                // `record.content_id` (see resurrect_original) — a deleted
                // original is not normally dirty-overwritten, but the same
                // latent mislabel defect applies.
                let content_id =
                    (record.kind == Kind::Original).then(|| ContentId::from_blake3(&blake3));
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
                    content_id,
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
            // Transfer-lane normalization (doc comment).
            match record.state {
                ItemState::PendingDown => {
                    if record.blake3.is_some() {
                        t.queue_push(Queue::Down, relkey, transfer_class(record.kind))?;
                    }
                }
                state @ (ItemState::Queued | ItemState::Uploading) => {
                    t.transition(relkey, state, ItemState::Dirty, |_| {})?;
                }
                ItemState::Verifying => {
                    // No direct Verifying → Dirty edge: via the legal
                    // retry demotion first.
                    t.transition(relkey, ItemState::Verifying, ItemState::Queued, |_| {})?;
                    t.transition(relkey, ItemState::Queued, ItemState::Dirty, |_| {})?;
                }
                _ => {}
            }
            t.remove_deleted(relkey)?;
            restored.push(relkey.clone());
        }
        Ok(())
    })?;
    Ok(restored)
}

/// The image a **base** sidecar item (`<image>.rrdata`) belongs to, or
/// `None` for a virtual-copy sidecar (`<image>.<6hex>.rrdata`, whose
/// original is reached through the base sidecar) or a non-sidecar key.
/// Used only to enumerate images for [`reconcile_wholeness`]; a RAW whose
/// own stem happens to end in `.<6hex>` reads as a vc here and is simply
/// skipped (a harmless miss, never a wrong re-advertisement).
fn image_of_base_sidecar_item(item: &RelKey) -> Option<RelKey> {
    let rest = item.as_str().strip_suffix(".rrdata")?;
    if let Some((_, tail)) = rest.rsplit_once('.') {
        if crate::hexutil::is_lower_hex(tail, 6) {
            return None;
        }
    }
    RelKey::new(rest.to_string()).ok()
}

/// Re-advertise `rec` (a held, tombstoned item) as a live put dominating
/// its own del — the metadata-only half the resurrection lanes share with
/// [`restore_item`]. Returns `false` (minting nothing) when this device
/// does not hold the bytes (no `blake3`): an [`EnginePut`] is
/// unconstructible without one, so another holder must close the gap.
///
/// The resurrection vv is built from the item's **own** lineage
/// (`rec.vv` ∪ its deleted-set row) plus a self bump — strictly per item
/// key, never mixing a sibling's lineage.
fn resurrect_tombstoned_item(
    t: &StateTxn<'_>,
    own: &DeviceId,
    now: i64,
    item: &RelKey,
    rec: &ItemRecord,
) -> Result<bool, EngineError> {
    let Some(blake3) = rec.blake3.clone() else {
        return Ok(false);
    };
    let mut vv = rec.vv.clone();
    if let Some(row) = t.get_deleted(item)? {
        vv.merge(&row.vv);
    }
    vv.bump(own);
    // §1.2: derive from the advertised blake3, never copy `rec.content_id`
    // (see resurrect_original) — same latent defect.
    let content_id = (rec.kind == Kind::Original).then(|| ContentId::from_blake3(&blake3));
    let put = EnginePut {
        device: own.clone(),
        kind: rec.kind,
        item: item.clone(),
        vv: vv.clone(),
        blake3,
        size: rec.size,
        ts: now,
        sem_hash: rec.sem_hash.clone(),
        rating: rec.rating,
        color_label: rec.color_label.clone(),
        content_id,
        w: rec.w,
        h: rec.h,
        mtime: (rec.kind == Kind::Original).then(|| rec.mtime_unix_ns.div_euclid(1_000_000_000)),
    };
    enqueue_entry_in(t, own, &put.entry())?;
    t.update_item(item, rec.state, |r| {
        r.vv = vv.clone();
        r.deleted = false;
        r.head_ts = Some(now);
        r.device = Some(own.clone());
    })?;
    // Transfer-lane normalization, mirroring restore_item/resurrect_original:
    // an un-hidden PendingDown record must re-push its Down row or it is
    // pump-invisible until the next startup sweep.
    if rec.state == ItemState::PendingDown {
        t.queue_push(Queue::Down, item, transfer_class(rec.kind))?;
    }
    // The item's own deleted-set row is superseded by the resurrection put.
    if let Some(row) = t.get_deleted(item)? {
        if matches!(
            compare(&vv, &row.vv),
            VvOrder::Greater | VvOrder::Concurrent
        ) {
            t.remove_deleted(item)?;
        }
    }
    Ok(true)
}

/// §2.7/§2.11 whole-item wholeness reconciliation — the quiescent,
/// order-independent fallback re-advertiser (review round 3; two verified
/// blockers + one major).
///
/// Call it at **quiescence** — after a [`crate::reader::poll`] has applied
/// every entry currently available, alongside [`publish_pending`] — never
/// mid-stream: it reads the device's CONVERGED local state, so its
/// decision is a pure function of that state, identical for every arrival
/// order (a fresh replay that stops short of quiescence simply adopts the
/// resurrection puts this already minted on an online device, and never
/// re-mints — the §2.11 order-equivalence the scenario suite pins). For
/// every image left in the forbidden half-deleted shape it mints a
/// metadata-only resurrection (the data keys are still in the bucket
/// during the §2.10 grace window):
///
/// - **sidecar live while the original is tombstoned** (§2.7) — the
///   original is re-advertised, so §2.10 GC can never destroy a RAW a
///   live sidecar still references. This is the catch-all the author-only
///   `apply_del` lanes miss: a committed edit whose author went
///   permanently offline before applying the delete (BLOCKER 1), or an
///   edit whose surviving device never learned the original and could
///   only fire [`ResurrectionIncompleteEvent`] (BLOCKER 2). A device that
///   does not hold the original either (no record / no `blake3`) re-fires
///   that event and mints nothing — the deleter, or any holder, or the
///   §2.10 GC worker (which re-applies journals through the same engine)
///   closes it.
/// - **original live while its base sidecar is tombstoned** (§2.11
///   "resurrection restores a whole item") — the sidecar's develop edits
///   are re-advertised, the converse lane the engine had no path for: an
///   original-overwrite (§2.8) that survives a concurrent whole-photo
///   delete used to drop the photo's edits silently (MAJOR 3). Symmetric
///   with the forward lane (round 5): a device that holds the tombstoned
///   sidecar record but cannot re-advertise its edits (a `blake3`-less
///   deleted record) mints nothing and re-fires
///   [`ResurrectionIncompleteEvent`] for the sidecar, so a live original
///   whose edits no reachable device can resurrect is reported, not
///   silently dropped. (A live original with NO sidecar record is a
///   normal shape, not a forbidden half-deleted one, so it fires nothing
///   — the converse of the forward lane's record-is-`None` arm has no
///   forbidden shape to signal.)
///
/// This heals the shape unconditionally: it cannot distinguish a
/// half-deleted-by-race state (where re-advertisement is the §2.11
/// intent) from a deliberate single-kind [`delete_item`] of one half of
/// a live image. The latter is therefore unsupported — a partial delete
/// whose counterpart stays live is re-advertised here at the next
/// quiescence (see [`delete_item`]); whole-photo deletes tombstone both
/// kinds, so neither lane matches and nothing is undone.
///
/// Holder-based and idempotent: the resurrection put carries the item's
/// own content hash, so N holders re-advertising the same head emit
/// byte-identical-content puts that the §2.6 case-1 lane
/// ([`EngineConsumer::converge`]) vv-max-merges into one live version —
/// no divergence, exactly the dedup model §2.6 already relies on. Once the
/// whole item is live everywhere, the shape no longer matches and nothing
/// is minted, so repeated calls converge to a fixed point. Returns the
/// item relkeys re-advertised this call (ascending, deduplicated).
pub fn reconcile_wholeness(
    db: &SyncDb,
    events: &mut impl EngineEvents,
) -> Result<Vec<RelKey>, EngineError> {
    let own = db.device_id().clone();
    let now = crate::transfer::server_ts_estimate(db)?;
    // Enumerate every image with an original or a base sidecar record.
    let mut images: Vec<RelKey> = Vec::new();
    for (key, record) in db.iter_items()? {
        let image = match record.kind {
            Kind::Original => Some(key.clone()),
            Kind::Sidecar => image_of_base_sidecar_item(&key),
            _ => None,
        };
        if let Some(image) = image {
            if !images.contains(&image) {
                images.push(image);
            }
        }
    }
    images.sort();

    let mut minted: Vec<RelKey> = Vec::new();
    let mut incomplete: Vec<RelKey> = Vec::new();
    db.with_txn_err::<(), EngineError>(|t| {
        for image in &images {
            let sidecar_rel = sidecar_item_relkey(image)?;
            let original = t.get_item(image)?.filter(|r| r.kind == Kind::Original);
            let sidecar = t
                .get_item(&sidecar_rel)?
                .filter(|r| r.kind == Kind::Sidecar);
            let sidecar_live = sidecar.as_ref().is_some_and(|r| !r.deleted);
            let original_live = original.as_ref().is_some_and(|r| !r.deleted);

            // §2.7: a live sidecar must never leave its original tombstoned.
            if sidecar_live {
                match &original {
                    Some(rec) if rec.deleted => {
                        if resurrect_tombstoned_item(t, &own, now, image, rec)? {
                            minted.push(image.clone());
                        } else {
                            incomplete.push(image.clone());
                        }
                    }
                    // The original was never known here (or is unheld): this
                    // device cannot resurrect it; another holder must.
                    None => incomplete.push(image.clone()),
                    _ => {}
                }
            }
            // §2.11: a live original must never leave its base sidecar's
            // (edits) tombstoned — the converse lane. Symmetric with the
            // forward lane: when this device holds the tombstoned sidecar
            // record but cannot re-advertise its edits (no `blake3`), it
            // mints nothing and surfaces the gap. (The forward lane's
            // record-is-`None` arm has no converse: a live original with no
            // sidecar record is a normal shape, not a forbidden one, so an
            // absent sidecar never fires the event here.)
            if original_live {
                if let Some(rec) = &sidecar {
                    if rec.deleted {
                        if resurrect_tombstoned_item(t, &own, now, &sidecar_rel, rec)? {
                            minted.push(sidecar_rel.clone());
                        } else {
                            incomplete.push(sidecar_rel.clone());
                        }
                    }
                }
            }
        }
        Ok(())
    })?;
    for image in incomplete {
        events.resurrection_incomplete(ResurrectionIncompleteEvent { relkey: image });
    }
    minted.sort();
    minted.dedup();
    Ok(minted)
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
