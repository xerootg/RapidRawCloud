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

use crate::clock::{DeviceId, VersionVector};
use crate::journal::{JournalEntry, JournalError, Kind, Tombstone};
use crate::keys::{KeyClass, KeyError, RelKey};
use crate::publisher::PublisherError;
use crate::reader::{ConsumerError, JournalConsumer};
use crate::s3::{S3Api, S3Error};
use crate::semhash::{Blake3Hex, ContentId, SemHash, SemHashError};
use crate::state::{ItemRecord, StateError, StateTxn, SyncDb};
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
    let _ = image;
    todo!("P1-U5: sidecar item relkey (<image>.rrdata)")
}

/// The items-table key of `image`'s **virtual-copy sidecar** with
/// 6-lowercase-hex suffix `vc6`: `<image>.<vc6>.rrdata`. Rejects a
/// malformed suffix ([`KeyError::BadVcSuffix`]).
pub fn vc_item_relkey(image: &RelKey, vc6: &str) -> Result<RelKey, KeyError> {
    let _ = (image, vc6);
    todo!("P1-U5: vc item relkey (<image>.<6hex>.rrdata)")
}

/// The deterministic §2.6 loser virtual-copy suffix:
/// `blake3(canonical loser document)[..6]` — six lowercase hex chars of
/// the blake3 of [`crate::semhash::canonical_json`] over the parsed
/// loser sidecar document. Canonicalization makes the suffix a function
/// of the document's **content**, not its spelling, so every holder of
/// the loser materializes the **same** key (§2.6 / review B2). Invalid
/// JSON fails closed like [`crate::semhash::sem_hash`].
pub fn loser_vc_suffix(loser_doc: &[u8]) -> Result<String, EngineError> {
    let _ = loser_doc;
    todo!("P1-U5: blake3(canonical loser doc)[..6]")
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
    let _ = (image, displaced);
    todo!("P1-U5: §2.8 deterministic conflict-copy relkey")
}

/// The items-table key a classified bucket key addresses (`None` for
/// control-plane and foreign keys): the single inverse of the module's
/// file-relkey convention — `Original`/`Xmp` keep their relkey,
/// `Sidecar { relkey, vc: None }` maps to `<relkey>.rrdata`, and
/// `Sidecar { relkey, vc: Some(h) }` to `<relkey>.<h>.rrdata`.
pub fn item_relkey_for(class: &KeyClass) -> Option<RelKey> {
    let _ = class;
    todo!("P1-U5: bucket-key class -> items-table key")
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
///   `PendingDown → Dirty` records the §3.4 offline-edit lane; a `Stub`
///   original whose bytes were replaced out-of-band takes the §2.8
///   guarded [`crate::state::SyncDb::replay_put_item_cas`] bypass,
///   `Stub` → `Dirty` with the new `content_id`), refresh
///   `sem_hash`/badges (sidecars) or `content_id` (originals), and
///   leave the record's `vv`/`blake3`/`size` advertising the last
///   published version per the §2.6 coordination note — admission owns
///   the version mint. An already-`Dirty` item just refreshes the
///   scanned identity.
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
    let _ = (db, image, kind, scan);
    todo!("P1-U5: §2.5 churn-gated local change intake")
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
    quiesced: impl FnMut(&RelKey, &ItemRecord) -> bool,
) -> Result<Vec<RelKey>, EngineError> {
    let _ = (db, quiesced);
    todo!("P1-U5: §3.7 quiescence-gated admission (vv bump + intent snapshot)")
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
        todo!("P1-U5: EnginePut -> always-blake3 v1 journal entry")
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
/// [`JournalConsumer`] error contract).
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
        let _ = (db, sync_root.into(), &events);
        todo!("P1-U5: consumer construction (identity + server-time snapshot)")
    }

    /// Test hook: pins the pass's "now" (the `ts`/`head_ts` stamped on
    /// in-consumer admissions and resurrection puts) instead of the
    /// constructor's estimate, making §2.6 tiebreaks deterministic in
    /// unit tests.
    pub fn with_now(mut self, now_unix: i64) -> Self {
        self.now_unix = now_unix;
        self
    }
}

impl<E: EngineEvents> JournalConsumer for EngineConsumer<'_, E> {
    fn apply(&mut self, txn: &StateTxn<'_>, entry: &JournalEntry) -> Result<(), ConsumerError> {
        let _ = (txn, entry, &self.own_device, &self.sync_root, self.now_unix);
        let _ = &mut self.events;
        todo!("P1-U5: §2.6 unified apply rule")
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
    let _ = (db, s3, bucket, image, kinds);
    todo!("P1-U5: §2.7 soft delete (tombstone PUT + del entries + hidden records)")
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
    let _ = (db, image);
    todo!("P1-U5: §2.7 metadata-only restore (dominating-vv puts)")
}

/// The §2.7 "Recently Deleted" listing: every item record carrying the
/// `deleted` flag, ascending by relkey.
pub fn recently_deleted(db: &SyncDb) -> Result<Vec<(RelKey, ItemRecord)>, EngineError> {
    let _ = db;
    todo!("P1-U5: deleted-flag listing")
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
    let _ = (sync_root, item);
    todo!("P1-U5: file-relkey local path")
}
