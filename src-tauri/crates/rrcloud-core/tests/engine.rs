//! The unit suite for `rrcloud_core::engine` (architecture §2.5–§2.8):
//! the file-relkey item keying, the §2.5 churn-gated local change
//! intake, §3.7 quiescence admission (the §2.6 version mint and its
//! `admitted_vv`/`head_ts` intent snapshot), the §2.6 unified apply
//! rule driven directly through [`EngineConsumer`] (cases 1–4, the
//! deterministic winner, loser materialization and its `(key,
//! sem_hash)` dedup), §2.7 soft delete / restore / edits-beat-deletes
//! resurrection, the §2.8 original-overwrite conflict copies, the
//! always-blake3 [`EnginePut`] (S8's unit half), and the additive seams
//! this unit owns in `transfer` (suffix-aware sidecar mapping, the
//! admitted-vv entry seam) and `manifest` (badge/device provenance,
//! deleted-flag withholding).
//!
//! Garage-backed only where a real bucket is the thing under test
//! (upload seam, tombstone PUT, restore); the apply-rule core drives
//! the consumer directly against scratch state dbs.

mod common;

use std::path::Path;

use common::engine as eh;
use common::engine::RecordedEvents;
use common::garage;
use common::sync::{dev, open_db, rel, DEV_A, DEV_B, DEV_C, DEV_X};
use common::transfer as th;
use common::transfer::CountingS3;
use rrcloud_core::clock::{compare, VersionVector, VvOrder};
use rrcloud_core::engine::{
    admit_pending, delete_item, item_local_path, item_relkey_for, loser_vc_suffix,
    notify_local_change, original_conflict_relkey, recently_deleted, reconcile_wholeness,
    restore_item, sidecar_item_relkey, vc_item_relkey, ChangeOutcome, EngineConsumer, EngineError,
    EnginePut, LocalScan, ResurrectionIncompleteEvent,
};
use rrcloud_core::journal::JournalEntryExt as _;
use rrcloud_core::journal::{JournalEntry, Kind, Op, Tombstone, JOURNAL_VERSION};
use rrcloud_core::keys::{
    classify_key, library_key, local_path, sidecar_key, tombstone_key, vc_sidecar_key, KeyClass,
    KeyError, RelKey,
};
use rrcloud_core::manifest::build_manifest;
use rrcloud_core::reader::{ConsumerError, JournalConsumer};
use rrcloud_core::semhash::{sem_hash, sidecar_badges, Blake3Hex, ContentId};
use rrcloud_core::state::{DeletedRecord, ItemRecord, ItemState, Queue, SyncDb};
use rrcloud_core::transfer::{bucket_key_for, local_target_path, upload_item};

// ---------------------------------------------------------------------------
// Local helpers
// ---------------------------------------------------------------------------

/// A version vector from literal (device, counter) pairs.
fn vv(pairs: &[(&str, u32)]) -> VersionVector {
    pairs
        .iter()
        .map(|(d, n)| (dev(d), *n))
        .collect::<VersionVector>()
}

/// A seeded item record; customize by mutation.
fn rec(kind: Kind, state: ItemState) -> ItemRecord {
    ItemRecord {
        kind,
        state,
        size: 0,
        mtime_unix_ns: 0,
        blake3: None,
        sem_hash: None,
        vv: VersionVector::new(),
        content_id: None,
        w: None,
        h: None,
        pinned: false,
        last_access_unix: 0,
        verified_remote: false,
        attested: false,
        base_unknown: false,
        rating: None,
        color_label: None,
        device: None,
        head_ts: None,
        admitted_vv: None,
        admitted_ts: None,
        deleted: false,
    }
}

/// A `put sidecar` entry for `image` advertising `doc`, with explicit
/// `vv` and `ts` (the §2.6 inputs the tests control).
fn put_sidecar(
    author: &str,
    image: &RelKey,
    doc: &[u8],
    v: VersionVector,
    ts: i64,
) -> JournalEntry {
    let badges = sidecar_badges(doc).expect("doc badges");
    JournalEntry {
        v: JOURNAL_VERSION,
        seq: 0,
        ts,
        device: dev(author),
        op: Op::Put,
        kind: Kind::Sidecar,
        key: sidecar_key(image),
        vv: v,
        size: Some(doc.len() as u64),
        blake3: Some(Blake3Hex::from_bytes(doc)),
        sem_hash: Some(sem_hash(doc).expect("doc sem")),
        rating: badges.rating,
        color_label: badges.color_label,
        content_id: None,
        w: None,
        h: None,
        mtime: None,
        from_key: None,
    }
}

/// A `put original` entry for `image` advertising `bytes`.
fn put_original(
    author: &str,
    image: &RelKey,
    bytes: &[u8],
    v: VersionVector,
    ts: i64,
) -> JournalEntry {
    JournalEntry {
        v: JOURNAL_VERSION,
        seq: 0,
        ts,
        device: dev(author),
        op: Op::Put,
        kind: Kind::Original,
        key: library_key(image),
        vv: v,
        size: Some(bytes.len() as u64),
        blake3: Some(Blake3Hex::from_bytes(bytes)),
        sem_hash: None,
        rating: None,
        color_label: None,
        content_id: Some(ContentId::from_bytes(bytes)),
        w: None,
        h: None,
        mtime: Some(1_769_899_000),
        from_key: None,
    }
}

/// A `del` entry for the given bucket key.
fn del(author: &str, kind: Kind, key: String, v: VersionVector, ts: i64) -> JournalEntry {
    JournalEntry {
        v: JOURNAL_VERSION,
        seq: 0,
        ts,
        device: dev(author),
        op: Op::Del,
        kind,
        key,
        vv: v,
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

/// Applies one entry through the consumer inside one state transaction
/// (what the reader's apply composite does, minus the cursor
/// bookkeeping that is irrelevant to these units).
fn apply(db: &SyncDb, consumer: &mut EngineConsumer<'_, RecordedEvents>, entry: &JournalEntry) {
    db.with_txn_err::<_, ConsumerError>(|t| consumer.apply(t, entry))
        .expect("consumer apply");
}

/// A scratch db + sync-root pair for consumer-only tests.
fn scratch(device: &str) -> (tempfile::TempDir, tempfile::TempDir, SyncDb) {
    let (dbdir, _path, db) = open_db(&dev(device));
    let root = tempfile::tempdir().expect("sync root");
    (dbdir, root, db)
}

/// The item's record, which must exist.
fn item(db: &SyncDb, key: &RelKey) -> ItemRecord {
    db.get_item(key)
        .expect("get_item")
        .unwrap_or_else(|| panic!("no record for {key}"))
}

/// Every queued relkey of `q`, drained (tests call this last).
fn drain_queue(db: &SyncDb, q: Queue) -> Vec<String> {
    let mut out = Vec::new();
    while let Some((k, _)) = db.queue_pop(q).expect("queue_pop") {
        out.push(k.as_str().to_string());
    }
    out.sort();
    out
}

const IMG: &str = "p/img.NEF";

// ===========================================================================
// Item keying (module docs: the file-relkey convention)
// ===========================================================================

#[test]
fn sidecar_item_relkey_is_the_rrdata_file_path() {
    let image = rel(IMG);
    let item = sidecar_item_relkey(&image).expect("sidecar item relkey");
    assert_eq!(item.as_str(), "p/img.NEF.rrdata");
    // The uniform bucket key of the item equals the canonical sidecar
    // wire key of the image — one spelling on the wire.
    assert_eq!(library_key(&item), sidecar_key(&image));
}

#[test]
fn vc_item_relkey_is_the_vc_file_path_and_validates_the_suffix() {
    let image = rel(IMG);
    let item = vc_item_relkey(&image, "ab12cd").expect("vc item relkey");
    assert_eq!(item.as_str(), "p/img.NEF.ab12cd.rrdata");
    assert_eq!(
        library_key(&item),
        vc_sidecar_key(&image, "ab12cd").expect("vc key")
    );
    for bad in ["AB12CD", "ab12c", "ab12cde", "ab12cg", ""] {
        assert!(
            matches!(vc_item_relkey(&image, bad), Err(KeyError::BadVcSuffix(_))),
            "suffix {bad:?} must be rejected"
        );
    }
}

#[test]
fn item_relkey_for_inverts_the_library_classifications() {
    let image = rel(IMG);
    let sidecar = sidecar_item_relkey(&image).expect("sidecar");
    let cases: Vec<(String, &RelKey)> = vec![
        (library_key(&image), &image),
        (sidecar_key(&image), &sidecar),
    ];
    for (key, want) in &cases {
        let class = classify_key(key);
        assert_eq!(
            item_relkey_for(&class).as_ref(),
            Some(*want),
            "classify({key}) must map back to the item relkey"
        );
    }
    // Virtual copy.
    let vc = vc_item_relkey(&image, "ab12cd").expect("vc");
    let class = classify_key(&library_key(&vc));
    assert_eq!(
        class,
        KeyClass::Sidecar {
            relkey: image.clone(),
            vc: Some("ab12cd".to_string())
        },
        "precondition: the vc file key classifies with the 6-hex suffix"
    );
    assert_eq!(item_relkey_for(&class), Some(vc));
    // Xmp keeps its own file path.
    let xmp = rel("p/img.xmp");
    assert_eq!(
        item_relkey_for(&classify_key(&library_key(&xmp))),
        Some(xmp)
    );
    // Control-plane and foreign keys have no item. (The non-NFC spelling
    // uses `e\u{301}`, the suite's composable NFD pair — `t\u{301}` has
    // no precomposition, so a key carrying it is already NFC and
    // legitimately classifies.)
    for key in [
        ".rrcloud/v1/tombstones/0123456789abcdef0123456789abcdef.json".to_string(),
        "random/garbage".to_string(),
        "library/cafe\u{301}.NEF".to_string(),
    ] {
        assert_eq!(item_relkey_for(&classify_key(&key)), None, "key {key:?}");
    }
}

#[test]
fn loser_vc_suffix_is_content_deterministic_six_hex() {
    let d = eh::doc(3, Some("red"), 0.25);
    let suffix = loser_vc_suffix(&d).expect("suffix");
    assert_eq!(suffix.len(), 6);
    assert!(
        suffix
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "suffix {suffix:?} must be lowercase hex"
    );
    // Pinned derivation (§2.6, churn-stable — review round 0): the
    // canonical loser document is the §2.5 SEMANTIC canonical form, so
    // the suffix is the first 6 hex of the loser's sem_hash.
    let expected = sem_hash(&d).expect("sem");
    assert_eq!(suffix, expected.as_str()[..6].to_string());
    // Churn-stable: an EXIF-cache rewrite (same semantics, different
    // bytes) must NOT move the key, or two holders of one loser would
    // materialize two different vcs (the review's duplicate-vc finding).
    assert_eq!(
        loser_vc_suffix(&eh::churned(&d)).expect("churned suffix"),
        suffix,
        "a §2.5 churn rewrite must not move the loser's vc key"
    );
    // Spelling-insensitive: key order and number spelling do not move
    // the key (two writers of the same document agree).
    let respelled =
        br#"{"tags":["color:red"],"rating":3.0,"version":1,"adjustments":{"exposure":2.5e-1,"contrast":0},"exif":{"iso":100,"camera":"TestCam"}}"#;
    assert_eq!(
        loser_vc_suffix(respelled).expect("respelled suffix"),
        suffix,
        "canonicalization must make the suffix content-determined"
    );
    // Content-sensitive.
    assert_ne!(
        loser_vc_suffix(&eh::doc(4, Some("red"), 0.25)).expect("other"),
        suffix
    );
    // Fail closed on garbage, like sem_hash.
    assert!(loser_vc_suffix(b"not json").is_err());
}

#[test]
fn original_conflict_relkey_is_deterministic_stem_conflict_ext() {
    let image = rel(IMG);
    let displaced = ContentId::from_bytes(b"displaced bytes");
    let first = original_conflict_relkey(&image, &displaced).expect("conflict relkey");
    assert_eq!(
        first.as_str(),
        format!("p/img.conflict-{}.NEF", &displaced.as_str()[..6])
    );
    // Deterministic and content-keyed.
    assert_eq!(
        original_conflict_relkey(&image, &displaced).expect("again"),
        first
    );
    assert_ne!(
        original_conflict_relkey(&image, &ContentId::from_bytes(b"other bytes")).expect("other"),
        first
    );
    // No extension: no trailing dot (which RelKey would reject anyway).
    let bare = rel("p/scan");
    let conflict = original_conflict_relkey(&bare, &displaced).expect("bare conflict");
    assert_eq!(
        conflict.as_str(),
        format!("p/scan.conflict-{}", &displaced.as_str()[..6])
    );
}

#[test]
fn item_local_path_is_the_plain_file_path() {
    let root = Path::new("/sync/root");
    let image = rel(IMG);
    let sidecar = sidecar_item_relkey(&image).expect("sidecar");
    assert_eq!(item_local_path(root, &image), local_path(&image, root));
    assert_eq!(
        item_local_path(root, &sidecar),
        root.join("p").join("img.NEF.rrdata")
    );
}

// ===========================================================================
// Transfer seam: the suffix-aware sidecar mapping (additive)
// ===========================================================================

#[test]
fn transfer_mapping_is_suffix_aware_for_engine_keyed_sidecars() {
    // Engine-keyed sidecar items carry the .rrdata suffix IN the relkey;
    // the transfer mapping must not append a second one.
    let item = rel("p/img.NEF.rrdata");
    assert_eq!(
        bucket_key_for(&item, Kind::Sidecar).expect("bucket key"),
        "library/p/img.NEF.rrdata"
    );
    let root = Path::new("/sync/root");
    assert_eq!(
        local_target_path(root, &item, Kind::Sidecar),
        root.join("p").join("img.NEF.rrdata")
    );
    // Virtual-copy spelling too.
    let vc = rel("p/img.NEF.ab12cd.rrdata");
    assert_eq!(
        bucket_key_for(&vc, Kind::Sidecar).expect("vc bucket key"),
        "library/p/img.NEF.ab12cd.rrdata"
    );
    assert_eq!(
        local_target_path(root, &vc, Kind::Sidecar),
        root.join("p").join("img.NEF.ab12cd.rrdata")
    );
    // The landed base-relkey convention is untouched.
    let base = rel(IMG);
    assert_eq!(
        bucket_key_for(&base, Kind::Sidecar).expect("base"),
        "library/p/img.NEF.rrdata"
    );
    assert_eq!(
        local_target_path(root, &base, Kind::Sidecar),
        root.join("p").join("img.NEF.rrdata")
    );
}

// ===========================================================================
// §2.5 local change intake (notify_local_change)
// ===========================================================================

/// A scan over in-memory bytes with a fixed mtime.
fn scan(bytes: &[u8], mtime_unix_ns: i64) -> LocalScan<'_> {
    LocalScan {
        size: bytes.len() as u64,
        mtime_unix_ns,
        bytes,
    }
}

#[test]
fn notify_creates_a_dirty_sidecar_item_with_badges() {
    let (_d, _r, db) = scratch(DEV_A);
    let image = rel(IMG);
    let d = eh::doc(4, Some("blue"), 0.5);
    let outcome =
        notify_local_change(&db, &image, Kind::Sidecar, &scan(&d, 7_000)).expect("notify");
    assert_eq!(outcome, ChangeOutcome::MarkedDirty);

    let key = sidecar_item_relkey(&image).expect("item key");
    let record = item(&db, &key);
    assert_eq!(record.kind, Kind::Sidecar);
    assert_eq!(record.state, ItemState::Dirty);
    assert_eq!(record.sem_hash, Some(sem_hash(&d).expect("sem")));
    assert_eq!(record.rating, Some(4));
    assert_eq!(record.color_label, Some("blue".to_string()));
    assert_eq!(record.size, d.len() as u64);
    assert!(record.vv.is_empty(), "the version is minted at admission");
    assert_eq!(record.admitted_vv, None);
    assert_eq!(record.blake3, None);
    // Nothing queued yet: admission is a separate, quiescence-gated step.
    assert_eq!(db.queue_len(Queue::Up).expect("queue_len"), 0);
    assert_eq!(db.outbound_len().expect("outbound"), 0);
}

#[test]
fn notify_churn_rewrite_changes_nothing_at_all() {
    // §2.5 / S3's unit half: an EXIF-cache-style rewrite (same sem_hash,
    // different bytes) marks nothing dirty, bumps nothing, queues
    // nothing — the record is bit-identical before and after.
    let (_d, _r, db) = scratch(DEV_A);
    let image = rel(IMG);
    let key = sidecar_item_relkey(&image).expect("item key");
    let d1 = eh::doc(3, Some("red"), 0.25);

    // Seed a synced head whose published facts the churn must preserve.
    let mut seeded = rec(Kind::Sidecar, ItemState::Synced);
    seeded.sem_hash = Some(sem_hash(&d1).expect("sem"));
    seeded.blake3 = Some(Blake3Hex::from_bytes(&d1));
    seeded.size = d1.len() as u64;
    seeded.vv = vv(&[(DEV_A, 1)]);
    seeded.rating = Some(3);
    seeded.color_label = Some("red".to_string());
    seeded.device = Some(dev(DEV_A));
    seeded.head_ts = Some(1_769_900_000);
    db.insert_item(&key, &seeded).expect("seed");

    let rewritten = eh::churned(&d1);
    assert_ne!(rewritten, d1, "precondition: the bytes changed");
    assert_eq!(
        sem_hash(&rewritten).expect("sem"),
        sem_hash(&d1).expect("sem"),
        "precondition: the semantics did not"
    );
    let outcome =
        notify_local_change(&db, &image, Kind::Sidecar, &scan(&rewritten, 9_000)).expect("notify");
    assert_eq!(outcome, ChangeOutcome::Unchanged);
    assert_eq!(item(&db, &key), seeded, "record untouched, bit for bit");
    assert_eq!(db.queue_len(Queue::Up).expect("queue_len"), 0);
    assert_eq!(db.outbound_len().expect("outbound"), 0);
    assert!(
        admit_pending(&db, |_, _| true).expect("admit").is_empty(),
        "nothing became admissible"
    );
}

#[test]
fn notify_semantic_change_marks_dirty_but_keeps_published_identity() {
    let (_d, _r, db) = scratch(DEV_A);
    let image = rel(IMG);
    let key = sidecar_item_relkey(&image).expect("item key");
    let d1 = eh::doc(3, Some("red"), 0.25);
    let mut seeded = rec(Kind::Sidecar, ItemState::Synced);
    seeded.sem_hash = Some(sem_hash(&d1).expect("sem"));
    seeded.blake3 = Some(Blake3Hex::from_bytes(&d1));
    seeded.size = d1.len() as u64;
    seeded.vv = vv(&[(DEV_A, 1)]);
    seeded.rating = Some(3);
    seeded.color_label = Some("red".to_string());
    db.insert_item(&key, &seeded).expect("seed");

    let d2 = eh::doc(5, Some("green"), 0.5);
    let outcome =
        notify_local_change(&db, &image, Kind::Sidecar, &scan(&d2, 10_000)).expect("notify");
    assert_eq!(outcome, ChangeOutcome::MarkedDirty);
    let record = item(&db, &key);
    assert_eq!(record.state, ItemState::Dirty);
    // Badges are advisory and follow the document immediately (§2.2
    // intake contract) ...
    assert_eq!(record.rating, Some(5));
    assert_eq!(record.color_label, Some("green".to_string()));
    // ... while the integrity fields keep naming the last PUBLISHED
    // version until the §2.4 verify-commit (the §2.6 coordination note:
    // manifests pair blake3 with vv/size/hashes).
    assert_eq!(record.vv, seeded.vv);
    assert_eq!(record.blake3, seeded.blake3);
    assert_eq!(record.sem_hash, seeded.sem_hash);
    assert_eq!(record.size, seeded.size);
    assert_eq!(record.admitted_vv, None, "no version minted yet");
}

#[test]
fn notify_original_overwrite_from_stub_takes_the_guarded_bypass() {
    // §2.8: an out-of-band rewrite of an evicted original. Stub → Dirty
    // has no legal() edge (the bytes were replaced, not downloaded), so
    // intake must go through the replay_put_item_cas guarded bypass.
    let (_d, _r, db) = scratch(DEV_A);
    let image = rel(IMG);
    let old = th::patterned(512, 1);
    let mut seeded = rec(Kind::Original, ItemState::Stub);
    seeded.blake3 = Some(Blake3Hex::from_bytes(&old));
    seeded.content_id = Some(ContentId::from_bytes(&old));
    seeded.size = old.len() as u64;
    seeded.vv = vv(&[(DEV_A, 1)]);
    db.insert_item(&image, &seeded).expect("seed");

    let new = th::patterned(512, 2);
    let outcome =
        notify_local_change(&db, &image, Kind::Original, &scan(&new, 11_000)).expect("notify");
    assert_eq!(outcome, ChangeOutcome::MarkedDirty);
    assert_eq!(item(&db, &image).state, ItemState::Dirty);

    // Same bytes again: the content hash gate says unchanged.
    let mut hydrated = seeded.clone();
    hydrated.state = ItemState::Hydrated;
    let image2 = rel("p/other.NEF");
    db.insert_item(&image2, &hydrated).expect("seed 2");
    let outcome = notify_local_change(&db, &image2, Kind::Original, &scan(&old, 12_000))
        .expect("notify unchanged");
    assert_eq!(outcome, ChangeOutcome::Unchanged);
    assert_eq!(item(&db, &image2), hydrated);
}

#[test]
fn notify_kind_mismatch_is_typed() {
    let (_d, _r, db) = scratch(DEV_A);
    let image = rel(IMG);
    db.insert_item(&image, &rec(Kind::Original, ItemState::Hydrated))
        .expect("seed");
    let d = eh::doc(1, None, 0.0);
    // Addressing the ORIGINAL's record with kind Xmp (same item key) is
    // a caller bug, surfaced typed.
    let err = notify_local_change(&db, &image, Kind::Xmp, &scan(&d, 1)).expect_err("mismatch");
    assert!(
        matches!(err, EngineError::KindMismatch { .. }),
        "got {err:?}"
    );
}

// ===========================================================================
// §3.7 admission (admit_pending)
// ===========================================================================

#[test]
fn admission_mints_one_version_and_snapshots_the_intent() {
    let (_d, _r, db) = scratch(DEV_A);
    let image = rel(IMG);
    let key = sidecar_item_relkey(&image).expect("item key");
    let d1 = eh::doc(2, None, 0.1);
    notify_local_change(&db, &image, Kind::Sidecar, &scan(&d1, 1_000)).expect("notify");

    let admitted = admit_pending(&db, |_, _| true).expect("admit");
    assert_eq!(admitted, vec![key.clone()]);
    let record = item(&db, &key);
    assert_eq!(record.state, ItemState::Queued);
    // The record's own vv keeps naming the published history (empty for
    // a fresh item); the minted version lives in the intent snapshot.
    assert!(record.vv.is_empty());
    assert_eq!(record.admitted_vv, Some(vv(&[(DEV_A, 1)])));
    assert_eq!(record.device, Some(dev(DEV_A)));
    assert!(
        record.head_ts.is_some(),
        "admission freezes the §2.6 tiebreak ts"
    );
    assert_eq!(drain_queue(&db, Queue::Up), vec![key.as_str().to_string()]);

    // Re-admitting is a no-op: only Dirty items are candidates.
    assert!(admit_pending(&db, |_, _| true)
        .expect("admit again")
        .is_empty());
}

#[test]
fn admission_respects_the_quiescence_check() {
    let (_d, _r, db) = scratch(DEV_A);
    let calm = rel("a/calm.NEF");
    let busy = rel("a/busy.NEF");
    let d = eh::doc(1, None, 0.0);
    notify_local_change(&db, &calm, Kind::Sidecar, &scan(&d, 1)).expect("notify calm");
    notify_local_change(&db, &busy, Kind::Sidecar, &scan(&d, 2)).expect("notify busy");
    let calm_key = sidecar_item_relkey(&calm).expect("calm key");
    let busy_key = sidecar_item_relkey(&busy).expect("busy key");

    let admitted = admit_pending(&db, |k, _| *k == calm_key).expect("admit with quiescence filter");
    assert_eq!(admitted, vec![calm_key.clone()]);
    assert_eq!(item(&db, &busy_key).state, ItemState::Dirty, "held back");
    assert_eq!(item(&db, &busy_key).admitted_vv, None);
    assert_eq!(
        drain_queue(&db, Queue::Up),
        vec![calm_key.as_str().to_string()]
    );
}

#[test]
fn admission_bumps_past_the_published_history() {
    let (_d, _r, db) = scratch(DEV_A);
    let image = rel(IMG);
    let key = sidecar_item_relkey(&image).expect("item key");
    let d1 = eh::doc(3, None, 0.2);
    let mut seeded = rec(Kind::Sidecar, ItemState::Synced);
    seeded.sem_hash = Some(sem_hash(&d1).expect("sem"));
    seeded.blake3 = Some(Blake3Hex::from_bytes(&d1));
    seeded.vv = vv(&[(DEV_A, 2), (DEV_B, 1)]);
    db.insert_item(&key, &seeded).expect("seed");
    let d2 = eh::doc(4, None, 0.3);
    notify_local_change(&db, &image, Kind::Sidecar, &scan(&d2, 5)).expect("notify");

    admit_pending(&db, |_, _| true).expect("admit");
    let record = item(&db, &key);
    assert_eq!(
        record.vv,
        vv(&[(DEV_A, 2), (DEV_B, 1)]),
        "published vv kept"
    );
    assert_eq!(
        record.admitted_vv,
        Some(vv(&[(DEV_A, 3), (DEV_B, 1)])),
        "the mint bumps exactly vv[self] over the known history"
    );
}

// ===========================================================================
// §2.4 seam (Garage): the admitted (vv, ts) + badges travel in the entry
// ===========================================================================

#[tokio::test]
async fn upload_journals_the_admitted_version_and_badges() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("eng-admit-seam");
    let s3 = CountingS3::new(g.client());
    let root = tempfile::tempdir().expect("root");
    let (_dbdir, _p, db) = open_db(&dev(DEV_A));
    let cfg = th::test_cfg(&bucket, root.path());

    let image = rel(IMG);
    let key = sidecar_item_relkey(&image).expect("item key");
    let d1 = eh::doc(5, Some("red"), 0.75);
    let path = item_local_path(root.path(), &key);
    th::write_file(&path, &d1);
    notify_local_change(
        &db,
        &image,
        Kind::Sidecar,
        &scan(&d1, th::mtime_unix_ns(&path)),
    )
    .expect("notify");
    admit_pending(&db, |_, _| true).expect("admit");
    let admitted = item(&db, &key);
    let admitted_vv = admitted.admitted_vv.clone().expect("intent snapshot");
    let admitted_ts = admitted.head_ts.expect("frozen ts");

    let outcome = upload_item(&db, &s3, &cfg, &key, &path)
        .await
        .expect("upload");

    // The staged entry carries exactly the admitted (vv, ts) and the
    // §2.2 badge fields — and, by construction, a blake3.
    let staged: Vec<JournalEntry> = db
        .iter_outbound()
        .expect("outbound")
        .into_iter()
        .map(|(_, bytes)| {
            JournalEntry::from_json_line(std::str::from_utf8(&bytes).expect("utf8"))
                .expect("staged entry decodes")
        })
        .collect();
    assert_eq!(staged.len(), 1);
    let entry = &staged[0];
    assert_eq!(entry.op, Op::Put);
    assert_eq!(entry.kind, Kind::Sidecar);
    assert_eq!(entry.key, sidecar_key(&image), "canonical wire key");
    assert_eq!(entry.vv, admitted_vv, "entry vv == admission snapshot");
    assert_eq!(entry.ts, admitted_ts, "entry ts == admission-frozen ts");
    assert_eq!(entry.rating, Some(5));
    assert_eq!(entry.color_label, Some("red".to_string()));
    assert_eq!(entry.blake3, Some(outcome.blake3.clone()));
    assert_eq!(entry.sem_hash, Some(sem_hash(&d1).expect("sem")));

    // The record promoted the snapshot: vv is now the admitted version,
    // the intent is cleared, and the head facts describe it.
    let record = item(&db, &key);
    assert_eq!(record.state, ItemState::Synced);
    assert_eq!(record.vv, admitted_vv);
    assert_eq!(record.admitted_vv, None);
    assert_eq!(record.head_ts, Some(admitted_ts));
    assert_eq!(record.device, Some(dev(DEV_A)));
    assert_eq!(record.rating, Some(5));
    assert_eq!(record.color_label, Some("red".to_string()));

    // The object landed at the canonical sidecar key.
    assert_eq!(
        th::get_bytes(&g.client(), &bucket, &sidecar_key(&image)).await,
        d1
    );
}

#[tokio::test]
async fn conflict_merge_before_upload_does_not_pollute_the_admitted_entry() {
    // The S4 seam half: an upload whose record vv was max-merged by a
    // §2.6 case-4 resolution mid-flight must still journal exactly the
    // ADMITTED snapshot — advertising components the entry's version
    // does not descend from would silently fast-forward peers.
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("eng-admit-merge");
    let s3 = CountingS3::new(g.client());
    let root = tempfile::tempdir().expect("root");
    let (_dbdir, _p, db) = open_db(&dev(DEV_B));
    let cfg = th::test_cfg(&bucket, root.path());

    let image = rel(IMG);
    let key = sidecar_item_relkey(&image).expect("item key");
    let d1 = eh::doc(2, None, 0.5);
    let path = item_local_path(root.path(), &key);
    th::write_file(&path, &d1);
    notify_local_change(
        &db,
        &image,
        Kind::Sidecar,
        &scan(&d1, th::mtime_unix_ns(&path)),
    )
    .expect("notify");
    admit_pending(&db, |_, _| true).expect("admit");
    let admitted_vv = item(&db, &key).admitted_vv.clone().expect("snapshot");
    assert_eq!(admitted_vv, vv(&[(DEV_B, 1)]));

    // A conflict resolution merges a foreign component into the record's
    // vv while the upload is still queued (state-preserving mutation).
    db.update_item(&key, ItemState::Queued, |r| {
        r.vv.merge(&vv(&[(DEV_A, 2)]));
    })
    .expect("merge vv");

    upload_item(&db, &s3, &cfg, &key, &path)
        .await
        .expect("upload");
    let staged = db.iter_outbound().expect("outbound");
    let entry = JournalEntry::from_json_line(
        std::str::from_utf8(&staged.last().expect("one staged").1).expect("utf8"),
    )
    .expect("decodes");
    assert_eq!(
        entry.vv, admitted_vv,
        "the entry must carry the admitted snapshot, not the merged record vv"
    );
    // The record keeps the merge: elementwise max of both.
    let record = item(&db, &key);
    assert_eq!(record.vv, vv(&[(DEV_A, 2), (DEV_B, 1)]));
    assert_eq!(record.admitted_vv, None);
}

// ===========================================================================
// §2.6 unified apply rule: case 1 (converged)
// ===========================================================================

/// Seeds a synced sidecar head for `doc` authored by `author` at `v`,
/// with its local file written under `root`.
fn seed_synced_sidecar(
    db: &SyncDb,
    root: &Path,
    image: &RelKey,
    doc: &[u8],
    author: &str,
    v: VersionVector,
    ts: i64,
) -> RelKey {
    let key = sidecar_item_relkey(image).expect("item key");
    let mut record = rec(Kind::Sidecar, ItemState::Synced);
    record.sem_hash = Some(sem_hash(doc).expect("sem"));
    record.blake3 = Some(Blake3Hex::from_bytes(doc));
    record.size = doc.len() as u64;
    record.vv = v;
    record.device = Some(dev(author));
    record.head_ts = Some(ts);
    let badges = sidecar_badges(doc).expect("badges");
    record.rating = badges.rating;
    record.color_label = badges.color_label;
    db.insert_item(&key, &record).expect("seed");
    th::write_file(&item_local_path(root, &key), doc);
    key
}

#[test]
fn case1_equal_vv_adopts_metadata_without_download() {
    let (_d, root, db) = scratch(DEV_A);
    let image = rel(IMG);
    let d1 = eh::doc(3, Some("red"), 0.25);
    let key = seed_synced_sidecar(&db, root.path(), &image, &d1, DEV_B, vv(&[(DEV_B, 1)]), 100);
    let before = item(&db, &key);

    let mut events = RecordedEvents::default();
    let mut consumer = EngineConsumer::new(&db, root.path(), &mut events)
        .expect("consumer")
        .with_now(500);
    apply(
        &db,
        &mut consumer,
        &put_sidecar(DEV_B, &image, &d1, vv(&[(DEV_B, 1)]), 100),
    );

    assert_eq!(item(&db, &key), before, "equal version: nothing moves");
    assert_eq!(db.queue_len(Queue::Down).expect("len"), 0);
    assert!(events.conflicts.is_empty());
}

#[test]
fn case1_same_semantics_merges_concurrent_vvs_without_download() {
    let (_d, root, db) = scratch(DEV_A);
    let image = rel(IMG);
    let d1 = eh::doc(3, Some("red"), 0.25);
    let key = seed_synced_sidecar(&db, root.path(), &image, &d1, DEV_A, vv(&[(DEV_A, 1)]), 100);

    // Same document, different spelling, concurrent vv: converged.
    let respelled = eh::churned(&d1);
    let mut events = RecordedEvents::default();
    let mut consumer = EngineConsumer::new(&db, root.path(), &mut events)
        .expect("consumer")
        .with_now(500);
    apply(
        &db,
        &mut consumer,
        &put_sidecar(DEV_B, &image, &respelled, vv(&[(DEV_B, 1)]), 200),
    );

    let record = item(&db, &key);
    assert_eq!(record.state, ItemState::Synced, "no transfer needed");
    assert_eq!(record.vv, vv(&[(DEV_A, 1), (DEV_B, 1)]), "vv max-merge");
    assert_eq!(
        record.blake3,
        Some(Blake3Hex::from_bytes(&d1)),
        "local bytes stay authoritative for a spelling-divergent twin"
    );
    // The merged head identity must converge DETERMINISTICALLY — it is
    // the §2.6 candidate for any future case 4, so two devices folding
    // the same twin pair must agree on it: the pick_winner of the two
    // (ts, device) candidates. Here (200, B) beats (100, A).
    assert_eq!(record.head_ts, Some(200));
    assert_eq!(record.device, Some(dev(DEV_B)));
    assert_eq!(db.queue_len(Queue::Down).expect("len"), 0);
    assert!(events.conflicts.is_empty(), "convergence is not a conflict");
}

#[test]
fn case1_converged_dirty_drops_the_dirt_without_upload() {
    // §2.6 case 1 over a dirty item: the local FILE already holds the
    // remote semantics, so the dirt collapses (the landed
    // replay_put_item_cas Dirty → Synced row) — no upload, no version.
    let (_d, root, db) = scratch(DEV_A);
    let image = rel(IMG);
    let key = sidecar_item_relkey(&image).expect("item key");
    let published = eh::doc(1, None, 0.0);
    let edited = eh::doc(4, Some("red"), 0.5);
    let mut record = rec(Kind::Sidecar, ItemState::Dirty);
    record.sem_hash = Some(sem_hash(&published).expect("sem"));
    record.blake3 = Some(Blake3Hex::from_bytes(&published));
    record.vv = vv(&[(DEV_A, 1)]);
    record.rating = Some(4);
    record.color_label = Some("red".to_string());
    db.insert_item(&key, &record).expect("seed");
    th::write_file(&item_local_path(root.path(), &key), &edited);

    // B published the SAME semantics our dirty file holds.
    let mut events = RecordedEvents::default();
    let mut consumer = EngineConsumer::new(&db, root.path(), &mut events)
        .expect("consumer")
        .with_now(900);
    apply(
        &db,
        &mut consumer,
        &put_sidecar(DEV_B, &image, &edited, vv(&[(DEV_A, 1), (DEV_B, 1)]), 300),
    );

    let after = item(&db, &key);
    assert_eq!(after.state, ItemState::Synced, "dirt dropped, no upload");
    assert_eq!(after.vv, vv(&[(DEV_A, 1), (DEV_B, 1)]));
    assert_eq!(after.admitted_vv, None, "no version was minted");
    assert_eq!(db.queue_len(Queue::Up).expect("len"), 0);
    assert_eq!(db.outbound_len().expect("outbound"), 0);
    assert!(events.conflicts.is_empty());
}

// ===========================================================================
// §2.6 case 2 (remote dominates) and case 3 (local dominates)
// ===========================================================================

#[test]
fn case2_dominating_put_adopts_and_queues_download() {
    let (_d, root, db) = scratch(DEV_A);
    let image = rel(IMG);
    let d1 = eh::doc(1, None, 0.0);
    let key = seed_synced_sidecar(&db, root.path(), &image, &d1, DEV_A, vv(&[(DEV_A, 1)]), 100);

    let d2 = eh::doc(5, Some("green"), 1.0);
    let entry = put_sidecar(DEV_B, &image, &d2, vv(&[(DEV_A, 1), (DEV_B, 1)]), 200);
    let mut events = RecordedEvents::default();
    let mut consumer = EngineConsumer::new(&db, root.path(), &mut events)
        .expect("consumer")
        .with_now(500);
    apply(&db, &mut consumer, &entry);

    let record = item(&db, &key);
    assert_eq!(record.state, ItemState::PendingDown);
    assert_eq!(record.vv, entry.vv);
    assert_eq!(record.blake3, entry.blake3);
    assert_eq!(record.sem_hash, entry.sem_hash);
    assert_eq!(record.size, d2.len() as u64);
    assert_eq!(record.rating, Some(5));
    assert_eq!(record.color_label, Some("green".to_string()));
    assert_eq!(record.device, Some(dev(DEV_B)));
    assert_eq!(record.head_ts, Some(200));
    assert_eq!(
        drain_queue(&db, Queue::Down),
        vec![key.as_str().to_string()]
    );
    assert!(events.conflicts.is_empty());
}

#[test]
fn case2_unknown_item_is_created_pending_down() {
    let (_d, root, db) = scratch(DEV_A);
    let image = rel(IMG);
    let d2 = eh::doc(2, Some("blue"), 0.1);
    let entry = put_sidecar(DEV_B, &image, &d2, vv(&[(DEV_B, 3)]), 400);
    let mut events = RecordedEvents::default();
    let mut consumer = EngineConsumer::new(&db, root.path(), &mut events)
        .expect("consumer")
        .with_now(500);
    apply(&db, &mut consumer, &entry);

    let key = sidecar_item_relkey(&image).expect("item key");
    let record = item(&db, &key);
    assert_eq!(record.state, ItemState::PendingDown);
    assert_eq!(record.kind, Kind::Sidecar);
    assert_eq!(record.vv, vv(&[(DEV_B, 3)]));
    assert_eq!(record.blake3, entry.blake3);
    assert_eq!(record.rating, Some(2));
    assert_eq!(
        drain_queue(&db, Queue::Down),
        vec![key.as_str().to_string()]
    );
}

#[test]
fn case3_ancestor_put_is_ignored() {
    let (_d, root, db) = scratch(DEV_A);
    let image = rel(IMG);
    let d2 = eh::doc(4, None, 0.5);
    let key = seed_synced_sidecar(
        &db,
        root.path(),
        &image,
        &d2,
        DEV_A,
        vv(&[(DEV_A, 2), (DEV_B, 1)]),
        300,
    );
    let before = item(&db, &key);

    let old = eh::doc(1, None, 0.0);
    let mut events = RecordedEvents::default();
    let mut consumer = EngineConsumer::new(&db, root.path(), &mut events)
        .expect("consumer")
        .with_now(999);
    apply(
        &db,
        &mut consumer,
        &put_sidecar(DEV_B, &image, &old, vv(&[(DEV_B, 1)]), 50),
    );

    assert_eq!(item(&db, &key), before, "ancestors change nothing");
    assert_eq!(db.queue_len(Queue::Down).expect("len"), 0);
    assert!(events.conflicts.is_empty());
}

// ===========================================================================
// §2.6 case 4 (concurrent): deterministic winner + loser materialization
// ===========================================================================

#[test]
fn case4_remote_wins_materializes_the_held_loser_and_adopts_the_winner() {
    let (_d, root, db) = scratch(DEV_A);
    let image = rel(IMG);
    let ours = eh::doc(3, Some("red"), 0.25);
    let key = seed_synced_sidecar(
        &db,
        root.path(),
        &image,
        &ours,
        DEV_A,
        vv(&[(DEV_A, 2)]),
        1_000,
    );

    let theirs = eh::doc(5, Some("green"), 0.75);
    let entry = put_sidecar(DEV_B, &image, &theirs, vv(&[(DEV_A, 1), (DEV_B, 1)]), 2_000);
    assert_eq!(
        compare(&entry.vv, &vv(&[(DEV_A, 2)])),
        VvOrder::Concurrent,
        "precondition"
    );
    let mut events = RecordedEvents::default();
    let mut consumer = EngineConsumer::new(&db, root.path(), &mut events)
        .expect("consumer")
        .with_now(2_500);
    apply(&db, &mut consumer, &entry);

    // Winner (higher ts: remote) becomes the primary.
    let record = item(&db, &key);
    assert_eq!(record.state, ItemState::PendingDown, "fetch the winner");
    assert_eq!(
        record.vv,
        vv(&[(DEV_A, 2), (DEV_B, 1)]),
        "path vv <- elementwise max of both branches"
    );
    assert_eq!(record.sem_hash, Some(sem_hash(&theirs).expect("sem")));
    assert_eq!(record.blake3, Some(Blake3Hex::from_bytes(&theirs)));
    assert_eq!(record.device, Some(dev(DEV_B)));
    assert_eq!(record.head_ts, Some(2_000));

    // We hold the loser (we authored it): it materialized at the
    // deterministic vc key as the CANONICAL loser document (the §2.5
    // semantic form — byte-identical on every holder, churn included;
    // review round 0), admitted for upload with a fresh
    // single-component vv.
    let suffix = loser_vc_suffix(&ours).expect("suffix");
    let vc_key = vc_item_relkey(&image, &suffix).expect("vc relkey");
    let vc = item(&db, &vc_key);
    assert_eq!(vc.kind, Kind::Sidecar);
    assert_eq!(vc.state, ItemState::Queued);
    assert!(vc.vv.is_empty(), "nothing published yet for the new key");
    assert_eq!(
        vc.admitted_vv,
        Some(vv(&[(DEV_A, 1)])),
        "fresh single-component vv"
    );
    assert_eq!(
        vc.admitted_ts,
        Some(2_500),
        "the vc admission freezes the pass's now as its entry ts"
    );
    assert_eq!(vc.sem_hash, Some(sem_hash(&ours).expect("sem")));
    let vc_bytes = eh::read_file(&item_local_path(root.path(), &vc_key));
    assert_eq!(vc_bytes, eh::semantic(&ours), "canonical loser doc");
    assert_eq!(
        sem_hash(&vc_bytes).expect("vc sem"),
        sem_hash(&ours).expect("loser sem"),
        "the canonical form preserves the loser's semantic identity"
    );

    assert_eq!(
        drain_queue(&db, Queue::Down),
        vec![key.as_str().to_string()]
    );
    assert_eq!(
        drain_queue(&db, Queue::Up),
        vec![vc_key.as_str().to_string()]
    );

    assert_eq!(
        events.conflicts,
        vec![rrcloud_core::engine::ConflictEvent {
            relkey: image.clone(),
            winner_device: dev(DEV_B),
            copy_relkey: Some(vc_key),
        }]
    );
}

#[test]
fn case4_local_wins_merges_without_materialization() {
    let (_d, root, db) = scratch(DEV_A);
    let image = rel(IMG);
    let ours = eh::doc(3, Some("red"), 0.25);
    let key = seed_synced_sidecar(
        &db,
        root.path(),
        &image,
        &ours,
        DEV_A,
        vv(&[(DEV_A, 2)]),
        5_000,
    );
    let before = item(&db, &key);

    let theirs = eh::doc(5, Some("green"), 0.75);
    let entry = put_sidecar(DEV_B, &image, &theirs, vv(&[(DEV_A, 1), (DEV_B, 1)]), 2_000);
    let mut events = RecordedEvents::default();
    let mut consumer = EngineConsumer::new(&db, root.path(), &mut events)
        .expect("consumer")
        .with_now(6_000);
    apply(&db, &mut consumer, &entry);

    let record = item(&db, &key);
    // Review round 1: the winning AUTHOR is re-marked Dirty so the next
    // admission re-publishes the winner over the §1.2 shared bucket key
    // (the loser's upload may have landed after ours; see
    // remote_loser_resolution_renudges_only_the_quiescent_winning_author).
    // Our version stays the primary either way.
    assert_eq!(record.state, ItemState::Dirty, "re-publish nudge (round 1)");
    assert_eq!(
        record.vv,
        vv(&[(DEV_A, 2), (DEV_B, 1)]),
        "vv still max-merges"
    );
    assert_eq!(record.sem_hash, before.sem_hash);
    assert_eq!(record.blake3, before.blake3);
    assert_eq!(record.device, before.device);
    assert_eq!(record.head_ts, before.head_ts);
    assert_eq!(
        db.queue_len(Queue::Down).expect("len"),
        0,
        "nothing to fetch"
    );
    // We do not hold the loser (B's doc): no vc item, no vc file.
    assert_eq!(db.iter_items().expect("items").len(), 1);
    assert_eq!(
        events.conflicts,
        vec![rrcloud_core::engine::ConflictEvent {
            relkey: image,
            winner_device: dev(DEV_A),
            copy_relkey: None,
        }]
    );
}

#[test]
fn case4_equal_ts_breaks_ties_by_greater_device_id() {
    // DEV_A ("d1f0…") > DEV_B ("a3b2…") lexicographically.
    assert!(DEV_A > DEV_B, "precondition");
    let image = rel(IMG);
    let doc_a = eh::doc(1, None, 0.1);
    let doc_b = eh::doc(2, None, 0.2);

    // On a third device whose local head is B's version: A's entry at
    // the SAME ts must win the tie.
    let (_d, root, db) = scratch(DEV_C);
    let key = seed_synced_sidecar(
        &db,
        root.path(),
        &image,
        &doc_b,
        DEV_B,
        vv(&[(DEV_B, 1)]),
        7_000,
    );
    let entry = put_sidecar(DEV_A, &image, &doc_a, vv(&[(DEV_A, 1)]), 7_000);
    let mut events = RecordedEvents::default();
    let mut consumer = EngineConsumer::new(&db, root.path(), &mut events)
        .expect("consumer")
        .with_now(8_000);
    apply(&db, &mut consumer, &entry);
    let record = item(&db, &key);
    assert_eq!(
        record.sem_hash,
        Some(sem_hash(&doc_a).expect("sem")),
        "A wins the tie"
    );
    assert_eq!(record.state, ItemState::PendingDown);
    assert_eq!(events.conflicts[0].winner_device, dev(DEV_A));

    // Mirror: local head is A's version, B's entry at the same ts loses.
    let (_d2, root2, db2) = scratch(DEV_C);
    let key2 = seed_synced_sidecar(
        &db2,
        root2.path(),
        &image,
        &doc_a,
        DEV_A,
        vv(&[(DEV_A, 1)]),
        7_000,
    );
    let entry_b = put_sidecar(DEV_B, &image, &doc_b, vv(&[(DEV_B, 1)]), 7_000);
    let mut events2 = RecordedEvents::default();
    let mut consumer2 = EngineConsumer::new(&db2, root2.path(), &mut events2)
        .expect("consumer")
        .with_now(8_000);
    apply(&db2, &mut consumer2, &entry_b);
    let record2 = item(&db2, &key2);
    assert_eq!(
        record2.sem_hash,
        Some(sem_hash(&doc_a).expect("sem")),
        "A still primary"
    );
    assert_eq!(record2.state, ItemState::Synced);
    assert_eq!(record2.vv, vv(&[(DEV_A, 1), (DEV_B, 1)]));
    assert_eq!(events2.conflicts[0].winner_device, dev(DEV_A));
}

#[test]
fn case4_both_arrival_orders_reach_the_identical_state() {
    // The §2.11 B1 regression at unit scale: a third device applying
    // the same concurrent pair in both orders ends bit-identical.
    let image = rel(IMG);
    let doc_a = eh::doc(1, None, 0.1);
    let doc_b = eh::doc(2, None, 0.2);
    let entry_a = put_sidecar(DEV_A, &image, &doc_a, vv(&[(DEV_A, 1)]), 1_000);
    let entry_b = put_sidecar(DEV_B, &image, &doc_b, vv(&[(DEV_B, 1)]), 2_000);

    let run = |first: &JournalEntry, second: &JournalEntry| {
        let (_d, root, db) = scratch(DEV_C);
        let mut events = RecordedEvents::default();
        let mut consumer = EngineConsumer::new(&db, root.path(), &mut events)
            .expect("consumer")
            .with_now(9_000);
        apply(&db, &mut consumer, first);
        apply(&db, &mut consumer, second);
        (eh::full_items(&db), events.conflicts)
    };
    let (items_ab, conflicts_ab) = run(&entry_a, &entry_b);
    let (items_ba, conflicts_ba) = run(&entry_b, &entry_a);
    assert_eq!(items_ab, items_ba, "arrival order must not matter");
    // Both orders resolved to the same winner (B: higher ts).
    assert_eq!(
        conflicts_ab.last().expect("conflict").winner_device,
        dev(DEV_B)
    );
    assert_eq!(
        conflicts_ba.last().expect("conflict").winner_device,
        dev(DEV_B)
    );
    let key = sidecar_item_relkey(&image).expect("key");
    assert_eq!(
        items_ab[key.as_str()].sem_hash,
        Some(sem_hash(&doc_b).expect("sem"))
    );
    assert_eq!(items_ab[key.as_str()].vv, vv(&[(DEV_A, 1), (DEV_B, 1)]));
}

#[test]
fn case4_vc_put_dedups_by_key_and_sem_hash() {
    // S7's unit half: when the peer's materialization of the SAME loser
    // arrives, the vc item converges (case 1 by sem_hash) into ONE
    // record whose vv is the union of both authors' fresh components.
    let (_d, root, db) = scratch(DEV_A);
    let image = rel(IMG);
    let loser = eh::doc(3, Some("red"), 0.25);
    let suffix = loser_vc_suffix(&loser).expect("suffix");
    let vc_rel = vc_item_relkey(&image, &suffix).expect("vc relkey");

    // We already materialized and published our copy ({A:1}).
    let mut mine = rec(Kind::Sidecar, ItemState::Synced);
    mine.sem_hash = Some(sem_hash(&loser).expect("sem"));
    mine.blake3 = Some(Blake3Hex::from_bytes(&loser));
    mine.size = loser.len() as u64;
    mine.vv = vv(&[(DEV_A, 1)]);
    mine.device = Some(dev(DEV_A));
    mine.head_ts = Some(2_500);
    db.insert_item(&vc_rel, &mine).expect("seed vc");
    th::write_file(&item_local_path(root.path(), &vc_rel), &loser);

    // C's byte-identical materialization arrives with ITS fresh vv.
    let mut entry = put_sidecar(DEV_C, &image, &loser, vv(&[(DEV_C, 1)]), 3_000);
    entry.key = library_key(&vc_rel);
    let mut events = RecordedEvents::default();
    let mut consumer = EngineConsumer::new(&db, root.path(), &mut events)
        .expect("consumer")
        .with_now(4_000);
    apply(&db, &mut consumer, &entry);

    let vc_records: Vec<(RelKey, ItemRecord)> = db
        .iter_items()
        .expect("items")
        .into_iter()
        .filter(|(k, _)| k.as_str().contains(&suffix))
        .collect();
    assert_eq!(vc_records.len(), 1, "apply dedup leaves ONE vc item record");
    let record = &vc_records[0].1;
    assert_eq!(record.vv, vv(&[(DEV_A, 1), (DEV_C, 1)]), "fresh vvs union");
    assert_eq!(record.state, ItemState::Synced, "no transfer needed");
    // Twin materializations are independently authored: the converged
    // head identity must be the deterministic pick_winner of the two
    // candidates — (3_000, C) beats (2_500, A) — so every device folding
    // this pair agrees on the future-§2.6 candidate.
    assert_eq!(record.head_ts, Some(3_000));
    assert_eq!(record.device, Some(dev(DEV_C)));
    assert_eq!(db.queue_len(Queue::Down).expect("len"), 0);
    assert!(
        events.conflicts.is_empty(),
        "dedup is convergence, not conflict"
    );
}

#[test]
fn case2_with_uncommitted_dirty_commits_local_first_then_case4() {
    // §2.6 case 2's dirty clause, S4's unit half: B holds published
    // {A:1} with uncommitted dirty edits when A's dominating {A:2}
    // arrives. The consumer first commits the local version through the
    // admission path (bump vv[B], freeze head_ts = now), which makes
    // the pair concurrent, then resolves; with the local commit newer,
    // the local version wins, nothing is lost, and the still-queued
    // upload's admitted snapshot is NOT polluted by the merge.
    let (_d, root, db) = scratch(DEV_B);
    let image = rel(IMG);
    let key = sidecar_item_relkey(&image).expect("item key");
    let published = eh::doc(1, None, 0.0);
    let edited = eh::doc(4, Some("red"), 0.5);
    let mut record = rec(Kind::Sidecar, ItemState::Dirty);
    record.sem_hash = Some(sem_hash(&published).expect("sem"));
    record.blake3 = Some(Blake3Hex::from_bytes(&published));
    record.vv = vv(&[(DEV_A, 1)]);
    record.rating = Some(4);
    record.color_label = Some("red".to_string());
    db.insert_item(&key, &record).expect("seed");
    th::write_file(&item_local_path(root.path(), &key), &edited);

    let remote = eh::doc(2, Some("blue"), 0.9);
    let entry = put_sidecar(DEV_A, &image, &remote, vv(&[(DEV_A, 2)]), 1_000);
    let mut events = RecordedEvents::default();
    let mut consumer = EngineConsumer::new(&db, root.path(), &mut events)
        .expect("consumer")
        .with_now(5_000); // local commit is newer: local wins case 4
    apply(&db, &mut consumer, &entry);

    let after = item(&db, &key);
    assert_eq!(
        after.state,
        ItemState::Queued,
        "committed local version still uploads"
    );
    assert_eq!(
        after.admitted_vv,
        Some(vv(&[(DEV_A, 1), (DEV_B, 1)])),
        "the admitted snapshot is the pre-merge local commit"
    );
    assert_eq!(
        after.vv,
        vv(&[(DEV_A, 2)]),
        "the record's published history max-merges the remote branch"
    );
    assert_eq!(
        after.head_ts,
        Some(5_000),
        "admission froze now as the commit ts"
    );
    assert_eq!(after.device, Some(dev(DEV_B)));
    assert_eq!(drain_queue(&db, Queue::Up), vec![key.as_str().to_string()]);
    assert_eq!(
        db.queue_len(Queue::Down).expect("len"),
        0,
        "local won: no fetch"
    );
    assert_eq!(
        events.conflicts,
        vec![rrcloud_core::engine::ConflictEvent {
            relkey: image,
            winner_device: dev(DEV_B),
            copy_relkey: None,
        }]
    );
}

#[test]
fn case4_remote_wins_over_committed_queued_local_redirects_it_to_the_vc() {
    // The committed-but-not-yet-uploaded local version loses: its
    // primary upload is withdrawn (the bytes belong at the vc key now),
    // the winner is adopted at the primary, and the loser lives on as
    // the materialized vc.
    let (_d, root, db) = scratch(DEV_B);
    let image = rel(IMG);
    let key = sidecar_item_relkey(&image).expect("item key");
    let ours = eh::doc(4, Some("red"), 0.5);
    let mut record = rec(Kind::Sidecar, ItemState::Queued);
    record.sem_hash = Some(sem_hash(&ours).expect("sem"));
    record.vv = vv(&[(DEV_A, 1)]);
    record.admitted_vv = Some(vv(&[(DEV_A, 1), (DEV_B, 1)]));
    record.head_ts = Some(1_000);
    record.device = Some(dev(DEV_B));
    record.rating = Some(4);
    db.replay_put_item(&key, &record).expect("seed queued");
    db.queue_push(Queue::Up, &key, 1).expect("queue");
    th::write_file(&item_local_path(root.path(), &key), &ours);

    let theirs = eh::doc(5, Some("green"), 0.75);
    let entry = put_sidecar(DEV_A, &image, &theirs, vv(&[(DEV_A, 2)]), 9_000);
    let mut events = RecordedEvents::default();
    let mut consumer = EngineConsumer::new(&db, root.path(), &mut events)
        .expect("consumer")
        .with_now(2_000);
    apply(&db, &mut consumer, &entry);

    let after = item(&db, &key);
    assert_eq!(after.state, ItemState::PendingDown, "winner fetch pending");
    assert_eq!(
        after.admitted_vv, None,
        "the primary upload intent is withdrawn"
    );
    // The withdrawn intent's vv does NOT fold into the primary path
    // (review round 0): {A:1, B:1} was never published on this key — the
    // losing version lives on at the vc key with its own fresh vv — so
    // folding it would leave this device with a component no peer can
    // ever learn, making every future honest descendant of the
    // converged state read as concurrent here. The record converges to
    // exactly what every other device computes.
    assert_eq!(after.vv, vv(&[(DEV_A, 2)]));
    assert_eq!(after.sem_hash, Some(sem_hash(&theirs).expect("sem")));

    let suffix = loser_vc_suffix(&ours).expect("suffix");
    let vc_rel = vc_item_relkey(&image, &suffix).expect("vc relkey");
    let vc = item(&db, &vc_rel);
    assert_eq!(vc.state, ItemState::Queued);
    assert_eq!(
        vc.admitted_vv,
        Some(vv(&[(DEV_B, 1)])),
        "fresh vv for the new key"
    );
    assert_eq!(
        eh::read_file(&item_local_path(root.path(), &vc_rel)),
        eh::semantic(&ours),
        "canonical loser doc at the vc path"
    );
    assert_eq!(
        drain_queue(&db, Queue::Up),
        vec![vc_rel.as_str().to_string()],
        "only the vc upload remains queued"
    );
    assert_eq!(
        drain_queue(&db, Queue::Down),
        vec![key.as_str().to_string()]
    );
    assert_eq!(events.conflicts[0].winner_device, dev(DEV_A));
    assert_eq!(events.conflicts[0].copy_relkey, Some(vc_rel));
}

// ===========================================================================
// §2.7 del ordering: dominate / ancestor / resurrect
// ===========================================================================

#[test]
fn dominating_del_hides_the_item_and_records_the_deletion() {
    let (_d, root, db) = scratch(DEV_B);
    let image = rel(IMG);
    let d1 = eh::doc(3, None, 0.25);
    let key = seed_synced_sidecar(&db, root.path(), &image, &d1, DEV_A, vv(&[(DEV_A, 1)]), 100);

    let entry = del(
        DEV_A,
        Kind::Sidecar,
        sidecar_key(&image),
        vv(&[(DEV_A, 2)]),
        900,
    );
    let mut events = RecordedEvents::default();
    let mut consumer = EngineConsumer::new(&db, root.path(), &mut events)
        .expect("consumer")
        .with_now(1_000);
    apply(&db, &mut consumer, &entry);

    let record = item(&db, &key);
    assert!(record.deleted, "hidden, not destroyed");
    assert_eq!(record.state, ItemState::Synced, "the record survives whole");
    assert_eq!(
        record.vv,
        vv(&[(DEV_A, 2)]),
        "the del is a version of the key"
    );
    // The hidden record keeps the deleted CONTENT version's head
    // identity (review round 0: it is what restore re-advertises, and
    // what a del-first bootstrapper records — so arrival orders agree).
    assert_eq!(record.head_ts, Some(100));
    assert_eq!(record.device, Some(dev(DEV_A)));
    // §2.3: the deleted-set row is keyed by the deleted ITEM's own file
    // relkey (one row per deleted key — review round 0).
    assert_eq!(db.get_deleted(&image).expect("no image-keyed row"), None);
    let deleted = db
        .get_deleted(&key)
        .expect("get_deleted")
        .expect("recorded under the item key");
    assert_eq!(deleted.vv, vv(&[(DEV_A, 2)]));
    assert_eq!(deleted.server_ts, 900);
    let listed = recently_deleted(&db).expect("recently_deleted");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].0, key);
    assert!(events.conflicts.is_empty());
}

#[test]
fn ancestor_del_is_ignored() {
    let (_d, root, db) = scratch(DEV_B);
    let image = rel(IMG);
    let d2 = eh::doc(4, None, 0.5);
    let key = seed_synced_sidecar(
        &db,
        root.path(),
        &image,
        &d2,
        DEV_B,
        vv(&[(DEV_A, 2), (DEV_B, 1)]),
        500,
    );
    let before = item(&db, &key);

    let entry = del(
        DEV_A,
        Kind::Sidecar,
        sidecar_key(&image),
        vv(&[(DEV_A, 2)]),
        400,
    );
    let mut events = RecordedEvents::default();
    let mut consumer = EngineConsumer::new(&db, root.path(), &mut events)
        .expect("consumer")
        .with_now(600);
    apply(&db, &mut consumer, &entry);

    assert_eq!(item(&db, &key), before, "a superseded del changes nothing");
    assert_eq!(db.get_deleted(&image).expect("get_deleted"), None);
}

#[test]
fn concurrent_del_with_dirty_resurrects_sidecar_and_original() {
    // §2.7 edits-beat-deletes, S2's unit half: a del concurrent with
    // local dirty resurrects BOTH keys with dominating vvs; the
    // original (evicted to a stub, blake3 known) is re-advertised
    // metadata-only through an always-blake3 EnginePut.
    let (_d, root, db) = scratch(DEV_B);
    let image = rel(IMG);
    let key = sidecar_item_relkey(&image).expect("sidecar item");
    let published = eh::doc(1, None, 0.0);
    let edited = eh::doc(5, Some("red"), 0.9);
    let mut sidecar = rec(Kind::Sidecar, ItemState::Dirty);
    sidecar.sem_hash = Some(sem_hash(&published).expect("sem"));
    sidecar.blake3 = Some(Blake3Hex::from_bytes(&published));
    sidecar.vv = vv(&[(DEV_A, 1)]);
    db.insert_item(&key, &sidecar).expect("seed sidecar");
    th::write_file(&item_local_path(root.path(), &key), &edited);

    let orig_bytes = th::patterned(2048, 7);
    let mut original = rec(Kind::Original, ItemState::Stub);
    original.blake3 = Some(Blake3Hex::from_bytes(&orig_bytes));
    original.content_id = Some(ContentId::from_bytes(&orig_bytes));
    original.size = orig_bytes.len() as u64;
    original.vv = vv(&[(DEV_A, 1)]);
    db.insert_item(&image, &original).expect("seed original");

    // A's tombstone dels: both bumped to {A:2}.
    let del_sidecar = del(
        DEV_A,
        Kind::Sidecar,
        sidecar_key(&image),
        vv(&[(DEV_A, 2)]),
        700,
    );
    let del_original = del(
        DEV_A,
        Kind::Original,
        library_key(&image),
        vv(&[(DEV_A, 2)]),
        700,
    );
    let mut events = RecordedEvents::default();
    let mut consumer = EngineConsumer::new(&db, root.path(), &mut events)
        .expect("consumer")
        .with_now(1_500);
    apply(&db, &mut consumer, &del_sidecar);
    apply(&db, &mut consumer, &del_original);

    // Sidecar: committed as a local version whose admitted vv dominates
    // the del, still bound for upload.
    let sidecar_after = item(&db, &key);
    assert!(!sidecar_after.deleted, "resurrected, never hidden");
    assert_eq!(sidecar_after.state, ItemState::Queued);
    let admitted = sidecar_after.admitted_vv.clone().expect("admitted");
    assert_eq!(
        compare(&admitted, &vv(&[(DEV_A, 2)])),
        VvOrder::Greater,
        "the resurrection put must dominate the del, got {admitted:?}"
    );

    // Original: re-advertised metadata-only, dominating the del, with
    // the known identity — and NOT queued for upload (the bytes are
    // still in the bucket during grace).
    let original_after = item(&db, &image);
    assert!(!original_after.deleted);
    assert_eq!(original_after.state, ItemState::Stub, "still evicted");
    assert_eq!(
        compare(&original_after.vv, &vv(&[(DEV_A, 2)])),
        VvOrder::Greater
    );
    assert_eq!(
        original_after.blake3,
        Some(Blake3Hex::from_bytes(&orig_bytes))
    );
    assert_eq!(
        original_after.content_id,
        Some(ContentId::from_bytes(&orig_bytes))
    );
    let staged: Vec<JournalEntry> = db
        .iter_outbound()
        .expect("outbound")
        .into_iter()
        .map(|(_, bytes)| {
            JournalEntry::from_json_line(std::str::from_utf8(&bytes).expect("utf8"))
                .expect("decodes")
        })
        .collect();
    let original_put = staged
        .iter()
        .find(|e| e.op == Op::Put && e.key == library_key(&image))
        .expect("original resurrection put staged");
    assert_eq!(original_put.vv, original_after.vv);
    assert_eq!(
        original_put.blake3,
        Some(Blake3Hex::from_bytes(&orig_bytes))
    );
    assert_eq!(
        original_put.content_id,
        Some(ContentId::from_bytes(&orig_bytes))
    );
    assert_eq!(
        drain_queue(&db, Queue::Up),
        vec![key.as_str().to_string()],
        "only the sidecar's byte upload is queued"
    );
    assert_eq!(
        db.get_deleted(&image).expect("get_deleted"),
        None,
        "no deletion recorded"
    );
    assert!(
        events.resurrection_incomplete.is_empty(),
        "the original was covered"
    );
}

#[test]
fn resurrection_without_a_known_original_surfaces_the_incomplete_edge() {
    // §2.7's documented edge: the editing device never downloaded (nor
    // recorded) the original — it can re-advertise only the sidecar,
    // and must say so.
    let (_d, root, db) = scratch(DEV_B);
    let image = rel(IMG);
    let key = sidecar_item_relkey(&image).expect("sidecar item");
    let published = eh::doc(1, None, 0.0);
    let edited = eh::doc(5, Some("red"), 0.9);
    let mut sidecar = rec(Kind::Sidecar, ItemState::Dirty);
    sidecar.sem_hash = Some(sem_hash(&published).expect("sem"));
    sidecar.vv = vv(&[(DEV_A, 1)]);
    db.insert_item(&key, &sidecar).expect("seed sidecar");
    th::write_file(&item_local_path(root.path(), &key), &edited);
    // No original record at all.

    let del_sidecar = del(
        DEV_A,
        Kind::Sidecar,
        sidecar_key(&image),
        vv(&[(DEV_A, 2)]),
        700,
    );
    let mut events = RecordedEvents::default();
    let mut consumer = EngineConsumer::new(&db, root.path(), &mut events)
        .expect("consumer")
        .with_now(1_500);
    apply(&db, &mut consumer, &del_sidecar);

    assert!(!item(&db, &key).deleted);
    assert_eq!(item(&db, &key).state, ItemState::Queued);
    assert_eq!(
        events.resurrection_incomplete,
        vec![rrcloud_core::engine::ResurrectionIncompleteEvent {
            relkey: image.clone()
        }]
    );
    let staged = db.iter_outbound().expect("outbound");
    for (_, bytes) in &staged {
        let entry = JournalEntry::from_json_line(std::str::from_utf8(bytes).expect("utf8"))
            .expect("decodes");
        assert_ne!(
            entry.key,
            library_key(&image),
            "no blind original put may be fabricated"
        );
    }
}

#[test]
fn reconcile_wholeness_converse_lane_surfaces_an_unresurrectable_sidecar() {
    // Round 5 MINOR (observability symmetry): the converse wholeness lane
    // (live original + tombstoned base sidecar, the §2.11 "resurrection
    // restores a whole item" case) must mirror the forward lane — when it
    // CANNOT re-advertise the tombstoned sidecar's develop edits (the
    // deleted record carries no `blake3`), it mints nothing and must say
    // so, exactly as the forward lane fires the event for a blake3-less
    // deleted original.
    let (_d, _root, db) = scratch(DEV_A);
    let image = rel(IMG);
    let sidecar_rel = sidecar_item_relkey(&image).expect("sidecar item");

    // A live original.
    let mut original = rec(Kind::Original, ItemState::Synced);
    original.blake3 = Some(Blake3Hex::from_bytes(b"raw-bytes"));
    original.vv = vv(&[(DEV_A, 1)]);
    db.insert_item(&image, &original).expect("seed original");

    // A tombstoned base sidecar this device never held the content of
    // (`blake3 == None`): it learned the edits were deleted but cannot
    // re-advertise them — the converse of the forward lane's
    // blake3-less-deleted-original case.
    let mut sidecar = rec(Kind::Sidecar, ItemState::Synced);
    sidecar.deleted = true;
    sidecar.vv = vv(&[(DEV_B, 2)]);
    db.insert_item(&sidecar_rel, &sidecar)
        .expect("seed sidecar");

    let mut events = RecordedEvents::default();
    let minted = reconcile_wholeness(&db, &mut events).expect("reconcile");

    assert!(
        minted.is_empty(),
        "no blake3: nothing can be minted on this device"
    );
    assert_eq!(
        events.resurrection_incomplete,
        vec![ResurrectionIncompleteEvent {
            relkey: sidecar_rel.clone()
        }],
        "the converse lane surfaces the un-resurrectable sidecar, mirroring the forward lane"
    );
    assert!(
        item(&db, &sidecar_rel).deleted,
        "the sidecar stays tombstoned until a holder re-advertises it"
    );
    // A live original with NO sidecar record is a normal shape, not a
    // forbidden half-deleted one: it must NOT fire the event.
    let (_d2, _root2, db2) = scratch(DEV_A);
    let lone = rel("p/lone.NEF");
    let mut lone_orig = rec(Kind::Original, ItemState::Synced);
    lone_orig.blake3 = Some(Blake3Hex::from_bytes(b"lone"));
    lone_orig.vv = vv(&[(DEV_A, 1)]);
    db2.insert_item(&lone, &lone_orig)
        .expect("seed lone original");
    let mut events2 = RecordedEvents::default();
    assert!(reconcile_wholeness(&db2, &mut events2)
        .expect("reconcile")
        .is_empty());
    assert!(
        events2.resurrection_incomplete.is_empty(),
        "a live original with no sidecar is not a forbidden shape"
    );
}

#[test]
fn dominating_put_over_a_deleted_record_undeletes_and_clears_the_row() {
    // The restore/resurrection receiver side (§2.7): a put that
    // dominates the known deletion un-hides the item and withdraws this
    // device's deleted-set row (the deletion was superseded).
    let (_d, root, db) = scratch(DEV_A);
    let image = rel(IMG);
    let d1 = eh::doc(3, None, 0.25);
    let key = seed_synced_sidecar(&db, root.path(), &image, &d1, DEV_A, vv(&[(DEV_A, 2)]), 100);
    db.update_item(&key, ItemState::Synced, |r| r.deleted = true)
        .expect("mark deleted");
    db.record_deleted(
        &key,
        &rrcloud_core::state::DeletedRecord {
            vv: vv(&[(DEV_A, 2)]),
            server_ts: 900,
        },
    )
    .expect("record deleted");

    let d2 = eh::doc(4, Some("red"), 0.5);
    let entry = put_sidecar(DEV_B, &image, &d2, vv(&[(DEV_A, 2), (DEV_B, 1)]), 2_000);
    let mut events = RecordedEvents::default();
    let mut consumer = EngineConsumer::new(&db, root.path(), &mut events)
        .expect("consumer")
        .with_now(2_500);
    apply(&db, &mut consumer, &entry);

    let record = item(&db, &key);
    assert!(!record.deleted, "undeleted by the dominating put");
    assert_eq!(record.state, ItemState::PendingDown, "new content fetches");
    assert_eq!(
        db.get_deleted(&key).expect("get_deleted"),
        None,
        "row withdrawn"
    );
    assert!(recently_deleted(&db).expect("listing").is_empty());
}

// ===========================================================================
// Record-only kinds (metadata placeholders at this unit)
// ===========================================================================

#[test]
fn placeholder_kinds_apply_without_state_or_queues() {
    let (_d, root, db) = scratch(DEV_A);
    let mut events = RecordedEvents::default();
    let mut consumer = EngineConsumer::new(&db, root.path(), &mut events)
        .expect("consumer")
        .with_now(100);
    let cid = ContentId::from_bytes(b"content");
    for (kind, key) in [
        (Kind::Preview, rrcloud_core::keys::preview_key(&cid)),
        (
            Kind::Thumb,
            rrcloud_core::keys::thumb_key(&cid, rrcloud_core::keys::ThumbSize::Small),
        ),
        (
            Kind::Albums,
            rrcloud_core::keys::ALBUMS_META_KEY.to_string(),
        ),
        (
            Kind::Presets,
            rrcloud_core::keys::PRESETS_META_KEY.to_string(),
        ),
    ] {
        let mut entry = put_sidecar(
            DEV_B,
            &rel(IMG),
            &eh::doc(1, None, 0.0),
            vv(&[(DEV_B, 1)]),
            10,
        );
        entry.kind = kind;
        entry.key = key;
        entry.sem_hash = None;
        entry.rating = None;
        entry.color_label = None;
        apply(&db, &mut consumer, &entry);
    }
    assert_eq!(db.queue_len(Queue::Up).expect("len"), 0);
    assert_eq!(db.queue_len(Queue::Down).expect("len"), 0);
    assert_eq!(db.outbound_len().expect("outbound"), 0);
    assert!(
        db.iter_items().expect("items").is_empty(),
        "no item records at this unit"
    );
}

#[test]
fn xmp_puts_record_metadata_without_transfers_or_conflicts() {
    let (_d, root, db) = scratch(DEV_A);
    let xmp = rel("p/img.xmp");
    let bytes = b"<x:xmpmeta/>";
    let mut entry = put_original(DEV_B, &xmp, bytes, vv(&[(DEV_B, 1)]), 100);
    entry.kind = Kind::Xmp;
    entry.content_id = None;
    let mut events = RecordedEvents::default();
    let mut consumer = EngineConsumer::new(&db, root.path(), &mut events)
        .expect("consumer")
        .with_now(200);
    apply(&db, &mut consumer, &entry);

    let record = item(&db, &xmp);
    assert_eq!(record.kind, Kind::Xmp);
    assert_eq!(record.vv, vv(&[(DEV_B, 1)]));
    assert_eq!(record.blake3, Some(Blake3Hex::from_bytes(bytes)));
    assert_eq!(
        db.queue_len(Queue::Down).expect("len"),
        0,
        "download policy is P2"
    );

    // A concurrent xmp put is not a conflict domain: latest-by-§2.6
    // ordering applies, no vc materializes, no event fires.
    let other = b"<x:xmpmeta>2</x:xmpmeta>";
    let mut concurrent = put_original(DEV_C, &xmp, other, vv(&[(DEV_C, 1)]), 300);
    concurrent.kind = Kind::Xmp;
    concurrent.content_id = None;
    apply(&db, &mut consumer, &concurrent);
    assert!(
        events.conflicts.is_empty(),
        "xmp has no conflict domain of its own"
    );
    assert_eq!(
        db.iter_items().expect("items").len(),
        1,
        "no vc items for xmp"
    );
    let merged = item(&db, &xmp);
    assert_eq!(
        merged.vv,
        vv(&[(DEV_B, 1), (DEV_C, 1)]),
        "vv still max-merges"
    );
}

#[test]
fn foreign_and_unclassifiable_keys_are_skipped_not_errors() {
    let (_d, root, db) = scratch(DEV_A);
    let mut events = RecordedEvents::default();
    let mut consumer = EngineConsumer::new(&db, root.path(), &mut events)
        .expect("consumer")
        .with_now(100);
    for key in [
        "library/../escape.NEF".to_string(),
        "library/cafe\u{301}.NEF".to_string(),
        "garbage".to_string(),
    ] {
        let mut entry = put_sidecar(
            DEV_B,
            &rel(IMG),
            &eh::doc(1, None, 0.0),
            vv(&[(DEV_B, 1)]),
            10,
        );
        entry.key = key;
        // The JournalConsumer error contract: content-level rejection is
        // a skip, never an Err.
        apply(&db, &mut consumer, &entry);
    }
    assert!(db.iter_items().expect("items").is_empty());
}

// ===========================================================================
// §2.8 original overwrite apply
// ===========================================================================

#[test]
fn original_overwrite_remote_wins_stages_the_displaced_copy() {
    let (_d, root, db) = scratch(DEV_A);
    let image = rel(IMG);
    let ours = th::patterned(4096, 3);
    let mut record = rec(Kind::Original, ItemState::Hydrated);
    record.blake3 = Some(Blake3Hex::from_bytes(&ours));
    record.content_id = Some(ContentId::from_bytes(&ours));
    record.size = ours.len() as u64;
    record.vv = vv(&[(DEV_A, 2)]);
    record.device = Some(dev(DEV_A));
    record.head_ts = Some(1_000);
    db.insert_item(&image, &record).expect("seed");
    th::write_file(&item_local_path(root.path(), &image), &ours);

    let theirs = th::patterned(4096, 9);
    let entry = put_original(DEV_B, &image, &theirs, vv(&[(DEV_A, 1), (DEV_B, 1)]), 9_000);
    let mut events = RecordedEvents::default();
    let mut consumer = EngineConsumer::new(&db, root.path(), &mut events)
        .expect("consumer")
        .with_now(2_000);
    apply(&db, &mut consumer, &entry);

    // Primary adopts the winner lazily (download queue).
    let primary = item(&db, &image);
    assert_eq!(primary.state, ItemState::PendingDown);
    assert_eq!(primary.vv, vv(&[(DEV_A, 2), (DEV_B, 1)]));
    assert_eq!(primary.content_id, Some(ContentId::from_bytes(&theirs)));
    assert_eq!(primary.blake3, Some(Blake3Hex::from_bytes(&theirs)));

    // Our displaced bytes are staged at the deterministic conflict key.
    let displaced_cid = ContentId::from_bytes(&ours);
    let conflict_rel = original_conflict_relkey(&image, &displaced_cid).expect("conflict relkey");
    let copy = item(&db, &conflict_rel);
    assert_eq!(copy.kind, Kind::Original);
    assert_eq!(copy.state, ItemState::Queued);
    assert_eq!(
        copy.admitted_vv,
        Some(vv(&[(DEV_A, 1)])),
        "fresh single-component vv"
    );
    assert_eq!(
        eh::read_file(&item_local_path(root.path(), &conflict_rel)),
        ours,
        "the displaced bytes back the staged upload"
    );
    assert_eq!(
        drain_queue(&db, Queue::Up),
        vec![conflict_rel.as_str().to_string()]
    );
    assert_eq!(
        drain_queue(&db, Queue::Down),
        vec![image.as_str().to_string()]
    );
    assert_eq!(
        events.original_conflicts,
        vec![rrcloud_core::engine::OriginalConflictEvent {
            relkey: image,
            conflict_relkey: conflict_rel,
            displaced_content_id: displaced_cid,
        }]
    );
}

#[test]
fn original_overwrite_local_wins_or_stub_stages_nothing() {
    // Local wins: no copy (the displaced bytes are the REMOTE's; their
    // holder stages them), vv merges.
    let (_d, root, db) = scratch(DEV_A);
    let image = rel(IMG);
    let ours = th::patterned(1024, 4);
    let mut record = rec(Kind::Original, ItemState::Hydrated);
    record.blake3 = Some(Blake3Hex::from_bytes(&ours));
    record.content_id = Some(ContentId::from_bytes(&ours));
    record.size = ours.len() as u64;
    record.vv = vv(&[(DEV_A, 2)]);
    record.device = Some(dev(DEV_A));
    record.head_ts = Some(9_000);
    db.insert_item(&image, &record).expect("seed");
    th::write_file(&item_local_path(root.path(), &image), &ours);
    let theirs = th::patterned(1024, 5);
    let entry = put_original(DEV_B, &image, &theirs, vv(&[(DEV_A, 1), (DEV_B, 1)]), 2_000);
    let mut events = RecordedEvents::default();
    let mut consumer = EngineConsumer::new(&db, root.path(), &mut events)
        .expect("consumer")
        .with_now(9_500);
    apply(&db, &mut consumer, &entry);
    let after = item(&db, &image);
    assert_eq!(after.state, ItemState::Hydrated, "our bytes stay primary");
    assert_eq!(after.content_id, Some(ContentId::from_bytes(&ours)));
    assert_eq!(after.vv, vv(&[(DEV_A, 2), (DEV_B, 1)]));
    assert_eq!(db.iter_items().expect("items").len(), 1, "no conflict copy");
    assert_eq!(db.queue_len(Queue::Up).expect("len"), 0);

    // A stub holder of the losing content holds no displaced bytes:
    // metadata-only adoption, no copy, no download queue (hydration is
    // on-demand, P2).
    let (_d2, root2, db2) = scratch(DEV_C);
    let mut stub = rec(Kind::Original, ItemState::Stub);
    stub.blake3 = Some(Blake3Hex::from_bytes(&ours));
    stub.content_id = Some(ContentId::from_bytes(&ours));
    stub.vv = vv(&[(DEV_A, 2)]);
    stub.device = Some(dev(DEV_A));
    stub.head_ts = Some(1_000);
    db2.insert_item(&image, &stub).expect("seed stub");
    let mut events2 = RecordedEvents::default();
    let mut consumer2 = EngineConsumer::new(&db2, root2.path(), &mut events2)
        .expect("consumer")
        .with_now(1_500);
    apply(&db2, &mut consumer2, &entry);
    let after2 = item(&db2, &image);
    assert_eq!(
        after2.state,
        ItemState::Stub,
        "still evicted; hydration is on demand"
    );
    assert_eq!(
        after2.content_id,
        Some(ContentId::from_bytes(&theirs)),
        "winner adopted"
    );
    assert_eq!(after2.vv, vv(&[(DEV_A, 2), (DEV_B, 1)]));
    assert_eq!(db2.iter_items().expect("items").len(), 1);
    assert_eq!(db2.queue_len(Queue::Up).expect("len"), 0);
    assert_eq!(db2.queue_len(Queue::Down).expect("len"), 0);
    assert!(
        events2.original_conflicts.is_empty(),
        "nothing displaced here"
    );
}

// ===========================================================================
// §2.7 delete_item / restore_item (Garage: the tombstone is a real PUT)
// ===========================================================================

#[tokio::test]
async fn delete_puts_tombstone_stages_dels_and_hides_records() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("eng-delete");
    let client = g.client();
    let (_dbdir, _p, db) = open_db(&dev(DEV_A));
    let image = rel(IMG);
    let sidecar_rel = sidecar_item_relkey(&image).expect("sidecar item");

    let d1 = eh::doc(3, Some("red"), 0.25);
    let orig = th::patterned(1024, 1);
    let mut sidecar = rec(Kind::Sidecar, ItemState::Synced);
    sidecar.sem_hash = Some(sem_hash(&d1).expect("sem"));
    sidecar.blake3 = Some(Blake3Hex::from_bytes(&d1));
    sidecar.size = d1.len() as u64;
    sidecar.vv = vv(&[(DEV_A, 2), (DEV_B, 1)]);
    db.insert_item(&sidecar_rel, &sidecar)
        .expect("seed sidecar");
    let mut original = rec(Kind::Original, ItemState::Hydrated);
    original.blake3 = Some(Blake3Hex::from_bytes(&orig));
    original.content_id = Some(ContentId::from_bytes(&orig));
    original.size = orig.len() as u64;
    original.vv = vv(&[(DEV_A, 1)]);
    db.insert_item(&image, &original).expect("seed original");

    // Pre-existing data objects must survive the delete untouched.
    client
        .put_object(
            &bucket,
            &sidecar_key(&image),
            bytes::Bytes::from(d1.clone()),
            &rrcloud_core::s3::PutObjectOptions::default(),
        )
        .await
        .expect("seed sidecar object");
    client
        .put_object(
            &bucket,
            &library_key(&image),
            bytes::Bytes::from(orig.clone()),
            &rrcloud_core::s3::PutObjectOptions::default(),
        )
        .await
        .expect("seed original object");

    let outcome = delete_item(
        &db,
        &client,
        &bucket,
        &image,
        &[Kind::Sidecar, Kind::Original],
    )
    .await
    .expect("delete_item");

    // The tombstone landed at its key and decodes strict-wire.
    let tomb_bytes = th::get_bytes(&client, &bucket, &tombstone_key(&image)).await;
    let tombstone: Tombstone = serde_json::from_slice(&tomb_bytes).expect("tombstone decodes");
    assert_eq!(tombstone, outcome.tombstone);
    assert_eq!(tombstone.relkey, image);
    assert_eq!(tombstone.device, dev(DEV_A));
    assert_eq!(tombstone.kinds, vec![Kind::Sidecar, Kind::Original]);
    assert_eq!(
        tombstone.vv,
        vv(&[(DEV_A, 3), (DEV_B, 1)]),
        "the tombstone carries the sidecar del's bumped vv (the §2.7 anchor)"
    );

    // Both del entries staged, each bumping its own key's vv.
    let staged: Vec<JournalEntry> = db
        .iter_outbound()
        .expect("outbound")
        .into_iter()
        .map(|(_, b)| {
            JournalEntry::from_json_line(std::str::from_utf8(&b).expect("utf8")).expect("decodes")
        })
        .collect();
    assert_eq!(staged.len(), 2);
    let sdel = staged
        .iter()
        .find(|e| e.key == sidecar_key(&image))
        .expect("sidecar del");
    assert_eq!(sdel.op, Op::Del);
    assert_eq!(sdel.vv, vv(&[(DEV_A, 3), (DEV_B, 1)]));
    let odel = staged
        .iter()
        .find(|e| e.key == library_key(&image))
        .expect("original del");
    assert_eq!(odel.op, Op::Del);
    assert_eq!(odel.vv, vv(&[(DEV_A, 2)]));

    // Records hidden, vv advanced, data intact, deletion recorded.
    for key in [&sidecar_rel, &image] {
        let record = item(&db, key);
        assert!(record.deleted, "{key} must be hidden");
    }
    assert_eq!(item(&db, &sidecar_rel).vv, vv(&[(DEV_A, 3), (DEV_B, 1)]));
    assert_eq!(item(&db, &image).vv, vv(&[(DEV_A, 2)]));
    // §2.3 "one row per deleted key" (review round 0): one deleted-set
    // row PER ITEM, keyed by the item's file relkey with ITS del vv.
    assert_eq!(
        db.get_deleted(&sidecar_rel)
            .expect("get_deleted")
            .expect("sidecar row")
            .vv,
        vv(&[(DEV_A, 3), (DEV_B, 1)])
    );
    assert_eq!(
        db.get_deleted(&image)
            .expect("get_deleted")
            .expect("original row")
            .vv,
        vv(&[(DEV_A, 2)])
    );
    assert_eq!(
        recently_deleted(&db)
            .expect("listing")
            .iter()
            .map(|(k, _)| k.as_str().to_string())
            .collect::<Vec<_>>(),
        vec![image.as_str().to_string(), sidecar_rel.as_str().to_string()]
    );
    // §2.7: data keys are NOT deleted.
    assert_eq!(
        th::get_bytes(&client, &bucket, &sidecar_key(&image)).await,
        d1
    );
    assert_eq!(
        th::get_bytes(&client, &bucket, &library_key(&image)).await,
        orig
    );

    // The manifest hides the items as live rows and carries the del row.
    let manifest = build_manifest(&db, 1_769_950_000).expect("build_manifest");
    assert!(
        manifest.rows.is_empty(),
        "deleted items must not be advertised live: {:?}",
        manifest.rows
    );
    assert_eq!(manifest.deleted.len(), 2, "one deleted row per item");
    assert_eq!(manifest.deleted[0].del, image);
    assert_eq!(manifest.deleted[0].vv, vv(&[(DEV_A, 2)]));
    assert_eq!(manifest.deleted[1].del, sidecar_rel);
    assert_eq!(manifest.deleted[1].vv, vv(&[(DEV_A, 3), (DEV_B, 1)]));

    // Restore: metadata-only dominating puts, flags cleared, row gone.
    let restored = restore_item(&db, &image).expect("restore_item");
    assert_eq!(restored.len(), 2);
    // Restore dominates each ITEM's own deletion (per-lineage vvs —
    // the original's counters are independent of the sidecar's).
    for (key, del_vv) in [
        (&sidecar_rel, vv(&[(DEV_A, 3), (DEV_B, 1)])),
        (&image, vv(&[(DEV_A, 2)])),
    ] {
        let record = item(&db, key);
        assert!(!record.deleted, "{key} restored");
        assert_eq!(
            compare(&record.vv, &del_vv),
            VvOrder::Greater,
            "restore dominates the deletion for {key}"
        );
        assert_eq!(db.get_deleted(key).expect("get_deleted"), None);
    }
    assert!(recently_deleted(&db).expect("listing").is_empty());
    let staged_after: Vec<JournalEntry> = db
        .iter_outbound()
        .expect("outbound")
        .into_iter()
        .map(|(_, b)| {
            JournalEntry::from_json_line(std::str::from_utf8(&b).expect("utf8")).expect("decodes")
        })
        .collect();
    let restore_puts: Vec<&JournalEntry> =
        staged_after.iter().filter(|e| e.op == Op::Put).collect();
    assert_eq!(restore_puts.len(), 2, "sidecar + original restore puts");
    for put in restore_puts {
        assert!(put.blake3.is_some(), "every engine put carries blake3");
        let del_vv = if put.key == sidecar_key(&image) {
            vv(&[(DEV_A, 3), (DEV_B, 1)])
        } else {
            vv(&[(DEV_A, 2)])
        };
        assert_eq!(
            compare(&put.vv, &del_vv),
            VvOrder::Greater,
            "restore put {} dominates its item's del",
            put.key
        );
    }
    assert_eq!(
        db.queue_len(Queue::Up).expect("len"),
        0,
        "restore moves no bytes"
    );

    // Restoring again: nothing is deleted.
    let err = restore_item(&db, &image).expect_err("second restore");
    assert!(matches!(err, EngineError::NotDeleted { .. }), "got {err:?}");
}

#[tokio::test]
async fn delete_of_an_unknown_image_is_typed() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("eng-delete-unknown");
    let client = g.client();
    let (_dbdir, _p, db) = open_db(&dev(DEV_A));
    let err = delete_item(&db, &client, &bucket, &rel("no/such.NEF"), &[Kind::Sidecar])
        .await
        .expect_err("unknown");
    assert!(
        matches!(err, EngineError::UnknownItem { .. }),
        "got {err:?}"
    );
}

// ===========================================================================
// Manifest provenance extensions (owed to this unit by manifest.rs docs)
// ===========================================================================

#[test]
fn manifest_rows_carry_badges_and_device_provenance() {
    let (_dbdir, _p, db) = open_db(&dev(DEV_A));
    let image = rel(IMG);
    let key = sidecar_item_relkey(&image).expect("item");
    let d1 = eh::doc(4, Some("blue"), 0.5);
    let mut record = rec(Kind::Sidecar, ItemState::Synced);
    record.sem_hash = Some(sem_hash(&d1).expect("sem"));
    record.blake3 = Some(Blake3Hex::from_bytes(&d1));
    record.size = d1.len() as u64;
    record.vv = vv(&[(DEV_A, 1)]);
    record.rating = Some(4);
    record.color_label = Some("blue".to_string());
    record.device = Some(dev(DEV_A));
    record.head_ts = Some(1_000);
    db.insert_item(&key, &record).expect("seed");

    let manifest = build_manifest(&db, 1_769_950_000).expect("build");
    assert_eq!(manifest.rows.len(), 1);
    let row = &manifest.rows[0];
    assert_eq!(row.rating, Some(4));
    assert_eq!(row.color_label, Some("blue".to_string()));
    assert_eq!(row.device, Some(dev(DEV_A)));
}

// ===========================================================================
// S8 (unit half): engine puts are unconstructible without a blake3
// ===========================================================================

/// Compile-time pin: the field is a bare [`Blake3Hex`], not an Option —
/// no [`EnginePut`] can exist without one.
fn _engine_put_blake3_is_mandatory(put: &EnginePut) -> &Blake3Hex {
    &put.blake3
}

#[test]
fn engine_put_entries_always_carry_blake3() {
    let image = rel(IMG);
    let bytes = th::patterned(64, 1);
    let put = EnginePut {
        device: dev(DEV_A),
        kind: Kind::Original,
        item: image.clone(),
        vv: vv(&[(DEV_A, 3)]),
        blake3: Blake3Hex::from_bytes(&bytes),
        size: bytes.len() as u64,
        ts: 1_769_900_000,
        sem_hash: None,
        rating: None,
        color_label: None,
        content_id: Some(ContentId::from_bytes(&bytes)),
        w: None,
        h: None,
        mtime: Some(1_769_899_000),
    };
    let entry = put.entry();
    assert_eq!(entry.op, Op::Put);
    assert_eq!(entry.v, JOURNAL_VERSION);
    assert_eq!(entry.seq, 0, "seqs are stamped at publication");
    assert_eq!(entry.key, library_key(&image));
    assert_eq!(entry.vv, vv(&[(DEV_A, 3)]));
    assert_eq!(
        entry.blake3,
        Some(Blake3Hex::from_bytes(&bytes)),
        "blake3 is always Some, by construction from the required field"
    );
    assert_eq!(entry.content_id, Some(ContentId::from_bytes(&bytes)));
    // The sidecar spelling keys canonically too.
    let sput = EnginePut {
        kind: Kind::Sidecar,
        item: sidecar_item_relkey(&image).expect("item"),
        content_id: None,
        sem_hash: Some(sem_hash(&eh::doc(1, None, 0.0)).expect("sem")),
        ..put
    };
    assert_eq!(sput.entry().key, sidecar_key(&image));
    assert!(sput.entry().blake3.is_some());
}

// ===========================================================================
// Additive-field hygiene shared with the manifest advertise gate
// ===========================================================================

#[test]
fn admitted_items_keep_advertising_the_published_version_in_manifests() {
    // A previously published item re-admitted for upload must stay in
    // the manifest as its PUBLISHED version: the row pairs the old vv
    // with the old blake3, never the bumped admitted vv (which would
    // poison peer idempotency — build_manifest's §2.6 coordination
    // note, now load-bearing).
    let (_dbdir, _p, db) = open_db(&dev(DEV_A));
    let image = rel(IMG);
    let key = sidecar_item_relkey(&image).expect("item");
    let d1 = eh::doc(3, None, 0.25);
    let mut record = rec(Kind::Sidecar, ItemState::Synced);
    record.sem_hash = Some(sem_hash(&d1).expect("sem"));
    record.blake3 = Some(Blake3Hex::from_bytes(&d1));
    record.size = d1.len() as u64;
    record.vv = vv(&[(DEV_A, 1)]);
    db.insert_item(&key, &record).expect("seed");
    let d2 = eh::doc(5, None, 0.5);
    notify_local_change(
        &db,
        &image,
        Kind::Sidecar,
        &LocalScan {
            size: d2.len() as u64,
            mtime_unix_ns: 10,
            bytes: &d2,
        },
    )
    .expect("notify");
    admit_pending(&db, |_, _| true).expect("admit");

    let manifest = build_manifest(&db, 1_769_950_000).expect("build");
    assert_eq!(manifest.rows.len(), 1);
    let row = &manifest.rows[0];
    assert_eq!(
        row.vv,
        vv(&[(DEV_A, 1)]),
        "published vv, not the admitted mint"
    );
    assert_eq!(row.blake3, Some(Blake3Hex::from_bytes(&d1)));
    assert_eq!(row.sem_hash, Some(sem_hash(&d1).expect("sem")));
}

// ===========================================================================
// Order-equivalence of the full del/put mix (consumer-level)
// ===========================================================================

#[test]
fn put_and_del_mix_is_order_equivalent() {
    // del-vs-put through the same §2.6 ordering: any arrival order of
    // {v1 put, v2 put, dominating del, dominating restore put} lands in
    // the same final state.
    let image = rel(IMG);
    let d1 = eh::doc(1, None, 0.0);
    let d2 = eh::doc(2, None, 0.1);
    let e1 = put_sidecar(DEV_A, &image, &d1, vv(&[(DEV_A, 1)]), 100);
    let e2 = put_sidecar(DEV_A, &image, &d2, vv(&[(DEV_A, 2)]), 200);
    let e3 = del(
        DEV_A,
        Kind::Sidecar,
        sidecar_key(&image),
        vv(&[(DEV_A, 3)]),
        300,
    );
    let e4 = put_sidecar(DEV_B, &image, &d2, vv(&[(DEV_A, 3), (DEV_B, 1)]), 400);

    let run = |order: &[&JournalEntry]| {
        let (_d, root, db) = scratch(DEV_C);
        let mut events = RecordedEvents::default();
        let mut consumer = EngineConsumer::new(&db, root.path(), &mut events)
            .expect("consumer")
            .with_now(9_000);
        for e in order {
            apply(&db, &mut consumer, e);
        }
        (
            eh::full_items(&db),
            db.iter_deleted().expect("iter_deleted"),
        )
    };
    let forward = run(&[&e1, &e2, &e3, &e4]);
    let reversed = run(&[&e4, &e3, &e2, &e1]);
    let shuffled = run(&[&e2, &e4, &e1, &e3]);
    assert_eq!(forward, reversed, "reversed order diverged");
    assert_eq!(forward, shuffled, "shuffled order diverged");
    let key = sidecar_item_relkey(&image).expect("key");
    let record = &forward.0[key.as_str()];
    assert!(
        !record.deleted,
        "the dominating restore put wins in every order"
    );
    assert_eq!(record.vv, vv(&[(DEV_A, 3), (DEV_B, 1)]));
}

// ===========================================================================
// Review round 0 regressions — the verified failure interleavings, pinned
// ===========================================================================

/// Finding 1 (B1) unit half: with an admitted upload in flight, the
/// §2.6 case-1 content test must compare the entry against the
/// IN-FLIGHT head's content (the local file), never the record's
/// stale published `sem_hash` — an entry matching the OLD published
/// semantics but concurrent with the admitted version is case 4, not
/// convergence.
#[test]
fn in_flight_head_resolves_content_against_the_local_file() {
    let (_d, root, db) = scratch(DEV_A);
    let image = rel(IMG);
    let key = sidecar_item_relkey(&image).expect("item key");
    let v1 = eh::doc(1, None, 0.0);
    let v2 = eh::doc(4, Some("red"), 0.5);
    // Published v1; v2 admitted and in flight (Queued, intent {A:2});
    // the record's sem_hash still names v1 (§2.6 coordination note).
    let mut record = rec(Kind::Sidecar, ItemState::Queued);
    record.sem_hash = Some(sem_hash(&v1).expect("sem"));
    record.blake3 = Some(Blake3Hex::from_bytes(&v1));
    record.vv = vv(&[(DEV_A, 1)]);
    record.admitted_vv = Some(vv(&[(DEV_A, 2)]));
    record.admitted_ts = Some(1_000);
    record.head_ts = Some(1_000);
    record.device = Some(dev(DEV_A));
    db.replay_put_item(&key, &record).expect("seed queued");
    db.queue_push(Queue::Up, &key, 1).expect("queue");
    th::write_file(&item_local_path(root.path(), &key), &v2);

    // B edited-then-reverted: its entry re-advertises v1's SEMANTICS
    // under a vv concurrent with the admitted v2, with a winning ts.
    let entry = put_sidecar(DEV_B, &image, &v1, vv(&[(DEV_A, 1), (DEV_B, 1)]), 9_000);
    assert_eq!(compare(&entry.vv, &vv(&[(DEV_A, 2)])), VvOrder::Concurrent);
    let mut events = RecordedEvents::default();
    let mut consumer = EngineConsumer::new(&db, root.path(), &mut events)
        .expect("consumer")
        .with_now(1_500);
    apply(&db, &mut consumer, &entry);

    // Case 4 fired (pre-fix: silent convergence, zero conflict events,
    // v2 destined to publish under a ts that was not its freeze).
    assert_eq!(events.conflicts.len(), 1, "case 4 must fire");
    assert_eq!(events.conflicts[0].winner_device, dev(DEV_B));
    let suffix = loser_vc_suffix(&v2).expect("suffix");
    let vc_rel = vc_item_relkey(&image, &suffix).expect("vc relkey");
    assert_eq!(events.conflicts[0].copy_relkey, Some(vc_rel.clone()));
    let after = item(&db, &key);
    assert_eq!(after.state, ItemState::PendingDown, "winner fetch pending");
    assert_eq!(after.admitted_vv, None, "intent withdrawn");
    assert_eq!(after.admitted_ts, None);
    assert_eq!(after.sem_hash, Some(sem_hash(&v1).expect("sem")));
    // The in-flight v2 survives as the vc.
    let vc = item(&db, &vc_rel);
    assert_eq!(vc.sem_hash, Some(sem_hash(&v2).expect("sem")));
    assert_eq!(vc.state, ItemState::Queued);
    assert_eq!(
        eh::read_file(&item_local_path(root.path(), &vc_rel)),
        eh::semantic(&v2)
    );
}

/// Finding 1 (B1), local-wins direction: the in-flight head WINS the
/// pick — the entry's branch folds into the record vv, the intent stays
/// exactly as admitted, and nothing converges silently.
#[test]
fn in_flight_head_winning_keeps_its_intent_untouched() {
    let (_d, root, db) = scratch(DEV_A);
    let image = rel(IMG);
    let key = sidecar_item_relkey(&image).expect("item key");
    let v1 = eh::doc(1, None, 0.0);
    let v2 = eh::doc(4, Some("red"), 0.5);
    let mut record = rec(Kind::Sidecar, ItemState::Queued);
    record.sem_hash = Some(sem_hash(&v1).expect("sem"));
    record.blake3 = Some(Blake3Hex::from_bytes(&v1));
    record.vv = vv(&[(DEV_A, 1)]);
    record.admitted_vv = Some(vv(&[(DEV_A, 2)]));
    record.admitted_ts = Some(8_000);
    record.head_ts = Some(8_000);
    record.device = Some(dev(DEV_A));
    db.replay_put_item(&key, &record).expect("seed queued");
    db.queue_push(Queue::Up, &key, 1).expect("queue");
    th::write_file(&item_local_path(root.path(), &key), &v2);

    let entry = put_sidecar(DEV_B, &image, &v1, vv(&[(DEV_A, 1), (DEV_B, 1)]), 2_000);
    let mut events = RecordedEvents::default();
    let mut consumer = EngineConsumer::new(&db, root.path(), &mut events)
        .expect("consumer")
        .with_now(8_500);
    apply(&db, &mut consumer, &entry);

    let after = item(&db, &key);
    assert_eq!(after.state, ItemState::Queued, "still bound for upload");
    assert_eq!(
        after.admitted_vv,
        Some(vv(&[(DEV_A, 2)])),
        "the admitted snapshot is untouched by the losing branch"
    );
    assert_eq!(after.admitted_ts, Some(8_000));
    assert_eq!(after.vv, vv(&[(DEV_A, 1), (DEV_B, 1)]), "branch folded");
    assert_eq!(events.conflicts.len(), 1);
    assert_eq!(events.conflicts[0].winner_device, dev(DEV_A));
}

/// Finding 2/5 (B2/B5) unit half, receiver side: a del CONCURRENT with
/// a committed live head loses (§2.7 edits beat deletes) — the item
/// stays live and ABSORBS the del's vv; no deleted-set row stands.
#[test]
fn concurrent_del_vs_committed_head_folds_and_stays_live() {
    let (_d, root, db) = scratch(DEV_B);
    let image = rel(IMG);
    let d2 = eh::doc(2, Some("blue"), 0.4);
    let key = seed_synced_sidecar(
        &db,
        root.path(),
        &image,
        &d2,
        DEV_B,
        vv(&[(DEV_A, 1), (DEV_B, 1)]),
        2_000,
    );

    // A deleted from the base version: concurrent with our edit.
    let entry = del(
        DEV_A,
        Kind::Sidecar,
        sidecar_key(&image),
        vv(&[(DEV_A, 2)]),
        1_500,
    );
    assert_eq!(
        compare(&entry.vv, &vv(&[(DEV_A, 1), (DEV_B, 1)])),
        VvOrder::Concurrent,
        "precondition"
    );
    let mut events = RecordedEvents::default();
    let mut consumer = EngineConsumer::new(&db, root.path(), &mut events)
        .expect("consumer")
        .with_now(3_000);
    apply(&db, &mut consumer, &entry);

    let record = item(&db, &key);
    assert!(!record.deleted, "edits beat deletes: the item stays live");
    assert_eq!(
        record.vv,
        vv(&[(DEV_A, 2), (DEV_B, 1)]),
        "the losing del's vv is absorbed (pre-fix: dropped entirely)"
    );
    assert_eq!(record.sem_hash, Some(sem_hash(&d2).expect("sem")));
    assert_eq!(db.get_deleted(&key).expect("row"), None, "no standing row");
    assert_eq!(db.get_deleted(&image).expect("row"), None);
    assert!(recently_deleted(&db).expect("listing").is_empty());
}

/// Finding 2/5 (B2/B5) unit half, deleter side: a put CONCURRENT with
/// the applied deletion un-hides the item and adopts the put as the
/// head — with NO vc materialized from the deleted content and no
/// conflict pick (the delete superseded the local content; the edit
/// beat the delete).
#[test]
fn concurrent_put_over_a_deleted_record_undeletes_and_adopts() {
    let (_d, root, db) = scratch(DEV_A);
    let image = rel(IMG);
    let d1 = eh::doc(1, None, 0.0);
    let key = seed_synced_sidecar(&db, root.path(), &image, &d1, DEV_A, vv(&[(DEV_A, 1)]), 100);
    let mut events = RecordedEvents::default();
    let mut consumer = EngineConsumer::new(&db, root.path(), &mut events)
        .expect("consumer")
        .with_now(1_000);
    // Our own deletion applies first.
    apply(
        &db,
        &mut consumer,
        &del(
            DEV_A,
            Kind::Sidecar,
            sidecar_key(&image),
            vv(&[(DEV_A, 2)]),
            900,
        ),
    );
    assert!(item(&db, &key).deleted);
    assert!(db.get_deleted(&key).expect("row").is_some());

    // B's edit, concurrent with the deletion, wins (edits beat deletes).
    let d2 = eh::doc(5, Some("green"), 0.9);
    let entry = put_sidecar(DEV_B, &image, &d2, vv(&[(DEV_A, 1), (DEV_B, 1)]), 2_000);
    apply(&db, &mut consumer, &entry);

    let record = item(&db, &key);
    assert!(!record.deleted, "the winning edit un-hides the item");
    assert_eq!(record.state, ItemState::PendingDown, "fetch the edit");
    assert_eq!(record.vv, vv(&[(DEV_A, 2), (DEV_B, 1)]), "both folded");
    assert_eq!(record.sem_hash, Some(sem_hash(&d2).expect("sem")));
    assert_eq!(db.get_deleted(&key).expect("row"), None, "row withdrawn");
    assert_eq!(
        drain_queue(&db, Queue::Down),
        vec![key.as_str().to_string()]
    );
    assert!(
        events.conflicts.is_empty(),
        "no §2.6 pick runs against deleted content"
    );
    assert_eq!(
        db.iter_items().expect("items").len(),
        1,
        "no vc leaks the deleted content back"
    );
}

/// Finding 5 (B5), third-device half: the probe's exact entry pair —
/// put {A:2} ts 2000 (committed edit) vs del {A:1,X:1} ts 1500
/// (concurrent delete) — lands IDENTICAL state in both arrival orders,
/// on a fresh device and on one that knew v1 first.
#[test]
fn concurrent_del_vs_committed_put_is_arrival_order_equivalent() {
    let image = rel(IMG);
    let d2 = eh::doc(2, None, 0.1);
    let put = put_sidecar(DEV_A, &image, &d2, vv(&[(DEV_A, 2)]), 2_000);
    let dele = del(
        "deadbeef-0000-4000-9000-000000000002",
        Kind::Sidecar,
        sidecar_key(&image),
        vv(&[(DEV_A, 1), ("deadbeef-0000-4000-9000-000000000002", 1)]),
        1_500,
    );
    assert_eq!(compare(&put.vv, &dele.vv), VvOrder::Concurrent);

    let run = |seed_v1: bool, order: &[&JournalEntry]| {
        let (_d, root, db) = scratch(DEV_C);
        if seed_v1 {
            let d1 = eh::doc(1, None, 0.0);
            seed_synced_sidecar(&db, root.path(), &image, &d1, DEV_A, vv(&[(DEV_A, 1)]), 100);
        }
        let mut events = RecordedEvents::default();
        let mut consumer = EngineConsumer::new(&db, root.path(), &mut events)
            .expect("consumer")
            .with_now(9_000);
        for e in order {
            apply(&db, &mut consumer, e);
        }
        (
            eh::full_items(&db),
            db.iter_deleted().expect("iter_deleted"),
        )
    };
    for seed_v1 in [false, true] {
        let put_first = run(seed_v1, &[&put, &dele]);
        let del_first = run(seed_v1, &[&dele, &put]);
        assert_eq!(
            put_first, del_first,
            "arrival order diverged (seed_v1={seed_v1})"
        );
        let key = sidecar_item_relkey(&image).expect("key");
        let record = &put_first.0[key.as_str()];
        assert!(!record.deleted, "edits beat deletes in every order");
        assert_eq!(
            record.vv,
            vv(&[(DEV_A, 2), ("deadbeef-0000-4000-9000-000000000002", 1)])
        );
        assert!(put_first.1.is_empty(), "no standing deletion rows");
    }
}

/// Two CONCURRENT deletes of one item both stand: the record and the
/// row fold both vvs, in both arrival orders.
#[test]
fn concurrent_dels_fold_rows_and_stay_hidden_in_both_orders() {
    let image = rel(IMG);
    let del_a = del(
        DEV_A,
        Kind::Sidecar,
        sidecar_key(&image),
        vv(&[(DEV_A, 2)]),
        900,
    );
    let del_b = del(
        DEV_B,
        Kind::Sidecar,
        sidecar_key(&image),
        vv(&[(DEV_A, 1), (DEV_B, 1)]),
        950,
    );
    let run = |order: &[&JournalEntry]| {
        let (_d, root, db) = scratch(DEV_C);
        let d1 = eh::doc(1, None, 0.0);
        seed_synced_sidecar(&db, root.path(), &image, &d1, DEV_A, vv(&[(DEV_A, 1)]), 100);
        let mut events = RecordedEvents::default();
        let mut consumer = EngineConsumer::new(&db, root.path(), &mut events)
            .expect("consumer")
            .with_now(9_000);
        for e in order {
            apply(&db, &mut consumer, e);
        }
        (
            eh::full_items(&db),
            db.iter_deleted().expect("iter_deleted"),
        )
    };
    let ab = run(&[&del_a, &del_b]);
    let ba = run(&[&del_b, &del_a]);
    assert_eq!(ab, ba, "del/del arrival order diverged");
    let key = sidecar_item_relkey(&image).expect("key");
    assert!(ab.0[key.as_str()].deleted, "both deletes stand");
    assert_eq!(ab.0[key.as_str()].vv, vv(&[(DEV_A, 2), (DEV_B, 1)]));
    assert_eq!(ab.1.len(), 1);
    assert_eq!(ab.1[0].0, key);
    assert_eq!(ab.1[0].1.vv, vv(&[(DEV_A, 2), (DEV_B, 1)]), "row folded");
}

/// Finding 7 (B6) unit half: edits-beat-deletes resurrection with
/// ASYMMETRIC vvs — original authored on A, sidecar authored on B, A
/// deletes both, C holds uncommitted dirt. The original's resurrection
/// put is concurrent with the original's OWN del, and the deleter must
/// still un-hide it (content-equal converge + §2.7 edits-beat-deletes
/// undelete).
#[test]
fn asymmetric_resurrection_unhides_the_original_on_the_deleter() {
    let image = rel(IMG);
    let orig_bytes = th::patterned(2048, 7);
    let key = sidecar_item_relkey(&image).expect("sidecar item");
    let published = eh::doc(1, None, 0.0);
    let edited = eh::doc(5, Some("red"), 0.9);

    // --- C: dirty sidecar (authored on B: vv {B:1}), original {A:1}.
    let (_d, root, db_c) = scratch(DEV_C);
    let mut sidecar = rec(Kind::Sidecar, ItemState::Dirty);
    sidecar.sem_hash = Some(sem_hash(&published).expect("sem"));
    sidecar.blake3 = Some(Blake3Hex::from_bytes(&published));
    sidecar.vv = vv(&[(DEV_B, 1)]);
    db_c.insert_item(&key, &sidecar).expect("seed sidecar");
    th::write_file(&item_local_path(root.path(), &key), &edited);
    let mut original = rec(Kind::Original, ItemState::Stub);
    original.blake3 = Some(Blake3Hex::from_bytes(&orig_bytes));
    original.content_id = Some(ContentId::from_bytes(&orig_bytes));
    original.size = orig_bytes.len() as u64;
    original.vv = vv(&[(DEV_A, 1)]);
    db_c.insert_item(&image, &original).expect("seed original");

    // A's dels: sidecar del {A:1,B:1}, original del {A:2} (per-lineage).
    let del_sidecar = del(
        DEV_A,
        Kind::Sidecar,
        sidecar_key(&image),
        vv(&[(DEV_A, 1), (DEV_B, 1)]),
        700,
    );
    let del_original = del(
        DEV_A,
        Kind::Original,
        library_key(&image),
        vv(&[(DEV_A, 2)]),
        700,
    );
    let mut events = RecordedEvents::default();
    let mut consumer = EngineConsumer::new(&db_c, root.path(), &mut events)
        .expect("consumer")
        .with_now(1_500);
    apply(&db_c, &mut consumer, &del_sidecar);
    apply(&db_c, &mut consumer, &del_original);

    // C: whole item live, both resurrection puts staged.
    assert!(!item(&db_c, &key).deleted && !item(&db_c, &image).deleted);
    let staged: Vec<JournalEntry> = db_c
        .iter_outbound()
        .expect("outbound")
        .into_iter()
        .map(|(_, b)| {
            JournalEntry::from_json_line(std::str::from_utf8(&b).expect("utf8")).expect("decodes")
        })
        .collect();
    let orig_put = staged
        .iter()
        .find(|e| e.op == Op::Put && e.key == library_key(&image))
        .expect("original resurrection put");
    // The resurrection vv {A:1,B:1,C:1} is CONCURRENT with del_original
    // {A:2} — the shape the old strictly-Greater undelete lost.
    assert_eq!(compare(&orig_put.vv, &del_original.vv), VvOrder::Concurrent);

    // --- A (the deleter): both items hidden, then C's puts arrive.
    let (_d2, root_a, db_a) = scratch(DEV_A);
    let mut a_sidecar = rec(Kind::Sidecar, ItemState::Synced);
    a_sidecar.sem_hash = Some(sem_hash(&published).expect("sem"));
    a_sidecar.blake3 = Some(Blake3Hex::from_bytes(&published));
    a_sidecar.vv = vv(&[(DEV_A, 1), (DEV_B, 1)]);
    a_sidecar.deleted = true;
    db_a.replay_put_item(&key, &a_sidecar).expect("seed");
    let mut a_original = rec(Kind::Original, ItemState::Hydrated);
    a_original.blake3 = Some(Blake3Hex::from_bytes(&orig_bytes));
    a_original.content_id = Some(ContentId::from_bytes(&orig_bytes));
    a_original.vv = vv(&[(DEV_A, 2)]);
    a_original.deleted = true;
    db_a.replay_put_item(&image, &a_original).expect("seed");
    db_a.record_deleted(
        &key,
        &rrcloud_core::state::DeletedRecord {
            vv: vv(&[(DEV_A, 1), (DEV_B, 1)]),
            server_ts: 700,
        },
    )
    .expect("row");
    db_a.record_deleted(
        &image,
        &rrcloud_core::state::DeletedRecord {
            vv: vv(&[(DEV_A, 2)]),
            server_ts: 700,
        },
    )
    .expect("row");

    // The sidecar resurrection goes through the upload lane (its dirt
    // was admitted); the entry the §2.4 seam will publish carries
    // exactly the admitted (vv, ts) and the sent bytes' facts.
    let c_sidecar = item(&db_c, &key);
    let admitted = c_sidecar.admitted_vv.clone().expect("admitted intent");
    let sidecar_put = put_sidecar(
        DEV_C,
        &image,
        &edited,
        admitted,
        c_sidecar.admitted_ts.expect("admitted ts"),
    );
    let mut events_a = RecordedEvents::default();
    let mut consumer_a = EngineConsumer::new(&db_a, root_a.path(), &mut events_a)
        .expect("consumer")
        .with_now(2_000);
    apply(&db_a, &mut consumer_a, &sidecar_put);
    apply(&db_a, &mut consumer_a, orig_put);

    assert!(
        !item(&db_a, &key).deleted,
        "sidecar un-hidden on the deleter"
    );
    assert!(
        !item(&db_a, &image).deleted,
        "original un-hidden on the deleter although its resurrection vv \
         is only CONCURRENT with its del (review round 0)"
    );
    assert_eq!(
        item(&db_a, &image).vv,
        vv(&[(DEV_A, 2), (DEV_B, 1), (DEV_C, 1)]),
        "both branches folded"
    );
    assert_eq!(db_a.get_deleted(&key).expect("row"), None);
    assert_eq!(db_a.get_deleted(&image).expect("row"), None);
    assert!(recently_deleted(&db_a).expect("listing").is_empty());
}

/// Finding 8 (B7): deleting a VIRTUAL COPY records its deleted-set row
/// under the VC ITEM's own key on the deleter AND on appliers, and a
/// fresh device applying either prefix order keeps the primary
/// original+sidecar live — the vc del hides only the vc.
#[tokio::test]
async fn deleting_a_virtual_copy_hides_only_the_vc_in_every_order() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("eng-vc-delete");
    let client = g.client();
    let image = rel(IMG);
    let key = sidecar_item_relkey(&image).expect("sidecar item");
    let loser = eh::doc(3, Some("red"), 0.25);
    let suffix = loser_vc_suffix(&loser).expect("suffix");
    let vc_rel = vc_item_relkey(&image, &suffix).expect("vc relkey");
    let d1 = eh::doc(1, None, 0.0);
    let orig_bytes = th::patterned(1024, 3);

    // Deleter B holds primaries + the vc.
    let (_d, _root, db_b) = open_db(&dev(DEV_B));
    let mut orig = rec(Kind::Original, ItemState::Hydrated);
    orig.blake3 = Some(Blake3Hex::from_bytes(&orig_bytes));
    orig.content_id = Some(ContentId::from_bytes(&orig_bytes));
    orig.vv = vv(&[(DEV_A, 1)]);
    db_b.insert_item(&image, &orig).expect("seed orig");
    let mut primary = rec(Kind::Sidecar, ItemState::Synced);
    primary.sem_hash = Some(sem_hash(&d1).expect("sem"));
    primary.blake3 = Some(Blake3Hex::from_bytes(&d1));
    primary.vv = vv(&[(DEV_A, 1)]);
    db_b.insert_item(&key, &primary).expect("seed primary");
    let mut vc = rec(Kind::Sidecar, ItemState::Synced);
    vc.sem_hash = Some(sem_hash(&loser).expect("sem"));
    vc.blake3 = Some(Blake3Hex::from_bytes(&loser));
    vc.vv = vv(&[(DEV_B, 2)]);
    db_b.insert_item(&vc_rel, &vc).expect("seed vc");

    // The only lane for discarding a conflict copy: delete the vc item.
    let outcome = delete_item(&db_b, &client, &bucket, &vc_rel, &[Kind::Sidecar])
        .await
        .expect("delete vc");
    assert_eq!(outcome.staged, vec![vc_rel.clone()]);
    assert!(item(&db_b, &vc_rel).deleted, "vc hidden on the deleter");
    assert!(!item(&db_b, &key).deleted, "primary sidecar untouched");
    assert!(!item(&db_b, &image).deleted, "original untouched");
    assert!(
        db_b.get_deleted(&vc_rel).expect("row").is_some(),
        "the row is keyed by the VC ITEM's own relkey on the deleter"
    );
    assert_eq!(db_b.get_deleted(&image).expect("row"), None);
    assert_eq!(db_b.get_deleted(&key).expect("row"), None);

    // The staged del aims at the vc's own bucket key.
    let staged: Vec<JournalEntry> = db_b
        .iter_outbound()
        .expect("outbound")
        .into_iter()
        .map(|(_, b)| {
            JournalEntry::from_json_line(std::str::from_utf8(&b).expect("utf8")).expect("decodes")
        })
        .collect();
    let vc_del = staged.iter().find(|e| e.op == Op::Del).expect("vc del");
    assert_eq!(vc_del.key, library_key(&vc_rel));

    // An APPLYING device with the same items records the row under the
    // SAME key (pre-fix: deleter keyed by vc relkey, appliers by the
    // base image — fleet-divergent deleted sets).
    let (_d2, root_x, db_x) = scratch(DEV_X);
    db_x.insert_item(&image, &orig).expect("seed orig");
    db_x.insert_item(&key, &primary).expect("seed primary");
    db_x.insert_item(&vc_rel, &vc).expect("seed vc");
    let mut events = RecordedEvents::default();
    let mut consumer = EngineConsumer::new(&db_x, root_x.path(), &mut events)
        .expect("consumer")
        .with_now(5_000);
    apply(&db_x, &mut consumer, vc_del);
    assert!(item(&db_x, &vc_rel).deleted);
    assert!(!item(&db_x, &key).deleted && !item(&db_x, &image).deleted);
    assert!(db_x.get_deleted(&vc_rel).expect("row").is_some());
    assert_eq!(db_x.get_deleted(&image).expect("row"), None);

    // A FRESH device applying {vc put, vc del} before the primaries —
    // or after — keeps the primaries live either way (pre-fix: the
    // base-keyed row hid the whole image on the del-first order).
    let mut vc_put = put_sidecar(DEV_B, &image, &loser, vv(&[(DEV_B, 2)]), 1_000);
    vc_put.key = library_key(&vc_rel);
    let primary_put = put_sidecar(DEV_A, &image, &d1, vv(&[(DEV_A, 1)]), 500);
    let orig_put = put_original(DEV_A, &image, &orig_bytes, vv(&[(DEV_A, 1)]), 500);
    let run = |order: &[&JournalEntry]| {
        let (_d, root, db) = scratch(DEV_C);
        let mut events = RecordedEvents::default();
        let mut consumer = EngineConsumer::new(&db, root.path(), &mut events)
            .expect("consumer")
            .with_now(9_000);
        for e in order {
            apply(&db, &mut consumer, e);
        }
        (
            eh::full_items(&db),
            db.iter_deleted().expect("iter_deleted"),
        )
    };
    let b_first = run(&[&vc_put, vc_del, &primary_put, &orig_put]);
    let a_first = run(&[&primary_put, &orig_put, &vc_put, vc_del]);
    assert_eq!(b_first, a_first, "arrival order diverged");
    assert!(!b_first.0[key.as_str()].deleted, "primary sidecar live");
    assert!(!b_first.0[image.as_str()].deleted, "original live");
    assert!(b_first.0[vc_rel.as_str()].deleted, "only the vc hidden");
}

/// Finding M2: delete_item addresses an xmp item (self-keyed by the
/// caller) and restore brings it back; the kind filter no longer drops
/// the request on the floor.
#[tokio::test]
async fn delete_item_addresses_xmp_items_and_restore_reverses_it() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("eng-xmp-delete");
    let client = g.client();
    let (_d, _root, db) = open_db(&dev(DEV_A));
    let xmp_rel = rel("p/img.xmp");
    let xmp_bytes = b"<x:xmpmeta/>";
    let mut xmp = rec(Kind::Xmp, ItemState::Synced);
    xmp.blake3 = Some(Blake3Hex::from_bytes(xmp_bytes));
    xmp.content_id = Some(ContentId::from_bytes(xmp_bytes));
    xmp.vv = vv(&[(DEV_A, 1)]);
    db.insert_item(&xmp_rel, &xmp).expect("seed xmp");

    let outcome = delete_item(&db, &client, &bucket, &xmp_rel, &[Kind::Xmp])
        .await
        .expect("delete xmp (pre-fix: UnknownItem)");
    assert_eq!(outcome.staged, vec![xmp_rel.clone()]);
    assert_eq!(outcome.tombstone.kinds, vec![Kind::Xmp]);
    assert!(item(&db, &xmp_rel).deleted);
    assert!(db.get_deleted(&xmp_rel).expect("row").is_some());
    let staged: Vec<JournalEntry> = db
        .iter_outbound()
        .expect("outbound")
        .into_iter()
        .map(|(_, b)| {
            JournalEntry::from_json_line(std::str::from_utf8(&b).expect("utf8")).expect("decodes")
        })
        .collect();
    let xdel = staged.iter().find(|e| e.op == Op::Del).expect("xmp del");
    assert_eq!(xdel.kind, Kind::Xmp);
    assert_eq!(xdel.key, library_key(&xmp_rel));

    let restored = restore_item(&db, &xmp_rel).expect("restore xmp");
    assert_eq!(restored, vec![xmp_rel.clone()]);
    assert!(!item(&db, &xmp_rel).deleted);
    assert_eq!(db.get_deleted(&xmp_rel).expect("row"), None);
}

/// Finding 10 (B8a): a deleted-then-restored `PendingDown` item is
/// re-pushed onto the download queue — the delete dequeued it and
/// nothing else ever would have re-queued it.
#[test]
fn restore_requeues_a_pending_down_item() {
    let (_d, root, db) = scratch(DEV_B);
    let image = rel(IMG);
    let key = sidecar_item_relkey(&image).expect("item key");
    let d1 = eh::doc(1, None, 0.0);
    let mut events = RecordedEvents::default();
    let mut consumer = EngineConsumer::new(&db, root.path(), &mut events)
        .expect("consumer")
        .with_now(1_000);
    // Remote put creates the item queued-down; a dominating del hides
    // and dequeues it.
    apply(
        &db,
        &mut consumer,
        &put_sidecar(DEV_A, &image, &d1, vv(&[(DEV_A, 1)]), 500),
    );
    apply(
        &db,
        &mut consumer,
        &del(
            DEV_A,
            Kind::Sidecar,
            sidecar_key(&image),
            vv(&[(DEV_A, 2)]),
            900,
        ),
    );
    assert!(item(&db, &key).deleted);
    assert_eq!(db.queue_len(Queue::Down).expect("len"), 0, "dequeued");

    let restored = restore_item(&db, &image).expect("restore");
    assert_eq!(restored, vec![key.clone()]);
    let record = item(&db, &key);
    assert!(!record.deleted);
    assert_eq!(record.state, ItemState::PendingDown);
    assert_eq!(
        drain_queue(&db, Queue::Down),
        vec![key.as_str().to_string()],
        "restore must re-queue the fetch (pre-fix: wedged off the queue)"
    );
}

/// Finding 10 (B8b) unit half: restoring an item whose upload lane was
/// stranded by the delete (Queued with a withdrawn intent) demotes it
/// to Dirty so the next admission re-admits the LOCAL FILE's content —
/// the lane that makes a withdrawn v2 reachable again.
#[test]
fn restore_normalizes_a_wedged_queued_item_to_dirty() {
    let (_d, _root, db) = scratch(DEV_B);
    let image = rel(IMG);
    let key = sidecar_item_relkey(&image).expect("item key");
    let d1 = eh::doc(1, None, 0.0);
    // Published v1, then v2 admitted (Queued) — and then deleted: the
    // delete withdrew the intent, dequeued, and hid the record.
    let mut record = rec(Kind::Sidecar, ItemState::Queued);
    record.sem_hash = Some(sem_hash(&d1).expect("sem"));
    record.blake3 = Some(Blake3Hex::from_bytes(&d1));
    record.vv = vv(&[(DEV_B, 2)]);
    record.deleted = true;
    db.replay_put_item(&key, &record).expect("seed");
    db.record_deleted(
        &key,
        &rrcloud_core::state::DeletedRecord {
            vv: vv(&[(DEV_B, 2)]),
            server_ts: 900,
        },
    )
    .expect("row");

    let restored = restore_item(&db, &image).expect("restore");
    assert_eq!(restored, vec![key.clone()]);
    let after = item(&db, &key);
    assert!(!after.deleted);
    assert_eq!(
        after.state,
        ItemState::Dirty,
        "the stranded upload lane is normalized to Dirty (pre-fix: \
         Queued forever with an empty queue — the local edit unreachable)"
    );
    assert_eq!(after.admitted_vv, None);
    // The next admission picks it up: the heal lane exists.
    let admitted = admit_pending(&db, |_, _| true).expect("admit");
    assert_eq!(admitted, vec![key.clone()]);
    assert_eq!(item(&db, &key).state, ItemState::Queued);
    assert!(item(&db, &key).admitted_vv.is_some());
}

/// Finding m2: restore's target matcher is structural — a sibling image
/// whose relkey merely extends `<image>.` is NOT restored and keeps its
/// own deletion row.
#[test]
fn restore_does_not_overmatch_sibling_images() {
    let (_d, _root, db) = scratch(DEV_A);
    let image = rel(IMG);
    let key = sidecar_item_relkey(&image).expect("item key");
    let sibling = rel("p/img.NEF.bak"); // its own image, not ours
    let d1 = eh::doc(1, None, 0.0);
    for (k, kind) in [
        (&image, Kind::Original),
        (&key, Kind::Sidecar),
        (&sibling, Kind::Original),
    ] {
        let mut r = rec(kind, ItemState::Synced);
        r.blake3 = Some(Blake3Hex::from_bytes(&d1));
        if kind == Kind::Sidecar {
            r.sem_hash = Some(sem_hash(&d1).expect("sem"));
        } else {
            r.content_id = Some(ContentId::from_bytes(&d1));
        }
        r.vv = vv(&[(DEV_A, 1)]);
        r.deleted = true;
        db.replay_put_item(k, &r).expect("seed");
        db.record_deleted(
            k,
            &rrcloud_core::state::DeletedRecord {
                vv: vv(&[(DEV_A, 2)]),
                server_ts: 900,
            },
        )
        .expect("row");
    }

    let mut restored = restore_item(&db, &image).expect("restore");
    restored.sort();
    assert_eq!(restored, vec![image.clone(), key.clone()]);
    assert!(!item(&db, &image).deleted && !item(&db, &key).deleted);
    assert!(
        item(&db, &sibling).deleted,
        "the sibling image stays deleted (pre-fix: prefix over-match \
         resurrected it while leaving its row standing)"
    );
    assert!(
        db.get_deleted(&sibling).expect("row").is_some(),
        "the sibling's row stands with its items"
    );
}

/// Finding m4: a dominating put without size/mtime keeps the local
/// facts instead of zeroing them (adopt path parity with converge).
#[test]
fn adopt_without_size_keeps_local_size_and_mtime() {
    let (_d, root, db) = scratch(DEV_A);
    let image = rel(IMG);
    let d1 = eh::doc(1, None, 0.0);
    let key = seed_synced_sidecar(&db, root.path(), &image, &d1, DEV_A, vv(&[(DEV_A, 1)]), 100);
    let before = item(&db, &key);
    assert!(before.size > 0, "precondition");

    let d2 = eh::doc(5, Some("green"), 1.0);
    let mut entry = put_sidecar(DEV_B, &image, &d2, vv(&[(DEV_A, 1), (DEV_B, 1)]), 200);
    entry.size = None;
    entry.mtime = None;
    let mut events = RecordedEvents::default();
    let mut consumer = EngineConsumer::new(&db, root.path(), &mut events)
        .expect("consumer")
        .with_now(500);
    apply(&db, &mut consumer, &entry);

    let record = item(&db, &key);
    assert_eq!(record.state, ItemState::PendingDown);
    assert_eq!(record.size, before.size, "size keeps the local fallback");
    assert_eq!(record.mtime_unix_ns, before.mtime_unix_ns);
}

/// Finding m7: intake over a pipeline-interior state refreshes the
/// scanned identity but defers the dirty mark — and says so with the
/// dedicated outcome instead of a misleading `MarkedDirty`.
#[test]
fn notify_on_pipeline_interior_state_returns_deferred() {
    let (_d, _root, db) = scratch(DEV_A);
    let image = rel(IMG);
    let key = sidecar_item_relkey(&image).expect("item key");
    let d1 = eh::doc(1, None, 0.0);
    let mut record = rec(Kind::Sidecar, ItemState::Queued);
    record.sem_hash = Some(sem_hash(&d1).expect("sem"));
    record.vv = vv(&[(DEV_A, 1)]);
    record.admitted_vv = Some(vv(&[(DEV_A, 2)]));
    db.replay_put_item(&key, &record).expect("seed");

    let d2 = eh::doc(4, Some("red"), 0.5);
    let outcome = notify_local_change(
        &db,
        &image,
        Kind::Sidecar,
        &LocalScan {
            size: d2.len() as u64,
            mtime_unix_ns: 10,
            bytes: &d2,
        },
    )
    .expect("notify");
    assert_eq!(
        outcome,
        ChangeOutcome::DeferredToPipeline,
        "the pipeline owns the state; nothing was marked dirty"
    );
    assert_eq!(item(&db, &key).state, ItemState::Queued, "state untouched");
    assert_eq!(
        item(&db, &key).rating,
        Some(4),
        "the badge refresh still lands"
    );
}

/// Finding M5: loser preservation is never skipped silently — a missing
/// local file fires the event (and the apply still completes), while a
/// transient I/O failure aborts the apply transaction entirely.
#[test]
fn loser_preservation_failures_are_event_or_abort_never_silent() {
    // (a) FileMissing: state says we hold the loser, the file is gone.
    let (_d, root, db) = scratch(DEV_A);
    let image = rel(IMG);
    let ours = eh::doc(3, Some("red"), 0.25);
    let key = seed_synced_sidecar(
        &db,
        root.path(),
        &image,
        &ours,
        DEV_A,
        vv(&[(DEV_A, 2)]),
        100,
    );
    std::fs::remove_file(item_local_path(root.path(), &key)).expect("drop the file");
    let theirs = eh::doc(5, Some("green"), 0.75);
    let entry = put_sidecar(DEV_B, &image, &theirs, vv(&[(DEV_A, 1), (DEV_B, 1)]), 9_000);
    let mut events = RecordedEvents::default();
    let mut consumer = EngineConsumer::new(&db, root.path(), &mut events)
        .expect("consumer")
        .with_now(500);
    apply(&db, &mut consumer, &entry);
    assert_eq!(events.loser_skipped.len(), 1, "no silent skip");
    assert_eq!(
        events.loser_skipped[0].reason,
        rrcloud_core::engine::LoserSkipReason::FileMissing
    );
    assert_eq!(
        item(&db, &key).state,
        ItemState::PendingDown,
        "winner adopted"
    );

    // (b) Unparsable: the file exists but is not a sidecar document.
    let (_d2, root2, db2) = scratch(DEV_A);
    let key2 = seed_synced_sidecar(
        &db2,
        root2.path(),
        &image,
        &ours,
        DEV_A,
        vv(&[(DEV_A, 2)]),
        100,
    );
    th::write_file(&item_local_path(root2.path(), &key2), b"not json");
    let mut events2 = RecordedEvents::default();
    let mut consumer2 = EngineConsumer::new(&db2, root2.path(), &mut events2)
        .expect("consumer")
        .with_now(500);
    apply(&db2, &mut consumer2, &entry);
    assert_eq!(events2.loser_skipped.len(), 1);
    assert_eq!(
        events2.loser_skipped[0].reason,
        rrcloud_core::engine::LoserSkipReason::Unparsable
    );

    // (c) A transient read failure (here: the path is a directory, so
    // the read errors without being NotFound) aborts the apply — the
    // reader would retry next poll; nothing half-applies.
    let (_d3, root3, db3) = scratch(DEV_A);
    let key3 = seed_synced_sidecar(
        &db3,
        root3.path(),
        &image,
        &ours,
        DEV_A,
        vv(&[(DEV_A, 2)]),
        100,
    );
    std::fs::remove_file(item_local_path(root3.path(), &key3)).expect("drop");
    std::fs::create_dir(item_local_path(root3.path(), &key3)).expect("dir in the way");
    let before = item(&db3, &key3);
    let mut events3 = RecordedEvents::default();
    let mut consumer3 = EngineConsumer::new(&db3, root3.path(), &mut events3)
        .expect("consumer")
        .with_now(500);
    let result = db3.with_txn_err::<_, ConsumerError>(|t| consumer3.apply(t, &entry));
    assert!(result.is_err(), "transient I/O must abort the apply");
    assert_eq!(item(&db3, &key3), before, "transaction rolled back whole");
    assert!(events3.loser_skipped.is_empty());

    // (d) Displaced-original staging: same no-silent-skip contract.
    let (_d4, root4, db4) = scratch(DEV_A);
    let orig_bytes = th::patterned(512, 9);
    let mut orig = rec(Kind::Original, ItemState::Hydrated);
    orig.blake3 = Some(Blake3Hex::from_bytes(&orig_bytes));
    orig.content_id = Some(ContentId::from_bytes(&orig_bytes));
    orig.vv = vv(&[(DEV_A, 2)]);
    orig.device = Some(dev(DEV_A));
    orig.head_ts = Some(100);
    db4.replay_put_item(&image, &orig).expect("seed");
    // No local file at all.
    let other = th::patterned(512, 10);
    let oentry = put_original(DEV_B, &image, &other, vv(&[(DEV_A, 1), (DEV_B, 1)]), 9_000);
    let mut events4 = RecordedEvents::default();
    let mut consumer4 = EngineConsumer::new(&db4, root4.path(), &mut events4)
        .expect("consumer")
        .with_now(500);
    apply(&db4, &mut consumer4, &oentry);
    assert_eq!(events4.loser_skipped.len(), 1);
    assert_eq!(
        events4.loser_skipped[0].reason,
        rrcloud_core::engine::LoserSkipReason::FileMissing
    );
    assert!(events4.original_conflicts.is_empty(), "nothing was staged");
}

/// Finding M3: the loser-vc dedup is honestly (key, sem_hash) — a
/// DIFFERENT document already resident at the derived vc key is never
/// clobbered (file or record), and nothing is staged for the collider.
#[test]
fn vc_suffix_collision_with_different_document_never_clobbers() {
    let (_d, root, db) = scratch(DEV_A);
    let image = rel(IMG);
    let ours = eh::doc(3, Some("red"), 0.25);
    let key = seed_synced_sidecar(
        &db,
        root.path(),
        &image,
        &ours,
        DEV_A,
        vv(&[(DEV_A, 2)]),
        100,
    );
    let _ = key;
    // A legitimately different vc already lives at OUR loser's key
    // (modeling a 24-bit suffix-prefix collision).
    let suffix = loser_vc_suffix(&ours).expect("suffix");
    let vc_rel = vc_item_relkey(&image, &suffix).expect("vc relkey");
    let resident_doc = eh::doc(1, Some("blue"), 0.9);
    let mut resident = rec(Kind::Sidecar, ItemState::Synced);
    resident.sem_hash = Some(sem_hash(&resident_doc).expect("sem"));
    resident.blake3 = Some(Blake3Hex::from_bytes(&resident_doc));
    resident.vv = vv(&[(DEV_C, 1)]);
    db.insert_item(&vc_rel, &resident)
        .expect("seed resident vc");
    th::write_file(&item_local_path(root.path(), &vc_rel), &resident_doc);

    let theirs = eh::doc(5, Some("green"), 0.75);
    let entry = put_sidecar(DEV_B, &image, &theirs, vv(&[(DEV_A, 1), (DEV_B, 1)]), 9_000);
    let mut events = RecordedEvents::default();
    let mut consumer = EngineConsumer::new(&db, root.path(), &mut events)
        .expect("consumer")
        .with_now(500);
    apply(&db, &mut consumer, &entry);

    assert_eq!(
        eh::read_file(&item_local_path(root.path(), &vc_rel)),
        resident_doc,
        "the resident vc's file is never overwritten (pre-fix: clobbered \
         while the record kept the old document's identity)"
    );
    assert_eq!(
        item(&db, &vc_rel).sem_hash,
        Some(sem_hash(&resident_doc).expect("sem")),
        "record untouched"
    );
    assert_eq!(
        item(&db, &vc_rel).state,
        ItemState::Synced,
        "nothing staged"
    );
    assert_eq!(events.loser_skipped.len(), 1);
    assert_eq!(
        events.loser_skipped[0].reason,
        rrcloud_core::engine::LoserSkipReason::KeyCollision
    );
    assert_eq!(
        events.conflicts.len(),
        1,
        "the conflict itself still resolves"
    );
}

/// Finding 3 (B3), gate half: a Dirty record carrying a STALE leftover
/// intent is still uncommitted dirt to the apply rule — the losing
/// concurrent remote triggers commit-then-case-4 with a freshly minted
/// admitted vv that is CONCURRENT with (never a false descendant of)
/// the remote branch.
#[test]
fn stale_intent_on_dirty_is_treated_as_uncommitted_dirt() {
    let (_d, root, db) = scratch(DEV_B);
    let image = rel(IMG);
    let key = sidecar_item_relkey(&image).expect("item key");
    let published = eh::doc(1, None, 0.0);
    let edited = eh::doc(4, Some("red"), 0.5);
    let mut record = rec(Kind::Sidecar, ItemState::Dirty);
    record.sem_hash = Some(sem_hash(&published).expect("sem"));
    record.blake3 = Some(Blake3Hex::from_bytes(&published));
    record.vv = vv(&[(DEV_A, 1)]);
    // The poison shape the demotion bug left behind: Dirty + intent.
    record.admitted_vv = Some(vv(&[(DEV_A, 1), (DEV_B, 1)]));
    record.admitted_ts = Some(50);
    db.replay_put_item(&key, &record).expect("seed");
    th::write_file(&item_local_path(root.path(), &key), &edited);

    // A concurrent remote that LOSES the pick (our now is newer).
    let remote = eh::doc(2, Some("blue"), 0.9);
    let entry = put_sidecar(DEV_A, &image, &remote, vv(&[(DEV_A, 2)]), 1_000);
    let mut events = RecordedEvents::default();
    let mut consumer = EngineConsumer::new(&db, root.path(), &mut events)
        .expect("consumer")
        .with_now(5_000);
    apply(&db, &mut consumer, &entry);

    let after = item(&db, &key);
    assert_eq!(after.state, ItemState::Queued, "dirt committed, not merged");
    let minted = after.admitted_vv.clone().expect("fresh intent");
    assert_eq!(minted, vv(&[(DEV_A, 1), (DEV_B, 1)]));
    assert_eq!(
        compare(&minted, &entry.vv),
        VvOrder::Concurrent,
        "the minted version is concurrent with the remote branch — never \
         a false descendant of it (the pre-fix shape destroyed the \
         remote's edit fleet-wide with no loser copy)"
    );
    assert_eq!(events.conflicts.len(), 1, "case 4 fired");
}

/// Finding 3 (B3), park half (Garage): an invalid-sidecar park —
/// `uploading → dirty` — WITHDRAWS the admission intent; re-admission
/// after the file heals mints a fresh version from the record's
/// published history, never from a stale leftover.
#[tokio::test]
async fn invalid_sidecar_park_withdraws_the_admission_intent() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("eng-park-intent");
    let s3 = CountingS3::new(g.client());
    let root = tempfile::tempdir().expect("root");
    let (_dbdir, _p, db) = open_db(&dev(DEV_B));
    let cfg = th::test_cfg(&bucket, root.path());

    let image = rel(IMG);
    let key = sidecar_item_relkey(&image).expect("item key");
    let d1 = eh::doc(2, None, 0.5);
    let path = item_local_path(root.path(), &key);
    th::write_file(&path, &d1);
    notify_local_change(
        &db,
        &image,
        Kind::Sidecar,
        &scan(&d1, th::mtime_unix_ns(&path)),
    )
    .expect("notify");
    admit_pending(&db, |_, _| true).expect("admit");
    assert!(item(&db, &key).admitted_vv.is_some());

    // The file turns invalid before the transfer reads it: the upload
    // parks the item dirty (§2.4 "upload abandoned").
    th::write_file(&path, b"not a sidecar");
    let err = upload_item(&db, &s3, &cfg, &key, &path)
        .await
        .expect_err("invalid sidecar");
    assert!(
        matches!(
            err,
            rrcloud_core::transfer::TransferError::SidecarInvalid { .. }
        ),
        "got {err:?}"
    );
    let parked = item(&db, &key);
    assert_eq!(parked.state, ItemState::Dirty);
    assert_eq!(
        parked.admitted_vv, None,
        "the park withdraws the intent (pre-fix: a Dirty record kept it, \
         read as committed to the apply rule, and the next admission \
         minted a false descendant of a merged-in concurrent branch)"
    );
    assert_eq!(parked.admitted_ts, None);

    // Heal the file; re-admission mints from the published history.
    th::write_file(&path, &d1);
    let admitted = admit_pending(&db, |_, _| true).expect("re-admit");
    assert_eq!(admitted, vec![key.clone()]);
    assert_eq!(
        item(&db, &key).admitted_vv,
        Some(vv(&[(DEV_B, 1)])),
        "fresh mint from the (empty) published vv"
    );
}

/// Finding 1 (B1), seam half (Garage): the published entry's `ts` is
/// the ADMISSION freeze even when a converged twin applied mid-flight
/// legitimately moved the record's `head_ts` to the twin's identity.
#[tokio::test]
async fn published_entry_ts_is_the_admission_freeze_not_the_converged_head_ts() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("eng-freeze-ts");
    let s3 = CountingS3::new(g.client());
    let root = tempfile::tempdir().expect("root");
    let (_dbdir, _p, db) = open_db(&dev(DEV_B));
    let cfg = th::test_cfg(&bucket, root.path());

    let image = rel(IMG);
    let key = sidecar_item_relkey(&image).expect("item key");
    let d2 = eh::doc(4, Some("red"), 0.5);
    let path = item_local_path(root.path(), &key);
    th::write_file(&path, &d2);
    notify_local_change(
        &db,
        &image,
        Kind::Sidecar,
        &scan(&d2, th::mtime_unix_ns(&path)),
    )
    .expect("notify");
    admit_pending(&db, |_, _| true).expect("admit");
    let freeze = item(&db, &key).admitted_ts.expect("admitted ts");

    // A converged twin (same content, concurrent vv, far-future ts)
    // applies while the upload is still queued: the record's head
    // identity converges to the twin's — but the entry we publish must
    // still carry OUR freeze.
    let twin = put_sidecar(DEV_A, &image, &d2, vv(&[(DEV_A, 7)]), freeze + 7_200);
    let mut events = RecordedEvents::default();
    let mut consumer = EngineConsumer::new(&db, root.path(), &mut events)
        .expect("consumer")
        .with_now(freeze + 1);
    apply(&db, &mut consumer, &twin);
    let converged = item(&db, &key);
    assert_eq!(
        converged.head_ts,
        Some(freeze + 7_200),
        "the converged head identity is the twin's (deterministic pick)"
    );
    assert_eq!(converged.state, ItemState::Queued, "upload still owed");
    assert_eq!(converged.admitted_ts, Some(freeze), "freeze intact");

    upload_item(&db, &s3, &cfg, &key, &path)
        .await
        .expect("upload");
    let staged = db.iter_outbound().expect("outbound");
    let entry = JournalEntry::from_json_line(
        std::str::from_utf8(&staged.last().expect("one staged").1).expect("utf8"),
    )
    .expect("decodes");
    assert_eq!(
        entry.ts, freeze,
        "the published ts is the admission freeze (pre-fix: the twin's \
         head_ts leaked into the entry — 'X's entry carried Y's ts')"
    );
    assert_eq!(
        entry.vv,
        vv(&[(DEV_B, 1)]),
        "and the admitted vv snapshot, untouched by the converge merge"
    );
}

/// Finding 9 (B9) build half: manifest rows carry the head version's
/// journal `ts`; rows advertised while an upload intent is in flight
/// omit `ts` AND `device` (the record's identity then names the
/// in-flight version, not the advertised published one).
#[test]
fn manifest_rows_carry_head_ts_and_omit_identity_while_in_flight() {
    let (_dbdir, _p, db) = open_db(&dev(DEV_A));
    let image = rel(IMG);
    let key = sidecar_item_relkey(&image).expect("item");
    let d1 = eh::doc(4, Some("blue"), 0.5);
    let mut record = rec(Kind::Sidecar, ItemState::Synced);
    record.sem_hash = Some(sem_hash(&d1).expect("sem"));
    record.blake3 = Some(Blake3Hex::from_bytes(&d1));
    record.size = d1.len() as u64;
    record.vv = vv(&[(DEV_A, 1)]);
    record.device = Some(dev(DEV_A));
    record.head_ts = Some(1_000);
    db.insert_item(&key, &record).expect("seed");

    let manifest = build_manifest(&db, 1_769_950_000).expect("build");
    assert_eq!(manifest.rows.len(), 1);
    assert_eq!(manifest.rows[0].ts, Some(1_000), "head ts travels");
    assert_eq!(manifest.rows[0].device, Some(dev(DEV_A)));

    // Re-admit: the in-flight row keeps the published vv/blake3 but
    // withholds ts/device — head identity now names the in-flight
    // version.
    let d2 = eh::doc(5, None, 0.9);
    notify_local_change(
        &db,
        &image,
        Kind::Sidecar,
        &LocalScan {
            size: d2.len() as u64,
            mtime_unix_ns: 10,
            bytes: &d2,
        },
    )
    .expect("notify");
    admit_pending(&db, |_, _| true).expect("admit");
    let manifest = build_manifest(&db, 1_769_950_100).expect("build");
    assert_eq!(manifest.rows.len(), 1);
    assert_eq!(manifest.rows[0].vv, vv(&[(DEV_A, 1)]), "published vv");
    assert_eq!(manifest.rows[0].ts, None, "in-flight: ts withheld");
    assert_eq!(manifest.rows[0].device, None, "in-flight: device withheld");
}

// ===========================================================================
// Review round 1 — probe-verified regressions, pinned at unit scale
// (the end-to-end interleavings live in tests/engine_scenarios.rs s17–s20)
// ===========================================================================

/// Round 1 blockers 1+3: `resurrect_original` meeting an original whose
/// upload intent is IN FLIGHT must stage NOTHING — the in-flight put's
/// admitted vv carries this device's unpublished self component, so it
/// is concurrent with the del and resurrects the item by itself
/// (edits-beat-deletes on every side). Pre-fix the re-advertisement of
/// the OLD blake3 minted the same self component the upload publishes
/// (probe-verified fleet divergence) or, folding the snapshot, strictly
/// dominated the in-flight NEW version (probe-verified edit loss +
/// CorruptRemote poisoning). Both del arrival orders are pinned.
#[test]
fn resurrection_skips_the_original_while_its_upload_intent_is_in_flight() {
    let v1 = th::patterned(2048, 7);
    let v2 = th::patterned(2048, 8);
    for order in ["sidecar_first", "original_first"] {
        let (_d, root, db) = scratch(DEV_B);
        let image = rel(IMG);
        let key = sidecar_item_relkey(&image).expect("sidecar item");

        // Sidecar: published {A:1}, locally dirty (held back by §3.7).
        let published = eh::doc(1, None, 0.0);
        let edited = eh::doc(5, Some("red"), 0.9);
        let mut sidecar = rec(Kind::Sidecar, ItemState::Dirty);
        sidecar.sem_hash = Some(sem_hash(&published).expect("sem"));
        sidecar.blake3 = Some(Blake3Hex::from_bytes(&published));
        sidecar.vv = vv(&[(DEV_A, 1)]);
        db.insert_item(&key, &sidecar).expect("seed sidecar");
        th::write_file(&item_local_path(root.path(), &key), &edited);

        // Original: v1 published at {A:1}; overwritten out of band (v2)
        // and ADMITTED — Queued with the intent snapshot {A:1,B:1}, the
        // record still advertising v1's blake3 (§2.6 coordination note)
        // while content_id tracks the local v2 bytes.
        let mut original = rec(Kind::Original, ItemState::Dirty);
        original.blake3 = Some(Blake3Hex::from_bytes(&v1));
        original.content_id = Some(ContentId::from_bytes(&v2));
        original.size = v1.len() as u64;
        original.vv = vv(&[(DEV_A, 1)]);
        db.insert_item(&image, &original).expect("seed original");
        let admitted = vv(&[(DEV_A, 1), (DEV_B, 1)]);
        let snapshot = admitted.clone();
        db.transition(&image, ItemState::Dirty, ItemState::Queued, |r| {
            r.admitted_vv = Some(snapshot.clone());
            r.admitted_ts = Some(900);
            r.head_ts = Some(900);
            r.device = Some(dev(DEV_B));
        })
        .expect("admit original");
        db.queue_push(Queue::Up, &image, 2).expect("queue original");

        let del_sidecar = del(
            DEV_A,
            Kind::Sidecar,
            sidecar_key(&image),
            vv(&[(DEV_A, 2)]),
            700,
        );
        let del_original = del(
            DEV_A,
            Kind::Original,
            library_key(&image),
            vv(&[(DEV_A, 2)]),
            700,
        );
        let mut events = RecordedEvents::default();
        let mut consumer = EngineConsumer::new(&db, root.path(), &mut events)
            .expect("consumer")
            .with_now(1_500);
        match order {
            "sidecar_first" => {
                apply(&db, &mut consumer, &del_sidecar);
                apply(&db, &mut consumer, &del_original);
            }
            _ => {
                apply(&db, &mut consumer, &del_original);
                apply(&db, &mut consumer, &del_sidecar);
            }
        }

        // The original stays live with the INTENT INTACT: the in-flight
        // upload is the resurrection. No second B component was minted.
        let after = item(&db, &image);
        assert!(!after.deleted, "{order}: live via edits-beat-deletes");
        assert_eq!(after.state, ItemState::Queued, "{order}: upload owed");
        assert_eq!(
            after.admitted_vv,
            Some(admitted.clone()),
            "{order}: the intent stands untouched"
        );
        assert_eq!(after.admitted_ts, Some(900), "{order}");
        assert_eq!(
            after.vv,
            vv(&[(DEV_A, 2)]),
            "{order}: the del folded into the published vv (absorbing arm)"
        );
        // NOTHING was staged for the original: pre-fix a metadata put
        // re-advertised v1's blake3 under a vv reusing (or dominating)
        // the in-flight B:1 component.
        for (_, bytes) in db.iter_outbound().expect("outbound") {
            let entry = JournalEntry::from_json_line(std::str::from_utf8(&bytes).expect("utf8"))
                .expect("decodes");
            assert_ne!(
                entry.key,
                library_key(&image),
                "{order}: no original re-advertisement while in flight"
            );
        }
        // The sidecar still resurrected normally.
        let sidecar_after = item(&db, &key);
        assert!(!sidecar_after.deleted, "{order}");
        assert_eq!(sidecar_after.state, ItemState::Queued, "{order}");
        assert_eq!(
            compare(
                sidecar_after.admitted_vv.as_ref().expect("admitted"),
                &vv(&[(DEV_A, 2)])
            ),
            VvOrder::Greater,
            "{order}: the sidecar resurrection dominates the del"
        );
        // The original's upload queue row survived (its del never
        // dominated the in-flight head).
        assert_eq!(
            drain_queue(&db, Queue::Up),
            vec![image.as_str().to_string(), key.as_str().to_string()],
            "{order}"
        );
        assert!(
            events.resurrection_incomplete.is_empty(),
            "{order}: the in-flight upload covers the original"
        );
    }
}

/// Round 1 blocker 2: a Concurrent entry whose content matches the
/// local FILE (uncommitted dirt) collapses the dirt ONLY when it wins
/// the §2.6 (ts, device) pick against the committed base — the same
/// pick every other device runs on that pair. A file-equal entry that
/// LOSES the pick must commit-then-fold instead (pre-fix it silently
/// became the primary: probe-verified permanent fleet divergence under
/// identical vvs, with zero events).
#[test]
fn concurrent_file_equal_entry_collapses_dirt_only_when_it_wins_the_pick() {
    let image = rel(IMG);
    let base_doc = eh::doc(3, None, 0.25); // the committed base (v1)
    let edited = eh::doc(5, Some("red"), 0.9); // the dirt AND the twin entry

    let seed = |db: &SyncDb, root: &Path| -> RelKey {
        let key = sidecar_item_relkey(&image).expect("item key");
        let mut record = rec(Kind::Sidecar, ItemState::Dirty);
        record.sem_hash = Some(sem_hash(&base_doc).expect("sem"));
        record.blake3 = Some(Blake3Hex::from_bytes(&base_doc));
        record.vv = vv(&[(DEV_A, 1)]);
        record.device = Some(dev(DEV_A));
        record.head_ts = Some(1_000); // the base version's identity
        db.insert_item(&key, &record).expect("seed");
        th::write_file(&item_local_path(root, &key), &edited);
        key
    };

    // Entry WINS the pick (ts 5_000 > base 1_000): the collapse is the
    // fleet's own resolution — adopt, no version minted, no conflict.
    {
        let (_d, root, db) = scratch(DEV_X);
        let key = seed(&db, root.path());
        let entry = put_sidecar(DEV_B, &image, &edited, vv(&[(DEV_B, 1)]), 5_000);
        let mut events = RecordedEvents::default();
        let mut consumer = EngineConsumer::new(&db, root.path(), &mut events)
            .expect("consumer")
            .with_now(9_000);
        apply(&db, &mut consumer, &entry);
        let after = item(&db, &key);
        assert_eq!(after.state, ItemState::Synced, "dirt collapsed");
        assert_eq!(after.vv, vv(&[(DEV_A, 1), (DEV_B, 1)]));
        assert_eq!(after.sem_hash, Some(sem_hash(&edited).expect("sem")));
        assert_eq!(after.head_ts, Some(5_000));
        assert_eq!(after.device, Some(dev(DEV_B)));
        assert_eq!(after.admitted_vv, None, "no version minted");
        assert_eq!(db.queue_len(Queue::Up).expect("len"), 0);
        assert!(events.conflicts.is_empty(), "twin convergence, no event");
    }

    // Entry LOSES the pick (ts 500 < base 1_000) — the probe's shape:
    // collapsing would park this device on the branch the rest of the
    // fleet resolves as the LOSER, under the identical folded vv. The
    // dirt commits as a local version (it IS a newer edit over our
    // base) and the entry folds as a content twin of the committed head.
    {
        let (_d, root, db) = scratch(DEV_X);
        let key = seed(&db, root.path());
        let entry = put_sidecar(DEV_B, &image, &edited, vv(&[(DEV_B, 1)]), 500);
        let mut events = RecordedEvents::default();
        let mut consumer = EngineConsumer::new(&db, root.path(), &mut events)
            .expect("consumer")
            .with_now(9_000);
        apply(&db, &mut consumer, &entry);
        let after = item(&db, &key);
        assert_eq!(
            after.state,
            ItemState::Queued,
            "pre-fix: Synced — the losing branch silently became primary"
        );
        assert_eq!(
            after.admitted_vv,
            Some(vv(&[(DEV_A, 1), (DEV_X, 1)])),
            "the dirt committed from the base, concurrent with the entry"
        );
        assert_eq!(
            after.vv,
            vv(&[(DEV_A, 1), (DEV_B, 1)]),
            "the entry still folded (content twin of the committed head)"
        );
        assert_eq!(after.head_ts, Some(9_000), "the commit's identity");
        assert_eq!(after.device, Some(dev(DEV_X)));
        assert_eq!(
            after.sem_hash,
            Some(sem_hash(&base_doc).expect("sem")),
            "record fields keep naming the last PUBLISHED version"
        );
        assert_eq!(
            drain_queue(&db, Queue::Up),
            vec![key.as_str().to_string()],
            "the committed version is bound for upload"
        );
        assert!(
            events.conflicts.is_empty(),
            "content-equal concurrency is convergence, not a conflict"
        );
    }
}

/// Round 1 major 4: a strictly dominating (Greater) content-equal entry
/// IS the newer version — a record whose state holds no local bytes
/// (PendingDown here) must adopt its blake3/size, or the pending fetch
/// verifies churn-divergent bytes against the superseded hash and
/// condemns a healthy remote (probe-verified CorruptRemote wedge). A
/// HOLDING record keeps local bytes authoritative, exactly like the
/// pinned Concurrent twin case.
#[test]
fn greater_converge_adopts_content_identity_for_non_holding_records() {
    let image = rel(IMG);
    let d1 = eh::doc(3, Some("red"), 0.25);
    let churned = eh::churned(&d1); // same sem, different bytes

    // Non-holding (PendingDown): adopt blake3/size, clear the stale
    // integrity flags (they described the superseded blake3).
    {
        let (_d, root, db) = scratch(DEV_C);
        let key = sidecar_item_relkey(&image).expect("item key");
        let mut record = rec(Kind::Sidecar, ItemState::PendingDown);
        record.sem_hash = Some(sem_hash(&d1).expect("sem"));
        record.blake3 = Some(Blake3Hex::from_bytes(&d1));
        record.size = d1.len() as u64;
        record.vv = vv(&[(DEV_A, 1)]);
        record.device = Some(dev(DEV_A));
        record.head_ts = Some(100);
        record.verified_remote = true;
        record.attested = true;
        db.insert_item(&key, &record).expect("seed");
        db.queue_push(Queue::Down, &key, 1).expect("queued fetch");

        let entry = put_sidecar(DEV_B, &image, &churned, vv(&[(DEV_A, 1), (DEV_B, 1)]), 200);
        let mut events = RecordedEvents::default();
        let mut consumer = EngineConsumer::new(&db, root.path(), &mut events)
            .expect("consumer")
            .with_now(500);
        apply(&db, &mut consumer, &entry);

        let after = item(&db, &key);
        assert_eq!(after.state, ItemState::PendingDown);
        assert_eq!(after.vv, vv(&[(DEV_A, 1), (DEV_B, 1)]));
        assert_eq!(
            after.blake3,
            Some(Blake3Hex::from_bytes(&churned)),
            "pre-fix: still v1's blake3 — the fetch then wedged CorruptRemote"
        );
        assert_eq!(after.size, churned.len() as u64);
        assert!(!after.verified_remote, "facts described the old blake3");
        assert!(!after.attested);
        assert_eq!(after.head_ts, Some(200), "identity adopts (Greater)");
        assert_eq!(after.device, Some(dev(DEV_B)));
        assert!(events.conflicts.is_empty());
    }

    // Holding (Synced): local bytes stay authoritative.
    {
        let (_d, root, db) = scratch(DEV_C);
        let key = seed_synced_sidecar(&db, root.path(), &image, &d1, DEV_A, vv(&[(DEV_A, 1)]), 100);
        let entry = put_sidecar(DEV_B, &image, &churned, vv(&[(DEV_A, 1), (DEV_B, 1)]), 200);
        let mut events = RecordedEvents::default();
        let mut consumer = EngineConsumer::new(&db, root.path(), &mut events)
            .expect("consumer")
            .with_now(500);
        apply(&db, &mut consumer, &entry);
        let after = item(&db, &key);
        assert_eq!(after.state, ItemState::Synced, "no transfer");
        assert_eq!(
            after.blake3,
            Some(Blake3Hex::from_bytes(&d1)),
            "a holder keeps its own (sem-equal) bytes authoritative"
        );
        assert_eq!(after.vv, vv(&[(DEV_A, 1), (DEV_B, 1)]));
    }
}

/// Round 1 major 6 (receiver half): a Greater content-equal entry
/// arriving at a CorruptRemote record is the §2.4 "repair landed
/// elsewhere" edge — the condemned advertisement is superseded, so the
/// item re-enters the fetch lane instead of staying wedged off-queue.
#[test]
fn greater_converge_rescues_a_corrupt_remote_record_into_the_fetch_lane() {
    let (_d, root, db) = scratch(DEV_C);
    let image = rel(IMG);
    let key = sidecar_item_relkey(&image).expect("item key");
    let d1 = eh::doc(3, Some("red"), 0.25);
    let mut record = rec(Kind::Sidecar, ItemState::PendingDown);
    record.sem_hash = Some(sem_hash(&d1).expect("sem"));
    record.blake3 = Some(Blake3Hex::from_bytes(&d1));
    record.size = d1.len() as u64;
    record.vv = vv(&[(DEV_A, 1), (DEV_B, 1)]);
    record.device = Some(dev(DEV_B));
    record.head_ts = Some(200);
    db.insert_item(&key, &record).expect("seed");
    db.transition(&key, ItemState::PendingDown, ItemState::Downloading, |_| {})
        .expect("slot");
    db.transition(
        &key,
        ItemState::Downloading,
        ItemState::CorruptRemote,
        |_| {},
    )
    .expect("condemned");

    // The author re-publishes the same content as a fresh dominating
    // version (the §1.2 shared-key repair): rescue + re-fetch.
    let churned = eh::churned(&d1);
    let entry = put_sidecar(DEV_B, &image, &churned, vv(&[(DEV_A, 1), (DEV_B, 2)]), 900);
    let mut events = RecordedEvents::default();
    let mut consumer = EngineConsumer::new(&db, root.path(), &mut events)
        .expect("consumer")
        .with_now(1_000);
    apply(&db, &mut consumer, &entry);

    let after = item(&db, &key);
    assert_eq!(
        after.state,
        ItemState::PendingDown,
        "pre-fix: CorruptRemote forever (converge kept the state and the \
         item had no queue row left)"
    );
    assert_eq!(after.blake3, Some(Blake3Hex::from_bytes(&churned)));
    assert_eq!(after.vv, vv(&[(DEV_A, 1), (DEV_B, 2)]));
    assert_eq!(
        drain_queue(&db, Queue::Down),
        vec![key.as_str().to_string()],
        "re-queued for the fetch"
    );
    assert!(events.conflicts.is_empty());
}

/// Round 1 major 6 (author half): resolving a conflict whose REMOTE
/// branch loses re-marks the item Dirty on exactly the device that
/// AUTHORED the winning head and quiescently holds its bytes — the next
/// admission re-publishes the winner as a fresh dominating version, so
/// the §1.2 shared bucket key ends holding winner bytes whatever order
/// the two concurrent uploads landed in (probe-verified: a loser upload
/// landing last poisoned the key and wedged every fetcher). Non-authors
/// and in-flight heads are left alone (an in-flight upload re-PUTs by
/// itself).
#[test]
fn remote_loser_resolution_renudges_only_the_quiescent_winning_author() {
    let image = rel(IMG);
    let ours = eh::doc(3, Some("red"), 0.25);
    let theirs = eh::doc(5, Some("green"), 0.75);

    // (a) We authored the winning head and sit Synced: re-nudged Dirty.
    {
        let (_d, root, db) = scratch(DEV_A);
        let key = seed_synced_sidecar(
            &db,
            root.path(),
            &image,
            &ours,
            DEV_A,
            vv(&[(DEV_A, 2)]),
            5_000,
        );
        let entry = put_sidecar(DEV_B, &image, &theirs, vv(&[(DEV_A, 1), (DEV_B, 1)]), 2_000);
        let mut events = RecordedEvents::default();
        let mut consumer = EngineConsumer::new(&db, root.path(), &mut events)
            .expect("consumer")
            .with_now(6_000);
        apply(&db, &mut consumer, &entry);
        let after = item(&db, &key);
        assert_eq!(
            after.state,
            ItemState::Dirty,
            "pre-fix: Synced and oblivious — nothing in the unit ever \
             re-uploaded the winner over a loser-last bucket key"
        );
        assert_eq!(after.vv, vv(&[(DEV_A, 2), (DEV_B, 1)]), "still folds");
        assert_eq!(after.sem_hash, Some(sem_hash(&ours).expect("sem")));
        assert_eq!(after.blake3, Some(Blake3Hex::from_bytes(&ours)));
        assert_eq!(after.head_ts, Some(5_000), "identity kept");
        assert_eq!(after.device, Some(dev(DEV_A)));
        assert_eq!(after.admitted_vv, None, "admission happens later (§3.7)");
        assert_eq!(db.queue_len(Queue::Up).expect("len"), 0);
        assert_eq!(events.conflicts.len(), 1);
        assert_eq!(events.conflicts[0].copy_relkey, None);
    }

    // (b) We merely HOLD the winner (authored elsewhere): no nudge —
    // only the author re-publishes, or N holders would mint N versions.
    {
        let (_d, root, db) = scratch(DEV_C);
        let key = seed_synced_sidecar(
            &db,
            root.path(),
            &image,
            &ours,
            DEV_A,
            vv(&[(DEV_A, 2)]),
            5_000,
        );
        let entry = put_sidecar(DEV_B, &image, &theirs, vv(&[(DEV_A, 1), (DEV_B, 1)]), 2_000);
        let mut events = RecordedEvents::default();
        let mut consumer = EngineConsumer::new(&db, root.path(), &mut events)
            .expect("consumer")
            .with_now(6_000);
        apply(&db, &mut consumer, &entry);
        assert_eq!(item(&db, &key).state, ItemState::Synced, "holder stays");
    }

    // (c) The winning head is committed IN FLIGHT: its own upload PUTs
    // the bytes after the fold, repairing the key — no nudge.
    {
        let (_d, root, db) = scratch(DEV_A);
        let key = sidecar_item_relkey(&image).expect("item key");
        let mut record = rec(Kind::Sidecar, ItemState::Dirty);
        record.sem_hash = Some(sem_hash(&ours).expect("sem"));
        record.vv = vv(&[(DEV_A, 2)]);
        db.insert_item(&key, &record).expect("seed");
        th::write_file(&item_local_path(root.path(), &key), &ours);
        db.transition(&key, ItemState::Dirty, ItemState::Queued, |r| {
            r.admitted_vv = Some(vv(&[(DEV_A, 3)]));
            r.admitted_ts = Some(5_000);
            r.head_ts = Some(5_000);
            r.device = Some(dev(DEV_A));
        })
        .expect("admit");
        db.queue_push(Queue::Up, &key, 1).expect("queue");
        let entry = put_sidecar(DEV_B, &image, &theirs, vv(&[(DEV_A, 1), (DEV_B, 1)]), 2_000);
        let mut events = RecordedEvents::default();
        let mut consumer = EngineConsumer::new(&db, root.path(), &mut events)
            .expect("consumer")
            .with_now(6_000);
        apply(&db, &mut consumer, &entry);
        let after = item(&db, &key);
        assert_eq!(after.state, ItemState::Queued, "in-flight head untouched");
        assert_eq!(after.admitted_vv, Some(vv(&[(DEV_A, 3)])));
    }
}

/// Round 1 major 5: the loser-vc `(key, sem_hash)` apply dedup meeting
/// a DELETED record at the vc key is not "already preserved" — the new
/// conflict supersedes the vc's deletion, so the vc is RESTORED (un-hidden
/// with a dominating metadata put, row cleared), never silently left in
/// Recently Deleted while the ConflictEvent claims a live copy
/// (probe-verified: the round-2 losing version was unreachable through
/// live state on every device).
#[test]
fn deleted_vc_at_the_dedup_key_is_restored_not_conflated_with_a_live_copy() {
    let (_d, root, db) = scratch(DEV_A);
    let image = rel(IMG);
    let key = seed_synced_sidecar(
        &db,
        root.path(),
        &image,
        &eh::doc(3, Some("red"), 0.25),
        DEV_A,
        vv(&[(DEV_A, 2)]),
        1_000,
    );
    let our_doc = eh::doc(3, Some("red"), 0.25);

    // The vc for our document's sem already exists — materialized by a
    // ROUND-1 conflict and then deleted by the user: hidden, with its
    // deletion row standing.
    let canonical = eh::semantic(&our_doc);
    let suffix = loser_vc_suffix(&our_doc).expect("suffix");
    let vc_rel = vc_item_relkey(&image, &suffix).expect("vc relkey");
    let mut vc = rec(Kind::Sidecar, ItemState::Synced);
    vc.sem_hash = Some(sem_hash(&canonical).expect("sem"));
    vc.blake3 = Some(Blake3Hex::from_bytes(&canonical));
    vc.size = canonical.len() as u64;
    vc.vv = vv(&[(DEV_A, 4)]);
    vc.device = Some(dev(DEV_A));
    vc.head_ts = Some(2_000);
    vc.deleted = true;
    db.insert_item(&vc_rel, &vc).expect("seed vc");
    db.with_txn(|t| {
        t.record_deleted(
            &vc_rel,
            &DeletedRecord {
                vv: vv(&[(DEV_A, 4)]),
                server_ts: 2_500,
            },
        )
    })
    .expect("seed row");

    // Round-2 conflict: a remote branch wins against our head; we hold
    // the loser (our_doc) and its deterministic vc key is the deleted one.
    let winner = eh::doc(5, Some("green"), 0.9);
    let entry = put_sidecar(DEV_B, &image, &winner, vv(&[(DEV_A, 1), (DEV_B, 1)]), 9_000);
    let mut events = RecordedEvents::default();
    let mut consumer = EngineConsumer::new(&db, root.path(), &mut events)
        .expect("consumer")
        .with_now(9_500);
    apply(&db, &mut consumer, &entry);

    // The vc is LIVE again, past its deletion.
    let restored = item(&db, &vc_rel);
    assert!(
        !restored.deleted,
        "pre-fix: still hidden while the event claimed a copy"
    );
    assert_eq!(restored.vv, vv(&[(DEV_A, 5)]), "record ∪ row, bumped");
    assert_eq!(restored.head_ts, Some(9_500));
    assert_eq!(restored.device, Some(dev(DEV_A)));
    assert_eq!(
        db.get_deleted(&vc_rel).expect("row"),
        None,
        "the deletion row is superseded"
    );
    // The restore was JOURNALED (dominating metadata put re-advertising
    // the vc's published identity), so every device un-hides it.
    let staged: Vec<JournalEntry> = db
        .iter_outbound()
        .expect("outbound")
        .into_iter()
        .map(|(_, bytes)| {
            JournalEntry::from_json_line(std::str::from_utf8(&bytes).expect("utf8"))
                .expect("decodes")
        })
        .collect();
    let vc_put = staged
        .iter()
        .find(|e| e.op == Op::Put && e.key == library_key(&vc_rel))
        .expect("pre-fix: nothing staged — outbound unchanged");
    assert_eq!(vc_put.vv, vv(&[(DEV_A, 5)]));
    assert_eq!(vc_put.blake3, Some(Blake3Hex::from_bytes(&canonical)));
    assert_eq!(
        compare(&vc_put.vv, &vv(&[(DEV_A, 4)])),
        VvOrder::Greater,
        "dominates the deletion lineage"
    );
    // The local copy healed and the event names a LIVE copy honestly.
    assert_eq!(
        eh::read_file(&item_local_path(root.path(), &vc_rel)),
        canonical
    );
    assert_eq!(events.conflicts.len(), 1);
    assert_eq!(events.conflicts[0].copy_relkey, Some(vc_rel));
    assert!(events.loser_skipped.is_empty(), "nothing was skipped");
    // And the primary adopted the round-2 winner as usual.
    assert_eq!(item(&db, &key).state, ItemState::PendingDown);
    assert_eq!(
        item(&db, &key).sem_hash,
        Some(sem_hash(&winner).expect("sem"))
    );
}

/// Round 1 minor 10: when the dels apply in [Original, Sidecar] order,
/// the original's dominating del removed its Down queue row; the
/// sidecar-driven resurrection un-hides the PendingDown original and
/// must re-push that row (mirroring restore_item's lane normalization)
/// — pre-fix the item was pump-invisible until the next startup sweep.
#[test]
fn resurrection_repushes_the_down_row_of_a_hidden_pending_down_original() {
    let (_d, root, db) = scratch(DEV_B);
    let image = rel(IMG);
    let key = sidecar_item_relkey(&image).expect("sidecar item");
    let published = eh::doc(1, None, 0.0);
    let edited = eh::doc(5, Some("red"), 0.9);
    let mut sidecar = rec(Kind::Sidecar, ItemState::Dirty);
    sidecar.sem_hash = Some(sem_hash(&published).expect("sem"));
    sidecar.vv = vv(&[(DEV_A, 1)]);
    db.insert_item(&key, &sidecar).expect("seed sidecar");
    th::write_file(&item_local_path(root.path(), &key), &edited);

    let orig_bytes = th::patterned(2048, 7);
    let mut original = rec(Kind::Original, ItemState::PendingDown);
    original.blake3 = Some(Blake3Hex::from_bytes(&orig_bytes));
    original.content_id = Some(ContentId::from_bytes(&orig_bytes));
    original.size = orig_bytes.len() as u64;
    original.vv = vv(&[(DEV_A, 1)]);
    db.insert_item(&image, &original).expect("seed original");
    db.queue_push(Queue::Down, &image, 2)
        .expect("pending fetch");

    let del_original = del(
        DEV_A,
        Kind::Original,
        library_key(&image),
        vv(&[(DEV_A, 2)]),
        700,
    );
    let del_sidecar = del(
        DEV_A,
        Kind::Sidecar,
        sidecar_key(&image),
        vv(&[(DEV_A, 2)]),
        700,
    );
    let mut events = RecordedEvents::default();
    let mut consumer = EngineConsumer::new(&db, root.path(), &mut events)
        .expect("consumer")
        .with_now(1_500);
    apply(&db, &mut consumer, &del_original);
    assert!(item(&db, &image).deleted, "dominating del hid the original");
    assert_eq!(
        db.queue_len(Queue::Down).expect("len"),
        0,
        "and dequeued its fetch"
    );
    apply(&db, &mut consumer, &del_sidecar);

    let after = item(&db, &image);
    assert!(!after.deleted, "resurrected");
    assert_eq!(after.state, ItemState::PendingDown);
    assert_eq!(
        drain_queue(&db, Queue::Down),
        vec![image.as_str().to_string()],
        "pre-fix: empty — un-hidden but pump-invisible until restart"
    );
    assert!(events.resurrection_incomplete.is_empty());
}

/// Round 1 minor 7: the converged-dirt collapse re-points the record at
/// the entry's blake3, so integrity facts earned for the superseded
/// version must clear — exactly as adopt_remote and commit_verified do
/// on their re-points (the §3.5 eviction gate reads both flags).
#[test]
fn converged_dirt_collapse_clears_stale_integrity_flags() {
    let (_d, root, db) = scratch(DEV_A);
    let image = rel(IMG);
    let key = sidecar_item_relkey(&image).expect("item key");
    let published = eh::doc(1, None, 0.0);
    let edited = eh::doc(4, Some("red"), 0.5);
    let mut record = rec(Kind::Sidecar, ItemState::Dirty);
    record.sem_hash = Some(sem_hash(&published).expect("sem"));
    record.blake3 = Some(Blake3Hex::from_bytes(&published));
    record.vv = vv(&[(DEV_A, 1)]);
    record.verified_remote = true;
    record.attested = true;
    db.insert_item(&key, &record).expect("seed");
    th::write_file(&item_local_path(root.path(), &key), &edited);

    let mut events = RecordedEvents::default();
    let mut consumer = EngineConsumer::new(&db, root.path(), &mut events)
        .expect("consumer")
        .with_now(900);
    apply(
        &db,
        &mut consumer,
        &put_sidecar(DEV_B, &image, &edited, vv(&[(DEV_A, 1), (DEV_B, 1)]), 300),
    );

    let after = item(&db, &key);
    assert_eq!(after.state, ItemState::Synced);
    assert_eq!(after.blake3, Some(Blake3Hex::from_bytes(&edited)));
    assert!(
        !after.verified_remote,
        "upload-side verify was never performed for the adopted blake3"
    );
    assert!(!after.attested, "no attest entry covers the adopted blake3");
}
