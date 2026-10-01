//! Failing tests for `rrcloud_core::engine` (architecture §2.5–§2.8):
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
use common::sync::{dev, open_db, rel, DEV_A, DEV_B, DEV_C};
use common::transfer as th;
use common::transfer::CountingS3;
use rrcloud_core::clock::{compare, VersionVector, VvOrder};
use rrcloud_core::engine::{
    admit_pending, delete_item, item_local_path, item_relkey_for, loser_vc_suffix,
    notify_local_change, original_conflict_relkey, recently_deleted, restore_item,
    sidecar_item_relkey, vc_item_relkey, ChangeOutcome, EngineConsumer, EngineError, EnginePut,
    LocalScan,
};
use rrcloud_core::journal::{JournalEntry, Kind, Op, Tombstone, JOURNAL_VERSION};
use rrcloud_core::keys::{
    classify_key, library_key, local_path, sidecar_key, tombstone_key, vc_sidecar_key, KeyClass,
    KeyError, RelKey,
};
use rrcloud_core::manifest::build_manifest;
use rrcloud_core::reader::{ConsumerError, JournalConsumer};
use rrcloud_core::semhash::{canonical_json, sem_hash, sidecar_badges, Blake3Hex, ContentId};
use rrcloud_core::state::{ItemRecord, ItemState, Queue, SyncDb};
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
    // Control-plane and foreign keys have no item.
    for key in [
        ".rrcloud/v1/tombstones/0123456789abcdef0123456789abcdef.json".to_string(),
        "random/garbage".to_string(),
        "library/not\u{0301}nfc.NEF".to_string(),
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
    // Pinned derivation (§2.6): blake3(canonical loser document)[..6].
    let value: serde_json::Value = serde_json::from_slice(&d).expect("doc parses");
    let expected = blake3::hash(canonical_json(&value).as_bytes()).to_hex();
    assert_eq!(suffix, expected.as_str()[..6].to_string());
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
    // deterministic vc key with our exact bytes, admitted for upload
    // with a fresh single-component vv.
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
    assert_eq!(vc.sem_hash, Some(sem_hash(&ours).expect("sem")));
    assert_eq!(
        eh::read_file(&item_local_path(root.path(), &vc_key)),
        ours,
        "loser bytes written to the vc file"
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
    assert_eq!(record.state, ItemState::Synced, "our version stays primary");
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
    assert_eq!(after.vv, vv(&[(DEV_A, 2), (DEV_B, 1)]));
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
    assert_eq!(eh::read_file(&item_local_path(root.path(), &vc_rel)), ours);
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
    let deleted = db
        .get_deleted(&image)
        .expect("get_deleted")
        .expect("recorded");
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
        &image,
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
        db.get_deleted(&image).expect("get_deleted"),
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
        "library/not\u{0301}nfc.NEF".to_string(),
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
    assert_eq!(
        db.get_deleted(&image)
            .expect("get_deleted")
            .expect("row")
            .vv,
        vv(&[(DEV_A, 3), (DEV_B, 1)])
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
    assert_eq!(manifest.deleted.len(), 1);
    assert_eq!(manifest.deleted[0].del, image);
    assert_eq!(manifest.deleted[0].vv, vv(&[(DEV_A, 3), (DEV_B, 1)]));

    // Restore: metadata-only dominating puts, flags cleared, row gone.
    let restored = restore_item(&db, &image).expect("restore_item");
    assert_eq!(restored.len(), 2);
    for key in [&sidecar_rel, &image] {
        let record = item(&db, key);
        assert!(!record.deleted, "{key} restored");
        assert_eq!(
            compare(&record.vv, &vv(&[(DEV_A, 3), (DEV_B, 1)])),
            VvOrder::Greater,
            "restore dominates the deletion for {key}"
        );
    }
    assert_eq!(db.get_deleted(&image).expect("get_deleted"), None);
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
        assert_eq!(
            compare(&put.vv, &tombstone.vv.clone()),
            VvOrder::Greater,
            "restore put {} dominates the tombstone",
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
