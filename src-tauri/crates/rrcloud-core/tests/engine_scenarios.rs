//! The P1 acceptance scenarios (architecture §2.11), Garage-backed, two
//! or three `SyncDb` "devices" inside one test process, driving
//! publish / poll / pump manually through the engine's synchronous-async
//! entry points: s1–s8 are the plan's original acceptance suite, and
//! s9–s11, s13–s20 pin review-round failure interleavings end to end
//! (s12 was never assigned — the review-round numbering jumped from s11
//! to s13; the gap is deliberate, nothing was removed). Every scenario
//! asserts by **state equivalence** (full item records or the
//! cross-device [`common::engine::SyncView`] projection, vv maps,
//! deleted sets) and **bucket inspection** (key listings and object
//! bytes), never by smoke signals.
//!
//! Determinism: the §2.6 case-4 winner is `(ts, device)`-driven, and
//! entry `ts` is frozen at admission from the device's server-time
//! estimate — so scenarios steer the winner by planting a large
//! server-time offset on the device that must win
//! ([`SyncDb::set_server_time_offset_ms`]), and cross-check the pick
//! against [`pick_winner`] over the actually published entries.

mod common;

use common::engine as eh;
use common::engine::RecordedEvents;
use common::garage;
use common::sync::{dev, open_db, rel, DEV_A, DEV_B, DEV_C, DEV_X};
use common::transfer as th;
use common::transfer::CountingS3;
use rrcloud_core::clock::{compare, pick_winner, Candidate, DeviceId, VvOrder};
use rrcloud_core::engine::{
    admit_pending, delete_item, item_local_path, loser_vc_suffix, notify_local_change,
    original_conflict_relkey, recently_deleted, restore_item, sidecar_item_relkey, vc_item_relkey,
    ChangeOutcome, EngineConsumer, LocalScan,
};
use rrcloud_core::journal::{JournalEntry, Kind, Op, Tombstone};
use rrcloud_core::keys::{library_key, sidecar_key, tombstone_key, RelKey};
use rrcloud_core::manifest::{build_manifest, merge};
use rrcloud_core::publisher::publish_pending;
use rrcloud_core::reader::{ConsumerError, JournalConsumer as _};
use rrcloud_core::semhash::{sem_hash, Blake3Hex, ContentId};
use rrcloud_core::state::{ItemRecord, ItemState, Queue, SyncDb};
use rrcloud_core::transfer::{pump_downloads, CancelFlag, TransferConfig, TransferError};

/// One scenario device: its state db, sync root, counting S3 wrapper,
/// transfer config, and recorded engine events.
struct Device {
    db: SyncDb,
    _dbdir: tempfile::TempDir,
    root: tempfile::TempDir,
    s3: CountingS3,
    cfg: TransferConfig,
    events: RecordedEvents,
}

fn device(g: &'static common::garage::Garage, bucket: &str, id: &str) -> Device {
    let (_dbdir, _path, db) = open_db(&dev(id));
    let root = tempfile::tempdir().expect("sync root");
    let cfg = th::test_cfg(bucket, root.path());
    Device {
        db,
        _dbdir,
        root,
        s3: CountingS3::new(g.client()),
        cfg,
        events: RecordedEvents::default(),
    }
}

impl Device {
    /// Writes `bytes` as the local file of `item` and runs the §2.5
    /// intake for it.
    fn write_and_notify(&self, image: &RelKey, kind: Kind, bytes: &[u8]) -> ChangeOutcome {
        let item = match kind {
            Kind::Sidecar => sidecar_item_relkey(image).expect("sidecar item"),
            _ => image.clone(),
        };
        let path = item_local_path(self.root.path(), &item);
        th::write_file(&path, bytes);
        notify_local_change(
            &self.db,
            image,
            kind,
            &LocalScan {
                size: bytes.len() as u64,
                mtime_unix_ns: th::mtime_unix_ns(&path),
                bytes,
            },
        )
        .expect("notify_local_change")
    }

    /// Admit + pump uploads + publish.
    async fn sync_up(&self) {
        eh::sync_up(&self.db, &self.s3, &self.cfg).await;
    }

    /// One poll through a fresh [`EngineConsumer`].
    async fn poll_apply(&mut self) {
        eh::poll_apply(&self.db, &self.s3, &self.cfg, &mut self.events).await;
    }

    /// Pump downloads to completion.
    async fn pump_down(&self) {
        eh::pump_down(&self.db, &self.s3, &self.cfg).await;
    }

    /// The local bytes of `item`.
    fn file(&self, item: &RelKey) -> Vec<u8> {
        eh::read_file(&item_local_path(self.root.path(), item))
    }

    /// The item's record, which must exist.
    fn item(&self, key: &RelKey) -> ItemRecord {
        self.db
            .get_item(key)
            .expect("get_item")
            .unwrap_or_else(|| panic!("no record for {key}"))
    }

    /// How many times this device PUT object bytes at `key`.
    fn puts_to(&self, key: &str) -> usize {
        self.s3.put_keys().iter().filter(|k| *k == key).count()
    }
}

/// Seeds the standard photo on `a` (original + base sidecar), publishes
/// it, and pulls it onto every device in `rest`. Returns
/// `(original bytes, base sidecar doc)`.
async fn seed_photo(image: &RelKey, a: &Device, rest: &mut [&mut Device]) -> (Vec<u8>, Vec<u8>) {
    let orig = th::patterned(2048, 42);
    let base = eh::doc(0, None, 0.0);
    assert_eq!(
        a.write_and_notify(image, Kind::Original, &orig),
        ChangeOutcome::MarkedDirty
    );
    assert_eq!(
        a.write_and_notify(image, Kind::Sidecar, &base),
        ChangeOutcome::MarkedDirty
    );
    a.sync_up().await;
    for d in rest {
        d.poll_apply().await;
        d.pump_down().await;
        let sidecar = sidecar_item_relkey(image).expect("sidecar item");
        assert_eq!(d.file(image), orig, "original mirrored");
        assert_eq!(d.file(&sidecar), base, "sidecar mirrored");
        assert_eq!(d.item(image).state, ItemState::Hydrated);
        assert_eq!(d.item(&sidecar).state, ItemState::Synced);
    }
    (orig, base)
}

/// The two concurrent sidecar head entries (one per author) among the
/// published journals, as [`pick_winner`] candidates with their docs'
/// sem hashes.
fn concurrent_pair<'a>(
    entries_a: &'a [JournalEntry],
    entries_b: &'a [JournalEntry],
    image: &RelKey,
) -> (&'a JournalEntry, &'a JournalEntry) {
    let key = sidecar_key(image);
    let last_of = |entries: &'a [JournalEntry]| {
        entries
            .iter()
            .rfind(|e| e.op == Op::Put && e.key == key)
            .expect("author's sidecar head entry")
    };
    (last_of(entries_a), last_of(entries_b))
}

/// A fresh db + sync root for the same device id (order-equivalence
/// harness scaffolding).
fn fresh_dev(id: &str) -> (tempfile::TempDir, tempfile::TempDir, SyncDb) {
    let (d, _p, db) = open_db(&dev(id));
    (d, tempfile::tempdir().expect("root"), db)
}

/// Applies `entries` to `db` through a fresh engine consumer (manual
/// arrival-order driving for the order-equivalence assertions).
fn apply_manually(db: &SyncDb, root: &std::path::Path, entries: &[&JournalEntry]) {
    let mut events = RecordedEvents::default();
    let mut consumer = EngineConsumer::new(db, root, &mut events)
        .expect("consumer")
        .with_now(9_999_999);
    for entry in entries {
        db.with_txn_err::<_, ConsumerError>(|t| consumer.apply(t, entry))
            .expect("manual apply");
    }
}

// ===========================================================================
// S1 — concurrent offline sidecar edits
// ===========================================================================

#[tokio::test]
async fn s1_concurrent_offline_sidecar_edits_converge_with_one_loser_vc() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("s1-concurrent-edits");
    let client = g.client();
    let a = device(g, &bucket, DEV_A);
    let mut b = device(g, &bucket, DEV_B);
    let image = rel("p/IMG_0042.NEF");
    let sidecar_rel = sidecar_item_relkey(&image).expect("sidecar item");
    let (_orig, _base) = {
        let mut rest = [&mut b];
        seed_photo(&image, &a, &mut rest).await
    };

    // Offline edits on both sides; B must deterministically win the
    // §2.6 tiebreak (its admission ts is planted an hour ahead).
    let doc_a = eh::doc(5, Some("red"), 0.6);
    let doc_b = eh::doc(2, Some("blue"), 0.3);
    assert_eq!(
        a.write_and_notify(&image, Kind::Sidecar, &doc_a),
        ChangeOutcome::MarkedDirty
    );
    assert_eq!(
        b.write_and_notify(&image, Kind::Sidecar, &doc_b),
        ChangeOutcome::MarkedDirty
    );
    b.db.set_server_time_offset_ms(3_600_000).expect("offset");
    a.sync_up().await;
    b.sync_up().await;

    // Cross-check the deterministic winner against the published pair.
    let entries_a = eh::journal_entries_of(&client, &bucket, &dev(DEV_A)).await;
    let entries_b = eh::journal_entries_of(&client, &bucket, &dev(DEV_B)).await;
    let (head_a, head_b) = concurrent_pair(&entries_a, &entries_b, &image);
    assert_eq!(
        compare(&head_a.vv, &head_b.vv),
        VvOrder::Concurrent,
        "precondition"
    );
    let winner = pick_winner(
        Candidate {
            ts: head_a.ts,
            device: &head_a.device,
        },
        Candidate {
            ts: head_b.ts,
            device: &head_b.device,
        },
    );
    assert_eq!(
        *winner.device,
        dev(DEV_B),
        "the planted offset makes B the winner"
    );

    // Exchange: both converge, the loser materializes once per holder.
    let mut a = a;
    a.poll_apply().await;
    a.pump_down().await; // the winner's bytes become A's primary
    a.sync_up().await; // the materialized loser vc uploads + publishes
    b.poll_apply().await; // A's losing head: same resolution, no copy held
    b.poll_apply().await; // A's vc advertisement
    b.pump_down().await;

    // Both primaries hold the winner; ONE identical loser vc exists.
    let suffix = loser_vc_suffix(&doc_a).expect("loser suffix");
    let vc_rel = vc_item_relkey(&image, &suffix).expect("vc relkey");
    for d in [&a, &b] {
        assert_eq!(d.file(&sidecar_rel), doc_b, "winner is the primary");
        assert_eq!(
            d.file(&vc_rel),
            eh::semantic(&doc_a),
            "loser preserved as the vc (canonical semantic doc — review round 0)"
        );
    }
    let library = eh::library_keys(&client, &bucket).await;
    assert_eq!(
        library,
        vec![
            library_key(&image),
            library_key(&vc_rel),
            sidecar_key(&image),
        ],
        "exactly one loser vc key materialized"
    );
    assert_eq!(
        th::get_bytes(&client, &bucket, &library_key(&vc_rel)).await,
        eh::semantic(&doc_a)
    );
    assert_eq!(
        th::get_bytes(&client, &bucket, &sidecar_key(&image)).await,
        doc_b
    );

    // Exact state equivalence across the devices.
    assert_eq!(eh::sync_view(&a.db), eh::sync_view(&b.db));
    let primary = a.item(&sidecar_rel);
    assert_eq!(
        primary.vv,
        [(dev(DEV_A), 2), (dev(DEV_B), 1)].into_iter().collect()
    );
    assert_eq!(primary.sem_hash, Some(sem_hash(&doc_b).expect("sem")));
    let vc = a.item(&vc_rel);
    assert_eq!(vc.vv, [(dev(DEV_A), 1)].into_iter().collect());
    assert_eq!(vc.sem_hash, Some(sem_hash(&doc_a).expect("sem")));

    // Events: the loser holder names the copy; the winner author holds
    // no loser and names none.
    assert_eq!(a.events.conflicts.len(), 1);
    assert_eq!(a.events.conflicts[0].winner_device, dev(DEV_B));
    assert_eq!(a.events.conflicts[0].copy_relkey, Some(vc_rel.clone()));
    assert_eq!(b.events.conflicts.len(), 1);
    assert_eq!(b.events.conflicts[0].winner_device, dev(DEV_B));
    assert_eq!(b.events.conflicts[0].copy_relkey, None);

    // The §2.11 B1/B2 regression: a third device applying both arrival
    // orders over fresh dbs reaches the IDENTICAL end state.
    let entries_a = eh::journal_entries_of(&client, &bucket, &dev(DEV_A)).await;
    let entries_b = eh::journal_entries_of(&client, &bucket, &dev(DEV_B)).await;
    let a_first: Vec<&JournalEntry> = entries_a.iter().chain(entries_b.iter()).collect();
    let b_first: Vec<&JournalEntry> = entries_b.iter().chain(entries_a.iter()).collect();
    let (_d1, root1, db1) = {
        let (d, p, db) = open_db(&dev(DEV_C));
        let _ = p;
        (d, tempfile::tempdir().expect("root"), db)
    };
    apply_manually(&db1, root1.path(), &a_first);
    let (_d2, root2, db2) = {
        let (d, p, db) = open_db(&dev(DEV_C));
        let _ = p;
        (d, tempfile::tempdir().expect("root"), db)
    };
    apply_manually(&db2, root2.path(), &b_first);
    assert_eq!(
        eh::full_items(&db1),
        eh::full_items(&db2),
        "arrival order must not matter (full record equality)"
    );
    assert_eq!(
        db1.iter_deleted().expect("deleted"),
        db2.iter_deleted().expect("deleted")
    );
    // And a real third device polling the real bucket agrees with them.
    let mut c = device(g, &bucket, DEV_C);
    c.poll_apply().await;
    assert_eq!(eh::full_items(&c.db), eh::full_items(&db1));
    // C never held the loser: it converged without materializing.
    assert_eq!(c.events.conflicts.len(), 1);
    assert_eq!(c.events.conflicts[0].copy_relkey, None);
    assert_eq!(
        c.item(&sidecar_rel).vv,
        [(dev(DEV_A), 2), (dev(DEV_B), 1)].into_iter().collect()
    );
}

// ===========================================================================
// S2 — delete vs edit with the editor's original evicted
// ===========================================================================

#[tokio::test]
async fn s2_delete_vs_edit_with_evicted_original_resurrects_the_whole_item() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("s2-del-vs-edit");
    let client = g.client();
    let mut a = device(g, &bucket, DEV_A);
    let mut b = device(g, &bucket, DEV_B);
    let image = rel("p/IMG_0042.NEF");
    let sidecar_rel = sidecar_item_relkey(&image).expect("sidecar item");
    let (orig, _base) = {
        let mut rest = [&mut b];
        seed_photo(&image, &a, &mut rest).await
    };

    // B's original is evicted to a stub (P2 machinery, simulated here).
    b.db.transition(&image, ItemState::Hydrated, ItemState::Stub, |_| {})
        .expect("evict");
    std::fs::remove_file(item_local_path(b.root.path(), &image)).expect("drop local bytes");

    // B edits the sidecar offline (dirty, never admitted).
    let doc_b = eh::doc(4, Some("green"), 0.8);
    assert_eq!(
        b.write_and_notify(&image, Kind::Sidecar, &doc_b),
        ChangeOutcome::MarkedDirty
    );

    // A deletes the photo: tombstone + dels, data keys untouched.
    let outcome = delete_item(
        &a.db,
        &a.s3,
        &bucket,
        &image,
        &[Kind::Sidecar, Kind::Original],
    )
    .await
    .expect("delete_item");
    publish_pending(&a.db, &a.s3, &bucket)
        .await
        .expect("publish dels");
    assert!(a.item(&image).deleted && a.item(&sidecar_rel).deleted);

    // B polls: edits beat deletes — resurrection puts for BOTH keys.
    b.poll_apply().await;
    assert!(!b.item(&sidecar_rel).deleted, "sidecar resurrected");
    assert!(!b.item(&image).deleted, "original resurrected");
    assert_eq!(
        b.item(&image).state,
        ItemState::Stub,
        "still evicted, still whole"
    );
    assert!(
        b.events.resurrection_incomplete.is_empty(),
        "the original was known (blake3 on the stub record): full resurrection"
    );
    assert_eq!(
        compare(&b.item(&image).vv, &outcome.tombstone.vv),
        VvOrder::Greater,
        "the original's resurrection vv dominates the deletion"
    );
    b.sync_up().await;

    // The resurrection moved NO original bytes: B never PUT the
    // original's data key — in the whole scenario.
    assert_eq!(
        b.puts_to(&library_key(&image)),
        0,
        "zero original-kind data PUTs from the resurrecting device"
    );
    // Its journal does advertise the original again, with the known
    // identity, dominating the del.
    let entries_b = eh::journal_entries_of(&client, &bucket, &dev(DEV_B)).await;
    let orig_put = entries_b
        .iter()
        .find(|e| e.op == Op::Put && e.key == library_key(&image))
        .expect("original resurrection put published");
    assert_eq!(orig_put.content_id, Some(ContentId::from_bytes(&orig)));
    assert!(orig_put.blake3.is_some());
    let sidecar_put = entries_b
        .iter()
        .rfind(|e| e.op == Op::Put && e.key == sidecar_key(&image))
        .expect("sidecar resurrection put published");
    assert_eq!(
        compare(&sidecar_put.vv, &outcome.tombstone.vv),
        VvOrder::Greater
    );

    // A polls: the item reappears; Recently Deleted empties.
    a.poll_apply().await;
    assert!(!a.item(&image).deleted && !a.item(&sidecar_rel).deleted);
    assert!(recently_deleted(&a.db).expect("listing").is_empty());
    assert_eq!(
        a.db.get_deleted(&image).expect("row"),
        None,
        "deletion superseded"
    );
    a.pump_down().await; // B's sidecar edit
    assert_eq!(a.file(&sidecar_rel), doc_b);
    assert_eq!(
        a.item(&image).state,
        ItemState::Hydrated,
        "A still holds the original bytes; the converged put moved nothing"
    );

    // Data keys and tombstone are all still in the bucket (GC is §2.10).
    assert_eq!(
        th::get_bytes(&client, &bucket, &library_key(&image)).await,
        orig
    );
    let tomb: Tombstone =
        serde_json::from_slice(&th::get_bytes(&client, &bucket, &tombstone_key(&image)).await)
            .expect("tombstone still present and decodable");
    assert_eq!(tomb, outcome.tombstone);

    assert_eq!(eh::sync_view(&a.db), eh::sync_view(&b.db));
}

// ===========================================================================
// S3 — churn immunity
// ===========================================================================

#[tokio::test]
async fn s3_churn_rewrite_causes_zero_uploads_and_zero_version_bumps() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("s3-churn");
    let a = device(g, &bucket, DEV_A);
    let image = rel("p/IMG_0042.NEF");
    let sidecar_rel = sidecar_item_relkey(&image).expect("sidecar item");
    let doc = eh::doc(3, Some("red"), 0.25);
    assert_eq!(
        a.write_and_notify(&image, Kind::Sidecar, &doc),
        ChangeOutcome::MarkedDirty
    );
    a.sync_up().await;

    let record_before = a.item(&sidecar_rel);
    let puts_before = a.s3.put_keys().len();
    let seq_before = a.db.last_allocated_seq().expect("seq");

    // An EXIF-cache-style rewrite: same sem_hash, different bytes.
    let rewritten = eh::churned(&doc);
    assert_ne!(rewritten, doc);
    assert_eq!(
        a.write_and_notify(&image, Kind::Sidecar, &rewritten),
        ChangeOutcome::Unchanged
    );
    assert!(
        admit_pending(&a.db, |_, _| true).expect("admit").is_empty(),
        "nothing to admit"
    );
    a.sync_up().await; // a full no-op pass

    assert_eq!(a.item(&sidecar_rel), record_before, "record bit-identical");
    assert_eq!(
        a.s3.put_keys().len(),
        puts_before,
        "zero uploads (CountingS3)"
    );
    assert_eq!(
        a.db.last_allocated_seq().expect("seq"),
        seq_before,
        "zero journal growth, zero version bumps"
    );
    assert_eq!(a.db.outbound_len().expect("outbound"), 0);
    assert_eq!(a.db.queue_len(Queue::Up).expect("queue"), 0);
}

// ===========================================================================
// S4 — dominating remote meets uncommitted local dirt (case 2 -> 4)
// ===========================================================================

#[tokio::test]
async fn s4_uncommitted_dirty_commits_first_and_no_edit_bytes_are_lost() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("s4-dirty-vs-dominating");
    let client = g.client();
    let mut a = device(g, &bucket, DEV_A);
    let mut b = device(g, &bucket, DEV_B);
    let image = rel("p/IMG_0042.NEF");
    let sidecar_rel = sidecar_item_relkey(&image).expect("sidecar item");
    {
        let mut rest = [&mut b];
        seed_photo(&image, &a, &mut rest).await;
    }

    // A publishes a dominating second version while B sits on
    // uncommitted dirty edits of the first.
    let doc_a2 = eh::doc(5, Some("red"), 0.9);
    let doc_b2 = eh::doc(1, Some("blue"), 0.2);
    assert_eq!(
        a.write_and_notify(&image, Kind::Sidecar, &doc_a2),
        ChangeOutcome::MarkedDirty
    );
    a.sync_up().await;
    assert_eq!(
        b.write_and_notify(&image, Kind::Sidecar, &doc_b2),
        ChangeOutcome::MarkedDirty
    );
    // B's local commit must win the ensuing case 4 deterministically.
    b.db.set_server_time_offset_ms(7_200_000).expect("offset");

    b.poll_apply().await;
    let committed = b.item(&sidecar_rel);
    assert_eq!(
        committed.state,
        ItemState::Queued,
        "local dirt was committed through the admission path, not clobbered"
    );
    let admitted = committed.admitted_vv.clone().expect("admitted snapshot");
    assert_eq!(
        admitted,
        [(dev(DEV_A), 1), (dev(DEV_B), 1)].into_iter().collect(),
        "committed from the v1 base it was edited on"
    );
    assert_eq!(
        b.events.conflicts.len(),
        1,
        "case 4 fired after the local commit"
    );
    assert_eq!(b.events.conflicts[0].winner_device, dev(DEV_B));
    b.sync_up().await;

    // B's published head carries exactly the admitted vv — concurrent
    // with A's v2, never a fabricated descendant of it.
    let entries_b = eh::journal_entries_of(&client, &bucket, &dev(DEV_B)).await;
    let head_b = entries_b
        .iter()
        .rfind(|e| e.op == Op::Put && e.key == sidecar_key(&image))
        .expect("B's head");
    assert_eq!(head_b.vv, admitted);

    // A applies it: same winner, and A (holding the losing v2) is the
    // one that materializes the loser copy.
    a.poll_apply().await;
    a.pump_down().await;
    a.sync_up().await;
    b.poll_apply().await;
    b.pump_down().await;

    let suffix = loser_vc_suffix(&doc_a2).expect("suffix");
    let vc_rel = vc_item_relkey(&image, &suffix).expect("vc relkey");
    for d in [&a, &b] {
        assert_eq!(d.file(&sidecar_rel), doc_b2, "B's edit is the primary");
        assert_eq!(
            d.file(&vc_rel),
            eh::semantic(&doc_a2),
            "A's edit survives as the vc (canonical semantic doc)"
        );
    }
    assert_eq!(
        th::get_bytes(&client, &bucket, &library_key(&vc_rel)).await,
        eh::semantic(&doc_a2)
    );
    assert_eq!(eh::sync_view(&a.db), eh::sync_view(&b.db));
    assert_eq!(a.events.conflicts.len(), 1);
    assert_eq!(a.events.conflicts[0].copy_relkey, Some(vc_rel));
    let primary = a.item(&sidecar_rel);
    assert_eq!(
        primary.vv,
        [(dev(DEV_A), 2), (dev(DEV_B), 1)].into_iter().collect(),
        "both branches folded"
    );
}

// ===========================================================================
// S5 — del dominates; restore round-trips
// ===========================================================================

#[tokio::test]
async fn s5_dominating_del_hides_everywhere_and_restore_reappears() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("s5-del-dominates");
    let mut a = device(g, &bucket, DEV_A);
    let mut b = device(g, &bucket, DEV_B);
    let image = rel("p/IMG_0042.NEF");
    let sidecar_rel = sidecar_item_relkey(&image).expect("sidecar item");
    {
        let mut rest = [&mut b];
        seed_photo(&image, &a, &mut rest).await;
    }

    // B's edit is published and APPLIED by A before A deletes: the del
    // therefore dominates B's version.
    let doc_b2 = eh::doc(2, Some("blue"), 0.4);
    assert_eq!(
        b.write_and_notify(&image, Kind::Sidecar, &doc_b2),
        ChangeOutcome::MarkedDirty
    );
    b.sync_up().await;
    a.poll_apply().await;
    a.pump_down().await;
    assert_eq!(a.file(&sidecar_rel), doc_b2);

    let outcome = delete_item(
        &a.db,
        &a.s3,
        &bucket,
        &image,
        &[Kind::Sidecar, Kind::Original],
    )
    .await
    .expect("delete");
    assert_eq!(
        compare(
            &outcome.tombstone.vv,
            &[(dev(DEV_A), 1), (dev(DEV_B), 1)].into_iter().collect()
        ),
        VvOrder::Greater,
        "the del is a strictly newer version than B's edit"
    );
    publish_pending(&a.db, &a.s3, &bucket)
        .await
        .expect("publish");

    // B polls: clean (not dirty), dominated — hidden, not resurrected.
    b.poll_apply().await;
    assert!(b.item(&sidecar_rel).deleted && b.item(&image).deleted);
    // Each item's deletion vv (per-lineage): what the restore must beat.
    let sidecar_del_vv = b.item(&sidecar_rel).vv.clone();
    let original_del_vv = b.item(&image).vv.clone();
    assert_eq!(sidecar_del_vv, outcome.tombstone.vv, "sidecar del anchor");
    assert_eq!(
        recently_deleted(&b.db)
            .expect("listing")
            .iter()
            .map(|(k, _)| k.as_str().to_string())
            .collect::<Vec<_>>(),
        vec![image.as_str().to_string(), sidecar_rel.as_str().to_string()]
    );
    assert!(b.events.resurrection_incomplete.is_empty());
    assert!(
        b.db.outbound_len().expect("outbound") == 0,
        "a dominated receiver emits nothing"
    );

    // Restore on B: metadata-only dominating puts; reappears on A.
    let restored = restore_item(&b.db, &image).expect("restore");
    assert_eq!(restored.len(), 2);
    assert!(!b.item(&sidecar_rel).deleted && !b.item(&image).deleted);
    publish_pending(&b.db, &b.s3, &bucket)
        .await
        .expect("publish restore");

    a.poll_apply().await;
    assert!(!a.item(&sidecar_rel).deleted && !a.item(&image).deleted);
    assert!(recently_deleted(&a.db).expect("listing").is_empty());
    assert_eq!(a.db.get_deleted(&image).expect("row"), None);
    assert_eq!(a.db.get_deleted(&sidecar_rel).expect("row"), None);
    for (key, del_vv) in [(&sidecar_rel, &sidecar_del_vv), (&image, &original_del_vv)] {
        assert_eq!(
            compare(&a.item(key).vv, del_vv),
            VvOrder::Greater,
            "{key}: restored past its own deletion"
        );
    }
    assert_eq!(eh::sync_view(&a.db), eh::sync_view(&b.db));
    // Restore moved no bytes anywhere: B PUT no data keys after its
    // original seed-phase downloads (it only ever uploaded its edit).
    assert_eq!(b.puts_to(&library_key(&image)), 0);
    assert_eq!(
        b.puts_to(&sidecar_key(&image)),
        1,
        "exactly the earlier edit upload"
    );
}

// ===========================================================================
// S6 — concurrent original overwrite (§2.8)
// ===========================================================================

#[tokio::test]
async fn s6_original_overwrite_conflict_preserves_displaced_bytes_once() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("s6-original-overwrite");
    let client = g.client();
    let mut a = device(g, &bucket, DEV_A);
    let mut b = device(g, &bucket, DEV_B);
    let image = rel("p/IMG_0042.NEF");

    // Seed the original only.
    let orig = th::patterned(4096, 42);
    assert_eq!(
        a.write_and_notify(&image, Kind::Original, &orig),
        ChangeOutcome::MarkedDirty
    );
    a.sync_up().await;
    b.poll_apply().await;
    b.pump_down().await;
    assert_eq!(b.file(&image), orig);

    // Both replace the original offline with different bytes; B wins.
    let bytes_a = th::patterned(4096, 11);
    let bytes_b = th::patterned(4096, 22);
    assert_eq!(
        a.write_and_notify(&image, Kind::Original, &bytes_a),
        ChangeOutcome::MarkedDirty
    );
    assert_eq!(
        b.write_and_notify(&image, Kind::Original, &bytes_b),
        ChangeOutcome::MarkedDirty
    );
    b.db.set_server_time_offset_ms(10_800_000).expect("offset");
    a.sync_up().await;
    b.sync_up().await;

    // A applies B's winning overwrite: it holds the displaced bytes and
    // stages them at the deterministic conflict key.
    a.poll_apply().await;
    let displaced_cid = ContentId::from_bytes(&bytes_a);
    let conflict_rel = original_conflict_relkey(&image, &displaced_cid).expect("conflict relkey");
    assert_eq!(a.events.original_conflicts.len(), 1);
    assert_eq!(a.events.original_conflicts[0].relkey, image);
    assert_eq!(a.events.original_conflicts[0].conflict_relkey, conflict_rel);
    assert_eq!(
        a.events.original_conflicts[0].displaced_content_id,
        displaced_cid
    );
    a.pump_down().await; // adopt the winner's bytes lazily
    a.sync_up().await; // upload the displaced copy + publish its entry
    assert_eq!(a.file(&image), bytes_b, "winner adopted at the primary");
    assert_eq!(
        a.file(&conflict_rel),
        bytes_a,
        "displaced bytes preserved locally"
    );

    // B applies A's losing overwrite (no displaced bytes held there)
    // and then A's conflict-copy advertisement.
    b.poll_apply().await;
    assert!(
        b.events.original_conflicts.is_empty(),
        "the winner's author holds nothing displaced"
    );
    b.pump_down().await;
    assert_eq!(
        b.file(&conflict_rel),
        bytes_a,
        "the copy reaches the other device"
    );

    // Bucket truth: one content wins the library key; the displaced
    // bytes exist under exactly one deterministic conflict key.
    assert_eq!(
        eh::library_keys(&client, &bucket).await,
        vec![library_key(&image), library_key(&conflict_rel)]
    );
    assert_eq!(
        th::get_bytes(&client, &bucket, &library_key(&image)).await,
        bytes_b
    );
    assert_eq!(
        th::get_bytes(&client, &bucket, &library_key(&conflict_rel)).await,
        bytes_a
    );
    assert_eq!(a.puts_to(&library_key(&conflict_rel)), 1);

    // Idempotence: a second holder's byte-identical PUT to the same
    // deterministic key leaves a single unchanged object.
    client
        .put_object(
            &bucket,
            &library_key(&conflict_rel),
            bytes::Bytes::from(bytes_a.clone()),
            &rrcloud_core::s3::PutObjectOptions::default(),
        )
        .await
        .expect("replayed holder PUT");
    assert_eq!(
        eh::library_keys(&client, &bucket).await,
        vec![library_key(&image), library_key(&conflict_rel)],
        "still a single conflict object"
    );
    assert_eq!(
        th::get_bytes(&client, &bucket, &library_key(&conflict_rel)).await,
        bytes_a
    );

    // State equivalence: primary + conflict-copy items agree.
    assert_eq!(eh::sync_view(&a.db), eh::sync_view(&b.db));
    let primary = a.item(&image);
    assert_eq!(primary.content_id, Some(ContentId::from_bytes(&bytes_b)));
    assert_eq!(
        primary.vv,
        [(dev(DEV_A), 2), (dev(DEV_B), 1)].into_iter().collect()
    );
}

// ===========================================================================
// S7 — loser-vc materialization is idempotent across holders
// ===========================================================================

#[tokio::test]
async fn s7_two_holders_materialize_one_identical_loser_vc() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("s7-vc-idempotence");
    let client = g.client();
    let mut a = device(g, &bucket, DEV_A);
    let mut b = device(g, &bucket, DEV_B);
    let mut c = device(g, &bucket, DEV_C);
    let image = rel("p/IMG_0042.NEF");
    let sidecar_rel = sidecar_item_relkey(&image).expect("sidecar item");

    // v1 everywhere.
    let doc1 = eh::doc(0, None, 0.0);
    assert_eq!(
        a.write_and_notify(&image, Kind::Sidecar, &doc1),
        ChangeOutcome::MarkedDirty
    );
    a.sync_up().await;
    for d in [&mut b, &mut c] {
        d.poll_apply().await;
        d.pump_down().await;
    }

    // A publishes v2; C downloads it — A and C now both HOLD v2.
    let doc2 = eh::doc(3, Some("red"), 0.5);
    assert_eq!(
        a.write_and_notify(&image, Kind::Sidecar, &doc2),
        ChangeOutcome::MarkedDirty
    );
    a.sync_up().await;
    c.poll_apply().await;
    c.pump_down().await;
    assert_eq!(c.file(&sidecar_rel), doc2);

    // B edits concurrently from v1 and wins (planted offset): v2 loses.
    let doc3 = eh::doc(5, Some("blue"), 0.9);
    assert_eq!(
        b.write_and_notify(&image, Kind::Sidecar, &doc3),
        ChangeOutcome::MarkedDirty
    );
    b.db.set_server_time_offset_ms(14_400_000).expect("offset");
    b.sync_up().await;

    // Both v2 holders resolve the conflict and materialize the SAME vc.
    a.poll_apply().await;
    c.poll_apply().await;
    let suffix = loser_vc_suffix(&doc2).expect("suffix");
    let vc_rel = vc_item_relkey(&image, &suffix).expect("vc relkey");
    for d in [&a, &c] {
        assert_eq!(
            d.file(&vc_rel),
            eh::semantic(&doc2),
            "each holder wrote the identical (canonical) vc file"
        );
    }
    a.pump_down().await;
    a.sync_up().await;
    c.pump_down().await;
    c.sync_up().await;

    // Both holders uploaded byte-identical content to the one vc key.
    assert_eq!(a.puts_to(&library_key(&vc_rel)), 1, "A uploaded the vc");
    assert_eq!(c.puts_to(&library_key(&vc_rel)), 1, "C uploaded the vc");
    let library = eh::library_keys(&client, &bucket).await;
    assert_eq!(
        library,
        vec![library_key(&vc_rel), sidecar_key(&image)],
        "ONE vc key despite two materializers"
    );
    assert_eq!(
        th::get_bytes(&client, &bucket, &library_key(&vc_rel)).await,
        eh::semantic(&doc2)
    );

    // Cross-apply the two vc advertisements: apply dedup by
    // (key, sem_hash) folds them into ONE vc item whose vv is the union
    // of both fresh single-component vvs.
    a.poll_apply().await;
    c.poll_apply().await;
    b.poll_apply().await;
    b.poll_apply().await;
    b.pump_down().await;
    let expected_vc_vv: rrcloud_core::clock::VersionVector =
        [(dev(DEV_A), 1), (dev(DEV_C), 1)].into_iter().collect();
    for d in [&a, &b, &c] {
        let vc_records: Vec<(RelKey, ItemRecord)> =
            d.db.iter_items()
                .expect("items")
                .into_iter()
                .filter(|(k, _)| k.as_str().contains(&suffix))
                .collect();
        assert_eq!(vc_records.len(), 1, "exactly ONE vc item record");
        assert_eq!(vc_records[0].0, vc_rel);
        assert_eq!(vc_records[0].1.vv, expected_vc_vv);
        assert_eq!(
            vc_records[0].1.sem_hash,
            Some(sem_hash(&doc2).expect("sem"))
        );
    }
    assert_eq!(eh::sync_view(&a.db), eh::sync_view(&c.db));
    assert_eq!(eh::sync_view(&a.db), eh::sync_view(&b.db));
    assert_eq!(
        b.file(&vc_rel),
        eh::semantic(&doc2),
        "the vc reached the winner's author too"
    );
}

// ===========================================================================
// S8 — engine puts always carry blake3, end to end
// ===========================================================================

#[tokio::test]
async fn s8_every_published_engine_put_carries_blake3() {
    // The runtime half of S8 (the type-level unconstructibility pin
    // lives in tests/engine.rs): across a flow that exercises every
    // engine put lane — upload puts, loser-vc puts, metadata-only
    // restore puts — no published put entry is ever blake3-less.
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("s8-blake3-always");
    let client = g.client();
    let mut a = device(g, &bucket, DEV_A);
    let mut b = device(g, &bucket, DEV_B);
    let image = rel("p/IMG_0042.NEF");
    {
        let mut rest = [&mut b];
        seed_photo(&image, &a, &mut rest).await;
    }
    // Concurrent edits -> conflict -> vc materialization + upload.
    assert_eq!(
        a.write_and_notify(&image, Kind::Sidecar, &eh::doc(5, Some("red"), 0.6)),
        ChangeOutcome::MarkedDirty
    );
    assert_eq!(
        b.write_and_notify(&image, Kind::Sidecar, &eh::doc(2, Some("blue"), 0.3)),
        ChangeOutcome::MarkedDirty
    );
    b.db.set_server_time_offset_ms(3_600_000).expect("offset");
    a.sync_up().await;
    b.sync_up().await;
    a.poll_apply().await;
    a.pump_down().await;
    a.sync_up().await;
    b.poll_apply().await;
    // Delete + metadata-only restore (the EnginePut lane).
    delete_item(
        &a.db,
        &a.s3,
        &bucket,
        &image,
        &[Kind::Sidecar, Kind::Original],
    )
    .await
    .expect("delete");
    publish_pending(&a.db, &a.s3, &bucket)
        .await
        .expect("publish dels");
    restore_item(&a.db, &image).expect("restore");
    publish_pending(&a.db, &a.s3, &bucket)
        .await
        .expect("publish restores");

    let mut puts = 0usize;
    let mut dels = 0usize;
    for device_id in [DEV_A, DEV_B] {
        let device_id: DeviceId = dev(device_id);
        for entry in eh::journal_entries_of(&client, &bucket, &device_id).await {
            match entry.op {
                Op::Put => {
                    puts += 1;
                    assert!(
                        entry.blake3.is_some(),
                        "blake3-less engine put on the wire: {entry:?}"
                    );
                }
                Op::Del => dels += 1,
                _ => {}
            }
        }
    }
    assert!(
        puts >= 7,
        "the flow must have exercised seed (2) + edits (2) + vc (1) + restores (2) puts, saw {puts}"
    );
    assert_eq!(dels, 2, "the §2.7 delete staged both dels");
}

// ===========================================================================
// Review round 0 — the verified failure interleavings, pinned end to end
// ===========================================================================

/// Finding 1 (B1): a device polls while an admitted upload is in
/// flight, and the arriving entry is concurrent with the ADMITTED
/// version while matching the stale PUBLISHED sem (Y edited then
/// reverted). Pre-fix: X converged silently (0 conflict events) and the
/// fleet ended with identical vvs over different primaries. Now: case 4
/// fires, X's in-flight edit survives as the vc, and every arrival
/// order of the final entry set lands identically.
#[tokio::test]
async fn s9_in_flight_admission_window_conflicts_instead_of_converging() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("s9-inflight-window");
    let client = g.client();
    let mut a = device(g, &bucket, DEV_A); // X
    let mut b = device(g, &bucket, DEV_B); // Y
    let image = rel("p/IMG_0042.NEF");
    let sidecar_rel = sidecar_item_relkey(&image).expect("sidecar item");
    let (_orig, base) = {
        let mut rest = [&mut b];
        seed_photo(&image, &a, &mut rest).await
    };

    // Y edits then REVERTS (same sem as the published base), admits and
    // publishes with its clock two hours ahead: {A:1,B:1}, base sem.
    let detour = eh::doc(2, Some("blue"), 0.3);
    assert_eq!(
        b.write_and_notify(&image, Kind::Sidecar, &detour),
        ChangeOutcome::MarkedDirty
    );
    assert_eq!(
        b.write_and_notify(&image, Kind::Sidecar, &base),
        ChangeOutcome::Unchanged,
        "the revert collapses under the churn gate but the dirt stands"
    );
    b.db.set_server_time_offset_ms(7_200_000).expect("offset");
    b.sync_up().await;

    // X admits its own edit ({A:2}) and polls BEFORE pumping.
    let doc_x = eh::doc(5, Some("red"), 0.6);
    assert_eq!(
        a.write_and_notify(&image, Kind::Sidecar, &doc_x),
        ChangeOutcome::MarkedDirty
    );
    let admitted = eh::sync_up_admit_only(&a.db);
    assert_eq!(admitted, vec![sidecar_rel.clone()]);
    a.poll_apply().await;

    // Case 4 fired on X (pre-fix: zero events, silent divergence).
    assert_eq!(a.events.conflicts.len(), 1, "the window is a conflict");
    assert_eq!(a.events.conflicts[0].winner_device, dev(DEV_B));
    let suffix = loser_vc_suffix(&doc_x).expect("suffix");
    let vc_rel = vc_item_relkey(&image, &suffix).expect("vc relkey");
    assert_eq!(a.events.conflicts[0].copy_relkey, Some(vc_rel.clone()));

    a.pump_down().await; // fetch Y's (reverted) winner bytes
    a.sync_up().await; // upload + publish the vc
    b.poll_apply().await; // Y learns the vc
    b.pump_down().await;

    // Both primaries hold the winner; X's edit survives as the vc.
    for d in [&a, &b] {
        assert_eq!(d.file(&sidecar_rel), base, "winner is the reverted base");
        assert_eq!(d.file(&vc_rel), eh::semantic(&doc_x), "edit preserved");
    }
    assert_eq!(
        th::get_bytes(&client, &bucket, &library_key(&vc_rel)).await,
        eh::semantic(&doc_x)
    );
    assert_eq!(eh::sync_view(&a.db), eh::sync_view(&b.db));
    let primary = a.item(&sidecar_rel);
    assert_eq!(
        primary.vv,
        [(dev(DEV_A), 1), (dev(DEV_B), 1)].into_iter().collect(),
        "the fleet-learnable history; the withdrawn {{A:2}} never published"
    );
    assert_eq!(primary.sem_hash, Some(sem_hash(&base).expect("sem")));

    // §2.11: a fresh device applying both full arrival orders agrees.
    let entries_a = eh::journal_entries_of(&client, &bucket, &dev(DEV_A)).await;
    let entries_b = eh::journal_entries_of(&client, &bucket, &dev(DEV_B)).await;
    let a_first: Vec<&JournalEntry> = entries_a.iter().chain(entries_b.iter()).collect();
    let b_first: Vec<&JournalEntry> = entries_b.iter().chain(entries_a.iter()).collect();
    let (_d1, root1, db1) = fresh_dev(DEV_C);
    apply_manually(&db1, root1.path(), &a_first);
    let (_d2, root2, db2) = fresh_dev(DEV_C);
    apply_manually(&db2, root2.path(), &b_first);
    assert_eq!(eh::full_items(&db1), eh::full_items(&db2));
    assert_eq!(
        eh::sync_view(&db1),
        eh::sync_view(&a.db),
        "the fresh device agrees with the fleet"
    );
}

/// Finding 2/5 (B2/B5): a del genuinely CONCURRENT with a
/// committed/published edit. Pre-fix: no-op on the editor, half-applied
/// on the deleter, arrival-order divergent on third devices. Now: the
/// edit beats the delete on the edited key everywhere, the original's
/// own del stands everywhere (per-key ordering), and both arrival
/// orders agree bit for bit.
#[tokio::test]
async fn s10_del_concurrent_with_committed_edit_converges_in_every_order() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("s10-del-vs-edit");
    let client = g.client();
    let mut a = device(g, &bucket, DEV_A);
    let mut b = device(g, &bucket, DEV_B);
    let image = rel("p/IMG_0042.NEF");
    let sidecar_rel = sidecar_item_relkey(&image).expect("sidecar item");
    {
        let mut rest = [&mut b];
        seed_photo(&image, &a, &mut rest).await;
    }

    // B's edit is committed and published (clock ahead: it wins every
    // tiebreak) — but A has NOT polled it when A deletes.
    let doc_b2 = eh::doc(4, Some("green"), 0.8);
    assert_eq!(
        b.write_and_notify(&image, Kind::Sidecar, &doc_b2),
        ChangeOutcome::MarkedDirty
    );
    b.db.set_server_time_offset_ms(7_200_000).expect("offset");
    b.sync_up().await;

    delete_item(
        &a.db,
        &a.s3,
        &bucket,
        &image,
        &[Kind::Sidecar, Kind::Original],
    )
    .await
    .expect("delete");
    publish_pending(&a.db, &a.s3, &bucket)
        .await
        .expect("publish dels");

    // Cross-apply.
    a.poll_apply().await; // B's edit: concurrent with A's del -> edit wins
    a.pump_down().await;
    b.poll_apply().await; // A's dels: sidecar del loses, original del stands

    // The edited key is LIVE everywhere with B's edit; the original's
    // own deletion stands everywhere (per-key §2.6 ordering of dels).
    for d in [&a, &b] {
        let sidecar = d.item(&sidecar_rel);
        assert!(!sidecar.deleted, "edits beat deletes on the edited key");
        assert_eq!(sidecar.sem_hash, Some(sem_hash(&doc_b2).expect("sem")));
        assert!(d.item(&image).deleted, "the original's del stands");
        assert_eq!(
            d.db.get_deleted(&sidecar_rel).expect("row"),
            None,
            "no standing row for the key the edit won"
        );
        assert!(
            d.db.get_deleted(&image).expect("row").is_some(),
            "the original's row stands"
        );
    }
    assert_eq!(a.file(&sidecar_rel), doc_b2, "the edit's bytes landed on A");
    assert_eq!(eh::sync_view(&a.db), eh::sync_view(&b.db));
    assert!(
        a.events.conflicts.is_empty() && b.events.conflicts.is_empty(),
        "del-vs-edit is §2.7 ordering, not a §2.6 content conflict"
    );

    // §2.11: both arrival orders on a fresh device agree bit for bit
    // (pre-fix: deleted=true on del-first, deleted=false on put-first).
    let entries_a = eh::journal_entries_of(&client, &bucket, &dev(DEV_A)).await;
    let entries_b = eh::journal_entries_of(&client, &bucket, &dev(DEV_B)).await;
    let a_first: Vec<&JournalEntry> = entries_a.iter().chain(entries_b.iter()).collect();
    let b_first: Vec<&JournalEntry> = entries_b.iter().chain(entries_a.iter()).collect();
    let (_d1, root1, db1) = fresh_dev(DEV_C);
    apply_manually(&db1, root1.path(), &a_first);
    let (_d2, root2, db2) = fresh_dev(DEV_C);
    apply_manually(&db2, root2.path(), &b_first);
    assert_eq!(eh::full_items(&db1), eh::full_items(&db2));
    assert_eq!(
        db1.iter_deleted().expect("deleted"),
        db2.iter_deleted().expect("deleted")
    );
    assert!(
        !db1.get_item(&sidecar_rel)
            .expect("get")
            .expect("rec")
            .deleted
    );
    assert!(db1.get_item(&image).expect("get").expect("rec").deleted);

    // Restore the original on B; A agrees after a poll.
    restore_item(&b.db, &image).expect("restore");
    publish_pending(&b.db, &b.s3, &bucket)
        .await
        .expect("publish restore");
    a.poll_apply().await;
    for d in [&a, &b] {
        assert!(!d.item(&image).deleted, "whole item live again");
        assert!(recently_deleted(&d.db).expect("listing").is_empty());
    }
    assert_eq!(eh::sync_view(&a.db), eh::sync_view(&b.db));
}

/// Finding 7 (B6): edits-beat-deletes resurrection with ASYMMETRIC
/// per-item vvs — the original authored on A, the sidecar on B, deleted
/// by A while C holds uncommitted dirt. Pre-fix: the original stayed
/// hidden on the deleter forever (the resurrection vv is only
/// concurrent with the original's own del). Now the whole item is live
/// on every device.
#[tokio::test]
async fn s11_asymmetric_vvs_resurrect_the_whole_item_everywhere() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("s11-asymmetric-resurrection");
    let mut a = device(g, &bucket, DEV_A);
    let mut b = device(g, &bucket, DEV_B);
    let mut c = device(g, &bucket, DEV_C);
    let image = rel("p/IMG_0042.NEF");
    let sidecar_rel = sidecar_item_relkey(&image).expect("sidecar item");

    // A authors the original; B authors the first sidecar version —
    // the normal shape for an image imported on one device and edited
    // on another.
    let orig = th::patterned(2048, 42);
    assert_eq!(
        a.write_and_notify(&image, Kind::Original, &orig),
        ChangeOutcome::MarkedDirty
    );
    a.sync_up().await;
    b.poll_apply().await;
    b.pump_down().await;
    let base = eh::doc(1, None, 0.1);
    assert_eq!(
        b.write_and_notify(&image, Kind::Sidecar, &base),
        ChangeOutcome::MarkedDirty
    );
    b.sync_up().await;
    for d in [&mut a, &mut c] {
        d.poll_apply().await;
        d.pump_down().await;
    }
    assert_eq!(
        a.item(&sidecar_rel).vv,
        [(dev(DEV_B), 1)]
            .into_iter()
            .collect::<rrcloud_core::clock::VersionVector>(),
        "asymmetric precondition: sidecar lineage is B's, original is A's"
    );

    // C edits the sidecar offline (uncommitted dirt); A deletes both.
    let edited = eh::doc(5, Some("red"), 0.9);
    assert_eq!(
        c.write_and_notify(&image, Kind::Sidecar, &edited),
        ChangeOutcome::MarkedDirty
    );
    delete_item(
        &a.db,
        &a.s3,
        &bucket,
        &image,
        &[Kind::Sidecar, Kind::Original],
    )
    .await
    .expect("delete");
    publish_pending(&a.db, &a.s3, &bucket)
        .await
        .expect("publish dels");

    // C polls -> resurrection; its puts publish.
    c.poll_apply().await;
    assert!(!c.item(&sidecar_rel).deleted && !c.item(&image).deleted);
    assert!(c.events.resurrection_incomplete.is_empty());
    c.sync_up().await;

    // The deleter and the sidecar's author both converge to the whole
    // live item (pre-fix: A kept the original hidden forever).
    a.poll_apply().await;
    a.pump_down().await;
    b.poll_apply().await;
    b.pump_down().await;
    for (name, d) in [("A", &a), ("B", &b), ("C", &c)] {
        assert!(
            !d.item(&sidecar_rel).deleted && !d.item(&image).deleted,
            "{name}: the whole item is live"
        );
        assert!(
            recently_deleted(&d.db).expect("listing").is_empty(),
            "{name}: Recently Deleted is empty"
        );
    }
    assert_eq!(eh::sync_view(&a.db), eh::sync_view(&c.db));
    assert_eq!(eh::sync_view(&b.db), eh::sync_view(&c.db));
    assert_eq!(
        a.item(&sidecar_rel).sem_hash,
        Some(sem_hash(&edited).expect("sem")),
        "C's edit is the surviving head"
    );
}

/// Finding 10 (B8): delete -> restore of an item whose edit was admitted
/// but never pumped. Pre-fix: the item wedged at Queued with an empty
/// queue and the v2 edit became unreachable (later destroyed by the
/// next remote adopt). Now restore normalizes the lane to Dirty and the
/// next sync_up publishes the v2 edit.
#[tokio::test]
async fn s13_delete_restore_of_an_admitted_edit_still_publishes_it() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("s13-delete-restore");
    let a = device(g, &bucket, DEV_A);
    let mut b = device(g, &bucket, DEV_B);
    let image = rel("p/IMG_0042.NEF");
    let sidecar_rel = sidecar_item_relkey(&image).expect("sidecar item");
    {
        let mut rest = [&mut b];
        seed_photo(&image, &a, &mut rest).await;
    }

    // v2 admitted (Queued, intent minted) but NOT pumped.
    let v2 = eh::doc(5, Some("red"), 0.9);
    assert_eq!(
        a.write_and_notify(&image, Kind::Sidecar, &v2),
        ChangeOutcome::MarkedDirty
    );
    let admitted = eh::sync_up_admit_only(&a.db);
    assert_eq!(admitted, vec![sidecar_rel.clone()]);

    // Delete withdraws the intent; restore normalizes the lane.
    delete_item(
        &a.db,
        &a.s3,
        &bucket,
        &image,
        &[Kind::Sidecar, Kind::Original],
    )
    .await
    .expect("delete");
    restore_item(&a.db, &image).expect("restore");
    let restored = a.item(&sidecar_rel);
    assert!(!restored.deleted);
    assert_eq!(
        restored.state,
        ItemState::Dirty,
        "the stranded upload lane is normalized (pre-fix: Queued with an \
         empty queue — the v2 edit unreachable forever)"
    );

    // The next ordinary pass publishes the local file's content: v2.
    a.sync_up().await;
    b.poll_apply().await;
    b.pump_down().await;
    assert_eq!(b.file(&sidecar_rel), v2, "the v2 edit reached the fleet");
    assert_eq!(
        b.item(&sidecar_rel).sem_hash,
        Some(sem_hash(&v2).expect("sem"))
    );
    for d in [&a, &b] {
        assert!(!d.item(&sidecar_rel).deleted && !d.item(&image).deleted);
        assert!(recently_deleted(&d.db).expect("listing").is_empty());
    }
    assert_eq!(eh::sync_view(&a.db), eh::sync_view(&b.db));
}

/// Finding 4 (B4): a §2.10 laggard catching a deletion up via
/// manifest::merge — driven through the REAL EngineConsumer — hides the
/// original AND the sidecar, identically to journal replay, and a
/// fresh bootstrap ends equivalent too.
#[tokio::test]
async fn s14_manifest_deletion_catchup_equals_journal_replay() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("s14-manifest-deletion");
    let client = g.client();
    let a = device(g, &bucket, DEV_A);
    let mut laggard = device(g, &bucket, DEV_B); // catches up via manifest
    let mut replayer = device(g, &bucket, DEV_C); // catches up via journal
    let image = rel("p/IMG_0042.NEF");
    let sidecar_rel = sidecar_item_relkey(&image).expect("sidecar item");
    {
        let mut rest = [&mut laggard, &mut replayer];
        seed_photo(&image, &a, &mut rest).await;
    }

    // A deletes and publishes; its manifest carries the deleted set.
    delete_item(
        &a.db,
        &a.s3,
        &bucket,
        &image,
        &[Kind::Sidecar, Kind::Original],
    )
    .await
    .expect("delete");
    publish_pending(&a.db, &a.s3, &bucket)
        .await
        .expect("publish dels");
    let manifest = build_manifest(&a.db, 1_769_960_000).expect("manifest");
    assert_eq!(manifest.deleted.len(), 2, "one deleted row per item");

    // The laggard merges the manifest through the real EngineConsumer
    // (its §2.6 unified apply rule), never polling A's segments.
    {
        let mut consumer =
            EngineConsumer::new(&laggard.db, laggard.root.path(), &mut laggard.events)
                .expect("consumer");
        merge(
            &[(dev(DEV_A), manifest.clone())],
            &laggard.db,
            &mut consumer,
        )
        .expect("merge");
    }
    // The replayer polls the journal.
    replayer.poll_apply().await;

    // Both items are hidden on the laggard (pre-fix: only the sidecar;
    // the original stayed live and its next manifest advertised the
    // exact live/deleted contradiction build_manifest forbids).
    assert!(laggard.item(&sidecar_rel).deleted, "sidecar hidden");
    assert!(laggard.item(&image).deleted, "original hidden too");
    assert_eq!(
        eh::full_items(&laggard.db),
        eh::full_items(&replayer.db),
        "merge is the same idempotent apply as journal replay (§2.3)"
    );
    assert_eq!(
        laggard.db.iter_deleted().expect("deleted"),
        replayer.db.iter_deleted().expect("deleted")
    );

    // A fresh bootstrapping device (manifest only, then the journal):
    // the header's own-cursor attestation covers A's segments, so no
    // puts replay — the deleted items exist as ROWS only, and both rows
    // (original AND sidecar — pre-fix only the sidecar) match the
    // fleet's. A put that later sneaks in behind the rows (a laggard
    // writer re-advertising the deleted version) stays hidden.
    let mut boot = device(g, &bucket, DEV_X);
    {
        let mut consumer =
            EngineConsumer::new(&boot.db, boot.root.path(), &mut boot.events).expect("consumer");
        merge(&[(dev(DEV_A), manifest)], &boot.db, &mut consumer).expect("merge");
    }
    boot.poll_apply().await;
    assert!(
        boot.db.iter_items().expect("items").is_empty(),
        "nothing live to bootstrap: the deletion rows fold the puts"
    );
    assert_eq!(
        boot.db.iter_deleted().expect("deleted"),
        replayer.db.iter_deleted().expect("deleted"),
        "the full per-item deleted set reached the bootstrapper"
    );
    let entries_a = eh::journal_entries_of(&client, &bucket, &dev(DEV_A)).await;
    let seed_puts: Vec<&JournalEntry> = entries_a.iter().filter(|e| e.op == Op::Put).collect();
    apply_manually(&boot.db, boot.root.path(), &seed_puts.to_vec());
    assert!(
        boot.item(&sidecar_rel).deleted && boot.item(&image).deleted,
        "re-delivered puts stay hidden behind the per-item rows"
    );
}

/// Finding 9 (B9): a device that learns one branch of a true concurrent
/// pair via manifest merge resolves the §2.6 case-4 pick with the SAME
/// candidate as every journal replayer — the manifest row carries the
/// head version's real ts — and its downloads verify (no CorruptRemote
/// wedge).
#[tokio::test]
async fn s15_manifest_learned_heads_pick_the_same_case4_winner() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("s15-manifest-ts");
    let a = device(g, &bucket, DEV_A);
    let mut b = device(g, &bucket, DEV_B);
    let image = rel("p/IMG_0042.NEF");
    let sidecar_rel = sidecar_item_relkey(&image).expect("sidecar item");
    {
        let mut rest = [&mut b];
        seed_photo(&image, &a, &mut rest).await;
    }

    // True concurrent pair: A's v{A:2} (older ts), B's v{A:1,B:1}
    // (clock two hours ahead — wins every honest pick).
    let doc_a2 = eh::doc(2, Some("red"), 0.2);
    let doc_b2 = eh::doc(4, Some("blue"), 0.7);
    assert_eq!(
        a.write_and_notify(&image, Kind::Sidecar, &doc_a2),
        ChangeOutcome::MarkedDirty
    );
    a.sync_up().await;
    assert_eq!(
        b.write_and_notify(&image, Kind::Sidecar, &doc_b2),
        ChangeOutcome::MarkedDirty
    );
    b.db.set_server_time_offset_ms(7_200_000).expect("offset");
    b.sync_up().await;

    // The journal replayer picks B.
    let mut replayer = device(g, &bucket, DEV_C);
    replayer.poll_apply().await;
    assert_eq!(
        replayer.item(&sidecar_rel).sem_hash,
        Some(sem_hash(&doc_b2).expect("sem")),
        "journal pick: B wins"
    );

    // The merge-then-poll device learns A's branch from A's manifest —
    // written much later, so the pre-fix header-ts stamp would have
    // made A's branch unbeatable here — then B's branch from the
    // journal.
    let manifest = build_manifest(&a.db, 1_769_990_000).expect("manifest");
    let mut merged = device(g, &bucket, DEV_X);
    {
        let mut consumer = EngineConsumer::new(&merged.db, merged.root.path(), &mut merged.events)
            .expect("consumer");
        merge(&[(dev(DEV_A), manifest)], &merged.db, &mut consumer).expect("merge");
    }
    merged.poll_apply().await;
    assert_eq!(
        merged.item(&sidecar_rel).sem_hash,
        Some(sem_hash(&doc_b2).expect("sem")),
        "manifest-learned head loses to B exactly like everywhere else \
         (pre-fix: equal folded vvs over DIFFERENT primaries)"
    );
    assert_eq!(
        eh::sync_view(&merged.db),
        eh::sync_view(&replayer.db),
        "merge-then-poll converges with pure replay"
    );
    // And the fetch verifies — the record's blake3 names the bytes that
    // actually won the bucket key (pre-fix: IntegrityMismatch wedge).
    merged.pump_down().await;
    assert_eq!(merged.file(&sidecar_rel), doc_b2);
}

/// Finding M1/M3: two holders of one loser whose local files diverged
/// only by §2.5 churn materialize ONE vc — same key, byte-identical
/// canonical content — and the fleet converges on a single vc item
/// with one blake3.
#[tokio::test]
async fn s16_churn_divergent_holders_materialize_one_identical_vc() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("s16-churned-vc");
    let client = g.client();
    let mut a = device(g, &bucket, DEV_A);
    let mut b = device(g, &bucket, DEV_B);
    let mut c = device(g, &bucket, DEV_C);
    let image = rel("p/IMG_0042.NEF");

    // v1 everywhere; A publishes v2; C downloads it and then suffers a
    // §2.5 churn rewrite (same sem, different bytes) — A and C now hold
    // byte-DIVERGENT copies of the same version.
    let doc1 = eh::doc(0, None, 0.0);
    assert_eq!(
        a.write_and_notify(&image, Kind::Sidecar, &doc1),
        ChangeOutcome::MarkedDirty
    );
    a.sync_up().await;
    for d in [&mut b, &mut c] {
        d.poll_apply().await;
        d.pump_down().await;
    }
    let doc2 = eh::doc(3, Some("red"), 0.5);
    assert_eq!(
        a.write_and_notify(&image, Kind::Sidecar, &doc2),
        ChangeOutcome::MarkedDirty
    );
    a.sync_up().await;
    c.poll_apply().await;
    c.pump_down().await;
    let churned = eh::churned(&doc2);
    assert_eq!(
        c.write_and_notify(&image, Kind::Sidecar, &churned),
        ChangeOutcome::Unchanged,
        "churn: same sem, different bytes"
    );

    // B edits concurrently from v1 and wins: v2 loses on A and C.
    let doc3 = eh::doc(5, Some("blue"), 0.9);
    assert_eq!(
        b.write_and_notify(&image, Kind::Sidecar, &doc3),
        ChangeOutcome::MarkedDirty
    );
    b.db.set_server_time_offset_ms(14_400_000).expect("offset");
    b.sync_up().await;
    a.poll_apply().await;
    c.poll_apply().await;

    // ONE deterministic key — derived from the SEMANTIC form, so the
    // churn-divergent holders agree (pre-fix: two different keys, two
    // journal entries, duplicate vcs fleet-wide) — and byte-identical
    // canonical content on both.
    let suffix = loser_vc_suffix(&doc2).expect("suffix");
    assert_eq!(
        loser_vc_suffix(&churned).expect("churned suffix"),
        suffix,
        "churn-stable key"
    );
    let vc_rel = vc_item_relkey(&image, &suffix).expect("vc relkey");
    for d in [&a, &c] {
        assert_eq!(d.file(&vc_rel), eh::semantic(&doc2));
    }
    a.pump_down().await;
    a.sync_up().await;
    c.pump_down().await;
    c.sync_up().await;
    let library = eh::library_keys(&client, &bucket).await;
    assert_eq!(
        library,
        vec![library_key(&vc_rel), sidecar_key(&image)],
        "ONE vc key despite churn-divergent materializers"
    );
    assert_eq!(
        th::get_bytes(&client, &bucket, &library_key(&vc_rel)).await,
        eh::semantic(&doc2)
    );

    // Cross-apply the advertisements: one vc item, fully converged —
    // including blake3 (the canonical bytes hash identically).
    a.poll_apply().await;
    c.poll_apply().await;
    b.poll_apply().await;
    b.poll_apply().await;
    b.pump_down().await;
    for d in [&a, &b, &c] {
        let vc = d.item(&vc_rel);
        assert_eq!(vc.sem_hash, Some(sem_hash(&doc2).expect("sem")));
        assert_eq!(
            vc.vv,
            [(dev(DEV_A), 1), (dev(DEV_C), 1)]
                .into_iter()
                .collect::<rrcloud_core::clock::VersionVector>()
        );
    }
    assert_eq!(eh::sync_view(&a.db), eh::sync_view(&b.db));
    assert_eq!(eh::sync_view(&a.db), eh::sync_view(&c.db));
}

// ===========================================================================
// Review round 1 — the probe-verified interleavings, pinned end to end
// ===========================================================================

/// Round 1 blockers 1+3: a §2.7 resurrection firing while the ORIGINAL
/// has an admitted upload in flight. B overwrites the RAW out of band
/// and admits it (the actively-edited sidecar held back by §3.7); A
/// deletes the image. Pre-fix, B's resurrection re-advertised the OLD
/// blake3 under a vv that reused (and, published first, dominated) the
/// in-flight version's self component: the new RAW version was
/// unreachable fleet-wide while the bucket key held its bytes — every
/// fetcher of the winning head wedged CorruptRemote, and two devices
/// held different blake3s under one vv. Now the in-flight upload IS the
/// resurrection: one original put on the wire, the new content wins
/// everywhere, and a fresh device converges onto it.
#[tokio::test]
async fn s17_resurrection_with_an_in_flight_original_upload_keeps_the_new_version() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("s17-inflight-resurrection");
    let client = g.client();
    let mut a = device(g, &bucket, DEV_A);
    let mut b = device(g, &bucket, DEV_B);
    let image = rel("p/IMG_0042.NEF");
    let sidecar_rel = sidecar_item_relkey(&image).expect("sidecar item");
    let (_v1, _base) = {
        let mut rest = [&mut b];
        seed_photo(&image, &a, &mut rest).await
    };

    // B replaces the RAW out of band (v2) and keeps editing the sidecar.
    let v2 = th::patterned(2048, 77);
    assert_eq!(
        b.write_and_notify(&image, Kind::Original, &v2),
        ChangeOutcome::MarkedDirty
    );
    let doc_b = eh::doc(4, Some("green"), 0.8);
    assert_eq!(
        b.write_and_notify(&image, Kind::Sidecar, &doc_b),
        ChangeOutcome::MarkedDirty
    );
    // §3.7: the original quiesces and is admitted; the sidecar (open in
    // the editor) is held back — the probe's exact window: original
    // Queued with admitted_vv, upload not yet run.
    let admitted = admit_pending(&b.db, |k, _| *k == image).expect("admit");
    assert_eq!(admitted, vec![image.clone()]);
    let in_flight = b.item(&image).admitted_vv.clone().expect("intent");

    // A deletes the image and publishes the dels.
    delete_item(
        &a.db,
        &a.s3,
        &bucket,
        &image,
        &[Kind::Sidecar, Kind::Original],
    )
    .await
    .expect("delete");
    publish_pending(&a.db, &a.s3, &bucket)
        .await
        .expect("publish dels");

    // B polls: the sidecar's dirty resurrection fires, and the original
    // rides its in-flight intent instead of a stale re-advertisement.
    b.poll_apply().await;
    let orig_b = b.item(&image);
    assert!(!orig_b.deleted, "live via edits-beat-deletes");
    assert_eq!(orig_b.state, ItemState::Queued, "upload still owed");
    assert_eq!(
        orig_b.admitted_vv,
        Some(in_flight.clone()),
        "the in-flight intent stands untouched"
    );
    for (_, bytes) in b.db.iter_outbound().expect("outbound") {
        let entry = rrcloud_core::journal::JournalEntry::from_json_line(
            std::str::from_utf8(&bytes).expect("utf8"),
        )
        .expect("decodes");
        assert!(
            !(entry.op == Op::Put && entry.key == library_key(&image)),
            "no metadata re-advertisement of the original (pre-fix: a put \
             re-advertising v1's blake3 under a colliding/dominating vv)"
        );
    }
    assert!(b.events.resurrection_incomplete.is_empty());

    // B pumps + publishes: the v2 upload is the original's resurrection.
    b.sync_up().await;
    let entries_b = eh::journal_entries_of(&client, &bucket, &dev(DEV_B)).await;
    let orig_puts: Vec<&JournalEntry> = entries_b
        .iter()
        .filter(|e| e.op == Op::Put && e.key == library_key(&image))
        .collect();
    assert_eq!(
        orig_puts.len(),
        1,
        "exactly ONE original put on B's wire (pre-fix: two — the stale \
         re-advertisement and the upload — sharing B's self component)"
    );
    assert_eq!(orig_puts[0].vv, in_flight);
    assert_eq!(orig_puts[0].blake3, Some(Blake3Hex::from_bytes(&v2)));
    assert_eq!(orig_puts[0].content_id, Some(ContentId::from_bytes(&v2)));

    // The deleter converges onto the new content and un-hides.
    a.poll_apply().await;
    a.pump_down().await;
    assert!(!a.item(&image).deleted && !a.item(&sidecar_rel).deleted);
    assert!(recently_deleted(&a.db).expect("listing").is_empty());
    assert_eq!(a.file(&image), v2, "the NEW original version won");
    assert_eq!(a.file(&sidecar_rel), doc_b);
    assert_eq!(a.item(&image).blake3, Some(Blake3Hex::from_bytes(&v2)));

    // A fresh device converges to the same head and FETCHES it intact —
    // the probe's divergence ('vv equal, blake3 different') and the
    // CorruptRemote poisoning are both gone.
    let mut c = device(g, &bucket, DEV_C);
    c.poll_apply().await;
    c.pump_down().await;
    assert_eq!(c.file(&image), v2);
    assert_eq!(eh::sync_view(&a.db), eh::sync_view(&b.db));
    assert_eq!(eh::sync_view(&a.db), eh::sync_view(&c.db));
    assert_eq!(
        th::get_bytes(&client, &bucket, &library_key(&image)).await,
        v2,
        "the bucket key's bytes match the fleet-winning head"
    );
}

/// Round 1 blocker 2: the same semantic edit authored independently on
/// a laggard with a slow clock. X holds uncommitted dirt (rating 5 over
/// v1) when B's identical-content {B:1} arrives with ts < v1.ts —
/// concurrent with X's base but file-sem-equal. Pre-fix X silently
/// adopted the branch that loses the fleet's §2.6 pick: A and B ended
/// on v1 under {A:1,B:1} while X held B's content under the IDENTICAL
/// vv — an unhealable divergence with zero events. Now X commits its
/// dirt (a genuinely newer edit over v1) and the whole fleet converges
/// on it, with v1 preserved as a vc.
#[tokio::test]
async fn s18_laggard_twin_of_uncommitted_dirt_commits_instead_of_adopting_the_loser() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("s18-laggard-twin");
    let client = g.client();
    let mut a = device(g, &bucket, DEV_A);
    let mut b = device(g, &bucket, DEV_B);
    let mut x = device(g, &bucket, DEV_X);
    let image = rel("p/IMG_0042.NEF");
    let sidecar_rel = sidecar_item_relkey(&image).expect("sidecar item");

    // A publishes v1; X applies it. B never does (the laggard).
    let v1 = eh::doc(3, Some("red"), 0.25);
    assert_eq!(
        a.write_and_notify(&image, Kind::Sidecar, &v1),
        ChangeOutcome::MarkedDirty
    );
    a.sync_up().await;
    x.poll_apply().await;
    x.pump_down().await;
    assert_eq!(x.file(&sidecar_rel), v1);

    // The same rating-5 edit on both: X as dirt over v1, B from nothing
    // with its clock an hour BEHIND (its ts loses to v1's).
    let r5 = eh::doc(5, Some("red"), 0.25);
    assert_eq!(
        x.write_and_notify(&image, Kind::Sidecar, &r5),
        ChangeOutcome::MarkedDirty
    );
    assert_eq!(
        b.write_and_notify(&image, Kind::Sidecar, &r5),
        ChangeOutcome::MarkedDirty
    );
    b.db.set_server_time_offset_ms(-3_600_000).expect("offset");
    b.sync_up().await;

    // X applies B's {B:1}: concurrent with X's base {A:1}, file-equal,
    // and it LOSES the (ts, device) pick against v1 — so the dirt
    // commits instead of collapsing onto the losing branch.
    x.poll_apply().await;
    let committed = x.item(&sidecar_rel);
    assert_eq!(
        committed.state,
        ItemState::Queued,
        "pre-fix: Synced — X silently adopted the fleet-losing branch"
    );
    assert_eq!(
        committed.admitted_vv,
        Some([(dev(DEV_A), 1), (dev(DEV_X), 1)].into_iter().collect()),
        "committed from the v1 base"
    );
    assert_eq!(
        committed.vv,
        [(dev(DEV_A), 1), (dev(DEV_B), 1)]
            .into_iter()
            .collect::<rrcloud_core::clock::VersionVector>(),
        "B's twin branch still folded"
    );
    assert!(
        x.events.conflicts.is_empty(),
        "content-equal concurrency is convergence, not a conflict"
    );
    x.sync_up().await;

    // Full exchange: everyone folds everyone.
    a.poll_apply().await;
    a.pump_down().await;
    a.sync_up().await; // publishes A's loser-vc materialization (v1)
    b.poll_apply().await;
    b.pump_down().await;
    b.sync_up().await; // publishes B's own round-1 loser vc, if any
    x.poll_apply().await;
    x.pump_down().await;
    a.poll_apply().await;
    a.pump_down().await;
    b.poll_apply().await;
    b.pump_down().await;

    // The fleet agrees — the pre-fix end state was A,B on v1 and X on
    // B's content under the SAME vv, which this equality catches.
    assert_eq!(eh::sync_view(&a.db), eh::sync_view(&x.db));
    assert_eq!(eh::sync_view(&b.db), eh::sync_view(&x.db));
    let primary = x.item(&sidecar_rel);
    assert_eq!(
        primary.sem_hash,
        Some(sem_hash(&r5).expect("sem")),
        "the latest real edit (X's commit) is the fleet primary"
    );
    assert_eq!(
        primary.vv,
        [(dev(DEV_A), 1), (dev(DEV_B), 1), (dev(DEV_X), 1)]
            .into_iter()
            .collect::<rrcloud_core::clock::VersionVector>()
    );
    // v1 survives as the deterministic loser vc on every device.
    let vc_v1 = vc_item_relkey(&image, &loser_vc_suffix(&v1).expect("suffix")).expect("vc");
    for d in [&a, &b, &x] {
        assert_eq!(d.file(&sidecar_rel), r5, "primary bytes");
        assert_eq!(
            d.item(&vc_v1).sem_hash,
            Some(sem_hash(&v1).expect("sem")),
            "v1 is preserved, not destroyed"
        );
    }
    assert_eq!(
        th::get_bytes(&client, &bucket, &library_key(&vc_v1)).await,
        eh::semantic(&v1)
    );
}

/// Round 1 major 4: a PendingDown receiver converging a strictly
/// dominating sem-equal head whose BYTES differ (B's detour edit plus a
/// churned revert uploads churned bytes under v1's sem). Pre-fix the
/// converge kept v1's blake3, so the receiver's fetch condemned the
/// healthy remote to CorruptRemote; now the Greater converge adopts the
/// entry's content identity and the fetch verifies.
#[tokio::test]
async fn s19_pending_receiver_of_a_churned_revert_fetches_clean() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("s19-churned-revert");
    let client = g.client();
    let a = device(g, &bucket, DEV_A);
    let mut b = device(g, &bucket, DEV_B);
    let mut c = device(g, &bucket, DEV_C);
    let image = rel("p/IMG_0042.NEF");
    let sidecar_rel = sidecar_item_relkey(&image).expect("sidecar item");

    // A publishes v1. C applies WITHOUT pumping (PendingDown). B holds it.
    let v1 = eh::doc(3, Some("red"), 0.25);
    assert_eq!(
        a.write_and_notify(&image, Kind::Sidecar, &v1),
        ChangeOutcome::MarkedDirty
    );
    a.sync_up().await;
    c.poll_apply().await;
    assert_eq!(c.item(&sidecar_rel).state, ItemState::PendingDown);
    b.poll_apply().await;
    b.pump_down().await;

    // B detours (real edit) then reverts via a churn rewrite: the gate
    // reports Unchanged but the standing dirt uploads the churned BYTES
    // under v1's sem — the bucket key no longer holds v1's bytes.
    let detour = eh::doc(5, Some("blue"), 0.9);
    assert_eq!(
        b.write_and_notify(&image, Kind::Sidecar, &detour),
        ChangeOutcome::MarkedDirty
    );
    let churned = eh::churned(&v1);
    assert_eq!(
        b.write_and_notify(&image, Kind::Sidecar, &churned),
        ChangeOutcome::Unchanged,
        "churn gate: same sem — but the dirt stands"
    );
    b.sync_up().await;
    assert_eq!(
        th::get_bytes(&client, &bucket, &sidecar_key(&image)).await,
        churned,
        "precondition: the shared key holds the churned bytes"
    );

    // C converges the Greater sem-equal head: it holds NOTHING locally,
    // so it must adopt the entry's blake3/size.
    c.poll_apply().await;
    let converged = c.item(&sidecar_rel);
    assert_eq!(converged.state, ItemState::PendingDown);
    assert_eq!(
        converged.blake3,
        Some(Blake3Hex::from_bytes(&churned)),
        "pre-fix: still v1's blake3 — the fetch below then condemned a \
         healthy remote to CorruptRemote with the vv already advanced"
    );
    assert_eq!(converged.size, churned.len() as u64);
    c.pump_down().await; // pre-fix: IntegrityMismatch wedge
    assert_eq!(c.file(&sidecar_rel), churned);
    assert_eq!(c.item(&sidecar_rel).state, ItemState::Synced);

    // The uploader and the receiver agree exactly; the v1-holding author
    // keeps its own sem-equal bytes authoritative (the pinned twin rule)
    // while folding the same vv and head identity.
    let mut a = a;
    a.poll_apply().await;
    assert_eq!(eh::sync_view(&b.db), eh::sync_view(&c.db));
    let (a_rec, c_rec) = (a.item(&sidecar_rel), c.item(&sidecar_rel));
    assert_eq!(a_rec.vv, c_rec.vv);
    assert_eq!(a_rec.sem_hash, c_rec.sem_hash);
    assert_eq!(a_rec.head_ts, c_rec.head_ts);
    assert_eq!(a_rec.device, c_rec.device);
    assert_eq!(
        a_rec.blake3,
        Some(Blake3Hex::from_bytes(&v1)),
        "the holder's local bytes stay authoritative"
    );
}

/// Round 1 major 6: concurrent sidecar edits whose LOSING upload lands
/// last on the §1.2 shared bucket key. B (clock ahead) wins the §2.6
/// pick but syncs FIRST; A syncs second, so the key holds loser bytes
/// when A (and a fresh C) adopt the winner's blake3 — both wedge
/// CorruptRemote on a key nothing pre-fix would ever repair, while
/// winner-author B stayed Synced and oblivious. Now B's remote-loses
/// resolution re-marks its head Dirty, the next admission re-publishes
/// the winner as a fresh dominating version (re-PUTting its bytes), and
/// the Greater converge rescues every CorruptRemote receiver into the
/// fetch lane.
#[tokio::test]
async fn s20_loser_upload_landing_last_is_repaired_by_the_winning_author() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("s20-loser-lands-last");
    let client = g.client();
    let mut a = device(g, &bucket, DEV_A);
    let mut b = device(g, &bucket, DEV_B);
    let image = rel("p/IMG_0042.NEF");
    let sidecar_rel = sidecar_item_relkey(&image).expect("sidecar item");
    {
        let mut rest = [&mut b];
        seed_photo(&image, &a, &mut rest).await;
    }

    // Concurrent edits; B wins every pick (clock an hour ahead) and its
    // upload lands FIRST — then A's losing upload overwrites the key.
    let doc_a2 = eh::doc(2, Some("red"), 0.2);
    let doc_b2 = eh::doc(5, Some("blue"), 0.9);
    assert_eq!(
        a.write_and_notify(&image, Kind::Sidecar, &doc_a2),
        ChangeOutcome::MarkedDirty
    );
    assert_eq!(
        b.write_and_notify(&image, Kind::Sidecar, &doc_b2),
        ChangeOutcome::MarkedDirty
    );
    b.db.set_server_time_offset_ms(3_600_000).expect("offset");
    b.sync_up().await;
    a.sync_up().await;
    assert_eq!(
        th::get_bytes(&client, &bucket, &sidecar_key(&image)).await,
        doc_a2,
        "precondition: the shared key holds the LOSER's bytes"
    );

    // A resolves (remote wins), preserves its loser vc, and fetches the
    // winner — straight into the poisoned key: CorruptRemote.
    a.poll_apply().await;
    let suffix = loser_vc_suffix(&doc_a2).expect("suffix");
    let vc_rel = vc_item_relkey(&image, &suffix).expect("vc relkey");
    assert_eq!(a.events.conflicts.len(), 1);
    assert_eq!(a.events.conflicts[0].copy_relkey, Some(vc_rel.clone()));
    let summary = pump_downloads(&a.db, &a.s3, &a.cfg, 2, &CancelFlag::new())
        .await
        .expect("pump");
    assert!(
        summary
            .failed
            .iter()
            .any(|(k, e)| k == &sidecar_rel
                && matches!(e, TransferError::IntegrityMismatch { .. })),
        "the winner's holders advertised a blake3 the key's bytes fail: {:?}",
        summary.failed
    );
    assert_eq!(a.item(&sidecar_rel).state, ItemState::CorruptRemote);
    a.sync_up().await; // the loser vc still uploads + publishes

    // A fresh third device wedges identically (the probe's C).
    let mut c = device(g, &bucket, DEV_C);
    c.poll_apply().await;
    let summary = pump_downloads(&c.db, &c.s3, &c.cfg, 2, &CancelFlag::new())
        .await
        .expect("pump");
    assert!(summary.failed.iter().any(|(k, _)| k == &sidecar_rel));
    assert_eq!(c.item(&sidecar_rel).state, ItemState::CorruptRemote);

    // B applies A's losing head: it authored the winner and holds its
    // bytes quiescently — the resolution re-marks it Dirty so the next
    // admission re-publishes the winner over the poisoned key.
    b.poll_apply().await;
    assert_eq!(
        b.item(&sidecar_rel).state,
        ItemState::Dirty,
        "pre-fix: Synced and oblivious — nothing ever re-uploaded the winner"
    );
    b.sync_up().await;
    b.pump_down().await; // A's vc advertisement
    assert_eq!(
        th::get_bytes(&client, &bucket, &sidecar_key(&image)).await,
        doc_b2,
        "the repair re-PUT the winner's bytes over the key"
    );

    // The Greater re-advertisement rescues both wedged receivers.
    a.poll_apply().await;
    assert_eq!(a.item(&sidecar_rel).state, ItemState::PendingDown);
    a.pump_down().await;
    assert_eq!(a.file(&sidecar_rel), doc_b2);
    assert_eq!(a.item(&sidecar_rel).state, ItemState::Synced);
    c.poll_apply().await;
    c.pump_down().await;
    assert_eq!(c.file(&sidecar_rel), doc_b2);

    // Fleet truth: winner primary everywhere, loser preserved as the vc,
    // and the shared key's bytes match the advertised head.
    assert_eq!(eh::sync_view(&a.db), eh::sync_view(&b.db));
    assert_eq!(eh::sync_view(&a.db), eh::sync_view(&c.db));
    let primary = a.item(&sidecar_rel);
    assert_eq!(primary.sem_hash, Some(sem_hash(&doc_b2).expect("sem")));
    assert_eq!(primary.blake3, Some(Blake3Hex::from_bytes(&doc_b2)));
    for d in [&a, &b, &c] {
        assert_eq!(
            d.item(&vc_rel).sem_hash,
            Some(sem_hash(&doc_a2).expect("sem")),
            "A's edit survives as the vc"
        );
    }
    assert_eq!(
        b.puts_to(&sidecar_key(&image)),
        2,
        "B's original upload + exactly one repair re-upload"
    );
}
