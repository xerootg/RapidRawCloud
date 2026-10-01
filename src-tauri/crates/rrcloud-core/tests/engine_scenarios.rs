//! The P1 acceptance scenarios (architecture §2.11): S1–S8, Garage-backed,
//! two or three `SyncDb` "devices" inside one test process, driving
//! publish / poll / pump manually through the engine's synchronous-async
//! entry points. Every scenario asserts by **state equivalence** (full
//! item records or the cross-device [`common::engine::SyncView`]
//! projection, vv maps, deleted sets) and **bucket inspection** (key
//! listings and object bytes), never by smoke signals.
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
use common::sync::{dev, open_db, rel, DEV_A, DEV_B, DEV_C};
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
use rrcloud_core::publisher::publish_pending;
use rrcloud_core::reader::{ConsumerError, JournalConsumer as _};
use rrcloud_core::semhash::{sem_hash, ContentId};
use rrcloud_core::state::{ItemRecord, ItemState, Queue, SyncDb};
use rrcloud_core::transfer::TransferConfig;

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
            .filter(|e| e.op == Op::Put && e.key == key)
            .next_back()
            .expect("author's sidecar head entry")
    };
    (last_of(entries_a), last_of(entries_b))
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
        assert_eq!(d.file(&vc_rel), doc_a, "loser preserved as the vc");
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
        doc_a
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
        .filter(|e| e.op == Op::Put && e.key == sidecar_key(&image))
        .next_back()
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
        .filter(|e| e.op == Op::Put && e.key == sidecar_key(&image))
        .next_back()
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
        assert_eq!(d.file(&vc_rel), doc_a2, "A's edit survives as the vc");
    }
    assert_eq!(
        th::get_bytes(&client, &bucket, &library_key(&vc_rel)).await,
        doc_a2
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
    for key in [&sidecar_rel, &image] {
        assert_eq!(
            compare(&a.item(key).vv, &outcome.tombstone.vv),
            VvOrder::Greater,
            "{key}: restored past the deletion"
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
            doc2,
            "each holder wrote the identical vc file"
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
        doc2
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
        doc2,
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
