//! The §2.2 earliest-mtime rule (`rrcloud_core::mtime`) and its one-time
//! backfill.
//!
//! The motivating history, reproduced here in miniature: the worker adopted
//! a dock upload before the dock's journal landed and stamped its
//! `{worker:1}` version with the upload time, while the dock's own entry for
//! the same bytes carried the capture time. Whichever twin a device applied
//! first used to own the record's mtime — and the file's, which the library
//! sorts by.
//!
//! - **Rule** — content-equal original versions converge on the earliest
//!   mtime in either arrival order, never move it later, and re-stamp the
//!   stub or hydrated file the engine materialized.
//! - **Backfill** (Garage-backed) — records that converged before the rule
//!   existed take the earliest mtime any foreign journal `put` or manifest
//!   row carries for their exact bytes, files are re-stamped, and the pass
//!   runs once.

mod common;

use std::path::Path;

use common::garage;
use common::sync::{dev, entry as journal_entry, open_db, rel, DEV_A, DEV_B, DEV_C};
use rrcloud_core::clock::VersionVector;
use rrcloud_core::engine::{item_local_path, EngineConsumer};
use rrcloud_core::journal::{JournalEntry, Kind, Op};
use rrcloud_core::keys::{library_key, RelKey};
use rrcloud_core::manifest::{build_manifest, put_manifest};
use rrcloud_core::mtime::{backfill_earliest_mtimes, earlier_mtime};
use rrcloud_core::publisher::{enqueue_entry, publish_pending};
use rrcloud_core::reader::{ConsumerError, JournalConsumer};
use rrcloud_core::semhash::{Blake3Hex, ContentId};
use rrcloud_core::state::{ItemRecord, ItemState, SyncDb};
use rrcloud_core::transfer::{stub_pending_originals_bounded, CancelFlag};

/// The dock's capture-time mtime and the worker's upload-time mtime for the
/// same bytes (2026-08-29 vs 2026-10-13).
const CAPTURE: i64 = 1_787_996_925;
const UPLOAD: i64 = 1_791_900_000;
const NS: i64 = 1_000_000_000;

const BYTES: &[u8] = b"the same camera bytes";

fn vv(pairs: &[(&str, u32)]) -> VersionVector {
    pairs.iter().map(|(d, n)| (dev(d), *n)).collect()
}

/// A `put original` for `item` advertising [`BYTES`] at `mtime`.
fn put_original(
    author: &str,
    item: &RelKey,
    v: VersionVector,
    ts: i64,
    mtime: i64,
) -> JournalEntry {
    let mut e = journal_entry(&dev(author), Op::Put, Kind::Original, library_key(item));
    e.vv = v;
    e.ts = ts;
    e.size = Some(BYTES.len() as u64);
    e.blake3 = Some(Blake3Hex::from_bytes(BYTES));
    e.content_id = Some(ContentId::from_bytes(BYTES));
    e.mtime = Some(mtime);
    e
}

fn apply(db: &SyncDb, root: &Path, entry: &JournalEntry) {
    let mut events = ();
    let mut consumer = EngineConsumer::new(db, root, &mut events).expect("consumer");
    db.with_txn_err::<_, ConsumerError>(|t| consumer.apply(t, entry))
        .expect("consumer apply");
}

fn record(db: &SyncDb, item: &RelKey) -> ItemRecord {
    db.get_item(item).expect("get_item").expect("record")
}

fn file_mtime(path: &Path) -> i64 {
    let meta = std::fs::metadata(path).expect("file present");
    filetime::FileTime::from_last_modification_time(&meta).unix_seconds()
}

// ===========================================================================
// The rule
// ===========================================================================

#[test]
fn earlier_mtime_only_ever_moves_the_mtime_earlier() {
    let local = UPLOAD * NS + 123;
    assert_eq!(earlier_mtime(local, Some(CAPTURE)), Some(CAPTURE));
    assert_eq!(
        earlier_mtime(local, Some(UPLOAD + 5)),
        None,
        "later never wins"
    );
    assert_eq!(
        earlier_mtime(local, Some(UPLOAD)),
        None,
        "equal seconds keep the local nanoseconds"
    );
    assert_eq!(earlier_mtime(local, None), None);
    assert_eq!(
        earlier_mtime(local, Some(0)),
        None,
        "0 is unknown, not 1970"
    );
    assert_eq!(earlier_mtime(local, Some(-5)), None);
    assert_eq!(
        earlier_mtime(0, Some(UPLOAD)),
        Some(UPLOAD),
        "a record without an mtime takes any"
    );
}

/// The worker's `{worker:1}` twin and the dock's `{dock:1}` twin carry the
/// same bytes: a device that applies the worker's first and one that applies
/// the dock's first both settle on the capture time.
#[test]
fn content_equal_twins_converge_on_the_earliest_mtime_in_either_order() {
    let item = rel("Camera Import/Z 7_2/2026/08/29/_DSC0001.NEF");
    let worker_twin = put_original(DEV_A, &item, vv(&[(DEV_A, 1)]), UPLOAD + 60, UPLOAD);
    let dock_twin = put_original(DEV_B, &item, vv(&[(DEV_B, 1)]), UPLOAD - 3600, CAPTURE);

    for (label, order) in [
        ("worker first", [&worker_twin, &dock_twin]),
        ("dock first", [&dock_twin, &worker_twin]),
    ] {
        let (_dir, _path, db) = open_db(&dev(DEV_C));
        let root = tempfile::tempdir().expect("root");
        for e in order {
            apply(&db, root.path(), e);
        }
        let r = record(&db, &item);
        assert_eq!(r.mtime_unix_ns, CAPTURE * NS, "{label}");
        assert_eq!(r.vv, vv(&[(DEV_A, 1), (DEV_B, 1)]), "{label}");
    }
}

/// A device that already turned the worker's twin into a cloud stub (the
/// 0-byte placeholder is written at the record's mtime) re-stamps the
/// placeholder when the dock's twin lowers the record's mtime.
#[test]
fn a_stub_is_restamped_when_its_twin_lowers_the_mtime() {
    let item = rel("Camera Import/Z 7_2/2026/08/29/_DSC0002.NEF");
    let (_dir, _path, db) = open_db(&dev(DEV_C));
    let root = tempfile::tempdir().expect("root");

    apply(
        &db,
        root.path(),
        &put_original(DEV_A, &item, vv(&[(DEV_A, 1)]), UPLOAD + 60, UPLOAD),
    );
    let stubbed =
        stub_pending_originals_bounded(&db, root.path(), &CancelFlag::new()).expect("stub pass");
    assert_eq!(stubbed, 1);
    let path = item_local_path(root.path(), &item);
    assert_eq!(
        file_mtime(&path),
        UPLOAD,
        "stub written at the worker's mtime"
    );

    apply(
        &db,
        root.path(),
        &put_original(DEV_B, &item, vv(&[(DEV_B, 1)]), UPLOAD - 3600, CAPTURE),
    );
    let r = record(&db, &item);
    assert_eq!(r.state, ItemState::Stub);
    assert_eq!(r.mtime_unix_ns, CAPTURE * NS);
    assert_eq!(
        file_mtime(&path),
        CAPTURE,
        "the placeholder follows the record"
    );
    assert_eq!(std::fs::metadata(&path).unwrap().len(), 0, "still a stub");
}

/// A hydrated copy (real bytes installed at the record's mtime) is
/// re-stamped the same way; its bytes are untouched.
#[test]
fn a_hydrated_file_is_restamped_when_its_twin_lowers_the_mtime() {
    let item = rel("Camera Import/Z 7_2/2026/08/29/_DSC0003.NEF");
    let (_dir, _path, db) = open_db(&dev(DEV_C));
    let root = tempfile::tempdir().expect("root");
    let path = item_local_path(root.path(), &item);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, BYTES).unwrap();
    filetime::set_file_mtime(&path, filetime::FileTime::from_unix_time(UPLOAD, 0)).unwrap();
    db.insert_item(
        &item,
        &ItemRecord {
            kind: Kind::Original,
            state: ItemState::Hydrated,
            size: BYTES.len() as u64,
            mtime_unix_ns: UPLOAD * NS,
            blake3: Some(Blake3Hex::from_bytes(BYTES)),
            sem_hash: None,
            vv: vv(&[(DEV_A, 1)]),
            content_id: Some(ContentId::from_bytes(BYTES)),
            w: None,
            h: None,
            pinned: false,
            last_access_unix: 0,
            verified_remote: true,
            attested: true,
            base_unknown: false,
            rating: None,
            color_label: None,
            device: Some(dev(DEV_A)),
            head_ts: Some(UPLOAD + 60),
            admitted_vv: None,
            admitted_ts: None,
            deleted: false,
        },
    )
    .expect("seed hydrated record");

    apply(
        &db,
        root.path(),
        &put_original(DEV_B, &item, vv(&[(DEV_B, 1)]), UPLOAD - 3600, CAPTURE),
    );
    assert_eq!(record(&db, &item).mtime_unix_ns, CAPTURE * NS);
    assert_eq!(file_mtime(&path), CAPTURE);
    assert_eq!(std::fs::read(&path).unwrap(), BYTES, "bytes untouched");
}

/// Different bytes are a different version, not a twin: the §2.6 apply
/// adopts the dominating entry's own mtime as before (the rule never mixes
/// the mtimes of distinct content).
#[test]
fn a_different_content_version_carries_its_own_mtime() {
    let item = rel("p/edited-elsewhere.NEF");
    let (_dir, _path, db) = open_db(&dev(DEV_C));
    let root = tempfile::tempdir().expect("root");
    apply(
        &db,
        root.path(),
        &put_original(DEV_A, &item, vv(&[(DEV_A, 1)]), UPLOAD, CAPTURE),
    );
    let mut newer = put_original(DEV_A, &item, vv(&[(DEV_A, 2)]), UPLOAD + 10, UPLOAD);
    newer.blake3 = Some(Blake3Hex::from_bytes(b"replaced bytes"));
    newer.content_id = Some(ContentId::from_bytes(b"replaced bytes"));
    apply(&db, root.path(), &newer);
    assert_eq!(record(&db, &item).mtime_unix_ns, UPLOAD * NS);
}

// ===========================================================================
// The one-time backfill (Garage-backed)
// ===========================================================================

/// A record as the pre-rule engine left it: both twins applied (merged vv),
/// the worker's upload time kept as the mtime, turned into a cloud stub.
fn converged_stub(blake3_of: &[u8], mtime: i64) -> ItemRecord {
    ItemRecord {
        kind: Kind::Original,
        state: ItemState::Stub,
        size: blake3_of.len() as u64,
        mtime_unix_ns: mtime * NS,
        blake3: Some(Blake3Hex::from_bytes(blake3_of)),
        sem_hash: None,
        vv: vv(&[(DEV_A, 1), (DEV_B, 1)]),
        content_id: Some(ContentId::from_bytes(blake3_of)),
        w: None,
        h: None,
        pinned: false,
        last_access_unix: 0,
        verified_remote: true,
        attested: false,
        base_unknown: false,
        rating: None,
        color_label: None,
        device: Some(dev(DEV_A)),
        head_ts: Some(UPLOAD + 60),
        admitted_vv: None,
        admitted_ts: None,
        deleted: false,
    }
}

fn write_stub(root: &Path, item: &RelKey, mtime: i64) -> std::path::PathBuf {
    let path = item_local_path(root, item);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, b"").unwrap();
    filetime::set_file_mtime(&path, filetime::FileTime::from_unix_time(mtime, 0)).unwrap();
    path
}

/// A device that converged before the rule existed: the backfill finds the
/// earliest mtime for each record's exact bytes in the peers' journals and
/// manifests, re-dates the records and their stubs, leaves records whose
/// bytes no peer advertised earlier alone, and runs only once.
#[tokio::test]
async fn the_backfill_redates_previously_converged_records_once() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("mtime-backfill");
    let client = g.client();

    // Journal-advertised: the worker's twin (upload time) and the dock's
    // twin (capture time) for the same bytes.
    let journaled = rel("Camera Import/Z 7_2/2026/08/29/_DSC0001.NEF");
    // Manifest-only: the dock compacted its entry into its manifest.
    let manifested = rel("Camera Import/Z 7_2/2026/08/29/_DSC0002.NEF");
    // A record whose bytes differ from every advertised put: untouched.
    let other_bytes = rel("Camera Import/Z 7_2/2026/08/29/_DSC0003.NEF");

    let (_wd, _wp, worker_db) = open_db(&dev(DEV_A));
    for item in [&journaled, &other_bytes] {
        enqueue_entry(
            &worker_db,
            &put_original(DEV_A, item, vv(&[(DEV_A, 1)]), UPLOAD + 60, UPLOAD),
        )
        .unwrap();
    }
    publish_pending(&worker_db, &client, &bucket).await.unwrap();

    let (_dd, _dp, dock_db) = open_db(&dev(DEV_B));
    enqueue_entry(
        &dock_db,
        &put_original(DEV_B, &journaled, vv(&[(DEV_B, 1)]), UPLOAD - 3600, CAPTURE),
    )
    .unwrap();
    let mut different = put_original(DEV_B, &other_bytes, vv(&[(DEV_B, 1)]), UPLOAD, CAPTURE);
    different.blake3 = Some(Blake3Hex::from_bytes(b"not what the device holds"));
    enqueue_entry(&dock_db, &different).unwrap();
    publish_pending(&dock_db, &client, &bucket).await.unwrap();
    let mut row_rec = converged_stub(BYTES, CAPTURE - 7);
    row_rec.state = ItemState::Synced;
    row_rec.vv = vv(&[(DEV_B, 1)]);
    dock_db.insert_item(&manifested, &row_rec).unwrap();
    let manifest = build_manifest(&dock_db, UPLOAD).unwrap();
    put_manifest(&client, &bucket, &dev(DEV_B), &manifest)
        .await
        .unwrap();

    // The device under repair (pre-rule state: upload time everywhere).
    let (_cd, _cp, db) = open_db(&dev(DEV_C));
    let root = tempfile::tempdir().expect("root");
    let mut paths = Vec::new();
    for item in [&journaled, &manifested, &other_bytes] {
        db.insert_item(item, &converged_stub(BYTES, UPLOAD))
            .unwrap();
        paths.push(write_stub(root.path(), item, UPLOAD));
    }

    let report = backfill_earliest_mtimes(&db, &client, &bucket, Some(root.path()))
        .await
        .expect("backfill");
    assert!(report.ran);
    assert_eq!(
        report.corrected,
        vec![journaled.clone(), manifested.clone()]
    );
    assert!(
        report.segments_read >= 2,
        "read both peers' segments: {report:?}"
    );
    assert_eq!(report.manifests_read, 1);

    assert_eq!(record(&db, &journaled).mtime_unix_ns, CAPTURE * NS);
    assert_eq!(file_mtime(&paths[0]), CAPTURE);
    assert_eq!(record(&db, &manifested).mtime_unix_ns, (CAPTURE - 7) * NS);
    assert_eq!(file_mtime(&paths[1]), CAPTURE - 7);
    assert_eq!(record(&db, &other_bytes).mtime_unix_ns, UPLOAD * NS);
    assert_eq!(file_mtime(&paths[2]), UPLOAD);
    assert_eq!(
        record(&db, &journaled).state,
        ItemState::Stub,
        "state preserved"
    );

    // Done once: a second call reads nothing.
    assert!(db.mtime_backfill_done().unwrap());
    let again = backfill_earliest_mtimes(&db, &client, &bucket, Some(root.path()))
        .await
        .expect("second backfill");
    assert!(!again.ran);
    assert_eq!(again.segments_read, 0);
}
