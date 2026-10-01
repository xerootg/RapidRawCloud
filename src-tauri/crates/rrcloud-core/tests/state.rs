//! Failing tests for `rrcloud_core::state` (architecture §3.2, §2.1.5,
//! §2.4, §2.2, §5.1).
//!
//! In-process suite: open/init semantics, schema gates, record round-trips
//! across reopen, the §2.4 transition table + atomic CAS transitions
//! (including the two-thread race), journal-publication primitives
//! (allocate/freeze/unpublished/mark_published), queues, applied/cursors,
//! upload resume state, and the seen-caches.
//!
//! The child-process suites (SIGKILL crash injection, cross-process lock
//! exclusivity) live in `tests/state_crash.rs` (`harness = false`).

use std::path::PathBuf;

use rrcloud_core::clock::{DeviceId, VersionVector};
use rrcloud_core::journal::Kind;
use rrcloud_core::keys::RelKey;
use rrcloud_core::semhash::{Blake3Hex, ContentId, SemHash};
use rrcloud_core::state::{
    legal, legal_entry, ItemRecord, ItemState, MultipartUploadState, Queue, StateError, SyncDb,
    UploadPart,
};

const DEV1: &str = "d1f0c2aa-9d2b-4a6e-8f1c-3b7d5e9a0c42";
const DEV2: &str = "a3b2e1d0-5c4f-4b3a-9e2d-1f0a9b8c7d6e";
const BLAKE3_HEX: &str = "4878ca0425c739fa427f7eda20fe845f6b2e46ba5fe2a14df5b1e32f50603215";
const SEMHASH_HEX: &str = "af1349b9f5f9a1a6a0404dee36dcc9499bcb25c9adc112b7cc9a93cae41f3262";
const CONTENT_HEX: &str = "0000ca0425c739fa427f7eda20fe845f6b2e46ba5fe2a14df5b1e32f50603215";

fn dev(s: &str) -> DeviceId {
    DeviceId::new(s).expect("valid device id")
}

fn rel(s: &str) -> RelKey {
    RelKey::new(s).expect("valid relkey")
}

/// A scratch dir + db path; the dir is removed on drop.
fn scratch() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("state.redb");
    (dir, path)
}

fn open_fresh(path: &std::path::Path) -> SyncDb {
    SyncDb::open(path, Some(dev(DEV1))).expect("fresh open with device id")
}

/// A fully-populated record (every Option Some, multi-device vv).
fn full_record() -> ItemRecord {
    ItemRecord {
        kind: Kind::Original,
        state: ItemState::Synced,
        size: 31_457_280,
        mtime_unix_ns: 1_769_899_000_123_456_789,
        blake3: Some(Blake3Hex::parse(BLAKE3_HEX).expect("blake3")),
        sem_hash: Some(SemHash::parse(SEMHASH_HEX).expect("semhash")),
        vv: [(dev(DEV1), 9u32), (dev(DEV2), 4u32)].into_iter().collect(),
        content_id: Some(ContentId::parse(CONTENT_HEX).expect("content id")),
        w: Some(6000),
        h: Some(4000),
        pinned: true,
        last_access_unix: 1_769_900_000,
        verified_remote: true,
        attested: true,
        base_unknown: false,
        rating: Some(3),
        color_label: Some("red".to_string()),
        device: Some(dev(DEV1)),
        head_ts: Some(1_769_900_000),
        admitted_vv: Some([(dev(DEV1), 10u32)].into_iter().collect()),
        deleted: false,
    }
}

/// A minimal record in `state` (every Option None).
fn bare_record(state: ItemState) -> ItemRecord {
    ItemRecord {
        kind: Kind::Sidecar,
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

// ---------------------------------------------------------------------------
// Open / init / identity (§3.2, §5.1)
// ---------------------------------------------------------------------------

#[test]
fn first_open_mints_supplied_device_id_and_reopen_returns_it() {
    let (_dir, path) = scratch();
    {
        let db = open_fresh(&path);
        assert_eq!(db.device_id(), &dev(DEV1));
        assert_eq!(db.path(), path.as_path());
    }
    // Reopen with None: identity comes from the db.
    let db = SyncDb::open(&path, None).expect("reopen");
    assert_eq!(db.device_id(), &dev(DEV1));
    // Reopen with the matching id: fine too.
    drop(db);
    let db = SyncDb::open(&path, Some(dev(DEV1))).expect("reopen with matching id");
    assert_eq!(db.device_id(), &dev(DEV1));
}

#[test]
fn fresh_open_without_device_id_is_typed_error_and_leaves_no_usable_db() {
    let (_dir, path) = scratch();
    let err = SyncDb::open(&path, None).expect_err("fresh db without id must refuse");
    assert!(matches!(err, StateError::DeviceIdRequired), "got {err:?}");
    // The refusal must not have half-initialized the file: a subsequent
    // open WITH an id succeeds and mints it.
    let db = SyncDb::open(&path, Some(dev(DEV2))).expect("open with id after refusal");
    assert_eq!(db.device_id(), &dev(DEV2));
}

#[test]
fn reopen_with_mismatched_device_id_is_typed_error() {
    let (_dir, path) = scratch();
    drop(open_fresh(&path));
    let err = SyncDb::open(&path, Some(dev(DEV2))).expect_err("mismatched id must refuse");
    match err {
        StateError::DeviceIdMismatch { stored, given } => {
            assert_eq!(stored, dev(DEV1));
            assert_eq!(given, dev(DEV2));
        }
        other => panic!("expected DeviceIdMismatch, got {other:?}"),
    }
    // And the stored identity is untouched.
    let db = SyncDb::open(&path, None).expect("reopen");
    assert_eq!(db.device_id(), &dev(DEV1));
}

#[test]
fn second_open_in_same_process_is_already_locked_not_a_hang() {
    // Empirical redb behavior (module docs): a second open fails in
    // microseconds with DatabaseAlreadyOpen even within one process. §5.1
    // depends on this being a fast typed refusal. The cross-process variant
    // is pinned in tests/state_crash.rs.
    let (_dir, path) = scratch();
    let _held = open_fresh(&path);
    let started = std::time::Instant::now();
    let err = SyncDb::open(&path, None).expect_err("second open must refuse");
    assert!(
        started.elapsed() < std::time::Duration::from_secs(2),
        "refusal must be immediate, took {:?}",
        started.elapsed()
    );
    match err {
        StateError::AlreadyLocked { path: p } => assert_eq!(p, path),
        other => panic!("expected AlreadyLocked, got {other:?}"),
    }
}

#[test]
fn drop_releases_the_lock() {
    let (_dir, path) = scratch();
    drop(open_fresh(&path));
    SyncDb::open(&path, None).expect("reopen after drop must succeed");
}

// ---------------------------------------------------------------------------
// Schema versioning (fail closed)
// ---------------------------------------------------------------------------

#[test]
fn higher_schema_version_is_rejected_typed() {
    let (_dir, path) = scratch();
    {
        let db = open_fresh(&path);
        db.force_schema_version(Some(rrcloud_core::state::SCHEMA_VERSION + 1))
            .expect("test override");
    }
    let err = SyncDb::open(&path, None).expect_err("newer schema must refuse");
    match err {
        StateError::SchemaTooNew { found, supported } => {
            assert_eq!(found, rrcloud_core::state::SCHEMA_VERSION + 1);
            assert_eq!(supported, rrcloud_core::state::SCHEMA_VERSION);
        }
        other => panic!("expected SchemaTooNew, got {other:?}"),
    }
}

#[test]
fn lower_schema_version_is_rejected_typed() {
    let (_dir, path) = scratch();
    {
        let db = open_fresh(&path);
        db.force_schema_version(Some(0)).expect("test override");
    }
    let err = SyncDb::open(&path, None).expect_err("older schema must refuse");
    assert!(
        matches!(err, StateError::SchemaUnsupported { found: Some(0) }),
        "got {err:?}"
    );
}

#[test]
fn missing_schema_version_on_initialized_db_is_rejected_typed() {
    let (_dir, path) = scratch();
    {
        let db = open_fresh(&path);
        db.force_schema_version(None).expect("test override");
    }
    let err = SyncDb::open(&path, None).expect_err("versionless schema must refuse");
    assert!(
        matches!(err, StateError::SchemaUnsupported { found: None }),
        "got {err:?}"
    );
}

// ---------------------------------------------------------------------------
// ItemRecord round-trips (durability across reopen)
// ---------------------------------------------------------------------------

#[test]
fn item_record_full_roundtrip_survives_reopen() {
    let (_dir, path) = scratch();
    let key = rel("2026/10/IMG_0042.NEF");
    let record = full_record();
    {
        let db = open_fresh(&path);
        db.replay_put_item(&key, &record).expect("put");
        // Visible before reopen too.
        assert_eq!(db.get_item(&key).expect("get"), Some(record.clone()));
    }
    let db = SyncDb::open(&path, None).expect("reopen");
    let got = db.get_item(&key).expect("get after reopen");
    assert_eq!(got, Some(record), "full record must round-trip byte-stable");
}

#[test]
fn item_record_minimal_roundtrip_and_missing_key() {
    let (_dir, path) = scratch();
    let key = rel("a.dng");
    let record = bare_record(ItemState::Dirty);
    {
        let db = open_fresh(&path);
        assert_eq!(db.get_item(&key).expect("get missing"), None);
        db.insert_item(&key, &record).expect("put");
    }
    let db = SyncDb::open(&path, None).expect("reopen");
    assert_eq!(db.get_item(&key).expect("get"), Some(record));
    assert_eq!(db.get_item(&rel("other.dng")).expect("get other"), None);
}

#[test]
fn delete_item_removes_and_reports() {
    let (_dir, path) = scratch();
    let key = rel("a.dng");
    let db = open_fresh(&path);
    db.insert_item(&key, &bare_record(ItemState::Dirty))
        .expect("put");
    assert!(db.delete_item(&key).expect("delete"));
    assert_eq!(db.get_item(&key).expect("get"), None);
    assert!(!db.delete_item(&key).expect("second delete"), "idempotent");
}

// ---------------------------------------------------------------------------
// Transition table (§2.4) — the single legal() authority
// ---------------------------------------------------------------------------

#[test]
fn legal_upload_pipeline_chain() {
    use ItemState::*;
    for (from, to) in [
        (Dirty, Queued),
        (Queued, Uploading),
        (Uploading, Verifying),
        (Verifying, Synced),
        (Synced, Dirty),
    ] {
        assert!(legal(from, to), "{from:?} -> {to:?} must be legal");
    }
}

#[test]
fn legal_download_and_hydration_chain() {
    use ItemState::*;
    for (from, to) in [
        (Synced, PendingDown),
        (PendingDown, Downloading),
        (Downloading, Synced),
        (Downloading, Hydrated),
        (Hydrated, Stub),
        (Stub, Downloading),
        (Stub, PendingDown),
        (Downloading, PendingDown),
        (Hydrated, Dirty),
        (Synced, Stub),
    ] {
        assert!(legal(from, to), "{from:?} -> {to:?} must be legal");
    }
}

#[test]
fn legal_failure_and_conflict_edges() {
    use ItemState::*;
    for (from, to) in [
        (Uploading, Queued),
        (Uploading, Dirty),
        (Verifying, Queued),
        (Verifying, CorruptRemote),
        (Synced, CorruptRemote),
        (Downloading, CorruptRemote),
        (CorruptRemote, Queued),
        (CorruptRemote, PendingDown),
        (Dirty, Conflict),
        (Synced, Conflict),
        (Conflict, Dirty),
        (Conflict, PendingDown),
    ] {
        assert!(legal(from, to), "{from:?} -> {to:?} must be legal");
    }
}

#[test]
fn legal_mid_pipeline_conflict_and_pending_down_edit_edges() {
    // §2.6: a concurrent remote entry can arrive while the committed local
    // version sits anywhere in the upload pipeline (vv bumps at queue
    // admission, §3.7), and the unified apply rule's case 4 must be able to
    // record Conflict from there. Likewise a local edit on a PendingDown
    // item (remote advertised newer, not yet downloaded) is a concurrent
    // edit the §3.4 chokepoint must be able to record.
    use ItemState::*;
    for (from, to) in [
        (Queued, Conflict),
        (Uploading, Conflict),
        (Verifying, Conflict),
        (PendingDown, Conflict),
        (PendingDown, Dirty),
    ] {
        assert!(legal(from, to), "{from:?} -> {to:?} must be legal");
    }
}

#[test]
fn illegal_shortcuts_rejected() {
    use ItemState::*;
    for (from, to) in [
        (Dirty, Uploading),      // must pass through the queue
        (Dirty, Synced),         // no teleporting to synced
        (Queued, Synced),        // upload + verify cannot be skipped
        (Queued, Verifying),     // upload cannot be skipped
        (Synced, Uploading),     // must re-dirty + queue first
        (Synced, Verifying),     // nothing to verify
        (Stub, Hydrated),        // bytes must actually download
        (PendingDown, Synced),   // bytes must actually download
        (PendingDown, Hydrated), // likewise
        (CorruptRemote, Synced), // repair goes through upload/download
        (Conflict, Synced),      // resolution goes through a real path
        (Hydrated, Synced),      // distinct terminal rows, no silent alias
        (Uploading, Synced),     // verify cannot be skipped
    ] {
        assert!(!legal(from, to), "{from:?} -> {to:?} must be illegal");
    }
}

#[test]
fn self_loops_are_illegal() {
    for s in ItemState::ALL {
        assert!(!legal(s, s), "{s:?} -> {s:?} must be illegal");
    }
}

// ---------------------------------------------------------------------------
// transition(): atomic CAS semantics
// ---------------------------------------------------------------------------

#[test]
fn transition_applies_mutation_and_new_state_atomically() {
    let (_dir, path) = scratch();
    let key = rel("a.rrdata");
    {
        let db = open_fresh(&path);
        db.insert_item(&key, &bare_record(ItemState::Dirty))
            .expect("put");
        let committed = db
            .transition(&key, ItemState::Dirty, ItemState::Queued, |r| {
                r.size = 123;
                r.last_access_unix = 42;
            })
            .expect("legal transition");
        assert_eq!(committed.state, ItemState::Queued);
        assert_eq!(committed.size, 123);
        let got = db.get_item(&key).expect("get").expect("exists");
        assert_eq!(got, committed, "returned record is the committed record");
    }
    // Durable across reopen.
    let db = SyncDb::open(&path, None).expect("reopen");
    let got = db.get_item(&key).expect("get").expect("exists");
    assert_eq!(got.state, ItemState::Queued);
    assert_eq!(got.size, 123);
    assert_eq!(got.last_access_unix, 42);
}

#[test]
fn transition_overrides_state_written_by_mutate() {
    let (_dir, path) = scratch();
    let key = rel("a.rrdata");
    let db = open_fresh(&path);
    db.insert_item(&key, &bare_record(ItemState::Dirty))
        .expect("put");
    let committed = db
        .transition(&key, ItemState::Dirty, ItemState::Queued, |r| {
            r.state = ItemState::Synced; // mutate must not control state
        })
        .expect("transition");
    assert_eq!(committed.state, ItemState::Queued);
}

#[test]
fn transition_with_wrong_expected_from_is_stale_and_mutates_nothing() {
    let (_dir, path) = scratch();
    let key = rel("a.rrdata");
    let db = open_fresh(&path);
    let before = bare_record(ItemState::Dirty);
    db.insert_item(&key, &before).expect("put");
    let err = db
        .transition(&key, ItemState::Queued, ItemState::Uploading, |r| {
            r.size = 999; // must never land
        })
        .expect_err("stale expected_from must refuse");
    match err {
        StateError::StaleState {
            relkey,
            expected,
            found,
        } => {
            assert_eq!(relkey, key);
            assert_eq!(expected, ItemState::Queued);
            assert_eq!(found, Some(ItemState::Dirty));
        }
        other => panic!("expected StaleState, got {other:?}"),
    }
    let got = db.get_item(&key).expect("get").expect("exists");
    assert_eq!(got, before, "failed transition must not mutate the record");
}

#[test]
fn transition_illegal_pair_is_illegal_even_when_stored_state_differs_too() {
    let (_dir, path) = scratch();
    let key = rel("a.rrdata");
    let db = open_fresh(&path);
    let before = bare_record(ItemState::Synced);
    db.replay_put_item(&key, &before).expect("put");
    // (Dirty -> Synced) is statically illegal AND the stored state is
    // Synced, not Dirty. The static check is pinned to run first.
    let err = db
        .transition(&key, ItemState::Dirty, ItemState::Synced, |_| {})
        .expect_err("illegal pair must refuse");
    assert!(
        matches!(
            err,
            StateError::IllegalTransition {
                from: ItemState::Dirty,
                to: ItemState::Synced,
                ..
            }
        ),
        "got {err:?}"
    );
    assert_eq!(db.get_item(&key).expect("get"), Some(before));
}

#[test]
fn transition_on_missing_item_is_stale_with_found_none() {
    let (_dir, path) = scratch();
    let db = open_fresh(&path);
    let err = db
        .transition(
            &rel("ghost.dng"),
            ItemState::Dirty,
            ItemState::Queued,
            |_| {},
        )
        .expect_err("missing item must refuse");
    assert!(
        matches!(err, StateError::StaleState { found: None, .. }),
        "got {err:?}"
    );
}

#[test]
fn concurrent_same_transition_exactly_one_wins() {
    // Pinned semantics: redb serializes write txns, so racing transitions
    // execute in some order; with the same expected_from exactly one
    // succeeds and the loser reads the winner's committed state ->
    // StaleState. Repeat to make the race real.
    let (_dir, path) = scratch();
    let db = open_fresh(&path);
    for i in 0..20 {
        let key = rel(&format!("race-{i}.rrdata"));
        db.insert_item(&key, &bare_record(ItemState::Dirty))
            .expect("put");
        let barrier = std::sync::Barrier::new(2);
        let (r1, r2) = std::thread::scope(|s| {
            let t1 = s.spawn(|| {
                barrier.wait();
                db.transition(&key, ItemState::Dirty, ItemState::Queued, |_| {})
            });
            let t2 = s.spawn(|| {
                barrier.wait();
                db.transition(&key, ItemState::Dirty, ItemState::Queued, |_| {})
            });
            (t1.join().expect("no panic"), t2.join().expect("no panic"))
        });
        let oks = [r1.is_ok(), r2.is_ok()].iter().filter(|&&b| b).count();
        assert_eq!(
            oks, 1,
            "exactly one winner (iteration {i}): {r1:?} / {r2:?}"
        );
        let loser = if r1.is_ok() { r2 } else { r1 };
        assert!(
            matches!(
                loser,
                Err(StateError::StaleState {
                    expected: ItemState::Dirty,
                    found: Some(ItemState::Queued),
                    ..
                })
            ),
            "loser must observe the winner's state, got {loser:?}"
        );
        let got = db.get_item(&key).expect("get").expect("exists");
        assert_eq!(got.state, ItemState::Queued);
    }
}

#[test]
fn concurrent_sequential_transitions_both_succeed_when_both_legal() {
    // The other half of the pinned race semantics: a losing-ordered
    // transition whose expected_from matches the winner's committed state
    // succeeds (serialized success, not an error).
    let (_dir, path) = scratch();
    let db = open_fresh(&path);
    let key = rel("seq.rrdata");
    db.insert_item(&key, &bare_record(ItemState::Dirty))
        .expect("put");
    db.transition(&key, ItemState::Dirty, ItemState::Queued, |_| {})
        .expect("first");
    db.transition(&key, ItemState::Queued, ItemState::Uploading, |_| {})
        .expect("second");
    let got = db.get_item(&key).expect("get").expect("exists");
    assert_eq!(got.state, ItemState::Uploading);
}

// ---------------------------------------------------------------------------
// allocate_seq / freeze_segment / unpublished_segments / mark_published
// (§2.1.5, §2.2 monotonicity)
// ---------------------------------------------------------------------------

#[test]
fn allocate_seq_starts_at_one_and_is_strictly_increasing() {
    let (_dir, path) = scratch();
    let db = open_fresh(&path);
    assert_eq!(db.last_allocated_seq().expect("last"), 0);
    let s1 = db.allocate_seq().expect("alloc");
    let s2 = db.allocate_seq().expect("alloc");
    let s3 = db.allocate_seq().expect("alloc");
    assert_eq!(s1, 1, "first allocation is 1");
    assert!(s1 < s2 && s2 < s3, "strictly increasing: {s1} {s2} {s3}");
    assert_eq!(db.last_allocated_seq().expect("last"), s3);
}

#[test]
fn allocate_seq_never_regresses_across_reopen() {
    let (_dir, path) = scratch();
    let last = {
        let db = open_fresh(&path);
        let mut last = 0;
        for _ in 0..10 {
            last = db.allocate_seq().expect("alloc");
        }
        last
    };
    let db = SyncDb::open(&path, None).expect("reopen");
    assert_eq!(db.last_allocated_seq().expect("last"), last);
    let next = db.allocate_seq().expect("alloc after reopen");
    assert!(next > last, "post-reopen seq {next} must exceed {last}");
}

#[test]
fn allocate_seq_unique_across_threads() {
    let (_dir, path) = scratch();
    let db = open_fresh(&path);
    let mut all: Vec<u64> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..4)
            .map(|_| {
                s.spawn(|| {
                    (0..25)
                        .map(|_| db.allocate_seq().expect("alloc"))
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        handles
            .into_iter()
            .flat_map(|h| h.join().expect("no panic"))
            .collect()
    });
    all.sort_unstable();
    let before = all.len();
    all.dedup();
    assert_eq!(all.len(), before, "no seq handed out twice");
    assert_eq!(all.len(), 100);
    assert_eq!(
        db.last_allocated_seq().expect("last"),
        *all.last().expect("nonempty")
    );
}

#[test]
fn freeze_then_reopen_returns_byte_identical_segments_in_seq_order() {
    let (_dir, path) = scratch();
    // Bytes with embedded NUL, newline, invalid-UTF8 — byte identity, not
    // string identity.
    let b1: Vec<u8> = vec![0x00, 0xff, b'\n', 0x80, 0x01];
    let b2: Vec<u8> = b"{\"v\":1,\"seq\":2}\n".to_vec();
    let b3: Vec<u8> = (0..=255u8).collect();
    let (s1, s2, s3) = {
        let db = open_fresh(&path);
        let s1 = db.allocate_seq().expect("alloc");
        let s2 = db.allocate_seq().expect("alloc");
        let s3 = db.allocate_seq().expect("alloc");
        // Freeze out of allocation order: returned order is seq order.
        db.freeze_segment(s2, &b2).expect("freeze s2");
        db.freeze_segment(s1, &b1).expect("freeze s1");
        db.freeze_segment(s3, &b3).expect("freeze s3");
        (s1, s2, s3)
    };
    // Crash-before-publish modeled as drop + reopen (the real SIGKILL
    // variant lives in state_crash.rs).
    let db = SyncDb::open(&path, None).expect("reopen");
    let segs = db.unpublished_segments().expect("unpublished");
    assert_eq!(
        segs,
        vec![(s1, b1.clone()), (s2, b2.clone()), (s3, b3.clone())],
        "byte-identical, ascending seq order"
    );
    // Publish the first; the rest remain.
    db.mark_published(s1).expect("publish s1");
    assert_eq!(
        db.unpublished_segments().expect("unpublished"),
        vec![(s2, b2.clone()), (s3, b3.clone())]
    );
    assert_eq!(db.published_cursor().expect("cursor"), s1);
    // Idempotent re-publish.
    db.mark_published(s1).expect("re-publish s1 is a no-op");
    assert_eq!(db.published_cursor().expect("cursor"), s1);
    // Out-of-order publish is allowed; cursor is the max, and the earlier
    // unpublished segment s2 stays visible.
    db.mark_published(s3).expect("publish s3");
    assert_eq!(db.published_cursor().expect("cursor"), s3);
    assert_eq!(
        db.unpublished_segments().expect("unpublished"),
        vec![(s2, b2.clone())]
    );
    db.mark_published(s2).expect("publish s2");
    assert_eq!(db.unpublished_segments().expect("unpublished"), vec![]);
    assert_eq!(
        db.published_cursor().expect("cursor"),
        s3,
        "cursor stays at max"
    );
    // Publication state survives reopen.
    drop(db);
    let db = SyncDb::open(&path, None).expect("reopen 2");
    assert_eq!(db.unpublished_segments().expect("unpublished"), vec![]);
    assert_eq!(db.published_cursor().expect("cursor"), s3);
}

#[test]
fn freeze_of_unallocated_seq_is_typed_error() {
    let (_dir, path) = scratch();
    let db = open_fresh(&path);
    let err = db.freeze_segment(7, b"x").expect_err("unallocated seq");
    assert!(
        matches!(err, StateError::SeqNotAllocated { seq: 7 }),
        "got {err:?}"
    );
}

#[test]
fn double_freeze_is_typed_error_and_keeps_original_bytes() {
    let (_dir, path) = scratch();
    let db = open_fresh(&path);
    let s = db.allocate_seq().expect("alloc");
    db.freeze_segment(s, b"original").expect("freeze");
    let err = db.freeze_segment(s, b"imposter").expect_err("re-freeze");
    assert!(
        matches!(err, StateError::AlreadyFrozen { seq } if seq == s),
        "got {err:?}"
    );
    assert_eq!(
        db.unpublished_segments().expect("unpublished"),
        vec![(s, b"original".to_vec())],
        "frozen bytes are immutable"
    );
}

#[test]
fn mark_published_of_unfrozen_seq_is_typed_error() {
    let (_dir, path) = scratch();
    let db = open_fresh(&path);
    // Never allocated:
    let err = db.mark_published(9).expect_err("unfrozen seq");
    assert!(
        matches!(err, StateError::NotFrozen { seq: 9 }),
        "got {err:?}"
    );
    // Allocated but never frozen:
    let s = db.allocate_seq().expect("alloc");
    let err = db.mark_published(s).expect_err("allocated-not-frozen seq");
    assert!(
        matches!(err, StateError::NotFrozen { seq } if seq == s),
        "got {err:?}"
    );
    assert_eq!(
        db.published_cursor().expect("cursor"),
        0,
        "cursor untouched"
    );
}

// ---------------------------------------------------------------------------
// applied / cursors (§2.2 idempotent apply)
// ---------------------------------------------------------------------------

#[test]
fn mark_applied_is_idempotent_and_survives_reopen() {
    let (_dir, path) = scratch();
    let d2 = dev(DEV2);
    {
        let db = open_fresh(&path);
        assert!(!db.has_applied(&d2, 5).expect("has"));
        db.mark_applied(&d2, 5).expect("mark");
        assert!(db.has_applied(&d2, 5).expect("has"));
        db.mark_applied(&d2, 5).expect("re-mark is a no-op");
        assert!(db.has_applied(&d2, 5).expect("has"));
        // Independent per (device, seq).
        assert!(!db.has_applied(&d2, 6).expect("has"));
        assert!(!db.has_applied(&dev(DEV1), 5).expect("has"));
    }
    let db = SyncDb::open(&path, None).expect("reopen");
    assert!(db.has_applied(&d2, 5).expect("has after reopen"));
    assert!(!db.has_applied(&d2, 6).expect("has after reopen"));
}

#[test]
fn cursors_default_zero_set_and_survive_reopen() {
    let (_dir, path) = scratch();
    let d2 = dev(DEV2);
    {
        let db = open_fresh(&path);
        assert_eq!(db.cursor(&d2).expect("cursor"), 0, "default is 0");
        db.set_cursor(&d2, 412).expect("set");
        assert_eq!(db.cursor(&d2).expect("cursor"), 412);
        db.set_cursor(&d2, 500).expect("advance");
    }
    let db = SyncDb::open(&path, None).expect("reopen");
    assert_eq!(db.cursor(&d2).expect("cursor"), 500);
    assert_eq!(db.cursor(&dev(DEV1)).expect("other cursor"), 0);
}

#[test]
fn segment_spans_record_and_survive_reopen() {
    let (_dir, path) = scratch();
    let d2 = dev(DEV2);
    {
        let db = open_fresh(&path);
        assert_eq!(
            db.segment_span(&d2, 1).expect("span"),
            None,
            "unknown span reads as None"
        );
        db.set_segment_span(&d2, 1, 4).expect("set");
        db.set_segment_span(&d2, 5, 5)
            .expect("set single-entry span");
        assert_eq!(db.segment_span(&d2, 1).expect("span"), Some(4));
        assert_eq!(db.segment_span(&d2, 5).expect("span"), Some(5));
        // Keyed per (device, first_seq).
        assert_eq!(db.segment_span(&dev(DEV1), 1).expect("span"), None);
        assert_eq!(db.segment_span(&d2, 2).expect("span"), None);
    }
    let db = SyncDb::open(&path, None).expect("reopen");
    assert_eq!(db.segment_span(&d2, 1).expect("span after reopen"), Some(4));
    assert_eq!(db.segment_span(&d2, 5).expect("span after reopen"), Some(5));
}

// ---------------------------------------------------------------------------
// Multipart upload resume state (§2.4)
// ---------------------------------------------------------------------------

#[test]
fn upload_state_and_parts_roundtrip_across_reopen() {
    let (_dir, path) = scratch();
    let key = rel("2026/10/IMG_0042.NEF");
    let st = MultipartUploadState {
        upload_id: "2~abcdef0123".to_string(),
        part_size: 16 * 1024 * 1024,
        started_unix: 1_769_900_000,
        size: 0,
        mtime_unix_ns: 0,
    };
    let p1 = UploadPart {
        etag: "\"9bb58f26192e4ba00f01e2e7b136bbd8\"".to_string(),
        md5_b64: "m7WPJhkuS6APAeLnsTa72A==".to_string(),
    };
    let p3 = UploadPart {
        etag: "\"00000000000000000000000000000003\"".to_string(),
        md5_b64: "AAAAAAAAAAAAAAAAAAAAAw==".to_string(),
    };
    {
        let db = open_fresh(&path);
        assert_eq!(db.get_upload(&key).expect("get"), None);
        db.set_upload(&key, &st).expect("set");
        // Record out of order; read-back is part-number ordered.
        db.record_upload_part(&key, 3, &p3).expect("part 3");
        db.record_upload_part(&key, 1, &p1).expect("part 1");
    }
    let db = SyncDb::open(&path, None).expect("reopen");
    assert_eq!(db.get_upload(&key).expect("get"), Some(st));
    assert_eq!(
        db.upload_parts(&key).expect("parts"),
        vec![(1, p1.clone()), (3, p3.clone())]
    );
    // Re-recording a part replaces it (resume re-uploaded it).
    let p3b = UploadPart {
        etag: "\"replacement\"".to_string(),
        md5_b64: "AAAAAAAAAAAAAAAAAAAABB==".to_string(),
    };
    db.record_upload_part(&key, 3, &p3b)
        .expect("replace part 3");
    assert_eq!(
        db.upload_parts(&key).expect("parts"),
        vec![(1, p1), (3, p3b)]
    );
}

#[test]
fn clear_upload_removes_state_and_parts_together() {
    let (_dir, path) = scratch();
    let key = rel("a.NEF");
    let other = rel("b.NEF");
    let db = open_fresh(&path);
    let st = MultipartUploadState {
        upload_id: "u".to_string(),
        part_size: 1,
        started_unix: 0,
        size: 0,
        mtime_unix_ns: 0,
    };
    let part = UploadPart {
        etag: "e".to_string(),
        md5_b64: "m".to_string(),
    };
    db.set_upload(&key, &st).expect("set");
    db.set_upload(&other, &st).expect("set other");
    db.record_upload_part(&key, 1, &part).expect("part");
    db.record_upload_part(&other, 1, &part).expect("other part");
    db.clear_upload(&key).expect("clear");
    assert_eq!(db.get_upload(&key).expect("get"), None);
    assert_eq!(db.upload_parts(&key).expect("parts"), vec![]);
    // Unrelated relkey untouched.
    assert_eq!(db.get_upload(&other).expect("get other"), Some(st));
    assert_eq!(db.upload_parts(&other).expect("other parts").len(), 1);
    // Idempotent.
    db.clear_upload(&key).expect("clear again");
}

// ---------------------------------------------------------------------------
// Queues: priority classes, FIFO within class, idempotent push, reopen
// ---------------------------------------------------------------------------

#[test]
fn queue_pops_lower_class_first_then_fifo_within_class() {
    let (_dir, path) = scratch();
    let db = open_fresh(&path);
    db.queue_push(Queue::Up, &rel("b1.rrdata"), 1)
        .expect("push");
    db.queue_push(Queue::Up, &rel("b2.rrdata"), 1)
        .expect("push");
    db.queue_push(Queue::Up, &rel("a1.rrdata"), 0)
        .expect("push");
    db.queue_push(Queue::Up, &rel("c1.rrdata"), 2)
        .expect("push");
    db.queue_push(Queue::Up, &rel("a2.rrdata"), 0)
        .expect("push");
    assert_eq!(db.queue_len(Queue::Up).expect("len"), 5);
    let popped: Vec<_> = std::iter::from_fn(|| db.queue_pop(Queue::Up).expect("pop")).collect();
    assert_eq!(
        popped,
        vec![
            (rel("a1.rrdata"), 0),
            (rel("a2.rrdata"), 0),
            (rel("b1.rrdata"), 1),
            (rel("b2.rrdata"), 1),
            (rel("c1.rrdata"), 2),
        ],
        "class ascending, FIFO within class"
    );
    assert_eq!(db.queue_len(Queue::Up).expect("len"), 0);
    assert_eq!(db.queue_pop(Queue::Up).expect("pop empty"), None);
}

#[test]
fn queue_repush_is_noop_even_with_different_class() {
    let (_dir, path) = scratch();
    let db = open_fresh(&path);
    assert!(db.queue_push(Queue::Up, &rel("x.rrdata"), 1).expect("push"));
    assert!(db.queue_push(Queue::Up, &rel("y.rrdata"), 1).expect("push"));
    // Re-push same class: no-op, keeps position behind nothing new.
    assert!(!db
        .queue_push(Queue::Up, &rel("x.rrdata"), 1)
        .expect("repush"));
    // Re-push different (even more urgent) class: still a no-op — pinned.
    assert!(!db
        .queue_push(Queue::Up, &rel("y.rrdata"), 0)
        .expect("repush class 0"));
    assert_eq!(db.queue_len(Queue::Up).expect("len"), 2);
    assert_eq!(
        db.queue_pop(Queue::Up).expect("pop"),
        Some((rel("x.rrdata"), 1)),
        "x kept its original slot"
    );
    assert_eq!(
        db.queue_pop(Queue::Up).expect("pop"),
        Some((rel("y.rrdata"), 1)),
        "y kept its original class and slot"
    );
    // After popping, a push re-enqueues fresh.
    assert!(db
        .queue_push(Queue::Up, &rel("x.rrdata"), 0)
        .expect("fresh push"));
    assert_eq!(
        db.queue_pop(Queue::Up).expect("pop"),
        Some((rel("x.rrdata"), 0))
    );
}

#[test]
fn queue_fifo_order_survives_reopen() {
    let (_dir, path) = scratch();
    {
        let db = open_fresh(&path);
        db.queue_push(Queue::Down, &rel("one.NEF"), 3)
            .expect("push");
        db.queue_push(Queue::Down, &rel("two.NEF"), 3)
            .expect("push");
    }
    let db = SyncDb::open(&path, None).expect("reopen");
    // Arrivals after reopen stay behind pre-reopen arrivals of the same
    // class: the arrival counter is persisted, not in-memory.
    db.queue_push(Queue::Down, &rel("three.NEF"), 3)
        .expect("push");
    assert_eq!(
        db.queue_pop(Queue::Down).expect("pop"),
        Some((rel("one.NEF"), 3))
    );
    assert_eq!(
        db.queue_pop(Queue::Down).expect("pop"),
        Some((rel("two.NEF"), 3))
    );
    assert_eq!(
        db.queue_pop(Queue::Down).expect("pop"),
        Some((rel("three.NEF"), 3))
    );
}

#[test]
fn queues_are_independent_and_remove_works() {
    let (_dir, path) = scratch();
    let db = open_fresh(&path);
    let key = rel("both.rrdata");
    assert!(db.queue_push(Queue::Up, &key, 0).expect("push up"));
    assert!(
        db.queue_push(Queue::Down, &key, 1).expect("push down"),
        "same relkey may sit in both queues"
    );
    assert!(db.queue_remove(Queue::Up, &key).expect("remove up"));
    assert!(
        !db.queue_remove(Queue::Up, &key).expect("remove again"),
        "idempotent"
    );
    assert_eq!(db.queue_pop(Queue::Up).expect("pop up"), None);
    assert_eq!(db.queue_pop(Queue::Down).expect("pop down"), Some((key, 1)));
}

// ---------------------------------------------------------------------------
// Change-detection caches (§2.5) and meta
// ---------------------------------------------------------------------------

#[test]
fn xmp_seen_roundtrip_across_reopen() {
    let (_dir, path) = scratch();
    let key = rel("a.NEF");
    let hash = Blake3Hex::parse(BLAKE3_HEX).expect("hash");
    {
        let db = open_fresh(&path);
        assert_eq!(db.xmp_seen(&key).expect("get"), None);
        db.set_xmp_seen(&key, &hash).expect("set");
        assert_eq!(db.xmp_seen(&key).expect("get"), Some(hash.clone()));
    }
    let db = SyncDb::open(&path, None).expect("reopen");
    assert_eq!(db.xmp_seen(&key).expect("get"), Some(hash));
}

#[test]
fn dcim_seen_keys_on_path_size_and_mtime() {
    let (_dir, path) = scratch();
    let db = open_fresh(&path);
    let cid = ContentId::parse(CONTENT_HEX).expect("cid");
    db.set_dcim_seen("/dcim/IMG_1.jpg", 100, 1_769_000_000, &cid)
        .expect("set");
    assert_eq!(
        db.dcim_seen("/dcim/IMG_1.jpg", 100, 1_769_000_000)
            .expect("get"),
        Some(cid)
    );
    // Any component differing = a different source file = a miss.
    assert_eq!(
        db.dcim_seen("/dcim/IMG_1.jpg", 101, 1_769_000_000)
            .expect("get"),
        None
    );
    assert_eq!(
        db.dcim_seen("/dcim/IMG_1.jpg", 100, 1_769_000_001)
            .expect("get"),
        None
    );
    assert_eq!(
        db.dcim_seen("/dcim/IMG_2.jpg", 100, 1_769_000_000)
            .expect("get"),
        None
    );
}

#[test]
fn server_time_offset_roundtrip_across_reopen() {
    let (_dir, path) = scratch();
    {
        let db = open_fresh(&path);
        assert_eq!(db.server_time_offset_ms().expect("get"), None);
        db.set_server_time_offset_ms(-1500).expect("set");
        assert_eq!(db.server_time_offset_ms().expect("get"), Some(-1500));
    }
    let db = SyncDb::open(&path, None).expect("reopen");
    assert_eq!(db.server_time_offset_ms().expect("get"), Some(-1500));
}

// ---------------------------------------------------------------------------
// Thread-safety contract
// ---------------------------------------------------------------------------

#[test]
fn syncdb_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<SyncDb>();
}

// ---------------------------------------------------------------------------
// Schema gate is keyed on the schema stamp, not device-id presence
// ---------------------------------------------------------------------------

#[test]
fn newer_schema_without_device_id_is_rejected_and_never_restamped() {
    // The bypass probe from review: a db whose meta carries a NEWER schema
    // stamp but no device identity (future layout moved identity, or
    // targeted corruption) must refuse SchemaTooNew — never be treated as
    // fresh, never mint, never overwrite the stamp back to v1.
    let (_dir, path) = scratch();
    let newer = rrcloud_core::state::SCHEMA_VERSION + 98;
    {
        let db = open_fresh(&path);
        db.force_schema_version(Some(newer)).expect("test override");
        db.force_remove_device_id().expect("test override");
    }
    // Even an open that supplies an id (the fresh-open calling convention)
    // must not mint.
    let err = SyncDb::open(&path, Some(dev(DEV2))).expect_err("newer schema must refuse");
    assert!(
        matches!(err, StateError::SchemaTooNew { found, .. } if found == newer),
        "got {err:?}"
    );
    // And the refusal re-stamped nothing: a second open still sees the
    // newer version (the v1 binary left the newer-format file untouched).
    let err = SyncDb::open(&path, None).expect_err("still refuses");
    assert!(
        matches!(err, StateError::SchemaTooNew { found, .. } if found == newer),
        "stamp must be untouched, got {err:?}"
    );
}

#[test]
fn matching_schema_without_device_id_is_corruption_not_a_mint() {
    let (_dir, path) = scratch();
    {
        let db = open_fresh(&path);
        db.force_remove_device_id().expect("test override");
    }
    let err = SyncDb::open(&path, Some(dev(DEV2))).expect_err("missing identity must refuse");
    assert!(matches!(err, StateError::DeviceIdMissing), "got {err:?}");
    let err = SyncDb::open(&path, None).expect_err("missing identity must refuse");
    assert!(matches!(err, StateError::DeviceIdMissing), "got {err:?}");
}

// ---------------------------------------------------------------------------
// insert_item / replay_put_item split (the state-machine bypass is explicit)
// ---------------------------------------------------------------------------

#[test]
fn insert_item_refuses_to_overwrite_existing_record() {
    let (_dir, path) = scratch();
    let key = rel("a.rrdata");
    let db = open_fresh(&path);
    let original = bare_record(ItemState::Hydrated);
    assert!(db.insert_item(&key, &original).expect("insert"));
    let mut imposter = bare_record(ItemState::Dirty);
    imposter.size = 999;
    assert!(
        !db.insert_item(&key, &imposter).expect("second insert"),
        "insert on existing key must be a refused no-op"
    );
    assert_eq!(
        db.get_item(&key).expect("get"),
        Some(original),
        "existing record (incl. its state) must be untouched"
    );
}

#[test]
fn replay_put_item_wholesale_replaces_including_state() {
    // The ingest/replay bypass: wholesale replacement with no legality
    // check, under its greppable name.
    let (_dir, path) = scratch();
    let key = rel("a.rrdata");
    let db = open_fresh(&path);
    db.replay_put_item(&key, &bare_record(ItemState::Dirty))
        .expect("insert via replay");
    let replacement = full_record();
    db.replay_put_item(&key, &replacement).expect("replace");
    assert_eq!(db.get_item(&key).expect("get"), Some(replacement));
}

// ---------------------------------------------------------------------------
// update_item: CAS-guarded state-preserving mutation
// ---------------------------------------------------------------------------

#[test]
fn update_item_mutates_fields_and_preserves_state_durably() {
    let (_dir, path) = scratch();
    let key = rel("a.rrdata");
    {
        let db = open_fresh(&path);
        db.replay_put_item(&key, &bare_record(ItemState::Synced))
            .expect("insert");
        let committed = db
            .update_item(&key, ItemState::Synced, |r| {
                r.attested = true;
                r.last_access_unix = 777;
                r.state = ItemState::Dirty; // must NOT control state
            })
            .expect("update");
        assert_eq!(committed.state, ItemState::Synced, "state preserved");
        assert!(committed.attested);
        assert_eq!(
            db.get_item(&key).expect("get"),
            Some(committed.clone()),
            "returned record is the committed record"
        );
    }
    let db = SyncDb::open(&path, None).expect("reopen");
    let got = db.get_item(&key).expect("get").expect("exists");
    assert_eq!(got.state, ItemState::Synced);
    assert!(got.attested);
    assert_eq!(got.last_access_unix, 777);
}

#[test]
fn update_item_with_wrong_expected_state_is_stale_and_mutates_nothing() {
    let (_dir, path) = scratch();
    let key = rel("a.rrdata");
    let db = open_fresh(&path);
    let before = bare_record(ItemState::Dirty);
    db.insert_item(&key, &before).expect("insert");
    let err = db
        .update_item(&key, ItemState::Synced, |r| r.size = 999)
        .expect_err("stale expected state must refuse");
    assert!(
        matches!(
            err,
            StateError::StaleState {
                expected: ItemState::Synced,
                found: Some(ItemState::Dirty),
                ..
            }
        ),
        "got {err:?}"
    );
    assert_eq!(db.get_item(&key).expect("get"), Some(before));
    // Missing record: StaleState with found: None.
    let err = db
        .update_item(&rel("ghost.dng"), ItemState::Synced, |_| {})
        .expect_err("missing item must refuse");
    assert!(
        matches!(err, StateError::StaleState { found: None, .. }),
        "got {err:?}"
    );
}

#[test]
fn update_item_racing_a_transition_never_loses_the_transition() {
    // The exact lost-update class the review named: the chokepoint's
    // Synced→Dirty landing concurrently with the apply loop's
    // state-preserving metadata adoption. Pinned semantics: the transition
    // always wins eventually (if the update ran first, the state was still
    // Synced; if the transition ran first, the update observes Dirty and
    // gets StaleState — it can re-read, never silently stomp).
    let (_dir, path) = scratch();
    let db = open_fresh(&path);
    for i in 0..20 {
        let key = rel(&format!("race-up-{i}.rrdata"));
        db.replay_put_item(&key, &bare_record(ItemState::Synced))
            .expect("insert");
        let barrier = std::sync::Barrier::new(2);
        let (t_res, u_res) = std::thread::scope(|s| {
            let t = s.spawn(|| {
                barrier.wait();
                db.transition(&key, ItemState::Synced, ItemState::Dirty, |r| r.size = 1)
            });
            let u = s.spawn(|| {
                barrier.wait();
                db.update_item(&key, ItemState::Synced, |r| r.attested = true)
            });
            (t.join().expect("no panic"), u.join().expect("no panic"))
        });
        let dirty = t_res.expect("the legal transition must always succeed");
        assert_eq!(dirty.state, ItemState::Dirty);
        let got = db.get_item(&key).expect("get").expect("exists");
        assert_eq!(got.state, ItemState::Dirty, "transition never lost");
        match u_res {
            // Update won the serialization race: its mutation is in the
            // final record (the transition carried it forward).
            Ok(updated) => {
                assert_eq!(updated.state, ItemState::Synced);
                assert!(got.attested, "update's mutation must not be lost");
            }
            // Transition won: update observed the new state and refused.
            Err(StateError::StaleState {
                expected: ItemState::Synced,
                found: Some(ItemState::Dirty),
                ..
            }) => {
                assert!(!got.attested);
            }
            other => panic!("unexpected update outcome: {other:?}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Enumeration / scan accessors (crash recovery, hygiene, status counts)
// ---------------------------------------------------------------------------

#[test]
fn iter_items_and_items_in_state_scan_everything() {
    let (_dir, path) = scratch();
    let dirty1 = rel("a/dirty1.rrdata");
    let dirty2 = rel("b/dirty2.rrdata");
    let uploading = rel("c/uploading.rrdata");
    {
        let db = open_fresh(&path);
        db.insert_item(&dirty1, &bare_record(ItemState::Dirty))
            .expect("insert");
        db.insert_item(&dirty2, &bare_record(ItemState::Dirty))
            .expect("insert");
        db.replay_put_item(&uploading, &bare_record(ItemState::Uploading))
            .expect("insert");
    }
    // Scans see committed state across reopen — the post-crash "find the
    // item that is in no queue" path.
    let db = SyncDb::open(&path, None).expect("reopen");
    let all = db.iter_items().expect("iter");
    assert_eq!(
        all.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>(),
        vec![dirty1.clone(), dirty2.clone(), uploading.clone()],
        "ascending by relkey"
    );
    let dirty = db.items_in_state(ItemState::Dirty).expect("in state");
    assert_eq!(
        dirty.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>(),
        vec![dirty1.clone(), dirty2.clone()]
    );
    let stuck = db.items_in_state(ItemState::Uploading).expect("in state");
    assert_eq!(stuck.len(), 1);
    assert_eq!(stuck[0].0, uploading);
    assert_eq!(db.count_in_state(ItemState::Dirty).expect("count"), 2);
    assert_eq!(db.count_in_state(ItemState::Uploading).expect("count"), 1);
    assert_eq!(db.count_in_state(ItemState::Synced).expect("count"), 0);
    assert!(db
        .items_in_state(ItemState::Conflict)
        .expect("none")
        .is_empty());
}

#[test]
fn iter_uploads_scans_all_inflight_multiparts() {
    let (_dir, path) = scratch();
    let k1 = rel("a.NEF");
    let k2 = rel("b.NEF");
    let st = |id: &str| MultipartUploadState {
        upload_id: id.to_string(),
        part_size: 16 * 1024 * 1024,
        started_unix: 1_000,
        size: 0,
        mtime_unix_ns: 0,
    };
    {
        let db = open_fresh(&path);
        db.set_upload(&k2, &st("u2")).expect("set");
        db.set_upload(&k1, &st("u1")).expect("set");
    }
    let db = SyncDb::open(&path, None).expect("reopen");
    // The §2.4 stale-upload hygiene scan: every in-flight upload_id is
    // discoverable without knowing its relkey.
    assert_eq!(
        db.iter_uploads().expect("iter"),
        vec![(k1.clone(), st("u1")), (k2.clone(), st("u2"))]
    );
    db.clear_upload(&k1).expect("clear");
    assert_eq!(db.iter_uploads().expect("iter"), vec![(k2, st("u2"))]);
}

// ---------------------------------------------------------------------------
// Codec error path: corrupt stored bytes surface typed, never panic
// ---------------------------------------------------------------------------

#[test]
fn corrupt_stored_record_surfaces_codec_error_not_panic() {
    let (_dir, path) = scratch();
    let good = rel("good.rrdata");
    let bad = rel("bad.rrdata");
    let db = open_fresh(&path);
    db.insert_item(&good, &bare_record(ItemState::Dirty))
        .expect("insert");
    db.force_corrupt_item(&bad, b"\xff\x00 not json at all")
        .expect("corrupt");
    let err = db.get_item(&bad).expect_err("corrupt value must refuse");
    assert!(matches!(err, StateError::Codec(_)), "got {err:?}");
    // Scans hit the corrupt row too — still typed, still no panic, and
    // the enumeration error names the offending key (round 3: a point
    // read's caller already holds the key; a scan's caller has no other
    // way to learn it).
    let err = db.iter_items().expect_err("scan over corrupt value");
    assert!(
        matches!(&err, StateError::CodecAt { key, .. } if key == bad.as_str()),
        "got {err:?}"
    );
    // Unrelated keys stay readable.
    assert!(db.get_item(&good).expect("get good").is_some());
}

// ---------------------------------------------------------------------------
// freeze_next_segment: allocation + bytes in ONE committed transaction
// ---------------------------------------------------------------------------

#[test]
fn freeze_next_segment_allocates_and_freezes_atomically() {
    let (_dir, path) = scratch();
    let (s1, s2) = {
        let db = open_fresh(&path);
        let s1 = db
            .freeze_next_segment(1, |seq| Ok(format!("segment-{seq}").into_bytes()))
            .expect("freeze");
        assert_eq!(s1, 1, "first allocation is 1");
        assert_eq!(db.last_allocated_seq().expect("last"), s1);
        let s2 = db
            .freeze_next_segment(1, |seq| Ok(format!("segment-{seq}").into_bytes()))
            .expect("freeze");
        assert_eq!(s2, s1 + 1);
        (s1, s2)
    };
    let db = SyncDb::open(&path, None).expect("reopen");
    assert_eq!(
        db.unpublished_segments().expect("unpublished"),
        vec![(s1, b"segment-1".to_vec()), (s2, b"segment-2".to_vec()),],
        "builder-produced bytes stored byte-identically under their seq"
    );
    // Interleaves with the legacy pair on the same counter.
    let s3 = db.allocate_seq().expect("alloc");
    assert_eq!(s3, s2 + 1);
    let s4 = db
        .freeze_next_segment(1, |seq| Ok(vec![seq as u8]))
        .expect("freeze");
    assert_eq!(s4, s3 + 1);
}

#[test]
fn freeze_next_segment_unique_across_threads() {
    let (_dir, path) = scratch();
    let db = open_fresh(&path);
    let mut all: Vec<u64> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..4)
            .map(|_| {
                s.spawn(|| {
                    (0..25)
                        .map(|_| {
                            db.freeze_next_segment(1, |seq| Ok(seq.to_le_bytes().to_vec()))
                                .expect("freeze")
                        })
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        handles
            .into_iter()
            .flat_map(|h| h.join().expect("no panic"))
            .collect()
    });
    all.sort_unstable();
    let before = all.len();
    all.dedup();
    assert_eq!(all.len(), before, "no seq handed out twice");
    assert_eq!(all.len(), 100);
    // Every allocated seq has its bytes: no holes, by construction.
    let segs = db.unpublished_segments().expect("unpublished");
    assert_eq!(segs.len(), 100);
    for (seq, bytes) in segs {
        assert_eq!(bytes, seq.to_le_bytes().to_vec());
    }
}

// ---------------------------------------------------------------------------
// unpublished_segments floor scan stays correct with holes + odd orders
// ---------------------------------------------------------------------------

#[test]
fn unpublished_scan_survives_holes_and_out_of_order_publish() {
    let (_dir, path) = scratch();
    let db = open_fresh(&path);
    // s1 allocated but never frozen (legacy-pair hole), s2..s4 frozen.
    let s1 = db.allocate_seq().expect("alloc");
    let s2 = db.allocate_seq().expect("alloc");
    let s3 = db.allocate_seq().expect("alloc");
    let s4 = db.allocate_seq().expect("alloc");
    db.freeze_segment(s2, b"two").expect("freeze");
    db.freeze_segment(s3, b"three").expect("freeze");
    db.freeze_segment(s4, b"four").expect("freeze");
    // Publish out of order, leaving s3 unpublished behind the cursor.
    db.mark_published(s4).expect("publish");
    db.mark_published(s2).expect("publish");
    assert_eq!(
        db.unpublished_segments().expect("unpublished"),
        vec![(s3, b"three".to_vec())],
        "an unpublished seq below the published cursor must stay visible"
    );
    db.mark_published(s3).expect("publish");
    assert_eq!(db.unpublished_segments().expect("unpublished"), vec![]);
    // A LATE freeze of the hole seq must become visible (the floor can
    // never have skipped past an unpublished frozen segment).
    db.freeze_segment(s1, b"one-late").expect("late freeze");
    assert_eq!(
        db.unpublished_segments().expect("unpublished"),
        vec![(s1, b"one-late".to_vec())]
    );
    db.mark_published(s1).expect("publish");
    assert_eq!(db.unpublished_segments().expect("unpublished"), vec![]);
    // And new work after full publication is seen.
    let s5 = db
        .freeze_next_segment(1, |_| Ok(b"five".to_vec()))
        .expect("freeze");
    assert_eq!(
        db.unpublished_segments().expect("unpublished"),
        vec![(s5, b"five".to_vec())]
    );
    // All of it durable.
    drop(db);
    let db = SyncDb::open(&path, None).expect("reopen");
    assert_eq!(
        db.unpublished_segments().expect("unpublished"),
        vec![(s5, b"five".to_vec())]
    );
}

// ---------------------------------------------------------------------------
// Counter overflow: typed error, no panic, no wrap
// ---------------------------------------------------------------------------

#[test]
fn saturated_seq_counter_is_typed_error_not_panic_or_wrap() {
    let (_dir, path) = scratch();
    let db = open_fresh(&path);
    db.force_last_seq(u64::MAX).expect("tamper");
    let err = db.allocate_seq().expect_err("must refuse to wrap");
    assert!(
        matches!(err, StateError::CounterSaturated { .. }),
        "got {err:?}"
    );
    let err = db
        .freeze_next_segment(1, |_| Ok(vec![]))
        .expect_err("must refuse to wrap");
    assert!(
        matches!(err, StateError::CounterSaturated { .. }),
        "got {err:?}"
    );
    // No wrap: the counter still reads MAX, and no segment appeared.
    assert_eq!(db.last_allocated_seq().expect("last"), u64::MAX);
    assert_eq!(db.unpublished_segments().expect("unpublished"), vec![]);
}

#[test]
fn saturated_queue_arrival_counter_is_typed_error() {
    let (_dir, path) = scratch();
    let db = open_fresh(&path);
    db.force_queue_arrival(u64::MAX).expect("tamper");
    let err = db
        .queue_push(Queue::Up, &rel("x.rrdata"), 0)
        .expect_err("must refuse to wrap");
    assert!(
        matches!(err, StateError::CounterSaturated { .. }),
        "got {err:?}"
    );
    assert_eq!(db.queue_len(Queue::Up).expect("len"), 0, "nothing enqueued");
}

// ---------------------------------------------------------------------------
// Queue peek + atomic reprioritize
// ---------------------------------------------------------------------------

#[test]
fn queue_peek_is_nondestructive_and_matches_pop() {
    let (_dir, path) = scratch();
    let db = open_fresh(&path);
    assert_eq!(db.queue_peek(Queue::Up).expect("peek empty"), None);
    db.queue_push(Queue::Up, &rel("b.rrdata"), 1).expect("push");
    db.queue_push(Queue::Up, &rel("a.rrdata"), 0).expect("push");
    assert_eq!(
        db.queue_peek(Queue::Up).expect("peek"),
        Some((rel("a.rrdata"), 0))
    );
    // Peeking removed nothing and changed no order.
    assert_eq!(db.queue_len(Queue::Up).expect("len"), 2);
    assert_eq!(
        db.queue_pop(Queue::Up).expect("pop"),
        Some((rel("a.rrdata"), 0)),
        "pop returns exactly what peek showed"
    );
    assert_eq!(
        db.queue_peek(Queue::Up).expect("peek"),
        Some((rel("b.rrdata"), 1))
    );
}

#[test]
fn queue_reprioritize_moves_class_atomically() {
    let (_dir, path) = scratch();
    let db = open_fresh(&path);
    db.queue_push(Queue::Down, &rel("early.NEF"), 2)
        .expect("push");
    db.queue_push(Queue::Down, &rel("bump.NEF"), 2)
        .expect("push");
    db.queue_push(Queue::Down, &rel("top.NEF"), 0)
        .expect("push");
    // Not queued at all: Ok(false), nothing changed.
    assert!(!db
        .queue_reprioritize(Queue::Down, &rel("ghost.NEF"), 0)
        .expect("reprioritize missing"));
    assert_eq!(db.queue_len(Queue::Down).expect("len"), 3);
    // Same class: no-op keep-position, Ok(true).
    assert!(db
        .queue_reprioritize(Queue::Down, &rel("early.NEF"), 2)
        .expect("same class"));
    // Bump to the urgent class (§3.5 thumbs became visible): lands at the
    // BACK of class 0, membership stays single.
    assert!(db
        .queue_reprioritize(Queue::Down, &rel("bump.NEF"), 0)
        .expect("bump"));
    assert_eq!(db.queue_len(Queue::Down).expect("len"), 3);
    assert_eq!(
        db.queue_pop(Queue::Down).expect("pop"),
        Some((rel("top.NEF"), 0))
    );
    assert_eq!(
        db.queue_pop(Queue::Down).expect("pop"),
        Some((rel("bump.NEF"), 0)),
        "reprioritized entry carries its new class, behind existing class-0"
    );
    assert_eq!(
        db.queue_pop(Queue::Down).expect("pop"),
        Some((rel("early.NEF"), 2))
    );
    assert_eq!(db.queue_pop(Queue::Down).expect("pop"), None);
}

// ---------------------------------------------------------------------------
// with_txn: multi-operation atomicity (commit together or not at all)
// ---------------------------------------------------------------------------

#[test]
fn with_txn_commits_composite_transition_plus_enqueue() {
    let (_dir, path) = scratch();
    let key = rel("a.rrdata");
    {
        let db = open_fresh(&path);
        db.insert_item(&key, &bare_record(ItemState::Dirty))
            .expect("insert");
        // The §2.4 composite the engine needs: Dirty→Queued and the queue
        // entry commit in ONE transaction.
        db.with_txn(|t| {
            t.transition(&key, ItemState::Dirty, ItemState::Queued, |_| {})?;
            t.queue_push(Queue::Up, &key, 1)?;
            Ok(())
        })
        .expect("composite");
    }
    let db = SyncDb::open(&path, None).expect("reopen");
    assert_eq!(
        db.get_item(&key).expect("get").expect("exists").state,
        ItemState::Queued
    );
    assert_eq!(db.queue_pop(Queue::Up).expect("pop"), Some((key, 1)));
}

#[test]
fn with_txn_error_rolls_back_everything() {
    let (_dir, path) = scratch();
    let key = rel("a.rrdata");
    let db = open_fresh(&path);
    db.insert_item(&key, &bare_record(ItemState::Dirty))
        .expect("insert");
    // The transition SUCCEEDS inside the txn, then a later step fails: the
    // whole transaction must vanish — state unchanged, queue empty, no
    // seq allocated, no segment frozen.
    let err = db
        .with_txn(|t| {
            t.transition(&key, ItemState::Dirty, ItemState::Queued, |r| r.size = 42)?;
            t.queue_push(Queue::Up, &key, 0)?;
            t.freeze_next_segment(1, |_| Ok(b"doomed".to_vec()))?;
            // A failing CAS on another (missing) item aborts the closure.
            t.transition(
                &rel("ghost.dng"),
                ItemState::Dirty,
                ItemState::Queued,
                |_| {},
            )?;
            Ok(())
        })
        .expect_err("closure error must propagate");
    assert!(matches!(err, StateError::StaleState { .. }), "got {err:?}");
    let got = db.get_item(&key).expect("get").expect("exists");
    assert_eq!(got.state, ItemState::Dirty, "transition rolled back");
    assert_eq!(got.size, 0, "mutation rolled back");
    assert_eq!(db.queue_len(Queue::Up).expect("len"), 0, "push rolled back");
    assert_eq!(db.last_allocated_seq().expect("last"), 0, "seq rolled back");
    assert_eq!(
        db.unpublished_segments().expect("unpublished"),
        vec![],
        "freeze rolled back"
    );
}

#[test]
fn with_txn_pop_and_transition_is_atomic() {
    // The §2.4 "queue_pop + Queued→Uploading" step: after a crash the item
    // can never be durably gone from the queue while still Queued, because
    // both commit together.
    let (_dir, path) = scratch();
    let key = rel("a.rrdata");
    let db = open_fresh(&path);
    db.replay_put_item(&key, &bare_record(ItemState::Queued))
        .expect("insert");
    db.queue_push(Queue::Up, &key, 1).expect("push");
    let popped = db
        .with_txn(|t| {
            let popped = t.queue_pop(Queue::Up)?.expect("queue nonempty");
            t.transition(&popped.0, ItemState::Queued, ItemState::Uploading, |_| {})?;
            Ok(popped)
        })
        .expect("composite");
    assert_eq!(popped, (key.clone(), 1));
    assert_eq!(
        db.get_item(&key).expect("get").expect("exists").state,
        ItemState::Uploading
    );
    assert_eq!(db.queue_len(Queue::Up).expect("len"), 0);
}

// ---------------------------------------------------------------------------
// Seen-cache hygiene: removal + prune-on-set
// ---------------------------------------------------------------------------

#[test]
fn remove_xmp_seen_deletes_the_row() {
    let (_dir, path) = scratch();
    let key = rel("a.NEF");
    let db = open_fresh(&path);
    let hash = Blake3Hex::parse(BLAKE3_HEX).expect("hash");
    db.set_xmp_seen(&key, &hash).expect("set");
    assert!(db.remove_xmp_seen(&key).expect("remove"));
    assert_eq!(db.xmp_seen(&key).expect("get"), None);
    assert!(
        !db.remove_xmp_seen(&key).expect("remove again"),
        "idempotent"
    );
}

#[test]
fn set_dcim_seen_prunes_stale_rows_for_the_same_path() {
    let (_dir, path) = scratch();
    let db = open_fresh(&path);
    let cid = ContentId::parse(CONTENT_HEX).expect("cid");
    let cid2 = ContentId::parse(BLAKE3_HEX).expect("cid2");
    db.set_dcim_seen("/dcim/IMG_1.jpg", 100, 1_000, &cid)
        .expect("set");
    db.set_dcim_seen("/dcim/IMG_2.jpg", 50, 2_000, &cid2)
        .expect("set other");
    // The watched file was modified: a new observation replaces the old
    // row instead of stranding it.
    db.set_dcim_seen("/dcim/IMG_1.jpg", 120, 1_500, &cid2)
        .expect("set modified");
    assert_eq!(
        db.dcim_seen("/dcim/IMG_1.jpg", 100, 1_000).expect("get"),
        None,
        "stale observation pruned"
    );
    assert_eq!(
        db.dcim_seen("/dcim/IMG_1.jpg", 120, 1_500).expect("get"),
        Some(cid2.clone())
    );
    // Unrelated paths untouched.
    assert_eq!(
        db.dcim_seen("/dcim/IMG_2.jpg", 50, 2_000).expect("get"),
        Some(cid2)
    );
    // Explicit removal (source file gone).
    assert_eq!(db.remove_dcim_seen("/dcim/IMG_1.jpg").expect("remove"), 1);
    assert_eq!(
        db.dcim_seen("/dcim/IMG_1.jpg", 120, 1_500).expect("get"),
        None
    );
    assert_eq!(db.remove_dcim_seen("/dcim/IMG_1.jpg").expect("again"), 0);
}

// ---------------------------------------------------------------------------
// Review round 1: per-entry seq spans (§2.2), entry-state creation gate,
// cursor monotonicity, open-gate ordering, panic rollback, mint detection
// ---------------------------------------------------------------------------

#[test]
fn freeze_next_segment_multi_entry_spans_never_collide() {
    // §2.2: seq is a per-ENTRY counter. A 3-entry segment frozen through
    // the blessed path covers seqs 1..=3 (its entries embed them), so the
    // NEXT freeze must start at 4 — a remote device's per-entry
    // (device, seq) applied-dedup can never see a later segment reuse an
    // earlier segment's entry seqs.
    let (_dir, path) = scratch();
    let (first_a, first_b) = {
        let db = open_fresh(&path);
        let first_a = db
            .freeze_next_segment(3, |first| {
                assert_eq!(first, 1, "builder receives the FIRST entry seq");
                Ok(b"seg-a(1,2,3)".to_vec())
            })
            .expect("freeze 3-entry segment");
        assert_eq!(first_a, 1);
        assert_eq!(
            db.last_allocated_seq().expect("last"),
            3,
            "the whole span is allocated"
        );
        let first_b = db
            .freeze_next_segment(2, |first| {
                assert_eq!(first, 4, "no overlap with the previous span");
                Ok(b"seg-b(4,5)".to_vec())
            })
            .expect("freeze 2-entry segment");
        assert_eq!(first_b, 4);
        assert_eq!(db.last_allocated_seq().expect("last"), 5);
        (first_a, first_b)
    };
    // Spans + bytes durable across reopen, listed by first seq.
    let db = SyncDb::open(&path, None).expect("reopen");
    assert_eq!(
        db.unpublished_segments().expect("unpublished"),
        vec![
            (first_a, b"seg-a(1,2,3)".to_vec()),
            (first_b, b"seg-b(4,5)".to_vec()),
        ]
    );
    // And the counter keeps going from the span end after reopen.
    let next = db.allocate_seq().expect("alloc");
    assert_eq!(next, 6);
}

#[test]
fn mark_published_advances_cursor_and_floor_over_the_whole_span() {
    let (_dir, path) = scratch();
    let db = open_fresh(&path);
    let s1 = db
        .freeze_next_segment(3, |_| Ok(b"abc".to_vec()))
        .expect("freeze");
    let s2 = db
        .freeze_next_segment(1, |_| Ok(b"d".to_vec()))
        .expect("freeze");
    assert_eq!((s1, s2), (1, 4));
    db.mark_published(s1).expect("publish span 1..=3");
    assert_eq!(
        db.published_cursor().expect("cursor"),
        3,
        "cursor covers the span's LAST entry seq, not its filename seq"
    );
    assert_eq!(
        db.unpublished_segments().expect("unpublished"),
        vec![(s2, b"d".to_vec())],
        "the floor stepped over the whole span to the next segment"
    );
    // Idempotent re-publish of a span.
    db.mark_published(s1).expect("re-publish is a no-op");
    assert_eq!(db.published_cursor().expect("cursor"), 3);
    // Interior seqs of a span are not individually publishable: they are
    // published with their segment.
    for interior in [2u64, 3] {
        let err = db
            .mark_published(interior)
            .expect_err("interior seq is not a segment");
        assert!(
            matches!(err, StateError::NotFrozen { seq } if seq == interior),
            "got {err:?}"
        );
    }
    db.mark_published(s2).expect("publish");
    assert_eq!(db.published_cursor().expect("cursor"), 4);
    assert_eq!(db.unpublished_segments().expect("unpublished"), vec![]);
    // New work after the fully-published prefix is still seen (the floor
    // advanced over spans, it did not stall inside one).
    let s3 = db
        .freeze_next_segment(2, |first| Ok(vec![first as u8]))
        .expect("freeze");
    assert_eq!(s3, 5);
    assert_eq!(
        db.unpublished_segments().expect("unpublished"),
        vec![(s3, vec![5u8])]
    );
    // All durable.
    drop(db);
    let db = SyncDb::open(&path, None).expect("reopen");
    assert_eq!(db.published_cursor().expect("cursor"), 4);
    assert_eq!(
        db.unpublished_segments().expect("unpublished"),
        vec![(s3, vec![5u8])]
    );
}

#[test]
fn freeze_next_segment_zero_entries_is_typed_error() {
    let (_dir, path) = scratch();
    let db = open_fresh(&path);
    let err = db
        .freeze_next_segment(0, |_| Ok(vec![]))
        .expect_err("empty segment must refuse");
    assert!(matches!(err, StateError::EmptySegment), "got {err:?}");
    assert_eq!(db.last_allocated_seq().expect("last"), 0, "no seq consumed");
    assert_eq!(db.unpublished_segments().expect("unpublished"), vec![]);
}

#[test]
fn legacy_freeze_inside_an_existing_span_is_already_frozen() {
    // Overlap guard: the legacy pair cannot freeze bytes under a seq a
    // multi-entry segment's span already covers (first, interior, or
    // last) — spans never overlap, so frozen bytes stay immutable.
    let (_dir, path) = scratch();
    let db = open_fresh(&path);
    let first = db
        .freeze_next_segment(3, |_| Ok(b"span".to_vec()))
        .expect("freeze");
    assert_eq!(first, 1);
    for covered in [1u64, 2, 3] {
        let err = db
            .freeze_segment(covered, b"imposter")
            .expect_err("covered seq must refuse");
        assert!(
            matches!(err, StateError::AlreadyFrozen { seq } if seq == covered),
            "got {err:?}"
        );
    }
    assert_eq!(
        db.unpublished_segments().expect("unpublished"),
        vec![(1, b"span".to_vec())],
        "span bytes untouched"
    );
}

#[test]
fn set_cursor_never_regresses() {
    // §2.2: the cursor is the highest contiguously-applied seq. §2.3
    // bootstrap jumps it forward; nothing legitimately moves it back, so a
    // stale write-back is a committed no-op (max semantics, like
    // mark_published's cursor).
    let (_dir, path) = scratch();
    let d2 = dev(DEV2);
    {
        let db = open_fresh(&path);
        db.set_cursor(&d2, 5).expect("set");
        db.set_cursor(&d2, 3)
            .expect("stale set is a no-op, not an error");
        assert_eq!(db.cursor(&d2).expect("cursor"), 5, "no regression");
        db.set_cursor(&d2, 9).expect("advance");
        assert_eq!(db.cursor(&d2).expect("cursor"), 9);
        db.set_cursor(&d2, 9).expect("equal set is a no-op");
        assert_eq!(db.cursor(&d2).expect("cursor"), 9);
    }
    let db = SyncDb::open(&path, None).expect("reopen");
    assert_eq!(db.cursor(&d2).expect("cursor"), 9);
}

#[test]
fn insert_item_rejects_non_entry_birth_states() {
    // Creation goes through the state machine too: an item can be born
    // only in the §2.4 entry states; pipeline-interior births (Uploading,
    // Verifying, …) would bypass the machine with no greppable marker.
    let (_dir, path) = scratch();
    let db = open_fresh(&path);
    for (i, state) in ItemState::ALL.into_iter().enumerate() {
        let key = rel(&format!("entry-{i}.rrdata"));
        let result = db.insert_item(&key, &bare_record(state));
        if legal_entry(state) {
            assert!(
                matches!(result, Ok(true)),
                "{state:?} is an entry state, insert must succeed: {result:?}"
            );
        } else {
            let err = result.expect_err("non-entry birth state must refuse");
            assert!(
                matches!(
                    &err,
                    StateError::IllegalCreationState { relkey, state: s }
                        if *relkey == key && *s == state
                ),
                "got {err:?}"
            );
            assert_eq!(
                db.get_item(&key).expect("get"),
                None,
                "refused insert must store nothing"
            );
        }
    }
    // The documented entry set, pinned exactly.
    let entry: Vec<ItemState> = ItemState::ALL
        .into_iter()
        .filter(|s| legal_entry(*s))
        .collect();
    assert_eq!(
        entry,
        vec![
            ItemState::Dirty,
            ItemState::PendingDown,
            ItemState::Stub,
            ItemState::Hydrated,
        ],
        "update this assertion deliberately when the entry set changes"
    );
}

#[test]
fn newer_schema_with_corrupt_device_id_is_schema_too_new_not_codec() {
    // Open-gate ordering: the schema gate runs before ANY identity logic,
    // including parsing the stored id. A newer-format file whose identity
    // bytes do not parse as a v1 DeviceId must still be refused as
    // SchemaTooNew (the future "app update required" branch), never as a
    // Codec error.
    let (_dir, path) = scratch();
    let newer = rrcloud_core::state::SCHEMA_VERSION + 1;
    {
        let db = open_fresh(&path);
        db.force_schema_version(Some(newer)).expect("test override");
        db.force_corrupt_device_id(b"\xff\x00 not a device id")
            .expect("test override");
    }
    let err = SyncDb::open(&path, None).expect_err("newer schema must refuse");
    assert!(
        matches!(err, StateError::SchemaTooNew { found, .. } if found == newer),
        "schema gate must win over unparseable identity bytes, got {err:?}"
    );
    // A v1-stamped file with unparseable identity bytes, by contrast, IS
    // a codec failure of this build's own format — still typed, never a
    // panic, never a re-mint.
    {
        // Reset to v1 stamp with corrupt id: build the state via a fresh db.
        let (_dir2, path2) = scratch();
        let db = SyncDb::open(&path2, Some(dev(DEV1))).expect("fresh");
        db.force_corrupt_device_id(b"\xff").expect("test override");
        drop(db);
        let err = SyncDb::open(&path2, None).expect_err("corrupt v1 id must refuse");
        assert!(matches!(err, StateError::Codec(_)), "got {err:?}");
    }
}

#[test]
fn with_txn_panic_unwinds_without_committing_and_db_stays_usable() {
    // A panic inside the closure (a bug, not an Err) must behave like the
    // Err path: nothing commits, and the store remains usable afterwards.
    let (_dir, path) = scratch();
    let key = rel("a.rrdata");
    let db = open_fresh(&path);
    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        db.with_txn(|t| -> Result<(), StateError> {
            t.insert_item(&key, &bare_record(ItemState::Dirty))?;
            t.freeze_next_segment(1, |_| Ok(b"doomed".to_vec()))?;
            panic!("boom: simulated bug inside the composite step");
        })
    }));
    assert!(
        unwound.is_err(),
        "the panic must propagate, not be swallowed"
    );
    assert_eq!(db.get_item(&key).expect("get"), None, "insert rolled back");
    assert_eq!(db.last_allocated_seq().expect("last"), 0, "seq rolled back");
    assert_eq!(
        db.unpublished_segments().expect("unpublished"),
        vec![],
        "freeze rolled back"
    );
    // The store is not poisoned: normal writes still work.
    assert!(db
        .insert_item(&key, &bare_record(ItemState::Dirty))
        .expect("insert after panic"));
    assert_eq!(
        db.freeze_next_segment(1, |_| Ok(b"ok".to_vec()))
            .expect("freeze after panic"),
        1
    );
}

#[test]
fn minted_identity_reports_fresh_files_including_silent_db_loss() {
    // The §2.2 hazard documented on open(): a caller that caches the
    // device id and passes Some(id) on every open will silently re-mint —
    // with the seq counter reset — on an unexpectedly fresh file.
    // minted_identity() is the detection signal.
    let (_dir, path) = scratch();
    {
        let db = open_fresh(&path);
        assert!(db.minted_identity(), "first open minted");
        assert_eq!(
            db.freeze_next_segment(2, |_| Ok(b"published-elsewhere".to_vec()))
                .expect("freeze"),
            1
        );
    }
    {
        let db = SyncDb::open(&path, None).expect("reopen");
        assert!(!db.minted_identity(), "reopen of an existing db");
        drop(db);
        let db = SyncDb::open(&path, Some(dev(DEV1))).expect("reopen with id");
        assert!(!db.minted_identity());
    }
    // Whole-file loss with a cached id: the open SUCCEEDS silently and the
    // seq counter restarts — exactly the (device, seq)-reuse hazard. The
    // minted flag is the only signal, and with None instead the open fails
    // typed.
    std::fs::remove_file(&path).expect("simulate whole-file loss");
    let err = SyncDb::open(&path, None).expect_err("None on a fresh file fails typed");
    assert!(matches!(err, StateError::DeviceIdRequired), "got {err:?}");
    let db = SyncDb::open(&path, Some(dev(DEV1))).expect("cached-id reopen re-mints");
    assert!(
        db.minted_identity(),
        "an unexpected mint is detectable — the engine must treat it as db loss"
    );
    assert_eq!(
        db.last_allocated_seq().expect("last"),
        0,
        "the counter DID reset: publishing now would reuse (device, seq) pairs"
    );
}

// ---------------------------------------------------------------------------
// Review round 2: fallible segment builder (journal-encoder composition),
// StateTxn read accessors, queue_clear corruption recovery, delete_item
// companion-cleanup composite
// ---------------------------------------------------------------------------

/// A journal entry stamped with `seq`, as the engine's publisher builds
/// them inside the freeze builder.
fn journal_entry(seq: u64) -> rrcloud_core::journal::JournalEntry {
    rrcloud_core::journal::JournalEntry {
        v: rrcloud_core::journal::JOURNAL_VERSION,
        seq,
        ts: 1_769_900_000,
        device: dev(DEV1),
        op: rrcloud_core::journal::Op::Put,
        kind: Kind::Sidecar,
        key: format!("library/edit-{seq}.rrdata"),
        vv: [(dev(DEV1), seq as u32)].into_iter().collect(),
        size: Some(2048),
        blake3: Some(Blake3Hex::parse(BLAKE3_HEX).expect("blake3")),
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

#[test]
fn freeze_next_segment_builder_error_aborts_consuming_no_seqs() {
    // The engine's builder is the FALLIBLE journal encoder: a batch sized
    // near the byte cap with small placeholder seqs can cross it once the
    // real seq digits are stamped. That failure must be a typed abort that
    // consumes nothing — not a release-mode panic escape hatch.
    let (_dir, path) = scratch();
    let db = open_fresh(&path);
    let err = db
        .freeze_next_segment(1, |_| {
            Err(rrcloud_core::journal::JournalError::SegmentTooLarge { size: 2_000_000 }.into())
        })
        .expect_err("builder failure must abort the freeze");
    assert!(
        matches!(
            err,
            StateError::SegmentBuild(rrcloud_core::journal::JournalError::SegmentTooLarge {
                size: 2_000_000
            })
        ),
        "got {err:?}"
    );
    assert_eq!(db.last_allocated_seq().expect("last"), 0, "no seq consumed");
    assert_eq!(
        db.unpublished_segments().expect("unpublished"),
        vec![],
        "nothing stored"
    );
    // The caller can shrink the batch and retry: the next freeze still
    // starts the tiling at 1 (no hole was left).
    assert_eq!(
        db.freeze_next_segment(1, |_| Ok(b"retry".to_vec()))
            .expect("retry"),
        1
    );

    // Inside a composite, a builder error aborts the WHOLE transaction.
    let key = rel("a.rrdata");
    let err = db
        .with_txn(|t| {
            t.insert_item(&key, &bare_record(ItemState::Dirty))?;
            t.freeze_next_segment(1, |_| {
                Err(rrcloud_core::journal::JournalError::TooManyEntries { count: 1001 }.into())
            })?;
            Ok(())
        })
        .expect_err("builder failure must abort the composite");
    assert!(matches!(err, StateError::SegmentBuild(_)), "got {err:?}");
    assert_eq!(db.get_item(&key).expect("get"), None, "insert rolled back");
    assert_eq!(db.last_allocated_seq().expect("last"), 1, "retry seq only");
}

#[test]
fn freeze_next_segment_composes_with_the_journal_encoder() {
    // The mandated publication path: stamp the real seqs inside the
    // builder (the first seq is only known there) and encode fallibly with
    // `?` — JournalError converts into StateError::SegmentBuild.
    let (_dir, path) = scratch();
    let db = open_fresh(&path);
    let first = db
        .freeze_next_segment(3, |first| {
            let entries: Vec<_> = (0..3).map(|i| journal_entry(first + i)).collect();
            Ok(rrcloud_core::journal::encode_segment(&entries)?)
        })
        .expect("freeze encoded segment");
    assert_eq!(first, 1, "first allocation is 1");
    // The stored bytes decode back to entries stamped first..first+3.
    let segs = db.unpublished_segments().expect("unpublished");
    assert_eq!(segs.len(), 1);
    assert_eq!(segs[0].0, first);
    let decoded = rrcloud_core::journal::decode_segment(&segs[0].1).expect("decode");
    assert_eq!(
        decoded.iter().map(|e| e.seq).collect::<Vec<_>>(),
        vec![first, first + 1, first + 2],
        "entries embed the allocated span's seqs"
    );
    assert_eq!(
        decoded,
        vec![journal_entry(1), journal_entry(2), journal_entry(3)]
    );
    // And byte-identity survives reopen (§2.1.5 republish guarantee).
    drop(db);
    let db = SyncDb::open(&path, None).expect("reopen");
    assert_eq!(db.unpublished_segments().expect("unpublished"), segs);
}

#[test]
fn state_txn_read_accessors_see_uncommitted_writes() {
    // The §2.2 apply composite ("if !has_applied { mutate; mark_applied;
    // set_cursor }") and the transfer resume check must be able to run
    // ENTIRELY inside one transaction — no check-outside-txn pattern.
    let (_dir, path) = scratch();
    let db = open_fresh(&path);
    let d2 = dev(DEV2);
    let key = rel("a.rrdata");
    let upload = MultipartUploadState {
        upload_id: "upl-1".into(),
        part_size: 16 * 1024 * 1024,
        started_unix: 1_769_900_000,
        size: 0,
        mtime_unix_ns: 0,
    };
    let part = UploadPart {
        etag: "\"abc\"".into(),
        md5_b64: "md5md5==".into(),
    };
    db.with_txn(|t| {
        // applied / cursor
        assert!(!t.has_applied(&d2, 7)?);
        t.mark_applied(&d2, 7)?;
        assert!(t.has_applied(&d2, 7)?, "sees its own uncommitted apply");
        assert_eq!(t.cursor(&d2)?, 0);
        t.set_cursor(&d2, 7)?;
        assert_eq!(t.cursor(&d2)?, 7);
        // upload resume state
        assert_eq!(t.get_upload(&key)?, None);
        t.set_upload(&key, &upload)?;
        assert_eq!(t.get_upload(&key)?.as_ref(), Some(&upload));
        t.record_upload_part(&key, 1, &part)?;
        assert_eq!(t.upload_parts(&key)?, vec![(1, part.clone())]);
        // queue peek
        assert_eq!(t.queue_peek(Queue::Up)?, None);
        t.queue_push(Queue::Up, &key, 2)?;
        assert_eq!(t.queue_peek(Queue::Up)?, Some((key.clone(), 2)));
        // journal counters
        assert_eq!(t.last_allocated_seq()?, 0);
        let s = t.freeze_next_segment(2, |_| Ok(b"seg".to_vec()))?;
        assert_eq!(t.last_allocated_seq()?, s + 1, "whole span allocated");
        assert_eq!(t.published_cursor()?, 0);
        t.mark_published(s)?;
        assert_eq!(t.published_cursor()?, s + 1);
        Ok(())
    })
    .expect("composite");
    // Everything committed together and matches what the txn reads saw.
    assert!(db.has_applied(&d2, 7).expect("has_applied"));
    assert_eq!(db.cursor(&d2).expect("cursor"), 7);
    assert_eq!(db.get_upload(&key).expect("get_upload"), Some(upload));
    assert_eq!(db.upload_parts(&key).expect("parts").len(), 1);
    assert_eq!(
        db.queue_peek(Queue::Up).expect("peek"),
        Some((key.clone(), 2))
    );
    assert_eq!(db.last_allocated_seq().expect("last"), 2);
    assert_eq!(db.published_cursor().expect("published"), 2);
}

#[test]
fn queue_clear_recovers_a_queue_wedged_by_a_corrupt_row() {
    let (_dir, path) = scratch();
    let db = open_fresh(&path);
    // Clearing an empty queue is an idempotent no-op.
    assert_eq!(db.queue_clear(Queue::Up).expect("clear empty"), 0);
    // A corrupt row at the HEAD (class 0 beats the good row's class 1).
    db.queue_push(Queue::Up, &rel("good.rrdata"), 1)
        .expect("push good");
    db.force_corrupt_queue_row(Queue::Up, 0, "../not-a-relkey")
        .expect("corrupt row");
    // The wedge: every pop fails typed on the same head, the row stays,
    // and queue_remove cannot name it (it takes a validated RelKey).
    for _ in 0..2 {
        let err = db.queue_pop(Queue::Up).expect_err("corrupt head refuses");
        assert!(
            matches!(&err, StateError::CodecAt { key, .. } if key == "../not-a-relkey"),
            "the error must name the corrupt raw row, got {err:?}"
        );
    }
    let err = db.queue_peek(Queue::Up).expect_err("peek refuses too");
    assert!(
        matches!(&err, StateError::CodecAt { key, .. } if key == "../not-a-relkey"),
        "got {err:?}"
    );
    assert_eq!(db.queue_len(Queue::Up).expect("len"), 2, "nothing removed");
    // The recovery path: raw drain, no decoding, typed count back.
    assert_eq!(db.queue_clear(Queue::Up).expect("clear"), 2);
    assert_eq!(db.queue_len(Queue::Up).expect("len"), 0);
    assert_eq!(db.queue_peek(Queue::Up).expect("peek"), None);
    // The queue is fully usable again (rebuild from items_in_state).
    db.queue_push(Queue::Up, &rel("good.rrdata"), 1)
        .expect("re-push");
    assert_eq!(
        db.queue_pop(Queue::Up).expect("pop"),
        Some((rel("good.rrdata"), 1))
    );
    // The other queue was never touched, and the clear is durable.
    db.queue_push(Queue::Down, &rel("other.NEF"), 0)
        .expect("push down");
    drop(db);
    let db = SyncDb::open(&path, None).expect("reopen");
    assert_eq!(db.queue_len(Queue::Up).expect("len up"), 0);
    assert_eq!(db.queue_len(Queue::Down).expect("len down"), 1);
}

#[test]
fn delete_item_composite_cleans_companion_tables_atomically() {
    // The documented §2.7 tombstone-apply composite: delete_item alone
    // leaves queue/upload/xmp orphans (and an orphaned queue entry wedges
    // the engine's pop+transition composite), so the full deletion runs
    // all companion removals in ONE transaction.
    let (_dir, path) = scratch();
    let db = open_fresh(&path);
    let key = rel("a.rrdata");
    db.replay_put_item(&key, &bare_record(ItemState::Queued))
        .expect("item");
    db.queue_push(Queue::Up, &key, 1).expect("queued");
    db.set_upload(
        &key,
        &MultipartUploadState {
            upload_id: "upl-9".into(),
            part_size: 16 * 1024 * 1024,
            started_unix: 1_769_900_000,
            size: 0,
            mtime_unix_ns: 0,
        },
    )
    .expect("upload");
    db.record_upload_part(
        &key,
        1,
        &UploadPart {
            etag: "\"e\"".into(),
            md5_b64: "m==".into(),
        },
    )
    .expect("part");
    db.set_xmp_seen(&key, &Blake3Hex::parse(BLAKE3_HEX).expect("hash"))
        .expect("xmp");
    db.with_txn(|t| {
        t.delete_item(&key)?;
        t.queue_remove(Queue::Up, &key)?;
        t.queue_remove(Queue::Down, &key)?;
        t.clear_upload(&key)?;
        t.remove_xmp_seen(&key)?;
        Ok(())
    })
    .expect("full deletion composite");
    assert_eq!(db.get_item(&key).expect("get"), None);
    assert_eq!(db.queue_len(Queue::Up).expect("len"), 0);
    assert_eq!(db.get_upload(&key).expect("upload"), None);
    assert_eq!(db.upload_parts(&key).expect("parts"), vec![]);
    assert_eq!(db.xmp_seen(&key).expect("xmp"), None);
    // No orphan: a fresh pop on the emptied queue simply reports empty
    // instead of wedging on a deleted item's entry.
    assert_eq!(db.queue_pop(Queue::Up).expect("pop"), None);
}

// ---------------------------------------------------------------------------
// Round 3 review findings
// ---------------------------------------------------------------------------

#[test]
fn builder_error_caught_inside_with_txn_consumes_no_seqs() {
    // Regression: StateTxn::freeze_next_segment bumps the seq counter
    // BEFORE running the builder. If the builder's error is caught inside
    // the with_txn closure — the shrink-and-retry pattern the SegmentBuild
    // error doc explicitly invites — and the closure then commits, the
    // bump must not survive. Before the fix, this committed
    // last_allocated_seq past the retry's span, leaving the failed
    // attempt's seqs as a permanent allocated-never-frozen hole: remote
    // §2.2 contiguity cursors stall below it forever and the published
    // floor is pinned, the exact degradations freeze_next_segment's doc
    // rules out "by construction".
    let (_dir, path) = scratch();
    let db = open_fresh(&path);
    // A successful segment first, so the retry must stay contiguous.
    assert_eq!(
        db.freeze_next_segment(1, |_| Ok(b"one".to_vec()))
            .expect("seed segment"),
        1
    );
    db.with_txn(|t| {
        let err = t
            .freeze_next_segment(3, |_| {
                Err(rrcloud_core::journal::JournalError::SegmentTooLarge { size: 2_000_000 }.into())
            })
            .expect_err("builder must fail");
        assert!(matches!(err, StateError::SegmentBuild(_)), "got {err:?}");
        // The failed attempt consumed nothing, even inside this txn.
        assert_eq!(
            t.last_allocated_seq()?,
            1,
            "counter restored after builder error"
        );
        // Shrink and retry in the SAME transaction.
        let first = t.freeze_next_segment(1, |_| Ok(b"retry".to_vec()))?;
        assert_eq!(first, 2, "retry is contiguous with the frozen tiling");
        Ok(())
    })
    .expect("composite commits");
    assert_eq!(
        db.last_allocated_seq().expect("last"),
        2,
        "no allocated-never-frozen seqs"
    );
    assert_eq!(
        db.unpublished_segments().expect("unpublished"),
        vec![(1, b"one".to_vec()), (2, b"retry".to_vec())]
    );
    // The published floor advances over everything — no hole pins it.
    db.mark_published(1).expect("publish 1");
    db.mark_published(2).expect("publish 2");
    assert_eq!(db.published_cursor().expect("cursor"), 2);
    assert_eq!(db.unpublished_segments().expect("after publish"), vec![]);
}

#[test]
fn scan_errors_name_the_corrupt_item_key_so_it_can_be_deleted() {
    // Regression: a single corrupt `items` value used to abort every
    // crash-recovery scan with a key-less Codec error — and since the
    // scans are the only way to enumerate item keys (raw table names are
    // internal) and delete_item needs a known RelKey, the one bad record
    // could not even be found to delete it. The enumeration error must
    // name the offending key: the key side of such a row is intact (only
    // the value is garbage), so it is available at the failure site.
    let (_dir, path) = scratch();
    let db = open_fresh(&path);
    let good = rel("good.rrdata");
    let bad = rel("bad.rrdata");
    db.insert_item(&good, &bare_record(ItemState::Dirty))
        .expect("good item");
    db.force_corrupt_item(&bad, b"\xff\x00 not json at all")
        .expect("corrupt item");
    for err in [
        db.iter_items().expect_err("iter_items must refuse"),
        db.items_in_state(ItemState::Dirty)
            .expect_err("items_in_state must refuse"),
        db.count_in_state(ItemState::Dirty)
            .expect_err("count_in_state must refuse"),
    ] {
        assert!(
            matches!(&err, StateError::CodecAt { key, .. } if key == bad.as_str()),
            "enumeration error must name the corrupt key, got {err:?}"
        );
    }
    // The named key is exactly what makes the surface-and-delete recovery
    // real (and the queue_clear doc's "a corrupt item is still deletable"
    // claim true): delete the one bad record, every scan recovers.
    assert!(db.delete_item(&bad).expect("targeted delete"));
    assert_eq!(
        db.iter_items().expect("iter recovered"),
        vec![(good.clone(), bare_record(ItemState::Dirty))]
    );
    assert_eq!(db.count_in_state(ItemState::Dirty).expect("count"), 1);
}

#[test]
fn iter_uploads_error_names_the_corrupt_key_and_clear_upload_recovers() {
    // Same wedge class as the items scans: one corrupt `uploads` value
    // must not permanently blind the §2.4 stale-upload/resume scan.
    let (_dir, path) = scratch();
    let db = open_fresh(&path);
    let good = rel("good.rrdata");
    let bad = rel("bad.rrdata");
    let upload = MultipartUploadState {
        upload_id: "upl-1".into(),
        part_size: 16 * 1024 * 1024,
        started_unix: 1_769_900_000,
        size: 0,
        mtime_unix_ns: 0,
    };
    db.set_upload(&good, &upload).expect("good upload");
    db.force_corrupt_upload(&bad, b"{ not json")
        .expect("corrupt");
    let err = db.iter_uploads().expect_err("scan must refuse");
    assert!(
        matches!(&err, StateError::CodecAt { key, .. } if key == bad.as_str()),
        "got {err:?}"
    );
    // The key side is intact, so clear_upload can name the bad row.
    db.clear_upload(&bad).expect("targeted clear");
    assert_eq!(db.iter_uploads().expect("recovered"), vec![(good, upload)]);
}

#[test]
fn iter_cursors_enumerates_the_peer_applied_map_across_reopen() {
    // The §1.2 device-registry heartbeat publishes applied: {device: seq}
    // for every known peer, and §2.3 bootstrap compares the merged
    // cursors against journal segments — both need the full map, not
    // point reads, or the engine ends up keeping a shadow map of peers
    // outside the durable store.
    let (_dir, path) = scratch();
    let db = open_fresh(&path);
    assert_eq!(db.iter_cursors().expect("empty"), vec![]);
    db.set_cursor(&dev(DEV1), 5).expect("cursor d1");
    db.set_cursor(&dev(DEV2), 9).expect("cursor d2");
    db.set_cursor(&dev(DEV2), 7).expect("lower is a no-op");
    drop(db);
    let db = SyncDb::open(&path, None).expect("reopen");
    // Ascending by device id (DEV2 "a3…" < DEV1 "d1…").
    assert_eq!(
        db.iter_cursors().expect("iter"),
        vec![(dev(DEV2), 9), (dev(DEV1), 5)]
    );
}

#[test]
fn replay_put_item_cas_is_a_guarded_bypass_for_apply_loop_edges() {
    // The two reachable apply-loop situations with no legal() edge —
    // Stub -> Dirty (§2.8 out-of-band overwrite of an evicted original)
    // and Dirty -> Synced (§2.6 apply rule case 1: converged, adopt
    // metadata, no upload) — must not force the CAS-free replay_put_item
    // bypass, whose lost-update hazard is exactly what update_item was
    // added to close. replay_put_item_cas is the expected_state-checked
    // wholesale replace for those edges.
    let (_dir, path) = scratch();
    let db = open_fresh(&path);
    let key = rel("a.rrdata");
    // Absent record: typed stale, nothing created.
    let err = db
        .replay_put_item_cas(&key, ItemState::Stub, &bare_record(ItemState::Dirty))
        .expect_err("absent record must be stale");
    assert!(
        matches!(
            &err,
            StateError::StaleState {
                relkey,
                expected: ItemState::Stub,
                found: None,
            } if relkey == &key
        ),
        "got {err:?}"
    );
    assert_eq!(db.get_item(&key).expect("get"), None, "nothing created");
    // §2.8: Stub -> Dirty with a new content_id. Not a legal() edge (the
    // bytes were REPLACED out-of-band, not downloaded), hence the bypass.
    assert!(!legal(ItemState::Stub, ItemState::Dirty), "precondition");
    db.insert_item(&key, &bare_record(ItemState::Stub))
        .expect("stub record");
    let mut dirty = full_record();
    dirty.state = ItemState::Dirty;
    db.replay_put_item_cas(&key, ItemState::Stub, &dirty)
        .expect("overwrite-detected replace");
    assert_eq!(db.get_item(&key).expect("get"), Some(dirty.clone()));
    // Wrong expected state: typed stale, record untouched — the CAS
    // protection a racing transfer-engine transition relies on.
    let mut synced = full_record();
    synced.state = ItemState::Synced;
    let err = db
        .replay_put_item_cas(&key, ItemState::Stub, &synced)
        .expect_err("stale expectation must refuse");
    assert!(
        matches!(
            &err,
            StateError::StaleState {
                expected: ItemState::Stub,
                found: Some(ItemState::Dirty),
                ..
            }
        ),
        "got {err:?}"
    );
    assert_eq!(
        db.get_item(&key).expect("get"),
        Some(dirty.clone()),
        "nothing mutated on the stale path"
    );
    // §2.6 case 1: Dirty -> Synced converged adoption, inside the same
    // commit as the §2.2 apply bookkeeping (the StateTxn variant).
    assert!(!legal(ItemState::Dirty, ItemState::Synced), "precondition");
    db.with_txn(|t| {
        t.replay_put_item_cas(&key, ItemState::Dirty, &synced)?;
        t.mark_applied(&dev(DEV2), 3)?;
        Ok(())
    })
    .expect("converged-apply composite");
    assert!(db.has_applied(&dev(DEV2), 3).expect("applied"));
    drop(db);
    let db = SyncDb::open(&path, None).expect("reopen");
    assert_eq!(db.get_item(&key).expect("get"), Some(synced));
}

// ---------------------------------------------------------------------------
// P1-U3 additive extensions: outbound staging, deleted set, with_txn_err,
// and corruption-recovery raw deletes (inherited U2 review minors).
// ---------------------------------------------------------------------------

use rrcloud_core::state::DeletedRecord;

#[test]
fn outbound_staging_is_durable_fifo_with_monotonic_ids() {
    let (_dir, path) = scratch();
    let db = open_fresh(&path);
    assert_eq!(db.outbound_len().expect("len"), 0);
    assert!(db.iter_outbound().expect("iter").is_empty());

    let id1 = db.stage_outbound(b"first").expect("stage");
    let id2 = db.stage_outbound(b"second").expect("stage");
    let id3 = db.stage_outbound(b"third").expect("stage");
    assert!(
        id1 < id2 && id2 < id3,
        "strictly increasing ids: {id1} {id2} {id3}"
    );
    assert_eq!(db.outbound_len().expect("len"), 3);
    assert_eq!(
        db.iter_outbound().expect("iter"),
        vec![
            (id1, b"first".to_vec()),
            (id2, b"second".to_vec()),
            (id3, b"third".to_vec()),
        ],
        "FIFO order, exact bytes"
    );

    // Durable across reopen; the id counter never regresses.
    drop(db);
    let db = SyncDb::open(&path, None).expect("reopen");
    assert_eq!(db.outbound_len().expect("len"), 3);
    let id4 = db.stage_outbound(b"fourth").expect("stage");
    assert!(
        id4 > id3,
        "ids stay monotonic across reopen: {id4} vs {id3}"
    );
}

#[test]
fn outbound_remove_composes_atomically_in_a_txn() {
    let (_dir, path) = scratch();
    let db = open_fresh(&path);
    let id1 = db.stage_outbound(b"one").expect("stage");
    let id2 = db.stage_outbound(b"two").expect("stage");

    // The publisher's drain shape: consume staged records inside the same
    // transaction that freezes their segment. An aborted closure must
    // restore the staged record.
    let err = db
        .with_txn(|t| {
            assert!(t.remove_outbound(id1)?);
            Err::<(), _>(StateError::EmptySegment) // any error: abort
        })
        .expect_err("closure error aborts");
    assert!(matches!(err, StateError::EmptySegment));
    assert_eq!(
        db.outbound_len().expect("len"),
        2,
        "aborted removal rolled back"
    );

    db.with_txn(|t| {
        assert!(t.remove_outbound(id1)?);
        assert!(
            !t.remove_outbound(id1)?,
            "second removal in-txn reports absent"
        );
        Ok(())
    })
    .expect("commit removal");
    assert_eq!(
        db.iter_outbound().expect("iter"),
        vec![(id2, b"two".to_vec())],
        "only the removed record is gone"
    );

    // Staging composes in a txn too (the §3.4 chokepoint shape).
    let id3 = db
        .with_txn(|t| t.stage_outbound(b"three"))
        .expect("stage in txn");
    assert!(id3 > id2);
    assert_eq!(db.outbound_len().expect("len"), 2);
}

/// A domain error type that is not `StateError`, for `with_txn_err`.
#[derive(Debug)]
enum TestTxnErr {
    #[allow(dead_code)] // constructed via From, carried for the Debug rendering
    State(StateError),
    Domain(&'static str),
}

impl From<StateError> for TestTxnErr {
    fn from(e: StateError) -> Self {
        TestTxnErr::State(e)
    }
}

#[test]
fn with_txn_err_commits_on_ok_and_aborts_on_any_error_kind() {
    let (_dir, path) = scratch();
    let db = open_fresh(&path);
    let key = rel("txn-err.NEF");
    let record = bare_record(ItemState::Dirty);

    // Ok commits.
    let out = db
        .with_txn_err::<u32, TestTxnErr>(|t| {
            t.replay_put_item(&key, &record)?;
            Ok(7)
        })
        .expect("commit");
    assert_eq!(out, 7);
    assert_eq!(db.get_item(&key).expect("get"), Some(record.clone()));

    // A DOMAIN error (not a StateError) aborts everything the closure did
    // — this is what lets a journal consumer's failure roll back the
    // whole apply composite.
    let key2 = rel("txn-err-2.NEF");
    let err = db
        .with_txn_err::<(), TestTxnErr>(|t| {
            t.replay_put_item(&key2, &record)?;
            t.mark_applied(&dev(DEV2), 41)?;
            Err(TestTxnErr::Domain("consumer said no"))
        })
        .expect_err("domain error aborts");
    assert!(
        matches!(err, TestTxnErr::Domain("consumer said no")),
        "got {err:?}"
    );
    assert_eq!(
        db.get_item(&key2).expect("get"),
        None,
        "item write rolled back"
    );
    assert!(
        !db.has_applied(&dev(DEV2), 41).expect("applied"),
        "applied mark rolled back with it — one transaction"
    );

    // E = StateError works too (drop-in for with_txn call sites).
    db.with_txn_err::<(), StateError>(|t| t.set_cursor(&dev(DEV2), 5))
        .expect("plain StateError closure");
    assert_eq!(db.cursor(&dev(DEV2)).expect("cursor"), 5);
}

#[test]
fn deleted_set_roundtrips_replaces_and_survives_reopen() {
    let (_dir, path) = scratch();
    let db = open_fresh(&path);
    let gone = rel("albums/gone.NEF");
    let also = rel("albums/also-gone.NEF");
    assert_eq!(db.get_deleted(&gone).expect("get"), None);
    assert!(db.iter_deleted().expect("iter").is_empty());
    assert!(
        !db.remove_deleted(&gone).expect("remove"),
        "removing absent is false"
    );

    let first = DeletedRecord {
        vv: [(dev(DEV1), 3u32)].into_iter().collect(),
        server_ts: 1_769_940_000,
    };
    db.record_deleted(&gone, &first).expect("record");
    db.record_deleted(&also, &first).expect("record");
    assert_eq!(db.get_deleted(&gone).expect("get"), Some(first.clone()));
    assert_eq!(
        db.iter_deleted()
            .expect("iter")
            .iter()
            .map(|(k, _)| k.as_str().to_string())
            .collect::<Vec<_>>(),
        vec!["albums/also-gone.NEF", "albums/gone.NEF"],
        "ascending by relkey"
    );

    // Re-recording replaces (a later deletion observation wins).
    let second = DeletedRecord {
        vv: [(dev(DEV1), 3u32), (dev(DEV2), 1u32)].into_iter().collect(),
        server_ts: 1_769_941_111,
    };
    db.record_deleted(&gone, &second).expect("replace");
    assert_eq!(db.get_deleted(&gone).expect("get"), Some(second.clone()));

    // Composes in a txn (the §2.7 tombstone-apply shape: delete the item
    // and record the deletion in one commit).
    let item_key = rel("albums/tombstoned.NEF");
    db.insert_item(&item_key, &bare_record(ItemState::Dirty))
        .expect("insert");
    db.with_txn(|t| {
        t.delete_item(&item_key)?;
        t.record_deleted(&item_key, &first)?;
        assert_eq!(
            t.get_deleted(&item_key)?,
            Some(first.clone()),
            "txn sees its own write"
        );
        Ok(())
    })
    .expect("composite");
    assert_eq!(db.get_item(&item_key).expect("get"), None);

    drop(db);
    let db = SyncDb::open(&path, None).expect("reopen");
    assert_eq!(db.get_deleted(&gone).expect("get"), Some(second), "durable");
    assert!(db.remove_deleted(&gone).expect("remove"));
    assert_eq!(db.get_deleted(&gone).expect("get"), None);
}

// ---------------------------------------------------------------------------
// Inherited U2 verification-review minors
// ---------------------------------------------------------------------------

#[test]
fn upload_parts_enumeration_surfaces_codec_at_with_relkey_and_part_number() {
    // Minor 1: the per-relkey part-range scan is an enumeration like any
    // other — a corrupt part row must surface as CodecAt naming its row
    // (relkey + part number, spelled "<relkey>:<part>"; ':' is illegal in
    // relkeys, so the spelling is unambiguous), never as a bare Codec that
    // hides WHICH row is bad.
    let (_dir, path) = scratch();
    let db = open_fresh(&path);
    let key = rel("img.NEF");
    db.set_upload(
        &key,
        &MultipartUploadState {
            upload_id: "uid-1".to_string(),
            part_size: 16 << 20,
            started_unix: 1_769_900_000,
            size: 0,
            mtime_unix_ns: 0,
        },
    )
    .expect("set upload");
    db.record_upload_part(
        &key,
        1,
        &UploadPart {
            etag: "abc".to_string(),
            md5_b64: "xyz".to_string(),
        },
    )
    .expect("part 1");
    db.force_corrupt_upload_part(key.as_str(), 2, b"not json")
        .expect("corrupt part 2");

    let err = db
        .upload_parts(&key)
        .expect_err("corrupt part row must fail typed");
    match &err {
        StateError::CodecAt { key: at, .. } => {
            assert_eq!(
                at, "img.NEF:2",
                "the row's context is the relkey + part number"
            );
        }
        other => panic!("expected CodecAt naming the part row, got {other:?}"),
    }
}

#[test]
fn key_side_corrupt_item_row_is_surfaced_and_deletable_by_raw_key() {
    // Minor 2 (items): CodecAt.key can carry a corrupt raw key, but
    // delete_item takes a validated RelKey — the recovery loop needs a
    // raw-key delete.
    let (_dir, path) = scratch();
    let db = open_fresh(&path);
    db.insert_item(&rel("good.NEF"), &bare_record(ItemState::Dirty))
        .expect("good item");
    let raw = "bad\\key.NEF"; // backslash: can never be a RelKey
    let valid_value = serde_json::to_vec(&bare_record(ItemState::Dirty)).expect("encode");
    db.force_corrupt_item_key(raw, &valid_value)
        .expect("corrupt key");

    // Surface: the scan names the corrupt raw key.
    let err = db
        .iter_items()
        .expect_err("key-side corruption must fail typed");
    match &err {
        StateError::CodecAt { key, .. } => assert_eq!(key, raw),
        other => panic!("expected CodecAt with the raw key, got {other:?}"),
    }
    assert!(
        rrcloud_core::keys::RelKey::new(raw).is_err(),
        "precondition: the surfaced key is NOT constructible as a RelKey"
    );

    // Recover: delete by raw key, then the scan works again.
    assert!(db.delete_item_raw(raw).expect("raw delete"), "row existed");
    assert!(
        !db.delete_item_raw(raw).expect("raw delete again"),
        "idempotent"
    );
    let items = db.iter_items().expect("scan recovers");
    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0].0,
        rel("good.NEF"),
        "healthy rows survive the recovery"
    );
}

#[test]
fn key_side_corrupt_upload_row_is_surfaced_and_clearable_by_raw_key() {
    // Minor 2 (uploads): same loop for the uploads table; the raw clear
    // also drops part rows stored under the corrupt key.
    let (_dir, path) = scratch();
    let db = open_fresh(&path);
    let good = rel("good.NEF");
    db.set_upload(
        &good,
        &MultipartUploadState {
            upload_id: "uid-good".to_string(),
            part_size: 16 << 20,
            started_unix: 1_769_900_000,
            size: 0,
            mtime_unix_ns: 0,
        },
    )
    .expect("good upload");
    let raw = "bad\\upload.NEF";
    let valid_value = serde_json::to_vec(&MultipartUploadState {
        upload_id: "uid-bad".to_string(),
        part_size: 16 << 20,
        started_unix: 1_769_900_000,
        size: 0,
        mtime_unix_ns: 0,
    })
    .expect("encode");
    db.force_corrupt_upload_key(raw, &valid_value)
        .expect("corrupt key");

    let err = db
        .iter_uploads()
        .expect_err("key-side corruption must fail typed");
    match &err {
        StateError::CodecAt { key, .. } => assert_eq!(key, raw),
        other => panic!("expected CodecAt with the raw key, got {other:?}"),
    }

    db.clear_upload_raw(raw).expect("raw clear");
    db.clear_upload_raw(raw).expect("raw clear is idempotent");
    let uploads = db.iter_uploads().expect("scan recovers");
    assert_eq!(uploads.len(), 1);
    assert_eq!(uploads[0].0, good, "healthy rows survive the recovery");
}

#[test]
fn legacy_multipart_rows_without_captured_source_facts_decode_as_changed() {
    // P1-U4 review round: `MultipartUploadState` gained `size` /
    // `mtime_unix_ns` (the §2.4 mid-resume source-change baseline,
    // captured at upload creation). A row written before the fields
    // existed must still decode — with `0` defaults, which the transfer
    // engine reads as "source changed" (abort + restart, the safe
    // direction), never a decode failure that wedges the resume scan.
    let (_dir, path) = scratch();
    let db = open_fresh(&path);
    let r = rel("legacy.NEF");
    let legacy = br#"{"upload_id":"upl-legacy","part_size":16777216,"started_unix":1769900000}"#;
    db.force_corrupt_upload(&r, legacy).expect("raw legacy row");
    assert_eq!(
        db.get_upload(&r).expect("legacy row decodes"),
        Some(MultipartUploadState {
            upload_id: "upl-legacy".into(),
            part_size: 16 * 1024 * 1024,
            started_unix: 1_769_900_000,
            size: 0,
            mtime_unix_ns: 0,
        })
    );
}

// ---------------------------------------------------------------------------
// P1-U5 additive extensions: engine-unit ItemRecord fields
// ---------------------------------------------------------------------------

#[test]
fn v1_records_without_engine_fields_decode_with_defaults() {
    // A record stored by a pre-engine build carries none of the additive
    // fields; it must decode with rating/color_label/device/head_ts/
    // admitted_vv = None and deleted = false — never a codec error, and
    // never an invented value.
    let (_dir, path) = scratch();
    let db = open_fresh(&path);
    let key = rel("legacy.NEF");
    let legacy_json = format!(
        concat!(
            r#"{{"kind":"original","state":"synced","size":31457280,"#,
            r#""mtime_unix_ns":1769899000123456789,"blake3":"{BLAKE3}","#,
            r#""sem_hash":"{SEM}","vv":{{"{D1}":9,"{D2}":4}},"#,
            r#""content_id":"{CID}","w":6000,"h":4000,"pinned":true,"#,
            r#""last_access_unix":1769900000,"verified_remote":true,"#,
            r#""attested":true,"base_unknown":false}}"#
        ),
        BLAKE3 = BLAKE3_HEX,
        SEM = SEMHASH_HEX,
        D1 = DEV1,
        D2 = DEV2,
        CID = CONTENT_HEX,
    );
    db.force_corrupt_item(&key, legacy_json.as_bytes())
        .expect("store legacy record bytes");
    let record = db.get_item(&key).expect("get").expect("decodes");
    assert_eq!(record.rating, None);
    assert_eq!(record.color_label, None);
    assert_eq!(record.device, None);
    assert_eq!(record.head_ts, None);
    assert_eq!(record.admitted_vv, None);
    assert!(!record.deleted);
    // And the pre-existing fields still carried through.
    assert_eq!(record.kind, Kind::Original);
    assert_eq!(record.state, ItemState::Synced);
    assert_eq!(record.w, Some(6000));
}

#[test]
fn engine_fields_round_trip_across_reopen() {
    let (_dir, path) = scratch();
    let key = rel("badged.NEF");
    let mut record = full_record();
    record.rating = Some(5);
    record.color_label = Some("green".to_string());
    record.device = Some(dev(DEV2));
    record.head_ts = Some(1_769_901_234);
    record.admitted_vv = Some([(dev(DEV1), 10u32)].into_iter().collect());
    record.deleted = true;
    {
        let db = open_fresh(&path);
        // full_record() sits in Synced, which is not a birth state:
        // ingest through the greppable replay bypass.
        db.replay_put_item(&key, &record).expect("ingest");
    }
    let db = SyncDb::open(&path, None).expect("reopen");
    assert_eq!(db.get_item(&key).expect("get"), Some(record));
}

#[test]
fn transitions_and_updates_preserve_the_engine_fields() {
    // The deleted flag and the badge/provenance fields are ordinary
    // record fields: transition()'s CAS carries them unless `mutate`
    // changes them, and update_item can flip them state-preserving —
    // §2.7's "items keep their record with a deleted flag" depends on
    // both.
    let (_dir, path) = scratch();
    let db = open_fresh(&path);
    let key = rel("flagged.NEF");
    let mut record = bare_record(ItemState::Dirty);
    record.rating = Some(2);
    record.color_label = Some("red".to_string());
    record.device = Some(dev(DEV1));
    record.head_ts = Some(77);
    db.insert_item(&key, &record).expect("insert");

    // A state transition leaves them untouched.
    let after = db
        .transition(&key, ItemState::Dirty, ItemState::Queued, |_| {})
        .expect("transition");
    assert_eq!(after.rating, Some(2));
    assert_eq!(after.color_label, Some("red".to_string()));
    assert_eq!(after.device, Some(dev(DEV1)));
    assert_eq!(after.head_ts, Some(77));
    assert!(!after.deleted);

    // update_item flips the deleted flag without a state change (§2.7:
    // deletion composes with every pipeline state — it is a flag, not a
    // state, so hiding needs no new legal() edges).
    let hidden = db
        .update_item(&key, ItemState::Queued, |r| r.deleted = true)
        .expect("hide");
    assert!(hidden.deleted);
    assert_eq!(hidden.state, ItemState::Queued);
    let shown = db
        .update_item(&key, ItemState::Queued, |r| r.deleted = false)
        .expect("unhide");
    assert!(!shown.deleted);

    // The admitted_vv intent snapshot survives a transition's CAS too
    // (the §2.4 pipeline moves around it; only the verify-commit's own
    // mutate clears it).
    let mut admitted = bare_record(ItemState::Dirty);
    admitted.admitted_vv = Some([(dev(DEV1), 3u32)].into_iter().collect());
    let key2 = rel("admitted.NEF");
    db.insert_item(&key2, &admitted).expect("insert");
    let moved = db
        .transition(&key2, ItemState::Dirty, ItemState::Queued, |_| {})
        .expect("transition");
    assert_eq!(
        moved.admitted_vv,
        Some([(dev(DEV1), 3u32)].into_iter().collect())
    );
}

#[test]
fn engine_fields_do_not_change_the_stored_encoding_when_absent() {
    // skip_serializing_if keeps a default-valued record's JSON free of
    // the new optional keys, so records written by this build decode in
    // an older reader that ignores unknown fields — and byte-stable
    // hashes over stored records (debug tooling) do not churn.
    let record = bare_record(ItemState::Dirty);
    let json = serde_json::to_string(&record).expect("encode");
    for absent in [
        "rating",
        "color_label",
        "\"device\"",
        "head_ts",
        "admitted_vv",
    ] {
        assert!(
            !json.contains(absent),
            "default-valued {absent} must not serialize: {json}"
        );
    }
    assert!(
        json.contains("\"deleted\":false"),
        "the flag is unconditional: {json}"
    );
}
