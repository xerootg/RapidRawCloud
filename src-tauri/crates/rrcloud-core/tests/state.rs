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
    legal, ItemRecord, ItemState, MultipartUploadState, Queue, StateError, SyncDb, UploadPart,
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
        db.insert_item(&key, &record).expect("put");
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
    db.insert_item(&key, &before).expect("put");
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
    let original = bare_record(ItemState::Synced);
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
        db.insert_item(&key, &bare_record(ItemState::Synced))
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
        db.insert_item(&key, &bare_record(ItemState::Synced))
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
        db.insert_item(&uploading, &bare_record(ItemState::Uploading))
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
    // Scans hit the corrupt row too — still typed, still no panic.
    let err = db.iter_items().expect_err("scan over corrupt value");
    assert!(matches!(err, StateError::Codec(_)), "got {err:?}");
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
            .freeze_next_segment(|seq| format!("segment-{seq}").into_bytes())
            .expect("freeze");
        assert_eq!(s1, 1, "first allocation is 1");
        assert_eq!(db.last_allocated_seq().expect("last"), s1);
        let s2 = db
            .freeze_next_segment(|seq| format!("segment-{seq}").into_bytes())
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
        .freeze_next_segment(|seq| vec![seq as u8])
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
                            db.freeze_next_segment(|seq| seq.to_le_bytes().to_vec())
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
        .freeze_next_segment(|_| b"five".to_vec())
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
        .freeze_next_segment(|_| vec![])
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
            t.freeze_next_segment(|_| b"doomed".to_vec())?;
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
    db.insert_item(&key, &bare_record(ItemState::Queued))
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
