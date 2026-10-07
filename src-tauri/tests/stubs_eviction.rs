//! P2 §3.5 Garage-backed eviction tests: the LRU cache gate and the "never
//! evict unverified bytes" invariant. Feature-gated to `sync`; set
//! `GARAGE_BIN=/tmp/claude-0/garage` to run.
//!
//! Pins:
//!   * the evictor demotes least-recently-accessed, non-pinned, verified
//!     originals back to 0-byte stubs (remote mtime restored so the
//!     thumbnail cache key survives), and keeps pinned ones hydrated;
//!   * an original whose remote copy is NOT content-verified (no attest
//!     entry and the read-back re-hash cannot confirm) is NOT evicted; a
//!     planted remote hash mismatch routes the item to `corrupt_remote`,
//!     never evicted, never served.

#![cfg(feature = "sync")]

mod common;

use std::path::Path;
use std::sync::OnceLock;

use common::garage;
use rapidraw_lib::rrcloud_core::journal::Kind;
use rapidraw_lib::rrcloud_core::keys::relkey;
use rapidraw_lib::rrcloud_core::s3::PutObjectOptions;
use rapidraw_lib::rrcloud_core::transfer::bucket_key_for;
use rapidraw_lib::sync::{self, Credentials, SyncManager, SyncSettings};
use tokio::sync::{Mutex, MutexGuard};

async fn global_guard() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(())).lock().await
}

fn settings_for(garage: &garage::Garage, bucket: &str, cache_size_gb: u32) -> SyncSettings {
    SyncSettings {
        enabled: true,
        endpoint: garage.endpoint(),
        bucket: bucket.to_string(),
        region: garage.region().to_string(),
        force_path_style: true,
        cache_size_gb,
        ..SyncSettings::default()
    }
}

fn creds_for(garage: &garage::Garage) -> Credentials {
    Credentials {
        access_key: garage.access_key_id.clone(),
        secret_key: garage.secret_access_key.clone(),
    }
}

fn original_bytes(seed: u8, len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| seed.wrapping_add((i % 251) as u8))
        .collect()
}

fn mtime_unix_secs(path: &Path) -> i64 {
    std::fs::metadata(path)
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

struct RemoteOriginal {
    blake3_hex: String,
    size: u64,
    mtime_unix: i64,
}

async fn upload_original(
    mgr: &SyncManager,
    root: &Path,
    rel: &str,
    bytes: &[u8],
) -> RemoteOriginal {
    let path = root.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).expect("mkdir original");
    std::fs::write(&path, bytes).expect("write original");
    let blake3_hex = blake3::hash(bytes).to_hex().to_string();
    let size = bytes.len() as u64;
    let mtime_unix = mtime_unix_secs(&path);
    mgr.note_new_original(&path);
    mgr.run_once().await.expect("upload cycle");
    RemoteOriginal {
        blake3_hex,
        size,
        mtime_unix,
    }
}

#[tokio::test]
async fn evictor_demotes_lru_verified_originals_and_keeps_pinned() {
    let _g = global_guard().await;
    let Some(garage) = garage::shared() else {
        eprintln!("SKIP: no Garage binary; set GARAGE_BIN to run");
        return;
    };
    let bucket = garage.create_unique_bucket("p2-evict-lru");
    // cache_size_gb irrelevant here: the byte-budget seam drives the gate.
    let settings = settings_for(garage, &bucket, 8);
    let creds = creds_for(garage);

    // Device A publishes two originals.
    let root_a = tempfile::tempdir().expect("root a");
    let state_a = tempfile::tempdir().expect("state a");
    let mgr_a = SyncManager::new_inert();
    mgr_a
        .configure(
            settings.clone(),
            creds.clone(),
            root_a.path().to_path_buf(),
            state_a.path().to_path_buf(),
        )
        .expect("configure a");
    sync::install_global_manager(mgr_a.clone());

    let rel_old = "lib/OLD_01.NEF";
    let rel_new = "lib/NEW_01.NEF";
    let r_old = upload_original(
        &mgr_a,
        root_a.path(),
        rel_old,
        &original_bytes(1, 80 * 1024),
    )
    .await;
    let r_new = upload_original(
        &mgr_a,
        root_a.path(),
        rel_new,
        &original_bytes(2, 80 * 1024),
    )
    .await;

    // Device B stubs both, then hydrates OLD first and NEW second, so NEW is
    // the more-recently-accessed (higher LRU rank).
    let root_b = tempfile::tempdir().expect("root b");
    let state_b = tempfile::tempdir().expect("state b");
    let mgr_b = SyncManager::new_inert();
    mgr_b
        .configure(
            settings,
            creds,
            root_b.path().to_path_buf(),
            state_b.path().to_path_buf(),
        )
        .expect("configure b");
    sync::install_global_manager(mgr_b.clone());

    let stub_old = root_b.path().join(rel_old);
    let stub_new = root_b.path().join(rel_new);
    std::fs::create_dir_all(stub_old.parent().unwrap()).expect("mkdir");
    mgr_b
        .create_stub(&stub_old, &r_old.blake3_hex, r_old.size, r_old.mtime_unix)
        .expect("stub old");
    mgr_b
        .create_stub(&stub_new, &r_new.blake3_hex, r_new.size, r_new.mtime_unix)
        .expect("stub new");
    mgr_b.ensure_local(&stub_old, "test").expect("hydrate old");
    mgr_b.ensure_local(&stub_new, "test").expect("hydrate new");

    // The thumbnail key of OLD, to prove mtime (and thus the key) survives
    // the round-trip to a stub.
    let old_path_str = stub_old.to_string_lossy().to_string();
    let adjustments = br#"{"exposure":0.0}"#;
    let old_hash_before = sync::compute_thumbnail_cache_hash(&old_path_str, adjustments).unwrap();

    // Budget that fits exactly ONE original ⇒ the LRU victim (OLD) is
    // demoted, the recently-accessed NEW is kept.
    let report = mgr_b
        .run_evictor_with_budget(r_new.size)
        .await
        .expect("evictor");

    assert!(
        report.evicted.contains(&stub_old),
        "the LRU original must be evicted, got {:?}",
        report.evicted
    );
    assert!(
        !report.evicted.contains(&stub_new),
        "the recently-accessed original must be kept"
    );
    assert!(mgr_b.is_stub(&stub_old), "OLD is a stub again");
    assert!(!mgr_b.is_stub(&stub_new), "NEW stays hydrated");
    assert_eq!(
        std::fs::metadata(&stub_old).unwrap().len(),
        0,
        "the evicted original is a 0-byte stub"
    );
    assert_eq!(
        mtime_unix_secs(&stub_old),
        r_old.mtime_unix,
        "eviction restores the remote mtime"
    );
    let old_hash_after = sync::compute_thumbnail_cache_hash(&old_path_str, adjustments).unwrap();
    assert_eq!(
        old_hash_before, old_hash_after,
        "the thumbnail cache key survives eviction (mtime preserved)"
    );

    // Pinning protects the next victim: pin NEW, re-hydrate OLD, then a
    // zero budget must evict only the unpinned OLD.
    mgr_b
        .ensure_local(&stub_old, "test")
        .expect("re-hydrate old");
    let pinned = mgr_b
        .pin_paths(std::slice::from_ref(&stub_new), true)
        .expect("pin new");
    assert_eq!(pinned, 1, "one item's pin flag changed");

    let report2 = mgr_b.run_evictor_with_budget(0).await.expect("evictor 2");
    assert!(
        report2.kept_pinned.contains(&stub_new),
        "a pinned original is never evicted, got kept_pinned {:?}",
        report2.kept_pinned
    );
    assert!(
        !mgr_b.is_stub(&stub_new),
        "pinned NEW stays hydrated under a 0 budget"
    );
    assert!(
        report2.evicted.contains(&stub_old),
        "the unpinned original is evicted under a 0 budget"
    );
}

#[tokio::test]
async fn evictor_never_evicts_unverified_or_corrupt_remote_bytes() {
    let _g = global_guard().await;
    let Some(garage) = garage::shared() else {
        eprintln!("SKIP: no Garage binary; set GARAGE_BIN to run");
        return;
    };
    let bucket = garage.create_unique_bucket("p2-evict-verify");
    let settings = settings_for(garage, &bucket, 8);
    let creds = creds_for(garage);

    // Device A publishes three originals and keeps the verified local bytes;
    // A's own uploads are `synced` + `verified_remote` but un-attested (A
    // never hydrated them), so the evictor must read-back before evicting.
    let root_a = tempfile::tempdir().expect("root a");
    let state_a = tempfile::tempdir().expect("state a");
    let mgr_a = SyncManager::new_inert();
    mgr_a
        .configure(
            settings,
            creds,
            root_a.path().to_path_buf(),
            state_a.path().to_path_buf(),
        )
        .expect("configure a");
    sync::install_global_manager(mgr_a.clone());

    let rel_ok = "v/OK_01.NEF"; // readback confirms → evictable
    let rel_gone = "v/GONE_01.NEF"; // remote deleted → unverifiable → kept
    let rel_bad = "v/BAD_01.NEF"; // remote corrupted → corrupt_remote
    let b_ok = original_bytes(10, 48 * 1024);
    let b_gone = original_bytes(20, 48 * 1024);
    let b_bad = original_bytes(30, 48 * 1024);
    upload_original(&mgr_a, root_a.path(), rel_ok, &b_ok).await;
    upload_original(&mgr_a, root_a.path(), rel_gone, &b_gone).await;
    upload_original(&mgr_a, root_a.path(), rel_bad, &b_bad).await;

    let p_ok = root_a.path().join(rel_ok);
    let p_gone = root_a.path().join(rel_gone);
    let p_bad = root_a.path().join(rel_bad);

    // Plant the remote faults directly in the bucket.
    let client = garage.client();
    let key_gone =
        bucket_key_for(&relkey(&p_gone, root_a.path()).unwrap(), Kind::Original).unwrap();
    client
        .delete_object(&bucket, &key_gone)
        .await
        .expect("delete remote GONE");
    let key_bad = bucket_key_for(&relkey(&p_bad, root_a.path()).unwrap(), Kind::Original).unwrap();
    client
        .put_object(
            &bucket,
            &key_bad,
            bytes::Bytes::from(original_bytes(99, 48 * 1024)),
            &PutObjectOptions::default(),
        )
        .await
        .expect("overwrite remote BAD");

    // A zero budget asks the evictor to reclaim everything it safely can.
    let report = mgr_a.run_evictor_with_budget(0).await.expect("evictor");

    // OK: readback confirms the remote matches → evicted to a stub.
    assert!(
        report.evicted.contains(&p_ok),
        "a readback-verified original may be evicted, got {:?}",
        report.evicted
    );
    assert!(mgr_a.is_stub(&p_ok));

    // GONE: the remote cannot be read back → NOT evicted (local bytes stay).
    assert!(
        !report.evicted.contains(&p_gone),
        "an original whose remote cannot be read back must not be evicted"
    );
    assert!(!mgr_a.is_stub(&p_gone), "GONE keeps its local bytes");
    assert!(
        std::fs::metadata(&p_gone).unwrap().len() > 0,
        "the unverifiable original is never reduced to a 0-byte stub"
    );

    // BAD: readback hash mismatch → corrupt_remote, never evicted/served.
    assert!(
        !report.evicted.contains(&p_bad),
        "a corrupt-remote original must never be evicted"
    );
    assert_eq!(
        mgr_a.item_sync_state(&p_bad).as_deref(),
        Some("corrupt_remote"),
        "a planted remote mismatch must route the item to corrupt_remote"
    );
    assert!(
        report.corrupt.contains(&p_bad),
        "the evictor must report the corrupt original"
    );
    assert!(
        std::fs::metadata(&p_bad).unwrap().len() > 0,
        "corrupt_remote keeps the good local bytes, never a 0-byte stub"
    );
}

/// Regression (blocker): a mid-pass eviction error must not desync the
/// in-memory stub mirror from redb/disk. The evictor commits each
/// `Hydrated/Synced → Stub` transition + truncation per item; if a LATER
/// candidate makes the pass return `Err` (here: a vanished local file makes
/// `evict_to_stub`'s truncate `open()` fail), the items already demoted are
/// 0-byte stubs on disk + redb, yet `is_stub`/`is_cloud_placeholder` would
/// answer `false` for them until the next process restart — so every §3.5
/// guard site bypasses hydration and reads the 0-byte placeholder as content.
/// The mirror must learn of every committed stub even on the error path.
#[tokio::test]
async fn evictor_error_keeps_the_stub_mirror_consistent_with_redb() {
    let _g = global_guard().await;
    let Some(garage) = garage::shared() else {
        eprintln!("SKIP: no Garage binary; set GARAGE_BIN to run");
        return;
    };
    let bucket = garage.create_unique_bucket("p2-evict-desync");
    let settings = settings_for(garage, &bucket, 8);
    let creds = creds_for(garage);

    // Device A publishes two originals.
    let root_a = tempfile::tempdir().expect("root a");
    let state_a = tempfile::tempdir().expect("state a");
    let mgr_a = SyncManager::new_inert();
    mgr_a
        .configure(
            settings.clone(),
            creds.clone(),
            root_a.path().to_path_buf(),
            state_a.path().to_path_buf(),
        )
        .expect("configure a");
    sync::install_global_manager(mgr_a.clone());

    let rel_old = "lib/OLD_01.NEF";
    let rel_new = "lib/NEW_01.NEF";
    let r_old = upload_original(
        &mgr_a,
        root_a.path(),
        rel_old,
        &original_bytes(1, 80 * 1024),
    )
    .await;
    let r_new = upload_original(
        &mgr_a,
        root_a.path(),
        rel_new,
        &original_bytes(2, 80 * 1024),
    )
    .await;

    // Device B stubs both, hydrates OLD first (LRU victim) then NEW.
    let root_b = tempfile::tempdir().expect("root b");
    let state_b = tempfile::tempdir().expect("state b");
    let mgr_b = SyncManager::new_inert();
    mgr_b
        .configure(
            settings,
            creds,
            root_b.path().to_path_buf(),
            state_b.path().to_path_buf(),
        )
        .expect("configure b");
    sync::install_global_manager(mgr_b.clone());

    let stub_old = root_b.path().join(rel_old);
    let stub_new = root_b.path().join(rel_new);
    std::fs::create_dir_all(stub_old.parent().unwrap()).expect("mkdir");
    mgr_b
        .create_stub(&stub_old, &r_old.blake3_hex, r_old.size, r_old.mtime_unix)
        .expect("stub old");
    mgr_b
        .create_stub(&stub_new, &r_new.blake3_hex, r_new.size, r_new.mtime_unix)
        .expect("stub new");
    mgr_b.ensure_local(&stub_old, "test").expect("hydrate old");
    mgr_b.ensure_local(&stub_new, "test").expect("hydrate new");

    // Delete NEW's local file so that when the pass reaches it (after OLD has
    // already been committed to a stub), `evict_to_stub`'s truncate `open()`
    // fails and the whole pass returns `Err`.
    std::fs::remove_file(&stub_new).expect("delete NEW local file");

    // A zero budget evicts OLD first (committed: redb Stub + 0-byte file),
    // then errors on the vanished NEW.
    let result = mgr_b.run_evictor_with_budget(0).await;
    assert!(
        result.is_err(),
        "a vanished local file must make the pass return Err"
    );

    // OLD was committed to a stub on disk + redb before the error.
    assert_eq!(
        mgr_b.item_sync_state(&stub_old).as_deref(),
        Some("stub"),
        "OLD is durably a stub in redb despite the mid-pass error"
    );
    assert_eq!(
        std::fs::metadata(&stub_old).unwrap().len(),
        0,
        "OLD is a 0-byte stub on disk"
    );

    // The invariant under test: the in-memory mirror agrees with redb for
    // every committed stub, so the §3.5 guard sites re-hydrate instead of
    // reading the 0-byte placeholder as content.
    assert!(
        mgr_b.is_stub(&stub_old),
        "the mirror must recognize OLD as a stub even though the pass errored"
    );
    assert!(
        sync::hooks::is_stub(&stub_old),
        "is_cloud_placeholder's backing query must also recognize the stub"
    );
}
