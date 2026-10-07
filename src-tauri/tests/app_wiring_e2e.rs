//! Garage-backed end-to-end desktop sync test (ARCHITECTURE.md §3.3/§3.4),
//! plus the bounded exit-flush test. Feature-gated to `sync` (reaches
//! rrcloud-core's S3 client through `rapidraw_lib::rrcloud_core`); set
//! `GARAGE_BIN=/tmp/claude-0/garage` to run it.
//!
//! This proves the *real app code path*: an edit made through
//! `save_sidecar` (the chokepoint), driven by a real [`SyncManager`]
//! configured against a real Garage bucket, lands as a sidecar object plus
//! a journal entry; and a second [`SyncManager`] with a distinct device
//! (own state dir) and its own temp root, pointed at the same bucket,
//! polls and converges the edit locally.

#![cfg(feature = "sync")]

mod common;

use std::path::Path;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use common::garage;
use rapidraw_lib::rrcloud_core::keys::{CONTROL_PREFIX, LIBRARY_PREFIX};
use rapidraw_lib::rrcloud_core::s3::{ListObjectsV2Request, S3Client};
use rapidraw_lib::sync::{
    self, Credentials, ImageMetadata, SyncManager, SyncSettings, WriteOrigin, save_sidecar,
};
use tokio::sync::{Mutex, MutexGuard};

/// Serializes the tests that install the process-global [`SyncManager`]
/// (`sync::install_global_manager`). libtest runs both `#[tokio::test]`s in
/// this binary in parallel, so without this guard they race to overwrite
/// the one process-global `GLOBAL_MANAGER`: if the exit-flush test's manager
/// wins the global between this test's `install_global_manager` and its
/// `save_sidecar`, the chokepoint routes `note_local_sidecar` to the wrong
/// manager, `relkey` resolves against a foreign sync-root and fails, nothing
/// is marked dirty, and `run_once` uploads nothing — the `got []` panic at
/// :116 (P1-U7 review, reproduced 5/16 before this guard). Mirrors
/// `app_wiring_chokepoint.rs::global_guard` and `hooks.rs`'s `serial`.
///
/// The guard is held for the whole test, across the async `run_once` /
/// `exit_flush` awaits — so it is an await-aware [`tokio::sync::Mutex`]
/// (rather than the sibling files' `std::sync::Mutex`, which would trip
/// `clippy::await_holding_lock` and risks blocking the runtime when held
/// across an await). It simply ensures the two tests never overlap.
async fn global_guard() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(())).lock().await
}

fn settings_for(garage: &garage::Garage, bucket: &str) -> SyncSettings {
    SyncSettings {
        enabled: true,
        endpoint: garage.endpoint(),
        bucket: bucket.to_string(),
        region: garage.region().to_string(),
        force_path_style: true,
        ..SyncSettings::default()
    }
}

fn creds_for(garage: &garage::Garage) -> Credentials {
    Credentials {
        access_key: garage.access_key_id.clone(),
        secret_key: garage.secret_access_key.clone(),
    }
}

fn edit(rating: u8, exposure: f64) -> ImageMetadata {
    ImageMetadata {
        version: 1,
        rating,
        adjustments: serde_json::json!({ "exposure": exposure, "contrast": 0 }),
        tags: Some(vec![]),
        exif: None,
        flag: None,
    }
}

/// Every key in `bucket` under `prefix`, sorted.
async fn keys_under(client: &S3Client, bucket: &str, prefix: &str) -> Vec<String> {
    let mut keys = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let page = client
            .list_objects_v2(
                bucket,
                &ListObjectsV2Request {
                    prefix: Some(prefix.to_string()),
                    continuation_token: token.take(),
                    ..Default::default()
                },
            )
            .await
            .expect("list_objects_v2");
        keys.extend(page.objects.into_iter().map(|o| o.key));
        if !page.is_truncated {
            break;
        }
        token = page.next_continuation_token;
    }
    keys.sort();
    keys
}

#[tokio::test]
async fn two_sync_managers_converge_through_a_real_bucket() {
    // Serialize installers of the one process-global manager (see
    // `global_guard`): held for the whole test so the sibling exit-flush
    // test cannot overwrite `GLOBAL_MANAGER` mid-cycle.
    let _global = global_guard().await;
    let Some(garage) = garage::shared() else {
        eprintln!("SKIP: no Garage binary; set GARAGE_BIN to run the e2e test");
        return;
    };
    let bucket = garage.create_unique_bucket("app-e2e");
    let settings = settings_for(garage, &bucket);
    let creds = creds_for(garage);

    // ---- device A: edit through the chokepoint, then sync up ----
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

    let rel = Path::new("trip/DSC_0001.NEF.rrdata");
    let sidecar_a = root_a.path().join(rel);
    std::fs::create_dir_all(sidecar_a.parent().unwrap()).expect("mkdir a");
    let the_edit = edit(4, 0.75);
    save_sidecar(None, &sidecar_a, &the_edit, WriteOrigin::User).expect("save through chokepoint");

    mgr_a.run_once().await.expect("device A sync cycle");

    // The sidecar object and a journal segment must have landed.
    let client = garage.client();
    let lib_keys = keys_under(&client, &bucket, LIBRARY_PREFIX).await;
    assert!(
        lib_keys.iter().any(|k| k.ends_with(".rrdata")),
        "a sidecar object must land in the bucket, got {lib_keys:?}"
    );
    let journal_keys = keys_under(&client, &bucket, &format!("{CONTROL_PREFIX}journal/")).await;
    assert!(
        !journal_keys.is_empty(),
        "a journal segment must land in the bucket"
    );

    // ---- device B: distinct device + root, same bucket, poll + converge ----
    let root_b = tempfile::tempdir().expect("root b");
    let state_b = tempfile::tempdir().expect("state b");
    let mgr_b = SyncManager::new_inert();
    mgr_b
        .configure(
            settings.clone(),
            creds.clone(),
            root_b.path().to_path_buf(),
            state_b.path().to_path_buf(),
        )
        .expect("configure b");

    mgr_b.run_once().await.expect("device B sync cycle");

    let sidecar_b = root_b.path().join(rel);
    assert!(
        sidecar_b.exists(),
        "device B must converge the sidecar locally at {}",
        sidecar_b.display()
    );
    let b_meta: ImageMetadata =
        serde_json::from_slice(&std::fs::read(&sidecar_b).expect("read b sidecar"))
            .expect("parse b sidecar");
    assert_eq!(
        b_meta.rating, the_edit.rating,
        "the converged sidecar must carry device A's edit"
    );
}

#[tokio::test]
async fn bounded_exit_flush_drains_or_times_out_without_hanging() {
    // Serialize installers of the one process-global manager (see
    // `global_guard`): held for the whole test so it cannot overwrite
    // `GLOBAL_MANAGER` while the convergence test is mid-cycle.
    let _global = global_guard().await;
    let Some(garage) = garage::shared() else {
        eprintln!("SKIP: no Garage binary; set GARAGE_BIN to run the e2e test");
        return;
    };
    let bucket = garage.create_unique_bucket("app-exitflush");
    let settings = settings_for(garage, &bucket);
    let creds = creds_for(garage);

    let root = tempfile::tempdir().expect("root");
    let state = tempfile::tempdir().expect("state");
    let mgr = SyncManager::new_inert();
    mgr.configure(
        settings,
        creds,
        root.path().to_path_buf(),
        state.path().to_path_buf(),
    )
    .expect("configure");
    sync::install_global_manager(mgr.clone());

    // Queue a small sidecar upload.
    let sidecar = root.path().join("flush/IMG_0009.NEF.rrdata");
    std::fs::create_dir_all(sidecar.parent().unwrap()).expect("mkdir");
    save_sidecar(None, &sidecar, &edit(2, 0.0), WriteOrigin::User).expect("save");

    // The exit flush must return within a hard outer bound — it must never
    // hang. Its own budget is ~2 s; give the outer guard generous slack.
    let start = Instant::now();
    let outcome = tokio::time::timeout(
        Duration::from_secs(15),
        mgr.exit_flush(Duration::from_secs(2)),
    )
    .await;
    assert!(
        outcome.is_ok(),
        "exit_flush must return within its budget, never hang"
    );
    outcome.unwrap().expect("exit_flush result");
    assert!(
        start.elapsed() < Duration::from_secs(10),
        "exit_flush must respect its ~2s budget (took {:?})",
        start.elapsed()
    );
}
