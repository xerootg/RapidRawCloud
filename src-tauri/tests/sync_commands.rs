//! U8 §3.3/§3.5/§3.6/§3.8 Garage-backed command-surface tests. Feature-gated
//! to `sync`; set `GARAGE_BIN=/tmp/claude-0/garage` to run them against a real
//! Garage bucket.
//!
//! These drive the plain-Rust command cores (`sync::commands::*_core`) and the
//! new `SyncManager` control-surface methods directly — exactly as P1/P2 drive
//! the manager API without a Tauri app harness (the IPC wrappers are thin and
//! covered by that same seam). They pin:
//!   * a configure → set-credentials → sidecar-edit → status round-trip whose
//!     `SyncStatusDto` reflects pending-then-synced and `credentialsConfigured`;
//!   * "make available offline" hydrating a stub to the real original bytes;
//!   * pin + "free up space" flipping pinned / evicting a named path to a stub;
//!   * **credential non-leakage** (blocker-class): neither the full `AppSettings`
//!     serialization nor the `SyncStatusDto` ever carries access/secret key
//!     material — the webview learns only `credentialsConfigured: bool`;
//!   * recently-deleted / restore, resolve-conflict, retire-device, verify.
//!
//! U8 RED: the command cores and the new manager methods are `todo!()`, so each
//! test panics at the gap. The structural credential-leak assertion in
//! `credentials_never_leak` runs *before* the gap and already holds (the guard
//! that must never regress).

#![cfg(feature = "sync")]

mod common;

use std::path::Path;
use std::sync::OnceLock;

use common::garage;
use rapidraw_lib::sync::commands::{
    self, SyncStatusDto, configure_core, credentials_configured_core, hydrate_core,
    set_credentials_core, status_core,
};
use rapidraw_lib::sync::{
    self, AppSettings, ConflictKeep, Credentials, FileCredentialStore, ImageMetadata, SyncManager,
    SyncSettings, WriteOrigin, save_sidecar,
};
use tokio::sync::{Mutex, MutexGuard};

/// Serializes installers of the one process-global `SyncManager` (see the
/// sibling e2e/hydration tests): held across async awaits, so await-aware.
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

/// Device A writes an original at `rel`, enqueues it, and runs one cycle so
/// the bytes + journal land in the bucket. Returns (blake3_hex, size, mtime).
async fn upload_original(
    mgr: &SyncManager,
    root: &Path,
    rel: &str,
    bytes: &[u8],
) -> (String, u64, i64) {
    let path = root.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).expect("mkdir original");
    std::fs::write(&path, bytes).expect("write original");
    let blake3_hex = blake3::hash(bytes).to_hex().to_string();
    let size = bytes.len() as u64;
    let mtime_unix = mtime_unix_secs(&path);
    mgr.note_new_original(&path);
    mgr.run_once().await.expect("upload cycle");
    (blake3_hex, size, mtime_unix)
}

// =========================================================================

/// configure + set_credentials (via the store) + a sidecar edit through the
/// chokepoint + status reflects pending → synced, with `credentialsConfigured`.
#[tokio::test]
async fn command_cycle_reflects_pending_then_synced() {
    let _g = global_guard().await;
    let Some(garage) = garage::shared() else {
        eprintln!("SKIP: no Garage binary; set GARAGE_BIN to run");
        return;
    };
    let bucket = garage.create_unique_bucket("u8-cycle");
    let settings = settings_for(garage, &bucket);
    let gcreds = creds_for(garage);

    let root = tempfile::tempdir().expect("root");
    let state = tempfile::tempdir().expect("state");
    let store = FileCredentialStore::new(state.path());
    let mgr = SyncManager::new_inert();

    // Credentials go to the Rust-only store (RED: `set_credentials_core` todo).
    set_credentials_core(&store, gcreds.access_key.clone(), gcreds.secret_key.clone())
        .expect("set credentials");
    assert!(
        credentials_configured_core(&store),
        "both halves present => credentials_configured"
    );

    // (Re)configure the manager from settings + the store (RED: todo).
    configure_core(
        &mgr,
        &store,
        settings.clone(),
        root.path().to_path_buf(),
        state.path().to_path_buf(),
    )
    .expect("configure_core");
    sync::install_global_manager(mgr.clone());

    // An edit through the real chokepoint must register pending.
    let sidecar = root.path().join("trip/DSC_0001.NEF.rrdata");
    std::fs::create_dir_all(sidecar.parent().unwrap()).expect("mkdir");
    save_sidecar(None, &sidecar, &edit(4, 0.75), WriteOrigin::User)
        .expect("save through chokepoint");

    let before: SyncStatusDto = status_core(&mgr, credentials_configured_core(&store));
    assert!(before.configured, "status.configured after configure_core");
    assert!(
        before.credentials_configured,
        "status.credentialsConfigured"
    );
    assert!(before.pending_up >= 1, "a fresh edit is pending upload");

    mgr.run_once().await.expect("sync cycle");

    let after: SyncStatusDto = status_core(&mgr, credentials_configured_core(&store));
    assert_eq!(after.pending_up, 0, "after a cycle nothing is pending up");
    assert_eq!(
        after.dirty_unbacked, 0,
        "after a cycle nothing is dirty-unbacked"
    );
}

/// "Make available offline": `sync_hydrate` installs the original over a stub.
#[tokio::test]
async fn hydrate_installs_original_on_stub() {
    let _g = global_guard().await;
    let Some(garage) = garage::shared() else {
        eprintln!("SKIP: no Garage binary; set GARAGE_BIN to run");
        return;
    };
    let bucket = garage.create_unique_bucket("u8-hydrate");
    let settings = settings_for(garage, &bucket);
    let creds = creds_for(garage);

    // Device A publishes an original.
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
    let rel = "trip/DSC_1001.NEF";
    let bytes = original_bytes(7, 64 * 1024);
    let (blake3_hex, size, mtime_unix) = upload_original(&mgr_a, root_a.path(), rel, &bytes).await;

    // Device B stubs it, then hydrates through the command core.
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
    let stub_path = root_b.path().join(rel);
    std::fs::create_dir_all(stub_path.parent().unwrap()).expect("mkdir stub dir");
    mgr_b
        .create_stub(&stub_path, &blake3_hex, size, mtime_unix)
        .expect("create stub");
    assert!(mgr_b.is_stub(&stub_path), "pre: path is a stub");

    // RED: `hydrate_core` is todo!().
    hydrate_core(&mgr_b, stub_path.clone()).expect("hydrate");

    assert!(!mgr_b.is_stub(&stub_path), "post: no longer a stub");
    assert_eq!(
        std::fs::read(&stub_path).expect("read hydrated"),
        bytes,
        "real bytes installed"
    );
    assert_eq!(
        mtime_unix_secs(&stub_path),
        mtime_unix,
        "remote mtime restored"
    );
}

/// pin + "free up space": pin protects a path; `evict_paths` demotes an
/// unpinned, verified original back to a stub.
#[tokio::test]
async fn pin_and_free_space_flip_pinned_and_evict() {
    let _g = global_guard().await;
    let Some(garage) = garage::shared() else {
        eprintln!("SKIP: no Garage binary; set GARAGE_BIN to run");
        return;
    };
    let bucket = garage.create_unique_bucket("u8-free");
    let settings = settings_for(garage, &bucket);
    let creds = creds_for(garage);

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
    let (hp, hs, hm) = upload_original(
        &mgr_a,
        root_a.path(),
        "a/keep.NEF",
        &original_bytes(3, 32 * 1024),
    )
    .await;
    let (fp, fs_, fm) = upload_original(
        &mgr_a,
        root_a.path(),
        "a/free.NEF",
        &original_bytes(9, 32 * 1024),
    )
    .await;

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

    let keep = root_b.path().join("a/keep.NEF");
    let free = root_b.path().join("a/free.NEF");
    std::fs::create_dir_all(keep.parent().unwrap()).expect("mkdir");
    mgr_b.create_stub(&keep, &hp, hs, hm).expect("stub keep");
    mgr_b.create_stub(&free, &fp, fs_, fm).expect("stub free");
    mgr_b.ensure_local(&keep, "test").expect("hydrate keep");
    mgr_b.ensure_local(&free, "test").expect("hydrate free");

    // Pin `keep` (existing API), then "free up space" over both paths.
    let pinned = mgr_b.pin_paths(&[keep.clone()], true).expect("pin");
    assert_eq!(pinned, 1, "one path newly pinned");

    // RED: `evict_paths` is todo!(). Pinned `keep` must survive; `free` demotes.
    let demoted = mgr_b
        .evict_paths(&[keep.clone(), free.clone()])
        .expect("free space");
    assert_eq!(demoted, 1, "only the unpinned, verified path is demoted");
    assert!(!mgr_b.is_stub(&keep), "pinned path stays hydrated");
    assert!(mgr_b.is_stub(&free), "unpinned path is demoted to a stub");
}

/// Credential non-leakage (blocker-class): neither the full `AppSettings`
/// serialization (what the webview round-trips via `save_settings`) nor the
/// `SyncStatusDto` carries access/secret-key material — only
/// `credentialsConfigured: bool`.
#[tokio::test]
async fn credentials_never_leak() {
    // (1) Structural guard that must ALWAYS hold — runs before any gap.
    let mut app = AppSettings::default();
    app.sync = SyncSettings {
        enabled: true,
        endpoint: "https://garage.example".to_string(),
        bucket: "b".to_string(),
        region: "garage".to_string(),
        ..SyncSettings::default()
    };
    let settings_json = serde_json::to_string(&app).expect("serialize AppSettings");
    for needle in ["accessKey", "access_key", "secretKey", "secret_key"] {
        assert!(
            !settings_json.contains(needle),
            "AppSettings serialization must never contain `{needle}`: {settings_json}"
        );
    }

    // (2) A real secret written to the store, then the status projection must
    // expose only `credentialsConfigured` — RED: `set_credentials_core` todo.
    let state = tempfile::tempdir().expect("state");
    let store = FileCredentialStore::new(state.path());
    const SECRET_AK: &str = "AKIA_LEAK_CANARY_0001";
    const SECRET_SK: &str = "s3cr3t_LEAK_CANARY_do_not_echo";
    set_credentials_core(&store, SECRET_AK.to_string(), SECRET_SK.to_string())
        .expect("set credentials");
    assert!(credentials_configured_core(&store), "store now configured");

    let mgr = SyncManager::new_inert();
    let dto = status_core(&mgr, credentials_configured_core(&store));
    let dto_json = serde_json::to_string(&dto).expect("serialize dto");
    assert!(
        dto_json.contains("credentialsConfigured"),
        "dto exposes the bool"
    );
    for needle in [SECRET_AK, SECRET_SK, "accessKey", "secretKey"] {
        assert!(
            !dto_json.contains(needle),
            "SyncStatusDto must never carry `{needle}`: {dto_json}"
        );
    }
    let _ = commands::credentials_configured_core; // keep the module import used
}

/// Recently-deleted listing + restore (§2.7 / §3.8).
#[tokio::test]
async fn recently_deleted_and_restore() {
    let _g = global_guard().await;
    let Some(garage) = garage::shared() else {
        eprintln!("SKIP: no Garage binary; set GARAGE_BIN to run");
        return;
    };
    let bucket = garage.create_unique_bucket("u8-deleted");
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

    // RED: `recently_deleted` / `restore` are todo!().
    let deleted = mgr.recently_deleted().expect("recently deleted");
    if let Some(first) = deleted.first() {
        mgr.restore(Path::new(&first.path)).expect("restore");
    }
}

/// Resolve-conflict, retire-device, and verify-library control surface.
#[tokio::test]
async fn conflict_retire_verify_surface() {
    let _g = global_guard().await;
    let Some(garage) = garage::shared() else {
        eprintln!("SKIP: no Garage binary; set GARAGE_BIN to run");
        return;
    };
    let bucket = garage.create_unique_bucket("u8-admin");
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

    // RED: each of these is todo!().
    mgr.resolve_conflict(&root.path().join("x.NEF"), ConflictKeep::Copy)
        .expect("resolve conflict");
    mgr.retire_device("some-dead-device")
        .await
        .expect("retire device");
    let report = mgr.verify_library().await.expect("verify library");
    assert_eq!(
        report.corrupt, 0,
        "a clean library verifies with no corruption"
    );
}
