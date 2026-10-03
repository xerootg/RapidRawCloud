//! P2 §3.5 Garage-backed guard-site tests: every reader that previously
//! bypassed the placeholder check now hydrates a stub first, so none of them
//! ever decodes a 0-byte placeholder. Feature-gated to `sync`; set
//! `GARAGE_BIN=/tmp/claude-0/garage` to run.
//!
//! `load_image` and `export_images_impl` both guard through the single
//! one-line hook `sync::hooks::ensure_local(&source_path, reason)` before any
//! read (they otherwise need a full Tauri `AppHandle` + GPU `AppState` to
//! drive). This suite pins that exact seam for them, and drives the real,
//! directly-callable `copy_files` guard end to end (hydrate-then-copy, never
//! a 0-byte copy).

#![cfg(feature = "sync")]

mod common;

use std::path::Path;
use std::sync::OnceLock;

use common::garage;
use rapidraw_lib::sync::{self, Credentials, SyncManager, SyncSettings};
use tokio::sync::{Mutex, MutexGuard};

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

/// Facts a stub needs, plus the source bytes for assertions.
struct RemoteOriginal {
    blake3_hex: String,
    size: u64,
    mtime_unix: i64,
    bytes: Vec<u8>,
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
        bytes: bytes.to_vec(),
    }
}

/// Boots A (uploader) + B (stub holder) against one bucket, publishes `rel`
/// from A, and returns B's manager/root/temp handles plus a stub path + the
/// remote facts. B is installed as the process-global manager (so the hook
/// and `is_cloud_placeholder` route through it).
async fn stub_on_device_b(
    garage: &garage::Garage,
    tag: &str,
    rel: &str,
    bytes: &[u8],
) -> (
    std::sync::Arc<SyncManager>,
    tempfile::TempDir,
    tempfile::TempDir,
    std::path::PathBuf,
    RemoteOriginal,
) {
    let bucket = garage.create_unique_bucket(tag);
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
    let remote = upload_original(&mgr_a, root_a.path(), rel, bytes).await;

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
        .create_stub(
            &stub_path,
            &remote.blake3_hex,
            remote.size,
            remote.mtime_unix,
        )
        .expect("create stub");

    (mgr_b, root_b, state_b, stub_path, remote)
}

#[tokio::test]
async fn load_image_guard_seam_hydrates_a_stub_before_read() {
    let _g = global_guard().await;
    let Some(garage) = garage::shared() else {
        eprintln!("SKIP: no Garage binary; set GARAGE_BIN to run");
        return;
    };
    let (_mgr_b, _root_b, _state_b, stub_path, remote) = stub_on_device_b(
        garage,
        "p2-guard-loadimage",
        "a/LOAD_01.NEF",
        &original_bytes(3, 96 * 1024),
    )
    .await;

    // Before: a stub reads as a placeholder and is 0 bytes — the raw read a
    // guarded `load_image` must never perform.
    assert!(sync::is_cloud_placeholder(&stub_path));
    assert_eq!(std::fs::metadata(&stub_path).unwrap().len(), 0);

    // The exact one-line hook `load_image` calls at `image_loader.rs:940`.
    let hydrated = sync::hooks::ensure_local(&stub_path, "load_image").expect("ensure_local");
    assert_eq!(hydrated, stub_path);

    // After: real bytes present, placeholder flag cleared — no 0-byte decode.
    assert!(!sync::is_cloud_placeholder(&stub_path));
    assert_eq!(
        std::fs::read(&stub_path).expect("read hydrated"),
        remote.bytes,
        "the guard must install the real bytes before any reader runs"
    );
}

#[tokio::test]
async fn export_guard_seam_hydrates_a_stub_before_per_image_load() {
    let _g = global_guard().await;
    let Some(garage) = garage::shared() else {
        eprintln!("SKIP: no Garage binary; set GARAGE_BIN to run");
        return;
    };
    let (_mgr_b, _root_b, _state_b, stub_path, remote) = stub_on_device_b(
        garage,
        "p2-guard-export",
        "b/EXP_01.CR3",
        &original_bytes(9, 128 * 1024),
    )
    .await;

    assert_eq!(std::fs::metadata(&stub_path).unwrap().len(), 0);

    // The exact one-line hook `export_images_impl` calls before the per-image
    // `load_and_composite`.
    sync::hooks::ensure_local(&stub_path, "export").expect("ensure_local");

    assert!(
        std::fs::metadata(&stub_path).unwrap().len() > 0,
        "export guard must hydrate"
    );
    assert_eq!(std::fs::read(&stub_path).unwrap(), remote.bytes);
}

#[tokio::test]
async fn copy_files_hydrates_then_copies_real_bytes_not_the_stub() {
    let _g = global_guard().await;
    let Some(garage) = garage::shared() else {
        eprintln!("SKIP: no Garage binary; set GARAGE_BIN to run");
        return;
    };
    let (_mgr_b, _root_b, _state_b, stub_path, remote) = stub_on_device_b(
        garage,
        "p2-guard-copy",
        "c/COPY_01.NEF",
        &original_bytes(5, 72 * 1024),
    )
    .await;

    // Copy the stub to a fresh destination directory outside the library.
    let dest_dir = tempfile::tempdir().expect("dest dir");
    sync::copy_files_guarded(
        vec![stub_path.to_string_lossy().into_owned()],
        dest_dir.path().to_string_lossy().into_owned(),
    )
    .expect("copy_files");

    // The copied file must carry the real (hydrated) bytes, never 0 bytes.
    let copied = dest_dir.path().join(stub_path.file_name().unwrap());
    assert!(
        copied.exists(),
        "copy_files must place the file at {copied:?}"
    );
    let copied_bytes = std::fs::read(&copied).expect("read copied");
    assert_eq!(
        copied_bytes, remote.bytes,
        "copy_files must copy hydrated bytes, not the 0-byte stub"
    );
    assert_eq!(copied_bytes.len() as u64, remote.size);
}
