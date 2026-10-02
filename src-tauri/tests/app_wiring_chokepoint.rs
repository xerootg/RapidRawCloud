//! Chokepoint unit tests for the `save_sidecar` refactor (ARCHITECTURE.md
//! §3.4), with the `sync` feature ON and no network.
//!
//! These pin the behaviors that make the chokepoint safe:
//!   * atomic write (no partial file; no leftover temp siblings; a failed
//!     write leaves nothing behind),
//!   * a 0-byte / corrupt existing sidecar is quarantined to
//!     `.corrupt-<ts>` and the write is aborted (the `load_sidecar` trap —
//!     a real edit is never clobbered by a defaults-based document),
//!   * the §2.5 churn gate: an EXIF-cache rewrite with the SAME semantic
//!     hash does not notify the engine, a real edit does,
//!   * the per-path lock serializes concurrent writers (no interleave /
//!     corruption).

#![cfg(feature = "sync")]

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use rapidraw_lib::sync::{
    self, Credentials, ImageMetadata, SyncManager, SyncSettings, WriteOrigin, save_sidecar,
};

/// Serializes the tests that install the process-global `SyncManager`, so
/// the shared global does not race under libtest's parallel runner.
fn global_guard() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

/// A sidecar with a genuine edit: `rating` + one adjustment, plus `exif`
/// cache noise the §2.5 semantic projection excludes.
fn edit(rating: u8, exposure: f64) -> ImageMetadata {
    let mut exif = HashMap::new();
    exif.insert("camera".to_string(), "TestCam".to_string());
    ImageMetadata {
        version: 1,
        rating,
        adjustments: serde_json::json!({ "exposure": exposure, "contrast": 0 }),
        tags: Some(vec![]),
        exif: Some(exif),
    }
}

/// A §2.5 churn rewrite of `base`: identical rating / tags / adjustments,
/// only the `exif` cache differs — the `sem_hash` must be unchanged.
fn churn(base: &ImageMetadata) -> ImageMetadata {
    let mut m = base.clone();
    let mut exif = HashMap::new();
    exif.insert("camera".to_string(), "TestCam".to_string());
    exif.insert("cached_at".to_string(), "1769912345".to_string());
    exif.insert("lens".to_string(), "50mm".to_string());
    m.exif = Some(exif);
    m
}

fn configured_manager(
    sync_root: &std::path::Path,
    state_dir: &std::path::Path,
) -> Arc<SyncManager> {
    let mgr = SyncManager::new_inert();
    let settings = SyncSettings {
        enabled: true,
        endpoint: "http://127.0.0.1:1".to_string(),
        bucket: "chokepoint-test".to_string(),
        region: "garage".to_string(),
        ..SyncSettings::default()
    };
    let creds = Credentials {
        access_key: "AKIATEST".to_string(),
        secret_key: "secrettest".to_string(),
    };
    mgr.configure(
        settings,
        creds,
        sync_root.to_path_buf(),
        state_dir.to_path_buf(),
    )
    .expect("configure");
    mgr
}

#[test]
fn atomic_write_persists_full_bytes_and_leaves_no_temp_sibling() {
    let dir = tempfile::tempdir().expect("tempdir");
    let sidecar = dir.path().join("img.NEF.rrdata");
    let meta = edit(3, 0.5);

    save_sidecar(None, &sidecar, &meta, WriteOrigin::User).expect("save_sidecar");

    let written = std::fs::read(&sidecar).expect("sidecar present");
    let expected = serde_json::to_vec_pretty(&meta).expect("serialize");
    assert_eq!(
        written, expected,
        "the chokepoint must write exactly the pretty-printed document"
    );

    // No leftover temp file from the temp+rename.
    let siblings: Vec<_> = std::fs::read_dir(dir.path())
        .expect("read_dir")
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(
        siblings,
        vec!["img.NEF.rrdata".to_string()],
        "atomic temp+rename must leave no sibling temp file, got {siblings:?}"
    );
}

#[test]
fn failed_write_leaves_no_partial_file() {
    // A parent directory that does not exist: the write must fail and
    // create nothing (no partial, no temp).
    let dir = tempfile::tempdir().expect("tempdir");
    let missing = dir.path().join("does-not-exist").join("img.NEF.rrdata");

    let result = save_sidecar(None, &missing, &edit(2, 0.1), WriteOrigin::User);
    assert!(result.is_err(), "writing under a missing dir must error");
    assert!(
        !missing.exists() && !missing.parent().map(|p| p.exists()).unwrap_or(false),
        "a failed atomic write must leave nothing behind"
    );
}

#[test]
fn corrupt_existing_sidecar_is_quarantined_and_write_aborts() {
    let _g = global_guard();
    let root = tempfile::tempdir().expect("root");
    let state = tempfile::tempdir().expect("state");
    let mgr = configured_manager(root.path(), state.path());
    sync::install_global_manager(mgr);

    let sidecar = root.path().join("photo.RAF.rrdata");
    // A 0-byte / unparseable existing sidecar (the A4 trap).
    std::fs::write(&sidecar, b"").expect("seed corrupt sidecar");

    let result = save_sidecar(None, &sidecar, &edit(5, -1.0), WriteOrigin::User);
    assert!(
        result.is_err(),
        "a corrupt existing sidecar must abort the write (no defaults clobber)"
    );

    // The corrupt file is quarantined, not overwritten with defaults.
    let quarantined: Vec<_> = std::fs::read_dir(root.path())
        .expect("read_dir")
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.contains(".rrdata.corrupt-"))
        .collect();
    assert_eq!(
        quarantined.len(),
        1,
        "the 0-byte sidecar must be renamed to .rrdata.corrupt-<ts>, got {quarantined:?}"
    );
}

#[test]
fn churn_rewrite_does_not_notify_but_real_edit_does() {
    let _g = global_guard();
    let root = tempfile::tempdir().expect("root");
    let state = tempfile::tempdir().expect("state");
    let mgr = configured_manager(root.path(), state.path());
    sync::install_global_manager(mgr.clone());

    let sidecar = root.path().join("burst/img-0001.NEF.rrdata");
    std::fs::create_dir_all(sidecar.parent().unwrap()).expect("mkdir");

    let base = edit(3, 0.5);
    save_sidecar(None, &sidecar, &base, WriteOrigin::User).expect("first edit");
    let after_first = mgr.notify_count();
    assert_eq!(
        after_first, 1,
        "a genuine first edit must notify the engine"
    );

    // A churn rewrite (EXIF cache only) must not notify again (§2.5).
    save_sidecar(None, &sidecar, &churn(&base), WriteOrigin::ExifCache).expect("churn rewrite");
    assert_eq!(
        mgr.notify_count(),
        after_first,
        "a same-sem_hash rewrite must not fire notify_sidecar_saved (churn gate)"
    );

    // A real edit must notify again.
    save_sidecar(None, &sidecar, &edit(5, -1.0), WriteOrigin::User).expect("real edit");
    assert_eq!(
        mgr.notify_count(),
        after_first + 1,
        "a genuine edit must fire notify_sidecar_saved"
    );
}

#[cfg(unix)]
#[test]
fn atomic_write_preserves_existing_sidecar_mode_on_overwrite() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().expect("tempdir");
    let sidecar = dir.path().join("keepmode.NEF.rrdata");
    // Seed a valid sidecar with an unusual, tighter-than-default mode.
    std::fs::write(&sidecar, serde_json::to_vec_pretty(&edit(0, 0.0)).unwrap())
        .expect("seed valid sidecar");
    std::fs::set_permissions(&sidecar, std::fs::Permissions::from_mode(0o640)).expect("chmod");

    save_sidecar(None, &sidecar, &edit(3, 0.2), WriteOrigin::User).expect("overwrite");

    let mode = std::fs::metadata(&sidecar)
        .expect("stat")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(
        mode, 0o640,
        "overwriting a sidecar must preserve its existing mode (like fs::write), got {mode:o}"
    );
}

#[test]
fn per_path_lock_entry_is_pruned_after_write() {
    let dir = tempfile::tempdir().expect("tempdir");
    let sidecar = dir.path().join("prune-me.NEF.rrdata");

    save_sidecar(None, &sidecar, &edit(1, 0.0), WriteOrigin::User).expect("save");

    // Once no writer holds the lock, its process-global map entry must be
    // gone — the map does not grow one permanent entry per distinct path.
    assert!(
        !sync::sidecar_locks().contains_key(sidecar.as_path()),
        "the per-path lock entry must be pruned once no writer holds it"
    );
}

#[test]
fn per_path_lock_serializes_concurrent_writers() {
    let dir = tempfile::tempdir().expect("tempdir");
    let sidecar = dir.path().join("contended.NEF.rrdata");
    std::fs::write(&sidecar, serde_json::to_vec_pretty(&edit(0, 0.0)).unwrap())
        .expect("seed valid sidecar");

    let a = sidecar.clone();
    let b = sidecar.clone();
    let ta = std::thread::spawn(move || {
        for i in 0..50 {
            let _ = save_sidecar(None, &a, &edit(1, i as f64), WriteOrigin::Batch);
        }
    });
    let tb = std::thread::spawn(move || {
        for i in 0..50 {
            let _ = save_sidecar(None, &b, &edit(2, -(i as f64)), WriteOrigin::AiTagging);
        }
    });
    ta.join().expect("writer a");
    tb.join().expect("writer b");

    // Whatever the interleaving, the final file must be a single complete,
    // parseable document — never a half-written / interleaved one.
    let bytes = std::fs::read(&sidecar).expect("sidecar present");
    serde_json::from_slice::<ImageMetadata>(&bytes)
        .expect("the serialized sidecar must always be a complete, parseable document");
}
