//! P2 §3.5 restart regression: the in-memory `stub_set` is a *mirror of
//! redb* and must be rebuilt from the durable `ItemState::Stub` records when
//! a fresh process reopens an existing state dir. Otherwise, after any app
//! restart, `is_stub()` / `is_cloud_placeholder()` answer `false` for every
//! persisted stub, so every §3.5 guard site bypasses hydration and reads (or
//! copies) the 0-byte stub as content.
//!
//! These are deliberately Garage-free: `create_stub` and the reopen path are
//! pure-local (redb + a 0-byte file), so the regression reproduces without a
//! bucket and always runs under `--features sync`.

#![cfg(feature = "sync")]

use std::sync::{Mutex, MutexGuard, OnceLock};

use rapidraw_lib::sync::{self, Credentials, SyncManager, SyncSettings};

/// Serializes installers of the process-global `SyncManager` within this
/// test binary.
fn serial() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

fn settings() -> SyncSettings {
    SyncSettings {
        enabled: true,
        // Never contacted: create_stub and reopen do no network I/O.
        endpoint: "http://127.0.0.1:1".to_string(),
        bucket: "p2-restart".to_string(),
        region: "garage".to_string(),
        force_path_style: true,
        ..SyncSettings::default()
    }
}

fn creds() -> Credentials {
    Credentials {
        access_key: "AKIATEST".to_string(),
        secret_key: "secrettest".to_string(),
    }
}

#[test]
fn stub_set_is_rehydrated_from_redb_after_restart() {
    let _g = serial();

    // Keep root + state dirs alive across the simulated restart.
    let root = tempfile::tempdir().expect("root").keep();
    let state = tempfile::tempdir().expect("state").keep();

    let rel = "trip/DSC_9001.NEF";
    let stub_path = root.join(rel);
    std::fs::create_dir_all(stub_path.parent().unwrap()).expect("mkdir stub dir");

    // Deterministic remote facts (no network): a valid 64-hex blake3 and a
    // fixed remote mtime.
    let remote_bytes = (0..4096u32).map(|i| (i % 251) as u8).collect::<Vec<u8>>();
    let blake3_hex = blake3::hash(&remote_bytes).to_hex().to_string();
    let remote_size = remote_bytes.len() as u64;
    let remote_mtime = 1_700_000_000i64;

    // --- First process: configure, create the stub, confirm it registers.
    {
        let mgr = SyncManager::new_inert();
        mgr.configure(settings(), creds(), root.clone(), state.clone())
            .expect("configure (first process)");
        sync::install_global_manager(mgr.clone());

        mgr.create_stub(&stub_path, &blake3_hex, remote_size, remote_mtime)
            .expect("create stub");

        assert!(mgr.is_stub(&stub_path), "stub must register in-process");
        assert!(
            sync::is_cloud_placeholder(&stub_path),
            "a fresh stub must read as a cloud placeholder"
        );
        assert_eq!(
            mgr.item_sync_state(&stub_path).as_deref(),
            Some("stub"),
            "the durable redb record must be in the Stub state"
        );

        // Simulate process exit: drop the manager so the redb handle (and the
        // in-memory stub_set) are released. The process-global slot also holds
        // a clone, so evict it with a throwaway inert manager — otherwise the
        // old `Configured` (and its redb advisory lock) outlives this block
        // and the reopen below fails with "already open in another process".
        drop(mgr);
        sync::install_global_manager(SyncManager::new_inert());
    }

    // --- Second process ("restart"): a brand-new manager reopens the SAME
    // state dir. The 0-byte stub file is still on disk; the durable redb
    // record still says Stub. The in-memory mirror must be rebuilt from it.
    let mgr2 = SyncManager::new_inert();
    mgr2.configure(settings(), creds(), root.clone(), state.clone())
        .expect("configure (second process / restart)");
    sync::install_global_manager(mgr2.clone());

    // Sanity: the durable record survived and the file is still a 0-byte stub.
    assert_eq!(
        mgr2.item_sync_state(&stub_path).as_deref(),
        Some("stub"),
        "the persisted redb record must still be Stub after reopen"
    );
    assert_eq!(
        std::fs::metadata(&stub_path).expect("stat stub").len(),
        0,
        "the stub is still a 0-byte file after reopen"
    );

    // The regression: after restart the mirror must recognize the persisted
    // stub. Before the fix, stub_set starts empty and is never seeded from
    // redb, so both of these are false and every guard site reads the 0-byte
    // file as content.
    assert!(
        mgr2.is_stub(&stub_path),
        "is_stub must be rebuilt from the durable redb Stub record after restart"
    );
    assert!(
        sync::is_cloud_placeholder(&stub_path),
        "is_cloud_placeholder must be true for a persisted stub after restart"
    );

    // Cleanup the leaked temp dirs.
    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(&state);
}

/// §3.5 verify-before-truncate: `create_stub` must never clobber a local
/// original that is actually present with real bytes. The apply/reconcile
/// caller is contracted to prove the original is remote-only first, but a
/// reconcile race or mis-decision could still target such a relkey — and the
/// underlying `File::create` would truncate it to a 0-byte stub with no
/// recovery. The defensive guard refuses that, writing nothing.
#[test]
fn create_stub_refuses_to_truncate_an_existing_non_stub_file() {
    let _g = serial();

    let root = tempfile::tempdir().expect("root").keep();
    let state = tempfile::tempdir().expect("state").keep();

    let rel = "trip/REAL_0001.NEF";
    let real_path = root.join(rel);
    std::fs::create_dir_all(real_path.parent().unwrap()).expect("mkdir real dir");

    // A local original actually present with real bytes — the exact case a
    // reconcile mis-decision could wrongly drive create_stub against.
    let real_bytes = (0..8192u32).map(|i| (i % 253) as u8).collect::<Vec<u8>>();
    std::fs::write(&real_path, &real_bytes).expect("write real original");

    let blake3_hex = blake3::hash(&real_bytes).to_hex().to_string();
    let remote_size = real_bytes.len() as u64;
    let remote_mtime = 1_700_000_000i64;

    let mgr = SyncManager::new_inert();
    mgr.configure(settings(), creds(), root.clone(), state.clone())
        .expect("configure");
    sync::install_global_manager(mgr.clone());

    // The guard: refuse rather than truncate content never proven discardable.
    mgr.create_stub(&real_path, &blake3_hex, remote_size, remote_mtime)
        .expect_err("create_stub must refuse to truncate an existing non-stub file");

    // The bytes survive untouched, and no Stub record was recorded.
    assert_eq!(
        std::fs::read(&real_path).expect("read preserved original"),
        real_bytes,
        "create_stub must not truncate the existing local original"
    );
    assert_ne!(
        mgr.item_sync_state(&real_path).as_deref(),
        Some("stub"),
        "no Stub record may be recorded when the guard bails"
    );
    assert!(
        !mgr.is_stub(&real_path),
        "the mirror must not mark a refused target as a stub"
    );

    drop(mgr);
    sync::install_global_manager(SyncManager::new_inert());

    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(&state);
}
