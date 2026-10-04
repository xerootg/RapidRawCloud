//! P5 review round-1 major regression: ARCHITECTURE.md §5.1 says redb's
//! file lock plus an in-process `Mutex` "make app-process and
//! worker-process cycles mutually exclusive" — meant to bound a single
//! in-flight cycle, not the app's entire time in the background. Before
//! `SyncManager::release_for_background`/`reacquire_after_foreground`
//! existed, `configure()` opened its `Configured` (and the redb handle
//! inside it) once and nothing short of process death ever closed it, so
//! a WorkManager-triggered bridge cycle — which opens its own independent
//! redb handle on the same `state.redb`, possibly in the very same OS
//! process under WorkManager's default in-process executor — saw the lock
//! held (and backed off) for as long as the app merely stayed alive in
//! the background, starving every background sync window.
//!
//! These tests stand a second `SyncManager` in for the WorkManager bridge's
//! independent open (the same "two processes, one state dir" shape
//! `tests/stubs_restart.rs` already uses to simulate a restart) — pure
//! local redb lock contention, no Garage/network needed, so these always
//! run under `--features sync`.

#![cfg(feature = "sync")]

use rapidraw_lib::sync::{Credentials, SyncManager, SyncSettings};

fn settings() -> SyncSettings {
    SyncSettings {
        enabled: true,
        // Never contacted: configure/release/reacquire do no network I/O.
        endpoint: "http://127.0.0.1:1".to_string(),
        bucket: "p5-bg-handoff".to_string(),
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

/// The main regression: release actually frees the OS lock for a second
/// opener, and a later reacquire (once that second opener lets go) resumes
/// the original manager without needing settings/credentials resupplied.
#[test]
fn release_for_background_frees_the_lock_and_reacquire_resumes_cleanly() {
    let root = tempfile::tempdir().expect("root").keep();
    let state = tempfile::tempdir().expect("state").keep();

    let mgr1 = SyncManager::new_inert();
    mgr1.configure(settings(), creds(), root.clone(), state.clone())
        .expect("configure mgr1 (the foreground app)");
    assert!(mgr1.is_configured());

    // Baseline: while mgr1 is foreground-configured, a second independent
    // opener on the SAME state dir (standing in for the WorkManager
    // bridge's own redb open) must see the lock genuinely held — otherwise
    // this whole regression test would be vacuous.
    let mgr2 = SyncManager::new_inert();
    mgr2.configure(settings(), creds(), root.clone(), state.clone())
        .expect_err(
            "a second opener must see the redb lock held while mgr1 is foreground-configured",
        );

    // The fix: releasing for background actually drops mgr1's handle, so
    // the second opener can now acquire it — exactly what a
    // WorkManager-triggered bridge cycle needs while the app is merely
    // backgrounded (not killed).
    mgr1.release_for_background()
        .expect("release_for_background");
    // Still conceptually configured — merely parked — per the documented
    // contract (not literally torn down / NotConfigured forever).
    assert!(
        mgr1.is_configured(),
        "release_for_background must not flip is_configured() false"
    );

    mgr2.configure(settings(), creds(), root.clone(), state.clone())
        .expect("a second opener must succeed once mgr1 has released for background");

    // mgr1 cannot reacquire yet: mgr2 still holds the lock (the documented
    // best-effort caveat — this is not expected to block/retry on its
    // own).
    mgr1.reacquire_after_foreground()
        .expect_err("reacquire must fail while another opener still holds the redb lock");

    // Once mgr2 lets go (its own background release, or process exit), the
    // original manager can resume exactly where it left off, without the
    // caller resupplying settings or credentials.
    drop(mgr2);
    mgr1.reacquire_after_foreground()
        .expect("reacquire_after_foreground once the lock is free again");
    assert!(mgr1.is_configured());

    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(&state);
}

/// Idempotent in the "already open" direction: calling reacquire while
/// still fully configured (never released) must be a harmless no-op —
/// e.g. a cold start calling `onResume` before any `release` ever ran, or
/// a stray double-fire of the Android lifecycle callback.
#[test]
fn reacquire_after_foreground_is_a_noop_when_still_configured() {
    let root = tempfile::tempdir().expect("root").keep();
    let state = tempfile::tempdir().expect("state").keep();

    let mgr = SyncManager::new_inert();
    mgr.configure(settings(), creds(), root.clone(), state.clone())
        .expect("configure");

    mgr.reacquire_after_foreground()
        .expect("reacquire_after_foreground must be a no-op Ok when already configured");
    assert!(mgr.is_configured());

    // And still genuinely usable (not torn down by the no-op call): a
    // second independent opener on the same dir must still see the lock
    // held.
    let mgr2 = SyncManager::new_inert();
    mgr2.configure(settings(), creds(), root.clone(), state.clone())
        .expect_err("the no-op reacquire must not have released the lock");

    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(&state);
}

/// Idempotent in the "never configured" direction: both halves must be
/// safe, error-free no-ops on a brand-new inert manager — the Android
/// lifecycle callbacks fire unconditionally (e.g. `onStop`/`onResume` on
/// the very first cold start, before any `configure()` call), and must
/// never panic or surface an error with nothing to release/reacquire.
#[test]
fn release_and_reacquire_without_ever_configuring_are_safe_noops() {
    let mgr = SyncManager::new_inert();
    assert!(!mgr.is_configured());

    mgr.release_for_background()
        .expect("release_for_background on a never-configured manager must be a safe no-op");
    mgr.reacquire_after_foreground()
        .expect("reacquire_after_foreground on a never-configured manager must be a safe no-op");

    assert!(
        !mgr.is_configured(),
        "neither call may spuriously flip is_configured() on a manager that was never configured"
    );
}
