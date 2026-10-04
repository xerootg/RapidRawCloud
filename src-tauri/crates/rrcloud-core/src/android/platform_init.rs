//! Process-wide idempotent guards for the two Android platform globals
//! that more than one call site in this process may need to touch before
//! anything else: `ndk_context`'s process-global JVM/Context pair, and
//! `rustls_platform_verifier`'s process-global trust-anchor state.
//!
//! ## The crash this module fixes
//!
//! `android_integration.rs` (the app's own Tauri/MainActivity bootstrap,
//! in the `src-tauri` crate) and [`super::bridge`] (the
//! `Java_com_plugin_rrcloud_RrcloudBridge_*` JNI entry points, called by a
//! `WorkManager` worker) both call
//! `ndk_context::initialize_android_context`. This app installs no
//! `android:process` override on any manifest `<service>` entry (checked:
//! neither `tauri-plugin-rrcloud`'s own manifest nor the generated
//! `gen/android/app/.../AndroidManifest.xml` sets one), so
//! `SyncCycleWorker`/`DcimScanWorker` always run in the *same* process as
//! the app, not a separate one — the old doc comment on `bridge.rs`'s
//! local `Once` claimed otherwise; that premise was wrong, and is the
//! root cause this module fixes.
//!
//! `ndk_context::initialize_android_context` panics if the process-global
//! it sets has already been set (its own internal `Once::call_once`
//! body). Each call site previously guarded itself with its OWN local
//! `static ...: std::sync::Once` — `android_integration.rs` had one,
//! `bridge.rs` had another — which only stops THAT call site from calling
//! twice; neither knows the other exists. Whichever call site runs first
//! in a given process wins silently, and the next caller from the *other*
//! site still panics on the already-initialized global, taking the whole
//! process down with it (no process isolation to contain the panic to a
//! worker thread). Confirmed on-device: `DcimScanWorker` won the race in
//! the background, `SyncCycleWorker` lost it seconds later on
//! `androidx.work-2` and crashed the app with `SIGABRT`.
//!
//! The fix is one process-wide `Once` **per global**, shared by every
//! call site via the `pub fn`s below — not two (or more) independent
//! ones. Both `android_integration.rs` and `bridge.rs` route through
//! these now; neither touches `ndk_context::initialize_android_context`
//! or `rustls_platform_verifier::android::init_with_env` directly, and
//! neither declares its own `Once` for either global any more.
//!
//! ## Is a stored vm/context pointer pair from either call site as good as the other's?
//!
//! Yes, for this app's lifecycle. Whichever caller's `(vm_ptr,
//! context_ptr)` pair wins the race is the one
//! `ndk_context::android_context()` serves back to every later reader in
//! this process, from either call site. There is exactly one `JavaVM` per
//! process, so the VM half is identical no matter who wins. The context
//! half is an `ApplicationContext` from both sides in this app:
//! `SyncCycleWorker`/`DcimScanWorker` are hitting this through their own
//! `applicationContext` (a `Worker`'s `applicationContext` is always the
//! process's `Application` object, never an `Activity`), and
//! `android_integration.rs`'s call site is handed the `Context` behind
//! Tauri's webview setup, which is likewise the application's context,
//! not a bare `Activity` context — `ndk_context` only needs a `Context`
//! good enough for JNI `FindClass`/resource-lookup purposes, which an
//! `ApplicationContext` satisfies for the lifetime of the process either
//! way.
//!
//! ## Does `rustls_platform_verifier::android::init_with_env` need this?
//!
//! Checked by reading its source (`rustls-platform-verifier` 0.7.1,
//! `src/android.rs`): it stores its global state in a
//! `once_cell::sync::OnceCell` and initializes via
//! `OnceCell::get_or_try_init`, so a second call in the same process is
//! already a documented no-op — it does not re-run the init closure, and
//! it does not error or panic. It was never the crash's cause and does
//! not strictly need a shared guard for correctness. It gets one anyway,
//! for one reason only: symmetry. With `ndk_context` fixed to route both
//! call sites through one shared `Once`, leaving `rustls_platform_verifier`
//! on two separate local `Once`s would be the same divergent-duplicate
//! shape that caused the actual bug, just on a global that happens to
//! tolerate it — not a pattern worth leaving behind.
//!
//! ## Testability
//!
//! The guard mechanic itself ([`run_exactly_once`]) is plain
//! `std::sync::Once` with no `target_os = "android"` gate, specifically so
//! the property this module exists to guarantee — concurrent callers
//! racing on one *shared* `Once` run the wrapped init exactly once, and
//! every caller (winner and loser alike) returns normally, with no panic
//! escaping to a loser — is provable with `cargo test` on the host. See
//! the tests below, including a regression pinning the actual bug shape
//! (two independent `Once`s both firing).

use std::sync::Once;

/// Runs `init` exactly once across any number of callers racing on the
/// same `once`, no matter which caller gets there first. This is the
/// entire fix, made host-testable: every production call site below
/// shares exactly one `Once` per global, instead of each declaring its
/// own.
///
/// Callers that lose the race block inside [`Once::call_once`] until the
/// winner's `init` returns, then return normally themselves — `init`
/// never runs twice for a given `once`, and a loser never observes a
/// panic from the winner's closure (std poisons the `Once` for *future*
/// calls if `init` panics, which is not a path either production closure
/// below takes after this fix).
pub fn run_exactly_once(once: &Once, init: impl FnOnce()) {
    once.call_once(init);
}

/// The single process-wide guard for `ndk_context::initialize_android_context`,
/// shared by every call site in this process (`android_integration.rs`'s
/// app bootstrap and [`super::bridge`]'s JNI entry points).
#[cfg(target_os = "android")]
static NDK_CONTEXT_INIT: Once = Once::new();

/// The single process-wide guard for
/// `rustls_platform_verifier::android::init_with_env`, shared the same
/// way as [`NDK_CONTEXT_INIT`] (see the module doc on why this one did
/// not strictly need it).
#[cfg(target_os = "android")]
static RUSTLS_PLATFORM_VERIFIER_INIT: Once = Once::new();

/// Initializes `ndk_context` for this process exactly once, no matter
/// which call site gets here first. `vm_ptr`/`context_ptr` are a
/// `JavaVM`/`Context` pointer pair, obtained exactly as every call site
/// already computes them: `JNIEnv::get_java_vm().get_java_vm_pointer()`
/// and `JObject::as_raw()`, both cast to `*mut c_void`.
#[cfg(target_os = "android")]
pub fn ensure_ndk_context_initialized(
    vm_ptr: *mut std::ffi::c_void,
    context_ptr: *mut std::ffi::c_void,
) {
    run_exactly_once(&NDK_CONTEXT_INIT, || {
        // SAFETY: `vm_ptr`/`context_ptr` come from a live `JNIEnv`/`JObject`
        // pair in the caller's current JNI call, and this closure runs at
        // most once per process (guarded by `NDK_CONTEXT_INIT`), which is
        // exactly `ndk_context::initialize_android_context`'s own
        // documented safety/panic contract.
        unsafe {
            ndk_context::initialize_android_context(vm_ptr, context_ptr);
        }
        eprintln!("rrcloud: ndk_context initialized for this process (shared guard).");
    });
}

/// Initializes `rustls_platform_verifier` for this process exactly once,
/// no matter which call site gets here first. `raw_env`/`raw_context` are
/// the `jni` 0.22 (`jni22`) raw JNI handles, obtained exactly as every
/// call site already computes them: `JNIEnv::get_raw()` and
/// `JObject::as_raw()`.
#[cfg(target_os = "android")]
pub fn ensure_rustls_platform_verifier_initialized(
    raw_env: *mut jni22::sys::JNIEnv,
    raw_context: jni22::sys::jobject,
) {
    run_exactly_once(&RUSTLS_PLATFORM_VERIFIER_INIT, || {
        let mut env_unowned = unsafe { jni22::EnvUnowned::from_raw(raw_env) };
        match env_unowned
            .with_env(|env22| {
                // SAFETY: `raw_context` is a valid local/global JNI
                // reference for the lifetime of this call, handed to us
                // by the caller's own live JNI call.
                let verifier_context =
                    unsafe { jni22::objects::JObject::from_raw(env22, raw_context) };
                rustls_platform_verifier::android::init_with_env(env22, verifier_context)
            })
            .into_outcome()
        {
            jni22::Outcome::Ok(()) => {
                eprintln!(
                    "rrcloud: rustls_platform_verifier initialized for this process (shared guard)."
                );
            }
            jni22::Outcome::Err(e) => {
                eprintln!("rrcloud: rustls_platform_verifier init failed: {e}");
            }
            jni22::Outcome::Panic(_) => {
                eprintln!("rrcloud: rustls_platform_verifier init panicked");
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread;

    /// Pins the property the double-init crash violated: any number of
    /// callers racing on ONE SHARED `Once` (not two-or-more independent
    /// ones, as `android_integration.rs` and the old `bridge.rs` each had
    /// before this fix) run the wrapped init exactly once between them,
    /// and EVERY caller — whichever wins the race and whichever loses it
    /// — returns successfully. No panic from a non-idempotent init (stood
    /// in for here by a counting closure; the real `ndk_context` call is
    /// android-only, and panics on a true double-init, which is exactly
    /// the bug) ever reaches a loser.
    #[test]
    fn concurrent_callers_run_shared_init_exactly_once() {
        let once = Arc::new(Once::new());
        let calls = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..8 {
            let once = Arc::clone(&once);
            let calls = Arc::clone(&calls);
            handles.push(thread::spawn(move || {
                run_exactly_once(&once, || {
                    calls.fetch_add(1, Ordering::SeqCst);
                });
            }));
        }

        for h in handles {
            // Every caller -- winner or loser -- must return normally: a
            // loser observing the winner's init panic would make this
            // `join()` return `Err`, which is exactly what the shared
            // guard must never let happen.
            h.join().expect("no caller should panic, winner or loser");
        }

        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "the wrapped init must run exactly once no matter how many callers race on the shared Once"
        );
    }

    /// Regression for the ACTUAL bug shape: two INDEPENDENT `Once`s (what
    /// `android_integration.rs` and `bridge.rs` each declared locally
    /// before this fix) guarding the SAME non-idempotent resource do not
    /// know about each other, so both fire — which is exactly what made
    /// real-device `ndk_context::initialize_android_context` panic on its
    /// second caller. This test does not call real `ndk_context` (host
    /// has none); it proves the structural property with a closure that
    /// panics on a second call, the same shape
    /// `ndk_context::initialize_android_context` has. If a future change
    /// reintroduces a second independent `Once` for a global this module
    /// guards, this test documents exactly why that is the bug this
    /// module exists to prevent.
    #[test]
    fn two_independent_onces_both_fire_unlike_one_shared_once() {
        let once_a = Once::new();
        let once_b = Once::new();
        let calls = AtomicUsize::new(0);

        let panics_on_second_call = || {
            let n = calls.fetch_add(1, Ordering::SeqCst);
            assert_eq!(n, 0, "simulated non-idempotent init called more than once");
        };

        // Site A (stands in for `android_integration.rs`) initializes
        // first and succeeds.
        run_exactly_once(&once_a, panics_on_second_call);

        // Site B (stands in for the old `bridge.rs`) has its OWN Once and
        // has no idea site A already ran -- this is the bug: it still
        // calls the non-idempotent init a second time, which panics.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_exactly_once(&once_b, panics_on_second_call);
        }));

        assert!(
            result.is_err(),
            "two independent Onces guarding a shared non-idempotent resource must \
             both fire -- demonstrating why `android_integration.rs` and the old \
             `bridge.rs` each having their OWN local Once was the actual bug; the \
             fix is the single shared Once exercised by the test above"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}
