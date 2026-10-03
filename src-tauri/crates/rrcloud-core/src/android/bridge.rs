//! The JNI bridge proper (ARCHITECTURE.md §5.1): `#[no_mangle] extern
//! "system"` entry points `SyncCycleWorker.doWork()`, the user-initiated
//! foreground-sync service, and `DcimScanWorker` call from Kotlin. Compiled
//! **only** under `target_os = "android"` — it needs a live JVM to mean
//! anything, and the `jni` crate is a `target.'cfg(target_os =
//! "android")'` dependency of this crate (see `Cargo.toml`) for exactly
//! that reason, so `cargo test` on the host never touches this module.
//!
//! The Kotlin side of this contract is `RrcloudBridge.kt`
//! (`tauri-plugin-rrcloud/android/src/main/java/com/plugin/rrcloud/`):
//! that file's `external fun runSyncCycle(...)` must keep this function's
//! exact JNI name (`Java_<package_with_underscores>_RrcloudBridge_
//! runSyncCycle`, package `com.plugin.rrcloud`) and parameter order — the
//! JNI name mangling has no compiler to catch a drift between the two
//! sides, only a runtime `UnsatisfiedLinkError`.
//!
//! All of the actual decision-making this function performs — the
//! `budget_ms` deadline, the `jint` it returns — is delegated to
//! [`super::bounded_cycle`] / [`super::result_code`], which are
//! host-testable on their own; this function's body is pure glue (JNI
//! unwrapping, `ndk_context`/`rustls_platform_verifier` init, opening redb
//! + the Keystore-backed `CredentialStore` via a JNI callback, running the
//! bounded cycle) that cannot be exercised without a JVM and is therefore
//! proven correct by the Android build gate + (eventually) on-device
//! verification, not `cargo test`.

#[cfg(target_os = "android")]
mod imp {
    use jni::objects::{JClass, JObject};
    use jni::sys::{jint, jlong};
    use jni::JNIEnv;

    /// Runs one bounded sync cycle (ARCHITECTURE.md §5.1).
    ///
    /// Java/Kotlin signature this must match exactly:
    /// `external fun runSyncCycle(context: Context, budgetMs: Long): Int`
    /// in the `RrcloudBridge` Kotlin object, package `com.plugin.rrcloud`.
    ///
    /// Parameters:
    /// - `ctx`: the Android `Context` (`ApplicationContext` from the
    ///   caller — `SyncCycleWorker`/`DcimScanWorker`'s `applicationContext`,
    ///   or the foreground service's own context), used to initialize
    ///   `ndk_context` and to reach the Keystore `CredentialStore` via a
    ///   JNI callback.
    /// - `budget_ms`: milliseconds remaining in the caller's execution
    ///   window (the WorkManager ~10-minute expedited budget minus
    ///   already-spent time, or the FGS path's own pacing) — becomes a
    ///   [`super::super::bounded_cycle::CycleBudget`].
    ///
    /// Returns a [`BridgeResult::to_code`] value; see
    /// `super::result_code`'s module table for the complete mapping the
    /// Kotlin caller must apply to the return value.
    #[unsafe(no_mangle)]
    pub extern "system" fn Java_com_plugin_rrcloud_RrcloudBridge_runSyncCycle<'local>(
        mut env: JNIEnv<'local>,
        _class: JClass<'local>,
        ctx: JObject<'local>,
        budget_ms: jlong,
    ) -> jint {
        let _ = (&mut env, &ctx, budget_ms);
        // P5 green:
        //   1. ndk_context::initialize_android_context from `ctx` (Once-
        //      guarded; the worker process needs its own init even when
        //      `android_integration::initialize_android` already ran in
        //      this process, since WorkManager's default executor runs in
        //      a *separate* process — see that function's doc for why a
        //      second, worker-local Once is required rather than reusing
        //      the app's).
        //   2. rustls_platform_verifier::android::init_with_env (same
        //      idempotency note).
        //   3. Open redb under the device's state dir + the Keystore
        //      CredentialStore (a JNI callback into Kotlin) — back off
        //      with BridgeResult::RetryLockHeld on redb's file-lock error
        //      AND on the in-process static Mutex (same-process Worker
        //      configuration) being held.
        //   4. Build a bounded_cycle::CycleBudget from `budget_ms`, run
        //      sync::manager one-cycle-equivalent logic checking the
        //      budget between work units (never mid-item), via the
        //      existing CancelFlag machinery.
        //   5. Map the outcome through BridgeResult::to_code.
        todo!("P5 green: see the numbered steps above; final step is BridgeResult::to_code")
    }
}

#[cfg(target_os = "android")]
pub use imp::*;
