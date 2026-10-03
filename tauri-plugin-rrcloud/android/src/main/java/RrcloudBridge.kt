package com.plugin.rrcloud

import android.content.Context

/**
 * The Kotlin side of the JNI bridge (ARCHITECTURE.md §5.1). Calls straight
 * into `rrcloud-core`'s native entry point — no Tauri/webview involved, so
 * this is callable from `SyncCycleWorker.doWork()`, `DcimScanWorker.doWork()`,
 * and the user-initiated foreground-sync service alike.
 *
 * The native symbol this binds to is
 * `Java_com_plugin_rrcloud_RrcloudBridge_runSyncCycle`
 * (`rrcloud_core::android::bridge`, `tauri-plugin-rrcloud`'s package
 * `com.plugin.rrcloud` — JNI name-mangles dots to underscores). Keep this
 * object's package, name, and this function's name/parameter order in sync
 * with that Rust function by hand: a JNI name mismatch is a runtime
 * `UnsatisfiedLinkError`, not a compile error on either side.
 *
 * The shared library is `librapidraw_lib.so` — the same cdylib the main
 * app already loads for its own JNI surface
 * (`android_integration.rs`), built once per ABI and already packaged by
 * the app's Gradle `rust` plugin; this object does not build or package
 * anything of its own.
 */
object RrcloudBridge {
    init {
        System.loadLibrary("rapidraw_lib")
    }

    /**
     * Runs one bounded sync cycle. `budgetMs` is the caller's remaining
     * execution-window time (WorkManager's ~10-minute expedited budget
     * minus time already spent, or the foreground-sync service's own
     * pacing); the native side checks it between discrete work units
     * (upload one item, apply one journal page — never mid-item).
     *
     * Returns one of the `RESULT_*` codes below. **Pinned mapping** (the
     * Rust-side source of truth is `rrcloud_core::android::result_code`;
     * this table must match it exactly):
     *
     * | code | meaning                                         | `doWork()` returns |
     * |------|--------------------------------------------------|---------------------|
     * | 0    | cycle reached quiescence (or ran out of work)     | `Result.success()`  |
     * | 1    | transient failure (network/backend) — retry       | `Result.retry()`    |
     * | 2    | redb/in-process lock held by a concurrent cycle   | `Result.retry()`    |
     * | 3    | not configured (no credentials on this device)    | `Result.failure()`  |
     * | 4    | non-retryable engine error                        | `Result.failure()`  |
     */
    external fun runSyncCycle(context: Context, budgetMs: Long): Int

    const val RESULT_SUCCESS = 0
    const val RESULT_RETRY_TRANSIENT = 1
    const val RESULT_RETRY_LOCK_HELD = 2
    const val RESULT_FAILURE_NOT_CONFIGURED = 3
    const val RESULT_FAILURE_PERMANENT = 4
}
