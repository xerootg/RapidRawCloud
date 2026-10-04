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

    /**
     * Maps a [runSyncCycle]/etc result code to the `androidx.work.
     * ListenableWorker.Result` a `Worker.doWork()` must return, per the
     * table above. Centralized here so `SyncCycleWorker`/`DcimScanWorker`
     * do not each hand-duplicate the switch.
     */
    fun toWorkResult(code: Int): androidx.work.ListenableWorker.Result = when (code) {
        RESULT_SUCCESS -> androidx.work.ListenableWorker.Result.success()
        RESULT_RETRY_TRANSIENT, RESULT_RETRY_LOCK_HELD -> androidx.work.ListenableWorker.Result.retry()
        else -> androidx.work.ListenableWorker.Result.failure()
    }

    // ---- §5.2 DCIM batch dedupe / import recording -------------------------

    /**
     * Batch dedupe decision over one scan pass's candidates (§5.2).
     * `candidatesJson` is `[{"path":str,"size":u64,"mtimeUnix":i64}, ...]`;
     * the result is `[{"path":str,"decision":"skip"|"rehash"|"new",
     * "contentId":str?}, ...]` in the same order, or `null` on failure (the
     * caller should simply retry this pass next time — see the Rust doc).
     */
    external fun dcimScanDecisions(context: Context, candidatesJson: String): String?

    /**
     * Records one freshly-streamed-and-renamed DCIM import (§5.2). Returns
     * a [RESULT_SUCCESS]-family code, same table as [runSyncCycle].
     */
    external fun dcimRecordImport(
        context: Context,
        sourcePath: String,
        sourceSize: Long,
        sourceMtimeUnix: Long,
        finalPath: String
    ): Int

    /**
     * The §5.4 "N edits not backed up" count — items dirty-and-not-yet-
     * uploaded. `-1` on failure (state db unavailable): callers should skip
     * the check for this run rather than treat it as zero.
     */
    external fun dirtyUnbackedCount(context: Context): Int

    // ---- Keystore `CredentialStore` callbacks (called BY the native side) --
    // `@JvmStatic` so `JNIEnv::call_static_method` can find them: these are
    // the Kotlin-to-nowhere-else half of the contract, invoked FROM
    // `rrcloud_core::android::bridge` via `GetStaticMethodID` — a plain
    // (non-`external`) Kotlin function, the mirror image of `runSyncCycle`
    // above.

    @JvmStatic
    fun loadCredentialsJson(context: Context): String? = AndroidCredentialStore.load(context)

    @JvmStatic
    fun storeCredentialsJson(context: Context, json: String): Boolean =
        AndroidCredentialStore.store(context, json)

    @JvmStatic
    fun clearCredentials(context: Context): Boolean = AndroidCredentialStore.clear(context)

    @JvmStatic
    fun loadSyncSettingsJson(context: Context): String? =
        RrcloudSyncSettingsReader.readSyncSettingsJson(context)

    /** Called by `sync::hooks::sync_exit_flush` on Android (§3.3 exit flush). */
    @JvmStatic
    fun enqueueExpeditedSync(context: Context) {
        RrcloudWorkScheduler.enqueueExpeditedSyncCycle(context)
    }
}
