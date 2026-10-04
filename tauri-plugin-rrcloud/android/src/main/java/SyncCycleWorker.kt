package com.plugin.rrcloud

import android.content.Context
import android.util.Log
import androidx.work.ListenableWorker
import androidx.work.Worker
import androidx.work.WorkerParameters

/**
 * The §5.1 point 3 background-maintenance worker: one bounded
 * [RrcloudBridge.runSyncCycle] call per run, budgeted under WorkManager's
 * ~10-minute expedited-job ceiling. Scheduled both periodically (~1h,
 * [RrcloudWorkScheduler.ensurePeriodicWorkScheduled]) and as an expedited
 * one-shot on app-background when the queue is non-empty
 * ([RrcloudWorkScheduler.enqueueExpeditedSyncCycle], §3.3 exit flush).
 *
 * Deliberately a plain [Worker], not a `CoroutineWorker`: [RrcloudBridge.
 * runSyncCycle] is itself a blocking JNI call that only returns once the
 * whole bounded cycle (or the budget) has run its course, so there is no
 * suspension point worth modeling as a coroutine here — `Worker.doWork()`
 * already runs on WorkManager's own background executor.
 *
 * No `setForeground()` — background maintenance mode never requests
 * foreground (§5.1 point 3's explicit "these run **without**
 * `setForeground`").
 */
class SyncCycleWorker(context: Context, params: WorkerParameters) : Worker(context, params) {
    override fun doWork(): ListenableWorker.Result {
        val code = RrcloudBridge.runSyncCycle(applicationContext, BUDGET_MS)
        if (code != RrcloudBridge.RESULT_SUCCESS) {
            Log.w(TAG, "runSyncCycle returned code $code")
        }

        // §5.4 piggyback: check the standing-dirty duration on every run
        // (cheap — one redb read) rather than running a third periodic
        // worker just for this.
        DirtyWatch.checkAndMaybeNotify(applicationContext)

        return RrcloudBridge.toWorkResult(code)
    }

    private companion object {
        const val TAG = "RrcloudSyncWorker"

        /** Comfortably under WorkManager's ~10-minute expedited-job
         * ceiling (ARCHITECTURE.md §5.1), leaving headroom for process
         * startup/JNI init overhead before the budget clock in Rust even
         * starts. */
        const val BUDGET_MS = 9L * 60L * 1000L
    }
}
