package com.plugin.rrcloud

import android.content.Context
import androidx.work.BackoffPolicy
import androidx.work.Constraints
import androidx.work.ExistingPeriodicWorkPolicy
import androidx.work.ExistingWorkPolicy
import androidx.work.NetworkType
import androidx.work.OneTimeWorkRequestBuilder
import androidx.work.PeriodicWorkRequestBuilder
import androidx.work.WorkManager
import androidx.work.WorkRequest
import java.util.concurrent.TimeUnit

/**
 * ARCHITECTURE.md §5.1 point 3 / §5.2: schedules the two periodic workers
 * (`SyncCycleWorker` ~1h, `DcimScanWorker` ~30min) with constraints mapped
 * 1:1 from `settings.json`'s `sync` block, and the expedited one-shot
 * `SyncCycleWorker` run enqueued on app-background when the queue is
 * non-empty (§3.3 exit flush) or on a live DCIM change (the
 * `ContentObserver` debounce).
 */
object RrcloudWorkScheduler {
    const val PERIODIC_SYNC_WORK_NAME = "rrcloud_periodic_sync"
    const val PERIODIC_DCIM_SCAN_WORK_NAME = "rrcloud_periodic_dcim_scan"
    const val EXPEDITED_SYNC_WORK_NAME = "rrcloud_expedited_sync"
    const val ONE_SHOT_DCIM_SCAN_WORK_NAME = "rrcloud_dcim_scan_now"

    private fun syncConstraints(context: Context): Constraints {
        val settings = RrcloudSyncSettingsReader.readWorkConstraintSettings(context)
        val builder = Constraints.Builder()
            .setRequiredNetworkType(if (settings.requiresUnmetered) NetworkType.UNMETERED else NetworkType.CONNECTED)
        if (settings.requiresCharging) {
            builder.setRequiresCharging(true)
        }
        return builder.build()
    }

    /** Schedules both periodic workers if sync/DCIM-watch is enabled; a
     * no-op `ExistingPeriodicWorkPolicy.KEEP` enqueue otherwise is harmless
     * (the worker itself reads settings fresh on every run and no-ops when
     * unconfigured), so this can be called unconditionally from plugin
     * `load()` without re-checking settings on every app start. */
    fun ensurePeriodicWorkScheduled(context: Context) {
        val workManager = WorkManager.getInstance(context)
        val constraints = syncConstraints(context)

        val syncRequest = PeriodicWorkRequestBuilder<SyncCycleWorker>(1, TimeUnit.HOURS)
            .setConstraints(constraints)
            .setBackoffCriteria(BackoffPolicy.EXPONENTIAL, WorkRequest.MIN_BACKOFF_MILLIS, TimeUnit.MILLISECONDS)
            .build()
        workManager.enqueueUniquePeriodicWork(
            PERIODIC_SYNC_WORK_NAME,
            ExistingPeriodicWorkPolicy.UPDATE,
            syncRequest
        )

        val dcimRequest = PeriodicWorkRequestBuilder<DcimScanWorker>(30, TimeUnit.MINUTES)
            .setConstraints(Constraints.Builder().setRequiredNetworkType(NetworkType.NOT_REQUIRED).build())
            .build()
        workManager.enqueueUniquePeriodicWork(
            PERIODIC_DCIM_SCAN_WORK_NAME,
            ExistingPeriodicWorkPolicy.UPDATE,
            dcimRequest
        )
    }

    /** §3.3 exit flush / expedited drain: a `KEEP`-policy unique expedited
     * one-shot, so a flurry of near-simultaneous triggers (e.g. the app
     * backgrounding right as a DCIM import lands) coalesces into one run
     * rather than queueing several. */
    fun enqueueExpeditedSyncCycle(context: Context) {
        val request = OneTimeWorkRequestBuilder<SyncCycleWorker>()
            .setExpedited(androidx.work.OutOfQuotaPolicy.RUN_AS_NON_EXPEDITED_WORK_REQUEST)
            .setConstraints(syncConstraints(context))
            .build()
        WorkManager.getInstance(context).enqueueUniqueWork(
            EXPEDITED_SYNC_WORK_NAME,
            ExistingWorkPolicy.KEEP,
            request
        )
    }

    /** The `ContentObserver` debounce trigger (§5.2 live path): a one-shot
     * `DcimScanWorker` run, coalesced the same way as the expedited sync
     * above. */
    fun enqueueDcimScanNow(context: Context) {
        val request = OneTimeWorkRequestBuilder<DcimScanWorker>().build()
        WorkManager.getInstance(context).enqueueUniqueWork(
            ONE_SHOT_DCIM_SCAN_WORK_NAME,
            ExistingWorkPolicy.KEEP,
            request
        )
    }
}
