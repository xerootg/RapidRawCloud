package com.plugin.rrcloud

import android.app.NotificationManager
import android.content.Context
import androidx.core.app.NotificationCompat
import java.util.concurrent.TimeUnit

/**
 * ARCHITECTURE.md §5.4: "a persistent 'N edits not backed up' notification
 * after 24h of standing dirty state" — the uninstall/process-death
 * mitigation's visibility half (the durability half is redb itself; see
 * the architecture section's doc). Piggybacks on [SyncCycleWorker]'s own
 * run rather than a third periodic worker.
 *
 * Bookkeeping is a single `SharedPreferences` timestamp: the wall-clock
 * instant [RrcloudBridge.dirtyUnbackedCount] was first observed non-zero.
 * The count dropping back to zero (a cycle caught up) clears it, so a
 * transient dirty blip (normal editing) never accumulates toward the 24h
 * threshold across unrelated sessions.
 */
object DirtyWatch {
    private const val PREFS_FILE = "rrcloud_dirty_watch"
    private const val KEY_DIRTY_SINCE_MS = "dirty_since_ms"
    private const val NOTIFICATION_ID = 42
    private val THRESHOLD_MS = TimeUnit.HOURS.toMillis(24)

    fun checkAndMaybeNotify(context: Context) {
        val count = RrcloudBridge.dirtyUnbackedCount(context)
        val prefs = context.getSharedPreferences(PREFS_FILE, Context.MODE_PRIVATE)

        if (count < 0) {
            // Unknown (-1: state db unavailable, e.g. CYCLE_LOCK contention
            // from a concurrent cycle, or sync simply not configured yet —
            // RrcloudBridge.dirtyUnbackedCount's own doc says to treat this
            // as "skip this check", NOT as "nothing is dirty". Regression
            // (P5 review round 0): a transient failure must never reset the
            // standing-dirty timer or dismiss an already-shown warning —
            // §5.4 requires the uninstall data-loss window stay "visible,
            // not silent", and lumping -1 in with a genuine 0 would let a
            // flaky read silently restart the 24h clock.
            return
        }

        if (count == 0) {
            // Genuinely nothing dirty: clear any standing timer and dismiss
            // a stale notification.
            prefs.edit().remove(KEY_DIRTY_SINCE_MS).apply()
            dismiss(context)
            return
        }

        val now = System.currentTimeMillis()
        val since = prefs.getLong(KEY_DIRTY_SINCE_MS, 0L)
        val dirtySince = if (since == 0L) {
            prefs.edit().putLong(KEY_DIRTY_SINCE_MS, now).apply()
            now
        } else {
            since
        }

        if (now - dirtySince >= THRESHOLD_MS) {
            notify(context, count)
        }
    }

    private fun notify(context: Context, count: Int) {
        val manager = context.getSystemService(Context.NOTIFICATION_SERVICE) as? NotificationManager
            ?: return
        val notification = NotificationCompat.Builder(context, RrcloudNotificationChannels.SYNC_ALERTS)
            .setSmallIcon(android.R.drawable.stat_sys_warning)
            .setContentTitle("$count edits not backed up")
            .setContentText("These changes have not reached the cloud in over 24 hours.")
            .setOngoing(false)
            .setAutoCancel(true)
            .build()
        manager.notify(NOTIFICATION_ID, notification)
    }

    private fun dismiss(context: Context) {
        val manager = context.getSystemService(Context.NOTIFICATION_SERVICE) as? NotificationManager
            ?: return
        manager.cancel(NOTIFICATION_ID)
    }
}
