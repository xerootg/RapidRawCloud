package com.plugin.rrcloud

import android.app.Service
import android.content.Intent
import android.content.pm.ServiceInfo
import android.os.Build
import android.os.IBinder
import android.util.Log
import androidx.core.app.NotificationCompat
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread

/**
 * ARCHITECTURE.md §5.1 point 2: the user-initiated long operation. Started
 * only while the app is foreground (`RrcloudPlugin.startForegroundSync`,
 * which is itself only reachable via a Tauri command the webview issues —
 * the webview cannot be running from the background), so
 * `startForeground()` here is always legal.
 *
 * Runs bounded cycles back-to-back on a dedicated thread (never the main
 * thread — [RrcloudBridge.runSyncCycle] is a blocking JNI call) until
 * either:
 * - a cycle finishes "fast" (under [QUIESCENCE_THRESHOLD_MS]), taken as a
 *   signal the engine reached quiescence with nothing left to move — a
 *   bounded cycle call has no separate "more work remains" signal (see the
 *   Rust `result_code` module doc's code-0 table entry, which already
 *   conflates "reached quiescence" and "ran out of work" into one
 *   `Success`), so timing is the pragmatic way to tell the two apart here;
 * - the Android 15 6h/24h `dataSync` cap is approached
 *   ([SESSION_CAP_MS], kept comfortably under 6h so the service always
 *   stops itself cleanly rather than racing the OS's own enforcement); or
 * - a non-retryable failure code comes back.
 *
 * Degrades gracefully at the cap exactly as §5.1 describes: stops cleanly,
 * final notification invites the user to continue (tapping it re-opens the
 * app, from which "Sync now" can restart this service).
 */
class SyncForegroundService : Service() {
    @Volatile
    private var running = false

    override fun onBind(intent: Intent?): IBinder? = null

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        if (running) {
            return START_NOT_STICKY
        }
        running = true
        RrcloudNotificationChannels.ensureCreated(applicationContext)
        startForegroundWithNotification(progressText(0))

        thread(name = "rrcloud-fgs-sync") {
            runLoop()
            running = false
            stopForeground(STOP_FOREGROUND_REMOVE)
            stopSelf(startId)
        }

        return START_NOT_STICKY
    }

    private fun runLoop() {
        val deadline = System.currentTimeMillis() + SESSION_CAP_MS
        var cyclesRun = 0
        while (System.currentTimeMillis() < deadline) {
            val remaining = deadline - System.currentTimeMillis()
            val budget = minOf(CYCLE_BUDGET_MS, remaining)
            if (budget <= 0) break

            val started = System.currentTimeMillis()
            val code = RrcloudBridge.runSyncCycle(applicationContext, budget)
            val elapsed = System.currentTimeMillis() - started
            cyclesRun += 1

            when (code) {
                RrcloudBridge.RESULT_SUCCESS -> {
                    updateNotification(progressText(cyclesRun))
                    if (elapsed < QUIESCENCE_THRESHOLD_MS) {
                        // Reached quiescence well inside its budget — caught up.
                        updateNotification("Cloud sync up to date")
                        return
                    }
                }
                RrcloudBridge.RESULT_RETRY_TRANSIENT, RrcloudBridge.RESULT_RETRY_LOCK_HELD -> {
                    // Transient — brief pause, then keep trying within the
                    // session cap.
                    Thread.sleep(RETRY_BACKOFF_MS)
                }
                else -> {
                    Log.w(TAG, "foreground sync stopped: code $code")
                    updateNotification("Cloud sync could not continue")
                    return
                }
            }
        }
        updateNotification("Cloud sync paused — tap to continue")
    }

    private fun progressText(cyclesRun: Int): String =
        if (cyclesRun == 0) "Starting cloud sync…" else "Syncing… ($cyclesRun cycles so far)"

    private fun startForegroundWithNotification(text: String) {
        val notification = buildNotification(text)
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
            startForeground(NOTIFICATION_ID, notification, ServiceInfo.FOREGROUND_SERVICE_TYPE_DATA_SYNC)
        } else {
            startForeground(NOTIFICATION_ID, notification)
        }
    }

    private fun updateNotification(text: String) {
        val manager = getSystemService(NOTIFICATION_SERVICE) as? android.app.NotificationManager ?: return
        manager.notify(NOTIFICATION_ID, buildNotification(text))
    }

    private fun buildNotification(text: String): android.app.Notification =
        NotificationCompat.Builder(applicationContext, RrcloudNotificationChannels.SYNC_PROGRESS)
            .setSmallIcon(android.R.drawable.stat_notify_sync)
            .setContentTitle("Cloud sync")
            .setContentText(text)
            .setOngoing(running)
            .setPriority(NotificationCompat.PRIORITY_LOW)
            .build()

    companion object {
        private const val TAG = "RrcloudFgsSync"
        private const val NOTIFICATION_ID = 7

        /** Per-cycle budget — generous since the FGS path has no
         * WorkManager execution-window ceiling, only the 6h session cap
         * below. */
        private val CYCLE_BUDGET_MS = TimeUnit.MINUTES.toMillis(5)

        /** Kept comfortably under the Android 15 6h/24h `dataSync` cap
         * (ARCHITECTURE.md §5.1) so this service always stops itself first. */
        private val SESSION_CAP_MS = TimeUnit.HOURS.toMillis(5) + TimeUnit.MINUTES.toMillis(30)

        private const val QUIESCENCE_THRESHOLD_MS = 1_500L
        private const val RETRY_BACKOFF_MS = 5_000L
    }
}
