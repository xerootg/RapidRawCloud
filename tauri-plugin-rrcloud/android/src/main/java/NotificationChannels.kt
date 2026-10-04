package com.plugin.rrcloud

import android.app.NotificationChannel
import android.app.NotificationManager
import android.content.Context
import android.os.Build

/**
 * The two notification channels this plugin owns (ARCHITECTURE.md §5.3:
 * "a notification channel, runtime-requested on Android 13+"):
 *
 * - [SYNC_PROGRESS]: the §5.1 point 2 foreground-sync service's ongoing
 *   progress notification. `LOW` importance — informational, no sound —
 *   since it exists only to satisfy the `FOREGROUND_SERVICE_DATA_SYNC`
 *   requirement, not to alert the user.
 * - [SYNC_ALERTS]: the §5.4 "N edits not backed up" notification raised
 *   after 24h of standing dirty state. `DEFAULT` importance — this one is
 *   worth the user's attention.
 */
object RrcloudNotificationChannels {
    const val SYNC_PROGRESS = "rrcloud_sync_progress"
    const val SYNC_ALERTS = "rrcloud_sync_alerts"

    fun ensureCreated(context: Context) {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.O) {
            return
        }
        val manager = context.getSystemService(Context.NOTIFICATION_SERVICE) as? NotificationManager
            ?: return

        manager.createNotificationChannel(
            NotificationChannel(
                SYNC_PROGRESS,
                "Cloud sync progress",
                NotificationManager.IMPORTANCE_LOW
            ).apply {
                description = "Shows while a cloud sync is actively uploading or downloading."
            }
        )
        manager.createNotificationChannel(
            NotificationChannel(
                SYNC_ALERTS,
                "Cloud sync alerts",
                NotificationManager.IMPORTANCE_DEFAULT
            ).apply {
                description = "Alerts when edits have gone unbacked-up for an extended period."
            }
        )
    }
}
