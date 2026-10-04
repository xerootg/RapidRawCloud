package com.plugin.rrcloud

import android.Manifest
import android.app.Activity
import android.content.Intent
import android.content.pm.PackageManager
import android.os.Build
import android.provider.MediaStore
import android.webkit.WebView
import androidx.core.app.ActivityCompat
import androidx.core.content.ContextCompat
import app.tauri.annotation.Command
import app.tauri.annotation.TauriPlugin
import app.tauri.plugin.Invoke
import app.tauri.plugin.JSObject
import app.tauri.plugin.Plugin

/**
 * The Tauri-facing half of the P5 Android platform work
 * (ARCHITECTURE.md §5).
 *
 * [load] runs once, while the process (and this plugin) is alive, and
 * wires up everything that is not itself a JS-facing command:
 * notification channels, the two periodic WorkManager jobs, the §5.2 live
 * `ContentObserver` DCIM watch, and the Android 13+ runtime
 * `POST_NOTIFICATIONS` prompt.
 *
 * [startForegroundSync]/[checkPartialMediaAccess] back the
 * `sync_start_foreground_sync`/`sync_dcim_access_status` Tauri commands
 * (`tauri-plugin-rrcloud/src/commands.rs`).
 */
@TauriPlugin
class RrcloudPlugin(private val activity: Activity) : Plugin(activity) {
    private var dcimObserver: DcimContentObserver? = null

    override fun load(webView: WebView) {
        super.load(webView)

        RrcloudNotificationChannels.ensureCreated(activity.applicationContext)
        RrcloudWorkScheduler.ensurePeriodicWorkScheduled(activity.applicationContext)
        registerDcimObserver()
        requestNotificationPermissionIfNeeded()
    }

    private fun registerDcimObserver() {
        if (dcimObserver != null) {
            return
        }
        val observer = DcimContentObserver(activity.applicationContext)
        activity.applicationContext.contentResolver.registerContentObserver(
            MediaStore.Images.Media.EXTERNAL_CONTENT_URI,
            true,
            observer
        )
        dcimObserver = observer
    }

    private fun requestNotificationPermissionIfNeeded() {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.TIRAMISU) {
            return
        }
        if (ContextCompat.checkSelfPermission(activity, Manifest.permission.POST_NOTIFICATIONS)
            != PackageManager.PERMISSION_GRANTED
        ) {
            ActivityCompat.requestPermissions(
                activity,
                arrayOf(Manifest.permission.POST_NOTIFICATIONS),
                POST_NOTIFICATIONS_REQUEST_CODE
            )
        }
    }

    /** §5.1 point 2: starts [SyncForegroundService]. Only meaningful while
     * the app (and therefore this command's caller, the webview) is
     * foreground — see the service's own doc. */
    @Command
    fun startForegroundSync(invoke: Invoke) {
        // `ContextCompat.startForegroundService`, not the raw `Context`
        // method (API26+ only) — falls back to `startService` on API
        // 24-25, which `minSdk` (`tauri-plugin-rrcloud/android/build.
        // gradle.kts`) still supports.
        ContextCompat.startForegroundService(
            activity.applicationContext,
            Intent(activity.applicationContext, SyncForegroundService::class.java)
        )
        invoke.resolve()
    }

    /** §5.2 partial-photo-access (Android 14 `READ_MEDIA_VISUAL_USER_SELECTED`)
     * detection. */
    @Command
    fun checkPartialMediaAccess(invoke: Invoke) {
        val partial = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.UPSIDE_DOWN_CAKE) {
            val hasSelected = ContextCompat.checkSelfPermission(
                activity,
                Manifest.permission.READ_MEDIA_VISUAL_USER_SELECTED
            ) == PackageManager.PERMISSION_GRANTED
            val hasFull = ContextCompat.checkSelfPermission(
                activity,
                Manifest.permission.READ_MEDIA_IMAGES
            ) == PackageManager.PERMISSION_GRANTED
            hasSelected && !hasFull
        } else {
            false
        }
        val result = JSObject()
        result.put("partial", partial)
        invoke.resolve(result)
    }

    companion object {
        private const val POST_NOTIFICATIONS_REQUEST_CODE = 9001
    }
}
