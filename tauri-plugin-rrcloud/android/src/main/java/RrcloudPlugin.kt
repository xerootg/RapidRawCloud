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
import app.tauri.PermissionState
import app.tauri.annotation.Command
import app.tauri.annotation.InvokeArg
import app.tauri.annotation.Permission
import app.tauri.annotation.PermissionCallback
import app.tauri.annotation.TauriPlugin
import app.tauri.plugin.Invoke
import app.tauri.plugin.JSObject
import app.tauri.plugin.Plugin

@InvokeArg
class DockConnectArgs {
    var address: String = ""
}

@InvokeArg
class DockRpcArgs {
    var method: String = "GET"
    var path: String = ""
    var bodyJson: String? = null
    var auth: String? = null
}

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
@TauriPlugin(
    permissions = [
        // Android 12+: scanning and connecting are runtime permissions of their own
        // (`neverForLocation` in the manifest keeps location out of it).
        Permission(strings = [Manifest.permission.BLUETOOTH_SCAN, Manifest.permission.BLUETOOTH_CONNECT], alias = "bluetooth"),
        // Android 6–11: a BLE scan needs fine location.
        Permission(strings = [Manifest.permission.ACCESS_FINE_LOCATION], alias = "bluetoothLegacy"),
    ]
)
class RrcloudPlugin(private val activity: Activity) : Plugin(activity) {
    private var dcimObserver: DcimContentObserver? = null
    private val dock: DockBleClient by lazy {
        DockBleClient(activity.applicationContext) { event, data -> trigger(event, data) }
    }
    private val bleAlias: String
        get() = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S) "bluetooth" else "bluetoothLegacy"

    // ---- camera dock over Bluetooth LE (DockBleClient) --------------------------
    private fun withBle(invoke: Invoke, callback: String, body: () -> Unit) {
        if (getPermissionState(bleAlias) == PermissionState.GRANTED) body()
        else requestPermissionForAlias(bleAlias, invoke, callback)
    }

    private fun bleGranted(): Boolean = getPermissionState(bleAlias) == PermissionState.GRANTED

    @Command
    fun dockScanStart(invoke: Invoke) = withBle(invoke, "dockScanStartGranted") { doScanStart(invoke) }

    @PermissionCallback
    private fun dockScanStartGranted(invoke: Invoke) {
        if (bleGranted()) doScanStart(invoke) else invoke.reject("Bluetooth permission denied")
    }

    private fun doScanStart(invoke: Invoke) {
        val err = dock.startScan()
        if (err == null) invoke.resolve() else invoke.reject(err)
    }

    @Command
    fun dockScanStop(invoke: Invoke) {
        dock.stopScan()
        invoke.resolve()
    }

    @Command
    fun dockConnect(invoke: Invoke) = withBle(invoke, "dockConnectGranted") { doConnect(invoke) }

    @PermissionCallback
    private fun dockConnectGranted(invoke: Invoke) {
        if (bleGranted()) doConnect(invoke) else invoke.reject("Bluetooth permission denied")
    }

    private fun doConnect(invoke: Invoke) {
        val args = invoke.parseArgs(DockConnectArgs::class.java)
        dock.connect(args.address) { r ->
            r.onSuccess { info -> invoke.resolve(JSObject.fromJSONObject(info)) }
                .onFailure { e -> invoke.reject(e.message ?: "connection failed") }
        }
    }

    @Command
    fun dockDisconnect(invoke: Invoke) {
        dock.disconnect()
        invoke.resolve()
    }

    @Command
    fun dockRpc(invoke: Invoke) {
        val args = invoke.parseArgs(DockRpcArgs::class.java)
        dock.rpc(args.method, args.path, args.bodyJson, args.auth) { r ->
            r.onSuccess { (status, bodyJson) ->
                val o = JSObject()
                o.put("status", status)
                o.put("bodyJson", bodyJson)
                invoke.resolve(o)
            }.onFailure { e -> invoke.reject(e.message ?: "request failed") }
        }
    }

    override fun load(webView: WebView) {
        super.load(webView)

        RrcloudNotificationChannels.ensureCreated(activity.applicationContext)
        RrcloudWorkScheduler.ensurePeriodicWorkScheduled(activity.applicationContext)
        registerDcimObserver()
        requestNotificationPermissionIfNeeded()
    }

    /** §5.1 round-1 major fix: yield the app-process redb lock once the
     * app is no longer visible, so `SyncCycleWorker`/`DcimScanWorker`
     * (which may run in-process under WorkManager's default executor) can
     * actually acquire it instead of backing off for as long as this
     * process merely survives in the background. `onStop` (not `onPause`,
     * which also fires for transient interruptions like a permission
     * dialog or an incoming call) is the "no longer visible" signal — see
     * `Plugin.onStop`'s own doc. */
    override fun onStop() {
        super.onStop()
        RrcloudBridge.releaseStateLock()
    }

    /** The other half: reacquire on return to foreground. Also fires on a
     * cold start (before any `configure()` has ever run), which the
     * Rust-side `reacquire_after_foreground` treats as a no-op. */
    override fun onResume() {
        super.onResume()
        RrcloudBridge.reacquireStateLock()
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
