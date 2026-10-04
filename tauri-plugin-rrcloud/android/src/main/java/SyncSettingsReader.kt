package com.plugin.rrcloud

import android.content.Context
import android.util.Log
import org.json.JSONObject
import java.io.File

/**
 * Reads the `sync` block out of the app's `settings.json`
 * (ARCHITECTURE.md §5.1): the same file `app_settings::get_settings_path`/
 * `save_settings` reads and writes on the Rust side, at
 * `context.filesDir/settings.json` — Tauri's Android `app_data_dir`
 * resolves to `context.filesDir` directly, so this is the exact path the
 * app process itself uses.
 *
 * Returns just the `sync` sub-object as a JSON string (matching
 * `rrcloud_core::android::bridge`'s minimal `AndroidSyncSettings` shape:
 * `enabled`/`endpoint`/`bucket`/`region`), or `null` when `settings.json`
 * does not exist yet or has no `sync` block — both read by the Rust side
 * as "not configured".
 */
object RrcloudSyncSettingsReader {
    private const val TAG = "RrcloudSettingsReader"

    private fun readSyncObject(context: Context): JSONObject? {
        val file = File(context.filesDir, "settings.json")
        if (!file.exists()) {
            return null
        }
        return try {
            val root = JSONObject(file.readText())
            if (root.has("sync")) root.getJSONObject("sync") else null
        } catch (e: Exception) {
            Log.e(TAG, "Failed to read/parse settings.json for sync settings", e)
            null
        }
    }

    fun readSyncSettingsJson(context: Context): String? = readSyncObject(context)?.toString()

    /** [`WorkManagerConstraints`] built straight from `settings.json`'s
     * `sync` block (`uploadRequiresUnmetered`/`uploadRequiresCharging`,
     * ARCHITECTURE.md §5.1 point 3's "constraints mapped 1:1 to settings")
     * — defaults (both `false`, matching `SyncSettings::default()` on the
     * Rust side) when `settings.json` or its `sync` block is absent.
     */
    fun readWorkConstraintSettings(context: Context): WorkConstraintSettings {
        val sync = readSyncObject(context) ?: return WorkConstraintSettings()
        return WorkConstraintSettings(
            requiresUnmetered = sync.optBoolean("uploadRequiresUnmetered", false),
            requiresCharging = sync.optBoolean("uploadRequiresCharging", false),
            autoWatchDcim = sync.optBoolean("autoWatchDcim", false),
            enabled = sync.optBoolean("enabled", false)
        )
    }
}

data class WorkConstraintSettings(
    val requiresUnmetered: Boolean = false,
    val requiresCharging: Boolean = false,
    val autoWatchDcim: Boolean = false,
    val enabled: Boolean = false
)
