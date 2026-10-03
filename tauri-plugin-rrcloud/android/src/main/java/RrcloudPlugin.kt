package com.plugin.rrcloud

import android.app.Activity
import app.tauri.annotation.TauriPlugin
import app.tauri.plugin.Plugin

/**
 * The Tauri-facing half of the P5 Android platform work
 * (ARCHITECTURE.md §5). No commands yet (see the Rust
 * `tauri-plugin-rrcloud::commands` module doc) — `sync_start_foreground_sync`
 * (§5.1 point 2) is the one genuine command this plugin needs, and lands in
 * the P5 green pass alongside the `SyncCycleWorker`/`DcimScanWorker`
 * registration, the Keystore-backed `CredentialStore`, and the
 * `ContentObserver` DCIM watch this class will own.
 *
 * Registering the (currently empty) plugin class here still matters: it is
 * what makes [RrcloudBridge]'s native-library load happen as part of the
 * app's normal Tauri plugin lifecycle, from the moment this crate is a
 * dependency — not deferred to whenever the first command lands.
 */
@TauriPlugin
class RrcloudPlugin(private val activity: Activity) : Plugin(activity)
