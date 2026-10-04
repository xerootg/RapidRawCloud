//! The one genuine JS-facing command this plugin needs (ARCHITECTURE.md
//! §5.1 point 2): starting the user-initiated foreground sync service while
//! the app is in the foreground (the only time `FOREGROUND_SERVICE_DATA_SYNC`
//! may legally be requested). A settings-panel "Sync now" button wiring this
//! up is P6/polish-adjacent and out of scope here; this command is the whole
//! trigger surface.
//!
//! [`sync_dcim_access_status`] is the §5.2 partial-photo-access (Android 14
//! `READ_MEDIA_VISUAL_USER_SELECTED`) detection surface: the settings UI can
//! poll it to show "watching N selected photos — expand access" instead of
//! implying full DCIM coverage. Desktop has no such grant model, so it
//! always reports full access.

use serde::{Deserialize, Serialize};
use tauri::{command, AppHandle, Runtime};

use crate::RrcloudExt;

/// §5.2 DCIM watch coverage, as the settings UI would render it. Also
/// `Deserialize` so `PluginHandle::run_mobile_plugin` can decode the
/// Kotlin `checkPartialMediaAccess` command's JSON response into it.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DcimAccessStatus {
    /// `true` when the OS granted only a user-selected subset of photos
    /// (Android 14 `READ_MEDIA_VISUAL_USER_SELECTED`) rather than full
    /// `READ_MEDIA_IMAGES`/`READ_EXTERNAL_STORAGE` — the DCIM watch only
    /// sees the selected subset until the user expands it.
    pub partial: bool,
}

/// Starts the §5.1 point 2 user-initiated long operation: a genuine
/// `FOREGROUND_SERVICE_DATA_SYNC` with a progress notification, running
/// bounded cycles back-to-back until caught up or the Android 15 6h/24h
/// `dataSync` cap. No-op on desktop, where the in-process engine already
/// runs freely in the foreground (§5.1 point 1) — there is no separate
/// "foreground sync mode" to start.
#[command]
pub async fn sync_start_foreground_sync<R: Runtime>(app: AppHandle<R>) -> crate::Result<()> {
    app.rrcloud().start_foreground_sync()
}

/// §5.2 partial-photo-access detection. Always `partial: false` on desktop.
#[command]
pub async fn sync_dcim_access_status<R: Runtime>(
    app: AppHandle<R>,
) -> crate::Result<DcimAccessStatus> {
    app.rrcloud().dcim_access_status()
}
