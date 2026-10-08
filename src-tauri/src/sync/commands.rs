//! Tauri command surface for cloud sync (ARCHITECTURE.md §3.3/§3.5/§3.6/§3.8).
//!
//! The whole module is gated on the `sync` feature: a `--no-default-features`
//! build links none of it and `lib.rs` registers no `sync_*` command, so the
//! IPC surface is functionally upstream (§7). The webview call-guards on the
//! absence of these commands (an `invoke` of a missing command rejects, which
//! the frontend treats as "sync unavailable").
//!
//! Handlers are thin wrappers over [`SyncManager`] and the credential store.
//! The webview-facing logic that is worth testing without a Tauri app harness
//! (status projection, credential writes, (re)configure, hydrate) lives in the
//! plain-Rust `*_core` functions below, which the command tests drive directly
//! — mirroring how P1/P2 drive the manager API without the IPC layer.
//!
//! **Credentials never cross this boundary outbound (§3.6).** `sync_status`
//! exposes only `credentials_configured: bool`; `sync_set_credentials` writes
//! to the Rust-only store and returns `()` — never an echo.

#![cfg(feature = "sync")]

use std::path::PathBuf;

use serde::Serialize;
use tauri::{AppHandle, Manager, State};

use crate::app_settings::SyncSettings;
use crate::app_state::AppState;
#[cfg(target_os = "android")]
use crate::sync::credentials::AndroidCredentialStore;
#[cfg(not(target_os = "android"))]
use crate::sync::credentials::FileCredentialStore;
use crate::sync::credentials::{CredentialStore, Credentials};
use crate::sync::manager::{
    ConflictKeep, PeerDevice, RecentlyDeleted, SyncManager, SyncState, VerifyReport,
};

/// The §3.8 `sync-status` snapshot surfaced to the webview. Credentials NEVER
/// appear here — only [`credentials_configured`](Self::credentials_configured)
/// (§3.6). Mirrors [`crate::sync::events::SyncStatusEvent`] plus the
/// command-only `configured` / `credentials_configured` / device facts.
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncStatusDto {
    /// `idle` | `syncing` | `offline` | `error`.
    pub state: String,
    pub pending_up: usize,
    pub pending_down: usize,
    pub bytes_up: u64,
    pub bytes_down: u64,
    pub dirty_unbacked: usize,
    /// Whether [`SyncManager::configure`] has run.
    pub configured: bool,
    /// Whether both credential halves are present in the Rust-only store.
    pub credentials_configured: bool,
    /// This device's registry id (`None` when unconfigured).
    pub device_id: Option<String>,
    /// The shared device registry (§2.10) for the settings device panel.
    pub peer_devices: Vec<PeerDevice>,
}

// ===== testable command cores (no Tauri types) ============================

/// Projects [`SyncManager::status`] + device facts into the webview DTO,
/// stamping `credentials_configured` from the (separately-read) store. The
/// projection that must never carry secret material (§3.6).
pub fn status_core(manager: &SyncManager, credentials_configured: bool) -> SyncStatusDto {
    let status = manager.status();
    let state = match status.state {
        SyncState::Idle => "idle",
        SyncState::Syncing => "syncing",
        SyncState::Offline => "offline",
        SyncState::Error => "error",
    };
    SyncStatusDto {
        state: state.to_string(),
        pending_up: status.pending_up,
        pending_down: status.pending_down,
        // Byte-level transfer accounting is not tracked in `SyncStatus` yet
        // (the per-cycle counters are object counts); report 0 rather than a
        // fabricated figure. Never carries credential material (§3.6).
        bytes_up: 0,
        bytes_down: 0,
        dirty_unbacked: status.dirty_unbacked,
        configured: status.configured,
        credentials_configured,
        device_id: manager.device_id(),
        peer_devices: manager.peer_devices(),
    }
}

/// Persists `access_key`/`secret_key` to the Rust-only credential store and
/// returns NOTHING (§3.6) — the webview never receives a secret echo.
pub fn set_credentials_core(
    store: &dyn CredentialStore,
    access_key: String,
    secret_key: String,
) -> Result<(), String> {
    let creds = Credentials {
        access_key,
        secret_key,
    };
    store.store(&creds).map_err(|e| e.to_string())
}

/// Whether both credential halves are present in the store (§3.6) — the only
/// credential fact the webview ever learns.
pub fn credentials_configured_core(store: &dyn CredentialStore) -> bool {
    matches!(store.load(), Ok(Some(creds)) if creds.is_complete())
}

/// Reads credentials from `store` and (re)points the manager at `settings` +
/// the given roots (§3.3). Settings persistence (`save_settings`) is the
/// command wrapper's job — this core owns only the manager rebind so it is
/// testable without an `AppHandle`.
pub fn configure_core(
    manager: &SyncManager,
    store: &dyn CredentialStore,
    settings: SyncSettings,
    sync_root: PathBuf,
    state_dir: PathBuf,
) -> Result<(), String> {
    // Credentials come only from the Rust-only store (§3.6) — never from the
    // settings the webview round-trips. An unconfigured store binds the engine
    // with empty credentials (the first cycle then surfaces the auth gap),
    // rather than refusing to (re)point the manager at the new settings.
    let creds = store.load().map_err(|e| e.to_string())?.unwrap_or_default();
    // `SyncManager::configure` tears down any prior `Configured` (dropping its
    // handle) as it installs the fresh engine, so a re-configure rebinds the
    // manager cleanly.
    manager
        .configure(settings, creds, sync_root, state_dir)
        .map_err(|e| e.to_string())
}

/// Startup auto-configure (§3.3). On launch, re-point the in-process
/// [`SyncManager`] at the saved sync settings + stored credentials so the
/// app-crate engine is actually LIVE for the session — the status badge then
/// reflects the real state db, imports get tracked (`notify_new_original`), and
/// the foreground cycle ([`SyncManager::spawn_foreground_cycle`]) can run and
/// emit live events. Without this the manager stays inert until the user opens
/// Settings → Sync and taps Save, so a normal launch's imports never sync and
/// the badge is only coincidentally right. A no-op when sync is disabled or no
/// library is open. A state-db lock contention (the Android background worker
/// momentarily holds it) is logged, not fatal — the §5.1 foreground reacquire /
/// the next cycle recovers.
pub fn auto_configure_on_startup(
    app: &AppHandle,
    manager: &SyncManager,
    settings: &crate::app_settings::AppSettings,
) {
    if !settings.sync.enabled {
        return;
    }
    let Some(sync_root) = settings
        .root_folders
        .first()
        .cloned()
        .or_else(|| settings.last_root_path.clone())
        .map(PathBuf::from)
    else {
        return; // no library open yet; nothing to key the engine against
    };
    let state_dir = match app.path().app_data_dir() {
        Ok(dir) => dir.join("rrcloud"),
        Err(e) => {
            log::warn!("sync auto-configure: no app_data_dir: {e}");
            return;
        }
    };
    let store = match credential_store(app) {
        Ok(store) => store,
        Err(e) => {
            log::warn!("sync auto-configure: credential store unavailable: {e}");
            return;
        }
    };
    match configure_core(
        manager,
        &*store,
        settings.sync.clone(),
        sync_root,
        state_dir,
    ) {
        Ok(()) => log::info!("sync: auto-configured the in-process engine from saved settings"),
        Err(e) => log::warn!("sync auto-configure failed (will retry on a later cycle): {e}"),
    }
}

/// "Make available offline" (§3.5): hydrate the stub at `path` to its original
/// bytes, returning when done. Wraps [`SyncManager::ensure_local`] on a
/// blocking worker so the async command never stalls the runtime.
pub fn hydrate_core(manager: &SyncManager, path: PathBuf) -> Result<(), String> {
    // `ensure_local` is idempotent (a no-op on a non-stub) and runs the ranged
    // download on its own dedicated-thread runtime, so it is safe to call from
    // the async command without stalling the app's runtime on a nested
    // `block_on`.
    manager
        .ensure_local(&path, "make-available-offline")
        .map(|_| ())
        .map_err(|e| e.to_string())
}

// ===== credential store location ==========================================

/// The platform credential store seam (§3.6/§5.1): a Keystore-backed JNI
/// shim on Android, a `0600` JSON file under `app_data_dir/rrcloud` on
/// desktop. Boxed so both platforms share one return type at this, the
/// single decision point every command below goes through — the Android
/// `tauri-plugin-rrcloud` JNI bridge's own worker-process credential read
/// is a *separate* call path (it has no `AppHandle`/webview to be a Tauri
/// command in the first place) that happens to reach the exact same
/// Kotlin-side Keystore store via the same JNI methods (see
/// `android_integration::android_credential_store_load` and
/// `rrcloud_core::android::bridge`'s doc).
pub(crate) fn credential_store(app: &AppHandle) -> Result<Box<dyn CredentialStore>, String> {
    #[cfg(target_os = "android")]
    {
        let _ = app;
        Ok(Box::new(AndroidCredentialStore::new()))
    }
    #[cfg(not(target_os = "android"))]
    {
        let dir = app
            .path()
            .app_data_dir()
            .map_err(|e| e.to_string())?
            .join("rrcloud");
        Ok(Box::new(FileCredentialStore::new(&dir)))
    }
}

// ===== Tauri commands =====================================================

/// §3.8 status snapshot. Carries `credentials_configured: bool` only (§3.6).
#[tauri::command]
pub fn sync_status(state: State<'_, AppState>, app: AppHandle) -> Result<SyncStatusDto, String> {
    let store = credential_store(&app)?;
    let creds = credentials_configured_core(&*store);
    Ok(status_core(&state.sync_manager, creds))
}

/// §3.3 foreground sync cycle. Runs one full cycle IN-PROCESS through the
/// app-crate [`SyncManager`] (upload lane → inbound poll + apply → download
/// lane) and returns the resulting status. Unlike the Android background
/// WorkManager cycle (which runs a parallel implementation in `rrcloud-core`
/// and never touches this manager), this path emits the live `sync-status` /
/// `sync-item-state` events the webview badges consume — so the frontend calls
/// it on resume / right after an import / on a light foreground interval to make
/// sync visibly progress and to bootstrap the bucket's photos into the library
/// grid. A no-op (returns the current status) when sync is not configured.
#[tauri::command]
pub fn sync_run_cycle(state: State<'_, AppState>, app: AppHandle) -> Result<SyncStatusDto, String> {
    // Kick the cycle on its own thread and return the current status at once;
    // the cycle emits live `sync-*` events as it progresses, so the webview
    // updates without this call blocking on uploads/downloads. The guard inside
    // `spawn_foreground_cycle` coalesces overlapping pokes into one cycle.
    state.sync_manager.spawn_foreground_cycle();
    let store = credential_store(&app)?;
    let creds = credentials_configured_core(&*store);
    Ok(status_core(&state.sync_manager, creds))
}

/// Persists `settings` (via the settings path) and reconfigures the manager.
#[tauri::command]
pub async fn sync_configure(
    settings: SyncSettings,
    state: State<'_, AppState>,
    app: AppHandle,
) -> Result<(), String> {
    // Persist the sync block through the existing settings path (credentials
    // are NOT part of `AppSettings`, so nothing secret is written — §3.6).
    let mut app_settings = crate::app_settings::load_settings(app.clone())?;
    app_settings.sync = settings.clone();
    crate::app_settings::save_settings(app_settings.clone(), app.clone())?;

    // The library root the engine keys relative to: the first configured root
    // folder (falling back to the last opened one). Reconfiguring sync with no
    // library open is a user error, not a panic.
    let sync_root = app_settings
        .root_folders
        .first()
        .cloned()
        .or_else(|| app_settings.last_root_path.clone())
        .map(PathBuf::from)
        .ok_or_else(|| "cannot enable sync: no library folder is open".to_string())?;
    let state_dir = app
        .path()
        .app_data_dir()
        .map_err(|e| e.to_string())?
        .join("rrcloud");

    let store = credential_store(&app)?;
    configure_core(&state.sync_manager, &*store, settings, sync_root, state_dir)
}

/// The device's camera-roll folder names (MediaStore image buckets) for
/// the per-device "Add folder" DCIM-watch picker. Android-only data;
/// desktop returns an empty list (the DCIM watch is an Android feature).
#[tauri::command]
pub fn sync_list_media_buckets() -> Result<Vec<String>, String> {
    #[cfg(target_os = "android")]
    {
        crate::android_integration::android_list_media_buckets()
    }
    #[cfg(not(target_os = "android"))]
    {
        Ok(Vec::new())
    }
}

/// Writes credentials to the Rust-only store (§3.6). Returns `()`, never an
/// echo of the secret material.
#[tauri::command]
pub fn sync_set_credentials(
    access_key: String,
    secret_key: String,
    app: AppHandle,
) -> Result<(), String> {
    let store = credential_store(&app)?;
    set_credentials_core(&*store, access_key, secret_key)
}

/// Pins paths so the evictor never reclaims them (§3.5).
#[tauri::command]
pub fn sync_pin_paths(paths: Vec<String>, state: State<'_, AppState>) -> Result<usize, String> {
    let paths: Vec<PathBuf> = paths.into_iter().map(PathBuf::from).collect();
    state
        .sync_manager
        .pin_paths(&paths, true)
        .map_err(|e| e.to_string())
}

/// Unpins paths (§3.5).
#[tauri::command]
pub fn sync_unpin_paths(paths: Vec<String>, state: State<'_, AppState>) -> Result<usize, String> {
    let paths: Vec<PathBuf> = paths.into_iter().map(PathBuf::from).collect();
    state
        .sync_manager
        .pin_paths(&paths, false)
        .map_err(|e| e.to_string())
}

/// "Free up space" (§3.5): evict the named originals back to stubs.
#[tauri::command]
pub fn sync_free_space(paths: Vec<String>, state: State<'_, AppState>) -> Result<usize, String> {
    let paths: Vec<PathBuf> = paths.into_iter().map(PathBuf::from).collect();
    state
        .sync_manager
        .evict_paths(&paths)
        .map_err(|e| e.to_string())
}

/// "Make available offline" (§3.5): hydrate the stub at `path`.
///
/// The ranged GET inside [`hydrate_core`] is blocking, so it runs on a
/// dedicated `spawn_blocking` thread rather than parking a tokio worker for the
/// full multi-MB download.
#[tauri::command]
pub async fn sync_hydrate(path: String, state: State<'_, AppState>) -> Result<(), String> {
    let manager = state.sync_manager.clone();
    tokio::task::spawn_blocking(move || hydrate_core(&manager, PathBuf::from(path)))
        .await
        .map_err(|e| format!("hydrate task join error: {e}"))?
}

/// The "Recently Deleted" view (§3.8).
#[tauri::command]
pub fn sync_recently_deleted(state: State<'_, AppState>) -> Result<Vec<RecentlyDeleted>, String> {
    state
        .sync_manager
        .recently_deleted()
        .map_err(|e| e.to_string())
}

/// Restores a soft-deleted item (§2.7). Returns the restored paths.
#[tauri::command]
pub fn sync_restore(path: String, state: State<'_, AppState>) -> Result<Vec<String>, String> {
    state
        .sync_manager
        .restore(&PathBuf::from(path))
        .map(|paths| {
            paths
                .into_iter()
                .map(|p| p.to_string_lossy().into_owned())
                .collect()
        })
        .map_err(|e| e.to_string())
}

/// Resolves a §2.6 conflict from the `sync-conflict` toast (§3.8).
#[tauri::command]
pub fn sync_resolve_conflict(
    path: String,
    keep: ConflictKeep,
    state: State<'_, AppState>,
) -> Result<(), String> {
    state
        .sync_manager
        .resolve_conflict(&PathBuf::from(path), keep)
        .map_err(|e| e.to_string())
}

/// Retires a device from the shared registry (§2.10).
#[tauri::command]
pub async fn sync_retire_device(
    device_id: String,
    state: State<'_, AppState>,
) -> Result<(), String> {
    state
        .sync_manager
        .retire_device(&device_id)
        .await
        .map_err(|e| e.to_string())
}

/// §3.7 editor-hold flush hint: the webview calls this at the three
/// `debouncedSave` flush points so the editor-open path's queued sidecar is
/// admitted promptly on image-switch / back-to-library. Advisory (the P1
/// admission policy already quiesces every dirty item), so it is best-effort
/// and never fails.
#[tauri::command]
pub fn sync_flush_path(path: String, state: State<'_, AppState>) {
    state.sync_manager.note_flush_hint(&PathBuf::from(path));
}

/// Kicks a wholeness reconcile (§3.5) — the settings "Verify library" action.
#[tauri::command]
pub async fn sync_verify_library(state: State<'_, AppState>) -> Result<VerifyReport, String> {
    state
        .sync_manager
        .verify_library()
        .await
        .map_err(|e| e.to_string())
}
