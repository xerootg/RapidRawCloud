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
use crate::sync::credentials::{CredentialStore, FileCredentialStore};
use crate::sync::manager::{ConflictKeep, PeerDevice, RecentlyDeleted, SyncManager, VerifyReport};

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
    let _ = (manager, credentials_configured);
    todo!("U8 green: project status + device_id + peer_devices into SyncStatusDto")
}

/// Persists `access_key`/`secret_key` to the Rust-only credential store and
/// returns NOTHING (§3.6) — the webview never receives a secret echo.
pub fn set_credentials_core(
    store: &dyn CredentialStore,
    access_key: String,
    secret_key: String,
) -> Result<(), String> {
    let _ = (store, access_key, secret_key);
    todo!("U8 green: write Credentials to the 0600 store, return ()")
}

/// Whether both credential halves are present in the store (§3.6) — the only
/// credential fact the webview ever learns.
pub fn credentials_configured_core(store: &dyn CredentialStore) -> bool {
    let _ = store;
    todo!("U8 green: report Credentials::is_complete from the store")
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
    let _ = (manager, store, settings, sync_root, state_dir);
    todo!(
        "U8 green: read creds from store + SyncManager::configure (stopping any running cycle first)"
    )
}

/// "Make available offline" (§3.5): hydrate the stub at `path` to its original
/// bytes, returning when done. Wraps [`SyncManager::ensure_local`] on a
/// blocking worker so the async command never stalls the runtime.
pub fn hydrate_core(manager: &SyncManager, path: PathBuf) -> Result<(), String> {
    let _ = (manager, path);
    todo!("U8 green: SyncManager::ensure_local on spawn_blocking, map to ()")
}

// ===== credential store location ==========================================

/// The desktop credential store under `app_data_dir/rrcloud` (§3.6).
fn credential_store(app: &AppHandle) -> Result<FileCredentialStore, String> {
    let dir = app
        .path()
        .app_data_dir()
        .map_err(|e| e.to_string())?
        .join("rrcloud");
    Ok(FileCredentialStore::new(&dir))
}

// ===== Tauri commands =====================================================

/// §3.8 status snapshot. Carries `credentials_configured: bool` only (§3.6).
#[tauri::command]
pub fn sync_status(state: State<'_, AppState>, app: AppHandle) -> Result<SyncStatusDto, String> {
    let store = credential_store(&app)?;
    let creds = credentials_configured_core(&store);
    Ok(status_core(&state.sync_manager, creds))
}

/// Persists `settings` (via the settings path) and reconfigures the manager.
#[tauri::command]
pub async fn sync_configure(
    settings: SyncSettings,
    state: State<'_, AppState>,
    app: AppHandle,
) -> Result<(), String> {
    // GREEN: load AppSettings, set `.sync = settings`, save_settings(app),
    // resolve sync_root (library root) + state_dir (app_data/rrcloud), then
    // configure_core(&state.sync_manager, &store, settings, root, state_dir).
    let _ = (settings, &state, &app);
    todo!("U8 green: persist settings + configure_core")
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
    set_credentials_core(&store, access_key, secret_key)
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
#[tauri::command]
pub async fn sync_hydrate(path: String, state: State<'_, AppState>) -> Result<(), String> {
    hydrate_core(&state.sync_manager, PathBuf::from(path))
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
