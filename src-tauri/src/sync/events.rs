//! `sync-*` event emission (ARCHITECTURE.md §3.8).
//!
//! No frontend consumer ships in this unit, but the emit API exists so the
//! engine and the (next unit's) command layer have one place to batch and
//! emit status. Each helper is always-compiled; the body emits under the
//! `sync` feature and is a no-op otherwise.

use serde::Serialize;
use tauri::AppHandle;

/// `sync-status` payload (§3.8), capped to ~1 Hz by the emitter.
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncStatusEvent {
    /// `idle` | `syncing` | `offline` | `error`.
    pub state: String,
    pub pending_up: usize,
    pub pending_down: usize,
    pub bytes_up: u64,
    pub bytes_down: u64,
    pub dirty_unbacked: usize,
}

/// `sync-item-state` payload (§3.8), batched.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncItemStateEvent {
    pub path: String,
    pub state: String,
}

/// `sync-hydrate-progress` payload (§3.5): per-stub hydration byte progress.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncHydrateProgressEvent {
    pub path: String,
    pub bytes: u64,
    pub total: u64,
}

/// `sync-hydrated` payload (§3.5): a stub finished hydrating to the real
/// original bytes.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncHydratedEvent {
    pub path: String,
}

/// Emits `sync-status`.
pub fn emit_status(app: &AppHandle, status: &SyncStatusEvent) {
    emit(app, "sync-status", status);
}

/// Emits a batch of `sync-item-state` updates.
pub fn emit_item_states(app: &AppHandle, items: &[SyncItemStateEvent]) {
    emit(app, "sync-item-state", items);
}

/// Emits `sync-hydrate-progress {path, bytes, total}` (§3.5).
pub fn emit_hydrate_progress(app: &AppHandle, progress: &SyncHydrateProgressEvent) {
    emit(app, "sync-hydrate-progress", progress);
}

/// Emits `sync-hydrated {path}` (§3.5).
pub fn emit_hydrated(app: &AppHandle, hydrated: &SyncHydratedEvent) {
    emit(app, "sync-hydrated", hydrated);
}

/// Emits `sync-error {path?, message}`.
pub fn emit_error(app: &AppHandle, path: Option<&str>, message: &str) {
    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct ErrorEvent<'a> {
        path: Option<&'a str>,
        message: &'a str,
    }
    emit(app, "sync-error", &ErrorEvent { path, message });
}

fn emit<T: Serialize + ?Sized>(app: &AppHandle, event: &str, payload: &T) {
    #[cfg(feature = "sync")]
    {
        use tauri::Emitter;
        // Serialize to an owned `Value` (which is `Serialize + Clone`, as
        // `Emitter::emit` requires) so the batched slice / borrowed error
        // payloads need not be `Clone` themselves.
        match serde_json::to_value(payload) {
            Ok(value) => {
                if let Err(e) = app.emit(event, value) {
                    log::warn!("failed to emit {event}: {e}");
                }
            }
            Err(e) => log::warn!("failed to serialize {event} payload: {e}"),
        }
    }
    #[cfg(not(feature = "sync"))]
    {
        let _ = (app, event, payload);
    }
}
