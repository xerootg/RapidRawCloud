//! `sync-*` event emission (ARCHITECTURE.md §3.8).
//!
//! The engine drives these from [`crate::sync::manager::SyncManager`]:
//! `publish_sync_events` emits `sync-status` + the batched `sync-item-state`
//! deltas at the end of each cycle, right after a local edit marks an item
//! dirty, and after a hydration; the hydrate path also emits `sync-hydrated`.
//! The frontend consumes them in `src/hooks/useTauriListeners.ts` to drive the
//! header badge and the per-photo grid icons without polling. Each helper is
//! always-compiled; the body emits under the `sync` feature and is a no-op
//! otherwise.

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

/// Diffs two `path -> ui-state` maps into the batch of `sync-item-state`
/// updates to emit: every path present in `now` whose state is new or changed
/// relative to `prev`. Paths that vanished from `now` are not reported (a
/// removed item simply drops out of the grid listing). Pure so the live-event
/// plumbing is unit-tested without a Tauri `AppHandle`.
pub fn diff_item_states(
    prev: &std::collections::HashMap<String, String>,
    now: &std::collections::HashMap<String, String>,
) -> Vec<SyncItemStateEvent> {
    let mut out: Vec<SyncItemStateEvent> = now
        .iter()
        .filter(|(path, state)| prev.get(*path) != Some(*state))
        .map(|(path, state)| SyncItemStateEvent {
            path: path.clone(),
            state: state.clone(),
        })
        .collect();
    // Deterministic order (the map iteration order is not) so tests and logs
    // are stable; the frontend applies them as an unordered batch regardless.
    out.sort_by(|a, b| a.path.cmp(&b.path));
    out
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn diff_reports_new_and_changed_paths_only() {
        let prev = map(&[("/a", "synced"), ("/b", "pending_up")]);
        // /a unchanged, /b changed, /c new.
        let now = map(&[("/a", "synced"), ("/b", "synced"), ("/c", "pending_down")]);
        let got = diff_item_states(&prev, &now);
        assert_eq!(
            got.iter()
                .map(|e| (e.path.as_str(), e.state.as_str()))
                .collect::<Vec<_>>(),
            vec![("/b", "synced"), ("/c", "pending_down")],
            "only changed (/b) and new (/c) paths, sorted by path"
        );
    }

    #[test]
    fn diff_is_empty_when_nothing_changed() {
        let m = map(&[("/a", "synced"), ("/b", "stub")]);
        assert!(
            diff_item_states(&m, &m).is_empty(),
            "an identical snapshot yields no events"
        );
    }

    #[test]
    fn diff_ignores_vanished_paths() {
        let prev = map(&[("/a", "synced"), ("/gone", "pending_up")]);
        let now = map(&[("/a", "synced")]);
        assert!(
            diff_item_states(&prev, &now).is_empty(),
            "a path dropping out of `now` emits nothing"
        );
    }
}
