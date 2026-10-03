//! Always-compiled hook shims (ARCHITECTURE.md §7 "single hook style").
//!
//! Every function here has an unconditional public signature and two
//! bodies: a real one under `cfg(feature = "sync")` and a no-op under
//! `cfg(not(feature = "sync"))`. The upstream call sites call these
//! unconditionally — never `#[cfg]`-gated — so `--no-default-features`
//! yields a build that is functionally identical to upstream while the
//! default build drives the engine.

use std::path::{Path, PathBuf};

use crate::image_processing::ImageMetadata;
use crate::sync::WriteOrigin;

/// A sidecar was atomically written and its semantic hash changed
/// (ARCHITECTURE.md §3.4 step 5). Enqueues the §3.7 debounced upload.
/// No-op when the semantic hash is unchanged (the churn gate lives in the
/// [`crate::exif_processing::save_sidecar`] chokepoint, which only calls
/// this when the hash actually moved).
pub fn notify_sidecar_saved(sidecar_path: &Path, meta: &ImageMetadata, origin: WriteOrigin) {
    #[cfg(feature = "sync")]
    {
        imp::notify_sidecar_saved(sidecar_path, meta, origin);
    }
    #[cfg(not(feature = "sync"))]
    {
        let _ = (sidecar_path, meta, origin);
    }
}

/// A new original (import / derived output / duplicate / copy) landed and
/// should be enqueued for upload (§3.4 "new-original hooks").
pub fn notify_new_original(path: &Path) {
    #[cfg(feature = "sync")]
    {
        imp::notify_new_original(path);
    }
    #[cfg(not(feature = "sync"))]
    {
        let _ = path;
    }
}

/// The local `albums.json` was saved (§2.9): relativize its in-root image
/// paths to `rr://` and sync it as the `.rrcloud/v1/meta/albums.json`
/// whole-document meta object. No-op when sync is off, so `--no-default-
/// features` keeps album save as plain local JSON (no `rr://` rewrite, no
/// engine call), functionally identical to upstream.
pub fn notify_albums_saved(albums_json_path: &Path) {
    #[cfg(feature = "sync")]
    {
        imp::notify_meta_saved(crate::sync::MetaKind::Albums, albums_json_path);
    }
    #[cfg(not(feature = "sync"))]
    {
        let _ = albums_json_path;
    }
}

/// The local `presets.json` was saved (§2.9): relativize its `lutPath`
/// references and sync it as the `.rrcloud/v1/meta/presets.json` whole-
/// document meta object. No-op when sync is off (parity with upstream,
/// same contract as [`notify_albums_saved`]).
pub fn notify_presets_saved(presets_json_path: &Path) {
    #[cfg(feature = "sync")]
    {
        imp::notify_meta_saved(crate::sync::MetaKind::Presets, presets_json_path);
    }
    #[cfg(not(feature = "sync"))]
    {
        let _ = presets_json_path;
    }
}

/// An item was deleted locally → soft delete + tombstone (§2.7). Deferred:
/// the real body currently only logs (no durable tombstone yet) and has no
/// upstream call site.
pub fn notify_deleted(path: &Path) {
    #[cfg(feature = "sync")]
    {
        imp::notify_deleted(path);
    }
    #[cfg(not(feature = "sync"))]
    {
        let _ = path;
    }
}

/// An item was moved/renamed → remote move (§2.7). Deferred: the real body
/// currently only logs (no durable move recorded yet) and has no upstream
/// call site.
pub fn notify_moved(from: &Path, to: &Path) {
    #[cfg(feature = "sync")]
    {
        imp::notify_moved(from, to);
    }
    #[cfg(not(feature = "sync"))]
    {
        let _ = (from, to);
    }
}

/// Whether `path` is a cloud stub (0-byte placeholder for a not-yet-
/// hydrated original). Backs `file_management::is_cloud_placeholder`
/// (§3.5). Always `false` when sync is off.
pub fn is_stub(path: &Path) -> bool {
    #[cfg(feature = "sync")]
    {
        imp::is_stub(path)
    }
    #[cfg(not(feature = "sync"))]
    {
        let _ = path;
        false
    }
}

/// A locally present smart preview for a sync stub (ARCHITECTURE.md §4.4).
/// Carries the proxy DNG path and the **original** (journal) dimensions the
/// proxy-mode load must report, plus the proxy's own long edge so the loader
/// can compute `proxy_scale`. Always compiled so the loader branch is
/// unconditional; `proxy_handle` returns `None` when sync is off or no proxy
/// is present, leaving the upstream hydrate/decode path untouched.
#[derive(Clone, Debug, PartialEq)]
pub struct ProxyHandle {
    /// Path to the local `.pxy.dng` smart preview.
    pub dng_path: PathBuf,
    /// Original displayed width from the journal/manifest (§2.2 provenance).
    pub orig_width: u32,
    /// Original displayed height from the journal/manifest.
    pub orig_height: u32,
    /// The proxy DNG's long edge (`<= 2560`), for `proxy_scale`.
    pub proxy_long_edge: u32,
}

/// Returns a [`ProxyHandle`] when `path` is a sync stub whose smart preview
/// is present locally (§4.4). `None` when sync is off, `path` is not a stub,
/// or no proxy has been downloaded — in which case the loader falls through
/// to the normal hydrate-then-decode path.
pub fn proxy_handle(path: &Path) -> Option<ProxyHandle> {
    #[cfg(feature = "sync")]
    {
        imp::proxy_handle(path)
    }
    #[cfg(not(feature = "sync"))]
    {
        let _ = path;
        None
    }
}

/// Ensures the original at `path` is present locally, hydrating a stub if
/// needed (§3.5). When sync is off this is an identity pass: the real file
/// is always present upstream, so it returns `path` unchanged.
pub fn ensure_local(path: &Path, reason: &str) -> std::io::Result<PathBuf> {
    #[cfg(feature = "sync")]
    {
        imp::ensure_local(path, reason)
    }
    #[cfg(not(feature = "sync"))]
    {
        let _ = reason;
        Ok(path.to_path_buf())
    }
}

/// The frontend's debounced-save flush points hint that `path` can leave
/// the §3.7 editor hold and be admitted for upload. No-op when sync is
/// off.
pub fn sync_flush_path(path: &Path) {
    #[cfg(feature = "sync")]
    {
        imp::sync_flush_path(path);
    }
    #[cfg(not(feature = "sync"))]
    {
        let _ = path;
    }
}

/// The `RunEvent::ExitRequested` bounded exit-flush hook (§3.3): drains
/// queued small sidecar uploads for up to ~2 s, then returns. No-op when
/// sync is off.
pub fn sync_exit_flush(app: &tauri::AppHandle) {
    #[cfg(feature = "sync")]
    {
        imp::sync_exit_flush(app);
    }
    #[cfg(not(feature = "sync"))]
    {
        let _ = app;
    }
}

#[cfg(feature = "sync")]
mod imp {
    use std::path::{Path, PathBuf};
    use std::time::Duration;

    use crate::image_processing::ImageMetadata;
    use crate::sync::{WriteOrigin, global_manager};

    /// The §3.3 exit-flush budget: bounded so the process never stalls on
    /// shutdown (ARCHITECTURE.md §3.3).
    const EXIT_FLUSH_BUDGET: Duration = Duration::from_secs(2);

    pub fn notify_sidecar_saved(sidecar_path: &Path, _meta: &ImageMetadata, origin: WriteOrigin) {
        // Route into the process-global manager (installed in `setup()` and
        // by the integration tests). When none is installed, or sync is
        // unconfigured, this is a cheap no-op (§3.3). The manager re-reads
        // the just-persisted bytes so the engine hashes exactly what landed
        // on disk.
        if let Some(mgr) = global_manager() {
            mgr.note_local_sidecar(sidecar_path, origin);
        }
    }

    pub fn notify_new_original(path: &Path) {
        // A new original (import / derived output / duplicate / copy) is
        // enqueued for upload through the same §2.5 intake, keyed by its own
        // relkey (§3.4 new-original hooks). Best-effort: any failure is
        // logged, never propagated to the upstream call site.
        if let Some(mgr) = global_manager() {
            mgr.note_new_original(path);
        }
    }

    pub fn notify_meta_saved(kind: crate::sync::MetaKind, meta_json_path: &Path) {
        // §2.9 albums/presets meta sync: hand the just-saved local document
        // to the process-global manager, which relativizes it and marks the
        // meta kind dirty for the next cycle. Best-effort: a missing manager
        // or an unconfigured engine is a cheap no-op, and any failure is
        // logged, never propagated to the upstream save site.
        if let Some(mgr) = global_manager() {
            mgr.note_local_meta(kind, meta_json_path);
        }
    }

    pub fn notify_deleted(path: &Path) {
        // §2.7 soft delete + tombstone — DEFERRED. `EngineConsumer` applies
        // only Put/Del inbound in P1; local delete→tombstone propagation is
        // documented v1 debt. For now `note_deleted` only logs the event —
        // nothing durable is recorded and no tombstone is produced. The hook
        // is also not yet wired at any upstream call site. Best-effort,
        // never panics.
        if let Some(mgr) = global_manager() {
            mgr.note_deleted(path);
        }
    }

    pub fn notify_moved(from: &Path, to: &Path) {
        // §2.7 remote move — DEFERRED, same status as `notify_deleted`:
        // `note_moved` only logs; nothing durable is recorded and the hook
        // has no upstream call site yet.
        if let Some(mgr) = global_manager() {
            mgr.note_moved(from, to);
        }
    }

    pub fn is_stub(path: &Path) -> bool {
        // Route through the process-global manager's in-memory stub mirror
        // (§3.5). No configured manager, or a path that is not a stub, both
        // answer `false` — exactly upstream behavior.
        global_manager()
            .map(|mgr| mgr.is_stub(path))
            .unwrap_or(false)
    }

    pub fn proxy_handle(path: &Path) -> Option<super::ProxyHandle> {
        // §4.4 proxy edit mode: a stub with a locally present smart preview
        // routes the editor load through the proxy DNG. No configured manager
        // or no proxy ⇒ `None`, so the loader falls through to hydrate.
        global_manager().and_then(|mgr| mgr.proxy_handle(path))
    }

    pub fn ensure_local(path: &Path, reason: &str) -> std::io::Result<PathBuf> {
        // §3.5 guard-site hook: hydrate a stub before any reader touches it.
        // No configured manager ⇒ identity pass (the real original is
        // already present). A hydration failure is surfaced as an io error
        // so the guarded call site reports it rather than decoding a 0-byte
        // stub.
        match global_manager() {
            Some(mgr) => mgr
                .ensure_local(path, reason)
                .map_err(|e| std::io::Error::other(e.to_string())),
            None => Ok(path.to_path_buf()),
        }
    }

    pub fn sync_flush_path(path: &Path) {
        // §3.7 editor-hold release hint. The P1 admission policy quiesces
        // every dirty item (`admit_pending(|_, _| true)`), so there is no
        // per-path hold to release yet; record the hint for the manager.
        if let Some(mgr) = global_manager() {
            mgr.note_flush_hint(path);
        }
    }

    pub fn sync_exit_flush(app: &tauri::AppHandle) {
        // Bounded (<=2s) opportunistic drain of queued small sidecar uploads
        // on `RunEvent::ExitRequested` (§3.3). The `AppHandle` is accepted
        // for call-site symmetry only; the drain routes through the
        // process-global manager.
        let _ = app;
        run_exit_flush(EXIT_FLUSH_BUDGET);
    }

    /// Drives the bounded exit flush to completion from a *synchronous*
    /// caller, hard-bounded so shutdown can never hang.
    ///
    /// The async drain is always run on a dedicated thread that owns a fresh
    /// current-thread runtime, rather than a `block_on` on the calling
    /// thread. Today the Tauri `RunEvent::ExitRequested` callback is not
    /// inside a tokio runtime, so a direct `block_on` would be fine; but once
    /// the P2 `configure` command lands this may be invoked from within the
    /// app's runtime, where a nested `block_on` panics with "Cannot start a
    /// runtime from within a runtime" (P1-U7 latent finding). Running on a
    /// separate thread removes that ambient runtime entirely, so it is safe
    /// from either context. The join is bounded because `exit_flush` itself
    /// times out at `budget`.
    pub(crate) fn run_exit_flush(budget: Duration) {
        let Some(mgr) = global_manager() else {
            return;
        };
        if !mgr.is_configured() {
            return;
        }
        let worker = std::thread::spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    log::warn!("exit flush: could not build runtime: {e}");
                    return;
                }
            };
            runtime.block_on(async {
                if let Err(e) = mgr.exit_flush(budget).await {
                    log::warn!("exit flush: {e}");
                }
            });
        });
        // Bounded by `exit_flush`'s own timeout; a join error (worker panic)
        // must not propagate onto the shutdown path.
        if worker.join().is_err() {
            log::warn!("exit flush: worker thread panicked");
        }
    }

    #[cfg(test)]
    mod tests {
        use std::sync::{Mutex, OnceLock};
        use std::time::{Duration, Instant};

        use crate::sync::{Credentials, SyncManager, SyncSettings};

        /// Serializes the tests that install the process-global manager.
        fn serial() -> std::sync::MutexGuard<'static, ()> {
            static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
            LOCK.get_or_init(|| Mutex::new(()))
                .lock()
                .unwrap_or_else(|e| e.into_inner())
        }

        /// A manager configured against an unreachable endpoint with nothing
        /// queued: `exit_flush` returns quickly (empty drain) and, even if it
        /// touched the network, is hard-bounded by its own timeout.
        fn configured() -> std::sync::Arc<SyncManager> {
            let root = tempfile::tempdir().expect("root");
            let state = tempfile::tempdir().expect("state");
            // Keep the state dir on disk for the lifetime of the test; the
            // manager opens a redb file under it.
            let root = root.keep();
            let state = state.keep();
            let mgr = SyncManager::new_inert();
            mgr.configure(
                SyncSettings {
                    enabled: true,
                    endpoint: "http://127.0.0.1:1".to_string(),
                    bucket: "exitflush-unit".to_string(),
                    region: "garage".to_string(),
                    ..SyncSettings::default()
                },
                Credentials {
                    access_key: "AKIATEST".to_string(),
                    secret_key: "secrettest".to_string(),
                },
                root,
                state,
            )
            .expect("configure");
            mgr
        }

        #[test]
        fn run_exit_flush_from_sync_context_returns_bounded() {
            let _g = serial();
            crate::sync::install_global_manager(configured());
            let start = Instant::now();
            super::run_exit_flush(Duration::from_millis(200));
            assert!(
                start.elapsed() < Duration::from_secs(8),
                "the exit-flush wrapper must return within its bound, got {:?}",
                start.elapsed()
            );
        }

        #[tokio::test]
        async fn run_exit_flush_within_a_tokio_runtime_does_not_panic() {
            let _g = serial();
            crate::sync::install_global_manager(configured());
            // Regression for the P1-U7 latent finding: invoking the exit-flush
            // wrapper from *inside* a tokio runtime must not panic with
            // "Cannot start a runtime from within a runtime".
            super::run_exit_flush(Duration::from_millis(200));
        }
    }
}
