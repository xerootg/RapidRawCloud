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

/// An item was deleted locally → soft delete + tombstone (§2.7).
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

/// An item was moved/renamed → remote move (§2.7).
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

    pub fn notify_deleted(path: &Path) {
        // §2.7 soft delete + tombstone. The full remote tombstone is an
        // async S3 effect driven by the supervisor; the hook records the
        // local intent for the next cycle. Best-effort, never panics.
        if let Some(mgr) = global_manager() {
            mgr.note_deleted(path);
        }
    }

    pub fn notify_moved(from: &Path, to: &Path) {
        // §2.7 remote move. Same deferral as `notify_deleted`: the hook
        // records intent; the async move effect rides the supervisor.
        if let Some(mgr) = global_manager() {
            mgr.note_moved(from, to);
        }
    }

    pub fn is_stub(path: &Path) -> bool {
        // Stubs are created only once the §3.5 hydration/eviction unit (P2)
        // lands; until then no path is a cloud stub, so the honest answer is
        // always `false` (every original is present on disk).
        let _ = path;
        false
    }

    pub fn ensure_local(path: &Path, _reason: &str) -> std::io::Result<PathBuf> {
        // No stubs exist yet (see `is_stub`), so hydration is an identity
        // pass: the real original is already present at `path` (P2 adds the
        // ranged-resume download).
        Ok(path.to_path_buf())
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
        // Bounded (<=2s) opportunistic drain of queued small sidecar
        // uploads on `RunEvent::ExitRequested` (§3.3). Runs the async flush
        // to completion on a transient current-thread runtime so the
        // synchronous Tauri run-event callback can call it directly, and is
        // itself hard-bounded so shutdown never hangs.
        let _ = app;
        let Some(mgr) = global_manager() else {
            return;
        };
        if !mgr.is_configured() {
            return;
        }
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
            if let Err(e) = mgr.exit_flush(EXIT_FLUSH_BUDGET).await {
                log::warn!("exit flush: {e}");
            }
        });
    }
}
