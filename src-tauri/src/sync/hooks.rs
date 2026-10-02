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

    use crate::image_processing::ImageMetadata;
    use crate::sync::WriteOrigin;

    pub fn notify_sidecar_saved(sidecar_path: &Path, meta: &ImageMetadata, origin: WriteOrigin) {
        let _ = (sidecar_path, meta, origin);
        todo!(
            "P1-U7: route sidecar-saved into the global SyncManager (§3.4 step 5 / §3.7 admission)"
        )
    }

    pub fn notify_new_original(path: &Path) {
        let _ = path;
        todo!("P1-U7: enqueue a new original for upload (§3.4 new-original hooks)")
    }

    pub fn notify_deleted(path: &Path) {
        let _ = path;
        todo!("P1-U7: soft delete + tombstone (§2.7)")
    }

    pub fn notify_moved(from: &Path, to: &Path) {
        let _ = (from, to);
        todo!("P1-U7: remote move (§2.7)")
    }

    pub fn is_stub(path: &Path) -> bool {
        let _ = path;
        todo!("P2: in-memory stub mirror of redb (§3.5)")
    }

    pub fn ensure_local(path: &Path, reason: &str) -> std::io::Result<PathBuf> {
        let _ = (path, reason);
        todo!("P2: hydrate stub with ranged resume (§3.5)")
    }

    pub fn sync_flush_path(path: &Path) {
        let _ = path;
        todo!("P1-U7: release the §3.7 editor hold for this path")
    }

    pub fn sync_exit_flush(app: &tauri::AppHandle) {
        let _ = app;
        todo!("P1-U7: bounded (<=2s) exit flush of queued small sidecar uploads (§3.3)")
    }
}
