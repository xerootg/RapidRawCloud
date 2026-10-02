//! Tauri bridge for the `rrcloud-core` sync engine (architecture §3.1).
//!
//! This module is the *only* always-compiled surface the upstream files
//! touch. Every hook call site in an upstream file is an unconditional
//! one-line call into [`hooks`]; whether those calls do anything is
//! decided by the `sync` cargo feature (default on in the fork), never by
//! a `#[cfg]` at the call site (ARCHITECTURE.md §7, "single hook style").
//!
//! - [`hooks`]: the always-compiled shim fns (feature-gated bodies).
//! - [`manager`]: [`SyncManager`] — the lifecycle object held in
//!   `AppState`, with a plain-Rust API (`configure` / `run_once` /
//!   `status` / `exit_flush`) that the integration tests drive directly so
//!   the Tauri command/IPC layer can stay in the next unit (§3.3).
//! - [`credentials`]: the desktop file-backed credential store plus the
//!   trait seam for an Android keystore store (§3.6). Credentials never
//!   enter `settings.json` or the webview.
//! - [`events`]: the `sync-*` emit helpers (§3.8), batched; no frontend
//!   yet but the emit API exists.
//!
//! When the `sync` feature is off the whole bridge is inert: hooks are
//! no-ops, `SyncManager` is an empty shell, and the build is functionally
//! identical to upstream (§7).

use std::path::PathBuf;
use std::sync::Arc;

use dashmap::DashMap;
use once_cell::sync::Lazy;

pub mod credentials;
pub mod events;
pub mod hooks;
pub mod manager;

pub use credentials::{CredentialStore, Credentials, FileCredentialStore};
pub use manager::{SyncError, SyncManager, SyncState, SyncStatus};

// Re-exports so the integration tests (and future command layer) reach the
// chokepoint and settings types through one module path.
pub use crate::app_settings::SyncSettings;
pub use crate::exif_processing::{save_sidecar, update_sidecar};
pub use crate::image_processing::ImageMetadata;

/// Where a sidecar write came from, threaded through the [`save_sidecar`]
/// chokepoint (ARCHITECTURE.md §3.4). Lets the engine distinguish a user
/// edit from an EXIF-cache rewrite / AI-tagging pass / XMP import for the
/// §2.5 churn gate and the §2.6 version-vector provenance.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteOrigin {
    /// A direct user edit (editor autosave).
    User,
    /// A batch adjustment/reset/label/rating operation over many paths.
    Batch,
    /// A user tag edit (`modify_tags_for_path`).
    UserTag,
    /// AI tagging (background indexing / clear-tags passes).
    AiTagging,
    /// An EXIF-cache field rewrite (`update_exif_fields`) — usually churn.
    ExifCache,
    /// A metadata field pulled in from a sibling XMP (`enable_xmp_sync`).
    XmpImport,
    /// A new virtual copy's sidecar.
    VirtualCopy,
    /// `load_sidecar`'s auto-heal rewrite (bloated-exif truncation).
    AutoHeal,
}

/// The per-path sidecar lock map (ARCHITECTURE.md §3.4 step 1): serializes
/// the three writers to one sidecar (user edit, AI tagging, sync's inbound
/// apply). A process-global map, held by `AppState` as a shared handle, so
/// the chokepoint serializes correctly whether or not an `AppHandle` is in
/// scope at the call site.
pub type SidecarLockMap = Arc<DashMap<PathBuf, Arc<std::sync::Mutex<()>>>>;

static SIDECAR_LOCKS: Lazy<SidecarLockMap> = Lazy::new(|| Arc::new(DashMap::new()));

/// A shared handle to the process-global sidecar lock map.
pub fn sidecar_locks() -> SidecarLockMap {
    SIDECAR_LOCKS.clone()
}

/// Acquires (creating if needed) the lock guarding `path`.
pub fn sidecar_lock_for(path: &std::path::Path) -> Arc<std::sync::Mutex<()>> {
    SIDECAR_LOCKS
        .entry(path.to_path_buf())
        .or_insert_with(|| Arc::new(std::sync::Mutex::new(())))
        .clone()
}

/// Removes `path`'s lock entry iff no writer currently holds it (its only
/// remaining strong ref is the map's own, `strong_count == 1`). Keeps the
/// map from growing one permanent entry per distinct sidecar path for the
/// process lifetime (P1-U7 review).
///
/// Safe w.r.t. the per-path serialization guarantee: `remove_if` and
/// `sidecar_lock_for`'s `entry` both take the same DashMap shard write lock,
/// so a concurrent `sidecar_lock_for(path)` cannot clone the `Arc` between
/// the count check and the removal. Removal therefore happens only when no
/// live writer holds the lock; a subsequent writer re-creates a fresh lock,
/// and two live writers for one path can never hold different mutexes.
pub fn prune_sidecar_lock(path: &std::path::Path) {
    SIDECAR_LOCKS.remove_if(path, |_, lock| Arc::strong_count(lock) == 1);
}

// The process-global manager handle. The chokepoint's hook bodies route
// through this so a `save_sidecar` call reaches the configured engine even
// at call sites that never see the Tauri `AppHandle` (the ~15 batch write
// sites). Installed in `setup()` and by the integration tests.
static GLOBAL_MANAGER: Lazy<std::sync::RwLock<Option<Arc<SyncManager>>>> =
    Lazy::new(|| std::sync::RwLock::new(None));

/// Installs the process-global [`SyncManager`] the hooks route through.
pub fn install_global_manager(manager: Arc<SyncManager>) {
    if let Ok(mut guard) = GLOBAL_MANAGER.write() {
        *guard = Some(manager);
    }
}

/// The process-global [`SyncManager`], if one has been installed.
pub fn global_manager() -> Option<Arc<SyncManager>> {
    GLOBAL_MANAGER.read().ok().and_then(|g| g.clone())
}
