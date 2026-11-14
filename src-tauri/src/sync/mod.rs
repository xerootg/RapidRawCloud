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

use std::sync::Arc;

use once_cell::sync::Lazy;

#[cfg(feature = "sync")]
pub mod commands;
pub mod credentials;
pub mod events;
pub mod headless_gpu;
pub mod hooks;
pub mod manager;
#[cfg(feature = "sync")]
pub mod pairing;

pub use credentials::{CredentialStore, Credentials, FileCredentialStore};

// §2.9 albums/presets meta sync. `MetaKind` is a `rrcloud-core` type, and
// the crate is linked only under the `sync` feature, so the re-export is
// feature-gated. Its only non-test consumers — the save-site hooks in
// `hooks::imp` and `SyncManager::note_local_meta` — are feature-gated too,
// so `--no-default-features` never names it.
#[cfg(feature = "sync")]
pub use ::rrcloud_core::meta::MetaKind;
pub use manager::{
    ConflictKeep, EvictionReport, PeerDevice, RecentlyDeleted, SyncError, SyncManager, SyncState,
    SyncStatus, ThumbVariant, VerifyReport,
};

// Re-exports so the integration tests (and future command layer) reach the
// chokepoint and settings types through one module path.
pub use crate::app::settings::{AppSettings, SyncSettings};
pub use crate::metadata::sidecar::ImageMetadata;
pub use crate::metadata::sidecar::{save_sidecar, update_sidecar};

// §3.5 guard-site test seams. The upstream placeholder check and the stub
// thumbnail-cache key live in the private `file_management` module; re-export
// them through `sync` (pure visibility, no behavior change) so the P2
// integration suite can assert placeholder/hash behavior. Both are plain
// functions (not `#[tauri::command]`s), so a `pub use` is safe.
pub use crate::library::{
    file_ops::is_cloud_placeholder, thumbnails::compute_thumbnail_cache_hash,
};

/// P3 smart-preview fidelity test seams (ARCHITECTURE.md §8-P3).
///
/// Thin `pub` re-exports of the otherwise module-private develop / resample /
/// GPU entry points, plus the proxy-mode dimension-reporting helpers, so the
/// `tests/proxy_fidelity.rs` integration suite can drive the real code paths
/// (RawDevelop LinearRaw branch, the clamp fix, linear-space Lanczos3, the
/// wgpu adapter probe, the original-vs-proxy as-shot readers) without
/// widening those modules' own visibility. No behavior: re-exports and the
/// `ProxyHandle` type only.
#[doc(hidden)]
pub mod proxy_support {
    pub use crate::decode::image_loader::{
        load_image_with_orientation, proxy_decode_settings, proxy_reported_dimensions,
    };
    pub use crate::decode::raw_processing::{develop_raw_image, read_as_shot};
    pub use crate::render::gpu::context::GpuContext;
    pub use crate::render::image_processing::{apply_srgb_to_linear, downscale_f32_image};
    pub use crate::sync::headless_gpu::{
        gpu_adapter_probe, init_gpu_context_headless, render_adjustments_headless,
    };
    pub use crate::sync::hooks::{ProxyHandle, proxy_as_shot};
    // The as-shot types the two readers above return (§4.4 parity test).
    pub use crate::decode::as_shot::AsShot;
    pub use crate::decode::baseline_exposure::RawExposure;
    pub use crate::decode::white_balance::WhiteBalance;
}

/// Test seam for the §3.5 `copy_files` guard: a plain-function wrapper over
/// the private `file_management::copy_files` command so the integration suite
/// can drive the real hydrate-then-copy guard. A direct `pub use` of the
/// command is avoided — `#[tauri::command]` also defines a same-named macro,
/// which a re-export would pull into this module. No behavior change.
#[doc(hidden)]
pub async fn copy_files_guarded(
    source_paths: Vec<String>,
    destination_folder: String,
    app_handle: tauri::AppHandle,
) -> Result<(), String> {
    crate::library::file_ops::copy_files(source_paths, destination_folder, app_handle).await
}

/// Test seams for integration tests that need private upstream commands:
/// plain-function wrappers over the `library` commands (`rename_separators`,
/// `show_in_finder_injection`) and the app settings type (`proxy_fidelity`).
/// A direct `pub use` of a `#[tauri::command]` is avoided because the attribute
/// also defines a same-named macro which a re-export would pull along. No
/// behavior change.
#[doc(hidden)]
pub mod test_seams {
    pub use crate::app::settings::AppSettings;

    pub fn rename_folder(
        path: String,
        new_name: String,
        app_handle: tauri::AppHandle,
    ) -> Result<(), String> {
        crate::library::folders::rename_folder(path, new_name, app_handle)
    }

    pub fn rename_files(
        paths: Vec<String>,
        name_template: String,
        app_handle: tauri::AppHandle,
    ) -> Result<Vec<String>, String> {
        crate::library::file_ops::rename_files(paths, name_template, app_handle)
    }

    pub fn show_in_finder(path: String) -> Result<(), String> {
        crate::library::file_ops::show_in_finder(path)
    }
}

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
