//! Android platform integration (ARCHITECTURE.md §5): the JNI bridge the
//! `tauri-plugin-rrcloud` Kotlin side calls into from `WorkManager`
//! (`SyncCycleWorker`/`DcimScanWorker`), the user-initiated foreground-sync
//! path, and the `ContentObserver` DCIM watch — plus every piece of
//! decision logic those call sites need that is pure enough to unit-test
//! on the host, with no JNI/Android toolchain involved.
//!
//! Module split, by testability:
//!
//! - [`bridge`]: the actual `#[no_mangle] extern "system"` entry points.
//!   Compiled **only** under `target_os = "android"` (the `jni` crate is a
//!   `target.'cfg(target_os = "android")'` dependency, exactly like
//!   `src-tauri`'s own `android_integration.rs`), so it is unreachable —
//!   and untested — from `cargo test` on the host. It calls straight into
//!   the pure modules below for every decision it makes.
//! - [`bounded_cycle`]: the `budget_ms` deadline a bounded sync cycle
//!   checks **between** work units (upload one item, apply one journal
//!   page — never mid-item), per §5.1 point 3.
//! - [`result_code`]: the bridge's `jint` return code, and its exhaustive
//!   mapping to the `Result.success()` / `.retry()` / `.failure()` the
//!   Kotlin `Worker.doWork()` returns.
//! - [`dcim_dedupe`]: the §5.2 `dcim_seen` skip/rehash/new decision —
//!   reuses the existing `dcim_seen` table's row shape (fetched per path
//!   via [`crate::state::SyncDb::dcim_seen_for_path`], not the
//!   exact-`(size, mtime)`-keyed [`crate::state::SyncDb::dcim_seen`]),
//!   does not touch redb itself.
//! - [`scan_window`]: the §5.2 API 24-29 `DATE_ADDED` 48 h overlap-window
//!   arithmetic (the API 30+ path uses `MediaStore.getGeneration()`
//!   instead, which has no host-side arithmetic to pull out).
//! - [`platform_init`]: the process-wide `ndk_context`/
//!   `rustls_platform_verifier` init guards shared by [`bridge`]'s JNI
//!   entry points **and** `src-tauri`'s own `android_integration.rs` —
//!   see that module's doc for the double-init crash this exists to
//!   prevent. The guard mechanic itself has no `target_os` gate, so its
//!   regression tests run on the host.

pub mod bounded_cycle;
pub mod bridge;
pub mod dcim_dedupe;
pub mod platform_init;
pub mod result_code;
pub mod scan_window;

#[cfg(target_os = "android")]
pub use platform_init::{ensure_ndk_context_initialized, ensure_rustls_platform_verifier_initialized};
