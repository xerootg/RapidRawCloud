# Upstream files touched by RapidRawCloud

This is the rebase checklist: every hook into an upstream file is listed here with a one-line
rationale. Everything not listed is additive (new files/crates/modules only). Hooks are
unconditional one-line calls into always-compiled `sync::hooks::*` shims; the `sync` cargo
feature (default on) controls whether shim bodies do anything. `--no-default-features` must
build and behave functionally identical to upstream (CI-checked).

Status legend: [ ] planned (phase) · [x] landed

| File | Change | Status |
|---|---|---|
| `src-tauri/Cargo.toml` | workspace member `crates/rrcloud-core`, deps, `sync` feature | [ ] P1 |
| `src-tauri/src/lib.rs` | `mod sync;` + command registration + SyncManager start + exit flush + `ensure_local` guards in merge commands | [ ] P1/P2 |
| `src-tauri/src/app_state.rs` | `sync_manager` field + sidecar lock map | [ ] P1 |
| `src-tauri/src/app_settings.rs` | `sync: SyncSettings` field | [ ] P1 |
| `src-tauri/src/exif_processing.rs` | `save_sidecar` chokepoint (corruption + remote-head guards); `load_sidecar` warn on parse failure | [ ] P1 |
| `src-tauri/src/file_management.rs` | ~12 write sites → chokepoint; `is_cloud_placeholder` `\|\| sync::is_stub`; `ensure_local` guards; import/derived/delete hooks; XMP-import hash gate; `ImageFile.sync_state` | [ ] P1/P2 |
| `src-tauri/src/tagging.rs` | 4 write sites → chokepoint | [ ] P1 |
| `src-tauri/src/image_loader.rs` | proxy branch + `ensure_local` at the iCloud error branch; pin `(apply_ungamma, apply_calibration)` for tagged proxies; `proxy_scale`-scaled enhance amounts | [ ] P2/P3 |
| `src-tauri/src/raw_processing.rs` | `clamp_limit`: `fast_demosaic && !is_linear_format` (ARCHITECTURE.md §4.1/E3) | [ ] P3 |
| `src-tauri/src/export_processing.rs` | `ensure_local` before per-image load | [ ] P2 |
| `src-tauri/gen/android/...` | manifest permissions, gradle deps, plugin registration | [ ] P5 |
| `src/hooks/useTauriListeners.ts` | `sync-*` listeners | [ ] P2 |
| `src/hooks/useAppNavigation.ts` | `sync_flush_path` hint at the three debouncedSave flush points | [ ] P1 |
| UI components | status badge, pin/evict menu, sync settings, conflict toast (mount-point one-liners) | [ ] P2/P6 |

Notes:
- `tauri.conf.json` is deliberately untouched — thumbnail seeding hard-links into the existing
  `$APPCACHE/thumbnails` asset-protocol scope (ARCHITECTURE.md §3.5).
- P0 keeps `crates/rrcloud-core` standalone (own lockfile, no workspace membership) so the
  upstream manifest stays untouched until the first real consumer lands in P1.
- Branch model note: the session's push policy only permits the designated work branch, so
  `main` (= upstream `a82251f`) is cut by the repo owner when merging; this branch carries
  upstream history + additive commits + hook commits.
