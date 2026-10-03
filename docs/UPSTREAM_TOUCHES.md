# Upstream files touched by RapidRawCloud

This is the rebase checklist: every hook into an upstream file is listed here with a one-line
rationale. Everything not listed is additive (new files/crates/modules only). Hooks are
unconditional one-line calls into always-compiled `sync::hooks::*` shims; the `sync` cargo
feature (default on) controls whether shim bodies do anything. `--no-default-features` must
build and behave functionally identical to upstream (CI-checked).

Status legend: [ ] planned (phase) · [x] landed

| File | Change | Status |
|---|---|---|
| `src-tauri/Cargo.toml` | optional path dep `crates/rrcloud-core` + `sync` feature (default on) + `dashmap` | [x] P1 |
| `src-tauri/src/lib.rs` | `pub mod sync;` + `#[cfg(sync)] pub use ::rrcloud_core;` + SyncManager start in `setup()` + `RunEvent::ExitRequested` exit flush (P1); `ensure_local` guards in `get_image_dimensions`, `generate_preview_for_path`, and the `merge_hdr` merge command (P2). (command registration remains for the P2 command-layer unit) | [x] P1/P2 |
| `src-tauri/src/app_state.rs` | `sync_manager` field + sidecar lock map (P1); P3: `proxy_scale: Mutex<Option<f32>>` (set on the §4.4 proxy-edit-mode load, `None` for a full-resolution original) + its `Mutex::new(None)` init in `lib.rs` `.manage(AppState { .. })` | [x] P1/P3 |
| `src-tauri/src/app_settings.rs` | `sync: SyncSettings` field (`#[serde(default)]`; credentials excluded) | [x] P1 |
| `src-tauri/src/exif_processing.rs` | `save_sidecar` chokepoint (per-path lock + bounded lock-map prune, 0-byte/corrupt quarantine + abort, atomic temp+rename with `fs::write`-matching mode + fsync for §7 perm-parity, churn-gated notify) + `update_sidecar` read-modify-write variant (load+mutate+write under the one per-path lock — closes the AI-tagging-vs-user-edit lost-update race, P1-U7 review); `load_sidecar` warn on parse failure + auto-heal routes through chokepoint; `merge_exif_from_source` factored for the RMW sites and uses pure `read_exif_data_from_bytes` (no persist) so it cannot re-enter the held lock (P1-U7 round-3 blocker); the EXIF-population caches (`read_exif_data`/`persist_exif_if_missing`/`write_rrexif_sidecar`) write via `update_sidecar` (RMW under the lock) rather than `save_sidecar` on an out-of-lock load (P1-U7 round-3 lost-update) | [x] P1 |
| `src-tauri/src/file_management.rs` | ~11 sidecar write sites → chokepoint; read-modify-write sites (rating/color-label/adjustment/auto/XMP-import/exif-field edit `update_exif_fields`) → `update_sidecar`; `create_virtual_copy` copy branch now also routes through the chokepoint (P1). P2: `is_cloud_placeholder` gains `\|\| sync::hooks::is_stub(path)` on all platforms (reverts to the upstream macOS-only `SF_DATALESS` check with sync off); `ensure_local` hydrate-then-copy/move guards in `copy_files` / `move_files` / `duplicate_file`; `get_cached_or_generate_thumbnail_image` stub skip; `ImageFile.sync_state` `#[serde(default)]` badge field. (import/derived/delete hooks; XMP-import hash gate remain later) | [x] P1/P2 |
| `src-tauri/src/tagging.rs` | 4 sidecar write sites, all read-modify-write → `update_sidecar` (RMW under the per-path lock): `start_background_indexing` + `modify_tags_for_path` (tag edits) and `clear_ai_tags` + `clear_all_tags` (AI-tag clears, via the shared `clear_tags_in_sidecar` helper) — so every tag/AI-tag writer loads+mutates+writes under the lock and cannot lose a concurrent edit's field (P1-U7 review) | [x] P1 |
| `src-tauri/src/image_loader.rs` | P2: `ensure_local` replaces the iCloud error branch for a sync stub (hydrate-in-place), keeping the upstream iCloud error for a non-stub placeholder / sync-off. P3: `load_image` routes a stub with a present smart preview through `load_image_from_proxy` (§4.4) — decode the proxy DNG via `load_base_image_from_bytes`, pin `(apply_ungamma=false, apply_calibration=true)` via `proxy_decode_settings`, report the ORIGINAL journal dims via `proxy_reported_dimensions`, store `proxy_scale`, and re-apply `remove_raw_artifacts_and_enhance` with `proxy_scale`-scaled amounts (E2). The proxy fn + call site are `#[cfg(feature = "sync")]`, so `--no-default-features` is untouched | [x] P2 (ensure_local) · [x] P3 (proxy) |
| `src-tauri/src/raw_processing.rs` | `clamp_limit` via `resolve_clamp_limit(fast_demosaic, is_linear_format, hl)` (ARCHITECTURE.md §4.1/E3) — fast demosaic is meaningless for the LinearRaw branch (which skips Demosaic), so a fast proxy/thumb decode keeps the >1.0 headroom. **The relaxation is `#[cfg(feature = "sync")]`-gated**: `--no-default-features` is byte-identical to upstream (`if fast_demosaic { 1.0 }`, format-independent) — the hard §7 parity guarantee, now covered by a `#[cfg(not(feature="sync"))]` regression test (`clamp_gate_tests`). Honest scope note (P3 review): in a `sync` build the relaxation also reaches a FOREIGN linear DNG decoded fast (the `linear_mode` setting's reason to exist), not proxies only; this is intentional and harmless (fast demosaic is a no-op for LinearRaw) and is NOT claimed to be proxy-exclusive | [x] P3 |
| `src-tauri/src/gpu_processing.rs` | P3: additive test seams for the §8-P3 GPU ΔE confirmation — `gpu_adapter_probe` (headless lavapipe adapter probe), `init_gpu_context_headless` (compute-only device/queue bring-up, no surface/AppState), `render_adjustments_headless` (full adjustment pipeline via `GpuProcessor::run`). Additive `pub fn`s only (not hooks); inert for `--no-default-features` (never called off the proxy path) | [x] P3 |
| `src-tauri/src/export_processing.rs` | `ensure_local` before the per-image `load_and_composite` in `export_images_impl` | [x] P2 |
| `src-tauri/src/denoising.rs` | `ensure_local` guard before the source load in `apply_denoising` + the `batch_denoise` loop | [x] P2 |
| `src-tauri/src/focus_stacking.rs` | `ensure_local` guard before each source load in `stitch_focus_stack` | [x] P2 |
| `src-tauri/src/panorama_stitching.rs` | `ensure_local` guard before each source load in `stitch_panorama` | [x] P2 |
| `src-tauri/gen/android/...` | manifest permissions, gradle deps, plugin registration | [ ] P5 |
| `src/hooks/useTauriListeners.ts` | `sync-*` listeners | [ ] P2 |
| `src/hooks/useAppNavigation.ts` | `sync_flush_path` hint at the three debouncedSave flush points | [ ] P1 |
| UI components | status badge, pin/evict menu, sync settings, conflict toast (mount-point one-liners) | [ ] P2/P6 |

Notes:
- **P4 headless worker touches NO upstream file (documented §6 refinement).** The worker is a
  `[[bin]]` *inside* `crates/rrcloud-core` (`src/bin/rrcloud-worker.rs` over `worker::run_cycle`),
  depending on `rrcloud-core` only — not `rapidraw_lib`/tauri/gtk/webkit. ARCHITECTURE.md §6 prose
  originally put the bin in the host app crate linking `rapidraw_lib` for color parity; that is
  unnecessary because `proxy.rs` lives in `rrcloud-core` and pins the same rawler rev the app
  resolves, so parity is already this crate's property. The refinement therefore *removes* a would-be
  upstream hook (no `src-tauri/Cargo.toml` bin target, no app-crate linkage, no gtk/webkit base
  image) rather than adding one — strictly additive to `rrcloud-core`. The only manifest change is
  additive and *inside* `crates/rrcloud-core/Cargo.toml` (the `[[bin]]` plus the `tokio`
  `rt-multi-thread`/`macros`/`time` runtime features the bin's `#[tokio::main]` needs, already in the
  test build); no upstream manifest is touched. See `rrcloud_core::worker` module docs and
  ARCHITECTURE.md §6. The worker is **pure orchestration** over existing `rrcloud-core` modules and
  reimplements no protocol logic: P4 review round 0 wired `worker::run_cycle` to the two engine
  entry points the GC worker must be the backstop for — `engine::reconcile_wholeness` (the §2.7/§6
  whole-item backstop, previously with no production caller) and the §2.3 gap-routed
  `manifest::merge` (`worker::catch_up`, so a manifest-only peer original is not re-adopted) — plus
  per-item proxy isolation and server-time stale-upload hygiene. Still additive to `rrcloud-core`.
- `tauri.conf.json` is deliberately untouched — thumbnail seeding hard-links into the existing
  `$APPCACHE/thumbnails` asset-protocol scope (ARCHITECTURE.md §3.5).
- P0 keeps `crates/rrcloud-core` standalone (own lockfile, no workspace membership) so the
  upstream manifest stays untouched until the first real consumer lands in P1.
- Branch model note: the session's push policy only permits the designated work branch, so
  `main` (= upstream `a82251f`) is cut by the repo owner when merging; this branch carries
  upstream history + additive commits + hook commits.
