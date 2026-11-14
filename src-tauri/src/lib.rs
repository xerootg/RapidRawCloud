#[cfg(not(all(target_os = "windows", target_arch = "aarch64")))]
use mimalloc::MiMalloc;

#[cfg(not(all(target_os = "windows", target_arch = "aarch64")))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

macro_rules! cli_println {
    ($($arg:tt)*) => {{
        use std::io::Write as _;
        let _ = writeln!(std::io::stdout(), $($arg)*);
    }};
}

macro_rules! cli_eprintln {
    ($($arg:tt)*) => {{
        use std::io::Write as _;
        let _ = writeln!(std::io::stderr(), $($arg)*);
    }};
}

mod adjustments;
mod ai;
mod app;
mod decode;
mod effects;
mod export;
mod integrations;
mod library;
mod masks;
mod merge;
mod metadata;
mod platform;
mod render;
mod retouch;
pub mod sync;

// Re-export the pure-core sync crate so the integration tests reach its S3
// client / engine types through `rapidraw_lib::rrcloud_core` without a
// separate dev-dependency. Only present when the `sync` feature is on, so
// a `--no-default-features` build links no rrcloud-core (ARCHITECTURE.md §7).
#[cfg(feature = "sync")]
pub use ::rrcloud_core;

use std::collections::HashMap;
#[cfg(target_os = "macos")]
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, AtomicUsize};

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tauri::Manager;
use tokio::sync::Mutex as TokioMutex;

#[cfg(not(any(target_os = "android", target_os = "ios")))]
use crate::app::launch_request::emit_launch_request;
use crate::app::launch_request::{
    LaunchRequest, parse_launch_args, process_exit_code, resolve_exit_code,
    resolve_gui_launch_paths, set_process_exit_code,
};
use crate::app::logging::setup_logging;
use crate::app::settings::{AppSettings, load_settings, save_settings};
#[cfg(not(target_os = "android"))]
use crate::app::state::WindowState;
use crate::app::state::{
    AppState, MetadataManager, ThumbnailManager, ThumbnailProgressTracker, ViewerSession,
    ViewerState,
};
#[cfg(not(target_os = "android"))]
use crate::platform::main_window::{
    MAIN_WINDOW_MIN_SIZE, MAIN_WINDOW_STATE_FILE, available_monitor_bounds, read_window_state,
    saved_window_state_is_usable, write_window_state,
};
use crate::platform::window_customizer::PinchZoomDisablePlugin;
use crate::render::cache_utils::DecodedImageCache;
use crate::render::gpu::context::get_or_init_gpu_context;
use crate::render::preview_worker::{start_analytics_worker, start_preview_worker};

/// The current proxy-edit-mode scale (ARCHITECTURE.md §4.4). `Some(s)` iff the
/// loaded `original_image` is a smart-preview proxy decoded at scale `s =
/// proxy_long_edge / orig_long_edge`; `None`/normal load returns `1.0`. Read by
/// the render paths so original-pixel-space geometry maps onto the proxy base.
pub fn current_proxy_scale(state: &tauri::State<AppState>) -> f32 {
    state
        .proxy_scale
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .unwrap_or(1.0)
}

/// §4.4: the scale that maps ORIGINAL-pixel-space geometry (crop offset, mask
/// coordinates, AI-patch offsets) onto the rendered output image.
///
/// In normal mode the render base *is* the original, so this is just
/// `base_to_output` (the base→preview downscale). In proxy edit mode the render
/// base is the smart-preview proxy — already downscaled from the original by
/// `proxy_scale` — so original→output = `proxy_scale · (proxy→output)`. This is
/// the editor-preview analogue of `generate_thumbnail_data`'s
/// `total_scale = gpu_scale · raw_scale_factor` (`library/thumbnails.rs`), where
/// `raw_scale_factor` carries the decode downscale; here it carries
/// `proxy_scale`. Without this factor every mask/crop was displaced by
/// `orig/proxy` (~2.3×) when editing an evicted photo (P3 review blocker).
pub fn effective_geometry_scale(base_to_output: f32, proxy_scale: f32) -> f32 {
    base_to_output * proxy_scale
}

/// §4.4: folds `proxy_scale` into a patched-base cache key so a proxy render
/// and an original render of the same path/adjustments never share a cached
/// stage. Identity at `1.0`, so normal-mode keys (and `cached_patched_image`
/// hits) are exactly upstream's.
pub fn proxy_cache_key(key: u64, proxy_scale: f32) -> u64 {
    use std::hash::{DefaultHasher, Hash, Hasher};
    if (proxy_scale - 1.0).abs() <= f32::EPSILON {
        return key;
    }
    let mut hasher = DefaultHasher::new();
    (key, proxy_scale.to_bits()).hash(&mut hasher);
    hasher.finish()
}

#[cfg(target_os = "macos")]
static SHUTTING_DOWN: AtomicBool = AtomicBool::new(false);

#[cfg(target_os = "macos")]
extern "C" fn mark_shutting_down() {
    SHUTTING_DOWN.store(true, Ordering::SeqCst);
}

#[cfg(target_os = "macos")]
extern "C" fn force_exit(_signal: libc::c_int) {
    if SHUTTING_DOWN.load(Ordering::SeqCst) {
        unsafe {
            libc::_exit(process_exit_code());
        }
    }
}

#[cfg(target_os = "macos")]
pub fn register_exit_handler() {
    static ATEXIT: std::sync::Once = std::sync::Once::new();
    unsafe {
        libc::signal(libc::SIGABRT, force_exit as *const () as libc::sighandler_t);
    }
    ATEXIT.call_once(|| unsafe {
        libc::atexit(mark_shutting_down);
    });
}

#[cfg(not(target_os = "macos"))]
pub fn register_exit_handler() {}

#[cfg(target_os = "linux")]
fn force_c_numeric_locale() {
    unsafe {
        if let Some(all) = std::env::var_os("LC_ALL").filter(|v| !v.is_empty()) {
            std::env::set_var("LANG", all);
            std::env::remove_var("LC_ALL");
        }
        std::env::set_var("LC_NUMERIC", "C");
    }
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    #[cfg(target_os = "linux")]
    force_c_numeric_locale();

    let _ = rayon::ThreadPoolBuilder::new()
        .stack_size(8 * 1024 * 1024)
        .build_global();

    let args: Vec<String> = std::env::args().skip(1).collect();
    let launch_req = resolve_gui_launch_paths(
        parse_launch_args(&args),
        &std::env::current_dir().unwrap_or_default(),
    );
    if let LaunchRequest::InvalidHeadless(error) = &launch_req {
        eprintln!("Headless export failed: {}", error);
        std::process::exit(2);
    }
    let is_headless = matches!(launch_req, LaunchRequest::HeadlessExport(_));

    #[cfg_attr(any(target_os = "android", target_os = "ios"), allow(unused_mut))]
    let mut builder = tauri::Builder::default();

    #[cfg(target_os = "linux")]
    {
        if !is_headless {
            builder = builder.plugin(tauri_plugin_wayland_nvidia_quirk::init());
        }
    }

    #[cfg(not(any(target_os = "android", target_os = "ios")))]
    {
        if !is_headless {
            builder = builder.plugin(tauri_plugin_single_instance::init(|app, argv, cwd| {
                log::info!(
                    "New instance launched with args: {:?}. Focusing main window.",
                    argv
                );
                if let Some(window) = app.get_webview_window("main") {
                    if let Err(e) = window.unminimize() {
                        log::error!("Failed to unminimize window: {}", e);
                    }
                    if let Err(e) = window.set_focus() {
                        log::error!("Failed to set focus on window: {}", e);
                    }
                }

                let forwarded_args = argv.get(1..).unwrap_or(&[]);
                emit_launch_request(
                    app,
                    resolve_gui_launch_paths(
                        parse_launch_args(forwarded_args),
                        std::path::Path::new(&cwd),
                    ),
                );
            }));
        }
    }

    #[cfg(feature = "sync")]
    {
        // The P5 Android platform plugin (ARCHITECTURE.md §5): registers
        // the Kotlin `RrcloudPlugin` class on Android (a no-op `Rrcloud`
        // handle on desktop), which is what wires the `RrcloudBridge` JNI
        // entry points' native-library load into the app's lifecycle.
        // Feature-gated to mirror `SyncManager`'s own `sync`-only wiring
        // (`crate::sync::manager::start_in_setup`, below) — a
        // `--no-default-features` build links neither.
        builder = builder.plugin(tauri_plugin_rrcloud::init());
        // "Pair with cloud" (docs/CLOUD_SETUP.md §6): receives the
        // rapidraw://auth-callback OIDC redirect. The Android intent-filter
        // lives on MainActivity (gen/android manifest); this plugin surfaces
        // the intent to Rust, and the setup hook below forwards it to the
        // webview, which calls `sync_pair_complete`.
        builder = builder.plugin(tauri_plugin_deep_link::init());
    }

    // Upstream's own cloud (tauri-plugin-clerk) is desktop-only and
    // independent of the fork's sync engine; both can register side by side.
    #[cfg(any(target_os = "windows", target_os = "macos", target_os = "linux"))]
    {
        builder = builder
            .plugin(tauri_plugin_store::Builder::new().build())
            .plugin(
                tauri_plugin_clerk::ClerkPluginBuilder::new()
                    .publishable_key("pk_live_Y2xlcmsuZ2V0cmFwaWRyYXcuY29tJA".to_string())
                    .with_tauri_store()
                    .build(),
            );
    }

    builder
        .plugin(tauri_plugin_os::init())
        .plugin(tauri_plugin_fs::init())
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_process::init())
        .plugin(tauri_plugin_shell::init())
        .plugin(tauri_plugin_http::init())
        .plugin(PinchZoomDisablePlugin)
        .on_window_event(|window, event| match event {
            tauri::WindowEvent::Resized(size) => {
                render::gpu::display::resize_display(window, *size);
                if window.label() == platform::viewer_window::VIEWER_LABEL {
                    platform::viewer_window::handle_viewer_resized(window, *size);
                }
            }
            tauri::WindowEvent::CloseRequested { api, .. } => {
                platform::viewer_window::handle_close_requested(window, api)
            }
            tauri::WindowEvent::Destroyed if window.label() == platform::viewer_window::VIEWER_LABEL => {
                platform::viewer_window::handle_viewer_destroyed(window.app_handle())
            }
            _ => {}
        })
        .setup(move |app| {
            // "Pair with cloud" deep-link return (docs/CLOUD_SETUP.md §6):
            // the OIDC redirect rapidraw://auth-callback lands here (plugin
            // callback runs on both the cold-start intent and onNewIntent).
            // Forward the URL to the webview; the Sync settings screen calls
            // `sync_pair_complete` with it. Forward-only — no OAuth logic on
            // this side of the boundary.
            #[cfg(feature = "sync")]
            {
                use tauri::Emitter;
                use tauri_plugin_deep_link::DeepLinkExt;
                let handle = app.handle().clone();
                app.deep_link().on_open_url(move |event| {
                    for url in event.urls() {
                        let url = url.to_string();
                        if url.starts_with(crate::sync::pairing::REDIRECT_URI) {
                            let _ = handle.emit("rrcloud-pair-callback", url);
                        }
                    }
                });
            }

            #[cfg(feature = "tethering")]
            {
                std::thread::spawn(|| {
                    match gphoto2::Context::new() {
                        Ok(context) => {
                            log::info!("gphoto2 context initialized successfully.");
                            match gphoto2::Camera::autodetect(&context) {
                                Ok(cameras) => {
                                    log::info!("Found {} attached camera(s)", cameras.len());
                                }
                                Err(e) => log::warn!("Failed to autodetect cameras: {}", e),
                            }
                        }
                        Err(e) => log::error!("Failed to initialize gphoto2 context: {}", e),
                    }
                });
            }

            #[cfg(any(windows, target_os = "linux", target_os = "macos"))]
            let state = app.state::<AppState>();

            #[cfg(any(windows, target_os = "linux", target_os = "macos"))]
            {
                match launch_req.clone() {
                    LaunchRequest::EditSession(session) => {
                        log::info!("Initial launch with external edit session for: {}", session.source);
                        *state.pending_edit_session.lock().unwrap() = Some(session);
                    }
                    LaunchRequest::OpenFile(path) => {
                        log::info!("Initial open: Storing path {} for later.", path);
                        *state.initial_file_path.lock().unwrap() = Some(path);
                    }
                    _ => {}
                }
            }

            let app_handle = app.handle().clone();
            crate::render::gpu::context::register_app_handle(&app_handle);
            adjustments::develop_defaults::refresh(&app_handle);

            if !is_headless {
                crate::integrations::immich::start(&app_handle);
            }

            if let Ok(cache_dir) = app_handle.path().app_cache_dir() {
                crate::ai::denoise::initialize_cache_dir(&cache_dir);
                crate::ai::upscale::initialize_cache_dir(&cache_dir);
                crate::effects::maps::initialize_cache_dir(cache_dir.clone());
                crate::metadata::exif_cache::initialize_cache_dir(cache_dir.clone());
                crate::app::storage::initialize_thumbnail_cache(cache_dir);
            }

            {
                let disks_app_handle = app_handle.clone();
                std::thread::spawn(move || {
                    let disks = sysinfo::Disks::new_with_refreshed_list();
                    let state = disks_app_handle.state::<AppState>();
                    *state.disks_cache.lock().unwrap() = Some(disks);
                });
            }

            let config_dir = app_handle.path().app_config_dir().expect("Failed to get config dir");
            let crash_flag_path = config_dir.join(".gpu_init_crash_flag");

            {
                let state = app.state::<AppState>();
                *state.gpu_crash_flag_path.lock().unwrap() = Some(crash_flag_path.clone());
            }

            let mut settings: AppSettings = load_settings(app_handle.clone()).unwrap_or_default();

            {
                let state = app.state::<AppState>();
                let cache_size = settings.image_cache_size.unwrap_or(5) as usize;
                state.decoded_image_cache.lock().unwrap().set_capacity(cache_size);
            }
            crate::app::storage::apply_cache_limits(&settings);

            if crash_flag_path.exists() {
                log::warn!("GPU Driver crash detected on last run! Falling back to OpenGL backend.");
                settings.processing_backend = Some("gl".to_string());
                let _ = save_settings(settings.clone(), app_handle.clone());
                let _ = std::fs::remove_file(&crash_flag_path);
            }

            adjustments::lens_correction::init_lensfun_db(&app_handle);

            unsafe {
                if let Some(backend) = &settings.processing_backend
                    && backend != "auto" {
                        std::env::set_var("WGPU_BACKEND", backend);
                    }

                #[cfg(target_os = "linux")]
                {
                    if settings.linux_gpu_optimization.unwrap_or(false) {
                        std::env::set_var("WEBKIT_DISABLE_DMABUF_RENDERER", "1");
                        std::env::set_var("WEBKIT_DISABLE_COMPOSITING_MODE", "1");
                        std::env::set_var("NODEVICE_SELECT", "1");
                    }
                }

                #[cfg(not(target_os = "android"))]
                {
                    let resource_path = app_handle
                        .path()
                        .resolve("resources", tauri::path::BaseDirectory::Resource)
                        .expect("failed to resolve resource directory");

                    let ort_library_name = {
                        #[cfg(target_os = "windows")]
                        { "onnxruntime.dll" }
                        #[cfg(target_os = "linux")]
                        { "libonnxruntime.so" }
                        #[cfg(target_os = "macos")]
                        { "libonnxruntime.dylib" }
                        #[cfg(not(any(windows, target_os = "linux", target_os = "macos")))]
                        { "libonnxruntime.so" }
                    };
                    let ort_library_path = resource_path.join(ort_library_name);
                    std::env::set_var("ORT_DYLIB_PATH", &ort_library_path);
                    cli_println!("Set ORT_DYLIB_PATH to: {}", ort_library_path.display());
                }
            }

            setup_logging(&app_handle);

            if let Some(backend) = &settings.processing_backend
                && backend != "auto" {
                    log::info!("Applied processing backend setting: {}", backend);
                }

            #[cfg(target_os = "linux")]
            if settings.linux_gpu_optimization.unwrap_or(false) {
                log::info!("Applied Linux Compatibility Mode (forced software compositing).");
            } else {
                log::info!(
                    "Wayland Nvidia quirk status: {:?}",
                    tauri_plugin_wayland_nvidia_quirk::status()
                );
            }

            jxl_oxide::integration::register_image_decoding_hook();

            match launch_req {
                LaunchRequest::HeadlessExport(session) => {
                    let app_handle_clone = app_handle.clone();
                    tauri::async_runtime::spawn(async move {
                        match crate::export::run_headless_export(session, app_handle_clone.clone()).await {
                            Ok(_) => {
                                cli_println!("Headless export completed successfully.");
                                app_handle_clone.exit(0);
                            }
                            Err(e) => {
                                cli_eprintln!("Headless export failed: {}", e);
                                set_process_exit_code(1);
                                app_handle_clone.exit(1);
                            }
                        }
                    });

                    return Ok(());
                }
                LaunchRequest::InvalidHeadless(_) => unreachable!("invalid headless arguments exit before app setup"),
                _ => {}
            }

            start_preview_worker(app_handle.clone());
            start_analytics_worker(app_handle.clone());
            decode::image_preload::start_preload_worker(app_handle.clone());
            library::thumbnails::start_thumbnail_workers(app_handle.clone());
            library::listing::start_metadata_workers(app_handle.clone());
            {
                let state = app.state::<AppState>();
                crate::sync::manager::start_in_setup(&app_handle, &state.sync_manager);
                // NOTE: the in-process engine is made live for the session from
                // saved settings via the `sync_ensure_configured` command, which
                // the webview calls on startup (`useSyncDriver`). It cannot run
                // here: on Android the credential-store JNI needs the ndk
                // context, which is not initialized this early in `setup()`.
            }
            ai::jobs::start_worker(app_handle.clone());

            let window_cfg = app.config().app.windows.first().unwrap().clone();
            let decorations = settings.decorations.unwrap_or(window_cfg.decorations);
            #[cfg(target_os = "android")]
            let _ = decorations;

            let main_window_cfg = app
                .config()
                .app
                .windows
                .iter()
                .find(|w| w.label == "main")
                .expect("Main window config not found")
                .clone();

            #[cfg_attr(target_os = "android", allow(unused_mut))]
            let mut window_builder =
                tauri::WebviewWindowBuilder::from_config(app.handle(), &main_window_cfg)
                    .unwrap();

            #[cfg(not(target_os = "android"))]
            {
                window_builder = window_builder.decorations(decorations).visible(false);
            }

            let window = window_builder.build().expect("Failed to build window");

            #[cfg(not(target_os = "android"))]
            app::settings::apply_saved_ui_scale(&window, &settings);

            #[cfg(target_os = "android")]
            platform::android::initialize_android(&window);

            #[cfg(not(target_os = "android"))]
            {
                let app_state = app.state::<AppState>();
                if let Err(error) = get_or_init_gpu_context(&app_state, app.handle()) {
                    log::warn!(
                        "GPU pre-initialization failed (editing and thumbnails may be degraded): {}",
                        error
                    );
                }

                let scale = window.scale_factor().unwrap_or(1.0);
                let default_w = (1280.0 * scale) as u32;
                let default_h = (720.0 * scale) as u32;

                let mut initial_normal_rect = (default_w, default_h, 100i32, 100i32);
                let mut state_loaded = false;

                if let Some(state) = read_window_state(app.handle(), MAIN_WINDOW_STATE_FILE) {
                    let monitor_bounds = available_monitor_bounds(&window);
                    let monitor_size = window.current_monitor().ok().flatten().map(|m| *m.size());

                    let is_maximized_size = monitor_size.map(|m| {
                        state.width >= m.width.saturating_sub(60) && state.height >= m.height.saturating_sub(60)
                    }).unwrap_or(false);

                    if saved_window_state_is_usable(&state, &monitor_bounds, scale, MAIN_WINDOW_MIN_SIZE) && !is_maximized_size {
                        initial_normal_rect = (state.width, state.height, state.x, state.y);
                        state_loaded = true;

                        let _ = window.set_size(tauri::Size::Physical(
                            tauri::PhysicalSize::new(state.width, state.height),
                        ));
                        let _ = window.set_position(tauri::Position::Physical(
                            tauri::PhysicalPosition::new(state.x, state.y),
                        ));
                    }
                }

                if !state_loaded {
                    let (mut w, mut h) = (1280.0_f64, 720.0_f64);
                    if let Some(m) = window.current_monitor().ok().flatten() {
                        let ms = m.size().to_logical::<f64>(m.scale_factor());
                        w = w.min(ms.width - 80.0).max(MAIN_WINDOW_MIN_SIZE.0);
                        h = h.min(ms.height - 120.0).max(MAIN_WINDOW_MIN_SIZE.1);
                    }

                    let _ = window.set_size(tauri::Size::Logical(tauri::LogicalSize::new(w, h)));
                    let _ = window.center();

                    initial_normal_rect.0 = (w * scale) as u32;
                    initial_normal_rect.1 = (h * scale) as u32;
                    if let Ok(pos) = window.outer_position() {
                        initial_normal_rect.2 = pos.x;
                        initial_normal_rect.3 = pos.y;
                    }
                }

                let window_failsafe = window.clone();
                tauri::async_runtime::spawn(async move {
                    tokio::time::sleep(std::time::Duration::from_secs(4)).await;
                    if let Ok(false) = window_failsafe.is_visible() {
                        log::warn!(
                            "Frontend failed to report ready within timeout. Forcing window visibility."
                        );
                        let _ = window_failsafe.show();
                        let _ = window_failsafe.set_focus();
                    }
                });

                let pending_window_state = Arc::new(Mutex::new(None::<WindowState>));
                let pending_state_for_saver = Arc::clone(&pending_window_state);
                let app_handle_for_saver = app.handle().clone();

                tauri::async_runtime::spawn(async move {
                    loop {
                        tokio::time::sleep(Duration::from_millis(500)).await;

                        let state_to_save = {
                            let mut lock = pending_state_for_saver.lock().unwrap();
                            lock.take()
                        };

                        if let Some(state) = state_to_save {
                            write_window_state(&app_handle_for_saver, MAIN_WINDOW_STATE_FILE, &state);
                        }
                    }
                });

                let window_for_handler = window.clone();
                let pending_state_for_handler = Arc::clone(&pending_window_state);
                let last_normal_rect = Arc::new(Mutex::new(initial_normal_rect));
                let last_normal_rect_handler = Arc::clone(&last_normal_rect);

                window.on_window_event(move |event| match event {
                    tauri::WindowEvent::Resized(_) | tauri::WindowEvent::Moved(_) => {
                        #[cfg(any(windows, target_os = "linux"))]
                        let maximized = window_for_handler.is_maximized().unwrap_or(false);
                        #[cfg(not(any(windows, target_os = "linux")))]
                        let maximized = false;

                        #[cfg(any(windows, target_os = "linux"))]
                        let fullscreen = window_for_handler.is_fullscreen().unwrap_or(false);
                        #[cfg(not(any(windows, target_os = "linux")))]
                        let fullscreen = false;

                        if window_for_handler.is_minimized().unwrap_or(false) {
                            return;
                        }

                        let mut normal = last_normal_rect_handler.lock().unwrap();

                        if !maximized && !fullscreen {
                            let (curr_w, curr_h) = window_for_handler
                                .outer_size()
                                .map(|s| (s.width, s.height))
                                .unwrap_or((0, 0));

                            let event_scale = window_for_handler.scale_factor().unwrap_or(1.0);
                            let min_w = (MAIN_WINDOW_MIN_SIZE.0 * event_scale) as u32;
                            let min_h = (MAIN_WINDOW_MIN_SIZE.1 * event_scale) as u32;

                            let is_maximize_in_progress = window_for_handler
                                .current_monitor()
                                .ok()
                                .flatten()
                                .map(|m| {
                                    let ms = m.size();
                                    curr_w >= ms.width.saturating_sub(20) && curr_h >= ms.height.saturating_sub(20)
                                })
                                .unwrap_or(false);

                            if !is_maximize_in_progress && curr_w >= min_w && curr_h >= min_h {
                                normal.0 = curr_w;
                                normal.1 = curr_h;

                                if let Ok(position) = window_for_handler.outer_position() {
                                    normal.2 = position.x;
                                    normal.3 = position.y;
                                }
                            }
                        }

                        let state = WindowState {
                            width: normal.0,
                            height: normal.1,
                            x: normal.2,
                            y: normal.3,
                            maximized,
                            fullscreen,
                        };

                        *pending_state_for_handler.lock().unwrap() = Some(state);
                    }
                    _ => {}
                });
            }

            Ok(())
        })
        .manage(AppState {
            window_setup_complete: AtomicBool::new(false),
            gpu_crash_flag_path: Mutex::new(None),
            original_image: Mutex::new(None),
            proxy_scale: Mutex::new(None),
            cached_preview: Mutex::new(None),
            viewer_cached_preview: Mutex::new(None),
            gpu_context: Mutex::new(None),
            gpu_image_cache: Mutex::new(None),
            viewer_gpu_image_cache: Mutex::new(None),
            gpu_processor: Mutex::new(None),
            ai_state: Mutex::new(None),
            ai_init_lock: TokioMutex::new(()),
            active_ai_tasks: Mutex::new(HashMap::new()),
            export_task_token: Arc::new(Mutex::new(None)),
            hdr_result: Arc::new(Mutex::new(None)),
            panorama_result: Arc::new(Mutex::new(None)),
            focus_stack_result: Arc::new(Mutex::new(None)),
            indexing_task_handle: Mutex::new(None),
            lut_cache: Mutex::new(HashMap::new()),
            initial_file_path: Mutex::new(None),
            pending_edit_session: Mutex::new(None),
            thumbnail_cancellation_token: Arc::new(AtomicBool::new(false)),
            thumbnail_progress: Mutex::new(ThumbnailProgressTracker { total: 0, completed: 0 }),
            preview_worker_tx: Mutex::new(None),
            analytics_worker_tx: Mutex::new(None),
            analytics_channel: Mutex::new(None),
            mask_cache: Mutex::new(HashMap::new()),
            patch_cache: Mutex::new(HashMap::new()),
            uncropped_source_cache: Mutex::new(HashMap::new()),
            thumbnail_source_cache: Mutex::new(HashMap::new()),
            load_image_generation: Arc::new(AtomicUsize::new(0)),
            full_warped_cache: Mutex::new(None),
            full_warped_build_lock: Mutex::new(()),
            wgpu_presented_generation: AtomicUsize::new(usize::MAX),
            patched_cache: Mutex::new(None),
            cleanup_source_cache: Mutex::new(None),
            working_cache: Mutex::new(None),
            decoded_image_cache: Mutex::new(DecodedImageCache::new(5)),
            decode_flights: Default::default(),
            image_preloader: Default::default(),
            thumbnail_manager: ThumbnailManager::new(),
            memory_budget: crate::app::memory_budget::MemoryBudget::from_total_ram(0.6),
            metadata_manager: MetadataManager::new(),
            disks_cache: Mutex::new(None),
            disks_cache_refreshing: AtomicBool::new(false),
            camera_session: Mutex::new(integrations::tethering::CameraSession::new()),
            sync_manager: crate::sync::SyncManager::new_inert(),
            viewer: Mutex::new(ViewerState {
                background: render::gpu::display::DisplayTransform::default().bg_secondary,
                session: ViewerSession::default(),
            }),
        })
        .invoke_handler(tauri::generate_handler![
            render::commands::apply_adjustments,
            render::commands::subscribe_analytics,
            render::commands::generate_preview_for_path,
            library::presets::generate_preset_preview,
            render::commands::generate_uncropped_preview,
            app::logging::get_log_file_path,
            app::logging::frontend_log,
            merge::save_collage,
            merge::hdr::merge_hdr,
            merge::hdr::save_hdr,
            adjustments::lut_processing::load_and_parse_lut,
            adjustments::lut_processing::list_luts,
            adjustments::lut_processing::import_luts,
            adjustments::lut_processing::remove_lut,
            adjustments::lut_processing::generate_lut_previews,
            library::presets::fetch_community_presets,
            library::presets::generate_all_community_previews,
            library::file_ops::save_temp_file,
            render::commands::get_image_dimensions,
            platform::main_window::frontend_ready,
            library::thumbnails::cancel_thumbnail_generation,
            render::commands::update_wgpu_transform,
            render::commands::release_original_preview,
            platform::viewer_window::open_viewer_window,
            platform::viewer_window::close_viewer_window,
            platform::viewer_window::set_viewer_view,
            platform::viewer_window::get_viewer_info,
            platform::viewer_window::get_viewer_frame,
            render::commands::sample_hsl_mixer_area,
            platform::android::resolve_android_content_uri_name,
            render::cache_utils::clear_session_caches,
            render::cache_utils::clear_image_caches,
            app::settings::load_settings,
            app::settings::save_settings,
            app::settings::set_ui_scale,
            app::settings::is_tethering_supported,
            app::settings::is_wgpu_renderer_supported,
            ai::commands::generate_ai_subject_mask,
            ai::commands::precompute_ai_subject_mask,
            ai::commands::generate_ai_foreground_mask,
            ai::commands::generate_ai_sky_mask,
            ai::commands::generate_ai_text_mask,
            ai::commands::generate_ai_face_mask,
            ai::commands::generate_ai_depth_mask,
            ai::commands::check_ai_connector_status,
            ai::commands::test_ai_connector_connection,
            ai::commands::generate_full_image_depth_map,
            ai::commands::generate_relight_normal_map,
            ai::commands::get_depth_preview,
            ai::denoise::clear_ai_denoise_cache,
            ai::upscale::clear_ai_upscale_cache,
            ai::jobs::get_ai_jobs_status,
            ai::jobs::get_pending_ai_paths,
            ai::jobs::pause_ai_jobs,
            ai::jobs::resume_ai_jobs,
            ai::jobs::discard_ai_jobs,
            ai::jobs::queue_ai_jobs,
            ai::jobs::retry_failed_ai_jobs,
            ai::commands::cancel_ai_task,
            retouch::inpainting::invoke_generative_replace_with_mask_def,
            retouch::inpainting::generate_manual_cleanup_patch,
            retouch::inpainting::generate_liquify_patch,
            retouch::inpainting::generate_retouch_patch,
            merge::focus_stacking::stitch_focus_stack,
            merge::focus_stacking::save_focus_stack,
            decode::image_loader::load_image,
            decode::image_loader::is_image_cached,
            decode::image_preload::preload_images,
            merge::panorama::stitch_panorama,
            merge::panorama::save_panorama,
            export::export_images,
            export::tasks::cancel_export,
            export::size_estimate::estimate_export_sizes,
            integrations::edit_in::edit_in_app,
            integrations::edit_in::cancel_edit_in_app,
            adjustments::auto_adjust::calculate_auto_adjustments,
            adjustments::white_balance_picker::sample_white_balance,
            adjustments::auto_straighten::calculate_auto_straighten,
            library::sidecars::apply_auto_straighten_to_paths,
            masks::generation::generate_mask_overlay,
            library::listing::update_exif_fields,
            library::listing::get_supported_file_types,
            library::listing::read_exif_for_paths,
            library::listing::list_images_in_dir,
            library::listing::list_images_recursive,
            library::folders::get_folder_tree,
            library::folders::get_folder_children,
            library::folders::get_pinned_folder_trees,
            library::thumbnails::update_thumbnail_queue,
            library::folders::create_folder,
            library::folders::delete_folder,
            library::file_ops::copy_files,
            library::file_ops::move_files,
            library::folders::rename_folder,
            library::file_ops::rename_files,
            library::file_ops::duplicate_file,
            library::file_ops::show_in_finder,
            library::file_ops::delete_files_from_disk,
            library::file_ops::delete_files_with_associated,
            library::sidecars::save_metadata_and_update_thumbnail,
            library::sidecars::apply_adjustments_to_paths,
            library::sidecars::load_metadata,
            library::presets::load_presets,
            library::presets::save_presets,
            library::folders::get_or_create_internal_library_root,
            library::sidecars::reset_adjustments_for_paths,
            library::sidecars::apply_auto_lens_correction_to_paths,
            adjustments::develop_defaults::get_default_adjustments,
            library::sidecars::apply_auto_adjustments_to_paths,
            library::presets::handle_import_presets_from_file,
            library::presets::handle_import_legacy_presets_from_file,
            library::presets::handle_import_presets_from_files,
            library::presets::handle_export_presets_to_file,
            library::presets::save_community_preset,
            library::sidecars::clear_all_sidecars,
            library::thumbnails::clear_thumbnail_cache,
            app::storage::get_storage_usage,
            app::storage::clear_ai_maps_cache,
            app::storage::clear_metadata_cache,
            app::storage::clear_immich_cache,
            app::storage::clear_ai_models,
            library::sidecars::set_color_label_for_paths,
            library::sidecars::set_rating_for_paths,
            library::sidecars::set_flag_for_paths,
            library::file_ops::import_files,
            library::sidecars::create_virtual_copy,
            library::albums::get_albums,
            library::albums::save_albums,
            library::albums::add_to_album,
            library::albums::get_album_images,
            library::tagging::start_background_indexing,
            library::tagging::clear_ai_tags,
            library::tagging::clear_all_tags,
            library::tagging::add_tag_for_paths,
            library::tagging::remove_tag_for_paths,
            library::culling::cull_images,
            adjustments::lens_correction::get_lensfun_makers,
            adjustments::lens_correction::get_lensfun_lenses_for_maker,
            adjustments::lens_correction::get_lens_correction_info,
            integrations::tethering::tether_list_cameras,
            integrations::tethering::tether_connect,
            integrations::tethering::tether_get_settings,
            integrations::tethering::tether_set_setting,
            integrations::tethering::tether_capture,
            integrations::tethering::tether_get_preview,
            integrations::tethering::tether_autofocus,
            adjustments::guided_perspective::calculate_guided_perspective,
            // Cloud-sync control surface (ARCHITECTURE.md §3.3/§3.5/§3.6/§3.8,
            // U8). cfg-gated on `sync`: a `--no-default-features` build
            // registers none of these and stays functionally upstream (§7);
            // the webview call-guards on their absence.
            #[cfg(feature = "sync")]
            sync::commands::sync_status,
            #[cfg(feature = "sync")]
            sync::commands::sync_ensure_configured,
            #[cfg(feature = "sync")]
            sync::commands::sync_run_cycle,
            #[cfg(feature = "sync")]
            sync::commands::sync_configure,
            #[cfg(feature = "sync")]
            sync::commands::sync_set_credentials,
            #[cfg(feature = "sync")]
            sync::commands::sync_list_media_buckets,
            #[cfg(feature = "sync")]
            sync::pairing::sync_pair_begin,
            #[cfg(feature = "sync")]
            sync::pairing::sync_pair_complete,
            #[cfg(feature = "sync")]
            sync::commands::sync_pin_paths,
            #[cfg(feature = "sync")]
            sync::commands::sync_unpin_paths,
            #[cfg(feature = "sync")]
            sync::commands::sync_free_space,
            #[cfg(feature = "sync")]
            sync::commands::sync_hydrate,
            #[cfg(feature = "sync")]
            sync::commands::sync_recently_deleted,
            #[cfg(feature = "sync")]
            sync::commands::sync_restore,
            #[cfg(feature = "sync")]
            sync::commands::sync_resolve_conflict,
            #[cfg(feature = "sync")]
            sync::commands::sync_flush_path,
            #[cfg(feature = "sync")]
            sync::commands::sync_retire_device,
            #[cfg(feature = "sync")]
            sync::commands::sync_verify_library,
            integrations::immich::commands::immich_get_api_key,
            integrations::immich::commands::immich_set_api_key,
            integrations::immich::commands::immich_test_connection,
            integrations::immich::commands::immich_list_albums,
            integrations::immich::commands::immich_create_album,
            integrations::immich::commands::immich_get_images,
            integrations::immich::commands::immich_sync_status,
            integrations::immich::commands::immich_flush,
            integrations::immich::commands::immich_prepare_export,
            integrations::immich::commands::immich_remove_from_album,
            integrations::immich::commands::immich_timeline,
        ])
        .build(tauri::generate_context!())
        .expect("error while building tauri application")
        .run(#[allow(unused_variables)] |app_handle, event| {
            match event {
                #[cfg(target_os = "macos")]
				tauri::RunEvent::Opened { urls } => {
				    if let Some(url) = urls.first()
				        && let Ok(path) = url.to_file_path()
				        && let Some(path_str) = path.to_str()
				    {
				        let state = app_handle.state::<AppState>();

				        if state
				            .window_setup_complete
				            .load(std::sync::atomic::Ordering::Relaxed)
				        {
				            if let Some(window) = app_handle.get_webview_window("main") {
				                let _ = window.unminimize();
				                let _ = window.show();
				                let _ = window.set_focus();
				            }

				            log::info!("macOS runtime open: Opening {}.", path_str);

				            emit_launch_request(
				                app_handle,
				                LaunchRequest::OpenFile(path_str.to_string()),
				            );
				        } else {
				            *state.initial_file_path.lock().unwrap() =
				                Some(path_str.to_string());

				            log::info!(
				                "macOS initial open: Stored path {} for later.",
				                path_str
				            );
				        }
				    }
				}
                tauri::RunEvent::ExitRequested { api, code, .. } => {
                    api.prevent_exit();
                    let code = resolve_exit_code(code, process_exit_code());

                    // Bounded (<=2s) opportunistic flush of queued small
                    // sidecar uploads before the process exits (§3.3).
                    crate::sync::hooks::sync_exit_flush(app_handle);

                    #[cfg(target_os = "macos")]
                    unsafe { libc::_exit(code); }

                    #[cfg(not(target_os = "macos"))]
                    std::process::exit(code);
                }
                tauri::RunEvent::Exit => {
                    let code = process_exit_code();

                    #[cfg(target_os = "macos")]
                    unsafe { libc::_exit(code); }

                    #[cfg(not(target_os = "macos"))]
                    std::process::exit(code);
                }
                _ => {}
            }
        });
}

#[cfg(test)]
mod proxy_render_scale_tests {
    //! P3 review BLOCKER regression (ARCHITECTURE.md §4.4): in proxy edit mode
    //! the render path must map original-pixel-space mask/crop geometry onto the
    //! downscaled proxy base via `proxy_scale`. Before the fix the editor-preview
    //! path derived the mask scale from the loaded PROXY dims alone
    //! (`scale_for_gpu`), ignoring `proxy_scale`, so every mask/crop was
    //! displaced by `orig/proxy` (~2.3x) when editing an evicted photo.
    //!
    //! These tests pin the pure scale seam the render path now routes through
    //! (`effective_geometry_scale`) and rasterize a mask through it to prove the
    //! geometry lands where the original-pixel coordinates say it should.

    use super::effective_geometry_scale;
    use crate::masks::generation::{MaskDefinition, SubMask, SubMaskMode, generate_mask_bitmap};

    fn radial_def(center_x: f64, center_y: f64, radius: f64) -> MaskDefinition {
        MaskDefinition {
            id: "m".into(),
            name: "radial".into(),
            visible: true,
            invert: false,
            opacity: 100.0,
            adjustments: serde_json::Value::Null,
            sub_masks: vec![SubMask {
                id: "s".into(),
                mask_type: "radial".into(),
                visible: true,
                invert: false,
                opacity: 100.0,
                mode: SubMaskMode::Additive,
                parameters: serde_json::json!({
                    "centerX": center_x,
                    "centerY": center_y,
                    "radiusX": radius,
                    "radiusY": radius,
                    "rotation": 0.0,
                    "feather": 0.0,
                }),
            }],
        }
    }

    fn centroid(bmp: &image::GrayImage) -> Option<(f32, f32)> {
        let (mut sx, mut sy, mut n) = (0.0f64, 0.0f64, 0u64);
        for (x, y, p) in bmp.enumerate_pixels() {
            if p[0] > 127 {
                sx += x as f64;
                sy += y as f64;
                n += 1;
            }
        }
        (n > 0).then(|| ((sx / n as f64) as f32, (sy / n as f64) as f32))
    }

    #[test]
    fn effective_geometry_scale_folds_proxy_scale() {
        // Normal mode: base IS the original, so the mask scale is just the
        // base->preview downscale.
        assert_eq!(effective_geometry_scale(0.5, 1.0), 0.5);
        // Proxy mode: base is the proxy (orig * proxy_scale), so original->preview
        // = proxy_scale * (proxy->preview). A missing proxy_scale factor (the
        // blocker) would leave this at `base_to_preview`.
        let proxy_scale = 2560.0 / 6000.0;
        let eff = effective_geometry_scale(1.0, proxy_scale);
        assert!((eff - proxy_scale).abs() < 1e-6);
    }

    #[test]
    fn proxy_mode_mask_lands_at_original_pixel_coordinate() {
        // Original 6000x4000; smart-preview proxy long edge 2560 => 2560x1707.
        // proxy_scale = proxy_long_edge / orig_long_edge (§4.4); computed inline
        // so this guard compiles in both feature configs.
        let proxy_scale = 2560.0f32 / 6000.0f32;
        let (pw, ph) = (2560u32, 1707u32);

        // The editor preview equals the proxy base here (no further downscale),
        // so base->preview = 1.0 and the mask scale is purely `proxy_scale`.
        let eff = effective_geometry_scale(1.0, proxy_scale);

        // A radial mask centered at ORIGINAL pixel (3000, 2000) must land at the
        // corresponding proxy-space point (1280, 853) on the 2560x1707 canvas.
        let def = radial_def(3000.0, 2000.0, 300.0);
        let bmp = generate_mask_bitmap(&def, pw, ph, eff, (0.0, 0.0), None).expect("mask bitmap");
        let (cx, cy) = centroid(&bmp).expect(
            "mask must be visible on the proxy canvas; a missing proxy_scale displaces it \
             off-canvas (to original coord 3000,2000 on a 2560x1707 base)",
        );

        let (ex, ey) = (3000.0 * proxy_scale, 2000.0 * proxy_scale);
        assert!(
            (cx - ex).abs() < 20.0 && (cy - ey).abs() < 20.0,
            "radial mask centroid ({cx:.1},{cy:.1}) must match original->proxy ({ex:.1},{ey:.1}); \
             a ~2.3x displacement here is the P3 proxy_scale blocker"
        );
    }
}
