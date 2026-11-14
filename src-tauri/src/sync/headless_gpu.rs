//! Headless GPU seams for the P3 smart-preview fidelity suite (ARCHITECTURE.md
//! §8-P3), re-exported through [`super::proxy_support`].
//!
//! These used to live in upstream's `gpu_processing.rs`; upstream split that
//! file into `render::gpu::*`, so the fork keeps them here instead of adding
//! fork code to the new upstream modules. They only *call* upstream's public
//! GPU API (`GpuContext`, `GpuProcessor`, `RenderSource`, `compute::upload_image`)
//! and never touch `AppState`, so they cannot change app behavior.

use std::sync::{Arc, OnceLock};

use image::{DynamicImage, GenericImageView, ImageBuffer, Rgba};

use crate::app::state::GpuImageEntry;
use crate::render::gpu::context::GpuContext;
use crate::render::gpu::display::align_texture_dimension;
use crate::render::gpu::processor::{
    GpuProcessor, RenderOutputPrecision, RenderRequest, RenderedPixels,
};
use crate::render::source::RenderSource;

fn headless_instance() -> wgpu::Instance {
    wgpu::Instance::new(wgpu::InstanceDescriptor::new_without_display_handle_from_env())
}

fn headless_adapter(instance: &wgpu::Instance) -> Result<wgpu::Adapter, wgpu::RequestAdapterError> {
    pollster::block_on(instance.request_adapter(&wgpu::RequestAdapterOptions {
        power_preference: wgpu::PowerPreference::HighPerformance,
        compatible_surface: None,
        force_fallback_adapter: false,
    }))
}

/// Headless wgpu adapter probe (P3 §8 fidelity GPU confirmation).
///
/// Builds a compute-only `wgpu::Instance` (honoring `WGPU_BACKEND` /
/// `VK_ICD_FILENAMES` so a software Vulkan adapter — Mesa lavapipe — is
/// picked up in CI) and requests an adapter with **no** surface. Returns
/// `Some(adapter description)` when one initializes, `None` otherwise.
///
/// The P3 GPU-rendered ΔE confirmation test calls this to decide whether to
/// run or to skip-with-loud-eprintln (documented user-run confirmation);
/// never fail a build on GPU unavailability (§8-P3 / task GPU note).
pub fn gpu_adapter_probe() -> Option<String> {
    match headless_adapter(&headless_instance()) {
        Ok(adapter) => {
            let info = adapter.get_info();
            Some(format!(
                "{} ({:?}, {:?})",
                info.name, info.backend, info.device_type
            ))
        }
        Err(e) => {
            log::warn!("gpu_adapter_probe: no wgpu adapter: {e}");
            None
        }
    }
}

/// Build a compute-only [`GpuContext`] with no window surface and no
/// `AppState` (P3 fidelity GPU confirmation seam). Mirrors the adapter /
/// device bring-up in `render::gpu::context::get_or_init_gpu_context` without
/// the surface, crash-flag, or display plumbing.
pub fn init_gpu_context_headless() -> Result<GpuContext, String> {
    let instance = headless_instance();
    let adapter = headless_adapter(&instance)
        .map_err(|e| format!("headless: failed to find a wgpu adapter: {e}"))?;

    let mut required_features = wgpu::Features::empty();
    if adapter
        .features()
        .contains(wgpu::Features::TEXTURE_ADAPTER_SPECIFIC_FORMAT_FEATURES)
    {
        required_features |= wgpu::Features::TEXTURE_ADAPTER_SPECIFIC_FORMAT_FEATURES;
    }

    let limits = adapter.limits();

    let (device, queue) = pollster::block_on(adapter.request_device(&wgpu::DeviceDescriptor {
        label: Some("Headless Processing Device"),
        required_features,
        required_limits: limits.clone(),
        experimental_features: wgpu::ExperimentalFeatures::default(),
        memory_hints: wgpu::MemoryHints::Performance,
        trace: wgpu::Trace::Off,
    }))
    .map_err(|e| e.to_string())?;

    device.on_uncaptured_error(Arc::new(|err: wgpu::Error| {
        log::error!("[wgpu-error][headless] {}", err);
    }));

    Ok(GpuContext {
        instance,
        adapter,
        device: Arc::new(device),
        queue: Arc::new(queue),
        limits,
        display: Arc::new(std::sync::Mutex::new(None)),
        viewer_display: Arc::new(std::sync::Mutex::new(None)),
        pipelines: Arc::new(OnceLock::new()),
        compute: Arc::new(OnceLock::new()),
    })
}

/// Render `base_image` through the real adjustment pipeline at its native
/// size under `adjustments` (serde_json adjustments object), returning the
/// 8-bit sRGB result — the §8-P3 GPU-rendered ΔE confirmation seam, driving
/// the same `GpuProcessor` path the editor uses but without `AppState` caches.
pub fn render_adjustments_headless(
    context: &GpuContext,
    base_image: &DynamicImage,
    adjustments: &serde_json::Value,
) -> Result<DynamicImage, String> {
    let (width, height) = base_image.dimensions();
    if width == 0 || height == 0 {
        return Err("headless render: empty base image".to_string());
    }

    let max_dim = context.limits.max_texture_dimension_2d;
    if width > max_dim || height > max_dim {
        return Err(format!(
            "headless render: image {width}x{height} exceeds GPU limit {max_dim}"
        ));
    }

    let processor = GpuProcessor::new(
        context.clone(),
        align_texture_dimension(width),
        align_texture_dimension(height),
    )?;

    // Upload the linear base exactly as the editor's input path does, through
    // an identity `RenderSource` (no crop/warp/effects).
    let source = RenderSource::plain(base_image);
    let slot = crate::render::gpu::compute::upload_image(
        context,
        (width, height),
        &crate::render::gpu::compute::rgba_halves(base_image),
    );
    let input = source.render(context, &slot)?;

    // Raw-mode adjustments (the proxy and original develop bases are both raw
    // develop output); no masks, no LUT, full frame. The base is already a
    // developed linear frame with no camera metadata here, so the as-shot
    // white balance and exposure are the reference (neutral, 0 EV baseline
    // compensation); the user's WB adjustments in `adjustments` apply on top.
    let all_adjustments = crate::render::uniforms::get_all_adjustments_from_json(
        adjustments,
        true,
        crate::decode::as_shot::AsShot::reference(),
        crate::render::options::RenderOptions::default(),
    );
    let is_raw = all_adjustments.global.is_raw_image;
    let guided = processor.build_guided_coeffs(&input.view, width, height, is_raw);
    let entry = GpuImageEntry {
        id: 0,
        texture: input.texture,
        texture_view: input.view,
        gf_coeffs_view: guided.coeffs,
        tone_base_view: guided.tone_base,
        haze_view: guided.haze,
        airlight_view: guided.airlight,
        is_raw,
        width,
        height,
        transform_key: source.key(),
        source_identity: source.identity(),
    };
    let mask_view = processor.mask_view(&[], width, height, None);
    let request = RenderRequest {
        adjustments: all_adjustments,
        mask_bitmaps: &[],
        mask_cache_key: None,
        lut: None,
        roi: None,
        hsl_probe: false,
        motion_blur: Default::default(),
    };

    let (processed_pixels, out_w, out_h, _x, _y) = processor.run(
        &entry,
        &mask_view,
        request,
        false,
        RenderOutputPrecision::EightBit,
    )?;

    match processed_pixels {
        RenderedPixels::U8(pixels) => {
            let img_buf = ImageBuffer::<Rgba<u8>, Vec<u8>>::from_raw(out_w, out_h, pixels)
                .ok_or("headless render: failed to build 8-bit image buffer")?;
            Ok(DynamicImage::ImageRgba8(img_buf))
        }
        RenderedPixels::U16(pixels) => {
            let img_buf = ImageBuffer::<Rgba<u16>, Vec<u16>>::from_raw(out_w, out_h, pixels)
                .ok_or("headless render: failed to build 16-bit image buffer")?;
            Ok(DynamicImage::ImageRgba16(img_buf))
        }
    }
}
