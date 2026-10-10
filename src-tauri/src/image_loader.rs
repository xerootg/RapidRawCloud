use crate::Cursor;
use crate::app_settings::{AppSettings, load_settings};
use crate::app_state::{AppState, LoadedImage};
use crate::exif_processing;
use crate::file_management::{parse_virtual_path, read_file_mapped};
use crate::formats::is_raw_file;
use crate::image_processing::ImageMetadata;
use crate::image_processing::{
    apply_orientation, apply_srgb_to_linear, remove_raw_artifacts_and_enhance,
};
use crate::mask_generation::{MaskDefinition, SubMask, generate_mask_bitmap};
use crate::white_balance::WhiteBalance;
use anyhow::{Context, Result, anyhow};
use base64::{Engine as _, engine::general_purpose};
use exif::{Reader as ExifReader, Tag};
use image::{DynamicImage, GenericImageView, ImageReader, imageops};
use rawler::Orientation;
use rayon::prelude::*;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::panic;
use std::path::Path;
use std::sync::OnceLock;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Instant;

#[derive(serde::Serialize)]
pub struct LoadImageResult {
    pub width: u32,
    pub height: u32,
    pub metadata: ImageMetadata,
    pub exif: HashMap<String, String>,
    pub is_raw: bool,
    pub as_shot_white_balance: WhiteBalance,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PatchMaskInfo {
    id: String,
    name: String,
    #[serde(default)]
    invert: bool,
    #[serde(default)]
    sub_masks: Vec<SubMask>,
}

fn srgb_to_linear_lut() -> &'static [f32; 256] {
    static LUT: OnceLock<[f32; 256]> = OnceLock::new();
    LUT.get_or_init(|| {
        let mut lut = [0.0f32; 256];
        for (i, v) in lut.iter_mut().enumerate() {
            let x = i as f32 / 255.0;
            *v = if x <= 0.04045 {
                x / 12.92
            } else {
                ((x + 0.055) / 1.055).powf(2.4)
            };
        }
        lut
    })
}

pub fn load_and_composite(
    base_image: &[u8],
    path: &str,
    adjustments: &Value,
    use_fast_raw_dev: bool,
    settings: &AppSettings,
    cancel_token: Option<(Arc<AtomicUsize>, usize)>,
) -> Result<DynamicImage> {
    let base_image =
        load_base_image_from_bytes(base_image, path, use_fast_raw_dev, settings, cancel_token)?;
    // Base is a full decode of the original bytes, so patch geometry is already
    // in this image's pixel space: proxy_scale = 1.0 (§4.4, no-op).
    composite_patches_on_image(&base_image, adjustments, 1.0)
}

pub fn load_base_image_from_bytes(
    bytes: &[u8],
    path_for_ext_check: &str,
    use_fast_raw_dev: bool,
    settings: &AppSettings,
    cancel_token: Option<(Arc<AtomicUsize>, usize)>,
) -> Result<DynamicImage> {
    let highlight_compression = settings.raw_highlight_compression.unwrap_or(2.5);
    let linear_mode = settings.linear_raw_mode.clone();
    let color_nr_setting = settings.raw_preprocessing_color_nr.unwrap_or(0.5);
    let color_nr_amount = if color_nr_setting <= 0.0 {
        0.0
    } else {
        let x = color_nr_setting.clamp(0.01, 1.0);
        (12.0 / x - 10.0).max(0.1)
    };
    let sharpening_amount = settings.raw_preprocessing_sharpening.unwrap_or(0.35);
    let apply_to_non_raws = settings.apply_preprocessing_to_non_raws.unwrap_or(false);

    crate::exif_processing::persist_exif_if_missing(
        Path::new(path_for_ext_check),
        path_for_ext_check,
        bytes,
    );

    if is_raw_file(path_for_ext_check)
        && !use_fast_raw_dev
        && settings.use_apple_raw9.unwrap_or(false)
    {
        if let Some((tracker, generation)) = &cancel_token
            && tracker.load(Ordering::SeqCst) != *generation
        {
            return Err(anyhow!("Load cancelled"));
        }

        match crate::apple_raw::develop_raw9(
            bytes,
            path_for_ext_check,
            &crate::apple_raw::Raw9Options::for_loading(),
        ) {
            Ok(image) => return Ok(image),
            Err(e) => log::warn!(
                "Apple RAW 9 unavailable for '{}', falling back to rawler: {}",
                path_for_ext_check,
                e
            ),
        }
    }

    if is_raw_file(path_for_ext_check) {
        match panic::catch_unwind(move || {
            crate::raw_processing::develop_raw_image(
                bytes,
                use_fast_raw_dev,
                highlight_compression,
                linear_mode,
                cancel_token,
            )
        }) {
            Ok(Ok(mut image)) => {
                if !use_fast_raw_dev && (color_nr_amount > 0.0 || sharpening_amount > 0.0) {
                    let start = Instant::now();
                    remove_raw_artifacts_and_enhance(
                        &mut image,
                        color_nr_amount,
                        sharpening_amount,
                    );
                    let duration = start.elapsed();
                    log::info!(
                        "Raw enhancing for '{}' took {:?}",
                        path_for_ext_check,
                        duration
                    );
                }
                Ok(image)
            }
            Ok(Err(e)) => {
                let classified = classify_raw_develop_error(path_for_ext_check, e);

                if classified.to_string().contains("Load cancelled") {
                    return Err(classified);
                }

                log::warn!(
                    "Error developing RAW file '{}': {}",
                    path_for_ext_check,
                    classified
                );
                if let Some(preview) = safe_embedded_preview_fallback(bytes, path_for_ext_check) {
                    log::warn!(
                        "Using embedded preview fallback for '{}' ({}x{})",
                        path_for_ext_check,
                        preview.width(),
                        preview.height()
                    );

                    return Ok(linearize_embedded_preview(preview));
                }
                Err(classified)
            }
            Err(_) => {
                log::error!("Panic while processing RAW file: {}", path_for_ext_check);
                if let Some(preview) = safe_embedded_preview_fallback(bytes, path_for_ext_check) {
                    log::warn!(
                        "Using embedded preview fallback for '{}' after RAW decoder panic ({}x{})",
                        path_for_ext_check,
                        preview.width(),
                        preview.height()
                    );

                    return Ok(linearize_embedded_preview(preview));
                }
                Err(anyhow!(
                    "Failed to process RAW file: {}",
                    path_for_ext_check
                ))
            }
        }
    } else {
        let mut image = load_image_with_orientation(bytes, cancel_token)?;

        if apply_to_non_raws
            && !use_fast_raw_dev
            && (color_nr_amount > 0.0 || sharpening_amount > 0.0)
        {
            let start = Instant::now();
            remove_raw_artifacts_and_enhance(&mut image, color_nr_amount, sharpening_amount);
            let duration = start.elapsed();
            log::info!(
                "Enhancing non-RAW '{}' took {:?}",
                path_for_ext_check,
                duration
            );
        }

        Ok(image)
    }
}

/// §4.4 (w,h) provenance invariant: proxy-mode load reports the **original**
/// (journal) dimensions, never the decoded proxy's reduced size. The reduced
/// size feeds only `proxy_scale`. A pure helper so the invariant is directly
/// unit-testable — "journal dims == proxy-mode reported dims" — and so the
/// loader branch cannot drift from it.
pub fn proxy_reported_dimensions(
    journal_dims: (u32, u32),
    _proxy_decoded_dims: (u32, u32),
) -> (u32, u32) {
    journal_dims
}

/// Settings used to decode a smart preview (§4.1/§4.4): the proxy load path
/// pins `linear_raw_mode` so user `linear_mode` settings never alter a
/// tagged `RapidRawCloud/pxy1` proxy — `(apply_ungamma = false,
/// apply_calibration = true)`, which is the `develop_internal` default arm
/// (`linear_raw_mode = ""`). Returns a copy of `base` with that pin applied.
pub fn proxy_decode_settings(base: &AppSettings) -> AppSettings {
    let mut s = base.clone();
    s.linear_raw_mode = String::new();
    s
}

/// §4.4 proxy edit mode load. Decodes the smart preview DNG through
/// [`load_base_image_from_bytes`] (hitting the LinearRaw branch with the
/// §4.1 clamp fix and the pinned `(apply_ungamma=false, apply_calibration=true)`),
/// stores `proxy_scale = proxy_long_edge / orig_long_edge` in `AppState`, sets
/// `original_image`, and returns a [`LoadImageResult`] reporting the ORIGINAL
/// (journal) dimensions via [`proxy_reported_dimensions`].
///
/// Reached when `SyncManager::proxy_handle` returns a handle — i.e. the original
/// is an evicted stub with a present `.pxy.dng` proxy and journaled `(w,h)`.
/// Otherwise `load_image` falls through to the §3.5 hydrate path. The stored
/// `proxy_scale` (§4.4) is consumed by the render paths
/// (`generate_transformed_preview` / `generate_thumbnail_data`) so
/// original-pixel-space mask/crop geometry maps onto the proxy base.
#[cfg(feature = "sync")]
#[allow(clippy::needless_pass_by_value)]
async fn load_image_from_proxy(
    path: String,
    handle: crate::sync::hooks::ProxyHandle,
    metadata: ImageMetadata,
    settings: AppSettings,
    state: tauri::State<'_, AppState>,
    my_generation: usize,
) -> Result<LoadImageResult, String> {
    let generation_tracker = state.load_image_generation.clone();
    let cancel_token = Some((generation_tracker.clone(), my_generation));

    // §4.4: proxy_scale = proxy_long_edge / orig_long_edge. Both share the
    // §4.2-step-3 develop provenance, so this cannot misplace masks.
    let orig_long = handle.orig_width.max(handle.orig_height);
    let proxy_scale = rrcloud_core::proxy::proxy_scale(orig_long, handle.proxy_long_edge);

    // §4.1/§4.4: pin the proxy decode past the user's `linear_raw_mode`
    // (apply_ungamma=false, apply_calibration=true). Disable the loader's own
    // color-NR / sharpening so we can re-apply them scaled by `proxy_scale`
    // (E2) rather than at full-resolution amounts.
    let orig_color_nr = settings.raw_preprocessing_color_nr.unwrap_or(0.5);
    let orig_sharpening = settings.raw_preprocessing_sharpening.unwrap_or(0.35);
    let mut decode_settings = proxy_decode_settings(&settings);
    decode_settings.raw_preprocessing_color_nr = Some(0.0);
    decode_settings.raw_preprocessing_sharpening = Some(0.0);

    // E2: the full-resolution enhance amounts, scaled to the proxy resolution.
    let color_nr_amount = if orig_color_nr <= 0.0 {
        0.0
    } else {
        let x = orig_color_nr.clamp(0.01, 1.0);
        (12.0 / x - 10.0).max(0.1)
    } * proxy_scale;
    let sharpening_amount = orig_sharpening * proxy_scale;

    let dng_path = handle.dng_path.clone();
    let dng_path_str = dng_path.to_string_lossy().to_string();

    let decoded = tokio::task::spawn_blocking(
        move || -> Result<(DynamicImage, HashMap<String, String>), String> {
            if generation_tracker.load(Ordering::SeqCst) != my_generation {
                return Err("Load cancelled".to_string());
            }
            let bytes = match read_file_mapped(dng_path.as_path()) {
                Ok(mmap) => mmap.to_vec(),
                Err(_) => fs::read(&dng_path).map_err(|e| {
                    format!("Failed to read smart preview {}: {}", dng_path.display(), e)
                })?,
            };
            if generation_tracker.load(Ordering::SeqCst) != my_generation {
                return Err("Load cancelled".to_string());
            }
            let mut img = load_base_image_from_bytes(
                &bytes,
                &dng_path_str,
                false,
                &decode_settings,
                cancel_token.clone(),
            )
            .map_err(|e| e.to_string())?;
            if color_nr_amount > 0.0 || sharpening_amount > 0.0 {
                remove_raw_artifacts_and_enhance(&mut img, color_nr_amount, sharpening_amount);
            }
            let exif = crate::exif_processing::read_exif_data(&dng_path_str, &bytes);
            Ok((img, exif))
        },
    )
    .await
    .map_err(|e| e.to_string())??;

    let (proxy_img, exif_data) = decoded;

    if state.load_image_generation.load(Ordering::SeqCst) != my_generation {
        return Err("Load cancelled".to_string());
    }

    // §4.4 (w,h) provenance: report the ORIGINAL (journal) dimensions, never
    // the decoded proxy's reduced size — the latter only feeds `proxy_scale`.
    let (reported_width, reported_height) = proxy_reported_dimensions(
        (handle.orig_width, handle.orig_height),
        proxy_img.dimensions(),
    );

    *state.proxy_scale.lock().unwrap_or_else(|e| e.into_inner()) = Some(proxy_scale);

    // As-shot white balance is a property of the ORIGINAL raw, not the linear
    // proxy DNG; `as_shot_white_balance` reads it from the original path (and
    // falls back to a reference neutral when the original is not a readable raw
    // — the expected case while editing an evicted stub via its proxy).
    let as_shot_white_balance = crate::white_balance::as_shot_white_balance(&path);

    *state
        .original_image
        .lock()
        .unwrap_or_else(|e| e.into_inner()) = Some(crate::app_state::LoadedImage {
        path,
        image: Arc::new(proxy_img),
        is_raw: true,
        as_shot_white_balance,
    });

    Ok(LoadImageResult {
        width: reported_width,
        height: reported_height,
        metadata,
        exif: exif_data,
        is_raw: true,
        as_shot_white_balance,
    })
}

fn classify_raw_develop_error(path: &str, err: anyhow::Error) -> anyhow::Error {
    let error_text = err.to_string();
    let lowered = error_text.to_ascii_lowercase();
    let unsupported_compression =
        lowered.contains("nef compression") && lowered.contains("not supported");

    if unsupported_compression {
        return anyhow!(
            "Unsupported RAW compression format for '{}'. Original error: {}",
            path,
            error_text
        );
    }

    err
}

fn largest_tiff_jpeg_preview(buf: &[u8]) -> Option<DynamicImage> {
    let le = match buf.get(..4)? {
        [0x49, 0x49, 0x2A, 0x00] => true,
        [0x4D, 0x4D, 0x00, 0x2A] => false,
        _ => return None,
    };
    let rd16 = |o: usize| -> Option<u64> {
        let b: [u8; 2] = buf.get(o..o + 2)?.try_into().ok()?;
        Some(if le {
            u16::from_le_bytes(b)
        } else {
            u16::from_be_bytes(b)
        } as u64)
    };
    let rd32 = |o: usize| -> Option<u64> {
        let b: [u8; 4] = buf.get(o..o + 4)?.try_into().ok()?;
        Some(if le {
            u32::from_le_bytes(b)
        } else {
            u32::from_be_bytes(b)
        } as u64)
    };

    let mut candidates: Vec<(u64, u64)> = Vec::new();
    let mut queue: Vec<u64> = vec![rd32(4)?];
    let mut seen = HashMap::new();

    while let Some(ifd) = queue.pop() {
        if seen.insert(ifd, ()).is_some() || seen.len() > 64 {
            continue;
        }
        let Some(n) = rd16(ifd as usize) else {
            continue;
        };

        let mut compression: u64 = 0;
        let mut strip: Option<(u64, u64)> = None;
        let mut old_jpeg: Option<(u64, u64)> = None;

        for i in 0..n {
            let e = ifd as usize + 2 + (i as usize) * 12;
            let (Some(tag), Some(count), Some(val)) = (rd16(e), rd32(e + 4), rd32(e + 8)) else {
                continue;
            };
            match tag {
                259 => compression = val,
                273 if count == 1 => strip = Some((val, strip.map_or(0, |s| s.1))),
                279 if count == 1 => strip = strip.map(|s| (s.0, val)).or(Some((0, val))),
                513 => old_jpeg = Some((val, old_jpeg.map_or(0, |s| s.1))),
                514 => old_jpeg = old_jpeg.map(|s| (s.0, val)).or(Some((0, val))),
                330 => {
                    if count == 1 {
                        queue.push(val);
                    } else {
                        for j in 0..count.min(8) {
                            if let Some(p) = rd32(val as usize + (j as usize) * 4) {
                                queue.push(p);
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        if matches!(compression, 6 | 7)
            && let Some(s) = strip
        {
            candidates.push(s);
        }
        if let Some(oj) = old_jpeg {
            candidates.push(oj);
        }
        if let Some(next) = rd32(ifd as usize + 2 + (n as usize) * 12)
            && next != 0
        {
            queue.push(next);
        }
    }

    candidates.sort_by_key(|&(_, len)| std::cmp::Reverse(len));

    for (off, len) in candidates {
        if let Some(bytes) = buf.get(off as usize..(off + len) as usize)
            && let Ok(img) = image::load_from_memory_with_format(bytes, image::ImageFormat::Jpeg)
        {
            return Some(img);
        }
    }

    None
}

fn embedded_preview_fallback(bytes: &[u8]) -> Option<DynamicImage> {
    let Some(img) = largest_tiff_jpeg_preview(bytes) else {
        return crate::raw_processing::extract_embedded_preview(bytes);
    };

    let orientation = ExifReader::new()
        .read_from_container(&mut Cursor::new(bytes))
        .ok()
        .and_then(|exif| {
            exif.get_field(Tag::Orientation, exif::In::PRIMARY)?
                .value
                .get_uint(0)
        });

    Some(match orientation {
        Some(o) if o > 1 => apply_orientation(img, Orientation::from_u16(o as u16)),
        _ => img,
    })
}

pub fn safe_embedded_preview_fallback(bytes: &[u8], path: &str) -> Option<DynamicImage> {
    match panic::catch_unwind(panic::AssertUnwindSafe(|| embedded_preview_fallback(bytes))) {
        Ok(preview) => preview,
        Err(_) => {
            log::warn!("Embedded RAW preview extraction panicked for '{}'", path);
            None
        }
    }
}

fn linearize_embedded_preview(preview: DynamicImage) -> DynamicImage {
    let preview = DynamicImage::ImageRgb32F(preview.to_rgb32f());
    let mut linear_preview = apply_srgb_to_linear(preview).into_rgb32f();
    for pixel in linear_preview.pixels_mut() {
        pixel[0] *= 0.4;
        pixel[1] *= 0.4;
        pixel[2] *= 0.4;
    }
    DynamicImage::ImageRgb32F(linear_preview)
}

pub fn load_image_with_orientation(
    bytes: &[u8],
    cancel_token: Option<(Arc<AtomicUsize>, usize)>,
) -> Result<DynamicImage> {
    let check_cancel = || -> Result<()> {
        if let Some((tracker, generation)) = &cancel_token
            && tracker.load(Ordering::SeqCst) != *generation
        {
            return Err(anyhow!("Load cancelled"));
        }
        Ok(())
    };

    let cursor = Cursor::new(bytes);
    let mut reader = ImageReader::new(cursor.clone())
        .with_guessed_format()
        .context("Failed to guess image format")?;

    // Never decode without limits: the decoder allocates the full output
    // buffer from the header-declared dimensions before validating any pixel
    // data, so a tiny hostile file claiming 100000x100000 would request 40 GB
    // and abort the process. 65536 px per side and 4 GiB of decoded pixels are
    // far beyond any camera file (a 150 MP RGBA8 decode is ~600 MB) while
    // still refusing absurd headers up front.
    let mut limits = image::Limits::default();
    limits.max_image_width = Some(65_536);
    limits.max_image_height = Some(65_536);
    limits.max_alloc = Some(4 * 1024 * 1024 * 1024);
    reader.limits(limits);

    check_cancel()?;

    let image = reader.decode().map_err(|e| match e {
        image::ImageError::Limits(limit_err) => {
            anyhow!("Image dimensions or decoded size exceed supported limits: {limit_err}")
        }
        other => anyhow::Error::new(other).context("Failed to decode image"),
    })?;
    check_cancel()?;

    let oriented_image = {
        let exif_reader = ExifReader::new();
        if let Ok(exif) = exif_reader.read_from_container(&mut cursor.clone()) {
            if let Some(orientation) = exif
                .get_field(Tag::Orientation, exif::In::PRIMARY)
                .and_then(|f| f.value.get_uint(0))
            {
                check_cancel()?;
                apply_orientation(image, Orientation::from_u16(orientation as u16))
            } else {
                image
            }
        } else {
            image
        }
    };

    Ok(DynamicImage::ImageRgb32F(oriented_image.to_rgb32f()))
}

/// Composite AI patches onto `base_image`.
///
/// §4.4: a cropped patch (`offsetX`/`offsetY`/`width`/`height` present) stores
/// its geometry — and its `mask`/`color` bitmaps — in ORIGINAL pixel space. When
/// the base is a smart-preview proxy the base is downscaled by `proxy_scale`
/// (`= proxy_long_edge / orig_long_edge`, always `<= 1.0`), so the patch offset
/// and bitmaps must be scaled into proxy space before compositing; pasting them
/// verbatim lands the patch grossly displaced (mostly off-canvas) and mis-sized.
/// Non-cropped (full-frame) patches resize their bitmaps to the base dimensions
/// and so are already correct at any resolution. Callers whose base IS the
/// original (export / hydrate / load-from-original) pass `proxy_scale = 1.0`,
/// which makes every scaling step below an exact no-op.
pub fn composite_patches_on_image(
    base_image: &DynamicImage,
    current_adjustments: &Value,
    proxy_scale: f32,
) -> Result<DynamicImage> {
    let patches_val = match current_adjustments.get("aiPatches") {
        Some(val) => val,
        None => return Ok(base_image.clone()),
    };

    let patches_arr = match patches_val.as_array() {
        Some(arr) if !arr.is_empty() => arr,
        _ => return Ok(base_image.clone()),
    };

    let visible_patches: Vec<&Value> = patches_arr
        .par_iter()
        .filter(|patch_obj| {
            let is_visible = patch_obj
                .get("visible")
                .and_then(|v| v.as_bool())
                .unwrap_or(true);
            if !is_visible {
                return false;
            }
            patch_obj
                .get("patchData")
                .and_then(|data| data.get("color"))
                .and_then(|color| color.as_str())
                .is_some_and(|s| !s.is_empty())
        })
        .collect();

    if visible_patches.is_empty() {
        return Ok(base_image.clone());
    }

    let (base_w, base_h) = base_image.dimensions();

    struct DecodedPatch {
        offset_x: Option<u32>,
        offset_y: Option<u32>,
        mask: image::GrayImage,
        color: image::RgbImage,
        is_srgb_encoded: bool,
    }

    let decoded_patches: Result<Vec<DecodedPatch>> = visible_patches
        .par_iter()
        .map(|patch_obj| {
            let patch_data = patch_obj.get("patchData").context("Missing patchData")?;
            let offset_x = patch_data
                .get("offsetX")
                .and_then(|v| v.as_u64())
                .map(|v| v as u32);
            let offset_y = patch_data
                .get("offsetY")
                .and_then(|v| v.as_u64())
                .map(|v| v as u32);
            let is_cropped = offset_x.is_some() && offset_y.is_some();

            // §4.4: a cropped patch's offset/size and its mask/color bitmaps are
            // stored in ORIGINAL pixel space. When the base is a downscaled proxy
            // (`proxy_scale < 1.0`) scale them into proxy space; verbatim use lands
            // the patch displaced/off-canvas and mis-sized. No-op at scale 1.0
            // (original base). Full-frame patches resize to base dims below, so
            // only cropped patches need this.
            let scale_patch = is_cropped && (proxy_scale - 1.0).abs() > f32::EPSILON;
            let scale_off = |v: u32| (v as f32 * proxy_scale).round() as u32;
            let scale_size = |v: u32| ((v as f32 * proxy_scale).round() as u32).max(1);
            let offset_x = offset_x.map(|v| if scale_patch { scale_off(v) } else { v });
            let offset_y = offset_y.map(|v| if scale_patch { scale_off(v) } else { v });

            let is_srgb_encoded = patch_data
                .get("isSrgbEncoded")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);

            let mask_bitmap = if let Some(mask_b64) = patch_data
                .get("mask")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
            {
                let mask_bytes = general_purpose::STANDARD.decode(mask_b64)?;
                let mask_img = image::load_from_memory(&mask_bytes)?.to_luma8();
                if !is_cropped && (mask_img.width() != base_w || mask_img.height() != base_h) {
                    imageops::resize(&mask_img, base_w, base_h, imageops::FilterType::Lanczos3)
                } else if scale_patch {
                    // Cropped mask is original-space; downscale into proxy space.
                    imageops::resize(
                        &mask_img,
                        scale_size(mask_img.width()),
                        scale_size(mask_img.height()),
                        imageops::FilterType::Lanczos3,
                    )
                } else {
                    mask_img
                }
            } else {
                let patch_info: PatchMaskInfo = serde_json::from_value((*patch_obj).clone())
                    .context("Failed to deserialize patch info for mask generation")?;

                let mask_def = MaskDefinition {
                    id: patch_info.id,
                    name: patch_info.name,
                    visible: true,
                    invert: patch_info.invert,
                    opacity: 100.0,
                    adjustments: Value::Null,
                    sub_masks: patch_info.sub_masks,
                };

                let orientation_steps = current_adjustments
                    .get("orientationSteps")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0) as u8;
                let (trans_w, trans_h) = if orientation_steps % 2 == 1 {
                    (base_h, base_w)
                } else {
                    (base_w, base_h)
                };

                let mut gen_mask =
                    generate_mask_bitmap(&mask_def, trans_w, trans_h, 1.0, (0.0, 0.0), None)
                        .context("Failed to generate mask from sub_masks for compositing")?;

                gen_mask =
                    crate::image_processing::inverse_transform_mask(gen_mask, current_adjustments);

                if let (Some(ox), Some(oy)) = (offset_x, offset_y) {
                    // `gen_mask` is generated at base (= proxy) dims, so the crop
                    // offset `ox`/`oy` is already in proxy space (scaled above).
                    // The width/height come from patchData in ORIGINAL space, so
                    // scale them too (only when actually provided; an absent field
                    // means "to the base edge" and must not be scaled).
                    let w = patch_data
                        .get("width")
                        .and_then(|v| v.as_u64())
                        .map(|v| v as u32)
                        .map(|v| if scale_patch { scale_size(v) } else { v })
                        .unwrap_or(base_w);
                    let h = patch_data
                        .get("height")
                        .and_then(|v| v.as_u64())
                        .map(|v| v as u32)
                        .map(|v| if scale_patch { scale_size(v) } else { v })
                        .unwrap_or(base_h);
                    let crop_w = w.min(base_w.saturating_sub(ox));
                    let crop_h = h.min(base_h.saturating_sub(oy));
                    gen_mask = imageops::crop_imm(&gen_mask, ox, oy, crop_w, crop_h).to_image();
                }
                gen_mask
            };

            let color_b64 = patch_data
                .get("color")
                .and_then(|v| v.as_str())
                .context("Missing color data")?;
            let color_bytes = general_purpose::STANDARD.decode(color_b64)?;
            let color_image_u8 = image::load_from_memory(&color_bytes)?.to_rgb8();

            let (patch_w, patch_h) = color_image_u8.dimensions();
            // The compositing loop indexes `color` by the mask's linear index, so
            // color and mask MUST share dimensions. Full-frame: resize to base.
            // Cropped: match the (possibly proxy-scaled) mask so the invariant
            // holds whether or not `proxy_scale` shrank the mask.
            let (mask_w, mask_h) = mask_bitmap.dimensions();
            let final_color = if !is_cropped && (base_w != patch_w || base_h != patch_h) {
                imageops::resize(
                    &color_image_u8,
                    base_w,
                    base_h,
                    imageops::FilterType::Lanczos3,
                )
            } else if is_cropped && (patch_w != mask_w || patch_h != mask_h) {
                imageops::resize(
                    &color_image_u8,
                    mask_w,
                    mask_h,
                    imageops::FilterType::Lanczos3,
                )
            } else {
                color_image_u8
            };

            Ok(DecodedPatch {
                offset_x,
                offset_y,
                mask: mask_bitmap,
                color: final_color,
                is_srgb_encoded,
            })
        })
        .collect();

    let decoded_patches = decoded_patches?;

    let mut composited_image = base_image.clone();
    let lut = srgb_to_linear_lut();

    let get_color = |patch: &DecodedPatch, r: u8, g: u8, b: u8| -> (f32, f32, f32) {
        if patch.is_srgb_encoded {
            (lut[r as usize], lut[g as usize], lut[b as usize])
        } else {
            (r as f32 / 255.0, g as f32 / 255.0, b as f32 / 255.0)
        }
    };

    match &mut composited_image {
        DynamicImage::ImageRgb32F(img_buf) => {
            for patch in decoded_patches {
                let mask_raw = patch.mask.as_raw();
                let color_raw = patch.color.as_raw();
                let patch_w = patch.mask.width() as usize;

                if let (Some(ox), Some(oy)) = (patch.offset_x, patch.offset_y) {
                    let max_x = (ox + patch.mask.width()).min(base_w);
                    let max_y = (oy + patch.mask.height()).min(base_h);

                    let crop_w = max_x.saturating_sub(ox) as usize;
                    let crop_h = max_y.saturating_sub(oy) as usize;

                    if crop_w == 0 || crop_h == 0 {
                        continue;
                    }

                    let base_w_usize = base_w as usize;
                    let ox_usize = ox as usize;
                    let oy_usize = oy as usize;

                    img_buf
                        .par_chunks_mut(base_w_usize * 3)
                        .enumerate()
                        .skip(oy_usize)
                        .take(crop_h)
                        .for_each(|(y, row)| {
                            let py = y - oy_usize;
                            let patch_row_start = py * patch_w;

                            for x in ox_usize..(ox_usize + crop_w) {
                                let px = x - ox_usize;
                                let mask_idx = patch_row_start + px;
                                let mask_value = mask_raw[mask_idx];

                                if mask_value > 0 {
                                    let color_idx = mask_idx * 3;
                                    let pr_u8 = color_raw[color_idx];
                                    let pg_u8 = color_raw[color_idx + 1];
                                    let pb_u8 = color_raw[color_idx + 2];

                                    let (pr, pg, pb) = get_color(&patch, pr_u8, pg_u8, pb_u8);

                                    let alpha = mask_value as f32 / 255.0;
                                    let one_minus_alpha = 1.0 - alpha;

                                    let base_idx = x * 3;
                                    row[base_idx] = pr * alpha + row[base_idx] * one_minus_alpha;
                                    row[base_idx + 1] =
                                        pg * alpha + row[base_idx + 1] * one_minus_alpha;
                                    row[base_idx + 2] =
                                        pb * alpha + row[base_idx + 2] * one_minus_alpha;
                                }
                            }
                        });
                } else {
                    img_buf
                        .par_chunks_mut((base_w * 3) as usize)
                        .enumerate()
                        .for_each(|(y, row)| {
                            let patch_row_start = y * patch_w;
                            for x in 0..base_w as usize {
                                let mask_idx = patch_row_start + x;
                                let mask_value = mask_raw[mask_idx];
                                if mask_value > 0 {
                                    let color_idx = mask_idx * 3;
                                    let pr_u8 = color_raw[color_idx];
                                    let pg_u8 = color_raw[color_idx + 1];
                                    let pb_u8 = color_raw[color_idx + 2];

                                    let (pr, pg, pb) = get_color(&patch, pr_u8, pg_u8, pb_u8);

                                    let alpha = mask_value as f32 / 255.0;
                                    let one_minus_alpha = 1.0 - alpha;

                                    row[x * 3] = pr * alpha + row[x * 3] * one_minus_alpha;
                                    row[x * 3 + 1] = pg * alpha + row[x * 3 + 1] * one_minus_alpha;
                                    row[x * 3 + 2] = pb * alpha + row[x * 3 + 2] * one_minus_alpha;
                                }
                            }
                        });
                }
            }
        }
        DynamicImage::ImageRgba32F(img_buf) => {
            for patch in decoded_patches {
                let mask_raw = patch.mask.as_raw();
                let color_raw = patch.color.as_raw();
                let patch_w = patch.mask.width() as usize;

                if let (Some(ox), Some(oy)) = (patch.offset_x, patch.offset_y) {
                    let max_x = (ox + patch.mask.width()).min(base_w);
                    let max_y = (oy + patch.mask.height()).min(base_h);

                    let crop_w = max_x.saturating_sub(ox) as usize;
                    let crop_h = max_y.saturating_sub(oy) as usize;

                    if crop_w == 0 || crop_h == 0 {
                        continue;
                    }

                    let base_w_usize = base_w as usize;
                    let ox_usize = ox as usize;
                    let oy_usize = oy as usize;

                    img_buf
                        .par_chunks_mut(base_w_usize * 4)
                        .enumerate()
                        .skip(oy_usize)
                        .take(crop_h)
                        .for_each(|(y, row)| {
                            let py = y - oy_usize;
                            let patch_row_start = py * patch_w;

                            for x in ox_usize..(ox_usize + crop_w) {
                                let px = x - ox_usize;
                                let mask_idx = patch_row_start + px;
                                let mask_value = mask_raw[mask_idx];

                                if mask_value > 0 {
                                    let color_idx = mask_idx * 3;
                                    let pr_u8 = color_raw[color_idx];
                                    let pg_u8 = color_raw[color_idx + 1];
                                    let pb_u8 = color_raw[color_idx + 2];

                                    let (pr, pg, pb) = get_color(&patch, pr_u8, pg_u8, pb_u8);
                                    let alpha = mask_value as f32 / 255.0;
                                    let one_minus_alpha = 1.0 - alpha;

                                    let base_idx = x * 4;
                                    row[base_idx] = pr * alpha + row[base_idx] * one_minus_alpha;
                                    row[base_idx + 1] =
                                        pg * alpha + row[base_idx + 1] * one_minus_alpha;
                                    row[base_idx + 2] =
                                        pb * alpha + row[base_idx + 2] * one_minus_alpha;
                                }
                            }
                        });
                } else {
                    img_buf
                        .par_chunks_mut((base_w * 4) as usize)
                        .enumerate()
                        .for_each(|(y, row)| {
                            let patch_row_start = y * patch_w;
                            for x in 0..base_w as usize {
                                let mask_idx = patch_row_start + x;
                                let mask_value = mask_raw[mask_idx];
                                if mask_value > 0 {
                                    let color_idx = mask_idx * 3;
                                    let pr_u8 = color_raw[color_idx];
                                    let pg_u8 = color_raw[color_idx + 1];
                                    let pb_u8 = color_raw[color_idx + 2];

                                    let (pr, pg, pb) = get_color(&patch, pr_u8, pg_u8, pb_u8);

                                    let alpha = mask_value as f32 / 255.0;
                                    let one_minus_alpha = 1.0 - alpha;

                                    row[x * 4] = pr * alpha + row[x * 4] * one_minus_alpha;
                                    row[x * 4 + 1] = pg * alpha + row[x * 4 + 1] * one_minus_alpha;
                                    row[x * 4 + 2] = pb * alpha + row[x * 4 + 2] * one_minus_alpha;
                                }
                            }
                        });
                }
            }
        }
        _ => {
            let mut rgba32_img = composited_image.to_rgba32f();
            for patch in decoded_patches {
                let mask_raw = patch.mask.as_raw();
                let color_raw = patch.color.as_raw();
                let patch_w = patch.mask.width() as usize;

                if let (Some(ox), Some(oy)) = (patch.offset_x, patch.offset_y) {
                    let max_x = (ox + patch.mask.width()).min(base_w);
                    let max_y = (oy + patch.mask.height()).min(base_h);

                    let crop_w = max_x.saturating_sub(ox) as usize;
                    let crop_h = max_y.saturating_sub(oy) as usize;

                    if crop_w == 0 || crop_h == 0 {
                        continue;
                    }

                    let base_w_usize = base_w as usize;
                    let ox_usize = ox as usize;
                    let oy_usize = oy as usize;

                    rgba32_img
                        .par_chunks_mut(base_w_usize * 4)
                        .enumerate()
                        .skip(oy_usize)
                        .take(crop_h)
                        .for_each(|(y, row)| {
                            let py = y - oy_usize;
                            let patch_row_start = py * patch_w;

                            for x in ox_usize..(ox_usize + crop_w) {
                                let px = x - ox_usize;
                                let mask_idx = patch_row_start + px;
                                let mask_value = mask_raw[mask_idx];

                                if mask_value > 0 {
                                    let color_idx = mask_idx * 3;
                                    let pr_u8 = color_raw[color_idx];
                                    let pg_u8 = color_raw[color_idx + 1];
                                    let pb_u8 = color_raw[color_idx + 2];

                                    let (pr, pg, pb) = get_color(&patch, pr_u8, pg_u8, pb_u8);
                                    let alpha = mask_value as f32 / 255.0;
                                    let one_minus_alpha = 1.0 - alpha;

                                    let base_idx = x * 4;
                                    row[base_idx] = pr * alpha + row[base_idx] * one_minus_alpha;
                                    row[base_idx + 1] =
                                        pg * alpha + row[base_idx + 1] * one_minus_alpha;
                                    row[base_idx + 2] =
                                        pb * alpha + row[base_idx + 2] * one_minus_alpha;
                                }
                            }
                        });
                } else {
                    rgba32_img
                        .par_chunks_mut((base_w * 4) as usize)
                        .enumerate()
                        .for_each(|(y, row)| {
                            let patch_row_start = y * patch_w;
                            for x in 0..base_w as usize {
                                let mask_idx = patch_row_start + x;
                                let mask_value = mask_raw[mask_idx];
                                if mask_value > 0 {
                                    let color_idx = mask_idx * 3;
                                    let pr_u8 = color_raw[color_idx];
                                    let pg_u8 = color_raw[color_idx + 1];
                                    let pb_u8 = color_raw[color_idx + 2];

                                    let (pr, pg, pb) = get_color(&patch, pr_u8, pg_u8, pb_u8);
                                    let alpha = mask_value as f32 / 255.0;
                                    let one_minus_alpha = 1.0 - alpha;

                                    row[x * 4] = pr * alpha + row[x * 4] * one_minus_alpha;
                                    row[x * 4 + 1] = pg * alpha + row[x * 4 + 1] * one_minus_alpha;
                                    row[x * 4 + 2] = pb * alpha + row[x * 4 + 2] * one_minus_alpha;
                                }
                            }
                        });
                }
            }
            composited_image = DynamicImage::ImageRgba32F(rgba32_img);
        }
    }

    Ok(composited_image)
}

#[tauri::command]
pub fn is_image_cached(path: String, state: tauri::State<'_, AppState>) -> bool {
    let (source_path, _) = parse_virtual_path(&path);
    let source_path_str = source_path.to_string_lossy().to_string();
    state
        .decoded_image_cache
        .lock()
        .unwrap()
        .get(&source_path_str)
        .is_some()
}

#[tauri::command]
pub async fn load_image(
    path: String,
    state: tauri::State<'_, AppState>,
    app_handle: tauri::AppHandle,
) -> Result<LoadImageResult, String> {
    let my_generation = state.load_image_generation.fetch_add(1, Ordering::SeqCst) + 1;
    let generation_tracker = state.load_image_generation.clone();
    let cancel_token = Some((generation_tracker.clone(), my_generation));

    {
        *state
            .original_image
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = None;
        // §4.4: clear any prior proxy scale; set again only on the proxy path.
        *state.proxy_scale.lock().unwrap_or_else(|e| e.into_inner()) = None;
        *state
            .cached_preview
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = None;
        *state
            .gpu_image_cache
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = None;
        *state
            .full_warped_cache
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = None;
        crate::cache_utils::clear_preview_stage_caches(&state);

        state
            .mask_cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
        state
            .patch_cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
        state
            .geometry_cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();

        *state
            .denoise_result
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = None;
        *state.hdr_result.lock().unwrap_or_else(|e| e.into_inner()) = None;
        *state
            .panorama_result
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = None;
    }

    let (source_path, sidecar_path) = parse_virtual_path(&path);
    let source_path_str = source_path.to_string_lossy().to_string();

    let metadata: ImageMetadata = crate::exif_processing::load_sidecar(&sidecar_path);

    let settings = load_settings(app_handle.clone()).unwrap_or_default();

    let path_clone = source_path_str.clone();

    let cached_data = state
        .decoded_image_cache
        .lock()
        .unwrap()
        .get(&source_path_str);

    let (pristine_arc, exif_data) = if let Some((cached_img, cached_exif)) = cached_data {
        (cached_img, cached_exif)
    } else {
        if crate::file_management::is_cloud_placeholder(&source_path) {
            // §3.5 guard: a RapidRawCloud sync stub is hydrated in place and
            // the load proceeds on the real bytes. A non-stub placeholder (a
            // macOS iCloud dataless file, or any placeholder with sync off,
            // where `is_stub` is a const `false`) keeps the upstream error.
            if crate::sync::hooks::is_stub(&source_path) {
                // P3 proxy edit mode (§4.4): if a smart preview is present
                // locally, decode the PROXY instead of hydrating, but report
                // the ORIGINAL (journal) dimensions. `proxy_handle` is `None`
                // when sync is off or no proxy is present, so this is inert on
                // the upstream/hydrate path.
                #[cfg(feature = "sync")]
                if let Some(handle) = crate::sync::hooks::proxy_handle(&source_path) {
                    return load_image_from_proxy(
                        path,
                        handle,
                        metadata,
                        settings.clone(),
                        state,
                        my_generation,
                    )
                    .await;
                }
                crate::sync::hooks::ensure_local(&source_path, "load_image")
                    .map_err(|e| e.to_string())?;
            } else {
                return Err(format!(
                    "'{}' is stored in iCloud and hasn't been downloaded yet. Download it in Finder, then try again.",
                    source_path_str
                ));
            }
        }

        let (pristine_img, exif_data_loaded) = tokio::task::spawn_blocking(move || {
            if generation_tracker.load(Ordering::SeqCst) != my_generation {
                return Err("Load cancelled".to_string());
            }

            let result: Result<(DynamicImage, HashMap<String, String>), String> =
                (|| match read_file_mapped(Path::new(&path_clone)) {
                    Ok(mmap) => {
                        if generation_tracker.load(Ordering::SeqCst) != my_generation {
                            return Err("Load cancelled".to_string());
                        }

                        let img = load_base_image_from_bytes(
                            &mmap,
                            &path_clone,
                            false,
                            &settings,
                            cancel_token.clone(),
                        )
                        .map_err(|e| e.to_string())?;
                        let exif = exif_processing::read_exif_data(&path_clone, &mmap);
                        Ok((img, exif))
                    }
                    Err(e) => {
                        log::warn!(
                            "Failed to memory-map file '{}': {}. Falling back to standard read.",
                            path_clone,
                            e
                        );
                        let bytes = fs::read(&path_clone).map_err(|io_err| {
                            format!("Fallback read failed for {}: {}", path_clone, io_err)
                        })?;

                        if generation_tracker.load(Ordering::SeqCst) != my_generation {
                            return Err("Load cancelled".to_string());
                        }

                        let img = load_base_image_from_bytes(
                            &bytes,
                            &path_clone,
                            false,
                            &settings,
                            cancel_token.clone(),
                        )
                        .map_err(|e| e.to_string())?;
                        let exif = exif_processing::read_exif_data(&path_clone, &bytes);
                        Ok((img, exif))
                    }
                })();
            result
        })
        .await
        .map_err(|e| e.to_string())??;

        let arc_img = Arc::new(pristine_img);

        state.decoded_image_cache.lock().unwrap().insert(
            source_path_str.clone(),
            arc_img.clone(),
            exif_data_loaded.clone(),
        );

        (arc_img, exif_data_loaded)
    };

    if state.load_image_generation.load(Ordering::SeqCst) != my_generation {
        return Err("Load cancelled".to_string());
    }

    let is_raw = is_raw_file(&source_path_str);

    if state.load_image_generation.load(Ordering::SeqCst) != my_generation {
        return Err("Load cancelled".to_string());
    }

    let (orig_width, orig_height) = pristine_arc.dimensions();
    let as_shot_white_balance = crate::white_balance::as_shot_white_balance(&source_path_str);

    *state.original_image.lock().unwrap() = Some(LoadedImage {
        path,
        image: pristine_arc,
        is_raw,
        as_shot_white_balance,
    });

    Ok(LoadImageResult {
        width: orig_width,
        height: orig_height,
        metadata,
        exif: exif_data,
        is_raw,
        as_shot_white_balance,
    })
}

#[cfg(test)]
mod proxy_patch_tests {
    //! §4.4 regression: an offset-cropped AI patch stores its offset/size and
    //! its mask/color bitmaps in ORIGINAL pixel space. When `composite_patches_
    //! on_image` runs over a smart-preview PROXY base it must scale that geometry
    //! and those bitmaps by `proxy_scale`; before the fix the offset was applied
    //! verbatim, so on a 2x-downscaled proxy an original-space offset of 600 px
    //! fell past the 500 px proxy edge and the patch was dropped entirely
    //! (crop width saturated to 0) — a grossly displaced / missing patch in the
    //! editor preview. `proxy_scale == 1.0` (original base) must stay a no-op.

    use super::*;
    use image::{DynamicImage, GrayImage, Rgb, RgbImage};

    fn png_b64_rgb(w: u32, h: u32, px: [u8; 3]) -> String {
        let img = RgbImage::from_pixel(w, h, Rgb(px));
        let mut buf = std::io::Cursor::new(Vec::new());
        DynamicImage::ImageRgb8(img)
            .write_to(&mut buf, image::ImageFormat::Png)
            .unwrap();
        general_purpose::STANDARD.encode(buf.into_inner())
    }

    fn png_b64_mask(w: u32, h: u32, v: u8) -> String {
        let img = GrayImage::from_pixel(w, h, image::Luma([v]));
        let mut buf = std::io::Cursor::new(Vec::new());
        DynamicImage::ImageLuma8(img)
            .write_to(&mut buf, image::ImageFormat::Png)
            .unwrap();
        general_purpose::STANDARD.encode(buf.into_inner())
    }

    /// A fully-opaque solid-red cropped patch at ORIGINAL offset (ox, oy) of
    /// size (pw, ph). `isSrgbEncoded=false` so red decodes to linear (1,0,0).
    fn cropped_red_patch(ox: u32, oy: u32, pw: u32, ph: u32) -> Value {
        serde_json::json!({
            "aiPatches": [{
                "visible": true,
                "patchData": {
                    "offsetX": ox,
                    "offsetY": oy,
                    "width": pw,
                    "height": ph,
                    "isSrgbEncoded": false,
                    "color": png_b64_rgb(pw, ph, [255, 0, 0]),
                    "mask": png_b64_mask(pw, ph, 255),
                }
            }]
        })
    }

    fn black_base(w: u32, h: u32) -> DynamicImage {
        DynamicImage::ImageRgb32F(image::ImageBuffer::from_pixel(w, h, Rgb([0.0, 0.0, 0.0])))
    }

    fn is_red(p: &Rgb<f32>) -> bool {
        p[0] > 0.9 && p[1] < 0.1 && p[2] < 0.1
    }
    fn is_black(p: &Rgb<f32>) -> bool {
        p[0] < 0.1 && p[1] < 0.1 && p[2] < 0.1
    }

    #[test]
    fn cropped_patch_is_scaled_into_proxy_space() {
        // Original 1000x1000, proxy 500x500 => proxy_scale 0.5. The patch lives
        // at original [600,800)x[600,800); scaled it must land at proxy
        // [300,400)x[300,400).
        let proxy = black_base(500, 500);
        let adj = cropped_red_patch(600, 600, 200, 200);
        let out = composite_patches_on_image(&proxy, &adj, 0.5).unwrap();
        let rgb = out.to_rgb32f();

        // Center of the scaled patch must be red (before the fix the verbatim
        // offset 600 > 500 saturated crop width to 0 and this stayed black).
        assert!(
            is_red(rgb.get_pixel(350, 350)),
            "scaled patch center must be red; got {:?}",
            rgb.get_pixel(350, 350)
        );
        // Just inside the scaled patch edges.
        assert!(is_red(rgb.get_pixel(305, 305)));
        assert!(is_red(rgb.get_pixel(395, 395)));
        // Outside the scaled patch stays untouched.
        assert!(is_black(rgb.get_pixel(50, 50)));
        assert!(
            is_black(rgb.get_pixel(450, 450)),
            "pixel beyond the scaled patch must stay black"
        );
        // The UNSCALED (buggy) location is off the 500px proxy canvas entirely,
        // so there is nothing to check there — the whole point of the bug.
    }

    #[test]
    fn cropped_patch_unscaled_base_is_unchanged() {
        // proxy_scale == 1.0: the base IS the original, so the offset/bitmaps are
        // used verbatim (export / hydrate / load-from-original parity).
        let base = black_base(1000, 1000);
        let adj = cropped_red_patch(600, 600, 200, 200);
        let out = composite_patches_on_image(&base, &adj, 1.0).unwrap();
        let rgb = out.to_rgb32f();
        assert!(
            is_red(rgb.get_pixel(700, 700)),
            "at scale 1.0 the patch must sit at its original coords"
        );
        assert!(is_black(rgb.get_pixel(300, 300)));
    }
}
