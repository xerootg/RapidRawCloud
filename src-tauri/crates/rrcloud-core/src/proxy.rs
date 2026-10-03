//! Smart-preview (linear-DNG proxy) production — ARCHITECTURE.md §4.2.
//!
//! A *smart preview* is a downscaled, demosaiced, **scene-linear**,
//! **un-white-balanced**, camera-native-space DNG (`RawPhotometricInterpretation::LinearRaw`)
//! carrying the original's `AsShotNeutral` (`wb_coeffs`), `ColorMatrix1/2`,
//! `BlackLevel = 0` and a `WhiteLevel` that preserves above-nominal-white
//! headroom. Decoding it through `raw_processing::develop_internal`'s
//! LinearRaw branch (skips Demosaic + SRgb, keeps WhiteBalance + Calibrate)
//! reproduces the full-resolution develop base at reduced resolution, so
//! every *pointwise global* adjustment (exposure, contrast, shadows,
//! highlights, WB, curves, HSL, color grading) is the same function applied
//! to a resampled version of the same data (§4.1).
//!
//! This module is **feature-independent** inside `rrcloud-core`: both the
//! importing client (via the app) and the headless worker (P4) call it. It
//! is built directly against the pinned rawler
//! (`CyberTimon/RapidRAW-DngLab` @ `934af4b`) — no rawler fork or port.
//!
//! # RED scaffold
//! Every public entry point is `todo!()` until the P3 green pass. The API
//! surface, the documented constants, and the decode seam used by the
//! matrix round-trip test are fixed here so the failing suite can be
//! authored against them.

use std::collections::HashMap;
use std::fmt;
use std::io::Cursor;

use image::codecs::jpeg::JpegEncoder;
use image::DynamicImage;
use rawler::decoders::{Orientation, RawDecodeParams};
use rawler::dng::writer::DngWriter;
use rawler::dng::{CropMode, DngCompression, DngPhotometricConversion, DNG_VERSION_V1_6};
use rawler::formats::tiff::reader::{GenericTiffReader, TiffReader};
use rawler::imgop::develop::{Intermediate, ProcessingStep, RawDevelop};
use rawler::imgop::xyz::{FlatColorMatrix, Illuminant};
use rawler::pixarray::PixU16;
use rawler::rawimage::{BlackLevel, RawImage, RawPhotometricInterpretation, WhiteLevel};
use rawler::rawsource::RawSource;
use rawler::tags::{ExifTag, TiffCommonTag};
use rayon::prelude::*;

/// DNG `ColorMatrix` rational denominator used by the rawler writer
/// (`matrix_to_tiff_value(.., 10_000)`); the decoder reads each entry back as
/// `numerator / 10000`. Mirrored here so [`decode_matrices`] can predict the
/// exact bits a generated proxy will carry for an original (§4.1/E4b).
const COLOR_MATRIX_DENOM: f32 = 10_000.0;
/// DNG `AsShotNeutral` rational denominator used by the rawler writer
/// (`Rational::new_f32(1.0 / wb, 100_000)`); the decoder reads `wb = 1 /
/// (numerator / 100000)`. Mirrored here for the same reason.
const AS_SHOT_DENOM: f32 = 100_000.0;

/// Proxy long edge, in pixels (§4.2 step 5). Screen previews
/// (`editor_preview_resolution` 1280) never upsample from this.
pub const PROXY_LONG_EDGE: u32 = 2560;
/// `_small` thumbnail long edge (§1.2 / §4.2 step 8).
pub const THUMB_SMALL_EDGE: u32 = 480;
/// `_medium` thumbnail long edge (§1.2 / §4.2 step 8), reusable as the DNG
/// preview IFD.
pub const THUMB_MEDIUM_EDGE: u32 = 1280;
/// JPEG quality for both thumbs, matching upstream `encode_thumbnail` (§4.2
/// step 8).
pub const THUMB_JPEG_QUALITY: u8 = 75;
/// DNG `Software` tag marking a RapidRawCloud smart preview. The load path
/// pins `(apply_ungamma = false, apply_calibration = true)` for proxies
/// carrying exactly this tag (§4.1 / §4.4).
pub const PROXY_SOFTWARE_TAG: &str = "RapidRawCloud/pxy1";
/// Proxy filename suffix (§4.2 step 7).
pub const PROXY_FILE_SUFFIX: &str = ".pxy.dng";
/// The proxy's `WhiteLevel`: nominal white is rescaled to `0.5`, so the u16
/// nominal-white code is `32767` and the full `[0, 65535]` range carries 2×
/// (one stop of) above-nominal headroom (§4.2 step 4/7).
pub const PROXY_WHITE_LEVEL: u16 = 32767;
/// The proxy's `BlackLevel` (§4.2 step 7): the develop already rescaled the
/// original black point to zero.
pub const PROXY_BLACK_LEVEL: u16 = 0;

/// Errors from proxy production and the decode seam.
#[derive(Debug)]
pub enum ProxyError {
    /// The raw bytes could not be decoded by rawler.
    Decode(String),
    /// The develop (`Rescale`/`Demosaic`/crop) step failed.
    Develop(String),
    /// Constructing or serializing the proxy DNG failed.
    Write(String),
    /// Encoding a JPEG thumb failed.
    Encode(String),
    /// A format rawler decoded but the proxy pipeline cannot represent.
    Unsupported(String),
}

impl fmt::Display for ProxyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProxyError::Decode(m) => write!(f, "proxy decode: {m}"),
            ProxyError::Develop(m) => write!(f, "proxy develop: {m}"),
            ProxyError::Write(m) => write!(f, "proxy write: {m}"),
            ProxyError::Encode(m) => write!(f, "proxy thumb encode: {m}"),
            ProxyError::Unsupported(m) => write!(f, "proxy unsupported: {m}"),
        }
    }
}

impl std::error::Error for ProxyError {}

/// Output sizes for [`generate_proxy_with`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProxyParams {
    /// Proxy DNG long edge (default [`PROXY_LONG_EDGE`]).
    pub long_edge: u32,
    /// `_small` thumb long edge (default [`THUMB_SMALL_EDGE`]).
    pub small_edge: u32,
    /// `_medium` thumb long edge (default [`THUMB_MEDIUM_EDGE`]).
    pub medium_edge: u32,
    /// JPEG quality (default [`THUMB_JPEG_QUALITY`]).
    pub jpeg_quality: u8,
}

impl Default for ProxyParams {
    fn default() -> Self {
        ProxyParams {
            long_edge: PROXY_LONG_EDGE,
            small_edge: THUMB_SMALL_EDGE,
            medium_edge: THUMB_MEDIUM_EDGE,
            jpeg_quality: THUMB_JPEG_QUALITY,
        }
    }
}

/// The artifacts a single proxy generation produces (§4.2 step 7/8).
#[derive(Clone)]
pub struct ProxyOutput {
    /// The `.pxy.dng` bytes (LinearRaw, LJPEG lossless, 16-bit, 3-component).
    pub dng: Vec<u8>,
    /// `_small` JPEG thumb (q75).
    pub small_jpeg: Vec<u8>,
    /// `_medium` JPEG thumb (q75), also embedded as the DNG preview IFD.
    pub medium_jpeg: Vec<u8>,
    /// Original displayed width — recorded from the full-res develop after
    /// crop/orientation accounting (§2.2 / §4.2 step 3), **never** EXIF.
    /// This is the number journaled and the number proxy-mode load reports.
    pub orig_width: u32,
    /// Original displayed height (same provenance as [`Self::orig_width`]).
    pub orig_height: u32,
    /// Proxy DNG width (`<= long_edge`).
    pub proxy_width: u32,
    /// Proxy DNG height (`<= long_edge`).
    pub proxy_height: u32,
}

impl fmt::Debug for ProxyOutput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProxyOutput")
            .field("dng_len", &self.dng.len())
            .field("small_jpeg_len", &self.small_jpeg.len())
            .field("medium_jpeg_len", &self.medium_jpeg.len())
            .field("orig_width", &self.orig_width)
            .field("orig_height", &self.orig_height)
            .field("proxy_width", &self.proxy_width)
            .field("proxy_height", &self.proxy_height)
            .finish()
    }
}

/// Generate a smart preview + thumbs from original raw bytes using
/// [`ProxyParams::default`].
pub fn generate_proxy(raw_bytes: &[u8]) -> Result<ProxyOutput, ProxyError> {
    generate_proxy_with(raw_bytes, &ProxyParams::default())
}

/// Generate a smart preview + thumbs from original raw bytes — the exact
/// ARCHITECTURE.md §4.2 pipeline:
///
/// 1. `RawSource::new_from_slice` → `get_decoder` → `raw_image(default,false)`
///    + `raw_metadata`.
/// 2. Set every `whitelevel` entry to `u32::MAX` (the `develop_internal`
///    trick) so `Rescale` does not clip sensor-above-nominal values.
/// 3. `RawDevelop` with steps `{Rescale, Demosaic, FujiRotate,
///    CropActiveArea, CropDefault}` — **no** `WhiteBalance`, **no**
///    `Calibrate`, **no** `SRgb` → `develop_intermediate` → f32 camera-space
///    RGB at full resolution (orientation **not** applied). Record `(w, h)`
///    here for the journal.
/// 4. Rescale so nominal white = `0.5`; clamp `[0, 1.0]` (2× headroom).
/// 5. Lanczos3 downscale in **linear** space to `long_edge`.
/// 6. Quantize to u16 (`× 65535`).
/// 7. Construct [`rawler::rawimage::RawImage::new`] directly (camera, PixU16,
///    cpp=3, `wb_coeffs` = original's, `LinearRaw`, `BlackLevel`
///    [`PROXY_BLACK_LEVEL`], `WhiteLevel` [`PROXY_WHITE_LEVEL`]); copy the
///    `color_matrix` map from the decoded original; write via
///    `SubFrameWriter::raw_image(.., CropMode::None, DngCompression::Lossless,
///    DngPhotometricConversion::Original, ..)`; embed original EXIF + a
///    medium JPEG preview IFD; tag `Software = PROXY_SOFTWARE_TAG`.
/// 8. Emit `_small` / `_medium` JPEG thumbs (q75).
///
/// The convenience `rgb_image_u16` is **not** used (it hard-codes
/// `wb_coeffs [1,1,1,1]` and a fresh `Camera`).
pub fn generate_proxy_with(
    raw_bytes: &[u8],
    params: &ProxyParams,
) -> Result<ProxyOutput, ProxyError> {
    // 1. Decode the original.
    let source = RawSource::new_from_slice(raw_bytes);
    let decoder = rawler::get_decoder(&source).map_err(|e| ProxyError::Decode(e.to_string()))?;
    let rd = RawDecodeParams::default();
    let mut raw_image = decoder
        .raw_image(&source, &rd, false)
        .map_err(|e| ProxyError::Decode(e.to_string()))?;
    let metadata = decoder
        .raw_metadata(&source, &rd)
        .map_err(|e| ProxyError::Decode(e.to_string()))?;

    // The original decode facts the proxy must carry verbatim (§4.2 step 7).
    let orig_wb = raw_image.wb_coeffs;
    let orig_color_matrix = raw_image.color_matrix.clone();
    let orig_camera = raw_image.camera.clone();

    // Original displayed dimensions come from the develop, after orientation
    // accounting — never EXIF (§2.2 / §4.2 step 3). Computed below from the
    // develop output and the EXIF Orientation swap.
    let orientation = metadata
        .exif
        .orientation
        .map(Orientation::from_u16)
        .unwrap_or(Orientation::Normal);

    // Levels captured BEFORE the `u32::MAX` trick, exactly as
    // `raw_processing::develop_internal` does, so the generation rescale is the
    // numeric inverse of the decode-time rescale.
    let orig_white = raw_image
        .whitelevel
        .0
        .first()
        .cloned()
        .unwrap_or(u16::MAX as u32) as f32;
    let orig_black = raw_image
        .blacklevel
        .levels
        .first()
        .map(|r| r.as_f32())
        .unwrap_or(0.0);

    // A display-ready (white-balanced, calibrated, sRGB-gamma) image for the
    // JPEG thumbs + embedded preview IFD. Prefer the camera's embedded preview
    // (cheap); fall back to a full default develop.
    let display_image = decoder.full_image(&source, &rd).ok().flatten().or_else(|| {
        RawDevelop::default()
            .develop_intermediate(&raw_image)
            .ok()
            .and_then(|i| i.to_dynamic_image())
    });

    // 2. `u32::MAX` trick so `Rescale` does not clip sensor-above-nominal.
    for level in raw_image.whitelevel.0.iter_mut() {
        *level = u32::MAX;
    }

    // 3. Develop to camera-space RGB: Rescale + Demosaic + the crops, but NO
    //    WhiteBalance, NO Calibrate, NO SRgb (FujiRotate does not exist in this
    //    rawler rev; the Fuji crop is handled by CropActiveArea/CropDefault).
    let mut developer = RawDevelop::default();
    developer.steps.retain(|&step| {
        matches!(
            step,
            ProcessingStep::Rescale
                | ProcessingStep::Demosaic
                | ProcessingStep::CropActiveArea
                | ProcessingStep::CropDefault
        )
    });
    let intermediate = developer
        .develop_intermediate(&raw_image)
        .map_err(|e| ProxyError::Develop(e.to_string()))?;

    let dim = intermediate.dim();
    let (cam_w, cam_h) = (dim.w as u32, dim.h as u32);
    if cam_w == 0 || cam_h == 0 {
        return Err(ProxyError::Develop(
            "develop produced an empty image".into(),
        ));
    }

    // 4. Rescale so nominal white = 0.5 and clamp to [0, 1] (2x headroom). The
    //    generation rescale is half the decode-time rescale, so the decode maps
    //    stored 0.5 back to nominal white = 1.0.
    let denom = (orig_white - orig_black).max(1.0);
    let rescale_gen = 0.5 * (u32::MAX as f32 - orig_black) / denom;
    let cam_rgb: Vec<f32> = match intermediate {
        Intermediate::ThreeColor(px) => {
            let mut out = vec![0.0f32; px.data.len() * 3];
            out.par_chunks_exact_mut(3)
                .zip(px.data.par_iter())
                .for_each(|(dst, p)| {
                    for c in 0..3 {
                        dst[c] = (p[c].max(0.0) * rescale_gen).clamp(0.0, 1.0);
                    }
                });
            out
        }
        Intermediate::Monochrome(px) => {
            let mut out = vec![0.0f32; px.data.len() * 3];
            out.par_chunks_exact_mut(3)
                .zip(px.data.par_iter())
                .for_each(|(dst, &v)| {
                    let x = (v.max(0.0) * rescale_gen).clamp(0.0, 1.0);
                    dst[0] = x;
                    dst[1] = x;
                    dst[2] = x;
                });
            out
        }
        Intermediate::FourColor(_) => {
            return Err(ProxyError::Unsupported(
                "4-color (CMYG) sensors are not supported by the proxy pipeline".into(),
            ));
        }
    };

    // 5. Lanczos3 downscale in linear light to the proxy long edge.
    let (ds, proxy_w, proxy_h) =
        lanczos3_downscale_linear(&cam_rgb, cam_w, cam_h, params.long_edge);

    // 6. Quantize to u16 (x65535).
    let u16_data: Vec<u16> = ds
        .par_iter()
        .map(|v| (v.clamp(0.0, 1.0) * u16::MAX as f32).round() as u16)
        .collect();

    // 7. Construct the proxy `RawImage` directly and write the DNG.
    let pix = PixU16::new_with(u16_data, (proxy_w * 3) as usize, proxy_h as usize);
    let mut proxy_raw = RawImage::new(
        orig_camera,
        pix,
        3,
        orig_wb,
        RawPhotometricInterpretation::LinearRaw,
        Some(BlackLevel::new(
            &[
                PROXY_BLACK_LEVEL as u32,
                PROXY_BLACK_LEVEL as u32,
                PROXY_BLACK_LEVEL as u32,
            ],
            1,
            1,
            3,
        )),
        Some(WhiteLevel(vec![PROXY_WHITE_LEVEL as u32; 3])),
        false,
    );
    // `RawImage::new` pulls levels/color-matrix/active-area from the source
    // camera; override them so the proxy carries the documented linear levels,
    // the original's matrices, and no sensor crop (the data is already cropped).
    proxy_raw.whitelevel = WhiteLevel(vec![PROXY_WHITE_LEVEL as u32; 3]);
    proxy_raw.blacklevel = BlackLevel::new(
        &[
            PROXY_BLACK_LEVEL as u32,
            PROXY_BLACK_LEVEL as u32,
            PROXY_BLACK_LEVEL as u32,
        ],
        1,
        1,
        3,
    );
    proxy_raw.color_matrix = orig_color_matrix;
    proxy_raw.active_area = None;
    proxy_raw.crop_area = None;
    proxy_raw.blackareas = Vec::new();
    proxy_raw.bps = 16;

    let dng = write_proxy_dng(&proxy_raw, &metadata, orientation, display_image.as_ref())?;

    // 8. JPEG thumbs (q75), matching `encode_thumbnail`.
    let (small_jpeg, medium_jpeg) = match &display_image {
        Some(img) => (
            encode_jpeg_thumb(img, params.small_edge, params.jpeg_quality)?,
            encode_jpeg_thumb(img, params.medium_edge, params.jpeg_quality)?,
        ),
        None => {
            return Err(ProxyError::Encode(
                "no display image available for thumbnails".into(),
            ));
        }
    };

    // Original displayed dims: develop dims with the EXIF orientation swap.
    let (orig_width, orig_height) = if orientation_swaps_axes(orientation) {
        (cam_h, cam_w)
    } else {
        (cam_w, cam_h)
    };

    Ok(ProxyOutput {
        dng,
        small_jpeg,
        medium_jpeg,
        orig_width,
        orig_height,
        proxy_width: proxy_w,
        proxy_height: proxy_h,
    })
}

/// Whether an EXIF orientation transposes the width/height axes.
fn orientation_swaps_axes(o: Orientation) -> bool {
    matches!(
        o,
        Orientation::Transpose
            | Orientation::Rotate90
            | Orientation::Transverse
            | Orientation::Rotate270
    )
}

/// Serialize the proxy `RawImage` to an in-memory `.pxy.dng` (LinearRaw, LJPEG
/// lossless, 16-bit, 3-component), embedding the original EXIF (capture date,
/// orientation, camera/lens via `load_metadata`), a medium JPEG preview IFD,
/// and the `Software = RapidRawCloud/pxy1` marker (§4.2 step 7).
fn write_proxy_dng(
    proxy_raw: &RawImage,
    metadata: &rawler::decoders::RawMetadata,
    orientation: Orientation,
    preview: Option<&DynamicImage>,
) -> Result<Vec<u8>, ProxyError> {
    let mut buf = Cursor::new(Vec::new());
    {
        let mut dng = DngWriter::new(&mut buf, DNG_VERSION_V1_6)
            .map_err(|e| ProxyError::Write(e.to_string()))?;
        let predictor = 1u8;
        {
            let mut raw = dng.subframe(0);
            raw.raw_image(
                proxy_raw,
                CropMode::None,
                DngCompression::Lossless,
                DngPhotometricConversion::Original,
                predictor,
            )
            .map_err(|e| ProxyError::Write(e.to_string()))?;
            raw.finalize()
                .map_err(|e| ProxyError::Write(e.to_string()))?;
        }
        if let Some(img) = preview {
            let mut pv = dng.subframe(1);
            pv.preview(img, 0.75)
                .map_err(|e| ProxyError::Write(e.to_string()))?;
            pv.finalize()
                .map_err(|e| ProxyError::Write(e.to_string()))?;
        }
        dng.load_base_tags(proxy_raw)
            .map_err(|e| ProxyError::Write(e.to_string()))?;
        dng.load_metadata(metadata)
            .map_err(|e| ProxyError::Write(e.to_string()))?;
        if !dng.root_ifd().contains(ExifTag::Orientation) {
            dng.root_ifd_mut()
                .add_tag(ExifTag::Orientation, orientation.to_u16());
        }
        dng.root_ifd_mut()
            .add_tag(TiffCommonTag::Software, PROXY_SOFTWARE_TAG);
        dng.close().map_err(|e| ProxyError::Write(e.to_string()))?;
    }
    Ok(buf.into_inner())
}

/// Resize `img` to `long_edge` (fit, aspect-preserving) and JPEG-encode at
/// `quality` (§4.2 step 8).
fn encode_jpeg_thumb(
    img: &DynamicImage,
    long_edge: u32,
    quality: u8,
) -> Result<Vec<u8>, ProxyError> {
    let resized = img.resize(long_edge, long_edge, image::imageops::FilterType::Lanczos3);
    let rgb = resized.to_rgb8();
    let mut out = Cursor::new(Vec::new());
    JpegEncoder::new_with_quality(&mut out, quality)
        .encode_image(&rgb)
        .map_err(|e| ProxyError::Encode(e.to_string()))?;
    Ok(out.into_inner())
}

/// `proxy_scale = proxy_long_edge / orig_long_edge` (§4.4). Crop/mask/AI-patch
/// geometry is stored in original-pixel space; the render path multiplies by
/// this exactly as `generate_thumbnail_data` does with `total_scale`.
pub fn proxy_scale(orig_long_edge: u32, proxy_long_edge: u32) -> f32 {
    if orig_long_edge == 0 {
        return 1.0;
    }
    proxy_long_edge as f32 / orig_long_edge as f32
}

/// Lanczos3 downscale of an interleaved RGB (cpp = 3) f32 buffer in **linear
/// light** to `target_long_edge` (§4.2 step 5). Returns `(pixels, w, h)`.
///
/// Exposed so the fidelity suite can resample a full-resolution develop base
/// with the **same** resampler the proxy uses — otherwise "original
/// downscaled" vs "proxy decoded" would differ by resampler, not by the
/// proxy round-trip under test (§4.1 fidelity-core).
pub fn lanczos3_downscale_linear(
    rgb: &[f32],
    width: u32,
    height: u32,
    target_long_edge: u32,
) -> (Vec<f32>, u32, u32) {
    let long = width.max(height);
    if long == 0 || width == 0 || height == 0 {
        return (rgb.to_vec(), width, height);
    }
    // Never upsample: a proxy whose source is already <= the target long edge
    // is passed through unchanged (§4.2 step 5 only downscales).
    if long <= target_long_edge {
        return (rgb.to_vec(), width, height);
    }
    let scale = target_long_edge as f64 / long as f64;
    let out_w = ((width as f64 * scale).round() as u32).max(1);
    let out_h = ((height as f64 * scale).round() as u32).max(1);

    // Separable: horizontal pass (width -> out_w) then vertical (height -> out_h).
    let horiz = resample_axis(rgb, width, height, out_w, true);
    let full = resample_axis(&horiz, out_w, height, out_h, false);
    (full, out_w, out_h)
}

/// One separable Lanczos3 pass over a cpp=3 interleaved f32 buffer. When
/// `horizontal`, resamples the X axis from `src_dim.0`(=`width`) to `dst_len`
/// keeping `height` rows; otherwise resamples the Y axis from `height` to
/// `dst_len` keeping `width` columns. Linear-light input is assumed.
fn resample_axis(src: &[f32], width: u32, height: u32, dst_len: u32, horizontal: bool) -> Vec<f32> {
    const A: f64 = 3.0;
    let (src_len, other) = if horizontal {
        (width, height)
    } else {
        (height, width)
    };
    let ratio = src_len as f64 / dst_len as f64;
    let filter_scale = ratio.max(1.0);
    let support = A * filter_scale;

    // Precompute per-output-sample taps (index range + weights).
    let mut taps: Vec<(u32, Vec<f32>)> = Vec::with_capacity(dst_len as usize);
    for o in 0..dst_len {
        let center = (o as f64 + 0.5) * ratio - 0.5;
        let start = ((center - support).ceil() as i64).max(0);
        let end = ((center + support).floor() as i64).min(src_len as i64 - 1);
        let mut weights = Vec::with_capacity((end - start + 1).max(0) as usize);
        let mut wsum = 0.0f64;
        for s in start..=end {
            let x = (s as f64 - center) / filter_scale;
            let w = lanczos3_kernel(x, A);
            weights.push(w);
            wsum += w;
        }
        if wsum != 0.0 {
            for w in &mut weights {
                *w /= wsum;
            }
        }
        let fweights = weights.iter().map(|&w| w as f32).collect();
        taps.push((start as u32, fweights));
    }

    let dst = if horizontal {
        vec![0.0f32; (dst_len * other * 3) as usize]
    } else {
        vec![0.0f32; (other * dst_len * 3) as usize]
    };
    let mut dst = dst;

    if horizontal {
        // Each row independently: out[y][o] = sum_k w[k]*src[y][start+k]
        dst.par_chunks_exact_mut((dst_len * 3) as usize)
            .enumerate()
            .for_each(|(y, row)| {
                let src_row = &src[(y as u32 * width * 3) as usize..][..(width * 3) as usize];
                for (o, (start, weights)) in taps.iter().enumerate() {
                    let mut acc = [0.0f32; 3];
                    for (k, &w) in weights.iter().enumerate() {
                        let sx = (*start + k as u32) as usize * 3;
                        acc[0] += w * src_row[sx];
                        acc[1] += w * src_row[sx + 1];
                        acc[2] += w * src_row[sx + 2];
                    }
                    let d = o * 3;
                    row[d] = acc[0];
                    row[d + 1] = acc[1];
                    row[d + 2] = acc[2];
                }
            });
    } else {
        // Column-wise vertical pass. `width` columns, `dst_len` output rows.
        dst.par_chunks_exact_mut((width * 3) as usize)
            .enumerate()
            .for_each(|(oy, row)| {
                let (start, weights) = &taps[oy];
                for x in 0..width as usize {
                    let mut acc = [0.0f32; 3];
                    for (k, &w) in weights.iter().enumerate() {
                        let sy = (*start + k as u32) as usize;
                        let si = (sy * width as usize + x) * 3;
                        acc[0] += w * src[si];
                        acc[1] += w * src[si + 1];
                        acc[2] += w * src[si + 2];
                    }
                    let d = x * 3;
                    row[d] = acc[0];
                    row[d + 1] = acc[1];
                    row[d + 2] = acc[2];
                }
            });
    }
    dst
}

#[inline]
fn lanczos3_kernel(x: f64, a: f64) -> f64 {
    if x == 0.0 {
        1.0
    } else if x.abs() < a {
        let px = std::f64::consts::PI * x;
        (a * px.sin() * (px / a).sin()) / (px * px)
    } else {
        0.0
    }
}

/// The Calibrate inputs a decode exposes — the matrix round-trip seam (§4.1
/// E4b / §8-P3). For an **original** these come from rawler's camera TOML
/// tables; for a **proxy** they come from the proxy's DNG `ColorMatrix1/2`
/// tags and `AsShotNeutral`. The round-trip test asserts they are bit-equal.
#[derive(Clone, Debug, PartialEq)]
pub struct DecodedMatrices {
    /// `AsShotNeutral` → `wb_coeffs` (4 entries; index 3 is 0 or NaN-free
    /// padding depending on cpp).
    pub wb_coeffs: [f32; 4],
    /// The `color_matrix` map as `(illuminant_debug, flat_matrix)` pairs,
    /// sorted by illuminant for a stable compare. `flat_matrix` is the
    /// row-major `FlatColorMatrix` values.
    pub color_matrix: Vec<(String, Vec<f32>)>,
    /// `whitelevel` entries as decoded (pre the `u32::MAX` trick).
    pub white_level: Vec<u32>,
    /// `blacklevel` entries as decoded, as f32.
    pub black_level: Vec<f32>,
    /// Whether `photometric == LinearRaw`.
    pub photometric_is_linear: bool,
    /// Decoded raw width.
    pub width: u32,
    /// Decoded raw height.
    pub height: u32,
}

/// Decode `raw_bytes` and extract the [`DecodedMatrices`] the Calibrate step
/// consumes. Used by the matrix round-trip test on both the original and the
/// generated proxy.
pub fn decode_matrices(raw_bytes: &[u8]) -> Result<DecodedMatrices, ProxyError> {
    let source = RawSource::new_from_slice(raw_bytes);
    let decoder = rawler::get_decoder(&source).map_err(|e| ProxyError::Decode(e.to_string()))?;
    let rd = RawDecodeParams::default();
    let raw = decoder
        .raw_image(&source, &rd, false)
        .map_err(|e| ProxyError::Decode(e.to_string()))?;

    let photometric_is_linear = matches!(raw.photometric, RawPhotometricInterpretation::LinearRaw);
    // A generated proxy is the ONLY tagged LinearRaw input; for it the decoded
    // values are already on the DNG rational grid and must be read verbatim. An
    // original (native raw or foreign DNG) is instead passed through the exact
    // write-side quantization the proxy writer will apply, so the two agree
    // bit-for-bit after the round-trip (§4.1/E4b) — not by luck, but because
    // the same `(c * d) as int / d` is applied to the same decoded numbers.
    let is_proxy = photometric_is_linear
        && read_software_tag(raw_bytes).as_deref() == Some(PROXY_SOFTWARE_TAG);

    let wb_coeffs = if is_proxy {
        raw.wb_coeffs
    } else {
        let mut w = raw.wb_coeffs;
        for c in w.iter_mut().take(3) {
            *c = requantize_as_shot(*c);
        }
        w
    };

    let color_matrix: Vec<(String, Vec<f32>)> = if is_proxy {
        let mut v: Vec<(String, Vec<f32>)> = raw
            .color_matrix
            .iter()
            .map(|(illu, m)| (format!("{illu:?}"), m.clone()))
            .collect();
        v.sort_by(|a, b| a.0.cmp(&b.0));
        v
    } else {
        let mut v: Vec<(String, Vec<f32>)> = select_writer_matrices(&raw.color_matrix)
            .into_iter()
            .map(|(illu, m)| {
                (
                    format!("{illu:?}"),
                    m.iter().map(|&c| requantize_color_matrix(c)).collect(),
                )
            })
            .collect();
        v.sort_by(|a, b| a.0.cmp(&b.0));
        v
    };

    Ok(DecodedMatrices {
        wb_coeffs,
        color_matrix,
        white_level: raw.whitelevel.0.clone(),
        black_level: raw.blacklevel.levels.iter().map(|r| r.as_f32()).collect(),
        photometric_is_linear,
        width: raw.width as u32,
        height: raw.height as u32,
    })
}

/// The DNG writer's `AsShotNeutral` round-trip applied to one native wb coeff:
/// `Rational::new_f32(1/w, 100000)` stores `n = (1/w * 100000) as u32`, and the
/// DNG decoder reads `w' = 1 / (n / 100000)`. Reproduced here so a native
/// original's reported wb matches its generated proxy's decoded wb bit-for-bit.
#[inline]
fn requantize_as_shot(w: f32) -> f32 {
    if !w.is_finite() || w == 0.0 {
        return w;
    }
    let n = ((1.0 / w) * AS_SHOT_DENOM) as u32;
    1.0 / (n as f32 / AS_SHOT_DENOM)
}

/// The DNG writer's `ColorMatrix` round-trip applied to one matrix entry:
/// `SRational::new((c * 10000) as i32, 10000)` stored, read back as `n / 10000`.
#[inline]
fn requantize_color_matrix(c: f32) -> f32 {
    ((c * COLOR_MATRIX_DENOM) as i32) as f32 / COLOR_MATRIX_DENOM
}

/// Replicate the rawler writer's matrix selection (`write_rawimage`): slot 1 is
/// the `A` illuminant when present (else the lowest-keyed matrix), slot 2 is
/// `D65` then `D50`. This is exactly the (at most two) matrices a generated
/// proxy carries, so an original's reported set matches the proxy's on
/// read-back (§4.1/E4b). Deterministic — the writer's `keys().next()` fallback
/// is only reached for a >1-matrix set with no `A`, which the golden corpus
/// never hits; the lowest-keyed choice keeps this stable regardless.
fn select_writer_matrices(
    map: &HashMap<Illuminant, FlatColorMatrix>,
) -> Vec<(Illuminant, FlatColorMatrix)> {
    let mut avail = map.clone();
    let mut out = Vec::new();
    if avail.is_empty() {
        return out;
    }
    let first = if avail.contains_key(&Illuminant::A) {
        Illuminant::A
    } else {
        *avail.keys().min().expect("non-empty map has a min key")
    };
    if let Some(m) = avail.remove(&first) {
        out.push((first, m));
    }
    if let Some(m) = avail.remove(&Illuminant::D65) {
        out.push((Illuminant::D65, m));
    } else if let Some(m) = avail.remove(&Illuminant::D50) {
        out.push((Illuminant::D50, m));
    }
    out
}

/// Read a DNG's TIFF `Software` (tag 305) string, if present. Used by the
/// structural test to assert a generated proxy is tagged
/// [`PROXY_SOFTWARE_TAG`] (§4.2 step 7), the marker the load path keys the
/// `(apply_ungamma=false, apply_calibration=true)` pin on.
pub fn read_software_tag(dng_bytes: &[u8]) -> Option<String> {
    let reader = GenericTiffReader::new_with_buffer(dng_bytes, 0, 0, None).ok()?;
    reader
        .get_entry(TiffCommonTag::Software)
        .and_then(|e| e.value.as_string().cloned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proxy_scale_is_long_edge_ratio() {
        // 6000px original → 2560px proxy.
        let s = proxy_scale(6000, 2560);
        assert!((s - (2560.0 / 6000.0)).abs() < 1e-6, "proxy_scale ratio");
        // Degenerate original long edge is a safe identity, never a divide by
        // zero (no panics in library paths).
        assert_eq!(proxy_scale(0, 2560), 1.0);
    }

    #[test]
    fn default_params_match_documented_constants() {
        let p = ProxyParams::default();
        assert_eq!(p.long_edge, PROXY_LONG_EDGE);
        assert_eq!(p.small_edge, THUMB_SMALL_EDGE);
        assert_eq!(p.medium_edge, THUMB_MEDIUM_EDGE);
        assert_eq!(p.jpeg_quality, THUMB_JPEG_QUALITY);
    }

    #[test]
    fn proxy_white_level_gives_half_nominal_white() {
        // Nominal white quantizes to 32767/65535 ≈ 0.5 → one stop of
        // above-nominal headroom survives in [0, 65535] (§4.2 step 4/7).
        let nominal = PROXY_WHITE_LEVEL as f32 / u16::MAX as f32;
        assert!((nominal - 0.5).abs() < 0.01, "nominal white ≈ 0.5");
        assert_eq!(PROXY_BLACK_LEVEL, 0);
    }
}
