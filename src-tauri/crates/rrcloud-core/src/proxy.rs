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

use std::fmt;

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
    let _ = (raw_bytes, params);
    todo!("P3 green: direct-RawImage::new proxy pipeline (ARCHITECTURE.md §4.2)")
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
    let _ = (rgb, width, height, target_long_edge);
    todo!("P3 green: linear-space Lanczos3 resample (shared with the fidelity test)")
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
    let _ = raw_bytes;
    todo!("P3 green: decode via rawler and read wb_coeffs + color_matrix + levels")
}

/// Read a DNG's TIFF `Software` (tag 305) string, if present. Used by the
/// structural test to assert a generated proxy is tagged
/// [`PROXY_SOFTWARE_TAG`] (§4.2 step 7), the marker the load path keys the
/// `(apply_ungamma=false, apply_calibration=true)` pin on.
pub fn read_software_tag(dng_bytes: &[u8]) -> Option<String> {
    let _ = dng_bytes;
    todo!("P3 green: parse TIFF tag 305 (Software) from the proxy DNG")
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
