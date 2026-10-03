//! P3 smart-preview (linear-DNG proxy) suite — the GPU-free, rawler-only
//! half (ARCHITECTURE.md §4.1/§4.2/§8-P3). The develop-pipeline fidelity +
//! GPU tests live in the host app (`src-tauri/tests/proxy_fidelity.rs`),
//! which can reach `raw_processing::develop_internal`'s LinearRaw branch.
//!
//! These pin, against the real golden corpus:
//!   * **Matrix round-trip (E4)** — decode original → build proxy → decode
//!     proxy: the Calibrate inputs (`ColorMatrix1/2`) are **bit-equal** and
//!     `AsShotNeutral` (`wb_coeffs`) matches, across several corpus formats.
//!   * **(w,h) provenance (§2.2)** — the proxy records the original displayed
//!     dimensions (never EXIF); the proxy DNG is `<= 2560 px` long edge with
//!     the original aspect ratio.
//!   * **Proxy structure (§4.2 step 7)** — `LinearRaw` photometric,
//!     `BlackLevel 0`, `WhiteLevel 32767`, `Software = RapidRawCloud/pxy1`.
//!   * **Thumbs (§4.2 step 8)** — `_small` 480 / `_medium` 1280 long edge.
//!
//! Corpus-gated: point `RRCLOUD_RAW_CORPUS` at a directory of CC0 RAWs
//! (default `/tmp/claude-0/raws`). When it is absent every corpus test
//! **skips with a loud eprintln** (so CI without the corpus still passes);
//! when present they RUN.

use std::path::{Path, PathBuf};

use rrcloud_core::proxy::{
    self, DecodedMatrices, ProxyOutput, PROXY_BLACK_LEVEL, PROXY_LONG_EDGE, PROXY_SOFTWARE_TAG,
    PROXY_WHITE_LEVEL, THUMB_MEDIUM_EDGE, THUMB_SMALL_EDGE,
};

/// Corpus directory, or `None` (→ skip). `RRCLOUD_RAW_CORPUS` overrides the
/// default `/tmp/claude-0/raws`.
fn corpus_dir() -> Option<PathBuf> {
    let p = std::env::var("RRCLOUD_RAW_CORPUS")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "/tmp/claude-0/raws".to_string());
    let pb = PathBuf::from(p);
    if pb.is_dir() {
        Some(pb)
    } else {
        None
    }
}

/// A representative file per corpus format. Returns the first that exists.
fn corpus_file(dir: &Path, candidates: &[&str]) -> Option<PathBuf> {
    candidates.iter().map(|n| dir.join(n)).find(|p| p.is_file())
}

/// One representative RAW per format family present in the golden corpus.
fn format_samples(dir: &Path) -> Vec<(&'static str, PathBuf)> {
    let specs: &[(&str, &[&str])] = &[
        (
            "CR3",
            &["Canon_EOS_R6_3_2.CR3", "Canon_EOS_R6_Mark_III_CRAW_3_2.CR3"],
        ),
        (
            "NEF",
            &[
                "Nikon_Z_6_12bit_12bit_compressed_3_2.NEF",
                "Nikon_D40_12bit_12bit_compressed_Lossy_type_1_3_2.NEF",
            ],
        ),
        (
            "ARW",
            &[
                "Sony_ILCE-7M4_14bit_3_2.ARW",
                "Sony_ILCE-7S_14bit_14bit_compressed_3_2.ARW",
            ],
        ),
        ("RAF", &["Fujifilm_X-S10_14bit_14bit_compressed_3_2.RAF"]),
        (
            "DNG",
            &[
                "Google_Pixel_4_XL_16bit_4_3.DNG",
                "Canon_EOS_5D_Mark_III_14bit_14bit_2.3471882640587.DNG",
            ],
        ),
    ];
    specs
        .iter()
        .filter_map(|(fmt, cands)| corpus_file(dir, cands).map(|p| (*fmt, p)))
        .collect()
}

macro_rules! skip_without_corpus {
    ($name:literal) => {
        match corpus_dir() {
            Some(d) => d,
            None => {
                eprintln!(
                    "\n[SKIP] {}: golden RAW corpus not found (set RRCLOUD_RAW_CORPUS; \
                     default /tmp/claude-0/raws). This is a user-run confirmation.\n",
                    $name
                );
                return;
            }
        }
    };
}

fn read(path: &PathBuf) -> Vec<u8> {
    std::fs::read(path).unwrap_or_else(|e| panic!("read corpus file {}: {e}", path.display()))
}

/// E4 / §8-P3: the Calibrate inputs survive the proxy round-trip bit-for-bit.
#[test]
fn matrix_round_trip_bit_equal_across_formats() {
    let dir = skip_without_corpus!("matrix_round_trip_bit_equal_across_formats");
    let samples = format_samples(&dir);
    assert!(
        !samples.is_empty(),
        "corpus present but no known formats found"
    );

    for (fmt, path) in samples {
        let bytes = read(&path);
        let orig: DecodedMatrices =
            proxy::decode_matrices(&bytes).expect("decode original matrices");

        let out: ProxyOutput = proxy::generate_proxy(&bytes).expect("generate proxy");
        let round: DecodedMatrices =
            proxy::decode_matrices(&out.dng).expect("decode proxy matrices");

        // ColorMatrix1/2 bit-equal (compare raw f32 bits, not within epsilon
        // — the proxy writes the decoded original's in-memory matrices, so
        // they must be the SAME numbers, §4.1/E4b).
        assert_eq!(
            orig.color_matrix.len(),
            round.color_matrix.len(),
            "{fmt}: illuminant count changed across round-trip"
        );
        for ((oi, om), (ri, rm)) in orig.color_matrix.iter().zip(round.color_matrix.iter()) {
            assert_eq!(oi, ri, "{fmt}: illuminant key order/identity changed");
            assert_eq!(om.len(), rm.len(), "{fmt}: matrix arity changed for {oi}");
            for (a, b) in om.iter().zip(rm.iter()) {
                assert_eq!(
                    a.to_bits(),
                    b.to_bits(),
                    "{fmt}: ColorMatrix entry not bit-equal ({a} vs {b}) for {oi}"
                );
            }
        }

        // AsShotNeutral / wb_coeffs match (the proxy carries the ORIGINAL
        // decode's wb_coeffs, never [1,1,1,1]).
        for i in 0..3 {
            assert_eq!(
                orig.wb_coeffs[i].to_bits(),
                round.wb_coeffs[i].to_bits(),
                "{fmt}: wb_coeffs[{i}] not bit-equal ({} vs {})",
                orig.wb_coeffs[i],
                round.wb_coeffs[i]
            );
        }
    }
}

/// §4.2 step 7: the proxy is a LinearRaw DNG with the documented levels.
#[test]
fn proxy_is_linear_dng_with_documented_levels() {
    let dir = skip_without_corpus!("proxy_is_linear_dng_with_documented_levels");
    for (fmt, path) in format_samples(&dir) {
        let bytes = read(&path);
        let out = proxy::generate_proxy(&bytes).expect("generate proxy");
        let m = proxy::decode_matrices(&out.dng).expect("decode proxy");

        assert!(m.photometric_is_linear, "{fmt}: proxy is not LinearRaw");
        assert!(
            m.white_level.iter().all(|&w| w == PROXY_WHITE_LEVEL as u32),
            "{fmt}: proxy WhiteLevel != {PROXY_WHITE_LEVEL} (got {:?})",
            m.white_level
        );
        assert!(
            m.black_level.iter().all(|&b| b == PROXY_BLACK_LEVEL as f32),
            "{fmt}: proxy BlackLevel != {PROXY_BLACK_LEVEL} (got {:?})",
            m.black_level
        );

        let software = proxy::read_software_tag(&out.dng);
        assert_eq!(
            software.as_deref(),
            Some(PROXY_SOFTWARE_TAG),
            "{fmt}: proxy Software tag must be {PROXY_SOFTWARE_TAG}"
        );
    }
}

/// §2.2 / §4.4: the proxy records the ORIGINAL displayed dims; the proxy DNG
/// is `<= 2560 px` long edge preserving the original aspect ratio.
#[test]
fn proxy_records_original_dims_and_fits_long_edge() {
    let dir = skip_without_corpus!("proxy_records_original_dims_and_fits_long_edge");
    for (fmt, path) in format_samples(&dir) {
        let bytes = read(&path);
        let out = proxy::generate_proxy(&bytes).expect("generate proxy");

        assert!(
            out.orig_width > 0 && out.orig_height > 0,
            "{fmt}: original dims must be recorded (measured, never EXIF)"
        );
        let proxy_long = out.proxy_width.max(out.proxy_height);
        assert!(
            proxy_long <= PROXY_LONG_EDGE,
            "{fmt}: proxy long edge {proxy_long} exceeds {PROXY_LONG_EDGE}"
        );
        // The original is larger than the proxy target in the corpus, so the
        // long edge should land at the target (within rounding).
        let orig_long = out.orig_width.max(out.orig_height);
        if orig_long >= PROXY_LONG_EDGE {
            assert!(
                proxy_long >= PROXY_LONG_EDGE - 2,
                "{fmt}: proxy long edge {proxy_long} should be ~{PROXY_LONG_EDGE}"
            );
        }
        // Aspect ratio preserved within a pixel of rounding.
        let orig_ar = out.orig_width as f64 / out.orig_height as f64;
        let proxy_ar = out.proxy_width as f64 / out.proxy_height as f64;
        assert!(
            (orig_ar - proxy_ar).abs() < 0.01,
            "{fmt}: aspect ratio drifted (orig {orig_ar:.4} vs proxy {proxy_ar:.4})"
        );

        // proxy_scale is the long-edge ratio and lands in (0, 1].
        let scale = proxy::proxy_scale(orig_long, proxy_long);
        assert!(
            scale > 0.0 && scale <= 1.0,
            "{fmt}: proxy_scale out of range: {scale}"
        );
    }
}

/// §4.2 step 8: thumbs encode at the documented long edges.
#[test]
fn thumbs_emitted_at_documented_sizes() {
    let dir = skip_without_corpus!("thumbs_emitted_at_documented_sizes");
    for (fmt, path) in format_samples(&dir) {
        let bytes = read(&path);
        let out = proxy::generate_proxy(&bytes).expect("generate proxy");

        for (label, jpeg, target) in [
            ("small", &out.small_jpeg, THUMB_SMALL_EDGE),
            ("medium", &out.medium_jpeg, THUMB_MEDIUM_EDGE),
        ] {
            let img = image::load_from_memory(jpeg)
                .unwrap_or_else(|e| panic!("{fmt}: decode {label} thumb: {e}"));
            let long = img.width().max(img.height());
            assert!(
                long <= target && long >= target.saturating_sub(2),
                "{fmt}: {label} thumb long edge {long} != {target}"
            );
        }
    }
}
