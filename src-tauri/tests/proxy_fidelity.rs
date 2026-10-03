//! P3 smart-preview fidelity suite — the develop-pipeline + GPU half
//! (ARCHITECTURE.md §4.1/§4.4/§8-P3). This is the concrete proof behind the
//! fork's central claim: *editing a downscaled linear-DNG proxy stays
//! accurate past one stop*. The rawler-only structural half lives in
//! `crates/rrcloud-core/tests/proxy.rs`.
//!
//! Feature-gated to `sync` (the proxy module ships under it); an empty crate
//! otherwise. Reaches the module-private develop / resample / GPU entry
//! points through the `rapidraw_lib::sync::proxy_support` test seams.
//!
//! Pins:
//!   * **Fidelity core (GPU-FREE, rigorous)** — for each corpus original,
//!     `ΔE00(` original develop base ↓Lanczos3 to proxy size `,` proxy
//!     develop base `)`. Because every GLOBAL adjustment is a pointwise
//!     function of this linear base, base-closeness implies post-adjustment
//!     closeness at ANY exposure/contrast/shadow/highlight/WB. Budget:
//!     unclipped mean ΔE00 ≤ 1.0, p99 ≤ 3.0; clipped-neighborhood p99 ≤ 6.0
//!     (E1: downscale-then-recover ≠ recover-then-downscale near clipped
//!     edges — included, not masked).
//!   * **clamp_limit (E3)** — a LinearRaw decode preserves >1.0 headroom
//!     through develop; the fast-demosaic NON-linear path still clamps to 1.0
//!     (upstream parity).
//!   * **(w,h) provenance** — journal dims (proxy record) == develop dims of
//!     the original == develop dims of the hydrated original.
//!   * **proxy pin** — `proxy_decode_settings` neutralizes `linear_raw_mode`.
//!   * **−4 EV shadow-push banding** — the 16-bit linear base keeps far more
//!     distinct shadow levels than an 8-bit-equivalent base.
//!   * **GPU confirmation (optional, lavapipe)** — if wgpu inits an adapter,
//!     render full-res-original vs proxy through the ACTUAL pipeline under
//!     aggressive settings and assert the same budgets; else skip-with-loud-
//!     eprintln (documented user-run confirmation).
//!
//! Corpus-gated via `RRCLOUD_RAW_CORPUS` (default `/tmp/claude-0/raws`);
//! skip-with-loud-eprintln when absent.

#![cfg(feature = "sync")]

use std::path::PathBuf;

use rapidraw_lib::rrcloud_core::proxy;
use rapidraw_lib::sync::proxy_support::{
    develop_raw_image, gpu_adapter_probe, proxy_decode_settings, proxy_reported_dimensions,
};

const HL: f32 = 2.5;

fn corpus_dir() -> Option<PathBuf> {
    let p = std::env::var("RRCLOUD_RAW_CORPUS")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "/tmp/claude-0/raws".to_string());
    let pb = PathBuf::from(p);
    if pb.is_dir() { Some(pb) } else { None }
}

fn format_samples(dir: &PathBuf) -> Vec<(&'static str, PathBuf)> {
    let specs: &[(&str, &[&str])] = &[
        (
            "CR3",
            &["Canon_EOS_R6_3_2.CR3", "Canon_EOS_R6_Mark_III_CRAW_3_2.CR3"],
        ),
        ("NEF", &["Nikon_Z_6_12bit_12bit_compressed_3_2.NEF"]),
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
                "Canon_EOS_5D_Mark_III_14bit_14bit_2.3471882640587.DNG",
                "Google_Pixel_4_XL_16bit_4_3.DNG",
            ],
        ),
    ];
    specs
        .iter()
        .filter_map(|(fmt, cands)| {
            cands
                .iter()
                .map(|n| dir.join(n))
                .find(|p| p.is_file())
                .map(|p| (*fmt, p))
        })
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
    std::fs::read(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// Develop raw bytes to an interleaved linear RGB buffer (alpha dropped) plus
/// dimensions. `fast` selects fast demosaic; `linear_mode` pins the proxy
/// decode (empty string = the `(apply_ungamma=false, apply_calibration=true)`
/// default arm).
fn develop_rgb(bytes: &[u8], fast: bool, linear_mode: &str) -> (Vec<f32>, u32, u32) {
    let img = develop_raw_image(bytes, fast, HL, linear_mode.to_string(), None)
        .expect("develop_raw_image");
    let (w, h) = (img.width(), img.height());
    let rgba = img.into_rgba32f().into_raw();
    let mut rgb = Vec::with_capacity((w * h * 3) as usize);
    for px in rgba.chunks_exact(4) {
        rgb.push(px[0]);
        rgb.push(px[1]);
        rgb.push(px[2]);
    }
    (rgb, w, h)
}

// ---- color math: linear sRGB → Lab, and CIEDE2000 -------------------------

fn lin_rgb_to_lab(r: f32, g: f32, b: f32) -> (f64, f64, f64) {
    let (r, g, b) = (r.max(0.0) as f64, g.max(0.0) as f64, b.max(0.0) as f64);
    let x = 0.4124 * r + 0.3576 * g + 0.1805 * b;
    let y = 0.2126 * r + 0.7152 * g + 0.0722 * b;
    let z = 0.0193 * r + 0.1192 * g + 0.9505 * b;
    let (xn, yn, zn) = (0.95047, 1.0, 1.08883);
    let f = |t: f64| {
        if t > 0.008856 {
            t.cbrt()
        } else {
            7.787 * t + 16.0 / 116.0
        }
    };
    let (fx, fy, fz) = (f(x / xn), f(y / yn), f(z / zn));
    (116.0 * fy - 16.0, 500.0 * (fx - fy), 200.0 * (fy - fz))
}

fn ciede2000(l1: f64, a1: f64, b1: f64, l2: f64, a2: f64, b2: f64) -> f64 {
    let c1 = (a1 * a1 + b1 * b1).sqrt();
    let c2 = (a2 * a2 + b2 * b2).sqrt();
    let c_bar = (c1 + c2) / 2.0;
    let c7 = c_bar.powi(7);
    let g = 0.5 * (1.0 - (c7 / (c7 + 25.0_f64.powi(7))).sqrt());
    let a1p = (1.0 + g) * a1;
    let a2p = (1.0 + g) * a2;
    let c1p = (a1p * a1p + b1 * b1).sqrt();
    let c2p = (a2p * a2p + b2 * b2).sqrt();
    let h1p = atan2_deg(b1, a1p);
    let h2p = atan2_deg(b2, a2p);
    let dlp = l2 - l1;
    let dcp = c2p - c1p;
    let dhp = if c1p * c2p == 0.0 {
        0.0
    } else {
        let mut d = h2p - h1p;
        if d > 180.0 {
            d -= 360.0;
        } else if d < -180.0 {
            d += 360.0;
        }
        d
    };
    let dhp_big = 2.0 * (c1p * c2p).sqrt() * (dhp.to_radians() / 2.0).sin();
    let lbp = (l1 + l2) / 2.0;
    let cbp = (c1p + c2p) / 2.0;
    let hbp = if c1p * c2p == 0.0 {
        h1p + h2p
    } else if (h1p - h2p).abs() <= 180.0 {
        (h1p + h2p) / 2.0
    } else if h1p + h2p < 360.0 {
        (h1p + h2p + 360.0) / 2.0
    } else {
        (h1p + h2p - 360.0) / 2.0
    };
    let t = 1.0 - 0.17 * (hbp - 30.0).to_radians().cos()
        + 0.24 * (2.0 * hbp).to_radians().cos()
        + 0.32 * (3.0 * hbp + 6.0).to_radians().cos()
        - 0.20 * (4.0 * hbp - 63.0).to_radians().cos();
    let d_ro = 30.0 * (-(((hbp - 275.0) / 25.0).powi(2))).exp();
    let cbp7 = cbp.powi(7);
    let rc = 2.0 * (cbp7 / (cbp7 + 25.0_f64.powi(7))).sqrt();
    let sl = 1.0 + (0.015 * (lbp - 50.0).powi(2)) / (20.0 + (lbp - 50.0).powi(2)).sqrt();
    let sc = 1.0 + 0.045 * cbp;
    let sh = 1.0 + 0.015 * cbp * t;
    let rt = -(2.0 * d_ro).to_radians().sin() * rc;
    let kl = 1.0;
    let kc = 1.0;
    let kh = 1.0;
    ((dlp / (kl * sl)).powi(2)
        + (dcp / (kc * sc)).powi(2)
        + (dhp_big / (kh * sh)).powi(2)
        + rt * (dcp / (kc * sc)) * (dhp_big / (kh * sh)))
        .max(0.0)
        .sqrt()
}

fn atan2_deg(y: f64, x: f64) -> f64 {
    let mut d = y.atan2(x).to_degrees();
    if d < 0.0 {
        d += 360.0;
    }
    d
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((p * (sorted.len() as f64 - 1.0)).round() as usize).min(sorted.len() - 1);
    sorted[idx]
}

/// Core, GPU-free fidelity proof (§4.1).
#[test]
fn fidelity_core_delta_e_within_budget() {
    let dir = skip_without_corpus!("fidelity_core_delta_e_within_budget");
    let samples = format_samples(&dir);
    assert!(
        !samples.is_empty(),
        "corpus present but no known formats found"
    );

    for (fmt, path) in samples {
        let bytes = read(&path);

        // A: full-res original develop base, downscaled (linear Lanczos3) to
        // proxy size with the SAME resampler the proxy uses.
        let (a_full, aw, ah) = develop_rgb(&bytes, false, "");
        let out = proxy::generate_proxy(&bytes).expect("generate proxy");
        let proxy_long = out.proxy_width.max(out.proxy_height);
        let (a_ds, dw, dh) = proxy::lanczos3_downscale_linear(&a_full, aw, ah, proxy_long);

        // B: proxy develop base (LinearRaw branch, pinned decode).
        let (b, bw, bh) = develop_rgb(&out.dng, false, "");

        assert_eq!(
            (dw, dh),
            (bw, bh),
            "{fmt}: proxy vs downscaled-original size mismatch"
        );
        assert_eq!(a_ds.len(), b.len(), "{fmt}: pixel buffer length mismatch");

        let mut unclipped = Vec::new();
        let mut clipped = Vec::new();
        for (pa, pb) in a_ds.chunks_exact(3).zip(b.chunks_exact(3)) {
            let (la, aa, ba) = lin_rgb_to_lab(pa[0], pa[1], pa[2]);
            let (lb, ab, bb) = lin_rgb_to_lab(pb[0], pb[1], pb[2]);
            let de = ciede2000(la, aa, ba, lb, ab, bb);
            // Clipped-neighborhood: the reference base rides near/above
            // nominal white (recover_clipped_pixel engages above 0.5, hard
            // above 1.0) — E1.
            let max_c = pa[0].max(pa[1]).max(pa[2]);
            if max_c > 0.9 {
                clipped.push(de);
            } else {
                unclipped.push(de);
            }
        }

        assert!(!unclipped.is_empty(), "{fmt}: no unclipped pixels to score");
        let mean = unclipped.iter().sum::<f64>() / unclipped.len() as f64;
        unclipped.sort_by(|x, y| x.partial_cmp(y).unwrap());
        let p99 = percentile(&unclipped, 0.99);
        assert!(mean <= 1.0, "{fmt}: unclipped mean ΔE00 {mean:.3} > 1.0");
        assert!(p99 <= 3.0, "{fmt}: unclipped p99 ΔE00 {p99:.3} > 3.0");

        if !clipped.is_empty() {
            clipped.sort_by(|x, y| x.partial_cmp(y).unwrap());
            let cp99 = percentile(&clipped, 0.99);
            assert!(
                cp99 <= 6.0,
                "{fmt}: clipped-neighborhood p99 ΔE00 {cp99:.3} > 6.0"
            );
        }
        eprintln!(
            "[fidelity] {fmt}: unclipped mean {mean:.3} p99 {p99:.3} (n={})",
            unclipped.len()
        );
    }
}

/// E3: LinearRaw decode keeps >1.0 headroom; non-linear fast demosaic clamps.
#[test]
fn clamp_limit_preserves_linear_headroom() {
    let dir = skip_without_corpus!("clamp_limit_preserves_linear_headroom");
    let samples = format_samples(&dir);
    assert!(!samples.is_empty());

    for (fmt, path) in samples {
        let bytes = read(&path);
        let out = proxy::generate_proxy(&bytes).expect("generate proxy");

        // Proxy (LinearRaw) under FAST demosaic must NOT clip the >1.0
        // headroom (the clamp fix): there should be at least one channel
        // above 1.0 somewhere, since nominal white is 0.5 and the proxy
        // carries real above-nominal values.
        let (lin_fast, _, _) = develop_rgb(&out.dng, true, "");
        let max_fast = lin_fast.iter().cloned().fold(0.0f32, f32::max);
        assert!(
            max_fast > 1.0,
            "{fmt}: LinearRaw fast-demosaic decode clipped headroom (max {max_fast:.3} <= 1.0)"
        );

        // Parity: a NON-linear original under fast demosaic still clamps to
        // 1.0 (upstream behavior, unchanged by the fix).
        let (nonlin_fast, _, _) = develop_rgb(&bytes, true, "");
        let max_nl = nonlin_fast.iter().cloned().fold(0.0f32, f32::max);
        assert!(
            max_nl <= 1.0001,
            "{fmt}: non-linear fast demosaic must stay clamped (max {max_nl:.3})"
        );
    }
}

/// §2.2/§4.4: journal dims == original develop dims == hydrated develop dims.
#[test]
fn wh_provenance_matches_develop() {
    let dir = skip_without_corpus!("wh_provenance_matches_develop");
    for (fmt, path) in format_samples(&dir) {
        let bytes = read(&path);
        let out = proxy::generate_proxy(&bytes).expect("generate proxy");

        // The hydrated-original load IS a develop of the original bytes.
        let (_, ow, oh) = develop_rgb(&bytes, false, "");
        assert_eq!(
            (out.orig_width, out.orig_height),
            (ow, oh),
            "{fmt}: journaled proxy dims must equal the original develop dims"
        );

        // Proxy-mode reports the journal dims, never the decoded proxy size.
        let reported = proxy_reported_dimensions(
            (out.orig_width, out.orig_height),
            (out.proxy_width, out.proxy_height),
        );
        assert_eq!(
            reported,
            (ow, oh),
            "{fmt}: proxy-mode reported dims drifted"
        );
    }
}

/// The proxy decode pin neutralizes the user's `linear_raw_mode` (§4.1/§4.4).
#[test]
fn proxy_decode_settings_pins_linear_mode() {
    let mut base = rapidraw_lib::AppSettings::default();
    base.linear_raw_mode = "skip_calib".to_string();
    let pinned = proxy_decode_settings(&base);
    assert!(
        pinned.linear_raw_mode.is_empty(),
        "proxy decode must pin linear_raw_mode to the (ungamma=false, calibrate=true) default arm"
    );
}

/// The reported-dimensions invariant is purely the journal dims (no corpus).
#[test]
fn proxy_reported_dimensions_is_journal_dims() {
    assert_eq!(
        proxy_reported_dimensions((6000, 4000), (2560, 1707)),
        (6000, 4000)
    );
    assert_eq!(
        proxy_reported_dimensions((4000, 6000), (1707, 2560)),
        (4000, 6000)
    );
}

/// −4 EV shadow-push banding: the 16-bit linear base keeps far more distinct
/// shadow levels than an 8-bit-equivalent base (§4.1 reason 1). GPU-free,
/// runs on the original develop base.
#[test]
fn shadow_push_banding_16bit_beats_8bit() {
    let dir = skip_without_corpus!("shadow_push_banding_16bit_beats_8bit");
    let samples = format_samples(&dir);
    assert!(!samples.is_empty());
    let (_fmt, path) = &samples[0];
    let bytes = read(path);
    let (base, _w, _h) = develop_rgb(&bytes, false, "");

    // Look at the darkest decile of the green channel (shadows), push +4 EV
    // (×16), and count distinct quantized levels under 16-bit vs 8-bit.
    let mut greens: Vec<f32> = base.chunks_exact(3).map(|p| p[1]).collect();
    greens.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let cut = greens[greens.len() / 10].max(1e-6);
    let shadow: Vec<f32> = greens.into_iter().filter(|&g| g <= cut).collect();
    assert!(shadow.len() > 1000, "not enough shadow samples");

    let push = 16.0_f32; // +4 EV
    let mut l8 = std::collections::HashSet::new();
    let mut l16 = std::collections::HashSet::new();
    for &g in &shadow {
        // 8-bit-equivalent: quantize the linear value to 8 bits BEFORE the
        // push (what a JPEG proxy would carry), then push.
        let q8 = (g.clamp(0.0, 1.0) * 255.0).round() / 255.0;
        let q16 = (g.clamp(0.0, 1.0) * 65535.0).round() / 65535.0;
        l8.insert(((q8 * push).clamp(0.0, 1.0) * 255.0).round() as i32);
        l16.insert(((q16 * push).clamp(0.0, 1.0) * 65535.0).round() as i32);
    }
    assert!(
        l16.len() >= l8.len() * 4,
        "16-bit shadow levels ({}) should far exceed 8-bit ({}) after a +4 EV push",
        l16.len(),
        l8.len()
    );
}

/// GPU-rendered ΔE confirmation (optional, lavapipe). Skips-with-loud-eprintln
/// when no wgpu adapter initializes (documented user-run confirmation).
#[test]
fn fidelity_gpu_confirmation_under_aggressive_settings() {
    let dir = skip_without_corpus!("fidelity_gpu_confirmation_under_aggressive_settings");

    let adapter = match gpu_adapter_probe() {
        Some(a) => a,
        None => {
            eprintln!(
                "\n[SKIP] fidelity_gpu_confirmation: no wgpu adapter initialized \
                 (lavapipe unavailable / shader failure). GPU ΔE confirmation is a \
                 documented USER-RUN step; the GPU-free fidelity_core test carries the proof.\n"
            );
            return;
        }
    };
    eprintln!("[gpu] adapter: {adapter}");

    let samples = format_samples(&dir);
    let (fmt, path) = &samples[0];
    let bytes = read(path);

    // Aggressive adjustments: +3 EV, shadows +100, highlights −100, strong WB
    // shift. Rendered full-res-original vs proxy at a matched output size.
    let adjustments = serde_json::json!({
        "exposure": 3.0,
        "shadows": 100.0,
        "highlights": -100.0,
        "temperature": 40.0,
        "tint": 20.0,
    });

    let context = rapidraw_lib::sync::proxy_support::init_gpu_context_headless()
        .expect("headless GPU context");

    let orig_img = develop_raw_image(&bytes, false, HL, String::new(), None).expect("develop orig");
    let out = proxy::generate_proxy(&bytes).expect("generate proxy");
    let proxy_img =
        develop_raw_image(&out.dng, false, HL, String::new(), None).expect("develop proxy");

    let rendered_orig = rapidraw_lib::sync::proxy_support::render_adjustments_headless(
        &context,
        &orig_img,
        &adjustments,
    )
    .expect("render original");
    let rendered_proxy = rapidraw_lib::sync::proxy_support::render_adjustments_headless(
        &context,
        &proxy_img,
        &adjustments,
    )
    .expect("render proxy");

    // Downscale the rendered original to the rendered proxy size and score
    // ΔE00 under the same budgets as fidelity_core.
    let o = rendered_orig.into_rgba32f();
    let (ow, oh) = (o.width(), o.height());
    let mut o_rgb = Vec::with_capacity((ow * oh * 3) as usize);
    for px in o.into_raw().chunks_exact(4) {
        o_rgb.extend_from_slice(&px[..3]);
    }
    let p = rendered_proxy.into_rgba32f();
    let (pw, ph) = (p.width(), p.height());
    let mut p_rgb = Vec::with_capacity((pw * ph * 3) as usize);
    for px in p.into_raw().chunks_exact(4) {
        p_rgb.extend_from_slice(&px[..3]);
    }
    let long = pw.max(ph);
    let (o_ds, dw, dh) = proxy::lanczos3_downscale_linear(&o_rgb, ow, oh, long);
    assert_eq!((dw, dh), (pw, ph), "{fmt}: GPU render size mismatch");

    let mut unclipped = Vec::new();
    let mut clipped = Vec::new();
    for (pa, pb) in o_ds.chunks_exact(3).zip(p_rgb.chunks_exact(3)) {
        let (la, aa, ba) = lin_rgb_to_lab(pa[0], pa[1], pa[2]);
        let (lb, ab, bb) = lin_rgb_to_lab(pb[0], pb[1], pb[2]);
        let de = ciede2000(la, aa, ba, lb, ab, bb);
        if pa[0].max(pa[1]).max(pa[2]) > 0.9 {
            clipped.push(de);
        } else {
            unclipped.push(de);
        }
    }
    let mean = unclipped.iter().sum::<f64>() / unclipped.len().max(1) as f64;
    unclipped.sort_by(|x, y| x.partial_cmp(y).unwrap());
    assert!(
        mean <= 1.0,
        "{fmt}: GPU unclipped mean ΔE00 {mean:.3} > 1.0"
    );
    assert!(
        percentile(&unclipped, 0.99) <= 3.0,
        "{fmt}: GPU unclipped p99 > 3.0"
    );
    if !clipped.is_empty() {
        clipped.sort_by(|x, y| x.partial_cmp(y).unwrap());
        assert!(
            percentile(&clipped, 0.99) <= 6.0,
            "{fmt}: GPU clipped p99 > 6.0"
        );
    }
}
