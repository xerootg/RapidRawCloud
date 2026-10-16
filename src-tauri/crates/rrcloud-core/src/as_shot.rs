//! The original's as-shot **exposure** a smart preview carries
//! (ARCHITECTURE.md §4.2 step 7, §4.4).
//!
//! The app renders every RAW with an exposure compensation it derives from
//! the original file (upstream `raw_processing::read_as_shot` →
//! `baseline_exposure::read`): the DNG `BaselineExposure` (+ `Offset`),
//! whether the data is scene-referred, and the camera's
//! highlight-preservation bias, read from the maker notes (Nikon Active
//! D-Lighting, Canon ALO/HTP, Fuji DR, Olympus gradation, Pentax DR
//! expansion). Highlight-preservation compensation is on by default.
//!
//! A proxy already carries what the as-shot *white balance* needs — the
//! original's `wb_coeffs` and color matrices (§4.2 step 7) — but not the
//! maker notes: decoded on its own it reports no highlight preservation, so
//! a stub edited through it rendered up to several stops away from its
//! original. Proxy generation therefore computes the original's exposure
//! here and stores it in the proxy as a [`AsShotExposure`] record, which the
//! app's proxy-mode as-shot reader takes in place of the proxy's own.
//!
//! **Parity by construction.** The computation is upstream's own
//! `src-tauri/src/baseline_exposure.rs`, compiled into this crate verbatim
//! (`#[path]`, against the same rawler lineage) and fed exactly what
//! `read_as_shot` feeds it, rather than a port that could drift. Its one
//! app-crate import, `crate::app_settings::AppSettings` (used only by
//! `BaselineOptions::from_settings`, which this crate never calls), is met
//! by a field-only shim in `lib.rs`. If upstream gives the file another
//! app-crate import, this crate stops compiling until the shim covers it;
//! the worker image and the `rrcloud` CI path filter both name the file for
//! that reason.
//!
//! **Record** — IFD0 `DNGPrivateData` (tag 50740, BYTE), the DNG field a
//! writer uses for its own raw converter's data:
//!
//! | bytes | content |
//! |---|---|
//! | `0..14` | `RapidRawCloud\0` — the null-terminated creator id the DNG spec requires |
//! | `14` | record version, `1` |
//! | `15` | flags: bit 0 = `scene_referred` |
//! | `16..20` | `dng_baseline`, f32 little-endian (EV) |
//! | `20..24` | `highlight_preservation`, f32 little-endian (EV) |
//!
//! A proxy without the record (generated before it existed) decodes to
//! `None`. The record is read back from synced, untrusted bytes, so
//! [`AsShotExposure::decode`] accepts only that exact layout with finite,
//! in-range values.

use std::ops::RangeInclusive;

use rawler::decoders::Decoder;
use rawler::rawimage::{RawImage, RawPhotometricInterpretation};

// Upstream's maker-note / baseline-exposure reader, verbatim. Only `read`
// and `RawExposure` are used here (`BaselineOptions` is the app's). Not
// formatted from here: the file is upstream's, in the app crate's edition
// 2024 style, which this edition-2021 crate's rustfmt would rewrite.
#[path = "../../../src/baseline_exposure.rs"]
#[allow(dead_code)]
#[rustfmt::skip]
mod upstream;

/// The DNG spec's null-terminated creator id that opens a `DNGPrivateData`
/// payload.
const RECORD_CREATOR: &[u8] = b"RapidRawCloud\0";
/// Record layout version (see the module table).
const RECORD_VERSION: u8 = 1;
/// Creator id + version + flags + two f32s.
const RECORD_LEN: usize = RECORD_CREATOR.len() + 2 + 4 + 4;
const FLAG_SCENE_REFERRED: u8 = 0b1;
/// Upstream clamps highlight preservation to this range
/// (`HIGHLIGHT_BIAS_MIN..=HIGHLIGHT_BIAS_MAX`), so a record outside it was
/// not written by this module.
const HIGHLIGHT_PRESERVATION_RANGE: RangeInclusive<f32> = -1.0..=4.0;
/// `BaselineExposure` is a free SRATIONAL and real cameras stay within a few
/// EV; this only stops a hostile record from driving the render to a
/// degenerate exposure.
const DNG_BASELINE_RANGE: RangeInclusive<f32> = -16.0..=16.0;

/// The exposure half of upstream's `AsShot` (field for field its
/// `baseline_exposure::RawExposure`), as computed from an original RAW.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct AsShotExposure {
    /// The data is scene-referred (not monochrome): upstream adds the
    /// user's baseline-exposure amount only when this is set.
    pub scene_referred: bool,
    /// DNG `BaselineExposure` + `BaselineExposureOffset` (EV).
    pub dng_baseline: f32,
    /// The camera's highlight-preservation bias from the maker notes (EV,
    /// upstream-clamped to `-1..=4`).
    pub highlight_preservation: f32,
}

impl AsShotExposure {
    /// Computes the original's exposure exactly as upstream `read_as_shot`
    /// does: same monochrome test on the decoded `RawImage`, then upstream's
    /// own `baseline_exposure::read` on the decoder and the original bytes.
    pub fn from_original(decoder: &dyn Decoder, raw_bytes: &[u8], raw_image: &RawImage) -> Self {
        let is_monochrome = raw_image.cpp == 1
            && !matches!(raw_image.photometric, RawPhotometricInterpretation::Cfa(_));
        let exposure = upstream::read(decoder, raw_bytes, is_monochrome);
        AsShotExposure {
            scene_referred: exposure.scene_referred,
            dng_baseline: exposure.dng_baseline,
            highlight_preservation: exposure.highlight_preservation,
        }
    }

    /// The `DNGPrivateData` payload for this record (module table).
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(RECORD_LEN);
        out.extend_from_slice(RECORD_CREATOR);
        out.push(RECORD_VERSION);
        out.push(if self.scene_referred {
            FLAG_SCENE_REFERRED
        } else {
            0
        });
        out.extend_from_slice(&self.dng_baseline.to_le_bytes());
        out.extend_from_slice(&self.highlight_preservation.to_le_bytes());
        out
    }

    /// Parses a `DNGPrivateData` payload written by [`Self::encode`]. `None`
    /// for anything else: another writer's private data, a different
    /// version or length, unknown flags, or a non-finite or out-of-range
    /// value.
    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.len() != RECORD_LEN {
            return None;
        }
        let rest = data.strip_prefix(RECORD_CREATOR)?;
        let (&version, rest) = rest.split_first()?;
        let (&flags, rest) = rest.split_first()?;
        if version != RECORD_VERSION || flags & !FLAG_SCENE_REFERRED != 0 {
            return None;
        }
        let dng_baseline = f32::from_le_bytes(rest.get(0..4)?.try_into().ok()?);
        let highlight_preservation = f32::from_le_bytes(rest.get(4..8)?.try_into().ok()?);
        // `RangeInclusive::contains` is false for NaN, so this also rejects
        // non-finite values.
        if !DNG_BASELINE_RANGE.contains(&dng_baseline)
            || !HIGHLIGHT_PRESERVATION_RANGE.contains(&highlight_preservation)
        {
            return None;
        }
        Some(AsShotExposure {
            scene_referred: flags & FLAG_SCENE_REFERRED != 0,
            dng_baseline,
            highlight_preservation,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> AsShotExposure {
        AsShotExposure {
            scene_referred: true,
            dng_baseline: -0.35,
            highlight_preservation: 0.66,
        }
    }

    #[test]
    fn record_round_trips() {
        for e in [
            sample(),
            AsShotExposure {
                scene_referred: false,
                dng_baseline: 0.0,
                highlight_preservation: 0.0,
            },
            AsShotExposure {
                scene_referred: true,
                dng_baseline: 16.0,
                highlight_preservation: 4.0,
            },
        ] {
            let bytes = e.encode();
            assert_eq!(bytes.len(), RECORD_LEN);
            assert!(bytes.starts_with(b"RapidRawCloud\0"));
            assert_eq!(AsShotExposure::decode(&bytes), Some(e));
        }
    }

    /// The record is read from synced bytes: every malformed or foreign
    /// payload must decode to `None`, never to a value the render would use.
    #[test]
    fn decode_rejects_foreign_or_malformed_payloads() {
        let good = sample().encode();

        // Another writer's private data (the spec's "Adobe\0MakN..." form).
        let mut adobe = b"Adobe\0MakN".to_vec();
        adobe.resize(RECORD_LEN, 0);
        assert_eq!(AsShotExposure::decode(&adobe), None);

        // Truncated and over-long payloads.
        assert_eq!(AsShotExposure::decode(&good[..RECORD_LEN - 1]), None);
        let mut long = good.clone();
        long.push(0);
        assert_eq!(AsShotExposure::decode(&long), None);
        assert_eq!(AsShotExposure::decode(&[]), None);

        // Unknown version and unknown flag bits.
        let mut version = good.clone();
        version[RECORD_CREATOR.len()] = 2;
        assert_eq!(AsShotExposure::decode(&version), None);
        let mut flags = good.clone();
        flags[RECORD_CREATOR.len() + 1] = 0b10;
        assert_eq!(AsShotExposure::decode(&flags), None);

        // Non-finite and out-of-range values.
        let at = |off: usize, v: f32| {
            let mut b = good.clone();
            let start = RECORD_CREATOR.len() + 2 + off;
            b[start..start + 4].copy_from_slice(&v.to_le_bytes());
            b
        };
        for v in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY, 16.5, -17.0] {
            assert_eq!(AsShotExposure::decode(&at(0, v)), None, "dng_baseline {v}");
        }
        for v in [f32::NAN, f32::INFINITY, 4.01, -1.5] {
            assert_eq!(
                AsShotExposure::decode(&at(4, v)),
                None,
                "highlight_preservation {v}"
            );
        }
    }
}
