//! Quick Cull backend: the two commands the swipe-to-cull overlay needs.
//!
//! Lives under `sync/` because that is where the fork's additive app code
//! goes (root `CLAUDE.md` rule 1), but it is **not** sync-gated: culling is
//! a library feature that works on a build without the `sync` feature too.
//! Only the stub/proxy branches go through the always-compiled
//! `sync::hooks` shims, which are identity passes when sync is off.
//!
//! **Data model.** Quick Cull stores no new field. A decision is
//! upstream's own metadata: *keep* = `rating = max(rating, 3)` with any
//! Reject cleared, *archive* = `flag = Reject`. The frontend computes the
//! target `{rating, flag}` (and keeps the pre-swipe snapshot for undo) and
//! sends exact values here, so keep, archive and undo are all one shape.
//! Both fields are written in **one** `update_sidecar` so the pair can
//! never interleave with an AI-tagging read-modify-write (rule 2). The
//! flag reaches other devices because §2.5 now projects it.
//!
//! **Bandwidth.** 100 MP raws are the norm for this feature's user, so
//! [`preview_jpeg`] never downloads an original unless asked with
//! [`PreviewLevel::Full`] — the frontend sends that only after an explicit
//! "Load full" tap. [`PreviewLevel::Local`] uses only bytes already on the
//! device: the original's embedded JPEG when the original is local, or the
//! embedded preview of the locally cached §4.4 proxy DNG for a stub.
//!
//! **Untrusted input (rule 5).** Paths come from the webview. Every path is
//! required to be absolute, free of `..`, an existing regular file with a
//! supported image extension, and any `?vc=` virtual-copy id must be a
//! short alphanumeric token, before a sidecar is touched or a byte read.

use std::path::{Component, Path, PathBuf};

use image::DynamicImage;
use rayon::prelude::*;
use serde::Deserialize;

use crate::metadata::sidecar::ImageFlag;

/// Most entries one `cull_apply` call may carry. A group approval in
/// group mode is the largest legitimate batch; a 500-frame burst is far
/// below this, and the cap bounds the work a hostile webview can queue.
pub const MAX_APPLY_ENTRIES: usize = 5_000;

/// Long-edge cap of an encoded preview. Above any embedded camera JPEG
/// (a 100 MP frame is ~12 300 px, but its embedded preview is usually
/// ≤ 8 256 px), and keeps the JPEG we ship over IPC to tens of MB.
pub const MAX_PREVIEW_EDGE: u32 = 8_192;

/// Largest original we will read into memory for a preview. 100 MP
/// lossless raws are ~200 MB; this leaves headroom while refusing a
/// pathological file.
pub const MAX_SOURCE_BYTES: u64 = 1024 * 1024 * 1024;

/// One decision to write: exact target values, not a delta.
#[derive(Debug, Clone, Deserialize)]
pub struct CullEntry {
    pub path: String,
    pub rating: u8,
    #[serde(default)]
    pub flag: Option<ImageFlag>,
}

/// How far [`preview_jpeg`] may go to find pixels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PreviewLevel {
    /// Only bytes already on this device. Never touches the network.
    Local,
    /// Hydrate a stub's original first (explicit user request only).
    Full,
}

/// Validates a webview-supplied (possibly virtual) image path and returns
/// `(source_path, sidecar_path)`. See the module doc for the rules.
pub fn validate_image_path(virtual_path: &str) -> Result<(PathBuf, PathBuf), String> {
    if virtual_path.is_empty() || virtual_path.len() > 4096 || virtual_path.contains('\0') {
        return Err("invalid path".into());
    }
    if let Some((_, id)) = virtual_path.rsplit_once("?vc=")
        && (id.is_empty() || id.len() > 32 || !id.chars().all(|c| c.is_ascii_alphanumeric()))
    {
        return Err("invalid virtual copy id".into());
    }
    let (source, sidecar) = crate::library::sidecars::parse_virtual_path(virtual_path);
    if !source.is_absolute() {
        return Err("path must be absolute".into());
    }
    // Checked on the raw string: `Path::components` silently drops interior
    // `.` segments, so it cannot see them.
    if source
        .to_string_lossy()
        .split(['/', '\\'])
        .any(|seg| seg == "." || seg == "..")
        || source
            .components()
            .any(|c| matches!(c, Component::ParentDir))
    {
        return Err("path must not contain '.' or '..' components".into());
    }
    if !crate::decode::formats::is_supported_image_file(&source) {
        return Err("not a supported image".into());
    }
    let meta = std::fs::symlink_metadata(&source).map_err(|_| "no such image".to_string())?;
    if !meta.is_file() {
        return Err("not a regular file".into());
    }
    if sidecar.parent() != source.parent() {
        return Err("sidecar outside the image's directory".into());
    }
    Ok((source, sidecar))
}

/// Writes every entry's `{rating, flag}` atomically per image. All paths
/// are validated before any write, so a single bad path rejects the whole
/// batch (a group approval is all-or-nothing from the user's view).
/// Returns the paths that failed to write (I/O), which the frontend rolls
/// back optimistically.
pub fn apply_entries(
    entries: &[CullEntry],
    xmp: Option<bool>, // Some(create_if_missing) when XMP mirroring is on
) -> Result<Vec<String>, String> {
    if entries.len() > MAX_APPLY_ENTRIES {
        return Err(format!("at most {MAX_APPLY_ENTRIES} entries per call"));
    }
    let mut validated = Vec::with_capacity(entries.len());
    for entry in entries {
        if entry.rating > 5 {
            return Err(format!("rating {} out of range 0..=5", entry.rating));
        }
        let (source, sidecar) = validate_image_path(&entry.path)?;
        validated.push((entry, source, sidecar));
    }
    let failed = validated
        .par_iter()
        .filter_map(|(entry, source, sidecar)| {
            let result = crate::metadata::sidecar::update_sidecar(sidecar, source, |m| {
                m.rating = entry.rating;
                m.flag = entry.flag;
            });
            match result {
                Ok(metadata) => {
                    if let Some(create_if_missing) = xmp {
                        crate::metadata::xmp::sync_metadata_to_xmp(
                            source,
                            sidecar,
                            &metadata,
                            create_if_missing,
                        );
                    }
                    None
                }
                Err(e) => {
                    log::error!("quick cull: failed to update {}: {e}", entry.path);
                    Some(entry.path.clone())
                }
            }
        })
        .collect();
    Ok(failed)
}

/// Browser-decodable non-raw formats whose original bytes are returned
/// as-is (the webview applies EXIF orientation itself).
fn is_browser_native(path: &Path) -> bool {
    path.extension().and_then(|e| e.to_str()).is_some_and(|e| {
        matches!(
            e.to_ascii_lowercase().as_str(),
            "jpg" | "jpeg" | "png" | "webp" | "avif"
        )
    })
}

fn read_bounded(path: &Path) -> Result<Vec<u8>, String> {
    let len = std::fs::metadata(path).map_err(|e| e.to_string())?.len();
    if len == 0 {
        return Err("empty file".into());
    }
    if len > MAX_SOURCE_BYTES {
        return Err("file too large to preview".into());
    }
    std::fs::read(path).map_err(|e| e.to_string())
}

fn encode_jpeg(img: DynamicImage) -> Result<Vec<u8>, String> {
    let img = if img.width().max(img.height()) > MAX_PREVIEW_EDGE {
        img.resize(
            MAX_PREVIEW_EDGE,
            MAX_PREVIEW_EDGE,
            image::imageops::FilterType::Triangle,
        )
    } else {
        img
    };
    let rgb = img.to_rgb8();
    let mut out = Vec::with_capacity(rgb.len() / 6);
    image::codecs::jpeg::JpegEncoder::new_with_quality(&mut out, 90)
        .encode_image(&rgb)
        .map_err(|e| e.to_string())?;
    Ok(out)
}

/// The best inspection pixels for `virtual_path` at `level`, as encoded
/// image bytes (JPEG, or the original bytes of a browser-native file).
///
/// `Err("no-local-preview")` means nothing better than the grid's 1280 px
/// thumbnail is on this device — the frontend keeps showing that and
/// offers "Load full". A raw whose embedded JPEG is small (some bodies
/// embed ~1600 px) still returns it; the frontend falls back to upstream's
/// full render (`generate_preview_for_path`) when the user zooms past it.
pub fn preview_jpeg(virtual_path: &str, level: PreviewLevel) -> Result<Vec<u8>, String> {
    let (source, _) = validate_image_path(virtual_path)?;
    let source_str = source.to_string_lossy().to_string();

    let local = if crate::sync::hooks::is_stub(&source) {
        match level {
            PreviewLevel::Local => {
                let handle = crate::sync::hooks::proxy_handle(&source).ok_or("no-local-preview")?;
                let bytes = read_bounded(&handle.dng_path)?;
                let preview = crate::decode::image_loader::safe_embedded_preview_fallback(
                    &bytes,
                    &handle.dng_path.to_string_lossy(),
                )
                .ok_or("no-local-preview")?;
                return encode_jpeg(preview);
            }
            PreviewLevel::Full => {
                crate::sync::hooks::ensure_local(&source, "quick cull: load full")
                    .map_err(|e| e.to_string())?
            }
        }
    } else {
        source
    };

    let bytes = read_bounded(&local)?;
    if is_browser_native(&local) {
        return Ok(bytes);
    }
    let preview = crate::decode::image_loader::safe_embedded_preview_fallback(&bytes, &source_str)
        .ok_or("no-local-preview")?;
    encode_jpeg(preview)
}

#[tauri::command]
pub async fn cull_apply(
    entries: Vec<CullEntry>,
    app_handle: tauri::AppHandle,
) -> Result<Vec<String>, String> {
    let settings = crate::app::settings::load_settings(app_handle).unwrap_or_default();
    let xmp = settings
        .enable_xmp_sync
        .unwrap_or(false)
        .then(|| settings.create_xmp_if_missing.unwrap_or(false));
    tokio::task::spawn_blocking(move || apply_entries(&entries, xmp))
        .await
        .map_err(|e| e.to_string())?
}

#[tauri::command]
pub async fn cull_preview(
    path: String,
    level: PreviewLevel,
) -> Result<tauri::ipc::Response, String> {
    tokio::task::spawn_blocking(move || preview_jpeg(&path, level))
        .await
        .map_err(|e| e.to_string())?
        .map(tauri::ipc::Response::new)
}
