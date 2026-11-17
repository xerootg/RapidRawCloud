//! Quick Cull backend (`sync::cull`): webview-path validation, atomic
//! keep/archive/undo writes, and the bandwidth rule that a preview never
//! downloads an original unless explicitly asked (`PreviewLevel::Full`).
//!
//! The pure functions are driven directly (no `AppHandle` needed). The
//! stub test is Garage-backed and `sync`-gated like `stubs_guard_sites.rs`.

use std::fs;
use std::path::{Path, PathBuf};

use rapidraw_lib::sync::cull::{
    CullEntry, MAX_APPLY_ENTRIES, PreviewLevel, apply_entries, preview_jpeg, validate_image_path,
};
use serde_json::{Value, json};

fn tiny_jpeg() -> Vec<u8> {
    let img = image::RgbImage::from_fn(8, 6, |x, y| image::Rgb([x as u8 * 30, y as u8 * 40, 90]));
    let mut out = Vec::new();
    image::codecs::jpeg::JpegEncoder::new_with_quality(&mut out, 90)
        .encode_image(&img)
        .expect("encode");
    out
}

fn image_in(dir: &Path, name: &str) -> PathBuf {
    let p = dir.join(name);
    fs::write(&p, tiny_jpeg()).expect("write image");
    p
}

fn sidecar_of(p: &Path) -> PathBuf {
    let mut s = p.as_os_str().to_owned();
    s.push(".rrdata");
    PathBuf::from(s)
}

fn read_sidecar(p: &Path) -> Value {
    serde_json::from_slice(&fs::read(sidecar_of(p)).expect("sidecar exists")).expect("json")
}

fn entry(path: &Path, rating: u8, flag: Option<&str>) -> CullEntry {
    serde_json::from_value(json!({
        "path": path.to_string_lossy(),
        "rating": rating,
        "flag": flag,
    }))
    .expect("entry")
}

// ---------------------------------------------------------------------------
// Path validation (rule 5: webview input is untrusted)
// ---------------------------------------------------------------------------

#[test]
fn validation_rejects_hostile_and_non_image_paths() {
    let dir = tempfile::tempdir().unwrap();
    let ok = image_in(dir.path(), "a.jpg");
    fs::create_dir(dir.path().join("folder.jpg")).unwrap();
    fs::write(dir.path().join("notes.txt"), b"x").unwrap();
    let d = dir.path().to_string_lossy();

    assert!(validate_image_path(&ok.to_string_lossy()).is_ok());
    for bad in [
        String::new(),
        "a.jpg".to_string(),             // relative
        format!("{d}/../{}/a.jpg", d),   // parent component
        format!("{d}/./a.jpg"),          // cur-dir component
        format!("{d}/missing.jpg"),      // nonexistent
        format!("{d}/folder.jpg"),       // directory
        format!("{d}/notes.txt"),        // not an image
        format!("{d}/a.jpg?vc=../../x"), // hostile vc id
        format!("{d}/a.jpg?vc="),        // empty vc id
        format!("{d}/a.jpg\0.png"),      // NUL
    ] {
        assert!(validate_image_path(&bad).is_err(), "must reject {bad:?}");
    }
    let vc = validate_image_path(&format!("{d}/a.jpg?vc=ab12cd")).expect("valid vc");
    assert_eq!(vc.0, ok);
    assert_eq!(vc.1.parent(), ok.parent());
}

// ---------------------------------------------------------------------------
// Atomic writes: keep, archive, undo
// ---------------------------------------------------------------------------

#[test]
fn keep_archive_and_undo_write_exact_values_and_preserve_other_fields() {
    let dir = tempfile::tempdir().unwrap();
    let img = image_in(dir.path(), "IMG_1.jpg");
    fs::write(
        sidecar_of(&img),
        serde_json::to_vec(&json!({
            "version": 1, "rating": 4, "tags": ["user:dog"],
            "adjustments": { "exposure": 0.7 }
        }))
        .unwrap(),
    )
    .unwrap();

    // Archive: Reject, rating untouched.
    assert!(
        apply_entries(&[entry(&img, 4, Some("reject"))], None)
            .unwrap()
            .is_empty()
    );
    let s = read_sidecar(&img);
    assert_eq!(s["flag"], "reject");
    assert_eq!(s["rating"], 4);
    assert_eq!(s["tags"], json!(["user:dog"]));
    assert_eq!(s["adjustments"]["exposure"], 0.7);

    // Undo: restore the exact pre-swipe snapshot.
    apply_entries(&[entry(&img, 4, None)], None).unwrap();
    let s = read_sidecar(&img);
    assert!(s.get("flag").is_none(), "flag cleared on undo: {s}");
    assert_eq!(s["rating"], 4);
}

#[test]
fn an_archived_unedited_image_keeps_its_sidecar() {
    // Upstream deletes a sidecar that is back to defaults; a Reject flag
    // must count as non-default or the archive decision would vanish.
    let dir = tempfile::tempdir().unwrap();
    let img = image_in(dir.path(), "IMG_2.jpg");
    apply_entries(&[entry(&img, 0, Some("reject"))], None).unwrap();
    assert_eq!(read_sidecar(&img)["flag"], "reject");
}

#[test]
fn one_bad_path_or_value_rejects_the_whole_batch_before_any_write() {
    let dir = tempfile::tempdir().unwrap();
    let good = image_in(dir.path(), "g.jpg");
    let missing = dir.path().join("nope.jpg");

    assert!(apply_entries(&[entry(&good, 3, None), entry(&missing, 3, None)], None).is_err());
    assert!(!sidecar_of(&good).exists(), "no partial write");

    assert!(
        apply_entries(&[entry(&good, 6, None)], None).is_err(),
        "rating > 5"
    );
    assert!(!sidecar_of(&good).exists());

    let too_many: Vec<_> = (0..=MAX_APPLY_ENTRIES)
        .map(|_| entry(&good, 3, None))
        .collect();
    assert!(
        apply_entries(&too_many, None).is_err(),
        "over the batch cap"
    );
    assert!(!sidecar_of(&good).exists());
}

// ---------------------------------------------------------------------------
// Previews
// ---------------------------------------------------------------------------

#[test]
fn a_local_jpeg_previews_as_its_own_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let img = image_in(dir.path(), "local.jpg");
    let bytes = preview_jpeg(&img.to_string_lossy(), PreviewLevel::Local).expect("preview");
    assert_eq!(bytes, fs::read(&img).unwrap());
}

#[test]
fn preview_rejects_hostile_paths() {
    assert!(preview_jpeg("/etc/../etc/passwd", PreviewLevel::Local).is_err());
    assert!(preview_jpeg("relative.jpg", PreviewLevel::Full).is_err());
}

// ---------------------------------------------------------------------------
// Bandwidth: a stub is never hydrated by a Local preview
// ---------------------------------------------------------------------------

#[cfg(all(feature = "sync", any(windows, target_os = "linux")))]
mod common;

#[cfg(all(feature = "sync", any(windows, target_os = "linux")))]
mod stub {
    use super::*;
    use crate::common::garage;
    use rapidraw_lib::sync::{self, Credentials, SyncManager, SyncSettings};

    #[tokio::test]
    async fn local_preview_never_hydrates_a_stub_but_full_does() {
        let Some(g) = garage::shared() else {
            eprintln!("SKIP: no Garage binary; set GARAGE_BIN to run");
            return;
        };
        let bucket = g.create_unique_bucket("quick-cull-stub");
        let settings = SyncSettings {
            enabled: true,
            endpoint: g.endpoint(),
            bucket: bucket.clone(),
            region: g.region().to_string(),
            force_path_style: true,
            ..SyncSettings::default()
        };
        let creds = Credentials {
            access_key: g.access_key_id.clone(),
            secret_key: g.secret_access_key.clone(),
        };
        let rel = "p/CULL_01.jpg";
        let original = tiny_jpeg();

        // Device A uploads the original.
        let (root_a, state_a) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let mgr_a = SyncManager::new_inert();
        mgr_a
            .configure(
                settings.clone(),
                creds.clone(),
                root_a.path().into(),
                state_a.path().into(),
            )
            .expect("configure a");
        sync::install_global_manager(mgr_a.clone());
        let path_a = root_a.path().join(rel);
        fs::create_dir_all(path_a.parent().unwrap()).unwrap();
        fs::write(&path_a, &original).unwrap();
        let mtime = fs::metadata(&path_a)
            .unwrap()
            .modified()
            .unwrap()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        mgr_a.note_new_original(&path_a);
        mgr_a.run_once().await.expect("upload cycle");

        // Device B holds only a stub.
        let (root_b, state_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let mgr_b = SyncManager::new_inert();
        mgr_b
            .configure(settings, creds, root_b.path().into(), state_b.path().into())
            .expect("configure b");
        sync::install_global_manager(mgr_b.clone());
        let stub = root_b.path().join(rel);
        fs::create_dir_all(stub.parent().unwrap()).unwrap();
        mgr_b
            .create_stub(
                &stub,
                &blake3::hash(&original).to_hex(),
                original.len() as u64,
                mtime,
            )
            .expect("create stub");
        assert!(sync::is_cloud_placeholder(&stub));

        let stub_str = stub.to_string_lossy().to_string();
        let err = tokio::task::spawn_blocking({
            let s = stub_str.clone();
            move || preview_jpeg(&s, PreviewLevel::Local)
        })
        .await
        .unwrap()
        .expect_err("no proxy, no local bytes");
        assert_eq!(err, "no-local-preview");
        assert_eq!(
            fs::metadata(&stub).unwrap().len(),
            0,
            "Local must not download"
        );

        let bytes =
            tokio::task::spawn_blocking(move || preview_jpeg(&stub_str, PreviewLevel::Full))
                .await
                .unwrap()
                .expect("full preview");
        assert_eq!(bytes, original, "Full hydrates and returns the original");
        assert_eq!(fs::metadata(&stub).unwrap().len(), original.len() as u64);
    }
}
