//! `rename_folder` / `rename_files` must not let a webview-supplied name
//! relocate the target outside its parent directory.
//!
//! Both commands take the new name straight from the frontend and do
//! `parent.join(new_name)`. A name containing `/`, a `..` component, or an
//! absolute path (which `Path::join` lets *replace* the parent) turns a rename
//! into an arbitrary move anywhere the process can write. The tests below
//! drive the real commands through a headless `AppHandle<Wry>` and assert that
//! such names are rejected and that nothing moved.
//!
//! Harness notes:
//! - The commands are typed `tauri::AppHandle` (= `AppHandle<Wry>`), so the
//!   `MockRuntime` from `tauri::test` cannot be used. A real Wry app is built
//!   once via `tauri::Builder::default().any_thread().build(generate_context!())`
//!   and never `run()`, so no window is ever created (config windows are only
//!   created in `setup`, which `run` triggers). Wry still needs a GTK display
//!   to construct its event loop, so run this under `xvfb-run -a`.
//! - `app.appDirectoriesOverride` is pointed at a temp dir so the commands'
//!   `sync_album_path_changes` -> `get_albums_path` never touches `$HOME`.
//! - Reached through `rapidraw_lib::rename_test_seams`, a `#[doc(hidden)]`
//!   plain-function wrapper over the private `file_management` commands
//!   (same pattern as `sync::copy_files_guarded`).

// `tauri::Builder::any_thread()` only exists on Windows and Linux (macOS must
// run its event loop on the main thread), so this harness cannot compile there.
#![cfg(any(windows, target_os = "linux"))]

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use rapidraw_lib::rename_test_seams::{rename_files, rename_folder};
use tauri::AppHandle;
use tauri::utils::config::AppDirectoriesOverride;
use tempfile::TempDir;

/// Builds the headless Wry app exactly once per test binary and leaks it so
/// the handle stays valid for every test thread.
fn app_handle() -> AppHandle {
    static HANDLE: OnceLock<AppHandle> = OnceLock::new();
    HANDLE
        .get_or_init(|| {
            static APP_DATA: OnceLock<TempDir> = OnceLock::new();
            let data_root = APP_DATA.get_or_init(|| tempfile::tempdir().expect("app data tempdir"));

            let mut ctx = tauri::generate_context!();
            ctx.config_mut().app.app_directories_override =
                Some(AppDirectoriesOverride::Root(data_root.path().to_path_buf()));
            // Never create the configured windows (that only happens in
            // `setup`, driven by `run`), but be explicit anyway.
            ctx.config_mut().app.windows.clear();

            let app = tauri::Builder::default()
                .any_thread()
                .build(ctx)
                .expect("build headless tauri app (needs a display: run under xvfb-run)");
            let handle = app.handle().clone();
            // Keep the runtime alive for the rest of the process.
            std::mem::forget(app);
            handle
        })
        .clone()
}

/// A throwaway library: `<root>/library/album/` holds the pictures that the
/// webview is allowed to rename; `<root>/elsewhere/` is a sibling of the
/// library root that nothing under `library/` should ever be able to reach.
struct Fixture {
    _root: TempDir,
    root: PathBuf,
    library: PathBuf,
    album: PathBuf,
    elsewhere: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let root_dir = tempfile::tempdir().expect("library tempdir");
        let root = root_dir.path().canonicalize().expect("canonical root");
        let library = root.join("library");
        let album = library.join("album");
        let elsewhere = root.join("elsewhere");
        fs::create_dir_all(&album).unwrap();
        fs::create_dir_all(&elsewhere).unwrap();
        Self {
            _root: root_dir,
            root,
            library,
            album,
            elsewhere,
        }
    }

    fn album_str(&self) -> String {
        self.album.to_string_lossy().into_owned()
    }

    /// Creates `album/IMG_0001.jpg` plus its `IMG_0001.jpg.rrdata` sidecar and
    /// returns the image path.
    fn add_image(&self) -> PathBuf {
        let image = self.album.join("IMG_0001.jpg");
        fs::write(&image, b"not really a jpeg").unwrap();
        fs::write(self.album.join("IMG_0001.jpg.rrdata"), b"{}").unwrap();
        image
    }
}

fn list(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(dir)
        .map(|rd| {
            rd.filter_map(Result::ok)
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default();
    names.sort();
    names
}

fn assert_album_untouched(fx: &Fixture, what: &str) {
    assert!(
        fx.album.is_dir(),
        "{what}: `library/album` must still exist in place; library now holds {:?}",
        list(&fx.library)
    );
}

// ---------------------------------------------------------------------------
// rename_folder
// ---------------------------------------------------------------------------

#[test]
fn rename_folder_rejects_dotdot_escape_to_sibling_of_library_root() {
    let fx = Fixture::new();
    let escaped = fx.elsewhere.join("stolen");

    let result = rename_folder(
        fx.album_str(),
        "../elsewhere/stolen".to_string(),
        app_handle(),
    );

    assert!(
        result.is_err(),
        "a new folder name containing `..` must be rejected, got {result:?}"
    );
    assert_album_untouched(&fx, "dotdot folder rename");
    assert!(
        !escaped.exists(),
        "folder escaped the library root to {}",
        escaped.display()
    );
}

#[test]
fn rename_folder_rejects_absolute_path_replacing_parent() {
    let fx = Fixture::new();
    let abs_root = tempfile::tempdir().expect("absolute target tempdir");
    let abs_target = abs_root.path().join("abs");

    let result = rename_folder(
        fx.album_str(),
        abs_target.to_string_lossy().into_owned(),
        app_handle(),
    );

    assert!(
        result.is_err(),
        "an absolute new folder name must be rejected, got {result:?}"
    );
    assert_album_untouched(&fx, "absolute folder rename");
    assert!(
        !abs_target.exists(),
        "folder was relocated to the absolute path {}",
        abs_target.display()
    );
}

#[test]
fn rename_folder_rejects_separator_moving_into_subdirectory() {
    let fx = Fixture::new();
    let nested = fx.library.join("nested");
    fs::create_dir_all(&nested).unwrap();
    let moved = nested.join("album");

    let result = rename_folder(fx.album_str(), "nested/album".to_string(), app_handle());

    assert!(
        result.is_err(),
        "a new folder name containing a path separator must be rejected, got {result:?}"
    );
    assert_album_untouched(&fx, "separator folder rename");
    assert!(
        !moved.exists(),
        "folder was moved into a subdirectory at {}",
        moved.display()
    );
}

#[test]
fn rename_folder_plain_name_still_works() {
    let fx = Fixture::new();

    let result = rename_folder(fx.album_str(), "album2".to_string(), app_handle());

    assert_eq!(
        result,
        Ok(()),
        "positive control: plain rename must succeed"
    );
    assert!(!fx.album.exists(), "old folder name should be gone");
    assert!(
        fx.library.join("album2").is_dir(),
        "renamed folder missing; library holds {:?}",
        list(&fx.library)
    );
}

// ---------------------------------------------------------------------------
// rename_files
// ---------------------------------------------------------------------------

#[test]
fn rename_files_rejects_template_with_dotdot_components() {
    let fx = Fixture::new();
    let image = fx.add_image();
    let escaped = fx.root.join("escaped_IMG_0001.jpg");

    let result = rename_files(
        vec![image.to_string_lossy().into_owned()],
        "../../escaped_{original_filename}".to_string(),
        app_handle(),
    );

    assert!(
        result.is_err(),
        "a rendered name template containing `..` must be rejected, got {result:?}"
    );
    assert!(
        image.exists(),
        "image must still be in place; album holds {:?}, root holds {:?}",
        list(&fx.album),
        list(&fx.root)
    );
    assert!(
        fx.album.join("IMG_0001.jpg.rrdata").exists(),
        "sidecar must still be in place"
    );
    assert!(
        !escaped.exists(),
        "file escaped the album to {}",
        escaped.display()
    );
    assert!(
        !fx.root.join("escaped_IMG_0001.jpg.rrdata").exists(),
        "sidecar followed the file out of the album"
    );
}

#[test]
fn rename_files_rejects_absolute_template() {
    let fx = Fixture::new();
    let image = fx.add_image();
    let abs_root = tempfile::tempdir().expect("absolute target tempdir");
    let template = format!(
        "{}/abs_{{original_filename}}",
        abs_root.path().to_string_lossy()
    );
    let abs_target = abs_root.path().join("abs_IMG_0001.jpg");

    let result = rename_files(
        vec![image.to_string_lossy().into_owned()],
        template,
        app_handle(),
    );

    assert!(
        result.is_err(),
        "an absolute rendered name template must be rejected, got {result:?}"
    );
    assert!(image.exists(), "image must still be in place");
    assert!(
        !abs_target.exists(),
        "file was relocated to the absolute path {}",
        abs_target.display()
    );
}

#[test]
fn rename_files_plain_template_still_works_and_moves_sidecar() {
    let fx = Fixture::new();
    let image = fx.add_image();

    let result = rename_files(
        vec![image.to_string_lossy().into_owned()],
        "renamed_{original_filename}".to_string(),
        app_handle(),
    );

    let renamed = fx.album.join("renamed_IMG_0001.jpg");
    assert_eq!(
        result,
        Ok(vec![renamed.to_string_lossy().into_owned()]),
        "positive control: plain template rename must succeed"
    );
    assert!(!image.exists(), "old image name should be gone");
    assert!(renamed.exists(), "renamed image missing");
    assert!(
        fx.album.join("renamed_IMG_0001.jpg.rrdata").exists(),
        "sidecar must be renamed alongside; album holds {:?}",
        list(&fx.album)
    );
}
