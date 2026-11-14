use crate::decode::formats::is_supported_image_file;
use crate::library::albums::{add_to_album, sync_album_path_changes};
use crate::library::sidecars::{parse_sidecar_name, parse_virtual_path};
use crate::metadata::exif_processing;
#[cfg(target_os = "android")]
use crate::platform::android::*;
use anyhow::Result;
use chrono::{DateTime, Utc};
use memmap2::{Mmap, MmapOptions};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::path::{Path, PathBuf};
#[cfg(any(target_os = "windows", target_os = "macos", target_os = "linux"))]
use std::process::Command;
use std::{fmt, fs, thread};
use tauri::{AppHandle, Emitter};
use tempfile::NamedTempFile;

#[tauri::command]
pub async fn save_temp_file(bytes: Vec<u8>) -> Result<String, String> {
    let mut temp_file = NamedTempFile::new().map_err(|e| e.to_string())?;
    temp_file.write_all(&bytes).map_err(|e| e.to_string())?;
    let (_file, path) = temp_file.keep().map_err(|e| e.to_string())?;
    Ok(path.to_string_lossy().to_string())
}

#[derive(Debug)]
pub enum ReadFileError {
    Io(std::io::Error),
    Locked,
    Empty,
    NotFound,
    Invalid,
}

impl fmt::Display for ReadFileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReadFileError::Io(err) => write!(f, "IO error: {}", err),
            ReadFileError::Locked => write!(f, "File is locked"),
            ReadFileError::Empty => write!(f, "File is empty"),
            ReadFileError::NotFound => write!(f, "File not found"),
            ReadFileError::Invalid => write!(f, "Invalid file"),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ImportSettings {
    pub filename_template: String,
    pub organize_by_date: bool,
    pub date_folder_format: String,
    pub delete_after_import: bool,
}

/// Checks if the given path exists and is an iCloud placeholder file on
/// macOS, **or** a RapidRawCloud sync stub on any platform (§3.5). The
/// `|| crate::sync::hooks::is_stub(path)` hook lights up every existing
/// placeholder consumer (listing flags, CloudOff icon, thumbnail skip,
/// MetadataManager deferral, the `load_image` guard) for free; with the
/// `sync` feature off the hook is a const `false`, so this reverts to the
/// upstream macOS-only check.
#[cfg(target_os = "macos")]
pub fn is_cloud_placeholder(path: &Path) -> bool {
    use std::os::unix::ffi::OsStrExt;
    const SF_DATALESS: u32 = 0x4000_0000;

    if crate::integrations::immich::is_placeholder(path) {
        return true;
    }

    let c_path = match std::ffi::CString::new(path.as_os_str().as_bytes()) {
        Ok(p) => p,
        Err(_) => return crate::sync::hooks::is_stub(path),
    };
    let mut stat_buf: libc::stat = unsafe { std::mem::zeroed() };
    let ret = unsafe { libc::lstat(c_path.as_ptr(), &mut stat_buf) };
    (ret == 0 && (stat_buf.st_flags & SF_DATALESS) != 0) || crate::sync::hooks::is_stub(path)
}

#[cfg(not(target_os = "macos"))]
pub fn is_cloud_placeholder(path: &Path) -> bool {
    crate::integrations::immich::is_placeholder(path) || crate::sync::hooks::is_stub(path)
}

pub fn read_file_mapped(path: &Path) -> Result<Mmap, ReadFileError> {
    if !path.is_file() {
        return Err(ReadFileError::Invalid);
    }
    if !path.exists() {
        return Err(ReadFileError::NotFound);
    }
    if path.metadata().map_err(ReadFileError::Io)?.len() == 0 {
        return Err(ReadFileError::Empty);
    }
    let file = fs::File::open(path).map_err(ReadFileError::Io)?;
    // Filesystems that cannot lock, such as the Flatpak document portal, are mapped unlocked.
    if let Err(fs::TryLockError::WouldBlock) = file.try_lock_shared() {
        return Err(ReadFileError::Locked);
    }
    let mmap = unsafe {
        MmapOptions::new()
            .len(file.metadata().map_err(ReadFileError::Io)?.len() as usize)
            .map(&file)
            .map_err(ReadFileError::Io)?
    };
    Ok(mmap)
}

pub fn write_unique_output(
    dir: &Path,
    stem: &str,
    extension: &str,
    contents: &[u8],
) -> std::io::Result<PathBuf> {
    use std::io::{self, Write};

    let mut builder = tempfile::Builder::new();
    builder.prefix(".rr-output-").suffix(".tmp");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        builder.permissions(fs::Permissions::from_mode(0o666));
    }
    let mut temporary = builder.tempfile_in(dir)?;
    temporary.write_all(contents)?;
    sync_file(temporary.as_file())?;

    let mut sidecar_sources = HashSet::new();
    for entry in fs::read_dir(dir)? {
        let name = entry?.file_name();
        if let Some(name) = name.to_str() {
            let name = name.to_lowercase();
            let source = parse_sidecar_name(&name)
                .map(|(source, _)| source)
                .or_else(|| name.strip_suffix(".rrexif"));
            if let Some(source) = source {
                sidecar_sources.insert(source.to_owned());
            }
        }
    }

    let mut counter = 0_u64;
    loop {
        let name = if counter == 0 {
            format!("{stem}.{extension}")
        } else {
            format!("{stem}_{counter}.{extension}")
        };
        let path = dir.join(&name);
        let mut has_sidecar = sidecar_sources.contains(&name.to_lowercase());
        for suffix in [".rrdata", ".rrexif"] {
            if has_sidecar {
                break;
            }
            let mut sidecar = path.as_os_str().to_owned();
            sidecar.push(suffix);
            has_sidecar = Path::new(&sidecar).try_exists()?;
        }
        if !has_sidecar {
            match temporary.persist_noclobber(&path) {
                Ok(_) => return Ok(path),
                Err(error) if error.error.kind() == io::ErrorKind::AlreadyExists => {
                    temporary = error.file;
                }
                Err(error) => return Err(error.error),
            }
        }
        counter = counter
            .checked_add(1)
            .ok_or_else(|| io::Error::other("No available output filename"))?;
    }
}

pub fn write_file_atomic(path: &Path, contents: &[u8]) -> std::io::Result<()> {
    use std::io::Write;

    let dir = path
        .parent()
        .filter(|dir| !dir.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let mut tmp = tempfile::Builder::new()
        .prefix(".rr-")
        .suffix(".tmp")
        .tempfile_in(dir)?;
    tmp.write_all(contents)?;
    sync_file(tmp.as_file())?;

    let mut tmp = tmp.into_temp_path();
    let mut attempt = 0;
    loop {
        match tmp.persist(path) {
            Ok(()) => return Ok(()),
            Err(e) if attempt < 5 && e.error.kind() == std::io::ErrorKind::PermissionDenied => {
                tmp = e.path;
                attempt += 1;
                thread::sleep(std::time::Duration::from_millis(20 << attempt));
            }
            Err(e) => return Err(e.error),
        }
    }
}

fn sync_file(file: &fs::File) -> std::io::Result<()> {
    match file.sync_all() {
        #[cfg(unix)]
        Err(e) if is_sync_unsupported(&e) => {
            #[cfg(target_vendor = "apple")]
            {
                use std::os::fd::AsRawFd;
                if unsafe { libc::fsync(file.as_raw_fd()) } != 0 {
                    let e = std::io::Error::last_os_error();
                    if !is_sync_unsupported(&e) {
                        return Err(e);
                    }
                }
            }
            Ok(())
        }
        result => result,
    }
}

#[cfg(unix)]
fn is_sync_unsupported(e: &std::io::Error) -> bool {
    matches!(e.raw_os_error(), Some(code) if code == libc::ENOTSUP || code == libc::EOPNOTSUPP)
}

#[tauri::command]
pub fn duplicate_file(
    path: String,
    target_album_id: Option<String>,
    app_handle: AppHandle,
) -> Result<String, String> {
    let (source_path, source_sidecar_path) = parse_virtual_path(&path);
    if !source_path.is_file() {
        return Err("Source path is not a file.".to_string());
    }
    // §3.5 guard: hydrate a stub original before duplicating so the new copy
    // carries the real bytes, not the 0-byte placeholder. No-op when sync is
    // off or already local.
    crate::sync::hooks::ensure_local(&source_path, "duplicate_file").map_err(|e| e.to_string())?;

    let parent = source_path
        .parent()
        .ok_or("Could not get parent directory")?;
    let stem = source_path
        .file_stem()
        .and_then(|s| s.to_str())
        .ok_or("Could not get file stem")?;
    let extension = source_path
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("");

    let mut counter = 1;
    let mut dest_path;
    loop {
        let new_stem = if counter == 1 {
            format!("{}_copy", stem)
        } else {
            format!("{}_copy_{}", stem, counter - 1)
        };
        dest_path = parent.join(format!("{}.{}", new_stem, extension));
        if !dest_path.exists() {
            break;
        }
        counter += 1;
    }

    fs::copy(&source_path, &dest_path).map_err(|e| e.to_string())?;

    if source_sidecar_path.exists()
        && let Some(dest_str) = dest_path.to_str()
    {
        let (_, dest_sidecar_path) = parse_virtual_path(dest_str);
        fs::copy(&source_sidecar_path, &dest_sidecar_path).map_err(|e| e.to_string())?;
    }

    let mut source_rrexif_name = source_path.file_name().unwrap().to_os_string();
    source_rrexif_name.push(".rrexif");
    let source_rrexif = source_path.with_file_name(source_rrexif_name);

    if source_rrexif.exists() {
        let mut dest_rrexif_name = dest_path.file_name().unwrap().to_os_string();
        dest_rrexif_name.push(".rrexif");
        let dest_rrexif = dest_path.with_file_name(dest_rrexif_name);
        let _ = fs::copy(&source_rrexif, &dest_rrexif);
    }

    let dest_path_str = dest_path.to_string_lossy().into_owned();

    if let Some(album_id) = target_album_id {
        let _ = add_to_album(album_id, vec![dest_path_str.clone()], app_handle);
    }

    Ok(dest_path_str)
}

fn find_all_associated_files(source_image_path: &Path) -> Result<Vec<PathBuf>, String> {
    let mut associated_files = vec![source_image_path.to_path_buf()];

    let mut rrexif_name = source_image_path
        .file_name()
        .unwrap_or_default()
        .to_os_string();
    rrexif_name.push(".rrexif");
    let rrexif_path = source_image_path.with_file_name(rrexif_name);

    if rrexif_path.exists() {
        associated_files.push(rrexif_path);
    }

    let parent_dir = source_image_path
        .parent()
        .ok_or("Could not determine parent directory")?;
    let source_filename = source_image_path
        .file_name()
        .ok_or("Could not get source filename")?
        .to_string_lossy();

    if let Ok(entries) = fs::read_dir(parent_dir) {
        for entry in entries.filter_map(Result::ok) {
            let entry_path = entry.path();
            if !entry_path.is_file() {
                continue;
            }

            let entry_os_filename = entry.file_name();
            let entry_filename = entry_os_filename.to_string_lossy();

            if parse_sidecar_name(&entry_filename).is_some_and(|(name, _)| name == source_filename)
            {
                associated_files.push(entry_path);
            }
        }
    }

    Ok(associated_files)
}

#[tauri::command]
pub async fn copy_files(
    source_paths: Vec<String>,
    destination_folder: String,
    app_handle: AppHandle,
) -> Result<(), String> {
    let dest_path = Path::new(&destination_folder);
    if !dest_path.is_dir() {
        return Err(format!(
            "Destination is not a folder: {}",
            destination_folder
        ));
    }

    let unique_source_images: HashSet<PathBuf> = source_paths
        .iter()
        .map(|p| parse_virtual_path(p).0)
        .collect();

    for source_image_path in &unique_source_images {
        // §3.5 guard: hydrate a stub original before copying so the copy
        // reproduces the real bytes rather than the 0-byte placeholder
        // (hydrate-then-copy). No-op when sync is off or already local.
        crate::sync::hooks::ensure_local(source_image_path, "copy_files")
            .map_err(|e| e.to_string())?;
        crate::integrations::immich::ensure_local(&app_handle, source_image_path).await?;
    }

    let mut operations_to_perform = Vec::new();

    for source_image_path in &unique_source_images {
        let all_files_to_copy = find_all_associated_files(source_image_path)?;

        let source_parent = source_image_path
            .parent()
            .ok_or("Could not get parent directory")?;

        if source_parent == dest_path {
            let stem = source_image_path
                .file_stem()
                .and_then(|s| s.to_str())
                .ok_or("Could not get file stem")?;
            let extension = source_image_path
                .extension()
                .and_then(|s| s.to_str())
                .unwrap_or("");

            let mut counter = 1;
            let new_base_path = loop {
                let new_stem = format!("{}_copy_{}", stem, counter);
                let temp_path = source_parent.join(format!("{}.{}", new_stem, extension));
                if !temp_path.exists() {
                    break temp_path;
                }
                counter += 1;
            };
            let new_filename = new_base_path.file_name().unwrap().to_string_lossy();

            for original_file in all_files_to_copy {
                let original_full_filename = original_file.file_name().unwrap().to_string_lossy();
                let source_base_filename = source_image_path.file_name().unwrap().to_string_lossy();
                let new_dest_filename =
                    original_full_filename.replacen(&*source_base_filename, &new_filename, 1);

                let final_dest_path = dest_path.join(new_dest_filename);
                operations_to_perform.push((original_file, final_dest_path));
            }
        } else {
            for file_to_copy in all_files_to_copy {
                if let Some(file_name) = file_to_copy.file_name() {
                    let dest_file_path = dest_path.join(file_name);

                    if dest_file_path.exists()
                        || operations_to_perform
                            .iter()
                            .any(|(_, dest)| *dest == dest_file_path)
                    {
                        return Err(format!(
                            "Copy aborted: File already exists at destination: {}",
                            dest_file_path.display()
                        ));
                    }

                    operations_to_perform.push((file_to_copy, dest_file_path));
                }
            }
        }
    }

    for (source, dest) in operations_to_perform {
        fs::copy(&source, &dest)
            .map_err(|e| format!("Copy failed for {}: {}", source.display(), e))?;
    }

    Ok(())
}

#[tauri::command]
pub async fn move_files(
    source_paths: Vec<String>,
    destination_folder: String,
    app_handle: AppHandle,
) -> Result<(), String> {
    let dest_path = Path::new(&destination_folder);
    if !dest_path.is_dir() {
        return Err(format!(
            "Destination is not a folder: {}",
            destination_folder
        ));
    }

    let unique_source_images: HashSet<PathBuf> = source_paths
        .iter()
        .map(|p| parse_virtual_path(p).0)
        .collect();

    for source_image_path in &unique_source_images {
        // §3.5 guard: hydrate a stub original before moving so the moved
        // file carries the real bytes rather than the 0-byte placeholder.
        // No-op when sync is off or already local.
        crate::sync::hooks::ensure_local(source_image_path, "move_files")
            .map_err(|e| e.to_string())?;
        crate::integrations::immich::ensure_local(&app_handle, source_image_path).await?;
    }

    let mut operations_to_perform = Vec::new();
    let mut renames = HashMap::new();

    for source_image_path in &unique_source_images {
        let source_parent = source_image_path
            .parent()
            .ok_or("Could not get parent directory")?;

        if source_parent == dest_path {
            return Err("Cannot move files into the same folder they are already in.".to_string());
        }

        let all_files_to_move = find_all_associated_files(source_image_path)?;
        let is_remote = crate::integrations::immich::is_remote(source_image_path);

        for file_to_move in &all_files_to_move {
            if let Some(file_name) = file_to_move.file_name() {
                let dest_file_path = dest_path.join(file_name);

                if dest_file_path.exists()
                    || operations_to_perform
                        .iter()
                        .any(|(_, dest, _)| *dest == dest_file_path)
                {
                    return Err(format!(
                        "Move aborted: File already exists at destination: {}",
                        dest_file_path.display()
                    ));
                }

                operations_to_perform.push((file_to_move.clone(), dest_file_path, is_remote));
            }
        }

        if is_remote {
            continue;
        }
        let dest_image_path = dest_path.join(source_image_path.file_name().unwrap());
        renames.insert(
            source_image_path.to_string_lossy().into_owned(),
            dest_image_path.to_string_lossy().into_owned(),
        );
    }

    let mut moved = HashSet::new();
    let mut result = Ok(());
    for (source, dest, is_remote) in operations_to_perform {
        if is_remote {
            if let Err(e) = fs::copy(&source, &dest) {
                result = Err(format!("Copy failed for {}: {}", source.display(), e));
                break;
            }
        } else if fs::rename(&source, &dest).is_err() {
            if let Err(e) = fs::copy(&source, &dest) {
                result = Err(format!(
                    "Move failed during copy for {}: {}",
                    source.display(),
                    e
                ));
                break;
            }

            if let Err(e) = fs::remove_file(&source) {
                log::warn!(
                    "Moved file successfully, but failed to delete original {}: {}",
                    source.display(),
                    e
                );
            }
        }
        moved.insert(source);
    }

    renames.retain(|source, _| moved.contains(Path::new(source)));
    sync_album_path_changes(&app_handle, Some(&renames), None, None);

    result
}

/// Ensures `name` is exactly one plain file or folder name (a single
/// `Component::Normal`), so joining it onto a parent directory can never
/// escape that directory via separators, `..`, or an absolute path.
pub(crate) fn validate_single_path_component(name: &str) -> Result<(), String> {
    const ERR: &str = "Invalid name: must be a single file or folder name without path separators";
    if name.is_empty() || name == "." || name == ".." || name.contains(['/', '\\', '\0']) {
        return Err(ERR.to_string());
    }
    let mut components = Path::new(name).components();
    match (components.next(), components.next()) {
        (Some(std::path::Component::Normal(c)), None) if c == name => Ok(()),
        _ => Err(ERR.to_string()),
    }
}

#[tauri::command]
pub fn show_in_finder(path: String) -> Result<(), String> {
    let (source_path, _) = parse_virtual_path(&path);

    // Only ever hand the system file manager a canonical, existing file or
    // directory whose parent is a directory. Anything else (URLs, non-paths,
    // dangling symlinks, a "parent" that is a regular file) is rejected before
    // any external command is spawned.
    let source_path = fs::canonicalize(&source_path)
        .map_err(|e| format!("Cannot show '{}' in file manager: {}", path, e))?;
    let metadata = fs::metadata(&source_path).map_err(|e| e.to_string())?;
    if !metadata.is_file() && !metadata.is_dir() {
        return Err("Path is not a regular file or directory".into());
    }
    #[cfg_attr(not(target_os = "linux"), allow(unused_variables))]
    let parent = source_path
        .parent()
        .filter(|parent| parent.is_dir())
        .ok_or_else(|| "Could not get parent directory".to_string())?;

    #[cfg(target_os = "windows")]
    {
        // `canonicalize` yields an extended-length path on Windows (`\\?\C:\...`,
        // or `\\?\UNC\server\share\...` for network shares and mapped drives),
        // which `explorer /select,` does not accept. Validation above used the
        // canonical path; only the spawned argument is mapped back to the
        // plain `C:\...` / `\\server\share\...` form.
        let lossy = source_path.to_string_lossy();
        let source_path_str: String = if let Some(rest) = lossy.strip_prefix(r"\\?\UNC\") {
            format!(r"\\{rest}")
        } else {
            lossy.strip_prefix(r"\\?\").unwrap_or(&lossy).to_string()
        };
        Command::new("explorer")
            .args(["/select,", source_path_str.as_str()])
            .spawn()
            .map_err(|e| e.to_string())?;
    }

    #[cfg(target_os = "macos")]
    {
        Command::new("open")
            .args(["-R", "--"])
            .arg(&source_path)
            .spawn()
            .map_err(|e| e.to_string())?;
    }

    #[cfg(target_os = "linux")]
    {
        Command::new("xdg-open")
            .arg(parent)
            .spawn()
            .map_err(|e| e.to_string())?;
    }

    #[cfg(target_os = "android")]
    {
        let _ = source_path;
        Err("Show in File Manager is not natively supported via CLI on Android.".into())
    }

    #[cfg(target_os = "ios")]
    {
        let _ = source_path;
        Err("Show in File Manager is not supported on iOS.".into())
    }

    #[cfg(not(any(target_os = "android", target_os = "ios")))]
    {
        Ok(())
    }
}

#[tauri::command]
pub fn delete_files_from_disk(paths: Vec<String>, app_handle: AppHandle) -> Result<(), String> {
    let paths = crate::integrations::immich::trash_remote(&app_handle, paths, false)?;
    let mut files_to_trash = HashSet::new();
    let mut deletions = HashSet::new();

    for path_str in paths {
        let (source_path, sidecar_path) = parse_virtual_path(&path_str);
        deletions.insert(path_str.clone());

        if path_str.contains("?vc=") {
            if sidecar_path.exists() {
                files_to_trash.insert(sidecar_path);
            }
        } else {
            if source_path.exists() {
                match find_all_associated_files(&source_path) {
                    Ok(associated_files) => {
                        for file in associated_files {
                            files_to_trash.insert(file);
                        }
                    }
                    Err(e) => {
                        log::warn!(
                            "Could not find associated files for {}: {}",
                            source_path.display(),
                            e
                        );
                    }
                }
            }
        }
    }

    if files_to_trash.is_empty() {
        return Ok(());
    }

    let final_paths_to_delete: Vec<PathBuf> = files_to_trash.into_iter().collect();

    #[cfg(any(target_os = "windows", target_os = "macos", target_os = "linux"))]
    if let Err(trash_error) = trash::delete_all(&final_paths_to_delete) {
        log::warn!(
            "Failed to move files to trash: {}. Falling back to permanent delete.",
            trash_error
        );
        for path in final_paths_to_delete {
            if path.is_file() {
                if let Err(e) = fs::remove_file(&path) {
                    log::warn!("Failed to delete file {}: {}", path.display(), e);
                }
            } else if path.is_dir()
                && let Err(e) = fs::remove_dir_all(&path)
            {
                log::warn!("Failed to delete directory {}: {}", path.display(), e);
            }
        }
    }

    #[cfg(not(any(target_os = "windows", target_os = "macos", target_os = "linux")))]
    for path in final_paths_to_delete {
        if path.is_file() {
            if let Err(e) = fs::remove_file(&path) {
                log::warn!("Failed to delete file {}: {}", path.display(), e);
            }
        } else if path.is_dir() {
            if let Err(e) = fs::remove_dir_all(&path) {
                log::warn!("Failed to delete directory {}: {}", path.display(), e);
            }
        }
    }

    sync_album_path_changes(&app_handle, None, Some(&deletions), None);

    Ok(())
}

fn deletion_stem_for(filename: &str) -> Option<&str> {
    let image_filename = if let Some((image_name, _)) = parse_sidecar_name(filename) {
        image_name
    } else if filename.ends_with(".rrexif") {
        filename.trim_end_matches(".rrexif")
    } else if is_supported_image_file(filename) {
        filename
    } else {
        return None;
    };
    Path::new(image_filename)
        .file_stem()
        .and_then(|s| s.to_str())
}

#[tauri::command]
pub fn delete_files_with_associated(
    paths: Vec<String>,
    app_handle: AppHandle,
) -> Result<(), String> {
    let paths = crate::integrations::immich::trash_remote(&app_handle, paths, true)?;
    if paths.is_empty() {
        return Ok(());
    }

    let mut stems_by_dir: HashMap<PathBuf, HashSet<String>> = HashMap::new();
    let mut deletions = HashSet::new();

    for path_str in &paths {
        deletions.insert(path_str.clone());
        let (source_path, _) = parse_virtual_path(path_str);
        if let (Some(stem), Some(parent)) = (
            source_path.file_stem().and_then(|s| s.to_str()),
            source_path.parent(),
        ) {
            stems_by_dir
                .entry(parent.to_path_buf())
                .or_default()
                .insert(stem.to_string());
        }
    }

    if stems_by_dir.is_empty() {
        return Ok(());
    }

    let mut files_to_trash = HashSet::new();

    for (parent_dir, stems_to_delete) in stems_by_dir {
        if let Ok(entries) = fs::read_dir(parent_dir) {
            for entry in entries.filter_map(Result::ok) {
                let entry_path = entry.path();
                if !entry_path.is_file() {
                    continue;
                }

                let entry_filename = entry.file_name();
                let entry_filename_str = entry_filename.to_string_lossy();

                if let Some(stem) = deletion_stem_for(&entry_filename_str)
                    && stems_to_delete.contains(stem)
                {
                    files_to_trash.insert(entry_path);
                }
            }
        }
    }

    if files_to_trash.is_empty() {
        return Ok(());
    }

    for path in &files_to_trash {
        if is_supported_image_file(path) {
            deletions.insert(path.to_string_lossy().into_owned());
        }
    }

    let final_paths_to_delete: Vec<PathBuf> = files_to_trash.into_iter().collect();

    #[cfg(any(target_os = "windows", target_os = "macos", target_os = "linux"))]
    if let Err(trash_error) = trash::delete_all(&final_paths_to_delete) {
        log::warn!(
            "Failed to move files to trash: {}. Falling back to permanent delete.",
            trash_error
        );
        for path in final_paths_to_delete {
            if path.is_file()
                && let Err(e) = fs::remove_file(&path)
            {
                log::warn!("Failed to delete file {}: {}", path.display(), e);
            }
        }
    }

    #[cfg(not(any(target_os = "windows", target_os = "macos", target_os = "linux")))]
    for path in final_paths_to_delete {
        if path.is_file() {
            if let Err(e) = fs::remove_file(&path) {
                log::warn!("Failed to delete file {}: {}", path.display(), e);
            }
        }
    }

    sync_album_path_changes(&app_handle, None, Some(&deletions), None);

    Ok(())
}

#[tauri::command]
pub async fn import_files(
    source_paths: Vec<String>,
    destination_folder: String,
    settings: ImportSettings,
    app_handle: AppHandle,
) -> Result<(), String> {
    let total_files = source_paths.len();

    // The date folder format comes from the webview. Validate it once, before
    // anything is emitted or touched on disk: chrono panics on an invalid
    // specifier inside `to_string()`, which would kill the un-awaited blocking
    // task below without ever emitting `import-error`/`import-complete`.
    let date_format_str = settings
        .date_folder_format
        .replace("YYYY", "%Y")
        .replace("MM", "%m")
        .replace("DD", "%d");
    if settings.organize_by_date {
        use std::fmt::Write as _;
        let mut probe = String::new();
        write!(probe, "{}", Utc::now().format(&date_format_str)).map_err(|_| {
            format!(
                "Invalid date folder format: {:?}",
                settings.date_folder_format
            )
        })?;
    }

    let _ = app_handle.emit("import-start", serde_json::json!({ "total": total_files }));

    tauri::async_runtime::spawn_blocking(move || {
        let mut imported = 0usize;
        for (i, source_path_str) in source_paths.iter().enumerate() {
            let _ = app_handle.emit(
                "import-progress",
                serde_json::json!({ "current": i, "total": total_files, "path": source_path_str }),
            );

            let import_result: Result<(), String> = (|| {
                #[cfg(target_os = "android")]
                if is_android_content_uri(source_path_str) {
                    let resolved_name = resolve_android_content_uri_name(source_path_str)?;
                    let source_bytes = read_android_content_uri(source_path_str)?;
                    let source_name_path = Path::new(&resolved_name);
                    let file_date = exif_processing::get_creation_date_from_bytes(
                        &resolved_name,
                        &source_bytes,
                    );

                    let mut final_dest_folder = PathBuf::from(&destination_folder);
                    if settings.organize_by_date {
                        let subfolder = file_date.format(&date_format_str).to_string();
                        final_dest_folder.push(subfolder);
                    }

                    fs::create_dir_all(&final_dest_folder)
                        .map_err(|e| format!("Failed to create destination folder: {}", e))?;

                    let new_stem = generate_filename_from_template(
                        &settings.filename_template,
                        source_name_path,
                        i + 1,
                        total_files,
                        &file_date,
                    );
                    let extension = source_name_path
                        .extension()
                        .and_then(|s| s.to_str())
                        .unwrap_or("");
                    let new_filename = format!("{}.{}", new_stem, extension);
                    let dest_file_path = final_dest_folder.join(new_filename);

                    if dest_file_path.exists() {
                        return Err(format!(
                            "File already exists at destination: {}",
                            dest_file_path.display()
                        ));
                    }

                    fs::write(&dest_file_path, source_bytes).map_err(|e| e.to_string())?;

                    // §2.5: tell the sync engine a new original landed in the
                    // library so it is tracked + uploaded. Without this the
                    // imported file sits in the sync_root untracked forever (no
                    // cycle rescans the tree for untracked files).
                    crate::sync::hooks::notify_new_original(&dest_file_path);

                    if settings.delete_after_import {
                        log::info!(
                            "Skipping delete_after_import for Android content URI source: {}",
                            source_path_str
                        );
                    }

                    return Ok(());
                }

                let (source_path, source_sidecar) = parse_virtual_path(source_path_str);
                if !source_path.exists() {
                    return Err(format!("Source file not found: {}", source_path_str));
                }

                let file_date = exif_processing::get_creation_date_from_path(&source_path);

                let mut final_dest_folder = PathBuf::from(&destination_folder);
                if settings.organize_by_date {
                    let subfolder = file_date.format(&date_format_str).to_string();
                    final_dest_folder.push(subfolder);
                }

                fs::create_dir_all(&final_dest_folder)
                    .map_err(|e| format!("Failed to create destination folder: {}", e))?;

                let new_stem = generate_filename_from_template(
                    &settings.filename_template,
                    &source_path,
                    i + 1,
                    total_files,
                    &file_date,
                );
                let extension = source_path
                    .extension()
                    .and_then(|s| s.to_str())
                    .unwrap_or("");
                let new_filename = format!("{}.{}", new_stem, extension);
                let dest_file_path = final_dest_folder.join(new_filename);

                if dest_file_path.exists() {
                    return Err(format!(
                        "File already exists at destination: {}",
                        dest_file_path.display()
                    ));
                }

                fs::copy(&source_path, &dest_file_path).map_err(|e| e.to_string())?;
                if source_sidecar.exists()
                    && let Some(dest_str) = dest_file_path.to_str()
                {
                    let (_, dest_sidecar) = parse_virtual_path(dest_str);
                    fs::copy(&source_sidecar, &dest_sidecar).map_err(|e| e.to_string())?;
                }

                let mut source_rrexif_name = source_path.file_name().unwrap().to_os_string();
                source_rrexif_name.push(".rrexif");
                let source_rrexif = source_path.with_file_name(source_rrexif_name);

                if source_rrexif.exists() {
                    let mut dest_rrexif_name = dest_file_path.file_name().unwrap().to_os_string();
                    dest_rrexif_name.push(".rrexif");
                    let dest_rrexif = dest_file_path.with_file_name(dest_rrexif_name);
                    let _ = fs::copy(&source_rrexif, &dest_rrexif);
                }

                // §2.5: register the newly-imported original with the sync
                // engine (marks it dirty for upload). Copied sidecars ride
                // along with the original's item; only the original is
                // announced here.
                crate::sync::hooks::notify_new_original(&dest_file_path);

                if settings.delete_after_import {
                    #[cfg(any(target_os = "windows", target_os = "macos", target_os = "linux"))]
                    {
                        if let Err(trash_error) = trash::delete(&source_path) {
                            log::warn!(
                                "Failed to trash source file {}: {}. Deleting permanently.",
                                source_path.display(),
                                trash_error
                            );
                            fs::remove_file(&source_path).map_err(|e| e.to_string())?;
                        }
                        if source_sidecar.exists()
                            && let Err(trash_error) = trash::delete(&source_sidecar)
                        {
                            log::warn!(
                                "Failed to trash source sidecar {}: {}. Deleting permanently.",
                                source_sidecar.display(),
                                trash_error
                            );
                            fs::remove_file(&source_sidecar).map_err(|e| e.to_string())?;
                        }
                        if source_rrexif.exists() {
                            let _ = fs::remove_file(&source_rrexif);
                        }
                    }

                    #[cfg(not(any(
                        target_os = "windows",
                        target_os = "macos",
                        target_os = "linux"
                    )))]
                    {
                        fs::remove_file(&source_path).map_err(|e| e.to_string())?;
                        if source_sidecar.exists() {
                            fs::remove_file(&source_sidecar).map_err(|e| e.to_string())?;
                        }
                        if source_rrexif.exists() {
                            let _ = fs::remove_file(&source_rrexif);
                        }
                    }
                }

                Ok(())
            })();

            if let Err(e) = import_result {
                eprintln!("Failed to import {}: {}", source_path_str, e);
                let _ = app_handle.emit("import-error", e);
                continue;
            }
            imported += 1;
        }

        // §3.3: kick a prompt sync cycle so the just-imported originals upload
        // soon, instead of waiting for the periodic ~1h background worker. The
        // in-process `notify_new_original` above already marked them dirty and
        // emitted the live pending-upload badge; this enqueues the WorkManager
        // cycle that actually pushes the bytes. Android-only (desktop has no
        // WorkManager; its cycle is driven foreground via `sync_run_cycle`).
        #[cfg(target_os = "android")]
        if imported > 0 {
            if let Err(e) = crate::platform::android::android_enqueue_expedited_sync() {
                log::warn!("import: failed to enqueue expedited sync: {e}");
            }
        }
        let _ = imported;

        let _ = app_handle.emit(
            "import-progress",
            serde_json::json!({ "current": total_files, "total": total_files, "path": "" }),
        );
        let _ = app_handle.emit("import-complete", ());
    });

    Ok(())
}

pub fn generate_filename_from_template(
    template: &str,
    original_path: &std::path::Path,
    sequence: usize,
    total: usize,
    file_date: &DateTime<Utc>,
) -> String {
    let stem = original_path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("image");
    let sequence_str = format!(
        "{:0width$}",
        sequence,
        width = total.to_string().len().max(1)
    );
    let local_date = file_date.with_timezone(&chrono::Local);
    let stem_prefix = stem.trim_end_matches(|c: char| c.is_ascii_digit());
    let filename_number = &stem[stem_prefix.len()..];

    let mut result = template.to_string();
    result = result.replace("{original_filename}", stem);
    result = result.replace("{original_filename_number}", filename_number);
    result = result.replace("{sequence}", &sequence_str);
    result = result.replace("{YYYY}", &local_date.format("%Y").to_string());
    result = result.replace("{MM}", &local_date.format("%m").to_string());
    result = result.replace("{DD}", &local_date.format("%d").to_string());
    result = result.replace("{hh}", &local_date.format("%H").to_string());
    result = result.replace("{mm}", &local_date.format("%M").to_string());
    result = result.replace("{ss}", &local_date.format("%S").to_string());

    result
}

#[tauri::command]
pub fn rename_files(
    paths: Vec<String>,
    name_template: String,
    app_handle: AppHandle,
) -> Result<Vec<String>, String> {
    if paths.is_empty() {
        return Ok(Vec::new());
    }

    // An image and its virtual copies share one file, so rename each file once.
    let mut original_paths: Vec<PathBuf> = Vec::new();
    for path_str in &paths {
        let (original_path, _) = parse_virtual_path(path_str);
        if !original_paths.contains(&original_path) {
            original_paths.push(original_path);
        }
    }

    let mut operations: Vec<(PathBuf, PathBuf)> = Vec::new();
    let mut renames = HashMap::new();

    for (i, original_path) in original_paths.iter().enumerate() {
        let original_path = original_path.clone();
        if !original_path.exists() {
            return Err(format!("File not found: {}", original_path.display()));
        }

        let parent = original_path
            .parent()
            .ok_or("Could not get parent directory")?;
        let extension = original_path
            .extension()
            .and_then(|s| s.to_str())
            .unwrap_or("");

        let file_date = exif_processing::get_creation_date_from_path(&original_path);

        let new_stem = generate_filename_from_template(
            &name_template,
            &original_path,
            i + 1,
            original_paths.len(),
            &file_date,
        );
        let new_filename = format!("{}.{}", new_stem, extension);
        validate_single_path_component(&new_filename)?;
        let new_path = parent.join(new_filename);

        let case_only_rename = new_path != original_path
            && new_path.to_string_lossy().to_lowercase()
                == original_path.to_string_lossy().to_lowercase()
            && !fs::read_dir(parent).is_ok_and(|entries| {
                entries
                    .filter_map(Result::ok)
                    .any(|entry| Some(entry.file_name().as_os_str()) == new_path.file_name())
            });

        if new_path.exists() && new_path != original_path && !case_only_rename {
            return Err(format!(
                "A file with the name {} already exists.",
                new_path.display()
            ));
        }

        if operations.iter().any(|(other_original, other_new)| {
            *other_new == new_path && *other_original != original_path
        }) {
            return Err(format!(
                "More than one file would be renamed to {}.",
                new_path.display()
            ));
        }

        operations.push((original_path, new_path));
    }

    let mut sidecar_operations: Vec<Vec<(PathBuf, PathBuf)>> = Vec::new();
    for (original_path, new_path) in &operations {
        let mut image_sidecars = Vec::new();
        let parent = original_path
            .parent()
            .ok_or("Could not get parent directory")?;
        let original_filename_str = original_path.file_name().unwrap().to_string_lossy();
        let new_filename_str = new_path.file_name().unwrap().to_string_lossy();

        if let Ok(entries) = fs::read_dir(parent) {
            for entry in entries.filter_map(Result::ok) {
                let entry_path = entry.path();
                let entry_os_filename = entry.file_name();
                let entry_filename = entry_os_filename.to_string_lossy();

                if parse_sidecar_name(&entry_filename)
                    .is_some_and(|(name, _)| name == original_filename_str)
                {
                    let new_sidecar_filename =
                        entry_filename.replacen(&*original_filename_str, &new_filename_str, 1);
                    let new_sidecar_path = parent.join(new_sidecar_filename);
                    image_sidecars.push((entry_path, new_sidecar_path));
                }
            }
        }

        let mut old_rrexif_name = original_path.file_name().unwrap().to_os_string();
        old_rrexif_name.push(".rrexif");
        let old_rrexif = original_path.with_file_name(old_rrexif_name);

        if old_rrexif.exists() {
            let mut new_rrexif_name = new_path.file_name().unwrap().to_os_string();
            new_rrexif_name.push(".rrexif");
            let new_rrexif = new_path.with_file_name(new_rrexif_name);
            image_sidecars.push((old_rrexif, new_rrexif));
        }
        sidecar_operations.push(image_sidecars);
    }

    for (image_operation, image_sidecars) in operations.into_iter().zip(sidecar_operations) {
        for (index, (old_path, new_path)) in std::iter::once(image_operation)
            .chain(image_sidecars)
            .enumerate()
        {
            if let Err(e) = fs::rename(&old_path, &new_path) {
                log::warn!(
                    "Failed to rename {} to {}: {}",
                    old_path.display(),
                    new_path.display(),
                    e
                );
                if index == 0 {
                    break;
                }
                continue;
            }

            let old_str = old_path.to_string_lossy().into_owned();
            let new_str = new_path.to_string_lossy().into_owned();

            renames.insert(old_str, new_str);
        }
    }

    sync_album_path_changes(&app_handle, Some(&renames), None, None);

    // One entry per input path, in order, keeping virtual copy ids; unrenamed files keep their path.
    let final_new_paths = paths
        .iter()
        .map(|path_str| {
            let (base, copy_suffix) = match path_str.rsplit_once("?vc=") {
                Some((base, id)) => (base, format!("?vc={}", id)),
                None => (path_str.as_str(), String::new()),
            };
            let new_base = renames.get(base).map_or(base, String::as_str);
            format!("{}{}", new_base, copy_suffix)
        })
        .collect();

    Ok(final_new_paths)
}

#[cfg(all(test, any(windows, target_os = "linux")))]
mod import_date_format_tests {
    //! Regression: `import_files` hands the webview-supplied
    //! `ImportSettings::date_folder_format` straight to chrono
    //! (`file_date.format(&date_format_str).to_string()`). chrono 0.4.45 panics
    //! in `to_string()` on an invalid specifier such as `%Q` ("a Display
    //! implementation returned an error unexpectedly"). The panic happens
    //! inside the `spawn_blocking` task the command never awaits, so the
    //! process survives, `import_files` has already returned `Ok(())`, and
    //! neither `import-error` nor `import-complete` is ever emitted: the UI
    //! hangs on the import dialog.
    //!
    //! Expected behavior: the format string is validated up front and the
    //! command returns `Err(..)` naming the format, before any file is
    //! touched. The positive control proves a valid `%Y/%m` still lands the
    //! file in `<dest>/<YYYY>/<MM>/`.
    //!
    //! Harness: `import_files` takes the concrete `tauri::AppHandle`
    //! (`AppHandle<Wry>`), so `tauri::test::mock_builder()` (MockRuntime)
    //! cannot drive it. A real `Wry` app is built headless on a parked helper
    //! thread (`Builder::any_thread`, the app's own `generate_context!()`,
    //! whose only window is `create: false`, so no webview is made) and shared
    //! by the tests; on Linux that needs a display, so run under
    //! `xvfb-run -a cargo test --lib import_date_format_tests`. Without a
    //! display the tests print `SKIP` and return, mirroring the Garage e2e
    //! suite. The module lives in-file because `library::file_ops` is private
    //! and has no `sync::`-style test seam for this command.

    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::mpsc::{self, RecvTimeoutError};
    use std::sync::{Mutex, OnceLock};
    use std::time::Duration;

    use tauri::{AppHandle, Listener};

    use super::{ImportSettings, import_files};

    /// Serializes the tests: they share one `AppHandle`, and a stray
    /// `import-complete` from a sibling test running in parallel would be
    /// indistinguishable from this test's own.
    fn serial() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: Mutex<()> = Mutex::new(());
        LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// One headless `Wry` app for the whole test process, kept alive on a
    /// parked thread. `None` when there is no display to initialize GTK on.
    fn app_handle() -> Option<AppHandle> {
        static HANDLE: OnceLock<Option<AppHandle>> = OnceLock::new();
        HANDLE
            .get_or_init(|| {
                if cfg!(target_os = "linux")
                    && std::env::var_os("DISPLAY").is_none()
                    && std::env::var_os("WAYLAND_DISPLAY").is_none()
                {
                    eprintln!(
                        "SKIP: no display; run `xvfb-run -a cargo test --lib \
                         import_date_format_tests` to exercise import_files"
                    );
                    return None;
                }
                let (tx, rx) = mpsc::channel();
                std::thread::spawn(move || {
                    let app = tauri::Builder::<tauri::Wry>::default()
                        .any_thread()
                        .build(tauri::generate_context!())
                        .expect("build headless Wry app");
                    let _ = tx.send(app.handle().clone());
                    // Keep the app (and its runtime) alive for the whole
                    // process; `park` may return spuriously, so loop.
                    loop {
                        std::thread::park();
                    }
                });
                Some(rx.recv().expect("app thread handed back a handle"))
            })
            .clone()
    }

    /// A small valid JPEG plus a primary `.rrdata` sidecar carrying a fixed
    /// `DateTimeOriginal`, so the date folder is deterministic
    /// (`get_creation_date_from_path` reads the sidecar's exif map first).
    fn source_photo(dir: &Path) -> PathBuf {
        let jpeg = dir.join("photo.jpg");
        image::DynamicImage::ImageRgb8(image::RgbImage::from_pixel(
            8,
            8,
            image::Rgb([200, 120, 40]),
        ))
        .save_with_format(&jpeg, image::ImageFormat::Jpeg)
        .expect("write fixture jpeg");
        fs::write(
            dir.join("photo.jpg.rrdata"),
            r#"{"version":1,"rating":0,"adjustments":{},"exif":{"DateTimeOriginal":"2021:07:15 12:00:00"}}"#,
        )
        .expect("write fixture sidecar");
        jpeg
    }

    fn settings(date_folder_format: &str) -> ImportSettings {
        ImportSettings {
            filename_template: "{original_filename}".to_string(),
            organize_by_date: true,
            date_folder_format: date_folder_format.to_string(),
            delete_after_import: false,
        }
    }

    /// Runs one import and reports (command result, first terminal event
    /// within `wait`, number of entries created under `dest`).
    fn run_import(
        app: &AppHandle,
        date_folder_format: &str,
        wait: Duration,
    ) -> (
        Result<(), String>,
        Result<&'static str, RecvTimeoutError>,
        usize,
        PathBuf,
    ) {
        let src = tempfile::tempdir().expect("source dir");
        let dest = tempfile::tempdir().expect("destination dir");
        let jpeg = source_photo(src.path());

        let (tx, rx) = mpsc::channel();
        let tx_complete = tx.clone();
        let complete = app.listen_any("import-complete", move |_| {
            let _ = tx_complete.send("import-complete");
        });
        let tx_error = tx.clone();
        let error = app.listen_any("import-error", move |_| {
            let _ = tx_error.send("import-error");
        });

        let result = tauri::async_runtime::block_on(import_files(
            vec![jpeg.to_string_lossy().into_owned()],
            dest.path().to_string_lossy().into_owned(),
            settings(date_folder_format),
            app.clone(),
        ));
        let outcome = rx.recv_timeout(wait);

        app.unlisten(complete);
        app.unlisten(error);

        let created = fs::read_dir(dest.path()).map(Iterator::count).unwrap_or(0);
        let dest_path = dest.keep();
        let _ = src;
        (result, outcome, created, dest_path)
    }

    #[test]
    fn invalid_date_folder_format_is_rejected_before_any_file_is_touched() {
        let _serial = serial();
        let Some(app) = app_handle() else {
            return;
        };

        let (result, outcome, created, dest) = run_import(&app, "%Q", Duration::from_secs(5));
        let _ = fs::remove_dir_all(&dest);

        match result {
            Err(e) => {
                assert!(
                    e.contains("%Q") || e.to_lowercase().contains("format"),
                    "error should name the rejected date folder format, got: {e}"
                );
                assert_eq!(
                    created, 0,
                    "an invalid format must be rejected before anything is written"
                );
            }
            Ok(()) => panic!(
                "import_files accepted date_folder_format \"%Q\" and returned Ok(()). \
                 Terminal event within 5s: {outcome:?} (Err(Timeout) = neither \
                 import-error nor import-complete was emitted: chrono panicked inside \
                 the un-awaited spawn_blocking task, so the UI would hang). \
                 Entries created under the destination: {created}. Expected Err(..) \
                 naming the format, before any file is touched."
            ),
        }
    }

    #[test]
    fn valid_date_folder_format_imports_into_year_month_subfolder() {
        let _serial = serial();
        let Some(app) = app_handle() else {
            return;
        };

        let (result, outcome, _created, dest) = run_import(&app, "%Y/%m", Duration::from_secs(10));
        let imported = dest.join("2021").join("07").join("photo.jpg");
        let exists = imported.exists();
        let _ = fs::remove_dir_all(&dest);

        assert_eq!(result, Ok(()), "a valid format must import");
        assert_eq!(
            outcome,
            Ok("import-complete"),
            "the import task must finish and emit import-complete"
        );
        assert!(exists, "expected {} to exist", imported.display());
    }
}
