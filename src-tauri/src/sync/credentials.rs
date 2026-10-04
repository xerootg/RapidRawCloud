//! Credential storage (ARCHITECTURE.md §3.6).
//!
//! Credentials **never** enter `settings.json` or the webview. On desktop
//! they live in `app_data_dir/rrcloud/credentials.json` with `0600`
//! permissions and are read only in Rust. The [`CredentialStore`] trait is
//! the seam an Android Keystore-backed store slots into later; the
//! frontend only ever learns `credentials_configured: bool`.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// S3 access credentials for the sync bucket. Deliberately *not* part of
/// [`crate::app_settings::AppSettings`].
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Credentials {
    pub access_key: String,
    pub secret_key: String,
}

impl Credentials {
    /// Whether both halves are present.
    pub fn is_complete(&self) -> bool {
        !self.access_key.is_empty() && !self.secret_key.is_empty()
    }
}

/// The platform seam: desktop uses [`FileCredentialStore`]; Android will
/// provide a Keystore-backed implementation.
pub trait CredentialStore: Send + Sync {
    /// Loads the stored credentials, or `None` when none are set.
    fn load(&self) -> std::io::Result<Option<Credentials>>;
    /// Persists `creds`, replacing any prior value.
    fn store(&self, creds: &Credentials) -> std::io::Result<()>;
    /// Removes any stored credentials.
    fn clear(&self) -> std::io::Result<()>;
}

/// Desktop credential store: a `0600` JSON file under
/// `app_data_dir/rrcloud/`.
pub struct FileCredentialStore {
    path: PathBuf,
}

impl FileCredentialStore {
    /// Builds a store rooted at `rrcloud_dir` (typically
    /// `app_data_dir/rrcloud`). The file itself is
    /// `<rrcloud_dir>/credentials.json`.
    pub fn new(rrcloud_dir: &Path) -> Self {
        FileCredentialStore {
            path: rrcloud_dir.join("credentials.json"),
        }
    }

    /// The backing file path.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Android credential store (ARCHITECTURE.md §5.1): a thin JNI shim over
/// the Keystore-backed `CredentialStore` Kotlin owns
/// (`tauri-plugin-rrcloud/android/src/main/java/com/plugin/rrcloud/
/// AndroidCredentialStore.kt`, `EncryptedSharedPreferences` wrapping a
/// Keystore-generated AES key). Credentials never touch Rust-side disk on
/// Android — this struct makes exactly the same three JNI calls
/// (`loadCredentialsJson`/`storeCredentialsJson`/`clearCredentials` on
/// `com.plugin.rrcloud.RrcloudBridge`) that `rrcloud_core::android::bridge`
/// makes from the worker process, so both the app-foreground command path
/// (this struct) and the WorkManager/FGS path read/write the identical
/// store.
///
/// Requires `ndk_context` to already be initialized
/// (`android_integration::initialize_android`, which the app's webview
/// setup calls before any sync command can run) — this struct attaches to
/// the JVM `ndk_context` already holds rather than taking its own
/// `Context`/`JNIEnv`, since every call site here is already inside the
/// app process with that context live.
#[cfg(target_os = "android")]
pub struct AndroidCredentialStore;

#[cfg(target_os = "android")]
impl AndroidCredentialStore {
    pub fn new() -> Self {
        AndroidCredentialStore
    }
}

#[cfg(target_os = "android")]
impl Default for AndroidCredentialStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(target_os = "android")]
impl CredentialStore for AndroidCredentialStore {
    fn load(&self) -> std::io::Result<Option<Credentials>> {
        let json = crate::android_integration::android_credential_store_load()
            .map_err(std::io::Error::other)?;
        let Some(json) = json else {
            return Ok(None);
        };
        let creds: Credentials = serde_json::from_str(&json)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        Ok(Some(creds))
    }

    fn store(&self, creds: &Credentials) -> std::io::Result<()> {
        let json = serde_json::to_string(creds)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        crate::android_integration::android_credential_store_save(&json)
            .map_err(std::io::Error::other)
    }

    fn clear(&self) -> std::io::Result<()> {
        crate::android_integration::android_credential_store_clear().map_err(std::io::Error::other)
    }
}

impl CredentialStore for FileCredentialStore {
    fn load(&self) -> std::io::Result<Option<Credentials>> {
        match std::fs::read(&self.path) {
            Ok(bytes) => {
                let creds: Credentials = serde_json::from_slice(&bytes)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                Ok(Some(creds))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn store(&self, creds: &Credentials) -> std::io::Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let json = serde_json::to_vec_pretty(creds)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        // Atomic temp+rename in the same directory so a crash mid-write can
        // never leave a half-written credential file.
        let dir = self.path.parent().unwrap_or_else(|| Path::new("."));
        let mut tmp = tempfile::NamedTempFile::new_in(dir)?;
        {
            use std::io::Write;
            tmp.write_all(&json)?;
            tmp.flush()?;
        }
        // Tighten to 0600 before the file carries a secret under its final
        // name (§3.6: read only in Rust, never world-readable).
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            tmp.as_file()
                .set_permissions(std::fs::Permissions::from_mode(0o600))?;
        }
        tmp.persist(&self.path)
            .map_err(|e| std::io::Error::other(e.error))?;
        Ok(())
    }

    fn clear(&self) -> std::io::Result<()> {
        match std::fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }
}
