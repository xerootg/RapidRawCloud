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

impl CredentialStore for FileCredentialStore {
    fn load(&self) -> std::io::Result<Option<Credentials>> {
        let _ = &self.path;
        todo!("P1-U7: read + parse credentials.json (0600), returning None when absent (§3.6)")
    }

    fn store(&self, creds: &Credentials) -> std::io::Result<()> {
        let _ = creds;
        todo!("P1-U7: atomically write credentials.json and chmod 0600 on unix (§3.6)")
    }

    fn clear(&self) -> std::io::Result<()> {
        todo!("P1-U7: remove credentials.json if present (§3.6)")
    }
}
