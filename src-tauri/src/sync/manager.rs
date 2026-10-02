//! [`SyncManager`]: the per-process sync lifecycle object (ARCHITECTURE.md
//! §3.3), held as `Arc<SyncManager>` in `AppState`.
//!
//! It exposes a plain-Rust API — [`SyncManager::configure`],
//! [`SyncManager::run_once`], [`SyncManager::status`],
//! [`SyncManager::exit_flush`] — that the integration tests drive
//! directly, so the Tauri command/IPC surface can stay in the next unit
//! (§3.3). When the `sync` feature is off the manager is an inert shell:
//! `new_inert` constructs it, `is_configured` is always `false`, and the
//! engine methods report [`SyncError::FeatureDisabled`].

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::app_settings::SyncSettings;
use crate::sync::WriteOrigin;
use crate::sync::credentials::Credentials;

/// High-level sync state surfaced to the UI (§3.8).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SyncState {
    #[default]
    Idle,
    Syncing,
    Offline,
    Error,
}

/// A snapshot of the manager's progress, returned by
/// [`SyncManager::status`] and [`SyncManager::run_once`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SyncStatus {
    pub configured: bool,
    pub state: SyncState,
    /// Items dirty-and-admitted, awaiting upload.
    pub pending_up: usize,
    /// Items with a known remote head not yet downloaded.
    pub pending_down: usize,
    /// Objects uploaded during the last `run_once`.
    pub uploaded: usize,
    /// Objects downloaded during the last `run_once`.
    pub downloaded: usize,
    /// Dirty items not yet backed up remotely (§3.3 exit-flush accounting).
    pub dirty_unbacked: usize,
}

/// Errors from the manager's plain-Rust API.
#[derive(Debug)]
pub enum SyncError {
    /// The `sync` cargo feature is not compiled in.
    FeatureDisabled,
    /// A method needing configuration ran before [`SyncManager::configure`].
    NotConfigured,
    /// Any other failure, carrying a message.
    Message(String),
}

impl SyncError {
    /// Convenience constructor for a message error.
    pub fn msg(s: impl Into<String>) -> Self {
        SyncError::Message(s.into())
    }
}

impl std::fmt::Display for SyncError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SyncError::FeatureDisabled => write!(f, "sync feature is disabled"),
            SyncError::NotConfigured => write!(f, "sync manager is not configured"),
            SyncError::Message(m) => write!(f, "{m}"),
        }
    }
}

impl std::error::Error for SyncError {}

impl From<String> for SyncError {
    fn from(s: String) -> Self {
        SyncError::Message(s)
    }
}

/// How the engine was pointed at a bucket + local roots for a run.
///
/// `sync_root` is the library directory (keys are relative to it);
/// `state_dir` holds the redb state store and credential file
/// (`app_data_dir/rrcloud` on desktop).
#[derive(Clone, Debug)]
pub struct SyncConfig {
    pub settings: SyncSettings,
    pub sync_root: PathBuf,
    pub state_dir: PathBuf,
}

/// The sync lifecycle object (§3.3).
pub struct SyncManager {
    configured: AtomicBool,
    #[cfg(feature = "sync")]
    inner: std::sync::Mutex<Option<imp::Configured>>,
}

impl SyncManager {
    /// Constructs an inert manager (the `AppState` initial value). It
    /// becomes live only after [`SyncManager::configure`].
    pub fn new_inert() -> Arc<Self> {
        Arc::new(SyncManager {
            configured: AtomicBool::new(false),
            #[cfg(feature = "sync")]
            inner: std::sync::Mutex::new(None),
        })
    }

    /// Whether [`configure`](Self::configure) has run.
    pub fn is_configured(&self) -> bool {
        self.configured.load(Ordering::SeqCst)
    }

    /// Points the engine at a bucket and local roots: opens the redb state
    /// store under `state_dir`, builds the S3 client from `settings` +
    /// `creds`, and arms the supervisor. Inert (no network) — a bad
    /// endpoint surfaces at [`run_once`](Self::run_once), not here.
    pub fn configure(
        &self,
        settings: SyncSettings,
        creds: Credentials,
        sync_root: PathBuf,
        state_dir: PathBuf,
    ) -> Result<(), SyncError> {
        #[cfg(feature = "sync")]
        {
            let _ = (&settings, &creds, &sync_root, &state_dir, &self.inner);
            todo!(
                "P1-U7: open redb at state_dir, build S3 client, store SyncConfig, mark configured (§3.3)"
            )
        }
        #[cfg(not(feature = "sync"))]
        {
            let _ = (settings, creds, sync_root, state_dir);
            Err(SyncError::FeatureDisabled)
        }
    }

    /// Runs one full sync cycle to quiescence: admit quiesced-dirty items,
    /// pump the upload queue, publish the staged journal, poll the inbound
    /// journal and apply it, then pump the download queue (§3.3, driven by
    /// the rrcloud-core engine pure functions). Returns the resulting
    /// [`SyncStatus`].
    pub async fn run_once(&self) -> Result<SyncStatus, SyncError> {
        #[cfg(feature = "sync")]
        {
            let _ = &self.inner;
            todo!(
                "P1-U7: admit -> pump_uploads -> publish_pending -> poll/apply -> pump_downloads (§3.3)"
            )
        }
        #[cfg(not(feature = "sync"))]
        {
            Err(SyncError::FeatureDisabled)
        }
    }

    /// A status snapshot without running a cycle.
    pub fn status(&self) -> SyncStatus {
        #[cfg(feature = "sync")]
        {
            let _ = &self.inner;
            todo!("P1-U7: project redb item states into SyncStatus (§3.8)")
        }
        #[cfg(not(feature = "sync"))]
        {
            SyncStatus::default()
        }
    }

    /// Number of dirty (not-yet-backed-up) items in the state store — the
    /// churn-gate observable for the chokepoint tests and §3.3 exit-flush
    /// accounting.
    pub fn dirty_count(&self) -> usize {
        #[cfg(feature = "sync")]
        {
            let _ = &self.inner;
            todo!("P1-U7: count Dirty/pending-upload items in redb")
        }
        #[cfg(not(feature = "sync"))]
        {
            0
        }
    }

    /// The §3.4 step-5 entry point the chokepoint reaches through the
    /// global-manager hook: record that `sidecar_path` was saved with a
    /// changed semantic hash, mapping it to a relkey relative to
    /// `sync_root` and marking the item dirty for the next cycle.
    pub fn note_local_sidecar(&self, sidecar_path: &std::path::Path, origin: WriteOrigin) {
        #[cfg(feature = "sync")]
        {
            let _ = (sidecar_path, origin, &self.inner);
            todo!(
                "P1-U7: relkey(sidecar_path, sync_root) -> engine::notify_local_change (§3.4/§2.5)"
            )
        }
        #[cfg(not(feature = "sync"))]
        {
            let _ = (sidecar_path, origin);
        }
    }

    /// How many times the chokepoint has notified this manager of a
    /// semantically-changed sidecar save — the §2.5 churn-gate observable
    /// the chokepoint tests assert on (a churn rewrite must not bump it).
    pub fn notify_count(&self) -> usize {
        #[cfg(feature = "sync")]
        {
            let _ = &self.inner;
            todo!("P1-U7: expose the sidecar-saved notification counter")
        }
        #[cfg(not(feature = "sync"))]
        {
            0
        }
    }

    /// Bounded opportunistic flush of queued small sidecar uploads on exit
    /// (§3.3): drains within `budget`, or returns cleanly on timeout —
    /// never hangs.
    pub async fn exit_flush(&self, budget: Duration) -> Result<(), SyncError> {
        #[cfg(feature = "sync")]
        {
            let _ = (budget, &self.inner);
            todo!("P1-U7: bounded drain of queued small sidecar uploads, commit state (§3.3)")
        }
        #[cfg(not(feature = "sync"))]
        {
            let _ = budget;
            Ok(())
        }
    }
}

/// Wires the manager into `setup()` next to `start_thumbnail_workers`
/// (§3.3): installs it as the process-global manager the hooks route
/// through. The supervisor task and credential/settings load land in the
/// GREEN pass.
pub fn start_in_setup(app: &tauri::AppHandle, manager: &Arc<SyncManager>) {
    let _ = app;
    crate::sync::install_global_manager(manager.clone());
}

#[cfg(feature = "sync")]
mod imp {
    use super::SyncConfig;

    /// The live engine handle populated by `configure`. Fields land in the
    /// GREEN pass (redb `SyncDb`, the hand-rolled S3 client, the
    /// `TransferConfig`, and the supervisor `JoinHandle`).
    pub struct Configured {
        #[allow(dead_code)]
        pub config: SyncConfig,
    }
}
