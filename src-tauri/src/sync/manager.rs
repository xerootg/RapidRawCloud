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
    inner: std::sync::Mutex<Option<Arc<imp::Configured>>>,
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
            let configured = imp::Configured::open(settings, creds, sync_root, state_dir)?;
            let mut guard = self
                .inner
                .lock()
                .map_err(|_| SyncError::msg("sync manager lock poisoned"))?;
            *guard = Some(Arc::new(configured));
            self.configured.store(true, Ordering::SeqCst);
            Ok(())
        }
        #[cfg(not(feature = "sync"))]
        {
            let _ = (settings, creds, sync_root, state_dir);
            Err(SyncError::FeatureDisabled)
        }
    }

    /// The configured inner handle, cloned out so async work does not hold
    /// the lock across `.await`.
    #[cfg(feature = "sync")]
    fn configured(&self) -> Result<Arc<imp::Configured>, SyncError> {
        self.inner
            .lock()
            .map_err(|_| SyncError::msg("sync manager lock poisoned"))?
            .clone()
            .ok_or(SyncError::NotConfigured)
    }

    /// Runs one full sync cycle to quiescence: admit quiesced-dirty items,
    /// pump the upload queue, publish the staged journal, poll the inbound
    /// journal and apply it, then pump the download queue (§3.3, driven by
    /// the rrcloud-core engine pure functions). Returns the resulting
    /// [`SyncStatus`].
    pub async fn run_once(&self) -> Result<SyncStatus, SyncError> {
        #[cfg(feature = "sync")]
        {
            let cfg = self.configured()?;
            cfg.run_cycle().await
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
            match self.configured() {
                Ok(cfg) => cfg.status_snapshot(0, 0),
                Err(_) => SyncStatus::default(),
            }
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
            match self.configured() {
                Ok(cfg) => cfg.dirty_count(),
                Err(_) => 0,
            }
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
            if let Ok(cfg) = self.configured() {
                cfg.note_local_sidecar(sidecar_path, origin);
            }
        }
        #[cfg(not(feature = "sync"))]
        {
            let _ = (sidecar_path, origin);
        }
    }

    /// A new original landed locally (import / derived output / duplicate /
    /// copy): records it through the §2.5 intake, keyed by its own relkey,
    /// so the next cycle uploads it (§3.4 new-original hooks). Best-effort.
    pub fn note_new_original(&self, path: &std::path::Path) {
        #[cfg(feature = "sync")]
        {
            if let Ok(cfg) = self.configured() {
                cfg.note_new_original(path);
            }
        }
        #[cfg(not(feature = "sync"))]
        {
            let _ = path;
        }
    }

    /// A local item was deleted (§2.7 soft delete). Records the intent; the
    /// async remote tombstone rides the supervisor in a later pass.
    pub fn note_deleted(&self, path: &std::path::Path) {
        #[cfg(feature = "sync")]
        {
            if let Ok(cfg) = self.configured() {
                cfg.note_deleted(path);
            }
        }
        #[cfg(not(feature = "sync"))]
        {
            let _ = path;
        }
    }

    /// A local item was moved/renamed (§2.7). Records the intent; the async
    /// remote move rides the supervisor in a later pass.
    pub fn note_moved(&self, from: &std::path::Path, to: &std::path::Path) {
        #[cfg(feature = "sync")]
        {
            if let Ok(cfg) = self.configured() {
                cfg.note_moved(from, to);
            }
        }
        #[cfg(not(feature = "sync"))]
        {
            let _ = (from, to);
        }
    }

    /// The §3.7 editor-hold flush hint for `path`. The P1 admission policy
    /// quiesces every dirty item, so this is advisory for now.
    pub fn note_flush_hint(&self, path: &std::path::Path) {
        #[cfg(feature = "sync")]
        {
            let _ = (self.configured(), path);
        }
        #[cfg(not(feature = "sync"))]
        {
            let _ = path;
        }
    }

    /// How many times the chokepoint has notified this manager of a
    /// semantically-changed sidecar save — the §2.5 churn-gate observable
    /// the chokepoint tests assert on (a churn rewrite must not bump it).
    pub fn notify_count(&self) -> usize {
        #[cfg(feature = "sync")]
        {
            match self.configured() {
                Ok(cfg) => cfg.notify_count(),
                Err(_) => 0,
            }
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
            let cfg = match self.configured() {
                Ok(cfg) => cfg,
                // Nothing to flush when unconfigured — a clean, immediate
                // return (never an error on the shutdown path).
                Err(_) => return Ok(()),
            };
            // Hard-bound the drain so shutdown can never hang: on timeout we
            // return cleanly, leaving queued work durable for the next run.
            match tokio::time::timeout(budget, cfg.flush_uploads()).await {
                Ok(result) => result,
                Err(_) => {
                    log::warn!("exit flush timed out after {budget:?}; queued work left durable");
                    Ok(())
                }
            }
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
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use rrcloud_core::clock::DeviceId;
    use rrcloud_core::engine::{
        ChangeOutcome, EngineConsumer, LocalScan, admit_pending, notify_local_change,
    };
    use rrcloud_core::journal::Kind;
    use rrcloud_core::keys::relkey;
    use rrcloud_core::publisher::publish_pending;
    use rrcloud_core::reader::poll;
    use rrcloud_core::s3::{S3Client, S3Config};
    use rrcloud_core::state::{ItemState, StateError, SyncDb};
    use rrcloud_core::transfer::{
        BackendProfile, CancelFlag, TransferConfig, probe_backend, pump_downloads, pump_uploads,
        stored_backend_profile,
    };

    use super::{SyncError, SyncState, SyncStatus};
    use crate::app_settings::SyncSettings;
    use crate::sync::WriteOrigin;
    use crate::sync::credentials::Credentials;

    /// Transfer concurrency per lane (§2.4 / §3.3). Small and fixed: the
    /// desktop app is not the bulk worker.
    const TRANSFER_CONCURRENCY: usize = 2;

    /// Maps any displayable engine error into a [`SyncError`].
    fn se<E: std::fmt::Display>(e: E) -> SyncError {
        SyncError::Message(e.to_string())
    }

    /// The file's mtime in unix nanoseconds, or 0 when unavailable.
    fn mtime_unix_ns(path: &Path) -> i64 {
        std::fs::metadata(path)
            .and_then(|m| m.modified())
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0)
    }

    /// The live engine handle populated by `configure`: the redb state
    /// store, the hand-rolled S3 client, the resolved sync root/bucket, the
    /// lazily-probed backend profile, and the §2.5 notification counter the
    /// chokepoint tests observe.
    pub struct Configured {
        /// Retained for the supervisor task and the command layer (next
        /// unit); not read on the P1 cycle paths yet.
        #[allow(dead_code)]
        settings: SyncSettings,
        sync_root: PathBuf,
        bucket: String,
        db: Arc<SyncDb>,
        s3: Arc<S3Client>,
        backend: std::sync::Mutex<Option<BackendProfile>>,
        notify_count: AtomicUsize,
    }

    impl Configured {
        /// Opens the redb state store under `state_dir`, persists the
        /// credentials to the file-backed store, and builds the S3 client.
        /// No network: a bad endpoint surfaces at the first `run_cycle`.
        pub fn open(
            settings: SyncSettings,
            creds: Credentials,
            sync_root: PathBuf,
            state_dir: PathBuf,
        ) -> Result<Self, SyncError> {
            std::fs::create_dir_all(&state_dir).map_err(se)?;

            // Credentials live only in the file-backed store (§3.6), never
            // in settings.json / the webview.
            let store = crate::sync::credentials::FileCredentialStore::new(&state_dir);
            if creds.is_complete() {
                crate::sync::credentials::CredentialStore::store(&store, &creds).map_err(se)?;
            }

            let redb_path = state_dir.join("state.redb");
            // Fresh databases need a device id minted; an existing one keeps
            // its stored identity. Try without first, then mint on demand.
            let db = match SyncDb::open(&redb_path, None) {
                Err(StateError::DeviceIdRequired) => {
                    let id = DeviceId::new(uuid::Uuid::new_v4().to_string())
                        .map_err(|e| SyncError::msg(format!("mint device id: {e}")))?;
                    SyncDb::open(&redb_path, Some(id)).map_err(se)?
                }
                other => other.map_err(se)?,
            };

            let s3 = S3Client::new(S3Config {
                endpoint: settings.endpoint.clone(),
                region: settings.region.clone(),
                access_key_id: creds.access_key.clone(),
                secret_access_key: creds.secret_key.clone(),
                connect_timeout: None,
                read_timeout: None,
                request_timeout: None,
            })
            .map_err(se)?;

            Ok(Configured {
                bucket: settings.bucket.clone(),
                settings,
                sync_root,
                db: Arc::new(db),
                s3: Arc::new(s3),
                backend: std::sync::Mutex::new(None),
                notify_count: AtomicUsize::new(0),
            })
        }

        pub fn notify_count(&self) -> usize {
            self.notify_count.load(Ordering::SeqCst)
        }

        /// The §2.5 local-change intake for a just-written sidecar: maps the
        /// path to a relkey relative to the sync root and records the change
        /// (the churn gate lives in [`notify_local_change`]). Bumps the
        /// notification counter only when a semantic change was recorded.
        pub fn note_local_sidecar(&self, sidecar_path: &Path, origin: WriteOrigin) {
            // `origin` is reserved for §2.6 per-field provenance; the P1
            // intake keys purely on the semantic hash.
            let _ = origin;
            let bytes = match std::fs::read(sidecar_path) {
                Ok(b) => b,
                Err(e) => {
                    log::warn!("sync intake: read {}: {e}", sidecar_path.display());
                    return;
                }
            };
            let rk = match relkey(sidecar_path, &self.sync_root) {
                Ok(r) => r,
                Err(e) => {
                    log::warn!("sync intake: relkey {}: {e}", sidecar_path.display());
                    return;
                }
            };
            let scan = LocalScan {
                size: bytes.len() as u64,
                mtime_unix_ns: mtime_unix_ns(sidecar_path),
                bytes: &bytes,
            };
            match notify_local_change(&self.db, &rk, Kind::Sidecar, &scan) {
                Ok(ChangeOutcome::Unchanged) => {}
                Ok(_) => {
                    self.notify_count.fetch_add(1, Ordering::SeqCst);
                }
                Err(e) => log::warn!("sync intake: {}: {e}", sidecar_path.display()),
            }
        }

        /// §2.5 intake for a new original, keyed by its own relkey.
        pub fn note_new_original(&self, path: &Path) {
            let bytes = match std::fs::read(path) {
                Ok(b) => b,
                Err(e) => {
                    log::warn!("sync intake (original): read {}: {e}", path.display());
                    return;
                }
            };
            let rk = match relkey(path, &self.sync_root) {
                Ok(r) => r,
                Err(e) => {
                    log::warn!("sync intake (original): relkey {}: {e}", path.display());
                    return;
                }
            };
            let scan = LocalScan {
                size: bytes.len() as u64,
                mtime_unix_ns: mtime_unix_ns(path),
                bytes: &bytes,
            };
            if let Err(e) = notify_local_change(&self.db, &rk, Kind::Original, &scan) {
                log::warn!("sync intake (original): {}: {e}", path.display());
            }
        }

        /// §2.7 soft delete — intent only in this unit. The async remote
        /// tombstone (`engine::delete_item`) rides a later pass; recording
        /// it here synchronously would require network, which the hook must
        /// not perform. Logged so the gap is visible.
        pub fn note_deleted(&self, path: &Path) {
            log::debug!(
                "sync: local delete noted for {} (remote tombstone deferred)",
                path.display()
            );
        }

        /// §2.7 remote move — intent only in this unit (see `note_deleted`).
        pub fn note_moved(&self, from: &Path, to: &Path) {
            log::debug!(
                "sync: local move noted {} -> {} (remote move deferred)",
                from.display(),
                to.display()
            );
        }

        /// Lazily resolves the §2.4 backend profile: the persisted value if
        /// present, else a one-shot digest probe (persisted by the probe).
        async fn ensure_backend(&self) -> Result<BackendProfile, SyncError> {
            if let Some(b) = *self
                .backend
                .lock()
                .map_err(|_| se("backend lock poisoned"))?
            {
                return Ok(b);
            }
            let profile = match stored_backend_profile(&self.db).map_err(se)? {
                Some(b) => b,
                None => probe_backend(&self.db, self.s3.as_ref(), &self.bucket)
                    .await
                    .map_err(se)?,
            };
            *self
                .backend
                .lock()
                .map_err(|_| se("backend lock poisoned"))? = Some(profile);
            Ok(profile)
        }

        fn transfer_cfg(&self, backend: BackendProfile) -> TransferConfig {
            TransferConfig::new(self.bucket.clone(), self.sync_root.clone(), backend)
        }

        /// The upload half of a cycle (§3.3): admit quiesced-dirty items,
        /// pump the upload queue, then publish the staged journal.
        async fn drain_uploads(&self, cfg: &TransferConfig) -> Result<usize, SyncError> {
            // P1 admission policy: quiesce every dirty item (§3.7 debounce
            // lives in the frontend flush hints, not modeled here yet).
            admit_pending(&self.db, |_, _| true).map_err(se)?;
            let cancel = CancelFlag::new();
            let summary = pump_uploads(
                &self.db,
                self.s3.as_ref(),
                cfg,
                TRANSFER_CONCURRENCY,
                &cancel,
            )
            .await
            .map_err(se)?;
            if let Some((relkey, err)) = summary.failed.into_iter().next() {
                return Err(SyncError::msg(format!("upload failed for {relkey}: {err}")));
            }
            publish_pending(&self.db, self.s3.as_ref(), &self.bucket)
                .await
                .map_err(se)?;
            Ok(summary.completed.len())
        }

        /// One full sync cycle to quiescence (§3.3): upload lane, then poll
        /// + apply the inbound journal, then pump the download queue.
        pub async fn run_cycle(&self) -> Result<SyncStatus, SyncError> {
            let backend = self.ensure_backend().await?;
            let cfg = self.transfer_cfg(backend);

            let uploaded = self.drain_uploads(&cfg).await?;

            // Inbound journal: poll through a fresh EngineConsumer applying
            // under the §2.6 unified rule (no byte transfers — the pump
            // below moves bytes).
            let mut events = ();
            {
                let mut consumer =
                    EngineConsumer::new(&self.db, self.sync_root.clone(), &mut events)
                        .map_err(se)?;
                poll(&self.db, self.s3.as_ref(), &self.bucket, &mut consumer)
                    .await
                    .map_err(se)?;
            }

            let cancel = CancelFlag::new();
            let down = pump_downloads(
                &self.db,
                self.s3.as_ref(),
                &cfg,
                TRANSFER_CONCURRENCY,
                &cancel,
            )
            .await
            .map_err(se)?;
            if let Some((relkey, err)) = down.failed.into_iter().next() {
                return Err(SyncError::msg(format!(
                    "download failed for {relkey}: {err}"
                )));
            }

            Ok(self.status_snapshot(uploaded, down.completed.len()))
        }

        /// The bounded exit-flush body (§3.3): drain the upload lane only
        /// (queued small sidecars), leaving inbound work for the next run.
        pub async fn flush_uploads(&self) -> Result<(), SyncError> {
            let backend = self.ensure_backend().await?;
            let cfg = self.transfer_cfg(backend);
            self.drain_uploads(&cfg).await.map(|_| ())
        }

        /// Items in an upload-lane state (dirty-and-not-yet-backed-up).
        pub fn dirty_count(&self) -> usize {
            self.count_states(&[
                ItemState::Dirty,
                ItemState::Queued,
                ItemState::Uploading,
                ItemState::Verifying,
            ])
        }

        fn count_states(&self, states: &[ItemState]) -> usize {
            match self.db.iter_items() {
                Ok(items) => items
                    .into_iter()
                    .filter(|(_, r)| !r.deleted && states.contains(&r.state))
                    .count(),
                Err(e) => {
                    log::warn!("sync: iter_items for status: {e}");
                    0
                }
            }
        }

        /// Projects the current item states into a [`SyncStatus`], carrying
        /// the last cycle's `uploaded`/`downloaded` counts.
        pub fn status_snapshot(&self, uploaded: usize, downloaded: usize) -> SyncStatus {
            let pending_up = self.dirty_count();
            let pending_down = self.count_states(&[ItemState::PendingDown, ItemState::Downloading]);
            let state = if pending_up > 0 || pending_down > 0 {
                SyncState::Syncing
            } else {
                SyncState::Idle
            };
            SyncStatus {
                configured: true,
                state,
                pending_up,
                pending_down,
                uploaded,
                downloaded,
                dirty_unbacked: pending_up,
            }
        }
    }
}
