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

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
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

/// Which cached-thumbnail size class is being seeded (§3.5 thumb seeding).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ThumbVariant {
    /// The grid thumbnail (`<hash>_small.jpg`).
    Small,
    /// The filmstrip / detail thumbnail (`<hash>_medium.jpg`).
    Medium,
}

impl ThumbVariant {
    /// The `_small` / `_medium` suffix used in both the durable app-data
    /// store and the hard-linked webview cache key (§3.5).
    pub fn suffix(self) -> &'static str {
        match self {
            ThumbVariant::Small => "small",
            ThumbVariant::Medium => "medium",
        }
    }
}

/// What one evictor pass did (§3.5 LRU eviction gate), returned by
/// [`SyncManager::run_evictor`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EvictionReport {
    /// Originals demoted back to 0-byte stubs this pass, LRU order.
    pub evicted: Vec<PathBuf>,
    /// Originals kept because they are pinned (never evicted, §3.5).
    pub kept_pinned: Vec<PathBuf>,
    /// Originals kept because their remote copy is not content-verified
    /// (no attest entry and read-back disabled/failed) — the "never evict
    /// unverified bytes" invariant (§3.5).
    pub kept_unverified: Vec<PathBuf>,
    /// Originals whose remote bytes were found wrong on the eviction
    /// read-back: routed to `corrupt_remote`, never evicted, never served.
    pub corrupt: Vec<PathBuf>,
    /// Total bytes still resident in hydrated originals after the pass.
    pub resident_bytes: u64,
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
    /// In-memory mirror of the redb `Stub`-state item set (§3.5): the set
    /// of absolute original paths that are currently 0-byte cloud stubs.
    /// Always compiled (so [`SyncManager::is_stub`] answers even in an
    /// inert build), populated only by the stub-creation / hydration paths
    /// under the `sync` feature. Empty ⇒ every `is_stub` query is `false`,
    /// which is exactly upstream behavior.
    stub_set: Mutex<HashSet<PathBuf>>,
    #[cfg(feature = "sync")]
    inner: std::sync::Mutex<Option<Arc<imp::Configured>>>,
}

impl SyncManager {
    /// Constructs an inert manager (the `AppState` initial value). It
    /// becomes live only after [`SyncManager::configure`].
    pub fn new_inert() -> Arc<Self> {
        Arc::new(SyncManager {
            configured: AtomicBool::new(false),
            stub_set: Mutex::new(HashSet::new()),
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
    ///
    /// **Reconfigure-while-running hazard (for the next unit's command
    /// layer):** this opens a *fresh* [`imp::Configured`] (a new redb
    /// `Database` on `state_dir/state.redb`) and only then swaps it into
    /// `inner`. [`run_once`](Self::run_once) / [`status`](Self::status) clone
    /// the current `Arc<Configured>` out of the lock and hold it across their
    /// `.await`s, so a `configure()` call made *while a cycle is still in
    /// flight* would try to open a second redb handle on the same file before
    /// the in-flight `Arc` releases the first — redb's advisory file lock
    /// then makes this return `Err` rather than cleanly rebinding. P1 drives
    /// these sequentially with no supervisor, so it is not reachable yet; the
    /// P2 command layer must stop/await any running cycle (or tear down the
    /// prior `Configured`) before calling `configure` again.
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

    /// A local item was deleted (§2.7 soft delete). DEFERRED: currently only
    /// logs — nothing durable is recorded and no remote tombstone is produced
    /// yet (v1 debt; `EngineConsumer` applies only Put/Del inbound in P1).
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

    /// A local item was moved/renamed (§2.7). DEFERRED: currently only logs —
    /// nothing durable is recorded and no remote move is produced yet (same
    /// status as `note_deleted`).
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

    // ---- §3.5 stubs / hydration / pinning / eviction / thumb seeding ----

    /// Whether `path` is currently a 0-byte cloud stub (§3.5). Reads the
    /// in-memory [`Self::stub_set`] mirror, so it is cheap and answers even
    /// in an inert / sync-off build (always `false` there). Backs
    /// [`crate::sync::hooks::is_stub`] and, through it,
    /// `file_management::is_cloud_placeholder`.
    pub fn is_stub(&self, path: &Path) -> bool {
        match self.stub_set.lock() {
            Ok(set) => set.contains(path),
            Err(poisoned) => poisoned.into_inner().contains(path),
        }
    }

    /// Records `path` as a stub in the in-memory mirror. The durable
    /// `ItemState::Stub` record and the 0-byte file are written by
    /// [`Self::create_stub`]; this keeps the fast-path set in sync.
    #[cfg(feature = "sync")]
    fn mark_stub(&self, path: &Path, is_stub: bool) {
        let mut set = match self.stub_set.lock() {
            Ok(set) => set,
            Err(poisoned) => poisoned.into_inner(),
        };
        if is_stub {
            set.insert(path.to_path_buf());
        } else {
            set.remove(path);
        }
    }

    /// Creates a 0-byte cloud stub for a remote-only original at
    /// `image_path` (§3.5): writes the empty file, sets its mtime to the
    /// remote original's `remote_mtime_unix` via `filetime` (so
    /// `compute_thumbnail_cache_hash` stays stable across a later
    /// hydration), and records an `ItemState::Stub` item carrying the
    /// remote `blake3_hex`/`size` so hydration and the eviction gate have
    /// the verified-remote facts.
    ///
    /// This is the API the engine apply / reconcile path drives when it
    /// learns of an original it does not hold locally (§3.5); the P2 tests
    /// drive it directly to stand in for that apply step.
    pub fn create_stub(
        &self,
        image_path: &Path,
        blake3_hex: &str,
        size: u64,
        remote_mtime_unix: i64,
    ) -> Result<(), SyncError> {
        #[cfg(feature = "sync")]
        {
            let cfg = self.configured()?;
            cfg.create_stub(image_path, blake3_hex, size, remote_mtime_unix)?;
            self.mark_stub(image_path, true);
            Ok(())
        }
        #[cfg(not(feature = "sync"))]
        {
            let _ = (image_path, blake3_hex, size, remote_mtime_unix);
            Err(SyncError::FeatureDisabled)
        }
    }

    /// Ensures the original at `path` is present locally, hydrating a stub
    /// if needed (§3.5): resumable ranged GET through the transfer engine,
    /// blake3 verify, atomic rename over the stub, restore mtime, mark the
    /// item `Hydrated`, emit an `attest` journal entry, bump LRU last
    /// access, and emit `sync-hydrate-progress` / `sync-hydrated`.
    /// Idempotent: a no-op that returns `path` when the item is already
    /// local. Synchronous (the guard sites call it inline); the ranged
    /// download runs on a bounded worker runtime internally.
    pub fn ensure_local(&self, path: &Path, reason: &str) -> Result<PathBuf, SyncError> {
        #[cfg(feature = "sync")]
        {
            // Fast path: not a stub ⇒ the real bytes are already present
            // (idempotent hydration, the common guard-site case).
            if !self.is_stub(path) {
                return Ok(path.to_path_buf());
            }
            let cfg = self.configured()?;
            let hydrated = cfg.hydrate(path, reason)?;
            self.mark_stub(path, false);
            Ok(hydrated)
        }
        #[cfg(not(feature = "sync"))]
        {
            let _ = reason;
            Ok(path.to_path_buf())
        }
    }

    /// Pins (or unpins) the originals at `image_paths` so the evictor never
    /// reclaims them (§3.5). A directory path fans out to every item under
    /// it ("pin this folder offline"). Returns the number of items whose
    /// pin flag changed.
    pub fn pin_paths(&self, image_paths: &[PathBuf], pinned: bool) -> Result<usize, SyncError> {
        #[cfg(feature = "sync")]
        {
            let cfg = self.configured()?;
            cfg.pin_paths(image_paths, pinned)
        }
        #[cfg(not(feature = "sync"))]
        {
            let _ = (image_paths, pinned);
            Err(SyncError::FeatureDisabled)
        }
    }

    /// Runs one LRU eviction pass (§3.5): keeps the sum of hydrated-original
    /// sizes `≤ settings.sync.cache_size_gb` by demoting the
    /// least-recently-accessed, **non-pinned**, `Synced`-state originals
    /// back to 0-byte stubs (restoring the remote mtime so thumbnail keys
    /// survive) — and **only** originals whose remote copy is
    /// content-verified: `verified_remote == true` and (an `attest` journal
    /// entry exists for the blake3 **or** a one-time full ranged-GET
    /// re-hash confirms the remote bytes). A hash mismatch routes the item
    /// to `corrupt_remote` and never evicts it (the "never evict unverified
    /// bytes" invariant).
    pub async fn run_evictor(&self) -> Result<EvictionReport, SyncError> {
        #[cfg(feature = "sync")]
        {
            let cfg = self.configured()?;
            let report = cfg.run_evictor().await?;
            for p in &report.evicted {
                self.mark_stub(p, true);
            }
            Ok(report)
        }
        #[cfg(not(feature = "sync"))]
        {
            Err(SyncError::FeatureDisabled)
        }
    }

    /// [`Self::run_evictor`] with an explicit byte budget instead of
    /// `settings.sync.cache_size_gb` — the test seam that exercises the LRU
    /// order and the verification gate at byte granularity (the public
    /// setting is whole gigabytes).
    pub async fn run_evictor_with_budget(
        &self,
        max_resident_bytes: u64,
    ) -> Result<EvictionReport, SyncError> {
        #[cfg(feature = "sync")]
        {
            let cfg = self.configured()?;
            let report = cfg.run_evictor_with_budget(max_resident_bytes).await?;
            for p in &report.evicted {
                self.mark_stub(p, true);
            }
            Ok(report)
        }
        #[cfg(not(feature = "sync"))]
        {
            let _ = max_resident_bytes;
            Err(SyncError::FeatureDisabled)
        }
    }

    /// The current per-item sync-lane state for `image_path`, as the
    /// snake_case string the §3.8 `sync-item-state` event and the
    /// `ImageFile.sync_state` badge use (e.g. `"stub"`, `"hydrated"`,
    /// `"synced"`, `"corrupt_remote"`). `None` when the path has no item
    /// record (or sync is off). A read-only query, not part of the §3.5
    /// mutation surface.
    pub fn item_sync_state(&self, image_path: &Path) -> Option<String> {
        #[cfg(feature = "sync")]
        {
            self.configured()
                .ok()
                .and_then(|cfg| cfg.item_sync_state(image_path))
        }
        #[cfg(not(feature = "sync"))]
        {
            let _ = image_path;
            None
        }
    }

    /// Seeds a downloaded/engine-provided JPEG thumbnail (§3.5): stores it
    /// durably under `app_data_dir/rrcloud/thumbs/<content_id>_<variant>.jpg`
    /// and surfaces it to the webview by hard-linking (copy fallback) into
    /// `cache_thumbnails_dir/<hash>_<variant>.jpg`, where `<hash>` is the
    /// stub-path thumbnail cache key — so the existing asset-protocol scope
    /// (`$APPCACHE/thumbnails/*`) and `tauri.conf.json` stay untouched.
    /// Returns the cache path that was linked. Emits `thumbnail-generated`
    /// so the existing UI picks it up.
    pub fn seed_thumbnail(
        &self,
        image_path: &Path,
        variant: ThumbVariant,
        jpeg_bytes: &[u8],
        cache_thumbnails_dir: &Path,
    ) -> Result<PathBuf, SyncError> {
        #[cfg(feature = "sync")]
        {
            let cfg = self.configured()?;
            cfg.seed_thumbnail(image_path, variant, jpeg_bytes, cache_thumbnails_dir)
        }
        #[cfg(not(feature = "sync"))]
        {
            let _ = (image_path, variant, jpeg_bytes, cache_thumbnails_dir);
            Err(SyncError::FeatureDisabled)
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
/// through. This unit installs the manager only — it is left unconfigured,
/// so sync is inert until the P2 command layer lands. The §3.3 supervisor
/// task and the credential/settings load are part of that P2 work (command
/// registration / `configure`, per UPSTREAM_TOUCHES.md), not this unit.
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

    use super::{EvictionReport, SyncError, SyncState, SyncStatus, ThumbVariant};
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

            // Timeouts are all `None` here (no reqwest network bound).
            // `exit_flush` time-boxes shutdown by wrapping `flush_uploads` in
            // `tokio::time::timeout`, so shutdown is safe; but `run_cycle` /
            // `run_once` and the backend probe have no outer bound. This is
            // unreachable in P1 — the manager is installed-but-unconfigured in
            // setup(), there is no supervisor, and the tests drive cycles
            // under their own harness bounds — so it is not a live defect for
            // this unit. It becomes relevant at the P2 command layer, where a
            // `sync_run_once` command against an unreachable endpoint could
            // hang indefinitely; close it there with a sensible default
            // `request_timeout` (and/or an outer bound on `run_once`) rather
            // than leaving the per-call network unbounded (P1-U7 round-3
            // minor).
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

        /// §3.5 stub creation — writes the 0-byte placeholder at
        /// `image_path`, sets its mtime to `remote_mtime_unix` via
        /// `filetime`, and records an `ItemState::Stub` item carrying the
        /// remote `blake3_hex`/`size`/`verified_remote` facts. Scaffold:
        /// unimplemented until the P2 green pass.
        pub fn create_stub(
            &self,
            image_path: &Path,
            blake3_hex: &str,
            size: u64,
            remote_mtime_unix: i64,
        ) -> Result<(), SyncError> {
            let _ = (
                &self.db,
                &self.sync_root,
                image_path,
                blake3_hex,
                size,
                remote_mtime_unix,
            );
            todo!("P2 green: write 0-byte stub + set remote mtime + record ItemState::Stub")
        }

        /// §3.5 hydration — the resumable ranged download of a stub's real
        /// bytes, blake3 verify, atomic install over the stub, mtime
        /// restore, `Hydrated` transition, `attest` entry, LRU bump, and
        /// `sync-hydrate-progress` / `sync-hydrated` emits. Synchronous
        /// wrapper over the async transfer-engine download (runs on a
        /// bounded worker runtime, like the exit-flush path). Scaffold:
        /// unimplemented until the P2 green pass.
        pub fn hydrate(&self, image_path: &Path, reason: &str) -> Result<PathBuf, SyncError> {
            let _ = (
                &self.db,
                &self.s3,
                &self.bucket,
                &self.sync_root,
                image_path,
                reason,
            );
            todo!("P2 green: resumable ranged GET + blake3 verify + atomic install + attest")
        }

        /// §3.5 pin/unpin — sets the `pinned` flag on each named original,
        /// fanning a directory out to every item under it. Scaffold:
        /// unimplemented until the P2 green pass.
        pub fn pin_paths(&self, image_paths: &[PathBuf], pinned: bool) -> Result<usize, SyncError> {
            let _ = (&self.db, &self.sync_root, image_paths, pinned);
            todo!("P2 green: set pinned flag, folder fan-out")
        }

        /// §3.5 LRU eviction pass using `settings.sync.cache_size_gb` as the
        /// resident-bytes budget. Scaffold: unimplemented until the P2 green
        /// pass.
        pub async fn run_evictor(&self) -> Result<EvictionReport, SyncError> {
            let budget = (self.settings.cache_size_gb as u64) << 30;
            self.run_evictor_with_budget(budget).await
        }

        /// §3.5 LRU eviction pass with an explicit `max_resident_bytes`
        /// budget — the attestation/read-back-gated demotion of
        /// least-recently-accessed non-pinned verified originals back to
        /// stubs. Scaffold: unimplemented until the P2 green pass.
        pub async fn run_evictor_with_budget(
            &self,
            max_resident_bytes: u64,
        ) -> Result<EvictionReport, SyncError> {
            let _ = (
                &self.db,
                &self.s3,
                &self.bucket,
                &self.sync_root,
                max_resident_bytes,
            );
            todo!("P2 green: LRU eviction with the 'never evict unverified bytes' gate")
        }

        /// §3.5 thumb seeding — durable app-data store + hard-link into the
        /// webview thumbnail cache under the stub-path cache key. Scaffold:
        /// unimplemented until the P2 green pass.
        pub fn seed_thumbnail(
            &self,
            image_path: &Path,
            variant: ThumbVariant,
            jpeg_bytes: &[u8],
            cache_thumbnails_dir: &Path,
        ) -> Result<PathBuf, SyncError> {
            let _ = (
                &self.db,
                &self.sync_root,
                image_path,
                variant,
                jpeg_bytes,
                cache_thumbnails_dir,
            );
            todo!("P2 green: durable thumbs store + hard-link into $APPCACHE/thumbnails")
        }

        /// Read-only query of an item's current state as a snake_case string
        /// (§3.8 badge / test observability). Not part of the §3.5 mutation
        /// surface, so it is implemented rather than scaffolded.
        pub fn item_sync_state(&self, image_path: &Path) -> Option<String> {
            let rk = relkey(image_path, &self.sync_root).ok()?;
            let record = self.db.get_item(&rk).ok().flatten()?;
            serde_json::to_value(record.state)
                .ok()
                .and_then(|v| v.as_str().map(str::to_string))
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
