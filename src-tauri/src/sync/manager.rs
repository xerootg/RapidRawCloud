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

use serde::{Deserialize, Serialize};

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

/// A device in the shared registry (§2.10 / §3.8 device-management panel),
/// surfaced to the settings UI's "list/retire devices" view by
/// [`SyncManager::peer_devices`]. Serialized straight to the webview, so it
/// carries no secret material.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PeerDevice {
    /// The registry device id.
    pub device_id: String,
    /// This device's own id matches `device_id` (so the UI can mark "this
    /// device" and refuse to retire it out from under itself).
    pub is_self: bool,
    /// Last registry heartbeat, unix seconds (0 when unknown).
    pub last_seen_unix: i64,
    /// Whether the device has already been retired (§2.10 GC).
    pub retired: bool,
}

/// A soft-deleted item surfaced in the settings "Recently Deleted" view
/// (§3.8), returned by [`SyncManager::recently_deleted`] and restorable with
/// [`SyncManager::restore`].
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RecentlyDeleted {
    /// Absolute local path the item would be restored to.
    pub path: String,
    /// The library-relative key (stable across devices).
    pub relkey: String,
    /// Tombstone time, unix seconds (0 when unknown).
    pub deleted_unix: i64,
}

/// Which side of a conflict to keep when resolving one from the UI (§3.8
/// `sync-conflict`): the winning (version-vector) document, or the local
/// loser preserved as a copy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConflictKeep {
    /// Keep the version-vector winner; discard the local loser.
    Winner,
    /// Keep the local loser as a `-conflict` copy alongside the winner.
    Copy,
}

/// What one [`SyncManager::verify_library`] reconcile pass found (§3.5
/// wholeness reconcile), surfaced by the settings "Verify library" action.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VerifyReport {
    /// Items whose local/remote facts were checked.
    pub checked: usize,
    /// Items whose missing originals/sidecars were re-queued for download.
    pub repaired: usize,
    /// Items the remote no longer holds (reported, not deleted locally).
    pub missing: usize,
    /// Items whose remote bytes failed a read-back hash (`corrupt_remote`).
    pub corrupt: usize,
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
            // Rebuild the in-memory stub mirror from the durable redb records
            // this `Configured` just reopened (§3.5): the mirror is written
            // only in-process by create_stub/ensure_local/run_evictor, so
            // without this seeding every stub persisted by a previous process
            // would read as a non-stub after restart and every guard site
            // would touch the 0-byte placeholder as content.
            let stubs = configured.stub_paths()?;
            {
                let mut set = self
                    .stub_set
                    .lock()
                    .map_err(|_| SyncError::msg("sync manager stub_set lock poisoned"))?;
                // This `configure` binds a fresh engine on `state_dir`, so the
                // mirror must reflect exactly that redb — start clean.
                set.clear();
                set.extend(stubs);
            }
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

    /// §2.9 save-site intake: the local `albums.json` / `presets.json` at
    /// `local_path` was just written. Relativize its in-root image / LUT
    /// paths to `rr://`, store the device-local path so the apply loop can
    /// write a converged document back here, and mark the meta kind dirty
    /// for the next cycle. Best-effort (never panics at the upstream save
    /// site). Only compiled under the `sync` feature — its sole caller, the
    /// feature-gated `hooks::imp` shim, carries the parity guarantee for
    /// `--no-default-features` (album/preset save stays plain local JSON).
    #[cfg(feature = "sync")]
    pub fn note_local_meta(&self, kind: crate::sync::MetaKind, local_path: &std::path::Path) {
        if let Ok(cfg) = self.configured() {
            cfg.note_local_meta(kind, local_path);
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

    /// A locally present smart preview for the stub at `path` (§4.4), or
    /// `None` when sync is off, `path` is not a stub, or no proxy DNG has been
    /// downloaded. Backs [`crate::sync::hooks::proxy_handle`]; the proxy-mode
    /// loader branch uses it to decode the preview while reporting the
    /// original (journal) dimensions. Consults the durable preview store
    /// (`previews/<content_id>.pxy.dng`) and the item's journaled `(w, h)`
    /// (§4.4).
    pub fn proxy_handle(&self, path: &Path) -> Option<super::hooks::ProxyHandle> {
        #[cfg(feature = "sync")]
        {
            self.configured()
                .ok()
                .and_then(|cfg| cfg.proxy_handle(path))
        }
        #[cfg(not(feature = "sync"))]
        {
            let _ = path;
            None
        }
    }

    /// §4.3 importing-client generation entry point: build and durably store
    /// the smart preview + thumbs for the local original at `path` (callable by
    /// the P4 headless-worker backfill as well). A no-op when sync is off; a
    /// best-effort no-op when `path` is not a decodable, fully-synced original.
    pub fn generate_proxy_for(&self, path: &Path) -> Result<(), SyncError> {
        #[cfg(feature = "sync")]
        {
            match self.configured() {
                Ok(cfg) => cfg.generate_and_store_proxy(path),
                Err(_) => Ok(()),
            }
        }
        #[cfg(not(feature = "sync"))]
        {
            let _ = path;
            Ok(())
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
    ///
    /// Contract: the caller MUST prove the original is absent / remote-only
    /// before invoking this. As a defensive backstop against a reconcile race
    /// or mis-decision, `create_stub` refuses (returns an error, writing
    /// nothing) when `image_path` already exists with real bytes and is not
    /// already a known stub, rather than truncating content it cannot recover.
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
            // The mirror is updated per item as the pass commits each stub
            // (see `Configured::evict_to_stub`), not only on a clean `Ok`, so a
            // mid-pass error cannot desync it from redb/disk.
            let mut on_evicted = |p: &Path| self.mark_stub(p, true);
            cfg.run_evictor(&mut on_evicted).await
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
            // The mirror is updated per item as the pass commits each stub
            // (see `Configured::evict_to_stub`), not only on a clean `Ok`, so a
            // mid-pass error cannot desync it from redb/disk.
            let mut on_evicted = |p: &Path| self.mark_stub(p, true);
            cfg.run_evictor_with_budget(max_resident_bytes, &mut on_evicted)
                .await
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
    /// exact thumbnail cache key `generate_single_thumbnail_and_cache` looks
    /// up (path + mtime + current adjustments) — so the existing
    /// asset-protocol scope (`$APPCACHE/thumbnails/*`) and `tauri.conf.json`
    /// stay untouched. Returns the cache path that was linked. It does **not**
    /// emit `thumbnail-generated`: the §3.5 apply loop that fires that event
    /// after seeding is wired in the next unit (this fn takes no `AppHandle`).
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

    // ---- §3.8 control surface (U8 command layer) --------------------------
    //
    // These back the Tauri command handlers in `sync::commands`. They are
    // genuine gaps over the existing engine primitives (which already ship in
    // rrcloud-core: `device_id`, `active_devices`, `recently_deleted`,
    // `restore_item`, the conflict relkey helpers, `retire_device`,
    // `reconcile_wholeness`). The bodies land in U8 green; U8 red leaves them
    // `todo!()` under the feature and inert (never-panicking) when sync is off,
    // so both cargo configs compile and the command tests fail on the gap.

    /// This device's registry id (§2.10), or `None` when sync is off or
    /// unconfigured. Backs `sync_status().device_id`.
    pub fn device_id(&self) -> Option<String> {
        #[cfg(feature = "sync")]
        {
            self.configured().ok().map(|cfg| cfg.device_id())
        }
        #[cfg(not(feature = "sync"))]
        {
            None
        }
    }

    /// The shared device registry (§2.10) for the settings device panel.
    /// Empty when sync is off or unconfigured. Backs
    /// `sync_status().peer_devices`.
    pub fn peer_devices(&self) -> Vec<PeerDevice> {
        #[cfg(feature = "sync")]
        {
            self.configured()
                .ok()
                .map(|cfg| cfg.peer_devices())
                .unwrap_or_default()
        }
        #[cfg(not(feature = "sync"))]
        {
            Vec::new()
        }
    }

    /// Soft-deleted items (§2.7) for the "Recently Deleted" view (§3.8).
    pub fn recently_deleted(&self) -> Result<Vec<RecentlyDeleted>, SyncError> {
        #[cfg(feature = "sync")]
        {
            self.configured()?.recently_deleted()
        }
        #[cfg(not(feature = "sync"))]
        {
            Err(SyncError::FeatureDisabled)
        }
    }

    /// Restores a soft-deleted item (§2.7), re-queuing its download. Returns
    /// the paths restored (an item plus any associated sidecar).
    pub fn restore(&self, image_path: &Path) -> Result<Vec<PathBuf>, SyncError> {
        #[cfg(feature = "sync")]
        {
            self.configured()?.restore(image_path)
        }
        #[cfg(not(feature = "sync"))]
        {
            let _ = image_path;
            Err(SyncError::FeatureDisabled)
        }
    }

    /// Resolves a §2.6 conflict on `image_path` from the UI (§3.8): keep the
    /// version-vector winner, or preserve the local loser as a copy.
    pub fn resolve_conflict(&self, image_path: &Path, keep: ConflictKeep) -> Result<(), SyncError> {
        #[cfg(feature = "sync")]
        {
            self.configured()?.resolve_conflict(image_path, keep)
        }
        #[cfg(not(feature = "sync"))]
        {
            let _ = (image_path, keep);
            Err(SyncError::FeatureDisabled)
        }
    }

    /// "Free up space" for specific paths (§3.5): evict the named hydrated
    /// originals back to 0-byte stubs (honoring the same verified-remote gate
    /// as [`Self::run_evictor`], never evicting unverified bytes), regardless
    /// of the LRU budget. Returns the number actually demoted.
    pub fn evict_paths(&self, image_paths: &[PathBuf]) -> Result<usize, SyncError> {
        #[cfg(feature = "sync")]
        {
            let cfg = self.configured()?;
            // Mark the in-memory stub mirror as each stub commits (before the
            // fallible truncate inside `evict_to_stub`), mirroring
            // `run_evictor`'s per-item update so a mid-pass failure can never
            // leave redb/disk ahead of the mirror.
            let mut on_evicted = |p: &Path| self.mark_stub(p, true);
            cfg.evict_paths(image_paths, &mut on_evicted)
        }
        #[cfg(not(feature = "sync"))]
        {
            let _ = image_paths;
            Err(SyncError::FeatureDisabled)
        }
    }

    /// Retires a device from the shared registry (§2.10) from the settings
    /// device panel.
    pub async fn retire_device(&self, device_id: &str) -> Result<(), SyncError> {
        #[cfg(feature = "sync")]
        {
            let cfg = self.configured()?;
            cfg.retire_device(device_id).await
        }
        #[cfg(not(feature = "sync"))]
        {
            let _ = device_id;
            Err(SyncError::FeatureDisabled)
        }
    }

    /// Kicks the §3.5 "Verify library" reconcile: re-advertises any
    /// wholeness-violating tombstoned items and `HEAD`s every backed original
    /// against the remote, so the report's `missing` / `corrupt` reflect real
    /// local-vs-remote facts (not a hardcoded clean bill). Backs the settings
    /// "Verify library" action.
    pub async fn verify_library(&self) -> Result<VerifyReport, SyncError> {
        #[cfg(feature = "sync")]
        {
            self.configured()?.verify_library().await
        }
        #[cfg(not(feature = "sync"))]
        {
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

    use rrcloud_core::clock::{DeviceId, VersionVector};
    use rrcloud_core::engine::{
        ChangeOutcome, EngineConsumer, LocalScan, admit_pending, item_local_path,
        notify_local_change, recently_deleted as engine_recently_deleted, reconcile_wholeness,
        restore_item,
    };
    use rrcloud_core::journal::{JournalEntry, Kind, Op};
    use rrcloud_core::keys::{RelKey, relkey};
    use rrcloud_core::publisher::{enqueue_entry, publish_pending};
    use rrcloud_core::reader::poll;
    use rrcloud_core::s3::{S3Client, S3Config};
    use rrcloud_core::semhash::{Blake3Hex, ContentId};
    use rrcloud_core::state::{ItemRecord, ItemState, StateError, SyncDb};
    use rrcloud_core::transfer::{
        BackendProfile, CancelFlag, ExpectedDownload, TransferConfig, TransferError,
        bucket_key_for, download_item, local_target_path, probe_backend, pump_downloads,
        pump_uploads, stored_backend_profile,
    };

    use super::{
        ConflictKeep, EvictionReport, PeerDevice, RecentlyDeleted, SyncError, SyncState,
        SyncStatus, ThumbVariant, VerifyReport,
    };
    use crate::app_settings::SyncSettings;
    use crate::sync::credentials::Credentials;
    use crate::sync::{MetaKind, WriteOrigin};

    /// Transfer concurrency per lane (§2.4 / §3.3). Small and fixed: the
    /// desktop app is not the bulk worker.
    const TRANSFER_CONCURRENCY: usize = 2;

    /// Smart previews generated per `run_cycle` (§4.3). Small: generation is a
    /// full raw decode + demosaic, scheduled on the thumbnail worker priority
    /// tier in the architecture; the desktop cycle only nibbles the backlog.
    const PROXY_BACKFILL_PER_CYCLE: usize = 2;

    /// Write `bytes` to `path` via a sibling temp file + rename, so a crash
    /// mid-write never leaves a torn smart preview in the durable store.
    fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), SyncError> {
        let tmp = path.with_extension(format!("tmp-{}", std::process::id()));
        std::fs::write(&tmp, bytes).map_err(se)?;
        std::fs::rename(&tmp, path).map_err(se)?;
        Ok(())
    }

    /// How long a hydration that loses the single-driver race will wait for
    /// the in-flight transfer owning the item to finish before giving up and
    /// driving the download itself (§3.5). Generous: a guard-site hydration of
    /// a multi-tens-of-MB original over flaky mobile can legitimately take a
    /// while, and the winning downloader has no shorter bound either.
    const HYDRATE_INFLIGHT_WAIT: std::time::Duration = std::time::Duration::from_secs(600);

    /// The result of one `download_item` attempt inside [`Configured::hydrate`].
    enum HydrateOutcome {
        /// This call installed the verified bytes; its path.
        Installed(PathBuf),
        /// Another driver owns the live transfer (`download_item` refused with
        /// `StaleState{found: Downloading}`); the caller must wait, not fail.
        InFlight,
    }

    /// The result of [`Configured::wait_for_hydrated`].
    enum WaitOutcome {
        /// The in-flight transfer finished; the bytes are installed.
        Hydrated,
        /// The in-flight transfer condemned the remote as corrupt.
        Corrupt,
        /// The owner released the item to a retryable state (or the wait
        /// deadline passed); the caller should drive the download itself.
        Released,
    }

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

    /// Wall-clock unix seconds (attest `ts`, §2.2).
    fn now_unix() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0)
    }

    /// A strictly-monotonic access stamp for the LRU order
    /// (`last_access_unix`, §3.5). Seeded from wall-clock **nanoseconds** so
    /// the ordering survives a process restart (newer bytes keep a larger
    /// stamp), but forced strictly increasing within the process via a
    /// process-global floor so two accesses in the same wall-clock
    /// nanosecond — two guard-site hydrations microseconds apart — still
    /// order deterministically (the eviction tests pin which of two
    /// back-to-back hydrations is the LRU victim). The field is documented
    /// as unix seconds but is only ever compared, never read as a clock, so
    /// a finer unit is safe.
    fn access_stamp() -> u64 {
        use std::sync::atomic::AtomicU64;
        static LAST: AtomicU64 = AtomicU64::new(0);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        loop {
            let prev = LAST.load(Ordering::Relaxed);
            let next = now.max(prev.saturating_add(1));
            if LAST
                .compare_exchange(prev, next, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
            {
                return next;
            }
        }
    }

    /// Drives one `async` body to completion from a **synchronous** caller
    /// that may itself be inside a tokio runtime (the §3.5 `ensure_local`
    /// guard sites run on the app's runtime). A nested `block_on` on the
    /// calling thread would panic with "Cannot start a runtime from within a
    /// runtime"; running on a dedicated thread that owns a fresh
    /// current-thread runtime removes the ambient runtime entirely, exactly
    /// as the §3.3 exit-flush wrapper does.
    fn run_blocking<T, F>(fut: F) -> Result<T, SyncError>
    where
        T: Send + 'static,
        F: std::future::Future<Output = Result<T, SyncError>> + Send + 'static,
    {
        let handle = std::thread::spawn(move || -> Result<T, SyncError> {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| se(format!("blocking runtime: {e}")))?;
            runtime.block_on(fut)
        });
        match handle.join() {
            Ok(result) => result,
            Err(_) => Err(se("blocking worker thread panicked")),
        }
    }

    /// Resolves the §2.4 backend profile without the `self.backend` cache
    /// (the cached path is [`Configured::ensure_backend`]): the persisted
    /// value if present, else a one-shot digest probe that persists itself.
    /// Used from the dedicated-thread hydrate runtime, which holds only
    /// owned clones.
    async fn resolve_backend(
        db: &SyncDb,
        s3: &S3Client,
        bucket: &str,
    ) -> Result<BackendProfile, SyncError> {
        match stored_backend_profile(db).map_err(se)? {
            Some(b) => Ok(b),
            None => probe_backend(db, s3, bucket).await.map_err(se),
        }
    }

    /// The outcome of a §3.5 eviction read-back re-hash.
    enum Readback {
        /// Remote bytes blake3-match the journal head — safe to evict.
        Confirmed,
        /// The remote object could not be read (deleted / unreachable) — the
        /// "never evict unverified bytes" invariant keeps the local copy.
        Unverifiable,
        /// Remote bytes exist but hash wrong — route to `corrupt_remote`.
        Mismatch,
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
        /// §2.9 meta kinds with a local edit not yet uploaded this cycle.
        /// The save-site intake ([`Configured::note_local_meta`]) inserts;
        /// the cycle's meta upload drains. Empty ⇒ no meta upload work.
        meta_dirty: std::sync::Mutex<std::collections::HashSet<MetaKind>>,
        /// §2.9 device-local path of each meta document (where the save site
        /// wrote it, and where the apply loop writes a converged document
        /// back). Learned from [`Configured::note_local_meta`]. Written and
        /// read only once the P6-green intake/apply bodies land; allow dead
        /// until then (mirrors the `settings` field's pattern).
        #[allow(dead_code)]
        meta_local_paths: std::sync::Mutex<std::collections::HashMap<MetaKind, PathBuf>>,
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
                meta_dirty: std::sync::Mutex::new(std::collections::HashSet::new()),
                meta_local_paths: std::sync::Mutex::new(std::collections::HashMap::new()),
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

        /// §2.9 save-site intake for a just-written meta document. Reads the
        /// local bytes, records the device-local path, relativizes in-root
        /// image / LUT paths to `rr://` (dropping out-of-root entries from
        /// the copy the engine will advertise), and records the kind as a
        /// dirty whole-document version for the next cycle — reusing the
        /// §2.6 version mint exactly as a sidecar edit does. Best-effort:
        /// an unreadable or unmappable document is logged, never propagated.
        pub fn note_local_meta(&self, kind: MetaKind, local_path: &Path) {
            let _ = (kind, local_path);
            todo!("P6 green: read + relativize the meta doc and mark the kind dirty")
        }

        /// The device-local path of meta document `kind`:
        /// `<app_data_dir>/<stem>/<stem>.json`, where `app_data_dir` is the
        /// parent of the redb state dir (§3.3) — exactly where
        /// `file_management::get_albums_path` / `get_presets_path` write. The
        /// §2.9 apply loop writes a converged document back here even on a
        /// device that never saved locally (it has no `note_local_meta`
        /// record), so the location must be derivable, not only remembered.
        #[allow(dead_code)]
        fn meta_local_path(&self, kind: MetaKind) -> PathBuf {
            let (dir, file) = match kind {
                MetaKind::Albums => ("albums", "albums.json"),
                MetaKind::Presets => ("presets", "presets.json"),
            };
            let app_data_dir = self
                .db
                .path()
                .parent()
                .map(Path::to_path_buf)
                .unwrap_or_else(|| self.sync_root.clone());
            app_data_dir.join(dir).join(file)
        }

        /// Whether any meta document needs uploading this cycle (§2.9). Cheap
        /// local check so [`Configured::run_cycle`] skips the meta lane
        /// entirely when there is nothing to do (the common case).
        fn has_meta_upload_work(&self) -> bool {
            self.meta_dirty
                .lock()
                .map(|s| !s.is_empty())
                .unwrap_or(false)
        }

        /// The §2.9 meta lane of one cycle: upload every dirty meta document
        /// (relativized, with a bumped version vector + journal entry) and
        /// apply any converged remote meta head — download it, `localize`
        /// it to this device's sync root, atomically replace the local file,
        /// and surface a conflict loser via a `sync-conflict` event. The
        /// whole-document resolution reuses `meta::decide_meta` (which in
        /// turn reuses the §2.6 `compare` / `pick_winner`), never a bespoke
        /// rule.
        ///
        /// Skips cleanly when there is no meta work pending, so a cycle on a
        /// device that never touched albums/presets is a no-op.
        async fn sync_meta_documents(&self, cfg: &TransferConfig) -> Result<(), SyncError> {
            // Nothing dirty locally and nothing converged remotely to write
            // back ⇒ the meta lane is idle this cycle.
            if !self.has_meta_upload_work() {
                return Ok(());
            }
            let _ = cfg;
            todo!("P6 green: upload dirty meta docs and apply converged remote meta heads")
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

        /// The durable smart-preview store directory
        /// (`app_data_dir/rrcloud/previews`, alongside the redb and the
        /// `thumbs` store — §3.5/§4.3). Proxies are content-keyed:
        /// `previews/<content_id>.pxy.dng`.
        fn preview_store_dir(&self) -> PathBuf {
            self.db
                .path()
                .parent()
                .unwrap_or(self.sync_root.as_path())
                .join("previews")
        }

        /// §4.4 proxy edit-mode lookup: `Some(ProxyHandle)` when `path` is a
        /// `Stub` original whose content id is known, whose durable
        /// `previews/<content_id>.pxy.dng` is present locally, and which
        /// carries journaled `(w, h)`; otherwise `None`, so the loader falls
        /// through to the §3.5 hydrate path.
        pub fn proxy_handle(&self, path: &Path) -> Option<super::super::hooks::ProxyHandle> {
            let rk = relkey(path, &self.sync_root).ok()?;
            let record = self.db.get_item(&rk).ok().flatten()?;
            if record.deleted || record.state != ItemState::Stub {
                return None;
            }
            let content_id = record.content_id?;
            let (w, h) = (record.w?, record.h?);
            if w == 0 || h == 0 {
                return None;
            }
            let dng_path = self.preview_store_dir().join(format!(
                "{}{}",
                content_id.as_str(),
                rrcloud_core::proxy::PROXY_FILE_SUFFIX
            ));
            if !dng_path.is_file() {
                return None;
            }
            let orig_long = w.max(h);
            let proxy_long_edge = orig_long.min(rrcloud_core::proxy::PROXY_LONG_EDGE);
            Some(super::super::hooks::ProxyHandle {
                dng_path,
                orig_width: w,
                orig_height: h,
                proxy_long_edge,
            })
        }

        /// §4.3 importing-client generation: decode the local original at
        /// `image_path`, build the linear-DNG smart preview + JPEG thumbs
        /// (`rrcloud_core::proxy`), write them content-keyed into the durable
        /// preview store, and record the measured original `(w, h)` + content
        /// id on the item so [`Self::proxy_handle`] fires once the original is
        /// later evicted to a stub. Best-effort and side-effect-free on
        /// failure: a non-original, a stub (no local bytes), an undecodable
        /// file, or a missing content identity all return `Ok(())` without
        /// touching the store or the record. The P4 worker reuses the same
        /// `proxy` module for backfill + the S3/journal upload of previews.
        pub fn generate_and_store_proxy(&self, image_path: &Path) -> Result<(), SyncError> {
            let rk = relkey(image_path, &self.sync_root).map_err(se)?;
            let Some(record) = self.db.get_item(&rk).map_err(se)? else {
                return Ok(());
            };
            if record.deleted || record.kind != Kind::Original || record.state == ItemState::Stub {
                return Ok(());
            }
            let content_id = match record
                .content_id
                .clone()
                .or_else(|| record.blake3.as_ref().map(ContentId::from_blake3))
            {
                Some(cid) => cid,
                None => return Ok(()),
            };
            let dir = self.preview_store_dir();
            let dng_path = dir.join(format!(
                "{}{}",
                content_id.as_str(),
                rrcloud_core::proxy::PROXY_FILE_SUFFIX
            ));
            // Already generated — nothing to do.
            if dng_path.is_file() {
                return Ok(());
            }
            let bytes = match std::fs::read(image_path) {
                Ok(b) if !b.is_empty() => b,
                _ => return Ok(()),
            };
            let out = match rrcloud_core::proxy::generate_proxy(&bytes) {
                Ok(o) => o,
                Err(e) => {
                    log::warn!("proxy generation skipped for {}: {e}", image_path.display());
                    return Ok(());
                }
            };
            std::fs::create_dir_all(&dir).map_err(se)?;
            write_atomic(&dng_path, &out.dng)?;
            write_atomic(
                &dir.join(format!("{}_small.jpg", content_id.as_str())),
                &out.small_jpeg,
            )?;
            write_atomic(
                &dir.join(format!("{}_medium.jpg", content_id.as_str())),
                &out.medium_jpeg,
            )?;
            // Record the measured (never-EXIF) original dims + content id so a
            // later eviction-to-stub makes this proxy editable (§2.2/§4.4).
            let _ = self.db.update_item(&rk, record.state, |r| {
                r.w = Some(out.orig_width);
                r.h = Some(out.orig_height);
                if r.content_id.is_none() {
                    r.content_id = Some(content_id.clone());
                }
            });
            Ok(())
        }

        /// §4.3 opportunistic generation pass: build smart previews for up to
        /// `budget` `Synced` originals that do not yet have one. Runs after the
        /// upload lane drains (so only fully-backed-up originals are
        /// considered) and is wholly best-effort — undecodable or
        /// content-id-less items are silently skipped.
        pub fn generate_pending_proxies(&self, budget: usize) {
            if budget == 0 {
                return;
            }
            let items = match self.db.iter_items() {
                Ok(i) => i,
                Err(e) => {
                    log::warn!("proxy backfill: iter_items: {e}");
                    return;
                }
            };
            let mut made = 0usize;
            for (rk, record) in items {
                if made >= budget {
                    break;
                }
                if record.deleted
                    || record.kind != Kind::Original
                    || record.state != ItemState::Synced
                {
                    continue;
                }
                let path = local_target_path(&self.sync_root, &rk, record.kind);
                match self.generate_and_store_proxy(&path) {
                    Ok(()) => made += 1,
                    Err(e) => log::warn!("proxy backfill {}: {e}", path.display()),
                }
            }
        }

        /// §3.5 stub creation — writes the 0-byte placeholder at
        /// `image_path`, sets its mtime to `remote_mtime_unix` via
        /// `filetime`, and records an `ItemState::Stub` item carrying the
        /// remote `blake3_hex`/`size`/`verified_remote` facts.
        pub fn create_stub(
            &self,
            image_path: &Path,
            blake3_hex: &str,
            size: u64,
            remote_mtime_unix: i64,
        ) -> Result<(), SyncError> {
            let rk = relkey(image_path, &self.sync_root).map_err(se)?;
            let blake3 = Blake3Hex::parse(blake3_hex)
                .map_err(|e| se(format!("create_stub: bad remote blake3: {e}")))?;

            // Verify-before-truncate (§3.5): the apply/reconcile caller must
            // prove the original is absent / remote-only before driving this,
            // but a reconcile race or mis-decision could still target a relkey
            // whose local file actually holds real bytes — and the
            // `File::create` below would truncate it to a 0-byte stub,
            // destroying the content with no recovery. Refuse that: bail when
            // the target exists with `size > 0` and redb does not already
            // track it as a `Stub`. Re-adopting an existing stub (its file is
            // 0 bytes) still passes, so idempotent re-stubbing is unaffected.
            if let Ok(meta) = std::fs::metadata(image_path)
                && meta.len() > 0
            {
                let known_stub = self
                    .db
                    .get_item(&rk)
                    .map_err(se)?
                    .is_some_and(|r| matches!(r.state, ItemState::Stub));
                if !known_stub {
                    return Err(se(format!(
                        "create_stub: refusing to truncate existing {}-byte file at {} \
                         that is not a known stub (caller must prove the original is \
                         remote-only before stubbing, §3.5)",
                        meta.len(),
                        image_path.display()
                    )));
                }
            }

            // The 0-byte placeholder at the real path, with the remote
            // original's mtime replayed so `compute_thumbnail_cache_hash`
            // (blake3 of abs path + mtime + adjustments) is identical before
            // and after a later hydration (§3.5).
            if let Some(parent) = image_path.parent() {
                std::fs::create_dir_all(parent).map_err(se)?;
            }
            std::fs::File::create(image_path).map_err(se)?;
            filetime::set_file_mtime(
                image_path,
                filetime::FileTime::from_unix_time(remote_mtime_unix, 0),
            )
            .map_err(se)?;

            // The durable `ItemState::Stub` record carrying the verified
            // remote facts the hydration + eviction gate read. A remote-only
            // original the engine learned of from a journal/manifest head is
            // content-verified upstream (§2.4), so `verified_remote` holds;
            // `attested` stays false until this device itself hydrates and
            // verifies the bytes. `replay_put_item` is the §2.4 ingest path
            // for materializing an adopted record in an entry state.
            let record = ItemRecord {
                kind: Kind::Original,
                state: ItemState::Stub,
                size,
                mtime_unix_ns: remote_mtime_unix.saturating_mul(1_000_000_000),
                blake3: Some(blake3),
                sem_hash: None,
                vv: VersionVector::new(),
                content_id: None,
                w: None,
                h: None,
                pinned: false,
                last_access_unix: 0,
                verified_remote: true,
                attested: false,
                base_unknown: false,
                rating: None,
                color_label: None,
                device: None,
                head_ts: None,
                admitted_vv: None,
                admitted_ts: None,
                deleted: false,
            };
            self.db.replay_put_item(&rk, &record).map_err(se)?;
            Ok(())
        }

        /// §3.5 hydration — the resumable ranged download of a stub's real
        /// bytes, blake3 verify, atomic install over the stub, mtime
        /// restore, `Hydrated` transition, `attest` entry, LRU bump, and
        /// `sync-hydrate-progress` / `sync-hydrated` emits. Synchronous
        /// wrapper over the async transfer-engine download (runs on a
        /// bounded worker runtime, like the exit-flush path). Scaffold:
        /// unimplemented until the P2 green pass.
        pub fn hydrate(&self, image_path: &Path, reason: &str) -> Result<PathBuf, SyncError> {
            let _ = reason; // threaded for the command-layer event emit (next unit)
            let rk = relkey(image_path, &self.sync_root).map_err(se)?;
            let record = self
                .db
                .get_item(&rk)
                .map_err(se)?
                .ok_or_else(|| se("hydrate: no item record for stub"))?;

            // Idempotent: the bytes are already present.
            if matches!(record.state, ItemState::Hydrated | ItemState::Synced) {
                return Ok(image_path.to_path_buf());
            }

            let blake3 = record
                .blake3
                .clone()
                .ok_or_else(|| se("hydrate: stub has no remote blake3"))?;
            let expected = ExpectedDownload {
                blake3,
                size: record.size,
                mtime_unix: record.mtime_unix_ns.div_euclid(1_000_000_000),
            };

            // Single-flight per relkey (§3.5 / `download_item`'s single-driver
            // contract): the transfer engine refuses an item already
            // `Downloading` with a typed `StaleState` so a second writer never
            // races onto the live `.rr.part` partial. A guard site that loses
            // that race — a concurrent `ensure_local` for the same stub, or
            // the background download pump that already owns the transfer —
            // must treat it as "already in flight: WAIT", not surface it as a
            // hard failure to the guarded command (the module docs spell this
            // out). So on `StaleState{found: Downloading}` we wait for the
            // in-flight transfer to reach a terminal state and adopt its
            // installed bytes, rather than erroring.
            let final_path = local_target_path(&self.sync_root, &rk, record.kind);
            let deadline = std::time::Instant::now() + HYDRATE_INFLIGHT_WAIT;
            let (installed, we_installed) = loop {
                // Another driver may have finished since the initial read;
                // adopt a terminal result without re-downloading.
                let cur = self
                    .db
                    .get_item(&rk)
                    .map_err(se)?
                    .ok_or_else(|| se("hydrate: item record vanished"))?;
                if matches!(cur.state, ItemState::Hydrated | ItemState::Synced) {
                    break (final_path.clone(), false);
                }

                // The resumable ranged GET + blake3 verify + atomic install +
                // mtime restore + `Stub → Downloading → Hydrated` terminal
                // commit is the transfer engine's `download_item` (§3.5). Run
                // it on a dedicated-thread runtime so a guard site already
                // inside the app's runtime does not panic on a nested
                // `block_on`. An `InFlight` outcome means another driver owns
                // the live transfer.
                let db = self.db.clone();
                let s3 = self.s3.clone();
                let bucket = self.bucket.clone();
                let root = self.sync_root.clone();
                let rk_dl = rk.clone();
                let expected_dl = expected.clone();
                let outcome = run_blocking(async move {
                    let backend = resolve_backend(&db, s3.as_ref(), &bucket).await?;
                    let cfg = TransferConfig::new(bucket, root.clone(), backend);
                    match download_item(&db, s3.as_ref(), &cfg, &rk_dl, &root, &expected_dl).await {
                        Ok(o) => Ok::<HydrateOutcome, SyncError>(HydrateOutcome::Installed(o.path)),
                        Err(TransferError::State(StateError::StaleState {
                            found: Some(ItemState::Downloading),
                            ..
                        })) => Ok(HydrateOutcome::InFlight),
                        Err(e) => Err(se(e)),
                    }
                })?;

                match outcome {
                    HydrateOutcome::Installed(path) => break (path, true),
                    HydrateOutcome::InFlight => {
                        // Wait for the owning transfer to finish, then adopt.
                        match self.wait_for_hydrated(&rk, deadline)? {
                            WaitOutcome::Hydrated => break (final_path.clone(), false),
                            WaitOutcome::Corrupt => {
                                return Err(se(
                                    "hydrate: the in-flight transfer marked the remote corrupt",
                                ));
                            }
                            // The owner released the item to a retryable state
                            // (transport blip); we take over and drive it —
                            // unless we are out of time.
                            WaitOutcome::Released => {
                                if std::time::Instant::now() >= deadline {
                                    return Err(se(
                                        "hydrate: timed out waiting for an in-flight hydration",
                                    ));
                                }
                            }
                        }
                    }
                }
            };

            if we_installed {
                // Hydration verified the bytes against the journal head, so
                // this device can attest the current version and gate its own
                // future eviction without a read-back (§3.5). Bump the LRU
                // stamp.
                let attested_record = self
                    .db
                    .update_item(&rk, ItemState::Hydrated, |r| {
                        r.verified_remote = true;
                        r.attested = true;
                        r.last_access_unix = access_stamp();
                    })
                    .map_err(se)?;

                // Emit the `attest` journal entry (§2.2/§3.5). Best-effort: a
                // staging failure must not fail a successful hydration — the
                // durable `attested` flag above is the eviction gate; the
                // journal entry additionally advertises the attestation.
                if let Err(e) = self.stage_attest(&rk, &attested_record) {
                    log::warn!("hydrate: stage attest for {}: {e}", image_path.display());
                }
            } else {
                // Adopted another driver's freshly-installed bytes. That driver
                // runs its own attest; we only bump our LRU access stamp so a
                // later eviction treats this guard-site touch as a recent use.
                // Best-effort: a concurrent transition must not fail hydration.
                let cur = self.db.get_item(&rk).map_err(se)?;
                if let Some(cur) = cur {
                    let _ = self.db.update_item(&rk, cur.state, |r| {
                        r.last_access_unix = access_stamp();
                    });
                }
            }

            Ok(installed)
        }

        /// Blocks until the in-flight transfer for `rk` reaches a terminal
        /// state, the owner releases it back to a retryable state, or
        /// `deadline` passes (§3.5 single-flight wait). Polls the durable
        /// record; the transfer engine commits each state edge, so a reader
        /// observes the transition without holding any transfer lock.
        fn wait_for_hydrated(
            &self,
            rk: &RelKey,
            deadline: std::time::Instant,
        ) -> Result<WaitOutcome, SyncError> {
            loop {
                match self.db.get_item(rk).map_err(se)?.map(|r| r.state) {
                    Some(ItemState::Hydrated | ItemState::Synced) => {
                        return Ok(WaitOutcome::Hydrated);
                    }
                    Some(ItemState::CorruptRemote) => return Ok(WaitOutcome::Corrupt),
                    // Still owned by the live transfer: keep waiting (bounded).
                    Some(ItemState::Downloading) => {
                        if std::time::Instant::now() >= deadline {
                            return Ok(WaitOutcome::Released);
                        }
                        std::thread::sleep(std::time::Duration::from_millis(25));
                    }
                    // Any other (Stub / PendingDown / …): the owner released
                    // it without finishing — we take over and drive it.
                    _ => return Ok(WaitOutcome::Released),
                }
            }
        }

        /// Stages an `attest` journal entry for the current head of `rk`
        /// (§2.2 full v1 envelope; `vv` snapshots the verified version). The
        /// next publish cycle freezes and uploads it.
        fn stage_attest(&self, rk: &RelKey, record: &ItemRecord) -> Result<(), SyncError> {
            let key = bucket_key_for(rk, Kind::Original).map_err(se)?;
            let blake3 = record
                .blake3
                .clone()
                .ok_or_else(|| se("stage_attest: record has no blake3"))?;
            let entry = JournalEntry {
                v: rrcloud_core::journal::JOURNAL_VERSION,
                seq: 0, // allocated at freeze time (publisher contract)
                ts: now_unix(),
                device: self.db.device_id().clone(),
                op: Op::Attest,
                kind: Kind::Original,
                key,
                vv: record.vv.clone(),
                size: Some(record.size),
                blake3: Some(blake3),
                sem_hash: None,
                rating: None,
                color_label: None,
                content_id: record.content_id.clone(),
                w: None,
                h: None,
                mtime: Some(record.mtime_unix_ns.div_euclid(1_000_000_000)),
                from_key: None,
            };
            enqueue_entry(&self.db, &entry).map_err(se)?;
            Ok(())
        }

        /// §3.5 pin/unpin — sets the `pinned` flag on each named original,
        /// fanning a directory out to every item under it. Scaffold:
        /// unimplemented until the P2 green pass.
        pub fn pin_paths(&self, image_paths: &[PathBuf], pinned: bool) -> Result<usize, SyncError> {
            // Resolve each argument to the set of item relkeys it covers: a
            // file maps to its own relkey; a directory fans out to every
            // item whose local path is under it ("pin this folder offline",
            // §3.5). Dedupe so an overlapping file + folder argument counts
            // one item once.
            let mut targets: std::collections::HashSet<RelKey> = std::collections::HashSet::new();
            for arg in image_paths {
                if arg.is_dir() {
                    for (rk, record) in self.db.iter_items().map_err(se)? {
                        if record.deleted {
                            continue;
                        }
                        let local = local_target_path(&self.sync_root, &rk, record.kind);
                        if local.starts_with(arg) {
                            targets.insert(rk);
                        }
                    }
                } else if let Ok(rk) = relkey(arg, &self.sync_root)
                    && self.db.get_item(&rk).map_err(se)?.is_some()
                {
                    targets.insert(rk);
                }
            }

            let mut changed = 0usize;
            for rk in targets {
                let Some(record) = self.db.get_item(&rk).map_err(se)? else {
                    continue;
                };
                if record.pinned == pinned {
                    continue;
                }
                // State-preserving durable write (§3.5): CAS against the
                // observed state so a concurrent legal transition is not
                // stomped. A `StaleState` race just skips this item.
                if self
                    .db
                    .update_item(&rk, record.state, |r| r.pinned = pinned)
                    .is_ok()
                {
                    changed += 1;
                }
            }
            Ok(changed)
        }

        /// §3.5 LRU eviction pass using `settings.sync.cache_size_gb` as the
        /// resident-bytes budget. Scaffold: unimplemented until the P2 green
        /// pass.
        pub async fn run_evictor(
            &self,
            on_evicted: &mut dyn FnMut(&Path),
        ) -> Result<EvictionReport, SyncError> {
            let budget = (self.settings.cache_size_gb as u64) << 30;
            self.run_evictor_with_budget(budget, on_evicted).await
        }

        /// §3.5 LRU eviction pass with an explicit `max_resident_bytes`
        /// budget — the attestation/read-back-gated demotion of
        /// least-recently-accessed non-pinned verified originals back to
        /// stubs. Scaffold: unimplemented until the P2 green pass.
        pub async fn run_evictor_with_budget(
            &self,
            max_resident_bytes: u64,
            on_evicted: &mut dyn FnMut(&Path),
        ) -> Result<EvictionReport, SyncError> {
            // Resident originals: items holding local bytes (`Hydrated`, or a
            // `Synced` original this device uploaded) — never stubs. Sidecars
            // are never stubbed (§3.5), so they never enter the budget.
            let mut candidates: Vec<(RelKey, ItemRecord, PathBuf)> = self
                .db
                .iter_items()
                .map_err(se)?
                .into_iter()
                .filter(|(_, r)| {
                    !r.deleted
                        && r.kind == Kind::Original
                        && matches!(r.state, ItemState::Hydrated | ItemState::Synced)
                })
                .map(|(rk, r)| {
                    let path = local_target_path(&self.sync_root, &rk, r.kind);
                    (rk, r, path)
                })
                .collect();

            // Least-recently-accessed first (eviction order, §3.5).
            candidates.sort_by_key(|(_, r, _)| r.last_access_unix);

            let mut report = EvictionReport::default();
            let mut resident: u64 = candidates.iter().map(|(_, r, _)| r.size).sum();

            for (rk, record, path) in candidates {
                if resident <= max_resident_bytes {
                    break;
                }
                // Pinned originals are never evicted (§3.5).
                if record.pinned {
                    report.kept_pinned.push(path);
                    continue;
                }
                // Upload-side integrity must hold before anything else (§2.4).
                if !record.verified_remote {
                    report.kept_unverified.push(path);
                    continue;
                }
                // The content-verified gate: an `attest` entry covers the
                // version, OR a one-time read-back re-hash confirms the
                // remote bytes. A mismatch is `corrupt_remote`; an
                // unreadable remote keeps the local copy — the "never evict
                // unverified bytes" invariant (§3.5).
                if !record.attested {
                    match self.readback_verify(&rk, &record).await? {
                        Readback::Confirmed => {}
                        Readback::Unverifiable => {
                            report.kept_unverified.push(path);
                            continue;
                        }
                        Readback::Mismatch => {
                            let _ = self.db.transition(
                                &rk,
                                record.state,
                                ItemState::CorruptRemote,
                                |_| {},
                            );
                            report.corrupt.push(path);
                            continue;
                        }
                    }
                }
                // Demote to a 0-byte stub: terminal transition, truncate the
                // file, restore the remote mtime so the thumbnail cache key
                // survives (§3.5).
                self.evict_to_stub(&rk, &record, &path, on_evicted)?;
                resident = resident.saturating_sub(record.size);
                report.evicted.push(path);
            }

            report.resident_bytes = resident;
            Ok(report)
        }

        /// Reads the remote original back in full and blake3-compares it
        /// against the journal head (§3.5 eviction read-back gate).
        async fn readback_verify(
            &self,
            rk: &RelKey,
            record: &ItemRecord,
        ) -> Result<Readback, SyncError> {
            let key = bucket_key_for(rk, Kind::Original).map_err(se)?;
            let output = match self.s3.get_object(&self.bucket, &key, None).await {
                Ok(o) => o,
                // A deleted / unreachable remote cannot be confirmed: keep
                // the only good copy (the local bytes).
                Err(_) => return Ok(Readback::Unverifiable),
            };
            let bytes = match output.body.collect().await {
                Ok(b) => b,
                Err(_) => return Ok(Readback::Unverifiable),
            };
            let actual = Blake3Hex::from_bytes(&bytes);
            match &record.blake3 {
                Some(expected) if &actual == expected => Ok(Readback::Confirmed),
                Some(_) => Ok(Readback::Mismatch),
                None => Ok(Readback::Unverifiable),
            }
        }

        /// Demotes one verified resident original back to a 0-byte stub:
        /// `Hydrated`/`Synced → Stub`, truncate the file, restore the remote
        /// mtime (§3.5 — keeps the thumbnail cache key stable).
        fn evict_to_stub(
            &self,
            rk: &RelKey,
            record: &ItemRecord,
            path: &Path,
            on_evicted: &mut dyn FnMut(&Path),
        ) -> Result<(), SyncError> {
            self.db
                .transition(rk, record.state, ItemState::Stub, |_| {})
                .map_err(se)?;
            // The redb transition is the authoritative commit: the item is now
            // durably a `Stub`. Notify the caller's in-memory stub mirror here,
            // BEFORE the fallible truncate/mtime steps below, so a later failure
            // in this pass (e.g. a vanished local file) cannot leave redb/disk
            // ahead of the mirror. Otherwise already-demoted 0-byte stubs would
            // answer `is_stub()`/`is_cloud_placeholder()` false until restart and
            // every §3.5 guard site would read the placeholder as content.
            on_evicted(path);
            // Truncate in place (never a remove+recreate: the directory entry
            // and inode stay put under any concurrent reader).
            std::fs::OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(path)
                .map_err(se)?;
            filetime::set_file_mtime(
                path,
                filetime::FileTime::from_unix_time(
                    record.mtime_unix_ns.div_euclid(1_000_000_000),
                    0,
                ),
            )
            .map_err(se)?;
            Ok(())
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
            let suffix = variant.suffix();

            // 1. Durable app-data store: `app_data_dir/rrcloud/thumbs/` is
            //    the redb's own directory (`state_dir`, §3.6). The canonical
            //    name keys on the content identity when known — the `<content_id>`
            //    scheme of §1.2, used verbatim (it is already hex `blake3(file
            //    bytes)`, so it must NOT be re-hashed) so a move/rename reuses
            //    the content-keyed thumb — else on the stub's relkey (no
            //    content_id yet). This store is *not* the OS-clearable cache
            //    (§3.5 D1 fix).
            let store_dir = self
                .db
                .path()
                .parent()
                .unwrap_or(self.sync_root.as_path())
                .join("thumbs");
            std::fs::create_dir_all(&store_dir).map_err(se)?;
            let content_key = match relkey(image_path, &self.sync_root) {
                Ok(rk) => match self
                    .db
                    .get_item(&rk)
                    .map_err(se)?
                    .and_then(|r| r.content_id)
                {
                    // §1.2: the thumb store key IS the content_id (already a
                    // hex blake3 of the original bytes), not a re-hash of it.
                    Some(cid) => cid.as_str().to_string(),
                    None => Blake3Hex::from_bytes(rk.as_str().as_bytes())
                        .as_str()
                        .to_string(),
                },
                Err(_) => Blake3Hex::from_bytes(image_path.to_string_lossy().as_bytes())
                    .as_str()
                    .to_string(),
            };
            let durable = store_dir.join(format!("{content_key}_{suffix}.jpg"));
            std::fs::write(&durable, jpeg_bytes).map_err(se)?;

            // 2. Surface it to the webview by hard-linking (copy fallback)
            //    into `$APPCACHE/thumbnails/<hash>_<suffix>.jpg`, where
            //    `<hash>` is the exact cache key
            //    `generate_single_thumbnail_and_cache` looks up: the stub's
            //    path + mtime + the **current adjustments** (§3.5). Sidecars
            //    are never stubbed, so an edited cloud image has its real
            //    sidecar on disk; keying with empty adjustments here would
            //    land the thumb under a key the grid never looks up (orphaned
            //    thumb, permanent cache miss). The asset-protocol scope and
            //    `tauri.conf.json` stay untouched.
            let path_str = image_path.to_string_lossy();
            let adjustments = crate::file_management::thumbnail_adjustments_key_bytes(&path_str);
            let hash =
                crate::file_management::compute_thumbnail_cache_hash(&path_str, &adjustments)
                    .ok_or_else(|| se("seed_thumbnail: cannot compute thumbnail cache key"))?;
            std::fs::create_dir_all(cache_thumbnails_dir).map_err(se)?;
            let linked = cache_thumbnails_dir.join(format!("{hash}_{suffix}.jpg"));
            // Replace any stale link/file at the key first.
            let _ = std::fs::remove_file(&linked);
            if std::fs::hard_link(&durable, &linked).is_err() {
                // Cross-filesystem (desktop) — copy fallback (§3.5).
                std::fs::copy(&durable, &linked).map_err(se)?;
            }
            Ok(linked)
        }

        /// The absolute local paths of every durable `ItemState::Stub`
        /// original record (§3.5). `configure`/`open` call this to rebuild the
        /// manager's in-memory `stub_set` mirror on process start, so a stub
        /// persisted by a previous run is recognized again. Without it,
        /// `is_stub`/`is_cloud_placeholder` answer `false` for every persisted
        /// stub after a restart and every §3.5 guard site reads (or copies)
        /// the 0-byte stub as content — the "mirror of redb" the doc promises
        /// is only ever written in-process, never read back on load.
        pub fn stub_paths(&self) -> Result<Vec<PathBuf>, SyncError> {
            let mut out = Vec::new();
            for (rk, record) in self.db.iter_items().map_err(se)? {
                if record.deleted || record.state != ItemState::Stub {
                    continue;
                }
                out.push(local_target_path(&self.sync_root, &rk, record.kind));
            }
            Ok(out)
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

        // ---- §3.8 control-surface helpers (U8 command layer) ------------

        /// This device's registry id (§2.10), as a string.
        pub fn device_id(&self) -> String {
            self.db.device_id().to_string()
        }

        /// A cheap, local view of the device fleet for the settings panel:
        /// this device plus every peer this device has applied a journal
        /// from (the redb cursor table, §2.3), ascending by id. No network —
        /// the status path never blocks on a registry round-trip, so the
        /// remote `last_seen` / `retired` facts are left at their defaults
        /// (a richer view is a future async panel refresh, not the snapshot).
        pub fn peer_devices(&self) -> Vec<PeerDevice> {
            let own = self.db.device_id().to_string();
            let mut ids: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
            ids.insert(own.clone());
            if let Ok(cursors) = self.db.iter_cursors() {
                for (device, _) in cursors {
                    ids.insert(device.to_string());
                }
            }
            ids.into_iter()
                .map(|id| PeerDevice {
                    is_self: id == own,
                    device_id: id,
                    last_seen_unix: 0,
                    retired: false,
                })
                .collect()
        }

        /// §2.7 "Recently Deleted": the tombstoned item records, projected
        /// to the webview DTO (ascending by relkey, as the engine lists them).
        pub fn recently_deleted(&self) -> Result<Vec<RecentlyDeleted>, SyncError> {
            let mut out = Vec::new();
            for (rk, record) in engine_recently_deleted(&self.db).map_err(se)? {
                out.push(RecentlyDeleted {
                    path: item_local_path(&self.sync_root, &rk)
                        .to_string_lossy()
                        .into_owned(),
                    relkey: rk.as_str().to_string(),
                    deleted_unix: record.head_ts.unwrap_or(0),
                });
            }
            Ok(out)
        }

        /// §2.7 restore: un-hides every tombstoned item of `image` and
        /// re-queues it, returning the local paths restored.
        pub fn restore(&self, image_path: &Path) -> Result<Vec<PathBuf>, SyncError> {
            let image = relkey(image_path, &self.sync_root).map_err(se)?;
            let restored = restore_item(&self.db, &image).map_err(se)?;
            Ok(restored
                .iter()
                .map(|rk| item_local_path(&self.sync_root, rk))
                .collect())
        }

        /// §2.6 conflict resolution from the UI. P1 `converge` resolves
        /// concurrent versions automatically — the version-vector winner
        /// stays live and the loser materializes as a `-conflict` copy — so
        /// an item is normally never parked in `Conflict`. This drives the
        /// local pick only when a record *is* in `Conflict` (keep the winner
        /// → fetch it; keep the local loser → re-admit it as a fresh
        /// version); a path with no parked conflict is a clean no-op.
        pub fn resolve_conflict(
            &self,
            image_path: &Path,
            keep: ConflictKeep,
        ) -> Result<(), SyncError> {
            let rk = relkey(image_path, &self.sync_root).map_err(se)?;
            let Some(record) = self.db.get_item(&rk).map_err(se)? else {
                return Ok(());
            };
            if record.state != ItemState::Conflict {
                return Ok(());
            }
            let target = match keep {
                ConflictKeep::Winner => ItemState::PendingDown,
                ConflictKeep::Copy => ItemState::Dirty,
            };
            self.db
                .transition(&rk, ItemState::Conflict, target, |_| {})
                .map_err(se)?;
            Ok(())
        }

        /// "Free up space" for the named paths (§3.5): evict each resolved,
        /// verified, non-pinned resident original back to a 0-byte stub,
        /// ignoring the LRU budget. Honors the same "never evict unverified
        /// bytes" gate as [`Self::run_evictor_with_budget`] (`verified_remote`
        /// plus an `attest` or a one-time read-back re-hash; a mismatch routes
        /// to `CorruptRemote` and is never evicted). `on_evicted` fires as
        /// each stub commits so the caller's mirror never trails redb/disk.
        /// Returns the number demoted.
        pub fn evict_paths(
            &self,
            image_paths: &[PathBuf],
            on_evicted: &mut dyn FnMut(&Path),
        ) -> Result<usize, SyncError> {
            // Resolve args to target relkeys (file → own; directory → fan-out),
            // exactly as `pin_paths` does.
            let mut targets: std::collections::HashSet<RelKey> = std::collections::HashSet::new();
            for arg in image_paths {
                if arg.is_dir() {
                    for (rk, record) in self.db.iter_items().map_err(se)? {
                        if record.deleted {
                            continue;
                        }
                        let local = local_target_path(&self.sync_root, &rk, record.kind);
                        if local.starts_with(arg) {
                            targets.insert(rk);
                        }
                    }
                } else if let Ok(rk) = relkey(arg, &self.sync_root)
                    && self.db.get_item(&rk).map_err(se)?.is_some()
                {
                    targets.insert(rk);
                }
            }

            let mut demoted = 0usize;
            for rk in targets {
                let Some(record) = self.db.get_item(&rk).map_err(se)? else {
                    continue;
                };
                // Only resident originals can be freed (sidecars are never
                // stubbed, §3.5); a stub / in-flight item is skipped.
                if record.deleted
                    || record.kind != Kind::Original
                    || !matches!(record.state, ItemState::Hydrated | ItemState::Synced)
                {
                    continue;
                }
                // Pinned originals are never evicted (§3.5).
                if record.pinned {
                    continue;
                }
                // Upload-side integrity must hold first (§2.4).
                if !record.verified_remote {
                    continue;
                }
                // Content-verified gate: an `attest` covers the version, else
                // a one-time ranged read-back re-hash confirms the remote
                // bytes. A mismatch is `corrupt_remote`; an unreadable remote
                // keeps the local copy (the "never evict unverified bytes"
                // invariant, §3.5).
                if !record.attested {
                    match self.readback_verify_blocking(&rk, &record)? {
                        Readback::Confirmed => {}
                        Readback::Unverifiable => continue,
                        Readback::Mismatch => {
                            let _ = self.db.transition(
                                &rk,
                                record.state,
                                ItemState::CorruptRemote,
                                |_| {},
                            );
                            continue;
                        }
                    }
                }
                let path = local_target_path(&self.sync_root, &rk, record.kind);
                self.evict_to_stub(&rk, &record, &path, on_evicted)?;
                demoted += 1;
            }
            Ok(demoted)
        }

        /// Synchronous wrapper over the async eviction read-back (§3.5): runs
        /// a full GET + blake3 re-hash on a dedicated-thread runtime, so
        /// the synchronous "free up space" path need not itself be `async`
        /// (mirroring how [`Configured::hydrate`] drives its download).
        fn readback_verify_blocking(
            &self,
            rk: &RelKey,
            record: &ItemRecord,
        ) -> Result<Readback, SyncError> {
            let s3 = self.s3.clone();
            let bucket = self.bucket.clone();
            let rk = rk.clone();
            let expected = record.blake3.clone();
            run_blocking(async move {
                let key = bucket_key_for(&rk, Kind::Original).map_err(se)?;
                let output = match s3.get_object(&bucket, &key, None).await {
                    Ok(o) => o,
                    Err(_) => return Ok(Readback::Unverifiable),
                };
                let bytes = match output.body.collect().await {
                    Ok(b) => b,
                    Err(_) => return Ok(Readback::Unverifiable),
                };
                let actual = Blake3Hex::from_bytes(&bytes);
                Ok(match expected {
                    Some(e) if actual == e => Readback::Confirmed,
                    Some(_) => Readback::Mismatch,
                    None => Readback::Unverifiable,
                })
            })
        }

        /// §2.10 retire a device from the shared registry — the settings
        /// device panel's action. The panel hands ids straight from the
        /// registry, which are canonical UUIDv4s (the typed
        /// [`compact::retire_device`](rrcloud_core::compact::retire_device)
        /// path). Retirement is a single idempotent PUT of the device's
        /// `.retired` marker, so an id that is not a canonical device id (a
        /// stale or hand-entered value) still marks retirement via a direct
        /// PUT at its marker key rather than hard-failing the action.
        pub async fn retire_device(&self, device_id: &str) -> Result<(), SyncError> {
            match DeviceId::new(device_id.to_string()) {
                Ok(did) => {
                    rrcloud_core::compact::retire_device(self.s3.as_ref(), &self.bucket, &did)
                        .await
                        .map_err(se)
                }
                Err(_) => {
                    let key = format!(
                        "{}devices/{device_id}.retired",
                        rrcloud_core::keys::CONTROL_PREFIX
                    );
                    // Empty marker body. `Default::default()` yields an empty
                    // `bytes::Bytes` (a dev-only dependency here, so it is not
                    // named) inferred from `put_object`'s signature.
                    self.s3
                        .put_object(
                            &self.bucket,
                            &key,
                            Default::default(),
                            &rrcloud_core::s3::PutObjectOptions::default(),
                        )
                        .await
                        .map(|_| ())
                        .map_err(se)
                }
            }
        }

        /// §3.5 "Verify library" reconcile: re-advertises any
        /// wholeness-violating tombstoned items (the `repaired` tally) **and**
        /// re-checks local-vs-remote facts for every live original the engine
        /// believes is backed, so the report is not a fabricated clean bill.
        ///
        /// For each such original it `HEAD`s the remote blob:
        /// - a `NoSuchKey` (the remote no longer holds it) counts as `missing`;
        /// - a present blob whose byte length disagrees with the verified size
        ///   counts as `corrupt` (a read-back discrepancy the §2.4 verify gate
        ///   would flag);
        /// - an original already parked in the `corrupt_remote` lane counts as
        ///   `corrupt` without a round-trip.
        ///
        /// The reconcile re-queues locally and re-advertised versions upload on
        /// the next cycle; the HEAD pass is read-only (it reports, it does not
        /// mutate state here).
        pub async fn verify_library(&self) -> Result<VerifyReport, SyncError> {
            // Local wholeness reconcile first (re-advertise half-deleted shapes).
            let repaired = reconcile_wholeness(&self.db, &mut ()).map_err(se)?.len();

            let mut checked = 0usize;
            let mut missing = 0usize;
            let mut corrupt = 0usize;
            for (rel, rec) in self.db.iter_items().map_err(se)? {
                if rec.kind != Kind::Original || rec.deleted {
                    continue;
                }
                checked += 1;
                // An original already flagged corrupt_remote (§2.4) is corrupt
                // without a round-trip.
                if rec.state == ItemState::CorruptRemote {
                    corrupt += 1;
                    continue;
                }
                // Only originals the engine treats as remotely backed are
                // checkable; a purely-local Dirty/Queued original has no remote
                // fact to verify yet.
                let backed = rec.verified_remote
                    || matches!(
                        rec.state,
                        ItemState::Synced
                            | ItemState::Hydrated
                            | ItemState::Stub
                            | ItemState::PendingDown
                    );
                if !backed {
                    continue;
                }
                let key = bucket_key_for(&rel, Kind::Original).map_err(se)?;
                match self.s3.head_object(&self.bucket, &key).await {
                    Ok(head) => {
                        // A present blob whose length no longer matches the
                        // verified size is a read-back discrepancy.
                        if rec.size > 0 && head.content_length != rec.size {
                            corrupt += 1;
                        }
                    }
                    Err(e) if e.is_no_such_key() => missing += 1,
                    Err(e) => return Err(se(e)),
                }
            }
            Ok(VerifyReport {
                checked,
                repaired,
                missing,
                corrupt,
            })
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

            // §4.3 importing-client generation: once the upload lane has
            // drained (so the considered originals are fully backed up), build
            // smart previews for a bounded number of `Synced` originals that do
            // not have one yet. Best-effort: never fails the cycle.
            self.generate_pending_proxies(PROXY_BACKFILL_PER_CYCLE);

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

            // §2.9 albums/presets meta lane: upload dirty meta documents and
            // apply any converged remote meta head. A no-op when no meta work
            // is pending, so cycles on devices that never touched
            // albums/presets are unaffected.
            self.sync_meta_documents(&cfg).await?;

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
