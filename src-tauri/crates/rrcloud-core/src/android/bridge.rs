//! The JNI bridge proper (ARCHITECTURE.md §5.1/§5.2): `#[no_mangle] extern
//! "system"` entry points `SyncCycleWorker.doWork()`, the user-initiated
//! foreground-sync service, and `DcimScanWorker` call from Kotlin. Compiled
//! **only** under `target_os = "android"` — it needs a live JVM to mean
//! anything, and the `jni` crate is a `target.'cfg(target_os =
//! "android")'` dependency of this crate (see `Cargo.toml`) for exactly
//! that reason, so `cargo test` on the host never touches this module.
//!
//! The Kotlin side of this contract is `RrcloudBridge.kt`
//! (`tauri-plugin-rrcloud/android/src/main/java/com/plugin/rrcloud/`):
//! that file's `external fun` declarations must keep these functions'
//! exact JNI names (`Java_<package_with_underscores>_RrcloudBridge_<fn>`,
//! package `com.plugin.rrcloud`) and parameter order — the JNI name
//! mangling has no compiler to catch a drift between the two sides, only a
//! runtime `UnsatisfiedLinkError`. Conversely, this module calls back into
//! several `@JvmStatic` Kotlin methods on the same `RrcloudBridge` object
//! (settings/credentials reads) — those names/signatures are just as
//! unchecked in the other direction.
//!
//! All of the actual decision-making these functions perform that is pure
//! enough to unit-test — the `budget_ms` deadline, the `jint` result code,
//! the DCIM dedupe decision — is delegated to [`super::bounded_cycle`] /
//! [`super::result_code`] / [`super::dcim_dedupe`], which are host-testable
//! on their own; this module's bodies are glue (JNI unwrapping,
//! `ndk_context`/`rustls_platform_verifier` init, opening redb + reading
//! settings/credentials via a JNI callback into Kotlin, running the bounded
//! cycle over the same engine primitives `sync::manager::Configured` uses
//! on desktop) that cannot be exercised without a JVM and is therefore
//! proven correct by the Android build gate + on-device verification, not
//! `cargo test`.

#[cfg(target_os = "android")]
mod imp {
    use std::path::PathBuf;
    use std::sync::Mutex;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use jni::objects::{JClass, JObject, JObjectArray, JString};
    use jni::sys::{jint, jlong};
    use jni::JNIEnv;

    use crate::engine::{admit_pending, notify_local_change, EngineConsumer, LocalScan};
    use crate::journal::Kind;
    use crate::keys::relkey;
    use crate::publisher::publish_pending;
    use crate::reader::poll_with_cancel;
    use crate::s3::{S3Client, S3Config};
    use crate::semhash::ContentId;
    use crate::state::{StateError, SyncDb};
    use crate::transfer::{
        probe_backend, pump_downloads, pump_uploads, stored_backend_profile,
        stub_pending_originals_bounded, CancelFlag, TransferConfig,
    };
    use crate::worker::mint_worker_device_id;

    use super::super::bounded_cycle::CycleBudget;
    use super::super::dcim_dedupe::{decide, DcimDecision, SeenRow};
    use super::super::result_code::BridgeResult;

    /// Guards one sync cycle at a time **within this process** (§5.1):
    /// WorkManager can in some configurations run a `Worker` in-process
    /// with the app (a default-executor `Worker`, as opposed to a remote
    /// process), so redb's own cross-*process* file lock
    /// ([`StateError::AlreadyLocked`]) is not enough on its own — two
    /// cycles racing inside the same process would both see the lock free.
    /// A contended lock here backs off immediately with
    /// [`BridgeResult::RetryLockHeld`] rather than blocking: WorkManager
    /// already owns the retry/backoff policy, this just refuses to run two
    /// cycles concurrently in-process.
    static CYCLE_LOCK: Mutex<()> = Mutex::new(());

    /// Minimal subset of `AppSettings.sync` this bridge needs to build an
    /// `S3Client` + `TransferConfig` (ARCHITECTURE.md §5.1). Deliberately
    /// not the full `app_settings::SyncSettings` type: this crate has no
    /// dependency on the host app crate (`rrcloud-core`'s whole point, see
    /// the crate doc), so the Kotlin side hands over just these fields as
    /// JSON (`RrcloudBridge.loadSyncSettingsJson`, which reads the SAME
    /// `settings.json` the app writes via
    /// `app_settings::get_settings_path`). The two shapes must be kept in
    /// sync by hand — same caveat as the JNI name mangling above: no
    /// compiler link between the two sides.
    #[derive(serde::Deserialize, Default)]
    struct AndroidSyncSettings {
        #[serde(default)]
        enabled: bool,
        #[serde(default)]
        endpoint: String,
        #[serde(default)]
        bucket: String,
        #[serde(default)]
        region: String,
    }

    impl AndroidSyncSettings {
        fn is_usable(&self) -> bool {
            self.enabled && !self.endpoint.is_empty() && !self.bucket.is_empty()
        }
    }

    /// The credential pair, decoded from the Keystore-backed Kotlin
    /// `CredentialStore`'s JSON (`RrcloudBridge.loadCredentialsJson`).
    #[derive(serde::Deserialize, Default)]
    struct AndroidCredentials {
        #[serde(default)]
        access_key: String,
        #[serde(default)]
        secret_key: String,
    }

    impl AndroidCredentials {
        fn is_complete(&self) -> bool {
            !self.access_key.is_empty() && !self.secret_key.is_empty()
        }
    }

    fn now_ms() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }

    /// Clears a pending Java exception after a failed JNI call (mirrors
    /// `android_integration::clear_pending_android_exception`; duplicated
    /// rather than shared because the two live in different crates with no
    /// dependency edge between them, by design — see the crate doc).
    fn clear_exception(env: &mut JNIEnv) {
        if env.exception_check().unwrap_or(false) {
            let _ = env.exception_describe();
            let _ = env.exception_clear();
        }
    }

    /// Calls a `@JvmStatic String?(Context)` method on
    /// `com.plugin.rrcloud.RrcloudBridge` — the shape every settings/
    /// credentials callback in this module uses. `None` on a Java `null`
    /// return *or* a JNI-level failure (logged either way the caller can
    /// tell apart only by checking logcat; both are treated as "nothing to
    /// read" by every call site, which is the safe default for a store
    /// that is simply unconfigured yet).
    fn call_static_string_method(env: &mut JNIEnv, ctx: &JObject, method: &str) -> Option<String> {
        let result = env
            .call_static_method(
                "com/plugin/rrcloud/RrcloudBridge",
                method,
                "(Landroid/content/Context;)Ljava/lang/String;",
                &[ctx.into()],
            )
            .and_then(|v| v.l());
        let obj = match result {
            Ok(obj) => obj,
            Err(e) => {
                eprintln!("rrcloud bridge: {method} JNI call failed: {e}");
                clear_exception(env);
                return None;
            }
        };
        if obj.is_null() {
            return None;
        }
        let jstr: JString = obj.into();
        let java_str = match env.get_string(&jstr) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("rrcloud bridge: {method} returned non-UTF8: {e}");
                clear_exception(env);
                return None;
            }
        };
        Some(java_str.into())
    }

    /// `ctx.getFilesDir().getAbsolutePath()` — the Android analogue of
    /// desktop's `app_data_dir` (`app_settings::get_settings_path` and
    /// `sync::commands::credential_store` both root under it on desktop),
    /// so `state_dir` below ends up at the same `<app_data_dir>/rrcloud`
    /// desktop uses, and `loadSyncSettingsJson`'s Kotlin implementation can
    /// locate the exact `settings.json` the app itself writes.
    fn files_dir(env: &mut JNIEnv, ctx: &JObject) -> Result<PathBuf, String> {
        let dir = env
            .call_method(ctx, "getFilesDir", "()Ljava/io/File;", &[])
            .and_then(|v| v.l())
            .map_err(|e| {
                clear_exception(env);
                format!("getFilesDir: {e}")
            })?;
        absolute_path(env, &dir)
    }

    /// `ctx.getExternalMediaDirs()[0]/.library` — the same primary-library
    /// root `android_integration::get_android_internal_library_root`
    /// already uses for manual-import writes, reused here so DCIM imports
    /// and manual SAF imports land under one library root.
    fn sync_root_dir(env: &mut JNIEnv, ctx: &JObject) -> Result<PathBuf, String> {
        let dirs_obj = env
            .call_method(ctx, "getExternalMediaDirs", "()[Ljava/io/File;", &[])
            .and_then(|v| v.l())
            .map_err(|e| {
                clear_exception(env);
                format!("getExternalMediaDirs: {e}")
            })?;
        if dirs_obj.is_null() {
            return Err("getExternalMediaDirs returned null".into());
        }
        let dirs: JObjectArray = dirs_obj.into();
        let first = env.get_object_array_element(&dirs, 0).map_err(|e| {
            clear_exception(env);
            format!("getExternalMediaDirs[0]: {e}")
        })?;
        if first.is_null() {
            return Err("primary external media dir is null".into());
        }
        Ok(absolute_path(env, &first)?.join(".library"))
    }

    fn absolute_path(env: &mut JNIEnv, file: &JObject) -> Result<PathBuf, String> {
        let path_jstring = env
            .call_method(file, "getAbsolutePath", "()Ljava/lang/String;", &[])
            .and_then(|v| v.l())
            .map_err(|e| {
                clear_exception(env);
                format!("getAbsolutePath: {e}")
            })?;
        let path: String = env
            .get_string(&path_jstring.into())
            .map_err(|e| {
                clear_exception(env);
                format!("getAbsolutePath string: {e}")
            })?
            .into();
        Ok(PathBuf::from(path))
    }

    /// Opens (minting a device id on first run via
    /// [`mint_worker_device_id`], reusing the §6 worker's own mint
    /// routine) the redb state store under `state_dir`. Classifies
    /// [`StateError::AlreadyLocked`] distinctly so the caller can map it to
    /// [`BridgeResult::RetryLockHeld`] (§5.1 cross-process exclusion).
    ///
    /// Only [`Java_com_plugin_rrcloud_RrcloudBridge_runSyncCycle`] takes
    /// [`CYCLE_LOCK`] before calling this; `dcimScanDecisions`,
    /// `dcimRecordImport` and `dirtyUnbackedCount` call it directly and
    /// rely on this function's own `AlreadyLocked`/`Locked` path for
    /// exclusion against a concurrently-running cycle. That is sound even
    /// same-process (not just cross-process): redb's `FileBackend` takes
    /// the OS file lock with a plain `libc::flock(fd, LOCK_EX | LOCK_NB)`
    /// (see redb's `src/tree_store/page_store/file_backend/unix.rs`), and
    /// POSIX `flock()` conflicts across *any* two open file descriptors on
    /// the same file, including two fds opened by the same process — not
    /// only across processes. So a `dcimScanDecisions` call racing a
    /// `runSyncCycle` in the same process still gets a real `Locked`
    /// result here, never a false "lock free". Verified by reading that
    /// redb source directly (redb 2.6.3); this is not inferred from
    /// behavior observed on a device.
    enum OpenOutcome {
        Opened(SyncDb),
        Locked,
        Failed(String),
    }

    fn open_state_db(state_dir: &std::path::Path) -> OpenOutcome {
        let redb_path = state_dir.join("state.redb");
        match SyncDb::open(&redb_path, None) {
            Ok(db) => OpenOutcome::Opened(db),
            Err(StateError::AlreadyLocked { .. }) => OpenOutcome::Locked,
            Err(StateError::DeviceIdRequired) => {
                let id = match mint_worker_device_id(state_dir) {
                    Ok(id) => id,
                    Err(e) => return OpenOutcome::Failed(format!("mint device id: {e}")),
                };
                match SyncDb::open(&redb_path, Some(id)) {
                    Ok(db) => OpenOutcome::Opened(db),
                    Err(StateError::AlreadyLocked { .. }) => OpenOutcome::Locked,
                    Err(e) => OpenOutcome::Failed(e.to_string()),
                }
            }
            Err(e) => OpenOutcome::Failed(e.to_string()),
        }
    }

    /// Reads `settings`+`credentials` via the two Kotlin callbacks and
    /// validates both are present/complete. `None` ⇒ the caller should
    /// report [`BridgeResult::FailureNotConfigured`].
    fn load_config(
        env: &mut JNIEnv,
        ctx: &JObject,
    ) -> Option<(AndroidSyncSettings, AndroidCredentials)> {
        let settings_json = call_static_string_method(env, ctx, "loadSyncSettingsJson")?;
        let settings: AndroidSyncSettings = match serde_json::from_str(&settings_json) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("rrcloud bridge: parse sync settings: {e}");
                return None;
            }
        };
        if !settings.is_usable() {
            return None;
        }
        let creds_json = call_static_string_method(env, ctx, "loadCredentialsJson")?;
        let creds: AndroidCredentials = match serde_json::from_str(&creds_json) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("rrcloud bridge: parse credentials: {e}");
                return None;
            }
        };
        if !creds.is_complete() {
            return None;
        }
        Some((settings, creds))
    }

    /// A [`CancelFlag`] that fires on its own once `budget`'s deadline
    /// passes, via a background task sleeping for the remaining time. The
    /// pumps below only ever observe it **between** items (never mid-item,
    /// [`CancelFlag`]'s own contract) — this is the mechanism that turns
    /// "budget expired" into "stop admitting new work units", per §5.1
    /// point 3.
    fn budget_cancel_flag(budget: CycleBudget) -> CancelFlag {
        let cancel = CancelFlag::new();
        let remaining = budget.remaining_ms(now_ms()).max(0) as u64;
        let spawned = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(remaining)).await;
            spawned.cancel();
        });
        cancel
    }

    /// One bounded sync cycle (ARCHITECTURE.md §5.1): upload lane, inbound
    /// journal apply, download lane — admitting/popping new work units
    /// only while `budget` has time left, never abandoning an in-flight
    /// item. Mirrors `sync::manager::Configured::run_cycle`'s shape
    /// (desktop, unbounded-to-quiescence) over the same `rrcloud-core`
    /// primitives, with a budget check between each lane and between each
    /// pump's admitted items.
    ///
    /// A per-item failure in the upload or download lane does NOT abort
    /// the cycle early: every lane still runs (budget permitting) —
    /// `publish_pending` still advertises whatever did upload, the
    /// inbound journal still applies, the download lane still runs — and
    /// only the first collected error is returned at the end, purely so
    /// the caller still maps this cycle to a retry. Returning early on
    /// the first failed item would otherwise waste a whole WorkManager
    /// window under ordinary flaky mobile connectivity (§5.1: "every
    /// window uploads a few items, applies journal pages, and commits").
    ///
    /// Proxy backfill (§4.3) and the §2.9 albums/presets meta lane are
    /// deliberately left to the desktop/worker roles for this first
    /// Android cut: both are best-effort additions layered on top of the
    /// same three lanes here, and a tight WorkManager window is better
    /// spent moving bytes than decoding a RAW for a smart preview. A later
    /// unit can fold them in following this function's exact budget-check
    /// shape.
    async fn run_bounded_cycle(
        db: &SyncDb,
        s3: &S3Client,
        bucket: &str,
        root: PathBuf,
        budget: CycleBudget,
    ) -> Result<(), String> {
        let backend = match stored_backend_profile(db).map_err(|e| e.to_string())? {
            Some(b) => b,
            None => probe_backend(db, s3, bucket)
                .await
                .map_err(|e| e.to_string())?,
        };
        let cfg = TransferConfig::new(bucket, root.clone(), backend);

        // Collected across every lane rather than returned on the first
        // failure (regression, P5 review round 0): an ordinary per-item
        // transient failure — one flaky S3 PUT/GET among several, not an
        // unrecoverable one — used to abort the WHOLE cycle immediately,
        // before `publish_pending` advertised whatever *did* upload
        // successfully and before the poll/download lanes got to run at
        // all that cycle. Every lane below now always runs (budget
        // permitting), and only the first collected error is surfaced at
        // the end — still enough to make the caller map this cycle to
        // [`BridgeResult::RetryTransient`], but no longer at the cost of
        // silently withholding already-finished work or skipping inbound
        // sync entirely under realistic flaky mobile connectivity.
        let mut first_error: Option<String> = None;

        if budget.is_expired(now_ms()) {
            return Ok(());
        }

        // Upload lane: admit every quiesced-dirty item (P1 admission
        // policy, same as desktop), pump under a budget-bound cancel flag,
        // publish whatever got staged either way (so a budget-interrupted
        // OR partially-failed pass still advertises what it did finish).
        admit_pending(db, |_, _| true).map_err(|e| e.to_string())?;
        let cancel = budget_cancel_flag(budget);
        let up = pump_uploads(db, s3, &cfg, 2, &cancel)
            .await
            .map_err(|e| e.to_string())?;
        if let Some((relkey, err)) = up.failed.into_iter().next() {
            first_error.get_or_insert(format!("upload failed for {relkey}: {err}"));
        }
        publish_pending(db, s3, bucket)
            .await
            .map_err(|e| e.to_string())?;

        if budget.is_expired(now_ms()) {
            return first_error.map_or(Ok(()), Err);
        }

        // Inbound journal: one poll pass, checked against the same
        // budget-bound cancel flag the transfer pumps use so a large
        // foreign backlog cannot alone run this lane past the caller's
        // execution window (regression, P5 review round 0 — see
        // `reader::poll_with_cancel`'s doc for the between-device/
        // between-segment checkpoint granularity).
        let mut events = ();
        let mut consumer =
            EngineConsumer::new(db, root.clone(), &mut events).map_err(|e| e.to_string())?;
        let poll_cancel = budget_cancel_flag(budget);
        poll_with_cancel(db, s3, bucket, &mut consumer, &poll_cancel)
            .await
            .map_err(|e| e.to_string())?;

        if budget.is_expired(now_ms()) {
            return first_error.map_or(Ok(()), Err);
        }

        // §3.5 client download POLICY: the poll above turned every foreign
        // original into a `PendingDown` download; before the pump fetches
        // their full bytes, demote the unpinned ones to browsable 0-byte cloud
        // STUBs that hydrate on demand. This is what makes an Android device a
        // client of the one shared per-user library (it mirrors the desktop
        // `sync::manager` cycle) rather than a full mirror of every S3 object.
        // The pass shares the cycle's cancel flag, so a budget expiry stops it
        // between items. If it FAILS (a state-db error, not a per-item
        // filesystem slip — those are logged inside and skipped) the pump
        // must not run: with the stub pass gone, `pump_downloads` would
        // eagerly fetch the full bytes of every received original, which is
        // the exact opposite of the §3.5 client policy and could pull
        // gigabytes onto the phone. The error is reported and the cycle ends
        // here; the next cycle retries the pass.
        let cancel = budget_cancel_flag(budget);
        match stub_pending_originals_bounded(db, &root, &cancel) {
            Ok(n) if n > 0 => log::info!("stubbed {n} received original(s)"),
            Ok(_) => {}
            Err(e) => {
                log::warn!("stub_pending_originals: {e}");
                first_error.get_or_insert(format!("stub pass failed: {e}"));
                return first_error.map_or(Ok(()), Err);
            }
        }

        let down = pump_downloads(db, s3, &cfg, 2, &cancel)
            .await
            .map_err(|e| e.to_string())?;
        if let Some((relkey, err)) = down.failed.into_iter().next() {
            first_error.get_or_insert(format!("download failed for {relkey}: {err}"));
        }
        first_error.map_or(Ok(()), Err)
    }

    /// Runs one bounded sync cycle (ARCHITECTURE.md §5.1).
    ///
    /// Java/Kotlin signature this must match exactly:
    /// `external fun runSyncCycle(context: Context, budgetMs: Long): Int`
    /// in the `RrcloudBridge` Kotlin object, package `com.plugin.rrcloud`.
    ///
    /// Parameters:
    /// - `ctx`: the Android `Context` (`ApplicationContext` from the
    ///   caller — `SyncCycleWorker`/`DcimScanWorker`'s `applicationContext`,
    ///   or the foreground service's own context), used to initialize
    ///   `ndk_context`/`rustls_platform_verifier` and to reach the
    ///   settings/`CredentialStore` callbacks.
    /// - `budget_ms`: milliseconds remaining in the caller's execution
    ///   window (the WorkManager ~10-minute expedited budget minus
    ///   already-spent time, or the foreground-sync service's own pacing)
    ///   — becomes a [`CycleBudget`].
    ///
    /// Returns a [`BridgeResult::to_code`] value; see
    /// `super::result_code`'s module table for the complete mapping the
    /// Kotlin caller must apply to the return value.
    #[unsafe(no_mangle)]
    pub extern "system" fn Java_com_plugin_rrcloud_RrcloudBridge_runSyncCycle<'local>(
        mut env: JNIEnv<'local>,
        _class: JClass<'local>,
        ctx: JObject<'local>,
        budget_ms: jlong,
    ) -> jint {
        let start = now_ms();
        let budget = CycleBudget::from_budget_ms(start, budget_ms);

        // Single-instance safety, in-process half (§5.1): a same-process
        // concurrent cycle backs off immediately. The cross-process half
        // (redb's own file lock) is checked when `open_state_db` runs,
        // below.
        let _guard = match CYCLE_LOCK.try_lock() {
            Ok(g) => g,
            Err(_) => return BridgeResult::RetryLockHeld.to_code(),
        };

        super::init_ndk_context(&mut env, &ctx);
        super::init_rustls_platform_verifier(&mut env, &ctx);

        let Some((settings, creds)) = load_config(&mut env, &ctx) else {
            return BridgeResult::FailureNotConfigured.to_code();
        };

        let state_dir = match files_dir(&mut env, &ctx) {
            Ok(p) => p.join("rrcloud"),
            Err(e) => {
                eprintln!("rrcloud bridge: {e}");
                return BridgeResult::FailurePermanent.to_code();
            }
        };
        if let Err(e) = std::fs::create_dir_all(&state_dir) {
            eprintln!("rrcloud bridge: create state dir: {e}");
            return BridgeResult::FailurePermanent.to_code();
        }
        let root = match sync_root_dir(&mut env, &ctx) {
            Ok(p) => p,
            Err(e) => {
                eprintln!("rrcloud bridge: {e}");
                return BridgeResult::FailurePermanent.to_code();
            }
        };

        let db = match open_state_db(&state_dir) {
            OpenOutcome::Opened(db) => db,
            OpenOutcome::Locked => return BridgeResult::RetryLockHeld.to_code(),
            OpenOutcome::Failed(e) => {
                eprintln!("rrcloud bridge: open state db: {e}");
                return BridgeResult::FailurePermanent.to_code();
            }
        };

        let s3 = match S3Client::new(S3Config {
            endpoint: settings.endpoint.clone(),
            region: settings.region.clone(),
            access_key_id: creds.access_key.clone(),
            secret_access_key: creds.secret_key.clone(),
            connect_timeout: None,
            read_timeout: None,
            request_timeout: None,
        }) {
            Ok(s3) => s3,
            Err(e) => {
                eprintln!("rrcloud bridge: build S3 client: {e}");
                return BridgeResult::FailureNotConfigured.to_code();
            }
        };

        let runtime = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => rt,
            Err(e) => {
                eprintln!("rrcloud bridge: build runtime: {e}");
                return BridgeResult::FailurePermanent.to_code();
            }
        };

        match runtime.block_on(run_bounded_cycle(&db, &s3, &settings.bucket, root, budget)) {
            Ok(()) => BridgeResult::Success.to_code(),
            Err(e) => {
                eprintln!("rrcloud bridge: cycle: {e}");
                BridgeResult::RetryTransient.to_code()
            }
        }
    }

    /// The §5.2 batch DCIM dedupe decision: Kotlin's `DcimScanWorker`/
    /// `ContentObserver` path hands over one `MediaStore` scan pass's
    /// candidates as a JSON array `[{"path":str,"size":u64,
    /// "mtimeUnix":i64}, ...]`; this opens the redb state store once for
    /// the whole batch (not once per candidate) and returns a JSON array
    /// `[{"path":str,"decision":"skip"|"rehash"|"new",
    /// "contentId":str|null}, ...]` in the same order, by delegating each
    /// row to [`decide`] against the existing `dcim_seen` table, fetched
    /// per candidate via [`SyncDb::dcim_seen_for_path`] — **not**
    /// [`SyncDb::dcim_seen`], whose exact-`(size, mtime)` key can only
    /// ever confirm an unchanged file; `dcim_seen_for_path` reuses the
    /// table as-is but at the one-row-per-path granularity `decide` needs
    /// to be able to return `Rehash`. On `skip` Kotlin does no I/O at all;
    /// on `rehash`/`new`
    /// Kotlin streams the file and calls
    /// [`Java_com_plugin_rrcloud_RrcloudBridge_dcimRecordImport`], whose
    /// content-hash churn gate ([`notify_local_change`]) is the actual
    /// authority on whether the bytes changed — this decision only saves
    /// Kotlin the I/O of re-copying an unchanged file.
    ///
    /// Failure (state db unavailable, bad JSON) returns Java `null`, which
    /// the Kotlin caller treats as "scan this pass again next time" — no
    /// candidate is marked seen either way, so nothing is lost.
    #[unsafe(no_mangle)]
    pub extern "system" fn Java_com_plugin_rrcloud_RrcloudBridge_dcimScanDecisions<'local>(
        mut env: JNIEnv<'local>,
        _class: JClass<'local>,
        ctx: JObject<'local>,
        candidates_json: JString<'local>,
    ) -> jni::sys::jstring {
        let null = std::ptr::null_mut();
        let candidates_str: String = match env.get_string(&candidates_json) {
            Ok(s) => s.into(),
            Err(e) => {
                eprintln!("rrcloud bridge: dcimScanDecisions: bad argument: {e}");
                return null;
            }
        };
        #[derive(serde::Deserialize)]
        struct Candidate {
            path: String,
            size: u64,
            #[serde(rename = "mtimeUnix")]
            mtime_unix: i64,
        }
        let candidates: Vec<Candidate> = match serde_json::from_str(&candidates_str) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("rrcloud bridge: dcimScanDecisions: bad JSON: {e}");
                return null;
            }
        };

        let state_dir = match files_dir(&mut env, &ctx) {
            Ok(p) => p.join("rrcloud"),
            Err(e) => {
                eprintln!("rrcloud bridge: {e}");
                return null;
            }
        };
        // `DcimScanWorker` can run before `SyncCycleWorker` ever has in this
        // process/device's lifetime (independent periodic schedules, no
        // ordering guarantee) — confirmed on-device: a fresh install's
        // first-ever scan hit `open_state_db` before `<filesDir>/rrcloud`
        // existed at all, since only `runSyncCycle` created it. Every
        // bridge entry point that touches the state db must ensure this
        // directory exists itself, not assume another entry point already
        // ran first.
        if let Err(e) = std::fs::create_dir_all(&state_dir) {
            eprintln!("rrcloud bridge: dcimScanDecisions: create state dir: {e}");
            return null;
        }
        let db = match open_state_db(&state_dir) {
            OpenOutcome::Opened(db) => db,
            OpenOutcome::Locked => {
                eprintln!("rrcloud bridge: dcimScanDecisions: state db locked");
                return null;
            }
            OpenOutcome::Failed(e) => {
                eprintln!("rrcloud bridge: dcimScanDecisions: open state db: {e}");
                return null;
            }
        };

        #[derive(serde::Serialize)]
        struct Decision {
            path: String,
            decision: &'static str,
            #[serde(rename = "contentId", skip_serializing_if = "Option::is_none")]
            content_id: Option<String>,
        }
        let mut out = Vec::with_capacity(candidates.len());
        for c in candidates {
            // `dcim_seen_for_path` (not `dcim_seen`, which is keyed by the
            // exact candidate `(size, mtime)` and so can only ever confirm
            // an unchanged file) fetches whatever row currently exists for
            // this path regardless of its recorded `(size, mtime)` — the
            // one-row-per-path shape `decide` needs to be able to return
            // `Rehash` for a changed file.
            let row = match db.dcim_seen_for_path(&c.path) {
                Ok(Some((size, mtime_unix, content_id))) => Some(SeenRow {
                    size,
                    mtime_unix,
                    content_id,
                }),
                Ok(None) => None,
                Err(e) => {
                    eprintln!(
                        "rrcloud bridge: dcimScanDecisions: dcim_seen_for_path {}: {e}",
                        c.path
                    );
                    None
                }
            };
            let decided = decide(row.as_ref(), c.size, c.mtime_unix);
            out.push(match decided {
                DcimDecision::Skip { content_id } => Decision {
                    path: c.path,
                    decision: "skip",
                    content_id: Some(content_id.as_str().to_string()),
                },
                DcimDecision::Rehash => Decision {
                    path: c.path,
                    decision: "rehash",
                    content_id: None,
                },
                DcimDecision::New => Decision {
                    path: c.path,
                    decision: "new",
                    content_id: None,
                },
            });
        }

        let json = match serde_json::to_string(&out) {
            Ok(j) => j,
            Err(e) => {
                eprintln!("rrcloud bridge: dcimScanDecisions: encode result: {e}");
                return null;
            }
        };
        match env.new_string(json) {
            Ok(s) => s.into_raw(),
            Err(e) => {
                eprintln!("rrcloud bridge: dcimScanDecisions: new_string: {e}");
                null
            }
        }
    }

    /// Records one freshly-copied DCIM import (§5.2): `source_path`/
    /// `source_size`/`source_mtime_unix` identify the *MediaStore* file the
    /// scan observed (the [`SyncDb::set_dcim_seen`] key); `final_path` is
    /// where Kotlin already streamed+renamed its bytes to (under
    /// `<library>/DCIM-import/`, §5.2 "streaming copy"). Reads
    /// `final_path`'s bytes once to run the normal new-original intake
    /// ([`notify_local_change`] — the same content-hash churn gate every
    /// other new-original hook uses, see `sync::hooks::notify_new_original`
    /// on desktop) and records the dedupe row so a later unchanged re-scan
    /// of the same source short-circuits at
    /// [`Java_com_plugin_rrcloud_RrcloudBridge_dcimScanDecisions`]'s `skip`
    /// path without touching the file again.
    #[unsafe(no_mangle)]
    pub extern "system" fn Java_com_plugin_rrcloud_RrcloudBridge_dcimRecordImport<'local>(
        mut env: JNIEnv<'local>,
        _class: JClass<'local>,
        ctx: JObject<'local>,
        source_path: JString<'local>,
        source_size: jlong,
        source_mtime_unix: jlong,
        final_path: JString<'local>,
    ) -> jint {
        let source_path: String = match env.get_string(&source_path) {
            Ok(s) => s.into(),
            Err(e) => {
                eprintln!("rrcloud bridge: dcimRecordImport: bad source_path: {e}");
                return BridgeResult::FailurePermanent.to_code();
            }
        };
        let final_path: String = match env.get_string(&final_path) {
            Ok(s) => s.into(),
            Err(e) => {
                eprintln!("rrcloud bridge: dcimRecordImport: bad final_path: {e}");
                return BridgeResult::FailurePermanent.to_code();
            }
        };
        let final_path = PathBuf::from(final_path);

        let state_dir = match files_dir(&mut env, &ctx) {
            Ok(p) => p.join("rrcloud"),
            Err(e) => {
                eprintln!("rrcloud bridge: {e}");
                return BridgeResult::FailurePermanent.to_code();
            }
        };
        // See the matching comment in `dcimScanDecisions`: this entry point
        // can likewise run before `runSyncCycle` ever has.
        if let Err(e) = std::fs::create_dir_all(&state_dir) {
            eprintln!("rrcloud bridge: dcimRecordImport: create state dir: {e}");
            return BridgeResult::FailurePermanent.to_code();
        }
        let root = match sync_root_dir(&mut env, &ctx) {
            Ok(p) => p,
            Err(e) => {
                eprintln!("rrcloud bridge: {e}");
                return BridgeResult::FailurePermanent.to_code();
            }
        };
        let db = match open_state_db(&state_dir) {
            OpenOutcome::Opened(db) => db,
            OpenOutcome::Locked => return BridgeResult::RetryLockHeld.to_code(),
            OpenOutcome::Failed(e) => {
                eprintln!("rrcloud bridge: dcimRecordImport: open state db: {e}");
                return BridgeResult::FailurePermanent.to_code();
            }
        };

        let bytes = match std::fs::read(&final_path) {
            Ok(b) => b,
            Err(e) => {
                eprintln!(
                    "rrcloud bridge: dcimRecordImport: read {}: {e}",
                    final_path.display()
                );
                return BridgeResult::FailurePermanent.to_code();
            }
        };
        let rk = match relkey(&final_path, &root) {
            Ok(r) => r,
            Err(e) => {
                eprintln!(
                    "rrcloud bridge: dcimRecordImport: relkey {}: {e}",
                    final_path.display()
                );
                return BridgeResult::FailurePermanent.to_code();
            }
        };
        let mtime_unix_ns = std::fs::metadata(&final_path)
            .and_then(|m| m.modified())
            .ok()
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0);
        let scan = LocalScan {
            size: bytes.len() as u64,
            mtime_unix_ns,
            bytes: &bytes,
        };
        // The outcome (new/changed/unchanged) only matters for logging —
        // `notify_local_change` already applied the right churn-gated
        // effect in every case, including `Unchanged` (a retried copy after
        // a crash before `set_dcim_seen` committed is a legitimate no-op,
        // not an error). It never hands back a content id either way, so
        // hash the bytes already in hand instead of re-reading the file.
        if let Err(e) = notify_local_change(&db, &rk, Kind::Original, &scan) {
            eprintln!("rrcloud bridge: dcimRecordImport: {}: {e}", rk);
            return BridgeResult::FailurePermanent.to_code();
        }
        let content_id = ContentId::from_bytes(&bytes);

        if let Err(e) = db.set_dcim_seen(
            &source_path,
            source_size as u64,
            source_mtime_unix,
            &content_id,
        ) {
            eprintln!("rrcloud bridge: dcimRecordImport: set_dcim_seen {source_path}: {e}");
            return BridgeResult::FailurePermanent.to_code();
        }

        BridgeResult::Success.to_code()
    }

    /// The §5.4 "N edits not backed up" persistent-notification trigger:
    /// counts items in an upload-lane state (dirty-and-not-yet-backed-up —
    /// the same set `sync::manager::Configured::dirty_count` counts on
    /// desktop). Kotlin's periodic check (piggybacked on
    /// `SyncCycleWorker`'s own run) persists the wall-clock instant this
    /// first reads non-zero and raises the notification once 24h have
    /// elapsed with it still non-zero — that bookkeeping lives entirely on
    /// the Kotlin side (`DirtyWatch.kt`) since it is a plain `SharedPreferences`
    /// timestamp, not engine state. Returns `-1` on any failure (state db
    /// unavailable) — Kotlin treats that as "unknown, skip this check" the
    /// same way it treats a `null` settings/credentials read.
    #[unsafe(no_mangle)]
    pub extern "system" fn Java_com_plugin_rrcloud_RrcloudBridge_dirtyUnbackedCount<'local>(
        mut env: JNIEnv<'local>,
        _class: JClass<'local>,
        ctx: JObject<'local>,
    ) -> jint {
        let state_dir = match files_dir(&mut env, &ctx) {
            Ok(p) => p.join("rrcloud"),
            Err(e) => {
                eprintln!("rrcloud bridge: dirtyUnbackedCount: {e}");
                return -1;
            }
        };
        // See the matching comment in `dcimScanDecisions`: this entry point
        // can likewise run before `runSyncCycle` ever has.
        if let Err(e) = std::fs::create_dir_all(&state_dir) {
            eprintln!("rrcloud bridge: dirtyUnbackedCount: create state dir: {e}");
            return -1;
        }
        let db = match open_state_db(&state_dir) {
            OpenOutcome::Opened(db) => db,
            OpenOutcome::Locked => return -1,
            OpenOutcome::Failed(e) => {
                eprintln!("rrcloud bridge: dirtyUnbackedCount: open state db: {e}");
                return -1;
            }
        };
        let items = match db.iter_items() {
            Ok(items) => items,
            Err(e) => {
                eprintln!("rrcloud bridge: dirtyUnbackedCount: iter_items: {e}");
                return -1;
            }
        };
        use crate::state::ItemState;
        items
            .into_iter()
            .filter(|(_, r)| {
                !r.deleted
                    && matches!(
                        r.state,
                        ItemState::Dirty
                            | ItemState::Queued
                            | ItemState::Uploading
                            | ItemState::Verifying
                    )
            })
            .count() as jint
    }
}

#[cfg(target_os = "android")]
pub use imp::*;

/// Android platform init shared by every entry point above: `ndk_context`
/// (so this process's `rrcloud-core` code — none of which calls it today,
/// but keeping the context initialized is cheap and matches
/// `android_integration::initialize_android`'s own pattern exactly) and
/// `rustls_platform_verifier` (so the `S3Client`'s `reqwest`/`rustls`
/// stack trusts the Android platform's certificate store in this
/// process). Both calls here go through [`super::platform_init`]'s
/// process-wide shared guards, NOT a `Once` local to this module — this
/// process is the SAME process as the app (ARCHITECTURE.md §5.1; no
/// `android:process` override exists on any manifest entry), so a local
/// guard here would have no idea `android_integration.rs`'s own call site
/// already ran, or is about to. See `platform_init`'s module doc for the
/// on-device crash that shape caused.
#[cfg(target_os = "android")]
fn init_ndk_context(env: &mut jni::JNIEnv, ctx: &jni::objects::JObject) {
    if let Ok(vm) = env.get_java_vm() {
        let vm_ptr = vm.get_java_vm_pointer() as *mut std::ffi::c_void;
        // `ctx` here is a plain native-method argument -- the JNI spec
        // guarantees that is a fresh LOCAL reference, valid only for this
        // one call. It must NOT be handed to the shared guard as-is: if
        // this call wins the process-wide `Once` race (plausible -- a
        // `WorkManager` worker can run before the app ever opens a
        // webview in a cold-started process), `ndk_context` would keep
        // this local's raw pointer forever, and every later
        // `android_integration.rs` read of
        // `ndk_context::android_context().context()` would dereference a
        // handle freed when this JNI call returns -- see
        // `platform_init`'s module doc for the full "local vs. global
        // reference" rationale.
        //
        // `ensure_ndk_context_initialized` only calls this closure at
        // all if this call actually wins that race (at most once per
        // process, see that function's doc), so only the single winning
        // call ever promotes-and-leaks a global ref here -- a losing
        // call (the common case: this runs on every `SyncCycleWorker`/
        // `DcimScanWorker` invocation) never does.
        super::platform_init::ensure_ndk_context_initialized(vm_ptr, || {
            match env.new_global_ref(ctx) {
                Ok(global_ctx) => {
                    let context_ptr = global_ctx.as_obj().as_raw() as *mut std::ffi::c_void;
                    // Intentionally leaked: this global reference must
                    // outlive this native call -- outlive this whole
                    // process, in fact, exactly like `ndk_context` itself
                    // assumes of whatever pointer it is given. Dropping
                    // `global_ctx` would `DeleteGlobalRef` it and
                    // reintroduce the exact dangling-pointer bug this
                    // promotion exists to prevent.
                    std::mem::forget(global_ctx);
                    context_ptr
                }
                Err(e) => {
                    eprintln!(
                        "rrcloud bridge: failed to promote ctx to a global ref for ndk_context init: {e}"
                    );
                    std::ptr::null_mut()
                }
            }
        });
    } else {
        eprintln!("rrcloud bridge: could not obtain JavaVM for ndk_context init");
    }
}

/// See [`init_ndk_context`] — same shared-guard rationale, routed through
/// [`super::platform_init::ensure_rustls_platform_verifier_initialized`].
#[cfg(target_os = "android")]
fn init_rustls_platform_verifier(env: &mut jni::JNIEnv, ctx: &jni::objects::JObject) {
    let raw_env = env.get_raw() as *mut jni22::sys::JNIEnv;
    let raw_context = ctx.as_raw() as jni22::sys::jobject;
    super::platform_init::ensure_rustls_platform_verifier_initialized(raw_env, raw_context);
}
