//! Multi-user "fleet" backfill for the optional pairing deployment.
//!
//! The pairing service (`rrc.themissing.xyz`, see `pairing/`) stores one
//! config document per user in a small **admin** S3 bucket at
//! `users/<username>/config.json`:
//!
//! ```json
//! { "version": 1, "sync": { "endpoint": "...", "bucket": "...",
//!     "region": "...", "workerBackfill": true },
//!   "credentials": { "accessKeyId": "...", "secretAccessKey": "..." } }
//! ```
//!
//! A single headless worker deployment can then serve **every** paired
//! user: [`run_fleet_cycle`] lists those config docs, and for each user runs
//! exactly one ordinary [`crate::worker`] cycle against **that user's own
//! library bucket with that user's own credentials**, in a per-user state
//! sub-directory. The worker never holds the users' secrets itself — it
//! reads them from the admin bucket (with a read-only admin key) at the
//! start of each cycle.
//!
//! This module is pure orchestration on top of [`crate::worker`]: it adds
//! no new sync logic. A single user's failure (unreachable bucket, bad
//! credentials, corrupt config doc) is recorded and skipped, never aborting
//! the rest of the fleet. Users who turned `workerBackfill` off are skipped.

use std::path::PathBuf;

use serde::Deserialize;

use crate::s3::{S3Client, S3Config};
use crate::worker::{self, CycleOptions, Worker, WorkerConfig, WorkerError, DEFAULT_REGION};

/// `RRCLOUD_ADMIN_BUCKET` — the admin/config bucket name.
pub const ENV_ADMIN_BUCKET: &str = "RRCLOUD_ADMIN_BUCKET";
/// `RRCLOUD_ADMIN_ENDPOINT` — S3 endpoint for the admin bucket.
pub const ENV_ADMIN_ENDPOINT: &str = "RRCLOUD_ADMIN_ENDPOINT";
/// `RRCLOUD_ADMIN_REGION` — admin bucket region (default [`DEFAULT_REGION`]).
pub const ENV_ADMIN_REGION: &str = "RRCLOUD_ADMIN_REGION";
/// `RRCLOUD_ADMIN_ACCESS_KEY` — admin bucket access key id (read-only key).
pub const ENV_ADMIN_ACCESS_KEY: &str = "RRCLOUD_ADMIN_ACCESS_KEY";
/// `RRCLOUD_ADMIN_SECRET_KEY` — admin bucket secret key.
pub const ENV_ADMIN_SECRET_KEY: &str = "RRCLOUD_ADMIN_SECRET_KEY";

/// Per-user config docs larger than this are rejected (corrupt / hostile) —
/// a real doc is well under a kilobyte.
const CONFIG_DOC_MAX_BYTES: usize = 64 * 1024;

/// Admin-bucket coordinates + the base state directory for a fleet run.
#[derive(Clone, Debug)]
pub struct FleetConfig {
    /// Base persistent state directory; each user gets a `<base>/<username>`
    /// sub-directory so their redb state stores never collide.
    pub state_dir: PathBuf,
    /// The admin/config bucket.
    pub admin_bucket: String,
    /// S3 coordinates + (read-only) credentials for the admin bucket.
    pub admin_s3: S3Config,
}

impl FleetConfig {
    /// Resolve from the environment. All of [`ENV_ADMIN_BUCKET`],
    /// [`ENV_ADMIN_ENDPOINT`], [`ENV_ADMIN_ACCESS_KEY`],
    /// [`ENV_ADMIN_SECRET_KEY`] and [`crate::worker::ENV_STATE_DIR`] are
    /// mandatory (the fleet always journals, so it needs a state dir);
    /// [`ENV_ADMIN_REGION`] defaults to [`DEFAULT_REGION`].
    pub fn from_env() -> Result<FleetConfig, WorkerError> {
        fn req(name: &'static str) -> Result<String, WorkerError> {
            std::env::var(name)
                .ok()
                .filter(|s| !s.is_empty())
                .ok_or(WorkerError::MissingConfig(name))
        }
        let state_dir = req(worker::ENV_STATE_DIR)?;
        let region = std::env::var(ENV_ADMIN_REGION)
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| DEFAULT_REGION.to_string());
        Ok(FleetConfig {
            state_dir: PathBuf::from(state_dir),
            admin_bucket: req(ENV_ADMIN_BUCKET)?,
            admin_s3: S3Config {
                endpoint: req(ENV_ADMIN_ENDPOINT)?,
                region,
                access_key_id: req(ENV_ADMIN_ACCESS_KEY)?,
                secret_access_key: req(ENV_ADMIN_SECRET_KEY)?,
                connect_timeout: Some(std::time::Duration::from_secs(30)),
                read_timeout: Some(std::time::Duration::from_secs(60)),
                request_timeout: None,
            },
        })
    }
}

/// The `sync` sub-object of a config doc — only the fields the worker needs.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DocSync {
    endpoint: String,
    bucket: String,
    #[serde(default)]
    region: Option<String>,
    /// When `false`, this user opted out of worker-side backfill; the fleet
    /// skips them (their own devices still back up their edits).
    #[serde(default)]
    worker_backfill: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DocCreds {
    access_key_id: String,
    secret_access_key: String,
}

#[derive(Deserialize)]
struct ConfigDoc {
    sync: DocSync,
    credentials: DocCreds,
}

/// What happened to one user in a fleet cycle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UserOutcome {
    /// The user's cycle ran successfully.
    Ran,
    /// The user has `workerBackfill` disabled — intentionally skipped.
    SkippedOptedOut,
    /// Something went wrong for this user (recorded, not fatal to the fleet).
    Failed(String),
}

/// Aggregate result of one fleet cycle.
#[derive(Clone, Debug, Default)]
pub struct FleetReport {
    /// One entry per config doc found, in listing order: `(username, outcome)`.
    pub users: Vec<(String, UserOutcome)>,
}

impl FleetReport {
    /// Count of users whose cycle actually ran.
    pub fn ran(&self) -> usize {
        self.users
            .iter()
            .filter(|(_, o)| matches!(o, UserOutcome::Ran))
            .count()
    }
    /// Count of users that failed (non-fatal).
    pub fn failed(&self) -> usize {
        self.users
            .iter()
            .filter(|(_, o)| matches!(o, UserOutcome::Failed(_)))
            .count()
    }
}

/// `users/<username>/config.json` → `<username>`. Returns `None` for any key
/// that is not exactly a per-user config doc, or whose username segment is
/// unsafe to use as a path component (empty, `.`/`..`, or containing a path
/// separator — the last cannot occur after the split, but is checked
/// defensively).
pub fn username_from_config_key(key: &str) -> Option<&str> {
    let rest = key.strip_prefix("users/")?;
    let user = rest.strip_suffix("/config.json")?;
    if user.is_empty() || user == "." || user == ".." || user.contains('/') || user.contains('\\') {
        return None;
    }
    Some(user)
}

/// Run one backfill cycle for every paired user in the admin bucket.
///
/// Fatal (returns `Err`): the admin bucket cannot be listed. Everything
/// per-user — a missing/corrupt config doc, a user whose library is
/// unreachable, a redb open failure — is captured in the [`FleetReport`] and
/// the loop continues to the next user. Users are processed sequentially so
/// that redb state stores and transfer concurrency never pile up across the
/// whole fleet at once.
pub async fn run_fleet_cycle(
    fleet: &FleetConfig,
    opts: &CycleOptions,
) -> Result<FleetReport, WorkerError> {
    let admin = S3Client::new(fleet.admin_s3.clone())?;
    // Fatal if the admin bucket itself is unreachable — without the config
    // docs there is nothing to do and retrying the whole cycle is correct.
    let objects = admin
        .list_all_objects(&fleet.admin_bucket, Some("users/"))
        .await?;

    let mut report = FleetReport::default();
    for obj in objects {
        let Some(username) = username_from_config_key(&obj.key) else {
            continue; // not a per-user config doc (e.g. a nested key)
        };
        let outcome = run_one_user(fleet, &admin, username, opts).await;
        report.users.push((username.to_string(), outcome));
    }
    Ok(report)
}

/// Fetch + parse one user's config doc and run their cycle. Never panics or
/// propagates — every failure becomes a [`UserOutcome::Failed`].
async fn run_one_user(
    fleet: &FleetConfig,
    admin: &S3Client,
    username: &str,
    opts: &CycleOptions,
) -> UserOutcome {
    let key = format!("users/{username}/config.json");
    let bytes = match admin.get_object(&fleet.admin_bucket, &key, None).await {
        Ok(out) => match out.body.collect_capped(CONFIG_DOC_MAX_BYTES).await {
            Ok(b) => b,
            Err(e) => return UserOutcome::Failed(format!("read config doc: {e}")),
        },
        Err(e) => return UserOutcome::Failed(format!("get config doc: {e}")),
    };
    let doc: ConfigDoc = match serde_json::from_slice(&bytes) {
        Ok(d) => d,
        Err(e) => return UserOutcome::Failed(format!("parse config doc: {e}")),
    };
    if !doc.sync.worker_backfill {
        return UserOutcome::SkippedOptedOut;
    }
    if doc.sync.endpoint.is_empty()
        || doc.sync.bucket.is_empty()
        || doc.credentials.access_key_id.is_empty()
        || doc.credentials.secret_access_key.is_empty()
    {
        return UserOutcome::Failed("config doc missing endpoint/bucket/credentials".into());
    }

    let cfg = WorkerConfig {
        state_dir: Some(fleet.state_dir.join(username)),
        endpoint: doc.sync.endpoint,
        bucket: doc.sync.bucket,
        region: doc
            .sync
            .region
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| DEFAULT_REGION.to_string()),
        access_key_id: doc.credentials.access_key_id,
        secret_access_key: doc.credentials.secret_access_key,
    };
    let worker = match Worker::open(&cfg) {
        Ok(w) => w,
        Err(e) => return UserOutcome::Failed(format!("open worker: {e}")),
    };
    match worker::run_cycle(&worker, opts).await {
        Ok(_) => UserOutcome::Ran,
        Err(e) => UserOutcome::Failed(format!("cycle: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn username_parsing_accepts_only_real_config_docs() {
        assert_eq!(
            username_from_config_key("users/alice/config.json"),
            Some("alice")
        );
        assert_eq!(
            username_from_config_key("users/bob.smith_1/config.json"),
            Some("bob.smith_1")
        );
        // Not config docs.
        assert_eq!(username_from_config_key("users/alice/other.json"), None);
        assert_eq!(username_from_config_key("users/config.json"), None);
        assert_eq!(username_from_config_key("library/x/config.json"), None);
        assert_eq!(username_from_config_key("users//config.json"), None);
        // Path-traversal / unsafe segments are rejected so the per-user
        // state_dir join can never escape the base directory.
        assert_eq!(username_from_config_key("users/../config.json"), None);
        assert_eq!(username_from_config_key("users/./config.json"), None);
        // A nested "username" containing a separator (can't come from our
        // own keys, but defended anyway).
        assert_eq!(username_from_config_key("users/a/b/config.json"), None);
    }

    #[test]
    fn config_doc_parses_the_pairing_services_exact_json() {
        // Byte-identical shape to what `pairing/` writes (camelCase).
        let json = br#"{
            "version": 1,
            "updatedAt": "2026-10-07T00:00:00Z",
            "sync": {
                "enabled": true,
                "endpoint": "https://garage.themissing.xyz",
                "bucket": "rapidraw-cloud",
                "region": "garage",
                "forcePathStyle": true,
                "cacheSizeGb": 8,
                "previewBudgetGb": 10,
                "previewPrefetchMonths": 12,
                "autoWatchDcim": false,
                "watchedMediaBuckets": [],
                "workerBackfill": true
            },
            "credentials": {
                "accessKeyId": "GKexample",
                "secretAccessKey": "secretexample"
            }
        }"#;
        let doc: ConfigDoc = serde_json::from_slice(json).expect("parse");
        assert_eq!(doc.sync.endpoint, "https://garage.themissing.xyz");
        assert_eq!(doc.sync.bucket, "rapidraw-cloud");
        assert_eq!(doc.sync.region.as_deref(), Some("garage"));
        assert!(doc.sync.worker_backfill);
        assert_eq!(doc.credentials.access_key_id, "GKexample");
        assert_eq!(doc.credentials.secret_access_key, "secretexample");
    }

    #[test]
    fn config_doc_defaults_worker_backfill_off_when_absent() {
        // A doc without workerBackfill → opted out (skipped), never a parse
        // error, and never an accidental opt-in.
        let json = br#"{
            "sync": { "endpoint": "https://e", "bucket": "b" },
            "credentials": { "accessKeyId": "k", "secretAccessKey": "s" }
        }"#;
        let doc: ConfigDoc = serde_json::from_slice(json).expect("parse");
        assert!(!doc.sync.worker_backfill);
        assert!(doc.sync.region.is_none());
    }

    #[test]
    fn fleet_report_counts() {
        let report = FleetReport {
            users: vec![
                ("a".into(), UserOutcome::Ran),
                ("b".into(), UserOutcome::SkippedOptedOut),
                ("c".into(), UserOutcome::Failed("boom".into())),
                ("d".into(), UserOutcome::Ran),
            ],
        };
        assert_eq!(report.ran(), 2);
        assert_eq!(report.failed(), 1);
    }
}
