//! Secret redaction in `Debug` output.
//!
//! `S3Config` (`rrcloud_core::s3::S3Config`) hand-writes its `Debug` so the
//! secret access key renders as `<redacted>` (`src/s3/client.rs`, pinned by
//! its own unit test). Every other public struct in this crate that carries
//! an S3 secret must give the same guarantee: `{:?}` is routine in
//! `tracing` fields, `anyhow`/`thiserror` context, and panic messages, and
//! a derived `Debug` prints the plaintext secret into whichever log file
//! those end up in.
//!
//! Redaction must be **targeted**: the access key id (and the other
//! coordinates) stay visible so the output is still useful for debugging.

use std::path::PathBuf;
use std::time::Duration;

use rrcloud_core::fleet::FleetConfig;
use rrcloud_core::s3::S3Config;
use rrcloud_core::worker::WorkerConfig;

const SECRET: &str = "SUPERSECRET-DO-NOT-PRINT";
const ACCESS_KEY_ID: &str = "GKtestaccesskeyid";

fn assert_redacted(rendered: &str, type_name: &str) {
    assert!(
        !rendered.contains(SECRET),
        "{type_name}: secret access key leaked into Debug output: {rendered}"
    );
    assert!(
        rendered.contains("<redacted>"),
        "{type_name}: Debug output should carry an explicit redaction marker: {rendered}"
    );
    // Targeted redaction: the non-secret coordinates must stay visible.
    assert!(
        rendered.contains(ACCESS_KEY_ID),
        "{type_name}: access key id should remain visible in Debug output: {rendered}"
    );
}

/// `WorkerConfig` (`src/worker.rs`) is built from `RRCLOUD_SECRET_KEY` (and,
/// in the fleet, from each user's config doc). Its `Debug` must redact the
/// secret exactly like `S3Config` does.
#[test]
fn worker_config_debug_redacts_the_secret_access_key() {
    let cfg = WorkerConfig {
        state_dir: Some(PathBuf::from("/var/lib/rrcloud/state")),
        endpoint: "http://127.0.0.1:3900".to_string(),
        bucket: "library".to_string(),
        region: "garage".to_string(),
        access_key_id: ACCESS_KEY_ID.to_string(),
        secret_access_key: SECRET.to_string(),
    };

    for rendered in [format!("{cfg:?}"), format!("{cfg:#?}")] {
        assert_redacted(&rendered, "WorkerConfig");
        // The rest of the config stays visible for debugging.
        assert!(rendered.contains("http://127.0.0.1:3900"), "{rendered}");
        assert!(rendered.contains("library"), "{rendered}");
        assert!(rendered.contains("garage"), "{rendered}");
        assert!(rendered.contains("/var/lib/rrcloud/state"), "{rendered}");
    }
}

/// `FleetConfig` (`src/fleet.rs`) holds its admin credentials inside an
/// `S3Config`, so it inherits that type's redaction transitively. Pinned
/// here so a future flattening of the credentials into `FleetConfig`
/// itself cannot silently reintroduce the leak.
#[test]
fn fleet_config_debug_redacts_the_admin_secret_access_key() {
    let cfg = FleetConfig {
        state_dir: PathBuf::from("/var/lib/rrcloud/fleet"),
        admin_bucket: "rrcloud-admin".to_string(),
        admin_s3: S3Config {
            endpoint: "http://127.0.0.1:3900".to_string(),
            region: "garage".to_string(),
            access_key_id: ACCESS_KEY_ID.to_string(),
            secret_access_key: SECRET.to_string(),
            connect_timeout: Some(Duration::from_secs(30)),
            read_timeout: Some(Duration::from_secs(60)),
            request_timeout: None,
        },
    };

    for rendered in [format!("{cfg:?}"), format!("{cfg:#?}")] {
        assert_redacted(&rendered, "FleetConfig");
        assert!(rendered.contains("rrcloud-admin"), "{rendered}");
    }
}
