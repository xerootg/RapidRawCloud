//! Fleet endpoint validation (SSRF): a paired user's config doc in the admin
//! bucket must NOT be able to point the fleet worker at an internal /
//! non-public / plaintext endpoint.
//!
//! `fleet::run_one_user` builds a `WorkerConfig` from `sync.endpoint` and
//! `sync.bucket` exactly as the (user-controlled) config doc states them and
//! then runs a full worker cycle against it — heartbeat GET + PUT, listing,
//! adoption GET/PUT, DELETEs — with SigV4-signed requests. The worker runs
//! inside the cluster, so any paired user can aim those verbs at loopback,
//! RFC1918 space, the cloud metadata address, or an in-cluster service name
//! (`*.svc.cluster.local`), and plain `http://` to a public host is a MITM
//! exposure. The only check today is `is_empty()`.
//!
//! The pin is twofold: each hostile user must come back as
//! `UserOutcome::Failed(msg)` where `msg` names the endpoint rejection and
//! the failure happened BEFORE the cycle (no `cycle:` prefix), and — the
//! part that cannot be faked by a message — a real `TcpListener` standing in
//! for a loopback-only service must observe **zero** connections.
//!
//! Positive control: an `https://` public-looking host passes validation and
//! fails later, inside the cycle, with a transport error — so a fix cannot
//! simply reject everything.

mod common;

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use common::garage;

use rrcloud_core::fleet::{run_fleet_cycle, FleetConfig, UserOutcome};
use rrcloud_core::s3::{PutObjectOptions, S3Client};
use rrcloud_core::worker::CycleOptions;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// A config doc byte-for-byte in the shape the pairing service writes, with
/// `workerBackfill: true` so the fleet does not short-circuit on opt-out
/// before it ever looks at the endpoint.
fn config_doc(endpoint: &str) -> String {
    format!(
        r#"{{
            "version": 1,
            "sync": {{
                "enabled": true,
                "endpoint": "{endpoint}",
                "bucket": "victim-bucket",
                "region": "garage",
                "forcePathStyle": true,
                "workerBackfill": true
            }},
            "credentials": {{
                "accessKeyId": "GKattacker",
                "secretAccessKey": "attacker-secret"
            }}
        }}"#
    )
}

async fn put_config_doc(admin: &S3Client, admin_bucket: &str, user: &str, endpoint: &str) {
    let key = format!("users/{user}/config.json");
    admin
        .put_object(
            admin_bucket,
            &key,
            Bytes::from(config_doc(endpoint)),
            &PutObjectOptions::default(),
        )
        .await
        .unwrap_or_else(|e| panic!("put {key}: {e}"));
}

/// What a loopback-only service (think `garage-admin` on `127.0.0.1:3903`)
/// would see: every accepted connection's request head, verbatim.
#[derive(Default)]
struct Observed {
    requests: Vec<String>,
}

/// Binds `127.0.0.1:0` and records every connection the worker makes to it.
/// Each accepted connection gets a short `403` so the worker fails fast
/// instead of hanging on its read timeout — the point is to *observe* the
/// contact, not to emulate S3.
fn spawn_canary() -> (u16, Arc<Mutex<Observed>>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind canary");
    let port = listener.local_addr().expect("canary addr").port();
    let observed = Arc::new(Mutex::new(Observed::default()));
    let sink = Arc::clone(&observed);
    std::thread::Builder::new()
        .name("ssrf-canary".into())
        .spawn(move || {
            for conn in listener.incoming() {
                let Ok(mut stream) = conn else { continue };
                let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
                let mut buf = vec![0u8; 8192];
                let n = stream.read(&mut buf).unwrap_or(0);
                let head = String::from_utf8_lossy(&buf[..n]).into_owned();
                sink.lock().expect("canary lock").requests.push(head);
                let _ = stream.write_all(
                    b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                );
                let _ = stream.flush();
            }
        })
        .expect("spawn canary thread");
    (port, observed)
}

fn fleet_cfg(g: &garage::Garage, admin_bucket: &str, state_dir: &tempfile::TempDir) -> FleetConfig {
    FleetConfig {
        state_dir: state_dir.path().to_path_buf(),
        admin_bucket: admin_bucket.to_string(),
        admin_s3: g.s3_config(),
    }
}

/// The failure a fix must produce for a hostile endpoint: it names the
/// endpoint, and it was raised BEFORE the per-user cycle started (the
/// `cycle:` prefix means the worker already heartbeat against the endpoint,
/// i.e. the request went out).
fn is_endpoint_rejection(msg: &str) -> bool {
    msg.to_ascii_lowercase().contains("endpoint") && !msg.starts_with("cycle:")
}

fn outcome_of<'a>(report: &'a rrcloud_core::fleet::FleetReport, user: &str) -> &'a UserOutcome {
    report
        .users
        .iter()
        .find(|(u, _)| u == user)
        .map(|(_, o)| o)
        .unwrap_or_else(|| panic!("no outcome recorded for user {user:?}: {:?}", report.users))
}

// ---------------------------------------------------------------------------
// The pin
// ---------------------------------------------------------------------------

/// Every hostile `sync.endpoint` below is rejected up front, with no TCP
/// contact to the target. The loopback cases are aimed at a real listener
/// whose accept log must stay empty.
#[tokio::test]
async fn fleet_rejects_non_public_or_plaintext_endpoints_without_connecting() {
    let Some(g) = garage::shared() else { return };
    let admin = g.client();
    let admin_bucket = g.create_unique_bucket("fleet-ssrf-admin");
    let state_dir = tempfile::tempdir().expect("tempdir");

    let (canary_port, observed) = spawn_canary();

    // (a) loopback — three spellings of the same address, all aimed at the
    //     canary. A fix that string-matches "127.0.0.1" and misses
    //     `localhost` or the decimal form is not a fix.
    // (b) RFC1918
    // (c) link-local / cloud metadata (the address is the point; a port is
    //     given so the test never talks to a real metadata service)
    // (d) in-cluster service DNS
    // (e) plain http:// to a public-looking host (MITM exposure). `.test`
    //     is an RFC 6761 reserved TLD, so it can never resolve.
    let hostile: Vec<(&str, String)> = vec![
        ("loopback-ip", format!("http://127.0.0.1:{canary_port}")),
        ("loopback-name", format!("http://localhost:{canary_port}")),
        (
            "loopback-decimal",
            format!("http://2130706433:{canary_port}"),
        ),
        ("loopback-v6", format!("http://[::1]:{canary_port}")),
        ("rfc1918", "http://10.0.0.5:3903".to_string()),
        ("metadata", "http://169.254.169.254:3903".to_string()),
        (
            "cluster-svc",
            "http://garage-admin.garage.svc.cluster.local:3903".to_string(),
        ),
        (
            "plain-http",
            "http://s3.rrcloud-public-looking.test".to_string(),
        ),
    ];
    for (user, endpoint) in &hostile {
        put_config_doc(&admin, &admin_bucket, user, endpoint).await;
    }

    let fleet = fleet_cfg(g, &admin_bucket, &state_dir);
    let report = run_fleet_cycle(&fleet, &CycleOptions::default())
        .await
        .expect("admin bucket listing must succeed");

    eprintln!("fleet report: {:#?}", report.users);
    let seen = observed.lock().expect("canary lock").requests.clone();
    eprintln!(
        "canary on 127.0.0.1:{canary_port} observed {} connection(s)",
        seen.len()
    );
    for (i, head) in seen.iter().enumerate() {
        let line = head.lines().next().unwrap_or("");
        let signed = head
            .lines()
            .find(|l| l.to_ascii_lowercase().starts_with("authorization:"))
            .map(|l| l.chars().take(60).collect::<String>())
            .unwrap_or_else(|| "<no Authorization header>".into());
        eprintln!("  canary[{i}]: {line}  |  {signed}");
    }

    assert_eq!(
        report.users.len(),
        hostile.len(),
        "one outcome per hostile config doc"
    );

    let mut problems = Vec::new();
    for (user, endpoint) in &hostile {
        match outcome_of(&report, user) {
            UserOutcome::Failed(msg) if is_endpoint_rejection(msg) => {}
            UserOutcome::Failed(msg) => problems.push(format!(
                "{user} ({endpoint}): failed, but NOT with an endpoint rejection raised before \
                 the cycle: {msg:?}"
            )),
            other => problems.push(format!(
                "{user} ({endpoint}): expected Failed(<endpoint rejection>), got {other:?}"
            )),
        }
    }

    // The part a message cannot fake: the loopback-only "service" must never
    // have been contacted at all.
    if !seen.is_empty() {
        problems.push(format!(
            "the fleet worker opened {} TCP connection(s) to the loopback canary at \
             127.0.0.1:{canary_port} (first request line: {:?}) — a user-controlled \
             endpoint reached a loopback service from the worker's network position",
            seen.len(),
            seen[0].lines().next().unwrap_or("")
        ));
    }

    assert!(
        problems.is_empty(),
        "fleet accepted hostile endpoints:\n  - {}",
        problems.join("\n  - ")
    );
}

/// Positive control: an `https://` endpoint on a public-looking host passes
/// endpoint validation. It is unreachable here (reserved TLD → NXDOMAIN), so
/// the user fails *inside* the cycle with a transport error — never with the
/// endpoint rejection. A fix that rejects every endpoint fails this test.
#[tokio::test]
async fn fleet_still_accepts_public_https_endpoints() {
    let Some(g) = garage::shared() else { return };
    let admin = g.client();
    let admin_bucket = g.create_unique_bucket("fleet-ssrf-control");
    let state_dir = tempfile::tempdir().expect("tempdir");

    put_config_doc(
        &admin,
        &admin_bucket,
        "control",
        "https://s3.rrcloud-control.test",
    )
    .await;

    let fleet = fleet_cfg(g, &admin_bucket, &state_dir);
    let report = run_fleet_cycle(&fleet, &CycleOptions::default())
        .await
        .expect("admin bucket listing must succeed");
    eprintln!("control report: {:#?}", report.users);

    match outcome_of(&report, "control") {
        UserOutcome::Failed(msg) => {
            assert!(
                !is_endpoint_rejection(msg),
                "public https endpoint was rejected by endpoint validation: {msg:?}"
            );
            assert!(
                msg.starts_with("cycle:"),
                "public https endpoint should get past validation and Worker::open and fail \
                 inside the cycle (transport error); got {msg:?}"
            );
        }
        other => panic!("control user should fail with a transport error, got {other:?}"),
    }
}
