//! Failing test: the headless worker's `ListObjectsV2` pagination loops
//! (`worker::list_library_keys`, `worker::list_preview_content_ids`) have
//! **no page cap and no key cap**, unlike `reader::list_journal_keys` and
//! `compact::list_keys_under`, which refuse after a bounded number of pages
//! / keys.
//!
//! A hostile S3 endpoint — or, in fleet mode, a hostile peer who controls
//! the bucket — that answers every page with `IsTruncated=true` and a fresh
//! `NextContinuationToken` therefore makes one `run_cycle` spin forever
//! while the collected key vector grows without bound. In fleet mode users
//! are processed sequentially, so one such bucket stalls every other user's
//! backfill until the process is restarted.
//!
//! The suite does NOT need Garage: it runs a minimal fake S3 HTTP server on
//! `127.0.0.1` (std threads, keep-alive HTTP/1.1) that satisfies exactly the
//! requests a journaling cycle makes before the listing under test —
//! heartbeat GET (404 `NoSuchKey`), heartbeat PUT (200 + ETag + Date), the
//! reader's journal LIST (empty) — and answers the listing under test with
//! an endless stream of truncated pages. The cycle is run under a hard
//! timeout so a hang is reported as a clear failure (with the page counter)
//! rather than wedging the whole test suite.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rrcloud_core::s3::S3Error;
use rrcloud_core::worker::{self, CycleOptions, Worker, WorkerConfig, WorkerError};

/// Bucket the fake server serves. Any path-style request to another bucket
/// is answered 404.
const BUCKET: &str = "hostile-bucket";

/// Keys per runaway page. Deliberately > 1 so a fix that caps total keys
/// (like `reader::list_journal_keys`'s `MAX_KEYS`) is also exercised:
/// `MAX_KEYS / KEYS_PER_PAGE` pages are then enough to trip it.
const KEYS_PER_PAGE: usize = 16;

/// How long one cycle may run before the test declares the loop unbounded.
/// A capped cycle issues at most `MAX_ACCEPTABLE_LIST_PAGES` round-trips
/// before refusing; at the ~600 pages/s this fake server sustains in a
/// debug build with both tests running in parallel, 10_000 pages take
/// ~17 s, so 60 s leaves a wide margin for a correct fix while still
/// bounding the failing run.
const CYCLE_TIMEOUT: Duration = Duration::from_secs(60);

/// Upper bound on LIST requests a correctly capped cycle may issue. The
/// reader's cap is 10_000 pages, so a fix mirroring it issues at most
/// 10_000 LIST requests before refusing.
const MAX_ACCEPTABLE_LIST_PAGES: u64 = 10_000;

// ---------------------------------------------------------------------------
// Fake S3 server
// ---------------------------------------------------------------------------

/// A running fake S3 endpoint.
struct FakeS3 {
    /// `http://127.0.0.1:<port>`.
    endpoint: String,
    /// Number of `ListObjectsV2` requests seen under the runaway prefix.
    runaway_pages: Arc<AtomicU64>,
    /// Number of `ListObjectsV2` requests seen under any other prefix.
    other_list_pages: Arc<AtomicU64>,
}

/// Start a fake S3 server whose `ListObjectsV2` under `runaway_prefix`
/// never terminates (always `IsTruncated=true` + a fresh token), while
/// every other LIST is an empty, non-truncated page.
fn spawn_fake_s3(runaway_prefix: &'static str) -> FakeS3 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake S3");
    let addr = listener.local_addr().expect("local_addr");
    let runaway_pages = Arc::new(AtomicU64::new(0));
    let other_list_pages = Arc::new(AtomicU64::new(0));
    {
        let runaway_pages = Arc::clone(&runaway_pages);
        let other_list_pages = Arc::clone(&other_list_pages);
        std::thread::spawn(move || {
            for conn in listener.incoming() {
                let Ok(sock) = conn else { break };
                let runaway_pages = Arc::clone(&runaway_pages);
                let other_list_pages = Arc::clone(&other_list_pages);
                std::thread::spawn(move || {
                    serve_connection(sock, runaway_prefix, &runaway_pages, &other_list_pages);
                });
            }
        });
    }
    FakeS3 {
        endpoint: format!("http://127.0.0.1:{}", addr.port()),
        runaway_pages,
        other_list_pages,
    }
}

/// One parsed HTTP/1.1 request head.
struct Request {
    method: String,
    path: String,
    query: String,
}

/// Read one request head (and discard its body) from a keep-alive socket.
/// Returns `None` on EOF / error / timeout.
fn read_request(sock: &mut TcpStream, carry: &mut Vec<u8>) -> Option<Request> {
    let mut buf = [0u8; 8192];
    let head_end = loop {
        if let Some(pos) = carry.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos + 4;
        }
        let n = sock.read(&mut buf).ok()?;
        if n == 0 {
            return None;
        }
        carry.extend_from_slice(&buf[..n]);
    };
    let head = String::from_utf8_lossy(&carry[..head_end]).into_owned();
    let mut lines = head.split("\r\n");
    let request_line = lines.next()?;
    let mut parts = request_line.split(' ');
    let method = parts.next()?.to_string();
    let target = parts.next()?;
    let (path, query) = match target.split_once('?') {
        Some((p, q)) => (p.to_string(), q.to_string()),
        None => (target.to_string(), String::new()),
    };
    let mut content_length = 0usize;
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            if name.eq_ignore_ascii_case("content-length") {
                content_length = value.trim().parse().unwrap_or(0);
            }
        }
    }
    // Drain the body (we never need its contents).
    carry.drain(..head_end);
    while carry.len() < content_length {
        let n = sock.read(&mut buf).ok()?;
        if n == 0 {
            return None;
        }
        carry.extend_from_slice(&buf[..n]);
    }
    carry.drain(..content_length);
    Some(Request {
        method,
        path,
        query,
    })
}

/// Minimal percent-decoding for query values (SigV4 canonical encoding:
/// `%XX` only, no `+`).
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(v) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The decoded value of query parameter `name`, if present.
fn query_param(query: &str, name: &str) -> Option<String> {
    query.split('&').find_map(|kv| {
        let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
        (k == name).then(|| percent_decode(v))
    })
}

/// An RFC 7231 IMF-fixdate `Date` header value for "now", in the exact
/// shape `publisher::parse_http_date` accepts (`..., DD Mon YYYY HH:MM:SS GMT`).
fn http_date_now() -> String {
    let now = time::OffsetDateTime::now_utc();
    let fmt = time::macros::format_description!(
        "[weekday repr:short], [day] [month repr:short] [year] [hour]:[minute]:[second] GMT"
    );
    now.format(&fmt).expect("format http date")
}

fn write_response(
    sock: &mut TcpStream,
    status: &str,
    extra_headers: &[(&str, &str)],
    body: &[u8],
    include_body: bool,
) -> std::io::Result<()> {
    let mut head = format!(
        "HTTP/1.1 {status}\r\nDate: {}\r\nContent-Length: {}\r\nConnection: keep-alive\r\n",
        http_date_now(),
        body.len()
    );
    for (k, v) in extra_headers {
        head.push_str(k);
        head.push_str(": ");
        head.push_str(v);
        head.push_str("\r\n");
    }
    head.push_str("\r\n");
    sock.write_all(head.as_bytes())?;
    if include_body {
        sock.write_all(body)?;
    }
    sock.flush()
}

/// Serve one keep-alive connection until the client closes it.
fn serve_connection(
    mut sock: TcpStream,
    runaway_prefix: &str,
    runaway_pages: &AtomicU64,
    other_list_pages: &AtomicU64,
) {
    // A dropped client (the test timed out and tore the future down) must
    // not pin this thread forever.
    sock.set_read_timeout(Some(Duration::from_secs(60))).ok();
    sock.set_nodelay(true).ok();
    let mut carry = Vec::new();
    while let Some(req) = read_request(&mut sock, &mut carry) {
        let is_head = req.method == "HEAD";
        let bucket_path = format!("/{BUCKET}");
        let on_bucket = req.path == bucket_path || req.path == format!("{bucket_path}/");
        let is_list = req.method == "GET"
            && on_bucket
            && query_param(&req.query, "list-type").as_deref() == Some("2");

        let result = if is_list {
            let prefix = query_param(&req.query, "prefix").unwrap_or_default();
            if prefix.starts_with(runaway_prefix) {
                // The hostile answer: a valid, non-empty page that is ALWAYS
                // truncated with a fresh continuation token.
                let page = runaway_pages.fetch_add(1, Ordering::SeqCst) + 1;
                let mut contents = String::new();
                for i in 0..KEYS_PER_PAGE {
                    contents.push_str(&format!(
                        "<Contents><Key>{prefix}hostile/page-{page}/img-{i}.dng</Key>\
                         <LastModified>2026-10-03T13:47:23.060Z</LastModified>\
                         <ETag>&quot;00000000000000000000000000000000&quot;</ETag>\
                         <Size>4096</Size></Contents>"
                    ));
                }
                let body = format!(
                    r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>{BUCKET}</Name><Prefix>{prefix}</Prefix><KeyCount>{KEYS_PER_PAGE}</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>true</IsTruncated><NextContinuationToken>tok-{page}</NextContinuationToken>{contents}</ListBucketResult>"#
                );
                write_response(
                    &mut sock,
                    "200 OK",
                    &[("Content-Type", "application/xml")],
                    body.as_bytes(),
                    true,
                )
            } else {
                other_list_pages.fetch_add(1, Ordering::SeqCst);
                let body = format!(
                    r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>{BUCKET}</Name><Prefix>{prefix}</Prefix><KeyCount>0</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated></ListBucketResult>"#
                );
                write_response(
                    &mut sock,
                    "200 OK",
                    &[("Content-Type", "application/xml")],
                    body.as_bytes(),
                    true,
                )
            }
        } else if req.method == "PUT" {
            // Heartbeat registry PUT (and anything else written): accepted.
            // ETag + Date are what `put_device_entry` needs.
            write_response(
                &mut sock,
                "200 OK",
                &[("ETag", "\"d41d8cd98f00b204e9800998ecf8427e\"")],
                b"",
                true,
            )
        } else {
            // Any object GET/HEAD/DELETE: nothing exists here.
            let body = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>NoSuchKey</Code>\
                 <Message>The specified key does not exist.</Message>\
                 <Resource>{}</Resource><RequestId>fake</RequestId></Error>",
                req.path
            );
            write_response(
                &mut sock,
                "404 Not Found",
                &[("Content-Type", "application/xml")],
                body.as_bytes(),
                !is_head,
            )
        };
        if result.is_err() {
            break;
        }
    }
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

fn worker_cfg(endpoint: &str, state_dir: std::path::PathBuf) -> WorkerConfig {
    WorkerConfig {
        state_dir: Some(state_dir),
        endpoint: endpoint.to_string(),
        bucket: BUCKET.to_string(),
        region: "garage".to_string(),
        access_key_id: "GKtestaccesskey".to_string(),
        secret_access_key: "testsecret".to_string(),
    }
}

/// Run one journaling cycle against a fake endpoint whose listing under
/// `runaway_prefix` never ends, and assert the cycle TERMINATES with a
/// typed refusal after a bounded number of LIST pages.
async fn assert_cycle_refuses_runaway_listing(runaway_prefix: &'static str, lane: &str) {
    let fake = spawn_fake_s3(runaway_prefix);
    let state = tempfile::tempdir().expect("state dir");
    let cfg = worker_cfg(&fake.endpoint, state.path().to_path_buf());
    let worker = Worker::open(&cfg).expect("open journaling worker");

    // Progress monitor: makes the runaway visible under --nocapture (the
    // page counter climbing while the cycle never returns).
    let monitor = {
        let pages = Arc::clone(&fake.runaway_pages);
        let lane = lane.to_string();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(2));
            tick.tick().await;
            loop {
                tick.tick().await;
                eprintln!(
                    "[{lane}] fake S3: {} runaway ListObjectsV2 pages served so far, cycle still running",
                    pages.load(Ordering::SeqCst)
                );
            }
        })
    };

    let outcome = tokio::time::timeout(
        CYCLE_TIMEOUT,
        worker::run_cycle(&worker, &CycleOptions::default()),
    )
    .await;
    monitor.abort();

    let pages = fake.runaway_pages.load(Ordering::SeqCst);
    let other = fake.other_list_pages.load(Ordering::SeqCst);
    eprintln!(
        "[{lane}] cycle finished={} runaway_pages={pages} other_list_pages={other}",
        outcome.is_ok()
    );

    // The listing under test must actually have been reached (otherwise
    // the test proves nothing about the loop).
    assert!(
        pages > 0,
        "[{lane}] the cycle never issued a ListObjectsV2 under {runaway_prefix:?}; \
         the fake server precondition is wrong (other LIST pages: {other})"
    );

    let result = match outcome {
        Ok(result) => result,
        Err(_elapsed) => panic!(
            "[{lane}] worker list loop did not terminate: {pages} runaway ListObjectsV2 pages \
             served under {runaway_prefix:?} in {CYCLE_TIMEOUT:?} and the cycle was still \
             paging. The loop has no page cap and no key cap (compare \
             reader::list_journal_keys / compact::list_keys_under); a hostile bucket \
             stalls the worker (and, in fleet mode, every subsequent user) forever."
        ),
    };

    assert!(
        pages <= MAX_ACCEPTABLE_LIST_PAGES,
        "[{lane}] cycle terminated but only after {pages} LIST pages under {runaway_prefix:?} \
         (expected a cap of at most {MAX_ACCEPTABLE_LIST_PAGES} pages)"
    );
    match result {
        Err(WorkerError::S3(S3Error::InvalidResponse(msg))) => {
            assert!(
                msg.contains("ListObjectsV2"),
                "[{lane}] refusal must name the runaway listing, got: {msg}"
            );
        }
        Err(other) => panic!(
            "[{lane}] expected WorkerError::S3(S3Error::InvalidResponse(..)) naming the \
             listing cap, got {other:?}"
        ),
        Ok(report) => panic!(
            "[{lane}] cycle returned Ok after {pages} truncated pages under \
             {runaway_prefix:?}: a never-ending listing must be a typed refusal, not a \
             silently truncated success: {report:?}"
        ),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// `worker::list_library_keys` (reached from `adopt_foreign_originals`, and
/// from the read-only reporting path) pages `library/` with no cap.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn library_listing_that_never_ends_is_refused_after_bounded_pages() {
    assert_cycle_refuses_runaway_listing("library/", "library").await;
}

/// `worker::list_preview_content_ids` (reached unconditionally from
/// `backfill_known_previews`) pages `.rrcloud/v1/previews/` with no cap.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preview_listing_that_never_ends_is_refused_after_bounded_pages() {
    assert_cycle_refuses_runaway_listing(".rrcloud/v1/previews/", "previews").await;
}
