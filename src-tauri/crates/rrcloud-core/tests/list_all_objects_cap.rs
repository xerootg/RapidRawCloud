//! Failing test: `S3Client::list_all_objects` (`src/s3/client.rs`) paginates
//! under a **page** cap (`MAX_LIST_PAGES` = 100_000) but has **no cumulative
//! key cap**. A page may legally carry up to 1000 keys, so a hostile
//! endpoint — or, in fleet mode, a hostile admin bucket, which
//! `fleet::run_fleet_cycle` lists under `users/` with exactly this helper —
//! that answers every page with 1000 keys, `IsTruncated=true` and a fresh
//! `NextContinuationToken` makes the caller buffer up to 100 million
//! `ObjectSummary` values (several GB) before the page cap trips.
//!
//! The sibling listers were capped on keys in PR #11
//! (`worker::list_objects_bounded`, `LIST_MAX_KEYS = 1_000_000`;
//! `reader::list_journal_keys`, `compact::list_keys_under`). This suite
//! pins the same bound on `list_all_objects`: the listing must be refused
//! with [`S3Error::InvalidResponse`] after at most ~1_000_000 buffered keys,
//! i.e. after at most [`MAX_ACCEPTABLE_LIST_PAGES`] full pages — long
//! before the 100_000-page cap.
//!
//! The suite does NOT need Garage: it runs a minimal fake S3 HTTP server on
//! `127.0.0.1` (std threads, keep-alive HTTP/1.1) that answers only
//! `ListObjectsV2`. The 1000-key page body is rendered ONCE; only the
//! fixed-width continuation token varies per page, so the server costs
//! almost nothing and the measured behaviour is the client's. The listing
//! under test runs under a hard timeout AND a page-count watchdog so a
//! runaway is reported as a clear failure (pages served, keys buffered)
//! instead of eating all memory or wedging the suite.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rrcloud_core::s3::{S3Client, S3Config, S3Error};

/// Bucket the fake server serves (any other path is answered 404).
const BUCKET: &str = "hostile-admin-bucket";

/// The prefix `fleet::run_fleet_cycle` lists on the admin bucket.
const PREFIX: &str = "users/";

/// Keys per page: the S3 default/maximum `MaxKeys`. A hostile endpoint
/// uses the biggest legal page so each round-trip buys the most buffering.
const KEYS_PER_PAGE: u64 = 1000;

/// The cumulative key cap the fix must enforce (mirrors
/// `worker::LIST_MAX_KEYS` / `compact::list_keys_under`'s `MAX_KEYS`).
const EXPECTED_KEY_CAP: u64 = 1_000_000;

/// Upper bound on LIST pages a correctly capped `list_all_objects` may
/// consume before refusing. With 1000 keys per page the cap of
/// `EXPECTED_KEY_CAP` keys is exceeded on page `EXPECTED_KEY_CAP / 1000 + 1`
/// (`listed > cap` after that page), so a conforming fix issues at most
/// 1001 LIST requests.
const MAX_ACCEPTABLE_LIST_PAGES: u64 = EXPECTED_KEY_CAP / KEYS_PER_PAGE + 1;

/// Watchdog: once the endpoint has served this many pages, the client has
/// buffered `WATCHDOG_PAGES * KEYS_PER_PAGE` keys — twice the expected
/// cap — and the test stops the run and fails rather than letting the
/// uncapped loop keep growing the result vector (the current code would
/// otherwise only stop at `MAX_LIST_PAGES` = 100_000 pages = 100 million
/// keys, several GB).
const WATCHDOG_PAGES: u64 = 2 * MAX_ACCEPTABLE_LIST_PAGES;

/// Hard wall-clock bound on the listing under test. A debug build parses
/// a full 1000-key page (~170 KiB of XML) at roughly 40 pages/s, so a
/// capped `list_all_objects` needs ~25 s for its 1001 round-trips; 120 s
/// leaves a wide margin on a loaded machine. The failing run is normally
/// stopped by the page watchdog (~2000 pages, ~50 s) before this elapses.
const LIST_TIMEOUT: Duration = Duration::from_secs(120);

// ---------------------------------------------------------------------------
// Fake S3 server
// ---------------------------------------------------------------------------

/// How the fake endpoint answers `ListObjectsV2` under `PREFIX`.
#[derive(Clone, Copy)]
enum Listing {
    /// Every page is full, truncated and carries a fresh token: the listing
    /// never ends.
    Endless,
    /// Pages `1..n` are full and truncated; page `n` is full and final.
    Pages(u64),
}

/// A running fake S3 endpoint.
struct FakeS3 {
    /// `http://127.0.0.1:<port>`.
    endpoint: String,
    /// Number of `ListObjectsV2` pages served under `PREFIX`.
    pages: Arc<AtomicU64>,
    /// Number of LIST requests whose `continuation-token` was not the token
    /// the previous page handed out (a client bug, not the one under test —
    /// must stay 0).
    bad_tokens: Arc<AtomicU64>,
}

/// Fixed-width continuation token for `page` (constant length keeps the
/// pre-rendered body's `Content-Length` constant too).
fn token_for(page: u64) -> String {
    format!("tok-{page:020}")
}

/// Render the two halves of a full 1000-key page body around the
/// `NextContinuationToken` value, so a page is `head + token + tail` with
/// no per-page formatting. `truncated=false` renders a final page (no
/// token element; `tail` is then the whole remainder and `head` the whole
/// start, the token slot being empty).
fn render_page(truncated: bool) -> (String, String) {
    let mut contents = String::with_capacity(200 * KEYS_PER_PAGE as usize);
    for i in 0..KEYS_PER_PAGE {
        contents.push_str(&format!(
            "<Contents><Key>{PREFIX}hostile/{i:04}.dng</Key>\
             <LastModified>2026-10-03T13:47:23.060Z</LastModified>\
             <ETag>&quot;00000000000000000000000000000000&quot;</ETag>\
             <Size>4096</Size></Contents>"
        ));
    }
    let head = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Name>{BUCKET}</Name><Prefix>{PREFIX}</Prefix><KeyCount>{KEYS_PER_PAGE}</KeyCount>\
         <MaxKeys>{KEYS_PER_PAGE}</MaxKeys><IsTruncated>{truncated}</IsTruncated>\
         {contents}"
    );
    if truncated {
        (
            format!("{head}<NextContinuationToken>"),
            "</NextContinuationToken></ListBucketResult>".to_string(),
        )
    } else {
        (head, "</ListBucketResult>".to_string())
    }
}

/// Start the fake server.
fn spawn_fake_s3(listing: Listing) -> FakeS3 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake S3");
    let addr = listener.local_addr().expect("local_addr");
    let pages = Arc::new(AtomicU64::new(0));
    let bad_tokens = Arc::new(AtomicU64::new(0));
    let truncated_page: Arc<(String, String)> = Arc::new(render_page(true));
    let final_page: Arc<(String, String)> = Arc::new(render_page(false));
    {
        let pages = Arc::clone(&pages);
        let bad_tokens = Arc::clone(&bad_tokens);
        std::thread::spawn(move || {
            for conn in listener.incoming() {
                let Ok(sock) = conn else { break };
                let pages = Arc::clone(&pages);
                let bad_tokens = Arc::clone(&bad_tokens);
                let truncated_page = Arc::clone(&truncated_page);
                let final_page = Arc::clone(&final_page);
                std::thread::spawn(move || {
                    serve_connection(
                        sock,
                        listing,
                        &truncated_page,
                        &final_page,
                        &pages,
                        &bad_tokens,
                    );
                });
            }
        });
    }
    FakeS3 {
        endpoint: format!("http://127.0.0.1:{}", addr.port()),
        pages,
        bad_tokens,
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

/// Write one keep-alive response whose body is `parts` concatenated.
fn write_response(sock: &mut TcpStream, status: &str, parts: &[&[u8]]) -> std::io::Result<()> {
    let len: usize = parts.iter().map(|p| p.len()).sum();
    let head = format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/xml\r\nContent-Length: {len}\r\n\
         Connection: keep-alive\r\n\r\n"
    );
    sock.write_all(head.as_bytes())?;
    for part in parts {
        sock.write_all(part)?;
    }
    sock.flush()
}

/// Serve one keep-alive connection until the client closes it.
fn serve_connection(
    mut sock: TcpStream,
    listing: Listing,
    truncated_page: &(String, String),
    final_page: &(String, String),
    pages: &AtomicU64,
    bad_tokens: &AtomicU64,
) {
    // A dropped client (the test tore the future down) must not pin this
    // thread forever.
    sock.set_read_timeout(Some(Duration::from_secs(60))).ok();
    sock.set_nodelay(true).ok();
    let mut carry = Vec::new();
    while let Some(req) = read_request(&mut sock, &mut carry) {
        let bucket_path = format!("/{BUCKET}");
        let on_bucket = req.path == bucket_path || req.path == format!("{bucket_path}/");
        let is_list = req.method == "GET"
            && on_bucket
            && query_param(&req.query, "list-type").as_deref() == Some("2")
            && query_param(&req.query, "prefix").as_deref() == Some(PREFIX);
        let result = if is_list {
            let page = pages.fetch_add(1, Ordering::SeqCst) + 1;
            // The client must echo exactly the token the previous page
            // handed out (none on the first page).
            let expected = (page > 1).then(|| token_for(page - 1));
            if query_param(&req.query, "continuation-token") != expected {
                bad_tokens.fetch_add(1, Ordering::SeqCst);
            }
            let is_final = match listing {
                Listing::Endless => false,
                Listing::Pages(n) => page >= n,
            };
            if is_final {
                write_response(
                    &mut sock,
                    "200 OK",
                    &[final_page.0.as_bytes(), final_page.1.as_bytes()],
                )
            } else {
                let token = token_for(page);
                write_response(
                    &mut sock,
                    "200 OK",
                    &[
                        truncated_page.0.as_bytes(),
                        token.as_bytes(),
                        truncated_page.1.as_bytes(),
                    ],
                )
            }
        } else {
            let body = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>NoSuchKey</Code>\
                 <Message>The specified key does not exist.</Message>\
                 <Resource>{}</Resource><RequestId>fake</RequestId></Error>",
                req.path
            );
            write_response(&mut sock, "404 Not Found", &[body.as_bytes()])
        };
        if result.is_err() {
            break;
        }
    }
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

fn client_for(endpoint: &str) -> S3Client {
    S3Client::new(S3Config {
        endpoint: endpoint.to_string(),
        region: "garage".to_string(),
        access_key_id: "GKtestaccesskey".to_string(),
        secret_access_key: "testsecret".to_string(),
        connect_timeout: None,
        read_timeout: None,
        request_timeout: None,
    })
    .expect("build S3 client")
}

/// Spawn a task that prints the served-page counter every few seconds so a
/// runaway is visible under `--nocapture`.
fn spawn_progress_monitor(pages: Arc<AtomicU64>, lane: &str) -> tokio::task::JoinHandle<()> {
    let lane = lane.to_string();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(2));
        tick.tick().await;
        loop {
            tick.tick().await;
            let served = pages.load(Ordering::SeqCst);
            eprintln!(
                "[{lane}] fake S3: {served} ListObjectsV2 pages served so far \
                 (~{} keys buffered by the client), list_all_objects still running",
                served * KEYS_PER_PAGE
            );
        }
    })
}

/// Completes once the fake server has handed out `WATCHDOG_PAGES` pages.
async fn page_watchdog(pages: Arc<AtomicU64>) {
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    loop {
        tick.tick().await;
        if pages.load(Ordering::SeqCst) >= WATCHDOG_PAGES {
            return;
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// A `users/` listing that never ends (1000 keys per page, always
/// truncated, fresh token every time) must be refused by
/// `list_all_objects` with `S3Error::InvalidResponse` naming
/// `ListObjectsV2`, after at most ~1_000_000 buffered keys — the key cap,
/// not the 100_000-page cap, has to be what stops it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn endless_full_pages_are_refused_after_bounded_keys() {
    const LANE: &str = "endless";
    let fake = spawn_fake_s3(Listing::Endless);
    let client = client_for(&fake.endpoint);
    let monitor = spawn_progress_monitor(Arc::clone(&fake.pages), LANE);

    let listing = client.list_all_objects(BUCKET, Some(PREFIX));
    let watchdog = page_watchdog(Arc::clone(&fake.pages));
    let started = std::time::Instant::now();
    let outcome = tokio::time::timeout(LIST_TIMEOUT, async {
        tokio::select! {
            result = listing => Some(result),
            () = watchdog => None,
        }
    })
    .await;
    monitor.abort();

    let pages = fake.pages.load(Ordering::SeqCst);
    let keys = pages * KEYS_PER_PAGE;
    let elapsed = started.elapsed();
    eprintln!(
        "[{LANE}] list_all_objects returned={} pages_served={pages} keys_buffered~{keys} \
         elapsed={elapsed:?}",
        matches!(outcome, Ok(Some(_)))
    );
    assert_eq!(
        fake.bad_tokens.load(Ordering::SeqCst),
        0,
        "[{LANE}] the client sent a continuation token the server never issued"
    );
    assert!(
        pages > 0,
        "[{LANE}] no ListObjectsV2 reached the fake server; the harness is wrong"
    );

    let result = match outcome {
        Ok(Some(result)) => result,
        Ok(None) => panic!(
            "[{LANE}] list_all_objects has NO cumulative key cap: the endpoint served \
             {pages} full ListObjectsV2 pages ({keys} keys buffered in the result Vec, \
             {elapsed:?}) and the client was still paging. A conforming cap refuses after \
             at most {MAX_ACCEPTABLE_LIST_PAGES} pages (> {EXPECTED_KEY_CAP} keys, like \
             worker::list_objects_bounded's LIST_MAX_KEYS); the only bound today is \
             MAX_LIST_PAGES = 100_000 pages = 100 million ObjectSummary values."
        ),
        Err(_elapsed) => panic!(
            "[{LANE}] list_all_objects did not terminate within {LIST_TIMEOUT:?}: {pages} \
             full ListObjectsV2 pages served ({keys} keys buffered) and still paging. \
             It has no cumulative key cap (compare worker::list_objects_bounded's \
             LIST_MAX_KEYS = {EXPECTED_KEY_CAP})."
        ),
    };

    assert!(
        pages <= MAX_ACCEPTABLE_LIST_PAGES,
        "[{LANE}] list_all_objects terminated, but only after {pages} full pages \
         ({keys} keys buffered): expected a cumulative key cap of {EXPECTED_KEY_CAP} \
         keys (at most {MAX_ACCEPTABLE_LIST_PAGES} pages), not the 100_000-page cap"
    );
    match result {
        Err(S3Error::InvalidResponse(msg)) => {
            assert!(
                msg.contains("ListObjectsV2"),
                "[{LANE}] refusal must name the runaway listing, got: {msg}"
            );
            eprintln!("[{LANE}] refused as expected after {pages} pages: {msg}");
        }
        Err(other) => panic!(
            "[{LANE}] expected S3Error::InvalidResponse(..) naming the ListObjectsV2 key \
             cap after {pages} pages, got {other:?}"
        ),
        Ok(objects) => panic!(
            "[{LANE}] list_all_objects returned Ok with {} objects after {pages} truncated \
             pages: a never-ending listing must be a typed refusal, not a silently \
             truncated success",
            objects.len()
        ),
    }
}

/// Positive control: a listing that ends normally after three full pages
/// returns every key, in order, through the same fake server — so the
/// refusal above cannot be explained by the harness rejecting ordinary
/// paging.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn listing_that_ends_after_three_pages_returns_all_keys() {
    const LANE: &str = "control";
    const PAGES: u64 = 3;
    let fake = spawn_fake_s3(Listing::Pages(PAGES));
    let client = client_for(&fake.endpoint);

    let objects = tokio::time::timeout(LIST_TIMEOUT, client.list_all_objects(BUCKET, Some(PREFIX)))
        .await
        .expect("a 3-page listing must finish well within the timeout")
        .expect("a 3-page listing must succeed");

    let pages = fake.pages.load(Ordering::SeqCst);
    eprintln!(
        "[{LANE}] list_all_objects returned {} objects over {pages} pages",
        objects.len()
    );
    assert_eq!(
        pages, PAGES,
        "[{LANE}] expected exactly {PAGES} LIST round-trips"
    );
    assert_eq!(
        fake.bad_tokens.load(Ordering::SeqCst),
        0,
        "[{LANE}] the client sent a continuation token the server never issued"
    );
    assert_eq!(objects.len() as u64, PAGES * KEYS_PER_PAGE);
    for (i, object) in objects.iter().enumerate() {
        let expected = format!("{PREFIX}hostile/{:04}.dng", i as u64 % KEYS_PER_PAGE);
        assert_eq!(object.key, expected, "[{LANE}] object #{i}");
        assert_eq!(object.size, 4096);
        assert_eq!(object.e_tag, "00000000000000000000000000000000");
        assert_eq!(
            object.last_modified.as_deref(),
            Some("2026-10-03T13:47:23.060Z")
        );
    }
}
