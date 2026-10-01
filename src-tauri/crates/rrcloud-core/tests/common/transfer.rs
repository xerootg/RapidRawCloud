//! Shared test support for the transfer-engine suite: the counting /
//! fault-injecting [`S3TransferApi`] wrapper, a tampering [`ChunkSource`]
//! (pinning the §2.4 "hash of what was actually sent" requirement by
//! construction), item/record seeding, and byte helpers.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use base64::Engine as _;
use bytes::Bytes;
use futures::StreamExt as _;
use futures::TryStreamExt as _;
use md5::{Digest as _, Md5};
use rrcloud_core::clock::DeviceId;
use rrcloud_core::journal::Kind;
use rrcloud_core::keys::RelKey;
use rrcloud_core::s3::{
    ByteRange, ByteStream, CompleteMultipartUploadOutput, CompletedPart,
    CreateMultipartUploadOutput, GetObjectOutput, HeadObjectOutput, ListMultipartUploadsOutput,
    ListMultipartUploadsRequest, ListObjectsV2Output, ListObjectsV2Request, ListPartsOutput,
    ListPartsRequest, PartBody, PutObjectOptions, PutObjectOutput, S3Api, S3Client, S3Error,
    S3TransferApi, UploadPartOutput,
};
use rrcloud_core::semhash::Blake3Hex;
use rrcloud_core::state::{ItemRecord, ItemState, SyncDb};
use rrcloud_core::transfer::{BackendProfile, ChunkSource, SourceStream, TransferConfig};

/// 5 MiB — the smallest legal non-final part, used as the test part size
/// so a 3-part object stays ~10.5 MiB.
pub const PART_5MIB: u64 = 5 * 1024 * 1024;

/// A [`TransferConfig`] with 5 MiB parts and a 5 MiB multipart threshold
/// (so multipart engages on small test files) and a digest-verifying
/// backend profile.
pub fn test_cfg(bucket: &str, sync_root: &Path) -> TransferConfig {
    let mut cfg = TransferConfig::new(
        bucket,
        sync_root,
        BackendProfile {
            digest_rejection_works: true,
        },
    );
    cfg.part_size = PART_5MIB;
    cfg.multipart_threshold = PART_5MIB;
    cfg
}

// ---------------------------------------------------------------------------
// Bytes / hashes / files
// ---------------------------------------------------------------------------

/// Deterministic pseudo-random bytes.
pub fn patterned(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| ((i as u64).wrapping_mul(31).wrapping_add(seed as u64) % 251) as u8)
        .collect()
}

/// A 3-part multipart body for [`PART_5MIB`] parts: 2 × 5 MiB + a short
/// tail.
pub fn three_part_bytes(seed: u8) -> Vec<u8> {
    patterned(2 * PART_5MIB as usize + 512 * 1024 + 7, seed)
}

pub fn md5_hex(data: &[u8]) -> String {
    hex::encode(Md5::digest(data))
}

pub fn md5_b64(data: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(Md5::digest(data))
}

pub fn b3(data: &[u8]) -> Blake3Hex {
    Blake3Hex::from_bytes(data)
}

/// Writes `bytes` to `path`, creating parent directories.
pub fn write_file(path: &Path, bytes: &[u8]) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("create parent dirs");
    }
    std::fs::write(path, bytes).expect("write file");
}

/// The file's mtime as unix nanoseconds.
pub fn mtime_unix_ns(path: &Path) -> i64 {
    std::fs::metadata(path)
        .expect("metadata")
        .modified()
        .expect("mtime")
        .duration_since(std::time::UNIX_EPOCH)
        .expect("epoch")
        .as_nanos() as i64
}

/// The file's mtime as unix seconds.
pub fn mtime_unix(path: &Path) -> i64 {
    mtime_unix_ns(path) / 1_000_000_000
}

// ---------------------------------------------------------------------------
// Item seeding
// ---------------------------------------------------------------------------

/// A fresh item record in `state` for `kind`, describing a local file of
/// `size`/`mtime_unix_ns`, with a single-component vv for `device`.
pub fn item_record(
    kind: Kind,
    state: ItemState,
    size: u64,
    mtime_unix_ns: i64,
    device: &DeviceId,
) -> ItemRecord {
    ItemRecord {
        kind,
        state,
        size,
        mtime_unix_ns,
        blake3: None,
        sem_hash: None,
        vv: [(device.clone(), 1u32)].into_iter().collect(),
        content_id: None,
        w: None,
        h: None,
        pinned: false,
        last_access_unix: 0,
        verified_remote: false,
        attested: false,
        base_unknown: false,
    }
}

/// Inserts `relkey` as a `dirty` item describing the file at `source`,
/// then admits it (`dirty → queued`) — the state [`upload_item`] expects.
///
/// [`upload_item`]: rrcloud_core::transfer::upload_item
pub fn seed_queued(db: &SyncDb, relkey: &RelKey, kind: Kind, source: &Path) {
    let size = std::fs::metadata(source).expect("metadata").len();
    let fresh = db
        .insert_item(
            relkey,
            &item_record(
                kind,
                ItemState::Dirty,
                size,
                mtime_unix_ns(source),
                db.device_id(),
            ),
        )
        .expect("insert item");
    assert!(fresh, "seed_queued must create the item");
    db.transition(relkey, ItemState::Dirty, ItemState::Queued, |_| {})
        .expect("dirty -> queued");
}

/// Inserts `relkey` as a `pending_down` item whose record carries the
/// expected `blake3`/`size`/`mtime` of `bytes` (what the journal head
/// would have advertised).
pub fn seed_pending_down(db: &SyncDb, relkey: &RelKey, kind: Kind, bytes: &[u8], mtime_unix: i64) {
    let mut record = item_record(
        kind,
        ItemState::PendingDown,
        bytes.len() as u64,
        mtime_unix * 1_000_000_000,
        db.device_id(),
    );
    record.blake3 = Some(b3(bytes));
    let fresh = db.insert_item(relkey, &record).expect("insert item");
    assert!(fresh, "seed_pending_down must create the item");
}

/// Walks the item through successive legal transitions.
pub fn advance(db: &SyncDb, relkey: &RelKey, states: &[ItemState]) {
    for &to in states {
        let from = db
            .get_item(relkey)
            .expect("get item")
            .expect("item exists")
            .state;
        db.transition(relkey, from, to, |_| {})
            .unwrap_or_else(|e| panic!("transition {from:?} -> {to:?}: {e:?}"));
    }
}

/// The item's current state.
pub fn state_of(db: &SyncDb, relkey: &RelKey) -> ItemState {
    db.get_item(relkey)
        .expect("get item")
        .expect("item exists")
        .state
}

// ---------------------------------------------------------------------------
// Tampering chunk source (§2.4 streamed-hash truth)
// ---------------------------------------------------------------------------

/// A [`ChunkSource`] that reads the real file but XORs `mask` into the
/// byte at absolute file offset `offset` — so the engine's hashes (and
/// the bytes it sends) differ from the file by construction, on every
/// read including resume re-hashes.
pub struct XorSource {
    pub offset: u64,
    pub mask: u8,
}

impl ChunkSource for XorSource {
    async fn open(&self, path: &Path, start: u64) -> Result<SourceStream, std::io::Error> {
        let mut bytes = std::fs::read(path)?;
        if (self.offset as usize) < bytes.len() {
            bytes[self.offset as usize] ^= self.mask;
        }
        let tail: Vec<u8> = bytes
            .get(start as usize..)
            .map(|t| t.to_vec())
            .unwrap_or_default();
        let chunks: Vec<Result<Bytes, std::io::Error>> = tail
            .chunks(64 * 1024)
            .map(|c| Ok(Bytes::copy_from_slice(c)))
            .collect();
        Ok(futures::stream::iter(chunks).boxed())
    }
}

// ---------------------------------------------------------------------------
// Counting / fault-injecting S3 wrapper
// ---------------------------------------------------------------------------

/// A counting, fault-injecting [`S3TransferApi`] wrapper around the real
/// client. Configure the plain-field knobs before sharing a reference;
/// the recording fields use interior mutability.
pub struct CountingS3 {
    pub inner: S3Client,

    // -- recording ---------------------------------------------------------
    /// Every attempted `put_object` key, in call order.
    pub put_keys: Mutex<Vec<String>>,
    /// Every `get_object` call: `(key, range)`, in call order.
    pub get_requests: Mutex<Vec<(String, Option<ByteRange>)>>,
    /// Every attempted `upload_part` call: `(key, part_number)`.
    pub part_calls: Mutex<Vec<(String, u32)>>,
    pub create_calls: AtomicU32,
    pub complete_calls: AtomicU32,
    pub abort_calls: AtomicU32,
    pub list_parts_calls: AtomicU32,
    pub list_uploads_calls: AtomicU32,
    /// Concurrency gauge for transfer operations (put/get/upload_part).
    /// A GET's guard lives inside the returned body stream, so streaming
    /// consumption counts as in-flight (not just the header exchange).
    gauge: Arc<InFlightGauge>,

    // -- fault injection ---------------------------------------------------
    /// `put_object` of these keys fails (typed, without reaching the
    /// backend).
    pub fail_puts: HashSet<String>,
    /// `(key, part_number)` → remaining times `upload_part` fails
    /// transport-style without reaching the backend.
    pub fail_parts: Mutex<HashMap<(String, u32), u32>>,
    /// `(key, part_number)` → remaining times the part's **bytes** are
    /// corrupted while the engine's `Content-MD5` is forwarded unchanged
    /// (the server must digest-reject). `u32::MAX` ≈ always.
    pub corrupt_parts: Mutex<HashMap<(String, u32), u32>>,
    /// Model a backend that performs no digest verification: every
    /// `Content-MD5` (PUT and part) is stripped before forwarding.
    pub strip_digests: bool,
    /// `put_object` bodies for these keys are corrupted (last byte
    /// flipped) before forwarding — pair with `strip_digests` to land
    /// wrong bytes.
    pub corrupt_put_bodies: HashSet<String>,
    /// One-shot: the next `get_object` of this key yields exactly N body
    /// bytes and then a stream error (modeling a cut connection that
    /// leaves a partial on disk).
    pub cut_get_after: Mutex<HashMap<String, u64>>,
    /// Model a backend/intermediary that ignores the `Range` header:
    /// every GET is forwarded rangeless and the response carries no
    /// `Content-Range` (a plain 200 with the full object).
    pub ignore_range: bool,
    /// `key` → ETag `head_object` reports instead of the backend's — a
    /// deterministic stand-in for "the key no longer holds the object we
    /// stored" (e.g. a sibling device replaced the shared key between
    /// Complete and the verify HEAD).
    pub fake_head_etags: Mutex<HashMap<String, String>>,
    /// `key` → remaining times `head_object` fails transport-style
    /// without reaching the backend (a transient verify-HEAD failure).
    pub fail_heads: Mutex<HashMap<String, u32>>,
    /// `key` → remaining times `abort_multipart_upload` fails
    /// transport-style without reaching the backend.
    pub fail_aborts: Mutex<HashMap<String, u32>>,
    /// Hook run at every `put_object` entry (e.g. to fire a cancel flag
    /// deterministically mid-pump).
    #[allow(clippy::type_complexity)]
    pub on_put: Mutex<Option<Box<dyn FnMut(&str) + Send>>>,
    /// When non-zero: the FIRST `put_object` to enter holds (async, with
    /// a 10s safety cap) until this many transfers are in flight at once
    /// — makes a `max_in_flight == N` assertion deterministic instead of
    /// timing-dependent (a pump silently degraded to serial admission
    /// never reaches N, trips the cap, and fails the assertion).
    pub gate_first_put_until: AtomicU32,
}

/// Shared in-flight gauge; `Arc`-owned so a GET's guard can live inside
/// the returned (`'static`) body stream.
#[derive(Default)]
pub struct InFlightGauge {
    in_flight: AtomicU32,
    max_in_flight: AtomicU32,
}

impl InFlightGauge {
    fn enter(self: &Arc<Self>) -> InFlightGuard {
        let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_in_flight.fetch_max(now, Ordering::SeqCst);
        InFlightGuard(Arc::clone(self))
    }
}

pub struct InFlightGuard(Arc<InFlightGauge>);

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.0.in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}

impl CountingS3 {
    pub fn new(inner: S3Client) -> Self {
        CountingS3 {
            inner,
            put_keys: Mutex::new(Vec::new()),
            get_requests: Mutex::new(Vec::new()),
            part_calls: Mutex::new(Vec::new()),
            create_calls: AtomicU32::new(0),
            complete_calls: AtomicU32::new(0),
            abort_calls: AtomicU32::new(0),
            list_parts_calls: AtomicU32::new(0),
            list_uploads_calls: AtomicU32::new(0),
            gauge: Arc::new(InFlightGauge::default()),
            fail_puts: HashSet::new(),
            fail_parts: Mutex::new(HashMap::new()),
            corrupt_parts: Mutex::new(HashMap::new()),
            strip_digests: false,
            corrupt_put_bodies: HashSet::new(),
            cut_get_after: Mutex::new(HashMap::new()),
            ignore_range: false,
            fake_head_etags: Mutex::new(HashMap::new()),
            fail_heads: Mutex::new(HashMap::new()),
            fail_aborts: Mutex::new(HashMap::new()),
            on_put: Mutex::new(None),
            gate_first_put_until: AtomicU32::new(0),
        }
    }

    pub fn put_keys(&self) -> Vec<String> {
        self.put_keys.lock().expect("lock").clone()
    }

    pub fn get_requests(&self) -> Vec<(String, Option<ByteRange>)> {
        self.get_requests.lock().expect("lock").clone()
    }

    /// Ranges of every `get_object` of `key`, in call order.
    pub fn get_ranges_for(&self, key: &str) -> Vec<Option<ByteRange>> {
        self.get_requests()
            .into_iter()
            .filter(|(k, _)| k == key)
            .map(|(_, r)| r)
            .collect()
    }

    pub fn part_calls(&self) -> Vec<(String, u32)> {
        self.part_calls.lock().expect("lock").clone()
    }

    /// Part numbers of every attempted `upload_part` for `key`, in call
    /// order.
    pub fn part_attempts_for(&self, key: &str) -> Vec<u32> {
        self.part_calls()
            .into_iter()
            .filter(|(k, _)| k == key)
            .map(|(_, n)| n)
            .collect()
    }

    pub fn max_in_flight(&self) -> u32 {
        self.gauge.max_in_flight.load(Ordering::SeqCst)
    }

    fn enter(&self) -> InFlightGuard {
        self.gauge.enter()
    }

    /// The `gate_first_put_until` hold (see the field doc): called with
    /// the guard already held, after which the first gated entrant waits
    /// for the target concurrency (10s cap — on a degraded pump the cap
    /// elapses and the caller's `== N` assertion fails).
    async fn await_put_gate(&self) {
        let target = self.gate_first_put_until.swap(0, Ordering::SeqCst);
        if target == 0 {
            return;
        }
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while self.gauge.in_flight.load(Ordering::SeqCst) < target
            && std::time::Instant::now() < deadline
        {
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    }

    /// Takes a pending fault count from `map` for `(key, part)`.
    fn take_count(map: &Mutex<HashMap<(String, u32), u32>>, key: &str, part: u32) -> bool {
        let mut map = map.lock().expect("lock");
        match map.get_mut(&(key.to_string(), part)) {
            Some(0) | None => false,
            Some(n) => {
                if *n != u32::MAX {
                    *n -= 1;
                }
                true
            }
        }
    }

    /// Takes a pending fault count from a per-key `map`.
    fn take_key_count(map: &Mutex<HashMap<String, u32>>, key: &str) -> bool {
        let mut map = map.lock().expect("lock");
        match map.get_mut(key) {
            Some(0) | None => false,
            Some(n) => {
                if *n != u32::MAX {
                    *n -= 1;
                }
                true
            }
        }
    }
}

fn flip_last(bytes: &mut [u8]) {
    if let Some(last) = bytes.last_mut() {
        *last ^= 0x5A;
    }
}

impl S3Api for CountingS3 {
    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: Bytes,
        opts: &PutObjectOptions,
    ) -> Result<PutObjectOutput, S3Error> {
        if let Some(hook) = self.on_put.lock().expect("lock").as_mut() {
            hook(key);
        }
        self.put_keys.lock().expect("lock").push(key.to_string());
        if self.fail_puts.contains(key) {
            return Err(S3Error::InvalidRequest(format!(
                "injected PUT failure for {key}"
            )));
        }
        let mut opts = opts.clone();
        if self.strip_digests {
            opts.content_md5 = None;
        }
        let body = if self.corrupt_put_bodies.contains(key) {
            let mut v = body.to_vec();
            flip_last(&mut v);
            Bytes::from(v)
        } else {
            body
        };
        let _g = self.enter();
        self.await_put_gate().await;
        self.inner.put_object(bucket, key, body, &opts).await
    }

    async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<GetObjectOutput, S3Error> {
        self.get_requests
            .lock()
            .expect("lock")
            .push((key.to_string(), range));
        let cut = self.cut_get_after.lock().expect("lock").remove(key);
        // The guard rides inside the returned body stream: a GET is
        // in-flight until its bytes are consumed (or the stream dropped),
        // not merely until the response headers arrive — otherwise a
        // max_in_flight assertion over downloads would be vacuous.
        let guard = self.enter();
        let effective_range = if self.ignore_range { None } else { range };
        let mut out = self.inner.get_object(bucket, key, effective_range).await?;
        if self.ignore_range {
            out.content_range = None;
        }
        if let Some(limit) = cut {
            let body = out.body;
            let stream = futures::stream::unfold(
                (body, limit, false),
                |(mut body, mut left, done)| async move {
                    if done {
                        return None;
                    }
                    if left == 0 {
                        return Some((
                            Err(S3Error::InvalidRequest(
                                "injected mid-stream cut".to_string(),
                            )),
                            (body, 0, true),
                        ));
                    }
                    match body.next().await {
                        Some(Ok(chunk)) => {
                            let take = (chunk.len() as u64).min(left) as usize;
                            left -= take as u64;
                            Some((Ok(chunk.slice(0..take)), (body, left, false)))
                        }
                        Some(Err(e)) => Some((Err(e), (body, 0, true))),
                        None => None,
                    }
                },
            );
            out.body = ByteStream::new(stream.boxed());
        }
        let body = out.body;
        out.body = ByteStream::new(
            body.map(move |item| {
                let _held = &guard;
                item
            })
            .boxed(),
        );
        Ok(out)
    }

    async fn head_object(&self, bucket: &str, key: &str) -> Result<HeadObjectOutput, S3Error> {
        if Self::take_key_count(&self.fail_heads, key) {
            return Err(S3Error::InvalidRequest(format!(
                "injected HEAD failure for {key}"
            )));
        }
        let mut out = self.inner.head_object(bucket, key).await?;
        if let Some(etag) = self.fake_head_etags.lock().expect("lock").get(key) {
            out.e_tag = etag.clone();
        }
        Ok(out)
    }

    async fn list_objects_v2(
        &self,
        bucket: &str,
        request: &ListObjectsV2Request,
    ) -> Result<ListObjectsV2Output, S3Error> {
        self.inner.list_objects_v2(bucket, request).await
    }
}

impl S3TransferApi for CountingS3 {
    async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), S3Error> {
        self.inner.delete_object(bucket, key).await
    }

    async fn create_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        opts: &PutObjectOptions,
    ) -> Result<CreateMultipartUploadOutput, S3Error> {
        self.create_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.create_multipart_upload(bucket, key, opts).await
    }

    async fn upload_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: u32,
        body: PartBody,
        content_md5: Option<&str>,
    ) -> Result<UploadPartOutput, S3Error> {
        self.part_calls
            .lock()
            .expect("lock")
            .push((key.to_string(), part_number));
        if Self::take_count(&self.fail_parts, key, part_number) {
            return Err(S3Error::InvalidRequest(format!(
                "injected part failure for {key} part {part_number}"
            )));
        }
        let content_md5 = if self.strip_digests {
            None
        } else {
            content_md5
        };
        let corrupt = Self::take_count(&self.corrupt_parts, key, part_number);
        let _g = self.enter();
        if corrupt {
            let (mut stream, _len) = body.into_stream();
            let mut buf: Vec<u8> = Vec::new();
            while let Some(chunk) = stream
                .try_next()
                .await
                .map_err(|e| S3Error::InvalidRequest(format!("part body io: {e}")))?
            {
                buf.extend_from_slice(&chunk);
            }
            flip_last(&mut buf);
            self.inner
                .upload_part(
                    bucket,
                    key,
                    upload_id,
                    part_number,
                    PartBody::from(Bytes::from(buf)),
                    content_md5,
                )
                .await
        } else {
            self.inner
                .upload_part(bucket, key, upload_id, part_number, body, content_md5)
                .await
        }
    }

    async fn complete_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: &[CompletedPart],
    ) -> Result<CompleteMultipartUploadOutput, S3Error> {
        self.complete_calls.fetch_add(1, Ordering::SeqCst);
        self.inner
            .complete_multipart_upload(bucket, key, upload_id, parts)
            .await
    }

    async fn abort_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<(), S3Error> {
        self.abort_calls.fetch_add(1, Ordering::SeqCst);
        if Self::take_key_count(&self.fail_aborts, key) {
            return Err(S3Error::InvalidRequest(format!(
                "injected abort failure for {key}"
            )));
        }
        self.inner
            .abort_multipart_upload(bucket, key, upload_id)
            .await
    }

    async fn list_multipart_uploads(
        &self,
        bucket: &str,
        request: &ListMultipartUploadsRequest,
    ) -> Result<ListMultipartUploadsOutput, S3Error> {
        self.list_uploads_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.list_multipart_uploads(bucket, request).await
    }

    async fn list_parts(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        request: &ListPartsRequest,
    ) -> Result<ListPartsOutput, S3Error> {
        self.list_parts_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.list_parts(bucket, key, upload_id, request).await
    }
}

/// All in-progress multipart uploads in `bucket` (single page is enough
/// for test-scale buckets).
pub async fn backend_uploads(client: &S3Client, bucket: &str) -> Vec<(String, String)> {
    client
        .list_multipart_uploads(bucket, &ListMultipartUploadsRequest::default())
        .await
        .expect("list_multipart_uploads")
        .uploads
        .into_iter()
        .map(|u| (u.key, u.upload_id))
        .collect()
}

/// GETs the whole object through the raw client.
pub async fn get_bytes(client: &S3Client, bucket: &str, key: &str) -> Vec<u8> {
    client
        .get_object(bucket, key, None)
        .await
        .expect("get_object")
        .body
        .collect()
        .await
        .expect("collect body")
        .to_vec()
}

/// Convenience: a file path under `root` for `relkey`-ish name.
pub fn under(root: &Path, name: &str) -> PathBuf {
    root.join(name)
}
