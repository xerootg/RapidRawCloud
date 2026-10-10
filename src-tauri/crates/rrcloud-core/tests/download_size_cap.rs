//! Download size cap (§3.5 hostile-input hardening): the download engine
//! must stop streaming as soon as the body exceeds the size the
//! journal/manifest advertised, instead of accepting an arbitrarily long
//! body and trusting the final blake3 check alone.
//!
//! Threat model pinned here: the advertised `size`/`blake3` come from a
//! peer-controlled journal record and the body length from the server.
//! A hostile pair can advertise a tiny size while serving a huge body
//! whose blake3 matches (the peer simply hashes the big body), so the
//! only thing that is wrong is `size`. Today the engine streams the
//! whole body to the `.rr.part` (filling the disk before any check),
//! verifies the hash — which matches — and INSTALLS the oversized file
//! with the record's size set to the advertised value; for
//! `Kind::Sidecar` it additionally `tokio::fs::read`s the whole body into
//! memory for parse-validation (phone OOM).
//!
//! Measurement: the engine is driven against the shared Garage through a
//! thin [`S3TransferApi`] wrapper whose `get_object` body stream counts
//! every byte the engine pulls. This is the least invasive, most robust
//! place to measure (the existing `CountingS3` fault injector already
//! interposes at this seam): no TCP forwarder, no timing, and it does
//! not depend on whether the engine deletes or keeps the `.rr.part` on
//! failure — the leftover partial is checked as a secondary signal only.

mod common;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use common::garage;
use common::sync::{dev, open_db, rel, DEV_A};
use common::transfer as h;
use futures::StreamExt as _;
use rrcloud_core::journal::Kind;
use rrcloud_core::keys::{library_key, sidecar_key, RelKey};
use rrcloud_core::s3::{
    ByteRange, ByteStream, CompleteMultipartUploadOutput, CompletedPart,
    CreateMultipartUploadOutput, GetObjectOutput, HeadObjectOutput, ListMultipartUploadsOutput,
    ListMultipartUploadsRequest, ListObjectsV2Output, ListObjectsV2Request, ListPartsOutput,
    ListPartsRequest, PartBody, PutObjectOptions, PutObjectOutput, S3Api, S3Client, S3Error,
    S3TransferApi, UploadPartOutput,
};
use rrcloud_core::state::ItemState;
use rrcloud_core::transfer::{
    download_item, local_target_path, partial_path, ExpectedDownload, TransferError,
};

/// The body a hostile server actually streams.
const BODY_LEN: usize = 4 * 1024 * 1024;
/// The size the hostile peer's journal record advertises.
const ADVERTISED: u64 = 64 * 1024;
/// How far past the advertised size the engine may legitimately read
/// before noticing (one or a few transport chunks). Generous on purpose:
/// a conforming engine stops within a chunk of 64 KiB; the buggy engine
/// reads all 4 MiB, far beyond this.
const OVERREAD_SLACK: u64 = 1024 * 1024;

// ---------------------------------------------------------------------------
// Byte-counting S3 wrapper
// ---------------------------------------------------------------------------

/// Pure delegation to the real client, except that every `get_object`
/// body is wrapped so `body_bytes` counts the bytes the ENGINE pulled out
/// of the stream (not what the server offered).
struct BodyCountingS3 {
    inner: S3Client,
    body_bytes: Arc<AtomicU64>,
    get_calls: AtomicU64,
}

impl BodyCountingS3 {
    fn new(inner: S3Client) -> Self {
        Self {
            inner,
            body_bytes: Arc::new(AtomicU64::new(0)),
            get_calls: AtomicU64::new(0),
        }
    }

    fn body_bytes(&self) -> u64 {
        self.body_bytes.load(Ordering::SeqCst)
    }
}

impl S3Api for BodyCountingS3 {
    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: Bytes,
        opts: &PutObjectOptions,
    ) -> Result<PutObjectOutput, S3Error> {
        self.inner.put_object(bucket, key, body, opts).await
    }

    async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<GetObjectOutput, S3Error> {
        self.get_calls.fetch_add(1, Ordering::SeqCst);
        let mut out = self.inner.get_object(bucket, key, range).await?;
        let counter = Arc::clone(&self.body_bytes);
        let body = out.body;
        out.body = ByteStream::new(
            body.map(move |item| {
                if let Ok(chunk) = &item {
                    counter.fetch_add(chunk.len() as u64, Ordering::SeqCst);
                }
                item
            })
            .boxed(),
        );
        Ok(out)
    }

    async fn head_object(&self, bucket: &str, key: &str) -> Result<HeadObjectOutput, S3Error> {
        self.inner.head_object(bucket, key).await
    }

    async fn list_objects_v2(
        &self,
        bucket: &str,
        request: &ListObjectsV2Request,
    ) -> Result<ListObjectsV2Output, S3Error> {
        self.inner.list_objects_v2(bucket, request).await
    }
}

impl S3TransferApi for BodyCountingS3 {
    async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), S3Error> {
        self.inner.delete_object(bucket, key).await
    }

    async fn create_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        opts: &PutObjectOptions,
    ) -> Result<CreateMultipartUploadOutput, S3Error> {
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
        self.inner
            .upload_part(bucket, key, upload_id, part_number, body, content_md5)
            .await
    }

    async fn complete_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: &[CompletedPart],
    ) -> Result<CompleteMultipartUploadOutput, S3Error> {
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
        self.inner
            .abort_multipart_upload(bucket, key, upload_id)
            .await
    }

    async fn list_multipart_uploads(
        &self,
        bucket: &str,
        request: &ListMultipartUploadsRequest,
    ) -> Result<ListMultipartUploadsOutput, S3Error> {
        self.inner.list_multipart_uploads(bucket, request).await
    }

    async fn list_parts(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        request: &ListPartsRequest,
    ) -> Result<ListPartsOutput, S3Error> {
        self.inner.list_parts(bucket, key, upload_id, request).await
    }
}

// ---------------------------------------------------------------------------
// Driver
// ---------------------------------------------------------------------------

/// Everything observable about one oversized-body download attempt.
struct Observed {
    result: Result<u64, TransferError>,
    /// Body bytes the engine pulled out of the GET stream.
    streamed: u64,
    final_len: Option<u64>,
    partial_len: Option<u64>,
    state: ItemState,
}

/// Uploads `body` to Garage under `relkey`/`kind`, seeds a `pending_down`
/// record, and drives `download_item` with an [`ExpectedDownload`] whose
/// `blake3` is the TRUE hash of `body` but whose `size` is `advertised`
/// (only the size is wrong — exactly what a hostile peer+server can
/// arrange).
async fn drive_oversized_download(
    tag: &str,
    relkey: &RelKey,
    kind: Kind,
    body: Vec<u8>,
    advertised: u64,
) -> Option<Observed> {
    let g = garage::shared()?;
    let bucket = g.create_unique_bucket(tag);
    let root = tempfile::tempdir().expect("tempdir");
    let (_dbdir, _dbpath, db) = open_db(&dev(DEV_A));
    let cfg = h::test_cfg(&bucket, root.path());
    let key = match kind {
        Kind::Sidecar => sidecar_key(relkey),
        _ => library_key(relkey),
    };
    g.client()
        .put_object(
            &bucket,
            &key,
            Bytes::from(body.clone()),
            &PutObjectOptions::default(),
        )
        .await
        .expect("put oversized body");

    let mtime = 1_700_000_900i64;
    // The record (what the peer's journal advertised): tiny size, but the
    // true hash of the big body.
    let mut record = h::item_record(
        kind,
        ItemState::PendingDown,
        advertised,
        mtime * 1_000_000_000,
        db.device_id(),
    );
    record.blake3 = Some(h::b3(&body));
    assert!(db.insert_item(relkey, &record).expect("insert item"));
    let expected = ExpectedDownload {
        blake3: h::b3(&body),
        size: advertised,
        mtime_unix: mtime,
    };

    let s3 = BodyCountingS3::new(g.client());
    let result = download_item(&db, &s3, &cfg, relkey, root.path(), &expected)
        .await
        .map(|o| o.bytes_fetched);

    let final_path = local_target_path(root.path(), relkey, kind);
    let partial = partial_path(&final_path);
    let len = |p: &std::path::Path| std::fs::metadata(p).ok().map(|m| m.len());
    Some(Observed {
        result,
        streamed: s3.body_bytes(),
        final_len: len(&final_path),
        partial_len: len(&partial),
        state: h::state_of(&db, relkey),
    })
}

/// A rejection that is ABOUT the size overflow: the dedicated S3 body
/// cap, the remote-object-is-corrupt lane, or any (possibly new) typed
/// variant whose message names the size/cap/advertised overflow. A bare
/// `IntegrityMismatch` is deliberately NOT accepted: the hash here is the
/// true hash of the body, so a mismatch would only mean the engine hashed
/// a truncated prefix by accident rather than rejecting the size.
fn is_size_rejection(err: &TransferError) -> bool {
    match err {
        TransferError::S3(S3Error::BodyCapExceeded { .. }) => true,
        TransferError::CorruptRemote { .. } => true,
        TransferError::Io { .. }
        | TransferError::State(_)
        | TransferError::MissingItem { .. }
        | TransferError::IntegrityMismatch { .. } => false,
        other => {
            let msg = other.to_string().to_ascii_lowercase();
            ["size", "exceed", "cap", "advertis", "too large", "oversiz"]
                .iter()
                .any(|needle| msg.contains(needle))
        }
    }
}

fn assert_capped(kind: Kind, obs: Observed) {
    let Observed {
        result,
        streamed,
        final_len,
        partial_len,
        state,
    } = obs;
    let outcome = match &result {
        Ok(fetched) => format!("Ok(bytes_fetched={fetched}) — INSTALLED the oversized object"),
        Err(e) => format!("Err({e})"),
    };
    let summary = format!(
        "{kind:?}: advertised size = {ADVERTISED} bytes, server body = {BODY_LEN} bytes; \
         engine pulled {streamed} bytes from the GET stream; result = {outcome}; \
         final file = {final_len:?} bytes; .rr.part = {partial_len:?} bytes; state = {state:?}"
    );
    println!("{summary}");

    // The key proof: the engine must stop reading shortly after the
    // advertised size, never anywhere near the hostile body length.
    assert!(
        streamed <= ADVERTISED + OVERREAD_SLACK,
        "engine kept streaming past the advertised size: read {streamed} bytes \
         (allowed at most {} = advertised {ADVERTISED} + slack {OVERREAD_SLACK}) of a \
         {BODY_LEN}-byte body — a hostile server fills the disk before any check. {summary}",
        ADVERTISED + OVERREAD_SLACK
    );
    match &result {
        Ok(_) => panic!("oversized download must fail, not install. {summary}"),
        Err(e) => assert!(
            is_size_rejection(e),
            "oversized download must fail with a size-related typed error, got {e:?}. {summary}"
        ),
    }
    assert_eq!(final_len, None, "nothing may be installed. {summary}");
    if let Some(len) = partial_len {
        assert!(
            len <= ADVERTISED + OVERREAD_SLACK,
            "a surviving .rr.part must be bounded by the advertised size. {summary}"
        );
    }
    assert!(
        !matches!(state, ItemState::Hydrated | ItemState::Synced),
        "item must not land in a terminal success state. {summary}"
    );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// An original whose journal record advertises 64 KiB while the server
/// streams 4 MiB with a matching blake3: the engine must abort the
/// stream as soon as the advertised size is exceeded.
#[tokio::test]
async fn original_download_aborts_once_body_exceeds_advertised_size() {
    let body = h::patterned(BODY_LEN, 42);
    let Some(obs) = drive_oversized_download(
        "tr-dl-sizecap-orig",
        &rel("cap/huge.NEF"),
        Kind::Original,
        body,
        ADVERTISED,
    )
    .await
    else {
        return; // no Garage binary: harness skip
    };
    assert_capped(Kind::Original, obs);
}

/// A sidecar (downloaded eagerly, never stubbed) whose record advertises
/// 64 KiB while the server streams a 4 MiB body that is a VALID sidecar
/// (the fixture padded with trailing whitespace, so both the hash and the
/// parse would pass). Today the whole 4 MiB is streamed to disk, then
/// `tokio::fs::read` into memory for `sem_hash`, then installed. The
/// engine must reject on size before the body is read fully.
#[tokio::test]
async fn sidecar_download_aborts_once_body_exceeds_advertised_size() {
    let mut body = std::fs::read(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/sidecar_full.json"
    ))
    .expect("fixture");
    body.resize(BODY_LEN, b' '); // trailing whitespace keeps it valid JSON
    assert!(
        rrcloud_core::semhash::sem_hash(&body).is_ok(),
        "padded fixture must still parse as a valid sidecar"
    );
    let Some(obs) = drive_oversized_download(
        "tr-dl-sizecap-sidecar",
        &rel("cap/huge.NEF"),
        Kind::Sidecar,
        body,
        ADVERTISED,
    )
    .await
    else {
        return; // no Garage binary: harness skip
    };
    assert_capped(Kind::Sidecar, obs);
}
