//! Thin, typed S3 client with hand-rolled SigV4 signing (architecture §3.9).
//!
//! Design constraints (normative, from `docs/ARCHITECTURE.md` §3.9 and §2.4):
//!
//! - **No AWS SDK crates.** Signing is implemented directly over `reqwest`
//!   (rustls TLS) using `hmac` + `sha2` in [`sigv4`].
//! - **Path-style addressing is forced**: the bucket is always the first path
//!   segment (`http://host:port/bucket/key`), never a subdomain. This is what
//!   Garage/MinIO self-hosted deployments expect.
//! - The region is configurable (the test harness and homelab deployment use
//!   `"garage"`), and the endpoint is an arbitrary `http(s)://host:port`.
//! - Small/buffered request bodies are signed with a real payload hash
//!   (`x-amz-content-sha256` = hex SHA-256); streamed multipart parts use
//!   `UNSIGNED-PAYLOAD`, with end-to-end integrity carried by a `Content-MD5`
//!   header that the server verifies (the §2.4 integrity backbone).
//! - Errors are typed: [`S3Error`] exposes the HTTP status and the S3 error
//!   code parsed from the XML error body, so callers can match on digest
//!   rejection (`BadDigest`/`InvalidDigest`), `NoSuchKey`, etc.
//!
//! Empirical conformance notes against Garage v2.2.0 (probed by hand; the
//! integration suite re-verifies them):
//!
//! - A wrong `Content-MD5` on `PutObject` *and* `UploadPart` is rejected with
//!   HTTP 400 and error code **`InvalidDigest`** (AWS S3 uses `BadDigest` for
//!   a well-formed-but-mismatched MD5; Garage uses `InvalidDigest` for both
//!   malformed and mismatched). [`S3ErrorCode::is_digest_rejection`] treats
//!   both codes as the same probe outcome.
//! - `HEAD` responses carry no XML body, so a missing key surfaces only as a
//!   bare 404; the client synthesizes [`S3ErrorCode::NoSuchKey`] for
//!   `head_object` from the status code. A missing *bucket* is
//!   indistinguishable on `HEAD` (same body-less 404) and also reports
//!   `NoSuchKey`; use `get_object`/`list_objects_v2` when the distinction
//!   matters.
//! - Ranged GET supports `bytes=a-b`, `bytes=a-` and suffix `bytes=-n` forms,
//!   returning 206 + `Content-Range`.
//! - Error XML is `<Error><Code>..</Code><Message>..</Message><Resource>..`
//!   `</Resource><Region>..</Region></Error>` (no `RequestId` on Garage).

pub mod client;
pub mod error;
pub mod multipart;
pub mod sigv4;
pub mod xml;

pub use client::{
    ByteRange, ByteStream, CopyObjectOutput, GetObjectOutput, HeadObjectOutput,
    ListObjectsV2Output, ListObjectsV2Request, ObjectSummary, PutObjectOptions, PutObjectOutput,
    S3Client, S3Config,
};
pub use error::{S3Error, S3ErrorCode};

/// The subset of [`S3Client`] operations the sync engine's journal,
/// manifest, and device-registry lanes use, lifted into a trait so tests
/// can interpose thin wrappers — a call counter that pins the §2.2
/// steady-state polling cost ("exactly one `ListObjectsV2` page when
/// nothing changed"), and fault injectors that pin the §2.1.5 publish
/// ordering under PUT failure — without a mock HTTP layer. Production code
/// passes the concrete [`S3Client`], whose impl is pure delegation.
///
/// Static dispatch only (no `dyn`): callers are generic over
/// `&impl S3Api`.
#[allow(async_fn_in_trait)] // engine-internal seam; no dyn dispatch, no cross-task Send bound needed
pub trait S3Api {
    /// [`S3Client::put_object`].
    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: bytes::Bytes,
        opts: &PutObjectOptions,
    ) -> Result<PutObjectOutput, S3Error>;

    /// [`S3Client::get_object`].
    async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<GetObjectOutput, S3Error>;

    /// [`S3Client::head_object`].
    async fn head_object(&self, bucket: &str, key: &str) -> Result<HeadObjectOutput, S3Error>;

    /// [`S3Client::list_objects_v2`].
    async fn list_objects_v2(
        &self,
        bucket: &str,
        request: &ListObjectsV2Request,
    ) -> Result<ListObjectsV2Output, S3Error>;
}

impl S3Api for S3Client {
    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: bytes::Bytes,
        opts: &PutObjectOptions,
    ) -> Result<PutObjectOutput, S3Error> {
        S3Client::put_object(self, bucket, key, body, opts).await
    }

    async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<GetObjectOutput, S3Error> {
        S3Client::get_object(self, bucket, key, range).await
    }

    async fn head_object(&self, bucket: &str, key: &str) -> Result<HeadObjectOutput, S3Error> {
        S3Client::head_object(self, bucket, key).await
    }

    async fn list_objects_v2(
        &self,
        bucket: &str,
        request: &ListObjectsV2Request,
    ) -> Result<ListObjectsV2Output, S3Error> {
        S3Client::list_objects_v2(self, bucket, request).await
    }
}

impl<T: S3Api> S3Api for &T {
    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: bytes::Bytes,
        opts: &PutObjectOptions,
    ) -> Result<PutObjectOutput, S3Error> {
        T::put_object(self, bucket, key, body, opts).await
    }

    async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<GetObjectOutput, S3Error> {
        T::get_object(self, bucket, key, range).await
    }

    async fn head_object(&self, bucket: &str, key: &str) -> Result<HeadObjectOutput, S3Error> {
        T::head_object(self, bucket, key).await
    }

    async fn list_objects_v2(
        &self,
        bucket: &str,
        request: &ListObjectsV2Request,
    ) -> Result<ListObjectsV2Output, S3Error> {
        T::list_objects_v2(self, bucket, request).await
    }
}
pub use multipart::{
    CompleteMultipartUploadOutput, CompletedPart, CreateMultipartUploadOutput,
    ListMultipartUploadsOutput, ListMultipartUploadsRequest, ListPartsOutput, ListPartsRequest,
    MultipartUploadSummary, PartBody, PartSummary, UploadPartOutput,
};
