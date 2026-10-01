//! The typed S3 client: configuration, object operations, and streaming
//! bodies. Multipart operations live in [`crate::s3::multipart`] but are
//! methods on [`S3Client`] too.

use std::collections::BTreeMap;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures::stream::BoxStream;
use futures::{Stream, StreamExt as _, TryStreamExt as _};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::Method;

use super::error::S3Error;
use super::multipart::{
    CompleteMultipartUploadOutput, CompletedPart, CreateMultipartUploadOutput,
    ListMultipartUploadsOutput, ListMultipartUploadsRequest, ListPartsOutput, ListPartsRequest,
    MultipartUploadSummary, PartBody, PartBodyInner, PartSummary, UploadPartOutput,
};
use super::{sigv4, xml};

/// Upper bound on pages [`S3Client::list_all_objects`] will follow. At the
/// S3 default of 1000 keys per page this covers 100M objects — far beyond
/// any library sync root — while bounding a backend that never stops
/// truncating (see the stealth-200 / captive-portal defenses elsewhere in
/// this module).
pub const MAX_LIST_PAGES: u32 = 100_000;

/// Connection settings for an S3-compatible endpoint.
///
/// Addressing is always path-style (`<endpoint>/<bucket>/<key>`); the
/// endpoint can be any `http://` or `https://` host:port (Garage, MinIO,
/// B2, R2, AWS).
#[derive(Clone)]
pub struct S3Config {
    /// Base endpoint, e.g. `http://127.0.0.1:3900` or
    /// `https://s3.eu-central-003.backblazeb2.com`. No trailing slash
    /// required; a path component is not supported and is rejected by
    /// [`S3Client::new`].
    pub endpoint: String,
    /// SigV4 signing region. Garage deployments use `"garage"`.
    pub region: String,
    /// Access key id.
    pub access_key_id: String,
    /// Secret access key. Redacted from the [`std::fmt::Debug`] output so a
    /// logged config/client cannot leak the credential into log files.
    pub secret_access_key: String,
    /// TCP connect timeout. `None` (the default) means no limit beyond the
    /// OS's own.
    pub connect_timeout: Option<Duration>,
    /// Idle-read timeout: the longest the socket may go without producing a
    /// single byte before the request fails with a transport error. This is
    /// the knob that unsticks a silently dead TCP connection (mobile
    /// networks, §2.4) without capping the total duration of a large
    /// streamed transfer. `None` (the default) means no limit.
    pub read_timeout: Option<Duration>,
    /// Whole-request deadline, covering connect through the end of the
    /// response body. A streamed GET body is included, so for large
    /// transfers prefer `read_timeout` plus an external deadline in the
    /// transfer layer. `None` (the default) means no limit: deadline
    /// enforcement is then entirely the transfer layer's responsibility.
    pub request_timeout: Option<Duration>,
}

/// Hand-written so `{:?}` (routine in tracing/anyhow error context) never
/// prints the secret access key. Fixes [`S3Client`]'s derived `Debug`
/// transitively.
impl std::fmt::Debug for S3Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Config")
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"<redacted>")
            .field("connect_timeout", &self.connect_timeout)
            .field("read_timeout", &self.read_timeout)
            .field("request_timeout", &self.request_timeout)
            .finish()
    }
}

/// Options for `put_object` / `create_multipart_upload`.
#[derive(Debug, Clone, Default)]
pub struct PutObjectOptions {
    /// Base64-encoded MD5 of the body, sent as `Content-MD5` so the server
    /// verifies the received bytes (architecture §2.4). `None` omits the
    /// header.
    pub content_md5: Option<String>,
    /// `Content-Type` to store with the object.
    pub content_type: Option<String>,
    /// User metadata, sent/returned as `x-amz-meta-<key>` headers. Keys here
    /// are the bare suffix (e.g. `"rr-blake3"`), lowercase, without the
    /// `x-amz-meta-` prefix.
    ///
    /// NOTE: header values (metadata values and `content_type`) are
    /// whitespace-sanitized before sending so the signed value is
    /// byte-identical to the wire value: surrounding whitespace is trimmed
    /// and every internal run of whitespace collapses to one space — even
    /// inside quoted strings, where strict SigV4 would preserve it (e.g.
    /// `boundary="a  b"` is stored as `boundary="a b"`). Avoid values that
    /// depend on exact interior whitespace.
    pub metadata: BTreeMap<String, String>,
}

/// Result of a `PutObject`.
#[derive(Debug, Clone)]
pub struct PutObjectOutput {
    /// ETag returned by the server, surrounding quotes stripped. For a
    /// simple PUT this is the hex MD5 of the body on every S3
    /// implementation tested (Garage v2.2.0 included).
    pub e_tag: String,
}

/// Byte range for a ranged `GetObject` (all forms verified against Garage
/// v2.2.0, which answers 206 + `Content-Range` for each).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ByteRange {
    /// `bytes=<first>-` — from `first` to the end.
    From(u64),
    /// `bytes=<first>-<last>` — inclusive on both ends.
    Bounded(u64, u64),
    /// `bytes=-<n>` — the final `n` bytes.
    Suffix(u64),
}

impl ByteRange {
    /// Renders the `Range` header value (e.g. `bytes=100-199`).
    pub fn to_header_value(self) -> String {
        match self {
            ByteRange::From(first) => format!("bytes={first}-"),
            ByteRange::Bounded(first, last) => format!("bytes={first}-{last}"),
            ByteRange::Suffix(n) => format!("bytes=-{n}"),
        }
    }
}

/// A streaming response body.
///
/// Implements [`Stream`] of [`Bytes`] chunks; use [`ByteStream::collect`]
/// to buffer the whole body (tests and small objects).
pub struct ByteStream {
    inner: BoxStream<'static, Result<Bytes, S3Error>>,
}

impl ByteStream {
    /// Wraps an existing chunk stream.
    pub fn new(inner: BoxStream<'static, Result<Bytes, S3Error>>) -> Self {
        Self { inner }
    }

    /// Buffers the remainder of the stream into one contiguous [`Bytes`].
    pub async fn collect(mut self) -> Result<Bytes, S3Error> {
        let mut buf = BytesMut::new();
        while let Some(chunk) = self.inner.try_next().await? {
            buf.extend_from_slice(&chunk);
        }
        Ok(buf.freeze())
    }
}

impl Stream for ByteStream {
    type Item = Result<Bytes, S3Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

impl std::fmt::Debug for ByteStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ByteStream").finish_non_exhaustive()
    }
}

/// Result of a `GetObject`.
#[derive(Debug)]
pub struct GetObjectOutput {
    /// ETag, quotes stripped.
    pub e_tag: String,
    /// Length of *this response's* body (for a ranged GET, the range
    /// length, not the full object size).
    pub content_length: u64,
    /// `Content-Range` header for ranged requests (e.g.
    /// `bytes 100-199/1024`).
    pub content_range: Option<String>,
    /// `Content-Type` stored with the object, when present.
    pub content_type: Option<String>,
    /// User metadata (`x-amz-meta-*` echoed back, prefix stripped,
    /// lowercase keys).
    pub metadata: BTreeMap<String, String>,
    /// The streaming body.
    pub body: ByteStream,
}

/// Result of a `HeadObject`.
#[derive(Debug, Clone)]
pub struct HeadObjectOutput {
    /// ETag, quotes stripped.
    pub e_tag: String,
    /// Full object size in bytes.
    pub content_length: u64,
    /// `Content-Type` stored with the object, when present.
    pub content_type: Option<String>,
    /// User metadata (`x-amz-meta-*`, prefix stripped, lowercase keys).
    pub metadata: BTreeMap<String, String>,
    /// `Last-Modified` header, verbatim.
    pub last_modified: Option<String>,
}

/// Result of a `CopyObject`.
#[derive(Debug, Clone)]
pub struct CopyObjectOutput {
    /// ETag of the new (destination) object, quotes stripped.
    pub e_tag: String,
}

/// Parameters for one `ListObjectsV2` page request.
#[derive(Debug, Clone, Default)]
pub struct ListObjectsV2Request {
    /// Restrict the listing to keys beginning with this prefix.
    pub prefix: Option<String>,
    /// Group keys by this delimiter into `common_prefixes`.
    pub delimiter: Option<String>,
    /// Page size cap (`max-keys`).
    pub max_keys: Option<u32>,
    /// Token from the previous page's `next_continuation_token`.
    pub continuation_token: Option<String>,
    /// Start listing after this key (`start-after`).
    pub start_after: Option<String>,
}

/// One object entry from a listing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectSummary {
    /// Full object key.
    pub key: String,
    /// Object size in bytes.
    pub size: u64,
    /// ETag, quotes stripped. Empty when the listing entry carried no
    /// `<ETag>` element (the schema permits omitting it; not observed on
    /// Garage v2.2.0) — treat `""` as "unknown", never compare it as a
    /// real ETag during reconciliation.
    pub e_tag: String,
    /// `LastModified` timestamp, verbatim from the XML.
    pub last_modified: Option<String>,
}

/// One `ListObjectsV2` page.
#[derive(Debug, Clone, Default)]
pub struct ListObjectsV2Output {
    /// Objects on this page, in lexicographic key order.
    pub objects: Vec<ObjectSummary>,
    /// Grouped prefixes (only when a delimiter was given).
    pub common_prefixes: Vec<String>,
    /// Whether more pages follow.
    pub is_truncated: bool,
    /// Token for the next page when `is_truncated`.
    pub next_continuation_token: Option<String>,
}

/// The S3 client. Cheap to clone is *not* promised; share it behind an `Arc`
/// if needed. All methods are async and safe to call concurrently.
#[derive(Debug, Clone)]
pub struct S3Client {
    config: S3Config,
    http: reqwest::Client,
    /// `endpoint` without any trailing slash, URL-validated in [`Self::new`].
    endpoint_base: String,
    /// The exact `Host` header value the HTTP layer will send (host, plus
    /// `:port` when the port is not the scheme default) — signed verbatim.
    host_header: String,
}

impl S3Client {
    /// Builds a client for `config`.
    ///
    /// Fails if the underlying HTTP client cannot be constructed (TLS
    /// backend initialization), the endpoint cannot be parsed, or the
    /// endpoint carries a path/query/fragment component (the signer builds
    /// `/bucket/key` paths directly under the host, so a base path would
    /// de-sync the signed and sent paths — it is rejected up front instead
    /// of failing every request with an opaque signature error).
    pub fn new(config: S3Config) -> Result<Self, S3Error> {
        let endpoint_base = config.endpoint.trim_end_matches('/').to_string();
        let url = reqwest::Url::parse(&endpoint_base).map_err(|e| {
            S3Error::InvalidRequest(format!("unparsable endpoint {endpoint_base:?}: {e}"))
        })?;
        let host = url.host_str().ok_or_else(|| {
            S3Error::InvalidRequest(format!("endpoint {endpoint_base:?} has no host"))
        })?;
        if url.path() != "/" || url.query().is_some() || url.fragment().is_some() {
            return Err(S3Error::InvalidRequest(format!(
                "endpoint {endpoint_base:?} has a path/query/fragment component, which is \
                 not supported: use a bare http(s)://host[:port]"
            )));
        }
        // `Url::port()` is `None` for the scheme default port, matching the
        // `Host` header the HTTP layer sends.
        let host_header = match url.port() {
            Some(port) => format!("{host}:{port}"),
            None => host.to_string(),
        };
        // SigV4 signs the exact host and path; following a redirect would
        // replay the original Authorization against a different host/path
        // and fail with a baffling signature error, so 3xx responses are
        // surfaced as typed API errors instead.
        let mut builder = reqwest::Client::builder().redirect(reqwest::redirect::Policy::none());
        if let Some(timeout) = config.connect_timeout {
            builder = builder.connect_timeout(timeout);
        }
        if let Some(timeout) = config.read_timeout {
            builder = builder.read_timeout(timeout);
        }
        if let Some(timeout) = config.request_timeout {
            builder = builder.timeout(timeout);
        }
        let http = builder.build()?;
        Ok(Self {
            config,
            http,
            endpoint_base,
            host_header,
        })
    }

    /// The configuration this client was built with.
    pub fn config(&self) -> &S3Config {
        &self.config
    }

    /// Returns a clone of the inner HTTP client (for advanced callers; used
    /// by the transfer layer to share connection pools).
    pub fn http_client(&self) -> reqwest::Client {
        self.http.clone()
    }

    /// Signs one request and returns the final URL (path + query exactly as
    /// canonicalized) plus the headers to send (everything signed except
    /// `host`, which the HTTP layer adds itself, plus `Authorization`).
    ///
    /// `extra_headers` are both *sent and signed* verbatim, so values must
    /// already be in canonical whitespace form (see [`sanitize_header_value`]).
    fn sign_request(
        &self,
        method: &Method,
        path: &str,
        query: &[(String, String)],
        extra_headers: &[(String, String)],
        payload_hash: &str,
    ) -> Result<(reqwest::Url, HeaderMap), S3Error> {
        // '.' and '..' path segments are legal S3 key content, but the URL
        // layer (reqwest::Url, per the WHATWG spec) removes dot segments, so
        // the path actually sent would diverge from the path signed and the
        // request could only ever fail with a misleading AccessDenied — or,
        // on a backend that normalizes before signature verification,
        // silently address the WRONG key. Fail fast and typed instead.
        if path
            .split('/')
            .any(|segment| segment == "." || segment == "..")
        {
            return Err(S3Error::InvalidRequest(format!(
                "unsupported key: path {path:?} contains a '.' or '..' segment, which \
                 HTTP URL normalization would rewrite; such keys cannot be addressed \
                 by this client"
            )));
        }

        let amz_date = sigv4::format_amz_date(time::OffsetDateTime::now_utc());
        let date = &amz_date[..8];

        let canonical_uri = sigv4::canonical_uri(path);
        let query_refs: Vec<(&str, &str)> = query
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect();
        let canonical_query = sigv4::canonical_query_string(&query_refs);

        let mut to_sign: Vec<(&str, &str)> = vec![
            ("host", self.host_header.as_str()),
            ("x-amz-content-sha256", payload_hash),
            ("x-amz-date", amz_date.as_str()),
        ];
        for (name, value) in extra_headers {
            to_sign.push((name.as_str(), value.as_str()));
        }
        let headers = sigv4::canonical_headers(&to_sign);

        let canonical_request = sigv4::canonical_request(
            method.as_str(),
            &canonical_uri,
            &canonical_query,
            &headers,
            payload_hash,
        );
        let scope = sigv4::credential_scope(date, &self.config.region, sigv4::SERVICE);
        let string_to_sign = sigv4::string_to_sign(&amz_date, &scope, &canonical_request);
        let signing_key = sigv4::derive_signing_key(
            &self.config.secret_access_key,
            date,
            &self.config.region,
            sigv4::SERVICE,
        );
        let signature = sigv4::signature_hex(&signing_key, &string_to_sign);
        let authorization = sigv4::authorization_header(
            &self.config.access_key_id,
            &scope,
            &headers.signed_headers,
            &signature,
        );

        let url_str = if canonical_query.is_empty() {
            format!("{}{}", self.endpoint_base, canonical_uri)
        } else {
            format!(
                "{}{}?{}",
                self.endpoint_base, canonical_uri, canonical_query
            )
        };
        let url = reqwest::Url::parse(&url_str).map_err(|e| {
            S3Error::InvalidRequest(format!("unparsable request URL {url_str:?}: {e}"))
        })?;
        // Defense in depth: the signature covers `canonical_uri` verbatim,
        // so if URL parsing rewrote the path in any way the request is
        // unsendable (the dot-segment check above makes this unreachable).
        if url.path() != canonical_uri {
            return Err(S3Error::InvalidRequest(format!(
                "URL normalization rewrote the request path from {canonical_uri:?} to \
                 {:?}; refusing to send a request whose path differs from the signed one",
                url.path()
            )));
        }

        let mut map = HeaderMap::with_capacity(extra_headers.len() + 3);
        insert_header(&mut map, "x-amz-date", &amz_date)?;
        insert_header(&mut map, "x-amz-content-sha256", payload_hash)?;
        insert_header(&mut map, "authorization", &authorization)?;
        for (name, value) in extra_headers {
            insert_header(&mut map, name, value)?;
        }
        Ok((url, map))
    }

    /// Passes through 2xx responses; everything else is read and turned into
    /// a typed [`S3Error::Api`] (synthesized from the status when the error
    /// body is absent, as on `HEAD`).
    async fn ensure_success(resp: reqwest::Response) -> Result<reqwest::Response, S3Error> {
        if resp.status().is_success() {
            return Ok(resp);
        }
        let status = resp.status().as_u16();
        let body = resp.bytes().await.unwrap_or_default();
        Err(xml::parse_error_response(status, &body))
    }

    /// `PutObject`: uploads `body` (buffered; use multipart for anything
    /// large) with a **signed payload** (`x-amz-content-sha256` = real
    /// SHA-256). Sends `Content-MD5` and `x-amz-meta-*` per `opts`.
    pub async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: Bytes,
        opts: &PutObjectOptions,
    ) -> Result<PutObjectOutput, S3Error> {
        let payload_hash = sigv4::sha256_hex(&body);
        let extra = put_option_headers(opts, /* include_md5: */ true)?;
        let (url, headers) = self.sign_request(
            &Method::PUT,
            &object_path(bucket, key),
            &[],
            &extra,
            &payload_hash,
        )?;
        let resp = self
            .http
            .put(url)
            .headers(headers)
            .body(body)
            .send()
            .await?;
        let resp = Self::ensure_success(resp).await?;
        Ok(PutObjectOutput {
            e_tag: required_etag(resp.headers())?,
        })
    }

    /// `GetObject`, optionally ranged. The body is streamed, not buffered.
    pub async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<GetObjectOutput, S3Error> {
        let mut extra: Vec<(String, String)> = Vec::new();
        if let Some(range) = range {
            extra.push(("range".to_string(), range.to_header_value()));
        }
        let (url, headers) = self.sign_request(
            &Method::GET,
            &object_path(bucket, key),
            &[],
            &extra,
            sigv4::EMPTY_PAYLOAD_SHA256,
        )?;
        let resp = self.http.get(url).headers(headers).send().await?;
        let resp = Self::ensure_success(resp).await?;

        let e_tag = required_etag(resp.headers())?;
        let content_length = required_content_length(resp.headers())?;
        let content_range = header_string(resp.headers(), "content-range");
        let content_type = header_string(resp.headers(), "content-type");
        let metadata = user_metadata(resp.headers());
        let body = ByteStream::new(
            resp.bytes_stream()
                .map(|chunk| chunk.map_err(S3Error::from))
                .boxed(),
        );
        Ok(GetObjectOutput {
            e_tag,
            content_length,
            content_range,
            content_type,
            metadata,
            body,
        })
    }

    /// `HeadObject`. HEAD responses have no XML body, so missing keys are
    /// surfaced as [`S3Error::Api`] with a code synthesized from the HTTP
    /// status (404 → `NoSuchKey`; Garage v2.2.0 verified to answer bare
    /// 404s here). A missing *bucket* cannot be told apart from a missing
    /// key on HEAD (same body-less 404) and also reports `NoSuchKey`; use
    /// `get_object` when the distinction matters.
    pub async fn head_object(&self, bucket: &str, key: &str) -> Result<HeadObjectOutput, S3Error> {
        let (url, headers) = self.sign_request(
            &Method::HEAD,
            &object_path(bucket, key),
            &[],
            &[],
            sigv4::EMPTY_PAYLOAD_SHA256,
        )?;
        let resp = self.http.head(url).headers(headers).send().await?;
        let resp = Self::ensure_success(resp).await?;
        Ok(HeadObjectOutput {
            e_tag: required_etag(resp.headers())?,
            content_length: required_content_length(resp.headers())?,
            content_type: header_string(resp.headers(), "content-type"),
            metadata: user_metadata(resp.headers()),
            last_modified: header_string(resp.headers(), "last-modified"),
        })
    }

    /// `DeleteObject`. Deleting a nonexistent key is a success (S3
    /// semantics: 204 either way).
    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), S3Error> {
        let (url, headers) = self.sign_request(
            &Method::DELETE,
            &object_path(bucket, key),
            &[],
            &[],
            sigv4::EMPTY_PAYLOAD_SHA256,
        )?;
        let resp = self.http.delete(url).headers(headers).send().await?;
        Self::ensure_success(resp).await?;
        Ok(())
    }

    /// `CopyObject`: server-side copy of `src_bucket/src_key` to
    /// `dest_bucket/dest_key` via `x-amz-copy-source`.
    pub async fn copy_object(
        &self,
        src_bucket: &str,
        src_key: &str,
        dest_bucket: &str,
        dest_key: &str,
    ) -> Result<CopyObjectOutput, S3Error> {
        let copy_source = sigv4::uri_encode(&object_path(src_bucket, src_key), false);
        let extra = vec![("x-amz-copy-source".to_string(), copy_source)];
        let (url, headers) = self.sign_request(
            &Method::PUT,
            &object_path(dest_bucket, dest_key),
            &[],
            &extra,
            sigv4::EMPTY_PAYLOAD_SHA256,
        )?;
        let resp = self.http.put(url).headers(headers).send().await?;
        let resp = Self::ensure_success(resp).await?;
        let status = resp.status().as_u16();
        let body = resp.bytes().await?;
        let result: xml::CopyObjectResult = parse_xml_body(status, &body)?;
        Ok(CopyObjectOutput {
            e_tag: xml::strip_etag_quotes(&result.e_tag),
        })
    }

    /// One page of `ListObjectsV2`.
    ///
    /// `encoding-type=url` is always requested: keys may legally contain
    /// characters that are invalid in XML 1.0 (e.g. control characters),
    /// which AWS would otherwise emit raw and make the document unparsable.
    /// Keys and common prefixes are decoded client-side when the response
    /// echoes `<EncodingType>url</EncodingType>` (a backend that ignores the
    /// parameter returns raw keys, which are passed through unchanged).
    pub async fn list_objects_v2(
        &self,
        bucket: &str,
        request: &ListObjectsV2Request,
    ) -> Result<ListObjectsV2Output, S3Error> {
        let mut query: Vec<(String, String)> = vec![
            ("list-type".to_string(), "2".to_string()),
            ("encoding-type".to_string(), "url".to_string()),
        ];
        if let Some(token) = &request.continuation_token {
            query.push(("continuation-token".to_string(), token.clone()));
        }
        if let Some(delimiter) = &request.delimiter {
            query.push(("delimiter".to_string(), delimiter.clone()));
        }
        if let Some(max_keys) = request.max_keys {
            query.push(("max-keys".to_string(), max_keys.to_string()));
        }
        if let Some(prefix) = &request.prefix {
            query.push(("prefix".to_string(), prefix.clone()));
        }
        if let Some(start_after) = &request.start_after {
            query.push(("start-after".to_string(), start_after.clone()));
        }
        let (url, headers) = self.sign_request(
            &Method::GET,
            &bucket_path(bucket),
            &query,
            &[],
            sigv4::EMPTY_PAYLOAD_SHA256,
        )?;
        let resp = self.http.get(url).headers(headers).send().await?;
        let resp = Self::ensure_success(resp).await?;
        let status = resp.status().as_u16();
        let body = resp.bytes().await?;
        let result: xml::ListBucketResult = parse_xml_body(status, &body)?;
        let url_encoded = result.encoding_type.as_deref() == Some("url");
        let decode = |s: String| -> Result<String, S3Error> {
            if url_encoded {
                xml::url_decode_key(&s)
            } else {
                Ok(s)
            }
        };
        let mut objects = Vec::with_capacity(result.contents.len());
        for entry in result.contents {
            objects.push(ObjectSummary {
                key: decode(entry.key)?,
                size: entry.size,
                e_tag: entry
                    .e_tag
                    .as_deref()
                    .map(xml::strip_etag_quotes)
                    .unwrap_or_default(),
                last_modified: entry.last_modified,
            });
        }
        let mut common_prefixes = Vec::with_capacity(result.common_prefixes.len());
        for entry in result.common_prefixes {
            common_prefixes.push(decode(entry.prefix)?);
        }
        Ok(ListObjectsV2Output {
            objects,
            common_prefixes,
            is_truncated: result.is_truncated,
            next_continuation_token: result.next_continuation_token,
        })
    }

    /// Paging helper: follows continuation tokens until exhaustion and
    /// returns every object under `prefix` (no delimiter grouping), in
    /// lexicographic key order.
    ///
    /// Capped at [`MAX_LIST_PAGES`] pages: a backend (or middlebox) that
    /// keeps answering truncated pages with a non-advancing token must not
    /// loop forever and grow the result without bound; the cap surfaces as
    /// [`S3Error::InvalidResponse`].
    pub async fn list_all_objects(
        &self,
        bucket: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<ObjectSummary>, S3Error> {
        let mut all = Vec::new();
        let mut continuation_token: Option<String> = None;
        let mut pages: u32 = 0;
        loop {
            if pages >= MAX_LIST_PAGES {
                return Err(S3Error::InvalidResponse(format!(
                    "ListObjectsV2 still truncated after {MAX_LIST_PAGES} pages; \
                     refusing a runaway paging loop"
                )));
            }
            pages += 1;
            let page = self
                .list_objects_v2(
                    bucket,
                    &ListObjectsV2Request {
                        prefix: prefix.map(str::to_string),
                        continuation_token: continuation_token.take(),
                        ..Default::default()
                    },
                )
                .await?;
            all.extend(page.objects);
            if !page.is_truncated {
                return Ok(all);
            }
            match page.next_continuation_token {
                Some(token) => continuation_token = Some(token),
                None => {
                    return Err(S3Error::InvalidResponse(
                        "truncated ListObjectsV2 page without a NextContinuationToken".to_string(),
                    ))
                }
            }
        }
    }

    /// `CreateMultipartUpload`. `opts.content_md5` is ignored here (MD5s are
    /// per-part); `content_type` and `metadata` apply to the final object.
    pub async fn create_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        opts: &PutObjectOptions,
    ) -> Result<CreateMultipartUploadOutput, S3Error> {
        let query = vec![("uploads".to_string(), String::new())];
        let extra = put_option_headers(opts, /* include_md5: */ false)?;
        let (url, headers) = self.sign_request(
            &Method::POST,
            &object_path(bucket, key),
            &query,
            &extra,
            sigv4::EMPTY_PAYLOAD_SHA256,
        )?;
        let resp = self.http.post(url).headers(headers).send().await?;
        let resp = Self::ensure_success(resp).await?;
        let status = resp.status().as_u16();
        let body = resp.bytes().await?;
        let result: xml::InitiateMultipartUploadResult = parse_xml_body(status, &body)?;
        Ok(CreateMultipartUploadOutput {
            upload_id: result.upload_id,
        })
    }

    /// `UploadPart`. Streamed bodies are signed with `UNSIGNED-PAYLOAD`;
    /// integrity is carried by `content_md5` (base64), which the server
    /// verifies against the received bytes and rejects with a digest error
    /// on mismatch (Garage v2.2.0: HTTP 400 `InvalidDigest`; AWS:
    /// `BadDigest`). `part_number` starts at 1.
    pub async fn upload_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: u32,
        body: PartBody,
        content_md5: Option<&str>,
    ) -> Result<UploadPartOutput, S3Error> {
        let query = vec![
            ("partNumber".to_string(), part_number.to_string()),
            ("uploadId".to_string(), upload_id.to_string()),
        ];
        let mut extra: Vec<(String, String)> = Vec::new();
        if let Some(md5) = content_md5 {
            extra.push(("content-md5".to_string(), md5.to_string()));
        }

        let path = object_path(bucket, key);
        let resp = match body.inner {
            // Buffered part: sign the real payload hash.
            PartBodyInner::Bytes(bytes) => {
                let payload_hash = sigv4::sha256_hex(&bytes);
                let (url, headers) =
                    self.sign_request(&Method::PUT, &path, &query, &extra, &payload_hash)?;
                self.http
                    .put(url)
                    .headers(headers)
                    .body(bytes)
                    .send()
                    .await?
            }
            // Streamed part: UNSIGNED-PAYLOAD, explicit Content-Length
            // (mandatory for UploadPart), integrity carried by Content-MD5.
            PartBodyInner::Stream {
                stream,
                content_length,
            } => {
                let (url, mut headers) = self.sign_request(
                    &Method::PUT,
                    &path,
                    &query,
                    &extra,
                    sigv4::UNSIGNED_PAYLOAD,
                )?;
                headers.insert(
                    reqwest::header::CONTENT_LENGTH,
                    HeaderValue::from(content_length),
                );
                self.http
                    .put(url)
                    .headers(headers)
                    .body(reqwest::Body::wrap_stream(stream))
                    .send()
                    .await?
            }
        };
        let resp = Self::ensure_success(resp).await?;
        Ok(UploadPartOutput {
            e_tag: required_etag(resp.headers())?,
        })
    }

    /// `CompleteMultipartUpload` with the recorded part list (ascending
    /// `part_number`).
    pub async fn complete_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: &[CompletedPart],
    ) -> Result<CompleteMultipartUploadOutput, S3Error> {
        let request = xml::CompleteMultipartUploadRequest {
            parts: parts
                .iter()
                .map(|part| xml::CompletePartRequest {
                    part_number: part.part_number,
                    e_tag: quote_etag(&part.e_tag),
                })
                .collect(),
        };
        let body = quick_xml::se::to_string(&request).map_err(|e| {
            S3Error::InvalidRequest(format!("serializing CompleteMultipartUpload: {e}"))
        })?;
        let query = vec![("uploadId".to_string(), upload_id.to_string())];
        let payload_hash = sigv4::sha256_hex(body.as_bytes());
        let (url, headers) = self.sign_request(
            &Method::POST,
            &object_path(bucket, key),
            &query,
            &[],
            &payload_hash,
        )?;
        let resp = self
            .http
            .post(url)
            .headers(headers)
            .body(body)
            .send()
            .await?;
        let resp = Self::ensure_success(resp).await?;
        let status = resp.status().as_u16();
        let body = resp.bytes().await?;
        // AWS can answer 200 with an <Error> body on Complete; parse_xml_body
        // surfaces that as a typed API error, and anything else that is not a
        // CompleteMultipartUploadResult (empty body, truncated XML from a cut
        // connection, non-XML) as an InvalidResponse.
        let result: xml::CompleteMultipartUploadResult = parse_xml_body(status, &body)?;
        Ok(CompleteMultipartUploadOutput {
            e_tag: xml::strip_etag_quotes(&result.e_tag),
            key: result.key,
        })
    }

    /// `AbortMultipartUpload`.
    pub async fn abort_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<(), S3Error> {
        let query = vec![("uploadId".to_string(), upload_id.to_string())];
        let (url, headers) = self.sign_request(
            &Method::DELETE,
            &object_path(bucket, key),
            &query,
            &[],
            sigv4::EMPTY_PAYLOAD_SHA256,
        )?;
        let resp = self.http.delete(url).headers(headers).send().await?;
        Self::ensure_success(resp).await?;
        Ok(())
    }

    /// `ListMultipartUploads` (in-progress uploads for `bucket`). Used by
    /// the worker's stale-upload sweep (§2.4). Truncated pages are continued
    /// by passing `next_key_marker` / `next_upload_id_marker` back through
    /// `request` (verified against Garage v2.2.0, which pages on the key
    /// marker and omits `NextUploadIdMarker`).
    ///
    /// `encoding-type=url` is always requested, for the same reason as on
    /// [`Self::list_objects_v2`]: upload keys may contain characters that are
    /// invalid in XML 1.0. Keys and `NextKeyMarker` are decoded client-side
    /// when the response echoes `<EncodingType>url</EncodingType>`, so the
    /// decoded marker can be passed straight back as `key_marker`.
    pub async fn list_multipart_uploads(
        &self,
        bucket: &str,
        request: &ListMultipartUploadsRequest,
    ) -> Result<ListMultipartUploadsOutput, S3Error> {
        let mut query = vec![
            ("uploads".to_string(), String::new()),
            ("encoding-type".to_string(), "url".to_string()),
        ];
        if let Some(key_marker) = &request.key_marker {
            query.push(("key-marker".to_string(), key_marker.clone()));
        }
        if let Some(max_uploads) = request.max_uploads {
            query.push(("max-uploads".to_string(), max_uploads.to_string()));
        }
        if let Some(prefix) = &request.prefix {
            query.push(("prefix".to_string(), prefix.clone()));
        }
        if let Some(upload_id_marker) = &request.upload_id_marker {
            query.push(("upload-id-marker".to_string(), upload_id_marker.clone()));
        }
        let (url, headers) = self.sign_request(
            &Method::GET,
            &bucket_path(bucket),
            &query,
            &[],
            sigv4::EMPTY_PAYLOAD_SHA256,
        )?;
        let resp = self.http.get(url).headers(headers).send().await?;
        let resp = Self::ensure_success(resp).await?;
        let status = resp.status().as_u16();
        let body = resp.bytes().await?;
        let result: xml::ListMultipartUploadsResult = parse_xml_body(status, &body)?;
        let url_encoded = result.encoding_type.as_deref() == Some("url");
        let decode = |s: String| -> Result<String, S3Error> {
            if url_encoded {
                xml::url_decode_key(&s)
            } else {
                Ok(s)
            }
        };
        let mut uploads = Vec::with_capacity(result.uploads.len());
        for upload in result.uploads {
            uploads.push(MultipartUploadSummary {
                key: decode(upload.key)?,
                upload_id: upload.upload_id,
                initiated: upload.initiated,
            });
        }
        let next_key_marker = result.next_key_marker.map(decode).transpose()?;
        Ok(ListMultipartUploadsOutput {
            uploads,
            is_truncated: result.is_truncated,
            next_key_marker,
            next_upload_id_marker: result.next_upload_id_marker,
        })
    }

    /// `ListParts` for a live multipart upload (resume support, §2.4).
    /// Truncated pages (uploads with more than `max-parts`/1000 parts) are
    /// continued by passing `next_part_number_marker` back through
    /// `request`.
    ///
    /// `encoding-type=url` is always requested: the response's `<Key>` echo
    /// (ignored by this client, but part of the document) may contain
    /// characters invalid in XML 1.0, which a backend would otherwise emit
    /// raw and make the whole document unparsable.
    pub async fn list_parts(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        request: &ListPartsRequest,
    ) -> Result<ListPartsOutput, S3Error> {
        let mut query = vec![
            ("uploadId".to_string(), upload_id.to_string()),
            ("encoding-type".to_string(), "url".to_string()),
        ];
        if let Some(max_parts) = request.max_parts {
            query.push(("max-parts".to_string(), max_parts.to_string()));
        }
        if let Some(marker) = request.part_number_marker {
            query.push(("part-number-marker".to_string(), marker.to_string()));
        }
        let (url, headers) = self.sign_request(
            &Method::GET,
            &object_path(bucket, key),
            &query,
            &[],
            sigv4::EMPTY_PAYLOAD_SHA256,
        )?;
        let resp = self.http.get(url).headers(headers).send().await?;
        let resp = Self::ensure_success(resp).await?;
        let status = resp.status().as_u16();
        let body = resp.bytes().await?;
        let result: xml::ListPartsResult = parse_xml_body(status, &body)?;
        Ok(ListPartsOutput {
            parts: result
                .parts
                .into_iter()
                .map(|part| PartSummary {
                    part_number: part.part_number,
                    e_tag: xml::strip_etag_quotes(&part.e_tag),
                    size: part.size,
                })
                .collect(),
            is_truncated: result.is_truncated,
            next_part_number_marker: result.next_part_number_marker,
        })
    }
}

/// `/bucket/key` (unencoded; canonicalization encodes it).
fn object_path(bucket: &str, key: &str) -> String {
    format!("/{bucket}/{key}")
}

/// `/bucket` (bucket-level operations).
fn bucket_path(bucket: &str) -> String {
    format!("/{bucket}")
}

/// Headers derived from [`PutObjectOptions`], in the canonical whitespace
/// form that is both signed and sent.
///
/// Metadata keys differing only in ASCII case (e.g. `Rr-Tag` and `rr-tag`)
/// are rejected with [`S3Error::InvalidRequest`]: both would lowercase to
/// the same header name, and SigV4 would then sign the comma-joined pair
/// while the HTTP layer sends only one value — an unsignable request that
/// could otherwise only fail with a baffling remote 403
/// `SignatureDoesNotMatch` (verified against Garage v2.2.0).
fn put_option_headers(
    opts: &PutObjectOptions,
    include_md5: bool,
) -> Result<Vec<(String, String)>, S3Error> {
    let mut extra: Vec<(String, String)> = Vec::new();
    if include_md5 {
        if let Some(md5) = &opts.content_md5 {
            extra.push(("content-md5".to_string(), sanitize_header_value(md5)));
        }
    }
    if let Some(content_type) = &opts.content_type {
        extra.push((
            "content-type".to_string(),
            sanitize_header_value(content_type),
        ));
    }
    let mut seen_meta: BTreeMap<String, &String> = BTreeMap::new();
    for (meta_key, value) in &opts.metadata {
        let lowered = meta_key.to_ascii_lowercase();
        if let Some(previous) = seen_meta.insert(lowered.clone(), meta_key) {
            return Err(S3Error::InvalidRequest(format!(
                "metadata keys {previous:?} and {meta_key:?} differ only in case: both \
                 become the header x-amz-meta-{lowered}, which cannot be signed and \
                 sent consistently"
            )));
        }
        extra.push((
            format!("x-amz-meta-{lowered}"),
            sanitize_header_value(value),
        ));
    }
    Ok(extra)
}

/// Trims and collapses internal whitespace so the value sent on the wire is
/// byte-identical to its SigV4 canonical form.
///
/// Deliberately stricter than the SigV4 trimall rule, which preserves
/// whitespace inside quoted strings: collapsing unconditionally keeps
/// signed == sent with a canonicalizer that is idempotent server-side, at
/// the cost of mutating quoted runs of spaces in caller values (documented
/// on [`PutObjectOptions::metadata`]).
fn sanitize_header_value(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn insert_header(map: &mut HeaderMap, name: &str, value: &str) -> Result<(), S3Error> {
    let name = HeaderName::from_bytes(name.as_bytes())
        .map_err(|e| S3Error::InvalidRequest(format!("invalid header name {name:?}: {e}")))?;
    let value = HeaderValue::from_str(value)
        .map_err(|e| S3Error::InvalidRequest(format!("invalid value for header {name:?}: {e}")))?;
    map.insert(name, value);
    Ok(())
}

/// The `ETag` response header with quotes stripped; 2xx responses without
/// one are malformed.
fn required_etag(headers: &HeaderMap) -> Result<String, S3Error> {
    header_string(headers, "etag")
        .map(|raw| xml::strip_etag_quotes(&raw))
        .ok_or_else(|| S3Error::InvalidResponse("response is missing an ETag header".to_string()))
}

/// Parsed `Content-Length`; required on GET/HEAD responses.
fn required_content_length(headers: &HeaderMap) -> Result<u64, S3Error> {
    let raw = header_string(headers, "content-length").ok_or_else(|| {
        S3Error::InvalidResponse("response is missing a Content-Length header".to_string())
    })?;
    raw.parse::<u64>()
        .map_err(|e| S3Error::InvalidResponse(format!("unparsable Content-Length {raw:?}: {e}")))
}

fn header_string(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
}

/// `x-amz-meta-*` headers with the prefix stripped (names arrive lowercased
/// from the HTTP layer).
fn user_metadata(headers: &HeaderMap) -> BTreeMap<String, String> {
    headers
        .iter()
        .filter_map(|(name, value)| {
            let suffix = name.as_str().strip_prefix("x-amz-meta-")?;
            Some((suffix.to_string(), value.to_str().ok()?.to_string()))
        })
        .collect()
}

/// Ensures an ETag is in its quoted wire form for XML request bodies.
fn quote_etag(e_tag: &str) -> String {
    if e_tag.starts_with('"') {
        e_tag.to_string()
    } else {
        format!("\"{e_tag}\"")
    }
}

/// Deserializes a 2xx XML response body, treating a stealth `<Error>`
/// document (or garbage) as a typed/invalid response error.
fn parse_xml_body<T: serde::de::DeserializeOwned>(status: u16, body: &[u8]) -> Result<T, S3Error> {
    let text = String::from_utf8_lossy(body);
    match quick_xml::de::from_str::<T>(&text) {
        Ok(parsed) => Ok(parsed),
        Err(parse_err) => {
            if quick_xml::de::from_str::<xml::ErrorDocument>(&text).is_ok() {
                return Err(xml::parse_error_response(status, body));
            }
            Err(S3Error::InvalidResponse(format!(
                "unparsable XML response body: {parse_err}"
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(endpoint: &str) -> S3Config {
        S3Config {
            endpoint: endpoint.to_string(),
            region: "garage".to_string(),
            access_key_id: "GKtestaccesskey".to_string(),
            secret_access_key: "SUPERSECRET123".to_string(),
            connect_timeout: None,
            read_timeout: None,
            request_timeout: None,
        }
    }

    #[tokio::test]
    async fn read_timeout_unsticks_a_silently_dead_connection() {
        // A server that accepts the connection and then never produces a
        // byte models a silently dead TCP connection on a mobile network
        // (§2.4). With read_timeout configured the request must fail with a
        // timeout transport error instead of hanging a transfer slot
        // forever.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        std::thread::spawn(move || {
            // Accept and hold the socket open, reading nothing back.
            let sock = listener.accept().map(|(sock, _)| sock);
            std::thread::sleep(std::time::Duration::from_secs(20));
            drop(sock);
        });
        let mut cfg = config(&format!("http://127.0.0.1:{}", addr.port()));
        cfg.read_timeout = Some(std::time::Duration::from_millis(200));
        let client = S3Client::new(cfg).expect("client");
        let start = std::time::Instant::now();
        let err = client
            .head_object("bucket", "key")
            .await
            .expect_err("silent server must time out");
        assert!(
            matches!(&err, S3Error::Transport(e) if e.is_timeout()),
            "expected a timeout transport error, got {err:?}"
        );
        assert!(
            start.elapsed() < std::time::Duration::from_secs(10),
            "timed out far too slowly: {:?}",
            start.elapsed()
        );
    }

    #[test]
    fn debug_output_redacts_the_secret_access_key() {
        // The engine/worker will inevitably log the client or its config in
        // error context ({:?} in tracing/anyhow chains); the secret must
        // never appear there.
        let cfg = config("http://127.0.0.1:3900");
        let client = S3Client::new(cfg.clone()).expect("client");
        for rendered in [format!("{cfg:?}"), format!("{client:?}")] {
            assert!(
                !rendered.contains("SUPERSECRET123"),
                "secret leaked into Debug output: {rendered}"
            );
            assert!(
                rendered.contains("<redacted>"),
                "Debug output should show an explicit redaction marker: {rendered}"
            );
        }
        // Non-secret fields stay visible for debugging.
        let rendered = format!("{cfg:?}");
        assert!(rendered.contains("http://127.0.0.1:3900"));
        assert!(rendered.contains("GKtestaccesskey"));
    }

    #[test]
    fn endpoint_with_path_component_is_rejected() {
        // The signer builds paths as /bucket/key directly under the host; a
        // base path would silently de-sync the signed and sent paths, so it
        // must be rejected up front with a clear error.
        for bad in [
            "http://127.0.0.1:3900/garage",
            "https://s3.example.com/base/path",
        ] {
            let err = S3Client::new(config(bad)).expect_err(bad);
            assert!(matches!(err, S3Error::InvalidRequest(_)), "{bad}: {err:?}");
        }
        // A bare trailing slash is fine (it is trimmed).
        assert!(S3Client::new(config("http://127.0.0.1:3900/")).is_ok());
        assert!(S3Client::new(config("http://127.0.0.1:3900")).is_ok());
    }

    #[test]
    fn stealth_error_xml_is_not_an_empty_listing() {
        // A 200 response whose body is a well-formed <Error> document (a
        // "stealth error": SlowDown/InternalError answered with HTTP 200, or
        // a proxy rewriting the status) must surface as a typed API error,
        // never as "the bucket is empty" / "no uploads in progress" / "no
        // parts uploaded". The §2.4 stale-upload sweep and multipart resume
        // would otherwise act on phantom-empty listings.
        use crate::s3::error::S3ErrorCode;
        let stealth = "<Error><Code>SlowDown</Code><Message>please slow down</Message></Error>";

        let err = parse_xml_body::<xml::ListBucketResult>(200, stealth.as_bytes())
            .expect_err("stealth <Error> must not parse as an empty ListBucketResult");
        assert_eq!(
            err.code(),
            Some(&S3ErrorCode::Other("SlowDown".to_string())),
            "{err:?}"
        );

        let err = parse_xml_body::<xml::ListMultipartUploadsResult>(200, stealth.as_bytes())
            .expect_err("stealth <Error> must not parse as an empty ListMultipartUploadsResult");
        assert_eq!(
            err.code(),
            Some(&S3ErrorCode::Other("SlowDown".to_string())),
            "{err:?}"
        );

        let err = parse_xml_body::<xml::ListPartsResult>(200, stealth.as_bytes())
            .expect_err("stealth <Error> must not parse as an empty ListPartsResult");
        assert_eq!(
            err.code(),
            Some(&S3ErrorCode::Other("SlowDown".to_string())),
            "{err:?}"
        );
    }

    #[test]
    fn arbitrary_non_list_xml_is_not_an_empty_listing() {
        // A captive portal / misbehaving proxy answering 200 with arbitrary
        // well-formed XML (e.g. an XHTML login page) must be an
        // InvalidResponse, not an empty listing.
        let portal = "<html><body>login required</body></html>";

        let err = parse_xml_body::<xml::ListBucketResult>(200, portal.as_bytes())
            .expect_err("portal HTML must not parse as an empty ListBucketResult");
        assert!(matches!(err, S3Error::InvalidResponse(_)), "{err:?}");

        let err = parse_xml_body::<xml::ListMultipartUploadsResult>(200, portal.as_bytes())
            .expect_err("portal HTML must not parse as an empty ListMultipartUploadsResult");
        assert!(matches!(err, S3Error::InvalidResponse(_)), "{err:?}");

        let err = parse_xml_body::<xml::ListPartsResult>(200, portal.as_bytes())
            .expect_err("portal HTML must not parse as an empty ListPartsResult");
        assert!(matches!(err, S3Error::InvalidResponse(_)), "{err:?}");
    }

    #[test]
    fn genuine_list_documents_still_parse() {
        // The guard above must not break parsing of real (Garage/AWS-shaped)
        // listing documents, including genuinely empty ones, which always
        // carry <Name> (ListObjectsV2) / <Bucket> (multipart listings).
        let list = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>bkt</Name><KeyCount>1</KeyCount><IsTruncated>false</IsTruncated>
  <Contents><Key>k</Key><Size>3</Size><ETag>&quot;abc&quot;</ETag></Contents>
</ListBucketResult>"#;
        let parsed: xml::ListBucketResult =
            parse_xml_body(200, list.as_bytes()).expect("real ListBucketResult");
        assert_eq!(parsed.contents.len(), 1);
        assert!(!parsed.is_truncated);

        let empty_list = r#"<ListBucketResult><Name>bkt</Name><IsTruncated>false</IsTruncated></ListBucketResult>"#;
        let parsed: xml::ListBucketResult =
            parse_xml_body(200, empty_list.as_bytes()).expect("empty ListBucketResult");
        assert!(parsed.contents.is_empty());

        let uploads = r#"<ListMultipartUploadsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Bucket>bkt</Bucket><IsTruncated>false</IsTruncated>
  <Upload><Key>k</Key><UploadId>uid</UploadId></Upload>
</ListMultipartUploadsResult>"#;
        let parsed: xml::ListMultipartUploadsResult =
            parse_xml_body(200, uploads.as_bytes()).expect("real ListMultipartUploadsResult");
        assert_eq!(parsed.uploads.len(), 1);

        let parts = r#"<ListPartsResult><Bucket>bkt</Bucket><Key>k</Key><UploadId>uid</UploadId>
  <IsTruncated>false</IsTruncated>
  <Part><PartNumber>1</PartNumber><ETag>&quot;abc&quot;</ETag><Size>5</Size></Part>
</ListPartsResult>"#;
        let parsed: xml::ListPartsResult =
            parse_xml_body(200, parts.as_bytes()).expect("real ListPartsResult");
        assert_eq!(parsed.parts.len(), 1);
    }

    #[tokio::test]
    async fn case_colliding_metadata_keys_are_rejected_before_sending() {
        // put_option_headers lowercases metadata keys, so {"Rr-Tag", "rr-tag"}
        // would produce two x-amz-meta-rr-tag headers: SigV4 signs the
        // comma-joined pair but HeaderMap::insert sends only one, yielding a
        // baffling remote 403 (verified against Garage v2.2.0). Fail fast
        // with a typed InvalidRequest instead, before any network I/O (port
        // 9 is never connected to).
        let client = S3Client::new(config("http://127.0.0.1:9")).expect("client");
        let mut metadata = BTreeMap::new();
        metadata.insert("Rr-Tag".to_string(), "upper".to_string());
        metadata.insert("rr-tag".to_string(), "lower".to_string());
        let opts = PutObjectOptions {
            metadata,
            ..Default::default()
        };

        let err = client
            .put_object("bucket", "key", Bytes::from_static(b"x"), &opts)
            .await
            .expect_err("case-colliding metadata keys must be rejected");
        assert!(matches!(err, S3Error::InvalidRequest(_)), "{err:?}");

        let err = client
            .create_multipart_upload("bucket", "key", &opts)
            .await
            .expect_err("case-colliding metadata keys must be rejected on create too");
        assert!(matches!(err, S3Error::InvalidRequest(_)), "{err:?}");
    }

    /// One-shot fake HTTP server: accepts a single connection, reads the
    /// request, answers with `response` verbatim, and closes.
    fn one_shot_server(response: String) -> String {
        use std::io::{Read as _, Write as _};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        std::thread::spawn(move || {
            if let Ok((mut sock, _)) = listener.accept() {
                sock.set_read_timeout(Some(std::time::Duration::from_secs(5)))
                    .ok();
                // Read until the end of headers plus whatever body bytes
                // arrive in the same window; the response does not depend on
                // the request.
                let mut buf = [0u8; 16384];
                let mut seen = Vec::new();
                while let Ok(n) = sock.read(&mut buf) {
                    if n == 0 {
                        break;
                    }
                    seen.extend_from_slice(&buf[..n]);
                    if seen.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                sock.write_all(response.as_bytes()).ok();
                sock.flush().ok();
            }
        });
        format!("http://127.0.0.1:{}", addr.port())
    }

    #[tokio::test]
    async fn complete_multipart_200_with_empty_body_is_invalid_response() {
        // A 200 CompleteMultipartUpload response whose body is neither a
        // CompleteMultipartUploadResult nor an <Error> document (empty body,
        // cut connection mid-XML, non-XML) is a 2xx-but-uninterpretable
        // response: the defined meaning of S3Error::InvalidResponse. It must
        // NOT be reported as the self-contradictory
        // Api{status: 200, code: Other("Http200")}.
        let endpoint = one_shot_server("HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n".to_string());
        let client = S3Client::new(config(&endpoint)).expect("client");
        let parts = [crate::s3::multipart::CompletedPart {
            part_number: 1,
            e_tag: "abc".to_string(),
        }];
        let err = client
            .complete_multipart_upload("bucket", "key", "upload-id", &parts)
            .await
            .expect_err("empty 200 body must be an error");
        assert!(
            matches!(err, S3Error::InvalidResponse(_)),
            "expected InvalidResponse, got {err:?}"
        );
    }

    #[tokio::test]
    async fn complete_multipart_200_with_stealth_error_body_is_typed_api_error() {
        // The AWS-documented "200 with an <Error> body" case on Complete
        // must keep surfacing as a typed Api error (this pins the behavior
        // the InvalidResponse fix above must not regress).
        use crate::s3::error::S3ErrorCode;
        let body = "<Error><Code>InternalError</Code><Message>retry</Message></Error>";
        let endpoint = one_shot_server(format!(
            "HTTP/1.1 200 OK\r\ncontent-length: {}\r\n\r\n{body}",
            body.len()
        ));
        let client = S3Client::new(config(&endpoint)).expect("client");
        let parts = [crate::s3::multipart::CompletedPart {
            part_number: 1,
            e_tag: "abc".to_string(),
        }];
        let err = client
            .complete_multipart_upload("bucket", "key", "upload-id", &parts)
            .await
            .expect_err("stealth error body must be an error");
        assert_eq!(
            err.code(),
            Some(&S3ErrorCode::Other("InternalError".to_string())),
            "{err:?}"
        );
        assert_eq!(err.http_status(), Some(200));
    }

    #[tokio::test]
    async fn dot_segment_keys_are_rejected_with_typed_error() {
        // reqwest's URL layer removes '.'/'..' path segments, so a key
        // containing one can never be addressed correctly: the signed path
        // would diverge from the sent path. The client must fail fast with
        // a typed error instead of a baffling 403 from the server.
        // Port 9 (discard) is never connected to: rejection happens before
        // any network I/O.
        let client = S3Client::new(config("http://127.0.0.1:9")).expect("client");
        for key in ["a/../b", "x/./y", ".", "..", "trailing/.", "../leading"] {
            let err = client
                .head_object("bucket", key)
                .await
                .expect_err("dot-segment key must be rejected");
            assert!(
                matches!(err, S3Error::InvalidRequest(_)),
                "{key:?}: {err:?}"
            );
        }
        // Dots *inside* a segment are legal S3 key content and must pass
        // validation (the request then fails at the transport layer because
        // nothing listens on the port).
        let err = client
            .head_object("bucket", "a.b/c..d/...")
            .await
            .expect_err("no server is listening");
        assert!(matches!(err, S3Error::Transport(_)), "{err:?}");
    }
}
