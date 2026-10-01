//! Multipart upload types.
//!
//! The sync engine (architecture §2.4) persists `{upload_id}` and
//! `{part_no, etag, md5}` per uploaded part in redb so uploads survive
//! process death; these are the wire-facing types that state maps to.
//! The operations themselves are methods on [`crate::s3::S3Client`].

use bytes::Bytes;
use futures::stream::BoxStream;

/// Result of `CreateMultipartUpload`.
#[derive(Debug, Clone)]
pub struct CreateMultipartUploadOutput {
    /// Server-assigned upload id. Persisted for resume.
    pub upload_id: String,
}

/// Result of `UploadPart`.
#[derive(Debug, Clone)]
pub struct UploadPartOutput {
    /// Part ETag (quotes stripped); on Garage v2.2.0 and AWS this is the
    /// hex MD5 of the part body. Persisted and replayed verbatim into
    /// `CompleteMultipartUpload`.
    pub e_tag: String,
}

/// One entry of the part list sent to `CompleteMultipartUpload`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletedPart {
    /// 1-based part number.
    pub part_number: u32,
    /// The ETag returned by `UploadPart` (quotes optional; the client
    /// normalizes when serializing).
    pub e_tag: String,
}

/// Result of `CompleteMultipartUpload`.
#[derive(Debug, Clone)]
pub struct CompleteMultipartUploadOutput {
    /// ETag of the assembled object (multipart form `"<md5-of-md5s>-<n>"`
    /// on Garage/AWS), quotes stripped.
    pub e_tag: String,
    /// Key echoed back by the server, when present.
    pub key: Option<String>,
}

/// Parameters for one `ListMultipartUploads` page request.
#[derive(Debug, Clone, Default)]
pub struct ListMultipartUploadsRequest {
    /// Restrict the listing to keys beginning with this prefix.
    pub prefix: Option<String>,
    /// Resume after this key (`key-marker`), from the previous page's
    /// `next_key_marker`.
    pub key_marker: Option<String>,
    /// Together with `key_marker`, resume after this upload id
    /// (`upload-id-marker`), from the previous page's
    /// `next_upload_id_marker` when the server provided one (Garage v2.2.0
    /// pages correctly on `key-marker` alone and omits the upload-id
    /// marker).
    pub upload_id_marker: Option<String>,
    /// Page size cap (`max-uploads`).
    pub max_uploads: Option<u32>,
}

/// Parameters for one `ListParts` page request.
#[derive(Debug, Clone, Default)]
pub struct ListPartsRequest {
    /// Resume after this part number (`part-number-marker`), from the
    /// previous page's `next_part_number_marker`.
    pub part_number_marker: Option<u32>,
    /// Page size cap (`max-parts`).
    pub max_parts: Option<u32>,
}

/// One in-progress upload from `ListMultipartUploads`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultipartUploadSummary {
    /// Object key the upload targets.
    pub key: String,
    /// Upload id.
    pub upload_id: String,
    /// `Initiated` timestamp, verbatim from the XML.
    pub initiated: Option<String>,
}

/// Result of `ListMultipartUploads`.
#[derive(Debug, Clone, Default)]
pub struct ListMultipartUploadsOutput {
    /// In-progress uploads (key order, then initiation).
    pub uploads: Vec<MultipartUploadSummary>,
    /// Whether more pages follow. Paging is not driven automatically: the
    /// caller passes `next_key_marker` (and `next_upload_id_marker` when
    /// present) back via [`ListMultipartUploadsRequest`].
    pub is_truncated: bool,
    /// `NextKeyMarker` for the next page, when `is_truncated`.
    pub next_key_marker: Option<String>,
    /// `NextUploadIdMarker` for the next page. Garage v2.2.0 omits this and
    /// pages correctly on the key marker alone; pass it through when
    /// present.
    pub next_upload_id_marker: Option<String>,
}

/// One uploaded part from `ListParts`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartSummary {
    /// 1-based part number.
    pub part_number: u32,
    /// Part ETag, quotes stripped.
    pub e_tag: String,
    /// Part size in bytes.
    pub size: u64,
}

/// Result of `ListParts`.
#[derive(Debug, Clone, Default)]
pub struct ListPartsOutput {
    /// Parts uploaded so far, ascending part number.
    pub parts: Vec<PartSummary>,
    /// Whether more pages follow. The caller passes
    /// `next_part_number_marker` back via [`ListPartsRequest`].
    pub is_truncated: bool,
    /// `NextPartNumberMarker` for the next page, when `is_truncated`.
    pub next_part_number_marker: Option<u32>,
}

/// Body of an `UploadPart` request.
///
/// Buffered bodies are signed with a real payload hash; streamed bodies are
/// signed `UNSIGNED-PAYLOAD` (integrity carried by `Content-MD5`, §2.4/§3.9).
pub struct PartBody {
    pub(crate) inner: PartBodyInner,
}

pub(crate) enum PartBodyInner {
    /// Fully buffered part.
    Bytes(Bytes),
    /// Streamed part with a known length (`Content-Length` is mandatory for
    /// `UploadPart`).
    Stream {
        stream: BoxStream<'static, Result<Bytes, std::io::Error>>,
        content_length: u64,
    },
}

impl PartBody {
    /// A streamed part body of exactly `content_length` bytes.
    pub fn from_stream(
        stream: BoxStream<'static, Result<Bytes, std::io::Error>>,
        content_length: u64,
    ) -> Self {
        Self {
            inner: PartBodyInner::Stream {
                stream,
                content_length,
            },
        }
    }

    /// The number of bytes this body will produce.
    pub fn content_length(&self) -> u64 {
        match &self.inner {
            PartBodyInner::Bytes(b) => b.len() as u64,
            PartBodyInner::Stream { content_length, .. } => *content_length,
        }
    }

    /// Decomposes the body into a chunk stream plus its content length —
    /// the interposition seam for [`crate::s3::S3TransferApi`] test
    /// wrappers (which must be able to inspect, tamper with, or re-wrap a
    /// part body before forwarding it to a real client). A buffered body
    /// becomes a one-chunk stream.
    pub fn into_stream(self) -> (BoxStream<'static, Result<Bytes, std::io::Error>>, u64) {
        use futures::StreamExt as _;
        match self.inner {
            PartBodyInner::Bytes(b) => {
                let len = b.len() as u64;
                (futures::stream::once(async move { Ok(b) }).boxed(), len)
            }
            PartBodyInner::Stream {
                stream,
                content_length,
            } => (stream, content_length),
        }
    }
}

impl From<Bytes> for PartBody {
    fn from(bytes: Bytes) -> Self {
        Self {
            inner: PartBodyInner::Bytes(bytes),
        }
    }
}

impl std::fmt::Debug for PartBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.inner {
            PartBodyInner::Bytes(b) => f.debug_tuple("PartBody::Bytes").field(&b.len()).finish(),
            PartBodyInner::Stream { content_length, .. } => f
                .debug_struct("PartBody::Stream")
                .field("content_length", content_length)
                .finish(),
        }
    }
}
