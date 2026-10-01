//! `quick-xml`/serde wire types for S3 response bodies.
//!
//! Shapes match what Garage v2.2.0 actually emits (captured by hand against
//! the real binary) and the AWS S3 API reference. All fields the client does
//! not need are omitted; unknown elements are ignored by serde by default.
//!
//! Note on ETags: in XML bodies ETags arrive XML-escaped and quoted
//! (`&quot;abc...&quot;`); `quick-xml` unescapes to `"abc..."` and the
//! client strips the surrounding quotes before exposing them.

use serde::{Deserialize, Serialize};

use super::error::S3Error;

/// `<Error>` document returned with non-2xx statuses.
///
/// Garage emits `Code`, `Message`, `Resource`, `Region` (no `RequestId`);
/// AWS emits `Code`, `Message`, `RequestId`, sometimes `Resource`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ErrorDocument {
    /// Machine-readable error code (e.g. `NoSuchKey`, `InvalidDigest`).
    pub code: String,
    /// Human-readable message.
    #[serde(default)]
    pub message: Option<String>,
    /// Path of the resource the error refers to.
    #[serde(default)]
    pub resource: Option<String>,
    /// AWS request id (absent on Garage).
    #[serde(default)]
    pub request_id: Option<String>,
}

/// `InitiateMultipartUploadResult`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct InitiateMultipartUploadResult {
    /// Bucket echoed back.
    pub bucket: String,
    /// Key echoed back.
    pub key: String,
    /// Server-assigned upload id.
    pub upload_id: String,
}

/// `CompleteMultipartUploadResult`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct CompleteMultipartUploadResult {
    /// Key echoed back.
    #[serde(default)]
    pub key: Option<String>,
    /// Multipart ETag (`"<md5-of-md5s>-<n>"`), still quoted here.
    #[serde(rename = "ETag")]
    pub e_tag: String,
}

/// `CopyObjectResult`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct CopyObjectResult {
    /// ETag of the destination object, still quoted here.
    #[serde(rename = "ETag")]
    pub e_tag: String,
    /// Timestamp of the copy.
    #[serde(default)]
    pub last_modified: Option<String>,
}

/// `ListBucketResult` (ListObjectsV2).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListBucketResult {
    /// Bucket name echo. **Deliberately required** (no `#[serde(default)]`):
    /// `quick-xml`'s deserializer ignores the root element name, and every
    /// other field here is defaultable, so without one mandatory element any
    /// well-formed non-list XML (a stealth `<Error>` answered with HTTP 200,
    /// a captive portal's login page) would deserialize as an *empty
    /// listing*. AWS and Garage v2.2.0 always emit `<Name>`, even for empty
    /// buckets; requiring it makes such documents fail this parse and fall
    /// through to the stealth-error handling in the client.
    pub name: String,
    /// Whether this page was truncated.
    #[serde(default)]
    pub is_truncated: bool,
    /// Echo of the requested `encoding-type`. When `Some("url")`, keys and
    /// prefixes in this document are URL-encoded and must be decoded with
    /// [`url_decode_key`].
    #[serde(default)]
    pub encoding_type: Option<String>,
    /// Token for the next page.
    #[serde(default)]
    pub next_continuation_token: Option<String>,
    /// Number of keys on this page.
    #[serde(default)]
    pub key_count: Option<u32>,
    /// Object entries.
    #[serde(default)]
    pub contents: Vec<ListContents>,
    /// Delimiter-grouped prefixes.
    #[serde(default)]
    pub common_prefixes: Vec<CommonPrefix>,
}

/// One `<Contents>` entry of `ListBucketResult`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListContents {
    /// Object key.
    pub key: String,
    /// Object size in bytes.
    pub size: u64,
    /// ETag, still quoted here.
    #[serde(rename = "ETag", default)]
    pub e_tag: Option<String>,
    /// Last-modified timestamp.
    #[serde(default)]
    pub last_modified: Option<String>,
}

/// One `<CommonPrefixes>` entry.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct CommonPrefix {
    /// The grouped prefix (ends with the delimiter).
    pub prefix: String,
}

/// `ListMultipartUploadsResult`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListMultipartUploadsResult {
    /// Bucket name echo. **Deliberately required**: see
    /// [`ListBucketResult::name`] — one mandatory element per listing type
    /// keeps a well-formed non-list document (stealth error, captive portal)
    /// from parsing as an empty listing, which would make the §2.4
    /// stale-upload sweep conclude there is nothing to abort. AWS and Garage
    /// v2.2.0 always emit `<Bucket>`.
    pub bucket: String,
    /// Whether this page was truncated.
    #[serde(default)]
    pub is_truncated: bool,
    /// Echo of the requested `encoding-type`. When `Some("url")`, keys and
    /// the `NextKeyMarker` in this document are URL-encoded and must be
    /// decoded with [`url_decode_key`]. A backend that ignores the parameter
    /// omits the echo and returns raw keys.
    #[serde(default)]
    pub encoding_type: Option<String>,
    /// `NextKeyMarker` for the next page (present on truncated pages).
    #[serde(default)]
    pub next_key_marker: Option<String>,
    /// `NextUploadIdMarker` for the next page (AWS emits it; Garage v2.2.0
    /// omits it and pages on the key marker alone).
    #[serde(default)]
    pub next_upload_id_marker: Option<String>,
    /// In-progress uploads.
    #[serde(rename = "Upload", default)]
    pub uploads: Vec<UploadEntry>,
}

/// One `<Upload>` entry of `ListMultipartUploadsResult`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct UploadEntry {
    /// Object key the upload targets.
    pub key: String,
    /// Upload id.
    pub upload_id: String,
    /// Initiation timestamp.
    #[serde(default)]
    pub initiated: Option<String>,
}

/// `ListPartsResult`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListPartsResult {
    /// Bucket name echo. **Deliberately required**: see
    /// [`ListBucketResult::name`] — keeps non-list XML from parsing as "no
    /// parts uploaded yet", which would break multipart resume. AWS and
    /// Garage v2.2.0 always emit `<Bucket>`.
    pub bucket: String,
    /// Whether this page was truncated.
    #[serde(default)]
    pub is_truncated: bool,
    /// `NextPartNumberMarker` for the next page (present on truncated
    /// pages).
    #[serde(default)]
    pub next_part_number_marker: Option<u32>,
    /// Uploaded parts.
    #[serde(rename = "Part", default)]
    pub parts: Vec<PartEntry>,
}

/// One `<Part>` entry of `ListPartsResult`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct PartEntry {
    /// 1-based part number.
    pub part_number: u32,
    /// Part ETag, still quoted here.
    #[serde(rename = "ETag")]
    pub e_tag: String,
    /// Part size in bytes.
    pub size: u64,
}

/// Request body for `CompleteMultipartUpload` (serialized with the root
/// element name `CompleteMultipartUpload`).
#[derive(Debug, Clone, Serialize)]
#[serde(rename = "CompleteMultipartUpload")]
pub struct CompleteMultipartUploadRequest {
    /// Parts in ascending part-number order.
    #[serde(rename = "Part")]
    pub parts: Vec<CompletePartRequest>,
}

/// One `<Part>` of the `CompleteMultipartUpload` request body.
#[derive(Debug, Clone, Serialize)]
pub struct CompletePartRequest {
    /// 1-based part number.
    #[serde(rename = "PartNumber")]
    pub part_number: u32,
    /// The part's ETag (quoted form, as `UploadPart` returned it).
    #[serde(rename = "ETag")]
    pub e_tag: String,
}

/// Parses a non-2xx response body into [`S3Error::Api`], synthesizing a
/// code from `status` when the body is empty or unparsable (HEAD responses
/// have no body: 404 → `NoSuchKey`, 403 → `AccessDenied`).
pub fn parse_error_response(status: u16, body: &[u8]) -> S3Error {
    use super::error::S3ErrorCode;

    if !body.is_empty() {
        let text = String::from_utf8_lossy(body);
        if let Ok(doc) = quick_xml::de::from_str::<ErrorDocument>(&text) {
            return S3Error::Api {
                status,
                code: S3ErrorCode::from_code(&doc.code),
                message: doc.message.unwrap_or_default(),
                resource: doc.resource,
                request_id: doc.request_id,
            };
        }
    }
    // Body-less (HEAD) or unparsable: synthesize the code from the status.
    let code = match status {
        404 => S3ErrorCode::NoSuchKey,
        403 => S3ErrorCode::AccessDenied,
        412 => S3ErrorCode::PreconditionFailed,
        416 => S3ErrorCode::InvalidRange,
        other => S3ErrorCode::Other(format!("Http{other}")),
    };
    S3Error::Api {
        status,
        code,
        message: format!("HTTP {status} with no parsable error body"),
        resource: None,
        request_id: None,
    }
}

/// Strips one pair of surrounding double quotes from an ETag, when present.
pub fn strip_etag_quotes(e_tag: &str) -> String {
    e_tag
        .strip_prefix('"')
        .and_then(|rest| rest.strip_suffix('"'))
        .unwrap_or(e_tag)
        .to_string()
}

/// Decodes a key/prefix from a listing requested with `encoding-type=url`.
///
/// AWS encodes with form-urlencoding (space becomes `+`); Garage v2.2.0
/// percent-encodes space as `%20` and `+` as `%2B`. Decoding `+` to a space
/// and then percent-decoding handles both, because neither backend ever
/// emits a bare `+` that means a literal plus.
pub fn url_decode_key(encoded: &str) -> Result<String, S3Error> {
    let plus_decoded = encoded.replace('+', " ");
    percent_encoding::percent_decode_str(&plus_decoded)
        .decode_utf8()
        .map(|cow| cow.into_owned())
        .map_err(|e| {
            S3Error::InvalidResponse(format!(
                "URL-encoded key {encoded:?} does not decode to UTF-8: {e}"
            ))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::error::S3ErrorCode;

    #[test]
    fn body_less_statuses_synthesize_typed_codes() {
        // HEAD responses (and some ranged-GET rejections) carry no XML body;
        // the typed code must still be derivable from the status so the §2.4
        // probe/resume paths can match on it.
        for (status, expected) in [
            (404, S3ErrorCode::NoSuchKey),
            (403, S3ErrorCode::AccessDenied),
            (412, S3ErrorCode::PreconditionFailed),
            (416, S3ErrorCode::InvalidRange),
            (503, S3ErrorCode::Other("Http503".to_string())),
        ] {
            let err = parse_error_response(status, b"");
            assert_eq!(err.code(), Some(&expected), "status {status}: {err:?}");
            assert_eq!(err.http_status(), Some(status));
        }
        // With an XML body, the body's <Code> wins over the status.
        let err = parse_error_response(
            416,
            b"<Error><Code>InvalidRange</Code><Message>range not satisfiable</Message></Error>",
        );
        assert_eq!(err.code(), Some(&S3ErrorCode::InvalidRange), "{err:?}");
    }

    #[test]
    fn url_decode_key_handles_both_aws_and_garage_forms() {
        // Garage form: %20 for space, %2B for '+'.
        assert_eq!(
            url_decode_key("enc%2FK%C3%A4ch%20photos%2Fa%2Bb.txt").unwrap(),
            "enc/Käch photos/a+b.txt"
        );
        // AWS form: '+' for space.
        assert_eq!(url_decode_key("a+b%2Fc").unwrap(), "a b/c");
        // Control characters (the reason encoding-type=url is requested at
        // all: they are invalid in XML 1.0 when emitted raw).
        assert_eq!(
            url_decode_key("weird%2F%01ctl.bin").unwrap(),
            "weird/\u{1}ctl.bin"
        );
        // A literal '%' in the key round-trips through double encoding.
        assert_eq!(url_decode_key("p%2541.bin").unwrap(), "p%41.bin");
        // Invalid UTF-8 after decoding is a typed invalid-response error.
        assert!(matches!(
            url_decode_key("%FF%FE"),
            Err(S3Error::InvalidResponse(_))
        ));
    }
}
