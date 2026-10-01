//! Typed errors for the S3 client.
//!
//! The sync engine's integrity probe (architecture §2.4) *depends* on being
//! able to distinguish "the server rejected my deliberately wrong
//! `Content-MD5`" from every other failure, so API errors carry a parsed,
//! matchable [`S3ErrorCode`] rather than a bare string.

use std::fmt;

/// Well-known S3 error codes, parsed from the `<Code>` element of an XML
/// error body (or synthesized from the HTTP status for body-less `HEAD`
/// responses).
///
/// Codes outside this list are preserved verbatim in [`S3ErrorCode::Other`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum S3ErrorCode {
    /// AWS S3: the `Content-MD5` you specified did not match what the server
    /// received.
    BadDigest,
    /// Garage (v2.2.0, verified empirically) uses `InvalidDigest` both for a
    /// malformed `Content-MD5` *and* for a well-formed one that does not
    /// match the received bytes (where AWS would answer `BadDigest`).
    InvalidDigest,
    /// The specified key does not exist.
    NoSuchKey,
    /// The specified bucket does not exist.
    NoSuchBucket,
    /// The specified multipart upload does not exist (bad/aborted upload id).
    NoSuchUpload,
    /// One or more of the parts named in `CompleteMultipartUpload` could not
    /// be found or did not match its ETag.
    InvalidPart,
    /// Parts in `CompleteMultipartUpload` were not in ascending order.
    InvalidPartOrder,
    /// A non-final part was smaller than the minimum allowed part size.
    EntityTooSmall,
    /// A conditional request (`If-Match` etc.) failed. The client does not
    /// yet send conditional headers (future work for §2.6 conflict
    /// handling), so today this is only reachable via the synthesized
    /// body-less 412 mapping or a proxy-injected condition.
    PreconditionFailed,
    /// The requested `Range` cannot be satisfied (HTTP 416) — e.g. a resume
    /// offset at or past the current object length after a remote rewrite.
    /// The §2.4 ranged-GET resume path matches on this to restart from zero.
    /// Synthesized from a body-less 416 as well.
    InvalidRange,
    /// The request was not allowed for this key/bucket.
    AccessDenied,
    /// The computed request signature did not match (a SigV4 bug on our
    /// side, clock skew, or wrong credentials).
    SignatureDoesNotMatch,
    /// Any other code, preserved verbatim.
    Other(String),
}

impl S3ErrorCode {
    /// Parses a `<Code>` string into a typed code, falling back to
    /// [`S3ErrorCode::Other`] for anything unrecognized.
    pub fn from_code(code: &str) -> Self {
        match code {
            "BadDigest" => Self::BadDigest,
            "InvalidDigest" => Self::InvalidDigest,
            "NoSuchKey" => Self::NoSuchKey,
            "NoSuchBucket" => Self::NoSuchBucket,
            "NoSuchUpload" => Self::NoSuchUpload,
            "InvalidPart" => Self::InvalidPart,
            "InvalidPartOrder" => Self::InvalidPartOrder,
            "EntityTooSmall" => Self::EntityTooSmall,
            "PreconditionFailed" => Self::PreconditionFailed,
            "InvalidRange" => Self::InvalidRange,
            "AccessDenied" => Self::AccessDenied,
            "SignatureDoesNotMatch" => Self::SignatureDoesNotMatch,
            other => Self::Other(other.to_string()),
        }
    }

    /// The wire representation of this code (what the server's `<Code>`
    /// element contained).
    pub fn as_str(&self) -> &str {
        match self {
            Self::BadDigest => "BadDigest",
            Self::InvalidDigest => "InvalidDigest",
            Self::NoSuchKey => "NoSuchKey",
            Self::NoSuchBucket => "NoSuchBucket",
            Self::NoSuchUpload => "NoSuchUpload",
            Self::InvalidPart => "InvalidPart",
            Self::InvalidPartOrder => "InvalidPartOrder",
            Self::EntityTooSmall => "EntityTooSmall",
            Self::PreconditionFailed => "PreconditionFailed",
            Self::InvalidRange => "InvalidRange",
            Self::AccessDenied => "AccessDenied",
            Self::SignatureDoesNotMatch => "SignatureDoesNotMatch",
            Self::Other(code) => code,
        }
    }

    /// `true` when the server rejected the request body because its
    /// `Content-MD5` did not verify.
    ///
    /// Covers both AWS's `BadDigest` and Garage's `InvalidDigest` (verified
    /// against Garage v2.2.0 — see module docs on [`crate::s3`]). This is the
    /// predicate the §2.4 backend integrity probe matches on.
    pub fn is_digest_rejection(&self) -> bool {
        matches!(self, Self::BadDigest | Self::InvalidDigest)
    }
}

impl fmt::Display for S3ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Errors returned by every [`crate::s3::S3Client`] operation.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum S3Error {
    /// The server answered with a non-success HTTP status. `code`/`message`
    /// are parsed from the XML error body when one is present; for body-less
    /// responses (`HEAD`) the code is synthesized from the status.
    #[error("S3 error {code} (HTTP {status}): {message}")]
    Api {
        /// HTTP status code of the response.
        status: u16,
        /// Typed S3 error code.
        code: S3ErrorCode,
        /// Human-readable message from the error body (may be empty).
        message: String,
        /// `<Resource>` from the error body, when present.
        resource: Option<String>,
        /// `<RequestId>` from the error body, when present (Garage omits it).
        request_id: Option<String>,
    },

    /// The HTTP request itself failed (DNS, connect, TLS, timeout, broken
    /// stream, ...).
    ///
    /// An I/O error raised by a *caller-supplied* streaming part body also
    /// surfaces here: it passes through `reqwest::Body::wrap_stream` and
    /// comes back as a `reqwest::Error`. There is deliberately no separate
    /// "body I/O" variant until one can be constructed reliably.
    #[error("transport error: {0}")]
    Transport(#[from] reqwest::Error),

    /// The server answered 2xx but the response could not be interpreted
    /// (malformed XML, missing required element/header).
    #[error("invalid S3 response: {0}")]
    InvalidResponse(String),

    /// The request could not be constructed (unparsable endpoint, a header
    /// value that is not valid in HTTP, unserializable request body).
    #[error("invalid request: {0}")]
    InvalidRequest(String),
}

impl S3Error {
    /// The HTTP status, for [`S3Error::Api`] errors.
    pub fn http_status(&self) -> Option<u16> {
        match self {
            Self::Api { status, .. } => Some(*status),
            _ => None,
        }
    }

    /// The typed S3 error code, for [`S3Error::Api`] errors.
    pub fn code(&self) -> Option<&S3ErrorCode> {
        match self {
            Self::Api { code, .. } => Some(code),
            _ => None,
        }
    }

    /// `true` when this is an API error whose code is a Content-MD5 digest
    /// rejection (`BadDigest` / `InvalidDigest`). See
    /// [`S3ErrorCode::is_digest_rejection`].
    pub fn is_digest_rejection(&self) -> bool {
        self.code().is_some_and(S3ErrorCode::is_digest_rejection)
    }

    /// `true` when this is an API error with code `NoSuchKey`.
    pub fn is_no_such_key(&self) -> bool {
        self.code() == Some(&S3ErrorCode::NoSuchKey)
    }

    /// `true` when this is an API error with code `NoSuchBucket`.
    pub fn is_no_such_bucket(&self) -> bool {
        self.code() == Some(&S3ErrorCode::NoSuchBucket)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digest_rejection_covers_bad_digest_and_invalid_digest() {
        // AWS answers BadDigest for a well-formed, mismatched Content-MD5;
        // Garage v2.2.0 answers InvalidDigest (verified empirically). The
        // §2.4 integrity probe must treat both as "backend verifies digests".
        assert!(S3ErrorCode::BadDigest.is_digest_rejection());
        assert!(S3ErrorCode::InvalidDigest.is_digest_rejection());
        assert!(!S3ErrorCode::NoSuchKey.is_digest_rejection());
        assert!(!S3ErrorCode::Other("SlowDown".to_string()).is_digest_rejection());
    }

    #[test]
    fn error_codes_parse_from_wire_strings() {
        assert_eq!(S3ErrorCode::from_code("BadDigest"), S3ErrorCode::BadDigest);
        assert_eq!(
            S3ErrorCode::from_code("InvalidDigest"),
            S3ErrorCode::InvalidDigest
        );
        assert_eq!(S3ErrorCode::from_code("NoSuchKey"), S3ErrorCode::NoSuchKey);
        assert_eq!(
            S3ErrorCode::from_code("NoSuchBucket"),
            S3ErrorCode::NoSuchBucket
        );
        assert_eq!(
            S3ErrorCode::from_code("NoSuchUpload"),
            S3ErrorCode::NoSuchUpload
        );
        assert_eq!(
            S3ErrorCode::from_code("InvalidPart"),
            S3ErrorCode::InvalidPart
        );
        assert_eq!(
            S3ErrorCode::from_code("PreconditionFailed"),
            S3ErrorCode::PreconditionFailed
        );
        assert_eq!(
            S3ErrorCode::from_code("InvalidRange"),
            S3ErrorCode::InvalidRange
        );
        assert_eq!(
            S3ErrorCode::from_code("TotallyMadeUp"),
            S3ErrorCode::Other("TotallyMadeUp".to_string())
        );
    }

    #[test]
    fn error_codes_round_trip_through_as_str() {
        for wire in [
            "BadDigest",
            "InvalidDigest",
            "NoSuchKey",
            "NoSuchBucket",
            "NoSuchUpload",
            "InvalidPart",
            "InvalidPartOrder",
            "EntityTooSmall",
            "PreconditionFailed",
            "InvalidRange",
            "AccessDenied",
            "SignatureDoesNotMatch",
            "SomeUnknownCode",
        ] {
            assert_eq!(S3ErrorCode::from_code(wire).as_str(), wire);
        }
    }

    #[test]
    fn api_error_helpers_match_only_their_code() {
        let err = S3Error::Api {
            status: 404,
            code: S3ErrorCode::NoSuchKey,
            message: "Key not found".to_string(),
            resource: Some("/bucket/key".to_string()),
            request_id: None,
        };
        assert!(err.is_no_such_key());
        assert!(!err.is_no_such_bucket());
        assert!(!err.is_digest_rejection());
        assert_eq!(err.http_status(), Some(404));
        assert_eq!(err.code(), Some(&S3ErrorCode::NoSuchKey));

        let digest = S3Error::Api {
            status: 400,
            code: S3ErrorCode::InvalidDigest,
            message: "MD5 checksum verification failed".to_string(),
            resource: None,
            request_id: None,
        };
        assert!(digest.is_digest_rejection());
        assert!(!digest.is_no_such_key());
    }
}
