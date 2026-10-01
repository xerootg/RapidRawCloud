//! AWS Signature Version 4 request signing (hand-rolled, architecture §3.9).
//!
//! The functions here are deliberately small, pure, and string-in/string-out
//! so each canonicalization step can be unit-tested against the vectors in
//! AWS's SigV4 documentation without any network or clock.
//!
//! Signing pipeline (AWS "Signature Version 4 signing process"):
//!
//! 1. Build the *canonical request*: method, canonical URI, canonical query
//!    string, canonical headers, signed-header list, payload hash.
//! 2. Build the *string to sign* from the request timestamp, the credential
//!    scope and the SHA-256 of the canonical request.
//! 3. Derive the *signing key* with the HMAC chain
//!    `AWS4<secret> -> date -> region -> service -> "aws4_request"`.
//! 4. The signature is `hex(HMAC-SHA256(signing_key, string_to_sign))`, which
//!    goes into the `Authorization` header.
//!
//! Payload hashing policy (per §3.9): buffered bodies are signed with their
//! real hex SHA-256; streamed multipart parts are signed with
//! [`UNSIGNED_PAYLOAD`] (integrity is carried by `Content-MD5`, which the
//! server verifies — architecture §2.4). Garage v2.2.0 accepts
//! `UNSIGNED-PAYLOAD` (verified empirically).

/// `x-amz-content-sha256` value for bodies that are not covered by the
/// signature (streamed multipart parts).
pub const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";

/// Hex SHA-256 of the empty string — the payload hash of body-less requests
/// (GET/HEAD/DELETE).
pub const EMPTY_PAYLOAD_SHA256: &str =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// The service name S3 requests are scoped to.
pub const SERVICE: &str = "s3";

/// Canonical header block: the `canonical` text that goes into the canonical
/// request, and the `signed_headers` list (`;`-joined lowercase names) that
/// goes into both the canonical request and the `Authorization` header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalHeaders {
    /// One `name:value\n` line per header, names lowercased, values trimmed,
    /// lines sorted by name.
    pub canonical: String,
    /// Lowercase header names, sorted, joined with `;`.
    pub signed_headers: String,
}

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

/// HMAC-SHA256 of `data` under `key`.
///
/// HMAC accepts keys of any length (RFC 2104), so construction cannot fail.
fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC-SHA256 accepts keys of any length");
    mac.update(data);
    mac.finalize().into_bytes().into()
}

/// Hex-encoded SHA-256 of `data` (the payload-hash form SigV4 uses).
pub fn sha256_hex(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

/// AWS URI-encodes `input` per the SigV4 canonicalization rules:
/// unreserved characters (`A–Z a–z 0–9 - _ . ~`) are kept, everything else
/// (including space, `+`, and non-ASCII, which is encoded per UTF-8 byte) is
/// percent-encoded uppercase. `/` is kept literal only when `encode_slash`
/// is `false` (object-key path position); in query position it is encoded.
pub fn uri_encode(input: &str, encode_slash: bool) -> String {
    let mut out = String::with_capacity(input.len());
    for &b in input.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            b'/' if !encode_slash => out.push('/'),
            _ => {
                const HEX: &[u8; 16] = b"0123456789ABCDEF";
                out.push('%');
                out.push(HEX[usize::from(b >> 4)] as char);
                out.push(HEX[usize::from(b & 0x0f)] as char);
            }
        }
    }
    out
}

/// Canonical URI for an absolute request path: each `/`-separated segment is
/// URI-encoded (per [`uri_encode`]), with the `/` separators preserved. An
/// empty path canonicalizes to `/`. S3-style: no path normalization, and
/// already-literal characters like spaces, `+` and non-ASCII in object keys
/// must come out encoded exactly once.
pub fn canonical_uri(path: &str) -> String {
    if path.is_empty() {
        return "/".to_string();
    }
    let encoded = path
        .split('/')
        .map(|segment| uri_encode(segment, true))
        .collect::<Vec<_>>()
        .join("/");
    if encoded.starts_with('/') {
        encoded
    } else {
        format!("/{encoded}")
    }
}

/// Canonical query string: each name and value URI-encoded (slash encoded),
/// pairs sorted by encoded name (then encoded value), joined `name=value`
/// with `&`. A valueless parameter (e.g. `uploads`) serializes as `name=`.
pub fn canonical_query_string(params: &[(&str, &str)]) -> String {
    let mut pairs: Vec<(String, String)> = params
        .iter()
        .map(|(name, value)| (uri_encode(name, true), uri_encode(value, true)))
        .collect();
    pairs.sort();
    pairs
        .iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

/// Canonicalizes headers for signing: lowercases names, trims surrounding
/// whitespace from values (and collapses internal runs of spaces, per the
/// SigV4 spec), sorts by name, and produces both the canonical block and the
/// `;`-joined signed-header list. Repeated header names are merged into one
/// line with their values comma-joined in order of appearance (SigV4 spec).
///
/// Divergence from strict SigV4: the spec's trimall rule exempts quoted
/// strings from space-collapsing, and only collapses spaces; this collapses
/// *all* whitespace runs unconditionally. That is safe here because the
/// client sends exactly the collapsed value (see `sanitize_header_value` in
/// the client), so signed == sent and server-side re-canonicalization is
/// idempotent.
pub fn canonical_headers(headers: &[(&str, &str)]) -> CanonicalHeaders {
    let mut raw: Vec<(String, String)> = headers
        .iter()
        .map(|(name, value)| {
            (
                name.to_ascii_lowercase(),
                // Trim surrounding whitespace and collapse internal runs of
                // whitespace to a single space, per the SigV4 spec.
                value.split_whitespace().collect::<Vec<_>>().join(" "),
            )
        })
        .collect();
    // Stable sort by name only, so duplicate names keep their original
    // value order for the comma-join below.
    raw.sort_by(|a, b| a.0.cmp(&b.0));
    let mut entries: Vec<(String, String)> = Vec::with_capacity(raw.len());
    for (name, value) in raw {
        match entries.last_mut() {
            Some((last_name, joined)) if *last_name == name => {
                joined.push(',');
                joined.push_str(&value);
            }
            _ => entries.push((name, value)),
        }
    }
    let canonical = entries
        .iter()
        .map(|(name, value)| format!("{name}:{value}\n"))
        .collect::<String>();
    let signed_headers = entries
        .iter()
        .map(|(name, _)| name.as_str())
        .collect::<Vec<_>>()
        .join(";");
    CanonicalHeaders {
        canonical,
        signed_headers,
    }
}

/// Assembles the canonical request string:
///
/// ```text
/// <method>\n<canonical_uri>\n<canonical_query>\n<canonical_headers><signed_headers>\n<payload_hash>
/// ```
///
/// (`canonical_headers` already ends with the `\n` that separates the header
/// block from the signed-header list.)
pub fn canonical_request(
    method: &str,
    canonical_uri: &str,
    canonical_query: &str,
    headers: &CanonicalHeaders,
    payload_hash: &str,
) -> String {
    format!(
        "{method}\n{canonical_uri}\n{canonical_query}\n{}\n{}\n{payload_hash}",
        headers.canonical, headers.signed_headers
    )
}

/// Credential scope: `<yyyymmdd>/<region>/<service>/aws4_request`.
pub fn credential_scope(date: &str, region: &str, service: &str) -> String {
    format!("{date}/{region}/{service}/aws4_request")
}

/// String to sign:
///
/// ```text
/// AWS4-HMAC-SHA256\n<amz_date>\n<scope>\n<hex sha256(canonical_request)>
/// ```
pub fn string_to_sign(amz_date: &str, scope: &str, canonical_request: &str) -> String {
    format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    )
}

/// Derives the SigV4 signing key:
/// `HMAC(HMAC(HMAC(HMAC("AWS4"+secret, date), region), service), "aws4_request")`.
pub fn derive_signing_key(secret_key: &str, date: &str, region: &str, service: &str) -> [u8; 32] {
    let k_date = hmac_sha256(format!("AWS4{secret_key}").as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    hmac_sha256(&k_service, b"aws4_request")
}

/// Final signature: lowercase hex of `HMAC-SHA256(signing_key, string_to_sign)`.
pub fn signature_hex(signing_key: &[u8; 32], string_to_sign: &str) -> String {
    hex::encode(hmac_sha256(signing_key, string_to_sign.as_bytes()))
}

/// Builds the `Authorization` header value:
///
/// ```text
/// AWS4-HMAC-SHA256 Credential=<akid>/<scope>, SignedHeaders=<list>, Signature=<hex>
/// ```
pub fn authorization_header(
    access_key_id: &str,
    scope: &str,
    signed_headers: &str,
    signature: &str,
) -> String {
    format!(
        "AWS4-HMAC-SHA256 Credential={access_key_id}/{scope}, \
         SignedHeaders={signed_headers}, Signature={signature}"
    )
}

/// Formats a timestamp as the `x-amz-date` basic-ISO form
/// `YYYYMMDD'T'HHMMSS'Z'` (always UTC).
pub fn format_amz_date(t: time::OffsetDateTime) -> String {
    let t = t.to_offset(time::UtcOffset::UTC);
    format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        t.year(),
        u8::from(t.month()),
        t.day(),
        t.hour(),
        t.minute(),
        t.second()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- AWS documented vectors ------------------------------------------
    //
    // From "Examples of how to derive a signing key for Signature Version 4"
    // and "Signature calculations: using GET with authentication information
    // in the Authorization header" (the IAM ListUsers example), AWS General
    // Reference. These are published, stable test vectors.

    const AWS_SECRET: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";

    #[test]
    fn derive_signing_key_matches_aws_documented_vector() {
        let key = derive_signing_key(AWS_SECRET, "20150830", "us-east-1", "iam");
        assert_eq!(
            hex::encode(key),
            "c4afb1cc5771d871763a393e44b703571b55cc28424d1a5e86da6ed3c154a4b9"
        );
    }

    #[test]
    fn canonical_request_matches_aws_iam_example() {
        let headers = canonical_headers(&[
            ("Host", "iam.amazonaws.com"),
            (
                "Content-Type",
                "application/x-www-form-urlencoded; charset=utf-8",
            ),
            ("X-Amz-Date", "20150830T123600Z"),
        ]);
        assert_eq!(headers.signed_headers, "content-type;host;x-amz-date");

        let query = canonical_query_string(&[("Action", "ListUsers"), ("Version", "2010-05-08")]);
        assert_eq!(query, "Action=ListUsers&Version=2010-05-08");

        let creq = canonical_request("GET", "/", &query, &headers, EMPTY_PAYLOAD_SHA256);
        let expected = "GET\n\
                        /\n\
                        Action=ListUsers&Version=2010-05-08\n\
                        content-type:application/x-www-form-urlencoded; charset=utf-8\n\
                        host:iam.amazonaws.com\n\
                        x-amz-date:20150830T123600Z\n\
                        \n\
                        content-type;host;x-amz-date\n\
                        e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        assert_eq!(creq, expected);
        assert_eq!(
            sha256_hex(creq.as_bytes()),
            "f536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59"
        );
    }

    #[test]
    fn string_to_sign_and_signature_match_aws_iam_example() {
        let scope = credential_scope("20150830", "us-east-1", "iam");
        assert_eq!(scope, "20150830/us-east-1/iam/aws4_request");

        let sts = string_to_sign(
            "20150830T123600Z",
            &scope,
            "GET\n/\nAction=ListUsers&Version=2010-05-08\n\
             content-type:application/x-www-form-urlencoded; charset=utf-8\n\
             host:iam.amazonaws.com\nx-amz-date:20150830T123600Z\n\n\
             content-type;host;x-amz-date\n\
             e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        );
        let expected_sts = "AWS4-HMAC-SHA256\n\
                            20150830T123600Z\n\
                            20150830/us-east-1/iam/aws4_request\n\
                            f536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59";
        assert_eq!(sts, expected_sts);

        let key = derive_signing_key(AWS_SECRET, "20150830", "us-east-1", "iam");
        assert_eq!(
            signature_hex(&key, &sts),
            "5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7"
        );

        let auth = authorization_header(
            "AKIDEXAMPLE",
            &scope,
            "content-type;host;x-amz-date",
            "5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7",
        );
        assert_eq!(
            auth,
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/iam/aws4_request, \
             SignedHeaders=content-type;host;x-amz-date, \
             Signature=5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7"
        );
    }

    // ---- URI encoding edge cases -----------------------------------------

    #[test]
    fn uri_encode_edge_cases() {
        // Unreserved characters pass through.
        assert_eq!(
            uri_encode("AZaz09-_.~", true),
            "AZaz09-_.~",
            "unreserved characters must not be encoded"
        );
        // Space is %20 (never '+').
        assert_eq!(uri_encode("a b", true), "a%20b");
        // '+' is a literal plus in object keys and must be encoded.
        assert_eq!(uri_encode("a+b", true), "a%2Bb");
        // UTF-8 multibyte, uppercase hex.
        assert_eq!(uri_encode("Käch", true), "K%C3%A4ch");
        // Reserved punctuation.
        assert_eq!(uri_encode("a=b&c?d#e", true), "a%3Db%26c%3Fd%23e");
        assert_eq!(uri_encode("*'()", true), "%2A%27%28%29");
        // Slash policy.
        assert_eq!(uri_encode("a/b", true), "a%2Fb");
        assert_eq!(uri_encode("a/b", false), "a/b");
    }

    #[test]
    fn canonical_uri_encodes_segments_and_preserves_slashes() {
        assert_eq!(canonical_uri("/"), "/");
        assert_eq!(canonical_uri(""), "/");
        assert_eq!(canonical_uri("/bucket/plain-key"), "/bucket/plain-key");
        // The architecture's canonical nasty key.
        assert_eq!(
            canonical_uri("/bucket/2026/Käch photos/IMG 0042+1.NEF"),
            "/bucket/2026/K%C3%A4ch%20photos/IMG%200042%2B1.NEF"
        );
        // Trailing slash (prefix-like key) is preserved.
        assert_eq!(canonical_uri("/bucket/dir/"), "/bucket/dir/");
    }

    #[test]
    fn canonical_query_is_sorted_and_encoded() {
        // Sorted by name; empty value keeps 'name='.
        assert_eq!(
            canonical_query_string(&[("uploads", ""), ("prefix", "a b/c")]),
            "prefix=a%20b%2Fc&uploads="
        );
        // Values with '+' and unicode; sorting is by encoded name.
        assert_eq!(
            canonical_query_string(&[
                ("list-type", "2"),
                ("continuation-token", "abc+def="),
                ("delimiter", "/"),
                ("prefix", "Kä"),
            ]),
            "continuation-token=abc%2Bdef%3D&delimiter=%2F&list-type=2&prefix=K%C3%A4"
        );
        assert_eq!(canonical_query_string(&[]), "");
    }

    #[test]
    fn canonical_headers_are_lowercased_sorted_and_trimmed() {
        let ch = canonical_headers(&[
            ("X-Amz-Date", "20260101T000000Z"),
            ("Host", "  127.0.0.1:3900  "),
            ("x-amz-content-sha256", UNSIGNED_PAYLOAD),
            ("Content-MD5", "  q6yF1nIO8+jsDzrpg\t  "),
            ("X-Amz-Meta-Rr-Blake3", "  spaced   out value  "),
        ]);
        assert_eq!(
            ch.signed_headers,
            "content-md5;host;x-amz-content-sha256;x-amz-date;x-amz-meta-rr-blake3"
        );
        assert_eq!(
            ch.canonical,
            "content-md5:q6yF1nIO8+jsDzrpg\n\
             host:127.0.0.1:3900\n\
             x-amz-content-sha256:UNSIGNED-PAYLOAD\n\
             x-amz-date:20260101T000000Z\n\
             x-amz-meta-rr-blake3:spaced out value\n"
        );
    }

    #[test]
    fn canonical_headers_merge_duplicate_names_with_commas() {
        // SigV4 requires repeated headers to appear once, values joined by
        // commas in order of appearance, and the name listed once in
        // SignedHeaders.
        let ch = canonical_headers(&[
            ("X-Amz-Meta-Tag", "one"),
            ("Host", "h.example"),
            ("x-amz-meta-tag", " two "),
        ]);
        assert_eq!(ch.signed_headers, "host;x-amz-meta-tag");
        assert_eq!(ch.canonical, "host:h.example\nx-amz-meta-tag:one,two\n");
    }

    #[test]
    fn amz_date_is_basic_iso_utc() {
        let t = time::macros::datetime!(2015-08-30 12:36:00 UTC);
        assert_eq!(format_amz_date(t), "20150830T123600Z");
        // A non-UTC offset must be converted to UTC, not just reformatted.
        let offset = time::macros::datetime!(2015-08-30 14:36:00 +02:00);
        assert_eq!(format_amz_date(offset), "20150830T123600Z");
    }
}
