//! Hostile-XML resilience for the S3 response parsers (`rrcloud_core::s3::xml`).
//!
//! Every XML body this crate parses comes from the S3 endpoint (ListObjectsV2,
//! ListMultipartUploads, ListParts, multipart results, `<Error>` bodies), so
//! a hostile or compromised endpoint controls the parser's input. Both
//! `s3::client::parse_xml_body` and `s3::xml::parse_error_response` go
//! through `quick_xml::de::from_str`, i.e. through quick-xml's `NsReader` plus
//! the serde `ElementMapAccess` attribute iterator with duplicate checking on.
//!
//! Pins the two RustSec advisories against quick-xml < 0.41.0:
//!
//! * RUSTSEC-2026-0194 — the duplicate-attribute check was `O(N²)` in the
//!   number of attributes on one start tag. Pure CPU with no `.await`, so
//!   reqwest's read/request timeouts cannot interrupt it; a single crafted
//!   tag pins the thread doing the parse. Measured on 0.38.4 with the 60 000
//!   attributes used below: 7.8 s release / 67 s debug (`<ListBucketResult>`),
//!   15 s release / 145 s debug (`<Error>`); on 0.41.0: 24 ms release /
//!   185–285 ms debug.
//! * RUSTSEC-2026-0195 — `NsReader` allocated roughly 3× the start tag's
//!   size for `xmlns`/`xmlns:*` declarations before the consumer saw the
//!   event, with no cap. 0.41.0 rejects more than 256 declarations on one
//!   element with `NamespaceError::TooManyDeclarations`. Measured on 0.38.4:
//!   200 000 declarations (5.6 MB tag) → 61 s and 19.8 MB resolver heap,
//!   parsed Ok; on 0.41.0 → Err in 5 ms.
//!
//! The wall-clock bound below is deliberately loose (2 s for work that takes
//! well under 300 ms on the patched crate even in a debug build) so the tests
//! stay deterministic on a loaded CI runner while still failing by a wide
//! margin (>3x release, >30x debug) on the vulnerable crate.

use std::time::{Duration, Instant};

use rrcloud_core::s3::xml::{parse_error_response, ErrorDocument, ListBucketResult};
use rrcloud_core::s3::S3ErrorCode;

/// Generous bound: the patched parser finishes these inputs in well under
/// 300 ms (debug build); the vulnerable one needs many seconds.
const BOUND: Duration = Duration::from_secs(2);

/// Attribute count per start tag for the quadratic-check cases: large enough
/// that the vulnerable crate overshoots [`BOUND`] several times over even in
/// a release build, small enough that the failing run is about a minute in
/// debug.
const ATTRS: usize = 60_000;

/// Namespace declarations per start tag for the allocation case. Far above
/// quick-xml's default cap of 256 and far above anything a real S3 backend
/// emits (AWS and Garage emit exactly one `xmlns` on the root).
const NS_DECLS: usize = 10_000;

/// `<ListBucketResult>` whose root start tag carries `n` distinct attributes
/// (all ignored by serde, as attributes on every element here are).
fn list_bucket_result_with_attrs(n: usize) -> String {
    let mut xml = String::with_capacity(n * 10 + 128);
    xml.push_str(r#"<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/""#);
    for i in 0..n {
        xml.push_str(&format!(r#" a{i}="""#));
    }
    xml.push_str("><Name>photos</Name><IsTruncated>false</IsTruncated><KeyCount>0</KeyCount></ListBucketResult>");
    xml
}

/// `<Error>` body whose root start tag carries `n` distinct attributes.
fn error_body_with_attrs(n: usize) -> Vec<u8> {
    let mut xml = String::with_capacity(n * 10 + 128);
    xml.push_str("<Error");
    for i in 0..n {
        xml.push_str(&format!(r#" a{i}="""#));
    }
    xml.push_str("><Code>NoSuchKey</Code><Message>gone</Message></Error>");
    xml.into_bytes()
}

/// A start tag with `n` distinct `xmlns:pN` namespace declarations.
fn tag_with_ns_decls(name: &str, n: usize) -> String {
    let mut xml = String::with_capacity(n * 24 + 64);
    xml.push('<');
    xml.push_str(name);
    for i in 0..n {
        xml.push_str(&format!(r#" xmlns:p{i}="urn:x:{i}""#));
    }
    xml.push('>');
    xml
}

fn timed<T>(f: impl FnOnce() -> T) -> (T, Duration) {
    let start = Instant::now();
    let out = f();
    (out, start.elapsed())
}

#[test]
fn list_bucket_result_with_huge_attribute_count_parses_in_bounded_time() {
    // Exactly what `s3::client::parse_xml_body::<ListBucketResult>` does with
    // a 2xx ListObjectsV2 body.
    let xml = list_bucket_result_with_attrs(ATTRS);
    let (parsed, elapsed) = timed(|| quick_xml::de::from_str::<ListBucketResult>(&xml));
    let parsed = parsed.expect("attributes on the root are ignored, document still parses");
    assert_eq!(parsed.name, "photos");
    assert!(
        elapsed < BOUND,
        "RUSTSEC-2026-0194: parsing a ListBucketResult whose root tag has {ATTRS} attributes \
         took {elapsed:?} (bound {BOUND:?}); the duplicate-attribute check is quadratic"
    );
}

#[test]
fn error_body_with_huge_attribute_count_parses_in_bounded_time() {
    // The crate's own public entry point for non-2xx bodies.
    let body = error_body_with_attrs(ATTRS);
    let (err, elapsed) = timed(|| parse_error_response(404, &body));
    assert_eq!(
        err.code(),
        Some(&S3ErrorCode::NoSuchKey),
        "the body's <Code> is still honoured: {err:?}"
    );
    assert!(
        elapsed < BOUND,
        "RUSTSEC-2026-0194: parse_error_response on an <Error> tag with {ATTRS} attributes \
         took {elapsed:?} (bound {BOUND:?}); the duplicate-attribute check is quadratic"
    );
}

#[test]
fn list_bucket_result_with_namespace_declaration_flood_is_rejected() {
    // RUSTSEC-2026-0195: on the vulnerable crate this parses *successfully*
    // after allocating ~3x the tag size inside NsReader, before the caller can
    // inspect anything. The patched crate refuses the start tag outright
    // (NamespaceError::TooManyDeclarations above 256 declarations), so the
    // only acceptable outcome here is an error.
    let mut xml = tag_with_ns_decls("ListBucketResult", NS_DECLS);
    xml.push_str("<Name>photos</Name></ListBucketResult>");
    let (result, elapsed) = timed(|| quick_xml::de::from_str::<ListBucketResult>(&xml));
    assert!(
        result.is_err(),
        "RUSTSEC-2026-0195: a start tag with {NS_DECLS} xmlns declarations was accepted \
         (parsed {:?}) instead of being rejected",
        result.map(|r| r.name)
    );
    assert!(
        elapsed < BOUND,
        "rejecting the flood took {elapsed:?} (bound {BOUND:?})"
    );
}

#[test]
fn error_body_with_namespace_declaration_flood_is_not_trusted() {
    // Same flood through the crate's real error-body path: a body the parser
    // refuses must fall back to the status-derived code rather than being
    // processed as a genuine <Error> document.
    let mut xml = tag_with_ns_decls("Error", NS_DECLS);
    xml.push_str("<Code>NoSuchKey</Code><Message>gone</Message></Error>");
    let (err, elapsed) = timed(|| parse_error_response(500, xml.as_bytes()));
    assert_eq!(
        err.code(),
        Some(&S3ErrorCode::Other("Http500".to_string())),
        "RUSTSEC-2026-0195: an <Error> tag with {NS_DECLS} xmlns declarations was parsed \
         and its <Code> trusted: {err:?}"
    );
    assert_eq!(err.http_status(), Some(500));
    assert!(
        elapsed < BOUND,
        "rejecting the flood took {elapsed:?} (bound {BOUND:?})"
    );
}

#[test]
fn realistic_listing_with_single_default_namespace_still_parses() {
    // Guard against the fix over-tightening: a Garage/AWS-shaped document with
    // the one real xmlns declaration (plus a couple of harmless attributes)
    // must keep parsing after the upgrade.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <Name>photos</Name>
  <Prefix></Prefix>
  <KeyCount>1</KeyCount>
  <MaxKeys>1000</MaxKeys>
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>enc/a.dng</Key>
    <LastModified>2026-01-01T00:00:00.000Z</LastModified>
    <ETag>&quot;0123456789abcdef0123456789abcdef&quot;</ETag>
    <Size>42</Size>
  </Contents>
</ListBucketResult>"#;
    let parsed =
        quick_xml::de::from_str::<ListBucketResult>(xml).expect("realistic listing parses");
    assert_eq!(parsed.name, "photos");
    assert_eq!(parsed.key_count, Some(1));
    assert_eq!(parsed.contents.len(), 1);
    assert_eq!(parsed.contents[0].key, "enc/a.dng");

    let err_doc = quick_xml::de::from_str::<ErrorDocument>(
        r#"<Error xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Code>AccessDenied</Code><Message>no</Message><Resource>/b/k</Resource></Error>"#,
    )
    .expect("realistic error document parses");
    assert_eq!(err_doc.code, "AccessDenied");
}
