//! S3 client conformance suite against a real Garage v2.2.0 server
//! (architecture §3.9 / §8 P0 requirements).
//!
//! The harness in `common::garage` boots one shared Garage instance per test
//! binary; each test isolates itself in a uniquely named bucket. If the
//! Garage binary is missing (and `CI` is unset) every test skips with an
//! eprintln.

mod common;

use std::collections::BTreeMap;

use base64::Engine as _;
use bytes::Bytes;
use common::garage;
use futures::StreamExt as _;
use md5::Md5;
use rrcloud_core::s3::{
    ByteRange, CompletedPart, ListMultipartUploadsRequest, ListObjectsV2Request, ListPartsRequest,
    PartBody, PutObjectOptions, S3Error, S3ErrorCode,
};
use sha2::{Digest as _, Sha256};

/// Base64 `Content-MD5` of `data`.
fn md5_b64(data: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(Md5::digest(data))
}

/// Hex MD5 (the simple-PUT / per-part ETag form).
fn md5_hex(data: &[u8]) -> String {
    hex::encode(Md5::digest(data))
}

fn sha256_hex(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

/// Deterministic pseudo-random bytes.
fn pattern(len: usize, seed: u8) -> Bytes {
    let mut v = Vec::with_capacity(len);
    let mut x = seed as u32 | 1;
    for _ in 0..len {
        x = x.wrapping_mul(1103515245).wrapping_add(12345);
        v.push((x >> 16) as u8);
    }
    Bytes::from(v)
}

fn meta(entries: &[(&str, &str)]) -> BTreeMap<String, String> {
    entries
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

// ---------------------------------------------------------------------------
// 0. Harness smoke test (no S3 client involved): proves the Garage harness
//    itself boots, configures a layout, and can create buckets via the CLI.
// ---------------------------------------------------------------------------

#[test]
fn garage_harness_smoke() {
    let Some(g) = garage::shared() else { return };
    assert!(g.s3_port > 0);
    assert!(g.access_key_id.starts_with("GK"), "{}", g.access_key_id);
    assert_eq!(g.secret_access_key.len(), 64);
    let status = g.cli(&["status"]).expect("garage status");
    assert!(status.contains("HEALTHY NODES"), "{status}");
    let bucket = g.create_unique_bucket("smoke");
    let info = g.cli(&["bucket", "info", &bucket]).expect("bucket info");
    assert!(info.contains(&bucket), "{info}");
}

// ---------------------------------------------------------------------------
// 1. put/get/head/delete round-trip incl. x-amz-meta echo
// ---------------------------------------------------------------------------

#[tokio::test]
async fn put_get_head_delete_roundtrip_with_metadata() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("roundtrip");
    let client = g.client();

    let body = pattern(4096, 1);
    let opts = PutObjectOptions {
        content_md5: Some(md5_b64(&body)),
        content_type: Some("application/octet-stream".to_string()),
        metadata: meta(&[("rr-blake3", "0123abcd"), ("rr-origin", "conformance")]),
    };
    let put = client
        .put_object(&bucket, "dir/item.bin", body.clone(), &opts)
        .await
        .expect("put_object");
    assert_eq!(put.e_tag, md5_hex(&body), "simple PUT ETag is the body MD5");

    let head = client
        .head_object(&bucket, "dir/item.bin")
        .await
        .expect("head_object");
    assert_eq!(head.content_length, body.len() as u64);
    assert_eq!(head.e_tag, md5_hex(&body));
    assert_eq!(
        head.metadata.get("rr-blake3").map(String::as_str),
        Some("0123abcd")
    );
    assert_eq!(
        head.metadata.get("rr-origin").map(String::as_str),
        Some("conformance")
    );

    let got = client
        .get_object(&bucket, "dir/item.bin", None)
        .await
        .expect("get_object");
    assert_eq!(got.content_length, body.len() as u64);
    assert_eq!(
        got.metadata.get("rr-blake3").map(String::as_str),
        Some("0123abcd"),
        "x-amz-meta must be echoed on GET too"
    );
    let got_bytes = got.body.collect().await.expect("collect body");
    assert_eq!(got_bytes, body);

    client
        .delete_object(&bucket, "dir/item.bin")
        .await
        .expect("delete_object");
    let err = client
        .head_object(&bucket, "dir/item.bin")
        .await
        .expect_err("head after delete must fail");
    assert!(
        err.is_no_such_key(),
        "HEAD of a deleted key must surface NoSuchKey (synthesized from the \
         body-less 404), got: {err:?}"
    );
}

// ---------------------------------------------------------------------------
// 2. Keys that need canonical URI encoding
// ---------------------------------------------------------------------------

#[tokio::test]
async fn keys_requiring_percent_encoding() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("encoding");
    let client = g.client();

    let keys = [
        "2026/Käch photos/IMG 0042+1.NEF",
        "nested/pre fix/deep+er/ünï~code.dat",
        "plus+plus/a b (copy).txt",
    ];
    for (i, key) in keys.iter().enumerate() {
        let body = pattern(512 + i, 7 + i as u8);
        client
            .put_object(&bucket, key, body.clone(), &PutObjectOptions::default())
            .await
            .unwrap_or_else(|e| panic!("put {key:?}: {e:?}"));
        let head = client
            .head_object(&bucket, key)
            .await
            .unwrap_or_else(|e| panic!("head {key:?}: {e:?}"));
        assert_eq!(head.content_length, body.len() as u64, "{key:?}");
        let got = client
            .get_object(&bucket, key, None)
            .await
            .unwrap_or_else(|e| panic!("get {key:?}: {e:?}"));
        assert_eq!(
            got.body.collect().await.expect("collect"),
            body,
            "round-tripped bytes for {key:?}"
        );
    }

    // The nested unicode prefix must round-trip through listing too.
    let listed = client
        .list_all_objects(&bucket, Some("2026/"))
        .await
        .expect("list 2026/");
    assert_eq!(
        listed.iter().map(|o| o.key.as_str()).collect::<Vec<_>>(),
        vec!["2026/Käch photos/IMG 0042+1.NEF"]
    );

    // And deletion by encoded key must hit the right object.
    client
        .delete_object(&bucket, keys[0])
        .await
        .expect("delete encoded key");
    let err = client
        .get_object(&bucket, keys[0], None)
        .await
        .expect_err("get after delete");
    assert!(err.is_no_such_key(), "got: {err:?}");
}

// ---------------------------------------------------------------------------
// 3. Ranged GET: tail from an offset, a middle slice, and a suffix range
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ranged_get_tail_middle_and_suffix() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("ranges");
    let client = g.client();

    let body = pattern(100_000, 42);
    client
        .put_object(
            &bucket,
            "ranged.bin",
            body.clone(),
            &PutObjectOptions::default(),
        )
        .await
        .expect("put");

    // Tail from a byte offset (resume case): bytes=60000-
    let tail = client
        .get_object(&bucket, "ranged.bin", Some(ByteRange::From(60_000)))
        .await
        .expect("tail range");
    assert_eq!(
        tail.content_range.as_deref(),
        Some("bytes 60000-99999/100000")
    );
    assert_eq!(
        tail.body.collect().await.expect("collect tail"),
        body.slice(60_000..),
        "tail bytes must equal the suffix of the object from the offset"
    );

    // A middle range: bytes=10000-19999 (inclusive).
    let mid = client
        .get_object(
            &bucket,
            "ranged.bin",
            Some(ByteRange::Bounded(10_000, 19_999)),
        )
        .await
        .expect("middle range");
    assert_eq!(mid.content_length, 10_000);
    assert_eq!(
        mid.content_range.as_deref(),
        Some("bytes 10000-19999/100000")
    );
    assert_eq!(
        mid.body.collect().await.expect("collect mid"),
        body.slice(10_000..20_000)
    );

    // Suffix form: bytes=-5000 (the final 5000 bytes).
    let suffix = client
        .get_object(&bucket, "ranged.bin", Some(ByteRange::Suffix(5_000)))
        .await
        .expect("suffix range");
    assert_eq!(
        suffix.content_range.as_deref(),
        Some("bytes 95000-99999/100000")
    );
    assert_eq!(
        suffix.body.collect().await.expect("collect suffix"),
        body.slice(95_000..)
    );

    // Unsatisfiable ranges must surface as typed InvalidRange (HTTP 416):
    // this is what the §2.4 ranged-GET resume path matches on to restart a
    // download from zero after a remote rewrite shrank the object. Verified
    // against Garage v2.2.0 for all three shapes.
    for (label, range) in [
        ("offset == len", ByteRange::From(100_000)),
        ("offset past len", ByteRange::From(150_000)),
        ("empty suffix", ByteRange::Suffix(0)),
    ] {
        let err = match client.get_object(&bucket, "ranged.bin", Some(range)).await {
            Err(e) => e,
            Ok(_) => panic!("unsatisfiable range ({label}) must fail, got a 2xx response"),
        };
        assert_eq!(
            err.http_status(),
            Some(416),
            "unsatisfiable range ({label}) must be HTTP 416, got: {err:?}"
        );
        assert_eq!(
            err.code(),
            Some(&S3ErrorCode::InvalidRange),
            "unsatisfiable range ({label}) must be typed InvalidRange \
             (the §2.4 resume-restart trigger), got: {err:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// 4. ListObjectsV2: paging with continuation tokens; prefix + delimiter
// ---------------------------------------------------------------------------

#[tokio::test]
async fn list_objects_v2_paging_prefix_and_delimiter() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("paging");
    let client = g.client();

    let mut expected: Vec<String> = Vec::new();
    for i in 0..30 {
        let key = format!("paged/obj-{i:03}.dat");
        client
            .put_object(
                &bucket,
                &key,
                pattern(64, i as u8),
                &PutObjectOptions::default(),
            )
            .await
            .expect("put paged object");
        expected.push(key);
    }
    for key in [
        "grouped/a/one",
        "grouped/a/two",
        "grouped/b/one",
        "grouped/top",
    ] {
        client
            .put_object(&bucket, key, pattern(16, 9), &PutObjectOptions::default())
            .await
            .expect("put grouped object");
    }
    expected.sort();

    // Manual paging: max-keys=10 must take >= 3 pages for the 30 objects.
    let mut collected: Vec<String> = Vec::new();
    let mut token: Option<String> = None;
    let mut pages = 0u32;
    loop {
        let page = client
            .list_objects_v2(
                &bucket,
                &ListObjectsV2Request {
                    prefix: Some("paged/".to_string()),
                    max_keys: Some(10),
                    continuation_token: token.clone(),
                    ..Default::default()
                },
            )
            .await
            .expect("list page");
        pages += 1;
        assert!(page.objects.len() <= 10, "max-keys must cap the page");
        collected.extend(page.objects.iter().map(|o| o.key.clone()));
        if page.is_truncated {
            token = Some(
                page.next_continuation_token
                    .expect("truncated page must carry a continuation token"),
            );
        } else {
            break;
        }
        assert!(pages < 100, "runaway paging loop");
    }
    assert!(
        pages >= 3,
        "30 keys at max-keys=10 require >= 3 pages, got {pages}"
    );
    assert_eq!(
        collected, expected,
        "paged listing must be complete and in sorted key order"
    );

    // The paging helper must agree with manual paging.
    let all = client
        .list_all_objects(&bucket, Some("paged/"))
        .await
        .expect("list_all_objects");
    assert_eq!(
        all.iter().map(|o| o.key.clone()).collect::<Vec<_>>(),
        expected
    );
    for o in &all {
        assert_eq!(o.size, 64, "listed size for {}", o.key);
    }

    // start-after: listing must begin strictly after the given key.
    let after = client
        .list_objects_v2(
            &bucket,
            &ListObjectsV2Request {
                prefix: Some("paged/".to_string()),
                start_after: Some("paged/obj-024.dat".to_string()),
                ..Default::default()
            },
        )
        .await
        .expect("start-after listing");
    assert_eq!(
        after
            .objects
            .iter()
            .map(|o| o.key.as_str())
            .collect::<Vec<_>>(),
        expected
            .iter()
            .filter(|k| k.as_str() > "paged/obj-024.dat")
            .map(String::as_str)
            .collect::<Vec<_>>(),
        "start-after must resume strictly after the given key"
    );

    // Prefix + delimiter grouping.
    let grouped = client
        .list_objects_v2(
            &bucket,
            &ListObjectsV2Request {
                prefix: Some("grouped/".to_string()),
                delimiter: Some("/".to_string()),
                ..Default::default()
            },
        )
        .await
        .expect("delimiter listing");
    assert_eq!(
        grouped.common_prefixes,
        vec!["grouped/a/".to_string(), "grouped/b/".to_string()]
    );
    assert_eq!(
        grouped
            .objects
            .iter()
            .map(|o| o.key.as_str())
            .collect::<Vec<_>>(),
        vec!["grouped/top"],
        "keys under grouped sub-prefixes must not leak into Contents"
    );
}

// ---------------------------------------------------------------------------
// 5. Multipart: 2 x 5 MiB parts + small final part, per-part Content-MD5
// ---------------------------------------------------------------------------

#[tokio::test]
async fn multipart_upload_round_trip() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("multipart");
    let client = g.client();

    const FIVE_MIB: usize = 5 * 1024 * 1024;
    let p1 = pattern(FIVE_MIB, 11);
    let p2 = pattern(FIVE_MIB, 22);
    let p3 = pattern(1024, 33);

    let created = client
        .create_multipart_upload(
            &bucket,
            "assembled.raw",
            &PutObjectOptions {
                content_type: Some("image/x-raw".to_string()),
                metadata: meta(&[("rr-blake3", "deadbeef")]),
                ..Default::default()
            },
        )
        .await
        .expect("create_multipart_upload");
    assert!(!created.upload_id.is_empty());

    let mut parts: Vec<CompletedPart> = Vec::new();

    // Part 1: buffered body.
    let out1 = client
        .upload_part(
            &bucket,
            "assembled.raw",
            &created.upload_id,
            1,
            PartBody::from(p1.clone()),
            Some(&md5_b64(&p1)),
        )
        .await
        .expect("upload part 1");
    assert_eq!(out1.e_tag, md5_hex(&p1), "part ETag is the part's MD5");
    parts.push(CompletedPart {
        part_number: 1,
        e_tag: out1.e_tag,
    });

    // Part 2: streamed body (the UNSIGNED-PAYLOAD path), chunked.
    let chunks: Vec<Result<Bytes, std::io::Error>> = p2
        .chunks(64 * 1024)
        .map(|c| Ok(Bytes::copy_from_slice(c)))
        .collect();
    let stream = futures::stream::iter(chunks).boxed();
    let out2 = client
        .upload_part(
            &bucket,
            "assembled.raw",
            &created.upload_id,
            2,
            PartBody::from_stream(stream, p2.len() as u64),
            Some(&md5_b64(&p2)),
        )
        .await
        .expect("upload part 2 (streamed)");
    assert_eq!(out2.e_tag, md5_hex(&p2));
    parts.push(CompletedPart {
        part_number: 2,
        e_tag: out2.e_tag,
    });

    // Final small part.
    let out3 = client
        .upload_part(
            &bucket,
            "assembled.raw",
            &created.upload_id,
            3,
            PartBody::from(p3.clone()),
            Some(&md5_b64(&p3)),
        )
        .await
        .expect("upload part 3");
    parts.push(CompletedPart {
        part_number: 3,
        e_tag: out3.e_tag,
    });

    let completed = client
        .complete_multipart_upload(&bucket, "assembled.raw", &created.upload_id, &parts)
        .await
        .expect("complete_multipart_upload");
    assert!(
        completed.e_tag.ends_with("-3"),
        "multipart ETag carries the part count: {}",
        completed.e_tag
    );

    // GET and hash-compare the assembled object.
    let mut full = Vec::with_capacity(FIVE_MIB * 2 + 1024);
    full.extend_from_slice(&p1);
    full.extend_from_slice(&p2);
    full.extend_from_slice(&p3);

    let got = client
        .get_object(&bucket, "assembled.raw", None)
        .await
        .expect("get assembled");
    assert_eq!(got.content_length, full.len() as u64);
    let got_bytes = got.body.collect().await.expect("collect assembled");
    assert_eq!(
        sha256_hex(&got_bytes),
        sha256_hex(&full),
        "assembled object must be byte-identical to the concatenated parts"
    );

    // Metadata echo (§3.9 named conformance requirement): the x-amz-meta-*
    // and Content-Type given at CreateMultipartUpload must survive
    // CompleteMultipartUpload onto the assembled object — the §2.4
    // verifying/attestation flow reads x-amz-meta on multipart objects.
    let head = client
        .head_object(&bucket, "assembled.raw")
        .await
        .expect("head assembled");
    assert_eq!(head.content_length, full.len() as u64);
    assert_eq!(
        head.metadata.get("rr-blake3").map(String::as_str),
        Some("deadbeef"),
        "x-amz-meta set at CreateMultipartUpload must be echoed on HEAD of \
         the completed object; full metadata: {:?}",
        head.metadata
    );
    assert_eq!(
        head.content_type.as_deref(),
        Some("image/x-raw"),
        "Content-Type set at CreateMultipartUpload must survive completion"
    );
}

// ---------------------------------------------------------------------------
// 6. The §2.4 integrity probe: a deliberately wrong Content-MD5 must be
//    rejected with a typed digest error (BadDigest on AWS; Garage v2.2.0
//    answers InvalidDigest — verified empirically, both covered by
//    S3ErrorCode::is_digest_rejection).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn wrong_content_md5_is_rejected_as_typed_digest_error() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("baddigest");
    let client = g.client();

    let body = pattern(2048, 5);
    // A well-formed base64 MD5 — of *different* bytes.
    let wrong_md5 = md5_b64(b"not the body at all");
    assert_ne!(wrong_md5, md5_b64(&body));

    // --- small put_object ---
    let result = client
        .put_object(
            &bucket,
            "probe.bin",
            body.clone(),
            &PutObjectOptions {
                content_md5: Some(wrong_md5.clone()),
                ..Default::default()
            },
        )
        .await;
    match result {
        Err(S3Error::Api {
            status,
            code,
            message,
            ..
        }) => {
            assert!(
                code.is_digest_rejection(),
                "expected a digest rejection, got code {code:?} (HTTP {status}): {message}"
            );
            assert_eq!(status, 400, "digest rejection must be HTTP 400");
            if code != S3ErrorCode::BadDigest {
                eprintln!(
                    "=== CONFORMANCE NOTE ===============================================\n\
                     Backend rejects a wrong Content-MD5 on PutObject with code \
                     {code:?} (HTTP {status}) instead of AWS's BadDigest.\n\
                     Garage v2.2.0 is known to answer InvalidDigest; \
                     S3ErrorCode::is_digest_rejection treats both as the same probe \
                     outcome, so the §2.4 integrity probe works unchanged and \
                     requires_readback_verify is NOT needed for this backend.\n\
                     ===================================================================="
                );
            }
        }
        Err(other) => panic!("expected a typed API error, got: {other:?}"),
        Ok(out) => panic!(
            "backend ACCEPTED a wrong Content-MD5 on PutObject (ETag {}). \
             Garage v2.2.0 was empirically verified to reject wrong digests, so \
             either the backend changed or Content-MD5 was not sent. A backend \
             that truly accepts wrong digests requires the design's \
             requires_readback_verify fallback (§2.4) — update this test if \
             that backend is ever targeted.",
            out.e_tag
        ),
    }
    // The failed PUT must not have created the object.
    let err = client
        .get_object(&bucket, "probe.bin", None)
        .await
        .expect_err("rejected PUT must not create the key");
    assert!(err.is_no_such_key(), "got: {err:?}");

    // --- upload_part ---
    let created = client
        .create_multipart_upload(&bucket, "probe-mp.bin", &PutObjectOptions::default())
        .await
        .expect("create multipart");
    let part_result = client
        .upload_part(
            &bucket,
            "probe-mp.bin",
            &created.upload_id,
            1,
            PartBody::from(body.clone()),
            Some(&wrong_md5),
        )
        .await;
    match part_result {
        Err(S3Error::Api {
            status,
            code,
            message,
            ..
        }) => {
            assert!(
                code.is_digest_rejection(),
                "expected a digest rejection on UploadPart, got {code:?} \
                 (HTTP {status}): {message}"
            );
            assert_eq!(status, 400);
        }
        Err(other) => panic!("expected a typed API error on UploadPart, got: {other:?}"),
        Ok(out) => panic!(
            "backend ACCEPTED a wrong Content-MD5 on UploadPart (ETag {}) — \
             see the PutObject arm above for what that would mean.",
            out.e_tag
        ),
    }

    // --- upload_part, STREAMED body (UNSIGNED-PAYLOAD) ---
    // This is the production path for large parts (§2.4/§3.9): the payload
    // is NOT covered by the SigV4 signature, so Content-MD5 is the sole
    // server-side integrity check. A backend that verifies digests only for
    // signed bodies — or a client regression that drops the header on only
    // the streamed branch — must be caught here.
    let chunks: Vec<Result<Bytes, std::io::Error>> = body
        .chunks(512)
        .map(|c| Ok(Bytes::copy_from_slice(c)))
        .collect();
    let streamed_result = client
        .upload_part(
            &bucket,
            "probe-mp.bin",
            &created.upload_id,
            2,
            PartBody::from_stream(futures::stream::iter(chunks).boxed(), body.len() as u64),
            Some(&wrong_md5),
        )
        .await;
    match streamed_result {
        Err(S3Error::Api {
            status,
            code,
            message,
            ..
        }) => {
            assert!(
                code.is_digest_rejection(),
                "expected a digest rejection on a STREAMED (UNSIGNED-PAYLOAD) \
                 UploadPart, got {code:?} (HTTP {status}): {message}"
            );
            assert_eq!(status, 400);
        }
        Err(other) => {
            panic!("expected a typed API error on the streamed UploadPart, got: {other:?}")
        }
        Ok(out) => panic!(
            "backend ACCEPTED a wrong Content-MD5 on a STREAMED UNSIGNED-PAYLOAD \
             UploadPart (ETag {}). That means real uploads get ZERO server-side \
             integrity verification on this backend and the design's \
             requires_readback_verify fallback (§2.4) is mandatory — update this \
             test if such a backend is ever targeted.",
            out.e_tag
        ),
    }
    // Sanity: the same streamed path with the CORRECT MD5 succeeds, proving
    // the rejection above was about the digest and not the request shape.
    let chunks: Vec<Result<Bytes, std::io::Error>> = body
        .chunks(512)
        .map(|c| Ok(Bytes::copy_from_slice(c)))
        .collect();
    let ok = client
        .upload_part(
            &bucket,
            "probe-mp.bin",
            &created.upload_id,
            2,
            PartBody::from_stream(futures::stream::iter(chunks).boxed(), body.len() as u64),
            Some(&md5_b64(&body)),
        )
        .await
        .expect("streamed part with correct MD5");
    assert_eq!(ok.e_tag, md5_hex(&body));

    client
        .abort_multipart_upload(&bucket, "probe-mp.bin", &created.upload_id)
        .await
        .expect("abort probe upload");
}

// ---------------------------------------------------------------------------
// 7. abort_multipart_upload + list_multipart_uploads + list_parts
// ---------------------------------------------------------------------------

#[tokio::test]
async fn abort_and_list_multipart_uploads_and_parts() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("mp-admin");
    let client = g.client();

    let live = client
        .create_multipart_upload(&bucket, "upload-live", &PutObjectOptions::default())
        .await
        .expect("create live upload");
    let doomed = client
        .create_multipart_upload(&bucket, "upload-doomed", &PutObjectOptions::default())
        .await
        .expect("create doomed upload");
    assert_ne!(live.upload_id, doomed.upload_id);

    // One part on the live upload.
    let part = pattern(2048, 77);
    let uploaded = client
        .upload_part(
            &bucket,
            "upload-live",
            &live.upload_id,
            1,
            PartBody::from(part.clone()),
            Some(&md5_b64(&part)),
        )
        .await
        .expect("upload part on live upload");

    // Abort the other one.
    client
        .abort_multipart_upload(&bucket, "upload-doomed", &doomed.upload_id)
        .await
        .expect("abort doomed upload");

    // Listing must now show exactly the live upload.
    let listing = client
        .list_multipart_uploads(&bucket, &ListMultipartUploadsRequest::default())
        .await
        .expect("list_multipart_uploads");
    let summaries: Vec<(&str, &str)> = listing
        .uploads
        .iter()
        .map(|u| (u.key.as_str(), u.upload_id.as_str()))
        .collect();
    assert_eq!(
        summaries,
        vec![("upload-live", live.upload_id.as_str())],
        "after aborting one of two uploads, exactly the other must remain"
    );

    // list_parts on the live upload shows the one uploaded part.
    let parts = client
        .list_parts(
            &bucket,
            "upload-live",
            &live.upload_id,
            &ListPartsRequest::default(),
        )
        .await
        .expect("list_parts");
    assert_eq!(parts.parts.len(), 1);
    assert_eq!(parts.parts[0].part_number, 1);
    assert_eq!(parts.parts[0].size, part.len() as u64);
    assert_eq!(parts.parts[0].e_tag, uploaded.e_tag);

    // Cleanup: abort the live one too; listing becomes empty.
    client
        .abort_multipart_upload(&bucket, "upload-live", &live.upload_id)
        .await
        .expect("abort live upload");
    let empty = client
        .list_multipart_uploads(&bucket, &ListMultipartUploadsRequest::default())
        .await
        .expect("list after both aborted");
    assert!(empty.uploads.is_empty(), "{:?}", empty.uploads);
}

// ---------------------------------------------------------------------------
// 4b. Listing resilience: encoding-type=url is requested so keys containing
//     XML-hostile characters (control chars) or encoding-trap characters
//     ('%', '+') survive the listing round-trip on every backend.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn listing_round_trips_control_and_encoding_trap_keys() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("enclist");
    let client = g.client();

    let keys = [
        "weird/\u{1}ctl.bin",       // invalid raw in XML 1.0
        "weird/percent%41.bin",     // literal '%' must not double-decode
        "weird/plus+and space.bin", // '+' vs space form-encoding trap
    ];
    for (i, key) in keys.iter().enumerate() {
        client
            .put_object(
                &bucket,
                key,
                pattern(256, 50 + i as u8),
                &PutObjectOptions::default(),
            )
            .await
            .unwrap_or_else(|e| panic!("put {key:?}: {e:?}"));
    }

    let mut expected: Vec<&str> = keys.to_vec();
    expected.sort_unstable();
    let listed = client
        .list_all_objects(&bucket, Some("weird/"))
        .await
        .expect("list weird/");
    assert_eq!(
        listed.iter().map(|o| o.key.as_str()).collect::<Vec<_>>(),
        expected,
        "keys must round-trip through the URL-encoded listing exactly"
    );

    // Each listed key must address the object it names.
    for o in &listed {
        client
            .head_object(&bucket, &o.key)
            .await
            .unwrap_or_else(|e| panic!("head of listed key {:?}: {e:?}", o.key));
    }

    // Delimiter grouping must decode common prefixes too.
    let grouped = client
        .list_objects_v2(
            &bucket,
            &ListObjectsV2Request {
                delimiter: Some("/".to_string()),
                ..Default::default()
            },
        )
        .await
        .expect("delimiter listing");
    assert_eq!(grouped.common_prefixes, vec!["weird/".to_string()]);
}

// ---------------------------------------------------------------------------
// 7b. ListMultipartUploads / ListParts paging with markers (the §2.4 sweep
//     and resume paths must be able to continue truncated pages).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn multipart_listings_page_with_markers() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("mp-paging");
    let client = g.client();

    // Three in-progress uploads; page through them two at a time.
    let keys = ["page/mp-a", "page/mp-b", "page/mp-c"];
    let mut expected: Vec<(String, String)> = Vec::new();
    for key in keys {
        let created = client
            .create_multipart_upload(&bucket, key, &PutObjectOptions::default())
            .await
            .expect("create upload");
        expected.push((key.to_string(), created.upload_id));
    }
    expected.sort();

    let mut seen: Vec<(String, String)> = Vec::new();
    let mut key_marker: Option<String> = None;
    let mut upload_id_marker: Option<String> = None;
    let mut pages = 0u32;
    loop {
        let page = client
            .list_multipart_uploads(
                &bucket,
                &ListMultipartUploadsRequest {
                    max_uploads: Some(2),
                    key_marker: key_marker.take(),
                    upload_id_marker: upload_id_marker.take(),
                    ..Default::default()
                },
            )
            .await
            .expect("list uploads page");
        pages += 1;
        assert!(page.uploads.len() <= 2, "max-uploads must cap the page");
        seen.extend(
            page.uploads
                .iter()
                .map(|u| (u.key.clone(), u.upload_id.clone())),
        );
        if page.is_truncated {
            key_marker = Some(
                page.next_key_marker
                    .expect("a truncated ListMultipartUploads page must carry NextKeyMarker"),
            );
            // Garage v2.2.0 omits NextUploadIdMarker and pages on the key
            // marker alone; pass it through when a backend provides it.
            upload_id_marker = page.next_upload_id_marker;
        } else {
            break;
        }
        assert!(pages < 20, "runaway upload-paging loop");
    }
    assert!(pages >= 2, "3 uploads at max-uploads=2 require >= 2 pages");
    // No dedup here: a marker off-by-one that re-lists an upload on the next
    // page must fail this assertion, not be silently erased.
    seen.sort();
    assert_eq!(
        seen, expected,
        "paged upload listing must be complete, with every upload listed \
         exactly once across pages"
    );

    // Three parts on one upload; page through them two at a time.
    let (first_key, first_id) = &expected[0];
    let mut part_bodies = Vec::new();
    for n in 1..=3u32 {
        let body = pattern(1024, 100 + n as u8);
        client
            .upload_part(
                &bucket,
                first_key,
                first_id,
                n,
                PartBody::from(body.clone()),
                Some(&md5_b64(&body)),
            )
            .await
            .expect("upload part");
        part_bodies.push(body);
    }

    let mut part_numbers: Vec<u32> = Vec::new();
    let mut marker: Option<u32> = None;
    let mut pages = 0u32;
    loop {
        let page = client
            .list_parts(
                &bucket,
                first_key,
                first_id,
                &ListPartsRequest {
                    max_parts: Some(2),
                    part_number_marker: marker.take(),
                },
            )
            .await
            .expect("list parts page");
        pages += 1;
        assert!(page.parts.len() <= 2, "max-parts must cap the page");
        let already_seen = part_numbers.len().min(part_bodies.len());
        for (part, body) in page.parts.iter().zip(&part_bodies[already_seen..]) {
            assert_eq!(part.size, body.len() as u64);
            assert_eq!(part.e_tag, md5_hex(body));
        }
        part_numbers.extend(page.parts.iter().map(|p| p.part_number));
        if page.is_truncated {
            marker = Some(
                page.next_part_number_marker
                    .expect("a truncated ListParts page must carry NextPartNumberMarker"),
            );
        } else {
            break;
        }
        assert!(pages < 20, "runaway part-paging loop");
    }
    assert!(pages >= 2, "3 parts at max-parts=2 require >= 2 pages");
    assert_eq!(
        part_numbers,
        vec![1, 2, 3],
        "paged parts must be complete and in ascending part-number order"
    );

    // Cleanup.
    for (key, id) in &expected {
        client
            .abort_multipart_upload(&bucket, key, id)
            .await
            .expect("abort");
    }
}

// ---------------------------------------------------------------------------
// 7c. ListMultipartUploads / ListParts request encoding-type=url (like
//     ListObjectsV2), so upload keys containing XML-hostile or
//     encoding-trap characters survive the §2.4 stale-upload sweep and the
//     resume path's list_parts on every backend.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn multipart_listings_round_trip_encoded_keys() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("mp-enc");
    let client = g.client();

    let keys = [
        "enc/Käch photos/part 0042+1.NEF", // unicode, space, '+'
        "enc/\u{1}ctl.bin",                // invalid raw in XML 1.0
        "enc/percent%41.bin",              // literal '%' must not double-decode
    ];
    let mut expected: Vec<(String, String)> = Vec::new();
    for key in keys {
        let created = client
            .create_multipart_upload(&bucket, key, &PutObjectOptions::default())
            .await
            .unwrap_or_else(|e| panic!("create upload {key:?}: {e:?}"));
        expected.push((key.to_string(), created.upload_id));
    }
    expected.sort();

    // The decoded keys must round-trip through the listing exactly.
    let listing = client
        .list_multipart_uploads(&bucket, &ListMultipartUploadsRequest::default())
        .await
        .expect("list_multipart_uploads");
    let mut seen: Vec<(String, String)> = listing
        .uploads
        .iter()
        .map(|u| (u.key.clone(), u.upload_id.clone()))
        .collect();
    seen.sort();
    assert_eq!(
        seen, expected,
        "multipart upload keys must round-trip through the URL-encoded listing"
    );

    // Paging with a decoded NextKeyMarker must keep working: each listed key
    // (as returned) must address its upload when passed back as the marker.
    let mut paged: Vec<(String, String)> = Vec::new();
    let mut key_marker: Option<String> = None;
    let mut upload_id_marker: Option<String> = None;
    let mut pages = 0u32;
    loop {
        let page = client
            .list_multipart_uploads(
                &bucket,
                &ListMultipartUploadsRequest {
                    max_uploads: Some(1),
                    key_marker: key_marker.take(),
                    upload_id_marker: upload_id_marker.take(),
                    ..Default::default()
                },
            )
            .await
            .expect("list uploads page");
        pages += 1;
        paged.extend(
            page.uploads
                .iter()
                .map(|u| (u.key.clone(), u.upload_id.clone())),
        );
        if page.is_truncated {
            key_marker = Some(page.next_key_marker.expect("truncated page needs marker"));
            upload_id_marker = page.next_upload_id_marker;
        } else {
            break;
        }
        assert!(pages < 20, "runaway paging loop");
    }
    paged.sort();
    assert_eq!(
        paged, expected,
        "paging on a (decoded) NextKeyMarker over encoded keys must visit \
         every upload exactly once"
    );

    // list_parts on an upload with a nasty key must work too (its <Key> echo
    // is what encoding-type=url protects).
    let (nasty_key, nasty_id) = expected
        .iter()
        .find(|(k, _)| k.contains('\u{1}'))
        .expect("control-char upload present");
    let part = pattern(1024, 91);
    let uploaded = client
        .upload_part(
            &bucket,
            nasty_key,
            nasty_id,
            1,
            PartBody::from(part.clone()),
            Some(&md5_b64(&part)),
        )
        .await
        .expect("upload part on control-char key");
    let parts = client
        .list_parts(&bucket, nasty_key, nasty_id, &ListPartsRequest::default())
        .await
        .expect("list_parts on control-char key");
    assert_eq!(parts.parts.len(), 1);
    assert_eq!(parts.parts[0].e_tag, uploaded.e_tag);
    assert_eq!(parts.parts[0].size, part.len() as u64);

    for (key, id) in &expected {
        client
            .abort_multipart_upload(&bucket, key, id)
            .await
            .expect("abort");
    }
}

// ---------------------------------------------------------------------------
// 8. copy_object + delete; typed NoSuchKey / NoSuchBucket errors
// ---------------------------------------------------------------------------

#[tokio::test]
async fn copy_object_delete_and_typed_missing_errors() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("copydel");
    let client = g.client();

    let body = pattern(8192, 3);
    client
        .put_object(
            &bucket,
            "src/original.dat",
            body.clone(),
            &PutObjectOptions::default(),
        )
        .await
        .expect("put source");

    let copied = client
        .copy_object(&bucket, "src/original.dat", &bucket, "dst/copied.dat")
        .await
        .expect("copy_object");
    assert_eq!(
        copied.e_tag,
        md5_hex(&body),
        "copy ETag matches the content"
    );

    let got = client
        .get_object(&bucket, "dst/copied.dat", None)
        .await
        .expect("get copy");
    assert_eq!(got.body.collect().await.expect("collect copy"), body);

    // Delete the source; the copy must survive.
    client
        .delete_object(&bucket, "src/original.dat")
        .await
        .expect("delete source");
    client
        .head_object(&bucket, "dst/copied.dat")
        .await
        .expect("copy survives source deletion");

    // Copy with a source key that exercises the x-amz-copy-source
    // percent-encoding path (spaces, '+', unicode).
    let nasty_src = "src/Käch photos/IMG 0042+1.NEF";
    let nasty_body = pattern(4096, 17);
    client
        .put_object(
            &bucket,
            nasty_src,
            nasty_body.clone(),
            &PutObjectOptions::default(),
        )
        .await
        .expect("put nasty source");
    let nasty_copy = client
        .copy_object(&bucket, nasty_src, &bucket, "dst/nasty übercopy+1.NEF")
        .await
        .expect("copy with encoded source key");
    assert_eq!(nasty_copy.e_tag, md5_hex(&nasty_body));
    let got = client
        .get_object(&bucket, "dst/nasty übercopy+1.NEF", None)
        .await
        .expect("get nasty copy");
    assert_eq!(
        got.body.collect().await.expect("collect nasty copy"),
        nasty_body
    );

    // GET of the deleted source: typed NoSuchKey with status 404.
    let err = client
        .get_object(&bucket, "src/original.dat", None)
        .await
        .expect_err("get deleted source");
    assert!(err.is_no_such_key(), "got: {err:?}");
    assert_eq!(err.http_status(), Some(404));
    assert_eq!(err.code(), Some(&S3ErrorCode::NoSuchKey));

    // HEAD of a missing key: 404 with no body — still typed NoSuchKey.
    let err = client
        .head_object(&bucket, "never/existed")
        .await
        .expect_err("head missing");
    assert!(err.is_no_such_key(), "got: {err:?}");

    // GET against a bucket that does not exist: typed NoSuchBucket.
    let err = client
        .get_object("rrcloud-no-such-bucket-xyz", "any", None)
        .await
        .expect_err("get in missing bucket");
    assert!(err.is_no_such_bucket(), "got: {err:?}");
    assert_eq!(err.http_status(), Some(404));

    // Deleting an already-absent key is a success (S3 semantics).
    client
        .delete_object(&bucket, "src/original.dat")
        .await
        .expect("double delete is idempotent");
}
