//! Failing tests for `rrcloud_core::transfer` (architecture §2.4 upload
//! state machine, §3.5 download/hydration, §2.1.5 durability order): the
//! backend digest probe, single-PUT and multipart uploads with streamed
//! blake3 truth, crash-free resume, source-change abort, stale-upload
//! hygiene, verify/readback, the atomic verify+journal commit, resumable
//! verified downloads, and the bounded-concurrency queue pump.
//!
//! Garage-backed (shared instance, per-test buckets) except the pure
//! staging/atomicity tests. The child-process crash-injection scenario
//! lives in `tests/transfer_crash.rs`.

mod common;

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::Path;

use bytes::Bytes;
use common::garage;
use common::sync::{dev, entry, open_db, rel, DEV_B};
use common::transfer as h;
use common::transfer::CountingS3;
use rrcloud_core::journal::{JournalEntry, Kind, Op, SEGMENT_MAX_BYTES};
use rrcloud_core::keys::{library_key, sidecar_key};
use rrcloud_core::publisher::{enqueue_entry, enqueue_entry_in, PublisherError};
use rrcloud_core::s3::{
    ByteRange, CompletedPart, ListMultipartUploadsRequest, ListPartsRequest, PartBody,
    PutObjectOptions, S3TransferApi,
};
use rrcloud_core::semhash::{sem_hash, Blake3Hex, ContentId};
use rrcloud_core::state::{ItemState, MultipartUploadState, Queue, SyncDb};
use rrcloud_core::transfer::{
    abort_stale_uploads, bucket_key_for, commit_verified, download_item, local_target_path,
    partial_path, probe_backend, pump_downloads, pump_uploads, stored_backend_profile, upload_item,
    upload_item_from, BackendProfile, CancelFlag, ExpectedDownload, TransferConfig, TransferError,
    PROBE_KEY,
};

/// Decodes every staged outbound journal entry.
fn staged_entries(db: &SyncDb) -> Vec<JournalEntry> {
    db.iter_outbound()
        .expect("iter_outbound")
        .into_iter()
        .map(|(_, bytes)| {
            JournalEntry::from_json_line(std::str::from_utf8(&bytes).expect("utf8 staged entry"))
                .expect("staged entry decodes")
        })
        .collect()
}

fn outbound_len(db: &SyncDb) -> u64 {
    db.outbound_len().expect("outbound_len")
}

/// A `(garage, bucket, tempdir, db)` scaffold; the db's device is DEV_A.
macro_rules! scaffold {
    ($tag:expr) => {{
        let Some(g) = garage::shared() else {
            return;
        };
        let bucket = g.create_unique_bucket($tag);
        let root = tempfile::tempdir().expect("tempdir");
        let (dbdir, _dbpath, db) = open_db(&dev(common::sync::DEV_A));
        (g, bucket, root, dbdir, db)
    }};
}

// ===========================================================================
// Additive extensions: state meta + publisher txn staging + S3 trait
// ===========================================================================

#[test]
fn backend_digest_rejection_round_trips_through_meta() {
    let (_dir, _path, db) = open_db(&dev(common::sync::DEV_A));
    assert_eq!(
        db.backend_digest_rejection().expect("read"),
        None,
        "no probe has run yet"
    );
    db.set_backend_digest_rejection(true).expect("persist true");
    assert_eq!(db.backend_digest_rejection().expect("read"), Some(true));
    // Re-probing must overwrite (a reconfigured backend).
    db.set_backend_digest_rejection(false)
        .expect("persist false");
    assert_eq!(db.backend_digest_rejection().expect("read"), Some(false));

    let profile = stored_backend_profile(&db)
        .expect("stored profile")
        .expect("present");
    assert!(!profile.digest_rejection_works);
    assert!(profile.requires_readback_verify());
}

#[test]
fn enqueue_entry_in_refuses_foreign_entries_and_stages_nothing() {
    let (_dir, _path, db) = open_db(&dev(common::sync::DEV_A));
    let foreign = entry(
        &dev(DEV_B),
        Op::Put,
        Kind::Sidecar,
        sidecar_key(&rel("x/a.NEF")),
    );
    let result = db.with_txn_err::<_, PublisherError>(|t| {
        enqueue_entry_in(t, db.device_id(), &foreign).map(|_| ())
    });
    match result {
        Err(PublisherError::ForeignDevice { .. }) => {}
        other => panic!("expected ForeignDevice, got {other:?}"),
    }
    assert_eq!(outbound_len(&db), 0, "nothing staged");
}

#[test]
fn enqueue_entry_in_stages_byte_identical_to_enqueue_entry() {
    let a = dev(common::sync::DEV_A);
    let (_d1, _p1, db1) = open_db(&a);
    let (_d2, _p2, db2) = open_db(&a);
    let mut e = entry(&a, Op::Put, Kind::Sidecar, sidecar_key(&rel("x/b.NEF")));
    e.blake3 = Some(h::b3(b"some bytes"));
    e.size = Some(10);
    // Junk in the stamped-at-publication fields must normalize identically.
    e.seq = 777;
    e.v = 9;

    enqueue_entry(&db1, &e).expect("enqueue via db entry point");
    db2.with_txn_err::<_, PublisherError>(|t| enqueue_entry_in(t, db2.device_id(), &e).map(|_| ()))
        .expect("enqueue via txn entry point");

    let staged1 = db1.iter_outbound().expect("outbound 1");
    let staged2 = db2.iter_outbound().expect("outbound 2");
    assert_eq!(staged1.len(), 1);
    assert_eq!(staged2.len(), 1);
    assert_eq!(
        staged1[0].1, staged2[0].1,
        "both entry points must stage byte-identical records"
    );
}

#[tokio::test]
async fn s3_transfer_api_delegates_the_multipart_lifecycle() {
    let Some(g) = garage::shared() else {
        return;
    };
    let bucket = g.create_unique_bucket("tr-trait");
    let client = g.client();

    // Drive everything through the trait, generically — pinning that the
    // S3Client impl is real delegation.
    async fn drive(s3: &impl S3TransferApi, bucket: &str) {
        let key = "library/trait/roundtrip.bin";
        let part = h::patterned(h::PART_5MIB as usize, 11);

        let created = s3
            .create_multipart_upload(bucket, key, &PutObjectOptions::default())
            .await
            .expect("create");
        let md5 = h::md5_b64(&part);
        let up = s3
            .upload_part(
                bucket,
                key,
                &created.upload_id,
                1,
                PartBody::from(Bytes::from(part.clone())),
                Some(&md5),
            )
            .await
            .expect("upload_part");
        assert_eq!(up.e_tag, h::md5_hex(&part));

        let listed = s3
            .list_parts(
                bucket,
                key,
                &created.upload_id,
                &ListPartsRequest::default(),
            )
            .await
            .expect("list_parts");
        assert_eq!(listed.parts.len(), 1);
        assert_eq!(listed.parts[0].part_number, 1);
        assert_eq!(listed.parts[0].e_tag, h::md5_hex(&part));

        s3.complete_multipart_upload(
            bucket,
            key,
            &created.upload_id,
            &[CompletedPart {
                part_number: 1,
                e_tag: up.e_tag.clone(),
            }],
        )
        .await
        .expect("complete");
        let head = s3.head_object(bucket, key).await.expect("head");
        assert_eq!(head.content_length, part.len() as u64);

        s3.delete_object(bucket, key).await.expect("delete");
        let gone = s3.head_object(bucket, key).await;
        assert!(
            gone.expect_err("object deleted").is_no_such_key(),
            "delete_object must remove the key"
        );

        let orphan = s3
            .create_multipart_upload(bucket, key, &PutObjectOptions::default())
            .await
            .expect("create 2");
        s3.abort_multipart_upload(bucket, key, &orphan.upload_id)
            .await
            .expect("abort");
        let uploads = s3
            .list_multipart_uploads(bucket, &ListMultipartUploadsRequest::default())
            .await
            .expect("list uploads");
        assert!(uploads.uploads.is_empty(), "abort must leave no uploads");
    }
    drive(&client, &bucket).await;
}

// ===========================================================================
// §2.4 setup probe
// ===========================================================================

#[tokio::test]
async fn probe_reports_digest_rejection_on_garage_and_persists_it() {
    let (g, bucket, _root, _dbdir, db) = scaffold!("tr-probe-garage");
    let client = g.client();

    let profile = probe_backend(&db, &client, &bucket).await.expect("probe");
    assert!(
        profile.digest_rejection_works,
        "Garage v2.2.0 rejects a wrong Content-MD5 (InvalidDigest)"
    );
    assert!(!profile.requires_readback_verify());
    assert_eq!(
        db.backend_digest_rejection().expect("read"),
        Some(true),
        "the probe outcome is persisted in meta"
    );
    // The probe object must not survive on either path.
    let head = client.head_object(&bucket, PROBE_KEY).await;
    assert!(
        head.expect_err("probe object cleaned up").is_no_such_key(),
        "no probe object left behind"
    );
}

#[tokio::test]
async fn probe_detects_a_digest_accepting_backend_and_cleans_up() {
    let (g, bucket, _root, _dbdir, db) = scaffold!("tr-probe-accept");
    // A backend that performs no digest verification: the wrapper strips
    // Content-MD5, so Garage stores the probe object instead of
    // rejecting it.
    let mut s3 = CountingS3::new(g.client());
    s3.strip_digests = true;

    let profile = probe_backend(&db, &s3, &bucket).await.expect("probe");
    assert!(
        !profile.digest_rejection_works,
        "an accepted wrong digest means requires_readback_verify"
    );
    assert!(profile.requires_readback_verify());
    assert_eq!(db.backend_digest_rejection().expect("read"), Some(false));
    // The accepted probe object must have been deleted.
    let head = g.client().head_object(&bucket, PROBE_KEY).await;
    assert!(
        head.expect_err("probe object cleaned up").is_no_such_key(),
        "accepted probe object must be deleted"
    );
}

// ===========================================================================
// Upload: single PUT
// ===========================================================================

#[tokio::test]
async fn single_put_sidecar_round_trips_etag_hash_state_and_journal() {
    let (g, bucket, root, _dbdir, db) = scaffold!("tr-single-sidecar");
    let s3 = CountingS3::new(g.client());
    let cfg = h::test_cfg(&bucket, root.path());

    let bytes = std::fs::read(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/sidecar_full.json"
    ))
    .expect("fixture");
    let r = rel("2026/10/IMG_0042.NEF");
    let src = h::under(root.path(), "IMG_0042.NEF.rrdata");
    h::write_file(&src, &bytes);
    h::seed_queued(&db, &r, Kind::Sidecar, &src);

    let outcome = upload_item(&db, &s3, &cfg, &r, &src).await.expect("upload");

    // ETag == md5hex (single PUT with Content-MD5, §2.4).
    assert_eq!(outcome.e_tag, h::md5_hex(&bytes));
    assert!(!outcome.multipart);
    assert_eq!(outcome.size, bytes.len() as u64);
    assert_eq!(outcome.blake3, h::b3(&bytes));
    assert_eq!(s3.put_keys(), vec![sidecar_key(&r)]);
    assert_eq!(
        s3.create_calls.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "below the threshold nothing goes multipart"
    );

    // What Garage stores is byte-identical, and its hash matches the
    // journal blake3.
    let stored = h::get_bytes(&g.client(), &bucket, &sidecar_key(&r)).await;
    assert_eq!(stored, bytes);

    // State: synced + verified_remote, blake3 recorded.
    let record = db.get_item(&r).expect("get").expect("exists");
    assert_eq!(record.state, ItemState::Synced);
    assert!(record.verified_remote);
    assert_eq!(record.blake3, Some(h::b3(&bytes)));

    // Journal entry staged with the per-kind §2.2 fields.
    let entries = staged_entries(&db);
    assert_eq!(entries.len(), 1, "exactly one journal put entry");
    let e = &entries[0];
    assert_eq!(e.op, Op::Put);
    assert_eq!(e.kind, Kind::Sidecar);
    assert_eq!(e.key, sidecar_key(&r));
    assert_eq!(e.device, *db.device_id());
    assert_eq!(e.blake3, Some(h::b3(&bytes)));
    assert_eq!(e.blake3, Some(Blake3Hex::from_bytes(&stored)));
    assert_eq!(e.size, Some(bytes.len() as u64));
    assert_eq!(
        e.sem_hash,
        Some(sem_hash(&bytes).expect("fixture has a sem_hash")),
        "sidecar entries carry sem_hash computed over the sent bytes"
    );
    assert_eq!(e.vv, record.vv, "entry snapshots the record's vv");
}

#[tokio::test]
async fn single_put_original_journals_content_id_and_mtime() {
    let (g, bucket, root, _dbdir, db) = scaffold!("tr-single-orig");
    let s3 = CountingS3::new(g.client());
    let cfg = h::test_cfg(&bucket, root.path());

    let bytes = h::patterned(300_000, 3);
    let r = rel("2026/10/IMG_0001.NEF");
    let src = h::under(root.path(), "IMG_0001.NEF");
    h::write_file(&src, &bytes);
    h::seed_queued(&db, &r, Kind::Original, &src);

    upload_item(&db, &s3, &cfg, &r, &src).await.expect("upload");

    let entries = staged_entries(&db);
    assert_eq!(entries.len(), 1);
    let e = &entries[0];
    assert_eq!(e.kind, Kind::Original);
    assert_eq!(e.key, library_key(&r));
    assert_eq!(e.blake3, Some(h::b3(&bytes)));
    assert_eq!(
        e.content_id,
        Some(ContentId::from_bytes(&bytes)),
        "original entries carry content_id == blake3(bytes) (§1.2)"
    );
    assert_eq!(
        e.mtime,
        Some(h::mtime_unix(&src)),
        "original entries carry the source mtime in unix seconds"
    );
}

// ===========================================================================
// Upload: multipart
// ===========================================================================

#[tokio::test]
async fn multipart_three_part_upload_round_trips_and_journals() {
    let (g, bucket, root, _dbdir, db) = scaffold!("tr-mp-roundtrip");
    let s3 = CountingS3::new(g.client());
    let cfg = h::test_cfg(&bucket, root.path());

    let bytes = h::three_part_bytes(7);
    let r = rel("2026/10/BIG_0001.NEF");
    let src = h::under(root.path(), "BIG_0001.NEF");
    h::write_file(&src, &bytes);
    h::seed_queued(&db, &r, Kind::Original, &src);

    let outcome = upload_item(&db, &s3, &cfg, &r, &src).await.expect("upload");

    assert!(outcome.multipart);
    assert_eq!(outcome.size, bytes.len() as u64);
    assert_eq!(outcome.blake3, h::b3(&bytes));
    assert_eq!(s3.create_calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(
        s3.part_attempts_for(&library_key(&r)),
        vec![1, 2, 3],
        "exactly three parts, ascending"
    );
    assert_eq!(
        s3.complete_calls.load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    assert!(s3.put_keys().is_empty(), "no single PUT for multipart");

    // Stored bytes hash to the journal blake3.
    let stored = h::get_bytes(&g.client(), &bucket, &library_key(&r)).await;
    assert_eq!(Blake3Hex::from_bytes(&stored), h::b3(&bytes));

    // Multipart bookkeeping cleared after success.
    assert_eq!(db.get_upload(&r).expect("get_upload"), None);
    assert!(db.upload_parts(&r).expect("upload_parts").is_empty());

    let record = db.get_item(&r).expect("get").expect("exists");
    assert_eq!(record.state, ItemState::Synced);
    assert!(record.verified_remote);

    let entries = staged_entries(&db);
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].blake3, Some(h::b3(&bytes)));
    assert_eq!(entries[0].content_id, Some(ContentId::from_bytes(&bytes)));
}

#[tokio::test]
async fn upload_state_is_persisted_before_the_first_part() {
    let (g, bucket, root, _dbdir, db) = scaffold!("tr-mp-persist-first");
    let key = library_key(&rel("mp/persist.NEF"));
    let s3 = CountingS3::new(g.client());
    // Part 1's first attempt fails without reaching the backend: whatever
    // survives in redb was committed BEFORE any part was sent.
    s3.fail_parts.lock().unwrap().insert((key.clone(), 1), 1);
    let cfg = h::test_cfg(&bucket, root.path());

    let bytes = h::three_part_bytes(9);
    let r = rel("mp/persist.NEF");
    let src = h::under(root.path(), "persist.NEF");
    h::write_file(&src, &bytes);
    h::seed_queued(&db, &r, Kind::Original, &src);

    upload_item(&db, &s3, &cfg, &r, &src)
        .await
        .expect_err("injected part-1 failure");

    let upload = db
        .get_upload(&r)
        .expect("get_upload")
        .expect("upload_id/part_size persisted before the first part (§2.4)");
    assert!(!upload.upload_id.is_empty());
    assert_eq!(upload.part_size, cfg.part_size);
    assert!(
        db.upload_parts(&r).expect("upload_parts").is_empty(),
        "no part completed, so no part records"
    );
    assert_eq!(
        h::state_of(&db, &r),
        ItemState::Queued,
        "transfer failure re-queues (uploading -> queued), resumable"
    );

    // A clean retry resumes the SAME upload id and completes.
    let clean = CountingS3::new(g.client());
    let outcome = upload_item(&db, &clean, &cfg, &r, &src)
        .await
        .expect("resume");
    assert_eq!(outcome.blake3, h::b3(&bytes));
    assert_eq!(
        clean.create_calls.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "resume must reuse the persisted upload_id, never create anew"
    );
    let stored = h::get_bytes(&g.client(), &bucket, &key).await;
    assert_eq!(Blake3Hex::from_bytes(&stored), h::b3(&bytes));
}

#[tokio::test]
async fn resume_re_uploads_only_parts_lacking_records() {
    let (g, bucket, root, _dbdir, db) = scaffold!("tr-mp-resume");
    let key = library_key(&rel("mp/resume.NEF"));
    let s3 = CountingS3::new(g.client());
    // Part 3 fails once: parts 1 and 2 complete and must be recorded.
    s3.fail_parts.lock().unwrap().insert((key.clone(), 3), 1);
    let cfg = h::test_cfg(&bucket, root.path());

    let bytes = h::three_part_bytes(13);
    let r = rel("mp/resume.NEF");
    let src = h::under(root.path(), "resume.NEF");
    h::write_file(&src, &bytes);
    h::seed_queued(&db, &r, Kind::Original, &src);

    upload_item(&db, &s3, &cfg, &r, &src)
        .await
        .expect_err("injected part-3 failure");

    // {part_no, etag, md5} persisted AFTER each completed part (§2.4).
    let parts = db.upload_parts(&r).expect("upload_parts");
    assert_eq!(
        parts.iter().map(|(n, _)| *n).collect::<Vec<_>>(),
        vec![1, 2]
    );
    let part1 = &bytes[..h::PART_5MIB as usize];
    let part2 = &bytes[h::PART_5MIB as usize..2 * h::PART_5MIB as usize];
    assert_eq!(parts[0].1.etag, h::md5_hex(part1));
    assert_eq!(parts[0].1.md5_b64, h::md5_b64(part1));
    assert_eq!(parts[1].1.etag, h::md5_hex(part2));
    assert_eq!(parts[1].1.md5_b64, h::md5_b64(part2));

    // Fresh engine pass: only the missing part goes up; ListParts is
    // consulted opportunistically to reconcile.
    let clean = CountingS3::new(g.client());
    let outcome = upload_item(&db, &clean, &cfg, &r, &src)
        .await
        .expect("resume");
    assert_eq!(
        clean.part_attempts_for(&key),
        vec![3],
        "completed parts are never re-uploaded"
    );
    assert!(
        clean
            .list_parts_calls
            .load(std::sync::atomic::Ordering::SeqCst)
            >= 1,
        "resume reconciles via ListParts"
    );

    // The resumed journal blake3 still hashes the full sent bytes (the
    // already-sent ranges were re-hashed from the unchanged source).
    assert_eq!(outcome.blake3, h::b3(&bytes));
    let stored = h::get_bytes(&g.client(), &bucket, &key).await;
    assert_eq!(Blake3Hex::from_bytes(&stored), h::b3(&bytes));
    let entries = staged_entries(&db);
    assert_eq!(entries.len(), 1, "one entry total across both passes");
    assert_eq!(entries[0].blake3, Some(h::b3(&bytes)));
    assert_eq!(h::state_of(&db, &r), ItemState::Synced);
}

#[tokio::test]
async fn journal_blake3_hashes_exactly_the_sent_bytes_not_the_file() {
    let (g, bucket, root, _dbdir, db) = scaffold!("tr-mp-sent-truth");
    let s3 = CountingS3::new(g.client());
    let cfg = h::test_cfg(&bucket, root.path());

    let file_bytes = h::three_part_bytes(21);
    let r = rel("mp/tamper.NEF");
    let src = h::under(root.path(), "tamper.NEF");
    h::write_file(&src, &file_bytes);
    h::seed_queued(&db, &r, Kind::Original, &src);

    // The stream layer yields bytes DIFFERENT from the file inside part
    // 2. The engine's per-part MD5 and running blake3 are computed over
    // what the source yields, so the server accepts the parts and the
    // journal must record the SENT bytes' hash.
    let tamper_offset = h::PART_5MIB + 123_457;
    let source = common::transfer::XorSource {
        offset: tamper_offset,
        mask: 0xA5,
    };
    let mut sent = file_bytes.clone();
    sent[tamper_offset as usize] ^= 0xA5;

    let outcome = upload_item_from(&db, &s3, &cfg, &r, &src, &source)
        .await
        .expect("upload");

    assert_eq!(outcome.blake3, h::b3(&sent), "hash of what was sent");
    assert_ne!(outcome.blake3, h::b3(&file_bytes), "not the file's hash");

    let stored = h::get_bytes(&g.client(), &bucket, &library_key(&r)).await;
    assert_eq!(stored, sent, "Garage stores the sent bytes");
    let entries = staged_entries(&db);
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0].blake3,
        Some(Blake3Hex::from_bytes(&stored)),
        "journal blake3 == blake3 of what Garage stores == sent bytes"
    );
}

#[tokio::test]
async fn digest_rejection_mid_part_retries_once_then_fails_typed_and_resumable() {
    let (g, bucket, root, _dbdir, db) = scaffold!("tr-mp-digest-reject");
    let key = library_key(&rel("mp/reject.NEF"));
    let s3 = CountingS3::new(g.client());
    // Part 2's bytes are corrupted on EVERY attempt while the engine's
    // Content-MD5 goes through unchanged — Garage digest-rejects each
    // time.
    s3.corrupt_parts
        .lock()
        .unwrap()
        .insert((key.clone(), 2), u32::MAX);
    let cfg = h::test_cfg(&bucket, root.path());

    let bytes = h::three_part_bytes(29);
    let r = rel("mp/reject.NEF");
    let src = h::under(root.path(), "reject.NEF");
    h::write_file(&src, &bytes);
    h::seed_queued(&db, &r, Kind::Original, &src);

    let err = upload_item(&db, &s3, &cfg, &r, &src)
        .await
        .expect_err("persistent digest rejection");
    match err {
        TransferError::DigestRejected {
            relkey,
            part_number,
        } => {
            assert_eq!(relkey, r);
            assert_eq!(part_number, 2);
        }
        other => panic!("expected DigestRejected, got {other:?}"),
    }
    assert_eq!(
        s3.part_attempts_for(&key),
        vec![1, 2, 2],
        "the rejected part is retried exactly once; part 3 is never reached"
    );
    // Resumable: upload record + completed part survive, item re-queued.
    assert!(db.get_upload(&r).expect("get_upload").is_some());
    assert_eq!(
        db.upload_parts(&r)
            .expect("upload_parts")
            .iter()
            .map(|(n, _)| *n)
            .collect::<Vec<_>>(),
        vec![1]
    );
    assert_eq!(h::state_of(&db, &r), ItemState::Queued);
    assert_eq!(outbound_len(&db), 0, "nothing journaled");
}

#[tokio::test]
async fn transient_digest_rejection_recovers_via_the_single_retry() {
    let (g, bucket, root, _dbdir, db) = scaffold!("tr-mp-digest-transient");
    let key = library_key(&rel("mp/transient.NEF"));
    let s3 = CountingS3::new(g.client());
    s3.corrupt_parts.lock().unwrap().insert((key.clone(), 2), 1); // first attempt only
    let cfg = h::test_cfg(&bucket, root.path());

    let bytes = h::three_part_bytes(31);
    let r = rel("mp/transient.NEF");
    let src = h::under(root.path(), "transient.NEF");
    h::write_file(&src, &bytes);
    h::seed_queued(&db, &r, Kind::Original, &src);

    let outcome = upload_item(&db, &s3, &cfg, &r, &src).await.expect("upload");
    assert_eq!(
        s3.part_attempts_for(&key),
        vec![1, 2, 2, 3],
        "one rejection, one successful retry, then onward"
    );
    assert_eq!(outcome.blake3, h::b3(&bytes));
    let stored = h::get_bytes(&g.client(), &bucket, &key).await;
    assert_eq!(Blake3Hex::from_bytes(&stored), h::b3(&bytes));
}

#[tokio::test]
async fn resume_after_source_change_aborts_requeues_dirty_and_journals_nothing() {
    let (g, bucket, root, _dbdir, db) = scaffold!("tr-mp-source-change");
    let key = library_key(&rel("mp/changed.NEF"));
    let s3 = CountingS3::new(g.client());
    s3.fail_parts.lock().unwrap().insert((key.clone(), 2), 1);
    let cfg = h::test_cfg(&bucket, root.path());

    let bytes = h::three_part_bytes(37);
    let r = rel("mp/changed.NEF");
    let src = h::under(root.path(), "changed.NEF");
    h::write_file(&src, &bytes);
    h::seed_queued(&db, &r, Kind::Original, &src);

    upload_item(&db, &s3, &cfg, &r, &src)
        .await
        .expect_err("injected part-2 failure leaves a resumable upload");
    assert!(db.get_upload(&r).expect("get_upload").is_some());

    // The source changes before the resume: different size AND mtime.
    let changed = h::patterned(bytes.len() + 13, 41);
    h::write_file(&src, &changed);
    filetime::set_file_mtime(
        &src,
        filetime::FileTime::from_unix_time(h::mtime_unix(&src) + 5, 0),
    )
    .expect("bump mtime");

    let clean = CountingS3::new(g.client());
    let err = upload_item(&db, &clean, &cfg, &r, &src)
        .await
        .expect_err("mid-resume source change must abort");
    match err {
        TransferError::AbortedSourceChanged { relkey } => assert_eq!(relkey, r),
        other => panic!("expected AbortedSourceChanged, got {other:?}"),
    }
    // The multipart upload is gone on the backend...
    assert!(
        h::backend_uploads(&g.client(), &bucket).await.is_empty(),
        "the stale upload must be aborted remotely"
    );
    // ...and locally; the item is re-queued dirty with nothing journaled.
    assert_eq!(db.get_upload(&r).expect("get_upload"), None);
    assert!(db.upload_parts(&r).expect("upload_parts").is_empty());
    assert_eq!(h::state_of(&db, &r), ItemState::Dirty);
    assert_eq!(outbound_len(&db), 0, "nothing journaled for either version");
}

// ===========================================================================
// Stale-upload hygiene
// ===========================================================================

#[tokio::test]
async fn abort_stale_uploads_aborts_aged_own_uploads_and_re_marks_dirty() {
    let (g, bucket, root, _dbdir, db) = scaffold!("tr-stale-aged");
    let client = g.client();
    let cfg = h::test_cfg(&bucket, root.path());
    let r = rel("stale/aged.NEF");
    let key = library_key(&r);

    // A real in-progress upload whose record is 8 days old.
    let created = client
        .create_multipart_upload(&bucket, &key, &PutObjectOptions::default())
        .await
        .expect("create");
    let now = 1_769_900_000i64;
    let src = h::under(root.path(), "aged.NEF");
    h::write_file(&src, &h::patterned(1024, 1));
    h::seed_queued(&db, &r, Kind::Original, &src);
    h::advance(&db, &r, &[ItemState::Uploading]);
    db.set_upload(
        &r,
        &MultipartUploadState {
            upload_id: created.upload_id.clone(),
            part_size: cfg.part_size,
            started_unix: now - 8 * 24 * 60 * 60,
        },
    )
    .expect("set_upload");

    let report = abort_stale_uploads(&db, &client, &cfg, 7 * 24 * 60 * 60, now)
        .await
        .expect("sweep");
    assert_eq!(
        report.aborted_own,
        vec![(r.clone(), created.upload_id.clone())]
    );
    assert!(h::backend_uploads(&client, &bucket).await.is_empty());
    assert_eq!(db.get_upload(&r).expect("get_upload"), None);
    assert_eq!(
        h::state_of(&db, &r),
        ItemState::Dirty,
        "uploading -> dirty (upload abandoned, §2.4)"
    );
}

#[tokio::test]
async fn abort_stale_uploads_keeps_fresh_uploads_and_aborts_own_key_orphans() {
    let (g, bucket, root, _dbdir, db) = scaffold!("tr-stale-orphan");
    let client = g.client();
    let cfg = h::test_cfg(&bucket, root.path());
    let r = rel("stale/orphan.NEF");
    let key = library_key(&r);

    // Two backend uploads on our key; we hold a FRESH record for `ours`.
    let ours = client
        .create_multipart_upload(&bucket, &key, &PutObjectOptions::default())
        .await
        .expect("create ours");
    let orphan = client
        .create_multipart_upload(&bucket, &key, &PutObjectOptions::default())
        .await
        .expect("create orphan");
    // And one upload on a key we hold NO record for (another device's):
    // it must be left alone.
    let foreign_key = library_key(&rel("stale/other-device.NEF"));
    let foreign = client
        .create_multipart_upload(&bucket, &foreign_key, &PutObjectOptions::default())
        .await
        .expect("create foreign");

    let now = 1_769_900_000i64;
    let src = h::under(root.path(), "orphan.NEF");
    h::write_file(&src, &h::patterned(1024, 2));
    h::seed_queued(&db, &r, Kind::Original, &src);
    db.set_upload(
        &r,
        &MultipartUploadState {
            upload_id: ours.upload_id.clone(),
            part_size: cfg.part_size,
            started_unix: now - 60,
        },
    )
    .expect("set_upload");

    let report = abort_stale_uploads(&db, &client, &cfg, 7 * 24 * 60 * 60, now)
        .await
        .expect("sweep");
    assert!(report.aborted_own.is_empty(), "fresh record untouched");
    assert_eq!(
        report.aborted_orphans,
        vec![(key.clone(), orphan.upload_id.clone())],
        "a different upload_id on a key we track is an orphan"
    );
    let remaining = h::backend_uploads(&client, &bucket).await;
    assert!(remaining.contains(&(key.clone(), ours.upload_id.clone())));
    assert!(remaining.contains(&(foreign_key.clone(), foreign.upload_id.clone())));
    assert_eq!(remaining.len(), 2);
    assert!(db.get_upload(&r).expect("get_upload").is_some());
}

// ===========================================================================
// Verify / readback
// ===========================================================================

#[tokio::test]
async fn readback_verify_catches_a_corrupting_digest_accepting_backend() {
    let (g, bucket, root, _dbdir, db) = scaffold!("tr-verify-corrupt");
    let r = rel("verify/corrupt.NEF");
    let key = library_key(&r);
    // The backend accepts a bad digest AND stores corrupted bytes.
    let mut s3 = CountingS3::new(g.client());
    s3.strip_digests = true;
    s3.corrupt_put_bodies.insert(key.clone());
    let mut cfg = h::test_cfg(&bucket, root.path());
    cfg.backend = BackendProfile {
        digest_rejection_works: false,
    };

    let bytes = h::patterned(200_000, 5);
    let src = h::under(root.path(), "corrupt.NEF");
    h::write_file(&src, &bytes);
    h::seed_queued(&db, &r, Kind::Original, &src);

    let err = upload_item(&db, &s3, &cfg, &r, &src)
        .await
        .expect_err("read-back verify must catch the corruption");
    match err {
        TransferError::CorruptRemote { relkey, .. } => assert_eq!(relkey, r),
        other => panic!("expected CorruptRemote, got {other:?}"),
    }
    assert_eq!(
        h::state_of(&db, &r),
        ItemState::CorruptRemote,
        "verify mismatch lands corrupt_remote, never synced (§2.4)"
    );
    assert_eq!(outbound_len(&db), 0, "NO journal entry for corrupt content");
    assert!(
        !s3.get_ranges_for(&key).is_empty(),
        "the read-back re-hash actually fetched the object"
    );
}

#[tokio::test]
async fn readback_verify_runs_exactly_when_the_backend_requires_it() {
    let (g, bucket, root, _dbdir, db) = scaffold!("tr-verify-readback");
    let bytes = h::patterned(150_000, 6);

    // requires_readback_verify = true: the object is re-fetched.
    let r1 = rel("verify/readback.NEF");
    let src1 = h::under(root.path(), "readback.NEF");
    h::write_file(&src1, &bytes);
    h::seed_queued(&db, &r1, Kind::Original, &src1);
    let s3 = CountingS3::new(g.client());
    let mut cfg = h::test_cfg(&bucket, root.path());
    cfg.backend = BackendProfile {
        digest_rejection_works: false,
    };
    upload_item(&db, &s3, &cfg, &r1, &src1)
        .await
        .expect("upload");
    assert!(
        !s3.get_ranges_for(&library_key(&r1)).is_empty(),
        "requires_readback_verify must re-hash via GET"
    );
    assert_eq!(h::state_of(&db, &r1), ItemState::Synced);

    // digest_rejection_works = true: no read-back GET.
    let r2 = rel("verify/no-readback.NEF");
    let src2 = h::under(root.path(), "no-readback.NEF");
    h::write_file(&src2, &bytes);
    h::seed_queued(&db, &r2, Kind::Original, &src2);
    let s3b = CountingS3::new(g.client());
    let cfg2 = h::test_cfg(&bucket, root.path());
    upload_item(&db, &s3b, &cfg2, &r2, &src2)
        .await
        .expect("upload");
    assert!(
        s3b.get_ranges_for(&library_key(&r2)).is_empty(),
        "a digest-verifying backend needs no read-back"
    );
}

// ===========================================================================
// Atomic verify-commit (§2.1.5)
// ===========================================================================

/// Seeds an item at `verifying` and returns its would-be journal entry.
fn verifying_item(db: &SyncDb, name: &str) -> (rrcloud_core::keys::RelKey, JournalEntry) {
    let r = rel(name);
    let bytes = h::patterned(64, 8);
    let dir = tempfile::tempdir().expect("tempdir");
    let src = dir.path().join("f.bin");
    h::write_file(&src, &bytes);
    h::seed_queued(db, &r, Kind::Original, &src);
    h::advance(db, &r, &[ItemState::Uploading, ItemState::Verifying]);
    let mut e = entry(db.device_id(), Op::Put, Kind::Original, library_key(&r));
    e.blake3 = Some(h::b3(&bytes));
    (r, e)
}

#[test]
fn commit_verified_stages_entry_and_transitions_in_one_txn() {
    let (_dir, _path, db) = open_db(&dev(common::sync::DEV_A));
    let (r, e) = verifying_item(&db, "atomic/success.NEF");

    let id = commit_verified(
        &db,
        &r,
        |record| record.blake3 = e.blake3.clone(),
        |_record| Ok(e.clone()),
    )
    .expect("commit");

    let record = db.get_item(&r).expect("get").expect("exists");
    assert_eq!(record.state, ItemState::Synced);
    assert!(
        record.verified_remote,
        "commit_verified sets verified_remote"
    );
    assert_eq!(record.blake3, e.blake3);
    let entries = staged_entries(&db);
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].key, e.key);
    assert_eq!(entries[0].blake3, e.blake3);
    assert_eq!(entries[0].seq, 0, "seq is stamped at publication, not here");
    let _ = id;
}

#[test]
fn commit_verified_panic_mid_txn_leaves_neither_transition_nor_entry() {
    let (_dir, _path, db) = open_db(&dev(common::sync::DEV_A));
    let (r, _e) = verifying_item(&db, "atomic/panic.NEF");

    let outcome = catch_unwind(AssertUnwindSafe(|| {
        commit_verified(
            &db,
            &r,
            |record| record.verified_remote = true,
            |_record| -> Result<JournalEntry, TransferError> {
                panic!("injected panic between verify and journal-enqueue")
            },
        )
    }));
    // The propagated panic must be OUR injected one — anything else (a
    // todo!, an internal unwrap) means the engine never reached the entry
    // builder inside the transaction, and the rollback assertions below
    // would pass vacuously.
    let payload = outcome.expect_err("the injected panic must propagate");
    let message = payload
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_default();
    assert!(
        message.contains("injected panic between verify and journal-enqueue"),
        "the engine must run the entry builder inside the commit txn \
         (propagated panic was: {message:?})"
    );
    assert_eq!(
        h::state_of(&db, &r),
        ItemState::Verifying,
        "the unwound transaction must leave the transition uncommitted"
    );
    assert!(
        !db.get_item(&r).unwrap().unwrap().verified_remote,
        "no partial record mutation survives"
    );
    assert_eq!(outbound_len(&db), 0, "no staged entry survives");
}

#[test]
fn commit_verified_entry_error_rolls_back_the_transition() {
    let (_dir, _path, db) = open_db(&dev(common::sync::DEV_A));
    let (r, _e) = verifying_item(&db, "atomic/err.NEF");

    let err = commit_verified(
        &db,
        &r,
        |_record| {},
        |_record| {
            Err(TransferError::InvalidConfig(
                "injected build failure".to_string(),
            ))
        },
    )
    .expect_err("build error must propagate");
    assert!(matches!(err, TransferError::InvalidConfig(_)));
    assert_eq!(h::state_of(&db, &r), ItemState::Verifying);
    assert_eq!(outbound_len(&db), 0);
}

#[test]
fn commit_verified_staging_refusal_rolls_back_the_transition() {
    let (_dir, _path, db) = open_db(&dev(common::sync::DEV_A));
    let (r, mut e) = verifying_item(&db, "atomic/oversize.NEF");
    // An entry no segment can ever hold: staging must refuse it, and the
    // refusal must take the transition down with it.
    e.key = format!("library/{}", "a".repeat(SEGMENT_MAX_BYTES));

    let err = commit_verified(&db, &r, |_record| {}, |_record| Ok(e.clone()))
        .expect_err("oversized entry must be refused");
    assert!(
        matches!(
            err,
            TransferError::Publisher(PublisherError::OversizedEntry { .. })
        ),
        "got {err:?}"
    );
    assert_eq!(h::state_of(&db, &r), ItemState::Verifying);
    assert_eq!(outbound_len(&db), 0);
}

// ===========================================================================
// Download (§3.5)
// ===========================================================================

#[tokio::test]
async fn download_original_round_trips_installs_atomically_and_restores_mtime() {
    let (g, bucket, root, _dbdir, db) = scaffold!("tr-dl-roundtrip");
    let client = g.client();
    let cfg = h::test_cfg(&bucket, root.path());
    let r = rel("dl/orig.NEF");
    let key = library_key(&r);
    let bytes = h::patterned(400_000, 10);
    client
        .put_object(
            &bucket,
            &key,
            Bytes::from(bytes.clone()),
            &PutObjectOptions::default(),
        )
        .await
        .expect("put");

    let mtime = 1_700_000_123i64;
    h::seed_pending_down(&db, &r, Kind::Original, &bytes, mtime);
    let expected = ExpectedDownload {
        blake3: h::b3(&bytes),
        size: bytes.len() as u64,
        mtime_unix: mtime,
    };
    let s3 = CountingS3::new(g.client());
    let outcome = download_item(&db, &s3, &cfg, &r, root.path(), &expected)
        .await
        .expect("download");

    let final_path = local_target_path(root.path(), &r, Kind::Original);
    assert_eq!(outcome.path, final_path);
    assert_eq!(outcome.resumed_from, 0);
    assert_eq!(std::fs::read(&final_path).expect("installed"), bytes);
    assert_eq!(
        h::mtime_unix(&final_path),
        mtime,
        "mtime restored to the remote mtime (§3.5)"
    );
    assert!(
        !partial_path(&final_path).exists(),
        "the .rr.part temp is gone after install"
    );
    assert_eq!(
        h::state_of(&db, &r),
        ItemState::Hydrated,
        "a fully fetched + verified original lands hydrated"
    );
}

#[tokio::test]
async fn download_valid_sidecar_parse_validates_and_lands_synced() {
    let (g, bucket, root, _dbdir, db) = scaffold!("tr-dl-sidecar");
    let client = g.client();
    let cfg = h::test_cfg(&bucket, root.path());
    let r = rel("dl/edited.NEF");
    let key = sidecar_key(&r);
    let bytes = std::fs::read(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/sidecar_full.json"
    ))
    .expect("fixture");
    client
        .put_object(
            &bucket,
            &key,
            Bytes::from(bytes.clone()),
            &PutObjectOptions::default(),
        )
        .await
        .expect("put");

    let mtime = 1_700_000_200i64;
    h::seed_pending_down(&db, &r, Kind::Sidecar, &bytes, mtime);
    let expected = ExpectedDownload {
        blake3: h::b3(&bytes),
        size: bytes.len() as u64,
        mtime_unix: mtime,
    };
    let s3 = CountingS3::new(g.client());
    download_item(&db, &s3, &cfg, &r, root.path(), &expected)
        .await
        .expect("download");

    let final_path = local_target_path(root.path(), &r, Kind::Sidecar);
    assert!(final_path.to_string_lossy().ends_with(".rrdata"));
    assert_eq!(std::fs::read(&final_path).expect("installed"), bytes);
    assert_eq!(
        h::state_of(&db, &r),
        ItemState::Synced,
        "sidecars land synced, not hydrated"
    );
}

#[tokio::test]
async fn download_resumes_with_a_ranged_get_from_the_exact_partial_offset() {
    let (g, bucket, root, _dbdir, db) = scaffold!("tr-dl-resume");
    let client = g.client();
    let cfg = h::test_cfg(&bucket, root.path());
    let r = rel("dl/resume.NEF");
    let key = library_key(&r);
    let bytes = h::patterned(1_000_000, 17);
    client
        .put_object(
            &bucket,
            &key,
            Bytes::from(bytes.clone()),
            &PutObjectOptions::default(),
        )
        .await
        .expect("put");

    let cut_at = 300_000u64;
    let s3 = CountingS3::new(g.client());
    s3.cut_get_after.lock().unwrap().insert(key.clone(), cut_at);

    let mtime = 1_700_000_300i64;
    h::seed_pending_down(&db, &r, Kind::Original, &bytes, mtime);
    let expected = ExpectedDownload {
        blake3: h::b3(&bytes),
        size: bytes.len() as u64,
        mtime_unix: mtime,
    };

    download_item(&db, &s3, &cfg, &r, root.path(), &expected)
        .await
        .expect_err("the cut stream must fail the first attempt");
    let final_path = local_target_path(root.path(), &r, Kind::Original);
    let partial = partial_path(&final_path);
    assert_eq!(
        std::fs::metadata(&partial).expect("partial survives").len(),
        cut_at,
        "every received byte is appended to the .rr.part before failing"
    );
    assert_eq!(
        std::fs::read(&partial).expect("partial"),
        bytes[..cut_at as usize],
        "the partial holds exactly the prefix"
    );
    assert!(!final_path.exists(), "nothing installed on failure");
    assert_eq!(
        h::state_of(&db, &r),
        ItemState::PendingDown,
        "transport failure returns the item to pending_down (resumable)"
    );

    // Resume: ranged GET from the exact offset.
    let outcome = download_item(&db, &s3, &cfg, &r, root.path(), &expected)
        .await
        .expect("resume completes");
    assert_eq!(outcome.resumed_from, cut_at);
    assert_eq!(outcome.bytes_fetched, bytes.len() as u64 - cut_at);
    let ranges = s3.get_ranges_for(&key);
    assert_eq!(
        ranges.last().expect("resume GET"),
        &Some(ByteRange::From(cut_at)),
        "resume must request bytes={cut_at}-"
    );
    assert_eq!(std::fs::read(&final_path).expect("installed"), bytes);
    assert_eq!(h::mtime_unix(&final_path), mtime);
    assert!(!partial.exists(), "temp gone after install");
    assert_eq!(h::state_of(&db, &r), ItemState::Hydrated);
}

#[tokio::test]
async fn corrupt_sidecar_is_never_installed_and_preserves_the_destination() {
    let (g, bucket, root, _dbdir, db) = scaffold!("tr-dl-badsidecar");
    let client = g.client();
    let cfg = h::test_cfg(&bucket, root.path());
    let r = rel("dl/bad.NEF");
    let key = sidecar_key(&r);
    // Syntactically invalid sidecar whose blake3 matches what we expect:
    // the hash passes, the parse must not.
    let bytes = b"{ this is not valid sidecar json".to_vec();
    client
        .put_object(
            &bucket,
            &key,
            Bytes::from(bytes.clone()),
            &PutObjectOptions::default(),
        )
        .await
        .expect("put");

    let final_path = local_target_path(root.path(), &r, Kind::Sidecar);
    h::write_file(&final_path, b"precious pre-existing sidecar");

    h::seed_pending_down(&db, &r, Kind::Sidecar, &bytes, 1_700_000_400);
    let expected = ExpectedDownload {
        blake3: h::b3(&bytes),
        size: bytes.len() as u64,
        mtime_unix: 1_700_000_400,
    };
    let s3 = CountingS3::new(g.client());
    let err = download_item(&db, &s3, &cfg, &r, root.path(), &expected)
        .await
        .expect_err("invalid sidecar must never install");
    match err {
        TransferError::SidecarInvalid { relkey, .. } => assert_eq!(relkey, r),
        other => panic!("expected SidecarInvalid, got {other:?}"),
    }
    assert_eq!(
        std::fs::read(&final_path).expect("destination intact"),
        b"precious pre-existing sidecar",
        "the pre-existing file is untouched"
    );
    assert!(!partial_path(&final_path).exists(), "partial deleted");
    assert_eq!(h::state_of(&db, &r), ItemState::CorruptRemote);
}

#[tokio::test]
async fn download_hash_mismatch_is_typed_deletes_partial_and_marks_corrupt_remote() {
    let (g, bucket, root, _dbdir, db) = scaffold!("tr-dl-mismatch");
    let client = g.client();
    let cfg = h::test_cfg(&bucket, root.path());
    let r = rel("dl/mismatch.NEF");
    let key = library_key(&r);
    let stored = h::patterned(100_000, 19);
    let advertised = h::patterned(100_000, 23); // what the journal claims
    client
        .put_object(
            &bucket,
            &key,
            Bytes::from(stored.clone()),
            &PutObjectOptions::default(),
        )
        .await
        .expect("put");

    h::seed_pending_down(&db, &r, Kind::Original, &advertised, 1_700_000_500);
    let expected = ExpectedDownload {
        blake3: h::b3(&advertised),
        size: advertised.len() as u64,
        mtime_unix: 1_700_000_500,
    };
    let s3 = CountingS3::new(g.client());
    let err = download_item(&db, &s3, &cfg, &r, root.path(), &expected)
        .await
        .expect_err("hash mismatch");
    match err {
        TransferError::IntegrityMismatch {
            relkey,
            expected: want,
            actual,
        } => {
            assert_eq!(relkey, r);
            assert_eq!(want, h::b3(&advertised));
            assert_eq!(actual, h::b3(&stored));
        }
        other => panic!("expected IntegrityMismatch, got {other:?}"),
    }
    let final_path = local_target_path(root.path(), &r, Kind::Original);
    assert!(!final_path.exists(), "wrong bytes never install");
    assert!(!partial_path(&final_path).exists(), "partial deleted");
    assert_eq!(h::state_of(&db, &r), ItemState::CorruptRemote);
}

#[tokio::test]
async fn download_tolerates_a_mid_download_remote_replacement_via_the_hash_backstop() {
    let (g, bucket, root, _dbdir, db) = scaffold!("tr-dl-replaced");
    let client = g.client();
    let cfg = h::test_cfg(&bucket, root.path());
    let r = rel("dl/replaced.NEF");
    let key = library_key(&r);
    let original = h::patterned(600_000, 29);
    client
        .put_object(
            &bucket,
            &key,
            Bytes::from(original.clone()),
            &PutObjectOptions::default(),
        )
        .await
        .expect("put v1");

    let cut_at = 200_000u64;
    let s3 = CountingS3::new(g.client());
    s3.cut_get_after.lock().unwrap().insert(key.clone(), cut_at);

    h::seed_pending_down(&db, &r, Kind::Original, &original, 1_700_000_600);
    let expected = ExpectedDownload {
        blake3: h::b3(&original),
        size: original.len() as u64,
        mtime_unix: 1_700_000_600,
    };
    download_item(&db, &s3, &cfg, &r, root.path(), &expected)
        .await
        .expect_err("cut first attempt");

    // The object is replaced while our partial sits on disk (same size,
    // different bytes — the nastiest case for a byte-offset resume).
    let replaced = h::patterned(600_000, 31);
    client
        .put_object(
            &bucket,
            &key,
            Bytes::from(replaced.clone()),
            &PutObjectOptions::default(),
        )
        .await
        .expect("put v2");

    let err = download_item(&db, &s3, &cfg, &r, root.path(), &expected)
        .await
        .expect_err("a spliced object can never verify — the hash is the backstop");
    assert!(
        matches!(err, TransferError::IntegrityMismatch { .. }),
        "got {err:?}"
    );
    let final_path = local_target_path(root.path(), &r, Kind::Original);
    assert!(!final_path.exists(), "spliced bytes never install");
    assert!(!partial_path(&final_path).exists(), "partial deleted");
    assert_eq!(h::state_of(&db, &r), ItemState::CorruptRemote);
}

// ===========================================================================
// Queue pump
// ===========================================================================

/// Seeds `n` small queued originals under `prefix` with the given
/// priority classes, returning `(relkey, bucket_key)` per item in push
/// order.
fn seed_pump_items(
    db: &SyncDb,
    root: &Path,
    prefix: &str,
    classes: &[u8],
) -> Vec<(rrcloud_core::keys::RelKey, String)> {
    classes
        .iter()
        .enumerate()
        .map(|(i, &class)| {
            let r = rel(&format!("{prefix}/item-{i}.NEF"));
            let src = local_target_path(root, &r, Kind::Original);
            h::write_file(&src, &h::patterned(64 * 1024 + i, (i + 1) as u8));
            h::seed_queued(db, &r, Kind::Original, &src);
            assert!(db.queue_push(Queue::Up, &r, class).expect("queue_push"));
            let key = library_key(&r);
            (r, key)
        })
        .collect()
}

#[tokio::test]
async fn pump_uploads_drains_priority_classes_in_order() {
    let (g, bucket, root, _dbdir, db) = scaffold!("tr-pump-priority");
    let s3 = CountingS3::new(g.client());
    let cfg = h::test_cfg(&bucket, root.path());
    // Pushed deliberately out of class order.
    let items = seed_pump_items(&db, root.path(), "pump-prio", &[2, 0, 1, 2, 0, 1]);

    // Concurrency 1 makes admission order fully deterministic: strictly
    // (class, arrival).
    let summary = pump_uploads(&db, &s3, &cfg, 1, &CancelFlag::new())
        .await
        .expect("pump");
    assert_eq!(summary.completed.len(), 6);
    assert!(summary.failed.is_empty());
    assert!(!summary.cancelled);

    let expected_order: Vec<String> = [1usize, 4, 2, 5, 0, 3] // class 0s, 1s, 2s in arrival order
        .iter()
        .map(|&i| items[i].1.clone())
        .collect();
    assert_eq!(
        s3.put_keys(),
        expected_order,
        "pop order is lowest class first, FIFO within a class"
    );
    assert_eq!(db.queue_len(Queue::Up).expect("queue_len"), 0);
    assert_eq!(outbound_len(&db), 6, "every completed item journaled");
}

#[tokio::test]
async fn pump_uploads_caps_concurrency_and_isolates_a_failing_item() {
    let (g, bucket, root, _dbdir, db) = scaffold!("tr-pump-conc");
    let cfg = h::test_cfg(&bucket, root.path());
    let items = seed_pump_items(&db, root.path(), "pump-conc", &[0, 0, 1, 1, 2, 2]);
    let (failing_rel, failing_key) = items[2].clone();

    let mut s3 = CountingS3::new(g.client());
    s3.fail_puts.insert(failing_key.clone());

    let summary = pump_uploads(&db, &s3, &cfg, 2, &CancelFlag::new())
        .await
        .expect("pump");
    assert_eq!(
        summary.completed.len(),
        5,
        "one failing item must not stop the other five"
    );
    assert_eq!(summary.failed.len(), 1);
    assert_eq!(summary.failed[0].0, failing_rel);
    assert!(!summary.cancelled);
    assert!(
        s3.max_in_flight() <= 2,
        "at most `concurrency` transfers in flight (§2.4), saw {}",
        s3.max_in_flight()
    );

    for (r, _) in items.iter().filter(|(r, _)| *r != failing_rel) {
        assert_eq!(h::state_of(&db, r), ItemState::Synced);
    }
    assert_eq!(
        h::state_of(&db, &failing_rel),
        ItemState::Queued,
        "the failed item stays resumable"
    );
    assert_eq!(
        db.queue_len(Queue::Up).expect("queue_len"),
        1,
        "the failed item is re-queued for a later pass (no in-pass retry)"
    );
    assert_eq!(
        db.queue_peek(Queue::Up).expect("peek").expect("head").0,
        failing_rel
    );
    assert_eq!(outbound_len(&db), 5, "nothing journaled for the failure");
}

#[tokio::test]
async fn pump_cancel_stops_admission_and_waits_for_in_flight_items() {
    let (g, bucket, root, _dbdir, db) = scaffold!("tr-pump-cancel");
    let cfg = h::test_cfg(&bucket, root.path());
    let items = seed_pump_items(&db, root.path(), "pump-cancel", &[0, 0, 0, 0]);

    let cancel = CancelFlag::new();
    let s3 = CountingS3::new(g.client());
    let hook_flag = cancel.clone();
    // Fire the cancel from inside the FIRST item's transfer: with
    // concurrency 1, nothing further may be admitted, and the in-flight
    // item must still run to a committed, resumable completion.
    *s3.on_put.lock().unwrap() = Some(Box::new(move |_key| hook_flag.cancel()));

    let summary = pump_uploads(&db, &s3, &cfg, 1, &cancel)
        .await
        .expect("pump");
    assert!(summary.cancelled);
    assert_eq!(
        summary.completed,
        vec![items[0].0.clone()],
        "the in-flight item is awaited, not abandoned"
    );
    assert!(summary.failed.is_empty());
    assert_eq!(
        db.queue_len(Queue::Up).expect("queue_len"),
        3,
        "unadmitted items stay queued for a later pass"
    );
    assert_eq!(
        db.count_in_state(ItemState::Uploading).expect("count"),
        0,
        "deterministic shutdown leaves nothing mid-transfer"
    );
    for (r, _) in &items[1..] {
        assert_eq!(h::state_of(&db, r), ItemState::Queued);
    }
}

#[tokio::test]
async fn pump_pre_cancelled_admits_nothing_and_leaves_the_queue_intact() {
    let (g, bucket, root, _dbdir, db) = scaffold!("tr-pump-precancel");
    let s3 = CountingS3::new(g.client());
    let cfg = h::test_cfg(&bucket, root.path());
    seed_pump_items(&db, root.path(), "pump-pre", &[0, 1]);

    let cancel = CancelFlag::new();
    cancel.cancel();
    let summary = pump_uploads(&db, &s3, &cfg, 2, &cancel)
        .await
        .expect("pump");
    assert!(summary.cancelled);
    assert!(summary.completed.is_empty());
    assert!(summary.failed.is_empty());
    assert!(s3.put_keys().is_empty(), "no network I/O after cancel");
    assert_eq!(db.queue_len(Queue::Up).expect("queue_len"), 2);
}

#[tokio::test]
async fn pump_downloads_drains_by_priority_and_installs_verified_files() {
    let (g, bucket, root, _dbdir, db) = scaffold!("tr-pump-down");
    let client = g.client();
    let cfg = h::test_cfg(&bucket, root.path());

    // Three advertised originals, queued out of class order.
    let mut items = Vec::new();
    for (i, class) in [(0usize, 2u8), (1, 0), (2, 1)] {
        let r = rel(&format!("pump-down/item-{i}.NEF"));
        let key = library_key(&r);
        let bytes = h::patterned(80_000 + i, (40 + i) as u8);
        client
            .put_object(
                &bucket,
                &key,
                Bytes::from(bytes.clone()),
                &PutObjectOptions::default(),
            )
            .await
            .expect("put");
        h::seed_pending_down(&db, &r, Kind::Original, &bytes, 1_700_000_700 + i as i64);
        assert!(db.queue_push(Queue::Down, &r, class).expect("queue_push"));
        items.push((r, key, bytes, class));
    }

    let s3 = CountingS3::new(g.client());
    let summary = pump_downloads(&db, &s3, &cfg, 1, &CancelFlag::new())
        .await
        .expect("pump");
    assert_eq!(summary.completed.len(), 3);
    assert!(summary.failed.is_empty());

    // Admission strictly by class at concurrency 1.
    let expected_order: Vec<String> = {
        let mut by_class = items.clone();
        by_class.sort_by_key(|(_, _, _, class)| *class);
        by_class.into_iter().map(|(_, key, _, _)| key).collect()
    };
    let got_order: Vec<String> = s3.get_requests().into_iter().map(|(k, _)| k).collect();
    assert_eq!(got_order, expected_order);

    for (i, (r, _key, bytes, _class)) in items.iter().enumerate() {
        let path = local_target_path(root.path(), r, Kind::Original);
        assert_eq!(&std::fs::read(&path).expect("installed"), bytes);
        assert_eq!(h::mtime_unix(&path), 1_700_000_700 + i as i64);
        assert_eq!(h::state_of(&db, r), ItemState::Hydrated);
    }
    assert_eq!(db.queue_len(Queue::Down).expect("queue_len"), 0);
}

// ===========================================================================
// Misc surface
// ===========================================================================

#[test]
fn transferable_kinds_map_to_their_schema_keys_and_others_are_refused() {
    let r = rel("map/a.NEF");
    assert_eq!(
        bucket_key_for(&r, Kind::Original).expect("original"),
        library_key(&r)
    );
    assert_eq!(
        bucket_key_for(&r, Kind::Sidecar).expect("sidecar"),
        sidecar_key(&r)
    );
    let x = rel("map/a.xmp");
    assert_eq!(bucket_key_for(&x, Kind::Xmp).expect("xmp"), library_key(&x));
    assert!(matches!(
        bucket_key_for(&r, Kind::Preview),
        Err(TransferError::UnsupportedKind { .. })
    ));
}

/// Keep `TransferConfig` constructible with defaults the architecture
/// names (16 MiB part size and threshold).
#[test]
fn transfer_config_defaults_match_the_architecture() {
    let cfg = TransferConfig::new(
        "bucket",
        "/tmp/root",
        BackendProfile {
            digest_rejection_works: true,
        },
    );
    assert_eq!(cfg.part_size, 16 * 1024 * 1024);
    assert_eq!(cfg.multipart_threshold, 16 * 1024 * 1024);
}
