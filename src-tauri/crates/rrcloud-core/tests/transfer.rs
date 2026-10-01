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
use rrcloud_core::state::{ItemState, MultipartUploadState, Queue, StateError, SyncDb, UploadPart};
use rrcloud_core::transfer::{
    abort_stale_uploads, bucket_key_for, commit_verified, download_item, local_target_path,
    partial_path, probe_backend, pump_downloads, pump_uploads, recover_interrupted,
    stored_backend_profile, upload_item, upload_item_from, BackendProfile, CancelFlag, ChunkSource,
    ExpectedDownload, SourceStream, TransferConfig, TransferError, PROBE_KEY,
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
            size: std::fs::metadata(&src).expect("metadata").len(),
            mtime_unix_ns: h::mtime_unix_ns(&src),
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
async fn abort_stale_uploads_age_gates_own_key_orphans_and_leaves_live_uploads_alone() {
    let (g, bucket, root, _dbdir, db) = scaffold!("tr-stale-orphan");
    let client = g.client();
    let cfg = h::test_cfg(&bucket, root.path());
    let r = rel("stale/orphan.NEF");
    let key = library_key(&r);

    // Two backend uploads on our key; we hold a record for `ours`.
    let ours = client
        .create_multipart_upload(&bucket, &key, &PutObjectOptions::default())
        .await
        .expect("create ours");
    let orphan = client
        .create_multipart_upload(&bucket, &key, &PutObjectOptions::default())
        .await
        .expect("create orphan");
    // And one upload on a key we hold NO record for (another device's):
    // it must be left alone whatever its age.
    let foreign_key = library_key(&rel("stale/other-device.NEF"));
    let foreign = client
        .create_multipart_upload(&bucket, &foreign_key, &PutObjectOptions::default())
        .await
        .expect("create foreign");

    let real_now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_secs() as i64;
    let max_age = 7 * 24 * 60 * 60;
    let src = h::under(root.path(), "orphan.NEF");
    h::write_file(&src, &h::patterned(1024, 2));
    h::seed_queued(&db, &r, Kind::Original, &src);
    let set_ours = |started_unix: i64| {
        db.set_upload(
            &r,
            &MultipartUploadState {
                upload_id: ours.upload_id.clone(),
                part_size: cfg.part_size,
                started_unix,
                size: std::fs::metadata(&src).expect("metadata").len(),
                mtime_unix_ns: h::mtime_unix_ns(&src),
            },
        )
        .expect("set_upload");
    };
    set_ours(real_now - 60);

    // Pass 1, real clock: the orphan was Initiated seconds ago, so its
    // age is NOT provably past max_age — a sibling device (or this
    // engine's own pump, re-creating the upload after the table was
    // snapshotted) could be live on this shared key RIGHT NOW. Nothing
    // may be aborted (the old sweep killed exactly such live uploads).
    let report = abort_stale_uploads(&db, &client, &cfg, max_age, real_now)
        .await
        .expect("sweep 1");
    assert!(report.aborted_own.is_empty(), "fresh record untouched");
    assert!(
        report.aborted_orphans.is_empty(),
        "an orphan of unproven age must be left alone (it may be a live \
         sibling-device upload on the shared key)"
    );
    assert_eq!(h::backend_uploads(&client, &bucket).await.len(), 3);

    // Pass 2, clock advanced 8 days (the backend's Initiated timestamps
    // now prove every listed upload old): the own-key orphan is aborted;
    // our own fresh record and the foreign-key upload stay.
    let future_now = real_now + 8 * 24 * 60 * 60;
    set_ours(future_now - 60); // keep our own record fresh for this pass
    let report = abort_stale_uploads(&db, &client, &cfg, max_age, future_now)
        .await
        .expect("sweep 2");
    assert!(report.aborted_own.is_empty(), "fresh record untouched");
    assert_eq!(
        report.aborted_orphans,
        vec![(key.clone(), orphan.upload_id.clone())],
        "a provably aged different-id upload on a key we track is an orphan"
    );
    let remaining = h::backend_uploads(&client, &bucket).await;
    assert!(remaining.contains(&(key.clone(), ours.upload_id.clone())));
    assert!(remaining.contains(&(foreign_key.clone(), foreign.upload_id.clone())));
    assert_eq!(remaining.len(), 2);
    assert!(db.get_upload(&r).expect("get_upload").is_some());
}

#[tokio::test]
async fn abort_stale_uploads_clears_aged_recordless_rows_without_leaking_the_backend_upload() {
    let (g, bucket, root, _dbdir, db) = scaffold!("tr-stale-recordless");
    let client = g.client();
    let cfg = h::test_cfg(&bucket, root.path());
    // An uploads-table row whose item record is gone (e.g. a delete raced
    // the upload), pointing at a real backend upload.
    let r = rel("stale/recordless.NEF");
    let key = library_key(&r);
    let created = client
        .create_multipart_upload(&bucket, &key, &PutObjectOptions::default())
        .await
        .expect("create");
    let now = 1_769_900_000i64;
    db.set_upload(
        &r,
        &MultipartUploadState {
            upload_id: created.upload_id.clone(),
            part_size: cfg.part_size,
            started_unix: now - 8 * 24 * 60 * 60,
            size: 1024,
            mtime_unix_ns: 1,
        },
    )
    .expect("set_upload");
    assert!(db.get_item(&r).expect("get_item").is_none(), "no item row");

    let report = abort_stale_uploads(&db, &client, &cfg, 7 * 24 * 60 * 60, now)
        .await
        .expect("sweep");
    assert_eq!(
        report.cleared_recordless,
        vec![(r.clone(), created.upload_id.clone())],
        "the aged recordless row is reported"
    );
    assert!(report.aborted_own.is_empty());
    assert_eq!(
        db.get_upload(&r).expect("get_upload"),
        None,
        "the local row no longer leaks"
    );
    assert!(
        h::backend_uploads(&client, &bucket).await.is_empty(),
        "the backend upload no longer leaks either (best-effort abort \
         under both candidate keys)"
    );
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
    // The first PUT to enter holds until a second transfer is in flight:
    // the pump must actually RUN two at once, so the `== 2` below detects
    // a pump silently degraded to serial admission (a plain `<= 2` would
    // pass vacuously for one).
    s3.gate_first_put_until
        .store(2, std::sync::atomic::Ordering::SeqCst);

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
    assert_eq!(
        s3.max_in_flight(),
        2,
        "exactly `concurrency` transfers in flight at peak (§2.4): \
         never more, and demonstrably not serial"
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

// ===========================================================================
// Review-round regressions: resume integrity, crash re-entry, NoSuchUpload
// recovery, completion recheck, badge fields, download retry-from-scratch
// ===========================================================================

/// Blocker regression: a rewrite that preserves BOTH size and mtime
/// (coarse-mtime filesystems like exFAT; XMP tools that deliberately
/// restore mtime) must not defeat the mid-resume source-change guard. The
/// persisted per-part MD5s are the authority: a recorded range that
/// re-reads with a different MD5 is a changed source, and the resume must
/// abort — never complete an object that is old-parts + new-parts with a
/// journal blake3 matching nothing stored anywhere.
#[tokio::test]
async fn resume_aborts_on_a_same_size_same_mtime_rewrite_via_recorded_part_md5s() {
    let (g, bucket, root, _dbdir, db) = scaffold!("tr-mp-md5-guard");
    let key = library_key(&rel("mp/md5guard.NEF"));
    let s3 = CountingS3::new(g.client());
    s3.fail_parts.lock().unwrap().insert((key.clone(), 3), 1);
    let cfg = h::test_cfg(&bucket, root.path());

    let bytes = h::three_part_bytes(61);
    let r = rel("mp/md5guard.NEF");
    let src = h::under(root.path(), "md5guard.NEF");
    h::write_file(&src, &bytes);
    h::seed_queued(&db, &r, Kind::Original, &src);

    upload_item(&db, &s3, &cfg, &r, &src)
        .await
        .expect_err("injected part-3 failure leaves parts 1-2 recorded");
    assert_eq!(db.upload_parts(&r).expect("parts").len(), 2);

    // Rewrite with the SAME length and restore the exact mtime — the
    // size+mtime recheck alone cannot see this.
    let mtime = filetime::FileTime::from_system_time(
        std::fs::metadata(&src)
            .expect("meta")
            .modified()
            .expect("mtime"),
    );
    let rewritten = h::patterned(bytes.len(), 199);
    assert_ne!(rewritten, bytes);
    h::write_file(&src, &rewritten);
    filetime::set_file_mtime(&src, mtime).expect("restore mtime");
    assert_eq!(
        h::mtime_unix_ns(&src),
        mtime.unix_seconds() * 1_000_000_000 + i64::from(mtime.nanoseconds()),
        "the rewrite preserved the mtime exactly"
    );

    let clean = CountingS3::new(g.client());
    let err = upload_item(&db, &clean, &cfg, &r, &src)
        .await
        .expect_err("the recorded part MD5s must catch the rewrite");
    assert!(
        matches!(err, TransferError::AbortedSourceChanged { .. }),
        "got {err:?}"
    );
    assert!(
        h::backend_uploads(&g.client(), &bucket).await.is_empty(),
        "the poisoned upload is aborted on the backend"
    );
    assert_eq!(db.get_upload(&r).expect("get_upload"), None);
    assert_eq!(h::state_of(&db, &r), ItemState::Dirty);
    assert_eq!(outbound_len(&db), 0, "nothing journaled for either version");

    // The re-marked item uploads the NEW version cleanly on the next pass.
    h::advance(&db, &r, &[ItemState::Queued]);
    let retry = CountingS3::new(g.client());
    let outcome = upload_item(&db, &retry, &cfg, &r, &src)
        .await
        .expect("fresh upload of the new version");
    assert_eq!(outcome.blake3, h::b3(&rewritten));
    let stored = h::get_bytes(&g.client(), &bucket, &key).await;
    assert_eq!(h::b3(&stored), h::b3(&rewritten));
}

/// The mid-resume source-change baseline is the facts captured when the
/// upload was CREATED — never the item record's `size`/`mtime_unix_ns`,
/// which per state.rs's §2.6 coordination note keep naming the last
/// *published* version while a newer one is in flight. A resume of a
/// re-edited item (record facts ≠ current file) must still resume, not
/// spuriously abort.
#[tokio::test]
async fn resume_ignores_item_record_facts_that_name_the_published_version() {
    let (g, bucket, root, _dbdir, db) = scaffold!("tr-mp-pub-facts");
    let key = library_key(&rel("mp/pubfacts.NEF"));
    let s3 = CountingS3::new(g.client());
    s3.fail_parts.lock().unwrap().insert((key.clone(), 2), 1);
    let cfg = h::test_cfg(&bucket, root.path());

    let bytes = h::three_part_bytes(67);
    let r = rel("mp/pubfacts.NEF");
    let src = h::under(root.path(), "pubfacts.NEF");
    h::write_file(&src, &bytes);
    h::seed_queued(&db, &r, Kind::Original, &src);

    upload_item(&db, &s3, &cfg, &r, &src)
        .await
        .expect_err("injected part-2 failure leaves a resumable upload");

    // Simulate the §2.6 chokepoint posture: the record keeps advertising
    // the previously PUBLISHED version's facts, which do not match the
    // file being uploaded.
    db.update_item(&r, ItemState::Queued, |rec| {
        rec.size = 123;
        rec.mtime_unix_ns = 456;
    })
    .expect("record keeps published facts");

    let clean = CountingS3::new(g.client());
    let outcome = upload_item(&db, &clean, &cfg, &r, &src)
        .await
        .expect("resume must compare against the upload-time facts and proceed");
    assert_eq!(
        clean.part_attempts_for(&key),
        vec![2, 3],
        "a genuine resume: only the missing parts go up"
    );
    assert_eq!(outcome.blake3, h::b3(&bytes));
    assert_eq!(h::state_of(&db, &r), ItemState::Synced);
}

/// A crash in the window between `CompleteMultipartUpload` and the
/// bookkeeping-clear transaction leaves `uploading` + a full part-record
/// set for an upload id the backend has discarded. Re-entry must adopt
/// the durably stored object (HEAD + the MD5-verified re-hash) and drive
/// it through verify + journal — not loop on `NoSuchUpload` until the
/// 7-day hygiene ages the record out.
#[tokio::test]
async fn resume_after_crash_between_complete_and_bookkeeping_adopts_the_stored_object() {
    let (g, bucket, root, _dbdir, db) = scaffold!("tr-mp-postcomplete");
    let client = g.client();
    let cfg = h::test_cfg(&bucket, root.path());
    let r = rel("mp/postcomplete.NEF");
    let key = library_key(&r);
    let bytes = h::three_part_bytes(71);
    let src = h::under(root.path(), "postcomplete.NEF");
    h::write_file(&src, &bytes);
    h::seed_queued(&db, &r, Kind::Original, &src);
    h::advance(&db, &r, &[ItemState::Uploading]);

    // Reproduce the exact durable state the crash leaves: upload record +
    // all part records persisted, object completed on the backend, upload
    // id discarded, nothing cleared or journaled locally.
    let created = client
        .create_multipart_upload(&bucket, &key, &PutObjectOptions::default())
        .await
        .expect("create");
    let mut completed = Vec::new();
    for (i, range) in [
        (1u32, 0..h::PART_5MIB as usize),
        (2, h::PART_5MIB as usize..2 * h::PART_5MIB as usize),
        (3, 2 * h::PART_5MIB as usize..bytes.len()),
    ] {
        let part = &bytes[range];
        let md5 = h::md5_b64(part);
        let out = client
            .upload_part(
                &bucket,
                &key,
                &created.upload_id,
                i,
                PartBody::from(Bytes::from(part.to_vec())),
                Some(&md5),
            )
            .await
            .expect("upload part");
        db.record_upload_part(
            &r,
            i,
            &UploadPart {
                etag: out.e_tag.clone(),
                md5_b64: md5,
            },
        )
        .expect("record part");
        completed.push(CompletedPart {
            part_number: i,
            e_tag: out.e_tag,
        });
    }
    db.set_upload(
        &r,
        &MultipartUploadState {
            upload_id: created.upload_id.clone(),
            part_size: cfg.part_size,
            started_unix: 1_769_900_000,
            size: bytes.len() as u64,
            mtime_unix_ns: h::mtime_unix_ns(&src),
        },
    )
    .expect("set_upload");
    client
        .complete_multipart_upload(&bucket, &key, &created.upload_id, &completed)
        .await
        .expect("complete (the backend now forgets the upload id)");

    // The startup sweep re-admits the stranded item (uploading → queued,
    // record kept) — the single-driver entry gate means upload_item never
    // accepts `uploading` directly.
    recover_interrupted(&db, 0).expect("recovery sweep");
    assert_eq!(h::state_of(&db, &r), ItemState::Queued);

    let s3 = CountingS3::new(g.client());
    let outcome = upload_item(&db, &s3, &cfg, &r, &src)
        .await
        .expect("re-entry adopts the completed upload instead of wedging");
    assert!(
        s3.part_attempts_for(&key).is_empty(),
        "nothing is re-uploaded"
    );
    assert_eq!(
        s3.create_calls.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "no new upload is created"
    );
    assert_eq!(outcome.blake3, h::b3(&bytes));
    let record = db.get_item(&r).expect("get").expect("exists");
    assert_eq!(record.state, ItemState::Synced);
    assert!(record.verified_remote);
    assert_eq!(db.get_upload(&r).expect("get_upload"), None);
    let entries = staged_entries(&db);
    assert_eq!(entries.len(), 1, "the crash-lost journal entry is staged");
    assert_eq!(entries[0].blake3, Some(h::b3(&bytes)));
}

/// An upload id swept out from under us (another device's hygiene, an
/// operator abort) must not wedge the item: the `NoSuchUpload` from
/// `upload_part` clears the dead record so the next pass restarts with a
/// fresh upload instead of retrying the dead id until the 7-day age-out.
#[tokio::test]
async fn an_upload_id_swept_elsewhere_clears_the_record_and_restarts_cleanly() {
    let (g, bucket, root, _dbdir, db) = scaffold!("tr-mp-swept");
    let key = library_key(&rel("mp/swept.NEF"));
    let s3 = CountingS3::new(g.client());
    s3.fail_parts.lock().unwrap().insert((key.clone(), 2), 1);
    let cfg = h::test_cfg(&bucket, root.path());

    let bytes = h::three_part_bytes(73);
    let r = rel("mp/swept.NEF");
    let src = h::under(root.path(), "swept.NEF");
    h::write_file(&src, &bytes);
    h::seed_queued(&db, &r, Kind::Original, &src);

    upload_item(&db, &s3, &cfg, &r, &src)
        .await
        .expect_err("injected part-2 failure leaves a resumable upload");
    let upload = db.get_upload(&r).expect("get_upload").expect("record");

    // Out-of-band sweep (another party) aborts our upload id.
    g.client()
        .abort_multipart_upload(&bucket, &key, &upload.upload_id)
        .await
        .expect("out-of-band abort");

    // The dead id can never resume. Garage answers the doomed part/
    // Complete sometimes with a typed NoSuchUpload (record cleared at
    // once) and sometimes by dropping the connection (a transport error,
    // which the engine correctly treats as retryable and keeps the record
    // for) — so drive passes like the pump would until the typed answer
    // lands. Each failing pass must leave the item resumable.
    let clean = CountingS3::new(g.client());
    let mut cleared = false;
    for _ in 0..5 {
        upload_item(&db, &clean, &cfg, &r, &src)
            .await
            .expect_err("the dead upload id cannot be resumed");
        assert_eq!(h::state_of(&db, &r), ItemState::Queued, "still resumable");
        if db.get_upload(&r).expect("get_upload").is_none() {
            cleared = true;
            break;
        }
    }
    assert!(
        cleared,
        "the dead record is cleared so the next pass restarts"
    );
    assert!(db.upload_parts(&r).expect("parts").is_empty());

    let retry = CountingS3::new(g.client());
    let mut outcome = None;
    for _ in 0..3 {
        // Like the pump: a transient transport failure under load keeps
        // the item resumable and the next pass retries.
        match upload_item(&db, &retry, &cfg, &r, &src).await {
            Ok(out) => {
                outcome = Some(out);
                break;
            }
            Err(_) => assert_eq!(h::state_of(&db, &r), ItemState::Queued),
        }
    }
    let outcome = outcome.expect("fresh restart succeeds");
    assert!(
        retry.create_calls.load(std::sync::atomic::Ordering::SeqCst) >= 1,
        "a brand-new upload is created"
    );
    assert_eq!(outcome.blake3, h::b3(&bytes));
    assert_eq!(h::state_of(&db, &r), ItemState::Synced);
}

/// A crash mid-single-PUT (or between the queued→uploading CAS and
/// `set_upload`) leaves `uploading` with no multipart record. The startup
/// sweep demotes it back to `queued` and the next pass restarts the
/// transfer — the item never wedges.
#[tokio::test]
async fn upload_item_restarts_after_a_crash_mid_single_put() {
    let (g, bucket, root, _dbdir, db) = scaffold!("tr-single-crash");
    let s3 = CountingS3::new(g.client());
    let cfg = h::test_cfg(&bucket, root.path());

    let bytes = h::patterned(200_000, 77);
    let r = rel("single/crash.NEF");
    let src = h::under(root.path(), "single-crash.NEF");
    h::write_file(&src, &bytes);
    h::seed_queued(&db, &r, Kind::Original, &src);
    // The crash signature: uploading, no upload record, queue row gone.
    h::advance(&db, &r, &[ItemState::Uploading]);
    assert_eq!(db.get_upload(&r).expect("get_upload"), None);

    recover_interrupted(&db, 0).expect("recovery sweep");
    assert_eq!(h::state_of(&db, &r), ItemState::Queued);

    let outcome = upload_item(&db, &s3, &cfg, &r, &src)
        .await
        .expect("the recovered item restarts the single PUT instead of wedging");
    assert_eq!(outcome.blake3, h::b3(&bytes));
    assert_eq!(h::state_of(&db, &r), ItemState::Synced);
    assert_eq!(outbound_len(&db), 1);
}

/// The startup sweep re-drives every crash-stranded pipeline-interior
/// state the pump cannot see (its queue rows were popped pre-crash).
#[test]
fn recover_interrupted_requeues_every_crash_stranded_state() {
    let (_dir, _path, db) = open_db(&dev(common::sync::DEV_A));
    let root = tempfile::tempdir().expect("tempdir");
    let mk = |name: &str, bytes: &[u8]| {
        let r = rel(name);
        let src = root.path().join(name.replace('/', "-"));
        h::write_file(&src, bytes);
        h::seed_queued(&db, &r, Kind::Original, &src);
        (r, src)
    };

    // Stranded in verifying (object stored, nothing journaled).
    let (verifying, _) = mk("rec/verifying.NEF", &h::patterned(64, 1));
    h::advance(
        &db,
        &verifying,
        &[ItemState::Uploading, ItemState::Verifying],
    );
    // Stranded in uploading WITH a multipart record (resumable in place).
    let (up_rec, up_src) = mk("rec/up-record.NEF", &h::patterned(64, 2));
    h::advance(&db, &up_rec, &[ItemState::Uploading]);
    db.set_upload(
        &up_rec,
        &MultipartUploadState {
            upload_id: "live-upload".into(),
            part_size: 5 * 1024 * 1024,
            started_unix: 0,
            size: 64,
            mtime_unix_ns: h::mtime_unix_ns(&up_src),
        },
    )
    .expect("set_upload");
    // Stranded in uploading with NO record (mid-single-PUT crash).
    let (up_bare, _) = mk("rec/up-bare.NEF", &h::patterned(64, 3));
    h::advance(&db, &up_bare, &[ItemState::Uploading]);
    // Stranded in downloading.
    let down = rel("rec/down.NEF");
    h::seed_pending_down(
        &db,
        &down,
        Kind::Original,
        &h::patterned(64, 4),
        1_700_000_000,
    );
    h::advance(&db, &down, &[ItemState::Downloading]);

    let report = recover_interrupted(&db, 1).expect("sweep");
    let mut requeued_up = report.requeued_uploads.clone();
    requeued_up.sort();
    let mut want_up = vec![verifying.clone(), up_rec.clone(), up_bare.clone()];
    want_up.sort();
    assert_eq!(requeued_up, want_up);
    assert_eq!(report.requeued_downloads, vec![down.clone()]);

    assert_eq!(h::state_of(&db, &verifying), ItemState::Queued);
    assert_eq!(
        h::state_of(&db, &up_rec),
        ItemState::Queued,
        "uploading demotes to queued — the single-driver entry gate means a \
         record still at uploading is a LIVE transfer, never a crash leftover"
    );
    assert!(
        db.get_upload(&up_rec).expect("get_upload").is_some(),
        "the multipart record survives the demotion: it, not the state, \
         is what makes the next upload_item pass a resume"
    );
    assert_eq!(h::state_of(&db, &up_bare), ItemState::Queued);
    assert_eq!(h::state_of(&db, &down), ItemState::PendingDown);
    assert_eq!(db.queue_len(Queue::Up).expect("queue_len"), 3);
    assert_eq!(db.queue_len(Queue::Down).expect("queue_len"), 1);

    // Idempotent: a second sweep finds nothing stranded and re-pushes
    // nothing twice.
    let again = recover_interrupted(&db, 1).expect("sweep again");
    assert_eq!(db.queue_len(Queue::Up).expect("queue_len"), 3);
    assert_eq!(db.queue_len(Queue::Down).expect("queue_len"), 1);
    assert!(again.requeued_uploads.is_empty());
    assert!(again.requeued_downloads.is_empty());
}

/// A [`ChunkSource`] that reads the real file and, the first time it is
/// opened, rewrites the file afterwards — a deterministic stand-in for a
/// third-party tool rewriting the source mid-upload.
struct RewriteAfterFirstRead {
    rewrite_to: Vec<u8>,
    rewritten: std::sync::atomic::AtomicBool,
}

impl ChunkSource for RewriteAfterFirstRead {
    async fn open(&self, path: &Path, start: u64) -> Result<SourceStream, std::io::Error> {
        use futures::StreamExt as _;
        let bytes = std::fs::read(path)?;
        if !self
            .rewritten
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            std::fs::write(path, &self.rewrite_to)?;
        }
        let tail: Vec<u8> = bytes
            .get(start as usize..)
            .map(|t| t.to_vec())
            .unwrap_or_default();
        let chunks: Vec<Result<Bytes, std::io::Error>> = tail
            .chunks(64 * 1024)
            .map(|c| Ok(Bytes::copy_from_slice(c)))
            .collect();
        Ok(futures::stream::iter(chunks).boxed())
    }
}

/// §2.4 completion recheck: a source rewritten mid-upload has its OLD
/// version completed, verified and journaled honestly, and the item is
/// immediately re-marked dirty so the new version uploads next — never
/// left silently `synced` against stale journaled bytes.
#[tokio::test]
async fn a_mid_upload_rewrite_journals_the_old_version_and_re_marks_dirty() {
    let (g, bucket, root, _dbdir, db) = scaffold!("tr-completion-recheck");
    let s3 = CountingS3::new(g.client());
    let cfg = h::test_cfg(&bucket, root.path());

    let old_bytes = h::patterned(150_000, 81);
    let new_bytes = h::patterned(180_000, 83);
    let r = rel("recheck/rewrite.NEF");
    let src = h::under(root.path(), "rewrite.NEF");
    h::write_file(&src, &old_bytes);
    h::seed_queued(&db, &r, Kind::Original, &src);

    let source = RewriteAfterFirstRead {
        rewrite_to: new_bytes.clone(),
        rewritten: std::sync::atomic::AtomicBool::new(false),
    };
    let outcome = upload_item_from(&db, &s3, &cfg, &r, &src, &source)
        .await
        .expect("the OLD version's upload completes");
    assert!(
        outcome.source_changed_at_completion,
        "the completion recheck must detect the rewrite"
    );
    assert_eq!(outcome.blake3, h::b3(&old_bytes), "old version journaled");
    let stored = h::get_bytes(&g.client(), &bucket, &library_key(&r)).await;
    assert_eq!(stored, old_bytes);
    let entries = staged_entries(&db);
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].blake3, Some(h::b3(&old_bytes)));
    assert_eq!(
        h::state_of(&db, &r),
        ItemState::Dirty,
        "the new version is re-marked dirty, not left silently synced"
    );
}

/// §2.2: sidecar `put` entries carry `rating`/`color_label` (grid badges
/// before the sidecar bytes download, §3.5) and original entries carry
/// the record's measured `w`/`h`.
#[tokio::test]
async fn journal_entries_carry_badges_and_dimensions() {
    let (g, bucket, root, _dbdir, db) = scaffold!("tr-badges");
    let s3 = CountingS3::new(g.client());
    let cfg = h::test_cfg(&bucket, root.path());

    // Sidecar with a rating and an upstream-style color tag.
    let sidecar =
        br#"{"version":2,"rating":4,"tags":["keeper","color:red"],"adjustments":{"exposure":0.5}}"#
            .to_vec();
    let rs = rel("badges/a.NEF");
    let src_s = h::under(root.path(), "a.NEF.rrdata");
    h::write_file(&src_s, &sidecar);
    h::seed_queued(&db, &rs, Kind::Sidecar, &src_s);
    upload_item(&db, &s3, &cfg, &rs, &src_s)
        .await
        .expect("sidecar upload");

    // Original whose record carries measured dimensions (the import unit
    // populates these).
    let bytes = h::patterned(100_000, 91);
    let ro = rel("badges/b.NEF");
    let src_o = h::under(root.path(), "b.NEF");
    h::write_file(&src_o, &bytes);
    h::seed_queued(&db, &ro, Kind::Original, &src_o);
    db.update_item(&ro, ItemState::Queued, |rec| {
        rec.w = Some(6048);
        rec.h = Some(4024);
    })
    .expect("record dimensions");
    upload_item(&db, &s3, &cfg, &ro, &src_o)
        .await
        .expect("original upload");

    let entries = staged_entries(&db);
    assert_eq!(entries.len(), 2);
    let se = entries
        .iter()
        .find(|e| e.kind == Kind::Sidecar)
        .expect("sidecar entry");
    assert_eq!(se.rating, Some(4), "sidecar entry carries the rating");
    assert_eq!(
        se.color_label.as_deref(),
        Some("red"),
        "sidecar entry carries the color label (from the color: tag)"
    );
    assert_eq!(se.sem_hash, Some(sem_hash(&sidecar).expect("sem")));
    let oe = entries
        .iter()
        .find(|e| e.kind == Kind::Original)
        .expect("original entry");
    assert_eq!(oe.w, Some(6048), "original entry carries measured w");
    assert_eq!(oe.h, Some(4024), "original entry carries measured h");
    assert_eq!(oe.rating, None, "badges are sidecar-entry fields");
}

/// An unparseable sidecar is caught BEFORE anything is stored: no PUT, no
/// bucket garbage, nothing journaled, and the item parks `dirty` (not a
/// re-upload-forever queue loop).
#[tokio::test]
async fn an_invalid_sidecar_is_never_uploaded_and_parks_dirty() {
    let (g, bucket, root, _dbdir, db) = scaffold!("tr-badsidecar-up");
    let s3 = CountingS3::new(g.client());
    let cfg = h::test_cfg(&bucket, root.path());

    let r = rel("up/bad.NEF");
    let src = h::under(root.path(), "bad.NEF.rrdata");
    h::write_file(&src, b"{ this is not valid sidecar json");
    h::seed_queued(&db, &r, Kind::Sidecar, &src);

    let err = upload_item(&db, &s3, &cfg, &r, &src)
        .await
        .expect_err("invalid sidecar must fail before the PUT");
    assert!(
        matches!(err, TransferError::SidecarInvalid { .. }),
        "got {err:?}"
    );
    assert!(s3.put_keys().is_empty(), "nothing was sent at all");
    let head = g.client().head_object(&bucket, &sidecar_key(&r)).await;
    assert!(
        head.expect_err("no object stored").is_no_such_key(),
        "no garbage object lands in the bucket"
    );
    assert_eq!(
        h::state_of(&db, &r),
        ItemState::Dirty,
        "parked dirty: retrying unchanged bytes can never succeed"
    );
    assert_eq!(outbound_len(&db), 0);
}

/// A stale .rr.part from a superseded object version (the journal head
/// advanced between a cut first attempt and the retry) must not condemn
/// an intact remote: the resumed mismatch discards the partial and
/// retries once from scratch, which succeeds.
#[tokio::test]
async fn a_stale_partial_from_a_superseded_version_retries_from_scratch_and_installs() {
    let (g, bucket, root, _dbdir, db) = scaffold!("tr-dl-stale-partial");
    let client = g.client();
    let cfg = h::test_cfg(&bucket, root.path());
    let r = rel("dl/stale.NEF");
    let key = library_key(&r);
    let v1 = h::patterned(600_000, 101);
    client
        .put_object(
            &bucket,
            &key,
            Bytes::from(v1.clone()),
            &PutObjectOptions::default(),
        )
        .await
        .expect("put v1");

    // Cut mid-download of v1: a partial of v1's prefix survives.
    let cut_at = 200_000u64;
    let s3 = CountingS3::new(g.client());
    s3.cut_get_after.lock().unwrap().insert(key.clone(), cut_at);
    h::seed_pending_down(&db, &r, Kind::Original, &v1, 1_700_000_800);
    let expect_v1 = ExpectedDownload {
        blake3: h::b3(&v1),
        size: v1.len() as u64,
        mtime_unix: 1_700_000_800,
    };
    download_item(&db, &s3, &cfg, &r, root.path(), &expect_v1)
        .await
        .expect_err("cut first attempt");

    // The remote is legitimately replaced (head advanced) with a
    // same-size v2, and the caller now expects v2.
    let v2 = h::patterned(600_000, 103);
    client
        .put_object(
            &bucket,
            &key,
            Bytes::from(v2.clone()),
            &PutObjectOptions::default(),
        )
        .await
        .expect("put v2");
    let expect_v2 = ExpectedDownload {
        blake3: h::b3(&v2),
        size: v2.len() as u64,
        mtime_unix: 1_700_000_900,
    };
    let outcome = download_item(&db, &s3, &cfg, &r, root.path(), &expect_v2)
        .await
        .expect("the stale partial is discarded and the retry installs v2");
    assert_eq!(
        outcome.resumed_from, 0,
        "the successful attempt ran from scratch"
    );
    let final_path = local_target_path(root.path(), &r, Kind::Original);
    assert_eq!(std::fs::read(&final_path).expect("installed"), v2);
    assert!(!partial_path(&final_path).exists());
    assert_eq!(
        h::state_of(&db, &r),
        ItemState::Hydrated,
        "an intact remote must never land corrupt_remote over a stale partial"
    );
    // The spliced resume ran first (ranged GET), then the from-scratch
    // retry (no range).
    let ranges = s3.get_ranges_for(&key);
    assert_eq!(
        ranges,
        vec![None, Some(ByteRange::From(cut_at)), None],
        "cut attempt, poisoned resume, then the clean from-scratch retry"
    );
}

/// Zero-byte objects round-trip: the upload path journals one and the
/// download path installs one (partial created unconditionally, so the
/// final rename cannot fail NotFound and retry forever).
#[tokio::test]
async fn zero_byte_objects_round_trip_both_ways() {
    let (g, bucket, root, _dbdir, db) = scaffold!("tr-zero-byte");
    let client = g.client();
    let cfg = h::test_cfg(&bucket, root.path());

    // Upload an empty original.
    let ru = rel("zero/up.NEF");
    let src = h::under(root.path(), "zero-up.NEF");
    h::write_file(&src, b"");
    h::seed_queued(&db, &ru, Kind::Original, &src);
    let s3 = CountingS3::new(g.client());
    let outcome = upload_item(&db, &s3, &cfg, &ru, &src)
        .await
        .expect("zero-byte upload");
    assert_eq!(outcome.size, 0);
    assert_eq!(outcome.blake3, h::b3(b""));
    assert_eq!(h::state_of(&db, &ru), ItemState::Synced);

    // Download an empty original advertised by a peer.
    let rd = rel("zero/down.NEF");
    let key = library_key(&rd);
    client
        .put_object(&bucket, &key, Bytes::new(), &PutObjectOptions::default())
        .await
        .expect("put empty");
    h::seed_pending_down(&db, &rd, Kind::Original, b"", 1_700_001_000);
    let expected = ExpectedDownload {
        blake3: h::b3(b""),
        size: 0,
        mtime_unix: 1_700_001_000,
    };
    let outcome = download_item(&db, &s3, &cfg, &rd, root.path(), &expected)
        .await
        .expect("zero-byte download must install, not fail NotFound forever");
    let final_path = local_target_path(root.path(), &rd, Kind::Original);
    assert_eq!(outcome.path, final_path);
    assert_eq!(std::fs::metadata(&final_path).expect("installed").len(), 0);
    assert_eq!(h::mtime_unix(&final_path), 1_700_001_000);
    assert!(!partial_path(&final_path).exists());
    assert_eq!(h::state_of(&db, &rd), ItemState::Hydrated);
}

/// A backend/intermediary that ignores the Range header must not get the
/// full object spliced after the partial: a resume whose response is not
/// partial (no Content-Range at our offset) falls back to a from-scratch
/// download of the full body it was handed.
#[tokio::test]
async fn a_range_ignoring_backend_falls_back_to_a_full_download() {
    let (g, bucket, root, _dbdir, db) = scaffold!("tr-dl-norange");
    let client = g.client();
    let cfg = h::test_cfg(&bucket, root.path());
    let r = rel("dl/norange.NEF");
    let key = library_key(&r);
    let bytes = h::patterned(500_000, 107);
    client
        .put_object(
            &bucket,
            &key,
            Bytes::from(bytes.clone()),
            &PutObjectOptions::default(),
        )
        .await
        .expect("put");

    let cut_at = 150_000u64;
    let mut s3 = CountingS3::new(g.client());
    s3.cut_get_after.lock().unwrap().insert(key.clone(), cut_at);
    s3.ignore_range = true;

    h::seed_pending_down(&db, &r, Kind::Original, &bytes, 1_700_001_100);
    let expected = ExpectedDownload {
        blake3: h::b3(&bytes),
        size: bytes.len() as u64,
        mtime_unix: 1_700_001_100,
    };
    download_item(&db, &s3, &cfg, &r, root.path(), &expected)
        .await
        .expect_err("cut first attempt leaves a partial");

    let outcome = download_item(&db, &s3, &cfg, &r, root.path(), &expected)
        .await
        .expect("resume against a range-ignoring backend must still succeed");
    assert_eq!(
        outcome.resumed_from, 0,
        "the full 200-style response was taken from scratch, never spliced"
    );
    // The engine DID ask for the range; the backend ignored it.
    let ranges = s3.get_ranges_for(&key);
    assert_eq!(ranges, vec![None, Some(ByteRange::From(cut_at))]);
    let final_path = local_target_path(root.path(), &r, Kind::Original);
    assert_eq!(std::fs::read(&final_path).expect("installed"), bytes);
    assert_eq!(h::state_of(&db, &r), ItemState::Hydrated);
}

// ===========================================================================
// Round-1 review regressions: adoption must prove ownership, single-driver
// entry gates, stub hydration, hashless-download parking, post-complete
// key replacement
// ===========================================================================

/// BLOCKER regression: the crash-after-Complete adoption must prove the
/// stored object is OURS, not merely the right size. Reproduces the
/// review probe: every part recorded locally (honest MD5s of the local
/// byte ranges), the upload id aborted out-of-band (the §2.4 cross-device
/// hygiene), and the shared key holding a same-size DIFFERENT object (a
/// sibling's version of the fixed-size camera RAW). Adopting it would
/// journal a blake3 no stored object has (§2.1.5 violation — every
/// downloader would condemn the intact remote). The resume must refuse
/// (the recorded-part multipart-ETag proof fails against the stored
/// single-PUT-shaped ETag), clear the dead record, leave the sibling's
/// object alone, and restart cleanly.
#[tokio::test]
async fn adoption_refuses_a_same_size_foreign_object_at_the_shared_key() {
    let (g, bucket, root, _dbdir, db) = scaffold!("tr-mp-adopt-foreign");
    let client = g.client();
    let cfg = h::test_cfg(&bucket, root.path());
    let r = rel("mp/adopt-foreign.NEF");
    let key = library_key(&r);
    let bytes = h::three_part_bytes(111);
    let src = h::under(root.path(), "adopt-foreign.NEF");
    h::write_file(&src, &bytes);
    h::seed_queued(&db, &r, Kind::Original, &src);

    // The durable state after a crash + out-of-band sweep: upload record
    // + every part record persisted, upload id dead on the backend.
    let created = client
        .create_multipart_upload(&bucket, &key, &PutObjectOptions::default())
        .await
        .expect("create");
    for (i, range) in [
        (1u32, 0..h::PART_5MIB as usize),
        (2, h::PART_5MIB as usize..2 * h::PART_5MIB as usize),
        (3, 2 * h::PART_5MIB as usize..bytes.len()),
    ] {
        let part = &bytes[range];
        db.record_upload_part(
            &r,
            i,
            &UploadPart {
                etag: h::md5_hex(part),
                md5_b64: h::md5_b64(part),
            },
        )
        .expect("record part");
    }
    db.set_upload(
        &r,
        &MultipartUploadState {
            upload_id: created.upload_id.clone(),
            part_size: cfg.part_size,
            started_unix: 1_769_900_000,
            size: bytes.len() as u64,
            mtime_unix_ns: h::mtime_unix_ns(&src),
        },
    )
    .expect("set_upload");
    client
        .abort_multipart_upload(&bucket, &key, &created.upload_id)
        .await
        .expect("out-of-band sweep aborts our id");
    // A sibling device stored a same-size, different-content version at
    // the shared key.
    let foreign = h::patterned(bytes.len(), 113);
    assert_ne!(foreign, bytes);
    client
        .put_object(
            &bucket,
            &key,
            Bytes::from(foreign.clone()),
            &PutObjectOptions::default(),
        )
        .await
        .expect("sibling PUT");

    // Like the swept-id test: Garage answers the doomed Complete sometimes
    // typed (record cleared at once) and sometimes with a dropped
    // connection (retryable), so drive passes like the pump would. NO pass
    // may adopt, journal, or condemn the sibling's intact object.
    let s3 = CountingS3::new(g.client());
    let mut cleared = false;
    for _ in 0..5 {
        let err = upload_item(&db, &s3, &cfg, &r, &src)
            .await
            .expect_err("a same-size foreign object must NOT be adopted");
        assert!(
            !matches!(
                err,
                TransferError::CorruptRemote { .. } | TransferError::IntegrityMismatch { .. }
            ),
            "the intact sibling object must not be condemned, got {err:?}"
        );
        assert_eq!(
            outbound_len(&db),
            0,
            "no journal entry may advertise a blake3 the bucket does not hold"
        );
        let record = db.get_item(&r).expect("get").expect("exists");
        assert!(!record.verified_remote, "never marked verified");
        assert_eq!(record.state, ItemState::Queued, "resumable, not wedged");
        if db.get_upload(&r).expect("get_upload").is_none() {
            cleared = true;
            break;
        }
    }
    assert!(
        cleared,
        "the dead record is cleared so the next pass restarts cleanly"
    );
    let stored = h::get_bytes(&client, &bucket, &key).await;
    assert_eq!(
        stored, foreign,
        "the failed resume left the sibling's object alone"
    );

    // The restart uploads OUR bytes and journals exactly what is stored.
    let retry = CountingS3::new(g.client());
    let mut outcome = None;
    for _ in 0..3 {
        match upload_item(&db, &retry, &cfg, &r, &src).await {
            Ok(out) => {
                outcome = Some(out);
                break;
            }
            Err(_) => assert_eq!(h::state_of(&db, &r), ItemState::Queued),
        }
    }
    let outcome = outcome.expect("fresh restart succeeds");
    assert_eq!(outcome.blake3, h::b3(&bytes));
    let stored = h::get_bytes(&client, &bucket, &key).await;
    assert_eq!(h::b3(&stored), h::b3(&bytes));
    let entries = staged_entries(&db);
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0].blake3,
        Some(Blake3Hex::from_bytes(&stored)),
        "journal blake3 == blake3 of what the bucket actually stores"
    );
}

/// The positive adoption counterpart lives in
/// `resume_after_crash_between_complete_and_bookkeeping_adopts_the_stored_object`,
/// which (post round 1) passes only because Garage's stored multipart
/// ETag equals the one derivable from the recorded part MD5s — the §8
/// ETag-convention proof the adoption guard relies on.
///
/// The single-driver entry gate, upload side: a record sitting at
/// `uploading` means a LIVE concurrent transfer owns the item (the
/// startup sweep demotes crash leftovers to `queued` before any pump
/// pass), so a second caller is refused typed before touching the shared
/// multipart bookkeeping.
#[tokio::test]
async fn upload_item_refuses_entry_while_another_transfer_is_live() {
    let (g, bucket, root, _dbdir, db) = scaffold!("tr-up-live");
    let s3 = CountingS3::new(g.client());
    let cfg = h::test_cfg(&bucket, root.path());
    let bytes = h::three_part_bytes(121);
    let r = rel("up/live.NEF");
    let src = h::under(root.path(), "live.NEF");
    h::write_file(&src, &bytes);
    h::seed_queued(&db, &r, Kind::Original, &src);
    // The live first driver: queued → uploading CAS done, multipart
    // record persisted, parts in flight right now.
    h::advance(&db, &r, &[ItemState::Uploading]);
    db.set_upload(
        &r,
        &MultipartUploadState {
            upload_id: "live-first-driver".into(),
            part_size: cfg.part_size,
            started_unix: 0,
            size: bytes.len() as u64,
            mtime_unix_ns: h::mtime_unix_ns(&src),
        },
    )
    .expect("set_upload");

    let err = upload_item(&db, &s3, &cfg, &r, &src)
        .await
        .expect_err("a second concurrent driver must be refused");
    assert!(
        matches!(
            &err,
            TransferError::State(StateError::StaleState {
                found: Some(ItemState::Uploading),
                ..
            })
        ),
        "got {err:?}"
    );
    assert_eq!(
        h::state_of(&db, &r),
        ItemState::Uploading,
        "the live transfer still owns the item"
    );
    assert!(
        db.get_upload(&r).expect("get_upload").is_some(),
        "the live transfer's bookkeeping is untouched"
    );
    assert!(
        s3.part_calls().is_empty() && s3.put_keys().is_empty(),
        "the refused driver sent nothing"
    );
    assert_eq!(
        s3.create_calls.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "and created no competing upload"
    );
}

/// The single-driver entry gate, download side — the §3.5 `ensure_local`
/// race shape: a second `download_item` on an item already `downloading`
/// is refused typed, never admitted as a second writer onto the live
/// transfer's `.rr.part` (which could install a file the live writer
/// keeps appending to). The crash-leftover shape goes through
/// `recover_interrupted` instead, after which the download resumes off
/// the surviving partial.
#[tokio::test]
async fn download_item_refuses_entry_while_another_transfer_is_live() {
    let (g, bucket, root, _dbdir, db) = scaffold!("tr-dl-live");
    let client = g.client();
    let cfg = h::test_cfg(&bucket, root.path());
    let r = rel("dl/live.NEF");
    let key = library_key(&r);
    let bytes = h::patterned(400_000, 131);
    client
        .put_object(
            &bucket,
            &key,
            Bytes::from(bytes.clone()),
            &PutObjectOptions::default(),
        )
        .await
        .expect("put");
    let mtime = 1_700_002_000i64;
    h::seed_pending_down(&db, &r, Kind::Original, &bytes, mtime);
    // The live first driver: pending_down → downloading CAS done, a
    // partial growing on disk.
    h::advance(&db, &r, &[ItemState::Downloading]);
    let final_path = local_target_path(root.path(), &r, Kind::Original);
    h::write_file(&partial_path(&final_path), &bytes[..8192]);

    let expected = ExpectedDownload {
        blake3: h::b3(&bytes),
        size: bytes.len() as u64,
        mtime_unix: mtime,
    };
    let s3 = CountingS3::new(g.client());
    let err = download_item(&db, &s3, &cfg, &r, root.path(), &expected)
        .await
        .expect_err("a second concurrent driver must be refused");
    assert!(
        matches!(
            &err,
            TransferError::State(StateError::StaleState {
                found: Some(ItemState::Downloading),
                ..
            })
        ),
        "got {err:?}"
    );
    assert!(s3.get_requests().is_empty(), "no second writer, no network");
    assert_eq!(
        std::fs::read(partial_path(&final_path)).expect("partial intact"),
        &bytes[..8192],
        "the live transfer's partial is untouched"
    );
    assert!(!final_path.exists(), "nothing installed");
    assert_eq!(
        h::state_of(&db, &r),
        ItemState::Downloading,
        "the live transfer still owns the item"
    );

    // The CRASH shape: recover_interrupted demotes the stranded item,
    // then the next attempt resumes off the surviving partial.
    recover_interrupted(&db, 0).expect("recovery sweep");
    assert_eq!(h::state_of(&db, &r), ItemState::PendingDown);
    let outcome = download_item(&db, &s3, &cfg, &r, root.path(), &expected)
        .await
        .expect("recovered download completes");
    assert_eq!(outcome.resumed_from, 8192, "resumed off the partial");
    assert_eq!(std::fs::read(&final_path).expect("installed"), bytes);
    assert!(!partial_path(&final_path).exists());
    assert_eq!(h::state_of(&db, &r), ItemState::Hydrated);
}

/// §3.5 hydration from `stub` — the entry edge the P2 `ensure_local` unit
/// drives: CAS stub → downloading, stream to the `.rr.part`, rename
/// atomically OVER the 0-byte stub at the final path, restore the remote
/// mtime (the thumbnail-cache-hash stability §3.5 depends on), land
/// hydrated.
#[tokio::test]
async fn download_from_stub_replaces_the_stub_and_restores_mtime() {
    let (g, bucket, root, _dbdir, db) = scaffold!("tr-dl-stub");
    let client = g.client();
    let cfg = h::test_cfg(&bucket, root.path());
    let r = rel("dl/stub.NEF");
    let key = library_key(&r);
    let bytes = h::patterned(400_000, 137);
    client
        .put_object(
            &bucket,
            &key,
            Bytes::from(bytes.clone()),
            &PutObjectOptions::default(),
        )
        .await
        .expect("put");

    let mtime = 1_700_002_100i64;
    // An evicted original: record at `stub`, 0-byte placeholder at the
    // final path.
    let mut record = h::item_record(
        Kind::Original,
        ItemState::Stub,
        bytes.len() as u64,
        mtime * 1_000_000_000,
        db.device_id(),
    );
    record.blake3 = Some(h::b3(&bytes));
    assert!(db.insert_item(&r, &record).expect("insert stub item"));
    let final_path = local_target_path(root.path(), &r, Kind::Original);
    h::write_file(&final_path, b"");
    assert_eq!(std::fs::metadata(&final_path).expect("stub").len(), 0);

    let expected = ExpectedDownload {
        blake3: h::b3(&bytes),
        size: bytes.len() as u64,
        mtime_unix: mtime,
    };
    let s3 = CountingS3::new(g.client());
    let outcome = download_item(&db, &s3, &cfg, &r, root.path(), &expected)
        .await
        .expect("hydration from stub");
    assert_eq!(outcome.path, final_path);
    assert_eq!(
        std::fs::read(&final_path).expect("installed"),
        bytes,
        "the stub is replaced by the verified bytes"
    );
    assert_eq!(
        h::mtime_unix(&final_path),
        mtime,
        "mtime restored (§3.5 thumbnail-hash stability)"
    );
    assert!(!partial_path(&final_path).exists(), "temp gone");
    assert_eq!(h::state_of(&db, &r), ItemState::Hydrated);
}

/// A `pending_down` item whose record carries no `blake3` can never
/// download verified, and nothing inside the engine can ever supply the
/// hash: the pump records the typed failure once and PARKS the item off
/// the queue (state stays `pending_down`) instead of popping and
/// re-failing it on every pass forever.
#[tokio::test]
async fn pump_parks_a_hashless_download_instead_of_requeueing_forever() {
    let (g, bucket, root, _dbdir, db) = scaffold!("tr-pump-hashless");
    let cfg = h::test_cfg(&bucket, root.path());
    let r = rel("pump-park/nohash.NEF");
    let record = h::item_record(
        Kind::Original,
        ItemState::PendingDown,
        100,
        0,
        db.device_id(),
    );
    assert!(record.blake3.is_none(), "the record carries no hash");
    assert!(db.insert_item(&r, &record).expect("insert"));
    assert!(db.queue_push(Queue::Down, &r, 0).expect("push"));

    let s3 = CountingS3::new(g.client());
    let summary = pump_downloads(&db, &s3, &cfg, 2, &CancelFlag::new())
        .await
        .expect("pump");
    assert!(summary.completed.is_empty());
    assert_eq!(summary.failed.len(), 1);
    assert_eq!(summary.failed[0].0, r);
    assert_eq!(
        h::state_of(&db, &r),
        ItemState::PendingDown,
        "parked in its queueable state (a later journal apply re-queues it)"
    );
    assert_eq!(
        db.queue_len(Queue::Down).expect("queue_len"),
        0,
        "NOT re-queued: the engine can never heal a missing hash"
    );
    assert!(
        s3.get_requests().is_empty(),
        "the unverifiable download is never attempted"
    );

    // The next pass finds nothing: no unbounded churn.
    let again = pump_downloads(&db, &s3, &cfg, 2, &CancelFlag::new())
        .await
        .expect("pump again");
    assert!(again.completed.is_empty());
    assert!(again.failed.is_empty());
}

/// §2.4 `verifying`, multipart path on a digest-verifying backend: a HEAD
/// ETag that no longer equals what Complete returned means the shared key
/// was replaced out from under us between Complete and verify — the item
/// lands `corrupt_remote` with NOTHING journaled (journaling our blake3
/// against the replaced object would poison every downloader, §2.1.5).
#[tokio::test]
async fn multipart_verify_catches_a_post_complete_key_replacement() {
    let (g, bucket, root, _dbdir, db) = scaffold!("tr-verify-replaced");
    let r = rel("verify/replaced.NEF");
    let key = library_key(&r);
    let s3 = CountingS3::new(g.client());
    // Deterministic stand-in for the sibling replacement: HEAD reports a
    // different (single-PUT-shaped) ETag than our Complete returned.
    s3.fake_head_etags
        .lock()
        .unwrap()
        .insert(key.clone(), "d41d8cd98f00b204e9800998ecf8427e".to_string());
    let cfg = h::test_cfg(&bucket, root.path());
    let bytes = h::three_part_bytes(141);
    let src = h::under(root.path(), "replaced.NEF");
    h::write_file(&src, &bytes);
    h::seed_queued(&db, &r, Kind::Original, &src);

    let err = upload_item(&db, &s3, &cfg, &r, &src)
        .await
        .expect_err("verify must catch the replacement");
    match err {
        TransferError::CorruptRemote { relkey, .. } => assert_eq!(relkey, r),
        other => panic!("expected CorruptRemote, got {other:?}"),
    }
    assert_eq!(h::state_of(&db, &r), ItemState::CorruptRemote);
    assert_eq!(outbound_len(&db), 0, "nothing journaled");
}
