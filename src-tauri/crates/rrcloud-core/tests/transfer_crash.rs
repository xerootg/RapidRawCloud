//! Crash-injection test for the §2.4 multipart upload resume: a child
//! process (this binary re-exec'd, dispatched on
//! `RRCLOUD_TRANSFER_CRASH_CHILD` — same pattern as `tests/state_crash.rs`)
//! uploads a 3-part object against the shared Garage server through a
//! wrapper that ACKs each completed part on stdout and then stalls
//! forever before the next one. The parent waits for the ACK, SIGKILLs
//! the child **between parts**, reopens the state db, and RESUMES with a
//! fresh engine instance, asserting:
//!
//! - completed parts are NOT re-uploaded (the counting wrapper saw
//!   `upload_part` only for the missing parts, cross-checked against
//!   `ListParts`),
//! - the final object's bytes hash to the source's blake3,
//! - exactly one journal `put` entry is staged, carrying that blake3
//!   (the child crashed before the verify-commit, so it staged nothing).
//!
//! Linux-only (SIGKILL timing assumptions); elsewhere the binary prints a
//! skip notice and exits 0. Without a Garage binary (and `CI` unset) it
//! skips like the rest of the Garage-backed suite.

mod common;

#[cfg(not(target_os = "linux"))]
fn main() {
    println!("transfer_crash: skipped (linux-only crash-injection suite)");
}

#[cfg(target_os = "linux")]
fn main() {
    linux::main();
}

#[cfg(target_os = "linux")]
mod linux {
    use std::io::Write as _;
    use std::path::{Path, PathBuf};
    use std::process::{Child, Command, Stdio};
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::{Duration, Instant};

    use bytes::Bytes;
    use futures::stream::BoxStream;
    use rrcloud_core::clock::DeviceId;
    use rrcloud_core::journal::{JournalEntry, JournalEntryExt as _, Kind};
    use rrcloud_core::keys::{library_key, RelKey};
    use rrcloud_core::s3::{
        ByteRange, CompleteMultipartUploadOutput, CompletedPart, CreateMultipartUploadOutput,
        GetObjectOutput, HeadObjectOutput, ListMultipartUploadsOutput, ListMultipartUploadsRequest,
        ListObjectsV2Output, ListObjectsV2Request, ListPartsOutput, ListPartsRequest, PartBody,
        PutObjectOptions, PutObjectOutput, S3Api, S3Client, S3Config, S3Error, S3TransferApi,
        UploadPartOutput,
    };
    use rrcloud_core::semhash::Blake3Hex;
    use rrcloud_core::state::{ItemState, SyncDb};
    use rrcloud_core::transfer::{recover_interrupted, upload_item};

    use crate::common::garage;
    use crate::common::transfer as h;
    use crate::common::transfer::CountingS3;

    const CHILD_ENV: &str = "RRCLOUD_TRANSFER_CRASH_CHILD";
    const DB_ENV: &str = "RRCLOUD_TRANSFER_CRASH_DB";
    const ENDPOINT_ENV: &str = "RRCLOUD_TRANSFER_CRASH_ENDPOINT";
    const ACCESS_ENV: &str = "RRCLOUD_TRANSFER_CRASH_ACCESS";
    const SECRET_ENV: &str = "RRCLOUD_TRANSFER_CRASH_SECRET";
    const BUCKET_ENV: &str = "RRCLOUD_TRANSFER_CRASH_BUCKET";
    const SRC_ENV: &str = "RRCLOUD_TRANSFER_CRASH_SRC";
    const STALL_AFTER_ENV: &str = "RRCLOUD_TRANSFER_CRASH_STALL_AFTER";

    const DEV_SELF: &str = "d1f0c2aa-9d2b-4a6e-8f1c-3b7d5e9a0c42";
    const RELKEY: &str = "crash/mp-upload.NEF";
    /// Parts the child completes before stalling (of 3).
    const STALL_AFTER_PARTS: u32 = 1;

    fn dev() -> DeviceId {
        DeviceId::new(DEV_SELF).expect("valid device id")
    }

    fn relkey() -> RelKey {
        RelKey::new(RELKEY).expect("valid relkey")
    }

    fn env(name: &str) -> String {
        std::env::var(name).unwrap_or_else(|_| panic!("missing env {name}"))
    }

    pub fn main() {
        match std::env::var(CHILD_ENV).ok().as_deref() {
            Some("upload") => child_upload(),
            Some(other) => panic!("unknown child mode {other:?}"),
            None => parent(),
        }
    }

    // -- child -------------------------------------------------------------

    /// Delegating wrapper that prints `PART <n>` after each committed
    /// part and parks forever (awaiting the parent's SIGKILL) before
    /// starting the part after `stall_after` — so the kill deterministically
    /// lands *between* parts, after the completed part's redb record.
    struct AckingS3 {
        inner: S3Client,
        completed: AtomicU32,
        stall_after: u32,
    }

    fn ack(line: &str) {
        let mut out = std::io::stdout().lock();
        writeln!(out, "{line}").expect("write ack");
        out.flush().expect("flush ack");
    }

    impl S3Api for AckingS3 {
        async fn put_object(
            &self,
            bucket: &str,
            key: &str,
            body: Bytes,
            opts: &PutObjectOptions,
        ) -> Result<PutObjectOutput, S3Error> {
            self.inner.put_object(bucket, key, body, opts).await
        }

        async fn get_object(
            &self,
            bucket: &str,
            key: &str,
            range: Option<ByteRange>,
        ) -> Result<GetObjectOutput, S3Error> {
            self.inner.get_object(bucket, key, range).await
        }

        async fn head_object(&self, bucket: &str, key: &str) -> Result<HeadObjectOutput, S3Error> {
            self.inner.head_object(bucket, key).await
        }

        async fn list_objects_v2(
            &self,
            bucket: &str,
            request: &ListObjectsV2Request,
        ) -> Result<ListObjectsV2Output, S3Error> {
            self.inner.list_objects_v2(bucket, request).await
        }
    }

    impl S3TransferApi for AckingS3 {
        async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), S3Error> {
            self.inner.delete_object(bucket, key).await
        }

        async fn create_multipart_upload(
            &self,
            bucket: &str,
            key: &str,
            opts: &PutObjectOptions,
        ) -> Result<CreateMultipartUploadOutput, S3Error> {
            let out = self
                .inner
                .create_multipart_upload(bucket, key, opts)
                .await?;
            ack("CREATED");
            Ok(out)
        }

        async fn upload_part(
            &self,
            bucket: &str,
            key: &str,
            upload_id: &str,
            part_number: u32,
            body: PartBody,
            content_md5: Option<&str>,
        ) -> Result<UploadPartOutput, S3Error> {
            if self.completed.load(Ordering::SeqCst) >= self.stall_after {
                // The previous part's {part_no, etag, md5} record was
                // committed by the engine before it asked for this one.
                ack("STALLED");
                std::future::pending::<()>().await;
                unreachable!("parked forever awaiting SIGKILL");
            }
            let out = self
                .inner
                .upload_part(bucket, key, upload_id, part_number, body, content_md5)
                .await?;
            let n = self.completed.fetch_add(1, Ordering::SeqCst) + 1;
            ack(&format!("PART {n}"));
            Ok(out)
        }

        async fn complete_multipart_upload(
            &self,
            bucket: &str,
            key: &str,
            upload_id: &str,
            parts: &[CompletedPart],
        ) -> Result<CompleteMultipartUploadOutput, S3Error> {
            self.inner
                .complete_multipart_upload(bucket, key, upload_id, parts)
                .await
        }

        async fn abort_multipart_upload(
            &self,
            bucket: &str,
            key: &str,
            upload_id: &str,
        ) -> Result<(), S3Error> {
            self.inner
                .abort_multipart_upload(bucket, key, upload_id)
                .await
        }

        async fn list_multipart_uploads(
            &self,
            bucket: &str,
            request: &ListMultipartUploadsRequest,
        ) -> Result<ListMultipartUploadsOutput, S3Error> {
            self.inner.list_multipart_uploads(bucket, request).await
        }

        async fn list_parts(
            &self,
            bucket: &str,
            key: &str,
            upload_id: &str,
            request: &ListPartsRequest,
        ) -> Result<ListPartsOutput, S3Error> {
            self.inner.list_parts(bucket, key, upload_id, request).await
        }
    }

    // Silence a dead-code lint on the trait types the wrapper only passes
    // through.
    #[allow(dead_code)]
    type Unused = BoxStream<'static, ()>;

    fn s3_config_from_env() -> S3Config {
        S3Config {
            endpoint: env(ENDPOINT_ENV),
            region: "garage".to_string(),
            access_key_id: env(ACCESS_ENV),
            secret_access_key: env(SECRET_ENV),
            connect_timeout: Some(Duration::from_secs(10)),
            read_timeout: Some(Duration::from_secs(30)),
            request_timeout: None,
        }
    }

    fn child_upload() {
        let db_path = PathBuf::from(env(DB_ENV));
        let bucket = env(BUCKET_ENV);
        let src = PathBuf::from(env(SRC_ENV));
        let stall_after: u32 = env(STALL_AFTER_ENV).parse().expect("stall count");

        let db = SyncDb::open(&db_path, Some(dev())).expect("child open db");
        let r = relkey();
        h::seed_queued(&db, &r, Kind::Original, &src);

        let client = S3Client::new(s3_config_from_env()).expect("client");
        let s3 = AckingS3 {
            inner: client,
            completed: AtomicU32::new(0),
            stall_after,
        };
        let cfg = h::test_cfg(&bucket, src.parent().expect("src parent"));

        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let result = rt.block_on(upload_item(&db, &s3, &cfg, &r, &src));
        // The parent SIGKILLs us while STALLED; reaching here is a bug.
        eprintln!("child unexpectedly finished: {result:?}");
        std::process::exit(3);
    }

    // -- parent ------------------------------------------------------------

    struct KillOnDrop(Option<Child>);

    impl Drop for KillOnDrop {
        fn drop(&mut self) {
            if let Some(child) = &mut self.0 {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }

    fn complete_lines(raw: &[u8]) -> Vec<String> {
        let mut lines = Vec::new();
        let mut rest = raw;
        while let Some(pos) = rest.iter().position(|&b| b == b'\n') {
            lines.push(String::from_utf8_lossy(&rest[..pos]).into_owned());
            rest = &rest[pos + 1..];
        }
        lines
    }

    fn parent() {
        let Some(g) = garage::shared() else {
            println!("transfer_crash: skipped (no Garage binary and CI unset)");
            return;
        };
        println!("transfer_crash: multipart crash-resume scenario");

        let bucket = g.create_unique_bucket("tr-crash");
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("state.redb");
        let log_path = dir.path().join("acks.log");
        let src = dir.path().join("crash-src.NEF");
        let bytes = h::three_part_bytes(53);
        std::fs::write(&src, &bytes).expect("write source");

        let log = std::fs::File::create(&log_path).expect("create ack log");
        let child = Command::new(std::env::current_exe().expect("current_exe"))
            .env(CHILD_ENV, "upload")
            .env(DB_ENV, &db_path)
            .env(ENDPOINT_ENV, g.endpoint())
            .env(ACCESS_ENV, &g.access_key_id)
            .env(SECRET_ENV, &g.secret_access_key)
            .env(BUCKET_ENV, &bucket)
            .env(SRC_ENV, &src)
            .env(STALL_AFTER_ENV, STALL_AFTER_PARTS.to_string())
            .stdout(Stdio::from(log))
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn upload child");
        let mut guard = KillOnDrop(Some(child));

        // Wait until the child has committed part 1 and parked before
        // part 2 (the STALLED ack is printed strictly after the engine
        // persisted part 1's record and asked for the next part).
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            let raw = std::fs::read(&log_path).expect("read ack log");
            let lines = complete_lines(&raw);
            if lines.iter().any(|l| l == "STALLED") {
                assert!(
                    lines
                        .iter()
                        .any(|l| l == &format!("PART {STALL_AFTER_PARTS}")),
                    "STALLED must come after PART {STALL_AFTER_PARTS}; log: {lines:?}"
                );
                break;
            }
            let child = guard.0.as_mut().expect("child present");
            if let Some(status) = child.try_wait().expect("try_wait") {
                panic!(
                    "upload child exited on its own ({status:?}) before stalling; \
                     ack log: {lines:?}"
                );
            }
            assert!(
                Instant::now() < deadline,
                "child made no progress within 120s; ack log: {lines:?}"
            );
            std::thread::sleep(Duration::from_millis(20));
        }

        // SIGKILL between parts, and verify it died by exactly that.
        let mut child = guard.0.take().expect("child present");
        child.kill().expect("SIGKILL child");
        let status = child.wait().expect("reap child");
        assert_eq!(
            std::os::unix::process::ExitStatusExt::signal(&status),
            Some(libc::SIGKILL),
            "child must have died by the parent's SIGKILL, got {status:?}"
        );

        // Reopen (SIGKILL released the redb lock) and inspect what was
        // durably committed before the crash.
        let db = SyncDb::open(&db_path, None).expect("reopen after SIGKILL");
        let r = relkey();
        let key = library_key(&r);
        let upload = db
            .get_upload(&r)
            .expect("get_upload")
            .expect("upload_id persisted before the first part");
        assert!(!upload.upload_id.is_empty());
        let parts: Vec<u32> = db
            .upload_parts(&r)
            .expect("upload_parts")
            .iter()
            .map(|(n, _)| *n)
            .collect();
        assert_eq!(
            parts,
            (1..=STALL_AFTER_PARTS).collect::<Vec<_>>(),
            "exactly the completed parts have durable records"
        );
        assert_eq!(
            db.get_item(&r)
                .expect("get item")
                .expect("item exists")
                .state,
            ItemState::Uploading,
            "the crash left the item mid-uploading"
        );
        assert_eq!(db.outbound_len().expect("outbound_len"), 0);

        // The startup sweep re-admits the stranded item (uploading →
        // queued, multipart record kept): upload_item's single-driver
        // entry gate accepts only `queued`.
        let report = recover_interrupted(&db, 0).expect("recovery sweep");
        assert_eq!(report.requeued_uploads, vec![r.clone()]);
        assert_eq!(
            db.get_item(&r)
                .expect("get item")
                .expect("item exists")
                .state,
            ItemState::Queued,
            "recovery demotes the stranded uploading item back to queued"
        );
        assert!(
            db.get_upload(&r).expect("get_upload").is_some(),
            "the multipart record survives recovery and drives the resume"
        );

        // RESUME with a fresh engine instance and a counting client.
        let counting = CountingS3::new(g.client());
        let cfg = h::test_cfg(&bucket, dir.path());
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let outcome = rt
            .block_on(upload_item(&db, &counting, &cfg, &r, &src))
            .expect("resume after crash");

        // Completed parts are never re-uploaded: only the missing ones.
        let expected_missing: Vec<u32> = ((STALL_AFTER_PARTS + 1)..=3).collect();
        assert_eq!(
            counting.part_attempts_for(&key),
            expected_missing,
            "CountingS3 upload_part calls == missing parts only"
        );
        assert_eq!(
            counting.create_calls.load(Ordering::SeqCst),
            0,
            "resume reuses the persisted upload_id"
        );
        assert!(
            counting.list_parts_calls.load(Ordering::SeqCst) >= 1,
            "resume cross-checks via ListParts"
        );

        // Final object hash == source; journal entry correct.
        assert_eq!(outcome.blake3, Blake3Hex::from_bytes(&bytes));
        let stored = rt.block_on(h::get_bytes(&g.client(), &bucket, &key));
        assert_eq!(
            Blake3Hex::from_bytes(&stored),
            Blake3Hex::from_bytes(&bytes)
        );
        let staged = db.iter_outbound().expect("iter_outbound");
        assert_eq!(staged.len(), 1, "exactly one journal put entry");
        let entry =
            JournalEntry::from_json_line(std::str::from_utf8(&staged[0].1).expect("utf8 entry"))
                .expect("entry decodes");
        assert_eq!(entry.key, key);
        assert_eq!(entry.blake3, Some(Blake3Hex::from_bytes(&bytes)));
        assert_eq!(entry.size, Some(bytes.len() as u64));
        let record = db.get_item(&r).expect("get item").expect("item exists");
        assert_eq!(record.state, ItemState::Synced);
        assert!(record.verified_remote);
        assert_eq!(db.get_upload(&r).expect("get_upload"), None);

        let _ = Path::new("");
        println!("transfer_crash: ok");
    }
}
