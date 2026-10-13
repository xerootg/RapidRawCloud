//! Tombstone-GC LIST→GET swap (reviewer follow-up to the server-asserted
//! age fix): the age GC trusts comes from the **listing** entry, but the
//! body GC acts on comes from a **later `GetObject`** of the same key, and
//! nothing ties the two together.
//!
//! `tombstone_gc` (architecture §2.10) ages each tombstone by the
//! `LastModified` the `ListObjectsV2` entry carries (server-asserted, cannot
//! be backdated by the writer) and then GETs the tombstone body. It never
//! compares the GET's ETag with the listed ETag, and it never checks that
//! `tombstone_key(&body.relkey)` is the key it listed. Every destructive
//! step is driven by the **body**: the `library/…` data keys it DELETEs are
//! `library_key(&body.relkey)` / `sidecar_key(&body.relkey)`, the
//! deleted-set row it folds is for `body.relkey`, and supersession is
//! judged against `body.vv`.
//!
//! So a peer holding the bucket credentials overwrites an honestly-old
//! tombstone (item A, genuinely past grace) between the runner's LIST and
//! its GET with a body naming a *different, live* item B and a vv that
//! dominates B's live version. GC pairs A's old `LastModified` (past grace)
//! with B's fresh body: B's original and sidecar are DELETEd and B's
//! deletion is folded into the runner's deleted set, although B never had an
//! aged tombstone — the very thing the server-asserted age was meant to
//! make impossible.
//!
//! The swap is injected through a delegating [`S3TransferApi`] wrapper (the
//! same seam `download_size_cap.rs` uses): on the first `get_object` of A's
//! tombstone key it re-PUTs the hostile body at that key and then returns
//! the fresh body, exactly as a racing writer would.
//!
//! The positive control runs the same scenario without the swap and asserts
//! A's tombstone IS collected (so a fix cannot be "never GC").

mod common;

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use common::garage;
use common::sync::{dev, open_db, rel, DEV_A, DEV_B, DEV_X};

use rrcloud_core::clock::{DeviceId, VersionVector};
use rrcloud_core::compact::{
    tombstone_gc, CompactConfig, ServerClock, RECENTLY_DELETED_GRACE_SECS,
};
use rrcloud_core::journal::{Kind, Tombstone};
use rrcloud_core::keys::{
    device_registry_key, library_key, preview_key, sidecar_key, thumb_key, tombstone_key, RelKey,
    ThumbSize,
};
use rrcloud_core::manifest::get_manifest;
use rrcloud_core::publisher::{DeviceEntry, ProtoSupport, PROTO_READ, PROTO_WRITE};
use rrcloud_core::s3::{
    ByteRange, CompleteMultipartUploadOutput, CompletedPart, CreateMultipartUploadOutput,
    GetObjectOutput, HeadObjectOutput, ListMultipartUploadsOutput, ListMultipartUploadsRequest,
    ListObjectsV2Output, ListObjectsV2Request, ListPartsOutput, ListPartsRequest, PartBody,
    PutObjectOptions, PutObjectOutput, S3Api, S3Client, S3Error, S3TransferApi, UploadPartOutput,
};
use rrcloud_core::semhash::{Blake3Hex, ContentId};
use rrcloud_core::state::{ItemRecord, ItemState, SyncDb};

const DAY: i64 = 86_400;
const BLAKE3_HEX: &str = "4878ca0425c739fa427f7eda20fe845f6b2e46ba5fe2a14df5b1e32f50603215";

/// The real wall clock, unix seconds (Garage runs on this host, so this is
/// also the server time stamped on a freshly PUT object's `LastModified`).
fn real_now() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_secs(),
    )
    .expect("fits i64")
}

fn vv(pairs: &[(&DeviceId, u32)]) -> VersionVector {
    pairs.iter().map(|(d, c)| ((*d).clone(), *c)).collect()
}

fn device_entry(last_seen: i64, applied: &[(&DeviceId, u64)]) -> DeviceEntry {
    DeviceEntry {
        name: "dev".to_string(),
        platform: "linux".to_string(),
        created: 0,
        last_seen_server_ts: last_seen,
        applied: applied.iter().map(|(d, s)| ((*d).clone(), *s)).collect(),
        proto: ProtoSupport {
            read: PROTO_READ.to_vec(),
            write: PROTO_WRITE,
        },
    }
}

/// A **live** (not soft-deleted) synced original in the runner's state.
fn live_original(content: &ContentId, vv_: VersionVector) -> ItemRecord {
    ItemRecord {
        kind: Kind::Original,
        state: ItemState::Synced,
        size: 1024,
        mtime_unix_ns: 0,
        blake3: Some(Blake3Hex::parse(BLAKE3_HEX).unwrap()),
        sem_hash: None,
        vv: vv_,
        content_id: Some(content.clone()),
        w: Some(100),
        h: Some(100),
        pinned: false,
        last_access_unix: 0,
        verified_remote: true,
        attested: false,
        base_unknown: false,
        rating: None,
        color_label: None,
        device: None,
        head_ts: None,
        admitted_vv: None,
        admitted_ts: None,
        deleted: false,
    }
}

async fn put_raw(client: &S3Client, bucket: &str, key: &str, body: &[u8]) {
    client
        .put_object(
            bucket,
            key,
            Bytes::copy_from_slice(body),
            &PutObjectOptions::default(),
        )
        .await
        .expect("put object");
}

async fn exists(client: &S3Client, bucket: &str, key: &str) -> bool {
    client.head_object(bucket, key).await.is_ok()
}

// ---------------------------------------------------------------------------
// The racing writer: a delegating S3 wrapper that overwrites one tombstone
// between the runner's LIST and its GET
// ---------------------------------------------------------------------------

/// Pure delegation to the real client, except that the first `get_object`
/// of `target_key` first re-PUTs `swap_body` at that key (the hostile
/// overwrite landing between LIST and GET) and then returns whatever the
/// server now serves — the fresh body, with a fresh ETag.
struct SwappingS3 {
    inner: S3Client,
    target_key: String,
    swap_body: Vec<u8>,
    swaps: AtomicU32,
}

impl SwappingS3 {
    fn new(inner: S3Client, target_key: String, swap_body: Vec<u8>) -> Self {
        Self {
            inner,
            target_key,
            swap_body,
            swaps: AtomicU32::new(0),
        }
    }

    fn swaps(&self) -> u32 {
        self.swaps.load(Ordering::SeqCst)
    }
}

impl S3Api for SwappingS3 {
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
        if key == self.target_key && self.swaps.fetch_add(1, Ordering::SeqCst) == 0 {
            // The hostile peer's overwrite lands after the runner's LIST
            // (already taken) and before its GET (about to happen).
            self.inner
                .put_object(
                    bucket,
                    key,
                    Bytes::copy_from_slice(&self.swap_body),
                    &PutObjectOptions::default(),
                )
                .await?;
        }
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

impl S3TransferApi for SwappingS3 {
    async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), S3Error> {
        self.inner.delete_object(bucket, key).await
    }

    async fn create_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        opts: &PutObjectOptions,
    ) -> Result<CreateMultipartUploadOutput, S3Error> {
        self.inner.create_multipart_upload(bucket, key, opts).await
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
        self.inner
            .upload_part(bucket, key, upload_id, part_number, body, content_md5)
            .await
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

// ---------------------------------------------------------------------------
// Scenario
// ---------------------------------------------------------------------------

/// The shared scenario:
///
/// - runner `DEV_A` with a **live** item `b` (vv `{A:3}`) whose original,
///   sidecar, preview and thumb are all in the bucket and whose local record
///   is live with no deleted-set row — B has never been deleted by anyone;
/// - an unrelated item `a` whose data keys are in the bucket and which was
///   honestly deleted by peer `DEV_X` at `written_at`: its tombstone object
///   is PUT now (real time) with a body that agrees (`server_ts =
///   written_at`), so once the clock is pinned 31 days ahead it is genuinely
///   past grace by the server's own `LastModified`;
/// - one active peer `DEV_B` (heartbeat 60 s before the pinned `now`).
///
/// Returns the runner db (kept alive by the tempdir), the keys, and the
/// hostile body the racing writer will swap in for A's tombstone: a
/// tombstone naming **B**, with a vv dominating B's live vv (so GC does not
/// retain it as `Superseded`) and an old `server_ts` (so the body's claim
/// does not make it look younger than the listing).
struct Planted {
    _dir: tempfile::TempDir,
    db: SyncDb,
    runner: DeviceId,
    a: RelKey,
    b: RelKey,
    b_content: ContentId,
    hostile_body: Vec<u8>,
}

async fn plant(client: &S3Client, bucket: &str, tag: &str, now: i64, written_at: i64) -> Planted {
    let (dev_a, dev_b, dev_x) = (dev(DEV_A), dev(DEV_B), dev(DEV_X));
    let (dir, _path, db) = open_db(&dev_a);

    // Item B: live everywhere.
    let b = rel(&format!("img/{tag}-B-live.NEF"));
    let b_content = ContentId::from_bytes(format!("live-bytes-{tag}-B").as_bytes());
    let b_live_vv = vv(&[(&dev_a, 3)]);
    put_raw(client, bucket, &library_key(&b), b"B-original-bytes").await;
    put_raw(client, bucket, &sidecar_key(&b), b"{\"sidecar\":\"B\"}").await;
    put_raw(client, bucket, &preview_key(&b_content), b"B-preview").await;
    put_raw(
        client,
        bucket,
        &thumb_key(&b_content, ThumbSize::Small),
        b"B-thumb",
    )
    .await;
    db.replay_put_item(&b, &live_original(&b_content, b_live_vv.clone()))
        .expect("live item B");
    assert!(
        db.get_deleted(&b).expect("get_deleted").is_none(),
        "precondition: no deleted-set row for the live item B"
    );

    // Item A: honestly deleted long ago by X; its data keys still await GC.
    let a = rel(&format!("img/{tag}-A-deleted.NEF"));
    put_raw(client, bucket, &library_key(&a), b"A-original-bytes").await;
    put_raw(client, bucket, &sidecar_key(&a), b"{\"sidecar\":\"A\"}").await;
    let honest_tomb = Tombstone {
        relkey: a.clone(),
        vv: vv(&[(&dev_a, 1), (&dev_x, 1)]),
        device: dev_x.clone(),
        server_ts: written_at,
        kinds: vec![Kind::Original, Kind::Sidecar],
    };
    put_raw(
        client,
        bucket,
        &tombstone_key(&a),
        &serde_json::to_vec(&honest_tomb).unwrap(),
    )
    .await;
    assert!(
        !exists(client, bucket, &tombstone_key(&b)).await,
        "precondition: B has no tombstone object at all"
    );

    // One active peer.
    put_raw(
        client,
        bucket,
        &device_registry_key(&dev_b),
        &serde_json::to_vec(&device_entry(now - 60, &[(&dev_a, 3)])).unwrap(),
    )
    .await;

    // The hostile body the racing writer will plant at A's tombstone key:
    // it names B, dominates B's live vv the way a genuine delete would, and
    // claims the same old server_ts so the body cannot make the tombstone
    // look younger than the listing says.
    let hostile_tomb = Tombstone {
        relkey: b.clone(),
        vv: vv(&[(&dev_a, 3), (&dev_x, 1)]),
        device: dev_x,
        server_ts: written_at,
        kinds: vec![Kind::Original, Kind::Sidecar],
    };
    let hostile_body = serde_json::to_vec(&hostile_tomb).unwrap();

    Planted {
        _dir: dir,
        db,
        runner: dev_a,
        a,
        b,
        b_content,
        hostile_body,
    }
}

/// A tombstone overwritten between LIST and GET with a body naming a
/// different, live item must not destroy that item: the listed (aged)
/// entry and the fetched body are different objects, and GC may act on
/// neither until it knows they agree.
#[tokio::test]
async fn gc_does_not_destroy_live_item_named_by_tombstone_swapped_after_list() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("gc-swap");
    let client = g.client();

    let written_at = real_now();
    let now = written_at + 31 * DAY;
    const { assert!(31 * DAY > RECENTLY_DELETED_GRACE_SECS) };
    let p = plant(&client, &bucket, "swap", now, written_at).await;

    // The runner's S3 client, with the racing writer interposed on A's
    // tombstone key: LIST sees A's honest, aged entry; the GET that follows
    // returns the hostile body naming B.
    let swapping = SwappingS3::new(g.client(), tombstone_key(&p.a), p.hostile_body.clone());

    let clock = ServerClock::pinned(now);
    let summary = tombstone_gc(&p.db, &swapping, &bucket, &clock, &CompactConfig::default())
        .await
        .expect("gc");

    assert_eq!(
        swapping.swaps(),
        1,
        "precondition: the racing overwrite fired exactly once, on A's tombstone GET"
    );

    // B was never deleted by anyone and never had an aged tombstone: every
    // one of its keys must survive.
    assert!(
        exists(&client, &bucket, &library_key(&p.b)).await,
        "BUG: GC destroyed live item B's original on the say-so of a body \
         fetched from A's tombstone key after the listing was taken"
    );
    assert!(
        exists(&client, &bucket, &sidecar_key(&p.b)).await,
        "BUG: GC destroyed live item B's sidecar"
    );
    assert!(
        exists(&client, &bucket, &preview_key(&p.b_content)).await,
        "BUG: GC destroyed live item B's preview"
    );
    assert!(
        exists(&client, &bucket, &thumb_key(&p.b_content, ThumbSize::Small)).await,
        "BUG: GC destroyed live item B's thumb"
    );

    // Nothing about B reported destroyed, nothing about B folded into the
    // runner's deleted set (local table or manifest), and B still live.
    assert!(
        !summary.destroyed.iter().any(|d| d.relkey == p.b),
        "BUG: B reported destroyed: {:?}",
        summary.destroyed
    );
    assert!(
        p.db.get_deleted(&p.b).expect("get_deleted").is_none(),
        "BUG: B's deletion folded into the runner's local deleted set"
    );
    if let Ok(manifest) = get_manifest(&client, &bucket, &p.runner).await {
        assert!(
            !manifest.deleted.iter().any(|d| d.del == p.b),
            "BUG: B's deletion folded into the runner's manifest deleted set"
        );
    }
    let item = p.db.get_item(&p.b).expect("get_item").expect("item B");
    assert!(!item.deleted, "runner's record for B must remain live");
}

/// Positive control: the identical scenario with no racing writer. A's
/// tombstone object is genuinely 31 days old by the server's reckoning and
/// its body agrees, so it IS collected — A's data keys destroyed, A's
/// deletion folded — while B is untouched. A fix that simply never
/// collects fails here.
#[tokio::test]
async fn gc_collects_honest_aged_tombstone_without_swap() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("gc-swap-control");
    let client = g.client();

    let written_at = real_now();
    let now = written_at + 31 * DAY;
    let p = plant(&client, &bucket, "control", now, written_at).await;

    let clock = ServerClock::pinned(now);
    let summary = tombstone_gc(&p.db, &client, &bucket, &clock, &CompactConfig::default())
        .await
        .expect("gc");

    assert!(
        summary.destroyed.iter().any(|d| d.relkey == p.a),
        "A's 31-day-old tombstone is collected; retained={:?}",
        summary.retained
    );
    assert!(
        !exists(&client, &bucket, &library_key(&p.a)).await,
        "A's original destroyed"
    );
    assert!(
        !exists(&client, &bucket, &sidecar_key(&p.a)).await,
        "A's sidecar destroyed"
    );
    assert!(
        !exists(&client, &bucket, &tombstone_key(&p.a)).await,
        "A's tombstone object destroyed"
    );
    let manifest = get_manifest(&client, &bucket, &p.runner)
        .await
        .expect("runner manifest");
    assert!(
        manifest.deleted.iter().any(|d| d.del == p.a),
        "A's deletion folded into the runner's deleted set"
    );

    // And B, never deleted, is untouched.
    assert!(
        exists(&client, &bucket, &library_key(&p.b)).await,
        "B's original untouched"
    );
    assert!(
        exists(&client, &bucket, &sidecar_key(&p.b)).await,
        "B's sidecar untouched"
    );
    assert!(
        !summary.destroyed.iter().any(|d| d.relkey == p.b),
        "B not reported destroyed"
    );
    assert!(
        !manifest.deleted.iter().any(|d| d.del == p.b),
        "B not folded into the deleted set"
    );
}
