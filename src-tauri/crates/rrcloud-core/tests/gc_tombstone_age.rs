//! Tombstone-GC age must be **server-asserted**, not peer-asserted.
//!
//! `tombstone_gc` (architecture §2.10) gates destruction on the tombstone's
//! age: (b) the 30-day Recently-Deleted grace window and (a) the 14-day
//! laggard cap. Both are computed as `now - tomb.server_ts`, where
//! `server_ts` is a field *inside the tombstone JSON* — written by whichever
//! device created the tombstone. §2.7/§2.10 describe the grace window as the
//! safety net against a compromised or buggy device, but a peer holding the
//! bucket credentials can write a tombstone for any live item with
//! `server_ts: 0` (or `now - 400 days`) and the very next GC cycle destroys
//! the item's data keys and folds the deletion into the runner's deleted set,
//! so every device hides it. The bucket listing's `LastModified` for the
//! tombstone object is asserted by the server and cannot be backdated by the
//! writer, so a tombstone written a second ago can never be older than a
//! second, whatever its body claims.
//!
//! These tests plant a *live* item (bucket data keys + a live local item
//! record) and a freshly-written tombstone whose body claims an ancient
//! `server_ts`, pin the server clock to **real now** (so the tombstone
//! object's real `LastModified` is "now" by the server's own reckoning) and
//! run one GC pass. The item must survive, retained within grace, and
//! nothing may be folded into the deleted set.
//!
//! The positive control pins the clock 31 days *after* the real write, so
//! the object is genuinely older than grace by the server's reckoning too,
//! and asserts it IS collected — the fix cannot be "never GC".

mod common;

use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use common::garage;
use common::sync::{dev, open_db, rel, DEV_A, DEV_B, DEV_X};

use rrcloud_core::clock::{DeviceId, VersionVector};
use rrcloud_core::compact::{
    tombstone_gc, CompactConfig, GcSkipReason, ServerClock, LAGGARD_CAP_SECS,
    RECENTLY_DELETED_GRACE_SECS,
};
use rrcloud_core::journal::{Kind, Tombstone};
use rrcloud_core::keys::{
    device_registry_key, library_key, preview_key, sidecar_key, thumb_key, tombstone_key, RelKey,
    ThumbSize,
};
use rrcloud_core::manifest::get_manifest;
use rrcloud_core::publisher::{DeviceEntry, ProtoSupport, PROTO_READ, PROTO_WRITE};
use rrcloud_core::s3::{PutObjectOptions, S3Client};
use rrcloud_core::semhash::{Blake3Hex, ContentId};
use rrcloud_core::state::{ItemRecord, ItemState, SyncDb};

const DAY: i64 = 86_400;
const BLAKE3_HEX: &str = "4878ca0425c739fa427f7eda20fe845f6b2e46ba5fe2a14df5b1e32f50603215";

/// The real wall clock, unix seconds. Garage runs on this host, so this is
/// also (to within a second or two) the server time the bucket stamps on a
/// freshly PUT object's `LastModified`.
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

/// The scenario every test here shares:
///
/// - runner `DEV_A` with a live item `image` (vv `{A:3}`) whose original,
///   sidecar, preview and thumb are all present in the bucket;
/// - one active peer `DEV_B` (heartbeat 60 s before `now`), so the active
///   set is non-empty and realistic;
/// - a tombstone for `image` written **right now** by peer `DEV_X`, whose
///   vv `{A:3, X:1}` dominates the live vv exactly as a genuine delete
///   would (so it is not retained as `Superseded`), and whose body claims
///   `server_ts = claimed_server_ts`.
///
/// Returns the runner db (kept alive by the tempdir) and the planted keys.
struct Planted {
    _dir: tempfile::TempDir,
    db: SyncDb,
    runner: DeviceId,
    image: RelKey,
    content: ContentId,
}

async fn plant_live_item_with_tombstone(
    client: &S3Client,
    bucket: &str,
    tag: &str,
    now: i64,
    claimed_server_ts: i64,
) -> Planted {
    let (a, b, x) = (dev(DEV_A), dev(DEV_B), dev(DEV_X));
    let (dir, _path, db) = open_db(&a);

    let image = rel(&format!("img/{tag}.NEF"));
    let content = ContentId::from_bytes(format!("live-bytes-{tag}").as_bytes());
    let live_vv = vv(&[(&a, 3)]);

    // Live data in the bucket.
    put_raw(client, bucket, &library_key(&image), b"original-bytes").await;
    put_raw(client, bucket, &sidecar_key(&image), b"{\"sidecar\":true}").await;
    put_raw(client, bucket, &preview_key(&content), b"preview").await;
    put_raw(
        client,
        bucket,
        &thumb_key(&content, ThumbSize::Small),
        b"thumb",
    )
    .await;
    // Live in the runner's converged state: NOT deleted, no deleted-set row.
    db.replay_put_item(&image, &live_original(&content, live_vv.clone()))
        .expect("live item");
    assert!(
        db.get_deleted(&image).expect("get_deleted").is_none(),
        "precondition: no deleted-set row for the live item"
    );

    // One active peer.
    put_raw(
        client,
        bucket,
        &device_registry_key(&b),
        &serde_json::to_vec(&device_entry(now - 60, &[(&a, 3)])).unwrap(),
    )
    .await;

    // The tombstone: written NOW (its LastModified is the server's "now"),
    // body claiming `claimed_server_ts`.
    let tomb = Tombstone {
        relkey: image.clone(),
        vv: vv(&[(&a, 3), (&x, 1)]),
        device: x,
        server_ts: claimed_server_ts,
        kinds: vec![Kind::Original, Kind::Sidecar],
    };
    put_raw(
        client,
        bucket,
        &tombstone_key(&image),
        &serde_json::to_vec(&tomb).unwrap(),
    )
    .await;

    Planted {
        _dir: dir,
        db,
        runner: a,
        image,
        content,
    }
}

/// Runs one GC pass at `now` against a tombstone written *now* whose body
/// claims `claimed_server_ts`, and asserts the live item survived untouched.
async fn assert_backdated_tombstone_is_not_collected(tag: &str, claimed_server_ts: i64) {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket(&format!("gc-age-{tag}"));
    let client = g.client();

    let now = real_now();
    let p = plant_live_item_with_tombstone(&client, &bucket, tag, now, claimed_server_ts).await;

    // Sanity: by the tombstone's *claimed* server_ts it is far past both
    // gates — that is exactly what the attacker relies on. Grace is the
    // longer window, so exceeding it implies exceeding the cap too.
    let claimed_age = now - claimed_server_ts;
    const { assert!(RECENTLY_DELETED_GRACE_SECS >= LAGGARD_CAP_SECS) };
    assert!(
        claimed_age > RECENTLY_DELETED_GRACE_SECS,
        "precondition: the claimed age ({claimed_age}s) exceeds grace (and so the cap)"
    );

    // Server clock pinned to real now: the tombstone object was PUT a moment
    // ago, so by the server's own `LastModified` it is seconds old.
    let clock = ServerClock::pinned(now);
    let summary = tombstone_gc(&p.db, &client, &bucket, &clock, &CompactConfig::default())
        .await
        .expect("gc");

    // The data keys must still exist — the whole point of the grace window.
    assert!(
        exists(&client, &bucket, &library_key(&p.image)).await,
        "[{tag}] BUG: GC destroyed the live original on the say-so of a \
         tombstone written seconds ago whose body claims server_ts={claimed_server_ts}"
    );
    assert!(
        exists(&client, &bucket, &sidecar_key(&p.image)).await,
        "[{tag}] BUG: GC destroyed the live sidecar"
    );
    assert!(
        exists(&client, &bucket, &preview_key(&p.content)).await,
        "[{tag}] BUG: GC destroyed the preview"
    );
    assert!(
        exists(&client, &bucket, &thumb_key(&p.content, ThumbSize::Small)).await,
        "[{tag}] BUG: GC destroyed the thumb"
    );
    assert!(
        exists(&client, &bucket, &tombstone_key(&p.image)).await,
        "[{tag}] a within-grace tombstone object is kept"
    );

    // Reported as retained within grace, nothing destroyed.
    assert!(
        summary.destroyed.is_empty(),
        "[{tag}] nothing may be destroyed: {:?}",
        summary.destroyed
    );
    let retained = summary
        .retained
        .iter()
        .find(|r| r.relkey == p.image)
        .unwrap_or_else(|| panic!("[{tag}] tombstone must be reported retained"));
    assert!(
        matches!(retained.reason, GcSkipReason::WithinGrace { age_secs } if age_secs < RECENTLY_DELETED_GRACE_SECS),
        "[{tag}] retained reason must be WithinGrace with a server-asserted age, got {:?}",
        retained.reason
    );

    // Nothing folded into the deleted set: neither the runner's local
    // deleted table nor its manifest (which GC only PUTs when it folds).
    assert!(
        p.db.get_deleted(&p.image).expect("get_deleted").is_none(),
        "[{tag}] BUG: deletion folded into the runner's local deleted set"
    );
    if let Ok(manifest) = get_manifest(&client, &bucket, &p.runner).await {
        assert!(
            !manifest.deleted.iter().any(|d| d.del == p.image),
            "[{tag}] BUG: deletion folded into the runner's manifest deleted set"
        );
    }
    // And the runner still sees the item as live.
    let item = p.db.get_item(&p.image).expect("get_item").expect("item");
    assert!(
        !item.deleted,
        "[{tag}] runner's item record must remain live"
    );
}

/// A tombstone written *now* whose body claims `server_ts: 0` (the epoch)
/// must not be collected: its server-asserted age is seconds, not 56 years.
#[tokio::test]
async fn gc_ignores_tombstone_server_ts_zero_written_just_now() {
    assert_backdated_tombstone_is_not_collected("ts-zero", 0).await;
}

/// Same with a plausible-looking but backdated `server_ts` (400 days ago):
/// the bug is not an edge case around zero.
#[tokio::test]
async fn gc_ignores_tombstone_server_ts_backdated_400_days() {
    let claimed = real_now() - 400 * DAY;
    assert_backdated_tombstone_is_not_collected("ts-400d", claimed).await;
}

/// Positive control: when the tombstone **object** is genuinely older than
/// grace by the server's reckoning (clock pinned 31 days after the real
/// write) and its body agrees, the same scenario IS collected — data keys
/// destroyed, deletion folded. A fix that simply never collects fails here.
#[tokio::test]
async fn gc_collects_tombstone_whose_object_is_genuinely_older_than_grace() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("gc-age-control");
    let client = g.client();

    let written_at = real_now();
    // The device heartbeat must be fresh relative to the *pinned* now, so
    // plant it against that instant; the tombstone is honest about when it
    // was written.
    let now = written_at + 31 * DAY;
    let p = plant_live_item_with_tombstone(&client, &bucket, "control", now, written_at).await;
    const { assert!(31 * DAY > RECENTLY_DELETED_GRACE_SECS) };

    let clock = ServerClock::pinned(now);
    let summary = tombstone_gc(&p.db, &client, &bucket, &clock, &CompactConfig::default())
        .await
        .expect("gc");

    assert!(
        summary.destroyed.iter().any(|d| d.relkey == p.image),
        "a tombstone whose object is 31 days old is collected; retained={:?}",
        summary.retained
    );
    assert!(
        !exists(&client, &bucket, &library_key(&p.image)).await,
        "original destroyed"
    );
    assert!(
        !exists(&client, &bucket, &sidecar_key(&p.image)).await,
        "sidecar destroyed"
    );
    assert!(
        !exists(&client, &bucket, &tombstone_key(&p.image)).await,
        "tombstone object destroyed"
    );
    let manifest = get_manifest(&client, &bucket, &p.runner)
        .await
        .expect("runner manifest");
    assert!(
        manifest.deleted.iter().any(|d| d.del == p.image),
        "deletion folded into the runner's deleted set"
    );
}
