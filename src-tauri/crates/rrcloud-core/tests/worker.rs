//! Failing tests for the headless worker (`rrcloud_core::worker`,
//! ARCHITECTURE.md §6, with §2.1/§2.3/§2.10 as the surrounding contract).
//! Garage-backed, with the foreign-ingest E2E additionally corpus-gated.
//!
//! The suite drives `worker::run_cycle` **directly** (the bin's `main` is a
//! thin shell), so every §6 duty is pinned as a decision, not a smoke
//! signal:
//!
//! - **DURABLE-STATE-MANDATORY** — a worker opened without a persistent
//!   state dir refuses to journal (typed hard error, before any journal
//!   write); with one it journals under a stable device id whose seq never
//!   regresses across two `run_cycle` invocations in separate instances.
//! - **FOREIGN-INGEST E2E** — a real corpus RAW dropped into `library/`
//!   with no journal entry is adopted (attest + put), a `.pxy.dng` preview
//!   and `_small`/`_medium` thumbs are generated and PUT under `content_id`
//!   keys, and a second "phone" (`EngineConsumer`) converges to a cloud
//!   placeholder for the original and can seed the thumb — camera → bucket
//!   → phone with no bucket notifications.
//! - **IDEMPOTENCE** — a second cycle with nothing new adopts nothing,
//!   makes no duplicate proxies, and publishes no new journal entries.
//! - **GC / COMPACTION through the worker** — a tombstone past grace is
//!   destroyed (its data keys + the tombstone object) and folded into the
//!   worker's deleted set; covered, aged own segments are compacted away.
//! - **STATELESS REFUSAL** — the journaling role without a state dir is a
//!   typed hard error reached before any S3/journal contact.
//!
//! Server-time thresholds are exercised by planting history relative to a
//! fixed `NOW` and passing `CycleOptions { clock: Some(ServerClock::pinned
//! (..)), .. }`, never by sleeping (the `compact.rs` pattern).

mod common;

use std::path::PathBuf;

use bytes::Bytes;
use common::engine as eh;
use common::garage;
use common::sync::{dev, open_db, rel, DEV_A};
use common::transfer as th;
use common::transfer::CountingS3;

use rrcloud_core::clock::{DeviceId, VersionVector};
use rrcloud_core::compact::{CompactConfig, ServerClock};
use rrcloud_core::engine::item_local_path;
use rrcloud_core::journal::{Kind, Op, Tombstone};
use rrcloud_core::keys::{
    journal_segment_key, library_key, preview_key, sidecar_key, thumb_key, tombstone_key, RelKey,
    ThumbSize,
};
use rrcloud_core::manifest::get_manifest;
use rrcloud_core::publisher::{enqueue_entry, publish_pending};
use rrcloud_core::s3::{PutObjectOptions, S3Client};
use rrcloud_core::semhash::{Blake3Hex, ContentId};
use rrcloud_core::state::{DeletedRecord, ItemRecord, ItemState, SyncDb};
use rrcloud_core::worker::{self, CycleOptions, Worker, WorkerConfig, WorkerError};

// A fixed "server now" so every planted history is deterministic relative
// to it (same convention as `tests/compact.rs`).
const NOW: i64 = 1_769_904_000;

const DAY: i64 = 86_400;

// ---------------------------------------------------------------------------
// Config / small helpers
// ---------------------------------------------------------------------------

/// A [`WorkerConfig`] pointed at the shared Garage instance and `bucket`,
/// with `state_dir` as given (`None` is the stateless case the worker
/// refuses to journal in). Built directly rather than through `from_env`
/// so parallel tests never race on the process environment.
fn worker_cfg(g: &garage::Garage, bucket: &str, state_dir: Option<PathBuf>) -> WorkerConfig {
    let s3 = g.s3_config();
    WorkerConfig {
        state_dir,
        endpoint: s3.endpoint,
        bucket: bucket.to_string(),
        region: s3.region,
        access_key_id: s3.access_key_id,
        secret_access_key: s3.secret_access_key,
    }
}

fn vv(pairs: &[(&DeviceId, u32)]) -> VersionVector {
    pairs.iter().map(|(d, c)| ((*d).clone(), *c)).collect()
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
        .unwrap_or_else(|e| panic!("put {key}: {e}"));
}

async fn exists(client: &S3Client, bucket: &str, key: &str) -> bool {
    client.head_object(bucket, key).await.is_ok()
}

/// A soft-deleted original item record carrying `content`/`blake3` — the
/// runner's local view of an item whose tombstone the worker will GC.
fn deleted_original_record(content: &ContentId, vv_: VersionVector) -> ItemRecord {
    ItemRecord {
        kind: Kind::Original,
        state: ItemState::Synced,
        size: 1024,
        mtime_unix_ns: 0,
        blake3: Some(Blake3Hex::from_bytes(b"gc-original")),
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
        deleted: true,
    }
}

/// Publishes `n` single-entry own sidecar segments under `db`'s prefix
/// (seqs 1..=n), stamping each with a planted publish instant so the §2.10
/// 14-day cap is exercised deterministically. Returns the first-seqs.
async fn publish_own_segments(
    db: &SyncDb,
    client: &S3Client,
    bucket: &str,
    n: usize,
    seg_server_ts: i64,
) -> Vec<u64> {
    let mut segs = Vec::new();
    for i in 0..n {
        let e = common::sync::entry(
            db.device_id(),
            Op::Put,
            Kind::Sidecar,
            sidecar_key(&rel(&format!("img/i{i:04}.NEF"))),
        );
        enqueue_entry(db, &e).expect("enqueue");
        let report = publish_pending(db, client, bucket).await.expect("publish");
        for first in report.segments {
            db.set_segment_published_server_ts(first, seg_server_ts)
                .expect("stamp seg ts");
            segs.push(first);
        }
    }
    segs
}

// ---------------------------------------------------------------------------
// Corpus gating (mirrors tests/proxy.rs)
// ---------------------------------------------------------------------------

/// The corpus directory, or `None` → skip. `RRCLOUD_RAW_CORPUS` overrides
/// the default `/tmp/claude-0/raws`.
fn corpus_dir() -> Option<PathBuf> {
    let p = std::env::var("RRCLOUD_RAW_CORPUS")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "/tmp/claude-0/raws".to_string());
    let pb = PathBuf::from(p);
    pb.is_dir().then_some(pb)
}

/// The smallest decodable RAW in the corpus (fastest proxy generation), as
/// `(filename, bytes)`. Restricts to the extensions the golden corpus uses.
fn smallest_raw(dir: &std::path::Path) -> Option<(String, Vec<u8>)> {
    let exts = ["dng", "cr3", "nef", "arw", "raf"];
    let mut best: Option<(u64, PathBuf)> = None;
    for entry in std::fs::read_dir(dir).ok()? {
        let path = entry.ok()?.path();
        let is_raw = path
            .extension()
            .and_then(|e| e.to_str())
            .map(|e| exts.contains(&e.to_ascii_lowercase().as_str()))
            .unwrap_or(false);
        if !is_raw {
            continue;
        }
        let len = std::fs::metadata(&path).ok()?.len();
        if best.as_ref().map(|(b, _)| len < *b).unwrap_or(true) {
            best = Some((len, path));
        }
    }
    let (_, path) = best?;
    let name = path.file_name()?.to_str()?.to_string();
    Some((name, std::fs::read(&path).ok()?))
}

/// The relkey a corpus file is dropped under in `library/ingest/`,
/// preserving its real extension (so `classify_key` sees an Original).
fn ingest_relkey(filename: &str) -> RelKey {
    let ext = std::path::Path::new(filename)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("dng");
    rel(&format!("ingest/photo.{ext}"))
}

macro_rules! skip_without_corpus {
    ($name:literal) => {
        match corpus_dir().and_then(|d| smallest_raw(&d)) {
            Some(raw) => raw,
            None => {
                eprintln!(
                    "SKIP {}: no RAW corpus (set RRCLOUD_RAW_CORPUS or drop files in \
                     /tmp/claude-0/raws)",
                    $name
                );
                return;
            }
        }
    };
}

// ===========================================================================
// STATELESS REFUSAL / DURABLE-STATE-MANDATORY
// ===========================================================================

/// The journaling role without a state dir is a typed hard error, returned
/// before any S3 or journal contact — proven by a bogus endpoint the
/// refusal never reaches.
#[test]
fn stateless_refusal_is_typed_before_touching_s3() {
    let cfg = WorkerConfig {
        state_dir: None,
        endpoint: "http://127.0.0.1:1".to_string(),
        bucket: "nonexistent".to_string(),
        region: "garage".to_string(),
        access_key_id: "k".to_string(),
        secret_access_key: "s".to_string(),
    };
    let res = Worker::open(&cfg);
    assert!(
        matches!(res, Err(WorkerError::StatelessRefusal)),
        "a stateless worker must refuse the journaling role with a typed hard error \
         before any S3/journal contact"
    );
}

/// With no state dir the worker refuses to open for journaling, and nothing
/// is written to the journal prefix.
#[tokio::test]
async fn worker_without_state_dir_refuses_and_writes_no_journal() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("wk-nostate");
    let client = g.client();

    let cfg = worker_cfg(g, &bucket, None);
    let res = Worker::open(&cfg);
    assert!(
        matches!(res, Err(WorkerError::StatelessRefusal)),
        "DURABLE-STATE-MANDATORY: no persistent state dir must refuse to journal"
    );

    let journal = eh::keys_under(&client, &bucket, ".rrcloud/v1/journal/").await;
    assert!(
        journal.is_empty(),
        "the refusal must precede any journal write, found: {journal:?}"
    );
}

/// With a persistent state dir the worker journals under one stable device
/// identity whose seq never regresses across two `run_cycle` invocations in
/// separate instances (two SyncDb opens over the same state dir).
#[tokio::test]
async fn durable_state_stable_identity_and_monotonic_seq_across_runs() {
    let (filename, raw) = skip_without_corpus!("durable_state_stable_identity");
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("wk-durable");
    let client = g.client();
    let state = tempfile::tempdir().expect("state dir");
    let cfg = worker_cfg(g, &bucket, Some(state.path().to_path_buf()));

    let item = ingest_relkey(&filename);
    put_raw(&client, &bucket, &library_key(&item), &raw).await;

    // Instance 1: mints the identity, adopts the foreign original, journals.
    let (dev1, seq1) = {
        let worker = Worker::open(&cfg).expect("open instance 1");
        assert!(
            worker.db().minted_identity(),
            "first open mints the identity"
        );
        let report = worker::run_cycle(&worker, &CycleOptions::default())
            .await
            .expect("cycle 1");
        assert!(
            !report.adopted.is_empty(),
            "the foreign original is adopted"
        );
        let seq = worker.db().last_allocated_seq().expect("seq");
        assert!(seq > 0, "instance 1 journaled under a real seq");
        (worker.device_id().clone(), seq)
    }; // drop releases the redb lock

    // Instance 2: reopens the SAME state dir — same identity, no re-mint,
    // seq never regresses.
    {
        let worker = Worker::open(&cfg).expect("open instance 2");
        assert!(
            !worker.db().minted_identity(),
            "reopen must not re-mint (no phantom device)"
        );
        assert_eq!(
            worker.device_id(),
            &dev1,
            "one stable worker identity across runs"
        );
        worker::run_cycle(&worker, &CycleOptions::default())
            .await
            .expect("cycle 2");
        let seq2 = worker.db().last_allocated_seq().expect("seq");
        assert!(
            seq2 >= seq1,
            "monotonic seq across runs: {seq2} must not regress below {seq1}"
        );
    }
}

// ===========================================================================
// FOREIGN-INGEST E2E (the headline)
// ===========================================================================

/// A real corpus RAW dropped into `library/` with no journal entry
/// (SFTPGo/rclone/camera/desktop) is adopted whole: the original is
/// attested + journaled with a `content_id`, a `.pxy.dng` preview and
/// `_small`/`_medium` thumbs are generated and PUT under `content_id` keys,
/// and journal `put` entries for the original + preview + thumb are
/// published. THEN a distinct "phone" (`EngineConsumer`, its own device id +
/// temp root + the SAME bucket) polls and converges to a cloud placeholder
/// for the original and can seed the `_small` thumb — proving camera →
/// bucket → phone with no bucket notifications.
#[tokio::test]
async fn foreign_ingest_e2e_camera_to_bucket_to_phone() {
    let (filename, raw) = skip_without_corpus!("foreign_ingest_e2e");
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("wk-ingest");
    let client = g.client();
    let state = tempfile::tempdir().expect("state dir");
    let cfg = worker_cfg(g, &bucket, Some(state.path().to_path_buf()));
    let worker = Worker::open(&cfg).expect("open worker");

    // External drop: the original lands at library/<path> with NO journal.
    let item = ingest_relkey(&filename);
    put_raw(&client, &bucket, &library_key(&item), &raw).await;

    let report = worker::run_cycle(&worker, &CycleOptions::default())
        .await
        .expect("run_cycle");

    // (1) Adoption + generation reported.
    assert_eq!(
        report.adopted,
        vec![item.clone()],
        "exactly the dropped original adopted"
    );
    assert!(
        report.proxies_generated >= 1,
        "a smart preview was generated"
    );
    assert!(report.previews_put >= 1, "the preview object was PUT");
    assert!(
        report.thumbs_put >= 2,
        "both _small and _medium thumbs were PUT"
    );

    // (2) The worker's journal carries the attest + put original/preview/thumb.
    let w_entries = eh::journal_entries_of(&client, &bucket, worker.device_id()).await;
    assert!(
        w_entries.iter().any(|e| e.op == Op::Attest),
        "the adopted original is attested (§3.5 eviction gate / §6)"
    );
    let orig_put = w_entries
        .iter()
        .find(|e| e.op == Op::Put && e.kind == Kind::Original)
        .expect("a put-original entry");
    assert_eq!(
        orig_put.key,
        library_key(&item),
        "original put keyed at its library key"
    );
    let content_id = orig_put
        .content_id
        .clone()
        .expect("the put-original carries a content_id");
    assert!(
        orig_put.blake3.is_some(),
        "put-original carries the blake3 of the GET bytes"
    );
    assert!(
        w_entries
            .iter()
            .any(|e| e.op == Op::Put && e.kind == Kind::Preview),
        "a put-preview entry"
    );
    assert!(
        w_entries
            .iter()
            .any(|e| e.op == Op::Put && e.kind == Kind::Thumb),
        "a put-thumb entry"
    );

    // (3) The objects exist in the bucket under content_id keys; the
    //     original is left in place.
    assert!(
        exists(&client, &bucket, &library_key(&item)).await,
        "original left in place"
    );
    assert!(
        exists(&client, &bucket, &preview_key(&content_id)).await,
        "preview object present"
    );
    for size in [ThumbSize::Small, ThumbSize::Medium] {
        assert!(
            exists(&client, &bucket, &thumb_key(&content_id, size)).await,
            "thumb object present ({size:?})"
        );
    }

    // (4) The phone converges: its own device id, temp root, same bucket.
    let phone_client = g.client();
    let (_pdbdir, _ppath, phone_db) = open_db(&dev(DEV_A));
    let phone_root = tempfile::tempdir().expect("phone root");
    let phone_s3 = CountingS3::new(g.client());
    let phone_cfg = th::test_cfg(&bucket, phone_root.path());
    let mut phone_events = eh::RecordedEvents::default();
    eh::poll_apply(&phone_db, &phone_s3, &phone_cfg, &mut phone_events).await;

    // The phone learned the original as a cloud placeholder (no local bytes),
    // carrying the worker's content_id / blake3 / dims — without downloading
    // the full original.
    let phone_rec = phone_db
        .get_item(&item)
        .expect("get_item")
        .expect("phone learned the original");
    assert_eq!(
        phone_rec.content_id.as_ref(),
        Some(&content_id),
        "phone converged on the worker's content_id"
    );
    assert_eq!(
        phone_rec.blake3, orig_put.blake3,
        "phone learned the original's blake3"
    );
    assert!(
        matches!(phone_rec.state, ItemState::PendingDown | ItemState::Stub),
        "phone holds a cloud placeholder, not a downloaded original (state {:?})",
        phone_rec.state
    );
    let orig_path = item_local_path(phone_root.path(), &item);
    assert!(
        !orig_path.exists(),
        "the full original was NOT downloaded onto the phone"
    );

    // Materialize the 0-byte stub (the phone's §3.5 placeholder policy) and
    // confirm it reads as a cloud placeholder.
    th::write_file(&orig_path, b"");
    let mut stub = phone_rec.clone();
    stub.state = ItemState::Stub;
    phone_db
        .replay_put_item(&item, &stub)
        .expect("materialize stub");
    assert_eq!(
        std::fs::metadata(&orig_path).expect("stub").len(),
        0,
        "the stub is a 0-byte placeholder"
    );
    assert_eq!(
        phone_db.get_item(&item).unwrap().unwrap().state,
        ItemState::Stub,
        "is_cloud_placeholder: the original is a stub on the phone"
    );

    // The phone can seed the thumb: fetch the _small thumb keyed by the
    // learned content_id and confirm it is a JPEG.
    let small = th::get_bytes(
        &phone_client,
        &bucket,
        &thumb_key(&content_id, ThumbSize::Small),
    )
    .await;
    assert!(
        small.len() > 2 && small[0] == 0xFF && small[1] == 0xD8,
        "the seeded _small thumb is a JPEG the phone can fetch"
    );
}

// ===========================================================================
// IDEMPOTENCE
// ===========================================================================

/// A second cycle with nothing new adopts nothing, makes no duplicate
/// proxies, and publishes no new journal entries — a near-noop.
#[tokio::test]
async fn second_cycle_is_an_idempotent_near_noop() {
    let (filename, raw) = skip_without_corpus!("second_cycle_idempotent");
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("wk-idem");
    let client = g.client();
    let state = tempfile::tempdir().expect("state dir");
    let cfg = worker_cfg(g, &bucket, Some(state.path().to_path_buf()));
    let worker = Worker::open(&cfg).expect("open worker");

    let item = ingest_relkey(&filename);
    put_raw(&client, &bucket, &library_key(&item), &raw).await;

    let first = worker::run_cycle(&worker, &CycleOptions::default())
        .await
        .expect("cycle 1");
    assert!(
        !first.adopted.is_empty(),
        "cycle 1 adopts the dropped original"
    );
    let n1 = eh::journal_entries_of(&client, &bucket, worker.device_id())
        .await
        .len();

    let second = worker::run_cycle(&worker, &CycleOptions::default())
        .await
        .expect("cycle 2");
    assert!(second.adopted.is_empty(), "cycle 2 adopts nothing new");
    assert_eq!(second.proxies_generated, 0, "no duplicate proxy generation");
    assert_eq!(second.previews_put, 0, "no duplicate preview PUT");
    assert_eq!(second.thumbs_put, 0, "no duplicate thumb PUT");
    assert_eq!(
        second.journal_entries_published, 0,
        "cycle 2 publishes no new journal entries"
    );

    let n2 = eh::journal_entries_of(&client, &bucket, worker.device_id())
        .await
        .len();
    assert_eq!(
        n1, n2,
        "no new journal entries landed on the idempotent re-run"
    );
}

// ===========================================================================
// GC / COMPACTION through the worker
// ===========================================================================

/// A tombstone past the 30-day grace (hence past the 14-day cap) is
/// destroyed through the worker: its data keys + the tombstone object are
/// deleted and the deletion is folded into the worker's retained deleted
/// set (so bootstrappers still learn it, §2.3/A3).
#[tokio::test]
async fn worker_cycle_gc_destroys_tombstone_past_grace_and_folds_deleted_set() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("wk-gc");
    let client = g.client();
    let state = tempfile::tempdir().expect("state dir");
    let cfg = worker_cfg(g, &bucket, Some(state.path().to_path_buf()));
    let worker = Worker::open(&cfg).expect("open worker");

    let image = rel("gc/old.NEF");
    let content = ContentId::from_bytes(b"gc-old-image-bytes");
    let del_vv = vv(&[(worker.device_id(), 5)]);
    let server_ts = NOW - 31 * DAY; // past grace (and the cap)

    put_raw(&client, &bucket, &library_key(&image), b"orig").await;
    put_raw(&client, &bucket, &sidecar_key(&image), b"{}").await;
    put_raw(&client, &bucket, &preview_key(&content), b"preview").await;
    put_raw(
        &client,
        &bucket,
        &thumb_key(&content, ThumbSize::Small),
        b"thumb",
    )
    .await;
    let tomb = Tombstone {
        relkey: image.clone(),
        vv: del_vv.clone(),
        device: worker.device_id().clone(),
        server_ts,
        kinds: vec![Kind::Original, Kind::Sidecar],
    };
    put_raw(
        &client,
        &bucket,
        &tombstone_key(&image),
        &serde_json::to_vec(&tomb).unwrap(),
    )
    .await;

    // The worker's local view: a soft-deleted item + its deleted-set row.
    worker
        .db()
        .replay_put_item(&image, &deleted_original_record(&content, del_vv.clone()))
        .expect("deleted item");
    worker
        .db()
        .record_deleted(
            &image,
            &DeletedRecord {
                vv: del_vv,
                server_ts,
            },
        )
        .expect("deleted row");

    let opts = CycleOptions {
        clock: Some(ServerClock::pinned(NOW)),
        ..Default::default()
    };
    let report = worker::run_cycle(&worker, &opts).await.expect("cycle");

    assert!(
        report.gc.destroyed.iter().any(|d| d.relkey == image),
        "the tombstone was GC'd through the worker"
    );
    assert!(
        !exists(&client, &bucket, &library_key(&image)).await,
        "original destroyed"
    );
    assert!(
        !exists(&client, &bucket, &sidecar_key(&image)).await,
        "sidecar destroyed"
    );
    assert!(
        !exists(&client, &bucket, &preview_key(&content)).await,
        "preview destroyed"
    );
    assert!(
        !exists(&client, &bucket, &thumb_key(&content, ThumbSize::Small)).await,
        "thumb destroyed"
    );
    assert!(
        !exists(&client, &bucket, &tombstone_key(&image)).await,
        "tombstone object destroyed"
    );

    let manifest = get_manifest(&client, &bucket, worker.device_id())
        .await
        .expect("worker manifest");
    assert!(
        manifest.deleted.iter().any(|d| d.del == image),
        "the deletion is retained in the worker's deleted set for bootstrappers"
    );
}

/// Covered, aged own segments are compacted away through the worker: the
/// first cycle advances manifest coverage (nothing deleted — reconfirm
/// window open), the second cycle 25 h later deletes them (manifest aged
/// past 24 h, segments past the 14-day cap).
#[tokio::test]
async fn worker_cycle_compacts_covered_aged_own_segments() {
    let Some(g) = garage::shared() else { return };
    let bucket = g.create_unique_bucket("wk-compact");
    let client = g.client();
    let state = tempfile::tempdir().expect("state dir");
    let cfg = worker_cfg(g, &bucket, Some(state.path().to_path_buf()));
    let worker = Worker::open(&cfg).expect("open worker");

    // Three own segments, stamped old enough for the 14-day cap.
    let segs = publish_own_segments(worker.db(), &client, &bucket, 3, NOW - 20 * DAY).await;
    assert_eq!(segs.len(), 3, "three own segments published");
    let seg_keys: Vec<String> = segs
        .iter()
        .map(|s| journal_segment_key(worker.device_id(), *s))
        .collect();

    // Cycle 1 at NOW: builds/advances coverage; deletes nothing (age 0).
    let pass1 = worker::run_cycle(
        &worker,
        &CycleOptions {
            clock: Some(ServerClock::pinned(NOW)),
            ..Default::default()
        },
    )
    .await
    .expect("cycle 1");
    assert!(
        pass1.compaction.deleted_seqs.is_empty(),
        "reconfirm window still open on the first pass"
    );
    for key in &seg_keys {
        assert!(
            exists(&client, &bucket, key).await,
            "segment still present after pass 1"
        );
    }

    // Cycle 2 at NOW + 25 h: manifest aged past reconfirm, cap holds → delete.
    let pass2 = worker::run_cycle(
        &worker,
        &CycleOptions {
            clock: Some(ServerClock::pinned(NOW + 25 * 3600)),
            ..Default::default()
        },
    )
    .await
    .expect("cycle 2");
    assert!(
        !pass2.compaction.deleted_seqs.is_empty(),
        "covered, aged own segments are compacted on the second pass"
    );
    for key in &seg_keys {
        assert!(
            !exists(&client, &bucket, key).await,
            "segment object compacted away"
        );
    }
}

// A `CompactConfig` field is referenced so the import is load-bearing even
// while every §2.10 default is exercised through `CycleOptions::default`.
const _: fn() -> CompactConfig = CompactConfig::default;
