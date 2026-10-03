//! Garage-backed two-device tests for §2.9 albums & presets meta sync.
//!
//! These prove the real app path end to end: a local `albums.json` /
//! `presets.json` saved on device A is relativized (`rr://`), uploaded as a
//! `.rrcloud/v1/meta/*` whole-document meta object, and converged on a
//! second device whose sync root differs — with in-root membership
//! preserved, out-of-root entries dropped from the shared copy but kept
//! locally, and a §2.6-concurrent conflict resolved to one deterministic
//! winner plus one recoverable loser copy.
//!
//! Set `GARAGE_BIN=/tmp/claude-0/garage` to run; without it the suite skips.

#![cfg(feature = "sync")]

mod common;

use std::path::{Path, PathBuf};

use common::garage;
use rapidraw_lib::rrcloud_core::s3::{ListObjectsV2Request, S3Client};
use rapidraw_lib::sync::{Credentials, MetaKind, SyncManager, SyncSettings};

fn settings_for(garage: &garage::Garage, bucket: &str) -> SyncSettings {
    SyncSettings {
        enabled: true,
        endpoint: garage.endpoint(),
        bucket: bucket.to_string(),
        region: garage.region().to_string(),
        force_path_style: true,
        ..SyncSettings::default()
    }
}

fn creds_for(garage: &garage::Garage) -> Credentials {
    Credentials {
        access_key: garage.access_key_id.clone(),
        secret_key: garage.secret_access_key.clone(),
    }
}

/// One configured device: its own app-data dir (holding `rrcloud/` state and
/// the `albums/`+`presets/` meta docs, exactly the upstream layout), its own
/// sync root, and a live manager.
struct Device {
    _app_data: tempfile::TempDir,
    app_data_dir: PathBuf,
    _root: tempfile::TempDir,
    sync_root: PathBuf,
    mgr: std::sync::Arc<SyncManager>,
}

fn device(garage: &garage::Garage, bucket: &str) -> Device {
    let app_data = tempfile::tempdir().expect("app data dir");
    let root = tempfile::tempdir().expect("sync root");
    let app_data_dir = app_data.path().to_path_buf();
    let sync_root = root.path().to_path_buf();
    // State dir is `app_data_dir/rrcloud` (§3.3), so the manager derives the
    // meta docs' location as `app_data_dir/{albums,presets}/*.json`.
    let state_dir = app_data_dir.join("rrcloud");
    let mgr = SyncManager::new_inert();
    mgr.configure(
        settings_for(garage, bucket),
        creds_for(garage),
        sync_root.clone(),
        state_dir,
    )
    .expect("configure device");
    Device {
        _app_data: app_data,
        app_data_dir,
        _root: root,
        sync_root,
        mgr,
    }
}

impl Device {
    fn albums_path(&self) -> PathBuf {
        self.app_data_dir.join("albums").join("albums.json")
    }

    fn presets_path(&self) -> PathBuf {
        self.app_data_dir.join("presets").join("presets.json")
    }

    /// Writes `bytes` to the device-local meta doc for `kind` and notifies
    /// the engine (the §2.9 save-site hook's effect).
    fn save_meta(&self, kind: MetaKind, path: &Path, bytes: &[u8]) {
        std::fs::create_dir_all(path.parent().unwrap()).expect("mkdir meta");
        std::fs::write(path, bytes).expect("write meta doc");
        self.mgr.note_local_meta(kind, path);
    }

    /// Simulates a process restart: drops this device's live manager
    /// (releasing its redb handle — required before reopening the same
    /// `state.redb`) and returns a fresh one pointed at the SAME app-data
    /// dir / sync root. Whatever was only ever in that process's memory is
    /// gone; whatever was durably persisted survives.
    fn restart(self, garage: &garage::Garage, bucket: &str) -> Device {
        let Device {
            _app_data,
            app_data_dir,
            _root,
            sync_root,
            mgr,
        } = self;
        drop(mgr);
        let state_dir = app_data_dir.join("rrcloud");
        let mgr = SyncManager::new_inert();
        mgr.configure(
            settings_for(garage, bucket),
            creds_for(garage),
            sync_root.clone(),
            state_dir,
        )
        .expect("reconfigure device after simulated restart");
        Device {
            _app_data,
            app_data_dir,
            _root,
            sync_root,
            mgr,
        }
    }
}

/// Every key in `bucket` under `prefix`, sorted.
async fn keys_under(client: &S3Client, bucket: &str, prefix: &str) -> Vec<String> {
    let mut keys = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let page = client
            .list_objects_v2(
                bucket,
                &ListObjectsV2Request {
                    prefix: Some(prefix.to_string()),
                    continuation_token: token.take(),
                    ..Default::default()
                },
            )
            .await
            .expect("list_objects_v2");
        keys.extend(page.objects.into_iter().map(|o| o.key));
        if !page.is_truncated {
            break;
        }
        token = page.next_continuation_token;
    }
    keys.sort();
    keys
}

async fn get_bytes(client: &S3Client, bucket: &str, key: &str) -> Vec<u8> {
    client
        .get_object(bucket, key, None)
        .await
        .expect("get_object")
        .body
        .collect()
        .await
        .expect("collect body")
        .to_vec()
}

/// Flattened `images` of every album in a document, in order.
fn album_images(doc: &[u8]) -> Vec<String> {
    fn walk(v: &serde_json::Value, out: &mut Vec<String>) {
        match v {
            serde_json::Value::Array(items) => items.iter().for_each(|i| walk(i, out)),
            serde_json::Value::Object(map) => {
                if let Some(serde_json::Value::Array(imgs)) = map.get("images") {
                    out.extend(imgs.iter().filter_map(|i| i.as_str().map(str::to_string)));
                }
                if let Some(children) = map.get("children") {
                    walk(children, out);
                }
            }
            _ => {}
        }
    }
    let v: serde_json::Value = serde_json::from_slice(doc).expect("doc parses");
    let mut out = Vec::new();
    walk(&v, &mut out);
    out
}

/// The top-level album names present in a document.
fn album_names(doc: &[u8]) -> Vec<String> {
    let v: serde_json::Value = serde_json::from_slice(doc).expect("doc parses");
    v.as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(|i| i.get("name").and_then(|n| n.as_str()).map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

#[tokio::test]
async fn albums_round_trip_across_two_sync_roots() {
    let Some(garage) = garage::shared() else {
        eprintln!("SKIP: no Garage binary; set GARAGE_BIN to run the albums sync test");
        return;
    };
    let bucket = garage.create_unique_bucket("albums-sync");
    let a = device(garage, &bucket);
    let b = device(garage, &bucket);

    // Device A's album references two images inside its sync root and one
    // outside it. Icons/names are not paths and must survive untouched.
    let in1 = a.sync_root.join("trip").join("a.jpg");
    let in2 = a.sync_root.join("trip").join("b.jpg");
    let outside = Path::new("/not/under/the/sync/root/z.jpg");
    let doc_a = serde_json::to_vec(&serde_json::json!([
        {
            "type": "album",
            "id": "al1",
            "name": "Trip",
            "icon": "star",
            "images": [
                in1.to_string_lossy(),
                in2.to_string_lossy(),
                outside.to_string_lossy()
            ]
        }
    ]))
    .unwrap();
    a.save_meta(MetaKind::Albums, &a.albums_path(), &doc_a);

    a.mgr.run_once().await.expect("device A cycle");

    // The meta object must have landed.
    let client = garage.client();
    let meta_keys = keys_under(&client, &bucket, ".rrcloud/v1/meta/").await;
    assert!(
        meta_keys
            .iter()
            .any(|k| k == ".rrcloud/v1/meta/albums.json"),
        "albums meta object must land in the bucket, got {meta_keys:?}"
    );

    // Device A's LOCAL file keeps the out-of-root entry untouched (it stays
    // device-local; only the uploaded copy drops it).
    let a_local = std::fs::read(a.albums_path()).expect("read A local albums");
    assert!(
        album_images(&a_local)
            .iter()
            .any(|p| p == &outside.to_string_lossy()),
        "device A's local albums.json must keep the out-of-root image"
    );

    // Device B converges: its albums.json gains the album with in-root paths
    // rebased onto B's sync root, and WITHOUT the out-of-root entry.
    b.mgr.run_once().await.expect("device B cycle");
    let b_local = std::fs::read(b.albums_path()).expect("device B must write albums.json locally");
    assert_eq!(
        album_names(&b_local),
        vec!["Trip".to_string()],
        "B gains the album"
    );
    assert_eq!(
        album_images(&b_local),
        vec![
            b.sync_root
                .join("trip")
                .join("a.jpg")
                .to_string_lossy()
                .into_owned(),
            b.sync_root
                .join("trip")
                .join("b.jpg")
                .to_string_lossy()
                .into_owned(),
        ],
        "B's membership is the in-root images mapped to B's root; the out-of-root entry is absent"
    );
}

#[tokio::test]
async fn presets_sync_maps_lut_paths_to_second_device() {
    let Some(garage) = garage::shared() else {
        eprintln!("SKIP: no Garage binary; set GARAGE_BIN to run the presets sync test");
        return;
    };
    let bucket = garage.create_unique_bucket("presets-sync");
    let a = device(garage, &bucket);
    let b = device(garage, &bucket);

    let lut = a.sync_root.join("luts").join("warm.cube");
    let doc_a = serde_json::to_vec(&serde_json::json!([
        {
            "preset": {
                "id": "p1",
                "name": "Warm",
                "adjustments": { "exposure": 0.25, "lutPath": lut.to_string_lossy() }
            }
        }
    ]))
    .unwrap();
    a.save_meta(MetaKind::Presets, &a.presets_path(), &doc_a);
    a.mgr.run_once().await.expect("device A cycle");

    let client = garage.client();
    let meta_keys = keys_under(&client, &bucket, ".rrcloud/v1/meta/").await;
    assert!(
        meta_keys
            .iter()
            .any(|k| k == ".rrcloud/v1/meta/presets.json"),
        "presets meta object must land, got {meta_keys:?}"
    );

    b.mgr.run_once().await.expect("device B cycle");
    let b_local =
        std::fs::read(b.presets_path()).expect("device B must write presets.json locally");
    let v: serde_json::Value = serde_json::from_slice(&b_local).unwrap();
    assert_eq!(v[0]["preset"]["name"], "Warm", "B gains the preset");
    assert_eq!(
        v[0]["preset"]["adjustments"]["lutPath"],
        serde_json::Value::String(
            b.sync_root
                .join("luts")
                .join("warm.cube")
                .to_string_lossy()
                .into_owned()
        ),
        "the in-root lutPath rebases onto device B's sync root"
    );
}

#[tokio::test]
async fn concurrent_album_edits_converge_to_one_winner_and_one_recoverable_loser() {
    let Some(garage) = garage::shared() else {
        eprintln!("SKIP: no Garage binary; set GARAGE_BIN to run the conflict test");
        return;
    };
    let bucket = garage.create_unique_bucket("albums-conflict");
    let a = device(garage, &bucket);
    let b = device(garage, &bucket);

    // Both devices create a DISTINCT album while offline from each other.
    let doc_a = serde_json::to_vec(&serde_json::json!([
        { "type": "album", "id": "fa", "name": "FromA", "icon": null,
          "images": [a.sync_root.join("a.jpg").to_string_lossy()] }
    ]))
    .unwrap();
    let doc_b = serde_json::to_vec(&serde_json::json!([
        { "type": "album", "id": "fb", "name": "FromB", "icon": null,
          "images": [b.sync_root.join("b.jpg").to_string_lossy()] }
    ]))
    .unwrap();

    // A publishes first.
    a.save_meta(MetaKind::Albums, &a.albums_path(), &doc_a);
    a.mgr.run_once().await.expect("A publishes its version");

    // B edits WITHOUT having seen A's version, then cycles: its poll learns
    // A's head, its own dirty edit commits as a concurrent version, and §2.6
    // resolves the pair deterministically (winner live, loser materialized).
    b.save_meta(MetaKind::Albums, &b.albums_path(), &doc_b);
    b.mgr
        .run_once()
        .await
        .expect("B cycle resolves the conflict");

    // A cycles again to observe B's version + the loser copy and converge.
    a.mgr.run_once().await.expect("A converges");
    // A second B cycle guarantees both have applied every published entry.
    b.mgr.run_once().await.expect("B settles");

    // Both devices' local albums.json are byte-identical after converging on
    // the deterministic winner (modulo the per-device absolute paths, so
    // compare album NAMES, which are not relativized).
    let a_local = std::fs::read(a.albums_path()).expect("A local");
    let b_local = std::fs::read(b.albums_path()).expect("B local");
    let winner_a = album_names(&a_local);
    let winner_b = album_names(&b_local);
    assert_eq!(
        winner_a, winner_b,
        "both devices converge on the same winning album set"
    );
    assert_eq!(winner_a.len(), 1, "exactly one album is the live winner");

    // Exactly one conflict loser object exists, and it carries the OTHER
    // album — no edit was lost, the loser is fully recoverable.
    let client = garage.client();
    let meta_keys = keys_under(&client, &bucket, ".rrcloud/v1/meta/").await;
    let conflict_keys: Vec<&String> = meta_keys
        .iter()
        .filter(|k| k.starts_with(".rrcloud/v1/meta/albums.conflict-") && k.ends_with(".json"))
        .collect();
    assert_eq!(
        conflict_keys.len(),
        1,
        "exactly one deterministic loser copy must exist, got {meta_keys:?}"
    );
    let loser_bytes = get_bytes(&client, &bucket, conflict_keys[0]).await;
    let loser_names = album_names(&loser_bytes);
    assert_eq!(loser_names.len(), 1, "the loser copy holds one album");

    let winner = winner_a[0].clone();
    let loser = loser_names[0].clone();
    let mut got = [winner, loser];
    got.sort();
    assert_eq!(
        got,
        ["FromA".to_string(), "FromB".to_string()],
        "the winner and the recoverable loser together preserve both devices' edits"
    );
}

#[tokio::test]
async fn concurrent_first_publishers_recover_on_next_appearance_after_restart() {
    let Some(garage) = garage::shared() else {
        eprintln!("SKIP: no Garage binary; set GARAGE_BIN to run the race-recovery test");
        return;
    };
    let bucket = garage.create_unique_bucket("albums-race-restart");
    let a = device(garage, &bucket);
    let b = device(garage, &bucket);

    // Both devices save a DISTINCT album while NEITHER has ever polled —
    // each believes it is the first publisher of albums.json.
    let doc_a = serde_json::to_vec(&serde_json::json!([
        { "type": "album", "id": "fa", "name": "FromA", "icon": null,
          "images": [a.sync_root.join("a.jpg").to_string_lossy()] }
    ]))
    .unwrap();
    let doc_b = serde_json::to_vec(&serde_json::json!([
        { "type": "album", "id": "fb", "name": "FromB", "icon": null,
          "images": [b.sync_root.join("b.jpg").to_string_lossy()] }
    ]))
    .unwrap();
    a.save_meta(MetaKind::Albums, &a.albums_path(), &doc_a);
    b.save_meta(MetaKind::Albums, &b.albums_path(), &doc_b);

    // Race the two "first publish" cycles directly against each other. A
    // bare client-side retry cannot always out-race a second device writing
    // to the very same shared key with no conditional-PUT guarantee from
    // the backend to lean on (verified empirically: Garage v2.2.0 answers
    // 2xx to a PUT with `If-None-Match: *` regardless of whether the key
    // already has an object) — so this may, rarely, still leave the race
    // itself undetected by either side in the same instant.
    let (ra, rb) = tokio::join!(a.mgr.run_once(), b.mgr.run_once());
    ra.expect("A's racing cycle");
    rb.expect("B's racing cycle");

    // The scenario the finding actually describes: whichever device lost
    // the race goes offline immediately afterward — a process exit, not
    // just "the same still-running manager happens to poll again" — and
    // later comes back (a fresh process, same on-disk state). Each
    // device's own authored head must have survived that restart, so its
    // very next appearance deterministically resolves the conflict,
    // without relying on the original racing processes staying alive or on
    // winning a timing race a second time.
    let a = a.restart(garage, &bucket);
    let b = b.restart(garage, &bucket);
    a.mgr.run_once().await.expect("A's post-restart cycle");
    b.mgr.run_once().await.expect("B's post-restart cycle");

    let client = garage.client();
    let meta_keys = keys_under(&client, &bucket, ".rrcloud/v1/meta/").await;
    assert!(
        meta_keys
            .iter()
            .any(|k| k == ".rrcloud/v1/meta/albums.json"),
        "albums.json must exist after recovery, got {meta_keys:?}"
    );
    let live = get_bytes(&client, &bucket, ".rrcloud/v1/meta/albums.json").await;
    let live_names = album_names(&live);
    assert_eq!(live_names.len(), 1, "exactly one album is live");

    let conflict_keys: Vec<&String> = meta_keys
        .iter()
        .filter(|k| k.starts_with(".rrcloud/v1/meta/albums.conflict-") && k.ends_with(".json"))
        .collect();
    // The core guarantee this test exists to prove: the losing edit is
    // NEVER silently gone without a trace once each device has appeared
    // again after the race — at least one recoverable copy of it must
    // exist. (A fully adversarial same-instant race with no conditional-PUT
    // backend support can, rarely, leave more than one superseded-snapshot
    // copy rather than the single deterministic one a non-racing §2.6
    // resolution produces — still fully recoverable, never a silent loss,
    // just not a hard single-copy guarantee under true simultaneous
    // multi-way racing against a backend with no compare-and-swap.)
    assert!(
        !conflict_keys.is_empty(),
        "the losing edit must be recoverable as at least one conflict-loser object \
         once each device has appeared again after the race, got {meta_keys:?}"
    );

    let mut all_names = vec![live_names[0].clone()];
    for key in &conflict_keys {
        let loser_bytes = get_bytes(&client, &bucket, key).await;
        all_names.extend(album_names(&loser_bytes));
    }
    all_names.sort();
    all_names.dedup();
    assert_eq!(
        all_names,
        vec!["FromA".to_string(), "FromB".to_string()],
        "both devices' edits are preserved between the live doc and the \
         conflict-loser copies, got {meta_keys:?}"
    );
}

/// Round review blocker: `note_local_meta` bumps and durably persists the
/// [`MetaHead`] (`SyncDb::set_synced_meta_head`) immediately on save, but
/// previously left the dirty/pending-publish state in-memory only. A
/// restart between that save and the next sync cycle reseeds `meta_heads`
/// from redb with the ALREADY-BUMPED head but `meta_dirty` empty — so
/// `decide_meta` sees the local head as already dominant (`KeepLocal`) and,
/// seeing it as not-dirty, treats that as a clean no-op forever: the edit is
/// never published, silently and permanently, from every OTHER device's
/// point of view (the bytes are still on this device's own disk, so nothing
/// looks wrong locally — only a second device, or this device after a
/// factory-reset, would ever reveal it never arrived).
///
/// Exactly the repro the finding calls for: save, restart with NO cycle in
/// between, then run a single cycle and confirm the document landed.
#[tokio::test]
async fn a_local_meta_edit_survives_a_restart_before_the_first_sync_cycle() {
    let Some(garage) = garage::shared() else {
        eprintln!("SKIP: no Garage binary; set GARAGE_BIN to run the restart-durability test");
        return;
    };
    let bucket = garage.create_unique_bucket("albums-restart-durability");
    let a = device(garage, &bucket);

    let doc_a = serde_json::to_vec(&serde_json::json!([
        { "type": "album", "id": "fa", "name": "FromA", "icon": null,
          "images": [a.sync_root.join("a.jpg").to_string_lossy()] }
    ]))
    .unwrap();
    a.save_meta(MetaKind::Albums, &a.albums_path(), &doc_a);

    // Restart BEFORE any cycle ever ran — the save-site intake is the only
    // thing that has happened to this device so far.
    let a = a.restart(garage, &bucket);
    a.mgr
        .run_once()
        .await
        .expect("the first post-restart cycle");

    let client = garage.client();
    let meta_keys = keys_under(&client, &bucket, ".rrcloud/v1/meta/").await;
    assert!(
        meta_keys
            .iter()
            .any(|k| k == ".rrcloud/v1/meta/albums.json"),
        "the edit admitted before the restart must still publish on the first \
         post-restart cycle, got {meta_keys:?}"
    );
    let live = get_bytes(&client, &bucket, ".rrcloud/v1/meta/albums.json").await;
    assert_eq!(
        album_names(&live),
        vec!["FromA".to_string()],
        "the published document must carry the pre-restart edit"
    );

    // A second cycle with nothing new to say must be a clean no-op: no
    // duplicate publish, no conflict manufactured against itself.
    a.mgr.run_once().await.expect("a clean no-op cycle");
    let meta_keys_after = keys_under(&client, &bucket, ".rrcloud/v1/meta/").await;
    assert_eq!(
        meta_keys_after,
        vec![".rrcloud/v1/meta/albums.json".to_string()],
        "a settled, unchanged document must not be re-published or conflict \
         with itself: {meta_keys_after:?}"
    );
}

#[tokio::test]
async fn adopt_remote_preserves_this_devices_out_of_root_album_membership() {
    let Some(garage) = garage::shared() else {
        eprintln!("SKIP: no Garage binary; set GARAGE_BIN to run the adopt-merge test");
        return;
    };
    let bucket = garage.create_unique_bucket("albums-adopt-merge");
    let a = device(garage, &bucket);
    let b = device(garage, &bucket);

    // A publishes an album with one in-root image and one out-of-root
    // image (the upload drops the out-of-root one; A's local copy keeps it).
    let in1 = a.sync_root.join("trip").join("a.jpg");
    let outside = Path::new("/not/under/the/sync/root/z.jpg");
    let doc_a = serde_json::to_vec(&serde_json::json!([
        {
            "type": "album", "id": "al1", "name": "Trip", "icon": null,
            "images": [in1.to_string_lossy(), outside.to_string_lossy()]
        }
    ]))
    .unwrap();
    a.save_meta(MetaKind::Albums, &a.albums_path(), &doc_a);
    a.mgr.run_once().await.expect("A publishes");

    // B adopts (a plain, non-conflicting AdoptRemote — B never touched
    // albums.json before this), then makes an ordinary descendant edit
    // (adds another in-root image) and republishes it.
    b.mgr.run_once().await.expect("B adopts");
    let b_doc = std::fs::read(b.albums_path()).expect("B local after adopt");
    let mut v: serde_json::Value = serde_json::from_slice(&b_doc).unwrap();
    let in2_b = b.sync_root.join("trip").join("b.jpg");
    v[0]["images"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::Value::String(
            in2_b.to_string_lossy().into_owned(),
        ));
    let b_doc = serde_json::to_vec(&v).unwrap();
    b.save_meta(MetaKind::Albums, &b.albums_path(), &b_doc);
    b.mgr
        .run_once()
        .await
        .expect("B republishes its descendant edit");

    // A's next cycle is a plain (non-dirty) AdoptRemote: it must gain B's
    // new image WITHOUT losing its own out-of-root membership, which the
    // shared document never carried and never can.
    a.mgr
        .run_once()
        .await
        .expect("A adopts B's descendant edit");
    let a_local = std::fs::read(a.albums_path()).expect("A local after second adopt");
    let imgs = album_images(&a_local);
    assert!(
        imgs.iter().any(|p| p == &outside.to_string_lossy()),
        "A's out-of-root image must survive a later AdoptRemote, got {imgs:?}"
    );
    assert!(
        imgs.iter().any(|p| p == &in1.to_string_lossy()),
        "A's original in-root image must still be present, got {imgs:?}"
    );
    assert!(
        imgs.iter()
            .any(|p| p == &a.sync_root.join("trip").join("b.jpg").to_string_lossy()),
        "B's new in-root image must be adopted, rebased onto A's root, got {imgs:?}"
    );
}

/// Round review blocker: the `remote_wins: true` arm of a §2.6 concurrent
/// meta conflict must publish the elementwise-max vv to the SHARED key, not
/// just believe it locally — otherwise the live object's vv never advances
/// past the winner's own un-merged value, the same concurrent pair compares
/// `Concurrent` again next cycle (never `Equal`/`Greater`), and the resolving
/// device's dirty flag can never clear (architecture §2.6: "the key's vv ←
/// elementwise max of both vvs, so the next edit on either device dominates
/// both branches and the conflict cannot reopen").
///
/// Deterministic without timing luck: `ts` is captured at `note_local_meta`
/// time (wall-clock seconds), and the §2.6 concurrent tiebreak is `ts` first,
/// device id only on a tie (`clock::pick_winner`). B saves (and is dirty)
/// first; after a full second has elapsed, A saves and publishes — so A's
/// head strictly postdates B's, and when B resolves the conflict against A,
/// A (remote) deterministically wins regardless of either device's random
/// UUID, landing B in the very `remote_wins: true` arm under test.
#[tokio::test]
async fn concurrent_conflict_remote_wins_merges_the_live_vv_so_it_cannot_reopen() {
    let Some(garage) = garage::shared() else {
        eprintln!("SKIP: no Garage binary; set GARAGE_BIN to run the conflict-vv-merge test");
        return;
    };
    let bucket = garage.create_unique_bucket("albums-conflict-vv-merge");
    let a = device(garage, &bucket);
    let b = device(garage, &bucket);

    // B saves (and goes dirty) first, at the earlier ts.
    let doc_b = serde_json::to_vec(&serde_json::json!([
        { "type": "album", "id": "fb", "name": "FromB", "icon": null,
          "images": [b.sync_root.join("b.jpg").to_string_lossy()] }
    ]))
    .unwrap();
    b.save_meta(MetaKind::Albums, &b.albums_path(), &doc_b);

    // A full second later (ts is whole unix seconds), A saves and publishes
    // first — A's head strictly postdates B's.
    std::thread::sleep(std::time::Duration::from_millis(1_200));
    let doc_a = serde_json::to_vec(&serde_json::json!([
        { "type": "album", "id": "fa", "name": "FromA", "icon": null,
          "images": [a.sync_root.join("a.jpg").to_string_lossy()] }
    ]))
    .unwrap();
    a.save_meta(MetaKind::Albums, &a.albums_path(), &doc_a);
    a.mgr.run_once().await.expect("A publishes first");

    // B's cycle now resolves a concurrent conflict against A's (later-ts,
    // dominant) head: A/remote must win the deterministic tiebreak.
    b.mgr
        .run_once()
        .await
        .expect("B resolves the conflict (remote/A wins)");

    let client = garage.client();
    let a_id = a.mgr.device_id().expect("A has a device id");
    let b_id = b.mgr.device_id().expect("B has a device id");

    let head = client
        .head_object(&bucket, ".rrcloud/v1/meta/albums.json")
        .await
        .expect("head the live albums document");
    let vv_json = head
        .metadata
        .get("rr-vv")
        .expect("the live object must carry the rr-vv metadata head");
    let vv: std::collections::BTreeMap<String, u64> =
        serde_json::from_str(vv_json).expect("rr-vv metadata parses as a vv map");
    assert_eq!(
        vv.get(&a_id).copied(),
        Some(1),
        "live vv must carry the winner's (A's) bump: {vv:?}"
    );
    assert_eq!(
        vv.get(&b_id).copied(),
        Some(1),
        "live vv must ALSO carry the resolving device's (B's) bump — the \
         elementwise max of both sides — or the exact same concurrent pair \
         reopens the conflict on the next comparison: {vv:?}"
    );

    // The other half of the same bug: with the vv never advancing, B's
    // resolve-and-verify loop can never observe its own intended head, so it
    // falls through without ever clearing `meta_dirty` — re-resolving the
    // identical (already-resolved) conflict forever. A clean no-op re-cycle
    // must mint no additional conflict-loser copy.
    b.mgr
        .run_once()
        .await
        .expect("B's second cycle, with no further local edits, must be a clean no-op");
    let meta_keys_after = keys_under(&client, &bucket, ".rrcloud/v1/meta/").await;
    let conflict_keys: Vec<&String> = meta_keys_after
        .iter()
        .filter(|k| k.starts_with(".rrcloud/v1/meta/albums.conflict-") && k.ends_with(".json"))
        .collect();
    assert_eq!(
        conflict_keys.len(),
        1,
        "a settled conflict must mint exactly one recoverable loser copy, not re-resolve on \
         every subsequent cycle: {meta_keys_after:?}"
    );
}

/// Round review blocker: the overwrite PUT of the shared meta key (both the
/// `KeepLocal`-dirty republish and either side of a `Conflict` resolution) is
/// unconditional — it is issued from a decision made against a `HeadObject`
/// read that may by now be stale, with no recheck immediately before the
/// write. A third device's concurrent publish landing in that gap is
/// silently destroyed: not live, and — because the clobbering device never
/// knew the third device existed — captured in no conflict-loser copy
/// either, contradicting meta.rs's documented "no album/preset data is ever
/// lost" and §2.6/§2.9's lossless-conflict guarantee.
///
/// Reproduced with two devices (B, C) that each independently resolve a
/// concurrent conflict against the SAME already-published baseline (A's
/// document) **without ever learning of each other**, racing their
/// resolutions concurrently (`tokio::join!`) against the same real Garage
/// backend. Whichever finishes last unconditionally overwrites the other's
/// freshly-resolved document — real network interleaving decides the order,
/// but either order loses data identically on the current unconditional-PUT
/// code: both B and C compute a conflict-loser copy of A's content only
/// (each is unaware of the other), so the loser's own edit is recorded
/// nowhere once it is overwritten. A correct implementation must detect the
/// gap (a fresh recheck immediately before the overwrite) and fall back to
/// re-resolving against what is now actually live, so after a few settling
/// cycles every one of the three devices' edits is recoverable — live or as
/// a conflict-loser copy — never silently gone.
#[tokio::test]
async fn an_unaware_third_devices_concurrent_edit_is_never_silently_clobbered() {
    let Some(garage) = garage::shared() else {
        eprintln!("SKIP: no Garage binary; set GARAGE_BIN to run the third-writer clobber test");
        return;
    };
    let bucket = garage.create_unique_bucket("albums-third-writer");
    let a = device(garage, &bucket);
    let b = device(garage, &bucket);
    let c = device(garage, &bucket);

    let doc_a = serde_json::to_vec(&serde_json::json!([
        { "type": "album", "id": "fa", "name": "FromA", "icon": null,
          "images": [a.sync_root.join("a.jpg").to_string_lossy()] }
    ]))
    .unwrap();
    a.save_meta(MetaKind::Albums, &a.albums_path(), &doc_a);
    a.mgr.run_once().await.expect("A publishes the baseline");

    // B and C each save a DISTINCT concurrent edit, neither having seen the
    // other (nor re-fetched after A — both still only know A's baseline).
    // `ts` is whole unix seconds (§2.6 tiebreak); a second between each save
    // guarantees ts_A < ts_B < ts_C strictly, so BOTH B's and C's resolution
    // against A deterministically has the local (dirty) side win regardless
    // of either device's random UUID — isolating the test to the race
    // between B and C's own unconditional overwrites (the bug under test),
    // with no dependence on which side a concurrent-tiebreak happens to pick.
    std::thread::sleep(std::time::Duration::from_millis(1_200));
    let doc_b = serde_json::to_vec(&serde_json::json!([
        { "type": "album", "id": "fb", "name": "FromB", "icon": null,
          "images": [b.sync_root.join("b.jpg").to_string_lossy()] }
    ]))
    .unwrap();
    b.save_meta(MetaKind::Albums, &b.albums_path(), &doc_b);
    std::thread::sleep(std::time::Duration::from_millis(1_200));
    let doc_c = serde_json::to_vec(&serde_json::json!([
        { "type": "album", "id": "fc", "name": "FromC", "icon": null,
          "images": [c.sync_root.join("c.jpg").to_string_lossy()] }
    ]))
    .unwrap();
    c.save_meta(MetaKind::Albums, &c.albums_path(), &doc_c);

    // Race B's and C's resolutions concurrently against the same live key —
    // each believes it is only resolving against A, unaware the other is
    // doing the very same thing at the same time.
    let (r_b, r_c) = tokio::join!(b.mgr.run_once(), c.mgr.run_once());
    r_b.expect("B's cycle must not error");
    r_c.expect("C's cycle must not error");

    // Let everything settle: further cycles adopt whatever ultimately won,
    // with no more local edits pending anywhere.
    for _ in 0..3 {
        a.mgr.run_once().await.expect("A settles");
        b.mgr.run_once().await.expect("B settles");
        c.mgr.run_once().await.expect("C settles");
    }

    let client = garage.client();
    let meta_keys = keys_under(&client, &bucket, ".rrcloud/v1/meta/").await;
    let live = get_bytes(&client, &bucket, ".rrcloud/v1/meta/albums.json").await;
    let mut recoverable_names: Vec<String> = album_names(&live);
    let conflict_keys: Vec<&String> = meta_keys
        .iter()
        .filter(|k| k.starts_with(".rrcloud/v1/meta/albums.conflict-") && k.ends_with(".json"))
        .collect();
    for key in &conflict_keys {
        let bytes = get_bytes(&client, &bucket, key).await;
        recoverable_names.extend(album_names(&bytes));
    }
    recoverable_names.sort();
    recoverable_names.dedup();
    assert_eq!(
        recoverable_names,
        vec![
            "FromA".to_string(),
            "FromB".to_string(),
            "FromC".to_string()
        ],
        "every device's edit must be recoverable — live or as a conflict-loser copy — \
         never silently destroyed by an unconditional overwrite; got live+losers \
         {recoverable_names:?} from meta keys {meta_keys:?}"
    );
}

/// Round review blocker: in the `remote_wins: true` arm of a §2.6 concurrent
/// meta conflict, the losing device's ONLY on-disk copy of its own edit is
/// overwritten by `write_local_meta` (adopting the winner) BEFORE that edit's
/// bytes are durably preserved as the conflict loser. A crash — or here, an
/// ordinary I/O failure — landing between those two writes permanently
/// destroys the loser with no copy left anywhere: not on disk (overwritten
/// mid-way... except the write itself is what's under test and must be made
/// to fail), not in the bucket (the loser PUT never ran). This contradicts
/// meta.rs's and ARCHITECTURE §2.6/§2.9's "no album/preset data is ever
/// lost" guarantee, and inverts the already-established ordering this same
/// codebase uses correctly for sidecar conflicts (`engine.rs::resolve_concurrent`
/// materializes the loser before `adopt_remote` overwrites the primary file).
///
/// Deterministic, no timing luck: B saves (and is dirty) first; a full
/// second later A saves and publishes, so A's head strictly postdates B's
/// and B's resolution against it deterministically lands in the
/// `remote_wins: true` arm under test (same technique as
/// `concurrent_conflict_remote_wins_merges_the_live_vv_so_it_cannot_reopen`).
/// Just before B's resolving cycle, B's local `albums/` directory is
/// replaced with a plain file, so `write_local_meta`'s `create_dir_all`
/// deterministically fails — a real, reproducible I/O failure standing in
/// for "the process is interrupted right there" — exercising exactly the
/// ordering the finding is about without relying on process-kill timing.
///
/// Under the pre-fix ordering (overwrite, THEN stage the loser) this
/// deterministically fails: `write_local_meta`'s error returns before the
/// loser PUT ever runs, so B's edit is recoverable nowhere. Under the fix
/// (stage the loser durably, THEN attempt the overwrite) the loser is
/// already safely in the bucket by the time the injected failure surfaces.
#[tokio::test]
async fn conflict_loser_is_staged_before_the_destructive_local_overwrite_can_run() {
    let Some(garage) = garage::shared() else {
        eprintln!("SKIP: no Garage binary; set GARAGE_BIN to run the loser-ordering test");
        return;
    };
    let bucket = garage.create_unique_bucket("albums-loser-ordering");
    let a = device(garage, &bucket);
    let b = device(garage, &bucket);

    // B saves (and goes dirty) first, at the earlier ts.
    let doc_b = serde_json::to_vec(&serde_json::json!([
        { "type": "album", "id": "fb", "name": "FromB", "icon": null,
          "images": [b.sync_root.join("b.jpg").to_string_lossy()] }
    ]))
    .unwrap();
    b.save_meta(MetaKind::Albums, &b.albums_path(), &doc_b);

    // A full second later, A saves and publishes first — A's head strictly
    // postdates B's, so B's resolution against it is deterministically
    // `remote_wins: true`.
    std::thread::sleep(std::time::Duration::from_millis(1_200));
    let doc_a = serde_json::to_vec(&serde_json::json!([
        { "type": "album", "id": "fa", "name": "FromA", "icon": null,
          "images": [a.sync_root.join("a.jpg").to_string_lossy()] }
    ]))
    .unwrap();
    a.save_meta(MetaKind::Albums, &a.albums_path(), &doc_a);
    a.mgr.run_once().await.expect("A publishes first");

    // Sabotage B's local meta directory so `write_local_meta` (adopting A's
    // winning document) deterministically fails: `create_dir_all` on a path
    // whose component already exists as a plain file errors out, instead of
    // silently succeeding the way it would against an already-correct
    // directory.
    let albums_dir = b.albums_path().parent().unwrap().to_path_buf();
    std::fs::remove_dir_all(&albums_dir).expect("remove B's albums dir");
    std::fs::write(&albums_dir, b"not a directory").expect("replace it with a plain file");

    // B's cycle resolves the conflict (A/remote wins) and must fail trying
    // to adopt A's document locally — but the failure must happen AFTER B's
    // own losing edit is already durably staged as the recoverable loser.
    let result = b.mgr.run_once().await;
    assert!(
        result.is_err(),
        "the injected I/O failure must surface as a cycle error, not be silently swallowed"
    );

    let client = garage.client();
    let meta_keys = keys_under(&client, &bucket, ".rrcloud/v1/meta/").await;
    let conflict_keys: Vec<&String> = meta_keys
        .iter()
        .filter(|k| k.starts_with(".rrcloud/v1/meta/albums.conflict-") && k.ends_with(".json"))
        .collect();
    assert_eq!(
        conflict_keys.len(),
        1,
        "B's losing edit must already be durably staged as a recoverable conflict-loser \
         object by the time the local-overwrite failure surfaces, got meta keys {meta_keys:?}"
    );
    let loser_bytes = get_bytes(&client, &bucket, conflict_keys[0]).await;
    assert_eq!(
        album_names(&loser_bytes),
        vec!["FromB".to_string()],
        "the staged loser must hold B's own (about to be overwritten) content"
    );
}

/// Round 4 review blocker: `MetaDecision::Converged` fires not only for an
/// equal vv but ALSO for a *concurrent* vv when the two sides' content
/// happens to be byte-identical (§2.6 case 1, checked before the vv order
/// at all — two devices independently producing the same bytes, e.g. the
/// same album created on both before either ever saw the other). The §2.9
/// doc comment for that case is "adopt metadata" — the vv must become the
/// elementwise max of both sides, exactly as the `AdoptRemote`/`Conflict`
/// arms already do — because the whole point of carrying a vv forward is
/// so a LATER, genuinely divergent edit is compared against the device's
/// full causal history, not just its own component.
///
/// This device (B) converges on content byte-identical to A's (same
/// relative image path, so relativization produces the same `rr://`
/// bytes) while each side's vv is still only its own component — a
/// concurrent pair the blake3 check short-circuits to `Converged` before
/// the vv order is even consulted. B then makes a REAL, different edit on
/// top of that converged state. If the convergence correctly adopted the
/// vv-max, B's new version strictly dominates A's untouched remote
/// (`{A:1,B:2}` ⊇ `{A:1}`) and simply republishes — no conflict is
/// possible because there is no concurrent remote edit anywhere in this
/// scenario. If convergence left B's stale, A-less vv in place (the bug),
/// B's new version (`{B:2}`) is incomparable with remote's `{A:1}` and
/// §2.6 case 4 fires on a pair that was never actually concurrent,
/// manufacturing a spurious conflict-loser object out of thin air.
#[tokio::test]
async fn converged_equal_content_under_concurrent_vv_adopts_the_vv_max() {
    let Some(garage) = garage::shared() else {
        eprintln!("SKIP: no Garage binary; set GARAGE_BIN to run the converged-vv-max test");
        return;
    };
    let bucket = garage.create_unique_bucket("albums-converged-vv-max");
    let a = device(garage, &bucket);
    let b = device(garage, &bucket);

    // A and B each independently create THE SAME album, referencing an
    // image at the same relative path under their own (distinct) sync
    // roots — relativization depends only on the relative path, so the
    // uploaded/relativized bytes are byte-identical even though neither
    // device has ever talked to the other yet.
    let doc_a = serde_json::to_vec(&serde_json::json!([
        { "type": "album", "id": "seed", "name": "Seed", "icon": null,
          "images": [a.sync_root.join("trip").join("a.jpg").to_string_lossy()] }
    ]))
    .unwrap();
    let doc_b_seed = serde_json::to_vec(&serde_json::json!([
        { "type": "album", "id": "seed", "name": "Seed", "icon": null,
          "images": [b.sync_root.join("trip").join("a.jpg").to_string_lossy()] }
    ]))
    .unwrap();

    // A publishes first (vv {A:1}).
    a.save_meta(MetaKind::Albums, &a.albums_path(), &doc_a);
    a.mgr.run_once().await.expect("A publishes the seed album");

    // B independently saves the SAME content (vv {B:1} locally) and
    // converges against A's published head: equal blake3 short-circuits
    // the vv order (which is concurrent, {A:1} vs {B:1}) straight to
    // `Converged`. Nothing is written to the shared key.
    b.save_meta(MetaKind::Albums, &b.albums_path(), &doc_b_seed);
    b.mgr
        .run_once()
        .await
        .expect("B converges on identical content");

    let client = garage.client();
    let meta_keys_after_converge = keys_under(&client, &bucket, ".rrcloud/v1/meta/").await;
    assert_eq!(
        meta_keys_after_converge,
        vec![".rrcloud/v1/meta/albums.json".to_string()],
        "a pure content-convergence must not publish or conflict anything, \
         got {meta_keys_after_converge:?}"
    );

    // B now makes a REAL, different edit on top of the converged state —
    // there is no concurrent remote edit anywhere in this scenario (A has
    // not touched albums.json since its first publish), so this can only
    // ever be an ordinary descendant version.
    let doc_b_v2 = serde_json::to_vec(&serde_json::json!([
        { "type": "album", "id": "fromb2", "name": "FromBv2", "icon": null,
          "images": [b.sync_root.join("other").join("b.jpg").to_string_lossy()] }
    ]))
    .unwrap();
    b.save_meta(MetaKind::Albums, &b.albums_path(), &doc_b_v2);
    b.mgr
        .run_once()
        .await
        .expect("B republishes its genuine descendant edit");

    let meta_keys = keys_under(&client, &bucket, ".rrcloud/v1/meta/").await;
    let conflict_keys: Vec<&String> = meta_keys
        .iter()
        .filter(|k| k.starts_with(".rrcloud/v1/meta/albums.conflict-") && k.ends_with(".json"))
        .collect();
    assert!(
        conflict_keys.is_empty(),
        "B's edit strictly descends from the converged head (there was never \
         a concurrent remote edit) — no conflict-loser may be manufactured, \
         got meta keys {meta_keys:?}"
    );
    let live = get_bytes(&client, &bucket, ".rrcloud/v1/meta/albums.json").await;
    assert_eq!(
        album_names(&live),
        vec!["FromBv2".to_string()],
        "B's genuine descendant edit must be the live document, not reverted \
         or lost to a spurious conflict"
    );

    // A third device (or A itself, on its next cycle) must plainly adopt
    // B's edit as a clean descendant too.
    a.mgr
        .run_once()
        .await
        .expect("A adopts B's descendant edit");
    let a_local = std::fs::read(a.albums_path()).expect("A local after adopting");
    assert_eq!(
        album_names(&a_local),
        vec!["FromBv2".to_string()],
        "A must cleanly adopt B's descendant edit, not see a phantom conflict"
    );
    let meta_keys_final = keys_under(&client, &bucket, ".rrcloud/v1/meta/").await;
    assert_eq!(
        meta_keys_final,
        vec![".rrcloud/v1/meta/albums.json".to_string()],
        "A's adopt cycle must not manufacture a conflict either, got {meta_keys_final:?}"
    );
}
