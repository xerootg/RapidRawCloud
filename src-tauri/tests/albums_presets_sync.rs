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
        meta_keys.iter().any(|k| k == ".rrcloud/v1/meta/albums.json"),
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
        .push(serde_json::Value::String(in2_b.to_string_lossy().into_owned()));
    let b_doc = serde_json::to_vec(&v).unwrap();
    b.save_meta(MetaKind::Albums, &b.albums_path(), &b_doc);
    b.mgr.run_once().await.expect("B republishes its descendant edit");

    // A's next cycle is a plain (non-dirty) AdoptRemote: it must gain B's
    // new image WITHOUT losing its own out-of-root membership, which the
    // shared document never carried and never can.
    a.mgr.run_once().await.expect("A adopts B's descendant edit");
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
