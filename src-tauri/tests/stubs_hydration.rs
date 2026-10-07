//! P2 §3.5 Garage-backed tests: stub stability and the hydration
//! round-trip. Feature-gated to `sync`; set `GARAGE_BIN=/tmp/claude-0/garage`
//! to run them against a real Garage bucket.
//!
//! These pin the two hardest §3.5 guarantees:
//!   * **Stub stability** — a stub's mtime equals the remote original's, so
//!     `compute_thumbnail_cache_hash` (blake3 of abs path + mtime +
//!     adjustments) is byte-identical before and after hydration; a stub is
//!     an `is_cloud_placeholder`, a hydrated original is not.
//!   * **Hydration round-trip** — device A uploads an original; device B
//!     (its own SyncManager / temp root / state, same bucket) makes a stub
//!     for it, then `ensure_local` downloads + blake3-verifies + installs the
//!     real bytes (identical to A's source), restores the remote mtime, and
//!     drops the placeholder flag; a second `ensure_local` is a no-op.

#![cfg(feature = "sync")]

mod common;

use std::path::Path;
use std::sync::OnceLock;

use common::garage;
use rapidraw_lib::sync::{self, Credentials, SyncManager, SyncSettings, ThumbVariant};
use tokio::sync::{Mutex, MutexGuard};

/// Serializes installers of the one process-global `SyncManager` (see the
/// sibling e2e test): held across the async awaits, so an await-aware mutex.
async fn global_guard() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(())).lock().await
}

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

/// The remote facts of an original the eviction/hydration gate needs.
struct RemoteOriginal {
    blake3_hex: String,
    size: u64,
    mtime_unix: i64,
    bytes: Vec<u8>,
}

/// A deterministic non-trivial original (a small valid-ish blob; the
/// transfer engine treats originals as opaque bytes verified by blake3).
fn original_bytes(seed: u8, len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| seed.wrapping_add((i % 251) as u8))
        .collect()
}

fn mtime_unix_secs(path: &Path) -> i64 {
    std::fs::metadata(path)
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Device A writes an original at `rel`, enqueues it, and runs one cycle so
/// the bytes + journal land in the bucket. Returns the remote facts a stub
/// needs (blake3, size, mtime).
async fn upload_original(
    mgr: &SyncManager,
    root: &Path,
    rel: &str,
    bytes: &[u8],
) -> RemoteOriginal {
    let path = root.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).expect("mkdir original");
    std::fs::write(&path, bytes).expect("write original");
    let blake3_hex = blake3::hash(bytes).to_hex().to_string();
    let size = bytes.len() as u64;
    let mtime_unix = mtime_unix_secs(&path);

    mgr.note_new_original(&path);
    mgr.run_once().await.expect("device A upload cycle");

    RemoteOriginal {
        blake3_hex,
        size,
        mtime_unix,
        bytes: bytes.to_vec(),
    }
}

#[tokio::test]
async fn stub_mtime_keeps_the_thumbnail_cache_hash_stable_across_hydration() {
    let _g = global_guard().await;
    let Some(garage) = garage::shared() else {
        eprintln!("SKIP: no Garage binary; set GARAGE_BIN to run");
        return;
    };
    let bucket = garage.create_unique_bucket("p2-stub-stability");
    let settings = settings_for(garage, &bucket);
    let creds = creds_for(garage);

    // Device A publishes an original.
    let root_a = tempfile::tempdir().expect("root a");
    let state_a = tempfile::tempdir().expect("state a");
    let mgr_a = SyncManager::new_inert();
    mgr_a
        .configure(
            settings.clone(),
            creds.clone(),
            root_a.path().to_path_buf(),
            state_a.path().to_path_buf(),
        )
        .expect("configure a");
    sync::install_global_manager(mgr_a.clone());

    let rel = "trip/DSC_1001.NEF";
    let remote = upload_original(&mgr_a, root_a.path(), rel, &original_bytes(7, 64 * 1024)).await;

    // Device B makes a stub for the never-downloaded original.
    let root_b = tempfile::tempdir().expect("root b");
    let state_b = tempfile::tempdir().expect("state b");
    let mgr_b = SyncManager::new_inert();
    mgr_b
        .configure(
            settings,
            creds,
            root_b.path().to_path_buf(),
            state_b.path().to_path_buf(),
        )
        .expect("configure b");
    sync::install_global_manager(mgr_b.clone());

    let stub_path = root_b.path().join(rel);
    std::fs::create_dir_all(stub_path.parent().unwrap()).expect("mkdir stub dir");

    // The engine-apply API the test drives directly (§3.5): create a 0-byte
    // stub with the remote mtime. RED: `create_stub` is `todo!()`.
    mgr_b
        .create_stub(
            &stub_path,
            &remote.blake3_hex,
            remote.size,
            remote.mtime_unix,
        )
        .expect("create stub");

    // A stub is a 0-byte placeholder whose mtime equals the remote original.
    assert!(
        mgr_b.is_stub(&stub_path),
        "the path must register as a stub"
    );
    assert!(
        sync::is_cloud_placeholder(&stub_path),
        "a stub must read as a cloud placeholder on all platforms"
    );
    assert_eq!(
        std::fs::metadata(&stub_path).expect("stat stub").len(),
        0,
        "a stub is a 0-byte file"
    );
    assert_eq!(
        mtime_unix_secs(&stub_path),
        remote.mtime_unix,
        "the stub mtime must equal the remote original's"
    );

    // The thumbnail cache key is computed over abs path + mtime + adjustments.
    let path_str = stub_path.to_string_lossy().to_string();
    let adjustments = br#"{"exposure":0.0}"#;
    let hash_before =
        sync::compute_thumbnail_cache_hash(&path_str, adjustments).expect("hash before hydration");

    // Hydrate, then the hash must be identical (mtime preserved).
    mgr_b.ensure_local(&stub_path, "test").expect("hydrate");

    assert!(
        !mgr_b.is_stub(&stub_path),
        "after hydration the path is no longer a stub"
    );
    assert!(
        !sync::is_cloud_placeholder(&stub_path),
        "a hydrated original is not a cloud placeholder"
    );
    assert_eq!(
        mtime_unix_secs(&stub_path),
        remote.mtime_unix,
        "hydration must restore the remote mtime"
    );
    let hash_after =
        sync::compute_thumbnail_cache_hash(&path_str, adjustments).expect("hash after hydration");
    assert_eq!(
        hash_before, hash_after,
        "the thumbnail cache hash must be stable across hydration"
    );
}

#[tokio::test]
async fn ensure_local_downloads_verifies_installs_and_is_idempotent() {
    let _g = global_guard().await;
    let Some(garage) = garage::shared() else {
        eprintln!("SKIP: no Garage binary; set GARAGE_BIN to run");
        return;
    };
    let bucket = garage.create_unique_bucket("p2-hydrate-roundtrip");
    let settings = settings_for(garage, &bucket);
    let creds = creds_for(garage);

    let root_a = tempfile::tempdir().expect("root a");
    let state_a = tempfile::tempdir().expect("state a");
    let mgr_a = SyncManager::new_inert();
    mgr_a
        .configure(
            settings.clone(),
            creds.clone(),
            root_a.path().to_path_buf(),
            state_a.path().to_path_buf(),
        )
        .expect("configure a");
    sync::install_global_manager(mgr_a.clone());

    let rel = "album/IMG_2002.CR3";
    let remote = upload_original(&mgr_a, root_a.path(), rel, &original_bytes(42, 200 * 1024)).await;

    // Device B: separate manager, temp root, same bucket.
    let root_b = tempfile::tempdir().expect("root b");
    let state_b = tempfile::tempdir().expect("state b");
    let mgr_b = SyncManager::new_inert();
    mgr_b
        .configure(
            settings,
            creds,
            root_b.path().to_path_buf(),
            state_b.path().to_path_buf(),
        )
        .expect("configure b");
    sync::install_global_manager(mgr_b.clone());

    let stub_path = root_b.path().join(rel);
    std::fs::create_dir_all(stub_path.parent().unwrap()).expect("mkdir stub dir");
    mgr_b
        .create_stub(
            &stub_path,
            &remote.blake3_hex,
            remote.size,
            remote.mtime_unix,
        )
        .expect("create stub");

    // Pre-hydration: the original bytes are absent (0-byte stub).
    assert_eq!(
        std::fs::metadata(&stub_path).expect("stat stub").len(),
        0,
        "the original bytes must be absent before hydration"
    );

    // Hydrate: install the real bytes, blake3-identical to A's source.
    let installed = mgr_b.ensure_local(&stub_path, "test").expect("hydrate");
    assert_eq!(installed, stub_path, "hydration installs at the real path");

    let got = std::fs::read(&stub_path).expect("read hydrated original");
    assert_eq!(
        got, remote.bytes,
        "the hydrated bytes must equal device A's source bytes"
    );
    assert_eq!(
        blake3::hash(&got).to_hex().to_string(),
        remote.blake3_hex,
        "the hydrated bytes must blake3-match the remote original"
    );
    assert!(
        !mgr_b.is_stub(&stub_path),
        "no longer a stub after hydration"
    );

    // A second ensure_local is a no-op (already local).
    let again = mgr_b
        .ensure_local(&stub_path, "test")
        .expect("idempotent hydrate");
    assert_eq!(again, stub_path);
    let got2 = std::fs::read(&stub_path).expect("read again");
    assert_eq!(
        got2, remote.bytes,
        "a second ensure_local must not alter the bytes"
    );
}

#[tokio::test]
async fn seeding_a_thumb_hard_links_it_into_the_webview_cache_under_the_stub_key() {
    let _g = global_guard().await;
    let Some(garage) = garage::shared() else {
        eprintln!("SKIP: no Garage binary; set GARAGE_BIN to run");
        return;
    };
    let bucket = garage.create_unique_bucket("p2-thumb-seed");
    let settings = settings_for(garage, &bucket);
    let creds = creds_for(garage);

    let root_a = tempfile::tempdir().expect("root a");
    let state_a = tempfile::tempdir().expect("state a");
    let mgr_a = SyncManager::new_inert();
    mgr_a
        .configure(
            settings.clone(),
            creds.clone(),
            root_a.path().to_path_buf(),
            state_a.path().to_path_buf(),
        )
        .expect("configure a");
    sync::install_global_manager(mgr_a.clone());

    let rel = "seed/THM_01.NEF";
    let remote = upload_original(&mgr_a, root_a.path(), rel, &original_bytes(11, 32 * 1024)).await;

    let root_b = tempfile::tempdir().expect("root b");
    let state_b = tempfile::tempdir().expect("state b");
    let mgr_b = SyncManager::new_inert();
    mgr_b
        .configure(
            settings,
            creds,
            root_b.path().to_path_buf(),
            state_b.path().to_path_buf(),
        )
        .expect("configure b");
    sync::install_global_manager(mgr_b.clone());

    let stub_path = root_b.path().join(rel);
    std::fs::create_dir_all(stub_path.parent().unwrap()).expect("mkdir stub dir");
    mgr_b
        .create_stub(
            &stub_path,
            &remote.blake3_hex,
            remote.size,
            remote.mtime_unix,
        )
        .expect("create stub");

    // The webview thumbnail cache dir ($APPCACHE/thumbnails). Seeding must
    // hard-link (copy fallback) the JPEG here under the stub-path cache key,
    // leaving tauri.conf.json's asset scope untouched.
    let cache_thumbnails = tempfile::tempdir().expect("cache thumbs");
    let jpeg = original_bytes(200, 4096); // opaque bytes stand in for a JPEG

    let linked = mgr_b
        .seed_thumbnail(
            &stub_path,
            ThumbVariant::Small,
            &jpeg,
            cache_thumbnails.path(),
        )
        .expect("seed thumbnail");

    // The surfaced file lives in the webview cache dir, keyed `_small.jpg`,
    // and carries the seeded bytes — the UI picks it up with no network.
    assert!(
        linked.starts_with(cache_thumbnails.path()),
        "the seeded thumb must be surfaced under the webview cache dir, got {linked:?}"
    );
    assert!(
        linked
            .file_name()
            .unwrap()
            .to_string_lossy()
            .ends_with("_small.jpg"),
        "the surfaced thumb key must carry the `_small.jpg` suffix, got {linked:?}"
    );
    assert_eq!(
        std::fs::read(&linked).expect("read linked thumb"),
        jpeg,
        "the surfaced thumb must carry the seeded JPEG bytes"
    );
}

#[tokio::test]
async fn concurrent_ensure_local_waits_for_the_in_flight_hydration() {
    let _g = global_guard().await;
    let Some(garage) = garage::shared() else {
        eprintln!("SKIP: no Garage binary; set GARAGE_BIN to run");
        return;
    };
    let bucket = garage.create_unique_bucket("p2-hydrate-singleflight");
    let settings = settings_for(garage, &bucket);
    let creds = creds_for(garage);

    // Device A publishes a large-enough original that the hydration download
    // window is reliably observable.
    let root_a = tempfile::tempdir().expect("root a");
    let state_a = tempfile::tempdir().expect("state a");
    let mgr_a = SyncManager::new_inert();
    mgr_a
        .configure(
            settings.clone(),
            creds.clone(),
            root_a.path().to_path_buf(),
            state_a.path().to_path_buf(),
        )
        .expect("configure a");
    sync::install_global_manager(mgr_a.clone());

    let rel = "race/BIG_0001.NEF";
    let remote = upload_original(
        &mgr_a,
        root_a.path(),
        rel,
        &original_bytes(29, 20 * 1024 * 1024),
    )
    .await;

    // Device B makes a stub.
    let root_b = tempfile::tempdir().expect("root b");
    let state_b = tempfile::tempdir().expect("state b");
    let mgr_b = SyncManager::new_inert();
    mgr_b
        .configure(
            settings,
            creds,
            root_b.path().to_path_buf(),
            state_b.path().to_path_buf(),
        )
        .expect("configure b");
    sync::install_global_manager(mgr_b.clone());

    let stub_path = root_b.path().join(rel);
    std::fs::create_dir_all(stub_path.parent().unwrap()).expect("mkdir stub dir");
    mgr_b
        .create_stub(
            &stub_path,
            &remote.blake3_hex,
            remote.size,
            remote.mtime_unix,
        )
        .expect("create stub");

    // Thread A drives the real hydration.
    let mgr_thread = mgr_b.clone();
    let stub_thread = stub_path.clone();
    let handle = std::thread::spawn(move || mgr_thread.ensure_local(&stub_thread, "thread-a"));

    // Observe the in-flight `downloading` window, then race a second
    // ensure_local into it. download_item refuses an item already Downloading
    // with a typed StaleState (the single-driver contract); the manager must
    // treat that as "already in flight" and WAIT, not surface a hard failure.
    let mut observed_downloading = false;
    let start = std::time::Instant::now();
    while start.elapsed() < std::time::Duration::from_secs(60) {
        match mgr_b.item_sync_state(&stub_path).as_deref() {
            Some("downloading") => {
                observed_downloading = true;
                break;
            }
            // Thread A already reached a terminal state — window missed.
            Some("hydrated") | Some("synced") => break,
            _ => {}
        }
        std::thread::sleep(std::time::Duration::from_micros(200));
    }

    let concurrent = mgr_b.ensure_local(&stub_path, "concurrent");
    let a_result = handle.join().expect("thread A join");

    a_result.expect("thread A hydration must succeed");
    assert!(
        observed_downloading,
        "the test must observe the in-flight download window to exercise the race"
    );
    assert!(
        concurrent.is_ok(),
        "a concurrent ensure_local during an in-flight hydration must wait for it, \
         not hard-fail with StaleState: {concurrent:?}"
    );
    assert!(
        !mgr_b.is_stub(&stub_path),
        "the path is hydrated after both callers return"
    );
    assert_eq!(
        std::fs::read(&stub_path).expect("read hydrated"),
        remote.bytes,
        "the hydrated bytes must match the remote original"
    );
}

#[tokio::test]
async fn seeding_an_edited_stub_keys_the_thumb_where_the_grid_looks_it_up() {
    let _g = global_guard().await;
    let Some(garage) = garage::shared() else {
        eprintln!("SKIP: no Garage binary; set GARAGE_BIN to run");
        return;
    };
    let bucket = garage.create_unique_bucket("p2-thumb-seed-edited");
    let settings = settings_for(garage, &bucket);
    let creds = creds_for(garage);

    let root_a = tempfile::tempdir().expect("root a");
    let state_a = tempfile::tempdir().expect("state a");
    let mgr_a = SyncManager::new_inert();
    mgr_a
        .configure(
            settings.clone(),
            creds.clone(),
            root_a.path().to_path_buf(),
            state_a.path().to_path_buf(),
        )
        .expect("configure a");
    sync::install_global_manager(mgr_a.clone());

    let rel = "seed/THM_EDITED.NEF";
    let remote = upload_original(&mgr_a, root_a.path(), rel, &original_bytes(13, 16 * 1024)).await;

    let root_b = tempfile::tempdir().expect("root b");
    let state_b = tempfile::tempdir().expect("state b");
    let mgr_b = SyncManager::new_inert();
    mgr_b
        .configure(
            settings,
            creds,
            root_b.path().to_path_buf(),
            state_b.path().to_path_buf(),
        )
        .expect("configure b");
    sync::install_global_manager(mgr_b.clone());

    let stub_path = root_b.path().join(rel);
    std::fs::create_dir_all(stub_path.parent().unwrap()).expect("mkdir stub dir");
    mgr_b
        .create_stub(
            &stub_path,
            &remote.blake3_hex,
            remote.size,
            remote.mtime_unix,
        )
        .expect("create stub");

    // §3.5: sidecars are never stubbed — an edited cloud image has its real
    // sidecar on disk. Write an EDITED sidecar (non-default adjustments) next
    // to the stub, exactly as the eager sidecar mirror would.
    let sidecar = {
        let mut s = stub_path.clone().into_os_string();
        s.push(".rrdata");
        std::path::PathBuf::from(s)
    };
    let sidecar_json =
        r#"{"version":1,"rating":3,"adjustments":{"contrast":12.0,"exposure":0.75}}"#;
    std::fs::write(&sidecar, sidecar_json).expect("write edited sidecar");

    let cache_thumbnails = tempfile::tempdir().expect("cache thumbs");
    let jpeg = original_bytes(201, 4096);
    let linked = mgr_b
        .seed_thumbnail(
            &stub_path,
            ThumbVariant::Small,
            &jpeg,
            cache_thumbnails.path(),
        )
        .expect("seed thumbnail");

    // The key `generate_single_thumbnail_and_cache` looks up for an edited
    // image: stub path + mtime + the sidecar's serialized adjustments (§3.5
    // "the exact key generate_single_thumbnail_and_cache looks up"). The
    // seeded durable thumb must be surfaced under exactly that key, or the
    // grid gets a permanent cache miss and no thumbnail at all.
    let content = std::fs::read_to_string(&sidecar).expect("read sidecar");
    let meta: sync::ImageMetadata = serde_json::from_str(&content).expect("parse sidecar");
    let adjustments = serde_json::to_vec(&meta.adjustments).expect("serialize adjustments");
    let expected_hash =
        sync::compute_thumbnail_cache_hash(&stub_path.to_string_lossy(), &adjustments)
            .expect("expected cache hash");
    let expected_name = format!("{expected_hash}_small.jpg");

    assert_eq!(
        linked.file_name().unwrap().to_string_lossy(),
        expected_name,
        "an edited stub's seeded thumb must be keyed where the grid looks it up \
         (adjustments-keyed), not under the empty-adjustments key"
    );
}
