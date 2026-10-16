//! §4.4 Garage-backed proxy-mode as-shot test: a cloud stub edited through
//! its smart preview gets its ORIGINAL's as-shot inputs, including the
//! maker-note highlight preservation no DNG tag of the proxy can carry.
//!
//! Pins, on a real RAW:
//!   * eviction refreshes a local proxy that predates the as-shot record
//!     (simulated by breaking the record's creator id) while the original is
//!     still local, so the stub it leaves behind carries the record;
//!   * `sync::hooks::stub_as_shot` — what upstream's `as_shot(path)` returns
//!     for a stub — then yields exactly upstream's `read_as_shot` exposure of
//!     the original, and its white balance up to the DNG rational encoding.
//!
//! Needs Garage (`GARAGE_BIN`) and a NEF in the RAW corpus directory
//! (`RRCLOUD_RAW_CORPUS`, default `/tmp/claude-0/raws`); skips loudly without
//! either.

#![cfg(feature = "sync")]

mod common;

use std::path::{Path, PathBuf};

use common::garage;
use rapidraw_lib::rrcloud_core::proxy;
use rapidraw_lib::sync::proxy_support::read_as_shot;
use rapidraw_lib::sync::{self, Credentials, SyncManager, SyncSettings};

fn corpus_nef() -> Option<PathBuf> {
    let dir = std::env::var("RRCLOUD_RAW_CORPUS")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "/tmp/claude-0/raws".to_string());
    let mut nefs: Vec<PathBuf> = std::fs::read_dir(dir)
        .ok()?
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|e| e.eq_ignore_ascii_case("nef")))
        .collect();
    nefs.sort();
    nefs.into_iter().next()
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

fn mtime_unix_secs(path: &Path) -> i64 {
    std::fs::metadata(path)
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[tokio::test]
async fn an_evicted_stub_renders_with_its_originals_as_shot() {
    let Some(garage) = garage::shared() else {
        eprintln!("SKIP: no Garage binary; set GARAGE_BIN to run");
        return;
    };
    let Some(nef) = corpus_nef() else {
        eprintln!("SKIP: no NEF in the RAW corpus (set RRCLOUD_RAW_CORPUS)");
        return;
    };
    let raw = std::fs::read(&nef).expect("read corpus NEF");
    let original = read_as_shot(&raw).expect("upstream as-shot of the original");

    let bucket = garage.create_unique_bucket("p3-proxy-asshot");
    let settings = settings_for(garage, &bucket);
    let creds = creds_for(garage);
    let rel = "lib/DSC_0001.NEF";

    // Device A publishes the original.
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
    let path_a = root_a.path().join(rel);
    std::fs::create_dir_all(path_a.parent().unwrap()).expect("mkdir a");
    std::fs::write(&path_a, &raw).expect("write original");
    mgr_a.note_new_original(&path_a);
    mgr_a.run_once().await.expect("upload cycle");
    let blake3_hex = blake3::hash(&raw).to_hex().to_string();

    // Device B stubs it, hydrates it (so it is verified and evictable), and
    // builds its local smart preview.
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
    let path_b = root_b.path().join(rel);
    std::fs::create_dir_all(path_b.parent().unwrap()).expect("mkdir b");
    mgr_b
        .create_stub(
            &path_b,
            &blake3_hex,
            raw.len() as u64,
            mtime_unix_secs(&path_a),
        )
        .expect("stub");
    mgr_b.ensure_local(&path_b, "test").expect("hydrate");
    mgr_b.generate_proxy_for(&path_b).expect("generate proxy");

    let proxy_path = state_b
        .path()
        .join("previews")
        .join(format!("{blake3_hex}{}", proxy::PROXY_FILE_SUFFIX));
    let mut legacy = std::fs::read(&proxy_path).expect("local proxy written");
    assert!(
        proxy::read_as_shot_exposure(&legacy).is_some(),
        "a freshly generated proxy carries the as-shot record"
    );

    // Make it look like a proxy from before the record existed.
    let creator = b"RapidRawCloud\0";
    let at = legacy
        .windows(creator.len())
        .position(|w| w == creator)
        .expect("record present");
    legacy[at] = b'X';
    std::fs::write(&proxy_path, &legacy).expect("plant legacy proxy");
    assert_eq!(proxy::read_as_shot_exposure(&legacy), None);

    // Evict: the refresh runs while the original is still local.
    let report = mgr_b.run_evictor_with_budget(0).await.expect("evictor");
    assert!(
        report.evicted.contains(&path_b),
        "the original must be evicted, got {report:?}"
    );
    assert!(mgr_b.is_stub(&path_b), "the original is a stub again");

    let refreshed = std::fs::read(&proxy_path).expect("proxy survives eviction");
    let record = proxy::read_as_shot_exposure(&refreshed)
        .expect("eviction refreshed the legacy proxy with the as-shot record");
    let o = original.exposure;
    assert_eq!(
        (
            record.scene_referred,
            record.dng_baseline,
            record.highlight_preservation
        ),
        (o.scene_referred, o.dng_baseline, o.highlight_preservation),
        "the record is the original's exposure"
    );

    // What upstream's `as_shot(path)` returns for the stub.
    let stub = sync::hooks::stub_as_shot(&path_b).expect("a stub answers from its proxy");
    let p = stub.exposure;
    assert_eq!(
        (p.scene_referred, p.dng_baseline, p.highlight_preservation),
        (o.scene_referred, o.dng_baseline, o.highlight_preservation),
        "proxy-mode exposure must be the original's"
    );
    let (ow, pw) = (original.white_balance, stub.white_balance);
    assert!(
        (ow.temperature - pw.temperature).abs() <= 1.0 && (ow.tint - pw.tint).abs() <= 0.1,
        "proxy-mode as-shot WB drifted: original {ow:?}, stub {pw:?}"
    );
    eprintln!(
        "{}: stub highlight preservation {:+.2} EV, WB {:.1} K / {:+.2}",
        nef.display(),
        p.highlight_preservation,
        pw.temperature,
        pw.tint
    );
}
