//! §2.9 parity: with the `sync` feature OFF, the albums/presets save-site
//! hooks are inert — no `rr://` rewrite, no engine call, the local JSON is
//! left exactly as upstream wrote it. This binary is compiled and run only
//! under `--no-default-features`; the `sync` build's behavior is pinned by
//! `albums_presets_sync.rs` instead.

#![cfg(not(feature = "sync"))]

use rapidraw_lib::sync::hooks;

#[test]
fn meta_save_hooks_are_inert_without_sync() {
    let dir = tempfile::tempdir().expect("tempdir");

    let albums = dir.path().join("albums.json");
    let albums_bytes =
        br#"[{"type":"album","id":"a","name":"n","icon":null,"images":["/abs/x.jpg"]}]"#;
    std::fs::write(&albums, albums_bytes).expect("write albums");

    let presets = dir.path().join("presets.json");
    let presets_bytes =
        br#"[{"preset":{"id":"p","name":"n","adjustments":{"lutPath":"/abs/l.cube"}}}]"#;
    std::fs::write(&presets, presets_bytes).expect("write presets");

    // Must not panic and must not touch the files when sync is compiled out.
    hooks::notify_albums_saved(&albums);
    hooks::notify_presets_saved(&presets);

    assert_eq!(
        std::fs::read(&albums).expect("reread albums"),
        &albums_bytes[..],
        "sync-off albums hook must not rewrite the local document"
    );
    assert_eq!(
        std::fs::read(&presets).expect("reread presets"),
        &presets_bytes[..],
        "sync-off presets hook must not rewrite the local document"
    );
}
