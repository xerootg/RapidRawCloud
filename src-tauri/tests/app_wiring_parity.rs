//! Upstream-parity test for `--no-default-features` (ARCHITECTURE.md §7).
//!
//! The whole file compiles only when the `sync` feature is OFF, so it runs
//! under `cargo test --no-default-features` and is an empty crate
//! otherwise. It pins the fork's central promise: with sync off, the
//! `save_sidecar` chokepoint behaves exactly like upstream's old
//! `fs::write(path, serde_json::to_string_pretty(&meta))` — same bytes,
//! same path — and the always-compiled hook shims are inert no-ops.

#![cfg(not(feature = "sync"))]

use rapidraw_lib::sync::{ImageMetadata, WriteOrigin, hooks, save_sidecar};

fn meta() -> ImageMetadata {
    ImageMetadata {
        version: 1,
        rating: 4,
        adjustments: serde_json::json!({ "exposure": 0.25, "contrast": 10 }),
        tags: Some(vec!["user:keep".to_string()]),
        exif: None,
    }
}

#[test]
fn sync_off_chokepoint_is_byte_identical_to_upstream_fs_write() {
    let dir = tempfile::tempdir().expect("tempdir");
    let sidecar = dir.path().join("img.NEF.rrdata");
    let m = meta();

    save_sidecar(None, &sidecar, &m, WriteOrigin::User).expect("save_sidecar (sync off)");

    let written = std::fs::read(&sidecar).expect("sidecar present");
    // Exactly upstream's old write: the pretty-printed JSON, no trailing
    // newline, at the same path.
    let expected = serde_json::to_string_pretty(&m)
        .expect("serialize")
        .into_bytes();
    assert_eq!(
        written, expected,
        "with sync off, save_sidecar must write the same bytes as the old fs::write"
    );
    assert_eq!(
        sidecar,
        dir.path().join("img.NEF.rrdata"),
        "the sidecar path must be unchanged"
    );

    // ...and the same *permissions*. Upstream's old `fs::write` created a new
    // sidecar at `0o666 & !umask`; the chokepoint's temp+rename must not
    // tighten that to tempfile's 0600. Compare against a real `fs::write`
    // reference so the assertion holds under any umask (ARCHITECTURE.md §7).
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let reference = dir.path().join("reference.NEF.rrdata");
        std::fs::write(&reference, &expected).expect("fs::write reference");
        let got = std::fs::metadata(&sidecar)
            .expect("stat sidecar")
            .permissions()
            .mode()
            & 0o777;
        let want = std::fs::metadata(&reference)
            .expect("stat reference")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(
            got, want,
            "with sync off, save_sidecar must create the sidecar with the \
             same mode as fs::write (want {want:o}, got {got:o})"
        );
    }
}

#[test]
fn sync_off_hooks_are_inert_noops() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("photo.NEF");

    // is_stub is always false upstream; ensure_local is an identity pass;
    // the notify_* shims do nothing and never panic.
    assert!(!hooks::is_stub(&path));
    assert_eq!(
        hooks::ensure_local(&path, "parity").expect("ensure_local"),
        path
    );
    hooks::notify_new_original(&path);
    hooks::notify_deleted(&path);
    hooks::notify_moved(&path, &path);
    hooks::sync_flush_path(&path);
}
