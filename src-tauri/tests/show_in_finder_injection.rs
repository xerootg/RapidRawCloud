//! Regression tests for `file_management::show_in_finder` on Linux.
//!
//! The command is reachable from the webview with an arbitrary string. On
//! Linux it does `xdg-open <path.parent()>` with no validation, so:
//!   * `"http://evil.example/x"` has the parent `"http://evil.example"`, and
//!     xdg-open hands a URL to the default browser;
//!   * a path whose parent is a regular FILE (for example a file written by
//!     `save_temp_file`) makes xdg-open open that file with the system
//!     handler instead of revealing it in a file manager.
//!
//! The tests put a fake `xdg-open` (a shell script that logs its argv) at
//! the front of `PATH` and check what the real code hands to it.
//!
//! `PATH` is changed with `std::env::set_var`, which is process-global. This
//! is acceptable here because the file is a dedicated test binary: the
//! variable is set once, before any child process is spawned, and the three
//! tests are serialized behind one mutex so no test reads the log while
//! another one is still spawning.

#![cfg(target_os = "linux")]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use rapidraw_lib::file_management::show_in_finder;

/// The fake `xdg-open` and the log it appends to. Created once per test
/// binary; the directory is kept alive for the whole run.
struct FakeXdgOpen {
    _dir: tempfile::TempDir,
    log: PathBuf,
}

fn fake_xdg_open() -> &'static FakeXdgOpen {
    static FAKE: OnceLock<FakeXdgOpen> = OnceLock::new();
    FAKE.get_or_init(|| {
        let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).expect("fake bin dir");
        let log = dir.path().join("xdg-open.log");
        // One line per invocation: `CALL\t<arg1>\t<arg2>...`.
        let script = format!(
            "#!/bin/sh\n{{ printf 'CALL'; for a in \"$@\"; do printf '\\t%s' \"$a\"; done; \
             printf '\\n'; }} >> '{}'\n",
            log.display()
        );
        let script_path = dir.path().join("xdg-open");
        fs::write(&script_path, script).expect("write fake xdg-open");
        fs::set_permissions(&script_path, fs::Permissions::from_mode(0o755))
            .expect("chmod fake xdg-open");

        let old = std::env::var_os("PATH").unwrap_or_default();
        let mut paths = vec![dir.path().to_path_buf()];
        paths.extend(std::env::split_paths(&old));
        let new = std::env::join_paths(paths).expect("join PATH");
        // SAFETY: process-global mutation of the environment. This binary
        // owns its process, the call happens once (inside the OnceLock
        // initializer, under `serial()`), and no other thread of this test
        // binary reads the environment concurrently.
        unsafe { std::env::set_var("PATH", new) };

        FakeXdgOpen { _dir: dir, log }
    })
}

/// Serializes the tests: they share one fake binary and one log file.
fn serial() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

/// Every recorded `xdg-open` invocation, as its argv (program name excluded).
fn invocations(log: &Path) -> Vec<Vec<String>> {
    fs::read_to_string(log)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| {
            let mut parts = line.split('\t');
            (parts.next() == Some("CALL")).then(|| parts.map(str::to_string).collect())
        })
        .collect()
}

/// The real code uses `Command::spawn` and never waits, so the script may
/// still be running when `show_in_finder` returns. Poll the log for up to
/// one second; return as soon as `done` is satisfied.
fn wait_for_log(log: &Path, done: impl Fn(&[Vec<String>]) -> bool) -> Vec<Vec<String>> {
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        let calls = invocations(log);
        if done(&calls) || Instant::now() >= deadline {
            return calls;
        }
        thread::sleep(Duration::from_millis(20));
    }
}

fn reset_log(log: &Path) {
    fs::write(log, "").expect("truncate xdg-open log");
}

fn same_path(a: &str, b: &Path) -> bool {
    match (fs::canonicalize(a), fs::canonicalize(b)) {
        (Ok(x), Ok(y)) => x == y,
        _ => Path::new(a) == b,
    }
}

#[test]
fn url_string_is_rejected_and_never_reaches_xdg_open() {
    let _guard = serial();
    let fake = fake_xdg_open();
    reset_log(&fake.log);

    let result = show_in_finder("http://evil.example/x".into());

    let calls = wait_for_log(&fake.log, |c| !c.is_empty());
    let url_calls: Vec<_> = calls
        .iter()
        .filter(|argv| argv.iter().any(|a| a.starts_with("http://")))
        .collect();
    assert!(
        url_calls.is_empty(),
        "xdg-open was invoked with an attacker URL: {url_calls:?}"
    );
    assert!(
        result.is_err(),
        "show_in_finder accepted a URL string: {result:?}"
    );
}

#[test]
fn path_whose_parent_is_a_regular_file_is_rejected() {
    let _guard = serial();
    let fake = fake_xdg_open();
    reset_log(&fake.log);

    let tmp = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).expect("tempdir");
    let not_a_dir = tmp.path().join("notadir");
    fs::write(&not_a_dir, b"<html><script>alert(1)</script></html>").expect("write file");
    let requested = not_a_dir.join("x");

    let result = show_in_finder(requested.to_string_lossy().into_owned());

    let calls = wait_for_log(&fake.log, |c| !c.is_empty());
    let file_calls: Vec<_> = calls
        .iter()
        .filter(|argv| argv.iter().any(|a| same_path(a, &not_a_dir)))
        .collect();
    assert!(
        file_calls.is_empty(),
        "xdg-open was invoked with a regular file as the \"parent directory\": {file_calls:?}"
    );
    assert!(
        result.is_err(),
        "show_in_finder accepted a path whose parent is a regular file: {result:?}"
    );
}

#[test]
fn existing_file_reveals_its_parent_directory_exactly_once() {
    let _guard = serial();
    let fake = fake_xdg_open();
    reset_log(&fake.log);

    let tmp = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).expect("tempdir");
    let dir = tmp.path().join("photos");
    fs::create_dir(&dir).expect("create dir");
    let photo = dir.join("IMG_0001.jpg");
    fs::write(&photo, b"not really a jpeg").expect("write photo");

    let result = show_in_finder(photo.to_string_lossy().into_owned());
    assert!(
        result.is_ok(),
        "show_in_finder rejected a real file: {result:?}"
    );

    let calls = wait_for_log(&fake.log, |c| !c.is_empty());
    assert_eq!(
        calls.len(),
        1,
        "expected exactly one xdg-open invocation, got {calls:?}"
    );
    let argv = &calls[0];
    assert_eq!(argv.len(), 1, "expected a single argument, got {argv:?}");
    assert!(
        same_path(&argv[0], &dir),
        "expected xdg-open to receive the parent directory {}, got {argv:?}",
        dir.display()
    );
}
