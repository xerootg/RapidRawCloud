//! `rrcloud-worker` — the headless worker binary (ARCHITECTURE.md §6).
//!
//! DESIGN REFINEMENT vs §6 prose: this bin depends on **`rrcloud-core`
//! only** — not `rapidraw_lib`/tauri/gtk/webkit. Color parity with the app
//! is a property of `rrcloud-core::proxy` (same pinned rawler rev), so the
//! worker needs no app linkage. See `rrcloud_core::worker` module docs,
//! `docs/ARCHITECTURE.md` §6, and `docs/UPSTREAM_TOUCHES.md`.
//!
//! `main` is deliberately thin: it parses the CLI mode, resolves config
//! from the environment, opens the worker (which enforces the mandatory
//! state dir), and hands off to `worker::run`. All testable logic lives in
//! `worker::run_cycle`, which the integration suite drives directly.

use std::process::ExitCode;
use std::time::Duration;

use rrcloud_core::worker::{
    self, CycleOptions, RunMode, Worker, WorkerConfig, DEFAULT_DAEMON_INTERVAL,
};

#[tokio::main]
async fn main() -> ExitCode {
    match real_main().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("rrcloud-worker: fatal: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn real_main() -> Result<(), worker::WorkerError> {
    let mode = parse_mode().expect("usage: rrcloud-worker [--once | --daemon --interval <dur>]");
    let cfg = WorkerConfig::from_env()?;
    let worker = Worker::open(&cfg)?;
    worker::run(&worker, mode, &CycleOptions::default()).await
}

/// Parse `--once` (default) or `--daemon --interval <dur>` from argv.
/// `None` signals a usage error (unknown flag, `--interval` without a
/// parsable duration, or `--once` and `--daemon` together).
fn parse_mode() -> Option<RunMode> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut once = false;
    let mut daemon = false;
    let mut interval = DEFAULT_DAEMON_INTERVAL;
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--once" => once = true,
            "--daemon" => daemon = true,
            "--interval" => {
                i += 1;
                interval = parse_duration(args.get(i)?)?;
            }
            _ => return None,
        }
        i += 1;
    }
    if once && daemon {
        return None;
    }
    if daemon {
        Some(RunMode::Daemon { interval })
    } else {
        Some(RunMode::Once)
    }
}

/// Parse a human duration: a bare integer is seconds, or a `<n><unit>` with
/// unit `s`/`m`/`h`/`d` (e.g. `900s`, `15m`, `1h`). `None` on anything else.
fn parse_duration(s: &str) -> Option<Duration> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let (num, mult) = match s.as_bytes()[s.len() - 1] {
        b's' => (&s[..s.len() - 1], 1u64),
        b'm' => (&s[..s.len() - 1], 60),
        b'h' => (&s[..s.len() - 1], 3600),
        b'd' => (&s[..s.len() - 1], 86_400),
        _ => (s, 1),
    };
    let n: u64 = num.parse().ok()?;
    Some(Duration::from_secs(n.checked_mul(mult)?))
}
