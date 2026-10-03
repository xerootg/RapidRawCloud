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

use rrcloud_core::worker::{self, CycleOptions, RunMode, Worker, WorkerConfig};

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
fn parse_mode() -> Option<RunMode> {
    let _default_interval = Duration::from_secs(15 * 60);
    todo!("P4 green: parse --once / --daemon --interval <dur>")
}
