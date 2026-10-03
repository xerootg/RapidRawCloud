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

const USAGE: &str = "usage: rrcloud-worker [--once | --daemon --interval <dur> | --report]";

/// What the CLI resolves argv into.
enum Invocation {
    /// Stateless read-only reporting (§6): a single cycle under
    /// [`Worker::open_readonly`], NO journaling. The one CLI path to the
    /// documented stateless capability — reachable without `RRCLOUD_STATE_DIR`.
    Report,
    /// The journaling role ([`Worker::open`]) driven by `mode`.
    Run(RunMode),
}

#[tokio::main]
async fn main() -> ExitCode {
    // A CLI usage error is a clean usage message + nonzero exit (code 2),
    // never a Rust panic/backtrace — it is operator input, not a bug.
    let Some(inv) = parse_invocation() else {
        eprintln!("{USAGE}");
        return ExitCode::from(2);
    };
    match real_main(inv).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("rrcloud-worker: fatal: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn real_main(inv: Invocation) -> Result<(), worker::WorkerError> {
    let cfg = WorkerConfig::from_env()?;
    match inv {
        // Stateless read-only reporting: open_readonly never requires (nor
        // uses) a persistent state dir and never journals, so `--report`
        // works with RRCLOUD_STATE_DIR unset — the one CLI path to §6's
        // "a stateless invocation may at most do read-only reporting".
        Invocation::Report => {
            let worker = Worker::open_readonly(&cfg)?;
            let report = worker::run_cycle(&worker, &CycleOptions::default()).await?;
            println!(
                "rrcloud-worker report: foreign_originals_seen={}",
                report.foreign_originals_seen
            );
            Ok(())
        }
        Invocation::Run(mode) => {
            let worker = Worker::open(&cfg)?;
            worker::run(&worker, mode, &CycleOptions::default()).await
        }
    }
}

/// Parse `--once` (default), `--daemon --interval <dur>`, or `--report` from
/// argv. `None` signals a usage error (unknown flag, `--interval` without a
/// parsable duration, or mutually-exclusive modes combined).
fn parse_invocation() -> Option<Invocation> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut once = false;
    let mut daemon = false;
    let mut report = false;
    let mut interval = DEFAULT_DAEMON_INTERVAL;
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--once" => once = true,
            "--daemon" => daemon = true,
            "--report" => report = true,
            "--interval" => {
                i += 1;
                interval = parse_duration(args.get(i)?)?;
            }
            _ => return None,
        }
        i += 1;
    }
    // --report is the stateless reporting mode; it cannot combine with the
    // journaling modes.
    if report && (daemon || once) {
        return None;
    }
    if report {
        return Some(Invocation::Report);
    }
    if once && daemon {
        return None;
    }
    if daemon {
        Some(Invocation::Run(RunMode::Daemon { interval }))
    } else {
        Some(Invocation::Run(RunMode::Once))
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
