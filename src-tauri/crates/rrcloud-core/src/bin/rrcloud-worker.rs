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

use rrcloud_core::fleet::{self, FleetConfig};
use rrcloud_core::worker::{
    self, CycleOptions, RunMode, Worker, WorkerConfig, DEFAULT_DAEMON_INTERVAL,
};

const USAGE: &str =
    "usage: rrcloud-worker [--once | --daemon --interval <dur> | --report | --fleet [--once | --daemon --interval <dur>]]";

/// What the CLI resolves argv into.
enum Invocation {
    /// Stateless read-only reporting (§6): a single cycle under
    /// [`Worker::open_readonly`], NO journaling. The one CLI path to the
    /// documented stateless capability — reachable without `RRCLOUD_STATE_DIR`.
    Report,
    /// The journaling role ([`Worker::open`]) driven by `mode`.
    Run(RunMode),
    /// Multi-user fleet backfill (optional pairing deployment): read the
    /// admin bucket and run one cycle per paired user, driven by `mode`.
    Fleet(RunMode),
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
    // Resolve the single-user WorkerConfig only on the paths that use it.
    // Fleet mode gets its per-user configs from the admin bucket's config
    // docs (FleetConfig::from_env), so requiring RRCLOUD_ENDPOINT here made
    // every real fleet deployment crash-loop on startup with
    // "missing required configuration: RRCLOUD_ENDPOINT".
    match inv {
        // Stateless read-only reporting: open_readonly never requires (nor
        // uses) a persistent state dir and never journals, so `--report`
        // works with RRCLOUD_STATE_DIR unset — the one CLI path to §6's
        // "a stateless invocation may at most do read-only reporting".
        Invocation::Report => {
            let cfg = WorkerConfig::from_env()?;
            let worker = Worker::open_readonly(&cfg)?;
            let report = worker::run_cycle(&worker, &CycleOptions::default()).await?;
            println!(
                "rrcloud-worker report: foreign_originals_seen={}",
                report.foreign_originals_seen
            );
            Ok(())
        }
        Invocation::Run(mode) => {
            let cfg = WorkerConfig::from_env()?;
            let worker = Worker::open(&cfg)?;
            worker::run(&worker, mode, &CycleOptions::default()).await
        }
        Invocation::Fleet(mode) => run_fleet(mode).await,
    }
}

/// Fleet mode: resolve the admin-bucket config from the environment, then run
/// one backfill cycle per paired user (once) or on an interval (daemon). A
/// single user's failure is logged, never fatal; the whole-cycle `Err`
/// (admin bucket unreachable) propagates so the caller/daemon can retry.
async fn run_fleet(mode: RunMode) -> Result<(), worker::WorkerError> {
    let fleet_cfg = FleetConfig::from_env()?;
    let opts = CycleOptions::default();
    match mode {
        RunMode::Once => {
            let report = fleet::run_fleet_cycle(&fleet_cfg, &opts).await?;
            log_fleet_report(&report);
            Ok(())
        }
        RunMode::Daemon { interval } => loop {
            match fleet::run_fleet_cycle(&fleet_cfg, &opts).await {
                Ok(report) => log_fleet_report(&report),
                // A whole-cycle failure (admin bucket unreachable) is logged,
                // not fatal: the daemon sleeps and retries next interval
                // rather than exiting, matching `worker::run`'s daemon loop.
                Err(e) => eprintln!("rrcloud-worker fleet: cycle failed, will retry: {e}"),
            }
            tokio::time::sleep(interval).await;
        },
    }
}

fn log_fleet_report(report: &fleet::FleetReport) {
    println!(
        "rrcloud-worker fleet: {} users ({} ran, {} failed)",
        report.users.len(),
        report.ran(),
        report.failed()
    );
    for (user, outcome) in &report.users {
        if let fleet::UserOutcome::Failed(why) = outcome {
            eprintln!("rrcloud-worker fleet: user {user}: {why}");
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
    let mut fleet = false;
    let mut interval = DEFAULT_DAEMON_INTERVAL;
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--once" => once = true,
            "--daemon" => daemon = true,
            "--report" => report = true,
            "--fleet" => fleet = true,
            "--interval" => {
                i += 1;
                interval = parse_duration(args.get(i)?)?;
            }
            _ => return None,
        }
        i += 1;
    }
    // --report is the stateless reporting mode; it cannot combine with the
    // journaling modes (including --fleet).
    if report && (daemon || once || fleet) {
        return None;
    }
    if report {
        return Some(Invocation::Report);
    }
    // --once and --daemon are mutually exclusive, with or without --fleet.
    if once && daemon {
        return None;
    }
    let mode = if daemon {
        RunMode::Daemon { interval }
    } else {
        RunMode::Once
    };
    if fleet {
        Some(Invocation::Fleet(mode))
    } else {
        Some(Invocation::Run(mode))
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
    let secs = n.checked_mul(mult)?;
    // Reject a zero interval: `--daemon --interval 0` would otherwise loop
    // with `sleep(0)`, hammering poll/list/S3 at ~100% CPU with no backoff
    // (P4 review round 2). A zero duration is an operator typo, so it is a
    // clean usage error (`None` → the CLI prints USAGE and exits 2), never a
    // busy-loop. Any positive value — down to `1s` — is accepted as-is.
    if secs == 0 {
        return None;
    }
    Some(Duration::from_secs(secs))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_duration_rejects_zero_so_the_daemon_cannot_busy_loop() {
        // Every spelling of "zero" is a usage error, not a Duration::ZERO that
        // would make `--daemon --interval 0` spin on `sleep(0)`.
        for z in ["0", "0s", "0m", "0h", "0d"] {
            assert!(
                parse_duration(z).is_none(),
                "a zero interval ({z:?}) must be rejected, not accepted as a busy-loop"
            );
        }
    }

    #[test]
    fn parse_duration_accepts_positive_values_down_to_one_second() {
        assert_eq!(parse_duration("1"), Some(Duration::from_secs(1)));
        assert_eq!(parse_duration("1s"), Some(Duration::from_secs(1)));
        assert_eq!(parse_duration("15m"), Some(Duration::from_secs(15 * 60)));
        assert_eq!(parse_duration("1h"), Some(Duration::from_secs(3600)));
        assert_eq!(parse_duration("2d"), Some(Duration::from_secs(2 * 86_400)));
    }

    #[test]
    fn parse_duration_rejects_garbage_and_overflow() {
        assert!(parse_duration("").is_none());
        assert!(parse_duration("abc").is_none());
        assert!(parse_duration("12x").is_none());
        // n * mult overflow → None (no panic).
        assert!(parse_duration("99999999999999999999d").is_none());
    }
}
