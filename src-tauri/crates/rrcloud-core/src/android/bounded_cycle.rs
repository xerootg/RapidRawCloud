//! The `budget_ms`-bounded sync cycle deadline (ARCHITECTURE.md §5.1 point
//! 3): `SyncCycleWorker.doWork()` and the headless-worker-equivalent
//! bridge call hand the bridge a millisecond budget; the cycle checks a
//! [`CycleBudget`] **between** discrete units of work (upload one item,
//! apply one journal page, generate one proxy — never mid-item, so a
//! popped-but-not-finished transfer is never abandoned half-written) and
//! stops admitting new units once it is expired, letting redb resume carry
//! the rest into the next window.
//!
//! Deliberately not built on [`std::time::Instant`]: `Instant` has no
//! public constructor other than `now()` and cannot be rewound or forged,
//! so a deadline built from it cannot be driven through its boundary
//! conditions from a host test without a real sleep. Every function here
//! instead takes the caller's own monotonic millisecond reading (the
//! bridge's real caller converts `Instant`/`SystemTime` to millis once at
//! the boundary) — fully unit-testable, and the same shape
//! `CancelFlag`-style cooperative check already used by
//! [`crate::transfer::pump_uploads`]/`pump_downloads` (this is the
//! `budget_ms` sibling of that cancellation primitive, not a replacement
//! for it: the bridge's bounded cycle fires the existing `CancelFlag` once
//! [`CycleBudget::is_expired`] turns true, rather than inventing a second
//! cancellation channel).

/// A deadline expiring `budget_ms` milliseconds after a caller-supplied
/// monotonic start reading.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CycleBudget {
    /// The absolute monotonic-millis instant at which the cycle must stop
    /// admitting new work units. `None` encodes "already expired, by
    /// construction" (a non-positive `budget_ms`) without a sentinel value
    /// that could collide with a real deadline.
    deadline_ms: Option<i64>,
}

impl CycleBudget {
    /// Builds a budget expiring `budget_ms` after `start_ms` (both in the
    /// same monotonic millisecond unit; the bridge's only caller converts
    /// `Instant::now()` once at cycle start).
    ///
    /// A non-positive `budget_ms` (WorkManager handing the bridge zero or
    /// negative time left in its execution window) yields a budget that is
    /// **already expired** — [`CycleBudget::is_expired`] is `true` for
    /// every `now_ms` — rather than panicking or silently treating it as
    /// "unbounded": a bounded-cycle entry point must never run an
    /// unbounded cycle just because its caller mis-measured the remaining
    /// window.
    ///
    /// `start_ms.saturating_add(budget_ms)` never panics on overflow (a
    /// hostile or corrupt `budget_ms` saturates to `i64::MAX`, which simply
    /// never expires within any realistic test or process lifetime, rather
    /// than wrapping to a deadline in the past).
    pub fn from_budget_ms(start_ms: i64, budget_ms: i64) -> Self {
        if budget_ms <= 0 {
            CycleBudget { deadline_ms: None }
        } else {
            CycleBudget {
                deadline_ms: Some(start_ms.saturating_add(budget_ms)),
            }
        }
    }

    /// Whether the cycle must stop admitting new work units as of `now_ms`
    /// (`now_ms >= deadline`, inclusive — the boundary instant itself has
    /// no time left for another unit). Callers check this **between** work
    /// units only; a unit already popped/in-flight always runs to
    /// completion (mirroring [`crate::transfer::CancelFlag`]'s contract).
    pub fn is_expired(&self, now_ms: i64) -> bool {
        match self.deadline_ms {
            Some(deadline) => now_ms >= deadline,
            None => true,
        }
    }

    /// Milliseconds remaining as of `now_ms`; `0` once expired, never
    /// negative (so a caller can use it directly as e.g. a per-item
    /// timeout without an extra clamp).
    pub fn remaining_ms(&self, now_ms: i64) -> i64 {
        match self.deadline_ms {
            Some(deadline) => deadline.saturating_sub(now_ms).max(0),
            None => 0,
        }
    }
}
