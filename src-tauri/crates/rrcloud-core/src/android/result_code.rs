//! The bridge's `jint` result code and its mapping to a WorkManager
//! `ListenableWorker.Result` (ARCHITECTURE.md §5.1).
//!
//! `Java_…_RrcloudBridge_runSyncCycle` returns a plain `jint` across the
//! JNI boundary (JNI has no enum type), so the code<->meaning mapping must
//! be pinned and exhaustive on **both** sides: this module is the single
//! Rust-side source of truth the bridge consults before returning, and the
//! Kotlin `SyncCycleWorker`/`FgsSyncService` sides must match it exactly
//! (documented in the Kotlin bridge file as the same table).
//!
//! | code | [`BridgeResult`] | meaning | [`WorkOutcome`] |
//! |---|---|---|---|
//! | 0 | `Success` | cycle reached quiescence (or ran out of work) inside the budget | `Success` |
//! | 1 | `RetryTransient` | a transient failure (network/backend) interrupted the cycle — safe to retry | `Retry` |
//! | 2 | `RetryLockHeld` | redb's file lock (or the in-process `Mutex`) is held by a concurrent cycle — back off | `Retry` |
//! | 3 | `FailureNotConfigured` | no credentials / sync not configured on this device | `Failure` |
//! | 4 | `FailurePermanent` | a non-retryable engine error (corrupt state, protocol violation) | `Failure` |
//!
//! `Retry` and `Failure` are deliberately split into two codes each
//! (transient-vs-lock, not-configured-vs-permanent) even though both
//! halves of each pair map to the same [`WorkOutcome`]: the Kotlin side
//! logs/surfaces them differently (a lock-held retry is routine and
//! silent; a not-configured failure is worth telling the user about via
//! the settings UI), so the distinction must survive the JNI boundary even
//! though WorkManager itself only sees three outcomes.

/// The bridge's `jint` return value, decoded. See the module table for the
/// full contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BridgeResult {
    Success,
    RetryTransient,
    RetryLockHeld,
    FailureNotConfigured,
    FailurePermanent,
}

/// What a Kotlin `Worker.doWork()` should return for a given
/// [`BridgeResult`] — `ListenableWorker.Result.success()` / `.retry()` /
/// `.failure()`, named here rather than imported from `androidx.work`
/// (this crate has no Android/Kotlin dependency) so the mapping is
/// pinned and host-testable independent of the Kotlin toolchain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WorkOutcome {
    Success,
    Retry,
    Failure,
}

impl BridgeResult {
    /// Every variant, for exhaustive-coverage tests (`BridgeResult::ALL`
    /// style) without hand-duplicating the list and risking it drifting
    /// from the enum as codes are added.
    pub const ALL: [BridgeResult; 5] = [
        BridgeResult::Success,
        BridgeResult::RetryTransient,
        BridgeResult::RetryLockHeld,
        BridgeResult::FailureNotConfigured,
        BridgeResult::FailurePermanent,
    ];

    /// Decodes a bridge `jint` (passed as a plain `i32` here — the `jint`
    /// newtype is a target-gated alias with no host-testable surface of
    /// its own). `None` for any code outside the pinned table: the Kotlin
    /// side must never receive a code this function cannot name, so an
    /// unrecognized value is a decode failure, not a silently-defaulted
    /// outcome.
    pub fn from_code(code: i32) -> Option<Self> {
        match code {
            0 => Some(BridgeResult::Success),
            1 => Some(BridgeResult::RetryTransient),
            2 => Some(BridgeResult::RetryLockHeld),
            3 => Some(BridgeResult::FailureNotConfigured),
            4 => Some(BridgeResult::FailurePermanent),
            _ => None,
        }
    }

    /// Encodes `self` back to the `jint` the bridge returns. Round-trips
    /// with [`BridgeResult::from_code`] for every variant (pinned by the
    /// exhaustive test).
    pub fn to_code(self) -> i32 {
        match self {
            BridgeResult::Success => 0,
            BridgeResult::RetryTransient => 1,
            BridgeResult::RetryLockHeld => 2,
            BridgeResult::FailureNotConfigured => 3,
            BridgeResult::FailurePermanent => 4,
        }
    }

    /// The WorkManager outcome this code maps to (see the module table).
    pub fn work_outcome(self) -> WorkOutcome {
        match self {
            BridgeResult::Success => WorkOutcome::Success,
            BridgeResult::RetryTransient | BridgeResult::RetryLockHeld => WorkOutcome::Retry,
            BridgeResult::FailureNotConfigured | BridgeResult::FailurePermanent => {
                WorkOutcome::Failure
            }
        }
    }
}
