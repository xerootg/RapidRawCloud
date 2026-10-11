//! The process-wide "one sync cycle at a time" lock (ARCHITECTURE.md §5.1,
//! in-process half).
//!
//! Two cycle engines can run inside one app process: the host app's own
//! `SyncManager` cycle (the webview-driven foreground driver) and, on
//! Android, this crate's bridge cycle (`android::bridge::runSyncCycle`,
//! driven by WorkManager, which may execute its `Worker` in-process). They
//! share one redb, whose write transactions are serialized — but a cycle is
//! many transactions plus filesystem writes, and the §3.5 stub pass of one
//! cycle can interleave with the download pump of the other into a record
//! that says `Stub` over a file holding the full verified bytes (observed on
//! a device as a fully-local photo wearing the cloud-off badge). Both
//! engines take this lock for the whole cycle, so within a process at most
//! one cycle is ever in flight. A contender backs off immediately rather
//! than blocking: WorkManager already owns the retry/backoff policy, and the
//! foreground driver simply tries again on its next tick.
//!
//! Cross-process exclusion is redb's own file lock
//! ([`crate::state::StateError::AlreadyLocked`]); this lock is the
//! same-process complement. The on-demand hydrate path (`ensure_local` at
//! the open/edit guard sites) deliberately does NOT take it — it is
//! user-driven and already serialized per item by `download_item`'s
//! transition CAS.

use std::sync::{Mutex, MutexGuard, TryLockError};

static CYCLE_LOCK: Mutex<()> = Mutex::new(());

/// Held by the one running sync cycle in this process; dropping it releases
/// the lock.
#[must_use = "the cycle lock is released as soon as the guard is dropped"]
pub struct CycleGuard(#[allow(dead_code)] MutexGuard<'static, ()>);

impl std::fmt::Debug for CycleGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CycleGuard")
    }
}

/// Tries to become the one running sync cycle in this process. `None` when
/// another cycle currently holds the lock (the caller should back off and
/// retry later, not block).
///
/// A poisoned lock (a cycle panicked while holding it) is recovered: the
/// mutex guards no data of its own, only the "a cycle is running" fact, and
/// that cycle is certainly not running any more.
pub fn try_lock_cycle() -> Option<CycleGuard> {
    match CYCLE_LOCK.try_lock() {
        Ok(guard) => Some(CycleGuard(guard)),
        Err(TryLockError::WouldBlock) => None,
        Err(TryLockError::Poisoned(poisoned)) => Some(CycleGuard(poisoned.into_inner())),
    }
}

#[cfg(test)]
mod tests {
    use super::try_lock_cycle;

    // One test, run as sequential phases: the lock is process-global, so two
    // `#[test]`s touching it would race each other on the test harness's
    // threads.
    #[test]
    fn one_cycle_at_a_time_released_on_drop_and_recovered_from_poison() {
        let first = try_lock_cycle().expect("free lock");
        assert!(
            try_lock_cycle().is_none(),
            "a second cycle must not start while one is running"
        );
        drop(first);
        let again = try_lock_cycle().expect("released on drop");
        drop(again);

        let result = std::panic::catch_unwind(|| {
            let _guard = try_lock_cycle().expect("free lock");
            panic!("cycle blew up while holding the lock");
        });
        assert!(result.is_err());
        assert!(
            try_lock_cycle().is_some(),
            "poison is recovered: the next cycle can run"
        );
    }
}
