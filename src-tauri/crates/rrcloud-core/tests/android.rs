//! Failing tests for `rrcloud_core::android`'s host-testable pure logic
//! (ARCHITECTURE.md §5): the `budget_ms`-bounded cycle deadline (§5.1), the
//! JNI result-code <-> WorkManager-outcome mapping (§5.1), the §5.2
//! `dcim_seen` dedupe decision, and the §5.2 API 24-29 `DATE_ADDED`
//! overlap-window arithmetic.
//!
//! The actual JNI entry points in `rrcloud_core::android::bridge` are
//! compiled only under `target_os = "android"` and are deliberately **not**
//! exercised here — they need a live JVM and are proven by the Android
//! build gate plus on-device verification, not `cargo test` on the host.

use rrcloud_core::android::bounded_cycle::CycleBudget;
use rrcloud_core::android::dcim_dedupe::{decide, DcimDecision, SeenRow};
use rrcloud_core::android::result_code::{BridgeResult, WorkOutcome};
use rrcloud_core::android::scan_window::{effective_floor, OVERLAP_SECS};
use rrcloud_core::semhash::ContentId;

fn cid(byte: u8) -> ContentId {
    ContentId::from_bytes(&[byte; 8])
}

// ---------------------------------------------------------------------------
// bounded_cycle::CycleBudget (§5.1 point 3: deadline checked between work
// units, never mid-item)
// ---------------------------------------------------------------------------

#[test]
fn cycle_budget_not_expired_before_deadline() {
    let budget = CycleBudget::from_budget_ms(1_000, 5_000);
    assert!(!budget.is_expired(1_000));
    assert!(!budget.is_expired(5_999));
}

#[test]
fn cycle_budget_expired_at_exact_deadline() {
    // The boundary instant itself has no time left for another work unit:
    // `now_ms == deadline_ms` must already read as expired (inclusive), so
    // the caller never starts one more unit believing it has 0ms to spare.
    let budget = CycleBudget::from_budget_ms(1_000, 5_000);
    assert!(budget.is_expired(6_000));
}

#[test]
fn cycle_budget_expired_after_deadline() {
    let budget = CycleBudget::from_budget_ms(1_000, 5_000);
    assert!(budget.is_expired(100_000));
}

#[test]
fn cycle_budget_zero_budget_is_already_expired() {
    let budget = CycleBudget::from_budget_ms(1_000, 0);
    assert!(budget.is_expired(1_000));
}

#[test]
fn cycle_budget_negative_budget_is_already_expired() {
    // WorkManager handing the bridge a budget that is already spent (or a
    // corrupt negative value) must never be treated as "unbounded".
    let budget = CycleBudget::from_budget_ms(1_000, -500);
    assert!(budget.is_expired(1_000));
    assert!(budget.is_expired(0));
}

#[test]
fn cycle_budget_saturates_on_overflow_rather_than_wrapping() {
    // start_ms + budget_ms must never wrap around to a deadline in the
    // past; it saturates to i64::MAX, which never expires within any
    // realistic now_ms.
    let budget = CycleBudget::from_budget_ms(i64::MAX - 10, 1_000);
    assert!(!budget.is_expired(i64::MAX - 1));
}

#[test]
fn cycle_budget_remaining_ms_counts_down_to_zero() {
    let budget = CycleBudget::from_budget_ms(1_000, 5_000);
    assert_eq!(budget.remaining_ms(1_000), 5_000);
    assert_eq!(budget.remaining_ms(4_000), 2_000);
    assert_eq!(budget.remaining_ms(6_000), 0);
}

#[test]
fn cycle_budget_remaining_ms_never_negative_past_deadline() {
    let budget = CycleBudget::from_budget_ms(1_000, 5_000);
    assert_eq!(budget.remaining_ms(1_000_000), 0);
}

#[test]
fn cycle_budget_remaining_ms_zero_when_already_expired_by_construction() {
    let budget = CycleBudget::from_budget_ms(1_000, -500);
    assert_eq!(budget.remaining_ms(1_000), 0);
}

// ---------------------------------------------------------------------------
// result_code::BridgeResult (§5.1: the jint <-> WorkManager Result mapping)
// ---------------------------------------------------------------------------

#[test]
fn result_code_round_trips_every_variant() {
    for variant in BridgeResult::ALL {
        let code = variant.to_code();
        assert_eq!(
            BridgeResult::from_code(code),
            Some(variant),
            "round trip failed for {variant:?} (code {code})"
        );
    }
}

#[test]
fn result_code_assigns_distinct_codes_to_every_variant() {
    let codes: Vec<i32> = BridgeResult::ALL.iter().map(|v| v.to_code()).collect();
    let mut sorted = codes.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(
        sorted.len(),
        codes.len(),
        "two BridgeResult variants share a jint code: {codes:?}"
    );
}

#[test]
fn result_code_rejects_unknown_codes() {
    // Codes above the exhaustively pinned table must decode to None, never
    // silently default to some variant — an unrecognized code reaching the
    // Kotlin side is a protocol bug, not a value to guess at.
    assert_eq!(BridgeResult::from_code(999), None);
    assert_eq!(BridgeResult::from_code(-1), None);
}

#[test]
fn result_code_success_maps_to_work_success() {
    assert_eq!(BridgeResult::Success.work_outcome(), WorkOutcome::Success);
}

#[test]
fn result_code_retry_variants_map_to_work_retry() {
    assert_eq!(
        BridgeResult::RetryTransient.work_outcome(),
        WorkOutcome::Retry
    );
    assert_eq!(
        BridgeResult::RetryLockHeld.work_outcome(),
        WorkOutcome::Retry
    );
}

#[test]
fn result_code_failure_variants_map_to_work_failure() {
    assert_eq!(
        BridgeResult::FailureNotConfigured.work_outcome(),
        WorkOutcome::Failure
    );
    assert_eq!(
        BridgeResult::FailurePermanent.work_outcome(),
        WorkOutcome::Failure
    );
}

#[test]
fn result_code_work_outcome_is_exhaustively_covered_by_all() {
    // Every WorkOutcome variant must be reachable from at least one
    // BridgeResult — a mapping that silently never produces e.g. Failure
    // would hide a dead branch in the Kotlin caller.
    let outcomes: std::collections::HashSet<WorkOutcome> =
        BridgeResult::ALL.iter().map(|v| v.work_outcome()).collect();
    assert!(outcomes.contains(&WorkOutcome::Success));
    assert!(outcomes.contains(&WorkOutcome::Retry));
    assert!(outcomes.contains(&WorkOutcome::Failure));
}

// ---------------------------------------------------------------------------
// dcim_dedupe::decide (§5.2: skip/rehash/new over the existing dcim_seen
// table shape)
// ---------------------------------------------------------------------------

#[test]
fn dcim_dedupe_unseen_path_is_new() {
    assert_eq!(decide(None, 12_345, 1_700_000_000), DcimDecision::New);
}

#[test]
fn dcim_dedupe_exact_match_is_skip() {
    let row = SeenRow {
        size: 12_345,
        mtime_unix: 1_700_000_000,
        content_id: cid(0xAB),
    };
    assert_eq!(
        decide(Some(&row), 12_345, 1_700_000_000),
        DcimDecision::Skip {
            content_id: cid(0xAB)
        }
    );
}

#[test]
fn dcim_dedupe_size_changed_is_rehash() {
    let row = SeenRow {
        size: 12_345,
        mtime_unix: 1_700_000_000,
        content_id: cid(0xAB),
    };
    // Same mtime, different size.
    assert_eq!(
        decide(Some(&row), 99_999, 1_700_000_000),
        DcimDecision::Rehash
    );
}

#[test]
fn dcim_dedupe_mtime_changed_is_rehash() {
    let row = SeenRow {
        size: 12_345,
        mtime_unix: 1_700_000_000,
        content_id: cid(0xAB),
    };
    // Same size, different mtime (e.g. a touch).
    assert_eq!(
        decide(Some(&row), 12_345, 1_700_000_999),
        DcimDecision::Rehash
    );
}

#[test]
fn dcim_dedupe_both_changed_is_rehash() {
    let row = SeenRow {
        size: 12_345,
        mtime_unix: 1_700_000_000,
        content_id: cid(0xAB),
    };
    assert_eq!(decide(Some(&row), 1, 1), DcimDecision::Rehash);
}

// ---------------------------------------------------------------------------
// scan_window::effective_floor (§5.2 API 24-29 48h overlap arithmetic)
// ---------------------------------------------------------------------------

#[test]
fn scan_window_first_scan_has_no_cursor_scans_everything() {
    assert_eq!(effective_floor(None, 1_700_000_000), 0);
}

#[test]
fn scan_window_common_case_subtracts_overlap() {
    let cursor = 1_700_000_000_i64;
    let now = cursor + 3_600; // an hour after the cursor was recorded
    assert_eq!(effective_floor(Some(cursor), now), cursor - OVERLAP_SECS);
}

#[test]
fn scan_window_overlap_constant_is_48_hours() {
    assert_eq!(OVERLAP_SECS, 48 * 60 * 60);
}

#[test]
fn scan_window_near_epoch_cursor_clamps_to_zero_not_negative() {
    // cursor - OVERLAP_SECS would be negative here; DATE_ADDED has no
    // meaningful negative bound.
    let cursor = OVERLAP_SECS - 1;
    assert_eq!(effective_floor(Some(cursor), cursor + 10), 0);
}

#[test]
fn scan_window_cursor_exactly_at_overlap_boundary_is_zero() {
    let cursor = OVERLAP_SECS;
    assert_eq!(effective_floor(Some(cursor), cursor + 10), 0);
}

#[test]
fn scan_window_cursor_one_second_past_overlap_boundary() {
    let cursor = OVERLAP_SECS + 1;
    assert_eq!(effective_floor(Some(cursor), cursor + 10), 1);
}

#[test]
fn scan_window_future_cursor_clamps_to_now_not_inverted() {
    // A cursor from the future (clock stepped backward, or a corrupt
    // persisted value) must never produce a floor exceeding `now` — that
    // would invert the scan window (floor > now) and match nothing while
    // silently "succeeding".
    let now = 1_700_000_000_i64;
    let corrupt_future_cursor = now + 1_000_000;
    let floor = effective_floor(Some(corrupt_future_cursor), now);
    assert!(floor <= now, "floor {floor} must not exceed now {now}");
}
