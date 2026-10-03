//! The §5.2 API 24-29 `DATE_ADDED` overlap-window arithmetic.
//!
//! On API 30+, `DcimScanWorker` uses `MediaStore.getGeneration()` /
//! `GENERATION_ADDED` — a monotonic cursor with no arithmetic worth
//! pulling out here. On API 24-29 (no generation counter), the scan
//! instead re-queries every row with `DATE_ADDED > floor`, where `floor`
//! is the last cursor **minus a 48 h overlap** — immune to a bulk
//! import/clone/restore backdating `DATE_ADDED` into the just-passed
//! window, at the cost of re-examining up to 48 h of rows the scanner
//! already knows about (made free by the [`super::dcim_dedupe`] skip
//! path).

/// The API 24-29 re-scan overlap, in seconds (§5.2: "a 48 h overlap window
/// re-scan").
pub const OVERLAP_SECS: i64 = 48 * 60 * 60;

/// The effective `DATE_ADDED` floor to re-query from, given the last
/// persisted cursor (`None` on a device's first-ever scan) and the current
/// wall-clock unix time.
///
/// Contract pinned by the test suite:
/// - `cursor_unix: None` (first scan ever) => `0`: scan the entire
///   `MediaStore` rather than apply an overlap to a cursor that does not
///   exist yet.
/// - `cursor_unix: Some(c)` with `c > OVERLAP_SECS` => `c - OVERLAP_SECS`
///   exactly (the common case).
/// - `cursor_unix: Some(c)` with `c <= OVERLAP_SECS` (a cursor within 48 h
///   of the epoch — realistically only ever seen in tests) => `0`, never a
///   negative floor (`DATE_ADDED` is an unsigned-semantics unix-seconds
///   column; a negative query bound is nonsensical, not merely
///   "technically correct").
/// - `cursor_unix: Some(c)` with `c > now_unix` (a cursor from the future —
///   device clock stepped backward after the cursor was recorded, or a
///   corrupt persisted value) => the floor is clamped to **never exceed
///   `now_unix`**, so a corrupt forward cursor cannot invert the scan
///   window into one that matches nothing.
pub fn effective_floor(cursor_unix: Option<i64>, now_unix: i64) -> i64 {
    let _ = (cursor_unix, now_unix);
    todo!("P5 green: None => 0; Some(c) => (c - OVERLAP_SECS).max(0).min(now_unix.max(0))")
}
