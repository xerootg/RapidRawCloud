//! The §5.2 DCIM-scan dedupe decision: given what the existing
//! `dcim_seen` table already knows about a source path (see
//! [`crate::state::SyncDb::dcim_seen`]/`set_dcim_seen`/`remove_dcim_seen`
//! — **reused as-is**, not redesigned here) and a freshly-observed
//! `(path, size, mtime)` candidate from a `MediaStore` cursor row, decide
//! whether the scanner can skip the file untouched, must re-hash it before
//! deciding, or has never seen it before.
//!
//! Deliberately decoupled from redb: [`decide`] takes the previously
//! recorded row for this path (or its absence) as a plain value, so the
//! decision table is host-testable without opening a database. The
//! intended caller shape is one row-per-path fetch (e.g.
//! `SyncDb::dcim_seen` queried, or a batch pre-fetch of every watched
//! path's last-known row) compared against each freshly observed
//! `(size, mtime)` candidate from the `MediaStore` cursor.

use crate::semhash::ContentId;

/// A previously-recorded `dcim_seen` row for one source path: the same
/// `(size, mtime_unix, content_id)` triple `SyncDb::dcim_seen` returns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeenRow {
    pub size: u64,
    pub mtime_unix: i64,
    pub content_id: ContentId,
}

/// What the scanner should do with one candidate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DcimDecision {
    /// `row`'s `(size, mtime)` exactly matches the candidate's: the file is
    /// byte-identical to what was already imported. Skip without opening
    /// it, reusing `row.content_id`.
    Skip { content_id: ContentId },
    /// A row exists for this path, but at a different `size` and/or
    /// `mtime` than the candidate. Re-hash before deciding: the content
    /// may still be identical (e.g. a pure `touch` bumping only mtime),
    /// or genuinely new bytes.
    Rehash,
    /// No row for this path at all: a candidate `dcim_seen` has never
    /// scored.
    New,
}

/// Decides [`DcimDecision`] for one candidate `(candidate_size,
/// candidate_mtime_unix)`, given the previously recorded `row` for the
/// *same source path* (`None` when the path has never been seen).
///
/// Pinned by the test suite:
/// - `row: None` => [`DcimDecision::New`] (unseen path).
/// - `row: Some` with both `size` and `mtime_unix` equal to the candidate's
///   => [`DcimDecision::Skip`].
/// - `row: Some` with `size` different (mtime equal or not) =>
///   [`DcimDecision::Rehash`].
/// - `row: Some` with `mtime_unix` different (size equal) =>
///   [`DcimDecision::Rehash`].
pub fn decide(
    row: Option<&SeenRow>,
    candidate_size: u64,
    candidate_mtime_unix: i64,
) -> DcimDecision {
    match row {
        None => DcimDecision::New,
        Some(row) if row.size == candidate_size && row.mtime_unix == candidate_mtime_unix => {
            DcimDecision::Skip {
                content_id: row.content_id.clone(),
            }
        }
        Some(_) => DcimDecision::Rehash,
    }
}
