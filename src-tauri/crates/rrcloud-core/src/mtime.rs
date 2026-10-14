//! §2.2 original mtimes: the earliest-known-mtime rule and its one-time
//! backfill.
//!
//! An original's `mtime` rides its `put` entry so every device materializes
//! the file — stub or hydrated — at the date the library sorts by. Two
//! versions of one key can carry the **same bytes** under different mtimes.
//! The case that motivated this module: the ingest dock journals a whole card
//! run only when the run ends, hours after its first PUT, and the worker's
//! 15-minute reconcile adopted the not-yet-journaled objects as foreign drops,
//! stamping each `{worker:1}` version with the object's `LastModified` — the
//! upload time — while the dock's own entry later carried the file's
//! capture-time mtime. The §2.6 converged apply kept whichever twin a device
//! applied first, so most devices showed hundreds of photos on their upload
//! date.
//!
//! The rule ([`earlier_mtime`]): when content-equal versions of an original
//! converge, the record keeps the **earliest** positive mtime. Identical
//! bytes cannot be younger than their oldest honest timestamp (the later one
//! is an upload- or copy-time artifact), and a minimum does not depend on
//! arrival order, so every device settles on the same value. A device whose
//! file the engine materialized re-stamps it ([`restamp_local_file`]) so the
//! grid's date sort follows the record.
//!
//! The rule only acts when a twin is *applied*, and devices that converged
//! before it existed have already applied both twins. Their one-time repair
//! is [`backfill_earliest_mtimes`]: it re-reads the original `put`s still in
//! the bucket (every foreign journal segment and manifest), lowers each
//! content-equal record's mtime the same way, and then records that it ran.
//! It writes no journal entry — the facts are already in the bucket, so each
//! device re-derives the same answer locally and the outcome does not depend
//! on when a device updated relative to any other device.

use std::collections::HashMap;
use std::path::Path;

use crate::journal::{decode_segment, Kind, Op, JOURNAL_VERSION, SEGMENT_MAX_BYTES};
use crate::keys::{classify_key, KeyClass, RelKey, CONTROL_PREFIX};
use crate::manifest::{get_manifest, ManifestError};
use crate::s3::{ListObjectsV2Request, S3Api, S3Error};
use crate::semhash::Blake3Hex;
use crate::state::{ItemState, StateError, SyncDb};

const NANOS_PER_SEC: i64 = 1_000_000_000;

/// Page cap for the backfill's `ListObjectsV2` loops — the reader's journal
/// lane bound (a backend that keeps answering truncated pages is refused).
const LIST_MAX_PAGES: u32 = 10_000;

/// Key cap for the same loops — the reader's journal-lane bound: a
/// conforming fleet is compaction-bounded to a few thousand journal keys.
const LIST_MAX_KEYS: usize = 100_000;

/// The mtime (unix seconds) a converging content-equal version carrying
/// `candidate` hands a record whose mtime is `local_mtime_unix_ns`: `Some`
/// only when the candidate is positive and earlier than the record's at
/// whole-second precision, or the record has no positive mtime. Equal
/// seconds keep the record's nanosecond value, which the whole-second wire
/// value would only coarsen.
pub fn earlier_mtime(local_mtime_unix_ns: i64, candidate: Option<i64>) -> Option<i64> {
    let candidate = candidate.filter(|&secs| secs > 0)?;
    let local_secs = local_mtime_unix_ns.div_euclid(NANOS_PER_SEC);
    (local_secs <= 0 || candidate < local_secs).then_some(candidate)
}

/// Re-stamps the local file of an original whose record mtime just moved to
/// `mtime_unix`, when `state` says the engine materialized that file: a
/// `Stub` placeholder (written at the record's mtime by the stub pass) or
/// `Hydrated` bytes (installed at it). In every other state the file is
/// either not there yet — the stub pass or the download install reads the
/// corrected record — or owned by an upload lane.
///
/// Best-effort: the record is the source of truth and a failed stamp only
/// leaves the grid sorting by the old date. A missing file is silent (the
/// worker has no library tree under its state dir).
pub fn restamp_local_file(sync_root: &Path, item: &RelKey, state: ItemState, mtime_unix: i64) {
    if !matches!(state, ItemState::Stub | ItemState::Hydrated) {
        return;
    }
    let path = crate::keys::local_path(item, sync_root);
    if let Err(e) =
        filetime::set_file_mtime(&path, filetime::FileTime::from_unix_time(mtime_unix, 0))
    {
        if e.kind() != std::io::ErrorKind::NotFound {
            log::warn!("restamp mtime of {}: {e}", path.display());
        }
    }
}

/// A failure that stops [`backfill_earliest_mtimes`] before it completes; it
/// runs again on the next call (the done marker is written only on success).
#[derive(Debug, thiserror::Error)]
pub enum MtimeBackfillError {
    /// State-store failure.
    #[error(transparent)]
    State(#[from] StateError),
    /// A LIST or GET failed in a way the next attempt may not repeat.
    #[error(transparent)]
    S3(#[from] S3Error),
}

/// What one [`backfill_earliest_mtimes`] call did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct MtimeBackfillReport {
    /// `false` when the backfill had already completed on this db, so
    /// nothing was read.
    pub ran: bool,
    /// Foreign journal segments read.
    pub segments_read: usize,
    /// Foreign manifests read.
    pub manifests_read: usize,
    /// Segments or manifests skipped as unreadable (oversized, undecodable,
    /// a format this build does not read) — persistent conditions a retry
    /// would not clear.
    pub skipped_unreadable: usize,
    /// Originals whose record mtime moved earlier, ascending by relkey.
    pub corrected: Vec<RelKey>,
}

/// The one-time repair for records that converged before the
/// [`earlier_mtime`] rule existed (module docs).
///
/// Collects the earliest positive mtime that any foreign journal `put` or
/// manifest row in the bucket carries for each live local original's exact
/// `blake3`, lowers each record whose mtime is later (state-preserving, in
/// one transaction per item that re-checks the record), re-stamps the files
/// the engine materialized under `sync_root` (`None` for a role with no
/// library tree, the worker), and marks the backfill done. A no-op returning
/// `ran: false` once done.
///
/// Reads are bounded like the reader's: listings by [`LIST_MAX_KEYS`] /
/// [`LIST_MAX_PAGES`], segments by [`SEGMENT_MAX_BYTES`], manifests by the
/// manifest fetch caps. A segment or manifest that cannot be read for a
/// persistent reason is skipped and counted; a transport failure aborts
/// with `Err` so the next cycle retries the whole pass.
pub async fn backfill_earliest_mtimes(
    db: &SyncDb,
    s3: &impl S3Api,
    bucket: &str,
    sync_root: Option<&Path>,
) -> Result<MtimeBackfillReport, MtimeBackfillError> {
    let mut report = MtimeBackfillReport::default();
    if db.mtime_backfill_done()? {
        return Ok(report);
    }
    report.ran = true;

    // The live originals whose mtime a twin could correct, by exact bytes.
    let wanted: HashMap<RelKey, Blake3Hex> = db
        .iter_items()?
        .into_iter()
        .filter(|(_, r)| r.kind == Kind::Original && !r.deleted)
        .filter_map(|(rk, r)| r.blake3.map(|b| (rk, b)))
        .collect();
    let mut earliest: HashMap<RelKey, i64> = HashMap::new();
    let mut offer = |relkey: &RelKey, blake3: Option<&Blake3Hex>, mtime: Option<i64>| {
        let Some(mtime) = mtime.filter(|&m| m > 0) else {
            return;
        };
        if wanted.get(relkey).is_some_and(|w| Some(w) == blake3) {
            let slot = earliest.entry(relkey.clone()).or_insert(mtime);
            *slot = (*slot).min(mtime);
        }
    };

    let own = db.device_id().clone();
    if !wanted.is_empty() {
        for key in list_keys(s3, bucket, &format!("{CONTROL_PREFIX}journal/")).await? {
            let KeyClass::Journal {
                device, version, ..
            } = classify_key(&key)
            else {
                continue;
            };
            if device == own {
                continue;
            }
            if version != JOURNAL_VERSION {
                report.skipped_unreadable += 1;
                continue;
            }
            let Some(bytes) = get_capped(s3, bucket, &key, &mut report).await? else {
                continue;
            };
            let Ok(entries) = decode_segment(&bytes) else {
                report.skipped_unreadable += 1;
                continue;
            };
            report.segments_read += 1;
            for e in &entries {
                if e.op != Op::Put || e.kind != Kind::Original {
                    continue;
                }
                if let KeyClass::Original { relkey } = classify_key(&e.key) {
                    offer(&relkey, e.blake3.as_ref(), e.mtime);
                }
            }
        }

        for key in list_keys(s3, bucket, &format!("{CONTROL_PREFIX}manifests/")).await? {
            let KeyClass::Manifest { device } = classify_key(&key) else {
                continue;
            };
            if device == own {
                continue;
            }
            match get_manifest(s3, bucket, &device).await {
                Ok(manifest) => {
                    report.manifests_read += 1;
                    for row in manifest.rows.iter().filter(|r| r.kind == Kind::Original) {
                        offer(&row.key, row.blake3.as_ref(), row.mtime);
                    }
                }
                Err(ManifestError::S3(e)) if e.is_no_such_key() => {}
                Err(ManifestError::S3(e)) => return Err(e.into()),
                Err(ManifestError::State(e)) => return Err(e.into()),
                Err(e) => {
                    log::warn!("mtime backfill: skipping unreadable manifest {key}: {e}");
                    report.skipped_unreadable += 1;
                }
            }
        }
    }

    let mut corrections: Vec<(RelKey, i64)> = earliest.into_iter().collect();
    corrections.sort();
    for (relkey, candidate) in corrections {
        let expected = &wanted[&relkey];
        // Re-check against the record as it is NOW: a poll or hydrate may
        // have moved it on since the snapshot above.
        let moved = db.with_txn(|t| {
            let Some(current) = t.get_item(&relkey)? else {
                return Ok(None);
            };
            if current.kind != Kind::Original
                || current.deleted
                || current.blake3.as_ref() != Some(expected)
            {
                return Ok(None);
            }
            let Some(secs) = earlier_mtime(current.mtime_unix_ns, Some(candidate)) else {
                return Ok(None);
            };
            t.update_item(&relkey, current.state, |r| {
                r.mtime_unix_ns = secs.saturating_mul(NANOS_PER_SEC);
            })?;
            Ok(Some((secs, current.state)))
        })?;
        if let Some((secs, state)) = moved {
            if let Some(root) = sync_root {
                restamp_local_file(root, &relkey, state, secs);
            }
            report.corrected.push(relkey);
        }
    }

    db.set_mtime_backfill_done()?;
    Ok(report)
}

/// GET `key` buffered to [`SEGMENT_MAX_BYTES`]. `Ok(None)` — counted in
/// `report.skipped_unreadable` unless the object simply vanished — for an
/// object that is gone (compacted between LIST and GET) or over the cap;
/// `Err` for a transport failure worth retrying.
async fn get_capped(
    s3: &impl S3Api,
    bucket: &str,
    key: &str,
    report: &mut MtimeBackfillReport,
) -> Result<Option<bytes::Bytes>, S3Error> {
    let output = match s3.get_object(bucket, key, None).await {
        Ok(output) => output,
        Err(e) if e.is_no_such_key() => return Ok(None),
        Err(e) => return Err(e),
    };
    if output.content_length > SEGMENT_MAX_BYTES as u64 {
        report.skipped_unreadable += 1;
        return Ok(None);
    }
    match output.body.collect_capped(SEGMENT_MAX_BYTES).await {
        Ok(bytes) => Ok(Some(bytes)),
        Err(S3Error::BodyCapExceeded { .. }) => {
            report.skipped_unreadable += 1;
            Ok(None)
        }
        Err(e) => Err(e),
    }
}

/// Every key under `prefix`, paged and bounded by [`LIST_MAX_PAGES`] /
/// [`LIST_MAX_KEYS`] so a hostile bucket cannot force unbounded buffering.
async fn list_keys(s3: &impl S3Api, bucket: &str, prefix: &str) -> Result<Vec<String>, S3Error> {
    let mut keys = Vec::new();
    let mut continuation_token: Option<String> = None;
    for _ in 0..LIST_MAX_PAGES {
        let page = s3
            .list_objects_v2(
                bucket,
                &ListObjectsV2Request {
                    prefix: Some(prefix.to_string()),
                    continuation_token: continuation_token.take(),
                    ..Default::default()
                },
            )
            .await?;
        keys.extend(page.objects.into_iter().map(|o| o.key));
        if keys.len() > LIST_MAX_KEYS {
            return Err(S3Error::InvalidResponse(format!(
                "ListObjectsV2 returned more than {LIST_MAX_KEYS} keys under {prefix:?}; \
                 refusing unbounded key buffering"
            )));
        }
        if !page.is_truncated {
            return Ok(keys);
        }
        match page.next_continuation_token {
            Some(token) => continuation_token = Some(token),
            None => {
                return Err(S3Error::InvalidResponse(format!(
                    "truncated ListObjectsV2 page without a NextContinuationToken under {prefix:?}"
                )))
            }
        }
    }
    Err(S3Error::InvalidResponse(format!(
        "ListObjectsV2 still truncated after {LIST_MAX_PAGES} pages under {prefix:?}"
    )))
}
