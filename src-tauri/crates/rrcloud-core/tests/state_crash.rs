//! Crash-injection and cross-process lock tests for the redb state store
//! (architecture §2.1.5 durability order, §2.2 seq monotonicity, §5.1
//! single-instance exclusion).
//!
//! `harness = false`: this binary re-execs itself (`current_exe`) as a
//! child, dispatched on `RRCLOUD_STATE_CRASH_CHILD`:
//!
//! - `writer` — opens the db and loops forever: freeze_next_segment
//!   (single-txn span allocation + frozen bytes; 1–3 per-entry seqs per
//!   segment, §2.2) → one item transition → mark applied → set cursor,
//!   printing an ACK line to stdout strictly **after** each commit
//!   returns. The parent waits for committed progress, SIGKILLs it at a
//!   random moment (and verifies the child died by exactly that SIGKILL,
//!   so a panicking child cannot masquerade as a passed crash test), then
//!   asserts the reopened db's invariants against the ACK log (db state ≥
//!   every ACKed commit, every record parses, frozen bytes
//!   byte-identical, and — because allocation and freeze commit together —
//!   the segment spans tile the allocated seq range exactly: **no holes,
//!   no overlaps**, regardless of where the kill landed).
//! - `locker` — attempts to open a db the parent holds and must exit
//!   quickly with a code describing the typed outcome (the §5.1
//!   non-blocking refusal).
//!
//! Linux-only (SIGKILL timing + fork/exec assumptions); on other targets
//! the binary prints a skip notice and exits 0.

#[cfg(not(target_os = "linux"))]
fn main() {
    println!("state_crash: skipped (linux-only crash-injection suite)");
}

#[cfg(target_os = "linux")]
fn main() {
    linux::main();
}

#[cfg(target_os = "linux")]
mod linux {
    use std::collections::BTreeMap;
    use std::io::Write as _;
    use std::path::{Path, PathBuf};
    use std::process::{Child, Command, Stdio};
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    use rrcloud_core::clock::DeviceId;
    use rrcloud_core::keys::RelKey;
    use rrcloud_core::state::{legal, ItemRecord, ItemState, StateError, SyncDb};

    const CHILD_ENV: &str = "RRCLOUD_STATE_CRASH_CHILD";
    const DB_ENV: &str = "RRCLOUD_STATE_CRASH_DB";

    /// This device (the writer's own identity).
    const DEV_SELF: &str = "d1f0c2aa-9d2b-4a6e-8f1c-3b7d5e9a0c42";
    /// A peer device (applied/cursor bookkeeping target).
    const DEV_PEER: &str = "a3b2e1d0-5c4f-4b3a-9e2d-1f0a9b8c7d6e";

    /// Items the writer child cycles through the §2.4 upload pipeline.
    const ITEM_COUNT: u64 = 20;

    /// SIGKILL iterations in the crash scenario.
    const KILL_ITERATIONS: usize = 5;

    /// Exit codes of the `locker` child.
    const LOCKER_EXIT_LOCKED: i32 = 0; // typed AlreadyLocked observed (pass)
    const LOCKER_EXIT_OPENED: i32 = 20; // open unexpectedly succeeded
    const LOCKER_EXIT_OTHER_ERR: i32 = 21; // open failed with the wrong error

    fn dev(s: &str) -> DeviceId {
        DeviceId::new(s).expect("valid device id")
    }

    fn item_rel(idx: u64) -> RelKey {
        RelKey::new(format!("crash/item-{idx:02}.rrdata")).expect("valid relkey")
    }

    /// Deterministic entry count for the segment whose FIRST seq is
    /// `first` (1..=3): §2.2 seqs are per-entry, so the writer freezes
    /// multi-entry spans and both parent and child derive each span's
    /// width from its first seq alone.
    fn count_for(first: u64) -> u64 {
        first % 3 + 1
    }

    /// Deterministic segment bytes for the segment at first seq `seq`:
    /// both parent and child compute these independently, so byte equality
    /// after a crash proves the store returned exactly what was frozen
    /// (§2.1.5). Includes non-UTF8 bytes.
    fn seg_bytes(seq: u64) -> Vec<u8> {
        let mut v = format!("segment {seq} x{} \u{0000}", count_for(seq)).into_bytes();
        v.extend((0..(seq % 64 + 16)).map(|i| ((seq.wrapping_mul(31) + i) % 251) as u8));
        v
    }

    /// The writer child's transition cycle (all §2.4-legal edges).
    fn next_state(s: ItemState) -> ItemState {
        match s {
            ItemState::Dirty => ItemState::Queued,
            ItemState::Queued => ItemState::Uploading,
            ItemState::Uploading => ItemState::Verifying,
            ItemState::Verifying => ItemState::Synced,
            ItemState::Synced => ItemState::Dirty,
            other => panic!("writer child never puts an item in {other:?}"),
        }
    }

    fn state_name(s: ItemState) -> &'static str {
        match s {
            ItemState::Dirty => "dirty",
            ItemState::Queued => "queued",
            ItemState::Uploading => "uploading",
            ItemState::Verifying => "verifying",
            ItemState::Synced => "synced",
            ItemState::CorruptRemote => "corrupt_remote",
            ItemState::Conflict => "conflict",
            ItemState::PendingDown => "pending_down",
            ItemState::Downloading => "downloading",
            ItemState::Stub => "stub",
            ItemState::Hydrated => "hydrated",
        }
    }

    fn state_from_name(name: &str) -> ItemState {
        match name {
            "dirty" => ItemState::Dirty,
            "queued" => ItemState::Queued,
            "uploading" => ItemState::Uploading,
            "verifying" => ItemState::Verifying,
            "synced" => ItemState::Synced,
            "corrupt_remote" => ItemState::CorruptRemote,
            "conflict" => ItemState::Conflict,
            "pending_down" => ItemState::PendingDown,
            "downloading" => ItemState::Downloading,
            "stub" => ItemState::Stub,
            "hydrated" => ItemState::Hydrated,
            other => panic!("unknown state name in ACK log: {other:?}"),
        }
    }

    pub fn main() {
        match std::env::var(CHILD_ENV).ok().as_deref() {
            Some("writer") => child_writer(),
            Some("locker") => child_locker(),
            Some(other) => panic!("unknown child mode {other:?}"),
            None => parent(),
        }
    }

    // -- children ----------------------------------------------------------

    /// Infinite committed-write loop; one ACK line per committed operation,
    /// flushed, printed strictly after the commit returned. Runs until the
    /// parent SIGKILLs it.
    fn child_writer() {
        let path = std::env::var(DB_ENV).expect("db path env");
        let db = SyncDb::open(Path::new(&path), Some(dev(DEV_SELF))).expect("child open");
        let peer = dev(DEV_PEER);
        let stdout = std::io::stdout();
        let mut out = stdout.lock();
        let mut ack = |line: String| {
            writeln!(out, "{line}").expect("write ack");
            out.flush().expect("flush ack");
        };
        // Ensure the item set exists: insert-only (idempotent across child
        // restarts) and ONE committed transaction for all items, so the
        // parent's kill window reliably lands in the main loop rather than
        // in a 20-commit seeding phase. ACKs are printed strictly after the
        // batch commit returned.
        let inserted: Vec<u64> = db
            .with_txn(|t| {
                let mut inserted = Vec::new();
                for idx in 0..ITEM_COUNT {
                    let fresh = t.insert_item(
                        &item_rel(idx),
                        &ItemRecord {
                            kind: rrcloud_core::journal::Kind::Sidecar,
                            state: ItemState::Dirty,
                            size: 0,
                            mtime_unix_ns: 0,
                            blake3: None,
                            sem_hash: None,
                            vv: rrcloud_core::clock::VersionVector::new(),
                            content_id: None,
                            w: None,
                            h: None,
                            pinned: false,
                            last_access_unix: 0,
                            verified_remote: false,
                            attested: false,
                            base_unknown: false,
                        },
                    )?;
                    if fresh {
                        inserted.push(idx);
                    }
                }
                Ok(inserted)
            })
            .expect("insert items");
        for idx in inserted {
            ack(format!("PUT {idx}"));
        }
        loop {
            // Span allocation + frozen bytes in ONE committed transaction
            // (§2.1.5): the parent asserts gapless span tiling on the
            // strength of this. The entry count varies (1..=3) so the
            // crash coverage includes multi-entry spans; it is derived
            // from the first seq, which the child predicts from the
            // committed counter (freeze_next_segment confirms it).
            let expected_first = db.last_allocated_seq().expect("last_allocated_seq") + 1;
            let seq = db
                .freeze_next_segment(count_for(expected_first), |s| Ok(seg_bytes(s)))
                .expect("freeze_next_segment");
            assert_eq!(seq, expected_first, "span allocation is contiguous");
            ack(format!("SEQ {seq}"));
            ack(format!("FROZE {seq}"));
            let idx = seq % ITEM_COUNT;
            let rel = item_rel(idx);
            let from = db
                .get_item(&rel)
                .expect("get item")
                .expect("item exists")
                .state;
            let to = next_state(from);
            db.transition(&rel, from, to, |r| {
                r.size = seq;
                r.last_access_unix = seq;
            })
            .expect("transition");
            ack(format!("ITEM {idx} {}", state_name(to)));
            db.mark_applied(&peer, seq).expect("mark_applied");
            ack(format!("APPLIED {seq}"));
            db.set_cursor(&peer, seq).expect("set_cursor");
            ack(format!("CURSOR {seq}"));
        }
    }

    /// Tries to open a db the parent holds. Must observe the typed
    /// [`StateError::AlreadyLocked`] refusal immediately (no blocking).
    fn child_locker() {
        let path = std::env::var(DB_ENV).expect("db path env");
        match SyncDb::open(Path::new(&path), None) {
            Err(StateError::AlreadyLocked { .. }) => std::process::exit(LOCKER_EXIT_LOCKED),
            Ok(_) => std::process::exit(LOCKER_EXIT_OPENED),
            Err(e) => {
                eprintln!("locker: wrong error: {e:?}");
                std::process::exit(LOCKER_EXIT_OTHER_ERR);
            }
        }
    }

    // -- parent ------------------------------------------------------------

    /// Kills `child` on drop so a failing assertion cannot leak a looping
    /// writer process.
    struct KillOnDrop(Option<Child>);

    impl Drop for KillOnDrop {
        fn drop(&mut self) {
            if let Some(child) = &mut self.0 {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }

    fn pseudo_random_ms(lo: u64, hi: u64) -> u64 {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .subsec_nanos() as u64;
        lo + nanos % (hi - lo)
    }

    fn scratch_dir(tag: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("rrcloud-state-crash-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create scratch dir");
        dir
    }

    /// Complete, newline-terminated ACK lines (a SIGKILL can tear the last
    /// line mid-write; a torn fragment is not an acknowledged commit).
    fn complete_lines(raw: &[u8]) -> Vec<String> {
        let mut lines: Vec<String> = Vec::new();
        let mut rest = raw;
        while let Some(pos) = rest.iter().position(|&b| b == b'\n') {
            lines.push(String::from_utf8(rest[..pos].to_vec()).expect("utf8 ack line"));
            rest = &rest[pos + 1..];
        }
        lines
    }

    fn parent() {
        println!("state_crash: crash-injection scenario ({KILL_ITERATIONS} SIGKILL iterations)");
        crash_injection_scenario();
        println!("state_crash: lock-exclusivity scenario");
        lock_exclusivity_scenario();
        println!("state_crash: ok");
    }

    /// The load-bearing test (§2.1.5): SIGKILL a committing writer child at
    /// a random moment, reopen, and assert that the db is at or ahead of
    /// every ACKed commit and that nothing is torn.
    fn crash_injection_scenario() {
        let dir = scratch_dir("writer");
        let db_path = dir.join("state.redb");
        let log_path = dir.join("acks.log");

        // Accumulated ACKed facts across iterations (the db persists).
        let mut max_acked_seq: u64 = 0;
        let mut acked_frozen: Vec<u64> = Vec::new();
        let mut acked_item_state: BTreeMap<u64, ItemState> = BTreeMap::new();
        let mut acked_applied: Vec<u64> = Vec::new();
        let mut max_acked_cursor: u64 = 0;
        let peer = dev(DEV_PEER);

        for iteration in 0..KILL_ITERATIONS {
            let log = std::fs::File::create(&log_path).expect("create ack log");
            let child = Command::new(std::env::current_exe().expect("current_exe"))
                .env(CHILD_ENV, "writer")
                .env(DB_ENV, &db_path)
                .stdout(Stdio::from(log))
                .stderr(Stdio::inherit())
                .spawn()
                .expect("spawn writer child");
            let mut guard = KillOnDrop(Some(child));

            // Progress-gated kill trigger: wait until the child has ACKed
            // at least one committed span (so the kill window exercises
            // the main loop even on a loaded box where startup/recovery
            // eats wall time), then kill a short random moment later so
            // the SIGKILL lands at an arbitrary point mid-loop.
            let progress_deadline = Instant::now() + Duration::from_secs(30);
            loop {
                let raw = std::fs::read(&log_path).expect("read ack log");
                if complete_lines(&raw)
                    .iter()
                    .any(|line| line.starts_with("SEQ "))
                {
                    break;
                }
                let child = guard.0.as_mut().expect("child present");
                if let Some(status) = child.try_wait().expect("try_wait") {
                    panic!("iteration {iteration}: writer child exited on its own: {status:?}");
                }
                assert!(
                    Instant::now() < progress_deadline,
                    "iteration {iteration}: child made no committed progress within 30s"
                );
                std::thread::sleep(Duration::from_millis(5));
            }
            std::thread::sleep(Duration::from_millis(pseudo_random_ms(20, 300)));

            let mut child = guard.0.take().expect("child present");
            child.kill().expect("SIGKILL writer child"); // SIGKILL on unix
            let status = child.wait().expect("reap child");
            // The child must have died by OUR SIGKILL. A child that
            // self-terminated (a panic from a real store failure mid-loop)
            // also reports !success(), which would silently turn this into
            // a test of nothing — so pin the exact signal.
            assert_eq!(
                std::os::unix::process::ExitStatusExt::signal(&status),
                Some(libc::SIGKILL),
                "iteration {iteration}: child must have died by the parent's SIGKILL \
                 (a panic/self-exit means a store failure, not a crash test), got {status:?}"
            );

            // Parse this iteration's ACK log.
            let raw = std::fs::read(&log_path).expect("read ack log");
            let lines = complete_lines(&raw);
            let mut iteration_seqs = 0u64;
            for line in &lines {
                let mut parts = line.split(' ');
                let tag = parts.next().expect("tag");
                match tag {
                    "PUT" => {
                        let idx: u64 = parts.next().expect("idx").parse().expect("idx");
                        acked_item_state.entry(idx).or_insert(ItemState::Dirty);
                    }
                    "SEQ" => {
                        let seq: u64 = parts.next().expect("seq").parse().expect("seq");
                        assert!(
                            seq > max_acked_seq,
                            "iteration {iteration}: ACKed seq {seq} not strictly greater \
                             than previous {max_acked_seq} — seq regressed or repeated"
                        );
                        max_acked_seq = seq;
                        iteration_seqs += 1;
                    }
                    "FROZE" => {
                        let seq: u64 = parts.next().expect("seq").parse().expect("seq");
                        acked_frozen.push(seq);
                    }
                    "ITEM" => {
                        let idx: u64 = parts.next().expect("idx").parse().expect("idx");
                        let state = state_from_name(parts.next().expect("state"));
                        acked_item_state.insert(idx, state);
                    }
                    "APPLIED" => {
                        let seq: u64 = parts.next().expect("seq").parse().expect("seq");
                        acked_applied.push(seq);
                    }
                    "CURSOR" => {
                        let seq: u64 = parts.next().expect("seq").parse().expect("seq");
                        max_acked_cursor = max_acked_cursor.max(seq);
                    }
                    other => panic!("unknown ACK tag {other:?} in line {line:?}"),
                }
            }
            assert!(
                iteration_seqs > 0,
                "iteration {iteration}: child made no progress before the kill \
                 (ACK log had {} lines) — the test exercised nothing",
                lines.len()
            );

            // Reopen (also proves SIGKILL released the file lock) and
            // assert every invariant.
            let db = SyncDb::open(&db_path, None).expect("reopen after SIGKILL");
            assert_eq!(
                db.device_id(),
                &dev(DEV_SELF),
                "identity survived the crash"
            );

            // (1) §2.2: the counter never regresses below the last ACKed
            // span's END (an ACKed first seq of f with count_for(f)
            // entries means seqs f..=f+count-1 were committed).
            let acked_end = max_acked_seq + count_for(max_acked_seq).saturating_sub(1);
            let last = db.last_allocated_seq().expect("last_allocated_seq");
            assert!(
                max_acked_seq == 0 || last >= acked_end,
                "iteration {iteration}: last allocated seq {last} < last ACKed span end {acked_end}"
            );
            let probe_first = last + 1;
            let next = db
                .freeze_next_segment(count_for(probe_first), |s| Ok(seg_bytes(s)))
                .expect("freeze_next_segment after crash");
            assert_eq!(
                next, probe_first,
                "iteration {iteration}: post-crash allocation must continue the span tiling"
            );
            assert!(
                next > acked_end,
                "iteration {iteration}: post-crash allocation {next} <= ACKed span end {acked_end}"
            );
            max_acked_seq = next; // the probe freeze is itself committed
            acked_frozen.push(next);

            // (2) §2.1.5: the single-txn freeze means allocated seqs ALWAYS
            // have frozen bytes, wherever the SIGKILL landed — the stored
            // segment spans must tile 1..=last_allocated exactly (no
            // holes, no overlaps, ascending), every ACKed frozen segment
            // among them, and every one byte-identical to what was frozen
            // for its first seq (nothing is published in this scenario, so
            // unpublished_segments sees them all).
            let last_alloc = db.last_allocated_seq().expect("last_allocated_seq");
            let segs = db.unpublished_segments().expect("unpublished_segments");
            let mut expected_first = 1u64;
            for (seq, _) in &segs {
                assert_eq!(
                    *seq, expected_first,
                    "iteration {iteration}: segment spans must tile the allocated \
                     range gaplessly (a hole would stall every remote contiguity \
                     cursor forever; an overlap would reuse per-entry seqs)"
                );
                expected_first += count_for(*seq);
            }
            assert_eq!(
                expected_first,
                last_alloc + 1,
                "iteration {iteration}: spans must cover exactly 1..=last_allocated"
            );
            let frozen_firsts: std::collections::BTreeSet<u64> =
                segs.iter().map(|(s, _)| *s).collect();
            for &seq in &acked_frozen {
                assert!(
                    frozen_firsts.contains(&seq),
                    "iteration {iteration}: ACKed frozen segment {seq} missing after crash"
                );
            }
            for (seq, bytes) in &segs {
                assert_eq!(
                    *bytes,
                    seg_bytes(*seq),
                    "iteration {iteration}: stored segment {seq} torn/corrupt"
                );
            }

            // (3) No torn ItemRecord: every item parses, and its state is
            // the last known-committed one or one legal (unACKed committed)
            // step past it. After asserting, re-baseline on the OBSERVED
            // stored state — the committed ground truth the next iteration's
            // child starts from — so a kill inside the commit→ACK window
            // can leave the baseline stale by at most the current
            // iteration's one unACKed step, never accumulate across
            // iterations (which could make a correct store fail the
            // one-legal-step bound).
            for idx in 0..ITEM_COUNT {
                let rel = item_rel(idx);
                let record = db.get_item(&rel).unwrap_or_else(|e| {
                    panic!("iteration {iteration}: item {idx} failed to parse: {e:?}")
                });
                match (acked_item_state.get(&idx).copied(), record) {
                    (Some(_), None) => {
                        panic!("iteration {iteration}: ACKed item {idx} vanished")
                    }
                    (Some(acked), Some(record)) => {
                        assert!(
                            record.state == acked || legal(acked, record.state),
                            "iteration {iteration}: item {idx} state {:?} unreachable from \
                             last known-committed {acked:?}",
                            record.state
                        );
                        acked_item_state.insert(idx, record.state);
                    }
                    // PUT committed but its ACK was torn: adopt the
                    // observed committed state as the baseline.
                    (None, Some(record)) => {
                        acked_item_state.insert(idx, record.state);
                    }
                    (None, None) => {}
                }
            }

            // (4) applied/cursors: every ACKed apply is present (gaps are
            // fine — a kill between FROZE and APPLIED leaves that seq
            // legitimately unapplied) and the cursor did not regress.
            for &seq in &acked_applied {
                assert!(
                    db.has_applied(&peer, seq).expect("has_applied"),
                    "iteration {iteration}: ACKed applied seq {seq} lost"
                );
            }
            let cursor = db.cursor(&peer).expect("cursor");
            assert!(
                cursor >= max_acked_cursor,
                "iteration {iteration}: cursor {cursor} regressed below ACKed {max_acked_cursor}"
            );

            drop(db); // release the lock for the next iteration's child
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// §5.1: while this process holds the db, a child's open must be a
    /// fast typed refusal — bounded time, no hang, correct error.
    fn lock_exclusivity_scenario() {
        let dir = scratch_dir("locker");
        let db_path = dir.join("state.redb");
        let held = SyncDb::open(&db_path, Some(dev(DEV_SELF))).expect("parent open");

        let started = Instant::now();
        let child = Command::new(std::env::current_exe().expect("current_exe"))
            .env(CHILD_ENV, "locker")
            .env(DB_ENV, &db_path)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn locker child");
        let mut guard = KillOnDrop(Some(child));

        // Bounded wait: the empirical refusal is microseconds; 10 s is the
        // "it blocked" alarm threshold.
        let deadline = Instant::now() + Duration::from_secs(10);
        let status = loop {
            let child = guard.0.as_mut().expect("child present");
            if let Some(status) = child.try_wait().expect("try_wait") {
                break status;
            }
            assert!(
                Instant::now() < deadline,
                "locker child still running after 10s: open() blocked instead of refusing"
            );
            std::thread::sleep(Duration::from_millis(10));
        };
        guard.0 = None;

        let code = status.code().expect("locker exit code");
        assert_eq!(
            code,
            LOCKER_EXIT_LOCKED,
            "locker child must observe the typed AlreadyLocked refusal \
             (exit {LOCKER_EXIT_LOCKED}), got exit {code} \
             ({LOCKER_EXIT_OPENED} = open succeeded while held, \
             {LOCKER_EXIT_OTHER_ERR} = wrong error) after {:?}",
            started.elapsed()
        );

        drop(held);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
