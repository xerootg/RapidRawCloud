//! Crash-injection and cross-process lock tests for the redb state store
//! (architecture §2.1.5 durability order, §2.2 seq monotonicity, §5.1
//! single-instance exclusion).
//!
//! `harness = false`: this binary re-execs itself (`current_exe`) as a
//! child, dispatched on `RRCLOUD_STATE_CRASH_CHILD`:
//!
//! - `writer` — opens the db and loops forever: allocate seq → freeze
//!   segment → one item transition → mark applied → set cursor, printing an
//!   ACK line to stdout strictly **after** each commit returns. The parent
//!   SIGKILLs it at a random moment and then asserts the reopened db's
//!   invariants against the ACK log (db state ≥ every ACKed commit, every
//!   record parses, frozen bytes byte-identical).
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

    /// Deterministic segment bytes for `seq`: both parent and child compute
    /// these independently, so byte equality after a crash proves the store
    /// returned exactly what was frozen (§2.1.5). Includes non-UTF8 bytes.
    fn seg_bytes(seq: u64) -> Vec<u8> {
        let mut v = format!("segment {seq} \u{0000}").into_bytes();
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
        // Ensure the item set exists (idempotent across child restarts).
        for idx in 0..ITEM_COUNT {
            let rel = item_rel(idx);
            if db.get_item(&rel).expect("get item").is_none() {
                db.put_item(
                    &rel,
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
                )
                .expect("put item");
                ack(format!("PUT {idx}"));
            }
        }
        loop {
            let seq = db.allocate_seq().expect("allocate_seq");
            ack(format!("SEQ {seq}"));
            db.freeze_segment(seq, &seg_bytes(seq))
                .expect("freeze_segment");
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

            std::thread::sleep(Duration::from_millis(pseudo_random_ms(80, 400)));

            let mut child = guard.0.take().expect("child present");
            child.kill().expect("SIGKILL writer child"); // SIGKILL on unix
            let status = child.wait().expect("reap child");
            assert!(
                !status.success(),
                "iteration {iteration}: child must have died by signal, got {status:?}"
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

            // (1) §2.2: seq never regresses below the last ACKed allocation.
            let last = db.last_allocated_seq().expect("last_allocated_seq");
            assert!(
                last >= max_acked_seq,
                "iteration {iteration}: last allocated seq {last} < last ACKed {max_acked_seq}"
            );
            let next = db.allocate_seq().expect("allocate after crash");
            assert!(
                next > max_acked_seq,
                "iteration {iteration}: post-crash allocation {next} <= ACKed {max_acked_seq}"
            );
            max_acked_seq = next; // the probe allocation is itself committed

            // (2) §2.1.5: every ACKed frozen segment is present
            // byte-identically, in seq order; and every stored segment
            // (ACKed or committed-but-unACKed) carries exactly the bytes
            // frozen for its seq.
            let segs = db.unpublished_segments().expect("unpublished_segments");
            let seq_order: Vec<u64> = segs.iter().map(|(s, _)| *s).collect();
            let mut sorted = seq_order.clone();
            sorted.sort_unstable();
            assert_eq!(seq_order, sorted, "segments must come back in seq order");
            let by_seq: BTreeMap<u64, &Vec<u8>> = segs.iter().map(|(s, b)| (*s, b)).collect();
            for &seq in &acked_frozen {
                let bytes = by_seq.get(&seq).unwrap_or_else(|| {
                    panic!("iteration {iteration}: ACKed frozen seq {seq} missing after crash")
                });
                assert_eq!(
                    **bytes,
                    seg_bytes(seq),
                    "iteration {iteration}: frozen seq {seq} not byte-identical"
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
            // the last ACKed one or one legal (unACKed committed) step past
            // it.
            for idx in 0..ITEM_COUNT {
                let rel = item_rel(idx);
                let record = db.get_item(&rel).unwrap_or_else(|e| {
                    panic!("iteration {iteration}: item {idx} failed to parse: {e:?}")
                });
                if let Some(&acked) = acked_item_state.get(&idx) {
                    let record = record.unwrap_or_else(|| {
                        panic!("iteration {iteration}: ACKed item {idx} vanished")
                    });
                    assert!(
                        record.state == acked || legal(acked, record.state),
                        "iteration {iteration}: item {idx} state {:?} unreachable from \
                         last ACKed {acked:?}",
                        record.state
                    );
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
