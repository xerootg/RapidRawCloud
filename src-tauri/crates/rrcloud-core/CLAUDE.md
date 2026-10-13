# rrcloud-core — sync engine

Also read the root `CLAUDE.md` and this directory's `README.md`.

- Standalone crate with its own `Cargo.lock`; run every `cargo` command from this directory. No
  tauri dependency is allowed here — the headless worker (`src/bin/rrcloud-worker.rs`) and the Android
  plugin share this crate.
- Tests: `scripts/fetch-garage.sh` once, then `CI=1 cargo test`. **Without Garage and without `CI`
  set, the S3-backed tests print SKIP and pass without testing anything.** Never report a green run
  that skipped.
- Lint exactly like CI: `cargo fmt --check && cargo clippy --all-targets -- -D warnings`.
- Wire types (`clock`, `keys`, `semhash`, `journal`, `manifest`, `publisher`) re-export the generated
  `rrcloud-proto` crate. Change wire shapes in `protocol/rrcloud.protocol.toml`, not here, then run
  `cargo test --test proto_conformance --test firmware_interop`.
- Protocol invariants from `docs/ARCHITECTURE.md` §2.1 that code must not break: no reliance on
  conditional PUT, LIST may be stale (never delete or resurrect on LIST absence alone), journal
  segments are immutable once published, deletions are soft (tombstone + grace window, aged by the
  server's `LastModified`, not client clocks).
- Everything read from the bucket is untrusted: bound sizes, page counts and key counts (see
  `tests/download_size_cap.rs`, `worker_list_cap.rs`, `xml_hostile_input.rs`), validate endpoints
  (`fleet_endpoint_validation.rs`), and redact secrets in `Debug` (`secret_redaction.rs`).
- New behaviour gets a test in `tests/`; shared harness code is in `tests/common/`.
