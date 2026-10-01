# rrcloud-core

Sync-engine core for the RapidRawCloud fork of RapidRAW. It is a standalone
crate (own `Cargo.lock`, deliberately not a member of the `src-tauri`
workspace) shared by the desktop app, the Android plugin's Rust side and the
headless client. It contains:

- `s3/` — hand-rolled SigV4 client over `reqwest` (`S3Api`/`S3TransferApi`
  traits, multipart ops, typed errors); see
  [`docs/ARCHITECTURE.md`](../../../docs/ARCHITECTURE.md) §3.9 for why it is
  hand-rolled.
- `keys` — library-relative key mapping and the bucket key schema (§1.1/§1.2).
- `semhash` — sidecar semantic hash + parse-validation (§2.5).
- `clock` — per-key version vectors (§2.6).
- `journal` — append-only journal entry/segment wire format (§2.2).
- `state` — the redb-backed sync state store (`SyncDb`): item state machine,
  multipart bookkeeping, queues, outbound staging (§3.2).
- `publisher` / `reader` — journal segment publish and apply (§2.2/§2.3).
- `manifest` — per-writer manifest snapshots, bootstrap and compaction (§2.3).
- `transfer` — the per-item transfer engine: backend digest probe, verified
  upload/download with resume, and the bounded-concurrency queue pump (§2.4,
  §3.5, §2.1.5).

## Running the tests

```sh
scripts/fetch-garage.sh   # one-time: downloads + sha256-verifies Garage v2.2.0 into .garage/garage
cargo test
```

Or point the suite at your own binary with `GARAGE_BIN=/path/to/garage cargo test`
(`fetch-garage.sh` only supports x86_64 Linux; on other platforms use `GARAGE_BIN`).
The harness looks for the binary at `$GARAGE_BIN`, then `.garage/garage`, then
`/tmp/claude-0/garage`.

The integration suites (`tests/s3_conformance.rs`, `tests/publisher.rs`,
`tests/reader.rs`, `tests/manifest.rs`, `tests/transfer.rs`, and the two
harness-less child-process crash-injection binaries `tests/state_crash.rs`
and `tests/transfer_crash.rs`) spawn a shared real Garage v2.2.0 server per
test binary, so they need that binary. **With no binary found and `CI` unset,
the Garage-backed tests skip vacuously** — a loud `SKIP` message, every test
passes without asserting anything, and the harness-less crash binaries exit 0
without running at all — so a green `cargo test` without the binary proves
much less than it looks like. With `CI` set (as on GitHub Actions) a missing
binary is a hard failure. The crash binaries are additionally Linux-only
(SIGKILL timing assumptions) and print a skip notice elsewhere.

## Garage conformance notes

Observed against Garage v2.2.0 (the suite re-verifies these): a wrong
`Content-MD5` on `PutObject` and `UploadPart` is rejected with HTTP 400
`InvalidDigest`, where AWS S3 answers `BadDigest` for a well-formed but
mismatched digest, so the client treats both codes as the same digest
rejection. `HEAD` error responses carry no body, so the error code is
synthesized from the status (404 becomes `NoSuchKey`, which also means a
missing bucket is indistinguishable from a missing key on `HEAD`).

## Why no docker-compose harness

Spawning the Garage binary directly replaces a docker-compose setup, and
MinIO's open-source edition is archived, so Garage is the only S3 server we can
test against without a container.
