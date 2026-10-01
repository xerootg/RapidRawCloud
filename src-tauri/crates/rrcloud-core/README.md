# rrcloud-core

Sync-engine core for the RapidRawCloud fork of RapidRAW. It is a standalone
crate (own `Cargo.lock`, deliberately not a member of the `src-tauri`
workspace) shared by the desktop app, the Android plugin's Rust side and the
headless client. Currently it contains only the S3 layer: a hand-rolled SigV4
client over `reqwest`. See [`docs/ARCHITECTURE.md`](../../../docs/ARCHITECTURE.md)
§3.9 (why the S3 client is hand-rolled) and §2.4 (the upload state machine and
the `Content-MD5` integrity backbone it serves).

## Running the tests

```sh
scripts/fetch-garage.sh   # one-time: downloads + sha256-verifies Garage v2.2.0 into .garage/garage
cargo test
```

Or point the suite at your own binary with `GARAGE_BIN=/path/to/garage cargo test`
(`fetch-garage.sh` only supports x86_64 Linux; on other platforms use `GARAGE_BIN`).
The harness looks for the binary at `$GARAGE_BIN`, then `.garage/garage`, then
`/tmp/claude-0/garage`.

The integration suite (`tests/s3_conformance.rs`) spawns a real Garage v2.2.0
server per test binary, so it needs that binary. **With no binary found and
`CI` unset, the conformance suite skips vacuously**: it prints a loud `SKIP`
message and every test passes without asserting anything. With `CI` set (as on
GitHub Actions) a missing binary is a hard failure.

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
