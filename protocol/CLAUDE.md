# protocol/ — wire-format definition and SDK generator

Also read the root `CLAUDE.md` and `protocol/README.md`.

- Source of truth: `rrcloud.protocol.toml`. Generator: `rrcgen/` (Python ≥ 3.11, stdlib only).
- **Never edit `gen/` by hand.** After changing the definition or a generator:
  ```sh
  python3 protocol/rrcgen            # regenerate (from repo root)
  python3 protocol/rrcgen --check    # must print "generated output is up to date"
  ```
  Commit the definition and the regenerated `gen/` together.
- Conformance (run all three after any change):
  ```sh
  (cd protocol/tests/rust && cargo test)
  cmake -S protocol/tests/c -B protocol/tests/c/build && cmake --build protocol/tests/c/build \
    && ctest --test-dir protocol/tests/c/build --output-on-failure
  (cd src-tauri/crates/rrcloud-core && cargo test --test proto_conformance --test firmware_interop)
  ```
- Consumers: `rrcloud-core`, `pairing/`, and the firmware (`firmware/rrcloud-ingest/components/rrcloud_proto`).
  A change here can break all three; firmware CI also runs on `protocol/**` changes.
- Compatibility: adding an optional field is backward compatible. Anything else bumps
  `JOURNAL_VERSION` / `MANIFEST_PROTO` and extends `supported_versions` (README "Evolving the protocol").
- Fixtures in `fixtures/` are shared by every conformance suite; change them only on purpose.
