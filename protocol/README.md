# RapidRawCloud protocol definition & SDK generator

One file defines everything implementations exchange — through the S3 bucket
(`docs/ARCHITECTURE.md` §1–§2) and through the pairing service (`docs/CLOUD_SETUP.md` §6):

```
protocol/rrcloud.protocol.toml      the definition (constants, scalars, enums, maps, records, keys, documents, S3 operations)
protocol/rrcgen/                    the generator (Python ≥ 3.11, standard library only)
protocol/gen/                       committed generated output — never edit by hand
  rust/rrcloud-proto/               Rust crate: serde types, validated newtypes, version vectors, keys, ndjson/gzip codecs
  c/rrcloud_proto.{h,c}             C99: fixed-buffer structs, self-contained JSON encode/decode, validators, key builders
  c/rrcloud_proto.hpp               C++17 header-only wrapper (std::string / optional / vector / map)
  schema/*.schema.json              JSON Schema 2020-12 (one per record + a bundle) — input for other generators and validators
  PROTOCOL.md                       human-readable reference
protocol/fixtures/                  canonical documents + expected keys shared by every conformance suite
protocol/tests/rust, protocol/tests/c   conformance tests for the generated SDKs
```

```sh
python3 protocol/rrcgen            # regenerate everything
python3 protocol/rrcgen --check    # CI: fail if protocol/gen is stale
python3 protocol/rrcgen --lang c   # one emitter
```

## Who implements it

| Implementation | Relationship to the definition |
|---|---|
| `src-tauri/crates/rrcloud-core` (app engine, worker) | Its wire types **are** the generated crate's (`clock`, `keys`, `semhash`, `journal`, `manifest`, `publisher` re-export `rrcloud-proto`'s scalars, enums, maps and records); the engine keeps its own codecs with a richer error taxonomy (`JournalError`, `ManifestError`), its path-mapping and key-classification logic, and the sync state machine. `tests/proto_conformance.rs` proves both codec lanes agree byte-for-byte on every fixture. |
| `pairing/` (pairing/discovery service) | Serves and stores `PairingInfo` / `PairingConfigDoc` straight from the generated crate (the Docker build context is the repository root for that reason). |
| `firmware/rrcloud-ingest` (ESP32 dock) | Links `protocol/gen/c` as an IDF component; its journal/manifest/registry encoders and key builders delegate to the generated code. |
| future SDKs | Add an emitter (below), or feed `gen/schema` to an existing generator (e.g. Java via jsonschema2pojo) and reuse `fixtures/` for conformance. |

## What the definition captures

- **Constants**: format versions, size caps, prefixes, time windows, pairing paths.
- **Scalars** with validation `format`s: `uuid4-lower` (DeviceId), `lower-hex` (Blake3Hex/ContentId/SemHash; the Rust
  crate adds `parse`/`from_bytes`/`from_hash`, and `derived_from` yields `ContentId::from_blake3`), `relkey`
  (the full §1.1 rule set incl. NFC, Windows-reserved names, `.rr.` prefix — two lanes in Rust: `RelKey::new`
  normalizes NFC for local paths, `RelKey::parse_wire` is the strict decoder lane, and `RelKeyError` names the rule
  that failed), `any` (BucketKey — a plain `String`, classified by consumers).
- **Version gates** on records: `JournalEntry.v` and `ManifestHeader.proto` must be a supported value; a manifest
  without a header line is a fail-closed decode error in every SDK.
- **Enums** with explicit wire spellings.
- **Maps** with `drop_zero` semantics (version vectors: `{a:1,b:0}` decodes equal to `{a:1}`).
- **Records**: ordered fields (encoding order is normative), `optional` (omitted when absent), `default` (filled in by decoders),
  `naming = "camel"` for the pairing documents, and `version_field` + `supported_versions` for the §2.2 min-reader gate.
- **Keys**: templates with `{param}`, `{seq:hex16}` and `{blake3_32(param)}` parts; every parameter is validated.
- **Documents**: which record lives at which key / HTTP path, in which encoding (`json`, `ndjson`, `gzip-ndjson`, `empty`),
  who may write it, and which caps apply.
- **Operations**: the S3 calls a backend must support, with the integrity notes (Content-MD5, UNSIGNED-PAYLOAD, no reliance on conditional PUT).

## Decoder contract (all languages)

- Unknown fields are ignored (min-reader rule) — never an error.
- Required fields missing, invalid scalars (bad hex, non-NFC relkey, non-UUID device), wrong JSON types and unknown
  enum values fail the whole document.
- A `version_field` outside `supported_versions` is a distinct error (`UnsupportedVersion` / `RRCP_E_UNSUPPORTED_VERSION`) so
  callers can surface "update required" instead of skipping.
- Integers are exact 64-bit (the C decoder has its own parser; no `double` round-trip).
- `max_length` on plain `string` fields sizes fixed buffers; the C decoder truncates longer values at a UTF-8
  boundary (a display name is not a protocol invariant). Validated scalars (`DeviceId`, hex, `RelKey`) never truncate.
- Encoders validate scalars and emit fields in definition order with serde_json-compatible escaping, so every
  implementation produces identical bytes for identical content (required for crash-replayed segments, §2.1.5).

## Adding a language

1. Copy `rrcgen/emit_c.py` or `emit_rust.py` as `emit_<lang>.py`; it receives the `Model` (`model.py`) and an output dir.
2. Register it in `rrcgen/__main__.py` `EMITTERS`.
3. Make it pass the fixtures: decode each file in `fixtures/`, re-encode, compare bytes; check `fixtures/keys.expected`;
   implement the negative cases listed in `tests/c/test_conformance.c`.
4. Add the build + test to `.github/workflows/protocol.yml`.

Java note: the JSON Schema bundle is already sufficient for jsonschema2pojo-style type generation; a dedicated
`emit_java.py` would additionally give the validated scalars, drop-zero maps, key builders and the version gate.

## Evolving the protocol

A wire change is a definition change first. Adding an optional field is backward compatible (old readers ignore it).
Anything else bumps `JOURNAL_VERSION` / `MANIFEST_PROTO` and extends `supported_versions`; readers that do not list the new
version fail closed by construction — the min-reader rule in `docs/ARCHITECTURE.md` §2.2.
