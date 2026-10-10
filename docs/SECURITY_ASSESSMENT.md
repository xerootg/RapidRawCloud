# RapidRawCloud — Security & Safety Assessment

Date: 2026-10-10 · Commit assessed: `41dea5f` · Scope: whole repository (Tauri desktop/Android app, `rrcloud-core` sync engine, headless worker, pairing service, Helm chart, CI, dependencies).

## 1. Method and threat model

Manual code review of every trust boundary, backed by `cargo audit` on all three lockfiles and `npm audit` on the frontend. Line references were verified against the tree; two non-obvious behaviours (`Path::starts_with` not resolving `..`, chrono panicking on a bad format string) were confirmed by running small test programs.

Threat actors considered:

| Actor | Position |
|---|---|
| Web content / XSS in the webview | Full IPC access, because the Tauri CSP is disabled |
| Hostile or compromised peer device | Same S3 credentials, writes anything into the bucket |
| Hostile or MITM S3 endpoint | Controls every byte the engine reads |
| Another paired user (fleet / pairing deployment) | Authenticated, low-trust tenant |
| Other apps on the same Android device | Media-write permission, scheme hijack |
| Supply chain | Floating git deps, `:latest` images, unpinned actions |

Overall picture: **the sync engine core is unusually well defended** (see §8), the **pairing service and fleet worker have real multi-tenant gaps**, and **the desktop app's webview-to-Rust boundary has no scoping at all**, so any future XSS is a full compromise of the user's files.

## 2. Executive summary — what to fix first

| # | Finding | Severity | Where |
|---|---|---|---|
| 1 | Fleet worker follows any user-supplied S3 endpoint → blind SSRF with PUT/DELETE from inside the cluster | **High** | `rrcloud-core/src/fleet.rs:223-242`, `pairing/src/main.rs:340-370` |
| 2 | Pairing browser routes trust `X-Authentik-Username` with no secret; chart ships with forward-auth **off** by default | **High** (config-dependent) | `pairing/src/main.rs:271-282`, `deploy/helm/.../values.yaml:30-36` |
| 3 | CSP disabled and ~140 IPC commands operate on arbitrary paths (delete, copy into asset scope, rename, `xdg-open`/`open -R`) | **High** (architectural; needs an XSS or malicious local process to trigger) | `tauri.conf.json:31`, `file_management.rs` |
| 4 | `/api/config` accepts any Authentik access token (no audience check) and keys users by mutable `preferred_username` | Medium | `pairing/src/main.rs:238-256` |
| 5 | `POST /save` has no CSRF token; relies entirely on Authentik's cookie SameSite policy | Medium | `pairing/src/main.rs:302-370` |
| 6 | GC trusts remote `server_ts` and version vectors; a hostile peer can destroy data instantly and un-restorably | Medium | `rrcloud-core/src/compact.rs:729-751, 1562-1617` |
| 7 | Case-insensitive FS: a case-variant remote key silently overwrites a user's original | Medium | `rrcloud-core/src/transfer.rs:2109-2141, 2241` |
| 8 | Unbounded download bodies / sidecar buffering; unbounded LIST loops in worker | Medium | `transfer.rs:2191-2236`, `worker.rs:1342-1367` |
| 9 | `quick-xml` memory-exhaustion advisories (RUSTSEC-2026-0194/0195) in a parser fed by the S3 server | Medium | all three lockfiles |
| 10 | AI connector is plaintext HTTP, unauthenticated, uploads full-res photos; polled every 10 s | Medium | `ai_connector.rs:132`, `inpainting.rs:593` |
| 11 | GPS coordinates of viewed photos sent to openstreetmap.org automatically | Medium (privacy) | `MetadataPanel.tsx:779` |

## 3. Desktop app: webview ↔ Rust boundary

### 3.1 [High] No CSP, and the IPC surface is unscoped
`src-tauri/tauri.conf.json:31` sets `"csp": null`. Tauri's capability system only gates *which* commands exist; the app's own commands take raw paths and never check them against the open library roots. Consequence: any XSS (none found today, see §3.3) or any content-injection into the webview equals arbitrary file read/write/delete as the user. Individual primitives, all in `src-tauri/src/file_management.rs` unless noted:

| Primitive | Location | Notes |
|---|---|---|
| Arbitrary delete, with permanent-delete fallback | `delete_folder` :2333-2341, `delete_files_from_disk` :3617-3690, `delete_files_with_associated` :3718-3800 | `trash::delete` failure → `fs::remove_dir_all` |
| Arbitrary file read + exfiltration | `copy_files` :2476-2559, `move_files` :2561-2639 | Copy any file into `$APPCACHE/thumbnails` (the asset-protocol scope, path leaked via `thumbnail-generated` events :2110-2121) then `fetch(convertFileSrc(...))`; no CSP means it can be POSTed anywhere |
| Arbitrary overwrite | `handle_export_presets_to_file` :3455-3467; `save_collage` `lib.rs:1575-1602`; `save_hdr`/panorama/focus-stack | Base64 payload only prefix-checked |
| Arbitrary rename/move | `rename_folder` :2306-2331 (`parent.join(new_name)` with `/`, `..`); `rename_files` :4138-4245 (template substituted verbatim) | |
| Launch external handler on attacker target | `show_in_finder` :3570-3614 | Linux: `xdg-open parent("http://evil/x")` opens a URL; macOS: `open -R <path>` where path like `-aTerminal` is parsed as an option; pair with `save_temp_file` (`lib.rs:1461-1466`, unbounded, never cleaned) to "write bytes, then open with default handler" |
| Settings-driven XMP/sidecar writes anywhere | `save_metadata_and_update_thumbnail` :2641-2679, `sync_metadata_to_xmp` :4422-4537 | Unescaped tag strings spliced into XML (:4525); `create_xmp_if_missing` flippable via `save_settings` |
| Scope check bypass | `remove_lut` `lut_processing.rs:604-617` | `starts_with` on an un-canonicalized path; `luts/../../..` passes |
| FS-wide walks | `clear_all_sidecars` :3525-3549, `clear_*_tags` `tagging.rs:589-653`, `list_images_recursive` :746-879 | `WalkDir` from `/` deletes every `.rrdata` on every volume; also a directory-listing oracle |
| Panic on attacker string | `import_files` :3920, :3980 | `date_folder_format` → chrono `format()` panics on `%Q`; task dies, UI hangs |
| Credential redirect | `sync_set_credentials`, `sync_configure`, `save_settings` | XSS can repoint the whole library to an attacker bucket |

**Recommendation.** One shared `assert_within_library(path)` guard (canonicalize, compare against canonicalized `root_folders` + app-data dirs) applied to every mutating command; reject path separators in `new_name`/templates; set a real CSP (`default-src 'self'; img-src 'self' asset: blob: data:; connect-src` the few hosts listed in §5; `frame-src` none or OSM only); fix `remove_lut` by canonicalizing; validate the chrono format string; quote `--` on macOS `open`.

### 3.2 [Low] Launch arguments / file association
`lib.rs:1986-2002`, `launch_request.rs:44-162`: argv from a second instance (same local user via D-Bus/named pipe) or a double-clicked file is forwarded to the webview without checking existence/type. `--edit <src> --output <dst>` pre-fills an arbitrary export destination that is written when the user clicks finish (`useExternalEditSession.ts:80-92`). Validate extension + regular file; show the output path.

### 3.3 [Info] Frontend sinks
No reachable XSS sink was found. The only `dangerouslySetInnerHTML` is a static SVG (`CommunityPage.tsx:350`); all untrusted strings (EXIF, filenames, community preset names, S3 errors) render as React text; all `shell.open` calls use constants. i18next runs with `escapeValue: false` (`src/i18n/index.ts:42`) and interpolates network data (`presetBy`), which is safe only while the result stays a text node. Given §3.1, treat any future HTML sink as critical.

### 3.4 [Low] Other desktop notes
- `std::env::set_var` inside `unsafe` after threads are spawned (`lib.rs:2117-2195`); `WGPU_BACKEND` value comes from attacker-writable `settings.json`. Move before thread spawn.
- `read_file_mapped` (`file_management.rs:1452-1473`) mmaps a file another process can truncate → SIGBUS. Local only.
- `image_loader.rs:541` calls `reader.no_limits()`: a crafted header claiming e.g. 100k×100k pixels allocates tens of GB before decode. Keep a sane `Limits` (e.g. 1 GiB) instead of disabling.
- `shell:default` grants `shell:allow-open` with the default http/https/mailto/tel validator; fine, but `process:default` and `shell` could be narrowed further.
- Two different `rawler` revisions are compiled into the app (`src-tauri/Cargo.lock`: `934af4b` via rrcloud-core and floating `a32bc1f` via the app's unpinned `git =` dep). The "bit-identical develop path" claim in `docs/ARCHITECTURE.md §4.2` does not currently hold; pin the app dep to the same `rev`.

## 4. Pairing service (`pairing/`) and Helm chart

### 4.1 [High, config-dependent] Browser identity is an unauthenticated header
`pairing/src/main.rs:271-282` takes identity from `X-Authentik-Username` with no shared secret or JWT check. Correct only when every request to `/` and `/save` traverses the Traefik forward-auth middleware (Traefik does delete and re-set `authResponseHeaders`, so spoofing through the middleware fails). But:
- `deploy/helm/rapidraw-cloud/values.yaml:30-36` defaults `forwardAuth.enabled: false`; enabling `ingressRoute` without it exposes read/write of **every user's S3 credentials** to anyone who sets one header.
- The Service is ClusterIP with no NetworkPolicy, so any pod in the cluster can call it directly with a forged header.
- `trustForwardHeader: true` in the Middleware trusts client `X-Forwarded-*`.

**Fix.** Verify `X-Authentik-Jwt` against the Authentik JWKS (or require a shared-secret header injected by the middleware), fail closed when absent, add a NetworkPolicy allowing only Traefik, and make the chart refuse `ingressRoute.enabled` without `forwardAuth.enabled`.

### 4.2 [Medium] `/api/config` has no audience check; identity keyed on mutable username
`validate_bearer` (`main.rs:238-256`) calls userinfo and accepts any token Authentik honours. Authentik's userinfo endpoint accepts access tokens from **any** provider in the instance, so any other OAuth client registered there (or a token leaked from one) can retrieve a user's library credentials. Decode the JWT and require `aud`/`azp == OIDC_CLIENT_ID`, or introspect with the client id. Separately, both paths key the config doc on `preferred_username` (fallback `sub`); usernames are reassignable in Authentik, so a renamed or recreated account inherits another user's bucket credentials. Key on `sub` (the Authentik `X-Authentik-Uid` header is already in `authResponseHeaders`).

### 4.3 [Medium] CSRF on `POST /save`
No anti-CSRF token or `Origin`/`Sec-Fetch-Site` check (`main.rs:302-370`). A cross-site form post that carries the Authentik session cookie overwrites the victim's endpoint/bucket/credentials, after which every device *and the worker* start syncing the victim's library into the attacker's bucket. Authentik's proxy cookie is `SameSite=Lax` in current versions, which blocks the cross-site case but not same-site siblings (`*.themissing.xyz`). Add a synchronizer token or reject when `Sec-Fetch-Site` is not `same-origin`.

### 4.4 [Medium, design] Plaintext S3 secrets in the admin bucket
Every paired user's library secret sits in `users/<u>/config.json` in cleartext. The worker holds a read key for the whole admin bucket, so one worker compromise = all libraries. Consider envelope-encrypting `credentials` with a key only the pairing service and worker hold, or short-lived STS/scoped keys where the provider supports them.

### 4.5 Low / Info
- No security headers (`X-Frame-Options`/CSP) on the HTML page → clickjacking of the save form.
- `/api/config` is unauthenticated until the upstream userinfo call; no rate limit → amplification against Authentik.
- `rustls-webpki 0.101.7` (via rust-s3's hyper 0.14 stack) has three name-constraint/CRL advisories (RUSTSEC-2026-0098/0099/0104); low impact because the admin endpoint is in-cluster HTTP, but upgrade `rust-s3` or switch the admin client to the `reqwest` stack already present.
- Dockerfile uses floating `rust:1-bookworm`; chart uses `:latest` + `imagePullPolicy: Always` — any push to GHCR auto-deploys. Pin by digest.
- Good: non-root, read-only rootfs, all caps dropped, resource limits, graceful shutdown, HTML escaping is correct, username charset is restricted before use as a key.

## 5. Sync engine, worker, fleet (`src-tauri/crates/rrcloud-core`)

### 5.1 [High] Fleet SSRF
`fleet.rs:223-242` builds an `S3Client` from each user's `sync.endpoint`/`bucket` with only an `is_empty()` check; `s3/client.rs:330-343` accepts any `http(s)://host:port`. The reference deployment runs the worker in-cluster next to Garage's admin API. A paired user sets `endpoint=http://garage-admin:3903` (or the kube API, or a metadata endpoint) and the worker issues signed GET/HEAD/PUT/POST/DELETE requests on `/<bucket>/<key>` each cycle; `bucket` may contain `/` for prefix control. Redirects are disabled, so it is blind. Enforce `https`, deny loopback/link-local/RFC1918 and the cluster DNS suffix, or allowlist endpoints in the pairing form and re-validate in `run_one_user`.

### 5.2 [Medium] GC trusts remote timestamps and version vectors
`compact.rs:729-751`: tombstone age is `now - tomb.server_ts`, with `server_ts` read from the attacker-writable tombstone JSON; the 30-day grace and the 14-day laggard cap both pass with `server_ts: 0`. Supersession (`compact.rs:1562-1617`) is defeated by a tombstone whose `vv` components are `u32::MAX`, and because `clock.rs:144-150` saturates, `restore_item` can never dominate it. `record_server_time` (`compact.rs:299-311`) also trusts the S3 `Date` header, so a hostile endpoint can age out everything and drive `auto_retire_sweep`. Net effect: a compromised peer deletes any object immediately and un-restorably, bypassing the design's stated safety net for exactly this case. Clamp `server_ts` to the object's `LastModified`/first-seen time, reject vv components beyond what the journal has shown for that device (or near `u32::MAX`), bound `Date` skew.

### 5.3 [Medium] Case-fold collision clobbers local originals
`RelKey` defends against trailing-dot, reserved-name and `.rr.` collisions (`keys.rs:151-185`) but not case variants. On macOS/Windows a hostile `put library/Photos/img_0001.nef` installs over the user's `IMG_0001.NEF` via `rename(partial, final)` (`transfer.rs:2241`) with no conflict event or loser copy. Add a case-fold (ideally NFKC-fold) collision check before installing over an existing path.

### 5.4 [Medium] Unbounded bodies and loops from a hostile server or peer
- `transfer.rs:2191-2208` appends the GET body with no check against `expected.size` → disk fill; sidecars are read fully into memory (`:2227-2236`) with no cap while `size` is remote-supplied → phone OOM.
- `worker.rs:1342-1367` and `:1265-1294` paginate with no page/key cap (unlike `reader.rs:408-458`); `request_timeout: None` with a 300 s idle timeout lets a trickling endpoint stall the sequential fleet loop (`fleet.rs:190-196`) indefinitely.
- `s3/client.rs:503, 643, 692, 796, 910, 984, 1047` buffer error/XML bodies uncapped before `quick_xml` parses them; `collect_capped` exists but is unused there.
- `quick-xml 0.38.4` carries RUSTSEC-2026-0195 (unbounded namespace allocation) and -0194 (quadratic attribute check): the parser is fed by the S3 server, so upgrade to ≥ 0.41.

### 5.5 Low
- Plaintext `http://` endpoints accepted everywhere (`client.rs:330-343`, pairing `normalize_discovery_url`). SigV4 protects the secret, but a MITM becomes a hostile bucket and can replay signed PUT/DELETE within the skew window. Allow `http` only for loopback or an explicit flag.
- `WorkerConfig` derives `Debug` with the plaintext secret (`worker.rs:241-256`); `S3Config` redacts. Match it.
- Version-vector saturation wedge: a dominating put with `vv[victim]=u32::MAX` makes the victim's future edits to that key never propagate (`clock.rs:155-163`, `engine.rs:2272-2279`); debug builds panic on `debug_assert`.
- Stub/partial writes follow pre-planted local symlinks (`transfer.rs:1914, 2163-2168`; `engine.rs:2106`); only a local writer can plant them. No symlink tests exist.
- A hostile manifest can create millions of 0-byte stubs/dirs inside the root (`transfer.rs:1887-1943`); manifest decode cap is 256 MiB but rows are then held twice in memory (`manifest.rs:440-502, 684`).
- Info: GC can publish both a live row and a `del` row for the same key (`compact.rs:816-826` vs `manifest.rs:343-346`).

## 6. Network, AI features, privacy

Remote endpoint inventory (what the app talks to and when):

| Host | Trigger | Data | Integrity / auth |
|---|---|---|---|
| `huggingface.co/CyberTimon/RapidRAW-Models` | First use of an AI feature | GET models | HTTPS + per-file SHA-256, re-verified on every load (`ai_processing.rs:370-450`). `clip_tokenizer.json` is **not** hash-checked (`:668`) |
| `www.getrapidraw.com/api/inpaint`, `/usage` | Generative Replace with cloud provider | ≤1.5 MP crop + mask + prompt | HTTPS, Clerk bearer |
| `clerk.getrapidraw.com` | **Every desktop launch** | Clerk handshake | HTTPS; state persisted via tauri-store |
| `api.github.com/.../releases/latest` | **Every library mount** | GET | HTTPS, no opt-out |
| `raw.githubusercontent.com/.../RapidRAW-Presets` | Community page | GET | HTTPS, unsigned manifest; `lutPath` from the manifest is opened as a local LUT (`lib.rs:1380-1381`) |
| `openstreetmap.org/export/embed.html` (iframe) | **Viewing metadata of a geotagged photo** | **lat/lon** | third party, no consent |
| `http://<ai_connector_address>/health,/inpaint,/upload_source` | Poll every 10 s; Generative Replace | full-frame mask; on 404 the **entire source image** at full res | **plaintext HTTP only, no auth, no timeout** |

- **[Medium] AI connector** (`ai_connector.rs:132`, `inpainting.rs:593`): scheme is hard-coded `http://`, address unvalidated, `token: None`, unbounded response bodies decoded with `image::load_from_memory`. On-path attackers read every photo the user edits with it. Support/require `https`, validate host:port, add timeouts and body caps.
- **[Medium, privacy] GPS to OSM** (`MetadataPanel.tsx:779`): load the map on click or render coordinates locally.
- **[Low] Startup calls with no toggle** (Clerk init, GitHub version check). Lazy-init Clerk on first cloud use; add an "offline mode" setting. No telemetry SDK exists.
- **[Low] Pairing discovery** (`sync/pairing.rs:66-82, 286`): `http://` accepted, `authorization_endpoint` from the discovered issuer is passed to `shell().open` from Rust (the JS-side scheme validator does not apply), and no `issuer` match is performed. Trust root is the URL the user typed, so impact is limited, but reject non-http(s) schemes and require https outside loopback.
- **[Low] Tethering** (`camera_tethering.rs:486-487`): `save_dir.join(camera_file_path.name)` with a device-supplied name; an absolute or `..` name from a malicious PTP device writes anywhere. Use `file_name()`.
- **[Info] Logging**: `app.log` (default umask, typically 0644) holds absolute library paths and all forwarded `console.*` output; no secrets found in any log statement (Rust or Kotlin).

## 7. Android

Manifest facts: only `MainActivity` exported; `rapidraw://auth-callback` custom scheme (no App Links verification); FileProvider and `SyncForegroundService` not exported; cleartext off in release, on in debug; `targetSdk 36`, `minSdk 24`; permissions limited to media-read, network state, FGS data-sync, notifications.

- **[Low] `allowBackup` defaults to true, no extraction rules**: `settings.json` (endpoint/bucket/connector address/paths) and redb state go to Google backup. The S3 secret itself is protected (Keystore master key is not backed up). Set `allowBackup=false` or add `dataExtractionRules`.
- **[Low] DCIM auto-import is attacker-feedable** (`DcimScanWorker.kt:103-273`): any app with media-write can plant files in the `Camera` bucket that get imported into the library and uploaded to the bucket; on API ≤ 28 `DISPLAY_NAME` is not sanitized by MediaStore so `../` escapes `DCIM-import/`. Cap size, reject separators.
- **[Low] Library root is `Android/media/<pkg>/.library`**, readable by other apps with media permission (originals, sidecars). Design trade-off for gallery visibility; document it.
- **[Info]** Deep-link hijack by a same-scheme app yields only a DoS: PKCE S256, random `state`, single-use in-memory `PENDING.take()` and strict equality (`pairing.rs:308-316`) reject forged callbacks. JNI bridge `unsafe` is confined to `from_raw` on `ndk_context` globals and documented `mem::forget` of global refs; pending exceptions are cleared. Keystore-backed `EncryptedSharedPreferences` (AES-256-GCM/SIV) for credentials; Kotlin never logs the credential JSON.

## 8. Supply chain, CI, dependencies

`cargo audit` (2026-10-10):

| Lockfile | Vulnerabilities | Notes |
|---|---|---|
| `src-tauri` | quick-xml 0.37.5 / 0.38.4 (RUSTSEC-2026-0194, -0195); rustls 0.23.43 (RUSTSEC-2026-0285, TLS 1.3 handshake boundary) | unmaintained: `paste`, `proc-macro-error`, `ttf-parser`, `unic-*`; unsound `glib 0.18.5`; yanked `chacha20 0.10.1` |
| `rrcloud-core` | quick-xml 0.38.4 (same two) | parser is fed by the S3 server → prioritize |
| `pairing` | quick-xml 0.32.0 (same two); rustls-webpki 0.101.7 (RUSTSEC-2026-0098/0099/0104) | both via `rust-s3`'s legacy hyper 0.14 stack |

`npm audit`: 0 production vulnerabilities; 1 high in dev-only `source-map-js` (GHSA-68fv-2mgg-jv7q).

Other supply-chain observations:
- `rawler` and `gphoto2` are git deps without `rev` in `src-tauri/Cargo.toml` (pinned only by `Cargo.lock`; a `cargo update` silently moves them). `rrcloud-core` pins `rev` correctly.
- ONNX runtime download in `build.rs` and Garage in `scripts/fetch-garage.sh` are SHA-256 pinned — good.
- Workflows: `pull_request` (not `_target`) for PR CI, Android signing gated on non-PR events, `tauri-action` pinned to a SHA, `permissions: contents: read` on `rrcloud.yml`. Gaps: `dtolnay/rust-toolchain@master` (mutable ref), most actions pinned to major tags not SHAs, `release.yml` passes `secrets: inherit` into the reusable workflow, `worker-image.yml` publishes from a stale `ccr-70e6b1a3-bux8cz` branch, images tagged `:latest` and pulled with `Always`.
- No secrets in the tree or history (Clerk `pk_live_` is a publishable key by design; `keystore.*` is gitignored).

## 9. What is done well

- Path mapping: `RelKey::new`/`parse_wire` reject backslash, colon (kills Windows ADS and drive-relative), controls, `.`/`..`/empty segments, trailing dot/space, Win32 reserved names (including superscript variants), and the engine's `.rr.` namespace; every local join goes through `local_path`; `DeviceId`, `ContentId`, `SemHash` are strictly canonical. Extensive tests in `tests/keys.rs`, `tests/transfer.rs`.
- No remote-driven local deletion: remote `del` only soft-hides; the only `remove_file` on a library path is user-initiated quarantine discard.
- Download integrity: blake3 over the full stream vs the advertised head, from-scratch retry before condemning, sidecar parse-validation before install, atomic same-dir rename + parent fsync, records without a hash refused, Range-ignoring backends detected. Upload integrity via Content-MD5 and read-back.
- Size caps on journal segments, manifests (gzip-bomb cap tested), tombstones, device entries, config docs; reader fails closed on version gates and body/filename mismatch.
- SigV4: per-segment URI encoding, `.`/`..` refused, URL re-parse must equal signed path, secret never formatted into any string, `S3Config` redacts in `Debug` (tested).
- TLS: no `danger_accept_invalid_certs`, redirects disabled, Android pins webpki roots with http/1.1 ALPN and the rationale documented.
- Credentials: never in `settings.json` or the webview; 0600 atomic file on desktop, Keystore on Android; PKCE S256 + state + single-use pending flow for pairing.
- Pairing service: username charset restricted before key construction, HTML escaped, secret never rendered, hardened pod security context.
- Models: SHA-256 pinned and re-verified on every load; `ORT_DYLIB_PATH` points at the bundled runtime.

## 10. Prioritized remediation roadmap

**Now (days)**
1. Fleet/pairing endpoint validation: require `https`, deny private/loopback/cluster hosts (§5.1).
2. Pairing: verify `X-Authentik-Jwt` or a middleware secret and fail closed; chart refuses ingress without forward-auth; NetworkPolicy (§4.1). Add `aud` check and key on `sub` (§4.2). Add CSRF token (§4.3).
3. Upgrade `quick-xml` ≥ 0.41 in all three lockfiles, `rustls` ≥ 0.23.45 (§8).
4. Make the OSM map click-to-load; add `https` support + timeouts + body caps to the AI connector (§6).

**Next (weeks)**
5. Enable a CSP and add a single library-root guard to every mutating IPC command; canonicalize in `remove_lut`; reject separators in rename templates; fix `show_in_finder` argument handling; validate the chrono format string (§3.1).
6. GC hardening: clamp `server_ts`, reject implausible vv components, bound `Date` skew (§5.2). Case-fold collision check before install (§5.3). Enforce `expected.size` on download, cap sidecar size, cap worker LIST loops, add per-user deadline in fleet (§5.4).
7. Android: `allowBackup=false` or extraction rules; cap and sanitize DCIM imports (§7).

**Later (hygiene)**
8. Pin `rawler`/`gphoto2` by `rev`; pin actions by SHA; pin images by digest; drop the stale branch from `worker-image.yml`; prune `secrets: inherit`.
9. `WorkerConfig` redacting `Debug`; `image` decode limits instead of `no_limits()`; hash-pin `clip_tokenizer.json`; `file_name()` on tethered capture names; lazy Clerk init and an offline toggle.
10. Consider encrypting per-user credentials in the admin bucket (§4.4) and documenting that bucket contents are plaintext to the storage operator.
