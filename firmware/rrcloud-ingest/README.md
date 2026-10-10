# rrcloud-ingest — camera ingest dock firmware (ESP32-P4)

Plug a camera into the USB-A port of a [Waveshare ESP32-P4-WIFI6-POE-ETH](https://docs.waveshare.com/ESP32-P4-WIFI6-POE-ETH)
and every new photo is uploaded into your RapidRawCloud library bucket as a
**journaled original** — the same protocol documents a phone or desktop publishes
(`docs/ARCHITECTURE.md` §1–§2), so every other device sees the files on its next poll
and the headless worker backfills smart previews for them.

Supported cameras:

| Camera | USB mode | Driver |
|---|---|---|
| Nikon Z f, Z 7II (and other PTP/MTP bodies) | *MTP/PTP* | `components/rrc_ptp` — a USB Still Image Class (PTP) host driver written for this project |
| Sigma fp | *Mass Storage* | `espressif/usb_host_msc` + FATFS; the card is scanned under `DCIM/` |

The board runs on PoE (or USB-C), so the dock is a single cable on a shelf: Ethernet is
always on; Wi‑Fi 6 via the on-board ESP32‑C6 is optional.

## How it maps onto the sync protocol

This device is a **write-mostly participant**. It never materializes a library
locally and does not apply peer journals, which keeps it inside what a 32 MB device can
do honestly, while still being a first-class citizen of the bucket:

| Protocol element | What the dock does |
|---|---|
| Identity (§1.2) | Mints a UUIDv4 `device_id` once (NVS, outside the user config), platform `esp32` |
| Originals | `PUT library/<relkey>` with `Content-MD5` (single PUT ≤ 8 MiB, otherwise multipart with 8 MiB parts, each with `Content-MD5`); `x-amz-meta-rrc-device` tags our uploads |
| Verification (§2.4) | HEAD size check; single-part `ETag == MD5`; the backend digest probe (deliberately wrong `Content-MD5` must be rejected) decides whether multipart objects are additionally read back and re-hashed |
| Journal (§2.2) | One `{"v":1,…,"op":"put","kind":"original",…}` entry per upload with `blake3`, `content_id`, `size`, `mtime`, `vv:{self:1}`; entries are appended to flash **before** the upload is considered done, frozen into `<seq:016x>.v1.ndjson` segments (≤ 900 entries / 900 KiB) and PUT under `.rrcloud/v1/journal/<device_id>/`; a crash replays the byte-identical frozen file |
| Device registry (§2.2) | `.rrcloud/v1/devices/<id>.json` heartbeat every 15 min while a camera is attached, hourly otherwise, `last_seen_server_ts` from the S3 `Date` header |
| Manifest (§2.3) | `.rrcloud/v1/manifests/<id>.json.gz` (gzip NDJSON, stored-deflate) rebuilt from the on-flash ledger after every run that uploaded something — so a phone bootstrapping from manifests learns the dock's files, and the dock's own ledger is recoverable from the bucket |
| Compaction (§2.10) | Own prefix only: rule 1 (manifest cursor covers the segment) → rule 2 (manifest ≥ 24 h old and re-confirmed by HEAD/ETag) → rule 3 via the **14-day laggard cap**. The dock does not read peer registry entries, so the fast path is never taken; segments live ≥ 14 days |
| Deletions, conflicts, tombstones | Not produced. The dock never deletes or overwrites: an existing key with a different size is logged as a collision and skipped; an existing key with the same size written by another device is recorded as "already in library" |

`w`/`h` are omitted from the journal entries (the dock does not develop RAWs); the
worker's preview backfill fills them in as the architecture specifies for measured
dimensions.

Every journal/manifest/registry document the firmware emits is decoded by the real
Rust engine in `src-tauri/crates/rrcloud-core/tests/firmware_interop.rs` — the fixtures
there are produced by this firmware's host test-suite.

## Pairing ("fast association")

The dock has no browser, so instead of the app's `rapidraw://auth-callback` redirect it
uses the **OAuth 2.0 device-authorization grant (RFC 8628)** against the same pairing
service and Authentik provider (`docs/CLOUD_SETUP.md` §6):

1. `GET <service>/api/pairing-info` → issuer + public client id
2. `GET <issuer>/.well-known/openid-configuration` → `device_authorization_endpoint`, `token_endpoint`
3. `POST device_authorization_endpoint` → the web UI shows a **user code** and a verification link
4. The dock polls `token_endpoint` with `grant_type=urn:ietf:params:oauth:grant-type:device_code`
5. `GET <service>/api/config` with the bearer → endpoint / bucket / region / credentials are applied

**One-time Authentik prerequisite:** the brand must have a *device code flow* configured
(Flows → New flow, designation *Stage Configuration*, then System → Brands → *Default
code flow*). Authentik serves the endpoint at `/application/o/device/`; the firmware
falls back to that path when discovery does not advertise it. No change to the
`RapidRawCloud` OAuth2 provider is needed — device code is a grant on the same public
client.

Manual S3 configuration (endpoint, bucket, region, access key, write-only secret) remains
available in the UI for buckets without a pairing service.

## Web admin UI

`http://rrcloud-ingest.local/` (mDNS) or the IP from your DHCP server. Shows camera,
cloud, network and upload progress; configures what to sync:

- **Include / exclude patterns** — case-insensitive globs (`*`, `**`, `?`, `[abc]`).
  Default `*.nef *.nrw *.dng *.jpg *.jpeg *.heif *.hif *.tif *.tiff`; a *videos* toggle adds `*.mov *.mp4`.
- **Library path template** — default `Camera Import/{model}/{yyyy}/{mm}/{dd}/{name}`;
  placeholders `{name} {stem} {ext} {path} {model} {serial} {yyyy} {mm} {dd} {hh}`.
  Output is sanitized to the §1.1 relkey rules (no `\` `:`, control chars, reserved names, `.rr.` segments).
- **Auto sync on plug-in**, minimum size, mass-storage scan folder, device name, hostname,
  optional Wi‑Fi credentials, optional Basic-auth password for the page.
- JSON API under `/api/…` (see `main/web.c`). State-changing calls must send `Content-Type: application/json`;
  that requirement is the CSRF gate (an HTML form cannot send it without a CORS preflight the dock never approves).

Secrets are write-only: the S3 secret, Wi‑Fi and admin passwords are stored in NVS and
never returned by the API.

## Building and flashing

Requirements: ESP-IDF **v5.5** (`idf.py`), target `esp32p4`. Managed components
(`usb_host_msc`, `esp_wifi_remote`/`esp_hosted`, `littlefs`, `mdns`) are fetched by the
component manager on first build.

```sh
cd firmware/rrcloud-ingest
idf.py set-target esp32p4
idf.py build
idf.py -p /dev/ttyACM0 flash monitor      # USB-C console port (CH343)
```

Board facts baked into `sdkconfig.defaults` / `main/board.h`: 32 MB flash, 32 MB PSRAM
(hex, 200 MHz), IP101GRI RMII PHY (addr 1, MDC 31, MDIO 52, RST 51, REF_CLK in on 50),
ESP‑Hosted SDIO to the C6 on slot 1 (CLK 18, CMD 19, D0–D3 14–17, reset 54). Partition
table: two 4 MB OTA slots + 8 MB LittleFS (`storage`) for the ledger and frozen segments.

Wi‑Fi requires the factory ESP‑Hosted slave firmware on the ESP32‑C6 (Waveshare ships it);
Ethernet works regardless.

## Host tests

The pure components (hashing, SigV4, protocol codec, glob, PTP codec) build with plain
gcc and are verified against independent references:

```sh
cd firmware/rrcloud-ingest/host_tests
cmake -S . -B build && cmake --build build && ctest --test-dir build --output-on-failure
```

- BLAKE3 against the `blake3` Python package (`tools/gen_blake3_vectors.py`)
- SigV4 against botocore's `S3SigV4Auth` for the exact request shapes the firmware sends (`tools/gen_sigv4_vectors.py`)
- gzip output decoded by Python's stdlib
- `test_proto <out.gz> <fixtures-dir>` regenerates the fixtures consumed by the Rust interop test:
  `cargo test -p rrcloud-core --test firmware_interop` (run from `src-tauri/crates/rrcloud-core`)

## Layout

```
components/rrc_hash     SHA-256, MD5, HMAC, CRC-32, Base64 (pure C)
components/rrc_blake3   portable BLAKE3 (pure C)
components/rrc_s3       SigV4 signer (pure) + S3 client over esp_http_client (PUT/HEAD/GET/DELETE/multipart/probe)
components/rrcloud_proto the generated protocol SDK (protocol/gen/c) as an IDF component
components/rrc_proto    firmware conveniences over it: relkey sanitizer, templates, stored-deflate gzip, ledger codec
components/rrc_glob     case-insensitive glob matcher
components/rrc_ptp      PTP codec (pure) + USB still-image-class host driver
main/                   app_config (NVS), store (LittleFS ledger/segments), net, sync engine, camera sources, pairing, web UI
www/index.html          the admin page (embedded)
host_tests/             gcc/ctest suite + reference vectors + Rust interop fixtures
```

## Serial console

The USB-C console port is also a command line (115200 baud; `idf.py monitor`, `screen`,
PuTTY — anything that sends keystrokes). Type `help`. It is the fallback when the web UI
cannot be reached, and it is enough to configure and test a sync without it:

```
rrc> set s3_endpoint https://garage.example
rrc> set s3_bucket my-photos
rrc> set s3_region garage
rrc> set s3_access_key GK...
rrc> set s3_secret_key ...
rrc> status
rrc> sync
rrc> net            # addresses + lwIP packet counters
rrc> log            # the web UI's log panel
```

`set` takes any key that `config` prints, plus `s3_secret_key`, `wifi_password` and
`admin_password`; booleans accept `on`/`off`, `true`/`false`. `pair <url>` runs the
device-flow pairing and `pair-status` shows the code to enter.

## Troubleshooting the web UI

Eight seconds after boot the dock connects to its own port 80 over loopback and logs the
result (`web self-test over loopback: HTTP/1.1 200 OK`): a pass proves the server listens,
accepts and answers. While no outside client has ever connected it then logs the lwIP
packet counters once a minute. Try to open the page and watch `tcp rx`: if it does not
grow, your SYNs are not reaching the dock and the problem is on the network path.

When the dock gets an address it prints the URL on the console and in the log panel:

```
I rrc: ethernet ip 192.168.128.131 — web ui: http://192.168.128.131/ (http://rrcloud-ingest.local/)
```

Every accepted connection and request is logged (`I web: http: connection from 192.168.128.20`,
`I web: http: GET /api/status`), so the console settles the first question when a browser
times out: **did any packet reach the dock?**

| Console shows | Meaning | What to do |
|---|---|---|
| link up, an IP, SNTP synced, but no `http: connection from` line while the browser spins | The TCP SYN (or the dock's SYN-ACK) never made it. Typical in a segmented homelab: the dock landed in another VLAN and a *stateless* switch ACL drops the return traffic, or client isolation is on. DHCP, DNS and SNTP are UDP and unaffected, which is why the rest of the log looks healthy. | `ping` the address, then try from a device in the dock's own subnet. Check inter-VLAN rules (stateless L2/L3 switch ACLs need the reverse rule; a gateway/firewall rule is stateful). Confirm the browser is not forcing `https://`. |
| `http: connection from …` but no request line | The TCP connection opened and the browser sent nothing we parsed — almost always a TLS ClientHello to port 80 (an HTTPS-only browser setting). | Open `http://` explicitly. |
| request lines appear, page still blank | The HTML loaded but an `/api/*` call failed. | The log panel is `/api/log`; open `/api/status` directly in the browser. |
| no IP line at all, `ethernet link up` missing | No link partner: cable, switch port, or the PoE injector's data pass-through. | The RMII pins are fixed by the board (MDC 31, MDIO 52, PHY reset 51, REF_CLK in on 50). |

`rrcloud-ingest.local` needs an mDNS resolver on the client (macOS, iOS, Windows 10+, Linux
with Avahi); on Android use the IP address.

Wi‑Fi: the ESP32‑C6 co-processor speaks ESP-Hosted over SDIO. A boot line such as
`eh_init_evt: major version mismatch — OTA coprocessor from host` means the C6's slave
firmware is older than the host library (3.0.x); Wi‑Fi stays unavailable until the C6 is
reflashed with a matching ESP-Hosted slave image (its UART is on the 4-pin header). Ethernet
is unaffected.

## Troubleshooting camera detection

Watch the serial console (`idf.py monitor`, 115200 baud on the Type-C UART port) or the
**Log** panel of the web UI — every `E`/`W`/`I` line of the USB host stack is mirrored
there. With **Verbose USB host logging** on (the pre-release default; Device section of
the UI) the stack's `HUB`, `HCD DWC`, `USBH`, `ENUM` and `USB HOST` tags log at DEBUG on
the serial console, so a plug-in shows the connection, debounce, reset, speed negotiation
and every descriptor fetch.

What a healthy plug-in looks like:

```
D HUB: Root port reset
I usb: addr 1 high-speed 1003:c432 usb2.00 class 00/00/00 "SIGMA" "SIGMA fp" sn "…"
I usb:   intf 0: class 08/06/50, 2 endpoints (mass storage)
I rrc: camera attached: SIGMA fp via MSC
```

Every enumerated device is dumped like this whether or not a class driver claims it, so a
camera in the wrong USB mode is still visible (a Nikon exposing `06/01/01` is PTP; a
Sigma fp exposing `08/06/50` is mass storage; `ff/..` with three endpoints is MTP-shaped).

| Symptom | Meaning | What to do |
|---|---|---|
| `E HUB: Root port reset failed` and nothing else | The port saw a connection but the device did not come back enabled after the bus reset (it dropped off, or never answered the speed handshake). In the IDF host library this is terminal until the device is unplugged or the port is power-cycled. | The dock now power-cycles the root port itself whenever nothing has enumerated for 15 s, and **Retry USB** in the web UI does it on demand; check that the camera is **on** and that its USB mode is right (Sigma fp: *System → USB Mode → Mass Storage*; Nikon Z: the camera must not be in *Charging only*), then try a different cable (some USB-C cables are power-only). If it never enumerates, capture the DEBUG serial log around the plug-in — the `HCD DWC` lines say which state the port fell out of. |
| Device dumped, no `camera attached` | Enumerated, but neither driver matched the interface classes. | Compare the `intf` lines with the table above and switch the camera's USB mode. |
| `W rrc_ptp: … is not a PTP device` at DEBUG only | Normal for the mass-storage path (the MSC driver owns it). | — |
| `I usb: addr … full-speed` for a Sigma fp | The card reader fell back to USB 1.1 (12 Mbit/s): a bad cable or hub. | Use a direct USB 2.0 cable. |

The root-port timings are set above the IDF defaults in `sdkconfig.defaults`
(debounce 500 ms, reset hold 50 ms, recovery 100 ms) because cameras are slow to settle
compared with flash drives.

## Known limitations

- Write-only participant: deletions made elsewhere are not observed. A file already
  imported is never re-uploaded (the ledger remembers `(camera, path, size)`), so deleting
  it in the library does not resurrect it from the dock. A `(camera, path, size)` reused
  for a different file (card re-formatted, same numbering) uploads under the same key
  only when the previous object is gone; otherwise it is logged as a collision.
- Camera clocks are treated as UTC for `{yyyy}/{mm}/{dd}` and `mtime` (PTP `CaptureDate`
  carries no zone; FAT timestamps are local time).
- Objects ≥ 4 GiB (PTP reports `0xFFFFFFFF`) are skipped.
- One camera at a time; USB hubs are not supported. A camera re-attached while the previous session is still
  being torn down is queued and attached once the sync task has released the old source.
- A vendor-specific (`0xFF`) interface with three endpoints is treated as MTP-shaped and
  claimed as a camera; a non-camera device with that shape fails at `OpenSession` and is
  logged, nothing else happens to it.
