# RapidRawCloud — Cloud Sync Setup

RapidRawCloud is a Lightroom-mobile-style cloud-sync fork of
[RapidRAW](https://github.com/CyberTimon/RapidRAW). It adds offline-first
synchronization of your photo library — originals, smart previews, edit
sidecars, albums, and presets — across every device you run it on
(Android phone, desktop, and an optional headless worker).

The entire design goal is **"just buy a cheap S3 bucket."** There is no
RapidRawCloud server, no account system, and no vendor lock-in. All state
lives as plain objects in one S3 bucket, so the backend is any
S3-compatible object store:

- Amazon S3
- Backblaze B2 (S3-compatible endpoint)
- Cloudflare R2
- [Garage](https://garagehq.deuxfleurs.fr/) (self-hosted, what this repo
  is tested against)
- MinIO (self-hosted)

Everything below works with **manual configuration today** — three
coordinates and a key pair typed into the app. There is also an
**optional** zero-typing "pairing" convenience for self-hosters (see
[Optional: low-effort pairing](#optional-low-effort-pairing)); it is a
nicety, never a requirement — the bucket remains the single source of
truth.

---

## 1. What you need

1. **A bucket** on any S3-compatible provider. RapidRawCloud assumes it
   owns the bucket root (it writes `.rrcloud/…`, `library/…`, journals,
   previews, thumbnails). **Use a dedicated bucket**, one per library —
   don't point it at a bucket that holds other data.
2. **An access key / secret key** with read+write on that bucket.
3. **The S3 endpoint URL** and **region** of your provider.

That's it. No IAM roles, no STS, no bucket notifications, no server-side
compute, no object-lock or versioning — RapidRawCloud deliberately uses
only plain `GET`/`PUT`/`LIST`/`DELETE` so it is portable to the most
basic bucket you can rent.

### Addressing style

RapidRawCloud uses **path-style** addressing by default
(`https://endpoint/bucket/key`, not `https://bucket.endpoint/key`). This
is what single-host S3 gateways (Garage, MinIO, B2, R2) need, and AWS S3
accepts it too. Leave it as the default unless your provider specifically
requires virtual-host style.

---

## 2. Configure the app (manual setup)

The sync settings live in **Settings → the "Sync" tab**.

> **Finding the Sync tab:** on a phone the settings category row
> (General · Processing · Controls · Sync) is horizontally scrollable.
> "Sync" is the last tab and may be off the right edge — swipe the tab
> row left to reveal it.

Fill in, in this order:

1. **Endpoint** — e.g. `https://s3.us-west-002.backblazeb2.com`, or your
   Garage/MinIO URL. Include the scheme (`https://`).
2. **Bucket** — the dedicated bucket name.
3. **Region** — your provider's region string (`us-west-002`,
   `auto` for R2, `garage` for Garage, etc.).
4. Optionally adjust **Cache budget (GB)**, **Preview budget (GB)**,
   **Auto-watch DCIM**, **Watched buckets**, **Worker backfill** (see
   [§4](#4-what-the-knobs-do)).
5. Tap **Save & reconfigure**.

Then enter credentials (these are stored **locally only**, in the OS
keystore on Android / an OS-appropriate location on desktop — they are
**never** written to `settings.json` or uploaded):

6. Under **Credentials**, enter the **Access key** and **Secret key**,
   then tap **Save credentials**. The panel will then show
   *"Credentials are configured (stored locally, never shown)."*
7. Tick **Enable sync** and tap **Save & reconfigure** once more.

> **Gotcha — a library must be open:** enabling sync requires a library
> folder to be open first. If you toggle "Enable sync" from the launch
> screen you'll see *"cannot enable sync: no library folder is open."*
> Open (or continue) a library, then enable sync from Settings.

### Android permissions

For the phone to back up new camera shots automatically you must grant:

- **Photos and videos** (`READ_MEDIA_IMAGES`) — required for the DCIM
  watcher to see new RAW files. Without it the scan silently finds
  nothing.
- **Notifications** (`POST_NOTIFICATIONS`) — for sync/foreground-service
  status.

Grant them at first launch when prompted, or later in Android Settings →
Apps → RapidRAW → Permissions. (From a dev machine:
`adb shell pm grant io.github.CyberTimon.RapidRAW android.permission.READ_MEDIA_IMAGES`.)

---

## 3. Multiple devices

There is nothing extra to configure for multi-device sync. Point **every**
device (phone, laptop, the headless worker) at the **same bucket with the
same (or equivalently-scoped) credentials**, enable sync, and they
converge automatically.

Convergence is driven by per-document **version vectors**, not wall-clock
timestamps, so concurrent edits on two offline devices merge
deterministically and a device whose clock is wrong never wins or loses a
race because of it. Deletes are soft (tombstoned with a 30-day grace
window) so a device that was offline for a while never resurrects
already-deleted files or loses an edit it hadn't uploaded yet.

---

## 4. What the knobs do

| Setting | Meaning |
|---|---|
| **Enable sync** | Master on/off for this device. |
| **Endpoint / Bucket / Region** | S3 coordinates. |
| **Cache budget (GB)** | Max disk the device spends caching full-resolution **originals** pulled on demand. LRU-evicted; pinned items are never evicted. |
| **Preview budget (GB)** | Max disk for cached **smart previews** (always kept; see below). |
| **Auto-watch DCIM** | (Android) Watch the camera roll and auto-import new RAW shots into the library for backup. |
| **Watched buckets** | (Android) Comma-separated camera-roll *folder* names to watch (default: `Camera`). This is the on-device MediaStore album filter, unrelated to the S3 bucket. |
| **Worker backfill (this device)** | Let this device generate and upload missing smart previews for items it has locally (the same backfill the headless worker does). |

### Smart previews

RapidRawCloud never requires you to download full RAWs to browse or edit.
On import it generates a **smart preview** — a downscaled, scene-linear
"linear DNG" proxy — and keeps that cached everywhere. Editing the proxy
tracks editing the full-resolution file to within an imperceptible color
difference even under aggressive pushes, because the proxy is decoded
through the exact same develop path as the original, just at reduced
resolution. Full originals are fetched on demand (and cached within your
Cache budget) only when you actually need them — export, 1:1 zoom, etc.

---

## 5. Self-hosting Garage (optional)

Any S3-compatible store works; this repo is developed and tested against
[Garage](https://garagehq.deuxfleurs.fr/) because it is tiny, has no
external dependencies, and runs happily on a NAS, a free-tier box, or a
Raspberry Pi. See the Garage docs for a single-node install; the only
settings that matter for RapidRawCloud are a reachable `s3_api`
`api_bind_addr` and a `s3_region`.

Once Garage is up, provision a bucket and a scoped key (run the `garage`
CLI on any cluster node — distroless images have no shell, so invoke the
binary directly):

```sh
# Create a dedicated bucket for one library
garage bucket create rapidraw-cloud

# Create a key for it
garage key create rapidraw-cloud-owner
#   -> prints a Key ID (GK…) and a Secret key — copy both

# Grant that key read + write (+ owner, so it can manage the bucket)
garage bucket allow --read --write --owner rapidraw-cloud --key rapidraw-cloud-owner
```

Then point the app at:

- **Endpoint:** your Garage S3 URL (e.g. `https://garage.example.com`)
- **Bucket:** `rapidraw-cloud`
- **Region:** whatever your `s3_region` is (Garage's convention is
  `garage`)
- **Access key / Secret key:** the pair `garage key create` printed

### This repo's reference deployment

The maintainer's homelab runs Garage in Kubernetes (GitOps via
[`argo-things`](https://github.com/xerootg/argo-things), `configs/garage*`)
with the S3 API published through Traefik at
`https://garage.themissing.xyz` (region `garage`, path-style). A
`rapidraw-cloud` bucket and `rapidraw-cloud-owner` key are provisioned
there. The access/secret for it are **not** committed to this repo — ask
the maintainer, or mint your own with the commands above.

---

## 6. Optional: low-effort pairing (self-hosters)

Typing an endpoint, bucket, region, a `GK…` access key, and a 64-char
secret into a phone is the one genuinely tedious part of setup. For
self-hosters who already run [Authentik](https://goauthentik.io/), the
**pairing service** (`pairing/` in this repo,
`ghcr.io/<owner>/rrcloud-pairing`) removes it. The guiding idea is still
**"S3 is the config store"** — the service adds no new source of truth:

- One small **admin** bucket (e.g. `rapidraw-admin`) holds one config
  document per user at `users/<username>/config.json`:
  `{ sync: {…SyncSettings…}, credentials: { accessKeyId, secretAccessKey } }`.
- The service sits at a discovery URL (e.g. `https://rrc.themissing.xyz`):
  - **Browser** routes behind Authentik forward-auth. The first time you
    sign in, you paste *your own* library bucket's S3 coordinates once (in
    a browser, not on the phone); the service writes your `config.json`.
    Return visits show a "paired" page and let you edit the settings — your
    "knobs and dials" are managed here, in the cloud.
  - **App** route `/api/config` validates an OIDC/PKCE bearer token
    (Authentik) and returns your stored config so the app configures
    itself — zero typing on the device.
- **Bring your own bucket.** The service never creates buckets or mints
  credentials; it just remembers the ones you give it, so every device
  you pair *and* the headless worker can use them.

Deployment (this repo's reference homelab): the service runs in Kubernetes
via `argo-things` (`configs/rapidraw-pairing`, `apps/93-rapidraw-pairing`)
behind Traefik at `rrc.themissing.xyz`, with two Authentik applications —
an OAuth2/OIDC provider (public/PKCE, redirect `rapidraw://auth-callback`)
for the app and a forward-auth Proxy provider for the browser page. The
admin-bucket service key is supplied via a Kubernetes secret (see
`configs/rapidraw-pairing/secret.example.yaml`). The container image must
be public on GHCR (it is pulled without registry credentials).

This is purely a convenience layer. It is **not required** — the app is
fully functional with manual configuration (§2), and the admin bucket is
just another plain S3 bucket.

---

## 7. Building from source

The sync engine is a standalone Rust crate,
`src-tauri/crates/rrcloud-core` (its own `Cargo.lock`), consumed by the
desktop/Android app and by the headless worker.

### Prerequisites

- Rust toolchain **1.98** (pinned by `src-tauri/rust-toolchain.toml`;
  `rustup` installs it automatically).
- Node.js + `npm` (frontend + Tauri CLI).
- `src-tauri/build.rs` downloads the correct ONNX Runtime for your target
  automatically.

### Desktop

```sh
npm install
npm run tauri build        # or: npm run tauri dev
```

### Android

The authoritative build recipe is `.github/workflows/build.yml`; the
essentials:

```sh
# One-time: Android SDK + NDK r26d, and the Rust target on the PINNED toolchain
rustup target add aarch64-linux-android --toolchain 1.98-x86_64-unknown-linux-gnu

export ANDROID_HOME=/path/to/android-sdk
export ANDROID_NDK_HOME=$ANDROID_HOME/ndk/26.3.11579264   # r26d
export NDK_HOME=$ANDROID_NDK_HOME

# ort-sys must link the Android ONNX Runtime that build.rs fetches into
# src-tauri/libs/arm64-v8a/libonnxruntime.so — point the dependency at it:
export ORT_STRATEGY=manual
export ORT_SKIP_DOWNLOAD=1
export ORT_LIB_LOCATION=$PWD/src-tauri/libs/arm64-v8a

npx tauri android build --debug --target aarch64
# APK: src-tauri/gen/android/app/build/outputs/apk/universal/debug/app-universal-debug.apk
```

Install on a connected device: `adb install -r <that apk>`.

### Continuous integration

- **`rrcloud`** — fmt + clippy (`-D warnings`) + the full `rrcloud-core`
  test suite (spins up a real Garage v2.2.0 via
  `scripts/fetch-garage.sh`). Runs on the pinned 1.98 toolchain.
- **`worker-image`** — builds and publishes the headless worker image to
  `ghcr.io/xerootg/rrcloud-worker`.

---

## 8. Headless worker (optional)

`rrcloud-worker` is a server-side binary (deliberately depending only on
`rrcloud-core`, no app/Tauri code) that can run on a cheap always-on box
to do work a sometimes-offline phone can't guarantee:

- generate and upload missing smart previews (backfill),
- ingest from a watched bucket (server-side import path),
- run compaction / garbage collection of tombstones and orphaned blocks.

It is **optional** — the phone and desktop clients are fully capable on
their own. The image is published to `ghcr.io/xerootg/rrcloud-worker`
(public, so a free-tier box can pull without credentials); GitOps
manifests for running it on Kubernetes live in `argo-things`.

Modes: `--once` (one cycle, default), `--daemon --interval <dur>`, or
`--report` (stateless read-only). A single worker is configured for one
library via `RRCLOUD_ENDPOINT` / `RRCLOUD_BUCKET` / `RRCLOUD_REGION` /
`RRCLOUD_ACCESS_KEY` / `RRCLOUD_SECRET_KEY` and `RRCLOUD_STATE_DIR`.

**Fleet mode (`--fleet`)** pairs with the pairing service (§6): instead of
one library from env, it reads the **admin** bucket and runs one cycle per
paired user, against each user's own library with each user's own
credentials (a per-user sub-directory under `RRCLOUD_STATE_DIR`). Point it
at the admin bucket with a **read-only** admin key via
`RRCLOUD_ADMIN_BUCKET` / `RRCLOUD_ADMIN_ENDPOINT` / `RRCLOUD_ADMIN_REGION`
/ `RRCLOUD_ADMIN_ACCESS_KEY` / `RRCLOUD_ADMIN_SECRET_KEY`, e.g.
`rrcloud-worker --fleet --daemon --interval 1h`. Users who set
`workerBackfill: false` are skipped; one user's failure never aborts the
others.

---

## See also

- [`docs/ARCHITECTURE.md`](ARCHITECTURE.md) — the normative design (sync
  protocol, version vectors, journals/manifests, smart-preview fidelity,
  Android platform integration).
- [`docs/UPSTREAM_TOUCHES.md`](UPSTREAM_TOUCHES.md) — every upstream
  RapidRAW file this fork modifies, for rebase safety.
