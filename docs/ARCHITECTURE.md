# RapidRawCloud — Architecture Design (final, post-adversarial-review)

Cloud library sync fork of RapidRAW (`xerootg/RapidRawCloud`, forked from `CyberTimon/RapidRAW` at `a82251f`, v1.6.4). Android-first, Lightroom-mobile-style: previews local by policy, originals on demand, offline editing, plain-S3 backend.

Assumptions locked by the owner: plain S3 bucket, any implementation (AWS/B2/R2/Garage), path-style, no notifications/versioning/object-lock/STS/server-compute, no reliance on conditional PUT, tolerant of eventually-consistent LIST. Sync engine in shared Rust core. **Single user per bucket, multiple devices** — no multi-tenant concerns; access control is "whoever holds the scoped key owns the library."

This revision incorporates the adversarial review. The headline protocol changes: per-key **version vectors** replace scalar Lamport LWW (§2.6); **soft delete with a grace window** replaces immediate data-key deletion (§2.7); **per-writer immutable manifests that carry deletions** replace the single shared `manifest.json` (§2.3); **real upload integrity** (per-part Content-MD5 + attestation-gated eviction) replaces size-only verification (§2.4, §3.5); the **worker requires durable state** (§6); and the **Android background model is restated honestly** for Android 12–15 (§5.1). Appendix A gives the disposition of every review finding.

---

## 1. Bucket layout

### 1.1 Library-relative key mapping

RapidRAW embeds absolute paths everywhere (albums.json, settings, thumbnail cache key, `lutPath`). The cloud namespace is **library-relative**:

- **Sync root** = the library root: Android `getExternalMediaDirs()[0]/.library` (from `get_android_internal_library_root()`, `src-tauri/src/android_integration.rs`); desktop a user-chosen folder (default `app_data_dir/library` per `get_or_create_internal_library_root`, `file_management.rs:3142`).
- `relkey(path)` = path relative to sync root, `/` separators, Unicode NFC-normalized, no leading slash. Keys containing `\`, control chars, or `..` segments are rejected at the mapping layer. Reverse mapping joins onto the local root. Virtual-copy *virtual paths* (`<abs>?vc=<6hex>`) never appear as keys; only their sidecar files (`<name>.<6hex>.rrdata`) do.

### 1.2 Key schema

The `library/` prefix is a **byte-faithful mirror of the on-disk library tree**. Deliberate consequence: `rclone sync s3:bucket/library ~/photos` produces a working RapidRAW library, and desktop users can point the desktop build (or rclone) at the same bucket. Control plane lives under `.rrcloud/` — dot-prefixed, so `scan_dir_lazy` (which skips dot-entries) ignores it if the bucket is ever FUSE-mounted into a library.

| Key | Content |
|---|---|
| `library/<relpath>` | Originals (RAW/JPEG/derived outputs) and `.xmp` files, byte-identical to local |
| `library/<relpath>.rrdata` | Primary sidecar (whole-document JSON, may be multi-MB) |
| `library/<relpath'>.<6hex>.rrdata` | Virtual-copy sidecars (incl. conflict losers; conflict-loser suffixes are **deterministic**, §2.6) |
| `library/<stem>.conflict-<blake3[..6]>.<ext>` | Displaced original bytes from a concurrent original overwrite (§2.8) |
| `.rrcloud/v1/journal/<device_id>/<seq:016x>.v1.ndjson` | Append-only per-device journal segments (§2.2); format version in the filename (§2.2 schema evolution) |
| `.rrcloud/v1/manifests/<device_id>.json.gz` | **Per-writer** manifest snapshot (gzip NDJSON), single-writer per key (§2.3) |
| `.rrcloud/v1/devices/<device_id>.json` | Device registry entry: `{name, platform, created, last_seen_server_ts, applied: {device: seq}, proto: {read:[1], write:1}}` |
| `.rrcloud/v1/devices/<device_id>.retired` | Retirement marker (explicit from UI, or auto by GC; §2.10) |
| `.rrcloud/v1/tombstones/<blake3(relkey)[..32]>.json` | Deletion markers: `{relkey, vv, device, server_ts, kinds:["original","sidecar",...]}` |
| `.rrcloud/v1/previews/<content_id>.pxy.dng` | Smart preview: linear LJPEG DNG proxy (§4) |
| `.rrcloud/v1/thumbs/<content_id>_small.jpg`, `_medium.jpg` | 480/1280px JPEG thumbs, q75, matching `encode_thumbnail` output |
| `.rrcloud/v1/thumbpacks/<blake3(folder relkey)[..16]>.tar` | Optional worker-produced per-folder packs of `_small` thumbs (bootstrap accelerator, §4.3) |
| `.rrcloud/v1/meta/albums.json`, `meta/presets.json` | Relativized albums + presets docs (§2.9) |

`content_id` = lowercase hex `blake3(original file bytes)` (full 64 hex; prefix-free). Keying previews/thumbs by content, not path, means renames/moves never regenerate previews; the journal maps `relkey → content_id`.

`device_id` = UUIDv4 minted on first sync setup, persisted in the sync state DB (not in settings.json, so a settings wipe doesn't fork identity). **Every journaling participant — including the headless worker — must have durable local state** (§6); the engine refuses to enter write mode without a persistent state directory.

**Not synced v1:** `app_cache_dir` (thumbnail cache, EXIF cache), LUT files and `adjustments.lutPath` (stripped from semantic hash; a synced sidecar referencing a LUT renders without it on other devices — documented limitation), `.rrexif` legacy sidecars (migrated-and-deleted on read upstream; the engine ignores them), window state, AI models.

---

## 2. Sync protocol over plain S3

### 2.1 Principles that make it safe

1. **S3 objects are immutable facts; keys are the unit of replacement.** Every PUT is a whole object. No appends, no renames. Every shared-namespace key is **single-writer** (journal prefixes, manifests, device entries) or **idempotent/commutative under concurrent write** (tombstones, deterministic conflict copies). There is no multi-writer mutable key anywhere in the schema.
2. **Journal entries, not LIST, are the authoritative change feed.** LIST is used only (a) to page journal/manifest prefixes, (b) for periodic reconciliation, which is *additive-only* (it may discover objects, never conclude absence).
3. **Destruction requires a durable protocol record, never absence.** Data keys are deleted only by GC acting on a tombstone that has passed the horizon and grace rules of §2.10. Journal segments are deleted only after the owner's own manifest durably covers them. Tombstones are deleted only after they are folded into the GC runner's manifest **deleted set**, which is retained for 12 months. A stale LIST can therefore delay discovery or delay GC, never cause destruction or resurrection.
4. **Causality via per-key version vectors, never wall clock.** Wall-clock time appears in exactly two places, both bounded in blast radius: (a) tie-breaking the winner among *provably concurrent* sidecar edits, where the loser is always preserved as a virtual copy (§2.6); (b) age thresholds for GC/compaction, which are measured in **server time** (the `Date` header of S3 responses), not device clocks (§2.10).
5. **Durability order differs by plane.**
   - *Data objects:* object PUT → verify (§2.4) → journal entry → local DB commit. A crash leaves either an idempotently re-uploadable object or an unjournaled object that reconciliation/worker adopts.
   - *Journal segments:* serialize segment → **commit the exact segment bytes + seq to redb** → PUT → commit published cursor. Crash replay re-PUTs the byte-identical persisted segment, so a reader that saw the first PUT and deduped by `(device, seq)` cannot diverge. (This is the reverse order from data objects, deliberately: segment content must be frozen before publication.)

### 2.2 Journal

Each device appends to its own prefix only — single-writer per prefix, so no PUT races, no need for conditional PUT. (Opportunistic `If-None-Match: *` is sent on segment PUT where supported, purely as a tripwire for duplicated device ids; precondition failure logs loudly but the design doesn't depend on it.)

**Segment** = NDJSON, one entry per line, ≤1000 entries or 1 MiB, flushed when the engine's outbound queue drains or every 30 s while dirty. Segment key uses the device's monotonic `seq` of the first entry and carries the format version in the filename (`<seq:016x>.v1.ndjson`).

**Entry schema (v1):**

```json
{"v":1, "seq":412, "ts":1769900000, "device":"d1f0…", "op":"put",
 "kind":"sidecar", "key":"library/2026/10/IMG_0042.NEF.rrdata",
 "size":48213, "blake3":"<hash of uploaded bytes>", "sem_hash":"<semantic hash>",
 "vv":{"d1f0…":9,"a3b2…":4}, "rating":3, "color_label":"red",
 "content_id":null, "w":null, "h":null, "mtime":1769899000}
```

- `op`: `put` | `del` | `move` (with `from_key`) | `attest`. `kind`: `original` | `sidecar` | `xmp` | `preview` | `thumb` | `thumbpack` | `albums` | `presets`.
- **`vv` is a per-relkey version vector** `{device_id: counter}` maintained for every relkey regardless of kind; `put`, `del`, and `move` all bump the author's component (§2.6). `xmp` entries carry the vv of the sidecar version they project (§2.8).
- `sidecar` entries additionally carry `sem_hash` (§2.5) plus **`rating` and `color_label`**, so the grid can render correct badges before the sidecar bytes have downloaded (§3.5).
- `original` entries carry `content_id`, `w`, `h`, `mtime`. `w`/`h` are the **final displayed dimensions measured from the actual develop at proxy-generation or import time** (post-develop, post-`CropDefault`, post-orientation) — never EXIF-derived — so `proxy_scale` in §4.4 is computed from two numbers with identical provenance.
- `attest` entries record that a device fully downloaded an advertised object and verified its blake3. They carry the **full v1 envelope** (`v`, `seq`, `ts`, `device`, `op`, `kind`, `key`, `vv`) plus `blake3` — not a reduced `{key, blake3, device}` shape — so every v1 reader decodes every entry with one schema. The attest's `vv` snapshots the attesting device's current version vector for the key (i.e. the version whose bytes it verified), which lets the eviction gate check that an attestation covers the version being evicted. The worker emits one per original it GETs for proxy generation; any client that hydrates an original emits one. Attestations gate LRU eviction (§3.5).
- `seq` is a per-device monotonic counter persisted transactionally in redb with the serialized segment bytes (§2.1.5); it never regresses across process death, and a segment republished after a crash is byte-identical by construction.

**Schema evolution (min-reader rule).** Entries carry `v`; `devices/<id>.json` advertises `proto: {read:[1], write:1}`. A writer may emit `v:2` entries (or a `.v2.ndjson` segment) only when **every active device** advertises read support for 2. A reader that encounters an entry or segment version it cannot read **halts applying that prefix and surfaces "app update required"** — fail-closed, never skip-and-diverge. For a single-user fleet this is a UI nudge, not an outage: data objects keep flowing; only feed application pauses.

**Steady-state cost:** each device polls with a **single** `ListObjectsV2` of `.rrcloud/v1/journal/` (no delimiter, paged) — after compaction the total segment count across all devices is small, so this is one near-empty page per poll, not O(#devices) calls. Poll interval: 60 s while the app is foreground and the user is active, backing off to 5 min idle; immediate on `Notify`. New segments are GET-ed; entries applied idempotently (apply keyed by `(device, seq)`, recorded in redb; duplicates no-op). Heartbeat PUT of `devices/<id>.json`: 15 min active, 1 h idle. On B2 this is ~1.5k class-C transactions/device/day ≈ $0.006/day — negligible, and stated rather than asserted.

**Compaction** is specified in §2.10 (it interacts with manifests, horizons, and GC and is defined once, there).

### 2.3 Manifests and reconciliation

**Per-writer manifests.** `manifests/<device_id>.json.gz` (gzip NDJSON from v1 — a 50k library is ~100k+ rows; plain JSON does not survive contact with a phone): header row `{written_server_ts, cursors: {device: seq}, proto:1}`, then one row per **live** key `{key, kind, size, blake3, sem_hash, vv, device, content_id, w, h, mtime, rating, color_label}`, then one row per **deleted** key `{del: relkey, vv, server_ts}` — the **deleted set**, retained for 12 months after deletion. Each manifest is written only by its owning device (single-writer, same invariant as journal prefixes), so the multi-writer lost-update on the old shared `manifest.json` is gone by construction.

**Bootstrap / catch-up** = GET all manifests, merge their rows (rows are keyed by relkey and ordered by vv exactly like journal entries — merging manifests is the same idempotent apply operation as replaying the journal), then apply all journal segments newer than the merged cursors. Because manifests carry the deleted set, a device bootstrapping or catching up from manifests **does learn deletions** — this closes both resurrection paths of review finding A3 (a new device the compactor's LIST missed, and a stale device returning after tombstone GC).

**Full reconcile** — the only O(library) operation:

1. `ListObjectsV2` page through `library/` and `.rrcloud/v1/previews|thumbs|tombstones/`.
2. For each listed object with no journal-known state: adopt it as **foreign** (§6 worker does blake3 + preview backfill; clients just create stubs keyed by ETag+size until a journal entry upgrades them).
3. For each journal-known key *not* in the LIST: HEAD it (strongly consistent on all four targets).
   - 404 + tombstone or deleted-set entry → confirmed deleted (local copy goes to local trash, §2.7).
   - 404 + no tombstone **and** no deleted-set entry in any manifest → re-upload **only if** our local copy's blake3 matches the journal-known head; otherwise flag `missing` (surfaced in UI). Never silently drop.
   - **Pre-upload quarantine rule:** a device whose applied cursor predates the 12-month deleted-set retention window (i.e., it cannot prove a key wasn't deleted) must not auto-re-upload local-only files; it quarantines them behind a user prompt ("These N files no longer exist in the cloud library — restore or discard?"). This converts the mass-resurrection failure of a long-dormant device into an explicit user decision.
   - 200 but blake3/size inconsistent with the journal head → `corrupt_remote` state (§2.4), never eviction, never silent adoption.

Who reconciles: clients only on first bootstrap or manual "verify library" (phones never do it on a schedule); a desktop with `worker_backfill=true` on its own schedule; the worker on every run.

### 2.4 Upload state machine (per item)

States stored in redb, transitions crash-safe (every transition is one redb commit):

| State | Meaning / transition |
|---|---|
| `dirty` | Local change detected (chokepoint save, import, derived output). → `queued` by debouncer (§3.7). |
| `queued` | In upload queue. → `uploading` when a transfer slot (semaphore) acquires it. |
| `uploading` | Single PUT (<16 MiB) **with `Content-MD5`**, or multipart. Multipart: `CreateMultipartUpload` → store `{upload_id, part_size}`; each `UploadPart` is streamed with **`Content-MD5` computed over the streamed bytes** (the server verifies and rejects corrupt parts with `BadDigest` — this is the integrity backbone); a running **blake3 over the exact streamed bytes** is computed concurrently, so the journal's `blake3` is the hash of what was actually sent, by construction (not a separate read of a file that may have changed). Each part completion stores `{part_no, etag, md5}` in redb; → `verifying` after `CompleteMultipartUpload` with the recorded part list. On process death: resume from stored `upload_id` + recorded parts (re-upload any part lacking a stored ETag; `ListParts` used opportunistically to confirm). If the source file changed during upload (size/mtime recheck at completion; rehash on suspicion — and a same-size-same-mtime rewrite is caught by the streamed-hash design, since the journal records what was sent and the next chokepoint save re-marks the item `dirty`): complete the upload of the old version, journal it, and immediately queue the new version. Stale upload hygiene: engine aborts its own `upload_id`s older than 7 days; the worker's scan calls `ListMultipartUploads` + `AbortMultipartUpload` as the portable mechanism on every run. **The design does not rely on lifecycle rules anywhere** (Garage v2.2 lifecycle support is unverified; `ListMultipartUploads` conformance is proven by the §8 harness on Garage/MinIO and manually on B2/R2). |
| `verifying` | HEAD the key; confirm size; single-part: ETag == our MD5. Multipart: all parts were server-MD5-verified at receipt and the Complete used the recorded part list, so content integrity holds if the backend honors `Content-MD5`. **Backend probe at setup:** the engine uploads a small object with a deliberately wrong `Content-MD5` and expects `400 BadDigest`; a backend that accepts it sets `requires_readback_verify`, in which case `verifying` performs a full ranged-GET re-hash before passing. → write journal entry (sets `verified_remote=true`) → `synced`. |
| `synced` | Local and remote agree (`blake3`/`sem_hash` match journal head). `verified_remote=true`. **Eviction additionally requires attestation or read-back (§3.5).** |
| `corrupt_remote` | The advertised object exists but its content is wrong (hash mismatch on download, attestation mismatch, or reconcile inconsistency). Never evicted, never served. Repair: any device holding verified local bytes re-uploads (bumping nothing — same vv, same blake3 target); the worker flags and repairs on its runs; UI surfaces a persistent error until repaired. This is also the bit-rot answer. |
| `conflict` | §2.6. |

Part size 16 MiB (≥ AWS/B2/R2 5 MiB minimum; a 60 MB RAW is 4 parts). Concurrency: 2 transfers on Android, 4 desktop/worker (tokio `Semaphore`).

### 2.5 Change detection — semantic hashing

The sidecar is rewritten by EXIF caching, auto-heal, and XMP import *without* user intent (research report §1.1 write-site table). Therefore:

`sem_hash = blake3(canonical_json({rating, tags: sorted, adjustments'}))` where `adjustments'` = adjustments with `lutPath` removed and `Null` normalized; the `exif` field and `version` are **excluded**. Canonical JSON = serde_json with sorted keys (serialize via `BTreeMap` re-parse).

The save chokepoint (§3.4) computes `sem_hash` on every write; only a changed `sem_hash` marks `dirty`. EXIF-cache/auto-heal rewrites change file bytes but not `sem_hash` → no upload, no version bump, no churn. The same test is applied to *downloaded* sidecars before overwriting local state.

**XMP import is a real content change and is handled as one** (correcting the original design's false claim that it caused zero uploads): `sync_metadata_from_xmp` imports rating and merges tags, which changes `sem_hash`. The fix is to make it fire **once per actual external XMP change, not per listing**: the engine tracks `blake3(xmp bytes)` per path in redb, and the import path is gated on that hash changing. A genuinely edited XMP (from another tool — the interop XMP exists for) then syncs as a legitimate edit; a background folder listing re-reading an unchanged XMP produces zero writes and zero uploads. The chokepoint records the write origin (`UserEdit | XmpImport | Automated`) for logging/UI, but ordering does not special-case origins — an external XMP edit is an edit.

Originals: identity = `blake3(bytes)`; cheap pre-check by `(size, mtime)` against redb, rehash only on mismatch.

### 2.6 Conflict detection and resolution (version vectors)

Per-relkey state in redb: `{vv: {device: u32}, sem_hash, dirty: bool}`. The vv is the device's knowledge of the key's version history.

- **Local commit:** local saves do **not** bump the vv (they collapse); when the upload queue *admits* a sidecar version (§3.7 quiescence), the engine bumps its own component (`vv[self] += 1`) and snapshots the vv into the journal entry. One admitted upload = one version. (This also removes the "40 slider tweaks = 40 version bumps" inflation of the old Lamport scheme — a device's counter advances per synced version, not per keystroke.)
- **Ordering:** for entries/states a, b on the same key: `a ≥ b` iff `∀d: a.vv[d] ≥ b.vv[d]` (missing = 0). `a` and `b` are **concurrent** iff neither dominates. This replaces the old — and, as the review correctly noted, unfinished — detection predicate with an exact one.
- **Unified apply rule — every arriving entry is ordered against the local head; there is no separate fast-forward path that bypasses resolution** (fixing review B1):
  1. `remote.vv == local.vv` or `remote.sem_hash == local.sem_hash` → converged; adopt metadata.
  2. `remote.vv > local.vv` → remote is a descendant; adopt it (download, parse-validate, atomic replace). If the local copy had *uncommitted dirty edits* (dirty=true but not yet admitted), first commit them as a local version (bump `vv[self]`), which makes the comparison fall into case 4.
  3. `remote.vv < local.vv` → remote is an ancestor; ignore (our version supersedes it; it will reach the other side via our journal entry).
  4. **Concurrent** → conflict. Deterministic winner among the concurrent pair: higher `ts`; tie → lexicographically greater `device_id`. Wall-clock `ts` is used **only here**, only to pick which of two preserved versions is primary; clock skew can pick the "wrong" primary but can never destroy anything (the loser survives as a virtual copy) and never gates deletion. This matches the "latest edit wins" user expectation far better than counting saves. After resolution, the key's vv ← elementwise max of both vvs, so the next edit on either device dominates both branches and the conflict cannot reopen.
- **Loser materialization — deterministic and single-copy** (fixing review B2): the loser is written to `<file>.<6hex>.rrdata` where **`6hex = blake3(canonical loser document)[..6]`** — the same suffix on every device, satisfying `list_images_in_dir`'s 6-lowercase-hex recognition. Any device that *holds the loser bytes* (its author, and any device that had downloaded it before learning of the winner) materializes and uploads it; concurrent uploads are byte-identical PUTs to the same key, hence idempotent, and apply-side dedup is by `(key, sem_hash)`. The vc's journal entry carries a fresh single-component vv (it is a new key). `sync-conflict {path, winner_device, copy_path}` is emitted. No user edit is ever destroyed, only demoted.

Rating/tags vs adjustments field-level merge is explicitly *not* attempted in v1 (documented future work); whole-document resolution + loser-copy is simple and lossless.

### 2.7 Deletion: soft delete, tombstones, grace, moves

**Deletion is soft at the bucket level.** `delete_files_*` / `delete_folder` hooks, per affected relkey:

1. PUT tombstone `{relkey, vv (bumped), device, server_ts, kinds}` and append `del` journal entries (a `del` is a version of the key, ordered by vv like any other — so delete-vs-edit is resolved by the same §2.6 machinery).
2. Move the local files to the OS trash (desktop) or a local `.rrcloud-trash/` folder with 7-day retention (Android, where upstream bypasses trash).
3. **Do not DELETE the data keys.** Data keys (original, sidecars, xmp) are destroyed only by GC after the tombstone passes the horizon and grace rules of §2.10. Until then, every device hides the item from the UI and lists it under **"Recently Deleted"** (restore = journal `put` with a vv bumped past the tombstone's; the bytes are still in the bucket, so restore is metadata-only).

**Edits beat deletes — now actually implementable** (fixing review A2): a receiver applying a `del` that is *concurrent* with its local dirty sidecar resurrects: journal `put` with dominating vv for the sidecar **and** the original's relkey. Because the data keys still exist during the grace window, the resurrected item is whole even when the editing device holds only an evicted stub — the original is still in the bucket, the proxy/thumbs are still present (preview GC also honors the grace window: previews/thumbs are deleted only when no live relkey *and no in-grace tombstone* references the `content_id`). GC re-applies all journals before destruction and never destroys a superseded (resurrected) tombstone's keys.

**Moves:** `move/rename` = journal `move` entry (vv bump) + server-side `CopyObject` + tombstone on the old key (data-key delete again deferred to GC). Receivers rename locally (or move the stub), preserving sidecar state; album path patching rides the existing `sync_album_path_changes`.

### 2.8 XMP and original conflicts (previously unspecified)

- **`.xmp` is a projection of the sidecar, not an independent conflict domain.** When RapidRAW writes an XMP (rating/label/tags mirror), the `xmp` journal entry carries the sidecar version's vv; receivers apply an xmp object only when it matches their winning sidecar version, and regenerate it locally otherwise. Concurrent *external* XMP edits enter through the XMP-import path (§2.5), which folds them into the sidecar — where §2.6 resolves them and the loser survives in the loser's virtual-copy sidecar. Net: no silent xmp clobber can lose information that wasn't first captured in a sidecar version.
- **Originals are immutable in normal operation**, but an external overwrite (user replaces a file in the library out-of-band) is journaled as `put original` with a new `content_id` and bumped vv. Two concurrent overwrites: the bucket key is last-PUT-wins, but each `put` entry records the content_id it replaced; a device still holding the displaced bytes uploads them as `library/<stem>.conflict-<blake3[..6]>.<ext>` (deterministic, idempotent) and the UI flags `original-conflict`. Lossless, cheap, rare.

### 2.9 Albums & presets

Synced v1 as whole-document resolution with the same vv machinery (loser saved as `albums.conflict-<blake3[..6]>.json` under `.rrcloud/v1/meta/`, surfaced in settings UI): on upload, every absolute path in `albums.json` under the sync root is rewritten to `rr://<relkey>`; on download, rewritten back to the local root. Paths outside the sync root are dropped from the uploaded copy (kept locally) — albums referencing non-synced folders remain device-local. Presets contain no paths (except LUT refs — stripped identically) and sync trivially. Pseudo-paths like `"Album: <name>"` live only in frontend state, not in albums.json, and need no mapping.

### 2.10 Compaction, horizons, GC (consolidated; fixes A3/B4/B8/D3)

**Definitions.** *Active device* = registered, not retired, with `last_seen_server_ts` within 30 days. *Server time* = the `Date` header of S3 responses, recorded on every heartbeat; **all age thresholds in this section are evaluated in server time**, so a skew-forward device cannot block GC forever and a skew-back device cannot be falsely aged out (fixing B8).

**Segment compaction (own prefix only).** Device A may delete its own segments ≤ s when **all** hold:
1. A's own manifest (`manifests/A.json.gz`) has `cursors[A] ≥ s` — i.e., the covered entries (including their deleted-set rows) are durably folded into an object A alone writes. A verifies the manifest PUT with a read-back HEAD before any segment DELETE.
2. At least 24 h (server time) have elapsed since (1), and a second pass re-confirms the manifest is present — the cheap insurance against any transient PUT anomaly.
3. **Either** every active device's `applied[A] ≥ s` (the fast path), **or** the segments are older than a **14-day cap** (server time). The cap is the pressure valve (fixing D3): a laggard device that misses compacted segments catches up by merging manifests instead of replaying the journal — which is *correct*, because manifest rows carry vv (conflict resolution still runs) and the deleted set (deletions still propagate). Journal growth is therefore bounded at ~2 weeks of churn regardless of dormant devices.

Readers whose cursor falls below the available segments fall back to manifest merge (§2.3), which is now lossless for both liveness and deletion information.

**Tombstone GC** (worker, or a desktop with `worker_backfill`). A tombstone's data keys may be destroyed and the tombstone object deleted when **all** hold: (a) every active device has applied past it, or the 14-day cap has elapsed; (b) it is ≥30 days old (server time) — the user-facing "Recently Deleted" window; (c) it is folded into the GC runner's manifest **deleted set**, which is retained for 12 months; (d) a final journal re-read confirms it was not superseded by a resurrecting `put`.

**Device lifecycle.** Devices are retired explicitly from any device's settings UI ("remove device", writes `devices/<id>.retired`) or automatically by GC after 90 days of inactivity (server time). Retirement removes a device from the active set immediately (an uninstalled phone or a dead CronJob identity stops affecting horizons the moment it's retired, and affects them for at most 30 days of inactivity otherwise — fixing the orphan-blocks-horizons hole of B5/C3). A retired device that returns must re-register as a new device and bootstrap from manifests; the §2.3 pre-upload quarantine rule prevents it from resurrecting anything.

**Orphaned journal prefixes** (retired devices) are folded into the worker's manifest and deleted by the worker, under the same rules as its own prefix.

### 2.11 Why it's safe (required scenarios, including the review's)

- **Two devices editing offline:** both commit versions with concurrent vvs; on reconnect each uploads, each sees the other's entry, §2.6 case 4 fires identically on both (same deterministic winner), the loser materializes at the **same** deterministic key, both converge. Any third device applying either arrival order reaches the same state because the apply rule is a total order over versions, not an order-of-arrival rule.
- **Delete vs. edit, original evicted:** data keys survive the grace window, so resurrection restores a whole item (§2.7). The old design's promise now has an implementation.
- **Corrupt upload + eviction:** a corrupted part is rejected at receipt (`Content-MD5`); a backend that doesn't check is detected at setup and forces read-back verification; and eviction additionally requires a full-content attestation or read-back (§3.5). The weak-verify → evict → permanent-loss chain (review A1) is severed at three independent points.
- **Stale LIST (B2):** LIST remains advisory. New: destruction decisions no longer depend on LIST freshness either — segment deletion depends only on the owner's own durable manifest; tombstone GC depends on manifests' deleted sets which out-retain any realistic staleness by months.
- **Stale or unregistered device returning:** bootstraps from manifests that include deletions; cannot resurrect (§2.3 quarantine rule). Mass resurrection is no longer a designed behavior; the designed behavior is a user prompt.
- **Crash between journal PUT and DB commit:** replay re-PUTs byte-identical bytes (§2.1.5/§2.2), so `(device, seq)` dedup on readers is sound.
- **Partial upload / partial download + crash:** multipart state persisted per part; downloads go to a dot-prefixed temp file, are blake3-verified (sidecars additionally parse-validated), then atomically renamed — and resume via ranged GET after re-hashing the partial from disk (§3.5), so a 60 MB hydrate over flaky mobile does not restart from zero.
- **Sidecar rewrite-churn:** `sem_hash` ignores `exif`/formatting; auto-heal and EXIF caching cause zero uploads; XMP import causes exactly one upload per actual external XMP change (§2.5).

---

## 3. Client engine

### 3.1 Module layout

```
src-tauri/
  crates/rrcloud-core/           # NEW crate, workspace member; no tauri dependency
    src/lib.rs
    src/s3/{client.rs, sigv4.rs, multipart.rs, xml.rs}   # thin S3 client (§3.8)
    src/keys.rs                  # relkey <-> key schema mapping
    src/journal.rs               # entry schema, segments, cursors, publish/replay
    src/manifest.rs              # per-writer manifests, merge, deleted set
    src/state.rs                 # redb tables + typed accessors
    src/semhash.rs
    src/clock.rs                 # version vectors + device identity + server-time tracking
    src/engine.rs                # SyncEngine: apply loop, reconcile, queues, state machines
    src/transfer.rs              # resumable up/down, Content-MD5, streamed blake3, temp+verify+rename
    src/tombstone.rs             # soft delete, grace, GC
    src/compact.rs               # compaction + horizons (§2.10)
    src/proxy.rs                 # linear-DNG proxy generation (depends on rawler only, §4.2)
    src/thumbs.rs                # worker-side thumb encode + thumbpacks (image crate, q75)
  src/sync/                      # NEW module in rapidraw_lib (tauri bridge)
    mod.rs                       # SyncManager struct, setup wiring
    hooks.rs                     # always-compiled hook shims (single hook style, §7)
    commands.rs                  # tauri commands: configure, status, pin, evict, hydrate, resolve_conflict, retire_device, verify_library
    hydrate.rs                   # ensure_local(), stub creation/eviction, LRU, attestation gate
    placeholder.rs               # is_stub(path) backing is_cloud_placeholder
    events.rs                    # sync-* event emission (batched)
    credentials.rs               # keystore/file-backed credential store
  src/bin/rrcloud-worker.rs      # NEW headless worker bin target (§6)
  tauri-plugin-rrcloud/          # NEW Tauri mobile plugin (Kotlin + thin Rust), §5
```

`rrcloud-core` pins `rawler` to the **same git rev** as `src-tauri/Cargo.toml` (`CyberTimon/RapidRAW-DngLab` @ `934af4b`) so proxy generation and app decode share one decoder.

### 3.2 State store: redb (decision)

**redb 2.x over SQLite (rusqlite).** Rationale:

- Pure Rust: no C toolchain wrinkle added to the existing Android NDK cross-build (CI builds `aarch64-linux-android` with ORT already the one manual native lib; keeping the diff toolchain-neutral keeps `build.yml` untouched).
- Crash-safety matches the platform reality: Android `RunEvent::Exit` calls `std::process::exit(0)` immediately (`lib.rs:2303`) — redb commits are durable at commit (shadow-paging B-tree, no WAL to checkpoint). Every state-machine transition in §2.4, and every journal-segment freeze in §2.1.5, is one committed write txn.
- Access patterns are pure KV + small scans. Single file `app_data_dir/rrcloud/state.redb`; `Database::create` handles recovery.

Tables: `items` (relkey → ItemRecord: kind, state, size, mtime, blake3, sem_hash, **vv**, content_id, w, h, pinned, last_access, verified_remote, attested), `applied` ((device, seq) → ()), `cursors` (device → seq), `pending_segments` (seq → frozen bytes), `uploads` (relkey → MultipartState), `upload_parts` ((relkey, part_no) → {etag, md5}), `queue_up`/`queue_down` (priority, relkey), `xmp_seen` (path → blake3), `dcim_seen` ((path, size, mtime) → content_id), `meta` (device_id, server-time offset, settings cache).

### 3.3 SyncManager lifecycle

Follows `ThumbnailManager`/`MetadataManager` precedent (`app_state.rs:163-208`), but tokio-first like the export path:

- `Arc<SyncManager>` added to `AppState`; constructed in `setup()` next to `start_thumbnail_workers` (`lib.rs:1938-1941`). Holds: `SyncEngine` (from rrcloud-core), config snapshot, `Notify` wake handle, `AtomicBool` paused, `JoinHandle`s.
- One supervisor tokio task: loop { drain inbound journal (adaptive poll per §2.2), pump upload/download queues through a `Semaphore`, periodic device-registry heartbeat }. All sub-transfers are child tasks with cancel tokens, mirroring the export engine's `Semaphore + AtomicBool` pattern (`export_processing.rs:875-946`).
- If sync is unconfigured/disabled, `SyncManager::new` returns an inert instance; every hook (`notify_saved`, `ensure_local`, `is_stub`) is a cheap no-op. Hook call sites in upstream files are unconditional one-liners (single hook style, §7).
- **Exit flush:** the `RunEvent::ExitRequested` hook performs a bounded (≤2 s) opportunistic flush of queued *small sidecar* uploads, commits state, and (Android) enqueues an expedited WorkManager job so pending work resumes out-of-process (§5.1). When the dirty count stays >0 for 24 h, a persistent "N edits not backed up" notification is shown (§5) — uninstall data loss cannot be prevented, but it will not be silent.

### 3.4 The `save_sidecar` chokepoint (upstream refactor)

New `exif_processing::save_sidecar(app_handle: Option<&AppHandle>, sidecar_path: &Path, meta: &ImageMetadata, origin: WriteOrigin) -> Result<()>`:

1. Acquire per-path async-aware lock (`DashMap<PathBuf, Arc<Mutex<()>>>` in `AppState`) — closes the AI-tagging-vs-user-edit race and serializes sync's third-writer.
2. **Corruption guard, including empty files** (fixing A4): if the existing sidecar fails to parse — or is 0 bytes, or otherwise disagrees with a redb record that says this key has a non-default synced head — rename it to `<name>.rrdata.corrupt-<ts>`, trigger a priority re-download of the remote head, emit `sync-error {path, "sidecar restored — please retry"}`, and **abort this write**. A locally corrupted sidecar can therefore never be silently replaced by a defaults-based document that then out-versions and demotes the real edit everywhere. (`load_sidecar` itself gets a one-line `log::warn!` on parse failure.)
3. **Remote-head guard:** if the item's remote sidecar head is known but the local sidecar is still `pending_down` (§3.5), trigger the download first and retry; when offline, the write proceeds but is flagged `base=unknown` in redb, which forces its eventual upload through the conflict path (§2.6 case 4) rather than ever fast-forwarding over an unseen head — worst case is a spurious loser-copy, never loss.
4. Write temp file in same dir (`tempfile::NamedTempFile` + `persist`) — atomic rename.
5. Compute `sem_hash`; if changed, call `sync::hooks::notify_sidecar_saved(relkey, sem_hash, origin)` (no-op when disabled).

Route all ~15 write sites through it (table in research §1.1): `save_metadata_and_update_thumbnail`, the five batch `apply_*`/`reset_*` fns, `set_color_label_for_paths`, `set_rating_for_paths`, `load_metadata`, `resolve_image_metadata`, `update_exif_fields`, `create_virtual_copy` (file_management.rs); `start_background_indexing`, `modify_tags_for_path`, `clear_ai_tags`, `clear_all_tags` (tagging.rs); `save_primary_metadata`, `load_sidecar` auto-heal (exif_processing.rs). Each site is a mechanical one-line substitution of `fs::write(...)` — the dominant upstream diff, but line-local.

New-original hooks (enqueue upload): `import_files`, derived-output saves (`save_hdr`, `save_collage`, pano/focus/denoise/negative), `duplicate_file`, `copy_files`; `move_files`/`rename_files` → remote move (§2.7); `delete_*` → soft delete (§2.7).

### 3.5 Stubs, placeholders, hydration, sidecar policy, budgets

- **Sidecars are never stubbed.** (Fixing the biggest spec hole, A4/G1.) `.rrdata` files are eagerly mirrored, prioritized above previews in the download queue: thumbs-visible > sidecars > small thumbs crawl > medium thumbs/proxies. Cost at 50k images: sidecars exist only for edited images; a typical edited sidecar without AI patches is 10–300 KB (≈0.5–3 GB for a heavily edited library); AI-patch sidecars (multi-MB) are deprioritized behind small thumbs but ahead of proxies. Until a sidecar lands, the path is marked `pending_down`; `is_cloud_placeholder` returns true for it, so upstream's existing MetadataManager deferral machinery kicks in, and the grid shows correct rating/label badges from the journal entry's `rating`/`color_label` fields (§2.2). The §3.4 chokepoint guard prevents any write-before-download clobber.
- **Stub** (originals only) = 0-byte file at the real path, mtime set to the remote original's `mtime` via `filetime` — chosen because (a) listing/grouping/folder trees work unchanged, (b) `compute_thumbnail_cache_hash` (= blake3(abs path, mtime, adjustments), `file_management.rs:66`) stays **stable across hydration** since hydration restores the same mtime, (c) `read_file_mapped` already rejects empty files (`ReadFileError::Empty`), so any unguarded reader errors instead of decoding garbage.
- `is_cloud_placeholder` (`file_management.rs:1359`) becomes: macOS iCloud check `|| sync::is_stub(path)` on all platforms (backed by an in-memory `HashSet<PathBuf>` mirror of redb). All existing consumers (listing flags, CloudOff icon, thumbnail skip, metadata deferral, `load_image` guard) light up for free.
- **`sync::ensure_local(path, reason) -> Result<PathBuf>`**: if stub → download original to `<dir>/.rr.part-<name>` → blake3 verify → rename over stub → restore mtime → mark `hydrated`, emit `attest` journal entry, bump LRU. **Resumable:** the `.rr.part` file persists; on resume, the existing bytes are re-hashed from disk (cheap vs. network) and the transfer continues with a ranged GET from the current offset (fixing G2). Emits `sync-hydrate-progress {path, bytes, total}`. Guard call sites (all currently bypass the placeholder check):
  1. `image_loader::load_image` (replaces the iCloud error branch at `image_loader.rs:940` — but see proxy mode §4.4 first),
  2. `export_images_impl` (per image, before `load_and_composite`),
  3. `generate_preview_for_path`,
  4. the HDR / panorama / focus-stack / denoise merge commands in `lib.rs`,
  5. the culling view's full-resolution load path (goes through `load_image`, covered by 1; its thumbnail path is covered by the placeholder check),
  6. `copy_files` / `move_files` / `duplicate_file` (must not copy a stub as content; copy = remote `CopyObject` + new stub, or hydrate-then-copy if destination is outside the library),
  7. `get_image_dimensions`,
  8. tagging's `get_cached_or_generate_thumbnail_image` falls back safely (uses cached thumbs; skips stub paths without thumbs).
- **Pinning + LRU with a real eviction gate** (fixing A1): `pinned` flag per item (`pin_paths` command, folder-level pin fans out). Evictor keeps `Σ hydrated original sizes ≤ settings.sync.cache_size_gb`, evicting least-recently-accessed, non-pinned, `synced`-state originals back to stubs — **and only items whose remote copy is content-verified**: `verified_remote=true` (upload-side integrity, §2.4) **and** either an `attest` journal entry exists for the object's blake3 (the worker produces these for every original it touches; any hydration produces one) **or** the evictor performs a one-time full ranged-GET re-hash before evicting (read-back; acceptable because eviction is rare and bounded). A mismatch at any point → `corrupt_remote`, repair flow, no eviction. "Make available offline" / "Free up space" commands + context-menu UI.
- **Preview/thumb storage is budgeted and durable** (fixing D1): synced thumbs and proxies live canonically under `app_data_dir/rrcloud/{thumbs,previews}/<content_id>…` — **app data, not the OS-clearable cache dir**. They are surfaced to the webview by hard-linking (same filesystem on Android: both under `/data/user/0/<pkg>/`; copy fallback on desktop) into `app_cache_dir/thumbnails/{hash}_small.jpg` / `_medium.jpg`, where `{hash}` is computed with the *stub's* path/mtime and current adjustments — the exact key `generate_single_thumbnail_and_cache` looks up. The asset-protocol scope (`$APPCACHE/thumbnails/*`, tauri.conf.json) stays untouched; if the OS clears the cache dir, reseeding is a local re-link, zero network. `thumbnail-generated` events fire from the sync apply loop after seeding. Budgets: `preview_budget_gb` (default 10 on Android) LRU-bounds medium thumbs + proxies; small thumbs are costed (~2 GB at 50k × ~40 KB) and floor-evicted only under disk pressure. Download policy and bootstrap costs are in §4.3.

### 3.6 Settings vs credentials

New `AppSettings` fields (must be in the struct or serde drops them on the frontend round-trip — `useSettingsStore.handleSettingsChange` sends the whole object back): a nested `#[serde(default)] sync: SyncSettings`:

```rust
pub struct SyncSettings {
  pub enabled: bool,                 // default false; desktop gets same engine, gated here
  pub endpoint: String,              // e.g. https://garage.themissing.xyz
  pub bucket: String, pub region: String,      // "garage" for Garage
  pub force_path_style: bool,        // default true
  pub upload_requires_unmetered: bool, pub upload_requires_charging: bool,
  pub cache_size_gb: u32,            // hydrated-originals LRU budget, default 8
  pub preview_budget_gb: u32,        // proxies + medium thumbs LRU budget, default 10
  pub preview_prefetch_months: u32,  // proxy/medium prefetch recency window, default 12 (Android)
  pub auto_watch_dcim: bool, pub watched_media_buckets: Vec<String>,
  pub worker_backfill: bool,         // desktop app performs worker duties
}
```

**Credentials never enter settings.json or the webview.** `sync::credentials`: on Android, a Kotlin plugin command stores `{access_key, secret_key}` in Android Keystore-backed `EncryptedSharedPreferences`; Rust fetches them via the plugin at engine start. On desktop, `app_data_dir/rrcloud/credentials.json` with 0600 perms, read only in Rust. The frontend sees only `credentials_configured: bool` and submits new creds through a dedicated command (`sync_set_credentials`) that writes to the store and returns no echo.

### 3.7 Upload debounce vs 300 ms autosave

`debouncedSave` fires `save_metadata_and_update_thumbnail` every 300 ms during editing. The chokepoint marks `dirty` but the queue admits a sidecar only after **5 s of quiescence per path**, and the path currently open in the editor is additionally held until image-switch/back-to-library flush (the same signals that flush `debouncedSave` in `useAppNavigation.ts:140,162,387` call a `sync_flush_path` hint) or app-background. Admission is also the moment the version vector bumps (§2.6): one upload and one version per editing burst, not hundreds.

### 3.8 Event families (UI)

Emitted via `app_handle.emit`, consumed in `useTauriListeners.ts`, batched like the thumbnail buffer: `sync-status {state: idle|syncing|offline|error, pending_up, pending_down, bytes_up, bytes_down, dirty_unbacked}` (1 Hz max), `sync-item-state {path, state}` (batched), `sync-hydrate-progress`, `sync-hydrated {path}`, `sync-conflict {path, copy_path, winner_device}`, `sync-error {path?, message}`. `ImageFile` gains `#[serde(default)] sync_state: Option<String>` so the grid can badge cloud/local/uploading per item. Status badge lives in the `MainLibrary` header; a "Recently Deleted" view and a device-management panel (list/retire devices, §2.10) live in settings.

### 3.9 S3 client: hand-rolled SigV4 over reqwest (decision)

| Option | Verdict |
|---|---|
| `opendal` | Capable, but large dependency surface (binary size on Android matters; APK already carries ORT + lensfun), abstracts away multipart details we must own (per-part ETag/MD5 persistence for resume), frequent API churn. |
| `rust-s3` | Bundles its own TLS/HTTP decisions, patchy maintenance, awkward with `rustls-platform-verifier` (which `initialize_android` already sets up for the app's reqwest). |
| **Hand-rolled SigV4 + typed client (chosen)** | We need exactly 9 operations: PUT/GET(ranged)/HEAD/DELETE, CopyObject, ListObjectsV2, Create/UploadPart/Complete/Abort-Multipart, ListMultipartUploads/ListParts. SigV4 is ~300 LOC (`hmac` + `sha2`; `UNSIGNED-PAYLOAD` for streamed parts — integrity carried by `Content-MD5` per §2.4 — signed payload for small PUTs). Reuses the exact reqwest 0.13 + rustls + platform-verifier stack already shipping on Android; full control of path-style, `region=garage`, user metadata headers, and B2/R2 quirks. XML parsing via `quick-xml` (small). |

Portability conformance tests (§8 harness) run the client against MinIO and Garage (and manually B2/R2), and must specifically prove: multipart with `Content-MD5` rejection of bad digests (the §2.4 probe), `ListMultipartUploads`/`AbortMultipartUpload`, ranged GET resume, metadata echo, and ListObjectsV2 paging.

---

## 4. Smart previews and proxy edit mode

### 4.1 The fidelity answer (rigorous)

**Question:** does editing a preview stay accurate when pushing shadows/highlights/exposure/contrast past one stop?

**8-bit sRGB JPEG proxies: no — confirmed, for three independent reasons** visible in the existing fallback path `linearize_embedded_preview` (`image_loader.rs:366`, used at `:176,:190` when raw decode fails):
1. *Quantization:* inverse-gamma of 8-bit sRGB leaves ~1 code value per ~2% luminance step in shadows; a +2 EV shadow push stretches those steps ×4 → visible posterization/banding. 16-bit linear has 3.05e-5 absolute step — at 5 stops below white there are still ~1000 distinct levels.
2. *No highlight recovery:* JPEG is clipped at 1.0 per channel post-WB-and-tone; `recover_clipped_pixel` (`raw_processing.rs:61`) reconstructs highlights from per-channel >1.0 headroom that simply doesn't exist in a JPEG.
3. *Baked rendering:* camera WB, tone curve and color matrix are already applied, so RapidRaw's WB/Calibrate math operates on wrong inputs — sliders don't track the full-res render even at ±0.3 EV.

**Downscaled demosaiced linear DNG proxies: yes for all global tone/color operations, with a precise equivalence argument — and a precise divergence list.** `develop_internal` (`raw_processing.rs:109-272`) contains a first-class LinearRaw branch: for a linear DNG it *skips Demosaic and SRgb, keeps WhiteBalance and Calibrate* (`raw_processing.rs:168-173`), rescales by the file's WhiteLevel/BlackLevel (with the whitelevel→`u32::MAX` trick preserving above-nominal-white values as >1.0), then runs the identical `recover_clipped_pixel` highlight reconstruction and hands the identical `Rgba32F` buffer to the identical GPU adjustment pipeline. If the proxy contains **scene-linear, un-white-balanced, camera-native-space demosaiced RGB** with the original's `AsShotNeutral` (wb_coeffs), `ColorMatrix1/2`, `BlackLevel=0`, and a `WhiteLevel` that leaves headroom, then every *pointwise global* operation — exposure, contrast, shadows, highlights, WB, curves, HSL, color grading — is the same function applied to a resampled version of the same data: ±3-stop tone moves track the full-res render, because highlight headroom and 16-bit shadow precision are preserved and the decode path is literally the same code.

**What is *not* identical (complete list, with handling):**
- **Highlight reconstruction near clipped edges** (review E1): `recover_clipped_pixel` is nonlinear (smoothstep engagement above 0.50, magenta suppression, burn desaturation). The proxy is downscaled *before* this function runs at load; the full-res path runs it before any downscale. Downscale-then-recover ≠ recover-then-downscale in partially clipped neighborhoods: averaged part-clipped pixels land in different smoothstep regimes, shifting recovered hue/desaturation locally. This is inherent to any resolution-reduced proxy and is **included, not excluded, in the acceptance test** (§8 P3): clipped-neighborhood pixels get their own, looser ΔE budget plus a mandatory visual review of recovered-highlight edges, instead of being masked out.
- **`remove_raw_artifacts_and_enhance`** (review E2): `image_loader.rs:142/206` applies color NR + sharpening to every raw develop's base image before the GPU pipeline — at proxy resolution for the proxy, full resolution for the original. Mitigation: in proxy mode these amounts are scaled by `proxy_scale` (line-local parameterization in the loader); residual difference is part of the tested ΔE budget, not hand-waved.
- **`clamp_limit` under fast demosaic** (review E3): `raw_processing.rs:194-198` sets `clamp_limit = 1.0` whenever `fast_demosaic`, which would clip exactly the >1.0 headroom the proxy exists to carry (the thumbnail path uses fast demosaic). Fix: a line-local upstream change — `if fast_demosaic && !is_linear_format { 1.0 } else { 1000.0 }` — fast demosaic is meaningless for LinearRaw anyway (the branch skips Demosaic). Added to the touched-file table (§7).
- Detail-dependent *user* ops at 1:1 — sharpen, denoise, clarity/texture, grain, lens-blur depth estimation — operate at proxy resolution; representative at fit-to-screen, approximate at 1:1. The editor shows a "preview quality" badge in proxy mode; **export always forces hydration and re-renders from the original** (§4.4), as do HDR/pano/focus-stack.
- Demosaic algorithm choice is baked at generation time (default high-quality algorithm, not `DemosaicAlgorithm::Speed`); at 2560 px downscale the difference is sub-visible.
- `neutralize_wb_if_multiexposure` sniffs maker notes in the file bytes; proxy bytes won't trigger it. Multi-exposure raws render with a slightly different default WB in proxy mode — accepted v1 edge case, listed in docs.
- Settings `linear_mode` ("gamma"/"skip_calib") exists for *foreign* linear DNGs. Proxies are tagged (DNG `Software = "RapidRawCloud/pxy1"` + filename suffix `.pxy.dng`); the proxy load path pins `(apply_ungamma=false, apply_calibration=true)` regardless of user settings — threaded past `linear_raw_mode` at `image_loader.rs:88` (named in §7's table).
- **Calibration matrix round-trip** (review E4b): at load, Calibrate pulls ColorMatrix1/2 from the proxy's DNG tags, whereas native decode pulls them from rawler's camera TOML tables. The proxy writes the matrices *from the decoded original's in-memory RawImage*, so they are the same numbers — but illuminant selection/interpolation must behave identically on read-back. This gets an explicit unit test in P3 (decode original → write proxy → decode proxy → assert Calibrate inputs bit-equal), not an assumption.

### 4.2 Proxy production (exact pipeline, `rrcloud-core/src/proxy.rs`)

Runs on the importing client and the headless worker, using the same rawler rev the app builds (`CyberTimon/RapidRAW-DngLab` @ `934af4b`):

1. `RawSource::new_from_slice(bytes)` → `get_decoder` → `decoder.raw_image(&source, default, false)` and `raw_metadata`.
2. Set all `whitelevel` entries to `u32::MAX` (same trick as `develop_internal:162`) so Rescale doesn't clip sensor-above-nominal values.
3. `RawDevelop` with steps retained: `{Rescale, Demosaic, FujiRotate, CropActiveArea, CropDefault}` — **no** WhiteBalance, **no** Calibrate, **no** SRgb → `develop_intermediate` → f32 camera-space RGB at full resolution, sensor orientation (orientation is *not* applied; the proxy carries the original's EXIF Orientation so `apply_orientation` behaves identically on load). Record `(w, h)` for the journal from this exact decode after orientation accounting (§2.2) — never from EXIF.
4. Rescale so nominal white = 0.5; clamp to [0, 1.0] (i.e. 2× nominal-white headroom — real sensor overshoot above nominal white is ≤~10–50%, so 2× is lossless in practice).
5. Lanczos3 downscale in linear space to 2560 px long edge (linear-light resampling is the correct average).
6. Quantize to u16 (×65535).
7. Write DNG via the rawler writer: **construct `RawImage::new` directly** (camera, PixU16, cpp=3, `wb_coeffs` = original's, `RawPhotometricInterpretation::LinearRaw`, BlackLevel 0, WhiteLevel 32767), copy the `color_matrix` map from the decoded original — the convenience `rgb_image_u16` is explicitly **not** used, since it hard-codes `wb_coeffs [1,1,1,1]` and a fresh `Camera` (writer.rs:98-110 in the dnglab lineage); then `DngWriter`/`SubFrameWriter::raw_image(&rawimage, CropMode::None, DngCompression::Lossless /* LJPEG-92, 16-bit, 3-component: writer.rs:707 */, DngPhotometricConversion::Original, predictor)`; embed original EXIF (capture date, orientation, camera/lens) and a medium JPEG preview IFD (reusable as the 1280 thumb).
8. Also emit `_small`/`_medium` JPEG thumbs (q75, matching `encode_thumbnail`).

**rawler writer gap check (named, first implementation task of P3):** everything above exists in the xerootg dnglab lineage (`rawler/src/dng/writer.rs` in `/home/user/dnglab`: LinearRaw tag path `:314-317`, LJPEG LinearRaw tiles `:707-709`, public `SubFrameWriter::raw_image` `:116`). The CyberTimon fork at `934af4b` is also a dnglab descendant and demonstrably retains `RawPhotometricInterpretation` (RapidRaw imports it), but **the two dnglab forks have diverged APIs** (`DemosaicAlgorithm` exists only in CyberTimon's) and RapidRaw never links the `dng::writer` module. P3 therefore verifies, in the CyberTimon fork specifically: (a) the writer module exists and is `pub`; (b) `RawImage`'s `wb_coeffs`/`color_matrix` fields are publicly constructible/mutable (the "fields are public" claim is validated there, not inferred from xerootg's fork); (c) `SubFrameWriter` carries EXIF + matrix plumbing. If anything is missing, the scoped change to `CyberTimon/RapidRAW-DngLab` is: restore/port `rawler/src/dng/{writer.rs, convert.rs tags}` from upstream dnglab (additive, no decoder changes) and expose `DngWriter::close`/`SubFrameWriter::raw_image` publicly. No other rawler change is needed; the read side already works.

### 4.3 Generation sites, download policy, bootstrap cost

- **Importing client:** after an original reaches `synced` (or opportunistically right after import), generate proxy + thumbs locally, upload under `content_id` keys, journal `preview`/`thumb` entries. On-device cost ≈ one full decode + demosaic; scheduled on the thumbnail worker priority tier, charging-gated on Android by default.
- **Headless worker (§6):** backfills for foreign ingests and for clients that skipped generation; additionally emits per-folder **thumbpacks** (tar of `_small` thumbs for a folder) so bootstrap fetches 1 GET per folder instead of 1 per image; clients fall back to individual GETs when no pack exists or the pack's entry list is stale.
- **Download policy (costed at the brief's 50k scale — fixing D1):**
  - *Small thumbs (480px, ~40 KB):* whole library, ≈2 GB at 50k — acceptable and the backbone of the always-browsable grid. Fetched lazily: visible folders first, then a background crawl; with worker thumbpacks the crawl is ~#folders GETs, without it 1 GET/image (50k GETs ≈ 40+ min of foreground radio at 20 rps — acceptable once, resumable, and surfaced as bootstrap progress; see §5.1 for the recommended keep-open-on-charger first sync).
  - *Medium thumbs (~0.25 MB) and proxies (~6–12 MB):* by folder-recency policy — prefetched for the N most recent months + pinned folders (`preview_prefetch_months`, default all on desktop/worker, 12 on Android), remainder fetched on first editor open (single GET, ~8 MB). Both live under the `preview_budget_gb` LRU (§3.5).
  - *Bootstrap totals at 50k:* merged manifests ~10–15 MB gz (streamed parse); 50k stub creations (local, fast); sidecars per §3.5; small thumbs ≈2 GB (or ~#folders GETs with packs); proxies for the recency window only. "Edit anything you've recently touched, browse everything, hydrate anything on demand" — with every number stated.
- Proxies land in `app_data_dir/rrcloud/previews/<content_id>.pxy.dng` (not the webview-scoped cache; decoded in Rust only); thumbs seed the thumbnail cache via hard links from the durable store (§3.5).

### 4.4 Proxy edit mode mechanics

- `load_image(path)`: if `is_stub(path)` and proxy present → decode proxy DNG through `load_base_image_from_bytes` (hits the LinearRaw branch, with the §4.1 clamp fix and pinned `(apply_ungamma=false, apply_calibration=true)`), but **report the original dimensions** `(w,h)` from the journal/manifest record; store `proxy_scale = proxy_long_edge / orig_long_edge` in `AppState`. Both numbers share provenance (§4.2 step 3), so `proxy_scale` cannot misplace masks. A background `ensure_local` can be kicked off opportunistically (setting: hydrate-on-open when unmetered).
- Coordinate spaces: crop/masks/AI patches are stored in original-pixel space. The render path multiplies geometry by `proxy_scale` exactly as `generate_thumbnail_data` already does with `total_scale`/`raw_scale_factor` (`file_management.rs:1648-1658`) and as the fast-demosaic scale factor does for ¼/½-size decodes — the scaling plumbing exists; proxy mode generalizes it to the editor preview worker (`process_preview_job` downsamples from the proxy base; `editor_preview_resolution` 1280 ≤ 2560 proxy, so screen previews are never upsampled).
- When hydration completes while the editor is open: emit `sync-hydrated {path}`; the frontend re-invokes `load_image`, which now loads the original, reports identical dimensions, and re-renders — adjustments unchanged since they were always in original space.
- **Export forces hydration:** `export_images_impl` calls `ensure_local` per path before `load_and_composite`; batch exports hydrate with the transfer semaphore and surface per-file progress through the existing `batch-export-progress` channel. Same for HDR/pano/focus/denoise/culling-full-res.
- AI ops in proxy mode: subject/sky mask generation allowed on proxy pixels (resolution-independent enough for preview); generated `patchData` is resolution-tagged; v1 policy: AI patch generation requires hydration (guard in the AI commands).

---

## 5. Android platform work (`tauri-plugin-rrcloud`)

Kotlin + thin Rust Tauri mobile plugin, living in the fork; `gen/android` edits limited to manifest + gradle dep lines + plugin registration.

### 5.1 Background execution: the honest model (fixing C1)

Platform facts the design now builds on, instead of against: on Android 12+ (minSdk 24, targetSdk 36), `setForeground()` from a worker while the app is backgrounded throws `ForegroundServiceStartNotAllowedException`; expedited WorkManager jobs run as JobScheduler expedited work *without* FGS, under quota, in roughly 10-minute windows; and Android 15 caps `dataSync` FGS at 6 h per 24 h. Therefore three execution modes:

1. **App foreground:** the in-process engine runs freely (uploads, downloads, journal polling). This is where most sync happens in practice.
2. **User-initiated long operations** (initial bootstrap, mass hydration, "Sync now"): the plugin starts a genuine `FOREGROUND_SERVICE_DATA_SYNC` *while the app is foreground* (which is allowed), with a progress notification. The engine's cycles are bounded and resumable (§2.4/§5.4), so the Android 15 6 h/24 h cap degrades gracefully: the service stops at the cap, remaining work is persisted, and a notification invites the user to continue (the UI recommends "keep the app open on a charger for your first sync" — the 2 GB thumb crawl and any multi-GB hydration are explicitly first-sync, chargeable workloads).
3. **Background maintenance:** `SyncCycleWorker` — periodic (1 h, WorkManager constraints mapped 1:1 to settings: `NetworkType.UNMETERED` when `upload_requires_unmetered`, `requiresCharging`) plus an expedited one-shot enqueued whenever the queue is non-empty at app-background (§3.3 exit flush). These run **without** `setForeground`, inside JobScheduler's ~10-minute windows, under quota. The engine's one-bounded-cycle entry point (below) is designed for exactly this: every window uploads a few items, applies journal pages, and commits; redb resume makes windows compose. No claim is made that WorkManager delivers multi-hour transfers in the background — it doesn't, and the design no longer says it does.

**How Kotlin reaches the Rust engine:** the worker does *not* spin up Tauri/webview. `rrcloud-core` exports `#[no_mangle] pub extern "system" fn Java_…_RrcloudBridge_runSyncCycle(env, _class, ctx: JObject, budget_ms: jlong) -> jint`: initializes `ndk_context` from the passed `Context`, `rustls_platform_verifier`, opens redb + credentials (via JNI call back into `CredentialStore`), runs one bounded sync cycle (upload dirty, apply journals, prefetch — respecting `budget_ms`), returns a result code WorkManager maps to success/retry. The `.so` is the same `rapidraw_lib` cdylib already packaged. Single-instance safety: redb's file lock + an in-process static `Mutex` make app-process and worker-process cycles mutually exclusive (worker backs off with `Result.retry()` if the app holds the lock).

### 5.2 DCIM watch (fixing C2)

- Live path: `MediaStore` `ContentObserver` on `MediaStore.Images.Media.EXTERNAL_CONTENT_URI` registered while the process is alive → debounced trigger of the scan routine.
- Durable path: `DcimScanWorker` (periodic 30 min, same constraints). Cursor: on **API 30+, `MediaStore.getGeneration()` / `GENERATION_ADDED`** — the monotonic cursor built for this, immune to back-dated rows from bulk scans, OEM clone/restore, and clock changes. On API 24–29, fall back to `DATE_ADDED > cursor − overlap` with a 48 h overlap window re-scan (dedupe makes the overlap free). Filtered to configured bucket ids (default `DCIM/Camera`), including RAW mimetypes (`image/x-adobe-dng`, vendor raws by extension where mimetype is generic).
- **Dedupe key `(device_uuid, path, size, mtime)`** (Lightbox's proven `sd-card/check` scheme) in redb table `dcim_seen` — survives re-scans, re-mounts, and MediaStore id churn; on hit, skip; on size/mtime change, re-hash before deciding.
- **Partial photo access (Android 14):** manifest includes `READ_MEDIA_VISUAL_USER_SELECTED`; the plugin detects a partial grant, the settings UI shows "watching N selected photos — expand access" with the system re-selection prompt, and the watch status badge reflects partial coverage instead of implying all of DCIM is watched.
- **Streaming copy replacing `read_android_content_uri`:** Kotlin opens `ContentResolver.openInputStream` and streams (256 KiB buffer) into `<library>/DCIM-import/.rr.part-<name>`, then renames; Rust is handed the final path and runs the normal new-original flow (blake3 → journal → upload → proxy gen). No whole-RAW-in-JNI-heap. (Upstream `import_files`'s in-RAM branch is left untouched for manual SAF imports.)

### 5.3 Manifest additions

`READ_MEDIA_IMAGES` (API 33+), `READ_MEDIA_VISUAL_USER_SELECTED` (API 34+), `READ_EXTERNAL_STORAGE` with `android:maxSdkVersion="32"`, `ACCESS_NETWORK_STATE`, `FOREGROUND_SERVICE`, `FOREGROUND_SERVICE_DATA_SYNC`, `POST_NOTIFICATIONS` (sync notification channel; runtime-requested). Gradle: `androidx.work:work-runtime-ktx`, `androidx.security:security-crypto`.

### 5.4 Process-death and uninstall (fixing C3)

Everything resumable from redb (§2.4): multipart part map, download partials, queue, DCIM cursor, journal cursors, frozen journal segments. The 3 s EXIF-flush-thread fragility is not inherited: sync state never sits in memory past a transition commit. On next launch or next WorkManager fire, the engine replays from committed state; half-streamed copies are identifiable by the `.rr.part-` prefix and resumed. **Uninstall:** dirty-not-yet-uploaded edits are destroyed with the app's storage — unavoidable; the mitigations are the §3.3 exit flush, the expedited drain job, and the persistent "N edits not backed up" notification after 24 h of standing dirt, so the loss window is visible, not silent. Reinstall mints a new device_id (correct), bootstraps from manifests, and the dead device entry is retired from any device's settings UI or auto-retired by GC (§2.10) — it blocks nothing meanwhile beyond the 14-day compaction cap.

---

## 6. Headless worker

**`src-tauri/src/bin/rrcloud-worker.rs`** — a bin target in the existing crate, linking `rapidraw_lib` + `rrcloud-core`. This guarantees **exact color parity** (same rawler rev, same `proxy.rs`) with zero upstream-file refactoring. Cost accepted: the binary links tauri (gtk/webkit shared libs on Linux), so the container base installs `libwebkit2gtk-4.1 libgtk-3-0` — a fat but dumb image. (Rejected alternative: making `tauri` an optional feature of `rapidraw_lib` — touches `lib.rs` wholesale, rebase poison.) No GPU needed: decode/demosaic/DNG-write/JPEG-encode are CPU.

**The worker is a full protocol participant and therefore requires durable state** (fixing B5): a persistent state directory (`RRCLOUD_STATE_DIR`, containing redb with its device_id and seq). The engine **refuses to journal without it** (hard startup error, not a warning) — a stateless run can at most do read-only reporting. One stable worker identity, one journal prefix, monotonic seq across runs; no phantom devices, no seq reuse, by construction.

**Loop (cron-friendly, also `--daemon --interval 15m`):**
1. Full reconcile (§2.3) — discover foreign objects under `library/` (SFTPGo drops, rclone copies, desktop pushes).
2. For each original lacking `content_id`/preview/journal entry: GET → blake3 → journal `attest` → generate proxy + thumbs (§4.2) → PUT previews/thumbs (+ refresh the folder's thumbpack) → journal `put original` + `preview` + `thumb` entries under the worker's device_id → phones pick it up on their next poll and materialize stubs + seeded thumbs. (Camera-WiFi→SFTPGo→bucket flow appears in the phone library within one worker interval + one poll interval, no bucket notifications needed.) The attest entries double as the eviction gate for every original the worker touches (§3.5).
3. Validate `missing`/`corrupt_remote` flags; abort stale multipart uploads (`ListMultipartUploads` + `AbortMultipartUpload` — the portable mechanism; no lifecycle-rule dependency, §2.4); run tombstone GC and segment compaction per §2.10; fold into and rewrite `manifests/<worker-id>.json.gz`; retire long-dead devices; fold orphaned prefixes.

Idempotent and crash-safe by the same rules as the client (it *is* the same engine with `role=worker`). The desktop app performs the same duties when `settings.sync.worker_backfill = true` — the worker deployment is strictly optional. (Multiple backfillers are safe — each journals under its own identity and writes its own manifest — just wasteful; the UI warns when more than one device advertises the backfill role.)

**Deployment profiles:**
- *Generic:* `docker run -v rrcloud-state:/state -e RRCLOUD_STATE_DIR=/state -e RRCLOUD_ENDPOINT=… -e RRCLOUD_BUCKET=… -e RRCLOUD_ACCESS_KEY=… -e RRCLOUD_SECRET_KEY=… ghcr.io/xerootg/rrcloud-worker --once` — any free-tier box, cron, or systemd timer. **The volume is mandatory.** Image built by a GitHub Actions job in the fork (Dockerfile under `worker/`), published to ghcr (fits the homelab convention of public images from ghcr; avoids the private GitLab registry for an OSS fork).
- *Garage cluster (GitOps sketch, argo-things conventions):* new `apps/12-rapidraw-storage/rapidraw-storage-application.yaml` (wave 12 — a free number) sourcing `configs/rapidraw-storage/`: a README runbook, `rapidraw-sync-credentials.yaml.example` (standard hand-made-secret pattern), a **1 Gi `longhorn-ssd` PVC for `/state`**, and a `CronJob` (`schedule: "*/15 * * * *"`, `concurrencyPolicy: Forbid`, image `ghcr.io/xerootg/rrcloud-worker`, env from the secret, the PVC mounted at `/state`, `RRCLOUD_ENDPOINT=http://garage.garage.svc.cluster.local:3900`, `RRCLOUD_REGION=garage`, path-style) — in-cluster endpoint, no FUSE mounts anywhere (the two mountpoint-s3 incidents argue hard for the worker speaking S3 natively). `Forbid` + the RWO PVC also serialize runs.
- *Garage bucket provisioning runbook (one-liners, current pod names, distroless `/garage` direct):*

```
kubectl exec -n garage garage-nas-0 -- /garage key create rapidraw-key
kubectl exec -n garage garage-nas-0 -- /garage bucket create rapidraw-library
kubectl exec -n garage garage-nas-0 -- /garage bucket allow rapidraw-library --read --write --key rapidraw-key
# optional: /garage bucket set-quotas rapidraw-library --max-size 500GiB
kubectl create secret generic rapidraw-sync-credentials -n rapidraw \
  --from-literal=access_key=GK… --from-literal=secret_key=…
```

Phone endpoint: `https://garage.themissing.xyz` (already public, wildcard TLS, no Cloudflare body cap, 900 s read timeout), region `garage`, path-style forced. Revocation = `garage key delete`. Optional `library` mirroring for other consumers via a read-only grant to `s3-csi-driver`.

---

## 7. Fork & rebase strategy

- **Creation:** `gh repo fork CyberTimon/RapidRAW --org xerootg --fork-name RapidRawCloud` (the repo exists empty — push the fork content: add upstream remote, push `main` = upstream `main`, then branch `cloud` as the default release branch). `main` perpetually tracks upstream; `cloud` = `main` + the feature commits.
- **Commit discipline on `cloud`:** (1) one commit series that only *adds* files (crate, module, plugin, bin, workflows, docs); (2) one small "hooks" commit per upstream file touched, each line-local. `docs/UPSTREAM_TOUCHES.md` enumerates every hook with a one-line rationale — the rebase checklist.
- **Single hook style (resolving the §3.3-vs-§7 inconsistency the review flagged):** hook call sites in upstream files are **unconditional one-line calls** into always-compiled `sync::hooks::*` shims — no `#[cfg]` at call sites. The `sync` cargo feature (default on in the fork) controls whether the shim *bodies* do anything; `--no-default-features` yields a build that is **functionally identical to upstream** (shims are inert no-ops; the binary is not claimed to be bit-identical).
- **Upstream files touched (updated, honest list):**

| File | Change |
|---|---|
| `src-tauri/Cargo.toml` | workspace member, deps (`rrcloud-core`, `redb`, `hmac`, `sha2`, `md-5`, `quick-xml`, `dashmap`, `flate2`), `sync` feature (default on) |
| `src-tauri/src/lib.rs` | `mod sync;` + register ~12 commands + `SyncManager` start in `setup()` + exit-flush hook + `ensure_local` guards in the HDR/pano/focus-stack/denoise merge commands |
| `src-tauri/src/app_state.rs` | `sync_manager: Arc<SyncManager>` field + sidecar lock map |
| `src-tauri/src/app_settings.rs` | `sync: SyncSettings` field |
| `src-tauri/src/exif_processing.rs` | `save_sidecar` chokepoint (incl. corruption + remote-head guards); `load_sidecar` warn-on-parse-failure |
| `src-tauri/src/file_management.rs` | ~12 write sites → chokepoint; `is_cloud_placeholder` `|| sync::is_stub`; `ensure_local` guards in copy/move/duplicate/delete/preview/dimensions; import/derived-output/delete sync hooks; XMP-import hash gate; `ImageFile.sync_state` |
| `src-tauri/src/tagging.rs` | 4 write sites → chokepoint |
| `src-tauri/src/image_loader.rs` | proxy branch + `ensure_local` replacing the iCloud error at `:940`; pin `(apply_ungamma, apply_calibration)` for tagged proxies past `linear_raw_mode` at `:88`; `proxy_scale`-scaled `remove_raw_artifacts_and_enhance` amounts |
| `src-tauri/src/raw_processing.rs` | `clamp_limit` fix: `fast_demosaic && !is_linear_format` (§4.1/E3) |
| `src-tauri/src/export_processing.rs` | `ensure_local` before per-image load |
| `src-tauri/src/android_integration.rs` | none (plugin is separate); only if the credential bridge needs a helper |
| `src-tauri/gen/android/...` | manifest permissions (§5.3), gradle deps, plugin registration in `MainActivity.kt` |
| `src/hooks/useTauriListeners.ts` | `sync-*` listeners |
| `src/hooks/useAppNavigation.ts` | `sync_flush_path` hint at the three existing debouncedSave flush points (`:140,162,387`) |
| `src/components/...` | status badge, pin/evict context menu, sync settings panel (incl. device management + Recently Deleted), conflict toast (new components + small mount-point edits) |
| `.github/workflows/` | add `worker-image.yml` (new file only) |

Everything else is new files (`tauri.conf.json` is deliberately untouched — thumbnail seeding uses hard links into the existing `$APPCACHE/thumbnails` scope, §3.5). Frontend edits are the least rebase-stable; keep them to mount-point one-liners importing new components.
- **Rebase cost, stated honestly:** ~20 line-local hooks land in `file_management.rs`, upstream's largest and most actively churned file. Each hook is individually trivial, but an upstream refactor of the write sites means re-locating each one — budget hours, not minutes, for a bad rebase. `docs/UPSTREAM_TOUCHES.md` plus a CI check that `--no-default-features` builds and passes upstream's tests keep drift detectable.
- **Rebase cadence:** `git fetch upstream && git checkout main && git merge --ff-only upstream/main && git rebase main cloud`; conflicts concentrate in the hook commits.

---

## 8. Phased plan

Each phase ships and is verifiable on its own; order puts edit/sidecar value first.

**P0 — Fork + harness (3–5 d).** Create repos/branches per §7; `rrcloud-core` skeleton; S3 client with SigV4; **integration harness:** `docker-compose.test.yml` with `dxflrs/garage:v2.2.0` (matching prod) and `minio/minio`; Rust integration tests spin the compose stack, run the client conformance suite — path-style, multipart, **`Content-MD5` bad-digest rejection probe**, `ListMultipartUploads`/`AbortMultipartUpload`, **ranged-GET resume**, metadata echo, ListObjectsV2 paging — against both. *Verify:* conformance green on both backends; `--no-default-features` builds and passes upstream tests.

**P1 — Sidecar + original sync, desktop-first (2–2.5 wk).** Chokepoint refactor + per-path locks + corruption/remote-head guards; redb state; journal publish/replay (frozen-segment order); per-writer manifests + deleted set; semantic hashing + XMP hash gate; upload/download state machines with Content-MD5 + streamed blake3; **version-vector conflict machinery** + deterministic loser copies; soft delete + Recently Deleted; compaction/GC per §2.10. **Two-device fakes:** two `SyncEngine`s, distinct device ids and temp roots, one bucket; scripted scenarios: concurrent offline edits (assert identical primary and *single* identical loser key on both, plus on a third device under both arrival orders — the B1/B2 regression test); delete-vs-edit with the original evicted on the editor (assert whole-item resurrection); crash-mid-multipart (resume from redb); crash-between-segment-PUT-and-DB-commit (assert byte-identical republish); corrupt-sidecar-download (quarantine not clobber); empty-local-sidecar + rating tap (assert restore-and-retry, no global demotion); churn-rewrite (EXIF cache rewrite → zero uploads); XMP external edit (exactly one upload) vs. re-listing (zero); stale-LIST simulation (delayed LIST wrapper → no deletions, no resurrections); corrupt-complete-object (flip a byte server-side → `corrupt_remote`, repair, never evict); manifest-merge bootstrap of a laggard past compacted segments (assert deletions propagate); returning-dormant-device (assert quarantine prompt, not resurrection). *Ship:* desktop builds sync end-to-end; Android syncs foregrounded.

**P2 — Stubs, hydration, pinning (1 wk).** `is_stub` dispatch, stub materialization with mtime replay, `ensure_local` with ranged resume + all §3.5 guard sites, attestation-gated LRU eviction + pinning, durable thumb/proxy store with hard-link seeding, `sync-*` UI events + grid badges (journal-fed rating/label pre-sidecar). *Verify:* fresh-device bootstrap from manifests shows full library as stubs with thumbs; no O(library) LIST in steady state (client op counters); hydrate→edit→evict round-trip keeps thumbnail keys stable; evict refused without attestation and performs read-back when forced.

**P3 — Smart previews + proxy edit mode (1.5–2 wk).** Confirm/patch RapidRAW-DngLab writer (§4.2 gap check — first task, the only external dependency; includes the field-visibility and matrix-plumbing verification); `proxy.rs` with direct `RawImage::new` construction; `clamp_limit` fix; matrix round-trip unit test (DNG-tag vs TOML Calibrate inputs bit-equal); `w,h` provenance test (journal dims == proxy-mode reported dims == hydrated dims); importing-client generation; proxy load path + `proxy_scale`; export/merge hydration forcing. *Verify:* golden-image corpus (CR3/NEF/ARW/RAF/DNG), full-res vs proxy at matched output size under aggressive adjustments (±3 EV, shadows+100, highlights−100, strong WB shift): **unclipped regions** ΔE00 ≤ 1.0 mean / ≤ 3.0 p99; **clipped-neighborhood regions included** with their own budget (ΔE00 p99 ≤ 6.0) plus mandatory visual review of recovered-highlight edges and a −4 EV shadow-push banding check.

**P4 — Headless worker + homelab deploy (1 wk).** Bin target with mandatory state dir, Dockerfile, ghcr workflow, `apps/12-rapidraw-storage` CronJob + PVC + runbook; attestation emission; thumbpacks; foreign-ingest E2E: drop a NEF via `aws s3 cp` (and the real SFTPGo path), assert phone shows stub+thumb after one cycle. *Verify:* `--once` idempotence; stateless invocation refuses to journal; GC/compaction honored with a deliberately laggard fake device (14-day cap path) and a retired device; tombstone grace + Recently Deleted restore E2E.

**P5 — Android platform (1.5–2 wk).** Tauri plugin: WorkManager jobs (no background setForeground) + user-initiated FGS path + JNI bounded-cycle bridge, Keystore credentials, DCIM observer + `GENERATION_ADDED` scan with dedupe, partial-access UX, streaming copy, permission UX, constraints settings, exit flush + unsynced notification. *Verify:* airplane-mode edit → reconnect → auto-upload with app killed in between (expedited window); bootstrap chunking across simulated 10-min windows; 200-photo DCIM batch with heap monitoring; partial-grant and revoke-permission paths; Android 15 dataSync-cap graceful stop.

**P6 — Albums/presets sync, polish (1 wk).** §2.9 relativization, conflict surfacing UI, "verify library" command, device management + retire UI, docs. *Verify:* album created on desktop appears on phone with correct paths; retire-device unblocks horizons.

---

## Appendix A — Adversarial review disposition

**Accepted and fixed (design changed, not just acknowledged):**
- **A1** → §2.4 (per-part Content-MD5, streamed blake3, BadDigest probe, `corrupt_remote`), §3.5 (attestation/read-back eviction gate), §6 (worker attests).
- **A2 / G5** → §2.7 (soft delete, grace window, Recently Deleted; data keys outlive tombstones until GC).
- **A3** → §2.3 (manifests carry the deleted set; bootstrap learns deletions; pre-upload quarantine for dormant devices), §2.10 (destruction decoupled from LIST freshness).
- **A4 / G1** → §3.5 (sidecars never stubbed, costed eager mirror, journal-fed grid metadata), §3.4 (empty/corrupt-sidecar guard; remote-head guard).
- **B1** → §2.6 (unified vv apply rule; no resolution-bypassing fast-forward; exact concurrency predicate replaces the unfinished one).
- **B2** → §2.6 (deterministic `blake3(loser)[..6]` suffix; idempotent materialization).
- **B3** → §2.5 (XMP import gated on XMP content hash; §2.9-era "zero uploads" claim corrected) + §2.6 (version-per-admitted-upload and wall-ts-among-concurrent winner replace save-counting Lamport LWW).
- **B4** → §2.3 (per-writer immutable manifests; no shared mutable key; compaction gated on the owner's own manifest).
- **B5 / G6** → §6 (durable state mandatory; PVC in the CronJob; engine refuses stateless journaling) + §2.10 (device retirement).
- **B6** → §2.1.5/§2.2 (freeze segment bytes in redb before PUT; byte-identical replay).
- **B7** → §2.8 (XMP as sidecar projection; original-overwrite conflict files).
- **B8** → §2.10 (all GC/compaction ages in server time from S3 `Date` headers).
- **C1** → §5.1 (three-mode model; FGS only from foreground; 10-min-window engine cycles; Android 15 6 h cap handled).
- **C2** → §5.2 (`GENERATION_ADDED` on API 30+, overlap rescan below; `READ_MEDIA_VISUAL_USER_SELECTED` + partial-grant UX).
- **C3** → §3.3/§5.4 (exit flush, expedited drain, "not backed up" notification), §2.10 (orphan retirement; 14-day cap bounds residual blockage).
- **D1** → §3.5/§4.3 (durable budgeted preview store outside the OS-clearable cache; small-thumbs-always costed at ~2 GB/50k; medium+proxy recency windows; thumbpacks; gzip-NDJSON manifests from v1; bootstrap numbers stated).
- **D3** → §2.10 (14-day compaction cap with safe manifest catch-up).
- **E1** → §4.1/§8-P3 (divergence named; clipped regions tested with their own budget, not excluded).
- **E2** → §4.1 (named in the divergence list; proxy-scaled NR/sharpen amounts; in the ΔE budget).
- **E3** → §4.1/§7 (line-local `clamp_limit` fix for LinearRaw).
- **E4** → §4.2 (direct `RawImage::new`, no `rgb_image_u16`; CyberTimon-fork field-visibility verification made an explicit P3 task), §4.1/§8-P3 (matrix round-trip test), §2.2/§4.4 (`w,h` provenance defined and tested).
- **F** → §7 (table extended: lib.rs merge guards, raw_processing.rs, useAppNavigation.ts, image_loader linear-mode pin; single hook style chosen; "bit-identical" softened to "functionally identical"; rebase cost restated as hours-not-minutes).
- **G2** → §3.5 (ranged-GET resume with partial re-hash). **G3** → §2.4 (`corrupt_remote` + repair). **G4** → §2.2 (min-reader fail-closed rule, `proto` advertisement). **G6 (garbled Garage sentence)** → §2.4/§6 (no lifecycle reliance anywhere; `ListMultipartUploads` is the mechanism and is harness-proven). **G7** → §2.3 (reconcile cadence contradiction resolved: clients bootstrap/manual only), §2.10 (applied-cursor staleness tolerance made explicit via the 24 h double-check and 14-day cap).

**Rejected or partially rejected (one line each):**
- **B5, second bullet** ("seq re-derived by LISTing its own prefix"): rejected as stated — the design never proposed deriving seq from LIST and now explicitly forbids journaling without persisted seq; the underlying stateless-deployment hole was accepted and fixed.
- **D2**: rejected as a defect — by the reviewer's own arithmetic the cost is pennies and foreground-only; accepted as hygiene, with polling consolidated to one LIST and the numbers now stated in §2.2.
- **B8, framing** ("asserts 'never wall clock' then quietly reintroduces it"): partially rejected — the original claim covered causality only, which held; the substantive point (device clocks gating destruction) was accepted and fixed with server time.
- **B3, "LWW will pick the wrong winner routinely"**: the frequency claim is overstated for the common two-device case, but the mechanism critique is correct and the ordering was replaced anyway.

---

### Critical Files for Implementation
- /home/user/cybertimon/rapidraw/src-tauri/src/file_management.rs — sidecar write sites, placeholder plumbing, thumbnail cache key, XMP import gate, listing/copy/move/delete hook points
- /home/user/cybertimon/rapidraw/src-tauri/src/raw_processing.rs — the LinearRaw develop branch the proxy fidelity argument rests on, plus the `clamp_limit` fix (lines 168-198)
- /home/user/cybertimon/rapidraw/src-tauri/src/image_loader.rs — load_image proxy/hydration branch, `remove_raw_artifacts_and_enhance` scaling, linear-mode pinning at :88
- /home/user/cybertimon/rapidraw/src-tauri/src/exif_processing.rs — load_sidecar/save_sidecar chokepoint with corruption and remote-head guards
- /home/user/dnglab/rawler/src/dng/writer.rs — the LinearRaw DNG writer API the proxy generator targets (verify parity and field visibility in CyberTimon/RapidRAW-DngLab @ 934af4b)