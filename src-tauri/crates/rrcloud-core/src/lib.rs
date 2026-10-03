//! # rrcloud-core
//!
//! Core sync engine for RapidRawCloud (see `docs/ARCHITECTURE.md`).
//!
//! This crate is deliberately free of any Tauri dependency so it can be shared
//! between the desktop app, the Android plugin's Rust side, and the headless
//! worker binary.
//!
//! Present modules:
//!
//! - [`s3`]: hand-rolled SigV4 S3 client (architecture §3.9).
//! - [`keys`]: relkey mapping and the bucket key schema (§1.1–§1.2).
//! - [`semhash`]: semantic sidecar hashing and content hashing (§2.5).
//! - [`clock`]: device identity, version vectors, conflict winner (§2.6).
//! - [`journal`]: journal entry/segment schema and tombstones (§2.2, §2.7).
//! - [`state`]: the redb-backed durable state store (§3.2, §2.1.5, §2.4).
//! - [`publisher`]: the outbound journal lane + device-registry heartbeat
//!   (§2.1.5, §2.2, §1.2).
//! - [`reader`]: the inbound journal lane — poll, fail-closed apply,
//!   cursors (§2.2).
//! - [`manifest`]: per-writer manifests — build/encode/transfer/merge
//!   (§2.3).
//! - [`meta`]: albums & presets as synced whole-document meta objects —
//!   `rr://` relativization and the §2.6 whole-document resolution reused
//!   for them (§2.9).
//! - [`transfer`]: the per-item transfer engine — resumable
//!   uploads/downloads, Content-MD5, streamed blake3, temp+verify+rename,
//!   the backend digest probe, and the concurrency pump (§2.4, §3.5,
//!   §2.1.5).
//! - [`engine`]: the sync engine for one device — §2.5 churn-gated
//!   change intake, §3.7 quiescence admission (the §2.6 version mint),
//!   the §2.6 unified apply rule as a `JournalConsumer`, §2.7 soft
//!   delete/restore/resurrection, §2.8 original-overwrite conflicts.
//! - [`compact`]: §2.10 compaction, horizons, GC, device lifecycle, and
//!   the §2.3 pre-upload quarantine decision — the pure server-time
//!   decisions and their S3 effects, driven by explicit entry points.
//! - [`worker`]: the headless worker (§6) — the idempotent, crash-safe
//!   reconcile / foreign-adoption / proxy-backfill / §2.10-GC cycle, as
//!   pure orchestration over the modules above. Backs the
//!   `rrcloud-worker` bin (`src/bin/rrcloud-worker.rs`), which depends on
//!   this crate **only** (a documented refinement of §6 — no
//!   `rapidraw_lib`/tauri linkage; color parity comes from [`proxy`]).

pub mod clock;
pub mod compact;
pub mod engine;
pub mod journal;
pub mod keys;
pub mod manifest;
pub mod meta;
pub mod proxy;
pub mod publisher;
pub mod reader;
pub mod s3;
pub mod semhash;
pub mod state;
pub mod transfer;
pub mod worker;

/// Crate-private helpers shared across modules.
pub(crate) mod hexutil {
    /// `true` when `s` is exactly `len` lowercase hex characters.
    ///
    /// The single spelling of this predicate for the whole crate — key
    /// classification (`keys`), hash validation (`semhash`), and segment
    /// filename parsing (`journal`) must never drift apart on what counts
    /// as hex (e.g. one of them accepting uppercase would silently widen
    /// `classify_key`'s Foreign boundary).
    pub(crate) fn is_lower_hex(s: &str, len: usize) -> bool {
        s.len() == len
            && s.bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    }
}
