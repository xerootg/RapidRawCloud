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
//! - [`transfer`]: the per-item transfer engine — resumable
//!   uploads/downloads, Content-MD5, streamed blake3, temp+verify+rename,
//!   the backend digest probe, and the concurrency pump (§2.4, §3.5,
//!   §2.1.5).
//!
//! The remaining modules from architecture §3.1 (`engine`, `tombstone`,
//! `compact`, `proxy`, `thumbs`) land in later units.

pub mod clock;
pub mod journal;
pub mod keys;
pub mod manifest;
pub mod publisher;
pub mod reader;
pub mod s3;
pub mod semhash;
pub mod state;
pub mod transfer;

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
