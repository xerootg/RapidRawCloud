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
//!
//! The remaining modules from architecture §3.1 (`manifest`, `state`,
//! `engine`, `transfer`, `tombstone`, `compact`, `proxy`, `thumbs`) land in
//! later units.

pub mod clock;
pub mod journal;
pub mod keys;
pub mod s3;
pub mod semhash;
