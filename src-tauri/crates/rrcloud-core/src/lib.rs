//! # rrcloud-core
//!
//! Core sync engine for RapidRawCloud (see `docs/ARCHITECTURE.md`).
//!
//! This crate is deliberately free of any Tauri dependency so it can be shared
//! between the desktop app, the Android plugin's Rust side, and the headless
//! worker binary.
//!
//! Currently only the [`s3`] module (hand-rolled SigV4 S3 client, architecture
//! §3.9) is present; the remaining modules from architecture §3.1 (`keys`,
//! `journal`, `manifest`, `state`, `semhash`, `clock`, `engine`, `transfer`,
//! `tombstone`, `compact`, `proxy`, `thumbs`) land in later units.

pub mod s3;
