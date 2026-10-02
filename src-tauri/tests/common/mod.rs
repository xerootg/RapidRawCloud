//! Shared test support for the app-wiring integration suite.
//!
//! Only the Garage-backed end-to-end test pulls this in, so the whole
//! module is gated behind the `sync` feature (it reaches rrcloud-core's S3
//! client through the `rapidraw_lib::rrcloud_core` re-export).

#![cfg(feature = "sync")]

pub mod garage;
