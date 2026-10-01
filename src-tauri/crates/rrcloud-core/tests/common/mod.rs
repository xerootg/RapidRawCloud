//! Shared test support for the integration suite.

pub mod garage;
// Not every test binary uses every helper; the modules are shared.
#[allow(dead_code)]
pub mod engine;
#[allow(dead_code)]
pub mod sync;
#[allow(dead_code)]
pub mod transfer;
