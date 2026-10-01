//! Shared test support for the integration suite.

pub mod garage;
// Not every test binary uses every helper; the module is shared.
#[allow(dead_code)]
pub mod sync;
