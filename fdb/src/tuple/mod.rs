//! Provides a set of utilities for serializing and deserializing
//! typed data for use in FDB.

mod element;
mod tuple;
mod versionstamp;

pub use tuple::Tuple;
pub use versionstamp::Versionstamp;
