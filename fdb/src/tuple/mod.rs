//! Provides a set of utilities for serializing and deserializing
//! typed data for use in FDB.
//!
//! When data is packed together into a [`Tuple`] it is suitable for
//! use as an index or organizational structure within FDB. See
//! [general Tuple documentation] for more information about how
//! [`Tuple`] sort and can be uset to efficiently model data.
//!
//! [general Tuple documentation]: https://apple.github.io/foundationdb/data-modeling.html#data-modeling-tuples
mod element;
mod tuple;
mod versionstamp;

pub mod bytes_util;

pub use tuple::Tuple;
pub use versionstamp::Versionstamp;
