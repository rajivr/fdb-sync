#![warn(
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unreachable_pub
)]

//! FoundationDB Client for Tokio

// TODO: document the behavior of FdbFuture.
//
// 1. FdbFuture represents a computation that is happening in the
// background. In this regard it is similar to
// std::thread::JoinHandle. Unlike Rust Future, FdbFuture need not be
// polled in order to make progress. It makes progress the network
// thread.
//
// 2. Like Rust Futures, when FdbFuture is dropped, the computation is
// cancelled. In this regard, it is similar to Rust Future, and is not
// like JoinHandle. In case of JoinHandle, we only lose the ability to
// "join"

mod fdb;
mod option;

pub mod database;
pub mod error;
pub mod future;
pub mod range;
pub mod subspace;
pub mod transaction;
pub mod tuple;

/// Maximum API version supported by the client
pub use fdb_sys::FDB_API_VERSION;

pub use crate::fdb::{
    select_api_version, set_network_option, start_network, stop_network, Key, KeySelector,
    KeyValue, Value,
};

pub use crate::database::open_database::open_database;

pub use crate::option::NetworkOption;
