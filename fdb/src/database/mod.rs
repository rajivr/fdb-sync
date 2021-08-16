//! Provides [`FdbDatabase`] type and [`Database`] trait for working with FDB Database.

mod database;
mod fdb_database;

#[doc(hidden)]
pub mod open_database;

pub use crate::option::DatabaseOption;

pub use database::Database;
pub use fdb_database::FdbDatabase;
