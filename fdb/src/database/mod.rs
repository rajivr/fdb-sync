//! Provides [`FdbDatabase`] type and [`Database`] trait for working
//! with FDB Database.
//!
//! Clients operating on a [`Database`] should, in most cases use the
//! [`run`] method. This implements a proper retry loop around the
//! work that needs to be done and, in case of [`Database`], assure
//! that [`commit`] has returned successfully before returning.
//!
//! [`run`]: crate::transaction::TransactionContext::run
//! [`commit`]: crate::transaction::Transaction::commit

mod database;
mod fdb_database;

#[doc(hidden)]
pub mod open_database;

pub use crate::option::DatabaseOption;

pub use database::Database;
pub use fdb_database::FdbDatabase;
