//! Provides types and traits for working with FDB Transactions and
//! Snapshots.

mod fdb_transaction;
mod read_transaction;
mod read_transaction_context;
mod transaction;
mod transaction_context;

pub use crate::option::MutationType;
pub use crate::option::TransactionOption;

pub use fdb_transaction::FdbTransaction;
pub use read_transaction::ReadTransaction;
pub use read_transaction_context::ReadTransactionContext;
pub use transaction::Transaction;
pub use transaction_context::TransactionContext;
