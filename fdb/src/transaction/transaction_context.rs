use bytes::Bytes;

use crate::database::Database;
use crate::error::FdbResult;
use crate::transaction::{ReadTransactionContext, Transaction};

/// The context in which [`Transaction`] is available for database
/// operations.
///
/// The behavior of methods specified in this trait, especially in
/// face of errors, is implementation specific.
///
/// In particular, some implementations will run closure (`F: Fn(&dyn
/// Transaction<Database = Self::Database>) -> FdbResult<T>`) multiple
/// times (retry) when certain errors are encountered. Therefore the
/// closure should be prepared to be called more than once. This
/// consideration means that the closure should use caution when
/// modifying state.
pub trait TransactionContext: ReadTransactionContext {
    /// [`Database`] associated with the [`Transaction`]
    type Database: Database;

    /// Runs a closure in the context that takes a [`Transaction`].
    fn run<T, F>(&self, f: F) -> FdbResult<T>
    where
        Self: Sized,
        F: Fn(&dyn Transaction<Database = Self::Database>) -> FdbResult<T>;

    /// Runs a closure in the context that takes a [`Transaction`] and
    /// returns a Rust tuple containing the result of the closure and
    /// the [`Versionstamp`] without the user version.
    ///
    /// [`Versionstamp`]: crate::tuple::Versionstamp
    fn run_and_get_versionstamp<T, F>(&self, f: F) -> FdbResult<(T, Bytes)>
    where
        Self: Sized,
        F: Fn(&dyn Transaction<Database = Self::Database>) -> FdbResult<T>;

    /// Runs a closure in the context that takes a [`Transaction`] and
    /// returns a Rust tuple containing the result of the closure and
    /// the [`Transaction`] after the transaction has committed.
    //
    // We need to put `Transaction<Database = Self::Database>` inside
    // a `Box<dyn Transaction<Database = Self::Database>>`
    // because within traits we cannot do
    // `impl Transaction<Database = Self::Database>>`
    fn run_and_get_transaction<T, F, Tr>(
        self,
        f: F,
    ) -> FdbResult<(T, Box<dyn Transaction<Database = Self::Database>>)>
    where
        Self: Sized,
        F: Fn(&dyn Transaction<Database = Self::Database>) -> FdbResult<T>;
}
