use crate::error::FdbResult;
use crate::transaction::ReadTransaction;

/// A context in which [`ReadTransaction`] is available for database
/// operations.
///
/// The behavior of methods specified in this trait, especially in
/// face of errors, is implementation specific.
///
/// In particular, some implementations will run closure (`F: Fn(&dyn
/// ReadTransaction) -> FdbResult<T>`) multiple times (retry) when
/// certain errors are encountered. Therefore the closure should be
/// prepared to be called more than once. This consideration means
/// that the closure should use caution when modifying state.
pub trait ReadTransactionContext {
    /// Runs a closure in this context that takes a read-only
    /// transaction.
    fn read<T, F>(&self, f: F) -> FdbResult<T>
    where
        Self: Sized,
        F: Fn(&dyn ReadTransaction) -> FdbResult<T>;
}
