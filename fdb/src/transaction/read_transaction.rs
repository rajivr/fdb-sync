use crate::future::{FdbFutureI64, FdbFutureMaybeValue};
use crate::transaction::TransactionOption;
use crate::{FdbResult, Key};

/// A read-only subset of a FDB [`Transaction`].
///
/// [`Transaction`]: crate::transaction::Transaction
//
// NOTE: Unlike Java API, `ReadTransaction` does not extend (i.e., is
//       a subtrait of) `ReadTransactionContext`. This is to maintain
//       consistency with `Transaction`.
//
//       Also there is no `snapshot()` method on `ReadTransaction`
//       trait. Instead the `snapshot()` method is on `Transaction`.
pub trait ReadTransaction {
    /// Gets a value from the database.
    fn get(&self, key: Key) -> FdbFutureMaybeValue;

    /// Gets the version at which the reads for this [`Transaction`]
    /// will access the database.
    ///
    /// [`Transaction`]: crate::transaction::Transaction
    fn get_read_version(&self) -> FdbFutureI64;

    /// Set options on a [`Transaction`].
    ///
    /// [`Transaction`]: crate::transaction::Transaction
    fn set_option(&self, option: TransactionOption) -> FdbResult<()>;

    /// Directly sets the version of the database at which to execute
    /// reads.
    fn set_read_version(&self, version: i64);
}
