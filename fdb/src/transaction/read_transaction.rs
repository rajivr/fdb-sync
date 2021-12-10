use crate::error::{FdbError, FdbResult};
use crate::future::{
    FdbFutureCStringArray, FdbFutureI64, FdbFutureKey, FdbFutureMaybeValue, FdbFutureUnit,
};
use crate::range::{Range, RangeOptions, RangeResult};
use crate::transaction::{ReadTransactionContext, TransactionOption};
use crate::{Key, KeySelector};

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
    /// Adds the read [`conflict range`] that this [`ReadTransaction`]
    /// would have added as if it had read the given key.
    ///
    /// As a result, other transactions that concurrently write this
    /// key could cause the transaction to fail with a conflict.
    ///
    /// [`conflict range`]: https://apple.github.io/foundationdb/developer-guide.html#conflict-ranges
    fn add_read_conflict_key_if_not_snapshot(&self, key: Key) -> FdbResult<()>;

    /// Adds the read [`conflict range`] that this [`ReadTransaction`]
    /// would have added as if it had read the given key range.
    ///
    /// As a result, other transactions that concurrently write a key
    /// in this range could cause the transaction to fail with a
    /// conflict.
    ///
    /// [`conflict range`]: https://apple.github.io/foundationdb/developer-guide.html#conflict-ranges
    fn add_read_conflict_range_if_not_snapshot(&self, range: Range) -> FdbResult<()>;

    /// Gets a value from the database.
    fn get<'t>(&'t self, key: Key) -> FdbFutureMaybeValue<'t>;

    /// Get a list of public network addresses as [`CString`], one for
    /// each of the storage servers responsible for storing [`Key`]
    /// and its associated value.
    ///
    /// [`CString`]: std::ffi::CString
    fn get_addresses_for_key<'t>(&'t self, key: Key) -> FdbFutureCStringArray<'t>;

    /// Gets an estimate for the number of bytes stored in the given
    /// range.
    ///
    /// *Note*: The estimated size is calculated based on the sampling
    /// done by FDB server. The sampling algorithm roughly works this
    /// way: The sampling algorithm works roughly in this way: the
    /// lager the key-value pair is, the more likely it would be
    /// sampled and the more accurate its sampled size would be. And
    /// due to that reason, it is recommended to use this API to query
    /// against large ranges for accuracy considerations. For a rough
    /// reference, if the returned size is larger than 3MB, one can
    /// consider the size to be accurate.
    fn get_estimated_range_size_bytes<'t>(&'t self, range: Range) -> FdbFutureI64<'t>;

    /// Returns the key referenced by the specificed [`KeySelector`].
    fn get_key<'t>(&'t self, selector: KeySelector) -> FdbFutureKey<'t>;

    /// Gets an ordered range of keys and values from the database.
    ///
    /// The returned [`RangeResult`] implements [`Iterator`] trait
    /// that yields a [`KeyValue`] item.
    ///
    /// [`KeyValue`]: crate::KeyValue
    fn get_range<'t>(
        &'t self,
        begin: KeySelector,
        end: KeySelector,
        options: RangeOptions,
    ) -> RangeResult<'t>;

    /// Gets the version at which the reads for this [`Transaction`]
    /// will access the database.
    ///
    /// # Safety
    ///
    /// The [`FdbFuture`] resolves to an [`i64`] instead of a [`u64`]
    /// because of [internal representation]. Even though it is an
    /// [`i64`], the future will always return a positive
    /// number. Negative GRV numbers are used internally within FDB.
    ///
    /// You only rely on GRV only for read-only transactions. For
    /// read-write transactions you should use commit version.
    ///
    /// [`Transaction`]: crate::transaction::Transaction
    /// [`FdbFuture`]: crate::future::FdbFuture
    /// [internal representation]: https://github.com/apple/foundationdb/blob/6.3.22/fdbclient/FDBTypes.h#L32
    unsafe fn get_read_version<'t>(&'t self) -> FdbFutureI64<'t>;

    /// Gets whether this transaction is a snapshot view of the
    /// database.
    fn is_snapshot(&self) -> bool;

    /// Determines whether an error returned by a [`Transaction`]
    /// method is retryable. Waiting on the returned future will
    /// return the same error when fatal, or return `()` for retryable
    /// errors.
    ///
    /// Typical code will not used this method directly. It is used by
    /// [`run`] and [`read`] methods when they need to implement
    /// correct retry loop.
    ///
    /// # Safety
    ///
    /// See [C API] for more details.
    ///
    /// [`Transaction`]: crate::transaction::Transaction
    /// [`run`]: crate::transaction::TransactionContext::run
    /// [`read`]: crate::transaction::ReadTransactionContext::read
    /// [C API]: https://apple.github.io/foundationdb/api-c.html#c.fdb_transaction_on_error
    unsafe fn on_error<'t>(&'t self, e: FdbError) -> FdbFutureUnit<'t>;

    /// Set options on a [`Transaction`].
    ///
    /// [`Transaction`]: crate::transaction::Transaction
    fn set_option(&self, option: TransactionOption) -> FdbResult<()>;

    /// Directly sets the version of the database at which to execute
    /// reads.
    ///
    /// # Safety
    ///
    /// See [C API] for more details.
    ///
    /// [C API]: https://apple.github.io/foundationdb/api-c.html#c.fdb_transaction_set_read_version
    unsafe fn set_read_version(&self, version: i64);
}

impl ReadTransactionContext for &dyn ReadTransaction {
    fn read<T, F>(&self, f: F) -> FdbResult<T>
    where
        Self: Sized,
        F: Fn(&dyn ReadTransaction) -> FdbResult<T>,
    {
        f(*self)
    }
}

#[cfg(test)]
mod tests {
    use impls::impls;

    use crate::transaction::ReadTransactionContext;

    use super::ReadTransaction;

    #[test]
    fn impls() {
        #[rustfmt::skip]
        assert!(impls!(
	    &dyn ReadTransaction:
                ReadTransactionContext));
    }
}
