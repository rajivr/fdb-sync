use crate::future::{FdbFutureI64, FdbFutureKey, FdbFutureMaybeValue};
use crate::range::{Range, RangeOptions, RangeResult};
use crate::transaction::TransactionOption;
use crate::{FdbResult, Key, KeySelector};

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
    fn get(&self, key: Key) -> FdbFutureMaybeValue;

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
    fn get_estimated_range_size_bytes(&self, range: Range) -> FdbFutureI64;

    /// Returns the key referenced by the specificed [`KeySelector`].
    fn get_key(&self, selector: KeySelector) -> FdbFutureKey;

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