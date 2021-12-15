use tokio_util::sync::CancellationToken;

use crate::database::DatabaseOption;
use crate::error::FdbResult;
use crate::transaction::{Transaction, TransactionContext};
use crate::Key;

/// A mutable, lexicographically ordered mapping from binary keys to
/// binary values.
///
/// [`Transaction`]s are used to manipulate data
/// within a single [`Database`] - multiple concurrent [`Transaction`]s on
/// a [`Database`] enforce **ACID** properties.
///
/// The simplest correct programs using FDB will make use of the
/// methods defined in [`TransactionContext`] trait. When used on a
/// [`Database`], these methods will call [`Transaction::commit`]
/// after the user code has been executed. These methods will not
/// return until [`commit()`] has returned.
///
/// [`commit()`]: Transaction::commit
pub trait Database: TransactionContext {
    /// [`Transaction`] that can be created by [`Database`].
    type Transaction: Transaction;

    /// Creates a [`Transaction`] that operates on this [`Database`].
    ///
    /// Optionally takes a [`CancellationToken`], if you want to
    /// cancel the transaction.
    fn create_transaction(
        &self,
        cancellation_token: Option<CancellationToken>,
    ) -> FdbResult<Self::Transaction>;

    /// Set options on a [`Database`].
    fn set_option(&self, option: DatabaseOption) -> FdbResult<()>;

    /// Returns an array of [`Key`]s `k` such that `begin <= k < end`
    /// and `k` is located at the start of contiguous range stored on
    /// a single server.
    ///
    /// If `limit` is non-zero, only the first `limit` number of keys
    /// will be returned. In large databases, the number of boundary
    /// keys may be large. In these cases, a non-zero `limit` should
    /// be used, along with multiple calls to [`get_boundary_keys`].
    ///
    /// If `read_version` is non-zero, the boundary keys as of
    /// `read_version` will be returned.
    ///
    /// This method is not transactional.
    ///
    /// Optionally takes a [`CancellationToken`], if you want to
    /// cancel the transaction.
    ///
    /// [`get_boundary_keys`]: crate::database::Database::get_boundary_keys
    fn get_boundary_keys(
        &self,
        cancellation_token: Option<CancellationToken>,
        begin: Key,
        end: Key,
        limit: i32,
        read_version: i64,
    ) -> FdbResult<Vec<Key>>;
}
