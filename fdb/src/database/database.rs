use crate::database::DatabaseOption;
use crate::transaction::{Transaction, TransactionContext};
use crate::FdbResult;

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
/// return until `commit()` has returned.
pub trait Database: TransactionContext {
    /// [`Transaction`] that can be created by [`Database`].
    type Transaction: Transaction;

    /// Creates a [`Transaction`] that operates on this [`Database`].
    fn create_transaction(&self) -> FdbResult<Self::Transaction>;

    /// Set options on a [`Database`].
    fn set_option(&self, option: DatabaseOption) -> FdbResult<()>;
}
