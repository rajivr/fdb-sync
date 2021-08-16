use crate::database::Database;
use crate::future::FdbFutureUnit;
use crate::transaction::ReadTransaction;
use crate::{FdbError, Key, Value};

/// A [`Transaction`] represents a FDB database transaction.
///
/// All operations on FDB take place, explicity or implicity, through
/// a [`Transaction`].
///
/// In FDB, a transaction is a mutable snapshot of a database. All
/// read and write operations on a transaction see and modify an
/// otherwise-unchanging version of the database and only change the
/// underlying database if and when the transaction is committed. Read
/// operations do see the effects of previous write operations on the
/// same transactions. Committing a transaction usually succeeds in
/// the absence of [conflicts].
///
/// Transactions group operations into a unit with the properties of
/// atomicity, isolation, and durability. Transactions also provide
/// the ability to maintain an application's invariants or integrity
/// constraints, supporting the property of consistency. Together
/// these properties are known as [ACID].
///
/// Transactions are also causally consistent: once a transaction has
/// been successfully committed, all subsequently created transactions
/// will see the modifications made by it. The most convenient way for
/// a developer to manage the lifecycle and retrying of a transaction
/// is to use `run` method on a type that implements [`Database`]
/// trait (for example [`FdbDatabase`]). Otherwise, the client must
/// have retry logic for fatal failures, failures to commit, and other
/// transient errors.
///
/// Keys and values in FDB are byte arrays. To encode other data
/// types, see the [tuple layer] documentation.
///
/// **Note**: All keys with fist byte `0xff` are reserved for internal use.
///
/// [conflicts]: https://apple.github.io/foundationdb/developer-guide.html#developer-guide-transaction-conflicts
/// [ACID]: https://apple.github.io/foundationdb/developer-guide.html#acid
/// [`FdbDatabase`]: crate::database::FdbDatabase
/// [tuple layer]: TODO
//
// NOTE: Unlike Java API, `Transaction` does not extend (i.e., is a
//       subtrait of) `TransactionContext`. Trying to add
//       `TransactionContext` as a super trait causes rust to
//       complain. Also `snapshot()` method is on `Transaction` trait
//       instead of `ReadTransaction` trait.
pub trait Transaction: ReadTransaction {
    /// [`Database`] associated with the [`Transaction`].
    type Database: Database;

    /// Cancels the [`Transaction`].
    fn cancel(&self);

    /// Commit this [`Transaction`].
    fn commit(&self) -> FdbFutureUnit;

    /// Returns the [`Database`] that this [`Transaction`] is
    /// interacting with.
    //
    // NOTE: `FdbDatabase` is `Arc` counted, so we don't have to worry
    //        about issuing a new copy.
    fn get_database(&self) -> Self::Database;

    /// Determines whether an error returned by a [`Transaction`]
    /// method is retryable. Waiting on the returned future will
    /// return the same error when fatal, or return `()` for retryable
    /// errors.
    ///
    /// Typical code will not used this method directly. It is used by
    /// `run` and `read` methods when they need to implement correct retry
    /// loop.
    fn on_error(&self, e: FdbError) -> FdbFutureUnit;

    /// Sets the value for a given key.
    fn set(&self, key: Key, value: Value);

    /// Return a special-purpose, read-only view of the
    /// database. Reads done using `snapshot()` are known as
    /// *snapshot reads*. Snapshot reads selectively relax FDB's
    /// isolation property, reducing transaction conflicts but making
    /// reasoning about concurrency harder.
    ///
    /// For more information about how to use snapshot reads
    /// correctly, see [`snapshot reads`].
    ///
    /// [`snapshot reads`]: https://apple.github.io/foundationdb/developer-guide.html#snapshot-reads
    fn snapshot(&self) -> &dyn ReadTransaction;
}
