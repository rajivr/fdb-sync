use crate::database::Database;
use crate::future::{FdbFutureI64, FdbFutureKey, FdbFutureUnit};
use crate::range::Range;
use crate::transaction::{MutationType, ReadTransaction};
use crate::{FdbError, FdbResult, Key, Value};

use bytes::Bytes;

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

    /// Adds a key to the transaction's read conflict ranges as if you
    /// had read the key.
    fn add_read_conflict_key(&self, key: Key) -> FdbResult<()>;

    /// Adds a range of keys to the transaction's read conflict ranges
    /// as if you had read the range.
    fn add_read_conflict_range(&self, range: Range) -> FdbResult<()>;

    /// Adds a key to the transaction's write conflict ranges as if
    /// you had written the key.
    fn add_write_conflict_key(&self, key: Key) -> FdbResult<()>;

    /// Adds a range of keys to the transaction's write conflict
    /// ranges as if you had cleared the range.
    fn add_write_conflict_range(&self, range: Range) -> FdbResult<()>;

    /// Cancels the [`Transaction`].
    fn cancel(&self);

    /// Clears a given key from the database.
    fn clear(&self, key: Key);

    /// Clears a range of keys from the database.
    fn clear_range(&self, range: Range);

    /// Commit this [`Transaction`].
    fn commit(&self) -> FdbFutureUnit;

    /// Returns a future that will contain the approximated size of
    /// the commit, which is the summation of mutations, read conflict
    /// ranges, and write conflict ranges.
    fn get_approximate_size(&self) -> FdbFutureI64;

    /// Gets the version number at which a successful commit modified
    /// the database.
    fn get_committed_version(&self) -> FdbResult<i64>;

    /// Returns the [`Database`] that this [`Transaction`] is
    /// interacting with.
    //
    // NOTE: `FdbDatabase` is `Arc` counted, so we don't have to worry
    //        about issuing a new copy.
    fn get_database(&self) -> Self::Database;

    /// Returns a future which will contain the versionstamp which was
    /// used by any versionstamp operations in this transaction.
    fn get_versionstamp(&self) -> FdbFutureKey;

    /// An atomic operation is a single database command that carries
    /// out several logical steps: reading the value of a key,
    /// performing a transformation on that value, and writing the
    /// result.
    fn mutate(&self, optype: MutationType, key: Key, param: Bytes);

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

    /// Creates a watch that will become ready when it reports a
    /// change to the value of the specified key.
    ///
    /// A watch's behavior is relative to the transaction that created
    /// it.
    fn watch(&self, key: Key) -> FdbFutureUnit;
}
