use crate::database::FdbDatabase;
use crate::future::{FdbFuture, FdbFutureI64, FdbFutureMaybeValue, FdbFutureUnit};
use crate::transaction::{
    ReadTransaction, ReadTransactionContext, Transaction, TransactionContext, TransactionOption,
};
use crate::{FdbError, FdbResult, Key, Value};
use bytes::Bytes;
use std::convert::TryInto;
use std::ptr::NonNull;

/// A handle to a FDB transaction.
///
/// [`create_transaction`] method on [`Database`] trait implemented
/// for [`FdbDatabase`] can be used to create an [`FdbTransaction`].
///
/// [`create_transaction`]: crate::database::Database::create_transaction
/// [`Database`]: crate::database::Database
#[derive(Debug)]
pub struct FdbTransaction {
    c_ptr: Option<NonNull<fdb_sys::FDBTransaction>>,
    fdb_database: FdbDatabase,
    read_snapshot: ReadSnapshot,
}

impl FdbTransaction {
    pub(crate) fn new(
        c_ptr: NonNull<fdb_sys::FDBTransaction>,
        fdb_database: FdbDatabase,
    ) -> FdbTransaction {
        FdbTransaction {
            c_ptr: Some(c_ptr),
            fdb_database,
            read_snapshot: ReadSnapshot {
                c_ptr: c_ptr.as_ptr(),
            },
        }
    }

    fn get_read_snapshot(&self) -> &ReadSnapshot {
        &self.read_snapshot
    }
}

impl Drop for FdbTransaction {
    fn drop(&mut self) {
        self.c_ptr.take().map(|x| unsafe {
            fdb_sys::fdb_transaction_destroy(x.as_ptr());
        });
    }
}

impl ReadTransactionContext for FdbTransaction {
    fn read<T, F>(&self, f: F) -> FdbResult<T>
    where
        Self: Sized,
        F: Fn(&dyn ReadTransaction) -> FdbResult<T>,
    {
        f(self)
    }
}

impl ReadTransaction for FdbTransaction {
    fn get(&self, key: Key) -> FdbFutureMaybeValue {
        // Safety: It is safe to unwrap here because if we have a
        // `self: &FdbTransaction`, then `c_ptr` *must* be
        // `Some<NonNull<...>>`.
        internal::read_transaction::get((self.c_ptr.unwrap()).as_ptr(), key, false)
    }

    fn get_read_version(&self) -> FdbFutureI64 {
        // Safety: It is safe to unwrap here because if we have a
        // `self: &FdbTransaction`, then `c_ptr` *must* be
        // `Some<NonNull<...>>`.
        internal::read_transaction::get_read_version((self.c_ptr.unwrap()).as_ptr())
    }

    fn set_option(&self, option: TransactionOption) -> FdbResult<()> {
        // Safety: It is safe to unwrap here because if we have a
        // `self: &FdbTransaction`, then `c_ptr` *must* be
        // `Some<NonNull<...>>`.
        internal::read_transaction::set_option((self.c_ptr.unwrap()).as_ptr(), option)
    }

    fn set_read_version(&self, version: i64) {
        // Safety: It is safe to unwrap here because if we have a
        // `self: &FdbTransaction`, then `c_ptr` *must* be
        // `Some<NonNull<...>>`.
        internal::read_transaction::set_read_version((self.c_ptr.unwrap()).as_ptr(), version)
    }
}

impl TransactionContext for FdbTransaction {
    type Database = FdbDatabase;

    fn run<T, F>(&self, f: F) -> FdbResult<T>
    where
        Self: Sized,
        F: Fn(&dyn Transaction<Database = Self::Database>) -> FdbResult<T>,
    {
        f(self)
    }
}

impl Transaction for FdbTransaction {
    type Database = FdbDatabase;

    fn cancel(&self) {
        unsafe {
            // Safety: It is safe to unwrap here because if we have a
            // `self: &FdbTransaction`, then `c_ptr` *must* be
            // `Some<NonNull<...>>`.
            fdb_sys::fdb_transaction_cancel((self.c_ptr.unwrap()).as_ptr());
        }
    }

    fn commit(&self) -> FdbFutureUnit {
        FdbFuture::new(unsafe {
            // Safety: It is safe to unwrap here because if we have a
            // `self: &FdbTransaction`, then `c_ptr` *must* be
            // `Some<NonNull<...>>`.
            fdb_sys::fdb_transaction_commit((self.c_ptr.unwrap()).as_ptr())
        })
    }

    fn get_database(&self) -> FdbDatabase {
        self.fdb_database.clone()
    }

    fn on_error(&self, e: FdbError) -> FdbFutureUnit {
        FdbFuture::new(unsafe {
            // Safety: It is safe to unwrap here because if we have a
            // `self: &FdbTransaction`, then `c_ptr` *must* be
            // `Some<NonNull<...>>`.
            fdb_sys::fdb_transaction_on_error((self.c_ptr.unwrap()).as_ptr(), e.code())
        })
    }

    fn set(&self, key: Key, value: Value) {
        let k = Bytes::from(key);
        let key_name = k.as_ref().as_ptr();
        let key_name_length = k.as_ref().len().try_into().unwrap();

        // `value` is being overridden to get naming consistent with C
        // API parameters
        let v = Bytes::from(value);
        let value = v.as_ref().as_ptr();
        let value_length = v.as_ref().len().try_into().unwrap();

        unsafe {
            // Safety: It is safe to unwrap here because if we have a
            // `self: &FdbTransaction`, then `c_ptr` *must* be
            // `Some<NonNull<...>>`.
            fdb_sys::fdb_transaction_set(
                (self.c_ptr.unwrap()).as_ptr(),
                key_name,
                key_name_length,
                value,
                value_length,
            )
        }
    }

    fn snapshot(&self) -> &dyn ReadTransaction {
        self.get_read_snapshot()
    }
}

/// A handle to a FDB snapshot, suitable for performing snapshot
/// reads.
///
/// Snapshot reads offer more relaxed isolation level than FDB's
/// default serializable isolation, reducing transaction conflicts but
/// making it harder to reason about concurrency.
///
/// For more information about how to use snapshot reads correctly,
/// see [`snapshot reads`].
///
/// [`snapshot`] method on [`Transaction`] trait implemented for
/// [`FdbTransaction`] can be used to create a [`ReadSnapshot`].
///
/// **NOTE**: This type is not directly exposed. Instead
/// `ReadSnapshot` is returned as `&dyn ReadTransaction`.
///
/// [`snapshot reads`]: https://apple.github.io/foundationdb/developer-guide.html#snapshot-reads
/// [`snapshot`]: crate::transaction::Transaction::snapshot
//
// The `ReadSnapshot.c_ptr` is contained inside `FdbTransaction`. The
// *only* time when this `c_ptr` is invalid is after `Drop::drop` is
// called on `FdbTransaction` and before the compiler fully discards
// `ReadSnapshot`. This is safe because `ReadSnapshot` is `!Copy +
// !Clone`, so `ReadSnapshot.c_ptr` is tied to the lifetime of
// `FdbTransaction`.
#[derive(Debug)]
struct ReadSnapshot {
    c_ptr: *mut fdb_sys::FDBTransaction,
}

impl ReadTransaction for ReadSnapshot {
    fn get(&self, key: Key) -> FdbFutureMaybeValue {
        internal::read_transaction::get(self.c_ptr, key, true)
    }

    fn get_read_version(&self) -> FdbFutureI64 {
        internal::read_transaction::get_read_version(self.c_ptr)
    }

    fn set_option(&self, option: TransactionOption) -> FdbResult<()> {
        internal::read_transaction::set_option(self.c_ptr, option)
    }

    fn set_read_version(&self, version: i64) {
        internal::read_transaction::set_read_version(self.c_ptr, version)
    }
}

pub(super) mod internal {
    pub(super) mod read_transaction {
        use crate::future::{FdbFuture, FdbFutureI64, FdbFutureMaybeValue};
        use crate::transaction::TransactionOption;
        use crate::{FdbResult, Key};
        use bytes::Bytes;
        use std::convert::TryInto;

        pub(crate) fn get(
            transaction: *mut fdb_sys::FDBTransaction,
            key: Key,
            snapshot: bool,
        ) -> FdbFutureMaybeValue {
            let k = Bytes::from(key);
            let key_name = k.as_ref().as_ptr();
            let key_name_length = k.as_ref().len().try_into().unwrap();
            let s = if snapshot { 1 } else { 0 };

            FdbFuture::new(unsafe {
                fdb_sys::fdb_transaction_get(transaction, key_name, key_name_length, s)
            })
        }

        pub(crate) fn get_read_version(transaction: *mut fdb_sys::FDBTransaction) -> FdbFutureI64 {
            FdbFuture::new(unsafe { fdb_sys::fdb_transaction_get_read_version(transaction) })
        }

        pub(crate) fn set_option(
            transaction: *mut fdb_sys::FDBTransaction,
            option: TransactionOption,
        ) -> FdbResult<()> {
            unsafe { option.apply(transaction) }
        }

        pub(crate) fn set_read_version(transaction: *mut fdb_sys::FDBTransaction, version: i64) {
            unsafe { fdb_sys::fdb_transaction_set_read_version(transaction, version) }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{FdbTransaction, ReadSnapshot};
    use crate::transaction::{
        ReadTransaction, ReadTransactionContext, Transaction, TransactionContext,
    };
    use impls::impls;

    #[test]
    fn impls() {
        #[rustfmt::skip]
        assert!(impls!(
	    FdbTransaction:
	        ReadTransactionContext &
		ReadTransaction &
		TransactionContext &
		Transaction &
		Drop &
		!Copy &
		!Clone &
		!Send &
		!Sync));

        #[rustfmt::skip]
        assert!(impls!(
	    ReadSnapshot:
	        ReadTransaction &
		!Drop &
		!Copy &
		!Clone &
		!Send &
		!Sync));
    }
}
