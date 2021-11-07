use bytes::Bytes;

use std::convert::TryInto;
use std::marker::PhantomData;
use std::ptr::NonNull;

use crate::database::FdbDatabase;
use crate::error::{
    check, FdbError, FdbResult, TRANSACTION_ADD_READ_CONFLICT_KEY_IF_NOT_SNAPSHOT,
    TRANSACTION_ADD_READ_CONFLICT_RANGE_IF_NOT_SNAPSHOT,
};
use crate::future::{FdbFuture, FdbFutureI64, FdbFutureKey, FdbFutureMaybeValue, FdbFutureUnit};
use crate::option::ConflictRangeType;
use crate::range::{fdb_transaction_get_range, Range, RangeOptions, RangeResult};
use crate::transaction::{MutationType, ReadTransaction, Transaction, TransactionOption};
use crate::{Key, KeySelector, Value};

/// A handle to a FDB transaction.
///
/// [`create_transaction`] method on [`Database`] trait implemented
/// for [`FdbDatabase`] can be used to create an [`FdbTransaction`].
///
/// [`create_transaction`]: crate::database::Database::create_transaction
/// [`Database`]: crate::database::Database
//
// NOTE: `FdbTransaction` being `!Send + !Sync` is a *very* important
//       part of the API design. The *only* way to create a value of
//       `FdbTransaction` type is by `Database::create_transaction`
//       method. Once a value of `FdbTransaction` is created, it can
//       *only* be moved around in the thread that it was created in
//       and dropped in that thread. This is what allows us to safely
//       return `TransactionContext::run_and_get_transaction` method.
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

impl ReadTransaction for FdbTransaction {
    fn add_read_conflict_key_if_not_snapshot(&self, key: Key) -> FdbResult<()> {
        let begin_key = key;
        // Add a 0x00 to `end_key`. `begin_key` is inclusive and
        // `end_key` is exclusive. By appending `0x00` to `end_key` we
        // can make the range contain only `begin_key`.
        let end_key =
            Bytes::from([&(*(Bytes::from(begin_key.clone()))), &[0x00u8]].concat()).into();

        // Safety: It is safe to unwrap here because if we have a
        // `self: &FdbTransaction`, then `c_ptr` *must* be
        // `Some<NonNull<...>>`.
        internal::add_conflict_range(
            (self.c_ptr.unwrap()).as_ptr(),
            begin_key,
            end_key,
            ConflictRangeType::Read,
        )
    }

    fn add_read_conflict_range_if_not_snapshot(&self, range: Range) -> FdbResult<()> {
        let (begin, end) = range.destructure();

        // Safety: It is safe to unwrap here because if we have a
        // `self: &FdbTransaction`, then `c_ptr` *must* be
        // `Some<NonNull<...>>`.
        internal::add_conflict_range(
            (self.c_ptr.unwrap()).as_ptr(),
            begin,
            end,
            ConflictRangeType::Read,
        )
    }

    fn get<'t>(&'t self, key: Key) -> FdbFutureMaybeValue<'t> {
        // Safety: It is safe to unwrap here because if we have a
        // `self: &FdbTransaction`, then `c_ptr` *must* be
        // `Some<NonNull<...>>`.
        internal::read_transaction::get((self.c_ptr.unwrap()).as_ptr(), key, false)
    }

    fn get_estimated_range_size_bytes<'t>(&'t self, range: Range) -> FdbFutureI64<'t> {
        let (begin, end) = range.destructure();

        // Safety: It is safe to unwrap here because if we have a
        // `self: &FdbTransaction`, then `c_ptr` *must* be
        // `Some<NonNull<...>>`.
        internal::read_transaction::get_estimated_range_size_bytes(
            (self.c_ptr.unwrap()).as_ptr(),
            begin,
            end,
        )
    }

    fn get_key<'t>(&'t self, selector: KeySelector) -> FdbFutureKey<'t> {
        // Safety: It is safe to unwrap here because if we have a
        // `self: &FdbTransaction`, then `c_ptr` *must* be
        // `Some<NonNull<...>>`.
        internal::read_transaction::get_key((self.c_ptr.unwrap()).as_ptr(), selector, false)
    }

    fn get_range<'t>(
        &'t self,
        begin: KeySelector,
        end: KeySelector,
        options: RangeOptions,
    ) -> RangeResult<'t> {
        // Safety: It is safe to unwrap here because if we have a
        // `self: &FdbTransaction`, then `c_ptr` *must* be
        // `Some<NonNull<...>>`.
        let transaction = (self.c_ptr.unwrap()).as_ptr();

        let maybe_fut_key_value_array = fdb_transaction_get_range(
            transaction,
            begin.clone(),
            end.clone(),
            options.clone(),
            false,
            1,
        );

        RangeResult::new(
            transaction,
            PhantomData,
            begin,
            end,
            options,
            false,
            maybe_fut_key_value_array,
        )
    }

    fn get_read_version<'t>(&'t self) -> FdbFutureI64<'t> {
        // Safety: It is safe to unwrap here because if we have a
        // `self: &FdbTransaction`, then `c_ptr` *must* be
        // `Some<NonNull<...>>`.
        internal::read_transaction::get_read_version((self.c_ptr.unwrap()).as_ptr())
    }

    fn is_snapshot(&self) -> bool {
        false
    }

    fn set_option(&self, option: TransactionOption) -> FdbResult<()> {
        // Safety: It is safe to unwrap here because if we have a
        // `self: &FdbTransaction`, then `c_ptr` *must* be
        // `Some<NonNull<...>>`.
        internal::read_transaction::set_option((self.c_ptr.unwrap()).as_ptr(), option)
    }

    unsafe fn set_read_version(&self, version: i64) {
        // Safety: It is safe to unwrap here because if we have a
        // `self: &FdbTransaction`, then `c_ptr` *must* be
        // `Some<NonNull<...>>`.
        internal::read_transaction::set_read_version((self.c_ptr.unwrap()).as_ptr(), version)
    }
}

impl Transaction for FdbTransaction {
    type Database = FdbDatabase;

    fn add_read_conflict_key(&self, key: Key) -> FdbResult<()> {
        let begin_key = key;
        // Add a 0x00 to `end_key`. `begin_key` is inclusive and
        // `end_key` is exclusive. By appending `0x00` to `end_key` we
        // can make the range contain only `begin_key`.
        let end_key =
            Bytes::from([&(*(Bytes::from(begin_key.clone()))), &[0x00u8]].concat()).into();

        // Safety: It is safe to unwrap here because if we have a
        // `self: &FdbTransaction`, then `c_ptr` *must* be
        // `Some<NonNull<...>>`.
        internal::add_conflict_range(
            (self.c_ptr.unwrap()).as_ptr(),
            begin_key,
            end_key,
            ConflictRangeType::Read,
        )
    }

    fn add_read_conflict_range(&self, range: Range) -> FdbResult<()> {
        let (begin, end) = range.destructure();

        // Safety: It is safe to unwrap here because if we have a
        // `self: &FdbTransaction`, then `c_ptr` *must* be
        // `Some<NonNull<...>>`.
        internal::add_conflict_range(
            (self.c_ptr.unwrap()).as_ptr(),
            begin,
            end,
            ConflictRangeType::Read,
        )
    }

    fn add_write_conflict_key(&self, key: Key) -> FdbResult<()> {
        let begin_key = key;
        // Add a 0x00 to `end_key`. `begin_key` is inclusive and
        // `end_key` is exclusive. By appending `0x00` to `end_key` we
        // can make the range contain only `begin_key`.
        let end_key =
            Bytes::from([&(*(Bytes::from(begin_key.clone()))), &[0x00u8]].concat()).into();

        // Safety: It is safe to unwrap here because if we have a
        // `self: &FdbTransaction`, then `c_ptr` *must* be
        // `Some<NonNull<...>>`.
        internal::add_conflict_range(
            (self.c_ptr.unwrap()).as_ptr(),
            begin_key,
            end_key,
            ConflictRangeType::Write,
        )
    }

    fn add_write_conflict_range(&self, range: Range) -> FdbResult<()> {
        let (begin, end) = range.destructure();

        // Safety: It is safe to unwrap here because if we have a
        // `self: &FdbTransaction`, then `c_ptr` *must* be
        // `Some<NonNull<...>>`.
        internal::add_conflict_range(
            (self.c_ptr.unwrap()).as_ptr(),
            begin,
            end,
            ConflictRangeType::Write,
        )
    }

    unsafe fn cancel(&self) {
        // Safety: It is safe to unwrap here because if we have a
        // `self: &FdbTransaction`, then `c_ptr` *must* be
        // `Some<NonNull<...>>`.
        fdb_sys::fdb_transaction_cancel((self.c_ptr.unwrap()).as_ptr());
    }

    fn clear(&self, key: Key) {
        let k = Bytes::from(key);
        let key_name = k.as_ref().as_ptr();
        let key_name_length = k.as_ref().len().try_into().unwrap();

        unsafe {
            // Safety: It is safe to unwrap here because if we have a
            // `self: &FdbTransaction`, then `c_ptr` *must* be
            // `Some<NonNull<...>>`.
            fdb_sys::fdb_transaction_clear(
                (self.c_ptr.unwrap()).as_ptr(),
                key_name,
                key_name_length,
            )
        }
    }

    fn clear_range(&self, range: Range) {
        let (begin_key, end_key) = range.destructure();

        let bk = Bytes::from(begin_key);
        let begin_key_name = bk.as_ref().as_ptr();
        let begin_key_name_length = bk.as_ref().len().try_into().unwrap();

        let ek = Bytes::from(end_key);
        let end_key_name = ek.as_ref().as_ptr();
        let end_key_name_length = ek.as_ref().len().try_into().unwrap();

        unsafe {
            // Safety: It is safe to unwrap here because if we have a
            // `self: &FdbTransaction`, then `c_ptr` *must* be
            // `Some<NonNull<...>>`.
            fdb_sys::fdb_transaction_clear_range(
                (self.c_ptr.unwrap()).as_ptr(),
                begin_key_name,
                begin_key_name_length,
                end_key_name,
                end_key_name_length,
            )
        }
    }

    unsafe fn commit<'t>(&'t self) -> FdbFutureUnit<'t> {
        FdbFuture::new(
            // Safety: It is safe to unwrap here because if we have a
            // `self: &FdbTransaction`, then `c_ptr` *must* be
            // `Some<NonNull<...>>`.
            fdb_sys::fdb_transaction_commit((self.c_ptr.unwrap()).as_ptr()),
        )
    }

    fn get_approximate_size<'t>(&'t self) -> FdbFutureI64<'t> {
        FdbFuture::new(unsafe {
            // Safety: It is safe to unwrap here because if we have a
            // `self: &FdbTransaction`, then `c_ptr` *must* be
            // `Some<NonNull<...>>`.
            fdb_sys::fdb_transaction_get_approximate_size((self.c_ptr.unwrap()).as_ptr())
        })
    }

    unsafe fn get_committed_version(&self) -> FdbResult<i64> {
        let mut out_version = 0;

        check(
            // Safety: It is safe to unwrap here because if we have a
            // `self: &FdbTransaction`, then `c_ptr` *must* be
            // `Some<NonNull<...>>`.
            fdb_sys::fdb_transaction_get_committed_version(
                (self.c_ptr.unwrap()).as_ptr(),
                &mut out_version,
            ),
        )
        .map(|_| out_version)
    }

    fn get_database(&self) -> FdbDatabase {
        self.fdb_database.clone()
    }

    unsafe fn get_versionstamp<'t>(&'t self) -> FdbFutureKey<'t> {
        FdbFuture::new(
            // Safety: It is safe to unwrap here because if we have a
            // `self: &FdbTransaction`, then `c_ptr` *must* be
            // `Some<NonNull<...>>`.
            fdb_sys::fdb_transaction_get_versionstamp((self.c_ptr.unwrap()).as_ptr()),
        )
    }

    fn mutate(&self, optype: MutationType, key: Key, param: Bytes) {
        let k = Bytes::from(key);
        let key_name = k.as_ref().as_ptr();
        let key_name_length = k.as_ref().len().try_into().unwrap();

        let p = param;
        let param = p.as_ref().as_ptr();
        let param_length = p.as_ref().len().try_into().unwrap();

        unsafe {
            // Safety: It is safe to unwrap here because if we have a
            // `self: &FdbTransaction`, then `c_ptr` *must* be
            // `Some<NonNull<...>>`.
            fdb_sys::fdb_transaction_atomic_op(
                (self.c_ptr.unwrap()).as_ptr(),
                key_name,
                key_name_length,
                param,
                param_length,
                optype.code(),
            );
        }
    }

    unsafe fn on_error<'t>(&'t self, e: FdbError) -> FdbFutureUnit<'t> {
        FdbFuture::new(
            // Safety: It is safe to unwrap here because if we have a
            // `self: &FdbTransaction`, then `c_ptr` *must* be
            // `Some<NonNull<...>>`.
            fdb_sys::fdb_transaction_on_error((self.c_ptr.unwrap()).as_ptr(), e.code()),
        )
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

    fn watch<'t>(&'t self, key: Key) -> FdbFutureUnit<'t> {
        let k = Bytes::from(key);
        let key_name = k.as_ref().as_ptr();
        let key_name_length = k.as_ref().len().try_into().unwrap();

        FdbFuture::new(unsafe {
            // Safety: It is safe to unwrap here because if we have a
            // `self: &FdbTransaction`, then `c_ptr` *must* be
            // `Some<NonNull<...>>`.
            fdb_sys::fdb_transaction_watch(
                (self.c_ptr.unwrap()).as_ptr(),
                key_name,
                key_name_length,
            )
        })
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
    fn add_read_conflict_key_if_not_snapshot(&self, _key: Key) -> FdbResult<()> {
        // return error, as this is a snapshot
        Err(FdbError::new(
            TRANSACTION_ADD_READ_CONFLICT_KEY_IF_NOT_SNAPSHOT,
        ))
    }

    fn add_read_conflict_range_if_not_snapshot(&self, _range: Range) -> FdbResult<()> {
        // return error, as this is a snapshot
        Err(FdbError::new(
            TRANSACTION_ADD_READ_CONFLICT_RANGE_IF_NOT_SNAPSHOT,
        ))
    }

    fn get<'t>(&'t self, key: Key) -> FdbFutureMaybeValue<'t> {
        internal::read_transaction::get(self.c_ptr, key, true)
    }

    fn get_estimated_range_size_bytes<'t>(&'t self, range: Range) -> FdbFutureI64<'t> {
        let (begin, end) = range.destructure();

        internal::read_transaction::get_estimated_range_size_bytes(self.c_ptr, begin, end)
    }

    fn get_key<'t>(&'t self, selector: KeySelector) -> FdbFutureKey<'t> {
        internal::read_transaction::get_key(self.c_ptr, selector, true)
    }

    fn get_range<'t>(
        &'t self,
        begin: KeySelector,
        end: KeySelector,
        options: RangeOptions,
    ) -> RangeResult<'t> {
        let transaction = self.c_ptr;

        let maybe_fut_key_value_array = fdb_transaction_get_range(
            transaction,
            begin.clone(),
            end.clone(),
            options.clone(),
            true,
            1,
        );

        RangeResult::new(
            transaction,
            PhantomData,
            begin,
            end,
            options,
            true,
            maybe_fut_key_value_array,
        )
    }

    fn get_read_version<'t>(&'t self) -> FdbFutureI64<'t> {
        internal::read_transaction::get_read_version(self.c_ptr)
    }

    fn is_snapshot(&self) -> bool {
        true
    }

    fn set_option(&self, option: TransactionOption) -> FdbResult<()> {
        internal::read_transaction::set_option(self.c_ptr, option)
    }

    unsafe fn set_read_version(&self, version: i64) {
        internal::read_transaction::set_read_version(self.c_ptr, version)
    }
}

pub(super) mod internal {
    pub(super) mod read_transaction {
        use crate::error::FdbResult;
        use crate::future::{FdbFuture, FdbFutureI64, FdbFutureKey, FdbFutureMaybeValue};
        use crate::transaction::TransactionOption;
        use crate::{Key, KeySelector};
        use bytes::Bytes;
        use std::convert::TryInto;

        pub(crate) fn get<'t>(
            transaction: *mut fdb_sys::FDBTransaction,
            key: Key,
            snapshot: bool,
        ) -> FdbFutureMaybeValue<'t> {
            let k = Bytes::from(key);
            let key_name = k.as_ref().as_ptr();
            let key_name_length = k.as_ref().len().try_into().unwrap();
            let s = if snapshot { 1 } else { 0 };

            FdbFuture::new(unsafe {
                fdb_sys::fdb_transaction_get(transaction, key_name, key_name_length, s)
            })
        }

        pub(crate) fn get_estimated_range_size_bytes<'t>(
            transaction: *mut fdb_sys::FDBTransaction,
            begin_key: Key,
            end_key: Key,
        ) -> FdbFutureI64<'t> {
            let bk = Bytes::from(begin_key);
            let begin_key_name = bk.as_ref().as_ptr();
            let begin_key_name_length = bk.as_ref().len().try_into().unwrap();

            let ek = Bytes::from(end_key);
            let end_key_name = ek.as_ref().as_ptr();
            let end_key_name_length = ek.as_ref().len().try_into().unwrap();

            FdbFuture::new(unsafe {
                fdb_sys::fdb_transaction_get_estimated_range_size_bytes(
                    transaction,
                    begin_key_name,
                    begin_key_name_length,
                    end_key_name,
                    end_key_name_length,
                )
            })
        }

        pub(crate) fn get_key<'t>(
            transaction: *mut fdb_sys::FDBTransaction,
            selector: KeySelector,
            snapshot: bool,
        ) -> FdbFutureKey<'t> {
            let k = Bytes::from(selector.get_key().clone());
            let key_name = k.as_ref().as_ptr();
            let key_name_length = k.as_ref().len().try_into().unwrap();
            let or_equal = if selector.or_equal() { 1 } else { 0 };
            let offset = selector.get_offset();

            let s = if snapshot { 1 } else { 0 };

            FdbFuture::new(unsafe {
                fdb_sys::fdb_transaction_get_key(
                    transaction,
                    key_name,
                    key_name_length,
                    or_equal,
                    offset,
                    s,
                )
            })
        }

        pub(crate) fn get_read_version<'t>(
            transaction: *mut fdb_sys::FDBTransaction,
        ) -> FdbFutureI64<'t> {
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

    use crate::error::{check, FdbResult};
    use crate::option::ConflictRangeType;
    use crate::Key;
    use bytes::Bytes;
    use std::convert::TryInto;

    pub(crate) fn add_conflict_range(
        transaction: *mut fdb_sys::FDBTransaction,
        begin_key: Key,
        end_key: Key,
        ty: ConflictRangeType,
    ) -> FdbResult<()> {
        let bk = Bytes::from(begin_key);
        let begin_key_name = bk.as_ref().as_ptr();
        let begin_key_name_length = bk.as_ref().len().try_into().unwrap();

        let ek = Bytes::from(end_key);
        let end_key_name = ek.as_ref().as_ptr();
        let end_key_name_length = ek.as_ref().len().try_into().unwrap();

        check(unsafe {
            fdb_sys::fdb_transaction_add_conflict_range(
                transaction,
                begin_key_name,
                begin_key_name_length,
                end_key_name,
                end_key_name_length,
                ty.code(),
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use impls::impls;

    use crate::transaction::{ReadTransaction, Transaction};

    use super::{FdbTransaction, ReadSnapshot};

    #[test]
    fn impls() {
        #[rustfmt::skip]
        assert!(impls!(
	    FdbTransaction:
		ReadTransaction &
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
