use bytes::{BufMut, Bytes, BytesMut};

use std::ptr::{self, NonNull};
use std::sync::Arc;

use crate::database::{Database, DatabaseOption};
use crate::error::{check, FdbError, FdbResult};
use crate::range::{Range, RangeOptions};
use crate::transaction::{
    FdbTransaction, ReadTransaction, ReadTransactionContext, Transaction, TransactionContext,
    TransactionOption,
};
use crate::{Key, KeySelector};

/// A handle to FDB database. All reads and writes to the database are
/// transactional.
///
/// A [`FdbDatabase`] can be created using [`open_database`] function.
///
/// [`open_database`]: crate::open_database
//
// *NOTE*: If you make changes to this type, make sure you update
//         tests for `DummyFdbDatabase`, `DropTestDummyFdbDatabase`
//         accordingly.
#[derive(Clone, Debug)]
pub struct FdbDatabase {
    inner: Option<Arc<NonNull<fdb_sys::FDBDatabase>>>,
}

impl FdbDatabase {
    pub(crate) fn new(inner: Option<Arc<NonNull<fdb_sys::FDBDatabase>>>) -> FdbDatabase {
        FdbDatabase { inner }
    }

    /// Runs a closure in the context that takes a [`Transaction`] and
    /// returns a Rust tuple containing the result of the closure and
    /// the [`Versionstamp`] without the user version.
    ///
    /// [`Versionstamp`]: crate::tuple::Versionstamp
    pub fn run_and_get_versionstamp<T, F>(&self, f: F) -> FdbResult<(T, Bytes)>
    where
        F: Fn(&dyn Transaction<Database = Self>) -> FdbResult<T>,
    {
        let t = self.create_transaction()?;
        loop {
            let ret_val = f(&t);

            // Closure returned an error.
            if let Err(e) = ret_val {
                if FdbError::layer_error(e.code()) {
                    // Check if it is a layer error. If so, just
                    // return it.
                    return Err(e);
                } else if let Err(e1) = unsafe { t.on_error(e) }.join() {
                    // Check if `on_error` returned an error. This
                    // means we have a non-retryable error.
                    return Err(e1);
                } else {
                    continue;
                }
            }

            // Create a `get_versionstamp` FDB future. This future
            // needs to be created before calling `commit`.
            let vs_fut = unsafe { t.get_versionstamp() };

            // No error from closure. Attempt to commit the
            // transaction.
            if let Err(e) = unsafe { t.commit() }.join() {
                // Commit returned an error
                if let Err(e1) = unsafe { t.on_error(e) }.join() {
                    // Check if `on_error` returned an error. This
                    // means we have a non-retryable error.
                    return Err(e1);
                } else {
                    continue;
                }
            }

            // Once the commit is successful, we will resolve the
            // `vs_fut`.
            return vs_fut
                .join()
                .map_err(|_| {
                    // If `vs_fut` returns an error, after `commit`
                    // was successful, we are in a gnarly
                    // situation. Return `commit_unknown_result
                    // (1021)` error.
                    FdbError::new(1021)
                })
                .and_then(|k| ret_val.map(|x| -> (T, Bytes) { (x, k.into()) }));
        }
    }

    /// Runs a closure in the context that takes a [`FdbTransaction`]
    /// and returns a Rust tuple containing the result of the closure
    /// and the [`FdbTransaction`] after the transaction has
    /// committed.
    pub fn run_and_get_transaction<T, F>(self, f: F) -> FdbResult<(T, Box<FdbTransaction>)>
    where
        Self: Sized,
        F: Fn(&FdbTransaction) -> FdbResult<T>,
    {
        let t = self.create_transaction()?;
        loop {
            let ret_val = f(&t);

            // Closure returned an error.
            if let Err(e) = ret_val {
                if FdbError::layer_error(e.code()) {
                    // Check if it is a layer error. If so, just
                    // return it.
                    return Err(e);
                } else if let Err(e1) = unsafe { t.on_error(e) }.join() {
                    // Check if `on_error` returned an error. This
                    // means we have a non-retryable error.
                    return Err(e1);
                } else {
                    continue;
                }
            }

            // No error from closure. Attempt to commit the
            // transaction.
            if let Err(e) = unsafe { t.commit() }.join() {
                // Commit returned an error
                if let Err(e1) = unsafe { t.on_error(e) }.join() {
                    // Check if `on_error` returned an error. This
                    // means we have a non-retryable error.
                    return Err(e1);
                } else {
                    continue;
                }
            }

            // Commit successful, return
            // `Ok((T, Box<FdbTransaction>)))`
            return ret_val.map(|x| (x, Box::new(t)));
        }
    }
}

impl Drop for FdbDatabase {
    fn drop(&mut self) {
        self.inner.take().map(|a| {
            match Arc::try_unwrap(a) {
                Ok(a) => unsafe {
                    fdb_sys::fdb_database_destroy(a.as_ptr());
                },
                Err(at) => {
                    drop(at);
                }
            };
        });
    }
}

// # Safety
//
// After `FdbDatabase` is created, `NonNull<fdb_sys::FdbDatabase>` is
// accessed read-only, till it is finally dropped. The main reason
// for adding `Send` and `Sync` traits is so that it can be moved to
// other threads.
unsafe impl Send for FdbDatabase {}
unsafe impl Sync for FdbDatabase {}

impl ReadTransactionContext for FdbDatabase {
    fn read<T, F>(&self, f: F) -> FdbResult<T>
    where
        Self: Sized,
        F: Fn(&dyn ReadTransaction) -> FdbResult<T>,
    {
        let t = self.create_transaction()?;
        loop {
            let ret_val = f(&t);

            // Closure returned an error
            if let Err(e) = ret_val {
                if FdbError::layer_error(e.code()) {
                    // Check if it is a layer error. If so, just
                    // return it.
                    return Err(e);
                } else if let Err(e1) = unsafe { t.on_error(e) }.join() {
                    // Check if `on_error` returned an error. This
                    // means we have a non-retryable error.
                    return Err(e1);
                } else {
                    continue;
                }
            }

            // We don't need to commit read transaction, return
            // `Ok(T)`
            return ret_val;
        }
    }
}

impl TransactionContext for FdbDatabase {
    type Database = FdbDatabase;

    fn run<T, F>(&self, f: F) -> FdbResult<T>
    where
        Self: Sized,
        F: Fn(&dyn Transaction<Database = Self::Database>) -> FdbResult<T>,
    {
        let t = self.create_transaction()?;
        loop {
            let ret_val = f(&t);

            // Closure returned an error.
            if let Err(e) = ret_val {
                if FdbError::layer_error(e.code()) {
                    // Check if it is a layer error. If so, just
                    // return it.
                    return Err(e);
                } else if let Err(e1) = unsafe { t.on_error(e) }.join() {
                    // Check if `on_error` returned an error. This
                    // means we have a non-retryable error.
                    return Err(e1);
                } else {
                    continue;
                }
            }

            // No error from closure. Attempt to commit the
            // transaction.
            if let Err(e) = unsafe { t.commit() }.join() {
                // Commit returned an error
                if let Err(e1) = unsafe { t.on_error(e) }.join() {
                    // Check if `on_error` returned an error. This
                    // means we have a non-retryable error.
                    return Err(e1);
                } else {
                    continue;
                }
            }

            // Commit successful, return `Ok(T)`
            return ret_val;
        }
    }
}

impl Database for FdbDatabase {
    type Transaction = FdbTransaction;

    fn create_transaction(&self) -> FdbResult<FdbTransaction> {
        let mut ptr: *mut fdb_sys::FDB_transaction = ptr::null_mut();
        // Safety: It is safe to unwrap here because if we have given
        // out an `FdbDatabase` then `inner` *must* be
        // `Some<Arc<...>>`.
        check(unsafe {
            fdb_sys::fdb_database_create_transaction(
                (*(self.inner.as_ref().unwrap())).as_ptr(),
                &mut ptr,
            )
        })
        .map(|_| {
            FdbTransaction::new(
                NonNull::new(ptr).expect(
                    "fdb_database_create_transaction returned null, but did not return an error",
                ),
                self.clone(),
            )
        })
    }

    fn set_option(&self, option: DatabaseOption) -> FdbResult<()> {
        // Safety: It is safe to unwrap here because if we have given
        // out an `FdbDatabase` then `inner` *must* be
        // `Some<Arc<...>>`.
        unsafe { option.apply((*(self.inner.as_ref().unwrap())).as_ptr()) }
    }

    fn get_boundary_keys(
        &self,
        begin: Key,
        end: Key,
        limit: i32,
        read_version: i64,
    ) -> FdbResult<Vec<Key>> {
        let tr = self.create_transaction()?;

        if read_version != 0 {
            unsafe {
                tr.set_read_version(read_version);
            }
        }

        tr.set_option(TransactionOption::ReadSystemKeys)?;
        tr.set_option(TransactionOption::LockAware)?;

        let range = Range::new(
            {
                let mut b = BytesMut::new();
                b.put(&b"\xFF/keyServers/"[..]);
                b.put(Into::<Bytes>::into(begin));
                Into::<Bytes>::into(b)
            }
            .into(),
            {
                let mut b = BytesMut::new();
                b.put(&b"\xFF/keyServers/"[..]);
                b.put(Into::<Bytes>::into(end));
                Into::<Bytes>::into(b)
            }
            .into(),
        );

        Ok(tr
            .snapshot()
            .get_range(
                KeySelector::first_greater_or_equal(range.begin().clone()),
                KeySelector::first_greater_or_equal(range.end().clone()),
                {
                    let mut ro = RangeOptions::default();
                    ro.set_limit(limit);
                    ro
                },
            )
            .get()?
            .into_iter()
            .map(|kv| {
                // `13` because that is the length of
                // `"\xFF/keyServers/"`.
                Into::<Key>::into(Into::<Bytes>::into(kv.get_key().clone()).slice(13..))
            })
            .collect::<Vec<_>>())
    }
}

#[cfg(test)]
mod tests {
    use impls::impls;

    use std::ptr::NonNull;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::thread;

    use super::FdbDatabase;

    #[test]
    fn impls() {
        #[rustfmt::skip]
	assert!(impls!(
	    FdbDatabase:
	        Send &
		Sync &
		Clone &
		!Copy));
    }

    #[derive(Clone, Debug)]
    struct DummyFdbDatabase {
        inner: Option<Arc<NonNull<fdb_sys::FDBDatabase>>>,
    }

    unsafe impl Send for DummyFdbDatabase {}
    unsafe impl Sync for DummyFdbDatabase {}

    #[test]
    fn trait_bounds() {
        fn trait_bounds_for_fdb_database<T>(_t: T)
        where
            T: Send + Sync + 'static,
        {
        }
        let d = DummyFdbDatabase {
            inner: Some(Arc::new(NonNull::dangling())),
        };
        trait_bounds_for_fdb_database(d);
    }

    static mut DROP_TEST_DUMMY_FDB_DATABASE_HAS_DROPPED: AtomicBool = AtomicBool::new(false);

    #[derive(Clone, Debug)]
    struct DropTestDummyFdbDatabase {
        inner: Option<Arc<NonNull<fdb_sys::FDBDatabase>>>,
    }

    unsafe impl Send for DropTestDummyFdbDatabase {}
    unsafe impl Sync for DropTestDummyFdbDatabase {}

    impl Drop for DropTestDummyFdbDatabase {
        fn drop(&mut self) {
            self.inner.take().map(|a| {
                match Arc::try_unwrap(a) {
                    Ok(_) => {
                        unsafe {
                            DROP_TEST_DUMMY_FDB_DATABASE_HAS_DROPPED.store(true, Ordering::SeqCst);
                        };
                    }
                    Err(at) => {
                        drop(at);
                    }
                };
            });
        }
    }

    #[test]
    fn multiple_drop() {
        let d0 = DropTestDummyFdbDatabase {
            inner: Some(Arc::new(NonNull::dangling())),
        };

        // Initially this is false.
        assert_eq!(
            unsafe { DROP_TEST_DUMMY_FDB_DATABASE_HAS_DROPPED.load(Ordering::SeqCst) },
            false
        );

        let d1 = d0.clone();

        assert_eq!(Arc::strong_count(d1.inner.as_ref().unwrap()), 2);

        let t1 = thread::spawn(move || {
            let _ = d1;
        });
        t1.join().unwrap();

        assert_eq!(Arc::strong_count(d0.inner.as_ref().unwrap()), 1);

        let d2 = d0.clone();
        let d3 = d2.clone();

        let t2_3 = thread::spawn(move || {
            let _ = d2;
            let _ = d3;
        });
        t2_3.join().unwrap();

        assert_eq!(Arc::strong_count(d0.inner.as_ref().unwrap()), 1);

        drop(d0);

        assert_eq!(
            unsafe { DROP_TEST_DUMMY_FDB_DATABASE_HAS_DROPPED.load(Ordering::SeqCst) },
            true
        );
    }
}
