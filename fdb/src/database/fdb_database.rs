use crate::database::{Database, DatabaseOption};
use crate::error::{check, FdbResult};
use crate::transaction::{
    FdbTransaction, ReadTransaction, ReadTransactionContext, Transaction, TransactionContext,
};
use std::ptr::{self, NonNull};
use std::sync::Arc;

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

            // Closure returned an error. Check if its retryable.
            if let Err(e) = ret_val {
                if t.on_error(e).join().is_err() {
                    return ret_val;
                } else {
                    continue;
                }
            }

            // We don't need to commit read transaction, return `Ok(T)`
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

            // Closure returned an error. Check if its retryable.
            if let Err(e) = ret_val {
                if t.on_error(e).join().is_err() {
                    return ret_val;
                } else {
                    continue;
                }
            }

            // Commit returned an error. Check if its retryable.
            if let Err(e) = t.commit().join() {
                if t.on_error(e).join().is_err() {
                    return ret_val;
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

    fn set_option(&self, option: DatabaseOption) -> FdbResult<()> {
        // Safety: It is safe to unwrap here because if we have given
        // out an `FdbDatabase` then `inner` *must* be
        // `Some<Arc<...>>`.
        unsafe { option.apply((*(self.inner.as_ref().unwrap())).as_ptr()) }
    }

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
}

#[cfg(test)]
mod tests {
    use super::FdbDatabase;
    use impls::impls;
    use std::ptr::NonNull;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::thread;

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
