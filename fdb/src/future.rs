//! Provides [`FdbFuture`] type and [`FdbFutureGet`] trait for
//! working with FDB Future.

use bytes::Bytes;

use std::convert::TryInto;
use std::ffi::{CStr, CString};
use std::marker::PhantomData;
use std::ptr::{self, NonNull};
use std::slice;

use crate::error::{check, FdbResult};
use crate::range::KeyValueArray;
use crate::{Key, KeyValue, Value};

/// A [`FdbFuture`] represents a value (or error) to be available at
/// some other time.
///
/// Asynchronous FDB APIs return an [`FdbFuture`].
#[derive(Debug)]
pub struct FdbFuture<'t, T> {
    c_ptr: Option<NonNull<fdb_sys::FDBFuture>>,
    _marker: PhantomData<&'t T>,
}

impl<'t, T> FdbFuture<'t, T>
where
    T: FdbFutureGet,
{
    pub(crate) fn new(fdb_future: *mut fdb_sys::FDBFuture) -> FdbFuture<'t, T> {
        FdbFuture {
            c_ptr: Some(
                NonNull::new(fdb_future)
                    .expect("Tried to create FdbFuture with a null fdb_sys::FDBFuture"),
            ),
            _marker: PhantomData,
        }
    }

    /// Blocks the current thread until the FDB future is ready. A FDB
    /// future becomes ready either when it receives a value of type
    /// `T`, or when an error occurs.
    //
    // NOTE: `join` takes ownership of `FdbFuture`. This is an
    //       important aspect of the API design, as we rely on
    //       `Drop::drop` in order to call
    //       `fdb_sys::fdb_future_destroy`. If we don't call
    //       `fdb_sys::fdb_future_destroy`, then memory owned by FDB
    //       future won't be freed.
    pub fn join(self) -> FdbResult<T> {
        let fut_c_ptr = (&self.c_ptr.as_ref().unwrap()).as_ptr();

        unsafe {
            fdb_sys::fdb_future_block_until_ready(fut_c_ptr);
            FdbFutureGet::get(fut_c_ptr)
        }
    }

    /// Returns [`true`] if the FDB future is ready, [`false`]
    /// otherwise, without blocking. A FDB future is ready either when
    /// it has received a value or has been set to an error state.
    pub fn is_ready(&self) -> bool {
        let fut_c_ptr = (&self.c_ptr.as_ref().unwrap()).as_ptr();

        if unsafe { fdb_sys::fdb_future_is_ready(fut_c_ptr) } == 0 {
            false
        } else {
            true
        }
    }

    /// Consumes the [`FdbFuture`], returning a wrapped raw
    /// pointer. The pointer is wrapped using [`Box`].
    ///
    /// # Note
    ///
    /// You should not use this API. This API exists to support
    /// binding tester.
    pub fn into_raw(t: FdbFuture<'t, T>) -> *mut T {
        Box::into_raw(Box::new(t)) as *mut T
    }

    /// Constructs a boxed [`FdbFuture`] from a raw pointer. `'t` has
    /// unbounded lifetime.
    ///
    /// # Note
    ///
    /// You should not use this API. This API exists to support
    /// binding tester.
    pub unsafe fn from_raw(raw: *mut T) -> Box<FdbFuture<'t, T>> {
        Box::from_raw(raw as *mut FdbFuture<'t, T>)
    }
}

impl<'t, T> Drop for FdbFuture<'t, T> {
    fn drop(&mut self) {
        self.c_ptr.take().map(|ptr| unsafe {
            fdb_sys::fdb_future_destroy(ptr.as_ptr());
        });
    }
}

/// Extracts value that are owned by [`FdbFuture`].
///
/// # Note
///
/// You will not directly use this trait. It is used by
/// [`FdbFuture::join`] method.
pub trait FdbFutureGet {
    /// Extract value that are owned by [`FdbFuture`]
    unsafe fn get(future: *mut fdb_sys::FDBFuture) -> FdbResult<Self>
    where
        Self: Sized;
}

/// Represents the asynchronous result of a function that has no
/// return value.
pub type FdbFutureUnit<'t> = FdbFuture<'t, ()>;

impl FdbFutureGet for () {
    unsafe fn get(future: *mut fdb_sys::FDBFuture) -> FdbResult<()> {
        check(fdb_sys::fdb_future_get_error(future))
    }
}

/// Represents the asynchronous result of a function that returns a
/// database version.
pub type FdbFutureI64<'t> = FdbFuture<'t, i64>;

impl FdbFutureGet for i64 {
    unsafe fn get(future: *mut fdb_sys::FDBFuture) -> FdbResult<i64> {
        let mut out = 0;
        check(fdb_sys::fdb_future_get_int64(future, &mut out)).map(|_| out)
    }
}

/// Represents the asynchronous result of a function that returns a
/// [`Key`] from a database.
pub type FdbFutureKey<'t> = FdbFuture<'t, Key>;

impl FdbFutureGet for Key {
    unsafe fn get(future: *mut fdb_sys::FDBFuture) -> FdbResult<Key> {
        let mut out_key = ptr::null();
        let mut out_key_length = 0;

        check(fdb_sys::fdb_future_get_key(
            future,
            &mut out_key,
            &mut out_key_length,
        ))
        .map(|_| {
            Bytes::copy_from_slice(if out_key_length == 0 {
                &b""[..]
            } else {
                slice::from_raw_parts(out_key, out_key_length.try_into().unwrap())
            })
            .into()
        })
    }
}

/// Represents the asynchronous result of a function that *maybe* returns a
/// key [`Value`] from a database.
pub type FdbFutureMaybeValue<'t> = FdbFuture<'t, Option<Value>>;

impl FdbFutureGet for Option<Value> {
    unsafe fn get(future: *mut fdb_sys::FDBFuture) -> FdbResult<Option<Value>> {
        let mut out_present = 0;
        let mut out_value = ptr::null();
        let mut out_value_length = 0;

        check(fdb_sys::fdb_future_get_value(
            future,
            &mut out_present,
            &mut out_value,
            &mut out_value_length,
        ))
        .map(|_| {
            if out_present != 0 {
                Some(
                    Bytes::copy_from_slice(if out_value_length == 0 {
                        &b""[..]
                    } else {
                        slice::from_raw_parts(out_value, out_value_length.try_into().unwrap())
                    })
                    .into(),
                )
            } else {
                None
            }
        })
    }
}

/// Represents the asynchronous result of a function that returns an
/// array of [`CString`].
pub type FdbFutureCStringArray<'t> = FdbFuture<'t, Vec<CString>>;

impl FdbFutureGet for Vec<CString> {
    unsafe fn get(future: *mut fdb_sys::FDBFuture) -> FdbResult<Vec<CString>> {
        let mut out_strings = ptr::null_mut();
        let mut out_count = 0;

        check(fdb_sys::fdb_future_get_string_array(
            future,
            &mut out_strings,
            &mut out_count,
        ))
        .map(|_| {
            let mut cstring_list = Vec::with_capacity(out_count.try_into().unwrap());

            (0..out_count).into_iter().for_each(|i| {
                cstring_list.push(CString::from(CStr::from_ptr(
                    *out_strings.offset(i.try_into().unwrap()),
                )));
            });

            cstring_list
        })
    }
}

pub(crate) type FdbFutureKeyValueArray<'t> = FdbFuture<'t, KeyValueArray>;

impl FdbFutureGet for KeyValueArray {
    unsafe fn get(future: *mut fdb_sys::FDBFuture) -> FdbResult<KeyValueArray> {
        let mut out_kv = ptr::null();
        let mut out_count = 0;
        let mut out_more = 0;

        check(fdb_sys::fdb_future_get_keyvalue_array(
            future,
            &mut out_kv,
            &mut out_count,
            &mut out_more,
        ))
        .map(|_| {
            let mut kv_list = Vec::with_capacity(out_count.try_into().unwrap());

            (0..out_count).into_iter().for_each(|i| {
                let kv = out_kv.offset(i.try_into().unwrap());

                let key = Bytes::copy_from_slice(slice::from_raw_parts(
                    (*kv).key,
                    (*kv).key_length.try_into().unwrap(),
                ))
                .into();

                let value = Bytes::copy_from_slice(slice::from_raw_parts(
                    (*kv).value,
                    (*kv).value_length.try_into().unwrap(),
                ))
                .into();

                kv_list.push(KeyValue::new(key, value));
            });

            KeyValueArray::new(kv_list, if out_more == 0 { false } else { true })
        })
    }
}

#[cfg(test)]
mod tests {
    use impls::impls;

    use super::{
        FdbFutureCStringArray, FdbFutureI64, FdbFutureKey, FdbFutureKeyValueArray,
        FdbFutureMaybeValue, FdbFutureUnit,
    };

    #[test]
    fn impls() {
        #[rustfmt::skip]
	assert!(impls!(
	    FdbFutureUnit<'_>:
	        !Copy &
		!Clone &
		!Send &
		!Sync));

        #[rustfmt::skip]
	assert!(impls!(
	    FdbFutureI64<'_>:
	        !Copy &
		!Clone &
		!Send &
		!Sync));

        #[rustfmt::skip]
	assert!(impls!(
	    FdbFutureKey<'_>:
	        !Copy &
		!Clone &
		!Send &
		!Sync));

        #[rustfmt::skip]
	assert!(impls!(
	    FdbFutureMaybeValue<'_>:
	        !Copy &
		!Clone &
		!Send &
		!Sync));

        #[rustfmt::skip]
	assert!(impls!(
	    FdbFutureCStringArray<'_>:
	        !Copy &
		!Clone &
		!Send &
		!Sync));

        #[rustfmt::skip]
	assert!(impls!(
	    FdbFutureKeyValueArray<'_>:
	        !Copy &
		!Clone &
		!Send &
		!Sync));
    }
}
