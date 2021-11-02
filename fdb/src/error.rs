//! Provides [`FdbError`] type, [`FdbResult`] type alias and error
//! constants.

/// Error type for this crate.
///
/// Internally it wraps FDB [Error Codes]. Error codes from 100 thru'
/// 999 is generated by the binding layer and not the C API.
///
/// [Error Codes]: https://apple.github.io/foundationdb/api-error-codes.html
//
// 100 - `database` module
// 110 - `transaction` module
// 120 - `tuple` module
#[derive(Copy, Clone, Debug, PartialEq)]
pub struct FdbError {
    /// FoundationDB error code `fdb_error_t`
    error_code: i32,
}

/// Error occurred while opening database.
pub const DATABASE_OPEN: i32 = 100;

/// Error occured while trying to add read conflict key.
pub const TRANSACTION_ADD_READ_CONFLICT_KEY_IF_NOT_SNAPSHOT: i32 = 110;

/// Error occured while trying to add read conflict range.
pub const TRANSACTION_ADD_READ_CONFLICT_RANGE_IF_NOT_SNAPSHOT: i32 = 111;

/// Error occurred while getting a value from the tuple.
pub const TUPLE_GET: i32 = 120;

/// Error occurred extracting a [`Tuple`] value from [`Bytes`].
///
/// [`Tuple`]: crate::tuple::Tuple
/// [`Bytes`]: bytes::Bytes
pub const TUPLE_FROM_BYTES: i32 = 121;

/// Error occured when trying to pack [`Tuple`] containing an
/// incomplete [`Versionstamp`]. No incomplete [`Versionstamp`] found.
///
/// [`Tuple`]:  crate::tuple::Tuple
/// [`Versionstamp`]: crate::tuple::Versionstamp
pub const TUPLE_PACK_WITH_VERSIONSTAMP_NOT_FOUND: i32 = 122;

/// Error occured when trying to pack [`Tuple`] containing an
/// incomplete [`Versionstamp`]. Multiple incomplete [`Versionstamp`]
/// found.
///
/// [`Tuple`]:  crate::tuple::Tuple
/// [`Versionstamp`]: crate::tuple::Versionstamp
pub const TUPLE_PACK_WITH_VERSIONSTAMP_MULTIPLE_FOUND: i32 = 123;

/// Alias for [`Result`]`<T,`[`FdbError`]`>`
///
/// [`Result`]: std::result::Result
/// [`FdbError`]: crate::error::FdbError
pub type FdbResult<T> = Result<T, FdbError>;

impl FdbError {
    /// Create new `FdbError`
    pub(crate) fn new(err: fdb_sys::fdb_error_t) -> FdbError {
        FdbError {
            error_code: err as i32,
        }
    }

    /// Returns raw FDB error code (`fdb_error_t`)
    pub(crate) fn code(self) -> i32 {
        self.error_code
    }
}

/// Converts `fdb_error_t` to `FdbResult`
pub(crate) fn check(err: fdb_sys::fdb_error_t) -> FdbResult<()> {
    if err == 0 {
        Ok(())
    } else {
        Err(FdbError::new(err))
    }
}
