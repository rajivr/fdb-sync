//! Error types in fdb crate

/// Error type for this crate.
///
/// Internally it wraps FDB [Error Codes]. Error codes from 100 thru'
/// 999 is generated by the binding layer and not the C API.
///
/// [Error Codes]: https://apple.github.io/foundationdb/api-error-codes.html
//
// 100 - `open_database.rs`
// 101 - `fdb_transaction.rs`
// 102 - `fdb_transaction.rs`
#[derive(Copy, Clone, Debug)]
pub struct FdbError {
    /// FoundationDB error code `fdb_error_t`
    error_code: i32,
}

/// Alias for [`Result`]`<T,`[`FdbError`]`>`
///
/// [`Result`]: std::result::Result
/// [`FdbError`]: crate::FdbError
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