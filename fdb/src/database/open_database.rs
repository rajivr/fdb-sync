use crate::database::FdbDatabase;
use crate::error::check;
use crate::FdbError;
use crate::FdbResult;
use std::ffi::CString;
use std::path::Path;
use std::ptr::{self, NonNull};
use std::sync::Arc;

/// Returns [`Database`] handle to the FDB cluster identified by the
/// provided cluster file.
///
/// A single client can use this function multiple times to connect to
/// different clusters simultaneously, with each invocation requiring
/// its own cluster file.
///
/// [`Database`]: crate::database::Database
pub fn open_database<P>(cluster_file_path: P) -> FdbResult<FdbDatabase>
where
    P: AsRef<Path>,
{
    let path = CString::new(
        cluster_file_path
            .as_ref()
            .to_str()
            .ok_or(FdbError::new(100))?,
    )
    .map_err(|_| FdbError::new(100))?;

    // `path_ptr` is valid till we do `drop(path)`.
    let path_ptr = path.as_ptr();

    let mut v: *mut fdb_sys::FDBDatabase = ptr::null_mut();

    let err = unsafe { fdb_sys::fdb_create_database(path_ptr, &mut v) };

    drop(path);

    // At this stage, we either have an error or a valid v.
    check(err)?;

    Ok(FdbDatabase::new(Some(Arc::new(NonNull::new(v).expect(
        "fdb_create_database returned null, but did not return an error",
    )))))
}
