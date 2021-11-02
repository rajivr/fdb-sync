//! Starting point for accessing FoundationDB
use bytes::Bytes;
use parking_lot::{Once, OnceState};

use std::thread::{self, JoinHandle};

use crate::error::{check, FdbResult};
use crate::option::NetworkOption;

/// [`Key`] represents a FDB key, a lexicographically-ordered sequence
/// of bytes.
///
/// [`Key`] can be converted from and into [`Bytes`].
#[derive(Clone, Debug, PartialEq)]
pub struct Key(Bytes);

impl From<Bytes> for Key {
    fn from(b: Bytes) -> Key {
        Key(b)
    }
}

impl From<Key> for Bytes {
    fn from(k: Key) -> Bytes {
        k.0
    }
}

/// [`Value`] represents a value of an FDB [`Key`] and is a sequence
/// of bytes.
///
/// [`Value`] can be converted from and into [`Bytes`].
#[derive(Clone, Debug, PartialEq)]
pub struct Value(Bytes);

impl From<Bytes> for Value {
    fn from(b: Bytes) -> Value {
        Value(b)
    }
}

impl From<Value> for Bytes {
    fn from(v: Value) -> Bytes {
        v.0
    }
}

/// [`KeySelector`] identifies a particular key in the database.
///
/// FDB's lexicographically ordered data model permits finding keys
/// based on their order (for example, finding the first key in the
/// database greater than a given key). Key selectors represent a
/// description of a key in the database that could be resolved to an
/// actual key by transaction's [`get_key`] or used directly as the
/// beginning or end of a range in transaction's [`get_range`].
///
/// For more about how key selectors work in practive, see the [`key
/// selector`] documentation. Note that the way key selectors are
/// resolved is somewhat non-intuitive, so users who wish to use a key
/// selector other than the default ones described below should
/// probably consult that documentation before proceeding.
///
/// Generally one of the following methods should be used to construct
/// a [`KeySelector`].
/// - [`last_less_than`]
/// - [`last_less_or_equal`]
/// - [`first_greater_than`]
/// - [`first_greater_or_equal`]
///
/// This is an *immutable* type. The `add(i32)` call does not modify
/// internal state, but returns a new value.
///
/// [`get_key`]: crate::transaction::ReadTransaction::get_key
/// [`get_range`]: crate::transaction::ReadTransaction::get_range
/// [`key selector`]: https://apple.github.io/foundationdb/developer-guide.html#key-selectors
/// [`last_less_than`]: KeySelector::last_less_than
/// [`last_less_or_equal`]: KeySelector::last_less_or_equal
/// [`first_greater_than`]: KeySelector::first_greater_than
/// [`first_greater_or_equal`]: KeySelector::first_greater_or_equal
#[derive(Clone, Debug)]
pub struct KeySelector {
    key: Key,
    or_equal: bool,
    offset: i32,
}

impl KeySelector {
    /// Returns a new [`KeySelector`] offset by a given number of keys
    /// from this one.
    pub fn add(self, offset: i32) -> KeySelector {
        KeySelector::new(self.key, self.or_equal, self.offset + offset)
    }

    /// Creates a [`KeySelector`] that picks the first key greater
    /// than or equal to the parameter.
    pub fn first_greater_or_equal(key: Key) -> KeySelector {
        KeySelector::new(key, false, 1)
    }

    /// Creates a [`KeySelector`] that picks the first key greater
    /// than or equal to the parameter.
    pub fn first_greater_than(key: Key) -> KeySelector {
        KeySelector::new(key, true, 1)
    }

    /// Returns a reference to the key that serves as the anchor for
    /// this [`KeySelector`].
    pub fn get_key(&self) -> &Key {
        &self.key
    }

    /// Returns the key offset parameter for this [`KeySelector`].
    pub fn get_offset(&self) -> i32 {
        self.offset
    }

    /// Creates a [`KeySelector`] that picks the last key less than or
    /// equal to the parameter.
    pub fn last_less_or_equal(key: Key) -> KeySelector {
        KeySelector::new(key, true, 0)
    }

    /// Creates a [`KeySelector`] that picks the last key less than the parameter.
    pub fn last_less_than(key: Key) -> KeySelector {
        KeySelector::new(key, false, 0)
    }

    pub(crate) fn or_equal(&self) -> bool {
        self.or_equal
    }

    fn new(key: Key, or_equal: bool, offset: i32) -> KeySelector {
        KeySelector {
            key,
            or_equal,
            offset,
        }
    }
}

/// A key/value pair.
///
/// Range read operations on FDB return [`KeyValue`]s.
#[derive(Clone, Debug)]
pub struct KeyValue {
    key: Key,
    value: Value,
}

impl KeyValue {
    /// Gets the key from [`KeyValue`].
    pub fn get_key(&self) -> &Key {
        &self.key
    }

    /// Gets the value from [`KeyValue`].
    pub fn get_value(&self) -> &Value {
        &self.value
    }

    pub(crate) fn new(key: Key, value: Value) -> KeyValue {
        KeyValue { key, value }
    }
}

static SELECT_API_VERSION_INIT: Once = Once::new();

/// Select the version of the client API.
///
/// # Panic
///
/// This will panic if the version is not supported by the
/// implementation of the API.
///
/// As only one version can be selected for the lifetime of the
/// process, calling this function more than once will also result in
/// a panic.
///
/// # Safety
///
/// This API is part of FDB client setup. See [this] link for more
/// information on how to correctly setup and teardown FDB client.
///
/// # Warning
///
/// When using the multi-version client API, setting an API version
/// that is not supported by a particular client library will prevent
/// that client from being used to connect to the cluster.
///
/// In particular, you should not advance the API version of your
/// application after upgrading your client **until** the cluster has
/// also been upgraded.
pub unsafe fn select_api_version(version: i32) {
    if SELECT_API_VERSION_INIT.state() == OnceState::New {
        SELECT_API_VERSION_INIT.call_once(|| {
            // run initialization here
            check(fdb_sys::fdb_select_api_version_impl(
                version,
                // `bindgen` defaults `FDB_API_VERSION` to `u32`
                fdb_sys::FDB_API_VERSION as i32,
            ))
            .unwrap_or_else(|_| {
                panic!("Unable to call select_api_version for version {}", version)
            });
        });
    } else {
        panic!("select_api_version(...) was previously called!");
    }
}

/// Set global options for the [FDB API].
///
/// # Safety
///
/// This API is part of FDB client setup. See [this] link for more
/// information on how to correctly setup and teardown FDB client.
///
/// [FDB API]: crate
pub unsafe fn set_network_option(option: NetworkOption) -> FdbResult<()> {
    option.apply()
}

static mut FDB_NETWORK_THREAD: Option<JoinHandle<FdbResult<()>>> = None;

// `FDB_NETWORK_STARTED` is set to `true` in `main` thread, and
// `FDB_NETWORK_STOPPED` is set to `true`, in `fdb-network-thread`.
static mut FDB_NETWORK_STARTED: bool = false;
static mut FDB_NETWORK_STOPPED: bool = false;

/// Initializes FDB network.
///
/// # Safety
///
/// This API is part of FDB client setup. See [this] link for more
/// information on how to correctly setup and teardown FDB client.
pub unsafe fn start_network() {
    if FDB_NETWORK_STOPPED {
        panic!("Network has been stopped and cannot be started");
    }

    if FDB_NETWORK_STARTED {
        return;
    }

    check(fdb_sys::fdb_setup_network()).unwrap_or_else(|e| {
        panic!("fdb_sys::fdb_setup_network() failed with error {:?}", e);
    });

    FDB_NETWORK_STARTED = true;

    FDB_NETWORK_THREAD = Some(
        thread::Builder::new()
            .name("fdb-network-thread".into())
            .spawn(|| {
                let res = check(fdb_sys::fdb_run_network());
                FDB_NETWORK_STOPPED = true;
                res
            })
            .unwrap_or_else(|e| {
                panic!("unable to create fdb-network-thread: error = {}", e);
            }),
    );
}

/// Stops the FDB networking engine.
///
/// # Safety
///
/// This API is part of FDB client setup. See [this] link for more
/// information on how to correctly setup and teardown FDB client.
pub unsafe fn stop_network() {
    if !FDB_NETWORK_STARTED {
        panic!("Trying to stop the network, before network has been started");
    };

    check(fdb_sys::fdb_stop_network()).unwrap_or_else(|e| {
        panic!("fdb_sys::fdb_stop_network() failed with error {:?}", e);
    });

    FDB_NETWORK_THREAD
        .take()
        .unwrap()
        .join()
        .unwrap_or_else(|e| {
            panic!("failed to join on fdb-network-thread: error {:?}", e);
        })
        .unwrap_or_else(|e| {
            panic!("fdb_sys::fdb_run_network() failed with error {:?}", e);
        });
}
