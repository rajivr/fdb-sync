//! Provides types for working with FDB range.

use bytes::Bytes;
use std::convert::TryInto;
use std::marker::PhantomData;
use std::mem;

use crate::future::{FdbFuture, FdbFutureKeyValueArray};
use crate::transaction::ReadTransaction;
use crate::{FdbError, FdbResult};
use crate::{Key, KeySelector, KeyValue};

pub use crate::option::StreamingMode;

/// [`Range`] describes an exact range of keyspace, specified by a
/// begin and end key.
///
/// As with all FDB APIs, begin is inclusive, and end exclusive.
#[derive(Clone, Debug)]
pub struct Range {
    begin: Key,
    end: Key,
}

impl Range {
    /// Construct a new [`Range`] with an inclusive begin key an
    /// exclusive end key.
    pub fn new(begin: Key, end: Key) -> Range {
        Range { begin, end }
    }

    /// Return the beginning of the range.
    pub fn begin(&self) -> &Key {
        &self.begin
    }

    /// Return the end of the range.
    pub fn end(&self) -> &Key {
        &self.end
    }

    pub(crate) fn destructure(self) -> (Key, Key) {
        let Range { begin, end } = self;
        (begin, end)
    }
}

// Java API refers to this type `RangeResult` and Go API has something
// simliar with `futureKeyValueArray` and `[]KeyValue`. Go API
// `RangeResult` is similar to Java API `RangeQuery`. Be careful and
// don't confuse Java API `RangeResult` with Go API `RangeResult`.
#[derive(Clone, Debug)]
pub(crate) struct KeyValueArray {
    kv_list: Vec<KeyValue>,
    more: bool,
}

impl KeyValueArray {
    pub(crate) fn new(kv_list: Vec<KeyValue>, more: bool) -> KeyValueArray {
        KeyValueArray { kv_list, more }
    }
}

/// [`RangeOptions`] specify how a database range operation is carried out.
///
/// There are three parameters for which accessors methods are provided.
///
/// 1. Limit restricts the number of key-value pairs returned as part
///    of a range read. A value of zero indicates no limit.
///
/// 2. Mode sets the [streaming mode] of the range read, allowing
///    database to balance latency and bandwidth for this read.
///
/// 3. Reverse indicates that the read should be performed
///    lexicographic order (when false) or reverse lexicographic (when
///    true).
///
///    When reverse is true and limit is non-zero, last limit
///    key-value pairs in the range are returned. Ranges in reverse is
///    supported natively by the database should have minimal extra
///    cost.
///
/// To create a value of [`RangeOptions`] type, use
/// [`Default::default`] method. The default value represents - no
/// limit, [iterator streaming mode] and lexicographic order.
///
/// [streaming mode]: StreamingMode
/// [iterator streaming mode]: StreamingMode::Iterator
#[derive(Clone, Debug)]
pub struct RangeOptions {
    limit: i32,
    mode: StreamingMode,
    reverse: bool,
}

impl RangeOptions {
    /// Set limit
    pub fn set_limit(&mut self, limit: i32) {
        self.limit = limit;
    }

    /// Get limit
    pub fn get_limit(&self) -> i32 {
        self.limit
    }

    /// Set streaming mode
    pub fn set_mode(&mut self, mode: StreamingMode) {
        self.mode = mode;
    }

    /// Get streaming mode
    pub fn get_mode(&self) -> StreamingMode {
        self.mode
    }

    /// Set the read order (lexicographic or non-lexicographic)
    pub fn set_reverse(&mut self, reverse: bool) {
        self.reverse = reverse;
    }

    /// Get the read order (lexicographic or non-lexicographic)
    pub fn get_reverse(&self) -> bool {
        self.reverse
    }
}

impl Default for RangeOptions {
    fn default() -> RangeOptions {
        RangeOptions {
            limit: 0,
            mode: StreamingMode::Iterator,
            reverse: false,
        }
    }
}

/// A handle to the result of a range read.
///
/// [`RangeResult`] implements [`IntoIterator`] trait which can be
/// used to get an [`Iterator`] that yields a `FdbResult<KeyValue>>`.
//
#[derive(Debug)]
pub struct RangeResult<'t> {
    transaction: *mut fdb_sys::FDBTransaction,
    _marker: PhantomData<&'t dyn ReadTransaction>,
    begin: KeySelector,
    end: KeySelector,
    options: RangeOptions,
    snapshot: bool,
    fut_key_value_array: FdbFutureKeyValueArray,
}

impl<'t> RangeResult<'t> {
    /// Returns a vector of [`KeyValue`]s satisfying the range
    /// returned this [`RangeResult`], or an error if it did not
    /// successfully complete.
    pub fn get(mut self) -> FdbResult<Vec<KeyValue>> {
        let mut ret = Vec::new();

        if self.options.get_limit() != 0 {
            self.options.set_mode(StreamingMode::Exact);
        } else {
            self.options.set_mode(StreamingMode::WantAll);
        }

        for kv in self {
            ret.push(kv?);
        }

        Ok(ret)
    }

    pub(crate) fn new(
        transaction: *mut fdb_sys::FDBTransaction,
        _marker: PhantomData<&'t dyn ReadTransaction>,
        begin: KeySelector,
        end: KeySelector,
        options: RangeOptions,
        snapshot: bool,
        fut_key_value_array: FdbFutureKeyValueArray,
    ) -> RangeResult<'t> {
        RangeResult {
            transaction,
            _marker,
            begin,
            end,
            options,
            snapshot,
            fut_key_value_array,
        }
    }
}

impl<'t> IntoIterator for RangeResult<'t> {
    type Item = FdbResult<KeyValue>;

    type IntoIter = RangeResultIter<'t>;

    fn into_iter(self) -> RangeResultIter<'t> {
        RangeResultIter::new(self)
    }
}

/// An iterator that returns the key-value pairs in the database
/// satisfying the range specified in a range read.
//
// See `sismic/range_result_iter.yaml` for the design of the state
// machine.
#[derive(Debug)]
pub struct RangeResultIter<'t> {
    transaction: *mut fdb_sys::FDBTransaction,
    _marker: PhantomData<&'t dyn ReadTransaction>,
    snapshot: bool,
    options_mode: StreamingMode,
    options_reverse: bool,

    iteration: i32,
    // When `limit` is `None`, we are telling the C API to give us
    // data based `StreamingMode`. `StreamingMode::Exact` cannot be
    // used when `limit` is `None`. The terminating condition would be
    // when `more` returns `false`.
    //
    // When `limit` is `Some(x)`, we are setting a limit on the number
    // of rows that can be returned. `more` in this case can means
    // something else. Like in the previous case if `more` is `false`
    // then it means there are no more key-value pairs. *However* when
    // `more` returns `true`, we need to see if we have a
    // `Some(0)`. If that is the case, then we have reached the limit,
    // and hence would need to terminate, *despite* `more` being `true`.
    limit: Option<i32>,
    begin: KeySelector,
    end: KeySelector,

    range_result_iter_state: RangeResultIterState,
    range_result_iter_state_data: RangeResultIterStateData,
}

#[derive(Copy, Clone, Debug)]
enum RangeResultIterState {
    Fetching,
    KeyValueArrayAvailable,
    Error,
    Done,
}

#[derive(Debug)]
enum RangeResultIterStateData {
    InTransition,
    Fetching {
        fut_key_value_array: FdbFutureKeyValueArray,
    },
    KeyValueArrayAvailable {
        kv_list: Vec<KeyValue>,
        more: bool,
    },
    Error {
        fdb_error: FdbError,
    },
    Done,
}

enum RangeResultIterEvent {
    FetchOk {
        kv_list: Vec<KeyValue>,
        more: bool,
    },
    FetchNextBatch {
        fut_key_value_array: FdbFutureKeyValueArray,
    },
    FetchError {
        fdb_error: FdbError,
    },
    FetchDone,
}

impl<'t> RangeResultIter<'t> {
    fn new(rr: RangeResult<'t>) -> RangeResultIter<'t> {
        RangeResultIter {
            transaction: rr.transaction,
            _marker: rr._marker,
            snapshot: rr.snapshot,
            options_mode: rr.options.get_mode(),
            options_reverse: rr.options.get_reverse(),

            iteration: 1,
            limit: if rr.options.get_limit() == 0 {
                None
            } else {
                Some(rr.options.get_limit())
            },
            begin: rr.begin,
            end: rr.end,

            range_result_iter_state: RangeResultIterState::Fetching,
            range_result_iter_state_data: RangeResultIterStateData::Fetching {
                fut_key_value_array: rr.fut_key_value_array,
            },
        }
    }

    fn step_once_with_event(&mut self, event: RangeResultIterEvent) {
        self.range_result_iter_state = match self.range_result_iter_state {
            RangeResultIterState::Fetching => match event {
                RangeResultIterEvent::FetchOk { kv_list, more } => {
                    // transition action

                    // for the next iteration reduce the limit by the
                    // length of the returned key-value array.
                    if let Some(limit) = self.limit.as_mut() {
                        let len: i32 = kv_list.len().try_into().unwrap();
                        *limit = *limit - len;
                    }

                    self.iteration += 1;

                    if self.options_reverse {
                        self.end = KeySelector::first_greater_or_equal(
                            kv_list[kv_list.len() - 1].get_key().clone(),
                        );
                    } else {
                        self.begin = KeySelector::first_greater_than(
                            kv_list[kv_list.len() - 1].get_key().clone(),
                        );
                    }

                    self.range_result_iter_state_data =
                        RangeResultIterStateData::KeyValueArrayAvailable { kv_list, more };
                    RangeResultIterState::KeyValueArrayAvailable
                }
                RangeResultIterEvent::FetchError { fdb_error } => {
                    self.range_result_iter_state_data =
                        RangeResultIterStateData::Error { fdb_error };
                    RangeResultIterState::Error
                }
                _ => panic!("Invalid event!"),
            },
            RangeResultIterState::KeyValueArrayAvailable => match event {
                RangeResultIterEvent::FetchNextBatch {
                    fut_key_value_array,
                } => {
                    self.range_result_iter_state_data = RangeResultIterStateData::Fetching {
                        fut_key_value_array,
                    };
                    RangeResultIterState::Fetching
                }
                RangeResultIterEvent::FetchDone => {
                    self.range_result_iter_state_data = RangeResultIterStateData::Done;
                    RangeResultIterState::Done
                }
                _ => panic!("Invalid event!"),
            },
            RangeResultIterState::Error | RangeResultIterState::Done => {
                panic!("Invalid event!");
            }
        };
    }
}

impl<'t> Iterator for RangeResultIter<'t> {
    type Item = FdbResult<KeyValue>;

    fn next(&mut self) -> Option<FdbResult<KeyValue>> {
        loop {
            match self.range_result_iter_state {
                RangeResultIterState::Fetching => {
                    if let RangeResultIterStateData::Fetching {
                        fut_key_value_array,
                    } = mem::replace(
                        &mut self.range_result_iter_state_data,
                        RangeResultIterStateData::InTransition,
                    ) {
                        match fut_key_value_array.join() {
                            Ok(key_value_array) => {
                                let KeyValueArray { kv_list, more } = key_value_array;
                                self.step_once_with_event(RangeResultIterEvent::FetchOk {
                                    kv_list,
                                    more,
                                });
                            }
                            Err(fdb_error) => {
                                self.step_once_with_event(RangeResultIterEvent::FetchError {
                                    fdb_error,
                                });
                            }
                        }
                    } else {
                        panic!("invalid range_result_iter_state_data state");
                    }
                }
                RangeResultIterState::KeyValueArrayAvailable => {
                    if let RangeResultIterStateData::KeyValueArrayAvailable {
                        ref mut kv_list,
                        more,
                    } = self.range_result_iter_state_data
                    {
                        if kv_list.is_empty() {
                            if more {
                                if let Some(0) = self.limit {
                                    self.step_once_with_event(RangeResultIterEvent::FetchDone);
                                } else {
                                    let options = match self.limit {
                                        Some(limit) => RangeOptions {
                                            limit,
                                            mode: self.options_mode,
                                            reverse: self.options_reverse,
                                        },
                                        None => RangeOptions {
                                            limit: 0,
                                            mode: self.options_mode,
                                            reverse: self.options_reverse,
                                        },
                                    };

                                    let fut_key_value_array = fdb_transaction_get_range(
                                        self.transaction,
                                        self.begin.clone(),
                                        self.end.clone(),
                                        options,
                                        self.snapshot,
                                        self.iteration,
                                    );

                                    self.step_once_with_event(
                                        RangeResultIterEvent::FetchNextBatch {
                                            fut_key_value_array,
                                        },
                                    );
                                }
                            } else {
                                self.step_once_with_event(RangeResultIterEvent::FetchDone);
                            }
                        } else {
                            let kv = kv_list.remove(0);
                            return Some(Ok(kv));
                        }
                    } else {
                        panic!("invalid range_result_iter_state_data state");
                    }
                }
                RangeResultIterState::Error => {
                    if let RangeResultIterStateData::Error { fdb_error } =
                        self.range_result_iter_state_data
                    {
                        return Some(Err(fdb_error));
                    } else {
                        panic!("invalid range_result_iter_state_data state");
                    }
                }
                RangeResultIterState::Done => return None,
            }
        }
    }
}

pub(crate) fn fdb_transaction_get_range(
    transaction: *mut fdb_sys::FDBTransaction,
    begin_key: KeySelector,
    end_key: KeySelector,
    options: RangeOptions,
    snapshot: bool,
    iteration: i32,
) -> FdbFutureKeyValueArray {
    let bk = Bytes::from(begin_key.get_key().clone());
    let begin_key_name = bk.as_ref().as_ptr();
    let begin_key_name_length = bk.as_ref().len().try_into().unwrap();
    let begin_or_equal = if begin_key.or_equal() { 1 } else { 0 };
    let begin_offset = begin_key.get_offset();

    let ek = Bytes::from(end_key.get_key().clone());
    let end_key_name = ek.as_ref().as_ptr();
    let end_key_name_length = ek.as_ref().len().try_into().unwrap();
    let end_or_equal = if end_key.or_equal() { 1 } else { 0 };
    let end_offset = end_key.get_offset();

    let target_bytes = 0;

    let limit = options.get_limit();
    let mode = options.get_mode().code();
    let reverse = if options.get_reverse() { 1 } else { 0 };

    let s = if snapshot { 1 } else { 0 };

    FdbFuture::new(unsafe {
        fdb_sys::fdb_transaction_get_range(
            transaction,
            begin_key_name,
            begin_key_name_length,
            begin_or_equal,
            begin_offset,
            end_key_name,
            end_key_name_length,
            end_or_equal,
            end_offset,
            limit,
            target_bytes,
            mode,
            iteration,
            s,
            reverse,
        )
    })
}

#[cfg(test)]
mod tests {
    use super::{RangeOptions, RangeResult, RangeResultIter};
    use impls::impls;

    #[test]
    fn impls() {
        #[rustfmt::skip]
        assert!(impls!(
	    RangeOptions:
	    Default));

        #[rustfmt::skip]
        assert!(impls!(
	    RangeResult<'static>:
	    IntoIterator &
	    !Copy &
            !Clone &		
	    !Send &
            !Sync));

        #[rustfmt::skip]
        assert!(impls!(
	    RangeResultIter<'static>:
	    Iterator &
	    !Copy &
            !Clone &		
	    !Send &
            !Sync));
    }
}
