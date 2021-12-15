use bytes::{Buf, BufMut, Bytes, BytesMut};

use dashmap::DashMap;

use fdb::database::{Database, DatabaseOption, FdbDatabase};
use fdb::error::{
    FdbError, FdbResult, TUPLE_PACK_WITH_VERSIONSTAMP_MULTIPLE_FOUND,
    TUPLE_PACK_WITH_VERSIONSTAMP_NOT_FOUND,
};
use fdb::future::{FdbFuture, FdbFutureGet};
use fdb::range::{Range, RangeOptions, StreamingMode};
use fdb::subspace::Subspace;
use fdb::transaction::{
    FdbTransaction, MutationType, ReadTransaction, ReadTransactionContext, Transaction,
    TransactionContext, TransactionOption,
};
use fdb::tuple::{bytes_util, Tuple, Versionstamp};
use fdb::{self, Key, KeySelector, KeyValue, Value};

// This code is automatically generated, so we can ignore the
// warnings.
#[allow(non_upper_case_globals)]
use fdb_sys::{
    FDBStreamingMode_FDB_STREAMING_MODE_EXACT, FDBStreamingMode_FDB_STREAMING_MODE_ITERATOR,
    FDBStreamingMode_FDB_STREAMING_MODE_LARGE, FDBStreamingMode_FDB_STREAMING_MODE_MEDIUM,
    FDBStreamingMode_FDB_STREAMING_MODE_SERIAL, FDBStreamingMode_FDB_STREAMING_MODE_SMALL,
    FDBStreamingMode_FDB_STREAMING_MODE_WANT_ALL,
};

use itertools::Itertools;

use num_bigint::BigInt;

use tokio::runtime::{Handle, Runtime};
use tokio::sync::mpsc::{self, Sender};

use uuid::Uuid;

use std::cmp::Ordering;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::env;
use std::error::Error;
use std::iter;
use std::mem;
use std::ops::Range as OpsRange;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

const VERBOSE: bool = false;
const VERBOSE_INST_RANGE: Option<OpsRange<usize>> = None;
const VERBOSE_INST_ONLY: bool = false;

#[derive(Debug)]
struct FdbTransactionPtrWrapper {
    inner: *mut FdbTransaction,
}

impl FdbTransactionPtrWrapper {
    fn new(inner: *mut FdbTransaction) -> FdbTransactionPtrWrapper {
        FdbTransactionPtrWrapper { inner }
    }
}

unsafe impl Send for FdbTransactionPtrWrapper {}
unsafe impl Sync for FdbTransactionPtrWrapper {}

// `TRANSACTION_NAME` and thread `PREFIX` are maintained seperately in
// the stack machine. In the `StackMachine` type `TRANSACTION_NAME`
// maps to `tr_name` field and thread `PREFIX` maps to `prefix` field.
//
// `PREFIX` is passed either via command line or using the
// `START_THREAD` operation.
//
// While it is not mentioned explicity, from [this] Go binding code we
// can infer that when a value of `StackMachine` type is created,
// `tr_name` would be a copy of the `prefix` value.
//
// `NEW_TRANSACTION` and `USE_TRANSACTION` operations operate on
// `tr_name`.
//
// `TrMap` maps `tr_name` with a `*mut FdbTransaction`.
//
// In Go, `trMap` is of type `map[string]fdb.Transaction{}`. We are
// using `Bytes` for our key as that is what the spec says.
//
// [this]: https://github.com/apple/foundationdb/blob/6.3.22/bindings/go/src/_stacktester/stacktester.go#L88
type TrMap = Arc<DashMap<Bytes, FdbTransactionPtrWrapper>>;

// Semantically stack entries can either contain `FdbFuture` or they
// may not contain `FdbFuture`. We use types `NonFutureStackEntry` and
// `StackEntry` to represent these. There are methods on
// `StackMachine` to go back and forth between the two types.

// This is a subset of `StackEntryItem` that does not contain any
// `FdbFuture`.
#[derive(Debug, Clone)]
enum NonFutureStackEntryItem {
    BigInt(BigInt),
    Bool(bool),
    Bytes(Bytes),
    Float(f32),
    Double(f64),
    Null,
    String(String),
    Tuple(Tuple),
    Uuid(Uuid),
    Versionstamp(Versionstamp),
}

#[derive(Debug, Clone)]
struct NonFutureStackEntry {
    item: NonFutureStackEntryItem,
    inst_number: usize,
}

// These are items that can go on the stack.
//
// We don't construct variants `FdbFutureI64(*mut i64)`,
// `FdbFutureCStringArray(*mut Vec<CString>)`.
#[derive(Debug, Clone)]
enum StackEntryItem {
    FdbFutureKey(*mut Key),
    FdbFutureMaybeValue(*mut Option<Value>),
    FdbFutureUnit(*mut ()),
    BigInt(BigInt),
    Bool(bool),
    Bytes(Bytes),
    Float(f32),
    Double(f64),
    Null,
    String(String),
    Tuple(Tuple),
    Uuid(Uuid),
    Versionstamp(Versionstamp),
}

#[derive(Debug, Clone)]
struct StackEntry {
    item: StackEntryItem,
    inst_number: usize,
}

// We need to have `db` and `task_finished` because for `START_THREAD`
// operation we'll need to create a new Tokio `stack_tester_task`,
// which will requires us to clone `db` and `task_finished`. If the
// `StackMachine` does not create a new Tokio task, `task_finished`
// will get dropped when `StackMachine` is dropped.
#[derive(Debug)]
struct StackMachine {
    tr_map: TrMap,

    prefix: Bytes,
    tr_name: Bytes,
    stack: Vec<StackEntry>,
    verbose: bool,
    db: FdbDatabase,
    last_version: i64,

    task_finished: Sender<()>,
}

impl StackMachine {
    fn new(
        tr_map: TrMap,
        prefix: Bytes,
        db: FdbDatabase,
        task_finished: Sender<()>,
        verbose: bool,
    ) -> StackMachine {
        let stack = Vec::new();
        let tr_name = prefix.clone();
        let last_version = 0;
        StackMachine {
            tr_map,
            prefix,
            tr_name,
            stack,
            verbose,
            db,
            last_version,
            task_finished,
        }
    }

    // When we pop a `*mut T` that is a boxed `FdbFuture` this method
    // will first put it inside a `Box`, then unbox it and tie the
    // unbounded lifetime parameter `'t` to lifetime parameter
    // `'transaction`.
    fn get_bounded_fdb_future<'transaction, 't, T>(
        _transaction: &'transaction FdbTransaction,
        raw: *mut T,
    ) -> FdbFuture<'transaction, T>
    where
        T: FdbFutureGet + 't,
    {
        let b: Box<FdbFuture<'t, T>> = unsafe { FdbFuture::from_raw(raw) };

        // Move `FdbFuture<'t, T>` from the heap, into the stack, and
        // drop the boxed (heap) memory.
        let fdb_future = *b;

        unsafe { mem::transmute::<FdbFuture<'t, T>, FdbFuture<'transaction, T>>(fdb_future) }
    }

    fn bigint_to_bool(bi: BigInt) -> bool {
        if bi == BigInt::from(0) {
            false
        } else {
            true
        }
    }

    fn to_non_future_stack_entry_item(sei: StackEntryItem) -> Option<NonFutureStackEntryItem> {
        match sei {
            StackEntryItem::BigInt(b) => Some(NonFutureStackEntryItem::BigInt(b)),
            StackEntryItem::Bool(b) => Some(NonFutureStackEntryItem::Bool(b)),
            StackEntryItem::Bytes(b) => Some(NonFutureStackEntryItem::Bytes(b)),
            StackEntryItem::Float(f) => Some(NonFutureStackEntryItem::Float(f)),
            StackEntryItem::Double(f) => Some(NonFutureStackEntryItem::Double(f)),
            StackEntryItem::Null => Some(NonFutureStackEntryItem::Null),
            StackEntryItem::String(s) => Some(NonFutureStackEntryItem::String(s)),
            StackEntryItem::Tuple(t) => Some(NonFutureStackEntryItem::Tuple(t)),
            StackEntryItem::Uuid(u) => Some(NonFutureStackEntryItem::Uuid(u)),
            StackEntryItem::Versionstamp(vs) => Some(NonFutureStackEntryItem::Versionstamp(vs)),
            _ => None,
        }
    }

    fn to_stack_entry_item(nfsei: NonFutureStackEntryItem) -> StackEntryItem {
        match nfsei {
            NonFutureStackEntryItem::BigInt(b) => StackEntryItem::BigInt(b),
            NonFutureStackEntryItem::Bool(b) => StackEntryItem::Bool(b),
            NonFutureStackEntryItem::Bytes(b) => StackEntryItem::Bytes(b),
            NonFutureStackEntryItem::Float(f) => StackEntryItem::Float(f),
            NonFutureStackEntryItem::Double(f) => StackEntryItem::Double(f),
            NonFutureStackEntryItem::Null => StackEntryItem::Null,
            NonFutureStackEntryItem::String(s) => StackEntryItem::String(s),
            NonFutureStackEntryItem::Tuple(t) => StackEntryItem::Tuple(t),
            NonFutureStackEntryItem::Uuid(u) => StackEntryItem::Uuid(u),
            NonFutureStackEntryItem::Versionstamp(vs) => StackEntryItem::Versionstamp(vs),
        }
    }

    fn to_non_future_stack_entry(se: StackEntry) -> Option<NonFutureStackEntry> {
        let StackEntry { item, inst_number } = se;

        StackMachine::to_non_future_stack_entry_item(item)
            .map(|item| NonFutureStackEntry { item, inst_number })
    }

    // There is a crate only method `.code()` on `StreamingMode` that
    // gives converts a `StreamingMode` into C API level
    // constant. This method does the opposite. It takes the C API
    // level constant and returns a value of `StreamingMode` type.
    //
    // If additional streaming modes are added in the future, this API
    // will need to be revised.
    //
    // It will panic if the conversion fails.
    #[allow(non_upper_case_globals)]
    fn from_streaming_mode_code(code: i32) -> StreamingMode {
        match code {
            FDBStreamingMode_FDB_STREAMING_MODE_WANT_ALL => StreamingMode::WantAll,
            FDBStreamingMode_FDB_STREAMING_MODE_ITERATOR => StreamingMode::Iterator,
            FDBStreamingMode_FDB_STREAMING_MODE_EXACT => StreamingMode::Exact,
            FDBStreamingMode_FDB_STREAMING_MODE_SMALL => StreamingMode::Small,
            FDBStreamingMode_FDB_STREAMING_MODE_MEDIUM => StreamingMode::Medium,
            FDBStreamingMode_FDB_STREAMING_MODE_LARGE => StreamingMode::Large,
            FDBStreamingMode_FDB_STREAMING_MODE_SERIAL => StreamingMode::Serial,
            _ => panic!("Invalid streaming mode code provided {:?}", code),
        }
    }

    // This is used for `ATOMIC_OP(_DATABASE)` operation. The `OPTYPE`
    // is pushed on the stack as a string. The list of `OPTYPE`s that
    // is pushed onto the stack maintained here [1] in `atomic_ops`
    // variable. If new `OPTYPE`s are added in future, then we'll need
    // to update this method.
    //
    // [1]: https://github.com/apple/foundationdb/blob/6.3.22/bindings/bindingtester/tests/api.py#L177
    fn from_mutation_type_string(s: String) -> MutationType {
        match s.as_str() {
            "BIT_AND" => MutationType::BitAnd,
            "BIT_OR" => MutationType::BitOr,
            "MAX" => MutationType::Max,
            "MIN" => MutationType::Min,
            "BYTE_MIN" => MutationType::ByteMin,
            "BYTE_MAX" => MutationType::ByteMax,
            "ADD" => MutationType::Add,
            "BIT_XOR" => MutationType::BitXor,
            "APPEND_IF_FITS" => MutationType::AppendIfFits,
            "SET_VERSIONSTAMPED_KEY" => MutationType::SetVersionstampedKey,
            "SET_VERSIONSTAMPED_VALUE" => MutationType::SetVersionstampedValue,
            "COMPARE_AND_CLEAR" => MutationType::CompareAndClear,
            _ => panic!("Invalid mutation type string provided: {:?}", s),
        }
    }

    // From the spec [1]
    //
    // An operation may have a second element which provides
    // additional data, which may be of any tuple type.
    //
    // [1]: https://github.com/apple/foundationdb/blob/6.3.22/bindings/bindingtester/spec/bindingApiTester.md#overview
    fn get_additional_inst_data(inst: &Tuple) -> StackEntryItem {
        inst.get_bigint(1)
            .map(|b| StackEntryItem::BigInt(b))
            .or(inst.get_bool(1).map(|b| StackEntryItem::Bool(b)))
            .or(inst
                .get_bytes_ref(1)
                .map(|b| StackEntryItem::Bytes(b.clone())))
            .or(inst.get_f32(1).map(|f| StackEntryItem::Float(f)))
            .or(inst.get_f64(1).map(|f| StackEntryItem::Double(f)))
            .or(inst.get_null(1).map(|_| StackEntryItem::Null))
            .or(inst
                .get_string_ref(1)
                .map(|s| StackEntryItem::String(s.clone())))
            .or(inst
                .get_tuple_ref(1)
                .map(|t| StackEntryItem::Tuple(t.clone())))
            .or(inst
                .get_uuid_ref(1)
                .map(|u| StackEntryItem::Uuid(u.clone())))
            .or(inst
                .get_versionstamp_ref(1)
                .map(|v| StackEntryItem::Versionstamp(v.clone())))
            .unwrap_or_else(|err| {
                panic!(
                    "Error occurred during `instruction_additonal_data`: {:?}",
                    err
                )
            })
    }

    fn wait_and_pop<'transaction>(
        &mut self,
        transaction: &'transaction FdbTransaction,
    ) -> NonFutureStackEntry {
        let StackEntry { item, inst_number } = self.stack.pop().unwrap();

        match item {
            StackEntryItem::FdbFutureKey(raw_ptr_key) => {
                let fdb_fut_key = StackMachine::get_bounded_fdb_future(transaction, raw_ptr_key);
                fdb_fut_key
                    .join()
                    .map(|x| {
                        let item = NonFutureStackEntryItem::Bytes(x.into());
                        NonFutureStackEntry { item, inst_number }
                    })
                    .unwrap_or_else(|err| {
                        let item = NonFutureStackEntryItem::Bytes({
                            let mut t = Tuple::new();
                            t.add_bytes(Bytes::from(&b"ERROR"[..]));
                            t.add_bytes(Bytes::from(format!("{}", err.code())));
                            t.pack()
                        });
                        NonFutureStackEntry { item, inst_number }
                    })
            }
            StackEntryItem::FdbFutureMaybeValue(raw_ptr_maybe_value) => {
                let fdb_fut_maybe_value =
                    StackMachine::get_bounded_fdb_future(transaction, raw_ptr_maybe_value);

                fdb_fut_maybe_value
                    .join()
                    .map(|x| {
                        let item = NonFutureStackEntryItem::Bytes(
                            x.map(|y| y.into())
                                .unwrap_or_else(|| Bytes::from(&b"RESULT_NOT_PRESENT"[..])),
                        );
                        NonFutureStackEntry { item, inst_number }
                    })
                    .unwrap_or_else(|err| {
                        let item = NonFutureStackEntryItem::Bytes({
                            let mut t = Tuple::new();
                            t.add_bytes(Bytes::from(&b"ERROR"[..]));
                            t.add_bytes(Bytes::from(format!("{}", err.code())));
                            t.pack()
                        });
                        NonFutureStackEntry { item, inst_number }
                    })
            }
            StackEntryItem::FdbFutureUnit(raw_ptr_unit) => {
                let fdb_fut_unit = StackMachine::get_bounded_fdb_future(transaction, raw_ptr_unit);

                fdb_fut_unit
                    .join()
                    .map(|_| {
                        let item =
                            NonFutureStackEntryItem::Bytes(Bytes::from(&b"RESULT_NOT_PRESENT"[..]));
                        NonFutureStackEntry { item, inst_number }
                    })
                    .unwrap_or_else(|err| {
                        let item = NonFutureStackEntryItem::Bytes({
                            let mut t = Tuple::new();
                            t.add_bytes(Bytes::from(&b"ERROR"[..]));
                            t.add_bytes(Bytes::from(format!("{}", err.code())));
                            t.pack()
                        });
                        NonFutureStackEntry { item, inst_number }
                    })
            }
            StackEntryItem::BigInt(b) => {
                let item = NonFutureStackEntryItem::BigInt(b);
                NonFutureStackEntry { item, inst_number }
            }
            StackEntryItem::Bool(b) => {
                let item = NonFutureStackEntryItem::Bool(b);
                NonFutureStackEntry { item, inst_number }
            }
            StackEntryItem::Bytes(b) => {
                let item = NonFutureStackEntryItem::Bytes(b);
                NonFutureStackEntry { item, inst_number }
            }
            StackEntryItem::Float(f) => {
                let item = NonFutureStackEntryItem::Float(f);
                NonFutureStackEntry { item, inst_number }
            }
            StackEntryItem::Double(f) => {
                let item = NonFutureStackEntryItem::Double(f);
                NonFutureStackEntry { item, inst_number }
            }
            StackEntryItem::Null => {
                let item = NonFutureStackEntryItem::Null;
                NonFutureStackEntry { item, inst_number }
            }
            StackEntryItem::String(s) => {
                let item = NonFutureStackEntryItem::String(s);
                NonFutureStackEntry { item, inst_number }
            }
            StackEntryItem::Tuple(t) => {
                let item = NonFutureStackEntryItem::Tuple(t);
                NonFutureStackEntry { item, inst_number }
            }
            StackEntryItem::Uuid(u) => {
                let item = NonFutureStackEntryItem::Uuid(u);
                NonFutureStackEntry { item, inst_number }
            }
            StackEntryItem::Versionstamp(v) => {
                let item = NonFutureStackEntryItem::Versionstamp(v);
                NonFutureStackEntry { item, inst_number }
            }
        }
    }

    fn pop_selector<'transaction>(
        &mut self,
        transaction: &'transaction FdbTransaction,
    ) -> KeySelector {
        let key = if let NonFutureStackEntryItem::Bytes(b) = self.wait_and_pop(transaction).item {
            b
        } else {
            panic!("NonFutureStackEntryItem::Bytes was expected, but not found");
        }
        .into();

        // Even though `or_equal` is a bool, in the stack it is stored as a number.
        let or_equal = StackMachine::bigint_to_bool(
            if let NonFutureStackEntryItem::BigInt(bi) = self.wait_and_pop(transaction).item {
                bi
            } else {
                panic!("NonFutureStackEntryItem::BigInt was expected, but not found");
            },
        );

        let offset = i32::try_from(
            if let NonFutureStackEntryItem::BigInt(bi) = self.wait_and_pop(transaction).item {
                bi
            } else {
                panic!("NonFutureStackEntryItem::BigInt was expected, but not found");
            },
        )
        .unwrap_or_else(|err| panic!("Error occurred during `i32::try_from`: {:?}", err));

        KeySelector::new(key, or_equal, offset)
    }

    fn pop_range_options<'transaction>(
        &mut self,
        transaction: &'transaction FdbTransaction,
    ) -> RangeOptions {
        let limit = i32::try_from(
            if let NonFutureStackEntryItem::BigInt(bi) = self.wait_and_pop(transaction).item {
                bi
            } else {
                panic!("NonFutureStackEntryItem::BigInt was expected, but not found");
            },
        )
        .unwrap_or_else(|err| panic!("Error occurred during `i32::try_from`: {:?}", err));

        // Even though `reverse` is a bool, in the stack it is stored as a number.
        let reverse = StackMachine::bigint_to_bool(
            if let NonFutureStackEntryItem::BigInt(bi) = self.wait_and_pop(transaction).item {
                bi
            } else {
                panic!("NonFutureStackEntryItem::BigInt was expected, but not found");
            },
        );

        let mode = StackMachine::from_streaming_mode_code(
            i32::try_from(
                if let NonFutureStackEntryItem::BigInt(bi) = self.wait_and_pop(transaction).item {
                    bi
                } else {
                    panic!("NonFutureStackEntryItem::BigInt was expected, but not found");
                },
            )
            .unwrap_or_else(|err| panic!("Error occurred during `i32::try_from`: {:?}", err)),
        );

        let mut res = RangeOptions::default();

        res.set_limit(limit);
        res.set_reverse(reverse);
        res.set_mode(mode);

        res
    }

    fn pop_key_range<'transaction>(&mut self, transaction: &'transaction FdbTransaction) -> Range {
        let begin_key =
            if let NonFutureStackEntryItem::Bytes(b) = self.wait_and_pop(transaction).item {
                b
            } else {
                panic!("NonFutureStackEntryItem::Bytes was expected, but not found");
            }
            .into();

        let end_key =
            if let NonFutureStackEntryItem::Bytes(b) = self.wait_and_pop(transaction).item {
                b
            } else {
                panic!("NonFutureStackEntryItem::Bytes was expected, but not found");
            }
            .into();

        Range::new(begin_key, end_key)
    }

    fn pop_prefix_range<'transaction>(
        &mut self,
        transaction: &'transaction FdbTransaction,
    ) -> Range {
        let prefix_key =
            if let NonFutureStackEntryItem::Bytes(b) = self.wait_and_pop(transaction).item {
                b
            } else {
                panic!("NonFutureStackEntryItem::Bytes was expected, but not found");
            }
            .into();

        Range::starts_with(prefix_key)
    }

    fn test_db_options(&self) {
        self.db
            .set_option(DatabaseOption::LocationCacheSize(100001))
            .and_then(|_| self.db.set_option(DatabaseOption::MaxWatches(10001)))
            .and_then(|_| {
                self.db
                    .set_option(DatabaseOption::DatacenterId("dc_id".to_string()))
            })
            .and_then(|_| {
                self.db
                    .set_option(DatabaseOption::MachineId("machine_id".to_string()))
            })
            .and_then(|_| self.db.set_option(DatabaseOption::SnapshotRywEnable))
            .and_then(|_| self.db.set_option(DatabaseOption::SnapshotRywDisable))
            .and_then(|_| {
                self.db
                    .set_option(DatabaseOption::TransactionLoggingMaxFieldLength(1000))
            })
            .and_then(|_| {
                self.db
                    .set_option(DatabaseOption::TransactionTimeout(100000))
            })
            .and_then(|_| self.db.set_option(DatabaseOption::TransactionTimeout(0)))
            .and_then(|_| {
                self.db
                    .set_option(DatabaseOption::TransactionMaxRetryDelay(100))
            })
            .and_then(|_| {
                self.db
                    .set_option(DatabaseOption::TransactionSizeLimit(100000))
            })
            .and_then(|_| {
                self.db
                    .set_option(DatabaseOption::TransactionRetryLimit(10))
            })
            .and_then(|_| {
                self.db
                    .set_option(DatabaseOption::TransactionRetryLimit(-1))
            })
            .and_then(|_| {
                self.db
                    .set_option(DatabaseOption::TransactionCausalReadRisky)
            })
            .and_then(|_| {
                self.db
                    .set_option(DatabaseOption::TransactionIncludePortInAddress)
            })
            .unwrap_or_else(|err| panic!("Unit test failed {:?}", err));
    }

    fn test_tr_options(&self) {
        self.db
            .run(None, |t| {
                t.set_option(TransactionOption::PrioritySystemImmediate)?;
                t.set_option(TransactionOption::PriorityBatch)?;
                t.set_option(TransactionOption::CausalReadRisky)?;
                t.set_option(TransactionOption::CausalWriteRisky)?;
                t.set_option(TransactionOption::ReadYourWritesDisable)?;
                t.set_option(TransactionOption::ReadSystemKeys)?;
                t.set_option(TransactionOption::AccessSystemKeys)?;
                t.set_option(TransactionOption::TransactionLoggingMaxFieldLength(1000))?;
                t.set_option(TransactionOption::Timeout(60 * 1000))?;
                t.set_option(TransactionOption::RetryLimit(50))?;
                t.set_option(TransactionOption::MaxRetryDelay(100))?;
                t.set_option(TransactionOption::UsedDuringCommitProtectionDisable)?;
                t.set_option(TransactionOption::DebugTransactionIdentifier(
                    "my_transaction".to_string(),
                ))?;
                t.set_option(TransactionOption::LogTransaction)?;
                t.set_option(TransactionOption::ReadLockAware)?;
                t.set_option(TransactionOption::LockAware)?;
                t.set_option(TransactionOption::IncludePortInAddress)?;

                t.get(Bytes::from(&b"\xFF"[..]).into()).join()?;

                Ok(())
            })
            .unwrap_or_else(|err| panic!("Unit test failed {:?}", err));
    }

    fn check_watches<'transaction>(
        &self,
        watches: Vec<FdbFuture<'transaction, ()>>,
        expected: bool,
    ) -> (Vec<FdbFuture<'transaction, ()>>, bool) {
        let mut ret_watches = Vec::new();

        // When returning `false`, just accumulate rest of the futures
        // using this flag.
        let mut false_to_be_returned = false;

        for (i, w) in watches.into_iter().enumerate() {
            if false_to_be_returned {
                ret_watches.push(w);
            } else if w.is_ready() || expected {
                match w.join() {
                    Ok(_) => {
                        if !expected {
                            panic!("Watch {} triggered too early", i);
                        }
                    }
                    Err(err) => {
                        let tr = self.db.create_transaction(None).unwrap_or_else(|err| {
                            panic!("Error occurred during `create_transaction`: {:?}", err)
                        });

                        unsafe { tr.on_error(err) }.join().unwrap_or_else(|err| {
                            panic!("Error occurred during `on_error`: {:?}", err)
                        });

                        false_to_be_returned = true;
                    }
                }
            } else {
                ret_watches.push(w);
            }
        }

        (ret_watches, if false_to_be_returned { false } else { true })
    }

    fn test_watches(&self) {
        loop {
            self.db
                .run(None, |t| {
                    t.set(
                        Bytes::from(&b"w0"[..]).into(),
                        Bytes::from(&b"0"[..]).into(),
                    );
                    t.set(
                        Bytes::from(&b"w2"[..]).into(),
                        Bytes::from(&b"2"[..]).into(),
                    );
                    t.set(
                        Bytes::from(&b"w3"[..]).into(),
                        Bytes::from(&b"3"[..]).into(),
                    );
                    Ok(())
                })
                .unwrap_or_else(|err| panic!("Unit test failed {:?}", err));

            let (watches, boxed_tr) = self
                .db
                .clone()
                .run_and_get_transaction(None, |t| {
                    let mut watches = Vec::new();

                    watches.push(FdbFuture::into_raw(t.watch(Bytes::from(&b"w0"[..]).into())));
                    watches.push(FdbFuture::into_raw(t.watch(Bytes::from(&b"w1"[..]).into())));
                    watches.push(FdbFuture::into_raw(t.watch(Bytes::from(&b"w2"[..]).into())));
                    watches.push(FdbFuture::into_raw(t.watch(Bytes::from(&b"w3"[..]).into())));

                    // won't trigger watch `w0` as key `w0` already
                    // `0`
                    t.set(
                        Bytes::from(&b"w0"[..]).into(),
                        Bytes::from(&b"0"[..]).into(),
                    );

                    // won't trigger watch `w1` as key `w1` is already
                    // empty.
                    t.clear(Bytes::from(&b"w1"[..]).into());

                    Ok(watches)
                })
                .unwrap_or_else(|err| panic!("Unit test failed {:?}", err));

            thread::sleep(Duration::from_secs(5));

            let unboxed_tr = *boxed_tr;

            // Convert a `*mut ()` to a `FdbFuture<'t, ()>` where `'t` is
            // tied to the lifetime of `unboxed_tr`.
            let watches = watches
                .into_iter()
                .map(|f| StackMachine::get_bounded_fdb_future(&unboxed_tr, f))
                .collect::<Vec<_>>();

            let (watches, cw) = self.check_watches(watches, false);

            if !cw {
                continue;
            }

            self.db
                .run(None, |t| {
                    t.set(
                        Bytes::from(&b"w0"[..]).into(),
                        Bytes::from(&b"a"[..]).into(),
                    );
                    t.set(
                        Bytes::from(&b"w1"[..]).into(),
                        Bytes::from(&b"b"[..]).into(),
                    );
                    t.clear(Bytes::from(&b"w2"[..]).into());
                    unsafe {
                        t.mutate(
                            MutationType::BitXor,
                            Bytes::from(&b"w3"[..]).into(),
                            Bytes::from(&b"\xFF\xFF"[..]),
                        );
                    }
                    Ok(())
                })
                .unwrap_or_else(|err| panic!("Unit test failed {:?}", err));

            let (watches, cw) = self.check_watches(watches, true);

            drop(watches);

            if cw {
                return;
            }
        }
    }

    fn test_locality(&self) {
        self.db
            .run(None, |t| {
                t.set_option(TransactionOption::Timeout(60 * 1000))?;
                t.set_option(TransactionOption::ReadSystemKeys)?;

                let boundary_keys = self.db.get_boundary_keys(
                    None,
                    Bytes::from(&b""[..]).into(),
                    Bytes::from(&b"\xFF\xFF"[..]).into(),
                    0,
                    0,
                )?;

                for i in 0..boundary_keys.len() - 1 {
                    let start = boundary_keys[i].clone();
                    let end = t
                        .get_key(KeySelector::last_less_than(boundary_keys[i + 1].clone()))
                        .join()?;

                    let start_addresses = t.get_addresses_for_key(start).join()?;
                    let end_addresses = t.get_addresses_for_key(end).join()?;

                    for a in start_addresses.iter() {
                        if !end_addresses.contains(a) {
                            panic!("Locality not internally consistent.");
                        }
                    }
                }

                Ok(())
            })
            .unwrap_or_else(|err| panic!("Unit test failed {:?}", err));
    }

    // In Go bindings this is handled by `defer`.
    fn push_err(&mut self, inst_number: usize, err: FdbError) {
        let item = StackEntryItem::Bytes({
            let mut t = Tuple::new();
            t.add_bytes(Bytes::from(&b"ERROR"[..]));
            t.add_bytes(Bytes::from(format!("{}", err.code())));
            t.pack()
        });

        self.store(inst_number, item);
    }

    fn execute_mutation<T, F>(
        &mut self,
        f: F,
        tr: &FdbTransaction,
        is_database: bool,
        inst_number: usize,
    ) where
        F: Fn(&dyn Transaction<Database = FdbDatabase>) -> FdbResult<T>,
    {
        if is_database {
            match self.db.run(None, f) {
                Ok(_) => {
                    // We do this to simulate "_DATABASE may
                    // optionally push a future onto the stack".
                    self.store(
                        inst_number,
                        StackEntryItem::Bytes(Bytes::from_static(&b"RESULT_NOT_PRESENT"[..])),
                    );
                }
                Err(err) => self.push_err(inst_number, err),
            }
        } else {
            if let Err(err) = tr.run(None, f) {
                self.push_err(inst_number, err);
            }
        }
    }

    fn log_stack(&self, entries: HashMap<usize, NonFutureStackEntry>, log_prefix: Bytes) {
        self.db
            .run(None, |tr| {
                for (stack_index, stack_entry) in entries.clone().drain() {
                    let packed_key = {
                        let mut t = Tuple::new();

                        // We can't use `t.add_i64` because `usize`
                        // will overflow it. Instead use `BigInt` and
                        // let the Tuple layer take care of properly
                        // encoding it.
                        t.add_bigint(stack_index.into());
                        t.add_bigint(stack_entry.inst_number.into());

                        Subspace::new(log_prefix.clone()).subspace(&t).pack()
                    };

                    let packed_value = {
                        let mut t = Tuple::new();

                        match stack_entry.item {
                            NonFutureStackEntryItem::BigInt(b) => t.add_bigint(b),
                            NonFutureStackEntryItem::Bool(b) => t.add_bool(b),
                            NonFutureStackEntryItem::Bytes(b) => t.add_bytes(b),
                            NonFutureStackEntryItem::Float(f) => t.add_f32(f),
                            NonFutureStackEntryItem::Double(f) => t.add_f64(f),
                            NonFutureStackEntryItem::Null => t.add_null(),
                            NonFutureStackEntryItem::String(s) => t.add_string(s),
                            NonFutureStackEntryItem::Tuple(tu) => t.add_tuple(tu),
                            NonFutureStackEntryItem::Uuid(u) => t.add_uuid(u),
                            NonFutureStackEntryItem::Versionstamp(vs) => t.add_versionstamp(vs),
                        }

                        let mut packed_t = t.pack();

                        let max_value_length = 40000;

                        if packed_t.len() >= max_value_length {
                            packed_t = packed_t.slice(0..max_value_length);
                        }

                        packed_t
                    };

                    tr.set(packed_key.into(), packed_value.into());
                }

                Ok(())
            })
            .unwrap_or_else(|err| panic!("Error occurred during `run`: {:?}", err));
    }

    fn new_transaction(&self) {
        let new_fdb_tr_ptr =
            FdbTransaction::into_raw(self.db.create_transaction(None).unwrap_or_else(|err| {
                panic!("Error occurred during `create_transaction`: {:?}", err)
            }));

        self.tr_map
            .insert(
                self.tr_name.clone(),
                FdbTransactionPtrWrapper::new(new_fdb_tr_ptr),
            )
            .map(|x| {
                // Get rid of any old `FdbTransaction` that we might
                // have. In Go, the garbage collector will take care
                // of this.
                let old_fdb_tr_ptr = x.inner;
                drop(unsafe { FdbTransaction::from_raw(old_fdb_tr_ptr) });
            });
    }

    fn current_transaction<'s, 'transaction>(
        &'s self,
        _current_transaction_lifetime: &'transaction (),
    ) -> &'transaction FdbTransaction {
        // Here we assume that we have a valid `*mut FdbTransaction`
        // that was previously created using `new_transaction` or
        // `switch_transaction`.
        let fdb_tr_ptr = (*(self.tr_map.get(&self.tr_name).unwrap())).inner;

        // `*mut FdbTransaction` will eventually be properly dropped
        // either in the main thread before calling
        // `drop(fdb_database)` or in `new_transaction` or
        // `switch_transaction`.
        Box::leak(unsafe { Box::from_raw(fdb_tr_ptr) })
    }

    fn switch_transaction(&mut self, tr_name: Bytes) {
        self.tr_map.entry(tr_name.clone()).or_insert_with(|| {
            let new_fdb_tr_ptr =
                FdbTransaction::into_raw(self.db.create_transaction(None).unwrap_or_else(|err| {
                    panic!("Error occurred during `create_transaction`: {:?}", err);
                }));
            FdbTransactionPtrWrapper::new(new_fdb_tr_ptr)
        });

        // If the previous `tr_name` has a valid `*mut FdbTransaction`
        // in `TrMap` and is not dropped by any other thread, it will
        // eventually be garbage collected in the main thread.
        self.tr_name = tr_name;
    }

    fn run(&mut self) {
        let kvs = self
            .db
            .read(None, |tr| {
                let ops_range = {
                    let mut t = Tuple::new();
                    t.add_bytes(self.prefix.clone().into());
                    t
                }
                .range(Bytes::new());

                Ok(tr
                    .get_range(
                        KeySelector::first_greater_or_equal(ops_range.begin().clone().into()),
                        KeySelector::first_greater_or_equal(ops_range.end().clone().into()),
                        RangeOptions::default(),
                    )
                    .get()?)
            })
            .unwrap_or_else(|err| panic!("Error occurred during `read`: {:?}", err));

        for (inst_number, kv) in kvs.into_iter().enumerate() {
            let inst = Tuple::from_bytes(kv.get_value().clone().into()).unwrap_or_else(|err| {
                panic!("Error occurred during `Tuple::from_bytes`: {:?}", err)
            });
            self.process_inst(inst_number, inst);
        }
    }

    fn process_inst(&mut self, inst_number: usize, inst: Tuple) {
        let mut op = inst
            .get_string_ref(0)
            .unwrap_or_else(|err| panic!("Error occurred during `inst.get_string_ref`: {:?}", err))
            .clone();

        let verbose_inst_range = VERBOSE_INST_RANGE
            .map(|x| x.contains(&inst_number))
            .unwrap_or(false);

        if self.verbose || verbose_inst_range {
            println!("Stack from [");
            self.dump_stack();
            println!(" ] ({})", self.stack.len());
            println!("{}. Instruction is {} ({:?})", inst_number, op, self.prefix);
        } else if VERBOSE_INST_ONLY {
            println!("{}. Instruction is {} ({:?})", inst_number, op, self.prefix);
        }

        if [
            "NEW_TRANSACTION",
            "PUSH",
            "DUP",
            "EMPTY_STACK",
            "SWAP",
            "POP",
            "SUB",
            "CONCAT",
            "LOG_STACK",
            "START_THREAD",
            "UNIT_TESTS",
        ]
        .contains(&op.as_str())
        {
            // For these instructions we can potentially have an
            // invalid `TrMap` entry (which can call
            // `current_transaction` to fail) *or* we don't need to
            // use `current_transaction` as it only operates on the
            // stack.
            match op.as_str() {
                "NEW_TRANSACTION" => {
                    // `NEW_TRANSACTION` op is special. We assume that
                    // we have a valid transaction below in
                    // `self.current_transaction`. However, we won't
                    // have a valid transaction till `NEW_TRANSACTION`
                    // instruction create it.
                    self.new_transaction();
                }
                // Data Operations [1]
                //
                // [1]: https://github.com/apple/foundationdb/blob/6.3.22/bindings/bindingtester/spec/bindingApiTester.md#data-operations
                "PUSH" => self.store(inst_number, StackMachine::get_additional_inst_data(&inst)),
                "DUP" => {
                    let entry = &self.stack[self.stack.len() - 1];
                    let entry_inst_number = entry.inst_number;
                    let entry_item = entry.item.clone();
                    self.store(entry_inst_number, entry_item);
                }
                "EMPTY_STACK" => self.stack.clear(),
                "SWAP" => {
                    // We assume that the `INDEX` is stored using the
                    // variant `StackEntryItem::BigInt(..)`, which we then
                    // convert to a `usize`.
                    let index = if let StackEntryItem::BigInt(b) = self.stack.pop().unwrap().item {
                        usize::try_from(b).unwrap_or_else(|err| {
                            panic!("Error occurred during `try_from`: {:?}", err)
                        })
                    } else {
                        panic!("Expected StackEntryItem::BigInt variant, which was not found!");
                    };

                    let depth_0 = self.stack.len() - 1;
                    let depth_index = depth_0 - index;

                    self.stack.swap(depth_0, depth_index);
                }
                "POP" => {
                    self.stack.pop();
                }
                "SUB" => {
                    let a = if let StackEntryItem::BigInt(bi) = self.stack.pop().unwrap().item {
                        bi
                    } else {
                        panic!("Expected StackEntryItem::BigInt variant, which was not found!");
                    };
                    let b = if let StackEntryItem::BigInt(bi) = self.stack.pop().unwrap().item {
                        bi
                    } else {
                        panic!("Expected StackEntryItem::BigInt variant, which was not found!");
                    };
                    self.store(inst_number, StackEntryItem::BigInt(a - b));
                }
                "CONCAT" => {
                    let mut outer_a = self.stack.pop().unwrap().item;
                    let mut outer_b = self.stack.pop().unwrap().item;

                    // The block below is hack. It looks like
                    // `scripted` tests pushes a `FdbFutureKey` and
                    // `FdbFutureMaybeValue`. So, if either `outer_a` or
                    // `outer_b` is a future, join on it and covert it
                    // into `StackEntryItem::Bytes`.
                    //
                    // We could have moved `concat` below. However,
                    // then we'll have to assume that `CONCAT` always
                    // is run when there is an active transaction,
                    // which might not be the case.
                    //
                    // This is a hacky compromise.
                    {
                        if let StackEntryItem::FdbFutureKey(raw_ptr_key) = outer_a {
                            let current_transaction_lifetime = ();
                            let tr = self.current_transaction(&current_transaction_lifetime);

                            let fdb_fut_key = StackMachine::get_bounded_fdb_future(tr, raw_ptr_key);

                            outer_a = fdb_fut_key
                                .join()
                                .map(|x| StackEntryItem::Bytes(x.into()))
                                .unwrap_or_else(|err| {
                                    panic!("Error occurred during `join`: {:?}", err)
                                });
                        }

                        if let StackEntryItem::FdbFutureKey(raw_ptr_key) = outer_b {
                            let current_transaction_lifetime = ();
                            let tr = self.current_transaction(&current_transaction_lifetime);

                            let fdb_fut_key = StackMachine::get_bounded_fdb_future(tr, raw_ptr_key);
                            outer_b = fdb_fut_key
                                .join()
                                .map(|x| StackEntryItem::Bytes(x.into()))
                                .unwrap_or_else(|err| {
                                    panic!("Error occurred during `join`: {:?}", err)
                                });
                        }

                        if let StackEntryItem::FdbFutureMaybeValue(raw_ptr_maybe_value) = outer_a {
                            let current_transaction_lifetime = ();
                            let tr = self.current_transaction(&current_transaction_lifetime);

                            let fdb_fut_maybe_value =
                                StackMachine::get_bounded_fdb_future(tr, raw_ptr_maybe_value);

                            outer_a = fdb_fut_maybe_value
                                .join()
                                .map(|x| {
                                    StackEntryItem::Bytes(
                                        x.map(|y| y.into()).unwrap_or_else(|| Bytes::new()),
                                    )
                                })
                                .unwrap_or_else(|err| {
                                    panic!("Error occurred during `join`: {:?}", err)
                                });
                        }

                        if let StackEntryItem::FdbFutureMaybeValue(raw_ptr_maybe_value) = outer_b {
                            let current_transaction_lifetime = ();
                            let tr = self.current_transaction(&current_transaction_lifetime);

                            let fdb_fut_maybe_value =
                                StackMachine::get_bounded_fdb_future(tr, raw_ptr_maybe_value);

                            outer_b = fdb_fut_maybe_value
                                .join()
                                .map(|x| {
                                    StackEntryItem::Bytes(
                                        x.map(|y| y.into()).unwrap_or_else(|| Bytes::new()),
                                    )
                                })
                                .unwrap_or_else(|err| {
                                    panic!("Error occurred during `join`: {:?}", err)
                                });
                        }
                    }

                    match (outer_a, outer_b) {
                    (StackEntryItem::Bytes(a), StackEntryItem::Bytes(b)) => {
			let mut res = BytesMut::new();
			res.put(a);
			res.put(b);
			self.store(inst_number, StackEntryItem::Bytes(res.into()));
		    },
                    (StackEntryItem::String(a), StackEntryItem::String(b)) => {
			let mut res = String::new();
			res.push_str(&a);
			res.push_str(&b);
			self.store(inst_number, StackEntryItem::String(res));
		    },
                    _ => panic!("Expected StackEntryItem::Bytes or StackEntryItem::String variants, which was not found!"),
                };
                }
                "LOG_STACK" => {
                    // Here we assume that when running in `--compare` mode
                    // the stack *does not* have any `FdbFuture`
                    // variants, so we can successfully log the entire
                    // stack, *without* having to wait on any
                    // `FdbFuture`.
                    //
                    // In `--concurrency` mode, the stack can have a
                    // future in it, which we just ignore. This means,
                    // `--compare` mode and `--concurrency` modes are
                    // mutually exclusive.
                    //
                    // In both the cases `fdb-stacktester` won't panic
                    // here. Error has be detected by
                    // `bindingtester.py`.
                    //
                    // `log_prefix` is `PREFIX`.
                    let log_prefix =
                        if let StackEntryItem::Bytes(b) = self.stack.pop().unwrap().item {
                            b
                        } else {
                            panic!("Expected StackEntryItem::Bytes variant, which was not found!");
                        };

                    let mut entries = HashMap::new();

                    while self.stack.len() > 0 {
                        let k = self.stack.len() - 1;

                        StackMachine::to_non_future_stack_entry(self.stack.pop().unwrap())
                            .map(|i| entries.insert(k, i));

                        // Do the transaction on 100 entries.
                        if entries.len() == 100 {
                            self.log_stack(entries.clone(), log_prefix.clone());
                            entries.clear();
                        }
                    }

                    // Log remaining entires.
                    if entries.len() != 0 {
                        self.log_stack(entries, log_prefix);
                    }
                }
                // Thread Operations
                //
                // `WAIT_EMPTY` uses `current_transaction`.
                "START_THREAD" => {
                    let prefix = if let StackEntryItem::Bytes(b) = self.stack.pop().unwrap().item {
                        b
                    } else {
                        panic!("StackEntryItem::Bytes was expected, but not found");
                    };
                    tokio::spawn(stack_tester_task(StackTesterTask::new(
                        prefix,
                        self.db.clone(),
                        self.tr_map.clone(),
                        self.task_finished.clone(),
                    )));
                }
                // Miscellaneous
                "UNIT_TESTS" => {
                    self.test_db_options();

                    // We don't have `select_api_version` tests like
                    // Go and Java because in our case, trying to call
                    // `fdb::select_api_version` more than once will
                    // cause a panic. We have integration tests for
                    // `fdb::select_api_version`.

                    self.test_tr_options();
                    self.test_watches();
                    self.test_locality();
                }
                _ => panic!("Unhandled operation {}", op),
            }
        } else {
            // In the Go bindings, there is no `is_snapshot`.
            let mut is_snapshot = false;
            let mut is_database = false;

            // Tie the lifetime of transaction to this variable.  We
            // need to do this hackery because we don't what `'self`
            // to get immutably borrowed because of
            // `self.current_transaction`.
            let current_transaction_lifetime = ();

            let tr = self.current_transaction(&current_transaction_lifetime);
            let tr_snap = tr.snapshot();

            if op.ends_with("_SNAPSHOT") {
                op.drain((op.len() - 9)..);
                is_snapshot = true;
            } else if op.ends_with("_DATABASE") {
                op.drain((op.len() - 9)..);
                is_database = true;
            }

            match op.as_str() {
                // FoundationDB Operations [1]
                //
                // While there is no classification of FoundationDB
                // operations in the spec, we have a classification in the
                // `api.py` test generator [2].
                //
                // Following order is followed.
		// - resets
                // - tuples
                // - versions, snapshot_versions
                // - reads, snapshot_reads, database_reads
                // - mutations, database_mutations
		// - read_conflicts
		// - write_conflicts
		// - txn_sizes
		// - storage_metrics
		//
		// Then we do Thread Operations [3] and Miscellaneous [4]
		//
                // `NEW_TRANSACTION` is taken care of at the beginning
                // of this function.
                //
                // [1]: https://github.com/apple/foundationdb/blob/6.3.22/bindings/bindingtester/spec/bindingApiTester.md#foundationdb-operations
                // [2]: https://github.com/apple/foundationdb/blob/6.3.22/bindings/bindingtester/tests/api.py#L143-L174
		// [3]: https://github.com/apple/foundationdb/blob/6.3.22/bindings/bindingtester/spec/bindingApiTester.md#thread-operations
		// [4]: https://github.com/apple/foundationdb/blob/6.3.22/bindings/bindingtester/spec/bindingApiTester.md#miscellaneous
                "USE_TRANSACTION" => {
                    let tr_name =
                        if let NonFutureStackEntryItem::Bytes(b) = self.wait_and_pop(tr).item {
                            b
                        } else {
                            panic!("NonFutureStackEntryItem::Bytes was expected, but not found");
                        };
                    self.switch_transaction(tr_name);
                }
                "COMMIT" => {
                    let item = StackEntryItem::FdbFutureUnit(FdbFuture::into_raw(unsafe {
                        self.current_transaction(&current_transaction_lifetime)
                            .commit()
                    }));

                    self.store(inst_number, item);
                }
                "WAIT_FUTURE" => {
                    let NonFutureStackEntry {
                        // inst_number is passed in as one of the
                        // parameters to this function. We don't want to
                        // use that.
                        inst_number: i,
                        item,
                    } = self.wait_and_pop(tr);

                    self.store(i, StackMachine::to_stack_entry_item(item));
                }
                // resets
                "ON_ERROR" => {
                    let fdb_error = FdbError::new(
                        i32::try_from(
                            if let NonFutureStackEntryItem::BigInt(b) = self.wait_and_pop(tr).item {
                                b
                            } else {
                                panic!(
                                    "NonFutureStackEntryItem::BigInt was expected, but not found"
                                );
                            },
                        )
                        .unwrap_or_else(|err| {
                            panic!(
                                "Expected i32 inside BigInt, but conversion failed {:?}",
                                err
                            )
                        }),
                    );
                    // We are just pushing the future here, and
                    // letting the `WAIT_FUTURE` instruction take care
                    // of `join`-ing and indicating
                    // `RESULT_NOT_PRESENT` or `ERROR`.
                    self.store(
                        inst_number,
                        StackEntryItem::FdbFutureUnit(FdbFuture::into_raw(unsafe {
                            self.current_transaction(&current_transaction_lifetime)
                                .on_error(fdb_error)
                        })),
                    );
                }
                // In Java bindings, this is `self.new_transaction()`,
                // but Go bindings uses `reset()` on the transaction.
                "RESET" => unsafe {
                    self.current_transaction(&current_transaction_lifetime)
                        .reset();
                },
                "CANCEL" => unsafe {
                    self.current_transaction(&current_transaction_lifetime)
                        .cancel();
                },
		"GET_VERSIONSTAMP" => {
		    let item = StackEntryItem::FdbFutureKey(FdbFuture::into_raw(unsafe {
			self.current_transaction(&current_transaction_lifetime).get_versionstamp()
		    }));

		    self.store(inst_number, item);
		}
                // Take care of `GET_READ_VERSION`, `SET`, `ATOMIC_OP` here as it
                // is one of the first APIs that is needed for binding
                // tester to work.
                "GET_READ_VERSION" => match unsafe {
                    if is_snapshot {
                        tr_snap.get_read_version()
                    } else {
                        tr.get_read_version()
                    }
                }
                .join()
                {
                    Ok(last_version) => {
                        self.last_version = last_version;

                        self.store(
                            inst_number,
                            StackEntryItem::Bytes(Bytes::from_static(&b"GOT_READ_VERSION"[..])),
                        );
                    }
                    Err(err) => self.push_err(inst_number, err),
                },
                "SET" => {
                    let key = Into::<Key>::into(
                        if let NonFutureStackEntryItem::Bytes(b) = self.wait_and_pop(tr).item {
                            b
                        } else {
                            panic!("NonFutureStackEntryItem::Bytes was expected, but not found");
                        },
                    );

                    let value = Into::<Value>::into(
                        if let NonFutureStackEntryItem::Bytes(b) = self.wait_and_pop(tr).item {
                            b
                        } else {
                            panic!("NonFutureStackEntryItem::Bytes was expected, but not found");
                        },
                    );

                    self.execute_mutation(
                        |t| {
                            t.set(key.clone(), value.clone());
                            Ok(())
                        },
                        tr,
                        is_database,
                        inst_number,
                    );
                }
                "ATOMIC_OP" => {
                    // `OPTYPE` is a string, while `KEY` and `VALUE` are bytes.
                    let op_name =
                        if let NonFutureStackEntryItem::String(s) = self.wait_and_pop(tr).item {
                            s
                        } else {
                            panic!("NonFutureStackEntryItem::Bytes was expected, but not found");
                        };

                    let mutation_type = StackMachine::from_mutation_type_string(op_name);

                    let key = Into::<Key>::into(
                        if let NonFutureStackEntryItem::Bytes(b) = self.wait_and_pop(tr).item {
                            b
                        } else {
                            panic!("NonFutureStackEntryItem::Bytes was expected, but not found");
                        },
                    );

                    let param =
                        if let NonFutureStackEntryItem::Bytes(b) = self.wait_and_pop(tr).item {
                            b
                        } else {
                            panic!("NonFutureStackEntryItem::Bytes was expected, but not found");
                        };

                    self.execute_mutation(
                        |t| {
                            unsafe {
                                t.mutate(mutation_type, key.clone(), param.clone());
                            }
                            Ok(())
                        },
                        tr,
                        is_database,
                        inst_number,
                    );
                }
                // tuples
                //
                // NOTE: Even though `SUB` [1] is mentioned in
                // `tuples` in `api.py`, we deal with it as part of
                // data operations.
                //
                // [1]: https://github.com/apple/foundationdb/blob/6.3.22/bindings/bindingtester/tests/api.py#L153
                "TUPLE_PACK" => {
                    let count = usize::try_from(
                        if let NonFutureStackEntryItem::BigInt(bi) = self.wait_and_pop(tr).item {
                            bi
                        } else {
                            panic!("NonFutureStackEntryItem::BigInt was expected, but not found");
                        },
                    )
                    .unwrap_or_else(|err| {
                        panic!("Error occurred during `usize::try_from`: {:?}", err);
                    });

                    let mut res = Tuple::new();

                    for _ in 0..count {
                        // `add_bigint` code internally uses
                        // `add_i64`, `add_i32`, `add_i16, `add_i8`.
                        match self.wait_and_pop(tr).item {
                            NonFutureStackEntryItem::BigInt(bi) => res.add_bigint(bi),
                            NonFutureStackEntryItem::Bool(b) => res.add_bool(b),
                            NonFutureStackEntryItem::Bytes(b) => res.add_bytes(b),
                            NonFutureStackEntryItem::Float(f) => res.add_f32(f),
                            NonFutureStackEntryItem::Double(d) => res.add_f64(d),
                            NonFutureStackEntryItem::Null => res.add_null(),
                            NonFutureStackEntryItem::String(s) => res.add_string(s),
                            NonFutureStackEntryItem::Tuple(t) => res.add_tuple(t),
                            NonFutureStackEntryItem::Uuid(u) => res.add_uuid(u),
                            NonFutureStackEntryItem::Versionstamp(v) => res.add_versionstamp(v),
                        }
                    }

                    self.store(inst_number, StackEntryItem::Bytes(res.pack()));
                }
                "TUPLE_PACK_WITH_VERSIONSTAMP" => {
                    let prefix =
                        if let NonFutureStackEntryItem::Bytes(b) = self.wait_and_pop(tr).item {
                            b
                        } else {
                            panic!("NonFutureStackEntryItem::Bytes was expected, but not found");
                        };

                    let count = usize::try_from(
                        if let NonFutureStackEntryItem::BigInt(bi) = self.wait_and_pop(tr).item {
                            bi
                        } else {
                            panic!("NonFutureStackEntryItem::BigInt was expected, but not found");
                        },
                    )
                    .unwrap_or_else(|err| {
                        panic!("Error occurred during `usize::try_from`: {:?}", err)
                    });

                    let mut res = Tuple::new();

                    for _ in 0..count {
                        // `add_bigint` code internally uses
                        // `add_i64`, `add_i32`, `add_i16, `add_i8`.
                        match self.wait_and_pop(tr).item {
                            NonFutureStackEntryItem::BigInt(bi) => res.add_bigint(bi),
                            NonFutureStackEntryItem::Bool(b) => res.add_bool(b),
                            NonFutureStackEntryItem::Bytes(b) => res.add_bytes(b),
                            NonFutureStackEntryItem::Float(f) => res.add_f32(f),
                            NonFutureStackEntryItem::Double(d) => res.add_f64(d),
                            NonFutureStackEntryItem::Null => res.add_null(),
                            NonFutureStackEntryItem::String(s) => res.add_string(s),
                            NonFutureStackEntryItem::Tuple(t) => res.add_tuple(t),
                            NonFutureStackEntryItem::Uuid(u) => res.add_uuid(u),
                            NonFutureStackEntryItem::Versionstamp(v) => res.add_versionstamp(v),
                        }
                    }

                    match res.pack_with_versionstamp(prefix) {
                        Ok(packed) => {
                            self.store(
                                inst_number,
                                StackEntryItem::Bytes(Bytes::from_static(&b"OK"[..])),
                            );

                            self.store(inst_number, StackEntryItem::Bytes(packed));
                        }
                        Err(err) => match err.code() {
                            TUPLE_PACK_WITH_VERSIONSTAMP_NOT_FOUND => self.store(
                                inst_number,
                                StackEntryItem::Bytes(Bytes::from_static(&b"ERROR: NONE"[..])),
                            ),
                            TUPLE_PACK_WITH_VERSIONSTAMP_MULTIPLE_FOUND => self.store(
                                inst_number,
                                StackEntryItem::Bytes(Bytes::from_static(&b"ERROR: MULTIPLE"[..])),
                            ),
                            _ => panic!("Received invalid FdbError code: {:?}", err.code()),
                        },
                    }
                }
                "TUPLE_UNPACK" => {
                    let packed_tuple = Tuple::from_bytes(
                        if let NonFutureStackEntryItem::Bytes(b) = self.wait_and_pop(tr).item {
                            b
                        } else {
                            panic!("NonFutureStackEntryItem::Bytes was expected, but not found");
                        },
                    )
                    .unwrap_or_else(|err| {
                        panic!("Error occurred during `Types::from_bytes`: {:?}", err);
                    });

                    for ti in 0..packed_tuple.size() {
                        let mut res = Tuple::new();

                        let _ = packed_tuple
                            .get_bigint(ti)
                            .map(|bi| res.add_bigint(bi))
                            .or_else(|_| packed_tuple.get_bool(ti).map(|b| res.add_bool(b)))
                            .or_else(|_| {
                                packed_tuple
                                    .get_bytes_ref(ti)
                                    .map(|b| res.add_bytes(b.clone()))
                            })
                            .or_else(|_| packed_tuple.get_f32(ti).map(|f| res.add_f32(f)))
                            .or_else(|_| packed_tuple.get_f64(ti).map(|d| res.add_f64(d)))
                            .or_else(|_| packed_tuple.get_null(ti).map(|_| res.add_null()))
                            .or_else(|_| {
                                packed_tuple
                                    .get_string_ref(ti)
                                    .map(|s| res.add_string(s.clone()))
                            })
                            .or_else(|_| {
                                packed_tuple
                                    .get_tuple_ref(ti)
                                    .map(|tup_ref| res.add_tuple(tup_ref.clone()))
                            })
                            .or_else(|_| {
                                packed_tuple
                                    .get_uuid_ref(ti)
                                    .map(|u| res.add_uuid(u.clone()))
                            })
                            .or_else(|_| {
                                packed_tuple
                                    .get_versionstamp_ref(ti)
                                    .map(|vs| res.add_versionstamp(vs.clone()))
                            })
                            .unwrap_or_else(|_| {
                                panic!("Unable to unpack packed_tuple: {:?}", packed_tuple);
                            });

                        self.store(inst_number, StackEntryItem::Bytes(res.pack()));
                    }
                }
                "TUPLE_RANGE" => {
                    let count = usize::try_from(
                        if let NonFutureStackEntryItem::BigInt(bi) = self.wait_and_pop(tr).item {
                            bi
                        } else {
                            panic!("NonFutureStackEntryItem::BigInt was expected, but not found");
                        },
                    )
                    .unwrap_or_else(|err| {
                        panic!("Error occurred during `usize::try_from`: {:?}", err);
                    });

                    let mut res_tup = Tuple::new();

                    for _ in 0..count {
                        // `add_bigint` code internally uses
                        // `add_i64`, `add_i32`, `add_i16, `add_i8`.
                        match self.wait_and_pop(tr).item {
                            NonFutureStackEntryItem::BigInt(bi) => res_tup.add_bigint(bi),
                            NonFutureStackEntryItem::Bool(b) => res_tup.add_bool(b),
                            NonFutureStackEntryItem::Bytes(b) => res_tup.add_bytes(b),
                            NonFutureStackEntryItem::Float(f) => res_tup.add_f32(f),
                            NonFutureStackEntryItem::Double(d) => res_tup.add_f64(d),
                            NonFutureStackEntryItem::Null => res_tup.add_null(),
                            NonFutureStackEntryItem::String(s) => res_tup.add_string(s),
                            NonFutureStackEntryItem::Tuple(t) => res_tup.add_tuple(t),
                            NonFutureStackEntryItem::Uuid(u) => res_tup.add_uuid(u),
                            NonFutureStackEntryItem::Versionstamp(v) => res_tup.add_versionstamp(v),
                        }
                    }

                    let res_range = res_tup.range(Bytes::new());

                    self.store(
                        inst_number,
                        StackEntryItem::Bytes(res_range.begin().clone().into()),
                    );
                    self.store(
                        inst_number,
                        StackEntryItem::Bytes(res_range.end().clone().into()),
                    );
                }
                "TUPLE_SORT" => {
                    let mut count = usize::try_from(
                        if let NonFutureStackEntryItem::BigInt(bi) = self.wait_and_pop(tr).item {
                            bi
                        } else {
                            panic!("NonFutureStackEntryItem::BigInt was expected, but not found");
                        },
                    )
                    .unwrap_or_else(|err| {
                        panic!("Error occurred during `usize::try_from`: {:?}", err);
                    });

                    iter::from_fn(|| {
                        if count == 0 {
                            None
                        } else {
                            count -= 1;
                            Some(Tuple::from_bytes(
			    if let NonFutureStackEntryItem::Bytes(b) = self.wait_and_pop(tr).item
			    {
				b
			    } else {
				panic!("NonFutureStackEntryItem::Bytes was expected, but not found");
			    })
			.unwrap_or_else(|err| panic!("Error occurred during `Tuple::from_bytes`: {:?}", err)))
                        }
                    })
                    .sorted()
                    .for_each(|tup| self.store(inst_number, StackEntryItem::Bytes(tup.pack())));
                }
                "ENCODE_FLOAT" => {
                    let val_bytes =
                        if let NonFutureStackEntryItem::Bytes(b) = self.wait_and_pop(tr).item {
                            b
                        } else {
                            panic!("NonFutureStackEntryItem::Bytes was expected, but not found");
                        };

                    let val = (&val_bytes[..]).get_f32();

                    self.store(inst_number, StackEntryItem::Float(val));
                }
                "ENCODE_DOUBLE" => {
                    let val_bytes =
                        if let NonFutureStackEntryItem::Bytes(b) = self.wait_and_pop(tr).item {
                            b
                        } else {
                            panic!("NonFutureStackEntryItem::Bytes was expected, but not found");
                        };

                    let val = (&val_bytes[..]).get_f64();

                    self.store(inst_number, StackEntryItem::Double(val));
                }
                "DECODE_FLOAT" => {
                    let val = if let NonFutureStackEntryItem::Float(f) = self.wait_and_pop(tr).item
                    {
                        f
                    } else {
                        panic!("NonFutureStackEntryItem::Float was expected, but not found");
                    };

                    let val_bytes = {
                        let mut b = BytesMut::new();
                        b.put(&val.to_be_bytes()[..]);
                        b.into()
                    };

                    self.store(inst_number, StackEntryItem::Bytes(val_bytes));
                }
                "DECODE_DOUBLE" => {
                    let val = if let NonFutureStackEntryItem::Double(d) = self.wait_and_pop(tr).item
                    {
                        d
                    } else {
                        panic!("NonFutureStackEntryItem::Double was expected, but not found");
                    };

                    let val_bytes = {
                        let mut b = BytesMut::new();
                        b.put(&val.to_be_bytes()[..]);
                        b.into()
                    };

                    self.store(inst_number, StackEntryItem::Bytes(val_bytes));
                }
                // versions, snapshot_versions
                //
                // `GET_READ_VERSION` and `GET_READ_VERSION_SNAPSHOT`
                // is take care of above.
                "SET_READ_VERSION" => unsafe {
                    self.current_transaction(&current_transaction_lifetime)
                        .set_read_version(self.last_version)
                },
                "GET_COMMITTED_VERSION" => {
                    self.last_version = unsafe {
                        self.current_transaction(&current_transaction_lifetime)
                            .get_committed_version()
                    }
                    .unwrap_or_else(|err| {
                        panic!("Error occurred during `get_committed_version`: {:?}", err)
                    });
                    self.store(
                        inst_number,
                        StackEntryItem::Bytes(Bytes::from_static(&b"GOT_COMMITTED_VERSION"[..])),
                    );
                }
                // reads, snapshot_reads, database_reads
                "GET" => {
                    let key = if let NonFutureStackEntryItem::Bytes(b) = self.wait_and_pop(tr).item
                    {
                        b
                    } else {
                        panic!("NonFutureStackEntryItem::Bytes was expected, but not found");
                    };

                    // Only push future onto the stack for `GET` and
                    // `GET_SNAPSHOT`.
                    if is_database {
                        match self.db.read(None, |rtr| Ok(rtr.get(key.clone().into()).join()?)) {
                            Ok(value) => {
                                let item =
                                    StackEntryItem::Bytes(value.map(|v| v.into()).unwrap_or_else(
                                        || Bytes::from(&b"RESULT_NOT_PRESENT"[..]),
                                    ));
                                self.store(inst_number, item);
                            }
                            Err(err) => self.push_err(inst_number, err),
                        }
                    } else {
                        let item =
                            StackEntryItem::FdbFutureMaybeValue(FdbFuture::into_raw(
                                if is_snapshot {
                                    tr_snap.get(key.into())
                                } else {
                                    tr.get(key.into())
                                }
                            ));

                        self.store(inst_number, item);
                    }
                }
                "GET_KEY" => {
                    let sel = self.pop_selector(tr);

                    let prefix =
                        if let NonFutureStackEntryItem::Bytes(b) = self.wait_and_pop(tr).item {
                            b
                        } else {
                            panic!("NonFutureStackEntryItem::Bytes was expected, but not found");
                        };

                    // We don't push future onto the stack. This is
                    // because we'll then have to deal with the
                    // `RESULT` logic in `wait_and_pop`. Instead we
                    // just `join` and deal with the `RESULT` logic
                    // here.

                    let fn_closure = |t: &dyn ReadTransaction| Ok(t.get_key(sel.clone()).join()?);

                    match if is_database {
                        self.db.read(None, fn_closure)
                    } else if is_snapshot {
                        tr_snap.read(None, fn_closure)
                    } else {
                        tr.read(None, fn_closure)
                    } {
                        Ok(kb) => {
                            let key_bytes = Bytes::from(kb);

                            if bytes_util::starts_with(key_bytes.clone(), prefix.clone()) {
                                self.store(inst_number, StackEntryItem::Bytes(key_bytes));
                            } else if key_bytes.cmp(&prefix) == Ordering::Less {
                                self.store(inst_number, StackEntryItem::Bytes(prefix));
                            } else {
                                self.store(
                                    inst_number,
                                    StackEntryItem::Bytes(
                                        bytes_util::strinc(prefix).unwrap_or_else(|err| {
                                            panic!(
                                                "Error occurred during `bytes_util::strinc`: {:?}",
                                                err
                                            )
                                        }),
                                    ),
                                );
                            }
                        }
                        Err(err) => self.push_err(inst_number, err),
                    }
                }
                "GET_RANGE" | "GET_RANGE_STARTS_WITH" | "GET_RANGE_SELECTOR" => {
                    let (begin_key_selector, end_key_selector) = match op.as_str() {
                        "GET_RANGE_STARTS_WITH" => {
                            let prefix_range = self.pop_prefix_range(tr);
                            (
                                KeySelector::first_greater_or_equal(prefix_range.begin().clone()),
                                KeySelector::first_greater_or_equal(prefix_range.end().clone()),
                            )
                        }
                        "GET_RANGE_SELECTOR" => {
                            let begin_ks = self.pop_selector(tr);
                            let end_ks = self.pop_selector(tr);
                            (begin_ks, end_ks)
                        }
                        _ => {
                            // GET_RANGE
                            let key_range = self.pop_key_range(tr);
                            (
                                KeySelector::first_greater_or_equal(key_range.begin().clone()),
                                KeySelector::first_greater_or_equal(key_range.end().clone()),
                            )
                        }
                    };

                    let range_options = self.pop_range_options(tr);

                    let mut prefix = None;
                    if op.as_str() == "GET_RANGE_SELECTOR" {
                        prefix = Some(
                            if let NonFutureStackEntryItem::Bytes(b) = self.wait_and_pop(tr).item {
                                b
                            } else {
                                panic!(
                                    "NonFutureStackEntryItem::Bytes was expected, but not found"
                                );
                            },
                        );
                    }

                    let fn_closure = |t: &dyn ReadTransaction| -> FdbResult<Vec<KeyValue>> {
                        Ok(t.get_range(
                            begin_key_selector.clone(),
                            end_key_selector.clone(),
                            range_options.clone(),
                        )
                        .get()?)
                    };

                    match if is_database {
                        self.db.read(None, fn_closure)
                    } else if is_snapshot {
                        tr_snap.read(None, fn_closure)
                    } else {
                        tr.read(None, fn_closure)
                    } {
                        Ok(kvs) => {
                            let mut res = Tuple::new();

                            for kv in kvs {
                                if let Some(ref p) = prefix {
                                    // GET_RANGE_SELECTOR
                                    if bytes_util::starts_with(
                                        kv.get_key().clone().into(),
                                        p.clone(),
                                    ) {
                                        res.add_bytes(kv.get_key().clone().into());
                                        res.add_bytes(kv.get_value().clone().into());
                                    }
                                } else {
                                    // GET_RANGE, GET_RANGE_STARTS_WITH
                                    res.add_bytes(kv.get_key().clone().into());
                                    res.add_bytes(kv.get_value().clone().into());
                                }
                            }

                            self.store(inst_number, StackEntryItem::Bytes(res.pack()));
                        }
                        Err(err) => self.push_err(inst_number, err),
                    }
                }
                // mutations, database_mutations
                //
                // `SET` and `ATOMIC_OP` is taken care of above. Even
                // though mutations has `VERSIONSTAMP`, there is no
                // instruction with that name.
                "CLEAR" => {
                    let key = Into::<Key>::into(
                        if let NonFutureStackEntryItem::Bytes(b) = self.wait_and_pop(tr).item {
                            b
                        } else {
                            panic!("NonFutureStackEntryItem::Bytes was expected, but not found");
                        },
                    );

                    self.execute_mutation(
                        |t| {
                            t.clear(key.clone());
                            Ok(())
                        },
                        tr,
                        is_database,
                        inst_number,
                    );
                }
                "CLEAR_RANGE" | "CLEAR_RANGE_STARTS_WITH" => {
                    let exact_range = if op.as_str() == "CLEAR_RANGE_STARTS_WITH" {
                        self.pop_prefix_range(tr)
                    } else {
                        // CLEAR_RANGE
                        self.pop_key_range(tr)
                    };

                    self.execute_mutation(
                        |t| {
                            t.clear_range(exact_range.clone());
                            Ok(())
                        },
                        tr,
                        is_database,
                        inst_number,
                    );
                }
                // read_conflicts
                "READ_CONFLICT_RANGE" => {
                    let key_range = self.pop_key_range(tr);

                    match self
                        .current_transaction(&current_transaction_lifetime)
                        .add_read_conflict_range(key_range)
                    {
                        Ok(_) => self.store(
                            inst_number,
                            StackEntryItem::Bytes(Bytes::from_static(&b"SET_CONFLICT_RANGE"[..])),
                        ),
                        Err(err) => self.push_err(inst_number, err),
                    }
                }
                "READ_CONFLICT_KEY" => {
                    let key = if let NonFutureStackEntryItem::Bytes(b) = self.wait_and_pop(tr).item
                    {
                        b
                    } else {
                        panic!("NonFutureStackEntryItem::Bytes was expected, but not found");
                    }
                    .into();

                    match self
                        .current_transaction(&current_transaction_lifetime)
                        .add_read_conflict_key(key)
                    {
                        Ok(_) => self.store(
                            inst_number,
                            StackEntryItem::Bytes(Bytes::from_static(&b"SET_CONFLICT_KEY"[..])),
                        ),
                        Err(err) => self.push_err(inst_number, err),
                    }
                }
		// write_conflicts
		"WRITE_CONFLICT_RANGE" => {
                    let key_range = self.pop_key_range(tr);

                    match self
                        .current_transaction(&current_transaction_lifetime)
                        .add_write_conflict_range(key_range)
                    {
                        Ok(_) => self.store(
                            inst_number,
                            StackEntryItem::Bytes(Bytes::from_static(&b"SET_CONFLICT_RANGE"[..])),
                        ),
                        Err(err) => self.push_err(inst_number, err),
                    }
		}
                "WRITE_CONFLICT_KEY" => {
                    let key = if let NonFutureStackEntryItem::Bytes(b) = self.wait_and_pop(tr).item
                    {
                        b
                    } else {
                        panic!("NonFutureStackEntryItem::Bytes was expected, but not found");
                    }
                    .into();

                    match self
                        .current_transaction(&current_transaction_lifetime)
                        .add_write_conflict_key(key)
                    {
                        Ok(_) => self.store(
                            inst_number,
                            StackEntryItem::Bytes(Bytes::from_static(&b"SET_CONFLICT_KEY"[..])),
                        ),
                        Err(err) => self.push_err(inst_number, err),
                    }
                }
		"DISABLE_WRITE_CONFLICT" => self.current_transaction(&current_transaction_lifetime).set_option(TransactionOption::NextWriteNoWriteConflictRange)
		    .unwrap_or_else(|err| panic!("Error occurred during `set_option(TransactionOption::NextWriteNoWriteConflictRange)`: {:?}", err)),
		// txn_sizes
		"GET_APPROXIMATE_SIZE" => match self
                        .current_transaction(&current_transaction_lifetime)
                        .get_approximate_size().join()
                    {
                        Ok(_) => self.store(
                            inst_number,
                            StackEntryItem::Bytes(Bytes::from_static(&b"GOT_APPROXIMATE_SIZE"[..])),
                        ),
                        Err(err) => self.push_err(inst_number, err),
                    }
		// storage_metrics
		"GET_ESTIMATED_RANGE_SIZE" => {
		    let key_range = self.pop_key_range(tr);

		    match tr_snap.read(None, |t| Ok(t.get_estimated_range_size_bytes(key_range.clone()).join()?)) {
                        Ok(_) => self.store(
                            inst_number,
                            StackEntryItem::Bytes(Bytes::from_static(&b"GOT_ESTIMATED_RANGE_SIZE"[..])),
                        ),
                        Err(err) => self.push_err(inst_number, err),
		    }
		}
		// Thread Operations
		"WAIT_EMPTY" => {
		    let prefix_range = self.pop_prefix_range(tr);
		    let begin_key_selector = KeySelector::first_greater_or_equal(prefix_range.begin().clone());
		    let end_key_selector = KeySelector::first_greater_or_equal(prefix_range.end().clone());
		    match self.db.run(None, |t| {
			if t.get_range(
			    begin_key_selector.clone(),
			    end_key_selector.clone(),
			    RangeOptions::default()
			).get()?.len() != 0 {
			    Err(FdbError::new(1020))
			} else {
			    Ok(())
			}
		    }) {
                        Ok(_) => self.store(
                            inst_number,
                            StackEntryItem::Bytes(Bytes::from_static(&b"WAITED_FOR_EMPTY"[..])),
                        ),
                        Err(err) => self.push_err(inst_number, err),
		    }
		}
                _ => panic!("Unhandled operation {}", op),
            }
        }

        if self.verbose || verbose_inst_range {
            println!("        to [");
            self.dump_stack();
            println!(" ] ({})\n", self.stack.len());
        }
    }

    fn store(&mut self, inst_number: usize, item: StackEntryItem) {
        self.stack.push(StackEntry { item, inst_number });
    }

    fn dump_stack(&self) {
        let stack_len = self.stack.len();

        if stack_len == 0 {
            return;
        }

        let mut i = stack_len - 1;
        loop {
            print!(" {}. {:?}", self.stack[i].inst_number, self.stack[i].item);
            if i == 0 {
                return;
            } else {
                println!(",");
            }
            i -= 1;
        }
    }
}

// A binding tester `THREAD` would be a Tokio task.
#[derive(Debug)]
struct StackTesterTask {
    tr_map: TrMap,

    prefix: Bytes,
    db: FdbDatabase,

    // When all tasks drop `task_finished` then in the root task can
    // shutdown the runtime and prepare to exit the main thread.
    task_finished: Sender<()>,
}

impl StackTesterTask {
    fn new(
        prefix: Bytes,
        db: FdbDatabase,
        tr_map: TrMap,
        task_finished: Sender<()>,
    ) -> StackTesterTask {
        StackTesterTask {
            prefix,
            db,
            tr_map,
            task_finished,
        }
    }
}

async fn stack_tester_task(stt: StackTesterTask) -> Result<(), Box<dyn Error + Send + Sync>> {
    let StackTesterTask {
        prefix,
        db,
        tr_map,
        task_finished,
    } = stt;

    // Spawn a blocking thread from Tokio threadpool to run the
    // `StackMachine`. It just makes the process of managing
    // `StackMachine` easier, as we don't have to worry about passing
    // `StackMachine` between Tokio tasks.
    Handle::current()
        .spawn_blocking(move || {
            let mut sm = StackMachine::new(tr_map, prefix, db, task_finished, VERBOSE);
            sm.run();
        })
        .await
        .unwrap_or_else(|err| panic!("Error occurred during `spawn_blocking`: {:?}", err));

    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = env::args().collect::<Vec<String>>();

    let prefix = Bytes::from(args[1].clone());
    let api_version = args[2].parse::<i32>()?;
    let cluster_file_path = if args.len() > 3 { args[3].as_str() } else { "" };

    unsafe {
        fdb::select_api_version(api_version);
        fdb::start_network();
    }

    let fdb_database = fdb::open_database(cluster_file_path)?;

    let rt = Runtime::new()?;

    // FoundationDB binding tester specification [1] requires us to
    // maintain a global transacion that maps string to
    // transactions. In Go, this is called `trMap`. Because Go has a
    // garbage collector, its relatively easy to move a transaction
    // between threads and pass the buck over to the GC when we no
    // longer need it.
    //
    // We don't have that luxury in Rust. Also in our API design, we
    // force `FdbTransaction` to be `!Send + !Sync`.
    //
    // While the canonical way to run a transaction within a `.read`
    // or `.run` method, we can't use that in binding tester.
    //
    // Given these limitations, we are doing something very gnarly,
    // that throws the Rust provided safety guarantees out of the
    // window. We are taking a `FdbTransaction` value, putting it on
    // the heap by wrapping it inside a `Box`, and then leaking a raw
    // pointer that we are then putting into the `Dashmap`. In
    // addition we are also leaking a reference to `FdbTrasaction` in
    // `StackMachine::current_transaction`. Basically we are managing
    // the lifetime of `*mut FdbTransaction` ourselves.
    //
    // Because `*mut FdbTransaction` is `!Send`, we are putting that
    // into a `FdbTransactionPtrWrapper` and forcing it to be `Send`
    // by implementing `unsafe Send + Sync` for it.
    //
    // Please don't do something like this because you can. We are
    // doing it because we don't have a choice!
    //
    // [1]: https://github.com/apple/foundationdb/blob/69b572cc869856cd7c4d0917f73142b2ca68257c/bindings/bindingtester/spec/bindingApiTester.md

    let tr_map = Arc::new(DashMap::<Bytes, FdbTransactionPtrWrapper>::new());

    rt.block_on(async {
        let (task_finished, mut task_finished_recv) = mpsc::channel::<()>(1);

        tokio::spawn(stack_tester_task(StackTesterTask::new(
            prefix,
            fdb_database.clone(),
            tr_map.clone(),
            task_finished.clone(),
        )));

        // We need to drop this so that `.recv()` below can unblock
        // after all the remaining `task_finished` are dropped, which
        // happens when `stack_tester_task` exits.
        drop(task_finished);

        let _ = task_finished_recv.recv().await;
    });

    // Get rid of any old `FdbTransaction` that we might have. In Go,
    // the garbage collector will take care of this.
    //
    // By this time, we should have only one strong reference to
    // `tr_map`. So, the following `Arc::try_unwrap` should not fail.
    Arc::try_unwrap(tr_map)
        .unwrap_or_else(|err| panic!("Error occurred during `Arc::try_unwrap()`: {:?}", err))
        .into_iter()
        .for_each(|(_, v)| {
            let old_fdb_tr_ptr = v.inner;
            drop(unsafe { FdbTransaction::from_raw(old_fdb_tr_ptr) });
        });

    drop(fdb_database);

    unsafe {
        fdb::stop_network();
    }

    Ok(())
}
