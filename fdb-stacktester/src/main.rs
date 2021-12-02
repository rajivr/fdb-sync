use bytes::{Buf, BufMut, Bytes, BytesMut};

use dashmap::DashMap;

use fdb::database::{Database, FdbDatabase};
use fdb::error::{
    FdbError, FdbResult, TUPLE_PACK_WITH_VERSIONSTAMP_MULTIPLE_FOUND,
    TUPLE_PACK_WITH_VERSIONSTAMP_NOT_FOUND,
};
use fdb::future::{FdbFuture, FdbFutureGet};
use fdb::range::{Range, RangeOptions, StreamingMode};
use fdb::subspace::Subspace;
use fdb::transaction::{
    FdbTransaction, MutationType, ReadTransaction, ReadTransactionContext, Transaction,
    TransactionContext,
};
use fdb::tuple::{bytes_util, Tuple, Versionstamp};
use fdb::{self, Key, KeySelector, Value};

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

const VERBOSE: bool = true;
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
#[derive(Debug, Clone)]
enum StackEntryItem {
    FdbFutureI64(*mut i64),
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

    fn to_non_future_stack_entry_item(sei: StackEntryItem) -> NonFutureStackEntryItem {
        match sei {
            StackEntryItem::BigInt(b) => NonFutureStackEntryItem::BigInt(b),
            StackEntryItem::Bool(b) => NonFutureStackEntryItem::Bool(b),
            StackEntryItem::Bytes(b) => NonFutureStackEntryItem::Bytes(b),
            StackEntryItem::Float(f) => NonFutureStackEntryItem::Float(f),
            StackEntryItem::Double(f) => NonFutureStackEntryItem::Double(f),
            StackEntryItem::Null => NonFutureStackEntryItem::Null,
            StackEntryItem::String(s) => NonFutureStackEntryItem::String(s),
            StackEntryItem::Tuple(t) => NonFutureStackEntryItem::Tuple(t),
            StackEntryItem::Uuid(u) => NonFutureStackEntryItem::Uuid(u),
            StackEntryItem::Versionstamp(vs) => NonFutureStackEntryItem::Versionstamp(vs),
            _ => panic!(
                "Cannot convert to NonFutureStackEntryItem as the variant contains an FdbFuture"
            ),
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

    fn to_non_future_stack_entry(se: StackEntry) -> NonFutureStackEntry {
        let StackEntry { item, inst_number } = se;

        let item = StackMachine::to_non_future_stack_entry_item(item);

        NonFutureStackEntry { item, inst_number }
    }

    fn to_stack_entry(nfse: NonFutureStackEntry) -> StackEntry {
        let NonFutureStackEntry { item, inst_number } = nfse;

        let item = StackMachine::to_stack_entry_item(item);

        StackEntry { item, inst_number }
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

    fn wait_and_pop<'transaction>(
        &mut self,
        transaction: &'transaction FdbTransaction,
    ) -> NonFutureStackEntry {
        let StackEntry { item, inst_number } = self.stack.pop().unwrap();

        match item {
            StackEntryItem::FdbFutureI64(raw_ptr_i64) => {
                let fdb_fut_i64 = StackMachine::get_bounded_fdb_future(transaction, raw_ptr_i64);
                fdb_fut_i64
                    .join()
                    .map(|x| {
                        let item = NonFutureStackEntryItem::BigInt(x.into());
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

    // This function mutates the database and does not use the global
    // transaction map. In Go, it uses `Transact` method, which is on
    // `Database` type.
    //
    // In our case, we will use `self.db`. If we don't do this, then
    // multiple mutations will try to use the same transaction. After
    // the first mutation (for example `SET` operation) finishes, the
    // second mutation (for example another `SET` operation) would
    // fail with error code 2017 (Operation issued while a commit was
    // outstanding).
    //
    // It is also possible that `run` might fail, in which case we
    // push an error onto the stack.
    fn execute_mutation<'transaction, T, F>(&mut self, f: F, is_database: bool, inst_number: usize)
    where
        F: Fn(&dyn Transaction<Database = FdbDatabase>) -> FdbResult<T>,
    {
        match self.db.run(f) {
            Ok(_) => {
                if is_database {
                    self.store(
                        inst_number,
                        StackEntryItem::Bytes(Bytes::from_static(&b"RESULT_NOT_PRESENT"[..])),
                    );
                }
            }
            Err(err) => self.push_err(inst_number, err),
        }
    }

    fn log_stack(&self, entries: HashMap<usize, NonFutureStackEntry>, log_prefix: Bytes) {
        self.db
            .run(|tr| {
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
            FdbTransaction::into_raw(self.db.create_transaction().unwrap_or_else(|err| {
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
                FdbTransaction::into_raw(self.db.create_transaction().unwrap_or_else(|err| {
                    panic!("Error occurred during `create_transaction`: {:?}", err);
                }));
            FdbTransactionPtrWrapper::new(new_fdb_tr_ptr)
        });

        // If the previous `tr_name` has a valid `*mut FdbTransaction`
        // in `TrMap` and is not dropped by any other thread, it will
        // eventually be garbage collected in the main thread.
        self.tr_name = tr_name;
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

    fn run(&mut self) {
        for (inst_number, inst) in [
            // setup `last_version` to some good value
            "NEW_TRANSACTION",
            "GET_READ_VERSION",
            "COMMIT",
            "WAIT_FUTURE",
            // minimal
            "NEW_TRANSACTION",
            "COMMIT",
            "WAIT_FUTURE",
            "GET_COMMITTED_VERSION",
            "RESET",
            "SET_READ_VERSION",
            "CANCEL",
            "GET_READ_VERSION",
        ]
        .iter()
        .map(|x| {
            let mut t = Tuple::new();
            t.add_string(x.to_string());
            t
        })
        .enumerate()
        {
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
            println!("tr_map: {:?}", self.tr_map);
            println!("last_version: {:?}", self.last_version);
            println!("{}. Instruction is {} ({:?})", inst_number, op, self.prefix);
        } else if VERBOSE_INST_ONLY {
            println!("{}. Instruction is {} ({:?})", inst_number, op, self.prefix);
        }

        // `NEW_TRANSACTION` op is special. We assume that we have a
        // valid transaction below in
        // `self.current_transaction`. However, we won't have a valid
        // transaction till `NEW_TRANSACTION` instruction create it.
        if op.as_str() == "NEW_TRANSACTION" {
            self.new_transaction()
        } else {
            // When working on `_DATABASE` operations, store the
            // `FdbTransaction` value here.
            let mut database_fdb_transaction = None;

            // In the Go bindings, there is no `is_snapshot`.
            let mut is_snapshot = false;
            let mut is_database = false;

            // Tie the lifetime of transaction to this variable on the
            // stack. We need to do this hackery because we don't what
            // `'self` to get immutably borrowed because of
            // `self.current_transaction`.
            let current_transaction_lifetime = ();

            let t;
            let rt;

            if op.ends_with("_SNAPSHOT") {
                t = self.current_transaction(&current_transaction_lifetime);

                rt = t.snapshot();

                op.drain((op.len() - 9)..);

                is_snapshot = true;
            } else if op.ends_with("_DATABASE") {
                database_fdb_transaction =
                    Some(self.db.create_transaction().unwrap_or_else(|err| {
                        panic!("Error occurred during `create_transaction`: {:?}", err);
                    }));
                t = database_fdb_transaction.as_ref().unwrap();
                rt = t.snapshot();

                op.drain((op.len() - 9)..);

                is_database = true;
            } else {
                t = self.current_transaction(&current_transaction_lifetime);
                rt = t.snapshot();
            }

            match op.as_str() {
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
                    let outer_a = self.stack.pop().unwrap().item;
                    let outer_b = self.stack.pop().unwrap().item;

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
                    // Here we assume that the stack *does not* have any
                    // `FdbFuture` variants, so we can successfully log
                    // the entire stack, *without* having to wait on any
                    // `FdbFuture`.
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
                        entries.insert(
                            k,
                            StackMachine::to_non_future_stack_entry(self.stack.pop().unwrap()),
                        );
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

                // FoundationDB Operations [1]
                //
                // While there is no classification of FoundationDB
                // operations in the spec, we have a classification in the
                // `api.py` test generator [2].
                //
                // Following order is followed.
                // - resets
                // - tuples
                //
                // `NEW_TRANSACTION` is taken care of at the beginning
                // of this function.
                //
                // [1]: https://github.com/apple/foundationdb/blob/6.3.22/bindings/bindingtester/spec/bindingApiTester.md#foundationdb-operations
                // [2]: https://github.com/apple/foundationdb/blob/6.3.22/bindings/bindingtester/tests/api.py#L143-L174
                "USE_TRANSACTION" => {
                    let tr_name =
                        if let NonFutureStackEntryItem::Bytes(b) = self.wait_and_pop(t).item {
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
                    } = self.wait_and_pop(t);

                    self.store(i, StackMachine::to_stack_entry_item(item));
                }
                // resets
                "ON_ERROR" => {
                    let fdb_error = FdbError::new(
                        i32::try_from(
                            if let NonFutureStackEntryItem::BigInt(b) = self.wait_and_pop(t).item {
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
                    self.store(
                        inst_number,
                        StackEntryItem::FdbFutureUnit(FdbFuture::into_raw(unsafe {
                            self.current_transaction(&current_transaction_lifetime)
                                .on_error(fdb_error)
                        })),
                    );
                }
                "RESET" =>
                // TODO
                //
                // In Java bindings, this is `self.new_transaction()`,
                // but Go bindings uses `reset()` on the transaction.
                unsafe {
                    self.current_transaction(&current_transaction_lifetime)
                        .reset()
                }
                "CANCEL" =>
                // TODO
                unsafe {
                    self.current_transaction(&current_transaction_lifetime)
                        .cancel()
                }

                // Take care of `GET_READ_VERSION`, `SET`, `ATOMIC_OP` here as it
                // is one of the first APIs that is needed for binding
                // tester to work.
                "GET_READ_VERSION" => {
                    // If it is a `GET_READ_VERSION` then, `rt` would
                    // be `.snapshot()`.
                    //
                    // It is also possible that `read` might fail
                    // (error code 1025), in which case we push an
                    // error onto the stack.
                    match rt.read(|rtr| Ok(unsafe { rtr.get_read_version() }.join()?)) {
                        Ok(last_version) => {
                            self.last_version = last_version;

                            self.store(
                                inst_number,
                                StackEntryItem::Bytes(Bytes::from_static(&b"GOT_READ_VERSION"[..])),
                            );
                        }
                        Err(err) => self.push_err(inst_number, err),
                    }
                }
                "SET" => {
                    let key = Into::<Key>::into(
                        if let NonFutureStackEntryItem::Bytes(b) = self.wait_and_pop(t).item {
                            b
                        } else {
                            panic!("NonFutureStackEntryItem::Bytes was expected, but not found");
                        },
                    );

                    let value = Into::<Value>::into(
                        if let NonFutureStackEntryItem::Bytes(b) = self.wait_and_pop(t).item {
                            b
                        } else {
                            panic!("NonFutureStackEntryItem::Bytes was expected, but not found");
                        },
                    );

                    self.execute_mutation(
                        |tr| {
                            tr.set(key.clone(), value.clone());
                            Ok(())
                        },
                        is_database,
                        inst_number,
                    );
                }
                "ATOMIC_OP" => {
                    // `OPTYPE` is a string, while `KEY` and `VALUE` are bytes.
                    let op_name =
                        if let NonFutureStackEntryItem::String(s) = self.wait_and_pop(t).item {
                            s
                        } else {
                            panic!("NonFutureStackEntryItem::Bytes was expected, but not found");
                        };

                    let mutation_type = StackMachine::from_mutation_type_string(op_name);

                    let key = Into::<Key>::into(
                        if let NonFutureStackEntryItem::Bytes(b) = self.wait_and_pop(t).item {
                            b
                        } else {
                            panic!("NonFutureStackEntryItem::Bytes was expected, but not found");
                        },
                    );

                    let param = if let NonFutureStackEntryItem::Bytes(b) = self.wait_and_pop(t).item
                    {
                        b
                    } else {
                        panic!("NonFutureStackEntryItem::Bytes was expected, but not found");
                    };

                    self.execute_mutation(
                        |tr| {
                            unsafe {
                                tr.mutate(mutation_type, key.clone(), param.clone());
                            }
                            Ok(())
                        },
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
                        if let NonFutureStackEntryItem::BigInt(bi) = self.wait_and_pop(t).item {
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
                        match self.wait_and_pop(t).item {
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
                        if let NonFutureStackEntryItem::Bytes(b) = self.wait_and_pop(t).item {
                            b
                        } else {
                            panic!("NonFutureStackEntryItem::Bytes was expected, but not found");
                        };

                    let count = usize::try_from(
                        if let NonFutureStackEntryItem::BigInt(bi) = self.wait_and_pop(t).item {
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
                        match self.wait_and_pop(t).item {
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
                        if let NonFutureStackEntryItem::Bytes(b) = self.wait_and_pop(t).item {
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
                        if let NonFutureStackEntryItem::BigInt(bi) = self.wait_and_pop(t).item {
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
                        match self.wait_and_pop(t).item {
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
                        if let NonFutureStackEntryItem::BigInt(bi) = self.wait_and_pop(t).item {
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
			    if let NonFutureStackEntryItem::Bytes(b) = self.wait_and_pop(t).item
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
                        if let NonFutureStackEntryItem::Bytes(b) = self.wait_and_pop(t).item {
                            b
                        } else {
                            panic!("NonFutureStackEntryItem::Bytes was expected, but not found");
                        };

                    let val = (&val_bytes[..]).get_f32();

                    self.store(inst_number, StackEntryItem::Float(val));
                }
                "ENCODE_DOUBLE" => {
                    let val_bytes =
                        if let NonFutureStackEntryItem::Bytes(b) = self.wait_and_pop(t).item {
                            b
                        } else {
                            panic!("NonFutureStackEntryItem::Bytes was expected, but not found");
                        };

                    let val = (&val_bytes[..]).get_f64();

                    self.store(inst_number, StackEntryItem::Double(val));
                }
                "DECODE_FLOAT" => {
                    let val = if let NonFutureStackEntryItem::Float(f) = self.wait_and_pop(t).item {
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
                    let val = if let NonFutureStackEntryItem::Double(d) = self.wait_and_pop(t).item
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
                //
                // TODO: removed other tests to see if can track down
                // the "on_error" error incuding tuple
                "GET" => {
                    let key = if let NonFutureStackEntryItem::Bytes(b) = self.wait_and_pop(t).item {
                        b
                    } else {
                        panic!("NonFutureStackEntryItem::Bytes was expected, but not found");
                    };

                    // In Java and Go bindings, it is possible to leak a
                    // future from the closure. We do not allow this
                    // pattern in our bindings. So, we resolve the future
                    // within the closure.
                    match rt.read(|tr| Ok(tr.get(key.clone().into()).join()?)) {
                        Ok(value) => {
                            let item = StackEntryItem::Bytes(
                                value
                                    .map(|v| v.into())
                                    .unwrap_or_else(|| Bytes::from(&b"RESULT_NOT_PRESENT"[..])),
                            );
                            self.store(inst_number, item);
                        }
                        Err(err) => self.push_err(inst_number, err),
                    }
                }
                "GET_KEY" => {
                    let sel = self.pop_selector(t);

                    let prefix =
                        if let NonFutureStackEntryItem::Bytes(b) = self.wait_and_pop(t).item {
                            b
                        } else {
                            panic!("NonFutureStackEntryItem::Bytes was expected, but not found");
                        };

                    match rt.read(|tr| Ok(tr.get_key(sel.clone()).join()?)) {
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
                            let prefix_range = self.pop_prefix_range(t);
                            (
                                KeySelector::first_greater_or_equal(prefix_range.begin().clone()),
                                KeySelector::first_greater_or_equal(prefix_range.end().clone()),
                            )
                        }
                        "GET_RANGE_SELECTOR" => {
                            let begin_ks = self.pop_selector(t);
                            let end_ks = self.pop_selector(t);
                            (begin_ks, end_ks)
                        }
                        _ => {
                            // GET_RANGE
                            let key_range = self.pop_key_range(t);
                            (
                                KeySelector::first_greater_or_equal(key_range.begin().clone()),
                                KeySelector::first_greater_or_equal(key_range.end().clone()),
                            )
                        }
                    };

                    let range_options = self.pop_range_options(t);

                    let mut prefix = None;
                    if op.as_str() == "GET_RANGE_SELECTOR" {
                        prefix = Some(
                            if let NonFutureStackEntryItem::Bytes(b) = self.wait_and_pop(t).item {
                                b
                            } else {
                                panic!(
                                    "NonFutureStackEntryItem::Bytes was expected, but not found"
                                );
                            },
                        );
                    }

                    match rt.read(|tr| {
                        Ok(tr
                            .get_range(
                                begin_key_selector.clone(),
                                end_key_selector.clone(),
                                range_options.clone(),
                            )
                            .get()?)
                    }) {
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
                // // mutations, database_mutations
                // "CLEAR" => {
                //     let key = Into::<Key>::into(
                //         if let NonFutureStackEntryItem::Bytes(b) = self.wait_and_pop(t).item {
                //             b
                //         } else {
                //             panic!("NonFutureStackEntryItem::Bytes was expected, but not found");
                //         },
                //     );

                //     self.execute_mutation(
                //         |tr| {
                //             tr.clear(key.clone());
                //             Ok(())
                //         },
                //         is_database,
                //         inst_number,
                //     );
                // }
                // "CLEAR_RANGE" | "CLEAR_RANGE_STARTS_WITH" => {
                //     let exact_range = if op.as_str() == "CLEAR_RANGE_STARTS_WITH" {
                //         self.pop_prefix_range(t)
                //     } else {
                //         // CLEAR_RANGE
                //         self.pop_key_range(t)
                //     };

                //     self.execute_mutation(
                //         |tr| {
                //             tr.clear_range(exact_range.clone());
                //             Ok(())
                //         },
                //         is_database,
                //         inst_number,
                //     );
                // }
                _ => panic!("Unhandled operation {}", op),
            }
        }

        if self.verbose || verbose_inst_range {
            println!("last_version: {:?}", self.last_version);
            println!("tr_map: {:?}", self.tr_map);
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
