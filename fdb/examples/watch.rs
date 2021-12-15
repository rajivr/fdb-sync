use bytes::Bytes;

use fdb::database::Database;
use fdb::future::{FdbFuture, FdbFutureGet};
use fdb::transaction::{FdbTransaction, TransactionContext};

use std::env;
use std::mem;

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

fn main() {
    let fdb_cluster_file = env::var("FDB_CLUSTER_FILE").expect("FDB_CLUSTER_FILE not defined!");

    unsafe {
        fdb::select_api_version(630);
        fdb::start_network();
    }

    let db = fdb::open_database(fdb_cluster_file).unwrap();

    db.run(None, |t| {
        t.set(
            Bytes::from(&b"k0"[..]).into(),
            Bytes::from(&b"0"[..]).into(),
        );
        Ok(())
    })
    .unwrap_or_else(|err| panic!("run failed {:?}", err));

    let raw_watch_fut = db
        .run(None, |t| {
            // w0 = (grv, k0, 0)
            let w0 = t.watch(Bytes::from(&b"k0"[..]).into());

            // watch is now triggerred (grv, k0, 0) -> (grv, k0, 1)
            t.set(
                Bytes::from(&b"k0"[..]).into(),
                Bytes::from(&b"1"[..]).into(),
            );

            // this won't be printed. you can `join()?` here if you
            // want.
            if w0.is_ready() {
                println!("w0 is ready");
            }

            Ok(FdbFuture::into_raw(w0))
        })
        .unwrap_or_else(|err| panic!("run failed {:?}", err));

    let lifetime_tr = db
        .create_transaction(None)
        .unwrap_or_else(|err| panic!("create_transaction failed {:?}", err));

    let watch_fut = get_bounded_fdb_future(&lifetime_tr, raw_watch_fut);

    if watch_fut.is_ready() {
        // will be printed.
        println!("watch_fut is ready");
    } else {
        println!("watch_fut is not ready");
    }

    drop(db);

    unsafe {
        fdb::stop_network();
    }
}
