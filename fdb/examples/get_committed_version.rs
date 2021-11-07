use bytes::Bytes;
use fdb::transaction::{FdbTransaction, TransactionContext};

use std::env;

fn main() {
    let fdb_cluster_file_path =
        env::var("FDB_CLUSTER_FILE_PATH").expect("FDB_CLUSTER_FILE_PATH not defined!");

    unsafe {
        fdb::select_api_version(fdb_sys::FDB_API_VERSION as i32);
        fdb::start_network();
    }

    let fdb_database = fdb::open_database(fdb_cluster_file_path).unwrap();

    let (_, tr) = fdb_database
        .run_and_get_transaction::<_, _, FdbTransaction>(|tr| {
            tr.set(Bytes::from("hello").into(), Bytes::from("world").into());
            Ok(())
        })
        .unwrap_or_else(|err| panic!("Error occurred during `run_and_get_transaction`: {:?}", err));

    println!("get_commited_version: {:?}", unsafe {
        tr.get_committed_version()
    });

    unsafe {
        fdb::stop_network();
    }
}
