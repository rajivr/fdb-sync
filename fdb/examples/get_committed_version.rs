use bytes::Bytes;
use fdb::transaction::Transaction;

use std::env;

fn main() {
    let fdb_cluster_file = env::var("FDB_CLUSTER_FILE").expect("FDB_CLUSTER_FILE not defined!");

    unsafe {
        fdb::select_api_version(fdb::FDB_API_VERSION as i32);
        fdb::start_network();
    }

    let fdb_database = fdb::open_database(fdb_cluster_file).unwrap();

    // Ensure that the cloned `fdb_database` is dropped we exit the
    // block.
    {
        let (_, tr) = fdb_database
            .clone()
            .run_and_get_transaction(None, |tr| {
                tr.set(Bytes::from("hello").into(), Bytes::from("world").into());
                Ok(())
            })
            .unwrap_or_else(|err| {
                panic!("Error occurred during `run_and_get_transaction`: {:?}", err)
            });

        println!("get_commited_version: {:?}", unsafe {
            tr.get_committed_version()
        });
    }

    drop(fdb_database);

    unsafe {
        fdb::stop_network();
    }
}
