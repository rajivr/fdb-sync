use bytes::Bytes;
use fdb::transaction::{ReadTransactionContext, TransactionContext};
use std::env;

fn main() {
    let fdb_cluster_file = env::var("FDB_CLUSTER_FILE").expect("FDB_CLUSTER_FILE not defined!");

    unsafe {
        fdb::select_api_version(fdb::FDB_API_VERSION as i32);
        fdb::start_network();
    }

    let fdb_database = fdb::open_database(fdb_cluster_file).unwrap();

    fdb_database
        .run(|tr| {
            tr.set(Bytes::from("hello").into(), Bytes::from("world").into());
            Ok(())
        })
        .unwrap_or_else(|err| panic!("Error occurred during `run`: {:?}", err));

    let ret = fdb_database
        .read(|tr| tr.get(Bytes::from("hello").into()).join())
        .unwrap_or_else(|err| panic!("Error occured during `read`: {:?}", err))
        .expect("Key not found");

    println!(
        "non-snapshot get: hello, {}",
        String::from_utf8_lossy(&Bytes::from(ret)[..])
    );

    let ret = fdb_database
        .run(|tr| tr.snapshot().get(Bytes::from("hello").into()).join())
        .unwrap_or_else(|err| panic!("Error occured during `run`: {:?}", err))
        .expect("Key not found");

    println!(
        "snapshot get: hello, {}",
        String::from_utf8_lossy(&Bytes::from(ret)[..])
    );

    unsafe {
        fdb::stop_network();
    }
}
