use bytes::Bytes;
use fdb::transaction::{MutationType, TransactionContext};
use fdb::tuple::{Tuple, Versionstamp};

use std::env;

fn main() {
    let fdb_cluster_file_path =
        env::var("FDB_CLUSTER_FILE_PATH").expect("FDB_CLUSTER_FILE_PATH not defined!");

    unsafe {
        fdb::select_api_version(fdb_sys::FDB_API_VERSION as i32);
        fdb::start_network();
    }

    let fdb_database = fdb::open_database(fdb_cluster_file_path).unwrap();

    let vs = fdb_database
        .run(|tr| {
            let mut t = Tuple::new();
            t.add_versionstamp(Versionstamp::incomplete(0));
            tr.mutate(
                MutationType::SetVersionstampedKey,
                (t.pack_with_versionstamp(Bytes::from_static(&b"prefix"[..])))
                    .unwrap()
                    .into(),
                Bytes::from_static(&b""[..]),
            );

            let vs_fut = tr.get_versionstamp();

            tr.commit().join()?;

            let vs = vs_fut.join()?;

            Ok(vs)
        })
        .unwrap_or_else(|err| panic!("Error occurred during `run`: {:?}", err));

    println!("10 byte fdb_c level versionstamp: {:?}", vs);

    unsafe {
        fdb::stop_network();
    }
}
