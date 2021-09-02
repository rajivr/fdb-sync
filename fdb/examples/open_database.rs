use std::env;

fn main() {
    let fdb_cluster_file_path =
        env::var("FDB_CLUSTER_FILE_PATH").expect("FDB_CLUSTER_FILE_PATH not defined!");
    unsafe {
        fdb::select_api_version(fdb_sys::FDB_API_VERSION as i32);
        fdb::start_network();
    }

    let _fdb_database = fdb::open_database(fdb_cluster_file_path).unwrap();

    unsafe {
        fdb::stop_network();
    }
}