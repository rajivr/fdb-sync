use bytes::Bytes;
use fdb::range::RangeOptions;
use fdb::subspace::Subspace;
use fdb::transaction::{MutationType, TransactionContext};
use fdb::tuple::{Tuple, Versionstamp};
use fdb::KeySelector;

use std::env;

fn main() {
    let fdb_cluster_file = env::var("FDB_CLUSTER_FILE").expect("FDB_CLUSTER_FILE not defined!");

    unsafe {
        fdb::select_api_version(fdb::FDB_API_VERSION as i32);
        fdb::start_network();
    }

    let fdb_database = fdb::open_database(fdb_cluster_file).unwrap();

    let (_, tr_version) = fdb_database
        .run_and_get_versionstamp(None, |tr| {
            let mut t = Tuple::new();
            t.add_string("prefix".to_string());
            t.add_versionstamp(Versionstamp::incomplete(0));
            unsafe {
                tr.mutate(
                    MutationType::SetVersionstampedKey,
                    t.pack_with_versionstamp(Bytes::new())?.into(),
                    Bytes::new(),
                );
            }

            Ok(())
        })
        .unwrap_or_else(|err| {
            panic!(
                "Error occurred during `run_and_get_versionstamp`: {:?}",
                err
            )
        });

    let vs = fdb_database
        .run(None, |tr| {
            let subspace = Subspace::new(Bytes::new()).subspace(&{
                let mut t = Tuple::new();
                t.add_string("prefix".to_string());
                t
            });

            let subspace_range = subspace.range(&Tuple::new());

            let key = tr
                .get_range(
                    KeySelector::first_greater_or_equal(subspace_range.begin().clone().into()),
                    KeySelector::first_greater_or_equal(subspace_range.end().clone().into()),
                    RangeOptions::default(),
                )
                .into_iter()
                .next()
                .unwrap()?
                .get_key()
                .clone()
                .into();

            Ok(subspace.unpack(&key)?.get_versionstamp_ref(0)?.clone())
        })
        .unwrap_or_else(|err| panic!("Error occurred during `run`: {:?}", err));

    assert_eq!(vs, Versionstamp::complete(tr_version, 0));

    drop(fdb_database);

    unsafe {
        fdb::stop_network();
    }
}
