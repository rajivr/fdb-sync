use bytes::Bytes;
use fdb::range::{Range, RangeOptions};
use fdb::transaction::{ReadTransactionContext, TransactionContext};
use fdb::KeySelector;
use std::env;

fn main() {
    let fdb_cluster_file = env::var("FDB_CLUSTER_FILE").expect("FDB_CLUSTER_FILE not defined!");

    unsafe {
        fdb::select_api_version(fdb::FDB_API_VERSION as i32);
        fdb::start_network();
    }

    let fdb_database = fdb::open_database(fdb_cluster_file).unwrap();

    // Clear the database.
    fdb_database
        .run(None, |tr| {
            tr.clear_range(Range::new(
                Bytes::from(&b""[..]).into(),
                Bytes::from(&[0xFFu8][..]).into(),
            ));
            Ok(())
        })
        .unwrap_or_else(|err| panic!("Error occurred during `run`: {:?}", err));

    // Set a few key values.
    fdb_database
        .run(None, |tr| {
            tr.set(Bytes::from("apple").into(), Bytes::from("foo").into());
            tr.set(Bytes::from("cherry").into(), Bytes::from("baz").into());
            tr.set(Bytes::from("banana").into(), Bytes::from("bar").into());
            Ok(())
        })
        .unwrap_or_else(|err| panic!("Error occurred during `run`: {:?}", err));

    println!("read using Iterator trait");

    fdb_database
        .read(None, |tr| {
            for x in tr.get_range(
                KeySelector::first_greater_or_equal(Bytes::from(&b""[..]).into()),
                KeySelector::first_greater_or_equal(Bytes::from(&[0xFFu8][..]).into()),
                RangeOptions::default(),
            ) {
                let kv = x?;
                println!(
                    "{} is {}",
                    String::from_utf8_lossy(&Bytes::from(kv.get_key().clone())[..]),
                    String::from_utf8_lossy(&Bytes::from(kv.get_value().clone())[..])
                );
            }

            Ok(())
        })
        .unwrap_or_else(|err| panic!("Error occurred during `read`: {:?}", err));

    println!("");
    println!("read using RangeResult::get");

    fdb_database
        .read(None, |tr| {
            // iterating on a Vec<KeyValue>
            for kv in tr
                .get_range(
                    KeySelector::first_greater_or_equal(Bytes::from(&b""[..]).into()),
                    KeySelector::first_greater_or_equal(Bytes::from(&[0xFFu8][..]).into()),
                    RangeOptions::default(),
                )
                .get()?
            {
                println!(
                    "{} is {}",
                    String::from_utf8_lossy(&Bytes::from(kv.get_key().clone())[..]),
                    String::from_utf8_lossy(&Bytes::from(kv.get_value().clone())[..])
                );
            }

            Ok(())
        })
        .unwrap_or_else(|err| panic!("Error occurred during `read`: {:?}", err));

    drop(fdb_database);

    unsafe {
        fdb::stop_network();
    }
}
