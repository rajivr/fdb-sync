use bytes::Bytes;
use fdb::database::FdbDatabase;
use fdb::range::{Range, RangeOptions, StreamingMode};
use fdb::transaction::{ReadTransactionContext, TransactionContext};
use fdb::KeySelector;
use libc;
use parking_lot::Once;
use std::env;

static INIT: Once = Once::new();

fn ensure_network_started() {
    unsafe {
        INIT.call_once(|| {
            fdb::select_api_version(fdb::FDB_API_VERSION as i32);
            fdb::start_network();
            libc::atexit(stop_network);
        });
    }
}

extern "C" fn stop_network() {
    unsafe {
        fdb::stop_network();
    }
}

fn get_database() -> FdbDatabase {
    let fdb_cluster_file = env::var("FDB_CLUSTER_FILE").expect("FDB_CLUSTER_FILE not defined!");
    fdb::open_database(fdb_cluster_file).unwrap()
}

fn clear_database(fdb_database: &FdbDatabase) {
    fdb_database
        .run(|tr| {
            tr.clear_range(Range::new(
                Bytes::from(&b""[..]).into(),
                Bytes::from(&[0xFFu8][..]).into(),
            ));
            Ok(())
        })
        .unwrap_or_else(|err| panic!("Error occurred during `clear_database`: {:?}", err));
}

fn get_streaming_modes() -> Vec<StreamingMode> {
    let mut streaming_modes = Vec::new();

    streaming_modes.push(StreamingMode::WantAll);
    streaming_modes.push(StreamingMode::Iterator);
    streaming_modes.push(StreamingMode::Exact);
    streaming_modes.push(StreamingMode::Small);
    streaming_modes.push(StreamingMode::Medium);
    streaming_modes.push(StreamingMode::Large);
    streaming_modes.push(StreamingMode::Serial);

    streaming_modes
}

#[test]
fn range_scans_work_without_row_limit() {
    ensure_network_started();

    let fdb_database = get_database();

    clear_database(&fdb_database);

    // Set a few key values
    fdb_database
        .run(|tr| {
            (0..10)
                .map(|i| {
                    let k = format!("apple{}", i).to_string();
                    let v = format!("crunchy{}", i).to_string();
                    (k, v)
                })
                .for_each(|(k, v)| {
                    tr.set(Bytes::from(k).into(), Bytes::from(v).into());
                });

            Ok(())
        })
        .unwrap_or_else(|err| panic!("Error occurred during `run`: {:?}", err));

    let mut streaming_modes = get_streaming_modes();

    // We are not providing limit, so we cannot use
    // `StreamingMode::Exact`. We will get error code 2210, if we use
    // `StreamingMode::Exact`.
    streaming_modes.retain(|x| {
        if let StreamingMode::Exact = x {
            false
        } else {
            true
        }
    });

    for streaming_mode in streaming_modes {
        fdb_database
            .read(|tr| {
                let mut range_options = RangeOptions::default();
                range_options.set_mode(streaming_mode);

                let mut i = 0;

                for x in tr.get_range(
                    KeySelector::first_greater_or_equal(Bytes::from_static(b"a").into()),
                    KeySelector::first_greater_or_equal(Bytes::from_static(b"b").into()),
                    range_options,
                ) {
                    let kv = x?;

                    assert_eq!(
                        format!("apple{}", i).to_string(),
                        String::from_utf8_lossy(&Bytes::from(kv.get_key().clone())[..])
                    );
                    assert_eq!(
                        format!("crunchy{}", i).to_string(),
                        String::from_utf8_lossy(&Bytes::from(kv.get_value().clone())[..])
                    );

                    i += 1;
                }

                assert_eq!(i, 10);

                Ok(())
            })
            .unwrap_or_else(|err| panic!("Error occurred during `read`: {:?}", err));
    }

    // iterate over a Vec<KeyValue> using `.get()` method.

    let mut streaming_modes = get_streaming_modes();

    // We are not providing limit, so we cannot use
    // `StreamingMode::Exact`. We will get error code 2210, if we use
    // `StreamingMode::Exact`.
    streaming_modes.retain(|x| {
        if let StreamingMode::Exact = x {
            false
        } else {
            true
        }
    });

    for streaming_mode in streaming_modes {
        fdb_database
            .read(|tr| {
                let mut range_options = RangeOptions::default();
                range_options.set_mode(streaming_mode);

                let mut i = 0;

                for kv in tr
                    .get_range(
                        KeySelector::first_greater_or_equal(Bytes::from_static(b"a").into()),
                        KeySelector::first_greater_or_equal(Bytes::from_static(b"b").into()),
                        range_options,
                    )
                    .get()?
                {
                    assert_eq!(
                        format!("apple{}", i).to_string(),
                        String::from_utf8_lossy(&Bytes::from(kv.get_key().clone())[..])
                    );
                    assert_eq!(
                        format!("crunchy{}", i).to_string(),
                        String::from_utf8_lossy(&Bytes::from(kv.get_value().clone())[..])
                    );

                    i += 1;
                }

                assert_eq!(i, 10);

                Ok(())
            })
            .unwrap_or_else(|err| panic!("Error occurred during `read`: {:?}", err));
    }
}

#[test]
fn range_scans_work_with_row_limit() {
    ensure_network_started();

    let fdb_database = get_database();

    clear_database(&fdb_database);

    // Set a few key values
    fdb_database
        .run(|tr| {
            (0..10)
                .map(|i| {
                    let k = format!("apple{}", i).to_string();
                    let v = format!("crunchy{}", i).to_string();
                    (k, v)
                })
                .for_each(|(k, v)| {
                    tr.set(Bytes::from(k).into(), Bytes::from(v).into());
                });

            Ok(())
        })
        .unwrap_or_else(|err| panic!("Error occurred during `run`: {:?}", err));

    let streaming_modes = get_streaming_modes();

    for streaming_mode in streaming_modes {
        fdb_database
            .read(|tr| {
                let mut range_options = RangeOptions::default();
                range_options.set_mode(streaming_mode);
                range_options.set_limit(3);

                let mut i = 0;

                for x in tr.get_range(
                    KeySelector::first_greater_or_equal(Bytes::from_static(b"a").into()),
                    KeySelector::first_greater_or_equal(Bytes::from_static(b"b").into()),
                    range_options,
                ) {
                    let kv = x?;

                    assert_eq!(
                        format!("apple{}", i).to_string(),
                        String::from_utf8_lossy(&Bytes::from(kv.get_key().clone())[..])
                    );
                    assert_eq!(
                        format!("crunchy{}", i).to_string(),
                        String::from_utf8_lossy(&Bytes::from(kv.get_value().clone())[..])
                    );

                    i += 1;
                }

                assert_eq!(i, 3);

                Ok(())
            })
            .unwrap_or_else(|err| panic!("Error occurred during `read`: {:?}", err));
    }

    // iterate over a Vec<KeyValue> using `.get()` method.

    let streaming_modes = get_streaming_modes();

    for streaming_mode in streaming_modes {
        fdb_database
            .read(|tr| {
                let mut range_options = RangeOptions::default();
                range_options.set_mode(streaming_mode);
                range_options.set_limit(3);

                let mut i = 0;

                for kv in tr
                    .get_range(
                        KeySelector::first_greater_or_equal(Bytes::from_static(b"a").into()),
                        KeySelector::first_greater_or_equal(Bytes::from_static(b"b").into()),
                        range_options,
                    )
                    .get()?
                {
                    assert_eq!(
                        format!("apple{}", i).to_string(),
                        String::from_utf8_lossy(&Bytes::from(kv.get_key().clone())[..])
                    );
                    assert_eq!(
                        format!("crunchy{}", i).to_string(),
                        String::from_utf8_lossy(&Bytes::from(kv.get_value().clone())[..])
                    );

                    i += 1;
                }

                assert_eq!(i, 3);

                Ok(())
            })
            .unwrap_or_else(|err| panic!("Error occurred during `read`: {:?}", err));
    }
}

#[test]
fn range_scans_work_without_row_limit_reversed() {
    ensure_network_started();

    let fdb_database = get_database();

    clear_database(&fdb_database);

    // Set a few key values
    fdb_database
        .run(|tr| {
            (0..10)
                .map(|i| {
                    let k = format!("apple{}", i).to_string();
                    let v = format!("crunchy{}", i).to_string();
                    (k, v)
                })
                .for_each(|(k, v)| {
                    tr.set(Bytes::from(k).into(), Bytes::from(v).into());
                });

            Ok(())
        })
        .unwrap_or_else(|err| panic!("Error occurred during `run`: {:?}", err));

    let mut streaming_modes = get_streaming_modes();

    // We are not providing limit, so we cannot use
    // `StreamingMode::Exact`. We will get error code 2210, if we use
    // `StreamingMode::Exact`.
    streaming_modes.retain(|x| {
        if let StreamingMode::Exact = x {
            false
        } else {
            true
        }
    });

    for streaming_mode in streaming_modes {
        fdb_database
            .read(|tr| {
                let mut range_options = RangeOptions::default();
                range_options.set_mode(streaming_mode);
                range_options.set_reverse(true);

                let mut i = 9;

                for x in tr.get_range(
                    KeySelector::first_greater_or_equal(Bytes::from_static(b"a").into()),
                    KeySelector::first_greater_or_equal(Bytes::from_static(b"b").into()),
                    range_options,
                ) {
                    let kv = x?;

                    assert_eq!(
                        format!("apple{}", i).to_string(),
                        String::from_utf8_lossy(&Bytes::from(kv.get_key().clone())[..])
                    );
                    assert_eq!(
                        format!("crunchy{}", i).to_string(),
                        String::from_utf8_lossy(&Bytes::from(kv.get_value().clone())[..])
                    );

                    i -= 1;
                }

                assert_eq!(i, -1);

                Ok(())
            })
            .unwrap_or_else(|err| panic!("Error occurred during `read`: {:?}", err));
    }

    // iterate over a Vec<KeyValue> using `.get()` method.

    let mut streaming_modes = get_streaming_modes();

    // We are not providing limit, so we cannot use
    // `StreamingMode::Exact`. We will get error code 2210, if we use
    // `StreamingMode::Exact`.
    streaming_modes.retain(|x| {
        if let StreamingMode::Exact = x {
            false
        } else {
            true
        }
    });

    for streaming_mode in streaming_modes {
        fdb_database
            .read(|tr| {
                let mut range_options = RangeOptions::default();
                range_options.set_mode(streaming_mode);
                range_options.set_reverse(true);

                let mut i = 9;

                for kv in tr
                    .get_range(
                        KeySelector::first_greater_or_equal(Bytes::from_static(b"a").into()),
                        KeySelector::first_greater_or_equal(Bytes::from_static(b"b").into()),
                        range_options,
                    )
                    .get()?
                {
                    assert_eq!(
                        format!("apple{}", i).to_string(),
                        String::from_utf8_lossy(&Bytes::from(kv.get_key().clone())[..])
                    );
                    assert_eq!(
                        format!("crunchy{}", i).to_string(),
                        String::from_utf8_lossy(&Bytes::from(kv.get_value().clone())[..])
                    );

                    i -= 1;
                }

                assert_eq!(i, -1);

                Ok(())
            })
            .unwrap_or_else(|err| panic!("Error occurred during `read`: {:?}", err));
    }
}

#[test]
fn range_scans_work_with_row_limit_reversed() {
    ensure_network_started();

    let fdb_database = get_database();

    clear_database(&fdb_database);

    // Set a few key values
    fdb_database
        .run(|tr| {
            (0..10)
                .map(|i| {
                    let k = format!("apple{}", i).to_string();
                    let v = format!("crunchy{}", i).to_string();
                    (k, v)
                })
                .for_each(|(k, v)| {
                    tr.set(Bytes::from(k).into(), Bytes::from(v).into());
                });

            Ok(())
        })
        .unwrap_or_else(|err| panic!("Error occurred during `run`: {:?}", err));

    let streaming_modes = get_streaming_modes();

    for streaming_mode in streaming_modes {
        fdb_database
            .read(|tr| {
                let mut range_options = RangeOptions::default();
                range_options.set_mode(streaming_mode);
                range_options.set_limit(3);
                range_options.set_reverse(true);

                let mut i = 9;

                for x in tr.get_range(
                    KeySelector::first_greater_or_equal(Bytes::from_static(b"a").into()),
                    KeySelector::first_greater_or_equal(Bytes::from_static(b"b").into()),
                    range_options,
                ) {
                    let kv = x?;

                    assert_eq!(
                        format!("apple{}", i).to_string(),
                        String::from_utf8_lossy(&Bytes::from(kv.get_key().clone())[..])
                    );
                    assert_eq!(
                        format!("crunchy{}", i).to_string(),
                        String::from_utf8_lossy(&Bytes::from(kv.get_value().clone())[..])
                    );

                    i -= 1;
                }

                assert_eq!(i, 6);

                Ok(())
            })
            .unwrap_or_else(|err| panic!("Error occurred during `read`: {:?}", err));
    }

    // iterate over a Vec<KeyValue> using `.get()` method.

    let streaming_modes = get_streaming_modes();

    for streaming_mode in streaming_modes {
        fdb_database
            .read(|tr| {
                let mut range_options = RangeOptions::default();
                range_options.set_mode(streaming_mode);
                range_options.set_limit(3);
                range_options.set_reverse(true);

                let mut i = 9;

                for kv in tr
                    .get_range(
                        KeySelector::first_greater_or_equal(Bytes::from_static(b"a").into()),
                        KeySelector::first_greater_or_equal(Bytes::from_static(b"b").into()),
                        range_options,
                    )
                    .get()?
                {
                    assert_eq!(
                        format!("apple{}", i).to_string(),
                        String::from_utf8_lossy(&Bytes::from(kv.get_key().clone())[..])
                    );
                    assert_eq!(
                        format!("crunchy{}", i).to_string(),
                        String::from_utf8_lossy(&Bytes::from(kv.get_value().clone())[..])
                    );

                    i -= 1;
                }

                assert_eq!(i, 6);

                Ok(())
            })
            .unwrap_or_else(|err| panic!("Error occurred during `read`: {:?}", err));
    }
}
