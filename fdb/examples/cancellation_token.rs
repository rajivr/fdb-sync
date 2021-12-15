use bytes::Bytes;

use fdb::range::Range;
use fdb::transaction::TransactionContext;

use tokio::runtime::{Handle, Runtime};
use tokio::sync::mpsc;

use tokio_util::sync::CancellationToken;

use std::env;
use std::error::Error;
use std::thread;
use std::time::Duration;

#[derive(Clone, Debug)]
enum Message {
    Starting1SecSleep,
}

fn main() -> Result<(), Box<dyn Error>> {
    let fdb_cluster_file = env::var("FDB_CLUSTER_FILE").expect("FDB_CLUSTER_FILE not defined!");

    unsafe {
        fdb::select_api_version(fdb::FDB_API_VERSION as i32);
        fdb::start_network();
    }

    let fdb_database = fdb::open_database(fdb_cluster_file)?;

    // Clear the database.
    fdb_database.run(None, |tr| {
        tr.clear_range(Range::new(
            Bytes::new().into(),
            Bytes::from(&[0xFFu8][..]).into(),
        ));
        Ok(())
    })?;

    // Set a few key values.
    fdb_database
        .run(None, |tr| {
            tr.set(Bytes::from("apple").into(), Bytes::from("foo").into());
            tr.set(Bytes::from("cherry").into(), Bytes::from("baz").into());
            tr.set(Bytes::from("banana").into(), Bytes::from("bar").into());
            Ok(())
        })
        .unwrap_or_else(|err| panic!("Error occurred during `run`: {:?}", err));

    let rt = Runtime::new()?;

    let cloned_fdb_database = fdb_database.clone();

    rt.block_on(async {
        let fdb_database = cloned_fdb_database;

        let (msg_sender, mut msg_recv) = mpsc::channel::<Message>(1);

        let cancellation_token = CancellationToken::new();

        let cloned_cancellation_token = cancellation_token.clone();

        let thread_handle = Handle::current().spawn_blocking(move || {
            let cancellation_token = cloned_cancellation_token;

            fdb_database
                .run(Some(cancellation_token), |tr| {
                    println!(
                        "get: key: apple, value: {}",
                        String::from_utf8_lossy(
                            &Bytes::from(
                                tr.get(Bytes::from("apple").into())
                                    .join()?
                                    .unwrap_or(Bytes::new().into()),
                            )[..],
                        )
                    );

                    msg_sender
                        .blocking_send(Message::Starting1SecSleep)
                        .unwrap_or_else(|err| panic!("Error while sending message. {:?}", err));
                    thread::sleep(Duration::from_secs(1));

                    // By this point, the transaction should have been
                    // cancelled by the main task. So, the following
                    // two `get` *should* fail with FDB Error 1025
                    // (transaction_cancelled).

                    println!(
                        "get: key: cherry, value: {}",
                        String::from_utf8_lossy(
                            &Bytes::from(
                                tr.get(Bytes::from("cherry").into())
                                    .join()?
                                    .unwrap_or(Bytes::new().into()),
                            )[..],
                        )
                    );

                    println!(
                        "get: key: banana, value: {}",
                        String::from_utf8_lossy(
                            &Bytes::from(
                                tr.get(Bytes::from("banana").into())
                                    .join()?
                                    .unwrap_or(Bytes::new().into()),
                            )[..],
                        )
                    );

                    Ok(())
                })
                .unwrap_or_else(|err| println!("Error occurred during `run`: {:?}", err));
        });

        if let Some(Message::Starting1SecSleep) = msg_recv.recv().await {
            cancellation_token.cancel();
        } else {
            panic!("recv error.");
        }

        thread_handle.await?;

        Result::<(), Box<dyn Error>>::Ok(())
    })?;

    drop(fdb_database);

    unsafe {
        fdb::stop_network();
    }

    Ok(())
}
