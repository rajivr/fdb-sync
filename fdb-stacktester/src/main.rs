use bytes::Bytes;

use fdb::database::FdbDatabase;
use fdb::range::{Range, RangeOptions};
use fdb::transaction::ReadTransactionContext;
use fdb::tuple::Tuple;
use fdb::{self, KeySelector};

use tokio::runtime::{Handle, Runtime};
use tokio::sync::mpsc::{self, Sender};

use std::env;
use std::error::Error;

// A binding tester `THREAD` would be a Tokio task.
#[derive(Debug)]
struct StackTesterTask {
    spec: String,
    db: FdbDatabase,
    // Key range where the operations are stored by `bindingtester.py`
    ops_range: Range,
    // When all tasks drop `task_finished` then in the root task can
    // shutdown the runtime and prepare to exit the main thread.
    task_finished: Sender<()>,
}

impl StackTesterTask {
    fn new(spec: String, db: FdbDatabase, task_finished: Sender<()>) -> StackTesterTask {
        let ops_range = {
            let mut t = Tuple::new();
            t.add_bytes(spec.clone().into());
            t
        }
        .range(Bytes::new());
        StackTesterTask {
            spec,
            db,
            ops_range,
            task_finished,
        }
    }
}

async fn stack_tester_task(stt: StackTesterTask) -> Result<(), Box<dyn Error + Send + Sync>> {
    let db = stt.db.clone();
    let ops_range = stt.ops_range;

    let kvs = Handle::current()
        .spawn_blocking(move || {
            db.read(|tr| {
                Ok(tr
                    .get_range(
                        KeySelector::first_greater_or_equal(ops_range.begin().clone().into()),
                        KeySelector::first_greater_or_equal(ops_range.end().clone().into()),
                        RangeOptions::default(),
                    )
                    .get()?)
            })
            .unwrap_or_else(|err| panic!("Error occurred during `read`: {:?}", err))
        })
        .await
        .unwrap_or_else(|err| panic!("Error occurred during `spawn_blocking`: {:?}", err));

    for kv in kvs.iter() {
        println!("{:?}", kv);
    }

    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = env::args().collect::<Vec<String>>();

    let main_spec = args[1].clone();
    let api_version = args[2].parse::<i32>()?;
    let cluster_file_path = if args.len() > 3 { args[3].as_str() } else { "" };

    unsafe {
        fdb::select_api_version(api_version);
        fdb::start_network();
    }

    let fdb_database = fdb::open_database(cluster_file_path)?;

    let rt = Runtime::new()?;

    rt.block_on(async {
        let (task_finished, mut task_finished_recv) = mpsc::channel::<()>(1);

        tokio::spawn(stack_tester_task(StackTesterTask::new(
            main_spec,
            fdb_database,
            task_finished.clone(),
        )));

        // We need to drop this so that `.recv()` below can unblock
        // after all the remaining `task_finished` are dropped, which
        // happens when `stack_tester_task` exits.
        drop(task_finished);

        let _ = task_finished_recv.recv().await;
    });

    unsafe {
        fdb::stop_network();
    }

    Ok(())
}
