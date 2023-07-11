use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use rusqlite::ErrorCode;
use tokio::sync::{mpsc, oneshot};

use crate::database::libsql::open_db;
use crate::replication::primary::logger::{ReplicationLoggerHookCtx, REPLICATION_METHODS};
use crate::replication::ReplicationLogger;

type OpMsg = Box<dyn FnOnce(&rusqlite::Connection) + 'static + Send + Sync>;

#[derive(Debug)]
pub struct DumpLoader {
    sender: mpsc::Sender<OpMsg>,
}

impl DumpLoader {
    pub async fn new(
        path: PathBuf,
        logger: Arc<ReplicationLogger>,
        bottomless_replicator: Option<Arc<std::sync::Mutex<bottomless::replicator::Replicator>>>,
    ) -> anyhow::Result<Self> {
        let (sender, mut receiver) = mpsc::channel::<OpMsg>(1);

        let (ok_snd, ok_rcv) = oneshot::channel::<anyhow::Result<()>>();
        tokio::task::spawn_blocking(move || {
            let mut ctx = ReplicationLoggerHookCtx::new(logger, bottomless_replicator);
            let mut retries = 0;
            let db = loop {
                match open_db(&path, &REPLICATION_METHODS, &mut ctx, None) {
                    Ok(db) => {
                        if ok_snd.send(Ok(())).is_ok() {
                            break db;
                        } else {
                            return;
                        }
                    }
                    // Creating the loader database can, in rare occurences, return sqlite busy,
                    // because of a race condition opening the monitor thread db. This is there to
                    // retry a bunch of times if that happens.
                    Err(rusqlite::Error::SqliteFailure(
                        rusqlite::ffi::Error {
                            code: ErrorCode::DatabaseBusy,
                            ..
                        },
                        _,
                    )) if retries < 10 => {
                        retries += 1;
                        std::thread::sleep(Duration::from_millis(100));
                    }
                    Err(e) => {
                        let _ = ok_snd.send(Err(e.into()));
                        return;
                    }
                }
            };

            while let Some(f) = receiver.blocking_recv() {
                f(&db);
            }
        });

        ok_rcv.await??;

        Ok(Self { sender })
    }

    /// Attempts to load the dump at `path` into the database.
    pub async fn load_dump(&self, path: PathBuf) -> anyhow::Result<()> {
        tracing::info!("loading dump at `{}`", path.display());
        let (snd, ret) = oneshot::channel();
        self.sender
            .send(Box::new(move |conn| {
                let ret = perform_load_dump(conn, path);
                let _ = snd.send(ret);
            }))
            .await
            .map_err(|_| anyhow!("dump loader channel closed"))?;

        ret.await??;

        tracing::info!("dump loaded sucessfully");

        Ok(())
    }
}

const WASM_TABLE_CREATE: &str =
    "CREATE TABLE libsql_wasm_func_table (name text PRIMARY KEY, body text) WITHOUT ROWID;";

fn perform_load_dump(conn: &rusqlite::Connection, path: PathBuf) -> anyhow::Result<()> {
    let mut f = BufReader::new(File::open(path)?);
    let mut curr = String::new();
    let mut line = String::new();
    let mut skipped_wasm_table = false;
    while let Ok(n) = f.read_line(&mut curr) {
        if n == 0 {
            break;
        }
        let frag = curr.trim();

        if frag.is_empty() || frag.starts_with("--") {
            curr.clear();
            continue;
        }

        line.push_str(frag);
        curr.clear();

        // This is a hack to ignore the libsql_wasm_func_table table because it is already created
        // by the system.
        if !skipped_wasm_table && line == WASM_TABLE_CREATE {
            skipped_wasm_table = true;
            line.clear();
            continue;
        }

        if line.ends_with(';') {
            conn.execute(&line, ())?;
            line.clear();
        } else {
            line.push(' ');
        }
    }

    Ok(())
}
