use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

use anyhow::anyhow;
use sqld_libsql_bindings::wal_hook::WalHook;
use tokio::sync::{mpsc, oneshot};

use super::super::libsql::open_db;

type OpMsg = Box<dyn FnOnce(&rusqlite::Connection) + 'static + Send + Sync>;

pub struct DumpLoader {
    sender: mpsc::Sender<OpMsg>,
}

impl DumpLoader {
    pub async fn new(
        path: PathBuf,
        hooks: impl WalHook + Clone + Send + 'static,
    ) -> anyhow::Result<Self> {
        let (sender, mut receiver) = mpsc::channel::<OpMsg>(1);

        let (ok_snd, ok_rcv) = oneshot::channel::<anyhow::Result<()>>();
        tokio::task::spawn_blocking(move || {
            let db = match open_db(&path, hooks, false) {
                Ok(db) => {
                    let _ = ok_snd.send(Ok(()));
                    db
                }
                Err(e) => {
                    let _ = ok_snd.send(Err(e));
                    return;
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
    let mut line = String::new();
    let mut skipped_wasm_table = false;
    while let Ok(n) = f.read_line(&mut line) {
        if n == 0 {
            break;
        }

        // This is a hack to ignore the libsql_wasm_func_table table because it is already created
        // by the system.
        if !skipped_wasm_table && line.trim() == WASM_TABLE_CREATE {
            skipped_wasm_table = true;
            line.clear();
            continue;
        }

        conn.execute(&line, ())?;
        line.clear();
    }

    Ok(())
}
