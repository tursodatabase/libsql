use std::path::Path;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crossbeam::channel::RecvTimeoutError;
use rusqlite::types::Value;
use rusqlite::OpenFlags;
use tokio::sync::oneshot;
use tracing::warn;

use crate::libsql::wal_hook::WalHook;
use crate::libsql::WalMethods;
use crate::query::{Column, ErrorCode, QueryError, QueryResponse, QueryResult, ResultSet, Row};
use crate::query_analysis::{State, Statements};

use super::{Database, TXN_TIMEOUT_SECS};

#[derive(Clone)]
pub struct LibSqlDb {
    sender: crossbeam::channel::Sender<(Statements, oneshot::Sender<QueryResult>)>,
}

fn execute_query(conn: &rusqlite::Connection, stmts: &Statements) -> QueryResult {
    let mut rows = vec![];
    let mut prepared = conn.prepare(&stmts.stmts)?;
    let columns = prepared
        .columns()
        .iter()
        .map(|col| Column {
            name: col.name().into(),
            ty: col
                .decl_type()
                .map(FromStr::from_str)
                .transpose()
                .ok()
                .flatten(),
        })
        .collect::<Vec<_>>();
    let mut qresult = prepared.query([])?;
    while let Some(row) = qresult.next()? {
        let mut values = vec![];
        for (i, _) in columns.iter().enumerate() {
            values.push(row.get::<usize, Value>(i)?.into());
        }
        rows.push(Row { values });
    }
    Ok(QueryResponse::ResultSet(ResultSet { columns, rows }))
}

fn rollback(conn: &rusqlite::Connection) {
    conn.execute("rollback transaction;", ())
        .expect("failed to rollback");
}

macro_rules! ok_or_exit {
    ($e:expr) => {
        if let Err(_) = $e {
            return;
        }
    };
}

impl LibSqlDb {
    pub fn new(
        path: impl AsRef<Path> + Send + 'static,
        vwal_methods: Option<Arc<Mutex<WalMethods>>>,
        wal_hook: impl WalHook + Send + Clone + 'static,
    ) -> anyhow::Result<Self> {
        let (sender, receiver) =
            crossbeam::channel::unbounded::<(Statements, oneshot::Sender<QueryResult>)>();

        tokio::task::spawn_blocking(move || {
            let mut retries = 0;
            let conn = loop {
                let conn_result = match vwal_methods {
                    #[cfg(feature = "fdb")]
                    Some(ref vwal_methods) => crate::libsql::fdb::open_with_virtual_wal(
                        &path,
                        OpenFlags::SQLITE_OPEN_READ_WRITE
                            | OpenFlags::SQLITE_OPEN_CREATE
                            | OpenFlags::SQLITE_OPEN_URI
                            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
                        vwal_methods.clone(),
                    ),
                    #[cfg(not(feature = "fdb"))]
                    Some(_) => panic!("not compiled with fdb support"),
                    None => crate::libsql::open_with_regular_wal(
                        &path,
                        OpenFlags::SQLITE_OPEN_READ_WRITE
                            | OpenFlags::SQLITE_OPEN_CREATE
                            | OpenFlags::SQLITE_OPEN_URI
                            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
                        wal_hook.clone(),
                    ),
                };
                match conn_result {
                    Ok(conn) => break conn,
                    Err(e) => {
                        match e.downcast::<rusqlite::Error>() {
                            // > When the last connection to a particular database is closing, that
                            // > connection will acquire an exclusive lock for a short time while it cleans
                            // > up the WAL and shared-memory files. If a second database tries to open and
                            // > query the database while the first connection is still in the middle of its
                            // > cleanup process, the second connection might get an SQLITE_BUSY error.
                            //
                            // For this reason we may not be able to open the database right away, so we
                            // retry a couple of times before giving up.
                            Ok(rusqlite::Error::SqliteFailure(e, _))
                                if e.code == rusqlite::ffi::ErrorCode::DatabaseBusy
                                    && retries < 10 =>
                            {
                                std::thread::sleep(Duration::from_millis(10));
                                retries += 1;
                            }
                            Ok(e) => panic!("Unhandled error opening libsql: {}", e),
                            Err(e) => panic!("Unhandled error opening libsql: {}", e),
                        }
                    }
                }
            };

            let mut state = State::Start;
            let mut timeout_deadline = None;
            let mut timedout = false;
            loop {
                let (stmts, sender) = match timeout_deadline {
                    Some(deadline) => match receiver.recv_deadline(deadline) {
                        Ok(msg) => msg,
                        Err(RecvTimeoutError::Timeout) => {
                            warn!("transaction timed out");
                            rollback(&conn);
                            timeout_deadline = None;
                            timedout = true;
                            state = State::Start;
                            continue;
                        }
                        Err(RecvTimeoutError::Disconnected) => break,
                    },
                    None => match receiver.recv() {
                        Ok(msg) => msg,
                        Err(_) => break,
                    },
                };

                if !timedout {
                    let result = execute_query(&conn, &stmts);
                    match stmts.state(state) {
                        State::TxnOpened => {
                            timeout_deadline =
                                Some(Instant::now() + Duration::from_secs(TXN_TIMEOUT_SECS));
                            state = State::TxnOpened;
                        }
                        State::TxnClosed => {
                            if result.is_ok() {
                                state = State::Start;
                                timeout_deadline = None;
                            }
                        }
                        State::Start => (),
                        State::Invalid => panic!("invalid state!"),
                    }

                    ok_or_exit!(sender.send(result));
                } else {
                    ok_or_exit!(sender.send(Err(QueryError::new(
                        ErrorCode::TxTimeout,
                        "transaction timedout",
                    ))));
                    timedout = false;
                }
            }
        });

        Ok(Self { sender })
    }
}

#[async_trait::async_trait]
impl Database for LibSqlDb {
    async fn execute(&self, query: Statements) -> QueryResult {
        let (sender, receiver) = oneshot::channel();
        let _ = self.sender.send((query, sender));
        receiver
            .await
            .map_err(|e| QueryError::new(ErrorCode::Internal, e))?
    }
}
