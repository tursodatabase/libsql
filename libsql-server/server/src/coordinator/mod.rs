pub mod query;
pub mod scheduler;
pub mod statements;

use std::time::{Duration, Instant};

use anyhow::Result;
use crossbeam::channel::Sender;
use futures::stream::FuturesUnordered;
use rusqlite::types::Value;
use tokio::task::JoinHandle;

use crate::coordinator::query::{ErrorCode, QueryError, QueryResponse, QueryResult};
use crate::coordinator::scheduler::UpdateStateMessage;
use crate::coordinator::statements::{State, Statements};
use crate::job::Job;
use crate::wal::WalConnection;

const TXN_TIMEOUT_SECS: usize = 5;

/// Transaction coordinator.
pub struct Coordinator {
    worker_handles: FuturesUnordered<JoinHandle<()>>,
}

impl Coordinator {
    /// Create a new coordinator that will spawn `ncpu` threads.
    /// Each worker maintains a connections to the database, and process jobs sequentially.
    /// `conn_builder` must create a fresh db_connection each time it is called.
    /// If ncpu is 0, then the number of worker is determined automatically.
    pub fn new(
        mut ncpu: usize,
        conn_builder: impl Fn() -> WalConnection + Sync + Send,
    ) -> Result<(Self, Sender<Job>)> {
        if ncpu == 0 {
            ncpu = std::thread::available_parallelism()?.get();
        }
        let (fifo, receiver) = crossbeam::channel::unbounded();

        let worker_handles = FuturesUnordered::new();
        for id in 0..ncpu {
            let db_conn = conn_builder();
            let global_fifo = receiver.clone();
            worker_handles.push(tokio::task::spawn_blocking(move || {
                let worker = Worker {
                    global_fifo,
                    db_conn,
                    id,
                };

                worker.run();
            }));
        }

        let this = Self { worker_handles };
        Ok((this, fifo))
    }

    /// waits for all workers to finish their work and exit.
    pub async fn join(self) {
        for h in self.worker_handles {
            if let Err(e) = h.await {
                tracing::error!("{}", e);
            }
        }
    }
}

impl std::convert::From<rusqlite::Error> for QueryError {
    fn from(err: rusqlite::Error) -> Self {
        QueryError::new(ErrorCode::SQLError, err)
    }
}

struct Worker {
    global_fifo: crossbeam::channel::Receiver<Job>,
    db_conn: WalConnection,
    id: usize,
}

impl Worker {
    fn perform_oneshot(&self, stmts: &Statements) -> QueryResult {
        let mut result = vec![];
        let mut prepared = self.db_conn.prepare(&stmts.stmts)?;
        let columns: Vec<(String, Option<String>)> = prepared
            .columns()
            .iter()
            .map(|col| (col.name().into(), col.decl_type().map(str::to_lowercase)))
            .collect();
        let mut rows = prepared.query([])?;
        while let Some(row) = rows.next()? {
            let mut row_ = vec![];
            for (i, _) in columns.iter().enumerate() {
                row_.push(row.get::<usize, Value>(i)?);
            }
            result.push(row_);
        }
        Ok(QueryResponse::ResultSet(columns, result))
    }

    fn handle_transaction(&self, job: Job) {
        let (sender, receiver) = crossbeam::channel::unbounded();
        job.scheduler_sender
            .send(UpdateStateMessage::TxnBegin(job.client_id, sender))
            .unwrap();
        let mut stmts = job.statements;

        let txn_timeout = Instant::now() + Duration::from_secs(TXN_TIMEOUT_SECS as _);

        let mut responder = job.responder;
        loop {
            let message = self.perform_oneshot(&stmts);
            let is_err = message.is_err();

            let _ = responder.send(message);

            match stmts.state(State::TxnOpened) {
                State::TxnClosed if !is_err => {
                    // the transaction was closed successfully
                    job.scheduler_sender
                        .send(UpdateStateMessage::TxnEnded(job.client_id))
                        .unwrap();
                    break;
                }
                _ => {
                    // Let the database handle any other state
                    job.scheduler_sender
                        .send(UpdateStateMessage::Ready(job.client_id))
                        .unwrap();
                    match receiver.recv_timeout(txn_timeout - Instant::now()) {
                        Ok(job) => {
                            stmts = job.statements;
                            responder = job.responder;
                        }
                        Err(_) => {
                            tracing::warn!("rolling back transaction!");
                            let _ = self.db_conn.execute("ROLLBACK TRANSACTION;", ());
                            // FIXME: potential data race with Ready issued before.
                            job.scheduler_sender
                                .send(UpdateStateMessage::TxnTimeout(job.client_id))
                                .unwrap();
                            break;
                        }
                    }
                }
            }
        }
    }

    fn run(self) {
        while let Ok(job) = self.global_fifo.recv() {
            tracing::debug!("executing job `{:?}` on worker {}", job.statements, self.id);

            // This is an interactive transaction.
            if let State::TxnOpened = job.statements.state(State::Start) {
                self.handle_transaction(job)
            } else {
                // Any other state falls in this branch, even invalid: we let sqlite deal with the
                // error handling.
                let m = self.perform_oneshot(&job.statements);
                let _ = job.responder.send(m);
                job.scheduler_sender
                    .send(UpdateStateMessage::Ready(job.client_id))
                    .unwrap();
            }

            tracing::debug!("job finished on worker {}", self.id);
        }
    }
}
