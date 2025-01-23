use crate::{
    connection::Conn,
    hrana::{connection::HttpConnection, hyper::HttpSender},
    local::{self, impls::LibsqlStmt},
    params::Params,
    replication::connection::State,
    sync::SyncContext,
    BatchRows, Error, Result, Statement, Transaction, TransactionBehavior,
};
use std::sync::Arc;
use tokio::sync::Mutex;

use super::{statement::SyncedStatement, transaction::SyncedTx};

#[derive(Clone)]
pub struct SyncedConnection {
    pub remote: HttpConnection<HttpSender>,
    pub local: local::Connection,
    pub read_your_writes: bool,
    pub context: Arc<Mutex<SyncContext>>,
    pub state: Arc<Mutex<State>>,
}

impl SyncedConnection {
    async fn should_execute_local(&self, sql: &str) -> Result<bool> {
        let stmts = crate::parser::Statement::parse(sql)
            .collect::<Result<Vec<_>>>()
            .or_else(|err| match err {
                Error::Sqlite3UnsupportedStatement => Ok(vec![]),
                err => Err(err),
            })?;

        let mut state = self.state.lock().await;

        crate::replication::connection::should_execute_local(&mut state, stmts.as_slice())
    }
}

#[async_trait::async_trait]
impl Conn for SyncedConnection {
    async fn execute(&self, sql: &str, params: Params) -> Result<u64> {
        let mut stmt = self.prepare(sql).await?;
        stmt.execute(params).await.map(|v| v as u64)
    }

    async fn execute_batch(&self, sql: &str) -> Result<BatchRows> {
        if self.should_execute_local(sql).await? {
            self.local.execute_batch(sql)
        } else {
            self.remote.execute_batch(sql).await
        }
    }

    async fn execute_transactional_batch(&self, sql: &str) -> Result<BatchRows> {
        if self.should_execute_local(sql).await? {
            self.local.execute_transactional_batch(sql)?;
            Ok(BatchRows::empty())
        } else {
            self.remote.execute_transactional_batch(sql).await
        }
    }

    async fn prepare(&self, sql: &str) -> Result<Statement> {
        if self.should_execute_local(sql).await? {
            Ok(Statement {
                inner: Box::new(LibsqlStmt(self.local.prepare(sql)?)),
            })
        } else {
            let stmt = Statement {
                inner: Box::new(self.remote.prepare(sql)?),
            };

            if self.read_your_writes {
                Ok(Statement {
                    inner: Box::new(SyncedStatement {
                        conn: self.local.clone(),
                        context: self.context.clone(),
                        inner: stmt,
                    }),
                })
            } else {
                Ok(stmt)
            }
        }
    }

    async fn transaction(&self, tx_behavior: TransactionBehavior) -> Result<Transaction> {
        let tx = SyncedTx::begin(self.clone(), tx_behavior).await?;

        Ok(Transaction {
            inner: Box::new(tx),
            conn: crate::Connection {
                conn: Arc::new(self.clone()),
            },
            close: None,
        })
    }

    fn interrupt(&self) -> Result<()> {
        Ok(())
    }

    fn is_autocommit(&self) -> bool {
        self.remote.is_autocommit()
    }

    fn changes(&self) -> u64 {
        self.remote.changes()
    }

    fn total_changes(&self) -> u64 {
        self.remote.total_changes()
    }

    fn last_insert_rowid(&self) -> i64 {
        self.remote.last_insert_rowid()
    }

    async fn reset(&self) {}
}
