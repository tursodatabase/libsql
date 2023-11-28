use std::sync::Arc;

use crate::params::{IntoParams, Params};
use crate::rows::Rows;
use crate::statement::Statement;
use crate::transaction::Transaction;
use crate::{Result, TransactionBehavior};

#[async_trait::async_trait]
pub(crate) trait Conn {
    async fn execute(&self, sql: &str, params: Params) -> Result<u64>;

    async fn execute_batch(&self, sql: &str) -> Result<()>;

    async fn prepare(&self, sql: &str) -> Result<Statement>;

    async fn transaction(&self, tx_behavior: TransactionBehavior) -> Result<Transaction>;

    async fn is_autocommit(&self) -> Result<bool>;

    fn changes(&self) -> u64;

    fn last_insert_rowid(&self) -> i64;
}

#[derive(Clone)]
pub struct Connection {
    pub(crate) conn: Arc<dyn Conn + Send + Sync>,
}

// TODO(lucio): Convert to using tryinto params
impl Connection {
    pub async fn execute(&self, sql: &str, params: impl IntoParams) -> Result<u64> {
        tracing::trace!("executing `{}`", sql);
        self.conn.execute(sql, params.into_params()?).await
    }

    pub async fn execute_batch(&self, sql: &str) -> Result<()> {
        tracing::trace!("executing batch `{}`", sql);
        self.conn.execute_batch(sql).await
    }

    pub async fn prepare(&self, sql: &str) -> Result<Statement> {
        tracing::trace!("preparing `{}`", sql);
        self.conn.prepare(sql).await
    }

    pub async fn query(&self, sql: &str, params: impl IntoParams) -> Result<Rows> {
        let mut stmt = self.prepare(sql).await?;

        stmt.query(params).await
    }

    /// Begin a new transaction in DEFERRED mode, which is the default.
    pub async fn transaction(&self) -> Result<Transaction> {
        tracing::trace!("starting deferred transaction");
        self.transaction_with_behavior(TransactionBehavior::Deferred)
            .await
    }

    /// Begin a new transaction in the given mode.
    pub async fn transaction_with_behavior(
        &self,
        tx_behavior: TransactionBehavior,
    ) -> Result<Transaction> {
        tracing::trace!("starting {:?} transaction", tx_behavior);
        self.conn.transaction(tx_behavior).await
    }

    pub async fn is_autocommit(&self) -> Result<bool> {
        self.conn.is_autocommit().await
    }

    pub fn changes(&self) -> u64 {
        self.conn.changes()
    }

    pub fn last_insert_rowid(&self) -> i64 {
        self.conn.last_insert_rowid()
    }
}
