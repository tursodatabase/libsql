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

    fn is_autocommit(&self) -> bool;

    fn changes(&self) -> u64;

    fn last_insert_rowid(&self) -> i64;

    async fn reset(&self);
}

/// A connection to some libsql database, this can be a remote one or a local one.
#[derive(Clone)]
pub struct Connection {
    pub(crate) conn: Arc<dyn Conn + Send + Sync>,
}

impl Connection {
    /// Execute sql query provided some type that implements [`IntoParams`] returning
    /// on success the number of rows that were changed.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # async fn run(conn: &libsql::Connection) {
    /// # use libsql::params;
    /// conn.execute("INSERT INTO foo (id) VALUES (?1)", [42]).await.unwrap();
    /// conn.execute("INSERT INTO foo (id, name) VALUES (?1, ?2)", params![42, "baz"]).await.unwrap();
    /// # }
    /// ```
    ///
    /// For more info on how to pass params check [`IntoParams`]'s docs.
    pub async fn execute(&self, sql: &str, params: impl IntoParams) -> Result<u64> {
        tracing::trace!("executing `{}`", sql);
        self.conn.execute(sql, params.into_params()?).await
    }

    /// Execute a batch set of statements.
    pub async fn execute_batch(&self, sql: &str) -> Result<()> {
        tracing::trace!("executing batch `{}`", sql);
        self.conn.execute_batch(sql).await
    }

    /// Execute sql query provided some type that implements [`IntoParams`] returning
    /// on success the [`Rows`].
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # async fn run(conn: &libsql::Connection) {
    /// # use libsql::params;
    /// conn.query("SELECT foo FROM bar WHERE id = ?1", [42]).await.unwrap();
    /// conn.query("SELECT foo FROM bar WHERE id = ?1 AND name = ?2", params![42, "baz"]).await.unwrap();
    /// # }
    /// ```
    /// For more info on how to pass params check [`IntoParams`]'s docs and on how to
    /// extract values out of the rows check the [`Rows`] docs.
    pub async fn query(&self, sql: &str, params: impl IntoParams) -> Result<Rows> {
        let mut stmt = self.prepare(sql).await?;

        stmt.query(params).await
    }

    /// Prepares a cached statement.
    pub async fn prepare(&self, sql: &str) -> Result<Statement> {
        tracing::trace!("preparing `{}`", sql);
        self.conn.prepare(sql).await
    }

    /// Begin a new transaction in `DEFERRED` mode, which is the default.
    pub async fn transaction(&self) -> Result<Transaction> {
        tracing::trace!("starting deferred transaction");
        self.transaction_with_behavior(TransactionBehavior::Deferred)
            .await
    }

    /// Begin a new transaction in the given [`TransactionBehavior`].
    pub async fn transaction_with_behavior(
        &self,
        tx_behavior: TransactionBehavior,
    ) -> Result<Transaction> {
        tracing::trace!("starting {:?} transaction", tx_behavior);
        self.conn.transaction(tx_behavior).await
    }

    /// Check weather libsql is in `autocommit` or not.
    pub fn is_autocommit(&self) -> bool {
        self.conn.is_autocommit()
    }

    /// Check the amount of changes the last query created.
    pub fn changes(&self) -> u64 {
        self.conn.changes()
    }

    /// Check the last inserted row id.
    pub fn last_insert_rowid(&self) -> i64 {
        self.conn.last_insert_rowid()
    }

    pub async fn reset(&self) {
        self.conn.reset().await
    }
}
