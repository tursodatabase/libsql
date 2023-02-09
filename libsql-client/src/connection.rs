//! `Connection` is the main structure to interact with the database.

use async_trait::async_trait;

use anyhow::Result;

use super::{QueryResult, Statement};

/// Trait describing capabilities of a database connection:
/// - executing statements, batches, transactions
#[async_trait(?Send)]
pub trait Connection {
    /// Executes a single SQL statement
    ///
    /// # Arguments
    /// * `stmt` - the SQL statement
    async fn execute(&self, stmt: impl Into<Statement>) -> Result<QueryResult> {
        let mut results = self.batch(std::iter::once(stmt)).await?;
        Ok(results.remove(0))
    }

    /// Executes a batch of SQL statements.
    /// Each statement is going to run in its own transaction,
    /// unless they're wrapped in BEGIN and END
    ///
    /// # Arguments
    /// * `stmts` - SQL statements
    /// ```
    async fn batch(
        &self,
        stmts: impl IntoIterator<Item = impl Into<Statement>>,
    ) -> Result<Vec<QueryResult>>;

    /// Executes an SQL transaction.
    /// Does not support nested transactions - do not use BEGIN or END
    /// inside a transaction.
    ///
    /// # Arguments
    /// * `stmts` - SQL statements
    /// ```
    async fn transaction(
        &self,
        stmts: impl IntoIterator<Item = impl Into<Statement>>,
    ) -> Result<Vec<QueryResult>> {
        let mut ret: Vec<QueryResult> = self
            .batch(
                std::iter::once(Statement::new("BEGIN"))
                    .chain(stmts.into_iter().map(|s| s.into()))
                    .chain(std::iter::once(Statement::new("END"))),
            )
            .await?
            .into_iter()
            .skip(1)
            .collect();
        ret.pop();
        Ok(ret)
    }
}
