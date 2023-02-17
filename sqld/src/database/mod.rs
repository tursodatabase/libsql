use crate::query::{Query, Queries, QueryResult};
use crate::query_analysis::State;
use crate::Result;

pub mod libsql;
pub mod service;
pub mod write_proxy;

const TXN_TIMEOUT_SECS: u64 = 5;

#[async_trait::async_trait]
pub trait Database: Send + Sync {
    /// Executes a query (statement), and returns the result of the query and the state of the
    /// database connection after the query.
    async fn execute_one(&self, query: Query) -> Result<(QueryResult, State)> {
        let (mut results, state) = self.execute_batch(vec![query]).await?;
        let mut results = results.drain(..);
        Ok((results.next().unwrap(), state))
    }

    /// Executes a batch of queries, and returns a vec of results corresponding to the queries,
    /// and the state the database is in after the call to execute.
    async fn execute_batch(&self, queries: Queries) -> Result<(Vec<QueryResult>, State)>;
}
