use crate::query::{Queries, QueryResult};
use crate::query_analysis::State;

pub mod libsql;
pub mod service;
pub mod write_proxy;

const TXN_TIMEOUT_SECS: u64 = 5;

#[async_trait::async_trait]
pub trait Database {
    /// Executes a batch of queries, and return the a vec of results corresponding to the queries,
    /// and the state the database is in after the call to execute.
    async fn execute(&self, queries: Queries) -> anyhow::Result<(Vec<QueryResult>, State)>;
}
