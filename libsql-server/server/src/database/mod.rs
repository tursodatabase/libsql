use std::rc::Rc;

use crate::query::QueryResult;
use crate::query_analysis::Statements;

pub mod service;
pub mod sqlite;

const TXN_TIMEOUT_SECS: u64 = 5;

#[async_trait::async_trait(?Send)]
pub trait Database {
    async fn execute(&self, query: Statements) -> QueryResult;
}

#[async_trait::async_trait(?Send)]
impl<T: Database> Database for Rc<T> {
    async fn execute(&self, query: Statements) -> QueryResult {
        self.as_ref().execute(query).await
    }
}
