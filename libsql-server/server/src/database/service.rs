use std::future::ready;
use std::pin::Pin;
use std::rc::Rc;
use std::task::Poll;

use futures::Future;
use tower::Service;

use crate::query::{ErrorCode, Query, QueryError, QueryResponse, QueryResult};
use crate::query_analysis::Statements;

use super::Database;

pub trait DbFactory {
    type Future: Future<Output = anyhow::Result<Self::Db>>;
    type Db: Database;

    fn create(&mut self) -> Self::Future;
}

impl<F, DB, Fut> DbFactory for F
where
    F: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<DB>>,
    DB: Database,
{
    type Db = DB;
    type Future = Fut;

    fn create(&mut self) -> Self::Future {
        (self)()
    }
}

pub struct DbFactoryService<F> {
    factory: F,
}

impl<F> DbFactoryService<F> {
    pub fn new(factory: F) -> Self {
        Self { factory }
    }
}

impl<F> Service<()> for DbFactoryService<F>
where
    F: DbFactory,
    F::Future: 'static,
{
    type Response = DbService<F::Db>;
    type Error = anyhow::Error;
    type Future = Pin<Box<dyn Future<Output = std::result::Result<Self::Response, Self::Error>>>>;

    fn poll_ready(&mut self, _cx: &mut std::task::Context<'_>) -> Poll<anyhow::Result<()>> {
        Ok(()).into()
    }

    fn call(&mut self, _: ()) -> Self::Future {
        let fut = self.factory.create();

        Box::pin(async move {
            let db = Rc::new(fut.await?);
            Ok(DbService { db })
        })
    }
}

pub struct DbService<DB> {
    db: Rc<DB>,
}

impl<DB: Database + 'static> Service<Query> for DbService<DB> {
    type Response = QueryResponse;
    type Error = QueryError;
    type Future = Pin<Box<dyn Future<Output = QueryResult>>>;

    fn poll_ready(&mut self, _cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        // need to implement backpressure: one req at a time.
        Ok(()).into()
    }

    fn call(&mut self, query: Query) -> Self::Future {
        let db = self.db.clone();
        match query {
            Query::SimpleQuery(stmts) => Box::pin(async move {
                match Statements::parse(stmts) {
                    Ok(stmts) => db.execute(stmts).await,
                    Err(e) => Err(QueryError::new(ErrorCode::SQLError, e)),
                }
            }),
            Query::Disconnect => Box::pin(ready(Ok(QueryResponse::Ack))),
        }
    }
}
