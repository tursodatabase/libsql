use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;

use futures::Future;
use tower::Service;

use super::Database;
use crate::query::{Queries, QueryResult};
pub trait DbFactory: Send + Sync + 'static {
    type Future: Future<Output = anyhow::Result<Self::Db>> + Send;
    type Db: Database + Send + Sync;

    fn create(&self) -> Self::Future;
}

impl<F, DB, Fut> DbFactory for F
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = anyhow::Result<DB>> + Sync + Send,
    DB: Database + Sync + Send,
{
    type Db = DB;
    type Future = Fut;

    fn create(&self) -> Self::Future {
        (self)()
    }
}

#[derive(Clone)]
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
    F::Future: 'static + Send + Sync,
{
    type Response = DbService<F::Db>;
    type Error = anyhow::Error;
    type Future = Pin<
        Box<dyn Future<Output = std::result::Result<Self::Response, Self::Error>> + Send + Sync>,
    >;

    fn poll_ready(&mut self, _cx: &mut std::task::Context<'_>) -> Poll<anyhow::Result<()>> {
        Ok(()).into()
    }

    fn call(&mut self, _: ()) -> Self::Future {
        let fut = self.factory.create();

        Box::pin(async move {
            let db = Arc::new(fut.await?);
            Ok(DbService { db })
        })
    }
}

pub struct DbService<DB> {
    db: Arc<DB>,
}

impl<DB> Drop for DbService<DB> {
    fn drop(&mut self) {
        tracing::trace!("connection closed");
    }
}

impl<DB: Database + 'static + Send + Sync> Service<Queries> for DbService<DB> {
    type Response = Vec<QueryResult>;
    type Error = anyhow::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        // need to implement backpressure: one req at a time.
        Ok(()).into()
    }

    fn call(&mut self, queries: Queries) -> Self::Future {
        let db = self.db.clone();
        Box::pin(async move { Ok(db.execute(queries).await?.0) })
    }
}
