use std::sync::Arc;
use std::task::Poll;

use futures::future::BoxFuture;
use futures::Future;
use tower::Service;

use super::Database;
use crate::error::Error;
use crate::query::{Queries, QueryResult};

#[async_trait::async_trait]
pub trait DbFactory: Send + Sync {
    async fn create(&self) -> Result<Arc<dyn Database>, Error>;
}

#[async_trait::async_trait]
impl<F, DB, Fut> DbFactory for F
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<DB, Error>> + Send,
    DB: Database + Sync + Send + 'static,
{
    async fn create(&self) -> Result<Arc<dyn Database>, Error> {
        let db = (self)().await?;
        Ok(Arc::new(db))
    }
}

#[derive(Clone)]
pub struct DbFactoryService {
    factory: Arc<dyn DbFactory>,
}

impl DbFactoryService {
    pub fn new(factory: Arc<dyn DbFactory>) -> Self {
        Self { factory }
    }
}

impl Service<()> for DbFactoryService {
    type Response = DbService;
    type Error = Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut std::task::Context<'_>) -> Poll<Result<(), Error>> {
        Ok(()).into()
    }

    fn call(&mut self, _: ()) -> Self::Future {
        let factory = self.factory.clone();
        Box::pin(async move {
            let db = factory.create().await?;
            Ok(DbService { db })
        })
    }
}

pub struct DbService {
    db: Arc<dyn Database>,
}

impl Drop for DbService {
    fn drop(&mut self) {
        tracing::trace!("connection closed");
    }
}

impl Service<Queries> for DbService {
    type Response = Vec<QueryResult>;
    type Error = Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        // need to implement backpressure: one req at a time.
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, queries: Queries) -> Self::Future {
        let db = self.db.clone();
        Box::pin(async move { Ok(db.execute(queries).await?.0) })
    }
}
