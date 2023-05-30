use std::{sync::Arc, time::Duration};

use futures::Future;
use tokio::{sync::Semaphore, time::timeout};

use super::{Database, DescribeResult, Program};
use crate::{
    auth::Authenticated, error::Error, query_analysis::State,
    query_result_builder::QueryResultBuilder,
};

#[async_trait::async_trait]
pub trait DbFactory: Send + Sync + 'static {
    type Db: Database;

    async fn create(&self) -> Result<Self::Db, Error>;

    fn throttled(self, conccurency: usize, timeout: Option<Duration>) -> ThrottledDbFactory<Self>
    where
        Self: Sized,
    {
        ThrottledDbFactory::new(conccurency, self, timeout)
    }
}

#[async_trait::async_trait]
impl<F, DB, Fut> DbFactory for F
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<DB, Error>> + Send,
    DB: Database + Sync + Send + 'static,
{
    type Db = DB;

    async fn create(&self) -> Result<Self::Db, Error> {
        let db = (self)().await?;
        Ok(db)
    }
}

#[derive(Clone)]
pub struct ThrottledDbFactory<F> {
    semaphore: Arc<Semaphore>,
    factory: F,
    timeout: Option<Duration>,
}

impl<F> ThrottledDbFactory<F> {
    fn new(conccurency: usize, factory: F, timeout: Option<Duration>) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(conccurency)),
            factory,
            timeout,
        }
    }
}

#[async_trait::async_trait]
impl<F: DbFactory> DbFactory for ThrottledDbFactory<F> {
    type Db = TrackedDb<F::Db>;

    async fn create(&self) -> Result<Self::Db, Error> {
        let fut = self.semaphore.clone().acquire_owned();
        let permit = match self.timeout {
            Some(t) => timeout(t, fut).await.map_err(|_| Error::DbCreateTimeout)?,
            None => fut.await,
        }
        .expect("semaphore closed");
        let inner = self.factory.create().await?;
        Ok(TrackedDb { permit, inner })
    }
}

pub struct TrackedDb<DB> {
    inner: DB,
    #[allow(dead_code)] // just hold on to it
    permit: tokio::sync::OwnedSemaphorePermit,
}

#[async_trait::async_trait]
impl<DB: Database> Database for TrackedDb<DB> {
    #[inline]
    async fn execute_program<B: QueryResultBuilder>(
        &self,
        pgm: Program,
        auth: Authenticated,
        builder: B,
    ) -> crate::Result<(B, State)> {
        self.inner.execute_program(pgm, auth, builder).await
    }

    #[inline]
    async fn describe(&self, sql: String, auth: Authenticated) -> crate::Result<DescribeResult> {
        self.inner.describe(sql, auth).await
    }
}

#[cfg(test)]
mod test {
    use super::*;

    struct DummyDb;

    #[async_trait::async_trait]
    impl Database for DummyDb {
        async fn execute_program<B: QueryResultBuilder>(
            &self,
            _pgm: Program,
            _auth: Authenticated,
            _builder: B,
        ) -> crate::Result<(B, State)> {
            unreachable!()
        }

        async fn describe(
            &self,
            _sql: String,
            _auth: Authenticated,
        ) -> crate::Result<DescribeResult> {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn throttle_db_creation() {
        let factory = (|| async { Ok(DummyDb) }).throttled(10, Some(Duration::from_millis(100)));

        let mut conns = Vec::with_capacity(10);
        for _ in 0..10 {
            conns.push(factory.create().await.unwrap())
        }

        assert!(factory.create().await.is_err());

        drop(conns);

        assert!(factory.create().await.is_ok());
    }
}
