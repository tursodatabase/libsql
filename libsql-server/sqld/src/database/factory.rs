use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

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

    fn throttled(
        self,
        conccurency: usize,
        timeout: Option<Duration>,
        max_total_response_size: u64,
    ) -> ThrottledDbFactory<Self>
    where
        Self: Sized,
    {
        ThrottledDbFactory::new(conccurency, self, timeout, max_total_response_size)
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

pub struct ThrottledDbFactory<F> {
    semaphore: Arc<Semaphore>,
    factory: F,
    timeout: Option<Duration>,
    // Max memory available for responses. High memory pressure
    // will result in reducing concurrency to prevent out-of-memory errors.
    max_total_response_size: u64,
    waiters: AtomicUsize,
}

impl<F> ThrottledDbFactory<F> {
    fn new(
        conccurency: usize,
        factory: F,
        timeout: Option<Duration>,
        max_total_response_size: u64,
    ) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(conccurency)),
            factory,
            timeout,
            max_total_response_size,
            waiters: AtomicUsize::new(0),
        }
    }

    // How many units should be acquired from the semaphore,
    // depending on current memory pressure.
    fn units_to_take(&self) -> u32 {
        let total_response_size = crate::query_result_builder::TOTAL_RESPONSE_SIZE
            .load(std::sync::atomic::Ordering::Relaxed) as u64;
        if total_response_size * 2 > self.max_total_response_size {
            tracing::trace!("High memory pressure, reducing concurrency");
            16
        } else if total_response_size * 4 > self.max_total_response_size {
            tracing::trace!("Medium memory pressure, reducing concurrency");
            4
        } else {
            1
        }
    }
}

struct WaitersGuard<'a> {
    pub waiters: &'a AtomicUsize,
}

impl<'a> WaitersGuard<'a> {
    fn new(waiters: &'a AtomicUsize) -> Self {
        waiters.fetch_add(1, Ordering::Relaxed);
        Self { waiters }
    }
}

impl Drop for WaitersGuard<'_> {
    fn drop(&mut self) {
        self.waiters.fetch_sub(1, Ordering::Relaxed);
    }
}

#[async_trait::async_trait]
impl<F: DbFactory> DbFactory for ThrottledDbFactory<F> {
    type Db = TrackedDb<F::Db>;

    async fn create(&self) -> Result<Self::Db, Error> {
        // If the memory pressure is high, request more units to reduce concurrency.
        let units = self.units_to_take();
        let waiters_guard = WaitersGuard::new(&self.waiters);
        if waiters_guard.waiters.load(Ordering::Relaxed) >= 128 {
            return Err(Error::TooManyRequests);
        }
        let fut = self.semaphore.clone().acquire_many_owned(units);
        let mut permit = match self.timeout {
            Some(t) => timeout(t, fut).await.map_err(|_| Error::DbCreateTimeout)?,
            None => fut.await,
        }
        .expect("semaphore closed");

        let units = self.units_to_take();
        if units > 1 {
            tracing::debug!("Reacquiring {units} units due to high memory pressure");
            let fut = self.semaphore.clone().acquire_many_owned(64);
            let mem_permit = match self.timeout {
                Some(t) => timeout(t, fut).await.map_err(|_| Error::DbCreateTimeout)?,
                None => fut.await,
            }
            .expect("semaphore closed");
            permit.merge(mem_permit);
        }

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
        let factory =
            (|| async { Ok(DummyDb) }).throttled(10, Some(Duration::from_millis(100)), u64::MAX);

        let mut conns = Vec::with_capacity(10);
        for _ in 0..10 {
            conns.push(factory.create().await.unwrap())
        }

        assert!(factory.create().await.is_err());

        drop(conns);

        assert!(factory.create().await.is_ok());
    }
}
