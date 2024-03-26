use libsql_replication::rpc::replication::NAMESPACE_METADATA_KEY;
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::time::{Duration, Instant};

use futures::Future;
use tokio::{sync::Semaphore, time::timeout};
use tonic::metadata::BinaryMetadataValue;

use crate::auth::Authenticated;
use crate::error::Error;
use crate::metrics::{
    CONCURRENT_CONNECTIONS_COUNT, CONNECTION_ALIVE_DURATION, CONNECTION_CREATE_TIME,
};
use crate::namespace::meta_store::MetaStore;
use crate::namespace::NamespaceName;
use crate::query::{Params, Query};
use crate::query_analysis::Statement;
use crate::query_result_builder::{IgnoreResult, QueryResultBuilder};
use crate::replication::FrameNo;
use crate::Result;

use self::program::{Cond, DescribeResponse, Program, Step};

pub mod config;
pub mod connection_manager;
pub mod dump;
pub mod libsql;
pub mod program;
pub mod write_proxy;

#[cfg(not(test))]
const TXN_TIMEOUT: Duration = Duration::from_secs(5);
#[cfg(test)]
const TXN_TIMEOUT: Duration = Duration::from_millis(100);

#[derive(Clone)]
pub struct RequestContext {
    /// Authentication for this request
    auth: Authenticated,
    /// current namespace
    namespace: NamespaceName,
    meta_store: MetaStore,
}

impl RequestContext {
    pub fn new(auth: Authenticated, namespace: NamespaceName, meta_store: MetaStore) -> Self {
        Self {
            auth,
            namespace,
            meta_store,
        }
    }

    pub fn upgrade_grpc_request<T>(&self, req: &mut tonic::Request<T>) {
        let namespace = BinaryMetadataValue::from_bytes(self.namespace.as_slice());
        req.metadata_mut()
            .insert_bin(NAMESPACE_METADATA_KEY, namespace);
        self.auth.upgrade_grpc_request(req);
    }

    pub fn namespace(&self) -> &NamespaceName {
        &self.namespace
    }

    pub fn auth(&self) -> &Authenticated {
        &self.auth
    }
}

#[async_trait::async_trait]
pub trait Connection: Send + Sync + 'static {
    /// Executes a query program
    async fn execute_program<B: QueryResultBuilder>(
        &self,
        pgm: Program,
        ctx: RequestContext,
        response_builder: B,
        replication_index: Option<FrameNo>,
    ) -> Result<B>;

    /// Execute all the queries in the batch sequentially.
    /// If an query in the batch fails, the remaining queries are ignores, and the batch current
    /// transaction (if any) is rolledback.
    async fn execute_batch_or_rollback<B: QueryResultBuilder>(
        &self,
        batch: Vec<Query>,
        ctx: RequestContext,
        result_builder: B,
        replication_index: Option<FrameNo>,
    ) -> Result<B> {
        let batch_len = batch.len();
        let mut steps = make_batch_program(batch);

        if !steps.is_empty() {
            // We add a conditional rollback step if the last step was not successful.
            steps.push(Step {
                query: Query {
                    stmt: Statement::parse("ROLLBACK").next().unwrap().unwrap(),
                    params: Params::empty(),
                    want_rows: false,
                },
                cond: Some(Cond::Not {
                    cond: Box::new(Cond::Ok {
                        step: steps.len() - 1,
                    }),
                }),
            })
        }

        let pgm = Program::new(steps);

        // ignore the rollback result
        let builder = result_builder.take(batch_len);
        let builder = self
            .execute_program(pgm, ctx, builder, replication_index)
            .await?;

        Ok(builder.into_inner())
    }

    /// Execute all the queries in the batch sequentially.
    /// If an query in the batch fails, the remaining queries are ignored
    async fn execute_batch<B: QueryResultBuilder>(
        &self,
        batch: Vec<Query>,
        ctx: RequestContext,
        result_builder: B,
        replication_index: Option<FrameNo>,
    ) -> Result<B> {
        let steps = make_batch_program(batch);
        let pgm = Program::new(steps);
        self.execute_program(pgm, ctx, result_builder, replication_index)
            .await
    }

    async fn rollback(&self, ctx: RequestContext) -> Result<()> {
        self.execute_batch(
            vec![Query {
                stmt: Statement::parse("ROLLBACK").next().unwrap().unwrap(),
                params: Params::empty(),
                want_rows: false,
            }],
            ctx,
            IgnoreResult,
            None,
        )
        .await?;

        Ok(())
    }

    /// Parse the SQL statement and return information about it.
    async fn describe(
        &self,
        sql: String,
        ctx: RequestContext,
        replication_index: Option<FrameNo>,
    ) -> Result<Result<DescribeResponse>>;

    /// Check whether the connection is in autocommit mode.
    async fn is_autocommit(&self) -> Result<bool>;

    /// Calls for database checkpoint (if supported).
    async fn checkpoint(&self) -> Result<()>;

    // Calls for database vacuum (if supported).
    async fn vacuum_if_needed(&self) -> Result<()>;

    fn diagnostics(&self) -> String;
}

fn make_batch_program(batch: Vec<Query>) -> Vec<Step> {
    let mut steps = Vec::with_capacity(batch.len());
    for (i, query) in batch.into_iter().enumerate() {
        let cond = if i > 0 {
            // only execute if the previous step was a success
            Some(Cond::Ok { step: i - 1 })
        } else {
            None
        };

        let step = Step { cond, query };
        steps.push(step);
    }
    steps
}

#[async_trait::async_trait]
pub trait MakeConnection: Send + Sync + 'static {
    type Connection: Connection;

    /// Create a new connection of type Self::Connection
    async fn create(&self) -> Result<Self::Connection, Error>;

    fn throttled(
        self,
        semaphore: Arc<Semaphore>,
        timeout: Option<Duration>,
        max_total_response_size: u64,
        max_concurrent_requests: u64,
    ) -> MakeThrottledConnection<Self>
    where
        Self: Sized,
    {
        MakeThrottledConnection::new(
            semaphore,
            self,
            timeout,
            max_total_response_size,
            max_concurrent_requests,
        )
    }

    fn map<F, T>(self, f: F) -> Map<Self, F>
    where
        F: Fn(Self::Connection) -> T + Send + Sync + 'static,
        Self: Sized,
    {
        Map { inner: self, f }
    }
}

pub struct Map<T, F> {
    inner: T,
    f: F,
}

#[async_trait::async_trait]
impl<F, T, O> MakeConnection for Map<T, F>
where
    F: Fn(T::Connection) -> O + Send + Sync + 'static,
    T: MakeConnection,
    O: Connection,
{
    type Connection = O;

    async fn create(&self) -> Result<Self::Connection, Error> {
        let conn = self.inner.create().await?;
        Ok((self.f)(conn))
    }
}

#[async_trait::async_trait]
impl<T: MakeConnection> MakeConnection for Arc<T> {
    type Connection = T::Connection;

    async fn create(&self) -> Result<Self::Connection, Error> {
        self.as_ref().create().await
    }
}

#[async_trait::async_trait]
impl<F, C, Fut> MakeConnection for F
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<C, Error>> + Send,
    C: Connection + Sync + Send + 'static,
{
    type Connection = C;

    async fn create(&self) -> Result<Self::Connection, Error> {
        let db = (self)().await?;
        Ok(db)
    }
}

pub struct MakeThrottledConnection<F> {
    semaphore: Arc<Semaphore>,
    connection_maker: F,
    timeout: Option<Duration>,
    // Max memory available for responses. High memory pressure
    // will result in reducing concurrency to prevent out-of-memory errors.
    max_total_response_size: u64,
    waiters: AtomicUsize,
    max_concurrent_requests: u64,
}

impl<F> MakeThrottledConnection<F> {
    fn new(
        semaphore: Arc<Semaphore>,
        connection_maker: F,
        timeout: Option<Duration>,
        max_total_response_size: u64,
        max_concurrent_requests: u64,
    ) -> Self {
        Self {
            semaphore,
            connection_maker,
            timeout,
            max_total_response_size,
            waiters: AtomicUsize::new(0),
            max_concurrent_requests,
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
impl<F: MakeConnection> MakeConnection for MakeThrottledConnection<F> {
    type Connection = TrackedConnection<F::Connection>;

    async fn create(&self) -> Result<Self::Connection, Error> {
        let before_create = Instant::now();
        // If the memory pressure is high, request more units to reduce concurrency.
        tracing::trace!(
            "Available semaphore units: {}",
            self.semaphore.available_permits()
        );
        let units = self.units_to_take();
        let waiters_guard = WaitersGuard::new(&self.waiters);
        if (waiters_guard.waiters.load(Ordering::Relaxed) as u64) >= self.max_concurrent_requests {
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
            let fut = self.semaphore.clone().acquire_many_owned(units);
            let mem_permit = match self.timeout {
                Some(t) => timeout(t, fut).await.map_err(|_| Error::DbCreateTimeout)?,
                None => fut.await,
            }
            .expect("semaphore closed");
            permit.merge(mem_permit);
        }

        let inner = self.connection_maker.create().await?;

        CONCURRENT_CONNECTIONS_COUNT.increment(1.0);
        CONNECTION_CREATE_TIME.record(before_create.elapsed());

        Ok(TrackedConnection {
            permit,
            inner,
            created_at: Instant::now(),
        })
    }
}

#[derive(Debug)]
pub struct TrackedConnection<DB> {
    inner: DB,
    #[allow(dead_code)] // just hold on to it
    permit: tokio::sync::OwnedSemaphorePermit,
    created_at: Instant,
}

impl<T> Drop for TrackedConnection<T> {
    fn drop(&mut self) {
        CONCURRENT_CONNECTIONS_COUNT.decrement(1.0);
        CONNECTION_ALIVE_DURATION.record(self.created_at.elapsed());
    }
}

impl<T> Deref for TrackedConnection<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[async_trait::async_trait]
impl<DB: Connection> Connection for TrackedConnection<DB> {
    #[inline]
    async fn execute_program<B: QueryResultBuilder>(
        &self,
        pgm: Program,
        ctx: RequestContext,
        builder: B,
        replication_index: Option<FrameNo>,
    ) -> crate::Result<B> {
        self.inner
            .execute_program(pgm, ctx, builder, replication_index)
            .await
    }

    #[inline]
    async fn describe(
        &self,
        sql: String,
        ctx: RequestContext,
        replication_index: Option<FrameNo>,
    ) -> crate::Result<crate::Result<DescribeResponse>> {
        self.inner.describe(sql, ctx, replication_index).await
    }

    #[inline]
    async fn is_autocommit(&self) -> crate::Result<bool> {
        self.inner.is_autocommit().await
    }

    #[inline]
    async fn checkpoint(&self) -> Result<()> {
        self.inner.checkpoint().await
    }

    #[inline]
    async fn vacuum_if_needed(&self) -> Result<()> {
        self.inner.vacuum_if_needed().await
    }

    #[inline]
    fn diagnostics(&self) -> String {
        self.inner.diagnostics()
    }
}

#[cfg(test)]
pub mod test {
    use super::*;

    #[derive(Debug)]
    struct DummyDb;

    #[async_trait::async_trait]
    impl Connection for DummyDb {
        async fn execute_program<B: QueryResultBuilder>(
            &self,
            _pgm: Program,
            _ctx: RequestContext,
            _builder: B,
            _replication_index: Option<FrameNo>,
        ) -> crate::Result<B> {
            unreachable!()
        }

        async fn describe(
            &self,
            _sql: String,
            _ctx: RequestContext,
            _replication_index: Option<FrameNo>,
        ) -> crate::Result<crate::Result<DescribeResponse>> {
            unreachable!()
        }

        async fn is_autocommit(&self) -> crate::Result<bool> {
            unreachable!()
        }

        async fn checkpoint(&self) -> Result<()> {
            unreachable!()
        }

        async fn vacuum_if_needed(&self) -> Result<()> {
            unreachable!()
        }

        fn diagnostics(&self) -> String {
            "dummy".into()
        }
    }

    #[tokio::test]
    async fn throttle_db_creation() {
        let factory = (|| async { Ok(DummyDb) }).throttled(
            Arc::new(Semaphore::new(10)),
            Some(Duration::from_millis(100)),
            u64::MAX,
            u64::MAX,
        );

        let mut conns = Vec::with_capacity(10);
        for _ in 0..10 {
            conns.push(factory.create().await.unwrap())
        }

        assert!(factory.create().await.is_err());

        drop(conns);

        assert!(factory.create().await.is_ok());
    }
}
