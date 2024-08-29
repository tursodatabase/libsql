use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use libsql_sys::EncryptionConfig;
use libsql_wal::io::StdIO;
use libsql_wal::wal::{LibsqlWal, LibsqlWalManager};
use parking_lot::Mutex;
use tokio::sync::watch;

use crate::connection::program::check_program_auth;
use crate::metrics::DESCRIBE_COUNT;
use crate::namespace::broadcasters::BroadcasterHandle;
use crate::namespace::meta_store::MetaStoreHandle;
use crate::namespace::ResolveNamespacePathFn;
use crate::query_result_builder::{QueryBuilderConfig, QueryResultBuilder};
use crate::replication::FrameNo;
use crate::stats::Stats;
use crate::Result;
use crate::{record_time, SqldStorage, BLOCKING_RT};

use super::connection_core::CoreConnection;
use super::program::{check_describe_auth, DescribeResponse, Program};
use super::{MakeConnection, RequestContext};

pub struct MakeLibsqlConnection {
    pub(crate) inner: Arc<MakeLibsqlConnectionInner>,
}

pub struct MakeLibsqlConnectionInner {
    pub(crate) db_path: Arc<Path>,
    pub(crate) stats: Arc<Stats>,
    pub(crate) broadcaster: BroadcasterHandle,
    pub(crate) config_store: MetaStoreHandle,
    pub(crate) extensions: Arc<[PathBuf]>,
    pub(crate) max_response_size: u64,
    pub(crate) max_total_response_size: u64,
    pub(crate) auto_checkpoint: u32,
    pub(crate) current_frame_no_receiver: watch::Receiver<Option<FrameNo>>,
    pub(crate) encryption_config: Option<EncryptionConfig>,
    pub(crate) block_writes: Arc<AtomicBool>,
    pub(crate) resolve_attach_path: ResolveNamespacePathFn,
    pub(crate) wal_manager: LibsqlWalManager<StdIO, SqldStorage>,
}

#[async_trait::async_trait]
impl MakeConnection for MakeLibsqlConnection {
    type Connection = LibsqlConnection;

    async fn create(&self) -> crate::Result<Self::Connection> {
        let inner = self.inner.clone();
        let core = BLOCKING_RT
            .spawn_blocking(move || -> crate::Result<_> {
                let builder_config = QueryBuilderConfig {
                    max_size: Some(inner.max_response_size),
                    max_total_size: Some(inner.max_total_response_size),
                    auto_checkpoint: inner.auto_checkpoint,
                    encryption_config: inner.encryption_config.clone(),
                };

                // todo: handle retries
                CoreConnection::new(
                    &inner.db_path,
                    inner.extensions.clone(),
                    inner.wal_manager.clone(),
                    inner.stats.clone(),
                    inner.broadcaster.clone(),
                    inner.config_store.clone(),
                    builder_config,
                    inner.current_frame_no_receiver.clone(),
                    inner.block_writes.clone(),
                    inner.resolve_attach_path.clone(),
                )
            })
            .await
            .unwrap()?;

        Ok(LibsqlConnection {
            inner: Arc::new(Mutex::new(core)),
        })
    }
}

#[derive(Clone)]
pub struct LibsqlConnection {
    inner: Arc<Mutex<CoreConnection<LibsqlWal<StdIO>>>>,
}

impl LibsqlConnection {
    pub async fn execute<B: QueryResultBuilder>(
        &self,
        pgm: Program,
        ctx: RequestContext,
        builder: B,
    ) -> Result<B> {
        let config = self.inner.lock().config();
        check_program_auth(&ctx, &pgm, &config).await?;
        let conn = self.inner.clone();
        CoreConnection::run_async(conn, pgm, builder).await
    }
}

#[async_trait::async_trait]
impl super::Connection for LibsqlConnection {
    async fn execute_program<B: QueryResultBuilder>(
        &self,
        pgm: Program,
        ctx: RequestContext,
        builder: B,
        _replication_index: Option<FrameNo>,
    ) -> Result<B> {
        record_time! {
            "libsql_query_exec";
            self.execute(pgm, ctx, builder).await
        }
    }

    async fn describe(
        &self,
        sql: String,
        ctx: RequestContext,
        _replication_index: Option<FrameNo>,
    ) -> Result<crate::Result<DescribeResponse>> {
        DESCRIBE_COUNT.increment(1);
        check_describe_auth(ctx)?;
        let conn = self.inner.clone();
        let res = tokio::task::spawn_blocking(move || conn.lock().describe(&sql))
            .await
            .unwrap();

        Ok(res)
    }

    async fn is_autocommit(&self) -> Result<bool> {
        Ok(self.inner.lock().is_autocommit())
    }

    async fn checkpoint(&self) -> Result<()> {
        let conn = self.inner.clone();
        tokio::task::spawn_blocking(move || conn.lock().checkpoint())
            .await
            .unwrap()?;
        Ok(())
    }

    async fn vacuum_if_needed(&self) -> Result<()> {
        let conn = self.inner.clone();
        tokio::task::spawn_blocking(move || conn.lock().vacuum_if_needed())
            .await
            .unwrap()?;
        Ok(())
    }

    fn diagnostics(&self) -> String {
        String::new()
    }

    fn with_raw<R>(&self, f: impl FnOnce(&mut rusqlite::Connection) -> R) -> R {
        let mut inner = self.inner.lock();
        f(inner.raw_mut())
    }
}
