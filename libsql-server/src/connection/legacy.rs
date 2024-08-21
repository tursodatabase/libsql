use std::ffi::c_int;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use libsql_sys::wal::wrapper::{WrapWal, WrappedWal};
use libsql_sys::wal::{BusyHandler, CheckpointCallback, Wal, WalManager};
use libsql_sys::EncryptionConfig;
use parking_lot::Mutex;
use rusqlite::ffi::SQLITE_BUSY;
use rusqlite::{ErrorCode, OpenFlags};
use tokio::sync::watch;
use tokio::time::Duration;

use crate::error::Error;
use crate::metrics::DESCRIBE_COUNT;
use crate::namespace::broadcasters::BroadcasterHandle;
use crate::namespace::meta_store::MetaStoreHandle;
use crate::namespace::ResolveNamespacePathFn;
use crate::query_result_builder::{QueryBuilderConfig, QueryResultBuilder};
use crate::replication::FrameNo;
use crate::stats::Stats;
use crate::{record_time, Result};

use super::connection_core::CoreConnection;

use super::connection_manager::{
    ConnectionManager, InnerWalManager, ManagedConnectionWal, ManagedConnectionWalWrapper,
};
use super::program::{check_describe_auth, check_program_auth, DescribeResponse};
use super::{MakeConnection, Program, RequestContext, TXN_TIMEOUT};

pub struct MakeLegacyConnection<W> {
    db_path: PathBuf,
    wal_wrapper: W,
    stats: Arc<Stats>,
    broadcaster: BroadcasterHandle,
    config_store: MetaStoreHandle,
    extensions: Arc<[PathBuf]>,
    max_response_size: u64,
    max_total_response_size: u64,
    auto_checkpoint: u32,
    current_frame_no_receiver: watch::Receiver<Option<FrameNo>>,
    connection_manager: ConnectionManager,
    /// return sqlite busy. To mitigate that, we hold on to one connection
    _db: Option<LegacyConnection<W>>,
    encryption_config: Option<EncryptionConfig>,
    block_writes: Arc<AtomicBool>,
    resolve_attach_path: ResolveNamespacePathFn,
    make_wal_manager: Arc<dyn Fn() -> InnerWalManager + Sync + Send + 'static>,
}

impl<W> MakeLegacyConnection<W>
where
    W: WrapWal<ManagedConnectionWal> + Send + 'static + Clone,
{
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        db_path: PathBuf,
        wal_wrapper: W,
        stats: Arc<Stats>,
        broadcaster: BroadcasterHandle,
        config_store: MetaStoreHandle,
        extensions: Arc<[PathBuf]>,
        max_response_size: u64,
        max_total_response_size: u64,
        auto_checkpoint: u32,
        current_frame_no_receiver: watch::Receiver<Option<FrameNo>>,
        encryption_config: Option<EncryptionConfig>,
        block_writes: Arc<AtomicBool>,
        resolve_attach_path: ResolveNamespacePathFn,
        make_wal_manager: Arc<dyn Fn() -> InnerWalManager + Sync + Send + 'static>,
    ) -> Result<Self> {
        let txn_timeout = config_store.get().txn_timeout.unwrap_or(TXN_TIMEOUT);

        let mut this = Self {
            db_path,
            stats,
            broadcaster,
            config_store,
            extensions,
            max_response_size,
            max_total_response_size,
            auto_checkpoint,
            current_frame_no_receiver,
            _db: None,
            wal_wrapper,
            encryption_config,
            block_writes,
            resolve_attach_path,
            connection_manager: ConnectionManager::new(txn_timeout),
            make_wal_manager,
        };

        let db = this.try_create_db().await?;
        this._db = Some(db);

        Ok(this)
    }

    /// Tries to create a database, retrying if the database is busy.
    async fn try_create_db(&self) -> Result<LegacyConnection<W>> {
        // try 100 times to acquire initial db connection.
        let mut retries = 0;
        loop {
            match self.make_connection().await {
                Ok(conn) => return Ok(conn),
                Err(
                    err @ Error::RusqliteError(rusqlite::Error::SqliteFailure(
                        rusqlite::ffi::Error {
                            code: ErrorCode::DatabaseBusy,
                            ..
                        },
                        _,
                    )),
                ) => {
                    if retries < 100 {
                        tracing::warn!("Database file is busy, retrying...");
                        retries += 1;
                        tokio::time::sleep(Duration::from_millis(100)).await
                    } else {
                        Err(err)?;
                    }
                }
                Err(e) => Err(e)?,
            }
        }
    }

    #[tracing::instrument(skip(self))]
    pub(super) async fn make_connection(&self) -> Result<LegacyConnection<W>> {
        LegacyConnection::new(
            self.db_path.clone(),
            self.extensions.clone(),
            self.wal_wrapper.clone(),
            self.stats.clone(),
            self.broadcaster.clone(),
            self.config_store.clone(),
            QueryBuilderConfig {
                max_size: Some(self.max_response_size),
                max_total_size: Some(self.max_total_response_size),
                auto_checkpoint: self.auto_checkpoint,
                encryption_config: self.encryption_config.clone(),
            },
            self.current_frame_no_receiver.clone(),
            self.block_writes.clone(),
            self.resolve_attach_path.clone(),
            self.connection_manager.clone(),
            self.make_wal_manager.clone(),
        )
        .await
    }
}

#[async_trait::async_trait]
impl<W> MakeConnection for MakeLegacyConnection<W>
where
    W: WrapWal<ManagedConnectionWal> + Send + Sync + 'static + Clone,
{
    type Connection = LegacyConnection<W>;

    async fn create(&self) -> Result<Self::Connection, Error> {
        self.make_connection().await
    }
}

pub struct LegacyConnection<T> {
    pub(super) inner: Arc<Mutex<CoreConnection<WrappedWal<T, ManagedConnectionWal>>>>,
}

#[cfg(test)]
impl LegacyConnection<libsql_sys::wal::wrapper::PassthroughWalWrapper> {
    pub async fn new_test(path: &Path) -> Self {
        #[cfg(not(feature = "durable-wal"))]
        use libsql_sys::wal::either::Either as EitherWAL;
        #[cfg(feature = "durable-wal")]
        use libsql_sys::wal::either::Either3 as EitherWAL;
        use libsql_sys::wal::Sqlite3WalManager;

        Self::new(
            path.to_owned(),
            Arc::new([]),
            libsql_sys::wal::wrapper::PassthroughWalWrapper,
            Default::default(),
            Default::default(),
            MetaStoreHandle::new_test(),
            QueryBuilderConfig::default(),
            tokio::sync::watch::channel(None).1,
            Default::default(),
            Arc::new(|_| unreachable!()),
            ConnectionManager::new(TXN_TIMEOUT),
            Arc::new(|| EitherWAL::A(Sqlite3WalManager::default())),
        )
        .await
        .unwrap()
    }
}

impl<T> Clone for LegacyConnection<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

#[derive(Clone, Copy)]
pub struct InhibitCheckpointWalWrapper {
    close_only: bool,
}

impl InhibitCheckpointWalWrapper {
    pub fn new(close_only: bool) -> Self {
        Self { close_only }
    }
}

impl<W: Wal> WrapWal<W> for InhibitCheckpointWalWrapper {
    fn checkpoint(
        &mut self,
        wrapped: &mut W,
        db: &mut libsql_sys::wal::Sqlite3Db,
        mode: libsql_sys::wal::CheckpointMode,
        busy_handler: Option<&mut dyn BusyHandler>,
        sync_flags: u32,
        buf: &mut [u8],
        checkpoint_cb: Option<&mut dyn CheckpointCallback>,
        in_wal: Option<&mut i32>,
        backfilled: Option<&mut i32>,
    ) -> libsql_sys::wal::Result<()> {
        if !self.close_only {
            wrapped.checkpoint(
                db,
                mode,
                busy_handler,
                sync_flags,
                buf,
                checkpoint_cb,
                in_wal,
                backfilled,
            )
        } else {
            tracing::warn!(
                "checkpoint inhibited: this connection is not allowed to perform checkpoints"
            );
            Err(rusqlite::ffi::Error::new(SQLITE_BUSY))
        }
    }

    fn close<M: WalManager<Wal = W>>(
        &mut self,
        manager: &M,
        wrapped: &mut W,
        db: &mut libsql_sys::wal::Sqlite3Db,
        sync_flags: c_int,
        _scratch: Option<&mut [u8]>,
    ) -> libsql_sys::wal::Result<()> {
        // sqlite3 wall will not checkpoint if it's not provided with a scratch buffer. We take
        // advantage of that to prevent checpoint on such connections.
        manager.close(wrapped, db, sync_flags, None)
    }
}

pub type InhibitCheckpoint<T> = WrappedWal<InhibitCheckpointWalWrapper, T>;

// Opens a connection with checkpoint inhibited
pub fn open_conn<T>(
    path: &Path,
    wal_manager: T,
    flags: Option<OpenFlags>,
    encryption_config: Option<EncryptionConfig>,
) -> Result<libsql_sys::Connection<InhibitCheckpoint<T::Wal>>, rusqlite::Error>
where
    T: WalManager,
{
    open_conn_active_checkpoint(
        path,
        wal_manager.wrap(InhibitCheckpointWalWrapper::new(false)),
        flags,
        u32::MAX,
        encryption_config,
    )
}

/// Same as open_conn, but with checkpointing activated.
pub fn open_conn_active_checkpoint<T>(
    path: &Path,
    wal_manager: T,
    flags: Option<OpenFlags>,
    auto_checkpoint: u32,
    encryption_config: Option<EncryptionConfig>,
) -> Result<libsql_sys::Connection<T::Wal>, rusqlite::Error>
where
    T: WalManager,
{
    let flags = flags.unwrap_or(
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_URI
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    );

    libsql_sys::Connection::open(
        path.join("data"),
        flags,
        wal_manager,
        auto_checkpoint,
        encryption_config,
    )
}

impl<W> LegacyConnection<W>
where
    W: WrapWal<ManagedConnectionWal> + Send + Clone + 'static,
{
    pub async fn new(
        path: impl AsRef<Path> + Send + 'static,
        extensions: Arc<[PathBuf]>,
        wal_wrapper: W,
        stats: Arc<Stats>,
        broadcaster: BroadcasterHandle,
        config_store: MetaStoreHandle,
        builder_config: QueryBuilderConfig,
        current_frame_no_receiver: watch::Receiver<Option<FrameNo>>,
        block_writes: Arc<AtomicBool>,
        resolve_attach_path: ResolveNamespacePathFn,
        connection_manager: ConnectionManager,
        make_wal: Arc<dyn Fn() -> InnerWalManager + Sync + Send + 'static>,
    ) -> crate::Result<Self> {
        let (conn, id) = tokio::task::spawn_blocking({
            let connection_manager = connection_manager.clone();
            move || -> crate::Result<_> {
                let manager = ManagedConnectionWalWrapper::new(connection_manager);
                let id = manager.id();
                let wal = make_wal().wrap(manager).wrap(wal_wrapper);

                let conn = CoreConnection::new(
                    path.as_ref(),
                    extensions,
                    wal,
                    stats,
                    broadcaster,
                    config_store,
                    builder_config,
                    current_frame_no_receiver,
                    block_writes,
                    resolve_attach_path,
                )?;

                let namespace = path
                    .as_ref()
                    .file_name()
                    .unwrap_or_default()
                    .to_os_string()
                    .into_string()
                    .unwrap_or_default();
                conn.raw().create_scalar_function(
                    "libsql_server_database_name",
                    0,
                    rusqlite::functions::FunctionFlags::SQLITE_UTF8
                        | rusqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC,
                    move |_| Ok(namespace.clone()),
                )?;
                Ok((conn, id))
            }
        })
        .await
        .unwrap()?;

        let inner = Arc::new(Mutex::new(conn));

        connection_manager.register_connection(&inner, id);

        Ok(Self { inner })
    }

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
impl<W> super::Connection for LegacyConnection<W>
where
    W: WrapWal<ManagedConnectionWal> + Clone + Send + 'static,
{
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
