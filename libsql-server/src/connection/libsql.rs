use std::ffi::{c_int, c_void};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use libsql_sys::wal::wrapper::{WrapWal, WrappedWal};
use libsql_sys::wal::{BusyHandler, CheckpointCallback, Sqlite3WalManager, Wal, WalManager};
use libsql_sys::EncryptionConfig;
use metrics::histogram;
use parking_lot::Mutex;
use rusqlite::ffi::SQLITE_BUSY;
use rusqlite::{ErrorCode, OpenFlags};
use tokio::sync::watch;
use tokio::time::{Duration, Instant};

use crate::error::Error;
use crate::metrics::{DESCRIBE_COUNT, PROGRAM_EXEC_COUNT, VACUUM_COUNT, WAL_CHECKPOINT_COUNT};
use crate::namespace::meta_store::MetaStoreHandle;
use crate::namespace::ResolveNamespacePathFn;
use crate::query_analysis::StmtKind;
use crate::query_result_builder::{QueryBuilderConfig, QueryResultBuilder};
use crate::replication::FrameNo;
use crate::stats::{Stats, StatsUpdateMessage};
use crate::{Result, BLOCKING_RT};

use super::connection_manager::{
    ConnectionManager, ManagedConnectionWal, ManagedConnectionWalWrapper,
};
use super::program::{
    check_describe_auth, check_program_auth, DescribeCol, DescribeParam, DescribeResponse, Vm,
};
use super::{MakeConnection, Program, RequestContext};

pub struct MakeLibSqlConn<W> {
    db_path: PathBuf,
    wal_wrapper: W,
    stats: Arc<Stats>,
    config_store: MetaStoreHandle,
    extensions: Arc<[PathBuf]>,
    max_response_size: u64,
    max_total_response_size: u64,
    auto_checkpoint: u32,
    current_frame_no_receiver: watch::Receiver<Option<FrameNo>>,
    connection_manager: ConnectionManager,
    /// return sqlite busy. To mitigate that, we hold on to one connection
    _db: Option<LibSqlConnection<W>>,
    encryption_config: Option<EncryptionConfig>,
    block_writes: Arc<AtomicBool>,
    resolve_attach_path: ResolveNamespacePathFn,
}

impl<W> MakeLibSqlConn<W>
where
    W: WrapWal<ManagedConnectionWal> + Send + 'static + Clone,
{
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        db_path: PathBuf,
        wal_wrapper: W,
        stats: Arc<Stats>,
        config_store: MetaStoreHandle,
        extensions: Arc<[PathBuf]>,
        max_response_size: u64,
        max_total_response_size: u64,
        auto_checkpoint: u32,
        current_frame_no_receiver: watch::Receiver<Option<FrameNo>>,
        encryption_config: Option<EncryptionConfig>,
        block_writes: Arc<AtomicBool>,
        resolve_attach_path: ResolveNamespacePathFn,
    ) -> Result<Self> {
        let mut this = Self {
            db_path,
            stats,
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
            connection_manager: ConnectionManager::default(),
        };

        let db = this.try_create_db().await?;
        this._db = Some(db);

        Ok(this)
    }

    /// Tries to create a database, retrying if the database is busy.
    async fn try_create_db(&self) -> Result<LibSqlConnection<W>> {
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

    async fn make_connection(&self) -> Result<LibSqlConnection<W>> {
        LibSqlConnection::new(
            self.db_path.clone(),
            self.extensions.clone(),
            self.wal_wrapper.clone(),
            self.stats.clone(),
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
        )
        .await
    }
}

#[async_trait::async_trait]
impl<W> MakeConnection for MakeLibSqlConn<W>
where
    W: WrapWal<ManagedConnectionWal> + Send + Sync + 'static + Clone,
{
    type Connection = LibSqlConnection<W>;

    async fn create(&self) -> Result<Self::Connection, Error> {
        self.make_connection().await
    }
}

pub struct LibSqlConnection<T> {
    inner: Arc<Mutex<Connection<WrappedWal<T, ManagedConnectionWal>>>>,
}

#[cfg(test)]
impl LibSqlConnection<libsql_sys::wal::wrapper::PassthroughWalWrapper> {
    pub async fn new_test(path: &Path) -> Self {
        Self::new(
            path.to_owned(),
            Arc::new([]),
            libsql_sys::wal::wrapper::PassthroughWalWrapper,
            Default::default(),
            MetaStoreHandle::new_test(),
            QueryBuilderConfig::default(),
            tokio::sync::watch::channel(None).1,
            Default::default(),
            Arc::new(|_| unreachable!()),
            ConnectionManager::default(),
        )
        .await
        .unwrap()
    }
}

impl<T> Clone for LibSqlConnection<T> {
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

impl<W> LibSqlConnection<W>
where
    W: WrapWal<ManagedConnectionWal> + Send + Clone + 'static,
{
    pub async fn new(
        path: impl AsRef<Path> + Send + 'static,
        extensions: Arc<[PathBuf]>,
        wal_wrapper: W,
        stats: Arc<Stats>,
        config_store: MetaStoreHandle,
        builder_config: QueryBuilderConfig,
        current_frame_no_receiver: watch::Receiver<Option<FrameNo>>,
        block_writes: Arc<AtomicBool>,
        resolve_attach_path: ResolveNamespacePathFn,
        connection_manager: ConnectionManager,
    ) -> crate::Result<Self> {
        let (conn, id) = tokio::task::spawn_blocking({
            let connection_manager = connection_manager.clone();
            move || -> crate::Result<_> {
                let manager = ManagedConnectionWalWrapper::new(connection_manager);
                let id = manager.id();
                let wal = Sqlite3WalManager::default().wrap(manager).wrap(wal_wrapper);

                let conn = Connection::new(
                    path.as_ref(),
                    extensions,
                    wal,
                    stats,
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
                conn.conn.create_scalar_function(
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

    pub fn with_raw<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut rusqlite::Connection) -> R,
    {
        let mut inner = self.inner.lock();
        f(&mut inner.conn)
    }
}

pub(super) struct Connection<W> {
    conn: libsql_sys::Connection<W>,
    stats: Arc<Stats>,
    config_store: MetaStoreHandle,
    builder_config: QueryBuilderConfig,
    current_frame_no_receiver: watch::Receiver<Option<FrameNo>>,
    block_writes: Arc<AtomicBool>,
    resolve_attach_path: ResolveNamespacePathFn,
    forced_rollback: bool,
}

fn update_stats(
    stats: &Stats,
    sql: String,
    rows_read: u64,
    rows_written: u64,
    mem_used: u64,
    elapsed: Duration,
) {
    stats.send(StatsUpdateMessage {
        sql,
        elapsed,
        rows_read,
        rows_written,
        mem_used,
    });
}

impl<W: Wal> Connection<W> {
    fn new<T: WalManager<Wal = W>>(
        path: &Path,
        extensions: Arc<[PathBuf]>,
        wal_manager: T,
        stats: Arc<Stats>,
        config_store: MetaStoreHandle,
        builder_config: QueryBuilderConfig,
        current_frame_no_receiver: watch::Receiver<Option<FrameNo>>,
        block_writes: Arc<AtomicBool>,
        resolve_attach_path: ResolveNamespacePathFn,
    ) -> Result<Self> {
        let conn = open_conn_active_checkpoint(
            path,
            wal_manager,
            None,
            builder_config.auto_checkpoint,
            builder_config.encryption_config.clone(),
        )?;

        let config = config_store.get();
        conn.pragma_update(None, "max_page_count", config.max_db_pages)?;
        conn.set_limit(
            rusqlite::limits::Limit::SQLITE_LIMIT_LENGTH,
            config.max_row_size as i32,
        );

        unsafe {
            const MAX_RETRIES: c_int = 8;
            extern "C" fn do_nothing(_: *mut c_void, n: c_int) -> c_int {
                (n < MAX_RETRIES) as _
            }
            libsql_sys::ffi::sqlite3_busy_handler(
                conn.handle(),
                Some(do_nothing),
                std::ptr::null_mut(),
            );
        }

        let this = Self {
            conn,
            stats,
            config_store,
            builder_config,
            current_frame_no_receiver,
            block_writes,
            resolve_attach_path,
            forced_rollback: false,
        };

        for ext in extensions.iter() {
            unsafe {
                let _guard = rusqlite::LoadExtensionGuard::new(&this.conn).unwrap();
                if let Err(e) = this.conn.load_extension(ext, None) {
                    tracing::error!("failed to load extension: {}", ext.display());
                    Err(e)?;
                }
                tracing::trace!("Loaded extension {}", ext.display());
            }
        }

        Ok(this)
    }

    fn run<B: QueryResultBuilder>(
        this: Arc<Mutex<Self>>,
        pgm: Program,
        mut builder: B,
    ) -> Result<B> {
        let (config, stats, block_writes, resolve_attach_path) = {
            let lock = this.lock();
            let config = lock.config_store.get();
            let stats = lock.stats.clone();
            let block_writes = lock.block_writes.clone();
            let resolve_attach_path = lock.resolve_attach_path.clone();

            (config, stats, block_writes, resolve_attach_path)
        };

        builder.init(&this.lock().builder_config)?;
        let mut vm = Vm::new(
            builder,
            &pgm,
            move |stmt_kind| {
                let should_block = match stmt_kind {
                    StmtKind::Read | StmtKind::TxnBegin => config.block_reads,
                    StmtKind::Write => {
                        config.block_reads
                            || config.block_writes
                            || block_writes.load(Ordering::SeqCst)
                    }
                    StmtKind::DDL => config.block_reads || config.block_writes,
                    StmtKind::TxnEnd
                    | StmtKind::Release
                    | StmtKind::Savepoint
                    | StmtKind::Detach
                    | StmtKind::Attach(_) => false,
                };

                (
                    should_block,
                    should_block.then(|| config.block_reason.clone()).flatten(),
                )
            },
            move |sql, rows_read, rows_written, mem_used, elapsed| {
                update_stats(&stats, sql, rows_read, rows_written, mem_used, elapsed)
            },
            resolve_attach_path,
        );

        let mut has_timeout = false;
        while !vm.finished() {
            let mut conn = this.lock();

            if conn.forced_rollback {
                has_timeout = true;
                conn.forced_rollback = false;
            }

            // once there was a timeout, invalidate all the program steps
            if has_timeout {
                vm.builder().begin_step()?;
                vm.builder().step_error(Error::LibSqlTxTimeout)?;
                vm.builder().finish_step(0, None)?;
                vm.advance();
                continue;
            }

            let conn = conn.conn.deref();
            vm.step(conn)?;
        }

        {
            let mut lock = this.lock();
            let is_autocommit = lock.conn.is_autocommit();
            let current_fno = *lock.current_frame_no_receiver.borrow_and_update();
            vm.builder().finish(current_fno, is_autocommit)?;
        }

        Ok(vm.into_builder())
    }

    fn rollback(&self) {
        if let Err(e) = self.conn.execute("ROLLBACK", ()) {
            tracing::error!("failed to rollback: {e}");
        }
    }

    pub(super) fn force_rollback(&mut self) {
        if !self.forced_rollback {
            self.rollback();
            self.forced_rollback = true;
        }
    }

    fn checkpoint(&self) -> Result<()> {
        let start = Instant::now();
        self.conn
            .query_row("PRAGMA wal_checkpoint(TRUNCATE)", (), |_| Ok(()))?;
        WAL_CHECKPOINT_COUNT.increment(1);
        histogram!("libsql_server_wal_checkpoint_time", start.elapsed());
        Ok(())
    }

    fn vacuum_if_needed(&self) -> Result<()> {
        let page_count = self
            .conn
            .query_row("PRAGMA page_count", (), |row| row.get::<_, i64>(0))?;
        let freelist_count = self
            .conn
            .query_row("PRAGMA freelist_count", (), |row| row.get::<_, i64>(0))?;
        // NOTICE: don't bother vacuuming if we don't have at least 256MiB of data
        if page_count >= 65536 && freelist_count * 2 > page_count {
            tracing::info!("Vacuuming: pages={page_count} freelist={freelist_count}");
            self.conn.execute("VACUUM", ())?;
        } else {
            tracing::trace!("Not vacuuming: pages={page_count} freelist={freelist_count}");
        }
        VACUUM_COUNT.increment(1);
        Ok(())
    }

    fn describe(&self, sql: &str) -> crate::Result<DescribeResponse> {
        let stmt = self.conn.prepare(sql)?;

        let params = (1..=stmt.parameter_count())
            .map(|param_i| {
                let name = stmt.parameter_name(param_i).map(|n| n.into());
                DescribeParam { name }
            })
            .collect();

        let cols = stmt
            .columns()
            .into_iter()
            .map(|col| {
                let name = col.name().into();
                let decltype = col.decl_type().map(|t| t.into());
                DescribeCol { name, decltype }
            })
            .collect();

        let is_explain = stmt.is_explain() != 0;
        let is_readonly = stmt.readonly();
        Ok(DescribeResponse {
            params,
            cols,
            is_explain,
            is_readonly,
        })
    }

    fn is_autocommit(&self) -> bool {
        self.conn.is_autocommit()
    }
}

#[async_trait::async_trait]
impl<W> super::Connection for LibSqlConnection<W>
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
        PROGRAM_EXEC_COUNT.increment(1);

        check_program_auth(&ctx, &pgm, &self.inner.lock().config_store.get())?;
        let conn = self.inner.clone();
        BLOCKING_RT
            .spawn_blocking(move || Connection::run(conn, pgm, builder))
            .await
            .unwrap()
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
}

#[cfg(test)]
mod test {
    use itertools::Itertools;
    use libsql_sys::wal::wrapper::PassthroughWalWrapper;
    use libsql_sys::wal::Sqlite3Wal;
    use rand::Rng;
    use tempfile::tempdir;
    use tokio::task::JoinSet;

    use crate::auth::Authenticated;
    use crate::connection::{Connection as _, TXN_TIMEOUT};
    use crate::namespace::meta_store::{metastore_connection_maker, MetaStore};
    use crate::namespace::NamespaceName;
    use crate::query_result_builder::test::{test_driver, TestBuilder};
    use crate::query_result_builder::QueryResultBuilder;
    use crate::DEFAULT_AUTO_CHECKPOINT;

    use super::*;

    fn setup_test_conn() -> Arc<Mutex<Connection<Sqlite3Wal>>> {
        let conn = Connection {
            conn: libsql_sys::Connection::test(),
            stats: Arc::new(Stats::default()),
            config_store: MetaStoreHandle::new_test(),
            builder_config: QueryBuilderConfig::default(),
            current_frame_no_receiver: watch::channel(None).1,
            block_writes: Default::default(),
            resolve_attach_path: Arc::new(|_| unreachable!()),
            forced_rollback: false,
        };

        let conn = Arc::new(Mutex::new(conn));

        let stmts = std::iter::once("create table test (x)")
            .chain(std::iter::repeat("insert into test values ('hello world')").take(100))
            .collect_vec();
        Connection::run(conn.clone(), Program::seq(&stmts), TestBuilder::default()).unwrap();

        conn
    }

    #[test]
    fn test_libsql_conn_builder_driver() {
        test_driver(1000, |b| {
            let conn = setup_test_conn();
            Connection::run(conn, Program::seq(&["select * from test"]), b)
        })
    }

    #[ignore = "the new implementation doesn't steal if nobody is trying to acquire a write lock"]
    #[tokio::test]
    async fn txn_timeout_no_stealing() {
        let tmp = tempdir().unwrap();
        let make_conn = MakeLibSqlConn::new(
            tmp.path().into(),
            PassthroughWalWrapper,
            Default::default(),
            MetaStoreHandle::load(tmp.path()).unwrap(),
            Arc::new([]),
            100000000,
            100000000,
            DEFAULT_AUTO_CHECKPOINT,
            watch::channel(None).1,
            None,
            Default::default(),
            Arc::new(|_| unreachable!()),
        )
        .await
        .unwrap();

        tokio::time::pause();
        let conn = make_conn.make_connection().await.unwrap();
        let _builder = Connection::run(
            conn.inner.clone(),
            Program::seq(&["BEGIN IMMEDIATE"]),
            TestBuilder::default(),
        )
        .unwrap();
        assert!(!conn.inner.lock().conn.is_autocommit());

        tokio::time::sleep(Duration::from_secs(1)).await;

        let builder = Connection::run(
            conn.inner.clone(),
            Program::seq(&["create table test (c)"]),
            TestBuilder::default(),
        )
        .unwrap();
        assert!(!conn.is_autocommit().await.unwrap());
        assert!(matches!(builder.into_ret()[0], Err(Error::LibSqlTxTimeout)));
    }

    #[tokio::test]
    /// A bunch of txn try to acquire the lock, and never release it. They will try to steal the
    /// lock one after the other. All txn should eventually acquire the write lock
    async fn serialized_txn_timeouts() {
        let tmp = tempdir().unwrap();
        let make_conn = MakeLibSqlConn::new(
            tmp.path().into(),
            PassthroughWalWrapper,
            Default::default(),
            MetaStoreHandle::load(tmp.path()).unwrap(),
            Arc::new([]),
            100000000,
            100000000,
            DEFAULT_AUTO_CHECKPOINT,
            watch::channel(None).1,
            None,
            Default::default(),
            Arc::new(|_| unreachable!()),
        )
        .await
        .unwrap();

        let mut set = JoinSet::new();
        for _ in 0..10 {
            let conn = make_conn.make_connection().await.unwrap();
            set.spawn_blocking(move || {
                let builder = Connection::run(
                    conn.inner.clone(),
                    Program::seq(&["BEGIN IMMEDIATE"]),
                    TestBuilder::default(),
                )
                .unwrap();
                let ret = &builder.into_ret()[0];
                assert!(
                    (ret.is_ok() && !conn.inner.lock().conn.is_autocommit())
                        || (matches!(ret, Err(Error::RusqliteErrorExtended(_, 5)))
                            && conn.inner.lock().conn.is_autocommit())
                );
            });
        }

        tokio::time::pause();

        while let Some(ret) = set.join_next().await {
            assert!(ret.is_ok());
            // advance time by a bit more than the txn timeout
            tokio::time::advance(TXN_TIMEOUT + Duration::from_millis(100)).await;
        }
    }

    #[tokio::test]
    /// verify that releasing a txn before the timeout
    async fn release_before_timeout() {
        let tmp = tempdir().unwrap();
        let make_conn = MakeLibSqlConn::new(
            tmp.path().into(),
            PassthroughWalWrapper,
            Default::default(),
            MetaStoreHandle::load(tmp.path()).unwrap(),
            Arc::new([]),
            100000000,
            100000000,
            DEFAULT_AUTO_CHECKPOINT,
            watch::channel(None).1,
            None,
            Default::default(),
            Arc::new(|_| unreachable!()),
        )
        .await
        .unwrap();

        let conn1 = make_conn.make_connection().await.unwrap();
        tokio::task::spawn_blocking({
            let conn = conn1.clone();
            move || {
                let builder = Connection::run(
                    conn.inner.clone(),
                    Program::seq(&["BEGIN IMMEDIATE"]),
                    TestBuilder::default(),
                )
                .unwrap();
                assert!(!conn.inner.lock().is_autocommit());
                assert!(builder.into_ret()[0].is_ok());
            }
        })
        .await
        .unwrap();

        let conn2 = make_conn.make_connection().await.unwrap();
        let handle = tokio::task::spawn_blocking({
            let conn = conn2.clone();
            move || {
                let before = Instant::now();
                let builder = Connection::run(
                    conn.inner.clone(),
                    Program::seq(&["BEGIN IMMEDIATE"]),
                    TestBuilder::default(),
                )
                .unwrap();
                assert!(!conn.inner.lock().is_autocommit());
                assert!(builder.into_ret()[0].is_ok());
                before.elapsed()
            }
        });

        let wait_time = TXN_TIMEOUT / 10;
        tokio::time::sleep(wait_time).await;

        tokio::task::spawn_blocking({
            let conn = conn1.clone();
            move || {
                let builder = Connection::run(
                    conn.inner.clone(),
                    Program::seq(&["COMMIT"]),
                    TestBuilder::default(),
                )
                .unwrap();
                assert!(conn.inner.lock().is_autocommit());
                assert!(builder.into_ret()[0].is_ok());
            }
        })
        .await
        .unwrap();

        let elapsed = handle.await.unwrap();

        let epsilon = Duration::from_millis(100);
        assert!((wait_time..wait_time + epsilon).contains(&elapsed));
    }

    /// The goal of this test is to run many concurrent transaction and hopefully catch a bug in
    /// the lock stealing code. If this test becomes flaky check out the lock stealing code.
    #[tokio::test]
    async fn test_many_concurrent() {
        let tmp = tempdir().unwrap();
        let make_conn = MakeLibSqlConn::new(
            tmp.path().into(),
            PassthroughWalWrapper,
            Default::default(),
            MetaStoreHandle::load(tmp.path()).unwrap(),
            Arc::new([]),
            100000000,
            100000000,
            DEFAULT_AUTO_CHECKPOINT,
            watch::channel(None).1,
            None,
            Default::default(),
            Arc::new(|_| unreachable!()),
        )
        .await
        .unwrap();

        let conn = make_conn.make_connection().await.unwrap();
        let (maker, manager) = metastore_connection_maker(None, tmp.path()).await.unwrap();
        let ctx = RequestContext::new(
            Authenticated::FullAccess,
            NamespaceName::default(),
            MetaStore::new(Default::default(), tmp.path(), maker().unwrap(), manager)
                .await
                .unwrap(),
        );
        conn.execute_program(
            Program::seq(&["CREATE TABLE test (x)"]),
            ctx.clone(),
            TestBuilder::default(),
            None,
        )
        .await
        .unwrap();
        let run_conn = |maker: Arc<MakeLibSqlConn<PassthroughWalWrapper>>| {
            let ctx = ctx.clone();
            async move {
                for _ in 0..1000 {
                    let conn = maker.make_connection().await.unwrap();
                    let pgm = Program::seq(&["BEGIN IMMEDIATE", "INSERT INTO test VALUES (42)"]);
                    let res = conn
                        .execute_program(pgm, ctx.clone(), TestBuilder::default(), None)
                        .await
                        .unwrap()
                        .into_ret();
                    for result in res {
                        result.unwrap();
                    }
                    // with 99% change, commit the txn
                    if rand::thread_rng().gen_range(0..100) > 1 {
                        let pgm = Program::seq(&["INSERT INTO test VALUES (43)", "COMMIT"]);
                        let res = conn
                            .execute_program(pgm, ctx.clone(), TestBuilder::default(), None)
                            .await
                            .unwrap()
                            .into_ret();
                        for result in res {
                            result.unwrap();
                        }
                    }
                }
            }
        };

        let maker = Arc::new(make_conn);
        let mut join_set = JoinSet::new();
        for _ in 0..3 {
            join_set.spawn(run_conn(maker.clone()));
        }

        let join_all = async move {
            while let Some(next) = join_set.join_next().await {
                next.unwrap();
            }
        };

        tokio::time::timeout(Duration::from_secs(60), join_all)
            .await
            .expect("timed out running connections");
    }

    #[tokio::test]
    /// verify that releasing a txn before the timeout
    async fn force_rollback_reset() {
        let tmp = tempdir().unwrap();
        let make_conn = MakeLibSqlConn::new(
            tmp.path().into(),
            PassthroughWalWrapper,
            Default::default(),
            MetaStoreHandle::load(tmp.path()).unwrap(),
            Arc::new([]),
            100000000,
            100000000,
            DEFAULT_AUTO_CHECKPOINT,
            watch::channel(None).1,
            None,
            Default::default(),
            Arc::new(|_| unreachable!()),
        )
        .await
        .unwrap();

        let conn1 = make_conn.make_connection().await.unwrap();
        tokio::task::spawn_blocking({
            let conn = conn1.clone();
            move || {
                let builder = Connection::run(
                    conn.inner.clone(),
                    Program::seq(&["BEGIN IMMEDIATE"]),
                    TestBuilder::default(),
                )
                .unwrap();
                assert!(!conn.inner.lock().is_autocommit());
                assert!(builder.into_ret()[0].is_ok());
            }
        })
        .await
        .unwrap();

        let conn2 = make_conn.make_connection().await.unwrap();
        tokio::task::spawn_blocking({
            let conn = conn2.clone();
            move || {
                let before = Instant::now();
                let builder = Connection::run(
                    conn.inner.clone(),
                    Program::seq(&["BEGIN IMMEDIATE"]),
                    TestBuilder::default(),
                )
                .unwrap();
                assert!(!conn.inner.lock().is_autocommit());
                assert!(builder.into_ret()[0].is_ok());
                before.elapsed()
            }
        })
        .await
        .unwrap();

        tokio::time::sleep(TXN_TIMEOUT * 2).await;

        tokio::task::spawn_blocking({
            let conn = conn1.clone();
            move || {
                let builder = Connection::run(
                    conn.inner.clone(),
                    Program::seq(&["SELECT 1;"]),
                    TestBuilder::default(),
                )
                .unwrap();
                assert!(conn.inner.lock().is_autocommit());
                // timeout
                assert!(builder.into_ret()[0].is_err());

                let builder = Connection::run(
                    conn.inner.clone(),
                    Program::seq(&["SELECT 1;"]),
                    TestBuilder::default(),
                )
                .unwrap();
                assert!(conn.inner.lock().is_autocommit());
                // state reset
                assert!(builder.into_ret()[0].is_ok());
            }
        })
        .await
        .unwrap();
    }
}
