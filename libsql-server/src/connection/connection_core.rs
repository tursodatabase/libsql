use std::ffi::{c_int, c_void};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use libsql_sys::wal::{Wal, WalManager};
use metrics::histogram;
use parking_lot::Mutex;
use tokio::sync::watch;

use crate::connection::legacy::open_conn_active_checkpoint;
use crate::error::Error;
use crate::metrics::{PROGRAM_EXEC_COUNT, QUERY_CANCELED, VACUUM_COUNT, WAL_CHECKPOINT_COUNT};
use crate::namespace::broadcasters::BroadcasterHandle;
use crate::namespace::meta_store::MetaStoreHandle;
use crate::namespace::ResolveNamespacePathFn;
use crate::query_analysis::StmtKind;
use crate::query_result_builder::{QueryBuilderConfig, QueryResultBuilder};
use crate::replication::FrameNo;
use crate::stats::{Stats, StatsUpdateMessage};
use crate::{Result, BLOCKING_RT};

use super::config::DatabaseConfig;
use super::program::{DescribeCol, DescribeParam, DescribeResponse, Program, Vm};

/// The base connection type, shared between legacy and libsql-wal implementations
pub(super) struct CoreConnection<W> {
    conn: libsql_sys::Connection<W>,
    stats: Arc<Stats>,
    config_store: MetaStoreHandle,
    builder_config: QueryBuilderConfig,
    current_frame_no_receiver: watch::Receiver<Option<FrameNo>>,
    block_writes: Arc<AtomicBool>,
    resolve_attach_path: ResolveNamespacePathFn,
    forced_rollback: bool,
    broadcaster: BroadcasterHandle,
    hooked: bool,
    canceled: Arc<AtomicBool>,
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

impl<W: Wal + Send + 'static> CoreConnection<W> {
    pub(super) fn new<T: WalManager<Wal = W>>(
        path: &Path,
        extensions: Arc<[PathBuf]>,
        wal_manager: T,
        stats: Arc<Stats>,
        broadcaster: BroadcasterHandle,
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
        tracing::debug!("setting PRAGMA synchronous to {}", config.durability_mode);
        conn.pragma_update(None, "synchronous", config.durability_mode)?;

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

        let canceled = Arc::new(AtomicBool::new(false));

        conn.progress_handler(100, {
            let canceled = canceled.clone();
            Some(move || {
                let canceled = canceled.load(Ordering::Relaxed);
                if canceled {
                    QUERY_CANCELED.increment(1);
                    tracing::trace!("request canceled");
                }
                canceled
            })
        });

        let this = Self {
            conn,
            stats,
            config_store,
            builder_config,
            current_frame_no_receiver,
            block_writes,
            resolve_attach_path,
            forced_rollback: false,
            broadcaster,
            hooked: false,
            canceled,
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

    pub(super) fn raw_mut(&mut self) -> &mut libsql_sys::Connection<W> {
        &mut self.conn
    }

    pub(super) fn raw(&self) -> &libsql_sys::Connection<W> {
        &self.conn
    }

    pub(super) fn config(&self) -> Arc<DatabaseConfig> {
        self.config_store.get()
    }

    pub(super) async fn run_async<B: QueryResultBuilder>(
        this: Arc<Mutex<Self>>,
        pgm: Program,
        builder: B,
    ) -> Result<B> {
        struct Bomb {
            canceled: Arc<AtomicBool>,
            defused: bool,
        }

        impl Drop for Bomb {
            fn drop(&mut self) {
                if !self.defused {
                    tracing::trace!("cancelling request");
                    self.canceled.store(true, Ordering::Relaxed);
                }
            }
        }

        let canceled = {
            let cancelled = this.lock().canceled.clone();
            cancelled.store(false, Ordering::Relaxed);
            cancelled
        };

        PROGRAM_EXEC_COUNT.increment(1);

        // create the bomb right before spawning the blocking task.
        let mut bomb = Bomb {
            canceled,
            defused: false,
        };
        let ret = BLOCKING_RT
            .spawn_blocking(move || CoreConnection::run(this, pgm, builder))
            .await
            .unwrap();

        bomb.defused = true;

        ret
    }

    pub(super) fn run<B: QueryResultBuilder>(
        this: Arc<Mutex<Self>>,
        pgm: Program,
        mut builder: B,
    ) -> Result<B> {
        let (config, stats, block_writes, resolve_attach_path) = {
            let mut lock = this.lock();
            let config = lock.config_store.get();
            let stats = lock.stats.clone();
            let block_writes = lock.block_writes.clone();
            let resolve_attach_path = lock.resolve_attach_path.clone();

            lock.update_hooks();

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

            vm.step(&conn.raw())?;
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

    pub(super) fn checkpoint(&self) -> Result<()> {
        let start = Instant::now();
        self.conn
            .query_row("PRAGMA wal_checkpoint(TRUNCATE)", (), |row| {
                let status: i32 = row.get(0)?;
                let wal_frames: i32 = row.get(1)?;
                let moved_frames: i32 = row.get(2)?;
                tracing::info!(
                    "WAL checkpoint successful, status: {}, WAL frames: {}, moved frames: {}",
                    status,
                    wal_frames,
                    moved_frames
                );
                Ok(())
            })?;
        WAL_CHECKPOINT_COUNT.increment(1);
        histogram!("libsql_server_wal_checkpoint_time", start.elapsed());
        Ok(())
    }

    pub(super) fn vacuum_if_needed(&self) -> Result<()> {
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

    pub(super) fn describe(&self, sql: &str) -> crate::Result<DescribeResponse> {
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

    pub(super) fn is_autocommit(&self) -> bool {
        self.conn.is_autocommit()
    }

    fn update_hooks(&mut self) {
        let (update_fn, commit_fn, rollback_fn) = if self.hooked {
            if self.broadcaster.active() {
                return;
            }
            self.hooked = false;
            (None, None, None)
        } else {
            let Some(broadcaster) = self.broadcaster.get() else {
                return;
            };

            let update = broadcaster.clone();
            let update_fn = Some(move |action: _, _: &_, table: &_, _| {
                update.notify(table, action);
            });

            let commit = broadcaster.clone();
            let commit_fn = Some(move || {
                commit.commit();
                false // allow commit to go through
            });

            let rollback = broadcaster;
            let rollback_fn = Some(move || rollback.rollback());
            (update_fn, commit_fn, rollback_fn)
        };

        self.conn.update_hook(update_fn);
        self.conn.commit_hook(commit_fn);
        self.conn.rollback_hook(rollback_fn);
    }
}

#[cfg(test)]
mod test {
    use itertools::Itertools;
    #[cfg(not(feature = "durable-wal"))]
    use libsql_sys::wal::either::Either as EitherWAL;
    #[cfg(feature = "durable-wal")]
    use libsql_sys::wal::either::Either3 as EitherWAL;
    use libsql_sys::wal::wrapper::PassthroughWalWrapper;
    use libsql_sys::wal::{Sqlite3Wal, Sqlite3WalManager};
    use rand::Rng;
    use tempfile::tempdir;
    use tokio::task::JoinSet;
    use tokio::time::Instant;

    use crate::auth::Authenticated;
    use crate::connection::legacy::MakeLegacyConnection;
    use crate::connection::{Connection as _, RequestContext, TXN_TIMEOUT};
    use crate::namespace::meta_store::{metastore_connection_maker, MetaStore};
    use crate::namespace::NamespaceName;
    use crate::query_result_builder::test::{test_driver, TestBuilder};
    use crate::query_result_builder::QueryResultBuilder;
    use crate::DEFAULT_AUTO_CHECKPOINT;

    use super::*;

    fn setup_test_conn() -> Arc<Mutex<CoreConnection<Sqlite3Wal>>> {
        let conn = CoreConnection {
            conn: libsql_sys::Connection::test(),
            stats: Arc::new(Stats::default()),
            config_store: MetaStoreHandle::new_test(),
            builder_config: QueryBuilderConfig::default(),
            current_frame_no_receiver: watch::channel(None).1,
            block_writes: Default::default(),
            resolve_attach_path: Arc::new(|_| unreachable!()),
            forced_rollback: false,
            broadcaster: Default::default(),
            hooked: false,
            canceled: Arc::new(false.into()),
        };

        let conn = Arc::new(Mutex::new(conn));

        let stmts = std::iter::once("create table test (x)")
            .chain(std::iter::repeat("insert into test values ('hello world')").take(100))
            .collect_vec();
        CoreConnection::run(conn.clone(), Program::seq(&stmts), TestBuilder::default()).unwrap();

        conn
    }

    #[test]
    fn test_libsql_conn_builder_driver() {
        test_driver(1000, |b| {
            let conn = setup_test_conn();
            CoreConnection::run(conn, Program::seq(&["select * from test"]), b)
        })
    }

    #[ignore = "the new implementation doesn't steal if nobody is trying to acquire a write lock"]
    #[tokio::test]
    async fn txn_timeout_no_stealing() {
        let tmp = tempdir().unwrap();
        let make_conn = MakeLegacyConnection::new(
            tmp.path().into(),
            PassthroughWalWrapper,
            Default::default(),
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
            Arc::new(|| EitherWAL::A(Sqlite3WalManager::default())),
        )
        .await
        .unwrap();

        tokio::time::pause();
        let conn = make_conn.make_connection().await.unwrap();
        let _builder = CoreConnection::run(
            conn.inner.clone(),
            Program::seq(&["BEGIN IMMEDIATE"]),
            TestBuilder::default(),
        )
        .unwrap();
        assert!(!conn.inner.lock().conn.is_autocommit());

        tokio::time::sleep(Duration::from_secs(1)).await;

        let builder = CoreConnection::run(
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
        let make_conn = MakeLegacyConnection::new(
            tmp.path().into(),
            PassthroughWalWrapper,
            Default::default(),
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
            Arc::new(|| EitherWAL::A(Sqlite3WalManager::default())),
        )
        .await
        .unwrap();

        let mut set = JoinSet::new();
        for _ in 0..10 {
            let conn = make_conn.make_connection().await.unwrap();
            set.spawn_blocking(move || {
                let builder = CoreConnection::run(
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
        let make_conn = MakeLegacyConnection::new(
            tmp.path().into(),
            PassthroughWalWrapper,
            Default::default(),
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
            Arc::new(|| EitherWAL::A(Sqlite3WalManager::default())),
        )
        .await
        .unwrap();

        let conn1 = make_conn.make_connection().await.unwrap();
        tokio::task::spawn_blocking({
            let conn = conn1.clone();
            move || {
                let builder = CoreConnection::run(
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
                let builder = CoreConnection::run(
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
                let builder = CoreConnection::run(
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
        let make_conn = MakeLegacyConnection::new(
            tmp.path().into(),
            PassthroughWalWrapper,
            Default::default(),
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
            Arc::new(|| EitherWAL::A(Sqlite3WalManager::default())),
        )
        .await
        .unwrap();

        let conn = make_conn.make_connection().await.unwrap();
        let (maker, manager) = metastore_connection_maker(None, tmp.path()).await.unwrap();
        let ctx = RequestContext::new(
            Authenticated::FullAccess,
            NamespaceName::default(),
            MetaStore::new(
                Default::default(),
                tmp.path(),
                maker().unwrap(),
                manager,
                crate::database::DatabaseKind::Primary,
            )
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
        let run_conn = |maker: Arc<MakeLegacyConnection<PassthroughWalWrapper>>| {
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
        let make_conn = MakeLegacyConnection::new(
            tmp.path().into(),
            PassthroughWalWrapper,
            Default::default(),
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
            Arc::new(|| EitherWAL::A(Sqlite3WalManager::default())),
        )
        .await
        .unwrap();

        let conn1 = make_conn.make_connection().await.unwrap();
        tokio::task::spawn_blocking({
            let conn = conn1.clone();
            move || {
                let builder = CoreConnection::run(
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
                let builder = CoreConnection::run(
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
                let builder = CoreConnection::run(
                    conn.inner.clone(),
                    Program::seq(&["SELECT 1;"]),
                    TestBuilder::default(),
                )
                .unwrap();
                assert!(conn.inner.lock().is_autocommit());
                // timeout
                assert!(builder.into_ret()[0].is_err());

                let builder = CoreConnection::run(
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
