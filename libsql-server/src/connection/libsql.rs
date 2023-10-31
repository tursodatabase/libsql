use std::ffi::{c_int, c_void};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use metrics::histogram;
use parking_lot::{Mutex, RwLock};
use rusqlite::{DatabaseName, ErrorCode, OpenFlags, StatementStatus};
use sqld_libsql_bindings::wal_hook::{TransparentMethods, WalMethodsHook};
use tokio::sync::{watch, Notify};
use tokio::time::{Duration, Instant};

use crate::auth::{Authenticated, Authorized, Permission};
use crate::error::Error;
use crate::libsql_bindings::wal_hook::WalHook;
use crate::metrics::{READ_QUERY_COUNT, VACUUM_COUNT, WAL_CHECKPOINT_COUNT, WRITE_QUERY_COUNT};
use crate::query::Query;
use crate::query_analysis::{State, StmtKind};
use crate::query_result_builder::{QueryBuilderConfig, QueryResultBuilder};
use crate::replication::FrameNo;
use crate::stats::Stats;
use crate::Result;

use super::config::DatabaseConfigStore;
use super::program::{Cond, DescribeCol, DescribeParam, DescribeResponse, DescribeResult};
use super::{MakeConnection, Program, Step, TXN_TIMEOUT};

pub struct MakeLibSqlConn<W: WalHook + 'static> {
    db_path: PathBuf,
    hook: &'static WalMethodsHook<W>,
    ctx_builder: Box<dyn Fn() -> W::Context + Sync + Send + 'static>,
    stats: Arc<Stats>,
    config_store: Arc<DatabaseConfigStore>,
    extensions: Arc<[PathBuf]>,
    max_response_size: u64,
    max_total_response_size: u64,
    auto_checkpoint: u32,
    current_frame_no_receiver: watch::Receiver<Option<FrameNo>>,
    state: Arc<TxnState<W>>,
    /// In wal mode, closing the last database takes time, and causes other databases creation to
    /// return sqlite busy. To mitigate that, we hold on to one connection
    _db: Option<LibSqlConnection<W>>,
}

impl<W: WalHook + 'static> MakeLibSqlConn<W>
where
    W: WalHook + 'static + Sync + Send,
    W::Context: Send + 'static,
{
    #[allow(clippy::too_many_arguments)]
    pub async fn new<F>(
        db_path: PathBuf,
        hook: &'static WalMethodsHook<W>,
        ctx_builder: F,
        stats: Arc<Stats>,
        config_store: Arc<DatabaseConfigStore>,
        extensions: Arc<[PathBuf]>,
        max_response_size: u64,
        max_total_response_size: u64,
        auto_checkpoint: u32,
        current_frame_no_receiver: watch::Receiver<Option<FrameNo>>,
    ) -> Result<Self>
    where
        F: Fn() -> W::Context + Sync + Send + 'static,
    {
        let mut this = Self {
            db_path,
            hook,
            ctx_builder: Box::new(ctx_builder),
            stats,
            config_store,
            extensions,
            max_response_size,
            max_total_response_size,
            auto_checkpoint,
            current_frame_no_receiver,
            _db: None,
            state: Default::default(),
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
            self.hook,
            (self.ctx_builder)(),
            self.stats.clone(),
            self.config_store.clone(),
            QueryBuilderConfig {
                max_size: Some(self.max_response_size),
                max_total_size: Some(self.max_total_response_size),
                auto_checkpoint: self.auto_checkpoint,
            },
            self.current_frame_no_receiver.clone(),
            self.state.clone(),
        )
        .await
    }
}

#[async_trait::async_trait]
impl<W> MakeConnection for MakeLibSqlConn<W>
where
    W: WalHook + 'static + Sync + Send,
    W::Context: Send + 'static,
{
    type Connection = LibSqlConnection<W>;

    async fn create(&self) -> Result<Self::Connection, Error> {
        self.make_connection().await
    }
}

#[derive(Clone)]
pub struct LibSqlConnection<W: WalHook> {
    inner: Arc<Mutex<Connection<W>>>,
}

impl<W: WalHook> std::fmt::Debug for LibSqlConnection<W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.inner.try_lock() {
            Some(conn) => {
                write!(f, "{conn:?}")
            }
            None => write!(f, "<locked>"),
        }
    }
}

pub fn open_conn<W>(
    path: &Path,
    wal_methods: &'static WalMethodsHook<W>,
    hook_ctx: W::Context,
    flags: Option<OpenFlags>,
    auto_checkpoint: u32,
) -> Result<sqld_libsql_bindings::Connection<W>, rusqlite::Error>
where
    W: WalHook,
{
    let flags = flags.unwrap_or(
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_URI
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    );
    sqld_libsql_bindings::Connection::open(
        path.join("data"),
        flags,
        wal_methods,
        hook_ctx,
        auto_checkpoint,
    )
}

impl<W> LibSqlConnection<W>
where
    W: WalHook,
    W::Context: Send,
{
    pub async fn new(
        path: impl AsRef<Path> + Send + 'static,
        extensions: Arc<[PathBuf]>,
        wal_hook: &'static WalMethodsHook<W>,
        hook_ctx: W::Context,
        stats: Arc<Stats>,
        config_store: Arc<DatabaseConfigStore>,
        builder_config: QueryBuilderConfig,
        current_frame_no_receiver: watch::Receiver<Option<FrameNo>>,
        state: Arc<TxnState<W>>,
    ) -> crate::Result<Self> {
        let max_db_size = config_store.get().max_db_pages;
        let conn = tokio::task::spawn_blocking(move || -> crate::Result<_> {
            let conn = Connection::new(
                path.as_ref(),
                extensions,
                wal_hook,
                hook_ctx,
                stats,
                config_store,
                builder_config,
                current_frame_no_receiver,
                state,
            )?;
            conn.conn
                .pragma_update(None, "max_page_count", max_db_size)?;
            Ok(conn)
        })
        .await
        .unwrap()?;

        Ok(Self {
            inner: Arc::new(Mutex::new(conn)),
        })
    }
}

struct Connection<W: WalHook = TransparentMethods> {
    conn: sqld_libsql_bindings::Connection<W>,
    stats: Arc<Stats>,
    config_store: Arc<DatabaseConfigStore>,
    builder_config: QueryBuilderConfig,
    current_frame_no_receiver: watch::Receiver<Option<FrameNo>>,
    // must be dropped after the connection because the connection refers to it
    state: Arc<TxnState<W>>,
    // current txn slot if any
    slot: Option<Arc<TxnSlot<W>>>,
}

impl<W: WalHook> std::fmt::Debug for Connection<W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection")
            .field("slot", &self.slot)
            .finish()
    }
}

/// A slot for holding the state of a transaction lock permit
struct TxnSlot<T: WalHook> {
    /// Pointer to the connection holding the lock. Used to rollback the transaction when the lock
    /// is stolen.
    conn: Arc<Mutex<Connection<T>>>,
    /// Time at which the transaction can be stolen
    created_at: tokio::time::Instant,
    /// The transaction lock was stolen
    is_stolen: AtomicBool,
}

impl<T: WalHook> TxnSlot<T> {
    #[inline]
    fn expires_at(&self) -> Instant {
        self.created_at + TXN_TIMEOUT
    }

    fn abort(&self) {
        let conn = self.conn.lock();
        // we have a lock on the connection, we don't need mode than a
        // Relaxed store.
        conn.rollback();
        histogram!(
            "libsql_server_write_txn_duration",
            self.created_at.elapsed()
        )
        // WRITE_TXN_DURATION.record(self.created_at.elapsed());
    }
}

impl<T: WalHook> std::fmt::Debug for TxnSlot<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let stolen = self.is_stolen.load(Ordering::Relaxed);
        let time_left = self.expires_at().duration_since(Instant::now());
        write!(
            f,
            "(conn: {:?}, timeout: {time_left:?}, stolen: {stolen})",
            self.conn
        )
    }
}

/// The transaction state shared among all connections to the same database
#[derive(Debug)]
pub struct TxnState<T: WalHook> {
    /// Slot for the connection currently holding the transaction lock
    slot: RwLock<Option<Arc<TxnSlot<T>>>>,
    /// Notifier for when the lock gets dropped
    notify: Notify,
}

impl<W: WalHook> Default for TxnState<W> {
    fn default() -> Self {
        Self {
            slot: Default::default(),
            notify: Default::default(),
        }
    }
}

/// The lock-stealing busy handler.
/// Here is a detailed description of the algorithm:
/// - all connections to a database share a `TxnState`, that contains a `TxnSlot`
/// - when a connection acquire a write lock to the database, this is detected by monitoring the state of the
///   connection before and after the call thanks to [sqlite3_txn_state()](https://www.sqlite.org/c3ref/c_txn_none.html)
/// - if the connection acquired a write lock (txn state none/read -> write), a new txn slot is created. A clone of the
///   `TxnSlot` is placed in the `TxnState` shared with other connections to this database, while another clone is kept in
///   the transaction state. The TxnSlot contains: the instant at which the txn should timeout, a `is_stolen` flag, and a
///   pointer to the connection currently holding the lock.
/// - when another connection attempts to acquire the lock, the `busy_handler` callback will be called. The callback is being
///   passed the `TxnState` for the connection. The handler looks at the current slot to determine when the current txn will
///   timeout, and waits for that instant before retrying. The waiting handler can also be notified that the transaction has
///   been finished early.
/// - If the handler waits until the txn timeout and isn't notified of the termination of the txn, it will attempt to steal the lock.
///   This is done by calling rollback on the slot's txn, and marking the slot as stolen.
/// - When a connection notices that it's slot has been stolen, it returns a timedout error to the next request.
unsafe extern "C" fn busy_handler<W: WalHook>(state: *mut c_void, _retries: c_int) -> c_int {
    let state = &*(state as *mut TxnState<W>);
    let lock = state.slot.read();
    // we take a reference to the slot we will attempt to steal. this is to make sure that we
    // actually steal the correct lock.
    let slot = match &*lock {
        Some(slot) => slot.clone(),
        // fast path: there is no slot, try to acquire the lock again
        None => return 1,
    };

    tokio::runtime::Handle::current().block_on(async move {
        let timeout = {
            let slot = lock.as_ref().unwrap();
            let timeout_at = slot.expires_at();
            drop(lock);
            tokio::time::sleep_until(timeout_at)
        };

        tokio::select! {
            // The connection has notified us that it's txn has terminated, try to acquire again
            _ = state.notify.notified() => 1,
            // the current holder of the transaction has timedout, we will attempt to steal their
            // lock.
            _ = timeout => {
                // only a single connection gets to steal the lock, others retry
                if let Some(mut lock) = state.slot.try_write() {
                    // We check that slot wasn't already stolen, and that their is still a slot.
                    // The ordering is relaxed because the atomic is only set under the slot lock.
                    if slot.is_stolen.compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed).is_ok() {
                        // The connection holding the current txn will set itself as stolen when it
                        // detects a timeout, so if we arrive to this point, then there is
                        // necessarily a slot, and this slot has to be the one we attempted to
                        // steal.
                        assert!(lock.take().is_some());

                        slot.abort();
                        tracing::info!("stole transaction lock");
                    }
                }
                1
            }
        }
    })
}

fn value_size(val: &rusqlite::types::ValueRef) -> usize {
    use rusqlite::types::ValueRef;
    match val {
        ValueRef::Null => 0,
        ValueRef::Integer(_) => 8,
        ValueRef::Real(_) => 8,
        ValueRef::Text(s) => s.len(),
        ValueRef::Blob(b) => b.len(),
    }
}

impl<W: WalHook> Connection<W> {
    fn new(
        path: &Path,
        extensions: Arc<[PathBuf]>,
        wal_methods: &'static WalMethodsHook<W>,
        hook_ctx: W::Context,
        stats: Arc<Stats>,
        config_store: Arc<DatabaseConfigStore>,
        builder_config: QueryBuilderConfig,
        current_frame_no_receiver: watch::Receiver<Option<FrameNo>>,
        state: Arc<TxnState<W>>,
    ) -> Result<Self> {
        let conn = open_conn(
            path,
            wal_methods,
            hook_ctx,
            None,
            builder_config.auto_checkpoint,
        )?;

        // register the lock-stealing busy handler
        unsafe {
            let ptr = Arc::as_ptr(&state) as *mut _;
            rusqlite::ffi::sqlite3_busy_handler(conn.handle(), Some(busy_handler::<W>), ptr);
        }

        let this = Self {
            conn,
            stats,
            config_store,
            builder_config,
            current_frame_no_receiver,
            state,
            slot: None,
        };

        for ext in extensions.iter() {
            unsafe {
                let _guard = rusqlite::LoadExtensionGuard::new(&this.conn).unwrap();
                if let Err(e) = this.conn.load_extension(ext, None) {
                    tracing::error!("failed to load extension: {}", ext.display());
                    Err(e)?;
                }
                tracing::debug!("Loaded extension {}", ext.display());
            }
        }

        Ok(this)
    }

    fn run<B: QueryResultBuilder>(
        this: Arc<Mutex<Self>>,
        pgm: Program,
        mut builder: B,
    ) -> Result<(B, State)> {
        use rusqlite::TransactionState as Tx;

        let state = this.lock().state.clone();

        let mut results = Vec::with_capacity(pgm.steps.len());
        builder.init(&this.lock().builder_config)?;
        let mut previous_state = this
            .lock()
            .conn
            .transaction_state(Some(DatabaseName::Main))?;

        let mut has_timeout = false;
        for step in pgm.steps() {
            let mut lock = this.lock();

            if let Some(slot) = &lock.slot {
                if slot.is_stolen.load(Ordering::Relaxed) || Instant::now() > slot.expires_at() {
                    // we mark ourselves as stolen to notify any waiting lock thief.
                    slot.is_stolen.store(true, Ordering::Relaxed);
                    lock.rollback();
                    has_timeout = true;
                }
            }

            // once there was a timeout, invalidate all the program steps
            if has_timeout {
                lock.slot = None;
                builder.begin_step()?;
                builder.step_error(Error::LibSqlTxTimeout)?;
                builder.finish_step(0, None)?;
                continue;
            }

            let res = lock.execute_step(step, &results, &mut builder)?;

            let new_state = lock.conn.transaction_state(Some(DatabaseName::Main))?;
            match (previous_state, new_state) {
                // lock was upgraded, claim the slot
                (Tx::None | Tx::Read, Tx::Write) => {
                    let slot = Arc::new(TxnSlot {
                        conn: this.clone(),
                        created_at: Instant::now(),
                        is_stolen: AtomicBool::new(false),
                    });

                    lock.slot.replace(slot.clone());
                    state.slot.write().replace(slot);
                }
                // lock was downgraded, notify a waiter
                (Tx::Write, Tx::None | Tx::Read) => {
                    state.slot.write().take();
                    lock.slot.take();
                    state.notify.notify_one();
                }
                // nothing to do
                (_, _) => (),
            }

            previous_state = new_state;

            results.push(res);
        }

        builder.finish(*this.lock().current_frame_no_receiver.borrow_and_update())?;

        let state = if matches!(
            this.lock()
                .conn
                .transaction_state(Some(DatabaseName::Main))?,
            Tx::Read | Tx::Write
        ) {
            State::Txn
        } else {
            State::Init
        };

        Ok((builder, state))
    }

    fn execute_step(
        &mut self,
        step: &Step,
        results: &[bool],
        builder: &mut impl QueryResultBuilder,
    ) -> Result<bool> {
        builder.begin_step()?;

        let mut enabled = match step.cond.as_ref() {
            Some(cond) => match eval_cond(cond, results, self.is_autocommit()) {
                Ok(enabled) => enabled,
                Err(e) => {
                    builder.step_error(e).unwrap();
                    false
                }
            },
            None => true,
        };

        let (affected_row_count, last_insert_rowid) = if enabled {
            match self.execute_query(&step.query, builder) {
                // builder error interrupt the execution of query. we should exit immediately.
                Err(e @ Error::BuilderError(_)) => return Err(e),
                Err(mut e) => {
                    if let Error::RusqliteError(err) = e {
                        let extended_code =
                            unsafe { rusqlite::ffi::sqlite3_extended_errcode(self.conn.handle()) };

                        e = Error::RusqliteErrorExtended(err, extended_code as i32);
                    };

                    builder.step_error(e)?;
                    enabled = false;
                    (0, None)
                }
                Ok(x) => x,
            }
        } else {
            (0, None)
        };

        builder.finish_step(affected_row_count, last_insert_rowid)?;

        Ok(enabled)
    }

    fn execute_query(
        &self,
        query: &Query,
        builder: &mut impl QueryResultBuilder,
    ) -> Result<(u64, Option<i64>)> {
        tracing::trace!("executing query: {}", query.stmt.stmt);
        let start = Instant::now();
        let config = self.config_store.get();
        let blocked = match query.stmt.kind {
            StmtKind::Read | StmtKind::TxnBegin | StmtKind::Other => config.block_reads,
            StmtKind::Write => config.block_reads || config.block_writes,
            StmtKind::TxnEnd | StmtKind::Release | StmtKind::Savepoint => false,
        };
        if blocked {
            return Err(Error::Blocked(config.block_reason.clone()));
        }

        let mut stmt = self.conn.prepare(&query.stmt.stmt)?;
        if stmt.readonly() {
            READ_QUERY_COUNT.increment(1);
        } else {
            WRITE_QUERY_COUNT.increment(1);
        }

        let cols = stmt.columns();
        let cols_count = cols.len();
        builder.cols_description(cols.iter())?;
        drop(cols);

        query
            .params
            .bind(&mut stmt)
            .map_err(Error::LibSqlInvalidQueryParams)?;

        let mut qresult = stmt.raw_query();

        let mut values_total_bytes = 0;
        builder.begin_rows()?;
        while let Some(row) = qresult.next()? {
            builder.begin_row()?;
            for i in 0..cols_count {
                let val = row.get_ref(i)?;
                values_total_bytes += value_size(&val);
                builder.add_row_value(val)?;
            }
            builder.finish_row()?;
        }
        histogram!("libsql_server_returned_bytes", values_total_bytes as f64);

        builder.finish_rows()?;

        // sqlite3_changes() is only modified for INSERT, UPDATE or DELETE; it is not reset for SELECT,
        // but we want to return 0 in that case.
        let affected_row_count = match query.stmt.is_iud {
            true => self.conn.changes(),
            false => 0,
        };

        // sqlite3_last_insert_rowid() only makes sense for INSERTs into a rowid table. we can't detect
        // a rowid table, but at least we can detect an INSERT
        let last_insert_rowid = match query.stmt.is_insert {
            true => Some(self.conn.last_insert_rowid()),
            false => None,
        };

        drop(qresult);

        self.update_stats(query.stmt.stmt.clone(), &stmt, Instant::now() - start);

        Ok((affected_row_count, last_insert_rowid))
    }

    fn rollback(&self) {
        if let Err(e) = self.conn.execute("ROLLBACK", ()) {
            tracing::error!("failed to rollback: {e}");
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
            tracing::debug!("Not vacuuming: pages={page_count} freelist={freelist_count}");
        }
        VACUUM_COUNT.increment(1);
        Ok(())
    }

    fn update_stats(&self, sql: String, stmt: &rusqlite::Statement, elapsed: Duration) {
        histogram!("libsql_server_statement_execution_time", elapsed);
        let elapsed = elapsed.as_millis() as u64;
        let rows_read = stmt.get_status(StatementStatus::RowsRead) as u64;
        let rows_written = stmt.get_status(StatementStatus::RowsWritten) as u64;
        let mem_used = stmt.get_status(StatementStatus::MemUsed) as u64;
        histogram!("libsql_server_statement_mem_used_bytes", mem_used as f64);
        let rows_read = if rows_read == 0 && rows_written == 0 {
            1
        } else {
            rows_read
        };
        self.stats.inc_rows_read(rows_read);
        self.stats.inc_rows_written(rows_written);
        let weight = rows_read + rows_written;
        if self.stats.qualifies_as_top_query(weight) {
            self.stats.add_top_query(crate::stats::TopQuery::new(
                sql.clone(),
                rows_read,
                rows_written,
            ));
        }
        if self.stats.qualifies_as_slowest_query(elapsed) {
            self.stats
                .add_slowest_query(crate::stats::SlowestQuery::new(
                    sql.clone(),
                    elapsed,
                    rows_read,
                    rows_written,
                ));
        }

        self.stats
            .update_query_metrics(sql, rows_read, rows_written, mem_used, elapsed)
    }

    fn describe(&self, sql: &str) -> DescribeResult {
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

fn eval_cond(cond: &Cond, results: &[bool], is_autocommit: bool) -> Result<bool> {
    let get_step_res = |step: usize| -> Result<bool> {
        let res = results.get(step).ok_or(Error::InvalidBatchStep(step))?;
        Ok(*res)
    };

    Ok(match cond {
        Cond::Ok { step } => get_step_res(*step)?,
        Cond::Err { step } => !get_step_res(*step)?,
        Cond::Not { cond } => !eval_cond(cond, results, is_autocommit)?,
        Cond::And { conds } => conds.iter().try_fold(true, |x, cond| {
            eval_cond(cond, results, is_autocommit).map(|y| x & y)
        })?,
        Cond::Or { conds } => conds.iter().try_fold(false, |x, cond| {
            eval_cond(cond, results, is_autocommit).map(|y| x | y)
        })?,
        Cond::IsAutocommit => is_autocommit,
    })
}

fn check_program_auth(auth: Authenticated, pgm: &Program) -> Result<()> {
    for step in pgm.steps() {
        let query = &step.query;
        match (query.stmt.kind, &auth) {
            (_, Authenticated::Anonymous) => {
                return Err(Error::NotAuthorized(
                    "anonymous access not allowed".to_string(),
                ));
            }
            (StmtKind::Read, Authenticated::Authorized(_)) => (),
            (StmtKind::TxnBegin, _) | (StmtKind::TxnEnd, _) => (),
            (
                _,
                Authenticated::Authorized(Authorized {
                    permission: Permission::FullAccess,
                    ..
                }),
            ) => (),
            _ => {
                return Err(Error::NotAuthorized(format!(
                    "Current session is not authorized to run: {}",
                    query.stmt.stmt
                )));
            }
        }
    }
    Ok(())
}

fn check_describe_auth(auth: Authenticated) -> Result<()> {
    match auth {
        Authenticated::Anonymous => {
            Err(Error::NotAuthorized("anonymous access not allowed".into()))
        }
        Authenticated::Authorized(_) => Ok(()),
    }
}

#[async_trait::async_trait]
impl<W> super::Connection for LibSqlConnection<W>
where
    W: WalHook + 'static,
    W::Context: Send,
{
    async fn execute_program<B: QueryResultBuilder>(
        &self,
        pgm: Program,
        auth: Authenticated,
        builder: B,
        _replication_index: Option<FrameNo>,
    ) -> Result<(B, State)> {
        check_program_auth(auth, &pgm)?;
        let conn = self.inner.clone();
        tokio::task::spawn_blocking(move || Connection::run(conn, pgm, builder))
            .await
            .unwrap()
    }

    async fn describe(
        &self,
        sql: String,
        auth: Authenticated,
        _replication_index: Option<FrameNo>,
    ) -> Result<DescribeResult> {
        check_describe_auth(auth)?;
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
        match self.inner.try_lock() {
            Some(conn) => match conn.slot {
                Some(ref slot) => format!("{slot:?}"),
                None => "<no-transaction>".to_string(),
            },
            None => "[BUG] connection busy".to_string(),
        }
    }
}

#[cfg(test)]
mod test {
    use itertools::Itertools;
    use sqld_libsql_bindings::wal_hook::TRANSPARENT_METHODS;
    use tempfile::tempdir;
    use tokio::task::JoinSet;

    use crate::query_result_builder::test::{test_driver, TestBuilder};
    use crate::query_result_builder::QueryResultBuilder;
    use crate::DEFAULT_AUTO_CHECKPOINT;

    use super::*;

    fn setup_test_conn() -> Arc<Mutex<Connection>> {
        let conn = Connection {
            conn: sqld_libsql_bindings::Connection::test(),
            stats: Arc::new(Stats::default()),
            config_store: Arc::new(DatabaseConfigStore::new_test()),
            builder_config: QueryBuilderConfig::default(),
            current_frame_no_receiver: watch::channel(None).1,
            state: Default::default(),
            slot: None,
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
            Connection::run(conn, Program::seq(&["select * from test"]), b).map(|x| x.0)
        })
    }

    #[tokio::test]
    async fn txn_timeout_no_stealing() {
        let tmp = tempdir().unwrap();
        let make_conn = MakeLibSqlConn::new(
            tmp.path().into(),
            &TRANSPARENT_METHODS,
            || (),
            Default::default(),
            Arc::new(DatabaseConfigStore::load(tmp.path()).unwrap()),
            Arc::new([]),
            100000000,
            100000000,
            DEFAULT_AUTO_CHECKPOINT,
            watch::channel(None).1,
        )
        .await
        .unwrap();

        tokio::time::pause();
        let conn = make_conn.make_connection().await.unwrap();
        let (_builder, state) = Connection::run(
            conn.inner.clone(),
            Program::seq(&["BEGIN IMMEDIATE"]),
            TestBuilder::default(),
        )
        .unwrap();
        assert_eq!(state, State::Txn);

        tokio::time::advance(TXN_TIMEOUT * 2).await;

        let (builder, state) = Connection::run(
            conn.inner.clone(),
            Program::seq(&["BEGIN IMMEDIATE"]),
            TestBuilder::default(),
        )
        .unwrap();
        assert_eq!(state, State::Init);
        assert!(matches!(builder.into_ret()[0], Err(Error::LibSqlTxTimeout)));
    }

    #[tokio::test]
    /// A bunch of txn try to acquire the lock, and never release it. They will try to steal the
    /// lock one after the other. All txn should eventually acquire the write lock
    async fn serialized_txn_timeouts() {
        let tmp = tempdir().unwrap();
        let make_conn = MakeLibSqlConn::new(
            tmp.path().into(),
            &TRANSPARENT_METHODS,
            || (),
            Default::default(),
            Arc::new(DatabaseConfigStore::load(tmp.path()).unwrap()),
            Arc::new([]),
            100000000,
            100000000,
            DEFAULT_AUTO_CHECKPOINT,
            watch::channel(None).1,
        )
        .await
        .unwrap();

        let mut set = JoinSet::new();
        for _ in 0..10 {
            let conn = make_conn.make_connection().await.unwrap();
            set.spawn_blocking(move || {
                let (builder, state) = Connection::run(
                    conn.inner,
                    Program::seq(&["BEGIN IMMEDIATE"]),
                    TestBuilder::default(),
                )
                .unwrap();
                assert_eq!(state, State::Txn);
                assert!(builder.into_ret()[0].is_ok());
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
            &TRANSPARENT_METHODS,
            || (),
            Default::default(),
            Arc::new(DatabaseConfigStore::load(tmp.path()).unwrap()),
            Arc::new([]),
            100000000,
            100000000,
            DEFAULT_AUTO_CHECKPOINT,
            watch::channel(None).1,
        )
        .await
        .unwrap();

        let conn1 = make_conn.make_connection().await.unwrap();
        tokio::task::spawn_blocking({
            let conn = conn1.inner.clone();
            move || {
                let (builder, state) = Connection::run(
                    conn,
                    Program::seq(&["BEGIN IMMEDIATE"]),
                    TestBuilder::default(),
                )
                .unwrap();
                assert_eq!(state, State::Txn);
                assert!(builder.into_ret()[0].is_ok());
            }
        })
        .await
        .unwrap();

        let conn2 = make_conn.make_connection().await.unwrap();
        let handle = tokio::task::spawn_blocking({
            let conn = conn2.inner.clone();
            move || {
                let before = Instant::now();
                let (builder, state) = Connection::run(
                    conn,
                    Program::seq(&["BEGIN IMMEDIATE"]),
                    TestBuilder::default(),
                )
                .unwrap();
                assert_eq!(state, State::Txn);
                assert!(builder.into_ret()[0].is_ok());
                before.elapsed()
            }
        });

        let wait_time = TXN_TIMEOUT / 10;
        tokio::time::sleep(wait_time).await;

        tokio::task::spawn_blocking({
            let conn = conn1.inner.clone();
            move || {
                let (builder, state) =
                    Connection::run(conn, Program::seq(&["COMMIT"]), TestBuilder::default())
                        .unwrap();
                assert_eq!(state, State::Init);
                assert!(builder.into_ret()[0].is_ok());
            }
        })
        .await
        .unwrap();

        let elapsed = handle.await.unwrap();

        let epsilon = Duration::from_millis(100);
        assert!((wait_time..wait_time + epsilon).contains(&elapsed));
    }
}
