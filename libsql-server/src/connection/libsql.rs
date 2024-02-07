use std::ffi::{c_int, c_void};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use libsql_sys::wal::wrapper::{WrapWal, WrappedWal};
use libsql_sys::wal::{BusyHandler, CheckpointCallback, Wal, WalManager};
use metrics::{histogram, increment_counter};
use once_cell::sync::Lazy;
use parking_lot::{Mutex, RwLock};
use rusqlite::ffi::SQLITE_BUSY;
use rusqlite::{DatabaseName, ErrorCode, OpenFlags, StatementStatus, TransactionState};
use tokio::sync::{watch, Notify};
use tokio::time::{Duration, Instant};

use crate::auth::{Authenticated, Authorized, Permission};
use crate::connection::TXN_TIMEOUT;
use crate::error::Error;
use crate::metrics::{
    DESCRIBE_COUNT, PROGRAM_EXEC_COUNT, READ_QUERY_COUNT, VACUUM_COUNT, WAL_CHECKPOINT_COUNT,
    WRITE_QUERY_COUNT, WRITE_TXN_DURATION,
};
use crate::namespace::meta_store::MetaStoreHandle;
use crate::query::Query;
use crate::query_analysis::{StmtKind, TxnStatus};
use crate::query_result_builder::{QueryBuilderConfig, QueryResultBuilder};
use crate::replication::FrameNo;
use crate::stats::Stats;
use crate::Result;

use super::program::{Cond, DescribeCol, DescribeParam, DescribeResponse};
use super::{MakeConnection, Program, Step};

pub struct MakeLibSqlConn<T: WalManager> {
    db_path: PathBuf,
    wal_manager: T,
    stats: Arc<Stats>,
    config_store: MetaStoreHandle,
    extensions: Arc<[PathBuf]>,
    max_response_size: u64,
    max_total_response_size: u64,
    auto_checkpoint: u32,
    current_frame_no_receiver: watch::Receiver<Option<FrameNo>>,
    state: Arc<TxnState<T::Wal>>,
    /// In wal mode, closing the last database takes time, and causes other databases creation to
    /// return sqlite busy. To mitigate that, we hold on to one connection
    _db: Option<LibSqlConnection<T::Wal>>,
    encryption_key: Option<bytes::Bytes>,
}

impl<T> MakeLibSqlConn<T>
where
    T: WalManager + Clone + Send + 'static,
    T::Wal: Send + 'static,
{
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        db_path: PathBuf,
        wal_manager: T,
        stats: Arc<Stats>,
        config_store: MetaStoreHandle,
        extensions: Arc<[PathBuf]>,
        max_response_size: u64,
        max_total_response_size: u64,
        auto_checkpoint: u32,
        current_frame_no_receiver: watch::Receiver<Option<FrameNo>>,
        encryption_key: Option<bytes::Bytes>,
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
            state: Default::default(),
            wal_manager,
            encryption_key,
        };

        let db = this.try_create_db().await?;
        this._db = Some(db);

        Ok(this)
    }

    /// Tries to create a database, retrying if the database is busy.
    async fn try_create_db(&self) -> Result<LibSqlConnection<T::Wal>> {
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

    async fn make_connection(&self) -> Result<LibSqlConnection<T::Wal>> {
        LibSqlConnection::new(
            self.db_path.clone(),
            self.extensions.clone(),
            self.wal_manager.clone(),
            self.stats.clone(),
            self.config_store.clone(),
            QueryBuilderConfig {
                max_size: Some(self.max_response_size),
                max_total_size: Some(self.max_total_response_size),
                auto_checkpoint: self.auto_checkpoint,
                encryption_key: self.encryption_key.clone(),
            },
            self.current_frame_no_receiver.clone(),
            self.state.clone(),
        )
        .await
    }
}

#[async_trait::async_trait]
impl<T> MakeConnection for MakeLibSqlConn<T>
where
    T: WalManager + Clone + Send + Sync + 'static,
    T::Wal: Send,
{
    type Connection = LibSqlConnection<T::Wal>;

    async fn create(&self) -> Result<Self::Connection, Error> {
        self.make_connection().await
    }
}

pub struct LibSqlConnection<T> {
    inner: Arc<Mutex<Connection<T>>>,
}

impl<T> Clone for LibSqlConnection<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T> std::fmt::Debug for LibSqlConnection<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.inner.try_lock() {
            Some(conn) => {
                write!(f, "{conn:?}")
            }
            None => write!(f, "<locked>"),
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
    encryption_key: Option<bytes::Bytes>,
) -> Result<libsql_sys::Connection<InhibitCheckpoint<T::Wal>>, rusqlite::Error>
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
        wal_manager.wrap(InhibitCheckpointWalWrapper::new(false)),
        u32::MAX,
        encryption_key,
    )
}

/// Same as open_conn, but with checkpointing activated.
pub fn open_conn_active_checkpoint<T>(
    path: &Path,
    wal_manager: T,
    flags: Option<OpenFlags>,
    auto_checkpoint: u32,
    encryption_key: Option<bytes::Bytes>,
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
        encryption_key,
    )
}

impl<W> LibSqlConnection<W>
where
    W: Wal + Send + 'static,
{
    pub async fn new<T>(
        path: impl AsRef<Path> + Send + 'static,
        extensions: Arc<[PathBuf]>,
        wal_manager: T,
        stats: Arc<Stats>,
        config_store: MetaStoreHandle,
        builder_config: QueryBuilderConfig,
        current_frame_no_receiver: watch::Receiver<Option<FrameNo>>,
        state: Arc<TxnState<W>>,
    ) -> crate::Result<Self>
    where
        T: WalManager<Wal = W> + Send + 'static,
    {
        let max_db_size = config_store.get().max_db_pages;
        let conn = tokio::task::spawn_blocking(move || -> crate::Result<_> {
            let conn = Connection::new(
                path.as_ref(),
                extensions,
                wal_manager,
                stats,
                config_store,
                builder_config,
                current_frame_no_receiver,
                state,
            )?;
            conn.conn
                .pragma_update(None, "max_page_count", max_db_size)?;
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
            Ok(conn)
        })
        .await
        .unwrap()?;

        Ok(Self {
            inner: Arc::new(Mutex::new(conn)),
        })
    }

    pub fn txn_status(&self) -> crate::Result<TxnStatus> {
        Ok(self
            .inner
            .lock()
            .conn
            .transaction_state(Some(DatabaseName::Main))?
            .into())
    }
}

#[cfg(test)]
impl LibSqlConnection<libsql_sys::wal::Sqlite3Wal> {
    pub fn new_test(path: &Path) -> Self {
        let (_snd, rcv) = watch::channel(None);
        let conn = Connection::new(
            path,
            Arc::new([]),
            libsql_sys::wal::Sqlite3WalManager::new(),
            Default::default(),
            MetaStoreHandle::new_test(),
            QueryBuilderConfig::default(),
            rcv,
            Default::default(),
        )
        .unwrap();

        Self {
            inner: Arc::new(Mutex::new(conn)),
        }
    }
}

struct Connection<T> {
    conn: libsql_sys::Connection<T>,
    stats: Arc<Stats>,
    config_store: MetaStoreHandle,
    builder_config: QueryBuilderConfig,
    current_frame_no_receiver: watch::Receiver<Option<FrameNo>>,
    // must be dropped after the connection because the connection refers to it
    state: Arc<TxnState<T>>,
    // current txn slot if any
    slot: Option<Arc<TxnSlot<T>>>,
}

impl<T> std::fmt::Debug for Connection<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection")
            .field("slot", &self.slot)
            .finish()
    }
}

/// A slot for holding the state of a transaction lock permit
struct TxnSlot<T> {
    /// Pointer to the connection holding the lock. Used to rollback the transaction when the lock
    /// is stolen.
    conn: Arc<Mutex<Connection<T>>>,
    /// Time at which the transaction can be stolen
    created_at: tokio::time::Instant,
    /// The transaction lock was stolen
    is_stolen: parking_lot::Mutex<bool>,
    txn_timeout: Duration,
}

impl<T> TxnSlot<T> {
    #[inline]
    fn expires_at(&self) -> Instant {
        self.created_at + self.txn_timeout
    }

    /// abort the connection for that slot.
    /// This methods must not be called if a lock on the state's slot is still held.
    fn abort(&self)
    where
        T: Wal,
    {
        let conn = self.conn.lock();
        // we have a lock on the connection, we don't need mode than a
        // Relaxed store.
        conn.rollback();
        WRITE_TXN_DURATION.record(self.created_at.elapsed());
    }
}

impl<T> std::fmt::Debug for TxnSlot<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let stolen = self.is_stolen.lock();
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
pub struct TxnState<T> {
    /// Slot for the connection currently holding the transaction lock
    slot: RwLock<Option<Arc<TxnSlot<T>>>>,
    /// Notifier for when the lock gets dropped
    notify: Notify,
}

impl<T> Default for TxnState<T> {
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
const MAX_BUSY_RETRIES: c_int = 512;

unsafe extern "C" fn busy_handler<T: Wal>(state: *mut c_void, retries: c_int) -> c_int {
    let state = &*(state as *mut TxnState<T>);
    let lock = state.slot.read();
    // we take a reference to the slot we will attempt to steal. this is to make sure that we
    // actually steal the correct lock.
    let slot = match &*lock {
        Some(slot) => slot.clone(),
        // fast path: there is no slot, try to acquire the lock again
        None if retries < 512 => {
            std::thread::sleep(std::time::Duration::from_millis(10));
            return 1;
        }
        None => {
            tracing::info!("Failed to steal connection lock after {MAX_BUSY_RETRIES} retries.");
            return 0;
        }
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
                tracing::info!("transaction has timed-out, stealing lock");
                // only a single connection gets to steal the lock, others retry
                if let Some(mut lock) = state.slot.try_write() {
                    if let Some(ref s) = *lock {
                        // The state contains the same lock as the one we're attempting to steal
                        if Arc::ptr_eq(s, &slot) {
                            let can_steal = {
                                let mut can_steal = false;
                                let mut is_stolen = slot.is_stolen.lock();
                                if !*is_stolen {
                                    can_steal = true;
                                    *is_stolen = true;
                                }
                                can_steal
                            };

                            if can_steal {
                                // The connection holding the current txn will set itself as stolen when it
                                // detects a timeout, so if we arrive to this point, then there is
                                // necessarily a slot, and this slot has to be the one we attempted to
                                // steal.
                                assert!(lock.take().is_some());
                                // we drop the lock here, before aborting, because the connection
                                // may currently be waiting for the lock to commit/abort itself,
                                // and we don't need the slot lock past that point.
                                drop(lock);

                                slot.abort();
                                tracing::info!("stole transaction lock");
                            }
                        }
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

impl From<TransactionState> for TxnStatus {
    fn from(value: TransactionState) -> Self {
        match value {
            TransactionState::None => TxnStatus::Init,
            TransactionState::Read | TransactionState::Write => TxnStatus::Txn,
            _ => unreachable!(),
        }
    }
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
        state: Arc<TxnState<W>>,
    ) -> Result<Self> {
        let conn = open_conn_active_checkpoint(
            path,
            wal_manager,
            None,
            builder_config.auto_checkpoint,
            builder_config.encryption_key.clone(),
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
    ) -> Result<B> {
        let txn_timeout = this
            .lock()
            .config_store
            .get()
            .txn_timeout
            .unwrap_or(TXN_TIMEOUT);

        let mut results = Vec::with_capacity(pgm.steps.len());
        builder.init(&this.lock().builder_config)?;
        let mut previous_state = this
            .lock()
            .conn
            .transaction_state(Some(DatabaseName::Main))?;

        let mut has_timeout = false;
        for step in pgm.steps() {
            let mut lock = this.lock();

            if !has_timeout {
                if let Some(slot) = &lock.slot {
                    let mut is_stolen = slot.is_stolen.lock();
                    if *is_stolen || Instant::now() > slot.expires_at() {
                        // we mark ourselves as stolen to notify any waiting lock thief.
                        if !*is_stolen {
                            lock.rollback();
                        }
                        *is_stolen = true;
                        has_timeout = true;
                    }
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

            let ret = lock.execute_step(step, &results, &mut builder);
            // /!\ always make sure that the state is updated before returning
            previous_state = lock.update_state(this.clone(), previous_state, txn_timeout)?;
            let res = ret?;

            results.push(res);
        }

        {
            let mut lock = this.lock();
            let is_autocommit = lock.conn.is_autocommit();
            let current_fno = *lock.current_frame_no_receiver.borrow_and_update();
            builder.finish(current_fno, is_autocommit)?;
        }

        Ok(builder)
    }

    fn update_state(
        &mut self,
        arc_this: Arc<Mutex<Self>>,
        previous_state: TransactionState,
        txn_timeout: Duration,
    ) -> Result<TransactionState> {
        use rusqlite::TransactionState as Tx;

        let new_state = self.conn.transaction_state(Some(DatabaseName::Main))?;
        match (previous_state, new_state) {
            // lock was upgraded, claim the slot
            (Tx::None | Tx::Read, Tx::Write) => {
                let slot = Arc::new(TxnSlot {
                    conn: arc_this,
                    created_at: Instant::now(),
                    is_stolen: false.into(),
                    txn_timeout,
                });

                self.slot.replace(slot.clone());
                self.state.slot.write().replace(slot);
            }
            // lock was downgraded, notify a waiter
            (Tx::Write, Tx::None | Tx::Read) => {
                let old_slot = self
                    .slot
                    .take()
                    .expect("there should be a slot right after downgrading a txn");
                let mut maybe_state_slot = self.state.slot.write();
                // We need to make sure that the state slot is our slot before removing it.
                if let Some(ref state_slot) = *maybe_state_slot {
                    if Arc::ptr_eq(state_slot, &old_slot) {
                        maybe_state_slot.take();
                    }
                }

                drop(maybe_state_slot);

                self.state.notify.notify_waiters();
            }
            // nothing to do
            (_, _) => (),
        }

        Ok(new_state)
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

        increment_counter!("libsql_server_libsql_query_execute");

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

        if rows_read >= 10_000 || rows_written >= 1_000 {
            tracing::info!(
                "high read ({}) or write ({}) query: {}",
                rows_read,
                rows_written,
                sql
            );
        }

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
            .update_query_metrics(rows_read, rows_written, mem_used, elapsed)
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

/// We use a different runtime to run the connection, because long running tasks block turmoil
static CONN_RT: Lazy<tokio::runtime::Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_time()
        .build()
        .unwrap()
});

#[async_trait::async_trait]
impl<T> super::Connection for LibSqlConnection<T>
where
    T: Wal + Send + 'static,
{
    async fn execute_program<B: QueryResultBuilder>(
        &self,
        pgm: Program,
        auth: Authenticated,
        builder: B,
        _replication_index: Option<FrameNo>,
    ) -> Result<B> {
        PROGRAM_EXEC_COUNT.increment(1);

        check_program_auth(auth, &pgm)?;
        let conn = self.inner.clone();
        CONN_RT
            .spawn_blocking(move || Connection::run(conn, pgm, builder))
            .await
            .unwrap()
    }

    async fn describe(
        &self,
        sql: String,
        auth: Authenticated,
        _replication_index: Option<FrameNo>,
    ) -> Result<crate::Result<DescribeResponse>> {
        DESCRIBE_COUNT.increment(1);
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
    use libsql_sys::wal::{Sqlite3Wal, Sqlite3WalManager};
    use rand::Rng;
    use tempfile::tempdir;
    use tokio::task::JoinSet;

    use crate::connection::Connection as _;
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
            Connection::run(conn, Program::seq(&["select * from test"]), b)
        })
    }

    #[tokio::test]
    async fn txn_timeout_no_stealing() {
        let tmp = tempdir().unwrap();
        let make_conn = MakeLibSqlConn::new(
            tmp.path().into(),
            Sqlite3WalManager::new(),
            Default::default(),
            MetaStoreHandle::load(tmp.path()).unwrap(),
            Arc::new([]),
            100000000,
            100000000,
            DEFAULT_AUTO_CHECKPOINT,
            watch::channel(None).1,
            None,
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
        assert_eq!(conn.txn_status().unwrap(), TxnStatus::Txn);

        tokio::time::advance(TXN_TIMEOUT * 2).await;

        let builder = Connection::run(
            conn.inner.clone(),
            Program::seq(&["BEGIN IMMEDIATE"]),
            TestBuilder::default(),
        )
        .unwrap();
        assert_eq!(conn.txn_status().unwrap(), TxnStatus::Init);
        assert!(matches!(builder.into_ret()[0], Err(Error::LibSqlTxTimeout)));
    }

    #[tokio::test]
    /// A bunch of txn try to acquire the lock, and never release it. They will try to steal the
    /// lock one after the other. All txn should eventually acquire the write lock
    async fn serialized_txn_timeouts() {
        let tmp = tempdir().unwrap();
        let make_conn = MakeLibSqlConn::new(
            tmp.path().into(),
            Sqlite3WalManager::new(),
            Default::default(),
            MetaStoreHandle::load(tmp.path()).unwrap(),
            Arc::new([]),
            100000000,
            100000000,
            DEFAULT_AUTO_CHECKPOINT,
            watch::channel(None).1,
            None,
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
                    (ret.is_ok() && matches!(conn.txn_status().unwrap(), TxnStatus::Txn))
                        || (matches!(ret, Err(Error::RusqliteErrorExtended(_, 5)))
                            && matches!(conn.txn_status().unwrap(), TxnStatus::Init))
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
            Sqlite3WalManager::new(),
            Default::default(),
            MetaStoreHandle::load(tmp.path()).unwrap(),
            Arc::new([]),
            100000000,
            100000000,
            DEFAULT_AUTO_CHECKPOINT,
            watch::channel(None).1,
            None,
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
                assert_eq!(conn.txn_status().unwrap(), TxnStatus::Txn);
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
                assert_eq!(conn.txn_status().unwrap(), TxnStatus::Txn);
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
                assert_eq!(conn.txn_status().unwrap(), TxnStatus::Init);
                assert!(builder.into_ret()[0].is_ok());
            }
        })
        .await
        .unwrap();

        let elapsed = handle.await.unwrap();

        let epsilon = Duration::from_millis(100);
        assert!((wait_time..wait_time + epsilon).contains(&elapsed));
    }

    /// The goal of this test is to run many conccurent transaction and hopefully catch a bug in
    /// the lock stealing code. If this test becomes flaky check out the lock stealing code.
    #[tokio::test]
    async fn test_many_conccurent() {
        let tmp = tempdir().unwrap();
        let make_conn = MakeLibSqlConn::new(
            tmp.path().into(),
            Sqlite3WalManager::new(),
            Default::default(),
            MetaStoreHandle::load(tmp.path()).unwrap(),
            Arc::new([]),
            100000000,
            100000000,
            DEFAULT_AUTO_CHECKPOINT,
            watch::channel(None).1,
            None,
        )
        .await
        .unwrap();
        let auth = Authenticated::Authorized(Authorized {
            namespace: None,
            permission: Permission::FullAccess,
        });

        let conn = make_conn.make_connection().await.unwrap();
        conn.execute_program(
            Program::seq(&["CREATE TABLE test (x)"]),
            auth.clone(),
            TestBuilder::default(),
            None,
        )
        .await
        .unwrap();
        let run_conn = |maker: Arc<MakeLibSqlConn<Sqlite3WalManager>>| {
            let auth = auth.clone();
            async move {
                for _ in 0..1000 {
                    let conn = maker.make_connection().await.unwrap();
                    let pgm = Program::seq(&["BEGIN IMMEDIATE", "INSERT INTO test VALUES (42)"]);
                    let res = conn
                        .execute_program(pgm, auth.clone(), TestBuilder::default(), None)
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
                            .execute_program(pgm, auth.clone(), TestBuilder::default(), None)
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
}
