use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam::channel::RecvTimeoutError;
use rusqlite::{ErrorCode, OpenFlags, StatementStatus};
use sqld_libsql_bindings::wal_hook::WalMethodsHook;
use tokio::sync::oneshot;
use tracing::warn;

use crate::auth::{Authenticated, Authorized};
use crate::error::Error;
use crate::libsql::wal_hook::WalHook;
use crate::query::{Column, Query, QueryResponse, QueryResult, ResultSet, Row};
use crate::query_analysis::{State, Statement, StmtKind};
use crate::stats::Stats;
use crate::Result;

use super::factory::DbFactory;
use super::{
    Cond, Database, DescribeCol, DescribeParam, DescribeResponse, DescribeResult, Program, Step,
    TXN_TIMEOUT_SECS,
};

/// Internal message used to communicate between the database thread and the `LibSqlDb` handle.
enum Message {
    Program {
        pgm: Program,
        resp: oneshot::Sender<(Vec<Option<QueryResult>>, State)>,
    },
    Describe {
        sql: String,
        resp: oneshot::Sender<DescribeResult>,
    },
}

pub struct LibSqlDbFactory<W: WalHook + 'static> {
    db_path: PathBuf,
    hook: &'static WalMethodsHook<W>,
    ctx_builder: Box<dyn Fn() -> W::Context + Sync + Send + 'static>,
    stats: Stats,
    extensions: Vec<PathBuf>,
    /// In wal mode, closing the last database takes time, and causes other databases creation to
    /// return sqlite busy. To mitigate that, we hold on to one connection
    _db: Option<LibSqlDb>,
}

impl<W: WalHook + 'static> LibSqlDbFactory<W>
where
    W: WalHook + 'static + Sync + Send,
    W::Context: Send + 'static,
{
    pub async fn new<F>(
        db_path: PathBuf,
        hook: &'static WalMethodsHook<W>,
        ctx_builder: F,
        stats: Stats,
        extensions: Vec<PathBuf>,
    ) -> Result<Self>
    where
        F: Fn() -> W::Context + Sync + Send + 'static,
    {
        let mut this = Self {
            db_path,
            hook,
            ctx_builder: Box::new(ctx_builder),
            stats,
            extensions,
            _db: None,
        };

        let db = this.try_create_db().await?;
        this._db = Some(db);

        Ok(this)
    }

    /// Tries to create a database, retrying if the database is busy.
    async fn try_create_db(&self) -> Result<LibSqlDb> {
        // try 100 times to acquire initial db connection.
        let mut retries = 0;
        loop {
            match self.create_database().await {
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

    async fn create_database(&self) -> Result<LibSqlDb> {
        LibSqlDb::new(
            self.db_path.clone(),
            self.extensions.clone(),
            self.hook,
            (self.ctx_builder)(),
            self.stats.clone(),
        )
        .await
    }
}

#[async_trait::async_trait]
impl<W> DbFactory for LibSqlDbFactory<W>
where
    W: WalHook + 'static + Sync + Send,
    W::Context: Send + 'static,
{
    async fn create(&self) -> Result<Arc<dyn Database>, Error> {
        Ok(Arc::new(self.create_database().await?))
    }
}

#[derive(Clone)]
pub struct LibSqlDb {
    sender: crossbeam::channel::Sender<Message>,
}

struct ConnectionState {
    state: State,
    timeout_deadline: Option<Instant>,
}

impl ConnectionState {
    fn initial() -> Self {
        Self {
            state: State::Init,
            timeout_deadline: None,
        }
    }

    fn deadline(&self) -> Option<Instant> {
        self.timeout_deadline
    }

    fn reset(&mut self) {
        self.state.reset();
        self.timeout_deadline.take();
    }

    fn step(&mut self, stmt: &Statement) {
        let old_state = self.state;

        self.state.step(stmt.kind);

        match (old_state, self.state) {
            (State::Init, State::Txn) => {
                self.timeout_deadline
                    .replace(Instant::now() + Duration::from_secs(TXN_TIMEOUT_SECS));
            }
            (State::Txn, State::Init) => self.reset(),
            (_, State::Invalid) => panic!("invalid state"),
            _ => (),
        }
    }
}

macro_rules! ok_or_exit {
    ($e:expr) => {
        if let Err(_) = $e {
            return;
        }
    };
}

pub fn open_db<'a, W>(
    path: &Path,
    wal_methods: &'static WalMethodsHook<W>,
    hook_ctx: &'a mut W::Context,
    flags: Option<OpenFlags>,
) -> Result<sqld_libsql_bindings::Connection<'a>, rusqlite::Error>
where
    W: WalHook,
{
    let flags = flags.unwrap_or(
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_URI
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    );

    sqld_libsql_bindings::Connection::open(path, flags, wal_methods, hook_ctx)
}

impl LibSqlDb {
    pub async fn new<W>(
        path: impl AsRef<Path> + Send + 'static,
        extensions: Vec<PathBuf>,
        wal_hook: &'static WalMethodsHook<W>,
        hook_ctx: W::Context,
        stats: Stats,
    ) -> crate::Result<Self>
    where
        W: WalHook,
        W::Context: Send,
    {
        let (sender, receiver) = crossbeam::channel::unbounded::<Message>();
        let (init_sender, init_receiver) = oneshot::channel();

        tokio::task::spawn_blocking(move || {
            let mut ctx = hook_ctx;
            let mut connection =
                match Connection::new(path.as_ref(), extensions, wal_hook, &mut ctx, stats) {
                    Ok(conn) => {
                        let Ok(_) = init_sender.send(Ok(())) else { return };
                        conn
                    }
                    Err(e) => {
                        let _ = init_sender.send(Err(e));
                        return;
                    }
                };

            loop {
                let message = match connection.state.deadline() {
                    Some(deadline) => match receiver.recv_deadline(deadline) {
                        Ok(msg) => msg,
                        Err(RecvTimeoutError::Timeout) => {
                            warn!("transaction timed out");
                            connection.rollback();
                            connection.timed_out = true;
                            connection.state.reset();
                            continue;
                        }
                        Err(RecvTimeoutError::Disconnected) => break,
                    },
                    None => match receiver.recv() {
                        Ok(msg) => msg,
                        Err(_) => break,
                    },
                };

                match message {
                    Message::Program { pgm, resp } => {
                        if !connection.timed_out {
                            let results = connection.run(pgm);
                            ok_or_exit!(resp.send((results, connection.state.state)));
                        } else {
                            // fail all the queries in the batch with timeout error
                            let errors = (0..pgm.steps.len())
                                .map(|idx| Some(Err(Error::LibSqlTxTimeout(idx))))
                                .collect();
                            ok_or_exit!(resp.send((errors, connection.state.state)));
                            connection.timed_out = false;
                        }
                    }
                    Message::Describe { sql, resp } => {
                        let result = connection.describe(&sql);
                        ok_or_exit!(resp.send(result));
                    }
                }
            }
        });

        init_receiver.await??;

        Ok(Self { sender })
    }
}

struct Connection<'a> {
    state: ConnectionState,
    conn: sqld_libsql_bindings::Connection<'a>,
    timed_out: bool,
    stats: Stats,
}

impl<'a> Connection<'a> {
    fn new<W: WalHook>(
        path: &Path,
        extensions: Vec<PathBuf>,
        wal_methods: &'static WalMethodsHook<W>,
        hook_ctx: &'a mut W::Context,
        stats: Stats,
    ) -> Result<Self> {
        let this = Self {
            conn: open_db(path, wal_methods, hook_ctx, None)?,
            state: ConnectionState::initial(),
            timed_out: false,
            stats,
        };

        for ext in extensions {
            unsafe {
                let _guard = rusqlite::LoadExtensionGuard::new(&this.conn).unwrap();
                if let Err(e) = this.conn.load_extension(&ext, None) {
                    tracing::error!("failed to load extension: {}", ext.display());
                    Err(e)?;
                }
                tracing::debug!("Loaded extension {}", ext.display());
            }
        }

        Ok(this)
    }

    fn run(&mut self, pgm: Program) -> Vec<Option<QueryResult>> {
        let mut results = Vec::with_capacity(pgm.steps.len());

        for step in pgm.steps() {
            let res = self.execute_step(step, &results);
            results.push(res);
        }

        results
    }

    fn execute_step(
        &mut self,
        step: &Step,
        results: &[Option<QueryResult>],
    ) -> Option<QueryResult> {
        let enabled = match step.cond.as_ref() {
            Some(cond) => match eval_cond(cond, results) {
                Ok(enabled) => enabled,
                Err(e) => return Some(Err(e)),
            },
            None => true,
        };

        enabled.then(|| self.execute_query(&step.query))
    }

    fn execute_query(&mut self, query: &Query) -> QueryResult {
        let result = self.execute_query_inner(query);

        // We drive the connection state on success. This is how we keep track of whether
        // a transaction timeouts
        if result.is_ok() {
            self.state.step(&query.stmt)
        }

        result
    }

    fn execute_query_inner(&self, query: &Query) -> QueryResult {
        tracing::trace!("executing query: {}", query.stmt.stmt);

        let mut rows = vec![];
        let mut stmt = self.conn.prepare(&query.stmt.stmt)?;
        let columns = stmt
            .columns()
            .iter()
            .map(|col| Column {
                name: col.name().into(),
                ty: col
                    .decl_type()
                    .map(FromStr::from_str)
                    .transpose()
                    .ok()
                    .flatten(),
                decltype: col.decl_type().map(|t| t.into()),
            })
            .collect::<Vec<_>>();

        query
            .params
            .bind(&mut stmt)
            .map_err(Error::LibSqlInvalidQueryParams)?;

        let mut qresult = stmt.raw_query();
        while let Some(row) = qresult.next()? {
            if !query.want_rows {
                // if the caller does not want rows, we keep `rows` empty, but we still iterate the
                // statement to completion to make sure that we don't miss any errors or side
                // effects
                continue;
            }

            let mut values = vec![];
            for (i, _) in columns.iter().enumerate() {
                values.push(row.get::<usize, rusqlite::types::Value>(i)?.into());
            }
            rows.push(Row { values });
        }

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

        self.update_stats(&stmt);

        Ok(QueryResponse::ResultSet(ResultSet {
            columns,
            rows,
            affected_row_count,
            last_insert_rowid,
            include_column_defs: true,
        }))
    }

    fn rollback(&self) {
        self.conn
            .execute("ROLLBACK", ())
            .expect("failed to rollback");
    }

    fn update_stats(&self, stmt: &rusqlite::Statement) {
        self.stats
            .inc_rows_read(stmt.get_status(StatementStatus::RowsRead) as u64);
        self.stats
            .inc_rows_written(stmt.get_status(StatementStatus::RowsWritten) as u64);
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
}

fn eval_cond(cond: &Cond, results: &[Option<QueryResult>]) -> Result<bool> {
    let get_step_res = |step: usize| -> Result<Option<&QueryResult>> {
        let res = results
            .get(step)
            .ok_or(Error::InvalidBatchStep(step))?
            .as_ref();

        Ok(res)
    };

    Ok(match cond {
        Cond::Ok { step } => get_step_res(*step)?.map(|r| r.is_ok()).unwrap_or(false),
        Cond::Err { step } => get_step_res(*step)?.map(|r| r.is_err()).unwrap_or(false),
        Cond::Not { cond } => !eval_cond(cond, results)?,
        Cond::And { conds } => conds
            .iter()
            .try_fold(true, |x, cond| eval_cond(cond, results).map(|y| x & y))?,
        Cond::Or { conds } => conds
            .iter()
            .try_fold(false, |x, cond| eval_cond(cond, results).map(|y| x | y))?,
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
            (_, Authenticated::Authorized(Authorized::FullAccess)) => (),
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
impl Database for LibSqlDb {
    async fn execute_program(
        &self,
        pgm: Program,
        auth: Authenticated,
    ) -> Result<(Vec<Option<QueryResult>>, State)> {
        check_program_auth(auth, &pgm)?;
        let (resp, receiver) = oneshot::channel();
        let msg = Message::Program { pgm, resp };
        let _: Result<_, _> = self.sender.send(msg);

        Ok(receiver.await?)
    }

    async fn describe(&self, sql: String, auth: Authenticated) -> Result<DescribeResult> {
        check_describe_auth(auth)?;
        let (resp, receiver) = oneshot::channel();
        let msg = Message::Describe { sql, resp };
        let _: Result<_, _> = self.sender.send(msg);

        Ok(receiver.await?)
    }
}
