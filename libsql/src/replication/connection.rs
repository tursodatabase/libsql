// TODO(lucio): Move this to `remote/mod.rs`

use std::str::FromStr;
use std::sync::Arc;

use libsql_replication::rpc::proxy::{
    describe_result, query_result::RowResult, DescribeResult, ExecuteResults, ResultRows,
    State as RemoteState,
};
use parking_lot::Mutex;

use crate::parser;
use crate::parser::StmtKind;
use crate::rows::{RowInner, RowsInner};
use crate::statement::Stmt;
use crate::transaction::Tx;
use crate::{
    params::Params, replication::Writer, Error, Result, Statement, Transaction,
    TransactionBehavior, ValueType,
};
use crate::{Column, Row, Rows, Value};

use crate::connection::Conn;
use crate::local::impls::LibsqlConnection;

#[derive(Clone)]
pub struct RemoteConnection {
    pub(self) local: LibsqlConnection,
    writer: Option<Writer>,
    inner: Arc<Mutex<Inner>>,
}

#[derive(Default, Debug)]
struct Inner {
    state: State,
    changes: u64,
    last_insert_rowid: i64,
}

#[derive(Debug, Default, PartialEq, Eq, Clone, Copy)]
enum State {
    #[default]
    Init,
    Invalid,
    Txn,
    TxnReadOnly,
}

impl State {
    pub fn step(&self, kind: StmtKind) -> State {
        use State;

        match (*self, kind) {
            (State::TxnReadOnly, StmtKind::TxnBegin)
            | (State::Txn, StmtKind::TxnBegin)
            | (State::Init, StmtKind::TxnEnd) => State::Invalid,

            (State::TxnReadOnly, StmtKind::TxnEnd) | (State::Txn, StmtKind::TxnEnd) => State::Init,

            // Savepoint only makes sense within a transaction and doesn't change the transaction kind
            (State::TxnReadOnly, StmtKind::Savepoint) => State::TxnReadOnly,
            (State::Txn, StmtKind::Savepoint) => State::Txn,
            (_, StmtKind::Savepoint) => State::Invalid,
            // Releasing a savepoint only makes sense inside a transaction and it doesn't change its state
            (State::TxnReadOnly, StmtKind::Release) => State::TxnReadOnly,
            (State::Txn, StmtKind::Release) => State::Txn,
            (_, StmtKind::Release) => State::Invalid,

            (state, StmtKind::Other | StmtKind::Write | StmtKind::Read) => state,
            (State::Invalid, _) => State::Invalid,

            (State::Init, StmtKind::TxnBegin) => State::Txn,
            (State::Init, StmtKind::TxnBeginReadOnly) => State::TxnReadOnly,

            (State::Txn, StmtKind::TxnBeginReadOnly)
            | (State::TxnReadOnly, StmtKind::TxnBeginReadOnly) => State::Invalid,
        }
    }
}

/// Given a an initial state and an array of queries, attempts to predict what the final state will
/// be
fn predict_final_state<'a>(
    mut state: State,
    stmts: impl Iterator<Item = &'a parser::Statement>,
) -> State {
    for stmt in stmts {
        state = state.step(stmt.kind);
    }
    state
}

/// Determines if a set of statements should be executed locally or remotely. It takes into
/// account the current state of the connection and the potential final state of the statements
/// parsed. This means that we only take into account the entire passed sql statement set and
/// for example will reject writes if we are in a readonly txn to start with even if we commit
/// and start a new transaction with the write in it.
fn should_execute_local(state: &mut State, stmts: &[parser::Statement]) -> Result<bool> {
    let predicted_end_state = predict_final_state(*state, stmts.iter());

    let should_execute_local = match (*state, predicted_end_state) {
        (State::Init, State::Init) => {
            *state = State::Init;
            stmts.iter().all(parser::Statement::is_read_only)
        }

        (State::Init, State::TxnReadOnly) | (State::TxnReadOnly, State::TxnReadOnly) => {
            let is_read_only = stmts.iter().all(parser::Statement::is_read_only);

            if !is_read_only {
                return Err(Error::Misuse(
                    "Invalid write in a readonly transaction".into(),
                ));
            }

            *state = State::TxnReadOnly;
            true
        }

        (State::TxnReadOnly, State::Init) => {
            let is_read_only = stmts.iter().all(parser::Statement::is_read_only);

            if !is_read_only {
                return Err(Error::Misuse(
                    "Invalid write in a readonly transaction".into(),
                ));
            }

            *state = State::Init;
            true
        }

        (init, State::Invalid) => {
            // Panic here because the connection has become invalid and it can no longer be
            // used
            panic!(
                "replication connection has reached an invalid state, started with {:?}",
                init
            );
        }

        _ => false,
    };

    Ok(should_execute_local)
}

impl From<RemoteState> for State {
    fn from(value: RemoteState) -> Self {
        match value {
            RemoteState::Init => State::Init,
            RemoteState::Invalid => State::Invalid,
            RemoteState::Txn => State::Txn,
        }
    }
}

impl RemoteConnection {
    pub(crate) fn new(local: LibsqlConnection, writer: Option<Writer>) -> Self {
        let state = Arc::new(Mutex::new(Inner::default()));
        Self {
            local,
            writer,
            inner: state,
        }
    }

    fn is_state_init(&self) -> bool {
        matches!(self.inner.lock().state, State::Init)
    }

    pub(self) async fn execute_remote(
        &self,
        stmts: Vec<parser::Statement>,
        params: Params,
    ) -> Result<ExecuteResults> {
        let Some(ref writer) = self.writer else {
            return Err(Error::Misuse(
                "Cannot delegate write in local replica mode.".into(),
            ));
        };
        let res = writer
            .execute_program(stmts, params)
            .await
            .map_err(|e| Error::WriteDelegation(e.into()))?;

        {
            let mut inner = self.inner.lock();
            inner.state = RemoteState::try_from(res.state)
                .expect("Invalid state enum")
                .into();
        }

        Ok(res)
    }

    pub(self) async fn describe(&self, stmt: impl Into<String>) -> Result<DescribeResult> {
        let Some(ref writer) = self.writer else {
            return Err(Error::Misuse(
                "Cannot describe in local replica mode.".into(),
            ));
        };
        let res = writer
            .describe(stmt)
            .await
            .map_err(|e| Error::WriteDelegation(e.into()))?;

        Ok(res)
    }

    pub(self) fn update_state(&self, row: &ResultRows) {
        let mut state = self.inner.lock();

        if let Some(rowid) = &row.last_insert_rowid {
            state.last_insert_rowid = *rowid;
        }

        state.changes = row.affected_row_count;
    }

    pub(self) fn should_execute_local(&self, stmts: &[parser::Statement]) -> Result<bool> {
        let mut inner = self.inner.lock();

        should_execute_local(&mut inner.state, stmts)
    }

    // Will execute a rollback if the local conn is in TXN state
    // and will return false if no rollback happened and the
    // execute was valid.
    pub(self) async fn maybe_execute_rollback(&self) -> Result<bool> {
        if !self.local.is_autocommit() && self.inner.lock().state != State::TxnReadOnly {
            self.local.execute("ROLLBACK", Params::None).await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

#[async_trait::async_trait]
impl Conn for RemoteConnection {
    async fn execute(&self, sql: &str, params: Params) -> Result<u64> {
        let stmts = parser::Statement::parse(sql).collect::<Result<Vec<_>>>()?;

        if self.should_execute_local(&stmts[..])? {
            // TODO(lucio): See if we can arc the params here to cheaply clone
            // or convert the inner bytes type to an Arc<[u8]>
            let changes = self.local.execute(sql, params.clone()).await?;

            if !self.maybe_execute_rollback().await? {
                return Ok(changes);
            }
        }

        let res = self.execute_remote(stmts, params).await?;

        let result = res
            .results
            .into_iter()
            .next()
            .expect("Expected atleast one result");

        let affected_row_count = match result.row_result {
            Some(RowResult::Row(row)) => {
                self.update_state(&row);
                row.affected_row_count
            }
            Some(RowResult::Error(e)) => {
                return Err(Error::RemoteSqliteFailure(
                    e.code,
                    e.extended_code,
                    e.message,
                ))
            }
            None => panic!("unexpected empty result row"),
        };

        Ok(affected_row_count)
    }

    async fn execute_batch(&self, sql: &str) -> Result<()> {
        let stmts = parser::Statement::parse(sql).collect::<Result<Vec<_>>>()?;

        if self.should_execute_local(&stmts[..])? {
            self.local.execute_batch(sql).await?;

            if !self.maybe_execute_rollback().await? {
                return Ok(());
            }
        }

        let res = self.execute_remote(stmts, Params::None).await?;

        for result in res.results {
            match result.row_result {
                Some(RowResult::Row(row)) => self.update_state(&row),
                Some(RowResult::Error(e)) => {
                    return Err(Error::RemoteSqliteFailure(
                        e.code,
                        e.extended_code,
                        e.message,
                    ))
                }
                None => panic!("unexpected empty result row"),
            };
        }

        Ok(())
    }

    async fn prepare(&self, sql: &str) -> Result<Statement> {
        let stmt = RemoteStatement::prepare(self.clone(), sql).await?;

        Ok(crate::Statement {
            inner: Box::new(stmt),
        })
    }

    async fn transaction(&self, tx_behavior: TransactionBehavior) -> Result<Transaction> {
        let tx = RemoteTx::begin(self.clone(), tx_behavior).await?;

        Ok(crate::Transaction {
            inner: Box::new(tx),
            conn: crate::Connection {
                conn: Arc::new(self.clone()),
            },
        })
    }

    fn is_autocommit(&self) -> bool {
        self.is_state_init()
    }

    fn changes(&self) -> u64 {
        self.inner.lock().changes
    }

    fn last_insert_rowid(&self) -> i64 {
        self.inner.lock().last_insert_rowid
    }

    fn close(&mut self) {
        self.local.close()
    }
}

pub struct ColumnMeta {
    name: String,
    origin_name: Option<String>,
    table_name: Option<String>,
    database_name: Option<String>,
    decl_type: Option<String>,
}

impl From<libsql_replication::rpc::proxy::Column> for ColumnMeta {
    fn from(col: libsql_replication::rpc::proxy::Column) -> Self {
        Self {
            name: col.name.clone(),
            origin_name: None,
            table_name: None,
            database_name: None,
            decl_type: col.decltype,
        }
    }
}

impl<'a> From<&'a ColumnMeta> for Column<'a> {
    fn from(col: &'a ColumnMeta) -> Self {
        Self {
            name: col.name.as_str(),
            origin_name: col.origin_name.as_deref(),
            table_name: col.table_name.as_deref(),
            database_name: col.database_name.as_deref(),
            decl_type: col.decl_type.as_deref(),
        }
    }
}

pub struct StatementMeta {
    columns: Vec<ColumnMeta>,
    param_names: Vec<String>,
    param_count: u64,
}

pub struct RemoteStatement {
    conn: RemoteConnection,
    stmts: Vec<parser::Statement>,
    /// Empty if we should execute locally
    metas: Vec<StatementMeta>,
    /// Set to `Some` when we should execute this locally
    local_statement: Option<crate::Statement>,
}

impl RemoteStatement {
    pub async fn prepare(conn: RemoteConnection, sql: &str) -> Result<Self> {
        let stmts = parser::Statement::parse(sql).collect::<Result<Vec<_>>>()?;

        if conn.should_execute_local(&stmts[..])? {
            tracing::trace!("Preparing {sql} locally");
            let stmt = conn.local.prepare(sql).await?;
            return Ok(Self {
                conn,
                stmts,
                local_statement: Some(stmt),
                metas: vec![],
            });
        }

        let metas = fetch_metas(&conn, &stmts).await?;
        Ok(Self {
            conn,
            stmts,
            local_statement: None,
            metas,
        })
    }
}

async fn fetch_meta(conn: &RemoteConnection, stmt: &parser::Statement) -> Result<StatementMeta> {
    tracing::trace!("Fetching metadata of statement {}", stmt.stmt);
    match conn.describe(&stmt.stmt).await? {
        DescribeResult {
            describe_result: Some(describe_result::DescribeResult::Description(d)),
        } => Ok(StatementMeta {
            columns: d
                .column_descriptions
                .into_iter()
                .map(|c| c.into())
                .collect(),
            param_names: d.param_names.into_iter().collect(),
            param_count: d.param_count,
        }),
        DescribeResult {
            describe_result: Some(describe_result::DescribeResult::Error(e)),
        } => Err(Error::SqliteFailure(e.code, e.message)),
        _ => Err(Error::Misuse("unexpected describe result".into())),
    }
}

// FIXME(sarna): do we ever want to fetch metadata about multiple statements at one go?
async fn fetch_metas(
    conn: &RemoteConnection,
    stmts: &[parser::Statement],
) -> Result<Vec<StatementMeta>> {
    let mut metas = vec![];
    for stmt in stmts {
        let meta = fetch_meta(conn, stmt).await?;
        metas.push(meta);
    }
    Ok(metas)
}

#[async_trait::async_trait]
impl Stmt for RemoteStatement {
    fn finalize(&mut self) {}

    async fn execute(&mut self, params: &Params) -> Result<usize> {
        if let Some(stmt) = &mut self.local_statement {
            return stmt.execute(params.clone()).await;
        }

        let res = self
            .conn
            .execute_remote(self.stmts.clone(), params.clone())
            .await?;

        let result = res
            .results
            .into_iter()
            .next()
            .expect("Expected atleast one result");

        let affected_row_count = match result.row_result {
            Some(RowResult::Row(row)) => {
                self.conn.update_state(&row);
                row.affected_row_count
            }
            Some(RowResult::Error(e)) => {
                return Err(Error::RemoteSqliteFailure(
                    e.code,
                    e.extended_code,
                    e.message,
                ))
            }
            None => panic!("unexpected empty result row"),
        };

        Ok(affected_row_count as usize)
    }

    async fn query(&mut self, params: &Params) -> Result<Rows> {
        if let Some(stmt) = &mut self.local_statement {
            return stmt.query(params.clone()).await;
        }

        let res = self
            .conn
            .execute_remote(self.stmts.clone(), params.clone())
            .await?;

        let result = res
            .results
            .into_iter()
            .next()
            .expect("Expected atleast one result");

        let rows = match result.row_result {
            Some(RowResult::Row(row)) => {
                self.conn.update_state(&row);
                row
            }
            Some(RowResult::Error(e)) => {
                return Err(Error::RemoteSqliteFailure(
                    e.code,
                    e.extended_code,
                    e.message,
                ))
            }
            None => panic!("unexpected empty result row"),
        };

        Ok(Rows {
            inner: Box::new(RemoteRows(rows, 0)),
        })
    }

    fn reset(&mut self) {}

    fn parameter_count(&self) -> usize {
        if let Some(stmt) = self.local_statement.as_ref() {
            return stmt.parameter_count();
        }
        // FIXME: we need to decide if we keep RemoteStatement as a single statement, or else how to handle this
        match self.metas.first() {
            Some(meta) => meta.param_count as usize,
            None => 0,
        }
    }

    fn parameter_name(&self, idx: i32) -> Option<&str> {
        if let Some(stmt) = self.local_statement.as_ref() {
            return stmt.parameter_name(idx);
        }
        // FIXME: we need to decide if we keep RemoteStatement as a single statement, or else how to handle this
        match self.metas.first() {
            Some(meta) => meta.param_names.get(idx as usize).map(|s| s.as_str()),
            None => None,
        }
    }

    fn columns(&self) -> Vec<Column> {
        if let Some(stmt) = self.local_statement.as_ref() {
            return stmt.columns();
        }
        // FIXME: we need to decide if we keep RemoteStatement as a single statement, or else how to handle this
        match self.metas.first() {
            Some(meta) => meta
                .columns
                .iter()
                .map(|c| Column {
                    name: &c.name,
                    origin_name: c.origin_name.as_deref(),
                    database_name: c.database_name.as_deref(),
                    table_name: c.table_name.as_deref(),
                    decl_type: c.decl_type.as_deref(),
                })
                .collect(),
            None => vec![],
        }
    }
}

pub(crate) struct RemoteRows(pub(crate) ResultRows, pub(crate) usize);

impl RowsInner for RemoteRows {
    fn next(&mut self) -> Result<Option<Row>> {
        // TODO(lucio): Switch to a vecdeque and reduce allocations
        let cursor = self.1;
        self.1 += 1;
        let row = self.0.rows.get(cursor);

        if row.is_none() {
            return Ok(None);
        }

        let row = row.unwrap();

        let values = row
            .values
            .iter()
            .map(|v| bincode::deserialize(&v.data[..]).map_err(Error::from))
            .collect::<Result<Vec<_>>>()?;

        let row = RemoteRow(values, self.0.column_descriptions.clone());
        Ok(Some(row).map(Box::new).map(|inner| Row { inner }))
    }

    fn column_count(&self) -> i32 {
        self.0.column_descriptions.len() as i32
    }

    fn column_name(&self, idx: i32) -> Option<&str> {
        self.0
            .column_descriptions
            .get(idx as usize)
            .map(|s| s.name.as_str())
    }

    fn column_type(&self, idx: i32) -> Result<ValueType> {
        let col = self.0.column_descriptions.get(idx as usize).unwrap();
        col.decltype
            .as_deref()
            .and_then(|v| ValueType::from_str(v).ok())
            .map(ValueType::from)
            .ok_or(Error::InvalidColumnType)
    }
}

#[derive(Debug)]
struct RemoteRow(Vec<Value>, Vec<libsql_replication::rpc::proxy::Column>);

impl RowInner for RemoteRow {
    fn column_value(&self, idx: i32) -> Result<Value> {
        self.0
            .get(idx as usize)
            .cloned()
            .ok_or(Error::InvalidColumnIndex)
    }

    fn column_name(&self, idx: i32) -> Option<&str> {
        self.1.get(idx as usize).map(|s| s.name.as_str())
    }

    fn column_str(&self, idx: i32) -> Result<&str> {
        let value = self.0.get(idx as usize).ok_or(Error::InvalidColumnIndex)?;

        match &value {
            Value::Text(s) => Ok(s.as_str()),
            _ => Err(Error::InvalidColumnType),
        }
    }

    fn column_type(&self, idx: i32) -> Result<ValueType> {
        let col = self.1.get(idx as usize).unwrap();
        col.decltype
            .as_deref()
            .and_then(|v| ValueType::from_str(v).ok())
            .map(ValueType::from)
            .ok_or(Error::InvalidColumnType)
    }
}

pub(super) struct RemoteTx(pub(super) Option<RemoteConnection>);

impl RemoteTx {
    pub(crate) async fn begin(
        conn: RemoteConnection,
        tx_behavior: TransactionBehavior,
    ) -> Result<Self> {
        let begin_stmt = match tx_behavior {
            TransactionBehavior::Deferred => "BEGIN DEFERRED",
            TransactionBehavior::Immediate => "BEGIN IMMEDIATE",
            TransactionBehavior::Exclusive => "BEGIN EXCLUSIVE",
            TransactionBehavior::ReadOnly => "BEGIN READONLY",
        };

        let _ = conn.execute(begin_stmt, Params::None).await?;
        Ok(Self(Some(conn)))
    }
}

#[async_trait::async_trait]
impl Tx for RemoteTx {
    async fn commit(&mut self) -> Result<()> {
        let conn = self.0.take().expect("Tx already dropped");
        conn.execute("COMMIT", Params::None).await?;
        Ok(())
    }

    async fn rollback(&mut self) -> Result<()> {
        let conn = self.0.take().expect("Tx already dropped");
        conn.execute("ROLLBACK", Params::None).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::parser::Statement;

    use super::{should_execute_local, State};

    #[track_caller]
    fn assert_should_execute_local(
        sql: &str,
        mut state: State,
        expected_final_state: State,
        expected_final_output: Result<bool, ()>,
    ) {
        let stmts = Statement::parse(sql)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        let out = should_execute_local(&mut state, &stmts[..]);
        assert_eq!(state, expected_final_state);
        assert_eq!(out.map_err(|_| ()), expected_final_output);
    }

    #[test]
    #[should_panic]
    fn invalid() {
        assert_should_execute_local(
            "
            BEGIN READONLY;
            SELECT 1;
            COMMIT;
            ",
            State::Txn,
            State::Invalid,
            Err(()),
        );
    }

    #[test]
    fn valid() {
        assert_should_execute_local(
            "
            BEGIN READONLY;
            SELECT 1;
            COMMIT;
            ",
            State::Init,
            State::Init,
            Ok(true),
        );

        assert_should_execute_local(
            "
            BEGIN READONLY;
            ",
            State::Init,
            State::TxnReadOnly,
            Ok(true),
        );

        assert_should_execute_local(
            "
            SELECT 1;
            ",
            State::TxnReadOnly,
            State::TxnReadOnly,
            Ok(true),
        );

        assert_should_execute_local(
            "
           COMMIT; 
            ",
            State::TxnReadOnly,
            State::Init,
            Ok(true),
        );

        assert_should_execute_local(
            "
            BEGIN READONLY;
            SELECT 1;
            COMMIT;
            BEGIN IMMEDIATE;
            SELECT 1; 
            COMMIT;
            ",
            State::Init,
            State::Init,
            Ok(false),
        );
    }
}
