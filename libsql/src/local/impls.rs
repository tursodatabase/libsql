use std::fmt;
use std::sync::Arc;

use crate::{
    connection::Conn,
    params::Params,
    rows::{RowInner, RowsInner},
    statement::Stmt,
    transaction::Tx,
    Column, Connection, Result, Row, Rows, Statement, Transaction, TransactionBehavior, Value,
    ValueType,
};

#[derive(Clone)]
pub(crate) struct LibsqlConnection {
    pub(crate) conn: super::Connection,
}

#[async_trait::async_trait]
impl Conn for LibsqlConnection {
    async fn execute(&self, sql: &str, params: Params) -> Result<u64> {
        self.conn.execute(sql, params)
    }

    async fn execute_batch(&self, sql: &str) -> Result<()> {
        self.conn.execute_batch(sql)
    }

    async fn prepare(&self, sql: &str) -> Result<Statement> {
        let sql = sql.to_string();

        let stmt = self.conn.prepare(sql)?;

        Ok(Statement {
            inner: Box::new(LibsqlStmt(stmt)),
        })
    }

    async fn transaction(&self, tx_behavior: TransactionBehavior) -> Result<Transaction> {
        let tx = crate::local::Transaction::begin(self.conn.clone(), tx_behavior)?;
        // TODO(lucio): Can we just use the conn passed to the transaction?
        Ok(Transaction {
            inner: Box::new(LibsqlTx(Some(tx))),
            conn: Connection {
                conn: Arc::new(self.clone()),
            },
            close: None,
        })
    }

    fn is_autocommit(&self) -> bool {
        self.conn.is_autocommit()
    }

    fn changes(&self) -> u64 {
        self.conn.changes()
    }

    fn last_insert_rowid(&self) -> i64 {
        self.conn.last_insert_rowid()
    }

    async fn reset(&self) {}
}

impl Drop for LibsqlConnection {
    fn drop(&mut self) {
        self.conn.disconnect()
    }
}

pub(crate) struct LibsqlStmt(pub(super) crate::local::Statement);

#[async_trait::async_trait]
impl Stmt for LibsqlStmt {
    fn finalize(&mut self) {
        self.0.finalize();
    }

    async fn execute(&mut self, params: &Params) -> Result<usize> {
        let params = params.clone();
        let stmt = self.0.clone();

        stmt.execute(&params).map(|i| i as usize)
    }

    async fn query(&mut self, params: &Params) -> Result<Rows> {
        let params = params.clone();
        let stmt = self.0.clone();

        stmt.query(&params)
            .map(LibsqlRows)
            .map(|r| Rows { inner: Box::new(r) })
    }

    fn reset(&mut self) {
        self.0.reset();
    }

    fn parameter_count(&self) -> usize {
        self.0.parameter_count()
    }

    fn parameter_name(&self, idx: i32) -> Option<&str> {
        self.0.parameter_name(idx)
    }

    fn columns(&self) -> Vec<Column> {
        self.0.columns()
    }
}

pub(super) struct LibsqlTx(pub(super) Option<crate::local::Transaction>);

#[async_trait::async_trait]
impl Tx for LibsqlTx {
    async fn commit(&mut self) -> Result<()> {
        let tx = self.0.take().expect("Tx already dropped");
        tx.commit()
    }

    async fn rollback(&mut self) -> Result<()> {
        let tx = self.0.take().expect("Tx already dropped");
        tx.rollback()
    }
}

pub(crate) struct LibsqlRows(pub(crate) crate::local::Rows);

#[async_trait::async_trait]
impl RowsInner for LibsqlRows {
    async fn next(&mut self) -> Result<Option<Row>> {
        let row = self.0.next()?.map(|r| Row {
            inner: Box::new(LibsqlRow(r)),
        });

        Ok(row)
    }

    fn column_count(&self) -> i32 {
        self.0.column_count()
    }

    fn column_name(&self, idx: i32) -> Option<&str> {
        self.0.column_name(idx)
    }

    fn column_type(&self, idx: i32) -> Result<ValueType> {
        self.0.column_type(idx).map(ValueType::from)
    }
}

struct LibsqlRow(crate::local::Row);

impl RowInner for LibsqlRow {
    fn column_value(&self, idx: i32) -> Result<Value> {
        self.0.get_value(idx)
    }

    fn column_name(&self, idx: i32) -> Option<&str> {
        self.0.column_name(idx)
    }

    fn column_str(&self, idx: i32) -> Result<&str> {
        self.0.get::<&str>(idx)
    }

    fn column_type(&self, idx: i32) -> Result<ValueType> {
        self.0.column_type(idx).map(ValueType::from)
    }

    fn column_count(&self) -> usize {
        self.0.stmt.column_count()
    }
}

impl fmt::Debug for LibsqlRow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::result::Result<(), fmt::Error> {
        self.0.fmt(f)
    }
}
