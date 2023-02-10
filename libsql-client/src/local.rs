use super::{Meta, QueryResult, ResultSet, Row, Statement, Value};
use async_trait::async_trait;

pub use sqld_libsql_bindings::{
    libsql_compile_wasm_module, libsql_free_wasm_module, libsql_run_wasm, libsql_wasm_engine_new,
};

use rusqlite::types::Value as RusqliteValue;

/// Database connection. This is the main structure used to
/// communicate with the database.
#[derive(Debug)]
pub struct Connection {
    inner: rusqlite::Connection,
}

impl From<Value> for RusqliteValue {
    fn from(v: Value) -> Self {
        match v {
            Value::Null => RusqliteValue::Null,
            Value::Integer(n) => RusqliteValue::Integer(n),
            Value::Text(s) => RusqliteValue::Text(s),
            Value::Real(d) => RusqliteValue::Real(d),
            Value::Blob(b) => RusqliteValue::Blob(b),
        }
    }
}

impl From<RusqliteValue> for Value {
    fn from(v: RusqliteValue) -> Self {
        match v {
            RusqliteValue::Null => Value::Null,
            RusqliteValue::Integer(n) => Value::Integer(n),
            RusqliteValue::Text(s) => Value::Text(s),
            RusqliteValue::Real(d) => Value::Real(d),
            RusqliteValue::Blob(b) => Value::Blob(b),
        }
    }
}

impl Connection {
    /// Establishes a database connection.
    ///
    /// # Arguments
    /// * `path` - path of the local database
    pub fn connect(path: impl AsRef<std::path::Path>) -> anyhow::Result<Self> {
        Ok(Self {
            inner: rusqlite::Connection::open(path).map_err(|e| anyhow::anyhow!("{e}"))?,
        })
    }

    /// Executes a batch of SQL statements.
    /// Each statement is going to run in its own transaction,
    /// unless they're wrapped in BEGIN and END
    ///
    /// # Arguments
    /// * `stmts` - SQL statements
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn f() {
    /// let db = libsql_client::local::Connection::connect("/tmp/example321.db").unwrap();
    /// let result = db
    ///     .batch(["CREATE TABLE t(id)", "INSERT INTO t VALUES (42)"])
    ///     .await;
    /// # }
    /// ```
    pub async fn batch(
        &self,
        stmts: impl IntoIterator<Item = impl Into<Statement>>,
    ) -> anyhow::Result<Vec<QueryResult>> {
        let mut result = vec![];
        for stmt in stmts {
            let stmt = stmt.into();
            let sql_string = &stmt.q;
            let params =
                rusqlite::params_from_iter(stmt.params.into_iter().map(RusqliteValue::from));
            let mut stmt = self.inner.prepare(sql_string)?;
            let columns: Vec<String> = stmt
                .columns()
                .into_iter()
                .map(|c| c.name().to_string())
                .collect();
            let mut rows = Vec::new();
            let mut input_rows = match stmt.query(params) {
                Ok(rows) => rows,
                Err(e) => {
                    result.push(QueryResult::Error((format!("{e}"), Meta { duration: 0 })));
                    break;
                }
            };
            while let Some(row) = input_rows.next()? {
                let cells = columns
                    .iter()
                    .map(|col| {
                        (
                            col.clone(),
                            Value::from(row.get::<&str, RusqliteValue>(col.as_str()).unwrap()),
                        )
                    })
                    .collect();
                rows.push(Row { cells })
            }
            let meta = Meta { duration: 0 };
            let result_set = ResultSet { columns, rows };
            result.push(QueryResult::Success((result_set, meta)))
        }
        Ok(result)
    }
}

#[async_trait(?Send)]
impl super::Connection for Connection {
    async fn batch(
        &self,
        stmts: impl IntoIterator<Item = impl Into<Statement>>,
    ) -> anyhow::Result<Vec<QueryResult>> {
        self.batch(stmts).await
    }
}
