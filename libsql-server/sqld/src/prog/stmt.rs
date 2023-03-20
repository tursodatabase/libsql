use anyhow::{Result, bail};

use super::proto;
use crate::database::Database;
use crate::error::Error as SqldError;
use crate::hrana;
use crate::query::{Params, Query, QueryResponse, Value};
use crate::query_analysis::Statement;

#[derive(thiserror::Error, Debug)]
pub enum StmtError {
    #[error("SQL string could not be parsed: {source}")]
    SqlParse { source: anyhow::Error },
    #[error("SQL string does not contain any statement")]
    SqlNoStmt,
    #[error("SQL string contains more than one statement")]
    SqlManyStmts,
    #[error("Arguments do not match SQL parameters: {source}")]
    ArgsInvalid { source: anyhow::Error },
    #[error("Specifying both positional and named arguments is not supported")]
    ArgsBothPositionalAndNamed,

    #[error("Transaction timed out")]
    TransactionTimeout,
    #[error("Server cannot handle additional transactions")]
    TransactionBusy,
    #[error("SQLite error: {source}: {message:?}")]
    SqliteError {
        source: rusqlite::ffi::Error,
        message: Option<String>,
    },
    #[error("SQL input error: {source}: {message:?} at offset {offset}")]
    SqlInputError {
        source: rusqlite::ffi::Error,
        message: String,
        offset: i32,
    },
}

pub async fn execute_stmt(db: &dyn Database, stmt: &proto::Stmt) -> Result<proto::StmtResult> {
    let query = proto_stmt_to_query(stmt)?;
    let (query_result, _) = db.execute_one(query).await?;
    match query_result {
        Ok(query_response) => Ok(proto_stmt_result_from_query_response(query_response)),
        Err(sqld_error) => match stmt_error_from_sqld_error(sqld_error) {
            Ok(stmt_error) => bail!(stmt_error),
            Err(sqld_error) => bail!(sqld_error),
        },
    }
}

fn proto_stmt_to_query(proto_stmt: &proto::Stmt) -> Result<Query> {
    let mut stmt_iter = Statement::parse(&proto_stmt.sql);
    let stmt = match stmt_iter.next() {
        Some(Ok(stmt)) => stmt,
        Some(Err(err)) => bail!(StmtError::SqlParse { source: err }),
        None => bail!(StmtError::SqlNoStmt),
    };

    if stmt_iter.next().is_some() {
        bail!(StmtError::SqlManyStmts)
    }

    let params = if proto_stmt.named_args.is_empty() {
        let values = proto_stmt
            .args
            .iter()
            .map(proto_value_to_value)
            .collect();
        Params::Positional(values)
    } else if proto_stmt.args.is_empty() {
        let values = proto_stmt
            .named_args
            .iter()
            .map(|arg| (arg.name.clone(), proto_value_to_value(&arg.value)))
            .collect();
        Params::Named(values)
    } else {
        bail!(StmtError::ArgsBothPositionalAndNamed)
    };

    Ok(Query { stmt, params })
}

fn proto_stmt_result_from_query_response(query_response: QueryResponse) -> proto::StmtResult {
    let QueryResponse::ResultSet(result_set) = query_response;
    let proto_cols = result_set
        .columns
        .into_iter()
        .map(|col| proto::Col {
            name: Some(col.name),
        })
        .collect();
    let proto_rows = result_set
        .rows
        .into_iter()
        .map(|row| row.values.into_iter().map(proto::Value::from).collect())
        .collect();
    proto::StmtResult {
        cols: proto_cols,
        rows: proto_rows,
        affected_row_count: result_set.affected_row_count,
        last_insert_rowid: result_set.last_insert_rowid,
    }
}

fn proto_value_to_value(proto_value: &proto::Value) -> Value {
    match proto_value {
        proto::Value::Null => Value::Null,
        proto::Value::Integer { value } => Value::Integer(*value),
        proto::Value::Float { value } => Value::Real(*value),
        proto::Value::Text { value } => Value::Text(value.as_ref().into()),
        proto::Value::Blob { value } => Value::Blob(value.as_ref().into()),
    }
}

fn proto_value_from_value(value: Value) -> proto::Value {
    match value {
        Value::Null => proto::Value::Null,
        Value::Integer(value) => proto::Value::Integer { value },
        Value::Real(value) => proto::Value::Float { value },
        Value::Text(value) => proto::Value::Text { value: value.into() },
        Value::Blob(value) => proto::Value::Blob { value: value.into() },
    }
}

fn stmt_error_from_sqld_error(sqld_error: SqldError) -> Result<StmtError, SqldError> {
    Ok(match sqld_error {
        SqldError::LibSqlInvalidQueryParams(source) => StmtError::ArgsInvalid { source },
        SqldError::LibSqlTxTimeout(_) => StmtError::TransactionTimeout,
        SqldError::LibSqlTxBusy => StmtError::TransactionBusy,
        SqldError::RusqliteError(rusqlite_error) => match rusqlite_error {
            rusqlite::Error::SqliteFailure(sqlite_error, message) => StmtError::SqliteError {
                source: sqlite_error,
                message,
            },
            rusqlite::Error::SqlInputError {
                error: sqlite_error,
                msg: message,
                offset,
                ..
            } => StmtError::SqlInputError {
                source: sqlite_error,
                message,
                offset,
            },
            rusqlite_error => return Err(SqldError::RusqliteError(rusqlite_error)),
        },
        sqld_error => return Err(sqld_error),
    })
}

pub fn proto_error_from_stmt_error(error: &StmtError) -> hrana::proto::Error {
    hrana::proto::Error { message: error.to_string() }
}

impl From<&proto::Value> for Value {
    fn from(proto_value: &proto::Value) -> Value {
        proto_value_to_value(proto_value)
    }
}

impl From<Value> for proto::Value {
    fn from(value: Value) -> proto::Value {
        proto_value_from_value(value)
    }
}

