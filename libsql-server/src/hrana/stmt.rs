use anyhow::{anyhow, bail, Result};
use std::collections::HashMap;

use super::result_builder::SingleStatementBuilder;
use super::{proto, ProtocolError, Version};
use crate::connection::program::DescribeResponse;
use crate::connection::{Connection, RequestContext};
use crate::error::Error as SqldError;
use crate::hrana;
use crate::query::{Params, Query, Value};
use crate::query_analysis::Statement;
use crate::query_result_builder::{QueryResultBuilder, QueryResultBuilderError};
use crate::replication::FrameNo;

/// An error during execution of an SQL statement.
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
    #[error("SQLite error: {message}")]
    SqliteError {
        source: rusqlite::ffi::Error,
        message: String,
    },
    #[error("SQL input error: {message} (at offset {offset})")]
    SqlInputError {
        source: rusqlite::ffi::Error,
        message: String,
        offset: i32,
    },

    #[error("Operation was blocked{}", .reason.as_ref().map(|msg| format!(": {}", msg)).unwrap_or_default())]
    Blocked { reason: Option<String> },
    #[error("Response is too large")]
    ResponseTooLarge,
    #[error("error executing a request on the primary: {0}")]
    Proxy(String),
}

pub async fn execute_stmt(
    db: &impl Connection,
    ctx: RequestContext,
    query: Query,
    replication_index: Option<FrameNo>,
) -> Result<proto::StmtResult> {
    let builder = SingleStatementBuilder::default();
    let stmt_res = db
        .execute_batch(vec![query], ctx, builder, replication_index)
        .await
        .map_err(catch_stmt_error)?;
    stmt_res.into_ret().map_err(catch_stmt_error)
}

pub async fn describe_stmt(
    db: &impl Connection,
    ctx: RequestContext,
    sql: String,
    replication_index: Option<FrameNo>,
) -> Result<proto::DescribeResult> {
    match db.describe(sql, ctx, replication_index).await? {
        Ok(describe_response) => Ok(proto_describe_result_from_describe_response(
            describe_response,
        )),
        Err(sqld_error) => match stmt_error_from_sqld_error(sqld_error) {
            Ok(stmt_error) => bail!(stmt_error),
            Err(sqld_error) => bail!(sqld_error),
        },
    }
}

pub fn proto_stmt_to_query(
    proto_stmt: &proto::Stmt,
    sqls: &HashMap<i32, String>,
    version: Version,
) -> Result<Query> {
    let sql = proto_sql_to_sql(proto_stmt.sql.as_deref(), proto_stmt.sql_id, sqls, version)?;

    let mut stmt_iter = Statement::parse(sql);
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
            .collect::<Result<Vec<_>, _>>()?;
        Params::Positional(values)
    } else if proto_stmt.args.is_empty() {
        let values = proto_stmt
            .named_args
            .iter()
            .map(|arg| {
                proto_value_to_value(&arg.value).map(|arg_value| (arg.name.clone(), arg_value))
            })
            .collect::<Result<HashMap<_, _>, _>>()?;
        Params::Named(values)
    } else {
        bail!(StmtError::ArgsBothPositionalAndNamed)
    };

    let want_rows = proto_stmt.want_rows.unwrap_or(true);
    Ok(Query {
        stmt,
        params,
        want_rows,
    })
}

pub fn proto_sql_to_sql<'s>(
    proto_sql: Option<&'s str>,
    proto_sql_id: Option<i32>,
    sqls: &'s HashMap<i32, String>,
    version: Version,
) -> Result<&'s str, ProtocolError> {
    if proto_sql_id.is_some() && version < Version::Hrana2 {
        return Err(ProtocolError::NotSupported {
            what: "`sql_id`",
            min_version: Version::Hrana2,
        });
    }

    match (proto_sql, proto_sql_id) {
        (Some(sql), None) => Ok(sql),
        (None, Some(sql_id)) => match sqls.get(&sql_id) {
            Some(sql) => Ok(sql),
            None => Err(ProtocolError::SqlNotFound { sql_id }),
        },
        (Some(_), Some(_)) => Err(ProtocolError::SqlIdAndSqlGiven),
        (None, None) => Err(ProtocolError::SqlIdOrSqlNotGiven),
    }
}

fn proto_value_to_value(proto_value: &proto::Value) -> Result<Value, ProtocolError> {
    Ok(match proto_value {
        proto::Value::None => return Err(ProtocolError::NoneValue),
        proto::Value::Null => Value::Null,
        proto::Value::Integer { value } => Value::Integer(*value),
        proto::Value::Float { value } => Value::Real(*value),
        proto::Value::Text { value } => Value::Text(value.as_ref().into()),
        proto::Value::Blob { value } => Value::Blob(value.as_ref().into()),
    })
}

fn proto_value_from_value(value: Value) -> proto::Value {
    match value {
        Value::Null => proto::Value::Null,
        Value::Integer(value) => proto::Value::Integer { value },
        Value::Real(value) => proto::Value::Float { value },
        Value::Text(value) => proto::Value::Text {
            value: value.into(),
        },
        Value::Blob(value) => proto::Value::Blob {
            value: value.into(),
        },
    }
}

fn proto_describe_result_from_describe_response(
    response: DescribeResponse,
) -> proto::DescribeResult {
    proto::DescribeResult {
        params: response
            .params
            .into_iter()
            .map(|p| proto::DescribeParam { name: p.name })
            .collect(),
        cols: response
            .cols
            .into_iter()
            .map(|c| proto::DescribeCol {
                name: c.name,
                decltype: c.decltype,
            })
            .collect(),
        is_explain: response.is_explain,
        is_readonly: response.is_readonly,
    }
}

fn catch_stmt_error(sqld_error: SqldError) -> anyhow::Error {
    match stmt_error_from_sqld_error(sqld_error) {
        Ok(stmt_error) => anyhow!(stmt_error),
        Err(sqld_error) => anyhow!(sqld_error),
    }
}

pub fn stmt_error_from_sqld_error(sqld_error: SqldError) -> Result<StmtError, SqldError> {
    Ok(match sqld_error {
        SqldError::LibSqlInvalidQueryParams(source) => StmtError::ArgsInvalid { source },
        SqldError::LibSqlTxTimeout => StmtError::TransactionTimeout,
        SqldError::LibSqlTxBusy => StmtError::TransactionBusy,
        SqldError::BuilderError(QueryResultBuilderError::ResponseTooLarge(_)) => {
            StmtError::ResponseTooLarge
        }
        SqldError::Blocked(reason) => StmtError::Blocked { reason },
        SqldError::RpcQueryError(e) => StmtError::Proxy(e.message),
        SqldError::RusqliteError(rusqlite_error)
        | SqldError::RusqliteErrorExtended(rusqlite_error, _) => match rusqlite_error {
            rusqlite::Error::SqliteFailure(sqlite_error, Some(message)) => StmtError::SqliteError {
                source: sqlite_error,
                message,
            },
            rusqlite::Error::SqliteFailure(sqlite_error, None) => StmtError::SqliteError {
                message: sqlite_error.to_string(),
                source: sqlite_error,
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
    proto::Error {
        message: error.to_string(),
        code: error.code().into(),
    }
}

impl StmtError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::SqlParse { .. } => "SQL_PARSE_ERROR",
            Self::SqlNoStmt => "SQL_NO_STATEMENT",
            Self::SqlManyStmts => "SQL_MANY_STATEMENTS",
            Self::ArgsInvalid { .. } => "ARGS_INVALID",
            Self::ArgsBothPositionalAndNamed => "ARGS_BOTH_POSITIONAL_AND_NAMED",
            Self::TransactionTimeout => "TRANSACTION_TIMEOUT",
            Self::TransactionBusy => "TRANSACTION_BUSY",
            Self::SqliteError { source, .. } => sqlite_error_code(source.code),
            Self::SqlInputError { .. } => "SQL_INPUT_ERROR",
            Self::Blocked { .. } => "BLOCKED",
            Self::ResponseTooLarge => "RESPONSE_TOO_LARGE",
            Self::Proxy(_) => "PROXY_ERROR",
        }
    }
}

fn sqlite_error_code(code: rusqlite::ffi::ErrorCode) -> &'static str {
    match code {
        rusqlite::ErrorCode::InternalMalfunction => "SQLITE_INTERNAL",
        rusqlite::ErrorCode::PermissionDenied => "SQLITE_PERM",
        rusqlite::ErrorCode::OperationAborted => "SQLITE_ABORT",
        rusqlite::ErrorCode::DatabaseBusy => "SQLITE_BUSY",
        rusqlite::ErrorCode::DatabaseLocked => "SQLITE_LOCKED",
        rusqlite::ErrorCode::OutOfMemory => "SQLITE_NOMEM",
        rusqlite::ErrorCode::ReadOnly => "SQLITE_READONLY",
        rusqlite::ErrorCode::OperationInterrupted => "SQLITE_INTERRUPT",
        rusqlite::ErrorCode::SystemIoFailure => "SQLITE_IOERR",
        rusqlite::ErrorCode::DatabaseCorrupt => "SQLITE_CORRUPT",
        rusqlite::ErrorCode::NotFound => "SQLITE_NOTFOUND",
        rusqlite::ErrorCode::DiskFull => "SQLITE_FULL",
        rusqlite::ErrorCode::CannotOpen => "SQLITE_CANTOPEN",
        rusqlite::ErrorCode::FileLockingProtocolFailed => "SQLITE_PROTOCOL",
        rusqlite::ErrorCode::SchemaChanged => "SQLITE_SCHEMA",
        rusqlite::ErrorCode::TooBig => "SQLITE_TOOBIG",
        rusqlite::ErrorCode::ConstraintViolation => "SQLITE_CONSTRAINT",
        rusqlite::ErrorCode::TypeMismatch => "SQLITE_MISMATCH",
        rusqlite::ErrorCode::ApiMisuse => "SQLITE_MISUSE",
        rusqlite::ErrorCode::NoLargeFileSupport => "SQLITE_NOLFS",
        rusqlite::ErrorCode::AuthorizationForStatementDenied => "SQLITE_AUTH",
        rusqlite::ErrorCode::ParameterOutOfRange => "SQLITE_RANGE",
        rusqlite::ErrorCode::NotADatabase => "SQLITE_NOTADB",
        rusqlite::ErrorCode::Unknown => "SQLITE_UNKNOWN",
        _ => "SQLITE_UNKNOWN",
    }
}

impl From<Value> for proto::Value {
    fn from(value: Value) -> proto::Value {
        proto_value_from_value(value)
    }
}
