use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{bail, Context as _, Result};

use super::{proto, Server};
use crate::database::Database;
use crate::error::Error;
use crate::query::{Params, Query, QueryResponse, Value};
use crate::query_analysis::Statement;

pub struct Session {
    streams: HashMap<u32, Stream>,
}

struct Stream {
    db: Arc<dyn Database>,
}

#[derive(thiserror::Error, Debug)]
pub enum ResponseError {
    #[error("Stream {stream_id} not found")]
    StreamNotFound { stream_id: u32 },
    #[error("Stream {stream_id} already exists")]
    StreamExists { stream_id: u32 },
    #[error("SQL string does not contain any statement")]
    SqlNoStmt,
    #[error("SQL string contains more than one statement")]
    SqlManyStmts,
    #[error("Arguments do not match SQL parameters: {source}")]
    InvalidArgs { source: anyhow::Error },
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

pub async fn handle_hello(_jwt: Option<String>) -> Result<Session> {
    // TODO: handle the jwt
    Ok(Session {
        streams: HashMap::new(),
    })
}

pub(super) async fn handle_request(
    server: &Server,
    session: &mut Session,
    req: proto::Request,
) -> Result<proto::Response> {
    match req {
        proto::Request::OpenStream(req) => {
            let stream_id = req.stream_id;

            if session.streams.contains_key(&stream_id) {
                bail!(ResponseError::StreamExists { stream_id })
            }

            let db = server
                .db_factory
                .create()
                .await
                .context("Could not create a database connection")?;
            let stream = Stream { db };
            session.streams.insert(stream_id, stream);

            Ok(proto::Response::OpenStream(proto::OpenStreamResp {}))
        }
        proto::Request::CloseStream(req) => {
            session.streams.remove(&req.stream_id);
            Ok(proto::Response::CloseStream(proto::CloseStreamResp {}))
        }
        proto::Request::Execute(req) => {
            let stream_id = req.stream_id;

            let Some(stream) = session.streams.get_mut(&stream_id) else {
                bail!(ResponseError::StreamNotFound { stream_id })
            };

            let result = execute_stmt(stream, req.stmt).await?;
            Ok(proto::Response::Execute(proto::ExecuteResp { result }))
        }
    }
}

async fn execute_stmt(stream: &mut Stream, stmt: proto::Stmt) -> Result<proto::StmtResult> {
    let query = proto_stmt_to_query(stmt)?;
    let (query_result, _) = stream.db.execute_one(query).await?;
    match query_result {
        Ok(query_response) => Ok(proto_stmt_result_from_query_response(query_response)),
        Err(error) => match ResponseError::try_from(error) {
            Ok(resp_error) => bail!(resp_error),
            Err(error) => bail!(error),
        },
    }
}

fn proto_stmt_to_query(proto_stmt: proto::Stmt) -> Result<Query> {
    let mut stmt_iter = Statement::parse(&proto_stmt.sql);
    let stmt = match stmt_iter.next() {
        Some(stmt_res) => stmt_res?,
        None => bail!(ResponseError::SqlNoStmt),
    };

    if stmt_iter.next().is_some() {
        bail!(ResponseError::SqlManyStmts)
    }

    let params = proto_stmt
        .args
        .into_iter()
        .map(proto_value_to_value)
        .collect();
    let params = Params::Positional(params);
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
    }
}

fn proto_value_to_value(proto_value: proto::Value) -> Value {
    match proto_value {
        proto::Value::Null => Value::Null,
        proto::Value::Integer { value } => Value::Integer(value),
        proto::Value::Float { value } => Value::Real(value),
        proto::Value::Text { value } => Value::Text(value),
        proto::Value::Blob { value } => Value::Blob(value),
    }
}

fn proto_value_from_value(value: Value) -> proto::Value {
    match value {
        Value::Null => proto::Value::Null,
        Value::Integer(value) => proto::Value::Integer { value },
        Value::Real(value) => proto::Value::Float { value },
        Value::Text(value) => proto::Value::Text { value },
        Value::Blob(value) => proto::Value::Blob { value },
    }
}

fn proto_response_error_from_error(error: Error) -> Result<ResponseError, Error> {
    Ok(match error {
        Error::LibSqlInvalidQueryParams(source) => ResponseError::InvalidArgs { source },
        Error::LibSqlTxTimeout(_) => ResponseError::TransactionTimeout,
        Error::LibSqlTxBusy => ResponseError::TransactionBusy,
        Error::RusqliteError(rusqlite_error) => match rusqlite_error {
            rusqlite::Error::SqliteFailure(sqlite_error, message) => ResponseError::SqliteError {
                source: sqlite_error,
                message,
            },
            rusqlite::Error::SqlInputError {
                error: sqlite_error,
                msg: message,
                offset,
                ..
            } => ResponseError::SqlInputError {
                source: sqlite_error,
                message,
                offset,
            },
            rusqlite_error => return Err(Error::RusqliteError(rusqlite_error)),
        },
        error => return Err(error),
    })
}

impl From<proto::Value> for Value {
    fn from(proto_value: proto::Value) -> Value {
        proto_value_to_value(proto_value)
    }
}

impl From<Value> for proto::Value {
    fn from(value: Value) -> proto::Value {
        proto_value_from_value(value)
    }
}

impl TryFrom<Error> for ResponseError {
    type Error = Error;
    fn try_from(error: Error) -> Result<ResponseError, Error> {
        proto_response_error_from_error(error)
    }
}
