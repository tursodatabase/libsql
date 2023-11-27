#![allow(dead_code)]

pub mod connection;

cfg_remote! {
    mod hyper;
}

mod cursor;
pub mod pipeline;
pub mod proto;
mod stream;
pub mod transaction;

use crate::hrana::connection::HttpConnection;
pub(crate) use crate::hrana::pipeline::StreamResponseError;
use crate::hrana::proto::{Col, Stmt, StmtResult};
use crate::hrana::stream::HranaStream;
use crate::{params::Params, ValueType};
use bytes::{Bytes, BytesMut};
use futures::Stream;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio_stream::StreamExt;

use super::rows::{RowInner, RowsInner};

pub(crate) type Result<T> = std::result::Result<T, HranaError>;

/// Information about the current session: the server-generated cookie
/// and the URL that should be used for further communication.
#[derive(Clone, Debug, Default)]
struct Cookie {
    baton: Option<String>,
    base_url: Option<String>,
}

pub trait HttpSend<'a>: Clone {
    type Result: Future<Output = Result<HttpBody>> + 'a;
    fn http_send(&'a self, url: &'a str, auth: &'a str, body: String) -> Self::Result;
}

#[cfg(feature = "wasm")]
pub type ByteStream = Box<dyn Stream<Item = Result<Bytes>> + Unpin>;

#[cfg(not(feature = "wasm"))]
pub type ByteStream = Box<dyn Stream<Item = Result<Bytes>> + Send + Unpin>;

pub enum HttpBody {
    Body(Bytes),
    Stream(ByteStream),
}

impl HttpBody {
    pub async fn bytes(self) -> Result<Bytes> {
        match self {
            HttpBody::Body(bytes) => Ok(bytes),
            HttpBody::Stream(stream) => stream_to_bytes(stream).await,
        }
    }

    pub fn stream(self) -> ByteStream {
        match self {
            HttpBody::Stream(stream) => stream,
            HttpBody::Body(bytes) => Box::new(SimpleStream::new(bytes)),
        }
    }
}

struct SimpleStream(Option<Bytes>);
impl SimpleStream {
    fn new(bytes: Bytes) -> Self {
        SimpleStream(Some(bytes))
    }
}
impl Stream for SimpleStream {
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.0.take() {
            None => Poll::Ready(None),
            Some(bytes) => Poll::Ready(Some(Ok(bytes))),
        }
    }
}

async fn stream_to_bytes(mut stream: ByteStream) -> Result<Bytes> {
    let mut buf = BytesMut::new();
    while let Some(chunk) = stream.next().await {
        buf.extend_from_slice(&chunk?);
    }
    Ok(buf.freeze())
}

#[derive(Debug, thiserror::Error)]
pub enum HranaError {
    #[error("unexpected response: `{0}`")]
    UnexpectedResponse(String),
    #[error("stream closed: `{0}`")]
    StreamClosed(String),
    #[error("stream error: `{0:?}`")]
    StreamError(StreamResponseError),
    #[error("cursor error: `{0}`")]
    CursorError(CursorResponseError),
    #[error("json error: `{0}`")]
    Json(#[from] serde_json::Error),
    #[error("http error: `{0}`")]
    Http(String),
    #[error("api error: `{0}`")]
    Api(String),
}

#[derive(Debug, thiserror::Error)]
pub enum CursorResponseError {
    #[error("cursor step {actual} arrived before step {expected} end message")]
    NotClosed { expected: u32, actual: u32 },
    #[error("error at step {step}: `{error}`")]
    StepError { step: u32, error: String },
    #[error("cursor stream ended prematurely")]
    CursorClosed,
    #[error("{0}")]
    Other(String),
}

enum StatementExecutor<T: for<'a> HttpSend<'a>> {
    /// An opened HTTP Hrana stream - usually in scope of executing transaction. Operations over it
    /// will be scheduled for sequential execution.
    Stream(HranaStream<T>),
    /// Hrana HTTP connection. Operations executing over it are not attached to any sequential
    /// order of execution.
    Connection(HttpConnection<T>),
}

impl<T> StatementExecutor<T>
where
    T: for<'a> HttpSend<'a>,
{
    async fn execute(&self, stmt: Stmt) -> crate::Result<StmtResult> {
        let res = match self {
            StatementExecutor::Stream(stream) => stream.execute(stmt).await,
            StatementExecutor::Connection(conn) => conn.execute_inner(stmt).await,
        };
        res.map_err(|e| crate::Error::Hrana(e.into()))
    }
}

pub struct Statement<T>
where
    T: for<'a> HttpSend<'a>,
{
    executor: StatementExecutor<T>,
    inner: Stmt,
}

impl<T> Statement<T>
where
    T: for<'a> HttpSend<'a>,
{
    pub(crate) fn from_stream(stream: HranaStream<T>, sql: String, want_rows: bool) -> Self {
        Statement {
            executor: StatementExecutor::Stream(stream),
            inner: Stmt::new(sql, want_rows),
        }
    }

    pub(crate) fn from_connection(conn: HttpConnection<T>, sql: String, want_rows: bool) -> Self {
        Statement {
            executor: StatementExecutor::Connection(conn),
            inner: Stmt::new(sql, want_rows),
        }
    }

    pub async fn execute(&mut self, params: &Params) -> crate::Result<usize> {
        let mut stmt = self.inner.clone();
        bind_params(params.clone(), &mut stmt);

        let v = self.executor.execute(stmt).await?;
        Ok(v.affected_row_count as usize)
    }

    pub async fn query(&mut self, params: &Params) -> crate::Result<super::Rows> {
        let mut stmt = self.inner.clone();
        bind_params(params.clone(), &mut stmt);

        let StmtResult { rows, cols, .. } = self.executor.execute(stmt).await?;

        Ok(super::Rows {
            inner: Box::new(Rows {
                rows,
                cols: Arc::new(cols),
            }),
        })
    }
}

pub struct Rows {
    cols: Arc<Vec<Col>>,
    rows: VecDeque<Vec<proto::Value>>,
}

impl RowsInner for Rows {
    fn next(&mut self) -> crate::Result<Option<super::Row>> {
        let row = match self.rows.pop_front() {
            Some(row) => Row {
                cols: self.cols.clone(),
                inner: row,
            },
            None => return Ok(None),
        };

        Ok(Some(super::Row {
            inner: Box::new(row),
        }))
    }

    fn column_count(&self) -> i32 {
        self.cols.len() as i32
    }

    fn column_name(&self, idx: i32) -> Option<&str> {
        self.cols
            .get(idx as usize)
            .and_then(|c| c.name.as_ref())
            .map(|s| s.as_str())
    }

    fn column_type(&self, idx: i32) -> crate::Result<ValueType> {
        let row = match self.rows.get(0) {
            None => return Err(crate::Error::QueryReturnedNoRows),
            Some(row) => row,
        };
        let cell = match row.get(idx as usize) {
            None => return Err(crate::Error::ColumnNotFound(idx)),
            Some(cell) => cell,
        };
        Ok(match cell {
            proto::Value::Null => ValueType::Null,
            proto::Value::Integer { .. } => ValueType::Integer,
            proto::Value::Float { .. } => ValueType::Real,
            proto::Value::Text { .. } => ValueType::Text,
            proto::Value::Blob { .. } => ValueType::Blob,
        })
    }
}

#[derive(Debug)]
pub struct Row {
    cols: Arc<Vec<Col>>,
    inner: Vec<proto::Value>,
}

impl Row {
    pub(super) fn new(cols: Arc<Vec<Col>>, inner: Vec<proto::Value>) -> Self {
        Row { cols, inner }
    }
}

impl RowInner for Row {
    fn column_value(&self, idx: i32) -> crate::Result<crate::Value> {
        let v = self.inner.get(idx as usize).cloned().unwrap();
        Ok(into_value2(v))
    }

    fn column_name(&self, idx: i32) -> Option<&str> {
        self.cols
            .get(idx as usize)
            .and_then(|c| c.name.as_ref())
            .map(|s| s.as_str())
    }

    fn column_str(&self, _idx: i32) -> crate::Result<&str> {
        todo!()
    }

    fn column_type(&self, idx: i32) -> crate::Result<ValueType> {
        if let Some(value) = self.inner.get(idx as usize) {
            Ok(match value {
                proto::Value::Null => ValueType::Null,
                proto::Value::Integer { value: _ } => ValueType::Integer,
                proto::Value::Float { value: _ } => ValueType::Real,
                proto::Value::Text { value: _ } => ValueType::Text,
                proto::Value::Blob { value: _ } => ValueType::Blob,
            })
        } else {
            Err(crate::Error::ColumnNotFound(idx))
        }
    }

    fn column_count(&self) -> usize {
        self.cols.len()
    }
}

pub(super) fn bind_params(params: Params, stmt: &mut Stmt) {
    match params {
        Params::None => {}
        Params::Positional(values) => {
            for value in values {
                stmt.bind(into_value(value));
            }
        }
        Params::Named(values) => {
            for (name, value) in values {
                stmt.bind_named(name, into_value(value));
            }
        }
    }
}

fn into_value(value: crate::Value) -> proto::Value {
    match value {
        crate::Value::Null => proto::Value::Null,
        crate::Value::Integer(value) => proto::Value::Integer { value },
        crate::Value::Real(value) => proto::Value::Float { value },
        crate::Value::Text(value) => proto::Value::Text { value },
        crate::Value::Blob(value) => proto::Value::Blob { value },
    }
}

fn into_value2(value: proto::Value) -> crate::Value {
    match value {
        proto::Value::Null => crate::Value::Null,
        proto::Value::Integer { value } => crate::Value::Integer(value),
        proto::Value::Float { value } => crate::Value::Real(value),
        proto::Value::Text { value } => crate::Value::Text(value),
        proto::Value::Blob { value } => crate::Value::Blob(value),
    }
}
