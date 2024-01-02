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

use crate::hrana::cursor::{Cursor, Error, OwnedCursorStep};
pub(crate) use crate::hrana::pipeline::StreamResponseError;
use crate::hrana::proto::{Batch, Col, Stmt};
use crate::hrana::stream::HranaStream;
use crate::{params::Params, ValueType};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use super::rows::{RowInner, RowsInner};

pub(crate) type Result<T> = std::result::Result<T, HranaError>;

/// Information about the current session: the server-generated cookie
/// and the URL that should be used for further communication.
#[derive(Clone, Debug, Default)]
struct Cookie {
    baton: Option<String>,
    base_url: Option<String>,
}

pub trait HttpSend: Clone {
    type Stream: Stream<Item = std::io::Result<Bytes>> + Unpin;
    type Result: Future<Output = Result<Self::Stream>>;
    fn http_send(&self, url: Arc<str>, auth: Arc<str>, body: String) -> Self::Result;

    /// Schedule sending a HTTP post request without waiting for the completion.
    fn oneshot(self, url: Arc<str>, auth: Arc<str>, body: String);
}

pub enum HttpBody<S> {
    Body(Option<Bytes>),
    Stream(S),
}

impl<S> From<Bytes> for HttpBody<S> {
    fn from(value: Bytes) -> Self {
        HttpBody::Body(Some(value))
    }
}

impl<S> Stream for HttpBody<S>
where
    S: Stream<Item = std::io::Result<Bytes>> + Unpin,
{
    type Item = std::io::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.get_mut() {
            HttpBody::Body(bytes) => {
                if let Some(bytes) = bytes.take() {
                    Poll::Ready(Some(Ok(bytes)))
                } else {
                    Poll::Ready(None)
                }
            }
            HttpBody::Stream(stream) => {
                let pinned = Pin::new(stream);
                pinned.poll_next(cx)
            }
        }
    }
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
    #[error("error at step {step}: {error}")]
    StepError { step: u32, error: Error },
    #[error("cursor stream ended prematurely")]
    CursorClosed,
    #[error("cursor hasn't fetched any rows yet")]
    NoRowsFetched,
    #[error("{0}")]
    Other(String),
}

pub struct Statement<T>
where
    T: HttpSend,
{
    stream: HranaStream<T>,
    inner: Stmt,
}

impl<T> Statement<T>
where
    T: HttpSend,
{
    pub(crate) fn new(stream: HranaStream<T>, sql: String, want_rows: bool) -> Self {
        let inner = Stmt::new(sql, want_rows);
        Statement { stream, inner }
    }

    pub async fn execute(&mut self, params: &Params) -> crate::Result<usize> {
        let mut stmt = self.inner.clone();
        bind_params(params.clone(), &mut stmt);

        let mut v = self.stream.cursor(Batch::single(stmt)).await?;
        let mut step = v
            .next_step()
            .await
            .map_err(|e| crate::Error::Hrana(e.into()))?;
        step.consume()
            .await
            .map_err(|e| crate::Error::Hrana(e.into()))?;
        Ok(step.affected_rows() as usize)
    }

    pub(crate) async fn query_raw(
        &mut self,
        params: &Params,
    ) -> crate::Result<HranaRows<T::Stream>> {
        let mut stmt = self.inner.clone();
        bind_params(params.clone(), &mut stmt);

        let cursor = self.stream.cursor(Batch::single(stmt)).await?;
        let rows = HranaRows::from_cursor(cursor).await?;

        Ok(rows)
    }
}
impl<T> Statement<T>
where
    T: HttpSend,
    <T as HttpSend>::Stream: Send + Sync + 'static,
{
    pub async fn query(&mut self, params: &Params) -> crate::Result<super::Rows> {
        let rows = self.query_raw(params).await?;
        Ok(super::Rows {
            inner: Box::new(rows),
        })
    }
}

pub struct HranaRows<S> {
    cursor_step: OwnedCursorStep<S>,
    column_types: Option<Vec<ValueType>>,
}

impl<S> HranaRows<S>
where
    S: Stream<Item = std::io::Result<Bytes>> + Unpin,
{
    async fn from_cursor(cursor: Cursor<S>) -> Result<Self> {
        let cursor_step = cursor.next_step_owned().await?;
        Ok(HranaRows {
            cursor_step,
            column_types: None,
        })
    }

    pub async fn next(&mut self) -> crate::Result<Option<super::Row>> {
        let row = match self.cursor_step.next().await {
            Some(Ok(row)) => row,
            Some(Err(e)) => return Err(crate::Error::Hrana(Box::new(e))),
            None => return Ok(None),
        };

        if self.column_types.is_none() {
            self.init_column_types(&row);
        }

        Ok(Some(super::Row {
            inner: Box::new(row),
        }))
    }

    fn init_column_types(&mut self, row: &Row) {
        self.column_types = Some(
            row.inner
                .iter()
                .map(|value| match value {
                    proto::Value::Null => ValueType::Null,
                    proto::Value::Integer { value: _ } => ValueType::Integer,
                    proto::Value::Float { value: _ } => ValueType::Real,
                    proto::Value::Text { value: _ } => ValueType::Text,
                    proto::Value::Blob { value: _ } => ValueType::Blob,
                })
                .collect(),
        );
    }

    pub fn column_count(&self) -> i32 {
        self.cursor_step.cols().len() as i32
    }

    pub fn column_name(&self, idx: i32) -> Option<&str> {
        self.cursor_step
            .cols()
            .get(idx as usize)
            .and_then(|c| c.name.as_ref())
            .map(|s| s.as_str())
    }
}

#[async_trait::async_trait]
impl<S> RowsInner for HranaRows<S>
where
    S: Stream<Item = std::io::Result<Bytes>> + Send + Sync + Unpin,
{
    async fn next(&mut self) -> crate::Result<Option<super::Row>> {
        self.next().await
    }

    fn column_count(&self) -> i32 {
        self.column_count()
    }

    fn column_name(&self, idx: i32) -> Option<&str> {
        self.column_name(idx)
    }

    fn column_type(&self, idx: i32) -> crate::Result<ValueType> {
        if let Some(col_types) = &self.column_types {
            if let Some(t) = col_types.get(idx as usize) {
                Ok(*t)
            } else {
                Err(crate::Error::ColumnNotFound(idx))
            }
        } else {
            Err(crate::Error::Hrana(Box::new(HranaError::CursorError(
                CursorResponseError::NoRowsFetched,
            ))))
        }
    }
}

#[derive(Debug)]
pub struct Row {
    cols: Arc<[Col]>,
    inner: Vec<proto::Value>,
}

impl Row {
    pub(super) fn new(cols: Arc<[Col]>, inner: Vec<proto::Value>) -> Self {
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

    fn column_str(&self, idx: i32) -> crate::Result<&str> {
        if let Some(value) = self.inner.get(idx as usize) {
            if let proto::Value::Text { value } = value {
                Ok(&value)
            } else {
                Err(crate::Error::InvalidColumnType)
            }
        } else {
            Err(crate::Error::ColumnNotFound(idx))
        }
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
