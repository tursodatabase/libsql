#![allow(dead_code)]

pub mod connection;

cfg_remote! {
    mod hyper;
}

pub mod pipeline;
pub mod proto;

use crate::hrana::connection::HttpConnection;
pub(crate) use crate::hrana::pipeline::{ServerMsg, StreamResponseError};
use crate::hrana::proto::{Col, Stmt, StmtResult};
use crate::Error;
use crate::{params::Params, ValueType};
use std::collections::VecDeque;
use std::future::Future;
use std::sync::Arc;

use super::rows::{RowInner, RowsInner};

pub(crate) type Result<T> = std::result::Result<T, HranaError>;

/// Information about the current session: the server-generated cookie
/// and the URL that should be used for further communication.
#[derive(Clone, Debug, Default)]
struct Cookie {
    baton: Option<String>,
    base_url: Option<String>,
}

pub trait HttpSend<'a> {
    type Result: Future<Output = Result<ServerMsg>> + 'a;
    fn http_send(&'a self, url: String, auth: String, body: String) -> Self::Result;
}

#[derive(Debug, thiserror::Error)]
pub enum HranaError {
    #[error("unexpected response: `{0}`")]
    UnexpectedResponse(String),
    #[error("stream closed: `{0}`")]
    StreamClosed(String),
    #[error("stream error: `{0:?}`")]
    StreamError(StreamResponseError),
    #[error("json error: `{0}`")]
    Json(#[from] serde_json::Error),
    #[error("http error: `{0}`")]
    Http(String),
    #[error("api error: `{0}`")]
    Api(String),
}

pub struct Statement<T> {
    client: HttpConnection<T>,
    inner: Stmt,
}

impl<T> Statement<T>
where
    T: for<'a> HttpSend<'a>,
{
    pub(crate) fn new(conn: HttpConnection<T>, sql: String, want_rows: bool) -> Self {
        Statement {
            client: conn,
            inner: Stmt::new(sql, want_rows),
        }
    }

    pub async fn execute(&mut self, params: &Params) -> crate::Result<usize> {
        let mut stmt = self.inner.clone();
        bind_params(params.clone(), &mut stmt);

        let v = self
            .client
            .execute_inner(stmt, 0)
            .await
            .map_err(|e| Error::Hrana(e.into()))?;
        let affected_row_count = v.affected_row_count as usize;
        self.client
            .set_affected_row_count(affected_row_count as u64);
        if let Some(last_insert_rowid) = v.last_insert_rowid {
            self.client.set_last_insert_rowid(last_insert_rowid);
        }
        Ok(affected_row_count)
    }

    pub async fn query(&mut self, params: &Params) -> crate::Result<super::Rows> {
        let mut stmt = self.inner.clone();
        bind_params(params.clone(), &mut stmt);

        let StmtResult { rows, cols, .. } = self
            .client
            .execute_inner(stmt, 0)
            .await
            .map_err(|e| Error::Hrana(e.into()))?;

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
}

fn bind_params(params: Params, stmt: &mut Stmt) {
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
