#![allow(dead_code)]

macro_rules! cfg_cloudflare {
    ($($item:item)*) => {
        $(
            #[cfg(feature = "cloudflare")]
            #[cfg_attr(docsrs, doc(cfg(feature = "cloudflare")))]
            $item
        )*
    }
}

cfg_cloudflare! {
    mod cloudflare;

    pub type DbConnection = crate::cloudflare::Connection<crate::cloudflare::CloudflareSender>;
}

pub mod connection;
mod params;
mod pipeline;
mod proto;

pub use params::{IntoParams, Params};
pub use pipeline::{
    ClientMsg, Response, ServerMsg, StreamBatchReq, StreamExecuteReq, StreamRequest,
    StreamResponse, StreamResponseError, StreamResponseOk,
};
pub use proto::Value;
use proto::{Col, Stmt, StmtResult};

use crate::connection::Connection;
use std::collections::VecDeque;
use std::future::Future;
use std::ops::Index;
use std::sync::atomic::Ordering;
use std::sync::Arc;

type Result<T> = std::result::Result<T, Error>;

pub trait HttpSend<'a> {
    type Result: Future<Output = Result<ServerMsg>> + 'a;
    fn http_send(&'a self, url: String, auth: String, body: String) -> Self::Result;
}

#[cfg(not(target_family = "wasm"))]
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[cfg(target_family = "wasm")]
pub type BoxError = Box<dyn std::error::Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
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
    #[error("SQL conversion failure: `{0}`")]
    ToSqlConversionFailure(BoxError),
    #[error("Column not found: {0}")]
    ColumnNotFound(i32),
}

pub struct Statement<T> {
    client: Connection<T>,
    inner: Stmt,
}

impl<T> Statement<T>
where
    T: for<'a> HttpSend<'a>,
{
    pub async fn execute(&mut self, params: &Params) -> Result<usize> {
        let mut stmt = self.inner.clone();
        bind_params(params.clone(), &mut stmt);

        let v = self.client.execute_inner(stmt, 0).await?;
        let affected_row_count = v.affected_row_count as usize;
        self.client
            .affected_row_count
            .store(affected_row_count as u64, Ordering::SeqCst);
        if let Some(last_insert_rowid) = v.last_insert_rowid {
            self.client
                .last_insert_rowid
                .store(last_insert_rowid, Ordering::SeqCst);
        }
        Ok(affected_row_count)
    }

    pub async fn query(&mut self, params: &Params) -> Result<Rows> {
        let mut stmt = self.inner.clone();
        bind_params(params.clone(), &mut stmt);

        let StmtResult { rows, cols, .. } = self.client.execute_inner(stmt, 0).await?;

        Ok(Rows {
            rows,
            cols: Arc::new(cols),
        })
    }
}

pub struct Rows {
    cols: Arc<Vec<Col>>,
    rows: VecDeque<Vec<Value>>,
}

impl Iterator for Rows {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        let row = self.rows.pop_front()?;
        Some(Row {
            cols: self.cols.clone(),
            inner: row,
        })
    }
}

impl Rows {
    pub fn column_count(&self) -> i32 {
        self.cols.len() as i32
    }

    pub fn column_name(&self, idx: i32) -> Option<&str> {
        self.cols
            .get(idx as usize)
            .and_then(|c| c.name.as_ref())
            .map(|s| s.as_str())
    }
}

pub struct Row {
    cols: Arc<Vec<Col>>,
    inner: Vec<Value>,
}

impl Row {
    pub fn column_value(&self, idx: i32) -> Option<Value> {
        self.inner.get(idx as usize).cloned()
    }

    pub fn column_name(&self, idx: i32) -> Option<&str> {
        self.cols
            .get(idx as usize)
            .and_then(|c| c.name.as_ref())
            .map(|s| s.as_str())
    }
}

impl Index<i32> for Row {
    type Output = Value;

    fn index(&self, index: i32) -> &Self::Output {
        &self.inner[index as usize]
    }
}

impl<'a> Index<&'a str> for Row {
    type Output = Value;

    fn index(&self, column_name: &'a str) -> &Self::Output {
        for (i, col) in self.cols.iter().enumerate() {
            match &col.name {
                Some(name) if name == column_name => {
                    return &self.inner[i];
                }
                _ => {}
            }
        }
        panic!("column `{column_name}` not found")
    }
}

fn bind_params(params: Params, stmt: &mut Stmt) {
    match params {
        Params::None => {}
        Params::Positional(values) => {
            for value in values {
                stmt.bind(value);
            }
        }
        Params::Named(values) => {
            for (name, value) in values {
                stmt.bind_named(name, value);
            }
        }
    }
}

pub fn coerce_url_scheme(url: &str) -> String {
    let mut url = url.replace("libsql://", "https://");
    if !url.contains("://") {
        url = format!("https://{}", url)
    }
    url
}
