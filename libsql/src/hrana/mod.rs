#![allow(dead_code)]

mod pipeline;
mod proto;

use crate::util::coerce_url_scheme;
use hyper::header::AUTHORIZATION;
use pipeline::{
    ClientMsg, Response, ServerMsg, StreamBatchReq, StreamExecuteReq, StreamRequest,
    StreamResponse, StreamResponseError, StreamResponseOk,
};
use proto::{Batch, BatchResult, Col, Stmt, StmtResult};

use hyper::StatusCode;
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};

use crate::util::ConnectorService;
use crate::Error;
use crate::{params::Params, Column, ValueType};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use super::rows::{RowInner, RowsInner};
use crate::connection::Conn;
use crate::transaction::Transaction;

type Result<T> = std::result::Result<T, HranaError>;

/// Information about the current session: the server-generated cookie
/// and the URL that should be used for further communication.
#[derive(Clone, Debug, Default)]
struct Cookie {
    baton: Option<String>,
    base_url: Option<String>,
}

/// Generic HTTP client. Needs a helper function that actually sends
/// the request.
#[derive(Clone, Debug)]
pub struct Client {
    inner: InnerClient,
    cookies: Arc<RwLock<HashMap<u64, Cookie>>>,
    url_for_queries: String,
    auth: String,
    affected_row_count: Arc<AtomicU64>,
    last_insert_rowid: Arc<AtomicI64>,
}

#[derive(Clone, Debug)]
struct InnerClient {
    inner: hyper::Client<HttpsConnector<ConnectorService>, hyper::Body>,
}

impl InnerClient {
    fn new(connector: ConnectorService) -> Self {
        let https = HttpsConnectorBuilder::new()
            .with_native_roots()
            .https_or_http()
            .enable_http1()
            .wrap_connector(connector);
        let inner = hyper::Client::builder().build(https);

        Self { inner }
    }

    async fn send(&self, url: String, auth: String, body: String) -> Result<ServerMsg> {
        let req = hyper::Request::post(url)
            .header(AUTHORIZATION, auth)
            .body(hyper::Body::from(body))
            .unwrap();

        let res = self.inner.request(req).await.map_err(HranaError::from)?;

        if res.status() != StatusCode::OK {
            let body = hyper::body::to_bytes(res.into_body())
                .await
                .map_err(HranaError::from)?;
            let body = String::from_utf8(body.into()).unwrap();
            return Err(HranaError::Api(body));
        }

        let body = hyper::body::to_bytes(res.into_body())
            .await
            .map_err(HranaError::from)?;

        let msg = serde_json::from_slice::<ServerMsg>(&body[..]).map_err(HranaError::from)?;

        Ok(msg)
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
    #[error("json error: `{0}`")]
    Json(#[from] serde_json::Error),
    #[error("http error: `{0}`")]
    Http(#[from] hyper::Error),
    #[error("api error: `{0}`")]
    Api(String),
}

impl Client {
    pub(crate) fn new_with_connector(
        url: impl Into<String>,
        token: impl Into<String>,
        connector: ConnectorService,
    ) -> Self {
        let inner = InnerClient::new(connector);

        let token = token.into();
        let url = url.into();
        // The `libsql://` protocol is an alias for `https://`.
        let base_url = coerce_url_scheme(&url);
        let url_for_queries = format!("{base_url}/v2/pipeline");
        Self {
            inner,
            cookies: Arc::new(RwLock::new(HashMap::new())),
            url_for_queries,
            auth: format!("Bearer {token}"),
            affected_row_count: Arc::new(AtomicU64::new(0)),
            last_insert_rowid: Arc::new(AtomicI64::new(0)),
        }
    }
}

impl Client {
    pub async fn raw_batch(&self, stmts: impl IntoIterator<Item = Stmt>) -> Result<BatchResult> {
        let mut batch = Batch::new();
        for stmt in stmts.into_iter() {
            batch.step(None, stmt);
        }

        let msg = ClientMsg {
            baton: None,
            requests: vec![
                StreamRequest::Batch(StreamBatchReq { batch }),
                StreamRequest::Close,
            ],
        };
        let body = serde_json::to_string(&msg).map_err(HranaError::Json)?;
        let mut response: ServerMsg = self
            .inner
            .send(self.url_for_queries.clone(), self.auth.clone(), body)
            .await?;

        if response.results.is_empty() {
            Err(HranaError::UnexpectedResponse(format!(
                "Unexpected empty response from server: {:?}",
                response.results
            )))?;
        }
        if response.results.len() > 2 {
            // One with actual results, one closing the stream
            Err(HranaError::UnexpectedResponse(format!(
                "Unexpected multiple responses from server: {:?}",
                response.results
            )))?;
        }
        match response.results.swap_remove(0) {
            Response::Ok(StreamResponseOk {
                response: StreamResponse::Batch(batch_result),
            }) => Ok(batch_result.result),
            Response::Ok(_) => Err(HranaError::UnexpectedResponse(format!(
                "Unexpected response from server: {:?}",
                response.results
            ))),
            Response::Error(e) => Err(HranaError::StreamError(e)),
        }
    }

    async fn execute_inner(&self, stmt: Stmt, tx_id: u64) -> Result<StmtResult> {
        let cookie = if tx_id > 0 {
            self.cookies
                .read()
                .unwrap()
                .get(&tx_id)
                .cloned()
                .unwrap_or_default()
        } else {
            Cookie::default()
        };
        let msg = ClientMsg {
            baton: cookie.baton,
            requests: vec![
                StreamRequest::Execute(StreamExecuteReq { stmt }),
                StreamRequest::Close,
            ],
        };
        let body = serde_json::to_string(&msg).map_err(HranaError::Json)?;
        let url = cookie
            .base_url
            .unwrap_or_else(|| self.url_for_queries.clone());
        let mut response: ServerMsg = self.inner.send(url, self.auth.clone(), body).await?;

        if tx_id > 0 {
            let base_url = response.base_url;
            match response.baton {
                Some(baton) => {
                    self.cookies.write().unwrap().insert(
                        tx_id,
                        Cookie {
                            baton: Some(baton),
                            base_url,
                        },
                    );
                }
                None => Err(HranaError::StreamClosed(
                    "Stream closed: server returned empty baton".into(),
                ))?,
            }
        }

        if response.results.is_empty() {
            Err(HranaError::UnexpectedResponse(format!(
                "Unexpected empty response from server: {:?}",
                response.results
            )))?;
        }
        if response.results.len() > 2 {
            // One with actual results, one closing the stream
            Err(HranaError::UnexpectedResponse(format!(
                "Unexpected multiple responses from server: {:?}",
                response.results
            )))?;
        }
        match response.results.swap_remove(0) {
            Response::Ok(StreamResponseOk {
                response: StreamResponse::Execute(execute_result),
            }) => Ok(execute_result.result),
            Response::Ok(_) => Err(HranaError::UnexpectedResponse(format!(
                "Unexpected response from server: {:?}",
                response.results
            ))),
            Response::Error(e) => Err(HranaError::StreamError(e)),
        }
    }

    async fn _close_stream_for(&self, tx_id: u64) -> Result<()> {
        let cookie = self
            .cookies
            .read()
            .unwrap()
            .get(&tx_id)
            .cloned()
            .unwrap_or_default();
        let msg = ClientMsg {
            baton: cookie.baton,
            requests: vec![StreamRequest::Close],
        };
        let url = cookie
            .base_url
            .unwrap_or_else(|| self.url_for_queries.clone());
        let body = serde_json::to_string(&msg).map_err(HranaError::Json)?;
        self.inner.send(url, self.auth.clone(), body).await.ok();
        self.cookies.write().unwrap().remove(&tx_id);
        Ok(())
    }
}

#[async_trait::async_trait]
impl Conn for Client {
    async fn execute(&self, sql: &str, params: Params) -> crate::Result<u64> {
        let mut stmt = self.prepare(sql).await?;
        let rows = stmt.execute(params).await?;

        Ok(rows as u64)
    }

    async fn execute_batch(&self, _sql: &str) -> crate::Result<()> {
        todo!()
    }

    async fn prepare(&self, sql: &str) -> crate::Result<super::Statement> {
        let stmt = Statement {
            client: self.clone(),
            inner: Stmt::new(sql, true),
        };
        Ok(super::Statement {
            inner: Box::new(stmt),
        })
    }

    async fn transaction(
        &self,
        _tx_behavior: crate::TransactionBehavior,
    ) -> crate::Result<Transaction> {
        todo!()
    }

    fn is_autocommit(&self) -> bool {
        // TODO: Is this correct?
        false
    }

    fn changes(&self) -> u64 {
        self.affected_row_count.load(Ordering::SeqCst)
    }

    fn last_insert_rowid(&self) -> i64 {
        self.last_insert_rowid.load(Ordering::SeqCst)
    }

    fn close(&mut self) {
        todo!()
    }
}

pub struct Statement {
    client: Client,
    inner: Stmt,
}

#[async_trait::async_trait]
impl super::statement::Stmt for Statement {
    fn finalize(&mut self) {}

    async fn execute(&mut self, params: &Params) -> crate::Result<usize> {
        let mut stmt = self.inner.clone();
        bind_params(params.clone(), &mut stmt);

        let v = self
            .client
            .execute_inner(stmt, 0)
            .await
            .map_err(|e| Error::Hrana(e.into()))?;
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

    async fn query(&mut self, params: &Params) -> crate::Result<super::Rows> {
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

    fn reset(&mut self) {}

    fn parameter_count(&self) -> usize {
        todo!()
    }

    fn parameter_name(&self, _idx: i32) -> Option<&str> {
        todo!()
    }

    fn columns(&self) -> Vec<Column> {
        todo!()
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

    fn column_type(&self, _idx: i32) -> crate::Result<ValueType> {
        todo!("implement")
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

    fn column_index(&self, name: &str) -> Option<i32> {
        self.cols
            .iter()
            .position(|c| c.name.as_ref().map_or(false, |s| s == name))
            .map(|idx| idx as i32)
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
