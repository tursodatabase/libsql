mod pipeline;
mod proto;

use hyper::header::AUTHORIZATION;
use libsql_sys::ValueType;
use pipeline::{
    ClientMsg, Response, ServerMsg, StreamBatchReq, StreamExecuteReq, StreamRequest,
    StreamResponse, StreamResponseError, StreamResponseOk,
};
use proto::{Batch, BatchResult, Col, Stmt, StmtResult};

use hyper::client::HttpConnector;
use hyper::StatusCode;
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use tower::ServiceExt;

// use crate::client::Config;
use crate::{params::Params, Column, Result};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use super::rows::{RowInner, RowsInner};
use super::{Conn, ConnectorService, Socket, Transaction};

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
            return Err(HranaError::Api(body).into());
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
    #[error("missing environment variable: `{0}`")]
    MissingEnv(String),
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
    /// Creates a database client with JWT authentication.
    ///
    /// # Arguments
    /// * `url` - URL of the database endpoint
    /// * `token` - auth token
    pub fn new(url: impl Into<String>, token: impl Into<String>) -> Self {
        let connector = HttpConnector::new()
            .map_response(|s| Box::new(s) as Box<dyn Socket>)
            .map_err(|e| Box::new(e) as Box<_>);
        Self::new_with_connector(url, token, ConnectorService::new(connector))
    }

    pub fn from_env() -> Result<Client> {
        let url = std::env::var("LIBSQL_CLIENT_URL").map_err(|_| {
            HranaError::MissingEnv(
                "LIBSQL_CLIENT_URL variable should point to your sqld database".into(),
            )
        })?;

        let token = std::env::var("LIBSQL_CLIENT_TOKEN").unwrap_or_default();
        Ok(Client::new(url, token))
    }

    pub(crate) fn new_with_connector(
        url: impl Into<String>,
        token: impl Into<String>,
        connector: ConnectorService,
    ) -> Self {
        let inner = InnerClient::new(connector);

        let token = token.into();
        let url = url.into();
        // The `libsql://` protocol is an alias for `https://`.
        let url = url.replace("libsql://", "https://");
        // Auto-update the URL to start with https:// if no protocol was specified
        let base_url = if !url.contains("://") {
            format!("https://{}", &url)
        } else {
            url
        };
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
            ))
            .into()),
            Response::Error(e) => Err(HranaError::StreamError(e).into()),
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
            requests: vec![StreamRequest::Execute(StreamExecuteReq { stmt })],
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
        if response.results.len() > 1 {
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
            ))
            .into()),
            Response::Error(e) => Err(HranaError::StreamError(e).into()),
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
    async fn execute(&self, sql: &str, params: Params) -> Result<u64> {
        let mut stmt = self.prepare(sql).await?;
        let rows = stmt.execute(params).await?;

        Ok(rows as u64)
    }

    async fn execute_batch(&self, _sql: &str) -> Result<()> {
        todo!()
    }

    async fn prepare(&self, sql: &str) -> Result<super::Statement> {
        let stmt = Statement {
            client: self.clone(),
            inner: Stmt::new(sql, true),
        };
        Ok(super::Statement {
            inner: Box::new(stmt),
        })
    }

    async fn transaction(&self, _tx_behavior: crate::TransactionBehavior) -> Result<Transaction> {
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

    async fn execute(&mut self, params: &Params) -> Result<usize> {
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

    async fn query(&mut self, params: &Params) -> Result<super::Rows> {
        let mut stmt = self.inner.clone();
        bind_params(params.clone(), &mut stmt);

        let StmtResult { rows, cols, .. } = self.client.execute_inner(stmt, 0).await?;

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
    fn next(&mut self) -> Result<Option<super::Row>> {
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

    fn column_type(&self, _idx: i32) -> Result<ValueType> {
        todo!("implement")
    }
}

pub struct Row {
    cols: Arc<Vec<Col>>,
    inner: Vec<proto::Value>,
}

impl RowInner for Row {
    fn column_value(&self, idx: i32) -> Result<crate::Value> {
        let v = self.inner.get(idx as usize).cloned().unwrap();
        Ok(into_value2(v))
    }

    fn column_name(&self, idx: i32) -> Option<&str> {
        self.cols
            .get(idx as usize)
            .and_then(|c| c.name.as_ref())
            .map(|s| s.as_str())
    }

    fn column_str(&self, _idx: i32) -> Result<&str> {
        todo!()
    }

    fn column_type(&self, idx: i32) -> Result<ValueType> {
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
