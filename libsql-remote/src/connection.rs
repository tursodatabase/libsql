use crate::pipeline::{
    ClientMsg, Response, ServerMsg, StreamBatchReq, StreamExecuteReq, StreamRequest,
    StreamResponse, StreamResponseOk,
};
use crate::proto::{Batch, BatchResult, Stmt, StmtResult};
use crate::{coerce_url_scheme, Error, HttpSend, Result, Statement};
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::atomic::{AtomicI64, AtomicU64};
use std::sync::{Arc, RwLock};

/// Information about the current session: the server-generated cookie
/// and the URL that should be used for further communication.
#[derive(Clone, Debug, Default)]
struct Cookie {
    baton: Option<String>,
    base_url: Option<String>,
}

#[derive(Debug)]
pub struct Connection<T>(Arc<InnerClient<T>>);

impl<T> Clone for Connection<T> {
    fn clone(&self) -> Self {
        Connection(self.0.clone())
    }
}

impl<T> Deref for Connection<T> {
    type Target = InnerClient<T>;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

#[derive(Debug)]
pub struct InnerClient<T> {
    inner: T,
    cookies: RwLock<HashMap<u64, Cookie>>,
    url_for_queries: String,
    auth: String,
    pub(crate) affected_row_count: AtomicU64,
    pub(crate) last_insert_rowid: AtomicI64,
}

impl<T> Connection<T>
where
    T: for<'a> HttpSend<'a>,
{
    pub fn new(url: String, token: String, inner: T) -> Self {
        // The `libsql://` protocol is an alias for `https://`.
        let base_url = coerce_url_scheme(&url);
        let url_for_queries = format!("{base_url}/v2/pipeline");
        Connection(Arc::new(InnerClient {
            inner,
            cookies: RwLock::new(HashMap::new()),
            url_for_queries,
            auth: format!("Bearer {token}"),
            affected_row_count: AtomicU64::new(0),
            last_insert_rowid: AtomicI64::new(0),
        }))
    }

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
        let body = serde_json::to_string(&msg).map_err(Error::Json)?;
        let mut response: ServerMsg = self
            .inner
            .http_send(self.url_for_queries.clone(), self.auth.clone(), body)
            .await?;

        if response.results.is_empty() {
            Err(Error::UnexpectedResponse(format!(
                "Unexpected empty response from server: {:?}",
                response.results
            )))?;
        }
        if response.results.len() > 2 {
            // One with actual results, one closing the stream
            Err(Error::UnexpectedResponse(format!(
                "Unexpected multiple responses from server: {:?}",
                response.results
            )))?;
        }
        match response.results.swap_remove(0) {
            Response::Ok(StreamResponseOk {
                response: StreamResponse::Batch(batch_result),
            }) => Ok(batch_result.result),
            Response::Ok(_) => Err(Error::UnexpectedResponse(format!(
                "Unexpected response from server: {:?}",
                response.results
            ))),
            Response::Error(e) => Err(Error::StreamError(e)),
        }
    }

    pub(crate) async fn execute_inner(&self, stmt: Stmt, tx_id: u64) -> Result<StmtResult> {
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
        let body = serde_json::to_string(&msg).map_err(Error::Json)?;
        let url = cookie
            .base_url
            .unwrap_or_else(|| self.url_for_queries.clone());
        let mut response: ServerMsg = self.inner.http_send(url, self.auth.clone(), body).await?;

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
                None => Err(Error::StreamClosed(
                    "Stream closed: server returned empty baton".into(),
                ))?,
            }
        }

        if response.results.is_empty() {
            Err(Error::UnexpectedResponse(format!(
                "Unexpected empty response from server: {:?}",
                response.results
            )))?;
        }
        if response.results.len() > 2 {
            // One with actual results, one closing the stream
            Err(Error::UnexpectedResponse(format!(
                "Unexpected multiple responses from server: {:?}",
                response.results
            )))?;
        }
        match response.results.swap_remove(0) {
            Response::Ok(StreamResponseOk {
                response: StreamResponse::Execute(execute_result),
            }) => Ok(execute_result.result),
            Response::Ok(_) => Err(Error::UnexpectedResponse(format!(
                "Unexpected response from server: {:?}",
                response.results
            ))),
            Response::Error(e) => Err(Error::StreamError(e)),
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
        let body = serde_json::to_string(&msg).map_err(Error::Json)?;
        self.inner
            .http_send(url, self.auth.clone(), body)
            .await
            .ok();
        self.cookies.write().unwrap().remove(&tx_id);
        Ok(())
    }

    pub fn prepare(&self, sql: &str) -> Statement<T> {
        Statement {
            client: self.clone(),
            inner: Stmt::new(sql, true),
        }
    }
}
