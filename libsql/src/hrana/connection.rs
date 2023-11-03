use crate::hrana::pipeline::{
    ClientMsg, Response, ServerMsg, StreamBatchReq, StreamExecuteReq, StreamRequest,
    StreamResponse, StreamResponseOk,
};
use crate::hrana::proto::{Batch, BatchResult, Stmt, StmtResult};
use crate::hrana::{HranaError, HttpSend, Result, Statement};
use crate::util::coerce_url_scheme;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

/// Information about the current session: the server-generated cookie
/// and the URL that should be used for further communication.
#[derive(Clone, Debug, Default)]
struct Cookie {
    baton: Option<String>,
    base_url: Option<String>,
}

#[derive(Debug)]
pub struct HttpConnection<T>(Arc<InnerClient<T>>);

#[derive(Debug)]
struct InnerClient<T> {
    inner: T,
    cookies: RwLock<HashMap<u64, Cookie>>,
    url_for_queries: String,
    auth: String,
    affected_row_count: AtomicU64,
    last_insert_rowid: AtomicI64,
}

impl<T> HttpConnection<T>
where
    T: for<'a> HttpSend<'a>,
{
    pub fn new(url: String, token: String, inner: T) -> Self {
        // The `libsql://` protocol is an alias for `https://`.
        let base_url = coerce_url_scheme(&url);
        let url_for_queries = format!("{base_url}/v2/pipeline");
        HttpConnection(Arc::new(InnerClient {
            inner,
            cookies: RwLock::new(HashMap::new()),
            url_for_queries,
            auth: format!("Bearer {token}"),
            affected_row_count: AtomicU64::new(0),
            last_insert_rowid: AtomicI64::new(0),
        }))
    }

    pub fn affected_row_count(&self) -> u64 {
        self.client().affected_row_count.load(Ordering::SeqCst)
    }

    pub fn set_affected_row_count(&self, value: u64) {
        self.client()
            .affected_row_count
            .store(value, Ordering::SeqCst)
    }

    pub fn last_insert_rowid(&self) -> i64 {
        self.client().last_insert_rowid.load(Ordering::SeqCst)
    }

    pub fn set_last_insert_rowid(&self, value: i64) {
        self.client()
            .last_insert_rowid
            .store(value, Ordering::SeqCst)
    }

    fn client(&self) -> &InnerClient<T> {
        &self.0
    }

    pub(crate) async fn raw_batch(
        &self,
        stmts: impl IntoIterator<Item = Stmt>,
    ) -> Result<BatchResult> {
        let client = self.client();
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
        let mut response: ServerMsg = client
            .inner
            .http_send(client.url_for_queries.clone(), client.auth.clone(), body)
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

    pub(crate) async fn execute_inner(&self, stmt: Stmt, tx_id: u64) -> Result<StmtResult> {
        let client = self.client();
        let cookie = if tx_id > 0 {
            client
                .cookies
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
            .unwrap_or_else(|| client.url_for_queries.clone());
        let mut response: ServerMsg = client
            .inner
            .http_send(url, client.auth.clone(), body)
            .await?;

        if tx_id > 0 {
            let base_url = response.base_url;
            match response.baton {
                Some(baton) => {
                    client.cookies.write().unwrap().insert(
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
        let client = self.client();
        let cookie = client
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
            .unwrap_or_else(|| client.url_for_queries.clone());
        let body = serde_json::to_string(&msg).map_err(HranaError::Json)?;
        client
            .inner
            .http_send(url, client.auth.clone(), body)
            .await
            .ok();
        client.cookies.write().unwrap().remove(&tx_id);
        Ok(())
    }

    pub fn prepare(&self, sql: &str) -> Statement<T> {
        Statement {
            client: self.clone(),
            inner: Stmt::new(sql, true),
        }
    }
}

impl<T> Clone for HttpConnection<T> {
    fn clone(&self) -> Self {
        HttpConnection(self.0.clone())
    }
}
