use crate::hrana::pipeline::{BatchStreamReq, ExecuteStreamReq, StreamRequest, StreamResponse};
use crate::hrana::proto::{Batch, BatchResult, Stmt, StmtResult};
use crate::hrana::stream::HttpStream;
use crate::hrana::{HranaError, HttpSend, Result, Statement};
use crate::util::coerce_url_scheme;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

#[derive(Debug)]
pub struct HttpConnection<T>(Arc<InnerClient<T>>);

#[derive(Debug)]
struct InnerClient<T> {
    inner: T,
    streams: RwLock<HashMap<u64, HttpStream<T>>>,
    url_for_queries: String,
    auth: String,
    affected_row_count: AtomicU64,
    last_insert_rowid: AtomicI64,
}

impl<T> HttpConnection<T>
where
    T: for<'a> HttpSend<'a> + Clone,
{
    pub fn new(url: String, token: String, inner: T) -> Self {
        // The `libsql://` protocol is an alias for `https://`.
        let base_url = coerce_url_scheme(&url);
        let url_for_queries = format!("{base_url}/v2/pipeline");
        HttpConnection(Arc::new(InnerClient {
            inner,
            streams: RwLock::new(HashMap::new()),
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

    pub(super) fn open_stream(&self) -> HttpStream<T> {
        let client = self.client();
        HttpStream::open(
            client.inner.clone(),
            client.url_for_queries.clone(),
            client.auth.clone(),
        )
    }

    pub(crate) async fn raw_batch(
        &self,
        stmts: impl IntoIterator<Item = Stmt>,
    ) -> Result<BatchResult> {
        let mut batch = Batch::new();
        for stmt in stmts.into_iter() {
            batch.step(None, stmt);
        }
        let resp = self
            .open_stream()
            .finalize(StreamRequest::Batch(BatchStreamReq { batch }))
            .await?;
        match resp {
            StreamResponse::Batch(resp) => Ok(resp.result),
            other => Err(HranaError::UnexpectedResponse(format!(
                "Unexpected response from server: {:?}",
                other
            ))),
        }
    }

    pub(crate) async fn execute_inner(&self, stmt: Stmt) -> Result<StmtResult> {
        let resp = self
            .open_stream()
            .finalize(StreamRequest::Execute(ExecuteStreamReq { stmt }))
            .await?;
        match resp {
            StreamResponse::Execute(resp) => Ok(resp.result),
            other => Err(HranaError::UnexpectedResponse(format!(
                "Unexpected response from server: {:?}",
                other
            ))),
        }
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

pub(crate) enum CommitBehavior {
    Commit,
    Rollback,
}
