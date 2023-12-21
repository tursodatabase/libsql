use crate::hrana::pipeline::{BatchStreamReq, ExecuteStreamReq, StreamRequest, StreamResponse};
use crate::hrana::proto::{Batch, BatchResult, Stmt, StmtResult};
use crate::hrana::stream::HranaStream;
use crate::hrana::{HranaError, HttpSend, Result, Statement};
use crate::util::coerce_url_scheme;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug)]
pub struct HttpConnection<T>(Arc<InnerClient<T>>)
where
    T: HttpSend;

#[derive(Debug)]
struct InnerClient<T>
where
    T: HttpSend,
{
    inner: T,
    pipeline_url: Arc<str>,
    cursor_url: Arc<str>,
    auth: Arc<str>,
    affected_row_count: AtomicU64,
    last_insert_rowid: AtomicI64,
    is_autocommit: AtomicBool,
}

impl<T> HttpConnection<T>
where
    T: HttpSend,
{
    pub fn new(url: String, token: String, inner: T) -> Self {
        // The `libsql://` protocol is an alias for `https://`.
        let base_url = coerce_url_scheme(&url);
        let pipeline_url = Arc::from(format!("{base_url}/v3/pipeline"));
        let cursor_url = Arc::from(format!("{base_url}/v3/cursor"));
        HttpConnection(Arc::new(InnerClient {
            inner,
            pipeline_url,
            cursor_url,
            auth: Arc::from(format!("Bearer {token}")),
            affected_row_count: AtomicU64::new(0),
            last_insert_rowid: AtomicI64::new(0),
            is_autocommit: AtomicBool::new(true),
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

    pub fn is_autocommit(&self) -> bool {
        self.client().is_autocommit.load(Ordering::SeqCst)
    }

    fn set_autocommit(&self, value: bool) {
        self.client().is_autocommit.store(value, Ordering::SeqCst)
    }

    fn client(&self) -> &InnerClient<T> {
        &self.0
    }

    pub(crate) fn open_stream(&self) -> HranaStream<T> {
        let client = self.client();
        HranaStream::open(
            client.inner.clone(),
            client.pipeline_url.clone(),
            client.cursor_url.clone(),
            client.auth.clone(),
        )
    }

    pub(crate) async fn batch_inner(
        &self,
        stmts: impl IntoIterator<Item = Stmt>,
    ) -> Result<BatchResult> {
        let batch = Batch::from_iter(stmts, false);
        let (resp, is_autocommit) = self
            .open_stream()
            .finalize(StreamRequest::Batch(BatchStreamReq { batch }))
            .await?;
        self.set_autocommit(is_autocommit);
        match resp {
            StreamResponse::Batch(resp) => Ok(resp.result),
            other => Err(HranaError::UnexpectedResponse(format!(
                "Unexpected response from server: {:?}",
                other
            ))),
        }
    }

    pub(crate) async fn execute_inner(&self, stmt: Stmt) -> Result<StmtResult> {
        let (resp, is_autocommit) = self
            .open_stream()
            .finalize(StreamRequest::Execute(ExecuteStreamReq { stmt }))
            .await?;
        self.set_autocommit(is_autocommit);
        match resp {
            StreamResponse::Execute(resp) => Ok(resp.result),
            other => Err(HranaError::UnexpectedResponse(format!(
                "Unexpected response from server: {:?}",
                other
            ))),
        }
    }

    pub fn prepare(&self, sql: &str) -> Statement<T> {
        Statement::from_connection(self.clone(), sql.to_string(), true)
    }
}

impl<T> Clone for HttpConnection<T>
where
    T: HttpSend,
{
    fn clone(&self) -> Self {
        HttpConnection(self.0.clone())
    }
}

pub(crate) enum CommitBehavior {
    Commit,
    Rollback,
}
