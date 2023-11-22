use crate::hrana::pipeline::{BatchStreamReq, ExecuteStreamReq, StreamRequest, StreamResponse};
use crate::hrana::proto::{Batch, BatchCond, BatchResult, Stmt, StmtResult};
use crate::hrana::stream::HttpStream;
use crate::hrana::{HranaError, HttpSend, Result, Statement};
use crate::util::coerce_url_scheme;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug)]
pub struct HttpConnection<T>(Arc<InnerClient<T>>)
where
    T: for<'a> HttpSend<'a>;

#[derive(Debug)]
struct InnerClient<T>
where
    T: for<'a> HttpSend<'a>,
{
    inner: T,
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

    pub(crate) fn open_stream(&self) -> HttpStream<T> {
        let client = self.client();
        HttpStream::open(
            client.inner.clone(),
            client.url_for_queries.clone(),
            client.auth.clone(),
        )
    }

    pub(crate) async fn batch_inner(
        &self,
        stmts: impl IntoIterator<Item = Stmt>,
    ) -> Result<BatchResult> {
        let batch = stmts_to_batch(false, stmts);
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
        Statement::from_connection(self.clone(), sql.to_string(), true)
    }
}

impl<T> Clone for HttpConnection<T>
where
    T: for<'a> HttpSend<'a>,
{
    fn clone(&self) -> Self {
        HttpConnection(self.0.clone())
    }
}

pub(crate) enum CommitBehavior {
    Commit,
    Rollback,
}

pub(super) fn stmts_to_batch(protocol_v3: bool, stmts: impl IntoIterator<Item = Stmt>) -> Batch {
    let mut batch = Batch::new();
    let mut step = -1;
    for stmt in stmts.into_iter() {
        let cond = if step >= 0 {
            let mut cond = BatchCond::Ok { step };
            if protocol_v3 {
                cond = BatchCond::And {
                    conds: vec![cond, BatchCond::IsAutocommit],
                };
            }
            Some(cond)
        } else {
            None
        };
        batch.step(cond, stmt);
        step += 1;
    }
    batch
}
