use crate::hrana::pipeline::{
    BatchStreamReq, ClientMsg, CloseSqlStreamReq, DescribeStreamReq, ExecuteStreamReq, Response,
    SequenceStreamReq, ServerMsg, StoreSqlStreamReq, StreamRequest, StreamResponse,
    StreamResponseOk,
};
use crate::hrana::proto::{Batch, BatchResult, DescribeResult, Stmt, StmtResult};
use crate::hrana::{HranaError, HttpSend, Result};
use futures::lock::Mutex;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;

macro_rules! unexpected {
    ($value:ident) => {
        return Err(HranaError::UnexpectedResponse(format!(
            "Unexpected response from server: {:?}",
            $value
        )))
    };
}

pub type SqlId = i32;

/// A representation of Hrana HTTP stream. Since streams rely on sequential execution of requests,
/// it's realized internally as a mutex lock.
#[derive(Debug)]
pub struct HttpStream<T>
where
    T: for<'a> HttpSend<'a>,
{
    inner: Arc<Inner<T>>,
}

impl<T> Clone for HttpStream<T>
where
    T: for<'a> HttpSend<'a>,
{
    fn clone(&self) -> Self {
        HttpStream {
            inner: self.inner.clone(),
        }
    }
}

impl<T> HttpStream<T>
where
    T: for<'a> HttpSend<'a>,
{
    pub(super) fn open(client: T, base_url: String, auth_token: String) -> Self {
        HttpStream {
            inner: Arc::new(Inner {
                affected_row_count: AtomicU64::new(0),
                last_insert_rowid: AtomicI64::new(0),
                stream: Mutex::new(RawStream {
                    client,
                    base_url,
                    auth_token,
                    sql_id_generator: 0,
                    status: StreamStatus::Open,
                    baton: None,
                }),
            }),
        }
    }

    /// Executes a final request and immediately closes current stream - all in one request.
    pub async fn finalize(&mut self, req: StreamRequest) -> Result<StreamResponse> {
        let mut client = self.inner.stream.lock().await;
        let resp = client.finalize(req).await?;
        Ok(resp)
    }

    pub async fn execute(&self, stmt: Stmt) -> Result<StmtResult> {
        let mut client = self.inner.stream.lock().await;
        let resp = client
            .send(StreamRequest::Execute(ExecuteStreamReq { stmt }))
            .await?;
        match resp {
            StreamResponse::Execute(resp) => {
                let r = resp.result;
                self.inner
                    .affected_row_count
                    .store(r.affected_row_count, Ordering::SeqCst);
                self.inner
                    .last_insert_rowid
                    .store(r.last_insert_rowid.unwrap_or_default(), Ordering::SeqCst);
                Ok(r)
            }
            other => unexpected!(other),
        }
    }

    pub async fn batch(&self, batch: Batch) -> Result<BatchResult> {
        let mut client = self.inner.stream.lock().await;
        let resp = client
            .send(StreamRequest::Batch(BatchStreamReq { batch }))
            .await?;
        match resp {
            StreamResponse::Batch(resp) => Ok(resp.result),
            other => unexpected!(other),
        }
    }

    pub async fn store_sql(&self, sql: String) -> Result<StoredSql<T>> {
        let mut client = self.inner.stream.lock().await;
        let sql_id = client.next_sql_id();
        let resp = client
            .send(StreamRequest::StoreSql(StoreSqlStreamReq { sql, sql_id }))
            .await?;
        match resp {
            StreamResponse::StoreSql => {
                drop(client);
                Ok(StoredSql::new(self.clone(), sql_id))
            }
            other => unexpected!(other),
        }
    }

    async fn close_sql(&self, sql_id: SqlId) -> Result<()> {
        let mut client = self.inner.stream.lock().await;
        let resp = client
            .send(StreamRequest::CloseSql(CloseSqlStreamReq { sql_id }))
            .await?;
        match resp {
            StreamResponse::CloseSql => Ok(()),
            other => unexpected!(other),
        }
    }

    pub async fn describe<D: SqlDescriptor>(&self, descriptor: &D) -> Result<DescribeResult> {
        let mut client = self.inner.stream.lock().await;
        let req = match descriptor.sql_description() {
            SqlDescription::Sql(sql) => DescribeStreamReq {
                sql: Some(sql),
                sql_id: None,
            },
            SqlDescription::SqlId(sql_id) => DescribeStreamReq {
                sql: None,
                sql_id: Some(sql_id),
            },
        };
        let resp = client.send(StreamRequest::Describe(req)).await?;
        match resp {
            StreamResponse::Describe(resp) => Ok(resp.result),
            other => unexpected!(other),
        }
    }

    pub async fn sequence<D: SqlDescriptor>(&self, descriptor: &D) -> Result<()> {
        let mut client = self.inner.stream.lock().await;
        let req = match descriptor.sql_description() {
            SqlDescription::Sql(sql) => SequenceStreamReq {
                sql: Some(sql),
                sql_id: None,
            },
            SqlDescription::SqlId(sql_id) => SequenceStreamReq {
                sql: None,
                sql_id: Some(sql_id),
            },
        };
        let resp = client.send(StreamRequest::Sequence(req)).await?;
        match resp {
            StreamResponse::Sequence => Ok(()),
            other => unexpected!(other),
        }
    }

    pub async fn get_autocommit(&self) -> Result<bool> {
        let mut client = self.inner.stream.lock().await;
        let resp = client.send(StreamRequest::GetAutocommit).await?;
        match resp {
            StreamResponse::GetAutocommit(resp) => Ok(resp.is_autocommit),
            other => unexpected!(other),
        }
    }

    pub fn affected_row_count(&self) -> u64 {
        self.inner.affected_row_count.load(Ordering::SeqCst)
    }

    pub fn last_insert_rowid(&self) -> i64 {
        self.inner.last_insert_rowid.load(Ordering::SeqCst)
    }
}

#[derive(Debug)]
struct Inner<T>
where
    T: for<'a> HttpSend<'a>,
{
    affected_row_count: AtomicU64,
    last_insert_rowid: AtomicI64,
    stream: Mutex<RawStream<T>>,
}

#[derive(Debug)]
struct RawStream<T>
where
    T: for<'a> HttpSend<'a>,
{
    client: T,
    baton: Option<String>,
    base_url: String,
    auth_token: String,
    status: StreamStatus,
    sql_id_generator: SqlId,
}

impl<T> RawStream<T>
where
    T: for<'a> HttpSend<'a>,
{
    async fn send(&mut self, req: StreamRequest) -> Result<StreamResponse> {
        let [resp] = self.send_requests([req]).await?;
        Ok(resp)
    }

    async fn send_requests<const N: usize>(
        &mut self,
        requests: [StreamRequest; N],
    ) -> Result<[StreamResponse; N]> {
        if self.status == StreamStatus::Closed {
            return Err(HranaError::StreamClosed(
                "client stream has been closed by the servers".to_string(),
            ));
        }
        tracing::trace!(
            "client stream sending {} requests with baton `{}`",
            N,
            self.baton.as_deref().unwrap_or_default()
        );
        let msg = ClientMsg {
            baton: self.baton.clone(),
            requests: Vec::from(requests),
        };
        let body = serde_json::to_string(&msg).map_err(HranaError::Json)?;
        let mut response: ServerMsg = self
            .client
            .http_send(self.base_url.clone(), self.auth_token.clone(), body)
            .await?;
        if let Some(base_url) = response.base_url.take() {
            self.base_url = base_url;
        }
        match response.baton.take() {
            None => {
                tracing::trace!("client stream has been closed by the server");
                self.done();
            } // stream has been closed by the server
            Some(baton) => {
                tracing::trace!("client stream has been assigned with baton: `{}`", baton);
                self.baton = Some(baton)
            }
        }

        if response.results.is_empty() {
            Err(HranaError::UnexpectedResponse(format!(
                "Unexpected empty response from server: {:?}",
                response.results
            )))?;
        }
        if response.results.len() != N {
            // One with actual results, one closing the stream
            Err(HranaError::UnexpectedResponse(format!(
                "Unexpected multiple responses from server: {:?}",
                response.results
            )))?;
        }
        let mut responses = std::array::from_fn(|_| StreamResponse::Close);
        for i in 0..N {
            match response.results.swap_remove(0) {
                Response::Ok(StreamResponseOk { response }) => responses[i] = response,
                Response::Error(e) => return Err(HranaError::StreamError(e)),
            }
        }
        Ok(responses)
    }

    async fn finalize(&mut self, req: StreamRequest) -> Result<StreamResponse> {
        let [resp, _] = self.send_requests([req, StreamRequest::Close]).await?;
        self.done();
        Ok(resp)
    }

    fn done(&mut self) {
        if let Some(baton) = &self.baton {
            tracing::trace!("closing client stream (baton: `{}`)", baton);
        }
        self.baton = None;
        self.sql_id_generator = 0;
        self.status = StreamStatus::Closed;
    }

    async fn close(&mut self) -> Result<()> {
        if self.status == StreamStatus::Closed {
            return Ok(());
        }
        self.send(StreamRequest::Close).await?;
        self.done();
        Ok(())
    }

    fn next_sql_id(&mut self) -> SqlId {
        self.sql_id_generator = self.sql_id_generator.wrapping_add(1);
        self.sql_id_generator
    }
}

#[repr(u8)]
#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub enum StreamStatus {
    Open,
    Closed,
}

#[derive(Debug, Clone)]
pub struct StoredSql<T>
where
    T: for<'a> HttpSend<'a>,
{
    stream: HttpStream<T>,
    sql_id: SqlId,
}

impl<T> StoredSql<T>
where
    T: for<'a> HttpSend<'a>,
{
    fn new(stream: HttpStream<T>, sql_id: SqlId) -> Self {
        StoredSql { stream, sql_id }
    }

    pub async fn describe(&self) -> Result<DescribeResult> {
        self.stream.describe(self).await
    }

    pub async fn execute(&self) -> Result<()> {
        self.stream.sequence(self).await
    }

    pub async fn close(self) -> Result<()> {
        self.stream.close_sql(self.sql_id).await
    }
}

/// Trait that allows to refer to stored SQL statements. Stored SQL is first send by
/// the prepared statements as raw string and can be cached on the server side in scope of
/// a current transaction using a number identifier. This identifier can be send in subsequent
/// calls in place of SQL string to reduce the size of a message.
pub trait SqlDescriptor {
    fn sql_description(&self) -> SqlDescription;
}

impl SqlDescriptor for String {
    fn sql_description(&self) -> SqlDescription {
        SqlDescription::Sql(self.clone())
    }
}

impl<T> SqlDescriptor for StoredSql<T>
where
    T: for<'a> HttpSend<'a>,
{
    fn sql_description(&self) -> SqlDescription {
        SqlDescription::SqlId(self.sql_id)
    }
}

/// Enum used by stored SQL statements. It can refer to either fresh SQL statement text, or a
/// unique identifier assigned to that statement.
///
/// For the same statement executed many times over it's better to cache it first under [SqlId] key
/// and then re-execute it in subsequent calls using only identifier.
#[derive(Debug)]
pub enum SqlDescription {
    /// Non-cached SQL statement string.
    Sql(String),
    /// Key identifier of a SQL statement cached by the server. Key is valid only in
    /// the scope of current transaction/prepared statement, which sent a store SQL request.
    SqlId(SqlId),
}
