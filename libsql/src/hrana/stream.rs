use crate::hrana::cursor::{Cursor, CursorReq};
use crate::hrana::pipeline::{
    ClientMsg, CloseSqlStreamReq, DescribeStreamReq, Response, SequenceStreamReq, ServerMsg,
    StoreSqlStreamReq, StreamRequest, StreamResponse, StreamResponseOk,
};
use crate::hrana::proto::{Batch, BatchResult, DescribeResult, Stmt, StmtResult};
use crate::hrana::{CursorResponseError, HranaError, HttpSend, Result, StreamResponseError};
use bytes::{Bytes, BytesMut};
use futures::lock::Mutex;
use futures::Stream;
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
pub struct HranaStream<T>
where
    T: HttpSend,
{
    inner: Arc<Inner<T>>,
}

impl<T> Clone for HranaStream<T>
where
    T: HttpSend,
{
    fn clone(&self) -> Self {
        HranaStream {
            inner: self.inner.clone(),
        }
    }
}

impl<T> HranaStream<T>
where
    T: HttpSend,
{
    pub(super) fn open(
        client: T,
        pipeline_url: Arc<str>,
        cursor_url: Arc<str>,
        auth_token: Arc<str>,
    ) -> Self {
        HranaStream {
            inner: Arc::new(Inner {
                affected_row_count: AtomicU64::new(0),
                last_insert_rowid: AtomicI64::new(0),
                stream: Mutex::new(RawStream {
                    client,
                    pipeline_url,
                    cursor_url,
                    auth_token,
                    sql_id_generator: 0,
                    status: StreamStatus::Open,
                    baton: None,
                }),
            }),
        }
    }

    /// Executes a final request and immediately closes current stream - all in one request.
    pub async fn finalize(&mut self, req: StreamRequest) -> Result<(StreamResponse, bool)> {
        let mut client = self.inner.stream.lock().await;
        let resp = client.finalize(req).await?;
        Ok(resp)
    }

    pub async fn execute(&self, stmt: Stmt) -> Result<StmtResult> {
        //TODO: this trait shouldn't return BatchResult but an associated
        //      type that can respect Hrana async streaming cursor capabilities
        let mut batch = Batch::new();
        batch.step(None, stmt);
        let mut batch_res = self.batch(batch).await?;
        if let Some(Some(error)) = batch_res.step_errors.pop() {
            return Err(HranaError::StreamError(StreamResponseError { error }));
        }
        if let Some(Some(resp)) = batch_res.step_results.pop() {
            self.inner
                .affected_row_count
                .store(resp.affected_row_count, Ordering::SeqCst);
            self.inner
                .last_insert_rowid
                .store(resp.last_insert_rowid.unwrap_or_default(), Ordering::SeqCst);
            Ok(resp)
        } else {
            Err(HranaError::CursorError(CursorResponseError::Other(
                "no result has been returned".to_string(),
            )))
        }
    }

    pub async fn batch(&self, batch: Batch) -> Result<BatchResult> {
        //TODO: this trait shouldn't return BatchResult but an associated
        //      type that can respect Hrana async streaming cursor capabilities
        let cursor = self.cursor(batch).await?;
        Ok(cursor.into_batch_result().await?)
    }

    pub async fn cursor(&self, batch: Batch) -> Result<Cursor<T::Stream>> {
        let mut client = self.inner.stream.lock().await;
        let cursor = client.open_cursor(batch).await?;
        Ok(cursor)
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
    T: HttpSend,
{
    affected_row_count: AtomicU64,
    last_insert_rowid: AtomicI64,
    stream: Mutex<RawStream<T>>,
}

#[derive(Debug)]
struct RawStream<T>
where
    T: HttpSend,
{
    client: T,
    baton: Option<String>,
    pipeline_url: Arc<str>,
    cursor_url: Arc<str>,
    auth_token: Arc<str>,
    status: StreamStatus,
    sql_id_generator: SqlId,
}

impl<T> RawStream<T>
where
    T: HttpSend,
{
    async fn send(&mut self, req: StreamRequest) -> Result<StreamResponse> {
        let [resp] = self.send_requests([req]).await?;
        Ok(resp)
    }

    pub async fn open_cursor(&mut self, batch: Batch) -> Result<Cursor<T::Stream>> {
        if self.status == StreamStatus::Closed {
            return Err(HranaError::StreamClosed(
                "client stream has been closed by the servers".to_string(),
            ));
        }
        let msg = CursorReq {
            baton: self.baton.clone(),
            batch,
        };
        let body = serde_json::to_string(&msg).map_err(HranaError::Json)?;
        let stream = self
            .client
            .http_send(self.cursor_url.clone(), self.auth_token.clone(), body)
            .await?;
        let (cursor, mut response) = Cursor::open(stream).await?;
        if let Some(base_url) = response.base_url.take() {
            self.pipeline_url = Arc::from(format!("{base_url}/v3/pipeline"));
            self.cursor_url = Arc::from(format!("{base_url}/v3/cursor"));
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
        Ok(cursor)
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
        let body = self
            .client
            .http_send(self.pipeline_url.clone(), self.auth_token.clone(), body)
            .await?;
        let body = stream_to_bytes(body).await?;
        let mut response: ServerMsg = serde_json::from_slice(&body)?;
        if let Some(base_url) = response.base_url.take() {
            self.pipeline_url = Arc::from(base_url);
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
        for (i, result) in response.results.into_iter().enumerate() {
            match result {
                Response::Ok(StreamResponseOk { response }) => responses[i] = response,
                Response::Error(e) => return Err(HranaError::StreamError(e)),
            }
        }
        Ok(responses)
    }

    async fn finalize(&mut self, req: StreamRequest) -> Result<(StreamResponse, bool)> {
        let [resp, get_autocommit, _] = self
            .send_requests([req, StreamRequest::GetAutocommit, StreamRequest::Close])
            .await?;
        let is_autocommit = match get_autocommit {
            StreamResponse::GetAutocommit(resp) => resp.is_autocommit,
            other => {
                return Err(HranaError::UnexpectedResponse(format!(
                    "expected GetAutocommitResp but got {:?}",
                    other
                )))
            }
        };
        self.done();
        Ok((resp, is_autocommit))
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

async fn stream_to_bytes<S>(mut stream: S) -> Result<Bytes>
where
    S: Stream<Item = Result<Bytes>> + Unpin,
{
    use futures::StreamExt;

    let mut buf = BytesMut::new();
    while let Some(chunk) = stream.next().await {
        buf.extend_from_slice(&chunk?);
    }
    Ok(buf.freeze())
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
    T: HttpSend,
{
    stream: HranaStream<T>,
    sql_id: SqlId,
}

impl<T> StoredSql<T>
where
    T: HttpSend,
{
    fn new(stream: HranaStream<T>, sql_id: SqlId) -> Self {
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
    T: HttpSend,
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
