use crate::hrana::pipeline::{
    BatchStreamReq, ClientMsg, CloseSqlStreamReq, DescribeStreamReq, ExecuteStreamReq, Response,
    SequenceStreamReq, ServerMsg, StoreSqlStreamReq, StreamRequest, StreamResponse,
    StreamResponseOk,
};
use crate::hrana::proto::{Batch, BatchResult, DescribeResult, Stmt, StmtResult};
use crate::hrana::{HranaError, HttpSend, Result};
use futures::lock::Mutex;
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
pub(super) struct HttpStream<T> {
    inner: Arc<Mutex<Inner<T>>>,
}

impl<T> Clone for HttpStream<T> {
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
    pub fn open(client: T, base_url: String, auth_token: String) -> Self {
        let inner = Inner {
            client,
            base_url,
            auth_token,
            sql_id_generator: 0,
            status: StreamStatus::Open,
            baton: None,
        };
        HttpStream {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    /// Executes a final request and immediately closes current stream.
    pub async fn finalize(&mut self, req: StreamRequest) -> Result<StreamResponse> {
        let mut client = self.inner.lock().await;
        let resp = client.finalize(req).await?;
        Ok(resp)
    }

    pub async fn execute(&self, stmt: Stmt) -> Result<StmtResult> {
        let mut client = self.inner.lock().await;
        let resp = client
            .send(StreamRequest::Execute(ExecuteStreamReq { stmt }))
            .await?;
        match resp {
            StreamResponse::Execute(resp) => Ok(resp.result),
            other => unexpected!(other),
        }
    }

    pub async fn batch(&self, batch: Batch) -> Result<BatchResult> {
        let mut client = self.inner.lock().await;
        let resp = client
            .send(StreamRequest::Batch(BatchStreamReq { batch }))
            .await?;
        match resp {
            StreamResponse::Batch(resp) => Ok(resp.result),
            other => unexpected!(other),
        }
    }

    pub async fn store_sql(&self, sql: String) -> Result<StoredSql<T>> {
        let mut client = self.inner.lock().await;
        let sql_id = client.next_sql_id();
        let resp = client
            .send(StreamRequest::StoreSql(StoreSqlStreamReq { sql, sql_id }))
            .await?;
        match resp {
            StreamResponse::StoreSql => {
                drop(client);
                Ok(StoredSql::new(self.clone(), sql_id))
            }
            other => {
                client.sql_id_generator = client.sql_id_generator.wrapping_sub(1);
                unexpected!(other)
            }
        }
    }

    async fn close_sql(&self, sql_id: SqlId) -> Result<()> {
        let mut client = self.inner.lock().await;
        let resp = client
            .send(StreamRequest::CloseSql(CloseSqlStreamReq { sql_id }))
            .await?;
        match resp {
            StreamResponse::CloseSql => Ok(()),
            other => unexpected!(other),
        }
    }

    pub async fn describe<D: SqlDescriptor>(&self, descriptor: &D) -> Result<DescribeResult> {
        let mut client = self.inner.lock().await;
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
        let mut client = self.inner.lock().await;
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
        let mut client = self.inner.lock().await;
        let resp = client.send(StreamRequest::GetAutocommit).await?;
        match resp {
            StreamResponse::GetAutocommit(resp) => Ok(resp.is_autocommit),
            other => unexpected!(other),
        }
    }
}

struct Inner<T> {
    client: T,
    baton: Option<String>,
    base_url: String,
    auth_token: String,
    status: StreamStatus,
    sql_id_generator: SqlId,
}

impl<T> Inner<T>
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
                "stream has been closed by the servers".to_string(),
            ));
        }
        let msg = ClientMsg {
            baton: self.baton.clone(),
            requests: Vec::from(requests),
        };
        let body = serde_json::to_string(&msg).map_err(HranaError::Json)?;
        let mut response: ServerMsg = self
            .client
            .http_send(self.base_url.clone(), self.auth_token.clone(), body)
            .await?;
        match response.baton.take() {
            None => self.status = StreamStatus::Closed, // stream has been closed by the server
            baton => self.baton = baton,
        }
        if let Some(base_url) = response.base_url.take() {
            self.base_url = base_url;
        }

        if response.results.is_empty() {
            Err(HranaError::UnexpectedResponse(format!(
                "Unexpected empty response from server: {:?}",
                response.results
            )))?;
        }
        if response.results.len() > N {
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
        Ok(resp)
    }

    async fn close(&mut self) -> Result<()> {
        if self.status == StreamStatus::Closed {
            return Ok(());
        }
        self.send(StreamRequest::Close).await?;
        self.status = StreamStatus::Closed;
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
pub struct StoredSql<T> {
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

pub trait SqlDescriptor {
    fn sql_description(&self) -> SqlDescription;
}

impl SqlDescriptor for String {
    fn sql_description(&self) -> SqlDescription {
        SqlDescription::Sql(self.clone())
    }
}

impl<T> SqlDescriptor for StoredSql<T> {
    fn sql_description(&self) -> SqlDescription {
        SqlDescription::SqlId(self.sql_id)
    }
}

#[derive(Debug)]
pub enum SqlDescription {
    Sql(String),
    SqlId(SqlId),
}
