use crate::connection::Conn;
use crate::hrana::connection::HttpConnection;
use crate::hrana::proto::{Batch, Stmt};
use crate::hrana::stream::HranaStream;
use crate::hrana::transaction::HttpTransaction;
use crate::hrana::{bind_params, HranaError, HttpSend, Result};
use crate::params::Params;
use crate::transaction::Tx;
use crate::util::ConnectorService;
use crate::{Rows, Statement};
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{Stream, TryStreamExt};
use http::header::AUTHORIZATION;
use http::{HeaderValue, StatusCode};
use hyper::body::HttpBody;
use std::sync::Arc;

pub type ByteStream = Box<dyn Stream<Item = Result<Bytes>> + Send + Unpin>;

#[derive(Clone, Debug)]
pub struct HttpSender {
    inner: hyper::Client<ConnectorService, hyper::Body>,
    version: HeaderValue,
}

impl HttpSender {
    pub fn new(connector: ConnectorService, version: Option<&str>) -> Self {
        let ver = version.unwrap_or(env!("CARGO_PKG_VERSION"));

        let version = HeaderValue::try_from(format!("libsql-remote-{ver}")).unwrap();

        let inner = hyper::Client::builder().build(connector);

        Self { inner, version }
    }

    async fn send(
        self,
        url: Arc<str>,
        auth: Arc<str>,
        body: String,
    ) -> Result<super::HttpBody<ByteStream>> {
        let req = hyper::Request::post(url.as_ref())
            .header(AUTHORIZATION, auth.as_ref())
            .header("x-libsql-client-version", self.version.clone())
            .body(hyper::Body::from(body))
            .map_err(|err| HranaError::Http(format!("{:?}", err)))?;

        let resp = self.inner.request(req).await.map_err(HranaError::from)?;

        if resp.status() != StatusCode::OK {
            let body = hyper::body::to_bytes(resp.into_body())
                .await
                .map_err(HranaError::from)?;
            let body = String::from_utf8(body.into()).unwrap();
            return Err(HranaError::Api(body));
        }

        let body: super::HttpBody<ByteStream> = if resp.is_end_stream() {
            let body = hyper::body::to_bytes(resp.into_body())
                .await
                .map_err(HranaError::from)?;
            super::HttpBody::from(body)
        } else {
            let stream = resp
                .into_body()
                .into_stream()
                .map_err(|e| HranaError::Http(e.to_string()));
            super::HttpBody::Stream(Box::new(stream))
        };

        Ok(body)
    }
}

impl HttpSend for HttpSender {
    type Stream = super::HttpBody<ByteStream>;
    type Result = BoxFuture<'static, Result<Self::Stream>>;

    fn http_send(&self, url: Arc<str>, auth: Arc<str>, body: String) -> Self::Result {
        let fut = self.clone().send(url, auth, body);
        Box::pin(fut)
    }
}

impl From<hyper::Error> for HranaError {
    fn from(value: hyper::Error) -> Self {
        HranaError::Http(value.to_string())
    }
}

impl HttpConnection<HttpSender> {
    pub(crate) fn new_with_connector(
        url: impl Into<String>,
        token: impl Into<String>,
        connector: ConnectorService,
        version: Option<&str>,
    ) -> Self {
        let inner = HttpSender::new(connector, version);
        Self::new(url.into(), token.into(), inner)
    }
}

#[async_trait::async_trait]
impl Conn for HttpConnection<HttpSender> {
    async fn execute(&self, sql: &str, params: Params) -> crate::Result<u64> {
        let mut stmt = Stmt::new(sql, false);
        bind_params(params, &mut stmt);
        let res = self
            .execute_inner(stmt)
            .await
            .map_err(|e| crate::Error::Hrana(e.into()))?;
        Ok(res.affected_row_count)
    }

    async fn execute_batch(&self, sql: &str) -> crate::Result<()> {
        let mut statements = Vec::new();
        let stmts = crate::parser::Statement::parse(sql);
        for s in stmts {
            let s = s?;
            statements.push(Stmt::new(s.stmt, false));
        }
        self.batch_inner(statements)
            .await
            .map_err(|e| crate::Error::Hrana(e.into()))?;
        Ok(())
    }

    async fn prepare(&self, sql: &str) -> crate::Result<Statement> {
        let stream = self.open_stream();
        let stmt = crate::hrana::Statement::from_stream(stream, sql.to_string(), true);
        Ok(Statement {
            inner: Box::new(stmt),
        })
    }

    async fn transaction(
        &self,
        tx_behavior: crate::TransactionBehavior,
    ) -> crate::Result<crate::transaction::Transaction> {
        let stream = self.open_stream();
        let mut tx = HttpTransaction::open(stream, tx_behavior)
            .await
            .map_err(|e| crate::Error::Hrana(Box::new(e)))?;
        Ok(crate::Transaction {
            inner: Box::new(tx.clone()),
            conn: crate::Connection {
                conn: Arc::new(tx.stream().clone()),
            },
            close: Some(Box::new(|| {
                // make sure that Hrana connection is closed and all uncommitted changes
                // are rolled back when we're about to drop the transaction
                let _ = tokio::task::spawn(async move { tx.rollback().await });
            })),
        })
    }

    fn is_autocommit(&self) -> bool {
        self.is_autocommit()
    }

    fn changes(&self) -> u64 {
        self.affected_row_count()
    }

    fn last_insert_rowid(&self) -> i64 {
        self.last_insert_rowid()
    }
}

#[async_trait::async_trait]
impl crate::statement::Stmt for crate::hrana::Statement<HttpSender> {
    fn finalize(&mut self) {}

    async fn execute(&mut self, params: &Params) -> crate::Result<usize> {
        self.execute(params).await
    }

    async fn query(&mut self, params: &Params) -> crate::Result<Rows> {
        self.query(params).await
    }

    fn reset(&mut self) {}

    fn parameter_count(&self) -> usize {
        todo!()
    }

    fn parameter_name(&self, _idx: i32) -> Option<&str> {
        todo!()
    }

    fn columns(&self) -> Vec<crate::Column> {
        todo!()
    }
}

#[async_trait::async_trait]
impl Tx for HttpTransaction<HttpSender> {
    async fn commit(&mut self) -> crate::Result<()> {
        self.commit()
            .await
            .map_err(|e| crate::Error::Hrana(Box::new(e)))?;
        Ok(())
    }

    async fn rollback(&mut self) -> crate::Result<()> {
        self.rollback()
            .await
            .map_err(|e| crate::Error::Hrana(Box::new(e)))?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Conn for HranaStream<HttpSender> {
    async fn execute(&self, sql: &str, params: Params) -> crate::Result<u64> {
        let mut stmt = Stmt::new(sql, false);
        bind_params(params, &mut stmt);
        let result = self
            .execute(stmt)
            .await
            .map_err(|e| crate::Error::Hrana(e.into()))?;
        Ok(result.affected_row_count)
    }

    async fn execute_batch(&self, sql: &str) -> crate::Result<()> {
        let mut stmts = Vec::new();
        let parse = crate::parser::Statement::parse(sql);
        for s in parse {
            let s = s?;
            stmts.push(Stmt::new(s.stmt, false));
        }
        self.batch(Batch::from_iter(stmts, false))
            .await
            .map_err(|e| crate::Error::Hrana(e.into()))?;
        Ok(())
    }

    async fn prepare(&self, sql: &str) -> crate::Result<Statement> {
        let stmt = crate::hrana::Statement::from_stream(self.clone(), sql.to_string(), true);
        Ok(Statement {
            inner: Box::new(stmt),
        })
    }

    async fn transaction(
        &self,
        _tx_behavior: crate::TransactionBehavior,
    ) -> crate::Result<crate::transaction::Transaction> {
        todo!("sounds like nested transactions innit?")
    }

    fn is_autocommit(&self) -> bool {
        false // for streams this method is callable only when we're within explicit transaction
    }

    fn changes(&self) -> u64 {
        self.affected_row_count()
    }

    fn last_insert_rowid(&self) -> i64 {
        self.last_insert_rowid()
    }
}
