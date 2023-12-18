use crate::connection::Conn;
use crate::hrana::connection::HttpConnection;
use crate::hrana::pipeline::ServerMsg;
use crate::hrana::proto::{Batch, Stmt};
use crate::hrana::stream::HttpStream;
use crate::hrana::transaction::HttpTransaction;
use crate::hrana::{bind_params, HranaError, HttpSend, Result};
use crate::params::Params;
use crate::transaction::Tx;
use crate::util::ConnectorService;
use crate::{Rows, Statement};
use futures::future::BoxFuture;
use http::header::AUTHORIZATION;
use http::{HeaderValue, StatusCode};
use std::sync::Arc;

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

    async fn send(&self, url: String, auth: String, body: String) -> Result<ServerMsg> {
        let req = hyper::Request::post(url)
            .header(AUTHORIZATION, auth)
            .header("x-libsql-client-version", self.version.clone())
            .body(hyper::Body::from(body))
            .map_err(|err| HranaError::Http(format!("{:?}", err)))?;

        let res = self.inner.request(req).await?;

        if res.status() != StatusCode::OK {
            let body = hyper::body::to_bytes(res.into_body()).await?;
            let msg = String::from_utf8(body.into())
                .unwrap_or_else(|err| format!("Invalid payload: {}", err));
            return Err(HranaError::Api(msg));
        }

        let body = hyper::body::to_bytes(res.into_body()).await?;

        let msg = serde_json::from_slice::<ServerMsg>(&body[..])?;

        Ok(msg)
    }
}

impl<'a> HttpSend<'a> for HttpSender {
    type Result = BoxFuture<'a, Result<ServerMsg>>;

    fn http_send(&'a self, url: String, auth: String, body: String) -> Self::Result {
        let fut = self.send(url, auth, body);
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
impl Conn for HttpStream<HttpSender> {
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
        let mut batch = Batch::new();
        let stmts = crate::parser::Statement::parse(sql);
        for s in stmts {
            let s = s?;
            batch.step(None, Stmt::new(s.stmt, false));
        }
        self.batch(batch)
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
