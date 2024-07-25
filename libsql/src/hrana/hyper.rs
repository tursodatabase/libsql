use crate::connection::{BatchRows, Conn};
use crate::hrana::connection::HttpConnection;
use crate::hrana::proto::{Batch, Stmt};
use crate::hrana::stream::HranaStream;
use crate::hrana::transaction::{HttpTransaction, TxScopeCounter};
use crate::hrana::{bind_params, unwrap_err, HranaError, HttpSend, Result};
use crate::params::Params;
use crate::transaction::Tx;
use crate::util::ConnectorService;
use crate::{Error, Rows, Statement};
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{Stream, TryStreamExt};
use http::header::AUTHORIZATION;
use http::{HeaderValue, StatusCode};
use hyper::body::HttpBody;
use std::io::ErrorKind;
use std::sync::Arc;

use super::StmtResultRows;

pub type ByteStream = Box<dyn Stream<Item = std::io::Result<Bytes>> + Send + Sync + Unpin>;

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
                .map_err(|e| std::io::Error::new(ErrorKind::Other, e));
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

    fn oneshot(self, url: Arc<str>, auth: Arc<str>, body: String) {
        if let Ok(rt) = tokio::runtime::Handle::try_current() {
            rt.spawn(self.send(url, auth, body));
        } else {
            tracing::warn!("tried to send request to `{url}` while no runtime was available");
        }
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
        self.current_stream().execute(sql, params).await
    }

    async fn execute_batch(&self, sql: &str) -> crate::Result<BatchRows> {
        self.current_stream().execute_batch(sql).await
    }

    async fn execute_transactional_batch(&self, sql: &str) -> crate::Result<BatchRows> {
        self.current_stream().execute_transactional_batch(sql).await
    }

    async fn prepare(&self, sql: &str) -> crate::Result<Statement> {
        let stream = self.current_stream().clone();
        let stmt = crate::hrana::Statement::new(stream, sql.to_string(), true)?;
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
                if let Ok(rt) = tokio::runtime::Handle::try_current() {
                    // transaction will rollback automatically after timeout on the server side
                    // this is gracefull rollback on best-effort basis
                    rt.spawn(async move {
                        let _ = tx.rollback().await;
                    });
                }
            })),
        })
    }

    fn is_autocommit(&self) -> bool {
        self.is_autocommit()
    }

    fn changes(&self) -> u64 {
        self.affected_row_count()
    }

    fn total_changes(&self) -> u64 {
        self.total_changes()
    }

    fn last_insert_rowid(&self) -> i64 {
        self.last_insert_rowid()
    }

    async fn reset(&self) {
        self.current_stream().reset().await;
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

    async fn run(&mut self, params: &Params) -> crate::Result<()> {
        self.run(params).await
    }

    fn reset(&mut self) {}

    fn parameter_count(&self) -> usize {
        let stmt = &self.inner;
        stmt.args.len() + stmt.named_args.len()
    }

    fn parameter_name(&self, idx: i32) -> Option<&str> {
        //FIXME: actual rules of named args are pretty convoluted and may require full AST parsing. Here we basically
        //       assume, that if one needs a param name, they don't use named and un-named params mixed in.
        if !self.inner.args.is_empty() {
            return None;
        }
        let named_param = self.inner.named_args.get(idx as usize)?;
        Some(&named_param.name)
    }

    fn columns(&self) -> Vec<crate::Column> {
        //FIXME: there are several blockers here:
        // 1. We cannot know the column types before sending a query, so this method will never return results right
        //    away.
        // 2. Even if we do execute query, Hrana doesn't return all info that Column exposes.
        // 3. Even if we would like to return some of the column info ie. column [ValueType], this information is not
        //    present in Hrana [Col] but rather inferred from the row cell type.
        vec![]
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
        // SQLite: execute() will only execute a single SQL statement
        let mut parsed = crate::parser::Statement::parse(sql);
        let mut c = TxScopeCounter::default();
        if let Some(s) = parsed.next() {
            let s = s?;
            c.count(s.kind);
            let in_tx_scope = !self.is_autocommit() || c.begin_tx();
            let close = !in_tx_scope || c.end_tx();
            let mut stmt = Stmt::new(s.stmt, false);
            bind_params(params, &mut stmt);
            let result = self
                .execute_inner(stmt, close)
                .await
                .map_err(|e| crate::Error::Hrana(e.into()))?;
            Ok(result.affected_row_count)
        } else {
            Err(crate::Error::Misuse(
                "no SQL statement provided".to_string(),
            ))
        }
    }

    async fn execute_batch(&self, sql: &str) -> crate::Result<BatchRows> {
        let mut stmts = Vec::new();
        let parse = crate::parser::Statement::parse(sql);
        let mut c = TxScopeCounter::default();
        for s in parse {
            let s = s?;
            c.count(s.kind);
            stmts.push(Stmt::new(s.stmt, false));
        }
        let in_tx_scope = !self.is_autocommit() || c.begin_tx();
        let close = !in_tx_scope || c.end_tx();
        let res = self
            .batch_inner(Batch::from_iter(stmts), close)
            .await
            .map_err(|e| crate::Error::Hrana(e.into()))?;
        unwrap_err(&res)?;
        let rows = res
            .step_results
            .into_iter()
            .map(|r| r.map(StmtResultRows::new).map(Rows::new))
            .collect::<Vec<_>>();

        Ok(BatchRows::new(rows))
    }

    async fn execute_transactional_batch(&self, sql: &str) -> crate::Result<BatchRows> {
        let mut stmts = Vec::new();
        let parse = crate::parser::Statement::parse(sql);
        for s in parse {
            let s = s?;
            if s.kind == crate::parser::StmtKind::TxnBegin
                || s.kind == crate::parser::StmtKind::TxnBeginReadOnly
                || s.kind == crate::parser::StmtKind::TxnEnd
            {
                return Err(Error::TransactionalBatchError(
                    "Transactions forbidden inside transactional batch".to_string(),
                ));
            }
            stmts.push(Stmt::new(s.stmt, false));
        }
        let res = self
            .batch_inner(Batch::transactional(stmts), true)
            .await
            .map_err(|e| crate::Error::Hrana(e.into()))?;
        unwrap_err(&res)?;
        let rows = res
            .step_results
            .into_iter()
            // skip the first row since this is related to the already injected
            // BEGIN statement.
            .skip(1)
            .map(|r| r.map(StmtResultRows::new).map(Rows::new))
            .collect::<Vec<_>>();

        // Skip the last row as well since this corresponds to the injected commit statement
        // that the user never sees.
        Ok(BatchRows::new_skip_last(rows, 2))
    }

    async fn prepare(&self, sql: &str) -> crate::Result<Statement> {
        let stmt = crate::hrana::Statement::new(self.clone(), sql.to_string(), true)?;
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

    fn total_changes(&self) -> u64 {
        self.total_changes()
    }

    fn last_insert_rowid(&self) -> i64 {
        self.last_insert_rowid()
    }

    async fn reset(&self) {
        self.reset().await;
    }
}
