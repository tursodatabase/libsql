use crate::connection::Conn;
use crate::hrana::hyper::HttpSender;
use crate::hrana::pipeline::{ExecuteStreamReq, StreamRequest};
use crate::hrana::proto::{Batch, BatchResult, Stmt, StmtResult};
use crate::hrana::stream::HttpStream;
use crate::hrana::Result;
use crate::params::Params;
use crate::transaction::Tx;
use crate::{Statement, TransactionBehavior};

#[derive(Debug, Clone)]
pub(super) struct HttpTransaction {
    stream: HttpStream<HttpSender>,
}

impl HttpTransaction {
    pub async fn open(
        stream: HttpStream<HttpSender>,
        tx_behavior: TransactionBehavior,
    ) -> Result<Self> {
        let begin_stmt = match tx_behavior {
            TransactionBehavior::Deferred => "BEGIN DEFERRED",
            TransactionBehavior::Immediate => "BEGIN IMMEDIATE",
            TransactionBehavior::Exclusive => "BEGIN EXCLUSIVE",
            TransactionBehavior::ReadOnly => "BEGIN READONLY",
        };
        stream.execute(Stmt::new(begin_stmt, false)).await?;
        Ok(HttpTransaction { stream })
    }

    pub async fn execute(&self, stmt: Stmt) -> Result<StmtResult> {
        self.stream.execute(stmt).await
    }

    pub async fn execute_batch(
        &self,
        stmts: impl IntoIterator<Item = Stmt>,
    ) -> Result<BatchResult> {
        let mut batch = Batch::new();
        for stmt in stmts.into_iter() {
            batch.step(None, stmt);
        }
        self.stream.batch(batch).await
    }

    pub async fn commit(&mut self) -> Result<()> {
        let stmt = Stmt::new("COMMIT", false);
        self.stream
            .finalize(StreamRequest::Execute(ExecuteStreamReq { stmt }))
            .await?;
        Ok(())
    }

    pub async fn rollback(&mut self) -> Result<()> {
        let stmt = Stmt::new("ROLLBACK", false);
        self.stream
            .finalize(StreamRequest::Execute(ExecuteStreamReq { stmt }))
            .await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Tx for HttpTransaction {
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
impl Conn for HttpTransaction {
    async fn execute(&self, sql: &str, params: Params) -> crate::Result<u64> {
        let mut stmt = self.prepare(sql).await?;
        let rows = stmt.execute(params).await?;
        Ok(rows as u64)
    }

    async fn execute_batch(&self, sql: &str) -> crate::Result<()> {
        let mut statements = Vec::new();
        let stmts = crate::parser::Statement::parse(sql);
        for s in stmts {
            let s = s?;
            statements.push(Stmt::new(s.stmt, false));
        }
        self.execute_batch(statements)
            .await
            .map_err(|e| crate::Error::Hrana(e.into()))?;
        Ok(())
    }

    async fn prepare(&self, sql: &str) -> crate::Result<Statement> {
        let stmt = crate::hrana::Statement::new(self.stream.clone(), sql.to_string(), true);
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
        // TODO: Is this correct?
        false
    }

    fn changes(&self) -> u64 {
        self.stream.affected_row_count()
    }

    fn last_insert_rowid(&self) -> i64 {
        self.stream.last_insert_rowid()
    }

    fn close(&mut self) {
        todo!()
    }
}
