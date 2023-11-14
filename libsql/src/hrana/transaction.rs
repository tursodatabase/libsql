use crate::hrana::pipeline::{ExecuteStreamReq, StreamRequest};
use crate::hrana::proto::{Stmt, StmtResult};
use crate::hrana::stream::HttpStream;
use crate::hrana::{HttpSend, Result, ServerMsg};
use crate::transaction::Tx;
use crate::TransactionBehavior;
use futures::future::BoxFuture;

#[derive(Debug)]
pub(super) struct HttpTransaction<T> {
    stream: HttpStream<T>,
}

impl<T> HttpTransaction<T>
where
    T: for<'a> HttpSend<'a>,
{
    pub async fn open(stream: HttpStream<T>, tx_behavior: TransactionBehavior) -> Result<Self> {
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
impl<T> Tx for HttpTransaction<T>
where
    T: for<'a> HttpSend<'a, Result = BoxFuture<'a, Result<ServerMsg>>> + Send + Sync,
{
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
