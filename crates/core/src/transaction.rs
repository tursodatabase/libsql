use std::ops::Deref;

use crate::Result;

use super::Connection;

#[derive(Debug)]
pub enum TransactionBehavior {
    Deferred,
    Immediate,
    Exclusive,
    ReadOnly,
}

pub struct Transaction {
    pub(crate) inner: Box<dyn Tx + Send + Sync>,
    pub(crate) conn: Connection,
}

impl Transaction {
    pub async fn commit(mut self) -> Result<()> {
        self.inner.commit().await
    }

    pub async fn rollback(mut self) -> Result<()> {
        self.inner.rollback().await
    }
}

impl Deref for Transaction {
    type Target = Connection;

    #[inline]
    fn deref(&self) -> &Connection {
        &self.conn
    }
}

#[async_trait::async_trait]
pub(crate) trait Tx {
    async fn commit(&mut self) -> Result<()>;
    async fn rollback(&mut self) -> Result<()>;
}

pub(super) struct LibsqlTx(pub(super) Option<crate::local::Transaction>);

#[async_trait::async_trait]
impl Tx for LibsqlTx {
    async fn commit(&mut self) -> Result<()> {
        let tx = self.0.take().expect("Tx already dropped");
        tx.commit()
    }

    async fn rollback(&mut self) -> Result<()> {
        let tx = self.0.take().expect("Tx already dropped");
        tx.rollback()
    }
}
