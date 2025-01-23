use crate::{
    local::{self},
    params::Params,
    statement::Stmt,
    sync::SyncContext, Column, Result, Rows, Statement,
};
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct SyncedStatement {
    pub conn: local::Connection,
    pub context: Arc<Mutex<SyncContext>>,
    pub inner: Statement,
}

#[async_trait::async_trait]
impl Stmt for SyncedStatement {
    fn finalize(&mut self) {
        self.inner.finalize()
    }

    async fn execute(&mut self, params: &Params) -> Result<usize> {
        let result = self.inner.execute(params).await;
        let mut context = self.context.lock().await;
        let _ = crate::sync::sync_offline(&mut context, &self.conn).await;
        result
    }

    async fn query(&mut self, params: &Params) -> Result<Rows> {
        let result = self.inner.query(params).await;
        let mut context = self.context.lock().await;
        let _ = crate::sync::sync_offline(&mut context, &self.conn).await;
        result
    }

    async fn run(&mut self, params: &Params) -> Result<()> {
        let result = self.inner.run(params).await;
        let mut context = self.context.lock().await;
        let _ = crate::sync::sync_offline(&mut context, &self.conn).await;
        result
    }

    fn reset(&mut self) {
        self.inner.reset()
    }

    fn parameter_count(&self) -> usize {
        self.inner.parameter_count()
    }

    fn parameter_name(&self, idx: i32) -> Option<&str> {
        self.inner.parameter_name(idx)
    }

    fn columns(&self) -> Vec<Column> {
        self.inner.columns()
    }
}
