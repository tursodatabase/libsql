use crate::{
    local::{self},
    params::Params,
    statement::Stmt,
    sync::SyncContext, Column, Result, Rows, Statement,
};
use std::sync::{atomic::{AtomicBool, Ordering}, Arc};
use tokio::sync::Mutex;

pub struct SyncedStatement {
    pub conn: local::Connection,
    pub inner: Statement,
    pub context: Arc<Mutex<SyncContext>>,
    pub needs_pull: Arc<AtomicBool>,
}

#[async_trait::async_trait]
impl Stmt for SyncedStatement {
    fn finalize(&mut self) {
        self.inner.finalize()
    }

    async fn execute(&self, params: &Params) -> Result<usize> {
        if self.needs_pull.load(Ordering::Relaxed) {
            let mut context = self.context.lock().await;
            crate::sync::try_pull(&mut context, &self.conn).await?;
            self.needs_pull.store(false, Ordering::Relaxed);
        }
        self.inner.execute(params).await
    }

    async fn query(&self, params: &Params) -> Result<Rows> {
        if self.needs_pull.load(Ordering::Relaxed) {
            let mut context = self.context.lock().await;
            crate::sync::try_pull(&mut context, &self.conn).await?;
            self.needs_pull.store(false, Ordering::Relaxed);
        }
        self.inner.query(params).await
    }

    async fn run(&self, params: &Params) -> Result<()> {
        if self.needs_pull.load(Ordering::Relaxed) {
            let mut context = self.context.lock().await;
            crate::sync::try_pull(&mut context, &self.conn).await?;
            self.needs_pull.store(false, Ordering::Relaxed);
        }
        self.inner.run(params).await
    }

    fn interrupt(&self) -> Result<()> {
        self.inner.interrupt()
    }

    fn reset(&self) {
        self.inner.reset()
    }

    fn parameter_count(&self) -> usize {
        self.inner.parameter_count()
    }

    fn parameter_name(&self, idx: i32) -> Option<&str> {
        self.inner.parameter_name(idx)
    }

    fn column_count(&self) -> usize {
        self.inner.column_count()
    }

    fn columns(&self) -> Vec<Column> {
        self.inner.columns()
    }
}

