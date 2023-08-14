use std::sync::Arc;

use crate::{Params, Result};

use super::{rows::LibsqlRows, Rows};

#[async_trait::async_trait]
pub(super) trait Stmt {
    async fn execute(&self, params: &Params) -> Result<usize>;

    async fn query(&self, params: &Params) -> Result<Rows>;
}

pub struct Statement {
    pub(super) inner: Arc<dyn Stmt + Send + Sync>,
}

impl Statement {
    pub async fn execute(&self, params: &Params) -> Result<usize> {
        self.inner.execute(params).await
    }

    pub async fn query(&self, params: &Params) -> Result<Rows> {
        self.inner.query(params).await
    }
}

pub(super) struct LibsqlStmt(pub(super) crate::Statement);

#[async_trait::async_trait]
impl Stmt for LibsqlStmt {
    async fn execute(&self, params: &Params) -> Result<usize> {
        let params = params.clone();
        let stmt = self.0.clone();

        stmt.execute(&params).map(|i| i as usize)
    }

    async fn query(&self, params: &Params) -> Result<Rows> {
        let params = params.clone();
        let stmt = self.0.clone();

        stmt.query(&params).map(|rows| Rows {
            inner: Box::new(LibsqlRows(rows)),
        })
    }
}
