use std::sync::Arc;

use crate::{Column, Error, Params, Result};

use super::{rows::LibsqlRows, Row, Rows};

// TODO(lucio): Add `column_*` based fn
#[async_trait::async_trait]
pub(super) trait Stmt {
    fn finalize(&self);

    async fn execute(&self, params: &Params) -> Result<usize>;

    async fn query(&self, params: &Params) -> Result<Rows>;

    fn reset(&self);

    fn parameter_count(&self) -> usize;

    fn parameter_name(&self, idx: i32) -> Option<&str>;

    fn columns(&self) -> Vec<Column>;
}

pub struct Statement {
    pub(super) inner: Arc<dyn Stmt + Send + Sync>,
}

// TODO(lucio): Unify param usage, here we use & and in conn we use
//      Into.
impl Statement {
    pub fn finalize(&self) {
        self.inner.finalize();
    }

    pub async fn execute(&self, params: &Params) -> Result<usize> {
        self.inner.execute(params).await
    }

    pub async fn query(&self, params: &Params) -> Result<Rows> {
        self.inner.query(params).await
    }

    pub async fn query_map<F>(&self, params: &Params, map: F) -> Result<MappedRows<F>> {
        let rows = self.query(params).await?;

        Ok(MappedRows { rows, map })
    }

    pub async fn query_row(&self, params: &Params) -> Result<Row> {
        let mut rows = self.query(params).await?;

        let row = rows.next()?.ok_or(Error::QueryReturnedNoRows)?;

        Ok(row)
    }

    pub fn reset(&self) {
        self.inner.reset();
    }

    pub fn parameter_count(&self) -> usize {
        self.inner.parameter_count()
    }

    pub fn parameter_name(&self, idx: i32) -> Option<&str> {
        self.inner.parameter_name(idx)
    }

    pub fn columns(&self) -> Vec<Column> {
        self.inner.columns()
    }
}

pub struct MappedRows<F> {
    rows: Rows,
    map: F,
}

impl<F> MappedRows<F> {
    pub fn new(rows: Rows, map: F) -> Self {
        Self { rows, map }
    }
}

impl<F, T> Iterator for MappedRows<F>
where
    F: FnMut(Row) -> Result<T>,
{
    type Item = Result<T>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let map = &mut self.map;
        self.rows
            .next()
            .transpose()
            .map(|row_result| row_result.and_then(map))
    }
}

pub(super) struct LibsqlStmt(pub(super) crate::Statement);

#[async_trait::async_trait]
impl Stmt for LibsqlStmt {
    fn finalize(&self) {
        self.0.finalize();
    }

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

    fn reset(&self) {
        self.0.reset();
    }

    fn parameter_count(&self) -> usize {
        self.0.parameter_count()
    }

    fn parameter_name(&self, idx: i32) -> Option<&str> {
        self.0.parameter_name(idx)
    }

    fn columns(&self) -> Vec<Column> {
        self.0.columns()
    }
}
