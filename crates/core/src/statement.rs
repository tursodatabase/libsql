use crate::params::IntoParams;
use crate::params::Params;
pub use crate::Column;
use crate::{Error, Result};

use crate::{Row, Rows};

#[async_trait::async_trait]
pub(crate) trait Stmt {
    fn finalize(&mut self);

    // TODO(lucio): Update trait to take owned params
    async fn execute(&mut self, params: &Params) -> Result<usize>;

    async fn query(&mut self, params: &Params) -> Result<Rows>;

    fn reset(&mut self);

    fn parameter_count(&self) -> usize;

    fn parameter_name(&self, idx: i32) -> Option<&str>;

    fn columns(&self) -> Vec<Column>;
}

pub struct Statement {
    pub(crate) inner: Box<dyn Stmt + Send + Sync>,
}

// TODO(lucio): Unify param usage, here we use & and in conn we use
//      Into.
impl Statement {
    pub fn finalize(&mut self) {
        self.inner.finalize();
    }

    pub async fn execute(&mut self, params: impl IntoParams) -> Result<usize> {
        tracing::trace!("execute for prepared statement");
        self.inner.execute(&params.into_params()?).await
    }

    pub async fn query(&mut self, params: impl IntoParams) -> Result<Rows> {
        tracing::trace!("query for prepared statement");
        self.inner.query(&params.into_params()?).await
    }

    pub async fn query_map<F>(&mut self, params: impl IntoParams, map: F) -> Result<MappedRows<F>> {
        let rows = self.query(params).await?;

        Ok(MappedRows { rows, map })
    }

    pub async fn query_row(&mut self, params: impl IntoParams) -> Result<Row> {
        let mut rows = self.query(params).await?;

        let row = rows.next()?.ok_or(Error::QueryReturnedNoRows)?;

        Ok(row)
    }

    pub fn reset(&mut self) {
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
