use crate::params::IntoParams;
use crate::params::Params;
pub use crate::Column;
use crate::{Error, Result};

use crate::{Row, Rows};

#[async_trait::async_trait]
pub(crate) trait Stmt {
    fn finalize(&mut self);

    async fn execute(&mut self, params: &Params) -> Result<usize>;

    async fn query(&mut self, params: &Params) -> Result<Rows>;

    fn reset(&mut self);

    fn parameter_count(&self) -> usize;

    fn parameter_name(&self, idx: i32) -> Option<&str>;

    fn columns(&self) -> Vec<Column>;
}

/// A cached prepared statement.
pub struct Statement {
    pub(crate) inner: Box<dyn Stmt + Send + Sync>,
}

impl Statement {
    /// Finalize the cached statement.
    pub fn finalize(&mut self) {
        self.inner.finalize();
    }

    /// Execute queries on the statement, check [`Connection::execute`] for usage.
    pub async fn execute(&mut self, params: impl IntoParams) -> Result<usize> {
        tracing::trace!("execute for prepared statement");
        self.inner.execute(&params.into_params()?).await
    }

    /// Execute a query on the statement, check [`Connection::query`] for usage.
    pub async fn query(&mut self, params: impl IntoParams) -> Result<Rows> {
        tracing::trace!("query for prepared statement");
        self.inner.query(&params.into_params()?).await
    }

    /// Execute a query on the statment and return a mapped iterator.
    pub async fn query_map<F>(&mut self, params: impl IntoParams, map: F) -> Result<MappedRows<F>> {
        let rows = self.query(params).await?;

        Ok(MappedRows { rows, map })
    }

    /// Execute a query that returns the first [`Row`].
    ///
    /// # Errors
    ///
    /// - Returns `QueryReturnedNoRows` if no rows were returned.
    pub async fn query_row(&mut self, params: impl IntoParams) -> Result<Row> {
        let mut rows = self.query(params).await?;

        let row = rows.next()?.ok_or(Error::QueryReturnedNoRows)?;

        Ok(row)
    }

    /// Reset the state of this prepared statement.
    pub fn reset(&mut self) {
        self.inner.reset();
    }

    /// Fetch the amount of parameters in the prepared statement.
    pub fn parameter_count(&self) -> usize {
        self.inner.parameter_count()
    }

    /// Fetch the parameter name at the provided index.
    pub fn parameter_name(&self, idx: i32) -> Option<&str> {
        self.inner.parameter_name(idx)
    }

    /// Fetch the list of columns for the prepared statement.
    pub fn columns(&self) -> Vec<Column> {
        self.inner.columns()
    }
}

/// An iterator that maps over all the rows.
pub struct MappedRows<F> {
    rows: Rows,
    map: F,
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
