use crate::params::IntoParams;
use crate::params::IntoValue;
use crate::params::Params;
pub use crate::Column;
use crate::Value;
use crate::{Error, Result};
use crate::{Row, Rows};

#[async_trait::async_trait]
pub(crate) trait Stmt {
    fn finalize(&mut self);

    async fn execute(&self, params: &Params) -> Result<usize>;

    async fn query(&self, params: &Params) -> Result<Rows>;

    async fn run(&self, params: &Params) -> Result<()>;

    fn interrupt(&self) -> Result<()>;

    fn reset(&self);

    fn parameter_count(&self) -> usize;

    fn parameter_name(&self, idx: i32) -> Option<&str>;

    fn column_count(&self) -> usize;

    fn columns(&self) -> Vec<Column>;
}

/// A cached prepared statement.
pub struct Statement {
    pub(crate) inner: Box<dyn Stmt + Send + Sync>,
    /// Values set with [`Statement::raw_bind_parameter`], by 1-based index.
    raw_params: Vec<Value>,
}

impl Statement {
    pub(crate) fn new(inner: Box<dyn Stmt + Send + Sync>) -> Self {
        Self {
            inner,
            raw_params: Vec::new(),
        }
    }

    /// Finalize the cached statement.
    pub fn finalize(&mut self) {
        self.inner.finalize();
    }

    /// Execute queries on the statement, check [`Connection::execute`] for usage.
    pub async fn execute(&self, params: impl IntoParams) -> Result<usize> {
        tracing::trace!("execute for prepared statement");
        self.inner.execute(&params.into_params()?).await
    }

    /// Execute a query on the statement, check [`Connection::query`] for usage.
    pub async fn query(&self, params: impl IntoParams) -> Result<Rows> {
        tracing::trace!("query for prepared statement");
        self.inner.query(&params.into_params()?).await
    }

    /// Run a query on the statement.
    ///
    /// The `execute()` method returns an error if the query returns rows, which makes
    /// it unsuitable for running any type of SQL queries. Similarly, the `query()` method
    /// only works on SQL statements that return rows. Therefore, the `run()` method is
    /// provided to execute any type of SQL statement.
    ///
    /// Note: This is an extension to the Rusqlite API.
    pub async fn run(&self, params: impl IntoParams) -> Result<()> {
        tracing::trace!("run for prepared statement");
        self.inner.run(&params.into_params()?).await?;
        Ok(())
    }

    /// Interrupt the statement.
    pub fn interrupt(&self) -> Result<()> {
        self.inner.interrupt()
    }

    /// Execute a query that returns the first [`Row`].
    ///
    /// # Errors
    ///
    /// - Returns `QueryReturnedNoRows` if no rows were returned.
    pub async fn query_row(&mut self, params: impl IntoParams) -> Result<Row> {
        let mut rows = self.query(params).await?;

        let row = rows.next().await?.ok_or(Error::QueryReturnedNoRows)?;

        Ok(row)
    }

    /// Reset the state of this prepared statement.
    pub fn reset(&self) {
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

    /// Fetch the number of columns for the prepared statement.
    pub fn column_count(&self) -> usize {
        self.inner.column_count()
    }

    /// Fetch the list of columns for the prepared statement.
    pub fn columns(&self) -> Vec<Column> {
        self.inner.columns()
    }

    /// Bind a value to the parameter at `one_based_col_index`, to be used by
    /// [`Statement::raw_execute`] and [`Statement::raw_query`].
    ///
    /// This is the low-level binding API from rusqlite, for callers such as
    /// language bindings that receive parameters one at a time. Named
    /// parameters (`:name`, `@name`, `$name`) are bound by their index too;
    /// [`Statement::parameter_name`] maps an index to its name.
    ///
    /// Bindings are kept until [`Statement::clear_bindings`] is called, so
    /// the statement can be run again with some values changed. A parameter
    /// that was never bound is `NULL`, as in SQLite.
    ///
    /// # Errors
    ///
    /// Returns `Error::Misuse` if the index is `0`, or larger than
    /// [`Statement::parameter_count`] when the statement reports its
    /// parameters (statements prepared on a remote connection report none
    /// until they are executed, so only the lower bound is checked there).
    pub fn raw_bind_parameter(
        &mut self,
        one_based_col_index: usize,
        param: impl IntoValue,
    ) -> Result<()> {
        let count = self.parameter_count();
        if one_based_col_index == 0 || (count > 0 && one_based_col_index > count) {
            return Err(Error::Misuse(format!(
                "parameter index {one_based_col_index} is out of range (1..={count})"
            )));
        }
        let value = param.into_value()?;
        if self.raw_params.len() < one_based_col_index {
            self.raw_params.resize(one_based_col_index, Value::Null);
        }
        self.raw_params[one_based_col_index - 1] = value;
        Ok(())
    }

    /// Clear the values set with [`Statement::raw_bind_parameter`].
    pub fn clear_bindings(&mut self) {
        self.raw_params.clear();
    }

    /// Execute the statement with the values set with
    /// [`Statement::raw_bind_parameter`], check [`Connection::execute`] for usage.
    pub async fn raw_execute(&self) -> Result<usize> {
        self.inner.execute(&self.raw_params()).await
    }

    /// Run a query on the statement with the values set with
    /// [`Statement::raw_bind_parameter`], check [`Connection::query`] for usage.
    pub async fn raw_query(&self) -> Result<Rows> {
        self.inner.query(&self.raw_params()).await
    }

    /// Every parameter the statement reports is bound, unbound ones as `NULL`,
    /// so a value from an earlier run never survives in the underlying
    /// SQLite statement.
    fn raw_params(&self) -> Params {
        let len = self.raw_params.len().max(self.parameter_count());
        if len == 0 {
            return Params::None;
        }
        let mut values = self.raw_params.clone();
        values.resize(len, Value::Null);
        Params::Positional(values)
    }
}
