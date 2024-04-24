use std::time::Duration;

use crate::query_result_builder::QueryResultBuilder;

pub struct SchemaMigrationResultBuilder<B> {
    inner: B,
    step: usize,
    errors: Vec<(usize, String)>,
}

impl<B> SchemaMigrationResultBuilder<B> {
    pub(crate) fn new(inner: B) -> Self {
        Self {
            inner,
            step: 0,
            errors: Vec::new(),
        }
    }

    pub(crate) fn into_inner(self) -> B {
        self.inner
    }

    pub(crate) fn is_success(&self) -> bool {
        self.errors.is_empty()
    }

    pub(crate) fn into_error(self) -> (usize, String) {
        assert!(!self.errors.is_empty());
        self.errors.into_iter().next().unwrap()
    }
}

impl<B: QueryResultBuilder> QueryResultBuilder for SchemaMigrationResultBuilder<B> {
    type Ret = B::Ret;

    fn init(
        &mut self,
        config: &crate::query_result_builder::QueryBuilderConfig,
    ) -> Result<(), crate::query_result_builder::QueryResultBuilderError> {
        self.step = 0;
        self.inner.init(config)
    }

    fn begin_step(&mut self) -> Result<(), crate::query_result_builder::QueryResultBuilderError> {
        self.step += 1;
        self.inner.begin_step()
    }

    fn finish_step(
        &mut self,
        affected_row_count: u64,
        last_insert_rowid: Option<i64>,
    ) -> Result<(), crate::query_result_builder::QueryResultBuilderError> {
        self.inner
            .finish_step(affected_row_count, last_insert_rowid)
    }

    fn step_error(
        &mut self,
        error: crate::error::Error,
    ) -> Result<(), crate::query_result_builder::QueryResultBuilderError> {
        self.errors.push((self.step, error.to_string()));
        self.inner.step_error(error)
    }

    fn cols_description<'a>(
        &mut self,
        cols: impl IntoIterator<Item = impl Into<crate::query_result_builder::Column<'a>>>,
    ) -> Result<(), crate::query_result_builder::QueryResultBuilderError> {
        self.inner.cols_description(cols)
    }

    fn begin_rows(&mut self) -> Result<(), crate::query_result_builder::QueryResultBuilderError> {
        self.inner.begin_rows()
    }

    fn begin_row(&mut self) -> Result<(), crate::query_result_builder::QueryResultBuilderError> {
        self.inner.begin_row()
    }

    fn add_row_value(
        &mut self,
        v: rusqlite::types::ValueRef,
    ) -> Result<(), crate::query_result_builder::QueryResultBuilderError> {
        self.inner.add_row_value(v)
    }

    fn finish_row(&mut self) -> Result<(), crate::query_result_builder::QueryResultBuilderError> {
        self.inner.finish_row()
    }

    fn finish_rows(&mut self) -> Result<(), crate::query_result_builder::QueryResultBuilderError> {
        self.inner.finish_rows()
    }

    fn finish(
        &mut self,
        last_frame_no: Option<crate::replication::FrameNo>,
        is_auto_commit: bool,
    ) -> Result<(), crate::query_result_builder::QueryResultBuilderError> {
        self.inner.finish(last_frame_no, is_auto_commit)
    }

    fn into_ret(self) -> Self::Ret {
        self.inner.into_ret()
    }

    fn add_stats(&mut self, rows_read: u64, rows_written: u64, duration: Duration) {
        self.inner.add_stats(rows_read, rows_written, duration);
    }
}
