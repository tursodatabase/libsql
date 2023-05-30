use bytes::Bytes;
use rusqlite::types::ValueRef;

use crate::hrana::stmt::{proto_error_from_stmt_error, stmt_error_from_sqld_error};
use crate::query_result_builder::{Column, QueryResultBuilder, QueryResultBuilderError};

use super::proto;

#[derive(Debug, Default)]
pub struct SingleStatementBuilder {
    has_step: bool,
    cols: Vec<proto::Col>,
    rows: Vec<Vec<proto::Value>>,
    err: Option<crate::error::Error>,
    affected_row_count: u64,
    last_insert_rowid: Option<i64>,
}

impl QueryResultBuilder for SingleStatementBuilder {
    type Ret = Result<proto::StmtResult, crate::error::Error>;

    fn init(&mut self) -> Result<(), QueryResultBuilderError> {
        *self = Default::default();
        Ok(())
    }

    fn begin_step(&mut self) -> Result<(), QueryResultBuilderError> {
        // SingleStatementBuilder only builds a single statement
        assert!(!self.has_step);
        self.has_step = true;
        Ok(())
    }

    fn finish_step(
        &mut self,
        affected_row_count: u64,
        last_insert_rowid: Option<i64>,
    ) -> Result<(), QueryResultBuilderError> {
        self.last_insert_rowid = last_insert_rowid;
        self.affected_row_count = affected_row_count;

        Ok(())
    }

    fn step_error(&mut self, error: crate::error::Error) -> Result<(), QueryResultBuilderError> {
        assert!(self.err.is_none());

        self.err = Some(error);

        Ok(())
    }

    fn cols_description<'a>(
        &mut self,
        cols: impl IntoIterator<Item = impl Into<Column<'a>>>,
    ) -> Result<(), QueryResultBuilderError> {
        assert!(self.err.is_none());
        assert!(self.cols.is_empty());

        self.cols
            .extend(cols.into_iter().map(Into::into).map(|c| proto::Col {
                name: Some(c.name.to_owned()),
                decltype: c.decl_ty.map(ToString::to_string),
            }));

        Ok(())
    }

    fn begin_rows(&mut self) -> Result<(), QueryResultBuilderError> {
        assert!(self.err.is_none());
        assert!(self.rows.is_empty());
        Ok(())
    }

    fn begin_row(&mut self) -> Result<(), QueryResultBuilderError> {
        assert!(self.err.is_none());
        self.rows.push(Vec::with_capacity(self.cols.len()));
        Ok(())
    }

    fn add_row_value(&mut self, v: ValueRef) -> Result<(), QueryResultBuilderError> {
        assert!(self.err.is_none());
        let val = match v {
            ValueRef::Null => proto::Value::Null,
            ValueRef::Integer(value) => proto::Value::Integer { value },
            ValueRef::Real(value) => proto::Value::Float { value },
            ValueRef::Text(s) => proto::Value::Text {
                value: String::from_utf8(s.to_vec())
                    .map_err(QueryResultBuilderError::from_any)?
                    .into(),
            },
            ValueRef::Blob(d) => proto::Value::Blob {
                value: Bytes::copy_from_slice(d),
            },
        };

        self.rows
            .last_mut()
            .expect("row must be initialized")
            .push(val);

        Ok(())
    }

    fn finish_row(&mut self) -> Result<(), QueryResultBuilderError> {
        assert!(self.err.is_none());
        Ok(())
    }

    fn finish_rows(&mut self) -> Result<(), QueryResultBuilderError> {
        assert!(self.err.is_none());
        Ok(())
    }

    fn finish(&mut self) -> Result<(), QueryResultBuilderError> {
        Ok(())
    }

    fn into_ret(self) -> Self::Ret {
        match self.err {
            Some(err) => Err(err),
            None => Ok(proto::StmtResult {
                cols: self.cols,
                rows: self.rows,
                affected_row_count: self.affected_row_count,
                last_insert_rowid: self.last_insert_rowid,
            }),
        }
    }
}

#[derive(Debug, Default)]
pub struct HranaBatchProtoBuilder {
    step_results: Vec<Option<proto::StmtResult>>,
    step_errors: Vec<Option<crate::hrana::proto::Error>>,
    stmt_builder: SingleStatementBuilder,
}

impl QueryResultBuilder for HranaBatchProtoBuilder {
    type Ret = proto::BatchResult;

    fn init(&mut self) -> Result<(), QueryResultBuilderError> {
        *self = Default::default();
        Ok(())
    }

    fn begin_step(&mut self) -> Result<(), QueryResultBuilderError> {
        self.stmt_builder.begin_step()
    }

    fn finish_step(
        &mut self,
        affected_row_count: u64,
        last_insert_rowid: Option<i64>,
    ) -> Result<(), QueryResultBuilderError> {
        self.stmt_builder
            .finish_step(affected_row_count, last_insert_rowid)?;

        match std::mem::take(&mut self.stmt_builder).into_ret() {
            Ok(res) => {
                self.step_results.push(Some(res));
                self.step_errors.push(None);
            }
            Err(e) => {
                self.step_results.push(None);
                self.step_errors.push(Some(proto_error_from_stmt_error(
                    &stmt_error_from_sqld_error(e).map_err(QueryResultBuilderError::from_any)?,
                )));
            }
        }

        Ok(())
    }

    fn step_error(&mut self, error: crate::error::Error) -> Result<(), QueryResultBuilderError> {
        self.stmt_builder.step_error(error)
    }

    fn cols_description<'a>(
        &mut self,
        cols: impl IntoIterator<Item = impl Into<Column<'a>>>,
    ) -> Result<(), QueryResultBuilderError> {
        self.stmt_builder.cols_description(cols)
    }

    fn begin_rows(&mut self) -> Result<(), QueryResultBuilderError> {
        self.stmt_builder.begin_rows()
    }

    fn begin_row(&mut self) -> Result<(), QueryResultBuilderError> {
        self.stmt_builder.begin_row()
    }

    fn add_row_value(&mut self, v: ValueRef) -> Result<(), QueryResultBuilderError> {
        self.stmt_builder.add_row_value(v)
    }

    fn finish_row(&mut self) -> Result<(), QueryResultBuilderError> {
        self.stmt_builder.finish_row()
    }

    fn finish_rows(&mut self) -> Result<(), QueryResultBuilderError> {
        Ok(())
    }

    fn finish(&mut self) -> Result<(), QueryResultBuilderError> {
        Ok(())
    }

    fn into_ret(self) -> Self::Ret {
        proto::BatchResult {
            step_results: self.step_results,
            step_errors: self.step_errors,
        }
    }
}
