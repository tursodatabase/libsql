use std::fmt::{self, Write as _};
use std::io;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use rusqlite::types::ValueRef;

use crate::hrana::stmt::{proto_error_from_stmt_error, stmt_error_from_sqld_error};
use crate::query_result_builder::{
    Column, QueryBuilderConfig, QueryResultBuilder, QueryResultBuilderError, TOTAL_RESPONSE_SIZE,
};
use crate::replication::FrameNo;

use super::proto;

#[derive(Debug, Default)]
pub struct SingleStatementBuilder {
    has_step: bool,
    cols: Vec<proto::Col>,
    rows: Vec<proto::Row>,
    err: Option<crate::error::Error>,
    affected_row_count: u64,
    last_insert_rowid: Option<i64>,
    current_size: u64,
    max_response_size: u64,
    max_total_response_size: u64,
    last_frame_no: Option<FrameNo>,
    rows_read: u64,
    rows_written: u64,
    query_duration_ms: f64,
}

struct SizeFormatter {
    size: u64,
}

impl SizeFormatter {
    fn new() -> Self {
        Self { size: 0 }
    }
}

impl io::Write for SizeFormatter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.size += buf.len() as u64;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl fmt::Write for SizeFormatter {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        self.size += s.len() as u64;
        Ok(())
    }
}

pub fn value_json_size(v: &ValueRef) -> u64 {
    let mut f = SizeFormatter::new();
    match v {
        ValueRef::Null => write!(&mut f, r#"{{"type":"null"}}"#).unwrap(),
        ValueRef::Integer(i) => write!(&mut f, r#"{{"type":"integer","value":"{i}"}}"#).unwrap(),
        ValueRef::Real(x) => write!(&mut f, r#"{{"type":"float","value":{x}"}}"#).unwrap(),
        ValueRef::Text(s) => {
            // error will be caught later.
            if let Ok(s) = std::str::from_utf8(s) {
                write!(&mut f, r#"{{"type":"text","value":"{s}"}}"#).unwrap()
            }
        }
        ValueRef::Blob(b) => return b.len() as u64,
    }
    f.size
}

pub fn value_to_proto(v: ValueRef) -> Result<proto::Value, QueryResultBuilderError> {
    Ok(match v {
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
    })
}

impl Drop for SingleStatementBuilder {
    fn drop(&mut self) {
        TOTAL_RESPONSE_SIZE.fetch_sub(self.current_size as usize, Ordering::Relaxed);
    }
}

impl SingleStatementBuilder {
    fn inc_current_size(&mut self, size: u64) -> Result<(), QueryResultBuilderError> {
        if self.current_size + size > self.max_response_size {
            return Err(QueryResultBuilderError::ResponseTooLarge(
                self.current_size + size,
            ));
        }

        self.current_size += size;
        let total_size = TOTAL_RESPONSE_SIZE.fetch_add(size as usize, Ordering::Relaxed) as u64;
        if total_size + size > self.max_total_response_size {
            tracing::debug!(
                "Total responses exceeded threshold: {}/{}, aborting query",
                total_size + size,
                self.max_total_response_size
            );
            return Err(QueryResultBuilderError::ResponseTooLarge(total_size + size));
        }
        Ok(())
    }
}

impl QueryResultBuilder for SingleStatementBuilder {
    type Ret = Result<proto::StmtResult, crate::error::Error>;

    fn init(&mut self, config: &QueryBuilderConfig) -> Result<(), QueryResultBuilderError> {
        let _ = std::mem::take(self);

        self.max_response_size = config.max_size.unwrap_or(u64::MAX);
        self.max_total_response_size = config.max_total_size.unwrap_or(u64::MAX);

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
        let mut f = SizeFormatter::new();
        write!(&mut f, "{error}").unwrap();
        TOTAL_RESPONSE_SIZE.fetch_sub(self.current_size as usize, Ordering::Relaxed);
        self.current_size = f.size;
        TOTAL_RESPONSE_SIZE.fetch_add(self.current_size as usize, Ordering::Relaxed);
        self.err = Some(error);

        Ok(())
    }

    fn cols_description<'a>(
        &mut self,
        cols: impl IntoIterator<Item = impl Into<Column<'a>>>,
    ) -> Result<(), QueryResultBuilderError> {
        assert!(self.err.is_none());
        assert!(self.cols.is_empty());

        let mut cols_size = 0;

        self.cols.extend(cols.into_iter().map(Into::into).map(|c| {
            cols_size += estimate_cols_json_size(&c);
            proto::Col {
                name: Some(c.name.to_owned()),
                decltype: c.decl_ty.map(ToString::to_string),
            }
        }));

        self.inc_current_size(cols_size)?;

        Ok(())
    }

    fn begin_rows(&mut self) -> Result<(), QueryResultBuilderError> {
        assert!(self.err.is_none());
        assert!(self.rows.is_empty());
        Ok(())
    }

    fn begin_row(&mut self) -> Result<(), QueryResultBuilderError> {
        assert!(self.err.is_none());
        self.rows.push(proto::Row {
            values: Vec::with_capacity(self.cols.len()),
        });
        Ok(())
    }

    fn add_row_value(&mut self, v: ValueRef) -> Result<(), QueryResultBuilderError> {
        assert!(self.err.is_none());
        let estimate_size = value_json_size(&v);
        if self.current_size + estimate_size > self.max_response_size {
            return Err(QueryResultBuilderError::ResponseTooLarge(
                self.max_response_size,
            ));
        }

        self.inc_current_size(estimate_size)?;
        let val = value_to_proto(v)?;

        self.rows
            .last_mut()
            .expect("row must be initialized")
            .values
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

    fn finish(
        &mut self,
        last_frame_no: Option<FrameNo>,
        _is_autocommit: bool,
    ) -> Result<(), QueryResultBuilderError> {
        self.last_frame_no = last_frame_no;
        Ok(())
    }

    fn into_ret(mut self) -> Self::Ret {
        match std::mem::take(&mut self.err) {
            Some(err) => Err(err),
            None => Ok(proto::StmtResult {
                cols: std::mem::take(&mut self.cols),
                rows: std::mem::take(&mut self.rows),
                affected_row_count: std::mem::take(&mut self.affected_row_count),
                last_insert_rowid: std::mem::take(&mut self.last_insert_rowid),
                replication_index: self.last_frame_no,
                rows_read: self.rows_read,
                rows_written: self.rows_written,
                query_duration_ms: self.query_duration_ms,
            }),
        }
    }

    fn add_stats(&mut self, rows_read: u64, rows_written: u64, duration: Duration) {
        self.rows_read = self.rows_read.wrapping_add(rows_read);
        self.rows_written = self.rows_written.wrapping_add(rows_written);
        self.query_duration_ms = self.query_duration_ms + (duration.as_micros() as f64 / 1_000.0);
    }
}

pub fn estimate_cols_json_size(c: &Column) -> u64 {
    let mut f = SizeFormatter::new();
    write!(
        &mut f,
        r#"{{"name":"{}","decltype":"{}"}}"#,
        c.name,
        c.decl_ty.unwrap_or("null")
    )
    .unwrap();
    f.size
}

#[derive(Debug, Default)]
pub struct HranaBatchProtoBuilder {
    step_results: Vec<Option<proto::StmtResult>>,
    step_errors: Vec<Option<crate::hrana::proto::Error>>,
    stmt_builder: SingleStatementBuilder,
    current_size: u64,
    max_response_size: u64,
    step_empty: bool,
    last_frame_no: Option<FrameNo>,
}

impl QueryResultBuilder for HranaBatchProtoBuilder {
    type Ret = proto::BatchResult;

    fn init(&mut self, config: &QueryBuilderConfig) -> Result<(), QueryResultBuilderError> {
        *self = Self {
            max_response_size: config.max_size.unwrap_or(u64::MAX),
            ..Default::default()
        };
        self.stmt_builder.init(config)?;
        Ok(())
    }

    fn begin_step(&mut self) -> Result<(), QueryResultBuilderError> {
        self.step_empty = true;
        self.stmt_builder.begin_step()
    }

    fn finish_step(
        &mut self,
        affected_row_count: u64,
        last_insert_rowid: Option<i64>,
    ) -> Result<(), QueryResultBuilderError> {
        self.stmt_builder
            .finish_step(affected_row_count, last_insert_rowid)?;
        self.current_size += self.stmt_builder.current_size;

        let max_total_response_size = self.stmt_builder.max_total_response_size;
        let previous_builder = std::mem::take(&mut self.stmt_builder);
        self.stmt_builder.max_response_size = self.max_response_size - self.current_size;
        self.stmt_builder.max_total_response_size = max_total_response_size;
        match previous_builder.into_ret() {
            Ok(res) => {
                self.step_results.push((!self.step_empty).then_some(res));
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
        self.step_empty = false;
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

    fn finish(
        &mut self,
        last_frame_no: Option<FrameNo>,
        _is_autocommit: bool,
    ) -> Result<(), QueryResultBuilderError> {
        self.last_frame_no = last_frame_no;
        Ok(())
    }

    fn into_ret(self) -> Self::Ret {
        proto::BatchResult {
            step_results: self.step_results,
            step_errors: self.step_errors,
            replication_index: self.last_frame_no,
        }
    }

    fn add_stats(&mut self, rows_read: u64, rows_written: u64, duration: Duration) {
        self.stmt_builder
            .add_stats(rows_read, rows_written, duration);
    }
}
