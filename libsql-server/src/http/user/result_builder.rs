use std::io;
use std::ops::{Deref, DerefMut};
use std::time::Duration;

use rusqlite::types::ValueRef;
use serde::{Serialize, Serializer};
use serde_json::ser::{CompactFormatter, Formatter};
use std::sync::atomic::Ordering;

use crate::query_result_builder::{
    Column, JsonFormatter, QueryBuilderConfig, QueryResultBuilder, QueryResultBuilderError,
    TOTAL_RESPONSE_SIZE,
};
use crate::replication::FrameNo;

pub struct JsonHttpPayloadBuilder {
    formatter: JsonFormatter<CompactFormatter>,
    buffer: LimitBuffer,
    checkpoint: usize,
    /// number of steps
    step_count: usize,
    /// number of values in the current row.
    row_value_count: usize,
    /// number of row in the current step
    step_row_count: usize,
    is_step_error: bool,
    is_step_empty: bool,
}

#[derive(Default)]
struct LimitBuffer {
    buffer: Vec<u8>,
    limit: u64,
    global_limit: u64,
}

impl LimitBuffer {
    fn new(limit: u64, global_limit: u64) -> Self {
        Self {
            buffer: Vec::new(),
            limit,
            global_limit,
        }
    }

    fn into_inner(mut self) -> Vec<u8> {
        TOTAL_RESPONSE_SIZE.fetch_sub(self.buffer.len(), Ordering::Relaxed);
        std::mem::take(&mut self.buffer)
    }
}

impl Deref for LimitBuffer {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.buffer
    }
}

impl DerefMut for LimitBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.buffer
    }
}

impl io::Write for LimitBuffer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let total_size = TOTAL_RESPONSE_SIZE.fetch_add(buf.len(), Ordering::Relaxed);
        if (total_size + buf.len()) as u64 > self.global_limit {
            tracing::debug!(
                "Total responses exceeded threshold: {}/{}, aborting query",
                total_size + buf.len(),
                self.global_limit
            );
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                QueryResultBuilderError::ResponseTooLarge(self.global_limit),
            ));
        }
        if (self.buffer.len() + buf.len()) as u64 > self.limit {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                QueryResultBuilderError::ResponseTooLarge(self.limit),
            ));
        }
        self.buffer.extend(buf);

        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for LimitBuffer {
    fn drop(&mut self) {
        TOTAL_RESPONSE_SIZE.fetch_sub(self.buffer.len(), Ordering::Relaxed);
    }
}

struct HttpJsonValueSerializer<'a>(&'a ValueRef<'a>);

impl JsonHttpPayloadBuilder {
    pub fn new() -> Self {
        Self {
            formatter: JsonFormatter(CompactFormatter),
            buffer: LimitBuffer::new(0, 0),
            checkpoint: 0,
            step_count: 0,
            row_value_count: 0,
            step_row_count: 0,
            is_step_error: false,
            is_step_empty: false,
        }
    }
}

impl<'a> Serialize for HttpJsonValueSerializer<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        struct Base64<'a> {
            #[serde(serialize_with = "serialize_b64")]
            base64: &'a [u8],
        }

        fn serialize_b64<S>(b: &[u8], serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            use base64::Engine;

            base64::prelude::BASE64_STANDARD_NO_PAD
                .encode(b)
                .serialize(serializer)
        }

        match self.0 {
            ValueRef::Null => serializer.serialize_none(),
            ValueRef::Integer(i) => serializer.serialize_i64(*i),
            ValueRef::Real(x) => serializer.serialize_f64(*x),
            ValueRef::Text(value) => {
                serializer.serialize_str(std::str::from_utf8(value).expect("invalid string"))
            }
            ValueRef::Blob(base64) => Base64 { base64 }.serialize(serializer),
        }
    }
}

impl QueryResultBuilder for JsonHttpPayloadBuilder {
    type Ret = Vec<u8>;

    fn init(&mut self, config: &QueryBuilderConfig) -> Result<(), QueryResultBuilderError> {
        *self = Self {
            buffer: LimitBuffer::new(
                config.max_size.unwrap_or(u64::MAX),
                config.max_total_size.unwrap_or(u64::MAX),
            ),
            ..Self::new()
        };
        // write fragment: `[`
        self.formatter.begin_array(&mut self.buffer)?;
        Ok(())
    }

    fn begin_step(&mut self) -> Result<(), QueryResultBuilderError> {
        // reset step state
        self.is_step_empty = true;
        self.is_step_error = false;
        self.formatter
            .begin_array_value(&mut self.buffer, self.step_count == 0)?;

        self.checkpoint = self.buffer.len();

        // write fragment: `{ "results": {`
        self.formatter.begin_object(&mut self.buffer)?;
        self.formatter
            .serialize_key(&mut self.buffer, "results", true)?;
        self.formatter.begin_object_value(&mut self.buffer)?;
        self.formatter.begin_object(&mut self.buffer)?;

        Ok(())
    }

    fn finish_step(
        &mut self,
        _affected_row_count: u64,
        _last_insert_rowid: Option<i64>,
    ) -> Result<(), QueryResultBuilderError> {
        if self.is_step_empty && !self.is_step_error {
            // rollback buffer and write null
            self.buffer.truncate(self.checkpoint);
            self.formatter.write_null(&mut self.buffer)?;
        } else if self.is_step_error {
            // write fragment: `}`
            self.formatter.end_object(&mut self.buffer)?;
        } else {
            // write fragment: `}}`
            self.formatter.end_object(&mut self.buffer)?;
            self.formatter.end_object(&mut self.buffer)?;
        }
        self.formatter.end_array_value(&mut self.buffer)?;
        self.step_count += 1;

        Ok(())
    }

    fn step_error(&mut self, error: crate::error::Error) -> Result<(), QueryResultBuilderError> {
        self.is_step_error = true;
        self.is_step_empty = false;
        self.buffer.truncate(self.checkpoint);
        // write fragment: `{"error": "(error)"`
        self.formatter.begin_object(&mut self.buffer)?;
        self.formatter
            .serialize_key_value(&mut self.buffer, "error", &error.to_string(), true)?;

        Ok(())
    }

    fn cols_description<'a>(
        &mut self,
        cols: impl IntoIterator<Item = impl Into<Column<'a>>>,
    ) -> Result<(), QueryResultBuilderError> {
        assert!(!self.is_step_error);
        self.is_step_empty = false;
        // write fragment: `"columns": @cols`
        self.formatter
            .serialize_key(&mut self.buffer, "columns", true)?;
        self.formatter.begin_object_value(&mut self.buffer)?;
        self.formatter
            .serialize_array_iter(&mut self.buffer, cols.into_iter().map(|c| c.into().name))?;
        self.formatter.end_object_value(&mut self.buffer)?;

        Ok(())
    }

    fn begin_rows(&mut self) -> Result<(), QueryResultBuilderError> {
        assert!(!self.is_step_error);
        self.step_row_count = 0;
        // write fragment: `,"rows": [`
        self.formatter
            .serialize_key(&mut self.buffer, "rows", false)?;
        self.formatter.begin_object_value(&mut self.buffer)?;
        self.formatter.begin_array(&mut self.buffer)?;

        Ok(())
    }

    fn begin_row(&mut self) -> Result<(), QueryResultBuilderError> {
        self.row_value_count = 0;
        assert!(!self.is_step_error);
        // write fragment: `[`
        self.formatter
            .begin_array_value(&mut self.buffer, self.step_row_count == 0)?;
        self.formatter.begin_array(&mut self.buffer)?;

        Ok(())
    }

    fn add_row_value(&mut self, v: ValueRef) -> Result<(), QueryResultBuilderError> {
        assert!(!self.is_step_error);

        self.formatter.serialize_array_value(
            &mut self.buffer,
            &HttpJsonValueSerializer(&v),
            self.row_value_count == 0,
        )?;
        self.row_value_count += 1;

        Ok(())
    }

    fn finish_row(&mut self) -> Result<(), QueryResultBuilderError> {
        assert!(!self.is_step_error);
        self.step_row_count += 1;

        // write fragment: `]`
        self.formatter.end_array(&mut self.buffer)?;
        self.formatter.end_array_value(&mut self.buffer)?;

        Ok(())
    }

    fn finish_rows(&mut self) -> Result<(), QueryResultBuilderError> {
        assert!(!self.is_step_error);
        // write fragment: `]`
        self.formatter.end_array(&mut self.buffer)?;
        self.formatter.end_object_value(&mut self.buffer)?;

        Ok(())
    }

    // TODO: how do we return last_frame_no?
    fn finish(
        &mut self,
        _last_frame_no: Option<FrameNo>,
        _is_autocommit: bool,
    ) -> Result<(), QueryResultBuilderError> {
        self.formatter.end_array(&mut self.buffer)?;

        Ok(())
    }

    fn into_ret(self) -> Self::Ret {
        self.buffer.into_inner()
    }

    fn add_stats(&mut self, rows_read: u64, rows_written: u64, duration: Duration) {
        let _ =
            self.formatter
                .serialize_key_value(&mut self.buffer, "rows_read", &rows_read, false);
        let _ = self.formatter.serialize_key_value(
            &mut self.buffer,
            "rows_written",
            &rows_written,
            false,
        );

        let _ = self.formatter.serialize_key_value(
            &mut self.buffer,
            "query_duration_ms",
            &(duration.as_micros() as f64 / 1_000.0),
            false,
        );
    }
}

#[cfg(test)]
mod test {

    use crate::query_result_builder::test::{fsm_builder_driver, random_transition};

    use super::*;

    #[test]
    fn test_json_builder() {
        for _ in 0..1000 {
            let builder = JsonHttpPayloadBuilder::new();
            let trace = random_transition(100);
            let ret = fsm_builder_driver(&trace, builder).into_ret();
            println!("{}", std::str::from_utf8(&ret).unwrap());
            // we produce valid json
            serde_json::from_slice::<Vec<serde_json::Value>>(&ret).unwrap();
        }
    }
}
