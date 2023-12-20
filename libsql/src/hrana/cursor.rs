// https://github.com/tursodatabase/libsql/blob/main/docs/HRANA_3_SPEC.md#cursor-entries

use crate::hrana::proto::{Batch, BatchResult, Col, StmtResult, Value};
use crate::hrana::{CursorResponseError, HranaError, Result, Row};
use bytes::Bytes;
use futures::{ready, Future, Stream, StreamExt};
use serde::de::Error;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

#[derive(Serialize, Debug)]
pub struct CursorReq {
    pub baton: Option<String>,
    pub batch: Batch,
}

#[derive(Deserialize, Debug)]
pub struct CursorResp {
    pub baton: Option<String>,
    pub base_url: Option<String>,
}

#[derive(Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CursorEntry {
    StepBegin(StepBeginEntry),
    StepEnd(StepEndEntry),
    StepError(StepErrorEntry),
    Row(RowEntry),
    Error(ErrorEntry),
}

#[derive(Deserialize, Debug)]
pub struct StepBeginEntry {
    pub step: u32,
    pub cols: Vec<Col>,
}

#[derive(Deserialize, Debug)]
pub struct StepEndEntry {
    pub affected_row_count: u32,
    pub last_inserted_rowid: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct RowEntry {
    pub row: Vec<Value>,
}

#[derive(Deserialize, Debug)]
pub struct StepErrorEntry {
    pub step: u32,
    pub error: String,
}

#[derive(Deserialize, Debug)]
pub struct ErrorEntry {
    pub error: String,
}

pub struct Cursor<S> {
    stream: S,
    buf: VecDeque<u8>,
}

impl<S> Cursor<S>
where
    S: Stream<Item = Result<Bytes>> + Unpin,
{
    pub(super) async fn open(stream: S) -> Result<(Self, CursorResp)> {
        let mut cursor = Cursor {
            stream,
            buf: VecDeque::new(),
        };
        if let Some(line) = cursor.next_line().await {
            let response: CursorResp = serde_json::from_str(&line?)?;
            Ok((cursor, response))
        } else {
            Err(HranaError::CursorError(CursorResponseError::CursorClosed))
        }
    }

    pub async fn into_batch_result(mut self) -> Result<BatchResult> {
        //FIXME: this is for the compatibility with the current libsql client API,
        //       which expects BatchResult to be returned
        let mut step_results = Vec::new();
        let mut step_errors = Vec::new();

        while let Ok(mut step) = self.next_step().await {
            let cols = (&*step.cols).clone();
            let mut rows = VecDeque::new();
            while let Some(res) = step.next().await {
                match res {
                    Ok(row) => {
                        let values = row.inner;
                        rows.push_back(values);
                    }
                    Err(err) => step_errors.push(Some(crate::hrana::proto::Error {
                        message: err.to_string(),
                    })),
                }
            }
            let affected_row_count = step.affected_rows() as u64;
            let last_insert_rowid = if let Some(rowid) = step.last_inserted_rowid() {
                let id = rowid.parse::<i64>().map_err(|_| {
                    serde_json::Error::invalid_value(
                        serde::de::Unexpected::Str(rowid),
                        &"decimal integer as a string",
                    )
                })?;
                Some(id)
            } else {
                None
            };
            let step_res = StmtResult {
                cols,
                rows,
                affected_row_count,
                last_insert_rowid,
            };
            step_results.push(Some(step_res));
        }
        Ok(BatchResult {
            step_results,
            step_errors,
        })
    }

    pub async fn next_step(&mut self) -> Result<CursorStep<S>> {
        CursorStep::new(self).await
    }

    pub async fn next_line(&mut self) -> Option<Result<String>> {
        //TODO: this could be optimized into dedicated async STM
        const NEW_LINE: u8 = '\n' as u8;
        let mut len = self.buf.len();
        let mut index = self.buf.iter().position(|b| *b == NEW_LINE);
        while index.is_none() {
            match self.stream.next().await {
                Some(Err(e)) => return Some(Err(e.into())),
                Some(Ok(bytes)) if !bytes.is_empty() => {
                    index = bytes.iter().position(|b| *b == NEW_LINE).map(|i| i + len);
                    self.buf.extend(bytes);
                    len = self.buf.len();
                }
                _ => break,
            };
        }

        let line: Vec<_> = if let Some(index) = index {
            let line = self.buf.drain(..index).collect();
            self.buf.pop_front(); // remove new line character from the buffer
            line
        } else {
            self.buf.drain(..).collect()
        };
        if line.is_empty() {
            None
        } else {
            let result = String::from_utf8(line).map_err(|_| {
                HranaError::UnexpectedResponse("Response is not a valid UTF-8 string".to_string())
            });
            Some(result)
        }
    }
}

impl<S> Stream for Cursor<S>
where
    S: Stream<Item = Result<Bytes>> + Unpin,
{
    type Item = Result<CursorEntry>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut fut = Box::pin(self.next_line());
        let res = ready!(Pin::new(&mut fut).poll(cx));
        match res {
            None => Poll::Ready(None),
            Some(Err(e)) => Poll::Ready(Some(Err(e))),
            Some(Ok(line)) => {
                let entry: CursorEntry = match serde_json::from_str(line.as_str()) {
                    Ok(entry) => entry,
                    Err(e) => return Poll::Ready(Some(Err(e.into()))),
                };
                Poll::Ready(Some(Ok(entry)))
            }
        }
    }
}

pub struct CursorStep<'a, S> {
    cursor: Option<&'a mut Cursor<S>>,
    cols: Arc<Vec<Col>>,
    step_no: u32,
    affected_rows: u32,
    last_inserted_rowid: Option<String>,
}

impl<'a, S> CursorStep<'a, S>
where
    S: Stream<Item = Result<Bytes>> + Unpin,
{
    async fn new(cursor: &'a mut Cursor<S>) -> Result<CursorStep<'a, S>> {
        let mut begin = None;
        while let Some(res) = cursor.next().await {
            match res? {
                CursorEntry::StepBegin(entry) => {
                    begin = Some(entry);
                    break;
                }
                CursorEntry::Row(_) => {
                    tracing::trace!("skipping over row message for previous cursor step")
                }
                CursorEntry::StepEnd(_) => {
                    tracing::debug!("skipping over StepEnd message for previous cursor step")
                }
                CursorEntry::StepError(e) => {
                    return Err(HranaError::CursorError(CursorResponseError::StepError {
                        step: e.step,
                        error: e.error,
                    }))
                }
                CursorEntry::Error(e) => {
                    return Err(HranaError::CursorError(CursorResponseError::Other(e.error)))
                }
            }
        }
        if let Some(begin) = begin {
            Ok(CursorStep {
                cursor: Some(cursor),
                cols: Arc::new(begin.cols),
                step_no: begin.step,
                affected_rows: 0,
                last_inserted_rowid: None,
            })
        } else {
            Err(HranaError::CursorError(CursorResponseError::CursorClosed))
        }
    }

    pub fn cols(&self) -> &[Col] {
        &self.cols
    }

    pub fn step_no(&self) -> u32 {
        self.step_no
    }

    pub fn affected_rows(&self) -> u32 {
        self.affected_rows
    }

    pub fn last_inserted_rowid(&self) -> Option<&str> {
        self.last_inserted_rowid.as_deref()
    }
}

impl<'a, S> Stream for CursorStep<'a, S>
where
    S: Stream<Item = Result<Bytes>> + Unpin,
{
    type Item = Result<Row>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let cursor = if let Some(cursor) = &mut self.cursor {
            Pin::new(cursor)
        } else {
            return Poll::Ready(None);
        };
        match ready!(cursor.poll_next(cx)) {
            None => {
                self.cursor = None;
                Poll::Ready(None)
            }
            Some(Err(e)) => Poll::Ready(Some(Err(e))),
            Some(Ok(entry)) => match entry {
                CursorEntry::Row(row) => {
                    let row = Row::new(self.cols.clone(), row.row);
                    Poll::Ready(Some(Ok(row)))
                }
                CursorEntry::StepEnd(end) => {
                    self.affected_rows = end.affected_row_count;
                    self.last_inserted_rowid = end.last_inserted_rowid;
                    self.cursor = None;
                    Poll::Ready(None)
                }
                CursorEntry::StepBegin(begin) => Poll::Ready(Some(Err(HranaError::CursorError(
                    CursorResponseError::NotClosed {
                        expected: self.step_no,
                        actual: begin.step,
                    },
                )))),
                CursorEntry::StepError(e) => Poll::Ready(Some(Err(HranaError::CursorError(
                    CursorResponseError::StepError {
                        step: e.step,
                        error: e.error,
                    },
                )))),
                CursorEntry::Error(e) => Poll::Ready(Some(Err(HranaError::CursorError(
                    CursorResponseError::Other(e.error),
                )))),
            },
        }
    }
}

#[cfg(test)]
mod test {
    use crate::hrana::cursor::Cursor;
    use crate::hrana::Result;
    use crate::rows::RowInner;
    use crate::Value;
    use bytes::Bytes;
    use futures::{Stream, StreamExt};
    use serde_json::json;

    type ByteStream = Box<dyn Stream<Item = Result<Bytes>> + Unpin>;

    fn byte_stream(entries: impl IntoIterator<Item = serde_json::Value>) -> ByteStream {
        let mut payload = Vec::new();
        const NEW_LINE: &[u8] = "\n".as_bytes();
        for v in entries.into_iter() {
            serde_json::to_writer(&mut payload, &v).unwrap();
            payload.extend_from_slice(NEW_LINE);
        }
        let chunks: Vec<_> = Bytes::from(payload)
            .chunks(23)
            .map(|chunk| Ok(Bytes::copy_from_slice(chunk)))
            .collect();
        let stream = futures::stream::iter(chunks);
        Box::new(stream)
    }
    #[tokio::test]
    async fn cursor_streaming() {
        let byte_stream = byte_stream(vec![
            json!({"baton": null, "base_url": null}),
            json!({"type": "step_begin", "step": 0, "cols": [{"name": "id"}, {"name": "email"}]}),
            json!({"type": "row", "row": [{"type": "integer", "value": "1"}, {"type": "text", "value": "alice@test.com"}]}),
            json!({"type": "row", "row": [{"type": "integer", "value": "2"}, {"type": "text", "value": "bob@test.com"}]}),
            json!({"type": "step_end", "affected_row_count": 0, "last_insert_rowid": null}),
        ]);
        let (mut cursor, resp) = Cursor::open(byte_stream).await.unwrap();
        assert_eq!(resp.baton, None);
        assert_eq!(resp.base_url, None);

        let mut step = cursor.next_step().await.unwrap();
        assert_eq!(step.step_no(), 0);
        {
            let cols: Vec<_> = step
                .cols
                .iter()
                .map(|col| col.name.as_deref().unwrap_or(""))
                .collect();
            assert_eq!(cols, vec!["id", "email"]);
        }

        let row = step.next().await.unwrap().unwrap();
        assert_eq!(row.column_value(0).unwrap(), Value::from(1));
        assert_eq!(row.column_value(1).unwrap(), Value::from("alice@test.com"));

        let row = step.next().await.unwrap().unwrap();
        assert_eq!(row.column_value(0).unwrap(), Value::from(2));
        assert_eq!(row.column_value(1).unwrap(), Value::from("bob@test.com"));

        let row = step.next().await;
        assert!(row.is_none(), "last row should be None: {:?}", row);
    }
}
