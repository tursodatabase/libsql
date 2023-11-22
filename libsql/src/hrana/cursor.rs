// https://github.com/tursodatabase/libsql/blob/main/docs/HRANA_3_SPEC.md#cursor-entries

use crate::hrana::proto::{Batch, Col, Value};
use crate::hrana::{ByteStream, HttpBody, HttpSend, Result};
use serde::{Deserialize, Serialize};
use std::pin::Pin;
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

pub struct Cursor {
    response_stream: ByteStream,
}

impl Cursor {}

impl futures::Stream for Cursor {
    type Item = Result<CursorEntry>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}
