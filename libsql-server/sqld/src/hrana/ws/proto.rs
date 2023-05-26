//! Structures for Hrana-over-WebSockets.

pub use super::super::proto::*;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMsg {
    Hello { jwt: Option<String> },
    Request { request_id: i32, request: Request },
}

#[derive(Serialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMsg {
    HelloOk {},
    HelloError { error: Error },
    ResponseOk { request_id: i32, response: Response },
    ResponseError { request_id: i32, error: Error },
}

#[derive(Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Request {
    OpenStream(OpenStreamReq),
    CloseStream(CloseStreamReq),
    Execute(ExecuteReq),
    Batch(BatchReq),
    Sequence(SequenceReq),
    Describe(DescribeReq),
    StoreSql(StoreSqlReq),
    CloseSql(CloseSqlReq),
}

#[derive(Serialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Response {
    OpenStream(OpenStreamResp),
    CloseStream(CloseStreamResp),
    Execute(ExecuteResp),
    Batch(BatchResp),
    Sequence(SequenceResp),
    Describe(DescribeResp),
    StoreSql(StoreSqlResp),
    CloseSql(CloseSqlResp),
}

#[derive(Deserialize, Debug)]
pub struct OpenStreamReq {
    pub stream_id: i32,
}

#[derive(Serialize, Debug)]
pub struct OpenStreamResp {}

#[derive(Deserialize, Debug)]
pub struct CloseStreamReq {
    pub stream_id: i32,
}

#[derive(Serialize, Debug)]
pub struct CloseStreamResp {}

#[derive(Deserialize, Debug)]
pub struct ExecuteReq {
    pub stream_id: i32,
    pub stmt: Stmt,
}

#[derive(Serialize, Debug)]
pub struct ExecuteResp {
    pub result: StmtResult,
}

#[derive(Deserialize, Debug)]
pub struct BatchReq {
    pub stream_id: i32,
    pub batch: Batch,
}

#[derive(Serialize, Debug)]
pub struct BatchResp {
    pub result: BatchResult,
}

#[derive(Deserialize, Debug)]
pub struct SequenceReq {
    pub stream_id: i32,
    #[serde(default)]
    pub sql: Option<String>,
    #[serde(default)]
    pub sql_id: Option<i32>,
}

#[derive(Serialize, Debug)]
pub struct SequenceResp {}

#[derive(Deserialize, Debug)]
pub struct DescribeReq {
    pub stream_id: i32,
    #[serde(default)]
    pub sql: Option<String>,
    #[serde(default)]
    pub sql_id: Option<i32>,
}

#[derive(Serialize, Debug)]
pub struct DescribeResp {
    pub result: DescribeResult,
}

#[derive(Deserialize, Debug)]
pub struct StoreSqlReq {
    pub sql_id: i32,
    pub sql: String,
}

#[derive(Serialize, Debug)]
pub struct StoreSqlResp {}

#[derive(Deserialize, Debug)]
pub struct CloseSqlReq {
    pub sql_id: i32,
}

#[derive(Serialize, Debug)]
pub struct CloseSqlResp {}
