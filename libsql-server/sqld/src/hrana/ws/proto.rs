//! Structures for Hrana-over-WebSockets.

pub use super::super::proto::*;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Debug, Default)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMsg {
    #[serde(skip_deserializing)]
    #[default]
    None,
    Hello(HelloMsg),
    Request(RequestMsg),
}

#[derive(Deserialize, prost::Message)]
pub struct HelloMsg {
    #[prost(string, optional, tag = "1")]
    pub jwt: Option<String>,
}

#[derive(Deserialize, prost::Message)]
pub struct RequestMsg {
    #[prost(int32, tag = "1")]
    pub request_id: i32,
    #[prost(oneof = "Request", tags = "2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13")]
    pub request: Option<Request>,
}

#[derive(Serialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMsg {
    HelloOk(HelloOkMsg),
    HelloError(HelloErrorMsg),
    ResponseOk(ResponseOkMsg),
    ResponseError(ResponseErrorMsg),
}

#[derive(Serialize, prost::Message)]
pub struct HelloOkMsg {}

#[derive(Serialize, prost::Message)]
pub struct HelloErrorMsg {
    #[prost(message, required, tag = "1")]
    pub error: Error,
}

#[derive(Serialize, prost::Message)]
pub struct ResponseOkMsg {
    #[prost(int32, tag = "1")]
    pub request_id: i32,
    #[prost(oneof = "Response", tags = "2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13")]
    pub response: Option<Response>,
}

#[derive(Serialize, prost::Message)]
pub struct ResponseErrorMsg {
    #[prost(int32, tag = "1")]
    pub request_id: i32,
    #[prost(message, required, tag = "2")]
    pub error: Error,
}

#[derive(Deserialize, prost::Oneof)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Request {
    #[prost(message, tag = "2")]
    OpenStream(OpenStreamReq),
    #[prost(message, tag = "3")]
    CloseStream(CloseStreamReq),
    #[prost(message, tag = "4")]
    Execute(ExecuteReq),
    #[prost(message, tag = "5")]
    Batch(BatchReq),
    #[prost(message, tag = "6")]
    OpenCursor(OpenCursorReq),
    #[prost(message, tag = "7")]
    CloseCursor(CloseCursorReq),
    #[prost(message, tag = "8")]
    FetchCursor(FetchCursorReq),
    #[prost(message, tag = "9")]
    Sequence(SequenceReq),
    #[prost(message, tag = "10")]
    Describe(DescribeReq),
    #[prost(message, tag = "11")]
    StoreSql(StoreSqlReq),
    #[prost(message, tag = "12")]
    CloseSql(CloseSqlReq),
    #[prost(message, tag = "13")]
    GetAutocommit(GetAutocommitReq),
}

#[derive(Serialize, prost::Oneof)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Response {
    #[prost(message, tag = "2")]
    OpenStream(OpenStreamResp),
    #[prost(message, tag = "3")]
    CloseStream(CloseStreamResp),
    #[prost(message, tag = "4")]
    Execute(ExecuteResp),
    #[prost(message, tag = "5")]
    Batch(BatchResp),
    #[prost(message, tag = "6")]
    OpenCursor(OpenCursorResp),
    #[prost(message, tag = "7")]
    CloseCursor(CloseCursorResp),
    #[prost(message, tag = "8")]
    FetchCursor(FetchCursorResp),
    #[prost(message, tag = "9")]
    Sequence(SequenceResp),
    #[prost(message, tag = "10")]
    Describe(DescribeResp),
    #[prost(message, tag = "11")]
    StoreSql(StoreSqlResp),
    #[prost(message, tag = "12")]
    CloseSql(CloseSqlResp),
    #[prost(message, tag = "13")]
    GetAutocommit(GetAutocommitResp),
}

#[derive(Deserialize, prost::Message)]
pub struct OpenStreamReq {
    #[prost(int32, tag = "1")]
    pub stream_id: i32,
}

#[derive(Serialize, prost::Message)]
pub struct OpenStreamResp {}

#[derive(Deserialize, prost::Message)]
pub struct CloseStreamReq {
    #[prost(int32, tag = "1")]
    pub stream_id: i32,
}

#[derive(Serialize, prost::Message)]
pub struct CloseStreamResp {}

#[derive(Deserialize, prost::Message)]
pub struct ExecuteReq {
    #[prost(int32, tag = "1")]
    pub stream_id: i32,
    #[prost(message, required, tag = "2")]
    pub stmt: Stmt,
    #[serde(default)]
    #[prost(uint64, optional, tag = "3")]
    pub replication_index: Option<u64>,
}

#[derive(Serialize, prost::Message)]
pub struct ExecuteResp {
    #[prost(message, required, tag = "1")]
    pub result: StmtResult,
}

#[derive(Deserialize, prost::Message)]
pub struct BatchReq {
    #[prost(int32, tag = "1")]
    pub stream_id: i32,
    #[prost(message, required, tag = "2")]
    pub batch: Batch,
}

#[derive(Serialize, prost::Message)]
pub struct BatchResp {
    #[prost(message, required, tag = "1")]
    pub result: BatchResult,
}

#[derive(Deserialize, prost::Message)]
pub struct OpenCursorReq {
    #[prost(int32, tag = "1")]
    pub stream_id: i32,
    #[prost(int32, tag = "2")]
    pub cursor_id: i32,
    #[prost(message, required, tag = "3")]
    pub batch: Batch,
}

#[derive(Serialize, prost::Message)]
pub struct OpenCursorResp {}

#[derive(Deserialize, prost::Message)]
pub struct CloseCursorReq {
    #[prost(int32, tag = "1")]
    pub cursor_id: i32,
}

#[derive(Serialize, prost::Message)]
pub struct CloseCursorResp {}

#[derive(Deserialize, prost::Message)]
pub struct FetchCursorReq {
    #[prost(int32, tag = "1")]
    pub cursor_id: i32,
    #[prost(uint32, tag = "2")]
    pub max_count: u32,
}

#[derive(Serialize, prost::Message)]
pub struct FetchCursorResp {
    #[prost(message, repeated, tag = "1")]
    pub entries: Vec<CursorEntry>,
    #[prost(bool, tag = "2")]
    pub done: bool,
}

#[derive(Deserialize, prost::Message)]
pub struct SequenceReq {
    #[prost(int32, tag = "1")]
    pub stream_id: i32,
    #[serde(default)]
    #[prost(string, optional, tag = "2")]
    pub sql: Option<String>,
    #[serde(default)]
    #[prost(int32, optional, tag = "3")]
    pub sql_id: Option<i32>,
    #[serde(default)]
    #[prost(uint64, optional, tag = "4")]
    pub replication_index: Option<u64>,
}

#[derive(Serialize, prost::Message)]
pub struct SequenceResp {}

#[derive(Deserialize, prost::Message)]
pub struct DescribeReq {
    #[prost(int32, tag = "1")]
    pub stream_id: i32,
    #[serde(default)]
    #[prost(string, optional, tag = "2")]
    pub sql: Option<String>,
    #[serde(default)]
    #[prost(int32, optional, tag = "3")]
    pub sql_id: Option<i32>,
    #[serde(default)]
    #[prost(uint64, optional, tag = "4")]
    pub replication_index: Option<u64>,
}

#[derive(Serialize, prost::Message)]
pub struct DescribeResp {
    #[prost(message, required, tag = "1")]
    pub result: DescribeResult,
}

#[derive(Deserialize, prost::Message)]
pub struct StoreSqlReq {
    #[prost(int32, tag = "1")]
    pub sql_id: i32,
    #[prost(string, required, tag = "2")]
    pub sql: String,
}

#[derive(Serialize, prost::Message)]
pub struct StoreSqlResp {}

#[derive(Deserialize, prost::Message)]
pub struct CloseSqlReq {
    #[prost(int32, tag = "1")]
    pub sql_id: i32,
}

#[derive(Serialize, prost::Message)]
pub struct CloseSqlResp {}

#[derive(Deserialize, prost::Message)]
pub struct GetAutocommitReq {
    #[prost(int32, tag = "1")]
    pub stream_id: i32,
}

#[derive(Serialize, prost::Message)]
pub struct GetAutocommitResp {
    #[prost(bool, required, tag = "1")]
    pub is_autocommit: bool,
}
