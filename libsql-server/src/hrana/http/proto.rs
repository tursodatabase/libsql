//! Structures for Hrana-over-HTTP.

pub use super::super::proto::*;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, prost::Message)]
pub struct PipelineReqBody {
    #[prost(string, optional, tag = "1")]
    pub baton: Option<String>,
    #[prost(message, repeated, tag = "2")]
    pub requests: Vec<StreamRequest>,
}

#[derive(Serialize, prost::Message)]
pub struct PipelineRespBody {
    #[prost(string, optional, tag = "1")]
    pub baton: Option<String>,
    #[prost(string, optional, tag = "2")]
    pub base_url: Option<String>,
    #[prost(message, repeated, tag = "3")]
    pub results: Vec<StreamResult>,
}

#[derive(Serialize, Default, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamResult {
    #[default]
    None,
    Ok {
        response: StreamResponse,
    },
    Error {
        error: Error,
    },
}

#[derive(Deserialize, prost::Message)]
pub struct CursorReqBody {
    #[prost(string, optional, tag = "1")]
    pub baton: Option<String>,
    #[prost(message, required, tag = "2")]
    pub batch: Batch,
}

#[derive(Serialize, prost::Message)]
pub struct CursorRespBody {
    #[prost(string, optional, tag = "1")]
    pub baton: Option<String>,
    #[prost(string, optional, tag = "2")]
    pub base_url: Option<String>,
}

#[derive(Deserialize, Debug, Default)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamRequest {
    #[serde(skip_deserializing)]
    #[default]
    None,
    Close(CloseStreamReq),
    Execute(ExecuteStreamReq),
    Batch(BatchStreamReq),
    Sequence(SequenceStreamReq),
    Describe(DescribeStreamReq),
    StoreSql(StoreSqlStreamReq),
    CloseSql(CloseSqlStreamReq),
    GetAutocommit(GetAutocommitStreamReq),
}

#[derive(Serialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamResponse {
    Close(CloseStreamResp),
    Execute(ExecuteStreamResp),
    Batch(BatchStreamResp),
    Sequence(SequenceStreamResp),
    Describe(DescribeStreamResp),
    StoreSql(StoreSqlStreamResp),
    CloseSql(CloseSqlStreamResp),
    GetAutocommit(GetAutocommitStreamResp),
}

#[derive(Deserialize, prost::Message)]
pub struct CloseStreamReq {}

#[derive(Serialize, prost::Message)]
pub struct CloseStreamResp {}

#[derive(Deserialize, prost::Message)]
pub struct ExecuteStreamReq {
    #[prost(message, required, tag = "1")]
    pub stmt: Stmt,
}

#[derive(Serialize, prost::Message)]
pub struct ExecuteStreamResp {
    #[prost(message, required, tag = "1")]
    pub result: StmtResult,
}

#[derive(Deserialize, prost::Message)]
pub struct BatchStreamReq {
    #[prost(message, required, tag = "1")]
    pub batch: Batch,
}

#[derive(Serialize, prost::Message)]
pub struct BatchStreamResp {
    #[prost(message, required, tag = "1")]
    pub result: BatchResult,
}

#[derive(Deserialize, prost::Message)]
pub struct SequenceStreamReq {
    #[serde(default)]
    #[prost(string, optional, tag = "1")]
    pub sql: Option<String>,
    #[serde(default)]
    #[prost(int32, optional, tag = "2")]
    pub sql_id: Option<i32>,
    #[serde(default)]
    #[prost(uint64, optional, tag = "3")]
    pub replication_index: Option<u64>,
}

#[derive(Serialize, prost::Message)]
pub struct SequenceStreamResp {}

#[derive(Deserialize, prost::Message)]
pub struct DescribeStreamReq {
    #[serde(default)]
    #[prost(string, optional, tag = "1")]
    pub sql: Option<String>,
    #[serde(default)]
    #[prost(int32, optional, tag = "2")]
    pub sql_id: Option<i32>,
    #[serde(default)]
    #[prost(uint64, optional, tag = "3")]
    pub replication_index: Option<u64>,
}

#[derive(Serialize, prost::Message)]
pub struct DescribeStreamResp {
    #[prost(message, required, tag = "1")]
    pub result: DescribeResult,
}

#[derive(Deserialize, prost::Message)]
pub struct StoreSqlStreamReq {
    #[prost(int32, tag = "1")]
    pub sql_id: i32,
    #[prost(string, tag = "2")]
    pub sql: String,
}

#[derive(Serialize, prost::Message)]
pub struct StoreSqlStreamResp {}

#[derive(Deserialize, prost::Message)]
pub struct CloseSqlStreamReq {
    #[prost(int32, tag = "1")]
    pub sql_id: i32,
}

#[derive(Serialize, prost::Message)]
pub struct CloseSqlStreamResp {}

#[derive(Deserialize, prost::Message)]
pub struct GetAutocommitStreamReq {}

#[derive(Serialize, prost::Message)]
pub struct GetAutocommitStreamResp {
    #[prost(bool, tag = "1")]
    pub is_autocommit: bool,
}
