//! Structures for Hrana-over-HTTP.

pub use super::super::proto::*;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Debug)]
pub struct PipelineRequestBody {
    pub baton: Option<String>,
    pub requests: Vec<StreamRequest>,
}

#[derive(Serialize, Debug)]
pub struct PipelineResponseBody {
    pub baton: Option<String>,
    pub base_url: Option<String>,
    pub results: Vec<StreamResult>,
}

#[derive(Serialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamResult {
    Ok { response: StreamResponse },
    Error { error: Error },
}

#[derive(Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamRequest {
    Close(CloseStreamReq),
    Execute(ExecuteStreamReq),
    Batch(BatchStreamReq),
    Sequence(SequenceStreamReq),
    Describe(DescribeStreamReq),
    StoreSql(StoreSqlStreamReq),
    CloseSql(CloseSqlStreamReq),
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
}

#[derive(Deserialize, Debug)]
pub struct CloseStreamReq {}

#[derive(Serialize, Debug)]
pub struct CloseStreamResp {}

#[derive(Deserialize, Debug)]
pub struct ExecuteStreamReq {
    pub stmt: Stmt,
}

#[derive(Serialize, Debug)]
pub struct ExecuteStreamResp {
    pub result: StmtResult,
}

#[derive(Deserialize, Debug)]
pub struct BatchStreamReq {
    pub batch: Batch,
}

#[derive(Serialize, Debug)]
pub struct BatchStreamResp {
    pub result: BatchResult,
}

#[derive(Deserialize, Debug)]
pub struct SequenceStreamReq {
    #[serde(default)]
    pub sql: Option<String>,
    #[serde(default)]
    pub sql_id: Option<i32>,
}

#[derive(Serialize, Debug)]
pub struct SequenceStreamResp {}

#[derive(Deserialize, Debug)]
pub struct DescribeStreamReq {
    #[serde(default)]
    pub sql: Option<String>,
    #[serde(default)]
    pub sql_id: Option<i32>,
}

#[derive(Serialize, Debug)]
pub struct DescribeStreamResp {
    pub result: DescribeResult,
}

#[derive(Deserialize, Debug)]
pub struct StoreSqlStreamReq {
    pub sql_id: i32,
    pub sql: String,
}

#[derive(Serialize, Debug)]
pub struct StoreSqlStreamResp {}

#[derive(Deserialize, Debug)]
pub struct CloseSqlStreamReq {
    pub sql_id: i32,
}

#[derive(Serialize, Debug)]
pub struct CloseSqlStreamResp {}
