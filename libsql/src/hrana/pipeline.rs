// https://github.com/libsql/sqld/blob/main/docs/HTTP_V2_SPEC.md

use super::proto::{Batch, BatchResult, DescribeResult, Error, Stmt, StmtResult};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Debug)]
pub struct ClientMsg {
    pub baton: Option<String>,
    pub requests: Vec<StreamRequest>,
}

#[derive(Deserialize, Debug)]
pub struct ServerMsg {
    pub baton: Option<String>,
    pub base_url: Option<String>,
    pub results: Vec<Response>,
}

#[derive(Serialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamRequest {
    Close,
    Execute(ExecuteStreamReq),
    Batch(BatchStreamReq),
    Sequence(SequenceStreamReq),
    Describe(DescribeStreamReq),
    StoreSql(StoreSqlStreamReq),
    CloseSql(CloseSqlStreamReq),
    GetAutocommit,
}

#[derive(Serialize, Debug)]
pub struct ExecuteStreamReq {
    pub stmt: Stmt,
}

#[derive(Serialize, Debug)]
pub struct BatchStreamReq {
    pub batch: Batch,
}

#[derive(Serialize, Debug)]
pub struct SequenceStreamReq {
    pub sql: Option<String>,
    pub sql_id: Option<i32>,
}

#[derive(Serialize, Debug)]
pub struct DescribeStreamReq {
    pub sql: Option<String>,
    pub sql_id: Option<i32>,
}

#[derive(Serialize, Debug)]
pub struct StoreSqlStreamReq {
    pub sql: String,
    pub sql_id: i32,
}

#[derive(Serialize, Debug)]
pub struct CloseSqlStreamReq {
    pub sql_id: i32,
}
#[derive(Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Response {
    Ok(StreamResponseOk),
    Error(StreamResponseError),
}

#[derive(Deserialize, Debug)]
pub struct StreamResponseOk {
    pub response: StreamResponse,
}

#[derive(Deserialize, Debug)]
pub struct StreamResponseError {
    pub error: Error,
}

#[derive(Deserialize, Debug, Default)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamResponse {
    #[default]
    Close,
    Execute(ExecuteStreamResp),
    Batch(BatchStreamResp),
    Sequence,
    Describe(DescribeStreamResp),
    StoreSql,
    CloseSql,
    GetAutocommit(GetAutocommitStreamResp),
}

#[derive(Deserialize, Debug)]
pub struct ExecuteStreamResp {
    pub result: StmtResult,
}

#[derive(Deserialize, Debug)]
pub struct DescribeStreamResp {
    pub result: DescribeResult,
}

#[derive(Deserialize, Debug)]
pub struct GetAutocommitStreamResp {
    pub is_autocommit: bool,
}

#[derive(Deserialize, Debug)]
pub struct BatchStreamResp {
    pub result: BatchResult,
}
