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
    Execute(StreamExecuteReq),
    Batch(StreamBatchReq),
    Sequence(StreamSequenceReq),
    Describe(StreamDescribeReq),
    StoreSql(StreamStoreSqlReq),
    CloseSql(StreamCloseSqlReq),
    GetAutocommit,
}

#[derive(Serialize, Debug)]
pub struct StreamExecuteReq {
    pub stmt: Stmt,
}

#[derive(Serialize, Debug)]
pub struct StreamBatchReq {
    pub batch: Batch,
}

#[derive(Serialize, Debug)]
pub struct StreamSequenceReq {
    pub sql: Option<String>,
    pub sql_id: Option<i32>,
}

#[derive(Serialize, Debug)]
pub struct StreamDescribeReq {
    pub sql: Option<String>,
    pub sql_id: Option<i32>,
}

#[derive(Serialize, Debug)]
pub struct StreamStoreSqlReq {
    pub sql: String,
    pub sql_id: i32,
}

#[derive(Serialize, Debug)]
pub struct StreamCloseSqlReq {
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

#[derive(Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamResponse {
    Close,
    Execute(StreamExecuteResult),
    Batch(StreamBatchResult),
    Sequence,
    Describe(StreamDescribeResult),
    StoreSql,
    CloseSql,
}

#[derive(Deserialize, Debug)]
pub struct StreamExecuteResult {
    pub result: StmtResult,
}

#[derive(Deserialize, Debug)]
pub struct StreamBatchResult {
    pub result: BatchResult,
}

#[derive(Deserialize, Debug)]
pub struct StreamDescribeResult {
    pub result: DescribeResult,
}
