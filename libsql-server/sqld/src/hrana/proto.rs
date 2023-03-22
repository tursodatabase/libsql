//! Messages in the Hrana protocol.
//!
//! Please consult the Hrana specification in the `docs/` directory for more information.
use crate::batch;
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
    CloseStream(OpenStreamReq),
    Execute(ExecuteReq),
    Batch(BatchReq),
}

#[derive(Serialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Response {
    OpenStream(OpenStreamResp),
    CloseStream(CloseStreamResp),
    Execute(ExecuteResp),
    Batch(BatchResp),
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
    pub stmt: batch::proto::Stmt,
}

#[derive(Serialize, Debug)]
pub struct ExecuteResp {
    pub result: batch::proto::StmtResult,
}

#[derive(Deserialize, Debug)]
pub struct BatchReq {
    pub stream_id: i32,
    pub batch: batch::proto::Batch,
}

#[derive(Serialize, Debug)]
pub struct BatchResp {
    pub result: batch::proto::BatchResult,
}

#[derive(Serialize, Debug)]
pub struct Error {
    pub message: String,
}
