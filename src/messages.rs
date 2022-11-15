use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum ErrorCode {
    SQLError,
    TxBusy,
    TxTimeout,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Message {
    Execute(String),
    ResultSet(Vec<String>),
    Error(ErrorCode, String),
}
