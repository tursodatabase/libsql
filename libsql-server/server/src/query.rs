use rusqlite::types::Value;

pub type QueryResult = Result<QueryResponse, QueryError>;

#[derive(Debug)]
pub enum QueryResponse {
    Ack,
    ResultSet(Vec<(String, Option<String>)>, Vec<Vec<Value>>),
}

#[derive(Debug)]
pub enum Query {
    SimpleQuery(String),
    Disconnect,
}

#[derive(Debug, Clone)]
pub struct QueryError {
    pub code: ErrorCode,
    pub msg: String,
}

impl QueryError {
    pub fn new(code: ErrorCode, msg: impl ToString) -> Self {
        Self {
            code,
            msg: msg.to_string(),
        }
    }
}

impl From<rusqlite::Error> for QueryError {
    fn from(other: rusqlite::Error) -> Self {
        Self::new(ErrorCode::SQLError, other)
    }
}

#[derive(Debug, Clone)]
pub enum ErrorCode {
    SQLError,
    TxBusy,
    TxTimeout,
    Internal,
}
