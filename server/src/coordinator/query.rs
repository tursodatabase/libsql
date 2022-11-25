use crate::coordinator::scheduler::ClientId;

pub type QueryResult = Result<QueryResponse, QueryError>;

#[derive(Debug)]
pub enum QueryResponse {
    Ack,
    ResultSet(Vec<String>),
}

#[derive(Debug)]
pub struct QueryRequest {
    pub client_id: ClientId,
    pub query: Query,
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

#[derive(Debug, Clone)]
pub enum ErrorCode {
    SQLError,
    TxBusy,
    TxTimeout,
    Internal,
}
