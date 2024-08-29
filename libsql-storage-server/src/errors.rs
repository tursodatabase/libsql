use libsql_storage::rpc;
use prost::Message;
use tonic::{Code, Status};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Write conflict")]
    WriteConflict,
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl Error {
    fn code(&self) -> rpc::ErrorCode {
        match self {
            Error::WriteConflict => rpc::ErrorCode::WriteConflict,
            Error::Other(_) => rpc::ErrorCode::InternalError,
        }
    }
}

impl From<Error> for Status {
    fn from(error: Error) -> Self {
        let status_code = match error.code() {
            rpc::ErrorCode::InternalError => Code::Internal,
            rpc::ErrorCode::WriteConflict => Code::Aborted,
        };
        let details = rpc::ErrorDetails {
            message: error.to_string(),
            code: error.code() as i32,
        };

        let mut details_buf = Vec::new();
        if let Err(e) = details.encode(&mut details_buf) {
            Status::new(
                Code::Internal,
                format!("failed to encode error details: {}", e),
            )
        } else {
            Status::with_details(status_code, error.to_string(), details_buf.into())
        }
    }
}
