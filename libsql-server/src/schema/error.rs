use axum::response::IntoResponse;
use hyper::StatusCode;

use crate::{error::ResponseError, namespace::NamespaceName};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to register migration job: {0}")]
    Registration(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("migration scheduler exited")]
    SchedulerExited,
    #[error("corrupted job status: {0}")]
    CorruptedJobStatus(serde_json::Error),
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("`{0}` is not a schema database")]
    NotASchema(NamespaceName),
    #[error("schema `{0}` doesn't exist")]
    SchemaDoesntExist(NamespaceName),
    #[error("A migration job is already in progress for `{0}`")]
    MigrationJobAlreadyInProgress(NamespaceName),
    #[error("An error occured executing the migration at step {0}: {1}")]
    MigrationError(usize, String),
    #[error("migration is invalid: it contains transaction items (BEGIN, COMMIT, SAVEPOINT...) which are not allowed. The migration is already run within a transaction")]
    MigrationContainsTransactionStatements,
}

impl ResponseError for Error {}

impl IntoResponse for &Error {
    fn into_response(self) -> axum::response::Response {
        match self {
            // should that really be a bad request?
            Error::MigrationError { .. } => self.format_err(StatusCode::BAD_REQUEST),
            Error::MigrationContainsTransactionStatements { .. } => {
                self.format_err(StatusCode::BAD_REQUEST)
            }
            _ => self.format_err(StatusCode::INTERNAL_SERVER_ERROR),
        }
    }
}
