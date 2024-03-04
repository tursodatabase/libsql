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
}

impl ResponseError for Error {}

impl IntoResponse for &Error {
    fn into_response(self) -> axum::response::Response {
        self.format_err(StatusCode::INTERNAL_SERVER_ERROR)
    }
}
