use axum::response::IntoResponse;
use hyper::StatusCode;

use crate::{error::ResponseError, namespace::NamespaceName};

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

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
    #[error("an error occured while backing up the meta store")]
    MetaStoreBackupFailure,
    #[error("Failed to load namespace: {0}")]
    NamespaceLoad(BoxError),
    #[error("Failed to connect to namespace `{0}`: {1}")]
    FailedToConnect(NamespaceName, BoxError),
    #[error("Failed to step the job to `DryRunSuccess`")]
    CantStepJobDryRunSuccess,
    #[error("failed to backup namespace {0}: {1}")]
    NamespaceBackupFailure(NamespaceName, BoxError),
    #[error("migration dry run failed: {0}")]
    DryRunFailure(String),
    #[error("migration failed: {0}")]
    MigrationFailure(String),
    #[error("Error executing migration: {0}")]
    MigrationExecuteError(Box<crate::Error>),
    #[error("Interactive transactions are not allowed against a schema")]
    InteractiveTxnNotAllowed,
    #[error("Connection left in transaction state")]
    ConnectionInTxnState,
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
            Error::MigrationExecuteError(e) => e.as_ref().into_response(),
            _ => self.format_err(StatusCode::INTERNAL_SERVER_ERROR),
        }
    }
}
