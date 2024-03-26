use axum::response::IntoResponse;
use hyper::StatusCode;
use tonic::metadata::errors::InvalidMetadataValueBytes;

use crate::{
    auth::AuthError,
    namespace::{ForkError, NamespaceName},
    query_result_builder::QueryResultBuilderError,
};

#[allow(clippy::enum_variant_names)]
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("LibSQL failed to bind provided query parameters: `{0}`")]
    LibSqlInvalidQueryParams(anyhow::Error),
    #[error("Transaction timed-out")]
    LibSqlTxTimeout,
    #[error("Server can't handle additional transactions")]
    LibSqlTxBusy,
    #[error(transparent)]
    IOError(std::io::Error),
    #[error(transparent)]
    RusqliteError(#[from] rusqlite::Error),
    #[error("{0}")]
    RusqliteErrorExtended(rusqlite::Error, i32),
    #[error("Failed to execute query via RPC. Error code: {}, message: {}", .0.code, .0.message)]
    RpcQueryError(crate::rpc::proxy::rpc::Error),
    #[error("Failed to execute queries via RPC protocol: `{0}`")]
    RpcQueryExecutionError(#[from] tonic::Status),
    #[error("Database value error: `{0}`")]
    DbValueError(String),
    // Dedicated for most generic internal errors. Please use it sparingly.
    // Consider creating a dedicate enum value for your error.
    #[error("Internal Error: `{0}`")]
    Internal(String),
    #[error("Invalid batch step: {0}")]
    InvalidBatchStep(usize),
    #[error("Not authorized to execute query: {0}")]
    NotAuthorized(String),
    #[error("The replicator exited, instance cannot make any progress.")]
    ReplicatorExited,
    #[error("Timed out while opening database connection")]
    DbCreateTimeout,
    #[error(transparent)]
    BuilderError(#[from] QueryResultBuilderError),
    #[error("Operation was blocked{}", .0.as_ref().map(|msg| format!(": {}", msg)).unwrap_or_default())]
    Blocked(Option<String>),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("Too many concurrent requests")]
    TooManyRequests,
    #[error("Failed to parse query: `{0}`")]
    FailedToParse(String),
    #[error("Query error: `{0}`")]
    QueryError(String),
    #[error("Unauthorized: `{0}`")]
    AuthError(#[from] AuthError),
    // Catch-all error since we use anyhow in certain places
    #[error("Internal Error: `{0}`")]
    Anyhow(#[from] anyhow::Error),
    #[error("Invalid host header: `{0}`")]
    InvalidHost(String),
    #[error("Invalid path in URI: `{0}`")]
    InvalidPath(String),
    #[error("Namespace `{0}` doesn't exist")]
    NamespaceDoesntExist(String),
    #[error("Namespace `{0}` already exists")]
    NamespaceAlreadyExist(String),
    #[error("Invalid namespace")]
    InvalidNamespace,
    #[error("Replica meta error: {0}")]
    ReplicaMetaError(#[from] libsql_replication::meta::Error),
    #[error("Replicator error: {0}")]
    ReplicatorError(#[from] libsql_replication::replicator::Error),
    #[error("Failed to connect to primary")]
    PrimaryConnectionTimeout,
    #[error("Error while loading dump: {0}")]
    LoadDumpError(#[from] LoadDumpError),
    #[error("Unable to convert metadata value: `{0}`")]
    InvalidMetadataBytes(#[from] InvalidMetadataValueBytes),
    #[error("Cannot call parametrized restore over replica")]
    ReplicaRestoreError,
    #[error("Cannot load from a dump if a database already exists.")]
    LoadDumpExistingDb,
    #[error("Cannot restore database when conflicting params were provided")]
    ConflictingRestoreParameters,
    #[error("Failed to fork database: {0}")]
    Fork(#[from] ForkError),
    #[error("Fatal replication error")]
    FatalReplicationError,
    #[error("Connection with primary broken")]
    PrimaryStreamDisconnect,
    #[error("Proxy protocal misuse")]
    PrimaryStreamMisuse,
    #[error("Proxy request interupted")]
    PrimaryStreamInterupted,
    #[error("Wrong URL: {0}")]
    UrlParseError(#[from] url::ParseError),
    #[error("Namespace store has shutdown")]
    NamespaceStoreShutdown,
    #[error("Unable to update metastore: {0}")]
    MetaStoreUpdateFailure(Box<dyn std::error::Error + Send + Sync>),
    // This is for errors returned by moka
    #[error(transparent)]
    Ref(#[from] std::sync::Arc<Self>),
    #[error("Unable to decode protobuf: {0}")]
    ProstDecode(#[from] prost::DecodeError),
    #[error("Shared schema error: {0}")]
    SharedSchemaCreationError(String),
    #[error("Shared schema usage error: {0}")]
    SharedSchemaUsageError(String),

    #[error("migration error: {0}")]
    Migration(#[from] crate::schema::Error),
    #[error("cannot create/update/delete database config while there are pending migration on the shared schema `{0}`")]
    PendingMigrationOnSchema(NamespaceName),
    #[error("couldn't find requested migration job")]
    MigrationJobNotFound,
    #[error("cannot delete `{0}` because databases are still refering to it")]
    HasLinkedDbs(NamespaceName),
    #[error("ATTACH is not permitted in migration scripts")]
    AttachInMigration,
}

impl AsRef<Self> for Error {
    fn as_ref(&self) -> &Self {
        match self {
            Self::Ref(this) => this.as_ref(),
            _ => self,
        }
    }
}

pub trait ResponseError: std::error::Error {
    fn format_err(&self, status: StatusCode) -> axum::response::Response {
        let json = serde_json::json!({ "error": self.to_string() });
        tracing::error!("HTTP API: {}, {:?}", status, self);
        (status, axum::Json(json)).into_response()
    }
}

impl ResponseError for Error {}

impl IntoResponse for Error {
    fn into_response(self) -> axum::response::Response {
        (&self).into_response()
    }
}

impl IntoResponse for &Error {
    fn into_response(self) -> axum::response::Response {
        use Error::*;

        match self {
            FailedToParse(_) => self.format_err(StatusCode::BAD_REQUEST),
            AuthError(_) => self.format_err(StatusCode::UNAUTHORIZED),
            Anyhow(_) => self.format_err(StatusCode::INTERNAL_SERVER_ERROR),
            LibSqlInvalidQueryParams(_) => self.format_err(StatusCode::BAD_REQUEST),
            LibSqlTxTimeout => self.format_err(StatusCode::BAD_REQUEST),
            LibSqlTxBusy => self.format_err(StatusCode::TOO_MANY_REQUESTS),
            IOError(_) => self.format_err(StatusCode::INTERNAL_SERVER_ERROR),
            RusqliteError(_) => self.format_err(StatusCode::INTERNAL_SERVER_ERROR),
            RusqliteErrorExtended(_, _) => self.format_err(StatusCode::INTERNAL_SERVER_ERROR),
            RpcQueryError(_) => self.format_err(StatusCode::BAD_REQUEST),
            RpcQueryExecutionError(_) => self.format_err(StatusCode::INTERNAL_SERVER_ERROR),
            DbValueError(_) => self.format_err(StatusCode::BAD_REQUEST),
            Internal(_) => self.format_err(StatusCode::INTERNAL_SERVER_ERROR),
            InvalidBatchStep(_) => self.format_err(StatusCode::INTERNAL_SERVER_ERROR),
            NotAuthorized(_) => self.format_err(StatusCode::UNAUTHORIZED),
            ReplicatorExited => self.format_err(StatusCode::SERVICE_UNAVAILABLE),
            DbCreateTimeout => self.format_err(StatusCode::SERVICE_UNAVAILABLE),
            BuilderError(_) => self.format_err(StatusCode::INTERNAL_SERVER_ERROR),
            Blocked(_) => self.format_err(StatusCode::INTERNAL_SERVER_ERROR),
            Json(_) => self.format_err(StatusCode::INTERNAL_SERVER_ERROR),
            TooManyRequests => self.format_err(StatusCode::TOO_MANY_REQUESTS),
            QueryError(_) => self.format_err(StatusCode::BAD_REQUEST),
            InvalidHost(_) => self.format_err(StatusCode::BAD_REQUEST),
            InvalidPath(_) => self.format_err(StatusCode::BAD_REQUEST),
            NamespaceDoesntExist(_) => self.format_err(StatusCode::BAD_REQUEST),
            PrimaryConnectionTimeout => self.format_err(StatusCode::INTERNAL_SERVER_ERROR),
            NamespaceAlreadyExist(_) => self.format_err(StatusCode::BAD_REQUEST),
            InvalidNamespace => self.format_err(StatusCode::BAD_REQUEST),
            LoadDumpError(e) => e.into_response(),
            InvalidMetadataBytes(_) => self.format_err(StatusCode::INTERNAL_SERVER_ERROR),
            ReplicaRestoreError => self.format_err(StatusCode::BAD_REQUEST),
            LoadDumpExistingDb => self.format_err(StatusCode::BAD_REQUEST),
            ConflictingRestoreParameters => self.format_err(StatusCode::BAD_REQUEST),
            Fork(e) => e.into_response(),
            FatalReplicationError => self.format_err(StatusCode::INTERNAL_SERVER_ERROR),
            ReplicatorError(_) => self.format_err(StatusCode::INTERNAL_SERVER_ERROR),
            ReplicaMetaError(_) => self.format_err(StatusCode::INTERNAL_SERVER_ERROR),
            PrimaryStreamDisconnect => self.format_err(StatusCode::INTERNAL_SERVER_ERROR),
            PrimaryStreamMisuse => self.format_err(StatusCode::INTERNAL_SERVER_ERROR),
            PrimaryStreamInterupted => self.format_err(StatusCode::INTERNAL_SERVER_ERROR),
            UrlParseError(_) => self.format_err(StatusCode::BAD_REQUEST),
            NamespaceStoreShutdown => self.format_err(StatusCode::SERVICE_UNAVAILABLE),
            MetaStoreUpdateFailure(_) => self.format_err(StatusCode::INTERNAL_SERVER_ERROR),
            Ref(this) => this.as_ref().into_response(),
            ProstDecode(_) => self.format_err(StatusCode::INTERNAL_SERVER_ERROR),
            SharedSchemaCreationError(_) => self.format_err(StatusCode::BAD_REQUEST),
            SharedSchemaUsageError(_) => self.format_err(StatusCode::BAD_REQUEST),
            Migration(e) => e.into_response(),
            PendingMigrationOnSchema(_) => self.format_err(StatusCode::BAD_REQUEST),
            MigrationJobNotFound => self.format_err(StatusCode::NOT_FOUND),
            HasLinkedDbs(_) => self.format_err(StatusCode::BAD_REQUEST),
            AttachInMigration => self.format_err(StatusCode::BAD_REQUEST),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        tracing::error!("IO error reported: {:?}", value);

        Error::IOError(value)
    }
}

impl From<tokio::sync::oneshot::error::RecvError> for Error {
    fn from(inner: tokio::sync::oneshot::error::RecvError) -> Self {
        Self::Internal(format!(
            "Failed to receive response via oneshot channel: {inner}"
        ))
    }
}

impl From<bincode::Error> for Error {
    fn from(other: bincode::Error) -> Self {
        Self::Internal(other.to_string())
    }
}

macro_rules! internal_from {
    ($to:ty  => { $($from:ty,)* }) => {
        $(
            impl From<$from> for $to {
                fn from(v: $from) -> Self {
                    <$to>::Internal(v.to_string())
                }
            }
        )*

    };
}

internal_from! {
    LoadDumpError => {
        std::io::Error,
        rusqlite::Error,
        hyper::Error,
        tokio::task::JoinError,
    }
}

#[derive(Debug, thiserror::Error)]
pub enum LoadDumpError {
    #[error("Internal error: {0}")]
    Internal(String),
    #[error("Cannot load a dump on a replica")]
    ReplicaLoadDump,
    #[error("Cannot load from a dump if a database already exists")]
    LoadDumpExistingDb,
    #[error("The passed dump file path is not absolute")]
    DumpFilePathNotAbsolute,
    #[error("The passed dump file path doesn't exist")]
    DumpFileDoesntExist,
    #[error("Invalid dump url")]
    InvalidDumpUrl,
    #[error("Unsupported dump url scheme `{0}`, supported schemes are: `http`, `file`")]
    UnsupportedUrlScheme(String),
    #[error("A dump should execute within a transaction.")]
    NoTxn,
    #[error("The dump should commit the transaction.")]
    NoCommit,
}

impl ResponseError for LoadDumpError {}

impl IntoResponse for &LoadDumpError {
    fn into_response(self) -> axum::response::Response {
        use LoadDumpError::*;

        match &self {
            Internal(_) => self.format_err(StatusCode::INTERNAL_SERVER_ERROR),
            ReplicaLoadDump
            | LoadDumpExistingDb
            | InvalidDumpUrl
            | DumpFileDoesntExist
            | UnsupportedUrlScheme(_)
            | NoTxn
            | NoCommit
            | DumpFilePathNotAbsolute => self.format_err(StatusCode::BAD_REQUEST),
        }
    }
}

impl ResponseError for ForkError {}

impl IntoResponse for &ForkError {
    fn into_response(self) -> axum::response::Response {
        match self {
            ForkError::Internal(_)
            | ForkError::Io(_)
            | ForkError::LogRead(_)
            | ForkError::BackupServiceNotConfigured
            | ForkError::CreateNamespace(_) => self.format_err(StatusCode::INTERNAL_SERVER_ERROR),
            ForkError::ForkReplica => self.format_err(StatusCode::BAD_REQUEST),
        }
    }
}
