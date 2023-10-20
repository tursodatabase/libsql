use crate::namespace::NamespaceName;

#[derive(Debug, thiserror::Error)]
pub enum ReplicationError {
    #[error("Primary has incompatible log")]
    LogIncompatible,
    #[error("{0}")]
    Other(#[from] anyhow::Error),
    #[error("namespace {0} doesn't exist")]
    NamespaceDoesntExist(NamespaceName),
    #[error("Failed to commit current replication index")]
    FailedToCommit(std::io::Error),
    #[error("Rpc error: {0}")]
    Rpc(tonic::Status),
    #[error("Received invalid frame")]
    InvalidFrame,
}
