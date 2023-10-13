#[derive(Debug, thiserror::Error)]
pub enum ReplicationError {
    #[error("Primary has incompatible log")]
    LogIncompatible,
    #[error("{0}")]
    Other(#[from] anyhow::Error),
}
