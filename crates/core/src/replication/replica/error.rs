#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ReplicationError {
    // #[error("Replica is ahead of primary")]
    // Lagging,
    // #[error("Trying to replicate incompatible databases")]
    // DbIncompatible,
    #[error("{0}")]
    Other(#[from] anyhow::Error),
}
