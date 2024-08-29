use std::io;

pub mod injector;
pub mod replicator;
pub mod storage;

pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error {0}")]
    IO(#[from] io::Error),
    #[error("error fetching from storage: {0}")]
    Storage(#[from] super::storage::Error),
    #[error("error fetching from current segment: {0}")]
    CurrentSegment(BoxError),
    #[error("error fetching from sealed segment list: {0}")]
    SealedSegment(BoxError),
}
