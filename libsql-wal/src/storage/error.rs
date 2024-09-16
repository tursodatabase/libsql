use std::panic::Location;

use super::backend::FindSegmentReq;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("an error occured while storing a segment: {0}")]
    Store(String),
    #[error("error compacting segment: {0}")]
    Compact(#[from] crate::error::Error),
    #[error("segment not found for request {0:?}")]
    SegmentNotFound(FindSegmentReq),
    #[error("unhandled storage error: {error}, in {context}")]
    UnhandledStorageError {
        error: Box<dyn std::error::Error + Send + Sync + 'static>,
        context: String,
        loc: String,
    },
    // We may recover from this error, and rebuild the index from the data file.
    #[error("invalid index: {0}")]
    InvalidIndex(&'static str),
    #[error("Provided config is of an invalid type")]
    InvalidConfigType,
}

impl Error {
    #[track_caller]
    pub(crate) fn unhandled(
        e: impl std::error::Error + Send + Sync + 'static,
        ctx: impl Into<String>,
    ) -> Self {
        Self::UnhandledStorageError {
            error: Box::new(e),
            context: ctx.into(),
            loc: Location::caller().to_string(),
        }
    }
}
