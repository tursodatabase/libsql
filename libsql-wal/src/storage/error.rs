#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("an error occured while storing a segment: {0}")]
    Store(String),
    #[error("error compacting segment: {0}")]
    Compact(#[from] crate::error::Error),
    #[error("frame not {0} found")]
    FrameNotFound(u64),
    #[error("unhandled storage error: {error}, in {context}")]
    UnhandledStorageError {
        error: Box<dyn std::error::Error + Send + Sync + 'static>,
        context: String,
    },
}

impl Error {
    pub(crate) fn unhandled(
        e: impl std::error::Error + Send + Sync + 'static,
        ctx: impl Into<String>,
    ) -> Self {
        Self::UnhandledStorageError {
            error: Box::new(e),
            context: ctx.into(),
        }
    }
}
