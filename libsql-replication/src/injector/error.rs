pub type Result<T, E = Error> = std::result::Result<T, E>;
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("A fatal error occured injecting frames: {0}")]
    FatalInjectError(BoxError),
}
