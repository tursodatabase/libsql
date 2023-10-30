#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("SQLite error: {0}")]
    Sqlite(#[from] sqld_libsql_bindings::rusqlite::Error),
    #[error("A fatal error occured injecting frames")]
    FatalInjectError,
}
