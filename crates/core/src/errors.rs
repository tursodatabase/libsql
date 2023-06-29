#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Failed to connect to database: `{0}`")]
    ConnectionFailed(String),
    #[error("Failed to execute query: `{0}`")]
    QueryFailed(String),
}
