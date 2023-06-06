use std::fmt;

pub mod batch;
pub mod http;
pub mod proto;
mod result_builder;
pub mod stmt;
pub mod ws;

#[derive(Debug, Copy, Clone, PartialOrd, Ord, PartialEq, Eq)]
pub enum Version {
    Hrana1,
    Hrana2,
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Version::Hrana1 => write!(f, "hrana1"),
            Version::Hrana2 => write!(f, "hrana2"),
        }
    }
}

/// An unrecoverable protocol error that should close the WebSocket or HTTP stream. A correct
/// client should never trigger any of these errors.
#[derive(thiserror::Error, Debug)]
pub enum ProtocolError {
    #[error("Cannot deserialize client message: {source}")]
    Deserialize { source: serde_json::Error },
    #[error("Received a binary WebSocket message, which is not supported")]
    BinaryWebSocketMessage,
    #[error("Received a request before hello message")]
    RequestBeforeHello,

    #[error("Stream {stream_id} not found")]
    StreamNotFound { stream_id: i32 },
    #[error("Stream {stream_id} already exists")]
    StreamExists { stream_id: i32 },

    #[error("Either `sql` or `sql_id` are required, but not both")]
    SqlIdAndSqlGiven,
    #[error("Either `sql` or `sql_id` are required")]
    SqlIdOrSqlNotGiven,
    #[error("SQL text {sql_id} not found")]
    SqlNotFound { sql_id: i32 },
    #[error("SQL text {sql_id} already exists")]
    SqlExists { sql_id: i32 },

    #[error("Invalid reference to step in a batch condition")]
    BatchCondBadStep,

    #[error("Received an invalid baton")]
    BatonInvalid,
    #[error("Received a baton that has already been used")]
    BatonReused,
    #[error("Stream for this baton was closed")]
    BatonStreamClosed,

    #[error("{what} is only supported in protocol version {min_version} and higher")]
    NotSupported {
        what: &'static str,
        min_version: Version,
    },

    #[error("{0}")]
    ResponseTooLarge(String),
}
