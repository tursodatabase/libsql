use std::io;

use pgwire::error::{ErrorInfo, PgWireError};

use crate::coordinator::query::{ErrorCode, QueryError};

pub mod authenticator;
mod proto;
pub mod service;

impl From<QueryError> for PgWireError {
    fn from(other: QueryError) -> Self {
        match other.code {
            ErrorCode::SQLError => PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                other.msg,
            ))),
            ErrorCode::TxBusy => {
                PgWireError::IoError(io::Error::new(io::ErrorKind::WouldBlock, other.msg))
            }
            ErrorCode::TxTimeout => PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                other.msg,
            ))),
            ErrorCode::Internal => {
                PgWireError::IoError(io::Error::new(io::ErrorKind::Other, other.msg))
            }
        }
    }
}
