use std::io;

use pgwire::error::{ErrorInfo, PgWireError};

use crate::error::Error;

pub mod authenticator;
mod proto;
pub mod service;

impl From<Error> for PgWireError {
    fn from(other: Error) -> Self {
        match other {
            Error::LibSqlInvalidQueryParams(_) => PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                other.to_string(),
            ))),
            Error::LibSqlTxBusy => {
                PgWireError::IoError(io::Error::new(io::ErrorKind::WouldBlock, other.to_string()))
            }
            Error::LibSqlTxTimeout(_) => PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                other.to_string(),
            ))),
            _ => PgWireError::IoError(io::Error::new(io::ErrorKind::Other, other.to_string())),
        }
    }
}
