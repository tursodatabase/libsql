pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("error building wal index: {0}")]
    IndexError(#[from] fst::Error),
    /// The segment has changed since the connection last read, and it's now trying to upgrade
    #[error("busy snapshot")]
    BusySnapshot,
    #[error("invalid segment header checksum")]
    InvalidHeaderChecksum,
}

impl Into<libsql_sys::ffi::Error> for Error {
    fn into(self) -> libsql_sys::ffi::Error {
        let code = match self {
            Error::BusySnapshot => libsql_sys::ffi::SQLITE_BUSY_SNAPSHOT,
            Error::InvalidHeaderChecksum => libsql_sys::ffi::SQLITE_CORRUPT,
            e => {
                tracing::error!("wal error: {e}");
                libsql_sys::ffi::SQLITE_IOERR_WRITE
            }
        };

        libsql_sys::ffi::Error::new(code)
    }
}
