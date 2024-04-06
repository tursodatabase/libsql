#[derive(Debug)]
pub enum Error {
    /// The log has since the connection last read, and it's now trying to upgrade
    BusySnapshot,
}

impl Into<libsql_sys::ffi::Error> for Error {
    fn into(self) -> libsql_sys::ffi::Error {
        let code = match self {
            Error::BusySnapshot => libsql_sys::ffi::SQLITE_BUSY_SNAPSHOT,
        };

        libsql_sys::ffi::Error::new(code)
    }
}
