#[non_exhaustive]
#[derive(Debug, Clone)]
pub enum Error {
    LibError(libsql_ffi::Error),
    Bug(&'static str),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::LibError(e) => write!(f, "LibError({})", e),
            Self::Bug(e) => write!(f, "Bug({})", e),
        }
    }
}

impl From<i32> for Error {
    fn from(e: i32) -> Self {
        Self::LibError(libsql_ffi::Error::new(e))
    }
}

impl From<u32> for Error {
    fn from(e: u32) -> Self {
        Self::LibError(libsql_ffi::Error::new(e as _))
    }
}

impl From<libsql_ffi::Error> for Error {
    fn from(value: libsql_ffi::Error) -> Self {
        Self::LibError(value)
    }
}

pub type Result<T> = std::result::Result<T, Error>;
