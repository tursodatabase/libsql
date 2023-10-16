#[non_exhaustive]
#[derive(Debug, Clone)]
pub enum Error {
    LibError(std::ffi::c_int),
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
        Self::LibError(e as std::ffi::c_int)
    }
}

impl From<u32> for Error {
    fn from(e: u32) -> Self {
        Self::LibError(e as std::ffi::c_int)
    }
}

pub type Result<T> = std::result::Result<T, Error>;
