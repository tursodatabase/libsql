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

pub type Result<T> = std::result::Result<T, Error>;
