#[non_exhaustive]
#[derive(Debug, Clone)]
pub enum Error {
    LibError(std::ffi::c_int),
    Bug(&'static str),
}

pub type Result<T> = std::result::Result<T, Error>;
