#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Failed to connect to database: `{0}`")]
    ConnectionFailed(String),
    #[error("Failed to prepare statement `{0}`: `{1}`")]
    PrepareFailed(String, String),
    #[error("Failed to fetch row: `{0}`")]
    FetchRowFailed(String),
    #[error("Unknown value type for column `{0}`: `{1}`")]
    UnknownColumnType(i32, i32),
    #[error("The value is NULL")]
    NullValue,
    #[error("Library misuse: `{0}`")]
    Misuse(String),
    #[error("Invalid column name: {0}")]
    InvalidColumnName(String),
    #[error("libSQL error {0}")]
    LibError(std::ffi::c_int),
}

pub(crate) fn error_from_handle(raw: *mut libsql_sys::ffi::sqlite3) -> String {
    let errmsg = unsafe { libsql_sys::ffi::sqlite3_errmsg(raw) };
    sqlite_errmsg_to_string(errmsg)
}

pub fn error_from_code(code: i32) -> String {
    let errmsg = unsafe { libsql_sys::ffi::sqlite3_errstr(code) };
    sqlite_errmsg_to_string(errmsg)
}

pub fn sqlite_errmsg_to_string(errmsg: *const std::ffi::c_char) -> String {
    let errmsg = unsafe { std::ffi::CStr::from_ptr(errmsg) }.to_bytes();
    String::from_utf8_lossy(errmsg).to_string()
}
