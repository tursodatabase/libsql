#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Failed to connect to database: `{0}`")]
    ConnectionFailed(String),
    #[error("Failed to prepare statement `{1}`: `{2}`")]
    PrepareFailed(std::ffi::c_int, String, String),
    #[error("Failed to fetch row: `{1}`")]
    FetchRowFailed(std::ffi::c_int, String),
    #[error("Unknown value type for column `{0}`: `{1}`")]
    UnknownColumnType(i32, i32),
    #[error("The value is NULL")]
    NullValue,
    #[error("Library misuse: `{0}`")]
    Misuse(String),
    #[error("Invalid column name: {0}")]
    InvalidColumnName(String),
    #[error("libSQL error {0}: `{1}`")]
    LibError(std::ffi::c_int, String),
    #[error("Query returned no rows")]
    QueryReturnedNoRows,
    #[error("Execute returned rows")]
    ExecuteReturnedRows,
    #[error("unable to convert to sql: `{0}`")]
    ToSqlConversionFailure(crate::BoxError),
    #[error("Hrana: `{0}`")]
    Hrana(#[from] crate::v2::HranaError),
    #[error("Sync is not supported in databases opened in {0} mode.")]
    SyncNotSupported(String),
    #[error("Column not found: {0}")]
    ColumnNotFound(i32),
}

pub(crate) fn error_from_handle(raw: *mut libsql_sys::ffi::sqlite3) -> String {
    let errmsg = unsafe { libsql_sys::ffi::sqlite3_errmsg(raw) };
    sqlite_errmsg_to_string(errmsg)
}

pub(crate) fn extended_error_code(raw: *mut libsql_sys::ffi::sqlite3) -> std::ffi::c_int {
    unsafe { libsql_sys::ffi::sqlite3_extended_errcode(raw) }
}

pub fn error_from_code(code: i32) -> String {
    let errmsg = unsafe { libsql_sys::ffi::sqlite3_errstr(code) };
    sqlite_errmsg_to_string(errmsg)
}

pub fn sqlite_errmsg_to_string(errmsg: *const std::ffi::c_char) -> String {
    let errmsg = unsafe { std::ffi::CStr::from_ptr(errmsg) }.to_bytes();
    String::from_utf8_lossy(errmsg).to_string()
}
