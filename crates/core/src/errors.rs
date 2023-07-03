#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Failed to connect to database: `{0}`")]
    ConnectionFailed(String),
    #[error("Failed to execute query: `{0}`")]
    QueryFailed(String),
}

pub fn sqlite_error_message(raw: *mut libsql_sys::sqlite3) -> String {
    let error = unsafe { libsql_sys::sqlite3_errmsg(raw) };
    let error = unsafe { std::ffi::CStr::from_ptr(error) };
    let error = match error.to_str() {
        Ok(error) => error,
        Err(_) => "N/A",
    };
    format!("{}", error)
}
