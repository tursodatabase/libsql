use std::error;
use std::fmt;
use std::path::PathBuf;
use std::str;
use libc::c_int;
use {ffi, errmsg_to_string};

/// Old name for `Error`. `SqliteError` is deprecated.
pub type SqliteError = Error;

/// Enum listing possible errors from rusqlite.
#[derive(Debug)]
pub enum Error {
    /// An error from an underlying SQLite call.
    SqliteFailure(ffi::Error, Option<String>),

    /// Error reported when attempting to open a connection when SQLite was configured to
    /// allow single-threaded use only.
    SqliteSingleThreadedMode,

    /// An error case available for implementors of the `FromSql` trait.
    FromSqlConversionFailure(Box<error::Error + Send + Sync>),

    /// Error converting a string to UTF-8.
    Utf8Error(str::Utf8Error),

    /// Error converting a string to a C-compatible string because it contained an embedded nul.
    NulError(::std::ffi::NulError),

    /// Error when using SQL named parameters and passing a parameter name not present in the SQL.
    InvalidParameterName(String),

    /// Error converting a file path to a string.
    InvalidPath(PathBuf),

    /// Error returned when an `execute` call returns rowss.
    ExecuteReturnedResults,

    /// Error when a query that was expected to return at least one row (e.g., for `query_row`)
    /// did not return any.
    QueryReturnedNoRows,

    /// Error when trying to access a `Row` after stepping past it. See the discussion on
    /// the `Rows` type for more details.
    GetFromStaleRow,

    /// Error when the value of a particular column is requested, but the index is out of range
    /// for the statement.
    InvalidColumnIndex(c_int),

    /// Error when the value of a named column is requested, but no column matches the name
    /// for the statement.
    InvalidColumnName(String),

    /// Error when the value of a particular column is requested, but the type of the result in
    /// that column cannot be converted to the requested Rust type.
    InvalidColumnType,

    /// Error returned by `functions::Context::get` when the function argument cannot be converted
    /// to the requested type.
    #[cfg(feature = "functions")]
    InvalidFunctionParameterType,

    /// An error case available for implementors of custom user functions (e.g.,
    /// `create_scalar_function`).
    #[cfg(feature = "functions")]
    #[allow(dead_code)]
    UserFunctionError(Box<error::Error + Send + Sync>),
}

impl From<str::Utf8Error> for Error {
    fn from(err: str::Utf8Error) -> Error {
        Error::Utf8Error(err)
    }
}

impl From<::std::ffi::NulError> for Error {
    fn from(err: ::std::ffi::NulError) -> Error {
        Error::NulError(err)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &Error::SqliteFailure(ref err, None) => err.fmt(f),
            &Error::SqliteFailure(_, Some(ref s)) => write!(f, "{}", s),
            &Error::SqliteSingleThreadedMode => write!(f, "SQLite was compiled or configured for single-threaded use only"),
            &Error::FromSqlConversionFailure(ref err) => err.fmt(f),
            &Error::Utf8Error(ref err) => err.fmt(f),
            &Error::NulError(ref err) => err.fmt(f),
            &Error::InvalidParameterName(ref name) => write!(f, "Invalid parameter name: {}", name),
            &Error::InvalidPath(ref p) => write!(f, "Invalid path: {}", p.to_string_lossy()),
            &Error::ExecuteReturnedResults => write!(f, "Execute returned results - did you mean to call query?"),
            &Error::QueryReturnedNoRows => write!(f, "Query returned no rows"),
            &Error::GetFromStaleRow => write!(f, "Attempted to get a value from a stale row"),
            &Error::InvalidColumnIndex(i) => write!(f, "Invalid column index: {}", i),
            &Error::InvalidColumnName(ref name) => write!(f, "Invalid column name: {}", name),
            &Error::InvalidColumnType => write!(f, "Invalid column type"),

            #[cfg(feature = "functions")]
            &Error::InvalidFunctionParameterType => write!(f, "Invalid function parameter type"),
            #[cfg(feature = "functions")]
            &Error::UserFunctionError(ref err) => err.fmt(f),
        }
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match self {
            &Error::SqliteFailure(ref err, None) => err.description(),
            &Error::SqliteFailure(_, Some(ref s)) => s,
            &Error::SqliteSingleThreadedMode => "SQLite was compiled or configured for single-threaded use only",
            &Error::FromSqlConversionFailure(ref err) => err.description(),
            &Error::Utf8Error(ref err) => err.description(),
            &Error::InvalidParameterName(_) => "invalid parameter name",
            &Error::NulError(ref err) => err.description(),
            &Error::InvalidPath(_) => "invalid path",
            &Error::ExecuteReturnedResults => "execute returned results - did you mean to call query?",
            &Error::QueryReturnedNoRows => "query returned no rows",
            &Error::GetFromStaleRow => "attempted to get a value from a stale row",
            &Error::InvalidColumnIndex(_) => "invalid column index",
            &Error::InvalidColumnName(_) => "invalid column name",
            &Error::InvalidColumnType => "invalid column type",

            #[cfg(feature = "functions")]
            &Error::InvalidFunctionParameterType => "invalid function parameter type",
            #[cfg(feature = "functions")]
            &Error::UserFunctionError(ref err) => err.description(),
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match self {
            &Error::SqliteFailure(ref err, _) => Some(err),
            &Error::SqliteSingleThreadedMode => None,
            &Error::FromSqlConversionFailure(ref err) => Some(&**err),
            &Error::Utf8Error(ref err) => Some(err),
            &Error::NulError(ref err) => Some(err),
            &Error::InvalidParameterName(_) => None,
            &Error::InvalidPath(_) => None,
            &Error::ExecuteReturnedResults => None,
            &Error::QueryReturnedNoRows => None,
            &Error::GetFromStaleRow => None,
            &Error::InvalidColumnIndex(_) => None,
            &Error::InvalidColumnName(_) => None,
            &Error::InvalidColumnType => None,

            #[cfg(feature = "functions")]
            &Error::InvalidFunctionParameterType => None,
            #[cfg(feature = "functions")]
            &Error::UserFunctionError(ref err) => Some(&**err),
        }
    }
}

// These are public but not re-exported by lib.rs, so only visible within crate.

pub fn error_from_sqlite_code(code: c_int, message: Option<String>) -> Error {
    Error::SqliteFailure(ffi::Error::new(code), message)
}

pub fn error_from_handle(db: *mut ffi::Struct_sqlite3, code: c_int) -> Error {
    let message = if db.is_null() {
        None
    } else {
        Some(unsafe { errmsg_to_string(ffi::sqlite3_errmsg(db)) })
    };
    error_from_sqlite_code(code, message)
}
