use std::error;
use std::fmt;
use std::os::raw::c_int;
use std::path::PathBuf;
use std::str;
use types::Type;
use {errmsg_to_string, ffi};

/// Old name for `Error`. `SqliteError` is deprecated.
#[deprecated(since = "0.6.0", note = "Use Error instead")]
pub type SqliteError = Error;

/// Enum listing possible errors from rusqlite.
#[derive(Debug)]
#[allow(enum_variant_names)]
pub enum Error {
    /// An error from an underlying SQLite call.
    SqliteFailure(ffi::Error, Option<String>),

    /// Error reported when attempting to open a connection when SQLite was
    /// configured to allow single-threaded use only.
    SqliteSingleThreadedMode,

    /// Error when the value of a particular column is requested, but it cannot
    /// be converted to the requested Rust type.
    FromSqlConversionFailure(usize, Type, Box<error::Error + Send + Sync>),

    /// Error when SQLite gives us an integral value outside the range of the
    /// requested type (e.g., trying to get the value 1000 into a `u8`).
    /// The associated `usize` is the column index,
    /// and the associated `i64` is the value returned by SQLite.
    IntegralValueOutOfRange(usize, i64),

    /// Error converting a string to UTF-8.
    Utf8Error(str::Utf8Error),

    /// Error converting a string to a C-compatible string because it contained
    /// an embedded nul.
    NulError(::std::ffi::NulError),

    /// Error when using SQL named parameters and passing a parameter name not
    /// present in the SQL.
    InvalidParameterName(String),

    /// Error converting a file path to a string.
    InvalidPath(PathBuf),

    /// Error returned when an `execute` call returns rows.
    ExecuteReturnedResults,

    /// Error when a query that was expected to return at least one row (e.g.,
    /// for `query_row`) did not return any.
    QueryReturnedNoRows,

    /// Error when the value of a particular column is requested, but the index
    /// is out of range for the statement.
    InvalidColumnIndex(usize),

    /// Error when the value of a named column is requested, but no column
    /// matches the name for the statement.
    InvalidColumnName(String),

    /// Error when the value of a particular column is requested, but the type
    /// of the result in that column cannot be converted to the requested
    /// Rust type.
    InvalidColumnType(usize, Type),

    /// Error when a query that was expected to insert one row did not insert
    /// any or insert many.
    StatementChangedRows(usize),

    /// Error returned by `functions::Context::get` when the function argument
    /// cannot be converted to the requested type.
    #[cfg(feature = "functions")]
    InvalidFunctionParameterType(usize, Type),
    /// Error returned by `vtab::Values::get` when the filter argument cannot
    /// be converted to the requested type.
    #[cfg(feature = "vtab")]
    InvalidFilterParameterType(usize, Type),

    /// An error case available for implementors of custom user functions (e.g.,
    /// `create_scalar_function`).
    #[cfg(feature = "functions")]
    #[allow(dead_code)]
    UserFunctionError(Box<error::Error + Send + Sync>),

    /// Error available for the implementors of the `ToSql` trait.
    ToSqlConversionFailure(Box<error::Error + Send + Sync>),

    /// Error when the SQL is not a `SELECT`, is not read-only.
    InvalidQuery,

    /// An error case available for implementors of custom modules (e.g.,
    /// `create_module`).
    #[cfg(feature = "vtab")]
    #[allow(dead_code)]
    ModuleError(String),
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
        match *self {
            Error::SqliteFailure(ref err, None) => err.fmt(f),
            Error::SqliteFailure(_, Some(ref s)) => write!(f, "{}", s),
            Error::SqliteSingleThreadedMode => write!(
                f,
                "SQLite was compiled or configured for single-threaded use only"
            ),
            Error::FromSqlConversionFailure(i, ref t, ref err) => write!(
                f,
                "Conversion error from type {} at index: {}, {}",
                t, i, err
            ),
            Error::IntegralValueOutOfRange(col, val) => {
                write!(f, "Integer {} out of range at index {}", val, col)
            }
            Error::Utf8Error(ref err) => err.fmt(f),
            Error::NulError(ref err) => err.fmt(f),
            Error::InvalidParameterName(ref name) => write!(f, "Invalid parameter name: {}", name),
            Error::InvalidPath(ref p) => write!(f, "Invalid path: {}", p.to_string_lossy()),
            Error::ExecuteReturnedResults => {
                write!(f, "Execute returned results - did you mean to call query?")
            }
            Error::QueryReturnedNoRows => write!(f, "Query returned no rows"),
            Error::InvalidColumnIndex(i) => write!(f, "Invalid column index: {}", i),
            Error::InvalidColumnName(ref name) => write!(f, "Invalid column name: {}", name),
            Error::InvalidColumnType(i, ref t) => {
                write!(f, "Invalid column type {} at index: {}", t, i)
            }
            Error::StatementChangedRows(i) => write!(f, "Query changed {} rows", i),

            #[cfg(feature = "functions")]
            Error::InvalidFunctionParameterType(i, ref t) => {
                write!(f, "Invalid function parameter type {} at index {}", t, i)
            }
            #[cfg(feature = "vtab")]
            Error::InvalidFilterParameterType(i, ref t) => {
                write!(f, "Invalid filter parameter type {} at index {}", t, i)
            }
            #[cfg(feature = "functions")]
            Error::UserFunctionError(ref err) => err.fmt(f),
            Error::ToSqlConversionFailure(ref err) => err.fmt(f),
            Error::InvalidQuery => write!(f, "Query is not read-only"),
            #[cfg(feature = "vtab")]
            Error::ModuleError(ref desc) => write!(f, "{}", desc),
        }
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::SqliteFailure(ref err, None) => err.description(),
            Error::SqliteFailure(_, Some(ref s)) => s,
            Error::SqliteSingleThreadedMode => {
                "SQLite was compiled or configured for single-threaded use only"
            }
            Error::FromSqlConversionFailure(_, _, ref err) => err.description(),
            Error::IntegralValueOutOfRange(_, _) => "integral value out of range of requested type",
            Error::Utf8Error(ref err) => err.description(),
            Error::InvalidParameterName(_) => "invalid parameter name",
            Error::NulError(ref err) => err.description(),
            Error::InvalidPath(_) => "invalid path",
            Error::ExecuteReturnedResults => {
                "execute returned results - did you mean to call query?"
            }
            Error::QueryReturnedNoRows => "query returned no rows",
            Error::InvalidColumnIndex(_) => "invalid column index",
            Error::InvalidColumnName(_) => "invalid column name",
            Error::InvalidColumnType(_, _) => "invalid column type",
            Error::StatementChangedRows(_) => "query inserted zero or more than one row",

            #[cfg(feature = "functions")]
            Error::InvalidFunctionParameterType(_, _) => "invalid function parameter type",
            #[cfg(feature = "vtab")]
            Error::InvalidFilterParameterType(_, _) => "invalid filter parameter type",
            #[cfg(feature = "functions")]
            Error::UserFunctionError(ref err) => err.description(),
            Error::ToSqlConversionFailure(ref err) => err.description(),
            Error::InvalidQuery => "query is not read-only",
            #[cfg(feature = "vtab")]
            Error::ModuleError(ref desc) => desc,
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            Error::SqliteFailure(ref err, _) => Some(err),
            Error::Utf8Error(ref err) => Some(err),
            Error::NulError(ref err) => Some(err),

            Error::IntegralValueOutOfRange(_, _)
            | Error::SqliteSingleThreadedMode
            | Error::InvalidParameterName(_)
            | Error::ExecuteReturnedResults
            | Error::QueryReturnedNoRows
            | Error::InvalidColumnIndex(_)
            | Error::InvalidColumnName(_)
            | Error::InvalidColumnType(_, _)
            | Error::InvalidPath(_)
            | Error::StatementChangedRows(_)
            | Error::InvalidQuery => None,

            #[cfg(feature = "functions")]
            Error::InvalidFunctionParameterType(_, _) => None,
            #[cfg(feature = "vtab")]
            Error::InvalidFilterParameterType(_, _) => None,

            #[cfg(feature = "functions")]
            Error::UserFunctionError(ref err) => Some(&**err),

            Error::FromSqlConversionFailure(_, _, ref err)
            | Error::ToSqlConversionFailure(ref err) => Some(&**err),

            #[cfg(feature = "vtab")]
            Error::ModuleError(_) => None,
        }
    }
}

// These are public but not re-exported by lib.rs, so only visible within crate.

pub fn error_from_sqlite_code(code: c_int, message: Option<String>) -> Error {
    Error::SqliteFailure(ffi::Error::new(code), message)
}

pub fn error_from_handle(db: *mut ffi::sqlite3, code: c_int) -> Error {
    let message = if db.is_null() {
        None
    } else {
        Some(unsafe { errmsg_to_string(ffi::sqlite3_errmsg(db)) })
    };
    error_from_sqlite_code(code, message)
}
