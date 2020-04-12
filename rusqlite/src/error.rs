use crate::types::FromSqlError;
use crate::types::Type;
use crate::{errmsg_to_string, ffi};
use std::error;
use std::fmt;
use std::os::raw::c_int;
use std::path::PathBuf;
use std::str;

/// Enum listing possible errors from rusqlite.
#[derive(Debug)]
#[allow(clippy::enum_variant_names)]
#[non_exhaustive]
pub enum Error {
    /// An error from an underlying SQLite call.
    SqliteFailure(ffi::Error, Option<String>),

    /// Error reported when attempting to open a connection when SQLite was
    /// configured to allow single-threaded use only.
    SqliteSingleThreadedMode,

    /// Error when the value of a particular column is requested, but it cannot
    /// be converted to the requested Rust type.
    FromSqlConversionFailure(usize, Type, Box<dyn error::Error + Send + Sync + 'static>),

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
    InvalidColumnType(usize, String, Type),

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
    UserFunctionError(Box<dyn error::Error + Send + Sync + 'static>),

    /// Error available for the implementors of the `ToSql` trait.
    ToSqlConversionFailure(Box<dyn error::Error + Send + Sync + 'static>),

    /// Error when the SQL is not a `SELECT`, is not read-only.
    InvalidQuery,

    /// An error case available for implementors of custom modules (e.g.,
    /// `create_module`).
    #[cfg(feature = "vtab")]
    #[allow(dead_code)]
    ModuleError(String),

    #[cfg(feature = "functions")]
    UnwindingPanic,

    /// An error returned when `Context::get_aux` attempts to retrieve data
    /// of a different type than what had been stored using `Context::set_aux`.
    #[cfg(feature = "functions")]
    GetAuxWrongType,

    /// Error when the SQL contains multiple statements.
    MultipleStatement,
    /// Error when the number of bound parameters does not match the number of
    /// parameters in the query. The first `usize` is how many parameters were
    /// given, the 2nd is how many were expected.
    InvalidParameterCount(usize, usize),
}

impl PartialEq for Error {
    fn eq(&self, other: &Error) -> bool {
        match (self, other) {
            (Error::SqliteFailure(e1, s1), Error::SqliteFailure(e2, s2)) => e1 == e2 && s1 == s2,
            (Error::SqliteSingleThreadedMode, Error::SqliteSingleThreadedMode) => true,
            (Error::IntegralValueOutOfRange(i1, n1), Error::IntegralValueOutOfRange(i2, n2)) => {
                i1 == i2 && n1 == n2
            }
            (Error::Utf8Error(e1), Error::Utf8Error(e2)) => e1 == e2,
            (Error::NulError(e1), Error::NulError(e2)) => e1 == e2,
            (Error::InvalidParameterName(n1), Error::InvalidParameterName(n2)) => n1 == n2,
            (Error::InvalidPath(p1), Error::InvalidPath(p2)) => p1 == p2,
            (Error::ExecuteReturnedResults, Error::ExecuteReturnedResults) => true,
            (Error::QueryReturnedNoRows, Error::QueryReturnedNoRows) => true,
            (Error::InvalidColumnIndex(i1), Error::InvalidColumnIndex(i2)) => i1 == i2,
            (Error::InvalidColumnName(n1), Error::InvalidColumnName(n2)) => n1 == n2,
            (Error::InvalidColumnType(i1, n1, t1), Error::InvalidColumnType(i2, n2, t2)) => {
                i1 == i2 && t1 == t2 && n1 == n2
            }
            (Error::StatementChangedRows(n1), Error::StatementChangedRows(n2)) => n1 == n2,
            #[cfg(feature = "functions")]
            (
                Error::InvalidFunctionParameterType(i1, t1),
                Error::InvalidFunctionParameterType(i2, t2),
            ) => i1 == i2 && t1 == t2,
            #[cfg(feature = "vtab")]
            (
                Error::InvalidFilterParameterType(i1, t1),
                Error::InvalidFilterParameterType(i2, t2),
            ) => i1 == i2 && t1 == t2,
            (Error::InvalidQuery, Error::InvalidQuery) => true,
            #[cfg(feature = "vtab")]
            (Error::ModuleError(s1), Error::ModuleError(s2)) => s1 == s2,
            #[cfg(feature = "functions")]
            (Error::UnwindingPanic, Error::UnwindingPanic) => true,
            #[cfg(feature = "functions")]
            (Error::GetAuxWrongType, Error::GetAuxWrongType) => true,
            (Error::InvalidParameterCount(i1, n1), Error::InvalidParameterCount(i2, n2)) => {
                i1 == i2 && n1 == n2
            }
            (..) => false,
        }
    }
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

const UNKNOWN_COLUMN: usize = std::usize::MAX;

/// The conversion isn't precise, but it's convenient to have it
/// to allow use of `get_raw(…).as_…()?` in callbacks that take `Error`.
impl From<FromSqlError> for Error {
    fn from(err: FromSqlError) -> Error {
        // The error type requires index and type fields, but they aren't known in this
        // context.
        match err {
            FromSqlError::OutOfRange(val) => Error::IntegralValueOutOfRange(UNKNOWN_COLUMN, val),
            #[cfg(feature = "i128_blob")]
            FromSqlError::InvalidI128Size(_) => {
                Error::FromSqlConversionFailure(UNKNOWN_COLUMN, Type::Blob, Box::new(err))
            }
            #[cfg(feature = "uuid")]
            FromSqlError::InvalidUuidSize(_) => {
                Error::FromSqlConversionFailure(UNKNOWN_COLUMN, Type::Blob, Box::new(err))
            }
            FromSqlError::Other(source) => {
                Error::FromSqlConversionFailure(UNKNOWN_COLUMN, Type::Null, source)
            }
            _ => Error::FromSqlConversionFailure(UNKNOWN_COLUMN, Type::Null, Box::new(err)),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Error::SqliteFailure(ref err, None) => err.fmt(f),
            Error::SqliteFailure(_, Some(ref s)) => write!(f, "{}", s),
            Error::SqliteSingleThreadedMode => write!(
                f,
                "SQLite was compiled or configured for single-threaded use only"
            ),
            Error::FromSqlConversionFailure(i, ref t, ref err) => {
                if i != UNKNOWN_COLUMN {
                    write!(
                        f,
                        "Conversion error from type {} at index: {}, {}",
                        t, i, err
                    )
                } else {
                    err.fmt(f)
                }
            }
            Error::IntegralValueOutOfRange(col, val) => {
                if col != UNKNOWN_COLUMN {
                    write!(f, "Integer {} out of range at index {}", val, col)
                } else {
                    write!(f, "Integer {} out of range", val)
                }
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
            Error::InvalidColumnType(i, ref name, ref t) => write!(
                f,
                "Invalid column type {} at index: {}, name: {}",
                t, i, name
            ),
            Error::InvalidParameterCount(i1, n1) => write!(
                f,
                "Wrong number of parameters passed to query. Got {}, needed {}",
                i1, n1
            ),
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
            #[cfg(feature = "functions")]
            Error::UnwindingPanic => write!(f, "unwinding panic"),
            #[cfg(feature = "functions")]
            Error::GetAuxWrongType => write!(f, "get_aux called with wrong type"),
            Error::MultipleStatement => write!(f, "Multiple statements provided"),
        }
    }
}

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match *self {
            Error::SqliteFailure(ref err, _) => Some(err),
            Error::Utf8Error(ref err) => Some(err),
            Error::NulError(ref err) => Some(err),

            Error::IntegralValueOutOfRange(..)
            | Error::SqliteSingleThreadedMode
            | Error::InvalidParameterName(_)
            | Error::ExecuteReturnedResults
            | Error::QueryReturnedNoRows
            | Error::InvalidColumnIndex(_)
            | Error::InvalidColumnName(_)
            | Error::InvalidColumnType(..)
            | Error::InvalidPath(_)
            | Error::InvalidParameterCount(..)
            | Error::StatementChangedRows(_)
            | Error::InvalidQuery
            | Error::MultipleStatement => None,

            #[cfg(feature = "functions")]
            Error::InvalidFunctionParameterType(..) => None,
            #[cfg(feature = "vtab")]
            Error::InvalidFilterParameterType(..) => None,

            #[cfg(feature = "functions")]
            Error::UserFunctionError(ref err) => Some(&**err),

            Error::FromSqlConversionFailure(_, _, ref err)
            | Error::ToSqlConversionFailure(ref err) => Some(&**err),

            #[cfg(feature = "vtab")]
            Error::ModuleError(_) => None,

            #[cfg(feature = "functions")]
            Error::UnwindingPanic => None,

            #[cfg(feature = "functions")]
            Error::GetAuxWrongType => None,
        }
    }
}

// These are public but not re-exported by lib.rs, so only visible within crate.

pub fn error_from_sqlite_code(code: c_int, message: Option<String>) -> Error {
    Error::SqliteFailure(ffi::Error::new(code), message)
}

pub unsafe fn error_from_handle(db: *mut ffi::sqlite3, code: c_int) -> Error {
    let message = if db.is_null() {
        None
    } else {
        Some(errmsg_to_string(ffi::sqlite3_errmsg(db)))
    };
    error_from_sqlite_code(code, message)
}

macro_rules! check {
    ($funcall:expr) => {{
        let rc = $funcall;
        if rc != crate::ffi::SQLITE_OK {
            return Err(crate::error::error_from_sqlite_code(rc, None).into());
        }
    }};
}
