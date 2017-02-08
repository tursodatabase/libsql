// bindgen.rs was created with bindgen 0.15.0 against sqlite3 3.8.10

#![allow(non_snake_case)]

extern crate libc;

pub use self::bindgen::*;
pub use self::error::*;

use std::mem;
use libc::c_int;

mod bindgen;
mod error;

// SQLite datatype constants.
pub const SQLITE_INTEGER : c_int = 1;
pub const SQLITE_FLOAT   : c_int = 2;
pub const SQLITE_TEXT    : c_int = 3;
pub const SQLITE_BLOB    : c_int = 4;
pub const SQLITE_NULL    : c_int = 5;

pub fn SQLITE_STATIC() -> sqlite3_destructor_type {
    Some(unsafe { mem::transmute(0isize) })
}

pub fn SQLITE_TRANSIENT() -> sqlite3_destructor_type {
    Some(unsafe { mem::transmute(-1isize) })
}

pub const SQLITE_CONFIG_LOG: c_int = 16;
pub const SQLITE_UTF8: c_int = 1;
pub const SQLITE_DETERMINISTIC: c_int = 0x800;

/// Run-Time Limit Categories
#[repr(C)]
pub enum Limit {
    /// The maximum size of any string or BLOB or table row, in bytes.
    SQLITE_LIMIT_LENGTH = 0,
    /// The maximum length of an SQL statement, in bytes.
    SQLITE_LIMIT_SQL_LENGTH = 1,
    /// The maximum number of columns in a table definition or in the result set of a SELECT
    /// or the maximum number of columns in an index or in an ORDER BY or GROUP BY clause.
    SQLITE_LIMIT_COLUMN = 2,
    /// The maximum depth of the parse tree on any expression.
    SQLITE_LIMIT_EXPR_DEPTH = 3,
    /// The maximum number of terms in a compound SELECT statement.
    SQLITE_LIMIT_COMPOUND_SELECT = 4,
    /// The maximum number of instructions in a virtual machine program used to implement an SQL statement.
    SQLITE_LIMIT_VDBE_OP = 5,
    /// The maximum number of arguments on a function.
    SQLITE_LIMIT_FUNCTION_ARG = 6,
    /// The maximum number of attached databases.
    SQLITE_LIMIT_ATTACHED = 7,
    /// The maximum length of the pattern argument to the LIKE or GLOB operators.
    SQLITE_LIMIT_LIKE_PATTERN_LENGTH = 8,
    /// The maximum index number of any parameter in an SQL statement.
    SQLITE_LIMIT_VARIABLE_NUMBER = 9,
    /// The maximum depth of recursion for triggers.
    SQLITE_LIMIT_TRIGGER_DEPTH = 10,
    /// The maximum number of auxiliary worker threads that a single prepared statement may start.
    SQLITE_LIMIT_WORKER_THREADS = 11,
}
