#![allow(non_snake_case)]

extern crate libc;

pub use self::bindgen::*;

use std::mem;
use libc::c_int;

mod bindgen;

pub const SQLITE_OK        : c_int =   0;
pub const SQLITE_ERROR     : c_int =   1;
pub const SQLITE_INTERNAL  : c_int =   2;
pub const SQLITE_PERM      : c_int =   3;
pub const SQLITE_ABORT     : c_int =   4;
pub const SQLITE_BUSY      : c_int =   5;
pub const SQLITE_LOCKED    : c_int =   6;
pub const SQLITE_NOMEM     : c_int =   7;
pub const SQLITE_READONLY  : c_int =   8;
pub const SQLITE_INTERRUPT : c_int =   9;
pub const SQLITE_IOERR     : c_int =  10;
pub const SQLITE_CORRUPT   : c_int =  11;
pub const SQLITE_NOTFOUND  : c_int =  12;
pub const SQLITE_FULL      : c_int =  13;
pub const SQLITE_CANTOPEN  : c_int =  14;
pub const SQLITE_PROTOCOL  : c_int =  15;
pub const SQLITE_EMPTY     : c_int =  16;
pub const SQLITE_SCHEMA    : c_int =  17;
pub const SQLITE_TOOBIG    : c_int =  18;
pub const SQLITE_CONSTRAINT: c_int =  19;
pub const SQLITE_MISMATCH  : c_int =  20;
pub const SQLITE_MISUSE    : c_int =  21;
pub const SQLITE_NOLFS     : c_int =  22;
pub const SQLITE_AUTH      : c_int =  23;
pub const SQLITE_FORMAT    : c_int =  24;
pub const SQLITE_RANGE     : c_int =  25;
pub const SQLITE_NOTADB    : c_int =  26;
pub const SQLITE_NOTICE    : c_int =  27;
pub const SQLITE_WARNING   : c_int =  28;
pub const SQLITE_ROW       : c_int = 100;
pub const SQLITE_DONE      : c_int = 101;

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

pub fn code_to_str(code: c_int) -> &'static str {
    match code {
        SQLITE_OK        => "Successful result",
        SQLITE_ERROR     => "SQL error or missing database",
        SQLITE_INTERNAL  => "Internal logic error in SQLite",
        SQLITE_PERM      => "Access permission denied",
        SQLITE_ABORT     => "Callback routine requested an abort",
        SQLITE_BUSY      => "The database file is locked",
        SQLITE_LOCKED    => "A table in the database is locked",
        SQLITE_NOMEM     => "A malloc() failed",
        SQLITE_READONLY  => "Attempt to write a readonly database",
        SQLITE_INTERRUPT => "Operation terminated by sqlite3_interrupt()",
        SQLITE_IOERR     => "Some kind of disk I/O error occurred",
        SQLITE_CORRUPT   => "The database disk image is malformed",
        SQLITE_NOTFOUND  => "Unknown opcode in sqlite3_file_control()",
        SQLITE_FULL      => "Insertion failed because database is full",
        SQLITE_CANTOPEN  => "Unable to open the database file",
        SQLITE_PROTOCOL  => "Database lock protocol error",
        SQLITE_EMPTY     => "Database is empty",
        SQLITE_SCHEMA    => "The database schema changed",
        SQLITE_TOOBIG    => "String or BLOB exceeds size limit",
        SQLITE_CONSTRAINT=> "Abort due to constraint violation",
        SQLITE_MISMATCH  => "Data type mismatch",
        SQLITE_MISUSE    => "Library used incorrectly",
        SQLITE_NOLFS     => "Uses OS features not supported on host",
        SQLITE_AUTH      => "Authorization denied",
        SQLITE_FORMAT    => "Auxiliary database format error",
        SQLITE_RANGE     => "2nd parameter to sqlite3_bind out of range",
        SQLITE_NOTADB    => "File opened that is not a database file",
        SQLITE_NOTICE    => "Notifications from sqlite3_log()",
        SQLITE_WARNING   => "Warnings from sqlite3_log()",
        SQLITE_ROW       => "sqlite3_step() has another row ready",
        SQLITE_DONE      => "sqlite3_step() has finished executing",
        _                => "Unknown error code",
    }
}

pub const SQLITE_UTF8  : c_int = 1;
pub const SQLITE_DETERMINISTIC  : c_int = 0x800;
