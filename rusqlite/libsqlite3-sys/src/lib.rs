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

pub const SQLITE_CONFIG_LOG  : c_int = 16;
pub const SQLITE_UTF8  : c_int = 1;
pub const SQLITE_DETERMINISTIC  : c_int = 0x800;
