#![allow(non_camel_case_types)]

mod types;
mod errors;

use errors::libsql_error;
use types::{libsql_database, libsql_database_ref};

#[no_mangle]
pub unsafe extern "C" fn libsql_open(_path: *const std::ffi::c_char) -> libsql_database_ref {
    let db = libsql::Database {};
    let db = Box::leak(Box::new(libsql_database { db }));
    libsql_database_ref::from(db)
}

#[no_mangle]
pub unsafe extern "C" fn libsql_close(db: libsql_database_ref) {
    if db.is_null() {
        return;
    }
    let _ = unsafe { Box::from_raw(db.get_ref_mut()) };
}

#[no_mangle]
pub unsafe extern "C" fn libsql_exec(_db: libsql_database_ref, _sql: *const std::ffi::c_char) -> i32{
    libsql_error::LIBSQL_ERROR as i32
}
