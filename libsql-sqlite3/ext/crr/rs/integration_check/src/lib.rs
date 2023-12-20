#![no_std]
extern crate alloc;
mod t;
use alloc::ffi::CString;
pub use crsql_bundle;
use libc_print::std_name::println;

use core::ffi::c_char;
use sqlite::{Connection, ManagedConnection, ResultCode};
use sqlite_nostd as sqlite;

/**
 * Tests in a main crate because ubuntu is seriously fucked
 * and can't find `sqlite3_malloc` when compiling it as integration tests.
 */
#[no_mangle]
pub extern "C" fn crsql_integration_check() {
    println!("Running automigrate");
    t::automigrate::run_suite().expect("automigrate suite");
    println!("Running backfill");
    t::backfill::run_suite().expect("backfill suite");
    println!("Running fract");
    t::fract::run_suite();
    println!("Running pack_columns");
    t::pack_columns::run_suite().expect("pack columns suite");
    println!("Running pk_only_tables");
    t::pk_only_tables::run_suite();
    println!("Running sync_bit_honored");
    t::sync_bit_honored::run_suite().expect("sync bit honored suite");
    println!("Running tableinfo");
    t::tableinfo::run_suite();
    println!("Running tear_down");
    t::teardown::run_suite().expect("tear down suite");
    println!("Running cl_set_vtab");
    t::test_cl_set_vtab::run_suite().expect("test cl set vtab suite");
    println!("Running db_version");
    t::test_db_version::run_suite().expect("test db version suite");
}

pub fn opendb() -> Result<CRConnection, ResultCode> {
    let connection = sqlite::open(sqlite::strlit!(":memory:"))?;
    // connection.enable_load_extension(true)?;
    // connection.load_extension("../../dbg/crsqlite", None)?;
    Ok(CRConnection { db: connection })
}

pub fn opendb_file(f: &str) -> Result<CRConnection, ResultCode> {
    let f = CString::new(f)?;
    let connection = sqlite::open(f.as_ptr())?;
    // connection.enable_load_extension(true)?;
    // connection.load_extension("../../dbg/crsqlite", None)?;
    Ok(CRConnection { db: connection })
}

pub struct CRConnection {
    pub db: ManagedConnection,
}

impl Drop for CRConnection {
    fn drop(&mut self) {
        if let Err(_) = self.db.exec_safe("SELECT crsql_finalize()") {
            panic!("Failed to finalize cr sql statements");
        }
    }
}
