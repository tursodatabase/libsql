#![no_std]
#![feature(core_intrinsics)]
#![feature(lang_items)]

extern crate alloc;

use core::alloc::GlobalAlloc;
use core::alloc::Layout;
use core::ffi::{c_char, c_int};
use core::panic::PanicInfo;
use crsql_core;
use crsql_core::sqlite3_crsqlcore_init;
use crsql_fractindex_core::sqlite3_crsqlfractionalindex_init;
use sqlite_nostd as sqlite;
use sqlite_nostd::SQLite3Allocator;

// This must be our allocator so we can transfer ownership of memory to SQLite and have SQLite free that memory for us.
// This drastically reduces copies when passing strings and blobs back and forth between Rust and C.
#[global_allocator]
static ALLOCATOR: SQLite3Allocator = SQLite3Allocator {};

// This must be our panic handler for WASM builds. For simplicity, we make it our panic handler for
// all builds. Abort is also more portable than unwind, enabling us to go to more embedded use cases.
#[panic_handler]
fn panic(_info: &PanicInfo) -> ! {
    core::intrinsics::abort()
}

#[cfg(not(target_family = "wasm"))]
#[lang = "eh_personality"]
extern "C" fn eh_personality() {}

#[cfg(target_family = "wasm")]
#[no_mangle]
pub fn __rust_alloc_error_handler(_: Layout) -> ! {
    core::intrinsics::abort()
}

#[no_mangle]
pub extern "C" fn sqlite3_crsqlrustbundle_init(
    db: *mut sqlite::sqlite3,
    err_msg: *mut *mut c_char,
    api: *mut sqlite::api_routines,
) -> c_int {
    sqlite::EXTENSION_INIT2(api);

    let rc = sqlite3_crsqlfractionalindex_init(db, err_msg, api);
    if rc != 0 {
        return rc;
    }

    sqlite3_crsqlcore_init(db, err_msg, api)
}
