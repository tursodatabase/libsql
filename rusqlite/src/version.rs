use ffi;
use std::ffi::CStr;

/// Returns the SQLite version as an integer; e.g., `3016002` for version 3.16.2.
///
/// See [sqlite3_libversion_number()](https://www.sqlite.org/c3ref/libversion.html).
pub fn version_number() -> i32 {
    unsafe { ffi::sqlite3_libversion_number() }
}

/// Returns the SQLite version as a string; e.g., `"3.16.2"` for version 3.16.2.
///
/// See [sqlite3_libversion()](https://www.sqlite.org/c3ref/libversion.html).
pub fn version() -> &'static str {
    let cstr = unsafe { CStr::from_ptr(ffi::sqlite3_libversion()) };
    cstr.to_str().expect("SQLite version string is not valid UTF8 ?!")
}

/// Returns the source ID of SQLite, identifying the commit of SQLite for the current version.
///
/// See [sqlite3_sourceid()](https://www.sqlite.org/c3ref/libversion.html).
pub fn source_id() -> &'static str {
    let cstr = unsafe { CStr::from_ptr(ffi::sqlite3_sourceid()) };
    cstr.to_str().expect("SQLite source ID is not valid UTF8 ?!")
}
