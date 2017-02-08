extern crate bindgen;
extern crate gcc;
extern crate pkg_config;

use std::env;
use bindgen::chooser::{TypeChooser, IntKind};
use std::path::Path;

#[derive(Debug)]
struct SqliteTypeChooser;

impl TypeChooser for SqliteTypeChooser {
    fn int_macro(&self, _name: &str, value: i64) -> Option<IntKind> {
        if value >= i32::min_value() as i64 && value <= i32::max_value() as i64 {
            Some(IntKind::I32)
        } else {
            None
        }
    }
}

fn run_bindgen<T: Into<String>>(header: T) {
    let out_dir = env::var("OUT_DIR").unwrap();
    let header = header.into();
    let _ = bindgen::builder()
        .header(header.clone())
        .ctypes_prefix("::libc")
        .type_chooser(Box::new(SqliteTypeChooser))
        .generate()
        .expect(&format!("could not run bindgen on header {}", header))
        .write_to_file(Path::new(&out_dir).join("bindgen.rs"));
}

#[cfg(not(feature = "bundled"))]
fn main() {
    // Allow users to specify where to find SQLite.
    if let Ok(dir) = env::var("SQLITE3_LIB_DIR") {
        let mut header = env::var("SQLITE3_INCLUDE_DIR")
            .expect("SQLITE3_INCLUDE_DIR must be set if SQLITE3_LIB_DIR is set");
        header.push_str("/sqlite3.h");
        run_bindgen(header);
        println!("cargo:rustc-link-lib=sqlite3");
        println!("cargo:rustc-link-search={}", dir);
        return;
    }

    // See if pkg-config can do everything for us.
    match pkg_config::Config::new().print_system_libs(false).probe("sqlite3") {
        Ok(mut lib) => {
            if let Some(mut header) = lib.include_paths.pop() {
                header.push("sqlite3.h");
                run_bindgen(header.to_string_lossy());
            } else {
                run_bindgen("wrapper.h");
            }
        }
        Err(_) => {
            // No env var set and pkg-config couldn't help; just output the link-lib
            // request and hope that the library exists on the system paths. We used to
            // output /usr/lib explicitly, but that can introduce other linking problems; see
            // https://github.com/jgallagher/rusqlite/issues/207.
            println!("cargo:rustc-link-lib=sqlite3");
            run_bindgen("wrapper.h");
        }
    }
}

#[cfg(feature = "bundled")]
fn main() {
    run_bindgen("sqlite3/sqlite3.h");

    gcc::Config::new()
        .file("sqlite3/sqlite3.c")
        .flag("-DSQLITE_CORE")
        .flag("-DSQLITE_DEFAULT_FOREIGN_KEYS=1")
        .flag("-DSQLITE_ENABLE_API_ARMOR")
        .flag("-DSQLITE_ENABLE_COLUMN_METADATA")
        .flag("-DSQLITE_ENABLE_DBSTAT_VTAB")
        .flag("-DSQLITE_ENABLE_FTS3")
        .flag("-DSQLITE_ENABLE_FTS3_PARENTHESIS")
        .flag("-DSQLITE_ENABLE_FTS5")
        .flag("-DSQLITE_ENABLE_JSON1")
        .flag("-DSQLITE_ENABLE_LOAD_EXTENSION=1")
        .flag("-DSQLITE_ENABLE_MEMORY_MANAGEMENT")
        .flag("-DSQLITE_ENABLE_RTREE")
        .flag("-DSQLITE_ENABLE_STAT2")
        .flag("-DSQLITE_ENABLE_STAT4")
        .flag("-DSQLITE_HAVE_ISNAN")
        .flag("-DSQLITE_SOUNDEX")
        .flag("-DSQLITE_THREADSAFE=1")
        .flag("-DSQLITE_USE_URI")
        .compile("libsqlite3.a");
}
