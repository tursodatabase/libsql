extern crate gcc;
extern crate pkg_config;

#[cfg(not(feature = "bundled"))]
fn main() {
    use std::env;
    use std::fs;

    // Allow users to specify where to find SQLite.
    let lib_dir = match env::var("SQLITE3_LIB_DIR") {
        Ok(dir) => dir,
        Err(_) => {
            // See if pkg-config can do everything for us.
            if pkg_config::find_library("sqlite3").is_ok() {
                return
            }

            // Try to fall back to /usr/lib if pkg-config failed.
            match fs::metadata("/usr/lib") {
                Ok(ref attr) if attr.is_dir() => "/usr/lib".to_owned(),
                _ => panic!("Could not find sqlite3. Try setting SQLITE3_LIB_DIR."),
            }
        },
    };

    println!("cargo:rustc-link-lib=sqlite3");
    println!("cargo:rustc-link-search={}", lib_dir);
}

#[cfg(feature = "bundled")]
fn main() {
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
