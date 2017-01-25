extern crate gcc;
extern crate pkg_config;

#[cfg(not(feature = "bundled"))]
fn main() {
    use std::env;

    // Allow users to specify where to find SQLite.
    match env::var("SQLITE3_LIB_DIR") {
        Ok(dir) => {
            println!("cargo:rustc-link-lib=sqlite3");
            println!("cargo:rustc-link-search={}", dir);
        }
        Err(_) => {
            // See if pkg-config can do everything for us.
            if !pkg_config::Config::new().print_system_libs(false).probe("sqlite3").is_ok() {
                // No env var set and pkg-config couldn't help; just output the link-lib
                // request and hope that the library exists on the system paths. We used to
                // output /usr/lib explicitly, but that can introduce other linking problems; see
                // https://github.com/jgallagher/rusqlite/issues/207.
                println!("cargo:rustc-link-lib=sqlite3");
            }
        }
    };
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
