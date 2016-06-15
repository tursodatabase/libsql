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
    gcc::compile_library("libsqlite3.a", &["sqlite3/sqlite3.c"]);
}
