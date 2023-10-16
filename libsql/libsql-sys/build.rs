use std::env;
use std::path::PathBuf;

fn maybe_link() {
    let out_dir = env::var("OUT_DIR").unwrap();
    println!("cargo:rerun-if-env-changed=LIBSQL_LIB_DIR");
    println!("cargo:rerun-if-env-changed=LIBSQL_STATIC_LIB_DIR");
    println!("cargo:rerun-if-env-changed=LIBSQL_DYNAMIC_LIB_DIR");

    if let Ok(dir) = env::var("LIBSQL_STATIC_LIB_DIR") {
        println!("cargo:rustc-link-search={dir}");
        println!("cargo:rustc-link-lib=static=libsql");
    } else if let Ok(dir) = env::var("LIBSQL_DYNAMIC_LIB_DIR") {
        println!("cargo:rustc-link-search={dir}");
        println!("cargo:rustc-link-lib=dylib=libsql");
    } else if let Ok(dir) = env::var("LIBSQL_LIB_DIR") {
        println!("cargo:rustc-link-search={dir}");
        println!("cargo:rustc-link-lib=libsql");
    } else if env::var("LIBSQL_NO_AMALGAMATION_PLZ").is_err() {
        compile();
        println!("cargo:rustc-link-search={out_dir}");
        println!("cargo:rustc-link-lib=static=libsql");
    } else if cfg!(target_arch = "x86_64") {
        let path = env::current_dir()
            .unwrap_or_default()
            .join("bundled")
            .join("x86_64");
        println!(
            "cargo:warning=linking with bundled liblibsql.a from {}",
            path.display()
        );
        println!("cargo:rustc-link-search={}", path.display());
        println!("cargo:rustc-link-lib=static=libsql");
    } else if cfg!(target_arch = "aarch64") {
        let path = env::current_dir()
            .unwrap_or_default()
            .join("bundled")
            .join("aarch64");
        println!(
            "cargo:warning=linking with bundled liblibsql.a from {}",
            path.display()
        );
        println!("cargo:rustc-link-search={}", path.display());
        println!("cargo:rustc-link-lib=static=libsql");
    } else {
        println!("cargo:warning=not linking libSQL: set LIBSQL_LIB_DIR, LIBSQL_STATIC_LIB_DIR or LIBSQL_DYNAMIC_LIB_DIR to link automatically");
    }
}

// NOTICE: ripped from libsqlite3-sys
fn compile() {
    let out_dir = env::var("OUT_DIR").unwrap();
    println!("cargo:warning=Compiling from the bundled amalgamation file");

    println!("cargo:rerun-if-changed=bundled/src/sqlite3.c");
    println!("cargo:rerun-if-changed=bundled/src/wasm32-wasi-vfs.c");
    let mut cfg = cc::Build::new();
    cfg.file("bundled/src/sqlite3.c")
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
        .flag("-DSQLITE_SOUNDEX")
        .flag("-DSQLITE_THREADSAFE=1")
        .flag("-DSQLITE_USE_URI")
        .flag("-DHAVE_USLEEP=1")
        .flag("-D_POSIX_THREAD_SAFE_FUNCTIONS") // cross compile with MinGW
        .warnings(false);

    if cfg!(feature = "wasm") {
        cfg.flag("-DENABLE_WASM_RUNTIME");
    }

    if cfg!(feature = "with-asan") {
        cfg.flag("-fsanitize=address");
    }

    // Target wasm32-wasi can't compile the default VFS
    if env::var("TARGET").map_or(false, |v| v == "wasm32-wasi") {
        cfg.flag("-DSQLITE_OS_OTHER")
            // https://github.com/rust-lang/rust/issues/74393
            .flag("-DLONGDOUBLE_TYPE=double");
        if cfg!(feature = "wasm32-wasi-vfs") {
            cfg.file("bundled/src/wasm32-wasi-vfs.c");
        }
    }
    if cfg!(feature = "unlock_notify") {
        cfg.flag("-DSQLITE_ENABLE_UNLOCK_NOTIFY");
    }
    if cfg!(feature = "preupdate_hook") {
        cfg.flag("-DSQLITE_ENABLE_PREUPDATE_HOOK");
    }
    if cfg!(feature = "session") {
        cfg.flag("-DSQLITE_ENABLE_SESSION");
    }

    if let Ok(limit) = env::var("SQLITE_MAX_VARIABLE_NUMBER") {
        cfg.flag(&format!("-DSQLITE_MAX_VARIABLE_NUMBER={limit}"));
    }
    println!("cargo:rerun-if-env-changed=SQLITE_MAX_VARIABLE_NUMBER");

    if let Ok(limit) = env::var("SQLITE_MAX_EXPR_DEPTH") {
        cfg.flag(&format!("-DSQLITE_MAX_EXPR_DEPTH={limit}"));
    }
    println!("cargo:rerun-if-env-changed=SQLITE_MAX_EXPR_DEPTH");

    if let Ok(limit) = env::var("SQLITE_MAX_COLUMN") {
        cfg.flag(&format!("-DSQLITE_MAX_COLUMN={limit}"));
    }
    println!("cargo:rerun-if-env-changed=SQLITE_MAX_COLUMN");

    if let Ok(extras) = env::var("LIBLIBSQL_FLAGS") {
        for extra in extras.split_whitespace() {
            if extra.starts_with("-D") || extra.starts_with("-U") {
                cfg.flag(extra);
            } else if extra.starts_with("SQLITE_") {
                cfg.flag(&format!("-D{extra}"));
            } else {
                panic!("Don't understand {} in LIBLIBSQL_FLAGS", extra);
            }
        }
    }
    println!("cargo:rerun-if-env-changed=LIBLIBSQL_FLAGS");

    cfg.compile("libsql");

    println!("cargo:lib_dir={out_dir}");
}

fn main() {
    maybe_link();

    println!("cargo:rerun-if-env-changed=LIBSQL_SRC_DIR");
    let src_dir = match env::var("LIBSQL_SRC_DIR") {
        Ok(dir) => PathBuf::from(dir),
        Err(_) => {
            println!("cargo:warning=Using precompiled bindings: bindings.rs");
            println!(
                "cargo:warning=Specify LIBSQL_SRC_DIR env variable to regenerate bindings first."
            );
            return;
        }
    };
    let bindings = bindgen::Builder::default()
        .header(
            src_dir
                .join("sqlite3.h")
                .as_path()
                .to_str()
                .expect("Unable to parse path"),
        )
        .generate()
        .expect("Unable to generate bindings");

    bindings
        .write_to_file(src_dir.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}
