use std::env;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Command;

const SQLITE_DIR: &str = "../libsql-sqlite3";
const LIB_NAME: &str = "libsql";

fn run_make() {
    Command::new("./configure")
        .current_dir(SQLITE_DIR)
        .output()
        .unwrap();
    Command::new("make")
        .current_dir(SQLITE_DIR)
        .output()
        .unwrap();
}

fn precompiled() -> bool {
    std::fs::metadata(Path::new(SQLITE_DIR).join(".libs").join("liblibsql.a")).is_ok()
}

fn main() {
    let out_dir = env::var("OUT_DIR").unwrap();
    let out_path = Path::new(&out_dir).join("bindgen.rs");

    // Fast path: liblibsql.a exists and bindings are ready
    println!("cargo:rerun-if-env-changed=LIBSQL_REGENERATE_BINDINGS");
    if precompiled() && env::var("LIBSQL_REGENERATE_BINDINGS").is_err() {
        let bindgen_rs_path = if cfg!(feature = "session") {
            "bundled/bindings/session_bindgen.rs"
        } else {
            "bundled/bindings/bindgen.rs"
        };
        std::fs::copy(Path::new(bindgen_rs_path), &out_path).unwrap();
        std::fs::copy(
            Path::new(SQLITE_DIR).join(".libs").join("liblibsql.a"),
            Path::new(&out_dir).join("liblibsql.a"),
        )
        .unwrap();
        println!("cargo:lib_dir={out_dir}");
        println!("cargo:rustc-link-search={out_dir}");
        println!("cargo:rustc-link-lib=static={LIB_NAME}");
        return;
    }

    println!("cargo:rerun-if-changed={SQLITE_DIR}/src/");
    run_make();
    build_bundled(&out_dir, &out_path);
}

pub fn build_bundled(out_dir: &str, out_path: &Path) {
    let header = HeaderLocation::FromPath(format!("{SQLITE_DIR}/sqlite3.h"));
    bindings::write_to_out_dir(header, out_path);
    println!("cargo:rerun-if-changed={SQLITE_DIR}/sqlite3.c");
    let mut cfg = cc::Build::new();
    cfg.file(format!("{SQLITE_DIR}/sqlite3.c"))
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

    if cfg!(feature = "libsql-wasm-experimental") {
        cfg.flag("-DLIBSQL_ENABLE_WASM_RUNTIME=1");
    }

    if cfg!(feature = "bundled-sqlcipher") {
        cfg.flag("-DSQLITE_HAS_CODEC").flag("-DSQLITE_TEMP_STORE=2");

        let target = env::var("TARGET").unwrap();
        let host = env::var("HOST").unwrap();

        let is_windows = host.contains("windows") && target.contains("windows");
        let is_apple = host.contains("apple") && target.contains("apple");

        let lib_dir = env("OPENSSL_LIB_DIR").map(PathBuf::from);
        let inc_dir = env("OPENSSL_INCLUDE_DIR").map(PathBuf::from);
        let mut use_openssl = false;

        let (lib_dir, inc_dir) = match (lib_dir, inc_dir) {
            (Some(lib_dir), Some(inc_dir)) => {
                use_openssl = true;
                (lib_dir, inc_dir)
            }
            (lib_dir, inc_dir) => match find_openssl_dir(&host, &target) {
                None => {
                    if is_windows && !cfg!(feature = "bundled-sqlcipher-vendored-openssl") {
                        panic!("Missing environment variable OPENSSL_DIR or OPENSSL_DIR is not set")
                    } else {
                        (PathBuf::new(), PathBuf::new())
                    }
                }
                Some(openssl_dir) => {
                    let lib_dir = lib_dir.unwrap_or_else(|| openssl_dir.join("lib"));
                    let inc_dir = inc_dir.unwrap_or_else(|| openssl_dir.join("include"));

                    assert!(
                        Path::new(&lib_dir).exists(),
                        "OpenSSL library directory does not exist: {}",
                        lib_dir.to_string_lossy()
                    );

                    if !Path::new(&inc_dir).exists() {
                        panic!(
                            "OpenSSL include directory does not exist: {}",
                            inc_dir.to_string_lossy()
                        );
                    }

                    use_openssl = true;
                    (lib_dir, inc_dir)
                }
            },
        };

        if cfg!(feature = "bundled-sqlcipher-vendored-openssl") {
            cfg.include(env::var("DEP_OPENSSL_INCLUDE").unwrap());
            // cargo will resolve downstream to the static lib in
            // openssl-sys
        } else if use_openssl {
            cfg.include(inc_dir.to_string_lossy().as_ref());
            let lib_name = if is_windows { "libcrypto" } else { "crypto" };
            println!("cargo:rustc-link-lib=dylib={}", lib_name);
            println!("cargo:rustc-link-search={}", lib_dir.to_string_lossy());
        } else if is_apple {
            cfg.flag("-DSQLCIPHER_CRYPTO_CC");
            println!("cargo:rustc-link-lib=framework=Security");
            println!("cargo:rustc-link-lib=framework=CoreFoundation");
        } else {
            // branch not taken on Windows, just `crypto` is fine.
            println!("cargo:rustc-link-lib=dylib=crypto");
        }
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
            cfg.file("sqlite3/wasm32-wasi-vfs.c");
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

    if let Ok(extras) = env::var("LIBSQLITE3_FLAGS") {
        for extra in extras.split_whitespace() {
            if extra.starts_with("-D") || extra.starts_with("-U") {
                cfg.flag(extra);
            } else if extra.starts_with("SQLITE_") {
                cfg.flag(&format!("-D{extra}"));
            } else {
                panic!("Don't understand {} in LIBSQLITE3_FLAGS", extra);
            }
        }
    }
    println!("cargo:rerun-if-env-changed=LIBSQLITE3_FLAGS");

    cfg.compile(LIB_NAME);

    println!("cargo:lib_dir={out_dir}");
}

fn env(name: &str) -> Option<OsString> {
    let prefix = env::var("TARGET").unwrap().to_uppercase().replace('-', "_");
    let prefixed = format!("{prefix}_{name}");
    let var = env::var_os(prefixed);

    match var {
        None => env::var_os(name),
        _ => var,
    }
}

fn find_openssl_dir(_host: &str, _target: &str) -> Option<PathBuf> {
    let openssl_dir = env("OPENSSL_DIR");
    openssl_dir.map(PathBuf::from)
}

fn env_prefix() -> &'static str {
    if cfg!(any(feature = "sqlcipher", feature = "bundled-sqlcipher")) {
        "SQLCIPHER"
    } else {
        "SQLITE3"
    }
}

pub enum HeaderLocation {
    FromEnvironment,
    Wrapper,
    FromPath(String),
}

impl From<HeaderLocation> for String {
    fn from(header: HeaderLocation) -> String {
        match header {
            HeaderLocation::FromEnvironment => {
                let prefix = env_prefix();
                let mut header = env::var(format!("{prefix}_INCLUDE_DIR")).unwrap_or_else(|_| {
                    panic!(
                        "{}_INCLUDE_DIR must be set if {}_LIB_DIR is set",
                        prefix, prefix
                    )
                });
                header.push_str("/sqlite3.h");
                header
            }
            HeaderLocation::Wrapper => "wrapper.h".into(),
            HeaderLocation::FromPath(path) => path,
        }
    }
}

mod bindings {
    use super::HeaderLocation;
    use bindgen::callbacks::{IntKind, ParseCallbacks};

    use std::fs::OpenOptions;
    use std::io::Write;
    use std::path::Path;

    #[derive(Debug)]
    struct SqliteTypeChooser;

    impl ParseCallbacks for SqliteTypeChooser {
        fn int_macro(&self, _name: &str, value: i64) -> Option<IntKind> {
            if value >= i32::MIN as i64 && value <= i32::MAX as i64 {
                Some(IntKind::I32)
            } else {
                None
            }
        }
        fn item_name(&self, original_item_name: &str) -> Option<String> {
            original_item_name
                .strip_prefix("sqlite3_index_info_")
                .map(|s| s.to_owned())
        }
    }

    // Are we generating the bundled bindings? Used to avoid emitting things
    // that would be problematic in bundled builds. This env var is set by
    // `upgrade.sh`.
    fn generating_bundled_bindings() -> bool {
        // Hacky way to know if we're generating the bundled bindings
        println!("cargo:rerun-if-env-changed=LIBSQLITE3_SYS_BUNDLING");
        match std::env::var("LIBSQLITE3_SYS_BUNDLING") {
            Ok(v) => v != "0",
            Err(_) => false,
        }
    }

    pub fn write_to_out_dir(header: HeaderLocation, out_path: &Path) {
        let header: String = header.into();
        let mut output = Vec::new();
        let mut bindings = bindgen::builder()
            .trust_clang_mangling(false)
            .header(header.clone())
            .parse_callbacks(Box::new(SqliteTypeChooser))
            .blocklist_function("sqlite3_auto_extension")
            .raw_line(
                r#"extern "C" {
    pub fn sqlite3_auto_extension(
        xEntryPoint: ::std::option::Option<
            unsafe extern "C" fn(
                db: *mut sqlite3,
                pzErrMsg: *mut *const ::std::os::raw::c_char,
                pThunk: *const sqlite3_api_routines,
            ) -> ::std::os::raw::c_int,
        >,
    ) -> ::std::os::raw::c_int;
}"#,
            )
            .blocklist_function("sqlite3_cancel_auto_extension")
            .raw_line(
                r#"extern "C" {
    pub fn sqlite3_cancel_auto_extension(
        xEntryPoint: ::std::option::Option<
            unsafe extern "C" fn(
                db: *mut sqlite3,
                pzErrMsg: *mut *const ::std::os::raw::c_char,
                pThunk: *const sqlite3_api_routines,
            ) -> ::std::os::raw::c_int,
        >,
    ) -> ::std::os::raw::c_int;
}"#,
            );

        if cfg!(any(feature = "sqlcipher", feature = "bundled-sqlcipher")) {
            bindings = bindings.clang_arg("-DSQLITE_HAS_CODEC");
        }
        if cfg!(feature = "unlock_notify") {
            bindings = bindings.clang_arg("-DSQLITE_ENABLE_UNLOCK_NOTIFY");
        }
        if cfg!(feature = "preupdate_hook") {
            bindings = bindings.clang_arg("-DSQLITE_ENABLE_PREUPDATE_HOOK");
        }
        if cfg!(feature = "session") {
            bindings = bindings.clang_arg("-DSQLITE_ENABLE_SESSION");
        }

        // When cross compiling unless effort is taken to fix the issue, bindgen
        // will find the wrong headers. There's only one header included by the
        // amalgamated `sqlite.h`: `stdarg.h`.
        //
        // Thankfully, there's almost no case where rust code needs to use
        // functions taking `va_list` (It's nearly impossible to get a `va_list`
        // in Rust unless you get passed it by C code for some reason).
        //
        // Arguably, we should never be including these, but we include them for
        // the cases where they aren't totally broken...
        let target_arch = std::env::var("TARGET").unwrap();
        let host_arch = std::env::var("HOST").unwrap();
        let is_cross_compiling = target_arch != host_arch;

        // Note that when generating the bundled file, we're essentially always
        // cross compiling.
        if generating_bundled_bindings() || is_cross_compiling {
            // Get rid of va_list, as it's not
            bindings = bindings
                .blocklist_function("sqlite3_vmprintf")
                .blocklist_function("sqlite3_vsnprintf")
                .blocklist_function("sqlite3_str_vappendf")
                .blocklist_type("va_list")
                .blocklist_type("__builtin_va_list")
                .blocklist_type("__gnuc_va_list")
                .blocklist_type("__va_list_tag")
                .blocklist_item("__GNUC_VA_LIST");
        }

        bindings
            .layout_tests(false)
            .generate()
            .unwrap_or_else(|_| panic!("could not run bindgen on header {}", header))
            .write(Box::new(&mut output))
            .expect("could not write output of bindgen");
        let mut output = String::from_utf8(output).expect("bindgen output was not UTF-8?!");

        // rusqlite's functions feature ors in the SQLITE_DETERMINISTIC flag when it
        // can. This flag was added in SQLite 3.8.3, but oring it in in prior
        // versions of SQLite is harmless. We don't want to not build just
        // because this flag is missing (e.g., if we're linking against
        // SQLite 3.7.x), so append the flag manually if it isn't present in bindgen's
        // output.
        if !output.contains("pub const SQLITE_DETERMINISTIC") {
            output.push_str("\npub const SQLITE_DETERMINISTIC: i32 = 2048;\n");
        }

        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(out_path)
            .unwrap_or_else(|_| panic!("Could not write to {:?}", out_path));

        file.write_all(output.as_bytes())
            .unwrap_or_else(|_| panic!("Could not write to {:?}", out_path));
    }
}
