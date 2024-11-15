use std::env;
use std::ffi::OsString;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;

const LIB_NAME: &str = "libsql";
const BUNDLED_DIR: &str = "bundled";
const SQLITE_DIR: &str = "../libsql-sqlite3";

fn main() {
    let target = env::var("TARGET").unwrap();
    let host = env::var("HOST").unwrap();

    let is_apple = host.contains("apple") && target.contains("apple");
    if is_apple {
        println!("cargo:rustc-link-lib=framework=Security");
    }
    let out_dir = env::var("OUT_DIR").unwrap();
    let out_path = Path::new(&out_dir).join("bindgen.rs");

    println!("cargo:rerun-if-changed={BUNDLED_DIR}/src/sqlite3.c");

    if cfg!(feature = "multiple-ciphers") {
        println!("cargo:rerun-if-changed={out_dir}/sqlite3mc/libsqlite3mc_static.a");
    }

    if std::env::var("LIBSQL_DEV").is_ok() {
        make_amalgamation();
        build_multiple_ciphers(&target, &out_path);
    }

    let bindgen_rs_path = if cfg!(feature = "session") {
        "bundled/bindings/session_bindgen.rs"
    } else {
        "bundled/bindings/bindgen.rs"
    };

    let dir = env!("CARGO_MANIFEST_DIR");

    Command::new("cp")
        .arg("--no-preserve=mode,ownership")
        .arg("-R")
        .arg(format!("{dir}/{bindgen_rs_path}"))
        .arg(&out_path)
        .output()
        .unwrap();

    println!("cargo:lib_dir={out_dir}");

    if cfg!(feature = "wasmtime-bindings") && !cfg!(feature = "multiple-ciphers") {
        build_bundled(&out_dir, &out_path);
    }

    if cfg!(feature = "multiple-ciphers") {
        copy_multiple_ciphers(&target, &out_dir, &out_path);
        return;
    }

    build_bundled(&out_dir, &out_path);
}

fn make_amalgamation() {
    let flags = ["-DSQLITE_ENABLE_COLUMN_METADATA=1"];

    Command::new("make")
        .current_dir(SQLITE_DIR)
        .arg("clean")
        .output()
        .unwrap();

    Command::new("./configure")
        .current_dir(SQLITE_DIR)
        .env("CFLAGS", flags.join(" "))
        .output()
        .unwrap();
    Command::new("make")
        .current_dir(SQLITE_DIR)
        .output()
        .unwrap();

    std::fs::copy(
        (SQLITE_DIR.as_ref() as &Path).join("sqlite3.c"),
        (BUNDLED_DIR.as_ref() as &Path).join("src/sqlite3.c"),
    )
    .unwrap();
    std::fs::copy(
        (SQLITE_DIR.as_ref() as &Path).join("sqlite3.h"),
        (BUNDLED_DIR.as_ref() as &Path).join("src/sqlite3.h"),
    )
    .unwrap();
}

pub fn build_bundled(out_dir: &str, out_path: &Path) {
    let bindgen_rs_path = if cfg!(feature = "session") {
        "bundled/bindings/session_bindgen.rs"
    } else {
        "bundled/bindings/bindgen.rs"
    };

    if std::env::var("LIBSQL_DEV").is_ok() {
        let header = HeaderLocation::FromPath(format!("{BUNDLED_DIR}/src/sqlite3.h"));
        bindings::write_to_out_dir(header, bindgen_rs_path.as_ref());
    }

    let dir = env!("CARGO_MANIFEST_DIR");
    std::fs::copy(format!("{dir}/{bindgen_rs_path}"), out_path).unwrap();

    let mut cfg = cc::Build::new();
    cfg.file(format!("{BUNDLED_DIR}/src/sqlite3.c"))
        .flag("-std=c11")
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

    if cfg!(feature = "wasmtime-bindings") {
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

fn copy_multiple_ciphers(target: &str, out_dir: &str, out_path: &Path) {
    let dylib = format!("{out_dir}/sqlite3mc/libsqlite3mc_static.a");
    if !Path::new(&dylib).exists() {
        build_multiple_ciphers(target, out_path);
    }

    std::fs::copy(dylib, format!("{out_dir}/libsqlite3mc.a")).unwrap();
    println!("cargo:rustc-link-lib=static=sqlite3mc");
    println!("cargo:rustc-link-search={out_dir}");
}

fn build_multiple_ciphers(target: &str, out_path: &Path) {
    let bindgen_rs_path = if cfg!(feature = "session") {
        "bundled/bindings/session_bindgen.rs"
    } else {
        "bundled/bindings/bindgen.rs"
    };
    if std::env::var("LIBSQL_DEV").is_ok() {
        let header = HeaderLocation::FromPath(format!("{BUNDLED_DIR}/src/sqlite3.h"));
        bindings::write_to_out_dir(header, bindgen_rs_path.as_ref());
    }
    let dir = env!("CARGO_MANIFEST_DIR");
    std::fs::copy(format!("{dir}/{bindgen_rs_path}"), out_path).unwrap();

    std::fs::copy(
        (BUNDLED_DIR.as_ref() as &Path)
            .join("src")
            .join("sqlite3.c"),
        (BUNDLED_DIR.as_ref() as &Path)
            .join("SQLite3MultipleCiphers")
            .join("src")
            .join("sqlite3.c"),
    )
    .unwrap();

    let bundled_dir = env::current_dir()
        .unwrap()
        .join(BUNDLED_DIR)
        .join("SQLite3MultipleCiphers");
    let out_dir = env::var("OUT_DIR").unwrap();
    let sqlite3mc_build_dir = env::current_dir().unwrap().join(out_dir).join("sqlite3mc");
    let _ = fs::remove_dir_all(sqlite3mc_build_dir.clone());
    fs::create_dir_all(sqlite3mc_build_dir.clone()).unwrap();

    let mut cmake_opts: Vec<&str> = vec![];

    let target_postfix = target.to_string().replace("-", "_");
    let cross_cc_var_name = format!("CC_{}", target_postfix);
    println!("cargo:warning=CC_var_name={}", cross_cc_var_name);
    let cross_cc = env::var(&cross_cc_var_name).ok();

    let cross_cxx_var_name = format!("CXX_{}", target_postfix);
    let cross_cxx = env::var(&cross_cxx_var_name).ok();

    let toolchain_path = sqlite3mc_build_dir.join("toolchain.cmake");
    let cmake_toolchain_opt = "-DCMAKE_TOOLCHAIN_FILE=toolchain.cmake".to_string();

    let mut toolchain_file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(toolchain_path.clone())
        .unwrap();

    if let Some(ref cc) = cross_cc {
        if cc.contains("aarch64") && cc.contains("linux") {
            cmake_opts.push(&cmake_toolchain_opt);
            writeln!(toolchain_file, "set(CMAKE_SYSTEM_NAME \"Linux\")").unwrap();
            writeln!(toolchain_file, "set(CMAKE_SYSTEM_PROCESSOR \"arm64\")").unwrap();
        }
    }
    if let Some(cc) = cross_cc {
        writeln!(toolchain_file, "set(CMAKE_C_COMPILER {})", cc).unwrap();
    }
    if let Some(cxx) = cross_cxx {
        writeln!(toolchain_file, "set(CMAKE_CXX_COMPILER {})", cxx).unwrap();
    }

    cmake_opts.push("-DCMAKE_BUILD_TYPE=Release");
    cmake_opts.push("-DSQLITE3MC_STATIC=ON");
    cmake_opts.push("-DCODEC_TYPE=AES256");
    cmake_opts.push("-DSQLITE3MC_BUILD_SHELL=OFF");
    cmake_opts.push("-DSQLITE_SHELL_IS_UTF8=OFF");
    cmake_opts.push("-DSQLITE_USER_AUTHENTICATION=OFF");
    cmake_opts.push("-DSQLITE_SECURE_DELETE=OFF");
    cmake_opts.push("-DSQLITE_ENABLE_COLUMN_METADATA=ON");
    cmake_opts.push("-DSQLITE_USE_URI=ON");
    cmake_opts.push("-DCMAKE_POSITION_INDEPENDENT_CODE=ON");

    if target.contains("musl") {
        cmake_opts.push("-DCMAKE_C_FLAGS=\"-U_FORTIFY_SOURCE\" -D_FILE_OFFSET_BITS=32");
        cmake_opts.push("-DCMAKE_CXX_FLAGS=\"-U_FORTIFY_SOURCE\" -D_FILE_OFFSET_BITS=32");
    }

    let mut cmake = Command::new("cmake");
    cmake.current_dir(sqlite3mc_build_dir.clone());
    cmake.args(cmake_opts.clone());
    cmake.arg(bundled_dir.clone());
    if cfg!(feature = "wasmtime-bindings") {
        cmake.arg("-DLIBSQL_ENABLE_WASM_RUNTIME=1");
    }
    if cfg!(feature = "session") {
        cmake.arg("-DSQLITE_ENABLE_PREUPDATE_HOOK=ON");
        cmake.arg("-DSQLITE_ENABLE_SESSION=ON");
    }
    println!("Running `cmake` with options: {}", cmake_opts.join(" "));
    let status = cmake.status().unwrap();
    if !status.success() {
        panic!("Failed to run cmake with options: {}", cmake_opts.join(" "));
    }

    let mut make = Command::new("cmake");
    make.current_dir(sqlite3mc_build_dir.clone());
    make.args(["--build", "."]);
    make.args(["--config", "Release"]);
    if !make.status().unwrap().success() {
        panic!("Failed to run make");
    }
    // The `msbuild` tool puts the output in a different place so let's move it.
    if Path::exists(&sqlite3mc_build_dir.join("Release/sqlite3mc_static.lib")) {
        fs::rename(
            sqlite3mc_build_dir.join("Release/sqlite3mc_static.lib"),
            sqlite3mc_build_dir.join("libsqlite3mc_static.a"),
        )
        .unwrap();
    }
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

        if !cfg!(feature = "wasmtime-bindings") {
            bindings = bindings
                .blocklist_function("run_wasm")
                .blocklist_function("libsql_run_wasm")
                .blocklist_function("libsql_wasm_engine_new")
                .blocklist_function("libsql_compile_wasm_module")
                .blocklist_function("libsql_free_wasm_module")
                .blocklist_function("libsql_wasm_engine_free");
        }

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
