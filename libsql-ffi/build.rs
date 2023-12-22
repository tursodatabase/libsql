use std::env;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Command;

const LIB_NAME: &str = "libsql";
const BUNDLED_DIR: &str = "bundled";
const SQLITE_DIR: &str = "../libsql-sqlite3";

fn main() {
    let out_dir = env::var("OUT_DIR").unwrap();
    let out_path = Path::new(&out_dir).join("bindgen.rs");

    println!("cargo:rerun-if-changed={BUNDLED_DIR}/src");

    if std::env::var("LIBSQL_DEV").is_ok() || cfg!(feature = "wasmtime-bindings") {
        println!("cargo:rerun-if-changed={SQLITE_DIR}/src");
        make_amalgation();
    }

    build_bundled(&out_dir, &out_path);
}

fn make_amalgation() {
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

    let output = Command::new("./build_libsqlite3mc.sh")
        .current_dir(BUNDLED_DIR)
        .output()
        .unwrap();
    std::fs::copy(
        format!("{BUNDLED_DIR}/SQLite3MultipleCiphers/build/libsqlite3mc.so"),
        format!("{out_dir}/libsqlite3mc.so"),
    )
    .unwrap();
    println!("cargo:rustc-link-lib=dylib=sqlite3mc");
    println!("cargo:rustc-link-search={out_dir}");

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
