fn main() {
    build::main();
}

#[cfg(feature = "bundled")]
mod build {
    extern crate cc;
    use std::path::Path;
    use std::{env, fs};

    pub fn main() {
        if cfg!(feature = "sqlcipher") {
            panic!("Builds with bundled SQLCipher are not supported");
        }

        let out_dir = env::var("OUT_DIR").unwrap();
        let out_path = Path::new(&out_dir).join("bindgen.rs");
        fs::copy("sqlite3/bindgen_bundled_version.rs", out_path)
            .expect("Could not copy bindings to output directory");

        let mut cfg = cc::Build::new();
        cfg.file("sqlite3/sqlite3.c")
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
            .flag("-DHAVE_USLEEP=1");
        if cfg!(feature = "unlock_notify") {
            cfg.flag("-DSQLITE_ENABLE_UNLOCK_NOTIFY");
        }
        cfg.compile("libsqlite3.a");

        println!("cargo:lib_dir={}", out_dir);
    }
}

#[cfg(not(feature = "bundled"))]
mod build {
    extern crate pkg_config;

    #[cfg(all(feature = "vcpkg", target_env = "msvc"))]
    extern crate vcpkg;

    use std::env;

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
                    let mut header = env::var(format!("{}_INCLUDE_DIR", prefix)).expect(&format!(
                        "{}_INCLUDE_DIR must be set if {}_LIB_DIR is set",
                        prefix, prefix
                    ));
                    header.push_str("/sqlite3.h");
                    header
                }
                HeaderLocation::Wrapper => "wrapper.h".into(),
                HeaderLocation::FromPath(path) => path,
            }
        }
    }

    pub fn main() {
        let header = find_sqlite();
        bindings::write_to_out_dir(header);
    }

    // Prints the necessary cargo link commands and returns the path to the header.
    fn find_sqlite() -> HeaderLocation {
        let link_lib = link_lib();

        println!("cargo:rerun-if-env-changed={}_INCLUDE_DIR", env_prefix());
        println!("cargo:rerun-if-env-changed={}_LIB_DIR", env_prefix());
        if cfg!(target_os = "windows") {
            println!("cargo:rerun-if-env-changed=PATH");
        }
        // Allow users to specify where to find SQLite.
        if let Ok(dir) = env::var(format!("{}_LIB_DIR", env_prefix())) {
            println!("cargo:rustc-link-lib={}", link_lib);
            println!("cargo:rustc-link-search={}", dir);
            return HeaderLocation::FromEnvironment;
        }

        if let Some(header) = try_vcpkg() {
            return header;
        }

        // See if pkg-config can do everything for us.
        match pkg_config::Config::new()
            .print_system_libs(false)
            .probe(link_lib)
        {
            Ok(mut lib) => {
                if let Some(mut header) = lib.include_paths.pop() {
                    header.push("sqlite3.h");
                    HeaderLocation::FromPath(header.to_string_lossy().into())
                } else {
                    HeaderLocation::Wrapper
                }
            }
            Err(_) => {
                // No env var set and pkg-config couldn't help; just output the link-lib
                // request and hope that the library exists on the system paths. We used to
                // output /usr/lib explicitly, but that can introduce other linking problems; see
                // https://github.com/jgallagher/rusqlite/issues/207.
                println!("cargo:rustc-link-lib={}", link_lib);
                HeaderLocation::Wrapper
            }
        }
    }

    #[cfg(all(feature = "vcpkg", target_env = "msvc"))]
    fn try_vcpkg() -> Option<HeaderLocation> {
        // See if vcpkg can find it.
        if let Ok(mut lib) = vcpkg::Config::new().probe(link_lib()) {
            if let Some(mut header) = lib.include_paths.pop() {
                header.push("sqlite3.h");
                return Some(HeaderLocation::FromPath(header.to_string_lossy().into()));
            }
        }
        None
    }

    #[cfg(not(all(feature = "vcpkg", target_env = "msvc")))]
    fn try_vcpkg() -> Option<HeaderLocation> {
        None
    }

    fn env_prefix() -> &'static str {
        if cfg!(feature = "sqlcipher") {
            "SQLCIPHER"
        } else {
            "SQLITE3"
        }
    }

    fn link_lib() -> &'static str {
        if cfg!(feature = "sqlcipher") {
            "sqlcipher"
        } else {
            "sqlite3"
        }
    }

    #[cfg(not(feature = "buildtime_bindgen"))]
    mod bindings {
        use super::HeaderLocation;

        use std::path::Path;
        use std::{env, fs};

        static PREBUILT_BINDGEN_PATHS: &'static [&'static str] = &[
            "bindgen-bindings/bindgen_3.6.8.rs",
            #[cfg(feature = "min_sqlite_version_3_6_11")]
            "bindgen-bindings/bindgen_3.6.11.rs",
            #[cfg(feature = "min_sqlite_version_3_6_23")]
            "bindgen-bindings/bindgen_3.6.23.rs",
            #[cfg(feature = "min_sqlite_version_3_7_3")]
            "bindgen-bindings/bindgen_3.7.3.rs",
            #[cfg(feature = "min_sqlite_version_3_7_4")]
            "bindgen-bindings/bindgen_3.7.4.rs",
            #[cfg(feature = "min_sqlite_version_3_7_7")]
            "bindgen-bindings/bindgen_3.7.7.rs",
            #[cfg(feature = "min_sqlite_version_3_7_16")]
            "bindgen-bindings/bindgen_3.7.16.rs",
        ];

        pub fn write_to_out_dir(_header: HeaderLocation) {
            let out_dir = env::var("OUT_DIR").unwrap();
            let out_path = Path::new(&out_dir).join("bindgen.rs");
            let in_path = PREBUILT_BINDGEN_PATHS[PREBUILT_BINDGEN_PATHS.len() - 1];
            fs::copy(in_path, out_path).expect("Could not copy bindings to output directory");
        }
    }

    #[cfg(feature = "buildtime_bindgen")]
    mod bindings {
        extern crate bindgen;

        use self::bindgen::callbacks::{IntKind, ParseCallbacks};
        use super::HeaderLocation;

        use std::env;
        use std::fs::OpenOptions;
        use std::io::Write;
        use std::path::Path;

        #[derive(Debug)]
        struct SqliteTypeChooser;

        impl ParseCallbacks for SqliteTypeChooser {
            fn int_macro(&self, _name: &str, value: i64) -> Option<IntKind> {
                if value >= i32::min_value() as i64 && value <= i32::max_value() as i64 {
                    Some(IntKind::I32)
                } else {
                    None
                }
            }
        }

        pub fn write_to_out_dir(header: HeaderLocation) {
            let header: String = header.into();
            let out_dir = env::var("OUT_DIR").unwrap();
            let mut output = Vec::new();
            bindgen::builder()
                .header(header.clone())
                .parse_callbacks(Box::new(SqliteTypeChooser))
                .rustfmt_bindings(true)
                .generate()
                .expect(&format!("could not run bindgen on header {}", header))
                .write(Box::new(&mut output))
                .expect("could not write output of bindgen");
            let mut output = String::from_utf8(output).expect("bindgen output was not UTF-8?!");

            // rusqlite's functions feature ors in the SQLITE_DETERMINISTIC flag when it can. This flag
            // was added in SQLite 3.8.3, but oring it in in prior versions of SQLite is harmless. We
            // don't want to not build just because this flag is missing (e.g., if we're linking against
            // SQLite 3.7.x), so append the flag manually if it isn't present in bindgen's output.
            if !output.contains("pub const SQLITE_DETERMINISTIC") {
                output.push_str("\npub const SQLITE_DETERMINISTIC: i32 = 2048;\n");
            }

            let path = Path::new(&out_dir).join("bindgen.rs");

            let mut file = OpenOptions::new()
                .write(true)
                .truncate(true)
                .create(true)
                .open(path.clone())
                .expect(&format!("Could not write to {:?}", path));

            file.write_all(output.as_bytes())
                .expect(&format!("Could not write to {:?}", path));
        }
    }
}
