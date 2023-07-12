use std::env;
use std::path::PathBuf;

fn maybe_link() {
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
    } else if cfg!(target_arch = "x86_64") {
        let path = std::env::current_dir()
            .unwrap_or_default()
            .join("bundled")
            .join("x86_64");
        println!(
            "cargo:warning=linking with bundled liblibsql.a from {}",
            path.display()
        );
        println!("cargo:rustc-link-search={}", path.display());
        println!("cargo:rustc-link-lib=libsql");
    } else if cfg!(target_arch = "aarch64") {
        let path = std::env::current_dir()
            .unwrap_or_default()
            .join("bundled")
            .join("aarch64");
        println!(
            "cargo:warning=linking with bundled liblibsql.a from {}",
            path.display()
        );
    } else {
        println!("cargo:warning=not linking libSQL: set LIBSQL_LIB_DIR, LIBSQL_STATIC_LIB_DIR or LIBSQL_DYNAMIC_LIB_DIR to link automatically");
    }
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
