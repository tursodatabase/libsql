use std::env;
use std::path::PathBuf;

fn main() {
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
