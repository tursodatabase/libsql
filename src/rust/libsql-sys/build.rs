use std::env;
use std::path::PathBuf;

fn main() {
    let src_dir = PathBuf::from(
        env::var("LIBSQL_SRC_DIR").expect("LIBSQL_SRC_DIR must be defined at compile time"),
    );
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
