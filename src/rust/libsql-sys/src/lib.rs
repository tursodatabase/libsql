#[allow(clippy::all)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
mod bindings {
    include!(concat!(
        default_env::default_env!("LIBSQL_SRC_DIR", ".."),
        "/bindings.rs"
    ));
}
pub use bindings::*;
