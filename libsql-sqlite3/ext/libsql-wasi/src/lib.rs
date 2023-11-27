pub mod error;
pub mod memory;
mod vfs;

use wasmtime::{Engine, Instance, Linker, Module, Store};
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder};

pub use error::Error;
pub type Result<T> = std::result::Result<T, Error>;

pub type State = WasiCtx;

pub fn new_linker(engine: &Engine) -> Result<Linker<State>> {
    let mut linker = Linker::new(engine);
    vfs::link(&mut linker)?;
    wasmtime_wasi::add_to_linker(&mut linker, |s| s)?;
    Ok(linker)
}

pub fn instantiate(
    linker: &Linker<State>,
    libsql_wasm_path: impl AsRef<std::path::Path>,
) -> Result<(Store<State>, Instance)> {
    let wasi_ctx = WasiCtxBuilder::new()
        .inherit_stdio()
        .inherit_args()
        .map_err(|e| crate::error::Error::InternalError(Box::new(e)))?
        .build();

    let libsql_module = Module::from_file(linker.engine(), libsql_wasm_path.as_ref())?;

    let mut store = Store::new(linker.engine(), wasi_ctx);
    let instance = linker.instantiate(&mut store, &libsql_module)?;

    Ok((store, instance))
}
