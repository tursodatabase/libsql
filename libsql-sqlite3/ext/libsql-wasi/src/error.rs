#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Wasmtime error: {0}")]
    WasmtimeError(#[from] wasmtime::Error),
    #[error("Memory access error: {0}")]
    MemoryAccessError(#[from] wasmtime::MemoryAccessError),
    #[error("WASI error: {0}")]
    WasiError(#[from] wasmtime_wasi::Error),
    #[error("Memory error: {0}")]
    MemoryError(&'static str),
    #[error("I/O Error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Internal Error: {0}")]
    InternalError(#[from] Box<dyn std::error::Error + Sync + Send>),
    #[error("Runtime error: {0}")]
    RuntimeError(&'static str),
}
