pub mod memory;
mod vfs;

// FIXME: add any state we need to pass - WasiCtx is here to get free println! and stuff
type State = WasiCtx;

use anyhow::Context;
use wasmtime::{Engine, Linker, Module, Store};
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder};

fn main() -> anyhow::Result<()> {
    let engine = Engine::default();

    let libsql_module = Module::from_file(&engine, "../../libsql.wasm")?;

    let mut linker = Linker::new(&engine);
    vfs::link(&mut linker)?;
    wasmtime_wasi::add_to_linker(&mut linker, |s| s)?;

    // FIXME: we might as well not need it with VFS and virtual WAL, it's here to make stuff easier for debugging
    let wasi = WasiCtxBuilder::new()
        .inherit_stdio()
        .inherit_args()?
        .build();

    let mut store = Store::new(&engine, wasi);
    let instance = linker.instantiate(&mut store, &libsql_module)?;

    let malloc = instance.get_typed_func::<i32, i32>(&mut store, "malloc")?;
    let free = instance.get_typed_func::<i32, ()>(&mut store, "free")?;

    let memory = instance
        .get_memory(&mut store, "memory")
        .context("memory export not found")?;

    let db_path = malloc.call(&mut store, 16)?;
    memory.write(&mut store, db_path as usize, b"/tmp/wasm-demo.db\0")?;

    let libsql_wasi_init = instance.get_typed_func::<(), ()>(&mut store, "libsql_wasi_init")?;
    let open_func = instance.get_typed_func::<i32, i32>(&mut store, "libsql_wasi_open_db")?;
    let exec_func = instance.get_typed_func::<(i32, i32), i32>(&mut store, "libsql_wasi_exec")?;
    let close_func = instance.get_typed_func::<i32, i32>(&mut store, "sqlite3_close")?;

    libsql_wasi_init.call(&mut store, ())?;
    let db = open_func.call(&mut store, db_path)?;
    let sql = malloc.call(&mut store, 64)?;
    memory.write(&mut store, sql as usize, b"PRAGMA journal_mode=WAL;\0")?;
    let rc = exec_func.call(&mut store, (db, sql))?;
    let _ = free.call(&mut store, sql)?;
    let _ = close_func.call(&mut store, db)?;
    let _ = free.call(&mut store, db_path)?;

    println!("rc: {rc}");

    Ok(())
}
