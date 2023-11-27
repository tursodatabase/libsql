use libsql_wasi::{instantiate, new_linker, Error, Result};

fn main() -> Result<()> {
    tracing_subscriber::fmt::try_init().ok();

    let engine = wasmtime::Engine::default();
    let linker = new_linker(&engine)?;
    let (mut store, instance) = instantiate(&linker, "../../libsql.wasm")?;

    let malloc = instance.get_typed_func::<i32, i32>(&mut store, "malloc")?;
    let free = instance.get_typed_func::<i32, ()>(&mut store, "free")?;

    let memory = instance
        .get_memory(&mut store, "memory")
        .ok_or_else(|| Error::RuntimeError("no memory found"))?;

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
    free.call(&mut store, sql)?;
    if rc != 0 {
        return Err(Error::RuntimeError("Failed to execute SQL"));
    }

    let sql = malloc.call(&mut store, 64)?;
    memory.write(
        &mut store,
        sql as usize,
        b"CREATE TABLE testme(id, v1, v2);\0",
    )?;
    let rc = exec_func.call(&mut store, (db, sql))?;
    free.call(&mut store, sql)?;

    let _ = close_func.call(&mut store, db)?;
    free.call(&mut store, db_path)?;

    println!("rc: {rc}");

    Ok(())
}
