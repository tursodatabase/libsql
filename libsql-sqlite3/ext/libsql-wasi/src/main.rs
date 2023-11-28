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

    let mut exec = |sql: &str, times: usize| -> Result<()> {
        let sql_ptr = malloc.call(&mut store, sql.len() as i32 + 1)?;
        memory.write(&mut store, sql_ptr as usize, sql.as_bytes())?;
        for _ in 0..times {
            exec_func.call(&mut store, (db, sql_ptr))?;
        }
        free.call(&mut store, sql_ptr)?;
        Ok(())
    };

    exec("CREATE TABLE IF NOT EXISTS testme (id, name);", 1)?;

    let start = std::time::Instant::now();
    exec("INSERT INTO testme VALUES (42, zeroblob(512));", 100000)?;
    let elapsed = start.elapsed();
    println!("single SQL string allocation: ");
    println!("\t100k inserts took {:?}", elapsed);
    println!("\tper insert: {:?}", elapsed / 100000);

    let start = std::time::Instant::now();
    for _ in 0..100000 {
        exec("INSERT INTO testme VALUES (42, zeroblob(512));", 1)?;
    }
    let elapsed = start.elapsed();
    println!("SQL string allocation per insert: ");
    println!("\t100k inserts took {:?}", elapsed);
    println!("\tper insert: {:?}", elapsed / 100000);

    let _ = close_func.call(&mut store, db)?;
    free.call(&mut store, db_path)?;

    Ok(())
}
