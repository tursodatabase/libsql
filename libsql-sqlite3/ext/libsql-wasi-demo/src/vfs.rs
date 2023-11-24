use super::{memory, State};
use wasmtime::{Caller, Linker, Memory};

fn get_memory(caller: &mut Caller<'_, State>) -> Memory {
    caller.get_export("memory").unwrap().into_memory().unwrap()
}

fn open_fd(mut caller: Caller<'_, State>, name: i32, flags: i32) -> anyhow::Result<i64> {
    let memory = get_memory(&mut caller);
    let (memory, _state) = memory.data_and_store_mut(&mut caller);

    let name = memory::read_cstr(memory, name)?;

    println!("HOST OPEN_FD CALLED: {name:?} {flags:0o}");

    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(name)?;
    let file = Box::new(file);

    Ok(Box::into_raw(file) as i64)
}

fn delete(
    mut caller: Caller<'_, State>,
    vfs: i32,
    name: i32,
    sync_dir: i32,
) -> anyhow::Result<i32> {
    let memory = get_memory(&mut caller);
    let (_memory, _state) = memory.data_and_store_mut(&mut caller);

    println!("HOST DELETE CALLED");
    Ok(0)
}

fn access(
    mut caller: Caller<'_, State>,
    vfs: i32,
    name: i32,
    flags: i32,
    res_out: i32,
) -> anyhow::Result<i32> {
    let memory = get_memory(&mut caller);
    let (_memory, _state) = memory.data_and_store_mut(&mut caller);

    println!("HOST ACCESS CALLED");
    Ok(0)
}

fn full_pathname(
    mut caller: Caller<'_, State>,
    vfs: i32,
    name: i32,
    n_out: i32,
    out: i32,
) -> anyhow::Result<i32> {
    let memory = get_memory(&mut caller);
    let (memory, _state) = memory.data_and_store_mut(&mut caller);

    let name = memory::read_cstr(memory, name)?;
    let out = memory::slice_mut(memory, out, n_out as usize)?;

    out[..name.len()].copy_from_slice(name.as_bytes());
    Ok(0)
}

fn randomness(
    mut caller: Caller<'_, State>,
    vfs: i32,
    n_byte: i32,
    out: i32,
) -> anyhow::Result<i32> {
    let memory = get_memory(&mut caller);
    let (_memory, _state) = memory.data_and_store_mut(&mut caller);

    println!("HOST RANDOMNESS CALLED");
    Ok(0)
}

fn sleep(mut caller: Caller<'_, State>, vfs: i32, microseconds: i32) -> anyhow::Result<i32> {
    let memory = get_memory(&mut caller);
    let (_memory, _state) = memory.data_and_store_mut(&mut caller);

    println!("HOST SLEEP CALLED");
    Ok(0)
}

fn current_time(mut caller: Caller<'_, State>, vfs: i32, out: i32) -> anyhow::Result<i32> {
    let memory = get_memory(&mut caller);
    let (_memory, _state) = memory.data_and_store_mut(&mut caller);

    println!("HOST CURRENT TIME CALLED");
    Ok(0)
}

fn get_last_error(
    mut caller: Caller<'_, State>,
    vfs: i32,
    i: i32,
    out: i32,
) -> anyhow::Result<i32> {
    let memory = get_memory(&mut caller);
    let (_memory, _state) = memory.data_and_store_mut(&mut caller);

    println!("HOST GET LAST ERROR CALLED");
    Ok(0)
}

fn current_time_64(mut caller: Caller<'_, State>, vfs: i32, out: i32) -> anyhow::Result<i32> {
    let memory = get_memory(&mut caller);
    let (_memory, _state) = memory.data_and_store_mut(&mut caller);

    println!("HOST CURRENT TIME 64 CALLED");
    Ok(0)
}

fn close(mut caller: Caller<'_, State>, file: i32) -> anyhow::Result<i32> {
    let memory = get_memory(&mut caller);
    let (_memory, _state) = memory.data_and_store_mut(&mut caller);
    // TODO: read the file pointer from guest memory and feed it to Box::from_raw
    println!("HOST CLOSE CALLED");

    Ok(0)
}

fn read(
    mut caller: Caller<'_, State>,
    file: i32,
    buf: i32,
    amt: i32,
    offset: i64,
) -> anyhow::Result<i32> {
    let memory = get_memory(&mut caller);
    let (_memory, _state) = memory.data_and_store_mut(&mut caller);

    println!("HOST READ CALLED");
    Ok(0)
}

fn write(
    mut caller: Caller<'_, State>,
    file: i32,
    buf: i32,
    amt: i32,
    offset: i64,
) -> anyhow::Result<i32> {
    let memory = get_memory(&mut caller);
    let (_memory, _state) = memory.data_and_store_mut(&mut caller);

    println!("HOST WRITE CALLED");
    Ok(0)
}

fn truncate(mut caller: Caller<'_, State>, file: i32, size: i64) -> anyhow::Result<i32> {
    let memory = get_memory(&mut caller);
    let (_memory, _state) = memory.data_and_store_mut(&mut caller);
    println!("HOST TRUNCATE CALLED");
    Ok(0)
}

fn sync(mut caller: Caller<'_, State>, file: i32, flags: i32) -> anyhow::Result<i32> {
    let memory = get_memory(&mut caller);
    let (_memory, _state) = memory.data_and_store_mut(&mut caller);
    println!("HOST SYNC CALLED");
    Ok(0)
}

fn file_size(mut caller: Caller<'_, State>, file: i32, size: i32) -> anyhow::Result<i32> {
    let memory = get_memory(&mut caller);
    let (_memory, _state) = memory.data_and_store_mut(&mut caller);
    println!("HOST FILE SIZE CALLED");
    Ok(0)
}

fn lock(mut caller: Caller<'_, State>, file: i32, lock: i32) -> anyhow::Result<i32> {
    let memory = get_memory(&mut caller);
    let (_memory, _state) = memory.data_and_store_mut(&mut caller);
    println!("HOST LOCK CALLED");
    Ok(0)
}

fn unlock(mut caller: Caller<'_, State>, file: i32, lock: i32) -> anyhow::Result<i32> {
    let memory = get_memory(&mut caller);
    let (_memory, _state) = memory.data_and_store_mut(&mut caller);
    println!("HOST UNLOCK CALLED");
    Ok(0)
}

fn check_reserved_lock(
    mut caller: Caller<'_, State>,
    file: i32,
    reserved_lock: i32,
) -> anyhow::Result<i32> {
    let memory = get_memory(&mut caller);
    let (_memory, _state) = memory.data_and_store_mut(&mut caller);
    println!("HOST CHECK RESERVED LOCK CALLED");
    Ok(0)
}

fn file_control(
    mut caller: Caller<'_, State>,
    file: i32,
    op: i32,
    arg: i32,
) -> anyhow::Result<i32> {
    let memory = get_memory(&mut caller);
    let (_memory, _state) = memory.data_and_store_mut(&mut caller);

    println!("HOST FILE CONTROL CALLED");
    Ok(0)
}

fn sector_size(mut caller: Caller<'_, State>, file: i32) -> anyhow::Result<i32> {
    let memory = get_memory(&mut caller);
    let (_memory, _state) = memory.data_and_store_mut(&mut caller);

    println!("HOST SECTOR SIZE CALLED");
    Ok(0)
}

fn device_characteristics(mut caller: Caller<'_, State>, file: i32) -> anyhow::Result<i32> {
    let memory = get_memory(&mut caller);
    let (_memory, _state) = memory.data_and_store_mut(&mut caller);

    println!("HOST DEVICE CHARACTERISTICS CALLED");
    Ok(0)
}

pub fn link(linker: &mut Linker<State>) -> anyhow::Result<()> {
    linker.func_wrap("libsql_host", "open_fd", open_fd)?;
    linker.func_wrap("libsql_host", "delete", delete)?;
    linker.func_wrap("libsql_host", "access", access)?;
    linker.func_wrap("libsql_host", "full_pathname", full_pathname)?;
    linker.func_wrap("libsql_host", "randomness", randomness)?;
    linker.func_wrap("libsql_host", "sleep", sleep)?;
    linker.func_wrap("libsql_host", "current_time", current_time)?;
    linker.func_wrap("libsql_host", "get_last_error", get_last_error)?;
    linker.func_wrap("libsql_host", "current_time_64", current_time_64)?;

    linker.func_wrap("libsql_host", "close", close)?;
    linker.func_wrap("libsql_host", "read", read)?;
    linker.func_wrap("libsql_host", "write", write)?;
    linker.func_wrap("libsql_host", "truncate", truncate)?;
    linker.func_wrap("libsql_host", "sync", sync)?;
    linker.func_wrap("libsql_host", "file_size", file_size)?;
    linker.func_wrap("libsql_host", "lock", lock)?;
    linker.func_wrap("libsql_host", "unlock", unlock)?;
    linker.func_wrap("libsql_host", "check_reserved_lock", check_reserved_lock)?;
    linker.func_wrap("libsql_host", "file_control", file_control)?;
    linker.func_wrap("libsql_host", "sector_size", sector_size)?;
    linker.func_wrap(
        "libsql_host",
        "device_characteristics",
        device_characteristics,
    )?;
    Ok(())
}
