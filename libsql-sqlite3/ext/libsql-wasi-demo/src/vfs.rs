use super::{memory, State};
use wasmtime::{Caller, Linker, Memory};

/* Reference from C:
typedef struct libsql_wasi_file {
    const struct sqlite3_io_methods* pMethods;
    int64_t fd;
} libsql_wasi_file;

#[repr(C)]
struct LibsqlWasiFile {
    ptr: *mut std::ffi::c_void,
    fd: i64,
}
*/

fn get_memory(caller: &mut Caller<'_, State>) -> Memory {
    caller.get_export("memory").unwrap().into_memory().unwrap()
}

fn get_file(memory: &[u8], file_ptr: i32) -> &'static mut std::fs::File {
    let file_fd = i64::from_le_bytes(
        memory[file_ptr as usize + 8..file_ptr as usize + 8 + 8]
            .try_into()
            .unwrap(),
    );
    let mut file: &'static mut std::fs::File = unsafe { &mut *(file_fd as *mut std::fs::File) };

    tracing::debug!("Metadata: {:?}", file.metadata());
    file
}

fn open_fd(mut caller: Caller<'_, State>, name: i32, flags: i32) -> anyhow::Result<i64> {
    let memory = get_memory(&mut caller);
    let (memory, _state) = memory.data_and_store_mut(&mut caller);

    let name = memory::read_cstr(memory, name)?;

    tracing::debug!("Opening a file on host: {name:?} {flags:0o}");

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

    tracing::debug!("HOST DELETE CALLED");
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

    tracing::debug!("HOST ACCESS CALLED");
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

    tracing::debug!("HOST RANDOMNESS CALLED");
    Ok(0)
}

fn sleep(mut caller: Caller<'_, State>, vfs: i32, microseconds: i32) -> anyhow::Result<i32> {
    let memory = get_memory(&mut caller);
    let (_memory, _state) = memory.data_and_store_mut(&mut caller);

    tracing::debug!("HOST SLEEP CALLED");
    Ok(0)
}

fn current_time(mut caller: Caller<'_, State>, vfs: i32, out: i32) -> anyhow::Result<i32> {
    let memory = get_memory(&mut caller);
    let (_memory, _state) = memory.data_and_store_mut(&mut caller);

    tracing::debug!("HOST CURRENT TIME CALLED");
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

    tracing::debug!("HOST GET LAST ERROR CALLED");
    Ok(0)
}

fn current_time_64(mut caller: Caller<'_, State>, vfs: i32, out: i32) -> anyhow::Result<i32> {
    let memory = get_memory(&mut caller);
    let (_memory, _state) = memory.data_and_store_mut(&mut caller);

    tracing::debug!("HOST CURRENT TIME 64 CALLED");
    Ok(0)
}

fn close(mut caller: Caller<'_, State>, file: i32) -> anyhow::Result<i32> {
    let memory = get_memory(&mut caller);
    let (memory, _state) = memory.data_and_store_mut(&mut caller);

    let file_fd = i64::from_le_bytes(
        memory[file as usize + 8..file as usize + 8 + 8]
            .try_into()
            .unwrap(),
    );
    let file = unsafe { Box::from_raw(file_fd as *mut std::fs::File) };

    Ok(0)
}

fn read(
    mut caller: Caller<'_, State>,
    file: i32,
    buf: i32,
    amt: i32,
    offset: i64,
) -> anyhow::Result<i32> {
    use std::io::{Read, Seek};

    let memory = get_memory(&mut caller);
    let (memory, _state) = memory.data_and_store_mut(&mut caller);

    tracing::debug!("HOST READ CALLED: {amt} bytes starting at {offset}");

    let file = get_file(memory, file);
    file.seek(std::io::SeekFrom::Start(offset as u64))?;

    let buf = memory::slice_mut(memory, buf, amt as usize)?;
    match file.read_exact(buf) {
        Ok(_) => Ok(0),
        Err(e) => {
            let errno = e.raw_os_error().unwrap_or(0);
            tracing::debug!("Assuming short read, got: {e}");
            // 522 == SQLITE_IOERR_SHORT_READ
            buf.fill(0);
            Ok(522)
        }
    }
}

fn write(
    mut caller: Caller<'_, State>,
    file: i32,
    buf: i32,
    amt: i32,
    offset: i64,
) -> anyhow::Result<i32> {
    use std::io::{Seek, Write};

    let memory = get_memory(&mut caller);
    let (memory, _state) = memory.data_and_store_mut(&mut caller);

    tracing::debug!("HOST WRITE CALLED");

    let file = get_file(memory, file);
    file.seek(std::io::SeekFrom::Start(offset as u64))?;

    let buf = memory::slice(memory, buf, amt as usize)?;
    match file.write_all(buf) {
        Ok(_) => Ok(0),
        Err(e) => {
            let errno = e.raw_os_error().unwrap_or(0);
            tracing::debug!("Assuming short write, got: {e}");
            // 778 == SQLITE_IOERR_WRITE
            Ok(778)
        }
    }
}

fn truncate(mut caller: Caller<'_, State>, file: i32, size: i64) -> anyhow::Result<i32> {
    let memory = get_memory(&mut caller);
    let (_memory, _state) = memory.data_and_store_mut(&mut caller);
    tracing::debug!("HOST TRUNCATE CALLED");
    Ok(0)
}

fn sync(mut caller: Caller<'_, State>, file: i32, flags: i32) -> anyhow::Result<i32> {
    let memory = get_memory(&mut caller);
    let (_memory, _state) = memory.data_and_store_mut(&mut caller);
    tracing::debug!("HOST SYNC CALLED");
    Ok(0)
}

fn file_size(mut caller: Caller<'_, State>, file: i32, size: i32) -> anyhow::Result<i32> {
    let memory = get_memory(&mut caller);
    let (_memory, _state) = memory.data_and_store_mut(&mut caller);
    tracing::debug!("HOST FILE SIZE CALLED");
    Ok(0)
}

fn lock(mut caller: Caller<'_, State>, file: i32, lock: i32) -> anyhow::Result<i32> {
    let memory = get_memory(&mut caller);
    let (_memory, _state) = memory.data_and_store_mut(&mut caller);
    tracing::debug!("HOST LOCK CALLED");
    Ok(0)
}

fn unlock(mut caller: Caller<'_, State>, file: i32, lock: i32) -> anyhow::Result<i32> {
    let memory = get_memory(&mut caller);
    let (_memory, _state) = memory.data_and_store_mut(&mut caller);
    tracing::debug!("HOST UNLOCK CALLED");
    Ok(0)
}

fn check_reserved_lock(
    mut caller: Caller<'_, State>,
    file: i32,
    reserved_lock: i32,
) -> anyhow::Result<i32> {
    let memory = get_memory(&mut caller);
    let (_memory, _state) = memory.data_and_store_mut(&mut caller);
    tracing::debug!("HOST CHECK RESERVED LOCK CALLED");
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

    tracing::debug!("HOST FILE CONTROL CALLED: op={op}, arg={arg}");
    // 12 == SQLITE_NOTFOUND
    Ok(12)
}

fn sector_size(mut caller: Caller<'_, State>, _file: i32) -> anyhow::Result<i32> {
    tracing::debug!("HOST SECTOR SIZE CALLED");
    Ok(512)
}

fn device_characteristics(mut caller: Caller<'_, State>, _file: i32) -> anyhow::Result<i32> {
    /*
       #define SQLITE_IOCAP_ATOMIC                 0x00000001
       #define SQLITE_IOCAP_ATOMIC512              0x00000002
       #define SQLITE_IOCAP_ATOMIC1K               0x00000004
       #define SQLITE_IOCAP_ATOMIC2K               0x00000008
       #define SQLITE_IOCAP_ATOMIC4K               0x00000010
       #define SQLITE_IOCAP_ATOMIC8K               0x00000020
       #define SQLITE_IOCAP_ATOMIC16K              0x00000040
       #define SQLITE_IOCAP_ATOMIC32K              0x00000080
       #define SQLITE_IOCAP_ATOMIC64K              0x00000100
       #define SQLITE_IOCAP_SAFE_APPEND            0x00000200
       #define SQLITE_IOCAP_SEQUENTIAL             0x00000400
       #define SQLITE_IOCAP_UNDELETABLE_WHEN_OPEN  0x00000800
       #define SQLITE_IOCAP_POWERSAFE_OVERWRITE    0x00001000
       #define SQLITE_IOCAP_IMMUTABLE              0x00002000
       #define SQLITE_IOCAP_BATCH_ATOMIC           0x00004000
    */
    // ATOMIC | SAFE_APPEND | SEQUENTIAL
    tracing::debug!("dEVICE CHARACTERISTICS CALLED");
    Ok(0x00000001 | 0x00000200 | 0x00000400)
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
