use crate::{memory, State};
// anyhow is used in wasmtime_wasi for error wrapping
use anyhow::Result;
use wasmtime::{Caller, Linker, Memory};

const SQLITE_DATAONLY: i32 = 0x00010;
const SQLITE_IOERR_READ: i32 = 266;
const SQLITE_IOERR_SHORT_READ: i32 = 522;
const SQLITE_IOERR_WRITE: i32 = 778;

const SQLITE_ACCESS_EXISTS: i32 = 0;
const SQLITE_ACCESS_READWRITE: i32 = 1;

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
    let file: &'static mut std::fs::File = unsafe { &mut *(file_fd as *mut std::fs::File) };

    tracing::debug!("Metadata: {:?}", file.metadata());
    file
}

fn open_fd(mut caller: Caller<'_, State>, name: i32, flags: i32) -> Result<i64> {
    let memory = get_memory(&mut caller);
    let memory = memory.data_mut(&mut caller);

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

fn delete(mut caller: Caller<'_, State>, _vfs: i32, name: i32, sync_dir: i32) -> Result<i32> {
    let memory = get_memory(&mut caller);
    let memory = memory.data_mut(&mut caller);

    let name = memory::read_cstr(memory, name)?;
    tracing::debug!("HOST DELETE: {name:?}, sync_dir={sync_dir}");

    let _ = std::fs::remove_file(&name);
    Ok(0)
}

fn access(
    mut caller: Caller<'_, State>,
    _vfs: i32,
    name: i32,
    flags: i32,
    res_out: i32,
) -> Result<i32> {
    let memory = get_memory(&mut caller);
    let memory = memory.data_mut(&mut caller);

    let name = memory::read_cstr(memory, name)?;
    tracing::debug!("HOST ACCESS: {name:?} {flags:x}");

    let res_out = memory::slice_mut(memory, res_out, 4)?;
    if flags == SQLITE_ACCESS_EXISTS {
        if std::fs::metadata(&name).is_ok() {
            res_out[0] = 1;
        } else {
            res_out[0] = 0;
        }
    } else if flags == SQLITE_ACCESS_READWRITE {
        if std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&name)
            .is_ok()
        {
            res_out[0] = 1;
        } else {
            res_out[0] = 0;
        }
    } else {
        res_out[0] = 0;
    }

    Ok(0)
}

fn full_pathname(
    mut caller: Caller<'_, State>,
    _vfs: i32,
    name: i32,
    n_out: i32,
    out: i32,
) -> Result<i32> {
    let memory = get_memory(&mut caller);
    let memory = memory.data_mut(&mut caller);

    let name = memory::read_cstr(memory, name)?;
    let out = memory::slice_mut(memory, out, n_out as usize)?;

    out[..name.len()].copy_from_slice(name.as_bytes());
    Ok(0)
}

fn randomness(mut caller: Caller<'_, State>, _vfs: i32, n_byte: i32, out: i32) -> Result<i32> {
    use rand::Rng;

    let memory = get_memory(&mut caller);
    let memory = memory.data_mut(&mut caller);

    let out = memory::slice_mut(memory, out, n_byte as usize)?;
    let mut rng = rand::thread_rng();
    rng.fill(out);

    tracing::debug!("HOST RANDOMNESS: {n_byte} {out:0x?}");
    Ok(0)
}

fn sleep(mut caller: Caller<'_, State>, _vfs: i32, microseconds: i32) -> Result<i32> {
    let memory = get_memory(&mut caller);
    let (_memory, _state) = memory.data_and_store_mut(&mut caller);

    tracing::debug!("HOST SLEEP: {microseconds}ms");
    std::thread::sleep(std::time::Duration::from_micros(microseconds as u64));
    Ok(0)
}

fn current_time(mut caller: Caller<'_, State>, _vfs: i32, out: i32) -> Result<i32> {
    let memory = get_memory(&mut caller);
    let memory = memory.data_mut(&mut caller);

    tracing::debug!("HOST CURRENT TIME");

    let out = memory::slice_mut(memory, out, 8)?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as f64;

    out[0..8].copy_from_slice(&now.to_le_bytes());
    Ok(0)
}

fn get_last_error(mut caller: Caller<'_, State>, _vfs: i32, i: i32, out: i32) -> Result<i32> {
    let memory = get_memory(&mut caller);
    let memory = memory.data_mut(&mut caller);

    tracing::debug!("HOST GET LAST ERROR: STUB");

    let out = memory::slice_mut(memory, out, i as usize)?;
    out[0] = 0;
    Ok(0)
}

fn current_time_64(mut caller: Caller<'_, State>, _vfs: i32, out: i32) -> Result<i32> {
    let memory = get_memory(&mut caller);
    let memory = memory.data_mut(&mut caller);

    tracing::debug!("HOST CURRENT TIME 64");

    let out = memory::slice_mut(memory, out, 8)?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();

    out[0..8].copy_from_slice(&now.to_le_bytes());

    Ok(0)
}

fn close(mut caller: Caller<'_, State>, file: i32) -> Result<i32> {
    let memory = get_memory(&mut caller);
    let memory = memory.data_mut(&mut caller);

    let file_fd = i64::from_le_bytes(
        memory[file as usize + 8..file as usize + 8 + 8]
            .try_into()
            .unwrap(),
    );
    let _file = unsafe { Box::from_raw(file_fd as *mut std::fs::File) };

    Ok(0)
}

fn read(mut caller: Caller<'_, State>, file: i32, buf: i32, amt: i32, offset: i64) -> Result<i32> {
    use std::io::{Read, Seek};

    let memory = get_memory(&mut caller);
    let memory = memory.data_mut(&mut caller);

    tracing::debug!("HOST READ CALLED: {amt} bytes starting at {offset}");

    let file = get_file(memory, file);
    file.seek(std::io::SeekFrom::Start(offset as u64))?;

    let buf = memory::slice_mut(memory, buf, amt as usize)?;
    match file.read_exact(buf) {
        Ok(_) => Ok(0),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            tracing::debug!("(short read)");
            // VFS layer expects filling the buffer with zeros on short reads
            buf.fill(0);
            Ok(SQLITE_IOERR_SHORT_READ)
        }
        Err(e) => {
            tracing::error!("read error: {e}");
            Ok(SQLITE_IOERR_READ)
        }
    }
}

fn write(mut caller: Caller<'_, State>, file: i32, buf: i32, amt: i32, offset: i64) -> Result<i32> {
    use std::io::{Seek, Write};

    let memory = get_memory(&mut caller);
    let memory = memory.data_mut(&mut caller);

    tracing::debug!("HOST WRITE CALLED: {amt} bytes starting at {offset}");

    let file = get_file(memory, file);
    file.seek(std::io::SeekFrom::Start(offset as u64))?;

    let buf = memory::slice(memory, buf, amt as usize)?;
    match file.write_all(buf) {
        Ok(_) => Ok(0),
        Err(e) => {
            tracing::error!("write error: {e}");
            Ok(SQLITE_IOERR_WRITE)
        }
    }
}

fn truncate(mut caller: Caller<'_, State>, file: i32, size: i64) -> Result<i32> {
    let memory = get_memory(&mut caller);
    let memory = memory.data_mut(&mut caller);

    let file = get_file(memory, file);
    file.set_len(size as u64)?;

    tracing::debug!("HOST TRUNCATE: {size} bytes");
    Ok(0)
}

fn sync(mut caller: Caller<'_, State>, file: i32, flags: i32) -> Result<i32> {
    let memory = get_memory(&mut caller);
    let memory = memory.data_mut(&mut caller);

    tracing::debug!("HOST SYNC: flags={flags:x}");

    let file = get_file(memory, file);
    if flags & SQLITE_DATAONLY != 0 {
        file.sync_data()?;
    } else {
        file.sync_all()?;
    }

    Ok(0)
}

fn file_size(mut caller: Caller<'_, State>, file: i32, size_ptr: i32) -> Result<i32> {
    let memory = get_memory(&mut caller);
    let memory = memory.data_mut(&mut caller);
    tracing::debug!("HOST FILE SIZE");

    let file = get_file(memory, file);
    let file_size = file.metadata()?.len() as i64;
    memory[size_ptr as usize..size_ptr as usize + 8].copy_from_slice(&file_size.to_le_bytes());

    Ok(0)
}

pub fn link(linker: &mut Linker<State>) -> Result<()> {
    // VFS methods required by sqlite3_vfs
    linker.func_wrap("libsql_host", "open_fd", open_fd)?;
    linker.func_wrap("libsql_host", "delete", delete)?;
    linker.func_wrap("libsql_host", "access", access)?;
    linker.func_wrap("libsql_host", "full_pathname", full_pathname)?;
    linker.func_wrap("libsql_host", "randomness", randomness)?;
    linker.func_wrap("libsql_host", "sleep", sleep)?;
    linker.func_wrap("libsql_host", "current_time", current_time)?;
    linker.func_wrap("libsql_host", "get_last_error", get_last_error)?;
    linker.func_wrap("libsql_host", "current_time_64", current_time_64)?;

    // IO methods required by sqlite3_io_methods
    linker.func_wrap("libsql_host", "close", close)?;
    linker.func_wrap("libsql_host", "read", read)?;
    linker.func_wrap("libsql_host", "write", write)?;
    linker.func_wrap("libsql_host", "truncate", truncate)?;
    linker.func_wrap("libsql_host", "sync", sync)?;
    linker.func_wrap("libsql_host", "file_size", file_size)?;

    // NOTICE: locking is handled as no-ops in the VFS layer,
    // it is expected to be handled by the upper layers at the moment.

    Ok(())
}
