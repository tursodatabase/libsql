use std::ffi::{c_char, c_void, CStr};
use wasmtime::{Config, Engine, Instance, Module, Store, Val};

const LIBSQL_INTEGER: i8 = 1;
const LIBSQL_FLOAT: i8 = 2;
const LIBSQL_TEXT: i8 = 3;
const LIBSQL_BLOB: i8 = 4;
const LIBSQL_NULL: i8 = 5;

fn maybe_set_err_buf(
    err_buf: *mut *const u8,
    err_str: String,
    alloc_err: unsafe extern "C" fn(u64) -> *mut u8,
) {
    if !err_buf.is_null() {
        let err_ptr = unsafe { alloc_err(err_str.len() as u64 + 1) };
        unsafe { std::slice::from_raw_parts_mut(err_ptr, err_str.len()) }
            .copy_from_slice(err_str.as_bytes());
        unsafe { *err_buf = err_ptr as *const u8 };
    }
}

#[no_mangle]
pub fn libsql_compile_wasm_module(
    engine: *const wasmtime::Engine,
    p_src_body: *const u8,
    n_body: i32,
    alloc_err: unsafe extern "C" fn(u64) -> *mut u8,
    err_msg_buf: *mut *const u8,
) -> *const c_void {
    let src_body: &[u8] = unsafe { std::slice::from_raw_parts(p_src_body, n_body as usize) };

    let module = match Module::new(unsafe { &*engine }, src_body) {
        Ok(m) => m,
        Err(orig_e) => {
            // If compilation failed, let's assume it's unquoted .wat and retry
            let src_body_str: &str = match std::str::from_utf8(src_body) {
                Ok(src) => src,
                Err(e) => {
                    maybe_set_err_buf(
                        err_msg_buf,
                        format!(
                            "Failed to compile module: {}, and it's not valid .wat either: {}",
                            orig_e, e
                        ),
                        alloc_err,
                    );
                    return std::ptr::null() as *const c_void;
                }
            };
            if src_body_str.len() < 2 {
                maybe_set_err_buf(
                    err_msg_buf,
                    format!("Failed to compile module: {}", orig_e),
                    alloc_err,
                );
                return std::ptr::null() as *const c_void;
            }
            let src_body_dequoted =
                String::from(&src_body_str[1..src_body_str.len() - 2]).replace("''", "'");
            match Module::new(unsafe { &*engine }, src_body_dequoted.as_bytes()) {
                Ok(m) => m,
                Err(e) => {
                    maybe_set_err_buf(
                        err_msg_buf,
                        format!("Failed to compile .wat module: {}", e),
                        alloc_err,
                    );
                    return std::ptr::null();
                }
            }
        }
    };
    let module = Box::new(module);
    let module_ptr = &*module as *const Module as *const c_void;
    std::mem::forget(module);
    module_ptr
}

#[no_mangle]
pub fn libsql_wasm_engine_new() -> *const c_void {
    let engine = match Engine::new(&Config::new()) {
        Ok(eng) => eng,
        Err(_) => return std::ptr::null() as *const c_void,
    };
    let engine = Box::new(engine);
    let engine_ptr = &*engine as *const Engine as *const c_void;
    std::mem::forget(engine);
    engine_ptr
}

#[repr(C)]
pub struct libsql_wasm_udf_api {
    libsql_value_type: unsafe extern "C" fn(*const c_void) -> i32,
    libsql_value_int: unsafe extern "C" fn(*const c_void) -> i32,
    libsql_value_double: unsafe extern "C" fn(*const c_void) -> f64,
    libsql_value_text: unsafe extern "C" fn(*const c_void) -> *const u8,
    libsql_value_blob: unsafe extern "C" fn(*const c_void) -> *const c_void,
    libsql_value_bytes: unsafe extern "C" fn(*const c_void) -> i32,
    libsql_result_error: unsafe extern "C" fn(*const c_void, *const u8, i32),
    libsql_result_error_nomem: unsafe extern "C" fn(*const c_void),
    libsql_result_int: unsafe extern "C" fn(*const c_void, i32),
    libsql_result_double: unsafe extern "C" fn(*const c_void, f64),
    libsql_result_text: unsafe extern "C" fn(*const c_void, *const u8, i32, *const c_void),
    libsql_result_blob: unsafe extern "C" fn(*const c_void, *const c_void, i32, *const c_void),
    libsql_result_null: unsafe extern "C" fn(*const c_void),
    libsql_malloc: unsafe extern "C" fn(i32) -> *mut c_void,
    libsql_free: unsafe extern "C" fn(*mut c_void),
}

fn alloc_slice(api: *const libsql_wasm_udf_api, s: &[u8]) -> *const c_void {
    let len = s.len();
    let ptr = unsafe { ((*api).libsql_malloc)(len as i32) };
    unsafe { std::slice::from_raw_parts_mut(ptr as *mut u8, len) }.copy_from_slice(s);
    ptr as *const c_void
}

#[no_mangle]
pub fn libsql_run_wasm(
    api: *const libsql_wasm_udf_api,
    libsql_ctx: *const c_void,
    engine: *mut Engine,
    module: *mut Module,
    func_name: *const u8,
    argc: i32,
    argv: *mut *mut c_void,
) {
    let mut store = Store::new(unsafe { &*engine }, ());
    let instance = match Instance::new(&mut store, unsafe { &*module }, &[]) {
        Ok(inst) => inst,
        Err(e) => {
            let err = format!("Creating instance failed: {}", e);
            unsafe {
                ((*api).libsql_result_error)(
                    libsql_ctx,
                    err.as_ptr() as *const u8,
                    err.len() as i32,
                );
            }
            return;
        }
    };
    let func_name: &str = match unsafe { CStr::from_ptr(func_name as *const c_char) }.to_str() {
        Ok(s) => s,
        Err(e) => {
            let err = format!("Function name is not valid utf-8: {}", e);
            unsafe {
                ((*api).libsql_result_error)(
                    libsql_ctx,
                    err.as_ptr() as *const u8,
                    err.len() as i32,
                );
            }
            return;
        }
    };
    let func = match instance.get_func(&mut store, func_name) {
        Some(f) => f,
        None => {
            let err = format!("Function {} not found in Wasm module", func_name);
            unsafe {
                ((*api).libsql_result_error)(
                    libsql_ctx,
                    err.as_ptr() as *const u8,
                    err.len() as i32,
                );
            }
            return;
        }
    };
    let memory = match instance.get_memory(&mut store, "memory") {
        Some(mem) => mem,
        None => {
            unsafe {
                ((*api).libsql_result_error)(
                    libsql_ctx,
                    "Memory \"memory\" not found in wasm module".as_ptr() as *const u8,
                    -1,
                );
            }
            return;
        }
    };

    let mut mem_size = memory.size(&mut store) as usize;
    let mut mem_offset = mem_size;

    let mut vals: Vec<Val> = Vec::new();
    for i in 0..argc {
        let arg = unsafe { *argv.offset(i as isize) };
        match unsafe { ((*api).libsql_value_type)(arg) } as i8 {
            LIBSQL_INTEGER => vals.push(Val::I64(unsafe { ((*api).libsql_value_int)(arg) } as i64)),
            LIBSQL_FLOAT => vals.push(Val::F64(
                unsafe { ((*api).libsql_value_double)(arg) }.to_bits(),
            )),
            LIBSQL_TEXT => {
                let text: &str = unsafe {
                    match CStr::from_ptr(((*api).libsql_value_text)(arg) as *const c_char).to_str()
                    {
                        Ok(s) => s,
                        Err(e) => {
                            let err = format!("Function name is not valid utf-8: {}", e);
                            ((*api).libsql_result_error)(
                                libsql_ctx,
                                err.as_ptr() as *const u8,
                                err.len() as i32,
                            );
                            return;
                        }
                    }
                };
                let text_len = unsafe { ((*api).libsql_value_bytes)(arg) } as usize;

                if mem_offset + text_len + 2 > mem_size {
                    let delta = (text_len + 2 + 65535) / 65536;
                    match memory.grow(&mut store, delta as u64) {
                        Ok(_) => (),
                        Err(e) => {
                            let err = format!("Failed to grow memory: {}", e);
                            unsafe {
                                ((*api).libsql_result_error)(
                                    libsql_ctx,
                                    err.as_ptr() as *const u8,
                                    err.len() as i32,
                                );
                            }
                            return;
                        }
                    };
                    mem_size += delta * 65536;
                }
                let data = memory.data_mut(&mut store);
                data[mem_offset] = LIBSQL_TEXT as u8;
                data[mem_offset + 1..mem_offset + 1 + text_len].copy_from_slice(text.as_bytes());
                data[mem_offset + 1 + text_len] = 0;

                vals.push(Val::I32(mem_offset as i32));
                mem_offset += text_len + 2;
            }
            LIBSQL_BLOB => {
                let blob_len = unsafe { ((*api).libsql_value_bytes)(arg) } as usize;
                let blob: &[u8] = unsafe {
                    std::slice::from_raw_parts(
                        ((*api).libsql_value_blob)(arg) as *const u8,
                        blob_len,
                    )
                };
                let blob_len_i32 = blob_len as i32;

                if mem_offset + blob_len + 1 + 4 > mem_size {
                    let delta = (blob_len + 1 + 4 + 65535) / 65536;
                    match memory.grow(&mut store, delta as u64) {
                        Ok(_) => (),
                        Err(e) => {
                            let err = format!("Failed to grow memory: {}", e);
                            unsafe {
                                ((*api).libsql_result_error)(
                                    libsql_ctx,
                                    err.as_ptr() as *const u8,
                                    err.len() as i32,
                                );
                            }
                            return;
                        }
                    };
                    mem_size += delta * 65536;
                }
                let data = memory.data_mut(&mut store);
                data[mem_offset] = LIBSQL_BLOB as u8;
                data[mem_offset + 1..mem_offset + 1 + 4]
                    .copy_from_slice(&blob_len_i32.to_be_bytes());
                data[mem_offset + 1 + 4..mem_offset + 1 + 4 + blob_len].copy_from_slice(blob);

                vals.push(Val::I32(mem_offset as i32));
                mem_offset += blob_len + 4 + 1;
            }
            LIBSQL_NULL => {
                if mem_offset + 1 > mem_size {
                    match memory.grow(&mut store, 1_u64) {
                        Ok(_) => (),
                        Err(e) => {
                            let err = format!("Failed to grow memory: {}", e);
                            unsafe {
                                ((*api).libsql_result_error)(
                                    libsql_ctx,
                                    err.as_ptr() as *const u8,
                                    err.len() as i32,
                                );
                            }
                            return;
                        }
                    };
                    mem_size += 65536;
                }
                memory.data_mut(&mut store)[mem_offset] = LIBSQL_NULL as u8;

                vals.push(Val::I32(mem_offset as i32));
                mem_offset += 1;
            }
            _ => {
                unsafe {
                    ((*api).libsql_result_error)(
                        libsql_ctx,
                        "Unknown libSQL type".as_ptr() as *const u8,
                        19,
                    )
                }
                return;
            }
        }
    }

    let mut result = Val::null();
    match func.call(&mut store, &vals, std::slice::from_mut(&mut result)) {
        Ok(_) => (),
        Err(e) => {
            let err = format!("Calling function {} failed: {}", func_name, e);
            unsafe {
                ((*api).libsql_result_error)(
                    libsql_ctx,
                    err.as_ptr() as *const u8,
                    err.len() as i32,
                );
            }
            return;
        }
    };

    match result {
        Val::I64(v) => unsafe { ((*api).libsql_result_int)(libsql_ctx, v as i32) },
        Val::F64(v) => unsafe { ((*api).libsql_result_double)(libsql_ctx, f64::from_bits(v)) },
        Val::I32(v) => {
            let v = v as usize;
            match memory.data(&store)[v] as i8 {
                LIBSQL_TEXT => {
                    let result_str = unsafe {
                        CStr::from_ptr(
                            (memory.data(&store).as_ptr() as *const c_char).offset(v as isize + 1),
                        )
                    };
                    let result_ptr = alloc_slice(api, result_str.to_bytes_with_nul());
                    unsafe {
                        ((*api).libsql_result_text)(
                            libsql_ctx,
                            result_ptr as *const u8,
                            result_str.to_str().unwrap().len() as i32, // safe to unwrap, created in alloc_slice
                            (*api).libsql_free as *const c_void,
                        )
                    }
                }
                LIBSQL_BLOB => {
                    let blob_len = i32::from_be_bytes(
                        memory.data(&store)[v + 1..v + 1 + 4].try_into().unwrap(), // safe to unwrap, slice size == 4
                    );
                    let result_ptr = alloc_slice(
                        api,
                        &memory.data(&store)[v + 1 + 4..v + 1 + 4 + blob_len as usize],
                    );
                    unsafe {
                        ((*api).libsql_result_blob)(
                            libsql_ctx,
                            result_ptr as *const c_void,
                            blob_len,
                            (*api).libsql_free as *const c_void,
                        )
                    }
                }
                LIBSQL_NULL => unsafe { ((*api).libsql_result_null)(libsql_ctx) },
                _ => unsafe {
                    ((*api).libsql_result_error)(
                        libsql_ctx,
                        "Malformed result type byte".as_ptr() as *const u8,
                        26,
                    )
                },
            }
        }
        _ => unsafe {
            ((*api).libsql_result_error)(
                libsql_ctx,
                "Malformed result type".as_ptr() as *const u8,
                21,
            )
        },
    }
}

#[no_mangle]
pub fn libsql_free_wasm_module(module: *mut *mut Module) {
    unsafe { Box::from_raw(*module) };
}
