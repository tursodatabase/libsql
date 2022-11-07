#ifdef LIBSQL_ENABLE_WASM_RUNTIME

#include "ext/udf/wasm_bindings.h"
#include <wasm.h>
#include <wasmtime.h>
#include <wasmtime/error.h>

static int maybe_handle_wasm_error(sqlite3_context *context, wasmtime_error_t *error) {
  if (error) {
    wasm_name_t message;
    wasmtime_error_message(error, &message);
    sqlite3_result_error(context, message.data, -1);
    wasm_byte_vec_delete(&message);
    return 1;
  }
  return 0;
}

void libsql_run_wasm(libsql_wasm_udf_api *api, sqlite3_context *context, libsql_wasm_engine_t *engine,
    libsql_wasm_module_t *module, const char *func_name, int argc, sqlite3_value **argv) {
  assert(engine);
  wasmtime_store_t *store = wasmtime_store_new((wasm_engine_t*)engine, NULL, NULL);
  wasmtime_context_t *wasm_ctx = wasmtime_store_context(store);

  wasm_trap_t *trap = NULL;
  wasmtime_instance_t instance;
  wasmtime_error_t *error = wasmtime_instance_new(wasm_ctx, (wasmtime_module_t*)module, NULL, 0, &instance, &trap);
  if (maybe_handle_wasm_error(context, error)) {
    return;
  }

  // Look up the target function
  wasmtime_extern_t func;
  bool ok = wasmtime_instance_export_get(wasm_ctx, &instance, func_name, strlen(func_name), &func);
  if (!ok) {
    api->libsql_result_error(context, "Failed to extract function from the Wasm module", -1);
    return;
  }
  if (func.kind != WASMTIME_EXTERN_FUNC) {
    api->libsql_result_error(context, "Found exported symbol, but it's not a function", -1);
    return;
  }

  wasmtime_extern_t item;
  ok = wasmtime_instance_export_get(wasm_ctx, &instance, "memory", 6, &item);
  if (!ok || item.kind != WASMTIME_EXTERN_MEMORY) {
    api->libsql_result_error(context, "Failed to extract memory from the Wasm module", -1);
    return;
  }
  wasmtime_memory_t *mem = &item.of.memory;
  size_t mem_size = wasmtime_memory_data_size(wasm_ctx, mem);
  size_t mem_offset = mem_size; // next free offset to write at
  char *mem_base = (char *)wasmtime_memory_data(wasm_ctx, mem);

  wasmtime_val_t params[argc];
  for (unsigned i = 0; i < argc; ++i) {
    u8 type = api->libsql_value_type(argv[i]);
    switch (type) {
    case SQLITE_INTEGER:
      params[i].kind = WASMTIME_I64;
      params[i].of.i64 = api->libsql_value_int(argv[i]);
      break;
    case SQLITE_FLOAT:
      params[i].kind = WASMTIME_F64;
      params[i].of.f64 = api->libsql_value_double(argv[i]);
      break;
    case SQLITE_BLOB: {
      const void *blob = api->libsql_value_blob(argv[i]);
      int blob_len = api->libsql_value_bytes(argv[i]);

      if (mem_offset + blob_len + 1 + 4 > mem_size) {
        int delta = (blob_len + 1 + 4 + 65535) / 65536;
        error = wasmtime_memory_grow(wasm_ctx, mem, delta, &mem_size);
        if (maybe_handle_wasm_error(context, error)) {
          return;
        }
        mem_base = wasmtime_memory_data(wasm_ctx, mem);
        mem_size += delta * 65536;
      }
      // blob is encoded as: [1 byte of type information][4 bytes of size, big endian][data]
      mem_base[mem_offset] = type;
      sqlite3Put4byte(mem_base + mem_offset + 1, blob_len);
      memcpy(mem_base + mem_offset + 1 + 4, blob, blob_len);

      params[i].kind = WASMTIME_I32; // pointer
      params[i].of.i32 = mem_offset;

      mem_offset += blob_len + 4 + 1;
      break;
    }
    case SQLITE_TEXT: {
      const char *text = api->libsql_value_text(argv[i]);
      int text_len = api->libsql_value_bytes(argv[i]);

      if (mem_offset + text_len + 1 > mem_size) {
        int delta = (text_len + 1 + 65535) / 65536;
        error = wasmtime_memory_grow(wasm_ctx, mem, delta, &mem_size);
        if (maybe_handle_wasm_error(context, error)) {
          return;
        }
        mem_base = wasmtime_memory_data(wasm_ctx, mem);
        mem_size += delta * 65536;
      }
      // text is encoded as: [1 byte of type information][data][null terminator]
      mem_base[mem_offset] = type;
      memcpy(mem_base + mem_offset + 1, text, text_len);
      mem_base[mem_offset + 1 + text_len] = '\0';

      params[i].kind = WASMTIME_I32; // pointer
      params[i].of.i32 = mem_offset;

      mem_offset += text_len + 2;
      break;
    }
    case SQLITE_NULL:
      if (mem_offset + 1 > mem_size) {
        error = wasmtime_memory_grow(wasm_ctx, mem, 1, &mem_size);
        if (maybe_handle_wasm_error(context, error)) {
          return;
        }
        mem_base = wasmtime_memory_data(wasm_ctx, mem);
        mem_size += 65536;
      }
      // null is encoded as: [1 byte of type information]
      mem_base[mem_offset] = type;

      params[i].kind = WASMTIME_I32; // pointer
      params[i].of.i32 = mem_offset;

      mem_offset++;
      break;
    }
  }

  wasmtime_val_t results[1];
  error = wasmtime_func_call(wasm_ctx, &func.of.func, params, argc, results, 1, &trap);
  if (maybe_handle_wasm_error(context, error)) {
    return;
  }
  switch (results[0].kind) {
  case WASMTIME_I64:
    api->libsql_result_int(context, results[0].of.i64);
    break;
  case WASMTIME_F64:
    api->libsql_result_double(context, results[0].of.f64);
    break;
  case WASMTIME_I32: {
    char type = mem_base[results[0].of.i32];
    switch (type) {
    case SQLITE_TEXT: {
      const char *wasm_result = (const char *)mem_base + results[0].of.i32 + 1;
      size_t wasm_result_len = strlen(wasm_result);
      char *result = sqlite3Malloc(wasm_result_len + 1);
      if (!result) {
        api->libsql_result_error_nomem(context);
        return;
      }
      memcpy(result, wasm_result, wasm_result_len);
      api->libsql_result_text(context, result, wasm_result_len, sqlite3_free);
      break;
    }
    case SQLITE_BLOB: {
      void *wasm_result = mem_base + results[0].of.i32 + 1;
      int wasm_result_len = sqlite3Get4byte(wasm_result);
      wasm_result += 4;
      if (wasm_result_len > 2*1024*1024) {
        api->libsql_result_error_nomem(context);
        return;
      }
      char *result = sqlite3Malloc(wasm_result_len);
      if (!result) {
        api->libsql_result_error_nomem(context);
        return;
      }
      memcpy(result, wasm_result, wasm_result_len);
      api->libsql_result_blob(context, result, wasm_result_len, sqlite3_free);
      break;
    }
    case SQLITE_NULL:
      api->libsql_result_null(context);
      break;
    default:
      api->libsql_result_error(context, "Wasm function returned malformed result type", -1);
    }
    break;
  }
  default:
    api->libsql_result_error(context, "Wasm function returned unsupported result type", -1);
  }
}

void libsql_free_wasm_module(void *module) {
  wasmtime_module_delete(*(wasmtime_module_t **)module);
}

libsql_wasm_engine_t *libsql_wasm_engine_new() {
  return (libsql_wasm_engine_t*)wasm_engine_new();
}

libsql_wasm_module_t *libsql_compile_wasm_module(libsql_wasm_engine_t* engine, const char *pSrcBody, int nBody,
    void *(*alloc_err_buf)(unsigned long long), char **err_msg_buf) {
  wasm_byte_vec_t compiled_wasm;
  wasmtime_error_t *error = NULL;

  int source_already_compiled = (nBody >= 4
                                 && pSrcBody[0] == '\0'
                                 && pSrcBody[1] == 'a'
                                 && pSrcBody[2] == 's'
                                 && pSrcBody[3] == 'm');
  if (source_already_compiled) {
    compiled_wasm.data = (char *)pSrcBody;
    compiled_wasm.size = nBody;
  } else {
    error = wasmtime_wat2wasm(pSrcBody, nBody, &compiled_wasm);
    if (error) {
      char *zEscapedBody = sqlite3_malloc(nBody + 1);
      memcpy(zEscapedBody, pSrcBody, nBody + 1);
      sqlite3Dequote(zEscapedBody);
      error = wasmtime_wat2wasm(zEscapedBody, sqlite3Strlen30(zEscapedBody), &compiled_wasm);
      sqlite3_free(zEscapedBody);
      if (error) {
        if (err_msg_buf) {
          wasm_byte_vec_t err_str;
          wasmtime_error_message(error, &err_str);
          *err_msg_buf = sqlite3_malloc(err_str.size + 1);
          memcpy(*err_msg_buf, err_str.data, err_str.size);
          (*err_msg_buf)[err_str.size] = '\0';
          wasm_byte_vec_delete(&err_str);
        }
        return NULL;
      }
    }
  }

  wasmtime_module_t *module = NULL;
  error = wasmtime_module_new((wasm_engine_t*)engine, (uint8_t *)compiled_wasm.data, compiled_wasm.size, &module);
  if (!source_already_compiled) {
    wasm_byte_vec_delete(&compiled_wasm);
  }
  if (error) {
    if (err_msg_buf) {
          wasm_byte_vec_t err_str;
          wasmtime_error_message(error, &err_str);
          *err_msg_buf = sqlite3_malloc(err_str.size + 1);
          memcpy(*err_msg_buf, err_str.data, err_str.size);
          (*err_msg_buf)[err_str.size] = '\0';
          wasm_byte_vec_delete(&err_str);
    }
    return NULL;
  }
  return (libsql_wasm_module_t*)module;
}

void libsql_wasm_free_msg_buf(char *err_msg_buf) {
  sqlite3_free(err_msg_buf);
}

#endif
