#if defined(LIBSQL_ENABLE_WASM_RUNTIME) && defined(LIBSQL_ENABLE_WASM_RUNTIME_WASMEDGE)

#include "sqliteInt.h"
#include "wasm_bindings.h"
#include <wasmedge/wasmedge.h>

void libsql_run_wasm(libsql_wasm_udf_api *api, sqlite3_context *context, libsql_wasm_engine_t *engine,
    libsql_wasm_module_t *module, const char *func_name, int argc, sqlite3_value **argv) {

  WasmEdge_VMContext *ctx = (WasmEdge_VMContext *)module;


  WasmEdge_Result res = WasmEdge_VMInstantiate(ctx);
  if (!WasmEdge_ResultOK(res)) {
      sqlite3_result_error(context, "Instantiation failed", -1);
      return;
  }

  const WasmEdge_ModuleInstanceContext* instance_ctx = WasmEdge_VMGetActiveModule(ctx);
  WasmEdge_String mem_name = WasmEdge_StringCreateByCString("memory");
  WasmEdge_MemoryInstanceContext* mem_ctx = WasmEdge_ModuleInstanceFindMemory(instance_ctx, mem_name);
  WasmEdge_StringDelete(mem_name);

  WasmEdge_Value params[argc];
  WasmEdge_Value results[1];
  WasmEdge_Value malloc_param[1];

  int mem_size = WasmEdge_MemoryInstanceGetPageSize(mem_ctx) * 65536;
  int mem_offset = mem_size;

  for (unsigned i = 0; i < argc; ++i) {
    u8 type = sqlite3_value_type(argv[i]);
    switch (type) {
    case SQLITE_INTEGER:
      params[i] = WasmEdge_ValueGenI64(sqlite3_value_int(argv[i]));
      break;
    case SQLITE_FLOAT:
      params[i] = WasmEdge_ValueGenI64(sqlite3_value_double(argv[i]));
      break;
    case SQLITE_TEXT: {
      int text_len = sqlite3_value_bytes(argv[i]);
      const char *text = sqlite3_value_text(argv[i]);

      malloc_param[0] = WasmEdge_ValueGenI32(text_len + 2);
      WasmEdge_String wasmedge_func_name = WasmEdge_StringCreateByCString("libsql_malloc");
      res = WasmEdge_VMExecute(ctx, wasmedge_func_name, malloc_param, 1, results, 1);
      WasmEdge_StringDelete(wasmedge_func_name);
      if (!WasmEdge_ResultOK(res)) {
        sqlite3_result_error(context, "Execution failed", -1);
        return;
      }
      mem_offset = WasmEdge_ValueGetI32(results[0]);

      u8 *data = WasmEdge_MemoryInstanceGetPointer(mem_ctx, mem_offset, text_len + 2);
      data[0] = type;
      memcpy(data + 1, text, text_len);
      data[1 + text_len] = '\0';
      params[i] = WasmEdge_ValueGenI32(mem_offset);
      break;
    }
    case SQLITE_BLOB: {
      int blob_len = sqlite3_value_bytes(argv[i]);
      const void *blob = sqlite3_value_blob(argv[i]);
      
      malloc_param[0] = WasmEdge_ValueGenI32(blob_len + 5);
      WasmEdge_String wasmedge_func_name = WasmEdge_StringCreateByCString("libsql_malloc");
      res = WasmEdge_VMExecute(ctx, wasmedge_func_name, malloc_param, 1, results, 1);
      WasmEdge_StringDelete(wasmedge_func_name);
      if (!WasmEdge_ResultOK(res)) {
        sqlite3_result_error(context, "Execution failed", -1);
        return;
      }
      mem_offset = WasmEdge_ValueGetI32(results[0]);

      u8 *data = WasmEdge_MemoryInstanceGetPointer(mem_ctx, mem_offset, blob_len + 5);
      data[0] = type;
      sqlite3Put4byte(data + 1, blob_len);
      memcpy(data + 1 + 4, blob, blob_len);
      params[i] = WasmEdge_ValueGenI32(mem_offset);
      break;
    }
    case SQLITE_NULL:

      malloc_param[0] = WasmEdge_ValueGenI32(1);
      WasmEdge_String wasmedge_func_name = WasmEdge_StringCreateByCString("libsql_malloc");
      res = WasmEdge_VMExecute(ctx, wasmedge_func_name, malloc_param, 1, results, 1);
      WasmEdge_StringDelete(wasmedge_func_name);
      if (!WasmEdge_ResultOK(res)) {
        sqlite3_result_error(context, "Execution failed", -1);
        return;
      }
      mem_offset = WasmEdge_ValueGetI32(results[0]);

      u8 *data = WasmEdge_MemoryInstanceGetPointer(mem_ctx, mem_offset, 1);
      data[0] = type;
      params[i] = WasmEdge_ValueGenI32(mem_offset);
      break;
    }
  }

  WasmEdge_String wasmedge_func_name = WasmEdge_StringCreateByCString(func_name);
  res = WasmEdge_VMExecute(ctx, wasmedge_func_name, params, argc, results, 1);
  if (!WasmEdge_ResultOK(res)) {
      sqlite3_result_error(context, "Execution failed", -1);
      WasmEdge_StringDelete(wasmedge_func_name);
      return;
  }
  WasmEdge_StringDelete(wasmedge_func_name);

  switch (results[0].Type) {
  case WasmEdge_ValType_I64:
    sqlite3_result_int(context, WasmEdge_ValueGetI64(results[0]));
    break;
  case WasmEdge_ValType_F64:
    sqlite3_result_double(context, WasmEdge_ValueGetF64(results[0]));
    break;
  case WasmEdge_ValType_I32: {
    int type_offset = WasmEdge_ValueGetI32(results[0]);
    char *type_ptr = WasmEdge_MemoryInstanceGetPointer(mem_ctx, type_offset, 1);
    if (!type_ptr) {
      sqlite3_result_error(context, "Unexpected end of Wasm memory when trying to fetch results", -1);
      return;
    }
    char type = *type_ptr;
    switch (type) {
    case SQLITE_TEXT: {
      const char *wasm_result = type_ptr + 1;
      size_t wasm_result_len = strlen(wasm_result);
      char *result = sqlite3Malloc(wasm_result_len + 1);
      if (!result) {
        sqlite3_result_error_nomem(context);
        return;
      }
      memcpy(result, wasm_result, wasm_result_len);
      sqlite3_result_text(context, result, wasm_result_len, sqlite3_free);
      break;
    }
    case SQLITE_BLOB: {
      void *wasm_result = type_ptr + 1;
      int wasm_result_len = sqlite3Get4byte(wasm_result);
      wasm_result += 4;
      if (wasm_result_len > 2*1024*1024) {
        sqlite3_result_error_nomem(context);
        return;
      }
      char *result = sqlite3Malloc(wasm_result_len);
      if (!result) {
        sqlite3_result_error_nomem(context);
        return;
      }
      memcpy(result, wasm_result, wasm_result_len);
      sqlite3_result_blob(context, result, wasm_result_len, sqlite3_free);
      break;
    }
    case SQLITE_NULL:
      sqlite3_result_null(context);
      break;
    default:
      sqlite3_result_error(context, "Wasm function returned malformed result type", -1);
    }
  }
  break;
  default:
    fprintf(stderr, "res %d\n", results[0].Type);
    sqlite3_result_error(context, "Wasm function returned an unsupported result type", -1);
  }
}

void libsql_free_wasm_module(void *ctx) {
  WasmEdge_VMDelete(*(WasmEdge_VMContext **)ctx);
}

libsql_wasm_engine_t *libsql_wasm_engine_new() {
  WasmEdge_PluginLoadWithDefaultPaths();
  return NULL;
}

void libsql_wasm_engine_free(libsql_wasm_engine_t *eng) {
}

libsql_wasm_module_t *libsql_compile_wasm_module(libsql_wasm_engine_t* engine, const char *pSrcBody, int nBody,
    void *(*alloc_err_buf)(unsigned long long), char **err_msg_buf) {

  if (nBody < 4 || memcmp(pSrcBody, "\0asm", 4) != 0) {
    *err_msg_buf = sqlite3_mprintf("Magic header was not detected. "
        "WasmEdge backend supports compiled binary Wasm format only. "
        "If you passed WAT source, please transform it with wat2wasm or any similar tool");
    return NULL;
  }

  WasmEdge_ConfigureContext *ConfCxt = WasmEdge_ConfigureCreate();
  WasmEdge_ConfigureAddHostRegistration(ConfCxt, WasmEdge_HostRegistration_Wasi);

  WasmEdge_VMContext *ctx = WasmEdge_VMCreate(ConfCxt, NULL);
  WasmEdge_ConfigureDelete(ConfCxt);
  
  WasmEdge_Result res = WasmEdge_VMLoadWasmFromBuffer(ctx, (const uint8_t *)pSrcBody, nBody);
  if (!WasmEdge_ResultOK(res)) {
    *err_msg_buf = sqlite3_mprintf("Compilation failed: %s", WasmEdge_ResultGetMessage(res));
    return NULL;
  }
  res = WasmEdge_VMValidate(ctx);
  if (!WasmEdge_ResultOK(res)) {
    *err_msg_buf = sqlite3_mprintf("Validation failed: %s", WasmEdge_ResultGetMessage(res));
    return NULL;
  }
  return (libsql_wasm_module_t*)ctx;
}

void libsql_wasm_free_msg_buf(char *err_msg_buf) {
  sqlite3_free(err_msg_buf);
}

#endif
