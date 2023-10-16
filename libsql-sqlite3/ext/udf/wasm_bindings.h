/* SPDX-License-Identifier: MIT */
#ifdef LIBSQL_ENABLE_WASM_RUNTIME

#ifndef LIBSQL_WASM_BINDINGS_H
#define LIBSQL_WASM_BINDINGS_H

typedef struct libsql_wasm_engine_t libsql_wasm_engine_t;
typedef struct libsql_wasm_module_t libsql_wasm_module_t;

typedef struct libsql_wasm_udf_api {
    int (*libsql_value_type)(sqlite3_value*);
    int (*libsql_value_int)(sqlite3_value*);
    double (*libsql_value_double)(sqlite3_value*);
    const unsigned char *(*libsql_value_text)(sqlite3_value*);
    const void *(*libsql_value_blob)(sqlite3_value*);
    int (*libsql_value_bytes)(sqlite3_value*);
    void (*libsql_result_error)(sqlite3_context*, const char*, int);
    void (*libsql_result_error_nomem)(sqlite3_context*);
    void (*libsql_result_int)(sqlite3_context*, int);
    void (*libsql_result_double)(sqlite3_context*, double);
    void (*libsql_result_text)(sqlite3_context*, const char*, int, void(*)(void*));
    void (*libsql_result_blob)(sqlite3_context*, const void*, int, void(*)(void*));
    void (*libsql_result_null)(sqlite3_context*);
    void *(*libsql_malloc)(int);
    void (*libsql_free)(void *);
} libsql_wasm_udf_api;

/*
** Runs a WebAssembly user-defined function.
** Additional data can be accessed via sqlite3_user_data(context)
*/
void libsql_run_wasm(struct libsql_wasm_udf_api *api, sqlite3_context *context,
    libsql_wasm_engine_t *engine, libsql_wasm_module_t *module, const char *func_name, int argc, sqlite3_value **argv);

/*
** Compiles a WebAssembly module. Can accept both .wat and binary Wasm format, depending on the implementation.
** err_msg_buf needs to be deallocated with libsql_free_wasm_module.
*/
libsql_wasm_module_t *libsql_compile_wasm_module(libsql_wasm_engine_t* engine, const char *pSrcBody, int nBody,
    void *(*alloc_err_buf)(unsigned long long), char **err_msg_buf);

/*
** Frees a module allocated with libsql_compile_wasm_module
*/
void libsql_free_wasm_module(void *module);

/*
** Creates a new wasm engine
*/
libsql_wasm_engine_t *libsql_wasm_engine_new();
void libsql_wasm_engine_free(libsql_wasm_engine_t *);

#endif //LIBSQL_WASM_BINDINGS_H
#endif //LIBSQL_ENABLE_WASM_RUNTIME
