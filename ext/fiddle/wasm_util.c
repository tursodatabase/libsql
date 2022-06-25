#include "sqlite3.h"
#include <stdlib.h> /*atexit()*/
/*
**  2022-06-25
**
**  The author disclaims copyright to this source code.  In place of a
**  legal notice, here is a blessing:
**
**  *   May you do good and not evil.
**  *   May you find forgiveness for yourself and forgive others.
**  *   May you share freely, never taking more than you give.
**
***********************************************************************
**
** Utility functions for use with the emscripten/WASM bits. These
** functions ARE NOT part of the sqlite3 public API. They are strictly
** for internal use by the JS/WASM bindings.
**
** This file is intended to be WASM-compiled together with sqlite3.c,
** e.g.:
**
**  emcc ... sqlite3.c wasm_util.c
*/

/** Result value of sqlite3_wasm_enum_json(). */
static char * zWasmEnum = 0;
/* atexit() handler to clean up any WASM-related state. */
static void sqlite3_wasm_cleanup(void){
  free(zWasmEnum);
}

/*
** Returns a string containing a JSON-format "enum" of C-level
** constants intended to be imported into the JS environment. The JSON
** is initialized the first time this function is called and that
** result is reused for all future calls and cleaned up via atexit().
** (If we didn't cache the result, it would be leaked by the JS glue
** code on each call during the WASM-to-JS conversion.)
**
** This function is NOT part of the sqlite3 public API. It is strictly
** for use by the JS/WASM bindings.
*/
const char * sqlite3_wasm_enum_json(void){
  sqlite3_str * s;
  if(zWasmEnum) return zWasmEnum;
  s = sqlite3_str_new(0);
  sqlite3_str_appendall(s, "{");

#define SD_(X,S,FINAL)                                                  \
  sqlite3_str_appendf(s, "\"%s\": %d%s", S, (int)X, (FINAL ? "}" : ", "))
#define SD(X) SD_(X,#X,0)
#define SDFinal(X) SD_(X,#X,1)

  sqlite3_str_appendall(s,"\"resultCodes\": {");
  SD(SQLITE_OK);
  SD(SQLITE_ERROR);
  SD(SQLITE_INTERNAL);
  SD(SQLITE_PERM);
  SD(SQLITE_ABORT);
  SD(SQLITE_BUSY);
  SD(SQLITE_LOCKED);
  SD(SQLITE_NOMEM);
  SD(SQLITE_READONLY);
  SD(SQLITE_INTERRUPT);
  SD(SQLITE_IOERR);
  SD(SQLITE_CORRUPT);
  SD(SQLITE_NOTFOUND);
  SD(SQLITE_FULL);
  SD(SQLITE_CANTOPEN);
  SD(SQLITE_PROTOCOL);
  SD(SQLITE_EMPTY);
  SD(SQLITE_SCHEMA);
  SD(SQLITE_TOOBIG);
  SD(SQLITE_CONSTRAINT);
  SD(SQLITE_MISMATCH);
  SD(SQLITE_MISUSE);
  SD(SQLITE_NOLFS);
  SD(SQLITE_AUTH);
  SD(SQLITE_FORMAT);
  SD(SQLITE_RANGE);
  SD(SQLITE_NOTADB);
  SD(SQLITE_NOTICE);
  SD(SQLITE_WARNING);
  SD(SQLITE_ROW);
  SDFinal(SQLITE_DONE);

  sqlite3_str_appendall(s,",\"dataTypes\": {");
  SD(SQLITE_INTEGER);
  SD(SQLITE_FLOAT);
  SD(SQLITE_TEXT);
  SD(SQLITE_BLOB);
  SDFinal(SQLITE_NULL);

  sqlite3_str_appendf(s,",\"encodings\": {");
  SDFinal(SQLITE_UTF8);

  sqlite3_str_appendall(s,",\"blobFinalizers\": {");
  SD(SQLITE_STATIC);
  SDFinal(SQLITE_TRANSIENT);

  sqlite3_str_appendall(s,",\"udfFlags\": {");
  SD(SQLITE_DETERMINISTIC);
  SD(SQLITE_DIRECTONLY);
  SDFinal(SQLITE_INNOCUOUS);

#undef SD_
#undef SD
#undef SDFinal
  sqlite3_str_appendall(s, "}");
  zWasmEnum = sqlite3_str_finish(s);
  atexit(sqlite3_wasm_cleanup);
  return zWasmEnum;
}
