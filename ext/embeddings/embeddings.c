#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1
LIBSQL_EXTENSION_INIT1
#include <assert.h>
#include <string.h>

int sentence_embeddings(const char *sentence, int sentence_len, char *out_embedding);

static void sentence_embeddings_func(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
) {
  char result[384 * sizeof(float)];

  if(sqlite3_value_type(argv[0]) == SQLITE_NULL) {
    return;
  }
  const unsigned char *sentence = (const unsigned char*)sqlite3_value_text(argv[0]);
  int sentence_len = sqlite3_value_bytes(argv[0]);

  sentence_embeddings((const char*)sentence, sentence_len, result);

  sqlite3_result_blob(context, (char*)result, sizeof(result), SQLITE_TRANSIENT);
}

#ifdef _WIN32
__declspec(dllexport)
#endif
int embeddings_c_init(
  sqlite3 *db, 
  char **pzErrMsg, 
  const sqlite3_api_routines *pApi,
  const libsql_api_routines *pLibsqlApi
){
  int rc = SQLITE_OK;
  SQLITE_EXTENSION_INIT2(pApi);
  LIBSQL_EXTENSION_INIT2(pLibsqlApi);
  (void)pzErrMsg;  /* Unused parameter */
  rc = sqlite3_create_function(db, "sentence_embeddings", 1,
                   SQLITE_UTF8|SQLITE_INNOCUOUS|SQLITE_DETERMINISTIC,
                   0, sentence_embeddings_func, 0, 0);
  return rc;
}