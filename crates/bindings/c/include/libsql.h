#ifndef LIBSQL_EXPERIMENTAL_H
#define LIBSQL_EXPERIMENTAL_H

#include <stdint.h>

typedef struct libsql_database libsql_database;

typedef const libsql_database *libsql_database_ref;

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

libsql_database_ref libsql_open(const char *_path);

void libsql_close(libsql_database_ref db);

int32_t libsql_exec(libsql_database_ref _db, const char *_sql);

#ifdef __cplusplus
} // extern "C"
#endif // __cplusplus

#endif /* LIBSQL_EXPERIMENTAL_H */
