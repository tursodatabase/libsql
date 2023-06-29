#ifndef LIBSQL_EXPERIMENTAL_H
#define LIBSQL_EXPERIMENTAL_H

#include <stdint.h>

typedef struct libsql_connection libsql_connection;

typedef struct libsql_database libsql_database;

typedef struct libsql_result libsql_result;

typedef const libsql_database *libsql_database_t;

typedef const libsql_connection *libsql_connection_t;

typedef const libsql_result *libsql_result_t;

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

libsql_database_t libsql_open_ext(const char *url);

void libsql_close(libsql_database_t db);

libsql_connection_t libsql_connect(libsql_database_t db);

void libsql_disconnect(libsql_connection_t conn);

libsql_result_t libsql_execute(libsql_connection_t conn, const char *sql);

void libsql_wait_result(libsql_result_t res);

void libsql_free_result(libsql_result_t res);

int libsql_row_count(libsql_result_t res);

int libsql_column_count(libsql_result_t res);

const char *libsql_value_text(libsql_result_t _res, int _row, int _col);

#ifdef __cplusplus
} // extern "C"
#endif // __cplusplus

#endif /* LIBSQL_EXPERIMENTAL_H */
