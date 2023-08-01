#ifndef LIBSQL_EXPERIMENTAL_H
#define LIBSQL_EXPERIMENTAL_H

#include <stdint.h>

typedef struct libsql_connection libsql_connection;

typedef struct libsql_database libsql_database;

typedef struct libsql_row libsql_row;

typedef struct libsql_rows libsql_rows;

typedef struct libsql_rows_future libsql_rows_future;

typedef const libsql_database *libsql_database_t;

typedef const libsql_connection *libsql_connection_t;

typedef const libsql_rows *libsql_rows_t;

typedef const libsql_rows_future *libsql_rows_future_t;

typedef const libsql_row *libsql_row_t;

typedef struct {
  const char *ptr;
  int len;
} blob;

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

libsql_database_t libsql_open_ext(const char *url);

void libsql_close(libsql_database_t db);

libsql_connection_t libsql_connect(libsql_database_t db);

void libsql_disconnect(libsql_connection_t conn);

libsql_rows_t libsql_execute(libsql_connection_t conn, const char *sql);

void libsql_free_rows(libsql_rows_t res);

libsql_rows_future_t libsql_execute_async(const libsql_connection_t *conn, const char *sql);

void libsql_free_rows_future(libsql_rows_future_t res);

void libsql_wait_result(libsql_rows_future_t res);

int libsql_column_count(libsql_rows_t res);

const char *libsql_column_name(libsql_rows_t res, int col);

int libsql_column_type(libsql_rows_t res, int col);

libsql_row_t libsql_next_row(libsql_rows_t res);

void libsql_free_row(libsql_row_t res);

const char *libsql_get_string(libsql_row_t res, int col);

void libsql_free_string(const char *ptr);

long long libsql_get_int(libsql_row_t res, int col);

double libsql_get_float(libsql_row_t res, int col);

blob libsql_get_blob(libsql_row_t res, int col);

void libsql_free_blob(blob b);

#ifdef __cplusplus
} // extern "C"
#endif // __cplusplus

#endif /* LIBSQL_EXPERIMENTAL_H */
