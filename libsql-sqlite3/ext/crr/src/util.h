#ifndef CRSQLITE_UTIL
#define CRSQLITE_UTIL

#include <ctype.h>

#include "crsqlite.h"

size_t crsql_strnlen(const char *s, size_t n);
char *crsql_strndup(const char *s, size_t n);
char *crsql_strdup(const char *s);

char *crsql_getDbVersionUnionQuery(int numRows, char **tableNames);

char *crsql_join(char **in, size_t inlen);

int crsql_getCount(sqlite3 *db, char *zSql);

void crsql_joinWith(char *dest, char **src, size_t srcLen, char delim);

char *crsql_join2(char *(*map)(const char *), char **in, size_t len,
                  char *delim);
const char *crsql_identity(const char *x);

#endif