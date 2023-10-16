#ifndef CRSQLITE_UTIL
#define CRSQLITE_UTIL

#include <ctype.h>

#include "crsqlite.h"

size_t crsql_strnlen(const char *s, size_t n);
char *crsql_strndup(const char *s, size_t n);
char *crsql_strdup(const char *s);

char *crsql_getDbVersionUnionQuery(int numRows, char **tableNames);

char *crsql_join(char **in, size_t inlen);

int crsql_doesTableExist(sqlite3 *db, const char *tblName);

int crsql_getCount(sqlite3 *db, char *zSql);

void crsql_joinWith(char *dest, char **src, size_t srcLen, char delim);
char *crsql_asIdentifierListStr(char **idents, size_t identsLen, char delim);

int crsql_getIndexedCols(sqlite3 *db, const char *indexName,
                         char ***pIndexedCols, int *pIndexedColsLen,
                         char **pErrMsg);

char *crsql_join2(char *(*map)(const char *), char **in, size_t len,
                  char *delim);
const char *crsql_identity(const char *x);
int crsql_isIdentifierOpenQuote(char c);
char **crsql_split(const char *in, char *delim, int partsLen);
int crsql_siteIdCmp(const void *zLeft, int leftLen, const void *zRight,
                    int rightLen);
char **crsql_splitQuoteConcat(const char *in, int partsLen);

#endif