#include "util.h"

#include <assert.h>
#include <ctype.h>
#include <stdlib.h>
#include <string.h>

#include "consts.h"
#include "crsqlite.h"

size_t crsql_strnlen(const char *s, size_t n) {
  const char *p = memchr(s, 0, n);
  return p ? p - s : n;
}

// TODO: I don't think we need these crsql_ specific ones anymore now that we've
// set the allocator symbol in the WASM builds
char *crsql_strndup(const char *s, size_t n) {
  size_t l = crsql_strnlen(s, n);
  char *d = sqlite3_malloc(l + 1);
  if (!d) return NULL;
  memcpy(d, s, l);
  d[l] = 0;
  return d;
}

char *crsql_strdup(const char *s) {
  size_t l = strlen(s);
  char *d = sqlite3_malloc(l + 1);
  if (!d) return NULL;
  return memcpy(d, s, l + 1);
}

static char *joinHelper(char **in, size_t inlen, size_t inpos, size_t accum) {
  if (inpos == inlen) {
    return strcpy((char *)sqlite3_malloc(accum + 1) + accum, "");
  } else {
    size_t mylen = strlen(in[inpos]);
    return memcpy(joinHelper(in, inlen, inpos + 1, accum + mylen) - mylen,
                  in[inpos], mylen);
  }
}

// DO NOT dupe the memory!
const char *crsql_identity(const char *x) { return x; }

/**
 * @brief Join an array of strings into a single string
 *
 * @param in array of strings
 * @param inlen length of the array in
 * @return char* string -- must be freed by caller
 */
char *crsql_join(char **in, size_t inlen) {
  return joinHelper(in, inlen, 0, 0);
}

void crsql_joinWith(char *dest, char **src, size_t srcLen, char delim) {
  int j = 0;
  for (size_t i = 0; i < srcLen; ++i) {
    // copy mapped thing into ret at offset j.
    strcpy(dest + j, src[i]);
    // bump up j for next str.
    j += strlen(src[i]);

    // not the last element? then we need the separator
    if (i < srcLen - 1) {
      dest[j] = delim;
      j += 1;
    }
  }
}

char *crsql_join2(char *(*map)(const char *), char **in, size_t len,
                  char *delim) {
  if (len == 0) {
    return 0;
  }

  char **toJoin = sqlite3_malloc(len * sizeof(char *));
  int resultLen = 0;
  char *ret = 0;
  for (size_t i = 0; i < len; ++i) {
    toJoin[i] = map(in[i]);
    resultLen += strlen(toJoin[i]);
  }
  resultLen += (len - 1) * strlen(delim);
  ret = sqlite3_malloc((resultLen + 1) * sizeof(char));
  ret[resultLen] = '\0';

  int j = 0;
  for (size_t i = 0; i < len; ++i) {
    // copy mapped thing into ret at offset j.
    strcpy(ret + j, toJoin[i]);
    // bump up j for next str.
    j += strlen(toJoin[i]);

    // not the last element? then we need the separator
    if (i < len - 1) {
      strcpy(ret + j, delim);
      j += strlen(delim);
    }

    sqlite3_free(toJoin[i]);
  }
  sqlite3_free(toJoin);

  return ret;
}

/**
 * @brief Given a list of clock table names, construct a union query to get the
 * max clock value for our site.
 *
 * @param numRows the number of rows returned by the table names query
 * @param rQuery output param. Needs to be freed by the caller. The query being
 * build
 * @param tableNames array of clock table names
 * @return int success or not
 */
char *crsql_getDbVersionUnionQuery(int numRows, char **tableNames) {
  char **unionsArr = sqlite3_malloc(numRows * sizeof(char *));
  char *unionsStr;
  char *ret;
  int i = 0;

  for (i = 0; i < numRows; ++i) {
    unionsArr[i] = sqlite3_mprintf(
        "SELECT max(__crsql_db_version) as version FROM \"%w\" %s ",
        // the first result in tableNames is the column heading
        // so skip that
        tableNames[i + 1],
        // If we have more tables to process, union them in
        i < numRows - 1 ? UNION_ALL : "");
  }

  // move the array of strings into a single string
  unionsStr = crsql_join(unionsArr, numRows);
  // free the array of strings
  for (i = 0; i < numRows; ++i) {
    sqlite3_free(unionsArr[i]);
  }
  sqlite3_free(unionsArr);

  // compose the final query
  // and update the pointer to the string to point to it.
  ret = sqlite3_mprintf(
      "SELECT max(version) as version FROM (%z UNION SELECT value as "
      "version "
      "FROM crsql_master WHERE key = 'pre_compact_dbversion')",
      unionsStr);
  // %z frees unionsStr https://www.sqlite.org/printf.html#percentz
  return ret;
}

int crsql_getCount(sqlite3 *db, char *zSql) {
  int rc = SQLITE_OK;
  int count = 0;
  sqlite3_stmt *pStmt = 0;

  rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
  if (rc != SQLITE_OK) {
    sqlite3_finalize(pStmt);
    return -1 * rc;
  }

  rc = sqlite3_step(pStmt);
  if (rc != SQLITE_ROW) {
    sqlite3_finalize(pStmt);
    return -1 * rc;
  }

  count = sqlite3_column_int(pStmt, 0);
  sqlite3_finalize(pStmt);

  return count;
}
