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
 * Given a pointer to the inside of a string literal,
 * scan until we get to the end of the literal.
 *
 * Fails if we hit the end of the string without finding an unescaped
 * literal termination.
 */
const char *crsql_scanToEndOfLiteral(const char *in) {
  while (*in != '\0') {
    // hit a quote
    if (*in == '\'') {
      // with no quote after?
      // it is unescaped
      if (*(in + 1) != '\'') {
        return (in + 1);
      } else {
        // was a quote after? move into that quote
        // then past the quote at end of loop.
        in += 1;
      }
    }
    in += 1;
  }

  // we made it to the end of the string? well then
  // there was a bare quote which is an error case.
  return 0;
}

/**
 * Advance len characters through the string.
 * Fails (returns 0) if we cannot advance at least that much.
 */
const char *crsql_safelyAdvanceThroughString(const char *in, int len) {
  for (int i = 0; i < len; ++i) {
    if (*in == '\0') {
      return 0;
    }

    in += 1;
  }
  return in;
}

/**
 * Looks for the provided delimiter and returns a pointer to the character
 * after that delim.
 *
 * The technically allows many `e` and `.` characters in a number.
 * We can improve this to only allow them where they should occur but
 * this is good enough for our purposes of ensuring safe input.
 *
 * If no delim is found, returns a pointer to the end of the string.
 */
const char *crsql_consumeDigitsToDelimiter(const char *in, char delim) {
  int decimalCount = 0;
  int exponentCount = 0;
  while (*in != '\0' && *in != delim) {
    if (*in < 48 || *in > 57) {
      if (*in == '.') {
        if (decimalCount > 0) {
          return 0;
        }
        ++decimalCount;
      } else if (*in == 'e') {
        if (exponentCount > 0) {
          return 0;
        }
        ++exponentCount;
        if (*(in + 1) == '-' || *(in + 1) == '+') {
          in += 1;
        }
      } else {
        return 0;
      }
    }
    in += 1;
  }
  return in;
}

char **crsql_splitQuoteConcat(const char *in, int partsLen) {
  const char *curr = in;
  const char *last = in;
  char **zzParts = sqlite3_malloc(partsLen * sizeof(char *));
  int partIdx = 0;
  while (curr != 0 && *curr != '\0' && partIdx < partsLen) {
    if (*curr == '\'') {
      // scan till consumed string literal
      curr += 1;
      curr = crsql_scanToEndOfLiteral(curr);
    } else if (*curr == 'X') {
      // scan till consumed hex
      curr += 1;
      if (*curr != '\'') {
        // unexpected result
        // set curr = 0 so we can cleanup and exit
        curr = 0;
      } else {
        curr += 1;
        curr = crsql_scanToEndOfLiteral(curr);
      }
    } else if (*curr == 'N') {
      // scan till consumed NULL
      curr = crsql_safelyAdvanceThroughString(curr, 4);
    } else {
      if (*curr == '-') {
        curr += 1;
      }
      // scan till we hit the delimiter, consuming the digits
      curr = crsql_consumeDigitsToDelimiter(curr, QC_DELIM);
    }

    if (curr == 0) {
      // we had an error in scanning
      // free what we've allocated thus far and return null.
      for (int i = 0; i < partIdx; ++i) {
        sqlite3_free(zzParts[i]);
      }
      sqlite3_free(zzParts);
      return 0;
    }

    // pull from last to curr
    // advance last
    zzParts[partIdx] = sqlite3_malloc(((curr - last) + 1) * sizeof(char *));
    strncpy(zzParts[partIdx], last, curr - last);
    zzParts[partIdx][curr - last] = '\0';

    // pointing at a delim? Move off it.
    if (*curr == QC_DELIM) {
      curr += 1;
    }

    last = curr;
    partIdx += 1;
  }

  if (partIdx != partsLen || *curr != '\0') {
    // we did not consume the whole string or get the number of parts
    // expected. this is an error case.
    for (int i = 0; i < partIdx; ++i) {
      sqlite3_free(zzParts[i]);
    }
    sqlite3_free(zzParts);
    return 0;
  }

  return zzParts;
}

// TODO:
// have this take a function pointer that extracts the string so we can
// delete crsql_asIdentifierList
char *crsql_asIdentifierListStr(char **in, size_t inlen, char delim) {
  int finalLen = 0;
  char *ret = 0;
  char **mapped = sqlite3_malloc(inlen * sizeof(char *));

  for (size_t i = 0; i < inlen; ++i) {
    mapped[i] = sqlite3_mprintf("\"%w\"", in[i]);
    finalLen += strlen(mapped[i]);
  }
  // -1 for spearator not appended to last thing
  finalLen += inlen - 1;

  // + 1 for null terminator
  ret = sqlite3_malloc(finalLen * sizeof(char) + 1);
  ret[finalLen] = '\0';

  crsql_joinWith(ret, mapped, inlen, delim);

  // free everything we allocated, except ret.
  // caller will free ret.
  for (size_t i = 0; i < inlen; ++i) {
    sqlite3_free(mapped[i]);
  }
  sqlite3_free(mapped);

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
        i < numRows - 1 ? UNION : "");
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
  ret = sqlite3_mprintf("SELECT max(version) as version FROM (%z)", unionsStr);
  // %z frees unionsStr https://www.sqlite.org/printf.html#percentz
  return ret;
}

/**
 * Check if tblName exists.
 * Caller is responsible for freeing tblName.
 *
 * Returns -1 on error.
 */
int crsql_doesTableExist(sqlite3 *db, const char *tblName) {
  char *zSql;
  int ret = 0;

  zSql = sqlite3_mprintf(
      "SELECT count(*) as c FROM sqlite_master WHERE type='table' AND "
      "tbl_name "
      "= '%s'",
      tblName);
  ret = crsql_getCount(db, zSql);
  sqlite3_free(zSql);

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

/**
 * Given an index name, return all the columns in that index.
 * Fills pIndexedCols with an array of strings.
 * Caller is responsible for freeing pIndexedCols.
 */
int crsql_getIndexedCols(sqlite3 *db, const char *indexName,
                         char ***pIndexedCols, int *pIndexedColsLen,
                         char **pErrMsg) {
  int rc = SQLITE_OK;
  int numCols = 0;
  char **indexedCols;
  sqlite3_stmt *pStmt = 0;
  *pIndexedCols = 0;
  *pIndexedColsLen = 0;

  char *zSql = sqlite3_mprintf("SELECT count(*) FROM pragma_index_info('%s')",
                               indexName);
  numCols = crsql_getCount(db, zSql);
  sqlite3_free(zSql);

  if (numCols <= 0) {
    return numCols;
  }

  zSql = sqlite3_mprintf(
      "SELECT \"name\" FROM pragma_index_info('%s') ORDER BY \"seqno\" ASC",
      indexName);
  rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
  sqlite3_free(zSql);
  if (rc != SQLITE_OK) {
    *pErrMsg = sqlite3_mprintf("Failed preparing pragma_index_info('%s') stmt",
                               indexName);
    sqlite3_finalize(pStmt);
    return rc;
  }

  rc = sqlite3_step(pStmt);
  if (rc != SQLITE_ROW) {
    sqlite3_finalize(pStmt);
    return SQLITE_OK;
  }

  int j = 0;
  indexedCols = sqlite3_malloc(numCols * sizeof(char *));
  while (rc == SQLITE_ROW) {
    assert(j < numCols);

    indexedCols[j] = crsql_strdup((const char *)sqlite3_column_text(pStmt, 0));

    rc = sqlite3_step(pStmt);
    ++j;
  }
  sqlite3_finalize(pStmt);

  if (rc != SQLITE_DONE) {
    for (int i = 0; i < j; ++i) {
      sqlite3_free(indexedCols[i]);
    }
    sqlite3_free(indexedCols);
    *pErrMsg = sqlite3_mprintf("Failed allocating index info");
    return rc;
  }

  *pIndexedCols = indexedCols;
  *pIndexedColsLen = numCols;

  return SQLITE_OK;
}

int crsql_isIdentifierOpenQuote(char c) {
  switch (c) {
    case '[':
      return 1;
    case '`':
      return 1;
    case '"':
      return 1;
  }

  return 0;
}

int crsql_siteIdCmp(const void *zLeft, int leftLen, const void *zRight,
                    int rightLen) {
  int minLen = leftLen < rightLen ? leftLen : rightLen;
  int cmp = memcmp(zLeft, zRight, minLen);

  if (cmp == 0) {
    if (leftLen > rightLen) {
      return 1;
    } else if (leftLen < rightLen) {
      return -1;
    }

    return 0;
  }

  return cmp > 0 ? 1 : -1;
}
