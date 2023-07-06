/*
** 2022-06-14
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** This library is used by fuzzcheck to test query invariants.
**
** An sqlite3_stmt is passed in that has just returned SQLITE_ROW.  This
** routine does:
**
**     *   Record the output of the current row
**     *   Construct an alternative query that should return the same row
**     *   Run the alternative query and verify that it does in fact return
**         the same row
**
*/
#include "sqlite3.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

/* Forward references */
static char *fuzz_invariant_sql(sqlite3_stmt*, int);
static int sameValue(sqlite3_stmt*,int,sqlite3_stmt*,int,sqlite3_stmt*);
static void reportInvariantFailed(sqlite3_stmt*,sqlite3_stmt*,int);

/*
** Do an invariant check on pStmt.  iCnt determines which invariant check to
** perform.  The first check is iCnt==0.
**
** *pbCorrupt is a flag that, if true, indicates that the database file
** is known to be corrupt.  A value of non-zero means "yes, the database
** is corrupt".  A zero value means "we do not know whether or not the
** database is corrupt".  The value might be set prior to entry, or this
** routine might set the value.
**
** Return values:
**
**     SQLITE_OK          This check was successful.
**
**     SQLITE_DONE        iCnt is out of range.  The caller typically sets
**                        up a loop on iCnt starting with zero, and increments
**                        iCnt until this code is returned.
**
**     SQLITE_CORRUPT     The invariant failed, but the underlying database
**                        file is indicating that it is corrupt, which might
**                        be the cause of the malfunction.  The *pCorrupt
**                        value will also be set.
**
**     SQLITE_INTERNAL    The invariant failed, and the database file is not
**                        corrupt.  (This never happens because this function
**                        will call abort() following an invariant failure.)
**
**     (other)            Some other kind of error occurred.
*/
int fuzz_invariant(
  sqlite3 *db,            /* The database connection */
  sqlite3_stmt *pStmt,    /* Test statement stopped on an SQLITE_ROW */
  int iCnt,               /* Invariant sequence number, starting at 0 */
  int iRow,               /* Current row number */
  int nRow,               /* Number of output rows from pStmt */
  int *pbCorrupt,         /* IN/OUT: Flag indicating a corrupt database file */
  int eVerbosity          /* How much debugging output */
){
  char *zTest;
  sqlite3_stmt *pTestStmt = 0;
  int rc;
  int i;
  int nCol;
  int nParam;

  if( *pbCorrupt ) return SQLITE_DONE;
  nParam = sqlite3_bind_parameter_count(pStmt);
  if( nParam>100 ) return SQLITE_DONE;
  zTest = fuzz_invariant_sql(pStmt, iCnt);
  if( zTest==0 ) return SQLITE_DONE;
  rc = sqlite3_prepare_v2(db, zTest, -1, &pTestStmt, 0);
  if( rc ){
    if( eVerbosity ){
      printf("invariant compile failed: %s\n%s\n",
             sqlite3_errmsg(db), zTest);
    }
    sqlite3_free(zTest);
    sqlite3_finalize(pTestStmt);
    return rc;
  }
  sqlite3_free(zTest);
  nCol = sqlite3_column_count(pStmt);
  for(i=0; i<nCol; i++){
    rc = sqlite3_bind_value(pTestStmt,i+1+nParam,sqlite3_column_value(pStmt,i));
    if( rc!=SQLITE_OK && rc!=SQLITE_RANGE ){
      sqlite3_finalize(pTestStmt);
      return rc;
    }
  }
  if( eVerbosity>=2 ){
    char *zSql = sqlite3_expanded_sql(pTestStmt);
    printf("invariant-sql row=%d #%d:\n%s\n", iRow, iCnt, zSql);
    sqlite3_free(zSql);
  }
  while( (rc = sqlite3_step(pTestStmt))==SQLITE_ROW ){
    for(i=0; i<nCol; i++){
      if( !sameValue(pStmt, i, pTestStmt, i, 0) ) break;
    }
    if( i>=nCol ) break;
  }
  if( rc==SQLITE_DONE ){
    /* No matching output row found */
    sqlite3_stmt *pCk = 0;
    int iOrigRSO;


    /* This is not a fault if the database file is corrupt, because anything
    ** can happen with a corrupt database file */
    rc = sqlite3_prepare_v2(db, "PRAGMA integrity_check", -1, &pCk, 0);
    if( rc ){
      sqlite3_finalize(pCk);
      sqlite3_finalize(pTestStmt);
      return rc;
    }
    if( eVerbosity>=2 ){
      char *zSql = sqlite3_expanded_sql(pCk);
      printf("invariant-validity-check #1:\n%s\n", zSql);
      sqlite3_free(zSql);
    }

    rc = sqlite3_step(pCk);
    if( rc!=SQLITE_ROW
     || sqlite3_column_text(pCk, 0)==0
     || strcmp((const char*)sqlite3_column_text(pCk,0),"ok")!=0
    ){
      *pbCorrupt = 1;
      sqlite3_finalize(pCk);
      sqlite3_finalize(pTestStmt);
      return SQLITE_CORRUPT;
    }
    sqlite3_finalize(pCk);

    /*
    ** If inverting the scan order also results in a miss, assume that the
    ** query is ambiguous and do not report a fault.
    */
    sqlite3_db_config(db, SQLITE_DBCONFIG_REVERSE_SCANORDER, -1, &iOrigRSO);
    sqlite3_db_config(db, SQLITE_DBCONFIG_REVERSE_SCANORDER, !iOrigRSO, 0);
    sqlite3_prepare_v2(db, sqlite3_sql(pStmt), -1, &pCk, 0);
    sqlite3_db_config(db, SQLITE_DBCONFIG_REVERSE_SCANORDER, iOrigRSO, 0);
    if( eVerbosity>=2 ){
      char *zSql = sqlite3_expanded_sql(pCk);
      printf("invariant-validity-check #2:\n%s\n", zSql);
      sqlite3_free(zSql);
    }
    while( (rc = sqlite3_step(pCk))==SQLITE_ROW ){
      for(i=0; i<nCol; i++){
        if( !sameValue(pStmt, i, pTestStmt, i, 0) ) break;
      }
      if( i>=nCol ) break;
    }
    sqlite3_finalize(pCk);
    if( rc==SQLITE_DONE ){
      sqlite3_finalize(pTestStmt);
      return SQLITE_DONE;
    }

    /* The original sameValue() comparison assumed a collating sequence
    ** of "binary".  It can sometimes get an incorrect result for different
    ** collating sequences.  So rerun the test with no assumptions about
    ** collations.
    */
    rc = sqlite3_prepare_v2(db,
       "SELECT ?1=?2 OR ?1=?2 COLLATE nocase OR ?1=?2 COLLATE rtrim",
       -1, &pCk, 0);
    if( rc==SQLITE_OK ){
      if( eVerbosity>=2 ){
        char *zSql = sqlite3_expanded_sql(pCk);
        printf("invariant-validity-check #3:\n%s\n", zSql);
        sqlite3_free(zSql);
      }

      sqlite3_reset(pTestStmt);
      while( (rc = sqlite3_step(pTestStmt))==SQLITE_ROW ){
        for(i=0; i<nCol; i++){
          if( !sameValue(pStmt, i, pTestStmt, i, pCk) ) break;
        }
        if( i>=nCol ){
          sqlite3_finalize(pCk);
          goto not_a_fault;
        }
      }
    }
    sqlite3_finalize(pCk);

    /* Invariants do not necessarily work if there are virtual tables
    ** involved in the query */
    rc = sqlite3_prepare_v2(db, 
            "SELECT 1 FROM bytecode(?1) WHERE opcode='VOpen'", -1, &pCk, 0);
    if( rc==SQLITE_OK ){
      if( eVerbosity>=2 ){
        char *zSql = sqlite3_expanded_sql(pCk);
        printf("invariant-validity-check #4:\n%s\n", zSql);
        sqlite3_free(zSql);
      }
      sqlite3_bind_pointer(pCk, 1, pStmt, "stmt-pointer", 0);
      rc = sqlite3_step(pCk);
    }
    sqlite3_finalize(pCk);
    if( rc==SQLITE_DONE ){
      reportInvariantFailed(pStmt, pTestStmt, iRow);
      return SQLITE_INTERNAL;
    }else if( eVerbosity>0 ){
      printf("invariant-error ignored due to the use of virtual tables\n");
    }
  }
not_a_fault:
  sqlite3_finalize(pTestStmt);
  return SQLITE_OK;
}


/*
** Generate SQL used to test a statement invariant.
**
** Return 0 if the iCnt is out of range.
**
** iCnt meanings:
**
**   0     SELECT * FROM (<query>)
**   1     SELECT DISTINCT * FROM (<query>)
**   2     SELECT * FROM (<query>) WHERE ORDER BY 1
**   3     SELECT DISTINCT * FROM (<query>) ORDER BY 1
**   4     SELECT * FROM (<query>) WHERE <all-columns>=<all-values>
**   5     SELECT DISTINCT * FROM (<query>) WHERE <all-columns=<all-values
**   6     SELECT * FROM (<query>) WHERE <all-column>=<all-value> ORDER BY 1
**   7     SELECT DISTINCT * FROM (<query>) WHERE <all-column>=<all-value>
**                           ORDER BY 1
**   N+0   SELECT * FROM (<query>) WHERE <nth-column>=<value>
**   N+1   SELECT DISTINCT * FROM (<query>) WHERE <Nth-column>=<value>
**   N+2   SELECT * FROM (<query>) WHERE <Nth-column>=<value> ORDER BY 1
**   N+3   SELECT DISTINCT * FROM (<query>) WHERE <Nth-column>=<value>
**                           ORDER BY N
**
*/
static char *fuzz_invariant_sql(sqlite3_stmt *pStmt, int iCnt){
  const char *zIn;
  size_t nIn;
  const char *zAnd = "WHERE";
  int i, j;
  sqlite3_str *pTest;
  sqlite3_stmt *pBase = 0;
  sqlite3 *db = sqlite3_db_handle(pStmt);
  int rc;
  int nCol = sqlite3_column_count(pStmt);
  int mxCnt;
  int bDistinct = 0;
  int bOrderBy = 0;
  int nParam = sqlite3_bind_parameter_count(pStmt);

  switch( iCnt % 4 ){
    case 1:  bDistinct = 1;              break;
    case 2:  bOrderBy = 1;               break;
    case 3:  bDistinct = bOrderBy = 1;   break;
  }
  iCnt /= 4;
  mxCnt = nCol;
  if( iCnt<0 || iCnt>mxCnt ) return 0;
  zIn = sqlite3_sql(pStmt);
  if( zIn==0 ) return 0;
  nIn = strlen(zIn);
  while( nIn>0 && (isspace(zIn[nIn-1]) || zIn[nIn-1]==';') ) nIn--;
  if( strchr(zIn, '?') ) return 0;
  pTest = sqlite3_str_new(0);
  sqlite3_str_appendf(pTest, "SELECT %s* FROM (",  
                      bDistinct ? "DISTINCT " : "");
  sqlite3_str_append(pTest, zIn, (int)nIn);
  sqlite3_str_append(pTest, ")", 1);
  rc = sqlite3_prepare_v2(db, sqlite3_str_value(pTest), -1, &pBase, 0);
  if( rc ){
    sqlite3_finalize(pBase);
    pBase = pStmt;
  }
  for(i=0; i<sqlite3_column_count(pStmt); i++){
    const char *zColName = sqlite3_column_name(pBase,i);
    const char *zSuffix = zColName ? strrchr(zColName, ':') : 0;
    if( zSuffix 
     && isdigit(zSuffix[1])
     && (zSuffix[1]>'3' || isdigit(zSuffix[2]))
    ){
      /* This is a randomized column name and so cannot be used in the
      ** WHERE clause. */
      continue;
    }
    for(j=0; j<i; j++){
      const char *zPrior = sqlite3_column_name(pBase, j);
      if( sqlite3_stricmp(zPrior, zColName)==0 ) break;
    }
    if( j<i ){
      /* Duplicate column name */
      continue;
    }
    if( iCnt==0 ) continue;
    if( iCnt>1 && i+2!=iCnt ) continue;
    if( zColName==0 ) continue;
    if( sqlite3_column_type(pStmt, i)==SQLITE_NULL ){
      sqlite3_str_appendf(pTest, " %s \"%w\" ISNULL", zAnd, zColName);
    }else{
      sqlite3_str_appendf(pTest, " %s \"%w\"=?%d", zAnd, zColName, 
                          i+1+nParam);
    }
    zAnd = "AND";
  }
  if( pBase!=pStmt ) sqlite3_finalize(pBase);
  if( bOrderBy ){
    sqlite3_str_appendf(pTest, " ORDER BY %d", iCnt>2 ? iCnt-1 : 1);
  }
  return sqlite3_str_finish(pTest);
}

/*
** Return true if and only if v1 and is the same as v2.
*/
static int sameValue(
  sqlite3_stmt *pS1, int i1,       /* Value to text on the left */
  sqlite3_stmt *pS2, int i2,       /* Value to test on the right */
  sqlite3_stmt *pTestCompare       /* COLLATE comparison statement or NULL */
){
  int x = 1;
  int t1 = sqlite3_column_type(pS1,i1);
  int t2 = sqlite3_column_type(pS2,i2);
  if( t1!=t2 ){
    if( (t1==SQLITE_INTEGER && t2==SQLITE_FLOAT)
     || (t1==SQLITE_FLOAT && t2==SQLITE_INTEGER)
    ){
      /* Comparison of numerics is ok */
    }else{
      return 0;
    }
  }
  switch( sqlite3_column_type(pS1,i1) ){
    case SQLITE_INTEGER: {
      x =  sqlite3_column_int64(pS1,i1)==sqlite3_column_int64(pS2,i2);
      break;
    }
    case SQLITE_FLOAT: {
      x = sqlite3_column_double(pS1,i1)==sqlite3_column_double(pS2,i2);
      break;
    }
    case SQLITE_TEXT: {
      int e1 = sqlite3_value_encoding(sqlite3_column_value(pS1,i1));
      int e2 = sqlite3_value_encoding(sqlite3_column_value(pS2,i2));
      if( e1!=e2 ){
        const char *z1 = (const char*)sqlite3_column_text(pS1,i1);
        const char *z2 = (const char*)sqlite3_column_text(pS2,i2);
        x = ((z1==0 && z2==0) || (z1!=0 && z2!=0 && strcmp(z1,z1)==0));
        printf("Encodings differ.  %d on left and %d on right\n", e1, e2);
        abort();
      }
      if( pTestCompare ){
        sqlite3_bind_value(pTestCompare, 1, sqlite3_column_value(pS1,i1));
        sqlite3_bind_value(pTestCompare, 2, sqlite3_column_value(pS2,i2));
        x = sqlite3_step(pTestCompare)==SQLITE_ROW
                      && sqlite3_column_int(pTestCompare,0)!=0;
        sqlite3_reset(pTestCompare);
        break;
      }
      if( e1!=SQLITE_UTF8 ){
        int len1 = sqlite3_column_bytes16(pS1,i1);
        const unsigned char *b1 = sqlite3_column_blob(pS1,i1);
        int len2 = sqlite3_column_bytes16(pS2,i2);
        const unsigned char *b2 = sqlite3_column_blob(pS2,i2);
        if( len1!=len2 ){
          x = 0;
        }else if( len1==0 ){
          x = 1;
        }else{
          x = (b1!=0 && b2!=0 && memcmp(b1,b2,len1)==0);
        }
        break;
      }
      /* Fall through into the SQLITE_BLOB case */
    }
    case SQLITE_BLOB: {
      int len1 = sqlite3_column_bytes(pS1,i1);
      const unsigned char *b1 = sqlite3_column_blob(pS1,i1);
      int len2 = sqlite3_column_bytes(pS2,i2);
      const unsigned char *b2 = sqlite3_column_blob(pS2,i2);
      if( len1!=len2 ){
        x = 0;
      }else if( len1==0 ){
        x = 1;
      }else{
        x = (b1!=0 && b2!=0 && memcmp(b1,b2,len1)==0);
      }
      break;
    }
  }
  return x;
}

/*
** Print binary data as hex
*/
static void printHex(const unsigned char *a, int n, int mx){
  int j;
  for(j=0; j<mx && j<n; j++){
    printf("%02x", a[j]);
  }
  if( j<n ) printf("...");
}

/*
** Print a single row from the prepared statement
*/
static void printRow(sqlite3_stmt *pStmt, int iRow){
  int i, n, nCol;
  unsigned const char *data;
  nCol = sqlite3_column_count(pStmt);
  for(i=0; i<nCol; i++){
    printf("row%d.col%d = ", iRow, i);
    switch( sqlite3_column_type(pStmt, i) ){
      case SQLITE_NULL: {
        printf("NULL\n");
        break;
      }
      case SQLITE_INTEGER: {
        printf("(integer) %lld\n", sqlite3_column_int64(pStmt, i));
        break;
      }
      case SQLITE_FLOAT: {
        printf("(float) %f\n", sqlite3_column_double(pStmt, i));
        break;
      }
      case SQLITE_TEXT: {
        switch( sqlite3_value_encoding(sqlite3_column_value(pStmt,i)) ){
          case SQLITE_UTF8: {
            printf("(utf8) x'");
            n = sqlite3_column_bytes(pStmt, i);
            data = sqlite3_column_blob(pStmt, i);
            printHex(data, n, 35);
            printf("'\n");
            break;
          }
          case SQLITE_UTF16BE: {
            printf("(utf16be) x'");
            n = sqlite3_column_bytes16(pStmt, i);
            data = sqlite3_column_blob(pStmt, i);
            printHex(data, n, 35);
            printf("'\n");
            break;
          }
          case SQLITE_UTF16LE: {
            printf("(utf16le) x'");
            n = sqlite3_column_bytes16(pStmt, i);
            data = sqlite3_column_blob(pStmt, i);
            printHex(data, n, 35);
            printf("'\n");
            break;
          }
          default: {
            printf("Illegal return from sqlite3_value_encoding(): %d\n",
                sqlite3_value_encoding(sqlite3_column_value(pStmt,i)));
            abort();
          }
        }
        break;
      }
      case SQLITE_BLOB: {
        n = sqlite3_column_bytes(pStmt, i);
        data = sqlite3_column_blob(pStmt, i);
        printf("(blob %d bytes) x'", n);
        printHex(data, n, 35);
        printf("'\n");
        break;
      }
    }
  }
}

/*
** Report a failure of the invariant:  The current output row of pOrig
** does not appear in any row of the output from pTest.
*/
static void reportInvariantFailed(
  sqlite3_stmt *pOrig,   /* The original query */
  sqlite3_stmt *pTest,   /* The alternative test query with a missing row */
  int iRow               /* Row number in pOrig */
){
  int iTestRow = 0;
  printf("Invariant check failed on row %d.\n", iRow);
  printf("Original query --------------------------------------------------\n");
  printf("%s\n", sqlite3_expanded_sql(pOrig));
  printf("Alternative query -----------------------------------------------\n");
  printf("%s\n", sqlite3_expanded_sql(pTest));
  printf("Result row that is missing from the alternative -----------------\n");
  printRow(pOrig, iRow);
  printf("Complete results from the alternative query ---------------------\n");
  sqlite3_reset(pTest);
  while( sqlite3_step(pTest)==SQLITE_ROW ){
    iTestRow++;
    printRow(pTest, iTestRow);
  }
  sqlite3_finalize(pTest);
  abort();
}
