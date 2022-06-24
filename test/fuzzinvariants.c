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
static int sameValue(sqlite3_stmt*,int,sqlite3_stmt*,int);
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
**     SQLITE_DONE        iCnt is out of range.
**
**     SQLITE_CORRUPT     The invariant failed, but the underlying database
**                        file is indicating that it is corrupt, which might
**                        be the cause of the malfunction.
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
    printf("invariant-sql #%d:\n%s\n", iCnt, zSql);
    sqlite3_free(zSql);
  }
  while( (rc = sqlite3_step(pTestStmt))==SQLITE_ROW ){
    for(i=0; i<nCol; i++){
      if( !sameValue(pStmt, i, pTestStmt, i) ) break;
    }
    if( i>=nCol ) break;
  }
  if( rc==SQLITE_DONE ){
    /* No matching output row found */
    sqlite3_stmt *pCk = 0;
    rc = sqlite3_prepare_v2(db, "PRAGMA integrity_check", -1, &pCk, 0);
    if( rc ){
      sqlite3_finalize(pCk);
      sqlite3_finalize(pTestStmt);
      return rc;
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
    rc = sqlite3_prepare_v2(db, 
            "SELECT 1 FROM bytecode(?1) WHERE opcode='VOpen'", -1, &pCk, 0);
    if( rc==SQLITE_OK ){
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
  sqlite3_finalize(pTestStmt);
  return SQLITE_OK;
}


/*
** Generate SQL used to test a statement invariant.
**
** Return 0 if the iCnt is out of range.
*/
static char *fuzz_invariant_sql(sqlite3_stmt *pStmt, int iCnt){
  const char *zIn;
  size_t nIn;
  const char *zAnd = "WHERE";
  int i;
  sqlite3_str *pTest;
  sqlite3_stmt *pBase = 0;
  sqlite3 *db = sqlite3_db_handle(pStmt);
  int rc;
  int nCol = sqlite3_column_count(pStmt);
  int mxCnt;
  int bDistinct = 0;
  int bOrderBy = 0;
  int nParam = sqlite3_bind_parameter_count(pStmt);

  iCnt++;
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
  sqlite3_str_appendf(pTest, "SELECT %s* FROM (%s",
                      bDistinct ? "DISTINCT " : "", zIn);
  sqlite3_str_appendf(pTest, ")");
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
    if( i+1!=iCnt ) continue;
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
    sqlite3_str_appendf(pTest, " ORDER BY 1");
  }
  return sqlite3_str_finish(pTest);
}

/*
** Return true if and only if v1 and is the same as v2.
*/
static int sameValue(sqlite3_stmt *pS1, int i1, sqlite3_stmt *pS2, int i2){
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
      const char *z1 = (const char*)sqlite3_column_text(pS1,i1);
      const char *z2 = (const char*)sqlite3_column_text(pS2,i2);
      x = ((z1==0 && z2==0) || (z1!=0 && z2!=0 && strcmp(z1,z1)==0));
      break;
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
** Print a single row from the prepared statement
*/
static void printRow(sqlite3_stmt *pStmt, int iRow){
  int i, nCol;
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
        printf("(text) \"%s\"\n", sqlite3_column_text(pStmt, i));
        break;
      }
      case SQLITE_BLOB: {
        int n = sqlite3_column_bytes(pStmt, i);
        int j;
        unsigned const char *data = sqlite3_column_blob(pStmt, i);
        printf("(blob %d bytes) x'", n);
        for(j=0; j<20 && j<n; j++){
          printf("%02x", data[j]);
        }
        if( j<n ) printf("...");
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
