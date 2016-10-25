/*
** 2016-10-24
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
** This is a utility program that uses the est_count and btree_sample
** pragmas to try to approximate the content of the sqlite_stat1 table
** without doing a full table scan.
**
** To compile, simply link against SQLite.
**
** See the showHelp() routine below for a brief description of how to
** run the utility.
*/
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <ctype.h>
#include <string.h>
#include <assert.h>
#include "sqlite3.h"

/*
** All global variables are gathered into the "g" singleton.
*/
struct GlobalVars {
  const char *zArgv0;       /* Name of program */
  unsigned fDebug;          /* Debug flags */
  sqlite3 *db;              /* The database connection */
} g;

/*
** Allowed values for g.fDebug
*/
#define DEBUG_NONE          0

  
/*
** Print an error resulting from faulting command-line arguments and
** abort the program.
*/
static void cmdlineError(const char *zFormat, ...){
  va_list ap;
  fprintf(stderr, "%s: ", g.zArgv0);
  va_start(ap, zFormat);
  vfprintf(stderr, zFormat, ap);
  va_end(ap);
  fprintf(stderr, "\n\"%s --help\" for more help\n", g.zArgv0);
  exit(1);
}

/*
** Print an error message for an error that occurs at runtime, then
** abort the program.
*/
static void runtimeError(const char *zFormat, ...){
  va_list ap;
  fprintf(stderr, "%s: ", g.zArgv0);
  va_start(ap, zFormat);
  vfprintf(stderr, zFormat, ap);
  va_end(ap);
  fprintf(stderr, "\n");
  exit(1);
}

/*
** Prepare a new SQL statement.  Print an error and abort if anything
** goes wrong.
*/
static sqlite3_stmt *db_vprepare(const char *zFormat, va_list ap){
  char *zSql;
  int rc;
  sqlite3_stmt *pStmt;

  zSql = sqlite3_vmprintf(zFormat, ap);
  if( zSql==0 ) runtimeError("out of memory");
  rc = sqlite3_prepare_v2(g.db, zSql, -1, &pStmt, 0);
  if( rc ){
    runtimeError("SQL statement error: %s\n\"%s\"", sqlite3_errmsg(g.db),
                 zSql);
  }
  sqlite3_free(zSql);
  return pStmt;
}
static sqlite3_stmt *db_prepare(const char *zFormat, ...){
  va_list ap;
  sqlite3_stmt *pStmt;
  va_start(ap, zFormat);
  pStmt = db_vprepare(zFormat, ap);
  va_end(ap);
  return pStmt;
}

/*
** Estimate the number of rows in the given table or index.
*/
static sqlite3_int64 estEntryCount(const char *zTabIdx){
  double sum = 0.0;
  int i;
  int n = 0;
  sqlite3_stmt *pStmt;
# define N_CNT_SAMPLE 10
  for(i=0; i<=N_CNT_SAMPLE; i++){
    pStmt = db_prepare("PRAGMA est_count(\"%w\",%g)", 
                       zTabIdx, ((double)i)/(double)(N_CNT_SAMPLE));
    if( sqlite3_step(pStmt)==SQLITE_ROW ){
      sum += sqlite3_column_double(pStmt, 0);
      n++;
    }
    sqlite3_finalize(pStmt);
  }
  return n==0 ? 0 : (sqlite3_int64)(sum/n);
}

/*
** Compare the i-th column of pStmt against pValue.  Return true if they
** are different.
*/
static int columnNotEqual(sqlite3_stmt *pStmt, int i, sqlite3_value *pValue){
  int n1, n2, n;
  if( sqlite3_column_type(pStmt,i)!=sqlite3_value_type(pValue) ) return 1;
  switch( sqlite3_column_type(pStmt,i) ){
    case SQLITE_NULL:
      return 0;  /* Nulls compare equal to one another in this context */

    case SQLITE_INTEGER:
      return sqlite3_column_int64(pStmt,i)!=sqlite3_value_int64(pValue);

    case SQLITE_FLOAT:
      return sqlite3_column_double(pStmt,i)!=sqlite3_value_double(pValue);

    case SQLITE_BLOB:
      n1 = sqlite3_column_bytes(pStmt,i);
      n2 = sqlite3_value_bytes(pValue);
      n = n1<n2 ? n1 : n2;
      if( memcmp(sqlite3_column_blob(pStmt,i), sqlite3_value_blob(pValue),n) ){
        return 1;
      }
      return n1!=n2;

    case SQLITE_TEXT:
      n1 = sqlite3_column_bytes(pStmt,i);
      n2 = sqlite3_value_bytes(pValue);
      n = n1<n2 ? n1 : n2;
      if( memcmp(sqlite3_column_text(pStmt,i), sqlite3_value_text(pValue),n) ){
        return 1;
      }
      return n1!=n2;
 
  }
  return 1;
}

/*
** Stat1 for an index.  Return non-zero if an entry was created.
*/
static int analyzeIndex(const char *zTab, const char *zIdx){
  sqlite3_int64 n = estEntryCount(zIdx);
  sqlite3_stmt *pStmt;
  sqlite3_uint64 *aCnt;
  sqlite3_value **apValue;
  int nCol = 0;
  int nByte;
  int i, j, k;
  int iLimit;
  int nRow = 0;
  char *zRes;
  int szRes;
  int rc;

# define N_SPAN  5
  if( n==0 ) return 0;
  pStmt = db_prepare("PRAGMA index_xinfo=\"%w\"", zIdx);
  while( sqlite3_step(pStmt)==SQLITE_ROW ){
    const char *zColl = (const char*)sqlite3_column_text(pStmt,4);
    if( sqlite3_stricmp(zColl,"binary")!=0 ){
      printf("-- cannot analyze index \"%s\" because column \"%s\" uses"
             " collating sequence \"%s\".\n",
             zIdx, sqlite3_column_text(pStmt, 2), zColl);
      sqlite3_finalize(pStmt);
      return 0;
    }
    if( sqlite3_column_int(pStmt, 5)==0 ) break;
    nCol++;
  }
  sqlite3_finalize(pStmt);
  if( nCol==0 ) return 0;
  nByte = (sizeof(aCnt[0]) + sizeof(apValue[0]))*nCol + 30*(nCol+1);
  aCnt = sqlite3_malloc( nByte );
  if( aCnt==0 ){
    runtimeError("out of memory");
  }
  memset(aCnt, 0, nByte);
  apValue = (sqlite3_value**)&aCnt[nCol];
  zRes = (char*)&apValue[nCol];
  szRes = 30*(nCol+1);

  iLimit = n>10000 ? 100 : 20000;
  pStmt = db_prepare("PRAGMA btree_sample(\"%w\",0.0,%lld)",
                     zIdx, n*2);
  for(i=0; i<N_SPAN; i++){
    k = 0;
    while( k<iLimit && (rc = sqlite3_step(pStmt))==SQLITE_ROW ){
      int iFirst;
      for(iFirst=0; iFirst<nCol; iFirst++){
        if( apValue[iFirst]==0 ) break;
        if( columnNotEqual(pStmt, iFirst, apValue[iFirst]) ) break;
      }
      for(j=iFirst; j<nCol; j++){
        aCnt[j]++;
        sqlite3_value_free(apValue[j]);
        apValue[j] = sqlite3_value_dup(sqlite3_column_value(pStmt,j));
      }
      nRow++;
      k++;
    }
    sqlite3_finalize(pStmt);
    if( rc!=SQLITE_ROW || i==N_SPAN-1 ) break;
    pStmt = db_prepare("PRAGMA btree_sample(\"%w\",%g,%lld)",
                       zIdx, ((double)i)/(double)N_SPAN, n*2);
  }  
  for(j=0; j<nCol; j++) sqlite3_value_free(apValue[j]);
  sqlite3_snprintf(szRes, zRes, "%lld", n);
  k = (int)strlen(zRes);
  for(j=0; j<nCol; j++){
    sqlite3_snprintf(szRes-k, zRes+k, " %d", (nRow+aCnt[j]-1)/aCnt[j]);
    k += (int)strlen(zRes+k);
  }
  pStmt = db_prepare(
     "INSERT INTO temp.est_stat1(tbl,idx,stat)"
     "VALUES(\"%w\",\"%w\",'%s')", zTab, zIdx, zRes
  );
  sqlite3_step(pStmt);
  sqlite3_finalize(pStmt);
  return 1;
}

/*
** Stat1 for a table.
*/
static void analyzeTable(const char *zTab){
  sqlite3_int64 n = estEntryCount(zTab);
  sqlite3_stmt *pStmt;
  int nIndex = 0;
  int isWithoutRowid = 0;
  if( n==0 ){
    printf("-- empty table: %s\n", zTab);
    return;
  }
  if( analyzeIndex(zTab,zTab) ){
    isWithoutRowid = 1;
    nIndex++;
  }
  pStmt = db_prepare("PRAGMA index_list(\"%w\")", zTab);
  while( sqlite3_step(pStmt)==SQLITE_ROW ){
    if( sqlite3_column_text(pStmt,3)[0]=='p' && isWithoutRowid ) continue;
    if( sqlite3_column_int(pStmt,4)==0 ) nIndex++;
    analyzeIndex(zTab, (const char*)sqlite3_column_text(pStmt,1));
  }
  sqlite3_finalize(pStmt);
  if( nIndex==0 ){
    pStmt = db_prepare(
       "INSERT INTO temp.est_stat1(tbl,idx,stat)"
       "VALUES(\"%w\",NULL,'%lld')", zTab, n
    );
    sqlite3_step(pStmt);
    sqlite3_finalize(pStmt);
  }
}


/*
** Print the sqlite3_value X as an SQL literal.
*/
static void printQuoted(FILE *out, sqlite3_value *X){
  switch( sqlite3_value_type(X) ){
    case SQLITE_FLOAT: {
      double r1;
      char zBuf[50];
      r1 = sqlite3_value_double(X);
      sqlite3_snprintf(sizeof(zBuf), zBuf, "%!.15g", r1);
      fprintf(out, "%s", zBuf);
      break;
    }
    case SQLITE_INTEGER: {
      fprintf(out, "%lld", sqlite3_value_int64(X));
      break;
    }
    case SQLITE_BLOB: {
      const unsigned char *zBlob = sqlite3_value_blob(X);
      int nBlob = sqlite3_value_bytes(X);
      if( zBlob ){
        int i;
        fprintf(out, "x'");
        for(i=0; i<nBlob; i++){
          fprintf(out, "%02x", zBlob[i]);
        }
        fprintf(out, "'");
      }else{
        /* Could be an OOM, could be a zero-byte blob */
        fprintf(out, "X''");
      }
      break;
    }
    case SQLITE_TEXT: {
      const unsigned char *zArg = sqlite3_value_text(X);
      int i, j;

      if( zArg==0 ){
        fprintf(out, "NULL");
      }else{
        fprintf(out, "'");
        for(i=j=0; zArg[i]; i++){
          if( zArg[i]=='\'' ){
            fprintf(out, "%.*s'", i-j+1, &zArg[j]);
            j = i+1;
          }
        }
        fprintf(out, "%s'", &zArg[j]);
      }
      break;
    }
    case SQLITE_NULL: {
      fprintf(out, "NULL");
      break;
    }
  }
}

/*
** Output SQL that will recreate the aux.zTab table.
*/
static void dump_table(const char *zTab, const char *zAlias){
  int i;                    /* Loop counter */
  int nCol;                 /* Number of result columns */
  sqlite3_stmt *pStmt;      /* SQL statement */
  const char *zSep;         /* Separator string */

  pStmt = db_prepare("SELECT * FROM %s", zTab);
  nCol = sqlite3_column_count(pStmt);
  while( SQLITE_ROW==sqlite3_step(pStmt) ){
    printf("INSERT INTO %s VALUES", zAlias);
    zSep = "(";
    for(i=0; i<nCol; i++){
      fprintf(stdout, "%s",zSep);
      printQuoted(stdout, sqlite3_column_value(pStmt,i));
      zSep = ",";
    }
    fprintf(stdout, ");\n");
  }
  sqlite3_finalize(pStmt);
}


/*
** Print sketchy documentation for this utility program
*/
static void showHelp(void){
  printf("Usage: %s [options] DBFILE\n", g.zArgv0);
  printf(
"Generate an approximate sqlite_stat1 table for the database in the DBFILE\n"
"file. Write the result to standard output.\n"
"Options:\n"
"  (none yet....)\n"
  );
}

int main(int argc, char **argv){
  const char *zDb = 0;
  int i;
  int rc;
  char *zErrMsg = 0;
  sqlite3_stmt *pStmt;

  g.zArgv0 = argv[0];
  sqlite3_config(SQLITE_CONFIG_SINGLETHREAD);
  for(i=1; i<argc; i++){
    const char *z = argv[i];
    if( z[0]=='-' ){
      z++;
      if( z[0]=='-' ) z++;
      if( strcmp(z,"debug")==0 ){
        if( i==argc-1 ) cmdlineError("missing argument to %s", argv[i]);
        g.fDebug = strtol(argv[++i], 0, 0);
      }else
      if( strcmp(z,"help")==0 ){
        showHelp();
        return 0;
      }else
      {
        cmdlineError("unknown option: %s", argv[i]);
      }
    }else if( zDb==0 ){
      zDb = argv[i];
    }else{
      cmdlineError("unknown argument: %s", argv[i]);
    }
  }
  if( zDb==0 ){
    cmdlineError("database filename required");
  }
  rc = sqlite3_open(zDb, &g.db);
  if( rc ){
    cmdlineError("cannot open database file \"%s\"", zDb);
  }
  rc = sqlite3_exec(g.db, "SELECT * FROM sqlite_master", 0, 0, &zErrMsg);
  if( rc || zErrMsg ){
    cmdlineError("\"%s\" does not appear to be a valid SQLite database", zDb);
  }
  rc = sqlite3_exec(g.db, "CREATE TEMP TABLE est_stat1(tbl,idx,stat);",
                    0, 0, &zErrMsg);
  if( rc || zErrMsg ){
    cmdlineError("Cannot CREATE TEMP TABLE");
  }
  pStmt = db_prepare("SELECT name FROM sqlite_master"
                     " WHERE type='table' AND rootpage>0"
                     "   AND name NOT LIKE 'sqlite_%%'"
                     " ORDER BY name");
  while( sqlite3_step(pStmt)==SQLITE_ROW ){
    const char *zName = (const char*)sqlite3_column_text(pStmt, 0);
    analyzeTable(zName);
  }
  sqlite3_finalize(pStmt);
  dump_table("temp.est_stat1","sqlite_stat1");
  sqlite3_close(g.db);
  return 0;
}
