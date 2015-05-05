/*
** 2015-04-06
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
** This is a utility program that computes the differences in content
** between two SQLite databases.
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
#include "sqlite3.h"

/*
** All global variables are gathered into the "g" singleton.
*/
struct GlobalVars {
  const char *zArgv0;       /* Name of program */
  int bSchemaOnly;          /* Only show schema differences */
  int bSchemaPK;            /* Use the schema-defined PK, not the true PK */
  unsigned fDebug;          /* Debug flags */
  sqlite3 *db;              /* The database connection */
} g;

/*
** Allowed values for g.fDebug
*/
#define DEBUG_COLUMN_NAMES  0x000001
#define DEBUG_DIFF_SQL      0x000002

/*
** Dynamic string object
*/
typedef struct Str Str;
struct Str {
  char *z;        /* Text of the string */
  int nAlloc;     /* Bytes allocated in z[] */
  int nUsed;      /* Bytes actually used in z[] */
};

/*
** Initialize a Str object
*/
static void strInit(Str *p){
  p->z = 0;
  p->nAlloc = 0;
  p->nUsed = 0;
}
  
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
** Free all memory held by a Str object
*/
static void strFree(Str *p){
  sqlite3_free(p->z);
  strInit(p);
}

/*
** Add formatted text to the end of a Str object
*/
static void strPrintf(Str *p, const char *zFormat, ...){
  int nNew;
  for(;;){
    if( p->z ){
      va_list ap;
      va_start(ap, zFormat);
      sqlite3_vsnprintf(p->nAlloc-p->nUsed, p->z+p->nUsed, zFormat, ap);
      va_end(ap);
      nNew = (int)strlen(p->z + p->nUsed);
    }else{
      nNew = p->nAlloc;
    }
    if( p->nUsed+nNew < p->nAlloc-1 ){
      p->nUsed += nNew;
      break;
    }
    p->nAlloc = p->nAlloc*2 + 1000;
    p->z = sqlite3_realloc(p->z, p->nAlloc);
    if( p->z==0 ) runtimeError("out of memory");
  }
}



/* Safely quote an SQL identifier.  Use the minimum amount of transformation
** necessary to allow the string to be used with %s.
**
** Space to hold the returned string is obtained from sqlite3_malloc().  The
** caller is responsible for ensuring this space is freed when no longer
** needed.
*/
static char *safeId(const char *zId){
  /* All SQLite keywords, in alphabetical order */
  static const char *azKeywords[] = {
    "ABORT", "ACTION", "ADD", "AFTER", "ALL", "ALTER", "ANALYZE", "AND", "AS",
    "ASC", "ATTACH", "AUTOINCREMENT", "BEFORE", "BEGIN", "BETWEEN", "BY",
    "CASCADE", "CASE", "CAST", "CHECK", "COLLATE", "COLUMN", "COMMIT",
    "CONFLICT", "CONSTRAINT", "CREATE", "CROSS", "CURRENT_DATE",
    "CURRENT_TIME", "CURRENT_TIMESTAMP", "DATABASE", "DEFAULT", "DEFERRABLE",
    "DEFERRED", "DELETE", "DESC", "DETACH", "DISTINCT", "DROP", "EACH",
    "ELSE", "END", "ESCAPE", "EXCEPT", "EXCLUSIVE", "EXISTS", "EXPLAIN",
    "FAIL", "FOR", "FOREIGN", "FROM", "FULL", "GLOB", "GROUP", "HAVING", "IF",
    "IGNORE", "IMMEDIATE", "IN", "INDEX", "INDEXED", "INITIALLY", "INNER",
    "INSERT", "INSTEAD", "INTERSECT", "INTO", "IS", "ISNULL", "JOIN", "KEY",
    "LEFT", "LIKE", "LIMIT", "MATCH", "NATURAL", "NO", "NOT", "NOTNULL",
    "NULL", "OF", "OFFSET", "ON", "OR", "ORDER", "OUTER", "PLAN", "PRAGMA",
    "PRIMARY", "QUERY", "RAISE", "RECURSIVE", "REFERENCES", "REGEXP",
    "REINDEX", "RELEASE", "RENAME", "REPLACE", "RESTRICT", "RIGHT",
    "ROLLBACK", "ROW", "SAVEPOINT", "SELECT", "SET", "TABLE", "TEMP",
    "TEMPORARY", "THEN", "TO", "TRANSACTION", "TRIGGER", "UNION", "UNIQUE",
    "UPDATE", "USING", "VACUUM", "VALUES", "VIEW", "VIRTUAL", "WHEN", "WHERE",
    "WITH", "WITHOUT",
  };
  int lwr, upr, mid, c, i, x;
  for(i=x=0; (c = zId[i])!=0; i++){
    if( !isalpha(c) && c!='_' ){
      if( i>0 && isdigit(c) ){
        x++;
      }else{
        return sqlite3_mprintf("\"%w\"", zId);
      }
    }
  }
  if( x ) return sqlite3_mprintf("%s", zId);
  lwr = 0;
  upr = sizeof(azKeywords)/sizeof(azKeywords[0]) - 1;
  while( lwr<=upr ){
    mid = (lwr+upr)/2;
    c = sqlite3_stricmp(azKeywords[mid], zId);
    if( c==0 ) return sqlite3_mprintf("\"%w\"", zId);
    if( c<0 ){
      lwr = mid+1;
    }else{
      upr = mid-1;
    }
  }
  return sqlite3_mprintf("%s", zId);
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
** Free a list of strings
*/
static void namelistFree(char **az){
  if( az ){
    int i;
    for(i=0; az[i]; i++) sqlite3_free(az[i]);
    sqlite3_free(az);
  }
}

/*
** Return a list of column names for the table zDb.zTab.  Space to
** hold the list is obtained from sqlite3_malloc() and should released
** using namelistFree() when no longer needed.
**
** Primary key columns are listed first, followed by data columns.
** The number of columns in the primary key is returned in *pnPkey.
**
** Normally, the "primary key" in the previous sentence is the true
** primary key - the rowid or INTEGER PRIMARY KEY for ordinary tables
** or the declared PRIMARY KEY for WITHOUT ROWID tables.  However, if
** the g.bSchemaPK flag is set, then the schema-defined PRIMARY KEY is
** used in all cases.  In that case, entries that have NULL values in
** any of their primary key fields will be excluded from the analysis.
**
** If the primary key for a table is the rowid but rowid is inaccessible,
** then this routine returns a NULL pointer.
**
** Examples:
**    CREATE TABLE t1(a INT UNIQUE, b INTEGER, c TEXT, PRIMARY KEY(c));
**    *pnPKey = 1;
**    az = { "rowid", "a", "b", "c", 0 }  // Normal case
**    az = { "c", "a", "b", 0 }           // g.bSchemaPK==1
**
**    CREATE TABLE t2(a INT UNIQUE, b INTEGER, c TEXT, PRIMARY KEY(b));
**    *pnPKey = 1;
**    az = { "b", "a", "c", 0 }
**
**    CREATE TABLE t3(x,y,z,PRIMARY KEY(y,z));
**    *pnPKey = 1                         // Normal case
**    az = { "rowid", "x", "y", "z", 0 }  // Normal case
**    *pnPKey = 2                         // g.bSchemaPK==1
**    az = { "y", "x", "z", 0 }           // g.bSchemaPK==1
**
**    CREATE TABLE t4(x,y,z,PRIMARY KEY(y,z)) WITHOUT ROWID;
**    *pnPKey = 2
**    az = { "y", "z", "x", 0 }
**
**    CREATE TABLE t5(rowid,_rowid_,oid);
**    az = 0     // The rowid is not accessible
*/
static char **columnNames(const char *zDb, const char *zTab, int *pnPKey){
  char **az = 0;           /* List of column names to be returned */
  int naz = 0;             /* Number of entries in az[] */
  sqlite3_stmt *pStmt;     /* SQL statement being run */
  char *zPkIdxName = 0;    /* Name of the PRIMARY KEY index */
  int truePk = 0;          /* PRAGMA table_info indentifies the PK to use */
  int nPK = 0;             /* Number of PRIMARY KEY columns */
  int i, j;                /* Loop counters */

  if( g.bSchemaPK==0 ){
    /* Normal case:  Figure out what the true primary key is for the table.
    **   *  For WITHOUT ROWID tables, the true primary key is the same as
    **      the schema PRIMARY KEY, which is guaranteed to be present.
    **   *  For rowid tables with an INTEGER PRIMARY KEY, the true primary
    **      key is the INTEGER PRIMARY KEY.
    **   *  For all other rowid tables, the rowid is the true primary key.
    */
    pStmt = db_prepare("PRAGMA %s.index_list=%Q", zDb, zTab);
    while( SQLITE_ROW==sqlite3_step(pStmt) ){
      if( sqlite3_stricmp((const char*)sqlite3_column_text(pStmt,3),"pk")==0 ){
        zPkIdxName = sqlite3_mprintf("%s", sqlite3_column_text(pStmt, 1));
        break;
      }
    }
    sqlite3_finalize(pStmt);
    if( zPkIdxName ){
      int nKey = 0;
      int nCol = 0;
      truePk = 0;
      pStmt = db_prepare("PRAGMA %s.index_xinfo=%Q", zDb, zPkIdxName);
      while( SQLITE_ROW==sqlite3_step(pStmt) ){
        nCol++;
        if( sqlite3_column_int(pStmt,5) ){ nKey++; continue; }
        if( sqlite3_column_int(pStmt,1)>=0 ) truePk = 1;
      }
      if( nCol==nKey ) truePk = 1;
      if( truePk ){
        nPK = nKey;
      }else{
        nPK = 1;
      }
      sqlite3_finalize(pStmt);
      sqlite3_free(zPkIdxName);
    }else{
      truePk = 1;
      nPK = 1;
    }
    pStmt = db_prepare("PRAGMA %s.table_info=%Q", zDb, zTab);
  }else{
    /* The g.bSchemaPK==1 case:  Use whatever primary key is declared
    ** in the schema.  The "rowid" will still be used as the primary key
    ** if the table definition does not contain a PRIMARY KEY.
    */
    nPK = 0;
    pStmt = db_prepare("PRAGMA %s.table_info=%Q", zDb, zTab);
    while( SQLITE_ROW==sqlite3_step(pStmt) ){
      if( sqlite3_column_int(pStmt,5)>0 ) nPK++;
    }
    sqlite3_reset(pStmt);
    if( nPK==0 ) nPK = 1;
    truePk = 1;
  }
  *pnPKey = nPK;
  naz = nPK;
  az = sqlite3_malloc( sizeof(char*)*(nPK+1) );
  if( az==0 ) runtimeError("out of memory");
  memset(az, 0, sizeof(char*)*(nPK+1));
  while( SQLITE_ROW==sqlite3_step(pStmt) ){
    int iPKey;
    if( truePk && (iPKey = sqlite3_column_int(pStmt,5))>0 ){
      az[iPKey-1] = safeId((char*)sqlite3_column_text(pStmt,1));
    }else{
      az = sqlite3_realloc(az, sizeof(char*)*(naz+2) );
      if( az==0 ) runtimeError("out of memory");
      az[naz++] = safeId((char*)sqlite3_column_text(pStmt,1));
    }
  }
  sqlite3_finalize(pStmt);
  if( az ) az[naz] = 0;
  if( az[0]==0 ){
    const char *azRowid[] = { "rowid", "_rowid_", "oid" };
    for(i=0; i<sizeof(azRowid)/sizeof(azRowid[0]); i++){
      for(j=1; j<naz; j++){
        if( sqlite3_stricmp(az[j], azRowid[i])==0 ) break;
      }
      if( j>=naz ){
        az[0] = sqlite3_mprintf("%s", azRowid[i]);
        break;
      }
    }
    if( az[0]==0 ){
      for(i=1; i<naz; i++) sqlite3_free(az[i]);
      sqlite3_free(az);
      az = 0;
    }
  }
  return az;
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
        fprintf(out, "NULL");
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
static void dump_table(const char *zTab, FILE *out){
  char *zId = safeId(zTab); /* Name of the table */
  char **az = 0;            /* List of columns */
  int nPk;                  /* Number of true primary key columns */
  int nCol;                 /* Number of data columns */
  int i;                    /* Loop counter */
  sqlite3_stmt *pStmt;      /* SQL statement */
  const char *zSep;         /* Separator string */
  Str ins;                  /* Beginning of the INSERT statement */

  pStmt = db_prepare("SELECT sql FROM aux.sqlite_master WHERE name=%Q", zTab);
  if( SQLITE_ROW==sqlite3_step(pStmt) ){
    fprintf(out, "%s;\n", sqlite3_column_text(pStmt,0));
  }
  sqlite3_finalize(pStmt);
  if( !g.bSchemaOnly ){
    az = columnNames("aux", zTab, &nPk);
    strInit(&ins);
    if( az==0 ){
      pStmt = db_prepare("SELECT * FROM aux.%s", zId);
      strPrintf(&ins,"INSERT INTO %s VALUES", zId);
    }else{
      Str sql;
      strInit(&sql);
      zSep =  "SELECT";
      for(i=0; az[i]; i++){
        strPrintf(&sql, "%s %s", zSep, az[i]);
        zSep = ",";
      }
      strPrintf(&sql," FROM aux.%s", zId);
      zSep = " ORDER BY";
      for(i=1; i<=nPk; i++){
        strPrintf(&sql, "%s %d", zSep, i);
        zSep = ",";
      }
      pStmt = db_prepare("%s", sql.z);
      strFree(&sql);
      strPrintf(&ins, "INSERT INTO %s", zId);
      zSep = "(";
      for(i=0; az[i]; i++){
        strPrintf(&ins, "%s%s", zSep, az[i]);
        zSep = ",";
      }
      strPrintf(&ins,") VALUES");
      namelistFree(az);
    }
    nCol = sqlite3_column_count(pStmt);
    while( SQLITE_ROW==sqlite3_step(pStmt) ){
      fprintf(out, "%s",ins.z);
      zSep = "(";
      for(i=0; i<nCol; i++){
        fprintf(out, "%s",zSep);
        printQuoted(out, sqlite3_column_value(pStmt,i));
        zSep = ",";
      }
      fprintf(out, ");\n");
    }
    sqlite3_finalize(pStmt);
    strFree(&ins);
  } /* endif !g.bSchemaOnly */
  pStmt = db_prepare("SELECT sql FROM aux.sqlite_master"
                     " WHERE type='index' AND tbl_name=%Q AND sql IS NOT NULL",
                     zTab);
  while( SQLITE_ROW==sqlite3_step(pStmt) ){
    fprintf(out, "%s;\n", sqlite3_column_text(pStmt,0));
  }
  sqlite3_finalize(pStmt);
}


/*
** Compute all differences for a single table.
*/
static void diff_one_table(const char *zTab, FILE *out){
  char *zId = safeId(zTab); /* Name of table (translated for us in SQL) */
  char **az = 0;            /* Columns in main */
  char **az2 = 0;           /* Columns in aux */
  int nPk;                  /* Primary key columns in main */
  int nPk2;                 /* Primary key columns in aux */
  int n = 0;                /* Number of columns in main */
  int n2;                   /* Number of columns in aux */
  int nQ;                   /* Number of output columns in the diff query */
  int i;                    /* Loop counter */
  const char *zSep;         /* Separator string */
  Str sql;                  /* Comparison query */
  sqlite3_stmt *pStmt;      /* Query statement to do the diff */

  strInit(&sql);
  if( g.fDebug==DEBUG_COLUMN_NAMES ){
    /* Simply run columnNames() on all tables of the origin
    ** database and show the results.  This is used for testing
    ** and debugging of the columnNames() function.
    */
    az = columnNames("aux",zTab, &nPk);
    if( az==0 ){
      printf("Rowid not accessible for %s\n", zId);
    }else{
      printf("%s:", zId);
      for(i=0; az[i]; i++){
        printf(" %s", az[i]);
        if( i+1==nPk ) printf(" *");
      }
      printf("\n");
    }
    goto end_diff_one_table;
  }
    

  if( sqlite3_table_column_metadata(g.db,"aux",zTab,0,0,0,0,0,0) ){
    if( !sqlite3_table_column_metadata(g.db,"main",zTab,0,0,0,0,0,0) ){
      /* Table missing from second database. */
      fprintf(out, "DROP TABLE %s;\n", zId);
    }
    goto end_diff_one_table;
  }

  if( sqlite3_table_column_metadata(g.db,"main",zTab,0,0,0,0,0,0) ){
    /* Table missing from source */
    dump_table(zTab, out);
    goto end_diff_one_table;
  }

  az = columnNames("main", zTab, &nPk);
  az2 = columnNames("aux", zTab, &nPk2);
  if( az && az2 ){
    for(n=0; az[n]; n++){
      if( sqlite3_stricmp(az[n],az2[n])!=0 ) break;
    }
  }
  if( az==0
   || az2==0
   || nPk!=nPk2
   || az[n]
  ){
    /* Schema mismatch */
    fprintf(out, "DROP TABLE %s;\n", zId);
    dump_table(zTab, out);
    goto end_diff_one_table;
  }

  /* Build the comparison query */
  for(n2=n; az[n2]; n2++){}
  nQ = nPk2+1+2*(n2-nPk2);
  if( n2>nPk2 ){
    zSep = "SELECT ";
    for(i=0; i<nPk; i++){
      strPrintf(&sql, "%sB.%s", zSep, az[i]);
      zSep = ", ";
    }
    strPrintf(&sql, ", 1%s -- changed row\n", nPk==n ? "" : ",");
    while( az[i] ){
      strPrintf(&sql, "       A.%s IS NOT B.%s, B.%s%s\n",
                az[i], az[i], az[i], i==n2-1 ? "" : ",");
      i++;
    }
    strPrintf(&sql, "  FROM main.%s A, aux.%s B\n", zId, zId);
    zSep = " WHERE";
    for(i=0; i<nPk; i++){
      strPrintf(&sql, "%s A.%s=B.%s", zSep, az[i], az[i]);
      zSep = " AND";
    }
    zSep = "\n   AND (";
    while( az[i] ){
      strPrintf(&sql, "%sA.%s IS NOT B.%s%s\n",
                zSep, az[i], az[i], i==n2-1 ? ")" : "");
      zSep = "        OR ";
      i++;
    }
    strPrintf(&sql, " UNION ALL\n");
  }
  zSep = "SELECT ";
  for(i=0; i<nPk; i++){
    strPrintf(&sql, "%sA.%s", zSep, az[i]);
    zSep = ", ";
  }
  strPrintf(&sql, ", 2%s -- deleted row\n", nPk==n ? "" : ",");
  while( az[i] ){
    strPrintf(&sql, "       NULL, NULL%s\n", i==n2-1 ? "" : ",");
    i++;
  }
  strPrintf(&sql, "  FROM main.%s A\n", zId);
  strPrintf(&sql, " WHERE NOT EXISTS(SELECT 1 FROM aux.%s B\n", zId);
  zSep =          "                   WHERE";
  for(i=0; i<nPk; i++){
    strPrintf(&sql, "%s A.%s=B.%s", zSep, az[i], az[i]);
    zSep = " AND";
  }
  strPrintf(&sql, ")\n");
  zSep = " UNION ALL\nSELECT ";
  for(i=0; i<nPk; i++){
    strPrintf(&sql, "%sB.%s", zSep, az[i]);
    zSep = ", ";
  }
  strPrintf(&sql, ", 3%s -- inserted row\n", nPk==n ? "" : ",");
  while( az2[i] ){
    strPrintf(&sql, "       1, B.%s%s\n", az[i], i==n2-1 ? "" : ",");
    i++;
  }
  strPrintf(&sql, "  FROM aux.%s B\n", zId);
  strPrintf(&sql, " WHERE NOT EXISTS(SELECT 1 FROM main.%s A\n", zId);
  zSep =          "                   WHERE";
  for(i=0; i<nPk; i++){
    strPrintf(&sql, "%s A.%s=B.%s", zSep, az[i], az[i]);
    zSep = " AND";
  }
  strPrintf(&sql, ")\n ORDER BY");
  zSep = " ";
  for(i=1; i<=nPk; i++){
    strPrintf(&sql, "%s%d", zSep, i);
    zSep = ", ";
  }
  strPrintf(&sql, ";\n");

  if( g.fDebug & DEBUG_DIFF_SQL ){ 
    printf("SQL for %s:\n%s\n", zId, sql.z);
    goto end_diff_one_table;
  }

  /* Drop indexes that are missing in the destination */
  pStmt = db_prepare(
    "SELECT name FROM main.sqlite_master"
    " WHERE type='index' AND tbl_name=%Q"
    "   AND sql IS NOT NULL"
    "   AND sql NOT IN (SELECT sql FROM aux.sqlite_master"
    "                    WHERE type='index' AND tbl_name=%Q"
    "                      AND sql IS NOT NULL)",
    zTab, zTab);
  while( SQLITE_ROW==sqlite3_step(pStmt) ){
    char *z = safeId((const char*)sqlite3_column_text(pStmt,0));
    fprintf(out, "DROP INDEX %s;\n", z);
    sqlite3_free(z);
  }
  sqlite3_finalize(pStmt);

  /* Run the query and output differences */
  if( !g.bSchemaOnly ){
    pStmt = db_prepare(sql.z);
    while( SQLITE_ROW==sqlite3_step(pStmt) ){
      int iType = sqlite3_column_int(pStmt, nPk);
      if( iType==1 || iType==2 ){
        if( iType==1 ){       /* Change the content of a row */
          fprintf(out, "UPDATE %s", zId);
          zSep = " SET";
          for(i=nPk+1; i<nQ; i+=2){
            if( sqlite3_column_int(pStmt,i)==0 ) continue;
            fprintf(out, "%s %s=", zSep, az2[(i+nPk-1)/2]);
            zSep = ",";
            printQuoted(out, sqlite3_column_value(pStmt,i+1));
          }
        }else{                /* Delete a row */
          fprintf(out, "DELETE FROM %s", zId);
        }
        zSep = " WHERE";
        for(i=0; i<nPk; i++){
          fprintf(out, "%s %s=", zSep, az2[i]);
          printQuoted(out, sqlite3_column_value(pStmt,i));
          zSep = ",";
        }
        fprintf(out, ";\n");
      }else{                  /* Insert a row */
        fprintf(out, "INSERT INTO %s(%s", zId, az2[0]);
        for(i=1; az2[i]; i++) fprintf(out, ",%s", az2[i]);
        fprintf(out, ") VALUES");
        zSep = "(";
        for(i=0; i<nPk2; i++){
          fprintf(out, "%s", zSep);
          zSep = ",";
          printQuoted(out, sqlite3_column_value(pStmt,i));
        }
        for(i=nPk2+2; i<nQ; i+=2){
          fprintf(out, ",");
          printQuoted(out, sqlite3_column_value(pStmt,i));
        }
        fprintf(out, ");\n");
      }
    }
    sqlite3_finalize(pStmt);
  } /* endif !g.bSchemaOnly */

  /* Create indexes that are missing in the source */
  pStmt = db_prepare(
    "SELECT sql FROM aux.sqlite_master"
    " WHERE type='index' AND tbl_name=%Q"
    "   AND sql IS NOT NULL"
    "   AND sql NOT IN (SELECT sql FROM main.sqlite_master"
    "                    WHERE type='index' AND tbl_name=%Q"
    "                      AND sql IS NOT NULL)",
    zTab, zTab);
  while( SQLITE_ROW==sqlite3_step(pStmt) ){
    fprintf(out, "%s;\n", sqlite3_column_text(pStmt,0));
  }
  sqlite3_finalize(pStmt);

end_diff_one_table:
  strFree(&sql);
  sqlite3_free(zId);
  namelistFree(az);
  namelistFree(az2);
  return;
}

/*
** Display a summary of differences between two versions of the same
** table table.
**
**   *  Number of rows changed
**   *  Number of rows added
**   *  Number of rows deleted
**   *  Number of identical rows
*/
static void summarize_one_table(const char *zTab, FILE *out){
  char *zId = safeId(zTab); /* Name of table (translated for us in SQL) */
  char **az = 0;            /* Columns in main */
  char **az2 = 0;           /* Columns in aux */
  int nPk;                  /* Primary key columns in main */
  int nPk2;                 /* Primary key columns in aux */
  int n = 0;                /* Number of columns in main */
  int n2;                   /* Number of columns in aux */
  int i;                    /* Loop counter */
  const char *zSep;         /* Separator string */
  Str sql;                  /* Comparison query */
  sqlite3_stmt *pStmt;      /* Query statement to do the diff */
  sqlite3_int64 nUpdate;    /* Number of updated rows */
  sqlite3_int64 nUnchanged; /* Number of unmodified rows */
  sqlite3_int64 nDelete;    /* Number of deleted rows */
  sqlite3_int64 nInsert;    /* Number of inserted rows */

  strInit(&sql);
  if( sqlite3_table_column_metadata(g.db,"aux",zTab,0,0,0,0,0,0) ){
    if( !sqlite3_table_column_metadata(g.db,"main",zTab,0,0,0,0,0,0) ){
      /* Table missing from second database. */
      fprintf(out, "%s: missing from second database\n", zTab);
    }
    goto end_summarize_one_table;
  }

  if( sqlite3_table_column_metadata(g.db,"main",zTab,0,0,0,0,0,0) ){
    /* Table missing from source */
    fprintf(out, "%s: missing from first database\n", zTab);
    goto end_summarize_one_table;
  }

  az = columnNames("main", zTab, &nPk);
  az2 = columnNames("aux", zTab, &nPk2);
  if( az && az2 ){
    for(n=0; az[n]; n++){
      if( sqlite3_stricmp(az[n],az2[n])!=0 ) break;
    }
  }
  if( az==0
   || az2==0
   || nPk!=nPk2
   || az[n]
  ){
    /* Schema mismatch */
    fprintf(out, "%s: incompatible schema\n", zTab);
    goto end_summarize_one_table;
  }

  /* Build the comparison query */
  for(n2=n; az[n2]; n2++){}
  strPrintf(&sql, "SELECT 1, count(*)");
  if( n2==nPk2 ){
    strPrintf(&sql, ", 0\n");
  }else{
    zSep = ", sum(";
    for(i=nPk; az[i]; i++){
      strPrintf(&sql, "%sA.%s IS NOT B.%s", zSep, az[i], az[i]);
      zSep = " OR ";
    }
    strPrintf(&sql, ")\n");
  }
  strPrintf(&sql, "  FROM main.%s A, aux.%s B\n", zId, zId);
  zSep = " WHERE";
  for(i=0; i<nPk; i++){
    strPrintf(&sql, "%s A.%s=B.%s", zSep, az[i], az[i]);
    zSep = " AND";
  }
  strPrintf(&sql, " UNION ALL\n");
  strPrintf(&sql, "SELECT 2, count(*), 0\n");
  strPrintf(&sql, "  FROM main.%s A\n", zId);
  strPrintf(&sql, " WHERE NOT EXISTS(SELECT 1 FROM aux.%s B ", zId);
  zSep = "WHERE";
  for(i=0; i<nPk; i++){
    strPrintf(&sql, "%s A.%s=B.%s", zSep, az[i], az[i]);
    zSep = " AND";
  }
  strPrintf(&sql, ")\n");
  strPrintf(&sql, " UNION ALL\n");
  strPrintf(&sql, "SELECT 3, count(*), 0\n");
  strPrintf(&sql, "  FROM aux.%s B\n", zId);
  strPrintf(&sql, " WHERE NOT EXISTS(SELECT 1 FROM main.%s A ", zId);
  zSep = "WHERE";
  for(i=0; i<nPk; i++){
    strPrintf(&sql, "%s A.%s=B.%s", zSep, az[i], az[i]);
    zSep = " AND";
  }
  strPrintf(&sql, ")\n ORDER BY 1;\n");

  if( (g.fDebug & DEBUG_DIFF_SQL)!=0 ){ 
    printf("SQL for %s:\n%s\n", zId, sql.z);
    goto end_summarize_one_table;
  }

  /* Run the query and output difference summary */
  pStmt = db_prepare(sql.z);
  nUpdate = 0;
  nInsert = 0;
  nDelete = 0;
  nUnchanged = 0;
  while( SQLITE_ROW==sqlite3_step(pStmt) ){
    switch( sqlite3_column_int(pStmt,0) ){
      case 1:
        nUpdate = sqlite3_column_int64(pStmt,2);
        nUnchanged = sqlite3_column_int64(pStmt,1) - nUpdate;
        break;
      case 2:
        nDelete = sqlite3_column_int64(pStmt,1);
        break;
      case 3:
        nInsert = sqlite3_column_int64(pStmt,1);
        break;
    }
  }
  sqlite3_finalize(pStmt);
  fprintf(out, "%s: %lld changes, %lld inserts, %lld deletes, %lld unchanged\n",
          zTab, nUpdate, nInsert, nDelete, nUnchanged);

end_summarize_one_table:
  strFree(&sql);
  sqlite3_free(zId);
  namelistFree(az);
  namelistFree(az2);
  return;
}

/*
** Write a 64-bit signed integer as a varint onto out
*/
static void putsVarint(FILE *out, sqlite3_uint64 v){
  int i, n;
  unsigned char p[12];
  if( v & (((sqlite3_uint64)0xff000000)<<32) ){
    p[8] = (unsigned char)v;
    v >>= 8;
    for(i=7; i>=0; i--){
      p[i] = (unsigned char)((v & 0x7f) | 0x80);
      v >>= 7;
    }
    fwrite(p, 8, 1, out);
  }else{
    n = 9;
    do{
      p[n--] = (unsigned char)((v & 0x7f) | 0x80);
      v >>= 7;
    }while( v!=0 );
    p[9] &= 0x7f;
    fwrite(p+n+1, 9-n, 1, out);
  }
}

/*
** Write an SQLite value onto out.
*/
static void putValue(FILE *out, sqlite3_value *pVal){
  int iDType = sqlite3_value_type(pVal);
  sqlite3_int64 iX;
  double rX;
  sqlite3_uint64 uX;
  int j;

  putc(iDType, out);
  switch( iDType ){
    case SQLITE_INTEGER:
      iX = sqlite3_value_int64(pVal);
      memcpy(&uX, &iX, 8);
      for(j=56; j>=0; j-=8) putc((uX>>j)&0xff, out);
      break;
    case SQLITE_FLOAT:
      rX = sqlite3_value_double(pVal);
      memcpy(&uX, &rX, 8);
      for(j=56; j>=0; j-=8) putc((uX>>j)&0xff, out);
      break;
    case SQLITE_TEXT:
      iX = sqlite3_value_bytes(pVal);
      putsVarint(out, (sqlite3_uint64)iX);
      fwrite(sqlite3_value_text(pVal),1,(size_t)iX,out);
      break;
    case SQLITE_BLOB:
      iX = sqlite3_value_bytes(pVal);
      putsVarint(out, (sqlite3_uint64)iX);
      fwrite(sqlite3_value_blob(pVal),1,(size_t)iX,out);
      break;
    case SQLITE_NULL:
      break;
  }
}

/*
** Generate a CHANGESET for all differences from main.zTab to aux.zTab.
*/
static void changeset_one_table(const char *zTab, FILE *out){
  sqlite3_stmt *pStmt;          /* SQL statment */
  char *zId = safeId(zTab);     /* Escaped name of the table */
  char **azCol = 0;             /* List of escaped column names */
  int nCol = 0;                 /* Number of columns */
  int *aiFlg = 0;               /* 0 if column is not part of PK */
  int *aiPk = 0;                /* Column numbers for each PK column */
  int nPk = 0;                  /* Number of PRIMARY KEY columns */
  Str sql;                      /* SQL for the diff query */
  int i, k;                     /* Loop counters */
  const char *zSep;             /* List separator */

  pStmt = db_prepare(
      "SELECT A.sql=B.sql FROM main.sqlite_master A, aux.sqlite_master B"
      " WHERE A.name=%Q AND B.name=%Q", zTab, zTab
  );
  if( SQLITE_ROW==sqlite3_step(pStmt) ){
    if( sqlite3_column_int(pStmt,0)==0 ){
      runtimeError("schema changes for table %s", safeId(zTab));
    }
  }else{
    runtimeError("table %s missing from one or both databases", safeId(zTab));
  }
  sqlite3_finalize(pStmt);
  pStmt = db_prepare("PRAGMA main.table_info=%Q", zTab);
  while( SQLITE_ROW==sqlite3_step(pStmt) ){
    nCol++;
    azCol = sqlite3_realloc(azCol, sizeof(char*)*nCol);
    if( azCol==0 ) runtimeError("out of memory");
    aiFlg = sqlite3_realloc(aiFlg, sizeof(int)*nCol);
    if( aiFlg==0 ) runtimeError("out of memory");
    azCol[nCol-1] = safeId((const char*)sqlite3_column_text(pStmt,1));
    aiFlg[nCol-1] = i = sqlite3_column_int(pStmt,5);
    if( i>0 ){
      if( i>nPk ){
        nPk = i;
        aiPk = sqlite3_realloc(aiPk, sizeof(int)*nPk);
        if( aiPk==0 ) runtimeError("out of memory");
      }
      aiPk[i-1] = nCol-1;
    }
  }
  sqlite3_finalize(pStmt);
  if( nPk==0 ) goto end_changeset_one_table; 
  strInit(&sql);
  if( nCol>nPk ){
    strPrintf(&sql, "SELECT %d", SQLITE_UPDATE);
    for(i=0; i<nCol; i++){
      if( aiFlg[i] ){
        strPrintf(&sql, ",\n       A.%s", azCol[i]);
      }else{
        strPrintf(&sql, ",\n       A.%s IS NOT B.%s, A.%s, B.%s",
                  azCol[i], azCol[i], azCol[i], azCol[i]);
      }
    }
    strPrintf(&sql,"\n  FROM main.%s A, aux.%s B\n", zId, zId);
    zSep = " WHERE";
    for(i=0; i<nPk; i++){
      strPrintf(&sql, "%s A.%s=B.%s", zSep, azCol[aiPk[i]], azCol[aiPk[i]]);
      zSep = " AND";
    }
    zSep = "\n   AND (";
    for(i=0; i<nCol; i++){
      if( aiFlg[i] ) continue;
      strPrintf(&sql, "%sA.%s IS NOT B.%s", zSep, azCol[i], azCol[i]);
      zSep = " OR\n        ";
    }
    strPrintf(&sql,")\n UNION ALL\n");
  }
  strPrintf(&sql, "SELECT %d", SQLITE_DELETE);
  for(i=0; i<nCol; i++){
    if( aiFlg[i] ){
      strPrintf(&sql, ",\n       A.%s", azCol[i]);
    }else{
      strPrintf(&sql, ",\n       1, A.%s, NULL", azCol[i]);
    }
  }
  strPrintf(&sql, "\n  FROM main.%s A\n", zId);
  strPrintf(&sql, " WHERE NOT EXISTS(SELECT 1 FROM aux.%s B\n", zId);
  zSep =          "                   WHERE";
  for(i=0; i<nPk; i++){
    strPrintf(&sql, "%s A.%s=B.%s", zSep, azCol[aiPk[i]], azCol[aiPk[i]]);
    zSep = " AND";
  }
  strPrintf(&sql, ")\n UNION ALL\n");
  strPrintf(&sql, "SELECT %d", SQLITE_INSERT);
  for(i=0; i<nCol; i++){
    if( aiFlg[i] ){
      strPrintf(&sql, ",\n       B.%s", azCol[i]);
    }else{
      strPrintf(&sql, ",\n       1, NULL, B.%s", azCol[i]);
    }
  }
  strPrintf(&sql, "\n  FROM aux.%s B\n", zId);
  strPrintf(&sql, " WHERE NOT EXISTS(SELECT 1 FROM main.%s A\n", zId);
  zSep =          "                   WHERE";
  for(i=0; i<nPk; i++){
    strPrintf(&sql, "%s A.%s=B.%s", zSep, azCol[aiPk[i]], azCol[aiPk[i]]);
    zSep = " AND";
  }
  strPrintf(&sql, ")\n");
  strPrintf(&sql, " ORDER BY");
  zSep = " ";
  for(i=0; i<nPk; i++){
    strPrintf(&sql, "%s %d", zSep, aiPk[i]+2);
    zSep = ",";
  }
  strPrintf(&sql, ";\n");

  if( g.fDebug & DEBUG_DIFF_SQL ){ 
    printf("SQL for %s:\n%s\n", zId, sql.z);
    goto end_changeset_one_table;
  }

  putc('T', out);
  putsVarint(out, (sqlite3_uint64)nCol);
  for(i=0; i<nCol; i++) putc(aiFlg[i]!=0, out);
  fwrite(zTab, 1, strlen(zTab), out);
  putc(0, out);

  pStmt = db_prepare("%s", sql.z);
  while( SQLITE_ROW==sqlite3_step(pStmt) ){
    int iType = sqlite3_column_int(pStmt,0);
    putc(iType, out);
    putc(0, out);
    switch( sqlite3_column_int(pStmt,0) ){
      case SQLITE_UPDATE: {
        for(k=1, i=0; i<nCol; i++){
          if( aiFlg[i] ){
            putValue(out, sqlite3_column_value(pStmt,k));
            k++;
          }else if( sqlite3_column_int(pStmt,k) ){
            putValue(out, sqlite3_column_value(pStmt,k+1));
            k += 3;
          }else{
            putc(0, out);
            k += 3;
          }
        }
        for(k=1, i=0; i<nCol; i++){
          if( aiFlg[i] ){
            putc(0, out);
            k++;
          }else if( sqlite3_column_int(pStmt,k) ){
            putValue(out, sqlite3_column_value(pStmt,k+2));
            k += 3;
          }else{
            putc(0, out);
            k += 3;
          }
        }
        break;
      }
      case SQLITE_INSERT: {
        for(k=1, i=0; i<nCol; i++){
          if( aiFlg[i] ){
            putValue(out, sqlite3_column_value(pStmt,k));
            k++;
          }else{
            putValue(out, sqlite3_column_value(pStmt,k+2));
            k += 3;
          }
        }
        break;
      }
      case SQLITE_DELETE: {
        for(k=1, i=0; i<nCol; i++){
          if( aiFlg[i] ){
            putValue(out, sqlite3_column_value(pStmt,k));
            k++;
          }else{
            putValue(out, sqlite3_column_value(pStmt,k+1));
            k += 3;
          }
        }
        break;
      }
    }
  }
  sqlite3_finalize(pStmt);
  
end_changeset_one_table:
  while( nCol>0 ) sqlite3_free(azCol[--nCol]);
  sqlite3_free(azCol);
  sqlite3_free(aiPk);
  sqlite3_free(zId);
}

/*
** Print sketchy documentation for this utility program
*/
static void showHelp(void){
  printf("Usage: %s [options] DB1 DB2\n", g.zArgv0);
  printf(
"Output SQL text that would transform DB1 into DB2.\n"
"Options:\n"
"  --changeset FILE      Write a CHANGESET into FILE\n"
"  -L|--lib LIBRARY      Load an SQLite extension library\n"
"  --primarykey          Use schema-defined PRIMARY KEYs\n"
"  --schema              Show only differences in the schema\n"
"  --summary             Show only a summary of the differences\n"
"  --table TAB           Show only differences in table TAB\n"
  );
}

int main(int argc, char **argv){
  const char *zDb1 = 0;
  const char *zDb2 = 0;
  int i;
  int rc;
  char *zErrMsg = 0;
  char *zSql;
  sqlite3_stmt *pStmt;
  char *zTab = 0;
  FILE *out = stdout;
  void (*xDiff)(const char*,FILE*) = diff_one_table;
  int nExt = 0;
  char **azExt = 0;

  g.zArgv0 = argv[0];
  for(i=1; i<argc; i++){
    const char *z = argv[i];
    if( z[0]=='-' ){
      z++;
      if( z[0]=='-' ) z++;
      if( strcmp(z,"changeset")==0 ){
        if( i==argc-1 ) cmdlineError("missing argument to %s", argv[i]);
        out = fopen(argv[++i], "wb");
        if( out==0 ) cmdlineError("cannot open: %s", argv[i]);
        xDiff = changeset_one_table;
      }else
      if( strcmp(z,"debug")==0 ){
        if( i==argc-1 ) cmdlineError("missing argument to %s", argv[i]);
        g.fDebug = strtol(argv[++i], 0, 0);
      }else
      if( strcmp(z,"help")==0 ){
        showHelp();
        return 0;
      }else
      if( strcmp(z,"lib")==0 || strcmp(z,"L")==0 ){
        if( i==argc-1 ) cmdlineError("missing argument to %s", argv[i]);
        azExt = realloc(azExt, sizeof(azExt[0])*(nExt+1));
        if( azExt==0 ) cmdlineError("out of memory");
        azExt[nExt++] = argv[++i];
      }else
      if( strcmp(z,"primarykey")==0 ){
        g.bSchemaPK = 1;
      }else
      if( strcmp(z,"schema")==0 ){
        g.bSchemaOnly = 1;
      }else
      if( strcmp(z,"summary")==0 ){
        xDiff = summarize_one_table;
      }else
      if( strcmp(z,"table")==0 ){
        if( i==argc-1 ) cmdlineError("missing argument to %s", argv[i]);
        zTab = argv[++i];
      }else
      {
        cmdlineError("unknown option: %s", argv[i]);
      }
    }else if( zDb1==0 ){
      zDb1 = argv[i];
    }else if( zDb2==0 ){
      zDb2 = argv[i];
    }else{
      cmdlineError("unknown argument: %s", argv[i]);
    }
  }
  if( zDb2==0 ){
    cmdlineError("two database arguments required");
  }
  rc = sqlite3_open(zDb1, &g.db);
  if( rc ){
    cmdlineError("cannot open database file \"%s\"", zDb1);
  }
  rc = sqlite3_exec(g.db, "SELECT * FROM sqlite_master", 0, 0, &zErrMsg);
  if( rc || zErrMsg ){
    cmdlineError("\"%s\" does not appear to be a valid SQLite database", zDb1);
  }
  sqlite3_enable_load_extension(g.db, 1);
  for(i=0; i<nExt; i++){
    rc = sqlite3_load_extension(g.db, azExt[i], 0, &zErrMsg);
    if( rc || zErrMsg ){
      cmdlineError("error loading %s: %s", azExt[i], zErrMsg);
    }
  }
  free(azExt);
  zSql = sqlite3_mprintf("ATTACH %Q as aux;", zDb2);
  rc = sqlite3_exec(g.db, zSql, 0, 0, &zErrMsg);
  if( rc || zErrMsg ){
    cmdlineError("cannot attach database \"%s\"", zDb2);
  }
  rc = sqlite3_exec(g.db, "SELECT * FROM aux.sqlite_master", 0, 0, &zErrMsg);
  if( rc || zErrMsg ){
    cmdlineError("\"%s\" does not appear to be a valid SQLite database", zDb2);
  }

  if( zTab ){
    xDiff(zTab, out);
  }else{
    /* Handle tables one by one */
    pStmt = db_prepare(
      "SELECT name FROM main.sqlite_master\n"
      " WHERE type='table' AND sql NOT LIKE 'CREATE VIRTUAL%%'\n"
      " UNION\n"
      "SELECT name FROM aux.sqlite_master\n"
      " WHERE type='table' AND sql NOT LIKE 'CREATE VIRTUAL%%'\n"
      " ORDER BY name"
    );
    while( SQLITE_ROW==sqlite3_step(pStmt) ){
      xDiff((const char*)sqlite3_column_text(pStmt,0), out);
    }
    sqlite3_finalize(pStmt);
  }

  /* TBD: Handle trigger differences */
  /* TBD: Handle view differences */
  sqlite3_close(g.db);
  return 0;
}
