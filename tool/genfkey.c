/*
** 2008 October 10
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains C code for 'genfkey', a program to generate trigger
** definitions that emulate foreign keys. See genfkey.README for details.
**
** $Id: genfkey.c,v 1.2 2008/10/13 10:56:48 danielk1977 Exp $
*/

#include "sqlite3.h"
#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>

/**************************************************************************
***************************************************************************
** Start of virtual table implementations.
**************************************************************************/

/* The code in this file defines a sqlite3 virtual-table module that
** provides a read-only view of the current database schema. There is one
** row in the schema table for each column in the database schema.
*/
#define SCHEMA \
"CREATE TABLE x("                                                            \
  "database,"          /* Name of database (i.e. main, temp etc.) */         \
  "tablename,"         /* Name of table */                                   \
  "cid,"               /* Column number (from left-to-right, 0 upward) */    \
  "name,"              /* Column name */                                     \
  "type,"              /* Specified type (i.e. VARCHAR(32)) */               \
  "not_null,"          /* Boolean. True if NOT NULL was specified */         \
  "dflt_value,"        /* Default value for this column */                   \
  "pk"                 /* True if this column is part of the primary key */  \
")"

#define SCHEMA2 \
"CREATE TABLE x("                                                            \
  "database,"          /* Name of database (i.e. main, temp etc.) */         \
  "from_tbl,"          /* Name of table */                                   \
  "fkid,"                                                                    \
  "seq,"                                                                     \
  "to_tbl,"                                                                  \
  "from_col,"                                                                \
  "to_col,"                                                                  \
  "on_update,"                                                               \
  "on_delete,"                                                               \
  "match"                                                                    \
")"

#define SCHEMA3 \
"CREATE TABLE x("                                                            \
  "database,"          /* Name of database (i.e. main, temp etc.) */         \
  "tablename,"         /* Name of table */                                   \
  "seq,"                                                                     \
  "name,"                                                                    \
  "isunique"                                                                 \
")"

#define SCHEMA4 \
"CREATE TABLE x("                                                            \
  "database,"          /* Name of database (i.e. main, temp etc.) */         \
  "indexname,"         /* Name of table */                                   \
  "seqno,"                                                                   \
  "cid,"                                                                     \
  "name"                                                                     \
")"

typedef struct SchemaTable SchemaTable;
struct SchemaTable {
  const char *zName;
  const char *zObject;
  const char *zPragma;
  const char *zSchema;
} aSchemaTable[] = {
  { "table_info",       "table", "PRAGMA %Q.table_info(%Q)",       SCHEMA },
  { "foreign_key_list", "table", "PRAGMA %Q.foreign_key_list(%Q)", SCHEMA2 },
  { "index_list",       "table", "PRAGMA %Q.index_list(%Q)",       SCHEMA3 },
  { "index_info",       "index", "PRAGMA %Q.index_info(%Q)",       SCHEMA4 },
  { 0, 0, 0, 0 }
};

typedef struct schema_vtab schema_vtab;
typedef struct schema_cursor schema_cursor;

/* A schema table object */
struct schema_vtab {
  sqlite3_vtab base;
  sqlite3 *db;
  SchemaTable *pType;
};

/* A schema table cursor object */
struct schema_cursor {
  sqlite3_vtab_cursor base;
  sqlite3_stmt *pDbList;
  sqlite3_stmt *pTableList;
  sqlite3_stmt *pColumnList;
  int rowid;
};

/*
** Table destructor for the schema module.
*/
static int schemaDestroy(sqlite3_vtab *pVtab){
  sqlite3_free(pVtab);
  return 0;
}

/*
** Table constructor for the schema module.
*/
static int schemaCreate(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  int rc = SQLITE_NOMEM;
  schema_vtab *pVtab;
  SchemaTable *pType = &aSchemaTable[0];

  if( argc>3 ){
    int i;
    pType = 0;
    for(i=0; aSchemaTable[i].zName; i++){ 
      if( 0==strcmp(argv[3], aSchemaTable[i].zName) ){
        pType = &aSchemaTable[i];
      }
    }
    if( !pType ){
      return SQLITE_ERROR;
    }
  }

  pVtab = sqlite3_malloc(sizeof(schema_vtab));
  if( pVtab ){
    memset(pVtab, 0, sizeof(schema_vtab));
    pVtab->db = db;
    pVtab->pType = pType;
    rc = sqlite3_declare_vtab(db, pType->zSchema);
  }
  *ppVtab = (sqlite3_vtab *)pVtab;
  return rc;
}

/*
** Open a new cursor on the schema table.
*/
static int schemaOpen(sqlite3_vtab *pVTab, sqlite3_vtab_cursor **ppCursor){
  int rc = SQLITE_NOMEM;
  schema_cursor *pCur;
  pCur = sqlite3_malloc(sizeof(schema_cursor));
  if( pCur ){
    memset(pCur, 0, sizeof(schema_cursor));
    *ppCursor = (sqlite3_vtab_cursor *)pCur;
    rc = SQLITE_OK;
  }
  return rc;
}

/*
** Close a schema table cursor.
*/
static int schemaClose(sqlite3_vtab_cursor *cur){
  schema_cursor *pCur = (schema_cursor *)cur;
  sqlite3_finalize(pCur->pDbList);
  sqlite3_finalize(pCur->pTableList);
  sqlite3_finalize(pCur->pColumnList);
  sqlite3_free(pCur);
  return SQLITE_OK;
}

/*
** Retrieve a column of data.
*/
static int schemaColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int i){
  schema_cursor *pCur = (schema_cursor *)cur;
  switch( i ){
    case 0:
      sqlite3_result_value(ctx, sqlite3_column_value(pCur->pDbList, 1));
      break;
    case 1:
      sqlite3_result_value(ctx, sqlite3_column_value(pCur->pTableList, 0));
      break;
    default:
      sqlite3_result_value(ctx, sqlite3_column_value(pCur->pColumnList, i-2));
      break;
  }
  return SQLITE_OK;
}

/*
** Retrieve the current rowid.
*/
static int schemaRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  schema_cursor *pCur = (schema_cursor *)cur;
  *pRowid = pCur->rowid;
  return SQLITE_OK;
}

static int finalize(sqlite3_stmt **ppStmt){
  int rc = sqlite3_finalize(*ppStmt);
  *ppStmt = 0;
  return rc;
}

static int schemaEof(sqlite3_vtab_cursor *cur){
  schema_cursor *pCur = (schema_cursor *)cur;
  return (pCur->pDbList ? 0 : 1);
}

/*
** Advance the cursor to the next row.
*/
static int schemaNext(sqlite3_vtab_cursor *cur){
  int rc = SQLITE_OK;
  schema_cursor *pCur = (schema_cursor *)cur;
  schema_vtab *pVtab = (schema_vtab *)(cur->pVtab);
  char *zSql = 0;

  while( !pCur->pColumnList || SQLITE_ROW!=sqlite3_step(pCur->pColumnList) ){
    if( SQLITE_OK!=(rc = finalize(&pCur->pColumnList)) ) goto next_exit;

    while( !pCur->pTableList || SQLITE_ROW!=sqlite3_step(pCur->pTableList) ){
      if( SQLITE_OK!=(rc = finalize(&pCur->pTableList)) ) goto next_exit;

      assert(pCur->pDbList);
      while( SQLITE_ROW!=sqlite3_step(pCur->pDbList) ){
        rc = finalize(&pCur->pDbList);
        goto next_exit;
      }

      /* Set zSql to the SQL to pull the list of tables from the 
      ** sqlite_master (or sqlite_temp_master) table of the database
      ** identfied by the row pointed to by the SQL statement pCur->pDbList
      ** (iterating through a "PRAGMA database_list;" statement).
      */
      if( sqlite3_column_int(pCur->pDbList, 0)==1 ){
        zSql = sqlite3_mprintf(
            "SELECT name FROM sqlite_temp_master WHERE type=%Q",
            pVtab->pType->zObject
        );
      }else{
        sqlite3_stmt *pDbList = pCur->pDbList;
        zSql = sqlite3_mprintf(
            "SELECT name FROM %Q.sqlite_master WHERE type=%Q",
             sqlite3_column_text(pDbList, 1), pVtab->pType->zObject
        );
      }
      if( !zSql ){
        rc = SQLITE_NOMEM;
        goto next_exit;
      }

      rc = sqlite3_prepare(pVtab->db, zSql, -1, &pCur->pTableList, 0);
      sqlite3_free(zSql);
      if( rc!=SQLITE_OK ) goto next_exit;
    }

    /* Set zSql to the SQL to the table_info pragma for the table currently
    ** identified by the rows pointed to by statements pCur->pDbList and
    ** pCur->pTableList.
    */
    zSql = sqlite3_mprintf(pVtab->pType->zPragma,
        sqlite3_column_text(pCur->pDbList, 1),
        sqlite3_column_text(pCur->pTableList, 0)
    );

    if( !zSql ){
      rc = SQLITE_NOMEM;
      goto next_exit;
    }
    rc = sqlite3_prepare(pVtab->db, zSql, -1, &pCur->pColumnList, 0);
    sqlite3_free(zSql);
    if( rc!=SQLITE_OK ) goto next_exit;
  }
  pCur->rowid++;

next_exit:
  /* TODO: Handle rc */
  return rc;
}

/*
** Reset a schema table cursor.
*/
static int schemaFilter(
  sqlite3_vtab_cursor *pVtabCursor, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  int rc;
  schema_vtab *pVtab = (schema_vtab *)(pVtabCursor->pVtab);
  schema_cursor *pCur = (schema_cursor *)pVtabCursor;
  pCur->rowid = 0;
  finalize(&pCur->pTableList);
  finalize(&pCur->pColumnList);
  finalize(&pCur->pDbList);
  rc = sqlite3_prepare(pVtab->db,"SELECT 0, 'main'", -1, &pCur->pDbList, 0);
  return (rc==SQLITE_OK ? schemaNext(pVtabCursor) : rc);
}

/*
** Analyse the WHERE condition.
*/
static int schemaBestIndex(sqlite3_vtab *tab, sqlite3_index_info *pIdxInfo){
  return SQLITE_OK;
}

/*
** A virtual table module that merely echos method calls into TCL
** variables.
*/
static sqlite3_module schemaModule = {
  0,                           /* iVersion */
  schemaCreate,
  schemaCreate,
  schemaBestIndex,
  schemaDestroy,
  schemaDestroy,
  schemaOpen,                  /* xOpen - open a cursor */
  schemaClose,                 /* xClose - close a cursor */
  schemaFilter,                /* xFilter - configure scan constraints */
  schemaNext,                  /* xNext - advance a cursor */
  schemaEof,                   /* xEof */
  schemaColumn,                /* xColumn - read data */
  schemaRowid,                 /* xRowid - read data */
  0,                           /* xUpdate */
  0,                           /* xBegin */
  0,                           /* xSync */
  0,                           /* xCommit */
  0,                           /* xRollback */
  0,                           /* xFindMethod */
  0,                           /* xRename */
};

/*
** Extension load function.
*/
static int installSchemaModule(sqlite3 *db){
  sqlite3_create_module(db, "schema", &schemaModule, 0);
  return 0;
}

/**************************************************************************
***************************************************************************
** End of virtual table implementations.
** Start of SQL user function implementations.
*/

/*
**   sj(zValue, zJoin)
**
** The following block contains the implementation of an aggregate 
** function that returns a string. Each time the function is stepped, 
** it appends data to an internal buffer. When the aggregate is finalized,
** the contents of the buffer are returned.
**
** The first time the aggregate is stepped the buffer is set to a copy
** of the first argument. The second time and subsequent times it is
** stepped a copy of the second argument is appended to the buffer, then
** a copy of the first.
**
** Example:
**
**   INSERT INTO t1(a) VALUES('1');
**   INSERT INTO t1(a) VALUES('2');
**   INSERT INTO t1(a) VALUES('3');
**   SELECT sj(a, ', ') FROM t1;
**
**     =>  "1, 2, 3"
**
*/
struct StrBuffer {
  char *zBuf;
};
typedef struct StrBuffer StrBuffer;
static void joinFinalize(sqlite3_context *context){
  StrBuffer *p;
  p = (StrBuffer *)sqlite3_aggregate_context(context, sizeof(StrBuffer));
  sqlite3_result_text(context, p->zBuf, -1, SQLITE_TRANSIENT);
  sqlite3_free(p->zBuf);
}
static void joinStep(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  StrBuffer *p;
  p = (StrBuffer *)sqlite3_aggregate_context(context, sizeof(StrBuffer));
  if( p->zBuf==0 ){
    p->zBuf = sqlite3_mprintf("%s", sqlite3_value_text(argv[0]));
  }else{
    char *zTmp = p->zBuf;
    p->zBuf = sqlite3_mprintf("%s%s%s", 
        zTmp, sqlite3_value_text(argv[1]), sqlite3_value_text(argv[0])
    );
    sqlite3_free(zTmp);
  }
}

/*
**   dq(zString)
**
** This scalar function accepts a single argument and interprets it as
** a text value. The return value is the argument enclosed in double
** quotes. If any double quote characters are present in the argument, 
** these are escaped.
**
**   dq('the raven "Nevermore."') == '"the raven ""Nevermore."""'
*/
static void doublequote(
  sqlite3_context *context, 
  int argc, 
  sqlite3_value **argv
){
  int ii;
  char *zOut;
  char *zCsr;
  const char *zIn = (const char *)sqlite3_value_text(argv[0]);
  int nIn = sqlite3_value_bytes(argv[0]);

  zOut = sqlite3_malloc(nIn*2+3);
  zCsr = zOut;
  *zCsr++ = '"';
  for(ii=0; ii<nIn; ii++){
    *zCsr++ = zIn[ii];
    if( zIn[ii]=='"' ){
      *zCsr++ = '"';
    }
  }
  *zCsr++ = '"';
  *zCsr++ = '\0';

  sqlite3_result_text(context, zOut, -1, SQLITE_TRANSIENT);
  sqlite3_free(zOut);
}

/*
**   multireplace(zString, zSearch1, zReplace1, ...)
*/
static void multireplace(
  sqlite3_context *context, 
  int argc, 
  sqlite3_value **argv
){
  int i = 0;
  char *zOut = 0;
  int nOut = 0;
  int nMalloc = 0;
  const char *zIn = (const char *)sqlite3_value_text(argv[0]);
  int nIn = sqlite3_value_bytes(argv[0]);

  while( i<nIn ){
    const char *zCopy = &zIn[i];
    int nCopy = 1;
    int nReplace = 1;
    int j;
    for(j=1; j<(argc-1); j+=2){
      const char *z = (const char *)sqlite3_value_text(argv[j]);
      int n = sqlite3_value_bytes(argv[j]);
      if( n<=(nIn-i) && 0==strncmp(z, zCopy, n) ){
        zCopy = (const char *)sqlite3_value_text(argv[j+1]);
        nCopy = sqlite3_value_bytes(argv[j+1]);
        nReplace = n;
        break;
      }
    }
    if( (nOut+nCopy)>nMalloc ){
      nMalloc += (nMalloc + 16);
      zOut = (char *)sqlite3_realloc(zOut, nMalloc);
    }
    memcpy(&zOut[nOut], zCopy, nCopy);
    i += nReplace;
    nOut += nCopy;
  }

  sqlite3_result_text(context, zOut, nOut, SQLITE_TRANSIENT);
  sqlite3_free(zOut);
}

/**************************************************************************
***************************************************************************
** End of SQL user function implementations.
** Start of application implementation.
*/

typedef struct Options Options;
struct Options {
  char *zDb;
  int ignoreErrors;
  int noDrop;
};

/*
** Print out a usage message for the command line and exit. This is
** called from processCmdLine() if the program is invoked incorrectly.
*/
static int usage(char *zProgram){
  fprintf(stderr, 
      "Usage: %s ?--ignore-errors? ?--no-drop? <database file>\n", zProgram
  );
  exit(-1);
}

static void processCmdLine(int nArg, char **azArg, Options *p){
  int i;
  assert( nArg>0 );
  if( nArg<2 ){
    usage(azArg[0]);
  }
  for(i=1; i<(nArg-1); i++){
    char *z = azArg[i];
    if( 0==strcmp(z, "--ignore-errors") ){
      p->ignoreErrors = 1;
    }
    else if( 0==strcmp(z, "--no-drop") ){
      p->noDrop = 1;
    }
    else usage(azArg[0]);
  }
  p->zDb = azArg[nArg-1];
}

/*
** A callback for sqlite3_exec() that prints its first argument to
** stdout followed by a newline.
*/
static int printString(void *p, int nArg, char **azArg, char **azCol){
  printf("%s\n", azArg[0]);
  return SQLITE_OK;
}

int detectSchemaProblem(
  sqlite3 *db,                   /* Database connection */
  const char *zMessage,          /* English language error message */
  const char *zSql,              /* SQL statement to run */
  int *pHasErrors                /* Set *pHasErrors==1 if errors found */
){
  sqlite3_stmt *pStmt;
  int rc;
  rc = sqlite3_prepare(db, zSql, -1, &pStmt, 0);
  if( rc!=SQLITE_OK ){
    return rc;
  }
  while( SQLITE_ROW==sqlite3_step(pStmt) ){
    char *zDel;
    int iFk = sqlite3_column_int(pStmt, 0);
    const char *zTab = (const char *)sqlite3_column_text(pStmt, 1);
    fprintf(stderr, "Error in table %s: %s\n", zTab, zMessage);
    zDel = sqlite3_mprintf(
        "DELETE FROM temp.fkey WHERE from_tbl = %Q AND fkid = %d"
        , zTab, iFk
    );
    sqlite3_exec(db, zDel, 0, 0, 0);
    sqlite3_free(zDel);
    *pHasErrors = 1;
  }
  sqlite3_finalize(pStmt);
  return SQLITE_OK;
}

/*
** Create and populate temporary table "fkey".
*/
static int populateTempTable(sqlite3 *db, char **pzErr, int *pHasErrors){
  int rc;

  rc = sqlite3_exec(db, 
      "CREATE VIRTUAL TABLE temp.v_fkey USING schema(foreign_key_list);"
      "CREATE VIRTUAL TABLE temp.v_col USING schema(table_info);"
      "CREATE VIRTUAL TABLE temp.v_idxlist USING schema(index_list);"
      "CREATE VIRTUAL TABLE temp.v_idxinfo USING schema(index_info);"

      "CREATE TABLE temp.fkey AS "
        "SELECT from_tbl, to_tbl, fkid, from_col, to_col, on_update, on_delete "
        "FROM temp.v_fkey WHERE database = 'main';"

      , 0, 0, pzErr
  );
  if( rc!=SQLITE_OK ) return rc;

  rc = detectSchemaProblem(db, "foreign key columns do not exist",
    "SELECT fkid, from_tbl "
    "FROM temp.fkey "
    "WHERE to_col IS NOT NULL AND NOT EXISTS (SELECT 1 "
        "FROM temp.v_col WHERE tablename=to_tbl AND name==to_col"
    ")", pHasErrors
  );
  if( rc!=SQLITE_OK ) return rc;

  /* At this point the temp.fkey table is mostly populated. If any foreign
  ** keys were specified so that they implicitly refer to they primary
  ** key of the parent table, the "to_col" values of the temp.fkey rows
  ** are still set to NULL.
  **
  ** This is easily fixed for single column primary keys, but not for
  ** composites. With a composite primary key, there is no way to reliably
  ** query sqlite for the order in which the columns that make up the
  ** composite key were declared i.e. there is no way to tell if the
  ** schema actually contains "PRIMARY KEY(a, b)" or "PRIMARY KEY(b, a)".
  ** Therefore, this case is not handled. The following function call
  ** detects instances of this case.
  */
  rc = detectSchemaProblem(db, "implicit mapping to composite primary key",
    "SELECT fkid, from_tbl "
    "FROM temp.fkey "
    "WHERE to_col IS NULL "
    "GROUP BY fkid, from_tbl HAVING count(*) > 1", pHasErrors
  );
  if( rc!=SQLITE_OK ) return rc;

  /* Detect attempts to implicitly map to the primary key of a table 
  ** that has no primary key column.
  */
  rc = detectSchemaProblem(db, "implicit mapping to non-existant primary key",
    "SELECT fkid, from_tbl "
    "FROM temp.fkey "
    "WHERE to_col IS NULL AND NOT EXISTS "
      "(SELECT 1 FROM temp.v_col WHERE pk AND tablename = temp.fkey.to_tbl)"
    , pHasErrors
  );
  if( rc!=SQLITE_OK ) return rc;

  /* Fix all the implicit primary key mappings in the temp.fkey table. */
  rc = sqlite3_exec(db, 
    "UPDATE temp.fkey SET to_col = "
      "(SELECT name FROM temp.v_col WHERE pk AND tablename=temp.fkey.to_tbl)"
    " WHERE to_col IS NULL;"
    , 0, 0, pzErr
  );
  if( rc!=SQLITE_OK ) return rc;

  /* Now check that all all parent keys are either primary keys or 
  ** subject to a unique constraint.
  */
  rc = sqlite3_exec(db, 
    "CREATE TABLE temp.idx2 AS SELECT "
      "il.tablename AS tablename,"
      "ii.indexname AS indexname,"
      "ii.name AS col "
      "FROM temp.v_idxlist AS il, temp.v_idxinfo AS ii "
      "WHERE il.isunique AND il.database='main' AND ii.indexname = il.name;"
    "INSERT INTO temp.idx2 "
      "SELECT tablename, 'pk', name FROM temp.v_col WHERE pk;"

    "CREATE TABLE temp.idx AS SELECT "
      "tablename, indexname, sj(dq(col),',') AS cols "
      "FROM (SELECT * FROM temp.idx2 ORDER BY col) " 
      "GROUP BY tablename, indexname;"

    "CREATE TABLE temp.fkey2 AS SELECT "
        "fkid, from_tbl, to_tbl, sj(dq(to_col),',') AS cols "
        "FROM (SELECT * FROM temp.fkey ORDER BY to_col) " 
        "GROUP BY fkid, from_tbl;"
    , 0, 0, pzErr
  );
  if( rc!=SQLITE_OK ) return rc;
  rc = detectSchemaProblem(db, "foreign key is not unique",
    "SELECT fkid, from_tbl "
    "FROM temp.fkey2 "
    "WHERE NOT EXISTS (SELECT 1 "
        "FROM temp.idx WHERE tablename=to_tbl AND fkey2.cols==idx.cols"
    ")", pHasErrors
  );
  if( rc!=SQLITE_OK ) return rc;

  return rc;
}

int main(int argc, char **argv){
  sqlite3 *db;
  Options opt = {0, 0, 0};
  int rc;
  int hasErrors = 0;
  char *zErr = 0;
  const int enc = SQLITE_UTF8;

  const char *zSql =
    "SELECT multireplace('"

      "-- Triggers for foreign key mapping:\n"
      "--\n"
      "--     /from_readable/ REFERENCES /to_readable/\n"
      "--     on delete /on_delete/\n"
      "--     on update /on_update/\n"
      "--\n"

      /* The "BEFORE INSERT ON <referencing>" trigger. This trigger's job is to
      ** throw an exception if the user tries to insert a row into the
      ** referencing table for which there is no corresponding row in
      ** the referenced table.
      */
      "CREATE TRIGGER /name/_insert_referencing BEFORE INSERT ON /tbl/ WHEN \n"
      "    /key_notnull/ AND NOT EXISTS (SELECT 1 FROM /ref/ WHERE /cond1/)\n" 
      "BEGIN\n"
        "  SELECT RAISE(ABORT, ''constraint failed'');\n"
      "END;\n"

      /* The "BEFORE UPDATE ON <referencing>" trigger. This trigger's job 
      ** is to throw an exception if the user tries to update a row in the
      ** referencing table causing it to correspond to no row in the
      ** referenced table.
      */
      "CREATE TRIGGER /name/_update_referencing BEFORE\n"
      "    UPDATE OF /rkey_list/ ON /tbl/ WHEN \n"
      "    /key_notnull/ AND \n"
      "    NOT EXISTS (SELECT 1 FROM /ref/ WHERE /cond1/)\n" 
      "BEGIN\n"
        "  SELECT RAISE(ABORT, ''constraint failed'');\n"
      "END;\n"


      /* The "BEFORE DELETE ON <referenced>" trigger. This trigger's job 
      ** is to detect when a row is deleted from the referenced table to 
      ** which rows in the referencing table correspond. The action taken
      ** depends on the value of the 'ON DELETE' clause.
      */
      "CREATE TRIGGER /name/_delete_referenced BEFORE DELETE ON /ref/ WHEN\n"
      "    EXISTS (SELECT 1 FROM /tbl/ WHERE /cond2/)\n"
      "BEGIN\n"
      "  /delete_action/\n"
      "END;\n"

      /* The "BEFORE DELETE ON <referenced>" trigger. This trigger's job 
      ** is to detect when the key columns of a row in the referenced table 
      ** to which one or more rows in the referencing table correspond are
      ** updated. The action taken depends on the value of the 'ON UPDATE' 
      ** clause.
      */
      "CREATE TRIGGER /name/_update_referenced AFTER\n"
      "    UPDATE OF /fkey_list/ ON /ref/ WHEN \n"
      "    EXISTS (SELECT 1 FROM /tbl/ WHERE /cond2/)\n"
      "BEGIN\n"
      "  /update_action/\n"
      "END;\n"
    "'"

    /* These are used in the SQL comment written above each set of triggers */
    ", '/from_readable/',  from_tbl || '(' || sj(from_col, ', ') || ')'"
    ", '/to_readable/',    to_tbl || '(' || sj(to_col, ', ') || ')'"
    ", '/on_delete/', on_delete"
    ", '/on_update/', on_update"

    ", '/name/',   'genfkey' || min(rowid)"
    ", '/tbl/',    dq(from_tbl)"
    ", '/ref/',    dq(to_tbl)"
    ", '/key_notnull/', sj('new.' || dq(from_col) || ' IS NOT NULL', ' AND ')"

    ", '/fkey_list/', sj(to_col, ', ')"
    ", '/rkey_list/', sj(from_col, ', ')"

    ", '/cond1/',  sj(multireplace('new./from/ == /to/'"
                   ", '/from/', dq(from_col)"
                   ", '/to/',   dq(to_col)"
                   "), ' AND ')"
    ", '/cond2/',  sj(multireplace('old./to/ == /from/'"
                   ", '/from/', dq(from_col)"
                   ", '/to/',   dq(to_col)"
                   "), ' AND ')"

    ", '/update_action/', CASE on_update "
      "WHEN 'SET NULL' THEN "
        "multireplace('UPDATE /tbl/ SET /setlist/ WHERE /where/;' "
        ", '/setlist/', sj(from_col||' = NULL',', ')"
        ", '/tbl/',     dq(from_tbl)"
        ", '/where/',   sj(from_col||' = old.'||dq(to_col),' AND ')"
        ")"
      "WHEN 'CASCADE' THEN "
        "multireplace('UPDATE /tbl/ SET /setlist/ WHERE /where/;' "
        ", '/setlist/', sj(dq(from_col)||' = new.'||dq(to_col),', ')"
        ", '/tbl/',     dq(from_tbl)"
        ", '/where/',   sj(dq(from_col)||' = old.'||dq(to_col),' AND ')"
        ")"
      "ELSE "
      "  'SELECT RAISE(ABORT, ''constraint failed'');'"
      "END "

    ", '/delete_action/', CASE on_delete "
      "WHEN 'SET NULL' THEN "
        "multireplace('UPDATE /tbl/ SET /setlist/ WHERE /where/;' "
        ", '/setlist/', sj(from_col||' = NULL',', ')"
        ", '/tbl/',     dq(from_tbl)"
        ", '/where/',   sj(from_col||' = old.'||dq(to_col),' AND ')"
        ")"
      "WHEN 'CASCADE' THEN "
        "multireplace('DELETE FROM /tbl/ WHERE /where/;' "
        ", '/tbl/',     dq(from_tbl)"
        ", '/where/',   sj(dq(from_col)||' = old.'||dq(to_col),' AND ')"
        ")"
      "ELSE "
      "  'SELECT RAISE(ABORT, ''constraint failed'');'"
      "END "

    ") FROM temp.fkey "
    "GROUP BY from_tbl, fkid"
  ;

  processCmdLine(argc, argv, &opt);

  /* Open the database handle. */
  rc = sqlite3_open_v2(opt.zDb, &db, SQLITE_OPEN_READONLY, 0);
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "Error opening database file: %s\n", sqlite3_errmsg(db));
    return -1;
  }

  /* Create the special scalar and aggregate functions used by this program. */
  sqlite3_create_function(db, "dq", 1, enc, 0, doublequote, 0, 0);
  sqlite3_create_function(db, "multireplace", -1, enc, db, multireplace, 0, 0);
  sqlite3_create_function(db, "sj", 2, enc, 0, 0, joinStep, joinFinalize);

  /* Install the "schema" virtual table module */
  installSchemaModule(db);

  /* Create and populate a temp table with the information required to
  ** build the foreign key triggers. See function populateTempTable()
  ** for details.
  */
  rc = populateTempTable(db, &zErr, &hasErrors);
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "Error reading database: %s\n", zErr);
    return -1;
  }
  if( hasErrors && opt.ignoreErrors==0 ){
    return -1;
  }

  printf("BEGIN;\n");

  /* Unless the --no-drop option was specified, generate DROP TRIGGER
  ** statements to drop any triggers in the database generated by a
  ** previous run of this program.
  */
  if( opt.noDrop==0 ){
    rc = sqlite3_exec(db, 
      "SELECT 'DROP TRIGGER' || ' ' || dq(name) || ';'"
      "FROM sqlite_master "
      "WHERE type='trigger' AND substr(name, 0, 7) == 'genfkey'"
      , printString, 0, 0
    );
    if( rc!=SQLITE_OK ){
      const char *zMsg = sqlite3_errmsg(db);
      fprintf(stderr, "Generating drop triggers failed: %s\n", zMsg);
      return -1;
    }
  }

  /* Run the main query to create the trigger definitions. */
  rc = sqlite3_exec(db, zSql, printString, 0, 0);
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "Generating triggers failed: %s\n", sqlite3_errmsg(db));
    return -1;
  }

  printf("COMMIT;\n");
  return 0;
}

