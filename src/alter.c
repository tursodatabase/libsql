/*
** 2005 February 15
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains C code routines that used to generate VDBE code
** that implements the ALTER TABLE command.
**
** $Id: alter.c,v 1.1 2005/02/15 20:47:57 drh Exp $
*/
#include "sqliteInt.h"


#ifndef SQLITE_OMIT_ALTERTABLE
/*
** Generate code to implement the "ALTER TABLE xxx RENAME TO yyy" 
** command. 
*/
void sqlite3AlterRenameTable(
  Parse *pParse,            /* Parser context. */
  SrcList *pSrc,            /* The table to rename. */
  Token *pName              /* The new table name. */
){
  int iDb;                  /* Database that contains the table */
  char *zDb;                /* Name of database iDb */
  Table *pTab;              /* Table being renamed */
  char *zName = 0;          /* NULL-terminated version of pName */ 
  char *zWhere = 0;         /* Where clause of schema elements to reparse */
  sqlite3 *db = pParse->db; /* Database connection */
  Vdbe *v;
#ifndef SQLITE_OMIT_TRIGGER
  char *zTempTrig = 0;      /* Where clause to locate temp triggers */
#endif
  
  assert( pSrc->nSrc==1 );

  pTab = sqlite3LocateTable(pParse, pSrc->a[0].zName, pSrc->a[0].zDatabase);
  if( !pTab ) goto exit_rename_table;
  iDb = pTab->iDb;
  zDb = db->aDb[iDb].zName;

  /* Get a NULL terminated version of the new table name. */
  zName = sqlite3NameFromToken(pName);
  if( !zName ) goto exit_rename_table;

  /* Check that a table or index named 'zName' does not already exist
  ** in database iDb. If so, this is an error.
  */
  if( sqlite3FindTable(db, zName, zDb) || sqlite3FindIndex(db, zName, zDb) ){
    sqlite3ErrorMsg(pParse, 
        "there is already another table or index with this name: %s", zName);
    goto exit_rename_table;
  }

  /* Make sure it is not a system table being altered, or a reserved name
  ** that the table is being renamed to.
  */
  if( strlen(pTab->zName)>6 && 0==sqlite3StrNICmp(pTab->zName, "sqlite_", 7) ){
    sqlite3ErrorMsg(pParse, "table %s may not be altered", pTab->zName);
    goto exit_rename_table;
  }
  if( SQLITE_OK!=sqlite3CheckObjectName(pParse, zName) ){
    goto exit_rename_table;
  }

#ifndef SQLITE_OMIT_AUTHORIZATION
  /* Invoke the authorization callback. */
  if( sqlite3AuthCheck(pParse, SQLITE_ALTER_TABLE, zDb, pTab->zName, 0) ){
    goto exit_rename_table;
  }
#endif

  /* Begin a transaction and code the VerifyCookie for database iDb. 
  ** Then modify the schema cookie (since the ALTER TABLE modifies the
  ** schema).
  */
  v = sqlite3GetVdbe(pParse);
  if( v==0 ){
    goto exit_rename_table;
  }
  sqlite3BeginWriteOperation(pParse, 0, iDb);
  sqlite3ChangeCookie(db, v, iDb);

  /* Modify the sqlite_master table to use the new table name. */
  sqlite3NestedParse(pParse,
      "UPDATE %Q.%s SET "
#ifdef SQLITE_OMIT_TRIGGER
          "sql = sqlite_rename_table(sql, %Q), "
#else
          "sql = CASE "
            "WHEN type = 'trigger' THEN sqlite_rename_trigger(sql, %Q)"
            "ELSE sqlite_rename_table(sql, %Q) END, "
#endif
          "tbl_name = %Q, "
          "name = CASE "
            "WHEN type='table' THEN %Q "
            "WHEN name LIKE 'sqlite_autoindex%%' AND type='index' THEN "
              "'sqlite_autoindex_' || %Q || substr(name, %d+18,10) "
            "ELSE name END "
      "WHERE tbl_name=%Q AND "
          "(type='table' OR type='index' OR type='trigger');", 
      zDb, SCHEMA_TABLE(iDb), zName, zName, zName, 
#ifndef SQLITE_OMIT_TRIGGER
zName,
#endif
      zName, strlen(pTab->zName), pTab->zName
  );

#ifndef SQLITE_OMIT_AUTOINCREMENT
  /* If the sqlite_sequence table exists in this database, then update 
  ** it with the new table name.
  */
  if( sqlite3FindTable(db, "sqlite_sequence", zDb) ){
    sqlite3NestedParse(pParse,
        "UPDATE %Q.sqlite_sequence set name = %Q WHERE name = %Q",
        zDb, zName, pTab->zName);
  }
#endif

#ifndef SQLITE_OMIT_TRIGGER
  /* If there are TEMP triggers on this table, modify the sqlite_temp_master
  ** table. Don't do this if the table being ALTERed is itself located in
  ** the temp database.
  */
  if( iDb!=1 ){
    Trigger *pTrig;
    char *tmp = 0;
    for( pTrig=pTab->pTrigger; pTrig; pTrig=pTrig->pNext ){
      if( pTrig->iDb==1 ){
        if( !zTempTrig ){
          zTempTrig = 
              sqlite3MPrintf("type = 'trigger' AND (name=%Q", pTrig->name);
        }else{
          tmp = zTempTrig;
          zTempTrig = sqlite3MPrintf("%s OR name=%Q", zTempTrig, pTrig->name);
          sqliteFree(tmp);
        }
      }
    }
    if( zTempTrig ){
      tmp = zTempTrig;
      zTempTrig = sqlite3MPrintf("%s)", zTempTrig);
      sqliteFree(tmp);
      sqlite3NestedParse(pParse, 
          "UPDATE sqlite_temp_master SET "
              "sql = sqlite_rename_trigger(sql, %Q), "
              "tbl_name = %Q "
              "WHERE %s;", zName, zName, zTempTrig);
    }
  }
#endif

  /* Drop the elements of the in-memory schema that refered to the table
  ** renamed and load the new versions from the database.
  */
  if( pParse->nErr==0 ){
#ifndef SQLITE_OMIT_TRIGGER
    Trigger *pTrig;
    for( pTrig=pTab->pTrigger; pTrig; pTrig=pTrig->pNext ){
      assert( pTrig->iDb==iDb || pTrig->iDb==1 );
      sqlite3VdbeOp3(v, OP_DropTrigger, pTrig->iDb, 0, pTrig->name, 0);
    }
#endif
    sqlite3VdbeOp3(v, OP_DropTable, iDb, 0, pTab->zName, 0);
    zWhere = sqlite3MPrintf("tbl_name=%Q", zName);
    sqlite3VdbeOp3(v, OP_ParseSchema, iDb, 0, zWhere, P3_DYNAMIC);
#ifndef SQLITE_OMIT_TRIGGER
    if( zTempTrig ){
      sqlite3VdbeOp3(v, OP_ParseSchema, 1, 0, zTempTrig, P3_DYNAMIC);
    }
  }else{
    sqliteFree(zTempTrig);
#endif
  }

exit_rename_table:
  sqlite3SrcListDelete(pSrc);
  sqliteFree(zName);
}
#endif  /* SQLITE_ALTER_TABLE */
