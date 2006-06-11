/*
** 2006 June 10
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains code used to help implement virtual tables.
**
** $Id: vtab.c,v 1.1 2006/06/11 23:41:56 drh Exp $
*/
#ifndef SQLITE_OMIT_VIRTUALTABLE
#include "sqliteInt.h"

/*
** External API function used to create a new virtual-table module.
*/
int sqlite3_create_module(
  sqlite3 *db,                    /* Database in which module is registered */
  const char *zName,              /* Name assigned to this module */
  const sqlite3_module *pModule   /* The definition of the module */
){
  sqlite3HashInsert(&db->aModule, zName, strlen(zName), (void*)pModule);
  sqlite3ResetInternalSchema(db, 0);
  return SQLITE_OK;
}


/*
** Clear any and all virtual-table information from the Table record.
** This routine is called, for example, just before deleting the Table
** record.
*/
void sqlite3VtabClear(Table *p){
  if( p->pVtab ){
    assert( p->pModule!=0 );
    p->pModule->xDisconnect(p->pVtab);
  }
  if( p->azModuleArg ){
    int i;
    for(i=0; i<p->nModuleArg; i++){
      sqliteFree(p->azModuleArg[i]);
    }
    sqliteFree(p->azModuleArg);
  }
}

/*
** Add a new module argument to pTable->azModuleArg[].
** The string is not copied - the pointer is stored.  The
** string will be freed automatically when the table is
** deleted.
*/
static void addModuleArgument(Table *pTable, char *zArg){
  int i = pTable->nModuleArg++;
  pTable->azModuleArg = sqliteRealloc(pTable->azModuleArg,
                             sizeof(char*)*(pTable->nModuleArg+1));
  if( pTable->azModuleArg==0 ){
    pTable->nModuleArg = 0;
    sqliteFree(zArg);
  }else{
    pTable->azModuleArg[i] = zArg;
    pTable->azModuleArg[i+1] = 0;
  }
}

/*
** The parser calls this routine when it first sees a CREATE VIRTUAL TABLE
** statement.  The module name has been parsed, but the optional list
** of parameters that follow the module name are still pending.
*/
void sqlite3VtabBeginParse(
  Parse *pParse,        /* Parsing context */
  Token *pName1,        /* Name of new table, or database name */
  Token *pName2,        /* Name of new table or NULL */
  Token *pModuleName    /* Name of the module for the virtual table */
){
  Table *pTable;        /* The new virtual table */

  sqlite3StartTable(pParse, pName1, pName2, 0, 0, 0);
  pTable = pParse->pNewTable;
  if( pTable==0 ) return;
  pTable->isVirtual = 1;
  pTable->nModuleArg = 0;
  addModuleArgument(pTable, sqlite3NameFromToken(pModuleName));
  pParse->sNameToken.n = pModuleName->z + pModuleName->n - pName1->z;
}

/*
** This routine takes the module argument that has been accumulating
** in pParse->zArg[] and appends it to the list of arguments on the
** virtual table currently under construction in pParse->pTable.
*/
static void addArgumentToVtab(Parse *pParse){
  if( pParse->nArgUsed && pParse->pNewTable ){
    addModuleArgument(pParse->pNewTable, sqliteStrDup(pParse->zArg));
  }
  pParse->nArgUsed = 0;
}

/*
** The parser calls this routine after the CREATE VIRTUAL TABLE statement
** has been completely parsed.
*/
void sqlite3VtabFinishParse(Parse *pParse, Token *pEnd){
  Table *pTab;        /* The table being constructed */
  sqlite3 *db;        /* The database connection */
  char *zModule;      /* The module name of the table: USING modulename */

  addArgumentToVtab(pParse);
  sqliteFree(pParse->zArg);
  pParse->zArg = 0;
  pParse->nArgAlloc = 0;

  /* Lookup the module name. */
  pTab = pParse->pNewTable;
  if( pTab==0 ) return;
  db = pParse->db;
  if( pTab->nModuleArg<1 ) return;
  pParse->pNewTable = 0;
  zModule = pTab->azModuleArg[0];
  pTab->pModule = (sqlite3_module*)sqlite3HashFind(&db->aModule, 
                     zModule, strlen(zModule));
  
  /* If the CREATE VIRTUAL TABLE statement is being entered for the
  ** first time (in other words if the virtual table is actually being
  ** created now instead of just being read out of sqlite_master) then
  ** do additional initialization work and store the statement text
  ** in the sqlite_master table.
  */
  if( !db->init.busy ){
    char *zStmt;
    int iDb;
    Vdbe *v;
    if( pTab->pModule==0 ){
      sqlite3ErrorMsg(pParse, "unknown module: %s", zModule);
    }

    /* Compute the complete text of the CREATE VIRTUAL TABLE statement */
    if( pEnd ){
      pParse->sNameToken.n = pEnd->z - pParse->sNameToken.z + pEnd->n;
    }
    zStmt = sqlite3MPrintf("CREATE VIRTUAL TABLE %T", &pParse->sNameToken);

    /* A slot for the record has already been allocated in the 
    ** SQLITE_MASTER table.  We just need to update that slot with all
    ** the information we've collected.  The rowid for the preallocated
    ** slot is the top the stack.
    */
    iDb = sqlite3SchemaToIndex(db, pTab->pSchema);
    sqlite3NestedParse(pParse,
      "UPDATE %Q.%s "
         "SET type='table', name=%Q, tbl_name=%Q, rootpage=NULL, sql=%Q "
       "WHERE rowid=#0",
      db->aDb[iDb].zName, SCHEMA_TABLE(iDb),
      pTab->zName,
      pTab->zName,
      zStmt
    );
    sqliteFree(zStmt);
    v = sqlite3GetVdbe(pParse);
    sqlite3VdbeOp3(v, OP_VCreate, 0, 0, pTab->zName, P3_DYNAMIC);
    sqlite3ChangeCookie(db, v, iDb);
  }

  /* If we are rereading the sqlite_master table and we happen to
  ** currently know the module for the new table, create an
  ** sqlite3_vtab instance.
  */
  else if( pTab->pModule ){
    sqlite3_module *pMod = pTab->pModule;
    assert( pMod->xConnect );
    pMod->xConnect(db, pMod, pTab->nModuleArg, pTab->azModuleArg, &pTab->pVtab);
  }
}

/*
** The parser calls this routine when it sees the first token
** of an argument to the module name in a CREATE VIRTUAL TABLE statement.
*/
void sqlite3VtabArgInit(Parse *pParse){
  addArgumentToVtab(pParse);
  pParse->nArgUsed = 0;
}

/*
** The parser calls this routine for each token after the first token
** in an argument to the module name in a CREATE VIRTUAL TABLE statement.
*/
void sqlite3VtabArgExtend(Parse *pParse, Token *p){
  if( pParse->nArgUsed + p->n + 2 >= pParse->nArgAlloc ){
    pParse->nArgAlloc = pParse->nArgAlloc*2 + p->n + 200;
    pParse->zArg = sqliteRealloc(pParse->zArg, pParse->nArgAlloc);
    if( pParse->zArg==0 ){
      pParse->nArgAlloc = 0;
      return;
    }
  }
  if( pParse->nArgUsed ){
    pParse->zArg[pParse->nArgUsed++] = ' ';
  }
  memcpy(&pParse->zArg[pParse->nArgUsed], p->z, p->n);
  pParse->nArgUsed += p->n;
  pParse->zArg[pParse->nArgUsed] = 0;
}

#endif /* SQLITE_OMIT_VIRTUALTABLE */
