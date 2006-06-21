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
** $Id: vtab.c,v 1.20 2006/06/21 16:02:43 danielk1977 Exp $
*/
#ifndef SQLITE_OMIT_VIRTUALTABLE
#include "sqliteInt.h"

/*
** External API function used to create a new virtual-table module.
*/
int sqlite3_create_module(
  sqlite3 *db,                    /* Database in which module is registered */
  const char *zName,              /* Name assigned to this module */
  const sqlite3_module *pModule,  /* The definition of the module */
  void *pAux                      /* Context pointer for xCreate/xConnect */
){
  int nName = strlen(zName);
  Module *pMod = (Module *)sqliteMallocRaw(sizeof(Module) + nName + 1);
  if( pMod ){
    char *zCopy = (char *)(&pMod[1]);
    strcpy(zCopy, zName);
    pMod->zName = zCopy;
    pMod->pModule = pModule;
    pMod->pAux = pAux;
    pMod = (Module *)sqlite3HashInsert(&db->aModule, zCopy, nName, (void*)pMod);
    sqliteFree(pMod);
    sqlite3ResetInternalSchema(db, 0);
  }
  return sqlite3ApiExit(db, SQLITE_OK);
}

/*
** Clear any and all virtual-table information from the Table record.
** This routine is called, for example, just before deleting the Table
** record.
*/
void sqlite3VtabClear(Table *p){
  if( p->pVtab ){
    assert( p->pMod && p->pMod->pModule );
    p->pMod->pModule->xDisconnect(p->pVtab);
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
  int iDb;              /* The database the table is being created in */
  Table *pTable;        /* The new virtual table */
  Token *pDummy;        /* Dummy arg for sqlite3TwoPartName() */

  sqlite3StartTable(pParse, pName1, pName2, 0, 0, 1, 0);
  pTable = pParse->pNewTable;
  if( pTable==0 || pParse->nErr ) return;
  assert( 0==pTable->pIndex );

  iDb = sqlite3SchemaToIndex(pParse->db, pTable->pSchema);
  assert( iDb>=0 );

  pTable->isVirtual = 1;
  pTable->nModuleArg = 0;
  addModuleArgument(pTable, sqlite3NameFromToken(pModuleName));
  addModuleArgument(pTable, sqlite3StrDup(pParse->db->aDb[iDb].zName));
  addModuleArgument(pTable, sqlite3StrDup(pTable->zName));
  pParse->sNameToken.n = pModuleName->z + pModuleName->n - pName1->z;

#ifndef SQLITE_OMIT_AUTHORIZATION
  /* Creating a virtual table invokes the authorization callback twice.
  ** The first invocation, to obtain permission to INSERT a row into the
  ** sqlite_master table, has already been made by sqlite3StartTable().
  ** The second call, to obtain permission to create the table, is made now.
  */
  if( sqlite3AuthCheck(pParse, SQLITE_CREATE_VTABLE, pTable->zName, 
          pTable->azModuleArg[0], pParse->db->aDb[iDb].zName) 
  ){
    return;
  }
#endif
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
  Module *pMod = 0;

  addArgumentToVtab(pParse);
  sqliteFree(pParse->zArg);
  pParse->zArg = 0;
  pParse->nArgAlloc = 0;

  /* Lookup the module name. */
  pTab = pParse->pNewTable;
  if( pTab==0 ) return;
  db = pParse->db;
  if( pTab->nModuleArg<1 ) return;
  zModule = pTab->azModuleArg[0];
  pMod = (Module *)sqlite3HashFind(&db->aModule, zModule, strlen(zModule));
  pTab->pMod = pMod;
  
  /* If the CREATE VIRTUAL TABLE statement is being entered for the
  ** first time (in other words if the virtual table is actually being
  ** created now instead of just being read out of sqlite_master) then
  ** do additional initialization work and store the statement text
  ** in the sqlite_master table.
  */
  if( !db->init.busy ){
    char *zStmt;
    char *zWhere;
    int iDb;
    Vdbe *v;
    if( !pMod ){
      sqlite3ErrorMsg(pParse, "no such module: %s", zModule);
    }

    /* Compute the complete text of the CREATE VIRTUAL TABLE statement */
    if( pEnd ){
      pParse->sNameToken.n = pEnd->z - pParse->sNameToken.z + pEnd->n;
    }
    zStmt = sqlite3MPrintf("CREATE VIRTUAL TABLE %T", &pParse->sNameToken);

    /* A slot for the record has already been allocated in the 
    ** SQLITE_MASTER table.  We just need to update that slot with all
    ** the information we've collected.  
    **
    ** The top of the stack is the rootpage allocated by sqlite3StartTable().
    ** This value is always 0 and is ignored, a virtual table does not have a
    ** rootpage. The next entry on the stack is the rowid of the record
    ** in the sqlite_master table.
    */
    iDb = sqlite3SchemaToIndex(db, pTab->pSchema);
    sqlite3NestedParse(pParse,
      "UPDATE %Q.%s "
         "SET type='table', name=%Q, tbl_name=%Q, rootpage=0, sql=%Q "
       "WHERE rowid=#1",
      db->aDb[iDb].zName, SCHEMA_TABLE(iDb),
      pTab->zName,
      pTab->zName,
      zStmt
    );
    sqliteFree(zStmt);
    v = sqlite3GetVdbe(pParse);
    sqlite3ChangeCookie(db, v, iDb);

    sqlite3VdbeAddOp(v, OP_Expire, 0, 0);
    zWhere = sqlite3MPrintf("name='%q'", pTab->zName);
    sqlite3VdbeOp3(v, OP_ParseSchema, iDb, 0, zWhere, P3_DYNAMIC);
    sqlite3VdbeOp3(v, OP_VCreate, iDb, 0, pTab->zName, strlen(pTab->zName) + 1);
  }

  /* If we are rereading the sqlite_master table create the in-memory
  ** record of the table. If the module has already been registered,
  ** also call the xConnect method here.
  */
  else {
    Table *pOld;
    Schema *pSchema = pTab->pSchema;
    const char *zName = pTab->zName;
    int nName = strlen(zName) + 1;
    pOld = sqlite3HashInsert(&pSchema->tblHash, zName, nName, pTab);
    if( pOld ){
      assert( pTab==pOld );  /* Malloc must have failed inside HashInsert() */
      return;
    }
    pParse->pNewTable = 0;
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

/*
** Invoke a virtual table constructor (either xCreate or xConnect). The
** pointer to the function to invoke is passed as the fourth parameter
** to this procedure.
*/
static int vtabCallConstructor(
  sqlite3 *db, 
  Table *pTab,
  Module *pMod,
  int (*xConstruct)(sqlite3*, void *, int, char **, sqlite3_vtab **),
  char **pzErr
){
  int rc;
  int rc2;
  char **azArg = pTab->azModuleArg;
  int nArg = pTab->nModuleArg;

  assert( !db->pVTab );
  assert( xConstruct );

  db->pVTab = pTab;
  rc = sqlite3SafetyOff(db);
  assert( rc==SQLITE_OK );
  rc = xConstruct(db, pMod->pAux, nArg, azArg, &pTab->pVtab);
  rc2 = sqlite3SafetyOn(db);
  if( pTab->pVtab ){
    pTab->pVtab->pModule = pMod->pModule;
  }

  if( SQLITE_OK!=rc ){
    *pzErr = sqlite3MPrintf("vtable constructor failed: %s", pTab->zName);
  } else if( db->pVTab ){
    const char *zFormat = "vtable constructor did not declare schema: %s";
    *pzErr = sqlite3MPrintf(zFormat, pTab->zName);
    rc = SQLITE_ERROR;
  } 
  if( rc==SQLITE_OK ){
    rc = rc2;
  }
  db->pVTab = 0;
  return rc;
}

/*
** This function is invoked by the parser to call the xConnect() method
** of the virtual table pTab. If an error occurs, an error code is returned 
** and an error left in pParse.
**
** This call is a no-op if table pTab is not a virtual table.
*/
int sqlite3VtabCallConnect(Parse *pParse, Table *pTab){
  Module *pMod;
  const char *zModule;
  int rc = SQLITE_OK;

  if( !pTab || !pTab->isVirtual || pTab->pVtab ){
    return SQLITE_OK;
  }

  pMod = pTab->pMod;
  zModule = pTab->azModuleArg[0];
  if( !pMod ){
    const char *zModule = pTab->azModuleArg[0];
    sqlite3ErrorMsg(pParse, "no such module: %s", zModule);
    rc = SQLITE_ERROR;
  } else {
    char *zErr = 0;
    sqlite3 *db = pParse->db;
    rc = vtabCallConstructor(db, pTab, pMod, pMod->pModule->xConnect, &zErr);
    if( rc!=SQLITE_OK ){
      sqlite3ErrorMsg(pParse, "%s", zErr);
    }
    sqliteFree(zErr);
  }

  return rc;
}

/*
** Add the virtual table pVtab to the array sqlite3.aVTrans[].
*/
int addToVTrans(sqlite3 *db, sqlite3_vtab *pVtab){
  const int ARRAY_INCR = 5;

  /* Grow the sqlite3.aVTrans array if required */
  if( (db->nVTrans%ARRAY_INCR)==0 ){
    sqlite3_vtab **aVTrans;
    int nBytes = sizeof(sqlite3_vtab *) * (db->nVTrans + ARRAY_INCR);
    aVTrans = sqliteRealloc((void *)db->aVTrans, nBytes);
    if( !aVTrans ){
      return SQLITE_NOMEM;
    }
    memset(&aVTrans[db->nVTrans], 0, sizeof(sqlite3_vtab *)*ARRAY_INCR);
    db->aVTrans = aVTrans;
  }

  /* Add pVtab to the end of sqlite3.aVTrans */
  db->aVTrans[db->nVTrans++] = pVtab;
  return SQLITE_OK;
}

/*
** This function is invoked by the vdbe to call the xCreate method
** of the virtual table named zTab in database iDb. 
**
** If an error occurs, *pzErr is set to point an an English language
** description of the error and an SQLITE_XXX error code is returned.
** In this case the caller must call sqliteFree() on *pzErr.
*/
int sqlite3VtabCallCreate(sqlite3 *db, int iDb, const char *zTab, char **pzErr){
  int rc = SQLITE_OK;
  Table *pTab;
  Module *pMod;
  const char *zModule;

  pTab = sqlite3FindTable(db, zTab, db->aDb[iDb].zName);
  assert(pTab && pTab->isVirtual && !pTab->pVtab);
  pMod = pTab->pMod;
  zModule = pTab->azModuleArg[0];

  /* If the module has been registered and includes a Create method, 
  ** invoke it now. If the module has not been registered, return an 
  ** error. Otherwise, do nothing.
  */
  if( !pMod ){
    *pzErr = sqlite3MPrintf("no such module: %s", zModule);
    rc = SQLITE_ERROR;
  }else{
    rc = vtabCallConstructor(db, pTab, pMod, pMod->pModule->xCreate, pzErr);
  }

  if( rc==SQLITE_OK && pTab->pVtab ){
    rc = addToVTrans(db, pTab->pVtab);
  }

  return rc;
}

/*
** This function is used to set the schema of a virtual table.  It is only
** valid to call this function from within the xCreate() or xConnect() of a
** virtual table module.
*/
int sqlite3_declare_vtab(sqlite3 *db, const char *zCreateTable){
  Parse sParse;

  int rc = SQLITE_OK;
  Table *pTab = db->pVTab;
  char *zErr = 0;

  if( !pTab ){
    sqlite3Error(db, SQLITE_MISUSE, 0);
    return SQLITE_MISUSE;
  }
  assert(pTab->isVirtual && pTab->nCol==0 && pTab->aCol==0);

  memset(&sParse, 0, sizeof(Parse));
  sParse.declareVtab = 1;
  sParse.db = db;

  if( 
      SQLITE_OK == sqlite3RunParser(&sParse, zCreateTable, &zErr) && 
      sParse.pNewTable && 
      !sParse.pNewTable->pSelect && 
      !sParse.pNewTable->isVirtual 
  ){
    pTab->aCol = sParse.pNewTable->aCol;
    pTab->nCol = sParse.pNewTable->nCol;
    sParse.pNewTable->nCol = 0;
    sParse.pNewTable->aCol = 0;
  } else {
    sqlite3Error(db, SQLITE_ERROR, zErr);
    sqliteFree(zErr);
    rc = SQLITE_ERROR;
  }
  sParse.declareVtab = 0;

  sqlite3_finalize((sqlite3_stmt*)sParse.pVdbe);
  sqlite3DeleteTable(0, sParse.pNewTable);
  sParse.pNewTable = 0;
  db->pVTab = 0;

  return rc;
}

/*
** This function is invoked by the vdbe to call the xDestroy method
** of the virtual table named zTab in database iDb. This occurs
** when a DROP TABLE is mentioned.
**
** This call is a no-op if zTab is not a virtual table.
*/
int sqlite3VtabCallDestroy(sqlite3 *db, int iDb, const char *zTab)
{
  int rc = SQLITE_OK;
  Table *pTab;

  pTab = sqlite3FindTable(db, zTab, db->aDb[iDb].zName);
  assert(pTab);
  if( pTab->pVtab ){
    int (*xDestroy)(sqlite3_vtab *pVTab) = pTab->pMod->pModule->xDestroy;
    rc = sqlite3SafetyOff(db);
    assert( rc==SQLITE_OK );
    if( xDestroy ){
      rc = xDestroy(pTab->pVtab);
    }
    sqlite3SafetyOn(db);
    if( rc==SQLITE_OK ){
      pTab->pVtab = 0;
    }
  }

  return rc;
}

void sqlite3VtabCodeLock(Parse *pParse, Table *pTab){
  Vdbe *v = sqlite3GetVdbe(pParse);
  sqlite3VdbeOp3(v, OP_VBegin, 0, 0, (const char*)pTab->pVtab, P3_VTAB);
}

/*
** This function invokes either the xRollback or xCommit method
** of each of the virtual tables in the sqlite3.aVTrans array. The method
** called is identified by the second argument, "offset", which is
** the offset of the method to call in the sqlite3_module structure.
**
** The array is cleared after invoking the callbacks. 
*/
static void callFinaliser(sqlite3 *db, int offset){
  int i;
  for(i=0; i<db->nVTrans && db->aVTrans[i]; i++){
    sqlite3_vtab *pVtab = db->aVTrans[i];
    int (*x)(sqlite3_vtab *);
    x = *(int (**)(sqlite3_vtab *))((char *)pVtab->pModule + offset);
    if( x ) x(pVtab);
  }
  sqliteFree(db->aVTrans);
  db->nVTrans = 0;
  db->aVTrans = 0;
}

/*
** If argument rc is not SQLITE_OK, then return it and do nothing. 
** Otherwise, invoke the xSync method of all virtual tables in the 
** sqlite3.aVTrans array. Return the error code for the first error 
** that occurs, or SQLITE_OK if all xSync operations are successful.
*/
int sqlite3VtabSync(sqlite3 *db, int rc2){
  int i;
  int rc = SQLITE_OK;
  if( rc2!=SQLITE_OK ) return rc2;
  for(i=0; rc==SQLITE_OK && i<db->nVTrans && db->aVTrans[i]; i++){
    sqlite3_vtab *pVtab = db->aVTrans[i];
    int (*x)(sqlite3_vtab *);
    x = pVtab->pModule->xSync;
    if( x ){
      rc = x(pVtab);
    }
  }
  return rc;
}

/*
** Invoke the xRollback method of all virtual tables in the 
** sqlite3.aVTrans array. Then clear the array itself.
*/
int sqlite3VtabRollback(sqlite3 *db){
  callFinaliser(db, (int)(&((sqlite3_module *)0)->xRollback));
  return SQLITE_OK;
}

/*
** Invoke the xCommit method of all virtual tables in the 
** sqlite3.aVTrans array. Then clear the array itself.
*/
int sqlite3VtabCommit(sqlite3 *db){
  callFinaliser(db, (int)(&((sqlite3_module *)0)->xCommit));
  return SQLITE_OK;
}

/*
** If the virtual table pVtab supports the transaction interface
** (xBegin/xRollback/xCommit and optionally xSync) and a transaction is
** not currently open, invoke the xBegin method now.
**
** If the xBegin call is successful, place the sqlite3_vtab pointer
** in the sqlite3.aVTrans array.
*/
int sqlite3VtabBegin(sqlite3 *db, sqlite3_vtab *pVtab){
  int rc = SQLITE_OK;
  const sqlite3_module *pModule = pVtab->pModule;
  if( pModule->xBegin ){
    int i;

    /* If pVtab is already in the aVTrans array, return early */
    for(i=0; (i<db->nVTrans) && 0!=db->aVTrans[i]; i++){
      if( db->aVTrans[i]==pVtab ){
        return SQLITE_OK;
      }
    }

    /* Invoke the xBegin method */
    rc = pModule->xBegin(pVtab);
    if( rc!=SQLITE_OK ){
      return rc;
    }

    rc = addToVTrans(db, pVtab);
  }
  return rc;
}

#endif /* SQLITE_OMIT_VIRTUALTABLE */
