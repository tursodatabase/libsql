/*
** 2003 April 6
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains code used to implement the ATTACH and DETACH commands.
**
** $Id: attach.c,v 1.20 2004/06/29 08:59:35 danielk1977 Exp $
*/
#include "sqliteInt.h"

/*
** This routine is called by the parser to process an ATTACH statement:
**
**     ATTACH DATABASE filename AS dbname
**
** The pFilename and pDbname arguments are the tokens that define the
** filename and dbname in the ATTACH statement.
*/
void sqlite3Attach(Parse *pParse, Token *pFilename, Token *pDbname, Token *pKey){
  Db *aNew;
  int rc, i;
  char *zFile, *zName;
  sqlite *db;
  Vdbe *v;

  v = sqlite3GetVdbe(pParse);
  sqlite3VdbeAddOp(v, OP_Halt, 0, 0);
  if( pParse->explain ) return;
  db = pParse->db;
  if( db->nDb>=MAX_ATTACHED+2 ){
    sqlite3ErrorMsg(pParse, "too many attached databases - max %d", 
       MAX_ATTACHED);
    pParse->rc = SQLITE_ERROR;
    return;
  }

  if( !db->autoCommit ){
    sqlite3ErrorMsg(pParse, "cannot ATTACH database within transaction");
    pParse->rc = SQLITE_ERROR;
    return;
  }

  zFile = sqlite3NameFromToken(pFilename);;
  if( zFile==0 ) return;
#ifndef SQLITE_OMIT_AUTHORIZATION
  if( sqlite3AuthCheck(pParse, SQLITE_ATTACH, zFile, 0, 0)!=SQLITE_OK ){
    sqliteFree(zFile);
    return;
  }
#endif /* SQLITE_OMIT_AUTHORIZATION */

  zName = sqlite3NameFromToken(pDbname);
  if( zName==0 ) return;
  for(i=0; i<db->nDb; i++){
    char *z = db->aDb[i].zName;
    if( z && sqlite3StrICmp(z, zName)==0 ){
      sqlite3ErrorMsg(pParse, "database %z is already in use", zName);
      pParse->rc = SQLITE_ERROR;
      sqliteFree(zFile);
      return;
    }
  }

  if( db->aDb==db->aDbStatic ){
    aNew = sqliteMalloc( sizeof(db->aDb[0])*3 );
    if( aNew==0 ) return;
    memcpy(aNew, db->aDb, sizeof(db->aDb[0])*2);
  }else{
    aNew = sqliteRealloc(db->aDb, sizeof(db->aDb[0])*(db->nDb+1) );
    if( aNew==0 ) return;
  }
  db->aDb = aNew;
  aNew = &db->aDb[db->nDb++];
  memset(aNew, 0, sizeof(*aNew));
  sqlite3HashInit(&aNew->tblHash, SQLITE_HASH_STRING, 0);
  sqlite3HashInit(&aNew->idxHash, SQLITE_HASH_STRING, 0);
  sqlite3HashInit(&aNew->trigHash, SQLITE_HASH_STRING, 0);
  sqlite3HashInit(&aNew->aFKey, SQLITE_HASH_STRING, 1);
  aNew->zName = zName;
  aNew->safety_level = 3;
  rc = sqlite3BtreeFactory(db, zFile, 0, MAX_PAGES, &aNew->pBt);
  if( rc ){
    sqlite3ErrorMsg(pParse, "unable to open database: %s", zFile);
  }
#if SQLITE_HAS_CODEC
  {
    extern int sqliteCodecAttach(sqlite*, int, void*, int);
    char *zKey = 0;
    int nKey;
    if( pKey && pKey->z && pKey->n ){
      sqlite3SetNString(&zKey, pKey->z, pKey->n, 0);
      sqlite3Dequote(zKey);
      nKey = strlen(zKey);
    }else{
      zKey = 0;
      nKey = 0;
    }
    sqliteCodecAttach(db, db->nDb-1, zKey, nKey);
  }
#endif
  sqliteFree(zFile);
  db->flags &= ~SQLITE_Initialized;
  if( pParse->nErr ) return;
  if( rc==SQLITE_OK ){
    rc = sqlite3ReadSchema(pParse);
  }
  if( rc ){
    int i = db->nDb - 1;
    assert( i>=2 );
    if( db->aDb[i].pBt ){
      sqlite3BtreeClose(db->aDb[i].pBt);
      db->aDb[i].pBt = 0;
    }
    sqlite3ResetInternalSchema(db, 0);
    if( 0==pParse->nErr ){
      pParse->nErr++;
      pParse->rc = SQLITE_ERROR;
    }
  }
}

/*
** This routine is called by the parser to process a DETACH statement:
**
**    DETACH DATABASE dbname
**
** The pDbname argument is the name of the database in the DETACH statement.
*/
void sqlite3Detach(Parse *pParse, Token *pDbname){
  int i;
  sqlite *db;
  Vdbe *v;
  Db *pDb = 0;

  v = sqlite3GetVdbe(pParse);
  sqlite3VdbeAddOp(v, OP_Halt, 0, 0);
  if( pParse->explain ) return;
  db = pParse->db;
  for(i=0; i<db->nDb; i++){
    pDb = &db->aDb[i];
    if( pDb->pBt==0 || pDb->zName==0 ) continue;
    if( strlen(pDb->zName)!=pDbname->n ) continue;
    if( sqlite3StrNICmp(pDb->zName, pDbname->z, pDbname->n)==0 ) break;
  }
  if( i>=db->nDb ){
    sqlite3ErrorMsg(pParse, "no such database: %T", pDbname);
    return;
  }
  if( i<2 ){
    sqlite3ErrorMsg(pParse, "cannot detach database %T", pDbname);
    return;
  }
  if( !db->autoCommit ){
    sqlite3ErrorMsg(pParse, "cannot DETACH database within transaction");
    pParse->rc = SQLITE_ERROR;
    return;
  }
#ifndef SQLITE_OMIT_AUTHORIZATION
  if( sqlite3AuthCheck(pParse,SQLITE_DETACH,db->aDb[i].zName,0,0)!=SQLITE_OK ){
    return;
  }
#endif /* SQLITE_OMIT_AUTHORIZATION */
  sqlite3BtreeClose(pDb->pBt);
  pDb->pBt = 0;
  sqliteFree(pDb->zName);
  sqlite3ResetInternalSchema(db, i);
  db->nDb--;
  if( i<db->nDb ){
    db->aDb[i] = db->aDb[db->nDb];
    memset(&db->aDb[db->nDb], 0, sizeof(db->aDb[0]));
    sqlite3ResetInternalSchema(db, i);
  }
}

/*
** Initialize a DbFixer structure.  This routine must be called prior
** to passing the structure to one of the sqliteFixAAAA() routines below.
**
** The return value indicates whether or not fixation is required.  TRUE
** means we do need to fix the database references, FALSE means we do not.
*/
int sqlite3FixInit(
  DbFixer *pFix,      /* The fixer to be initialized */
  Parse *pParse,      /* Error messages will be written here */
  int iDb,            /* This is the database that must must be used */
  const char *zType,  /* "view", "trigger", or "index" */
  const Token *pName  /* Name of the view, trigger, or index */
){
  sqlite *db;

  if( iDb<0 || iDb==1 ) return 0;
  db = pParse->db;
  assert( db->nDb>iDb );
  pFix->pParse = pParse;
  pFix->zDb = db->aDb[iDb].zName;
  pFix->zType = zType;
  pFix->pName = pName;
  return 1;
}

/*
** The following set of routines walk through the parse tree and assign
** a specific database to all table references where the database name
** was left unspecified in the original SQL statement.  The pFix structure
** must have been initialized by a prior call to sqlite3FixInit().
**
** These routines are used to make sure that an index, trigger, or
** view in one database does not refer to objects in a different database.
** (Exception: indices, triggers, and views in the TEMP database are
** allowed to refer to anything.)  If a reference is explicitly made
** to an object in a different database, an error message is added to
** pParse->zErrMsg and these routines return non-zero.  If everything
** checks out, these routines return 0.
*/
int sqlite3FixSrcList(
  DbFixer *pFix,       /* Context of the fixation */
  SrcList *pList       /* The Source list to check and modify */
){
  int i;
  const char *zDb;

  if( pList==0 ) return 0;
  zDb = pFix->zDb;
  for(i=0; i<pList->nSrc; i++){
    if( pList->a[i].zDatabase==0 ){
      pList->a[i].zDatabase = sqliteStrDup(zDb);
    }else if( sqlite3StrICmp(pList->a[i].zDatabase,zDb)!=0 ){
      sqlite3ErrorMsg(pFix->pParse,
         "%s %z cannot reference objects in database %s",
         pFix->zType, sqliteStrNDup(pFix->pName->z, pFix->pName->n),
         pList->a[i].zDatabase);
      return 1;
    }
    if( sqlite3FixSelect(pFix, pList->a[i].pSelect) ) return 1;
    if( sqlite3FixExpr(pFix, pList->a[i].pOn) ) return 1;
  }
  return 0;
}
int sqlite3FixSelect(
  DbFixer *pFix,       /* Context of the fixation */
  Select *pSelect      /* The SELECT statement to be fixed to one database */
){
  while( pSelect ){
    if( sqlite3FixExprList(pFix, pSelect->pEList) ){
      return 1;
    }
    if( sqlite3FixSrcList(pFix, pSelect->pSrc) ){
      return 1;
    }
    if( sqlite3FixExpr(pFix, pSelect->pWhere) ){
      return 1;
    }
    if( sqlite3FixExpr(pFix, pSelect->pHaving) ){
      return 1;
    }
    pSelect = pSelect->pPrior;
  }
  return 0;
}
int sqlite3FixExpr(
  DbFixer *pFix,     /* Context of the fixation */
  Expr *pExpr        /* The expression to be fixed to one database */
){
  while( pExpr ){
    if( sqlite3FixSelect(pFix, pExpr->pSelect) ){
      return 1;
    }
    if( sqlite3FixExprList(pFix, pExpr->pList) ){
      return 1;
    }
    if( sqlite3FixExpr(pFix, pExpr->pRight) ){
      return 1;
    }
    pExpr = pExpr->pLeft;
  }
  return 0;
}
int sqlite3FixExprList(
  DbFixer *pFix,     /* Context of the fixation */
  ExprList *pList    /* The expression to be fixed to one database */
){
  int i;
  if( pList==0 ) return 0;
  for(i=0; i<pList->nExpr; i++){
    if( sqlite3FixExpr(pFix, pList->a[i].pExpr) ){
      return 1;
    }
  }
  return 0;
}
int sqlite3FixTriggerStep(
  DbFixer *pFix,     /* Context of the fixation */
  TriggerStep *pStep /* The trigger step be fixed to one database */
){
  while( pStep ){
    if( sqlite3FixSelect(pFix, pStep->pSelect) ){
      return 1;
    }
    if( sqlite3FixExpr(pFix, pStep->pWhere) ){
      return 1;
    }
    if( sqlite3FixExprList(pFix, pStep->pExprList) ){
      return 1;
    }
    pStep = pStep->pNext;
  }
  return 0;
}
