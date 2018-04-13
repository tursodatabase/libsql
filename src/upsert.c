/*
** 2018-04-12
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains code to implement various aspects of UPSERT
** processing and handling of the Upsert object.
*/
#include "sqliteInt.h"

#ifndef SQLITE_OMIT_UPSERT
/*
** Free a list of Upsert objects
*/
void sqlite3UpsertDelete(sqlite3 *db, Upsert *p){
  while( p ){
    Upsert *pNext = p->pUpsertNext;
    sqlite3ExprListDelete(db, p->pUpsertTarget);
    sqlite3ExprListDelete(db, p->pUpsertSet);
    sqlite3ExprDelete(db, p->pUpsertWhere);
    sqlite3DbFree(db, p);
    p = pNext;
  }
}

/*
** Duplicate an Upsert object.
*/
Upsert *sqlite3UpsertDup(sqlite3 *db, Upsert *p){
  if( p==0 ) return 0;
  return sqlite3UpsertNew(db,
           sqlite3UpsertDup(db, p->pUpsertNext),
           sqlite3ExprListDup(db, p->pUpsertTarget, 0),
           sqlite3ExprListDup(db, p->pUpsertSet, 0),
           sqlite3ExprDup(db, p->pUpsertWhere, 0)
         );
}

/*
** Create a new Upsert object.
*/
Upsert *sqlite3UpsertNew(
  sqlite3 *db,           /* Determines which memory allocator to use */
  Upsert *pPrior,        /* Append this upsert to the end of the new one */
  ExprList *pTarget,     /* Target argument to ON CONFLICT, or NULL */
  ExprList *pSet,        /* UPDATE columns, or NULL for a DO NOTHING */
  Expr *pWhere           /* WHERE clause for the ON CONFLICT UPDATE */
){
  Upsert *pNew;
  pNew = sqlite3DbMallocRaw(db, sizeof(Upsert));
  if( pNew==0 ){
    sqlite3UpsertDelete(db, pPrior);
    sqlite3ExprListDelete(db, pTarget);
    sqlite3ExprListDelete(db, pSet);
    sqlite3ExprDelete(db, pWhere);
    return 0;
  }else{
    pNew->pUpsertTarget = pTarget;
    pNew->pUpsertSet = pSet;
    pNew->pUpsertNext = pPrior;
    pNew->pUpsertWhere = pWhere;
  }
  return pNew;
}

/*
** Analyze the ON CONFLICT clause(s) described by pUpsert.  Resolve all
** symbols in the conflict-target clausees.  Fill in the pUpsertIdx pointers.
**
** Return non-zero if there are errors.
*/
int sqlite3UpsertAnalyze(
  Parse *pParse,     /* The parsing context */
  SrcList *pTabList, /* Table into which we are inserting */
  Upsert *pUpsert    /* The list of ON CONFLICT clauses */
){
  NameContext sNC;
  Upsert *p;
  Table *pTab;
  Index *pIdx;
  int rc = SQLITE_OK;
  int nDoNothing = 0;

  assert( pTabList->nSrc==1 );
  assert( pTabList->a[0].pTab!=0 );
  memset(&sNC, 0, sizeof(sNC));
  sNC.pParse = pParse;
  sNC.pSrcList = pTabList;
  pTab = pTabList->a[0].pTab;
  for(p=pUpsert; p; p=p->pUpsertNext){
    if( p->pUpsertTarget==0 ){
      if( p->pUpsertSet ){
        /* This is a MySQL-style ON DUPLICATE KEY clause.  The ON DUPLICATE
        ** KEY clause can only be used if there is exactly one uniqueness
        ** constraint and/or PRIMARY KEY */
        int nUnique = 0;
        for(pIdx = pTab->pIndex; pIdx; pIdx=pIdx->pNext){
          if( IsUniqueIndex(pIdx) ){
            p->pUpsertIdx = pIdx;
            nUnique++;
          }
        }
        if( pTab->iPKey>=0 ) nUnique++;
        if( nUnique!=0 ){
          sqlite3ErrorMsg(pParse, "ON DUPLICATE KEY may only be used if there "
               "is exactly one UNIQUE or PRIMARY KEY constraint");
          return SQLITE_ERROR;
        }
      }else{
        nDoNothing++;
        if( nDoNothing>1 ){
          sqlite3ErrorMsg(pParse, "multiple unconstrained DO NOTHING clauses");
          return SQLITE_ERROR;
        }
      }
      continue;
    }
    rc = sqlite3ResolveExprListNames(&sNC, p->pUpsertTarget);
    if( rc ) return rc;
  }
  return rc;
}

#endif /* SQLITE_OMIT_UPSERT */
