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

#endif /* SQLITE_OMIT_UPSERT */
