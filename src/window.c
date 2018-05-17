/*
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
*/
#include "sqliteInt.h"

void sqlite3WindowDelete(sqlite3 *db, Window *p){
  if( p ){
    sqlite3ExprDelete(db, p->pFilter);
    sqlite3ExprListDelete(db, p->pPartition);
    sqlite3ExprListDelete(db, p->pOrderBy);
    sqlite3ExprDelete(db, p->pEnd);
    sqlite3ExprDelete(db, p->pStart);
    sqlite3DbFree(db, p);
  }
}

Window *sqlite3WindowAlloc(
  Parse *pParse, 
  int eType,
  int eEnd, Expr *pEnd, 
  int eStart, Expr *pStart
){
  Window *pWin = (Window*)sqlite3DbMallocZero(pParse->db, sizeof(Window));

  if( pWin ){
    pWin->eType = eType;
    pWin->eStart = eStart;
    pWin->eEnd = eEnd;
    pWin->pEnd = pEnd;
    pWin->pStart = pStart;
  }else{
    sqlite3ExprDelete(pParse->db, pEnd);
    sqlite3ExprDelete(pParse->db, pStart);
  }

  return pWin;
}

void sqlite3WindowAttach(Parse *pParse, Expr *p, Window *pWin){
  if( p ){
    p->pWin = pWin;
  }else{
    sqlite3WindowDelete(pParse->db, pWin);
  }
}

/*
** Return 0 if the two window objects are identical, or non-zero otherwise.
*/
int sqlite3WindowCompare(Parse *pParse, Window *p1, Window *p2){
  if( p1->eType!=p2->eType ) return 1;
  if( p1->eStart!=p2->eStart ) return 1;
  if( p1->eEnd!=p2->eEnd ) return 1;
  if( sqlite3ExprCompare(pParse, p1->pStart, p2->pStart, -1) ) return 1;
  if( sqlite3ExprCompare(pParse, p1->pEnd, p2->pEnd, -1) ) return 1;
  if( sqlite3ExprListCompare(p1->pPartition, p2->pPartition, -1) ) return 1;
  if( sqlite3ExprListCompare(p1->pOrderBy, p2->pOrderBy, -1) ) return 1;
  return 0;
}


