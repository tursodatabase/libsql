/*
** 2004 November 21
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains code used to implement the DECLARE...CURSOR syntax
** of SQL and related processing.
**
** Do not confuse SQL cursors and B-tree cursors.  An SQL cursor (as
** implemented by this file) is a user-visible cursor that is created
** using the DECLARE...CURSOR command and deleted using CLOSE.  A
** B-tree cursor is an abstraction of the b-tree layer.  See the btree.c
** module for additional information.  There is also a VDBE-cursor that
** is used by the VDBE module.  Even though all these objects are called
** cursors, they are really very different things.  It is worth your while
** to fully understand the difference.
**
** @(#) $Id: cursor.c,v 1.2 2004/11/23 01:47:30 drh Exp $
*/
#ifndef SQLITE_OMIT_CURSOR
#include "sqliteInt.h"
#include "vdbeInt.h"

/*
** Delete a cursor object.
*/
void sqlite3CursorDelete(SqlCursor *p){
  if( p ){
    int i;
    sqlite3SelectDelete(p->pSelect);
    for(i=0; i<p->nPtr; i++){
      sqlite3VdbeMemRelease(&p->aPtr[i]);
    }
    sqliteFree(p->aPtr);
    sqliteFree(p);
  }
}

/*
** Look up a cursor by name.  Return NULL if not found.
*/
static SqlCursor *findCursor(sqlite3 *db, Token *pName){
  int i;
  SqlCursor *p;
  for(i=0; i<db->nSqlCursor; i++){
    p = db->apSqlCursor[i];
    if( p && sqlite3StrNICmp(p->zName, pName->z, pName->n)==0 ){
      return p;
    }
  }
  return 0;
}  

/*
** The parser calls this routine in order to create a new cursor.
** The arguments are the name of the new cursor and the SELECT statement
** that the new cursor will access.
*/
void sqlite3CursorCreate(Parse *pParse, Token *pName, Select *pSelect){
  SqlCursor *pNew;
  sqlite3 *db = pParse->db;
  int i;
  

  pNew = findCursor(db, pName);
  if( pNew ){
    sqlite3ErrorMsg(pParse, "another cursor named %T already exists", pName);
    goto end_create_cursor;
  }
  if( pSelect==0 ){
    /* This can only happen due to a prior malloc failure */
    goto end_create_cursor;
  }
  for(i=0; i<db->nSqlCursor; i++){
    if( db->apSqlCursor[i]==0 ) break;
  }
  if( i>=db->nSqlCursor ){
    db->apSqlCursor = sqliteRealloc(db->apSqlCursor, (i+1)*sizeof(pNew));
    db->nSqlCursor = i+1;
  }
  db->apSqlCursor[i] = pNew = sqliteMallocRaw( sizeof(*pNew) + pName->n + 1 );
  if( pNew==0 ) goto end_create_cursor;
  pNew->idx = i;
  pNew->zName = (char*)&pNew[1];
  memcpy(pNew->zName, pName->z, pName->n);
  pNew->zName[pName->n] = 0;
  pNew->pSelect = sqlite3SelectDup(pSelect);
  pNew->nPtr = 2;
  pNew->aPtr = sqliteMalloc( sizeof(Mem)*2 );
  for(i=0; i<2; i++){
    pNew->aPtr[i].flags = MEM_Null;
  }

end_create_cursor:
  sqlite3SelectDelete(pSelect);
}

/*
** The parser calls this routine in response to a CLOSE command.  Delete
** the cursor named in the argument.
*/
void sqlite3CursorClose(Parse *pParse, Token *pName){
  SqlCursor *p;
  sqlite3 *db = pParse->db;

  p = findCursor(db, pName);
  if( p==0 ){
    sqlite3ErrorMsg(pParse, "no such cursor: %T", pName);
    return;
  }
  assert( p->idx>=0 && p->idx<db->nSqlCursor );
  assert( db->apSqlCursor[p->idx]==p );
  db->apSqlCursor[p->idx] = 0;
  sqlite3CursorDelete(p);
}

/*
** Reverse the direction the ORDER BY clause on the SELECT statement.
*/
static void reverseSortOrder(Select *p){
  if( p->pOrderBy==0 ){
    /* If there no ORDER BY clause, add a new one that is "rowid DESC" */
    static const Token rowid = { "ROWID", 0, 5 };
    Expr *pExpr = sqlite3Expr(TK_ID, 0, 0, &rowid);
    ExprList *pList = sqlite3ExprListAppend(0, pExpr, 0);
    if( pList )  pList->a[0].sortOrder = SQLITE_SO_DESC;
    p->pOrderBy = pList;
  }else{
    int i;
    ExprList *pList = p->pOrderBy;
    for(i=0; i<pList->nExpr; i++){
      pList->a[i].sortOrder = !pList->a[i].sortOrder;
    }
  }
}

/*
** The parser calls this routine when it sees a complete FETCH statement.
** This routine generates code to implement the FETCH.
**
** Information about the direction of the FETCH has already been inserted
** into the pParse structure by parser rules.  The arguments specify the
** name of the cursor from which we are fetching and the optional INTO
** clause.
*/
void sqlite3Fetch(Parse *pParse, Token *pName, IdList *pInto){
  SqlCursor *p;
  sqlite3 *db = pParse->db;
  Select *pCopy;
  Fetch sFetch;

  p = findCursor(db, pName);
  if( p==0 ){
    sqlite3ErrorMsg(pParse, "no such cursor: %T", pName);
    return;
  }
  sFetch.pCursor = p;
  pCopy = sqlite3SelectDup(p->pSelect);
  pCopy->pFetch = &sFetch;
  switch( pParse->fetchDir ){
    case TK_FIRST: {
      sFetch.isBackwards = 0;
      sFetch.doRewind = 1;
      pCopy->nLimit = pParse->dirArg1;
      pCopy->nOffset = 0;
      break;
    }
    case TK_LAST: {
      reverseSortOrder(pCopy);
      sFetch.isBackwards = 1;
      sFetch.doRewind = 1;
      pCopy->nLimit = pParse->dirArg1;
      pCopy->nOffset = 0;
      break;
    }
    case TK_NEXT: {
      sFetch.isBackwards = 0;
      sFetch.doRewind = 0;
      pCopy->nLimit = pParse->dirArg1;
      pCopy->nOffset = 0;
      break;
    }
    case TK_PRIOR: {
      reverseSortOrder(pCopy);
      sFetch.isBackwards = 1;
      sFetch.doRewind = 0;
      pCopy->nLimit = pParse->dirArg1;
      pCopy->nOffset = 0;
      break;
    }
    case TK_ABSOLUTE: {
      sFetch.isBackwards = 0;
      sFetch.doRewind = 1;
      pCopy->nLimit = pParse->dirArg1;
      pCopy->nOffset = pParse->dirArg2;
      break;
    }
    default: {
      assert( pParse->fetchDir==TK_RELATIVE );
      if( pParse->dirArg2>=0 ){
        /* The index parameter is positive.  Move forward from the current
        ** location */
        sFetch.isBackwards = 0;
        sFetch.doRewind = 0;
        pCopy->nLimit = pParse->dirArg1;
        pCopy->nOffset = pParse->dirArg2;
      }else{
        /* The index is negative.  We have to code two separate SELECTs.
        ** The first one seeks to the no position and the second one does
        ** the query.
        */
        Select *pSeek = sqlite3SelectDup(pCopy);
        reverseSortOrder(pSeek);
        sFetch.isBackwards = 1;
        sFetch.doRewind = 0;
        pSeek->nLimit = pParse->dirArg2;
        pSeek->pFetch = &sFetch;
        sqlite3Select(pParse, pSeek, SRT_Discard, 0, 0, 0, 0, 0);
        sFetch.isBackwards = 0;
        sFetch.doRewind = 0;
        pCopy->nLimit = pParse->dirArg1;
        pCopy->nOffset = 0;
      }
      break;
    }
  }
  sqlite3Select(pParse, pCopy, SRT_Callback, 0, 0, 0, 0, 0);

end_fetch:
  sqlite3IdListDelete(pInto);
}

#endif /* SQLITE_OMIT_CURSOR */
