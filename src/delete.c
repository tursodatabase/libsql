/*
** 2001 September 15
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains C code routines that are called by the parser
** to handle DELETE FROM statements.
**
** $Id: delete.c,v 1.16 2001/10/08 13:22:32 drh Exp $
*/
#include "sqliteInt.h"

/*
** Process a DELETE FROM statement.
*/
void sqliteDeleteFrom(
  Parse *pParse,         /* The parser context */
  Token *pTableName,     /* The table from which we should delete things */
  Expr *pWhere           /* The WHERE clause.  May be null */
){
  Vdbe *v;               /* The virtual database engine */
  Table *pTab;           /* The table from which records will be deleted */
  IdList *pTabList;      /* An ID list holding pTab and nothing else */
  int end, addr;         /* A couple addresses of generated code */
  int i;                 /* Loop counter */
  WhereInfo *pWInfo;     /* Information about the WHERE clause */
  Index *pIdx;           /* For looping over indices of the table */
  int base;              /* Index of the first available table cursor */
  sqlite *db;            /* Main database structure */

  if( pParse->nErr || sqlite_malloc_failed ){
    pTabList = 0;
    goto delete_from_cleanup;
  }
  db = pParse->db;

  /* Locate the table which we want to delete.  This table has to be
  ** put in an IdList structure because some of the subroutines we
  ** will be calling are designed to work with multiple tables and expect
  ** an IdList* parameter instead of just a Table* parameger.
  */
  pTabList = sqliteIdListAppend(0, pTableName);
  if( pTabList==0 ) goto delete_from_cleanup;
  for(i=0; i<pTabList->nId; i++){
    pTabList->a[i].pTab = sqliteFindTable(db, pTabList->a[i].zName);
    if( pTabList->a[i].pTab==0 ){
      sqliteSetString(&pParse->zErrMsg, "no such table: ", 
         pTabList->a[i].zName, 0);
      pParse->nErr++;
      goto delete_from_cleanup;
    }
    if( pTabList->a[i].pTab->readOnly ){
      sqliteSetString(&pParse->zErrMsg, "table ", pTabList->a[i].zName,
        " may not be modified", 0);
      pParse->nErr++;
      goto delete_from_cleanup;
    }
  }
  pTab = pTabList->a[0].pTab;

  /* Resolve the column names in all the expressions.
  */
  if( pWhere ){
    sqliteExprResolveInSelect(pParse, pWhere);
    if( sqliteExprResolveIds(pParse, pTabList, pWhere) ){
      goto delete_from_cleanup;
    }
    if( sqliteExprCheck(pParse, pWhere, 0, 0) ){
      goto delete_from_cleanup;
    }
  }

  /* Begin generating code.
  */
  v = sqliteGetVdbe(pParse);
  if( v==0 ) goto delete_from_cleanup;
  if( (db->flags & SQLITE_InTrans)==0 ){
    sqliteVdbeAddOp(v, OP_Transaction, 0, 0, 0, 0);
    sqliteVdbeAddOp(v, OP_VerifyCookie, db->schema_cookie, 0, 0, 0);
    pParse->schemaVerified = 1;
  }


  /* Special case: A DELETE without a WHERE clause deletes everything.
  ** It is easier just to erase the whole table.
  */
  if( pWhere==0 ){
    sqliteVdbeAddOp(v, OP_Clear, pTab->tnum, pTab->isTemp, 0, 0);
    for(pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext){
      sqliteVdbeAddOp(v, OP_Clear, pIdx->tnum, pTab->isTemp, 0, 0);
    }
  }

  /* The usual case: There is a WHERE clause so we have to scan through
  ** the table an pick which records to delete.
  */
  else{
    int openOp;

    /* Begin the database scan
    */
    sqliteVdbeAddOp(v, OP_ListOpen, 0, 0, 0, 0);
    pWInfo = sqliteWhereBegin(pParse, pTabList, pWhere, 1);
    if( pWInfo==0 ) goto delete_from_cleanup;

    /* Remember the key of every item to be deleted.
    */
    sqliteVdbeAddOp(v, OP_ListWrite, 0, 0, 0, 0);

    /* End the database scan loop.
    */
    sqliteWhereEnd(pWInfo);

    /* Delete every item whose key was written to the list during the
    ** database scan.  We have to delete items after the scan is complete
    ** because deleting an item can change the scan order.
    */
    base = pParse->nTab;
    sqliteVdbeAddOp(v, OP_ListRewind, 0, 0, 0, 0);
    openOp = pTab->isTemp ? OP_OpenWrAux : OP_OpenWrite;
    sqliteVdbeAddOp(v, openOp, base, pTab->tnum, 0, 0);
    for(i=1, pIdx=pTab->pIndex; pIdx; i++, pIdx=pIdx->pNext){
      sqliteVdbeAddOp(v, openOp, base+i, pIdx->tnum, 0, 0);
    }
    end = sqliteVdbeMakeLabel(v);
    addr = sqliteVdbeAddOp(v, OP_ListRead, 0, end, 0, 0);
    sqliteVdbeAddOp(v, OP_MoveTo, base, 0, 0, 0);
    if( pTab->pIndex ){
      for(i=1, pIdx=pTab->pIndex; pIdx; i++, pIdx=pIdx->pNext){
        int j;
        sqliteVdbeAddOp(v, OP_Recno, base, 0, 0, 0);
        for(j=0; j<pIdx->nColumn; j++){
          sqliteVdbeAddOp(v, OP_Column, base, pIdx->aiColumn[j], 0, 0);
        }
        sqliteVdbeAddOp(v, OP_MakeIdxKey, pIdx->nColumn, 0, 0, 0);
        sqliteVdbeAddOp(v, OP_DeleteIdx, base+i, 0, 0, 0);
      }
    }
    sqliteVdbeAddOp(v, OP_Delete, base, 0, 0, 0);
    sqliteVdbeAddOp(v, OP_Goto, 0, addr, 0, 0);
    sqliteVdbeAddOp(v, OP_ListClose, 0, 0, 0, end);
  }
  if( (db->flags & SQLITE_InTrans)==0 ){
    sqliteVdbeAddOp(v, OP_Commit, 0, 0, 0, 0);
  }


delete_from_cleanup:
  sqliteIdListDelete(pTabList);
  sqliteExprDelete(pWhere);
  return;
}
