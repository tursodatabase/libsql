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
** $Id: delete.c,v 1.24 2002/01/29 18:41:25 drh Exp $
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
  int openOp;            /* Opcode used to open a cursor to the table */


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
    if( sqliteExprResolveIds(pParse, pTabList, 0, pWhere) ){
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
    sqliteVdbeAddOp(v, OP_Transaction, 0, 0);
    sqliteVdbeAddOp(v, OP_VerifyCookie, db->schema_cookie, 0);
    pParse->schemaVerified = 1;
  }

  /* Initialize the counter of the number of rows deleted, if
  ** we are counting rows.
  */
  if( db->flags & SQLITE_CountRows ){
    sqliteVdbeAddOp(v, OP_Integer, 0, 0);
  }

  /* Special case: A DELETE without a WHERE clause deletes everything.
  ** It is easier just to erase the whole table.
  */
  if( pWhere==0 ){
    if( db->flags & SQLITE_CountRows ){
      /* If counting rows deleted, just count the total number of
      ** entries in the table. */
      int endOfLoop = sqliteVdbeMakeLabel(v);
      int addr;
      openOp = pTab->isTemp ? OP_OpenAux : OP_Open;
      sqliteVdbeAddOp(v, openOp, 0, pTab->tnum);
      sqliteVdbeAddOp(v, OP_Rewind, 0, sqliteVdbeCurrentAddr(v)+2);
      addr = sqliteVdbeAddOp(v, OP_AddImm, 1, 0);
      sqliteVdbeAddOp(v, OP_Next, 0, addr);
      sqliteVdbeResolveLabel(v, endOfLoop);
      sqliteVdbeAddOp(v, OP_Close, 0, 0);
    }
    sqliteVdbeAddOp(v, OP_Clear, pTab->tnum, pTab->isTemp);
    for(pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext){
      sqliteVdbeAddOp(v, OP_Clear, pIdx->tnum, pTab->isTemp);
    }
  }

  /* The usual case: There is a WHERE clause so we have to scan through
  ** the table an pick which records to delete.
  */
  else{
    /* Begin the database scan
    */
    pWInfo = sqliteWhereBegin(pParse, pTabList, pWhere, 1);
    if( pWInfo==0 ) goto delete_from_cleanup;

    /* Remember the key of every item to be deleted.
    */
    sqliteVdbeAddOp(v, OP_ListWrite, 0, 0);
    if( db->flags & SQLITE_CountRows ){
      sqliteVdbeAddOp(v, OP_AddImm, 1, 0);
    }

    /* End the database scan loop.
    */
    sqliteWhereEnd(pWInfo);

    /* Delete every item whose key was written to the list during the
    ** database scan.  We have to delete items after the scan is complete
    ** because deleting an item can change the scan order.
    */
    base = pParse->nTab;
    sqliteVdbeAddOp(v, OP_ListRewind, 0, 0);
    openOp = pTab->isTemp ? OP_OpenWrAux : OP_OpenWrite;
    sqliteVdbeAddOp(v, openOp, base, pTab->tnum);
    for(i=1, pIdx=pTab->pIndex; pIdx; i++, pIdx=pIdx->pNext){
      sqliteVdbeAddOp(v, openOp, base+i, pIdx->tnum);
    }
    end = sqliteVdbeMakeLabel(v);
    addr = sqliteVdbeAddOp(v, OP_ListRead, 0, end);
    sqliteGenerateRowDelete(v, pTab, base);
    sqliteVdbeAddOp(v, OP_Goto, 0, addr);
    sqliteVdbeResolveLabel(v, end);
    sqliteVdbeAddOp(v, OP_ListReset, 0, 0);
  }
  if( (db->flags & SQLITE_InTrans)==0 ){
    sqliteVdbeAddOp(v, OP_Commit, 0, 0);
  }

  /*
  ** Return the number of rows that were deleted.
  */
  if( db->flags & SQLITE_CountRows ){
    sqliteVdbeAddOp(v, OP_ColumnCount, 1, 0);
    sqliteVdbeAddOp(v, OP_ColumnName, 0, 0);
    sqliteVdbeChangeP3(v, -1, "rows deleted", P3_STATIC);
    sqliteVdbeAddOp(v, OP_Callback, 1, 0);
  }

delete_from_cleanup:
  sqliteIdListDelete(pTabList);
  sqliteExprDelete(pWhere);
  return;
}

/*
** This routine generates VDBE code that causes a single row of a
** single table to be deleted.
**
** The VDBE must be in a particular state when this routine is called.
** These are the requirements:
**
**   1.  A read/write cursor pointing to pTab, the table containing the row
**       to be deleted, must be opened as cursor number "base".
**
**   2.  Read/write cursors for all indices of pTab must be open as
**       cursor number base+i for the i-th index.
**
**   3.  The record number of the row to be deleted must be on the top
**       of the stack.
**
** This routine pops the top of the stack to remove the record number
** and then generates code to remove both the table record and all index
** entries that point to that record.
*/
void sqliteGenerateRowDelete(
  Vdbe *v,           /* Generate code into this VDBE */
  Table *pTab,       /* Table containing the row to be deleted */
  int base           /* Cursor number for the table */
){
  int i;
  Index *pIdx;

  sqliteVdbeAddOp(v, OP_MoveTo, base, 0);
  if( pTab->pIndex ){
    for(i=1, pIdx=pTab->pIndex; pIdx; i++, pIdx=pIdx->pNext){
      int j;
      sqliteVdbeAddOp(v, OP_Recno, base, 0);
      for(j=0; j<pIdx->nColumn; j++){
        int idx = pIdx->aiColumn[j];
        if( idx==pTab->iPKey ){
          sqliteVdbeAddOp(v, OP_Dup, j, 0);
        }else{
          sqliteVdbeAddOp(v, OP_Column, base, idx);
        }
      }
      sqliteVdbeAddOp(v, OP_MakeIdxKey, pIdx->nColumn, 0);
      sqliteVdbeAddOp(v, OP_IdxDelete, base+i, 0);
    }
  }
  sqliteVdbeAddOp(v, OP_Delete, base, 0);
}
