/*
** Copyright (c) 1999, 2000 D. Richard Hipp
**
** This program is free software; you can redistribute it and/or
** modify it under the terms of the GNU General Public
** License as published by the Free Software Foundation; either
** version 2 of the License, or (at your option) any later version.
**
** This program is distributed in the hope that it will be useful,
** but WITHOUT ANY WARRANTY; without even the implied warranty of
** MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
** General Public License for more details.
** 
** You should have received a copy of the GNU General Public
** License along with this library; if not, write to the
** Free Software Foundation, Inc., 59 Temple Place - Suite 330,
** Boston, MA  02111-1307, USA.
**
** Author contact information:
**   drh@hwaci.com
**   http://www.hwaci.com/drh/
**
*************************************************************************
** This file contains C code routines that are called by the parser
** to handle DELETE FROM statements.
**
** $Id: delete.c,v 1.6 2000/06/21 13:59:11 drh Exp $
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

  /* Locate the table which we want to delete.  This table has to be
  ** put in an IdList structure because some of the subroutines we
  ** will be calling are designed to work with multiple tables and expect
  ** an IdList* parameter instead of just a Table* parameger.
  */
  pTabList = sqliteIdListAppend(0, pTableName);
  for(i=0; i<pTabList->nId; i++){
    pTabList->a[i].pTab = sqliteFindTable(pParse->db, pTabList->a[i].zName);
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
  sqliteVdbeAddOp(v, OP_Open, base, 1, pTab->zName, 0);
  for(i=1, pIdx=pTab->pIndex; pIdx; i++, pIdx=pIdx->pNext){
    sqliteVdbeAddOp(v, OP_Open, base+i, 1, pIdx->zName, 0);
  }
  end = sqliteVdbeMakeLabel(v);
  addr = sqliteVdbeAddOp(v, OP_ListRead, 0, end, 0, 0);
  if( pTab->pIndex ){
    sqliteVdbeAddOp(v, OP_Dup, 0, 0, 0, 0);
    sqliteVdbeAddOp(v, OP_Fetch, base, 0, 0, 0);
    for(i=1, pIdx=pTab->pIndex; pIdx; i++, pIdx=pIdx->pNext){
      int j;
      sqliteVdbeAddOp(v, OP_Dup, 0, 0, 0, 0);
      for(j=0; j<pIdx->nColumn; j++){
        sqliteVdbeAddOp(v, OP_Field, base, pIdx->aiColumn[j], 0, 0);
      }
      sqliteVdbeAddOp(v, OP_MakeKey, pIdx->nColumn, 0, 0, 0);
      sqliteVdbeAddOp(v, OP_DeleteIdx, base+i, 0, 0, 0);
    }
  }
  sqliteVdbeAddOp(v, OP_Delete, base, 0, 0, 0);
  sqliteVdbeAddOp(v, OP_Goto, 0, addr, 0, 0);
  sqliteVdbeAddOp(v, OP_ListClose, 0, 0, 0, end);

delete_from_cleanup:
  sqliteIdListDelete(pTabList);
  sqliteExprDelete(pWhere);
  return;
}
