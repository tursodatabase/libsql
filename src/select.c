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
** to handle SELECT statements.
**
** $Id: select.c,v 1.6 2000/06/04 12:58:38 drh Exp $
*/
#include "sqliteInt.h"


/*
** Process a SELECT statement.
*/
void sqliteSelect(
  Parse *pParse,         /* The parser context */
  ExprList *pEList,      /* List of fields to extract.  NULL means "*" */
  IdList *pTabList,      /* List of tables to select from */
  Expr *pWhere,          /* The WHERE clause.  May be NULL */
  ExprList *pOrderBy,    /* The ORDER BY clause.  May be NULL */
  int distinct           /* If true, only output distinct results */
){
  int i, j;
  WhereInfo *pWInfo;
  Vdbe *v;
  int isAgg = 0;         /* True for select lists like "count(*)" */

  if( pParse->nErr>0 ) goto select_cleanup;

  /* Look up every table in the table list.
  */
  for(i=0; i<pTabList->nId; i++){
    pTabList->a[i].pTab = sqliteFindTable(pParse->db, pTabList->a[i].zName);
    if( pTabList->a[i].pTab==0 ){
      sqliteSetString(&pParse->zErrMsg, "no such table: ", 
         pTabList->a[i].zName, 0);
      pParse->nErr++;
      goto select_cleanup;
    }
  }

  /* If the list of fields to retrieve is "*" then replace it with
  ** a list of all fields from all tables.
  */
  if( pEList==0 ){
    for(i=0; i<pTabList->nId; i++){
      Table *pTab = pTabList->a[i].pTab;
      for(j=0; j<pTab->nCol; j++){
        Expr *pExpr = sqliteExpr(TK_FIELD, 0, 0, 0);
        pExpr->iTable = i;
        pExpr->iField = j;
        pEList = sqliteExprListAppend(pEList, pExpr, 0);
      }
    }
  }

  /* Resolve the field names and do a semantics check on all the expressions.
  */
  for(i=0; i<pEList->nExpr; i++){
    if( sqliteExprResolveIds(pParse, pTabList, pEList->a[i].pExpr) ){
      goto select_cleanup;
    }
    if( sqliteExprCheck(pParse, pEList->a[i].pExpr, 1, &pEList->a[i].isAgg) ){
      goto select_cleanup;
    }
  }
  if( pEList->nExpr>0 ){
    isAgg = pEList->a[0].isAgg;
    for(i=1; i<pEList->nExpr; i++){
      if( pEList->a[i].isAgg!=isAgg ){
        sqliteSetString(&pParse->zErrMsg, "some selected items are aggregates "
          "and others are not", 0);
        pParse->nErr++;
        goto select_cleanup;
      }
    }
  }
  if( pWhere ){
    if( sqliteExprResolveIds(pParse, pTabList, pWhere) ){
      goto select_cleanup;
    }
    if( sqliteExprCheck(pParse, pWhere, 0, 0) ){
      goto select_cleanup;
    }
  }
  if( pOrderBy ){
    for(i=0; i<pOrderBy->nExpr; i++){
      if( sqliteExprResolveIds(pParse, pTabList, pOrderBy->a[i].pExpr) ){
        goto select_cleanup;
      }
      if( sqliteExprCheck(pParse, pOrderBy->a[i].pExpr, 0, 0) ){
        goto select_cleanup;
      }
    }
  }

  /* ORDER BY is ignored if this is an aggregate query like count(*)
  ** since only one row will be returned.
  */
  if( isAgg && pOrderBy ){
    sqliteExprListDelete(pOrderBy);
    pOrderBy = 0;
  }

  /* Turn off distinct if this is an aggregate
  */
  if( isAgg ){
    distinct = 0;
  }

  /* Begin generating code.
  */
  v = pParse->pVdbe;
  if( v==0 ){
    v = pParse->pVdbe = sqliteVdbeCreate(pParse->db->pBe);
  }
  if( v==0 ) goto select_cleanup;
  if( pOrderBy ){
    sqliteVdbeAddOp(v, OP_SortOpen, 0, 0, 0, 0);
  }

  /* Identify column names
  */
  sqliteVdbeAddOp(v, OP_ColumnCount, pEList->nExpr, 0, 0, 0);
  for(i=0; i<pEList->nExpr; i++){
    Expr *p;
    if( pEList->a[i].zName ){
      char *zName = pEList->a[i].zName;
      int addr = sqliteVdbeAddOp(v, OP_ColumnName, i, 0, zName, 0);
      if( zName[0]=='\'' || zName[0]=='"' ){
        sqliteVdbeDequoteP3(v, addr);
      }
      continue;
    }
    p = pEList->a[i].pExpr;
    if( p->op!=TK_FIELD ){
      char zName[30];
      sprintf(zName, "field%d", i+1);
      sqliteVdbeAddOp(v, OP_ColumnName, i, 0, zName, 0);
    }else{
      if( pTabList->nId>1 ){
        char *zName = 0;
        Table *pTab = pTabList->a[p->iTable].pTab;
        char *zTab;

        zTab = pTabList->a[p->iTable].zAlias;
        if( zTab==0 ) zTab = pTab->zName;
        sqliteSetString(&zName, zTab, ".", pTab->aCol[p->iField].zName, 0);
        sqliteVdbeAddOp(v, OP_ColumnName, i, 0, zName, 0);
        sqliteFree(zName);
      }else{
        Table *pTab = pTabList->a[0].pTab;
        sqliteVdbeAddOp(v, OP_ColumnName, i, 0, pTab->aCol[p->iField].zName, 0);
      }
    }
  }

  /* Initialize the stack to contain aggregate seed values
  */
  if( isAgg ){
    for(i=0; i<pEList->nExpr; i++){
      Expr *p = pEList->a[i].pExpr;
      switch( sqliteFuncId(&p->token) ){
        case FN_Min:
        case FN_Max: {
          sqliteVdbeAddOp(v, OP_Null, 0, 0, 0, 0);
          break;
        }
        default: {
          sqliteVdbeAddOp(v, OP_Integer, 0, 0, 0, 0);
          break;
        }
      }
    }
  }

  /* Begin the database scan
  */
  if( distinct ){
    distinct = pTabList->nId*2+1;
    sqliteVdbeAddOp(v, OP_Open, distinct, 1, 0, 0);
  }
  pWInfo = sqliteWhereBegin(pParse, pTabList, pWhere, 0);
  if( pWInfo==0 ) goto select_cleanup;

  /* Pull the requested fields.
  */
  if( !isAgg ){
    for(i=0; i<pEList->nExpr; i++){
      sqliteExprCode(pParse, pEList->a[i].pExpr);
    }
  }

  /* If the current result is not distinct, script the remainder
  ** of this processing.
  */
  if( distinct ){
    int isDistinct = sqliteVdbeMakeLabel(v);
    sqliteVdbeAddOp(v, OP_MakeKey, pEList->nExpr, 1, 0, 0);
    sqliteVdbeAddOp(v, OP_Distinct, distinct, isDistinct, 0, 0);
    sqliteVdbeAddOp(v, OP_Pop, pEList->nExpr+1, 0, 0, 0);
    sqliteVdbeAddOp(v, OP_Goto, 0, pWInfo->iContinue, 0, 0);
    sqliteVdbeAddOp(v, OP_String, 0, 0, "", isDistinct);
    sqliteVdbeAddOp(v, OP_Put, distinct, 0, 0, 0);
  }
  
  /* If there is no ORDER BY clause, then we can invoke the callback
  ** right away.  If there is an ORDER BY, then we need to put the
  ** data into an appropriate sorter record.
  */
  if( pOrderBy ){
    char *zSortOrder;
    sqliteVdbeAddOp(v, OP_SortMakeRec, pEList->nExpr, 0, 0, 0);
    zSortOrder = sqliteMalloc( pOrderBy->nExpr + 1 );
    if( zSortOrder==0 ) goto select_cleanup;
    for(i=0; i<pOrderBy->nExpr; i++){
      zSortOrder[i] = pOrderBy->a[i].idx ? '-' : '+';
      sqliteExprCode(pParse, pOrderBy->a[i].pExpr);
    }
    zSortOrder[pOrderBy->nExpr] = 0;
    sqliteVdbeAddOp(v, OP_SortMakeKey, pOrderBy->nExpr, 0, zSortOrder, 0);
    sqliteVdbeAddOp(v, OP_SortPut, 0, 0, 0, 0);
  }else if( isAgg ){
    int n = pEList->nExpr;
    for(i=0; i<n; i++){
      Expr *p = pEList->a[i].pExpr;
      int id = sqliteFuncId(&p->token);
      int op, p1;
      if( n>1 ){
        sqliteVdbeAddOp(v, OP_Pull, n-1, 0, 0, 0);
      }
      if( id!=FN_Count && p->pList && p->pList->nExpr>=1 ){
        sqliteExprCode(pParse, p->pList->a[0].pExpr);
        sqliteVdbeAddOp(v, OP_Concat, 1, 0, 0, 0);
      }
      switch( sqliteFuncId(&p->token) ){
        case FN_Count: op = OP_AddImm; p1 = 1; break;
        case FN_Sum:   op = OP_Add;    p1 = 0; break;
        case FN_Min:   op = OP_Min;    p1 = 1; break;
        case FN_Max:   op = OP_Max;    p1 = 0; break;
      }
      sqliteVdbeAddOp(v, op, p1, 0, 0, 0);
    }
  }else{
    sqliteVdbeAddOp(v, OP_Callback, pEList->nExpr, 0, 0, 0);
  }

  /* End the database scan loop.
  */
  sqliteWhereEnd(pWInfo);

  /* If there is an ORDER BY clause, then we need to sort the results
  ** and send them to the callback one by one.
  */
  if( pOrderBy ){
    int end = sqliteVdbeMakeLabel(v);
    int addr;
    sqliteVdbeAddOp(v, OP_Sort, 0, 0, 0, 0);
    addr = sqliteVdbeAddOp(v, OP_SortNext, 0, end, 0, 0);
    sqliteVdbeAddOp(v, OP_SortCallback, pEList->nExpr, 0, 0, 0);
    sqliteVdbeAddOp(v, OP_Goto, 0, addr, 0, 0);
    sqliteVdbeAddOp(v, OP_Noop, 0, 0, 0, end);
  }

  /* If this is an aggregate, then we need to invoke the callback
  ** exactly once.
  */
  if( isAgg ){
    sqliteVdbeAddOp(v, OP_Callback, pEList->nExpr, 0, 0, 0);
  }

  /* Always execute the following code before exiting, in order to
  ** release resources.
  */
select_cleanup:
  sqliteExprListDelete(pEList);
  sqliteIdListDelete(pTabList);
  sqliteExprDelete(pWhere);
  sqliteExprListDelete(pOrderBy);
  return;
}
