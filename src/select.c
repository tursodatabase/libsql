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
** $Id: select.c,v 1.13 2000/06/06 21:56:08 drh Exp $
*/
#include "sqliteInt.h"

/*
** Allocate a new Select structure and return a pointer to that
** structure.
*/
Select *sqliteSelectNew(
  ExprList *pEList,
  IdList *pSrc,
  Expr *pWhere,
  ExprList *pGroupBy,
  Expr *pHaving,
  ExprList *pOrderBy,
  int isDistinct
){
  Select *pNew;
  pNew = sqliteMalloc( sizeof(*pNew) );
  if( pNew==0 ) return 0;
  pNew->pEList = pEList;
  pNew->pSrc = pSrc;
  pNew->pWhere = pWhere;
  pNew->pGroupBy = pGroupBy;
  pNew->pHaving = pHaving;
  pNew->pOrderBy = pOrderBy;
  pNew->isDistinct = isDistinct;
  pNew->op = TK_SELECT;
  return pNew;
}

/*
** Delete the given Select structure and all of its substructures.
*/
void sqliteSelectDelete(Select *p){
  if( p==0 ) return;
  sqliteExprListDelete(p->pEList);
  sqliteIdListDelete(p->pSrc);
  sqliteExprDelete(p->pWhere);
  sqliteExprListDelete(p->pGroupBy);
  sqliteExprDelete(p->pHaving);
  sqliteExprListDelete(p->pOrderBy);
  sqliteSelectDelete(p->pPrior);
  sqliteFree(p);
}

/*
** Delete the aggregate information from the parse structure.
*/
void sqliteParseInfoReset(Parse *pParse){
  sqliteFree(pParse->aAgg);
  pParse->aAgg = 0;
  pParse->nAgg = 0;
  pParse->iAggCount = -1;
  pParse->useAgg = 0;
}

/*
** This routine generates the code for the inside of the inner loop
** of a SELECT.
**
** The pEList is used to determine the values for each column in the
** result row.  Except  if pEList==NULL, then we just read nField
** elements from the srcTab table.
*/
static int selectInnerLoop(
  Parse *pParse,          /* The parser context */
  ExprList *pEList,       /* List of values being extracted */
  int srcTab,             /* Pull data from this table */
  int nField,             /* Number of fields in the source table */
  ExprList *pOrderBy,     /* If not NULL, sort results using this key */
  int distinct,           /* If >=0, make sure results are distinct */
  int eDest,              /* How to dispose of the results */
  int iParm,              /* An argument to the disposal method */
  int iContinue,          /* Jump here to continue with next row */
  int iBreak              /* Jump here to break out of the inner loop */
){
  Vdbe *v = pParse->pVdbe;
  int i;

  /* Pull the requested fields.
  */
  if( pEList ){
    for(i=0; i<pEList->nExpr; i++){
      sqliteExprCode(pParse, pEList->a[i].pExpr);
    }
    nField = pEList->nExpr;
  }else{
    for(i=0; i<nField; i++){
      sqliteVdbeAddOp(v, OP_Field, srcTab, i, 0, 0);
    }
  }

  /* If the current result is not distinct, skip the rest
  ** of the processing for the current row.
  */
  if( distinct>=0 ){
    int lbl = sqliteVdbeMakeLabel(v);
    sqliteVdbeAddOp(v, OP_MakeKey, pEList->nExpr, 1, 0, 0);
    sqliteVdbeAddOp(v, OP_Distinct, distinct, lbl, 0, 0);
    sqliteVdbeAddOp(v, OP_Pop, pEList->nExpr+1, 0, 0, 0);
    sqliteVdbeAddOp(v, OP_Goto, 0, iContinue, 0, 0);
    sqliteVdbeAddOp(v, OP_String, 0, 0, "", lbl);
    sqliteVdbeAddOp(v, OP_Put, distinct, 0, 0, 0);
  }

  /* If there is an ORDER BY clause, then store the results
  ** in a sorter.
  */
  if( pOrderBy ){
    char *zSortOrder;
    sqliteVdbeAddOp(v, OP_SortMakeRec, pEList->nExpr, 0, 0, 0);
    zSortOrder = sqliteMalloc( pOrderBy->nExpr + 1 );
    if( zSortOrder==0 ) return 1;
    for(i=0; i<pOrderBy->nExpr; i++){
      zSortOrder[i] = pOrderBy->a[i].idx ? '-' : '+';
      sqliteExprCode(pParse, pOrderBy->a[i].pExpr);
    }
    zSortOrder[pOrderBy->nExpr] = 0;
    sqliteVdbeAddOp(v, OP_SortMakeKey, pOrderBy->nExpr, 0, zSortOrder, 0);
    sqliteVdbeAddOp(v, OP_SortPut, 0, 0, 0, 0);
  }else 

  /* In this mode, write each query result to the key of the temporary
  ** table iParm.
  */
  if( eDest==SRT_Union ){
    sqliteVdbeAddOp(v, OP_MakeRecord, nField, 0, 0, 0);
    sqliteVdbeAddOp(v, OP_String, iParm, 0, "", 0);
    sqliteVdbeAddOp(v, OP_Put, iParm, 0, 0, 0);
  }else 

  /* Construct a record from the query result, but instead of
  ** saving that record, use it as a key to delete elements from
  ** the temporary table iParm.
  */
  if( eDest==SRT_Except ){
    assert( pEList->nExpr==1 );
    sqliteVdbeAddOp(v, OP_String, 0, 0, "", 0);
    sqliteVdbeAddOp(v, OP_Put, iParm, 0, 0, 0);
  }else 

  /* If we are creating a set for an "expr IN (SELECT ...)" construct,
  ** then there should be a single item on the stack.  Write this
  ** item into the set table with bogus data.
  */
  if( eDest==SRT_Set ){
    assert( pEList->nExpr==1 );
    sqliteVdbeAddOp(v, OP_String, 0, 0, "", 0);
    sqliteVdbeAddOp(v, OP_Put, iParm, 0, 0, 0);
  }else 


  /* If this is a scalar select that is part of an expression, then
  ** store the results in the appropriate memory cell and break out
  ** of the scan loop.
  */
  if( eDest==SRT_Mem ){
    sqliteVdbeAddOp(v, OP_MemStore, iParm, 0, 0, 0);
    sqliteVdbeAddOp(v, OP_Goto, 0, iBreak, 0, 0);
  }else

  /* If none of the above, send the data to the callback function.
  */
  {
    sqliteVdbeAddOp(v, OP_Callback, nField, 0, 0, 0);
  }
  return 0;
}

/*
** Generate code that will tell the VDBE how many columns there
** are in the result and the name for each column.  This information
** is used to provide "argc" and "azCol[]" values in the callback.
*/
static void generateColumnNames(Vdbe *v, IdList *pTabList, ExprList *pEList){
  int i;
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
    if( p->op!=TK_FIELD || pTabList==0 ){
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
        char *zName = pTab->aCol[p->iField].zName;
        sqliteVdbeAddOp(v, OP_ColumnName, i, 0, zName, 0);
      }
    }
  }
}

/*
** This routine is called to process a query that is really the union
** or intersection of two or more separate queries.
*/
static int multiSelect(Parse *pParse, Select *p, int eDest, int iParm){
  int rc;
  Select *pPrior;
  Vdbe *v;
  int i;

  /* Make sure we have a valid query engine.  If not, create a new one.
  */
  v = pParse->pVdbe;
  if( v==0 ){
    v = pParse->pVdbe = sqliteVdbeCreate(pParse->db->pBe);
  }
  if( v==0 ){
    sqliteSetString(&pParse->zErrMsg, "out of memory", 0);
    pParse->nErr++;
    return 1;
  }

  assert( p->pPrior!=0 );
  pPrior = p->pPrior;
  switch( p->op ){
    case TK_ALL: {
      rc = sqliteSelect(pParse, pPrior, eDest, iParm);
      if( rc ) return rc;
      p->pPrior = 0;
      rc = sqliteSelect(pParse, p, eDest, iParm);
      p->pPrior = pPrior;
      break;
    }
    case TK_EXCEPT:
    case TK_UNION: {
      int unionTab;
      int op;

      if( eDest==SRT_Union ){
        unionTab = iParm;
      }else{
        unionTab = pParse->nTab++;          
        sqliteVdbeAddOp(v, OP_Open, unionTab, 1, 0, 0);
        sqliteVdbeAddOp(v, OP_KeyAsData, unionTab, 1, 0, 0);
      }
      rc = sqliteSelect(pParse, pPrior, SRT_Union, unionTab);
      if( rc ) return rc;
      op = p->op==TK_EXCEPT ? SRT_Except : SRT_Union;
      p->pPrior = 0;
      rc = sqliteSelect(pParse, p, op, unionTab);
      p->pPrior = pPrior;
      if( rc ) return rc;
      if( eDest!=SRT_Union ){
        int iCont, iBreak;
        assert( p->pEList );
        generateColumnNames(v, 0, p->pEList);
        iBreak = sqliteVdbeMakeLabel(v);
        iCont = sqliteVdbeAddOp(v, OP_Next, unionTab, iBreak, 0, 0);
        rc = selectInnerLoop(pParse, 0, unionTab, p->pEList->nExpr,
                             0, -1, eDest, iParm, 
                             iCont, iBreak);
        if( rc ) return 1;
        sqliteVdbeAddOp(v, OP_Goto, 0, iCont, 0, 0);
        sqliteVdbeAddOp(v, OP_Close, unionTab, 0, 0, iBreak);
      }
      break;
    }
    case TK_INTERSECT: {
      int tab1, tab2;
      Select *pPrior;
      int iCont, iBreak;

      tab1 = pParse->nTab++;
      tab2 = pParse->nTab++;
      sqliteVdbeAddOp(v, OP_Open, tab1, 1, 0, 0);
      sqliteVdbeAddOp(v, OP_KeyAsData, tab1, 1, 0, 0);
      rc = sqliteSelect(pParse, pPrior, SRT_Union, tab1);
      if( rc ) return rc;
      sqliteVdbeAddOp(v, OP_Open, tab2, 1, 0, 0);
      sqliteVdbeAddOp(v, OP_KeyAsData, tab2, 1, 0, 0);
      p->pPrior = 0;
      rc = sqliteSelect(pParse, p, SRT_Union, tab2);
      p->pPrior = pPrior;
      if( rc ) return rc;
      assert( p->pEList );
      generateColumnNames(v, 0, p->pEList);
      iBreak = sqliteVdbeMakeLabel(v);
      iCont = sqliteVdbeAddOp(v, OP_Next, tab1, iBreak, 0, 0);
      sqliteVdbeAddOp(v, OP_Key, tab1, 0, 0, 0);
      sqliteVdbeAddOp(v, OP_NotFound, tab2, iCont, 0, 0);
      rc = selectInnerLoop(pParse, 0, tab1, p->pEList->nExpr,
                             0, -1, eDest, iParm, 
                             iCont, iBreak);
      if( rc ) return 1;
      sqliteVdbeAddOp(v, OP_Goto, 0, iCont, 0, 0);
      sqliteVdbeAddOp(v, OP_Close, tab2, 0, 0, iBreak);
      sqliteVdbeAddOp(v, OP_Close, tab1, 0, 0, 0);
      break;
    }
  }
  assert( p->pEList && pPrior->pEList );
  if( p->pEList->nExpr!=pPrior->pEList->nExpr ){
    sqliteSetString(&pParse->zErrMsg, "SELECTs have different numbers "
       "of columns and therefore cannot be joined", 0);
    pParse->nErr++;
    return 1;
  }
  return 0;
}

/*
** Generate code for the given SELECT statement.
**
** The results are distributed in various ways depending on the
** value of eDest and iParm.
**
**     eDest Value       Result
**     ------------    -------------------------------------------
**     SRT_Callback    Invoke the callback for each row of the result.
**
**     SRT_Mem         Store first result in memory cell iParm
**
**     SRT_Set         Store results as keys of a table with cursor iParm
**
**     SRT_Union       Store results as a key in a temporary table iParm
**
**     SRT_Except      Remove results form the temporary talbe iParm.
**
** This routine returns the number of errors.  If any errors are
** encountered, then an appropriate error message is left in
** pParse->zErrMsg.
**
** This routine does NOT free the Select structure passed in.  The
** calling function needs to do that.
*/
int sqliteSelect(
  Parse *pParse,         /* The parser context */
  Select *p,             /* The SELECT statement being coded. */
  int eDest,             /* One of: SRT_Callback Mem Set Union Except */
  int iParm              /* Save result in this memory location, if >=0 */
){
  int i, j;
  WhereInfo *pWInfo;
  Vdbe *v;
  int isAgg = 0;         /* True for select lists like "count(*)" */
  ExprList *pEList;      /* List of fields to extract.  NULL means "*" */
  IdList *pTabList;      /* List of tables to select from */
  Expr *pWhere;          /* The WHERE clause.  May be NULL */
  ExprList *pOrderBy;    /* The ORDER BY clause.  May be NULL */
  ExprList *pGroupBy;    /* The GROUP BY clause.  May be NULL */
  Expr *pHaving;         /* The HAVING clause.  May be NULL */
  int isDistinct;        /* True if the DISTINCT keyword is present */
  int distinct;          /* Table to use for the distinct set */

  /* If there is are a sequence of queries, do the earlier ones first.
  */
  if( p->pPrior ){
    return multiSelect(pParse, p, eDest, iParm);
  }

  /* Make local copies of the parameters for this query.
  */
  pEList = p->pEList;
  pTabList = p->pSrc;
  pWhere = p->pWhere;
  pOrderBy = p->pOrderBy;
  pGroupBy = p->pGroupBy;
  pHaving = p->pHaving;
  isDistinct = p->isDistinct;

  /* 
  ** Do not even attempt to generate any code if we have already seen
  ** errors before this routine starts.
  */
  if( pParse->nErr>0 ) return 0;
  sqliteParseInfoReset(pParse);

  /* Look up every table in the table list.
  */
  for(i=0; i<pTabList->nId; i++){
    pTabList->a[i].pTab = sqliteFindTable(pParse->db, pTabList->a[i].zName);
    if( pTabList->a[i].pTab==0 ){
      sqliteSetString(&pParse->zErrMsg, "no such table: ", 
         pTabList->a[i].zName, 0);
      pParse->nErr++;
      return 1;
    }
  }

  /* Allocate a temporary table to use for the DISTINCT set, if
  ** necessary.  This must be done early to allocate the cursor before
  ** any calls to sqliteExprResolveIds().
  */
  if( isDistinct ){
    distinct = pParse->nTab++;
  }else{
    distinct = -1;
  }

  /* If the list of fields to retrieve is "*" then replace it with
  ** a list of all fields from all tables.
  */
  if( pEList==0 ){
    for(i=0; i<pTabList->nId; i++){
      Table *pTab = pTabList->a[i].pTab;
      for(j=0; j<pTab->nCol; j++){
        Expr *pExpr = sqliteExpr(TK_FIELD, 0, 0, 0);
        pExpr->iTable = i + pParse->nTab;
        pExpr->iField = j;
        p->pEList = pEList = sqliteExprListAppend(pEList, pExpr, 0);
      }
    }
  }

  /* If writing to memory or generating a set
  ** only a single column may be output.
  */
  if( (eDest==SRT_Mem || eDest==SRT_Set) && pEList->nExpr>1 ){
    sqliteSetString(&pParse->zErrMsg, "only a single result allowed for "
       "a SELECT that is part of an expression", 0);
    pParse->nErr++;
    return 1;
  }

  /* ORDER BY is ignored if we are not sending the result to a callback.
  */
  if( eDest!=SRT_Callback ){
    pOrderBy = 0;
  }

  /* Allocate cursors for "expr IN (SELECT ...)" constructs.
  */
  for(i=0; i<pEList->nExpr; i++){
    sqliteExprResolveInSelect(pParse, pEList->a[i].pExpr);
  }
  if( pWhere ) sqliteExprResolveInSelect(pParse, pWhere);
  if( pOrderBy ){
    for(i=0; i<pOrderBy->nExpr; i++){
      sqliteExprResolveInSelect(pParse, pOrderBy->a[i].pExpr);
    }
  }
  if( pGroupBy ){
    for(i=0; i<pGroupBy->nExpr; i++){
      sqliteExprResolveInSelect(pParse, pGroupBy->a[i].pExpr);
    }
  }
  if( pHaving ) sqliteExprResolveInSelect(pParse, pHaving);

  /* Resolve the field names and do a semantics check on all the expressions.
  */
  for(i=0; i<pEList->nExpr; i++){
    if( sqliteExprResolveIds(pParse, pTabList, pEList->a[i].pExpr) ){
      return 1;
    }
    if( sqliteExprCheck(pParse, pEList->a[i].pExpr, 1, &isAgg) ){
      return 1;
    }
  }
  if( pWhere ){
    if( sqliteExprResolveIds(pParse, pTabList, pWhere) ){
      return 1;
    }
    if( sqliteExprCheck(pParse, pWhere, 0, 0) ){
      return 1;
    }
  }
  if( pOrderBy ){
    for(i=0; i<pOrderBy->nExpr; i++){
      Expr *pE = pOrderBy->a[i].pExpr;
      if( sqliteExprResolveIds(pParse, pTabList, pE) ){
        return 1;
      }
      if( sqliteExprCheck(pParse, pE, isAgg, 0) ){
        return 1;
      }
    }
  }
  if( pGroupBy ){
    for(i=0; i<pGroupBy->nExpr; i++){
      Expr *pE = pGroupBy->a[i].pExpr;
      if( sqliteExprResolveIds(pParse, pTabList, pE) ){
        return 1;
      }
      if( sqliteExprCheck(pParse, pE, isAgg, 0) ){
        return 1;
      }
    }
  }
  if( pHaving ){
    if( pGroupBy==0 ){
      sqliteSetString(&pParse->zErrMsg, "a GROUP BY clause is required "
         "before HAVING", 0);
      pParse->nErr++;
      return 1;
    }
    if( sqliteExprResolveIds(pParse, pTabList, pHaving) ){
      return 1;
    }
    if( sqliteExprCheck(pParse, pHaving, isAgg, 0) ){
      return 1;
    }
  }

  /* Do an analysis of aggregate expressions.
  */
  if( isAgg ){
    for(i=0; i<pEList->nExpr; i++){
      if( sqliteExprAnalyzeAggregates(pParse, pEList->a[i].pExpr) ){
        return 1;
      }
    }
    if( pGroupBy ){
      for(i=0; i<pGroupBy->nExpr; i++){
        if( sqliteExprAnalyzeAggregates(pParse, pGroupBy->a[i].pExpr) ){
          return 1;
        }
      }
    }
    if( pHaving && sqliteExprAnalyzeAggregates(pParse, pHaving) ){
      return 1;
    }
  }

  /* Begin generating code.
  */
  v = pParse->pVdbe;
  if( v==0 ){
    v = pParse->pVdbe = sqliteVdbeCreate(pParse->db->pBe);
  }
  if( v==0 ){
    sqliteSetString(&pParse->zErrMsg, "out of memory", 0);
    pParse->nErr++;
    return 1;
  }
  if( pOrderBy ){
    sqliteVdbeAddOp(v, OP_SortOpen, 0, 0, 0, 0);
  }

  /* Identify column names if we will be using in the callback.  This
  ** step is skipped if the output is going to a table or a memory cell.
  */
  if( eDest==SRT_Callback ){
    generateColumnNames(v, pTabList, pEList);
  }

  /* Reset the aggregator
  */
  if( isAgg ){
    sqliteVdbeAddOp(v, OP_AggReset, 0, pParse->nAgg, 0, 0);
  }

  /* Initialize the memory cell to NULL
  */
  if( eDest==SRT_Mem ){
    sqliteVdbeAddOp(v, OP_Null, 0, 0, 0, 0);
    sqliteVdbeAddOp(v, OP_MemStore, iParm, 0, 0, 0);
  }

  /* Begin the database scan
  */
  if( isDistinct ){
    sqliteVdbeAddOp(v, OP_Open, distinct, 1, 0, 0);
  }
  pWInfo = sqliteWhereBegin(pParse, pTabList, pWhere, 0);
  if( pWInfo==0 ) return 1;

  /* Use the standard inner loop if we are not dealing with
  ** aggregates
  */
  if( !isAgg ){
    if( selectInnerLoop(pParse, pEList, 0, 0, pOrderBy, distinct, eDest, iParm,
                    pWInfo->iContinue, pWInfo->iBreak) ){
       return 1;
    }
  }

  /* If we are dealing with aggregates, then to the special aggregate
  ** processing.  
  */
  else{
    int doFocus;
    if( pGroupBy ){
      for(i=0; i<pGroupBy->nExpr; i++){
        sqliteExprCode(pParse, pGroupBy->a[i].pExpr);
      }
      sqliteVdbeAddOp(v, OP_MakeKey, pGroupBy->nExpr, 0, 0, 0);
      doFocus = 1;
    }else{
      doFocus = 0;
      for(i=0; i<pParse->nAgg; i++){
        if( !pParse->aAgg[i].isAgg ){
          doFocus = 1;
          break;
        }
      }
      if( doFocus ){
        sqliteVdbeAddOp(v, OP_String, 0, 0, "", 0);
      }
    }
    if( doFocus ){
      int lbl1 = sqliteVdbeMakeLabel(v);
      sqliteVdbeAddOp(v, OP_AggFocus, 0, lbl1, 0, 0);
      for(i=0; i<pParse->nAgg; i++){
        if( pParse->aAgg[i].isAgg ) continue;
        sqliteExprCode(pParse, pParse->aAgg[i].pExpr);
        sqliteVdbeAddOp(v, OP_AggSet, 0, i, 0, 0);
      }
      sqliteVdbeResolveLabel(v, lbl1);
    }
    for(i=0; i<pParse->nAgg; i++){
      Expr *pE;
      int op;
      if( !pParse->aAgg[i].isAgg ) continue;
      pE = pParse->aAgg[i].pExpr;
      if( pE==0 ){
        sqliteVdbeAddOp(v, OP_AggIncr, 1, i, 0, 0);
        continue;
      }
      assert( pE->op==TK_AGG_FUNCTION );
      assert( pE->pList!=0 && pE->pList->nExpr==1 );
      sqliteExprCode(pParse, pE->pList->a[0].pExpr);
      sqliteVdbeAddOp(v, OP_AggGet, 0, i, 0, 0);
      switch( pE->iField ){
        case FN_Min:  op = OP_Min;   break;
        case FN_Max:  op = OP_Max;   break;
        case FN_Avg:  op = OP_Add;   break;
        case FN_Sum:  op = OP_Add;   break;
      }
      sqliteVdbeAddOp(v, op, 0, 0, 0, 0);
      sqliteVdbeAddOp(v, OP_AggSet, 0, i, 0, 0);
    }
  }


  /* End the database scan loop.
  */
  sqliteWhereEnd(pWInfo);

  /* If we are processing aggregates, we need to set up a second loop
  ** over all of the aggregate values and process them.
  */
  if( isAgg ){
    int endagg = sqliteVdbeMakeLabel(v);
    int startagg;
    startagg = sqliteVdbeAddOp(v, OP_AggNext, 0, endagg, 0, 0);
    pParse->useAgg = 1;
    if( pHaving ){
      sqliteExprIfFalse(pParse, pHaving, startagg);
    }
    if( selectInnerLoop(pParse, pEList, 0, 0, pOrderBy, distinct, eDest, iParm,
                    startagg, endagg) ){
      return 1;
    }
    sqliteVdbeAddOp(v, OP_Goto, 0, startagg, 0, 0);
    sqliteVdbeAddOp(v, OP_Noop, 0, 0, 0, endagg);
    pParse->useAgg = 0;
  }

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
    sqliteVdbeAddOp(v, OP_SortClose, 0, 0, 0, end);
  }
  return 0;
}
