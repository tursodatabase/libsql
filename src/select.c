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
** to handle SELECT statements in SQLite.
**
** $Id: select.c,v 1.47 2001/11/06 14:10:42 drh Exp $
*/
#include "sqliteInt.h"

/*
** Allocate a new Select structure and return a pointer to that
** structure.
*/
Select *sqliteSelectNew(
  ExprList *pEList,     /* which columns to include in the result */
  IdList *pSrc,         /* the FROM clause -- which tables to scan */
  Expr *pWhere,         /* the WHERE clause */
  ExprList *pGroupBy,   /* the GROUP BY clause */
  Expr *pHaving,        /* the HAVING clause */
  ExprList *pOrderBy,   /* the ORDER BY clause */
  int isDistinct,       /* true if the DISTINCT keyword is present */
  int nLimit,           /* LIMIT value.  -1 means not used */
  int nOffset           /* OFFSET value.  -1 means not used */
){
  Select *pNew;
  pNew = sqliteMalloc( sizeof(*pNew) );
  if( pNew==0 ){
    sqliteExprListDelete(pEList);
    sqliteIdListDelete(pSrc);
    sqliteExprDelete(pWhere);
    sqliteExprListDelete(pGroupBy);
    sqliteExprDelete(pHaving);
    sqliteExprListDelete(pOrderBy);
  }else{
    pNew->pEList = pEList;
    pNew->pSrc = pSrc;
    pNew->pWhere = pWhere;
    pNew->pGroupBy = pGroupBy;
    pNew->pHaving = pHaving;
    pNew->pOrderBy = pOrderBy;
    pNew->isDistinct = isDistinct;
    pNew->op = TK_SELECT;
    pNew->nLimit = nLimit;
    pNew->nOffset = nOffset;
  }
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
** result row.  Except  if pEList==NULL, then we just read nColumn
** elements from the srcTab table.
*/
static int selectInnerLoop(
  Parse *pParse,          /* The parser context */
  ExprList *pEList,       /* List of values being extracted */
  int srcTab,             /* Pull data from this table */
  int nColumn,            /* Number of columns in the source table */
  ExprList *pOrderBy,     /* If not NULL, sort results using this key */
  int distinct,           /* If >=0, make sure results are distinct */
  int eDest,              /* How to dispose of the results */
  int iParm,              /* An argument to the disposal method */
  int iContinue,          /* Jump here to continue with next row */
  int iBreak              /* Jump here to break out of the inner loop */
){
  Vdbe *v = pParse->pVdbe;
  int i;
  if( v==0 ) return 0;

  /* Pull the requested columns.
  */
  if( pEList ){
    for(i=0; i<pEList->nExpr; i++){
      sqliteExprCode(pParse, pEList->a[i].pExpr);
    }
    nColumn = pEList->nExpr;
  }else{
    for(i=0; i<nColumn; i++){
      sqliteVdbeAddOp(v, OP_Column, srcTab, i);
    }
  }

  /* If the DISTINCT keyword was present on the SELECT statement
  ** and this row has been seen before, then do not make this row
  ** part of the result.
  */
  if( distinct>=0 ){
    int lbl = sqliteVdbeMakeLabel(v);
    sqliteVdbeAddOp(v, OP_MakeKey, pEList->nExpr, 1);
    sqliteVdbeAddOp(v, OP_Distinct, distinct, lbl);
    sqliteVdbeAddOp(v, OP_Pop, pEList->nExpr+1, 0);
    sqliteVdbeAddOp(v, OP_Goto, 0, iContinue);
    sqliteVdbeResolveLabel(v, lbl);
    sqliteVdbeAddOp(v, OP_String, 0, 0);
    sqliteVdbeChangeP3(v, -1, "", P3_STATIC);
    sqliteVdbeAddOp(v, OP_Put, distinct, 0);
  }

  /* If there is an ORDER BY clause, then store the results
  ** in a sorter.
  */
  if( pOrderBy ){
    char *zSortOrder;
    sqliteVdbeAddOp(v, OP_SortMakeRec, nColumn, 0);
    zSortOrder = sqliteMalloc( pOrderBy->nExpr + 1 );
    if( zSortOrder==0 ) return 1;
    for(i=0; i<pOrderBy->nExpr; i++){
      zSortOrder[i] = pOrderBy->a[i].sortOrder ? '-' : '+';
      sqliteExprCode(pParse, pOrderBy->a[i].pExpr);
    }
    zSortOrder[pOrderBy->nExpr] = 0;
    sqliteVdbeAddOp(v, OP_SortMakeKey, pOrderBy->nExpr, 0);
    sqliteVdbeChangeP3(v, -1, zSortOrder, strlen(zSortOrder));
    sqliteFree(zSortOrder);
    sqliteVdbeAddOp(v, OP_SortPut, 0, 0);
  }else 

  /* In this mode, write each query result to the key of the temporary
  ** table iParm.
  */
  if( eDest==SRT_Union ){
    sqliteVdbeAddOp(v, OP_MakeRecord, nColumn, 0);
    sqliteVdbeAddOp(v, OP_String, iParm, 0);
    sqliteVdbeChangeP3(v, -1, "", P3_STATIC);
    sqliteVdbeAddOp(v, OP_Put, iParm, 0);
  }else 

  /* Store the result as data using a unique key.
  */
  if( eDest==SRT_Table ){
    sqliteVdbeAddOp(v, OP_MakeRecord, nColumn, 0);
    sqliteVdbeAddOp(v, OP_NewRecno, iParm, 0);
    sqliteVdbeAddOp(v, OP_Pull, 1, 0);
    sqliteVdbeAddOp(v, OP_Put, iParm, 0);
  }else 

  /* Construct a record from the query result, but instead of
  ** saving that record, use it as a key to delete elements from
  ** the temporary table iParm.
  */
  if( eDest==SRT_Except ){
    int addr = sqliteVdbeAddOp(v, OP_MakeRecord, nColumn, 0);
    sqliteVdbeAddOp(v, OP_NotFound, iParm, addr+3);
    sqliteVdbeAddOp(v, OP_Delete, iParm, 0);
  }else 

  /* If we are creating a set for an "expr IN (SELECT ...)" construct,
  ** then there should be a single item on the stack.  Write this
  ** item into the set table with bogus data.
  */
  if( eDest==SRT_Set ){
    assert( nColumn==1 );
    sqliteVdbeAddOp(v, OP_String, 0, 0);
    sqliteVdbeChangeP3(v, -1, "", P3_STATIC);
    sqliteVdbeAddOp(v, OP_Put, iParm, 0);
  }else 


  /* If this is a scalar select that is part of an expression, then
  ** store the results in the appropriate memory cell and break out
  ** of the scan loop.
  */
  if( eDest==SRT_Mem ){
    assert( nColumn==1 );
    sqliteVdbeAddOp(v, OP_MemStore, iParm, 0);
    sqliteVdbeAddOp(v, OP_Goto, 0, iBreak);
  }else

  /* If none of the above, send the data to the callback function.
  */
  {
    sqliteVdbeAddOp(v, OP_Callback, nColumn, iBreak);
  }
  return 0;
}

/*
** If the inner loop was generated using a non-null pOrderBy argument,
** then the results were placed in a sorter.  After the loop is terminated
** we need to run the sorter and output the results.  The following
** routine generates the code needed to do that.
*/
static void generateSortTail(Vdbe *v, int nColumn){
  int end = sqliteVdbeMakeLabel(v);
  int addr;
  sqliteVdbeAddOp(v, OP_Sort, 0, 0);
  addr = sqliteVdbeAddOp(v, OP_SortNext, 0, end);
  sqliteVdbeAddOp(v, OP_SortCallback, nColumn, end);
  sqliteVdbeAddOp(v, OP_Goto, 0, addr);
  sqliteVdbeResolveLabel(v, end);
  sqliteVdbeAddOp(v, OP_SortReset, 0, 0);
}

/*
** Generate code that will tell the VDBE how many columns there
** are in the result and the name for each column.  This information
** is used to provide "argc" and "azCol[]" values in the callback.
*/
static 
void generateColumnNames(Parse *pParse, IdList *pTabList, ExprList *pEList){
  Vdbe *v = pParse->pVdbe;
  int i;
  if( pParse->colNamesSet || v==0 || sqlite_malloc_failed ) return;
  pParse->colNamesSet = 1;
  sqliteVdbeAddOp(v, OP_ColumnCount, pEList->nExpr, 0);
  for(i=0; i<pEList->nExpr; i++){
    Expr *p;
    int showFullNames;
    if( pEList->a[i].zName ){
      char *zName = pEList->a[i].zName;
      sqliteVdbeAddOp(v, OP_ColumnName, i, 0);
      sqliteVdbeChangeP3(v, -1, zName, strlen(zName));
      continue;
    }
    p = pEList->a[i].pExpr;
    if( p==0 ) continue;
    showFullNames = (pParse->db->flags & SQLITE_FullColNames)!=0;
    if( p->span.z && p->span.z[0] && !showFullNames ){
      int addr = sqliteVdbeAddOp(v,OP_ColumnName, i, 0);
      sqliteVdbeChangeP3(v, -1, p->span.z, p->span.n);
      sqliteVdbeCompressSpace(v, addr);
    }else if( p->op==TK_COLUMN && pTabList ){
      if( pTabList->nId>1 || showFullNames ){
        char *zName = 0;
        Table *pTab = pTabList->a[p->iTable - pParse->nTab].pTab;
        char *zTab;
 
        zTab = pTabList->a[p->iTable - pParse->nTab].zAlias;
        if( showFullNames || zTab==0 ) zTab = pTab->zName;
        sqliteSetString(&zName, zTab, ".", pTab->aCol[p->iColumn].zName, 0);
        sqliteVdbeAddOp(v, OP_ColumnName, i, 0);
        sqliteVdbeChangeP3(v, -1, zName, strlen(zName));
        sqliteFree(zName);
      }else{
        Table *pTab = pTabList->a[0].pTab;
        char *zName = pTab->aCol[p->iColumn].zName;
        sqliteVdbeAddOp(v, OP_ColumnName, i, 0);
        sqliteVdbeChangeP3(v, -1, zName, P3_STATIC);
      }
    }else if( p->span.z && p->span.z[0] ){
      int addr = sqliteVdbeAddOp(v,OP_ColumnName, i, 0);
      sqliteVdbeChangeP3(v, -1, p->span.z, p->span.n);
      sqliteVdbeCompressSpace(v, addr);
    }else{
      char zName[30];
      assert( p->op!=TK_COLUMN || pTabList==0 );
      sprintf(zName, "column%d", i+1);
      sqliteVdbeAddOp(v, OP_ColumnName, i, 0);
      sqliteVdbeChangeP3(v, -1, zName, strlen(zName));
    }
  }
}

/*
** Name of the connection operator, used for error messages.
*/
static const char *selectOpName(int id){
  char *z;
  switch( id ){
    case TK_ALL:       z = "UNION ALL";   break;
    case TK_INTERSECT: z = "INTERSECT";   break;
    case TK_EXCEPT:    z = "EXCEPT";      break;
    default:           z = "UNION";       break;
  }
  return z;
}

/*
** For the given SELECT statement, do two things.
**
**    (1)  Fill in the pTabList->a[].pTab fields in the IdList that 
**         defines the set of tables that should be scanned.
**
**    (2)  If the columns to be extracted variable (pEList) is NULL
**         (meaning that a "*" was used in the SQL statement) then
**         create a fake pEList containing the names of all columns
**         of all tables.
**
** Return 0 on success.  If there are problems, leave an error message
** in pParse and return non-zero.
*/
static int fillInColumnList(Parse *pParse, Select *p){
  int i, j;
  IdList *pTabList;
  ExprList *pEList;

  if( p==0 || p->pSrc==0 ) return 1;
  pTabList = p->pSrc;
  pEList = p->pEList;

  /* Look up every table in the table list.
  */
  for(i=0; i<pTabList->nId; i++){
    if( pTabList->a[i].pTab ){
      /* This routine has run before!  No need to continue */
      return 0;
    }
    if( pTabList->a[i].zName==0 ){
      /* No table name is given.  Instead, there is a (SELECT ...) statement
      ** the results of which should be used in place of the table.  The
      ** was this is implemented is that the (SELECT ...) writes its results
      ** into a temporary table which is then scanned like any other table.
      */
      sqliteSetString(&pParse->zErrMsg, 
          "(SELECT...) in a FROM clause is not yet implemented.", 0);
      pParse->nErr++;
      return 1;
    }
    pTabList->a[i].pTab = sqliteFindTable(pParse->db, pTabList->a[i].zName);
    if( pTabList->a[i].pTab==0 ){
      sqliteSetString(&pParse->zErrMsg, "no such table: ", 
         pTabList->a[i].zName, 0);
      pParse->nErr++;
      return 1;
    }
  }

  /* If the list of columns to retrieve is "*" then replace it with
  ** a list of all columns from all tables.
  */
  if( pEList==0 ){
    for(i=0; i<pTabList->nId; i++){
      Table *pTab = pTabList->a[i].pTab;
      for(j=0; j<pTab->nCol; j++){
        Expr *pExpr = sqliteExpr(TK_DOT, 0, 0, 0);
        if( pExpr==0 ) break;
        pExpr->pLeft = sqliteExpr(TK_ID, 0, 0, 0);
        if( pExpr->pLeft==0 ){ sqliteExprDelete(pExpr); break; }
        if( pTabList->a[i].zAlias && pTabList->a[i].zAlias[0] ){
          pExpr->pLeft->token.z = pTabList->a[i].zAlias;
          pExpr->pLeft->token.n = strlen(pTabList->a[i].zAlias);
        }else{
          pExpr->pLeft->token.z = pTab->zName;
          pExpr->pLeft->token.n = strlen(pTab->zName);
        }
        pExpr->pRight = sqliteExpr(TK_ID, 0, 0, 0);
        if( pExpr->pRight==0 ){ sqliteExprDelete(pExpr); break; }
        pExpr->pRight->token.z = pTab->aCol[j].zName;
        pExpr->pRight->token.n = strlen(pTab->aCol[j].zName);
        pExpr->span.z = "";
        pExpr->span.n = 0;
        pEList = sqliteExprListAppend(pEList, pExpr, 0);
      }
    }
    p->pEList = pEList;
  }
  return 0;
}

/*
** This routine associates entries in an ORDER BY expression list with
** columns in a result.  For each ORDER BY expression, the opcode of
** the top-level node is changed to TK_COLUMN and the iColumn value of
** the top-level node is filled in with column number and the iTable
** value of the top-level node is filled with iTable parameter.
**
** If there are prior SELECT clauses, they are processed first.  A match
** in an earlier SELECT takes precedence over a later SELECT.
**
** Any entry that does not match is flagged as an error.  The number
** of errors is returned.
*/
static int matchOrderbyToColumn(
  Parse *pParse,          /* A place to leave error messages */
  Select *pSelect,        /* Match to result columns of this SELECT */
  ExprList *pOrderBy,     /* The ORDER BY values to match against columns */
  int iTable,             /* Insert this this value in iTable */
  int mustComplete        /* If TRUE all ORDER BYs must match */
){
  int nErr = 0;
  int i, j;
  ExprList *pEList;

  if( pSelect==0 || pOrderBy==0 ) return 1;
  if( mustComplete ){
    for(i=0; i<pOrderBy->nExpr; i++){ pOrderBy->a[i].done = 0; }
  }
  if( fillInColumnList(pParse, pSelect) ){
    return 1;
  }
  if( pSelect->pPrior ){
    if( matchOrderbyToColumn(pParse, pSelect->pPrior, pOrderBy, iTable, 0) ){
      return 1;
    }
  }
  pEList = pSelect->pEList;
  for(i=0; i<pOrderBy->nExpr; i++){
    Expr *pE = pOrderBy->a[i].pExpr;
    int match = 0;
    if( pOrderBy->a[i].done ) continue;
    for(j=0; j<pEList->nExpr; j++){
      if( pEList->a[j].zName && (pE->op==TK_ID || pE->op==TK_STRING) ){
        char *zName = pEList->a[j].zName;
        char *zLabel = sqliteStrNDup(pE->token.z, pE->token.n);
        sqliteDequote(zLabel);
        if( sqliteStrICmp(zName, zLabel)==0 ){ 
          match = 1; 
        }
        sqliteFree(zLabel);
      }
      if( match==0 && sqliteExprCompare(pE, pEList->a[j].pExpr) ){
        match = 1;
      }
      if( match ){
        pE->op = TK_COLUMN;
        pE->iColumn = j;
        pE->iTable = iTable;
        pOrderBy->a[i].done = 1;
        break;
      }
    }
    if( !match && mustComplete ){
      char zBuf[30];
      sprintf(zBuf,"%d",i+1);
      sqliteSetString(&pParse->zErrMsg, "ORDER BY term number ", zBuf, 
        " does not match any result column", 0);
      pParse->nErr++;
      nErr++;
      break;
    }
  }
  return nErr;  
}

/*
** Get a VDBE for the given parser context.  Create a new one if necessary.
** If an error occurs, return NULL and leave a message in pParse.
*/
Vdbe *sqliteGetVdbe(Parse *pParse){
  Vdbe *v = pParse->pVdbe;
  if( v==0 ){
    v = pParse->pVdbe = sqliteVdbeCreate(pParse->db);
  }
  return v;
}
    

/*
** This routine is called to process a query that is really the union
** or intersection of two or more separate queries.
*/
static int multiSelect(Parse *pParse, Select *p, int eDest, int iParm){
  int rc;             /* Success code from a subroutine */
  Select *pPrior;     /* Another SELECT immediately to our left */
  Vdbe *v;            /* Generate code to this VDBE */
  int base;           /* Baseline value for pParse->nTab */

  /* Make sure there is no ORDER BY clause on prior SELECTs.  Only the 
  ** last SELECT in the series may have an ORDER BY.
  */
  if( p==0 || p->pPrior==0 ) return 1;
  pPrior = p->pPrior;
  if( pPrior->pOrderBy ){
    sqliteSetString(&pParse->zErrMsg,"ORDER BY clause should come after ",
      selectOpName(p->op), " not before", 0);
    pParse->nErr++;
    return 1;
  }

  /* Make sure we have a valid query engine.  If not, create a new one.
  */
  v = sqliteGetVdbe(pParse);
  if( v==0 ) return 1;

  /* Process the UNION or INTERSECTION
  */
  base = pParse->nTab;
  switch( p->op ){
    case TK_ALL:
    case TK_EXCEPT:
    case TK_UNION: {
      int unionTab;    /* Cursor number of the temporary table holding result */
      int op;          /* One of the SRT_ operations to apply to self */
      int priorOp;     /* The SRT_ operation to apply to prior selects */

      priorOp = p->op==TK_ALL ? SRT_Table : SRT_Union;
      if( eDest==priorOp ){
        /* We can reuse a temporary table generated by a SELECT to our
        ** right.  This also means we are not the right-most select and so
        ** we cannot have an ORDER BY clause
        */
        unionTab = iParm;
        assert( p->pOrderBy==0 );
      }else{
        /* We will need to create our own temporary table to hold the
        ** intermediate results.
        */
        unionTab = pParse->nTab++;
        if( p->pOrderBy 
        && matchOrderbyToColumn(pParse, p, p->pOrderBy, unionTab, 1) ){
          return 1;
        }
        if( p->op!=TK_ALL ){
          sqliteVdbeAddOp(v, OP_OpenTemp, unionTab, 0);
          sqliteVdbeAddOp(v, OP_KeyAsData, unionTab, 1);
        }else{
          sqliteVdbeAddOp(v, OP_OpenTemp, unionTab, 0);
        }
      }

      /* Code the SELECT statements to our left
      */
      rc = sqliteSelect(pParse, pPrior, priorOp, unionTab);
      if( rc ) return rc;

      /* Code the current SELECT statement
      */
      switch( p->op ){
         case TK_EXCEPT:  op = SRT_Except;   break;
         case TK_UNION:   op = SRT_Union;    break;
         case TK_ALL:     op = SRT_Table;    break;
      }
      p->pPrior = 0;
      rc = sqliteSelect(pParse, p, op, unionTab);
      p->pPrior = pPrior;
      if( rc ) return rc;

      /* Convert the data in the temporary table into whatever form
      ** it is that we currently need.
      */      
      if( eDest!=priorOp ){
        int iCont, iBreak;
        assert( p->pEList );
        generateColumnNames(pParse, 0, p->pEList);
        sqliteVdbeAddOp(v, OP_Rewind, unionTab, 0);
        iBreak = sqliteVdbeMakeLabel(v);
        iCont = sqliteVdbeAddOp(v, OP_Next, unionTab, iBreak);
        rc = selectInnerLoop(pParse, 0, unionTab, p->pEList->nExpr,
                             p->pOrderBy, -1, eDest, iParm, 
                             iCont, iBreak);
        if( rc ) return 1;
        sqliteVdbeAddOp(v, OP_Goto, 0, iCont);
        sqliteVdbeResolveLabel(v, iBreak);
        sqliteVdbeAddOp(v, OP_Close, unionTab, 0);
        if( p->pOrderBy ){
          generateSortTail(v, p->pEList->nExpr);
        }
      }
      break;
    }
    case TK_INTERSECT: {
      int tab1, tab2;
      int iCont, iBreak;

      /* INTERSECT is different from the others since it requires
      ** two temporary tables.  Hence it has its own case.  Begin
      ** by allocating the tables we will need.
      */
      tab1 = pParse->nTab++;
      tab2 = pParse->nTab++;
      if( p->pOrderBy && matchOrderbyToColumn(pParse,p,p->pOrderBy,tab1,1) ){
        return 1;
      }
      sqliteVdbeAddOp(v, OP_OpenTemp, tab1, 0);
      sqliteVdbeAddOp(v, OP_KeyAsData, tab1, 1);

      /* Code the SELECTs to our left into temporary table "tab1".
      */
      rc = sqliteSelect(pParse, pPrior, SRT_Union, tab1);
      if( rc ) return rc;

      /* Code the current SELECT into temporary table "tab2"
      */
      sqliteVdbeAddOp(v, OP_OpenTemp, tab2, 0);
      sqliteVdbeAddOp(v, OP_KeyAsData, tab2, 1);
      p->pPrior = 0;
      rc = sqliteSelect(pParse, p, SRT_Union, tab2);
      p->pPrior = pPrior;
      if( rc ) return rc;

      /* Generate code to take the intersection of the two temporary
      ** tables.
      */
      assert( p->pEList );
      generateColumnNames(pParse, 0, p->pEList);
      sqliteVdbeAddOp(v, OP_Rewind, tab1, 0);
      iBreak = sqliteVdbeMakeLabel(v);
      iCont = sqliteVdbeAddOp(v, OP_Next, tab1, iBreak);
      sqliteVdbeAddOp(v, OP_FullKey, tab1, 0);
      sqliteVdbeAddOp(v, OP_NotFound, tab2, iCont);
      rc = selectInnerLoop(pParse, 0, tab1, p->pEList->nExpr,
                             p->pOrderBy, -1, eDest, iParm, 
                             iCont, iBreak);
      if( rc ) return 1;
      sqliteVdbeAddOp(v, OP_Goto, 0, iCont);
      sqliteVdbeResolveLabel(v, iBreak);
      sqliteVdbeAddOp(v, OP_Close, tab2, 0);
      sqliteVdbeAddOp(v, OP_Close, tab1, 0);
      if( p->pOrderBy ){
        generateSortTail(v, p->pEList->nExpr);
      }
      break;
    }
  }
  assert( p->pEList && pPrior->pEList );
  if( p->pEList->nExpr!=pPrior->pEList->nExpr ){
    sqliteSetString(&pParse->zErrMsg, "SELECTs to the left and right of ",
      selectOpName(p->op), " do not have the same number of result columns", 0);
    pParse->nErr++;
    return 1;
  }
  pParse->nTab = base;
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
**     SRT_Except      Remove results form the temporary table iParm.
**
**     SRT_Table       Store results in temporary table iParm
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
  int i;
  WhereInfo *pWInfo;
  Vdbe *v;
  int isAgg = 0;         /* True for select lists like "count(*)" */
  ExprList *pEList;      /* List of columns to extract.  NULL means "*" */
  IdList *pTabList;      /* List of tables to select from */
  Expr *pWhere;          /* The WHERE clause.  May be NULL */
  ExprList *pOrderBy;    /* The ORDER BY clause.  May be NULL */
  ExprList *pGroupBy;    /* The GROUP BY clause.  May be NULL */
  Expr *pHaving;         /* The HAVING clause.  May be NULL */
  int isDistinct;        /* True if the DISTINCT keyword is present */
  int distinct;          /* Table to use for the distinct set */
  int base;              /* First cursor available for use */

  if( sqlite_malloc_failed || pParse->nErr || p==0 ) return 1;

  /* If there is are a sequence of queries, do the earlier ones first.
  */
  if( p->pPrior ){
    return multiSelect(pParse, p, eDest, iParm);
  }

  /* Make local copies of the parameters for this query.
  */
  pTabList = p->pSrc;
  pWhere = p->pWhere;
  pOrderBy = p->pOrderBy;
  pGroupBy = p->pGroupBy;
  pHaving = p->pHaving;
  isDistinct = p->isDistinct;

  /* Save the current value of pParse->nTab.  Restore this value before
  ** we exit.
  */
  base = pParse->nTab;

  /* 
  ** Do not even attempt to generate any code if we have already seen
  ** errors before this routine starts.
  */
  if( pParse->nErr>0 ) return 1;
  sqliteParseInfoReset(pParse);

  /* Look up every table in the table list and create an appropriate
  ** columnlist in pEList if there isn't one already.  (The parser leaves
  ** a NULL in the p->pEList if the SQL said "SELECT * FROM ...")
  */
  if( fillInColumnList(pParse, p) ){
    return 1;
  }
  pEList = p->pEList;
  if( pEList==0 ) return 1;

  /* Allocate a temporary table to use for the DISTINCT set, if
  ** necessary.  This must be done early to allocate the cursor before
  ** any calls to sqliteExprResolveIds().
  */
  if( isDistinct ){
    distinct = pParse->nTab++;
  }else{
    distinct = -1;
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

  /* At this point, we should have allocated all the cursors that we
  ** need to handle subquerys and temporary tables.  From here on we
  ** are committed to keeping the same value for pParse->nTab.
  **
  ** Resolve the column names and do a semantics check on all the expressions.
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
    assert( pParse->nAgg==0 && pParse->iAggCount<0 );
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
    if( pOrderBy ){
      for(i=0; i<pOrderBy->nExpr; i++){
        if( sqliteExprAnalyzeAggregates(pParse, pOrderBy->a[i].pExpr) ){
          return 1;
        }
      }
    }
  }

  /* Begin generating code.
  */
  v = sqliteGetVdbe(pParse);
  if( v==0 ) return 1;

  /* Set the limiter
  */
  if( p->nLimit<=0 ){
    p->nOffset = 0;
  }else{
    if( p->nOffset<0 ) p->nOffset = 0;
    sqliteVdbeAddOp(v, OP_Limit, p->nLimit, p->nOffset);
  }
    

  /* Identify column names if we will be using in the callback.  This
  ** step is skipped if the output is going to a table or a memory cell.
  */
  if( eDest==SRT_Callback ){
    generateColumnNames(pParse, pTabList, pEList);
  }

  /* Reset the aggregator
  */
  if( isAgg ){
    sqliteVdbeAddOp(v, OP_AggReset, 0, pParse->nAgg);
    if( pGroupBy==0 ){
      sqliteVdbeAddOp(v, OP_String, 0, 0);
      sqliteVdbeChangeP3(v, -1, "", P3_STATIC);
      sqliteVdbeAddOp(v, OP_AggFocus, 0, 0);
      for(i=0; i<pParse->nAgg; i++){
        Expr *pE;
        if( !pParse->aAgg[i].isAgg ) continue;
        pE = pParse->aAgg[i].pExpr;
        assert( pE==0 || pE->op==TK_AGG_FUNCTION );
        assert( pE==0 || (pE->pList!=0 && pE->pList->nExpr==1) );
        if( pE==0 || pE->iColumn==FN_Sum ){
          sqliteVdbeAddOp(v, OP_Integer, 0, 0);
          sqliteVdbeAddOp(v, OP_AggSet, 0, i);
          continue;
        }
      }
    }
  }

  /* Initialize the memory cell to NULL
  */
  if( eDest==SRT_Mem ){
    sqliteVdbeAddOp(v, OP_String, 0, 0);
    sqliteVdbeAddOp(v, OP_MemStore, iParm, 0);
  }

  /* Begin the database scan
  */
  if( isDistinct ){
    sqliteVdbeAddOp(v, OP_OpenTemp, distinct, 0);
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
    if( pGroupBy ){
      int lbl1;
      for(i=0; i<pGroupBy->nExpr; i++){
        sqliteExprCode(pParse, pGroupBy->a[i].pExpr);
      }
      sqliteVdbeAddOp(v, OP_MakeKey, pGroupBy->nExpr, 0);
      lbl1 = sqliteVdbeMakeLabel(v);
      sqliteVdbeAddOp(v, OP_AggFocus, 0, lbl1);
      for(i=0; i<pParse->nAgg; i++){
        if( pParse->aAgg[i].isAgg ) continue;
        sqliteExprCode(pParse, pParse->aAgg[i].pExpr);
        sqliteVdbeAddOp(v, OP_AggSet, 0, i);
      }
      sqliteVdbeResolveLabel(v, lbl1);
    }
    for(i=0; i<pParse->nAgg; i++){
      Expr *pE;
      int op;
      if( !pParse->aAgg[i].isAgg ) continue;
      pE = pParse->aAgg[i].pExpr;
      if( pE==0 ){
        sqliteVdbeAddOp(v, OP_AggIncr, 1, i);
        continue;
      }
      assert( pE->op==TK_AGG_FUNCTION );
      assert( pE->pList!=0 && pE->pList->nExpr==1 );
      sqliteExprCode(pParse, pE->pList->a[0].pExpr);
      sqliteVdbeAddOp(v, OP_AggGet, 0, i);
      switch( pE->iColumn ){
        case FN_Min:  op = OP_Min;   break;
        case FN_Max:  op = OP_Max;   break;
        case FN_Avg:  op = OP_Add;   break;
        case FN_Sum:  op = OP_Add;   break;
      }
      sqliteVdbeAddOp(v, op, 0, 0);
      sqliteVdbeAddOp(v, OP_AggSet, 0, i);
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
    startagg = sqliteVdbeAddOp(v, OP_AggNext, 0, endagg);
    pParse->useAgg = 1;
    if( pHaving ){
      sqliteExprIfFalse(pParse, pHaving, startagg);
    }
    if( selectInnerLoop(pParse, pEList, 0, 0, pOrderBy, distinct, eDest, iParm,
                    startagg, endagg) ){
      return 1;
    }
    sqliteVdbeAddOp(v, OP_Goto, 0, startagg);
    sqliteVdbeResolveLabel(v, endagg);
    sqliteVdbeAddOp(v, OP_Noop, 0, 0);
    pParse->useAgg = 0;
  }

  /* If there is an ORDER BY clause, then we need to sort the results
  ** and send them to the callback one by one.
  */
  if( pOrderBy ){
    generateSortTail(v, pEList->nExpr);
  }
  pParse->nTab = base;


  /* Issue a null callback if that is what the user wants.
  */
  if( (pParse->db->flags & SQLITE_NullCallback)!=0 && eDest==SRT_Callback ){
    sqliteVdbeAddOp(v, OP_NullCallback, pEList->nExpr, 0);
  }

  return 0;
}
