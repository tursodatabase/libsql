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
** $Id: select.c,v 1.69 2002/02/28 00:41:11 drh Exp $
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
  sqliteFree(p->zSelect);
  sqliteFree(p);
}

/*
** Delete the aggregate information from the parse structure.
*/
static void sqliteAggregateInfoReset(Parse *pParse){
  sqliteFree(pParse->aAgg);
  pParse->aAgg = 0;
  pParse->nAgg = 0;
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
    sqliteVdbeAddOp(v, OP_PutStrKey, distinct, 0);
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
    sqliteVdbeAddOp(v, OP_PutStrKey, iParm, 0);
  }else 

  /* Store the result as data using a unique key.
  */
  if( eDest==SRT_Table ){
    sqliteVdbeAddOp(v, OP_MakeRecord, nColumn, 0);
    sqliteVdbeAddOp(v, OP_NewRecno, iParm, 0);
    sqliteVdbeAddOp(v, OP_Pull, 1, 0);
    sqliteVdbeAddOp(v, OP_PutIntKey, iParm, 0);
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
    sqliteVdbeAddOp(v, OP_PutStrKey, iParm, 0);
  }else 


  /* If this is a scalar select that is part of an expression, then
  ** store the results in the appropriate memory cell and break out
  ** of the scan loop.
  */
  if( eDest==SRT_Mem ){
    assert( nColumn==1 );
    sqliteVdbeAddOp(v, OP_MemStore, iParm, 1);
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
      Table *pTab = pTabList->a[p->iTable - pParse->nTab].pTab;
      char *zCol;
      int iCol = p->iColumn;
      if( iCol<0 ) iCol = pTab->iPKey;
      assert( iCol==-1 || (iCol>=0 && iCol<pTab->nCol) );
      zCol = iCol<0 ? "_ROWID_" : pTab->aCol[iCol].zName;
      if( pTabList->nId>1 || showFullNames ){
        char *zName = 0;
        char *zTab;
 
        zTab = pTabList->a[p->iTable - pParse->nTab].zAlias;
        if( showFullNames || zTab==0 ) zTab = pTab->zName;
        sqliteSetString(&zName, zTab, ".", zCol, 0);
        sqliteVdbeAddOp(v, OP_ColumnName, i, 0);
        sqliteVdbeChangeP3(v, -1, zName, strlen(zName));
        sqliteFree(zName);
      }else{
        sqliteVdbeAddOp(v, OP_ColumnName, i, 0);
        sqliteVdbeChangeP3(v, -1, zCol, 0);
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
** Given a SELECT statement, generate a Table structure that describes
** the result set of that SELECT.
*/
Table *sqliteResultSetOfSelect(Parse *pParse, char *zTabName, Select *pSelect){
  Table *pTab;
  int i;
  ExprList *pEList;
  static int fillInColumnList(Parse*, Select*);

  if( fillInColumnList(pParse, pSelect) ){
    return 0;
  }
  pTab = sqliteMalloc( sizeof(Table) );
  if( pTab==0 ){
    return 0;
  }
  pTab->zName = zTabName ? sqliteStrDup(zTabName) : 0;
  pEList = pSelect->pEList;
  pTab->nCol = pEList->nExpr;
  pTab->aCol = sqliteMalloc( sizeof(pTab->aCol[0])*pTab->nCol );
  for(i=0; i<pTab->nCol; i++){
    Expr *p;
    if( pEList->a[i].zName ){
      pTab->aCol[i].zName = sqliteStrDup(pEList->a[i].zName);
    }else if( (p=pEList->a[i].pExpr)->span.z && p->span.z[0] ){
      sqliteSetNString(&pTab->aCol[i].zName, p->span.z, p->span.n, 0);
    }else if( p->op==TK_DOT && p->pRight && p->pRight->token.z &&
           p->pRight->token.z[0] ){
      sqliteSetNString(&pTab->aCol[i].zName, 
           p->pRight->token.z, p->pRight->token.n, 0);
    }else{
      char zBuf[30];
      sprintf(zBuf, "column%d", i+1);
      pTab->aCol[i].zName = sqliteStrDup(zBuf);
    }
  }
  pTab->iPKey = -1;
  return pTab;
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
  int i, j, k;
  IdList *pTabList;
  ExprList *pEList;
  Table *pTab;

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
      /* A sub-query in the FROM clause of a SELECT */
      assert( pTabList->a[i].pSelect!=0 );
      pTabList->a[i].pTab = pTab = 
        sqliteResultSetOfSelect(pParse, pTabList->a[i].zAlias,
                                        pTabList->a[i].pSelect);
      if( pTab==0 ){
        return 1;
      }
      pTab->isTransient = 1;
    }else{
      /* An ordinary table or view name in the FROM clause */
      pTabList->a[i].pTab = pTab = 
        sqliteFindTable(pParse->db, pTabList->a[i].zName);
      if( pTab==0 ){
        sqliteSetString(&pParse->zErrMsg, "no such table: ", 
           pTabList->a[i].zName, 0);
        pParse->nErr++;
        return 1;
      }
      if( pTab->pSelect ){
        pTabList->a[i].pSelect = sqliteSelectDup(pTab->pSelect);
      }
    }
  }

  /* For every "*" that occurs in the column list, insert the names of
  ** all columns in all tables.  The parser inserted a special expression
  ** with the TK_ALL operator for each "*" that it found in the column list.
  ** The following code just has to locate the TK_ALL expressions and expand
  ** each one to the list of all columns in all tables.
  */
  for(k=0; k<pEList->nExpr; k++){
    if( pEList->a[k].pExpr->op==TK_ALL ) break;
  }
  if( k<pEList->nExpr ){
    struct ExprList_item *a = pEList->a;
    ExprList *pNew = 0;
    for(k=0; k<pEList->nExpr; k++){
      if( a[k].pExpr->op!=TK_ALL ){
        pNew = sqliteExprListAppend(pNew, a[k].pExpr, 0);
        pNew->a[pNew->nExpr-1].zName = a[k].zName;
        a[k].pExpr = 0;
        a[k].zName = 0;
      }else{
        for(i=0; i<pTabList->nId; i++){
          Table *pTab = pTabList->a[i].pTab;
          for(j=0; j<pTab->nCol; j++){
            Expr *pExpr, *pLeft, *pRight;
            pRight = sqliteExpr(TK_ID, 0, 0, 0);
            if( pRight==0 ) break;
            pRight->token.z = pTab->aCol[j].zName;
            pRight->token.n = strlen(pTab->aCol[j].zName);
            if( pTab->zName ){
              pLeft = sqliteExpr(TK_ID, 0, 0, 0);
              if( pLeft==0 ) break;
              if( pTabList->a[i].zAlias && pTabList->a[i].zAlias[0] ){
                pLeft->token.z = pTabList->a[i].zAlias;
                pLeft->token.n = strlen(pTabList->a[i].zAlias);
              }else{
                pLeft->token.z = pTab->zName;
                pLeft->token.n = strlen(pTab->zName);
              }
              pExpr = sqliteExpr(TK_DOT, pLeft, pRight, 0);
              if( pExpr==0 ) break;
            }else{
              pExpr = pRight;
              pExpr->span = pExpr->token;
            }
            pNew = sqliteExprListAppend(pNew, pExpr, 0);
          }
        }
      }
    }
    sqliteExprListDelete(pEList);
    p->pEList = pNew;
  }
  return 0;
}

/*
** This routine recursively unlinks the Select.pSrc.a[].pTab pointers
** in a select structure.  It just sets the pointers to NULL.  This
** routine is recursive in the sense that if the Select.pSrc.a[].pSelect
** pointer is not NULL, this routine is called recursively on that pointer.
**
** This routine is called on the Select structure that defines a
** VIEW in order to undo any bindings to tables.  This is necessary
** because those tables might be DROPed by a subsequent SQL command.
*/
void sqliteSelectUnbind(Select *p){
  int i;
  IdList *pSrc = p->pSrc;
  Table *pTab;
  if( p==0 ) return;
  for(i=0; i<pSrc->nId; i++){
    if( (pTab = pSrc->a[i].pTab)!=0 ){
      if( pTab->isTransient ){
        sqliteDeleteTable(0, pTab);
        sqliteSelectDelete(pSrc->a[i].pSelect);
        pSrc->a[i].pSelect = 0;
      }
      pSrc->a[i].pTab = 0;
      if( pSrc->a[i].pSelect ){
        sqliteSelectUnbind(pSrc->a[i].pSelect);
      }
    }
  }
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
        char *zName, *zLabel;
        zName = pEList->a[j].zName;
        assert( pE->token.z );
        zLabel = sqliteStrNDup(pE->token.z, pE->token.n);
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
          sqliteVdbeAddOp(v, OP_OpenTemp, unionTab, 1);
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
        int iCont, iBreak, iStart;
        assert( p->pEList );
        generateColumnNames(pParse, 0, p->pEList);
        iBreak = sqliteVdbeMakeLabel(v);
        iCont = sqliteVdbeMakeLabel(v);
        sqliteVdbeAddOp(v, OP_Rewind, unionTab, iBreak);
        iStart = sqliteVdbeCurrentAddr(v);
        rc = selectInnerLoop(pParse, 0, unionTab, p->pEList->nExpr,
                             p->pOrderBy, -1, eDest, iParm, 
                             iCont, iBreak);
        if( rc ) return 1;
        sqliteVdbeResolveLabel(v, iCont);
        sqliteVdbeAddOp(v, OP_Next, unionTab, iStart);
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
      int iCont, iBreak, iStart;

      /* INTERSECT is different from the others since it requires
      ** two temporary tables.  Hence it has its own case.  Begin
      ** by allocating the tables we will need.
      */
      tab1 = pParse->nTab++;
      tab2 = pParse->nTab++;
      if( p->pOrderBy && matchOrderbyToColumn(pParse,p,p->pOrderBy,tab1,1) ){
        return 1;
      }
      sqliteVdbeAddOp(v, OP_OpenTemp, tab1, 1);
      sqliteVdbeAddOp(v, OP_KeyAsData, tab1, 1);

      /* Code the SELECTs to our left into temporary table "tab1".
      */
      rc = sqliteSelect(pParse, pPrior, SRT_Union, tab1);
      if( rc ) return rc;

      /* Code the current SELECT into temporary table "tab2"
      */
      sqliteVdbeAddOp(v, OP_OpenTemp, tab2, 1);
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
      iBreak = sqliteVdbeMakeLabel(v);
      iCont = sqliteVdbeMakeLabel(v);
      sqliteVdbeAddOp(v, OP_Rewind, tab1, iBreak);
      iStart = sqliteVdbeAddOp(v, OP_FullKey, tab1, 0);
      sqliteVdbeAddOp(v, OP_NotFound, tab2, iCont);
      rc = selectInnerLoop(pParse, 0, tab1, p->pEList->nExpr,
                             p->pOrderBy, -1, eDest, iParm, 
                             iCont, iBreak);
      if( rc ) return 1;
      sqliteVdbeResolveLabel(v, iCont);
      sqliteVdbeAddOp(v, OP_Next, tab1, iStart);
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
** This routine attempts to flatten subqueries in order to speed
** execution.  It returns 1 if it makes changes and 0 if no flattening
** occurs.
**
** To understand the concept of flattening, consider the following
** query:
**
**     SELECT a FROM (SELECT x+y AS a FROM t1 WHERE z<100) WHERE a>5
**
** The default way of implementing this query is to execute the
** subquery first and store the results in a temporary table, then
** run the outer query on that temporary table.  This requires two
** passes over the data.  Furthermore, because the temporary table
** has no indices, the WHERE clause on the outer query cannot be
** optimized using indices.
**
** This routine attempts to write queries such as the above into
** a single flat select, like this:
**
**     SELECT x+y AS a FROM t1 WHERE z<100 AND a>5
**
** The code generated for this simpification gives the same result
** but only has to scan the data once.
**
** Generally speaking, flattening is only possible if the subquery
** query is a simple query without a GROUP BY clause or the DISTINCT
** keyword and the outer query is not a join. 
**
** If flattening is not possible, this routine is a no-op and return 0.
** If flattening is possible, this routine  rewrites the query into
** the simplified form and return 1.
**
** All of the expression analysis must occur before this routine runs.
** This routine depends on the results of the expression analysis.
*/
int flattenSubqueries(Select *p){
  Select *pSub;
  if( p->pSrc->nId>1 ){
    return 0;   /* Cannot optimize: The outer query is a join. */
  }
  pSub = p->pSrc->a[0].pSelect;
  if( pSub==0 ){
    return 0;   /* Nothing to optimize: There is no subquery. */
  }
  if( pSub->isDistinct ){
    return 0;   /* Subquery contains DISTINCT keyword */
  }
  if( pSub->pGroupBy ){
    return 0;   /* Subquery contains a GROUP BY clause */
  }
  if( pSub->pPrior ){
    return 0;   /* Subquery is the union of two or more queries */
  } 

  return 0;
}	

/*
** Analyze the SELECT statement passed in as an argument to see if it
** is a simple min() or max() query.  If it is and this query can be
** satisfied using a single seek to the beginning or end of an index,
** then generate the code for this SELECT return 1.  If this is not a 
** simple min() or max() query, then return 0;
**
** A simply min() or max() query looks like this:
**
**    SELECT min(a) FROM table;
**    SELECT max(a) FROM table;
**
** The query may have only a single table in its FROM argument.  There
** can be no GROUP BY or HAVING or WHERE clauses.  The result set must
** be the min() or max() of a single column of the table.  The column
** in the min() or max() function must be indexed.
**
** The parameters to this routine are the same as for sqliteSelect().
** See the header comment on that routine for additional information.
*/
static int simpleMinMaxQuery(Parse *pParse, Select *p, int eDest, int iParm){
  Expr *pExpr;
  int iCol;
  Table *pTab;
  Index *pIdx;
  int base;
  Vdbe *v;
  int openOp;
  int seekOp;
  int cont;
  ExprList eList;
  struct ExprList_item eListItem;

  /* Check to see if this query is a simple min() or max() query.  Return
  ** zero if it is  not.
  */
  if( p->pGroupBy || p->pHaving || p->pWhere ) return 0;
  if( p->pSrc->nId!=1 ) return 0;
  if( p->pEList->nExpr!=1 ) return 0;
  pExpr = p->pEList->a[0].pExpr;
  if( pExpr->op!=TK_AGG_FUNCTION ) return 0;
  if( pExpr->pList==0 || pExpr->pList->nExpr!=1 ) return 0;
  if( pExpr->token.n!=3 ) return 0;
  if( sqliteStrNICmp(pExpr->token.z,"min",3)==0 ){
    seekOp = OP_Rewind;
  }else if( sqliteStrNICmp(pExpr->token.z,"max",3)==0 ){
    seekOp = OP_Last;
  }else{
    return 0;
  }
  pExpr = pExpr->pList->a[0].pExpr;
  if( pExpr->op!=TK_COLUMN ) return 0;
  iCol = pExpr->iColumn;
  pTab = p->pSrc->a[0].pTab;

  /* If we get to here, it means the query is of the correct form.
  ** Check to make sure we have an index and make pIdx point to the
  ** appropriate index.  If the min() or max() is on an INTEGER PRIMARY
  ** key column, no index is necessary so set pIdx to NULL.  If no
  ** usable index is found, return 0.
  */
  if( iCol<0 ){
    pIdx = 0;
  }else{
    for(pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext){
      assert( pIdx->nColumn>=1 );
      if( pIdx->aiColumn[0]==iCol ) break;
    }
    if( pIdx==0 ) return 0;
  }

  /* Identify column names if we will be using the callback.  This
  ** step is skipped if the output is going to a table or a memory cell.
  */
  v = sqliteGetVdbe(pParse);
  if( v==0 ) return 0;
  if( eDest==SRT_Callback ){
    generateColumnNames(pParse, p->pSrc, p->pEList);
  }

  /* Generating code to find the min or the max.  Basically all we have
  ** to do is find the first or the last entry in the chosen index.  If
  ** the min() or max() is on the INTEGER PRIMARY KEY, then find the first
  ** or last entry in the main table.
  */
  if( !pParse->schemaVerified && (pParse->db->flags & SQLITE_InTrans)==0 ){
    sqliteVdbeAddOp(v, OP_VerifyCookie, pParse->db->schema_cookie, 0);
    pParse->schemaVerified = 1;
  }
  openOp = pTab->isTemp ? OP_OpenAux : OP_Open;
  base = pParse->nTab;
  sqliteVdbeAddOp(v, openOp, base, pTab->tnum);
  sqliteVdbeChangeP3(v, -1, pTab->zName, P3_STATIC);
  if( pIdx==0 ){
    sqliteVdbeAddOp(v, seekOp, base, 0);
  }else{
    sqliteVdbeAddOp(v, openOp, base+1, pIdx->tnum);
    sqliteVdbeChangeP3(v, -1, pIdx->zName, P3_STATIC);
    sqliteVdbeAddOp(v, seekOp, base+1, 0);
    sqliteVdbeAddOp(v, OP_IdxRecno, base+1, 0);
    sqliteVdbeAddOp(v, OP_Close, base+1, 0);
    sqliteVdbeAddOp(v, OP_MoveTo, base, 0);
  }
  eList.nExpr = 1;
  memset(&eListItem, 0, sizeof(eListItem));
  eList.a = &eListItem;
  eList.a[0].pExpr = pExpr;
  cont = sqliteVdbeMakeLabel(v);
  selectInnerLoop(pParse, &eList, base, 1, 0, -1, eDest, iParm, cont, cont);
  sqliteVdbeResolveLabel(v, cont);
  sqliteVdbeAddOp(v, OP_Close, base, 0);
  return 1;
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
  ExprList *pEList;      /* List of columns to extract. */
  IdList *pTabList;      /* List of tables to select from */
  Expr *pWhere;          /* The WHERE clause.  May be NULL */
  ExprList *pOrderBy;    /* The ORDER BY clause.  May be NULL */
  ExprList *pGroupBy;    /* The GROUP BY clause.  May be NULL */
  Expr *pHaving;         /* The HAVING clause.  May be NULL */
  int isDistinct;        /* True if the DISTINCT keyword is present */
  int distinct;          /* Table to use for the distinct set */
  int base;              /* First cursor available for use */
  int rc = 1;            /* Value to return from this function */

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
  if( pParse->nErr>0 ) goto select_end;

  /* Look up every table in the table list and create an appropriate
  ** columnlist in pEList if there isn't one already.  (The parser leaves
  ** a NULL in the p->pEList if the SQL said "SELECT * FROM ...")
  */
  if( fillInColumnList(pParse, p) ){
    goto select_end;
  }
  pEList = p->pEList;
  if( pEList==0 ) goto select_end;

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
    goto select_end;
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
    if( sqliteExprResolveIds(pParse, pTabList, 0, pEList->a[i].pExpr) ){
      goto select_end;
    }
    if( sqliteExprCheck(pParse, pEList->a[i].pExpr, 1, &isAgg) ){
      goto select_end;
    }
  }
  if( pWhere ){
    if( sqliteExprResolveIds(pParse, pTabList, pEList, pWhere) ){
      goto select_end;
    }
    if( sqliteExprCheck(pParse, pWhere, 0, 0) ){
      goto select_end;
    }
  }
  if( pOrderBy ){
    for(i=0; i<pOrderBy->nExpr; i++){
      Expr *pE = pOrderBy->a[i].pExpr;
      if( sqliteExprIsConstant(pE) ){
        sqliteSetString(&pParse->zErrMsg, 
             "ORDER BY expressions should not be constant", 0);
        pParse->nErr++;
        goto select_end;
      }
      if( sqliteExprResolveIds(pParse, pTabList, pEList, pE) ){
        goto select_end;
      }
      if( sqliteExprCheck(pParse, pE, isAgg, 0) ){
        goto select_end;
      }
    }
  }
  if( pGroupBy ){
    for(i=0; i<pGroupBy->nExpr; i++){
      Expr *pE = pGroupBy->a[i].pExpr;
      if( sqliteExprIsConstant(pE) ){
        sqliteSetString(&pParse->zErrMsg, 
             "GROUP BY expressions should not be constant", 0);
        pParse->nErr++;
        goto select_end;
      }
      if( sqliteExprResolveIds(pParse, pTabList, pEList, pE) ){
        goto select_end;
      }
      if( sqliteExprCheck(pParse, pE, isAgg, 0) ){
        goto select_end;
      }
    }
  }
  if( pHaving ){
    if( pGroupBy==0 ){
      sqliteSetString(&pParse->zErrMsg, "a GROUP BY clause is required "
         "before HAVING", 0);
      pParse->nErr++;
      goto select_end;
    }
    if( sqliteExprResolveIds(pParse, pTabList, pEList, pHaving) ){
      goto select_end;
    }
    if( sqliteExprCheck(pParse, pHaving, isAgg, 0) ){
      goto select_end;
    }
  }

  /* Try to merge subqueries in the FROM clause into the main
  ** query.
  */
  if( flattenSubqueries(p) ){
    pEList = p->pEList;
    pWhere = p->pWhere;
  }

  /* Check for the special case of a min() or max() function by itself
  ** in the result set.
  */
  if( simpleMinMaxQuery(pParse, p, eDest, iParm) ){
    rc = 0;
    goto select_end;
  }

  /* Begin generating code.
  */
  v = sqliteGetVdbe(pParse);
  if( v==0 ) goto select_end;

  /* Generate code for all sub-queries in the FROM clause
  */
  for(i=0; i<pTabList->nId; i++){
    int oldNTab;
    if( pTabList->a[i].pSelect==0 ) continue;
    oldNTab = pParse->nTab;
    pParse->nTab += i+1;
    sqliteVdbeAddOp(v, OP_OpenTemp, oldNTab+i, 0);
    sqliteSelect(pParse, pTabList->a[i].pSelect, SRT_Table, oldNTab+i);
    pParse->nTab = oldNTab;
  }

  /* Do an analysis of aggregate expressions.
  */
  sqliteAggregateInfoReset(pParse);
  if( isAgg ){
    assert( pParse->nAgg==0 );
    for(i=0; i<pEList->nExpr; i++){
      if( sqliteExprAnalyzeAggregates(pParse, pEList->a[i].pExpr) ){
        goto select_end;
      }
    }
    if( pGroupBy ){
      for(i=0; i<pGroupBy->nExpr; i++){
        if( sqliteExprAnalyzeAggregates(pParse, pGroupBy->a[i].pExpr) ){
          goto select_end;
        }
      }
    }
    if( pHaving && sqliteExprAnalyzeAggregates(pParse, pHaving) ){
      goto select_end;
    }
    if( pOrderBy ){
      for(i=0; i<pOrderBy->nExpr; i++){
        if( sqliteExprAnalyzeAggregates(pParse, pOrderBy->a[i].pExpr) ){
          goto select_end;
        }
      }
    }
  }

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
    for(i=0; i<pParse->nAgg; i++){
      FuncDef *pFunc;
      if( (pFunc = pParse->aAgg[i].pFunc)!=0 && pFunc->xFinalize!=0 ){
        sqliteVdbeAddOp(v, OP_AggInit, 0, i);
        sqliteVdbeChangeP3(v, -1, (char*)pFunc, P3_POINTER);
      }
    }
    if( pGroupBy==0 ){
      sqliteVdbeAddOp(v, OP_String, 0, 0);
      sqliteVdbeAddOp(v, OP_AggFocus, 0, 0);
    }
  }

  /* Initialize the memory cell to NULL
  */
  if( eDest==SRT_Mem ){
    sqliteVdbeAddOp(v, OP_String, 0, 0);
    sqliteVdbeAddOp(v, OP_MemStore, iParm, 1);
  }

  /* Begin the database scan
  */
  if( isDistinct ){
    sqliteVdbeAddOp(v, OP_OpenTemp, distinct, 1);
  }
  pWInfo = sqliteWhereBegin(pParse, pTabList, pWhere, 0);
  if( pWInfo==0 ) goto select_end;

  /* Use the standard inner loop if we are not dealing with
  ** aggregates
  */
  if( !isAgg ){
    if( selectInnerLoop(pParse, pEList, 0, 0, pOrderBy, distinct, eDest, iParm,
                    pWInfo->iContinue, pWInfo->iBreak) ){
       goto select_end;
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
      int j;
      if( !pParse->aAgg[i].isAgg ) continue;
      pE = pParse->aAgg[i].pExpr;
      assert( pE->op==TK_AGG_FUNCTION );
      if( pE->pList ){
        for(j=0; j<pE->pList->nExpr; j++){
          sqliteExprCode(pParse, pE->pList->a[j].pExpr);
        }
      }
      sqliteVdbeAddOp(v, OP_Integer, i, 0);
      sqliteVdbeAddOp(v, OP_AggFunc, 0, pE->pList->nExpr);
      assert( pParse->aAgg[i].pFunc!=0 );
      assert( pParse->aAgg[i].pFunc->xStep!=0 );
      sqliteVdbeChangeP3(v, -1, (char*)pParse->aAgg[i].pFunc, P3_POINTER);
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
      goto select_end;
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

  /* The SELECT was successfully coded.   Set the return code to 0
  ** to indicate no errors.
  */
  rc = 0;

  /* Control jumps to here if an error is encountered above, or upon
  ** successful coding of the SELECT.
  */
select_end:
  sqliteAggregateInfoReset(pParse);
  return rc;
}
