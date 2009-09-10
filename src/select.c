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
** $Id: select.c,v 1.526 2009/08/01 15:09:58 drh Exp $
*/
#include "sqliteInt.h"


/*
** Delete all the content of a Select structure but do not deallocate
** the select structure itself.
*/
static void clearSelect(sqlite3 *db, Select *p){
  sqlite3ExprListDelete(db, p->pEList);
  sqlite3SrcListDelete(db, p->pSrc);
  sqlite3ExprDelete(db, p->pWhere);
  sqlite3ExprListDelete(db, p->pGroupBy);
  sqlite3ExprDelete(db, p->pHaving);
  sqlite3ExprListDelete(db, p->pOrderBy);
  sqlite3SelectDelete(db, p->pPrior);
  sqlite3ExprDelete(db, p->pLimit);
  sqlite3ExprDelete(db, p->pOffset);
}

/*
** Initialize a SelectDest structure.
*/
void sqlite3SelectDestInit(SelectDest *pDest, int eDest, int iParm){
  pDest->eDest = (u8)eDest;
  pDest->iParm = iParm;
  pDest->affinity = 0;
  pDest->iMem = 0;
  pDest->nMem = 0;
}


/*
** Allocate a new Select structure and return a pointer to that
** structure.
*/
Select *sqlite3SelectNew(
  Parse *pParse,        /* Parsing context */
  ExprList *pEList,     /* which columns to include in the result */
  SrcList *pSrc,        /* the FROM clause -- which tables to scan */
  Expr *pWhere,         /* the WHERE clause */
  ExprList *pGroupBy,   /* the GROUP BY clause */
  Expr *pHaving,        /* the HAVING clause */
  ExprList *pOrderBy,   /* the ORDER BY clause */
  int isDistinct,       /* true if the DISTINCT keyword is present */
  Expr *pLimit,         /* LIMIT value.  NULL means not used */
  Expr *pOffset         /* OFFSET value.  NULL means no offset */
){
  Select *pNew;
  Select standin;
  sqlite3 *db = pParse->db;
  pNew = sqlite3DbMallocZero(db, sizeof(*pNew) );
  assert( db->mallocFailed || !pOffset || pLimit ); /* OFFSET implies LIMIT */
  if( pNew==0 ){
    pNew = &standin;
    memset(pNew, 0, sizeof(*pNew));
  }
  if( pEList==0 ){
    pEList = sqlite3ExprListAppend(pParse, 0, sqlite3Expr(db,TK_ALL,0));
  }
  pNew->pEList = pEList;
  pNew->pSrc = pSrc;
  pNew->pWhere = pWhere;
  pNew->pGroupBy = pGroupBy;
  pNew->pHaving = pHaving;
  pNew->pOrderBy = pOrderBy;
  pNew->selFlags = isDistinct ? SF_Distinct : 0;
  pNew->op = TK_SELECT;
  pNew->pLimit = pLimit;
  pNew->pOffset = pOffset;
  assert( pOffset==0 || pLimit!=0 );
  pNew->addrOpenEphm[0] = -1;
  pNew->addrOpenEphm[1] = -1;
  pNew->addrOpenEphm[2] = -1;
  if( db->mallocFailed ) {
    clearSelect(db, pNew);
    if( pNew!=&standin ) sqlite3DbFree(db, pNew);
    pNew = 0;
  }
  return pNew;
}

/*
** Delete the given Select structure and all of its substructures.
*/
void sqlite3SelectDelete(sqlite3 *db, Select *p){
  if( p ){
    clearSelect(db, p);
    sqlite3DbFree(db, p);
  }
}

/*
** Given 1 to 3 identifiers preceeding the JOIN keyword, determine the
** type of join.  Return an integer constant that expresses that type
** in terms of the following bit values:
**
**     JT_INNER
**     JT_CROSS
**     JT_OUTER
**     JT_NATURAL
**     JT_LEFT
**     JT_RIGHT
**
** A full outer join is the combination of JT_LEFT and JT_RIGHT.
**
** If an illegal or unsupported join type is seen, then still return
** a join type, but put an error in the pParse structure.
*/
int sqlite3JoinType(Parse *pParse, Token *pA, Token *pB, Token *pC){
  int jointype = 0;
  Token *apAll[3];
  Token *p;
                             /*   0123456789 123456789 123456789 123 */
  static const char zKeyText[] = "naturaleftouterightfullinnercross";
  static const struct {
    u8 i;        /* Beginning of keyword text in zKeyText[] */
    u8 nChar;    /* Length of the keyword in characters */
    u8 code;     /* Join type mask */
  } aKeyword[] = {
    /* natural */ { 0,  7, JT_NATURAL                },
    /* left    */ { 6,  4, JT_LEFT|JT_OUTER          },
    /* outer   */ { 10, 5, JT_OUTER                  },
    /* right   */ { 14, 5, JT_RIGHT|JT_OUTER         },
    /* full    */ { 19, 4, JT_LEFT|JT_RIGHT|JT_OUTER },
    /* inner   */ { 23, 5, JT_INNER                  },
    /* cross   */ { 28, 5, JT_INNER|JT_CROSS         },
  };
  int i, j;
  apAll[0] = pA;
  apAll[1] = pB;
  apAll[2] = pC;
  for(i=0; i<3 && apAll[i]; i++){
    p = apAll[i];
    for(j=0; j<ArraySize(aKeyword); j++){
      if( p->n==aKeyword[j].nChar 
          && sqlite3StrNICmp((char*)p->z, &zKeyText[aKeyword[j].i], p->n)==0 ){
        jointype |= aKeyword[j].code;
        break;
      }
    }
    testcase( j==0 || j==1 || j==2 || j==3 || j==4 || j==5 || j==6 );
    if( j>=ArraySize(aKeyword) ){
      jointype |= JT_ERROR;
      break;
    }
  }
  if(
     (jointype & (JT_INNER|JT_OUTER))==(JT_INNER|JT_OUTER) ||
     (jointype & JT_ERROR)!=0
  ){
    const char *zSp = " ";
    assert( pB!=0 );
    if( pC==0 ){ zSp++; }
    sqlite3ErrorMsg(pParse, "unknown or unsupported join type: "
       "%T %T%s%T", pA, pB, zSp, pC);
    jointype = JT_INNER;
  }else if( (jointype & JT_OUTER)!=0 
         && (jointype & (JT_LEFT|JT_RIGHT))!=JT_LEFT ){
    sqlite3ErrorMsg(pParse, 
      "RIGHT and FULL OUTER JOINs are not currently supported");
    jointype = JT_INNER;
  }
  return jointype;
}

/*
** Return the index of a column in a table.  Return -1 if the column
** is not contained in the table.
*/
static int columnIndex(Table *pTab, const char *zCol){
  int i;
  for(i=0; i<pTab->nCol; i++){
    if( sqlite3StrICmp(pTab->aCol[i].zName, zCol)==0 ) return i;
  }
  return -1;
}

/*
** Create an expression node for an identifier with the name of zName
*/
Expr *sqlite3CreateIdExpr(Parse *pParse, const char *zName){
  return sqlite3Expr(pParse->db, TK_ID, zName);
}

/*
** Add a term to the WHERE expression in *ppExpr that requires the
** zCol column to be equal in the two tables pTab1 and pTab2.
*/
static void addWhereTerm(
  Parse *pParse,           /* Parsing context */
  const char *zCol,        /* Name of the column */
  const Table *pTab1,      /* First table */
  const char *zAlias1,     /* Alias for first table.  May be NULL */
  const Table *pTab2,      /* Second table */
  const char *zAlias2,     /* Alias for second table.  May be NULL */
  int iRightJoinTable,     /* VDBE cursor for the right table */
  Expr **ppExpr,           /* Add the equality term to this expression */
  int isOuterJoin          /* True if dealing with an OUTER join */
){
  Expr *pE1a, *pE1b, *pE1c;
  Expr *pE2a, *pE2b, *pE2c;
  Expr *pE;

  pE1a = sqlite3CreateIdExpr(pParse, zCol);
  pE2a = sqlite3CreateIdExpr(pParse, zCol);
  if( zAlias1==0 ){
    zAlias1 = pTab1->zName;
  }
  pE1b = sqlite3CreateIdExpr(pParse, zAlias1);
  if( zAlias2==0 ){
    zAlias2 = pTab2->zName;
  }
  pE2b = sqlite3CreateIdExpr(pParse, zAlias2);
  pE1c = sqlite3PExpr(pParse, TK_DOT, pE1b, pE1a, 0);
  pE2c = sqlite3PExpr(pParse, TK_DOT, pE2b, pE2a, 0);
  pE = sqlite3PExpr(pParse, TK_EQ, pE1c, pE2c, 0);
  if( pE && isOuterJoin ){
    ExprSetProperty(pE, EP_FromJoin);
    assert( !ExprHasAnyProperty(pE, EP_TokenOnly|EP_Reduced) );
    ExprSetIrreducible(pE);
    pE->iRightJoinTable = (i16)iRightJoinTable;
  }
  *ppExpr = sqlite3ExprAnd(pParse->db,*ppExpr, pE);
}

/*
** Set the EP_FromJoin property on all terms of the given expression.
** And set the Expr.iRightJoinTable to iTable for every term in the
** expression.
**
** The EP_FromJoin property is used on terms of an expression to tell
** the LEFT OUTER JOIN processing logic that this term is part of the
** join restriction specified in the ON or USING clause and not a part
** of the more general WHERE clause.  These terms are moved over to the
** WHERE clause during join processing but we need to remember that they
** originated in the ON or USING clause.
**
** The Expr.iRightJoinTable tells the WHERE clause processing that the
** expression depends on table iRightJoinTable even if that table is not
** explicitly mentioned in the expression.  That information is needed
** for cases like this:
**
**    SELECT * FROM t1 LEFT JOIN t2 ON t1.a=t2.b AND t1.x=5
**
** The where clause needs to defer the handling of the t1.x=5
** term until after the t2 loop of the join.  In that way, a
** NULL t2 row will be inserted whenever t1.x!=5.  If we do not
** defer the handling of t1.x=5, it will be processed immediately
** after the t1 loop and rows with t1.x!=5 will never appear in
** the output, which is incorrect.
*/
static void setJoinExpr(Expr *p, int iTable){
  while( p ){
    ExprSetProperty(p, EP_FromJoin);
    assert( !ExprHasAnyProperty(p, EP_TokenOnly|EP_Reduced) );
    ExprSetIrreducible(p);
    p->iRightJoinTable = (i16)iTable;
    setJoinExpr(p->pLeft, iTable);
    p = p->pRight;
  } 
}

/*
** This routine processes the join information for a SELECT statement.
** ON and USING clauses are converted into extra terms of the WHERE clause.
** NATURAL joins also create extra WHERE clause terms.
**
** The terms of a FROM clause are contained in the Select.pSrc structure.
** The left most table is the first entry in Select.pSrc.  The right-most
** table is the last entry.  The join operator is held in the entry to
** the left.  Thus entry 0 contains the join operator for the join between
** entries 0 and 1.  Any ON or USING clauses associated with the join are
** also attached to the left entry.
**
** This routine returns the number of errors encountered.
*/
static int sqliteProcessJoin(Parse *pParse, Select *p){
  SrcList *pSrc;                  /* All tables in the FROM clause */
  int i, j;                       /* Loop counters */
  struct SrcList_item *pLeft;     /* Left table being joined */
  struct SrcList_item *pRight;    /* Right table being joined */

  pSrc = p->pSrc;
  pLeft = &pSrc->a[0];
  pRight = &pLeft[1];
  for(i=0; i<pSrc->nSrc-1; i++, pRight++, pLeft++){
    Table *pLeftTab = pLeft->pTab;
    Table *pRightTab = pRight->pTab;
    int isOuter;

    if( NEVER(pLeftTab==0 || pRightTab==0) ) continue;
    isOuter = (pRight->jointype & JT_OUTER)!=0;

    /* When the NATURAL keyword is present, add WHERE clause terms for
    ** every column that the two tables have in common.
    */
    if( pRight->jointype & JT_NATURAL ){
      if( pRight->pOn || pRight->pUsing ){
        sqlite3ErrorMsg(pParse, "a NATURAL join may not have "
           "an ON or USING clause", 0);
        return 1;
      }
      for(j=0; j<pLeftTab->nCol; j++){
        char *zName = pLeftTab->aCol[j].zName;
        if( columnIndex(pRightTab, zName)>=0 ){
          addWhereTerm(pParse, zName, pLeftTab, pLeft->zAlias, 
                              pRightTab, pRight->zAlias,
                              pRight->iCursor, &p->pWhere, isOuter);
          
        }
      }
    }

    /* Disallow both ON and USING clauses in the same join
    */
    if( pRight->pOn && pRight->pUsing ){
      sqlite3ErrorMsg(pParse, "cannot have both ON and USING "
        "clauses in the same join");
      return 1;
    }

    /* Add the ON clause to the end of the WHERE clause, connected by
    ** an AND operator.
    */
    if( pRight->pOn ){
      if( isOuter ) setJoinExpr(pRight->pOn, pRight->iCursor);
      p->pWhere = sqlite3ExprAnd(pParse->db, p->pWhere, pRight->pOn);
      pRight->pOn = 0;
    }

    /* Create extra terms on the WHERE clause for each column named
    ** in the USING clause.  Example: If the two tables to be joined are 
    ** A and B and the USING clause names X, Y, and Z, then add this
    ** to the WHERE clause:    A.X=B.X AND A.Y=B.Y AND A.Z=B.Z
    ** Report an error if any column mentioned in the USING clause is
    ** not contained in both tables to be joined.
    */
    if( pRight->pUsing ){
      IdList *pList = pRight->pUsing;
      for(j=0; j<pList->nId; j++){
        char *zName = pList->a[j].zName;
        if( columnIndex(pLeftTab, zName)<0 || columnIndex(pRightTab, zName)<0 ){
          sqlite3ErrorMsg(pParse, "cannot join using column %s - column "
            "not present in both tables", zName);
          return 1;
        }
        addWhereTerm(pParse, zName, pLeftTab, pLeft->zAlias, 
                            pRightTab, pRight->zAlias,
                            pRight->iCursor, &p->pWhere, isOuter);
      }
    }
  }
  return 0;
}

/*
** Insert code into "v" that will push the record on the top of the
** stack into the sorter.
*/
static void pushOntoSorter(
  Parse *pParse,         /* Parser context */
  ExprList *pOrderBy,    /* The ORDER BY clause */
  Select *pSelect,       /* The whole SELECT statement */
  int regData            /* Register holding data to be sorted */
){
  Vdbe *v = pParse->pVdbe;
  int nExpr = pOrderBy->nExpr;
  int regBase = sqlite3GetTempRange(pParse, nExpr+2);
  int regRecord = sqlite3GetTempReg(pParse);
  sqlite3ExprCacheClear(pParse);
  sqlite3ExprCodeExprList(pParse, pOrderBy, regBase, 0);
  sqlite3VdbeAddOp2(v, OP_Sequence, pOrderBy->iECursor, regBase+nExpr);
  sqlite3ExprCodeMove(pParse, regData, regBase+nExpr+1, 1);
  sqlite3VdbeAddOp3(v, OP_MakeRecord, regBase, nExpr + 2, regRecord);
  sqlite3VdbeAddOp2(v, OP_IdxInsert, pOrderBy->iECursor, regRecord);
  sqlite3ReleaseTempReg(pParse, regRecord);
  sqlite3ReleaseTempRange(pParse, regBase, nExpr+2);
  if( pSelect->iLimit ){
    int addr1, addr2;
    int iLimit;
    if( pSelect->iOffset ){
      iLimit = pSelect->iOffset+1;
    }else{
      iLimit = pSelect->iLimit;
    }
    addr1 = sqlite3VdbeAddOp1(v, OP_IfZero, iLimit);
    sqlite3VdbeAddOp2(v, OP_AddImm, iLimit, -1);
    addr2 = sqlite3VdbeAddOp0(v, OP_Goto);
    sqlite3VdbeJumpHere(v, addr1);
    sqlite3VdbeAddOp1(v, OP_Last, pOrderBy->iECursor);
    sqlite3VdbeAddOp1(v, OP_Delete, pOrderBy->iECursor);
    sqlite3VdbeJumpHere(v, addr2);
    pSelect->iLimit = 0;
  }
}

/*
** Add code to implement the OFFSET
*/
static void codeOffset(
  Vdbe *v,          /* Generate code into this VM */
  Select *p,        /* The SELECT statement being coded */
  int iContinue     /* Jump here to skip the current record */
){
  if( p->iOffset && iContinue!=0 ){
    int addr;
    sqlite3VdbeAddOp2(v, OP_AddImm, p->iOffset, -1);
    addr = sqlite3VdbeAddOp1(v, OP_IfNeg, p->iOffset);
    sqlite3VdbeAddOp2(v, OP_Goto, 0, iContinue);
    VdbeComment((v, "skip OFFSET records"));
    sqlite3VdbeJumpHere(v, addr);
  }
}

/*
** Add code that will check to make sure the N registers starting at iMem
** form a distinct entry.  iTab is a sorting index that holds previously
** seen combinations of the N values.  A new entry is made in iTab
** if the current N values are new.
**
** A jump to addrRepeat is made and the N+1 values are popped from the
** stack if the top N elements are not distinct.
*/
static void codeDistinct(
  Parse *pParse,     /* Parsing and code generating context */
  int iTab,          /* A sorting index used to test for distinctness */
  int addrRepeat,    /* Jump to here if not distinct */
  int N,             /* Number of elements */
  int iMem           /* First element */
){
  Vdbe *v;
  int r1;

  v = pParse->pVdbe;
  r1 = sqlite3GetTempReg(pParse);
  sqlite3VdbeAddOp3(v, OP_MakeRecord, iMem, N, r1);
  sqlite3VdbeAddOp3(v, OP_Found, iTab, addrRepeat, r1);
  sqlite3VdbeAddOp2(v, OP_IdxInsert, iTab, r1);
  sqlite3ReleaseTempReg(pParse, r1);
}

/*
** Generate an error message when a SELECT is used within a subexpression
** (example:  "a IN (SELECT * FROM table)") but it has more than 1 result
** column.  We do this in a subroutine because the error occurs in multiple
** places.
*/
static int checkForMultiColumnSelectError(
  Parse *pParse,       /* Parse context. */
  SelectDest *pDest,   /* Destination of SELECT results */
  int nExpr            /* Number of result columns returned by SELECT */
){
  int eDest = pDest->eDest;
  if( nExpr>1 && (eDest==SRT_Mem || eDest==SRT_Set) ){
    sqlite3ErrorMsg(pParse, "only a single result allowed for "
       "a SELECT that is part of an expression");
    return 1;
  }else{
    return 0;
  }
}

/*
** This routine generates the code for the inside of the inner loop
** of a SELECT.
**
** If srcTab and nColumn are both zero, then the pEList expressions
** are evaluated in order to get the data for this row.  If nColumn>0
** then data is pulled from srcTab and pEList is used only to get the
** datatypes for each column.
*/
static void selectInnerLoop(
  Parse *pParse,          /* The parser context */
  Select *p,              /* The complete select statement being coded */
  ExprList *pEList,       /* List of values being extracted */
  int srcTab,             /* Pull data from this table */
  int nColumn,            /* Number of columns in the source table */
  ExprList *pOrderBy,     /* If not NULL, sort results using this key */
  int distinct,           /* If >=0, make sure results are distinct */
  SelectDest *pDest,      /* How to dispose of the results */
  int iContinue,          /* Jump here to continue with next row */
  int iBreak              /* Jump here to break out of the inner loop */
){
  Vdbe *v = pParse->pVdbe;
  int i;
  int hasDistinct;        /* True if the DISTINCT keyword is present */
  int regResult;              /* Start of memory holding result set */
  int eDest = pDest->eDest;   /* How to dispose of results */
  int iParm = pDest->iParm;   /* First argument to disposal method */
  int nResultCol;             /* Number of result columns */

  assert( v );
  if( NEVER(v==0) ) return;
  assert( pEList!=0 );
  hasDistinct = distinct>=0;
  if( pOrderBy==0 && !hasDistinct ){
    codeOffset(v, p, iContinue);
  }

  /* Pull the requested columns.
  */
  if( nColumn>0 ){
    nResultCol = nColumn;
  }else{
    nResultCol = pEList->nExpr;
  }
  if( pDest->iMem==0 ){
    pDest->iMem = pParse->nMem+1;
    pDest->nMem = nResultCol;
    pParse->nMem += nResultCol;
  }else{ 
    assert( pDest->nMem==nResultCol );
  }
  regResult = pDest->iMem;
  if( nColumn>0 ){
    for(i=0; i<nColumn; i++){
      sqlite3VdbeAddOp3(v, OP_Column, srcTab, i, regResult+i);
    }
  }else if( eDest!=SRT_Exists ){
    /* If the destination is an EXISTS(...) expression, the actual
    ** values returned by the SELECT are not required.
    */
    sqlite3ExprCacheClear(pParse);
    sqlite3ExprCodeExprList(pParse, pEList, regResult, eDest==SRT_Output);
  }
  nColumn = nResultCol;

  /* If the DISTINCT keyword was present on the SELECT statement
  ** and this row has been seen before, then do not make this row
  ** part of the result.
  */
  if( hasDistinct ){
    assert( pEList!=0 );
    assert( pEList->nExpr==nColumn );
    codeDistinct(pParse, distinct, iContinue, nColumn, regResult);
    if( pOrderBy==0 ){
      codeOffset(v, p, iContinue);
    }
  }

  if( checkForMultiColumnSelectError(pParse, pDest, pEList->nExpr) ){
    return;
  }

  switch( eDest ){
    /* In this mode, write each query result to the key of the temporary
    ** table iParm.
    */
#ifndef SQLITE_OMIT_COMPOUND_SELECT
    case SRT_Union: {
      int r1;
      r1 = sqlite3GetTempReg(pParse);
      sqlite3VdbeAddOp3(v, OP_MakeRecord, regResult, nColumn, r1);
      sqlite3VdbeAddOp2(v, OP_IdxInsert, iParm, r1);
      sqlite3ReleaseTempReg(pParse, r1);
      break;
    }

    /* Construct a record from the query result, but instead of
    ** saving that record, use it as a key to delete elements from
    ** the temporary table iParm.
    */
    case SRT_Except: {
      sqlite3VdbeAddOp3(v, OP_IdxDelete, iParm, regResult, nColumn);
      break;
    }
#endif

    /* Store the result as data using a unique key.
    */
    case SRT_Table:
    case SRT_EphemTab: {
      int r1 = sqlite3GetTempReg(pParse);
      testcase( eDest==SRT_Table );
      testcase( eDest==SRT_EphemTab );
      sqlite3VdbeAddOp3(v, OP_MakeRecord, regResult, nColumn, r1);
      if( pOrderBy ){
        pushOntoSorter(pParse, pOrderBy, p, r1);
      }else{
        int r2 = sqlite3GetTempReg(pParse);
        sqlite3VdbeAddOp2(v, OP_NewRowid, iParm, r2);
        sqlite3VdbeAddOp3(v, OP_Insert, iParm, r1, r2);
        sqlite3VdbeChangeP5(v, OPFLAG_APPEND);
        sqlite3ReleaseTempReg(pParse, r2);
      }
      sqlite3ReleaseTempReg(pParse, r1);
      break;
    }

#ifndef SQLITE_OMIT_SUBQUERY
    /* If we are creating a set for an "expr IN (SELECT ...)" construct,
    ** then there should be a single item on the stack.  Write this
    ** item into the set table with bogus data.
    */
    case SRT_Set: {
      assert( nColumn==1 );
      p->affinity = sqlite3CompareAffinity(pEList->a[0].pExpr, pDest->affinity);
      if( pOrderBy ){
        /* At first glance you would think we could optimize out the
        ** ORDER BY in this case since the order of entries in the set
        ** does not matter.  But there might be a LIMIT clause, in which
        ** case the order does matter */
        pushOntoSorter(pParse, pOrderBy, p, regResult);
      }else{
        int r1 = sqlite3GetTempReg(pParse);
        sqlite3VdbeAddOp4(v, OP_MakeRecord, regResult, 1, r1, &p->affinity, 1);
        sqlite3ExprCacheAffinityChange(pParse, regResult, 1);
        sqlite3VdbeAddOp2(v, OP_IdxInsert, iParm, r1);
        sqlite3ReleaseTempReg(pParse, r1);
      }
      break;
    }

    /* If any row exist in the result set, record that fact and abort.
    */
    case SRT_Exists: {
      sqlite3VdbeAddOp2(v, OP_Integer, 1, iParm);
      /* The LIMIT clause will terminate the loop for us */
      break;
    }

    /* If this is a scalar select that is part of an expression, then
    ** store the results in the appropriate memory cell and break out
    ** of the scan loop.
    */
    case SRT_Mem: {
      assert( nColumn==1 );
      if( pOrderBy ){
        pushOntoSorter(pParse, pOrderBy, p, regResult);
      }else{
        sqlite3ExprCodeMove(pParse, regResult, iParm, 1);
        /* The LIMIT clause will jump out of the loop for us */
      }
      break;
    }
#endif /* #ifndef SQLITE_OMIT_SUBQUERY */

    /* Send the data to the callback function or to a subroutine.  In the
    ** case of a subroutine, the subroutine itself is responsible for
    ** popping the data from the stack.
    */
    case SRT_Coroutine:
    case SRT_Output: {
      testcase( eDest==SRT_Coroutine );
      testcase( eDest==SRT_Output );
      if( pOrderBy ){
        int r1 = sqlite3GetTempReg(pParse);
        sqlite3VdbeAddOp3(v, OP_MakeRecord, regResult, nColumn, r1);
        pushOntoSorter(pParse, pOrderBy, p, r1);
        sqlite3ReleaseTempReg(pParse, r1);
      }else if( eDest==SRT_Coroutine ){
        sqlite3VdbeAddOp1(v, OP_Yield, pDest->iParm);
      }else{
        sqlite3VdbeAddOp2(v, OP_ResultRow, regResult, nColumn);
        sqlite3ExprCacheAffinityChange(pParse, regResult, nColumn);
      }
      break;
    }

#if !defined(SQLITE_OMIT_TRIGGER)
    /* Discard the results.  This is used for SELECT statements inside
    ** the body of a TRIGGER.  The purpose of such selects is to call
    ** user-defined functions that have side effects.  We do not care
    ** about the actual results of the select.
    */
    default: {
      assert( eDest==SRT_Discard );
      break;
    }
#endif
  }

  /* Jump to the end of the loop if the LIMIT is reached.
  */
  if( p->iLimit ){
    assert( pOrderBy==0 );  /* If there is an ORDER BY, the call to
                            ** pushOntoSorter() would have cleared p->iLimit */
    sqlite3VdbeAddOp2(v, OP_AddImm, p->iLimit, -1);
    sqlite3VdbeAddOp2(v, OP_IfZero, p->iLimit, iBreak);
  }
}

/*
** Given an expression list, generate a KeyInfo structure that records
** the collating sequence for each expression in that expression list.
**
** If the ExprList is an ORDER BY or GROUP BY clause then the resulting
** KeyInfo structure is appropriate for initializing a virtual index to
** implement that clause.  If the ExprList is the result set of a SELECT
** then the KeyInfo structure is appropriate for initializing a virtual
** index to implement a DISTINCT test.
**
** Space to hold the KeyInfo structure is obtain from malloc.  The calling
** function is responsible for seeing that this structure is eventually
** freed.  Add the KeyInfo structure to the P4 field of an opcode using
** P4_KEYINFO_HANDOFF is the usual way of dealing with this.
*/
static KeyInfo *keyInfoFromExprList(Parse *pParse, ExprList *pList){
  sqlite3 *db = pParse->db;
  int nExpr;
  KeyInfo *pInfo;
  struct ExprList_item *pItem;
  int i;

  nExpr = pList->nExpr;
  pInfo = sqlite3DbMallocZero(db, sizeof(*pInfo) + nExpr*(sizeof(CollSeq*)+1) );
  if( pInfo ){
    pInfo->aSortOrder = (u8*)&pInfo->aColl[nExpr];
    pInfo->nField = (u16)nExpr;
    pInfo->enc = ENC(db);
    pInfo->db = db;
    for(i=0, pItem=pList->a; i<nExpr; i++, pItem++){
      CollSeq *pColl;
      pColl = sqlite3ExprCollSeq(pParse, pItem->pExpr);
      if( !pColl ){
        pColl = db->pDfltColl;
      }
      pInfo->aColl[i] = pColl;
      pInfo->aSortOrder[i] = pItem->sortOrder;
    }
  }
  return pInfo;
}


/*
** If the inner loop was generated using a non-null pOrderBy argument,
** then the results were placed in a sorter.  After the loop is terminated
** we need to run the sorter and output the results.  The following
** routine generates the code needed to do that.
*/
static void generateSortTail(
  Parse *pParse,    /* Parsing context */
  Select *p,        /* The SELECT statement */
  Vdbe *v,          /* Generate code into this VDBE */
  int nColumn,      /* Number of columns of data */
  SelectDest *pDest /* Write the sorted results here */
){
  int addrBreak = sqlite3VdbeMakeLabel(v);     /* Jump here to exit loop */
  int addrContinue = sqlite3VdbeMakeLabel(v);  /* Jump here for next cycle */
  int addr;
  int iTab;
  int pseudoTab = 0;
  ExprList *pOrderBy = p->pOrderBy;

  int eDest = pDest->eDest;
  int iParm = pDest->iParm;

  int regRow;
  int regRowid;

  iTab = pOrderBy->iECursor;
  regRow = sqlite3GetTempReg(pParse);
  if( eDest==SRT_Output || eDest==SRT_Coroutine ){
    pseudoTab = pParse->nTab++;
    sqlite3VdbeAddOp3(v, OP_OpenPseudo, pseudoTab, regRow, nColumn);
    regRowid = 0;
  }else{
    regRowid = sqlite3GetTempReg(pParse);
  }
  addr = 1 + sqlite3VdbeAddOp2(v, OP_Sort, iTab, addrBreak);
  codeOffset(v, p, addrContinue);
  sqlite3VdbeAddOp3(v, OP_Column, iTab, pOrderBy->nExpr + 1, regRow);
  switch( eDest ){
    case SRT_Table:
    case SRT_EphemTab: {
      testcase( eDest==SRT_Table );
      testcase( eDest==SRT_EphemTab );
      sqlite3VdbeAddOp2(v, OP_NewRowid, iParm, regRowid);
      sqlite3VdbeAddOp3(v, OP_Insert, iParm, regRow, regRowid);
      sqlite3VdbeChangeP5(v, OPFLAG_APPEND);
      break;
    }
#ifndef SQLITE_OMIT_SUBQUERY
    case SRT_Set: {
      assert( nColumn==1 );
      sqlite3VdbeAddOp4(v, OP_MakeRecord, regRow, 1, regRowid, &p->affinity, 1);
      sqlite3ExprCacheAffinityChange(pParse, regRow, 1);
      sqlite3VdbeAddOp2(v, OP_IdxInsert, iParm, regRowid);
      break;
    }
    case SRT_Mem: {
      assert( nColumn==1 );
      sqlite3ExprCodeMove(pParse, regRow, iParm, 1);
      /* The LIMIT clause will terminate the loop for us */
      break;
    }
#endif
    default: {
      int i;
      assert( eDest==SRT_Output || eDest==SRT_Coroutine ); 
      testcase( eDest==SRT_Output );
      testcase( eDest==SRT_Coroutine );
      for(i=0; i<nColumn; i++){
        assert( regRow!=pDest->iMem+i );
        sqlite3VdbeAddOp3(v, OP_Column, pseudoTab, i, pDest->iMem+i);
        if( i==0 ){
          sqlite3VdbeChangeP5(v, OPFLAG_CLEARCACHE);
        }
      }
      if( eDest==SRT_Output ){
        sqlite3VdbeAddOp2(v, OP_ResultRow, pDest->iMem, nColumn);
        sqlite3ExprCacheAffinityChange(pParse, pDest->iMem, nColumn);
      }else{
        sqlite3VdbeAddOp1(v, OP_Yield, pDest->iParm);
      }
      break;
    }
  }
  sqlite3ReleaseTempReg(pParse, regRow);
  sqlite3ReleaseTempReg(pParse, regRowid);

  /* LIMIT has been implemented by the pushOntoSorter() routine.
  */
  assert( p->iLimit==0 );

  /* The bottom of the loop
  */
  sqlite3VdbeResolveLabel(v, addrContinue);
  sqlite3VdbeAddOp2(v, OP_Next, iTab, addr);
  sqlite3VdbeResolveLabel(v, addrBreak);
  if( eDest==SRT_Output || eDest==SRT_Coroutine ){
    sqlite3VdbeAddOp2(v, OP_Close, pseudoTab, 0);
  }
}

/*
** Return a pointer to a string containing the 'declaration type' of the
** expression pExpr. The string may be treated as static by the caller.
**
** The declaration type is the exact datatype definition extracted from the
** original CREATE TABLE statement if the expression is a column. The
** declaration type for a ROWID field is INTEGER. Exactly when an expression
** is considered a column can be complex in the presence of subqueries. The
** result-set expression in all of the following SELECT statements is 
** considered a column by this function.
**
**   SELECT col FROM tbl;
**   SELECT (SELECT col FROM tbl;
**   SELECT (SELECT col FROM tbl);
**   SELECT abc FROM (SELECT col AS abc FROM tbl);
** 
** The declaration type for any expression other than a column is NULL.
*/
static const char *columnType(
  NameContext *pNC, 
  Expr *pExpr,
  const char **pzOriginDb,
  const char **pzOriginTab,
  const char **pzOriginCol
){
  char const *zType = 0;
  char const *zOriginDb = 0;
  char const *zOriginTab = 0;
  char const *zOriginCol = 0;
  int j;
  if( NEVER(pExpr==0) || pNC->pSrcList==0 ) return 0;

  switch( pExpr->op ){
    case TK_AGG_COLUMN:
    case TK_COLUMN: {
      /* The expression is a column. Locate the table the column is being
      ** extracted from in NameContext.pSrcList. This table may be real
      ** database table or a subquery.
      */
      Table *pTab = 0;            /* Table structure column is extracted from */
      Select *pS = 0;             /* Select the column is extracted from */
      int iCol = pExpr->iColumn;  /* Index of column in pTab */
      testcase( pExpr->op==TK_AGG_COLUMN );
      testcase( pExpr->op==TK_COLUMN );
      while( pNC && !pTab ){
        SrcList *pTabList = pNC->pSrcList;
        for(j=0;j<pTabList->nSrc && pTabList->a[j].iCursor!=pExpr->iTable;j++);
        if( j<pTabList->nSrc ){
          pTab = pTabList->a[j].pTab;
          pS = pTabList->a[j].pSelect;
        }else{
          pNC = pNC->pNext;
        }
      }

      if( pTab==0 ){
        /* At one time, code such as "SELECT new.x" within a trigger would
        ** cause this condition to run.  Since then, we have restructured how
        ** trigger code is generated and so this condition is no longer 
        ** possible. However, it can still be true for statements like
        ** the following:
        **
        **   CREATE TABLE t1(col INTEGER);
        **   SELECT (SELECT t1.col) FROM FROM t1;
        **
        ** when columnType() is called on the expression "t1.col" in the 
        ** sub-select. In this case, set the column type to NULL, even
        ** though it should really be "INTEGER".
        **
        ** This is not a problem, as the column type of "t1.col" is never
        ** used. When columnType() is called on the expression 
        ** "(SELECT t1.col)", the correct type is returned (see the TK_SELECT
        ** branch below.  */
        break;
      }

      assert( pTab && pExpr->pTab==pTab );
      if( pS ){
        /* The "table" is actually a sub-select or a view in the FROM clause
        ** of the SELECT statement. Return the declaration type and origin
        ** data for the result-set column of the sub-select.
        */
        if( ALWAYS(iCol>=0 && iCol<pS->pEList->nExpr) ){
          /* If iCol is less than zero, then the expression requests the
          ** rowid of the sub-select or view. This expression is legal (see 
          ** test case misc2.2.2) - it always evaluates to NULL.
          */
          NameContext sNC;
          Expr *p = pS->pEList->a[iCol].pExpr;
          sNC.pSrcList = pS->pSrc;
          sNC.pNext = pNC;
          sNC.pParse = pNC->pParse;
          zType = columnType(&sNC, p, &zOriginDb, &zOriginTab, &zOriginCol); 
        }
      }else if( ALWAYS(pTab->pSchema) ){
        /* A real table */
        assert( !pS );
        if( iCol<0 ) iCol = pTab->iPKey;
        assert( iCol==-1 || (iCol>=0 && iCol<pTab->nCol) );
        if( iCol<0 ){
          zType = "INTEGER";
          zOriginCol = "rowid";
        }else{
          zType = pTab->aCol[iCol].zType;
          zOriginCol = pTab->aCol[iCol].zName;
        }
        zOriginTab = pTab->zName;
        if( pNC->pParse ){
          int iDb = sqlite3SchemaToIndex(pNC->pParse->db, pTab->pSchema);
          zOriginDb = pNC->pParse->db->aDb[iDb].zName;
        }
      }
      break;
    }
#ifndef SQLITE_OMIT_SUBQUERY
    case TK_SELECT: {
      /* The expression is a sub-select. Return the declaration type and
      ** origin info for the single column in the result set of the SELECT
      ** statement.
      */
      NameContext sNC;
      Select *pS = pExpr->x.pSelect;
      Expr *p = pS->pEList->a[0].pExpr;
      assert( ExprHasProperty(pExpr, EP_xIsSelect) );
      sNC.pSrcList = pS->pSrc;
      sNC.pNext = pNC;
      sNC.pParse = pNC->pParse;
      zType = columnType(&sNC, p, &zOriginDb, &zOriginTab, &zOriginCol); 
      break;
    }
#endif
  }
  
  if( pzOriginDb ){
    assert( pzOriginTab && pzOriginCol );
    *pzOriginDb = zOriginDb;
    *pzOriginTab = zOriginTab;
    *pzOriginCol = zOriginCol;
  }
  return zType;
}

/*
** Generate code that will tell the VDBE the declaration types of columns
** in the result set.
*/
static void generateColumnTypes(
  Parse *pParse,      /* Parser context */
  SrcList *pTabList,  /* List of tables */
  ExprList *pEList    /* Expressions defining the result set */
){
#ifndef SQLITE_OMIT_DECLTYPE
  Vdbe *v = pParse->pVdbe;
  int i;
  NameContext sNC;
  sNC.pSrcList = pTabList;
  sNC.pParse = pParse;
  for(i=0; i<pEList->nExpr; i++){
    Expr *p = pEList->a[i].pExpr;
    const char *zType;
#ifdef SQLITE_ENABLE_COLUMN_METADATA
    const char *zOrigDb = 0;
    const char *zOrigTab = 0;
    const char *zOrigCol = 0;
    zType = columnType(&sNC, p, &zOrigDb, &zOrigTab, &zOrigCol);

    /* The vdbe must make its own copy of the column-type and other 
    ** column specific strings, in case the schema is reset before this
    ** virtual machine is deleted.
    */
    sqlite3VdbeSetColName(v, i, COLNAME_DATABASE, zOrigDb, SQLITE_TRANSIENT);
    sqlite3VdbeSetColName(v, i, COLNAME_TABLE, zOrigTab, SQLITE_TRANSIENT);
    sqlite3VdbeSetColName(v, i, COLNAME_COLUMN, zOrigCol, SQLITE_TRANSIENT);
#else
    zType = columnType(&sNC, p, 0, 0, 0);
#endif
    sqlite3VdbeSetColName(v, i, COLNAME_DECLTYPE, zType, SQLITE_TRANSIENT);
  }
#endif /* SQLITE_OMIT_DECLTYPE */
}

/*
** Generate code that will tell the VDBE the names of columns
** in the result set.  This information is used to provide the
** azCol[] values in the callback.
*/
static void generateColumnNames(
  Parse *pParse,      /* Parser context */
  SrcList *pTabList,  /* List of tables */
  ExprList *pEList    /* Expressions defining the result set */
){
  Vdbe *v = pParse->pVdbe;
  int i, j;
  sqlite3 *db = pParse->db;
  int fullNames, shortNames;

#ifndef SQLITE_OMIT_EXPLAIN
  /* If this is an EXPLAIN, skip this step */
  if( pParse->explain ){
    return;
  }
#endif

  if( pParse->colNamesSet || NEVER(v==0) || db->mallocFailed ) return;
  pParse->colNamesSet = 1;
  fullNames = (db->flags & SQLITE_FullColNames)!=0;
  shortNames = (db->flags & SQLITE_ShortColNames)!=0;
  sqlite3VdbeSetNumCols(v, pEList->nExpr);
  for(i=0; i<pEList->nExpr; i++){
    Expr *p;
    p = pEList->a[i].pExpr;
    if( NEVER(p==0) ) continue;
    if( pEList->a[i].zName ){
      char *zName = pEList->a[i].zName;
      sqlite3VdbeSetColName(v, i, COLNAME_NAME, zName, SQLITE_TRANSIENT);
    }else if( (p->op==TK_COLUMN || p->op==TK_AGG_COLUMN) && pTabList ){
      Table *pTab;
      char *zCol;
      int iCol = p->iColumn;
      for(j=0; ALWAYS(j<pTabList->nSrc); j++){
        if( pTabList->a[j].iCursor==p->iTable ) break;
      }
      assert( j<pTabList->nSrc );
      pTab = pTabList->a[j].pTab;
      if( iCol<0 ) iCol = pTab->iPKey;
      assert( iCol==-1 || (iCol>=0 && iCol<pTab->nCol) );
      if( iCol<0 ){
        zCol = "rowid";
      }else{
        zCol = pTab->aCol[iCol].zName;
      }
      if( !shortNames && !fullNames ){
        sqlite3VdbeSetColName(v, i, COLNAME_NAME, 
            sqlite3DbStrDup(db, pEList->a[i].zSpan), SQLITE_DYNAMIC);
      }else if( fullNames ){
        char *zName = 0;
        zName = sqlite3MPrintf(db, "%s.%s", pTab->zName, zCol);
        sqlite3VdbeSetColName(v, i, COLNAME_NAME, zName, SQLITE_DYNAMIC);
      }else{
        sqlite3VdbeSetColName(v, i, COLNAME_NAME, zCol, SQLITE_TRANSIENT);
      }
    }else{
      sqlite3VdbeSetColName(v, i, COLNAME_NAME, 
          sqlite3DbStrDup(db, pEList->a[i].zSpan), SQLITE_DYNAMIC);
    }
  }
  generateColumnTypes(pParse, pTabList, pEList);
}

#ifndef SQLITE_OMIT_COMPOUND_SELECT
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
#endif /* SQLITE_OMIT_COMPOUND_SELECT */

/*
** Given a an expression list (which is really the list of expressions
** that form the result set of a SELECT statement) compute appropriate
** column names for a table that would hold the expression list.
**
** All column names will be unique.
**
** Only the column names are computed.  Column.zType, Column.zColl,
** and other fields of Column are zeroed.
**
** Return SQLITE_OK on success.  If a memory allocation error occurs,
** store NULL in *paCol and 0 in *pnCol and return SQLITE_NOMEM.
*/
static int selectColumnsFromExprList(
  Parse *pParse,          /* Parsing context */
  ExprList *pEList,       /* Expr list from which to derive column names */
  int *pnCol,             /* Write the number of columns here */
  Column **paCol          /* Write the new column list here */
){
  sqlite3 *db = pParse->db;   /* Database connection */
  int i, j;                   /* Loop counters */
  int cnt;                    /* Index added to make the name unique */
  Column *aCol, *pCol;        /* For looping over result columns */
  int nCol;                   /* Number of columns in the result set */
  Expr *p;                    /* Expression for a single result column */
  char *zName;                /* Column name */
  int nName;                  /* Size of name in zName[] */

  *pnCol = nCol = pEList->nExpr;
  aCol = *paCol = sqlite3DbMallocZero(db, sizeof(aCol[0])*nCol);
  if( aCol==0 ) return SQLITE_NOMEM;
  for(i=0, pCol=aCol; i<nCol; i++, pCol++){
    /* Get an appropriate name for the column
    */
    p = pEList->a[i].pExpr;
    assert( p->pRight==0 || ExprHasProperty(p->pRight, EP_IntValue)
               || p->pRight->u.zToken==0 || p->pRight->u.zToken[0]!=0 );
    if( (zName = pEList->a[i].zName)!=0 ){
      /* If the column contains an "AS <name>" phrase, use <name> as the name */
      zName = sqlite3DbStrDup(db, zName);
    }else{
      Expr *pColExpr = p;  /* The expression that is the result column name */
      Table *pTab;         /* Table associated with this expression */
      while( pColExpr->op==TK_DOT ) pColExpr = pColExpr->pRight;
      if( pColExpr->op==TK_COLUMN && ALWAYS(pColExpr->pTab!=0) ){
        /* For columns use the column name name */
        int iCol = pColExpr->iColumn;
        pTab = pColExpr->pTab;
        if( iCol<0 ) iCol = pTab->iPKey;
        zName = sqlite3MPrintf(db, "%s",
                 iCol>=0 ? pTab->aCol[iCol].zName : "rowid");
      }else if( pColExpr->op==TK_ID ){
        assert( !ExprHasProperty(pColExpr, EP_IntValue) );
        zName = sqlite3MPrintf(db, "%s", pColExpr->u.zToken);
      }else{
        /* Use the original text of the column expression as its name */
        zName = sqlite3MPrintf(db, "%s", pEList->a[i].zSpan);
      }
    }
    if( db->mallocFailed ){
      sqlite3DbFree(db, zName);
      break;
    }

    /* Make sure the column name is unique.  If the name is not unique,
    ** append a integer to the name so that it becomes unique.
    */
    nName = sqlite3Strlen30(zName);
    for(j=cnt=0; j<i; j++){
      if( sqlite3StrICmp(aCol[j].zName, zName)==0 ){
        char *zNewName;
        zName[nName] = 0;
        zNewName = sqlite3MPrintf(db, "%s:%d", zName, ++cnt);
        sqlite3DbFree(db, zName);
        zName = zNewName;
        j = -1;
        if( zName==0 ) break;
      }
    }
    pCol->zName = zName;
  }
  if( db->mallocFailed ){
    for(j=0; j<i; j++){
      sqlite3DbFree(db, aCol[j].zName);
    }
    sqlite3DbFree(db, aCol);
    *paCol = 0;
    *pnCol = 0;
    return SQLITE_NOMEM;
  }
  return SQLITE_OK;
}

/*
** Add type and collation information to a column list based on
** a SELECT statement.
** 
** The column list presumably came from selectColumnNamesFromExprList().
** The column list has only names, not types or collations.  This
** routine goes through and adds the types and collations.
**
** This routine requires that all identifiers in the SELECT
** statement be resolved.
*/
static void selectAddColumnTypeAndCollation(
  Parse *pParse,        /* Parsing contexts */
  int nCol,             /* Number of columns */
  Column *aCol,         /* List of columns */
  Select *pSelect       /* SELECT used to determine types and collations */
){
  sqlite3 *db = pParse->db;
  NameContext sNC;
  Column *pCol;
  CollSeq *pColl;
  int i;
  Expr *p;
  struct ExprList_item *a;

  assert( pSelect!=0 );
  assert( (pSelect->selFlags & SF_Resolved)!=0 );
  assert( nCol==pSelect->pEList->nExpr || db->mallocFailed );
  if( db->mallocFailed ) return;
  memset(&sNC, 0, sizeof(sNC));
  sNC.pSrcList = pSelect->pSrc;
  a = pSelect->pEList->a;
  for(i=0, pCol=aCol; i<nCol; i++, pCol++){
    p = a[i].pExpr;
    pCol->zType = sqlite3DbStrDup(db, columnType(&sNC, p, 0, 0, 0));
    pCol->affinity = sqlite3ExprAffinity(p);
    if( pCol->affinity==0 ) pCol->affinity = SQLITE_AFF_NONE;
    pColl = sqlite3ExprCollSeq(pParse, p);
    if( pColl ){
      pCol->zColl = sqlite3DbStrDup(db, pColl->zName);
    }
  }
}

/*
** Given a SELECT statement, generate a Table structure that describes
** the result set of that SELECT.
*/
Table *sqlite3ResultSetOfSelect(Parse *pParse, Select *pSelect){
  Table *pTab;
  sqlite3 *db = pParse->db;
  int savedFlags;

  savedFlags = db->flags;
  db->flags &= ~SQLITE_FullColNames;
  db->flags |= SQLITE_ShortColNames;
  sqlite3SelectPrep(pParse, pSelect, 0);
  if( pParse->nErr ) return 0;
  while( pSelect->pPrior ) pSelect = pSelect->pPrior;
  db->flags = savedFlags;
  pTab = sqlite3DbMallocZero(db, sizeof(Table) );
  if( pTab==0 ){
    return 0;
  }
  /* The sqlite3ResultSetOfSelect() is only used n contexts where lookaside
  ** is disabled, so we might as well hard-code pTab->dbMem to NULL. */
  assert( db->lookaside.bEnabled==0 );
  pTab->dbMem = 0;
  pTab->nRef = 1;
  pTab->zName = 0;
  selectColumnsFromExprList(pParse, pSelect->pEList, &pTab->nCol, &pTab->aCol);
  selectAddColumnTypeAndCollation(pParse, pTab->nCol, pTab->aCol, pSelect);
  pTab->iPKey = -1;
  if( db->mallocFailed ){
    sqlite3DeleteTable(pTab);
    return 0;
  }
  return pTab;
}

/*
** Get a VDBE for the given parser context.  Create a new one if necessary.
** If an error occurs, return NULL and leave a message in pParse.
*/
Vdbe *sqlite3GetVdbe(Parse *pParse){
  Vdbe *v = pParse->pVdbe;
  if( v==0 ){
    v = pParse->pVdbe = sqlite3VdbeCreate(pParse->db);
#ifndef SQLITE_OMIT_TRACE
    if( v ){
      sqlite3VdbeAddOp0(v, OP_Trace);
    }
#endif
  }
  return v;
}


/*
** Compute the iLimit and iOffset fields of the SELECT based on the
** pLimit and pOffset expressions.  pLimit and pOffset hold the expressions
** that appear in the original SQL statement after the LIMIT and OFFSET
** keywords.  Or NULL if those keywords are omitted. iLimit and iOffset 
** are the integer memory register numbers for counters used to compute 
** the limit and offset.  If there is no limit and/or offset, then 
** iLimit and iOffset are negative.
**
** This routine changes the values of iLimit and iOffset only if
** a limit or offset is defined by pLimit and pOffset.  iLimit and
** iOffset should have been preset to appropriate default values
** (usually but not always -1) prior to calling this routine.
** Only if pLimit!=0 or pOffset!=0 do the limit registers get
** redefined.  The UNION ALL operator uses this property to force
** the reuse of the same limit and offset registers across multiple
** SELECT statements.
*/
static void computeLimitRegisters(Parse *pParse, Select *p, int iBreak){
  Vdbe *v = 0;
  int iLimit = 0;
  int iOffset;
  int addr1;
  if( p->iLimit ) return;

  /* 
  ** "LIMIT -1" always shows all rows.  There is some
  ** contraversy about what the correct behavior should be.
  ** The current implementation interprets "LIMIT 0" to mean
  ** no rows.
  */
  sqlite3ExprCacheClear(pParse);
  assert( p->pOffset==0 || p->pLimit!=0 );
  if( p->pLimit ){
    p->iLimit = iLimit = ++pParse->nMem;
    v = sqlite3GetVdbe(pParse);
    if( NEVER(v==0) ) return;  /* VDBE should have already been allocated */
    sqlite3ExprCode(pParse, p->pLimit, iLimit);
    sqlite3VdbeAddOp1(v, OP_MustBeInt, iLimit);
    VdbeComment((v, "LIMIT counter"));
    sqlite3VdbeAddOp2(v, OP_IfZero, iLimit, iBreak);
    if( p->pOffset ){
      p->iOffset = iOffset = ++pParse->nMem;
      pParse->nMem++;   /* Allocate an extra register for limit+offset */
      sqlite3ExprCode(pParse, p->pOffset, iOffset);
      sqlite3VdbeAddOp1(v, OP_MustBeInt, iOffset);
      VdbeComment((v, "OFFSET counter"));
      addr1 = sqlite3VdbeAddOp1(v, OP_IfPos, iOffset);
      sqlite3VdbeAddOp2(v, OP_Integer, 0, iOffset);
      sqlite3VdbeJumpHere(v, addr1);
      sqlite3VdbeAddOp3(v, OP_Add, iLimit, iOffset, iOffset+1);
      VdbeComment((v, "LIMIT+OFFSET"));
      addr1 = sqlite3VdbeAddOp1(v, OP_IfPos, iLimit);
      sqlite3VdbeAddOp2(v, OP_Integer, -1, iOffset+1);
      sqlite3VdbeJumpHere(v, addr1);
    }
  }
}

#ifndef SQLITE_OMIT_COMPOUND_SELECT
/*
** Return the appropriate collating sequence for the iCol-th column of
** the result set for the compound-select statement "p".  Return NULL if
** the column has no default collating sequence.
**
** The collating sequence for the compound select is taken from the
** left-most term of the select that has a collating sequence.
*/
static CollSeq *multiSelectCollSeq(Parse *pParse, Select *p, int iCol){
  CollSeq *pRet;
  if( p->pPrior ){
    pRet = multiSelectCollSeq(pParse, p->pPrior, iCol);
  }else{
    pRet = 0;
  }
  assert( iCol>=0 );
  if( pRet==0 && iCol<p->pEList->nExpr ){
    pRet = sqlite3ExprCollSeq(pParse, p->pEList->a[iCol].pExpr);
  }
  return pRet;
}
#endif /* SQLITE_OMIT_COMPOUND_SELECT */

/* Forward reference */
static int multiSelectOrderBy(
  Parse *pParse,        /* Parsing context */
  Select *p,            /* The right-most of SELECTs to be coded */
  SelectDest *pDest     /* What to do with query results */
);


#ifndef SQLITE_OMIT_COMPOUND_SELECT
/*
** This routine is called to process a compound query form from
** two or more separate queries using UNION, UNION ALL, EXCEPT, or
** INTERSECT
**
** "p" points to the right-most of the two queries.  the query on the
** left is p->pPrior.  The left query could also be a compound query
** in which case this routine will be called recursively. 
**
** The results of the total query are to be written into a destination
** of type eDest with parameter iParm.
**
** Example 1:  Consider a three-way compound SQL statement.
**
**     SELECT a FROM t1 UNION SELECT b FROM t2 UNION SELECT c FROM t3
**
** This statement is parsed up as follows:
**
**     SELECT c FROM t3
**      |
**      `----->  SELECT b FROM t2
**                |
**                `------>  SELECT a FROM t1
**
** The arrows in the diagram above represent the Select.pPrior pointer.
** So if this routine is called with p equal to the t3 query, then
** pPrior will be the t2 query.  p->op will be TK_UNION in this case.
**
** Notice that because of the way SQLite parses compound SELECTs, the
** individual selects always group from left to right.
*/
static int multiSelect(
  Parse *pParse,        /* Parsing context */
  Select *p,            /* The right-most of SELECTs to be coded */
  SelectDest *pDest     /* What to do with query results */
){
  int rc = SQLITE_OK;   /* Success code from a subroutine */
  Select *pPrior;       /* Another SELECT immediately to our left */
  Vdbe *v;              /* Generate code to this VDBE */
  SelectDest dest;      /* Alternative data destination */
  Select *pDelete = 0;  /* Chain of simple selects to delete */
  sqlite3 *db;          /* Database connection */

  /* Make sure there is no ORDER BY or LIMIT clause on prior SELECTs.  Only
  ** the last (right-most) SELECT in the series may have an ORDER BY or LIMIT.
  */
  assert( p && p->pPrior );  /* Calling function guarantees this much */
  db = pParse->db;
  pPrior = p->pPrior;
  assert( pPrior->pRightmost!=pPrior );
  assert( pPrior->pRightmost==p->pRightmost );
  dest = *pDest;
  if( pPrior->pOrderBy ){
    sqlite3ErrorMsg(pParse,"ORDER BY clause should come after %s not before",
      selectOpName(p->op));
    rc = 1;
    goto multi_select_end;
  }
  if( pPrior->pLimit ){
    sqlite3ErrorMsg(pParse,"LIMIT clause should come after %s not before",
      selectOpName(p->op));
    rc = 1;
    goto multi_select_end;
  }

  v = sqlite3GetVdbe(pParse);
  assert( v!=0 );  /* The VDBE already created by calling function */

  /* Create the destination temporary table if necessary
  */
  if( dest.eDest==SRT_EphemTab ){
    assert( p->pEList );
    sqlite3VdbeAddOp2(v, OP_OpenEphemeral, dest.iParm, p->pEList->nExpr);
    dest.eDest = SRT_Table;
  }

  /* Make sure all SELECTs in the statement have the same number of elements
  ** in their result sets.
  */
  assert( p->pEList && pPrior->pEList );
  if( p->pEList->nExpr!=pPrior->pEList->nExpr ){
    sqlite3ErrorMsg(pParse, "SELECTs to the left and right of %s"
      " do not have the same number of result columns", selectOpName(p->op));
    rc = 1;
    goto multi_select_end;
  }

  /* Compound SELECTs that have an ORDER BY clause are handled separately.
  */
  if( p->pOrderBy ){
    return multiSelectOrderBy(pParse, p, pDest);
  }

  /* Generate code for the left and right SELECT statements.
  */
  switch( p->op ){
    case TK_ALL: {
      int addr = 0;
      assert( !pPrior->pLimit );
      pPrior->pLimit = p->pLimit;
      pPrior->pOffset = p->pOffset;
      rc = sqlite3Select(pParse, pPrior, &dest);
      p->pLimit = 0;
      p->pOffset = 0;
      if( rc ){
        goto multi_select_end;
      }
      p->pPrior = 0;
      p->iLimit = pPrior->iLimit;
      p->iOffset = pPrior->iOffset;
      if( p->iLimit ){
        addr = sqlite3VdbeAddOp1(v, OP_IfZero, p->iLimit);
        VdbeComment((v, "Jump ahead if LIMIT reached"));
      }
      rc = sqlite3Select(pParse, p, &dest);
      testcase( rc!=SQLITE_OK );
      pDelete = p->pPrior;
      p->pPrior = pPrior;
      if( addr ){
        sqlite3VdbeJumpHere(v, addr);
      }
      break;
    }
    case TK_EXCEPT:
    case TK_UNION: {
      int unionTab;    /* Cursor number of the temporary table holding result */
      u8 op = 0;       /* One of the SRT_ operations to apply to self */
      int priorOp;     /* The SRT_ operation to apply to prior selects */
      Expr *pLimit, *pOffset; /* Saved values of p->nLimit and p->nOffset */
      int addr;
      SelectDest uniondest;

      testcase( p->op==TK_EXCEPT );
      testcase( p->op==TK_UNION );
      priorOp = SRT_Union;
      if( dest.eDest==priorOp && ALWAYS(!p->pLimit &&!p->pOffset) ){
        /* We can reuse a temporary table generated by a SELECT to our
        ** right.
        */
        assert( p->pRightmost!=p );  /* Can only happen for leftward elements
                                     ** of a 3-way or more compound */
        assert( p->pLimit==0 );      /* Not allowed on leftward elements */
        assert( p->pOffset==0 );     /* Not allowed on leftward elements */
        unionTab = dest.iParm;
      }else{
        /* We will need to create our own temporary table to hold the
        ** intermediate results.
        */
        unionTab = pParse->nTab++;
        assert( p->pOrderBy==0 );
        addr = sqlite3VdbeAddOp2(v, OP_OpenEphemeral, unionTab, 0);
        assert( p->addrOpenEphm[0] == -1 );
        p->addrOpenEphm[0] = addr;
        p->pRightmost->selFlags |= SF_UsesEphemeral;
        assert( p->pEList );
      }

      /* Code the SELECT statements to our left
      */
      assert( !pPrior->pOrderBy );
      sqlite3SelectDestInit(&uniondest, priorOp, unionTab);
      rc = sqlite3Select(pParse, pPrior, &uniondest);
      if( rc ){
        goto multi_select_end;
      }

      /* Code the current SELECT statement
      */
      if( p->op==TK_EXCEPT ){
        op = SRT_Except;
      }else{
        assert( p->op==TK_UNION );
        op = SRT_Union;
      }
      p->pPrior = 0;
      pLimit = p->pLimit;
      p->pLimit = 0;
      pOffset = p->pOffset;
      p->pOffset = 0;
      uniondest.eDest = op;
      rc = sqlite3Select(pParse, p, &uniondest);
      testcase( rc!=SQLITE_OK );
      /* Query flattening in sqlite3Select() might refill p->pOrderBy.
      ** Be sure to delete p->pOrderBy, therefore, to avoid a memory leak. */
      sqlite3ExprListDelete(db, p->pOrderBy);
      pDelete = p->pPrior;
      p->pPrior = pPrior;
      p->pOrderBy = 0;
      sqlite3ExprDelete(db, p->pLimit);
      p->pLimit = pLimit;
      p->pOffset = pOffset;
      p->iLimit = 0;
      p->iOffset = 0;

      /* Convert the data in the temporary table into whatever form
      ** it is that we currently need.
      */
      assert( unionTab==dest.iParm || dest.eDest!=priorOp );
      if( dest.eDest!=priorOp ){
        int iCont, iBreak, iStart;
        assert( p->pEList );
        if( dest.eDest==SRT_Output ){
          Select *pFirst = p;
          while( pFirst->pPrior ) pFirst = pFirst->pPrior;
          generateColumnNames(pParse, 0, pFirst->pEList);
        }
        iBreak = sqlite3VdbeMakeLabel(v);
        iCont = sqlite3VdbeMakeLabel(v);
        computeLimitRegisters(pParse, p, iBreak);
        sqlite3VdbeAddOp2(v, OP_Rewind, unionTab, iBreak);
        iStart = sqlite3VdbeCurrentAddr(v);
        selectInnerLoop(pParse, p, p->pEList, unionTab, p->pEList->nExpr,
                        0, -1, &dest, iCont, iBreak);
        sqlite3VdbeResolveLabel(v, iCont);
        sqlite3VdbeAddOp2(v, OP_Next, unionTab, iStart);
        sqlite3VdbeResolveLabel(v, iBreak);
        sqlite3VdbeAddOp2(v, OP_Close, unionTab, 0);
      }
      break;
    }
    default: assert( p->op==TK_INTERSECT ); {
      int tab1, tab2;
      int iCont, iBreak, iStart;
      Expr *pLimit, *pOffset;
      int addr;
      SelectDest intersectdest;
      int r1;

      /* INTERSECT is different from the others since it requires
      ** two temporary tables.  Hence it has its own case.  Begin
      ** by allocating the tables we will need.
      */
      tab1 = pParse->nTab++;
      tab2 = pParse->nTab++;
      assert( p->pOrderBy==0 );

      addr = sqlite3VdbeAddOp2(v, OP_OpenEphemeral, tab1, 0);
      assert( p->addrOpenEphm[0] == -1 );
      p->addrOpenEphm[0] = addr;
      p->pRightmost->selFlags |= SF_UsesEphemeral;
      assert( p->pEList );

      /* Code the SELECTs to our left into temporary table "tab1".
      */
      sqlite3SelectDestInit(&intersectdest, SRT_Union, tab1);
      rc = sqlite3Select(pParse, pPrior, &intersectdest);
      if( rc ){
        goto multi_select_end;
      }

      /* Code the current SELECT into temporary table "tab2"
      */
      addr = sqlite3VdbeAddOp2(v, OP_OpenEphemeral, tab2, 0);
      assert( p->addrOpenEphm[1] == -1 );
      p->addrOpenEphm[1] = addr;
      p->pPrior = 0;
      pLimit = p->pLimit;
      p->pLimit = 0;
      pOffset = p->pOffset;
      p->pOffset = 0;
      intersectdest.iParm = tab2;
      rc = sqlite3Select(pParse, p, &intersectdest);
      testcase( rc!=SQLITE_OK );
      pDelete = p->pPrior;
      p->pPrior = pPrior;
      sqlite3ExprDelete(db, p->pLimit);
      p->pLimit = pLimit;
      p->pOffset = pOffset;

      /* Generate code to take the intersection of the two temporary
      ** tables.
      */
      assert( p->pEList );
      if( dest.eDest==SRT_Output ){
        Select *pFirst = p;
        while( pFirst->pPrior ) pFirst = pFirst->pPrior;
        generateColumnNames(pParse, 0, pFirst->pEList);
      }
      iBreak = sqlite3VdbeMakeLabel(v);
      iCont = sqlite3VdbeMakeLabel(v);
      computeLimitRegisters(pParse, p, iBreak);
      sqlite3VdbeAddOp2(v, OP_Rewind, tab1, iBreak);
      r1 = sqlite3GetTempReg(pParse);
      iStart = sqlite3VdbeAddOp2(v, OP_RowKey, tab1, r1);
      sqlite3VdbeAddOp3(v, OP_NotFound, tab2, iCont, r1);
      sqlite3ReleaseTempReg(pParse, r1);
      selectInnerLoop(pParse, p, p->pEList, tab1, p->pEList->nExpr,
                      0, -1, &dest, iCont, iBreak);
      sqlite3VdbeResolveLabel(v, iCont);
      sqlite3VdbeAddOp2(v, OP_Next, tab1, iStart);
      sqlite3VdbeResolveLabel(v, iBreak);
      sqlite3VdbeAddOp2(v, OP_Close, tab2, 0);
      sqlite3VdbeAddOp2(v, OP_Close, tab1, 0);
      break;
    }
  }

  /* Compute collating sequences used by 
  ** temporary tables needed to implement the compound select.
  ** Attach the KeyInfo structure to all temporary tables.
  **
  ** This section is run by the right-most SELECT statement only.
  ** SELECT statements to the left always skip this part.  The right-most
  ** SELECT might also skip this part if it has no ORDER BY clause and
  ** no temp tables are required.
  */
  if( p->selFlags & SF_UsesEphemeral ){
    int i;                        /* Loop counter */
    KeyInfo *pKeyInfo;            /* Collating sequence for the result set */
    Select *pLoop;                /* For looping through SELECT statements */
    CollSeq **apColl;             /* For looping through pKeyInfo->aColl[] */
    int nCol;                     /* Number of columns in result set */

    assert( p->pRightmost==p );
    nCol = p->pEList->nExpr;
    pKeyInfo = sqlite3DbMallocZero(db,
                       sizeof(*pKeyInfo)+nCol*(sizeof(CollSeq*) + 1));
    if( !pKeyInfo ){
      rc = SQLITE_NOMEM;
      goto multi_select_end;
    }

    pKeyInfo->enc = ENC(db);
    pKeyInfo->nField = (u16)nCol;

    for(i=0, apColl=pKeyInfo->aColl; i<nCol; i++, apColl++){
      *apColl = multiSelectCollSeq(pParse, p, i);
      if( 0==*apColl ){
        *apColl = db->pDfltColl;
      }
    }

    for(pLoop=p; pLoop; pLoop=pLoop->pPrior){
      for(i=0; i<2; i++){
        int addr = pLoop->addrOpenEphm[i];
        if( addr<0 ){
          /* If [0] is unused then [1] is also unused.  So we can
          ** always safely abort as soon as the first unused slot is found */
          assert( pLoop->addrOpenEphm[1]<0 );
          break;
        }
        sqlite3VdbeChangeP2(v, addr, nCol);
        sqlite3VdbeChangeP4(v, addr, (char*)pKeyInfo, P4_KEYINFO);
        pLoop->addrOpenEphm[i] = -1;
      }
    }
    sqlite3DbFree(db, pKeyInfo);
  }

multi_select_end:
  pDest->iMem = dest.iMem;
  pDest->nMem = dest.nMem;
  sqlite3SelectDelete(db, pDelete);
  return rc;
}
#endif /* SQLITE_OMIT_COMPOUND_SELECT */

/*
** Code an output subroutine for a coroutine implementation of a
** SELECT statment.
**
** The data to be output is contained in pIn->iMem.  There are
** pIn->nMem columns to be output.  pDest is where the output should
** be sent.
**
** regReturn is the number of the register holding the subroutine
** return address.
**
** If regPrev>0 then it is a the first register in a vector that
** records the previous output.  mem[regPrev] is a flag that is false
** if there has been no previous output.  If regPrev>0 then code is
** generated to suppress duplicates.  pKeyInfo is used for comparing
** keys.
**
** If the LIMIT found in p->iLimit is reached, jump immediately to
** iBreak.
*/
static int generateOutputSubroutine(
  Parse *pParse,          /* Parsing context */
  Select *p,              /* The SELECT statement */
  SelectDest *pIn,        /* Coroutine supplying data */
  SelectDest *pDest,      /* Where to send the data */
  int regReturn,          /* The return address register */
  int regPrev,            /* Previous result register.  No uniqueness if 0 */
  KeyInfo *pKeyInfo,      /* For comparing with previous entry */
  int p4type,             /* The p4 type for pKeyInfo */
  int iBreak              /* Jump here if we hit the LIMIT */
){
  Vdbe *v = pParse->pVdbe;
  int iContinue;
  int addr;

  addr = sqlite3VdbeCurrentAddr(v);
  iContinue = sqlite3VdbeMakeLabel(v);

  /* Suppress duplicates for UNION, EXCEPT, and INTERSECT 
  */
  if( regPrev ){
    int j1, j2;
    j1 = sqlite3VdbeAddOp1(v, OP_IfNot, regPrev);
    j2 = sqlite3VdbeAddOp4(v, OP_Compare, pIn->iMem, regPrev+1, pIn->nMem,
                              (char*)pKeyInfo, p4type);
    sqlite3VdbeAddOp3(v, OP_Jump, j2+2, iContinue, j2+2);
    sqlite3VdbeJumpHere(v, j1);
    sqlite3ExprCodeCopy(pParse, pIn->iMem, regPrev+1, pIn->nMem);
    sqlite3VdbeAddOp2(v, OP_Integer, 1, regPrev);
  }
  if( pParse->db->mallocFailed ) return 0;

  /* Suppress the the first OFFSET entries if there is an OFFSET clause
  */
  codeOffset(v, p, iContinue);

  switch( pDest->eDest ){
    /* Store the result as data using a unique key.
    */
    case SRT_Table:
    case SRT_EphemTab: {
      int r1 = sqlite3GetTempReg(pParse);
      int r2 = sqlite3GetTempReg(pParse);
      testcase( pDest->eDest==SRT_Table );
      testcase( pDest->eDest==SRT_EphemTab );
      sqlite3VdbeAddOp3(v, OP_MakeRecord, pIn->iMem, pIn->nMem, r1);
      sqlite3VdbeAddOp2(v, OP_NewRowid, pDest->iParm, r2);
      sqlite3VdbeAddOp3(v, OP_Insert, pDest->iParm, r1, r2);
      sqlite3VdbeChangeP5(v, OPFLAG_APPEND);
      sqlite3ReleaseTempReg(pParse, r2);
      sqlite3ReleaseTempReg(pParse, r1);
      break;
    }

#ifndef SQLITE_OMIT_SUBQUERY
    /* If we are creating a set for an "expr IN (SELECT ...)" construct,
    ** then there should be a single item on the stack.  Write this
    ** item into the set table with bogus data.
    */
    case SRT_Set: {
      int r1;
      assert( pIn->nMem==1 );
      p->affinity = 
         sqlite3CompareAffinity(p->pEList->a[0].pExpr, pDest->affinity);
      r1 = sqlite3GetTempReg(pParse);
      sqlite3VdbeAddOp4(v, OP_MakeRecord, pIn->iMem, 1, r1, &p->affinity, 1);
      sqlite3ExprCacheAffinityChange(pParse, pIn->iMem, 1);
      sqlite3VdbeAddOp2(v, OP_IdxInsert, pDest->iParm, r1);
      sqlite3ReleaseTempReg(pParse, r1);
      break;
    }

#if 0  /* Never occurs on an ORDER BY query */
    /* If any row exist in the result set, record that fact and abort.
    */
    case SRT_Exists: {
      sqlite3VdbeAddOp2(v, OP_Integer, 1, pDest->iParm);
      /* The LIMIT clause will terminate the loop for us */
      break;
    }
#endif

    /* If this is a scalar select that is part of an expression, then
    ** store the results in the appropriate memory cell and break out
    ** of the scan loop.
    */
    case SRT_Mem: {
      assert( pIn->nMem==1 );
      sqlite3ExprCodeMove(pParse, pIn->iMem, pDest->iParm, 1);
      /* The LIMIT clause will jump out of the loop for us */
      break;
    }
#endif /* #ifndef SQLITE_OMIT_SUBQUERY */

    /* The results are stored in a sequence of registers
    ** starting at pDest->iMem.  Then the co-routine yields.
    */
    case SRT_Coroutine: {
      if( pDest->iMem==0 ){
        pDest->iMem = sqlite3GetTempRange(pParse, pIn->nMem);
        pDest->nMem = pIn->nMem;
      }
      sqlite3ExprCodeMove(pParse, pIn->iMem, pDest->iMem, pDest->nMem);
      sqlite3VdbeAddOp1(v, OP_Yield, pDest->iParm);
      break;
    }

    /* If none of the above, then the result destination must be
    ** SRT_Output.  This routine is never called with any other
    ** destination other than the ones handled above or SRT_Output.
    **
    ** For SRT_Output, results are stored in a sequence of registers.  
    ** Then the OP_ResultRow opcode is used to cause sqlite3_step() to
    ** return the next row of result.
    */
    default: {
      assert( pDest->eDest==SRT_Output );
      sqlite3VdbeAddOp2(v, OP_ResultRow, pIn->iMem, pIn->nMem);
      sqlite3ExprCacheAffinityChange(pParse, pIn->iMem, pIn->nMem);
      break;
    }
  }

  /* Jump to the end of the loop if the LIMIT is reached.
  */
  if( p->iLimit ){
    sqlite3VdbeAddOp2(v, OP_AddImm, p->iLimit, -1);
    sqlite3VdbeAddOp2(v, OP_IfZero, p->iLimit, iBreak);
  }

  /* Generate the subroutine return
  */
  sqlite3VdbeResolveLabel(v, iContinue);
  sqlite3VdbeAddOp1(v, OP_Return, regReturn);

  return addr;
}

/*
** Alternative compound select code generator for cases when there
** is an ORDER BY clause.
**
** We assume a query of the following form:
**
**      <selectA>  <operator>  <selectB>  ORDER BY <orderbylist>
**
** <operator> is one of UNION ALL, UNION, EXCEPT, or INTERSECT.  The idea
** is to code both <selectA> and <selectB> with the ORDER BY clause as
** co-routines.  Then run the co-routines in parallel and merge the results
** into the output.  In addition to the two coroutines (called selectA and
** selectB) there are 7 subroutines:
**
**    outA:    Move the output of the selectA coroutine into the output
**             of the compound query.
**
**    outB:    Move the output of the selectB coroutine into the output
**             of the compound query.  (Only generated for UNION and
**             UNION ALL.  EXCEPT and INSERTSECT never output a row that
**             appears only in B.)
**
**    AltB:    Called when there is data from both coroutines and A<B.
**
**    AeqB:    Called when there is data from both coroutines and A==B.
**
**    AgtB:    Called when there is data from both coroutines and A>B.
**
**    EofA:    Called when data is exhausted from selectA.
**
**    EofB:    Called when data is exhausted from selectB.
**
** The implementation of the latter five subroutines depend on which 
** <operator> is used:
**
**
**             UNION ALL         UNION            EXCEPT          INTERSECT
**          -------------  -----------------  --------------  -----------------
**   AltB:   outA, nextA      outA, nextA       outA, nextA         nextA
**
**   AeqB:   outA, nextA         nextA             nextA         outA, nextA
**
**   AgtB:   outB, nextB      outB, nextB          nextB            nextB
**
**   EofA:   outB, nextB      outB, nextB          halt             halt
**
**   EofB:   outA, nextA      outA, nextA       outA, nextA         halt
**
** In the AltB, AeqB, and AgtB subroutines, an EOF on A following nextA
** causes an immediate jump to EofA and an EOF on B following nextB causes
** an immediate jump to EofB.  Within EofA and EofB, and EOF on entry or
** following nextX causes a jump to the end of the select processing.
**
** Duplicate removal in the UNION, EXCEPT, and INTERSECT cases is handled
** within the output subroutine.  The regPrev register set holds the previously
** output value.  A comparison is made against this value and the output
** is skipped if the next results would be the same as the previous.
**
** The implementation plan is to implement the two coroutines and seven
** subroutines first, then put the control logic at the bottom.  Like this:
**
**          goto Init
**     coA: coroutine for left query (A)
**     coB: coroutine for right query (B)
**    outA: output one row of A
**    outB: output one row of B (UNION and UNION ALL only)
**    EofA: ...
**    EofB: ...
**    AltB: ...
**    AeqB: ...
**    AgtB: ...
**    Init: initialize coroutine registers
**          yield coA
**          if eof(A) goto EofA
**          yield coB
**          if eof(B) goto EofB
**    Cmpr: Compare A, B
**          Jump AltB, AeqB, AgtB
**     End: ...
**
** We call AltB, AeqB, AgtB, EofA, and EofB "subroutines" but they are not
** actually called using Gosub and they do not Return.  EofA and EofB loop
** until all data is exhausted then jump to the "end" labe.  AltB, AeqB,
** and AgtB jump to either L2 or to one of EofA or EofB.
*/
#ifndef SQLITE_OMIT_COMPOUND_SELECT
static int multiSelectOrderBy(
  Parse *pParse,        /* Parsing context */
  Select *p,            /* The right-most of SELECTs to be coded */
  SelectDest *pDest     /* What to do with query results */
){
  int i, j;             /* Loop counters */
  Select *pPrior;       /* Another SELECT immediately to our left */
  Vdbe *v;              /* Generate code to this VDBE */
  SelectDest destA;     /* Destination for coroutine A */
  SelectDest destB;     /* Destination for coroutine B */
  int regAddrA;         /* Address register for select-A coroutine */
  int regEofA;          /* Flag to indicate when select-A is complete */
  int regAddrB;         /* Address register for select-B coroutine */
  int regEofB;          /* Flag to indicate when select-B is complete */
  int addrSelectA;      /* Address of the select-A coroutine */
  int addrSelectB;      /* Address of the select-B coroutine */
  int regOutA;          /* Address register for the output-A subroutine */
  int regOutB;          /* Address register for the output-B subroutine */
  int addrOutA;         /* Address of the output-A subroutine */
  int addrOutB = 0;     /* Address of the output-B subroutine */
  int addrEofA;         /* Address of the select-A-exhausted subroutine */
  int addrEofB;         /* Address of the select-B-exhausted subroutine */
  int addrAltB;         /* Address of the A<B subroutine */
  int addrAeqB;         /* Address of the A==B subroutine */
  int addrAgtB;         /* Address of the A>B subroutine */
  int regLimitA;        /* Limit register for select-A */
  int regLimitB;        /* Limit register for select-A */
  int regPrev;          /* A range of registers to hold previous output */
  int savedLimit;       /* Saved value of p->iLimit */
  int savedOffset;      /* Saved value of p->iOffset */
  int labelCmpr;        /* Label for the start of the merge algorithm */
  int labelEnd;         /* Label for the end of the overall SELECT stmt */
  int j1;               /* Jump instructions that get retargetted */
  int op;               /* One of TK_ALL, TK_UNION, TK_EXCEPT, TK_INTERSECT */
  KeyInfo *pKeyDup = 0; /* Comparison information for duplicate removal */
  KeyInfo *pKeyMerge;   /* Comparison information for merging rows */
  sqlite3 *db;          /* Database connection */
  ExprList *pOrderBy;   /* The ORDER BY clause */
  int nOrderBy;         /* Number of terms in the ORDER BY clause */
  int *aPermute;        /* Mapping from ORDER BY terms to result set columns */

  assert( p->pOrderBy!=0 );
  assert( pKeyDup==0 ); /* "Managed" code needs this.  Ticket #3382. */
  db = pParse->db;
  v = pParse->pVdbe;
  assert( v!=0 );       /* Already thrown the error if VDBE alloc failed */
  labelEnd = sqlite3VdbeMakeLabel(v);
  labelCmpr = sqlite3VdbeMakeLabel(v);


  /* Patch up the ORDER BY clause
  */
  op = p->op;  
  pPrior = p->pPrior;
  assert( pPrior->pOrderBy==0 );
  pOrderBy = p->pOrderBy;
  assert( pOrderBy );
  nOrderBy = pOrderBy->nExpr;

  /* For operators other than UNION ALL we have to make sure that
  ** the ORDER BY clause covers every term of the result set.  Add
  ** terms to the ORDER BY clause as necessary.
  */
  if( op!=TK_ALL ){
    for(i=1; db->mallocFailed==0 && i<=p->pEList->nExpr; i++){
      struct ExprList_item *pItem;
      for(j=0, pItem=pOrderBy->a; j<nOrderBy; j++, pItem++){
        assert( pItem->iCol>0 );
        if( pItem->iCol==i ) break;
      }
      if( j==nOrderBy ){
        Expr *pNew = sqlite3Expr(db, TK_INTEGER, 0);
        if( pNew==0 ) return SQLITE_NOMEM;
        pNew->flags |= EP_IntValue;
        pNew->u.iValue = i;
        pOrderBy = sqlite3ExprListAppend(pParse, pOrderBy, pNew);
        pOrderBy->a[nOrderBy++].iCol = (u16)i;
      }
    }
  }

  /* Compute the comparison permutation and keyinfo that is used with
  ** the permutation used to determine if the next
  ** row of results comes from selectA or selectB.  Also add explicit
  ** collations to the ORDER BY clause terms so that when the subqueries
  ** to the right and the left are evaluated, they use the correct
  ** collation.
  */
  aPermute = sqlite3DbMallocRaw(db, sizeof(int)*nOrderBy);
  if( aPermute ){
    struct ExprList_item *pItem;
    for(i=0, pItem=pOrderBy->a; i<nOrderBy; i++, pItem++){
      assert( pItem->iCol>0  && pItem->iCol<=p->pEList->nExpr );
      aPermute[i] = pItem->iCol - 1;
    }
    pKeyMerge =
      sqlite3DbMallocRaw(db, sizeof(*pKeyMerge)+nOrderBy*(sizeof(CollSeq*)+1));
    if( pKeyMerge ){
      pKeyMerge->aSortOrder = (u8*)&pKeyMerge->aColl[nOrderBy];
      pKeyMerge->nField = (u16)nOrderBy;
      pKeyMerge->enc = ENC(db);
      for(i=0; i<nOrderBy; i++){
        CollSeq *pColl;
        Expr *pTerm = pOrderBy->a[i].pExpr;
        if( pTerm->flags & EP_ExpCollate ){
          pColl = pTerm->pColl;
        }else{
          pColl = multiSelectCollSeq(pParse, p, aPermute[i]);
          pTerm->flags |= EP_ExpCollate;
          pTerm->pColl = pColl;
        }
        pKeyMerge->aColl[i] = pColl;
        pKeyMerge->aSortOrder[i] = pOrderBy->a[i].sortOrder;
      }
    }
  }else{
    pKeyMerge = 0;
  }

  /* Reattach the ORDER BY clause to the query.
  */
  p->pOrderBy = pOrderBy;
  pPrior->pOrderBy = sqlite3ExprListDup(pParse->db, pOrderBy, 0);

  /* Allocate a range of temporary registers and the KeyInfo needed
  ** for the logic that removes duplicate result rows when the
  ** operator is UNION, EXCEPT, or INTERSECT (but not UNION ALL).
  */
  if( op==TK_ALL ){
    regPrev = 0;
  }else{
    int nExpr = p->pEList->nExpr;
    assert( nOrderBy>=nExpr || db->mallocFailed );
    regPrev = sqlite3GetTempRange(pParse, nExpr+1);
    sqlite3VdbeAddOp2(v, OP_Integer, 0, regPrev);
    pKeyDup = sqlite3DbMallocZero(db,
                  sizeof(*pKeyDup) + nExpr*(sizeof(CollSeq*)+1) );
    if( pKeyDup ){
      pKeyDup->aSortOrder = (u8*)&pKeyDup->aColl[nExpr];
      pKeyDup->nField = (u16)nExpr;
      pKeyDup->enc = ENC(db);
      for(i=0; i<nExpr; i++){
        pKeyDup->aColl[i] = multiSelectCollSeq(pParse, p, i);
        pKeyDup->aSortOrder[i] = 0;
      }
    }
  }
 
  /* Separate the left and the right query from one another
  */
  p->pPrior = 0;
  pPrior->pRightmost = 0;
  sqlite3ResolveOrderGroupBy(pParse, p, p->pOrderBy, "ORDER");
  if( pPrior->pPrior==0 ){
    sqlite3ResolveOrderGroupBy(pParse, pPrior, pPrior->pOrderBy, "ORDER");
  }

  /* Compute the limit registers */
  computeLimitRegisters(pParse, p, labelEnd);
  if( p->iLimit && op==TK_ALL ){
    regLimitA = ++pParse->nMem;
    regLimitB = ++pParse->nMem;
    sqlite3VdbeAddOp2(v, OP_Copy, p->iOffset ? p->iOffset+1 : p->iLimit,
                                  regLimitA);
    sqlite3VdbeAddOp2(v, OP_Copy, regLimitA, regLimitB);
  }else{
    regLimitA = regLimitB = 0;
  }
  sqlite3ExprDelete(db, p->pLimit);
  p->pLimit = 0;
  sqlite3ExprDelete(db, p->pOffset);
  p->pOffset = 0;

  regAddrA = ++pParse->nMem;
  regEofA = ++pParse->nMem;
  regAddrB = ++pParse->nMem;
  regEofB = ++pParse->nMem;
  regOutA = ++pParse->nMem;
  regOutB = ++pParse->nMem;
  sqlite3SelectDestInit(&destA, SRT_Coroutine, regAddrA);
  sqlite3SelectDestInit(&destB, SRT_Coroutine, regAddrB);

  /* Jump past the various subroutines and coroutines to the main
  ** merge loop
  */
  j1 = sqlite3VdbeAddOp0(v, OP_Goto);
  addrSelectA = sqlite3VdbeCurrentAddr(v);


  /* Generate a coroutine to evaluate the SELECT statement to the
  ** left of the compound operator - the "A" select.
  */
  VdbeNoopComment((v, "Begin coroutine for left SELECT"));
  pPrior->iLimit = regLimitA;
  sqlite3Select(pParse, pPrior, &destA);
  sqlite3VdbeAddOp2(v, OP_Integer, 1, regEofA);
  sqlite3VdbeAddOp1(v, OP_Yield, regAddrA);
  VdbeNoopComment((v, "End coroutine for left SELECT"));

  /* Generate a coroutine to evaluate the SELECT statement on 
  ** the right - the "B" select
  */
  addrSelectB = sqlite3VdbeCurrentAddr(v);
  VdbeNoopComment((v, "Begin coroutine for right SELECT"));
  savedLimit = p->iLimit;
  savedOffset = p->iOffset;
  p->iLimit = regLimitB;
  p->iOffset = 0;  
  sqlite3Select(pParse, p, &destB);
  p->iLimit = savedLimit;
  p->iOffset = savedOffset;
  sqlite3VdbeAddOp2(v, OP_Integer, 1, regEofB);
  sqlite3VdbeAddOp1(v, OP_Yield, regAddrB);
  VdbeNoopComment((v, "End coroutine for right SELECT"));

  /* Generate a subroutine that outputs the current row of the A
  ** select as the next output row of the compound select.
  */
  VdbeNoopComment((v, "Output routine for A"));
  addrOutA = generateOutputSubroutine(pParse,
                 p, &destA, pDest, regOutA,
                 regPrev, pKeyDup, P4_KEYINFO_HANDOFF, labelEnd);
  
  /* Generate a subroutine that outputs the current row of the B
  ** select as the next output row of the compound select.
  */
  if( op==TK_ALL || op==TK_UNION ){
    VdbeNoopComment((v, "Output routine for B"));
    addrOutB = generateOutputSubroutine(pParse,
                 p, &destB, pDest, regOutB,
                 regPrev, pKeyDup, P4_KEYINFO_STATIC, labelEnd);
  }

  /* Generate a subroutine to run when the results from select A
  ** are exhausted and only data in select B remains.
  */
  VdbeNoopComment((v, "eof-A subroutine"));
  if( op==TK_EXCEPT || op==TK_INTERSECT ){
    addrEofA = sqlite3VdbeAddOp2(v, OP_Goto, 0, labelEnd);
  }else{  
    addrEofA = sqlite3VdbeAddOp2(v, OP_If, regEofB, labelEnd);
    sqlite3VdbeAddOp2(v, OP_Gosub, regOutB, addrOutB);
    sqlite3VdbeAddOp1(v, OP_Yield, regAddrB);
    sqlite3VdbeAddOp2(v, OP_Goto, 0, addrEofA);
  }

  /* Generate a subroutine to run when the results from select B
  ** are exhausted and only data in select A remains.
  */
  if( op==TK_INTERSECT ){
    addrEofB = addrEofA;
  }else{  
    VdbeNoopComment((v, "eof-B subroutine"));
    addrEofB = sqlite3VdbeAddOp2(v, OP_If, regEofA, labelEnd);
    sqlite3VdbeAddOp2(v, OP_Gosub, regOutA, addrOutA);
    sqlite3VdbeAddOp1(v, OP_Yield, regAddrA);
    sqlite3VdbeAddOp2(v, OP_Goto, 0, addrEofB);
  }

  /* Generate code to handle the case of A<B
  */
  VdbeNoopComment((v, "A-lt-B subroutine"));
  addrAltB = sqlite3VdbeAddOp2(v, OP_Gosub, regOutA, addrOutA);
  sqlite3VdbeAddOp1(v, OP_Yield, regAddrA);
  sqlite3VdbeAddOp2(v, OP_If, regEofA, addrEofA);
  sqlite3VdbeAddOp2(v, OP_Goto, 0, labelCmpr);

  /* Generate code to handle the case of A==B
  */
  if( op==TK_ALL ){
    addrAeqB = addrAltB;
  }else if( op==TK_INTERSECT ){
    addrAeqB = addrAltB;
    addrAltB++;
  }else{
    VdbeNoopComment((v, "A-eq-B subroutine"));
    addrAeqB =
    sqlite3VdbeAddOp1(v, OP_Yield, regAddrA);
    sqlite3VdbeAddOp2(v, OP_If, regEofA, addrEofA);
    sqlite3VdbeAddOp2(v, OP_Goto, 0, labelCmpr);
  }

  /* Generate code to handle the case of A>B
  */
  VdbeNoopComment((v, "A-gt-B subroutine"));
  addrAgtB = sqlite3VdbeCurrentAddr(v);
  if( op==TK_ALL || op==TK_UNION ){
    sqlite3VdbeAddOp2(v, OP_Gosub, regOutB, addrOutB);
  }
  sqlite3VdbeAddOp1(v, OP_Yield, regAddrB);
  sqlite3VdbeAddOp2(v, OP_If, regEofB, addrEofB);
  sqlite3VdbeAddOp2(v, OP_Goto, 0, labelCmpr);

  /* This code runs once to initialize everything.
  */
  sqlite3VdbeJumpHere(v, j1);
  sqlite3VdbeAddOp2(v, OP_Integer, 0, regEofA);
  sqlite3VdbeAddOp2(v, OP_Integer, 0, regEofB);
  sqlite3VdbeAddOp2(v, OP_Gosub, regAddrA, addrSelectA);
  sqlite3VdbeAddOp2(v, OP_Gosub, regAddrB, addrSelectB);
  sqlite3VdbeAddOp2(v, OP_If, regEofA, addrEofA);
  sqlite3VdbeAddOp2(v, OP_If, regEofB, addrEofB);

  /* Implement the main merge loop
  */
  sqlite3VdbeResolveLabel(v, labelCmpr);
  sqlite3VdbeAddOp4(v, OP_Permutation, 0, 0, 0, (char*)aPermute, P4_INTARRAY);
  sqlite3VdbeAddOp4(v, OP_Compare, destA.iMem, destB.iMem, nOrderBy,
                         (char*)pKeyMerge, P4_KEYINFO_HANDOFF);
  sqlite3VdbeAddOp3(v, OP_Jump, addrAltB, addrAeqB, addrAgtB);

  /* Release temporary registers
  */
  if( regPrev ){
    sqlite3ReleaseTempRange(pParse, regPrev, nOrderBy+1);
  }

  /* Jump to the this point in order to terminate the query.
  */
  sqlite3VdbeResolveLabel(v, labelEnd);

  /* Set the number of output columns
  */
  if( pDest->eDest==SRT_Output ){
    Select *pFirst = pPrior;
    while( pFirst->pPrior ) pFirst = pFirst->pPrior;
    generateColumnNames(pParse, 0, pFirst->pEList);
  }

  /* Reassembly the compound query so that it will be freed correctly
  ** by the calling function */
  if( p->pPrior ){
    sqlite3SelectDelete(db, p->pPrior);
  }
  p->pPrior = pPrior;

  /*** TBD:  Insert subroutine calls to close cursors on incomplete
  **** subqueries ****/
  return SQLITE_OK;
}
#endif

#if !defined(SQLITE_OMIT_SUBQUERY) || !defined(SQLITE_OMIT_VIEW)
/* Forward Declarations */
static void substExprList(sqlite3*, ExprList*, int, ExprList*);
static void substSelect(sqlite3*, Select *, int, ExprList *);

/*
** Scan through the expression pExpr.  Replace every reference to
** a column in table number iTable with a copy of the iColumn-th
** entry in pEList.  (But leave references to the ROWID column 
** unchanged.)
**
** This routine is part of the flattening procedure.  A subquery
** whose result set is defined by pEList appears as entry in the
** FROM clause of a SELECT such that the VDBE cursor assigned to that
** FORM clause entry is iTable.  This routine make the necessary 
** changes to pExpr so that it refers directly to the source table
** of the subquery rather the result set of the subquery.
*/
static Expr *substExpr(
  sqlite3 *db,        /* Report malloc errors to this connection */
  Expr *pExpr,        /* Expr in which substitution occurs */
  int iTable,         /* Table to be substituted */
  ExprList *pEList    /* Substitute expressions */
){
  if( pExpr==0 ) return 0;
  if( pExpr->op==TK_COLUMN && pExpr->iTable==iTable ){
    if( pExpr->iColumn<0 ){
      pExpr->op = TK_NULL;
    }else{
      Expr *pNew;
      assert( pEList!=0 && pExpr->iColumn<pEList->nExpr );
      assert( pExpr->pLeft==0 && pExpr->pRight==0 );
      pNew = sqlite3ExprDup(db, pEList->a[pExpr->iColumn].pExpr, 0);
      if( pNew && pExpr->pColl ){
        pNew->pColl = pExpr->pColl;
      }
      sqlite3ExprDelete(db, pExpr);
      pExpr = pNew;
    }
  }else{
    pExpr->pLeft = substExpr(db, pExpr->pLeft, iTable, pEList);
    pExpr->pRight = substExpr(db, pExpr->pRight, iTable, pEList);
    if( ExprHasProperty(pExpr, EP_xIsSelect) ){
      substSelect(db, pExpr->x.pSelect, iTable, pEList);
    }else{
      substExprList(db, pExpr->x.pList, iTable, pEList);
    }
  }
  return pExpr;
}
static void substExprList(
  sqlite3 *db,         /* Report malloc errors here */
  ExprList *pList,     /* List to scan and in which to make substitutes */
  int iTable,          /* Table to be substituted */
  ExprList *pEList     /* Substitute values */
){
  int i;
  if( pList==0 ) return;
  for(i=0; i<pList->nExpr; i++){
    pList->a[i].pExpr = substExpr(db, pList->a[i].pExpr, iTable, pEList);
  }
}
static void substSelect(
  sqlite3 *db,         /* Report malloc errors here */
  Select *p,           /* SELECT statement in which to make substitutions */
  int iTable,          /* Table to be replaced */
  ExprList *pEList     /* Substitute values */
){
  SrcList *pSrc;
  struct SrcList_item *pItem;
  int i;
  if( !p ) return;
  substExprList(db, p->pEList, iTable, pEList);
  substExprList(db, p->pGroupBy, iTable, pEList);
  substExprList(db, p->pOrderBy, iTable, pEList);
  p->pHaving = substExpr(db, p->pHaving, iTable, pEList);
  p->pWhere = substExpr(db, p->pWhere, iTable, pEList);
  substSelect(db, p->pPrior, iTable, pEList);
  pSrc = p->pSrc;
  assert( pSrc );  /* Even for (SELECT 1) we have: pSrc!=0 but pSrc->nSrc==0 */
  if( ALWAYS(pSrc) ){
    for(i=pSrc->nSrc, pItem=pSrc->a; i>0; i--, pItem++){
      substSelect(db, pItem->pSelect, iTable, pEList);
    }
  }
}
#endif /* !defined(SQLITE_OMIT_SUBQUERY) || !defined(SQLITE_OMIT_VIEW) */

#if !defined(SQLITE_OMIT_SUBQUERY) || !defined(SQLITE_OMIT_VIEW)
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
** optimized.
**
** This routine attempts to rewrite queries such as the above into
** a single flat select, like this:
**
**     SELECT x+y AS a FROM t1 WHERE z<100 AND a>5
**
** The code generated for this simpification gives the same result
** but only has to scan the data once.  And because indices might 
** exist on the table t1, a complete scan of the data might be
** avoided.
**
** Flattening is only attempted if all of the following are true:
**
**   (1)  The subquery and the outer query do not both use aggregates.
**
**   (2)  The subquery is not an aggregate or the outer query is not a join.
**
**   (3)  The subquery is not the right operand of a left outer join
**        (Originally ticket #306.  Strenghtened by ticket #3300)
**
**   (4)  The subquery is not DISTINCT or the outer query is not a join.
**
**   (5)  The subquery is not DISTINCT or the outer query does not use
**        aggregates.
**
**   (6)  The subquery does not use aggregates or the outer query is not
**        DISTINCT.
**
**   (7)  The subquery has a FROM clause.
**
**   (8)  The subquery does not use LIMIT or the outer query is not a join.
**
**   (9)  The subquery does not use LIMIT or the outer query does not use
**        aggregates.
**
**  (10)  The subquery does not use aggregates or the outer query does not
**        use LIMIT.
**
**  (11)  The subquery and the outer query do not both have ORDER BY clauses.
**
**  (12)  Not implemented.  Subsumed into restriction (3).  Was previously
**        a separate restriction deriving from ticket #350.
**
**  (13)  The subquery and outer query do not both use LIMIT
**
**  (14)  The subquery does not use OFFSET
**
**  (15)  The outer query is not part of a compound select or the
**        subquery does not have both an ORDER BY and a LIMIT clause.
**        (See ticket #2339)
**
**  (16)  The outer query is not an aggregate or the subquery does
**        not contain ORDER BY.  (Ticket #2942)  This used to not matter
**        until we introduced the group_concat() function.  
**
**  (17)  The sub-query is not a compound select, or it is a UNION ALL 
**        compound clause made up entirely of non-aggregate queries, and 
**        the parent query:
**
**          * is not itself part of a compound select,
**          * is not an aggregate or DISTINCT query, and
**          * has no other tables or sub-selects in the FROM clause.
**
**        The parent and sub-query may contain WHERE clauses. Subject to
**        rules (11), (13) and (14), they may also contain ORDER BY,
**        LIMIT and OFFSET clauses.
**
**  (18)  If the sub-query is a compound select, then all terms of the
**        ORDER by clause of the parent must be simple references to 
**        columns of the sub-query.
**
**  (19)  The subquery does not use LIMIT or the outer query does not
**        have a WHERE clause.
**
**  (20)  If the sub-query is a compound select, then it must not use
**        an ORDER BY clause.  Ticket #3773.  We could relax this constraint
**        somewhat by saying that the terms of the ORDER BY clause must
**        appear as unmodified result columns in the outer query.  But
**        have other optimizations in mind to deal with that case.
**
** In this routine, the "p" parameter is a pointer to the outer query.
** The subquery is p->pSrc->a[iFrom].  isAgg is true if the outer query
** uses aggregates and subqueryIsAgg is true if the subquery uses aggregates.
**
** If flattening is not attempted, this routine is a no-op and returns 0.
** If flattening is attempted this routine returns 1.
**
** All of the expression analysis must occur on both the outer query and
** the subquery before this routine runs.
*/
static int flattenSubquery(
  Parse *pParse,       /* Parsing context */
  Select *p,           /* The parent or outer SELECT statement */
  int iFrom,           /* Index in p->pSrc->a[] of the inner subquery */
  int isAgg,           /* True if outer SELECT uses aggregate functions */
  int subqueryIsAgg    /* True if the subquery uses aggregate functions */
){
  const char *zSavedAuthContext = pParse->zAuthContext;
  Select *pParent;
  Select *pSub;       /* The inner query or "subquery" */
  Select *pSub1;      /* Pointer to the rightmost select in sub-query */
  SrcList *pSrc;      /* The FROM clause of the outer query */
  SrcList *pSubSrc;   /* The FROM clause of the subquery */
  ExprList *pList;    /* The result set of the outer query */
  int iParent;        /* VDBE cursor number of the pSub result set temp table */
  int i;              /* Loop counter */
  Expr *pWhere;                    /* The WHERE clause */
  struct SrcList_item *pSubitem;   /* The subquery */
  sqlite3 *db = pParse->db;

  /* Check to see if flattening is permitted.  Return 0 if not.
  */
  assert( p!=0 );
  assert( p->pPrior==0 );  /* Unable to flatten compound queries */
  pSrc = p->pSrc;
  assert( pSrc && iFrom>=0 && iFrom<pSrc->nSrc );
  pSubitem = &pSrc->a[iFrom];
  iParent = pSubitem->iCursor;
  pSub = pSubitem->pSelect;
  assert( pSub!=0 );
  if( isAgg && subqueryIsAgg ) return 0;                 /* Restriction (1)  */
  if( subqueryIsAgg && pSrc->nSrc>1 ) return 0;          /* Restriction (2)  */
  pSubSrc = pSub->pSrc;
  assert( pSubSrc );
  /* Prior to version 3.1.2, when LIMIT and OFFSET had to be simple constants,
  ** not arbitrary expresssions, we allowed some combining of LIMIT and OFFSET
  ** because they could be computed at compile-time.  But when LIMIT and OFFSET
  ** became arbitrary expressions, we were forced to add restrictions (13)
  ** and (14). */
  if( pSub->pLimit && p->pLimit ) return 0;              /* Restriction (13) */
  if( pSub->pOffset ) return 0;                          /* Restriction (14) */
  if( p->pRightmost && pSub->pLimit && pSub->pOrderBy ){
    return 0;                                            /* Restriction (15) */
  }
  if( pSubSrc->nSrc==0 ) return 0;                       /* Restriction (7)  */
  if( ((pSub->selFlags & SF_Distinct)!=0 || pSub->pLimit) 
         && (pSrc->nSrc>1 || isAgg) ){          /* Restrictions (4)(5)(8)(9) */
     return 0;       
  }
  if( (p->selFlags & SF_Distinct)!=0 && subqueryIsAgg ){
     return 0;         /* Restriction (6)  */
  }
  if( p->pOrderBy && pSub->pOrderBy ){
     return 0;                                           /* Restriction (11) */
  }
  if( isAgg && pSub->pOrderBy ) return 0;                /* Restriction (16) */
  if( pSub->pLimit && p->pWhere ) return 0;              /* Restriction (19) */

  /* OBSOLETE COMMENT 1:
  ** Restriction 3:  If the subquery is a join, make sure the subquery is 
  ** not used as the right operand of an outer join.  Examples of why this
  ** is not allowed:
  **
  **         t1 LEFT OUTER JOIN (t2 JOIN t3)
  **
  ** If we flatten the above, we would get
  **
  **         (t1 LEFT OUTER JOIN t2) JOIN t3
  **
  ** which is not at all the same thing.
  **
  ** OBSOLETE COMMENT 2:
  ** Restriction 12:  If the subquery is the right operand of a left outer
  ** join, make sure the subquery has no WHERE clause.
  ** An examples of why this is not allowed:
  **
  **         t1 LEFT OUTER JOIN (SELECT * FROM t2 WHERE t2.x>0)
  **
  ** If we flatten the above, we would get
  **
  **         (t1 LEFT OUTER JOIN t2) WHERE t2.x>0
  **
  ** But the t2.x>0 test will always fail on a NULL row of t2, which
  ** effectively converts the OUTER JOIN into an INNER JOIN.
  **
  ** THIS OVERRIDES OBSOLETE COMMENTS 1 AND 2 ABOVE:
  ** Ticket #3300 shows that flattening the right term of a LEFT JOIN
  ** is fraught with danger.  Best to avoid the whole thing.  If the
  ** subquery is the right term of a LEFT JOIN, then do not flatten.
  */
  if( (pSubitem->jointype & JT_OUTER)!=0 ){
    return 0;
  }

  /* Restriction 17: If the sub-query is a compound SELECT, then it must
  ** use only the UNION ALL operator. And none of the simple select queries
  ** that make up the compound SELECT are allowed to be aggregate or distinct
  ** queries.
  */
  if( pSub->pPrior ){
    if( pSub->pOrderBy ){
      return 0;  /* Restriction 20 */
    }
    if( isAgg || (p->selFlags & SF_Distinct)!=0 || pSrc->nSrc!=1 ){
      return 0;
    }
    for(pSub1=pSub; pSub1; pSub1=pSub1->pPrior){
      testcase( (pSub1->selFlags & (SF_Distinct|SF_Aggregate))==SF_Distinct );
      testcase( (pSub1->selFlags & (SF_Distinct|SF_Aggregate))==SF_Aggregate );
      if( (pSub1->selFlags & (SF_Distinct|SF_Aggregate))!=0
       || (pSub1->pPrior && pSub1->op!=TK_ALL) 
       || NEVER(pSub1->pSrc==0) || pSub1->pSrc->nSrc!=1
      ){
        return 0;
      }
    }

    /* Restriction 18. */
    if( p->pOrderBy ){
      int ii;
      for(ii=0; ii<p->pOrderBy->nExpr; ii++){
        if( p->pOrderBy->a[ii].iCol==0 ) return 0;
      }
    }
  }

  /***** If we reach this point, flattening is permitted. *****/

  /* Authorize the subquery */
  pParse->zAuthContext = pSubitem->zName;
  sqlite3AuthCheck(pParse, SQLITE_SELECT, 0, 0, 0);
  pParse->zAuthContext = zSavedAuthContext;

  /* If the sub-query is a compound SELECT statement, then (by restrictions
  ** 17 and 18 above) it must be a UNION ALL and the parent query must 
  ** be of the form:
  **
  **     SELECT <expr-list> FROM (<sub-query>) <where-clause> 
  **
  ** followed by any ORDER BY, LIMIT and/or OFFSET clauses. This block
  ** creates N-1 copies of the parent query without any ORDER BY, LIMIT or 
  ** OFFSET clauses and joins them to the left-hand-side of the original
  ** using UNION ALL operators. In this case N is the number of simple
  ** select statements in the compound sub-query.
  **
  ** Example:
  **
  **     SELECT a+1 FROM (
  **        SELECT x FROM tab
  **        UNION ALL
  **        SELECT y FROM tab
  **        UNION ALL
  **        SELECT abs(z*2) FROM tab2
  **     ) WHERE a!=5 ORDER BY 1
  **
  ** Transformed into:
  **
  **     SELECT x+1 FROM tab WHERE x+1!=5
  **     UNION ALL
  **     SELECT y+1 FROM tab WHERE y+1!=5
  **     UNION ALL
  **     SELECT abs(z*2)+1 FROM tab2 WHERE abs(z*2)+1!=5
  **     ORDER BY 1
  **
  ** We call this the "compound-subquery flattening".
  */
  for(pSub=pSub->pPrior; pSub; pSub=pSub->pPrior){
    Select *pNew;
    ExprList *pOrderBy = p->pOrderBy;
    Expr *pLimit = p->pLimit;
    Select *pPrior = p->pPrior;
    p->pOrderBy = 0;
    p->pSrc = 0;
    p->pPrior = 0;
    p->pLimit = 0;
    pNew = sqlite3SelectDup(db, p, 0);
    p->pLimit = pLimit;
    p->pOrderBy = pOrderBy;
    p->pSrc = pSrc;
    p->op = TK_ALL;
    p->pRightmost = 0;
    if( pNew==0 ){
      pNew = pPrior;
    }else{
      pNew->pPrior = pPrior;
      pNew->pRightmost = 0;
    }
    p->pPrior = pNew;
    if( db->mallocFailed ) return 1;
  }

  /* Begin flattening the iFrom-th entry of the FROM clause 
  ** in the outer query.
  */
  pSub = pSub1 = pSubitem->pSelect;

  /* Delete the transient table structure associated with the
  ** subquery
  */
  sqlite3DbFree(db, pSubitem->zDatabase);
  sqlite3DbFree(db, pSubitem->zName);
  sqlite3DbFree(db, pSubitem->zAlias);
  pSubitem->zDatabase = 0;
  pSubitem->zName = 0;
  pSubitem->zAlias = 0;
  pSubitem->pSelect = 0;

  /* Defer deleting the Table object associated with the
  ** subquery until code generation is
  ** complete, since there may still exist Expr.pTab entries that
  ** refer to the subquery even after flattening.  Ticket #3346.
  **
  ** pSubitem->pTab is always non-NULL by test restrictions and tests above.
  */
  if( ALWAYS(pSubitem->pTab!=0) ){
    Table *pTabToDel = pSubitem->pTab;
    if( pTabToDel->nRef==1 ){
      Parse *pToplevel = sqlite3ParseToplevel(pParse);
      pTabToDel->pNextZombie = pToplevel->pZombieTab;
      pToplevel->pZombieTab = pTabToDel;
    }else{
      pTabToDel->nRef--;
    }
    pSubitem->pTab = 0;
  }

  /* The following loop runs once for each term in a compound-subquery
  ** flattening (as described above).  If we are doing a different kind
  ** of flattening - a flattening other than a compound-subquery flattening -
  ** then this loop only runs once.
  **
  ** This loop moves all of the FROM elements of the subquery into the
  ** the FROM clause of the outer query.  Before doing this, remember
  ** the cursor number for the original outer query FROM element in
  ** iParent.  The iParent cursor will never be used.  Subsequent code
  ** will scan expressions looking for iParent references and replace
  ** those references with expressions that resolve to the subquery FROM
  ** elements we are now copying in.
  */
  for(pParent=p; pParent; pParent=pParent->pPrior, pSub=pSub->pPrior){
    int nSubSrc;
    u8 jointype = 0;
    pSubSrc = pSub->pSrc;     /* FROM clause of subquery */
    nSubSrc = pSubSrc->nSrc;  /* Number of terms in subquery FROM clause */
    pSrc = pParent->pSrc;     /* FROM clause of the outer query */

    if( pSrc ){
      assert( pParent==p );  /* First time through the loop */
      jointype = pSubitem->jointype;
    }else{
      assert( pParent!=p );  /* 2nd and subsequent times through the loop */
      pSrc = pParent->pSrc = sqlite3SrcListAppend(db, 0, 0, 0);
      if( pSrc==0 ){
        assert( db->mallocFailed );
        break;
      }
    }

    /* The subquery uses a single slot of the FROM clause of the outer
    ** query.  If the subquery has more than one element in its FROM clause,
    ** then expand the outer query to make space for it to hold all elements
    ** of the subquery.
    **
    ** Example:
    **
    **    SELECT * FROM tabA, (SELECT * FROM sub1, sub2), tabB;
    **
    ** The outer query has 3 slots in its FROM clause.  One slot of the
    ** outer query (the middle slot) is used by the subquery.  The next
    ** block of code will expand the out query to 4 slots.  The middle
    ** slot is expanded to two slots in order to make space for the
    ** two elements in the FROM clause of the subquery.
    */
    if( nSubSrc>1 ){
      pParent->pSrc = pSrc = sqlite3SrcListEnlarge(db, pSrc, nSubSrc-1,iFrom+1);
      if( db->mallocFailed ){
        break;
      }
    }

    /* Transfer the FROM clause terms from the subquery into the
    ** outer query.
    */
    for(i=0; i<nSubSrc; i++){
      sqlite3IdListDelete(db, pSrc->a[i+iFrom].pUsing);
      pSrc->a[i+iFrom] = pSubSrc->a[i];
      memset(&pSubSrc->a[i], 0, sizeof(pSubSrc->a[i]));
    }
    pSrc->a[iFrom].jointype = jointype;
  
    /* Now begin substituting subquery result set expressions for 
    ** references to the iParent in the outer query.
    ** 
    ** Example:
    **
    **   SELECT a+5, b*10 FROM (SELECT x*3 AS a, y+10 AS b FROM t1) WHERE a>b;
    **   \                     \_____________ subquery __________/          /
    **    \_____________________ outer query ______________________________/
    **
    ** We look at every expression in the outer query and every place we see
    ** "a" we substitute "x*3" and every place we see "b" we substitute "y+10".
    */
    pList = pParent->pEList;
    for(i=0; i<pList->nExpr; i++){
      if( pList->a[i].zName==0 ){
        const char *zSpan = pList->a[i].zSpan;
        if( ALWAYS(zSpan) ){
          pList->a[i].zName = sqlite3DbStrDup(db, zSpan);
        }
      }
    }
    substExprList(db, pParent->pEList, iParent, pSub->pEList);
    if( isAgg ){
      substExprList(db, pParent->pGroupBy, iParent, pSub->pEList);
      pParent->pHaving = substExpr(db, pParent->pHaving, iParent, pSub->pEList);
    }
    if( pSub->pOrderBy ){
      assert( pParent->pOrderBy==0 );
      pParent->pOrderBy = pSub->pOrderBy;
      pSub->pOrderBy = 0;
    }else if( pParent->pOrderBy ){
      substExprList(db, pParent->pOrderBy, iParent, pSub->pEList);
    }
    if( pSub->pWhere ){
      pWhere = sqlite3ExprDup(db, pSub->pWhere, 0);
    }else{
      pWhere = 0;
    }
    if( subqueryIsAgg ){
      assert( pParent->pHaving==0 );
      pParent->pHaving = pParent->pWhere;
      pParent->pWhere = pWhere;
      pParent->pHaving = substExpr(db, pParent->pHaving, iParent, pSub->pEList);
      pParent->pHaving = sqlite3ExprAnd(db, pParent->pHaving, 
                                  sqlite3ExprDup(db, pSub->pHaving, 0));
      assert( pParent->pGroupBy==0 );
      pParent->pGroupBy = sqlite3ExprListDup(db, pSub->pGroupBy, 0);
    }else{
      pParent->pWhere = substExpr(db, pParent->pWhere, iParent, pSub->pEList);
      pParent->pWhere = sqlite3ExprAnd(db, pParent->pWhere, pWhere);
    }
  
    /* The flattened query is distinct if either the inner or the
    ** outer query is distinct. 
    */
    pParent->selFlags |= pSub->selFlags & SF_Distinct;
  
    /*
    ** SELECT ... FROM (SELECT ... LIMIT a OFFSET b) LIMIT x OFFSET y;
    **
    ** One is tempted to try to add a and b to combine the limits.  But this
    ** does not work if either limit is negative.
    */
    if( pSub->pLimit ){
      pParent->pLimit = pSub->pLimit;
      pSub->pLimit = 0;
    }
  }

  /* Finially, delete what is left of the subquery and return
  ** success.
  */
  sqlite3SelectDelete(db, pSub1);

  return 1;
}
#endif /* !defined(SQLITE_OMIT_SUBQUERY) || !defined(SQLITE_OMIT_VIEW) */

/*
** Analyze the SELECT statement passed as an argument to see if it
** is a min() or max() query. Return WHERE_ORDERBY_MIN or WHERE_ORDERBY_MAX if 
** it is, or 0 otherwise. At present, a query is considered to be
** a min()/max() query if:
**
**   1. There is a single object in the FROM clause.
**
**   2. There is a single expression in the result set, and it is
**      either min(x) or max(x), where x is a column reference.
*/
static u8 minMaxQuery(Select *p){
  Expr *pExpr;
  ExprList *pEList = p->pEList;

  if( pEList->nExpr!=1 ) return WHERE_ORDERBY_NORMAL;
  pExpr = pEList->a[0].pExpr;
  if( pExpr->op!=TK_AGG_FUNCTION ) return 0;
  if( NEVER(ExprHasProperty(pExpr, EP_xIsSelect)) ) return 0;
  pEList = pExpr->x.pList;
  if( pEList==0 || pEList->nExpr!=1 ) return 0;
  if( pEList->a[0].pExpr->op!=TK_AGG_COLUMN ) return WHERE_ORDERBY_NORMAL;
  assert( !ExprHasProperty(pExpr, EP_IntValue) );
  if( sqlite3StrICmp(pExpr->u.zToken,"min")==0 ){
    return WHERE_ORDERBY_MIN;
  }else if( sqlite3StrICmp(pExpr->u.zToken,"max")==0 ){
    return WHERE_ORDERBY_MAX;
  }
  return WHERE_ORDERBY_NORMAL;
}

/*
** The select statement passed as the first argument is an aggregate query.
** The second argment is the associated aggregate-info object. This 
** function tests if the SELECT is of the form:
**
**   SELECT count(*) FROM <tbl>
**
** where table is a database table, not a sub-select or view. If the query
** does match this pattern, then a pointer to the Table object representing
** <tbl> is returned. Otherwise, 0 is returned.
*/
static Table *isSimpleCount(Select *p, AggInfo *pAggInfo){
  Table *pTab;
  Expr *pExpr;

  assert( !p->pGroupBy );

  if( p->pWhere || p->pEList->nExpr!=1 
   || p->pSrc->nSrc!=1 || p->pSrc->a[0].pSelect
  ){
    return 0;
  }
  pTab = p->pSrc->a[0].pTab;
  pExpr = p->pEList->a[0].pExpr;
  assert( pTab && !pTab->pSelect && pExpr );

  if( IsVirtual(pTab) ) return 0;
  if( pExpr->op!=TK_AGG_FUNCTION ) return 0;
  if( (pAggInfo->aFunc[0].pFunc->flags&SQLITE_FUNC_COUNT)==0 ) return 0;
  if( pExpr->flags&EP_Distinct ) return 0;

  return pTab;
}

/*
** If the source-list item passed as an argument was augmented with an
** INDEXED BY clause, then try to locate the specified index. If there
** was such a clause and the named index cannot be found, return 
** SQLITE_ERROR and leave an error in pParse. Otherwise, populate 
** pFrom->pIndex and return SQLITE_OK.
*/
int sqlite3IndexedByLookup(Parse *pParse, struct SrcList_item *pFrom){
  if( pFrom->pTab && pFrom->zIndex ){
    Table *pTab = pFrom->pTab;
    char *zIndex = pFrom->zIndex;
    Index *pIdx;
    for(pIdx=pTab->pIndex; 
        pIdx && sqlite3StrICmp(pIdx->zName, zIndex); 
        pIdx=pIdx->pNext
    );
    if( !pIdx ){
      sqlite3ErrorMsg(pParse, "no such index: %s", zIndex, 0);
      return SQLITE_ERROR;
    }
    pFrom->pIndex = pIdx;
  }
  return SQLITE_OK;
}

/*
** This routine is a Walker callback for "expanding" a SELECT statement.
** "Expanding" means to do the following:
**
**    (1)  Make sure VDBE cursor numbers have been assigned to every
**         element of the FROM clause.
**
**    (2)  Fill in the pTabList->a[].pTab fields in the SrcList that 
**         defines FROM clause.  When views appear in the FROM clause,
**         fill pTabList->a[].pSelect with a copy of the SELECT statement
**         that implements the view.  A copy is made of the view's SELECT
**         statement so that we can freely modify or delete that statement
**         without worrying about messing up the presistent representation
**         of the view.
**
**    (3)  Add terms to the WHERE clause to accomodate the NATURAL keyword
**         on joins and the ON and USING clause of joins.
**
**    (4)  Scan the list of columns in the result set (pEList) looking
**         for instances of the "*" operator or the TABLE.* operator.
**         If found, expand each "*" to be every column in every table
**         and TABLE.* to be every column in TABLE.
**
*/
static int selectExpander(Walker *pWalker, Select *p){
  Parse *pParse = pWalker->pParse;
  int i, j, k;
  SrcList *pTabList;
  ExprList *pEList;
  struct SrcList_item *pFrom;
  sqlite3 *db = pParse->db;

  if( db->mallocFailed  ){
    return WRC_Abort;
  }
  if( NEVER(p->pSrc==0) || (p->selFlags & SF_Expanded)!=0 ){
    return WRC_Prune;
  }
  p->selFlags |= SF_Expanded;
  pTabList = p->pSrc;
  pEList = p->pEList;

  /* Make sure cursor numbers have been assigned to all entries in
  ** the FROM clause of the SELECT statement.
  */
  sqlite3SrcListAssignCursors(pParse, pTabList);

  /* Look up every table named in the FROM clause of the select.  If
  ** an entry of the FROM clause is a subquery instead of a table or view,
  ** then create a transient table structure to describe the subquery.
  */
  for(i=0, pFrom=pTabList->a; i<pTabList->nSrc; i++, pFrom++){
    Table *pTab;
    if( pFrom->pTab!=0 ){
      /* This statement has already been prepared.  There is no need
      ** to go further. */
      assert( i==0 );
      return WRC_Prune;
    }
    if( pFrom->zName==0 ){
#ifndef SQLITE_OMIT_SUBQUERY
      Select *pSel = pFrom->pSelect;
      /* A sub-query in the FROM clause of a SELECT */
      assert( pSel!=0 );
      assert( pFrom->pTab==0 );
      sqlite3WalkSelect(pWalker, pSel);
      pFrom->pTab = pTab = sqlite3DbMallocZero(db, sizeof(Table));
      if( pTab==0 ) return WRC_Abort;
      pTab->dbMem = db->lookaside.bEnabled ? db : 0;
      pTab->nRef = 1;
      pTab->zName = sqlite3MPrintf(db, "sqlite_subquery_%p_", (void*)pTab);
      while( pSel->pPrior ){ pSel = pSel->pPrior; }
      selectColumnsFromExprList(pParse, pSel->pEList, &pTab->nCol, &pTab->aCol);
      pTab->iPKey = -1;
      pTab->tabFlags |= TF_Ephemeral;
#endif
    }else{
      /* An ordinary table or view name in the FROM clause */
      assert( pFrom->pTab==0 );
      pFrom->pTab = pTab = 
        sqlite3LocateTable(pParse,0,pFrom->zName,pFrom->zDatabase);
      if( pTab==0 ) return WRC_Abort;
      pTab->nRef++;
#if !defined(SQLITE_OMIT_VIEW) || !defined (SQLITE_OMIT_VIRTUALTABLE)
      if( pTab->pSelect || IsVirtual(pTab) ){
        /* We reach here if the named table is a really a view */
        if( sqlite3ViewGetColumnNames(pParse, pTab) ) return WRC_Abort;
        assert( pFrom->pSelect==0 );
        pFrom->pSelect = sqlite3SelectDup(db, pTab->pSelect, 0);
        sqlite3WalkSelect(pWalker, pFrom->pSelect);
      }
#endif
    }

    /* Locate the index named by the INDEXED BY clause, if any. */
    if( sqlite3IndexedByLookup(pParse, pFrom) ){
      return WRC_Abort;
    }
  }

  /* Process NATURAL keywords, and ON and USING clauses of joins.
  */
  if( db->mallocFailed || sqliteProcessJoin(pParse, p) ){
    return WRC_Abort;
  }

  /* For every "*" that occurs in the column list, insert the names of
  ** all columns in all tables.  And for every TABLE.* insert the names
  ** of all columns in TABLE.  The parser inserted a special expression
  ** with the TK_ALL operator for each "*" that it found in the column list.
  ** The following code just has to locate the TK_ALL expressions and expand
  ** each one to the list of all columns in all tables.
  **
  ** The first loop just checks to see if there are any "*" operators
  ** that need expanding.
  */
  for(k=0; k<pEList->nExpr; k++){
    Expr *pE = pEList->a[k].pExpr;
    if( pE->op==TK_ALL ) break;
    assert( pE->op!=TK_DOT || pE->pRight!=0 );
    assert( pE->op!=TK_DOT || (pE->pLeft!=0 && pE->pLeft->op==TK_ID) );
    if( pE->op==TK_DOT && pE->pRight->op==TK_ALL ) break;
  }
  if( k<pEList->nExpr ){
    /*
    ** If we get here it means the result set contains one or more "*"
    ** operators that need to be expanded.  Loop through each expression
    ** in the result set and expand them one by one.
    */
    struct ExprList_item *a = pEList->a;
    ExprList *pNew = 0;
    int flags = pParse->db->flags;
    int longNames = (flags & SQLITE_FullColNames)!=0
                      && (flags & SQLITE_ShortColNames)==0;

    for(k=0; k<pEList->nExpr; k++){
      Expr *pE = a[k].pExpr;
      assert( pE->op!=TK_DOT || pE->pRight!=0 );
      if( pE->op!=TK_ALL && (pE->op!=TK_DOT || pE->pRight->op!=TK_ALL) ){
        /* This particular expression does not need to be expanded.
        */
        pNew = sqlite3ExprListAppend(pParse, pNew, a[k].pExpr);
        if( pNew ){
          pNew->a[pNew->nExpr-1].zName = a[k].zName;
          pNew->a[pNew->nExpr-1].zSpan = a[k].zSpan;
          a[k].zName = 0;
          a[k].zSpan = 0;
        }
        a[k].pExpr = 0;
      }else{
        /* This expression is a "*" or a "TABLE.*" and needs to be
        ** expanded. */
        int tableSeen = 0;      /* Set to 1 when TABLE matches */
        char *zTName;            /* text of name of TABLE */
        if( pE->op==TK_DOT ){
          assert( pE->pLeft!=0 );
          assert( !ExprHasProperty(pE->pLeft, EP_IntValue) );
          zTName = pE->pLeft->u.zToken;
        }else{
          zTName = 0;
        }
        for(i=0, pFrom=pTabList->a; i<pTabList->nSrc; i++, pFrom++){
          Table *pTab = pFrom->pTab;
          char *zTabName = pFrom->zAlias;
          if( zTabName==0 ){
            zTabName = pTab->zName;
          }
          if( db->mallocFailed ) break;
          if( zTName && sqlite3StrICmp(zTName, zTabName)!=0 ){
            continue;
          }
          tableSeen = 1;
          for(j=0; j<pTab->nCol; j++){
            Expr *pExpr, *pRight;
            char *zName = pTab->aCol[j].zName;
            char *zColname;  /* The computed column name */
            char *zToFree;   /* Malloced string that needs to be freed */
            Token sColname;  /* Computed column name as a token */

            /* If a column is marked as 'hidden' (currently only possible
            ** for virtual tables), do not include it in the expanded
            ** result-set list.
            */
            if( IsHiddenColumn(&pTab->aCol[j]) ){
              assert(IsVirtual(pTab));
              continue;
            }

            if( i>0 && zTName==0 ){
              struct SrcList_item *pLeft = &pTabList->a[i-1];
              if( (pLeft[1].jointype & JT_NATURAL)!=0 &&
                        columnIndex(pLeft->pTab, zName)>=0 ){
                /* In a NATURAL join, omit the join columns from the 
                ** table on the right */
                continue;
              }
              if( sqlite3IdListIndex(pLeft[1].pUsing, zName)>=0 ){
                /* In a join with a USING clause, omit columns in the
                ** using clause from the table on the right. */
                continue;
              }
            }
            pRight = sqlite3Expr(db, TK_ID, zName);
            zColname = zName;
            zToFree = 0;
            if( longNames || pTabList->nSrc>1 ){
              Expr *pLeft;
              pLeft = sqlite3Expr(db, TK_ID, zTabName);
              pExpr = sqlite3PExpr(pParse, TK_DOT, pLeft, pRight, 0);
              if( longNames ){
                zColname = sqlite3MPrintf(db, "%s.%s", zTabName, zName);
                zToFree = zColname;
              }
            }else{
              pExpr = pRight;
            }
            pNew = sqlite3ExprListAppend(pParse, pNew, pExpr);
            sColname.z = zColname;
            sColname.n = sqlite3Strlen30(zColname);
            sqlite3ExprListSetName(pParse, pNew, &sColname, 0);
            sqlite3DbFree(db, zToFree);
          }
        }
        if( !tableSeen ){
          if( zTName ){
            sqlite3ErrorMsg(pParse, "no such table: %s", zTName);
          }else{
            sqlite3ErrorMsg(pParse, "no tables specified");
          }
        }
      }
    }
    sqlite3ExprListDelete(db, pEList);
    p->pEList = pNew;
  }
#if SQLITE_MAX_COLUMN
  if( p->pEList && p->pEList->nExpr>db->aLimit[SQLITE_LIMIT_COLUMN] ){
    sqlite3ErrorMsg(pParse, "too many columns in result set");
  }
#endif
  return WRC_Continue;
}

/*
** No-op routine for the parse-tree walker.
**
** When this routine is the Walker.xExprCallback then expression trees
** are walked without any actions being taken at each node.  Presumably,
** when this routine is used for Walker.xExprCallback then 
** Walker.xSelectCallback is set to do something useful for every 
** subquery in the parser tree.
*/
static int exprWalkNoop(Walker *NotUsed, Expr *NotUsed2){
  UNUSED_PARAMETER2(NotUsed, NotUsed2);
  return WRC_Continue;
}

/*
** This routine "expands" a SELECT statement and all of its subqueries.
** For additional information on what it means to "expand" a SELECT
** statement, see the comment on the selectExpand worker callback above.
**
** Expanding a SELECT statement is the first step in processing a
** SELECT statement.  The SELECT statement must be expanded before
** name resolution is performed.
**
** If anything goes wrong, an error message is written into pParse.
** The calling function can detect the problem by looking at pParse->nErr
** and/or pParse->db->mallocFailed.
*/
static void sqlite3SelectExpand(Parse *pParse, Select *pSelect){
  Walker w;
  w.xSelectCallback = selectExpander;
  w.xExprCallback = exprWalkNoop;
  w.pParse = pParse;
  sqlite3WalkSelect(&w, pSelect);
}


#ifndef SQLITE_OMIT_SUBQUERY
/*
** This is a Walker.xSelectCallback callback for the sqlite3SelectTypeInfo()
** interface.
**
** For each FROM-clause subquery, add Column.zType and Column.zColl
** information to the Table structure that represents the result set
** of that subquery.
**
** The Table structure that represents the result set was constructed
** by selectExpander() but the type and collation information was omitted
** at that point because identifiers had not yet been resolved.  This
** routine is called after identifier resolution.
*/
static int selectAddSubqueryTypeInfo(Walker *pWalker, Select *p){
  Parse *pParse;
  int i;
  SrcList *pTabList;
  struct SrcList_item *pFrom;

  assert( p->selFlags & SF_Resolved );
  assert( (p->selFlags & SF_HasTypeInfo)==0 );
  p->selFlags |= SF_HasTypeInfo;
  pParse = pWalker->pParse;
  pTabList = p->pSrc;
  for(i=0, pFrom=pTabList->a; i<pTabList->nSrc; i++, pFrom++){
    Table *pTab = pFrom->pTab;
    if( ALWAYS(pTab!=0) && (pTab->tabFlags & TF_Ephemeral)!=0 ){
      /* A sub-query in the FROM clause of a SELECT */
      Select *pSel = pFrom->pSelect;
      assert( pSel );
      while( pSel->pPrior ) pSel = pSel->pPrior;
      selectAddColumnTypeAndCollation(pParse, pTab->nCol, pTab->aCol, pSel);
    }
  }
  return WRC_Continue;
}
#endif


/*
** This routine adds datatype and collating sequence information to
** the Table structures of all FROM-clause subqueries in a
** SELECT statement.
**
** Use this routine after name resolution.
*/
static void sqlite3SelectAddTypeInfo(Parse *pParse, Select *pSelect){
#ifndef SQLITE_OMIT_SUBQUERY
  Walker w;
  w.xSelectCallback = selectAddSubqueryTypeInfo;
  w.xExprCallback = exprWalkNoop;
  w.pParse = pParse;
  sqlite3WalkSelect(&w, pSelect);
#endif
}


/*
** This routine sets of a SELECT statement for processing.  The
** following is accomplished:
**
**     *  VDBE Cursor numbers are assigned to all FROM-clause terms.
**     *  Ephemeral Table objects are created for all FROM-clause subqueries.
**     *  ON and USING clauses are shifted into WHERE statements
**     *  Wildcards "*" and "TABLE.*" in result sets are expanded.
**     *  Identifiers in expression are matched to tables.
**
** This routine acts recursively on all subqueries within the SELECT.
*/
void sqlite3SelectPrep(
  Parse *pParse,         /* The parser context */
  Select *p,             /* The SELECT statement being coded. */
  NameContext *pOuterNC  /* Name context for container */
){
  sqlite3 *db;
  if( NEVER(p==0) ) return;
  db = pParse->db;
  if( p->selFlags & SF_HasTypeInfo ) return;
  sqlite3SelectExpand(pParse, p);
  if( pParse->nErr || db->mallocFailed ) return;
  sqlite3ResolveSelectNames(pParse, p, pOuterNC);
  if( pParse->nErr || db->mallocFailed ) return;
  sqlite3SelectAddTypeInfo(pParse, p);
}

/*
** Reset the aggregate accumulator.
**
** The aggregate accumulator is a set of memory cells that hold
** intermediate results while calculating an aggregate.  This
** routine simply stores NULLs in all of those memory cells.
*/
static void resetAccumulator(Parse *pParse, AggInfo *pAggInfo){
  Vdbe *v = pParse->pVdbe;
  int i;
  struct AggInfo_func *pFunc;
  if( pAggInfo->nFunc+pAggInfo->nColumn==0 ){
    return;
  }
  for(i=0; i<pAggInfo->nColumn; i++){
    sqlite3VdbeAddOp2(v, OP_Null, 0, pAggInfo->aCol[i].iMem);
  }
  for(pFunc=pAggInfo->aFunc, i=0; i<pAggInfo->nFunc; i++, pFunc++){
    sqlite3VdbeAddOp2(v, OP_Null, 0, pFunc->iMem);
    if( pFunc->iDistinct>=0 ){
      Expr *pE = pFunc->pExpr;
      assert( !ExprHasProperty(pE, EP_xIsSelect) );
      if( pE->x.pList==0 || pE->x.pList->nExpr!=1 ){
        sqlite3ErrorMsg(pParse, "DISTINCT aggregates must have exactly one "
           "argument");
        pFunc->iDistinct = -1;
      }else{
        KeyInfo *pKeyInfo = keyInfoFromExprList(pParse, pE->x.pList);
        sqlite3VdbeAddOp4(v, OP_OpenEphemeral, pFunc->iDistinct, 0, 0,
                          (char*)pKeyInfo, P4_KEYINFO_HANDOFF);
      }
    }
  }
}

/*
** Invoke the OP_AggFinalize opcode for every aggregate function
** in the AggInfo structure.
*/
static void finalizeAggFunctions(Parse *pParse, AggInfo *pAggInfo){
  Vdbe *v = pParse->pVdbe;
  int i;
  struct AggInfo_func *pF;
  for(i=0, pF=pAggInfo->aFunc; i<pAggInfo->nFunc; i++, pF++){
    ExprList *pList = pF->pExpr->x.pList;
    assert( !ExprHasProperty(pF->pExpr, EP_xIsSelect) );
    sqlite3VdbeAddOp4(v, OP_AggFinal, pF->iMem, pList ? pList->nExpr : 0, 0,
                      (void*)pF->pFunc, P4_FUNCDEF);
  }
}

/*
** Update the accumulator memory cells for an aggregate based on
** the current cursor position.
*/
static void updateAccumulator(Parse *pParse, AggInfo *pAggInfo){
  Vdbe *v = pParse->pVdbe;
  int i;
  struct AggInfo_func *pF;
  struct AggInfo_col *pC;

  pAggInfo->directMode = 1;
  sqlite3ExprCacheClear(pParse);
  for(i=0, pF=pAggInfo->aFunc; i<pAggInfo->nFunc; i++, pF++){
    int nArg;
    int addrNext = 0;
    int regAgg;
    ExprList *pList = pF->pExpr->x.pList;
    assert( !ExprHasProperty(pF->pExpr, EP_xIsSelect) );
    if( pList ){
      nArg = pList->nExpr;
      regAgg = sqlite3GetTempRange(pParse, nArg);
      sqlite3ExprCodeExprList(pParse, pList, regAgg, 0);
    }else{
      nArg = 0;
      regAgg = 0;
    }
    if( pF->iDistinct>=0 ){
      addrNext = sqlite3VdbeMakeLabel(v);
      assert( nArg==1 );
      codeDistinct(pParse, pF->iDistinct, addrNext, 1, regAgg);
    }
    if( pF->pFunc->flags & SQLITE_FUNC_NEEDCOLL ){
      CollSeq *pColl = 0;
      struct ExprList_item *pItem;
      int j;
      assert( pList!=0 );  /* pList!=0 if pF->pFunc has NEEDCOLL */
      for(j=0, pItem=pList->a; !pColl && j<nArg; j++, pItem++){
        pColl = sqlite3ExprCollSeq(pParse, pItem->pExpr);
      }
      if( !pColl ){
        pColl = pParse->db->pDfltColl;
      }
      sqlite3VdbeAddOp4(v, OP_CollSeq, 0, 0, 0, (char *)pColl, P4_COLLSEQ);
    }
    sqlite3VdbeAddOp4(v, OP_AggStep, 0, regAgg, pF->iMem,
                      (void*)pF->pFunc, P4_FUNCDEF);
    sqlite3VdbeChangeP5(v, (u8)nArg);
    sqlite3ReleaseTempRange(pParse, regAgg, nArg);
    sqlite3ExprCacheAffinityChange(pParse, regAgg, nArg);
    if( addrNext ){
      sqlite3VdbeResolveLabel(v, addrNext);
      sqlite3ExprCacheClear(pParse);
    }
  }
  for(i=0, pC=pAggInfo->aCol; i<pAggInfo->nAccumulator; i++, pC++){
    sqlite3ExprCode(pParse, pC->pExpr, pC->iMem);
  }
  pAggInfo->directMode = 0;
  sqlite3ExprCacheClear(pParse);
}

/*
** Generate code for the SELECT statement given in the p argument.  
**
** The results are distributed in various ways depending on the
** contents of the SelectDest structure pointed to by argument pDest
** as follows:
**
**     pDest->eDest    Result
**     ------------    -------------------------------------------
**     SRT_Output      Generate a row of output (using the OP_ResultRow
**                     opcode) for each row in the result set.
**
**     SRT_Mem         Only valid if the result is a single column.
**                     Store the first column of the first result row
**                     in register pDest->iParm then abandon the rest
**                     of the query.  This destination implies "LIMIT 1".
**
**     SRT_Set         The result must be a single column.  Store each
**                     row of result as the key in table pDest->iParm. 
**                     Apply the affinity pDest->affinity before storing
**                     results.  Used to implement "IN (SELECT ...)".
**
**     SRT_Union       Store results as a key in a temporary table pDest->iParm.
**
**     SRT_Except      Remove results from the temporary table pDest->iParm.
**
**     SRT_Table       Store results in temporary table pDest->iParm.
**                     This is like SRT_EphemTab except that the table
**                     is assumed to already be open.
**
**     SRT_EphemTab    Create an temporary table pDest->iParm and store
**                     the result there. The cursor is left open after
**                     returning.  This is like SRT_Table except that
**                     this destination uses OP_OpenEphemeral to create
**                     the table first.
**
**     SRT_Coroutine   Generate a co-routine that returns a new row of
**                     results each time it is invoked.  The entry point
**                     of the co-routine is stored in register pDest->iParm.
**
**     SRT_Exists      Store a 1 in memory cell pDest->iParm if the result
**                     set is not empty.
**
**     SRT_Discard     Throw the results away.  This is used by SELECT
**                     statements within triggers whose only purpose is
**                     the side-effects of functions.
**
** This routine returns the number of errors.  If any errors are
** encountered, then an appropriate error message is left in
** pParse->zErrMsg.
**
** This routine does NOT free the Select structure passed in.  The
** calling function needs to do that.
*/
int sqlite3Select(
  Parse *pParse,         /* The parser context */
  Select *p,             /* The SELECT statement being coded. */
  SelectDest *pDest      /* What to do with the query results */
){
  int i, j;              /* Loop counters */
  WhereInfo *pWInfo;     /* Return from sqlite3WhereBegin() */
  Vdbe *v;               /* The virtual machine under construction */
  int isAgg;             /* True for select lists like "count(*)" */
  ExprList *pEList;      /* List of columns to extract. */
  SrcList *pTabList;     /* List of tables to select from */
  Expr *pWhere;          /* The WHERE clause.  May be NULL */
  ExprList *pOrderBy;    /* The ORDER BY clause.  May be NULL */
  ExprList *pGroupBy;    /* The GROUP BY clause.  May be NULL */
  Expr *pHaving;         /* The HAVING clause.  May be NULL */
  int isDistinct;        /* True if the DISTINCT keyword is present */
  int distinct;          /* Table to use for the distinct set */
  int rc = 1;            /* Value to return from this function */
  int addrSortIndex;     /* Address of an OP_OpenEphemeral instruction */
  AggInfo sAggInfo;      /* Information used by aggregate queries */
  int iEnd;              /* Address of the end of the query */
  sqlite3 *db;           /* The database connection */

  db = pParse->db;
  if( p==0 || db->mallocFailed || pParse->nErr ){
    return 1;
  }
  if( sqlite3AuthCheck(pParse, SQLITE_SELECT, 0, 0, 0) ) return 1;
  memset(&sAggInfo, 0, sizeof(sAggInfo));

  if( IgnorableOrderby(pDest) ){
    assert(pDest->eDest==SRT_Exists || pDest->eDest==SRT_Union || 
           pDest->eDest==SRT_Except || pDest->eDest==SRT_Discard);
    /* If ORDER BY makes no difference in the output then neither does
    ** DISTINCT so it can be removed too. */
    sqlite3ExprListDelete(db, p->pOrderBy);
    p->pOrderBy = 0;
    p->selFlags &= ~SF_Distinct;
  }
  sqlite3SelectPrep(pParse, p, 0);
  pOrderBy = p->pOrderBy;
  pTabList = p->pSrc;
  pEList = p->pEList;
  if( pParse->nErr || db->mallocFailed ){
    goto select_end;
  }
  isAgg = (p->selFlags & SF_Aggregate)!=0;
  assert( pEList!=0 );

  /* Begin generating code.
  */
  v = sqlite3GetVdbe(pParse);
  if( v==0 ) goto select_end;

  /* Generate code for all sub-queries in the FROM clause
  */
#if !defined(SQLITE_OMIT_SUBQUERY) || !defined(SQLITE_OMIT_VIEW)
  for(i=0; !p->pPrior && i<pTabList->nSrc; i++){
    struct SrcList_item *pItem = &pTabList->a[i];
    SelectDest dest;
    Select *pSub = pItem->pSelect;
    int isAggSub;

    if( pSub==0 || pItem->isPopulated ) continue;

    /* Increment Parse.nHeight by the height of the largest expression
    ** tree refered to by this, the parent select. The child select
    ** may contain expression trees of at most
    ** (SQLITE_MAX_EXPR_DEPTH-Parse.nHeight) height. This is a bit
    ** more conservative than necessary, but much easier than enforcing
    ** an exact limit.
    */
    pParse->nHeight += sqlite3SelectExprHeight(p);

    /* Check to see if the subquery can be absorbed into the parent. */
    isAggSub = (pSub->selFlags & SF_Aggregate)!=0;
    if( flattenSubquery(pParse, p, i, isAgg, isAggSub) ){
      if( isAggSub ){
        isAgg = 1;
        p->selFlags |= SF_Aggregate;
      }
      i = -1;
    }else{
      sqlite3SelectDestInit(&dest, SRT_EphemTab, pItem->iCursor);
      assert( pItem->isPopulated==0 );
      sqlite3Select(pParse, pSub, &dest);
      pItem->isPopulated = 1;
    }
    if( /*pParse->nErr ||*/ db->mallocFailed ){
      goto select_end;
    }
    pParse->nHeight -= sqlite3SelectExprHeight(p);
    pTabList = p->pSrc;
    if( !IgnorableOrderby(pDest) ){
      pOrderBy = p->pOrderBy;
    }
  }
  pEList = p->pEList;
#endif
  pWhere = p->pWhere;
  pGroupBy = p->pGroupBy;
  pHaving = p->pHaving;
  isDistinct = (p->selFlags & SF_Distinct)!=0;

#ifndef SQLITE_OMIT_COMPOUND_SELECT
  /* If there is are a sequence of queries, do the earlier ones first.
  */
  if( p->pPrior ){
    if( p->pRightmost==0 ){
      Select *pLoop, *pRight = 0;
      int cnt = 0;
      int mxSelect;
      for(pLoop=p; pLoop; pLoop=pLoop->pPrior, cnt++){
        pLoop->pRightmost = p;
        pLoop->pNext = pRight;
        pRight = pLoop;
      }
      mxSelect = db->aLimit[SQLITE_LIMIT_COMPOUND_SELECT];
      if( mxSelect && cnt>mxSelect ){
        sqlite3ErrorMsg(pParse, "too many terms in compound SELECT");
        return 1;
      }
    }
    return multiSelect(pParse, p, pDest);
  }
#endif

  /* If writing to memory or generating a set
  ** only a single column may be output.
  */
#ifndef SQLITE_OMIT_SUBQUERY
  if( checkForMultiColumnSelectError(pParse, pDest, pEList->nExpr) ){
    goto select_end;
  }
#endif

  /* If possible, rewrite the query to use GROUP BY instead of DISTINCT.
  ** GROUP BY might use an index, DISTINCT never does.
  */
  assert( p->pGroupBy==0 || (p->selFlags & SF_Aggregate)!=0 );
  if( (p->selFlags & (SF_Distinct|SF_Aggregate))==SF_Distinct ){
    p->pGroupBy = sqlite3ExprListDup(db, p->pEList, 0);
    pGroupBy = p->pGroupBy;
    p->selFlags &= ~SF_Distinct;
    isDistinct = 0;
  }

  /* If there is an ORDER BY clause, then this sorting
  ** index might end up being unused if the data can be 
  ** extracted in pre-sorted order.  If that is the case, then the
  ** OP_OpenEphemeral instruction will be changed to an OP_Noop once
  ** we figure out that the sorting index is not needed.  The addrSortIndex
  ** variable is used to facilitate that change.
  */
  if( pOrderBy ){
    KeyInfo *pKeyInfo;
    pKeyInfo = keyInfoFromExprList(pParse, pOrderBy);
    pOrderBy->iECursor = pParse->nTab++;
    p->addrOpenEphm[2] = addrSortIndex =
      sqlite3VdbeAddOp4(v, OP_OpenEphemeral,
                           pOrderBy->iECursor, pOrderBy->nExpr+2, 0,
                           (char*)pKeyInfo, P4_KEYINFO_HANDOFF);
  }else{
    addrSortIndex = -1;
  }

  /* If the output is destined for a temporary table, open that table.
  */
  if( pDest->eDest==SRT_EphemTab ){
    sqlite3VdbeAddOp2(v, OP_OpenEphemeral, pDest->iParm, pEList->nExpr);
  }

  /* Set the limiter.
  */
  iEnd = sqlite3VdbeMakeLabel(v);
  computeLimitRegisters(pParse, p, iEnd);

  /* Open a virtual index to use for the distinct set.
  */
  if( isDistinct ){
    KeyInfo *pKeyInfo;
    assert( isAgg || pGroupBy );
    distinct = pParse->nTab++;
    pKeyInfo = keyInfoFromExprList(pParse, p->pEList);
    sqlite3VdbeAddOp4(v, OP_OpenEphemeral, distinct, 0, 0,
                        (char*)pKeyInfo, P4_KEYINFO_HANDOFF);
  }else{
    distinct = -1;
  }

  /* Aggregate and non-aggregate queries are handled differently */
  if( !isAgg && pGroupBy==0 ){
    /* This case is for non-aggregate queries
    ** Begin the database scan
    */
    pWInfo = sqlite3WhereBegin(pParse, pTabList, pWhere, &pOrderBy, 0);
    if( pWInfo==0 ) goto select_end;

    /* If sorting index that was created by a prior OP_OpenEphemeral 
    ** instruction ended up not being needed, then change the OP_OpenEphemeral
    ** into an OP_Noop.
    */
    if( addrSortIndex>=0 && pOrderBy==0 ){
      sqlite3VdbeChangeToNoop(v, addrSortIndex, 1);
      p->addrOpenEphm[2] = -1;
    }

    /* Use the standard inner loop
    */
    assert(!isDistinct);
    selectInnerLoop(pParse, p, pEList, 0, 0, pOrderBy, -1, pDest,
                    pWInfo->iContinue, pWInfo->iBreak);

    /* End the database scan loop.
    */
    sqlite3WhereEnd(pWInfo);
  }else{
    /* This is the processing for aggregate queries */
    NameContext sNC;    /* Name context for processing aggregate information */
    int iAMem;          /* First Mem address for storing current GROUP BY */
    int iBMem;          /* First Mem address for previous GROUP BY */
    int iUseFlag;       /* Mem address holding flag indicating that at least
                        ** one row of the input to the aggregator has been
                        ** processed */
    int iAbortFlag;     /* Mem address which causes query abort if positive */
    int groupBySort;    /* Rows come from source in GROUP BY order */
    int addrEnd;        /* End of processing for this SELECT */

    /* Remove any and all aliases between the result set and the
    ** GROUP BY clause.
    */
    if( pGroupBy ){
      int k;                        /* Loop counter */
      struct ExprList_item *pItem;  /* For looping over expression in a list */

      for(k=p->pEList->nExpr, pItem=p->pEList->a; k>0; k--, pItem++){
        pItem->iAlias = 0;
      }
      for(k=pGroupBy->nExpr, pItem=pGroupBy->a; k>0; k--, pItem++){
        pItem->iAlias = 0;
      }
    }

 
    /* Create a label to jump to when we want to abort the query */
    addrEnd = sqlite3VdbeMakeLabel(v);

    /* Convert TK_COLUMN nodes into TK_AGG_COLUMN and make entries in
    ** sAggInfo for all TK_AGG_FUNCTION nodes in expressions of the
    ** SELECT statement.
    */
    memset(&sNC, 0, sizeof(sNC));
    sNC.pParse = pParse;
    sNC.pSrcList = pTabList;
    sNC.pAggInfo = &sAggInfo;
    sAggInfo.nSortingColumn = pGroupBy ? pGroupBy->nExpr+1 : 0;
    sAggInfo.pGroupBy = pGroupBy;
    sqlite3ExprAnalyzeAggList(&sNC, pEList);
    sqlite3ExprAnalyzeAggList(&sNC, pOrderBy);
    if( pHaving ){
      sqlite3ExprAnalyzeAggregates(&sNC, pHaving);
    }
    sAggInfo.nAccumulator = sAggInfo.nColumn;
    for(i=0; i<sAggInfo.nFunc; i++){
      assert( !ExprHasProperty(sAggInfo.aFunc[i].pExpr, EP_xIsSelect) );
      sqlite3ExprAnalyzeAggList(&sNC, sAggInfo.aFunc[i].pExpr->x.pList);
    }
    if( db->mallocFailed ) goto select_end;

    /* Processing for aggregates with GROUP BY is very different and
    ** much more complex than aggregates without a GROUP BY.
    */
    if( pGroupBy ){
      KeyInfo *pKeyInfo;  /* Keying information for the group by clause */
      int j1;             /* A-vs-B comparision jump */
      int addrOutputRow;  /* Start of subroutine that outputs a result row */
      int regOutputRow;   /* Return address register for output subroutine */
      int addrSetAbort;   /* Set the abort flag and return */
      int addrTopOfLoop;  /* Top of the input loop */
      int addrSortingIdx; /* The OP_OpenEphemeral for the sorting index */
      int addrReset;      /* Subroutine for resetting the accumulator */
      int regReset;       /* Return address register for reset subroutine */

      /* If there is a GROUP BY clause we might need a sorting index to
      ** implement it.  Allocate that sorting index now.  If it turns out
      ** that we do not need it after all, the OpenEphemeral instruction
      ** will be converted into a Noop.  
      */
      sAggInfo.sortingIdx = pParse->nTab++;
      pKeyInfo = keyInfoFromExprList(pParse, pGroupBy);
      addrSortingIdx = sqlite3VdbeAddOp4(v, OP_OpenEphemeral, 
          sAggInfo.sortingIdx, sAggInfo.nSortingColumn, 
          0, (char*)pKeyInfo, P4_KEYINFO_HANDOFF);

      /* Initialize memory locations used by GROUP BY aggregate processing
      */
      iUseFlag = ++pParse->nMem;
      iAbortFlag = ++pParse->nMem;
      regOutputRow = ++pParse->nMem;
      addrOutputRow = sqlite3VdbeMakeLabel(v);
      regReset = ++pParse->nMem;
      addrReset = sqlite3VdbeMakeLabel(v);
      iAMem = pParse->nMem + 1;
      pParse->nMem += pGroupBy->nExpr;
      iBMem = pParse->nMem + 1;
      pParse->nMem += pGroupBy->nExpr;
      sqlite3VdbeAddOp2(v, OP_Integer, 0, iAbortFlag);
      VdbeComment((v, "clear abort flag"));
      sqlite3VdbeAddOp2(v, OP_Integer, 0, iUseFlag);
      VdbeComment((v, "indicate accumulator empty"));

      /* Begin a loop that will extract all source rows in GROUP BY order.
      ** This might involve two separate loops with an OP_Sort in between, or
      ** it might be a single loop that uses an index to extract information
      ** in the right order to begin with.
      */
      sqlite3VdbeAddOp2(v, OP_Gosub, regReset, addrReset);
      pWInfo = sqlite3WhereBegin(pParse, pTabList, pWhere, &pGroupBy, 0);
      if( pWInfo==0 ) goto select_end;
      if( pGroupBy==0 ){
        /* The optimizer is able to deliver rows in group by order so
        ** we do not have to sort.  The OP_OpenEphemeral table will be
        ** cancelled later because we still need to use the pKeyInfo
        */
        pGroupBy = p->pGroupBy;
        groupBySort = 0;
      }else{
        /* Rows are coming out in undetermined order.  We have to push
        ** each row into a sorting index, terminate the first loop,
        ** then loop over the sorting index in order to get the output
        ** in sorted order
        */
        int regBase;
        int regRecord;
        int nCol;
        int nGroupBy;

        groupBySort = 1;
        nGroupBy = pGroupBy->nExpr;
        nCol = nGroupBy + 1;
        j = nGroupBy+1;
        for(i=0; i<sAggInfo.nColumn; i++){
          if( sAggInfo.aCol[i].iSorterColumn>=j ){
            nCol++;
            j++;
          }
        }
        regBase = sqlite3GetTempRange(pParse, nCol);
        sqlite3ExprCacheClear(pParse);
        sqlite3ExprCodeExprList(pParse, pGroupBy, regBase, 0);
        sqlite3VdbeAddOp2(v, OP_Sequence, sAggInfo.sortingIdx,regBase+nGroupBy);
        j = nGroupBy+1;
        for(i=0; i<sAggInfo.nColumn; i++){
          struct AggInfo_col *pCol = &sAggInfo.aCol[i];
          if( pCol->iSorterColumn>=j ){
            int r1 = j + regBase;
            int r2;

            r2 = sqlite3ExprCodeGetColumn(pParse, 
                               pCol->pTab, pCol->iColumn, pCol->iTable, r1, 0);
            if( r1!=r2 ){
              sqlite3VdbeAddOp2(v, OP_SCopy, r2, r1);
            }
            j++;
          }
        }
        regRecord = sqlite3GetTempReg(pParse);
        sqlite3VdbeAddOp3(v, OP_MakeRecord, regBase, nCol, regRecord);
        sqlite3VdbeAddOp2(v, OP_IdxInsert, sAggInfo.sortingIdx, regRecord);
        sqlite3ReleaseTempReg(pParse, regRecord);
        sqlite3ReleaseTempRange(pParse, regBase, nCol);
        sqlite3WhereEnd(pWInfo);
        sqlite3VdbeAddOp2(v, OP_Sort, sAggInfo.sortingIdx, addrEnd);
        VdbeComment((v, "GROUP BY sort"));
        sAggInfo.useSortingIdx = 1;
        sqlite3ExprCacheClear(pParse);
      }

      /* Evaluate the current GROUP BY terms and store in b0, b1, b2...
      ** (b0 is memory location iBMem+0, b1 is iBMem+1, and so forth)
      ** Then compare the current GROUP BY terms against the GROUP BY terms
      ** from the previous row currently stored in a0, a1, a2...
      */
      addrTopOfLoop = sqlite3VdbeCurrentAddr(v);
      sqlite3ExprCacheClear(pParse);
      for(j=0; j<pGroupBy->nExpr; j++){
        if( groupBySort ){
          sqlite3VdbeAddOp3(v, OP_Column, sAggInfo.sortingIdx, j, iBMem+j);
        }else{
          sAggInfo.directMode = 1;
          sqlite3ExprCode(pParse, pGroupBy->a[j].pExpr, iBMem+j);
        }
      }
      sqlite3VdbeAddOp4(v, OP_Compare, iAMem, iBMem, pGroupBy->nExpr,
                          (char*)pKeyInfo, P4_KEYINFO);
      j1 = sqlite3VdbeCurrentAddr(v);
      sqlite3VdbeAddOp3(v, OP_Jump, j1+1, 0, j1+1);

      /* Generate code that runs whenever the GROUP BY changes.
      ** Changes in the GROUP BY are detected by the previous code
      ** block.  If there were no changes, this block is skipped.
      **
      ** This code copies current group by terms in b0,b1,b2,...
      ** over to a0,a1,a2.  It then calls the output subroutine
      ** and resets the aggregate accumulator registers in preparation
      ** for the next GROUP BY batch.
      */
      sqlite3ExprCodeMove(pParse, iBMem, iAMem, pGroupBy->nExpr);
      sqlite3VdbeAddOp2(v, OP_Gosub, regOutputRow, addrOutputRow);
      VdbeComment((v, "output one row"));
      sqlite3VdbeAddOp2(v, OP_IfPos, iAbortFlag, addrEnd);
      VdbeComment((v, "check abort flag"));
      sqlite3VdbeAddOp2(v, OP_Gosub, regReset, addrReset);
      VdbeComment((v, "reset accumulator"));

      /* Update the aggregate accumulators based on the content of
      ** the current row
      */
      sqlite3VdbeJumpHere(v, j1);
      updateAccumulator(pParse, &sAggInfo);
      sqlite3VdbeAddOp2(v, OP_Integer, 1, iUseFlag);
      VdbeComment((v, "indicate data in accumulator"));

      /* End of the loop
      */
      if( groupBySort ){
        sqlite3VdbeAddOp2(v, OP_Next, sAggInfo.sortingIdx, addrTopOfLoop);
      }else{
        sqlite3WhereEnd(pWInfo);
        sqlite3VdbeChangeToNoop(v, addrSortingIdx, 1);
      }

      /* Output the final row of result
      */
      sqlite3VdbeAddOp2(v, OP_Gosub, regOutputRow, addrOutputRow);
      VdbeComment((v, "output final row"));

      /* Jump over the subroutines
      */
      sqlite3VdbeAddOp2(v, OP_Goto, 0, addrEnd);

      /* Generate a subroutine that outputs a single row of the result
      ** set.  This subroutine first looks at the iUseFlag.  If iUseFlag
      ** is less than or equal to zero, the subroutine is a no-op.  If
      ** the processing calls for the query to abort, this subroutine
      ** increments the iAbortFlag memory location before returning in
      ** order to signal the caller to abort.
      */
      addrSetAbort = sqlite3VdbeCurrentAddr(v);
      sqlite3VdbeAddOp2(v, OP_Integer, 1, iAbortFlag);
      VdbeComment((v, "set abort flag"));
      sqlite3VdbeAddOp1(v, OP_Return, regOutputRow);
      sqlite3VdbeResolveLabel(v, addrOutputRow);
      addrOutputRow = sqlite3VdbeCurrentAddr(v);
      sqlite3VdbeAddOp2(v, OP_IfPos, iUseFlag, addrOutputRow+2);
      VdbeComment((v, "Groupby result generator entry point"));
      sqlite3VdbeAddOp1(v, OP_Return, regOutputRow);
      finalizeAggFunctions(pParse, &sAggInfo);
      sqlite3ExprIfFalse(pParse, pHaving, addrOutputRow+1, SQLITE_JUMPIFNULL);
      selectInnerLoop(pParse, p, p->pEList, 0, 0, pOrderBy,
                      distinct, pDest,
                      addrOutputRow+1, addrSetAbort);
      sqlite3VdbeAddOp1(v, OP_Return, regOutputRow);
      VdbeComment((v, "end groupby result generator"));

      /* Generate a subroutine that will reset the group-by accumulator
      */
      sqlite3VdbeResolveLabel(v, addrReset);
      resetAccumulator(pParse, &sAggInfo);
      sqlite3VdbeAddOp1(v, OP_Return, regReset);
     
    } /* endif pGroupBy.  Begin aggregate queries without GROUP BY: */
    else {
      ExprList *pDel = 0;
#ifndef SQLITE_OMIT_BTREECOUNT
      Table *pTab;
      if( (pTab = isSimpleCount(p, &sAggInfo))!=0 ){
        /* If isSimpleCount() returns a pointer to a Table structure, then
        ** the SQL statement is of the form:
        **
        **   SELECT count(*) FROM <tbl>
        **
        ** where the Table structure returned represents table <tbl>.
        **
        ** This statement is so common that it is optimized specially. The
        ** OP_Count instruction is executed either on the intkey table that
        ** contains the data for table <tbl> or on one of its indexes. It
        ** is better to execute the op on an index, as indexes are almost
        ** always spread across less pages than their corresponding tables.
        */
        const int iDb = sqlite3SchemaToIndex(pParse->db, pTab->pSchema);
        const int iCsr = pParse->nTab++;     /* Cursor to scan b-tree */
        Index *pIdx;                         /* Iterator variable */
        KeyInfo *pKeyInfo = 0;               /* Keyinfo for scanned index */
        Index *pBest = 0;                    /* Best index found so far */
        int iRoot = pTab->tnum;              /* Root page of scanned b-tree */

        sqlite3CodeVerifySchema(pParse, iDb);
        sqlite3TableLock(pParse, iDb, pTab->tnum, 0, pTab->zName);

        /* Search for the index that has the least amount of columns. If
        ** there is such an index, and it has less columns than the table
        ** does, then we can assume that it consumes less space on disk and
        ** will therefore be cheaper to scan to determine the query result.
        ** In this case set iRoot to the root page number of the index b-tree
        ** and pKeyInfo to the KeyInfo structure required to navigate the
        ** index.
        **
        ** In practice the KeyInfo structure will not be used. It is only 
        ** passed to keep OP_OpenRead happy.
        */
        for(pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext){
          if( !pBest || pIdx->nColumn<pBest->nColumn ){
            pBest = pIdx;
          }
        }
        if( pBest && pBest->nColumn<pTab->nCol ){
          iRoot = pBest->tnum;
          pKeyInfo = sqlite3IndexKeyinfo(pParse, pBest);
        }

        /* Open a read-only cursor, execute the OP_Count, close the cursor. */
        sqlite3VdbeAddOp3(v, OP_OpenRead, iCsr, iRoot, iDb);
        if( pKeyInfo ){
          sqlite3VdbeChangeP4(v, -1, (char *)pKeyInfo, P4_KEYINFO_HANDOFF);
        }
        sqlite3VdbeAddOp2(v, OP_Count, iCsr, sAggInfo.aFunc[0].iMem);
        sqlite3VdbeAddOp1(v, OP_Close, iCsr);
      }else
#endif /* SQLITE_OMIT_BTREECOUNT */
      {
        /* Check if the query is of one of the following forms:
        **
        **   SELECT min(x) FROM ...
        **   SELECT max(x) FROM ...
        **
        ** If it is, then ask the code in where.c to attempt to sort results
        ** as if there was an "ORDER ON x" or "ORDER ON x DESC" clause. 
        ** If where.c is able to produce results sorted in this order, then
        ** add vdbe code to break out of the processing loop after the 
        ** first iteration (since the first iteration of the loop is 
        ** guaranteed to operate on the row with the minimum or maximum 
        ** value of x, the only row required).
        **
        ** A special flag must be passed to sqlite3WhereBegin() to slightly
        ** modify behaviour as follows:
        **
        **   + If the query is a "SELECT min(x)", then the loop coded by
        **     where.c should not iterate over any values with a NULL value
        **     for x.
        **
        **   + The optimizer code in where.c (the thing that decides which
        **     index or indices to use) should place a different priority on 
        **     satisfying the 'ORDER BY' clause than it does in other cases.
        **     Refer to code and comments in where.c for details.
        */
        ExprList *pMinMax = 0;
        u8 flag = minMaxQuery(p);
        if( flag ){
          assert( !ExprHasProperty(p->pEList->a[0].pExpr, EP_xIsSelect) );
          pMinMax = sqlite3ExprListDup(db, p->pEList->a[0].pExpr->x.pList,0);
          pDel = pMinMax;
          if( pMinMax && !db->mallocFailed ){
            pMinMax->a[0].sortOrder = flag!=WHERE_ORDERBY_MIN ?1:0;
            pMinMax->a[0].pExpr->op = TK_COLUMN;
          }
        }
  
        /* This case runs if the aggregate has no GROUP BY clause.  The
        ** processing is much simpler since there is only a single row
        ** of output.
        */
        resetAccumulator(pParse, &sAggInfo);
        pWInfo = sqlite3WhereBegin(pParse, pTabList, pWhere, &pMinMax, flag);
        if( pWInfo==0 ){
          sqlite3ExprListDelete(db, pDel);
          goto select_end;
        }
        updateAccumulator(pParse, &sAggInfo);
        if( !pMinMax && flag ){
          sqlite3VdbeAddOp2(v, OP_Goto, 0, pWInfo->iBreak);
          VdbeComment((v, "%s() by index",
                (flag==WHERE_ORDERBY_MIN?"min":"max")));
        }
        sqlite3WhereEnd(pWInfo);
        finalizeAggFunctions(pParse, &sAggInfo);
      }

      pOrderBy = 0;
      sqlite3ExprIfFalse(pParse, pHaving, addrEnd, SQLITE_JUMPIFNULL);
      selectInnerLoop(pParse, p, p->pEList, 0, 0, 0, -1, 
                      pDest, addrEnd, addrEnd);
      sqlite3ExprListDelete(db, pDel);
    }
    sqlite3VdbeResolveLabel(v, addrEnd);
    
  } /* endif aggregate query */

  /* If there is an ORDER BY clause, then we need to sort the results
  ** and send them to the callback one by one.
  */
  if( pOrderBy ){
    generateSortTail(pParse, p, v, pEList->nExpr, pDest);
  }

  /* Jump here to skip this query
  */
  sqlite3VdbeResolveLabel(v, iEnd);

  /* The SELECT was successfully coded.   Set the return code to 0
  ** to indicate no errors.
  */
  rc = 0;

  /* Control jumps to here if an error is encountered above, or upon
  ** successful coding of the SELECT.
  */
select_end:

  /* Identify column names if results of the SELECT are to be output.
  */
  if( rc==SQLITE_OK && pDest->eDest==SRT_Output ){
    generateColumnNames(pParse, pTabList, pEList);
  }

  sqlite3DbFree(db, sAggInfo.aCol);
  sqlite3DbFree(db, sAggInfo.aFunc);
  return rc;
}

#if defined(SQLITE_DEBUG)
/*
*******************************************************************************
** The following code is used for testing and debugging only.  The code
** that follows does not appear in normal builds.
**
** These routines are used to print out the content of all or part of a 
** parse structures such as Select or Expr.  Such printouts are useful
** for helping to understand what is happening inside the code generator
** during the execution of complex SELECT statements.
**
** These routine are not called anywhere from within the normal
** code base.  Then are intended to be called from within the debugger
** or from temporary "printf" statements inserted for debugging.
*/
void sqlite3PrintExpr(Expr *p){
  if( !ExprHasProperty(p, EP_IntValue) && p->u.zToken ){
    sqlite3DebugPrintf("(%s", p->u.zToken);
  }else{
    sqlite3DebugPrintf("(%d", p->op);
  }
  if( p->pLeft ){
    sqlite3DebugPrintf(" ");
    sqlite3PrintExpr(p->pLeft);
  }
  if( p->pRight ){
    sqlite3DebugPrintf(" ");
    sqlite3PrintExpr(p->pRight);
  }
  sqlite3DebugPrintf(")");
}
void sqlite3PrintExprList(ExprList *pList){
  int i;
  for(i=0; i<pList->nExpr; i++){
    sqlite3PrintExpr(pList->a[i].pExpr);
    if( i<pList->nExpr-1 ){
      sqlite3DebugPrintf(", ");
    }
  }
}
void sqlite3PrintSelect(Select *p, int indent){
  sqlite3DebugPrintf("%*sSELECT(%p) ", indent, "", p);
  sqlite3PrintExprList(p->pEList);
  sqlite3DebugPrintf("\n");
  if( p->pSrc ){
    char *zPrefix;
    int i;
    zPrefix = "FROM";
    for(i=0; i<p->pSrc->nSrc; i++){
      struct SrcList_item *pItem = &p->pSrc->a[i];
      sqlite3DebugPrintf("%*s ", indent+6, zPrefix);
      zPrefix = "";
      if( pItem->pSelect ){
        sqlite3DebugPrintf("(\n");
        sqlite3PrintSelect(pItem->pSelect, indent+10);
        sqlite3DebugPrintf("%*s)", indent+8, "");
      }else if( pItem->zName ){
        sqlite3DebugPrintf("%s", pItem->zName);
      }
      if( pItem->pTab ){
        sqlite3DebugPrintf("(table: %s)", pItem->pTab->zName);
      }
      if( pItem->zAlias ){
        sqlite3DebugPrintf(" AS %s", pItem->zAlias);
      }
      if( i<p->pSrc->nSrc-1 ){
        sqlite3DebugPrintf(",");
      }
      sqlite3DebugPrintf("\n");
    }
  }
  if( p->pWhere ){
    sqlite3DebugPrintf("%*s WHERE ", indent, "");
    sqlite3PrintExpr(p->pWhere);
    sqlite3DebugPrintf("\n");
  }
  if( p->pGroupBy ){
    sqlite3DebugPrintf("%*s GROUP BY ", indent, "");
    sqlite3PrintExprList(p->pGroupBy);
    sqlite3DebugPrintf("\n");
  }
  if( p->pHaving ){
    sqlite3DebugPrintf("%*s HAVING ", indent, "");
    sqlite3PrintExpr(p->pHaving);
    sqlite3DebugPrintf("\n");
  }
  if( p->pOrderBy ){
    sqlite3DebugPrintf("%*s ORDER BY ", indent, "");
    sqlite3PrintExprList(p->pOrderBy);
    sqlite3DebugPrintf("\n");
  }
}
/* End of the structure debug printing code
*****************************************************************************/
#endif /* defined(SQLITE_TEST) || defined(SQLITE_DEBUG) */
