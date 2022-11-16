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
*/
#include "sqliteInt.h"

/*
** An instance of the following object is used to record information about
** how to process the DISTINCT keyword, to simplify passing that information
** into the selectInnerLoop() routine.
*/
typedef struct DistinctCtx DistinctCtx;
struct DistinctCtx {
  u8 isTnct;      /* 0: Not distinct. 1: DISTICT  2: DISTINCT and ORDER BY */
  u8 eTnctType;   /* One of the WHERE_DISTINCT_* operators */
  int tabTnct;    /* Ephemeral table used for DISTINCT processing */
  int addrTnct;   /* Address of OP_OpenEphemeral opcode for tabTnct */
};

/*
** An instance of the following object is used to record information about
** the ORDER BY (or GROUP BY) clause of query is being coded.
**
** The aDefer[] array is used by the sorter-references optimization. For
** example, assuming there is no index that can be used for the ORDER BY,
** for the query:
**
**     SELECT a, bigblob FROM t1 ORDER BY a LIMIT 10;
**
** it may be more efficient to add just the "a" values to the sorter, and
** retrieve the associated "bigblob" values directly from table t1 as the
** 10 smallest "a" values are extracted from the sorter.
**
** When the sorter-reference optimization is used, there is one entry in the
** aDefer[] array for each database table that may be read as values are
** extracted from the sorter.
*/
typedef struct SortCtx SortCtx;
struct SortCtx {
  ExprList *pOrderBy;   /* The ORDER BY (or GROUP BY clause) */
  int nOBSat;           /* Number of ORDER BY terms satisfied by indices */
  int iECursor;         /* Cursor number for the sorter */
  int regReturn;        /* Register holding block-output return address */
  int labelBkOut;       /* Start label for the block-output subroutine */
  int addrSortIndex;    /* Address of the OP_SorterOpen or OP_OpenEphemeral */
  int labelDone;        /* Jump here when done, ex: LIMIT reached */
  int labelOBLopt;      /* Jump here when sorter is full */
  u8 sortFlags;         /* Zero or more SORTFLAG_* bits */
#ifdef SQLITE_ENABLE_SORTER_REFERENCES
  u8 nDefer;            /* Number of valid entries in aDefer[] */
  struct DeferredCsr {
    Table *pTab;        /* Table definition */
    int iCsr;           /* Cursor number for table */
    int nKey;           /* Number of PK columns for table pTab (>=1) */
  } aDefer[4];
#endif
  struct RowLoadInfo *pDeferredRowLoad;  /* Deferred row loading info or NULL */
};
#define SORTFLAG_UseSorter  0x01   /* Use SorterOpen instead of OpenEphemeral */

/*
** Delete all the content of a Select structure.  Deallocate the structure
** itself depending on the value of bFree
**
** If bFree==1, call sqlite3DbFree() on the p object.
** If bFree==0, Leave the first Select object unfreed
*/
static void clearSelect(sqlite3 *db, Select *p, int bFree){
  assert( db!=0 );
  while( p ){
    Select *pPrior = p->pPrior;
    sqlite3ExprListDelete(db, p->pEList);
    sqlite3SrcListDelete(db, p->pSrc);
    sqlite3ExprDelete(db, p->pWhere);
    sqlite3ExprListDelete(db, p->pGroupBy);
    sqlite3ExprDelete(db, p->pHaving);
    sqlite3ExprListDelete(db, p->pOrderBy);
    sqlite3ExprDelete(db, p->pLimit);
    if( OK_IF_ALWAYS_TRUE(p->pWith) ) sqlite3WithDelete(db, p->pWith);
#ifndef SQLITE_OMIT_WINDOWFUNC
    if( OK_IF_ALWAYS_TRUE(p->pWinDefn) ){
      sqlite3WindowListDelete(db, p->pWinDefn);
    }
    while( p->pWin ){
      assert( p->pWin->ppThis==&p->pWin );
      sqlite3WindowUnlinkFromSelect(p->pWin);
    }
#endif
    if( bFree ) sqlite3DbNNFreeNN(db, p);
    p = pPrior;
    bFree = 1;
  }
}

/*
** Initialize a SelectDest structure.
*/
void sqlite3SelectDestInit(SelectDest *pDest, int eDest, int iParm){
  pDest->eDest = (u8)eDest;
  pDest->iSDParm = iParm;
  pDest->iSDParm2 = 0;
  pDest->zAffSdst = 0;
  pDest->iSdst = 0;
  pDest->nSdst = 0;
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
  u32 selFlags,         /* Flag parameters, such as SF_Distinct */
  Expr *pLimit          /* LIMIT value.  NULL means not used */
){
  Select *pNew, *pAllocated;
  Select standin;
  pAllocated = pNew = sqlite3DbMallocRawNN(pParse->db, sizeof(*pNew) );
  if( pNew==0 ){
    assert( pParse->db->mallocFailed );
    pNew = &standin;
  }
  if( pEList==0 ){
    pEList = sqlite3ExprListAppend(pParse, 0,
                                   sqlite3Expr(pParse->db,TK_ASTERISK,0));
  }
  pNew->pEList = pEList;
  pNew->op = TK_SELECT;
  pNew->selFlags = selFlags;
  pNew->iLimit = 0;
  pNew->iOffset = 0;
  pNew->selId = ++pParse->nSelect;
  pNew->addrOpenEphm[0] = -1;
  pNew->addrOpenEphm[1] = -1;
  pNew->nSelectRow = 0;
  if( pSrc==0 ) pSrc = sqlite3DbMallocZero(pParse->db, sizeof(*pSrc));
  pNew->pSrc = pSrc;
  pNew->pWhere = pWhere;
  pNew->pGroupBy = pGroupBy;
  pNew->pHaving = pHaving;
  pNew->pOrderBy = pOrderBy;
  pNew->pPrior = 0;
  pNew->pNext = 0;
  pNew->pLimit = pLimit;
  pNew->pWith = 0;
#ifndef SQLITE_OMIT_WINDOWFUNC
  pNew->pWin = 0;
  pNew->pWinDefn = 0;
#endif
  if( pParse->db->mallocFailed ) {
    clearSelect(pParse->db, pNew, pNew!=&standin);
    pAllocated = 0;
  }else{
    assert( pNew->pSrc!=0 || pParse->nErr>0 );
  }
  return pAllocated;
}


/*
** Delete the given Select structure and all of its substructures.
*/
void sqlite3SelectDelete(sqlite3 *db, Select *p){
  if( OK_IF_ALWAYS_TRUE(p) ) clearSelect(db, p, 1);
}

/*
** Return a pointer to the right-most SELECT statement in a compound.
*/
static Select *findRightmost(Select *p){
  while( p->pNext ) p = p->pNext;
  return p;
}

/*
** Given 1 to 3 identifiers preceding the JOIN keyword, determine the
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
**
** These are the valid join types:
**
**
**      pA       pB       pC               Return Value
**     -------  -----    -----             ------------
**     CROSS      -        -                 JT_CROSS
**     INNER      -        -                 JT_INNER
**     LEFT       -        -                 JT_LEFT|JT_OUTER
**     LEFT     OUTER      -                 JT_LEFT|JT_OUTER
**     RIGHT      -        -                 JT_RIGHT|JT_OUTER
**     RIGHT    OUTER      -                 JT_RIGHT|JT_OUTER
**     FULL       -        -                 JT_LEFT|JT_RIGHT|JT_OUTER
**     FULL     OUTER      -                 JT_LEFT|JT_RIGHT|JT_OUTER
**     NATURAL  INNER      -                 JT_NATURAL|JT_INNER
**     NATURAL  LEFT       -                 JT_NATURAL|JT_LEFT|JT_OUTER
**     NATURAL  LEFT     OUTER               JT_NATURAL|JT_LEFT|JT_OUTER
**     NATURAL  RIGHT      -                 JT_NATURAL|JT_RIGHT|JT_OUTER
**     NATURAL  RIGHT    OUTER               JT_NATURAL|JT_RIGHT|JT_OUTER
**     NATURAL  FULL       -                 JT_NATURAL|JT_LEFT|JT_RIGHT
**     NATURAL  FULL     OUTER               JT_NATRUAL|JT_LEFT|JT_RIGHT
**
** To preserve historical compatibly, SQLite also accepts a variety 
** of other non-standard and in many cases non-sensical join types.
** This routine makes as much sense at it can from the nonsense join
** type and returns a result.  Examples of accepted nonsense join types
** include but are not limited to:
**
**          INNER CROSS JOIN        ->   same as JOIN
**          NATURAL CROSS JOIN      ->   same as NATURAL JOIN
**          OUTER LEFT JOIN         ->   same as LEFT JOIN
**          LEFT NATURAL JOIN       ->   same as NATURAL LEFT JOIN
**          LEFT RIGHT JOIN         ->   same as FULL JOIN
**          RIGHT OUTER FULL JOIN   ->   same as FULL JOIN
**          CROSS CROSS CROSS JOIN  ->   same as JOIN
**
** The only restrictions on the join type name are:
**
**    *   "INNER" cannot appear together with "OUTER", "LEFT", "RIGHT",
**        or "FULL".
**
**    *   "CROSS" cannot appear together with "OUTER", "LEFT", "RIGHT,
**        or "FULL".
**
**    *   If "OUTER" is present then there must also be one of
**        "LEFT", "RIGHT", or "FULL"
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
    /* (0) natural */ { 0,  7, JT_NATURAL                },
    /* (1) left    */ { 6,  4, JT_LEFT|JT_OUTER          },
    /* (2) outer   */ { 10, 5, JT_OUTER                  },
    /* (3) right   */ { 14, 5, JT_RIGHT|JT_OUTER         },
    /* (4) full    */ { 19, 4, JT_LEFT|JT_RIGHT|JT_OUTER },
    /* (5) inner   */ { 23, 5, JT_INNER                  },
    /* (6) cross   */ { 28, 5, JT_INNER|JT_CROSS         },
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
     (jointype & JT_ERROR)!=0 ||
     (jointype & (JT_OUTER|JT_LEFT|JT_RIGHT))==JT_OUTER
  ){
    const char *zSp1 = " ";
    const char *zSp2 = " ";
    if( pB==0 ){ zSp1++; }
    if( pC==0 ){ zSp2++; }
    sqlite3ErrorMsg(pParse, "unknown join type: "
       "%T%s%T%s%T", pA, zSp1, pB, zSp2, pC);
    jointype = JT_INNER;
  }
  return jointype;
}

/*
** Return the index of a column in a table.  Return -1 if the column
** is not contained in the table.
*/
int sqlite3ColumnIndex(Table *pTab, const char *zCol){
  int i;
  u8 h = sqlite3StrIHash(zCol);
  Column *pCol;
  for(pCol=pTab->aCol, i=0; i<pTab->nCol; pCol++, i++){
    if( pCol->hName==h && sqlite3StrICmp(pCol->zCnName, zCol)==0 ) return i;
  }
  return -1;
}

/*
** Mark a subquery result column as having been used.
*/
void sqlite3SrcItemColumnUsed(SrcItem *pItem, int iCol){
  assert( pItem!=0 );
  assert( (int)pItem->fg.isNestedFrom == IsNestedFrom(pItem->pSelect) );
  if( pItem->fg.isNestedFrom ){
    ExprList *pResults;
    assert( pItem->pSelect!=0 );
    pResults = pItem->pSelect->pEList;
    assert( pResults!=0 );
    assert( iCol>=0 && iCol<pResults->nExpr );
    pResults->a[iCol].fg.bUsed = 1;
  }
}

/*
** Search the tables iStart..iEnd (inclusive) in pSrc, looking for a
** table that has a column named zCol.  The search is left-to-right.
** The first match found is returned.
**
** When found, set *piTab and *piCol to the table index and column index
** of the matching column and return TRUE.
**
** If not found, return FALSE.
*/
static int tableAndColumnIndex(
  SrcList *pSrc,       /* Array of tables to search */
  int iStart,          /* First member of pSrc->a[] to check */
  int iEnd,            /* Last member of pSrc->a[] to check */
  const char *zCol,    /* Name of the column we are looking for */
  int *piTab,          /* Write index of pSrc->a[] here */
  int *piCol,          /* Write index of pSrc->a[*piTab].pTab->aCol[] here */
  int bIgnoreHidden    /* Ignore hidden columns */
){
  int i;               /* For looping over tables in pSrc */
  int iCol;            /* Index of column matching zCol */

  assert( iEnd<pSrc->nSrc );
  assert( iStart>=0 );
  assert( (piTab==0)==(piCol==0) );  /* Both or neither are NULL */

  for(i=iStart; i<=iEnd; i++){
    iCol = sqlite3ColumnIndex(pSrc->a[i].pTab, zCol);
    if( iCol>=0 
     && (bIgnoreHidden==0 || IsHiddenColumn(&pSrc->a[i].pTab->aCol[iCol])==0)
    ){
      if( piTab ){
        sqlite3SrcItemColumnUsed(&pSrc->a[i], iCol);
        *piTab = i;
        *piCol = iCol;
      }
      return 1;
    }
  }
  return 0;
}

/*
** Set the EP_OuterON property on all terms of the given expression.
** And set the Expr.w.iJoin to iTable for every term in the
** expression.
**
** The EP_OuterON property is used on terms of an expression to tell
** the OUTER JOIN processing logic that this term is part of the
** join restriction specified in the ON or USING clause and not a part
** of the more general WHERE clause.  These terms are moved over to the
** WHERE clause during join processing but we need to remember that they
** originated in the ON or USING clause.
**
** The Expr.w.iJoin tells the WHERE clause processing that the
** expression depends on table w.iJoin even if that table is not
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
void sqlite3SetJoinExpr(Expr *p, int iTable, u32 joinFlag){
  assert( joinFlag==EP_OuterON || joinFlag==EP_InnerON );
  while( p ){
    ExprSetProperty(p, joinFlag);
    assert( !ExprHasProperty(p, EP_TokenOnly|EP_Reduced) );
    ExprSetVVAProperty(p, EP_NoReduce);
    p->w.iJoin = iTable;
    if( p->op==TK_FUNCTION ){
      assert( ExprUseXList(p) );
      if( p->x.pList ){
        int i;
        for(i=0; i<p->x.pList->nExpr; i++){
          sqlite3SetJoinExpr(p->x.pList->a[i].pExpr, iTable, joinFlag);
        }
      }
    }
    sqlite3SetJoinExpr(p->pLeft, iTable, joinFlag);
    p = p->pRight;
  } 
}

/* Undo the work of sqlite3SetJoinExpr().  This is used when a LEFT JOIN
** is simplified into an ordinary JOIN, and when an ON expression is
** "pushed down" into the WHERE clause of a subquery.
**
** Convert every term that is marked with EP_OuterON and w.iJoin==iTable into
** an ordinary term that omits the EP_OuterON mark.  Or if iTable<0, then
** just clear every EP_OuterON and EP_InnerON mark from the expression tree.
**
** If nullable is true, that means that Expr p might evaluate to NULL even
** if it is a reference to a NOT NULL column.  This can happen, for example,
** if the table that p references is on the left side of a RIGHT JOIN.
** If nullable is true, then take care to not remove the EP_CanBeNull bit.
** See forum thread https://sqlite.org/forum/forumpost/b40696f50145d21c
*/
static void unsetJoinExpr(Expr *p, int iTable, int nullable){
  while( p ){
    if( iTable<0 || (ExprHasProperty(p, EP_OuterON) && p->w.iJoin==iTable) ){
      ExprClearProperty(p, EP_OuterON|EP_InnerON);
      if( iTable>=0 ) ExprSetProperty(p, EP_InnerON);
    }
    if( p->op==TK_COLUMN && p->iTable==iTable && !nullable ){
      ExprClearProperty(p, EP_CanBeNull);
    }
    if( p->op==TK_FUNCTION ){
      assert( ExprUseXList(p) );
      if( p->x.pList ){
        int i;
        for(i=0; i<p->x.pList->nExpr; i++){
          unsetJoinExpr(p->x.pList->a[i].pExpr, iTable, nullable);
        }
      }
    }
    unsetJoinExpr(p->pLeft, iTable, nullable);
    p = p->pRight;
  } 
}

/*
** This routine processes the join information for a SELECT statement.
**
**   *  A NATURAL join is converted into a USING join.  After that, we
**      do not need to be concerned with NATURAL joins and we only have
**      think about USING joins.
**
**   *  ON and USING clauses result in extra terms being added to the
**      WHERE clause to enforce the specified constraints.  The extra
**      WHERE clause terms will be tagged with EP_OuterON or
**      EP_InnerON so that we know that they originated in ON/USING.
**
** The terms of a FROM clause are contained in the Select.pSrc structure.
** The left most table is the first entry in Select.pSrc.  The right-most
** table is the last entry.  The join operator is held in the entry to
** the right.  Thus entry 1 contains the join operator for the join between
** entries 0 and 1.  Any ON or USING clauses associated with the join are
** also attached to the right entry.
**
** This routine returns the number of errors encountered.
*/
static int sqlite3ProcessJoin(Parse *pParse, Select *p){
  SrcList *pSrc;                  /* All tables in the FROM clause */
  int i, j;                       /* Loop counters */
  SrcItem *pLeft;                 /* Left table being joined */
  SrcItem *pRight;                /* Right table being joined */

  pSrc = p->pSrc;
  pLeft = &pSrc->a[0];
  pRight = &pLeft[1];
  for(i=0; i<pSrc->nSrc-1; i++, pRight++, pLeft++){
    Table *pRightTab = pRight->pTab;
    u32 joinType;

    if( NEVER(pLeft->pTab==0 || pRightTab==0) ) continue;
    joinType = (pRight->fg.jointype & JT_OUTER)!=0 ? EP_OuterON : EP_InnerON;

    /* If this is a NATURAL join, synthesize an approprate USING clause
    ** to specify which columns should be joined.
    */
    if( pRight->fg.jointype & JT_NATURAL ){
      IdList *pUsing = 0;
      if( pRight->fg.isUsing || pRight->u3.pOn ){
        sqlite3ErrorMsg(pParse, "a NATURAL join may not have "
           "an ON or USING clause", 0);
        return 1;
      }
      for(j=0; j<pRightTab->nCol; j++){
        char *zName;   /* Name of column in the right table */

        if( IsHiddenColumn(&pRightTab->aCol[j]) ) continue;
        zName = pRightTab->aCol[j].zCnName;
        if( tableAndColumnIndex(pSrc, 0, i, zName, 0, 0, 1) ){
          pUsing = sqlite3IdListAppend(pParse, pUsing, 0);
          if( pUsing ){
            assert( pUsing->nId>0 );
            assert( pUsing->a[pUsing->nId-1].zName==0 );
            pUsing->a[pUsing->nId-1].zName = sqlite3DbStrDup(pParse->db, zName);
          }
        }
      }
      if( pUsing ){
        pRight->fg.isUsing = 1;
        pRight->fg.isSynthUsing = 1;
        pRight->u3.pUsing = pUsing;
      }
      if( pParse->nErr ) return 1;
    }

    /* Create extra terms on the WHERE clause for each column named
    ** in the USING clause.  Example: If the two tables to be joined are 
    ** A and B and the USING clause names X, Y, and Z, then add this
    ** to the WHERE clause:    A.X=B.X AND A.Y=B.Y AND A.Z=B.Z
    ** Report an error if any column mentioned in the USING clause is
    ** not contained in both tables to be joined.
    */
    if( pRight->fg.isUsing ){
      IdList *pList = pRight->u3.pUsing;
      sqlite3 *db = pParse->db;
      assert( pList!=0 );
      for(j=0; j<pList->nId; j++){
        char *zName;     /* Name of the term in the USING clause */
        int iLeft;       /* Table on the left with matching column name */
        int iLeftCol;    /* Column number of matching column on the left */
        int iRightCol;   /* Column number of matching column on the right */
        Expr *pE1;       /* Reference to the column on the LEFT of the join */
        Expr *pE2;       /* Reference to the column on the RIGHT of the join */
        Expr *pEq;       /* Equality constraint.  pE1 == pE2 */

        zName = pList->a[j].zName;
        iRightCol = sqlite3ColumnIndex(pRightTab, zName);
        if( iRightCol<0
         || tableAndColumnIndex(pSrc, 0, i, zName, &iLeft, &iLeftCol,
                                pRight->fg.isSynthUsing)==0
        ){
          sqlite3ErrorMsg(pParse, "cannot join using column %s - column "
            "not present in both tables", zName);
          return 1;
        }
        pE1 = sqlite3CreateColumnExpr(db, pSrc, iLeft, iLeftCol);
        sqlite3SrcItemColumnUsed(&pSrc->a[iLeft], iLeftCol);
        if( (pSrc->a[0].fg.jointype & JT_LTORJ)!=0 ){
          /* This branch runs if the query contains one or more RIGHT or FULL
          ** JOINs.  If only a single table on the left side of this join
          ** contains the zName column, then this branch is a no-op.
          ** But if there are two or more tables on the left side
          ** of the join, construct a coalesce() function that gathers all
          ** such tables.  Raise an error if more than one of those references
          ** to zName is not also within a prior USING clause.
          **
          ** We really ought to raise an error if there are two or more
          ** non-USING references to zName on the left of an INNER or LEFT
          ** JOIN.  But older versions of SQLite do not do that, so we avoid
          ** adding a new error so as to not break legacy applications.
          */
          ExprList *pFuncArgs = 0;   /* Arguments to the coalesce() */
          static const Token tkCoalesce = { "coalesce", 8 };
          while( tableAndColumnIndex(pSrc, iLeft+1, i, zName, &iLeft, &iLeftCol,
                                     pRight->fg.isSynthUsing)!=0 ){
            if( pSrc->a[iLeft].fg.isUsing==0
             || sqlite3IdListIndex(pSrc->a[iLeft].u3.pUsing, zName)<0
            ){
              sqlite3ErrorMsg(pParse, "ambiguous reference to %s in USING()",
                              zName);
              break;
            }
            pFuncArgs = sqlite3ExprListAppend(pParse, pFuncArgs, pE1);
            pE1 = sqlite3CreateColumnExpr(db, pSrc, iLeft, iLeftCol);
            sqlite3SrcItemColumnUsed(&pSrc->a[iLeft], iLeftCol);
          }
          if( pFuncArgs ){
            pFuncArgs = sqlite3ExprListAppend(pParse, pFuncArgs, pE1);
            pE1 = sqlite3ExprFunction(pParse, pFuncArgs, &tkCoalesce, 0);
          }
        }
        pE2 = sqlite3CreateColumnExpr(db, pSrc, i+1, iRightCol);
        sqlite3SrcItemColumnUsed(pRight, iRightCol);
        pEq = sqlite3PExpr(pParse, TK_EQ, pE1, pE2);
        assert( pE2!=0 || pEq==0 );
        if( pEq ){
          ExprSetProperty(pEq, joinType);
          assert( !ExprHasProperty(pEq, EP_TokenOnly|EP_Reduced) );
          ExprSetVVAProperty(pEq, EP_NoReduce);
          pEq->w.iJoin = pE2->iTable;
        }
        p->pWhere = sqlite3ExprAnd(pParse, p->pWhere, pEq);
      }
    }

    /* Add the ON clause to the end of the WHERE clause, connected by
    ** an AND operator.
    */
    else if( pRight->u3.pOn ){
      sqlite3SetJoinExpr(pRight->u3.pOn, pRight->iCursor, joinType);
      p->pWhere = sqlite3ExprAnd(pParse, p->pWhere, pRight->u3.pOn);
      pRight->u3.pOn = 0;
      pRight->fg.isOn = 1;
    }
  }
  return 0;
}

/*
** An instance of this object holds information (beyond pParse and pSelect)
** needed to load the next result row that is to be added to the sorter.
*/
typedef struct RowLoadInfo RowLoadInfo;
struct RowLoadInfo {
  int regResult;               /* Store results in array of registers here */
  u8 ecelFlags;                /* Flag argument to ExprCodeExprList() */
#ifdef SQLITE_ENABLE_SORTER_REFERENCES
  ExprList *pExtra;            /* Extra columns needed by sorter refs */
  int regExtraResult;          /* Where to load the extra columns */
#endif
};

/*
** This routine does the work of loading query data into an array of
** registers so that it can be added to the sorter.
*/
static void innerLoopLoadRow(
  Parse *pParse,             /* Statement under construction */
  Select *pSelect,           /* The query being coded */
  RowLoadInfo *pInfo         /* Info needed to complete the row load */
){
  sqlite3ExprCodeExprList(pParse, pSelect->pEList, pInfo->regResult,
                          0, pInfo->ecelFlags);
#ifdef SQLITE_ENABLE_SORTER_REFERENCES
  if( pInfo->pExtra ){
    sqlite3ExprCodeExprList(pParse, pInfo->pExtra, pInfo->regExtraResult, 0, 0);
    sqlite3ExprListDelete(pParse->db, pInfo->pExtra);
  }
#endif
}

/*
** Code the OP_MakeRecord instruction that generates the entry to be
** added into the sorter.
**
** Return the register in which the result is stored.
*/
static int makeSorterRecord(
  Parse *pParse,
  SortCtx *pSort,
  Select *pSelect,
  int regBase,
  int nBase
){
  int nOBSat = pSort->nOBSat;
  Vdbe *v = pParse->pVdbe;
  int regOut = ++pParse->nMem;
  if( pSort->pDeferredRowLoad ){
    innerLoopLoadRow(pParse, pSelect, pSort->pDeferredRowLoad);
  }
  sqlite3VdbeAddOp3(v, OP_MakeRecord, regBase+nOBSat, nBase-nOBSat, regOut);
  return regOut;
}

/*
** Generate code that will push the record in registers regData
** through regData+nData-1 onto the sorter.
*/
static void pushOntoSorter(
  Parse *pParse,         /* Parser context */
  SortCtx *pSort,        /* Information about the ORDER BY clause */
  Select *pSelect,       /* The whole SELECT statement */
  int regData,           /* First register holding data to be sorted */
  int regOrigData,       /* First register holding data before packing */
  int nData,             /* Number of elements in the regData data array */
  int nPrefixReg         /* No. of reg prior to regData available for use */
){
  Vdbe *v = pParse->pVdbe;                         /* Stmt under construction */
  int bSeq = ((pSort->sortFlags & SORTFLAG_UseSorter)==0);
  int nExpr = pSort->pOrderBy->nExpr;              /* No. of ORDER BY terms */
  int nBase = nExpr + bSeq + nData;                /* Fields in sorter record */
  int regBase;                                     /* Regs for sorter record */
  int regRecord = 0;                               /* Assembled sorter record */
  int nOBSat = pSort->nOBSat;                      /* ORDER BY terms to skip */
  int op;                            /* Opcode to add sorter record to sorter */
  int iLimit;                        /* LIMIT counter */
  int iSkip = 0;                     /* End of the sorter insert loop */

  assert( bSeq==0 || bSeq==1 );

  /* Three cases:
  **   (1) The data to be sorted has already been packed into a Record
  **       by a prior OP_MakeRecord.  In this case nData==1 and regData
  **       will be completely unrelated to regOrigData.
  **   (2) All output columns are included in the sort record.  In that
  **       case regData==regOrigData.
  **   (3) Some output columns are omitted from the sort record due to
  **       the SQLITE_ENABLE_SORTER_REFERENCE optimization, or due to the
  **       SQLITE_ECEL_OMITREF optimization, or due to the 
  **       SortCtx.pDeferredRowLoad optimiation.  In any of these cases
  **       regOrigData is 0 to prevent this routine from trying to copy
  **       values that might not yet exist.
  */
  assert( nData==1 || regData==regOrigData || regOrigData==0 );

  if( nPrefixReg ){
    assert( nPrefixReg==nExpr+bSeq );
    regBase = regData - nPrefixReg;
  }else{
    regBase = pParse->nMem + 1;
    pParse->nMem += nBase;
  }
  assert( pSelect->iOffset==0 || pSelect->iLimit!=0 );
  iLimit = pSelect->iOffset ? pSelect->iOffset+1 : pSelect->iLimit;
  pSort->labelDone = sqlite3VdbeMakeLabel(pParse);
  sqlite3ExprCodeExprList(pParse, pSort->pOrderBy, regBase, regOrigData,
                          SQLITE_ECEL_DUP | (regOrigData? SQLITE_ECEL_REF : 0));
  if( bSeq ){
    sqlite3VdbeAddOp2(v, OP_Sequence, pSort->iECursor, regBase+nExpr);
  }
  if( nPrefixReg==0 && nData>0 ){
    sqlite3ExprCodeMove(pParse, regData, regBase+nExpr+bSeq, nData);
  }
  if( nOBSat>0 ){
    int regPrevKey;   /* The first nOBSat columns of the previous row */
    int addrFirst;    /* Address of the OP_IfNot opcode */
    int addrJmp;      /* Address of the OP_Jump opcode */
    VdbeOp *pOp;      /* Opcode that opens the sorter */
    int nKey;         /* Number of sorting key columns, including OP_Sequence */
    KeyInfo *pKI;     /* Original KeyInfo on the sorter table */

    regRecord = makeSorterRecord(pParse, pSort, pSelect, regBase, nBase);
    regPrevKey = pParse->nMem+1;
    pParse->nMem += pSort->nOBSat;
    nKey = nExpr - pSort->nOBSat + bSeq;
    if( bSeq ){
      addrFirst = sqlite3VdbeAddOp1(v, OP_IfNot, regBase+nExpr); 
    }else{
      addrFirst = sqlite3VdbeAddOp1(v, OP_SequenceTest, pSort->iECursor);
    }
    VdbeCoverage(v);
    sqlite3VdbeAddOp3(v, OP_Compare, regPrevKey, regBase, pSort->nOBSat);
    pOp = sqlite3VdbeGetOp(v, pSort->addrSortIndex);
    if( pParse->db->mallocFailed ) return;
    pOp->p2 = nKey + nData;
    pKI = pOp->p4.pKeyInfo;
    memset(pKI->aSortFlags, 0, pKI->nKeyField); /* Makes OP_Jump testable */
    sqlite3VdbeChangeP4(v, -1, (char*)pKI, P4_KEYINFO);
    testcase( pKI->nAllField > pKI->nKeyField+2 );
    pOp->p4.pKeyInfo = sqlite3KeyInfoFromExprList(pParse,pSort->pOrderBy,nOBSat,
                                           pKI->nAllField-pKI->nKeyField-1);
    pOp = 0; /* Ensure pOp not used after sqltie3VdbeAddOp3() */
    addrJmp = sqlite3VdbeCurrentAddr(v);
    sqlite3VdbeAddOp3(v, OP_Jump, addrJmp+1, 0, addrJmp+1); VdbeCoverage(v);
    pSort->labelBkOut = sqlite3VdbeMakeLabel(pParse);
    pSort->regReturn = ++pParse->nMem;
    sqlite3VdbeAddOp2(v, OP_Gosub, pSort->regReturn, pSort->labelBkOut);
    sqlite3VdbeAddOp1(v, OP_ResetSorter, pSort->iECursor);
    if( iLimit ){
      sqlite3VdbeAddOp2(v, OP_IfNot, iLimit, pSort->labelDone);
      VdbeCoverage(v);
    }
    sqlite3VdbeJumpHere(v, addrFirst);
    sqlite3ExprCodeMove(pParse, regBase, regPrevKey, pSort->nOBSat);
    sqlite3VdbeJumpHere(v, addrJmp);
  }
  if( iLimit ){
    /* At this point the values for the new sorter entry are stored
    ** in an array of registers. They need to be composed into a record
    ** and inserted into the sorter if either (a) there are currently
    ** less than LIMIT+OFFSET items or (b) the new record is smaller than 
    ** the largest record currently in the sorter. If (b) is true and there
    ** are already LIMIT+OFFSET items in the sorter, delete the largest
    ** entry before inserting the new one. This way there are never more 
    ** than LIMIT+OFFSET items in the sorter.
    **
    ** If the new record does not need to be inserted into the sorter,
    ** jump to the next iteration of the loop. If the pSort->labelOBLopt
    ** value is not zero, then it is a label of where to jump.  Otherwise,
    ** just bypass the row insert logic.  See the header comment on the
    ** sqlite3WhereOrderByLimitOptLabel() function for additional info.
    */
    int iCsr = pSort->iECursor;
    sqlite3VdbeAddOp2(v, OP_IfNotZero, iLimit, sqlite3VdbeCurrentAddr(v)+4);
    VdbeCoverage(v);
    sqlite3VdbeAddOp2(v, OP_Last, iCsr, 0);
    iSkip = sqlite3VdbeAddOp4Int(v, OP_IdxLE,
                                 iCsr, 0, regBase+nOBSat, nExpr-nOBSat);
    VdbeCoverage(v);
    sqlite3VdbeAddOp1(v, OP_Delete, iCsr);
  }
  if( regRecord==0 ){
    regRecord = makeSorterRecord(pParse, pSort, pSelect, regBase, nBase);
  }
  if( pSort->sortFlags & SORTFLAG_UseSorter ){
    op = OP_SorterInsert;
  }else{
    op = OP_IdxInsert;
  }
  sqlite3VdbeAddOp4Int(v, op, pSort->iECursor, regRecord,
                       regBase+nOBSat, nBase-nOBSat);
  if( iSkip ){
    sqlite3VdbeChangeP2(v, iSkip,
         pSort->labelOBLopt ? pSort->labelOBLopt : sqlite3VdbeCurrentAddr(v));
  }
}

/*
** Add code to implement the OFFSET
*/
static void codeOffset(
  Vdbe *v,          /* Generate code into this VM */
  int iOffset,      /* Register holding the offset counter */
  int iContinue     /* Jump here to skip the current record */
){
  if( iOffset>0 ){
    sqlite3VdbeAddOp3(v, OP_IfPos, iOffset, iContinue, 1); VdbeCoverage(v);
    VdbeComment((v, "OFFSET"));
  }
}

/*
** Add code that will check to make sure the array of registers starting at
** iMem form a distinct entry. This is used by both "SELECT DISTINCT ..." and
** distinct aggregates ("SELECT count(DISTINCT <expr>) ..."). Three strategies
** are available. Which is used depends on the value of parameter eTnctType,
** as follows:
**
**   WHERE_DISTINCT_UNORDERED/WHERE_DISTINCT_NOOP:
**     Build an ephemeral table that contains all entries seen before and
**     skip entries which have been seen before.
**
**     Parameter iTab is the cursor number of an ephemeral table that must
**     be opened before the VM code generated by this routine is executed.
**     The ephemeral cursor table is queried for a record identical to the
**     record formed by the current array of registers. If one is found,
**     jump to VM address addrRepeat. Otherwise, insert a new record into
**     the ephemeral cursor and proceed.
**
**     The returned value in this case is a copy of parameter iTab.
**
**   WHERE_DISTINCT_ORDERED:
**     In this case rows are being delivered sorted order. The ephermal
**     table is not required. Instead, the current set of values
**     is compared against previous row. If they match, the new row
**     is not distinct and control jumps to VM address addrRepeat. Otherwise,
**     the VM program proceeds with processing the new row.
**
**     The returned value in this case is the register number of the first
**     in an array of registers used to store the previous result row so that
**     it can be compared to the next. The caller must ensure that this
**     register is initialized to NULL.  (The fixDistinctOpenEph() routine
**     will take care of this initialization.)
**
**   WHERE_DISTINCT_UNIQUE:
**     In this case it has already been determined that the rows are distinct.
**     No special action is required. The return value is zero.
**
** Parameter pEList is the list of expressions used to generated the 
** contents of each row. It is used by this routine to determine (a) 
** how many elements there are in the array of registers and (b) the 
** collation sequences that should be used for the comparisons if 
** eTnctType is WHERE_DISTINCT_ORDERED.
*/
static int codeDistinct(
  Parse *pParse,     /* Parsing and code generating context */
  int eTnctType,     /* WHERE_DISTINCT_* value */
  int iTab,          /* A sorting index used to test for distinctness */
  int addrRepeat,    /* Jump to here if not distinct */
  ExprList *pEList,  /* Expression for each element */
  int regElem        /* First element */
){
  int iRet = 0;
  int nResultCol = pEList->nExpr;
  Vdbe *v = pParse->pVdbe;

  switch( eTnctType ){
    case WHERE_DISTINCT_ORDERED: {
      int i;
      int iJump;              /* Jump destination */
      int regPrev;            /* Previous row content */

      /* Allocate space for the previous row */
      iRet = regPrev = pParse->nMem+1;
      pParse->nMem += nResultCol;

      iJump = sqlite3VdbeCurrentAddr(v) + nResultCol;
      for(i=0; i<nResultCol; i++){
        CollSeq *pColl = sqlite3ExprCollSeq(pParse, pEList->a[i].pExpr);
        if( i<nResultCol-1 ){
          sqlite3VdbeAddOp3(v, OP_Ne, regElem+i, iJump, regPrev+i);
          VdbeCoverage(v);
        }else{
          sqlite3VdbeAddOp3(v, OP_Eq, regElem+i, addrRepeat, regPrev+i);
          VdbeCoverage(v);
         }
        sqlite3VdbeChangeP4(v, -1, (const char *)pColl, P4_COLLSEQ);
        sqlite3VdbeChangeP5(v, SQLITE_NULLEQ);
      }
      assert( sqlite3VdbeCurrentAddr(v)==iJump || pParse->db->mallocFailed );
      sqlite3VdbeAddOp3(v, OP_Copy, regElem, regPrev, nResultCol-1);
      break;
    }

    case WHERE_DISTINCT_UNIQUE: {
      /* nothing to do */
      break;
    }

    default: {
      int r1 = sqlite3GetTempReg(pParse);
      sqlite3VdbeAddOp4Int(v, OP_Found, iTab, addrRepeat, regElem, nResultCol);
      VdbeCoverage(v);
      sqlite3VdbeAddOp3(v, OP_MakeRecord, regElem, nResultCol, r1);
      sqlite3VdbeAddOp4Int(v, OP_IdxInsert, iTab, r1, regElem, nResultCol);
      sqlite3VdbeChangeP5(v, OPFLAG_USESEEKRESULT);
      sqlite3ReleaseTempReg(pParse, r1);
      iRet = iTab;
      break;
    }
  }

  return iRet;
}

/*
** This routine runs after codeDistinct().  It makes necessary
** adjustments to the OP_OpenEphemeral opcode that the codeDistinct()
** routine made use of.  This processing must be done separately since
** sometimes codeDistinct is called before the OP_OpenEphemeral is actually
** laid down.
**
** WHERE_DISTINCT_NOOP:
** WHERE_DISTINCT_UNORDERED:
**
**     No adjustments necessary.  This function is a no-op.
**
** WHERE_DISTINCT_UNIQUE:
**
**     The ephemeral table is not needed.  So change the
**     OP_OpenEphemeral opcode into an OP_Noop.
**
** WHERE_DISTINCT_ORDERED:
**
**     The ephemeral table is not needed.  But we do need register
**     iVal to be initialized to NULL.  So change the OP_OpenEphemeral
**     into an OP_Null on the iVal register.
*/
static void fixDistinctOpenEph(
  Parse *pParse,     /* Parsing and code generating context */
  int eTnctType,     /* WHERE_DISTINCT_* value */
  int iVal,          /* Value returned by codeDistinct() */
  int iOpenEphAddr   /* Address of OP_OpenEphemeral instruction for iTab */
){
  if( pParse->nErr==0
   && (eTnctType==WHERE_DISTINCT_UNIQUE || eTnctType==WHERE_DISTINCT_ORDERED)
  ){
    Vdbe *v = pParse->pVdbe;
    sqlite3VdbeChangeToNoop(v, iOpenEphAddr);
    if( sqlite3VdbeGetOp(v, iOpenEphAddr+1)->opcode==OP_Explain ){
      sqlite3VdbeChangeToNoop(v, iOpenEphAddr+1);
    }
    if( eTnctType==WHERE_DISTINCT_ORDERED ){
      /* Change the OP_OpenEphemeral to an OP_Null that sets the MEM_Cleared 
      ** bit on the first register of the previous value.  This will cause the
      ** OP_Ne added in codeDistinct() to always fail on the first iteration of
      ** the loop even if the first row is all NULLs.  */
      VdbeOp *pOp = sqlite3VdbeGetOp(v, iOpenEphAddr);
      pOp->opcode = OP_Null;
      pOp->p1 = 1;
      pOp->p2 = iVal;
    }
  }
}

#ifdef SQLITE_ENABLE_SORTER_REFERENCES
/*
** This function is called as part of inner-loop generation for a SELECT
** statement with an ORDER BY that is not optimized by an index. It 
** determines the expressions, if any, that the sorter-reference 
** optimization should be used for. The sorter-reference optimization
** is used for SELECT queries like:
**
**   SELECT a, bigblob FROM t1 ORDER BY a LIMIT 10
**
** If the optimization is used for expression "bigblob", then instead of
** storing values read from that column in the sorter records, the PK of
** the row from table t1 is stored instead. Then, as records are extracted from
** the sorter to return to the user, the required value of bigblob is
** retrieved directly from table t1. If the values are very large, this 
** can be more efficient than storing them directly in the sorter records.
**
** The ExprList_item.fg.bSorterRef flag is set for each expression in pEList 
** for which the sorter-reference optimization should be enabled. 
** Additionally, the pSort->aDefer[] array is populated with entries
** for all cursors required to evaluate all selected expressions. Finally.
** output variable (*ppExtra) is set to an expression list containing
** expressions for all extra PK values that should be stored in the
** sorter records.
*/
static void selectExprDefer(
  Parse *pParse,                  /* Leave any error here */
  SortCtx *pSort,                 /* Sorter context */
  ExprList *pEList,               /* Expressions destined for sorter */
  ExprList **ppExtra              /* Expressions to append to sorter record */
){
  int i;
  int nDefer = 0;
  ExprList *pExtra = 0;
  for(i=0; i<pEList->nExpr; i++){
    struct ExprList_item *pItem = &pEList->a[i];
    if( pItem->u.x.iOrderByCol==0 ){
      Expr *pExpr = pItem->pExpr;
      Table *pTab;
      if( pExpr->op==TK_COLUMN
       && pExpr->iColumn>=0
       && ALWAYS( ExprUseYTab(pExpr) )
       && (pTab = pExpr->y.pTab)!=0
       && IsOrdinaryTable(pTab)
       && (pTab->aCol[pExpr->iColumn].colFlags & COLFLAG_SORTERREF)!=0
      ){
        int j;
        for(j=0; j<nDefer; j++){
          if( pSort->aDefer[j].iCsr==pExpr->iTable ) break;
        }
        if( j==nDefer ){
          if( nDefer==ArraySize(pSort->aDefer) ){
            continue;
          }else{
            int nKey = 1;
            int k;
            Index *pPk = 0;
            if( !HasRowid(pTab) ){
              pPk = sqlite3PrimaryKeyIndex(pTab);
              nKey = pPk->nKeyCol;
            }
            for(k=0; k<nKey; k++){
              Expr *pNew = sqlite3PExpr(pParse, TK_COLUMN, 0, 0);
              if( pNew ){
                pNew->iTable = pExpr->iTable;
                assert( ExprUseYTab(pNew) );
                pNew->y.pTab = pExpr->y.pTab;
                pNew->iColumn = pPk ? pPk->aiColumn[k] : -1;
                pExtra = sqlite3ExprListAppend(pParse, pExtra, pNew);
              }
            }
            pSort->aDefer[nDefer].pTab = pExpr->y.pTab;
            pSort->aDefer[nDefer].iCsr = pExpr->iTable;
            pSort->aDefer[nDefer].nKey = nKey;
            nDefer++;
          }
        }
        pItem->fg.bSorterRef = 1;
      }
    }
  }
  pSort->nDefer = (u8)nDefer;
  *ppExtra = pExtra;
}
#endif

/*
** This routine generates the code for the inside of the inner loop
** of a SELECT.
**
** If srcTab is negative, then the p->pEList expressions
** are evaluated in order to get the data for this row.  If srcTab is
** zero or more, then data is pulled from srcTab and p->pEList is used only 
** to get the number of columns and the collation sequence for each column.
*/
static void selectInnerLoop(
  Parse *pParse,          /* The parser context */
  Select *p,              /* The complete select statement being coded */
  int srcTab,             /* Pull data from this table if non-negative */
  SortCtx *pSort,         /* If not NULL, info on how to process ORDER BY */
  DistinctCtx *pDistinct, /* If not NULL, info on how to process DISTINCT */
  SelectDest *pDest,      /* How to dispose of the results */
  int iContinue,          /* Jump here to continue with next row */
  int iBreak              /* Jump here to break out of the inner loop */
){
  Vdbe *v = pParse->pVdbe;
  int i;
  int hasDistinct;            /* True if the DISTINCT keyword is present */
  int eDest = pDest->eDest;   /* How to dispose of results */
  int iParm = pDest->iSDParm; /* First argument to disposal method */
  int nResultCol;             /* Number of result columns */
  int nPrefixReg = 0;         /* Number of extra registers before regResult */
  RowLoadInfo sRowLoadInfo;   /* Info for deferred row loading */

  /* Usually, regResult is the first cell in an array of memory cells
  ** containing the current result row. In this case regOrig is set to the
  ** same value. However, if the results are being sent to the sorter, the
  ** values for any expressions that are also part of the sort-key are omitted
  ** from this array. In this case regOrig is set to zero.  */
  int regResult;              /* Start of memory holding current results */
  int regOrig;                /* Start of memory holding full result (or 0) */

  assert( v );
  assert( p->pEList!=0 );
  hasDistinct = pDistinct ? pDistinct->eTnctType : WHERE_DISTINCT_NOOP;
  if( pSort && pSort->pOrderBy==0 ) pSort = 0;
  if( pSort==0 && !hasDistinct ){
    assert( iContinue!=0 );
    codeOffset(v, p->iOffset, iContinue);
  }

  /* Pull the requested columns.
  */
  nResultCol = p->pEList->nExpr;

  if( pDest->iSdst==0 ){
    if( pSort ){
      nPrefixReg = pSort->pOrderBy->nExpr;
      if( !(pSort->sortFlags & SORTFLAG_UseSorter) ) nPrefixReg++;
      pParse->nMem += nPrefixReg;
    }
    pDest->iSdst = pParse->nMem+1;
    pParse->nMem += nResultCol;
  }else if( pDest->iSdst+nResultCol > pParse->nMem ){
    /* This is an error condition that can result, for example, when a SELECT
    ** on the right-hand side of an INSERT contains more result columns than
    ** there are columns in the table on the left.  The error will be caught
    ** and reported later.  But we need to make sure enough memory is allocated
    ** to avoid other spurious errors in the meantime. */
    pParse->nMem += nResultCol;
  }
  pDest->nSdst = nResultCol;
  regOrig = regResult = pDest->iSdst;
  if( srcTab>=0 ){
    for(i=0; i<nResultCol; i++){
      sqlite3VdbeAddOp3(v, OP_Column, srcTab, i, regResult+i);
      VdbeComment((v, "%s", p->pEList->a[i].zEName));
    }
  }else if( eDest!=SRT_Exists ){
#ifdef SQLITE_ENABLE_SORTER_REFERENCES
    ExprList *pExtra = 0;
#endif
    /* If the destination is an EXISTS(...) expression, the actual
    ** values returned by the SELECT are not required.
    */
    u8 ecelFlags;    /* "ecel" is an abbreviation of "ExprCodeExprList" */
    ExprList *pEList;
    if( eDest==SRT_Mem || eDest==SRT_Output || eDest==SRT_Coroutine ){
      ecelFlags = SQLITE_ECEL_DUP;
    }else{
      ecelFlags = 0;
    }
    if( pSort && hasDistinct==0 && eDest!=SRT_EphemTab && eDest!=SRT_Table ){
      /* For each expression in p->pEList that is a copy of an expression in
      ** the ORDER BY clause (pSort->pOrderBy), set the associated 
      ** iOrderByCol value to one more than the index of the ORDER BY 
      ** expression within the sort-key that pushOntoSorter() will generate.
      ** This allows the p->pEList field to be omitted from the sorted record,
      ** saving space and CPU cycles.  */
      ecelFlags |= (SQLITE_ECEL_OMITREF|SQLITE_ECEL_REF);

      for(i=pSort->nOBSat; i<pSort->pOrderBy->nExpr; i++){
        int j;
        if( (j = pSort->pOrderBy->a[i].u.x.iOrderByCol)>0 ){
          p->pEList->a[j-1].u.x.iOrderByCol = i+1-pSort->nOBSat;
        }
      }
#ifdef SQLITE_ENABLE_SORTER_REFERENCES
      selectExprDefer(pParse, pSort, p->pEList, &pExtra);
      if( pExtra && pParse->db->mallocFailed==0 ){
        /* If there are any extra PK columns to add to the sorter records,
        ** allocate extra memory cells and adjust the OpenEphemeral 
        ** instruction to account for the larger records. This is only
        ** required if there are one or more WITHOUT ROWID tables with
        ** composite primary keys in the SortCtx.aDefer[] array.  */
        VdbeOp *pOp = sqlite3VdbeGetOp(v, pSort->addrSortIndex);
        pOp->p2 += (pExtra->nExpr - pSort->nDefer);
        pOp->p4.pKeyInfo->nAllField += (pExtra->nExpr - pSort->nDefer);
        pParse->nMem += pExtra->nExpr;
      }
#endif

      /* Adjust nResultCol to account for columns that are omitted
      ** from the sorter by the optimizations in this branch */
      pEList = p->pEList;
      for(i=0; i<pEList->nExpr; i++){
        if( pEList->a[i].u.x.iOrderByCol>0
#ifdef SQLITE_ENABLE_SORTER_REFERENCES
         || pEList->a[i].fg.bSorterRef
#endif
        ){
          nResultCol--;
          regOrig = 0;
        }
      }

      testcase( regOrig );
      testcase( eDest==SRT_Set );
      testcase( eDest==SRT_Mem );
      testcase( eDest==SRT_Coroutine );
      testcase( eDest==SRT_Output );
      assert( eDest==SRT_Set || eDest==SRT_Mem 
           || eDest==SRT_Coroutine || eDest==SRT_Output
           || eDest==SRT_Upfrom );
    }
    sRowLoadInfo.regResult = regResult;
    sRowLoadInfo.ecelFlags = ecelFlags;
#ifdef SQLITE_ENABLE_SORTER_REFERENCES
    sRowLoadInfo.pExtra = pExtra;
    sRowLoadInfo.regExtraResult = regResult + nResultCol;
    if( pExtra ) nResultCol += pExtra->nExpr;
#endif
    if( p->iLimit
     && (ecelFlags & SQLITE_ECEL_OMITREF)!=0 
     && nPrefixReg>0
    ){
      assert( pSort!=0 );
      assert( hasDistinct==0 );
      pSort->pDeferredRowLoad = &sRowLoadInfo;
      regOrig = 0;
    }else{
      innerLoopLoadRow(pParse, p, &sRowLoadInfo);
    }
  }

  /* If the DISTINCT keyword was present on the SELECT statement
  ** and this row has been seen before, then do not make this row
  ** part of the result.
  */
  if( hasDistinct ){
    int eType = pDistinct->eTnctType;
    int iTab = pDistinct->tabTnct;
    assert( nResultCol==p->pEList->nExpr );
    iTab = codeDistinct(pParse, eType, iTab, iContinue, p->pEList, regResult);
    fixDistinctOpenEph(pParse, eType, iTab, pDistinct->addrTnct);
    if( pSort==0 ){
      codeOffset(v, p->iOffset, iContinue);
    }
  }

  switch( eDest ){
    /* In this mode, write each query result to the key of the temporary
    ** table iParm.
    */
#ifndef SQLITE_OMIT_COMPOUND_SELECT
    case SRT_Union: {
      int r1;
      r1 = sqlite3GetTempReg(pParse);
      sqlite3VdbeAddOp3(v, OP_MakeRecord, regResult, nResultCol, r1);
      sqlite3VdbeAddOp4Int(v, OP_IdxInsert, iParm, r1, regResult, nResultCol);
      sqlite3ReleaseTempReg(pParse, r1);
      break;
    }

    /* Construct a record from the query result, but instead of
    ** saving that record, use it as a key to delete elements from
    ** the temporary table iParm.
    */
    case SRT_Except: {
      sqlite3VdbeAddOp3(v, OP_IdxDelete, iParm, regResult, nResultCol);
      break;
    }
#endif /* SQLITE_OMIT_COMPOUND_SELECT */

    /* Store the result as data using a unique key.
    */
    case SRT_Fifo:
    case SRT_DistFifo:
    case SRT_Table:
    case SRT_EphemTab: {
      int r1 = sqlite3GetTempRange(pParse, nPrefixReg+1);
      testcase( eDest==SRT_Table );
      testcase( eDest==SRT_EphemTab );
      testcase( eDest==SRT_Fifo );
      testcase( eDest==SRT_DistFifo );
      sqlite3VdbeAddOp3(v, OP_MakeRecord, regResult, nResultCol, r1+nPrefixReg);
      if( pDest->zAffSdst ){
        sqlite3VdbeChangeP4(v, -1, pDest->zAffSdst, nResultCol);
      }
#ifndef SQLITE_OMIT_CTE
      if( eDest==SRT_DistFifo ){
        /* If the destination is DistFifo, then cursor (iParm+1) is open
        ** on an ephemeral index. If the current row is already present
        ** in the index, do not write it to the output. If not, add the
        ** current row to the index and proceed with writing it to the
        ** output table as well.  */
        int addr = sqlite3VdbeCurrentAddr(v) + 4;
        sqlite3VdbeAddOp4Int(v, OP_Found, iParm+1, addr, r1, 0);
        VdbeCoverage(v);
        sqlite3VdbeAddOp4Int(v, OP_IdxInsert, iParm+1, r1,regResult,nResultCol);
        assert( pSort==0 );
      }
#endif
      if( pSort ){
        assert( regResult==regOrig );
        pushOntoSorter(pParse, pSort, p, r1+nPrefixReg, regOrig, 1, nPrefixReg);
      }else{
        int r2 = sqlite3GetTempReg(pParse);
        sqlite3VdbeAddOp2(v, OP_NewRowid, iParm, r2);
        sqlite3VdbeAddOp3(v, OP_Insert, iParm, r1, r2);
        sqlite3VdbeChangeP5(v, OPFLAG_APPEND);
        sqlite3ReleaseTempReg(pParse, r2);
      }
      sqlite3ReleaseTempRange(pParse, r1, nPrefixReg+1);
      break;
    }

    case SRT_Upfrom: {
      if( pSort ){
        pushOntoSorter(
            pParse, pSort, p, regResult, regOrig, nResultCol, nPrefixReg);
      }else{
        int i2 = pDest->iSDParm2;
        int r1 = sqlite3GetTempReg(pParse);

        /* If the UPDATE FROM join is an aggregate that matches no rows, it
        ** might still be trying to return one row, because that is what
        ** aggregates do.  Don't record that empty row in the output table. */
        sqlite3VdbeAddOp2(v, OP_IsNull, regResult, iBreak); VdbeCoverage(v);

        sqlite3VdbeAddOp3(v, OP_MakeRecord,
                          regResult+(i2<0), nResultCol-(i2<0), r1);
        if( i2<0 ){
          sqlite3VdbeAddOp3(v, OP_Insert, iParm, r1, regResult);
        }else{
          sqlite3VdbeAddOp4Int(v, OP_IdxInsert, iParm, r1, regResult, i2);
        }
      }
      break;
    }

#ifndef SQLITE_OMIT_SUBQUERY
    /* If we are creating a set for an "expr IN (SELECT ...)" construct,
    ** then there should be a single item on the stack.  Write this
    ** item into the set table with bogus data.
    */
    case SRT_Set: {
      if( pSort ){
        /* At first glance you would think we could optimize out the
        ** ORDER BY in this case since the order of entries in the set
        ** does not matter.  But there might be a LIMIT clause, in which
        ** case the order does matter */
        pushOntoSorter(
            pParse, pSort, p, regResult, regOrig, nResultCol, nPrefixReg);
      }else{
        int r1 = sqlite3GetTempReg(pParse);
        assert( sqlite3Strlen30(pDest->zAffSdst)==nResultCol );
        sqlite3VdbeAddOp4(v, OP_MakeRecord, regResult, nResultCol, 
            r1, pDest->zAffSdst, nResultCol);
        sqlite3VdbeAddOp4Int(v, OP_IdxInsert, iParm, r1, regResult, nResultCol);
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
    ** store the results in the appropriate memory cell or array of 
    ** memory cells and break out of the scan loop.
    */
    case SRT_Mem: {
      if( pSort ){
        assert( nResultCol<=pDest->nSdst );
        pushOntoSorter(
            pParse, pSort, p, regResult, regOrig, nResultCol, nPrefixReg);
      }else{
        assert( nResultCol==pDest->nSdst );
        assert( regResult==iParm );
        /* The LIMIT clause will jump out of the loop for us */
      }
      break;
    }
#endif /* #ifndef SQLITE_OMIT_SUBQUERY */

    case SRT_Coroutine:       /* Send data to a co-routine */
    case SRT_Output: {        /* Return the results */
      testcase( eDest==SRT_Coroutine );
      testcase( eDest==SRT_Output );
      if( pSort ){
        pushOntoSorter(pParse, pSort, p, regResult, regOrig, nResultCol,
                       nPrefixReg);
      }else if( eDest==SRT_Coroutine ){
        sqlite3VdbeAddOp1(v, OP_Yield, pDest->iSDParm);
      }else{
        sqlite3VdbeAddOp2(v, OP_ResultRow, regResult, nResultCol);
      }
      break;
    }

#ifndef SQLITE_OMIT_CTE
    /* Write the results into a priority queue that is order according to
    ** pDest->pOrderBy (in pSO).  pDest->iSDParm (in iParm) is the cursor for an
    ** index with pSO->nExpr+2 columns.  Build a key using pSO for the first
    ** pSO->nExpr columns, then make sure all keys are unique by adding a
    ** final OP_Sequence column.  The last column is the record as a blob.
    */
    case SRT_DistQueue:
    case SRT_Queue: {
      int nKey;
      int r1, r2, r3;
      int addrTest = 0;
      ExprList *pSO;
      pSO = pDest->pOrderBy;
      assert( pSO );
      nKey = pSO->nExpr;
      r1 = sqlite3GetTempReg(pParse);
      r2 = sqlite3GetTempRange(pParse, nKey+2);
      r3 = r2+nKey+1;
      if( eDest==SRT_DistQueue ){
        /* If the destination is DistQueue, then cursor (iParm+1) is open
        ** on a second ephemeral index that holds all values every previously
        ** added to the queue. */
        addrTest = sqlite3VdbeAddOp4Int(v, OP_Found, iParm+1, 0, 
                                        regResult, nResultCol);
        VdbeCoverage(v);
      }
      sqlite3VdbeAddOp3(v, OP_MakeRecord, regResult, nResultCol, r3);
      if( eDest==SRT_DistQueue ){
        sqlite3VdbeAddOp2(v, OP_IdxInsert, iParm+1, r3);
        sqlite3VdbeChangeP5(v, OPFLAG_USESEEKRESULT);
      }
      for(i=0; i<nKey; i++){
        sqlite3VdbeAddOp2(v, OP_SCopy,
                          regResult + pSO->a[i].u.x.iOrderByCol - 1,
                          r2+i);
      }
      sqlite3VdbeAddOp2(v, OP_Sequence, iParm, r2+nKey);
      sqlite3VdbeAddOp3(v, OP_MakeRecord, r2, nKey+2, r1);
      sqlite3VdbeAddOp4Int(v, OP_IdxInsert, iParm, r1, r2, nKey+2);
      if( addrTest ) sqlite3VdbeJumpHere(v, addrTest);
      sqlite3ReleaseTempReg(pParse, r1);
      sqlite3ReleaseTempRange(pParse, r2, nKey+2);
      break;
    }
#endif /* SQLITE_OMIT_CTE */



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

  /* Jump to the end of the loop if the LIMIT is reached.  Except, if
  ** there is a sorter, in which case the sorter has already limited
  ** the output for us.
  */
  if( pSort==0 && p->iLimit ){
    sqlite3VdbeAddOp2(v, OP_DecrJumpZero, p->iLimit, iBreak); VdbeCoverage(v);
  }
}

/*
** Allocate a KeyInfo object sufficient for an index of N key columns and
** X extra columns.
*/
KeyInfo *sqlite3KeyInfoAlloc(sqlite3 *db, int N, int X){
  int nExtra = (N+X)*(sizeof(CollSeq*)+1) - sizeof(CollSeq*);
  KeyInfo *p = sqlite3DbMallocRawNN(db, sizeof(KeyInfo) + nExtra);
  if( p ){
    p->aSortFlags = (u8*)&p->aColl[N+X];
    p->nKeyField = (u16)N;
    p->nAllField = (u16)(N+X);
    p->enc = ENC(db);
    p->db = db;
    p->nRef = 1;
    memset(&p[1], 0, nExtra);
  }else{
    return (KeyInfo*)sqlite3OomFault(db);
  }
  return p;
}

/*
** Deallocate a KeyInfo object
*/
void sqlite3KeyInfoUnref(KeyInfo *p){
  if( p ){
    assert( p->db!=0 );
    assert( p->nRef>0 );
    p->nRef--;
    if( p->nRef==0 ) sqlite3DbNNFreeNN(p->db, p);
  }
}

/*
** Make a new pointer to a KeyInfo object
*/
KeyInfo *sqlite3KeyInfoRef(KeyInfo *p){
  if( p ){
    assert( p->nRef>0 );
    p->nRef++;
  }
  return p;
}

#ifdef SQLITE_DEBUG
/*
** Return TRUE if a KeyInfo object can be change.  The KeyInfo object
** can only be changed if this is just a single reference to the object.
**
** This routine is used only inside of assert() statements.
*/
int sqlite3KeyInfoIsWriteable(KeyInfo *p){ return p->nRef==1; }
#endif /* SQLITE_DEBUG */

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
** Space to hold the KeyInfo structure is obtained from malloc.  The calling
** function is responsible for seeing that this structure is eventually
** freed.
*/
KeyInfo *sqlite3KeyInfoFromExprList(
  Parse *pParse,       /* Parsing context */
  ExprList *pList,     /* Form the KeyInfo object from this ExprList */
  int iStart,          /* Begin with this column of pList */
  int nExtra           /* Add this many extra columns to the end */
){
  int nExpr;
  KeyInfo *pInfo;
  struct ExprList_item *pItem;
  sqlite3 *db = pParse->db;
  int i;

  nExpr = pList->nExpr;
  pInfo = sqlite3KeyInfoAlloc(db, nExpr-iStart, nExtra+1);
  if( pInfo ){
    assert( sqlite3KeyInfoIsWriteable(pInfo) );
    for(i=iStart, pItem=pList->a+iStart; i<nExpr; i++, pItem++){
      pInfo->aColl[i-iStart] = sqlite3ExprNNCollSeq(pParse, pItem->pExpr);
      pInfo->aSortFlags[i-iStart] = pItem->fg.sortFlags;
    }
  }
  return pInfo;
}

/*
** Name of the connection operator, used for error messages.
*/
const char *sqlite3SelectOpName(int id){
  char *z;
  switch( id ){
    case TK_ALL:       z = "UNION ALL";   break;
    case TK_INTERSECT: z = "INTERSECT";   break;
    case TK_EXCEPT:    z = "EXCEPT";      break;
    default:           z = "UNION";       break;
  }
  return z;
}

#ifndef SQLITE_OMIT_EXPLAIN
/*
** Unless an "EXPLAIN QUERY PLAN" command is being processed, this function
** is a no-op. Otherwise, it adds a single row of output to the EQP result,
** where the caption is of the form:
**
**   "USE TEMP B-TREE FOR xxx"
**
** where xxx is one of "DISTINCT", "ORDER BY" or "GROUP BY". Exactly which
** is determined by the zUsage argument.
*/
static void explainTempTable(Parse *pParse, const char *zUsage){
  ExplainQueryPlan((pParse, 0, "USE TEMP B-TREE FOR %s", zUsage));
}

/*
** Assign expression b to lvalue a. A second, no-op, version of this macro
** is provided when SQLITE_OMIT_EXPLAIN is defined. This allows the code
** in sqlite3Select() to assign values to structure member variables that
** only exist if SQLITE_OMIT_EXPLAIN is not defined without polluting the
** code with #ifndef directives.
*/
# define explainSetInteger(a, b) a = b

#else
/* No-op versions of the explainXXX() functions and macros. */
# define explainTempTable(y,z)
# define explainSetInteger(y,z)
#endif


/*
** If the inner loop was generated using a non-null pOrderBy argument,
** then the results were placed in a sorter.  After the loop is terminated
** we need to run the sorter and output the results.  The following
** routine generates the code needed to do that.
*/
static void generateSortTail(
  Parse *pParse,    /* Parsing context */
  Select *p,        /* The SELECT statement */
  SortCtx *pSort,   /* Information on the ORDER BY clause */
  int nColumn,      /* Number of columns of data */
  SelectDest *pDest /* Write the sorted results here */
){
  Vdbe *v = pParse->pVdbe;                     /* The prepared statement */
  int addrBreak = pSort->labelDone;            /* Jump here to exit loop */
  int addrContinue = sqlite3VdbeMakeLabel(pParse);/* Jump here for next cycle */
  int addr;                       /* Top of output loop. Jump for Next. */
  int addrOnce = 0;
  int iTab;
  ExprList *pOrderBy = pSort->pOrderBy;
  int eDest = pDest->eDest;
  int iParm = pDest->iSDParm;
  int regRow;
  int regRowid;
  int iCol;
  int nKey;                       /* Number of key columns in sorter record */
  int iSortTab;                   /* Sorter cursor to read from */
  int i;
  int bSeq;                       /* True if sorter record includes seq. no. */
  int nRefKey = 0;
  struct ExprList_item *aOutEx = p->pEList->a;

  assert( addrBreak<0 );
  if( pSort->labelBkOut ){
    sqlite3VdbeAddOp2(v, OP_Gosub, pSort->regReturn, pSort->labelBkOut);
    sqlite3VdbeGoto(v, addrBreak);
    sqlite3VdbeResolveLabel(v, pSort->labelBkOut);
  }

#ifdef SQLITE_ENABLE_SORTER_REFERENCES
  /* Open any cursors needed for sorter-reference expressions */
  for(i=0; i<pSort->nDefer; i++){
    Table *pTab = pSort->aDefer[i].pTab;
    int iDb = sqlite3SchemaToIndex(pParse->db, pTab->pSchema);
    sqlite3OpenTable(pParse, pSort->aDefer[i].iCsr, iDb, pTab, OP_OpenRead);
    nRefKey = MAX(nRefKey, pSort->aDefer[i].nKey);
  }
#endif

  iTab = pSort->iECursor;
  if( eDest==SRT_Output || eDest==SRT_Coroutine || eDest==SRT_Mem ){
    if( eDest==SRT_Mem && p->iOffset ){
      sqlite3VdbeAddOp2(v, OP_Null, 0, pDest->iSdst);
    }
    regRowid = 0;
    regRow = pDest->iSdst;
  }else{
    regRowid = sqlite3GetTempReg(pParse);
    if( eDest==SRT_EphemTab || eDest==SRT_Table ){
      regRow = sqlite3GetTempReg(pParse);
      nColumn = 0;
    }else{
      regRow = sqlite3GetTempRange(pParse, nColumn);
    }
  }
  nKey = pOrderBy->nExpr - pSort->nOBSat;
  if( pSort->sortFlags & SORTFLAG_UseSorter ){
    int regSortOut = ++pParse->nMem;
    iSortTab = pParse->nTab++;
    if( pSort->labelBkOut ){
      addrOnce = sqlite3VdbeAddOp0(v, OP_Once); VdbeCoverage(v);
    }
    sqlite3VdbeAddOp3(v, OP_OpenPseudo, iSortTab, regSortOut, 
        nKey+1+nColumn+nRefKey);
    if( addrOnce ) sqlite3VdbeJumpHere(v, addrOnce);
    addr = 1 + sqlite3VdbeAddOp2(v, OP_SorterSort, iTab, addrBreak);
    VdbeCoverage(v);
    assert( p->iLimit==0 && p->iOffset==0 );
    sqlite3VdbeAddOp3(v, OP_SorterData, iTab, regSortOut, iSortTab);
    bSeq = 0;
  }else{
    addr = 1 + sqlite3VdbeAddOp2(v, OP_Sort, iTab, addrBreak); VdbeCoverage(v);
    codeOffset(v, p->iOffset, addrContinue);
    iSortTab = iTab;
    bSeq = 1;
    if( p->iOffset>0 ){
      sqlite3VdbeAddOp2(v, OP_AddImm, p->iLimit, -1);
    }
  }
  for(i=0, iCol=nKey+bSeq-1; i<nColumn; i++){
#ifdef SQLITE_ENABLE_SORTER_REFERENCES
    if( aOutEx[i].fg.bSorterRef ) continue;
#endif
    if( aOutEx[i].u.x.iOrderByCol==0 ) iCol++;
  }
#ifdef SQLITE_ENABLE_SORTER_REFERENCES
  if( pSort->nDefer ){
    int iKey = iCol+1;
    int regKey = sqlite3GetTempRange(pParse, nRefKey);

    for(i=0; i<pSort->nDefer; i++){
      int iCsr = pSort->aDefer[i].iCsr;
      Table *pTab = pSort->aDefer[i].pTab;
      int nKey = pSort->aDefer[i].nKey;

      sqlite3VdbeAddOp1(v, OP_NullRow, iCsr);
      if( HasRowid(pTab) ){
        sqlite3VdbeAddOp3(v, OP_Column, iSortTab, iKey++, regKey);
        sqlite3VdbeAddOp3(v, OP_SeekRowid, iCsr, 
            sqlite3VdbeCurrentAddr(v)+1, regKey);
      }else{
        int k;
        int iJmp;
        assert( sqlite3PrimaryKeyIndex(pTab)->nKeyCol==nKey );
        for(k=0; k<nKey; k++){
          sqlite3VdbeAddOp3(v, OP_Column, iSortTab, iKey++, regKey+k);
        }
        iJmp = sqlite3VdbeCurrentAddr(v);
        sqlite3VdbeAddOp4Int(v, OP_SeekGE, iCsr, iJmp+2, regKey, nKey);
        sqlite3VdbeAddOp4Int(v, OP_IdxLE, iCsr, iJmp+3, regKey, nKey);
        sqlite3VdbeAddOp1(v, OP_NullRow, iCsr);
      }
    }
    sqlite3ReleaseTempRange(pParse, regKey, nRefKey);
  }
#endif
  for(i=nColumn-1; i>=0; i--){
#ifdef SQLITE_ENABLE_SORTER_REFERENCES
    if( aOutEx[i].fg.bSorterRef ){
      sqlite3ExprCode(pParse, aOutEx[i].pExpr, regRow+i);
    }else
#endif
    {
      int iRead;
      if( aOutEx[i].u.x.iOrderByCol ){
        iRead = aOutEx[i].u.x.iOrderByCol-1;
      }else{
        iRead = iCol--;
      }
      sqlite3VdbeAddOp3(v, OP_Column, iSortTab, iRead, regRow+i);
      VdbeComment((v, "%s", aOutEx[i].zEName));
    }
  }
  switch( eDest ){
    case SRT_Table:
    case SRT_EphemTab: {
      sqlite3VdbeAddOp3(v, OP_Column, iSortTab, nKey+bSeq, regRow);
      sqlite3VdbeAddOp2(v, OP_NewRowid, iParm, regRowid);
      sqlite3VdbeAddOp3(v, OP_Insert, iParm, regRow, regRowid);
      sqlite3VdbeChangeP5(v, OPFLAG_APPEND);
      break;
    }
#ifndef SQLITE_OMIT_SUBQUERY
    case SRT_Set: {
      assert( nColumn==sqlite3Strlen30(pDest->zAffSdst) );
      sqlite3VdbeAddOp4(v, OP_MakeRecord, regRow, nColumn, regRowid,
                        pDest->zAffSdst, nColumn);
      sqlite3VdbeAddOp4Int(v, OP_IdxInsert, iParm, regRowid, regRow, nColumn);
      break;
    }
    case SRT_Mem: {
      /* The LIMIT clause will terminate the loop for us */
      break;
    }
#endif
    case SRT_Upfrom: {
      int i2 = pDest->iSDParm2;
      int r1 = sqlite3GetTempReg(pParse);
      sqlite3VdbeAddOp3(v, OP_MakeRecord,regRow+(i2<0),nColumn-(i2<0),r1);
      if( i2<0 ){
        sqlite3VdbeAddOp3(v, OP_Insert, iParm, r1, regRow);
      }else{
        sqlite3VdbeAddOp4Int(v, OP_IdxInsert, iParm, r1, regRow, i2);
      }
      break;
    }
    default: {
      assert( eDest==SRT_Output || eDest==SRT_Coroutine ); 
      testcase( eDest==SRT_Output );
      testcase( eDest==SRT_Coroutine );
      if( eDest==SRT_Output ){
        sqlite3VdbeAddOp2(v, OP_ResultRow, pDest->iSdst, nColumn);
      }else{
        sqlite3VdbeAddOp1(v, OP_Yield, pDest->iSDParm);
      }
      break;
    }
  }
  if( regRowid ){
    if( eDest==SRT_Set ){
      sqlite3ReleaseTempRange(pParse, regRow, nColumn);
    }else{
      sqlite3ReleaseTempReg(pParse, regRow);
    }
    sqlite3ReleaseTempReg(pParse, regRowid);
  }
  /* The bottom of the loop
  */
  sqlite3VdbeResolveLabel(v, addrContinue);
  if( pSort->sortFlags & SORTFLAG_UseSorter ){
    sqlite3VdbeAddOp2(v, OP_SorterNext, iTab, addr); VdbeCoverage(v);
  }else{
    sqlite3VdbeAddOp2(v, OP_Next, iTab, addr); VdbeCoverage(v);
  }
  if( pSort->regReturn ) sqlite3VdbeAddOp1(v, OP_Return, pSort->regReturn);
  sqlite3VdbeResolveLabel(v, addrBreak);
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
**
** This routine has either 3 or 6 parameters depending on whether or not
** the SQLITE_ENABLE_COLUMN_METADATA compile-time option is used.
*/
#ifdef SQLITE_ENABLE_COLUMN_METADATA
# define columnType(A,B,C,D,E) columnTypeImpl(A,B,C,D,E)
#else /* if !defined(SQLITE_ENABLE_COLUMN_METADATA) */
# define columnType(A,B,C,D,E) columnTypeImpl(A,B)
#endif
static const char *columnTypeImpl(
  NameContext *pNC, 
#ifndef SQLITE_ENABLE_COLUMN_METADATA
  Expr *pExpr
#else
  Expr *pExpr,
  const char **pzOrigDb,
  const char **pzOrigTab,
  const char **pzOrigCol
#endif
){
  char const *zType = 0;
  int j;
#ifdef SQLITE_ENABLE_COLUMN_METADATA
  char const *zOrigDb = 0;
  char const *zOrigTab = 0;
  char const *zOrigCol = 0;
#endif

  assert( pExpr!=0 );
  assert( pNC->pSrcList!=0 );
  switch( pExpr->op ){
    case TK_COLUMN: {
      /* The expression is a column. Locate the table the column is being
      ** extracted from in NameContext.pSrcList. This table may be real
      ** database table or a subquery.
      */
      Table *pTab = 0;            /* Table structure column is extracted from */
      Select *pS = 0;             /* Select the column is extracted from */
      int iCol = pExpr->iColumn;  /* Index of column in pTab */
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

      assert( pTab && ExprUseYTab(pExpr) && pExpr->y.pTab==pTab );
      if( pS ){
        /* The "table" is actually a sub-select or a view in the FROM clause
        ** of the SELECT statement. Return the declaration type and origin
        ** data for the result-set column of the sub-select.
        */
        if( iCol<pS->pEList->nExpr
#ifdef SQLITE_ALLOW_ROWID_IN_VIEW
         && iCol>=0
#else
         && ALWAYS(iCol>=0)
#endif
        ){ 
          /* If iCol is less than zero, then the expression requests the
          ** rowid of the sub-select or view. This expression is legal (see 
          ** test case misc2.2.2) - it always evaluates to NULL.
          */
          NameContext sNC;
          Expr *p = pS->pEList->a[iCol].pExpr;
          sNC.pSrcList = pS->pSrc;
          sNC.pNext = pNC;
          sNC.pParse = pNC->pParse;
          zType = columnType(&sNC, p,&zOrigDb,&zOrigTab,&zOrigCol); 
        }
      }else{
        /* A real table or a CTE table */
        assert( !pS );
#ifdef SQLITE_ENABLE_COLUMN_METADATA
        if( iCol<0 ) iCol = pTab->iPKey;
        assert( iCol==XN_ROWID || (iCol>=0 && iCol<pTab->nCol) );
        if( iCol<0 ){
          zType = "INTEGER";
          zOrigCol = "rowid";
        }else{
          zOrigCol = pTab->aCol[iCol].zCnName;
          zType = sqlite3ColumnType(&pTab->aCol[iCol],0);
        }
        zOrigTab = pTab->zName;
        if( pNC->pParse && pTab->pSchema ){
          int iDb = sqlite3SchemaToIndex(pNC->pParse->db, pTab->pSchema);
          zOrigDb = pNC->pParse->db->aDb[iDb].zDbSName;
        }
#else
        assert( iCol==XN_ROWID || (iCol>=0 && iCol<pTab->nCol) );
        if( iCol<0 ){
          zType = "INTEGER";
        }else{
          zType = sqlite3ColumnType(&pTab->aCol[iCol],0);
        }
#endif
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
      Select *pS;
      Expr *p;
      assert( ExprUseXSelect(pExpr) );
      pS = pExpr->x.pSelect;
      p = pS->pEList->a[0].pExpr;
      sNC.pSrcList = pS->pSrc;
      sNC.pNext = pNC;
      sNC.pParse = pNC->pParse;
      zType = columnType(&sNC, p, &zOrigDb, &zOrigTab, &zOrigCol); 
      break;
    }
#endif
  }

#ifdef SQLITE_ENABLE_COLUMN_METADATA  
  if( pzOrigDb ){
    assert( pzOrigTab && pzOrigCol );
    *pzOrigDb = zOrigDb;
    *pzOrigTab = zOrigTab;
    *pzOrigCol = zOrigCol;
  }
#endif
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
  sNC.pNext = 0;
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
#endif /* !defined(SQLITE_OMIT_DECLTYPE) */
}


/*
** Compute the column names for a SELECT statement.
**
** The only guarantee that SQLite makes about column names is that if the
** column has an AS clause assigning it a name, that will be the name used.
** That is the only documented guarantee.  However, countless applications
** developed over the years have made baseless assumptions about column names
** and will break if those assumptions changes.  Hence, use extreme caution
** when modifying this routine to avoid breaking legacy.
**
** See Also: sqlite3ColumnsFromExprList()
**
** The PRAGMA short_column_names and PRAGMA full_column_names settings are
** deprecated.  The default setting is short=ON, full=OFF.  99.9% of all
** applications should operate this way.  Nevertheless, we need to support the
** other modes for legacy:
**
**    short=OFF, full=OFF:      Column name is the text of the expression has it
**                              originally appears in the SELECT statement.  In
**                              other words, the zSpan of the result expression.
**
**    short=ON, full=OFF:       (This is the default setting).  If the result
**                              refers directly to a table column, then the
**                              result column name is just the table column
**                              name: COLUMN.  Otherwise use zSpan.
**
**    full=ON, short=ANY:       If the result refers directly to a table column,
**                              then the result column name with the table name
**                              prefix, ex: TABLE.COLUMN.  Otherwise use zSpan.
*/
void sqlite3GenerateColumnNames(
  Parse *pParse,      /* Parser context */
  Select *pSelect     /* Generate column names for this SELECT statement */
){
  Vdbe *v = pParse->pVdbe;
  int i;
  Table *pTab;
  SrcList *pTabList;
  ExprList *pEList;
  sqlite3 *db = pParse->db;
  int fullName;    /* TABLE.COLUMN if no AS clause and is a direct table ref */
  int srcName;     /* COLUMN or TABLE.COLUMN if no AS clause and is direct */

#ifndef SQLITE_OMIT_EXPLAIN
  /* If this is an EXPLAIN, skip this step */
  if( pParse->explain ){
    return;
  }
#endif

  if( pParse->colNamesSet ) return;
  /* Column names are determined by the left-most term of a compound select */
  while( pSelect->pPrior ) pSelect = pSelect->pPrior;
  SELECTTRACE(1,pParse,pSelect,("generating column names\n"));
  pTabList = pSelect->pSrc;
  pEList = pSelect->pEList;
  assert( v!=0 );
  assert( pTabList!=0 );
  pParse->colNamesSet = 1;
  fullName = (db->flags & SQLITE_FullColNames)!=0;
  srcName = (db->flags & SQLITE_ShortColNames)!=0 || fullName;
  sqlite3VdbeSetNumCols(v, pEList->nExpr);
  for(i=0; i<pEList->nExpr; i++){
    Expr *p = pEList->a[i].pExpr;

    assert( p!=0 );
    assert( p->op!=TK_AGG_COLUMN );  /* Agg processing has not run yet */
    assert( p->op!=TK_COLUMN
        || (ExprUseYTab(p) && p->y.pTab!=0) ); /* Covering idx not yet coded */
    if( pEList->a[i].zEName && pEList->a[i].fg.eEName==ENAME_NAME ){
      /* An AS clause always takes first priority */
      char *zName = pEList->a[i].zEName;
      sqlite3VdbeSetColName(v, i, COLNAME_NAME, zName, SQLITE_TRANSIENT);
    }else if( srcName && p->op==TK_COLUMN ){
      char *zCol;
      int iCol = p->iColumn;
      pTab = p->y.pTab;
      assert( pTab!=0 );
      if( iCol<0 ) iCol = pTab->iPKey;
      assert( iCol==-1 || (iCol>=0 && iCol<pTab->nCol) );
      if( iCol<0 ){
        zCol = "rowid";
      }else{
        zCol = pTab->aCol[iCol].zCnName;
      }
      if( fullName ){
        char *zName = 0;
        zName = sqlite3MPrintf(db, "%s.%s", pTab->zName, zCol);
        sqlite3VdbeSetColName(v, i, COLNAME_NAME, zName, SQLITE_DYNAMIC);
      }else{
        sqlite3VdbeSetColName(v, i, COLNAME_NAME, zCol, SQLITE_TRANSIENT);
      }
    }else{
      const char *z = pEList->a[i].zEName;
      z = z==0 ? sqlite3MPrintf(db, "column%d", i+1) : sqlite3DbStrDup(db, z);
      sqlite3VdbeSetColName(v, i, COLNAME_NAME, z, SQLITE_DYNAMIC);
    }
  }
  generateColumnTypes(pParse, pTabList, pEList);
}

/*
** Given an expression list (which is really the list of expressions
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
**
** The only guarantee that SQLite makes about column names is that if the
** column has an AS clause assigning it a name, that will be the name used.
** That is the only documented guarantee.  However, countless applications
** developed over the years have made baseless assumptions about column names
** and will break if those assumptions changes.  Hence, use extreme caution
** when modifying this routine to avoid breaking legacy.
**
** See Also: sqlite3GenerateColumnNames()
*/
int sqlite3ColumnsFromExprList(
  Parse *pParse,          /* Parsing context */
  ExprList *pEList,       /* Expr list from which to derive column names */
  i16 *pnCol,             /* Write the number of columns here */
  Column **paCol          /* Write the new column list here */
){
  sqlite3 *db = pParse->db;   /* Database connection */
  int i, j;                   /* Loop counters */
  u32 cnt;                    /* Index added to make the name unique */
  Column *aCol, *pCol;        /* For looping over result columns */
  int nCol;                   /* Number of columns in the result set */
  char *zName;                /* Column name */
  int nName;                  /* Size of name in zName[] */
  Hash ht;                    /* Hash table of column names */
  Table *pTab;

  sqlite3HashInit(&ht);
  if( pEList ){
    nCol = pEList->nExpr;
    aCol = sqlite3DbMallocZero(db, sizeof(aCol[0])*nCol);
    testcase( aCol==0 );
    if( NEVER(nCol>32767) ) nCol = 32767;
  }else{
    nCol = 0;
    aCol = 0;
  }
  assert( nCol==(i16)nCol );
  *pnCol = nCol;
  *paCol = aCol;

  for(i=0, pCol=aCol; i<nCol && !db->mallocFailed; i++, pCol++){
    struct ExprList_item *pX = &pEList->a[i];
    struct ExprList_item *pCollide;
    /* Get an appropriate name for the column
    */
    if( (zName = pX->zEName)!=0 && pX->fg.eEName==ENAME_NAME ){
      /* If the column contains an "AS <name>" phrase, use <name> as the name */
    }else{
      Expr *pColExpr = sqlite3ExprSkipCollateAndLikely(pX->pExpr);
      while( ALWAYS(pColExpr!=0) && pColExpr->op==TK_DOT ){
        pColExpr = pColExpr->pRight;
        assert( pColExpr!=0 );
      }
      if( pColExpr->op==TK_COLUMN
       && ALWAYS( ExprUseYTab(pColExpr) )
       && ALWAYS( pColExpr->y.pTab!=0 )
      ){
        /* For columns use the column name name */
        int iCol = pColExpr->iColumn;
        pTab = pColExpr->y.pTab;
        if( iCol<0 ) iCol = pTab->iPKey;
        zName = iCol>=0 ? pTab->aCol[iCol].zCnName : "rowid";
      }else if( pColExpr->op==TK_ID ){
        assert( !ExprHasProperty(pColExpr, EP_IntValue) );
        zName = pColExpr->u.zToken;
      }else{
        /* Use the original text of the column expression as its name */
        assert( zName==pX->zEName );  /* pointer comparison intended */
      }
    }
    if( zName && !sqlite3IsTrueOrFalse(zName) ){
      zName = sqlite3DbStrDup(db, zName);
    }else{
      zName = sqlite3MPrintf(db,"column%d",i+1);
    }

    /* Make sure the column name is unique.  If the name is not unique,
    ** append an integer to the name so that it becomes unique.
    */
    cnt = 0;
    while( zName && (pCollide = sqlite3HashFind(&ht, zName))!=0 ){
      if( pCollide->fg.bUsingTerm ){
        pCol->colFlags |= COLFLAG_NOEXPAND;
      }
      nName = sqlite3Strlen30(zName);
      if( nName>0 ){
        for(j=nName-1; j>0 && sqlite3Isdigit(zName[j]); j--){}
        if( zName[j]==':' ) nName = j;
      }
      zName = sqlite3MPrintf(db, "%.*z:%u", nName, zName, ++cnt);
      if( cnt>3 ) sqlite3_randomness(sizeof(cnt), &cnt);
    }
    pCol->zCnName = zName;
    pCol->hName = sqlite3StrIHash(zName);
    if( pX->fg.bNoExpand ){
      pCol->colFlags |= COLFLAG_NOEXPAND;
    }
    sqlite3ColumnPropertiesFromName(0, pCol);
    if( zName && sqlite3HashInsert(&ht, zName, pX)==pX ){
      sqlite3OomFault(db);
    }
  }
  sqlite3HashClear(&ht);
  if( db->mallocFailed ){
    for(j=0; j<i; j++){
      sqlite3DbFree(db, aCol[j].zCnName);
    }
    sqlite3DbFree(db, aCol);
    *paCol = 0;
    *pnCol = 0;
    return SQLITE_NOMEM_BKPT;
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
void sqlite3SelectAddColumnTypeAndCollation(
  Parse *pParse,        /* Parsing contexts */
  Table *pTab,          /* Add column type information to this table */
  Select *pSelect,      /* SELECT used to determine types and collations */
  char aff              /* Default affinity for columns */
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
  assert( pTab->nCol==pSelect->pEList->nExpr || db->mallocFailed );
  if( db->mallocFailed ) return;
  memset(&sNC, 0, sizeof(sNC));
  sNC.pSrcList = pSelect->pSrc;
  a = pSelect->pEList->a;
  for(i=0, pCol=pTab->aCol; i<pTab->nCol; i++, pCol++){
    const char *zType;
    i64 n, m;
    pTab->tabFlags |= (pCol->colFlags & COLFLAG_NOINSERT);
    p = a[i].pExpr;
    zType = columnType(&sNC, p, 0, 0, 0);
    /* pCol->szEst = ... // Column size est for SELECT tables never used */
    pCol->affinity = sqlite3ExprAffinity(p);
    if( zType ){
      m = sqlite3Strlen30(zType);
      n = sqlite3Strlen30(pCol->zCnName);
      pCol->zCnName = sqlite3DbReallocOrFree(db, pCol->zCnName, n+m+2);
      if( pCol->zCnName ){
        memcpy(&pCol->zCnName[n+1], zType, m+1);
        pCol->colFlags |= COLFLAG_HASTYPE;
      }else{
        testcase( pCol->colFlags & COLFLAG_HASTYPE );
        pCol->colFlags &= ~(COLFLAG_HASTYPE|COLFLAG_HASCOLL);
      }
    }
    if( pCol->affinity<=SQLITE_AFF_NONE ) pCol->affinity = aff;
    pColl = sqlite3ExprCollSeq(pParse, p);
    if( pColl ){
      assert( pTab->pIndex==0 );
      sqlite3ColumnSetColl(db, pCol, pColl->zName);
    }
  }
  pTab->szTabRow = 1; /* Any non-zero value works */
}

/*
** Given a SELECT statement, generate a Table structure that describes
** the result set of that SELECT.
*/
Table *sqlite3ResultSetOfSelect(Parse *pParse, Select *pSelect, char aff){
  Table *pTab;
  sqlite3 *db = pParse->db;
  u64 savedFlags;

  savedFlags = db->flags;
  db->flags &= ~(u64)SQLITE_FullColNames;
  db->flags |= SQLITE_ShortColNames;
  sqlite3SelectPrep(pParse, pSelect, 0);
  db->flags = savedFlags;
  if( pParse->nErr ) return 0;
  while( pSelect->pPrior ) pSelect = pSelect->pPrior;
  pTab = sqlite3DbMallocZero(db, sizeof(Table) );
  if( pTab==0 ){
    return 0;
  }
  pTab->nTabRef = 1;
  pTab->zName = 0;
  pTab->nRowLogEst = 200; assert( 200==sqlite3LogEst(1048576) );
  sqlite3ColumnsFromExprList(pParse, pSelect->pEList, &pTab->nCol, &pTab->aCol);
  sqlite3SelectAddColumnTypeAndCollation(pParse, pTab, pSelect, aff);
  pTab->iPKey = -1;
  if( db->mallocFailed ){
    sqlite3DeleteTable(db, pTab);
    return 0;
  }
  return pTab;
}

/*
** Get a VDBE for the given parser context.  Create a new one if necessary.
** If an error occurs, return NULL and leave a message in pParse.
*/
Vdbe *sqlite3GetVdbe(Parse *pParse){
  if( pParse->pVdbe ){
    return pParse->pVdbe;
  }
  if( pParse->pToplevel==0
   && OptimizationEnabled(pParse->db,SQLITE_FactorOutConst)
  ){
    pParse->okConstFactor = 1;
  }
  return sqlite3VdbeCreate(pParse);
}


/*
** Compute the iLimit and iOffset fields of the SELECT based on the
** pLimit expressions.  pLimit->pLeft and pLimit->pRight hold the expressions
** that appear in the original SQL statement after the LIMIT and OFFSET
** keywords.  Or NULL if those keywords are omitted. iLimit and iOffset 
** are the integer memory register numbers for counters used to compute 
** the limit and offset.  If there is no limit and/or offset, then 
** iLimit and iOffset are negative.
**
** This routine changes the values of iLimit and iOffset only if
** a limit or offset is defined by pLimit->pLeft and pLimit->pRight.  iLimit
** and iOffset should have been preset to appropriate default values (zero)
** prior to calling this routine.
**
** The iOffset register (if it exists) is initialized to the value
** of the OFFSET.  The iLimit register is initialized to LIMIT.  Register
** iOffset+1 is initialized to LIMIT+OFFSET.
**
** Only if pLimit->pLeft!=0 do the limit registers get
** redefined.  The UNION ALL operator uses this property to force
** the reuse of the same limit and offset registers across multiple
** SELECT statements.
*/
static void computeLimitRegisters(Parse *pParse, Select *p, int iBreak){
  Vdbe *v = 0;
  int iLimit = 0;
  int iOffset;
  int n;
  Expr *pLimit = p->pLimit;

  if( p->iLimit ) return;

  /* 
  ** "LIMIT -1" always shows all rows.  There is some
  ** controversy about what the correct behavior should be.
  ** The current implementation interprets "LIMIT 0" to mean
  ** no rows.
  */
  if( pLimit ){
    assert( pLimit->op==TK_LIMIT );
    assert( pLimit->pLeft!=0 );
    p->iLimit = iLimit = ++pParse->nMem;
    v = sqlite3GetVdbe(pParse);
    assert( v!=0 );
    if( sqlite3ExprIsInteger(pLimit->pLeft, &n) ){
      sqlite3VdbeAddOp2(v, OP_Integer, n, iLimit);
      VdbeComment((v, "LIMIT counter"));
      if( n==0 ){
        sqlite3VdbeGoto(v, iBreak);
      }else if( n>=0 && p->nSelectRow>sqlite3LogEst((u64)n) ){
        p->nSelectRow = sqlite3LogEst((u64)n);
        p->selFlags |= SF_FixedLimit;
      }
    }else{
      sqlite3ExprCode(pParse, pLimit->pLeft, iLimit);
      sqlite3VdbeAddOp1(v, OP_MustBeInt, iLimit); VdbeCoverage(v);
      VdbeComment((v, "LIMIT counter"));
      sqlite3VdbeAddOp2(v, OP_IfNot, iLimit, iBreak); VdbeCoverage(v);
    }
    if( pLimit->pRight ){
      p->iOffset = iOffset = ++pParse->nMem;
      pParse->nMem++;   /* Allocate an extra register for limit+offset */
      sqlite3ExprCode(pParse, pLimit->pRight, iOffset);
      sqlite3VdbeAddOp1(v, OP_MustBeInt, iOffset); VdbeCoverage(v);
      VdbeComment((v, "OFFSET counter"));
      sqlite3VdbeAddOp3(v, OP_OffsetLimit, iLimit, iOffset+1, iOffset);
      VdbeComment((v, "LIMIT+OFFSET"));
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
  /* iCol must be less than p->pEList->nExpr.  Otherwise an error would
  ** have been thrown during name resolution and we would not have gotten
  ** this far */
  if( pRet==0 && ALWAYS(iCol<p->pEList->nExpr) ){
    pRet = sqlite3ExprCollSeq(pParse, p->pEList->a[iCol].pExpr);
  }
  return pRet;
}

/*
** The select statement passed as the second parameter is a compound SELECT
** with an ORDER BY clause. This function allocates and returns a KeyInfo
** structure suitable for implementing the ORDER BY.
**
** Space to hold the KeyInfo structure is obtained from malloc. The calling
** function is responsible for ensuring that this structure is eventually
** freed.
*/
static KeyInfo *multiSelectOrderByKeyInfo(Parse *pParse, Select *p, int nExtra){
  ExprList *pOrderBy = p->pOrderBy;
  int nOrderBy = ALWAYS(pOrderBy!=0) ? pOrderBy->nExpr : 0;
  sqlite3 *db = pParse->db;
  KeyInfo *pRet = sqlite3KeyInfoAlloc(db, nOrderBy+nExtra, 1);
  if( pRet ){
    int i;
    for(i=0; i<nOrderBy; i++){
      struct ExprList_item *pItem = &pOrderBy->a[i];
      Expr *pTerm = pItem->pExpr;
      CollSeq *pColl;

      if( pTerm->flags & EP_Collate ){
        pColl = sqlite3ExprCollSeq(pParse, pTerm);
      }else{
        pColl = multiSelectCollSeq(pParse, p, pItem->u.x.iOrderByCol-1);
        if( pColl==0 ) pColl = db->pDfltColl;
        pOrderBy->a[i].pExpr =
          sqlite3ExprAddCollateString(pParse, pTerm, pColl->zName);
      }
      assert( sqlite3KeyInfoIsWriteable(pRet) );
      pRet->aColl[i] = pColl;
      pRet->aSortFlags[i] = pOrderBy->a[i].fg.sortFlags;
    }
  }

  return pRet;
}

#ifndef SQLITE_OMIT_CTE
/*
** This routine generates VDBE code to compute the content of a WITH RECURSIVE
** query of the form:
**
**   <recursive-table> AS (<setup-query> UNION [ALL] <recursive-query>)
**                         \___________/             \_______________/
**                           p->pPrior                      p
**
**
** There is exactly one reference to the recursive-table in the FROM clause
** of recursive-query, marked with the SrcList->a[].fg.isRecursive flag.
**
** The setup-query runs once to generate an initial set of rows that go
** into a Queue table.  Rows are extracted from the Queue table one by
** one.  Each row extracted from Queue is output to pDest.  Then the single
** extracted row (now in the iCurrent table) becomes the content of the
** recursive-table for a recursive-query run.  The output of the recursive-query
** is added back into the Queue table.  Then another row is extracted from Queue
** and the iteration continues until the Queue table is empty.
**
** If the compound query operator is UNION then no duplicate rows are ever
** inserted into the Queue table.  The iDistinct table keeps a copy of all rows
** that have ever been inserted into Queue and causes duplicates to be
** discarded.  If the operator is UNION ALL, then duplicates are allowed.
** 
** If the query has an ORDER BY, then entries in the Queue table are kept in
** ORDER BY order and the first entry is extracted for each cycle.  Without
** an ORDER BY, the Queue table is just a FIFO.
**
** If a LIMIT clause is provided, then the iteration stops after LIMIT rows
** have been output to pDest.  A LIMIT of zero means to output no rows and a
** negative LIMIT means to output all rows.  If there is also an OFFSET clause
** with a positive value, then the first OFFSET outputs are discarded rather
** than being sent to pDest.  The LIMIT count does not begin until after OFFSET
** rows have been skipped.
*/
static void generateWithRecursiveQuery(
  Parse *pParse,        /* Parsing context */
  Select *p,            /* The recursive SELECT to be coded */
  SelectDest *pDest     /* What to do with query results */
){
  SrcList *pSrc = p->pSrc;      /* The FROM clause of the recursive query */
  int nCol = p->pEList->nExpr;  /* Number of columns in the recursive table */
  Vdbe *v = pParse->pVdbe;      /* The prepared statement under construction */
  Select *pSetup;               /* The setup query */
  Select *pFirstRec;            /* Left-most recursive term */
  int addrTop;                  /* Top of the loop */
  int addrCont, addrBreak;      /* CONTINUE and BREAK addresses */
  int iCurrent = 0;             /* The Current table */
  int regCurrent;               /* Register holding Current table */
  int iQueue;                   /* The Queue table */
  int iDistinct = 0;            /* To ensure unique results if UNION */
  int eDest = SRT_Fifo;         /* How to write to Queue */
  SelectDest destQueue;         /* SelectDest targetting the Queue table */
  int i;                        /* Loop counter */
  int rc;                       /* Result code */
  ExprList *pOrderBy;           /* The ORDER BY clause */
  Expr *pLimit;                 /* Saved LIMIT and OFFSET */
  int regLimit, regOffset;      /* Registers used by LIMIT and OFFSET */

#ifndef SQLITE_OMIT_WINDOWFUNC
  if( p->pWin ){
    sqlite3ErrorMsg(pParse, "cannot use window functions in recursive queries");
    return;
  }
#endif

  /* Obtain authorization to do a recursive query */
  if( sqlite3AuthCheck(pParse, SQLITE_RECURSIVE, 0, 0, 0) ) return;

  /* Process the LIMIT and OFFSET clauses, if they exist */
  addrBreak = sqlite3VdbeMakeLabel(pParse);
  p->nSelectRow = 320;  /* 4 billion rows */
  computeLimitRegisters(pParse, p, addrBreak);
  pLimit = p->pLimit;
  regLimit = p->iLimit;
  regOffset = p->iOffset;
  p->pLimit = 0;
  p->iLimit = p->iOffset = 0;
  pOrderBy = p->pOrderBy;

  /* Locate the cursor number of the Current table */
  for(i=0; ALWAYS(i<pSrc->nSrc); i++){
    if( pSrc->a[i].fg.isRecursive ){
      iCurrent = pSrc->a[i].iCursor;
      break;
    }
  }

  /* Allocate cursors numbers for Queue and Distinct.  The cursor number for
  ** the Distinct table must be exactly one greater than Queue in order
  ** for the SRT_DistFifo and SRT_DistQueue destinations to work. */
  iQueue = pParse->nTab++;
  if( p->op==TK_UNION ){
    eDest = pOrderBy ? SRT_DistQueue : SRT_DistFifo;
    iDistinct = pParse->nTab++;
  }else{
    eDest = pOrderBy ? SRT_Queue : SRT_Fifo;
  }
  sqlite3SelectDestInit(&destQueue, eDest, iQueue);

  /* Allocate cursors for Current, Queue, and Distinct. */
  regCurrent = ++pParse->nMem;
  sqlite3VdbeAddOp3(v, OP_OpenPseudo, iCurrent, regCurrent, nCol);
  if( pOrderBy ){
    KeyInfo *pKeyInfo = multiSelectOrderByKeyInfo(pParse, p, 1);
    sqlite3VdbeAddOp4(v, OP_OpenEphemeral, iQueue, pOrderBy->nExpr+2, 0,
                      (char*)pKeyInfo, P4_KEYINFO);
    destQueue.pOrderBy = pOrderBy;
  }else{
    sqlite3VdbeAddOp2(v, OP_OpenEphemeral, iQueue, nCol);
  }
  VdbeComment((v, "Queue table"));
  if( iDistinct ){
    p->addrOpenEphm[0] = sqlite3VdbeAddOp2(v, OP_OpenEphemeral, iDistinct, 0);
    p->selFlags |= SF_UsesEphemeral;
  }

  /* Detach the ORDER BY clause from the compound SELECT */
  p->pOrderBy = 0;

  /* Figure out how many elements of the compound SELECT are part of the
  ** recursive query.  Make sure no recursive elements use aggregate
  ** functions.  Mark the recursive elements as UNION ALL even if they
  ** are really UNION because the distinctness will be enforced by the
  ** iDistinct table.  pFirstRec is left pointing to the left-most
  ** recursive term of the CTE.
  */
  for(pFirstRec=p; ALWAYS(pFirstRec!=0); pFirstRec=pFirstRec->pPrior){
    if( pFirstRec->selFlags & SF_Aggregate ){
      sqlite3ErrorMsg(pParse, "recursive aggregate queries not supported");
      goto end_of_recursive_query;
    }
    pFirstRec->op = TK_ALL;
    if( (pFirstRec->pPrior->selFlags & SF_Recursive)==0 ) break;
  }

  /* Store the results of the setup-query in Queue. */
  pSetup = pFirstRec->pPrior;
  pSetup->pNext = 0;
  ExplainQueryPlan((pParse, 1, "SETUP"));
  rc = sqlite3Select(pParse, pSetup, &destQueue);
  pSetup->pNext = p;
  if( rc ) goto end_of_recursive_query;

  /* Find the next row in the Queue and output that row */
  addrTop = sqlite3VdbeAddOp2(v, OP_Rewind, iQueue, addrBreak); VdbeCoverage(v);

  /* Transfer the next row in Queue over to Current */
  sqlite3VdbeAddOp1(v, OP_NullRow, iCurrent); /* To reset column cache */
  if( pOrderBy ){
    sqlite3VdbeAddOp3(v, OP_Column, iQueue, pOrderBy->nExpr+1, regCurrent);
  }else{
    sqlite3VdbeAddOp2(v, OP_RowData, iQueue, regCurrent);
  }
  sqlite3VdbeAddOp1(v, OP_Delete, iQueue);

  /* Output the single row in Current */
  addrCont = sqlite3VdbeMakeLabel(pParse);
  codeOffset(v, regOffset, addrCont);
  selectInnerLoop(pParse, p, iCurrent,
      0, 0, pDest, addrCont, addrBreak);
  if( regLimit ){
    sqlite3VdbeAddOp2(v, OP_DecrJumpZero, regLimit, addrBreak);
    VdbeCoverage(v);
  }
  sqlite3VdbeResolveLabel(v, addrCont);

  /* Execute the recursive SELECT taking the single row in Current as
  ** the value for the recursive-table. Store the results in the Queue.
  */
  pFirstRec->pPrior = 0;
  ExplainQueryPlan((pParse, 1, "RECURSIVE STEP"));
  sqlite3Select(pParse, p, &destQueue);
  assert( pFirstRec->pPrior==0 );
  pFirstRec->pPrior = pSetup;

  /* Keep running the loop until the Queue is empty */
  sqlite3VdbeGoto(v, addrTop);
  sqlite3VdbeResolveLabel(v, addrBreak);

end_of_recursive_query:
  sqlite3ExprListDelete(pParse->db, p->pOrderBy);
  p->pOrderBy = pOrderBy;
  p->pLimit = pLimit;
  return;
}
#endif /* SQLITE_OMIT_CTE */

/* Forward references */
static int multiSelectOrderBy(
  Parse *pParse,        /* Parsing context */
  Select *p,            /* The right-most of SELECTs to be coded */
  SelectDest *pDest     /* What to do with query results */
);

/*
** Handle the special case of a compound-select that originates from a
** VALUES clause.  By handling this as a special case, we avoid deep
** recursion, and thus do not need to enforce the SQLITE_LIMIT_COMPOUND_SELECT
** on a VALUES clause.
**
** Because the Select object originates from a VALUES clause:
**   (1) There is no LIMIT or OFFSET or else there is a LIMIT of exactly 1
**   (2) All terms are UNION ALL
**   (3) There is no ORDER BY clause
**
** The "LIMIT of exactly 1" case of condition (1) comes about when a VALUES
** clause occurs within scalar expression (ex: "SELECT (VALUES(1),(2),(3))").
** The sqlite3CodeSubselect will have added the LIMIT 1 clause in tht case.
** Since the limit is exactly 1, we only need to evaluate the left-most VALUES.
*/
static int multiSelectValues(
  Parse *pParse,        /* Parsing context */
  Select *p,            /* The right-most of SELECTs to be coded */
  SelectDest *pDest     /* What to do with query results */
){
  int nRow = 1;
  int rc = 0;
  int bShowAll = p->pLimit==0;
  assert( p->selFlags & SF_MultiValue );
  do{
    assert( p->selFlags & SF_Values );
    assert( p->op==TK_ALL || (p->op==TK_SELECT && p->pPrior==0) );
    assert( p->pNext==0 || p->pEList->nExpr==p->pNext->pEList->nExpr );
#ifndef SQLITE_OMIT_WINDOWFUNC
    if( p->pWin ) return -1;
#endif
    if( p->pPrior==0 ) break;
    assert( p->pPrior->pNext==p );
    p = p->pPrior;
    nRow += bShowAll;
  }while(1);
  ExplainQueryPlan((pParse, 0, "SCAN %d CONSTANT ROW%s", nRow,
                    nRow==1 ? "" : "S"));
  while( p ){
    selectInnerLoop(pParse, p, -1, 0, 0, pDest, 1, 1);
    if( !bShowAll ) break;
    p->nSelectRow = nRow;
    p = p->pNext;
  }
  return rc;
}

/*
** Return true if the SELECT statement which is known to be the recursive
** part of a recursive CTE still has its anchor terms attached.  If the
** anchor terms have already been removed, then return false.
*/
static int hasAnchor(Select *p){
  while( p && (p->selFlags & SF_Recursive)!=0 ){ p = p->pPrior; }
  return p!=0;
}

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
  assert( (p->selFlags & SF_Recursive)==0 || p->op==TK_ALL || p->op==TK_UNION );
  assert( p->selFlags & SF_Compound );
  db = pParse->db;
  pPrior = p->pPrior;
  dest = *pDest;
  assert( pPrior->pOrderBy==0 );
  assert( pPrior->pLimit==0 );

  v = sqlite3GetVdbe(pParse);
  assert( v!=0 );  /* The VDBE already created by calling function */

  /* Create the destination temporary table if necessary
  */
  if( dest.eDest==SRT_EphemTab ){
    assert( p->pEList );
    sqlite3VdbeAddOp2(v, OP_OpenEphemeral, dest.iSDParm, p->pEList->nExpr);
    dest.eDest = SRT_Table;
  }

  /* Special handling for a compound-select that originates as a VALUES clause.
  */
  if( p->selFlags & SF_MultiValue ){
    rc = multiSelectValues(pParse, p, &dest);
    if( rc>=0 ) goto multi_select_end;
    rc = SQLITE_OK;
  }

  /* Make sure all SELECTs in the statement have the same number of elements
  ** in their result sets.
  */
  assert( p->pEList && pPrior->pEList );
  assert( p->pEList->nExpr==pPrior->pEList->nExpr );

#ifndef SQLITE_OMIT_CTE
  if( (p->selFlags & SF_Recursive)!=0 && hasAnchor(p) ){
    generateWithRecursiveQuery(pParse, p, &dest);
  }else
#endif

  /* Compound SELECTs that have an ORDER BY clause are handled separately.
  */
  if( p->pOrderBy ){
    return multiSelectOrderBy(pParse, p, pDest);
  }else{

#ifndef SQLITE_OMIT_EXPLAIN
    if( pPrior->pPrior==0 ){
      ExplainQueryPlan((pParse, 1, "COMPOUND QUERY"));
      ExplainQueryPlan((pParse, 1, "LEFT-MOST SUBQUERY"));
    }
#endif

    /* Generate code for the left and right SELECT statements.
    */
    switch( p->op ){
      case TK_ALL: {
        int addr = 0;
        int nLimit = 0;  /* Initialize to suppress harmless compiler warning */
        assert( !pPrior->pLimit );
        pPrior->iLimit = p->iLimit;
        pPrior->iOffset = p->iOffset;
        pPrior->pLimit = p->pLimit;
        SELECTTRACE(1, pParse, p, ("multiSelect UNION ALL left...\n"));
        rc = sqlite3Select(pParse, pPrior, &dest);
        pPrior->pLimit = 0;
        if( rc ){
          goto multi_select_end;
        }
        p->pPrior = 0;
        p->iLimit = pPrior->iLimit;
        p->iOffset = pPrior->iOffset;
        if( p->iLimit ){
          addr = sqlite3VdbeAddOp1(v, OP_IfNot, p->iLimit); VdbeCoverage(v);
          VdbeComment((v, "Jump ahead if LIMIT reached"));
          if( p->iOffset ){
            sqlite3VdbeAddOp3(v, OP_OffsetLimit,
                              p->iLimit, p->iOffset+1, p->iOffset);
          }
        }
        ExplainQueryPlan((pParse, 1, "UNION ALL"));
        SELECTTRACE(1, pParse, p, ("multiSelect UNION ALL right...\n"));
        rc = sqlite3Select(pParse, p, &dest);
        testcase( rc!=SQLITE_OK );
        pDelete = p->pPrior;
        p->pPrior = pPrior;
        p->nSelectRow = sqlite3LogEstAdd(p->nSelectRow, pPrior->nSelectRow);
        if( p->pLimit
         && sqlite3ExprIsInteger(p->pLimit->pLeft, &nLimit)
         && nLimit>0 && p->nSelectRow > sqlite3LogEst((u64)nLimit) 
        ){
          p->nSelectRow = sqlite3LogEst((u64)nLimit);
        }
        if( addr ){
          sqlite3VdbeJumpHere(v, addr);
        }
        break;
      }
      case TK_EXCEPT:
      case TK_UNION: {
        int unionTab;    /* Cursor number of the temp table holding result */
        u8 op = 0;       /* One of the SRT_ operations to apply to self */
        int priorOp;     /* The SRT_ operation to apply to prior selects */
        Expr *pLimit;    /* Saved values of p->nLimit  */
        int addr;
        SelectDest uniondest;
  
        testcase( p->op==TK_EXCEPT );
        testcase( p->op==TK_UNION );
        priorOp = SRT_Union;
        if( dest.eDest==priorOp ){
          /* We can reuse a temporary table generated by a SELECT to our
          ** right.
          */
          assert( p->pLimit==0 );      /* Not allowed on leftward elements */
          unionTab = dest.iSDParm;
        }else{
          /* We will need to create our own temporary table to hold the
          ** intermediate results.
          */
          unionTab = pParse->nTab++;
          assert( p->pOrderBy==0 );
          addr = sqlite3VdbeAddOp2(v, OP_OpenEphemeral, unionTab, 0);
          assert( p->addrOpenEphm[0] == -1 );
          p->addrOpenEphm[0] = addr;
          findRightmost(p)->selFlags |= SF_UsesEphemeral;
          assert( p->pEList );
        }
          
  
        /* Code the SELECT statements to our left
        */
        assert( !pPrior->pOrderBy );
        sqlite3SelectDestInit(&uniondest, priorOp, unionTab);
        SELECTTRACE(1, pParse, p, ("multiSelect EXCEPT/UNION left...\n"));
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
        uniondest.eDest = op;
        ExplainQueryPlan((pParse, 1, "%s USING TEMP B-TREE",
                          sqlite3SelectOpName(p->op)));
        SELECTTRACE(1, pParse, p, ("multiSelect EXCEPT/UNION right...\n"));
        rc = sqlite3Select(pParse, p, &uniondest);
        testcase( rc!=SQLITE_OK );
        assert( p->pOrderBy==0 );
        pDelete = p->pPrior;
        p->pPrior = pPrior;
        p->pOrderBy = 0;
        if( p->op==TK_UNION ){
          p->nSelectRow = sqlite3LogEstAdd(p->nSelectRow, pPrior->nSelectRow);
        }
        sqlite3ExprDelete(db, p->pLimit);
        p->pLimit = pLimit;
        p->iLimit = 0;
        p->iOffset = 0;
  
        /* Convert the data in the temporary table into whatever form
        ** it is that we currently need.
        */
        assert( unionTab==dest.iSDParm || dest.eDest!=priorOp );
        assert( p->pEList || db->mallocFailed );
        if( dest.eDest!=priorOp && db->mallocFailed==0 ){
          int iCont, iBreak, iStart;
          iBreak = sqlite3VdbeMakeLabel(pParse);
          iCont = sqlite3VdbeMakeLabel(pParse);
          computeLimitRegisters(pParse, p, iBreak);
          sqlite3VdbeAddOp2(v, OP_Rewind, unionTab, iBreak); VdbeCoverage(v);
          iStart = sqlite3VdbeCurrentAddr(v);
          selectInnerLoop(pParse, p, unionTab,
                          0, 0, &dest, iCont, iBreak);
          sqlite3VdbeResolveLabel(v, iCont);
          sqlite3VdbeAddOp2(v, OP_Next, unionTab, iStart); VdbeCoverage(v);
          sqlite3VdbeResolveLabel(v, iBreak);
          sqlite3VdbeAddOp2(v, OP_Close, unionTab, 0);
        }
        break;
      }
      default: assert( p->op==TK_INTERSECT ); {
        int tab1, tab2;
        int iCont, iBreak, iStart;
        Expr *pLimit;
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
        findRightmost(p)->selFlags |= SF_UsesEphemeral;
        assert( p->pEList );
  
        /* Code the SELECTs to our left into temporary table "tab1".
        */
        sqlite3SelectDestInit(&intersectdest, SRT_Union, tab1);
        SELECTTRACE(1, pParse, p, ("multiSelect INTERSECT left...\n"));
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
        intersectdest.iSDParm = tab2;
        ExplainQueryPlan((pParse, 1, "%s USING TEMP B-TREE",
                          sqlite3SelectOpName(p->op)));
        SELECTTRACE(1, pParse, p, ("multiSelect INTERSECT right...\n"));
        rc = sqlite3Select(pParse, p, &intersectdest);
        testcase( rc!=SQLITE_OK );
        pDelete = p->pPrior;
        p->pPrior = pPrior;
        if( p->nSelectRow>pPrior->nSelectRow ){
          p->nSelectRow = pPrior->nSelectRow;
        }
        sqlite3ExprDelete(db, p->pLimit);
        p->pLimit = pLimit;
  
        /* Generate code to take the intersection of the two temporary
        ** tables.
        */
        if( rc ) break;
        assert( p->pEList );
        iBreak = sqlite3VdbeMakeLabel(pParse);
        iCont = sqlite3VdbeMakeLabel(pParse);
        computeLimitRegisters(pParse, p, iBreak);
        sqlite3VdbeAddOp2(v, OP_Rewind, tab1, iBreak); VdbeCoverage(v);
        r1 = sqlite3GetTempReg(pParse);
        iStart = sqlite3VdbeAddOp2(v, OP_RowData, tab1, r1);
        sqlite3VdbeAddOp4Int(v, OP_NotFound, tab2, iCont, r1, 0);
        VdbeCoverage(v);
        sqlite3ReleaseTempReg(pParse, r1);
        selectInnerLoop(pParse, p, tab1,
                        0, 0, &dest, iCont, iBreak);
        sqlite3VdbeResolveLabel(v, iCont);
        sqlite3VdbeAddOp2(v, OP_Next, tab1, iStart); VdbeCoverage(v);
        sqlite3VdbeResolveLabel(v, iBreak);
        sqlite3VdbeAddOp2(v, OP_Close, tab2, 0);
        sqlite3VdbeAddOp2(v, OP_Close, tab1, 0);
        break;
      }
    }
  
  #ifndef SQLITE_OMIT_EXPLAIN
    if( p->pNext==0 ){
      ExplainQueryPlanPop(pParse);
    }
  #endif
  }
  if( pParse->nErr ) goto multi_select_end;
  
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

    assert( p->pNext==0 );
    assert( p->pEList!=0 );
    nCol = p->pEList->nExpr;
    pKeyInfo = sqlite3KeyInfoAlloc(db, nCol, 1);
    if( !pKeyInfo ){
      rc = SQLITE_NOMEM_BKPT;
      goto multi_select_end;
    }
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
        sqlite3VdbeChangeP4(v, addr, (char*)sqlite3KeyInfoRef(pKeyInfo),
                            P4_KEYINFO);
        pLoop->addrOpenEphm[i] = -1;
      }
    }
    sqlite3KeyInfoUnref(pKeyInfo);
  }

multi_select_end:
  pDest->iSdst = dest.iSdst;
  pDest->nSdst = dest.nSdst;
  if( pDelete ){
    sqlite3ParserAddCleanup(pParse,
        (void(*)(sqlite3*,void*))sqlite3SelectDelete,
        pDelete);
  }
  return rc;
}
#endif /* SQLITE_OMIT_COMPOUND_SELECT */

/*
** Error message for when two or more terms of a compound select have different
** size result sets.
*/
void sqlite3SelectWrongNumTermsError(Parse *pParse, Select *p){
  if( p->selFlags & SF_Values ){
    sqlite3ErrorMsg(pParse, "all VALUES must have the same number of terms");
  }else{
    sqlite3ErrorMsg(pParse, "SELECTs to the left and right of %s"
      " do not have the same number of result columns",
      sqlite3SelectOpName(p->op));
  }
}

/*
** Code an output subroutine for a coroutine implementation of a
** SELECT statment.
**
** The data to be output is contained in pIn->iSdst.  There are
** pIn->nSdst columns to be output.  pDest is where the output should
** be sent.
**
** regReturn is the number of the register holding the subroutine
** return address.
**
** If regPrev>0 then it is the first register in a vector that
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
  int iBreak              /* Jump here if we hit the LIMIT */
){
  Vdbe *v = pParse->pVdbe;
  int iContinue;
  int addr;

  addr = sqlite3VdbeCurrentAddr(v);
  iContinue = sqlite3VdbeMakeLabel(pParse);

  /* Suppress duplicates for UNION, EXCEPT, and INTERSECT 
  */
  if( regPrev ){
    int addr1, addr2;
    addr1 = sqlite3VdbeAddOp1(v, OP_IfNot, regPrev); VdbeCoverage(v);
    addr2 = sqlite3VdbeAddOp4(v, OP_Compare, pIn->iSdst, regPrev+1, pIn->nSdst,
                              (char*)sqlite3KeyInfoRef(pKeyInfo), P4_KEYINFO);
    sqlite3VdbeAddOp3(v, OP_Jump, addr2+2, iContinue, addr2+2); VdbeCoverage(v);
    sqlite3VdbeJumpHere(v, addr1);
    sqlite3VdbeAddOp3(v, OP_Copy, pIn->iSdst, regPrev+1, pIn->nSdst-1);
    sqlite3VdbeAddOp2(v, OP_Integer, 1, regPrev);
  }
  if( pParse->db->mallocFailed ) return 0;

  /* Suppress the first OFFSET entries if there is an OFFSET clause
  */
  codeOffset(v, p->iOffset, iContinue);

  assert( pDest->eDest!=SRT_Exists );
  assert( pDest->eDest!=SRT_Table );
  switch( pDest->eDest ){
    /* Store the result as data using a unique key.
    */
    case SRT_EphemTab: {
      int r1 = sqlite3GetTempReg(pParse);
      int r2 = sqlite3GetTempReg(pParse);
      sqlite3VdbeAddOp3(v, OP_MakeRecord, pIn->iSdst, pIn->nSdst, r1);
      sqlite3VdbeAddOp2(v, OP_NewRowid, pDest->iSDParm, r2);
      sqlite3VdbeAddOp3(v, OP_Insert, pDest->iSDParm, r1, r2);
      sqlite3VdbeChangeP5(v, OPFLAG_APPEND);
      sqlite3ReleaseTempReg(pParse, r2);
      sqlite3ReleaseTempReg(pParse, r1);
      break;
    }

#ifndef SQLITE_OMIT_SUBQUERY
    /* If we are creating a set for an "expr IN (SELECT ...)".
    */
    case SRT_Set: {
      int r1;
      testcase( pIn->nSdst>1 );
      r1 = sqlite3GetTempReg(pParse);
      sqlite3VdbeAddOp4(v, OP_MakeRecord, pIn->iSdst, pIn->nSdst, 
          r1, pDest->zAffSdst, pIn->nSdst);
      sqlite3VdbeAddOp4Int(v, OP_IdxInsert, pDest->iSDParm, r1,
                           pIn->iSdst, pIn->nSdst);
      sqlite3ReleaseTempReg(pParse, r1);
      break;
    }

    /* If this is a scalar select that is part of an expression, then
    ** store the results in the appropriate memory cell and break out
    ** of the scan loop.  Note that the select might return multiple columns
    ** if it is the RHS of a row-value IN operator.
    */
    case SRT_Mem: {
      testcase( pIn->nSdst>1 );
      sqlite3ExprCodeMove(pParse, pIn->iSdst, pDest->iSDParm, pIn->nSdst);
      /* The LIMIT clause will jump out of the loop for us */
      break;
    }
#endif /* #ifndef SQLITE_OMIT_SUBQUERY */

    /* The results are stored in a sequence of registers
    ** starting at pDest->iSdst.  Then the co-routine yields.
    */
    case SRT_Coroutine: {
      if( pDest->iSdst==0 ){
        pDest->iSdst = sqlite3GetTempRange(pParse, pIn->nSdst);
        pDest->nSdst = pIn->nSdst;
      }
      sqlite3ExprCodeMove(pParse, pIn->iSdst, pDest->iSdst, pIn->nSdst);
      sqlite3VdbeAddOp1(v, OP_Yield, pDest->iSDParm);
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
      sqlite3VdbeAddOp2(v, OP_ResultRow, pIn->iSdst, pIn->nSdst);
      break;
    }
  }

  /* Jump to the end of the loop if the LIMIT is reached.
  */
  if( p->iLimit ){
    sqlite3VdbeAddOp2(v, OP_DecrJumpZero, p->iLimit, iBreak); VdbeCoverage(v);
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
  Select *pSplit;       /* Left-most SELECT in the right-hand group */
  int nSelect;          /* Number of SELECT statements in the compound */
  Vdbe *v;              /* Generate code to this VDBE */
  SelectDest destA;     /* Destination for coroutine A */
  SelectDest destB;     /* Destination for coroutine B */
  int regAddrA;         /* Address register for select-A coroutine */
  int regAddrB;         /* Address register for select-B coroutine */
  int addrSelectA;      /* Address of the select-A coroutine */
  int addrSelectB;      /* Address of the select-B coroutine */
  int regOutA;          /* Address register for the output-A subroutine */
  int regOutB;          /* Address register for the output-B subroutine */
  int addrOutA;         /* Address of the output-A subroutine */
  int addrOutB = 0;     /* Address of the output-B subroutine */
  int addrEofA;         /* Address of the select-A-exhausted subroutine */
  int addrEofA_noB;     /* Alternate addrEofA if B is uninitialized */
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
  int addr1;            /* Jump instructions that get retargetted */
  int op;               /* One of TK_ALL, TK_UNION, TK_EXCEPT, TK_INTERSECT */
  KeyInfo *pKeyDup = 0; /* Comparison information for duplicate removal */
  KeyInfo *pKeyMerge;   /* Comparison information for merging rows */
  sqlite3 *db;          /* Database connection */
  ExprList *pOrderBy;   /* The ORDER BY clause */
  int nOrderBy;         /* Number of terms in the ORDER BY clause */
  u32 *aPermute;        /* Mapping from ORDER BY terms to result set columns */

  assert( p->pOrderBy!=0 );
  assert( pKeyDup==0 ); /* "Managed" code needs this.  Ticket #3382. */
  db = pParse->db;
  v = pParse->pVdbe;
  assert( v!=0 );       /* Already thrown the error if VDBE alloc failed */
  labelEnd = sqlite3VdbeMakeLabel(pParse);
  labelCmpr = sqlite3VdbeMakeLabel(pParse);


  /* Patch up the ORDER BY clause
  */
  op = p->op;  
  assert( p->pPrior->pOrderBy==0 );
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
        assert( pItem!=0 );
        assert( pItem->u.x.iOrderByCol>0 );
        if( pItem->u.x.iOrderByCol==i ) break;
      }
      if( j==nOrderBy ){
        Expr *pNew = sqlite3Expr(db, TK_INTEGER, 0);
        if( pNew==0 ) return SQLITE_NOMEM_BKPT;
        pNew->flags |= EP_IntValue;
        pNew->u.iValue = i;
        p->pOrderBy = pOrderBy = sqlite3ExprListAppend(pParse, pOrderBy, pNew);
        if( pOrderBy ) pOrderBy->a[nOrderBy++].u.x.iOrderByCol = (u16)i;
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
  aPermute = sqlite3DbMallocRawNN(db, sizeof(u32)*(nOrderBy + 1));
  if( aPermute ){
    struct ExprList_item *pItem;
    aPermute[0] = nOrderBy;
    for(i=1, pItem=pOrderBy->a; i<=nOrderBy; i++, pItem++){
      assert( pItem!=0 );
      assert( pItem->u.x.iOrderByCol>0 );
      assert( pItem->u.x.iOrderByCol<=p->pEList->nExpr );
      aPermute[i] = pItem->u.x.iOrderByCol - 1;
    }
    pKeyMerge = multiSelectOrderByKeyInfo(pParse, p, 1);
  }else{
    pKeyMerge = 0;
  }

  /* Allocate a range of temporary registers and the KeyInfo needed
  ** for the logic that removes duplicate result rows when the
  ** operator is UNION, EXCEPT, or INTERSECT (but not UNION ALL).
  */
  if( op==TK_ALL ){
    regPrev = 0;
  }else{
    int nExpr = p->pEList->nExpr;
    assert( nOrderBy>=nExpr || db->mallocFailed );
    regPrev = pParse->nMem+1;
    pParse->nMem += nExpr+1;
    sqlite3VdbeAddOp2(v, OP_Integer, 0, regPrev);
    pKeyDup = sqlite3KeyInfoAlloc(db, nExpr, 1);
    if( pKeyDup ){
      assert( sqlite3KeyInfoIsWriteable(pKeyDup) );
      for(i=0; i<nExpr; i++){
        pKeyDup->aColl[i] = multiSelectCollSeq(pParse, p, i);
        pKeyDup->aSortFlags[i] = 0;
      }
    }
  }
 
  /* Separate the left and the right query from one another
  */
  nSelect = 1;
  if( (op==TK_ALL || op==TK_UNION)
   && OptimizationEnabled(db, SQLITE_BalancedMerge)
  ){
    for(pSplit=p; pSplit->pPrior!=0 && pSplit->op==op; pSplit=pSplit->pPrior){
      nSelect++;
      assert( pSplit->pPrior->pNext==pSplit );
    }
  }
  if( nSelect<=3 ){
    pSplit = p;
  }else{
    pSplit = p;
    for(i=2; i<nSelect; i+=2){ pSplit = pSplit->pPrior; }
  }
  pPrior = pSplit->pPrior;
  assert( pPrior!=0 );
  pSplit->pPrior = 0;
  pPrior->pNext = 0;
  assert( p->pOrderBy == pOrderBy );
  assert( pOrderBy!=0 || db->mallocFailed );
  pPrior->pOrderBy = sqlite3ExprListDup(pParse->db, pOrderBy, 0);
  sqlite3ResolveOrderGroupBy(pParse, p, p->pOrderBy, "ORDER");
  sqlite3ResolveOrderGroupBy(pParse, pPrior, pPrior->pOrderBy, "ORDER");

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

  regAddrA = ++pParse->nMem;
  regAddrB = ++pParse->nMem;
  regOutA = ++pParse->nMem;
  regOutB = ++pParse->nMem;
  sqlite3SelectDestInit(&destA, SRT_Coroutine, regAddrA);
  sqlite3SelectDestInit(&destB, SRT_Coroutine, regAddrB);

  ExplainQueryPlan((pParse, 1, "MERGE (%s)", sqlite3SelectOpName(p->op)));

  /* Generate a coroutine to evaluate the SELECT statement to the
  ** left of the compound operator - the "A" select.
  */
  addrSelectA = sqlite3VdbeCurrentAddr(v) + 1;
  addr1 = sqlite3VdbeAddOp3(v, OP_InitCoroutine, regAddrA, 0, addrSelectA);
  VdbeComment((v, "left SELECT"));
  pPrior->iLimit = regLimitA;
  ExplainQueryPlan((pParse, 1, "LEFT"));
  sqlite3Select(pParse, pPrior, &destA);
  sqlite3VdbeEndCoroutine(v, regAddrA);
  sqlite3VdbeJumpHere(v, addr1);

  /* Generate a coroutine to evaluate the SELECT statement on 
  ** the right - the "B" select
  */
  addrSelectB = sqlite3VdbeCurrentAddr(v) + 1;
  addr1 = sqlite3VdbeAddOp3(v, OP_InitCoroutine, regAddrB, 0, addrSelectB);
  VdbeComment((v, "right SELECT"));
  savedLimit = p->iLimit;
  savedOffset = p->iOffset;
  p->iLimit = regLimitB;
  p->iOffset = 0;  
  ExplainQueryPlan((pParse, 1, "RIGHT"));
  sqlite3Select(pParse, p, &destB);
  p->iLimit = savedLimit;
  p->iOffset = savedOffset;
  sqlite3VdbeEndCoroutine(v, regAddrB);

  /* Generate a subroutine that outputs the current row of the A
  ** select as the next output row of the compound select.
  */
  VdbeNoopComment((v, "Output routine for A"));
  addrOutA = generateOutputSubroutine(pParse,
                 p, &destA, pDest, regOutA,
                 regPrev, pKeyDup, labelEnd);
  
  /* Generate a subroutine that outputs the current row of the B
  ** select as the next output row of the compound select.
  */
  if( op==TK_ALL || op==TK_UNION ){
    VdbeNoopComment((v, "Output routine for B"));
    addrOutB = generateOutputSubroutine(pParse,
                 p, &destB, pDest, regOutB,
                 regPrev, pKeyDup, labelEnd);
  }
  sqlite3KeyInfoUnref(pKeyDup);

  /* Generate a subroutine to run when the results from select A
  ** are exhausted and only data in select B remains.
  */
  if( op==TK_EXCEPT || op==TK_INTERSECT ){
    addrEofA_noB = addrEofA = labelEnd;
  }else{  
    VdbeNoopComment((v, "eof-A subroutine"));
    addrEofA = sqlite3VdbeAddOp2(v, OP_Gosub, regOutB, addrOutB);
    addrEofA_noB = sqlite3VdbeAddOp2(v, OP_Yield, regAddrB, labelEnd);
                                     VdbeCoverage(v);
    sqlite3VdbeGoto(v, addrEofA);
    p->nSelectRow = sqlite3LogEstAdd(p->nSelectRow, pPrior->nSelectRow);
  }

  /* Generate a subroutine to run when the results from select B
  ** are exhausted and only data in select A remains.
  */
  if( op==TK_INTERSECT ){
    addrEofB = addrEofA;
    if( p->nSelectRow > pPrior->nSelectRow ) p->nSelectRow = pPrior->nSelectRow;
  }else{  
    VdbeNoopComment((v, "eof-B subroutine"));
    addrEofB = sqlite3VdbeAddOp2(v, OP_Gosub, regOutA, addrOutA);
    sqlite3VdbeAddOp2(v, OP_Yield, regAddrA, labelEnd); VdbeCoverage(v);
    sqlite3VdbeGoto(v, addrEofB);
  }

  /* Generate code to handle the case of A<B
  */
  VdbeNoopComment((v, "A-lt-B subroutine"));
  addrAltB = sqlite3VdbeAddOp2(v, OP_Gosub, regOutA, addrOutA);
  sqlite3VdbeAddOp2(v, OP_Yield, regAddrA, addrEofA); VdbeCoverage(v);
  sqlite3VdbeGoto(v, labelCmpr);

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
    sqlite3VdbeAddOp2(v, OP_Yield, regAddrA, addrEofA); VdbeCoverage(v);
    sqlite3VdbeGoto(v, labelCmpr);
  }

  /* Generate code to handle the case of A>B
  */
  VdbeNoopComment((v, "A-gt-B subroutine"));
  addrAgtB = sqlite3VdbeCurrentAddr(v);
  if( op==TK_ALL || op==TK_UNION ){
    sqlite3VdbeAddOp2(v, OP_Gosub, regOutB, addrOutB);
  }
  sqlite3VdbeAddOp2(v, OP_Yield, regAddrB, addrEofB); VdbeCoverage(v);
  sqlite3VdbeGoto(v, labelCmpr);

  /* This code runs once to initialize everything.
  */
  sqlite3VdbeJumpHere(v, addr1);
  sqlite3VdbeAddOp2(v, OP_Yield, regAddrA, addrEofA_noB); VdbeCoverage(v);
  sqlite3VdbeAddOp2(v, OP_Yield, regAddrB, addrEofB); VdbeCoverage(v);

  /* Implement the main merge loop
  */
  sqlite3VdbeResolveLabel(v, labelCmpr);
  sqlite3VdbeAddOp4(v, OP_Permutation, 0, 0, 0, (char*)aPermute, P4_INTARRAY);
  sqlite3VdbeAddOp4(v, OP_Compare, destA.iSdst, destB.iSdst, nOrderBy,
                         (char*)pKeyMerge, P4_KEYINFO);
  sqlite3VdbeChangeP5(v, OPFLAG_PERMUTE);
  sqlite3VdbeAddOp3(v, OP_Jump, addrAltB, addrAeqB, addrAgtB); VdbeCoverage(v);

  /* Jump to the this point in order to terminate the query.
  */
  sqlite3VdbeResolveLabel(v, labelEnd);

  /* Make arrangements to free the 2nd and subsequent arms of the compound
  ** after the parse has finished */
  if( pSplit->pPrior ){
    sqlite3ParserAddCleanup(pParse, 
       (void(*)(sqlite3*,void*))sqlite3SelectDelete, pSplit->pPrior);
  }
  pSplit->pPrior = pPrior;
  pPrior->pNext = pSplit;
  sqlite3ExprListDelete(db, pPrior->pOrderBy);
  pPrior->pOrderBy = 0;

  /*** TBD:  Insert subroutine calls to close cursors on incomplete
  **** subqueries ****/
  ExplainQueryPlanPop(pParse);
  return pParse->nErr!=0;
}
#endif

#if !defined(SQLITE_OMIT_SUBQUERY) || !defined(SQLITE_OMIT_VIEW)

/* An instance of the SubstContext object describes an substitution edit
** to be performed on a parse tree.
**
** All references to columns in table iTable are to be replaced by corresponding
** expressions in pEList.
**
** ## About "isOuterJoin":
**
** The isOuterJoin column indicates that the replacement will occur into a
** position in the parent that NULL-able due to an OUTER JOIN.  Either the
** target slot in the parent is the right operand of a LEFT JOIN, or one of
** the left operands of a RIGHT JOIN.  In either case, we need to potentially
** bypass the substituted expression with OP_IfNullRow.
**
** Suppose the original expression is an integer constant. Even though the table
** has the nullRow flag set, because the expression is an integer constant,
** it will not be NULLed out.  So instead, we insert an OP_IfNullRow opcode
** that checks to see if the nullRow flag is set on the table.  If the nullRow
** flag is set, then the value in the register is set to NULL and the original
** expression is bypassed.  If the nullRow flag is not set, then the original
** expression runs to populate the register.
**
** Example where this is needed:
**
**      CREATE TABLE t1(a INTEGER PRIMARY KEY, b INT);
**      CREATE TABLE t2(x INT UNIQUE);
**
**      SELECT a,b,m,x FROM t1 LEFT JOIN (SELECT 59 AS m,x FROM t2) ON b=x;
**
** When the subquery on the right side of the LEFT JOIN is flattened, we
** have to add OP_IfNullRow in front of the OP_Integer that implements the
** "m" value of the subquery so that a NULL will be loaded instead of 59
** when processing a non-matched row of the left.
*/
typedef struct SubstContext {
  Parse *pParse;            /* The parsing context */
  int iTable;               /* Replace references to this table */
  int iNewTable;            /* New table number */
  int isOuterJoin;          /* Add TK_IF_NULL_ROW opcodes on each replacement */
  ExprList *pEList;         /* Replacement expressions */
  ExprList *pCList;         /* Collation sequences for replacement expr */
} SubstContext;

/* Forward Declarations */
static void substExprList(SubstContext*, ExprList*);
static void substSelect(SubstContext*, Select*, int);

/*
** Scan through the expression pExpr.  Replace every reference to
** a column in table number iTable with a copy of the iColumn-th
** entry in pEList.  (But leave references to the ROWID column 
** unchanged.)
**
** This routine is part of the flattening procedure.  A subquery
** whose result set is defined by pEList appears as entry in the
** FROM clause of a SELECT such that the VDBE cursor assigned to that
** FORM clause entry is iTable.  This routine makes the necessary 
** changes to pExpr so that it refers directly to the source table
** of the subquery rather the result set of the subquery.
*/
static Expr *substExpr(
  SubstContext *pSubst,  /* Description of the substitution */
  Expr *pExpr            /* Expr in which substitution occurs */
){
  if( pExpr==0 ) return 0;
  if( ExprHasProperty(pExpr, EP_OuterON|EP_InnerON)
   && pExpr->w.iJoin==pSubst->iTable
  ){
    testcase( ExprHasProperty(pExpr, EP_InnerON) );
    pExpr->w.iJoin = pSubst->iNewTable;
  }
  if( pExpr->op==TK_COLUMN
   && pExpr->iTable==pSubst->iTable
   && !ExprHasProperty(pExpr, EP_FixedCol)
  ){
#ifdef SQLITE_ALLOW_ROWID_IN_VIEW
    if( pExpr->iColumn<0 ){
      pExpr->op = TK_NULL;
    }else
#endif
    {
      Expr *pNew;
      int iColumn = pExpr->iColumn;
      Expr *pCopy = pSubst->pEList->a[iColumn].pExpr;
      Expr ifNullRow;
      assert( pSubst->pEList!=0 && iColumn<pSubst->pEList->nExpr );
      assert( pExpr->pRight==0 );
      if( sqlite3ExprIsVector(pCopy) ){
        sqlite3VectorErrorMsg(pSubst->pParse, pCopy);
      }else{
        sqlite3 *db = pSubst->pParse->db;
        if( pSubst->isOuterJoin && pCopy->op!=TK_COLUMN ){
          memset(&ifNullRow, 0, sizeof(ifNullRow));
          ifNullRow.op = TK_IF_NULL_ROW;
          ifNullRow.pLeft = pCopy;
          ifNullRow.iTable = pSubst->iNewTable;
          ifNullRow.iColumn = -99;
          ifNullRow.flags = EP_IfNullRow;
          pCopy = &ifNullRow;
        }
        testcase( ExprHasProperty(pCopy, EP_Subquery) );
        pNew = sqlite3ExprDup(db, pCopy, 0);
        if( db->mallocFailed ){
          sqlite3ExprDelete(db, pNew);
          return pExpr;
        }
        if( pSubst->isOuterJoin ){
          ExprSetProperty(pNew, EP_CanBeNull);
        }
        if( ExprHasProperty(pExpr,EP_OuterON|EP_InnerON) ){
          sqlite3SetJoinExpr(pNew, pExpr->w.iJoin,
                             pExpr->flags & (EP_OuterON|EP_InnerON));
        }
        sqlite3ExprDelete(db, pExpr);
        pExpr = pNew;
        if( pExpr->op==TK_TRUEFALSE ){
          pExpr->u.iValue = sqlite3ExprTruthValue(pExpr);
          pExpr->op = TK_INTEGER;
          ExprSetProperty(pExpr, EP_IntValue);
        }

        /* Ensure that the expression now has an implicit collation sequence,
        ** just as it did when it was a column of a view or sub-query. */
        {
          CollSeq *pNat = sqlite3ExprCollSeq(pSubst->pParse, pExpr);
          CollSeq *pColl = sqlite3ExprCollSeq(pSubst->pParse,
                pSubst->pCList->a[iColumn].pExpr
          );
          if( pNat!=pColl || (pExpr->op!=TK_COLUMN && pExpr->op!=TK_COLLATE) ){
            pExpr = sqlite3ExprAddCollateString(pSubst->pParse, pExpr,
                (pColl ? pColl->zName : "BINARY")
            );
          }
        }
        ExprClearProperty(pExpr, EP_Collate);
      }
    }
  }else{
    if( pExpr->op==TK_IF_NULL_ROW && pExpr->iTable==pSubst->iTable ){
      pExpr->iTable = pSubst->iNewTable;
    }
    pExpr->pLeft = substExpr(pSubst, pExpr->pLeft);
    pExpr->pRight = substExpr(pSubst, pExpr->pRight);
    if( ExprUseXSelect(pExpr) ){
      substSelect(pSubst, pExpr->x.pSelect, 1);
    }else{
      substExprList(pSubst, pExpr->x.pList);
    }
#ifndef SQLITE_OMIT_WINDOWFUNC
    if( ExprHasProperty(pExpr, EP_WinFunc) ){
      Window *pWin = pExpr->y.pWin;
      pWin->pFilter = substExpr(pSubst, pWin->pFilter);
      substExprList(pSubst, pWin->pPartition);
      substExprList(pSubst, pWin->pOrderBy);
    }
#endif
  }
  return pExpr;
}
static void substExprList(
  SubstContext *pSubst, /* Description of the substitution */
  ExprList *pList       /* List to scan and in which to make substitutes */
){
  int i;
  if( pList==0 ) return;
  for(i=0; i<pList->nExpr; i++){
    pList->a[i].pExpr = substExpr(pSubst, pList->a[i].pExpr);
  }
}
static void substSelect(
  SubstContext *pSubst, /* Description of the substitution */
  Select *p,            /* SELECT statement in which to make substitutions */
  int doPrior           /* Do substitutes on p->pPrior too */
){
  SrcList *pSrc;
  SrcItem *pItem;
  int i;
  if( !p ) return;
  do{
    substExprList(pSubst, p->pEList);
    substExprList(pSubst, p->pGroupBy);
    substExprList(pSubst, p->pOrderBy);
    p->pHaving = substExpr(pSubst, p->pHaving);
    p->pWhere = substExpr(pSubst, p->pWhere);
    pSrc = p->pSrc;
    assert( pSrc!=0 );
    for(i=pSrc->nSrc, pItem=pSrc->a; i>0; i--, pItem++){
      substSelect(pSubst, pItem->pSelect, 1);
      if( pItem->fg.isTabFunc ){
        substExprList(pSubst, pItem->u1.pFuncArg);
      }
    }
  }while( doPrior && (p = p->pPrior)!=0 );
}
#endif /* !defined(SQLITE_OMIT_SUBQUERY) || !defined(SQLITE_OMIT_VIEW) */

#if !defined(SQLITE_OMIT_SUBQUERY) || !defined(SQLITE_OMIT_VIEW)
/*
** pSelect is a SELECT statement and pSrcItem is one item in the FROM
** clause of that SELECT.
**
** This routine scans the entire SELECT statement and recomputes the
** pSrcItem->colUsed mask.
*/
static int recomputeColumnsUsedExpr(Walker *pWalker, Expr *pExpr){
  SrcItem *pItem;
  if( pExpr->op!=TK_COLUMN ) return WRC_Continue;
  pItem = pWalker->u.pSrcItem;
  if( pItem->iCursor!=pExpr->iTable ) return WRC_Continue;
  if( pExpr->iColumn<0 ) return WRC_Continue;
  pItem->colUsed |= sqlite3ExprColUsed(pExpr);
  return WRC_Continue;
}
static void recomputeColumnsUsed(
  Select *pSelect,                 /* The complete SELECT statement */
  SrcItem *pSrcItem                /* Which FROM clause item to recompute */
){
  Walker w;
  if( NEVER(pSrcItem->pTab==0) ) return;
  memset(&w, 0, sizeof(w));
  w.xExprCallback = recomputeColumnsUsedExpr;
  w.xSelectCallback = sqlite3SelectWalkNoop;
  w.u.pSrcItem = pSrcItem;
  pSrcItem->colUsed = 0;
  sqlite3WalkSelect(&w, pSelect);
}
#endif /* !defined(SQLITE_OMIT_SUBQUERY) || !defined(SQLITE_OMIT_VIEW) */

#if !defined(SQLITE_OMIT_SUBQUERY) || !defined(SQLITE_OMIT_VIEW)
/*
** Assign new cursor numbers to each of the items in pSrc. For each
** new cursor number assigned, set an entry in the aCsrMap[] array 
** to map the old cursor number to the new:
**
**     aCsrMap[iOld+1] = iNew;
**
** The array is guaranteed by the caller to be large enough for all
** existing cursor numbers in pSrc.  aCsrMap[0] is the array size.
**
** If pSrc contains any sub-selects, call this routine recursively
** on the FROM clause of each such sub-select, with iExcept set to -1.
*/
static void srclistRenumberCursors(
  Parse *pParse,                  /* Parse context */
  int *aCsrMap,                   /* Array to store cursor mappings in */
  SrcList *pSrc,                  /* FROM clause to renumber */
  int iExcept                     /* FROM clause item to skip */
){
  int i;
  SrcItem *pItem;
  for(i=0, pItem=pSrc->a; i<pSrc->nSrc; i++, pItem++){
    if( i!=iExcept ){
      Select *p;
      assert( pItem->iCursor < aCsrMap[0] );
      if( !pItem->fg.isRecursive || aCsrMap[pItem->iCursor+1]==0 ){
        aCsrMap[pItem->iCursor+1] = pParse->nTab++;
      }
      pItem->iCursor = aCsrMap[pItem->iCursor+1];
      for(p=pItem->pSelect; p; p=p->pPrior){
        srclistRenumberCursors(pParse, aCsrMap, p->pSrc, -1);
      }
    }
  }
}

/*
** *piCursor is a cursor number.  Change it if it needs to be mapped.
*/
static void renumberCursorDoMapping(Walker *pWalker, int *piCursor){
  int *aCsrMap = pWalker->u.aiCol;
  int iCsr = *piCursor;
  if( iCsr < aCsrMap[0] && aCsrMap[iCsr+1]>0 ){
    *piCursor = aCsrMap[iCsr+1];
  }
}

/*
** Expression walker callback used by renumberCursors() to update
** Expr objects to match newly assigned cursor numbers.
*/
static int renumberCursorsCb(Walker *pWalker, Expr *pExpr){
  int op = pExpr->op;
  if( op==TK_COLUMN || op==TK_IF_NULL_ROW ){
    renumberCursorDoMapping(pWalker, &pExpr->iTable);
  }
  if( ExprHasProperty(pExpr, EP_OuterON) ){
    renumberCursorDoMapping(pWalker, &pExpr->w.iJoin);
  }
  return WRC_Continue;
}

/*
** Assign a new cursor number to each cursor in the FROM clause (Select.pSrc)
** of the SELECT statement passed as the second argument, and to each 
** cursor in the FROM clause of any FROM clause sub-selects, recursively.
** Except, do not assign a new cursor number to the iExcept'th element in
** the FROM clause of (*p). Update all expressions and other references 
** to refer to the new cursor numbers.
**
** Argument aCsrMap is an array that may be used for temporary working
** space. Two guarantees are made by the caller:
**
**   * the array is larger than the largest cursor number used within the
**     select statement passed as an argument, and
**
**   * the array entries for all cursor numbers that do *not* appear in 
**     FROM clauses of the select statement as described above are 
**     initialized to zero.
*/
static void renumberCursors(
  Parse *pParse,                  /* Parse context */
  Select *p,                      /* Select to renumber cursors within */
  int iExcept,                    /* FROM clause item to skip */
  int *aCsrMap                    /* Working space */
){
  Walker w;
  srclistRenumberCursors(pParse, aCsrMap, p->pSrc, iExcept);
  memset(&w, 0, sizeof(w));
  w.u.aiCol = aCsrMap;
  w.xExprCallback = renumberCursorsCb;
  w.xSelectCallback = sqlite3SelectWalkNoop;
  sqlite3WalkSelect(&w, p);
}
#endif /* !defined(SQLITE_OMIT_SUBQUERY) || !defined(SQLITE_OMIT_VIEW) */

/*
** If pSel is not part of a compound SELECT, return a pointer to its
** expression list. Otherwise, return a pointer to the expression list
** of the leftmost SELECT in the compound.
*/
static ExprList *findLeftmostExprlist(Select *pSel){
  while( pSel->pPrior ){
    pSel = pSel->pPrior;
  }
  return pSel->pEList;
}

#if !defined(SQLITE_OMIT_SUBQUERY) || !defined(SQLITE_OMIT_VIEW)
/*
** This routine attempts to flatten subqueries as a performance optimization.
** This routine returns 1 if it makes changes and 0 if no flattening occurs.
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
** The code generated for this simplification gives the same result
** but only has to scan the data once.  And because indices might 
** exist on the table t1, a complete scan of the data might be
** avoided.
**
** Flattening is subject to the following constraints:
**
**  (**)  We no longer attempt to flatten aggregate subqueries. Was:
**        The subquery and the outer query cannot both be aggregates.
**
**  (**)  We no longer attempt to flatten aggregate subqueries. Was:
**        (2) If the subquery is an aggregate then
**        (2a) the outer query must not be a join and
**        (2b) the outer query must not use subqueries
**             other than the one FROM-clause subquery that is a candidate
**             for flattening.  (This is due to ticket [2f7170d73bf9abf80]
**             from 2015-02-09.)
**
**   (3)  If the subquery is the right operand of a LEFT JOIN then
**        (3a) the subquery may not be a join and
**        (3b) the FROM clause of the subquery may not contain a virtual
**             table and
**        (**) Was: "The outer query may not have a GROUP BY." This case
**             is now managed correctly
**        (3d) the outer query may not be DISTINCT.
**        See also (26) for restrictions on RIGHT JOIN.
**
**   (4)  The subquery can not be DISTINCT.
**
**  (**)  At one point restrictions (4) and (5) defined a subset of DISTINCT
**        sub-queries that were excluded from this optimization. Restriction 
**        (4) has since been expanded to exclude all DISTINCT subqueries.
**
**  (**)  We no longer attempt to flatten aggregate subqueries.  Was:
**        If the subquery is aggregate, the outer query may not be DISTINCT.
**
**   (7)  The subquery must have a FROM clause.  TODO:  For subqueries without
**        A FROM clause, consider adding a FROM clause with the special
**        table sqlite_once that consists of a single row containing a
**        single NULL.
**
**   (8)  If the subquery uses LIMIT then the outer query may not be a join.
**
**   (9)  If the subquery uses LIMIT then the outer query may not be aggregate.
**
**  (**)  Restriction (10) was removed from the code on 2005-02-05 but we
**        accidently carried the comment forward until 2014-09-15.  Original
**        constraint: "If the subquery is aggregate then the outer query 
**        may not use LIMIT."
**
**  (11)  The subquery and the outer query may not both have ORDER BY clauses.
**
**  (**)  Not implemented.  Subsumed into restriction (3).  Was previously
**        a separate restriction deriving from ticket #350.
**
**  (13)  The subquery and outer query may not both use LIMIT.
**
**  (14)  The subquery may not use OFFSET.
**
**  (15)  If the outer query is part of a compound select, then the
**        subquery may not use LIMIT.
**        (See ticket #2339 and ticket [02a8e81d44]).
**
**  (16)  If the outer query is aggregate, then the subquery may not
**        use ORDER BY.  (Ticket #2942)  This used to not matter
**        until we introduced the group_concat() function.  
**
**  (17)  If the subquery is a compound select, then
**        (17a) all compound operators must be a UNION ALL, and
**        (17b) no terms within the subquery compound may be aggregate
**              or DISTINCT, and
**        (17c) every term within the subquery compound must have a FROM clause
**        (17d) the outer query may not be
**              (17d1) aggregate, or
**              (17d2) DISTINCT
**        (17e) the subquery may not contain window functions, and
**        (17f) the subquery must not be the RHS of a LEFT JOIN.
**        (17g) either the subquery is the first element of the outer
**              query or there are no RIGHT or FULL JOINs in any arm
**              of the subquery.  (This is a duplicate of condition (27b).)
**        (17h) The corresponding result set expressions in all arms of the
**              compound must have the same affinity.
**
**        The parent and sub-query may contain WHERE clauses. Subject to
**        rules (11), (13) and (14), they may also contain ORDER BY,
**        LIMIT and OFFSET clauses.  The subquery cannot use any compound
**        operator other than UNION ALL because all the other compound
**        operators have an implied DISTINCT which is disallowed by
**        restriction (4).
**
**        Also, each component of the sub-query must return the same number
**        of result columns. This is actually a requirement for any compound
**        SELECT statement, but all the code here does is make sure that no
**        such (illegal) sub-query is flattened. The caller will detect the
**        syntax error and return a detailed message.
**
**  (18)  If the sub-query is a compound select, then all terms of the
**        ORDER BY clause of the parent must be copies of a term returned
**        by the parent query.
**
**  (19)  If the subquery uses LIMIT then the outer query may not
**        have a WHERE clause.
**
**  (20)  If the sub-query is a compound select, then it must not use
**        an ORDER BY clause.  Ticket #3773.  We could relax this constraint
**        somewhat by saying that the terms of the ORDER BY clause must
**        appear as unmodified result columns in the outer query.  But we
**        have other optimizations in mind to deal with that case.
**
**  (21)  If the subquery uses LIMIT then the outer query may not be
**        DISTINCT.  (See ticket [752e1646fc]).
**
**  (22)  The subquery may not be a recursive CTE.
**
**  (23)  If the outer query is a recursive CTE, then the sub-query may not be
**        a compound query.  This restriction is because transforming the
**        parent to a compound query confuses the code that handles
**        recursive queries in multiSelect().
**
**  (**)  We no longer attempt to flatten aggregate subqueries.  Was:
**        The subquery may not be an aggregate that uses the built-in min() or 
**        or max() functions.  (Without this restriction, a query like:
**        "SELECT x FROM (SELECT max(y), x FROM t1)" would not necessarily
**        return the value X for which Y was maximal.)
**
**  (25)  If either the subquery or the parent query contains a window
**        function in the select list or ORDER BY clause, flattening
**        is not attempted.
**
**  (26)  The subquery may not be the right operand of a RIGHT JOIN.
**        See also (3) for restrictions on LEFT JOIN.
**
**  (27)  The subquery may not contain a FULL or RIGHT JOIN unless it
**        is the first element of the parent query.  Two subcases:
**        (27a) the subquery is not a compound query.
**        (27b) the subquery is a compound query and the RIGHT JOIN occurs
**              in any arm of the compound query.  (See also (17g).)
**
**  (28)  The subquery is not a MATERIALIZED CTE.
**
**
** In this routine, the "p" parameter is a pointer to the outer query.
** The subquery is p->pSrc->a[iFrom].  isAgg is true if the outer query
** uses aggregates.
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
  int isAgg            /* True if outer SELECT uses aggregate functions */
){
  const char *zSavedAuthContext = pParse->zAuthContext;
  Select *pParent;    /* Current UNION ALL term of the other query */
  Select *pSub;       /* The inner query or "subquery" */
  Select *pSub1;      /* Pointer to the rightmost select in sub-query */
  SrcList *pSrc;      /* The FROM clause of the outer query */
  SrcList *pSubSrc;   /* The FROM clause of the subquery */
  int iParent;        /* VDBE cursor number of the pSub result set temp table */
  int iNewParent = -1;/* Replacement table for iParent */
  int isOuterJoin = 0; /* True if pSub is the right side of a LEFT JOIN */    
  int i;              /* Loop counter */
  Expr *pWhere;                    /* The WHERE clause */
  SrcItem *pSubitem;               /* The subquery */
  sqlite3 *db = pParse->db;
  Walker w;                        /* Walker to persist agginfo data */
  int *aCsrMap = 0;

  /* Check to see if flattening is permitted.  Return 0 if not.
  */
  assert( p!=0 );
  assert( p->pPrior==0 );
  if( OptimizationDisabled(db, SQLITE_QueryFlattener) ) return 0;
  pSrc = p->pSrc;
  assert( pSrc && iFrom>=0 && iFrom<pSrc->nSrc );
  pSubitem = &pSrc->a[iFrom];
  iParent = pSubitem->iCursor;
  pSub = pSubitem->pSelect;
  assert( pSub!=0 );

#ifndef SQLITE_OMIT_WINDOWFUNC
  if( p->pWin || pSub->pWin ) return 0;                  /* Restriction (25) */
#endif

  pSubSrc = pSub->pSrc;
  assert( pSubSrc );
  /* Prior to version 3.1.2, when LIMIT and OFFSET had to be simple constants,
  ** not arbitrary expressions, we allowed some combining of LIMIT and OFFSET
  ** because they could be computed at compile-time.  But when LIMIT and OFFSET
  ** became arbitrary expressions, we were forced to add restrictions (13)
  ** and (14). */
  if( pSub->pLimit && p->pLimit ) return 0;              /* Restriction (13) */
  if( pSub->pLimit && pSub->pLimit->pRight ) return 0;   /* Restriction (14) */
  if( (p->selFlags & SF_Compound)!=0 && pSub->pLimit ){
    return 0;                                            /* Restriction (15) */
  }
  if( pSubSrc->nSrc==0 ) return 0;                       /* Restriction (7)  */
  if( pSub->selFlags & SF_Distinct ) return 0;           /* Restriction (4)  */
  if( pSub->pLimit && (pSrc->nSrc>1 || isAgg) ){
     return 0;         /* Restrictions (8)(9) */
  }
  if( p->pOrderBy && pSub->pOrderBy ){
     return 0;                                           /* Restriction (11) */
  }
  if( isAgg && pSub->pOrderBy ) return 0;                /* Restriction (16) */
  if( pSub->pLimit && p->pWhere ) return 0;              /* Restriction (19) */
  if( pSub->pLimit && (p->selFlags & SF_Distinct)!=0 ){
     return 0;         /* Restriction (21) */
  }
  if( pSub->selFlags & (SF_Recursive) ){
    return 0; /* Restrictions (22) */
  }

  /*
  ** If the subquery is the right operand of a LEFT JOIN, then the
  ** subquery may not be a join itself (3a). Example of why this is not
  ** allowed:
  **
  **         t1 LEFT OUTER JOIN (t2 JOIN t3)
  **
  ** If we flatten the above, we would get
  **
  **         (t1 LEFT OUTER JOIN t2) JOIN t3
  **
  ** which is not at all the same thing.
  **
  ** See also tickets #306, #350, and #3300.
  */
  if( (pSubitem->fg.jointype & (JT_OUTER|JT_LTORJ))!=0 ){
    if( pSubSrc->nSrc>1                        /* (3a) */
     || IsVirtual(pSubSrc->a[0].pTab)          /* (3b) */
     || (p->selFlags & SF_Distinct)!=0         /* (3d) */
     || (pSubitem->fg.jointype & JT_RIGHT)!=0  /* (26) */
    ){
      return 0;
    }
    isOuterJoin = 1;
  }

  assert( pSubSrc->nSrc>0 );  /* True by restriction (7) */
  if( iFrom>0 && (pSubSrc->a[0].fg.jointype & JT_LTORJ)!=0 ){
    return 0;   /* Restriction (27a) */
  }
  if( pSubitem->fg.isCte && pSubitem->u2.pCteUse->eM10d==M10d_Yes ){
    return 0;       /* (28) */
  }

  /* Restriction (17): If the sub-query is a compound SELECT, then it must
  ** use only the UNION ALL operator. And none of the simple select queries
  ** that make up the compound SELECT are allowed to be aggregate or distinct
  ** queries.
  */
  if( pSub->pPrior ){
    int ii;
    if( pSub->pOrderBy ){
      return 0;  /* Restriction (20) */
    }
    if( isAgg || (p->selFlags & SF_Distinct)!=0 || isOuterJoin>0 ){
      return 0; /* (17d1), (17d2), or (17f) */
    }
    for(pSub1=pSub; pSub1; pSub1=pSub1->pPrior){
      testcase( (pSub1->selFlags & (SF_Distinct|SF_Aggregate))==SF_Distinct );
      testcase( (pSub1->selFlags & (SF_Distinct|SF_Aggregate))==SF_Aggregate );
      assert( pSub->pSrc!=0 );
      assert( (pSub->selFlags & SF_Recursive)==0 );
      assert( pSub->pEList->nExpr==pSub1->pEList->nExpr );
      if( (pSub1->selFlags & (SF_Distinct|SF_Aggregate))!=0    /* (17b) */
       || (pSub1->pPrior && pSub1->op!=TK_ALL)                 /* (17a) */
       || pSub1->pSrc->nSrc<1                                  /* (17c) */
#ifndef SQLITE_OMIT_WINDOWFUNC
       || pSub1->pWin                                          /* (17e) */
#endif
      ){
        return 0;
      }
      if( iFrom>0 && (pSub1->pSrc->a[0].fg.jointype & JT_LTORJ)!=0 ){
        /* Without this restriction, the JT_LTORJ flag would end up being
        ** omitted on left-hand tables of the right join that is being
        ** flattened. */
        return 0;   /* Restrictions (17g), (27b) */
      }
      testcase( pSub1->pSrc->nSrc>1 );
    }

    /* Restriction (18). */
    if( p->pOrderBy ){
      for(ii=0; ii<p->pOrderBy->nExpr; ii++){
        if( p->pOrderBy->a[ii].u.x.iOrderByCol==0 ) return 0;
      }
    }

    /* Restriction (23) */
    if( (p->selFlags & SF_Recursive) ) return 0;

    /* Restriction (17h) */
    for(ii=0; ii<pSub->pEList->nExpr; ii++){
      char aff;
      assert( pSub->pEList->a[ii].pExpr!=0 );
      aff = sqlite3ExprAffinity(pSub->pEList->a[ii].pExpr);
      for(pSub1=pSub->pPrior; pSub1; pSub1=pSub1->pPrior){
        assert( pSub1->pEList!=0 );
        assert( pSub1->pEList->nExpr>ii );
        assert( pSub1->pEList->a[ii].pExpr!=0 );
        if( sqlite3ExprAffinity(pSub1->pEList->a[ii].pExpr)!=aff ){
          return 0;
        }
      }
    }

    if( pSrc->nSrc>1 ){
      if( pParse->nSelect>500 ) return 0;
      if( OptimizationDisabled(db, SQLITE_FlttnUnionAll) ) return 0;
      aCsrMap = sqlite3DbMallocZero(db, ((i64)pParse->nTab+1)*sizeof(int));
      if( aCsrMap ) aCsrMap[0] = pParse->nTab;
    }
  }

  /***** If we reach this point, flattening is permitted. *****/
  SELECTTRACE(1,pParse,p,("flatten %u.%p from term %d\n",
                   pSub->selId, pSub, iFrom));

  /* Authorize the subquery */
  pParse->zAuthContext = pSubitem->zName;
  TESTONLY(i =) sqlite3AuthCheck(pParse, SQLITE_SELECT, 0, 0, 0);
  testcase( i==SQLITE_DENY );
  pParse->zAuthContext = zSavedAuthContext;

  /* Delete the transient structures associated with thesubquery */
  pSub1 = pSubitem->pSelect;
  sqlite3DbFree(db, pSubitem->zDatabase);
  sqlite3DbFree(db, pSubitem->zName);
  sqlite3DbFree(db, pSubitem->zAlias);
  pSubitem->zDatabase = 0;
  pSubitem->zName = 0;
  pSubitem->zAlias = 0;
  pSubitem->pSelect = 0;
  assert( pSubitem->fg.isUsing!=0 || pSubitem->u3.pOn==0 );

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
    Table *pItemTab = pSubitem->pTab;
    pSubitem->pTab = 0;
    p->pOrderBy = 0;
    p->pPrior = 0;
    p->pLimit = 0;
    pNew = sqlite3SelectDup(db, p, 0);
    p->pLimit = pLimit;
    p->pOrderBy = pOrderBy;
    p->op = TK_ALL;
    pSubitem->pTab = pItemTab;
    if( pNew==0 ){
      p->pPrior = pPrior;
    }else{
      pNew->selId = ++pParse->nSelect;
      if( aCsrMap && ALWAYS(db->mallocFailed==0) ){
        renumberCursors(pParse, pNew, iFrom, aCsrMap);
      }
      pNew->pPrior = pPrior;
      if( pPrior ) pPrior->pNext = pNew;
      pNew->pNext = p;
      p->pPrior = pNew;
      SELECTTRACE(2,pParse,p,("compound-subquery flattener"
                              " creates %u as peer\n",pNew->selId));
    }
    assert( pSubitem->pSelect==0 );
  }
  sqlite3DbFree(db, aCsrMap);
  if( db->mallocFailed ){
    pSubitem->pSelect = pSub1;
    return 1;
  }

  /* Defer deleting the Table object associated with the
  ** subquery until code generation is
  ** complete, since there may still exist Expr.pTab entries that
  ** refer to the subquery even after flattening.  Ticket #3346.
  **
  ** pSubitem->pTab is always non-NULL by test restrictions and tests above.
  */
  if( ALWAYS(pSubitem->pTab!=0) ){
    Table *pTabToDel = pSubitem->pTab;
    if( pTabToDel->nTabRef==1 ){
      Parse *pToplevel = sqlite3ParseToplevel(pParse);
      sqlite3ParserAddCleanup(pToplevel, 
         (void(*)(sqlite3*,void*))sqlite3DeleteTable,
         pTabToDel);
      testcase( pToplevel->earlyCleanup );
    }else{
      pTabToDel->nTabRef--;
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
  pSub = pSub1;
  for(pParent=p; pParent; pParent=pParent->pPrior, pSub=pSub->pPrior){
    int nSubSrc;
    u8 jointype = 0;
    u8 ltorj = pSrc->a[iFrom].fg.jointype & JT_LTORJ;
    assert( pSub!=0 );
    pSubSrc = pSub->pSrc;     /* FROM clause of subquery */
    nSubSrc = pSubSrc->nSrc;  /* Number of terms in subquery FROM clause */
    pSrc = pParent->pSrc;     /* FROM clause of the outer query */

    if( pParent==p ){
      jointype = pSubitem->fg.jointype;     /* First time through the loop */
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
    ** block of code will expand the outer query FROM clause to 4 slots.
    ** The middle slot is expanded to two slots in order to make space
    ** for the two elements in the FROM clause of the subquery.
    */
    if( nSubSrc>1 ){
      pSrc = sqlite3SrcListEnlarge(pParse, pSrc, nSubSrc-1,iFrom+1);
      if( pSrc==0 ) break;
      pParent->pSrc = pSrc;
    }

    /* Transfer the FROM clause terms from the subquery into the
    ** outer query.
    */
    for(i=0; i<nSubSrc; i++){
      SrcItem *pItem = &pSrc->a[i+iFrom];
      if( pItem->fg.isUsing ) sqlite3IdListDelete(db, pItem->u3.pUsing);
      assert( pItem->fg.isTabFunc==0 );
      *pItem = pSubSrc->a[i];
      pItem->fg.jointype |= ltorj;
      iNewParent = pSubSrc->a[i].iCursor;
      memset(&pSubSrc->a[i], 0, sizeof(pSubSrc->a[i]));
    }
    pSrc->a[iFrom].fg.jointype &= JT_LTORJ;
    pSrc->a[iFrom].fg.jointype |= jointype | ltorj;
  
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
    if( pSub->pOrderBy && (pParent->selFlags & SF_NoopOrderBy)==0 ){
      /* At this point, any non-zero iOrderByCol values indicate that the
      ** ORDER BY column expression is identical to the iOrderByCol'th
      ** expression returned by SELECT statement pSub. Since these values
      ** do not necessarily correspond to columns in SELECT statement pParent,
      ** zero them before transfering the ORDER BY clause.
      **
      ** Not doing this may cause an error if a subsequent call to this
      ** function attempts to flatten a compound sub-query into pParent
      ** (the only way this can happen is if the compound sub-query is
      ** currently part of pSub->pSrc). See ticket [d11a6e908f].  */
      ExprList *pOrderBy = pSub->pOrderBy;
      for(i=0; i<pOrderBy->nExpr; i++){
        pOrderBy->a[i].u.x.iOrderByCol = 0;
      }
      assert( pParent->pOrderBy==0 );
      pParent->pOrderBy = pOrderBy;
      pSub->pOrderBy = 0;
    }
    pWhere = pSub->pWhere;
    pSub->pWhere = 0;
    if( isOuterJoin>0 ){
      sqlite3SetJoinExpr(pWhere, iNewParent, EP_OuterON);
    }
    if( pWhere ){
      if( pParent->pWhere ){
        pParent->pWhere = sqlite3PExpr(pParse, TK_AND, pWhere, pParent->pWhere);
      }else{
        pParent->pWhere = pWhere;
      }
    }
    if( db->mallocFailed==0 ){
      SubstContext x;
      x.pParse = pParse;
      x.iTable = iParent;
      x.iNewTable = iNewParent;
      x.isOuterJoin = isOuterJoin;
      x.pEList = pSub->pEList;
      x.pCList = findLeftmostExprlist(pSub);
      substSelect(&x, pParent, 0);
    }
  
    /* The flattened query is a compound if either the inner or the
    ** outer query is a compound. */
    pParent->selFlags |= pSub->selFlags & SF_Compound;
    assert( (pSub->selFlags & SF_Distinct)==0 ); /* restriction (17b) */
  
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

    /* Recompute the SrcItem.colUsed masks for the flattened
    ** tables. */
    for(i=0; i<nSubSrc; i++){
      recomputeColumnsUsed(pParent, &pSrc->a[i+iFrom]);
    }
  }

  /* Finially, delete what is left of the subquery and return
  ** success.
  */
  sqlite3AggInfoPersistWalkerInit(&w, pParse);
  sqlite3WalkSelect(&w,pSub1);
  sqlite3SelectDelete(db, pSub1);

#if TREETRACE_ENABLED
  if( sqlite3TreeTrace & 0x100 ){
    SELECTTRACE(0x100,pParse,p,("After flattening:\n"));
    sqlite3TreeViewSelect(0, p, 0);
  }
#endif

  return 1;
}
#endif /* !defined(SQLITE_OMIT_SUBQUERY) || !defined(SQLITE_OMIT_VIEW) */

/*
** A structure to keep track of all of the column values that are fixed to
** a known value due to WHERE clause constraints of the form COLUMN=VALUE.
*/
typedef struct WhereConst WhereConst;
struct WhereConst {
  Parse *pParse;   /* Parsing context */
  u8 *pOomFault;   /* Pointer to pParse->db->mallocFailed */
  int nConst;      /* Number for COLUMN=CONSTANT terms */
  int nChng;       /* Number of times a constant is propagated */
  int bHasAffBlob; /* At least one column in apExpr[] as affinity BLOB */
  u32 mExcludeOn;  /* Which ON expressions to exclude from considertion.
                   ** Either EP_OuterON or EP_InnerON|EP_OuterON */
  Expr **apExpr;   /* [i*2] is COLUMN and [i*2+1] is VALUE */
};

/*
** Add a new entry to the pConst object.  Except, do not add duplicate
** pColumn entires.  Also, do not add if doing so would not be appropriate.
**
** The caller guarantees the pColumn is a column and pValue is a constant.
** This routine has to do some additional checks before completing the
** insert.
*/
static void constInsert(
  WhereConst *pConst,  /* The WhereConst into which we are inserting */
  Expr *pColumn,       /* The COLUMN part of the constraint */
  Expr *pValue,        /* The VALUE part of the constraint */
  Expr *pExpr          /* Overall expression: COLUMN=VALUE or VALUE=COLUMN */
){
  int i;
  assert( pColumn->op==TK_COLUMN );
  assert( sqlite3ExprIsConstant(pValue) );

  if( ExprHasProperty(pColumn, EP_FixedCol) ) return;
  if( sqlite3ExprAffinity(pValue)!=0 ) return;
  if( !sqlite3IsBinary(sqlite3ExprCompareCollSeq(pConst->pParse,pExpr)) ){
    return;
  }

  /* 2018-10-25 ticket [cf5ed20f]
  ** Make sure the same pColumn is not inserted more than once */
  for(i=0; i<pConst->nConst; i++){
    const Expr *pE2 = pConst->apExpr[i*2];
    assert( pE2->op==TK_COLUMN );
    if( pE2->iTable==pColumn->iTable
     && pE2->iColumn==pColumn->iColumn
    ){
      return;  /* Already present.  Return without doing anything. */
    }
  }
  if( sqlite3ExprAffinity(pColumn)==SQLITE_AFF_BLOB ){
    pConst->bHasAffBlob = 1;
  }

  pConst->nConst++;
  pConst->apExpr = sqlite3DbReallocOrFree(pConst->pParse->db, pConst->apExpr,
                         pConst->nConst*2*sizeof(Expr*));
  if( pConst->apExpr==0 ){
    pConst->nConst = 0;
  }else{
    pConst->apExpr[pConst->nConst*2-2] = pColumn;
    pConst->apExpr[pConst->nConst*2-1] = pValue;
  }
}

/*
** Find all terms of COLUMN=VALUE or VALUE=COLUMN in pExpr where VALUE
** is a constant expression and where the term must be true because it
** is part of the AND-connected terms of the expression.  For each term
** found, add it to the pConst structure.
*/
static void findConstInWhere(WhereConst *pConst, Expr *pExpr){
  Expr *pRight, *pLeft;
  if( NEVER(pExpr==0) ) return;
  if( ExprHasProperty(pExpr, pConst->mExcludeOn) ){
    testcase( ExprHasProperty(pExpr, EP_OuterON) );
    testcase( ExprHasProperty(pExpr, EP_InnerON) );
    return;
  }
  if( pExpr->op==TK_AND ){
    findConstInWhere(pConst, pExpr->pRight);
    findConstInWhere(pConst, pExpr->pLeft);
    return;
  }
  if( pExpr->op!=TK_EQ ) return;
  pRight = pExpr->pRight;
  pLeft = pExpr->pLeft;
  assert( pRight!=0 );
  assert( pLeft!=0 );
  if( pRight->op==TK_COLUMN && sqlite3ExprIsConstant(pLeft) ){
    constInsert(pConst,pRight,pLeft,pExpr);
  }
  if( pLeft->op==TK_COLUMN && sqlite3ExprIsConstant(pRight) ){
    constInsert(pConst,pLeft,pRight,pExpr);
  }
}

/*
** This is a helper function for Walker callback propagateConstantExprRewrite().
**
** Argument pExpr is a candidate expression to be replaced by a value. If 
** pExpr is equivalent to one of the columns named in pWalker->u.pConst, 
** then overwrite it with the corresponding value. Except, do not do so
** if argument bIgnoreAffBlob is non-zero and the affinity of pExpr
** is SQLITE_AFF_BLOB.
*/
static int propagateConstantExprRewriteOne(
  WhereConst *pConst,
  Expr *pExpr, 
  int bIgnoreAffBlob
){
  int i;
  if( pConst->pOomFault[0] ) return WRC_Prune;
  if( pExpr->op!=TK_COLUMN ) return WRC_Continue;
  if( ExprHasProperty(pExpr, EP_FixedCol|pConst->mExcludeOn) ){
    testcase( ExprHasProperty(pExpr, EP_FixedCol) );
    testcase( ExprHasProperty(pExpr, EP_OuterON) );
    testcase( ExprHasProperty(pExpr, EP_InnerON) );
    return WRC_Continue;
  }
  for(i=0; i<pConst->nConst; i++){
    Expr *pColumn = pConst->apExpr[i*2];
    if( pColumn==pExpr ) continue;
    if( pColumn->iTable!=pExpr->iTable ) continue;
    if( pColumn->iColumn!=pExpr->iColumn ) continue;
    if( bIgnoreAffBlob && sqlite3ExprAffinity(pColumn)==SQLITE_AFF_BLOB ){
      break;
    }
    /* A match is found.  Add the EP_FixedCol property */
    pConst->nChng++;
    ExprClearProperty(pExpr, EP_Leaf);
    ExprSetProperty(pExpr, EP_FixedCol);
    assert( pExpr->pLeft==0 );
    pExpr->pLeft = sqlite3ExprDup(pConst->pParse->db, pConst->apExpr[i*2+1], 0);
    if( pConst->pParse->db->mallocFailed ) return WRC_Prune;
    break;
  }
  return WRC_Prune;
}

/*
** This is a Walker expression callback. pExpr is a node from the WHERE
** clause of a SELECT statement. This function examines pExpr to see if
** any substitutions based on the contents of pWalker->u.pConst should
** be made to pExpr or its immediate children.
**
** A substitution is made if:
**
**   + pExpr is a column with an affinity other than BLOB that matches
**     one of the columns in pWalker->u.pConst, or
**
**   + pExpr is a binary comparison operator (=, <=, >=, <, >) that
**     uses an affinity other than TEXT and one of its immediate
**     children is a column that matches one of the columns in 
**     pWalker->u.pConst.
*/
static int propagateConstantExprRewrite(Walker *pWalker, Expr *pExpr){
  WhereConst *pConst = pWalker->u.pConst;
  assert( TK_GT==TK_EQ+1 );
  assert( TK_LE==TK_EQ+2 );
  assert( TK_LT==TK_EQ+3 );
  assert( TK_GE==TK_EQ+4 );
  if( pConst->bHasAffBlob ){
    if( (pExpr->op>=TK_EQ && pExpr->op<=TK_GE)
     || pExpr->op==TK_IS
    ){
      propagateConstantExprRewriteOne(pConst, pExpr->pLeft, 0);
      if( pConst->pOomFault[0] ) return WRC_Prune;
      if( sqlite3ExprAffinity(pExpr->pLeft)!=SQLITE_AFF_TEXT ){
        propagateConstantExprRewriteOne(pConst, pExpr->pRight, 0);
      }
    }
  }
  return propagateConstantExprRewriteOne(pConst, pExpr, pConst->bHasAffBlob);
}

/*
** The WHERE-clause constant propagation optimization.
**
** If the WHERE clause contains terms of the form COLUMN=CONSTANT or
** CONSTANT=COLUMN that are top-level AND-connected terms that are not
** part of a ON clause from a LEFT JOIN, then throughout the query
** replace all other occurrences of COLUMN with CONSTANT.
**
** For example, the query:
**
**      SELECT * FROM t1, t2, t3 WHERE t1.a=39 AND t2.b=t1.a AND t3.c=t2.b
**
** Is transformed into
**
**      SELECT * FROM t1, t2, t3 WHERE t1.a=39 AND t2.b=39 AND t3.c=39
**
** Return true if any transformations where made and false if not.
**
** Implementation note:  Constant propagation is tricky due to affinity
** and collating sequence interactions.  Consider this example:
**
**    CREATE TABLE t1(a INT,b TEXT);
**    INSERT INTO t1 VALUES(123,'0123');
**    SELECT * FROM t1 WHERE a=123 AND b=a;
**    SELECT * FROM t1 WHERE a=123 AND b=123;
**
** The two SELECT statements above should return different answers.  b=a
** is alway true because the comparison uses numeric affinity, but b=123
** is false because it uses text affinity and '0123' is not the same as '123'.
** To work around this, the expression tree is not actually changed from
** "b=a" to "b=123" but rather the "a" in "b=a" is tagged with EP_FixedCol
** and the "123" value is hung off of the pLeft pointer.  Code generator
** routines know to generate the constant "123" instead of looking up the
** column value.  Also, to avoid collation problems, this optimization is
** only attempted if the "a=123" term uses the default BINARY collation.
**
** 2021-05-25 forum post 6a06202608: Another troublesome case is...
**
**    CREATE TABLE t1(x);
**    INSERT INTO t1 VALUES(10.0);
**    SELECT 1 FROM t1 WHERE x=10 AND x LIKE 10;
**
** The query should return no rows, because the t1.x value is '10.0' not '10'
** and '10.0' is not LIKE '10'.  But if we are not careful, the first WHERE
** term "x=10" will cause the second WHERE term to become "10 LIKE 10",
** resulting in a false positive.  To avoid this, constant propagation for
** columns with BLOB affinity is only allowed if the constant is used with
** operators ==, <=, <, >=, >, or IS in a way that will cause the correct
** type conversions to occur.  See logic associated with the bHasAffBlob flag
** for details.
*/
static int propagateConstants(
  Parse *pParse,   /* The parsing context */
  Select *p        /* The query in which to propagate constants */
){
  WhereConst x;
  Walker w;
  int nChng = 0;
  x.pParse = pParse;
  x.pOomFault = &pParse->db->mallocFailed;
  do{
    x.nConst = 0;
    x.nChng = 0;
    x.apExpr = 0;
    x.bHasAffBlob = 0;
    if( ALWAYS(p->pSrc!=0)
     && p->pSrc->nSrc>0
     && (p->pSrc->a[0].fg.jointype & JT_LTORJ)!=0
    ){
      /* Do not propagate constants on any ON clause if there is a
      ** RIGHT JOIN anywhere in the query */
      x.mExcludeOn = EP_InnerON | EP_OuterON;
    }else{
      /* Do not propagate constants through the ON clause of a LEFT JOIN */
      x.mExcludeOn = EP_OuterON;
    }
    findConstInWhere(&x, p->pWhere);
    if( x.nConst ){
      memset(&w, 0, sizeof(w));
      w.pParse = pParse;
      w.xExprCallback = propagateConstantExprRewrite;
      w.xSelectCallback = sqlite3SelectWalkNoop;
      w.xSelectCallback2 = 0;
      w.walkerDepth = 0;
      w.u.pConst = &x;
      sqlite3WalkExpr(&w, p->pWhere);
      sqlite3DbFree(x.pParse->db, x.apExpr);
      nChng += x.nChng;
    }
  }while( x.nChng );  
  return nChng;
}

#if !defined(SQLITE_OMIT_SUBQUERY) || !defined(SQLITE_OMIT_VIEW)
# if !defined(SQLITE_OMIT_WINDOWFUNC)
/*
** This function is called to determine whether or not it is safe to
** push WHERE clause expression pExpr down to FROM clause sub-query
** pSubq, which contains at least one window function. Return 1
** if it is safe and the expression should be pushed down, or 0 
** otherwise.
**
** It is only safe to push the expression down if it consists only 
** of constants and copies of expressions that appear in the PARTITION
** BY clause of all window function used by the sub-query. It is safe
** to filter out entire partitions, but not rows within partitions, as
** this may change the results of the window functions.
**
** At the time this function is called it is guaranteed that 
**
**   * the sub-query uses only one distinct window frame, and 
**   * that the window frame has a PARTITION BY clase.
*/
static int pushDownWindowCheck(Parse *pParse, Select *pSubq, Expr *pExpr){
  assert( pSubq->pWin->pPartition );
  assert( (pSubq->selFlags & SF_MultiPart)==0 );
  assert( pSubq->pPrior==0 );
  return sqlite3ExprIsConstantOrGroupBy(pParse, pExpr, pSubq->pWin->pPartition);
}
# endif /* SQLITE_OMIT_WINDOWFUNC */
#endif /* !defined(SQLITE_OMIT_SUBQUERY) || !defined(SQLITE_OMIT_VIEW) */

#if !defined(SQLITE_OMIT_SUBQUERY) || !defined(SQLITE_OMIT_VIEW)
/*
** Make copies of relevant WHERE clause terms of the outer query into
** the WHERE clause of subquery.  Example:
**
**    SELECT * FROM (SELECT a AS x, c-d AS y FROM t1) WHERE x=5 AND y=10;
**
** Transformed into:
**
**    SELECT * FROM (SELECT a AS x, c-d AS y FROM t1 WHERE a=5 AND c-d=10)
**     WHERE x=5 AND y=10;
**
** The hope is that the terms added to the inner query will make it more
** efficient.
**
** Do not attempt this optimization if:
**
**   (1) (** This restriction was removed on 2017-09-29.  We used to
**           disallow this optimization for aggregate subqueries, but now
**           it is allowed by putting the extra terms on the HAVING clause.
**           The added HAVING clause is pointless if the subquery lacks
**           a GROUP BY clause.  But such a HAVING clause is also harmless
**           so there does not appear to be any reason to add extra logic
**           to suppress it. **)
**
**   (2) The inner query is the recursive part of a common table expression.
**
**   (3) The inner query has a LIMIT clause (since the changes to the WHERE
**       clause would change the meaning of the LIMIT).
**
**   (4) The inner query is the right operand of a LEFT JOIN and the
**       expression to be pushed down does not come from the ON clause
**       on that LEFT JOIN.
**
**   (5) The WHERE clause expression originates in the ON or USING clause
**       of a LEFT JOIN where iCursor is not the right-hand table of that
**       left join.  An example:
**
**           SELECT *
**           FROM (SELECT 1 AS a1 UNION ALL SELECT 2) AS aa
**           JOIN (SELECT 1 AS b2 UNION ALL SELECT 2) AS bb ON (a1=b2)
**           LEFT JOIN (SELECT 8 AS c3 UNION ALL SELECT 9) AS cc ON (b2=2);
**
**       The correct answer is three rows:  (1,1,NULL),(2,2,8),(2,2,9).
**       But if the (b2=2) term were to be pushed down into the bb subquery,
**       then the (1,1,NULL) row would be suppressed.
**
**   (6) Window functions make things tricky as changes to the WHERE clause 
**       of the inner query could change the window over which window 
**       functions are calculated. Therefore, do not attempt the optimization
**       if:
**
**     (6a) The inner query uses multiple incompatible window partitions.
**
**     (6b) The inner query is a compound and uses window-functions. 
**
**     (6c) The WHERE clause does not consist entirely of constants and
**          copies of expressions found in the PARTITION BY clause of
**          all window-functions used by the sub-query. It is safe to
**          filter out entire partitions, as this does not change the 
**          window over which any window-function is calculated.
**
**   (7) The inner query is a Common Table Expression (CTE) that should
**       be materialized.  (This restriction is implemented in the calling
**       routine.)
**
**   (8) The subquery may not be a compound that uses UNION, INTERSECT,
**       or EXCEPT.  (We could, perhaps, relax this restriction to allow
**       this case if none of the comparisons operators between left and
**       right arms of the compound use a collation other than BINARY.
**       But it is a lot of work to check that case for an obscure and
**       minor optimization, so we omit it for now.)
**
** Return 0 if no changes are made and non-zero if one or more WHERE clause
** terms are duplicated into the subquery.
*/
static int pushDownWhereTerms(
  Parse *pParse,        /* Parse context (for malloc() and error reporting) */
  Select *pSubq,        /* The subquery whose WHERE clause is to be augmented */
  Expr *pWhere,         /* The WHERE clause of the outer query */
  SrcItem *pSrc         /* The subquery term of the outer FROM clause */
){
  Expr *pNew;
  int nChng = 0;
  if( pWhere==0 ) return 0;
  if( pSubq->selFlags & (SF_Recursive|SF_MultiPart) ) return 0;
  if( pSrc->fg.jointype & (JT_LTORJ|JT_RIGHT) ) return 0;

#ifndef SQLITE_OMIT_WINDOWFUNC
  if( pSubq->pPrior ){
    Select *pSel;
    for(pSel=pSubq; pSel; pSel=pSel->pPrior){
      u8 op = pSel->op;
      assert( op==TK_ALL || op==TK_SELECT 
           || op==TK_UNION || op==TK_INTERSECT || op==TK_EXCEPT );
      if( op!=TK_ALL && op!=TK_SELECT ) return 0;  /* restriction (8) */
      if( pSel->pWin ) return 0;    /* restriction (6b) */
    }
  }else{
    if( pSubq->pWin && pSubq->pWin->pPartition==0 ) return 0;
  }
#endif

#ifdef SQLITE_DEBUG
  /* Only the first term of a compound can have a WITH clause.  But make
  ** sure no other terms are marked SF_Recursive in case something changes
  ** in the future.
  */
  {
    Select *pX;  
    for(pX=pSubq; pX; pX=pX->pPrior){
      assert( (pX->selFlags & (SF_Recursive))==0 );
    }
  }
#endif

  if( pSubq->pLimit!=0 ){
    return 0; /* restriction (3) */
  }
  while( pWhere->op==TK_AND ){
    nChng += pushDownWhereTerms(pParse, pSubq, pWhere->pRight, pSrc);
    pWhere = pWhere->pLeft;
  }

#if 0  /* Legacy code. Checks now done by sqlite3ExprIsTableConstraint() */
  if( isLeftJoin
   && (ExprHasProperty(pWhere,EP_OuterON)==0
         || pWhere->w.iJoin!=iCursor)
  ){
    return 0; /* restriction (4) */
  }
  if( ExprHasProperty(pWhere,EP_OuterON)
   && pWhere->w.iJoin!=iCursor 
  ){
    return 0; /* restriction (5) */
  }
#endif

  if( sqlite3ExprIsTableConstraint(pWhere, pSrc) ){
    nChng++;
    pSubq->selFlags |= SF_PushDown;
    while( pSubq ){
      SubstContext x;
      pNew = sqlite3ExprDup(pParse->db, pWhere, 0);
      unsetJoinExpr(pNew, -1, 1);
      x.pParse = pParse;
      x.iTable = pSrc->iCursor;
      x.iNewTable = pSrc->iCursor;
      x.isOuterJoin = 0;
      x.pEList = pSubq->pEList;
      x.pCList = findLeftmostExprlist(pSubq);
      pNew = substExpr(&x, pNew);
#ifndef SQLITE_OMIT_WINDOWFUNC
      if( pSubq->pWin && 0==pushDownWindowCheck(pParse, pSubq, pNew) ){
        /* Restriction 6c has prevented push-down in this case */
        sqlite3ExprDelete(pParse->db, pNew);
        nChng--;
        break;
      }
#endif
      if( pSubq->selFlags & SF_Aggregate ){
        pSubq->pHaving = sqlite3ExprAnd(pParse, pSubq->pHaving, pNew);
      }else{
        pSubq->pWhere = sqlite3ExprAnd(pParse, pSubq->pWhere, pNew);
      }
      pSubq = pSubq->pPrior;
    }
  }
  return nChng;
}
#endif /* !defined(SQLITE_OMIT_SUBQUERY) || !defined(SQLITE_OMIT_VIEW) */

/*
** The pFunc is the only aggregate function in the query.  Check to see
** if the query is a candidate for the min/max optimization. 
**
** If the query is a candidate for the min/max optimization, then set
** *ppMinMax to be an ORDER BY clause to be used for the optimization
** and return either WHERE_ORDERBY_MIN or WHERE_ORDERBY_MAX depending on
** whether pFunc is a min() or max() function.
**
** If the query is not a candidate for the min/max optimization, return
** WHERE_ORDERBY_NORMAL (which must be zero).
**
** This routine must be called after aggregate functions have been
** located but before their arguments have been subjected to aggregate
** analysis.
*/
static u8 minMaxQuery(sqlite3 *db, Expr *pFunc, ExprList **ppMinMax){
  int eRet = WHERE_ORDERBY_NORMAL;      /* Return value */
  ExprList *pEList;                     /* Arguments to agg function */
  const char *zFunc;                    /* Name of aggregate function pFunc */
  ExprList *pOrderBy;
  u8 sortFlags = 0;

  assert( *ppMinMax==0 );
  assert( pFunc->op==TK_AGG_FUNCTION );
  assert( !IsWindowFunc(pFunc) );
  assert( ExprUseXList(pFunc) );
  pEList = pFunc->x.pList;
  if( pEList==0 
   || pEList->nExpr!=1
   || ExprHasProperty(pFunc, EP_WinFunc)
   || OptimizationDisabled(db, SQLITE_MinMaxOpt)
  ){
    return eRet;
  }
  assert( !ExprHasProperty(pFunc, EP_IntValue) );
  zFunc = pFunc->u.zToken;
  if( sqlite3StrICmp(zFunc, "min")==0 ){
    eRet = WHERE_ORDERBY_MIN;
    if( sqlite3ExprCanBeNull(pEList->a[0].pExpr) ){
      sortFlags = KEYINFO_ORDER_BIGNULL;
    }
  }else if( sqlite3StrICmp(zFunc, "max")==0 ){
    eRet = WHERE_ORDERBY_MAX;
    sortFlags = KEYINFO_ORDER_DESC;
  }else{
    return eRet;
  }
  *ppMinMax = pOrderBy = sqlite3ExprListDup(db, pEList, 0);
  assert( pOrderBy!=0 || db->mallocFailed );
  if( pOrderBy ) pOrderBy->a[0].fg.sortFlags = sortFlags;
  return eRet;
}

/*
** The select statement passed as the first argument is an aggregate query.
** The second argument is the associated aggregate-info object. This 
** function tests if the SELECT is of the form:
**
**   SELECT count(*) FROM <tbl>
**
** where table is a database table, not a sub-select or view. If the query
** does match this pattern, then a pointer to the Table object representing
** <tbl> is returned. Otherwise, NULL is returned.
**
** This routine checks to see if it is safe to use the count optimization.
** A correct answer is still obtained (though perhaps more slowly) if
** this routine returns NULL when it could have returned a table pointer.
** But returning the pointer when NULL should have been returned can
** result in incorrect answers and/or crashes.  So, when in doubt, return NULL.
*/
static Table *isSimpleCount(Select *p, AggInfo *pAggInfo){
  Table *pTab;
  Expr *pExpr;

  assert( !p->pGroupBy );

  if( p->pWhere 
   || p->pEList->nExpr!=1 
   || p->pSrc->nSrc!=1
   || p->pSrc->a[0].pSelect
   || pAggInfo->nFunc!=1
   || p->pHaving
  ){
    return 0;
  }
  pTab = p->pSrc->a[0].pTab;
  assert( pTab!=0 );
  assert( !IsView(pTab) );
  if( !IsOrdinaryTable(pTab) ) return 0;
  pExpr = p->pEList->a[0].pExpr;
  assert( pExpr!=0 );
  if( pExpr->op!=TK_AGG_FUNCTION ) return 0;
  if( pExpr->pAggInfo!=pAggInfo ) return 0;
  if( (pAggInfo->aFunc[0].pFunc->funcFlags&SQLITE_FUNC_COUNT)==0 ) return 0;
  assert( pAggInfo->aFunc[0].pFExpr==pExpr );
  testcase( ExprHasProperty(pExpr, EP_Distinct) );
  testcase( ExprHasProperty(pExpr, EP_WinFunc) );
  if( ExprHasProperty(pExpr, EP_Distinct|EP_WinFunc) ) return 0;

  return pTab;
}

/*
** If the source-list item passed as an argument was augmented with an
** INDEXED BY clause, then try to locate the specified index. If there
** was such a clause and the named index cannot be found, return 
** SQLITE_ERROR and leave an error in pParse. Otherwise, populate 
** pFrom->pIndex and return SQLITE_OK.
*/
int sqlite3IndexedByLookup(Parse *pParse, SrcItem *pFrom){
  Table *pTab = pFrom->pTab;
  char *zIndexedBy = pFrom->u1.zIndexedBy;
  Index *pIdx;
  assert( pTab!=0 );
  assert( pFrom->fg.isIndexedBy!=0 );

  for(pIdx=pTab->pIndex; 
      pIdx && sqlite3StrICmp(pIdx->zName, zIndexedBy); 
      pIdx=pIdx->pNext
  );
  if( !pIdx ){
    sqlite3ErrorMsg(pParse, "no such index: %s", zIndexedBy, 0);
    pParse->checkSchema = 1;
    return SQLITE_ERROR;
  }
  assert( pFrom->fg.isCte==0 );
  pFrom->u2.pIBIndex = pIdx;
  return SQLITE_OK;
}

/*
** Detect compound SELECT statements that use an ORDER BY clause with 
** an alternative collating sequence.
**
**    SELECT ... FROM t1 EXCEPT SELECT ... FROM t2 ORDER BY .. COLLATE ...
**
** These are rewritten as a subquery:
**
**    SELECT * FROM (SELECT ... FROM t1 EXCEPT SELECT ... FROM t2)
**     ORDER BY ... COLLATE ...
**
** This transformation is necessary because the multiSelectOrderBy() routine
** above that generates the code for a compound SELECT with an ORDER BY clause
** uses a merge algorithm that requires the same collating sequence on the
** result columns as on the ORDER BY clause.  See ticket
** http://www.sqlite.org/src/info/6709574d2a
**
** This transformation is only needed for EXCEPT, INTERSECT, and UNION.
** The UNION ALL operator works fine with multiSelectOrderBy() even when
** there are COLLATE terms in the ORDER BY.
*/
static int convertCompoundSelectToSubquery(Walker *pWalker, Select *p){
  int i;
  Select *pNew;
  Select *pX;
  sqlite3 *db;
  struct ExprList_item *a;
  SrcList *pNewSrc;
  Parse *pParse;
  Token dummy;

  if( p->pPrior==0 ) return WRC_Continue;
  if( p->pOrderBy==0 ) return WRC_Continue;
  for(pX=p; pX && (pX->op==TK_ALL || pX->op==TK_SELECT); pX=pX->pPrior){}
  if( pX==0 ) return WRC_Continue;
  a = p->pOrderBy->a;
#ifndef SQLITE_OMIT_WINDOWFUNC
  /* If iOrderByCol is already non-zero, then it has already been matched
  ** to a result column of the SELECT statement. This occurs when the
  ** SELECT is rewritten for window-functions processing and then passed
  ** to sqlite3SelectPrep() and similar a second time. The rewriting done
  ** by this function is not required in this case. */
  if( a[0].u.x.iOrderByCol ) return WRC_Continue;
#endif
  for(i=p->pOrderBy->nExpr-1; i>=0; i--){
    if( a[i].pExpr->flags & EP_Collate ) break;
  }
  if( i<0 ) return WRC_Continue;

  /* If we reach this point, that means the transformation is required. */

  pParse = pWalker->pParse;
  db = pParse->db;
  pNew = sqlite3DbMallocZero(db, sizeof(*pNew) );
  if( pNew==0 ) return WRC_Abort;
  memset(&dummy, 0, sizeof(dummy));
  pNewSrc = sqlite3SrcListAppendFromTerm(pParse,0,0,0,&dummy,pNew,0);
  if( pNewSrc==0 ) return WRC_Abort;
  *pNew = *p;
  p->pSrc = pNewSrc;
  p->pEList = sqlite3ExprListAppend(pParse, 0, sqlite3Expr(db, TK_ASTERISK, 0));
  p->op = TK_SELECT;
  p->pWhere = 0;
  pNew->pGroupBy = 0;
  pNew->pHaving = 0;
  pNew->pOrderBy = 0;
  p->pPrior = 0;
  p->pNext = 0;
  p->pWith = 0;
#ifndef SQLITE_OMIT_WINDOWFUNC
  p->pWinDefn = 0;
#endif
  p->selFlags &= ~SF_Compound;
  assert( (p->selFlags & SF_Converted)==0 );
  p->selFlags |= SF_Converted;
  assert( pNew->pPrior!=0 );
  pNew->pPrior->pNext = pNew;
  pNew->pLimit = 0;
  return WRC_Continue;
}

/*
** Check to see if the FROM clause term pFrom has table-valued function
** arguments.  If it does, leave an error message in pParse and return
** non-zero, since pFrom is not allowed to be a table-valued function.
*/
static int cannotBeFunction(Parse *pParse, SrcItem *pFrom){
  if( pFrom->fg.isTabFunc ){
    sqlite3ErrorMsg(pParse, "'%s' is not a function", pFrom->zName);
    return 1;
  }
  return 0;
}

#ifndef SQLITE_OMIT_CTE
/*
** Argument pWith (which may be NULL) points to a linked list of nested 
** WITH contexts, from inner to outermost. If the table identified by 
** FROM clause element pItem is really a common-table-expression (CTE) 
** then return a pointer to the CTE definition for that table. Otherwise
** return NULL.
**
** If a non-NULL value is returned, set *ppContext to point to the With
** object that the returned CTE belongs to.
*/
static struct Cte *searchWith(
  With *pWith,                    /* Current innermost WITH clause */
  SrcItem *pItem,                 /* FROM clause element to resolve */
  With **ppContext                /* OUT: WITH clause return value belongs to */
){
  const char *zName = pItem->zName;
  With *p;
  assert( pItem->zDatabase==0 );
  assert( zName!=0 );
  for(p=pWith; p; p=p->pOuter){
    int i;
    for(i=0; i<p->nCte; i++){
      if( sqlite3StrICmp(zName, p->a[i].zName)==0 ){
        *ppContext = p;
        return &p->a[i];
      }
    }
    if( p->bView ) break;
  }
  return 0;
}

/* The code generator maintains a stack of active WITH clauses
** with the inner-most WITH clause being at the top of the stack.
**
** This routine pushes the WITH clause passed as the second argument
** onto the top of the stack. If argument bFree is true, then this
** WITH clause will never be popped from the stack but should instead
** be freed along with the Parse object. In other cases, when
** bFree==0, the With object will be freed along with the SELECT 
** statement with which it is associated.
**
** This routine returns a copy of pWith.  Or, if bFree is true and
** the pWith object is destroyed immediately due to an OOM condition,
** then this routine return NULL.
**
** If bFree is true, do not continue to use the pWith pointer after
** calling this routine,  Instead, use only the return value.
*/
With *sqlite3WithPush(Parse *pParse, With *pWith, u8 bFree){
  if( pWith ){
    if( bFree ){
      pWith = (With*)sqlite3ParserAddCleanup(pParse, 
                      (void(*)(sqlite3*,void*))sqlite3WithDelete,
                      pWith);
      if( pWith==0 ) return 0;
    }
    if( pParse->nErr==0 ){
      assert( pParse->pWith!=pWith );
      pWith->pOuter = pParse->pWith;
      pParse->pWith = pWith;
    }
  }
  return pWith;
}

/*
** This function checks if argument pFrom refers to a CTE declared by 
** a WITH clause on the stack currently maintained by the parser (on the
** pParse->pWith linked list).  And if currently processing a CTE
** CTE expression, through routine checks to see if the reference is
** a recursive reference to the CTE.
**
** If pFrom matches a CTE according to either of these two above, pFrom->pTab
** and other fields are populated accordingly.
**
** Return 0 if no match is found. 
** Return 1 if a match is found.
** Return 2 if an error condition is detected.
*/
static int resolveFromTermToCte(
  Parse *pParse,                  /* The parsing context */
  Walker *pWalker,                /* Current tree walker */
  SrcItem *pFrom                  /* The FROM clause term to check */
){
  Cte *pCte;               /* Matched CTE (or NULL if no match) */
  With *pWith;             /* The matching WITH */

  assert( pFrom->pTab==0 );
  if( pParse->pWith==0 ){
    /* There are no WITH clauses in the stack.  No match is possible */
    return 0;
  }
  if( pParse->nErr ){
    /* Prior errors might have left pParse->pWith in a goofy state, so
    ** go no further. */
    return 0;
  }
  if( pFrom->zDatabase!=0 ){
    /* The FROM term contains a schema qualifier (ex: main.t1) and so
    ** it cannot possibly be a CTE reference. */
    return 0;
  }
  if( pFrom->fg.notCte ){
    /* The FROM term is specifically excluded from matching a CTE.
    **   (1)  It is part of a trigger that used to have zDatabase but had
    **        zDatabase removed by sqlite3FixTriggerStep().
    **   (2)  This is the first term in the FROM clause of an UPDATE.
    */
    return 0;
  }
  pCte = searchWith(pParse->pWith, pFrom, &pWith);
  if( pCte ){
    sqlite3 *db = pParse->db;
    Table *pTab;
    ExprList *pEList;
    Select *pSel;
    Select *pLeft;                /* Left-most SELECT statement */
    Select *pRecTerm;             /* Left-most recursive term */
    int bMayRecursive;            /* True if compound joined by UNION [ALL] */
    With *pSavedWith;             /* Initial value of pParse->pWith */
    int iRecTab = -1;             /* Cursor for recursive table */
    CteUse *pCteUse;

    /* If pCte->zCteErr is non-NULL at this point, then this is an illegal
    ** recursive reference to CTE pCte. Leave an error in pParse and return
    ** early. If pCte->zCteErr is NULL, then this is not a recursive reference.
    ** In this case, proceed.  */
    if( pCte->zCteErr ){
      sqlite3ErrorMsg(pParse, pCte->zCteErr, pCte->zName);
      return 2;
    }
    if( cannotBeFunction(pParse, pFrom) ) return 2;

    assert( pFrom->pTab==0 );
    pTab = sqlite3DbMallocZero(db, sizeof(Table));
    if( pTab==0 ) return 2;
    pCteUse = pCte->pUse;
    if( pCteUse==0 ){
      pCte->pUse = pCteUse = sqlite3DbMallocZero(db, sizeof(pCteUse[0]));
      if( pCteUse==0
       || sqlite3ParserAddCleanup(pParse,sqlite3DbFree,pCteUse)==0
      ){
        sqlite3DbFree(db, pTab);
        return 2;
      }
      pCteUse->eM10d = pCte->eM10d;
    }
    pFrom->pTab = pTab;
    pTab->nTabRef = 1;
    pTab->zName = sqlite3DbStrDup(db, pCte->zName);
    pTab->iPKey = -1;
    pTab->nRowLogEst = 200; assert( 200==sqlite3LogEst(1048576) );
    pTab->tabFlags |= TF_Ephemeral | TF_NoVisibleRowid;
    pFrom->pSelect = sqlite3SelectDup(db, pCte->pSelect, 0);
    if( db->mallocFailed ) return 2;
    pFrom->pSelect->selFlags |= SF_CopyCte;
    assert( pFrom->pSelect );
    if( pFrom->fg.isIndexedBy ){
      sqlite3ErrorMsg(pParse, "no such index: \"%s\"", pFrom->u1.zIndexedBy);
      return 2;
    }
    pFrom->fg.isCte = 1;
    pFrom->u2.pCteUse = pCteUse;
    pCteUse->nUse++;
    if( pCteUse->nUse>=2 && pCteUse->eM10d==M10d_Any ){
      pCteUse->eM10d = M10d_Yes;
    }

    /* Check if this is a recursive CTE. */
    pRecTerm = pSel = pFrom->pSelect;
    bMayRecursive = ( pSel->op==TK_ALL || pSel->op==TK_UNION );
    while( bMayRecursive && pRecTerm->op==pSel->op ){
      int i;
      SrcList *pSrc = pRecTerm->pSrc;
      assert( pRecTerm->pPrior!=0 );
      for(i=0; i<pSrc->nSrc; i++){
        SrcItem *pItem = &pSrc->a[i];
        if( pItem->zDatabase==0 
         && pItem->zName!=0 
         && 0==sqlite3StrICmp(pItem->zName, pCte->zName)
        ){
          pItem->pTab = pTab;
          pTab->nTabRef++;
          pItem->fg.isRecursive = 1;
          if( pRecTerm->selFlags & SF_Recursive ){
            sqlite3ErrorMsg(pParse,
               "multiple references to recursive table: %s", pCte->zName
            );
            return 2;
          }
          pRecTerm->selFlags |= SF_Recursive;
          if( iRecTab<0 ) iRecTab = pParse->nTab++;
          pItem->iCursor = iRecTab;
        }
      }
      if( (pRecTerm->selFlags & SF_Recursive)==0 ) break;
      pRecTerm = pRecTerm->pPrior;
    }

    pCte->zCteErr = "circular reference: %s";
    pSavedWith = pParse->pWith;
    pParse->pWith = pWith;
    if( pSel->selFlags & SF_Recursive ){
      int rc;
      assert( pRecTerm!=0 );
      assert( (pRecTerm->selFlags & SF_Recursive)==0 );
      assert( pRecTerm->pNext!=0 );
      assert( (pRecTerm->pNext->selFlags & SF_Recursive)!=0 );
      assert( pRecTerm->pWith==0 );
      pRecTerm->pWith = pSel->pWith;
      rc = sqlite3WalkSelect(pWalker, pRecTerm);
      pRecTerm->pWith = 0;
      if( rc ){
        pParse->pWith = pSavedWith;
        return 2;
      }
    }else{
      if( sqlite3WalkSelect(pWalker, pSel) ){
        pParse->pWith = pSavedWith;
        return 2;
      }
    }
    pParse->pWith = pWith;

    for(pLeft=pSel; pLeft->pPrior; pLeft=pLeft->pPrior);
    pEList = pLeft->pEList;
    if( pCte->pCols ){
      if( pEList && pEList->nExpr!=pCte->pCols->nExpr ){
        sqlite3ErrorMsg(pParse, "table %s has %d values for %d columns",
            pCte->zName, pEList->nExpr, pCte->pCols->nExpr
        );
        pParse->pWith = pSavedWith;
        return 2;
      }
      pEList = pCte->pCols;
    }

    sqlite3ColumnsFromExprList(pParse, pEList, &pTab->nCol, &pTab->aCol);
    if( bMayRecursive ){
      if( pSel->selFlags & SF_Recursive ){
        pCte->zCteErr = "multiple recursive references: %s";
      }else{
        pCte->zCteErr = "recursive reference in a subquery: %s";
      }
      sqlite3WalkSelect(pWalker, pSel);
    }
    pCte->zCteErr = 0;
    pParse->pWith = pSavedWith;
    return 1;  /* Success */
  }
  return 0;  /* No match */
}
#endif

#ifndef SQLITE_OMIT_CTE
/*
** If the SELECT passed as the second argument has an associated WITH 
** clause, pop it from the stack stored as part of the Parse object.
**
** This function is used as the xSelectCallback2() callback by
** sqlite3SelectExpand() when walking a SELECT tree to resolve table
** names and other FROM clause elements. 
*/
void sqlite3SelectPopWith(Walker *pWalker, Select *p){
  Parse *pParse = pWalker->pParse;
  if( OK_IF_ALWAYS_TRUE(pParse->pWith) && p->pPrior==0 ){
    With *pWith = findRightmost(p)->pWith;
    if( pWith!=0 ){
      assert( pParse->pWith==pWith || pParse->nErr );
      pParse->pWith = pWith->pOuter;
    }
  }
}
#endif

/*
** The SrcItem structure passed as the second argument represents a
** sub-query in the FROM clause of a SELECT statement. This function
** allocates and populates the SrcItem.pTab object. If successful,
** SQLITE_OK is returned. Otherwise, if an OOM error is encountered,
** SQLITE_NOMEM.
*/
int sqlite3ExpandSubquery(Parse *pParse, SrcItem *pFrom){
  Select *pSel = pFrom->pSelect;
  Table *pTab;

  assert( pSel );
  pFrom->pTab = pTab = sqlite3DbMallocZero(pParse->db, sizeof(Table));
  if( pTab==0 ) return SQLITE_NOMEM;
  pTab->nTabRef = 1;
  if( pFrom->zAlias ){
    pTab->zName = sqlite3DbStrDup(pParse->db, pFrom->zAlias);
  }else{
    pTab->zName = sqlite3MPrintf(pParse->db, "%!S", pFrom);
  }
  while( pSel->pPrior ){ pSel = pSel->pPrior; }
  sqlite3ColumnsFromExprList(pParse, pSel->pEList,&pTab->nCol,&pTab->aCol);
  pTab->iPKey = -1;
  pTab->nRowLogEst = 200; assert( 200==sqlite3LogEst(1048576) );
#ifndef SQLITE_ALLOW_ROWID_IN_VIEW
  /* The usual case - do not allow ROWID on a subquery */
  pTab->tabFlags |= TF_Ephemeral | TF_NoVisibleRowid;
#else
  pTab->tabFlags |= TF_Ephemeral;  /* Legacy compatibility mode */
#endif
  return pParse->nErr ? SQLITE_ERROR : SQLITE_OK;
}


/*
** Check the N SrcItem objects to the right of pBase.  (N might be zero!)
** If any of those SrcItem objects have a USING clause containing zName
** then return true.
**
** If N is zero, or none of the N SrcItem objects to the right of pBase
** contains a USING clause, or if none of the USING clauses contain zName,
** then return false.
*/
static int inAnyUsingClause(
  const char *zName, /* Name we are looking for */
  SrcItem *pBase,    /* The base SrcItem.  Looking at pBase[1] and following */
  int N              /* How many SrcItems to check */
){
  while( N>0 ){
    N--;
    pBase++;
    if( pBase->fg.isUsing==0 ) continue;
    if( NEVER(pBase->u3.pUsing==0) ) continue;
    if( sqlite3IdListIndex(pBase->u3.pUsing, zName)>=0 ) return 1;
  }
  return 0;
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
**         without worrying about messing up the persistent representation
**         of the view.
**
**    (3)  Add terms to the WHERE clause to accommodate the NATURAL keyword
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
  int i, j, k, rc;
  SrcList *pTabList;
  ExprList *pEList;
  SrcItem *pFrom;
  sqlite3 *db = pParse->db;
  Expr *pE, *pRight, *pExpr;
  u16 selFlags = p->selFlags;
  u32 elistFlags = 0;

  p->selFlags |= SF_Expanded;
  if( db->mallocFailed  ){
    return WRC_Abort;
  }
  assert( p->pSrc!=0 );
  if( (selFlags & SF_Expanded)!=0 ){
    return WRC_Prune;
  }
  if( pWalker->eCode ){
    /* Renumber selId because it has been copied from a view */
    p->selId = ++pParse->nSelect;
  }
  pTabList = p->pSrc;
  pEList = p->pEList;
  if( pParse->pWith && (p->selFlags & SF_View) ){
    if( p->pWith==0 ){
      p->pWith = (With*)sqlite3DbMallocZero(db, sizeof(With));
      if( p->pWith==0 ){
        return WRC_Abort;
      }
    }
    p->pWith->bView = 1;
  }
  sqlite3WithPush(pParse, p->pWith, 0);

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
    assert( pFrom->fg.isRecursive==0 || pFrom->pTab!=0 );
    if( pFrom->pTab ) continue;
    assert( pFrom->fg.isRecursive==0 );
    if( pFrom->zName==0 ){
#ifndef SQLITE_OMIT_SUBQUERY
      Select *pSel = pFrom->pSelect;
      /* A sub-query in the FROM clause of a SELECT */
      assert( pSel!=0 );
      assert( pFrom->pTab==0 );
      if( sqlite3WalkSelect(pWalker, pSel) ) return WRC_Abort;
      if( sqlite3ExpandSubquery(pParse, pFrom) ) return WRC_Abort;
#endif
#ifndef SQLITE_OMIT_CTE
    }else if( (rc = resolveFromTermToCte(pParse, pWalker, pFrom))!=0 ){
      if( rc>1 ) return WRC_Abort;
      pTab = pFrom->pTab;
      assert( pTab!=0 );
#endif
    }else{
      /* An ordinary table or view name in the FROM clause */
      assert( pFrom->pTab==0 );
      pFrom->pTab = pTab = sqlite3LocateTableItem(pParse, 0, pFrom);
      if( pTab==0 ) return WRC_Abort;
      if( pTab->nTabRef>=0xffff ){
        sqlite3ErrorMsg(pParse, "too many references to \"%s\": max 65535",
           pTab->zName);
        pFrom->pTab = 0;
        return WRC_Abort;
      }
      pTab->nTabRef++;
      if( !IsVirtual(pTab) && cannotBeFunction(pParse, pFrom) ){
        return WRC_Abort;
      }
#if !defined(SQLITE_OMIT_VIEW) || !defined(SQLITE_OMIT_VIRTUALTABLE)
      if( !IsOrdinaryTable(pTab) ){
        i16 nCol;
        u8 eCodeOrig = pWalker->eCode;
        if( sqlite3ViewGetColumnNames(pParse, pTab) ) return WRC_Abort;
        assert( pFrom->pSelect==0 );
        if( IsView(pTab) ){
          if( (db->flags & SQLITE_EnableView)==0
           && pTab->pSchema!=db->aDb[1].pSchema
          ){
            sqlite3ErrorMsg(pParse, "access to view \"%s\" prohibited",
              pTab->zName);
          }
          pFrom->pSelect = sqlite3SelectDup(db, pTab->u.view.pSelect, 0);
        }
#ifndef SQLITE_OMIT_VIRTUALTABLE
        else if( ALWAYS(IsVirtual(pTab))
         && pFrom->fg.fromDDL
         && ALWAYS(pTab->u.vtab.p!=0)
         && pTab->u.vtab.p->eVtabRisk > ((db->flags & SQLITE_TrustedSchema)!=0)
        ){
          sqlite3ErrorMsg(pParse, "unsafe use of virtual table \"%s\"",
                                  pTab->zName);
        }
        assert( SQLITE_VTABRISK_Normal==1 && SQLITE_VTABRISK_High==2 );
#endif
        nCol = pTab->nCol;
        pTab->nCol = -1;
        pWalker->eCode = 1;  /* Turn on Select.selId renumbering */
        sqlite3WalkSelect(pWalker, pFrom->pSelect);
        pWalker->eCode = eCodeOrig;
        pTab->nCol = nCol;
      }
#endif
    }

    /* Locate the index named by the INDEXED BY clause, if any. */
    if( pFrom->fg.isIndexedBy && sqlite3IndexedByLookup(pParse, pFrom) ){
      return WRC_Abort;
    }
  }

  /* Process NATURAL keywords, and ON and USING clauses of joins.
  */
  assert( db->mallocFailed==0 || pParse->nErr!=0 );
  if( pParse->nErr || sqlite3ProcessJoin(pParse, p) ){
    return WRC_Abort;
  }

  /* For every "*" that occurs in the column list, insert the names of
  ** all columns in all tables.  And for every TABLE.* insert the names
  ** of all columns in TABLE.  The parser inserted a special expression
  ** with the TK_ASTERISK operator for each "*" that it found in the column
  ** list.  The following code just has to locate the TK_ASTERISK
  ** expressions and expand each one to the list of all columns in
  ** all tables.
  **
  ** The first loop just checks to see if there are any "*" operators
  ** that need expanding.
  */
  for(k=0; k<pEList->nExpr; k++){
    pE = pEList->a[k].pExpr;
    if( pE->op==TK_ASTERISK ) break;
    assert( pE->op!=TK_DOT || pE->pRight!=0 );
    assert( pE->op!=TK_DOT || (pE->pLeft!=0 && pE->pLeft->op==TK_ID) );
    if( pE->op==TK_DOT && pE->pRight->op==TK_ASTERISK ) break;
    elistFlags |= pE->flags;
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
      pE = a[k].pExpr;
      elistFlags |= pE->flags;
      pRight = pE->pRight;
      assert( pE->op!=TK_DOT || pRight!=0 );
      if( pE->op!=TK_ASTERISK
       && (pE->op!=TK_DOT || pRight->op!=TK_ASTERISK)
      ){
        /* This particular expression does not need to be expanded.
        */
        pNew = sqlite3ExprListAppend(pParse, pNew, a[k].pExpr);
        if( pNew ){
          pNew->a[pNew->nExpr-1].zEName = a[k].zEName;
          pNew->a[pNew->nExpr-1].fg.eEName = a[k].fg.eEName;
          a[k].zEName = 0;
        }
        a[k].pExpr = 0;
      }else{
        /* This expression is a "*" or a "TABLE.*" and needs to be
        ** expanded. */
        int tableSeen = 0;      /* Set to 1 when TABLE matches */
        char *zTName = 0;       /* text of name of TABLE */
        if( pE->op==TK_DOT ){
          assert( pE->pLeft!=0 );
          assert( !ExprHasProperty(pE->pLeft, EP_IntValue) );
          zTName = pE->pLeft->u.zToken;
        }
        for(i=0, pFrom=pTabList->a; i<pTabList->nSrc; i++, pFrom++){
          Table *pTab = pFrom->pTab;   /* Table for this data source */
          ExprList *pNestedFrom;       /* Result-set of a nested FROM clause */
          char *zTabName;              /* AS name for this data source */
          const char *zSchemaName = 0; /* Schema name for this data source */
          int iDb;                     /* Schema index for this data src */
          IdList *pUsing;              /* USING clause for pFrom[1] */

          if( (zTabName = pFrom->zAlias)==0 ){
            zTabName = pTab->zName;
          }
          if( db->mallocFailed ) break;
          assert( (int)pFrom->fg.isNestedFrom == IsNestedFrom(pFrom->pSelect) );
          if( pFrom->fg.isNestedFrom ){
            assert( pFrom->pSelect!=0 );
            pNestedFrom = pFrom->pSelect->pEList;
            assert( pNestedFrom!=0 );
            assert( pNestedFrom->nExpr==pTab->nCol );
          }else{
            if( zTName && sqlite3StrICmp(zTName, zTabName)!=0 ){
              continue;
            }
            pNestedFrom = 0;
            iDb = sqlite3SchemaToIndex(db, pTab->pSchema);
            zSchemaName = iDb>=0 ? db->aDb[iDb].zDbSName : "*";
          }
          if( i+1<pTabList->nSrc
           && pFrom[1].fg.isUsing
           && (selFlags & SF_NestedFrom)!=0
          ){
            int ii;
            pUsing = pFrom[1].u3.pUsing;
            for(ii=0; ii<pUsing->nId; ii++){
              const char *zUName = pUsing->a[ii].zName;
              pRight = sqlite3Expr(db, TK_ID, zUName);
              pNew = sqlite3ExprListAppend(pParse, pNew, pRight);
              if( pNew ){
                struct ExprList_item *pX = &pNew->a[pNew->nExpr-1];
                assert( pX->zEName==0 );
                pX->zEName = sqlite3MPrintf(db,"..%s", zUName);
                pX->fg.eEName = ENAME_TAB;
                pX->fg.bUsingTerm = 1;
              }
            }
          }else{
            pUsing = 0;
          }
          for(j=0; j<pTab->nCol; j++){
            char *zName = pTab->aCol[j].zCnName;
            struct ExprList_item *pX; /* Newly added ExprList term */

            assert( zName );
            if( zTName
             && pNestedFrom
             && sqlite3MatchEName(&pNestedFrom->a[j], 0, zTName, 0)==0
            ){
              continue;
            }

            /* If a column is marked as 'hidden', omit it from the expanded
            ** result-set list unless the SELECT has the SF_IncludeHidden
            ** bit set.
            */
            if( (p->selFlags & SF_IncludeHidden)==0
             && IsHiddenColumn(&pTab->aCol[j]) 
            ){
              continue;
            }
            if( (pTab->aCol[j].colFlags & COLFLAG_NOEXPAND)!=0
             && zTName==0
             && (selFlags & (SF_NestedFrom))==0
            ){
              continue;
            }
            tableSeen = 1;

            if( i>0 && zTName==0 && (selFlags & SF_NestedFrom)==0 ){
              if( pFrom->fg.isUsing
               && sqlite3IdListIndex(pFrom->u3.pUsing, zName)>=0
              ){
                /* In a join with a USING clause, omit columns in the
                ** using clause from the table on the right. */
                continue;
              }
            }
            pRight = sqlite3Expr(db, TK_ID, zName);
            if( (pTabList->nSrc>1
                 && (  (pFrom->fg.jointype & JT_LTORJ)==0
                     || (selFlags & SF_NestedFrom)!=0
                     || !inAnyUsingClause(zName,pFrom,pTabList->nSrc-i-1)
                    )
                )
             || IN_RENAME_OBJECT
            ){
              Expr *pLeft;
              pLeft = sqlite3Expr(db, TK_ID, zTabName);
              pExpr = sqlite3PExpr(pParse, TK_DOT, pLeft, pRight);
              if( IN_RENAME_OBJECT && pE->pLeft ){
                sqlite3RenameTokenRemap(pParse, pLeft, pE->pLeft);
              }
              if( zSchemaName ){
                pLeft = sqlite3Expr(db, TK_ID, zSchemaName);
                pExpr = sqlite3PExpr(pParse, TK_DOT, pLeft, pExpr);
              }
            }else{
              pExpr = pRight;
            }
            pNew = sqlite3ExprListAppend(pParse, pNew, pExpr);
            if( pNew==0 ){
              break;  /* OOM */
            }
            pX = &pNew->a[pNew->nExpr-1];
            assert( pX->zEName==0 );
            if( (selFlags & SF_NestedFrom)!=0 && !IN_RENAME_OBJECT ){
              if( pNestedFrom ){
                pX->zEName = sqlite3DbStrDup(db, pNestedFrom->a[j].zEName);
                testcase( pX->zEName==0 );
              }else{
                pX->zEName = sqlite3MPrintf(db, "%s.%s.%s",
                                           zSchemaName, zTabName, zName);
                testcase( pX->zEName==0 );
              }
              pX->fg.eEName = ENAME_TAB;
              if( (pFrom->fg.isUsing
                   && sqlite3IdListIndex(pFrom->u3.pUsing, zName)>=0)
               || (pUsing && sqlite3IdListIndex(pUsing, zName)>=0)
               || (pTab->aCol[j].colFlags & COLFLAG_NOEXPAND)!=0
              ){
                pX->fg.bNoExpand = 1;
              }
            }else if( longNames ){
              pX->zEName = sqlite3MPrintf(db, "%s.%s", zTabName, zName);
              pX->fg.eEName = ENAME_NAME;
            }else{
              pX->zEName = sqlite3DbStrDup(db, zName);
              pX->fg.eEName = ENAME_NAME;
            }
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
  if( p->pEList ){
    if( p->pEList->nExpr>db->aLimit[SQLITE_LIMIT_COLUMN] ){
      sqlite3ErrorMsg(pParse, "too many columns in result set");
      return WRC_Abort;
    }
    if( (elistFlags & (EP_HasFunc|EP_Subquery))!=0 ){
      p->selFlags |= SF_ComplexResult;
    }
  }
#if TREETRACE_ENABLED
  if( sqlite3TreeTrace & 0x100 ){
    SELECTTRACE(0x100,pParse,p,("After result-set wildcard expansion:\n"));
    sqlite3TreeViewSelect(0, p, 0);
  }
#endif
  return WRC_Continue;
}

#if SQLITE_DEBUG
/*
** Always assert.  This xSelectCallback2 implementation proves that the
** xSelectCallback2 is never invoked.
*/
void sqlite3SelectWalkAssert2(Walker *NotUsed, Select *NotUsed2){
  UNUSED_PARAMETER2(NotUsed, NotUsed2);
  assert( 0 );
}
#endif
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
  w.xExprCallback = sqlite3ExprWalkNoop;
  w.pParse = pParse;
  if( OK_IF_ALWAYS_TRUE(pParse->hasCompound) ){
    w.xSelectCallback = convertCompoundSelectToSubquery;
    w.xSelectCallback2 = 0;
    sqlite3WalkSelect(&w, pSelect);
  }
  w.xSelectCallback = selectExpander;
  w.xSelectCallback2 = sqlite3SelectPopWith;
  w.eCode = 0;
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
static void selectAddSubqueryTypeInfo(Walker *pWalker, Select *p){
  Parse *pParse;
  int i;
  SrcList *pTabList;
  SrcItem *pFrom;

  assert( p->selFlags & SF_Resolved );
  if( p->selFlags & SF_HasTypeInfo ) return;
  p->selFlags |= SF_HasTypeInfo;
  pParse = pWalker->pParse;
  pTabList = p->pSrc;
  for(i=0, pFrom=pTabList->a; i<pTabList->nSrc; i++, pFrom++){
    Table *pTab = pFrom->pTab;
    assert( pTab!=0 );
    if( (pTab->tabFlags & TF_Ephemeral)!=0 ){
      /* A sub-query in the FROM clause of a SELECT */
      Select *pSel = pFrom->pSelect;
      if( pSel ){
        while( pSel->pPrior ) pSel = pSel->pPrior;
        sqlite3SelectAddColumnTypeAndCollation(pParse, pTab, pSel,
                                               SQLITE_AFF_NONE);
      }
    }
  }
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
  w.xSelectCallback = sqlite3SelectWalkNoop;
  w.xSelectCallback2 = selectAddSubqueryTypeInfo;
  w.xExprCallback = sqlite3ExprWalkNoop;
  w.pParse = pParse;
  sqlite3WalkSelect(&w, pSelect);
#endif
}


/*
** This routine sets up a SELECT statement for processing.  The
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
  assert( p!=0 || pParse->db->mallocFailed );
  assert( pParse->db->pParse==pParse );
  if( pParse->db->mallocFailed ) return;
  if( p->selFlags & SF_HasTypeInfo ) return;
  sqlite3SelectExpand(pParse, p);
  if( pParse->nErr ) return;
  sqlite3ResolveSelectNames(pParse, p, pOuterNC);
  if( pParse->nErr ) return;
  sqlite3SelectAddTypeInfo(pParse, p);
}

/*
** Reset the aggregate accumulator.
**
** The aggregate accumulator is a set of memory cells that hold
** intermediate results while calculating an aggregate.  This
** routine generates code that stores NULLs in all of those memory
** cells.
*/
static void resetAccumulator(Parse *pParse, AggInfo *pAggInfo){
  Vdbe *v = pParse->pVdbe;
  int i;
  struct AggInfo_func *pFunc;
  int nReg = pAggInfo->nFunc + pAggInfo->nColumn;
  assert( pParse->db->pParse==pParse );
  assert( pParse->db->mallocFailed==0 || pParse->nErr!=0 );
  if( nReg==0 ) return;
  if( pParse->nErr ) return;
#ifdef SQLITE_DEBUG
  /* Verify that all AggInfo registers are within the range specified by
  ** AggInfo.mnReg..AggInfo.mxReg */
  assert( nReg==pAggInfo->mxReg-pAggInfo->mnReg+1 );
  for(i=0; i<pAggInfo->nColumn; i++){
    assert( pAggInfo->aCol[i].iMem>=pAggInfo->mnReg
         && pAggInfo->aCol[i].iMem<=pAggInfo->mxReg );
  }
  for(i=0; i<pAggInfo->nFunc; i++){
    assert( pAggInfo->aFunc[i].iMem>=pAggInfo->mnReg
         && pAggInfo->aFunc[i].iMem<=pAggInfo->mxReg );
  }
#endif
  sqlite3VdbeAddOp3(v, OP_Null, 0, pAggInfo->mnReg, pAggInfo->mxReg);
  for(pFunc=pAggInfo->aFunc, i=0; i<pAggInfo->nFunc; i++, pFunc++){
    if( pFunc->iDistinct>=0 ){
      Expr *pE = pFunc->pFExpr;
      assert( ExprUseXList(pE) );
      if( pE->x.pList==0 || pE->x.pList->nExpr!=1 ){
        sqlite3ErrorMsg(pParse, "DISTINCT aggregates must have exactly one "
           "argument");
        pFunc->iDistinct = -1;
      }else{
        KeyInfo *pKeyInfo = sqlite3KeyInfoFromExprList(pParse, pE->x.pList,0,0);
        pFunc->iDistAddr = sqlite3VdbeAddOp4(v, OP_OpenEphemeral, 
            pFunc->iDistinct, 0, 0, (char*)pKeyInfo, P4_KEYINFO);
        ExplainQueryPlan((pParse, 0, "USE TEMP B-TREE FOR %s(DISTINCT)",
                          pFunc->pFunc->zName));
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
    ExprList *pList;
    assert( ExprUseXList(pF->pFExpr) );
    pList = pF->pFExpr->x.pList;
    sqlite3VdbeAddOp2(v, OP_AggFinal, pF->iMem, pList ? pList->nExpr : 0);
    sqlite3VdbeAppendP4(v, pF->pFunc, P4_FUNCDEF);
  }
}


/*
** Update the accumulator memory cells for an aggregate based on
** the current cursor position.
**
** If regAcc is non-zero and there are no min() or max() aggregates
** in pAggInfo, then only populate the pAggInfo->nAccumulator accumulator
** registers if register regAcc contains 0. The caller will take care
** of setting and clearing regAcc.
*/
static void updateAccumulator(
  Parse *pParse, 
  int regAcc, 
  AggInfo *pAggInfo,
  int eDistinctType
){
  Vdbe *v = pParse->pVdbe;
  int i;
  int regHit = 0;
  int addrHitTest = 0;
  struct AggInfo_func *pF;
  struct AggInfo_col *pC;

  pAggInfo->directMode = 1;
  for(i=0, pF=pAggInfo->aFunc; i<pAggInfo->nFunc; i++, pF++){
    int nArg;
    int addrNext = 0;
    int regAgg;
    ExprList *pList;
    assert( ExprUseXList(pF->pFExpr) );
    assert( !IsWindowFunc(pF->pFExpr) );
    pList = pF->pFExpr->x.pList;
    if( ExprHasProperty(pF->pFExpr, EP_WinFunc) ){
      Expr *pFilter = pF->pFExpr->y.pWin->pFilter;
      if( pAggInfo->nAccumulator 
       && (pF->pFunc->funcFlags & SQLITE_FUNC_NEEDCOLL) 
       && regAcc
      ){
        /* If regAcc==0, there there exists some min() or max() function
        ** without a FILTER clause that will ensure the magnet registers
        ** are populated. */
        if( regHit==0 ) regHit = ++pParse->nMem;
        /* If this is the first row of the group (regAcc contains 0), clear the
        ** "magnet" register regHit so that the accumulator registers
        ** are populated if the FILTER clause jumps over the the 
        ** invocation of min() or max() altogether. Or, if this is not
        ** the first row (regAcc contains 1), set the magnet register so that
        ** the accumulators are not populated unless the min()/max() is invoked
        ** and indicates that they should be.  */
        sqlite3VdbeAddOp2(v, OP_Copy, regAcc, regHit);
      }
      addrNext = sqlite3VdbeMakeLabel(pParse);
      sqlite3ExprIfFalse(pParse, pFilter, addrNext, SQLITE_JUMPIFNULL);
    }
    if( pList ){
      nArg = pList->nExpr;
      regAgg = sqlite3GetTempRange(pParse, nArg);
      sqlite3ExprCodeExprList(pParse, pList, regAgg, 0, SQLITE_ECEL_DUP);
    }else{
      nArg = 0;
      regAgg = 0;
    }
    if( pF->iDistinct>=0 && pList ){
      if( addrNext==0 ){ 
        addrNext = sqlite3VdbeMakeLabel(pParse);
      }
      pF->iDistinct = codeDistinct(pParse, eDistinctType, 
          pF->iDistinct, addrNext, pList, regAgg);
    }
    if( pF->pFunc->funcFlags & SQLITE_FUNC_NEEDCOLL ){
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
      if( regHit==0 && pAggInfo->nAccumulator ) regHit = ++pParse->nMem;
      sqlite3VdbeAddOp4(v, OP_CollSeq, regHit, 0, 0, (char *)pColl, P4_COLLSEQ);
    }
    sqlite3VdbeAddOp3(v, OP_AggStep, 0, regAgg, pF->iMem);
    sqlite3VdbeAppendP4(v, pF->pFunc, P4_FUNCDEF);
    sqlite3VdbeChangeP5(v, (u8)nArg);
    sqlite3ReleaseTempRange(pParse, regAgg, nArg);
    if( addrNext ){
      sqlite3VdbeResolveLabel(v, addrNext);
    }
  }
  if( regHit==0 && pAggInfo->nAccumulator ){
    regHit = regAcc;
  }
  if( regHit ){
    addrHitTest = sqlite3VdbeAddOp1(v, OP_If, regHit); VdbeCoverage(v);
  }
  for(i=0, pC=pAggInfo->aCol; i<pAggInfo->nAccumulator; i++, pC++){
    sqlite3ExprCode(pParse, pC->pCExpr, pC->iMem);
  }

  pAggInfo->directMode = 0;
  if( addrHitTest ){
    sqlite3VdbeJumpHereOrPopInst(v, addrHitTest);
  }
}

/*
** Add a single OP_Explain instruction to the VDBE to explain a simple
** count(*) query ("SELECT count(*) FROM pTab").
*/
#ifndef SQLITE_OMIT_EXPLAIN
static void explainSimpleCount(
  Parse *pParse,                  /* Parse context */
  Table *pTab,                    /* Table being queried */
  Index *pIdx                     /* Index used to optimize scan, or NULL */
){
  if( pParse->explain==2 ){
    int bCover = (pIdx!=0 && (HasRowid(pTab) || !IsPrimaryKeyIndex(pIdx)));
    sqlite3VdbeExplain(pParse, 0, "SCAN %s%s%s",
        pTab->zName,
        bCover ? " USING COVERING INDEX " : "",
        bCover ? pIdx->zName : ""
    );
  }
}
#else
# define explainSimpleCount(a,b,c)
#endif

/*
** sqlite3WalkExpr() callback used by havingToWhere().
**
** If the node passed to the callback is a TK_AND node, return 
** WRC_Continue to tell sqlite3WalkExpr() to iterate through child nodes.
**
** Otherwise, return WRC_Prune. In this case, also check if the 
** sub-expression matches the criteria for being moved to the WHERE
** clause. If so, add it to the WHERE clause and replace the sub-expression
** within the HAVING expression with a constant "1".
*/
static int havingToWhereExprCb(Walker *pWalker, Expr *pExpr){
  if( pExpr->op!=TK_AND ){
    Select *pS = pWalker->u.pSelect;
    /* This routine is called before the HAVING clause of the current
    ** SELECT is analyzed for aggregates. So if pExpr->pAggInfo is set
    ** here, it indicates that the expression is a correlated reference to a
    ** column from an outer aggregate query, or an aggregate function that
    ** belongs to an outer query. Do not move the expression to the WHERE
    ** clause in this obscure case, as doing so may corrupt the outer Select
    ** statements AggInfo structure.  */
    if( sqlite3ExprIsConstantOrGroupBy(pWalker->pParse, pExpr, pS->pGroupBy) 
     && ExprAlwaysFalse(pExpr)==0
     && pExpr->pAggInfo==0
    ){
      sqlite3 *db = pWalker->pParse->db;
      Expr *pNew = sqlite3Expr(db, TK_INTEGER, "1");
      if( pNew ){
        Expr *pWhere = pS->pWhere;
        SWAP(Expr, *pNew, *pExpr);
        pNew = sqlite3ExprAnd(pWalker->pParse, pWhere, pNew);
        pS->pWhere = pNew;
        pWalker->eCode = 1;
      }
    }
    return WRC_Prune;
  }
  return WRC_Continue;
}

/*
** Transfer eligible terms from the HAVING clause of a query, which is
** processed after grouping, to the WHERE clause, which is processed before
** grouping. For example, the query:
**
**   SELECT * FROM <tables> WHERE a=? GROUP BY b HAVING b=? AND c=?
**
** can be rewritten as:
**
**   SELECT * FROM <tables> WHERE a=? AND b=? GROUP BY b HAVING c=?
**
** A term of the HAVING expression is eligible for transfer if it consists
** entirely of constants and expressions that are also GROUP BY terms that
** use the "BINARY" collation sequence.
*/
static void havingToWhere(Parse *pParse, Select *p){
  Walker sWalker;
  memset(&sWalker, 0, sizeof(sWalker));
  sWalker.pParse = pParse;
  sWalker.xExprCallback = havingToWhereExprCb;
  sWalker.u.pSelect = p;
  sqlite3WalkExpr(&sWalker, p->pHaving);
#if TREETRACE_ENABLED
  if( sWalker.eCode && (sqlite3TreeTrace & 0x100)!=0 ){
    SELECTTRACE(0x100,pParse,p,("Move HAVING terms into WHERE:\n"));
    sqlite3TreeViewSelect(0, p, 0);
  }
#endif
}

/*
** Check to see if the pThis entry of pTabList is a self-join of a prior view.
** If it is, then return the SrcItem for the prior view.  If it is not,
** then return 0.
*/
static SrcItem *isSelfJoinView(
  SrcList *pTabList,           /* Search for self-joins in this FROM clause */
  SrcItem *pThis               /* Search for prior reference to this subquery */
){
  SrcItem *pItem;
  assert( pThis->pSelect!=0 );
  if( pThis->pSelect->selFlags & SF_PushDown ) return 0;
  for(pItem = pTabList->a; pItem<pThis; pItem++){
    Select *pS1;
    if( pItem->pSelect==0 ) continue;
    if( pItem->fg.viaCoroutine ) continue;
    if( pItem->zName==0 ) continue;
    assert( pItem->pTab!=0 );
    assert( pThis->pTab!=0 );
    if( pItem->pTab->pSchema!=pThis->pTab->pSchema ) continue;
    if( sqlite3_stricmp(pItem->zName, pThis->zName)!=0 ) continue;
    pS1 = pItem->pSelect;
    if( pItem->pTab->pSchema==0 && pThis->pSelect->selId!=pS1->selId ){
      /* The query flattener left two different CTE tables with identical
      ** names in the same FROM clause. */
      continue;
    }
    if( pItem->pSelect->selFlags & SF_PushDown ){
      /* The view was modified by some other optimization such as
      ** pushDownWhereTerms() */
      continue;
    }
    return pItem;
  }
  return 0;
}

/*
** Deallocate a single AggInfo object
*/
static void agginfoFree(sqlite3 *db, AggInfo *p){
  sqlite3DbFree(db, p->aCol);
  sqlite3DbFree(db, p->aFunc);
  sqlite3DbFreeNN(db, p);
}

#ifdef SQLITE_COUNTOFVIEW_OPTIMIZATION
/*
** Attempt to transform a query of the form
**
**    SELECT count(*) FROM (SELECT x FROM t1 UNION ALL SELECT y FROM t2)
**
** Into this:
**
**    SELECT (SELECT count(*) FROM t1)+(SELECT count(*) FROM t2)
**
** The transformation only works if all of the following are true:
**
**   *  The subquery is a UNION ALL of two or more terms
**   *  The subquery does not have a LIMIT clause
**   *  There is no WHERE or GROUP BY or HAVING clauses on the subqueries
**   *  The outer query is a simple count(*) with no WHERE clause or other
**      extraneous syntax.
**
** Return TRUE if the optimization is undertaken.
*/
static int countOfViewOptimization(Parse *pParse, Select *p){
  Select *pSub, *pPrior;
  Expr *pExpr;
  Expr *pCount;
  sqlite3 *db;
  if( (p->selFlags & SF_Aggregate)==0 ) return 0;   /* This is an aggregate */
  if( p->pEList->nExpr!=1 ) return 0;               /* Single result column */
  if( p->pWhere ) return 0;
  if( p->pGroupBy ) return 0;
  pExpr = p->pEList->a[0].pExpr;
  if( pExpr->op!=TK_AGG_FUNCTION ) return 0;        /* Result is an aggregate */
  assert( ExprUseUToken(pExpr) );
  if( sqlite3_stricmp(pExpr->u.zToken,"count") ) return 0;  /* Is count() */
  assert( ExprUseXList(pExpr) );
  if( pExpr->x.pList!=0 ) return 0;                 /* Must be count(*) */
  if( p->pSrc->nSrc!=1 ) return 0;                  /* One table in FROM  */
  pSub = p->pSrc->a[0].pSelect;
  if( pSub==0 ) return 0;                           /* The FROM is a subquery */
  if( pSub->pPrior==0 ) return 0;                   /* Must be a compound ry */
  do{
    if( pSub->op!=TK_ALL && pSub->pPrior ) return 0;  /* Must be UNION ALL */
    if( pSub->pWhere ) return 0;                      /* No WHERE clause */
    if( pSub->pLimit ) return 0;                      /* No LIMIT clause */
    if( pSub->selFlags & SF_Aggregate ) return 0;     /* Not an aggregate */
    pSub = pSub->pPrior;                              /* Repeat over compound */
  }while( pSub );

  /* If we reach this point then it is OK to perform the transformation */

  db = pParse->db;
  pCount = pExpr;
  pExpr = 0;
  pSub = p->pSrc->a[0].pSelect;
  p->pSrc->a[0].pSelect = 0;
  sqlite3SrcListDelete(db, p->pSrc);
  p->pSrc = sqlite3DbMallocZero(pParse->db, sizeof(*p->pSrc));
  while( pSub ){
    Expr *pTerm;
    pPrior = pSub->pPrior;
    pSub->pPrior = 0;
    pSub->pNext = 0;
    pSub->selFlags |= SF_Aggregate;
    pSub->selFlags &= ~SF_Compound;
    pSub->nSelectRow = 0;
    sqlite3ExprListDelete(db, pSub->pEList);
    pTerm = pPrior ? sqlite3ExprDup(db, pCount, 0) : pCount;
    pSub->pEList = sqlite3ExprListAppend(pParse, 0, pTerm);
    pTerm = sqlite3PExpr(pParse, TK_SELECT, 0, 0);
    sqlite3PExprAddSelect(pParse, pTerm, pSub);
    if( pExpr==0 ){
      pExpr = pTerm;
    }else{
      pExpr = sqlite3PExpr(pParse, TK_PLUS, pTerm, pExpr);
    }
    pSub = pPrior;
  }
  p->pEList->a[0].pExpr = pExpr;
  p->selFlags &= ~SF_Aggregate;

#if TREETRACE_ENABLED
  if( sqlite3TreeTrace & 0x400 ){
    SELECTTRACE(0x400,pParse,p,("After count-of-view optimization:\n"));
    sqlite3TreeViewSelect(0, p, 0);
  }
#endif
  return 1;
}
#endif /* SQLITE_COUNTOFVIEW_OPTIMIZATION */

/*
** If any term of pSrc, or any SF_NestedFrom sub-query, is not the same
** as pSrcItem but has the same alias as p0, then return true.
** Otherwise return false.
*/
static int sameSrcAlias(SrcItem *p0, SrcList *pSrc){
  int i;
  for(i=0; i<pSrc->nSrc; i++){
    SrcItem *p1 = &pSrc->a[i];
    if( p1==p0 ) continue;
    if( p0->pTab==p1->pTab && 0==sqlite3_stricmp(p0->zAlias, p1->zAlias) ){
      return 1;
    }
    if( p1->pSelect
     && (p1->pSelect->selFlags & SF_NestedFrom)!=0
     && sameSrcAlias(p0, p1->pSelect->pSrc)
    ){
      return 1;
    }
  }
  return 0;
}

/*
** Generate code for the SELECT statement given in the p argument.  
**
** The results are returned according to the SelectDest structure.
** See comments in sqliteInt.h for further information.
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
  ExprList *pEList = 0;  /* List of columns to extract. */
  SrcList *pTabList;     /* List of tables to select from */
  Expr *pWhere;          /* The WHERE clause.  May be NULL */
  ExprList *pGroupBy;    /* The GROUP BY clause.  May be NULL */
  Expr *pHaving;         /* The HAVING clause.  May be NULL */
  AggInfo *pAggInfo = 0; /* Aggregate information */
  int rc = 1;            /* Value to return from this function */
  DistinctCtx sDistinct; /* Info on how to code the DISTINCT keyword */
  SortCtx sSort;         /* Info on how to code the ORDER BY clause */
  int iEnd;              /* Address of the end of the query */
  sqlite3 *db;           /* The database connection */
  ExprList *pMinMaxOrderBy = 0;  /* Added ORDER BY for min/max queries */
  u8 minMaxFlag;                 /* Flag for min/max queries */

  db = pParse->db;
  assert( pParse==db->pParse );
  v = sqlite3GetVdbe(pParse);
  if( p==0 || pParse->nErr ){
    return 1;
  }
  assert( db->mallocFailed==0 );
  if( sqlite3AuthCheck(pParse, SQLITE_SELECT, 0, 0, 0) ) return 1;
#if TREETRACE_ENABLED
  SELECTTRACE(1,pParse,p, ("begin processing:\n", pParse->addrExplain));
  if( sqlite3TreeTrace & 0x10100 ){
    if( (sqlite3TreeTrace & 0x10001)==0x10000 ){
      sqlite3TreeViewLine(0, "In sqlite3Select() at %s:%d",
                           __FILE__, __LINE__);
    }
    sqlite3ShowSelect(p);
  }
#endif

  assert( p->pOrderBy==0 || pDest->eDest!=SRT_DistFifo );
  assert( p->pOrderBy==0 || pDest->eDest!=SRT_Fifo );
  assert( p->pOrderBy==0 || pDest->eDest!=SRT_DistQueue );
  assert( p->pOrderBy==0 || pDest->eDest!=SRT_Queue );
  if( IgnorableDistinct(pDest) ){
    assert(pDest->eDest==SRT_Exists     || pDest->eDest==SRT_Union || 
           pDest->eDest==SRT_Except     || pDest->eDest==SRT_Discard ||
           pDest->eDest==SRT_DistQueue  || pDest->eDest==SRT_DistFifo );
    /* All of these destinations are also able to ignore the ORDER BY clause */
    if( p->pOrderBy ){
#if TREETRACE_ENABLED
      SELECTTRACE(1,pParse,p, ("dropping superfluous ORDER BY:\n"));
      if( sqlite3TreeTrace & 0x100 ){
        sqlite3TreeViewExprList(0, p->pOrderBy, 0, "ORDERBY");
      }
#endif    
      sqlite3ParserAddCleanup(pParse,
        (void(*)(sqlite3*,void*))sqlite3ExprListDelete,
        p->pOrderBy);
      testcase( pParse->earlyCleanup );
      p->pOrderBy = 0;
    }
    p->selFlags &= ~SF_Distinct;
    p->selFlags |= SF_NoopOrderBy;
  }
  sqlite3SelectPrep(pParse, p, 0);
  if( pParse->nErr ){
    goto select_end;
  }
  assert( db->mallocFailed==0 );
  assert( p->pEList!=0 );
#if TREETRACE_ENABLED
  if( sqlite3TreeTrace & 0x104 ){
    SELECTTRACE(0x104,pParse,p, ("after name resolution:\n"));
    sqlite3TreeViewSelect(0, p, 0);
  }
#endif

  /* If the SF_UFSrcCheck flag is set, then this function is being called
  ** as part of populating the temp table for an UPDATE...FROM statement.
  ** In this case, it is an error if the target object (pSrc->a[0]) name 
  ** or alias is duplicated within FROM clause (pSrc->a[1..n]).  
  **
  ** Postgres disallows this case too. The reason is that some other 
  ** systems handle this case differently, and not all the same way, 
  ** which is just confusing. To avoid this, we follow PG's lead and
  ** disallow it altogether.  */
  if( p->selFlags & SF_UFSrcCheck ){
    SrcItem *p0 = &p->pSrc->a[0];
    if( sameSrcAlias(p0, p->pSrc) ){
      sqlite3ErrorMsg(pParse, 
          "target object/alias may not appear in FROM clause: %s", 
          p0->zAlias ? p0->zAlias : p0->pTab->zName
      );
      goto select_end;
    }

    /* Clear the SF_UFSrcCheck flag. The check has already been performed,
    ** and leaving this flag set can cause errors if a compound sub-query
    ** in p->pSrc is flattened into this query and this function called
    ** again as part of compound SELECT processing.  */
    p->selFlags &= ~SF_UFSrcCheck;
  }

  if( pDest->eDest==SRT_Output ){
    sqlite3GenerateColumnNames(pParse, p);
  }

#ifndef SQLITE_OMIT_WINDOWFUNC
  if( sqlite3WindowRewrite(pParse, p) ){
    assert( pParse->nErr );
    goto select_end;
  }
#if TREETRACE_ENABLED
  if( p->pWin && (sqlite3TreeTrace & 0x108)!=0 ){
    SELECTTRACE(0x104,pParse,p, ("after window rewrite:\n"));
    sqlite3TreeViewSelect(0, p, 0);
  }
#endif
#endif /* SQLITE_OMIT_WINDOWFUNC */
  pTabList = p->pSrc;
  isAgg = (p->selFlags & SF_Aggregate)!=0;
  memset(&sSort, 0, sizeof(sSort));
  sSort.pOrderBy = p->pOrderBy;

  /* Try to do various optimizations (flattening subqueries, and strength
  ** reduction of join operators) in the FROM clause up into the main query
  */
#if !defined(SQLITE_OMIT_SUBQUERY) || !defined(SQLITE_OMIT_VIEW)
  for(i=0; !p->pPrior && i<pTabList->nSrc; i++){
    SrcItem *pItem = &pTabList->a[i];
    Select *pSub = pItem->pSelect;
    Table *pTab = pItem->pTab;

    /* The expander should have already created transient Table objects
    ** even for FROM clause elements such as subqueries that do not correspond
    ** to a real table */
    assert( pTab!=0 );

    /* Convert LEFT JOIN into JOIN if there are terms of the right table
    ** of the LEFT JOIN used in the WHERE clause.
    */
    if( (pItem->fg.jointype & (JT_LEFT|JT_RIGHT))==JT_LEFT
     && sqlite3ExprImpliesNonNullRow(p->pWhere, pItem->iCursor)
     && OptimizationEnabled(db, SQLITE_SimplifyJoin)
    ){
      SELECTTRACE(0x100,pParse,p,
                ("LEFT-JOIN simplifies to JOIN on term %d\n",i));
      pItem->fg.jointype &= ~(JT_LEFT|JT_OUTER);
      assert( pItem->iCursor>=0 );
      unsetJoinExpr(p->pWhere, pItem->iCursor,
                    pTabList->a[0].fg.jointype & JT_LTORJ);
    }

    /* No futher action if this term of the FROM clause is no a subquery */
    if( pSub==0 ) continue;

    /* Catch mismatch in the declared columns of a view and the number of
    ** columns in the SELECT on the RHS */
    if( pTab->nCol!=pSub->pEList->nExpr ){
      sqlite3ErrorMsg(pParse, "expected %d columns for '%s' but got %d",
                      pTab->nCol, pTab->zName, pSub->pEList->nExpr);
      goto select_end;
    }

    /* Do not try to flatten an aggregate subquery.
    **
    ** Flattening an aggregate subquery is only possible if the outer query
    ** is not a join.  But if the outer query is not a join, then the subquery
    ** will be implemented as a co-routine and there is no advantage to
    ** flattening in that case.
    */
    if( (pSub->selFlags & SF_Aggregate)!=0 ) continue;
    assert( pSub->pGroupBy==0 );

    /* If a FROM-clause subquery has an ORDER BY clause that is not
    ** really doing anything, then delete it now so that it does not
    ** interfere with query flattening.  See the discussion at
    ** https://sqlite.org/forum/forumpost/2d76f2bcf65d256a
    **
    ** Beware of these cases where the ORDER BY clause may not be safely
    ** omitted:
    **
    **    (1)   There is also a LIMIT clause
    **    (2)   The subquery was added to help with window-function
    **          processing
    **    (3)   The subquery is in the FROM clause of an UPDATE
    **    (4)   The outer query uses an aggregate function other than
    **          the built-in count(), min(), or max().
    **    (5)   The ORDER BY isn't going to accomplish anything because
    **          one of:
    **            (a)  The outer query has a different ORDER BY clause
    **            (b)  The subquery is part of a join
    **          See forum post 062d576715d277c8
    */
    if( pSub->pOrderBy!=0
     && (p->pOrderBy!=0 || pTabList->nSrc>1)      /* Condition (5) */
     && pSub->pLimit==0                           /* Condition (1) */
     && (pSub->selFlags & SF_OrderByReqd)==0      /* Condition (2) */
     && (p->selFlags & SF_OrderByReqd)==0         /* Condition (3) and (4) */
     && OptimizationEnabled(db, SQLITE_OmitOrderBy)
    ){
      SELECTTRACE(0x100,pParse,p,
                ("omit superfluous ORDER BY on %r FROM-clause subquery\n",i+1));
      sqlite3ParserAddCleanup(pParse, 
         (void(*)(sqlite3*,void*))sqlite3ExprListDelete,
         pSub->pOrderBy);
      pSub->pOrderBy = 0;
    }

    /* If the outer query contains a "complex" result set (that is,
    ** if the result set of the outer query uses functions or subqueries)
    ** and if the subquery contains an ORDER BY clause and if
    ** it will be implemented as a co-routine, then do not flatten.  This
    ** restriction allows SQL constructs like this:
    **
    **  SELECT expensive_function(x)
    **    FROM (SELECT x FROM tab ORDER BY y LIMIT 10);
    **
    ** The expensive_function() is only computed on the 10 rows that
    ** are output, rather than every row of the table.
    **
    ** The requirement that the outer query have a complex result set
    ** means that flattening does occur on simpler SQL constraints without
    ** the expensive_function() like:
    **
    **  SELECT x FROM (SELECT x FROM tab ORDER BY y LIMIT 10);
    */
    if( pSub->pOrderBy!=0
     && i==0
     && (p->selFlags & SF_ComplexResult)!=0
     && (pTabList->nSrc==1
         || (pTabList->a[1].fg.jointype&(JT_OUTER|JT_CROSS))!=0)
    ){
      continue;
    }

    if( flattenSubquery(pParse, p, i, isAgg) ){
      if( pParse->nErr ) goto select_end;
      /* This subquery can be absorbed into its parent. */
      i = -1;
    }
    pTabList = p->pSrc;
    if( db->mallocFailed ) goto select_end;
    if( !IgnorableOrderby(pDest) ){
      sSort.pOrderBy = p->pOrderBy;
    }
  }
#endif

#ifndef SQLITE_OMIT_COMPOUND_SELECT
  /* Handle compound SELECT statements using the separate multiSelect()
  ** procedure.
  */
  if( p->pPrior ){
    rc = multiSelect(pParse, p, pDest);
#if TREETRACE_ENABLED
    SELECTTRACE(0x1,pParse,p,("end compound-select processing\n"));
    if( (sqlite3TreeTrace & 0x2000)!=0 && ExplainQueryPlanParent(pParse)==0 ){
      sqlite3TreeViewSelect(0, p, 0);
    }
#endif
    if( p->pNext==0 ) ExplainQueryPlanPop(pParse);
    return rc;
  }
#endif

  /* Do the WHERE-clause constant propagation optimization if this is
  ** a join.  No need to speed time on this operation for non-join queries
  ** as the equivalent optimization will be handled by query planner in
  ** sqlite3WhereBegin().
  */
  if( p->pWhere!=0
   && p->pWhere->op==TK_AND
   && OptimizationEnabled(db, SQLITE_PropagateConst)
   && propagateConstants(pParse, p)
  ){
#if TREETRACE_ENABLED
    if( sqlite3TreeTrace & 0x100 ){
      SELECTTRACE(0x100,pParse,p,("After constant propagation:\n"));
      sqlite3TreeViewSelect(0, p, 0);
    }
#endif
  }else{
    SELECTTRACE(0x100,pParse,p,("Constant propagation not helpful\n"));
  }

#ifdef SQLITE_COUNTOFVIEW_OPTIMIZATION
  if( OptimizationEnabled(db, SQLITE_QueryFlattener|SQLITE_CountOfView)
   && countOfViewOptimization(pParse, p)
  ){
    if( db->mallocFailed ) goto select_end;
    pEList = p->pEList;
    pTabList = p->pSrc;
  }
#endif

  /* For each term in the FROM clause, do two things:
  ** (1) Authorized unreferenced tables
  ** (2) Generate code for all sub-queries
  */
  for(i=0; i<pTabList->nSrc; i++){
    SrcItem *pItem = &pTabList->a[i];
    SrcItem *pPrior;
    SelectDest dest;
    Select *pSub;
#if !defined(SQLITE_OMIT_SUBQUERY) || !defined(SQLITE_OMIT_VIEW)
    const char *zSavedAuthContext;
#endif

    /* Issue SQLITE_READ authorizations with a fake column name for any
    ** tables that are referenced but from which no values are extracted.
    ** Examples of where these kinds of null SQLITE_READ authorizations
    ** would occur:
    **
    **     SELECT count(*) FROM t1;   -- SQLITE_READ t1.""
    **     SELECT t1.* FROM t1, t2;   -- SQLITE_READ t2.""
    **
    ** The fake column name is an empty string.  It is possible for a table to
    ** have a column named by the empty string, in which case there is no way to
    ** distinguish between an unreferenced table and an actual reference to the
    ** "" column. The original design was for the fake column name to be a NULL,
    ** which would be unambiguous.  But legacy authorization callbacks might
    ** assume the column name is non-NULL and segfault.  The use of an empty
    ** string for the fake column name seems safer.
    */
    if( pItem->colUsed==0 && pItem->zName!=0 ){
      sqlite3AuthCheck(pParse, SQLITE_READ, pItem->zName, "", pItem->zDatabase);
    }

#if !defined(SQLITE_OMIT_SUBQUERY) || !defined(SQLITE_OMIT_VIEW)
    /* Generate code for all sub-queries in the FROM clause
    */
    pSub = pItem->pSelect;
    if( pSub==0 ) continue;

    /* The code for a subquery should only be generated once. */
    assert( pItem->addrFillSub==0 );

    /* Increment Parse.nHeight by the height of the largest expression
    ** tree referred to by this, the parent select. The child select
    ** may contain expression trees of at most
    ** (SQLITE_MAX_EXPR_DEPTH-Parse.nHeight) height. This is a bit
    ** more conservative than necessary, but much easier than enforcing
    ** an exact limit.
    */
    pParse->nHeight += sqlite3SelectExprHeight(p);

    /* Make copies of constant WHERE-clause terms in the outer query down
    ** inside the subquery.  This can help the subquery to run more efficiently.
    */
    if( OptimizationEnabled(db, SQLITE_PushDown)
     && (pItem->fg.isCte==0 
         || (pItem->u2.pCteUse->eM10d!=M10d_Yes && pItem->u2.pCteUse->nUse<2))
     && pushDownWhereTerms(pParse, pSub, p->pWhere, pItem)
    ){
#if TREETRACE_ENABLED
      if( sqlite3TreeTrace & 0x100 ){
        SELECTTRACE(0x100,pParse,p,
            ("After WHERE-clause push-down into subquery %d:\n", pSub->selId));
        sqlite3TreeViewSelect(0, p, 0);
      }
#endif
      assert( pItem->pSelect && (pItem->pSelect->selFlags & SF_PushDown)!=0 );
    }else{
      SELECTTRACE(0x100,pParse,p,("Push-down not possible\n"));
    }

    zSavedAuthContext = pParse->zAuthContext;
    pParse->zAuthContext = pItem->zName;

    /* Generate code to implement the subquery
    **
    ** The subquery is implemented as a co-routine if all of the following are
    ** true:
    **
    **    (1)  the subquery is guaranteed to be the outer loop (so that
    **         it does not need to be computed more than once), and
    **    (2)  the subquery is not a CTE that should be materialized
    **    (3)  the subquery is not part of a left operand for a RIGHT JOIN
    */
    if( i==0
     && (pTabList->nSrc==1
            || (pTabList->a[1].fg.jointype&(JT_OUTER|JT_CROSS))!=0)  /* (1) */
     && (pItem->fg.isCte==0 || pItem->u2.pCteUse->eM10d!=M10d_Yes)   /* (2) */
     && (pTabList->a[0].fg.jointype & JT_LTORJ)==0                   /* (3) */
    ){
      /* Implement a co-routine that will return a single row of the result
      ** set on each invocation.
      */
      int addrTop = sqlite3VdbeCurrentAddr(v)+1;
     
      pItem->regReturn = ++pParse->nMem;
      sqlite3VdbeAddOp3(v, OP_InitCoroutine, pItem->regReturn, 0, addrTop);
      VdbeComment((v, "%!S", pItem));
      pItem->addrFillSub = addrTop;
      sqlite3SelectDestInit(&dest, SRT_Coroutine, pItem->regReturn);
      ExplainQueryPlan((pParse, 1, "CO-ROUTINE %!S", pItem));
      sqlite3Select(pParse, pSub, &dest);
      pItem->pTab->nRowLogEst = pSub->nSelectRow;
      pItem->fg.viaCoroutine = 1;
      pItem->regResult = dest.iSdst;
      sqlite3VdbeEndCoroutine(v, pItem->regReturn);
      sqlite3VdbeJumpHere(v, addrTop-1);
      sqlite3ClearTempRegCache(pParse);
    }else if( pItem->fg.isCte && pItem->u2.pCteUse->addrM9e>0 ){
      /* This is a CTE for which materialization code has already been
      ** generated.  Invoke the subroutine to compute the materialization,
      ** the make the pItem->iCursor be a copy of the ephemerial table that
      ** holds the result of the materialization. */
      CteUse *pCteUse = pItem->u2.pCteUse;
      sqlite3VdbeAddOp2(v, OP_Gosub, pCteUse->regRtn, pCteUse->addrM9e);
      if( pItem->iCursor!=pCteUse->iCur ){
        sqlite3VdbeAddOp2(v, OP_OpenDup, pItem->iCursor, pCteUse->iCur);
        VdbeComment((v, "%!S", pItem));
      }
      pSub->nSelectRow = pCteUse->nRowEst;
    }else if( (pPrior = isSelfJoinView(pTabList, pItem))!=0 ){
      /* This view has already been materialized by a prior entry in
      ** this same FROM clause.  Reuse it. */
      if( pPrior->addrFillSub ){
        sqlite3VdbeAddOp2(v, OP_Gosub, pPrior->regReturn, pPrior->addrFillSub);
      }
      sqlite3VdbeAddOp2(v, OP_OpenDup, pItem->iCursor, pPrior->iCursor);
      pSub->nSelectRow = pPrior->pSelect->nSelectRow;
    }else{
      /* Materialize the view.  If the view is not correlated, generate a
      ** subroutine to do the materialization so that subsequent uses of
      ** the same view can reuse the materialization. */
      int topAddr;
      int onceAddr = 0;

      pItem->regReturn = ++pParse->nMem;
      topAddr = sqlite3VdbeAddOp0(v, OP_Goto);
      pItem->addrFillSub = topAddr+1;
      pItem->fg.isMaterialized = 1;
      if( pItem->fg.isCorrelated==0 ){
        /* If the subquery is not correlated and if we are not inside of
        ** a trigger, then we only need to compute the value of the subquery
        ** once. */
        onceAddr = sqlite3VdbeAddOp0(v, OP_Once); VdbeCoverage(v);
        VdbeComment((v, "materialize %!S", pItem));
      }else{
        VdbeNoopComment((v, "materialize %!S", pItem));
      }
      sqlite3SelectDestInit(&dest, SRT_EphemTab, pItem->iCursor);
      ExplainQueryPlan((pParse, 1, "MATERIALIZE %!S", pItem));
      dest.zAffSdst = sqlite3TableAffinityStr(db, pItem->pTab);
      sqlite3Select(pParse, pSub, &dest);
      sqlite3DbFree(db, dest.zAffSdst);
      dest.zAffSdst = 0;
      pItem->pTab->nRowLogEst = pSub->nSelectRow;
      if( onceAddr ) sqlite3VdbeJumpHere(v, onceAddr);
      sqlite3VdbeAddOp2(v, OP_Return, pItem->regReturn, topAddr+1);
      VdbeComment((v, "end %!S", pItem));
      sqlite3VdbeJumpHere(v, topAddr);
      sqlite3ClearTempRegCache(pParse);
      if( pItem->fg.isCte && pItem->fg.isCorrelated==0 ){
        CteUse *pCteUse = pItem->u2.pCteUse;
        pCteUse->addrM9e = pItem->addrFillSub;
        pCteUse->regRtn = pItem->regReturn;
        pCteUse->iCur = pItem->iCursor;
        pCteUse->nRowEst = pSub->nSelectRow;
      }
    }
    if( db->mallocFailed ) goto select_end;
    pParse->nHeight -= sqlite3SelectExprHeight(p);
    pParse->zAuthContext = zSavedAuthContext;
#endif
  }

  /* Various elements of the SELECT copied into local variables for
  ** convenience */
  pEList = p->pEList;
  pWhere = p->pWhere;
  pGroupBy = p->pGroupBy;
  pHaving = p->pHaving;
  sDistinct.isTnct = (p->selFlags & SF_Distinct)!=0;

#if TREETRACE_ENABLED
  if( sqlite3TreeTrace & 0x400 ){
    SELECTTRACE(0x400,pParse,p,("After all FROM-clause analysis:\n"));
    sqlite3TreeViewSelect(0, p, 0);
  }
#endif

  /* If the query is DISTINCT with an ORDER BY but is not an aggregate, and 
  ** if the select-list is the same as the ORDER BY list, then this query
  ** can be rewritten as a GROUP BY. In other words, this:
  **
  **     SELECT DISTINCT xyz FROM ... ORDER BY xyz
  **
  ** is transformed to:
  **
  **     SELECT xyz FROM ... GROUP BY xyz ORDER BY xyz
  **
  ** The second form is preferred as a single index (or temp-table) may be 
  ** used for both the ORDER BY and DISTINCT processing. As originally 
  ** written the query must use a temp-table for at least one of the ORDER 
  ** BY and DISTINCT, and an index or separate temp-table for the other.
  */
  if( (p->selFlags & (SF_Distinct|SF_Aggregate))==SF_Distinct 
   && sqlite3ExprListCompare(sSort.pOrderBy, pEList, -1)==0
#ifndef SQLITE_OMIT_WINDOWFUNC
   && p->pWin==0
#endif
  ){
    p->selFlags &= ~SF_Distinct;
    pGroupBy = p->pGroupBy = sqlite3ExprListDup(db, pEList, 0);
    p->selFlags |= SF_Aggregate;
    /* Notice that even thought SF_Distinct has been cleared from p->selFlags,
    ** the sDistinct.isTnct is still set.  Hence, isTnct represents the
    ** original setting of the SF_Distinct flag, not the current setting */
    assert( sDistinct.isTnct );
    sDistinct.isTnct = 2;

#if TREETRACE_ENABLED
    if( sqlite3TreeTrace & 0x400 ){
      SELECTTRACE(0x400,pParse,p,("Transform DISTINCT into GROUP BY:\n"));
      sqlite3TreeViewSelect(0, p, 0);
    }
#endif
  }

  /* If there is an ORDER BY clause, then create an ephemeral index to
  ** do the sorting.  But this sorting ephemeral index might end up
  ** being unused if the data can be extracted in pre-sorted order.
  ** If that is the case, then the OP_OpenEphemeral instruction will be
  ** changed to an OP_Noop once we figure out that the sorting index is
  ** not needed.  The sSort.addrSortIndex variable is used to facilitate
  ** that change.
  */
  if( sSort.pOrderBy ){
    KeyInfo *pKeyInfo;
    pKeyInfo = sqlite3KeyInfoFromExprList(
        pParse, sSort.pOrderBy, 0, pEList->nExpr);
    sSort.iECursor = pParse->nTab++;
    sSort.addrSortIndex =
      sqlite3VdbeAddOp4(v, OP_OpenEphemeral,
          sSort.iECursor, sSort.pOrderBy->nExpr+1+pEList->nExpr, 0,
          (char*)pKeyInfo, P4_KEYINFO
      );
  }else{
    sSort.addrSortIndex = -1;
  }

  /* If the output is destined for a temporary table, open that table.
  */
  if( pDest->eDest==SRT_EphemTab ){
    sqlite3VdbeAddOp2(v, OP_OpenEphemeral, pDest->iSDParm, pEList->nExpr);
    if( p->selFlags & SF_NestedFrom ){
      /* Delete or NULL-out result columns that will never be used */
      int ii;
      for(ii=pEList->nExpr-1; ii>0 && pEList->a[ii].fg.bUsed==0; ii--){
        sqlite3ExprDelete(db, pEList->a[ii].pExpr);
        sqlite3DbFree(db, pEList->a[ii].zEName);
        pEList->nExpr--;
      }
      for(ii=0; ii<pEList->nExpr; ii++){
        if( pEList->a[ii].fg.bUsed==0 ) pEList->a[ii].pExpr->op = TK_NULL;
      }
    }
  }

  /* Set the limiter.
  */
  iEnd = sqlite3VdbeMakeLabel(pParse);
  if( (p->selFlags & SF_FixedLimit)==0 ){
    p->nSelectRow = 320;  /* 4 billion rows */
  }
  if( p->pLimit ) computeLimitRegisters(pParse, p, iEnd);
  if( p->iLimit==0 && sSort.addrSortIndex>=0 ){
    sqlite3VdbeChangeOpcode(v, sSort.addrSortIndex, OP_SorterOpen);
    sSort.sortFlags |= SORTFLAG_UseSorter;
  }

  /* Open an ephemeral index to use for the distinct set.
  */
  if( p->selFlags & SF_Distinct ){
    sDistinct.tabTnct = pParse->nTab++;
    sDistinct.addrTnct = sqlite3VdbeAddOp4(v, OP_OpenEphemeral,
                       sDistinct.tabTnct, 0, 0,
                       (char*)sqlite3KeyInfoFromExprList(pParse, p->pEList,0,0),
                       P4_KEYINFO);
    sqlite3VdbeChangeP5(v, BTREE_UNORDERED);
    sDistinct.eTnctType = WHERE_DISTINCT_UNORDERED;
  }else{
    sDistinct.eTnctType = WHERE_DISTINCT_NOOP;
  }

  if( !isAgg && pGroupBy==0 ){
    /* No aggregate functions and no GROUP BY clause */
    u16 wctrlFlags = (sDistinct.isTnct ? WHERE_WANT_DISTINCT : 0)
                   | (p->selFlags & SF_FixedLimit);
#ifndef SQLITE_OMIT_WINDOWFUNC
    Window *pWin = p->pWin;      /* Main window object (or NULL) */
    if( pWin ){
      sqlite3WindowCodeInit(pParse, p);
    }
#endif
    assert( WHERE_USE_LIMIT==SF_FixedLimit );


    /* Begin the database scan. */
    SELECTTRACE(1,pParse,p,("WhereBegin\n"));
    pWInfo = sqlite3WhereBegin(pParse, pTabList, pWhere, sSort.pOrderBy,
                               p->pEList, p, wctrlFlags, p->nSelectRow);
    if( pWInfo==0 ) goto select_end;
    if( sqlite3WhereOutputRowCount(pWInfo) < p->nSelectRow ){
      p->nSelectRow = sqlite3WhereOutputRowCount(pWInfo);
    }
    if( sDistinct.isTnct && sqlite3WhereIsDistinct(pWInfo) ){
      sDistinct.eTnctType = sqlite3WhereIsDistinct(pWInfo);
    }
    if( sSort.pOrderBy ){
      sSort.nOBSat = sqlite3WhereIsOrdered(pWInfo);
      sSort.labelOBLopt = sqlite3WhereOrderByLimitOptLabel(pWInfo);
      if( sSort.nOBSat==sSort.pOrderBy->nExpr ){
        sSort.pOrderBy = 0;
      }
    }
    SELECTTRACE(1,pParse,p,("WhereBegin returns\n"));

    /* If sorting index that was created by a prior OP_OpenEphemeral 
    ** instruction ended up not being needed, then change the OP_OpenEphemeral
    ** into an OP_Noop.
    */
    if( sSort.addrSortIndex>=0 && sSort.pOrderBy==0 ){
      sqlite3VdbeChangeToNoop(v, sSort.addrSortIndex);
    }

    assert( p->pEList==pEList );
#ifndef SQLITE_OMIT_WINDOWFUNC
    if( pWin ){
      int addrGosub = sqlite3VdbeMakeLabel(pParse);
      int iCont = sqlite3VdbeMakeLabel(pParse);
      int iBreak = sqlite3VdbeMakeLabel(pParse);
      int regGosub = ++pParse->nMem;

      sqlite3WindowCodeStep(pParse, p, pWInfo, regGosub, addrGosub);

      sqlite3VdbeAddOp2(v, OP_Goto, 0, iBreak);
      sqlite3VdbeResolveLabel(v, addrGosub);
      VdbeNoopComment((v, "inner-loop subroutine"));
      sSort.labelOBLopt = 0;
      selectInnerLoop(pParse, p, -1, &sSort, &sDistinct, pDest, iCont, iBreak);
      sqlite3VdbeResolveLabel(v, iCont);
      sqlite3VdbeAddOp1(v, OP_Return, regGosub);
      VdbeComment((v, "end inner-loop subroutine"));
      sqlite3VdbeResolveLabel(v, iBreak);
    }else
#endif /* SQLITE_OMIT_WINDOWFUNC */
    {
      /* Use the standard inner loop. */
      selectInnerLoop(pParse, p, -1, &sSort, &sDistinct, pDest,
          sqlite3WhereContinueLabel(pWInfo),
          sqlite3WhereBreakLabel(pWInfo));

      /* End the database scan loop.
      */
      SELECTTRACE(1,pParse,p,("WhereEnd\n"));
      sqlite3WhereEnd(pWInfo);
    }
  }else{
    /* This case when there exist aggregate functions or a GROUP BY clause
    ** or both */
    NameContext sNC;    /* Name context for processing aggregate information */
    int iAMem;          /* First Mem address for storing current GROUP BY */
    int iBMem;          /* First Mem address for previous GROUP BY */
    int iUseFlag;       /* Mem address holding flag indicating that at least
                        ** one row of the input to the aggregator has been
                        ** processed */
    int iAbortFlag;     /* Mem address which causes query abort if positive */
    int groupBySort;    /* Rows come from source in GROUP BY order */
    int addrEnd;        /* End of processing for this SELECT */
    int sortPTab = 0;   /* Pseudotable used to decode sorting results */
    int sortOut = 0;    /* Output register from the sorter */
    int orderByGrp = 0; /* True if the GROUP BY and ORDER BY are the same */

    /* Remove any and all aliases between the result set and the
    ** GROUP BY clause.
    */
    if( pGroupBy ){
      int k;                        /* Loop counter */
      struct ExprList_item *pItem;  /* For looping over expression in a list */

      for(k=p->pEList->nExpr, pItem=p->pEList->a; k>0; k--, pItem++){
        pItem->u.x.iAlias = 0;
      }
      for(k=pGroupBy->nExpr, pItem=pGroupBy->a; k>0; k--, pItem++){
        pItem->u.x.iAlias = 0;
      }
      assert( 66==sqlite3LogEst(100) );
      if( p->nSelectRow>66 ) p->nSelectRow = 66;

      /* If there is both a GROUP BY and an ORDER BY clause and they are
      ** identical, then it may be possible to disable the ORDER BY clause 
      ** on the grounds that the GROUP BY will cause elements to come out 
      ** in the correct order. It also may not - the GROUP BY might use a
      ** database index that causes rows to be grouped together as required
      ** but not actually sorted. Either way, record the fact that the
      ** ORDER BY and GROUP BY clauses are the same by setting the orderByGrp
      ** variable.  */
      if( sSort.pOrderBy && pGroupBy->nExpr==sSort.pOrderBy->nExpr ){
        int ii;
        /* The GROUP BY processing doesn't care whether rows are delivered in
        ** ASC or DESC order - only that each group is returned contiguously.
        ** So set the ASC/DESC flags in the GROUP BY to match those in the 
        ** ORDER BY to maximize the chances of rows being delivered in an 
        ** order that makes the ORDER BY redundant.  */
        for(ii=0; ii<pGroupBy->nExpr; ii++){
          u8 sortFlags;
          sortFlags = sSort.pOrderBy->a[ii].fg.sortFlags & KEYINFO_ORDER_DESC;
          pGroupBy->a[ii].fg.sortFlags = sortFlags;
        }
        if( sqlite3ExprListCompare(pGroupBy, sSort.pOrderBy, -1)==0 ){
          orderByGrp = 1;
        }
      }
    }else{
      assert( 0==sqlite3LogEst(1) );
      p->nSelectRow = 0;
    }

    /* Create a label to jump to when we want to abort the query */
    addrEnd = sqlite3VdbeMakeLabel(pParse);

    /* Convert TK_COLUMN nodes into TK_AGG_COLUMN and make entries in
    ** sAggInfo for all TK_AGG_FUNCTION nodes in expressions of the
    ** SELECT statement.
    */
    pAggInfo = sqlite3DbMallocZero(db, sizeof(*pAggInfo) );
    if( pAggInfo ){
      sqlite3ParserAddCleanup(pParse,
          (void(*)(sqlite3*,void*))agginfoFree, pAggInfo);
      testcase( pParse->earlyCleanup );
    }
    if( db->mallocFailed ){
      goto select_end;
    }
    pAggInfo->selId = p->selId;
    memset(&sNC, 0, sizeof(sNC));
    sNC.pParse = pParse;
    sNC.pSrcList = pTabList;
    sNC.uNC.pAggInfo = pAggInfo;
    VVA_ONLY( sNC.ncFlags = NC_UAggInfo; )
    pAggInfo->mnReg = pParse->nMem+1;
    pAggInfo->nSortingColumn = pGroupBy ? pGroupBy->nExpr : 0;
    pAggInfo->pGroupBy = pGroupBy;
    sqlite3ExprAnalyzeAggList(&sNC, pEList);
    sqlite3ExprAnalyzeAggList(&sNC, sSort.pOrderBy);
    if( pHaving ){
      if( pGroupBy ){
        assert( pWhere==p->pWhere );
        assert( pHaving==p->pHaving );
        assert( pGroupBy==p->pGroupBy );
        havingToWhere(pParse, p);
        pWhere = p->pWhere;
      }
      sqlite3ExprAnalyzeAggregates(&sNC, pHaving);
    }
    pAggInfo->nAccumulator = pAggInfo->nColumn;
    if( p->pGroupBy==0 && p->pHaving==0 && pAggInfo->nFunc==1 ){
      minMaxFlag = minMaxQuery(db, pAggInfo->aFunc[0].pFExpr, &pMinMaxOrderBy);
    }else{
      minMaxFlag = WHERE_ORDERBY_NORMAL;
    }
    for(i=0; i<pAggInfo->nFunc; i++){
      Expr *pExpr = pAggInfo->aFunc[i].pFExpr;
      assert( ExprUseXList(pExpr) );
      sNC.ncFlags |= NC_InAggFunc;
      sqlite3ExprAnalyzeAggList(&sNC, pExpr->x.pList);
#ifndef SQLITE_OMIT_WINDOWFUNC
      assert( !IsWindowFunc(pExpr) );
      if( ExprHasProperty(pExpr, EP_WinFunc) ){
        sqlite3ExprAnalyzeAggregates(&sNC, pExpr->y.pWin->pFilter);
      }
#endif
      sNC.ncFlags &= ~NC_InAggFunc;
    }
    pAggInfo->mxReg = pParse->nMem;
    if( db->mallocFailed ) goto select_end;
#if TREETRACE_ENABLED
    if( sqlite3TreeTrace & 0x400 ){
      int ii;
      SELECTTRACE(0x400,pParse,p,("After aggregate analysis %p:\n", pAggInfo));
      sqlite3TreeViewSelect(0, p, 0);
      if( minMaxFlag ){
        sqlite3DebugPrintf("MIN/MAX Optimization (0x%02x) adds:\n", minMaxFlag);
        sqlite3TreeViewExprList(0, pMinMaxOrderBy, 0, "ORDERBY");
      }
      for(ii=0; ii<pAggInfo->nColumn; ii++){
        struct AggInfo_col *pCol = &pAggInfo->aCol[ii];
        sqlite3DebugPrintf(
           "agg-column[%d] pTab=%s iTable=%d iColumn=%d iMem=%d"
           " iSorterColumn=%d\n",
           ii, pCol->pTab ? pCol->pTab->zName : "NULL", 
           pCol->iTable, pCol->iColumn, pCol->iMem,
           pCol->iSorterColumn);
        sqlite3TreeViewExpr(0, pAggInfo->aCol[ii].pCExpr, 0);
      }
      for(ii=0; ii<pAggInfo->nFunc; ii++){
        sqlite3DebugPrintf("agg-func[%d]: iMem=%d\n",
            ii, pAggInfo->aFunc[ii].iMem);
        sqlite3TreeViewExpr(0, pAggInfo->aFunc[ii].pFExpr, 0);
      }
    }
#endif


    /* Processing for aggregates with GROUP BY is very different and
    ** much more complex than aggregates without a GROUP BY.
    */
    if( pGroupBy ){
      KeyInfo *pKeyInfo;  /* Keying information for the group by clause */
      int addr1;          /* A-vs-B comparision jump */
      int addrOutputRow;  /* Start of subroutine that outputs a result row */
      int regOutputRow;   /* Return address register for output subroutine */
      int addrSetAbort;   /* Set the abort flag and return */
      int addrTopOfLoop;  /* Top of the input loop */
      int addrSortingIdx; /* The OP_OpenEphemeral for the sorting index */
      int addrReset;      /* Subroutine for resetting the accumulator */
      int regReset;       /* Return address register for reset subroutine */
      ExprList *pDistinct = 0;
      u16 distFlag = 0;
      int eDist = WHERE_DISTINCT_NOOP;

      if( pAggInfo->nFunc==1 
       && pAggInfo->aFunc[0].iDistinct>=0
       && ALWAYS(pAggInfo->aFunc[0].pFExpr!=0)
       && ALWAYS(ExprUseXList(pAggInfo->aFunc[0].pFExpr))
       && pAggInfo->aFunc[0].pFExpr->x.pList!=0
      ){
        Expr *pExpr = pAggInfo->aFunc[0].pFExpr->x.pList->a[0].pExpr;
        pExpr = sqlite3ExprDup(db, pExpr, 0);
        pDistinct = sqlite3ExprListDup(db, pGroupBy, 0);
        pDistinct = sqlite3ExprListAppend(pParse, pDistinct, pExpr);
        distFlag = pDistinct ? (WHERE_WANT_DISTINCT|WHERE_AGG_DISTINCT) : 0;
      }

      /* If there is a GROUP BY clause we might need a sorting index to
      ** implement it.  Allocate that sorting index now.  If it turns out
      ** that we do not need it after all, the OP_SorterOpen instruction
      ** will be converted into a Noop.  
      */
      pAggInfo->sortingIdx = pParse->nTab++;
      pKeyInfo = sqlite3KeyInfoFromExprList(pParse, pGroupBy,
                                            0, pAggInfo->nColumn);
      addrSortingIdx = sqlite3VdbeAddOp4(v, OP_SorterOpen, 
          pAggInfo->sortingIdx, pAggInfo->nSortingColumn, 
          0, (char*)pKeyInfo, P4_KEYINFO);

      /* Initialize memory locations used by GROUP BY aggregate processing
      */
      iUseFlag = ++pParse->nMem;
      iAbortFlag = ++pParse->nMem;
      regOutputRow = ++pParse->nMem;
      addrOutputRow = sqlite3VdbeMakeLabel(pParse);
      regReset = ++pParse->nMem;
      addrReset = sqlite3VdbeMakeLabel(pParse);
      iAMem = pParse->nMem + 1;
      pParse->nMem += pGroupBy->nExpr;
      iBMem = pParse->nMem + 1;
      pParse->nMem += pGroupBy->nExpr;
      sqlite3VdbeAddOp2(v, OP_Integer, 0, iAbortFlag);
      VdbeComment((v, "clear abort flag"));
      sqlite3VdbeAddOp3(v, OP_Null, 0, iAMem, iAMem+pGroupBy->nExpr-1);

      /* Begin a loop that will extract all source rows in GROUP BY order.
      ** This might involve two separate loops with an OP_Sort in between, or
      ** it might be a single loop that uses an index to extract information
      ** in the right order to begin with.
      */
      sqlite3VdbeAddOp2(v, OP_Gosub, regReset, addrReset);
      SELECTTRACE(1,pParse,p,("WhereBegin\n"));
      pWInfo = sqlite3WhereBegin(pParse, pTabList, pWhere, pGroupBy, pDistinct,
          p, (sDistinct.isTnct==2 ? WHERE_DISTINCTBY : WHERE_GROUPBY) 
          |  (orderByGrp ? WHERE_SORTBYGROUP : 0) | distFlag, 0
      );
      if( pWInfo==0 ){
        sqlite3ExprListDelete(db, pDistinct);
        goto select_end;
      }
      eDist = sqlite3WhereIsDistinct(pWInfo);
      SELECTTRACE(1,pParse,p,("WhereBegin returns\n"));
      if( sqlite3WhereIsOrdered(pWInfo)==pGroupBy->nExpr ){
        /* The optimizer is able to deliver rows in group by order so
        ** we do not have to sort.  The OP_OpenEphemeral table will be
        ** cancelled later because we still need to use the pKeyInfo
        */
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

        explainTempTable(pParse, 
            (sDistinct.isTnct && (p->selFlags&SF_Distinct)==0) ?
                    "DISTINCT" : "GROUP BY");

        groupBySort = 1;
        nGroupBy = pGroupBy->nExpr;
        nCol = nGroupBy;
        j = nGroupBy;
        for(i=0; i<pAggInfo->nColumn; i++){
          if( pAggInfo->aCol[i].iSorterColumn>=j ){
            nCol++;
            j++;
          }
        }
        regBase = sqlite3GetTempRange(pParse, nCol);
        sqlite3ExprCodeExprList(pParse, pGroupBy, regBase, 0, 0);
        j = nGroupBy;
        pAggInfo->directMode = 1;
        for(i=0; i<pAggInfo->nColumn; i++){
          struct AggInfo_col *pCol = &pAggInfo->aCol[i];
          if( pCol->iSorterColumn>=j ){
            sqlite3ExprCode(pParse, pCol->pCExpr, j + regBase);
            j++;
          }
        }
        pAggInfo->directMode = 0;
        regRecord = sqlite3GetTempReg(pParse);
        sqlite3VdbeAddOp3(v, OP_MakeRecord, regBase, nCol, regRecord);
        sqlite3VdbeAddOp2(v, OP_SorterInsert, pAggInfo->sortingIdx, regRecord);
        sqlite3ReleaseTempReg(pParse, regRecord);
        sqlite3ReleaseTempRange(pParse, regBase, nCol);
        SELECTTRACE(1,pParse,p,("WhereEnd\n"));
        sqlite3WhereEnd(pWInfo);
        pAggInfo->sortingIdxPTab = sortPTab = pParse->nTab++;
        sortOut = sqlite3GetTempReg(pParse);
        sqlite3VdbeAddOp3(v, OP_OpenPseudo, sortPTab, sortOut, nCol);
        sqlite3VdbeAddOp2(v, OP_SorterSort, pAggInfo->sortingIdx, addrEnd);
        VdbeComment((v, "GROUP BY sort")); VdbeCoverage(v);
        pAggInfo->useSortingIdx = 1;
      }

      /* If the index or temporary table used by the GROUP BY sort
      ** will naturally deliver rows in the order required by the ORDER BY
      ** clause, cancel the ephemeral table open coded earlier.
      **
      ** This is an optimization - the correct answer should result regardless.
      ** Use the SQLITE_GroupByOrder flag with SQLITE_TESTCTRL_OPTIMIZER to 
      ** disable this optimization for testing purposes.  */
      if( orderByGrp && OptimizationEnabled(db, SQLITE_GroupByOrder) 
       && (groupBySort || sqlite3WhereIsSorted(pWInfo))
      ){
        sSort.pOrderBy = 0;
        sqlite3VdbeChangeToNoop(v, sSort.addrSortIndex);
      }

      /* Evaluate the current GROUP BY terms and store in b0, b1, b2...
      ** (b0 is memory location iBMem+0, b1 is iBMem+1, and so forth)
      ** Then compare the current GROUP BY terms against the GROUP BY terms
      ** from the previous row currently stored in a0, a1, a2...
      */
      addrTopOfLoop = sqlite3VdbeCurrentAddr(v);
      if( groupBySort ){
        sqlite3VdbeAddOp3(v, OP_SorterData, pAggInfo->sortingIdx,
                          sortOut, sortPTab);
      }
      for(j=0; j<pGroupBy->nExpr; j++){
        if( groupBySort ){
          sqlite3VdbeAddOp3(v, OP_Column, sortPTab, j, iBMem+j);
        }else{
          pAggInfo->directMode = 1;
          sqlite3ExprCode(pParse, pGroupBy->a[j].pExpr, iBMem+j);
        }
      }
      sqlite3VdbeAddOp4(v, OP_Compare, iAMem, iBMem, pGroupBy->nExpr,
                          (char*)sqlite3KeyInfoRef(pKeyInfo), P4_KEYINFO);
      addr1 = sqlite3VdbeCurrentAddr(v);
      sqlite3VdbeAddOp3(v, OP_Jump, addr1+1, 0, addr1+1); VdbeCoverage(v);

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
      sqlite3VdbeAddOp2(v, OP_IfPos, iAbortFlag, addrEnd); VdbeCoverage(v);
      VdbeComment((v, "check abort flag"));
      sqlite3VdbeAddOp2(v, OP_Gosub, regReset, addrReset);
      VdbeComment((v, "reset accumulator"));

      /* Update the aggregate accumulators based on the content of
      ** the current row
      */
      sqlite3VdbeJumpHere(v, addr1);
      updateAccumulator(pParse, iUseFlag, pAggInfo, eDist);
      sqlite3VdbeAddOp2(v, OP_Integer, 1, iUseFlag);
      VdbeComment((v, "indicate data in accumulator"));

      /* End of the loop
      */
      if( groupBySort ){
        sqlite3VdbeAddOp2(v, OP_SorterNext, pAggInfo->sortingIdx,addrTopOfLoop);
        VdbeCoverage(v);
      }else{
        SELECTTRACE(1,pParse,p,("WhereEnd\n"));
        sqlite3WhereEnd(pWInfo);
        sqlite3VdbeChangeToNoop(v, addrSortingIdx);
      }
      sqlite3ExprListDelete(db, pDistinct);

      /* Output the final row of result
      */
      sqlite3VdbeAddOp2(v, OP_Gosub, regOutputRow, addrOutputRow);
      VdbeComment((v, "output final row"));

      /* Jump over the subroutines
      */
      sqlite3VdbeGoto(v, addrEnd);

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
      VdbeCoverage(v);
      VdbeComment((v, "Groupby result generator entry point"));
      sqlite3VdbeAddOp1(v, OP_Return, regOutputRow);
      finalizeAggFunctions(pParse, pAggInfo);
      sqlite3ExprIfFalse(pParse, pHaving, addrOutputRow+1, SQLITE_JUMPIFNULL);
      selectInnerLoop(pParse, p, -1, &sSort,
                      &sDistinct, pDest,
                      addrOutputRow+1, addrSetAbort);
      sqlite3VdbeAddOp1(v, OP_Return, regOutputRow);
      VdbeComment((v, "end groupby result generator"));

      /* Generate a subroutine that will reset the group-by accumulator
      */
      sqlite3VdbeResolveLabel(v, addrReset);
      resetAccumulator(pParse, pAggInfo);
      sqlite3VdbeAddOp2(v, OP_Integer, 0, iUseFlag);
      VdbeComment((v, "indicate accumulator empty"));
      sqlite3VdbeAddOp1(v, OP_Return, regReset);

      if( distFlag!=0 && eDist!=WHERE_DISTINCT_NOOP ){
        struct AggInfo_func *pF = &pAggInfo->aFunc[0];
        fixDistinctOpenEph(pParse, eDist, pF->iDistinct, pF->iDistAddr);
      }
    } /* endif pGroupBy.  Begin aggregate queries without GROUP BY: */
    else {
      Table *pTab;
      if( (pTab = isSimpleCount(p, pAggInfo))!=0 ){
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
        Pgno iRoot = pTab->tnum;             /* Root page of scanned b-tree */

        sqlite3CodeVerifySchema(pParse, iDb);
        sqlite3TableLock(pParse, iDb, pTab->tnum, 0, pTab->zName);

        /* Search for the index that has the lowest scan cost.
        **
        ** (2011-04-15) Do not do a full scan of an unordered index.
        **
        ** (2013-10-03) Do not count the entries in a partial index.
        **
        ** In practice the KeyInfo structure will not be used. It is only 
        ** passed to keep OP_OpenRead happy.
        */
        if( !HasRowid(pTab) ) pBest = sqlite3PrimaryKeyIndex(pTab);
        if( !p->pSrc->a[0].fg.notIndexed ){
          for(pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext){
            if( pIdx->bUnordered==0
             && pIdx->szIdxRow<pTab->szTabRow
             && pIdx->pPartIdxWhere==0
             && (!pBest || pIdx->szIdxRow<pBest->szIdxRow)
            ){
              pBest = pIdx;
            }
          }
        }
        if( pBest ){
          iRoot = pBest->tnum;
          pKeyInfo = sqlite3KeyInfoOfIndex(pParse, pBest);
        }

        /* Open a read-only cursor, execute the OP_Count, close the cursor. */
        sqlite3VdbeAddOp4Int(v, OP_OpenRead, iCsr, (int)iRoot, iDb, 1);
        if( pKeyInfo ){
          sqlite3VdbeChangeP4(v, -1, (char *)pKeyInfo, P4_KEYINFO);
        }
        sqlite3VdbeAddOp2(v, OP_Count, iCsr, pAggInfo->aFunc[0].iMem);
        sqlite3VdbeAddOp1(v, OP_Close, iCsr);
        explainSimpleCount(pParse, pTab, pBest);
      }else{
        int regAcc = 0;           /* "populate accumulators" flag */
        ExprList *pDistinct = 0;
        u16 distFlag = 0;
        int eDist;

        /* If there are accumulator registers but no min() or max() functions
        ** without FILTER clauses, allocate register regAcc. Register regAcc
        ** will contain 0 the first time the inner loop runs, and 1 thereafter.
        ** The code generated by updateAccumulator() uses this to ensure
        ** that the accumulator registers are (a) updated only once if
        ** there are no min() or max functions or (b) always updated for the
        ** first row visited by the aggregate, so that they are updated at
        ** least once even if the FILTER clause means the min() or max() 
        ** function visits zero rows.  */
        if( pAggInfo->nAccumulator ){
          for(i=0; i<pAggInfo->nFunc; i++){
            if( ExprHasProperty(pAggInfo->aFunc[i].pFExpr, EP_WinFunc) ){
              continue;
            }
            if( pAggInfo->aFunc[i].pFunc->funcFlags&SQLITE_FUNC_NEEDCOLL ){
              break;
            }
          }
          if( i==pAggInfo->nFunc ){
            regAcc = ++pParse->nMem;
            sqlite3VdbeAddOp2(v, OP_Integer, 0, regAcc);
          }
        }else if( pAggInfo->nFunc==1 && pAggInfo->aFunc[0].iDistinct>=0 ){
          assert( ExprUseXList(pAggInfo->aFunc[0].pFExpr) );
          pDistinct = pAggInfo->aFunc[0].pFExpr->x.pList;
          distFlag = pDistinct ? (WHERE_WANT_DISTINCT|WHERE_AGG_DISTINCT) : 0;
        }

        /* This case runs if the aggregate has no GROUP BY clause.  The
        ** processing is much simpler since there is only a single row
        ** of output.
        */
        assert( p->pGroupBy==0 );
        resetAccumulator(pParse, pAggInfo);

        /* If this query is a candidate for the min/max optimization, then
        ** minMaxFlag will have been previously set to either
        ** WHERE_ORDERBY_MIN or WHERE_ORDERBY_MAX and pMinMaxOrderBy will
        ** be an appropriate ORDER BY expression for the optimization.
        */
        assert( minMaxFlag==WHERE_ORDERBY_NORMAL || pMinMaxOrderBy!=0 );
        assert( pMinMaxOrderBy==0 || pMinMaxOrderBy->nExpr==1 );

        SELECTTRACE(1,pParse,p,("WhereBegin\n"));
        pWInfo = sqlite3WhereBegin(pParse, pTabList, pWhere, pMinMaxOrderBy,
                                   pDistinct, p, minMaxFlag|distFlag, 0);
        if( pWInfo==0 ){
          goto select_end;
        }
        SELECTTRACE(1,pParse,p,("WhereBegin returns\n"));
        eDist = sqlite3WhereIsDistinct(pWInfo);
        updateAccumulator(pParse, regAcc, pAggInfo, eDist);
        if( eDist!=WHERE_DISTINCT_NOOP ){
          struct AggInfo_func *pF = pAggInfo->aFunc;
          if( pF ){
            fixDistinctOpenEph(pParse, eDist, pF->iDistinct, pF->iDistAddr);
          }
        }

        if( regAcc ) sqlite3VdbeAddOp2(v, OP_Integer, 1, regAcc);
        if( minMaxFlag ){
          sqlite3WhereMinMaxOptEarlyOut(v, pWInfo);
        }
        SELECTTRACE(1,pParse,p,("WhereEnd\n"));
        sqlite3WhereEnd(pWInfo);
        finalizeAggFunctions(pParse, pAggInfo);
      }

      sSort.pOrderBy = 0;
      sqlite3ExprIfFalse(pParse, pHaving, addrEnd, SQLITE_JUMPIFNULL);
      selectInnerLoop(pParse, p, -1, 0, 0, 
                      pDest, addrEnd, addrEnd);
    }
    sqlite3VdbeResolveLabel(v, addrEnd);
    
  } /* endif aggregate query */

  if( sDistinct.eTnctType==WHERE_DISTINCT_UNORDERED ){
    explainTempTable(pParse, "DISTINCT");
  }

  /* If there is an ORDER BY clause, then we need to sort the results
  ** and send them to the callback one by one.
  */
  if( sSort.pOrderBy ){
    explainTempTable(pParse,
                     sSort.nOBSat>0 ? "RIGHT PART OF ORDER BY":"ORDER BY");
    assert( p->pEList==pEList );
    generateSortTail(pParse, p, &sSort, pEList->nExpr, pDest);
  }

  /* Jump here to skip this query
  */
  sqlite3VdbeResolveLabel(v, iEnd);

  /* The SELECT has been coded. If there is an error in the Parse structure,
  ** set the return code to 1. Otherwise 0. */
  rc = (pParse->nErr>0);

  /* Control jumps to here if an error is encountered above, or upon
  ** successful coding of the SELECT.
  */
select_end:
  assert( db->mallocFailed==0 || db->mallocFailed==1 );
  assert( db->mallocFailed==0 || pParse->nErr!=0 );
  sqlite3ExprListDelete(db, pMinMaxOrderBy);
#ifdef SQLITE_DEBUG
  if( pAggInfo && !db->mallocFailed ){
    for(i=0; i<pAggInfo->nColumn; i++){
      Expr *pExpr = pAggInfo->aCol[i].pCExpr;
      assert( pExpr!=0 );
      assert( pExpr->pAggInfo==pAggInfo );
      assert( pExpr->iAgg==i );
    }
    for(i=0; i<pAggInfo->nFunc; i++){
      Expr *pExpr = pAggInfo->aFunc[i].pFExpr;
      assert( pExpr!=0 );
      assert( pExpr->pAggInfo==pAggInfo );
      assert( pExpr->iAgg==i );
    }
  }
#endif

#if TREETRACE_ENABLED
  SELECTTRACE(0x1,pParse,p,("end processing\n"));
  if( (sqlite3TreeTrace & 0x2000)!=0 && ExplainQueryPlanParent(pParse)==0 ){
    sqlite3TreeViewSelect(0, p, 0);
  }
#endif
  ExplainQueryPlanPop(pParse);
  return rc;
}
