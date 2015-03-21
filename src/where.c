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
** This module contains C code that generates VDBE code used to process
** the WHERE clause of SQL statements.  This module is responsible for
** generating the code that loops through a table looking for applicable
** rows.  Indices are selected and used to speed the search when doing
** so is applicable.  Because this module is responsible for selecting
** indices, you might also think of this module as the "query optimizer".
*/
#include "sqliteInt.h"
#include "whereInt.h"

/*
** Return the estimated number of output rows from a WHERE clause
*/
u64 sqlite3WhereOutputRowCount(WhereInfo *pWInfo){
  return sqlite3LogEstToInt(pWInfo->nRowOut);
}

/*
** Return one of the WHERE_DISTINCT_xxxxx values to indicate how this
** WHERE clause returns outputs for DISTINCT processing.
*/
int sqlite3WhereIsDistinct(WhereInfo *pWInfo){
  return pWInfo->eDistinct;
}

/*
** Return TRUE if the WHERE clause returns rows in ORDER BY order.
** Return FALSE if the output needs to be sorted.
*/
int sqlite3WhereIsOrdered(WhereInfo *pWInfo){
  return pWInfo->nOBSat;
}

/*
** Return the VDBE address or label to jump to in order to continue
** immediately with the next row of a WHERE clause.
*/
int sqlite3WhereContinueLabel(WhereInfo *pWInfo){
  assert( pWInfo->iContinue!=0 );
  return pWInfo->iContinue;
}

/*
** Return the VDBE address or label to jump to in order to break
** out of a WHERE loop.
*/
int sqlite3WhereBreakLabel(WhereInfo *pWInfo){
  return pWInfo->iBreak;
}

/*
** Return TRUE if an UPDATE or DELETE statement can operate directly on
** the rowids returned by a WHERE clause.  Return FALSE if doing an
** UPDATE or DELETE might change subsequent WHERE clause results.
**
** If the ONEPASS optimization is used (if this routine returns true)
** then also write the indices of open cursors used by ONEPASS
** into aiCur[0] and aiCur[1].  iaCur[0] gets the cursor of the data
** table and iaCur[1] gets the cursor used by an auxiliary index.
** Either value may be -1, indicating that cursor is not used.
** Any cursors returned will have been opened for writing.
**
** aiCur[0] and aiCur[1] both get -1 if the where-clause logic is
** unable to use the ONEPASS optimization.
*/
int sqlite3WhereOkOnePass(WhereInfo *pWInfo, int *aiCur){
  memcpy(aiCur, pWInfo->aiCurOnePass, sizeof(int)*2);
  return pWInfo->okOnePass;
}

/*
** Move the content of pSrc into pDest
*/
static void whereOrMove(WhereOrSet *pDest, WhereOrSet *pSrc){
  pDest->n = pSrc->n;
  memcpy(pDest->a, pSrc->a, pDest->n*sizeof(pDest->a[0]));
}

/*
** Try to insert a new prerequisite/cost entry into the WhereOrSet pSet.
**
** The new entry might overwrite an existing entry, or it might be
** appended, or it might be discarded.  Do whatever is the right thing
** so that pSet keeps the N_OR_COST best entries seen so far.
*/
static int whereOrInsert(
  WhereOrSet *pSet,      /* The WhereOrSet to be updated */
  Bitmask prereq,        /* Prerequisites of the new entry */
  LogEst rRun,           /* Run-cost of the new entry */
  LogEst nOut            /* Number of outputs for the new entry */
){
  u16 i;
  WhereOrCost *p;
  for(i=pSet->n, p=pSet->a; i>0; i--, p++){
    if( rRun<=p->rRun && (prereq & p->prereq)==prereq ){
      goto whereOrInsert_done;
    }
    if( p->rRun<=rRun && (p->prereq & prereq)==p->prereq ){
      return 0;
    }
  }
  if( pSet->n<N_OR_COST ){
    p = &pSet->a[pSet->n++];
    p->nOut = nOut;
  }else{
    p = pSet->a;
    for(i=1; i<pSet->n; i++){
      if( p->rRun>pSet->a[i].rRun ) p = pSet->a + i;
    }
    if( p->rRun<=rRun ) return 0;
  }
whereOrInsert_done:
  p->prereq = prereq;
  p->rRun = rRun;
  if( p->nOut>nOut ) p->nOut = nOut;
  return 1;
}

/*
** Initialize a preallocated WhereClause structure.
*/
static void whereClauseInit(
  WhereClause *pWC,        /* The WhereClause to be initialized */
  WhereInfo *pWInfo        /* The WHERE processing context */
){
  pWC->pWInfo = pWInfo;
  pWC->pOuter = 0;
  pWC->nTerm = 0;
  pWC->nSlot = ArraySize(pWC->aStatic);
  pWC->a = pWC->aStatic;
}

/* Forward reference */
static void whereClauseClear(WhereClause*);

/*
** Deallocate all memory associated with a WhereOrInfo object.
*/
static void whereOrInfoDelete(sqlite3 *db, WhereOrInfo *p){
  whereClauseClear(&p->wc);
  sqlite3DbFree(db, p);
}

/*
** Deallocate all memory associated with a WhereAndInfo object.
*/
static void whereAndInfoDelete(sqlite3 *db, WhereAndInfo *p){
  whereClauseClear(&p->wc);
  sqlite3DbFree(db, p);
}

/*
** Deallocate a WhereClause structure.  The WhereClause structure
** itself is not freed.  This routine is the inverse of whereClauseInit().
*/
static void whereClauseClear(WhereClause *pWC){
  int i;
  WhereTerm *a;
  sqlite3 *db = pWC->pWInfo->pParse->db;
  for(i=pWC->nTerm-1, a=pWC->a; i>=0; i--, a++){
    if( a->wtFlags & TERM_DYNAMIC ){
      sqlite3ExprDelete(db, a->pExpr);
    }
    if( a->wtFlags & TERM_ORINFO ){
      whereOrInfoDelete(db, a->u.pOrInfo);
    }else if( a->wtFlags & TERM_ANDINFO ){
      whereAndInfoDelete(db, a->u.pAndInfo);
    }
  }
  if( pWC->a!=pWC->aStatic ){
    sqlite3DbFree(db, pWC->a);
  }
}

/*
** Add a single new WhereTerm entry to the WhereClause object pWC.
** The new WhereTerm object is constructed from Expr p and with wtFlags.
** The index in pWC->a[] of the new WhereTerm is returned on success.
** 0 is returned if the new WhereTerm could not be added due to a memory
** allocation error.  The memory allocation failure will be recorded in
** the db->mallocFailed flag so that higher-level functions can detect it.
**
** This routine will increase the size of the pWC->a[] array as necessary.
**
** If the wtFlags argument includes TERM_DYNAMIC, then responsibility
** for freeing the expression p is assumed by the WhereClause object pWC.
** This is true even if this routine fails to allocate a new WhereTerm.
**
** WARNING:  This routine might reallocate the space used to store
** WhereTerms.  All pointers to WhereTerms should be invalidated after
** calling this routine.  Such pointers may be reinitialized by referencing
** the pWC->a[] array.
*/
static int whereClauseInsert(WhereClause *pWC, Expr *p, u16 wtFlags){
  WhereTerm *pTerm;
  int idx;
  testcase( wtFlags & TERM_VIRTUAL );
  if( pWC->nTerm>=pWC->nSlot ){
    WhereTerm *pOld = pWC->a;
    sqlite3 *db = pWC->pWInfo->pParse->db;
    pWC->a = sqlite3DbMallocRaw(db, sizeof(pWC->a[0])*pWC->nSlot*2 );
    if( pWC->a==0 ){
      if( wtFlags & TERM_DYNAMIC ){
        sqlite3ExprDelete(db, p);
      }
      pWC->a = pOld;
      return 0;
    }
    memcpy(pWC->a, pOld, sizeof(pWC->a[0])*pWC->nTerm);
    if( pOld!=pWC->aStatic ){
      sqlite3DbFree(db, pOld);
    }
    pWC->nSlot = sqlite3DbMallocSize(db, pWC->a)/sizeof(pWC->a[0]);
    memset(&pWC->a[pWC->nTerm], 0, sizeof(pWC->a[0])*(pWC->nSlot-pWC->nTerm));
  }
  pTerm = &pWC->a[idx = pWC->nTerm++];
  if( p && ExprHasProperty(p, EP_Unlikely) ){
    pTerm->truthProb = sqlite3LogEst(p->iTable) - 270;
  }else{
    pTerm->truthProb = 1;
  }
  pTerm->pExpr = sqlite3ExprSkipCollate(p);
  pTerm->wtFlags = wtFlags;
  pTerm->pWC = pWC;
  pTerm->iParent = -1;
  return idx;
}

/*
** This routine identifies subexpressions in the WHERE clause where
** each subexpression is separated by the AND operator or some other
** operator specified in the op parameter.  The WhereClause structure
** is filled with pointers to subexpressions.  For example:
**
**    WHERE  a=='hello' AND coalesce(b,11)<10 AND (c+12!=d OR c==22)
**           \________/     \_______________/     \________________/
**            slot[0]            slot[1]               slot[2]
**
** The original WHERE clause in pExpr is unaltered.  All this routine
** does is make slot[] entries point to substructure within pExpr.
**
** In the previous sentence and in the diagram, "slot[]" refers to
** the WhereClause.a[] array.  The slot[] array grows as needed to contain
** all terms of the WHERE clause.
*/
static void whereSplit(WhereClause *pWC, Expr *pExpr, u8 op){
  pWC->op = op;
  if( pExpr==0 ) return;
  if( pExpr->op!=op ){
    whereClauseInsert(pWC, pExpr, 0);
  }else{
    whereSplit(pWC, pExpr->pLeft, op);
    whereSplit(pWC, pExpr->pRight, op);
  }
}

/*
** Initialize a WhereMaskSet object
*/
#define initMaskSet(P)  (P)->n=0

/*
** Return the bitmask for the given cursor number.  Return 0 if
** iCursor is not in the set.
*/
static Bitmask getMask(WhereMaskSet *pMaskSet, int iCursor){
  int i;
  assert( pMaskSet->n<=(int)sizeof(Bitmask)*8 );
  for(i=0; i<pMaskSet->n; i++){
    if( pMaskSet->ix[i]==iCursor ){
      return MASKBIT(i);
    }
  }
  return 0;
}

/*
** Create a new mask for cursor iCursor.
**
** There is one cursor per table in the FROM clause.  The number of
** tables in the FROM clause is limited by a test early in the
** sqlite3WhereBegin() routine.  So we know that the pMaskSet->ix[]
** array will never overflow.
*/
static void createMask(WhereMaskSet *pMaskSet, int iCursor){
  assert( pMaskSet->n < ArraySize(pMaskSet->ix) );
  pMaskSet->ix[pMaskSet->n++] = iCursor;
}

/*
** These routines walk (recursively) an expression tree and generate
** a bitmask indicating which tables are used in that expression
** tree.
*/
static Bitmask exprListTableUsage(WhereMaskSet*, ExprList*);
static Bitmask exprSelectTableUsage(WhereMaskSet*, Select*);
static Bitmask exprTableUsage(WhereMaskSet *pMaskSet, Expr *p){
  Bitmask mask = 0;
  if( p==0 ) return 0;
  if( p->op==TK_COLUMN ){
    mask = getMask(pMaskSet, p->iTable);
    return mask;
  }
  mask = exprTableUsage(pMaskSet, p->pRight);
  mask |= exprTableUsage(pMaskSet, p->pLeft);
  if( ExprHasProperty(p, EP_xIsSelect) ){
    mask |= exprSelectTableUsage(pMaskSet, p->x.pSelect);
  }else{
    mask |= exprListTableUsage(pMaskSet, p->x.pList);
  }
  return mask;
}
static Bitmask exprListTableUsage(WhereMaskSet *pMaskSet, ExprList *pList){
  int i;
  Bitmask mask = 0;
  if( pList ){
    for(i=0; i<pList->nExpr; i++){
      mask |= exprTableUsage(pMaskSet, pList->a[i].pExpr);
    }
  }
  return mask;
}
static Bitmask exprSelectTableUsage(WhereMaskSet *pMaskSet, Select *pS){
  Bitmask mask = 0;
  while( pS ){
    SrcList *pSrc = pS->pSrc;
    mask |= exprListTableUsage(pMaskSet, pS->pEList);
    mask |= exprListTableUsage(pMaskSet, pS->pGroupBy);
    mask |= exprListTableUsage(pMaskSet, pS->pOrderBy);
    mask |= exprTableUsage(pMaskSet, pS->pWhere);
    mask |= exprTableUsage(pMaskSet, pS->pHaving);
    if( ALWAYS(pSrc!=0) ){
      int i;
      for(i=0; i<pSrc->nSrc; i++){
        mask |= exprSelectTableUsage(pMaskSet, pSrc->a[i].pSelect);
        mask |= exprTableUsage(pMaskSet, pSrc->a[i].pOn);
      }
    }
    pS = pS->pPrior;
  }
  return mask;
}

/*
** Return TRUE if the given operator is one of the operators that is
** allowed for an indexable WHERE clause term.  The allowed operators are
** "=", "<", ">", "<=", ">=", "IN", and "IS NULL"
*/
static int allowedOp(int op){
  assert( TK_GT>TK_EQ && TK_GT<TK_GE );
  assert( TK_LT>TK_EQ && TK_LT<TK_GE );
  assert( TK_LE>TK_EQ && TK_LE<TK_GE );
  assert( TK_GE==TK_EQ+4 );
  return op==TK_IN || (op>=TK_EQ && op<=TK_GE) || op==TK_ISNULL;
}

/*
** Commute a comparison operator.  Expressions of the form "X op Y"
** are converted into "Y op X".
**
** If left/right precedence rules come into play when determining the
** collating sequence, then COLLATE operators are adjusted to ensure
** that the collating sequence does not change.  For example:
** "Y collate NOCASE op X" becomes "X op Y" because any collation sequence on
** the left hand side of a comparison overrides any collation sequence 
** attached to the right. For the same reason the EP_Collate flag
** is not commuted.
*/
static void exprCommute(Parse *pParse, Expr *pExpr){
  u16 expRight = (pExpr->pRight->flags & EP_Collate);
  u16 expLeft = (pExpr->pLeft->flags & EP_Collate);
  assert( allowedOp(pExpr->op) && pExpr->op!=TK_IN );
  if( expRight==expLeft ){
    /* Either X and Y both have COLLATE operator or neither do */
    if( expRight ){
      /* Both X and Y have COLLATE operators.  Make sure X is always
      ** used by clearing the EP_Collate flag from Y. */
      pExpr->pRight->flags &= ~EP_Collate;
    }else if( sqlite3ExprCollSeq(pParse, pExpr->pLeft)!=0 ){
      /* Neither X nor Y have COLLATE operators, but X has a non-default
      ** collating sequence.  So add the EP_Collate marker on X to cause
      ** it to be searched first. */
      pExpr->pLeft->flags |= EP_Collate;
    }
  }
  SWAP(Expr*,pExpr->pRight,pExpr->pLeft);
  if( pExpr->op>=TK_GT ){
    assert( TK_LT==TK_GT+2 );
    assert( TK_GE==TK_LE+2 );
    assert( TK_GT>TK_EQ );
    assert( TK_GT<TK_LE );
    assert( pExpr->op>=TK_GT && pExpr->op<=TK_GE );
    pExpr->op = ((pExpr->op-TK_GT)^2)+TK_GT;
  }
}

/*
** Translate from TK_xx operator to WO_xx bitmask.
*/
static u16 operatorMask(int op){
  u16 c;
  assert( allowedOp(op) );
  if( op==TK_IN ){
    c = WO_IN;
  }else if( op==TK_ISNULL ){
    c = WO_ISNULL;
  }else{
    assert( (WO_EQ<<(op-TK_EQ)) < 0x7fff );
    c = (u16)(WO_EQ<<(op-TK_EQ));
  }
  assert( op!=TK_ISNULL || c==WO_ISNULL );
  assert( op!=TK_IN || c==WO_IN );
  assert( op!=TK_EQ || c==WO_EQ );
  assert( op!=TK_LT || c==WO_LT );
  assert( op!=TK_LE || c==WO_LE );
  assert( op!=TK_GT || c==WO_GT );
  assert( op!=TK_GE || c==WO_GE );
  return c;
}

/*
** Advance to the next WhereTerm that matches according to the criteria
** established when the pScan object was initialized by whereScanInit().
** Return NULL if there are no more matching WhereTerms.
*/
static WhereTerm *whereScanNext(WhereScan *pScan){
  int iCur;            /* The cursor on the LHS of the term */
  int iColumn;         /* The column on the LHS of the term.  -1 for IPK */
  Expr *pX;            /* An expression being tested */
  WhereClause *pWC;    /* Shorthand for pScan->pWC */
  WhereTerm *pTerm;    /* The term being tested */
  int k = pScan->k;    /* Where to start scanning */

  while( pScan->iEquiv<=pScan->nEquiv ){
    iCur = pScan->aEquiv[pScan->iEquiv-2];
    iColumn = pScan->aEquiv[pScan->iEquiv-1];
    while( (pWC = pScan->pWC)!=0 ){
      for(pTerm=pWC->a+k; k<pWC->nTerm; k++, pTerm++){
        if( pTerm->leftCursor==iCur
         && pTerm->u.leftColumn==iColumn
         && (pScan->iEquiv<=2 || !ExprHasProperty(pTerm->pExpr, EP_FromJoin))
        ){
          if( (pTerm->eOperator & WO_EQUIV)!=0
           && pScan->nEquiv<ArraySize(pScan->aEquiv)
          ){
            int j;
            pX = sqlite3ExprSkipCollate(pTerm->pExpr->pRight);
            assert( pX->op==TK_COLUMN );
            for(j=0; j<pScan->nEquiv; j+=2){
              if( pScan->aEquiv[j]==pX->iTable
               && pScan->aEquiv[j+1]==pX->iColumn ){
                  break;
              }
            }
            if( j==pScan->nEquiv ){
              pScan->aEquiv[j] = pX->iTable;
              pScan->aEquiv[j+1] = pX->iColumn;
              pScan->nEquiv += 2;
            }
          }
          if( (pTerm->eOperator & pScan->opMask)!=0 ){
            /* Verify the affinity and collating sequence match */
            if( pScan->zCollName && (pTerm->eOperator & WO_ISNULL)==0 ){
              CollSeq *pColl;
              Parse *pParse = pWC->pWInfo->pParse;
              pX = pTerm->pExpr;
              if( !sqlite3IndexAffinityOk(pX, pScan->idxaff) ){
                continue;
              }
              assert(pX->pLeft);
              pColl = sqlite3BinaryCompareCollSeq(pParse,
                                                  pX->pLeft, pX->pRight);
              if( pColl==0 ) pColl = pParse->db->pDfltColl;
              if( sqlite3StrICmp(pColl->zName, pScan->zCollName) ){
                continue;
              }
            }
            if( (pTerm->eOperator & WO_EQ)!=0
             && (pX = pTerm->pExpr->pRight)->op==TK_COLUMN
             && pX->iTable==pScan->aEquiv[0]
             && pX->iColumn==pScan->aEquiv[1]
            ){
              continue;
            }
            pScan->k = k+1;
            return pTerm;
          }
        }
      }
      pScan->pWC = pScan->pWC->pOuter;
      k = 0;
    }
    pScan->pWC = pScan->pOrigWC;
    k = 0;
    pScan->iEquiv += 2;
  }
  return 0;
}

/*
** Initialize a WHERE clause scanner object.  Return a pointer to the
** first match.  Return NULL if there are no matches.
**
** The scanner will be searching the WHERE clause pWC.  It will look
** for terms of the form "X <op> <expr>" where X is column iColumn of table
** iCur.  The <op> must be one of the operators described by opMask.
**
** If the search is for X and the WHERE clause contains terms of the
** form X=Y then this routine might also return terms of the form
** "Y <op> <expr>".  The number of levels of transitivity is limited,
** but is enough to handle most commonly occurring SQL statements.
**
** If X is not the INTEGER PRIMARY KEY then X must be compatible with
** index pIdx.
*/
static WhereTerm *whereScanInit(
  WhereScan *pScan,       /* The WhereScan object being initialized */
  WhereClause *pWC,       /* The WHERE clause to be scanned */
  int iCur,               /* Cursor to scan for */
  int iColumn,            /* Column to scan for */
  u32 opMask,             /* Operator(s) to scan for */
  Index *pIdx             /* Must be compatible with this index */
){
  int j;

  /* memset(pScan, 0, sizeof(*pScan)); */
  pScan->pOrigWC = pWC;
  pScan->pWC = pWC;
  if( pIdx && iColumn>=0 ){
    pScan->idxaff = pIdx->pTable->aCol[iColumn].affinity;
    for(j=0; pIdx->aiColumn[j]!=iColumn; j++){
      if( NEVER(j>pIdx->nColumn) ) return 0;
    }
    pScan->zCollName = pIdx->azColl[j];
  }else{
    pScan->idxaff = 0;
    pScan->zCollName = 0;
  }
  pScan->opMask = opMask;
  pScan->k = 0;
  pScan->aEquiv[0] = iCur;
  pScan->aEquiv[1] = iColumn;
  pScan->nEquiv = 2;
  pScan->iEquiv = 2;
  return whereScanNext(pScan);
}

/*
** Search for a term in the WHERE clause that is of the form "X <op> <expr>"
** where X is a reference to the iColumn of table iCur and <op> is one of
** the WO_xx operator codes specified by the op parameter.
** Return a pointer to the term.  Return 0 if not found.
**
** The term returned might by Y=<expr> if there is another constraint in
** the WHERE clause that specifies that X=Y.  Any such constraints will be
** identified by the WO_EQUIV bit in the pTerm->eOperator field.  The
** aEquiv[] array holds X and all its equivalents, with each SQL variable
** taking up two slots in aEquiv[].  The first slot is for the cursor number
** and the second is for the column number.  There are 22 slots in aEquiv[]
** so that means we can look for X plus up to 10 other equivalent values.
** Hence a search for X will return <expr> if X=A1 and A1=A2 and A2=A3
** and ... and A9=A10 and A10=<expr>.
**
** If there are multiple terms in the WHERE clause of the form "X <op> <expr>"
** then try for the one with no dependencies on <expr> - in other words where
** <expr> is a constant expression of some kind.  Only return entries of
** the form "X <op> Y" where Y is a column in another table if no terms of
** the form "X <op> <const-expr>" exist.   If no terms with a constant RHS
** exist, try to return a term that does not use WO_EQUIV.
*/
static WhereTerm *findTerm(
  WhereClause *pWC,     /* The WHERE clause to be searched */
  int iCur,             /* Cursor number of LHS */
  int iColumn,          /* Column number of LHS */
  Bitmask notReady,     /* RHS must not overlap with this mask */
  u32 op,               /* Mask of WO_xx values describing operator */
  Index *pIdx           /* Must be compatible with this index, if not NULL */
){
  WhereTerm *pResult = 0;
  WhereTerm *p;
  WhereScan scan;

  p = whereScanInit(&scan, pWC, iCur, iColumn, op, pIdx);
  while( p ){
    if( (p->prereqRight & notReady)==0 ){
      if( p->prereqRight==0 && (p->eOperator&WO_EQ)!=0 ){
        return p;
      }
      if( pResult==0 ) pResult = p;
    }
    p = whereScanNext(&scan);
  }
  return pResult;
}

/* Forward reference */
static void exprAnalyze(SrcList*, WhereClause*, int);

/*
** Call exprAnalyze on all terms in a WHERE clause.  
*/
static void exprAnalyzeAll(
  SrcList *pTabList,       /* the FROM clause */
  WhereClause *pWC         /* the WHERE clause to be analyzed */
){
  int i;
  for(i=pWC->nTerm-1; i>=0; i--){
    exprAnalyze(pTabList, pWC, i);
  }
}

#ifndef SQLITE_OMIT_LIKE_OPTIMIZATION
/*
** Check to see if the given expression is a LIKE or GLOB operator that
** can be optimized using inequality constraints.  Return TRUE if it is
** so and false if not.
**
** In order for the operator to be optimizible, the RHS must be a string
** literal that does not begin with a wildcard.  The LHS must be a column
** that may only be NULL, a string, or a BLOB, never a number. (This means
** that virtual tables cannot participate in the LIKE optimization.)  If the
** collating sequence for the column on the LHS must be appropriate for
** the operator.
*/
static int isLikeOrGlob(
  Parse *pParse,    /* Parsing and code generating context */
  Expr *pExpr,      /* Test this expression */
  Expr **ppPrefix,  /* Pointer to TK_STRING expression with pattern prefix */
  int *pisComplete, /* True if the only wildcard is % in the last character */
  int *pnoCase      /* True if uppercase is equivalent to lowercase */
){
  const char *z = 0;         /* String on RHS of LIKE operator */
  Expr *pRight, *pLeft;      /* Right and left size of LIKE operator */
  ExprList *pList;           /* List of operands to the LIKE operator */
  int c;                     /* One character in z[] */
  int cnt;                   /* Number of non-wildcard prefix characters */
  char wc[3];                /* Wildcard characters */
  sqlite3 *db = pParse->db;  /* Database connection */
  sqlite3_value *pVal = 0;
  int op;                    /* Opcode of pRight */

  if( !sqlite3IsLikeFunction(db, pExpr, pnoCase, wc) ){
    return 0;
  }
#ifdef SQLITE_EBCDIC
  if( *pnoCase ) return 0;
#endif
  pList = pExpr->x.pList;
  pLeft = pList->a[1].pExpr;
  if( pLeft->op!=TK_COLUMN 
   || sqlite3ExprAffinity(pLeft)!=SQLITE_AFF_TEXT 
   || IsVirtual(pLeft->pTab)  /* Value might be numeric */
  ){
    /* IMP: R-02065-49465 The left-hand side of the LIKE or GLOB operator must
    ** be the name of an indexed column with TEXT affinity. */
    return 0;
  }
  assert( pLeft->iColumn!=(-1) ); /* Because IPK never has AFF_TEXT */

  pRight = sqlite3ExprSkipCollate(pList->a[0].pExpr);
  op = pRight->op;
  if( op==TK_VARIABLE ){
    Vdbe *pReprepare = pParse->pReprepare;
    int iCol = pRight->iColumn;
    pVal = sqlite3VdbeGetBoundValue(pReprepare, iCol, SQLITE_AFF_NONE);
    if( pVal && sqlite3_value_type(pVal)==SQLITE_TEXT ){
      z = (char *)sqlite3_value_text(pVal);
    }
    sqlite3VdbeSetVarmask(pParse->pVdbe, iCol);
    assert( pRight->op==TK_VARIABLE || pRight->op==TK_REGISTER );
  }else if( op==TK_STRING ){
    z = pRight->u.zToken;
  }
  if( z ){
    cnt = 0;
    while( (c=z[cnt])!=0 && c!=wc[0] && c!=wc[1] && c!=wc[2] ){
      cnt++;
    }
    if( cnt!=0 && 255!=(u8)z[cnt-1] ){
      Expr *pPrefix;
      *pisComplete = c==wc[0] && z[cnt+1]==0;
      pPrefix = sqlite3Expr(db, TK_STRING, z);
      if( pPrefix ) pPrefix->u.zToken[cnt] = 0;
      *ppPrefix = pPrefix;
      if( op==TK_VARIABLE ){
        Vdbe *v = pParse->pVdbe;
        sqlite3VdbeSetVarmask(v, pRight->iColumn);
        if( *pisComplete && pRight->u.zToken[1] ){
          /* If the rhs of the LIKE expression is a variable, and the current
          ** value of the variable means there is no need to invoke the LIKE
          ** function, then no OP_Variable will be added to the program.
          ** This causes problems for the sqlite3_bind_parameter_name()
          ** API. To work around them, add a dummy OP_Variable here.
          */ 
          int r1 = sqlite3GetTempReg(pParse);
          sqlite3ExprCodeTarget(pParse, pRight, r1);
          sqlite3VdbeChangeP3(v, sqlite3VdbeCurrentAddr(v)-1, 0);
          sqlite3ReleaseTempReg(pParse, r1);
        }
      }
    }else{
      z = 0;
    }
  }

  sqlite3ValueFree(pVal);
  return (z!=0);
}
#endif /* SQLITE_OMIT_LIKE_OPTIMIZATION */


#ifndef SQLITE_OMIT_VIRTUALTABLE
/*
** Check to see if the given expression is of the form
**
**         column MATCH expr
**
** If it is then return TRUE.  If not, return FALSE.
*/
static int isMatchOfColumn(
  Expr *pExpr      /* Test this expression */
){
  ExprList *pList;

  if( pExpr->op!=TK_FUNCTION ){
    return 0;
  }
  if( sqlite3StrICmp(pExpr->u.zToken,"match")!=0 ){
    return 0;
  }
  pList = pExpr->x.pList;
  if( pList->nExpr!=2 ){
    return 0;
  }
  if( pList->a[1].pExpr->op != TK_COLUMN ){
    return 0;
  }
  return 1;
}
#endif /* SQLITE_OMIT_VIRTUALTABLE */

/*
** If the pBase expression originated in the ON or USING clause of
** a join, then transfer the appropriate markings over to derived.
*/
static void transferJoinMarkings(Expr *pDerived, Expr *pBase){
  if( pDerived ){
    pDerived->flags |= pBase->flags & EP_FromJoin;
    pDerived->iRightJoinTable = pBase->iRightJoinTable;
  }
}

/*
** Mark term iChild as being a child of term iParent
*/
static void markTermAsChild(WhereClause *pWC, int iChild, int iParent){
  pWC->a[iChild].iParent = iParent;
  pWC->a[iChild].truthProb = pWC->a[iParent].truthProb;
  pWC->a[iParent].nChild++;
}

/*
** Return the N-th AND-connected subterm of pTerm.  Or if pTerm is not
** a conjunction, then return just pTerm when N==0.  If N is exceeds
** the number of available subterms, return NULL.
*/
static WhereTerm *whereNthSubterm(WhereTerm *pTerm, int N){
  if( pTerm->eOperator!=WO_AND ){
    return N==0 ? pTerm : 0;
  }
  if( N<pTerm->u.pAndInfo->wc.nTerm ){
    return &pTerm->u.pAndInfo->wc.a[N];
  }
  return 0;
}

/*
** Subterms pOne and pTwo are contained within WHERE clause pWC.  The
** two subterms are in disjunction - they are OR-ed together.
**
** If these two terms are both of the form:  "A op B" with the same
** A and B values but different operators and if the operators are
** compatible (if one is = and the other is <, for example) then
** add a new virtual AND term to pWC that is the combination of the
** two.
**
** Some examples:
**
**    x<y OR x=y    -->     x<=y
**    x=y OR x=y    -->     x=y
**    x<=y OR x<y   -->     x<=y
**
** The following is NOT generated:
**
**    x<y OR x>y    -->     x!=y     
*/
static void whereCombineDisjuncts(
  SrcList *pSrc,         /* the FROM clause */
  WhereClause *pWC,      /* The complete WHERE clause */
  WhereTerm *pOne,       /* First disjunct */
  WhereTerm *pTwo        /* Second disjunct */
){
  u16 eOp = pOne->eOperator | pTwo->eOperator;
  sqlite3 *db;           /* Database connection (for malloc) */
  Expr *pNew;            /* New virtual expression */
  int op;                /* Operator for the combined expression */
  int idxNew;            /* Index in pWC of the next virtual term */

  if( (pOne->eOperator & (WO_EQ|WO_LT|WO_LE|WO_GT|WO_GE))==0 ) return;
  if( (pTwo->eOperator & (WO_EQ|WO_LT|WO_LE|WO_GT|WO_GE))==0 ) return;
  if( (eOp & (WO_EQ|WO_LT|WO_LE))!=eOp
   && (eOp & (WO_EQ|WO_GT|WO_GE))!=eOp ) return;
  assert( pOne->pExpr->pLeft!=0 && pOne->pExpr->pRight!=0 );
  assert( pTwo->pExpr->pLeft!=0 && pTwo->pExpr->pRight!=0 );
  if( sqlite3ExprCompare(pOne->pExpr->pLeft, pTwo->pExpr->pLeft, -1) ) return;
  if( sqlite3ExprCompare(pOne->pExpr->pRight, pTwo->pExpr->pRight, -1) )return;
  /* If we reach this point, it means the two subterms can be combined */
  if( (eOp & (eOp-1))!=0 ){
    if( eOp & (WO_LT|WO_LE) ){
      eOp = WO_LE;
    }else{
      assert( eOp & (WO_GT|WO_GE) );
      eOp = WO_GE;
    }
  }
  db = pWC->pWInfo->pParse->db;
  pNew = sqlite3ExprDup(db, pOne->pExpr, 0);
  if( pNew==0 ) return;
  for(op=TK_EQ; eOp!=(WO_EQ<<(op-TK_EQ)); op++){ assert( op<TK_GE ); }
  pNew->op = op;
  idxNew = whereClauseInsert(pWC, pNew, TERM_VIRTUAL|TERM_DYNAMIC);
  exprAnalyze(pSrc, pWC, idxNew);
}

#if !defined(SQLITE_OMIT_OR_OPTIMIZATION) && !defined(SQLITE_OMIT_SUBQUERY)
/*
** Analyze a term that consists of two or more OR-connected
** subterms.  So in:
**
**     ... WHERE  (a=5) AND (b=7 OR c=9 OR d=13) AND (d=13)
**                          ^^^^^^^^^^^^^^^^^^^^
**
** This routine analyzes terms such as the middle term in the above example.
** A WhereOrTerm object is computed and attached to the term under
** analysis, regardless of the outcome of the analysis.  Hence:
**
**     WhereTerm.wtFlags   |=  TERM_ORINFO
**     WhereTerm.u.pOrInfo  =  a dynamically allocated WhereOrTerm object
**
** The term being analyzed must have two or more of OR-connected subterms.
** A single subterm might be a set of AND-connected sub-subterms.
** Examples of terms under analysis:
**
**     (A)     t1.x=t2.y OR t1.x=t2.z OR t1.y=15 OR t1.z=t3.a+5
**     (B)     x=expr1 OR expr2=x OR x=expr3
**     (C)     t1.x=t2.y OR (t1.x=t2.z AND t1.y=15)
**     (D)     x=expr1 OR (y>11 AND y<22 AND z LIKE '*hello*')
**     (E)     (p.a=1 AND q.b=2 AND r.c=3) OR (p.x=4 AND q.y=5 AND r.z=6)
**     (F)     x>A OR (x=A AND y>=B)
**
** CASE 1:
**
** If all subterms are of the form T.C=expr for some single column of C and
** a single table T (as shown in example B above) then create a new virtual
** term that is an equivalent IN expression.  In other words, if the term
** being analyzed is:
**
**      x = expr1  OR  expr2 = x  OR  x = expr3
**
** then create a new virtual term like this:
**
**      x IN (expr1,expr2,expr3)
**
** CASE 2:
**
** If there are exactly two disjuncts one side has x>A and the other side
** has x=A (for the same x and A) then add a new virtual conjunct term to the
** WHERE clause of the form "x>=A".  Example:
**
**      x>A OR (x=A AND y>B)    adds:    x>=A
**
** The added conjunct can sometimes be helpful in query planning.
**
** CASE 3:
**
** If all subterms are indexable by a single table T, then set
**
**     WhereTerm.eOperator              =  WO_OR
**     WhereTerm.u.pOrInfo->indexable  |=  the cursor number for table T
**
** A subterm is "indexable" if it is of the form
** "T.C <op> <expr>" where C is any column of table T and 
** <op> is one of "=", "<", "<=", ">", ">=", "IS NULL", or "IN".
** A subterm is also indexable if it is an AND of two or more
** subsubterms at least one of which is indexable.  Indexable AND 
** subterms have their eOperator set to WO_AND and they have
** u.pAndInfo set to a dynamically allocated WhereAndTerm object.
**
** From another point of view, "indexable" means that the subterm could
** potentially be used with an index if an appropriate index exists.
** This analysis does not consider whether or not the index exists; that
** is decided elsewhere.  This analysis only looks at whether subterms
** appropriate for indexing exist.
**
** All examples A through E above satisfy case 2.  But if a term
** also satisfies case 1 (such as B) we know that the optimizer will
** always prefer case 1, so in that case we pretend that case 2 is not
** satisfied.
**
** It might be the case that multiple tables are indexable.  For example,
** (E) above is indexable on tables P, Q, and R.
**
** Terms that satisfy case 2 are candidates for lookup by using
** separate indices to find rowids for each subterm and composing
** the union of all rowids using a RowSet object.  This is similar
** to "bitmap indices" in other database engines.
**
** OTHERWISE:
**
** If neither case 1 nor case 2 apply, then leave the eOperator set to
** zero.  This term is not useful for search.
*/
static void exprAnalyzeOrTerm(
  SrcList *pSrc,            /* the FROM clause */
  WhereClause *pWC,         /* the complete WHERE clause */
  int idxTerm               /* Index of the OR-term to be analyzed */
){
  WhereInfo *pWInfo = pWC->pWInfo;        /* WHERE clause processing context */
  Parse *pParse = pWInfo->pParse;         /* Parser context */
  sqlite3 *db = pParse->db;               /* Database connection */
  WhereTerm *pTerm = &pWC->a[idxTerm];    /* The term to be analyzed */
  Expr *pExpr = pTerm->pExpr;             /* The expression of the term */
  int i;                                  /* Loop counters */
  WhereClause *pOrWc;       /* Breakup of pTerm into subterms */
  WhereTerm *pOrTerm;       /* A Sub-term within the pOrWc */
  WhereOrInfo *pOrInfo;     /* Additional information associated with pTerm */
  Bitmask chngToIN;         /* Tables that might satisfy case 1 */
  Bitmask indexable;        /* Tables that are indexable, satisfying case 2 */

  /*
  ** Break the OR clause into its separate subterms.  The subterms are
  ** stored in a WhereClause structure containing within the WhereOrInfo
  ** object that is attached to the original OR clause term.
  */
  assert( (pTerm->wtFlags & (TERM_DYNAMIC|TERM_ORINFO|TERM_ANDINFO))==0 );
  assert( pExpr->op==TK_OR );
  pTerm->u.pOrInfo = pOrInfo = sqlite3DbMallocZero(db, sizeof(*pOrInfo));
  if( pOrInfo==0 ) return;
  pTerm->wtFlags |= TERM_ORINFO;
  pOrWc = &pOrInfo->wc;
  whereClauseInit(pOrWc, pWInfo);
  whereSplit(pOrWc, pExpr, TK_OR);
  exprAnalyzeAll(pSrc, pOrWc);
  if( db->mallocFailed ) return;
  assert( pOrWc->nTerm>=2 );

  /*
  ** Compute the set of tables that might satisfy cases 1 or 2.
  */
  indexable = ~(Bitmask)0;
  chngToIN = ~(Bitmask)0;
  for(i=pOrWc->nTerm-1, pOrTerm=pOrWc->a; i>=0 && indexable; i--, pOrTerm++){
    if( (pOrTerm->eOperator & WO_SINGLE)==0 ){
      WhereAndInfo *pAndInfo;
      assert( (pOrTerm->wtFlags & (TERM_ANDINFO|TERM_ORINFO))==0 );
      chngToIN = 0;
      pAndInfo = sqlite3DbMallocRaw(db, sizeof(*pAndInfo));
      if( pAndInfo ){
        WhereClause *pAndWC;
        WhereTerm *pAndTerm;
        int j;
        Bitmask b = 0;
        pOrTerm->u.pAndInfo = pAndInfo;
        pOrTerm->wtFlags |= TERM_ANDINFO;
        pOrTerm->eOperator = WO_AND;
        pAndWC = &pAndInfo->wc;
        whereClauseInit(pAndWC, pWC->pWInfo);
        whereSplit(pAndWC, pOrTerm->pExpr, TK_AND);
        exprAnalyzeAll(pSrc, pAndWC);
        pAndWC->pOuter = pWC;
        testcase( db->mallocFailed );
        if( !db->mallocFailed ){
          for(j=0, pAndTerm=pAndWC->a; j<pAndWC->nTerm; j++, pAndTerm++){
            assert( pAndTerm->pExpr );
            if( allowedOp(pAndTerm->pExpr->op) ){
              b |= getMask(&pWInfo->sMaskSet, pAndTerm->leftCursor);
            }
          }
        }
        indexable &= b;
      }
    }else if( pOrTerm->wtFlags & TERM_COPIED ){
      /* Skip this term for now.  We revisit it when we process the
      ** corresponding TERM_VIRTUAL term */
    }else{
      Bitmask b;
      b = getMask(&pWInfo->sMaskSet, pOrTerm->leftCursor);
      if( pOrTerm->wtFlags & TERM_VIRTUAL ){
        WhereTerm *pOther = &pOrWc->a[pOrTerm->iParent];
        b |= getMask(&pWInfo->sMaskSet, pOther->leftCursor);
      }
      indexable &= b;
      if( (pOrTerm->eOperator & WO_EQ)==0 ){
        chngToIN = 0;
      }else{
        chngToIN &= b;
      }
    }
  }

  /*
  ** Record the set of tables that satisfy case 3.  The set might be
  ** empty.
  */
  pOrInfo->indexable = indexable;
  pTerm->eOperator = indexable==0 ? 0 : WO_OR;

  /* For a two-way OR, attempt to implementation case 2.
  */
  if( indexable && pOrWc->nTerm==2 ){
    int iOne = 0;
    WhereTerm *pOne;
    while( (pOne = whereNthSubterm(&pOrWc->a[0],iOne++))!=0 ){
      int iTwo = 0;
      WhereTerm *pTwo;
      while( (pTwo = whereNthSubterm(&pOrWc->a[1],iTwo++))!=0 ){
        whereCombineDisjuncts(pSrc, pWC, pOne, pTwo);
      }
    }
  }

  /*
  ** chngToIN holds a set of tables that *might* satisfy case 1.  But
  ** we have to do some additional checking to see if case 1 really
  ** is satisfied.
  **
  ** chngToIN will hold either 0, 1, or 2 bits.  The 0-bit case means
  ** that there is no possibility of transforming the OR clause into an
  ** IN operator because one or more terms in the OR clause contain
  ** something other than == on a column in the single table.  The 1-bit
  ** case means that every term of the OR clause is of the form
  ** "table.column=expr" for some single table.  The one bit that is set
  ** will correspond to the common table.  We still need to check to make
  ** sure the same column is used on all terms.  The 2-bit case is when
  ** the all terms are of the form "table1.column=table2.column".  It
  ** might be possible to form an IN operator with either table1.column
  ** or table2.column as the LHS if either is common to every term of
  ** the OR clause.
  **
  ** Note that terms of the form "table.column1=table.column2" (the
  ** same table on both sizes of the ==) cannot be optimized.
  */
  if( chngToIN ){
    int okToChngToIN = 0;     /* True if the conversion to IN is valid */
    int iColumn = -1;         /* Column index on lhs of IN operator */
    int iCursor = -1;         /* Table cursor common to all terms */
    int j = 0;                /* Loop counter */

    /* Search for a table and column that appears on one side or the
    ** other of the == operator in every subterm.  That table and column
    ** will be recorded in iCursor and iColumn.  There might not be any
    ** such table and column.  Set okToChngToIN if an appropriate table
    ** and column is found but leave okToChngToIN false if not found.
    */
    for(j=0; j<2 && !okToChngToIN; j++){
      pOrTerm = pOrWc->a;
      for(i=pOrWc->nTerm-1; i>=0; i--, pOrTerm++){
        assert( pOrTerm->eOperator & WO_EQ );
        pOrTerm->wtFlags &= ~TERM_OR_OK;
        if( pOrTerm->leftCursor==iCursor ){
          /* This is the 2-bit case and we are on the second iteration and
          ** current term is from the first iteration.  So skip this term. */
          assert( j==1 );
          continue;
        }
        if( (chngToIN & getMask(&pWInfo->sMaskSet, pOrTerm->leftCursor))==0 ){
          /* This term must be of the form t1.a==t2.b where t2 is in the
          ** chngToIN set but t1 is not.  This term will be either preceded
          ** or follwed by an inverted copy (t2.b==t1.a).  Skip this term 
          ** and use its inversion. */
          testcase( pOrTerm->wtFlags & TERM_COPIED );
          testcase( pOrTerm->wtFlags & TERM_VIRTUAL );
          assert( pOrTerm->wtFlags & (TERM_COPIED|TERM_VIRTUAL) );
          continue;
        }
        iColumn = pOrTerm->u.leftColumn;
        iCursor = pOrTerm->leftCursor;
        break;
      }
      if( i<0 ){
        /* No candidate table+column was found.  This can only occur
        ** on the second iteration */
        assert( j==1 );
        assert( IsPowerOfTwo(chngToIN) );
        assert( chngToIN==getMask(&pWInfo->sMaskSet, iCursor) );
        break;
      }
      testcase( j==1 );

      /* We have found a candidate table and column.  Check to see if that
      ** table and column is common to every term in the OR clause */
      okToChngToIN = 1;
      for(; i>=0 && okToChngToIN; i--, pOrTerm++){
        assert( pOrTerm->eOperator & WO_EQ );
        if( pOrTerm->leftCursor!=iCursor ){
          pOrTerm->wtFlags &= ~TERM_OR_OK;
        }else if( pOrTerm->u.leftColumn!=iColumn ){
          okToChngToIN = 0;
        }else{
          int affLeft, affRight;
          /* If the right-hand side is also a column, then the affinities
          ** of both right and left sides must be such that no type
          ** conversions are required on the right.  (Ticket #2249)
          */
          affRight = sqlite3ExprAffinity(pOrTerm->pExpr->pRight);
          affLeft = sqlite3ExprAffinity(pOrTerm->pExpr->pLeft);
          if( affRight!=0 && affRight!=affLeft ){
            okToChngToIN = 0;
          }else{
            pOrTerm->wtFlags |= TERM_OR_OK;
          }
        }
      }
    }

    /* At this point, okToChngToIN is true if original pTerm satisfies
    ** case 1.  In that case, construct a new virtual term that is 
    ** pTerm converted into an IN operator.
    */
    if( okToChngToIN ){
      Expr *pDup;            /* A transient duplicate expression */
      ExprList *pList = 0;   /* The RHS of the IN operator */
      Expr *pLeft = 0;       /* The LHS of the IN operator */
      Expr *pNew;            /* The complete IN operator */

      for(i=pOrWc->nTerm-1, pOrTerm=pOrWc->a; i>=0; i--, pOrTerm++){
        if( (pOrTerm->wtFlags & TERM_OR_OK)==0 ) continue;
        assert( pOrTerm->eOperator & WO_EQ );
        assert( pOrTerm->leftCursor==iCursor );
        assert( pOrTerm->u.leftColumn==iColumn );
        pDup = sqlite3ExprDup(db, pOrTerm->pExpr->pRight, 0);
        pList = sqlite3ExprListAppend(pWInfo->pParse, pList, pDup);
        pLeft = pOrTerm->pExpr->pLeft;
      }
      assert( pLeft!=0 );
      pDup = sqlite3ExprDup(db, pLeft, 0);
      pNew = sqlite3PExpr(pParse, TK_IN, pDup, 0, 0);
      if( pNew ){
        int idxNew;
        transferJoinMarkings(pNew, pExpr);
        assert( !ExprHasProperty(pNew, EP_xIsSelect) );
        pNew->x.pList = pList;
        idxNew = whereClauseInsert(pWC, pNew, TERM_VIRTUAL|TERM_DYNAMIC);
        testcase( idxNew==0 );
        exprAnalyze(pSrc, pWC, idxNew);
        pTerm = &pWC->a[idxTerm];
        markTermAsChild(pWC, idxNew, idxTerm);
      }else{
        sqlite3ExprListDelete(db, pList);
      }
      pTerm->eOperator = WO_NOOP;  /* case 1 trumps case 3 */
    }
  }
}
#endif /* !SQLITE_OMIT_OR_OPTIMIZATION && !SQLITE_OMIT_SUBQUERY */

/*
** The input to this routine is an WhereTerm structure with only the
** "pExpr" field filled in.  The job of this routine is to analyze the
** subexpression and populate all the other fields of the WhereTerm
** structure.
**
** If the expression is of the form "<expr> <op> X" it gets commuted
** to the standard form of "X <op> <expr>".
**
** If the expression is of the form "X <op> Y" where both X and Y are
** columns, then the original expression is unchanged and a new virtual
** term of the form "Y <op> X" is added to the WHERE clause and
** analyzed separately.  The original term is marked with TERM_COPIED
** and the new term is marked with TERM_DYNAMIC (because it's pExpr
** needs to be freed with the WhereClause) and TERM_VIRTUAL (because it
** is a commuted copy of a prior term.)  The original term has nChild=1
** and the copy has idxParent set to the index of the original term.
*/
static void exprAnalyze(
  SrcList *pSrc,            /* the FROM clause */
  WhereClause *pWC,         /* the WHERE clause */
  int idxTerm               /* Index of the term to be analyzed */
){
  WhereInfo *pWInfo = pWC->pWInfo; /* WHERE clause processing context */
  WhereTerm *pTerm;                /* The term to be analyzed */
  WhereMaskSet *pMaskSet;          /* Set of table index masks */
  Expr *pExpr;                     /* The expression to be analyzed */
  Bitmask prereqLeft;              /* Prerequesites of the pExpr->pLeft */
  Bitmask prereqAll;               /* Prerequesites of pExpr */
  Bitmask extraRight = 0;          /* Extra dependencies on LEFT JOIN */
  Expr *pStr1 = 0;                 /* RHS of LIKE/GLOB operator */
  int isComplete = 0;              /* RHS of LIKE/GLOB ends with wildcard */
  int noCase = 0;                  /* uppercase equivalent to lowercase */
  int op;                          /* Top-level operator.  pExpr->op */
  Parse *pParse = pWInfo->pParse;  /* Parsing context */
  sqlite3 *db = pParse->db;        /* Database connection */

  if( db->mallocFailed ){
    return;
  }
  pTerm = &pWC->a[idxTerm];
  pMaskSet = &pWInfo->sMaskSet;
  pExpr = pTerm->pExpr;
  assert( pExpr->op!=TK_AS && pExpr->op!=TK_COLLATE );
  prereqLeft = exprTableUsage(pMaskSet, pExpr->pLeft);
  op = pExpr->op;
  if( op==TK_IN ){
    assert( pExpr->pRight==0 );
    if( ExprHasProperty(pExpr, EP_xIsSelect) ){
      pTerm->prereqRight = exprSelectTableUsage(pMaskSet, pExpr->x.pSelect);
    }else{
      pTerm->prereqRight = exprListTableUsage(pMaskSet, pExpr->x.pList);
    }
  }else if( op==TK_ISNULL ){
    pTerm->prereqRight = 0;
  }else{
    pTerm->prereqRight = exprTableUsage(pMaskSet, pExpr->pRight);
  }
  prereqAll = exprTableUsage(pMaskSet, pExpr);
  if( ExprHasProperty(pExpr, EP_FromJoin) ){
    Bitmask x = getMask(pMaskSet, pExpr->iRightJoinTable);
    prereqAll |= x;
    extraRight = x-1;  /* ON clause terms may not be used with an index
                       ** on left table of a LEFT JOIN.  Ticket #3015 */
  }
  pTerm->prereqAll = prereqAll;
  pTerm->leftCursor = -1;
  pTerm->iParent = -1;
  pTerm->eOperator = 0;
  if( allowedOp(op) ){
    Expr *pLeft = sqlite3ExprSkipCollate(pExpr->pLeft);
    Expr *pRight = sqlite3ExprSkipCollate(pExpr->pRight);
    u16 opMask = (pTerm->prereqRight & prereqLeft)==0 ? WO_ALL : WO_EQUIV;
    if( pLeft->op==TK_COLUMN ){
      pTerm->leftCursor = pLeft->iTable;
      pTerm->u.leftColumn = pLeft->iColumn;
      pTerm->eOperator = operatorMask(op) & opMask;
    }
    if( pRight && pRight->op==TK_COLUMN ){
      WhereTerm *pNew;
      Expr *pDup;
      u16 eExtraOp = 0;        /* Extra bits for pNew->eOperator */
      if( pTerm->leftCursor>=0 ){
        int idxNew;
        pDup = sqlite3ExprDup(db, pExpr, 0);
        if( db->mallocFailed ){
          sqlite3ExprDelete(db, pDup);
          return;
        }
        idxNew = whereClauseInsert(pWC, pDup, TERM_VIRTUAL|TERM_DYNAMIC);
        if( idxNew==0 ) return;
        pNew = &pWC->a[idxNew];
        markTermAsChild(pWC, idxNew, idxTerm);
        pTerm = &pWC->a[idxTerm];
        pTerm->wtFlags |= TERM_COPIED;
        if( pExpr->op==TK_EQ
         && !ExprHasProperty(pExpr, EP_FromJoin)
         && OptimizationEnabled(db, SQLITE_Transitive)
        ){
          pTerm->eOperator |= WO_EQUIV;
          eExtraOp = WO_EQUIV;
        }
      }else{
        pDup = pExpr;
        pNew = pTerm;
      }
      exprCommute(pParse, pDup);
      pLeft = sqlite3ExprSkipCollate(pDup->pLeft);
      pNew->leftCursor = pLeft->iTable;
      pNew->u.leftColumn = pLeft->iColumn;
      testcase( (prereqLeft | extraRight) != prereqLeft );
      pNew->prereqRight = prereqLeft | extraRight;
      pNew->prereqAll = prereqAll;
      pNew->eOperator = (operatorMask(pDup->op) + eExtraOp) & opMask;
    }
  }

#ifndef SQLITE_OMIT_BETWEEN_OPTIMIZATION
  /* If a term is the BETWEEN operator, create two new virtual terms
  ** that define the range that the BETWEEN implements.  For example:
  **
  **      a BETWEEN b AND c
  **
  ** is converted into:
  **
  **      (a BETWEEN b AND c) AND (a>=b) AND (a<=c)
  **
  ** The two new terms are added onto the end of the WhereClause object.
  ** The new terms are "dynamic" and are children of the original BETWEEN
  ** term.  That means that if the BETWEEN term is coded, the children are
  ** skipped.  Or, if the children are satisfied by an index, the original
  ** BETWEEN term is skipped.
  */
  else if( pExpr->op==TK_BETWEEN && pWC->op==TK_AND ){
    ExprList *pList = pExpr->x.pList;
    int i;
    static const u8 ops[] = {TK_GE, TK_LE};
    assert( pList!=0 );
    assert( pList->nExpr==2 );
    for(i=0; i<2; i++){
      Expr *pNewExpr;
      int idxNew;
      pNewExpr = sqlite3PExpr(pParse, ops[i], 
                             sqlite3ExprDup(db, pExpr->pLeft, 0),
                             sqlite3ExprDup(db, pList->a[i].pExpr, 0), 0);
      transferJoinMarkings(pNewExpr, pExpr);
      idxNew = whereClauseInsert(pWC, pNewExpr, TERM_VIRTUAL|TERM_DYNAMIC);
      testcase( idxNew==0 );
      exprAnalyze(pSrc, pWC, idxNew);
      pTerm = &pWC->a[idxTerm];
      markTermAsChild(pWC, idxNew, idxTerm);
    }
  }
#endif /* SQLITE_OMIT_BETWEEN_OPTIMIZATION */

#if !defined(SQLITE_OMIT_OR_OPTIMIZATION) && !defined(SQLITE_OMIT_SUBQUERY)
  /* Analyze a term that is composed of two or more subterms connected by
  ** an OR operator.
  */
  else if( pExpr->op==TK_OR ){
    assert( pWC->op==TK_AND );
    exprAnalyzeOrTerm(pSrc, pWC, idxTerm);
    pTerm = &pWC->a[idxTerm];
  }
#endif /* SQLITE_OMIT_OR_OPTIMIZATION */

#ifndef SQLITE_OMIT_LIKE_OPTIMIZATION
  /* Add constraints to reduce the search space on a LIKE or GLOB
  ** operator.
  **
  ** A like pattern of the form "x LIKE 'aBc%'" is changed into constraints
  **
  **          x>='ABC' AND x<'abd' AND x LIKE 'aBc%'
  **
  ** The last character of the prefix "abc" is incremented to form the
  ** termination condition "abd".  If case is not significant (the default
  ** for LIKE) then the lower-bound is made all uppercase and the upper-
  ** bound is made all lowercase so that the bounds also work when comparing
  ** BLOBs.
  */
  if( pWC->op==TK_AND 
   && isLikeOrGlob(pParse, pExpr, &pStr1, &isComplete, &noCase)
  ){
    Expr *pLeft;       /* LHS of LIKE/GLOB operator */
    Expr *pStr2;       /* Copy of pStr1 - RHS of LIKE/GLOB operator */
    Expr *pNewExpr1;
    Expr *pNewExpr2;
    int idxNew1;
    int idxNew2;
    const char *zCollSeqName;     /* Name of collating sequence */
    const u16 wtFlags = TERM_LIKEOPT | TERM_VIRTUAL | TERM_DYNAMIC;

    pLeft = pExpr->x.pList->a[1].pExpr;
    pStr2 = sqlite3ExprDup(db, pStr1, 0);

    /* Convert the lower bound to upper-case and the upper bound to
    ** lower-case (upper-case is less than lower-case in ASCII) so that
    ** the range constraints also work for BLOBs
    */
    if( noCase && !pParse->db->mallocFailed ){
      int i;
      char c;
      pTerm->wtFlags |= TERM_LIKE;
      for(i=0; (c = pStr1->u.zToken[i])!=0; i++){
        pStr1->u.zToken[i] = sqlite3Toupper(c);
        pStr2->u.zToken[i] = sqlite3Tolower(c);
      }
    }

    if( !db->mallocFailed ){
      u8 c, *pC;       /* Last character before the first wildcard */
      pC = (u8*)&pStr2->u.zToken[sqlite3Strlen30(pStr2->u.zToken)-1];
      c = *pC;
      if( noCase ){
        /* The point is to increment the last character before the first
        ** wildcard.  But if we increment '@', that will push it into the
        ** alphabetic range where case conversions will mess up the 
        ** inequality.  To avoid this, make sure to also run the full
        ** LIKE on all candidate expressions by clearing the isComplete flag
        */
        if( c=='A'-1 ) isComplete = 0;
        c = sqlite3UpperToLower[c];
      }
      *pC = c + 1;
    }
    zCollSeqName = noCase ? "NOCASE" : "BINARY";
    pNewExpr1 = sqlite3ExprDup(db, pLeft, 0);
    pNewExpr1 = sqlite3PExpr(pParse, TK_GE,
           sqlite3ExprAddCollateString(pParse,pNewExpr1,zCollSeqName),
           pStr1, 0);
    transferJoinMarkings(pNewExpr1, pExpr);
    idxNew1 = whereClauseInsert(pWC, pNewExpr1, wtFlags);
    testcase( idxNew1==0 );
    exprAnalyze(pSrc, pWC, idxNew1);
    pNewExpr2 = sqlite3ExprDup(db, pLeft, 0);
    pNewExpr2 = sqlite3PExpr(pParse, TK_LT,
           sqlite3ExprAddCollateString(pParse,pNewExpr2,zCollSeqName),
           pStr2, 0);
    transferJoinMarkings(pNewExpr2, pExpr);
    idxNew2 = whereClauseInsert(pWC, pNewExpr2, wtFlags);
    testcase( idxNew2==0 );
    exprAnalyze(pSrc, pWC, idxNew2);
    pTerm = &pWC->a[idxTerm];
    if( isComplete ){
      markTermAsChild(pWC, idxNew1, idxTerm);
      markTermAsChild(pWC, idxNew2, idxTerm);
    }
  }
#endif /* SQLITE_OMIT_LIKE_OPTIMIZATION */

#ifndef SQLITE_OMIT_VIRTUALTABLE
  /* Add a WO_MATCH auxiliary term to the constraint set if the
  ** current expression is of the form:  column MATCH expr.
  ** This information is used by the xBestIndex methods of
  ** virtual tables.  The native query optimizer does not attempt
  ** to do anything with MATCH functions.
  */
  if( isMatchOfColumn(pExpr) ){
    int idxNew;
    Expr *pRight, *pLeft;
    WhereTerm *pNewTerm;
    Bitmask prereqColumn, prereqExpr;

    pRight = pExpr->x.pList->a[0].pExpr;
    pLeft = pExpr->x.pList->a[1].pExpr;
    prereqExpr = exprTableUsage(pMaskSet, pRight);
    prereqColumn = exprTableUsage(pMaskSet, pLeft);
    if( (prereqExpr & prereqColumn)==0 ){
      Expr *pNewExpr;
      pNewExpr = sqlite3PExpr(pParse, TK_MATCH, 
                              0, sqlite3ExprDup(db, pRight, 0), 0);
      idxNew = whereClauseInsert(pWC, pNewExpr, TERM_VIRTUAL|TERM_DYNAMIC);
      testcase( idxNew==0 );
      pNewTerm = &pWC->a[idxNew];
      pNewTerm->prereqRight = prereqExpr;
      pNewTerm->leftCursor = pLeft->iTable;
      pNewTerm->u.leftColumn = pLeft->iColumn;
      pNewTerm->eOperator = WO_MATCH;
      markTermAsChild(pWC, idxNew, idxTerm);
      pTerm = &pWC->a[idxTerm];
      pTerm->wtFlags |= TERM_COPIED;
      pNewTerm->prereqAll = pTerm->prereqAll;
    }
  }
#endif /* SQLITE_OMIT_VIRTUALTABLE */

#ifdef SQLITE_ENABLE_STAT3_OR_STAT4
  /* When sqlite_stat3 histogram data is available an operator of the
  ** form "x IS NOT NULL" can sometimes be evaluated more efficiently
  ** as "x>NULL" if x is not an INTEGER PRIMARY KEY.  So construct a
  ** virtual term of that form.
  **
  ** Note that the virtual term must be tagged with TERM_VNULL.  This
  ** TERM_VNULL tag will suppress the not-null check at the beginning
  ** of the loop.  Without the TERM_VNULL flag, the not-null check at
  ** the start of the loop will prevent any results from being returned.
  */
  if( pExpr->op==TK_NOTNULL
   && pExpr->pLeft->op==TK_COLUMN
   && pExpr->pLeft->iColumn>=0
   && OptimizationEnabled(db, SQLITE_Stat34)
  ){
    Expr *pNewExpr;
    Expr *pLeft = pExpr->pLeft;
    int idxNew;
    WhereTerm *pNewTerm;

    pNewExpr = sqlite3PExpr(pParse, TK_GT,
                            sqlite3ExprDup(db, pLeft, 0),
                            sqlite3PExpr(pParse, TK_NULL, 0, 0, 0), 0);

    idxNew = whereClauseInsert(pWC, pNewExpr,
                              TERM_VIRTUAL|TERM_DYNAMIC|TERM_VNULL);
    if( idxNew ){
      pNewTerm = &pWC->a[idxNew];
      pNewTerm->prereqRight = 0;
      pNewTerm->leftCursor = pLeft->iTable;
      pNewTerm->u.leftColumn = pLeft->iColumn;
      pNewTerm->eOperator = WO_GT;
      markTermAsChild(pWC, idxNew, idxTerm);
      pTerm = &pWC->a[idxTerm];
      pTerm->wtFlags |= TERM_COPIED;
      pNewTerm->prereqAll = pTerm->prereqAll;
    }
  }
#endif /* SQLITE_ENABLE_STAT3_OR_STAT4 */

  /* Prevent ON clause terms of a LEFT JOIN from being used to drive
  ** an index for tables to the left of the join.
  */
  pTerm->prereqRight |= extraRight;
}

/*
** This function searches pList for an entry that matches the iCol-th column
** of index pIdx.
**
** If such an expression is found, its index in pList->a[] is returned. If
** no expression is found, -1 is returned.
*/
static int findIndexCol(
  Parse *pParse,                  /* Parse context */
  ExprList *pList,                /* Expression list to search */
  int iBase,                      /* Cursor for table associated with pIdx */
  Index *pIdx,                    /* Index to match column of */
  int iCol                        /* Column of index to match */
){
  int i;
  const char *zColl = pIdx->azColl[iCol];

  for(i=0; i<pList->nExpr; i++){
    Expr *p = sqlite3ExprSkipCollate(pList->a[i].pExpr);
    if( p->op==TK_COLUMN
     && p->iColumn==pIdx->aiColumn[iCol]
     && p->iTable==iBase
    ){
      CollSeq *pColl = sqlite3ExprCollSeq(pParse, pList->a[i].pExpr);
      if( ALWAYS(pColl) && 0==sqlite3StrICmp(pColl->zName, zColl) ){
        return i;
      }
    }
  }

  return -1;
}

/*
** Return true if the DISTINCT expression-list passed as the third argument
** is redundant.
**
** A DISTINCT list is redundant if the database contains some subset of
** columns that are unique and non-null.
*/
static int isDistinctRedundant(
  Parse *pParse,            /* Parsing context */
  SrcList *pTabList,        /* The FROM clause */
  WhereClause *pWC,         /* The WHERE clause */
  ExprList *pDistinct       /* The result set that needs to be DISTINCT */
){
  Table *pTab;
  Index *pIdx;
  int i;                          
  int iBase;

  /* If there is more than one table or sub-select in the FROM clause of
  ** this query, then it will not be possible to show that the DISTINCT 
  ** clause is redundant. */
  if( pTabList->nSrc!=1 ) return 0;
  iBase = pTabList->a[0].iCursor;
  pTab = pTabList->a[0].pTab;

  /* If any of the expressions is an IPK column on table iBase, then return 
  ** true. Note: The (p->iTable==iBase) part of this test may be false if the
  ** current SELECT is a correlated sub-query.
  */
  for(i=0; i<pDistinct->nExpr; i++){
    Expr *p = sqlite3ExprSkipCollate(pDistinct->a[i].pExpr);
    if( p->op==TK_COLUMN && p->iTable==iBase && p->iColumn<0 ) return 1;
  }

  /* Loop through all indices on the table, checking each to see if it makes
  ** the DISTINCT qualifier redundant. It does so if:
  **
  **   1. The index is itself UNIQUE, and
  **
  **   2. All of the columns in the index are either part of the pDistinct
  **      list, or else the WHERE clause contains a term of the form "col=X",
  **      where X is a constant value. The collation sequences of the
  **      comparison and select-list expressions must match those of the index.
  **
  **   3. All of those index columns for which the WHERE clause does not
  **      contain a "col=X" term are subject to a NOT NULL constraint.
  */
  for(pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext){
    if( !IsUniqueIndex(pIdx) ) continue;
    for(i=0; i<pIdx->nKeyCol; i++){
      i16 iCol = pIdx->aiColumn[i];
      if( 0==findTerm(pWC, iBase, iCol, ~(Bitmask)0, WO_EQ, pIdx) ){
        int iIdxCol = findIndexCol(pParse, pDistinct, iBase, pIdx, i);
        if( iIdxCol<0 || pTab->aCol[iCol].notNull==0 ){
          break;
        }
      }
    }
    if( i==pIdx->nKeyCol ){
      /* This index implies that the DISTINCT qualifier is redundant. */
      return 1;
    }
  }

  return 0;
}


/*
** Estimate the logarithm of the input value to base 2.
*/
static LogEst estLog(LogEst N){
  return N<=10 ? 0 : sqlite3LogEst(N) - 33;
}

/*
** Two routines for printing the content of an sqlite3_index_info
** structure.  Used for testing and debugging only.  If neither
** SQLITE_TEST or SQLITE_DEBUG are defined, then these routines
** are no-ops.
*/
#if !defined(SQLITE_OMIT_VIRTUALTABLE) && defined(WHERETRACE_ENABLED)
static void TRACE_IDX_INPUTS(sqlite3_index_info *p){
  int i;
  if( !sqlite3WhereTrace ) return;
  for(i=0; i<p->nConstraint; i++){
    sqlite3DebugPrintf("  constraint[%d]: col=%d termid=%d op=%d usabled=%d\n",
       i,
       p->aConstraint[i].iColumn,
       p->aConstraint[i].iTermOffset,
       p->aConstraint[i].op,
       p->aConstraint[i].usable);
  }
  for(i=0; i<p->nOrderBy; i++){
    sqlite3DebugPrintf("  orderby[%d]: col=%d desc=%d\n",
       i,
       p->aOrderBy[i].iColumn,
       p->aOrderBy[i].desc);
  }
}
static void TRACE_IDX_OUTPUTS(sqlite3_index_info *p){
  int i;
  if( !sqlite3WhereTrace ) return;
  for(i=0; i<p->nConstraint; i++){
    sqlite3DebugPrintf("  usage[%d]: argvIdx=%d omit=%d\n",
       i,
       p->aConstraintUsage[i].argvIndex,
       p->aConstraintUsage[i].omit);
  }
  sqlite3DebugPrintf("  idxNum=%d\n", p->idxNum);
  sqlite3DebugPrintf("  idxStr=%s\n", p->idxStr);
  sqlite3DebugPrintf("  orderByConsumed=%d\n", p->orderByConsumed);
  sqlite3DebugPrintf("  estimatedCost=%g\n", p->estimatedCost);
  sqlite3DebugPrintf("  estimatedRows=%lld\n", p->estimatedRows);
}
#else
#define TRACE_IDX_INPUTS(A)
#define TRACE_IDX_OUTPUTS(A)
#endif

#ifndef SQLITE_OMIT_AUTOMATIC_INDEX
/*
** Return TRUE if the WHERE clause term pTerm is of a form where it
** could be used with an index to access pSrc, assuming an appropriate
** index existed.
*/
static int termCanDriveIndex(
  WhereTerm *pTerm,              /* WHERE clause term to check */
  struct SrcList_item *pSrc,     /* Table we are trying to access */
  Bitmask notReady               /* Tables in outer loops of the join */
){
  char aff;
  if( pTerm->leftCursor!=pSrc->iCursor ) return 0;
  if( (pTerm->eOperator & WO_EQ)==0 ) return 0;
  if( (pTerm->prereqRight & notReady)!=0 ) return 0;
  if( pTerm->u.leftColumn<0 ) return 0;
  aff = pSrc->pTab->aCol[pTerm->u.leftColumn].affinity;
  if( !sqlite3IndexAffinityOk(pTerm->pExpr, aff) ) return 0;
  return 1;
}
#endif


#ifndef SQLITE_OMIT_AUTOMATIC_INDEX
/*
** Generate code to construct the Index object for an automatic index
** and to set up the WhereLevel object pLevel so that the code generator
** makes use of the automatic index.
*/
static void constructAutomaticIndex(
  Parse *pParse,              /* The parsing context */
  WhereClause *pWC,           /* The WHERE clause */
  struct SrcList_item *pSrc,  /* The FROM clause term to get the next index */
  Bitmask notReady,           /* Mask of cursors that are not available */
  WhereLevel *pLevel          /* Write new index here */
){
  int nKeyCol;                /* Number of columns in the constructed index */
  WhereTerm *pTerm;           /* A single term of the WHERE clause */
  WhereTerm *pWCEnd;          /* End of pWC->a[] */
  Index *pIdx;                /* Object describing the transient index */
  Vdbe *v;                    /* Prepared statement under construction */
  int addrInit;               /* Address of the initialization bypass jump */
  Table *pTable;              /* The table being indexed */
  int addrTop;                /* Top of the index fill loop */
  int regRecord;              /* Register holding an index record */
  int n;                      /* Column counter */
  int i;                      /* Loop counter */
  int mxBitCol;               /* Maximum column in pSrc->colUsed */
  CollSeq *pColl;             /* Collating sequence to on a column */
  WhereLoop *pLoop;           /* The Loop object */
  char *zNotUsed;             /* Extra space on the end of pIdx */
  Bitmask idxCols;            /* Bitmap of columns used for indexing */
  Bitmask extraCols;          /* Bitmap of additional columns */
  u8 sentWarning = 0;         /* True if a warnning has been issued */
  Expr *pPartial = 0;         /* Partial Index Expression */
  int iContinue = 0;          /* Jump here to skip excluded rows */

  /* Generate code to skip over the creation and initialization of the
  ** transient index on 2nd and subsequent iterations of the loop. */
  v = pParse->pVdbe;
  assert( v!=0 );
  addrInit = sqlite3CodeOnce(pParse); VdbeCoverage(v);

  /* Count the number of columns that will be added to the index
  ** and used to match WHERE clause constraints */
  nKeyCol = 0;
  pTable = pSrc->pTab;
  pWCEnd = &pWC->a[pWC->nTerm];
  pLoop = pLevel->pWLoop;
  idxCols = 0;
  for(pTerm=pWC->a; pTerm<pWCEnd; pTerm++){
    Expr *pExpr = pTerm->pExpr;
    assert( !ExprHasProperty(pExpr, EP_FromJoin)    /* prereq always non-zero */
         || pExpr->iRightJoinTable!=pSrc->iCursor   /*   for the right-hand   */
         || pLoop->prereq!=0 );                     /*   table of a LEFT JOIN */
    if( pLoop->prereq==0
     && (pTerm->wtFlags & TERM_VIRTUAL)==0
     && !ExprHasProperty(pExpr, EP_FromJoin)
     && sqlite3ExprIsTableConstant(pExpr, pSrc->iCursor) ){
      pPartial = sqlite3ExprAnd(pParse->db, pPartial,
                                sqlite3ExprDup(pParse->db, pExpr, 0));
    }
    if( termCanDriveIndex(pTerm, pSrc, notReady) ){
      int iCol = pTerm->u.leftColumn;
      Bitmask cMask = iCol>=BMS ? MASKBIT(BMS-1) : MASKBIT(iCol);
      testcase( iCol==BMS );
      testcase( iCol==BMS-1 );
      if( !sentWarning ){
        sqlite3_log(SQLITE_WARNING_AUTOINDEX,
            "automatic index on %s(%s)", pTable->zName,
            pTable->aCol[iCol].zName);
        sentWarning = 1;
      }
      if( (idxCols & cMask)==0 ){
        if( whereLoopResize(pParse->db, pLoop, nKeyCol+1) ){
          goto end_auto_index_create;
        }
        pLoop->aLTerm[nKeyCol++] = pTerm;
        idxCols |= cMask;
      }
    }
  }
  assert( nKeyCol>0 );
  pLoop->u.btree.nEq = pLoop->nLTerm = nKeyCol;
  pLoop->wsFlags = WHERE_COLUMN_EQ | WHERE_IDX_ONLY | WHERE_INDEXED
                     | WHERE_AUTO_INDEX;

  /* Count the number of additional columns needed to create a
  ** covering index.  A "covering index" is an index that contains all
  ** columns that are needed by the query.  With a covering index, the
  ** original table never needs to be accessed.  Automatic indices must
  ** be a covering index because the index will not be updated if the
  ** original table changes and the index and table cannot both be used
  ** if they go out of sync.
  */
  extraCols = pSrc->colUsed & (~idxCols | MASKBIT(BMS-1));
  mxBitCol = MIN(BMS-1,pTable->nCol);
  testcase( pTable->nCol==BMS-1 );
  testcase( pTable->nCol==BMS-2 );
  for(i=0; i<mxBitCol; i++){
    if( extraCols & MASKBIT(i) ) nKeyCol++;
  }
  if( pSrc->colUsed & MASKBIT(BMS-1) ){
    nKeyCol += pTable->nCol - BMS + 1;
  }

  /* Construct the Index object to describe this index */
  pIdx = sqlite3AllocateIndexObject(pParse->db, nKeyCol+1, 0, &zNotUsed);
  if( pIdx==0 ) goto end_auto_index_create;
  pLoop->u.btree.pIndex = pIdx;
  pIdx->zName = "auto-index";
  pIdx->pTable = pTable;
  n = 0;
  idxCols = 0;
  for(pTerm=pWC->a; pTerm<pWCEnd; pTerm++){
    if( termCanDriveIndex(pTerm, pSrc, notReady) ){
      int iCol = pTerm->u.leftColumn;
      Bitmask cMask = iCol>=BMS ? MASKBIT(BMS-1) : MASKBIT(iCol);
      testcase( iCol==BMS-1 );
      testcase( iCol==BMS );
      if( (idxCols & cMask)==0 ){
        Expr *pX = pTerm->pExpr;
        idxCols |= cMask;
        pIdx->aiColumn[n] = pTerm->u.leftColumn;
        pColl = sqlite3BinaryCompareCollSeq(pParse, pX->pLeft, pX->pRight);
        pIdx->azColl[n] = ALWAYS(pColl) ? pColl->zName : "BINARY";
        n++;
      }
    }
  }
  assert( (u32)n==pLoop->u.btree.nEq );

  /* Add additional columns needed to make the automatic index into
  ** a covering index */
  for(i=0; i<mxBitCol; i++){
    if( extraCols & MASKBIT(i) ){
      pIdx->aiColumn[n] = i;
      pIdx->azColl[n] = "BINARY";
      n++;
    }
  }
  if( pSrc->colUsed & MASKBIT(BMS-1) ){
    for(i=BMS-1; i<pTable->nCol; i++){
      pIdx->aiColumn[n] = i;
      pIdx->azColl[n] = "BINARY";
      n++;
    }
  }
  assert( n==nKeyCol );
  pIdx->aiColumn[n] = -1;
  pIdx->azColl[n] = "BINARY";

  /* Create the automatic index */
  assert( pLevel->iIdxCur>=0 );
  pLevel->iIdxCur = pParse->nTab++;
  sqlite3VdbeAddOp2(v, OP_OpenAutoindex, pLevel->iIdxCur, nKeyCol+1);
  sqlite3VdbeSetP4KeyInfo(pParse, pIdx);
  VdbeComment((v, "for %s", pTable->zName));

  /* Fill the automatic index with content */
  sqlite3ExprCachePush(pParse);
  addrTop = sqlite3VdbeAddOp1(v, OP_Rewind, pLevel->iTabCur); VdbeCoverage(v);
  if( pPartial ){
    iContinue = sqlite3VdbeMakeLabel(v);
    sqlite3ExprIfFalse(pParse, pPartial, iContinue, SQLITE_JUMPIFNULL);
    pLoop->wsFlags |= WHERE_PARTIALIDX;
  }
  regRecord = sqlite3GetTempReg(pParse);
  sqlite3GenerateIndexKey(pParse, pIdx, pLevel->iTabCur, regRecord, 0, 0, 0, 0);
  sqlite3VdbeAddOp2(v, OP_IdxInsert, pLevel->iIdxCur, regRecord);
  sqlite3VdbeChangeP5(v, OPFLAG_USESEEKRESULT);
  if( pPartial ) sqlite3VdbeResolveLabel(v, iContinue);
  sqlite3VdbeAddOp2(v, OP_Next, pLevel->iTabCur, addrTop+1); VdbeCoverage(v);
  sqlite3VdbeChangeP5(v, SQLITE_STMTSTATUS_AUTOINDEX);
  sqlite3VdbeJumpHere(v, addrTop);
  sqlite3ReleaseTempReg(pParse, regRecord);
  sqlite3ExprCachePop(pParse);
  
  /* Jump here when skipping the initialization */
  sqlite3VdbeJumpHere(v, addrInit);

end_auto_index_create:
  sqlite3ExprDelete(pParse->db, pPartial);
}
#endif /* SQLITE_OMIT_AUTOMATIC_INDEX */

#ifndef SQLITE_OMIT_VIRTUALTABLE
/*
** Allocate and populate an sqlite3_index_info structure. It is the 
** responsibility of the caller to eventually release the structure
** by passing the pointer returned by this function to sqlite3_free().
*/
static sqlite3_index_info *allocateIndexInfo(
  Parse *pParse,
  WhereClause *pWC,
  struct SrcList_item *pSrc,
  ExprList *pOrderBy
){
  int i, j;
  int nTerm;
  struct sqlite3_index_constraint *pIdxCons;
  struct sqlite3_index_orderby *pIdxOrderBy;
  struct sqlite3_index_constraint_usage *pUsage;
  WhereTerm *pTerm;
  int nOrderBy;
  sqlite3_index_info *pIdxInfo;

  /* Count the number of possible WHERE clause constraints referring
  ** to this virtual table */
  for(i=nTerm=0, pTerm=pWC->a; i<pWC->nTerm; i++, pTerm++){
    if( pTerm->leftCursor != pSrc->iCursor ) continue;
    assert( IsPowerOfTwo(pTerm->eOperator & ~WO_EQUIV) );
    testcase( pTerm->eOperator & WO_IN );
    testcase( pTerm->eOperator & WO_ISNULL );
    testcase( pTerm->eOperator & WO_ALL );
    if( (pTerm->eOperator & ~(WO_ISNULL|WO_EQUIV))==0 ) continue;
    if( pTerm->wtFlags & TERM_VNULL ) continue;
    nTerm++;
  }

  /* If the ORDER BY clause contains only columns in the current 
  ** virtual table then allocate space for the aOrderBy part of
  ** the sqlite3_index_info structure.
  */
  nOrderBy = 0;
  if( pOrderBy ){
    int n = pOrderBy->nExpr;
    for(i=0; i<n; i++){
      Expr *pExpr = pOrderBy->a[i].pExpr;
      if( pExpr->op!=TK_COLUMN || pExpr->iTable!=pSrc->iCursor ) break;
    }
    if( i==n){
      nOrderBy = n;
    }
  }

  /* Allocate the sqlite3_index_info structure
  */
  pIdxInfo = sqlite3DbMallocZero(pParse->db, sizeof(*pIdxInfo)
                           + (sizeof(*pIdxCons) + sizeof(*pUsage))*nTerm
                           + sizeof(*pIdxOrderBy)*nOrderBy );
  if( pIdxInfo==0 ){
    sqlite3ErrorMsg(pParse, "out of memory");
    return 0;
  }

  /* Initialize the structure.  The sqlite3_index_info structure contains
  ** many fields that are declared "const" to prevent xBestIndex from
  ** changing them.  We have to do some funky casting in order to
  ** initialize those fields.
  */
  pIdxCons = (struct sqlite3_index_constraint*)&pIdxInfo[1];
  pIdxOrderBy = (struct sqlite3_index_orderby*)&pIdxCons[nTerm];
  pUsage = (struct sqlite3_index_constraint_usage*)&pIdxOrderBy[nOrderBy];
  *(int*)&pIdxInfo->nConstraint = nTerm;
  *(int*)&pIdxInfo->nOrderBy = nOrderBy;
  *(struct sqlite3_index_constraint**)&pIdxInfo->aConstraint = pIdxCons;
  *(struct sqlite3_index_orderby**)&pIdxInfo->aOrderBy = pIdxOrderBy;
  *(struct sqlite3_index_constraint_usage**)&pIdxInfo->aConstraintUsage =
                                                                   pUsage;

  for(i=j=0, pTerm=pWC->a; i<pWC->nTerm; i++, pTerm++){
    u8 op;
    if( pTerm->leftCursor != pSrc->iCursor ) continue;
    assert( IsPowerOfTwo(pTerm->eOperator & ~WO_EQUIV) );
    testcase( pTerm->eOperator & WO_IN );
    testcase( pTerm->eOperator & WO_ISNULL );
    testcase( pTerm->eOperator & WO_ALL );
    if( (pTerm->eOperator & ~(WO_ISNULL|WO_EQUIV))==0 ) continue;
    if( pTerm->wtFlags & TERM_VNULL ) continue;
    pIdxCons[j].iColumn = pTerm->u.leftColumn;
    pIdxCons[j].iTermOffset = i;
    op = (u8)pTerm->eOperator & WO_ALL;
    if( op==WO_IN ) op = WO_EQ;
    pIdxCons[j].op = op;
    /* The direct assignment in the previous line is possible only because
    ** the WO_ and SQLITE_INDEX_CONSTRAINT_ codes are identical.  The
    ** following asserts verify this fact. */
    assert( WO_EQ==SQLITE_INDEX_CONSTRAINT_EQ );
    assert( WO_LT==SQLITE_INDEX_CONSTRAINT_LT );
    assert( WO_LE==SQLITE_INDEX_CONSTRAINT_LE );
    assert( WO_GT==SQLITE_INDEX_CONSTRAINT_GT );
    assert( WO_GE==SQLITE_INDEX_CONSTRAINT_GE );
    assert( WO_MATCH==SQLITE_INDEX_CONSTRAINT_MATCH );
    assert( pTerm->eOperator & (WO_IN|WO_EQ|WO_LT|WO_LE|WO_GT|WO_GE|WO_MATCH) );
    j++;
  }
  for(i=0; i<nOrderBy; i++){
    Expr *pExpr = pOrderBy->a[i].pExpr;
    pIdxOrderBy[i].iColumn = pExpr->iColumn;
    pIdxOrderBy[i].desc = pOrderBy->a[i].sortOrder;
  }

  return pIdxInfo;
}

/*
** The table object reference passed as the second argument to this function
** must represent a virtual table. This function invokes the xBestIndex()
** method of the virtual table with the sqlite3_index_info object that
** comes in as the 3rd argument to this function.
**
** If an error occurs, pParse is populated with an error message and a
** non-zero value is returned. Otherwise, 0 is returned and the output
** part of the sqlite3_index_info structure is left populated.
**
** Whether or not an error is returned, it is the responsibility of the
** caller to eventually free p->idxStr if p->needToFreeIdxStr indicates
** that this is required.
*/
static int vtabBestIndex(Parse *pParse, Table *pTab, sqlite3_index_info *p){
  sqlite3_vtab *pVtab = sqlite3GetVTable(pParse->db, pTab)->pVtab;
  int i;
  int rc;

  TRACE_IDX_INPUTS(p);
  rc = pVtab->pModule->xBestIndex(pVtab, p);
  TRACE_IDX_OUTPUTS(p);

  if( rc!=SQLITE_OK ){
    if( rc==SQLITE_NOMEM ){
      pParse->db->mallocFailed = 1;
    }else if( !pVtab->zErrMsg ){
      sqlite3ErrorMsg(pParse, "%s", sqlite3ErrStr(rc));
    }else{
      sqlite3ErrorMsg(pParse, "%s", pVtab->zErrMsg);
    }
  }
  sqlite3_free(pVtab->zErrMsg);
  pVtab->zErrMsg = 0;

  for(i=0; i<p->nConstraint; i++){
    if( !p->aConstraint[i].usable && p->aConstraintUsage[i].argvIndex>0 ){
      sqlite3ErrorMsg(pParse, 
          "table %s: xBestIndex returned an invalid plan", pTab->zName);
    }
  }

  return pParse->nErr;
}
#endif /* !defined(SQLITE_OMIT_VIRTUALTABLE) */

#ifdef SQLITE_ENABLE_STAT3_OR_STAT4
/*
** Estimate the location of a particular key among all keys in an
** index.  Store the results in aStat as follows:
**
**    aStat[0]      Est. number of rows less than pRec
**    aStat[1]      Est. number of rows equal to pRec
**
** Return the index of the sample that is the smallest sample that
** is greater than or equal to pRec. Note that this index is not an index
** into the aSample[] array - it is an index into a virtual set of samples
** based on the contents of aSample[] and the number of fields in record 
** pRec. 
*/
static int whereKeyStats(
  Parse *pParse,              /* Database connection */
  Index *pIdx,                /* Index to consider domain of */
  UnpackedRecord *pRec,       /* Vector of values to consider */
  int roundUp,                /* Round up if true.  Round down if false */
  tRowcnt *aStat              /* OUT: stats written here */
){
  IndexSample *aSample = pIdx->aSample;
  int iCol;                   /* Index of required stats in anEq[] etc. */
  int i;                      /* Index of first sample >= pRec */
  int iSample;                /* Smallest sample larger than or equal to pRec */
  int iMin = 0;               /* Smallest sample not yet tested */
  int iTest;                  /* Next sample to test */
  int res;                    /* Result of comparison operation */
  int nField;                 /* Number of fields in pRec */
  tRowcnt iLower = 0;         /* anLt[] + anEq[] of largest sample pRec is > */

#ifndef SQLITE_DEBUG
  UNUSED_PARAMETER( pParse );
#endif
  assert( pRec!=0 );
  assert( pIdx->nSample>0 );
  assert( pRec->nField>0 && pRec->nField<=pIdx->nSampleCol );

  /* Do a binary search to find the first sample greater than or equal
  ** to pRec. If pRec contains a single field, the set of samples to search
  ** is simply the aSample[] array. If the samples in aSample[] contain more
  ** than one fields, all fields following the first are ignored.
  **
  ** If pRec contains N fields, where N is more than one, then as well as the
  ** samples in aSample[] (truncated to N fields), the search also has to
  ** consider prefixes of those samples. For example, if the set of samples
  ** in aSample is:
  **
  **     aSample[0] = (a, 5) 
  **     aSample[1] = (a, 10) 
  **     aSample[2] = (b, 5) 
  **     aSample[3] = (c, 100) 
  **     aSample[4] = (c, 105)
  **
  ** Then the search space should ideally be the samples above and the 
  ** unique prefixes [a], [b] and [c]. But since that is hard to organize, 
  ** the code actually searches this set:
  **
  **     0: (a) 
  **     1: (a, 5) 
  **     2: (a, 10) 
  **     3: (a, 10) 
  **     4: (b) 
  **     5: (b, 5) 
  **     6: (c) 
  **     7: (c, 100) 
  **     8: (c, 105)
  **     9: (c, 105)
  **
  ** For each sample in the aSample[] array, N samples are present in the
  ** effective sample array. In the above, samples 0 and 1 are based on 
  ** sample aSample[0]. Samples 2 and 3 on aSample[1] etc.
  **
  ** Often, sample i of each block of N effective samples has (i+1) fields.
  ** Except, each sample may be extended to ensure that it is greater than or
  ** equal to the previous sample in the array. For example, in the above, 
  ** sample 2 is the first sample of a block of N samples, so at first it 
  ** appears that it should be 1 field in size. However, that would make it 
  ** smaller than sample 1, so the binary search would not work. As a result, 
  ** it is extended to two fields. The duplicates that this creates do not 
  ** cause any problems.
  */
  nField = pRec->nField;
  iCol = 0;
  iSample = pIdx->nSample * nField;
  do{
    int iSamp;                    /* Index in aSample[] of test sample */
    int n;                        /* Number of fields in test sample */

    iTest = (iMin+iSample)/2;
    iSamp = iTest / nField;
    if( iSamp>0 ){
      /* The proposed effective sample is a prefix of sample aSample[iSamp].
      ** Specifically, the shortest prefix of at least (1 + iTest%nField) 
      ** fields that is greater than the previous effective sample.  */
      for(n=(iTest % nField) + 1; n<nField; n++){
        if( aSample[iSamp-1].anLt[n-1]!=aSample[iSamp].anLt[n-1] ) break;
      }
    }else{
      n = iTest + 1;
    }

    pRec->nField = n;
    res = sqlite3VdbeRecordCompare(aSample[iSamp].n, aSample[iSamp].p, pRec);
    if( res<0 ){
      iLower = aSample[iSamp].anLt[n-1] + aSample[iSamp].anEq[n-1];
      iMin = iTest+1;
    }else if( res==0 && n<nField ){
      iLower = aSample[iSamp].anLt[n-1];
      iMin = iTest+1;
      res = -1;
    }else{
      iSample = iTest;
      iCol = n-1;
    }
  }while( res && iMin<iSample );
  i = iSample / nField;

#ifdef SQLITE_DEBUG
  /* The following assert statements check that the binary search code
  ** above found the right answer. This block serves no purpose other
  ** than to invoke the asserts.  */
  if( pParse->db->mallocFailed==0 ){
    if( res==0 ){
      /* If (res==0) is true, then pRec must be equal to sample i. */
      assert( i<pIdx->nSample );
      assert( iCol==nField-1 );
      pRec->nField = nField;
      assert( 0==sqlite3VdbeRecordCompare(aSample[i].n, aSample[i].p, pRec) 
           || pParse->db->mallocFailed 
      );
    }else{
      /* Unless i==pIdx->nSample, indicating that pRec is larger than
      ** all samples in the aSample[] array, pRec must be smaller than the
      ** (iCol+1) field prefix of sample i.  */
      assert( i<=pIdx->nSample && i>=0 );
      pRec->nField = iCol+1;
      assert( i==pIdx->nSample 
           || sqlite3VdbeRecordCompare(aSample[i].n, aSample[i].p, pRec)>0
           || pParse->db->mallocFailed );

      /* if i==0 and iCol==0, then record pRec is smaller than all samples
      ** in the aSample[] array. Otherwise, if (iCol>0) then pRec must
      ** be greater than or equal to the (iCol) field prefix of sample i.
      ** If (i>0), then pRec must also be greater than sample (i-1).  */
      if( iCol>0 ){
        pRec->nField = iCol;
        assert( sqlite3VdbeRecordCompare(aSample[i].n, aSample[i].p, pRec)<=0
             || pParse->db->mallocFailed );
      }
      if( i>0 ){
        pRec->nField = nField;
        assert( sqlite3VdbeRecordCompare(aSample[i-1].n, aSample[i-1].p, pRec)<0
             || pParse->db->mallocFailed );
      }
    }
  }
#endif /* ifdef SQLITE_DEBUG */

  if( res==0 ){
    /* Record pRec is equal to sample i */
    assert( iCol==nField-1 );
    aStat[0] = aSample[i].anLt[iCol];
    aStat[1] = aSample[i].anEq[iCol];
  }else{
    /* At this point, the (iCol+1) field prefix of aSample[i] is the first 
    ** sample that is greater than pRec. Or, if i==pIdx->nSample then pRec
    ** is larger than all samples in the array. */
    tRowcnt iUpper, iGap;
    if( i>=pIdx->nSample ){
      iUpper = sqlite3LogEstToInt(pIdx->aiRowLogEst[0]);
    }else{
      iUpper = aSample[i].anLt[iCol];
    }

    if( iLower>=iUpper ){
      iGap = 0;
    }else{
      iGap = iUpper - iLower;
    }
    if( roundUp ){
      iGap = (iGap*2)/3;
    }else{
      iGap = iGap/3;
    }
    aStat[0] = iLower + iGap;
    aStat[1] = pIdx->aAvgEq[iCol];
  }

  /* Restore the pRec->nField value before returning.  */
  pRec->nField = nField;
  return i;
}
#endif /* SQLITE_ENABLE_STAT3_OR_STAT4 */

/*
** If it is not NULL, pTerm is a term that provides an upper or lower
** bound on a range scan. Without considering pTerm, it is estimated 
** that the scan will visit nNew rows. This function returns the number
** estimated to be visited after taking pTerm into account.
**
** If the user explicitly specified a likelihood() value for this term,
** then the return value is the likelihood multiplied by the number of
** input rows. Otherwise, this function assumes that an "IS NOT NULL" term
** has a likelihood of 0.50, and any other term a likelihood of 0.25.
*/
static LogEst whereRangeAdjust(WhereTerm *pTerm, LogEst nNew){
  LogEst nRet = nNew;
  if( pTerm ){
    if( pTerm->truthProb<=0 ){
      nRet += pTerm->truthProb;
    }else if( (pTerm->wtFlags & TERM_VNULL)==0 ){
      nRet -= 20;        assert( 20==sqlite3LogEst(4) );
    }
  }
  return nRet;
}

#ifdef SQLITE_ENABLE_STAT3_OR_STAT4
/* 
** This function is called to estimate the number of rows visited by a
** range-scan on a skip-scan index. For example:
**
**   CREATE INDEX i1 ON t1(a, b, c);
**   SELECT * FROM t1 WHERE a=? AND c BETWEEN ? AND ?;
**
** Value pLoop->nOut is currently set to the estimated number of rows 
** visited for scanning (a=? AND b=?). This function reduces that estimate 
** by some factor to account for the (c BETWEEN ? AND ?) expression based
** on the stat4 data for the index. this scan will be peformed multiple 
** times (once for each (a,b) combination that matches a=?) is dealt with 
** by the caller.
**
** It does this by scanning through all stat4 samples, comparing values
** extracted from pLower and pUpper with the corresponding column in each
** sample. If L and U are the number of samples found to be less than or
** equal to the values extracted from pLower and pUpper respectively, and
** N is the total number of samples, the pLoop->nOut value is adjusted
** as follows:
**
**   nOut = nOut * ( min(U - L, 1) / N )
**
** If pLower is NULL, or a value cannot be extracted from the term, L is
** set to zero. If pUpper is NULL, or a value cannot be extracted from it,
** U is set to N.
**
** Normally, this function sets *pbDone to 1 before returning. However,
** if no value can be extracted from either pLower or pUpper (and so the
** estimate of the number of rows delivered remains unchanged), *pbDone
** is left as is.
**
** If an error occurs, an SQLite error code is returned. Otherwise, 
** SQLITE_OK.
*/
static int whereRangeSkipScanEst(
  Parse *pParse,       /* Parsing & code generating context */
  WhereTerm *pLower,   /* Lower bound on the range. ex: "x>123" Might be NULL */
  WhereTerm *pUpper,   /* Upper bound on the range. ex: "x<455" Might be NULL */
  WhereLoop *pLoop,    /* Update the .nOut value of this loop */
  int *pbDone          /* Set to true if at least one expr. value extracted */
){
  Index *p = pLoop->u.btree.pIndex;
  int nEq = pLoop->u.btree.nEq;
  sqlite3 *db = pParse->db;
  int nLower = -1;
  int nUpper = p->nSample+1;
  int rc = SQLITE_OK;
  int iCol = p->aiColumn[nEq];
  u8 aff = iCol>=0 ? p->pTable->aCol[iCol].affinity : SQLITE_AFF_INTEGER;
  CollSeq *pColl;
  
  sqlite3_value *p1 = 0;          /* Value extracted from pLower */
  sqlite3_value *p2 = 0;          /* Value extracted from pUpper */
  sqlite3_value *pVal = 0;        /* Value extracted from record */

  pColl = sqlite3LocateCollSeq(pParse, p->azColl[nEq]);
  if( pLower ){
    rc = sqlite3Stat4ValueFromExpr(pParse, pLower->pExpr->pRight, aff, &p1);
    nLower = 0;
  }
  if( pUpper && rc==SQLITE_OK ){
    rc = sqlite3Stat4ValueFromExpr(pParse, pUpper->pExpr->pRight, aff, &p2);
    nUpper = p2 ? 0 : p->nSample;
  }

  if( p1 || p2 ){
    int i;
    int nDiff;
    for(i=0; rc==SQLITE_OK && i<p->nSample; i++){
      rc = sqlite3Stat4Column(db, p->aSample[i].p, p->aSample[i].n, nEq, &pVal);
      if( rc==SQLITE_OK && p1 ){
        int res = sqlite3MemCompare(p1, pVal, pColl);
        if( res>=0 ) nLower++;
      }
      if( rc==SQLITE_OK && p2 ){
        int res = sqlite3MemCompare(p2, pVal, pColl);
        if( res>=0 ) nUpper++;
      }
    }
    nDiff = (nUpper - nLower);
    if( nDiff<=0 ) nDiff = 1;

    /* If there is both an upper and lower bound specified, and the 
    ** comparisons indicate that they are close together, use the fallback
    ** method (assume that the scan visits 1/64 of the rows) for estimating
    ** the number of rows visited. Otherwise, estimate the number of rows
    ** using the method described in the header comment for this function. */
    if( nDiff!=1 || pUpper==0 || pLower==0 ){
      int nAdjust = (sqlite3LogEst(p->nSample) - sqlite3LogEst(nDiff));
      pLoop->nOut -= nAdjust;
      *pbDone = 1;
      WHERETRACE(0x10, ("range skip-scan regions: %u..%u  adjust=%d est=%d\n",
                           nLower, nUpper, nAdjust*-1, pLoop->nOut));
    }

  }else{
    assert( *pbDone==0 );
  }

  sqlite3ValueFree(p1);
  sqlite3ValueFree(p2);
  sqlite3ValueFree(pVal);

  return rc;
}
#endif /* SQLITE_ENABLE_STAT3_OR_STAT4 */

/*
** This function is used to estimate the number of rows that will be visited
** by scanning an index for a range of values. The range may have an upper
** bound, a lower bound, or both. The WHERE clause terms that set the upper
** and lower bounds are represented by pLower and pUpper respectively. For
** example, assuming that index p is on t1(a):
**
**   ... FROM t1 WHERE a > ? AND a < ? ...
**                    |_____|   |_____|
**                       |         |
**                     pLower    pUpper
**
** If either of the upper or lower bound is not present, then NULL is passed in
** place of the corresponding WhereTerm.
**
** The value in (pBuilder->pNew->u.btree.nEq) is the number of the index
** column subject to the range constraint. Or, equivalently, the number of
** equality constraints optimized by the proposed index scan. For example,
** assuming index p is on t1(a, b), and the SQL query is:
**
**   ... FROM t1 WHERE a = ? AND b > ? AND b < ? ...
**
** then nEq is set to 1 (as the range restricted column, b, is the second 
** left-most column of the index). Or, if the query is:
**
**   ... FROM t1 WHERE a > ? AND a < ? ...
**
** then nEq is set to 0.
**
** When this function is called, *pnOut is set to the sqlite3LogEst() of the
** number of rows that the index scan is expected to visit without 
** considering the range constraints. If nEq is 0, then *pnOut is the number of 
** rows in the index. Assuming no error occurs, *pnOut is adjusted (reduced)
** to account for the range constraints pLower and pUpper.
** 
** In the absence of sqlite_stat4 ANALYZE data, or if such data cannot be
** used, a single range inequality reduces the search space by a factor of 4. 
** and a pair of constraints (x>? AND x<?) reduces the expected number of
** rows visited by a factor of 64.
*/
static int whereRangeScanEst(
  Parse *pParse,       /* Parsing & code generating context */
  WhereLoopBuilder *pBuilder,
  WhereTerm *pLower,   /* Lower bound on the range. ex: "x>123" Might be NULL */
  WhereTerm *pUpper,   /* Upper bound on the range. ex: "x<455" Might be NULL */
  WhereLoop *pLoop     /* Modify the .nOut and maybe .rRun fields */
){
  int rc = SQLITE_OK;
  int nOut = pLoop->nOut;
  LogEst nNew;

#ifdef SQLITE_ENABLE_STAT3_OR_STAT4
  Index *p = pLoop->u.btree.pIndex;
  int nEq = pLoop->u.btree.nEq;

  if( p->nSample>0 && nEq<p->nSampleCol ){
    if( nEq==pBuilder->nRecValid ){
      UnpackedRecord *pRec = pBuilder->pRec;
      tRowcnt a[2];
      u8 aff;

      /* Variable iLower will be set to the estimate of the number of rows in 
      ** the index that are less than the lower bound of the range query. The
      ** lower bound being the concatenation of $P and $L, where $P is the
      ** key-prefix formed by the nEq values matched against the nEq left-most
      ** columns of the index, and $L is the value in pLower.
      **
      ** Or, if pLower is NULL or $L cannot be extracted from it (because it
      ** is not a simple variable or literal value), the lower bound of the
      ** range is $P. Due to a quirk in the way whereKeyStats() works, even
      ** if $L is available, whereKeyStats() is called for both ($P) and 
      ** ($P:$L) and the larger of the two returned values is used.
      **
      ** Similarly, iUpper is to be set to the estimate of the number of rows
      ** less than the upper bound of the range query. Where the upper bound
      ** is either ($P) or ($P:$U). Again, even if $U is available, both values
      ** of iUpper are requested of whereKeyStats() and the smaller used.
      **
      ** The number of rows between the two bounds is then just iUpper-iLower.
      */
      tRowcnt iLower;     /* Rows less than the lower bound */
      tRowcnt iUpper;     /* Rows less than the upper bound */
      int iLwrIdx = -2;   /* aSample[] for the lower bound */
      int iUprIdx = -1;   /* aSample[] for the upper bound */

      if( pRec ){
        testcase( pRec->nField!=pBuilder->nRecValid );
        pRec->nField = pBuilder->nRecValid;
      }
      if( nEq==p->nKeyCol ){
        aff = SQLITE_AFF_INTEGER;
      }else{
        aff = p->pTable->aCol[p->aiColumn[nEq]].affinity;
      }
      /* Determine iLower and iUpper using ($P) only. */
      if( nEq==0 ){
        iLower = 0;
        iUpper = p->nRowEst0;
      }else{
        /* Note: this call could be optimized away - since the same values must 
        ** have been requested when testing key $P in whereEqualScanEst().  */
        whereKeyStats(pParse, p, pRec, 0, a);
        iLower = a[0];
        iUpper = a[0] + a[1];
      }

      assert( pLower==0 || (pLower->eOperator & (WO_GT|WO_GE))!=0 );
      assert( pUpper==0 || (pUpper->eOperator & (WO_LT|WO_LE))!=0 );
      assert( p->aSortOrder!=0 );
      if( p->aSortOrder[nEq] ){
        /* The roles of pLower and pUpper are swapped for a DESC index */
        SWAP(WhereTerm*, pLower, pUpper);
      }

      /* If possible, improve on the iLower estimate using ($P:$L). */
      if( pLower ){
        int bOk;                    /* True if value is extracted from pExpr */
        Expr *pExpr = pLower->pExpr->pRight;
        rc = sqlite3Stat4ProbeSetValue(pParse, p, &pRec, pExpr, aff, nEq, &bOk);
        if( rc==SQLITE_OK && bOk ){
          tRowcnt iNew;
          iLwrIdx = whereKeyStats(pParse, p, pRec, 0, a);
          iNew = a[0] + ((pLower->eOperator & (WO_GT|WO_LE)) ? a[1] : 0);
          if( iNew>iLower ) iLower = iNew;
          nOut--;
          pLower = 0;
        }
      }

      /* If possible, improve on the iUpper estimate using ($P:$U). */
      if( pUpper ){
        int bOk;                    /* True if value is extracted from pExpr */
        Expr *pExpr = pUpper->pExpr->pRight;
        rc = sqlite3Stat4ProbeSetValue(pParse, p, &pRec, pExpr, aff, nEq, &bOk);
        if( rc==SQLITE_OK && bOk ){
          tRowcnt iNew;
          iUprIdx = whereKeyStats(pParse, p, pRec, 1, a);
          iNew = a[0] + ((pUpper->eOperator & (WO_GT|WO_LE)) ? a[1] : 0);
          if( iNew<iUpper ) iUpper = iNew;
          nOut--;
          pUpper = 0;
        }
      }

      pBuilder->pRec = pRec;
      if( rc==SQLITE_OK ){
        if( iUpper>iLower ){
          nNew = sqlite3LogEst(iUpper - iLower);
          /* TUNING:  If both iUpper and iLower are derived from the same
          ** sample, then assume they are 4x more selective.  This brings
          ** the estimated selectivity more in line with what it would be
          ** if estimated without the use of STAT3/4 tables. */
          if( iLwrIdx==iUprIdx ) nNew -= 20;  assert( 20==sqlite3LogEst(4) );
        }else{
          nNew = 10;        assert( 10==sqlite3LogEst(2) );
        }
        if( nNew<nOut ){
          nOut = nNew;
        }
        WHERETRACE(0x10, ("STAT4 range scan: %u..%u  est=%d\n",
                           (u32)iLower, (u32)iUpper, nOut));
      }
    }else{
      int bDone = 0;
      rc = whereRangeSkipScanEst(pParse, pLower, pUpper, pLoop, &bDone);
      if( bDone ) return rc;
    }
  }
#else
  UNUSED_PARAMETER(pParse);
  UNUSED_PARAMETER(pBuilder);
  assert( pLower || pUpper );
#endif
  assert( pUpper==0 || (pUpper->wtFlags & TERM_VNULL)==0 );
  nNew = whereRangeAdjust(pLower, nOut);
  nNew = whereRangeAdjust(pUpper, nNew);

  /* TUNING: If there is both an upper and lower limit and neither limit
  ** has an application-defined likelihood(), assume the range is
  ** reduced by an additional 75%. This means that, by default, an open-ended
  ** range query (e.g. col > ?) is assumed to match 1/4 of the rows in the
  ** index. While a closed range (e.g. col BETWEEN ? AND ?) is estimated to
  ** match 1/64 of the index. */ 
  if( pLower && pLower->truthProb>0 && pUpper && pUpper->truthProb>0 ){
    nNew -= 20;
  }

  nOut -= (pLower!=0) + (pUpper!=0);
  if( nNew<10 ) nNew = 10;
  if( nNew<nOut ) nOut = nNew;
#if defined(WHERETRACE_ENABLED)
  if( pLoop->nOut>nOut ){
    WHERETRACE(0x10,("Range scan lowers nOut from %d to %d\n",
                    pLoop->nOut, nOut));
  }
#endif
  pLoop->nOut = (LogEst)nOut;
  return rc;
}

#ifdef SQLITE_ENABLE_STAT3_OR_STAT4
/*
** Estimate the number of rows that will be returned based on
** an equality constraint x=VALUE and where that VALUE occurs in
** the histogram data.  This only works when x is the left-most
** column of an index and sqlite_stat3 histogram data is available
** for that index.  When pExpr==NULL that means the constraint is
** "x IS NULL" instead of "x=VALUE".
**
** Write the estimated row count into *pnRow and return SQLITE_OK. 
** If unable to make an estimate, leave *pnRow unchanged and return
** non-zero.
**
** This routine can fail if it is unable to load a collating sequence
** required for string comparison, or if unable to allocate memory
** for a UTF conversion required for comparison.  The error is stored
** in the pParse structure.
*/
static int whereEqualScanEst(
  Parse *pParse,       /* Parsing & code generating context */
  WhereLoopBuilder *pBuilder,
  Expr *pExpr,         /* Expression for VALUE in the x=VALUE constraint */
  tRowcnt *pnRow       /* Write the revised row estimate here */
){
  Index *p = pBuilder->pNew->u.btree.pIndex;
  int nEq = pBuilder->pNew->u.btree.nEq;
  UnpackedRecord *pRec = pBuilder->pRec;
  u8 aff;                   /* Column affinity */
  int rc;                   /* Subfunction return code */
  tRowcnt a[2];             /* Statistics */
  int bOk;

  assert( nEq>=1 );
  assert( nEq<=p->nColumn );
  assert( p->aSample!=0 );
  assert( p->nSample>0 );
  assert( pBuilder->nRecValid<nEq );

  /* If values are not available for all fields of the index to the left
  ** of this one, no estimate can be made. Return SQLITE_NOTFOUND. */
  if( pBuilder->nRecValid<(nEq-1) ){
    return SQLITE_NOTFOUND;
  }

  /* This is an optimization only. The call to sqlite3Stat4ProbeSetValue()
  ** below would return the same value.  */
  if( nEq>=p->nColumn ){
    *pnRow = 1;
    return SQLITE_OK;
  }

  aff = p->pTable->aCol[p->aiColumn[nEq-1]].affinity;
  rc = sqlite3Stat4ProbeSetValue(pParse, p, &pRec, pExpr, aff, nEq-1, &bOk);
  pBuilder->pRec = pRec;
  if( rc!=SQLITE_OK ) return rc;
  if( bOk==0 ) return SQLITE_NOTFOUND;
  pBuilder->nRecValid = nEq;

  whereKeyStats(pParse, p, pRec, 0, a);
  WHERETRACE(0x10,("equality scan regions: %d\n", (int)a[1]));
  *pnRow = a[1];
  
  return rc;
}
#endif /* SQLITE_ENABLE_STAT3_OR_STAT4 */

#ifdef SQLITE_ENABLE_STAT3_OR_STAT4
/*
** Estimate the number of rows that will be returned based on
** an IN constraint where the right-hand side of the IN operator
** is a list of values.  Example:
**
**        WHERE x IN (1,2,3,4)
**
** Write the estimated row count into *pnRow and return SQLITE_OK. 
** If unable to make an estimate, leave *pnRow unchanged and return
** non-zero.
**
** This routine can fail if it is unable to load a collating sequence
** required for string comparison, or if unable to allocate memory
** for a UTF conversion required for comparison.  The error is stored
** in the pParse structure.
*/
static int whereInScanEst(
  Parse *pParse,       /* Parsing & code generating context */
  WhereLoopBuilder *pBuilder,
  ExprList *pList,     /* The value list on the RHS of "x IN (v1,v2,v3,...)" */
  tRowcnt *pnRow       /* Write the revised row estimate here */
){
  Index *p = pBuilder->pNew->u.btree.pIndex;
  i64 nRow0 = sqlite3LogEstToInt(p->aiRowLogEst[0]);
  int nRecValid = pBuilder->nRecValid;
  int rc = SQLITE_OK;     /* Subfunction return code */
  tRowcnt nEst;           /* Number of rows for a single term */
  tRowcnt nRowEst = 0;    /* New estimate of the number of rows */
  int i;                  /* Loop counter */

  assert( p->aSample!=0 );
  for(i=0; rc==SQLITE_OK && i<pList->nExpr; i++){
    nEst = nRow0;
    rc = whereEqualScanEst(pParse, pBuilder, pList->a[i].pExpr, &nEst);
    nRowEst += nEst;
    pBuilder->nRecValid = nRecValid;
  }

  if( rc==SQLITE_OK ){
    if( nRowEst > nRow0 ) nRowEst = nRow0;
    *pnRow = nRowEst;
    WHERETRACE(0x10,("IN row estimate: est=%d\n", nRowEst));
  }
  assert( pBuilder->nRecValid==nRecValid );
  return rc;
}
#endif /* SQLITE_ENABLE_STAT3_OR_STAT4 */

/*
** Disable a term in the WHERE clause.  Except, do not disable the term
** if it controls a LEFT OUTER JOIN and it did not originate in the ON
** or USING clause of that join.
**
** Consider the term t2.z='ok' in the following queries:
**
**   (1)  SELECT * FROM t1 LEFT JOIN t2 ON t1.a=t2.x WHERE t2.z='ok'
**   (2)  SELECT * FROM t1 LEFT JOIN t2 ON t1.a=t2.x AND t2.z='ok'
**   (3)  SELECT * FROM t1, t2 WHERE t1.a=t2.x AND t2.z='ok'
**
** The t2.z='ok' is disabled in the in (2) because it originates
** in the ON clause.  The term is disabled in (3) because it is not part
** of a LEFT OUTER JOIN.  In (1), the term is not disabled.
**
** Disabling a term causes that term to not be tested in the inner loop
** of the join.  Disabling is an optimization.  When terms are satisfied
** by indices, we disable them to prevent redundant tests in the inner
** loop.  We would get the correct results if nothing were ever disabled,
** but joins might run a little slower.  The trick is to disable as much
** as we can without disabling too much.  If we disabled in (1), we'd get
** the wrong answer.  See ticket #813.
**
** If all the children of a term are disabled, then that term is also
** automatically disabled.  In this way, terms get disabled if derived
** virtual terms are tested first.  For example:
**
**      x GLOB 'abc*' AND x>='abc' AND x<'acd'
**      \___________/     \______/     \_____/
**         parent          child1       child2
**
** Only the parent term was in the original WHERE clause.  The child1
** and child2 terms were added by the LIKE optimization.  If both of
** the virtual child terms are valid, then testing of the parent can be 
** skipped.
**
** Usually the parent term is marked as TERM_CODED.  But if the parent
** term was originally TERM_LIKE, then the parent gets TERM_LIKECOND instead.
** The TERM_LIKECOND marking indicates that the term should be coded inside
** a conditional such that is only evaluated on the second pass of a
** LIKE-optimization loop, when scanning BLOBs instead of strings.
*/
static void disableTerm(WhereLevel *pLevel, WhereTerm *pTerm){
  int nLoop = 0;
  while( pTerm
      && (pTerm->wtFlags & TERM_CODED)==0
      && (pLevel->iLeftJoin==0 || ExprHasProperty(pTerm->pExpr, EP_FromJoin))
      && (pLevel->notReady & pTerm->prereqAll)==0
  ){
    if( nLoop && (pTerm->wtFlags & TERM_LIKE)!=0 ){
      pTerm->wtFlags |= TERM_LIKECOND;
    }else{
      pTerm->wtFlags |= TERM_CODED;
    }
    if( pTerm->iParent<0 ) break;
    pTerm = &pTerm->pWC->a[pTerm->iParent];
    pTerm->nChild--;
    if( pTerm->nChild!=0 ) break;
    nLoop++;
  }
}

/*
** Code an OP_Affinity opcode to apply the column affinity string zAff
** to the n registers starting at base. 
**
** As an optimization, SQLITE_AFF_NONE entries (which are no-ops) at the
** beginning and end of zAff are ignored.  If all entries in zAff are
** SQLITE_AFF_NONE, then no code gets generated.
**
** This routine makes its own copy of zAff so that the caller is free
** to modify zAff after this routine returns.
*/
static void codeApplyAffinity(Parse *pParse, int base, int n, char *zAff){
  Vdbe *v = pParse->pVdbe;
  if( zAff==0 ){
    assert( pParse->db->mallocFailed );
    return;
  }
  assert( v!=0 );

  /* Adjust base and n to skip over SQLITE_AFF_NONE entries at the beginning
  ** and end of the affinity string.
  */
  while( n>0 && zAff[0]==SQLITE_AFF_NONE ){
    n--;
    base++;
    zAff++;
  }
  while( n>1 && zAff[n-1]==SQLITE_AFF_NONE ){
    n--;
  }

  /* Code the OP_Affinity opcode if there is anything left to do. */
  if( n>0 ){
    sqlite3VdbeAddOp2(v, OP_Affinity, base, n);
    sqlite3VdbeChangeP4(v, -1, zAff, n);
    sqlite3ExprCacheAffinityChange(pParse, base, n);
  }
}


/*
** Generate code for a single equality term of the WHERE clause.  An equality
** term can be either X=expr or X IN (...).   pTerm is the term to be 
** coded.
**
** The current value for the constraint is left in register iReg.
**
** For a constraint of the form X=expr, the expression is evaluated and its
** result is left on the stack.  For constraints of the form X IN (...)
** this routine sets up a loop that will iterate over all values of X.
*/
static int codeEqualityTerm(
  Parse *pParse,      /* The parsing context */
  WhereTerm *pTerm,   /* The term of the WHERE clause to be coded */
  WhereLevel *pLevel, /* The level of the FROM clause we are working on */
  int iEq,            /* Index of the equality term within this level */
  int bRev,           /* True for reverse-order IN operations */
  int iTarget         /* Attempt to leave results in this register */
){
  Expr *pX = pTerm->pExpr;
  Vdbe *v = pParse->pVdbe;
  int iReg;                  /* Register holding results */

  assert( iTarget>0 );
  if( pX->op==TK_EQ ){
    iReg = sqlite3ExprCodeTarget(pParse, pX->pRight, iTarget);
  }else if( pX->op==TK_ISNULL ){
    iReg = iTarget;
    sqlite3VdbeAddOp2(v, OP_Null, 0, iReg);
#ifndef SQLITE_OMIT_SUBQUERY
  }else{
    int eType;
    int iTab;
    struct InLoop *pIn;
    WhereLoop *pLoop = pLevel->pWLoop;

    if( (pLoop->wsFlags & WHERE_VIRTUALTABLE)==0
      && pLoop->u.btree.pIndex!=0
      && pLoop->u.btree.pIndex->aSortOrder[iEq]
    ){
      testcase( iEq==0 );
      testcase( bRev );
      bRev = !bRev;
    }
    assert( pX->op==TK_IN );
    iReg = iTarget;
    eType = sqlite3FindInIndex(pParse, pX, IN_INDEX_LOOP, 0);
    if( eType==IN_INDEX_INDEX_DESC ){
      testcase( bRev );
      bRev = !bRev;
    }
    iTab = pX->iTable;
    sqlite3VdbeAddOp2(v, bRev ? OP_Last : OP_Rewind, iTab, 0);
    VdbeCoverageIf(v, bRev);
    VdbeCoverageIf(v, !bRev);
    assert( (pLoop->wsFlags & WHERE_MULTI_OR)==0 );
    pLoop->wsFlags |= WHERE_IN_ABLE;
    if( pLevel->u.in.nIn==0 ){
      pLevel->addrNxt = sqlite3VdbeMakeLabel(v);
    }
    pLevel->u.in.nIn++;
    pLevel->u.in.aInLoop =
       sqlite3DbReallocOrFree(pParse->db, pLevel->u.in.aInLoop,
                              sizeof(pLevel->u.in.aInLoop[0])*pLevel->u.in.nIn);
    pIn = pLevel->u.in.aInLoop;
    if( pIn ){
      pIn += pLevel->u.in.nIn - 1;
      pIn->iCur = iTab;
      if( eType==IN_INDEX_ROWID ){
        pIn->addrInTop = sqlite3VdbeAddOp2(v, OP_Rowid, iTab, iReg);
      }else{
        pIn->addrInTop = sqlite3VdbeAddOp3(v, OP_Column, iTab, 0, iReg);
      }
      pIn->eEndLoopOp = bRev ? OP_PrevIfOpen : OP_NextIfOpen;
      sqlite3VdbeAddOp1(v, OP_IsNull, iReg); VdbeCoverage(v);
    }else{
      pLevel->u.in.nIn = 0;
    }
#endif
  }
  disableTerm(pLevel, pTerm);
  return iReg;
}

/*
** Generate code that will evaluate all == and IN constraints for an
** index scan.
**
** For example, consider table t1(a,b,c,d,e,f) with index i1(a,b,c).
** Suppose the WHERE clause is this:  a==5 AND b IN (1,2,3) AND c>5 AND c<10
** The index has as many as three equality constraints, but in this
** example, the third "c" value is an inequality.  So only two 
** constraints are coded.  This routine will generate code to evaluate
** a==5 and b IN (1,2,3).  The current values for a and b will be stored
** in consecutive registers and the index of the first register is returned.
**
** In the example above nEq==2.  But this subroutine works for any value
** of nEq including 0.  If nEq==0, this routine is nearly a no-op.
** The only thing it does is allocate the pLevel->iMem memory cell and
** compute the affinity string.
**
** The nExtraReg parameter is 0 or 1.  It is 0 if all WHERE clause constraints
** are == or IN and are covered by the nEq.  nExtraReg is 1 if there is
** an inequality constraint (such as the "c>=5 AND c<10" in the example) that
** occurs after the nEq quality constraints.
**
** This routine allocates a range of nEq+nExtraReg memory cells and returns
** the index of the first memory cell in that range. The code that
** calls this routine will use that memory range to store keys for
** start and termination conditions of the loop.
** key value of the loop.  If one or more IN operators appear, then
** this routine allocates an additional nEq memory cells for internal
** use.
**
** Before returning, *pzAff is set to point to a buffer containing a
** copy of the column affinity string of the index allocated using
** sqlite3DbMalloc(). Except, entries in the copy of the string associated
** with equality constraints that use NONE affinity are set to
** SQLITE_AFF_NONE. This is to deal with SQL such as the following:
**
**   CREATE TABLE t1(a TEXT PRIMARY KEY, b);
**   SELECT ... FROM t1 AS t2, t1 WHERE t1.a = t2.b;
**
** In the example above, the index on t1(a) has TEXT affinity. But since
** the right hand side of the equality constraint (t2.b) has NONE affinity,
** no conversion should be attempted before using a t2.b value as part of
** a key to search the index. Hence the first byte in the returned affinity
** string in this example would be set to SQLITE_AFF_NONE.
*/
static int codeAllEqualityTerms(
  Parse *pParse,        /* Parsing context */
  WhereLevel *pLevel,   /* Which nested loop of the FROM we are coding */
  int bRev,             /* Reverse the order of IN operators */
  int nExtraReg,        /* Number of extra registers to allocate */
  char **pzAff          /* OUT: Set to point to affinity string */
){
  u16 nEq;                      /* The number of == or IN constraints to code */
  u16 nSkip;                    /* Number of left-most columns to skip */
  Vdbe *v = pParse->pVdbe;      /* The vm under construction */
  Index *pIdx;                  /* The index being used for this loop */
  WhereTerm *pTerm;             /* A single constraint term */
  WhereLoop *pLoop;             /* The WhereLoop object */
  int j;                        /* Loop counter */
  int regBase;                  /* Base register */
  int nReg;                     /* Number of registers to allocate */
  char *zAff;                   /* Affinity string to return */

  /* This module is only called on query plans that use an index. */
  pLoop = pLevel->pWLoop;
  assert( (pLoop->wsFlags & WHERE_VIRTUALTABLE)==0 );
  nEq = pLoop->u.btree.nEq;
  nSkip = pLoop->nSkip;
  pIdx = pLoop->u.btree.pIndex;
  assert( pIdx!=0 );

  /* Figure out how many memory cells we will need then allocate them.
  */
  regBase = pParse->nMem + 1;
  nReg = pLoop->u.btree.nEq + nExtraReg;
  pParse->nMem += nReg;

  zAff = sqlite3DbStrDup(pParse->db, sqlite3IndexAffinityStr(v, pIdx));
  if( !zAff ){
    pParse->db->mallocFailed = 1;
  }

  if( nSkip ){
    int iIdxCur = pLevel->iIdxCur;
    sqlite3VdbeAddOp1(v, (bRev?OP_Last:OP_Rewind), iIdxCur);
    VdbeCoverageIf(v, bRev==0);
    VdbeCoverageIf(v, bRev!=0);
    VdbeComment((v, "begin skip-scan on %s", pIdx->zName));
    j = sqlite3VdbeAddOp0(v, OP_Goto);
    pLevel->addrSkip = sqlite3VdbeAddOp4Int(v, (bRev?OP_SeekLT:OP_SeekGT),
                            iIdxCur, 0, regBase, nSkip);
    VdbeCoverageIf(v, bRev==0);
    VdbeCoverageIf(v, bRev!=0);
    sqlite3VdbeJumpHere(v, j);
    for(j=0; j<nSkip; j++){
      sqlite3VdbeAddOp3(v, OP_Column, iIdxCur, j, regBase+j);
      assert( pIdx->aiColumn[j]>=0 );
      VdbeComment((v, "%s", pIdx->pTable->aCol[pIdx->aiColumn[j]].zName));
    }
  }    

  /* Evaluate the equality constraints
  */
  assert( zAff==0 || (int)strlen(zAff)>=nEq );
  for(j=nSkip; j<nEq; j++){
    int r1;
    pTerm = pLoop->aLTerm[j];
    assert( pTerm!=0 );
    /* The following testcase is true for indices with redundant columns. 
    ** Ex: CREATE INDEX i1 ON t1(a,b,a); SELECT * FROM t1 WHERE a=0 AND b=0; */
    testcase( (pTerm->wtFlags & TERM_CODED)!=0 );
    testcase( pTerm->wtFlags & TERM_VIRTUAL );
    r1 = codeEqualityTerm(pParse, pTerm, pLevel, j, bRev, regBase+j);
    if( r1!=regBase+j ){
      if( nReg==1 ){
        sqlite3ReleaseTempReg(pParse, regBase);
        regBase = r1;
      }else{
        sqlite3VdbeAddOp2(v, OP_SCopy, r1, regBase+j);
      }
    }
    testcase( pTerm->eOperator & WO_ISNULL );
    testcase( pTerm->eOperator & WO_IN );
    if( (pTerm->eOperator & (WO_ISNULL|WO_IN))==0 ){
      Expr *pRight = pTerm->pExpr->pRight;
      if( sqlite3ExprCanBeNull(pRight) ){
        sqlite3VdbeAddOp2(v, OP_IsNull, regBase+j, pLevel->addrBrk);
        VdbeCoverage(v);
      }
      if( zAff ){
        if( sqlite3CompareAffinity(pRight, zAff[j])==SQLITE_AFF_NONE ){
          zAff[j] = SQLITE_AFF_NONE;
        }
        if( sqlite3ExprNeedsNoAffinityChange(pRight, zAff[j]) ){
          zAff[j] = SQLITE_AFF_NONE;
        }
      }
    }
  }
  *pzAff = zAff;
  return regBase;
}

#ifndef SQLITE_OMIT_EXPLAIN
/*
** This routine is a helper for explainIndexRange() below
**
** pStr holds the text of an expression that we are building up one term
** at a time.  This routine adds a new term to the end of the expression.
** Terms are separated by AND so add the "AND" text for second and subsequent
** terms only.
*/
static void explainAppendTerm(
  StrAccum *pStr,             /* The text expression being built */
  int iTerm,                  /* Index of this term.  First is zero */
  const char *zColumn,        /* Name of the column */
  const char *zOp             /* Name of the operator */
){
  if( iTerm ) sqlite3StrAccumAppend(pStr, " AND ", 5);
  sqlite3StrAccumAppendAll(pStr, zColumn);
  sqlite3StrAccumAppend(pStr, zOp, 1);
  sqlite3StrAccumAppend(pStr, "?", 1);
}

/*
** Argument pLevel describes a strategy for scanning table pTab. This 
** function appends text to pStr that describes the subset of table
** rows scanned by the strategy in the form of an SQL expression.
**
** For example, if the query:
**
**   SELECT * FROM t1 WHERE a=1 AND b>2;
**
** is run and there is an index on (a, b), then this function returns a
** string similar to:
**
**   "a=? AND b>?"
*/
static void explainIndexRange(StrAccum *pStr, WhereLoop *pLoop, Table *pTab){
  Index *pIndex = pLoop->u.btree.pIndex;
  u16 nEq = pLoop->u.btree.nEq;
  u16 nSkip = pLoop->nSkip;
  int i, j;
  Column *aCol = pTab->aCol;
  i16 *aiColumn = pIndex->aiColumn;

  if( nEq==0 && (pLoop->wsFlags&(WHERE_BTM_LIMIT|WHERE_TOP_LIMIT))==0 ) return;
  sqlite3StrAccumAppend(pStr, " (", 2);
  for(i=0; i<nEq; i++){
    char *z = aiColumn[i] < 0 ? "rowid" : aCol[aiColumn[i]].zName;
    if( i>=nSkip ){
      explainAppendTerm(pStr, i, z, "=");
    }else{
      if( i ) sqlite3StrAccumAppend(pStr, " AND ", 5);
      sqlite3XPrintf(pStr, 0, "ANY(%s)", z);
    }
  }

  j = i;
  if( pLoop->wsFlags&WHERE_BTM_LIMIT ){
    char *z = aiColumn[j] < 0 ? "rowid" : aCol[aiColumn[j]].zName;
    explainAppendTerm(pStr, i++, z, ">");
  }
  if( pLoop->wsFlags&WHERE_TOP_LIMIT ){
    char *z = aiColumn[j] < 0 ? "rowid" : aCol[aiColumn[j]].zName;
    explainAppendTerm(pStr, i, z, "<");
  }
  sqlite3StrAccumAppend(pStr, ")", 1);
}

/*
** This function is a no-op unless currently processing an EXPLAIN QUERY PLAN
** command, or if either SQLITE_DEBUG or SQLITE_ENABLE_STMT_SCANSTATUS was
** defined at compile-time. If it is not a no-op, a single OP_Explain opcode 
** is added to the output to describe the table scan strategy in pLevel.
**
** If an OP_Explain opcode is added to the VM, its address is returned.
** Otherwise, if no OP_Explain is coded, zero is returned.
*/
static int explainOneScan(
  Parse *pParse,                  /* Parse context */
  SrcList *pTabList,              /* Table list this loop refers to */
  WhereLevel *pLevel,             /* Scan to write OP_Explain opcode for */
  int iLevel,                     /* Value for "level" column of output */
  int iFrom,                      /* Value for "from" column of output */
  u16 wctrlFlags                  /* Flags passed to sqlite3WhereBegin() */
){
  int ret = 0;
#if !defined(SQLITE_DEBUG) && !defined(SQLITE_ENABLE_STMT_SCANSTATUS)
  if( pParse->explain==2 )
#endif
  {
    struct SrcList_item *pItem = &pTabList->a[pLevel->iFrom];
    Vdbe *v = pParse->pVdbe;      /* VM being constructed */
    sqlite3 *db = pParse->db;     /* Database handle */
    int iId = pParse->iSelectId;  /* Select id (left-most output column) */
    int isSearch;                 /* True for a SEARCH. False for SCAN. */
    WhereLoop *pLoop;             /* The controlling WhereLoop object */
    u32 flags;                    /* Flags that describe this loop */
    char *zMsg;                   /* Text to add to EQP output */
    StrAccum str;                 /* EQP output string */
    char zBuf[100];               /* Initial space for EQP output string */

    pLoop = pLevel->pWLoop;
    flags = pLoop->wsFlags;
    if( (flags&WHERE_MULTI_OR) || (wctrlFlags&WHERE_ONETABLE_ONLY) ) return 0;

    isSearch = (flags&(WHERE_BTM_LIMIT|WHERE_TOP_LIMIT))!=0
            || ((flags&WHERE_VIRTUALTABLE)==0 && (pLoop->u.btree.nEq>0))
            || (wctrlFlags&(WHERE_ORDERBY_MIN|WHERE_ORDERBY_MAX));

    sqlite3StrAccumInit(&str, zBuf, sizeof(zBuf), SQLITE_MAX_LENGTH);
    str.db = db;
    sqlite3StrAccumAppendAll(&str, isSearch ? "SEARCH" : "SCAN");
    if( pItem->pSelect ){
      sqlite3XPrintf(&str, 0, " SUBQUERY %d", pItem->iSelectId);
    }else{
      sqlite3XPrintf(&str, 0, " TABLE %s", pItem->zName);
    }

    if( pItem->zAlias ){
      sqlite3XPrintf(&str, 0, " AS %s", pItem->zAlias);
    }
    if( (flags & (WHERE_IPK|WHERE_VIRTUALTABLE))==0 ){
      const char *zFmt = 0;
      Index *pIdx;

      assert( pLoop->u.btree.pIndex!=0 );
      pIdx = pLoop->u.btree.pIndex;
      assert( !(flags&WHERE_AUTO_INDEX) || (flags&WHERE_IDX_ONLY) );
      if( !HasRowid(pItem->pTab) && IsPrimaryKeyIndex(pIdx) ){
        if( isSearch ){
          zFmt = "PRIMARY KEY";
        }
      }else if( flags & WHERE_PARTIALIDX ){
        zFmt = "AUTOMATIC PARTIAL COVERING INDEX";
      }else if( flags & WHERE_AUTO_INDEX ){
        zFmt = "AUTOMATIC COVERING INDEX";
      }else if( flags & WHERE_IDX_ONLY ){
        zFmt = "COVERING INDEX %s";
      }else{
        zFmt = "INDEX %s";
      }
      if( zFmt ){
        sqlite3StrAccumAppend(&str, " USING ", 7);
        sqlite3XPrintf(&str, 0, zFmt, pIdx->zName);
        explainIndexRange(&str, pLoop, pItem->pTab);
      }
    }else if( (flags & WHERE_IPK)!=0 && (flags & WHERE_CONSTRAINT)!=0 ){
      const char *zRange;
      if( flags&(WHERE_COLUMN_EQ|WHERE_COLUMN_IN) ){
        zRange = "(rowid=?)";
      }else if( (flags&WHERE_BOTH_LIMIT)==WHERE_BOTH_LIMIT ){
        zRange = "(rowid>? AND rowid<?)";
      }else if( flags&WHERE_BTM_LIMIT ){
        zRange = "(rowid>?)";
      }else{
        assert( flags&WHERE_TOP_LIMIT);
        zRange = "(rowid<?)";
      }
      sqlite3StrAccumAppendAll(&str, " USING INTEGER PRIMARY KEY ");
      sqlite3StrAccumAppendAll(&str, zRange);
    }
#ifndef SQLITE_OMIT_VIRTUALTABLE
    else if( (flags & WHERE_VIRTUALTABLE)!=0 ){
      sqlite3XPrintf(&str, 0, " VIRTUAL TABLE INDEX %d:%s",
                  pLoop->u.vtab.idxNum, pLoop->u.vtab.idxStr);
    }
#endif
#ifdef SQLITE_EXPLAIN_ESTIMATED_ROWS
    if( pLoop->nOut>=10 ){
      sqlite3XPrintf(&str, 0, " (~%llu rows)", sqlite3LogEstToInt(pLoop->nOut));
    }else{
      sqlite3StrAccumAppend(&str, " (~1 row)", 9);
    }
#endif
    zMsg = sqlite3StrAccumFinish(&str);
    ret = sqlite3VdbeAddOp4(v, OP_Explain, iId, iLevel, iFrom, zMsg,P4_DYNAMIC);
  }
  return ret;
}
#else
# define explainOneScan(u,v,w,x,y,z) 0
#endif /* SQLITE_OMIT_EXPLAIN */

#ifdef SQLITE_ENABLE_STMT_SCANSTATUS
/*
** Configure the VM passed as the first argument with an
** sqlite3_stmt_scanstatus() entry corresponding to the scan used to 
** implement level pLvl. Argument pSrclist is a pointer to the FROM 
** clause that the scan reads data from.
**
** If argument addrExplain is not 0, it must be the address of an 
** OP_Explain instruction that describes the same loop.
*/
static void addScanStatus(
  Vdbe *v,                        /* Vdbe to add scanstatus entry to */
  SrcList *pSrclist,              /* FROM clause pLvl reads data from */
  WhereLevel *pLvl,               /* Level to add scanstatus() entry for */
  int addrExplain                 /* Address of OP_Explain (or 0) */
){
  const char *zObj = 0;
  WhereLoop *pLoop = pLvl->pWLoop;
  if( (pLoop->wsFlags & WHERE_VIRTUALTABLE)==0  &&  pLoop->u.btree.pIndex!=0 ){
    zObj = pLoop->u.btree.pIndex->zName;
  }else{
    zObj = pSrclist->a[pLvl->iFrom].zName;
  }
  sqlite3VdbeScanStatus(
      v, addrExplain, pLvl->addrBody, pLvl->addrVisit, pLoop->nOut, zObj
  );
}
#else
# define addScanStatus(a, b, c, d) ((void)d)
#endif

/*
** If the most recently coded instruction is a constant range contraint
** that originated from the LIKE optimization, then change the P3 to be
** pLoop->iLikeRepCntr and set P5.
**
** The LIKE optimization trys to evaluate "x LIKE 'abc%'" as a range
** expression: "x>='ABC' AND x<'abd'".  But this requires that the range
** scan loop run twice, once for strings and a second time for BLOBs.
** The OP_String opcodes on the second pass convert the upper and lower
** bound string contants to blobs.  This routine makes the necessary changes
** to the OP_String opcodes for that to happen.
*/
static void whereLikeOptimizationStringFixup(
  Vdbe *v,                /* prepared statement under construction */
  WhereLevel *pLevel,     /* The loop that contains the LIKE operator */
  WhereTerm *pTerm        /* The upper or lower bound just coded */
){
  if( pTerm->wtFlags & TERM_LIKEOPT ){
    VdbeOp *pOp;
    assert( pLevel->iLikeRepCntr>0 );
    pOp = sqlite3VdbeGetOp(v, -1);
    assert( pOp!=0 );
    assert( pOp->opcode==OP_String8 
            || pTerm->pWC->pWInfo->pParse->db->mallocFailed );
    pOp->p3 = pLevel->iLikeRepCntr;
    pOp->p5 = 1;
  }
}

/*
** Generate code for the start of the iLevel-th loop in the WHERE clause
** implementation described by pWInfo.
*/
static Bitmask codeOneLoopStart(
  WhereInfo *pWInfo,   /* Complete information about the WHERE clause */
  int iLevel,          /* Which level of pWInfo->a[] should be coded */
  Bitmask notReady     /* Which tables are currently available */
){
  int j, k;            /* Loop counters */
  int iCur;            /* The VDBE cursor for the table */
  int addrNxt;         /* Where to jump to continue with the next IN case */
  int omitTable;       /* True if we use the index only */
  int bRev;            /* True if we need to scan in reverse order */
  WhereLevel *pLevel;  /* The where level to be coded */
  WhereLoop *pLoop;    /* The WhereLoop object being coded */
  WhereClause *pWC;    /* Decomposition of the entire WHERE clause */
  WhereTerm *pTerm;               /* A WHERE clause term */
  Parse *pParse;                  /* Parsing context */
  sqlite3 *db;                    /* Database connection */
  Vdbe *v;                        /* The prepared stmt under constructions */
  struct SrcList_item *pTabItem;  /* FROM clause term being coded */
  int addrBrk;                    /* Jump here to break out of the loop */
  int addrCont;                   /* Jump here to continue with next cycle */
  int iRowidReg = 0;        /* Rowid is stored in this register, if not zero */
  int iReleaseReg = 0;      /* Temp register to free before returning */

  pParse = pWInfo->pParse;
  v = pParse->pVdbe;
  pWC = &pWInfo->sWC;
  db = pParse->db;
  pLevel = &pWInfo->a[iLevel];
  pLoop = pLevel->pWLoop;
  pTabItem = &pWInfo->pTabList->a[pLevel->iFrom];
  iCur = pTabItem->iCursor;
  pLevel->notReady = notReady & ~getMask(&pWInfo->sMaskSet, iCur);
  bRev = (pWInfo->revMask>>iLevel)&1;
  omitTable = (pLoop->wsFlags & WHERE_IDX_ONLY)!=0 
           && (pWInfo->wctrlFlags & WHERE_FORCE_TABLE)==0;
  VdbeModuleComment((v, "Begin WHERE-loop%d: %s",iLevel,pTabItem->pTab->zName));

  /* Create labels for the "break" and "continue" instructions
  ** for the current loop.  Jump to addrBrk to break out of a loop.
  ** Jump to cont to go immediately to the next iteration of the
  ** loop.
  **
  ** When there is an IN operator, we also have a "addrNxt" label that
  ** means to continue with the next IN value combination.  When
  ** there are no IN operators in the constraints, the "addrNxt" label
  ** is the same as "addrBrk".
  */
  addrBrk = pLevel->addrBrk = pLevel->addrNxt = sqlite3VdbeMakeLabel(v);
  addrCont = pLevel->addrCont = sqlite3VdbeMakeLabel(v);

  /* If this is the right table of a LEFT OUTER JOIN, allocate and
  ** initialize a memory cell that records if this table matches any
  ** row of the left table of the join.
  */
  if( pLevel->iFrom>0 && (pTabItem[0].jointype & JT_LEFT)!=0 ){
    pLevel->iLeftJoin = ++pParse->nMem;
    sqlite3VdbeAddOp2(v, OP_Integer, 0, pLevel->iLeftJoin);
    VdbeComment((v, "init LEFT JOIN no-match flag"));
  }

  /* Special case of a FROM clause subquery implemented as a co-routine */
  if( pTabItem->viaCoroutine ){
    int regYield = pTabItem->regReturn;
    sqlite3VdbeAddOp3(v, OP_InitCoroutine, regYield, 0, pTabItem->addrFillSub);
    pLevel->p2 =  sqlite3VdbeAddOp2(v, OP_Yield, regYield, addrBrk);
    VdbeCoverage(v);
    VdbeComment((v, "next row of \"%s\"", pTabItem->pTab->zName));
    pLevel->op = OP_Goto;
  }else

#ifndef SQLITE_OMIT_VIRTUALTABLE
  if(  (pLoop->wsFlags & WHERE_VIRTUALTABLE)!=0 ){
    /* Case 1:  The table is a virtual-table.  Use the VFilter and VNext
    **          to access the data.
    */
    int iReg;   /* P3 Value for OP_VFilter */
    int addrNotFound;
    int nConstraint = pLoop->nLTerm;

    sqlite3ExprCachePush(pParse);
    iReg = sqlite3GetTempRange(pParse, nConstraint+2);
    addrNotFound = pLevel->addrBrk;
    for(j=0; j<nConstraint; j++){
      int iTarget = iReg+j+2;
      pTerm = pLoop->aLTerm[j];
      if( pTerm==0 ) continue;
      if( pTerm->eOperator & WO_IN ){
        codeEqualityTerm(pParse, pTerm, pLevel, j, bRev, iTarget);
        addrNotFound = pLevel->addrNxt;
      }else{
        sqlite3ExprCode(pParse, pTerm->pExpr->pRight, iTarget);
      }
    }
    sqlite3VdbeAddOp2(v, OP_Integer, pLoop->u.vtab.idxNum, iReg);
    sqlite3VdbeAddOp2(v, OP_Integer, nConstraint, iReg+1);
    sqlite3VdbeAddOp4(v, OP_VFilter, iCur, addrNotFound, iReg,
                      pLoop->u.vtab.idxStr,
                      pLoop->u.vtab.needFree ? P4_MPRINTF : P4_STATIC);
    VdbeCoverage(v);
    pLoop->u.vtab.needFree = 0;
    for(j=0; j<nConstraint && j<16; j++){
      if( (pLoop->u.vtab.omitMask>>j)&1 ){
        disableTerm(pLevel, pLoop->aLTerm[j]);
      }
    }
    pLevel->op = OP_VNext;
    pLevel->p1 = iCur;
    pLevel->p2 = sqlite3VdbeCurrentAddr(v);
    sqlite3ReleaseTempRange(pParse, iReg, nConstraint+2);
    sqlite3ExprCachePop(pParse);
  }else
#endif /* SQLITE_OMIT_VIRTUALTABLE */

  if( (pLoop->wsFlags & WHERE_IPK)!=0
   && (pLoop->wsFlags & (WHERE_COLUMN_IN|WHERE_COLUMN_EQ))!=0
  ){
    /* Case 2:  We can directly reference a single row using an
    **          equality comparison against the ROWID field.  Or
    **          we reference multiple rows using a "rowid IN (...)"
    **          construct.
    */
    assert( pLoop->u.btree.nEq==1 );
    pTerm = pLoop->aLTerm[0];
    assert( pTerm!=0 );
    assert( pTerm->pExpr!=0 );
    assert( omitTable==0 );
    testcase( pTerm->wtFlags & TERM_VIRTUAL );
    iReleaseReg = ++pParse->nMem;
    iRowidReg = codeEqualityTerm(pParse, pTerm, pLevel, 0, bRev, iReleaseReg);
    if( iRowidReg!=iReleaseReg ) sqlite3ReleaseTempReg(pParse, iReleaseReg);
    addrNxt = pLevel->addrNxt;
    sqlite3VdbeAddOp2(v, OP_MustBeInt, iRowidReg, addrNxt); VdbeCoverage(v);
    sqlite3VdbeAddOp3(v, OP_NotExists, iCur, addrNxt, iRowidReg);
    VdbeCoverage(v);
    sqlite3ExprCacheAffinityChange(pParse, iRowidReg, 1);
    sqlite3ExprCacheStore(pParse, iCur, -1, iRowidReg);
    VdbeComment((v, "pk"));
    pLevel->op = OP_Noop;
  }else if( (pLoop->wsFlags & WHERE_IPK)!=0
         && (pLoop->wsFlags & WHERE_COLUMN_RANGE)!=0
  ){
    /* Case 3:  We have an inequality comparison against the ROWID field.
    */
    int testOp = OP_Noop;
    int start;
    int memEndValue = 0;
    WhereTerm *pStart, *pEnd;

    assert( omitTable==0 );
    j = 0;
    pStart = pEnd = 0;
    if( pLoop->wsFlags & WHERE_BTM_LIMIT ) pStart = pLoop->aLTerm[j++];
    if( pLoop->wsFlags & WHERE_TOP_LIMIT ) pEnd = pLoop->aLTerm[j++];
    assert( pStart!=0 || pEnd!=0 );
    if( bRev ){
      pTerm = pStart;
      pStart = pEnd;
      pEnd = pTerm;
    }
    if( pStart ){
      Expr *pX;             /* The expression that defines the start bound */
      int r1, rTemp;        /* Registers for holding the start boundary */

      /* The following constant maps TK_xx codes into corresponding 
      ** seek opcodes.  It depends on a particular ordering of TK_xx
      */
      const u8 aMoveOp[] = {
           /* TK_GT */  OP_SeekGT,
           /* TK_LE */  OP_SeekLE,
           /* TK_LT */  OP_SeekLT,
           /* TK_GE */  OP_SeekGE
      };
      assert( TK_LE==TK_GT+1 );      /* Make sure the ordering.. */
      assert( TK_LT==TK_GT+2 );      /*  ... of the TK_xx values... */
      assert( TK_GE==TK_GT+3 );      /*  ... is correcct. */

      assert( (pStart->wtFlags & TERM_VNULL)==0 );
      testcase( pStart->wtFlags & TERM_VIRTUAL );
      pX = pStart->pExpr;
      assert( pX!=0 );
      testcase( pStart->leftCursor!=iCur ); /* transitive constraints */
      r1 = sqlite3ExprCodeTemp(pParse, pX->pRight, &rTemp);
      sqlite3VdbeAddOp3(v, aMoveOp[pX->op-TK_GT], iCur, addrBrk, r1);
      VdbeComment((v, "pk"));
      VdbeCoverageIf(v, pX->op==TK_GT);
      VdbeCoverageIf(v, pX->op==TK_LE);
      VdbeCoverageIf(v, pX->op==TK_LT);
      VdbeCoverageIf(v, pX->op==TK_GE);
      sqlite3ExprCacheAffinityChange(pParse, r1, 1);
      sqlite3ReleaseTempReg(pParse, rTemp);
      disableTerm(pLevel, pStart);
    }else{
      sqlite3VdbeAddOp2(v, bRev ? OP_Last : OP_Rewind, iCur, addrBrk);
      VdbeCoverageIf(v, bRev==0);
      VdbeCoverageIf(v, bRev!=0);
    }
    if( pEnd ){
      Expr *pX;
      pX = pEnd->pExpr;
      assert( pX!=0 );
      assert( (pEnd->wtFlags & TERM_VNULL)==0 );
      testcase( pEnd->leftCursor!=iCur ); /* Transitive constraints */
      testcase( pEnd->wtFlags & TERM_VIRTUAL );
      memEndValue = ++pParse->nMem;
      sqlite3ExprCode(pParse, pX->pRight, memEndValue);
      if( pX->op==TK_LT || pX->op==TK_GT ){
        testOp = bRev ? OP_Le : OP_Ge;
      }else{
        testOp = bRev ? OP_Lt : OP_Gt;
      }
      disableTerm(pLevel, pEnd);
    }
    start = sqlite3VdbeCurrentAddr(v);
    pLevel->op = bRev ? OP_Prev : OP_Next;
    pLevel->p1 = iCur;
    pLevel->p2 = start;
    assert( pLevel->p5==0 );
    if( testOp!=OP_Noop ){
      iRowidReg = ++pParse->nMem;
      sqlite3VdbeAddOp2(v, OP_Rowid, iCur, iRowidReg);
      sqlite3ExprCacheStore(pParse, iCur, -1, iRowidReg);
      sqlite3VdbeAddOp3(v, testOp, memEndValue, addrBrk, iRowidReg);
      VdbeCoverageIf(v, testOp==OP_Le);
      VdbeCoverageIf(v, testOp==OP_Lt);
      VdbeCoverageIf(v, testOp==OP_Ge);
      VdbeCoverageIf(v, testOp==OP_Gt);
      sqlite3VdbeChangeP5(v, SQLITE_AFF_NUMERIC | SQLITE_JUMPIFNULL);
    }
  }else if( pLoop->wsFlags & WHERE_INDEXED ){
    /* Case 4: A scan using an index.
    **
    **         The WHERE clause may contain zero or more equality 
    **         terms ("==" or "IN" operators) that refer to the N
    **         left-most columns of the index. It may also contain
    **         inequality constraints (>, <, >= or <=) on the indexed
    **         column that immediately follows the N equalities. Only 
    **         the right-most column can be an inequality - the rest must
    **         use the "==" and "IN" operators. For example, if the 
    **         index is on (x,y,z), then the following clauses are all 
    **         optimized:
    **
    **            x=5
    **            x=5 AND y=10
    **            x=5 AND y<10
    **            x=5 AND y>5 AND y<10
    **            x=5 AND y=5 AND z<=10
    **
    **         The z<10 term of the following cannot be used, only
    **         the x=5 term:
    **
    **            x=5 AND z<10
    **
    **         N may be zero if there are inequality constraints.
    **         If there are no inequality constraints, then N is at
    **         least one.
    **
    **         This case is also used when there are no WHERE clause
    **         constraints but an index is selected anyway, in order
    **         to force the output order to conform to an ORDER BY.
    */  
    static const u8 aStartOp[] = {
      0,
      0,
      OP_Rewind,           /* 2: (!start_constraints && startEq &&  !bRev) */
      OP_Last,             /* 3: (!start_constraints && startEq &&   bRev) */
      OP_SeekGT,           /* 4: (start_constraints  && !startEq && !bRev) */
      OP_SeekLT,           /* 5: (start_constraints  && !startEq &&  bRev) */
      OP_SeekGE,           /* 6: (start_constraints  &&  startEq && !bRev) */
      OP_SeekLE            /* 7: (start_constraints  &&  startEq &&  bRev) */
    };
    static const u8 aEndOp[] = {
      OP_IdxGE,            /* 0: (end_constraints && !bRev && !endEq) */
      OP_IdxGT,            /* 1: (end_constraints && !bRev &&  endEq) */
      OP_IdxLE,            /* 2: (end_constraints &&  bRev && !endEq) */
      OP_IdxLT,            /* 3: (end_constraints &&  bRev &&  endEq) */
    };
    u16 nEq = pLoop->u.btree.nEq;     /* Number of == or IN terms */
    int regBase;                 /* Base register holding constraint values */
    WhereTerm *pRangeStart = 0;  /* Inequality constraint at range start */
    WhereTerm *pRangeEnd = 0;    /* Inequality constraint at range end */
    int startEq;                 /* True if range start uses ==, >= or <= */
    int endEq;                   /* True if range end uses ==, >= or <= */
    int start_constraints;       /* Start of range is constrained */
    int nConstraint;             /* Number of constraint terms */
    Index *pIdx;                 /* The index we will be using */
    int iIdxCur;                 /* The VDBE cursor for the index */
    int nExtraReg = 0;           /* Number of extra registers needed */
    int op;                      /* Instruction opcode */
    char *zStartAff;             /* Affinity for start of range constraint */
    char cEndAff = 0;            /* Affinity for end of range constraint */
    u8 bSeekPastNull = 0;        /* True to seek past initial nulls */
    u8 bStopAtNull = 0;          /* Add condition to terminate at NULLs */

    pIdx = pLoop->u.btree.pIndex;
    iIdxCur = pLevel->iIdxCur;
    assert( nEq>=pLoop->nSkip );

    /* If this loop satisfies a sort order (pOrderBy) request that 
    ** was passed to this function to implement a "SELECT min(x) ..." 
    ** query, then the caller will only allow the loop to run for
    ** a single iteration. This means that the first row returned
    ** should not have a NULL value stored in 'x'. If column 'x' is
    ** the first one after the nEq equality constraints in the index,
    ** this requires some special handling.
    */
    assert( pWInfo->pOrderBy==0
         || pWInfo->pOrderBy->nExpr==1
         || (pWInfo->wctrlFlags&WHERE_ORDERBY_MIN)==0 );
    if( (pWInfo->wctrlFlags&WHERE_ORDERBY_MIN)!=0
     && pWInfo->nOBSat>0
     && (pIdx->nKeyCol>nEq)
    ){
      assert( pLoop->nSkip==0 );
      bSeekPastNull = 1;
      nExtraReg = 1;
    }

    /* Find any inequality constraint terms for the start and end 
    ** of the range. 
    */
    j = nEq;
    if( pLoop->wsFlags & WHERE_BTM_LIMIT ){
      pRangeStart = pLoop->aLTerm[j++];
      nExtraReg = 1;
      /* Like optimization range constraints always occur in pairs */
      assert( (pRangeStart->wtFlags & TERM_LIKEOPT)==0 || 
              (pLoop->wsFlags & WHERE_TOP_LIMIT)!=0 );
    }
    if( pLoop->wsFlags & WHERE_TOP_LIMIT ){
      pRangeEnd = pLoop->aLTerm[j++];
      nExtraReg = 1;
      if( (pRangeEnd->wtFlags & TERM_LIKEOPT)!=0 ){
        assert( pRangeStart!=0 );                     /* LIKE opt constraints */
        assert( pRangeStart->wtFlags & TERM_LIKEOPT );   /* occur in pairs */
        pLevel->iLikeRepCntr = ++pParse->nMem;
        testcase( bRev );
        testcase( pIdx->aSortOrder[nEq]==SQLITE_SO_DESC );
        sqlite3VdbeAddOp2(v, OP_Integer,
                          bRev ^ (pIdx->aSortOrder[nEq]==SQLITE_SO_DESC),
                          pLevel->iLikeRepCntr);
        VdbeComment((v, "LIKE loop counter"));
        pLevel->addrLikeRep = sqlite3VdbeCurrentAddr(v);
      }
      if( pRangeStart==0
       && (j = pIdx->aiColumn[nEq])>=0 
       && pIdx->pTable->aCol[j].notNull==0
      ){
        bSeekPastNull = 1;
      }
    }
    assert( pRangeEnd==0 || (pRangeEnd->wtFlags & TERM_VNULL)==0 );

    /* Generate code to evaluate all constraint terms using == or IN
    ** and store the values of those terms in an array of registers
    ** starting at regBase.
    */
    regBase = codeAllEqualityTerms(pParse,pLevel,bRev,nExtraReg,&zStartAff);
    assert( zStartAff==0 || sqlite3Strlen30(zStartAff)>=nEq );
    if( zStartAff ) cEndAff = zStartAff[nEq];
    addrNxt = pLevel->addrNxt;

    /* If we are doing a reverse order scan on an ascending index, or
    ** a forward order scan on a descending index, interchange the 
    ** start and end terms (pRangeStart and pRangeEnd).
    */
    if( (nEq<pIdx->nKeyCol && bRev==(pIdx->aSortOrder[nEq]==SQLITE_SO_ASC))
     || (bRev && pIdx->nKeyCol==nEq)
    ){
      SWAP(WhereTerm *, pRangeEnd, pRangeStart);
      SWAP(u8, bSeekPastNull, bStopAtNull);
    }

    testcase( pRangeStart && (pRangeStart->eOperator & WO_LE)!=0 );
    testcase( pRangeStart && (pRangeStart->eOperator & WO_GE)!=0 );
    testcase( pRangeEnd && (pRangeEnd->eOperator & WO_LE)!=0 );
    testcase( pRangeEnd && (pRangeEnd->eOperator & WO_GE)!=0 );
    startEq = !pRangeStart || pRangeStart->eOperator & (WO_LE|WO_GE);
    endEq =   !pRangeEnd || pRangeEnd->eOperator & (WO_LE|WO_GE);
    start_constraints = pRangeStart || nEq>0;

    /* Seek the index cursor to the start of the range. */
    nConstraint = nEq;
    if( pRangeStart ){
      Expr *pRight = pRangeStart->pExpr->pRight;
      sqlite3ExprCode(pParse, pRight, regBase+nEq);
      whereLikeOptimizationStringFixup(v, pLevel, pRangeStart);
      if( (pRangeStart->wtFlags & TERM_VNULL)==0
       && sqlite3ExprCanBeNull(pRight)
      ){
        sqlite3VdbeAddOp2(v, OP_IsNull, regBase+nEq, addrNxt);
        VdbeCoverage(v);
      }
      if( zStartAff ){
        if( sqlite3CompareAffinity(pRight, zStartAff[nEq])==SQLITE_AFF_NONE){
          /* Since the comparison is to be performed with no conversions
          ** applied to the operands, set the affinity to apply to pRight to 
          ** SQLITE_AFF_NONE.  */
          zStartAff[nEq] = SQLITE_AFF_NONE;
        }
        if( sqlite3ExprNeedsNoAffinityChange(pRight, zStartAff[nEq]) ){
          zStartAff[nEq] = SQLITE_AFF_NONE;
        }
      }  
      nConstraint++;
      testcase( pRangeStart->wtFlags & TERM_VIRTUAL );
    }else if( bSeekPastNull ){
      sqlite3VdbeAddOp2(v, OP_Null, 0, regBase+nEq);
      nConstraint++;
      startEq = 0;
      start_constraints = 1;
    }
    codeApplyAffinity(pParse, regBase, nConstraint - bSeekPastNull, zStartAff);
    op = aStartOp[(start_constraints<<2) + (startEq<<1) + bRev];
    assert( op!=0 );
    sqlite3VdbeAddOp4Int(v, op, iIdxCur, addrNxt, regBase, nConstraint);
    VdbeCoverage(v);
    VdbeCoverageIf(v, op==OP_Rewind);  testcase( op==OP_Rewind );
    VdbeCoverageIf(v, op==OP_Last);    testcase( op==OP_Last );
    VdbeCoverageIf(v, op==OP_SeekGT);  testcase( op==OP_SeekGT );
    VdbeCoverageIf(v, op==OP_SeekGE);  testcase( op==OP_SeekGE );
    VdbeCoverageIf(v, op==OP_SeekLE);  testcase( op==OP_SeekLE );
    VdbeCoverageIf(v, op==OP_SeekLT);  testcase( op==OP_SeekLT );

    /* Load the value for the inequality constraint at the end of the
    ** range (if any).
    */
    nConstraint = nEq;
    if( pRangeEnd ){
      Expr *pRight = pRangeEnd->pExpr->pRight;
      sqlite3ExprCacheRemove(pParse, regBase+nEq, 1);
      sqlite3ExprCode(pParse, pRight, regBase+nEq);
      whereLikeOptimizationStringFixup(v, pLevel, pRangeEnd);
      if( (pRangeEnd->wtFlags & TERM_VNULL)==0
       && sqlite3ExprCanBeNull(pRight)
      ){
        sqlite3VdbeAddOp2(v, OP_IsNull, regBase+nEq, addrNxt);
        VdbeCoverage(v);
      }
      if( sqlite3CompareAffinity(pRight, cEndAff)!=SQLITE_AFF_NONE
       && !sqlite3ExprNeedsNoAffinityChange(pRight, cEndAff)
      ){
        codeApplyAffinity(pParse, regBase+nEq, 1, &cEndAff);
      }
      nConstraint++;
      testcase( pRangeEnd->wtFlags & TERM_VIRTUAL );
    }else if( bStopAtNull ){
      sqlite3VdbeAddOp2(v, OP_Null, 0, regBase+nEq);
      endEq = 0;
      nConstraint++;
    }
    sqlite3DbFree(db, zStartAff);

    /* Top of the loop body */
    pLevel->p2 = sqlite3VdbeCurrentAddr(v);

    /* Check if the index cursor is past the end of the range. */
    if( nConstraint ){
      op = aEndOp[bRev*2 + endEq];
      sqlite3VdbeAddOp4Int(v, op, iIdxCur, addrNxt, regBase, nConstraint);
      testcase( op==OP_IdxGT );  VdbeCoverageIf(v, op==OP_IdxGT );
      testcase( op==OP_IdxGE );  VdbeCoverageIf(v, op==OP_IdxGE );
      testcase( op==OP_IdxLT );  VdbeCoverageIf(v, op==OP_IdxLT );
      testcase( op==OP_IdxLE );  VdbeCoverageIf(v, op==OP_IdxLE );
    }

    /* Seek the table cursor, if required */
    disableTerm(pLevel, pRangeStart);
    disableTerm(pLevel, pRangeEnd);
    if( omitTable ){
      /* pIdx is a covering index.  No need to access the main table. */
    }else if( HasRowid(pIdx->pTable) ){
      iRowidReg = ++pParse->nMem;
      sqlite3VdbeAddOp2(v, OP_IdxRowid, iIdxCur, iRowidReg);
      sqlite3ExprCacheStore(pParse, iCur, -1, iRowidReg);
      sqlite3VdbeAddOp2(v, OP_Seek, iCur, iRowidReg);  /* Deferred seek */
    }else if( iCur!=iIdxCur ){
      Index *pPk = sqlite3PrimaryKeyIndex(pIdx->pTable);
      iRowidReg = sqlite3GetTempRange(pParse, pPk->nKeyCol);
      for(j=0; j<pPk->nKeyCol; j++){
        k = sqlite3ColumnOfIndex(pIdx, pPk->aiColumn[j]);
        sqlite3VdbeAddOp3(v, OP_Column, iIdxCur, k, iRowidReg+j);
      }
      sqlite3VdbeAddOp4Int(v, OP_NotFound, iCur, addrCont,
                           iRowidReg, pPk->nKeyCol); VdbeCoverage(v);
    }

    /* Record the instruction used to terminate the loop. Disable 
    ** WHERE clause terms made redundant by the index range scan.
    */
    if( pLoop->wsFlags & WHERE_ONEROW ){
      pLevel->op = OP_Noop;
    }else if( bRev ){
      pLevel->op = OP_Prev;
    }else{
      pLevel->op = OP_Next;
    }
    pLevel->p1 = iIdxCur;
    pLevel->p3 = (pLoop->wsFlags&WHERE_UNQ_WANTED)!=0 ? 1:0;
    if( (pLoop->wsFlags & WHERE_CONSTRAINT)==0 ){
      pLevel->p5 = SQLITE_STMTSTATUS_FULLSCAN_STEP;
    }else{
      assert( pLevel->p5==0 );
    }
  }else

#ifndef SQLITE_OMIT_OR_OPTIMIZATION
  if( pLoop->wsFlags & WHERE_MULTI_OR ){
    /* Case 5:  Two or more separately indexed terms connected by OR
    **
    ** Example:
    **
    **   CREATE TABLE t1(a,b,c,d);
    **   CREATE INDEX i1 ON t1(a);
    **   CREATE INDEX i2 ON t1(b);
    **   CREATE INDEX i3 ON t1(c);
    **
    **   SELECT * FROM t1 WHERE a=5 OR b=7 OR (c=11 AND d=13)
    **
    ** In the example, there are three indexed terms connected by OR.
    ** The top of the loop looks like this:
    **
    **          Null       1                # Zero the rowset in reg 1
    **
    ** Then, for each indexed term, the following. The arguments to
    ** RowSetTest are such that the rowid of the current row is inserted
    ** into the RowSet. If it is already present, control skips the
    ** Gosub opcode and jumps straight to the code generated by WhereEnd().
    **
    **        sqlite3WhereBegin(<term>)
    **          RowSetTest                  # Insert rowid into rowset
    **          Gosub      2 A
    **        sqlite3WhereEnd()
    **
    ** Following the above, code to terminate the loop. Label A, the target
    ** of the Gosub above, jumps to the instruction right after the Goto.
    **
    **          Null       1                # Zero the rowset in reg 1
    **          Goto       B                # The loop is finished.
    **
    **       A: <loop body>                 # Return data, whatever.
    **
    **          Return     2                # Jump back to the Gosub
    **
    **       B: <after the loop>
    **
    ** Added 2014-05-26: If the table is a WITHOUT ROWID table, then
    ** use an ephemeral index instead of a RowSet to record the primary
    ** keys of the rows we have already seen.
    **
    */
    WhereClause *pOrWc;    /* The OR-clause broken out into subterms */
    SrcList *pOrTab;       /* Shortened table list or OR-clause generation */
    Index *pCov = 0;             /* Potential covering index (or NULL) */
    int iCovCur = pParse->nTab++;  /* Cursor used for index scans (if any) */

    int regReturn = ++pParse->nMem;           /* Register used with OP_Gosub */
    int regRowset = 0;                        /* Register for RowSet object */
    int regRowid = 0;                         /* Register holding rowid */
    int iLoopBody = sqlite3VdbeMakeLabel(v);  /* Start of loop body */
    int iRetInit;                             /* Address of regReturn init */
    int untestedTerms = 0;             /* Some terms not completely tested */
    int ii;                            /* Loop counter */
    u16 wctrlFlags;                    /* Flags for sub-WHERE clause */
    Expr *pAndExpr = 0;                /* An ".. AND (...)" expression */
    Table *pTab = pTabItem->pTab;
   
    pTerm = pLoop->aLTerm[0];
    assert( pTerm!=0 );
    assert( pTerm->eOperator & WO_OR );
    assert( (pTerm->wtFlags & TERM_ORINFO)!=0 );
    pOrWc = &pTerm->u.pOrInfo->wc;
    pLevel->op = OP_Return;
    pLevel->p1 = regReturn;

    /* Set up a new SrcList in pOrTab containing the table being scanned
    ** by this loop in the a[0] slot and all notReady tables in a[1..] slots.
    ** This becomes the SrcList in the recursive call to sqlite3WhereBegin().
    */
    if( pWInfo->nLevel>1 ){
      int nNotReady;                 /* The number of notReady tables */
      struct SrcList_item *origSrc;     /* Original list of tables */
      nNotReady = pWInfo->nLevel - iLevel - 1;
      pOrTab = sqlite3StackAllocRaw(db,
                            sizeof(*pOrTab)+ nNotReady*sizeof(pOrTab->a[0]));
      if( pOrTab==0 ) return notReady;
      pOrTab->nAlloc = (u8)(nNotReady + 1);
      pOrTab->nSrc = pOrTab->nAlloc;
      memcpy(pOrTab->a, pTabItem, sizeof(*pTabItem));
      origSrc = pWInfo->pTabList->a;
      for(k=1; k<=nNotReady; k++){
        memcpy(&pOrTab->a[k], &origSrc[pLevel[k].iFrom], sizeof(pOrTab->a[k]));
      }
    }else{
      pOrTab = pWInfo->pTabList;
    }

    /* Initialize the rowset register to contain NULL. An SQL NULL is 
    ** equivalent to an empty rowset.  Or, create an ephemeral index
    ** capable of holding primary keys in the case of a WITHOUT ROWID.
    **
    ** Also initialize regReturn to contain the address of the instruction 
    ** immediately following the OP_Return at the bottom of the loop. This
    ** is required in a few obscure LEFT JOIN cases where control jumps
    ** over the top of the loop into the body of it. In this case the 
    ** correct response for the end-of-loop code (the OP_Return) is to 
    ** fall through to the next instruction, just as an OP_Next does if
    ** called on an uninitialized cursor.
    */
    if( (pWInfo->wctrlFlags & WHERE_DUPLICATES_OK)==0 ){
      if( HasRowid(pTab) ){
        regRowset = ++pParse->nMem;
        sqlite3VdbeAddOp2(v, OP_Null, 0, regRowset);
      }else{
        Index *pPk = sqlite3PrimaryKeyIndex(pTab);
        regRowset = pParse->nTab++;
        sqlite3VdbeAddOp2(v, OP_OpenEphemeral, regRowset, pPk->nKeyCol);
        sqlite3VdbeSetP4KeyInfo(pParse, pPk);
      }
      regRowid = ++pParse->nMem;
    }
    iRetInit = sqlite3VdbeAddOp2(v, OP_Integer, 0, regReturn);

    /* If the original WHERE clause is z of the form:  (x1 OR x2 OR ...) AND y
    ** Then for every term xN, evaluate as the subexpression: xN AND z
    ** That way, terms in y that are factored into the disjunction will
    ** be picked up by the recursive calls to sqlite3WhereBegin() below.
    **
    ** Actually, each subexpression is converted to "xN AND w" where w is
    ** the "interesting" terms of z - terms that did not originate in the
    ** ON or USING clause of a LEFT JOIN, and terms that are usable as 
    ** indices.
    **
    ** This optimization also only applies if the (x1 OR x2 OR ...) term
    ** is not contained in the ON clause of a LEFT JOIN.
    ** See ticket http://www.sqlite.org/src/info/f2369304e4
    */
    if( pWC->nTerm>1 ){
      int iTerm;
      for(iTerm=0; iTerm<pWC->nTerm; iTerm++){
        Expr *pExpr = pWC->a[iTerm].pExpr;
        if( &pWC->a[iTerm] == pTerm ) continue;
        if( ExprHasProperty(pExpr, EP_FromJoin) ) continue;
        if( (pWC->a[iTerm].wtFlags & TERM_VIRTUAL)!=0 ) continue;
        if( (pWC->a[iTerm].eOperator & WO_ALL)==0 ) continue;
        testcase( pWC->a[iTerm].wtFlags & TERM_ORINFO );
        pExpr = sqlite3ExprDup(db, pExpr, 0);
        pAndExpr = sqlite3ExprAnd(db, pAndExpr, pExpr);
      }
      if( pAndExpr ){
        pAndExpr = sqlite3PExpr(pParse, TK_AND, 0, pAndExpr, 0);
      }
    }

    /* Run a separate WHERE clause for each term of the OR clause.  After
    ** eliminating duplicates from other WHERE clauses, the action for each
    ** sub-WHERE clause is to to invoke the main loop body as a subroutine.
    */
    wctrlFlags =  WHERE_OMIT_OPEN_CLOSE
                | WHERE_FORCE_TABLE
                | WHERE_ONETABLE_ONLY
                | WHERE_NO_AUTOINDEX;
    for(ii=0; ii<pOrWc->nTerm; ii++){
      WhereTerm *pOrTerm = &pOrWc->a[ii];
      if( pOrTerm->leftCursor==iCur || (pOrTerm->eOperator & WO_AND)!=0 ){
        WhereInfo *pSubWInfo;           /* Info for single OR-term scan */
        Expr *pOrExpr = pOrTerm->pExpr; /* Current OR clause term */
        int j1 = 0;                     /* Address of jump operation */
        if( pAndExpr && !ExprHasProperty(pOrExpr, EP_FromJoin) ){
          pAndExpr->pLeft = pOrExpr;
          pOrExpr = pAndExpr;
        }
        /* Loop through table entries that match term pOrTerm. */
        WHERETRACE(0xffff, ("Subplan for OR-clause:\n"));
        pSubWInfo = sqlite3WhereBegin(pParse, pOrTab, pOrExpr, 0, 0,
                                      wctrlFlags, iCovCur);
        assert( pSubWInfo || pParse->nErr || db->mallocFailed );
        if( pSubWInfo ){
          WhereLoop *pSubLoop;
          int addrExplain = explainOneScan(
              pParse, pOrTab, &pSubWInfo->a[0], iLevel, pLevel->iFrom, 0
          );
          addScanStatus(v, pOrTab, &pSubWInfo->a[0], addrExplain);

          /* This is the sub-WHERE clause body.  First skip over
          ** duplicate rows from prior sub-WHERE clauses, and record the
          ** rowid (or PRIMARY KEY) for the current row so that the same
          ** row will be skipped in subsequent sub-WHERE clauses.
          */
          if( (pWInfo->wctrlFlags & WHERE_DUPLICATES_OK)==0 ){
            int r;
            int iSet = ((ii==pOrWc->nTerm-1)?-1:ii);
            if( HasRowid(pTab) ){
              r = sqlite3ExprCodeGetColumn(pParse, pTab, -1, iCur, regRowid, 0);
              j1 = sqlite3VdbeAddOp4Int(v, OP_RowSetTest, regRowset, 0, r,iSet);
              VdbeCoverage(v);
            }else{
              Index *pPk = sqlite3PrimaryKeyIndex(pTab);
              int nPk = pPk->nKeyCol;
              int iPk;

              /* Read the PK into an array of temp registers. */
              r = sqlite3GetTempRange(pParse, nPk);
              for(iPk=0; iPk<nPk; iPk++){
                int iCol = pPk->aiColumn[iPk];
                sqlite3ExprCodeGetColumn(pParse, pTab, iCol, iCur, r+iPk, 0);
              }

              /* Check if the temp table already contains this key. If so,
              ** the row has already been included in the result set and
              ** can be ignored (by jumping past the Gosub below). Otherwise,
              ** insert the key into the temp table and proceed with processing
              ** the row.
              **
              ** Use some of the same optimizations as OP_RowSetTest: If iSet
              ** is zero, assume that the key cannot already be present in
              ** the temp table. And if iSet is -1, assume that there is no 
              ** need to insert the key into the temp table, as it will never 
              ** be tested for.  */ 
              if( iSet ){
                j1 = sqlite3VdbeAddOp4Int(v, OP_Found, regRowset, 0, r, nPk);
                VdbeCoverage(v);
              }
              if( iSet>=0 ){
                sqlite3VdbeAddOp3(v, OP_MakeRecord, r, nPk, regRowid);
                sqlite3VdbeAddOp3(v, OP_IdxInsert, regRowset, regRowid, 0);
                if( iSet ) sqlite3VdbeChangeP5(v, OPFLAG_USESEEKRESULT);
              }

              /* Release the array of temp registers */
              sqlite3ReleaseTempRange(pParse, r, nPk);
            }
          }

          /* Invoke the main loop body as a subroutine */
          sqlite3VdbeAddOp2(v, OP_Gosub, regReturn, iLoopBody);

          /* Jump here (skipping the main loop body subroutine) if the
          ** current sub-WHERE row is a duplicate from prior sub-WHEREs. */
          if( j1 ) sqlite3VdbeJumpHere(v, j1);

          /* The pSubWInfo->untestedTerms flag means that this OR term
          ** contained one or more AND term from a notReady table.  The
          ** terms from the notReady table could not be tested and will
          ** need to be tested later.
          */
          if( pSubWInfo->untestedTerms ) untestedTerms = 1;

          /* If all of the OR-connected terms are optimized using the same
          ** index, and the index is opened using the same cursor number
          ** by each call to sqlite3WhereBegin() made by this loop, it may
          ** be possible to use that index as a covering index.
          **
          ** If the call to sqlite3WhereBegin() above resulted in a scan that
          ** uses an index, and this is either the first OR-connected term
          ** processed or the index is the same as that used by all previous
          ** terms, set pCov to the candidate covering index. Otherwise, set 
          ** pCov to NULL to indicate that no candidate covering index will 
          ** be available.
          */
          pSubLoop = pSubWInfo->a[0].pWLoop;
          assert( (pSubLoop->wsFlags & WHERE_AUTO_INDEX)==0 );
          if( (pSubLoop->wsFlags & WHERE_INDEXED)!=0
           && (ii==0 || pSubLoop->u.btree.pIndex==pCov)
           && (HasRowid(pTab) || !IsPrimaryKeyIndex(pSubLoop->u.btree.pIndex))
          ){
            assert( pSubWInfo->a[0].iIdxCur==iCovCur );
            pCov = pSubLoop->u.btree.pIndex;
            wctrlFlags |= WHERE_REOPEN_IDX;
          }else{
            pCov = 0;
          }

          /* Finish the loop through table entries that match term pOrTerm. */
          sqlite3WhereEnd(pSubWInfo);
        }
      }
    }
    pLevel->u.pCovidx = pCov;
    if( pCov ) pLevel->iIdxCur = iCovCur;
    if( pAndExpr ){
      pAndExpr->pLeft = 0;
      sqlite3ExprDelete(db, pAndExpr);
    }
    sqlite3VdbeChangeP1(v, iRetInit, sqlite3VdbeCurrentAddr(v));
    sqlite3VdbeAddOp2(v, OP_Goto, 0, pLevel->addrBrk);
    sqlite3VdbeResolveLabel(v, iLoopBody);

    if( pWInfo->nLevel>1 ) sqlite3StackFree(db, pOrTab);
    if( !untestedTerms ) disableTerm(pLevel, pTerm);
  }else
#endif /* SQLITE_OMIT_OR_OPTIMIZATION */

  {
    /* Case 6:  There is no usable index.  We must do a complete
    **          scan of the entire table.
    */
    static const u8 aStep[] = { OP_Next, OP_Prev };
    static const u8 aStart[] = { OP_Rewind, OP_Last };
    assert( bRev==0 || bRev==1 );
    if( pTabItem->isRecursive ){
      /* Tables marked isRecursive have only a single row that is stored in
      ** a pseudo-cursor.  No need to Rewind or Next such cursors. */
      pLevel->op = OP_Noop;
    }else{
      pLevel->op = aStep[bRev];
      pLevel->p1 = iCur;
      pLevel->p2 = 1 + sqlite3VdbeAddOp2(v, aStart[bRev], iCur, addrBrk);
      VdbeCoverageIf(v, bRev==0);
      VdbeCoverageIf(v, bRev!=0);
      pLevel->p5 = SQLITE_STMTSTATUS_FULLSCAN_STEP;
    }
  }

#ifdef SQLITE_ENABLE_STMT_SCANSTATUS
  pLevel->addrVisit = sqlite3VdbeCurrentAddr(v);
#endif

  /* Insert code to test every subexpression that can be completely
  ** computed using the current set of tables.
  */
  for(pTerm=pWC->a, j=pWC->nTerm; j>0; j--, pTerm++){
    Expr *pE;
    int skipLikeAddr = 0;
    testcase( pTerm->wtFlags & TERM_VIRTUAL );
    testcase( pTerm->wtFlags & TERM_CODED );
    if( pTerm->wtFlags & (TERM_VIRTUAL|TERM_CODED) ) continue;
    if( (pTerm->prereqAll & pLevel->notReady)!=0 ){
      testcase( pWInfo->untestedTerms==0
               && (pWInfo->wctrlFlags & WHERE_ONETABLE_ONLY)!=0 );
      pWInfo->untestedTerms = 1;
      continue;
    }
    pE = pTerm->pExpr;
    assert( pE!=0 );
    if( pLevel->iLeftJoin && !ExprHasProperty(pE, EP_FromJoin) ){
      continue;
    }
    if( pTerm->wtFlags & TERM_LIKECOND ){
      assert( pLevel->iLikeRepCntr>0 );
      skipLikeAddr = sqlite3VdbeAddOp1(v, OP_IfNot, pLevel->iLikeRepCntr);
      VdbeCoverage(v);
    }
    sqlite3ExprIfFalse(pParse, pE, addrCont, SQLITE_JUMPIFNULL);
    if( skipLikeAddr ) sqlite3VdbeJumpHere(v, skipLikeAddr);
    pTerm->wtFlags |= TERM_CODED;
  }

  /* Insert code to test for implied constraints based on transitivity
  ** of the "==" operator.
  **
  ** Example: If the WHERE clause contains "t1.a=t2.b" and "t2.b=123"
  ** and we are coding the t1 loop and the t2 loop has not yet coded,
  ** then we cannot use the "t1.a=t2.b" constraint, but we can code
  ** the implied "t1.a=123" constraint.
  */
  for(pTerm=pWC->a, j=pWC->nTerm; j>0; j--, pTerm++){
    Expr *pE, *pEAlt;
    WhereTerm *pAlt;
    if( pTerm->wtFlags & (TERM_VIRTUAL|TERM_CODED) ) continue;
    if( pTerm->eOperator!=(WO_EQUIV|WO_EQ) ) continue;
    if( pTerm->leftCursor!=iCur ) continue;
    if( pLevel->iLeftJoin ) continue;
    pE = pTerm->pExpr;
    assert( !ExprHasProperty(pE, EP_FromJoin) );
    assert( (pTerm->prereqRight & pLevel->notReady)!=0 );
    pAlt = findTerm(pWC, iCur, pTerm->u.leftColumn, notReady, WO_EQ|WO_IN, 0);
    if( pAlt==0 ) continue;
    if( pAlt->wtFlags & (TERM_CODED) ) continue;
    testcase( pAlt->eOperator & WO_EQ );
    testcase( pAlt->eOperator & WO_IN );
    VdbeModuleComment((v, "begin transitive constraint"));
    pEAlt = sqlite3StackAllocRaw(db, sizeof(*pEAlt));
    if( pEAlt ){
      *pEAlt = *pAlt->pExpr;
      pEAlt->pLeft = pE->pLeft;
      sqlite3ExprIfFalse(pParse, pEAlt, addrCont, SQLITE_JUMPIFNULL);
      sqlite3StackFree(db, pEAlt);
    }
  }

  /* For a LEFT OUTER JOIN, generate code that will record the fact that
  ** at least one row of the right table has matched the left table.  
  */
  if( pLevel->iLeftJoin ){
    pLevel->addrFirst = sqlite3VdbeCurrentAddr(v);
    sqlite3VdbeAddOp2(v, OP_Integer, 1, pLevel->iLeftJoin);
    VdbeComment((v, "record LEFT JOIN hit"));
    sqlite3ExprCacheClear(pParse);
    for(pTerm=pWC->a, j=0; j<pWC->nTerm; j++, pTerm++){
      testcase( pTerm->wtFlags & TERM_VIRTUAL );
      testcase( pTerm->wtFlags & TERM_CODED );
      if( pTerm->wtFlags & (TERM_VIRTUAL|TERM_CODED) ) continue;
      if( (pTerm->prereqAll & pLevel->notReady)!=0 ){
        assert( pWInfo->untestedTerms );
        continue;
      }
      assert( pTerm->pExpr );
      sqlite3ExprIfFalse(pParse, pTerm->pExpr, addrCont, SQLITE_JUMPIFNULL);
      pTerm->wtFlags |= TERM_CODED;
    }
  }

  return pLevel->notReady;
}

#ifdef WHERETRACE_ENABLED
/*
** Print the content of a WhereTerm object
*/
static void whereTermPrint(WhereTerm *pTerm, int iTerm){
  if( pTerm==0 ){
    sqlite3DebugPrintf("TERM-%-3d NULL\n", iTerm);
  }else{
    char zType[4];
    memcpy(zType, "...", 4);
    if( pTerm->wtFlags & TERM_VIRTUAL ) zType[0] = 'V';
    if( pTerm->eOperator & WO_EQUIV  ) zType[1] = 'E';
    if( ExprHasProperty(pTerm->pExpr, EP_FromJoin) ) zType[2] = 'L';
    sqlite3DebugPrintf("TERM-%-3d %p %s cursor=%-3d prob=%-3d op=0x%03x\n",
                       iTerm, pTerm, zType, pTerm->leftCursor, pTerm->truthProb,
                       pTerm->eOperator);
    sqlite3TreeViewExpr(0, pTerm->pExpr, 0);
  }
}
#endif

#ifdef WHERETRACE_ENABLED
/*
** Print a WhereLoop object for debugging purposes
*/
static void whereLoopPrint(WhereLoop *p, WhereClause *pWC){
  WhereInfo *pWInfo = pWC->pWInfo;
  int nb = 1+(pWInfo->pTabList->nSrc+7)/8;
  struct SrcList_item *pItem = pWInfo->pTabList->a + p->iTab;
  Table *pTab = pItem->pTab;
  sqlite3DebugPrintf("%c%2d.%0*llx.%0*llx", p->cId,
                     p->iTab, nb, p->maskSelf, nb, p->prereq);
  sqlite3DebugPrintf(" %12s",
                     pItem->zAlias ? pItem->zAlias : pTab->zName);
  if( (p->wsFlags & WHERE_VIRTUALTABLE)==0 ){
    const char *zName;
    if( p->u.btree.pIndex && (zName = p->u.btree.pIndex->zName)!=0 ){
      if( strncmp(zName, "sqlite_autoindex_", 17)==0 ){
        int i = sqlite3Strlen30(zName) - 1;
        while( zName[i]!='_' ) i--;
        zName += i;
      }
      sqlite3DebugPrintf(".%-16s %2d", zName, p->u.btree.nEq);
    }else{
      sqlite3DebugPrintf("%20s","");
    }
  }else{
    char *z;
    if( p->u.vtab.idxStr ){
      z = sqlite3_mprintf("(%d,\"%s\",%x)",
                p->u.vtab.idxNum, p->u.vtab.idxStr, p->u.vtab.omitMask);
    }else{
      z = sqlite3_mprintf("(%d,%x)", p->u.vtab.idxNum, p->u.vtab.omitMask);
    }
    sqlite3DebugPrintf(" %-19s", z);
    sqlite3_free(z);
  }
  if( p->wsFlags & WHERE_SKIPSCAN ){
    sqlite3DebugPrintf(" f %05x %d-%d", p->wsFlags, p->nLTerm,p->nSkip);
  }else{
    sqlite3DebugPrintf(" f %05x N %d", p->wsFlags, p->nLTerm);
  }
  sqlite3DebugPrintf(" cost %d,%d,%d\n", p->rSetup, p->rRun, p->nOut);
  if( p->nLTerm && (sqlite3WhereTrace & 0x100)!=0 ){
    int i;
    for(i=0; i<p->nLTerm; i++){
      whereTermPrint(p->aLTerm[i], i);
    }
  }
}
#endif

/*
** Convert bulk memory into a valid WhereLoop that can be passed
** to whereLoopClear harmlessly.
*/
static void whereLoopInit(WhereLoop *p){
  p->aLTerm = p->aLTermSpace;
  p->nLTerm = 0;
  p->nLSlot = ArraySize(p->aLTermSpace);
  p->wsFlags = 0;
}

/*
** Clear the WhereLoop.u union.  Leave WhereLoop.pLTerm intact.
*/
static void whereLoopClearUnion(sqlite3 *db, WhereLoop *p){
  if( p->wsFlags & (WHERE_VIRTUALTABLE|WHERE_AUTO_INDEX) ){
    if( (p->wsFlags & WHERE_VIRTUALTABLE)!=0 && p->u.vtab.needFree ){
      sqlite3_free(p->u.vtab.idxStr);
      p->u.vtab.needFree = 0;
      p->u.vtab.idxStr = 0;
    }else if( (p->wsFlags & WHERE_AUTO_INDEX)!=0 && p->u.btree.pIndex!=0 ){
      sqlite3DbFree(db, p->u.btree.pIndex->zColAff);
      sqlite3DbFree(db, p->u.btree.pIndex);
      p->u.btree.pIndex = 0;
    }
  }
}

/*
** Deallocate internal memory used by a WhereLoop object
*/
static void whereLoopClear(sqlite3 *db, WhereLoop *p){
  if( p->aLTerm!=p->aLTermSpace ) sqlite3DbFree(db, p->aLTerm);
  whereLoopClearUnion(db, p);
  whereLoopInit(p);
}

/*
** Increase the memory allocation for pLoop->aLTerm[] to be at least n.
*/
static int whereLoopResize(sqlite3 *db, WhereLoop *p, int n){
  WhereTerm **paNew;
  if( p->nLSlot>=n ) return SQLITE_OK;
  n = (n+7)&~7;
  paNew = sqlite3DbMallocRaw(db, sizeof(p->aLTerm[0])*n);
  if( paNew==0 ) return SQLITE_NOMEM;
  memcpy(paNew, p->aLTerm, sizeof(p->aLTerm[0])*p->nLSlot);
  if( p->aLTerm!=p->aLTermSpace ) sqlite3DbFree(db, p->aLTerm);
  p->aLTerm = paNew;
  p->nLSlot = n;
  return SQLITE_OK;
}

/*
** Transfer content from the second pLoop into the first.
*/
static int whereLoopXfer(sqlite3 *db, WhereLoop *pTo, WhereLoop *pFrom){
  whereLoopClearUnion(db, pTo);
  if( whereLoopResize(db, pTo, pFrom->nLTerm) ){
    memset(&pTo->u, 0, sizeof(pTo->u));
    return SQLITE_NOMEM;
  }
  memcpy(pTo, pFrom, WHERE_LOOP_XFER_SZ);
  memcpy(pTo->aLTerm, pFrom->aLTerm, pTo->nLTerm*sizeof(pTo->aLTerm[0]));
  if( pFrom->wsFlags & WHERE_VIRTUALTABLE ){
    pFrom->u.vtab.needFree = 0;
  }else if( (pFrom->wsFlags & WHERE_AUTO_INDEX)!=0 ){
    pFrom->u.btree.pIndex = 0;
  }
  return SQLITE_OK;
}

/*
** Delete a WhereLoop object
*/
static void whereLoopDelete(sqlite3 *db, WhereLoop *p){
  whereLoopClear(db, p);
  sqlite3DbFree(db, p);
}

/*
** Free a WhereInfo structure
*/
static void whereInfoFree(sqlite3 *db, WhereInfo *pWInfo){
  if( ALWAYS(pWInfo) ){
    whereClauseClear(&pWInfo->sWC);
    while( pWInfo->pLoops ){
      WhereLoop *p = pWInfo->pLoops;
      pWInfo->pLoops = p->pNextLoop;
      whereLoopDelete(db, p);
    }
    sqlite3DbFree(db, pWInfo);
  }
}

/*
** Return TRUE if all of the following are true:
**
**   (1)  X has the same or lower cost that Y
**   (2)  X is a proper subset of Y
**   (3)  X skips at least as many columns as Y
**
** By "proper subset" we mean that X uses fewer WHERE clause terms
** than Y and that every WHERE clause term used by X is also used
** by Y.
**
** If X is a proper subset of Y then Y is a better choice and ought
** to have a lower cost.  This routine returns TRUE when that cost 
** relationship is inverted and needs to be adjusted.  The third rule
** was added because if X uses skip-scan less than Y it still might
** deserve a lower cost even if it is a proper subset of Y.
*/
static int whereLoopCheaperProperSubset(
  const WhereLoop *pX,       /* First WhereLoop to compare */
  const WhereLoop *pY        /* Compare against this WhereLoop */
){
  int i, j;
  if( pX->nLTerm-pX->nSkip >= pY->nLTerm-pY->nSkip ){
    return 0; /* X is not a subset of Y */
  }
  if( pY->nSkip > pX->nSkip ) return 0;
  if( pX->rRun >= pY->rRun ){
    if( pX->rRun > pY->rRun ) return 0;    /* X costs more than Y */
    if( pX->nOut > pY->nOut ) return 0;    /* X costs more than Y */
  }
  for(i=pX->nLTerm-1; i>=0; i--){
    if( pX->aLTerm[i]==0 ) continue;
    for(j=pY->nLTerm-1; j>=0; j--){
      if( pY->aLTerm[j]==pX->aLTerm[i] ) break;
    }
    if( j<0 ) return 0;  /* X not a subset of Y since term X[i] not used by Y */
  }
  return 1;  /* All conditions meet */
}

/*
** Try to adjust the cost of WhereLoop pTemplate upwards or downwards so
** that:
**
**   (1) pTemplate costs less than any other WhereLoops that are a proper
**       subset of pTemplate
**
**   (2) pTemplate costs more than any other WhereLoops for which pTemplate
**       is a proper subset.
**
** To say "WhereLoop X is a proper subset of Y" means that X uses fewer
** WHERE clause terms than Y and that every WHERE clause term used by X is
** also used by Y.
*/
static void whereLoopAdjustCost(const WhereLoop *p, WhereLoop *pTemplate){
  if( (pTemplate->wsFlags & WHERE_INDEXED)==0 ) return;
  for(; p; p=p->pNextLoop){
    if( p->iTab!=pTemplate->iTab ) continue;
    if( (p->wsFlags & WHERE_INDEXED)==0 ) continue;
    if( whereLoopCheaperProperSubset(p, pTemplate) ){
      /* Adjust pTemplate cost downward so that it is cheaper than its 
      ** subset p. */
      WHERETRACE(0x80,("subset cost adjustment %d,%d to %d,%d\n",
                       pTemplate->rRun, pTemplate->nOut, p->rRun, p->nOut-1));
      pTemplate->rRun = p->rRun;
      pTemplate->nOut = p->nOut - 1;
    }else if( whereLoopCheaperProperSubset(pTemplate, p) ){
      /* Adjust pTemplate cost upward so that it is costlier than p since
      ** pTemplate is a proper subset of p */
      WHERETRACE(0x80,("subset cost adjustment %d,%d to %d,%d\n",
                       pTemplate->rRun, pTemplate->nOut, p->rRun, p->nOut+1));
      pTemplate->rRun = p->rRun;
      pTemplate->nOut = p->nOut + 1;
    }
  }
}

/*
** Search the list of WhereLoops in *ppPrev looking for one that can be
** supplanted by pTemplate.
**
** Return NULL if the WhereLoop list contains an entry that can supplant
** pTemplate, in other words if pTemplate does not belong on the list.
**
** If pX is a WhereLoop that pTemplate can supplant, then return the
** link that points to pX.
**
** If pTemplate cannot supplant any existing element of the list but needs
** to be added to the list, then return a pointer to the tail of the list.
*/
static WhereLoop **whereLoopFindLesser(
  WhereLoop **ppPrev,
  const WhereLoop *pTemplate
){
  WhereLoop *p;
  for(p=(*ppPrev); p; ppPrev=&p->pNextLoop, p=*ppPrev){
    if( p->iTab!=pTemplate->iTab || p->iSortIdx!=pTemplate->iSortIdx ){
      /* If either the iTab or iSortIdx values for two WhereLoop are different
      ** then those WhereLoops need to be considered separately.  Neither is
      ** a candidate to replace the other. */
      continue;
    }
    /* In the current implementation, the rSetup value is either zero
    ** or the cost of building an automatic index (NlogN) and the NlogN
    ** is the same for compatible WhereLoops. */
    assert( p->rSetup==0 || pTemplate->rSetup==0 
                 || p->rSetup==pTemplate->rSetup );

    /* whereLoopAddBtree() always generates and inserts the automatic index
    ** case first.  Hence compatible candidate WhereLoops never have a larger
    ** rSetup. Call this SETUP-INVARIANT */
    assert( p->rSetup>=pTemplate->rSetup );

    /* Any loop using an appliation-defined index (or PRIMARY KEY or
    ** UNIQUE constraint) with one or more == constraints is better
    ** than an automatic index. Unless it is a skip-scan. */
    if( (p->wsFlags & WHERE_AUTO_INDEX)!=0
     && (pTemplate->nSkip)==0
     && (pTemplate->wsFlags & WHERE_INDEXED)!=0
     && (pTemplate->wsFlags & WHERE_COLUMN_EQ)!=0
     && (p->prereq & pTemplate->prereq)==pTemplate->prereq
    ){
      break;
    }

    /* If existing WhereLoop p is better than pTemplate, pTemplate can be
    ** discarded.  WhereLoop p is better if:
    **   (1)  p has no more dependencies than pTemplate, and
    **   (2)  p has an equal or lower cost than pTemplate
    */
    if( (p->prereq & pTemplate->prereq)==p->prereq    /* (1)  */
     && p->rSetup<=pTemplate->rSetup                  /* (2a) */
     && p->rRun<=pTemplate->rRun                      /* (2b) */
     && p->nOut<=pTemplate->nOut                      /* (2c) */
    ){
      return 0;  /* Discard pTemplate */
    }

    /* If pTemplate is always better than p, then cause p to be overwritten
    ** with pTemplate.  pTemplate is better than p if:
    **   (1)  pTemplate has no more dependences than p, and
    **   (2)  pTemplate has an equal or lower cost than p.
    */
    if( (p->prereq & pTemplate->prereq)==pTemplate->prereq   /* (1)  */
     && p->rRun>=pTemplate->rRun                             /* (2a) */
     && p->nOut>=pTemplate->nOut                             /* (2b) */
    ){
      assert( p->rSetup>=pTemplate->rSetup ); /* SETUP-INVARIANT above */
      break;   /* Cause p to be overwritten by pTemplate */
    }
  }
  return ppPrev;
}

/*
** Insert or replace a WhereLoop entry using the template supplied.
**
** An existing WhereLoop entry might be overwritten if the new template
** is better and has fewer dependencies.  Or the template will be ignored
** and no insert will occur if an existing WhereLoop is faster and has
** fewer dependencies than the template.  Otherwise a new WhereLoop is
** added based on the template.
**
** If pBuilder->pOrSet is not NULL then we care about only the
** prerequisites and rRun and nOut costs of the N best loops.  That
** information is gathered in the pBuilder->pOrSet object.  This special
** processing mode is used only for OR clause processing.
**
** When accumulating multiple loops (when pBuilder->pOrSet is NULL) we
** still might overwrite similar loops with the new template if the
** new template is better.  Loops may be overwritten if the following 
** conditions are met:
**
**    (1)  They have the same iTab.
**    (2)  They have the same iSortIdx.
**    (3)  The template has same or fewer dependencies than the current loop
**    (4)  The template has the same or lower cost than the current loop
*/
static int whereLoopInsert(WhereLoopBuilder *pBuilder, WhereLoop *pTemplate){
  WhereLoop **ppPrev, *p;
  WhereInfo *pWInfo = pBuilder->pWInfo;
  sqlite3 *db = pWInfo->pParse->db;

  /* If pBuilder->pOrSet is defined, then only keep track of the costs
  ** and prereqs.
  */
  if( pBuilder->pOrSet!=0 ){
#if WHERETRACE_ENABLED
    u16 n = pBuilder->pOrSet->n;
    int x =
#endif
    whereOrInsert(pBuilder->pOrSet, pTemplate->prereq, pTemplate->rRun,
                                    pTemplate->nOut);
#if WHERETRACE_ENABLED /* 0x8 */
    if( sqlite3WhereTrace & 0x8 ){
      sqlite3DebugPrintf(x?"   or-%d:  ":"   or-X:  ", n);
      whereLoopPrint(pTemplate, pBuilder->pWC);
    }
#endif
    return SQLITE_OK;
  }

  /* Look for an existing WhereLoop to replace with pTemplate
  */
  whereLoopAdjustCost(pWInfo->pLoops, pTemplate);
  ppPrev = whereLoopFindLesser(&pWInfo->pLoops, pTemplate);

  if( ppPrev==0 ){
    /* There already exists a WhereLoop on the list that is better
    ** than pTemplate, so just ignore pTemplate */
#if WHERETRACE_ENABLED /* 0x8 */
    if( sqlite3WhereTrace & 0x8 ){
      sqlite3DebugPrintf("   skip: ");
      whereLoopPrint(pTemplate, pBuilder->pWC);
    }
#endif
    return SQLITE_OK;  
  }else{
    p = *ppPrev;
  }

  /* If we reach this point it means that either p[] should be overwritten
  ** with pTemplate[] if p[] exists, or if p==NULL then allocate a new
  ** WhereLoop and insert it.
  */
#if WHERETRACE_ENABLED /* 0x8 */
  if( sqlite3WhereTrace & 0x8 ){
    if( p!=0 ){
      sqlite3DebugPrintf("replace: ");
      whereLoopPrint(p, pBuilder->pWC);
    }
    sqlite3DebugPrintf("    add: ");
    whereLoopPrint(pTemplate, pBuilder->pWC);
  }
#endif
  if( p==0 ){
    /* Allocate a new WhereLoop to add to the end of the list */
    *ppPrev = p = sqlite3DbMallocRaw(db, sizeof(WhereLoop));
    if( p==0 ) return SQLITE_NOMEM;
    whereLoopInit(p);
    p->pNextLoop = 0;
  }else{
    /* We will be overwriting WhereLoop p[].  But before we do, first
    ** go through the rest of the list and delete any other entries besides
    ** p[] that are also supplated by pTemplate */
    WhereLoop **ppTail = &p->pNextLoop;
    WhereLoop *pToDel;
    while( *ppTail ){
      ppTail = whereLoopFindLesser(ppTail, pTemplate);
      if( ppTail==0 ) break;
      pToDel = *ppTail;
      if( pToDel==0 ) break;
      *ppTail = pToDel->pNextLoop;
#if WHERETRACE_ENABLED /* 0x8 */
      if( sqlite3WhereTrace & 0x8 ){
        sqlite3DebugPrintf(" delete: ");
        whereLoopPrint(pToDel, pBuilder->pWC);
      }
#endif
      whereLoopDelete(db, pToDel);
    }
  }
  whereLoopXfer(db, p, pTemplate);
  if( (p->wsFlags & WHERE_VIRTUALTABLE)==0 ){
    Index *pIndex = p->u.btree.pIndex;
    if( pIndex && pIndex->tnum==0 ){
      p->u.btree.pIndex = 0;
    }
  }
  return SQLITE_OK;
}

/*
** Adjust the WhereLoop.nOut value downward to account for terms of the
** WHERE clause that reference the loop but which are not used by an
** index.
*
** For every WHERE clause term that is not used by the index
** and which has a truth probability assigned by one of the likelihood(),
** likely(), or unlikely() SQL functions, reduce the estimated number
** of output rows by the probability specified.
**
** TUNING:  For every WHERE clause term that is not used by the index
** and which does not have an assigned truth probability, heuristics
** described below are used to try to estimate the truth probability.
** TODO --> Perhaps this is something that could be improved by better
** table statistics.
**
** Heuristic 1:  Estimate the truth probability as 93.75%.  The 93.75%
** value corresponds to -1 in LogEst notation, so this means decrement
** the WhereLoop.nOut field for every such WHERE clause term.
**
** Heuristic 2:  If there exists one or more WHERE clause terms of the
** form "x==EXPR" and EXPR is not a constant 0 or 1, then make sure the
** final output row estimate is no greater than 1/4 of the total number
** of rows in the table.  In other words, assume that x==EXPR will filter
** out at least 3 out of 4 rows.  If EXPR is -1 or 0 or 1, then maybe the
** "x" column is boolean or else -1 or 0 or 1 is a common default value
** on the "x" column and so in that case only cap the output row estimate
** at 1/2 instead of 1/4.
*/
static void whereLoopOutputAdjust(
  WhereClause *pWC,      /* The WHERE clause */
  WhereLoop *pLoop,      /* The loop to adjust downward */
  LogEst nRow            /* Number of rows in the entire table */
){
  WhereTerm *pTerm, *pX;
  Bitmask notAllowed = ~(pLoop->prereq|pLoop->maskSelf);
  int i, j, k;
  LogEst iReduce = 0;    /* pLoop->nOut should not exceed nRow-iReduce */

  assert( (pLoop->wsFlags & WHERE_AUTO_INDEX)==0 );
  for(i=pWC->nTerm, pTerm=pWC->a; i>0; i--, pTerm++){
    if( (pTerm->wtFlags & TERM_VIRTUAL)!=0 ) break;
    if( (pTerm->prereqAll & pLoop->maskSelf)==0 ) continue;
    if( (pTerm->prereqAll & notAllowed)!=0 ) continue;
    for(j=pLoop->nLTerm-1; j>=0; j--){
      pX = pLoop->aLTerm[j];
      if( pX==0 ) continue;
      if( pX==pTerm ) break;
      if( pX->iParent>=0 && (&pWC->a[pX->iParent])==pTerm ) break;
    }
    if( j<0 ){
      if( pTerm->truthProb<=0 ){
        /* If a truth probability is specified using the likelihood() hints,
        ** then use the probability provided by the application. */
        pLoop->nOut += pTerm->truthProb;
      }else{
        /* In the absence of explicit truth probabilities, use heuristics to
        ** guess a reasonable truth probability. */
        pLoop->nOut--;
        if( pTerm->eOperator&WO_EQ ){
          Expr *pRight = pTerm->pExpr->pRight;
          if( sqlite3ExprIsInteger(pRight, &k) && k>=(-1) && k<=1 ){
            k = 10;
          }else{
            k = 20;
          }
          if( iReduce<k ) iReduce = k;
        }
      }
    }
  }
  if( pLoop->nOut > nRow-iReduce )  pLoop->nOut = nRow - iReduce;
}

/*
** Adjust the cost C by the costMult facter T.  This only occurs if
** compiled with -DSQLITE_ENABLE_COSTMULT
*/
#ifdef SQLITE_ENABLE_COSTMULT
# define ApplyCostMultiplier(C,T)  C += T
#else
# define ApplyCostMultiplier(C,T)
#endif

/*
** We have so far matched pBuilder->pNew->u.btree.nEq terms of the 
** index pIndex. Try to match one more.
**
** When this function is called, pBuilder->pNew->nOut contains the 
** number of rows expected to be visited by filtering using the nEq 
** terms only. If it is modified, this value is restored before this 
** function returns.
**
** If pProbe->tnum==0, that means pIndex is a fake index used for the
** INTEGER PRIMARY KEY.
*/
static int whereLoopAddBtreeIndex(
  WhereLoopBuilder *pBuilder,     /* The WhereLoop factory */
  struct SrcList_item *pSrc,      /* FROM clause term being analyzed */
  Index *pProbe,                  /* An index on pSrc */
  LogEst nInMul                   /* log(Number of iterations due to IN) */
){
  WhereInfo *pWInfo = pBuilder->pWInfo;  /* WHERE analyse context */
  Parse *pParse = pWInfo->pParse;        /* Parsing context */
  sqlite3 *db = pParse->db;       /* Database connection malloc context */
  WhereLoop *pNew;                /* Template WhereLoop under construction */
  WhereTerm *pTerm;               /* A WhereTerm under consideration */
  int opMask;                     /* Valid operators for constraints */
  WhereScan scan;                 /* Iterator for WHERE terms */
  Bitmask saved_prereq;           /* Original value of pNew->prereq */
  u16 saved_nLTerm;               /* Original value of pNew->nLTerm */
  u16 saved_nEq;                  /* Original value of pNew->u.btree.nEq */
  u16 saved_nSkip;                /* Original value of pNew->nSkip */
  u32 saved_wsFlags;              /* Original value of pNew->wsFlags */
  LogEst saved_nOut;              /* Original value of pNew->nOut */
  int iCol;                       /* Index of the column in the table */
  int rc = SQLITE_OK;             /* Return code */
  LogEst rSize;                   /* Number of rows in the table */
  LogEst rLogSize;                /* Logarithm of table size */
  WhereTerm *pTop = 0, *pBtm = 0; /* Top and bottom range constraints */

  pNew = pBuilder->pNew;
  if( db->mallocFailed ) return SQLITE_NOMEM;

  assert( (pNew->wsFlags & WHERE_VIRTUALTABLE)==0 );
  assert( (pNew->wsFlags & WHERE_TOP_LIMIT)==0 );
  if( pNew->wsFlags & WHERE_BTM_LIMIT ){
    opMask = WO_LT|WO_LE;
  }else if( pProbe->tnum<=0 || (pSrc->jointype & JT_LEFT)!=0 ){
    opMask = WO_EQ|WO_IN|WO_GT|WO_GE|WO_LT|WO_LE;
  }else{
    opMask = WO_EQ|WO_IN|WO_ISNULL|WO_GT|WO_GE|WO_LT|WO_LE;
  }
  if( pProbe->bUnordered ) opMask &= ~(WO_GT|WO_GE|WO_LT|WO_LE);

  assert( pNew->u.btree.nEq<pProbe->nColumn );
  iCol = pProbe->aiColumn[pNew->u.btree.nEq];

  pTerm = whereScanInit(&scan, pBuilder->pWC, pSrc->iCursor, iCol,
                        opMask, pProbe);
  saved_nEq = pNew->u.btree.nEq;
  saved_nSkip = pNew->nSkip;
  saved_nLTerm = pNew->nLTerm;
  saved_wsFlags = pNew->wsFlags;
  saved_prereq = pNew->prereq;
  saved_nOut = pNew->nOut;
  pNew->rSetup = 0;
  rSize = pProbe->aiRowLogEst[0];
  rLogSize = estLog(rSize);
  for(; rc==SQLITE_OK && pTerm!=0; pTerm = whereScanNext(&scan)){
    u16 eOp = pTerm->eOperator;   /* Shorthand for pTerm->eOperator */
    LogEst rCostIdx;
    LogEst nOutUnadjusted;        /* nOut before IN() and WHERE adjustments */
    int nIn = 0;
#ifdef SQLITE_ENABLE_STAT3_OR_STAT4
    int nRecValid = pBuilder->nRecValid;
#endif
    if( (eOp==WO_ISNULL || (pTerm->wtFlags&TERM_VNULL)!=0)
     && (iCol<0 || pSrc->pTab->aCol[iCol].notNull)
    ){
      continue; /* ignore IS [NOT] NULL constraints on NOT NULL columns */
    }
    if( pTerm->prereqRight & pNew->maskSelf ) continue;

    /* Do not allow the upper bound of a LIKE optimization range constraint
    ** to mix with a lower range bound from some other source */
    if( pTerm->wtFlags & TERM_LIKEOPT && pTerm->eOperator==WO_LT ) continue;

    pNew->wsFlags = saved_wsFlags;
    pNew->u.btree.nEq = saved_nEq;
    pNew->nLTerm = saved_nLTerm;
    if( whereLoopResize(db, pNew, pNew->nLTerm+1) ) break; /* OOM */
    pNew->aLTerm[pNew->nLTerm++] = pTerm;
    pNew->prereq = (saved_prereq | pTerm->prereqRight) & ~pNew->maskSelf;

    assert( nInMul==0
        || (pNew->wsFlags & WHERE_COLUMN_NULL)!=0 
        || (pNew->wsFlags & WHERE_COLUMN_IN)!=0 
        || (pNew->wsFlags & WHERE_SKIPSCAN)!=0 
    );

    if( eOp & WO_IN ){
      Expr *pExpr = pTerm->pExpr;
      pNew->wsFlags |= WHERE_COLUMN_IN;
      if( ExprHasProperty(pExpr, EP_xIsSelect) ){
        /* "x IN (SELECT ...)":  TUNING: the SELECT returns 25 rows */
        nIn = 46;  assert( 46==sqlite3LogEst(25) );
      }else if( ALWAYS(pExpr->x.pList && pExpr->x.pList->nExpr) ){
        /* "x IN (value, value, ...)" */
        nIn = sqlite3LogEst(pExpr->x.pList->nExpr);
      }
      assert( nIn>0 );  /* RHS always has 2 or more terms...  The parser
                        ** changes "x IN (?)" into "x=?". */

    }else if( eOp & (WO_EQ) ){
      pNew->wsFlags |= WHERE_COLUMN_EQ;
      if( iCol<0 || (nInMul==0 && pNew->u.btree.nEq==pProbe->nKeyCol-1) ){
        if( iCol>=0 && !IsUniqueIndex(pProbe) ){
          pNew->wsFlags |= WHERE_UNQ_WANTED;
        }else{
          pNew->wsFlags |= WHERE_ONEROW;
        }
      }
    }else if( eOp & WO_ISNULL ){
      pNew->wsFlags |= WHERE_COLUMN_NULL;
    }else if( eOp & (WO_GT|WO_GE) ){
      testcase( eOp & WO_GT );
      testcase( eOp & WO_GE );
      pNew->wsFlags |= WHERE_COLUMN_RANGE|WHERE_BTM_LIMIT;
      pBtm = pTerm;
      pTop = 0;
      if( pTerm->wtFlags & TERM_LIKEOPT ){
        /* Range contraints that come from the LIKE optimization are
        ** always used in pairs. */
        pTop = &pTerm[1];
        assert( (pTop-(pTerm->pWC->a))<pTerm->pWC->nTerm );
        assert( pTop->wtFlags & TERM_LIKEOPT );
        assert( pTop->eOperator==WO_LT );
        if( whereLoopResize(db, pNew, pNew->nLTerm+1) ) break; /* OOM */
        pNew->aLTerm[pNew->nLTerm++] = pTop;
        pNew->wsFlags |= WHERE_TOP_LIMIT;
      }
    }else{
      assert( eOp & (WO_LT|WO_LE) );
      testcase( eOp & WO_LT );
      testcase( eOp & WO_LE );
      pNew->wsFlags |= WHERE_COLUMN_RANGE|WHERE_TOP_LIMIT;
      pTop = pTerm;
      pBtm = (pNew->wsFlags & WHERE_BTM_LIMIT)!=0 ?
                     pNew->aLTerm[pNew->nLTerm-2] : 0;
    }

    /* At this point pNew->nOut is set to the number of rows expected to
    ** be visited by the index scan before considering term pTerm, or the
    ** values of nIn and nInMul. In other words, assuming that all 
    ** "x IN(...)" terms are replaced with "x = ?". This block updates
    ** the value of pNew->nOut to account for pTerm (but not nIn/nInMul).  */
    assert( pNew->nOut==saved_nOut );
    if( pNew->wsFlags & WHERE_COLUMN_RANGE ){
      /* Adjust nOut using stat3/stat4 data. Or, if there is no stat3/stat4
      ** data, using some other estimate.  */
      whereRangeScanEst(pParse, pBuilder, pBtm, pTop, pNew);
    }else{
      int nEq = ++pNew->u.btree.nEq;
      assert( eOp & (WO_ISNULL|WO_EQ|WO_IN) );

      assert( pNew->nOut==saved_nOut );
      if( pTerm->truthProb<=0 && iCol>=0 ){
        assert( (eOp & WO_IN) || nIn==0 );
        testcase( eOp & WO_IN );
        pNew->nOut += pTerm->truthProb;
        pNew->nOut -= nIn;
      }else{
#ifdef SQLITE_ENABLE_STAT3_OR_STAT4
        tRowcnt nOut = 0;
        if( nInMul==0 
         && pProbe->nSample 
         && pNew->u.btree.nEq<=pProbe->nSampleCol
         && ((eOp & WO_IN)==0 || !ExprHasProperty(pTerm->pExpr, EP_xIsSelect))
        ){
          Expr *pExpr = pTerm->pExpr;
          if( (eOp & (WO_EQ|WO_ISNULL))!=0 ){
            testcase( eOp & WO_EQ );
            testcase( eOp & WO_ISNULL );
            rc = whereEqualScanEst(pParse, pBuilder, pExpr->pRight, &nOut);
          }else{
            rc = whereInScanEst(pParse, pBuilder, pExpr->x.pList, &nOut);
          }
          if( rc==SQLITE_NOTFOUND ) rc = SQLITE_OK;
          if( rc!=SQLITE_OK ) break;          /* Jump out of the pTerm loop */
          if( nOut ){
            pNew->nOut = sqlite3LogEst(nOut);
            if( pNew->nOut>saved_nOut ) pNew->nOut = saved_nOut;
            pNew->nOut -= nIn;
          }
        }
        if( nOut==0 )
#endif
        {
          pNew->nOut += (pProbe->aiRowLogEst[nEq] - pProbe->aiRowLogEst[nEq-1]);
          if( eOp & WO_ISNULL ){
            /* TUNING: If there is no likelihood() value, assume that a 
            ** "col IS NULL" expression matches twice as many rows 
            ** as (col=?). */
            pNew->nOut += 10;
          }
        }
      }
    }

    /* Set rCostIdx to the cost of visiting selected rows in index. Add
    ** it to pNew->rRun, which is currently set to the cost of the index
    ** seek only. Then, if this is a non-covering index, add the cost of
    ** visiting the rows in the main table.  */
    rCostIdx = pNew->nOut + 1 + (15*pProbe->szIdxRow)/pSrc->pTab->szTabRow;
    pNew->rRun = sqlite3LogEstAdd(rLogSize, rCostIdx);
    if( (pNew->wsFlags & (WHERE_IDX_ONLY|WHERE_IPK))==0 ){
      pNew->rRun = sqlite3LogEstAdd(pNew->rRun, pNew->nOut + 16);
    }
    ApplyCostMultiplier(pNew->rRun, pProbe->pTable->costMult);

    nOutUnadjusted = pNew->nOut;
    pNew->rRun += nInMul + nIn;
    pNew->nOut += nInMul + nIn;
    whereLoopOutputAdjust(pBuilder->pWC, pNew, rSize);
    rc = whereLoopInsert(pBuilder, pNew);

    if( pNew->wsFlags & WHERE_COLUMN_RANGE ){
      pNew->nOut = saved_nOut;
    }else{
      pNew->nOut = nOutUnadjusted;
    }

    if( (pNew->wsFlags & WHERE_TOP_LIMIT)==0
     && pNew->u.btree.nEq<pProbe->nColumn
    ){
      whereLoopAddBtreeIndex(pBuilder, pSrc, pProbe, nInMul+nIn);
    }
    pNew->nOut = saved_nOut;
#ifdef SQLITE_ENABLE_STAT3_OR_STAT4
    pBuilder->nRecValid = nRecValid;
#endif
  }
  pNew->prereq = saved_prereq;
  pNew->u.btree.nEq = saved_nEq;
  pNew->nSkip = saved_nSkip;
  pNew->wsFlags = saved_wsFlags;
  pNew->nOut = saved_nOut;
  pNew->nLTerm = saved_nLTerm;

  /* Consider using a skip-scan if there are no WHERE clause constraints
  ** available for the left-most terms of the index, and if the average
  ** number of repeats in the left-most terms is at least 18. 
  **
  ** The magic number 18 is selected on the basis that scanning 17 rows
  ** is almost always quicker than an index seek (even though if the index
  ** contains fewer than 2^17 rows we assume otherwise in other parts of
  ** the code). And, even if it is not, it should not be too much slower. 
  ** On the other hand, the extra seeks could end up being significantly
  ** more expensive.  */
  assert( 42==sqlite3LogEst(18) );
  if( saved_nEq==saved_nSkip
   && saved_nEq+1<pProbe->nKeyCol
   && pProbe->noSkipScan==0
   && pProbe->aiRowLogEst[saved_nEq+1]>=42  /* TUNING: Minimum for skip-scan */
   && (rc = whereLoopResize(db, pNew, pNew->nLTerm+1))==SQLITE_OK
  ){
    LogEst nIter;
    pNew->u.btree.nEq++;
    pNew->nSkip++;
    pNew->aLTerm[pNew->nLTerm++] = 0;
    pNew->wsFlags |= WHERE_SKIPSCAN;
    nIter = pProbe->aiRowLogEst[saved_nEq] - pProbe->aiRowLogEst[saved_nEq+1];
    pNew->nOut -= nIter;
    /* TUNING:  Because uncertainties in the estimates for skip-scan queries,
    ** add a 1.375 fudge factor to make skip-scan slightly less likely. */
    nIter += 5;
    whereLoopAddBtreeIndex(pBuilder, pSrc, pProbe, nIter + nInMul);
    pNew->nOut = saved_nOut;
    pNew->u.btree.nEq = saved_nEq;
    pNew->nSkip = saved_nSkip;
    pNew->wsFlags = saved_wsFlags;
  }

  return rc;
}

/*
** Return True if it is possible that pIndex might be useful in
** implementing the ORDER BY clause in pBuilder.
**
** Return False if pBuilder does not contain an ORDER BY clause or
** if there is no way for pIndex to be useful in implementing that
** ORDER BY clause.
*/
static int indexMightHelpWithOrderBy(
  WhereLoopBuilder *pBuilder,
  Index *pIndex,
  int iCursor
){
  ExprList *pOB;
  int ii, jj;

  if( pIndex->bUnordered ) return 0;
  if( (pOB = pBuilder->pWInfo->pOrderBy)==0 ) return 0;
  for(ii=0; ii<pOB->nExpr; ii++){
    Expr *pExpr = sqlite3ExprSkipCollate(pOB->a[ii].pExpr);
    if( pExpr->op!=TK_COLUMN ) return 0;
    if( pExpr->iTable==iCursor ){
      if( pExpr->iColumn<0 ) return 1;
      for(jj=0; jj<pIndex->nKeyCol; jj++){
        if( pExpr->iColumn==pIndex->aiColumn[jj] ) return 1;
      }
    }
  }
  return 0;
}

/*
** Return a bitmask where 1s indicate that the corresponding column of
** the table is used by an index.  Only the first 63 columns are considered.
*/
static Bitmask columnsInIndex(Index *pIdx){
  Bitmask m = 0;
  int j;
  for(j=pIdx->nColumn-1; j>=0; j--){
    int x = pIdx->aiColumn[j];
    if( x>=0 ){
      testcase( x==BMS-1 );
      testcase( x==BMS-2 );
      if( x<BMS-1 ) m |= MASKBIT(x);
    }
  }
  return m;
}

/* Check to see if a partial index with pPartIndexWhere can be used
** in the current query.  Return true if it can be and false if not.
*/
static int whereUsablePartialIndex(int iTab, WhereClause *pWC, Expr *pWhere){
  int i;
  WhereTerm *pTerm;
  for(i=0, pTerm=pWC->a; i<pWC->nTerm; i++, pTerm++){
    Expr *pExpr = pTerm->pExpr;
    if( sqlite3ExprImpliesExpr(pExpr, pWhere, iTab) 
     && (!ExprHasProperty(pExpr, EP_FromJoin) || pExpr->iRightJoinTable==iTab)
    ){
      return 1;
    }
  }
  return 0;
}

/*
** Add all WhereLoop objects for a single table of the join where the table
** is idenfied by pBuilder->pNew->iTab.  That table is guaranteed to be
** a b-tree table, not a virtual table.
**
** The costs (WhereLoop.rRun) of the b-tree loops added by this function
** are calculated as follows:
**
** For a full scan, assuming the table (or index) contains nRow rows:
**
**     cost = nRow * 3.0                    // full-table scan
**     cost = nRow * K                      // scan of covering index
**     cost = nRow * (K+3.0)                // scan of non-covering index
**
** where K is a value between 1.1 and 3.0 set based on the relative 
** estimated average size of the index and table records.
**
** For an index scan, where nVisit is the number of index rows visited
** by the scan, and nSeek is the number of seek operations required on 
** the index b-tree:
**
**     cost = nSeek * (log(nRow) + K * nVisit)          // covering index
**     cost = nSeek * (log(nRow) + (K+3.0) * nVisit)    // non-covering index
**
** Normally, nSeek is 1. nSeek values greater than 1 come about if the 
** WHERE clause includes "x IN (....)" terms used in place of "x=?". Or when 
** implicit "x IN (SELECT x FROM tbl)" terms are added for skip-scans.
**
** The estimated values (nRow, nVisit, nSeek) often contain a large amount
** of uncertainty.  For this reason, scoring is designed to pick plans that
** "do the least harm" if the estimates are inaccurate.  For example, a
** log(nRow) factor is omitted from a non-covering index scan in order to
** bias the scoring in favor of using an index, since the worst-case
** performance of using an index is far better than the worst-case performance
** of a full table scan.
*/
static int whereLoopAddBtree(
  WhereLoopBuilder *pBuilder, /* WHERE clause information */
  Bitmask mExtra              /* Extra prerequesites for using this table */
){
  WhereInfo *pWInfo;          /* WHERE analysis context */
  Index *pProbe;              /* An index we are evaluating */
  Index sPk;                  /* A fake index object for the primary key */
  LogEst aiRowEstPk[2];       /* The aiRowLogEst[] value for the sPk index */
  i16 aiColumnPk = -1;        /* The aColumn[] value for the sPk index */
  SrcList *pTabList;          /* The FROM clause */
  struct SrcList_item *pSrc;  /* The FROM clause btree term to add */
  WhereLoop *pNew;            /* Template WhereLoop object */
  int rc = SQLITE_OK;         /* Return code */
  int iSortIdx = 1;           /* Index number */
  int b;                      /* A boolean value */
  LogEst rSize;               /* number of rows in the table */
  LogEst rLogSize;            /* Logarithm of the number of rows in the table */
  WhereClause *pWC;           /* The parsed WHERE clause */
  Table *pTab;                /* Table being queried */
  
  pNew = pBuilder->pNew;
  pWInfo = pBuilder->pWInfo;
  pTabList = pWInfo->pTabList;
  pSrc = pTabList->a + pNew->iTab;
  pTab = pSrc->pTab;
  pWC = pBuilder->pWC;
  assert( !IsVirtual(pSrc->pTab) );

  if( pSrc->pIndex ){
    /* An INDEXED BY clause specifies a particular index to use */
    pProbe = pSrc->pIndex;
  }else if( !HasRowid(pTab) ){
    pProbe = pTab->pIndex;
  }else{
    /* There is no INDEXED BY clause.  Create a fake Index object in local
    ** variable sPk to represent the rowid primary key index.  Make this
    ** fake index the first in a chain of Index objects with all of the real
    ** indices to follow */
    Index *pFirst;                  /* First of real indices on the table */
    memset(&sPk, 0, sizeof(Index));
    sPk.nKeyCol = 1;
    sPk.nColumn = 1;
    sPk.aiColumn = &aiColumnPk;
    sPk.aiRowLogEst = aiRowEstPk;
    sPk.onError = OE_Replace;
    sPk.pTable = pTab;
    sPk.szIdxRow = pTab->szTabRow;
    aiRowEstPk[0] = pTab->nRowLogEst;
    aiRowEstPk[1] = 0;
    pFirst = pSrc->pTab->pIndex;
    if( pSrc->notIndexed==0 ){
      /* The real indices of the table are only considered if the
      ** NOT INDEXED qualifier is omitted from the FROM clause */
      sPk.pNext = pFirst;
    }
    pProbe = &sPk;
  }
  rSize = pTab->nRowLogEst;
  rLogSize = estLog(rSize);

#ifndef SQLITE_OMIT_AUTOMATIC_INDEX
  /* Automatic indexes */
  if( !pBuilder->pOrSet
   && (pWInfo->wctrlFlags & WHERE_NO_AUTOINDEX)==0
   && (pWInfo->pParse->db->flags & SQLITE_AutoIndex)!=0
   && pSrc->pIndex==0
   && !pSrc->viaCoroutine
   && !pSrc->notIndexed
   && HasRowid(pTab)
   && !pSrc->isCorrelated
   && !pSrc->isRecursive
  ){
    /* Generate auto-index WhereLoops */
    WhereTerm *pTerm;
    WhereTerm *pWCEnd = pWC->a + pWC->nTerm;
    for(pTerm=pWC->a; rc==SQLITE_OK && pTerm<pWCEnd; pTerm++){
      if( pTerm->prereqRight & pNew->maskSelf ) continue;
      if( termCanDriveIndex(pTerm, pSrc, 0) ){
        pNew->u.btree.nEq = 1;
        pNew->nSkip = 0;
        pNew->u.btree.pIndex = 0;
        pNew->nLTerm = 1;
        pNew->aLTerm[0] = pTerm;
        /* TUNING: One-time cost for computing the automatic index is
        ** estimated to be X*N*log2(N) where N is the number of rows in
        ** the table being indexed and where X is 7 (LogEst=28) for normal
        ** tables or 1.375 (LogEst=4) for views and subqueries.  The value
        ** of X is smaller for views and subqueries so that the query planner
        ** will be more aggressive about generating automatic indexes for
        ** those objects, since there is no opportunity to add schema
        ** indexes on subqueries and views. */
        pNew->rSetup = rLogSize + rSize + 4;
        if( pTab->pSelect==0 && (pTab->tabFlags & TF_Ephemeral)==0 ){
          pNew->rSetup += 24;
        }
        ApplyCostMultiplier(pNew->rSetup, pTab->costMult);
        /* TUNING: Each index lookup yields 20 rows in the table.  This
        ** is more than the usual guess of 10 rows, since we have no way
        ** of knowing how selective the index will ultimately be.  It would
        ** not be unreasonable to make this value much larger. */
        pNew->nOut = 43;  assert( 43==sqlite3LogEst(20) );
        pNew->rRun = sqlite3LogEstAdd(rLogSize,pNew->nOut);
        pNew->wsFlags = WHERE_AUTO_INDEX;
        pNew->prereq = mExtra | pTerm->prereqRight;
        rc = whereLoopInsert(pBuilder, pNew);
      }
    }
  }
#endif /* SQLITE_OMIT_AUTOMATIC_INDEX */

  /* Loop over all indices
  */
  for(; rc==SQLITE_OK && pProbe; pProbe=pProbe->pNext, iSortIdx++){
    if( pProbe->pPartIdxWhere!=0
     && !whereUsablePartialIndex(pSrc->iCursor, pWC, pProbe->pPartIdxWhere) ){
      testcase( pNew->iTab!=pSrc->iCursor );  /* See ticket [98d973b8f5] */
      continue;  /* Partial index inappropriate for this query */
    }
    rSize = pProbe->aiRowLogEst[0];
    pNew->u.btree.nEq = 0;
    pNew->nSkip = 0;
    pNew->nLTerm = 0;
    pNew->iSortIdx = 0;
    pNew->rSetup = 0;
    pNew->prereq = mExtra;
    pNew->nOut = rSize;
    pNew->u.btree.pIndex = pProbe;
    b = indexMightHelpWithOrderBy(pBuilder, pProbe, pSrc->iCursor);
    /* The ONEPASS_DESIRED flags never occurs together with ORDER BY */
    assert( (pWInfo->wctrlFlags & WHERE_ONEPASS_DESIRED)==0 || b==0 );
    if( pProbe->tnum<=0 ){
      /* Integer primary key index */
      pNew->wsFlags = WHERE_IPK;

      /* Full table scan */
      pNew->iSortIdx = b ? iSortIdx : 0;
      /* TUNING: Cost of full table scan is (N*3.0). */
      pNew->rRun = rSize + 16;
      ApplyCostMultiplier(pNew->rRun, pTab->costMult);
      whereLoopOutputAdjust(pWC, pNew, rSize);
      rc = whereLoopInsert(pBuilder, pNew);
      pNew->nOut = rSize;
      if( rc ) break;
    }else{
      Bitmask m;
      if( pProbe->isCovering ){
        pNew->wsFlags = WHERE_IDX_ONLY | WHERE_INDEXED;
        m = 0;
      }else{
        m = pSrc->colUsed & ~columnsInIndex(pProbe);
        pNew->wsFlags = (m==0) ? (WHERE_IDX_ONLY|WHERE_INDEXED) : WHERE_INDEXED;
      }

      /* Full scan via index */
      if( b
       || !HasRowid(pTab)
       || ( m==0
         && pProbe->bUnordered==0
         && (pProbe->szIdxRow<pTab->szTabRow)
         && (pWInfo->wctrlFlags & WHERE_ONEPASS_DESIRED)==0
         && sqlite3GlobalConfig.bUseCis
         && OptimizationEnabled(pWInfo->pParse->db, SQLITE_CoverIdxScan)
          )
      ){
        pNew->iSortIdx = b ? iSortIdx : 0;

        /* The cost of visiting the index rows is N*K, where K is
        ** between 1.1 and 3.0, depending on the relative sizes of the
        ** index and table rows. If this is a non-covering index scan,
        ** also add the cost of visiting table rows (N*3.0).  */
        pNew->rRun = rSize + 1 + (15*pProbe->szIdxRow)/pTab->szTabRow;
        if( m!=0 ){
          pNew->rRun = sqlite3LogEstAdd(pNew->rRun, rSize+16);
        }
        ApplyCostMultiplier(pNew->rRun, pTab->costMult);
        whereLoopOutputAdjust(pWC, pNew, rSize);
        rc = whereLoopInsert(pBuilder, pNew);
        pNew->nOut = rSize;
        if( rc ) break;
      }
    }

    rc = whereLoopAddBtreeIndex(pBuilder, pSrc, pProbe, 0);
#ifdef SQLITE_ENABLE_STAT3_OR_STAT4
    sqlite3Stat4ProbeFree(pBuilder->pRec);
    pBuilder->nRecValid = 0;
    pBuilder->pRec = 0;
#endif

    /* If there was an INDEXED BY clause, then only that one index is
    ** considered. */
    if( pSrc->pIndex ) break;
  }
  return rc;
}

#ifndef SQLITE_OMIT_VIRTUALTABLE
/*
** Add all WhereLoop objects for a table of the join identified by
** pBuilder->pNew->iTab.  That table is guaranteed to be a virtual table.
*/
static int whereLoopAddVirtual(
  WhereLoopBuilder *pBuilder,  /* WHERE clause information */
  Bitmask mExtra
){
  WhereInfo *pWInfo;           /* WHERE analysis context */
  Parse *pParse;               /* The parsing context */
  WhereClause *pWC;            /* The WHERE clause */
  struct SrcList_item *pSrc;   /* The FROM clause term to search */
  Table *pTab;
  sqlite3 *db;
  sqlite3_index_info *pIdxInfo;
  struct sqlite3_index_constraint *pIdxCons;
  struct sqlite3_index_constraint_usage *pUsage;
  WhereTerm *pTerm;
  int i, j;
  int iTerm, mxTerm;
  int nConstraint;
  int seenIn = 0;              /* True if an IN operator is seen */
  int seenVar = 0;             /* True if a non-constant constraint is seen */
  int iPhase;                  /* 0: const w/o IN, 1: const, 2: no IN,  2: IN */
  WhereLoop *pNew;
  int rc = SQLITE_OK;

  pWInfo = pBuilder->pWInfo;
  pParse = pWInfo->pParse;
  db = pParse->db;
  pWC = pBuilder->pWC;
  pNew = pBuilder->pNew;
  pSrc = &pWInfo->pTabList->a[pNew->iTab];
  pTab = pSrc->pTab;
  assert( IsVirtual(pTab) );
  pIdxInfo = allocateIndexInfo(pParse, pWC, pSrc, pBuilder->pOrderBy);
  if( pIdxInfo==0 ) return SQLITE_NOMEM;
  pNew->prereq = 0;
  pNew->rSetup = 0;
  pNew->wsFlags = WHERE_VIRTUALTABLE;
  pNew->nLTerm = 0;
  pNew->u.vtab.needFree = 0;
  pUsage = pIdxInfo->aConstraintUsage;
  nConstraint = pIdxInfo->nConstraint;
  if( whereLoopResize(db, pNew, nConstraint) ){
    sqlite3DbFree(db, pIdxInfo);
    return SQLITE_NOMEM;
  }

  for(iPhase=0; iPhase<=3; iPhase++){
    if( !seenIn && (iPhase&1)!=0 ){
      iPhase++;
      if( iPhase>3 ) break;
    }
    if( !seenVar && iPhase>1 ) break;
    pIdxCons = *(struct sqlite3_index_constraint**)&pIdxInfo->aConstraint;
    for(i=0; i<pIdxInfo->nConstraint; i++, pIdxCons++){
      j = pIdxCons->iTermOffset;
      pTerm = &pWC->a[j];
      switch( iPhase ){
        case 0:    /* Constants without IN operator */
          pIdxCons->usable = 0;
          if( (pTerm->eOperator & WO_IN)!=0 ){
            seenIn = 1;
          }
          if( pTerm->prereqRight!=0 ){
            seenVar = 1;
          }else if( (pTerm->eOperator & WO_IN)==0 ){
            pIdxCons->usable = 1;
          }
          break;
        case 1:    /* Constants with IN operators */
          assert( seenIn );
          pIdxCons->usable = (pTerm->prereqRight==0);
          break;
        case 2:    /* Variables without IN */
          assert( seenVar );
          pIdxCons->usable = (pTerm->eOperator & WO_IN)==0;
          break;
        default:   /* Variables with IN */
          assert( seenVar && seenIn );
          pIdxCons->usable = 1;
          break;
      }
    }
    memset(pUsage, 0, sizeof(pUsage[0])*pIdxInfo->nConstraint);
    if( pIdxInfo->needToFreeIdxStr ) sqlite3_free(pIdxInfo->idxStr);
    pIdxInfo->idxStr = 0;
    pIdxInfo->idxNum = 0;
    pIdxInfo->needToFreeIdxStr = 0;
    pIdxInfo->orderByConsumed = 0;
    pIdxInfo->estimatedCost = SQLITE_BIG_DBL / (double)2;
    pIdxInfo->estimatedRows = 25;
    rc = vtabBestIndex(pParse, pTab, pIdxInfo);
    if( rc ) goto whereLoopAddVtab_exit;
    pIdxCons = *(struct sqlite3_index_constraint**)&pIdxInfo->aConstraint;
    pNew->prereq = mExtra;
    mxTerm = -1;
    assert( pNew->nLSlot>=nConstraint );
    for(i=0; i<nConstraint; i++) pNew->aLTerm[i] = 0;
    pNew->u.vtab.omitMask = 0;
    for(i=0; i<nConstraint; i++, pIdxCons++){
      if( (iTerm = pUsage[i].argvIndex - 1)>=0 ){
        j = pIdxCons->iTermOffset;
        if( iTerm>=nConstraint
         || j<0
         || j>=pWC->nTerm
         || pNew->aLTerm[iTerm]!=0
        ){
          rc = SQLITE_ERROR;
          sqlite3ErrorMsg(pParse, "%s.xBestIndex() malfunction", pTab->zName);
          goto whereLoopAddVtab_exit;
        }
        testcase( iTerm==nConstraint-1 );
        testcase( j==0 );
        testcase( j==pWC->nTerm-1 );
        pTerm = &pWC->a[j];
        pNew->prereq |= pTerm->prereqRight;
        assert( iTerm<pNew->nLSlot );
        pNew->aLTerm[iTerm] = pTerm;
        if( iTerm>mxTerm ) mxTerm = iTerm;
        testcase( iTerm==15 );
        testcase( iTerm==16 );
        if( iTerm<16 && pUsage[i].omit ) pNew->u.vtab.omitMask |= 1<<iTerm;
        if( (pTerm->eOperator & WO_IN)!=0 ){
          if( pUsage[i].omit==0 ){
            /* Do not attempt to use an IN constraint if the virtual table
            ** says that the equivalent EQ constraint cannot be safely omitted.
            ** If we do attempt to use such a constraint, some rows might be
            ** repeated in the output. */
            break;
          }
          /* A virtual table that is constrained by an IN clause may not
          ** consume the ORDER BY clause because (1) the order of IN terms
          ** is not necessarily related to the order of output terms and
          ** (2) Multiple outputs from a single IN value will not merge
          ** together.  */
          pIdxInfo->orderByConsumed = 0;
        }
      }
    }
    if( i>=nConstraint ){
      pNew->nLTerm = mxTerm+1;
      assert( pNew->nLTerm<=pNew->nLSlot );
      pNew->u.vtab.idxNum = pIdxInfo->idxNum;
      pNew->u.vtab.needFree = pIdxInfo->needToFreeIdxStr;
      pIdxInfo->needToFreeIdxStr = 0;
      pNew->u.vtab.idxStr = pIdxInfo->idxStr;
      pNew->u.vtab.isOrdered = (i8)(pIdxInfo->orderByConsumed ?
                                      pIdxInfo->nOrderBy : 0);
      pNew->rSetup = 0;
      pNew->rRun = sqlite3LogEstFromDouble(pIdxInfo->estimatedCost);
      pNew->nOut = sqlite3LogEst(pIdxInfo->estimatedRows);
      whereLoopInsert(pBuilder, pNew);
      if( pNew->u.vtab.needFree ){
        sqlite3_free(pNew->u.vtab.idxStr);
        pNew->u.vtab.needFree = 0;
      }
    }
  }  

whereLoopAddVtab_exit:
  if( pIdxInfo->needToFreeIdxStr ) sqlite3_free(pIdxInfo->idxStr);
  sqlite3DbFree(db, pIdxInfo);
  return rc;
}
#endif /* SQLITE_OMIT_VIRTUALTABLE */

/*
** Add WhereLoop entries to handle OR terms.  This works for either
** btrees or virtual tables.
*/
static int whereLoopAddOr(WhereLoopBuilder *pBuilder, Bitmask mExtra){
  WhereInfo *pWInfo = pBuilder->pWInfo;
  WhereClause *pWC;
  WhereLoop *pNew;
  WhereTerm *pTerm, *pWCEnd;
  int rc = SQLITE_OK;
  int iCur;
  WhereClause tempWC;
  WhereLoopBuilder sSubBuild;
  WhereOrSet sSum, sCur;
  struct SrcList_item *pItem;
  
  pWC = pBuilder->pWC;
  pWCEnd = pWC->a + pWC->nTerm;
  pNew = pBuilder->pNew;
  memset(&sSum, 0, sizeof(sSum));
  pItem = pWInfo->pTabList->a + pNew->iTab;
  iCur = pItem->iCursor;

  for(pTerm=pWC->a; pTerm<pWCEnd && rc==SQLITE_OK; pTerm++){
    if( (pTerm->eOperator & WO_OR)!=0
     && (pTerm->u.pOrInfo->indexable & pNew->maskSelf)!=0 
    ){
      WhereClause * const pOrWC = &pTerm->u.pOrInfo->wc;
      WhereTerm * const pOrWCEnd = &pOrWC->a[pOrWC->nTerm];
      WhereTerm *pOrTerm;
      int once = 1;
      int i, j;
    
      sSubBuild = *pBuilder;
      sSubBuild.pOrderBy = 0;
      sSubBuild.pOrSet = &sCur;

      WHERETRACE(0x200, ("Begin processing OR-clause %p\n", pTerm));
      for(pOrTerm=pOrWC->a; pOrTerm<pOrWCEnd; pOrTerm++){
        if( (pOrTerm->eOperator & WO_AND)!=0 ){
          sSubBuild.pWC = &pOrTerm->u.pAndInfo->wc;
        }else if( pOrTerm->leftCursor==iCur ){
          tempWC.pWInfo = pWC->pWInfo;
          tempWC.pOuter = pWC;
          tempWC.op = TK_AND;
          tempWC.nTerm = 1;
          tempWC.a = pOrTerm;
          sSubBuild.pWC = &tempWC;
        }else{
          continue;
        }
        sCur.n = 0;
#ifdef WHERETRACE_ENABLED
        WHERETRACE(0x200, ("OR-term %d of %p has %d subterms:\n", 
                   (int)(pOrTerm-pOrWC->a), pTerm, sSubBuild.pWC->nTerm));
        if( sqlite3WhereTrace & 0x400 ){
          for(i=0; i<sSubBuild.pWC->nTerm; i++){
            whereTermPrint(&sSubBuild.pWC->a[i], i);
          }
        }
#endif
#ifndef SQLITE_OMIT_VIRTUALTABLE
        if( IsVirtual(pItem->pTab) ){
          rc = whereLoopAddVirtual(&sSubBuild, mExtra);
        }else
#endif
        {
          rc = whereLoopAddBtree(&sSubBuild, mExtra);
        }
        if( rc==SQLITE_OK ){
          rc = whereLoopAddOr(&sSubBuild, mExtra);
        }
        assert( rc==SQLITE_OK || sCur.n==0 );
        if( sCur.n==0 ){
          sSum.n = 0;
          break;
        }else if( once ){
          whereOrMove(&sSum, &sCur);
          once = 0;
        }else{
          WhereOrSet sPrev;
          whereOrMove(&sPrev, &sSum);
          sSum.n = 0;
          for(i=0; i<sPrev.n; i++){
            for(j=0; j<sCur.n; j++){
              whereOrInsert(&sSum, sPrev.a[i].prereq | sCur.a[j].prereq,
                            sqlite3LogEstAdd(sPrev.a[i].rRun, sCur.a[j].rRun),
                            sqlite3LogEstAdd(sPrev.a[i].nOut, sCur.a[j].nOut));
            }
          }
        }
      }
      pNew->nLTerm = 1;
      pNew->aLTerm[0] = pTerm;
      pNew->wsFlags = WHERE_MULTI_OR;
      pNew->rSetup = 0;
      pNew->iSortIdx = 0;
      memset(&pNew->u, 0, sizeof(pNew->u));
      for(i=0; rc==SQLITE_OK && i<sSum.n; i++){
        /* TUNING: Currently sSum.a[i].rRun is set to the sum of the costs
        ** of all sub-scans required by the OR-scan. However, due to rounding
        ** errors, it may be that the cost of the OR-scan is equal to its
        ** most expensive sub-scan. Add the smallest possible penalty 
        ** (equivalent to multiplying the cost by 1.07) to ensure that 
        ** this does not happen. Otherwise, for WHERE clauses such as the
        ** following where there is an index on "y":
        **
        **     WHERE likelihood(x=?, 0.99) OR y=?
        **
        ** the planner may elect to "OR" together a full-table scan and an
        ** index lookup. And other similarly odd results.  */
        pNew->rRun = sSum.a[i].rRun + 1;
        pNew->nOut = sSum.a[i].nOut;
        pNew->prereq = sSum.a[i].prereq;
        rc = whereLoopInsert(pBuilder, pNew);
      }
      WHERETRACE(0x200, ("End processing OR-clause %p\n", pTerm));
    }
  }
  return rc;
}

/*
** Add all WhereLoop objects for all tables 
*/
static int whereLoopAddAll(WhereLoopBuilder *pBuilder){
  WhereInfo *pWInfo = pBuilder->pWInfo;
  Bitmask mExtra = 0;
  Bitmask mPrior = 0;
  int iTab;
  SrcList *pTabList = pWInfo->pTabList;
  struct SrcList_item *pItem;
  sqlite3 *db = pWInfo->pParse->db;
  int nTabList = pWInfo->nLevel;
  int rc = SQLITE_OK;
  u8 priorJoinType = 0;
  WhereLoop *pNew;

  /* Loop over the tables in the join, from left to right */
  pNew = pBuilder->pNew;
  whereLoopInit(pNew);
  for(iTab=0, pItem=pTabList->a; iTab<nTabList; iTab++, pItem++){
    pNew->iTab = iTab;
    pNew->maskSelf = getMask(&pWInfo->sMaskSet, pItem->iCursor);
    if( ((pItem->jointype|priorJoinType) & (JT_LEFT|JT_CROSS))!=0 ){
      mExtra = mPrior;
    }
    priorJoinType = pItem->jointype;
    if( IsVirtual(pItem->pTab) ){
      rc = whereLoopAddVirtual(pBuilder, mExtra);
    }else{
      rc = whereLoopAddBtree(pBuilder, mExtra);
    }
    if( rc==SQLITE_OK ){
      rc = whereLoopAddOr(pBuilder, mExtra);
    }
    mPrior |= pNew->maskSelf;
    if( rc || db->mallocFailed ) break;
  }
  whereLoopClear(db, pNew);
  return rc;
}

/*
** Examine a WherePath (with the addition of the extra WhereLoop of the 5th
** parameters) to see if it outputs rows in the requested ORDER BY
** (or GROUP BY) without requiring a separate sort operation.  Return N:
** 
**   N>0:   N terms of the ORDER BY clause are satisfied
**   N==0:  No terms of the ORDER BY clause are satisfied
**   N<0:   Unknown yet how many terms of ORDER BY might be satisfied.   
**
** Note that processing for WHERE_GROUPBY and WHERE_DISTINCTBY is not as
** strict.  With GROUP BY and DISTINCT the only requirement is that
** equivalent rows appear immediately adjacent to one another.  GROUP BY
** and DISTINCT do not require rows to appear in any particular order as long
** as equivalent rows are grouped together.  Thus for GROUP BY and DISTINCT
** the pOrderBy terms can be matched in any order.  With ORDER BY, the 
** pOrderBy terms must be matched in strict left-to-right order.
*/
static i8 wherePathSatisfiesOrderBy(
  WhereInfo *pWInfo,    /* The WHERE clause */
  ExprList *pOrderBy,   /* ORDER BY or GROUP BY or DISTINCT clause to check */
  WherePath *pPath,     /* The WherePath to check */
  u16 wctrlFlags,       /* Might contain WHERE_GROUPBY or WHERE_DISTINCTBY */
  u16 nLoop,            /* Number of entries in pPath->aLoop[] */
  WhereLoop *pLast,     /* Add this WhereLoop to the end of pPath->aLoop[] */
  Bitmask *pRevMask     /* OUT: Mask of WhereLoops to run in reverse order */
){
  u8 revSet;            /* True if rev is known */
  u8 rev;               /* Composite sort order */
  u8 revIdx;            /* Index sort order */
  u8 isOrderDistinct;   /* All prior WhereLoops are order-distinct */
  u8 distinctColumns;   /* True if the loop has UNIQUE NOT NULL columns */
  u8 isMatch;           /* iColumn matches a term of the ORDER BY clause */
  u16 nKeyCol;          /* Number of key columns in pIndex */
  u16 nColumn;          /* Total number of ordered columns in the index */
  u16 nOrderBy;         /* Number terms in the ORDER BY clause */
  int iLoop;            /* Index of WhereLoop in pPath being processed */
  int i, j;             /* Loop counters */
  int iCur;             /* Cursor number for current WhereLoop */
  int iColumn;          /* A column number within table iCur */
  WhereLoop *pLoop = 0; /* Current WhereLoop being processed. */
  WhereTerm *pTerm;     /* A single term of the WHERE clause */
  Expr *pOBExpr;        /* An expression from the ORDER BY clause */
  CollSeq *pColl;       /* COLLATE function from an ORDER BY clause term */
  Index *pIndex;        /* The index associated with pLoop */
  sqlite3 *db = pWInfo->pParse->db;  /* Database connection */
  Bitmask obSat = 0;    /* Mask of ORDER BY terms satisfied so far */
  Bitmask obDone;       /* Mask of all ORDER BY terms */
  Bitmask orderDistinctMask;  /* Mask of all well-ordered loops */
  Bitmask ready;              /* Mask of inner loops */

  /*
  ** We say the WhereLoop is "one-row" if it generates no more than one
  ** row of output.  A WhereLoop is one-row if all of the following are true:
  **  (a) All index columns match with WHERE_COLUMN_EQ.
  **  (b) The index is unique
  ** Any WhereLoop with an WHERE_COLUMN_EQ constraint on the rowid is one-row.
  ** Every one-row WhereLoop will have the WHERE_ONEROW bit set in wsFlags.
  **
  ** We say the WhereLoop is "order-distinct" if the set of columns from
  ** that WhereLoop that are in the ORDER BY clause are different for every
  ** row of the WhereLoop.  Every one-row WhereLoop is automatically
  ** order-distinct.   A WhereLoop that has no columns in the ORDER BY clause
  ** is not order-distinct. To be order-distinct is not quite the same as being
  ** UNIQUE since a UNIQUE column or index can have multiple rows that 
  ** are NULL and NULL values are equivalent for the purpose of order-distinct.
  ** To be order-distinct, the columns must be UNIQUE and NOT NULL.
  **
  ** The rowid for a table is always UNIQUE and NOT NULL so whenever the
  ** rowid appears in the ORDER BY clause, the corresponding WhereLoop is
  ** automatically order-distinct.
  */

  assert( pOrderBy!=0 );
  if( nLoop && OptimizationDisabled(db, SQLITE_OrderByIdxJoin) ) return 0;

  nOrderBy = pOrderBy->nExpr;
  testcase( nOrderBy==BMS-1 );
  if( nOrderBy>BMS-1 ) return 0;  /* Cannot optimize overly large ORDER BYs */
  isOrderDistinct = 1;
  obDone = MASKBIT(nOrderBy)-1;
  orderDistinctMask = 0;
  ready = 0;
  for(iLoop=0; isOrderDistinct && obSat<obDone && iLoop<=nLoop; iLoop++){
    if( iLoop>0 ) ready |= pLoop->maskSelf;
    pLoop = iLoop<nLoop ? pPath->aLoop[iLoop] : pLast;
    if( pLoop->wsFlags & WHERE_VIRTUALTABLE ){
      if( pLoop->u.vtab.isOrdered ) obSat = obDone;
      break;
    }
    iCur = pWInfo->pTabList->a[pLoop->iTab].iCursor;

    /* Mark off any ORDER BY term X that is a column in the table of
    ** the current loop for which there is term in the WHERE
    ** clause of the form X IS NULL or X=? that reference only outer
    ** loops.
    */
    for(i=0; i<nOrderBy; i++){
      if( MASKBIT(i) & obSat ) continue;
      pOBExpr = sqlite3ExprSkipCollate(pOrderBy->a[i].pExpr);
      if( pOBExpr->op!=TK_COLUMN ) continue;
      if( pOBExpr->iTable!=iCur ) continue;
      pTerm = findTerm(&pWInfo->sWC, iCur, pOBExpr->iColumn,
                       ~ready, WO_EQ|WO_ISNULL, 0);
      if( pTerm==0 ) continue;
      if( (pTerm->eOperator&WO_EQ)!=0 && pOBExpr->iColumn>=0 ){
        const char *z1, *z2;
        pColl = sqlite3ExprCollSeq(pWInfo->pParse, pOrderBy->a[i].pExpr);
        if( !pColl ) pColl = db->pDfltColl;
        z1 = pColl->zName;
        pColl = sqlite3ExprCollSeq(pWInfo->pParse, pTerm->pExpr);
        if( !pColl ) pColl = db->pDfltColl;
        z2 = pColl->zName;
        if( sqlite3StrICmp(z1, z2)!=0 ) continue;
      }
      obSat |= MASKBIT(i);
    }

    if( (pLoop->wsFlags & WHERE_ONEROW)==0 ){
      if( pLoop->wsFlags & WHERE_IPK ){
        pIndex = 0;
        nKeyCol = 0;
        nColumn = 1;
      }else if( (pIndex = pLoop->u.btree.pIndex)==0 || pIndex->bUnordered ){
        return 0;
      }else{
        nKeyCol = pIndex->nKeyCol;
        nColumn = pIndex->nColumn;
        assert( nColumn==nKeyCol+1 || !HasRowid(pIndex->pTable) );
        assert( pIndex->aiColumn[nColumn-1]==(-1) || !HasRowid(pIndex->pTable));
        isOrderDistinct = IsUniqueIndex(pIndex);
      }

      /* Loop through all columns of the index and deal with the ones
      ** that are not constrained by == or IN.
      */
      rev = revSet = 0;
      distinctColumns = 0;
      for(j=0; j<nColumn; j++){
        u8 bOnce;   /* True to run the ORDER BY search loop */

        /* Skip over == and IS NULL terms */
        if( j<pLoop->u.btree.nEq
         && pLoop->nSkip==0
         && ((i = pLoop->aLTerm[j]->eOperator) & (WO_EQ|WO_ISNULL))!=0
        ){
          if( i & WO_ISNULL ){
            testcase( isOrderDistinct );
            isOrderDistinct = 0;
          }
          continue;  
        }

        /* Get the column number in the table (iColumn) and sort order
        ** (revIdx) for the j-th column of the index.
        */
        if( pIndex ){
          iColumn = pIndex->aiColumn[j];
          revIdx = pIndex->aSortOrder[j];
          if( iColumn==pIndex->pTable->iPKey ) iColumn = -1;
        }else{
          iColumn = -1;
          revIdx = 0;
        }

        /* An unconstrained column that might be NULL means that this
        ** WhereLoop is not well-ordered
        */
        if( isOrderDistinct
         && iColumn>=0
         && j>=pLoop->u.btree.nEq
         && pIndex->pTable->aCol[iColumn].notNull==0
        ){
          isOrderDistinct = 0;
        }

        /* Find the ORDER BY term that corresponds to the j-th column
        ** of the index and mark that ORDER BY term off 
        */
        bOnce = 1;
        isMatch = 0;
        for(i=0; bOnce && i<nOrderBy; i++){
          if( MASKBIT(i) & obSat ) continue;
          pOBExpr = sqlite3ExprSkipCollate(pOrderBy->a[i].pExpr);
          testcase( wctrlFlags & WHERE_GROUPBY );
          testcase( wctrlFlags & WHERE_DISTINCTBY );
          if( (wctrlFlags & (WHERE_GROUPBY|WHERE_DISTINCTBY))==0 ) bOnce = 0;
          if( pOBExpr->op!=TK_COLUMN ) continue;
          if( pOBExpr->iTable!=iCur ) continue;
          if( pOBExpr->iColumn!=iColumn ) continue;
          if( iColumn>=0 ){
            pColl = sqlite3ExprCollSeq(pWInfo->pParse, pOrderBy->a[i].pExpr);
            if( !pColl ) pColl = db->pDfltColl;
            if( sqlite3StrICmp(pColl->zName, pIndex->azColl[j])!=0 ) continue;
          }
          isMatch = 1;
          break;
        }
        if( isMatch && (wctrlFlags & WHERE_GROUPBY)==0 ){
          /* Make sure the sort order is compatible in an ORDER BY clause.
          ** Sort order is irrelevant for a GROUP BY clause. */
          if( revSet ){
            if( (rev ^ revIdx)!=pOrderBy->a[i].sortOrder ) isMatch = 0;
          }else{
            rev = revIdx ^ pOrderBy->a[i].sortOrder;
            if( rev ) *pRevMask |= MASKBIT(iLoop);
            revSet = 1;
          }
        }
        if( isMatch ){
          if( iColumn<0 ){
            testcase( distinctColumns==0 );
            distinctColumns = 1;
          }
          obSat |= MASKBIT(i);
        }else{
          /* No match found */
          if( j==0 || j<nKeyCol ){
            testcase( isOrderDistinct!=0 );
            isOrderDistinct = 0;
          }
          break;
        }
      } /* end Loop over all index columns */
      if( distinctColumns ){
        testcase( isOrderDistinct==0 );
        isOrderDistinct = 1;
      }
    } /* end-if not one-row */

    /* Mark off any other ORDER BY terms that reference pLoop */
    if( isOrderDistinct ){
      orderDistinctMask |= pLoop->maskSelf;
      for(i=0; i<nOrderBy; i++){
        Expr *p;
        Bitmask mTerm;
        if( MASKBIT(i) & obSat ) continue;
        p = pOrderBy->a[i].pExpr;
        mTerm = exprTableUsage(&pWInfo->sMaskSet,p);
        if( mTerm==0 && !sqlite3ExprIsConstant(p) ) continue;
        if( (mTerm&~orderDistinctMask)==0 ){
          obSat |= MASKBIT(i);
        }
      }
    }
  } /* End the loop over all WhereLoops from outer-most down to inner-most */
  if( obSat==obDone ) return (i8)nOrderBy;
  if( !isOrderDistinct ){
    for(i=nOrderBy-1; i>0; i--){
      Bitmask m = MASKBIT(i) - 1;
      if( (obSat&m)==m ) return i;
    }
    return 0;
  }
  return -1;
}


/*
** If the WHERE_GROUPBY flag is set in the mask passed to sqlite3WhereBegin(),
** the planner assumes that the specified pOrderBy list is actually a GROUP
** BY clause - and so any order that groups rows as required satisfies the
** request.
**
** Normally, in this case it is not possible for the caller to determine
** whether or not the rows are really being delivered in sorted order, or
** just in some other order that provides the required grouping. However,
** if the WHERE_SORTBYGROUP flag is also passed to sqlite3WhereBegin(), then
** this function may be called on the returned WhereInfo object. It returns
** true if the rows really will be sorted in the specified order, or false
** otherwise.
**
** For example, assuming:
**
**   CREATE INDEX i1 ON t1(x, Y);
**
** then
**
**   SELECT * FROM t1 GROUP BY x,y ORDER BY x,y;   -- IsSorted()==1
**   SELECT * FROM t1 GROUP BY y,x ORDER BY y,x;   -- IsSorted()==0
*/
int sqlite3WhereIsSorted(WhereInfo *pWInfo){
  assert( pWInfo->wctrlFlags & WHERE_GROUPBY );
  assert( pWInfo->wctrlFlags & WHERE_SORTBYGROUP );
  return pWInfo->sorted;
}

#ifdef WHERETRACE_ENABLED
/* For debugging use only: */
static const char *wherePathName(WherePath *pPath, int nLoop, WhereLoop *pLast){
  static char zName[65];
  int i;
  for(i=0; i<nLoop; i++){ zName[i] = pPath->aLoop[i]->cId; }
  if( pLast ) zName[i++] = pLast->cId;
  zName[i] = 0;
  return zName;
}
#endif

/*
** Return the cost of sorting nRow rows, assuming that the keys have 
** nOrderby columns and that the first nSorted columns are already in
** order.
*/
static LogEst whereSortingCost(
  WhereInfo *pWInfo,
  LogEst nRow,
  int nOrderBy,
  int nSorted
){
  /* TUNING: Estimated cost of a full external sort, where N is 
  ** the number of rows to sort is:
  **
  **   cost = (3.0 * N * log(N)).
  ** 
  ** Or, if the order-by clause has X terms but only the last Y 
  ** terms are out of order, then block-sorting will reduce the 
  ** sorting cost to:
  **
  **   cost = (3.0 * N * log(N)) * (Y/X)
  **
  ** The (Y/X) term is implemented using stack variable rScale
  ** below.  */
  LogEst rScale, rSortCost;
  assert( nOrderBy>0 && 66==sqlite3LogEst(100) );
  rScale = sqlite3LogEst((nOrderBy-nSorted)*100/nOrderBy) - 66;
  rSortCost = nRow + estLog(nRow) + rScale + 16;

  /* TUNING: The cost of implementing DISTINCT using a B-TREE is
  ** similar but with a larger constant of proportionality. 
  ** Multiply by an additional factor of 3.0.  */
  if( pWInfo->wctrlFlags & WHERE_WANT_DISTINCT ){
    rSortCost += 16;
  }

  return rSortCost;
}

/*
** Given the list of WhereLoop objects at pWInfo->pLoops, this routine
** attempts to find the lowest cost path that visits each WhereLoop
** once.  This path is then loaded into the pWInfo->a[].pWLoop fields.
**
** Assume that the total number of output rows that will need to be sorted
** will be nRowEst (in the 10*log2 representation).  Or, ignore sorting
** costs if nRowEst==0.
**
** Return SQLITE_OK on success or SQLITE_NOMEM of a memory allocation
** error occurs.
*/
static int wherePathSolver(WhereInfo *pWInfo, LogEst nRowEst){
  int mxChoice;             /* Maximum number of simultaneous paths tracked */
  int nLoop;                /* Number of terms in the join */
  Parse *pParse;            /* Parsing context */
  sqlite3 *db;              /* The database connection */
  int iLoop;                /* Loop counter over the terms of the join */
  int ii, jj;               /* Loop counters */
  int mxI = 0;              /* Index of next entry to replace */
  int nOrderBy;             /* Number of ORDER BY clause terms */
  LogEst mxCost = 0;        /* Maximum cost of a set of paths */
  LogEst mxUnsorted = 0;    /* Maximum unsorted cost of a set of path */
  int nTo, nFrom;           /* Number of valid entries in aTo[] and aFrom[] */
  WherePath *aFrom;         /* All nFrom paths at the previous level */
  WherePath *aTo;           /* The nTo best paths at the current level */
  WherePath *pFrom;         /* An element of aFrom[] that we are working on */
  WherePath *pTo;           /* An element of aTo[] that we are working on */
  WhereLoop *pWLoop;        /* One of the WhereLoop objects */
  WhereLoop **pX;           /* Used to divy up the pSpace memory */
  LogEst *aSortCost = 0;    /* Sorting and partial sorting costs */
  char *pSpace;             /* Temporary memory used by this routine */
  int nSpace;               /* Bytes of space allocated at pSpace */

  pParse = pWInfo->pParse;
  db = pParse->db;
  nLoop = pWInfo->nLevel;
  /* TUNING: For simple queries, only the best path is tracked.
  ** For 2-way joins, the 5 best paths are followed.
  ** For joins of 3 or more tables, track the 10 best paths */
  mxChoice = (nLoop<=1) ? 1 : (nLoop==2 ? 5 : 10);
  assert( nLoop<=pWInfo->pTabList->nSrc );
  WHERETRACE(0x002, ("---- begin solver.  (nRowEst=%d)\n", nRowEst));

  /* If nRowEst is zero and there is an ORDER BY clause, ignore it. In this
  ** case the purpose of this call is to estimate the number of rows returned
  ** by the overall query. Once this estimate has been obtained, the caller
  ** will invoke this function a second time, passing the estimate as the
  ** nRowEst parameter.  */
  if( pWInfo->pOrderBy==0 || nRowEst==0 ){
    nOrderBy = 0;
  }else{
    nOrderBy = pWInfo->pOrderBy->nExpr;
  }

  /* Allocate and initialize space for aTo, aFrom and aSortCost[] */
  nSpace = (sizeof(WherePath)+sizeof(WhereLoop*)*nLoop)*mxChoice*2;
  nSpace += sizeof(LogEst) * nOrderBy;
  pSpace = sqlite3DbMallocRaw(db, nSpace);
  if( pSpace==0 ) return SQLITE_NOMEM;
  aTo = (WherePath*)pSpace;
  aFrom = aTo+mxChoice;
  memset(aFrom, 0, sizeof(aFrom[0]));
  pX = (WhereLoop**)(aFrom+mxChoice);
  for(ii=mxChoice*2, pFrom=aTo; ii>0; ii--, pFrom++, pX += nLoop){
    pFrom->aLoop = pX;
  }
  if( nOrderBy ){
    /* If there is an ORDER BY clause and it is not being ignored, set up
    ** space for the aSortCost[] array. Each element of the aSortCost array
    ** is either zero - meaning it has not yet been initialized - or the
    ** cost of sorting nRowEst rows of data where the first X terms of
    ** the ORDER BY clause are already in order, where X is the array 
    ** index.  */
    aSortCost = (LogEst*)pX;
    memset(aSortCost, 0, sizeof(LogEst) * nOrderBy);
  }
  assert( aSortCost==0 || &pSpace[nSpace]==(char*)&aSortCost[nOrderBy] );
  assert( aSortCost!=0 || &pSpace[nSpace]==(char*)pX );

  /* Seed the search with a single WherePath containing zero WhereLoops.
  **
  ** TUNING: Do not let the number of iterations go above 28.  If the cost
  ** of computing an automatic index is not paid back within the first 28
  ** rows, then do not use the automatic index. */
  aFrom[0].nRow = MIN(pParse->nQueryLoop, 48);  assert( 48==sqlite3LogEst(28) );
  nFrom = 1;
  assert( aFrom[0].isOrdered==0 );
  if( nOrderBy ){
    /* If nLoop is zero, then there are no FROM terms in the query. Since
    ** in this case the query may return a maximum of one row, the results
    ** are already in the requested order. Set isOrdered to nOrderBy to
    ** indicate this. Or, if nLoop is greater than zero, set isOrdered to
    ** -1, indicating that the result set may or may not be ordered, 
    ** depending on the loops added to the current plan.  */
    aFrom[0].isOrdered = nLoop>0 ? -1 : nOrderBy;
  }

  /* Compute successively longer WherePaths using the previous generation
  ** of WherePaths as the basis for the next.  Keep track of the mxChoice
  ** best paths at each generation */
  for(iLoop=0; iLoop<nLoop; iLoop++){
    nTo = 0;
    for(ii=0, pFrom=aFrom; ii<nFrom; ii++, pFrom++){
      for(pWLoop=pWInfo->pLoops; pWLoop; pWLoop=pWLoop->pNextLoop){
        LogEst nOut;                      /* Rows visited by (pFrom+pWLoop) */
        LogEst rCost;                     /* Cost of path (pFrom+pWLoop) */
        LogEst rUnsorted;                 /* Unsorted cost of (pFrom+pWLoop) */
        i8 isOrdered = pFrom->isOrdered;  /* isOrdered for (pFrom+pWLoop) */
        Bitmask maskNew;                  /* Mask of src visited by (..) */
        Bitmask revMask = 0;              /* Mask of rev-order loops for (..) */

        if( (pWLoop->prereq & ~pFrom->maskLoop)!=0 ) continue;
        if( (pWLoop->maskSelf & pFrom->maskLoop)!=0 ) continue;
        /* At this point, pWLoop is a candidate to be the next loop. 
        ** Compute its cost */
        rUnsorted = sqlite3LogEstAdd(pWLoop->rSetup,pWLoop->rRun + pFrom->nRow);
        rUnsorted = sqlite3LogEstAdd(rUnsorted, pFrom->rUnsorted);
        nOut = pFrom->nRow + pWLoop->nOut;
        maskNew = pFrom->maskLoop | pWLoop->maskSelf;
        if( isOrdered<0 ){
          isOrdered = wherePathSatisfiesOrderBy(pWInfo,
                       pWInfo->pOrderBy, pFrom, pWInfo->wctrlFlags,
                       iLoop, pWLoop, &revMask);
        }else{
          revMask = pFrom->revLoop;
        }
        if( isOrdered>=0 && isOrdered<nOrderBy ){
          if( aSortCost[isOrdered]==0 ){
            aSortCost[isOrdered] = whereSortingCost(
                pWInfo, nRowEst, nOrderBy, isOrdered
            );
          }
          rCost = sqlite3LogEstAdd(rUnsorted, aSortCost[isOrdered]);

          WHERETRACE(0x002,
              ("---- sort cost=%-3d (%d/%d) increases cost %3d to %-3d\n",
               aSortCost[isOrdered], (nOrderBy-isOrdered), nOrderBy, 
               rUnsorted, rCost));
        }else{
          rCost = rUnsorted;
        }

        /* Check to see if pWLoop should be added to the set of
        ** mxChoice best-so-far paths.
        **
        ** First look for an existing path among best-so-far paths
        ** that covers the same set of loops and has the same isOrdered
        ** setting as the current path candidate.
        **
        ** The term "((pTo->isOrdered^isOrdered)&0x80)==0" is equivalent
        ** to (pTo->isOrdered==(-1))==(isOrdered==(-1))" for the range
        ** of legal values for isOrdered, -1..64.
        */
        for(jj=0, pTo=aTo; jj<nTo; jj++, pTo++){
          if( pTo->maskLoop==maskNew
           && ((pTo->isOrdered^isOrdered)&0x80)==0
          ){
            testcase( jj==nTo-1 );
            break;
          }
        }
        if( jj>=nTo ){
          /* None of the existing best-so-far paths match the candidate. */
          if( nTo>=mxChoice
           && (rCost>mxCost || (rCost==mxCost && rUnsorted>=mxUnsorted))
          ){
            /* The current candidate is no better than any of the mxChoice
            ** paths currently in the best-so-far buffer.  So discard
            ** this candidate as not viable. */
#ifdef WHERETRACE_ENABLED /* 0x4 */
            if( sqlite3WhereTrace&0x4 ){
              sqlite3DebugPrintf("Skip   %s cost=%-3d,%3d order=%c\n",
                  wherePathName(pFrom, iLoop, pWLoop), rCost, nOut,
                  isOrdered>=0 ? isOrdered+'0' : '?');
            }
#endif
            continue;
          }
          /* If we reach this points it means that the new candidate path
          ** needs to be added to the set of best-so-far paths. */
          if( nTo<mxChoice ){
            /* Increase the size of the aTo set by one */
            jj = nTo++;
          }else{
            /* New path replaces the prior worst to keep count below mxChoice */
            jj = mxI;
          }
          pTo = &aTo[jj];
#ifdef WHERETRACE_ENABLED /* 0x4 */
          if( sqlite3WhereTrace&0x4 ){
            sqlite3DebugPrintf("New    %s cost=%-3d,%3d order=%c\n",
                wherePathName(pFrom, iLoop, pWLoop), rCost, nOut,
                isOrdered>=0 ? isOrdered+'0' : '?');
          }
#endif
        }else{
          /* Control reaches here if best-so-far path pTo=aTo[jj] covers the
          ** same set of loops and has the sam isOrdered setting as the
          ** candidate path.  Check to see if the candidate should replace
          ** pTo or if the candidate should be skipped */
          if( pTo->rCost<rCost || (pTo->rCost==rCost && pTo->nRow<=nOut) ){
#ifdef WHERETRACE_ENABLED /* 0x4 */
            if( sqlite3WhereTrace&0x4 ){
              sqlite3DebugPrintf(
                  "Skip   %s cost=%-3d,%3d order=%c",
                  wherePathName(pFrom, iLoop, pWLoop), rCost, nOut,
                  isOrdered>=0 ? isOrdered+'0' : '?');
              sqlite3DebugPrintf("   vs %s cost=%-3d,%d order=%c\n",
                  wherePathName(pTo, iLoop+1, 0), pTo->rCost, pTo->nRow,
                  pTo->isOrdered>=0 ? pTo->isOrdered+'0' : '?');
            }
#endif
            /* Discard the candidate path from further consideration */
            testcase( pTo->rCost==rCost );
            continue;
          }
          testcase( pTo->rCost==rCost+1 );
          /* Control reaches here if the candidate path is better than the
          ** pTo path.  Replace pTo with the candidate. */
#ifdef WHERETRACE_ENABLED /* 0x4 */
          if( sqlite3WhereTrace&0x4 ){
            sqlite3DebugPrintf(
                "Update %s cost=%-3d,%3d order=%c",
                wherePathName(pFrom, iLoop, pWLoop), rCost, nOut,
                isOrdered>=0 ? isOrdered+'0' : '?');
            sqlite3DebugPrintf("  was %s cost=%-3d,%3d order=%c\n",
                wherePathName(pTo, iLoop+1, 0), pTo->rCost, pTo->nRow,
                pTo->isOrdered>=0 ? pTo->isOrdered+'0' : '?');
          }
#endif
        }
        /* pWLoop is a winner.  Add it to the set of best so far */
        pTo->maskLoop = pFrom->maskLoop | pWLoop->maskSelf;
        pTo->revLoop = revMask;
        pTo->nRow = nOut;
        pTo->rCost = rCost;
        pTo->rUnsorted = rUnsorted;
        pTo->isOrdered = isOrdered;
        memcpy(pTo->aLoop, pFrom->aLoop, sizeof(WhereLoop*)*iLoop);
        pTo->aLoop[iLoop] = pWLoop;
        if( nTo>=mxChoice ){
          mxI = 0;
          mxCost = aTo[0].rCost;
          mxUnsorted = aTo[0].nRow;
          for(jj=1, pTo=&aTo[1]; jj<mxChoice; jj++, pTo++){
            if( pTo->rCost>mxCost 
             || (pTo->rCost==mxCost && pTo->rUnsorted>mxUnsorted) 
            ){
              mxCost = pTo->rCost;
              mxUnsorted = pTo->rUnsorted;
              mxI = jj;
            }
          }
        }
      }
    }

#ifdef WHERETRACE_ENABLED  /* >=2 */
    if( sqlite3WhereTrace & 0x02 ){
      sqlite3DebugPrintf("---- after round %d ----\n", iLoop);
      for(ii=0, pTo=aTo; ii<nTo; ii++, pTo++){
        sqlite3DebugPrintf(" %s cost=%-3d nrow=%-3d order=%c",
           wherePathName(pTo, iLoop+1, 0), pTo->rCost, pTo->nRow,
           pTo->isOrdered>=0 ? (pTo->isOrdered+'0') : '?');
        if( pTo->isOrdered>0 ){
          sqlite3DebugPrintf(" rev=0x%llx\n", pTo->revLoop);
        }else{
          sqlite3DebugPrintf("\n");
        }
      }
    }
#endif

    /* Swap the roles of aFrom and aTo for the next generation */
    pFrom = aTo;
    aTo = aFrom;
    aFrom = pFrom;
    nFrom = nTo;
  }

  if( nFrom==0 ){
    sqlite3ErrorMsg(pParse, "no query solution");
    sqlite3DbFree(db, pSpace);
    return SQLITE_ERROR;
  }
  
  /* Find the lowest cost path.  pFrom will be left pointing to that path */
  pFrom = aFrom;
  for(ii=1; ii<nFrom; ii++){
    if( pFrom->rCost>aFrom[ii].rCost ) pFrom = &aFrom[ii];
  }
  assert( pWInfo->nLevel==nLoop );
  /* Load the lowest cost path into pWInfo */
  for(iLoop=0; iLoop<nLoop; iLoop++){
    WhereLevel *pLevel = pWInfo->a + iLoop;
    pLevel->pWLoop = pWLoop = pFrom->aLoop[iLoop];
    pLevel->iFrom = pWLoop->iTab;
    pLevel->iTabCur = pWInfo->pTabList->a[pLevel->iFrom].iCursor;
  }
  if( (pWInfo->wctrlFlags & WHERE_WANT_DISTINCT)!=0
   && (pWInfo->wctrlFlags & WHERE_DISTINCTBY)==0
   && pWInfo->eDistinct==WHERE_DISTINCT_NOOP
   && nRowEst
  ){
    Bitmask notUsed;
    int rc = wherePathSatisfiesOrderBy(pWInfo, pWInfo->pResultSet, pFrom,
                 WHERE_DISTINCTBY, nLoop-1, pFrom->aLoop[nLoop-1], &notUsed);
    if( rc==pWInfo->pResultSet->nExpr ){
      pWInfo->eDistinct = WHERE_DISTINCT_ORDERED;
    }
  }
  if( pWInfo->pOrderBy ){
    if( pWInfo->wctrlFlags & WHERE_DISTINCTBY ){
      if( pFrom->isOrdered==pWInfo->pOrderBy->nExpr ){
        pWInfo->eDistinct = WHERE_DISTINCT_ORDERED;
      }
    }else{
      pWInfo->nOBSat = pFrom->isOrdered;
      if( pWInfo->nOBSat<0 ) pWInfo->nOBSat = 0;
      pWInfo->revMask = pFrom->revLoop;
    }
    if( (pWInfo->wctrlFlags & WHERE_SORTBYGROUP)
        && pWInfo->nOBSat==pWInfo->pOrderBy->nExpr
    ){
      Bitmask revMask = 0;
      int nOrder = wherePathSatisfiesOrderBy(pWInfo, pWInfo->pOrderBy, 
          pFrom, 0, nLoop-1, pFrom->aLoop[nLoop-1], &revMask
      );
      assert( pWInfo->sorted==0 );
      if( nOrder==pWInfo->pOrderBy->nExpr ){
        pWInfo->sorted = 1;
        pWInfo->revMask = revMask;
      }
    }
  }


  pWInfo->nRowOut = pFrom->nRow;

  /* Free temporary memory and return success */
  sqlite3DbFree(db, pSpace);
  return SQLITE_OK;
}

/*
** Most queries use only a single table (they are not joins) and have
** simple == constraints against indexed fields.  This routine attempts
** to plan those simple cases using much less ceremony than the
** general-purpose query planner, and thereby yield faster sqlite3_prepare()
** times for the common case.
**
** Return non-zero on success, if this query can be handled by this
** no-frills query planner.  Return zero if this query needs the 
** general-purpose query planner.
*/
static int whereShortCut(WhereLoopBuilder *pBuilder){
  WhereInfo *pWInfo;
  struct SrcList_item *pItem;
  WhereClause *pWC;
  WhereTerm *pTerm;
  WhereLoop *pLoop;
  int iCur;
  int j;
  Table *pTab;
  Index *pIdx;
  
  pWInfo = pBuilder->pWInfo;
  if( pWInfo->wctrlFlags & WHERE_FORCE_TABLE ) return 0;
  assert( pWInfo->pTabList->nSrc>=1 );
  pItem = pWInfo->pTabList->a;
  pTab = pItem->pTab;
  if( IsVirtual(pTab) ) return 0;
  if( pItem->zIndex ) return 0;
  iCur = pItem->iCursor;
  pWC = &pWInfo->sWC;
  pLoop = pBuilder->pNew;
  pLoop->wsFlags = 0;
  pLoop->nSkip = 0;
  pTerm = findTerm(pWC, iCur, -1, 0, WO_EQ, 0);
  if( pTerm ){
    pLoop->wsFlags = WHERE_COLUMN_EQ|WHERE_IPK|WHERE_ONEROW;
    pLoop->aLTerm[0] = pTerm;
    pLoop->nLTerm = 1;
    pLoop->u.btree.nEq = 1;
    /* TUNING: Cost of a rowid lookup is 10 */
    pLoop->rRun = 33;  /* 33==sqlite3LogEst(10) */
  }else{
    for(pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext){
      assert( pLoop->aLTermSpace==pLoop->aLTerm );
      if( !IsUniqueIndex(pIdx)
       || pIdx->pPartIdxWhere!=0 
       || pIdx->nKeyCol>ArraySize(pLoop->aLTermSpace) 
      ) continue;
      for(j=0; j<pIdx->nKeyCol; j++){
        pTerm = findTerm(pWC, iCur, pIdx->aiColumn[j], 0, WO_EQ, pIdx);
        if( pTerm==0 ) break;
        pLoop->aLTerm[j] = pTerm;
      }
      if( j!=pIdx->nKeyCol ) continue;
      pLoop->wsFlags = WHERE_COLUMN_EQ|WHERE_ONEROW|WHERE_INDEXED;
      if( pIdx->isCovering || (pItem->colUsed & ~columnsInIndex(pIdx))==0 ){
        pLoop->wsFlags |= WHERE_IDX_ONLY;
      }
      pLoop->nLTerm = j;
      pLoop->u.btree.nEq = j;
      pLoop->u.btree.pIndex = pIdx;
      /* TUNING: Cost of a unique index lookup is 15 */
      pLoop->rRun = 39;  /* 39==sqlite3LogEst(15) */
      break;
    }
  }
  if( pLoop->wsFlags ){
    pLoop->nOut = (LogEst)1;
    pWInfo->a[0].pWLoop = pLoop;
    pLoop->maskSelf = getMask(&pWInfo->sMaskSet, iCur);
    pWInfo->a[0].iTabCur = iCur;
    pWInfo->nRowOut = 1;
    if( pWInfo->pOrderBy ) pWInfo->nOBSat =  pWInfo->pOrderBy->nExpr;
    if( pWInfo->wctrlFlags & WHERE_WANT_DISTINCT ){
      pWInfo->eDistinct = WHERE_DISTINCT_UNIQUE;
    }
#ifdef SQLITE_DEBUG
    pLoop->cId = '0';
#endif
    return 1;
  }
  return 0;
}

/*
** Generate the beginning of the loop used for WHERE clause processing.
** The return value is a pointer to an opaque structure that contains
** information needed to terminate the loop.  Later, the calling routine
** should invoke sqlite3WhereEnd() with the return value of this function
** in order to complete the WHERE clause processing.
**
** If an error occurs, this routine returns NULL.
**
** The basic idea is to do a nested loop, one loop for each table in
** the FROM clause of a select.  (INSERT and UPDATE statements are the
** same as a SELECT with only a single table in the FROM clause.)  For
** example, if the SQL is this:
**
**       SELECT * FROM t1, t2, t3 WHERE ...;
**
** Then the code generated is conceptually like the following:
**
**      foreach row1 in t1 do       \    Code generated
**        foreach row2 in t2 do      |-- by sqlite3WhereBegin()
**          foreach row3 in t3 do   /
**            ...
**          end                     \    Code generated
**        end                        |-- by sqlite3WhereEnd()
**      end                         /
**
** Note that the loops might not be nested in the order in which they
** appear in the FROM clause if a different order is better able to make
** use of indices.  Note also that when the IN operator appears in
** the WHERE clause, it might result in additional nested loops for
** scanning through all values on the right-hand side of the IN.
**
** There are Btree cursors associated with each table.  t1 uses cursor
** number pTabList->a[0].iCursor.  t2 uses the cursor pTabList->a[1].iCursor.
** And so forth.  This routine generates code to open those VDBE cursors
** and sqlite3WhereEnd() generates the code to close them.
**
** The code that sqlite3WhereBegin() generates leaves the cursors named
** in pTabList pointing at their appropriate entries.  The [...] code
** can use OP_Column and OP_Rowid opcodes on these cursors to extract
** data from the various tables of the loop.
**
** If the WHERE clause is empty, the foreach loops must each scan their
** entire tables.  Thus a three-way join is an O(N^3) operation.  But if
** the tables have indices and there are terms in the WHERE clause that
** refer to those indices, a complete table scan can be avoided and the
** code will run much faster.  Most of the work of this routine is checking
** to see if there are indices that can be used to speed up the loop.
**
** Terms of the WHERE clause are also used to limit which rows actually
** make it to the "..." in the middle of the loop.  After each "foreach",
** terms of the WHERE clause that use only terms in that loop and outer
** loops are evaluated and if false a jump is made around all subsequent
** inner loops (or around the "..." if the test occurs within the inner-
** most loop)
**
** OUTER JOINS
**
** An outer join of tables t1 and t2 is conceptally coded as follows:
**
**    foreach row1 in t1 do
**      flag = 0
**      foreach row2 in t2 do
**        start:
**          ...
**          flag = 1
**      end
**      if flag==0 then
**        move the row2 cursor to a null row
**        goto start
**      fi
**    end
**
** ORDER BY CLAUSE PROCESSING
**
** pOrderBy is a pointer to the ORDER BY clause (or the GROUP BY clause
** if the WHERE_GROUPBY flag is set in wctrlFlags) of a SELECT statement
** if there is one.  If there is no ORDER BY clause or if this routine
** is called from an UPDATE or DELETE statement, then pOrderBy is NULL.
**
** The iIdxCur parameter is the cursor number of an index.  If 
** WHERE_ONETABLE_ONLY is set, iIdxCur is the cursor number of an index
** to use for OR clause processing.  The WHERE clause should use this
** specific cursor.  If WHERE_ONEPASS_DESIRED is set, then iIdxCur is
** the first cursor in an array of cursors for all indices.  iIdxCur should
** be used to compute the appropriate cursor depending on which index is
** used.
*/
WhereInfo *sqlite3WhereBegin(
  Parse *pParse,        /* The parser context */
  SrcList *pTabList,    /* FROM clause: A list of all tables to be scanned */
  Expr *pWhere,         /* The WHERE clause */
  ExprList *pOrderBy,   /* An ORDER BY (or GROUP BY) clause, or NULL */
  ExprList *pResultSet, /* Result set of the query */
  u16 wctrlFlags,       /* One of the WHERE_* flags defined in sqliteInt.h */
  int iIdxCur           /* If WHERE_ONETABLE_ONLY is set, index cursor number */
){
  int nByteWInfo;            /* Num. bytes allocated for WhereInfo struct */
  int nTabList;              /* Number of elements in pTabList */
  WhereInfo *pWInfo;         /* Will become the return value of this function */
  Vdbe *v = pParse->pVdbe;   /* The virtual database engine */
  Bitmask notReady;          /* Cursors that are not yet positioned */
  WhereLoopBuilder sWLB;     /* The WhereLoop builder */
  WhereMaskSet *pMaskSet;    /* The expression mask set */
  WhereLevel *pLevel;        /* A single level in pWInfo->a[] */
  WhereLoop *pLoop;          /* Pointer to a single WhereLoop object */
  int ii;                    /* Loop counter */
  sqlite3 *db;               /* Database connection */
  int rc;                    /* Return code */


  /* Variable initialization */
  db = pParse->db;
  memset(&sWLB, 0, sizeof(sWLB));

  /* An ORDER/GROUP BY clause of more than 63 terms cannot be optimized */
  testcase( pOrderBy && pOrderBy->nExpr==BMS-1 );
  if( pOrderBy && pOrderBy->nExpr>=BMS ) pOrderBy = 0;
  sWLB.pOrderBy = pOrderBy;

  /* Disable the DISTINCT optimization if SQLITE_DistinctOpt is set via
  ** sqlite3_test_ctrl(SQLITE_TESTCTRL_OPTIMIZATIONS,...) */
  if( OptimizationDisabled(db, SQLITE_DistinctOpt) ){
    wctrlFlags &= ~WHERE_WANT_DISTINCT;
  }

  /* The number of tables in the FROM clause is limited by the number of
  ** bits in a Bitmask 
  */
  testcase( pTabList->nSrc==BMS );
  if( pTabList->nSrc>BMS ){
    sqlite3ErrorMsg(pParse, "at most %d tables in a join", BMS);
    return 0;
  }

  /* This function normally generates a nested loop for all tables in 
  ** pTabList.  But if the WHERE_ONETABLE_ONLY flag is set, then we should
  ** only generate code for the first table in pTabList and assume that
  ** any cursors associated with subsequent tables are uninitialized.
  */
  nTabList = (wctrlFlags & WHERE_ONETABLE_ONLY) ? 1 : pTabList->nSrc;

  /* Allocate and initialize the WhereInfo structure that will become the
  ** return value. A single allocation is used to store the WhereInfo
  ** struct, the contents of WhereInfo.a[], the WhereClause structure
  ** and the WhereMaskSet structure. Since WhereClause contains an 8-byte
  ** field (type Bitmask) it must be aligned on an 8-byte boundary on
  ** some architectures. Hence the ROUND8() below.
  */
  nByteWInfo = ROUND8(sizeof(WhereInfo)+(nTabList-1)*sizeof(WhereLevel));
  pWInfo = sqlite3DbMallocZero(db, nByteWInfo + sizeof(WhereLoop));
  if( db->mallocFailed ){
    sqlite3DbFree(db, pWInfo);
    pWInfo = 0;
    goto whereBeginError;
  }
  pWInfo->aiCurOnePass[0] = pWInfo->aiCurOnePass[1] = -1;
  pWInfo->nLevel = nTabList;
  pWInfo->pParse = pParse;
  pWInfo->pTabList = pTabList;
  pWInfo->pOrderBy = pOrderBy;
  pWInfo->pResultSet = pResultSet;
  pWInfo->iBreak = pWInfo->iContinue = sqlite3VdbeMakeLabel(v);
  pWInfo->wctrlFlags = wctrlFlags;
  pWInfo->savedNQueryLoop = pParse->nQueryLoop;
  pMaskSet = &pWInfo->sMaskSet;
  sWLB.pWInfo = pWInfo;
  sWLB.pWC = &pWInfo->sWC;
  sWLB.pNew = (WhereLoop*)(((char*)pWInfo)+nByteWInfo);
  assert( EIGHT_BYTE_ALIGNMENT(sWLB.pNew) );
  whereLoopInit(sWLB.pNew);
#ifdef SQLITE_DEBUG
  sWLB.pNew->cId = '*';
#endif

  /* Split the WHERE clause into separate subexpressions where each
  ** subexpression is separated by an AND operator.
  */
  initMaskSet(pMaskSet);
  whereClauseInit(&pWInfo->sWC, pWInfo);
  whereSplit(&pWInfo->sWC, pWhere, TK_AND);
    
  /* Special case: a WHERE clause that is constant.  Evaluate the
  ** expression and either jump over all of the code or fall thru.
  */
  for(ii=0; ii<sWLB.pWC->nTerm; ii++){
    if( nTabList==0 || sqlite3ExprIsConstantNotJoin(sWLB.pWC->a[ii].pExpr) ){
      sqlite3ExprIfFalse(pParse, sWLB.pWC->a[ii].pExpr, pWInfo->iBreak,
                         SQLITE_JUMPIFNULL);
      sWLB.pWC->a[ii].wtFlags |= TERM_CODED;
    }
  }

  /* Special case: No FROM clause
  */
  if( nTabList==0 ){
    if( pOrderBy ) pWInfo->nOBSat = pOrderBy->nExpr;
    if( wctrlFlags & WHERE_WANT_DISTINCT ){
      pWInfo->eDistinct = WHERE_DISTINCT_UNIQUE;
    }
  }

  /* Assign a bit from the bitmask to every term in the FROM clause.
  **
  ** When assigning bitmask values to FROM clause cursors, it must be
  ** the case that if X is the bitmask for the N-th FROM clause term then
  ** the bitmask for all FROM clause terms to the left of the N-th term
  ** is (X-1).   An expression from the ON clause of a LEFT JOIN can use
  ** its Expr.iRightJoinTable value to find the bitmask of the right table
  ** of the join.  Subtracting one from the right table bitmask gives a
  ** bitmask for all tables to the left of the join.  Knowing the bitmask
  ** for all tables to the left of a left join is important.  Ticket #3015.
  **
  ** Note that bitmasks are created for all pTabList->nSrc tables in
  ** pTabList, not just the first nTabList tables.  nTabList is normally
  ** equal to pTabList->nSrc but might be shortened to 1 if the
  ** WHERE_ONETABLE_ONLY flag is set.
  */
  for(ii=0; ii<pTabList->nSrc; ii++){
    createMask(pMaskSet, pTabList->a[ii].iCursor);
  }
#ifndef NDEBUG
  {
    Bitmask toTheLeft = 0;
    for(ii=0; ii<pTabList->nSrc; ii++){
      Bitmask m = getMask(pMaskSet, pTabList->a[ii].iCursor);
      assert( (m-1)==toTheLeft );
      toTheLeft |= m;
    }
  }
#endif

  /* Analyze all of the subexpressions.  Note that exprAnalyze() might
  ** add new virtual terms onto the end of the WHERE clause.  We do not
  ** want to analyze these virtual terms, so start analyzing at the end
  ** and work forward so that the added virtual terms are never processed.
  */
  exprAnalyzeAll(pTabList, &pWInfo->sWC);
  if( db->mallocFailed ){
    goto whereBeginError;
  }

  if( wctrlFlags & WHERE_WANT_DISTINCT ){
    if( isDistinctRedundant(pParse, pTabList, &pWInfo->sWC, pResultSet) ){
      /* The DISTINCT marking is pointless.  Ignore it. */
      pWInfo->eDistinct = WHERE_DISTINCT_UNIQUE;
    }else if( pOrderBy==0 ){
      /* Try to ORDER BY the result set to make distinct processing easier */
      pWInfo->wctrlFlags |= WHERE_DISTINCTBY;
      pWInfo->pOrderBy = pResultSet;
    }
  }

  /* Construct the WhereLoop objects */
  WHERETRACE(0xffff,("*** Optimizer Start ***\n"));
#if defined(WHERETRACE_ENABLED)
  /* Display all terms of the WHERE clause */
  if( sqlite3WhereTrace & 0x100 ){
    int i;
    for(i=0; i<sWLB.pWC->nTerm; i++){
      whereTermPrint(&sWLB.pWC->a[i], i);
    }
  }
#endif

  if( nTabList!=1 || whereShortCut(&sWLB)==0 ){
    rc = whereLoopAddAll(&sWLB);
    if( rc ) goto whereBeginError;
  
    /* Display all of the WhereLoop objects if wheretrace is enabled */
#ifdef WHERETRACE_ENABLED /* !=0 */
    if( sqlite3WhereTrace ){
      WhereLoop *p;
      int i;
      static char zLabel[] = "0123456789abcdefghijklmnopqrstuvwyxz"
                                       "ABCDEFGHIJKLMNOPQRSTUVWYXZ";
      for(p=pWInfo->pLoops, i=0; p; p=p->pNextLoop, i++){
        p->cId = zLabel[i%sizeof(zLabel)];
        whereLoopPrint(p, sWLB.pWC);
      }
    }
#endif
  
    wherePathSolver(pWInfo, 0);
    if( db->mallocFailed ) goto whereBeginError;
    if( pWInfo->pOrderBy ){
       wherePathSolver(pWInfo, pWInfo->nRowOut+1);
       if( db->mallocFailed ) goto whereBeginError;
    }
  }
  if( pWInfo->pOrderBy==0 && (db->flags & SQLITE_ReverseOrder)!=0 ){
     pWInfo->revMask = (Bitmask)(-1);
  }
  if( pParse->nErr || NEVER(db->mallocFailed) ){
    goto whereBeginError;
  }
#ifdef WHERETRACE_ENABLED /* !=0 */
  if( sqlite3WhereTrace ){
    int ii;
    sqlite3DebugPrintf("---- Solution nRow=%d", pWInfo->nRowOut);
    if( pWInfo->nOBSat>0 ){
      sqlite3DebugPrintf(" ORDERBY=%d,0x%llx", pWInfo->nOBSat, pWInfo->revMask);
    }
    switch( pWInfo->eDistinct ){
      case WHERE_DISTINCT_UNIQUE: {
        sqlite3DebugPrintf("  DISTINCT=unique");
        break;
      }
      case WHERE_DISTINCT_ORDERED: {
        sqlite3DebugPrintf("  DISTINCT=ordered");
        break;
      }
      case WHERE_DISTINCT_UNORDERED: {
        sqlite3DebugPrintf("  DISTINCT=unordered");
        break;
      }
    }
    sqlite3DebugPrintf("\n");
    for(ii=0; ii<pWInfo->nLevel; ii++){
      whereLoopPrint(pWInfo->a[ii].pWLoop, sWLB.pWC);
    }
  }
#endif
  /* Attempt to omit tables from the join that do not effect the result */
  if( pWInfo->nLevel>=2
   && pResultSet!=0
   && OptimizationEnabled(db, SQLITE_OmitNoopJoin)
  ){
    Bitmask tabUsed = exprListTableUsage(pMaskSet, pResultSet);
    if( sWLB.pOrderBy ) tabUsed |= exprListTableUsage(pMaskSet, sWLB.pOrderBy);
    while( pWInfo->nLevel>=2 ){
      WhereTerm *pTerm, *pEnd;
      pLoop = pWInfo->a[pWInfo->nLevel-1].pWLoop;
      if( (pWInfo->pTabList->a[pLoop->iTab].jointype & JT_LEFT)==0 ) break;
      if( (wctrlFlags & WHERE_WANT_DISTINCT)==0
       && (pLoop->wsFlags & WHERE_ONEROW)==0
      ){
        break;
      }
      if( (tabUsed & pLoop->maskSelf)!=0 ) break;
      pEnd = sWLB.pWC->a + sWLB.pWC->nTerm;
      for(pTerm=sWLB.pWC->a; pTerm<pEnd; pTerm++){
        if( (pTerm->prereqAll & pLoop->maskSelf)!=0
         && !ExprHasProperty(pTerm->pExpr, EP_FromJoin)
        ){
          break;
        }
      }
      if( pTerm<pEnd ) break;
      WHERETRACE(0xffff, ("-> drop loop %c not used\n", pLoop->cId));
      pWInfo->nLevel--;
      nTabList--;
    }
  }
  WHERETRACE(0xffff,("*** Optimizer Finished ***\n"));
  pWInfo->pParse->nQueryLoop += pWInfo->nRowOut;

  /* If the caller is an UPDATE or DELETE statement that is requesting
  ** to use a one-pass algorithm, determine if this is appropriate.
  ** The one-pass algorithm only works if the WHERE clause constrains
  ** the statement to update a single row.
  */
  assert( (wctrlFlags & WHERE_ONEPASS_DESIRED)==0 || pWInfo->nLevel==1 );
  if( (wctrlFlags & WHERE_ONEPASS_DESIRED)!=0 
   && (pWInfo->a[0].pWLoop->wsFlags & WHERE_ONEROW)!=0 ){
    pWInfo->okOnePass = 1;
    if( HasRowid(pTabList->a[0].pTab) ){
      pWInfo->a[0].pWLoop->wsFlags &= ~WHERE_IDX_ONLY;
    }
  }

  /* Open all tables in the pTabList and any indices selected for
  ** searching those tables.
  */
  notReady = ~(Bitmask)0;
  for(ii=0, pLevel=pWInfo->a; ii<nTabList; ii++, pLevel++){
    Table *pTab;     /* Table to open */
    int iDb;         /* Index of database containing table/index */
    struct SrcList_item *pTabItem;

    pTabItem = &pTabList->a[pLevel->iFrom];
    pTab = pTabItem->pTab;
    iDb = sqlite3SchemaToIndex(db, pTab->pSchema);
    pLoop = pLevel->pWLoop;
    if( (pTab->tabFlags & TF_Ephemeral)!=0 || pTab->pSelect ){
      /* Do nothing */
    }else
#ifndef SQLITE_OMIT_VIRTUALTABLE
    if( (pLoop->wsFlags & WHERE_VIRTUALTABLE)!=0 ){
      const char *pVTab = (const char *)sqlite3GetVTable(db, pTab);
      int iCur = pTabItem->iCursor;
      sqlite3VdbeAddOp4(v, OP_VOpen, iCur, 0, 0, pVTab, P4_VTAB);
    }else if( IsVirtual(pTab) ){
      /* noop */
    }else
#endif
    if( (pLoop->wsFlags & WHERE_IDX_ONLY)==0
         && (wctrlFlags & WHERE_OMIT_OPEN_CLOSE)==0 ){
      int op = OP_OpenRead;
      if( pWInfo->okOnePass ){
        op = OP_OpenWrite;
        pWInfo->aiCurOnePass[0] = pTabItem->iCursor;
      };
      sqlite3OpenTable(pParse, pTabItem->iCursor, iDb, pTab, op);
      assert( pTabItem->iCursor==pLevel->iTabCur );
      testcase( !pWInfo->okOnePass && pTab->nCol==BMS-1 );
      testcase( !pWInfo->okOnePass && pTab->nCol==BMS );
      if( !pWInfo->okOnePass && pTab->nCol<BMS && HasRowid(pTab) ){
        Bitmask b = pTabItem->colUsed;
        int n = 0;
        for(; b; b=b>>1, n++){}
        sqlite3VdbeChangeP4(v, sqlite3VdbeCurrentAddr(v)-1, 
                            SQLITE_INT_TO_PTR(n), P4_INT32);
        assert( n<=pTab->nCol );
      }
    }else{
      sqlite3TableLock(pParse, iDb, pTab->tnum, 0, pTab->zName);
    }
    if( pLoop->wsFlags & WHERE_INDEXED ){
      Index *pIx = pLoop->u.btree.pIndex;
      int iIndexCur;
      int op = OP_OpenRead;
      /* iIdxCur is always set if to a positive value if ONEPASS is possible */
      assert( iIdxCur!=0 || (pWInfo->wctrlFlags & WHERE_ONEPASS_DESIRED)==0 );
      if( !HasRowid(pTab) && IsPrimaryKeyIndex(pIx)
       && (wctrlFlags & WHERE_ONETABLE_ONLY)!=0
      ){
        /* This is one term of an OR-optimization using the PRIMARY KEY of a
        ** WITHOUT ROWID table.  No need for a separate index */
        iIndexCur = pLevel->iTabCur;
        op = 0;
      }else if( pWInfo->okOnePass ){
        Index *pJ = pTabItem->pTab->pIndex;
        iIndexCur = iIdxCur;
        assert( wctrlFlags & WHERE_ONEPASS_DESIRED );
        while( ALWAYS(pJ) && pJ!=pIx ){
          iIndexCur++;
          pJ = pJ->pNext;
        }
        op = OP_OpenWrite;
        pWInfo->aiCurOnePass[1] = iIndexCur;
      }else if( iIdxCur && (wctrlFlags & WHERE_ONETABLE_ONLY)!=0 ){
        iIndexCur = iIdxCur;
        if( wctrlFlags & WHERE_REOPEN_IDX ) op = OP_ReopenIdx;
      }else{
        iIndexCur = pParse->nTab++;
      }
      pLevel->iIdxCur = iIndexCur;
      assert( pIx->pSchema==pTab->pSchema );
      assert( iIndexCur>=0 );
      if( op ){
        sqlite3VdbeAddOp3(v, op, iIndexCur, pIx->tnum, iDb);
        sqlite3VdbeSetP4KeyInfo(pParse, pIx);
        if( (pLoop->wsFlags & WHERE_CONSTRAINT)!=0
         && (pLoop->wsFlags & (WHERE_COLUMN_RANGE|WHERE_SKIPSCAN))==0
         && (pWInfo->wctrlFlags&WHERE_ORDERBY_MIN)==0
        ){
          sqlite3VdbeChangeP5(v, OPFLAG_SEEKEQ); /* Hint to COMDB2 */
        }
        VdbeComment((v, "%s", pIx->zName));
      }
    }
    if( iDb>=0 ) sqlite3CodeVerifySchema(pParse, iDb);
    notReady &= ~getMask(&pWInfo->sMaskSet, pTabItem->iCursor);
  }
  pWInfo->iTop = sqlite3VdbeCurrentAddr(v);
  if( db->mallocFailed ) goto whereBeginError;

  /* Generate the code to do the search.  Each iteration of the for
  ** loop below generates code for a single nested loop of the VM
  ** program.
  */
  notReady = ~(Bitmask)0;
  for(ii=0; ii<nTabList; ii++){
    int addrExplain;
    int wsFlags;
    pLevel = &pWInfo->a[ii];
    wsFlags = pLevel->pWLoop->wsFlags;
#ifndef SQLITE_OMIT_AUTOMATIC_INDEX
    if( (pLevel->pWLoop->wsFlags & WHERE_AUTO_INDEX)!=0 ){
      constructAutomaticIndex(pParse, &pWInfo->sWC,
                &pTabList->a[pLevel->iFrom], notReady, pLevel);
      if( db->mallocFailed ) goto whereBeginError;
    }
#endif
    addrExplain = explainOneScan(
        pParse, pTabList, pLevel, ii, pLevel->iFrom, wctrlFlags
    );
    pLevel->addrBody = sqlite3VdbeCurrentAddr(v);
    notReady = codeOneLoopStart(pWInfo, ii, notReady);
    pWInfo->iContinue = pLevel->addrCont;
    if( (wsFlags&WHERE_MULTI_OR)==0 && (wctrlFlags&WHERE_ONETABLE_ONLY)==0 ){
      addScanStatus(v, pTabList, pLevel, addrExplain);
    }
  }

  /* Done. */
  VdbeModuleComment((v, "Begin WHERE-core"));
  return pWInfo;

  /* Jump here if malloc fails */
whereBeginError:
  if( pWInfo ){
    pParse->nQueryLoop = pWInfo->savedNQueryLoop;
    whereInfoFree(db, pWInfo);
  }
  return 0;
}

/*
** Generate the end of the WHERE loop.  See comments on 
** sqlite3WhereBegin() for additional information.
*/
void sqlite3WhereEnd(WhereInfo *pWInfo){
  Parse *pParse = pWInfo->pParse;
  Vdbe *v = pParse->pVdbe;
  int i;
  WhereLevel *pLevel;
  WhereLoop *pLoop;
  SrcList *pTabList = pWInfo->pTabList;
  sqlite3 *db = pParse->db;

  /* Generate loop termination code.
  */
  VdbeModuleComment((v, "End WHERE-core"));
  sqlite3ExprCacheClear(pParse);
  for(i=pWInfo->nLevel-1; i>=0; i--){
    int addr;
    pLevel = &pWInfo->a[i];
    pLoop = pLevel->pWLoop;
    sqlite3VdbeResolveLabel(v, pLevel->addrCont);
    if( pLevel->op!=OP_Noop ){
      sqlite3VdbeAddOp3(v, pLevel->op, pLevel->p1, pLevel->p2, pLevel->p3);
      sqlite3VdbeChangeP5(v, pLevel->p5);
      VdbeCoverage(v);
      VdbeCoverageIf(v, pLevel->op==OP_Next);
      VdbeCoverageIf(v, pLevel->op==OP_Prev);
      VdbeCoverageIf(v, pLevel->op==OP_VNext);
    }
    if( pLoop->wsFlags & WHERE_IN_ABLE && pLevel->u.in.nIn>0 ){
      struct InLoop *pIn;
      int j;
      sqlite3VdbeResolveLabel(v, pLevel->addrNxt);
      for(j=pLevel->u.in.nIn, pIn=&pLevel->u.in.aInLoop[j-1]; j>0; j--, pIn--){
        sqlite3VdbeJumpHere(v, pIn->addrInTop+1);
        sqlite3VdbeAddOp2(v, pIn->eEndLoopOp, pIn->iCur, pIn->addrInTop);
        VdbeCoverage(v);
        VdbeCoverageIf(v, pIn->eEndLoopOp==OP_PrevIfOpen);
        VdbeCoverageIf(v, pIn->eEndLoopOp==OP_NextIfOpen);
        sqlite3VdbeJumpHere(v, pIn->addrInTop-1);
      }
      sqlite3DbFree(db, pLevel->u.in.aInLoop);
    }
    sqlite3VdbeResolveLabel(v, pLevel->addrBrk);
    if( pLevel->addrSkip ){
      sqlite3VdbeAddOp2(v, OP_Goto, 0, pLevel->addrSkip);
      VdbeComment((v, "next skip-scan on %s", pLoop->u.btree.pIndex->zName));
      sqlite3VdbeJumpHere(v, pLevel->addrSkip);
      sqlite3VdbeJumpHere(v, pLevel->addrSkip-2);
    }
    if( pLevel->addrLikeRep ){
      int op;
      if( sqlite3VdbeGetOp(v, pLevel->addrLikeRep-1)->p1 ){
        op = OP_DecrJumpZero;
      }else{
        op = OP_JumpZeroIncr;
      }
      sqlite3VdbeAddOp2(v, op, pLevel->iLikeRepCntr, pLevel->addrLikeRep);
      VdbeCoverage(v);
    }
    if( pLevel->iLeftJoin ){
      addr = sqlite3VdbeAddOp1(v, OP_IfPos, pLevel->iLeftJoin); VdbeCoverage(v);
      assert( (pLoop->wsFlags & WHERE_IDX_ONLY)==0
           || (pLoop->wsFlags & WHERE_INDEXED)!=0 );
      if( (pLoop->wsFlags & WHERE_IDX_ONLY)==0 ){
        sqlite3VdbeAddOp1(v, OP_NullRow, pTabList->a[i].iCursor);
      }
      if( pLoop->wsFlags & WHERE_INDEXED ){
        sqlite3VdbeAddOp1(v, OP_NullRow, pLevel->iIdxCur);
      }
      if( pLevel->op==OP_Return ){
        sqlite3VdbeAddOp2(v, OP_Gosub, pLevel->p1, pLevel->addrFirst);
      }else{
        sqlite3VdbeAddOp2(v, OP_Goto, 0, pLevel->addrFirst);
      }
      sqlite3VdbeJumpHere(v, addr);
    }
    VdbeModuleComment((v, "End WHERE-loop%d: %s", i,
                     pWInfo->pTabList->a[pLevel->iFrom].pTab->zName));
  }

  /* The "break" point is here, just past the end of the outer loop.
  ** Set it.
  */
  sqlite3VdbeResolveLabel(v, pWInfo->iBreak);

  assert( pWInfo->nLevel<=pTabList->nSrc );
  for(i=0, pLevel=pWInfo->a; i<pWInfo->nLevel; i++, pLevel++){
    int k, last;
    VdbeOp *pOp;
    Index *pIdx = 0;
    struct SrcList_item *pTabItem = &pTabList->a[pLevel->iFrom];
    Table *pTab = pTabItem->pTab;
    assert( pTab!=0 );
    pLoop = pLevel->pWLoop;

    /* For a co-routine, change all OP_Column references to the table of
    ** the co-routine into OP_SCopy of result contained in a register.
    ** OP_Rowid becomes OP_Null.
    */
    if( pTabItem->viaCoroutine && !db->mallocFailed ){
      last = sqlite3VdbeCurrentAddr(v);
      k = pLevel->addrBody;
      pOp = sqlite3VdbeGetOp(v, k);
      for(; k<last; k++, pOp++){
        if( pOp->p1!=pLevel->iTabCur ) continue;
        if( pOp->opcode==OP_Column ){
          pOp->opcode = OP_Copy;
          pOp->p1 = pOp->p2 + pTabItem->regResult;
          pOp->p2 = pOp->p3;
          pOp->p3 = 0;
        }else if( pOp->opcode==OP_Rowid ){
          pOp->opcode = OP_Null;
          pOp->p1 = 0;
          pOp->p3 = 0;
        }
      }
      continue;
    }

    /* Close all of the cursors that were opened by sqlite3WhereBegin.
    ** Except, do not close cursors that will be reused by the OR optimization
    ** (WHERE_OMIT_OPEN_CLOSE).  And do not close the OP_OpenWrite cursors
    ** created for the ONEPASS optimization.
    */
    if( (pTab->tabFlags & TF_Ephemeral)==0
     && pTab->pSelect==0
     && (pWInfo->wctrlFlags & WHERE_OMIT_OPEN_CLOSE)==0
    ){
      int ws = pLoop->wsFlags;
      if( !pWInfo->okOnePass && (ws & WHERE_IDX_ONLY)==0 ){
        sqlite3VdbeAddOp1(v, OP_Close, pTabItem->iCursor);
      }
      if( (ws & WHERE_INDEXED)!=0
       && (ws & (WHERE_IPK|WHERE_AUTO_INDEX))==0 
       && pLevel->iIdxCur!=pWInfo->aiCurOnePass[1]
      ){
        sqlite3VdbeAddOp1(v, OP_Close, pLevel->iIdxCur);
      }
    }

    /* If this scan uses an index, make VDBE code substitutions to read data
    ** from the index instead of from the table where possible.  In some cases
    ** this optimization prevents the table from ever being read, which can
    ** yield a significant performance boost.
    ** 
    ** Calls to the code generator in between sqlite3WhereBegin and
    ** sqlite3WhereEnd will have created code that references the table
    ** directly.  This loop scans all that code looking for opcodes
    ** that reference the table and converts them into opcodes that
    ** reference the index.
    */
    if( pLoop->wsFlags & (WHERE_INDEXED|WHERE_IDX_ONLY) ){
      pIdx = pLoop->u.btree.pIndex;
    }else if( pLoop->wsFlags & WHERE_MULTI_OR ){
      pIdx = pLevel->u.pCovidx;
    }
    if( pIdx && !db->mallocFailed ){
      last = sqlite3VdbeCurrentAddr(v);
      k = pLevel->addrBody;
      pOp = sqlite3VdbeGetOp(v, k);
      for(; k<last; k++, pOp++){
        if( pOp->p1!=pLevel->iTabCur ) continue;
        if( pOp->opcode==OP_Column ){
          int x = pOp->p2;
          assert( pIdx->pTable==pTab );
          if( !HasRowid(pTab) ){
            Index *pPk = sqlite3PrimaryKeyIndex(pTab);
            x = pPk->aiColumn[x];
          }
          x = sqlite3ColumnOfIndex(pIdx, x);
          if( x>=0 ){
            pOp->p2 = x;
            pOp->p1 = pLevel->iIdxCur;
          }
          assert( (pLoop->wsFlags & WHERE_IDX_ONLY)==0 || x>=0 );
        }else if( pOp->opcode==OP_Rowid ){
          pOp->p1 = pLevel->iIdxCur;
          pOp->opcode = OP_IdxRowid;
        }
      }
    }
  }

  /* Final cleanup
  */
  pParse->nQueryLoop = pWInfo->savedNQueryLoop;
  whereInfoFree(db, pWInfo);
  return;
}
