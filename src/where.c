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
** the WHERE clause of SQL statements.  This module is reponsible for
** generating the code that loops through a table looking for applicable
** rows.  Indices are selected and used to speed the search when doing
** so is applicable.  Because this module is responsible for selecting
** indices, you might also think of this module as the "query optimizer".
**
** $Id: where.c,v 1.151 2005/07/22 00:31:40 drh Exp $
*/
#include "sqliteInt.h"

/*
** The number of bits in a Bitmask.  "BMS" means "BitMask Size".
*/
#define BMS  (sizeof(Bitmask)*8)

/*
** Determine the number of elements in an array.
*/
#define ARRAYSIZE(X)  (sizeof(X)/sizeof(X[0]))

/* Forward reference
*/
typedef struct WhereClause WhereClause;

/*
** The query generator uses an array of instances of this structure to
** help it analyze the subexpressions of the WHERE clause.  Each WHERE
** clause subexpression is separated from the others by an AND operator.
**
** All WhereTerms are collected into a single WhereClause structure.  
** The following identity holds:
**
**        WhereTerm.pWC->a[WhereTerm.idx] == WhereTerm
**
** When a term is of the form:
**
**              X <op> <expr>
**
** where X is a column name and <op> is one of certain operators,
** then WhereTerm.leftCursor and WhereTerm.leftColumn record the
** cursor number and column number for X.  
**
** prereqRight and prereqAll record sets of cursor numbers,
** but they do so indirectly.  A single ExprMaskSet structure translates
** cursor number into bits and the translated bit is stored in the prereq
** fields.  The translation is used in order to maximize the number of
** bits that will fit in a Bitmask.  The VDBE cursor numbers might be
** spread out over the non-negative integers.  For example, the cursor
** numbers might be 3, 8, 9, 10, 20, 23, 41, and 45.  The ExprMaskSet
** translates these sparse cursor numbers into consecutive integers
** beginning with 0 in order to make the best possible use of the available
** bits in the Bitmask.  So, in the example above, the cursor numbers
** would be mapped into integers 0 through 7.
*/
typedef struct WhereTerm WhereTerm;
struct WhereTerm {
  Expr *pExpr;            /* Pointer to the subexpression */
  u16 idx;                /* Index of this term in pWC->a[] */
  i16 iPartner;           /* Disable pWC->a[iPartner] when this term disabled */
  u16 flags;              /* Bit flags.  See below */
  i16 leftCursor;         /* Cursor number of X in "X <op> <expr>" */
  i16 leftColumn;         /* Column number of X in "X <op> <expr>" */
  u8 operator;            /* A WO_xx value describing <op> */
  WhereClause *pWC;       /* The clause this term is part of */
  Bitmask prereqRight;    /* Bitmask of tables used by pRight */
  Bitmask prereqAll;      /* Bitmask of tables referenced by p */
};

/*
** Allowed values of WhereTerm.flags
*/
#define TERM_DYNAMIC    0x0001   /* Need to call sqlite3ExprDelete(p) */
#define TERM_VIRTUAL    0x0002   /* Added by the optimizer.  Do not code */
#define TERM_CODED      0x0004   /* This term is already coded */

/*
** An instance of the following structure holds all information about a
** WHERE clause.  Mostly this is a container for one or more WhereTerms.
*/
struct WhereClause {
  Parse *pParse;           /* The parser context */
  int nTerm;               /* Number of terms */
  int nSlot;               /* Number of entries in a[] */
  WhereTerm *a;            /* Pointer to an array of terms */
  WhereTerm aStatic[10];   /* Initial static space for the terms */
};

/*
** When WhereTerms are used to select elements from an index, we
** call those terms "constraints".  For example, consider the following
** SQL:
**
**       CREATE TABLE t1(a INTEGER PRIMARY KEY, b, c, d);
**       CREATE INDEX t1i1 ON t1(b,c);
**
**       SELECT * FROM t1 WHERE d=5 AND b=7 AND c>11;
**
** In the SELECT statement, the "b=7" and "c>11" terms are constraints
** because they can be used to choose rows out of the t1i1 index.  But
** the "d=5" term is not a constraint because it is not indexed.
**
** When generating code to access an index, we have to keep track of
** all of the constraints associated with that index.  This is done
** using an array of instanaces of the following structure.  There is
** one instance of this structure for each constraint on the index.
**
** Actually, we allocate the array of this structure based on the total
** number of terms in the entire WHERE clause (because the number of
** constraints can never be more than that) and reuse it when coding
** each index.
*/
typedef struct WhereConstraint WhereConstraint;
struct WhereConstraint {
  int iMem;            /* Mem cell used to hold <expr> part of constraint */
};

/*
** An instance of the following structure keeps track of a mapping
** between VDBE cursor numbers and bits of the bitmasks in WhereTerm.
**
** The VDBE cursor numbers are small integers contained in 
** SrcList_item.iCursor and Expr.iTable fields.  For any given WHERE 
** clause, the cursor numbers might not begin with 0 and they might
** contain gaps in the numbering sequence.  But we want to make maximum
** use of the bits in our bitmasks.  This structure provides a mapping
** from the sparse cursor numbers into consecutive integers beginning
** with 0.
**
** If ExprMaskSet.ix[A]==B it means that The A-th bit of a Bitmask
** corresponds VDBE cursor number B.  The A-th bit of a bitmask is 1<<A.
**
** For example, if the WHERE clause expression used these VDBE
** cursors:  4, 5, 8, 29, 57, 73.  Then the  ExprMaskSet structure
** would map those cursor numbers into bits 0 through 5.
**
** Note that the mapping is not necessarily ordered.  In the example
** above, the mapping might go like this:  4->3, 5->1, 8->2, 29->0,
** 57->5, 73->4.  Or one of 719 other combinations might be used. It
** does not really matter.  What is important is that sparse cursor
** numbers all get mapped into bit numbers that begin with 0 and contain
** no gaps.
*/
typedef struct ExprMaskSet ExprMaskSet;
struct ExprMaskSet {
  int n;                        /* Number of assigned cursor values */
  int ix[sizeof(Bitmask)*8];    /* Cursor assigned to each bit */
};


/*
** Initialize a preallocated WhereClause structure.
*/
static void whereClauseInit(WhereClause *pWC, Parse *pParse){
  pWC->pParse = pParse;
  pWC->nTerm = 0;
  pWC->nSlot = ARRAYSIZE(pWC->aStatic);
  pWC->a = pWC->aStatic;
}

/*
** Deallocate a WhereClause structure.  The WhereClause structure
** itself is not freed.  This routine is the inverse of whereClauseInit().
*/
static void whereClauseClear(WhereClause *pWC){
  int i;
  WhereTerm *a;
  for(i=pWC->nTerm-1, a=pWC->a; i>=0; i--, a++){
    if( a->flags & TERM_DYNAMIC ){
      sqlite3ExprDelete(a->pExpr);
    }
  }
  if( pWC->a!=pWC->aStatic ){
    sqliteFree(pWC->a);
  }
}

/*
** Add a new entries to the WhereClause structure.  Increase the allocated
** space as necessary.
*/
static WhereTerm *whereClauseInsert(WhereClause *pWC, Expr *p, int flags){
  WhereTerm *pTerm;
  if( pWC->nTerm>=pWC->nSlot ){
    WhereTerm *pOld = pWC->a;
    pWC->a = sqliteMalloc( sizeof(pWC->a[0])*pWC->nSlot*2 );
    if( pWC->a==0 ) return 0;
    memcpy(pWC->a, pOld, sizeof(pWC->a[0])*pWC->nTerm);
    if( pOld!=pWC->aStatic ){
      sqliteFree(pOld);
    }
    pWC->nSlot *= 2;
  }
  pTerm = &pWC->a[pWC->nTerm];
  pTerm->idx = pWC->nTerm;
  pWC->nTerm++;
  pTerm->pExpr = p;
  pTerm->flags = flags;
  pTerm->pWC = pWC;
  pTerm->iPartner = -1;
  return pTerm;
}

/*
** This routine identifies subexpressions in the WHERE clause where
** each subexpression is separate by the AND operator.  aSlot is 
** filled with pointers to the subexpressions.  For example:
**
**    WHERE  a=='hello' AND coalesce(b,11)<10 AND (c+12!=d OR c==22)
**           \________/     \_______________/     \________________/
**            slot[0]            slot[1]               slot[2]
**
** The original WHERE clause in pExpr is unaltered.  All this routine
** does is make aSlot[] entries point to substructure within pExpr.
**
** aSlot[] is an array of subexpressions structures.  There are nSlot
** spaces left in this array.  This routine finds as many AND-separated
** subexpressions as it can and puts pointers to those subexpressions
** into aSlot[] entries.  The return value is the number of slots filled.
*/
static void whereSplit(WhereClause *pWC, Expr *pExpr){
  if( pExpr==0 ) return;
  if( pExpr->op!=TK_AND ){
    whereClauseInsert(pWC, pExpr, 0);
  }else{
    whereSplit(pWC, pExpr->pLeft);
    whereSplit(pWC, pExpr->pRight);
  }
}

/*
** Initialize an expression mask set
*/
#define initMaskSet(P)  memset(P, 0, sizeof(*P))

/*
** Return the bitmask for the given cursor number.  Return 0 if
** iCursor is not in the set.
*/
static Bitmask getMask(ExprMaskSet *pMaskSet, int iCursor){
  int i;
  for(i=0; i<pMaskSet->n; i++){
    if( pMaskSet->ix[i]==iCursor ){
      return ((Bitmask)1)<<i;
    }
  }
  return 0;
}

/*
** Create a new mask for cursor iCursor.
**
** There is one cursor per table in the FROM clause.  The number of
** tables in the FROM clause is limited by a test early in the
** sqlite3WhereBegin() routien.  So we know that the pMaskSet->ix[]
** array will never overflow.
*/
static void createMask(ExprMaskSet *pMaskSet, int iCursor){
  assert( pMaskSet->n < ARRAYSIZE(pMaskSet->ix) );
  pMaskSet->ix[pMaskSet->n++] = iCursor;
}

/*
** This routine walks (recursively) an expression tree and generates
** a bitmask indicating which tables are used in that expression
** tree.
**
** In order for this routine to work, the calling function must have
** previously invoked sqlite3ExprResolveNames() on the expression.  See
** the header comment on that routine for additional information.
** The sqlite3ExprResolveNames() routines looks for column names and
** sets their opcodes to TK_COLUMN and their Expr.iTable fields to
** the VDBE cursor number of the table.
*/
static Bitmask exprListTableUsage(ExprMaskSet *, ExprList *);
static Bitmask exprTableUsage(ExprMaskSet *pMaskSet, Expr *p){
  Bitmask mask = 0;
  if( p==0 ) return 0;
  if( p->op==TK_COLUMN ){
    mask = getMask(pMaskSet, p->iTable);
    return mask;
  }
  mask = exprTableUsage(pMaskSet, p->pRight);
  mask |= exprTableUsage(pMaskSet, p->pLeft);
  mask |= exprListTableUsage(pMaskSet, p->pList);
  if( p->pSelect ){
    Select *pS = p->pSelect;
    mask |= exprListTableUsage(pMaskSet, pS->pEList);
    mask |= exprListTableUsage(pMaskSet, pS->pGroupBy);
    mask |= exprListTableUsage(pMaskSet, pS->pOrderBy);
    mask |= exprTableUsage(pMaskSet, pS->pWhere);
    mask |= exprTableUsage(pMaskSet, pS->pHaving);
  }
  return mask;
}
static Bitmask exprListTableUsage(ExprMaskSet *pMaskSet, ExprList *pList){
  int i;
  Bitmask mask = 0;
  if( pList ){
    for(i=0; i<pList->nExpr; i++){
      mask |= exprTableUsage(pMaskSet, pList->a[i].pExpr);
    }
  }
  return mask;
}

/*
** Return TRUE if the given operator is one of the operators that is
** allowed for an indexable WHERE clause term.  The allowed operators are
** "=", "<", ">", "<=", ">=", and "IN".
*/
static int allowedOp(int op){
  assert( TK_GT>TK_EQ && TK_GT<TK_GE );
  assert( TK_LT>TK_EQ && TK_LT<TK_GE );
  assert( TK_LE>TK_EQ && TK_LE<TK_GE );
  assert( TK_GE==TK_EQ+4 );
  return op==TK_IN || (op>=TK_EQ && op<=TK_GE);
}

/*
** Swap two objects of type T.
*/
#define SWAP(TYPE,A,B) {TYPE t=A; A=B; B=t;}

/*
** Commute a comparision operator.  Expressions of the form "X op Y"
** are converted into "Y op X".
*/
static void exprCommute(Expr *pExpr){
  assert( allowedOp(pExpr->op) && pExpr->op!=TK_IN );
  SWAP(CollSeq*,pExpr->pRight->pColl,pExpr->pLeft->pColl);
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
** Bitmasks for the operators that indices are able to exploit.  An
** OR-ed combination of these values can be used when searching for
** terms in the where clause.
*/
#define WO_IN  1
#define WO_EQ  2
#define WO_LT  (2<<(TK_LT-TK_EQ))
#define WO_LE  (2<<(TK_LE-TK_EQ))
#define WO_GT  (2<<(TK_GT-TK_EQ))
#define WO_GE  (2<<(TK_GE-TK_EQ))

/*
** Translate from TK_xx operator to WO_xx bitmask.
*/
static int operatorMask(int op){
  assert( allowedOp(op) );
  if( op==TK_IN ){
    return WO_IN;
  }else{
    return 1<<(op+1-TK_EQ);
  }
}

/*
** Search for a term in the WHERE clause that is of the form "X <op> <expr>"
** where X is a reference to the iColumn of table iCur and <op> is one of
** the WO_xx operator codes specified by the op parameter.
** Return a pointer to the term.  Return 0 if not found.
*/
static WhereTerm *findTerm(
  WhereClause *pWC,     /* The WHERE clause to be searched */
  int iCur,             /* Cursor number of LHS */
  int iColumn,          /* Column number of LHS */
  Bitmask notReady,     /* RHS must not overlap with this mask */
  u8 op,                /* Mask of WO_xx values describing operator */
  Index *pIdx           /* Must be compatible with this index, if not NULL */
){
  WhereTerm *pTerm;
  int k;
  for(pTerm=pWC->a, k=pWC->nTerm; k; k--, pTerm++){
    if( pTerm->leftCursor==iCur
       && (pTerm->prereqRight & notReady)==0
       && pTerm->leftColumn==iColumn
       && (pTerm->operator & op)!=0
    ){
      if( iCur>=0 && pIdx ){
        Expr *pX = pTerm->pExpr;
        CollSeq *pColl;
        char idxaff;
        int k;
        Parse *pParse = pWC->pParse;

        idxaff = pIdx->pTable->aCol[iColumn].affinity;
        if( !sqlite3IndexAffinityOk(pX, idxaff) ) continue;
        pColl = sqlite3ExprCollSeq(pParse, pX->pLeft);
        if( !pColl ){
          if( pX->pRight ){
            pColl = sqlite3ExprCollSeq(pParse, pX->pRight);
          }
          if( !pColl ){
            pColl = pParse->db->pDfltColl;
          }
        }
        for(k=0; k<pIdx->nColumn && pIdx->aiColumn[k]!=iColumn; k++){}
        assert( k<pIdx->nColumn );
        if( pColl!=pIdx->keyInfo.aColl[k] ) continue;
      }
      return pTerm;
    }
  }
  return 0;
}

/*
** The input to this routine is an WhereTerm structure with only the
** "p" field filled in.  The job of this routine is to analyze the
** subexpression and populate all the other fields of the WhereTerm
** structure.
*/
static void exprAnalyze(
  SrcList *pSrc,            /* the FROM clause */
  ExprMaskSet *pMaskSet,    /* table masks */
  WhereTerm *pTerm          /* the WHERE clause term to be analyzed */
){
  Expr *pExpr = pTerm->pExpr;
  Bitmask prereqLeft;
  Bitmask prereqAll;
  int idxRight;

  prereqLeft = exprTableUsage(pMaskSet, pExpr->pLeft);
  pTerm->prereqRight = exprTableUsage(pMaskSet, pExpr->pRight);
  pTerm->prereqAll = prereqAll = exprTableUsage(pMaskSet, pExpr);
  pTerm->leftCursor = -1;
  pTerm->iPartner = -1;
  pTerm->operator = 0;
  idxRight = -1;
  if( allowedOp(pExpr->op) && (pTerm->prereqRight & prereqLeft)==0 ){
    Expr *pLeft = pExpr->pLeft;
    Expr *pRight = pExpr->pRight;
    if( pLeft->op==TK_COLUMN ){
      pTerm->leftCursor = pLeft->iTable;
      pTerm->leftColumn = pLeft->iColumn;
      pTerm->operator = operatorMask(pExpr->op);
    }
    if( pRight && pRight->op==TK_COLUMN ){
      WhereTerm *pNew;
      Expr *pDup;
      if( pTerm->leftCursor>=0 ){
        pDup = sqlite3ExprDup(pExpr);
        pNew = whereClauseInsert(pTerm->pWC, pDup, TERM_VIRTUAL|TERM_DYNAMIC);
        if( pNew==0 ) return;
        pNew->iPartner = pTerm->idx;
      }else{
        pDup = pExpr;
        pNew = pTerm;
      }
      exprCommute(pDup);
      pLeft = pDup->pLeft;
      pNew->leftCursor = pLeft->iTable;
      pNew->leftColumn = pLeft->iColumn;
      pNew->prereqRight = prereqLeft;
      pNew->prereqAll = prereqAll;
      pNew->operator = operatorMask(pDup->op);
    }
  }
}


/*
** This routine decides if pIdx can be used to satisfy the ORDER BY
** clause.  If it can, it returns 1.  If pIdx cannot satisfy the
** ORDER BY clause, this routine returns 0.
**
** pOrderBy is an ORDER BY clause from a SELECT statement.  pTab is the
** left-most table in the FROM clause of that same SELECT statement and
** the table has a cursor number of "base".  pIdx is an index on pTab.
**
** nEqCol is the number of columns of pIdx that are used as equality
** constraints.  Any of these columns may be missing from the ORDER BY
** clause and the match can still be a success.
**
** If the index is UNIQUE, then the ORDER BY clause is allowed to have
** additional terms past the end of the index and the match will still
** be a success.
**
** All terms of the ORDER BY that match against the index must be either
** ASC or DESC.  (Terms of the ORDER BY clause past the end of a UNIQUE
** index do not need to satisfy this constraint.)  The *pbRev value is
** set to 1 if the ORDER BY clause is all DESC and it is set to 0 if
** the ORDER BY clause is all ASC.
*/
static int isSortingIndex(
  Parse *pParse,          /* Parsing context */
  Index *pIdx,            /* The index we are testing */
  Table *pTab,            /* The table to be sorted */
  int base,               /* Cursor number for pTab */
  ExprList *pOrderBy,     /* The ORDER BY clause */
  int nEqCol,             /* Number of index columns with == constraints */
  int *pbRev              /* Set to 1 if ORDER BY is DESC */
){
  int i, j;                    /* Loop counters */
  int sortOrder;               /* Which direction we are sorting */
  int nTerm;                   /* Number of ORDER BY terms */
  struct ExprList_item *pTerm; /* A term of the ORDER BY clause */
  sqlite3 *db = pParse->db;

  assert( pOrderBy!=0 );
  nTerm = pOrderBy->nExpr;
  assert( nTerm>0 );

  /* Match terms of the ORDER BY clause against columns of
  ** the index.
  */
  for(i=j=0, pTerm=pOrderBy->a; j<nTerm && i<pIdx->nColumn; i++){
    Expr *pExpr;       /* The expression of the ORDER BY pTerm */
    CollSeq *pColl;    /* The collating sequence of pExpr */

    pExpr = pTerm->pExpr;
    if( pExpr->op!=TK_COLUMN || pExpr->iTable!=base ){
      /* Can not use an index sort on anything that is not a column in the
      ** left-most table of the FROM clause */
      return 0;
    }
    pColl = sqlite3ExprCollSeq(pParse, pExpr);
    if( !pColl ) pColl = db->pDfltColl;
    if( pExpr->iColumn!=pIdx->aiColumn[i] || pColl!=pIdx->keyInfo.aColl[i] ){
      /* Term j of the ORDER BY clause does not match column i of the index */
      if( i<nEqCol ){
        /* If an index column that is constrained by == fails to match an
        ** ORDER BY term, that is OK.  Just ignore that column of the index
        */
        continue;
      }else{
        /* If an index column fails to match and is not constrained by ==
        ** then the index cannot satisfy the ORDER BY constraint.
        */
        return 0;
      }
    }
    if( i>nEqCol ){
      if( pTerm->sortOrder!=sortOrder ){
        /* Indices can only be used if all ORDER BY terms past the
        ** equality constraints are all either DESC or ASC. */
        return 0;
      }
    }else{
      sortOrder = pTerm->sortOrder;
    }
    j++;
    pTerm++;
  }

  /* The index can be used for sorting if all terms of the ORDER BY clause
  ** or covered or if we ran out of index columns and the it is a UNIQUE
  ** index.
  */
  if( j>=nTerm || (i>=pIdx->nColumn && pIdx->onError!=OE_None) ){
    *pbRev = sortOrder==SQLITE_SO_DESC;
    return 1;
  }
  return 0;
}

/*
** Check table to see if the ORDER BY clause in pOrderBy can be satisfied
** by sorting in order of ROWID.  Return true if so and set *pbRev to be
** true for reverse ROWID and false for forward ROWID order.
*/
static int sortableByRowid(
  int base,               /* Cursor number for table to be sorted */
  ExprList *pOrderBy,     /* The ORDER BY clause */
  int *pbRev              /* Set to 1 if ORDER BY is DESC */
){
  Expr *p;

  assert( pOrderBy!=0 );
  assert( pOrderBy->nExpr>0 );
  p = pOrderBy->a[0].pExpr;
  if( p->op==TK_COLUMN && p->iTable==base && p->iColumn==-1 ){
    *pbRev = pOrderBy->a[0].sortOrder;
    return 1;
  }
  return 0;
}

/*
** Value for flags returned by bestIndex()
*/
#define WHERE_ROWID_EQ       0x0001   /* rowid=EXPR or rowid IN (...) */
#define WHERE_ROWID_RANGE    0x0002   /* rowid<EXPR and/or rowid>EXPR */
#define WHERE_COLUMN_EQ      0x0004   /* x=EXPR or x IN (...) */
#define WHERE_COLUMN_RANGE   0x0008   /* x<EXPR and/or x>EXPR */
#define WHERE_SCAN           0x0010   /* Do a full table scan */
#define WHERE_REVERSE        0x0020   /* Scan in reverse order */
#define WHERE_ORDERBY        0x0040   /* Output will appear in correct order */
#define WHERE_IDX_ONLY       0x0080   /* Use index only - omit table */
#define WHERE_TOP_LIMIT      0x0100   /* x<EXPR or x<=EXPR constraint */
#define WHERE_BTM_LIMIT      0x0200   /* x>EXPR or x>=EXPR constraint */
#define WHERE_USES_IN        0x0400   /* True if the IN operator is used */
#define WHERE_UNIQUE         0x0800   /* True if fully specifies a unique idx */

/*
** Find the best index for accessing a particular table.  Return the index,
** flags that describe how the index should be used, and the "score" for
** this index.
*/
static double bestIndex(
  Parse *pParse,              /* The parsing context */
  WhereClause *pWC,           /* The WHERE clause */
  struct SrcList_item *pSrc,  /* The FROM clause term to search */
  Bitmask notReady,           /* Mask of cursors that are not available */
  ExprList *pOrderBy,         /* The order by clause */
  Index **ppIndex,            /* Make *ppIndex point to the best index */
  int *pFlags                 /* Put flags describing this choice in *pFlags */
){
  WhereTerm *pTerm;
  Index *pProbe;
  Index *bestIdx = 0;
  double bestScore = 0.0;
  int bestFlags = 0;
  int iCur = pSrc->iCursor;
  int rev;

  /* Check for a rowid=EXPR or rowid IN (...) constraint
  */
  pTerm = findTerm(pWC, iCur, -1, notReady, WO_EQ|WO_IN, 0);
  if( pTerm ){
    *ppIndex = 0;
    if( pTerm->operator & WO_EQ ){
      *pFlags = WHERE_ROWID_EQ;
      if( pOrderBy ) *pFlags |= WHERE_ORDERBY;
      return 1.0e10;
    }else{
      *pFlags = WHERE_ROWID_EQ | WHERE_USES_IN;
      return 1.0e9;
    }
  }

  /* Check for constraints on a range of rowids
  */
  pTerm = findTerm(pWC, iCur, -1, notReady, WO_LT|WO_LE|WO_GT|WO_GE, 0);
  if( pTerm ){
    int flags;
    *ppIndex = 0;
    if( pTerm->operator & (WO_LT|WO_LE) ){
      flags = WHERE_ROWID_RANGE | WHERE_TOP_LIMIT;
      if( findTerm(pWC, iCur, -1, notReady, WO_GT|WO_GE, 0) ){
        flags |= WHERE_BTM_LIMIT;
      }
    }else{
      flags = WHERE_ROWID_RANGE | WHERE_BTM_LIMIT;
      if( findTerm(pWC, iCur, -1, notReady, WO_LT|WO_LE, 0) ){
        flags |= WHERE_TOP_LIMIT;
      }
    }
    if( pOrderBy && sortableByRowid(iCur, pOrderBy, &rev) ){
      flags |= WHERE_ORDERBY;
      if( rev ) flags |= WHERE_REVERSE;
    }
    bestScore = 99.0;
    bestFlags = flags;
  }

  /* Look at each index.
  */
  for(pProbe=pSrc->pTab->pIndex; pProbe; pProbe=pProbe->pNext){
    int i;
    int nEq;
    int usesIN = 0;
    int flags;
    double score = 0.0;

    /* Count the number of columns in the index that are satisfied
    ** by x=EXPR constraints or x IN (...) constraints.
    */
    for(i=0; i<pProbe->nColumn; i++){
      int j = pProbe->aiColumn[i];
      pTerm = findTerm(pWC, iCur, j, notReady, WO_EQ|WO_IN, pProbe);
      if( pTerm==0 ) break;
      if( pTerm->operator==WO_IN ){
        if( i==0 ) usesIN = 1;
        break;
      }
    }
    nEq = i + usesIN;
    score = i*100.0 + usesIN*50.0;

    /* The optimization type is RANGE if there are no == or IN constraints
    */
    if( usesIN ){
      flags = WHERE_COLUMN_EQ | WHERE_USES_IN;
    }else if( nEq ){
      flags = WHERE_COLUMN_EQ;
    }else{
      flags = WHERE_COLUMN_RANGE;
    }

    /* Check for a uniquely specified row
    */
#if 0
    if( nEq==pProbe->nColumn && pProbe->isUnique ){
      flags |= WHERE_UNIQUE;
    }
#endif

    /* Look for range constraints
    */
    if( !usesIN && nEq<pProbe->nColumn ){
      int j = pProbe->aiColumn[nEq];
      pTerm = findTerm(pWC, iCur, j, notReady, WO_LT|WO_LE|WO_GT|WO_GE, pProbe);
      if( pTerm ){
        score += 20.0;
        flags = WHERE_COLUMN_RANGE;
        if( pTerm->operator & (WO_LT|WO_LE) ){
          flags |= WHERE_TOP_LIMIT;
          if( findTerm(pWC, iCur, j, notReady, WO_GT|WO_GE, pProbe) ){
            flags |= WHERE_BTM_LIMIT;
            score += 20.0;
          }
        }else{
          flags |= WHERE_BTM_LIMIT;
          if( findTerm(pWC, iCur, j, notReady, WO_LT|WO_LE, pProbe) ){
            flags |= WHERE_TOP_LIMIT;
            score += 20;
          }
        }
      }
    }

    /* Add extra points if this index can be used to satisfy the ORDER BY
    ** clause
    */
    if( pOrderBy && !usesIN &&
        isSortingIndex(pParse, pProbe, pSrc->pTab, iCur, pOrderBy, nEq, &rev) ){
      flags |= WHERE_ORDERBY;
      score += 10.0;
      if( rev ) flags |= WHERE_REVERSE;
    }

    /* Check to see if we can get away with using just the index without
    ** ever reading the table.  If that is the case, then add one bonus
    ** point to the score.
    */
    if( score>0.0 && pSrc->colUsed < (((Bitmask)1)<<(BMS-1)) ){
      Bitmask m = pSrc->colUsed;
      int j;
      for(j=0; j<pProbe->nColumn; j++){
        int x = pProbe->aiColumn[j];
        if( x<BMS-1 ){
          m &= ~(((Bitmask)1)<<x);
        }
      }
      if( m==0 ){
        flags |= WHERE_IDX_ONLY;
        score += 5;
      }
    }

    /* If this index has achieved the best score so far, then use it.
    */
    if( score>bestScore ){
      bestIdx = pProbe;
      bestScore = score;
      bestFlags = flags;
    }
  }

  /* Disable sorting if we are coming out in rowid order
  */
  if( bestIdx==0 && pOrderBy && sortableByRowid(iCur, pOrderBy, &rev) ){
    bestFlags |= WHERE_ORDERBY;
    if( rev ) bestFlags |= WHERE_REVERSE;
  }


  /* Report the best result
  */
  *ppIndex = bestIdx;
  *pFlags = bestFlags;
  return bestScore;
}


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
** of the join.  Disabling is an optimization.  We would get the correct
** results if nothing were ever disabled, but joins might run a little
** slower.  The trick is to disable as much as we can without disabling
** too much.  If we disabled in (1), we'd get the wrong answer.
** See ticket #813.
*/
static void disableTerm(WhereLevel *pLevel, WhereTerm *pTerm){
  if( pTerm
      && (pTerm->flags & TERM_CODED)==0
      && (pLevel->iLeftJoin==0 || ExprHasProperty(pTerm->pExpr, EP_FromJoin))
  ){
    pTerm->flags |= TERM_CODED;
    if( pTerm->iPartner>=0 ){
      disableTerm(pLevel, &pTerm->pWC->a[pTerm->iPartner]);
    }
  }
}

/*
** Generate code that builds a probe for an index.  Details:
**
**    *  Check the top nColumn entries on the stack.  If any
**       of those entries are NULL, jump immediately to brk,
**       which is the loop exit, since no index entry will match
**       if any part of the key is NULL.
**
**    *  Construct a probe entry from the top nColumn entries in
**       the stack with affinities appropriate for index pIdx.
*/
static void buildIndexProbe(Vdbe *v, int nColumn, int brk, Index *pIdx){
  sqlite3VdbeAddOp(v, OP_NotNull, -nColumn, sqlite3VdbeCurrentAddr(v)+3);
  sqlite3VdbeAddOp(v, OP_Pop, nColumn, 0);
  sqlite3VdbeAddOp(v, OP_Goto, 0, brk);
  sqlite3VdbeAddOp(v, OP_MakeRecord, nColumn, 0);
  sqlite3IndexAffinityStr(v, pIdx);
}


/*
** Generate code for an equality term of the WHERE clause.  An equality
** term can be either X=expr  or X IN (...).   pTerm is the X.  
*/
static void codeEqualityTerm(
  Parse *pParse,      /* The parsing context */
  WhereTerm *pTerm,   /* The term of the WHERE clause to be coded */
  int brk,            /* Jump here to abandon the loop */
  WhereLevel *pLevel  /* When level of the FROM clause we are working on */
){
  Expr *pX = pTerm->pExpr;
  if( pX->op!=TK_IN ){
    assert( pX->op==TK_EQ );
    sqlite3ExprCode(pParse, pX->pRight);
#ifndef SQLITE_OMIT_SUBQUERY
  }else{
    int iTab;
    int *aIn;
    Vdbe *v = pParse->pVdbe;

    sqlite3CodeSubselect(pParse, pX);
    iTab = pX->iTable;
    sqlite3VdbeAddOp(v, OP_Rewind, iTab, brk);
    VdbeComment((v, "# %.*s", pX->span.n, pX->span.z));
    pLevel->nIn++;
    pLevel->aInLoop = aIn = sqliteRealloc(pLevel->aInLoop,
                                 sizeof(pLevel->aInLoop[0])*3*pLevel->nIn);
    if( aIn ){
      aIn += pLevel->nIn*3 - 3;
      aIn[0] = OP_Next;
      aIn[1] = iTab;
      aIn[2] = sqlite3VdbeAddOp(v, OP_Column, iTab, 0);
    }
#endif
  }
  disableTerm(pLevel, pTerm);
}

#ifdef SQLITE_TEST
/*
** The following variable holds a text description of query plan generated
** by the most recent call to sqlite3WhereBegin().  Each call to WhereBegin
** overwrites the previous.  This information is used for testing and
** analysis only.
*/
char sqlite3_query_plan[BMS*2*40];  /* Text of the join */
static int nQPlan = 0;              /* Next free slow in _query_plan[] */

#endif /* SQLITE_TEST */



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
** use of indices.
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
** *ppOrderBy is a pointer to the ORDER BY clause of a SELECT statement,
** if there is one.  If there is no ORDER BY clause or if this routine
** is called from an UPDATE or DELETE statement, then ppOrderBy is NULL.
**
** If an index can be used so that the natural output order of the table
** scan is correct for the ORDER BY clause, then that index is used and
** *ppOrderBy is set to NULL.  This is an optimization that prevents an
** unnecessary sort of the result set if an index appropriate for the
** ORDER BY clause already exists.
**
** If the where clause loops cannot be arranged to provide the correct
** output order, then the *ppOrderBy is unchanged.
*/
WhereInfo *sqlite3WhereBegin(
  Parse *pParse,        /* The parser context */
  SrcList *pTabList,    /* A list of all tables to be scanned */
  Expr *pWhere,         /* The WHERE clause */
  ExprList **ppOrderBy  /* An ORDER BY clause, or NULL */
){
  int i;                     /* Loop counter */
  WhereInfo *pWInfo;         /* Will become the return value of this function */
  Vdbe *v = pParse->pVdbe;   /* The virtual database engine */
  int brk, cont = 0;         /* Addresses used during code generation */
  Bitmask notReady;          /* Cursors that are not yet positioned */
  WhereTerm *pTerm;          /* A single term in the WHERE clause */
  ExprMaskSet maskSet;       /* The expression mask set */
  WhereClause wc;            /* The WHERE clause is divided into these terms */
  struct SrcList_item *pTabItem;  /* A single entry from pTabList */
  WhereLevel *pLevel;             /* A single level in the pWInfo list */
  int iFrom;                      /* First unused FROM clause element */
  WhereConstraint *aConstraint;   /* Information on constraints */

  /* The number of tables in the FROM clause is limited by the number of
  ** bits in a Bitmask 
  */
  if( pTabList->nSrc>BMS ){
    sqlite3ErrorMsg(pParse, "at most %d tables in a join", BMS);
    return 0;
  }

  /* Split the WHERE clause into separate subexpressions where each
  ** subexpression is separated by an AND operator.
  */
  initMaskSet(&maskSet);
  whereClauseInit(&wc, pParse);
  whereSplit(&wc, pWhere);
    
  /* Allocate and initialize the WhereInfo structure that will become the
  ** return value.
  */
  pWInfo = sqliteMalloc( sizeof(WhereInfo) + pTabList->nSrc*sizeof(WhereLevel));
  if( sqlite3_malloc_failed ){
    goto whereBeginNoMem;
  }
  pWInfo->pParse = pParse;
  pWInfo->pTabList = pTabList;
  pWInfo->iBreak = sqlite3VdbeMakeLabel(v);

  /* Special case: a WHERE clause that is constant.  Evaluate the
  ** expression and either jump over all of the code or fall thru.
  */
  if( pWhere && (pTabList->nSrc==0 || sqlite3ExprIsConstant(pWhere)) ){
    sqlite3ExprIfFalse(pParse, pWhere, pWInfo->iBreak, 1);
    pWhere = 0;
  }

  /* Analyze all of the subexpressions.  Note that exprAnalyze() might
  ** add new virtual terms onto the end of the WHERE clause.  We do not
  ** want to analyze these virtual terms, so start analyzing at the end
  ** and work forward so that they added virtual terms are never processed.
  */
  for(i=0; i<pTabList->nSrc; i++){
    createMask(&maskSet, pTabList->a[i].iCursor);
  }
  for(i=wc.nTerm-1; i>=0; i--){
    exprAnalyze(pTabList, &maskSet, &wc.a[i]);
  }
  aConstraint = sqliteMalloc( wc.nTerm*sizeof(aConstraint[0]) );
  if( aConstraint==0 && wc.nTerm>0 ){
    goto whereBeginNoMem;
  }

  /* Chose the best index to use for each table in the FROM clause.
  **
  ** This loop fills in the pWInfo->a[].pIdx and pWInfo->a[].flags fields
  ** with information
  ** Reorder tables if necessary in order to choose a good ordering.
  ** However, LEFT JOIN tables cannot be reordered.
  */
  notReady = ~(Bitmask)0;
  pTabItem = pTabList->a;
  pLevel = pWInfo->a;
  for(i=iFrom=0, pLevel=pWInfo->a; i<pTabList->nSrc; i++, pLevel++){
    Index *pIdx;                /* Index for FROM table at pTabItem */
    int flags;                  /* Flags asssociated with pIdx */
    double score;               /* The score for pIdx */
    int j;                      /* For looping over FROM tables */
    Index *pBest = 0;           /* The best index seen so far */
    int bestFlags = 0;          /* Flags associated with pBest */
    double bestScore = -1.0;    /* The score of pBest */
    int bestJ;                  /* The value of j */
    Bitmask m;                  /* Bitmask value for j or bestJ */

    for(j=iFrom, pTabItem=&pTabList->a[j]; j<pTabList->nSrc; j++, pTabItem++){
      m = getMask(&maskSet, pTabItem->iCursor);
      if( (m & notReady)==0 ){
        if( j==iFrom ) iFrom++;
        continue;
      }
      score = bestIndex(pParse, &wc, pTabItem, notReady,
                        (j==0 && ppOrderBy) ? *ppOrderBy : 0,
                        &pIdx, &flags);
      if( score>bestScore ){
        bestScore = score;
        pBest = pIdx;
        bestFlags = flags;
        bestJ = j;
      }
      if( (pTabItem->jointype & JT_LEFT)!=0
         || (j>0 && (pTabItem[-1].jointype & JT_LEFT)!=0)
      ){
        break;
      }
    }
    if( bestFlags & WHERE_ORDERBY ){
      *ppOrderBy = 0;
    }
    pLevel->flags = bestFlags;
    pLevel->pIdx = pBest;
    pLevel->aInLoop = 0;
    pLevel->nIn = 0;
    if( pBest ){
      pLevel->iIdxCur = pParse->nTab++;
    }else{
      pLevel->iIdxCur = -1;
    }
    notReady &= ~getMask(&maskSet, pTabList->a[bestJ].iCursor);
    pLevel->iFrom = bestJ;
  }

  /* Open all tables in the pTabList and any indices selected for
  ** searching those tables.
  */
  sqlite3CodeVerifySchema(pParse, -1); /* Insert the cookie verifier Goto */
  pLevel = pWInfo->a;
  for(i=0, pLevel=pWInfo->a; i<pTabList->nSrc; i++, pLevel++){
    Table *pTab;
    Index *pIx;
    int iIdxCur = pLevel->iIdxCur;

    pTabItem = &pTabList->a[pLevel->iFrom];
    pTab = pTabItem->pTab;
    if( pTab->isTransient || pTab->pSelect ) continue;
    if( (pLevel->flags & WHERE_IDX_ONLY)==0 ){
      sqlite3OpenTableForReading(v, pTabItem->iCursor, pTab);
    }
    pLevel->iTabCur = pTabItem->iCursor;
    if( (pIx = pLevel->pIdx)!=0 ){
      sqlite3VdbeAddOp(v, OP_Integer, pIx->iDb, 0);
      VdbeComment((v, "# %s", pIx->zName));
      sqlite3VdbeOp3(v, OP_OpenRead, iIdxCur, pIx->tnum,
                     (char*)&pIx->keyInfo, P3_KEYINFO);
    }
    if( (pLevel->flags & WHERE_IDX_ONLY)!=0 ){
      sqlite3VdbeAddOp(v, OP_SetNumColumns, iIdxCur, pIx->nColumn+1);
    }
    sqlite3CodeVerifySchema(pParse, pTab->iDb);
  }
  pWInfo->iTop = sqlite3VdbeCurrentAddr(v);

  /* Generate the code to do the search.  Each iteration of the for
  ** loop below generates code for a single nested loop of the VM
  ** program.
  */
  notReady = ~(Bitmask)0;
  for(i=0, pLevel=pWInfo->a; i<pTabList->nSrc; i++, pLevel++){
    int j;
    int iCur = pTabItem->iCursor;  /* The VDBE cursor for the table */
    Index *pIdx;       /* The index we will be using */
    int iIdxCur;       /* The VDBE cursor for the index */
    int omitTable;     /* True if we use the index only */
    int bRev;          /* True if we need to scan in reverse order */

    pTabItem = &pTabList->a[pLevel->iFrom];
    iCur = pTabItem->iCursor;
    pIdx = pLevel->pIdx;
    iIdxCur = pLevel->iIdxCur;
    bRev = (pLevel->flags & WHERE_REVERSE)!=0;
    omitTable = (pLevel->flags & WHERE_IDX_ONLY)!=0;

    /* Create labels for the "break" and "continue" instructions
    ** for the current loop.  Jump to brk to break out of a loop.
    ** Jump to cont to go immediately to the next iteration of the
    ** loop.
    */
    brk = pLevel->brk = sqlite3VdbeMakeLabel(v);
    cont = pLevel->cont = sqlite3VdbeMakeLabel(v);

    /* If this is the right table of a LEFT OUTER JOIN, allocate and
    ** initialize a memory cell that records if this table matches any
    ** row of the left table of the join.
    */
    if( pLevel->iFrom>0 && (pTabItem[-1].jointype & JT_LEFT)!=0 ){
      if( !pParse->nMem ) pParse->nMem++;
      pLevel->iLeftJoin = pParse->nMem++;
      sqlite3VdbeAddOp(v, OP_Null, 0, 0);
      sqlite3VdbeAddOp(v, OP_MemStore, pLevel->iLeftJoin, 1);
      VdbeComment((v, "# init LEFT JOIN no-match flag"));
    }

    if( pLevel->flags & WHERE_ROWID_EQ ){
      /* Case 1:  We can directly reference a single row using an
      **          equality comparison against the ROWID field.  Or
      **          we reference multiple rows using a "rowid IN (...)"
      **          construct.
      */
      pTerm = findTerm(&wc, iCur, -1, notReady, WO_EQ|WO_IN, 0);
      assert( pTerm!=0 );
      assert( pTerm->pExpr!=0 );
      assert( pTerm->leftCursor==iCur );
      assert( omitTable==0 );
      codeEqualityTerm(pParse, pTerm, brk, pLevel);
      sqlite3VdbeAddOp(v, OP_MustBeInt, 1, brk);
      sqlite3VdbeAddOp(v, OP_NotExists, iCur, brk);
      VdbeComment((v, "pk"));
      pLevel->op = OP_Noop;
    }else if( pLevel->flags & WHERE_COLUMN_EQ ){
      /* Case 2:  There is an index and all terms of the WHERE clause that
      **          refer to the index using the "==" or "IN" operators.
      */
      int start;
      int nColumn;

      /* For each column of the index, find the term of the WHERE clause that
      ** constraints that column.  If the WHERE clause term is X=expr, then
      ** generate code to evaluate expr and leave the result on the stack */
      for(j=0; 1; j++){
        int k = pIdx->aiColumn[j];
        pTerm = findTerm(&wc, iCur, k, notReady, WO_EQ|WO_IN, pIdx);
        if( pTerm==0 ) break;
        if( pTerm->operator==WO_IN && j>0 ) break;
        assert( (pTerm->flags & TERM_CODED)==0 );
        codeEqualityTerm(pParse, pTerm, brk, pLevel);
        if( pTerm->operator==WO_IN ){
          j++;
          break;
        }
      }
      nColumn = j;
      pLevel->iMem = pParse->nMem++;
      buildIndexProbe(v, nColumn, brk, pIdx);
      sqlite3VdbeAddOp(v, OP_MemStore, pLevel->iMem, 0);

      /* Generate code (1) to move to the first matching element of the table.
      ** Then generate code (2) that jumps to "brk" after the cursor is past
      ** the last matching element of the table.  The code (1) is executed
      ** once to initialize the search, the code (2) is executed before each
      ** iteration of the scan to see if the scan has finished. */
      if( bRev ){
        /* Scan in reverse order */
        sqlite3VdbeAddOp(v, OP_MoveLe, iIdxCur, brk);
        start = sqlite3VdbeAddOp(v, OP_MemLoad, pLevel->iMem, 0);
        sqlite3VdbeAddOp(v, OP_IdxLT, iIdxCur, brk);
        pLevel->op = OP_Prev;
      }else{
        /* Scan in the forward order */
        sqlite3VdbeAddOp(v, OP_MoveGe, iIdxCur, brk);
        start = sqlite3VdbeAddOp(v, OP_MemLoad, pLevel->iMem, 0);
        sqlite3VdbeOp3(v, OP_IdxGE, iIdxCur, brk, "+", P3_STATIC);
        pLevel->op = OP_Next;
      }
      sqlite3VdbeAddOp(v, OP_RowKey, iIdxCur, 0);
      sqlite3VdbeAddOp(v, OP_IdxIsNull, nColumn, cont);
      if( !omitTable ){
        sqlite3VdbeAddOp(v, OP_IdxRowid, iIdxCur, 0);
        sqlite3VdbeAddOp(v, OP_MoveGe, iCur, 0);
      }
      pLevel->p1 = iIdxCur;
      pLevel->p2 = start;
    }else if( pLevel->flags & WHERE_ROWID_RANGE ){
      /* Case 3:  We have an inequality comparison against the ROWID field.
      */
      int testOp = OP_Noop;
      int start;
      WhereTerm *pStart, *pEnd;

      assert( omitTable==0 );
      if( pLevel->flags & WHERE_BTM_LIMIT ){
        pStart = findTerm(&wc, iCur, -1, notReady, WO_GT|WO_GE, 0);
        assert( pStart!=0 );
      }else{
        pStart = 0;
      }
      if( pLevel->flags & WHERE_TOP_LIMIT ){
        pEnd = findTerm(&wc, iCur, -1, notReady, WO_LT|WO_LE, 0);
        assert( pEnd!=0 );
      }else{
        pEnd = 0;
      }
      assert( pStart!=0 || pEnd!=0 );
      if( bRev ){
        pTerm = pStart;
        pStart = pEnd;
        pEnd = pTerm;
      }
      if( pStart ){
        Expr *pX;
        pX = pStart->pExpr;
        assert( pX!=0 );
        assert( pStart->leftCursor==iCur );
        sqlite3ExprCode(pParse, pX->pRight);
        sqlite3VdbeAddOp(v, OP_ForceInt, pX->op==TK_LE || pX->op==TK_GT, brk);
        sqlite3VdbeAddOp(v, bRev ? OP_MoveLt : OP_MoveGe, iCur, brk);
        VdbeComment((v, "pk"));
        disableTerm(pLevel, pStart);
      }else{
        sqlite3VdbeAddOp(v, bRev ? OP_Last : OP_Rewind, iCur, brk);
      }
      if( pEnd ){
        Expr *pX;
        pX = pEnd->pExpr;
        assert( pX!=0 );
        assert( pEnd->leftCursor==iCur );
        sqlite3ExprCode(pParse, pX->pRight);
        pLevel->iMem = pParse->nMem++;
        sqlite3VdbeAddOp(v, OP_MemStore, pLevel->iMem, 1);
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
      if( testOp!=OP_Noop ){
        sqlite3VdbeAddOp(v, OP_Rowid, iCur, 0);
        sqlite3VdbeAddOp(v, OP_MemLoad, pLevel->iMem, 0);
        sqlite3VdbeAddOp(v, testOp, 'n', brk);
      }
    }else if( pLevel->flags & WHERE_COLUMN_RANGE ){
      /* Case 4: The WHERE clause term that refers to the right-most
      **         column of the index is an inequality.  For example, if
      **         the index is on (x,y,z) and the WHERE clause is of the
      **         form "x=5 AND y<10" then this case is used.  Only the
      **         right-most column can be an inequality - the rest must
      **         use the "==" operator.
      **
      **         This case is also used when there are no WHERE clause
      **         constraints but an index is selected anyway, in order
      **         to force the output order to conform to an ORDER BY.
      */
      int nEqColumn;
      int start;
      int leFlag=0, geFlag=0;
      int testOp;
      int topLimit = (pLevel->flags & WHERE_TOP_LIMIT)!=0;
      int btmLimit = (pLevel->flags & WHERE_BTM_LIMIT)!=0;

      /* Evaluate the equality constraints
      */
      for(j=0; 1; j++){
        int k = pIdx->aiColumn[j];
        pTerm = findTerm(&wc, iCur, k, notReady, WO_EQ, pIdx);
        if( pTerm==0 ) break;
        assert( (pTerm->flags & TERM_CODED)==0 );
        sqlite3ExprCode(pParse, pTerm->pExpr->pRight);
        disableTerm(pLevel, pTerm);
      }
      nEqColumn = j;

      /* Duplicate the equality term values because they will all be
      ** used twice: once to make the termination key and once to make the
      ** start key.
      */
      for(j=0; j<nEqColumn; j++){
        sqlite3VdbeAddOp(v, OP_Dup, nEqColumn-1, 0);
      }

      /* Generate the termination key.  This is the key value that
      ** will end the search.  There is no termination key if there
      ** are no equality terms and no "X<..." term.
      **
      ** 2002-Dec-04: On a reverse-order scan, the so-called "termination"
      ** key computed here really ends up being the start key.
      */
      if( topLimit ){
        Expr *pX;
        int k = pIdx->aiColumn[j];
        pTerm = findTerm(&wc, iCur, k, notReady, WO_LT|WO_LE, pIdx);
        assert( pTerm!=0 );
        pX = pTerm->pExpr;
        assert( (pTerm->flags & TERM_CODED)==0 );
        sqlite3ExprCode(pParse, pX->pRight);
        leFlag = pX->op==TK_LE;
        disableTerm(pLevel, pTerm);
        testOp = OP_IdxGE;
      }else{
        testOp = nEqColumn>0 ? OP_IdxGE : OP_Noop;
        leFlag = 1;
      }
      if( testOp!=OP_Noop ){
        int nCol = nEqColumn + topLimit;
        pLevel->iMem = pParse->nMem++;
        buildIndexProbe(v, nCol, brk, pIdx);
        if( bRev ){
          int op = leFlag ? OP_MoveLe : OP_MoveLt;
          sqlite3VdbeAddOp(v, op, iIdxCur, brk);
        }else{
          sqlite3VdbeAddOp(v, OP_MemStore, pLevel->iMem, 1);
        }
      }else if( bRev ){
        sqlite3VdbeAddOp(v, OP_Last, iIdxCur, brk);
      }

      /* Generate the start key.  This is the key that defines the lower
      ** bound on the search.  There is no start key if there are no
      ** equality terms and if there is no "X>..." term.  In
      ** that case, generate a "Rewind" instruction in place of the
      ** start key search.
      **
      ** 2002-Dec-04: In the case of a reverse-order search, the so-called
      ** "start" key really ends up being used as the termination key.
      */
      if( btmLimit ){
        Expr *pX;
        int k = pIdx->aiColumn[j];
        pTerm = findTerm(&wc, iCur, k, notReady, WO_GT|WO_GE, pIdx);
        assert( pTerm!=0 );
        pX = pTerm->pExpr;
        assert( (pTerm->flags & TERM_CODED)==0 );
        sqlite3ExprCode(pParse, pX->pRight);
        geFlag = pX->op==TK_GE;
        disableTerm(pLevel, pTerm);
      }else{
        geFlag = 1;
      }
      if( nEqColumn>0 || btmLimit ){
        int nCol = nEqColumn + btmLimit;
        buildIndexProbe(v, nCol, brk, pIdx);
        if( bRev ){
          pLevel->iMem = pParse->nMem++;
          sqlite3VdbeAddOp(v, OP_MemStore, pLevel->iMem, 1);
          testOp = OP_IdxLT;
        }else{
          int op = geFlag ? OP_MoveGe : OP_MoveGt;
          sqlite3VdbeAddOp(v, op, iIdxCur, brk);
        }
      }else if( bRev ){
        testOp = OP_Noop;
      }else{
        sqlite3VdbeAddOp(v, OP_Rewind, iIdxCur, brk);
      }

      /* Generate the the top of the loop.  If there is a termination
      ** key we have to test for that key and abort at the top of the
      ** loop.
      */
      start = sqlite3VdbeCurrentAddr(v);
      if( testOp!=OP_Noop ){
        sqlite3VdbeAddOp(v, OP_MemLoad, pLevel->iMem, 0);
        sqlite3VdbeAddOp(v, testOp, iIdxCur, brk);
        if( (leFlag && !bRev) || (!geFlag && bRev) ){
          sqlite3VdbeChangeP3(v, -1, "+", P3_STATIC);
        }
      }
      sqlite3VdbeAddOp(v, OP_RowKey, iIdxCur, 0);
      sqlite3VdbeAddOp(v, OP_IdxIsNull, nEqColumn + topLimit, cont);
      if( !omitTable ){
        sqlite3VdbeAddOp(v, OP_IdxRowid, iIdxCur, 0);
        sqlite3VdbeAddOp(v, OP_MoveGe, iCur, 0);
      }

      /* Record the instruction used to terminate the loop.
      */
      pLevel->op = bRev ? OP_Prev : OP_Next;
      pLevel->p1 = iIdxCur;
      pLevel->p2 = start;
    }else{
      /* Case 5:  There is no usable index.  We must do a complete
      **          scan of the entire table.
      */
      int opRewind;

      assert( omitTable==0 );
      if( bRev ){
        opRewind = OP_Last;
        pLevel->op = OP_Prev;
      }else{
        opRewind = OP_Rewind;
        pLevel->op = OP_Next;
      }
      pLevel->p1 = iCur;
      pLevel->p2 = 1 + sqlite3VdbeAddOp(v, opRewind, iCur, brk);
    }
    notReady &= ~getMask(&maskSet, iCur);

    /* Insert code to test every subexpression that can be completely
    ** computed using the current set of tables.
    */
    for(pTerm=wc.a, j=wc.nTerm; j>0; j--, pTerm++){
      Expr *pE;
      if( pTerm->flags & (TERM_VIRTUAL|TERM_CODED) ) continue;
      if( (pTerm->prereqAll & notReady)!=0 ) continue;
      pE = pTerm->pExpr;
      assert( pE!=0 );
      if( pLevel->iLeftJoin && !ExprHasProperty(pE, EP_FromJoin) ){
        continue;
      }
      sqlite3ExprIfFalse(pParse, pE, cont, 1);
      pTerm->flags |= TERM_CODED;
    }

    /* For a LEFT OUTER JOIN, generate code that will record the fact that
    ** at least one row of the right table has matched the left table.  
    */
    if( pLevel->iLeftJoin ){
      pLevel->top = sqlite3VdbeCurrentAddr(v);
      sqlite3VdbeAddOp(v, OP_Integer, 1, 0);
      sqlite3VdbeAddOp(v, OP_MemStore, pLevel->iLeftJoin, 1);
      VdbeComment((v, "# record LEFT JOIN hit"));
      for(pTerm=wc.a, j=0; j<wc.nTerm; j++, pTerm++){
        if( pTerm->flags & (TERM_VIRTUAL|TERM_CODED) ) continue;
        if( (pTerm->prereqAll & notReady)!=0 ) continue;
        assert( pTerm->pExpr );
        sqlite3ExprIfFalse(pParse, pTerm->pExpr, cont, 1);
        pTerm->flags |= TERM_CODED;
      }
    }
  }

#ifdef SQLITE_TEST  /* For testing and debugging use only */
  /* Record in the query plan information about the current table
  ** and the index used to access it (if any).  If the table itself
  ** is not used, its name is just '{}'.  If no index is used
  ** the index is listed as "{}".  If the primary key is used the
  ** index name is '*'.
  */
  for(i=0; i<pTabList->nSrc; i++){
    char *z;
    int n;
    pLevel = &pWInfo->a[i];
    pTabItem = &pTabList->a[pLevel->iFrom];
    z = pTabItem->zAlias;
    if( z==0 ) z = pTabItem->pTab->zName;
    n = strlen(z);
    if( n+nQPlan < sizeof(sqlite3_query_plan)-10 ){
      if( pLevel->flags & WHERE_IDX_ONLY ){
        strcpy(&sqlite3_query_plan[nQPlan], "{}");
        nQPlan += 2;
      }else{
        strcpy(&sqlite3_query_plan[nQPlan], z);
        nQPlan += n;
      }
      sqlite3_query_plan[nQPlan++] = ' ';
    }
    if( pLevel->flags & (WHERE_ROWID_EQ|WHERE_ROWID_RANGE) ){
      strcpy(&sqlite3_query_plan[nQPlan], "* ");
      nQPlan += 2;
    }else if( pLevel->pIdx==0 ){
      strcpy(&sqlite3_query_plan[nQPlan], "{} ");
      nQPlan += 3;
    }else{
      n = strlen(pLevel->pIdx->zName);
      if( n+nQPlan < sizeof(sqlite3_query_plan)-2 ){
        strcpy(&sqlite3_query_plan[nQPlan], pLevel->pIdx->zName);
        nQPlan += n;
        sqlite3_query_plan[nQPlan++] = ' ';
      }
    }
  }
  while( nQPlan>0 && sqlite3_query_plan[nQPlan-1]==' ' ){
    sqlite3_query_plan[--nQPlan] = 0;
  }
  sqlite3_query_plan[nQPlan] = 0;
  nQPlan = 0;
#endif /* SQLITE_TEST // Testing and debugging use only */

  /* Record the continuation address in the WhereInfo structure.  Then
  ** clean up and return.
  */
  pWInfo->iContinue = cont;
  whereClauseClear(&wc);
  sqliteFree(aConstraint);
  return pWInfo;

  /* Jump here if malloc fails */
whereBeginNoMem:
  whereClauseClear(&wc);
  sqliteFree(pWInfo);
  return 0;
}

/*
** Generate the end of the WHERE loop.  See comments on 
** sqlite3WhereBegin() for additional information.
*/
void sqlite3WhereEnd(WhereInfo *pWInfo){
  Vdbe *v = pWInfo->pParse->pVdbe;
  int i;
  WhereLevel *pLevel;
  SrcList *pTabList = pWInfo->pTabList;

  /* Generate loop termination code.
  */
  for(i=pTabList->nSrc-1; i>=0; i--){
    pLevel = &pWInfo->a[i];
    sqlite3VdbeResolveLabel(v, pLevel->cont);
    if( pLevel->op!=OP_Noop ){
      sqlite3VdbeAddOp(v, pLevel->op, pLevel->p1, pLevel->p2);
    }
    sqlite3VdbeResolveLabel(v, pLevel->brk);
    if( pLevel->nIn ){
      int *a;
      int j;
      for(j=pLevel->nIn, a=&pLevel->aInLoop[j*3-3]; j>0; j--, a-=3){
        sqlite3VdbeAddOp(v, a[0], a[1], a[2]);
      }
      sqliteFree(pLevel->aInLoop);
    }
    if( pLevel->iLeftJoin ){
      int addr;
      addr = sqlite3VdbeAddOp(v, OP_MemLoad, pLevel->iLeftJoin, 0);
      sqlite3VdbeAddOp(v, OP_NotNull, 1, addr+4 + (pLevel->iIdxCur>=0));
      sqlite3VdbeAddOp(v, OP_NullRow, pTabList->a[i].iCursor, 0);
      if( pLevel->iIdxCur>=0 ){
        sqlite3VdbeAddOp(v, OP_NullRow, pLevel->iIdxCur, 0);
      }
      sqlite3VdbeAddOp(v, OP_Goto, 0, pLevel->top);
    }
  }

  /* The "break" point is here, just past the end of the outer loop.
  ** Set it.
  */
  sqlite3VdbeResolveLabel(v, pWInfo->iBreak);

  /* Close all of the cursors that were opened by sqlite3WhereBegin.
  */
  for(i=0, pLevel=pWInfo->a; i<pTabList->nSrc; i++, pLevel++){
    struct SrcList_item *pTabItem = &pTabList->a[pLevel->iFrom];
    Table *pTab = pTabItem->pTab;
    assert( pTab!=0 );
    if( pTab->isTransient || pTab->pSelect ) continue;
    if( (pLevel->flags & WHERE_IDX_ONLY)==0 ){
      sqlite3VdbeAddOp(v, OP_Close, pTabItem->iCursor, 0);
    }
    if( pLevel->pIdx!=0 ){
      sqlite3VdbeAddOp(v, OP_Close, pLevel->iIdxCur, 0);
    }

    /* Make cursor substitutions for cases where we want to use
    ** just the index and never reference the table.
    ** 
    ** Calls to the code generator in between sqlite3WhereBegin and
    ** sqlite3WhereEnd will have created code that references the table
    ** directly.  This loop scans all that code looking for opcodes
    ** that reference the table and converts them into opcodes that
    ** reference the index.
    */
    if( pLevel->flags & WHERE_IDX_ONLY ){
      int i, j, last;
      VdbeOp *pOp;
      Index *pIdx = pLevel->pIdx;

      assert( pIdx!=0 );
      pOp = sqlite3VdbeGetOp(v, pWInfo->iTop);
      last = sqlite3VdbeCurrentAddr(v);
      for(i=pWInfo->iTop; i<last; i++, pOp++){
        if( pOp->p1!=pLevel->iTabCur ) continue;
        if( pOp->opcode==OP_Column ){
          pOp->p1 = pLevel->iIdxCur;
          for(j=0; j<pIdx->nColumn; j++){
            if( pOp->p2==pIdx->aiColumn[j] ){
              pOp->p2 = j;
              break;
            }
          }
        }else if( pOp->opcode==OP_Rowid ){
          pOp->p1 = pLevel->iIdxCur;
          pOp->opcode = OP_IdxRowid;
        }else if( pOp->opcode==OP_NullRow ){
          pOp->opcode = OP_Noop;
        }
      }
    }
  }

  /* Final cleanup
  */
  sqliteFree(pWInfo);
  return;
}
