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
** the WHERE clause of SQL statements.  Also found here are subroutines
** to generate VDBE code to evaluate expressions.
**
** $Id: where.c,v 1.37 2002/02/23 02:32:10 drh Exp $
*/
#include "sqliteInt.h"

/*
** The query generator uses an array of instances of this structure to
** help it analyze the subexpressions of the WHERE clause.  Each WHERE
** clause subexpression is separated from the others by an AND operator.
*/
typedef struct ExprInfo ExprInfo;
struct ExprInfo {
  Expr *p;                /* Pointer to the subexpression */
  int indexable;          /* True if this subexprssion is usable by an index */
  int idxLeft;            /* p->pLeft is a column in this table number. -1 if
                          ** p->pLeft is not the column of any table */
  int idxRight;           /* p->pRight is a column in this table number. -1 if
                          ** p->pRight is not the column of any table */
  unsigned prereqLeft;    /* Tables referenced by p->pLeft */
  unsigned prereqRight;   /* Tables referenced by p->pRight */
};

/*
** Determine the number of elements in an array.
*/
#define ARRAYSIZE(X)  (sizeof(X)/sizeof(X[0]))

/*
** This routine is used to divide the WHERE expression into subexpressions
** separated by the AND operator.
**
** aSlot[] is an array of subexpressions structures.
** There are nSlot spaces left in this array.  This routine attempts to
** split pExpr into subexpressions and fills aSlot[] with those subexpressions.
** The return value is the number of slots filled.
*/
static int exprSplit(int nSlot, ExprInfo *aSlot, Expr *pExpr){
  int cnt = 0;
  if( pExpr==0 || nSlot<1 ) return 0;
  if( nSlot==1 || pExpr->op!=TK_AND ){
    aSlot[0].p = pExpr;
    return 1;
  }
  if( pExpr->pLeft->op!=TK_AND ){
    aSlot[0].p = pExpr->pLeft;
    cnt = 1 + exprSplit(nSlot-1, &aSlot[1], pExpr->pRight);
  }else{
    cnt = exprSplit(nSlot, aSlot, pExpr->pRight);
    cnt += exprSplit(nSlot-cnt, &aSlot[cnt], pExpr->pLeft);
  }
  return cnt;
}

/*
** This routine walks (recursively) an expression tree and generates
** a bitmask indicating which tables are used in that expression
** tree.  Bit 0 of the mask is set if table 0 is used.  But 1 is set
** if table 1 is used.  And so forth.
**
** In order for this routine to work, the calling function must have
** previously invoked sqliteExprResolveIds() on the expression.  See
** the header comment on that routine for additional information.
**
** "base" is the cursor number (the value of the iTable field) that
** corresponds to the first entry in the table list.  This is the
** same as pParse->nTab.
*/
static int exprTableUsage(int base, Expr *p){
  unsigned int mask = 0;
  if( p==0 ) return 0;
  if( p->op==TK_COLUMN ){
    return 1<< (p->iTable - base);
  }
  if( p->pRight ){
    mask = exprTableUsage(base, p->pRight);
  }
  if( p->pLeft ){
    mask |= exprTableUsage(base, p->pLeft);
  }
  return mask;
}

/*
** Return TRUE if the given operator is one of the operators that is
** allowed for an indexable WHERE clause.  The allowed operators are
** "=", "<", ">", "<=", and ">=".
*/
static int allowedOp(int op){
  switch( op ){
    case TK_LT:
    case TK_LE:
    case TK_GT:
    case TK_GE:
    case TK_EQ:
      return 1;
    default:
      return 0;
  }
}

/*
** The input to this routine is an ExprInfo structure with only the
** "p" field filled in.  The job of this routine is to analyze the
** subexpression and populate all the other fields of the ExprInfo
** structure.
**
** "base" is the cursor number (the value of the iTable field) that
** corresponds to the first entry in the table list.  This is the
** same as pParse->nTab.
*/
static void exprAnalyze(int base, ExprInfo *pInfo){
  Expr *pExpr = pInfo->p;
  pInfo->prereqLeft = exprTableUsage(base, pExpr->pLeft);
  pInfo->prereqRight = exprTableUsage(base, pExpr->pRight);
  pInfo->indexable = 0;
  pInfo->idxLeft = -1;
  pInfo->idxRight = -1;
  if( allowedOp(pExpr->op) && (pInfo->prereqRight & pInfo->prereqLeft)==0 ){
    if( pExpr->pRight->op==TK_COLUMN ){
      pInfo->idxRight = pExpr->pRight->iTable - base;
      pInfo->indexable = 1;
    }
    if( pExpr->pLeft->op==TK_COLUMN ){
      pInfo->idxLeft = pExpr->pLeft->iTable - base;
      pInfo->indexable = 1;
    }
  }
}

/*
** Generating the beginning of the loop used for WHERE clause processing.
** The return value is a pointer to an (opaque) structure that contains
** information needed to terminate the loop.  Later, the calling routine
** should invoke sqliteWhereEnd() with the return value of this function
** in order to complete the WHERE clause processing.
**
** If an error occurs, this routine returns NULL.
*/
WhereInfo *sqliteWhereBegin(
  Parse *pParse,       /* The parser context */
  IdList *pTabList,    /* A list of all tables */
  Expr *pWhere,        /* The WHERE clause */
  int pushKey          /* If TRUE, leave the table key on the stack */
){
  int i;                     /* Loop counter */
  WhereInfo *pWInfo;         /* Will become the return value of this function */
  Vdbe *v = pParse->pVdbe;   /* The virtual database engine */
  int brk, cont;             /* Addresses used during code generation */
  int *aOrder;         /* Order in which pTabList entries are searched */
  int nExpr;           /* Number of subexpressions in the WHERE clause */
  int loopMask;        /* One bit set for each outer loop */
  int haveKey;         /* True if KEY is on the stack */
  int base;            /* First available index for OP_Open opcodes */
  int nCur;            /* Next unused cursor number */
  int aDirect[32];     /* If TRUE, then index this table using ROWID */
  int iDirectEq[32];   /* Term of the form ROWID==X for the N-th table */
  int iDirectLt[32];   /* Term of the form ROWID<X or ROWID<=X */
  int iDirectGt[32];   /* Term of the form ROWID>X or ROWID>=X */
  ExprInfo aExpr[50];  /* The WHERE clause is divided into these expressions */

  /* Allocate space for aOrder[] and aiMem[]. */
  aOrder = sqliteMalloc( sizeof(int) * pTabList->nId );

  /* Allocate and initialize the WhereInfo structure that will become the
  ** return value.
  */
  pWInfo = sqliteMalloc( sizeof(WhereInfo) + pTabList->nId*sizeof(WhereLevel) );
  if( sqlite_malloc_failed ){
    sqliteFree(aOrder);
    sqliteFree(pWInfo);
    return 0;
  }
  pWInfo->pParse = pParse;
  pWInfo->pTabList = pTabList;
  base = pWInfo->base = pParse->nTab;
  nCur = base + pTabList->nId;
  pParse->nTab += nCur*2;

  /* Split the WHERE clause into as many as 32 separate subexpressions
  ** where each subexpression is separated by an AND operator.  Any additional
  ** subexpressions are attached in the aExpr[32] and will not enter
  ** into the query optimizer computations.  32 is chosen as the cutoff
  ** since that is the number of bits in an integer that we use for an
  ** expression-used mask.  
  */
  memset(aExpr, 0, sizeof(aExpr));
  nExpr = exprSplit(ARRAYSIZE(aExpr), aExpr, pWhere);

  /* Analyze all of the subexpressions.
  */
  for(i=0; i<nExpr; i++){
    exprAnalyze(base, &aExpr[i]);
  }

  /* Figure out a good nesting order for the tables.  aOrder[0] will
  ** be the index in pTabList of the outermost table.  aOrder[1] will
  ** be the first nested loop and so on.  aOrder[pTabList->nId-1] will
  ** be the innermost loop.
  **
  ** Someday will put in a good algorithm here to reorder the loops
  ** for an effiecient query.  But for now, just use whatever order the
  ** tables appear in in the pTabList.
  */
  for(i=0; i<pTabList->nId; i++){
    aOrder[i] = i;
  }

  /* Figure out what index to use (if any) for each nested loop.
  ** Make pWInfo->a[i].pIdx point to the index to use for the i-th nested
  ** loop where i==0 is the outer loop and i==pTabList->nId-1 is the inner
  ** loop. 
  **
  ** If terms exist that use the ROWID of any table, then set the
  ** iDirectEq[], iDirectLt[], or iDirectGt[] elements for that table
  ** to the index of the term containing the ROWID.  We always prefer
  ** to use a ROWID which can directly access a table rather than an
  ** index which requires reading an index first to get the rowid then
  ** doing a second read of the actual database table.
  **
  ** Actually, if there are more than 32 tables in the join, only the
  ** first 32 tables are candidates for indices.  This is (again) due
  ** to the limit of 32 bits in an integer bitmask.
  */
  loopMask = 0;
  for(i=0; i<pTabList->nId && i<ARRAYSIZE(aDirect); i++){
    int j;
    int idx = aOrder[i];
    Table *pTab = pTabList->a[idx].pTab;
    Index *pIdx;
    Index *pBestIdx = 0;
    int bestScore = 0;

    /* Check to see if there is an expression that uses only the
    ** ROWID field of this table.  For terms of the form ROWID==expr
    ** set iDirectEq[i] to the index of the term.  For terms of the
    ** form ROWID<expr or ROWID<=expr set iDirectLt[i] to the term index.
    ** For terms like ROWID>expr or ROWID>=expr set iDirectGt[i].
    */
    iDirectEq[i] = -1;
    iDirectLt[i] = -1;
    iDirectGt[i] = -1;
    for(j=0; j<nExpr; j++){
      if( aExpr[j].idxLeft==idx && aExpr[j].p->pLeft->iColumn<0
            && (aExpr[j].prereqRight & loopMask)==aExpr[j].prereqRight ){
        switch( aExpr[j].p->op ){
          case TK_EQ: iDirectEq[i] = j; break;
          case TK_LE:
          case TK_LT: iDirectLt[i] = j; break;
          case TK_GE:
          case TK_GT: iDirectGt[i] = j;  break;
        }
      }
      if( aExpr[j].idxRight==idx && aExpr[j].p->pRight->iColumn<0
            && (aExpr[j].prereqLeft & loopMask)==aExpr[j].prereqLeft ){
        switch( aExpr[j].p->op ){
          case TK_EQ: iDirectEq[i] = j;  break;
          case TK_LE:
          case TK_LT: iDirectGt[i] = j;  break;
          case TK_GE:
          case TK_GT: iDirectLt[i] = j;  break;
        }
      }
    }
    if( iDirectEq[i]>=0 ){
      loopMask |= 1<<idx;
      pWInfo->a[i].pIdx = 0;
      continue;
    }

    /* Do a search for usable indices.  Leave pBestIdx pointing to
    ** the "best" index.  pBestIdx is left set to NULL if no indices
    ** are usable.
    **
    ** The best index is determined as follows.  For each of the
    ** left-most terms that is fixed by an equality operator, add
    ** 4 to the score.  The right-most term of the index may be
    ** constrained by an inequality.  Add 1 if for an "x<..." constraint
    ** and add 2 for an "x>..." constraint.  Chose the index that
    ** gives the best score.
    **
    ** This scoring system is designed so that the score can later be
    ** used to determine how the index is used.  If the score&3 is 0
    ** then all constraints are equalities.  If score&1 is not 0 then
    ** there is an inequality used as a termination key.  (ex: "x<...")
    ** If score&2 is not 0 then there is an inequality used as the
    ** start key.  (ex: "x>...");
    */
    for(pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext){
      int eqMask = 0;  /* Index columns covered by an x=... constraint */
      int ltMask = 0;  /* Index columns covered by an x<... constraint */
      int gtMask = 0;  /* Index columns covered by an x>... constraing */
      int nEq, m, score;

      if( pIdx->isDropped ) continue;   /* Ignore dropped indices */
      if( pIdx->nColumn>32 ) continue;  /* Ignore indices too many columns */
      for(j=0; j<nExpr; j++){
        if( aExpr[j].idxLeft==idx 
             && (aExpr[j].prereqRight & loopMask)==aExpr[j].prereqRight ){
          int iColumn = aExpr[j].p->pLeft->iColumn;
          int k;
          for(k=0; k<pIdx->nColumn; k++){
            if( pIdx->aiColumn[k]==iColumn ){
              switch( aExpr[j].p->op ){
                case TK_EQ: {
                  eqMask |= 1<<k;
                  break;
                }
                case TK_LE:
                case TK_LT: {
                  ltMask |= 1<<k;
                  break;
                }
                case TK_GE:
                case TK_GT: {
                  gtMask |= 1<<k;
                  break;
                }
                default: {
                  /* CANT_HAPPEN */
                  assert( 0 );
                  break;
                }
              }
              break;
            }
          }
        }
        if( aExpr[j].idxRight==idx 
             && (aExpr[j].prereqLeft & loopMask)==aExpr[j].prereqLeft ){
          int iColumn = aExpr[j].p->pRight->iColumn;
          int k;
          for(k=0; k<pIdx->nColumn; k++){
            if( pIdx->aiColumn[k]==iColumn ){
              switch( aExpr[j].p->op ){
                case TK_EQ: {
                  eqMask |= 1<<k;
                  break;
                }
                case TK_LE:
                case TK_LT: {
                  gtMask |= 1<<k;
                  break;
                }
                case TK_GE:
                case TK_GT: {
                  ltMask |= 1<<k;
                  break;
                }
                default: {
                  /* CANT_HAPPEN */
                  assert( 0 );
                  break;
                }
              }
              break;
            }
          }
        }
      }
      for(nEq=0; nEq<pIdx->nColumn; nEq++){
        m = (1<<(nEq+1))-1;
        if( (m & eqMask)!=m ) break;
      }
      score = nEq*4;
      m = 1<<nEq;
      if( m & ltMask ) score++;
      if( m & gtMask ) score+=2;
      if( score>bestScore ){
        pBestIdx = pIdx;
        bestScore = score;
      }
    }
    pWInfo->a[i].pIdx = pBestIdx;
    pWInfo->a[i].score = bestScore;
    loopMask |= 1<<idx;
    if( pBestIdx ){
      pWInfo->a[i].iCur = nCur++;
    }
  }

  /* Open all tables in the pTabList and all indices used by those tables.
  */
  for(i=0; i<pTabList->nId; i++){
    int openOp;
    Table *pTab;

    pTab = pTabList->a[i].pTab;
    if( pTab->isTransient || pTab->pSelect ) continue;
    openOp = pTab->isTemp ? OP_OpenAux : OP_Open;
    sqliteVdbeAddOp(v, openOp, base+i, pTab->tnum);
    sqliteVdbeChangeP3(v, -1, pTab->zName, P3_STATIC);
    if( i==0 && !pParse->schemaVerified &&
          (pParse->db->flags & SQLITE_InTrans)==0 ){
      sqliteVdbeAddOp(v, OP_VerifyCookie, pParse->db->schema_cookie, 0);
      pParse->schemaVerified = 1;
    }
    if( pWInfo->a[i].pIdx!=0 ){
      sqliteVdbeAddOp(v, openOp, pWInfo->a[i].iCur, pWInfo->a[i].pIdx->tnum);
      sqliteVdbeChangeP3(v, -1, pWInfo->a[i].pIdx->zName, P3_STATIC);
    }
  }

  /* Generate the code to do the search
  */
  loopMask = 0;
  pWInfo->iBreak = sqliteVdbeMakeLabel(v);
  for(i=0; i<pTabList->nId; i++){
    int j, k;
    int idx = aOrder[i];
    Index *pIdx;
    WhereLevel *pLevel = &pWInfo->a[i];

    pIdx = pLevel->pIdx;
    if( i<ARRAYSIZE(iDirectEq) && iDirectEq[i]>=0 ){
      /* Case 1:  We can directly reference a single row using an
      **          equality comparison against the ROWID field.
      */
      k = iDirectEq[i];
      assert( k<nExpr );
      assert( aExpr[k].p!=0 );
      assert( aExpr[k].idxLeft==idx || aExpr[k].idxRight==idx );
      if( aExpr[k].idxLeft==idx ){
        sqliteExprCode(pParse, aExpr[k].p->pRight);
      }else{
        sqliteExprCode(pParse, aExpr[k].p->pLeft);
      }
      aExpr[k].p = 0;
      brk = pLevel->brk = sqliteVdbeMakeLabel(v);
      cont = pLevel->cont = brk;
      sqliteVdbeAddOp(v, OP_MustBeInt, 0, brk);
      if( i==pTabList->nId-1 && pushKey ){
        /* Note: The OP_Dup below will cause the recno to be left on the
        ** stack if the record does not exists and the OP_NotExists jump is
        ** taken.  This violates a general rule of the VDBE that you should
        ** never leave values on the stack in order to avoid a stack overflow.
        ** But in this case, the OP_Dup will never happen inside of a loop,
        ** because the pushKey flag is only true for UPDATE and DELETE, not
        ** for SELECT, and nested loops only occur on a SELECT.
        ** So it is safe to leave the recno on the stack.
        */
        haveKey = 1;
        sqliteVdbeAddOp(v, OP_Dup, 0, 0);
      }else{
        haveKey = 0;
      }
      sqliteVdbeAddOp(v, OP_NotExists, base+idx, brk);
      pLevel->op = OP_Noop;
    }else if( pIdx!=0 && pLevel->score%4==0 ){
      /* Case 2:  All index constraints are equality operators.
      */
      int start;
      int testOp;
      int nColumn = pLevel->score/4;
      for(j=0; j<nColumn; j++){
        for(k=0; k<nExpr; k++){
          if( aExpr[k].p==0 ) continue;
          if( aExpr[k].idxLeft==idx 
             && aExpr[k].p->op==TK_EQ
             && (aExpr[k].prereqRight & loopMask)==aExpr[k].prereqRight 
             && aExpr[k].p->pLeft->iColumn==pIdx->aiColumn[j]
          ){
            sqliteExprCode(pParse, aExpr[k].p->pRight);
            aExpr[k].p = 0;
            break;
          }
          if( aExpr[k].idxRight==idx 
             && aExpr[k].p->op==TK_EQ
             && (aExpr[k].prereqLeft & loopMask)==aExpr[k].prereqLeft
             && aExpr[k].p->pRight->iColumn==pIdx->aiColumn[j]
          ){
            sqliteExprCode(pParse, aExpr[k].p->pLeft);
            aExpr[k].p = 0;
            break;
          }
        }
      }
      pLevel->iMem = pParse->nMem++;
      brk = pLevel->brk = sqliteVdbeMakeLabel(v);
      cont = pLevel->cont = sqliteVdbeMakeLabel(v);
      sqliteVdbeAddOp(v, OP_MakeKey, nColumn, 0);
      if( nColumn==pIdx->nColumn ){
        sqliteVdbeAddOp(v, OP_MemStore, pLevel->iMem, 0);
        testOp = OP_IdxGT;
      }else{
        sqliteVdbeAddOp(v, OP_Dup, 0, 0);
        sqliteVdbeAddOp(v, OP_IncrKey, 0, 0);
        sqliteVdbeAddOp(v, OP_MemStore, pLevel->iMem, 1);
        testOp = OP_IdxGE;
      }
      sqliteVdbeAddOp(v, OP_MoveTo, pLevel->iCur, brk);
      start = sqliteVdbeAddOp(v, OP_MemLoad, pLevel->iMem, 0);
      sqliteVdbeAddOp(v, testOp, pLevel->iCur, brk);
      sqliteVdbeAddOp(v, OP_IdxRecno, pLevel->iCur, 0);
      if( i==pTabList->nId-1 && pushKey ){
        haveKey = 1;
      }else{
        sqliteVdbeAddOp(v, OP_MoveTo, base+idx, 0);
        haveKey = 0;
      }
      pLevel->op = OP_Next;
      pLevel->p1 = pLevel->iCur;
      pLevel->p2 = start;
    }else if( i<ARRAYSIZE(iDirectLt) && (iDirectLt[i]>=0 || iDirectGt[i]>=0) ){
      /* Case 3:  We have an inequality comparison against the ROWID field.
      */
      int testOp = OP_Noop;
      int start;

      brk = pLevel->brk = sqliteVdbeMakeLabel(v);
      cont = pLevel->cont = sqliteVdbeMakeLabel(v);
      if( iDirectGt[i]>=0 ){
        k = iDirectGt[i];
        assert( k<nExpr );
        assert( aExpr[k].p!=0 );
        assert( aExpr[k].idxLeft==idx || aExpr[k].idxRight==idx );
        if( aExpr[k].idxLeft==idx ){
          sqliteExprCode(pParse, aExpr[k].p->pRight);
        }else{
          sqliteExprCode(pParse, aExpr[k].p->pLeft);
        }
        sqliteVdbeAddOp(v, OP_MustBeInt, 0, brk);
        if( aExpr[k].p->op==TK_LT || aExpr[k].p->op==TK_GT ){
          sqliteVdbeAddOp(v, OP_AddImm, 1, 0);
        }
        sqliteVdbeAddOp(v, OP_MoveTo, base+idx, brk);
        aExpr[k].p = 0;
      }else{
        sqliteVdbeAddOp(v, OP_Rewind, base+idx, brk);
      }
      if( iDirectLt[i]>=0 ){
        k = iDirectLt[i];
        assert( k<nExpr );
        assert( aExpr[k].p!=0 );
        assert( aExpr[k].idxLeft==idx || aExpr[k].idxRight==idx );
        if( aExpr[k].idxLeft==idx ){
          sqliteExprCode(pParse, aExpr[k].p->pRight);
        }else{
          sqliteExprCode(pParse, aExpr[k].p->pLeft);
        }
        sqliteVdbeAddOp(v, OP_MustBeInt, 0, sqliteVdbeCurrentAddr(v)+1);
        pLevel->iMem = pParse->nMem++;
        sqliteVdbeAddOp(v, OP_MemStore, pLevel->iMem, 0);
        if( aExpr[k].p->op==TK_LT || aExpr[k].p->op==TK_GT ){
          testOp = OP_Ge;
        }else{
          testOp = OP_Gt;
        }
        aExpr[k].p = 0;
      }
      start = sqliteVdbeCurrentAddr(v);
      pLevel->op = OP_Next;
      pLevel->p1 = base+idx;
      pLevel->p2 = start;
      if( testOp!=OP_Noop ){
        sqliteVdbeAddOp(v, OP_Recno, base+idx, 0);
        sqliteVdbeAddOp(v, OP_MemLoad, pLevel->iMem, 0);
        sqliteVdbeAddOp(v, testOp, 0, brk);
      }
      haveKey = 0;
    }else if( pIdx==0 ){
      /* Case 4:  There was no usable index.  We must do a complete
      **          scan of the entire database table.
      */
      int start;

      brk = pLevel->brk = sqliteVdbeMakeLabel(v);
      cont = pLevel->cont = sqliteVdbeMakeLabel(v);
      sqliteVdbeAddOp(v, OP_Rewind, base+idx, brk);
      start = sqliteVdbeCurrentAddr(v);
      pLevel->op = OP_Next;
      pLevel->p1 = base+idx;
      pLevel->p2 = start;
      haveKey = 0;
    }else{
      /* Case 5: The contraint on the right-most index field is
      **         an inequality.
      */
      int score = pLevel->score;
      int nEqColumn = score/4;
      int start;
      int leFlag, geFlag;
      int testOp;

      /* Evaluate the equality constraints
      */
      for(j=0; j<nEqColumn; j++){
        for(k=0; k<nExpr; k++){
          if( aExpr[k].p==0 ) continue;
          if( aExpr[k].idxLeft==idx 
             && aExpr[k].p->op==TK_EQ
             && (aExpr[k].prereqRight & loopMask)==aExpr[k].prereqRight 
             && aExpr[k].p->pLeft->iColumn==pIdx->aiColumn[j]
          ){
            sqliteExprCode(pParse, aExpr[k].p->pRight);
            aExpr[k].p = 0;
            break;
          }
          if( aExpr[k].idxRight==idx 
             && aExpr[k].p->op==TK_EQ
             && (aExpr[k].prereqLeft & loopMask)==aExpr[k].prereqLeft
             && aExpr[k].p->pRight->iColumn==pIdx->aiColumn[j]
          ){
            sqliteExprCode(pParse, aExpr[k].p->pLeft);
            aExpr[k].p = 0;
            break;
          }
        }
      }

      /* Duplicate the equality contraint values because they will all be
      ** used twice: once to make the termination key and once to make the
      ** start key.
      */
      for(j=0; j<nEqColumn; j++){
        sqliteVdbeAddOp(v, OP_Dup, nEqColumn-1, 0);
      }

      /* Generate the termination key.  This is the key value that
      ** will end the search.  There is no termination key if there
      ** are no equality contraints and no "X<..." constraint.
      */
      if( (score & 1)!=0 ){
        for(k=0; k<nExpr; k++){
          Expr *pExpr = aExpr[k].p;
          if( pExpr==0 ) continue;
          if( aExpr[k].idxLeft==idx 
             && (pExpr->op==TK_LT || pExpr->op==TK_LE)
             && (aExpr[k].prereqRight & loopMask)==aExpr[k].prereqRight 
             && pExpr->pLeft->iColumn==pIdx->aiColumn[j]
          ){
            sqliteExprCode(pParse, pExpr->pRight);
            leFlag = pExpr->op==TK_LE;
            aExpr[k].p = 0;
            break;
          }
          if( aExpr[k].idxRight==idx 
             && (pExpr->op==TK_GT || pExpr->op==TK_GE)
             && (aExpr[k].prereqLeft & loopMask)==aExpr[k].prereqLeft
             && pExpr->pRight->iColumn==pIdx->aiColumn[j]
          ){
            sqliteExprCode(pParse, pExpr->pLeft);
            leFlag = pExpr->op==TK_GE;
            aExpr[k].p = 0;
            break;
          }
        }
        testOp = OP_IdxGE;
      }else{
        testOp = nEqColumn>0 ? OP_IdxGE : OP_Noop;
        leFlag = 1;
      }
      if( testOp!=OP_Noop ){
        pLevel->iMem = pParse->nMem++;
        sqliteVdbeAddOp(v, OP_MakeKey, nEqColumn + (score & 1), 0);
        if( leFlag ){
          sqliteVdbeAddOp(v, OP_IncrKey, 0, 0);
        }
        sqliteVdbeAddOp(v, OP_MemStore, pLevel->iMem, 1);
      }

      /* Generate the start key.  This is the key that defines the lower
      ** bound on the search.  There is no start key if there are not
      ** equality constraints and if there is no "X>..." constraint.  In
      ** that case, generate a "Rewind" instruction in place of the
      ** start key search.
      */
      if( (score & 2)!=0 ){
        for(k=0; k<nExpr; k++){
          Expr *pExpr = aExpr[k].p;
          if( pExpr==0 ) continue;
          if( aExpr[k].idxLeft==idx 
             && (pExpr->op==TK_GT || pExpr->op==TK_GE)
             && (aExpr[k].prereqRight & loopMask)==aExpr[k].prereqRight 
             && pExpr->pLeft->iColumn==pIdx->aiColumn[j]
          ){
            sqliteExprCode(pParse, pExpr->pRight);
            geFlag = pExpr->op==TK_GE;
            aExpr[k].p = 0;
            break;
          }
          if( aExpr[k].idxRight==idx 
             && (pExpr->op==TK_LT || pExpr->op==TK_LE)
             && (aExpr[k].prereqLeft & loopMask)==aExpr[k].prereqLeft
             && pExpr->pRight->iColumn==pIdx->aiColumn[j]
          ){
            sqliteExprCode(pParse, pExpr->pLeft);
            geFlag = pExpr->op==TK_LE;
            aExpr[k].p = 0;
            break;
          }
        }
      }else{
        geFlag = 1;
      }
      brk = pLevel->brk = sqliteVdbeMakeLabel(v);
      cont = pLevel->cont = sqliteVdbeMakeLabel(v);
      if( nEqColumn>0 || (score&2)!=0 ){
        sqliteVdbeAddOp(v, OP_MakeKey, nEqColumn + ((score&2)!=0), 0);
        if( !geFlag ){
          sqliteVdbeAddOp(v, OP_IncrKey, 0, 0);
        }
        sqliteVdbeAddOp(v, OP_MoveTo, pLevel->iCur, brk);
      }else{
        sqliteVdbeAddOp(v, OP_Rewind, pLevel->iCur, brk);
      }

      /* Generate the the top of the loop.  If there is a termination
      ** key we have to test for that key and abort at the top of the
      ** loop.
      */
      start = sqliteVdbeCurrentAddr(v);
      if( testOp!=OP_Noop ){
        sqliteVdbeAddOp(v, OP_MemLoad, pLevel->iMem, 0);
        sqliteVdbeAddOp(v, testOp, pLevel->iCur, brk);
      }
      sqliteVdbeAddOp(v, OP_IdxRecno, pLevel->iCur, 0);
      if( i==pTabList->nId-1 && pushKey ){
        haveKey = 1;
      }else{
        sqliteVdbeAddOp(v, OP_MoveTo, base+idx, 0);
        haveKey = 0;
      }

      /* Record the instruction used to terminate the loop.
      */
      pLevel->op = OP_Next;
      pLevel->p1 = pLevel->iCur;
      pLevel->p2 = start;
    }
    loopMask |= 1<<idx;

    /* Insert code to test every subexpression that can be completely
    ** computed using the current set of tables.
    */
    for(j=0; j<nExpr; j++){
      if( aExpr[j].p==0 ) continue;
      if( (aExpr[j].prereqRight & loopMask)!=aExpr[j].prereqRight ) continue;
      if( (aExpr[j].prereqLeft & loopMask)!=aExpr[j].prereqLeft ) continue;
      if( haveKey ){
        haveKey = 0;
        sqliteVdbeAddOp(v, OP_MoveTo, base+idx, 0);
      }
      sqliteExprIfFalse(pParse, aExpr[j].p, cont);
      aExpr[j].p = 0;
    }
    brk = cont;
  }
  pWInfo->iContinue = cont;
  if( pushKey && !haveKey ){
    sqliteVdbeAddOp(v, OP_Recno, base, 0);
  }
  sqliteFree(aOrder);
  return pWInfo;
}

/*
** Generate the end of the WHERE loop.
*/
void sqliteWhereEnd(WhereInfo *pWInfo){
  Vdbe *v = pWInfo->pParse->pVdbe;
  int i;
  int base = pWInfo->base;
  WhereLevel *pLevel;
  IdList *pTabList = pWInfo->pTabList;

  for(i=pTabList->nId-1; i>=0; i--){
    pLevel = &pWInfo->a[i];
    sqliteVdbeResolveLabel(v, pLevel->cont);
    if( pLevel->op!=OP_Noop ){
      sqliteVdbeAddOp(v, pLevel->op, pLevel->p1, pLevel->p2);
    }
    sqliteVdbeResolveLabel(v, pLevel->brk);
  }
  sqliteVdbeResolveLabel(v, pWInfo->iBreak);
  for(i=0; i<pTabList->nId; i++){
    if( pTabList->a[i].pTab->isTransient ) continue;
    pLevel = &pWInfo->a[i];
    sqliteVdbeAddOp(v, OP_Close, base+i, 0);
    if( pLevel->pIdx!=0 ){
      sqliteVdbeAddOp(v, OP_Close, pLevel->iCur, 0);
    }
  }
  pWInfo->pParse->nTab = base;
  sqliteFree(pWInfo);
  return;
}
