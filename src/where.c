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
** This module contains C code that generates VDBE code used to process
** the WHERE clause of SQL statements.  Also found here are subroutines
** to generate VDBE code to evaluate expressions.
**
** $Id: where.c,v 1.17 2001/09/13 14:46:11 drh Exp $
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
** The input to this routine is an ExprInfo structure with only the
** "p" field filled in.  The job of this routine is to analyze the
** subexpression and populate all the other fields of the ExprInfo
** structure.
**
** "base" is the cursor number (the value of the iTable field) that
** corresponds to the first entyr in the table list.  This is the
** same as pParse->nTab.
*/
static void exprAnalyze(int base, ExprInfo *pInfo){
  Expr *pExpr = pInfo->p;
  pInfo->prereqLeft = exprTableUsage(base, pExpr->pLeft);
  pInfo->prereqRight = exprTableUsage(base, pExpr->pRight);
  pInfo->indexable = 0;
  pInfo->idxLeft = -1;
  pInfo->idxRight = -1;
  if( pExpr->op==TK_EQ && (pInfo->prereqRight & pInfo->prereqLeft)==0 ){
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
  Index *aIdx[32];     /* Index to use on each nested loop.  */
  int aDirect[32];     /* If TRUE, then index this table using ROWID */
  ExprInfo aExpr[50];  /* The WHERE clause is divided into these expressions */

  /* Allocate space for aOrder[]. */
  aOrder = sqliteMalloc( sizeof(int) * pTabList->nId );

  /* Allocate and initialize the WhereInfo structure that will become the
  ** return value.
  */
  pWInfo = sqliteMalloc( sizeof(WhereInfo) );
  if( sqlite_malloc_failed ){
    sqliteFree(aOrder);
    sqliteFree(pWInfo);
    return 0;
  }
  pWInfo->pParse = pParse;
  pWInfo->pTabList = pTabList;
  base = pWInfo->base = pParse->nTab;

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
    exprAnalyze(pParse->nTab, &aExpr[i]);
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
  ** Make aIdx[i] point to the index to use for the i-th nested loop
  ** where i==0 is the outer loop and i==pTabList->nId-1 is the inner
  ** loop.  If the expression uses only the ROWID field, then set
  ** aDirect[i] to 1.
  **
  ** Actually, if there are more than 32 tables in the join, only the
  ** first 32 tables are candidates for indices.
  */
  loopMask = 0;
  for(i=0; i<pTabList->nId && i<ARRAYSIZE(aIdx); i++){
    int j;
    int idx = aOrder[i];
    Table *pTab = pTabList->a[idx].pTab;
    Index *pIdx;
    Index *pBestIdx = 0;

    /* Check to see if there is an expression that uses only the
    ** ROWID field of this table.  If so, set aDirect[i] to 1.
    ** If not, set aDirect[i] to 0.
    */
    aDirect[i] = 0;
    for(j=0; j<nExpr; j++){
      if( aExpr[j].idxLeft==idx && aExpr[j].p->pLeft->iColumn<0
            && (aExpr[j].prereqRight & loopMask)==aExpr[j].prereqRight ){
        aDirect[i] = 1;
        break;
      }
      if( aExpr[j].idxRight==idx && aExpr[j].p->pRight->iColumn<0
            && (aExpr[j].prereqLeft & loopMask)==aExpr[j].prereqLeft ){
        aDirect[i] = 1;
        break;
      }
    }
    if( aDirect[i] ){
      loopMask |= 1<<idx;
      aIdx[i] = 0;
      continue;
    }

    /* Do a search for usable indices.  Leave pBestIdx pointing to
    ** the most specific usable index.
    **
    ** "Most specific" means that pBestIdx is the usable index that
    ** has the largest value for nColumn.  A usable index is one for
    ** which there are subexpressions to compute every column of the
    ** index.
    */
    for(pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext){
      int columnMask = 0;

      if( pIdx->nColumn>32 ) continue;
      for(j=0; j<nExpr; j++){
        if( aExpr[j].idxLeft==idx 
             && (aExpr[j].prereqRight & loopMask)==aExpr[j].prereqRight ){
          int iColumn = aExpr[j].p->pLeft->iColumn;
          int k;
          for(k=0; k<pIdx->nColumn; k++){
            if( pIdx->aiColumn[k]==iColumn ){
              columnMask |= 1<<k;
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
              columnMask |= 1<<k;
              break;
            }
          }
        }
      }
      if( columnMask + 1 == (1<<pIdx->nColumn) ){
        if( pBestIdx==0 || pBestIdx->nColumn<pIdx->nColumn ){
          pBestIdx = pIdx;
        }
      }
    }
    aIdx[i] = pBestIdx;
    loopMask |= 1<<idx;
  }

  /* Open all tables in the pTabList and all indices in aIdx[].
  */
  for(i=0; i<pTabList->nId; i++){
    sqliteVdbeAddOp(v, OP_Open, base+i, pTabList->a[i].pTab->tnum,
         pTabList->a[i].pTab->zName, 0);
    if( i<ARRAYSIZE(aIdx) && aIdx[i]!=0 ){
      sqliteVdbeAddOp(v, OP_Open, base+pTabList->nId+i, aIdx[i]->tnum,
          aIdx[i]->zName, 0);
    }
  }
  memcpy(pWInfo->aIdx, aIdx, sizeof(aIdx));

  /* Generate the code to do the search
  */
  pWInfo->iBreak = brk = sqliteVdbeMakeLabel(v);
  loopMask = 0;
  for(i=0; i<pTabList->nId; i++){
    int j, k;
    int idx = aOrder[i];
    int goDirect;
    Index *pIdx;

    if( i<ARRAYSIZE(aIdx) ){
      pIdx = aIdx[i];
      goDirect = aDirect[i];
    }else{
      pIdx = 0;
      goDirect = 0;
    }

    if( goDirect ){
      /* Case 1:  We can directly reference a single row using the ROWID field.
      */
      cont = brk;
      for(k=0; k<nExpr; k++){
        if( aExpr[k].p==0 ) continue;
        if( aExpr[k].idxLeft==idx 
           && (aExpr[k].prereqRight & loopMask)==aExpr[k].prereqRight 
           && aExpr[k].p->pLeft->iColumn<0
        ){
          sqliteExprCode(pParse, aExpr[k].p->pRight);
          aExpr[k].p = 0;
          break;
        }
        if( aExpr[k].idxRight==idx 
           && (aExpr[k].prereqLeft & loopMask)==aExpr[k].prereqLeft
           && aExpr[k].p->pRight->iColumn<0
        ){
          sqliteExprCode(pParse, aExpr[k].p->pLeft);
          aExpr[k].p = 0;
          break;
        }
      }
      sqliteVdbeAddOp(v, OP_AddImm, 0, 0, 0, 0);
      if( i==pTabList->nId-1 && pushKey ){
        haveKey = 1;
      }else{
        sqliteVdbeAddOp(v, OP_MoveTo, base+idx, 0, 0, 0);
        haveKey = 0;
      }
    }else if( pIdx==0 ){
      /* Case 2:  There was no usable index.  We must do a complete
      ** scan of the table.
      */
      cont = sqliteVdbeMakeLabel(v);
      sqliteVdbeAddOp(v, OP_Next, base+idx, brk, 0, cont);
      haveKey = 0;
    }else{
      /* Case 3:  We do have a usable index in pIdx.
      */
      cont = sqliteVdbeMakeLabel(v);
      for(j=0; j<pIdx->nColumn; j++){
        for(k=0; k<nExpr; k++){
          if( aExpr[k].p==0 ) continue;
          if( aExpr[k].idxLeft==idx 
             && (aExpr[k].prereqRight & loopMask)==aExpr[k].prereqRight 
             && aExpr[k].p->pLeft->iColumn==pIdx->aiColumn[j]
          ){
            sqliteExprCode(pParse, aExpr[k].p->pRight);
            aExpr[k].p = 0;
            break;
          }
          if( aExpr[k].idxRight==idx 
             && (aExpr[k].prereqLeft & loopMask)==aExpr[k].prereqLeft
             && aExpr[k].p->pRight->iColumn==pIdx->aiColumn[j]
          ){
            sqliteExprCode(pParse, aExpr[k].p->pLeft);
            aExpr[k].p = 0;
            break;
          }
        }
      }
      sqliteVdbeAddOp(v, OP_MakeKey, pIdx->nColumn, 0, 0, 0);
      sqliteVdbeAddOp(v, OP_BeginIdx, base+pTabList->nId+i, 0, 0, 0);
      sqliteVdbeAddOp(v, OP_NextIdx, base+pTabList->nId+i, brk, 0, cont);
      if( i==pTabList->nId-1 && pushKey ){
        haveKey = 1;
      }else{
        sqliteVdbeAddOp(v, OP_MoveTo, base+idx, 0, 0, 0);
        haveKey = 0;
      }
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
        sqliteVdbeAddOp(v, OP_MoveTo, base+idx, 0, 0, 0);
      }
      sqliteExprIfFalse(pParse, aExpr[j].p, cont);
      aExpr[j].p = 0;
    }
    brk = cont;
  }
  pWInfo->iContinue = cont;
  if( pushKey && !haveKey ){
    sqliteVdbeAddOp(v, OP_Recno, base, 0, 0, 0);
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
  int brk = pWInfo->iBreak;
  int base = pWInfo->base;

  sqliteVdbeAddOp(v, OP_Goto, 0, pWInfo->iContinue, 0, 0);
  for(i=0; i<pWInfo->pTabList->nId; i++){
    sqliteVdbeAddOp(v, OP_Close, base+i, 0, 0, brk);
    brk = 0;
    if( i<ARRAYSIZE(pWInfo->aIdx) && pWInfo->aIdx[i]!=0 ){
      sqliteVdbeAddOp(v, OP_Close, base+pWInfo->pTabList->nId+i, 0, 0, 0);
    }
  }
  if( brk!=0 ){
    sqliteVdbeAddOp(v, OP_Noop, 0, 0, 0, brk);
  }
  sqliteFree(pWInfo);
  return;
}
