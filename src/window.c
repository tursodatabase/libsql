/*
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
*/
#include "sqliteInt.h"

void sqlite3WindowDelete(sqlite3 *db, Window *p){
  if( p ){
    sqlite3ExprDelete(db, p->pFilter);
    sqlite3ExprListDelete(db, p->pPartition);
    sqlite3ExprListDelete(db, p->pOrderBy);
    sqlite3ExprDelete(db, p->pEnd);
    sqlite3ExprDelete(db, p->pStart);
    sqlite3DbFree(db, p);
  }
}

Window *sqlite3WindowAlloc(
  Parse *pParse, 
  int eType,
  int eStart, Expr *pStart,
  int eEnd, Expr *pEnd
){
  Window *pWin = (Window*)sqlite3DbMallocZero(pParse->db, sizeof(Window));

  if( pWin ){
    pWin->eType = eType;
    pWin->eStart = eStart;
    pWin->eEnd = eEnd;
    pWin->pEnd = pEnd;
    pWin->pStart = pStart;
  }else{
    sqlite3ExprDelete(pParse->db, pEnd);
    sqlite3ExprDelete(pParse->db, pStart);
  }

  return pWin;
}

void sqlite3WindowAttach(Parse *pParse, Expr *p, Window *pWin){
  if( p ){
    p->pWin = pWin;
  }else{
    sqlite3WindowDelete(pParse->db, pWin);
  }
}

/*
** Return 0 if the two window objects are identical, or non-zero otherwise.
*/
int sqlite3WindowCompare(Parse *pParse, Window *p1, Window *p2){
  if( p1->eType!=p2->eType ) return 1;
  if( p1->eStart!=p2->eStart ) return 1;
  if( p1->eEnd!=p2->eEnd ) return 1;
  if( sqlite3ExprCompare(pParse, p1->pStart, p2->pStart, -1) ) return 1;
  if( sqlite3ExprCompare(pParse, p1->pEnd, p2->pEnd, -1) ) return 1;
  if( sqlite3ExprListCompare(p1->pPartition, p2->pPartition, -1) ) return 1;
  if( sqlite3ExprListCompare(p1->pOrderBy, p2->pOrderBy, -1) ) return 1;
  return 0;
}

void sqlite3WindowCodeInit(Parse *pParse, Window *pWin){
  Vdbe *v = sqlite3GetVdbe(pParse);
  int nPart = (pWin->pPartition ? pWin->pPartition->nExpr : 0);
  nPart += (pWin->pOrderBy ? pWin->pOrderBy->nExpr : 0);
  if( nPart ){
    pWin->regPart = pParse->nMem+1;
    pParse->nMem += nPart;
    sqlite3VdbeAddOp3(v, OP_Null, 0, pWin->regPart, pWin->regPart+nPart-1);
  }
}

static void windowCheckFrameValue(Parse *pParse, int reg, int bEnd){
  static const char *azErr[] = {
    "frame starting offset must be a non-negative integer",
    "frame ending offset must be a non-negative integer"
  };
  Vdbe *v = sqlite3GetVdbe(pParse);
  int regZero = ++pParse->nMem;


  sqlite3VdbeAddOp2(v, OP_Integer, 0, regZero);
  sqlite3VdbeAddOp2(v, OP_MustBeInt, reg, sqlite3VdbeCurrentAddr(v)+2);
  sqlite3VdbeAddOp3(v, OP_Ge, regZero, sqlite3VdbeCurrentAddr(v)+2, reg);
  sqlite3VdbeAddOp2(v, OP_Halt, SQLITE_ERROR, OE_Abort);
  sqlite3VdbeAppendP4(v, (void*)azErr[bEnd], P4_STATIC);
}

/*
** ROWS BETWEEN <expr> PRECEDING    AND <expr> FOLLOWING
**
**   ...
**     if( new partition ){
**       Gosub flush_partition
**     }
**     Insert (record in eph-table)
**   sqlite3WhereEnd()
**   Gosub flush_partition
**
** flush_partition:
**   OpenDup (csr -> csr2)
**   OpenDup (csr -> csr3)
**   regPrec = <expr1>            // PRECEDING expression
**   regFollow = <expr2>          // FOLLOWING expression
**   if( regPrec<0 || regFollow<0 ) throw exception!
**   Rewind (csr,csr2,csr3)       // if EOF goto flush_partition_done
**     Aggstep (csr3)
**     Next(csr3)                 // if EOF fall-through
**     if( (regFollow--)<=0 ){
**       AggFinal (xValue)
**       Gosub addrGosub
**       Next(csr)                // if EOF goto flush_partition_done
**       if( (regPrec--)<=0 ){
**         AggStep (csr2, xInverse)
**         Next(csr2)
**       }
**     }
** flush_partition_done:
**   Close (csr2)
**   Close (csr3)
**   ResetSorter (csr)
**   Return
**
** ROWS BETWEEN <expr> PRECEDING    AND CURRENT ROW
** ROWS BETWEEN CURRENT ROW         AND <expr> FOLLOWING
** ROWS BETWEEN <expr> PRECEDING    AND UNBOUNDED FOLLOWING
** ROWS BETWEEN UNBOUNDED PRECEDING AND <expr> FOLLOWING
**
**   These are similar to the above. For "CURRENT ROW", intialize the
**   register to 0. For "UNBOUNDED ..." to infinity.
**
*/
static void windowCodeRowExprStep(
  Parse *pParse, 
  Select *p,
  WhereInfo *pWInfo,
  int regGosub, 
  int addrGosub
){
  Window *pMWin = p->pWin;
  Vdbe *v = sqlite3GetVdbe(pParse);
  Window *pWin;
  int k;
  int iSubCsr = p->pSrc->a[0].iCursor;
  int nSub = p->pSrc->a[0].pTab->nCol;
  int regFlushPart;               /* Register for "Gosub flush_partition" */
  int addrFlushPart;              /* Label for "Gosub flush_partition" */
  int addrDone;                   /* Label for "Gosub flush_partition_done" */

  int reg = pParse->nMem+1;
  int regRecord = reg+nSub;
  int regRowid = regRecord+1;
  int addr;
  int csrPrec = pParse->nTab++;
  int csrFollow = pParse->nTab++;
  int regPrec;                    /* Value of <expr> PRECEDING */
  int regFollow;                  /* Value of <expr> FOLLOWING */
  int addrNext;
  int addrGoto;
  int addrIfPos1;
  int addrIfPos2;

  assert( pMWin->eStart==TK_PRECEDING 
       || pMWin->eStart==TK_CURRENT 
       || pMWin->eStart==TK_UNBOUNDED 
  );
  assert( pMWin->eEnd==TK_FOLLOWING 
       || pMWin->eEnd==TK_CURRENT 
       || pMWin->eEnd==TK_UNBOUNDED 
  );

  pParse->nMem += nSub + 2;

  /* Allocate register and label for the "flush_partition" sub-routine. */
  regFlushPart = ++pParse->nMem;
  addrFlushPart = sqlite3VdbeMakeLabel(v);
  addrDone = sqlite3VdbeMakeLabel(v);

  regPrec = ++pParse->nMem;
  regFollow = ++pParse->nMem;

  /* Martial the row returned by the sub-select into an array of 
  ** registers. */
  for(k=0; k<nSub; k++){
    sqlite3VdbeAddOp3(v, OP_Column, iSubCsr, k, reg+k);
  }
  sqlite3VdbeAddOp3(v, OP_MakeRecord, reg, nSub, regRecord);

  /* Check if this is the start of a new partition. If so, call the
  ** flush_partition sub-routine.  */
  if( pMWin->pPartition ){
    ExprList *pPart = pMWin->pPartition;
    int nPart = (pPart ? pPart->nExpr : 0);
    int addrJump = 0;
    int regNewPart = reg + pMWin->nBufferCol;
    KeyInfo *pKeyInfo = sqlite3KeyInfoFromExprList(pParse, pPart, 0, 0);

    addr = sqlite3VdbeAddOp3(v, OP_Compare, regNewPart, pMWin->regPart,nPart);
    sqlite3VdbeAppendP4(v, (void*)pKeyInfo, P4_KEYINFO);
    addrJump = sqlite3VdbeAddOp3(v, OP_Jump, addr+2, addr+4, addr+2);
    sqlite3VdbeAddOp2(v, OP_Gosub, regFlushPart, addrFlushPart);
    sqlite3VdbeAddOp3(v, OP_Copy, regNewPart, pMWin->regPart, nPart);
  }

  /* Buffer the current row in the ephemeral table. */
  sqlite3VdbeAddOp2(v, OP_NewRowid, pMWin->iEphCsr, regRowid);
  sqlite3VdbeAddOp3(v, OP_Insert, pMWin->iEphCsr, regRecord, regRowid);

  /* End of the input loop */
  sqlite3WhereEnd(pWInfo);

  /* Invoke "flush_partition" to deal with the final (or only) partition */
  sqlite3VdbeAddOp2(v, OP_Gosub, regFlushPart, addrFlushPart);
  addrGoto = sqlite3VdbeAddOp0(v, OP_Goto);

  /* flush_partition: */
  sqlite3VdbeResolveLabel(v, addrFlushPart);
  sqlite3VdbeAddOp2(v, OP_Once, 0, sqlite3VdbeCurrentAddr(v)+3);
  sqlite3VdbeAddOp2(v, OP_OpenDup, csrPrec, pMWin->iEphCsr);
  sqlite3VdbeAddOp2(v, OP_OpenDup, csrFollow, pMWin->iEphCsr);

  /* If either regPrec or regFollow are not non-negative integers, throw 
  ** an exception.  */
  if( pMWin->pStart ){
    assert( pMWin->eStart==TK_PRECEDING );
    sqlite3ExprCode(pParse, pMWin->pStart, regPrec);
    windowCheckFrameValue(pParse, regPrec, 0);
  }
  if( pMWin->pEnd ){
    assert( pMWin->eEnd==TK_FOLLOWING );
    sqlite3ExprCode(pParse, pMWin->pEnd, regFollow);
    windowCheckFrameValue(pParse, regFollow, 1);
  }

  sqlite3VdbeAddOp2(v, OP_Null, 0, pMWin->regResult);
  sqlite3VdbeAddOp2(v, OP_Null, 0, pMWin->regAccum);

  sqlite3VdbeAddOp2(v, OP_Rewind, pMWin->iEphCsr, addrDone);
  sqlite3VdbeAddOp2(v, OP_Rewind, csrPrec, addrDone);
  sqlite3VdbeChangeP5(v, 1);
  sqlite3VdbeAddOp2(v, OP_Rewind, csrFollow, addrDone);
  sqlite3VdbeChangeP5(v, 1);

  /* Invoke AggStep function for each window function using the row that
  ** csrFollow currently points to. Or, if csrFollow is already at EOF,
  ** do nothing.  */
  addrNext = sqlite3VdbeCurrentAddr(v);
  sqlite3VdbeAddOp2(v, OP_Next, csrFollow, addrNext+2);
  sqlite3VdbeAddOp0(v, OP_Goto);
  for(pWin=pMWin; pWin; pWin=pWin->pNextWin){
    int i;
    for(i=0; i<pWin->nArg; i++){
      sqlite3VdbeAddOp3(v, OP_Column, csrFollow, pWin->iArgCol+i, reg+i);
    }
    sqlite3VdbeAddOp3(v, OP_AggStep0, 0, reg, pWin->regAccum);
    sqlite3VdbeAppendP4(v, pWin->pFunc, P4_FUNCDEF);
    sqlite3VdbeChangeP5(v, (u8)pWin->nArg);
  }
  if( pMWin->eEnd==TK_UNBOUNDED ){
    sqlite3VdbeAddOp2(v, OP_Goto, 0, addrNext);
    sqlite3VdbeJumpHere(v, addrNext+1);
    addrNext = sqlite3VdbeCurrentAddr(v);
  }else{
    sqlite3VdbeJumpHere(v, addrNext+1);
  }

  if( pMWin->eEnd==TK_FOLLOWING ){
    addrIfPos1 = sqlite3VdbeAddOp3(v, OP_IfPos, regFollow, 0 , 1);
  }
  for(pWin=pMWin; pWin; pWin=pWin->pNextWin){
    sqlite3VdbeAddOp3(v, 
        OP_AggFinal, pWin->regAccum, pWin->nArg, pWin->regResult
    );
    sqlite3VdbeAppendP4(v, pWin->pFunc, P4_FUNCDEF);
  }
  sqlite3VdbeAddOp2(v, OP_Gosub, regGosub, addrGosub);
  sqlite3VdbeAddOp2(v, OP_Next, pMWin->iEphCsr, sqlite3VdbeCurrentAddr(v)+2);
  sqlite3VdbeAddOp2(v, OP_Goto, 0, addrDone);

  if( pMWin->eStart==TK_CURRENT || pMWin->eStart==TK_PRECEDING ){
    if( pMWin->eStart==TK_PRECEDING ){
      addrIfPos2 = sqlite3VdbeAddOp3(v, OP_IfPos, regPrec, 0 , 1);
    }
    sqlite3VdbeAddOp2(v, OP_Next, csrPrec, sqlite3VdbeCurrentAddr(v)+1);
    for(pWin=pMWin; pWin; pWin=pWin->pNextWin){
      int i;
      for(i=0; i<pWin->nArg; i++){
        sqlite3VdbeAddOp3(v, OP_Column, csrPrec, pWin->iArgCol+i, reg+i);
      }
      sqlite3VdbeAddOp3(v, OP_AggStep0, 1, reg, pWin->regAccum);
      sqlite3VdbeAppendP4(v, pWin->pFunc, P4_FUNCDEF);
      sqlite3VdbeChangeP5(v, (u8)pWin->nArg);
    }
    if( pMWin->eStart==TK_PRECEDING ){
      sqlite3VdbeJumpHere(v, addrIfPos2);
    }
  }
  if( pMWin->eEnd==TK_FOLLOWING ){
    sqlite3VdbeJumpHere(v, addrIfPos1);
  }
  sqlite3VdbeAddOp2(v, OP_Goto, 0, addrNext);

  /* flush_partition_done: */
  sqlite3VdbeResolveLabel(v, addrDone);
  sqlite3VdbeAddOp1(v, OP_ResetSorter, pMWin->iEphCsr);
  sqlite3VdbeAddOp1(v, OP_Return, regFlushPart);

  /* Jump to here to skip over flush_partition */
  sqlite3VdbeJumpHere(v, addrGoto);
}

static void windowCodeDefaultStep(
  Parse *pParse, 
  Select *p,
  WhereInfo *pWInfo,
  int regGosub, 
  int addrGosub
){
  Window *pMWin = p->pWin;
  Vdbe *v = sqlite3GetVdbe(pParse);
  Window *pWin;
  int k;
  int iSubCsr = p->pSrc->a[0].iCursor;
  int nSub = p->pSrc->a[0].pTab->nCol;
  int reg = pParse->nMem+1;
  int regRecord = reg+nSub;
  int regRowid = regRecord+1;
  int addr;

  pParse->nMem += nSub + 2;

  /* Martial the row returned by the sub-select into an array of 
  ** registers. */
  for(k=0; k<nSub; k++){
    sqlite3VdbeAddOp3(v, OP_Column, iSubCsr, k, reg+k);
  }

  /* Check if this is the start of a new partition or peer group. */
  if( pMWin->regPart ){
    ExprList *pPart = pMWin->pPartition;
    int nPart = (pPart ? pPart->nExpr : 0);
    ExprList *pOrderBy = pMWin->pOrderBy;
    int nPeer = (pOrderBy ? pOrderBy->nExpr : 0);
    int addrGoto = 0;
    int addrJump = 0;

    if( pPart ){
      int regNewPart = reg + pMWin->nBufferCol;
      KeyInfo *pKeyInfo = sqlite3KeyInfoFromExprList(pParse, pPart, 0, 0);
      addr = sqlite3VdbeAddOp3(v, OP_Compare, regNewPart, pMWin->regPart,nPart);
      sqlite3VdbeAppendP4(v, (void*)pKeyInfo, P4_KEYINFO);
      addrJump = sqlite3VdbeAddOp3(v, OP_Jump, addr+2, 0, addr+2);
      for(pWin=pMWin; pWin; pWin=pWin->pNextWin){
        sqlite3VdbeAddOp2(v, OP_AggFinal, pWin->regAccum, pWin->nArg);
        sqlite3VdbeAppendP4(v, pWin->pFunc, P4_FUNCDEF);
        sqlite3VdbeAddOp2(v, OP_Copy, pWin->regAccum, pWin->regResult);
      }
      if( pOrderBy ){
        addrGoto = sqlite3VdbeAddOp0(v, OP_Goto);
      }
    }

    if( pOrderBy ){
      int regNewPeer = reg + pMWin->nBufferCol + nPart;
      int regPeer = pMWin->regPart + nPart;

      KeyInfo *pKeyInfo = sqlite3KeyInfoFromExprList(pParse, pOrderBy, 0, 0);
      if( addrJump ) sqlite3VdbeJumpHere(v, addrJump);
      addr = sqlite3VdbeAddOp3(v, OP_Compare, regNewPeer, regPeer, nPeer);
      sqlite3VdbeAppendP4(v, (void*)pKeyInfo, P4_KEYINFO);
      addrJump = sqlite3VdbeAddOp3(v, OP_Jump, addr+2, 0, addr+2);
      for(pWin=pMWin; pWin; pWin=pWin->pNextWin){
        sqlite3VdbeAddOp3(v, 
            OP_AggFinal, pWin->regAccum, pWin->nArg, pWin->regResult
        );
        sqlite3VdbeAppendP4(v, pWin->pFunc, P4_FUNCDEF);
      }
      if( addrGoto ) sqlite3VdbeJumpHere(v, addrGoto);
    }

    sqlite3VdbeAddOp2(v, OP_Gosub, regGosub, addrGosub);
    sqlite3VdbeAddOp1(v, OP_ResetSorter, pMWin->iEphCsr);
    sqlite3VdbeAddOp3(
        v, OP_Copy, reg+pMWin->nBufferCol, pMWin->regPart, nPart+nPeer-1
    );

    sqlite3VdbeJumpHere(v, addrJump);
  }

  /* Invoke step function for window functions */
  for(pWin=pMWin; pWin; pWin=pWin->pNextWin){
    sqlite3VdbeAddOp3(v, OP_AggStep0, 0, reg+pWin->iArgCol, pWin->regAccum);
    sqlite3VdbeAppendP4(v, pWin->pFunc, P4_FUNCDEF);
    sqlite3VdbeChangeP5(v, (u8)pWin->nArg);
  }

  /* Buffer the current row in the ephemeral table. */
  if( pMWin->nBufferCol>0 ){
    sqlite3VdbeAddOp3(v, OP_MakeRecord, reg, pMWin->nBufferCol, regRecord);
  }else{
    sqlite3VdbeAddOp2(v, OP_Blob, 0, regRecord);
    sqlite3VdbeAppendP4(v, (void*)"", 0);
  }
  sqlite3VdbeAddOp2(v, OP_NewRowid, pMWin->iEphCsr, regRowid);
  sqlite3VdbeAddOp3(v, OP_Insert, pMWin->iEphCsr, regRecord, regRowid);

  /* End the database scan loop. */
  sqlite3WhereEnd(pWInfo);

  for(pWin=pMWin; pWin; pWin=pWin->pNextWin){
    sqlite3VdbeAddOp2(v, OP_AggFinal, pWin->regAccum, pWin->nArg);
    sqlite3VdbeAppendP4(v, pWin->pFunc, P4_FUNCDEF);
    sqlite3VdbeAddOp2(v, OP_Copy, pWin->regAccum, pWin->regResult);
  }
  sqlite3VdbeAddOp2(v, OP_Gosub, regGosub, addrGosub);
}


/*
** RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
**
**   ...
**     if( new partition ){
**       AggFinal (xFinalize)
**       Gosub addrGosub
**       ResetSorter eph-table
**     }
**     else if( new peer ){
**       AggFinal (xValue)
**       Gosub addrGosub
**       ResetSorter eph-table
**     }
**     AggStep
**     Insert (record into eph-table)
**   sqlite3WhereEnd()
**   AggFinal (xFinalize)
**   Gosub addrGosub
**
** RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
**
**   As above, except take no action for a "new peer". Invoke
**   the sub-routine once only for each partition.
**
** RANGE BETWEEN CURRENT ROW AND CURRENT ROW
**
**   As above, except that the "new peer" condition is handled in the
**   same way as "new partition" (so there is no "else if" block).
**
** RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
**
**   One way is to just reverse the sort order and do as for BETWEEN 
**   UNBOUNDED PRECEDING AND CURRENT ROW. But that is not quite the same for
**   things like group_concat(). And perhaps other user defined aggregates 
**   as well.
**
**   ...
**     if( new partition ){
**       Gosub flush_partition;
**       ResetSorter eph-table
**     }
**     AggStep
**     Insert (record into eph-table)
**   sqlite3WhereEnd()
**   Gosub flush_partition
**
**  flush_partition:
**   OpenDup (csr -> csr2)
**   foreach (record in eph-table) {
**     if( new peer ){
**       while( csr2!=csr ){
**         AggStep (xInverse)
**         Next (csr2)
**       }
**     }
**     AggFinal (xValue)
**     Gosub addrGosub
**   }
**
**========================================================================
**
** ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
**   ...
**     if( new partition ){
**       AggFinal (xFinalize)
**     }
**     AggStep
**     AggFinal (xValue)
**     Gosub addrGosub
**   sqlite3WhereEnd()
**
** ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
** ROWS BETWEEN CURRENT ROW AND CURRENT ROW
** ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
**
**========================================================================
**
** ROWS BETWEEN <expr> PRECEDING    AND <expr> PRECEDING
**
**   Replace the bit after "Rewind" in the above with:
**
**     if( (regFollow--)<=0 ){
**       AggStep (csr3)
**       Next (csr3)
**     }
**     AggFinal (xValue)
**     Gosub addrGosub
**     Next(csr)                  // if EOF goto flush_partition_done
**     if( (regPrec--)<=0 ){
**       AggStep (csr2, xInverse)
**       Next (csr2)
**     }
**
** ROWS BETWEEN <expr> FOLLOWING    AND <expr> FOLLOWING
**
**   regFollow = regFollow - regPrec
**   Rewind (csr,csr2,csr3)       // if EOF goto flush_partition_done
**     Aggstep (csr3)
**     Next(csr3)                 // if EOF fall-through
**     if( (regFollow--)<=0 ){
**       AggStep (csr2, xInverse)
**       Next (csr2)
**       if( (regPrec--)<=0 ){
**         AggFinal (xValue)
**         Gosub addrGosub
**         Next(csr)              // if EOF goto flush_partition_done
**       }
**     }
**
** ROWS BETWEEN UNBOUNDED PRECEDING AND <expr> PRECEDING
** ROWS BETWEEN <expr> FOLLOWING    AND UNBOUNDED FOLLOWING
**
**   Similar to the above, except with regPrec or regFollow set to infinity,
**   as appropriate.
**
**
**
*/
void sqlite3WindowCodeStep(
  Parse *pParse, 
  Select *p,
  WhereInfo *pWInfo,
  int regGosub, 
  int addrGosub,
  int *pbLoop
){
  Window *pMWin = p->pWin;

  if( pMWin->eType==TK_ROWS 
   && (pMWin->eStart==TK_PRECEDING || pMWin->eEnd==TK_FOLLOWING)
   && (pMWin->eStart!=TK_FOLLOWING || pMWin->eEnd==TK_PRECEDING)
  ){
    *pbLoop = 0;
    windowCodeRowExprStep(pParse, p, pWInfo, regGosub, addrGosub);
    return;
  }

  *pbLoop = 1;
  windowCodeDefaultStep(pParse, p, pWInfo, regGosub, addrGosub);
}


