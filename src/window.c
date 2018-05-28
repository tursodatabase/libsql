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

static void windowAggStep(
  Parse *pParse, 
  Window *pMWin, 
  int csr,
  int bInverse, 
  int reg
){
  Vdbe *v = sqlite3GetVdbe(pParse);
  Window *pWin;
  for(pWin=pMWin; pWin; pWin=pWin->pNextWin){
    int i;
    for(i=0; i<pWin->nArg; i++){
      sqlite3VdbeAddOp3(v, OP_Column, csr, pWin->iArgCol+i, reg+i);
    }
    sqlite3VdbeAddOp3(v, OP_AggStep0, bInverse, reg, pWin->regAccum);
    sqlite3VdbeAppendP4(v, pWin->pFunc, P4_FUNCDEF);
    sqlite3VdbeChangeP5(v, (u8)pWin->nArg);
  }
}

static void windowAggFinal(Parse *pParse, Window *pMWin, int bFinal){
  Vdbe *v = sqlite3GetVdbe(pParse);
  Window *pWin;

  for(pWin=pMWin; pWin; pWin=pWin->pNextWin){
    if( bFinal==0 ){
      sqlite3VdbeAddOp2(v, OP_Null, 0, pWin->regResult);
    }
    sqlite3VdbeAddOp2(v, OP_AggFinal, pWin->regAccum, pWin->nArg);
    sqlite3VdbeAppendP4(v, pWin->pFunc, P4_FUNCDEF);
    if( bFinal ){
      sqlite3VdbeAddOp2(v, OP_Copy, pWin->regAccum, pWin->regResult);
    }else{
      sqlite3VdbeChangeP3(v, -1, pWin->regResult);
    }
  }
}


/*
** ROWS BETWEEN <expr1> PRECEDING AND <expr2> FOLLOWING
** ----------------------------------------------------
**
** Pseudo-code for the implementation of this window frame type is as
** follows. sqlite3WhereBegin() has already been called to generate the
** top of the main loop when this function is called.
**
** Each time the sub-routine at addrGosub is invoked, a single output
** row is generated based on the current row indicated by Window.iEphCsr.
**
**     ...
**       if( new partition ){
**         Gosub flush_partition
**       }
**       Insert (record in eph-table)
**     sqlite3WhereEnd()
**     Gosub flush_partition
**  
**   flush_partition:
**     Once {
**       OpenDup (iEphCsr -> csrStart)
**       OpenDup (iEphCsr -> csrEnd)
**     }
**     regStart = <expr1>                // PRECEDING expression
**     regEnd = <expr2>                  // FOLLOWING expression
**     if( regStart<0 || regEnd<0 ){ error! }
**     Rewind (csr,csrStart,csrEnd)      // if EOF goto flush_partition_done
**       Next(csrEnd)                    // if EOF skip Aggstep
**       Aggstep (csrEnd)
**       if( (regEnd--)<=0 ){
**         AggFinal (xValue)
**         Gosub addrGosub
**         Next(csr)                // if EOF goto flush_partition_done
**         if( (regStart--)<=0 ){
**           AggStep (csrStart, xInverse)
**           Next(csrStart)
**         }
**       }
**   flush_partition_done:
**     ResetSorter (csr)
**     Return
**
** ROWS BETWEEN <expr> PRECEDING    AND CURRENT ROW
** ROWS BETWEEN CURRENT ROW         AND <expr> FOLLOWING
** ROWS BETWEEN UNBOUNDED PRECEDING AND <expr> FOLLOWING
**
**   These are similar to the above. For "CURRENT ROW", intialize the
**   register to 0. For "UNBOUNDED PRECEDING" to infinity.
**
** ROWS BETWEEN <expr> PRECEDING    AND UNBOUNDED FOLLOWING
** ROWS BETWEEN CURRENT ROW         AND UNBOUNDED FOLLOWING
**
**     Rewind (csr,csrStart,csrEnd)    // if EOF goto flush_partition_done
**     while( 1 ){
**       Next(csrEnd)                  // Exit while(1) at EOF
**       Aggstep (csrEnd)
**     }
**     while( 1 ){
**       AggFinal (xValue)
**       Gosub addrGosub
**       Next(csr)                     // if EOF goto flush_partition_done
**       if( (regStart--)<=0 ){
**         AggStep (csrStart, xInverse)
**         Next(csrStart)
**       }
**     }
**
**   For the "CURRENT ROW AND UNBOUNDED FOLLOWING" case, the final if() 
**   condition is always true (as if regStart were initialized to 0).
**
** RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
** 
**   This is the only RANGE case handled by this routine. It modifies the
**   second while( 1 ) loop in "ROWS BETWEEN CURRENT ... UNBOUNDED..." to
**   be:
**
**     while( 1 ){
**       AggFinal (xValue)
**       while( 1 ){
**         regPeer++
**         Gosub addrGosub
**         Next(csr)                     // if EOF goto flush_partition_done
**         if( new peer ) break;
**       }
**       while( (regPeer--)>0 ){
**         AggStep (csrStart, xInverse)
**         Next(csrStart)
**       }
**     }
**
** ROWS BETWEEN <expr> FOLLOWING    AND <expr> FOLLOWING
**
**   regEnd = regEnd - regStart
**   Rewind (csr,csrStart,csrEnd)   // if EOF goto flush_partition_done
**     Aggstep (csrEnd)
**     Next(csrEnd)                 // if EOF fall-through
**     if( (regEnd--)<=0 ){
**       if( (regStart--)<=0 ){
**         AggFinal (xValue)
**         Gosub addrGosub
**         Next(csr)              // if EOF goto flush_partition_done
**       }
**       AggStep (csrStart, xInverse)
**       Next (csrStart)
**     }
**
** ROWS BETWEEN <expr> PRECEDING    AND <expr> PRECEDING
**
**   Replace the bit after "Rewind" in the above with:
**
**     if( (regEnd--)<=0 ){
**       AggStep (csrEnd)
**       Next (csrEnd)
**     }
**     AggFinal (xValue)
**     Gosub addrGosub
**     Next(csr)                  // if EOF goto flush_partition_done
**     if( (regStart--)<=0 ){
**       AggStep (csr2, xInverse)
**       Next (csr2)
**     }
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
  int lblFlushPart;               /* Label for "Gosub flush_partition" */
  int lblFlushDone;               /* Label for "Gosub flush_partition_done" */

  int reg = pParse->nMem+1;
  int regRecord = reg+nSub;
  int regRowid = regRecord+1;
  int addr;
  int csrStart = pParse->nTab++;
  int csrEnd = pParse->nTab++;
  int regStart;                    /* Value of <expr> PRECEDING */
  int regEnd;                      /* Value of <expr> FOLLOWING */
  int addrNext;
  int addrGoto;
  int addrTop;
  int addrIfPos1;
  int addrIfPos2;

  int regPeer = 0;                 /* Number of peers in current group */
  int regPeerVal = 0;              /* Array of values identifying peer group */
  int iPeer = 0;                   /* Column offset in eph-table of peer vals */
  int nPeerVal;                    /* Number of peer values */
  int bRange = 0;

  assert( pMWin->eStart==TK_PRECEDING 
       || pMWin->eStart==TK_CURRENT 
       || pMWin->eStart==TK_FOLLOWING 
       || pMWin->eStart==TK_UNBOUNDED 
  );
  assert( pMWin->eEnd==TK_FOLLOWING 
       || pMWin->eEnd==TK_CURRENT 
       || pMWin->eEnd==TK_UNBOUNDED 
       || pMWin->eEnd==TK_PRECEDING 
  );

  if( pMWin->eType==TK_RANGE 
   && pMWin->eStart==TK_CURRENT 
   && pMWin->eEnd==TK_UNBOUNDED
  ){
    bRange = 1;
  }

  pParse->nMem += nSub + 2;

  /* Allocate register and label for the "flush_partition" sub-routine. */
  regFlushPart = ++pParse->nMem;
  lblFlushPart = sqlite3VdbeMakeLabel(v);
  lblFlushDone = sqlite3VdbeMakeLabel(v);

  regStart = ++pParse->nMem;
  regEnd = ++pParse->nMem;

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
    sqlite3VdbeAddOp2(v, OP_Gosub, regFlushPart, lblFlushPart);
    sqlite3VdbeAddOp3(v, OP_Copy, regNewPart, pMWin->regPart, nPart);
  }

  /* Buffer the current row in the ephemeral table. */
  sqlite3VdbeAddOp2(v, OP_NewRowid, pMWin->iEphCsr, regRowid);
  sqlite3VdbeAddOp3(v, OP_Insert, pMWin->iEphCsr, regRecord, regRowid);

  /* End of the input loop */
  sqlite3WhereEnd(pWInfo);

  /* Invoke "flush_partition" to deal with the final (or only) partition */
  sqlite3VdbeAddOp2(v, OP_Gosub, regFlushPart, lblFlushPart);
  addrGoto = sqlite3VdbeAddOp0(v, OP_Goto);

  /* flush_partition: */
  sqlite3VdbeResolveLabel(v, lblFlushPart);
  sqlite3VdbeAddOp2(v, OP_Once, 0, sqlite3VdbeCurrentAddr(v)+3);
  sqlite3VdbeAddOp2(v, OP_OpenDup, csrStart, pMWin->iEphCsr);
  sqlite3VdbeAddOp2(v, OP_OpenDup, csrEnd, pMWin->iEphCsr);

  /* If either regStart or regEnd are not non-negative integers, throw 
  ** an exception.  */
  if( pMWin->pStart ){
    sqlite3ExprCode(pParse, pMWin->pStart, regStart);
    windowCheckFrameValue(pParse, regStart, 0);
  }
  if( pMWin->pEnd ){
    sqlite3ExprCode(pParse, pMWin->pEnd, regEnd);
    windowCheckFrameValue(pParse, regEnd, 1);
    if( pMWin->pStart && pMWin->eStart==TK_FOLLOWING ){
      assert( pMWin->eEnd==TK_FOLLOWING );
      sqlite3VdbeAddOp3(v, OP_Subtract, regStart, regEnd, regEnd);
    }
  }

  for(pWin=pMWin; pWin; pWin=pWin->pNextWin){
    sqlite3VdbeAddOp2(v, OP_Null, 0, pWin->regAccum);
  }

  sqlite3VdbeAddOp2(v, OP_Rewind, pMWin->iEphCsr, lblFlushDone);
  sqlite3VdbeAddOp2(v, OP_Rewind, csrStart, lblFlushDone);
  sqlite3VdbeChangeP5(v, 1);
  sqlite3VdbeAddOp2(v, OP_Rewind, csrEnd, lblFlushDone);
  sqlite3VdbeChangeP5(v, 1);

  /* Invoke AggStep function for each window function using the row that
  ** csrEnd currently points to. Or, if csrEnd is already at EOF,
  ** do nothing.  */
  addrTop = sqlite3VdbeCurrentAddr(v);
  if( pMWin->eEnd==TK_PRECEDING ){
    addrIfPos1 = sqlite3VdbeAddOp3(v, OP_IfPos, regEnd, 0 , 1);
  }
  sqlite3VdbeAddOp2(v, OP_Next, csrEnd, sqlite3VdbeCurrentAddr(v)+2);
  addr = sqlite3VdbeAddOp0(v, OP_Goto);
  windowAggStep(pParse, pMWin, csrEnd, 0, reg);
  if( pMWin->eEnd==TK_UNBOUNDED ){
    sqlite3VdbeAddOp2(v, OP_Goto, 0, addrTop);
    sqlite3VdbeJumpHere(v, addr);
    addrTop = sqlite3VdbeCurrentAddr(v);
  }else{
    sqlite3VdbeJumpHere(v, addr);
    if( pMWin->eEnd==TK_PRECEDING ){
      sqlite3VdbeJumpHere(v, addrIfPos1);
    }
  }

  if( pMWin->eEnd==TK_FOLLOWING ){
    addrIfPos1 = sqlite3VdbeAddOp3(v, OP_IfPos, regEnd, 0 , 1);
  }
  if( pMWin->eStart==TK_FOLLOWING ){
    addrIfPos2 = sqlite3VdbeAddOp3(v, OP_IfPos, regStart, 0 , 1);
  }
  if( bRange ){
    assert( pMWin->eStart==TK_CURRENT && pMWin->pOrderBy );
    regPeer = ++pParse->nMem;
    regPeerVal = pParse->nMem+1;
    iPeer = pMWin->nBufferCol + (pMWin->pPartition?pMWin->pPartition->nExpr:0);
    nPeerVal = pMWin->pOrderBy->nExpr;
    pParse->nMem += (2 * nPeerVal);
    for(k=0; k<nPeerVal; k++){
      sqlite3VdbeAddOp3(v, OP_Column, pMWin->iEphCsr, iPeer+k, regPeerVal+k);
    }
    sqlite3VdbeAddOp2(v, OP_Integer, 0, regPeer);
  }
  windowAggFinal(pParse, pMWin, 0);
  if( bRange ){
    sqlite3VdbeAddOp2(v, OP_AddImm, regPeer, 1);
  }
  sqlite3VdbeAddOp2(v, OP_Gosub, regGosub, addrGosub);
  sqlite3VdbeAddOp2(v, OP_Next, pMWin->iEphCsr, sqlite3VdbeCurrentAddr(v)+2);
  sqlite3VdbeAddOp2(v, OP_Goto, 0, lblFlushDone);
  if( bRange ){
    KeyInfo *pKeyInfo = sqlite3KeyInfoFromExprList(pParse, pMWin->pOrderBy,0,0);
    int addrJump = sqlite3VdbeCurrentAddr(v)-4;
    for(k=0; k<nPeerVal; k++){
      int iOut = regPeerVal + nPeerVal + k;
      sqlite3VdbeAddOp3(v, OP_Column, pMWin->iEphCsr, iPeer+k, iOut);
    }
    sqlite3VdbeAddOp3(v, OP_Compare, regPeerVal, regPeerVal+nPeerVal, nPeerVal);
    sqlite3VdbeAppendP4(v, (void*)pKeyInfo, P4_KEYINFO);
    addr = sqlite3VdbeCurrentAddr(v)+1;
    sqlite3VdbeAddOp3(v, OP_Jump, addr, addrJump, addr);
  }
  if( pMWin->eStart==TK_FOLLOWING ){
    sqlite3VdbeJumpHere(v, addrIfPos2);
  }

  if( pMWin->eStart==TK_CURRENT 
   || pMWin->eStart==TK_PRECEDING 
   || pMWin->eStart==TK_FOLLOWING 
  ){
    int addrJumpHere = 0;
    if( pMWin->eStart==TK_PRECEDING ){
      addrJumpHere = sqlite3VdbeAddOp3(v, OP_IfPos, regStart, 0 , 1);
    }
    if( bRange ){
      sqlite3VdbeAddOp3(v, OP_IfPos, regPeer, sqlite3VdbeCurrentAddr(v)+2, 1);
      addrJumpHere = sqlite3VdbeAddOp0(v, OP_Goto);
    }
    sqlite3VdbeAddOp2(v, OP_Next, csrStart, sqlite3VdbeCurrentAddr(v)+1);
    windowAggStep(pParse, pMWin, csrStart, 1, reg);
    if( bRange ){
      sqlite3VdbeAddOp2(v, OP_Goto, 0, addrJumpHere-1);
    }
    if( addrJumpHere ){
      sqlite3VdbeJumpHere(v, addrJumpHere);
    }
  }
  if( pMWin->eEnd==TK_FOLLOWING ){
    sqlite3VdbeJumpHere(v, addrIfPos1);
  }
  sqlite3VdbeAddOp2(v, OP_Goto, 0, addrTop);

  /* flush_partition_done: */
  sqlite3VdbeResolveLabel(v, lblFlushDone);
  sqlite3VdbeAddOp1(v, OP_ResetSorter, pMWin->iEphCsr);
  sqlite3VdbeAddOp1(v, OP_Return, regFlushPart);

  /* Jump to here to skip over flush_partition */
  sqlite3VdbeJumpHere(v, addrGoto);
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
*/
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
  ExprList *pPart = pMWin->pPartition;
  ExprList *pOrderBy = pMWin->pOrderBy;

  assert( pMWin->eType==TK_RANGE 
      || (pMWin->eStart==TK_UNBOUNDED && pMWin->eEnd==TK_CURRENT)
  );

  assert( (pMWin->eStart==TK_UNBOUNDED && pMWin->eEnd==TK_CURRENT)
       || (pMWin->eStart==TK_UNBOUNDED && pMWin->eEnd==TK_UNBOUNDED)
       || (pMWin->eStart==TK_CURRENT && pMWin->eEnd==TK_CURRENT)
       || (pMWin->eStart==TK_CURRENT && pMWin->eEnd==TK_UNBOUNDED && !pOrderBy)
  );

  if( pMWin->eEnd==TK_UNBOUNDED ){
    pOrderBy = 0;
  }

  pParse->nMem += nSub + 2;

  /* Martial the row returned by the sub-select into an array of 
  ** registers. */
  for(k=0; k<nSub; k++){
    sqlite3VdbeAddOp3(v, OP_Column, iSubCsr, k, reg+k);
  }

  /* Check if this is the start of a new partition or peer group. */
  if( pPart || pOrderBy ){
    int nPart = (pPart ? pPart->nExpr : 0);
    int addrGoto = 0;
    int addrJump = 0;
    int nPeer = (pOrderBy ? pOrderBy->nExpr : 0);

    if( pPart ){
      int regNewPart = reg + pMWin->nBufferCol;
      KeyInfo *pKeyInfo = sqlite3KeyInfoFromExprList(pParse, pPart, 0, 0);
      addr = sqlite3VdbeAddOp3(v, OP_Compare, regNewPart, pMWin->regPart,nPart);
      sqlite3VdbeAppendP4(v, (void*)pKeyInfo, P4_KEYINFO);
      addrJump = sqlite3VdbeAddOp3(v, OP_Jump, addr+2, 0, addr+2);
      windowAggFinal(pParse, pMWin, 1);
      if( pOrderBy ){
        addrGoto = sqlite3VdbeAddOp0(v, OP_Goto);
      }
    }

    if( pOrderBy ){
      int regNewPeer = reg + pMWin->nBufferCol + nPart;
      int regPeer = pMWin->regPart + nPart;

      if( addrJump ) sqlite3VdbeJumpHere(v, addrJump);
      if( pMWin->eType==TK_RANGE ){
        KeyInfo *pKeyInfo = sqlite3KeyInfoFromExprList(pParse, pOrderBy, 0, 0);
        addr = sqlite3VdbeAddOp3(v, OP_Compare, regNewPeer, regPeer, nPeer);
        sqlite3VdbeAppendP4(v, (void*)pKeyInfo, P4_KEYINFO);
        addrJump = sqlite3VdbeAddOp3(v, OP_Jump, addr+2, 0, addr+2);
      }else{
        addrJump = 0;
      }
      windowAggFinal(pParse, pMWin, pMWin->eStart==TK_CURRENT);
      if( addrGoto ) sqlite3VdbeJumpHere(v, addrGoto);
    }

    sqlite3VdbeAddOp2(v, OP_Gosub, regGosub, addrGosub);
    sqlite3VdbeAddOp1(v, OP_ResetSorter, pMWin->iEphCsr);
    sqlite3VdbeAddOp3(
        v, OP_Copy, reg+pMWin->nBufferCol, pMWin->regPart, nPart+nPeer-1
    );

    if( addrJump ) sqlite3VdbeJumpHere(v, addrJump);
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

  windowAggFinal(pParse, pMWin, 1);
  sqlite3VdbeAddOp2(v, OP_Gosub, regGosub, addrGosub);
}


/*
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

  if( (pMWin->eType==TK_ROWS 
   && (pMWin->eStart!=TK_UNBOUNDED||pMWin->eEnd!=TK_CURRENT||!pMWin->pOrderBy))
   || (pMWin->eStart==TK_CURRENT&&pMWin->eEnd==TK_UNBOUNDED&&pMWin->pOrderBy)
  ){
    *pbLoop = 0;
    windowCodeRowExprStep(pParse, p, pWInfo, regGosub, addrGosub);
    return;
  }

  *pbLoop = 1;
  windowCodeDefaultStep(pParse, p, pWInfo, regGosub, addrGosub);
}


