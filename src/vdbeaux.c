/*
** 2003 September 6
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains code used for creating, destroying, and populating
** a VDBE (or an "sqlite3_stmt" as it is known to the outside world.)  Prior
** to version 2.8.7, all this code was combined into the vdbe.c source file.
** But that file was getting too big so this subroutines were split out.
**
** $Id: vdbeaux.c,v 1.480 2009/08/08 18:01:08 drh Exp $
*/
#include "sqliteInt.h"
#include "vdbeInt.h"



/*
** When debugging the code generator in a symbolic debugger, one can
** set the sqlite3VdbeAddopTrace to 1 and all opcodes will be printed
** as they are added to the instruction stream.
*/
#ifdef SQLITE_DEBUG
int sqlite3VdbeAddopTrace = 0;
#endif


/*
** Create a new virtual database engine.
*/
Vdbe *sqlite3VdbeCreate(sqlite3 *db){
  Vdbe *p;
  p = sqlite3DbMallocZero(db, sizeof(Vdbe) );
  if( p==0 ) return 0;
  p->db = db;
  if( db->pVdbe ){
    db->pVdbe->pPrev = p;
  }
  p->pNext = db->pVdbe;
  p->pPrev = 0;
  db->pVdbe = p;
  p->magic = VDBE_MAGIC_INIT;
  return p;
}

/*
** Remember the SQL string for a prepared statement.
*/
void sqlite3VdbeSetSql(Vdbe *p, const char *z, int n, int isPrepareV2){
  if( p==0 ) return;
#ifdef SQLITE_OMIT_TRACE
  if( !isPrepareV2 ) return;
#endif
  assert( p->zSql==0 );
  p->zSql = sqlite3DbStrNDup(p->db, z, n);
  p->isPrepareV2 = isPrepareV2 ? 1 : 0;
}

/*
** Return the SQL associated with a prepared statement
*/
const char *sqlite3_sql(sqlite3_stmt *pStmt){
  Vdbe *p = (Vdbe *)pStmt;
  return (p->isPrepareV2 ? p->zSql : 0);
}

/*
** Swap all content between two VDBE structures.
*/
void sqlite3VdbeSwap(Vdbe *pA, Vdbe *pB){
  Vdbe tmp, *pTmp;
  char *zTmp;
  tmp = *pA;
  *pA = *pB;
  *pB = tmp;
  pTmp = pA->pNext;
  pA->pNext = pB->pNext;
  pB->pNext = pTmp;
  pTmp = pA->pPrev;
  pA->pPrev = pB->pPrev;
  pB->pPrev = pTmp;
  zTmp = pA->zSql;
  pA->zSql = pB->zSql;
  pB->zSql = zTmp;
  pB->isPrepareV2 = pA->isPrepareV2;
}

#ifdef SQLITE_DEBUG
/*
** Turn tracing on or off
*/
void sqlite3VdbeTrace(Vdbe *p, FILE *trace){
  p->trace = trace;
}
#endif

/*
** Resize the Vdbe.aOp array so that it is at least one op larger than 
** it was.
**
** If an out-of-memory error occurs while resizing the array, return
** SQLITE_NOMEM. In this case Vdbe.aOp and Vdbe.nOpAlloc remain 
** unchanged (this is so that any opcodes already allocated can be 
** correctly deallocated along with the rest of the Vdbe).
*/
static int growOpArray(Vdbe *p){
  VdbeOp *pNew;
  int nNew = (p->nOpAlloc ? p->nOpAlloc*2 : (int)(1024/sizeof(Op)));
  pNew = sqlite3DbRealloc(p->db, p->aOp, nNew*sizeof(Op));
  if( pNew ){
    p->nOpAlloc = sqlite3DbMallocSize(p->db, pNew)/sizeof(Op);
    p->aOp = pNew;
  }
  return (pNew ? SQLITE_OK : SQLITE_NOMEM);
}

/*
** Add a new instruction to the list of instructions current in the
** VDBE.  Return the address of the new instruction.
**
** Parameters:
**
**    p               Pointer to the VDBE
**
**    op              The opcode for this instruction
**
**    p1, p2, p3      Operands
**
** Use the sqlite3VdbeResolveLabel() function to fix an address and
** the sqlite3VdbeChangeP4() function to change the value of the P4
** operand.
*/
int sqlite3VdbeAddOp3(Vdbe *p, int op, int p1, int p2, int p3){
  int i;
  VdbeOp *pOp;

  i = p->nOp;
  assert( p->magic==VDBE_MAGIC_INIT );
  assert( op>0 && op<0xff );
  if( p->nOpAlloc<=i ){
    if( growOpArray(p) ){
      return 1;
    }
  }
  p->nOp++;
  pOp = &p->aOp[i];
  pOp->opcode = (u8)op;
  pOp->p5 = 0;
  pOp->p1 = p1;
  pOp->p2 = p2;
  pOp->p3 = p3;
  pOp->p4.p = 0;
  pOp->p4type = P4_NOTUSED;
  p->expired = 0;
#ifdef SQLITE_DEBUG
  pOp->zComment = 0;
  if( sqlite3VdbeAddopTrace ) sqlite3VdbePrintOp(0, i, &p->aOp[i]);
#endif
#ifdef VDBE_PROFILE
  pOp->cycles = 0;
  pOp->cnt = 0;
#endif
  return i;
}
int sqlite3VdbeAddOp0(Vdbe *p, int op){
  return sqlite3VdbeAddOp3(p, op, 0, 0, 0);
}
int sqlite3VdbeAddOp1(Vdbe *p, int op, int p1){
  return sqlite3VdbeAddOp3(p, op, p1, 0, 0);
}
int sqlite3VdbeAddOp2(Vdbe *p, int op, int p1, int p2){
  return sqlite3VdbeAddOp3(p, op, p1, p2, 0);
}


/*
** Add an opcode that includes the p4 value as a pointer.
*/
int sqlite3VdbeAddOp4(
  Vdbe *p,            /* Add the opcode to this VM */
  int op,             /* The new opcode */
  int p1,             /* The P1 operand */
  int p2,             /* The P2 operand */
  int p3,             /* The P3 operand */
  const char *zP4,    /* The P4 operand */
  int p4type          /* P4 operand type */
){
  int addr = sqlite3VdbeAddOp3(p, op, p1, p2, p3);
  sqlite3VdbeChangeP4(p, addr, zP4, p4type);
  return addr;
}

/*
** Create a new symbolic label for an instruction that has yet to be
** coded.  The symbolic label is really just a negative number.  The
** label can be used as the P2 value of an operation.  Later, when
** the label is resolved to a specific address, the VDBE will scan
** through its operation list and change all values of P2 which match
** the label into the resolved address.
**
** The VDBE knows that a P2 value is a label because labels are
** always negative and P2 values are suppose to be non-negative.
** Hence, a negative P2 value is a label that has yet to be resolved.
**
** Zero is returned if a malloc() fails.
*/
int sqlite3VdbeMakeLabel(Vdbe *p){
  int i;
  i = p->nLabel++;
  assert( p->magic==VDBE_MAGIC_INIT );
  if( i>=p->nLabelAlloc ){
    int n = p->nLabelAlloc*2 + 5;
    p->aLabel = sqlite3DbReallocOrFree(p->db, p->aLabel,
                                       n*sizeof(p->aLabel[0]));
    p->nLabelAlloc = sqlite3DbMallocSize(p->db, p->aLabel)/sizeof(p->aLabel[0]);
  }
  if( p->aLabel ){
    p->aLabel[i] = -1;
  }
  return -1-i;
}

/*
** Resolve label "x" to be the address of the next instruction to
** be inserted.  The parameter "x" must have been obtained from
** a prior call to sqlite3VdbeMakeLabel().
*/
void sqlite3VdbeResolveLabel(Vdbe *p, int x){
  int j = -1-x;
  assert( p->magic==VDBE_MAGIC_INIT );
  assert( j>=0 && j<p->nLabel );
  if( p->aLabel ){
    p->aLabel[j] = p->nOp;
  }
}

#ifdef SQLITE_DEBUG

/*
** The following type and function are used to iterate through all opcodes
** in a Vdbe main program and each of the sub-programs (triggers) it may 
** invoke directly or indirectly. It should be used as follows:
**
**   Op *pOp;
**   VdbeOpIter sIter;
**
**   memset(&sIter, 0, sizeof(sIter));
**   sIter.v = v;                            // v is of type Vdbe* 
**   while( (pOp = opIterNext(&sIter)) ){
**     // Do something with pOp
**   }
**   sqlite3DbFree(v->db, sIter.apSub);
** 
*/
typedef struct VdbeOpIter VdbeOpIter;
struct VdbeOpIter {
  Vdbe *v;                   /* Vdbe to iterate through the opcodes of */
  SubProgram **apSub;        /* Array of subprograms */
  int nSub;                  /* Number of entries in apSub */
  int iAddr;                 /* Address of next instruction to return */
  int iSub;                  /* 0 = main program, 1 = first sub-program etc. */
};
static Op *opIterNext(VdbeOpIter *p){
  Vdbe *v = p->v;
  Op *pRet = 0;
  Op *aOp;
  int nOp;

  if( p->iSub<=p->nSub ){

    if( p->iSub==0 ){
      aOp = v->aOp;
      nOp = v->nOp;
    }else{
      aOp = p->apSub[p->iSub-1]->aOp;
      nOp = p->apSub[p->iSub-1]->nOp;
    }
    assert( p->iAddr<nOp );

    pRet = &aOp[p->iAddr];
    p->iAddr++;
    if( p->iAddr==nOp ){
      p->iSub++;
      p->iAddr = 0;
    }
  
    if( pRet->p4type==P4_SUBPROGRAM ){
      int nByte = (p->nSub+1)*sizeof(SubProgram*);
      int j;
      for(j=0; j<p->nSub; j++){
        if( p->apSub[j]==pRet->p4.pProgram ) break;
      }
      if( j==p->nSub ){
        p->apSub = sqlite3DbReallocOrFree(v->db, p->apSub, nByte);
        if( !p->apSub ){
          pRet = 0;
        }else{
          p->apSub[p->nSub++] = pRet->p4.pProgram;
        }
      }
    }
  }

  return pRet;
}

/*
** Check if the program stored in the VM associated with pParse may
** throw an ABORT exception (causing the statement, but not transaction
** to be rolled back). This condition is true if the main program or any
** sub-programs contains any of the following:
**
**   *  OP_Halt with P1=SQLITE_CONSTRAINT and P2=OE_Abort.
**   *  OP_HaltIfNull with P1=SQLITE_CONSTRAINT and P2=OE_Abort.
**   *  OP_Destroy
**   *  OP_VUpdate
**   *  OP_VRename
**
** Then check that the value of Parse.mayAbort is true if an
** ABORT may be thrown, or false otherwise. Return true if it does
** match, or false otherwise. This function is intended to be used as
** part of an assert statement in the compiler. Similar to:
**
**   assert( sqlite3VdbeAssertMayAbort(pParse->pVdbe, pParse->mayAbort) );
*/
int sqlite3VdbeAssertMayAbort(Vdbe *v, int mayAbort){
  int hasAbort = 0;
  Op *pOp;
  VdbeOpIter sIter;
  memset(&sIter, 0, sizeof(sIter));
  sIter.v = v;

  while( (pOp = opIterNext(&sIter))!=0 ){
    int opcode = pOp->opcode;
    if( opcode==OP_Destroy || opcode==OP_VUpdate || opcode==OP_VRename 
     || ((opcode==OP_Halt || opcode==OP_HaltIfNull) 
      && (pOp->p1==SQLITE_CONSTRAINT && pOp->p2==OE_Abort))
    ){
      hasAbort = 1;
      break;
    }
  }
  sqlite3DbFree(v->db, sIter.apSub);

  /* Return true if hasAbort==mayAbort. Or if a malloc failure occured.
  ** If malloc failed, then the while() loop above may not have iterated
  ** through all opcodes and hasAbort may be set incorrectly. Return
  ** true for this case to prevent the assert() in the callers frame
  ** from failing.  */
  return ( v->db->mallocFailed || hasAbort==mayAbort );
}
#endif

/*
** Loop through the program looking for P2 values that are negative
** on jump instructions.  Each such value is a label.  Resolve the
** label by setting the P2 value to its correct non-zero value.
**
** This routine is called once after all opcodes have been inserted.
**
** Variable *pMaxFuncArgs is set to the maximum value of any P2 argument 
** to an OP_Function, OP_AggStep or OP_VFilter opcode. This is used by 
** sqlite3VdbeMakeReady() to size the Vdbe.apArg[] array.
*/
static void resolveP2Values(Vdbe *p, int *pMaxFuncArgs){
  int i;
  int nMaxArgs = *pMaxFuncArgs;
  Op *pOp;
  int *aLabel = p->aLabel;
  p->readOnly = 1;
  for(pOp=p->aOp, i=p->nOp-1; i>=0; i--, pOp++){
    u8 opcode = pOp->opcode;

    if( opcode==OP_Function || opcode==OP_AggStep ){
      if( pOp->p5>nMaxArgs ) nMaxArgs = pOp->p5;
#ifndef SQLITE_OMIT_VIRTUALTABLE
    }else if( opcode==OP_VUpdate ){
      if( pOp->p2>nMaxArgs ) nMaxArgs = pOp->p2;
#endif
    }else if( opcode==OP_Transaction && pOp->p2!=0 ){
      p->readOnly = 0;
#ifndef SQLITE_OMIT_VIRTUALTABLE
    }else if( opcode==OP_VFilter ){
      int n;
      assert( p->nOp - i >= 3 );
      assert( pOp[-1].opcode==OP_Integer );
      n = pOp[-1].p1;
      if( n>nMaxArgs ) nMaxArgs = n;
#endif
    }

    if( sqlite3VdbeOpcodeHasProperty(opcode, OPFLG_JUMP) && pOp->p2<0 ){
      assert( -1-pOp->p2<p->nLabel );
      pOp->p2 = aLabel[-1-pOp->p2];
    }
  }
  sqlite3DbFree(p->db, p->aLabel);
  p->aLabel = 0;

  *pMaxFuncArgs = nMaxArgs;
}

/*
** Return the address of the next instruction to be inserted.
*/
int sqlite3VdbeCurrentAddr(Vdbe *p){
  assert( p->magic==VDBE_MAGIC_INIT );
  return p->nOp;
}

/*
** This function returns a pointer to the array of opcodes associated with
** the Vdbe passed as the first argument. It is the callers responsibility
** to arrange for the returned array to be eventually freed using the 
** vdbeFreeOpArray() function.
**
** Before returning, *pnOp is set to the number of entries in the returned
** array. Also, *pnMaxArg is set to the larger of its current value and 
** the number of entries in the Vdbe.apArg[] array required to execute the 
** returned program.
*/
VdbeOp *sqlite3VdbeTakeOpArray(Vdbe *p, int *pnOp, int *pnMaxArg){
  VdbeOp *aOp = p->aOp;
  assert( aOp && !p->db->mallocFailed );

  /* Check that sqlite3VdbeUsesBtree() was not called on this VM */
  assert( p->aMutex.nMutex==0 );

  resolveP2Values(p, pnMaxArg);
  *pnOp = p->nOp;
  p->aOp = 0;
  return aOp;
}

/*
** Add a whole list of operations to the operation stack.  Return the
** address of the first operation added.
*/
int sqlite3VdbeAddOpList(Vdbe *p, int nOp, VdbeOpList const *aOp){
  int addr;
  assert( p->magic==VDBE_MAGIC_INIT );
  if( p->nOp + nOp > p->nOpAlloc && growOpArray(p) ){
    return 0;
  }
  addr = p->nOp;
  if( ALWAYS(nOp>0) ){
    int i;
    VdbeOpList const *pIn = aOp;
    for(i=0; i<nOp; i++, pIn++){
      int p2 = pIn->p2;
      VdbeOp *pOut = &p->aOp[i+addr];
      pOut->opcode = pIn->opcode;
      pOut->p1 = pIn->p1;
      if( p2<0 && sqlite3VdbeOpcodeHasProperty(pOut->opcode, OPFLG_JUMP) ){
        pOut->p2 = addr + ADDR(p2);
      }else{
        pOut->p2 = p2;
      }
      pOut->p3 = pIn->p3;
      pOut->p4type = P4_NOTUSED;
      pOut->p4.p = 0;
      pOut->p5 = 0;
#ifdef SQLITE_DEBUG
      pOut->zComment = 0;
      if( sqlite3VdbeAddopTrace ){
        sqlite3VdbePrintOp(0, i+addr, &p->aOp[i+addr]);
      }
#endif
    }
    p->nOp += nOp;
  }
  return addr;
}

/*
** Change the value of the P1 operand for a specific instruction.
** This routine is useful when a large program is loaded from a
** static array using sqlite3VdbeAddOpList but we want to make a
** few minor changes to the program.
*/
void sqlite3VdbeChangeP1(Vdbe *p, int addr, int val){
  assert( p!=0 );
  assert( addr>=0 );
  if( p->nOp>addr ){
    p->aOp[addr].p1 = val;
  }
}

/*
** Change the value of the P2 operand for a specific instruction.
** This routine is useful for setting a jump destination.
*/
void sqlite3VdbeChangeP2(Vdbe *p, int addr, int val){
  assert( p!=0 );
  assert( addr>=0 );
  if( p->nOp>addr ){
    p->aOp[addr].p2 = val;
  }
}

/*
** Change the value of the P3 operand for a specific instruction.
*/
void sqlite3VdbeChangeP3(Vdbe *p, int addr, int val){
  assert( p!=0 );
  assert( addr>=0 );
  if( p->nOp>addr ){
    p->aOp[addr].p3 = val;
  }
}

/*
** Change the value of the P5 operand for the most recently
** added operation.
*/
void sqlite3VdbeChangeP5(Vdbe *p, u8 val){
  assert( p!=0 );
  if( p->aOp ){
    assert( p->nOp>0 );
    p->aOp[p->nOp-1].p5 = val;
  }
}

/*
** Change the P2 operand of instruction addr so that it points to
** the address of the next instruction to be coded.
*/
void sqlite3VdbeJumpHere(Vdbe *p, int addr){
  sqlite3VdbeChangeP2(p, addr, p->nOp);
}


/*
** If the input FuncDef structure is ephemeral, then free it.  If
** the FuncDef is not ephermal, then do nothing.
*/
static void freeEphemeralFunction(sqlite3 *db, FuncDef *pDef){
  if( ALWAYS(pDef) && (pDef->flags & SQLITE_FUNC_EPHEM)!=0 ){
    sqlite3DbFree(db, pDef);
  }
}

/*
** Delete a P4 value if necessary.
*/
static void freeP4(sqlite3 *db, int p4type, void *p4){
  if( p4 ){
    switch( p4type ){
      case P4_REAL:
      case P4_INT64:
      case P4_MPRINTF:
      case P4_DYNAMIC:
      case P4_KEYINFO:
      case P4_INTARRAY:
      case P4_KEYINFO_HANDOFF: {
        sqlite3DbFree(db, p4);
        break;
      }
      case P4_VDBEFUNC: {
        VdbeFunc *pVdbeFunc = (VdbeFunc *)p4;
        freeEphemeralFunction(db, pVdbeFunc->pFunc);
        sqlite3VdbeDeleteAuxData(pVdbeFunc, 0);
        sqlite3DbFree(db, pVdbeFunc);
        break;
      }
      case P4_FUNCDEF: {
        freeEphemeralFunction(db, (FuncDef*)p4);
        break;
      }
      case P4_MEM: {
        sqlite3ValueFree((sqlite3_value*)p4);
        break;
      }
      case P4_VTAB : {
        sqlite3VtabUnlock((VTable *)p4);
        break;
      }
      case P4_SUBPROGRAM : {
        sqlite3VdbeProgramDelete(db, (SubProgram *)p4, 1);
        break;
      }
    }
  }
}

/*
** Free the space allocated for aOp and any p4 values allocated for the
** opcodes contained within. If aOp is not NULL it is assumed to contain 
** nOp entries. 
*/
static void vdbeFreeOpArray(sqlite3 *db, Op *aOp, int nOp){
  if( aOp ){
    Op *pOp;
    for(pOp=aOp; pOp<&aOp[nOp]; pOp++){
      freeP4(db, pOp->p4type, pOp->p4.p);
#ifdef SQLITE_DEBUG
      sqlite3DbFree(db, pOp->zComment);
#endif     
    }
  }
  sqlite3DbFree(db, aOp);
}

/*
** Decrement the ref-count on the SubProgram structure passed as the
** second argument. If the ref-count reaches zero, free the structure.
**
** The array of VDBE opcodes stored as SubProgram.aOp is freed if
** either the ref-count reaches zero or parameter freeop is non-zero.
**
** Since the array of opcodes pointed to by SubProgram.aOp may directly
** or indirectly contain a reference to the SubProgram structure itself.
** By passing a non-zero freeop parameter, the caller may ensure that all
** SubProgram structures and their aOp arrays are freed, even when there
** are such circular references.
*/
void sqlite3VdbeProgramDelete(sqlite3 *db, SubProgram *p, int freeop){
  if( p ){
    assert( p->nRef>0 );
    if( freeop || p->nRef==1 ){
      Op *aOp = p->aOp;
      p->aOp = 0;
      vdbeFreeOpArray(db, aOp, p->nOp);
      p->nOp = 0;
    }
    p->nRef--;
    if( p->nRef==0 ){
      sqlite3DbFree(db, p);
    }
  }
}


/*
** Change N opcodes starting at addr to No-ops.
*/
void sqlite3VdbeChangeToNoop(Vdbe *p, int addr, int N){
  if( p->aOp ){
    VdbeOp *pOp = &p->aOp[addr];
    sqlite3 *db = p->db;
    while( N-- ){
      freeP4(db, pOp->p4type, pOp->p4.p);
      memset(pOp, 0, sizeof(pOp[0]));
      pOp->opcode = OP_Noop;
      pOp++;
    }
  }
}

/*
** Change the value of the P4 operand for a specific instruction.
** This routine is useful when a large program is loaded from a
** static array using sqlite3VdbeAddOpList but we want to make a
** few minor changes to the program.
**
** If n>=0 then the P4 operand is dynamic, meaning that a copy of
** the string is made into memory obtained from sqlite3_malloc().
** A value of n==0 means copy bytes of zP4 up to and including the
** first null byte.  If n>0 then copy n+1 bytes of zP4.
**
** If n==P4_KEYINFO it means that zP4 is a pointer to a KeyInfo structure.
** A copy is made of the KeyInfo structure into memory obtained from
** sqlite3_malloc, to be freed when the Vdbe is finalized.
** n==P4_KEYINFO_HANDOFF indicates that zP4 points to a KeyInfo structure
** stored in memory that the caller has obtained from sqlite3_malloc. The 
** caller should not free the allocation, it will be freed when the Vdbe is
** finalized.
** 
** Other values of n (P4_STATIC, P4_COLLSEQ etc.) indicate that zP4 points
** to a string or structure that is guaranteed to exist for the lifetime of
** the Vdbe. In these cases we can just copy the pointer.
**
** If addr<0 then change P4 on the most recently inserted instruction.
*/
void sqlite3VdbeChangeP4(Vdbe *p, int addr, const char *zP4, int n){
  Op *pOp;
  sqlite3 *db;
  assert( p!=0 );
  db = p->db;
  assert( p->magic==VDBE_MAGIC_INIT );
  if( p->aOp==0 || db->mallocFailed ){
    if ( n!=P4_KEYINFO && n!=P4_VTAB ) {
      freeP4(db, n, (void*)*(char**)&zP4);
    }
    return;
  }
  assert( p->nOp>0 );
  assert( addr<p->nOp );
  if( addr<0 ){
    addr = p->nOp - 1;
  }
  pOp = &p->aOp[addr];
  freeP4(db, pOp->p4type, pOp->p4.p);
  pOp->p4.p = 0;
  if( n==P4_INT32 ){
    /* Note: this cast is safe, because the origin data point was an int
    ** that was cast to a (const char *). */
    pOp->p4.i = SQLITE_PTR_TO_INT(zP4);
    pOp->p4type = P4_INT32;
  }else if( zP4==0 ){
    pOp->p4.p = 0;
    pOp->p4type = P4_NOTUSED;
  }else if( n==P4_KEYINFO ){
    KeyInfo *pKeyInfo;
    int nField, nByte;

    nField = ((KeyInfo*)zP4)->nField;
    nByte = sizeof(*pKeyInfo) + (nField-1)*sizeof(pKeyInfo->aColl[0]) + nField;
    pKeyInfo = sqlite3Malloc( nByte );
    pOp->p4.pKeyInfo = pKeyInfo;
    if( pKeyInfo ){
      u8 *aSortOrder;
      memcpy(pKeyInfo, zP4, nByte);
      aSortOrder = pKeyInfo->aSortOrder;
      if( aSortOrder ){
        pKeyInfo->aSortOrder = (unsigned char*)&pKeyInfo->aColl[nField];
        memcpy(pKeyInfo->aSortOrder, aSortOrder, nField);
      }
      pOp->p4type = P4_KEYINFO;
    }else{
      p->db->mallocFailed = 1;
      pOp->p4type = P4_NOTUSED;
    }
  }else if( n==P4_KEYINFO_HANDOFF ){
    pOp->p4.p = (void*)zP4;
    pOp->p4type = P4_KEYINFO;
  }else if( n==P4_VTAB ){
    pOp->p4.p = (void*)zP4;
    pOp->p4type = P4_VTAB;
    sqlite3VtabLock((VTable *)zP4);
    assert( ((VTable *)zP4)->db==p->db );
  }else if( n<0 ){
    pOp->p4.p = (void*)zP4;
    pOp->p4type = (signed char)n;
  }else{
    if( n==0 ) n = sqlite3Strlen30(zP4);
    pOp->p4.z = sqlite3DbStrNDup(p->db, zP4, n);
    pOp->p4type = P4_DYNAMIC;
  }
}

#ifndef NDEBUG
/*
** Change the comment on the the most recently coded instruction.  Or
** insert a No-op and add the comment to that new instruction.  This
** makes the code easier to read during debugging.  None of this happens
** in a production build.
*/
void sqlite3VdbeComment(Vdbe *p, const char *zFormat, ...){
  va_list ap;
  if( !p ) return;
  assert( p->nOp>0 || p->aOp==0 );
  assert( p->aOp==0 || p->aOp[p->nOp-1].zComment==0 || p->db->mallocFailed );
  if( p->nOp ){
    char **pz = &p->aOp[p->nOp-1].zComment;
    va_start(ap, zFormat);
    sqlite3DbFree(p->db, *pz);
    *pz = sqlite3VMPrintf(p->db, zFormat, ap);
    va_end(ap);
  }
}
void sqlite3VdbeNoopComment(Vdbe *p, const char *zFormat, ...){
  va_list ap;
  if( !p ) return;
  sqlite3VdbeAddOp0(p, OP_Noop);
  assert( p->nOp>0 || p->aOp==0 );
  assert( p->aOp==0 || p->aOp[p->nOp-1].zComment==0 || p->db->mallocFailed );
  if( p->nOp ){
    char **pz = &p->aOp[p->nOp-1].zComment;
    va_start(ap, zFormat);
    sqlite3DbFree(p->db, *pz);
    *pz = sqlite3VMPrintf(p->db, zFormat, ap);
    va_end(ap);
  }
}
#endif  /* NDEBUG */

/*
** Return the opcode for a given address.  If the address is -1, then
** return the most recently inserted opcode.
**
** If a memory allocation error has occurred prior to the calling of this
** routine, then a pointer to a dummy VdbeOp will be returned.  That opcode
** is readable and writable, but it has no effect.  The return of a dummy
** opcode allows the call to continue functioning after a OOM fault without
** having to check to see if the return from this routine is a valid pointer.
**
** About the #ifdef SQLITE_OMIT_TRACE:  Normally, this routine is never called
** unless p->nOp>0.  This is because in the absense of SQLITE_OMIT_TRACE,
** an OP_Trace instruction is always inserted by sqlite3VdbeGet() as soon as
** a new VDBE is created.  So we are free to set addr to p->nOp-1 without
** having to double-check to make sure that the result is non-negative. But
** if SQLITE_OMIT_TRACE is defined, the OP_Trace is omitted and we do need to
** check the value of p->nOp-1 before continuing.
*/
VdbeOp *sqlite3VdbeGetOp(Vdbe *p, int addr){
  static VdbeOp dummy;
  assert( p->magic==VDBE_MAGIC_INIT );
  if( addr<0 ){
#ifdef SQLITE_OMIT_TRACE
    if( p->nOp==0 ) return &dummy;
#endif
    addr = p->nOp - 1;
  }
  assert( (addr>=0 && addr<p->nOp) || p->db->mallocFailed );
  if( p->db->mallocFailed ){
    return &dummy;
  }else{
    return &p->aOp[addr];
  }
}

#if !defined(SQLITE_OMIT_EXPLAIN) || !defined(NDEBUG) \
     || defined(VDBE_PROFILE) || defined(SQLITE_DEBUG)
/*
** Compute a string that describes the P4 parameter for an opcode.
** Use zTemp for any required temporary buffer space.
*/
static char *displayP4(Op *pOp, char *zTemp, int nTemp){
  char *zP4 = zTemp;
  assert( nTemp>=20 );
  switch( pOp->p4type ){
    case P4_KEYINFO_STATIC:
    case P4_KEYINFO: {
      int i, j;
      KeyInfo *pKeyInfo = pOp->p4.pKeyInfo;
      sqlite3_snprintf(nTemp, zTemp, "keyinfo(%d", pKeyInfo->nField);
      i = sqlite3Strlen30(zTemp);
      for(j=0; j<pKeyInfo->nField; j++){
        CollSeq *pColl = pKeyInfo->aColl[j];
        if( pColl ){
          int n = sqlite3Strlen30(pColl->zName);
          if( i+n>nTemp-6 ){
            memcpy(&zTemp[i],",...",4);
            break;
          }
          zTemp[i++] = ',';
          if( pKeyInfo->aSortOrder && pKeyInfo->aSortOrder[j] ){
            zTemp[i++] = '-';
          }
          memcpy(&zTemp[i], pColl->zName,n+1);
          i += n;
        }else if( i+4<nTemp-6 ){
          memcpy(&zTemp[i],",nil",4);
          i += 4;
        }
      }
      zTemp[i++] = ')';
      zTemp[i] = 0;
      assert( i<nTemp );
      break;
    }
    case P4_COLLSEQ: {
      CollSeq *pColl = pOp->p4.pColl;
      sqlite3_snprintf(nTemp, zTemp, "collseq(%.20s)", pColl->zName);
      break;
    }
    case P4_FUNCDEF: {
      FuncDef *pDef = pOp->p4.pFunc;
      sqlite3_snprintf(nTemp, zTemp, "%s(%d)", pDef->zName, pDef->nArg);
      break;
    }
    case P4_INT64: {
      sqlite3_snprintf(nTemp, zTemp, "%lld", *pOp->p4.pI64);
      break;
    }
    case P4_INT32: {
      sqlite3_snprintf(nTemp, zTemp, "%d", pOp->p4.i);
      break;
    }
    case P4_REAL: {
      sqlite3_snprintf(nTemp, zTemp, "%.16g", *pOp->p4.pReal);
      break;
    }
    case P4_MEM: {
      Mem *pMem = pOp->p4.pMem;
      assert( (pMem->flags & MEM_Null)==0 );
      if( pMem->flags & MEM_Str ){
        zP4 = pMem->z;
      }else if( pMem->flags & MEM_Int ){
        sqlite3_snprintf(nTemp, zTemp, "%lld", pMem->u.i);
      }else if( pMem->flags & MEM_Real ){
        sqlite3_snprintf(nTemp, zTemp, "%.16g", pMem->r);
      }else{
        assert( pMem->flags & MEM_Blob );
        zP4 = "(blob)";
      }
      break;
    }
#ifndef SQLITE_OMIT_VIRTUALTABLE
    case P4_VTAB: {
      sqlite3_vtab *pVtab = pOp->p4.pVtab->pVtab;
      sqlite3_snprintf(nTemp, zTemp, "vtab:%p:%p", pVtab, pVtab->pModule);
      break;
    }
#endif
    case P4_INTARRAY: {
      sqlite3_snprintf(nTemp, zTemp, "intarray");
      break;
    }
    case P4_SUBPROGRAM: {
      sqlite3_snprintf(nTemp, zTemp, "program");
      break;
    }
    default: {
      zP4 = pOp->p4.z;
      if( zP4==0 ){
        zP4 = zTemp;
        zTemp[0] = 0;
      }
    }
  }
  assert( zP4!=0 );
  return zP4;
}
#endif

/*
** Declare to the Vdbe that the BTree object at db->aDb[i] is used.
*/
void sqlite3VdbeUsesBtree(Vdbe *p, int i){
  int mask;
  assert( i>=0 && i<p->db->nDb && i<sizeof(u32)*8 );
  assert( i<(int)sizeof(p->btreeMask)*8 );
  mask = ((u32)1)<<i;
  if( (p->btreeMask & mask)==0 ){
    p->btreeMask |= mask;
    sqlite3BtreeMutexArrayInsert(&p->aMutex, p->db->aDb[i].pBt);
  }
}


#if defined(VDBE_PROFILE) || defined(SQLITE_DEBUG)
/*
** Print a single opcode.  This routine is used for debugging only.
*/
void sqlite3VdbePrintOp(FILE *pOut, int pc, Op *pOp){
  char *zP4;
  char zPtr[50];
  static const char *zFormat1 = "%4d %-13s %4d %4d %4d %-4s %.2X %s\n";
  if( pOut==0 ) pOut = stdout;
  zP4 = displayP4(pOp, zPtr, sizeof(zPtr));
  fprintf(pOut, zFormat1, pc, 
      sqlite3OpcodeName(pOp->opcode), pOp->p1, pOp->p2, pOp->p3, zP4, pOp->p5,
#ifdef SQLITE_DEBUG
      pOp->zComment ? pOp->zComment : ""
#else
      ""
#endif
  );
  fflush(pOut);
}
#endif

/*
** Release an array of N Mem elements
*/
static void releaseMemArray(Mem *p, int N){
  if( p && N ){
    Mem *pEnd;
    sqlite3 *db = p->db;
    u8 malloc_failed = db->mallocFailed;
    for(pEnd=&p[N]; p<pEnd; p++){
      assert( (&p[1])==pEnd || p[0].db==p[1].db );

      /* This block is really an inlined version of sqlite3VdbeMemRelease()
      ** that takes advantage of the fact that the memory cell value is 
      ** being set to NULL after releasing any dynamic resources.
      **
      ** The justification for duplicating code is that according to 
      ** callgrind, this causes a certain test case to hit the CPU 4.7 
      ** percent less (x86 linux, gcc version 4.1.2, -O6) than if 
      ** sqlite3MemRelease() were called from here. With -O2, this jumps
      ** to 6.6 percent. The test case is inserting 1000 rows into a table 
      ** with no indexes using a single prepared INSERT statement, bind() 
      ** and reset(). Inserts are grouped into a transaction.
      */
      if( p->flags&(MEM_Agg|MEM_Dyn|MEM_Frame|MEM_RowSet) ){
        sqlite3VdbeMemRelease(p);
      }else if( p->zMalloc ){
        sqlite3DbFree(db, p->zMalloc);
        p->zMalloc = 0;
      }

      p->flags = MEM_Null;
    }
    db->mallocFailed = malloc_failed;
  }
}

/*
** Delete a VdbeFrame object and its contents. VdbeFrame objects are
** allocated by the OP_Program opcode in sqlite3VdbeExec().
*/
void sqlite3VdbeFrameDelete(VdbeFrame *p){
  int i;
  Mem *aMem = VdbeFrameMem(p);
  VdbeCursor **apCsr = (VdbeCursor **)&aMem[p->nChildMem];
  for(i=0; i<p->nChildCsr; i++){
    sqlite3VdbeFreeCursor(p->v, apCsr[i]);
  }
  releaseMemArray(aMem, p->nChildMem);
  sqlite3DbFree(p->v->db, p);
}


#ifdef SQLITE_ENABLE_MEMORY_MANAGEMENT
int sqlite3VdbeReleaseBuffers(Vdbe *p){
  int ii;
  int nFree = 0;
  assert( sqlite3_mutex_held(p->db->mutex) );
  for(ii=1; ii<=p->nMem; ii++){
    Mem *pMem = &p->aMem[ii];
    if( pMem->flags & MEM_RowSet ){
      sqlite3RowSetClear(pMem->u.pRowSet);
    }
    if( pMem->z && pMem->flags&MEM_Dyn ){
      assert( !pMem->xDel );
      nFree += sqlite3DbMallocSize(pMem->db, pMem->z);
      sqlite3VdbeMemRelease(pMem);
    }
  }
  return nFree;
}
#endif

#ifndef SQLITE_OMIT_EXPLAIN
/*
** Give a listing of the program in the virtual machine.
**
** The interface is the same as sqlite3VdbeExec().  But instead of
** running the code, it invokes the callback once for each instruction.
** This feature is used to implement "EXPLAIN".
**
** When p->explain==1, each instruction is listed.  When
** p->explain==2, only OP_Explain instructions are listed and these
** are shown in a different format.  p->explain==2 is used to implement
** EXPLAIN QUERY PLAN.
*/
int sqlite3VdbeList(
  Vdbe *p                   /* The VDBE */
){
  int nRow;                            /* Total number of rows to return */
  int nSub = 0;                        /* Number of sub-vdbes seen so far */
  SubProgram **apSub = 0;              /* Array of sub-vdbes */
  Mem *pSub = 0;
  sqlite3 *db = p->db;
  int i;
  int rc = SQLITE_OK;
  Mem *pMem = p->pResultSet = &p->aMem[1];

  assert( p->explain );
  assert( p->magic==VDBE_MAGIC_RUN );
  assert( db->magic==SQLITE_MAGIC_BUSY );
  assert( p->rc==SQLITE_OK || p->rc==SQLITE_BUSY || p->rc==SQLITE_NOMEM );

  /* Even though this opcode does not use dynamic strings for
  ** the result, result columns may become dynamic if the user calls
  ** sqlite3_column_text16(), causing a translation to UTF-16 encoding.
  */
  releaseMemArray(pMem, 8);

  if( p->rc==SQLITE_NOMEM ){
    /* This happens if a malloc() inside a call to sqlite3_column_text() or
    ** sqlite3_column_text16() failed.  */
    db->mallocFailed = 1;
    return SQLITE_ERROR;
  }

  /* Figure out total number of rows that will be returned by this 
  ** EXPLAIN program.  */
  nRow = p->nOp;
  if( p->explain==1 ){
    pSub = &p->aMem[9];
    if( pSub->flags&MEM_Blob ){
      nSub = pSub->n/sizeof(Vdbe*);
      apSub = (SubProgram **)pSub->z;
    }
    for(i=0; i<nSub; i++){
      nRow += apSub[i]->nOp;
    }
  }

  do{
    i = p->pc++;
  }while( i<nRow && p->explain==2 && p->aOp[i].opcode!=OP_Explain );
  if( i>=nRow ){
    p->rc = SQLITE_OK;
    rc = SQLITE_DONE;
  }else if( db->u1.isInterrupted ){
    p->rc = SQLITE_INTERRUPT;
    rc = SQLITE_ERROR;
    sqlite3SetString(&p->zErrMsg, db, "%s", sqlite3ErrStr(p->rc));
  }else{
    char *z;
    Op *pOp;
    if( i<p->nOp ){
      pOp = &p->aOp[i];
    }else{
      int j;
      i -= p->nOp;
      for(j=0; i>=apSub[j]->nOp; j++){
        i -= apSub[j]->nOp;
      }
      pOp = &apSub[j]->aOp[i];
    }
    if( p->explain==1 ){
      pMem->flags = MEM_Int;
      pMem->type = SQLITE_INTEGER;
      pMem->u.i = i;                                /* Program counter */
      pMem++;
  
      pMem->flags = MEM_Static|MEM_Str|MEM_Term;
      pMem->z = (char*)sqlite3OpcodeName(pOp->opcode);  /* Opcode */
      assert( pMem->z!=0 );
      pMem->n = sqlite3Strlen30(pMem->z);
      pMem->type = SQLITE_TEXT;
      pMem->enc = SQLITE_UTF8;
      pMem++;

      if( pOp->p4type==P4_SUBPROGRAM ){
        int nByte = (nSub+1)*sizeof(SubProgram*);
        int j;
        for(j=0; j<nSub; j++){
          if( apSub[j]==pOp->p4.pProgram ) break;
        }
        if( j==nSub && SQLITE_OK==sqlite3VdbeMemGrow(pSub, nByte, 1) ){
          apSub = (SubProgram **)pSub->z;
          apSub[nSub++] = pOp->p4.pProgram;
          pSub->flags |= MEM_Blob;
          pSub->n = nSub*sizeof(SubProgram*);
        }
      }
    }

    pMem->flags = MEM_Int;
    pMem->u.i = pOp->p1;                          /* P1 */
    pMem->type = SQLITE_INTEGER;
    pMem++;

    pMem->flags = MEM_Int;
    pMem->u.i = pOp->p2;                          /* P2 */
    pMem->type = SQLITE_INTEGER;
    pMem++;

    if( p->explain==1 ){
      pMem->flags = MEM_Int;
      pMem->u.i = pOp->p3;                          /* P3 */
      pMem->type = SQLITE_INTEGER;
      pMem++;
    }

    if( sqlite3VdbeMemGrow(pMem, 32, 0) ){            /* P4 */
      assert( p->db->mallocFailed );
      return SQLITE_ERROR;
    }
    pMem->flags = MEM_Dyn|MEM_Str|MEM_Term;
    z = displayP4(pOp, pMem->z, 32);
    if( z!=pMem->z ){
      sqlite3VdbeMemSetStr(pMem, z, -1, SQLITE_UTF8, 0);
    }else{
      assert( pMem->z!=0 );
      pMem->n = sqlite3Strlen30(pMem->z);
      pMem->enc = SQLITE_UTF8;
    }
    pMem->type = SQLITE_TEXT;
    pMem++;

    if( p->explain==1 ){
      if( sqlite3VdbeMemGrow(pMem, 4, 0) ){
        assert( p->db->mallocFailed );
        return SQLITE_ERROR;
      }
      pMem->flags = MEM_Dyn|MEM_Str|MEM_Term;
      pMem->n = 2;
      sqlite3_snprintf(3, pMem->z, "%.2x", pOp->p5);   /* P5 */
      pMem->type = SQLITE_TEXT;
      pMem->enc = SQLITE_UTF8;
      pMem++;
  
#ifdef SQLITE_DEBUG
      if( pOp->zComment ){
        pMem->flags = MEM_Str|MEM_Term;
        pMem->z = pOp->zComment;
        pMem->n = sqlite3Strlen30(pMem->z);
        pMem->enc = SQLITE_UTF8;
        pMem->type = SQLITE_TEXT;
      }else
#endif
      {
        pMem->flags = MEM_Null;                       /* Comment */
        pMem->type = SQLITE_NULL;
      }
    }

    p->nResColumn = 8 - 5*(p->explain-1);
    p->rc = SQLITE_OK;
    rc = SQLITE_ROW;
  }
  return rc;
}
#endif /* SQLITE_OMIT_EXPLAIN */

#ifdef SQLITE_DEBUG
/*
** Print the SQL that was used to generate a VDBE program.
*/
void sqlite3VdbePrintSql(Vdbe *p){
  int nOp = p->nOp;
  VdbeOp *pOp;
  if( nOp<1 ) return;
  pOp = &p->aOp[0];
  if( pOp->opcode==OP_Trace && pOp->p4.z!=0 ){
    const char *z = pOp->p4.z;
    while( sqlite3Isspace(*z) ) z++;
    printf("SQL: [%s]\n", z);
  }
}
#endif

#if !defined(SQLITE_OMIT_TRACE) && defined(SQLITE_ENABLE_IOTRACE)
/*
** Print an IOTRACE message showing SQL content.
*/
void sqlite3VdbeIOTraceSql(Vdbe *p){
  int nOp = p->nOp;
  VdbeOp *pOp;
  if( sqlite3IoTrace==0 ) return;
  if( nOp<1 ) return;
  pOp = &p->aOp[0];
  if( pOp->opcode==OP_Trace && pOp->p4.z!=0 ){
    int i, j;
    char z[1000];
    sqlite3_snprintf(sizeof(z), z, "%s", pOp->p4.z);
    for(i=0; sqlite3Isspace(z[i]); i++){}
    for(j=0; z[i]; i++){
      if( sqlite3Isspace(z[i]) ){
        if( z[i-1]!=' ' ){
          z[j++] = ' ';
        }
      }else{
        z[j++] = z[i];
      }
    }
    z[j] = 0;
    sqlite3IoTrace("SQL %s\n", z);
  }
}
#endif /* !SQLITE_OMIT_TRACE && SQLITE_ENABLE_IOTRACE */

/*
** Allocate space from a fixed size buffer.  Make *pp point to the
** allocated space.  (Note:  pp is a char* rather than a void** to
** work around the pointer aliasing rules of C.)  *pp should initially
** be zero.  If *pp is not zero, that means that the space has already
** been allocated and this routine is a noop.
**
** nByte is the number of bytes of space needed.
**
** *ppFrom point to available space and pEnd points to the end of the
** available space.
**
** *pnByte is a counter of the number of bytes of space that have failed
** to allocate.  If there is insufficient space in *ppFrom to satisfy the
** request, then increment *pnByte by the amount of the request.
*/
static void allocSpace(
  char *pp,            /* IN/OUT: Set *pp to point to allocated buffer */
  int nByte,           /* Number of bytes to allocate */
  u8 **ppFrom,         /* IN/OUT: Allocate from *ppFrom */
  u8 *pEnd,            /* Pointer to 1 byte past the end of *ppFrom buffer */
  int *pnByte          /* If allocation cannot be made, increment *pnByte */
){
  assert( EIGHT_BYTE_ALIGNMENT(*ppFrom) );
  if( (*(void**)pp)==0 ){
    nByte = ROUND8(nByte);
    if( &(*ppFrom)[nByte] <= pEnd ){
      *(void**)pp = (void *)*ppFrom;
      *ppFrom += nByte;
    }else{
      *pnByte += nByte;
    }
  }
}

/*
** Prepare a virtual machine for execution.  This involves things such
** as allocating stack space and initializing the program counter.
** After the VDBE has be prepped, it can be executed by one or more
** calls to sqlite3VdbeExec().  
**
** This is the only way to move a VDBE from VDBE_MAGIC_INIT to
** VDBE_MAGIC_RUN.
**
** This function may be called more than once on a single virtual machine.
** The first call is made while compiling the SQL statement. Subsequent
** calls are made as part of the process of resetting a statement to be
** re-executed (from a call to sqlite3_reset()). The nVar, nMem, nCursor 
** and isExplain parameters are only passed correct values the first time
** the function is called. On subsequent calls, from sqlite3_reset(), nVar
** is passed -1 and nMem, nCursor and isExplain are all passed zero.
*/
void sqlite3VdbeMakeReady(
  Vdbe *p,                       /* The VDBE */
  int nVar,                      /* Number of '?' see in the SQL statement */
  int nMem,                      /* Number of memory cells to allocate */
  int nCursor,                   /* Number of cursors to allocate */
  int nArg,                      /* Maximum number of args in SubPrograms */
  int isExplain,                 /* True if the EXPLAIN keywords is present */
  int usesStmtJournal            /* True to set Vdbe.usesStmtJournal */
){
  int n;
  sqlite3 *db = p->db;

  assert( p!=0 );
  assert( p->magic==VDBE_MAGIC_INIT );

  /* There should be at least one opcode.
  */
  assert( p->nOp>0 );

  /* Set the magic to VDBE_MAGIC_RUN sooner rather than later. */
  p->magic = VDBE_MAGIC_RUN;

  /* For each cursor required, also allocate a memory cell. Memory
  ** cells (nMem+1-nCursor)..nMem, inclusive, will never be used by
  ** the vdbe program. Instead they are used to allocate space for
  ** VdbeCursor/BtCursor structures. The blob of memory associated with 
  ** cursor 0 is stored in memory cell nMem. Memory cell (nMem-1)
  ** stores the blob of memory associated with cursor 1, etc.
  **
  ** See also: allocateCursor().
  */
  nMem += nCursor;

  /* Allocate space for memory registers, SQL variables, VDBE cursors and 
  ** an array to marshal SQL function arguments in. This is only done the
  ** first time this function is called for a given VDBE, not when it is
  ** being called from sqlite3_reset() to reset the virtual machine.
  */
  if( nVar>=0 && ALWAYS(db->mallocFailed==0) ){
    u8 *zCsr = (u8 *)&p->aOp[p->nOp];
    u8 *zEnd = (u8 *)&p->aOp[p->nOpAlloc];
    int nByte;
    resolveP2Values(p, &nArg);
    p->usesStmtJournal = (u8)usesStmtJournal;
    if( isExplain && nMem<10 ){
      nMem = 10;
    }
    memset(zCsr, 0, zEnd-zCsr);
    zCsr += (zCsr - (u8*)0)&7;
    assert( EIGHT_BYTE_ALIGNMENT(zCsr) );

    do {
      nByte = 0;
      allocSpace((char*)&p->aMem, nMem*sizeof(Mem), &zCsr, zEnd, &nByte);
      allocSpace((char*)&p->aVar, nVar*sizeof(Mem), &zCsr, zEnd, &nByte);
      allocSpace((char*)&p->apArg, nArg*sizeof(Mem*), &zCsr, zEnd, &nByte);
      allocSpace((char*)&p->azVar, nVar*sizeof(char*), &zCsr, zEnd, &nByte);
      allocSpace((char*)&p->apCsr, 
                 nCursor*sizeof(VdbeCursor*), &zCsr, zEnd, &nByte
      );
      if( nByte ){
        p->pFree = sqlite3DbMallocZero(db, nByte);
      }
      zCsr = p->pFree;
      zEnd = &zCsr[nByte];
    }while( nByte && !db->mallocFailed );

    p->nCursor = (u16)nCursor;
    if( p->aVar ){
      p->nVar = (u16)nVar;
      for(n=0; n<nVar; n++){
        p->aVar[n].flags = MEM_Null;
        p->aVar[n].db = db;
      }
    }
    if( p->aMem ){
      p->aMem--;                      /* aMem[] goes from 1..nMem */
      p->nMem = nMem;                 /*       not from 0..nMem-1 */
      for(n=1; n<=nMem; n++){
        p->aMem[n].flags = MEM_Null;
        p->aMem[n].db = db;
      }
    }
  }
#ifdef SQLITE_DEBUG
  for(n=1; n<p->nMem; n++){
    assert( p->aMem[n].db==db );
  }
#endif

  p->pc = -1;
  p->rc = SQLITE_OK;
  p->errorAction = OE_Abort;
  p->explain |= isExplain;
  p->magic = VDBE_MAGIC_RUN;
  p->nChange = 0;
  p->cacheCtr = 1;
  p->minWriteFileFormat = 255;
  p->iStatement = 0;
#ifdef VDBE_PROFILE
  {
    int i;
    for(i=0; i<p->nOp; i++){
      p->aOp[i].cnt = 0;
      p->aOp[i].cycles = 0;
    }
  }
#endif
}

/*
** Close a VDBE cursor and release all the resources that cursor 
** happens to hold.
*/
void sqlite3VdbeFreeCursor(Vdbe *p, VdbeCursor *pCx){
  if( pCx==0 ){
    return;
  }
  if( pCx->pBt ){
    sqlite3BtreeClose(pCx->pBt);
    /* The pCx->pCursor will be close automatically, if it exists, by
    ** the call above. */
  }else if( pCx->pCursor ){
    sqlite3BtreeCloseCursor(pCx->pCursor);
  }
#ifndef SQLITE_OMIT_VIRTUALTABLE
  if( pCx->pVtabCursor ){
    sqlite3_vtab_cursor *pVtabCursor = pCx->pVtabCursor;
    const sqlite3_module *pModule = pCx->pModule;
    p->inVtabMethod = 1;
    (void)sqlite3SafetyOff(p->db);
    pModule->xClose(pVtabCursor);
    (void)sqlite3SafetyOn(p->db);
    p->inVtabMethod = 0;
  }
#endif
}

/*
** Copy the values stored in the VdbeFrame structure to its Vdbe. This
** is used, for example, when a trigger sub-program is halted to restore
** control to the main program.
*/
int sqlite3VdbeFrameRestore(VdbeFrame *pFrame){
  Vdbe *v = pFrame->v;
  v->aOp = pFrame->aOp;
  v->nOp = pFrame->nOp;
  v->aMem = pFrame->aMem;
  v->nMem = pFrame->nMem;
  v->apCsr = pFrame->apCsr;
  v->nCursor = pFrame->nCursor;
  v->db->lastRowid = pFrame->lastRowid;
  v->nChange = pFrame->nChange;
  return pFrame->pc;
}

/*
** Close all cursors.
**
** Also release any dynamic memory held by the VM in the Vdbe.aMem memory 
** cell array. This is necessary as the memory cell array may contain
** pointers to VdbeFrame objects, which may in turn contain pointers to
** open cursors.
*/
static void closeAllCursors(Vdbe *p){
  if( p->pFrame ){
    VdbeFrame *pFrame = p->pFrame;
    for(pFrame=p->pFrame; pFrame->pParent; pFrame=pFrame->pParent);
    sqlite3VdbeFrameRestore(pFrame);
  }
  p->pFrame = 0;
  p->nFrame = 0;

  if( p->apCsr ){
    int i;
    for(i=0; i<p->nCursor; i++){
      VdbeCursor *pC = p->apCsr[i];
      if( pC ){
        sqlite3VdbeFreeCursor(p, pC);
        p->apCsr[i] = 0;
      }
    }
  }
  if( p->aMem ){
    releaseMemArray(&p->aMem[1], p->nMem);
  }
}

/*
** Clean up the VM after execution.
**
** This routine will automatically close any cursors, lists, and/or
** sorters that were left open.  It also deletes the values of
** variables in the aVar[] array.
*/
static void Cleanup(Vdbe *p){
  sqlite3 *db = p->db;

#ifdef SQLITE_DEBUG
  /* Execute assert() statements to ensure that the Vdbe.apCsr[] and 
  ** Vdbe.aMem[] arrays have already been cleaned up.  */
  int i;
  for(i=0; i<p->nCursor; i++) assert( p->apCsr==0 || p->apCsr[i]==0 );
  for(i=1; i<=p->nMem; i++) assert( p->aMem==0 || p->aMem[i].flags==MEM_Null );
#endif

  sqlite3DbFree(db, p->zErrMsg);
  p->zErrMsg = 0;
  p->pResultSet = 0;
}

/*
** Set the number of result columns that will be returned by this SQL
** statement. This is now set at compile time, rather than during
** execution of the vdbe program so that sqlite3_column_count() can
** be called on an SQL statement before sqlite3_step().
*/
void sqlite3VdbeSetNumCols(Vdbe *p, int nResColumn){
  Mem *pColName;
  int n;
  sqlite3 *db = p->db;

  releaseMemArray(p->aColName, p->nResColumn*COLNAME_N);
  sqlite3DbFree(db, p->aColName);
  n = nResColumn*COLNAME_N;
  p->nResColumn = (u16)nResColumn;
  p->aColName = pColName = (Mem*)sqlite3DbMallocZero(db, sizeof(Mem)*n );
  if( p->aColName==0 ) return;
  while( n-- > 0 ){
    pColName->flags = MEM_Null;
    pColName->db = p->db;
    pColName++;
  }
}

/*
** Set the name of the idx'th column to be returned by the SQL statement.
** zName must be a pointer to a nul terminated string.
**
** This call must be made after a call to sqlite3VdbeSetNumCols().
**
** The final parameter, xDel, must be one of SQLITE_DYNAMIC, SQLITE_STATIC
** or SQLITE_TRANSIENT. If it is SQLITE_DYNAMIC, then the buffer pointed
** to by zName will be freed by sqlite3DbFree() when the vdbe is destroyed.
*/
int sqlite3VdbeSetColName(
  Vdbe *p,                         /* Vdbe being configured */
  int idx,                         /* Index of column zName applies to */
  int var,                         /* One of the COLNAME_* constants */
  const char *zName,               /* Pointer to buffer containing name */
  void (*xDel)(void*)              /* Memory management strategy for zName */
){
  int rc;
  Mem *pColName;
  assert( idx<p->nResColumn );
  assert( var<COLNAME_N );
  if( p->db->mallocFailed ){
    assert( !zName || xDel!=SQLITE_DYNAMIC );
    return SQLITE_NOMEM;
  }
  assert( p->aColName!=0 );
  pColName = &(p->aColName[idx+var*p->nResColumn]);
  rc = sqlite3VdbeMemSetStr(pColName, zName, -1, SQLITE_UTF8, xDel);
  assert( rc!=0 || !zName || (pColName->flags&MEM_Term)!=0 );
  return rc;
}

/*
** A read or write transaction may or may not be active on database handle
** db. If a transaction is active, commit it. If there is a
** write-transaction spanning more than one database file, this routine
** takes care of the master journal trickery.
*/
static int vdbeCommit(sqlite3 *db, Vdbe *p){
  int i;
  int nTrans = 0;  /* Number of databases with an active write-transaction */
  int rc = SQLITE_OK;
  int needXcommit = 0;

#ifdef SQLITE_OMIT_VIRTUALTABLE
  /* With this option, sqlite3VtabSync() is defined to be simply 
  ** SQLITE_OK so p is not used. 
  */
  UNUSED_PARAMETER(p);
#endif

  /* Before doing anything else, call the xSync() callback for any
  ** virtual module tables written in this transaction. This has to
  ** be done before determining whether a master journal file is 
  ** required, as an xSync() callback may add an attached database
  ** to the transaction.
  */
  rc = sqlite3VtabSync(db, &p->zErrMsg);
  if( rc!=SQLITE_OK ){
    return rc;
  }

  /* This loop determines (a) if the commit hook should be invoked and
  ** (b) how many database files have open write transactions, not 
  ** including the temp database. (b) is important because if more than 
  ** one database file has an open write transaction, a master journal
  ** file is required for an atomic commit.
  */ 
  for(i=0; i<db->nDb; i++){ 
    Btree *pBt = db->aDb[i].pBt;
    if( sqlite3BtreeIsInTrans(pBt) ){
      needXcommit = 1;
      if( i!=1 ) nTrans++;
    }
  }

  /* If there are any write-transactions at all, invoke the commit hook */
  if( needXcommit && db->xCommitCallback ){
    (void)sqlite3SafetyOff(db);
    rc = db->xCommitCallback(db->pCommitArg);
    (void)sqlite3SafetyOn(db);
    if( rc ){
      return SQLITE_CONSTRAINT;
    }
  }

  /* The simple case - no more than one database file (not counting the
  ** TEMP database) has a transaction active.   There is no need for the
  ** master-journal.
  **
  ** If the return value of sqlite3BtreeGetFilename() is a zero length
  ** string, it means the main database is :memory: or a temp file.  In 
  ** that case we do not support atomic multi-file commits, so use the 
  ** simple case then too.
  */
  if( 0==sqlite3Strlen30(sqlite3BtreeGetFilename(db->aDb[0].pBt))
   || nTrans<=1
  ){
    for(i=0; rc==SQLITE_OK && i<db->nDb; i++){
      Btree *pBt = db->aDb[i].pBt;
      if( pBt ){
        rc = sqlite3BtreeCommitPhaseOne(pBt, 0);
      }
    }

    /* Do the commit only if all databases successfully complete phase 1. 
    ** If one of the BtreeCommitPhaseOne() calls fails, this indicates an
    ** IO error while deleting or truncating a journal file. It is unlikely,
    ** but could happen. In this case abandon processing and return the error.
    */
    for(i=0; rc==SQLITE_OK && i<db->nDb; i++){
      Btree *pBt = db->aDb[i].pBt;
      if( pBt ){
        rc = sqlite3BtreeCommitPhaseTwo(pBt);
      }
    }
    if( rc==SQLITE_OK ){
      sqlite3VtabCommit(db);
    }
  }

  /* The complex case - There is a multi-file write-transaction active.
  ** This requires a master journal file to ensure the transaction is
  ** committed atomicly.
  */
#ifndef SQLITE_OMIT_DISKIO
  else{
    sqlite3_vfs *pVfs = db->pVfs;
    int needSync = 0;
    char *zMaster = 0;   /* File-name for the master journal */
    char const *zMainFile = sqlite3BtreeGetFilename(db->aDb[0].pBt);
    sqlite3_file *pMaster = 0;
    i64 offset = 0;
    int res;

    /* Select a master journal file name */
    do {
      u32 iRandom;
      sqlite3DbFree(db, zMaster);
      sqlite3_randomness(sizeof(iRandom), &iRandom);
      zMaster = sqlite3MPrintf(db, "%s-mj%08X", zMainFile, iRandom&0x7fffffff);
      if( !zMaster ){
        return SQLITE_NOMEM;
      }
      rc = sqlite3OsAccess(pVfs, zMaster, SQLITE_ACCESS_EXISTS, &res);
    }while( rc==SQLITE_OK && res );
    if( rc==SQLITE_OK ){
      /* Open the master journal. */
      rc = sqlite3OsOpenMalloc(pVfs, zMaster, &pMaster, 
          SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE|
          SQLITE_OPEN_EXCLUSIVE|SQLITE_OPEN_MASTER_JOURNAL, 0
      );
    }
    if( rc!=SQLITE_OK ){
      sqlite3DbFree(db, zMaster);
      return rc;
    }
 
    /* Write the name of each database file in the transaction into the new
    ** master journal file. If an error occurs at this point close
    ** and delete the master journal file. All the individual journal files
    ** still have 'null' as the master journal pointer, so they will roll
    ** back independently if a failure occurs.
    */
    for(i=0; i<db->nDb; i++){
      Btree *pBt = db->aDb[i].pBt;
      if( i==1 ) continue;   /* Ignore the TEMP database */
      if( sqlite3BtreeIsInTrans(pBt) ){
        char const *zFile = sqlite3BtreeGetJournalname(pBt);
        if( zFile[0]==0 ) continue;  /* Ignore :memory: databases */
        if( !needSync && !sqlite3BtreeSyncDisabled(pBt) ){
          needSync = 1;
        }
        rc = sqlite3OsWrite(pMaster, zFile, sqlite3Strlen30(zFile)+1, offset);
        offset += sqlite3Strlen30(zFile)+1;
        if( rc!=SQLITE_OK ){
          sqlite3OsCloseFree(pMaster);
          sqlite3OsDelete(pVfs, zMaster, 0);
          sqlite3DbFree(db, zMaster);
          return rc;
        }
      }
    }

    /* Sync the master journal file. If the IOCAP_SEQUENTIAL device
    ** flag is set this is not required.
    */
    if( needSync 
     && 0==(sqlite3OsDeviceCharacteristics(pMaster)&SQLITE_IOCAP_SEQUENTIAL)
     && SQLITE_OK!=(rc = sqlite3OsSync(pMaster, SQLITE_SYNC_NORMAL))
    ){
      sqlite3OsCloseFree(pMaster);
      sqlite3OsDelete(pVfs, zMaster, 0);
      sqlite3DbFree(db, zMaster);
      return rc;
    }

    /* Sync all the db files involved in the transaction. The same call
    ** sets the master journal pointer in each individual journal. If
    ** an error occurs here, do not delete the master journal file.
    **
    ** If the error occurs during the first call to
    ** sqlite3BtreeCommitPhaseOne(), then there is a chance that the
    ** master journal file will be orphaned. But we cannot delete it,
    ** in case the master journal file name was written into the journal
    ** file before the failure occurred.
    */
    for(i=0; rc==SQLITE_OK && i<db->nDb; i++){ 
      Btree *pBt = db->aDb[i].pBt;
      if( pBt ){
        rc = sqlite3BtreeCommitPhaseOne(pBt, zMaster);
      }
    }
    sqlite3OsCloseFree(pMaster);
    if( rc!=SQLITE_OK ){
      sqlite3DbFree(db, zMaster);
      return rc;
    }

    /* Delete the master journal file. This commits the transaction. After
    ** doing this the directory is synced again before any individual
    ** transaction files are deleted.
    */
    rc = sqlite3OsDelete(pVfs, zMaster, 1);
    sqlite3DbFree(db, zMaster);
    zMaster = 0;
    if( rc ){
      return rc;
    }

    /* All files and directories have already been synced, so the following
    ** calls to sqlite3BtreeCommitPhaseTwo() are only closing files and
    ** deleting or truncating journals. If something goes wrong while
    ** this is happening we don't really care. The integrity of the
    ** transaction is already guaranteed, but some stray 'cold' journals
    ** may be lying around. Returning an error code won't help matters.
    */
    disable_simulated_io_errors();
    sqlite3BeginBenignMalloc();
    for(i=0; i<db->nDb; i++){ 
      Btree *pBt = db->aDb[i].pBt;
      if( pBt ){
        sqlite3BtreeCommitPhaseTwo(pBt);
      }
    }
    sqlite3EndBenignMalloc();
    enable_simulated_io_errors();

    sqlite3VtabCommit(db);
  }
#endif

  return rc;
}

/* 
** This routine checks that the sqlite3.activeVdbeCnt count variable
** matches the number of vdbe's in the list sqlite3.pVdbe that are
** currently active. An assertion fails if the two counts do not match.
** This is an internal self-check only - it is not an essential processing
** step.
**
** This is a no-op if NDEBUG is defined.
*/
#ifndef NDEBUG
static void checkActiveVdbeCnt(sqlite3 *db){
  Vdbe *p;
  int cnt = 0;
  int nWrite = 0;
  p = db->pVdbe;
  while( p ){
    if( p->magic==VDBE_MAGIC_RUN && p->pc>=0 ){
      cnt++;
      if( p->readOnly==0 ) nWrite++;
    }
    p = p->pNext;
  }
  assert( cnt==db->activeVdbeCnt );
  assert( nWrite==db->writeVdbeCnt );
}
#else
#define checkActiveVdbeCnt(x)
#endif

/*
** For every Btree that in database connection db which 
** has been modified, "trip" or invalidate each cursor in
** that Btree might have been modified so that the cursor
** can never be used again.  This happens when a rollback
*** occurs.  We have to trip all the other cursors, even
** cursor from other VMs in different database connections,
** so that none of them try to use the data at which they
** were pointing and which now may have been changed due
** to the rollback.
**
** Remember that a rollback can delete tables complete and
** reorder rootpages.  So it is not sufficient just to save
** the state of the cursor.  We have to invalidate the cursor
** so that it is never used again.
*/
static void invalidateCursorsOnModifiedBtrees(sqlite3 *db){
  int i;
  for(i=0; i<db->nDb; i++){
    Btree *p = db->aDb[i].pBt;
    if( p && sqlite3BtreeIsInTrans(p) ){
      sqlite3BtreeTripAllCursors(p, SQLITE_ABORT);
    }
  }
}

/*
** If the Vdbe passed as the first argument opened a statement-transaction,
** close it now. Argument eOp must be either SAVEPOINT_ROLLBACK or
** SAVEPOINT_RELEASE. If it is SAVEPOINT_ROLLBACK, then the statement
** transaction is rolled back. If eOp is SAVEPOINT_RELEASE, then the 
** statement transaction is commtted.
**
** If an IO error occurs, an SQLITE_IOERR_XXX error code is returned. 
** Otherwise SQLITE_OK.
*/
int sqlite3VdbeCloseStatement(Vdbe *p, int eOp){
  sqlite3 *const db = p->db;
  int rc = SQLITE_OK;

  /* If p->iStatement is greater than zero, then this Vdbe opened a 
  ** statement transaction that should be closed here. The only exception
  ** is that an IO error may have occured, causing an emergency rollback.
  ** In this case (db->nStatement==0), and there is nothing to do.
  */
  if( db->nStatement && p->iStatement ){
    int i;
    const int iSavepoint = p->iStatement-1;

    assert( eOp==SAVEPOINT_ROLLBACK || eOp==SAVEPOINT_RELEASE);
    assert( db->nStatement>0 );
    assert( p->iStatement==(db->nStatement+db->nSavepoint) );

    for(i=0; i<db->nDb; i++){ 
      int rc2 = SQLITE_OK;
      Btree *pBt = db->aDb[i].pBt;
      if( pBt ){
        if( eOp==SAVEPOINT_ROLLBACK ){
          rc2 = sqlite3BtreeSavepoint(pBt, SAVEPOINT_ROLLBACK, iSavepoint);
        }
        if( rc2==SQLITE_OK ){
          rc2 = sqlite3BtreeSavepoint(pBt, SAVEPOINT_RELEASE, iSavepoint);
        }
        if( rc==SQLITE_OK ){
          rc = rc2;
        }
      }
    }
    db->nStatement--;
    p->iStatement = 0;
  }
  return rc;
}

/*
** If SQLite is compiled to support shared-cache mode and to be threadsafe,
** this routine obtains the mutex associated with each BtShared structure
** that may be accessed by the VM passed as an argument. In doing so it
** sets the BtShared.db member of each of the BtShared structures, ensuring
** that the correct busy-handler callback is invoked if required.
**
** If SQLite is not threadsafe but does support shared-cache mode, then
** sqlite3BtreeEnterAll() is invoked to set the BtShared.db variables
** of all of BtShared structures accessible via the database handle 
** associated with the VM. Of course only a subset of these structures
** will be accessed by the VM, and we could use Vdbe.btreeMask to figure
** that subset out, but there is no advantage to doing so.
**
** If SQLite is not threadsafe and does not support shared-cache mode, this
** function is a no-op.
*/
#ifndef SQLITE_OMIT_SHARED_CACHE
void sqlite3VdbeMutexArrayEnter(Vdbe *p){
#if SQLITE_THREADSAFE
  sqlite3BtreeMutexArrayEnter(&p->aMutex);
#else
  sqlite3BtreeEnterAll(p->db);
#endif
}
#endif

/*
** This routine is called the when a VDBE tries to halt.  If the VDBE
** has made changes and is in autocommit mode, then commit those
** changes.  If a rollback is needed, then do the rollback.
**
** This routine is the only way to move the state of a VM from
** SQLITE_MAGIC_RUN to SQLITE_MAGIC_HALT.  It is harmless to
** call this on a VM that is in the SQLITE_MAGIC_HALT state.
**
** Return an error code.  If the commit could not complete because of
** lock contention, return SQLITE_BUSY.  If SQLITE_BUSY is returned, it
** means the close did not happen and needs to be repeated.
*/
int sqlite3VdbeHalt(Vdbe *p){
  int rc;                         /* Used to store transient return codes */
  sqlite3 *db = p->db;

  /* This function contains the logic that determines if a statement or
  ** transaction will be committed or rolled back as a result of the
  ** execution of this virtual machine. 
  **
  ** If any of the following errors occur:
  **
  **     SQLITE_NOMEM
  **     SQLITE_IOERR
  **     SQLITE_FULL
  **     SQLITE_INTERRUPT
  **
  ** Then the internal cache might have been left in an inconsistent
  ** state.  We need to rollback the statement transaction, if there is
  ** one, or the complete transaction if there is no statement transaction.
  */

  if( p->db->mallocFailed ){
    p->rc = SQLITE_NOMEM;
  }
  closeAllCursors(p);
  if( p->magic!=VDBE_MAGIC_RUN ){
    return SQLITE_OK;
  }
  checkActiveVdbeCnt(db);

  /* No commit or rollback needed if the program never started */
  if( p->pc>=0 ){
    int mrc;   /* Primary error code from p->rc */
    int eStatementOp = 0;
    int isSpecialError;            /* Set to true if a 'special' error */

    /* Lock all btrees used by the statement */
    sqlite3VdbeMutexArrayEnter(p);

    /* Check for one of the special errors */
    mrc = p->rc & 0xff;
    assert( p->rc!=SQLITE_IOERR_BLOCKED );  /* This error no longer exists */
    isSpecialError = mrc==SQLITE_NOMEM || mrc==SQLITE_IOERR
                     || mrc==SQLITE_INTERRUPT || mrc==SQLITE_FULL;
    if( isSpecialError ){
      /* If the query was read-only, we need do no rollback at all. Otherwise,
      ** proceed with the special handling.
      */
      if( !p->readOnly || mrc!=SQLITE_INTERRUPT ){
        if( (mrc==SQLITE_NOMEM || mrc==SQLITE_FULL) && p->usesStmtJournal ){
          eStatementOp = SAVEPOINT_ROLLBACK;
        }else{
          /* We are forced to roll back the active transaction. Before doing
          ** so, abort any other statements this handle currently has active.
          */
          invalidateCursorsOnModifiedBtrees(db);
          sqlite3RollbackAll(db);
          sqlite3CloseSavepoints(db);
          db->autoCommit = 1;
        }
      }
    }
  
    /* If the auto-commit flag is set and this is the only active writer 
    ** VM, then we do either a commit or rollback of the current transaction. 
    **
    ** Note: This block also runs if one of the special errors handled 
    ** above has occurred. 
    */
    if( !sqlite3VtabInSync(db) 
     && db->autoCommit 
     && db->writeVdbeCnt==(p->readOnly==0) 
    ){
      if( p->rc==SQLITE_OK || (p->errorAction==OE_Fail && !isSpecialError) ){
        /* The auto-commit flag is true, and the vdbe program was 
        ** successful or hit an 'OR FAIL' constraint. This means a commit 
        ** is required.
        */
        rc = vdbeCommit(db, p);
        if( rc==SQLITE_BUSY ){
          sqlite3BtreeMutexArrayLeave(&p->aMutex);
          return SQLITE_BUSY;
        }else if( rc!=SQLITE_OK ){
          p->rc = rc;
          sqlite3RollbackAll(db);
        }else{
          sqlite3CommitInternalChanges(db);
        }
      }else{
        sqlite3RollbackAll(db);
      }
      db->nStatement = 0;
    }else if( eStatementOp==0 ){
      if( p->rc==SQLITE_OK || p->errorAction==OE_Fail ){
        eStatementOp = SAVEPOINT_RELEASE;
      }else if( p->errorAction==OE_Abort ){
        eStatementOp = SAVEPOINT_ROLLBACK;
      }else{
        invalidateCursorsOnModifiedBtrees(db);
        sqlite3RollbackAll(db);
        sqlite3CloseSavepoints(db);
        db->autoCommit = 1;
      }
    }
  
    /* If eStatementOp is non-zero, then a statement transaction needs to
    ** be committed or rolled back. Call sqlite3VdbeCloseStatement() to
    ** do so. If this operation returns an error, and the current statement
    ** error code is SQLITE_OK or SQLITE_CONSTRAINT, then set the error
    ** code to the new value.
    */
    if( eStatementOp ){
      rc = sqlite3VdbeCloseStatement(p, eStatementOp);
      if( rc && (p->rc==SQLITE_OK || p->rc==SQLITE_CONSTRAINT) ){
        p->rc = rc;
        sqlite3DbFree(db, p->zErrMsg);
        p->zErrMsg = 0;
      }
    }
  
    /* If this was an INSERT, UPDATE or DELETE and no statement transaction
    ** has been rolled back, update the database connection change-counter. 
    */
    if( p->changeCntOn ){
      if( eStatementOp!=SAVEPOINT_ROLLBACK ){
        sqlite3VdbeSetChanges(db, p->nChange);
      }else{
        sqlite3VdbeSetChanges(db, 0);
      }
      p->nChange = 0;
    }
  
    /* Rollback or commit any schema changes that occurred. */
    if( p->rc!=SQLITE_OK && db->flags&SQLITE_InternChanges ){
      sqlite3ResetInternalSchema(db, 0);
      db->flags = (db->flags | SQLITE_InternChanges);
    }

    /* Release the locks */
    sqlite3BtreeMutexArrayLeave(&p->aMutex);
  }

  /* We have successfully halted and closed the VM.  Record this fact. */
  if( p->pc>=0 ){
    db->activeVdbeCnt--;
    if( !p->readOnly ){
      db->writeVdbeCnt--;
    }
    assert( db->activeVdbeCnt>=db->writeVdbeCnt );
  }
  p->magic = VDBE_MAGIC_HALT;
  checkActiveVdbeCnt(db);
  if( p->db->mallocFailed ){
    p->rc = SQLITE_NOMEM;
  }

  /* If the auto-commit flag is set to true, then any locks that were held
  ** by connection db have now been released. Call sqlite3ConnectionUnlocked() 
  ** to invoke any required unlock-notify callbacks.
  */
  if( db->autoCommit ){
    sqlite3ConnectionUnlocked(db);
  }

  assert( db->activeVdbeCnt>0 || db->autoCommit==0 || db->nStatement==0 );
  return SQLITE_OK;
}


/*
** Each VDBE holds the result of the most recent sqlite3_step() call
** in p->rc.  This routine sets that result back to SQLITE_OK.
*/
void sqlite3VdbeResetStepResult(Vdbe *p){
  p->rc = SQLITE_OK;
}

/*
** Clean up a VDBE after execution but do not delete the VDBE just yet.
** Write any error messages into *pzErrMsg.  Return the result code.
**
** After this routine is run, the VDBE should be ready to be executed
** again.
**
** To look at it another way, this routine resets the state of the
** virtual machine from VDBE_MAGIC_RUN or VDBE_MAGIC_HALT back to
** VDBE_MAGIC_INIT.
*/
int sqlite3VdbeReset(Vdbe *p){
  sqlite3 *db;
  db = p->db;

  /* If the VM did not run to completion or if it encountered an
  ** error, then it might not have been halted properly.  So halt
  ** it now.
  */
  (void)sqlite3SafetyOn(db);
  sqlite3VdbeHalt(p);
  (void)sqlite3SafetyOff(db);

  /* If the VDBE has be run even partially, then transfer the error code
  ** and error message from the VDBE into the main database structure.  But
  ** if the VDBE has just been set to run but has not actually executed any
  ** instructions yet, leave the main database error information unchanged.
  */
  if( p->pc>=0 ){
    if( p->zErrMsg ){
      sqlite3BeginBenignMalloc();
      sqlite3ValueSetStr(db->pErr,-1,p->zErrMsg,SQLITE_UTF8,SQLITE_TRANSIENT);
      sqlite3EndBenignMalloc();
      db->errCode = p->rc;
      sqlite3DbFree(db, p->zErrMsg);
      p->zErrMsg = 0;
    }else if( p->rc ){
      sqlite3Error(db, p->rc, 0);
    }else{
      sqlite3Error(db, SQLITE_OK, 0);
    }
  }else if( p->rc && p->expired ){
    /* The expired flag was set on the VDBE before the first call
    ** to sqlite3_step(). For consistency (since sqlite3_step() was
    ** called), set the database error in this case as well.
    */
    sqlite3Error(db, p->rc, 0);
    sqlite3ValueSetStr(db->pErr, -1, p->zErrMsg, SQLITE_UTF8, SQLITE_TRANSIENT);
    sqlite3DbFree(db, p->zErrMsg);
    p->zErrMsg = 0;
  }

  /* Reclaim all memory used by the VDBE
  */
  Cleanup(p);

  /* Save profiling information from this VDBE run.
  */
#ifdef VDBE_PROFILE
  {
    FILE *out = fopen("vdbe_profile.out", "a");
    if( out ){
      int i;
      fprintf(out, "---- ");
      for(i=0; i<p->nOp; i++){
        fprintf(out, "%02x", p->aOp[i].opcode);
      }
      fprintf(out, "\n");
      for(i=0; i<p->nOp; i++){
        fprintf(out, "%6d %10lld %8lld ",
           p->aOp[i].cnt,
           p->aOp[i].cycles,
           p->aOp[i].cnt>0 ? p->aOp[i].cycles/p->aOp[i].cnt : 0
        );
        sqlite3VdbePrintOp(out, i, &p->aOp[i]);
      }
      fclose(out);
    }
  }
#endif
  p->magic = VDBE_MAGIC_INIT;
  return p->rc & db->errMask;
}
 
/*
** Clean up and delete a VDBE after execution.  Return an integer which is
** the result code.  Write any error message text into *pzErrMsg.
*/
int sqlite3VdbeFinalize(Vdbe *p){
  int rc = SQLITE_OK;
  if( p->magic==VDBE_MAGIC_RUN || p->magic==VDBE_MAGIC_HALT ){
    rc = sqlite3VdbeReset(p);
    assert( (rc & p->db->errMask)==rc );
  }
  sqlite3VdbeDelete(p);
  return rc;
}

/*
** Call the destructor for each auxdata entry in pVdbeFunc for which
** the corresponding bit in mask is clear.  Auxdata entries beyond 31
** are always destroyed.  To destroy all auxdata entries, call this
** routine with mask==0.
*/
void sqlite3VdbeDeleteAuxData(VdbeFunc *pVdbeFunc, int mask){
  int i;
  for(i=0; i<pVdbeFunc->nAux; i++){
    struct AuxData *pAux = &pVdbeFunc->apAux[i];
    if( (i>31 || !(mask&(((u32)1)<<i))) && pAux->pAux ){
      if( pAux->xDelete ){
        pAux->xDelete(pAux->pAux);
      }
      pAux->pAux = 0;
    }
  }
}

/*
** Delete an entire VDBE.
*/
void sqlite3VdbeDelete(Vdbe *p){
  sqlite3 *db;

  if( NEVER(p==0) ) return;
  db = p->db;
  if( p->pPrev ){
    p->pPrev->pNext = p->pNext;
  }else{
    assert( db->pVdbe==p );
    db->pVdbe = p->pNext;
  }
  if( p->pNext ){
    p->pNext->pPrev = p->pPrev;
  }
  releaseMemArray(p->aVar, p->nVar);
  releaseMemArray(p->aColName, p->nResColumn*COLNAME_N);
  vdbeFreeOpArray(db, p->aOp, p->nOp);
  sqlite3DbFree(db, p->aLabel);
  sqlite3DbFree(db, p->aColName);
  sqlite3DbFree(db, p->zSql);
  p->magic = VDBE_MAGIC_DEAD;
  sqlite3DbFree(db, p->pFree);
  sqlite3DbFree(db, p);
}

/*
** Make sure the cursor p is ready to read or write the row to which it
** was last positioned.  Return an error code if an OOM fault or I/O error
** prevents us from positioning the cursor to its correct position.
**
** If a MoveTo operation is pending on the given cursor, then do that
** MoveTo now.  If no move is pending, check to see if the row has been
** deleted out from under the cursor and if it has, mark the row as
** a NULL row.
**
** If the cursor is already pointing to the correct row and that row has
** not been deleted out from under the cursor, then this routine is a no-op.
*/
int sqlite3VdbeCursorMoveto(VdbeCursor *p){
  if( p->deferredMoveto ){
    int res, rc;
#ifdef SQLITE_TEST
    extern int sqlite3_search_count;
#endif
    assert( p->isTable );
    rc = sqlite3BtreeMovetoUnpacked(p->pCursor, 0, p->movetoTarget, 0, &res);
    if( rc ) return rc;
    p->lastRowid = p->movetoTarget;
    p->rowidIsValid = ALWAYS(res==0) ?1:0;
    if( NEVER(res<0) ){
      rc = sqlite3BtreeNext(p->pCursor, &res);
      if( rc ) return rc;
    }
#ifdef SQLITE_TEST
    sqlite3_search_count++;
#endif
    p->deferredMoveto = 0;
    p->cacheStatus = CACHE_STALE;
  }else if( ALWAYS(p->pCursor) ){
    int hasMoved;
    int rc = sqlite3BtreeCursorHasMoved(p->pCursor, &hasMoved);
    if( rc ) return rc;
    if( hasMoved ){
      p->cacheStatus = CACHE_STALE;
      p->nullRow = 1;
    }
  }
  return SQLITE_OK;
}

/*
** The following functions:
**
** sqlite3VdbeSerialType()
** sqlite3VdbeSerialTypeLen()
** sqlite3VdbeSerialLen()
** sqlite3VdbeSerialPut()
** sqlite3VdbeSerialGet()
**
** encapsulate the code that serializes values for storage in SQLite
** data and index records. Each serialized value consists of a
** 'serial-type' and a blob of data. The serial type is an 8-byte unsigned
** integer, stored as a varint.
**
** In an SQLite index record, the serial type is stored directly before
** the blob of data that it corresponds to. In a table record, all serial
** types are stored at the start of the record, and the blobs of data at
** the end. Hence these functions allow the caller to handle the
** serial-type and data blob seperately.
**
** The following table describes the various storage classes for data:
**
**   serial type        bytes of data      type
**   --------------     ---------------    ---------------
**      0                     0            NULL
**      1                     1            signed integer
**      2                     2            signed integer
**      3                     3            signed integer
**      4                     4            signed integer
**      5                     6            signed integer
**      6                     8            signed integer
**      7                     8            IEEE float
**      8                     0            Integer constant 0
**      9                     0            Integer constant 1
**     10,11                               reserved for expansion
**    N>=12 and even       (N-12)/2        BLOB
**    N>=13 and odd        (N-13)/2        text
**
** The 8 and 9 types were added in 3.3.0, file format 4.  Prior versions
** of SQLite will not understand those serial types.
*/

/*
** Return the serial-type for the value stored in pMem.
*/
u32 sqlite3VdbeSerialType(Mem *pMem, int file_format){
  int flags = pMem->flags;
  int n;

  if( flags&MEM_Null ){
    return 0;
  }
  if( flags&MEM_Int ){
    /* Figure out whether to use 1, 2, 4, 6 or 8 bytes. */
#   define MAX_6BYTE ((((i64)0x00008000)<<32)-1)
    i64 i = pMem->u.i;
    u64 u;
    if( file_format>=4 && (i&1)==i ){
      return 8+(u32)i;
    }
    u = i<0 ? -i : i;
    if( u<=127 ) return 1;
    if( u<=32767 ) return 2;
    if( u<=8388607 ) return 3;
    if( u<=2147483647 ) return 4;
    if( u<=MAX_6BYTE ) return 5;
    return 6;
  }
  if( flags&MEM_Real ){
    return 7;
  }
  assert( pMem->db->mallocFailed || flags&(MEM_Str|MEM_Blob) );
  n = pMem->n;
  if( flags & MEM_Zero ){
    n += pMem->u.nZero;
  }
  assert( n>=0 );
  return ((n*2) + 12 + ((flags&MEM_Str)!=0));
}

/*
** Return the length of the data corresponding to the supplied serial-type.
*/
u32 sqlite3VdbeSerialTypeLen(u32 serial_type){
  if( serial_type>=12 ){
    return (serial_type-12)/2;
  }else{
    static const u8 aSize[] = { 0, 1, 2, 3, 4, 6, 8, 8, 0, 0, 0, 0 };
    return aSize[serial_type];
  }
}

/*
** If we are on an architecture with mixed-endian floating 
** points (ex: ARM7) then swap the lower 4 bytes with the 
** upper 4 bytes.  Return the result.
**
** For most architectures, this is a no-op.
**
** (later):  It is reported to me that the mixed-endian problem
** on ARM7 is an issue with GCC, not with the ARM7 chip.  It seems
** that early versions of GCC stored the two words of a 64-bit
** float in the wrong order.  And that error has been propagated
** ever since.  The blame is not necessarily with GCC, though.
** GCC might have just copying the problem from a prior compiler.
** I am also told that newer versions of GCC that follow a different
** ABI get the byte order right.
**
** Developers using SQLite on an ARM7 should compile and run their
** application using -DSQLITE_DEBUG=1 at least once.  With DEBUG
** enabled, some asserts below will ensure that the byte order of
** floating point values is correct.
**
** (2007-08-30)  Frank van Vugt has studied this problem closely
** and has send his findings to the SQLite developers.  Frank
** writes that some Linux kernels offer floating point hardware
** emulation that uses only 32-bit mantissas instead of a full 
** 48-bits as required by the IEEE standard.  (This is the
** CONFIG_FPE_FASTFPE option.)  On such systems, floating point
** byte swapping becomes very complicated.  To avoid problems,
** the necessary byte swapping is carried out using a 64-bit integer
** rather than a 64-bit float.  Frank assures us that the code here
** works for him.  We, the developers, have no way to independently
** verify this, but Frank seems to know what he is talking about
** so we trust him.
*/
#ifdef SQLITE_MIXED_ENDIAN_64BIT_FLOAT
static u64 floatSwap(u64 in){
  union {
    u64 r;
    u32 i[2];
  } u;
  u32 t;

  u.r = in;
  t = u.i[0];
  u.i[0] = u.i[1];
  u.i[1] = t;
  return u.r;
}
# define swapMixedEndianFloat(X)  X = floatSwap(X)
#else
# define swapMixedEndianFloat(X)
#endif

/*
** Write the serialized data blob for the value stored in pMem into 
** buf. It is assumed that the caller has allocated sufficient space.
** Return the number of bytes written.
**
** nBuf is the amount of space left in buf[].  nBuf must always be
** large enough to hold the entire field.  Except, if the field is
** a blob with a zero-filled tail, then buf[] might be just the right
** size to hold everything except for the zero-filled tail.  If buf[]
** is only big enough to hold the non-zero prefix, then only write that
** prefix into buf[].  But if buf[] is large enough to hold both the
** prefix and the tail then write the prefix and set the tail to all
** zeros.
**
** Return the number of bytes actually written into buf[].  The number
** of bytes in the zero-filled tail is included in the return value only
** if those bytes were zeroed in buf[].
*/ 
u32 sqlite3VdbeSerialPut(u8 *buf, int nBuf, Mem *pMem, int file_format){
  u32 serial_type = sqlite3VdbeSerialType(pMem, file_format);
  u32 len;

  /* Integer and Real */
  if( serial_type<=7 && serial_type>0 ){
    u64 v;
    u32 i;
    if( serial_type==7 ){
      assert( sizeof(v)==sizeof(pMem->r) );
      memcpy(&v, &pMem->r, sizeof(v));
      swapMixedEndianFloat(v);
    }else{
      v = pMem->u.i;
    }
    len = i = sqlite3VdbeSerialTypeLen(serial_type);
    assert( len<=(u32)nBuf );
    while( i-- ){
      buf[i] = (u8)(v&0xFF);
      v >>= 8;
    }
    return len;
  }

  /* String or blob */
  if( serial_type>=12 ){
    assert( pMem->n + ((pMem->flags & MEM_Zero)?pMem->u.nZero:0)
             == (int)sqlite3VdbeSerialTypeLen(serial_type) );
    assert( pMem->n<=nBuf );
    len = pMem->n;
    memcpy(buf, pMem->z, len);
    if( pMem->flags & MEM_Zero ){
      len += pMem->u.nZero;
      assert( nBuf>=0 );
      if( len > (u32)nBuf ){
        len = (u32)nBuf;
      }
      memset(&buf[pMem->n], 0, len-pMem->n);
    }
    return len;
  }

  /* NULL or constants 0 or 1 */
  return 0;
}

/*
** Deserialize the data blob pointed to by buf as serial type serial_type
** and store the result in pMem.  Return the number of bytes read.
*/ 
u32 sqlite3VdbeSerialGet(
  const unsigned char *buf,     /* Buffer to deserialize from */
  u32 serial_type,              /* Serial type to deserialize */
  Mem *pMem                     /* Memory cell to write value into */
){
  switch( serial_type ){
    case 10:   /* Reserved for future use */
    case 11:   /* Reserved for future use */
    case 0: {  /* NULL */
      pMem->flags = MEM_Null;
      break;
    }
    case 1: { /* 1-byte signed integer */
      pMem->u.i = (signed char)buf[0];
      pMem->flags = MEM_Int;
      return 1;
    }
    case 2: { /* 2-byte signed integer */
      pMem->u.i = (((signed char)buf[0])<<8) | buf[1];
      pMem->flags = MEM_Int;
      return 2;
    }
    case 3: { /* 3-byte signed integer */
      pMem->u.i = (((signed char)buf[0])<<16) | (buf[1]<<8) | buf[2];
      pMem->flags = MEM_Int;
      return 3;
    }
    case 4: { /* 4-byte signed integer */
      pMem->u.i = (buf[0]<<24) | (buf[1]<<16) | (buf[2]<<8) | buf[3];
      pMem->flags = MEM_Int;
      return 4;
    }
    case 5: { /* 6-byte signed integer */
      u64 x = (((signed char)buf[0])<<8) | buf[1];
      u32 y = (buf[2]<<24) | (buf[3]<<16) | (buf[4]<<8) | buf[5];
      x = (x<<32) | y;
      pMem->u.i = *(i64*)&x;
      pMem->flags = MEM_Int;
      return 6;
    }
    case 6:   /* 8-byte signed integer */
    case 7: { /* IEEE floating point */
      u64 x;
      u32 y;
#if !defined(NDEBUG) && !defined(SQLITE_OMIT_FLOATING_POINT)
      /* Verify that integers and floating point values use the same
      ** byte order.  Or, that if SQLITE_MIXED_ENDIAN_64BIT_FLOAT is
      ** defined that 64-bit floating point values really are mixed
      ** endian.
      */
      static const u64 t1 = ((u64)0x3ff00000)<<32;
      static const double r1 = 1.0;
      u64 t2 = t1;
      swapMixedEndianFloat(t2);
      assert( sizeof(r1)==sizeof(t2) && memcmp(&r1, &t2, sizeof(r1))==0 );
#endif

      x = (buf[0]<<24) | (buf[1]<<16) | (buf[2]<<8) | buf[3];
      y = (buf[4]<<24) | (buf[5]<<16) | (buf[6]<<8) | buf[7];
      x = (x<<32) | y;
      if( serial_type==6 ){
        pMem->u.i = *(i64*)&x;
        pMem->flags = MEM_Int;
      }else{
        assert( sizeof(x)==8 && sizeof(pMem->r)==8 );
        swapMixedEndianFloat(x);
        memcpy(&pMem->r, &x, sizeof(x));
        pMem->flags = sqlite3IsNaN(pMem->r) ? MEM_Null : MEM_Real;
      }
      return 8;
    }
    case 8:    /* Integer 0 */
    case 9: {  /* Integer 1 */
      pMem->u.i = serial_type-8;
      pMem->flags = MEM_Int;
      return 0;
    }
    default: {
      u32 len = (serial_type-12)/2;
      pMem->z = (char *)buf;
      pMem->n = len;
      pMem->xDel = 0;
      if( serial_type&0x01 ){
        pMem->flags = MEM_Str | MEM_Ephem;
      }else{
        pMem->flags = MEM_Blob | MEM_Ephem;
      }
      return len;
    }
  }
  return 0;
}


/*
** Given the nKey-byte encoding of a record in pKey[], parse the
** record into a UnpackedRecord structure.  Return a pointer to
** that structure.
**
** The calling function might provide szSpace bytes of memory
** space at pSpace.  This space can be used to hold the returned
** VDbeParsedRecord structure if it is large enough.  If it is
** not big enough, space is obtained from sqlite3_malloc().
**
** The returned structure should be closed by a call to
** sqlite3VdbeDeleteUnpackedRecord().
*/ 
UnpackedRecord *sqlite3VdbeRecordUnpack(
  KeyInfo *pKeyInfo,     /* Information about the record format */
  int nKey,              /* Size of the binary record */
  const void *pKey,      /* The binary record */
  char *pSpace,          /* Unaligned space available to hold the object */
  int szSpace            /* Size of pSpace[] in bytes */
){
  const unsigned char *aKey = (const unsigned char *)pKey;
  UnpackedRecord *p;  /* The unpacked record that we will return */
  int nByte;          /* Memory space needed to hold p, in bytes */
  int d;
  u32 idx;
  u16 u;              /* Unsigned loop counter */
  u32 szHdr;
  Mem *pMem;
  int nOff;           /* Increase pSpace by this much to 8-byte align it */
  
  /*
  ** We want to shift the pointer pSpace up such that it is 8-byte aligned.
  ** Thus, we need to calculate a value, nOff, between 0 and 7, to shift 
  ** it by.  If pSpace is already 8-byte aligned, nOff should be zero.
  */
  nOff = (8 - (SQLITE_PTR_TO_INT(pSpace) & 7)) & 7;
  pSpace += nOff;
  szSpace -= nOff;
  nByte = ROUND8(sizeof(UnpackedRecord)) + sizeof(Mem)*(pKeyInfo->nField+1);
  if( nByte>szSpace ){
    p = sqlite3DbMallocRaw(pKeyInfo->db, nByte);
    if( p==0 ) return 0;
    p->flags = UNPACKED_NEED_FREE | UNPACKED_NEED_DESTROY;
  }else{
    p = (UnpackedRecord*)pSpace;
    p->flags = UNPACKED_NEED_DESTROY;
  }
  p->pKeyInfo = pKeyInfo;
  p->nField = pKeyInfo->nField + 1;
  p->aMem = pMem = (Mem*)&((char*)p)[ROUND8(sizeof(UnpackedRecord))];
  assert( EIGHT_BYTE_ALIGNMENT(pMem) );
  idx = getVarint32(aKey, szHdr);
  d = szHdr;
  u = 0;
  while( idx<szHdr && u<p->nField && d<=nKey ){
    u32 serial_type;

    idx += getVarint32(&aKey[idx], serial_type);
    pMem->enc = pKeyInfo->enc;
    pMem->db = pKeyInfo->db;
    pMem->flags = 0;
    pMem->zMalloc = 0;
    d += sqlite3VdbeSerialGet(&aKey[d], serial_type, pMem);
    pMem++;
    u++;
  }
  assert( u<=pKeyInfo->nField + 1 );
  p->nField = u;
  return (void*)p;
}

/*
** This routine destroys a UnpackedRecord object.
*/
void sqlite3VdbeDeleteUnpackedRecord(UnpackedRecord *p){
  int i;
  Mem *pMem;

  assert( p!=0 );
  assert( p->flags & UNPACKED_NEED_DESTROY );
  for(i=0, pMem=p->aMem; i<p->nField; i++, pMem++){
    /* The unpacked record is always constructed by the
    ** sqlite3VdbeUnpackRecord() function above, which makes all
    ** strings and blobs static.  And none of the elements are
    ** ever transformed, so there is never anything to delete.
    */
    if( NEVER(pMem->zMalloc) ) sqlite3VdbeMemRelease(pMem);
  }
  if( p->flags & UNPACKED_NEED_FREE ){
    sqlite3DbFree(p->pKeyInfo->db, p);
  }
}

/*
** This function compares the two table rows or index records
** specified by {nKey1, pKey1} and pPKey2.  It returns a negative, zero
** or positive integer if key1 is less than, equal to or 
** greater than key2.  The {nKey1, pKey1} key must be a blob
** created by th OP_MakeRecord opcode of the VDBE.  The pPKey2
** key must be a parsed key such as obtained from
** sqlite3VdbeParseRecord.
**
** Key1 and Key2 do not have to contain the same number of fields.
** The key with fewer fields is usually compares less than the 
** longer key.  However if the UNPACKED_INCRKEY flags in pPKey2 is set
** and the common prefixes are equal, then key1 is less than key2.
** Or if the UNPACKED_MATCH_PREFIX flag is set and the prefixes are
** equal, then the keys are considered to be equal and
** the parts beyond the common prefix are ignored.
**
** If the UNPACKED_IGNORE_ROWID flag is set, then the last byte of
** the header of pKey1 is ignored.  It is assumed that pKey1 is
** an index key, and thus ends with a rowid value.  The last byte
** of the header will therefore be the serial type of the rowid:
** one of 1, 2, 3, 4, 5, 6, 8, or 9 - the integer serial types.
** The serial type of the final rowid will always be a single byte.
** By ignoring this last byte of the header, we force the comparison
** to ignore the rowid at the end of key1.
*/
int sqlite3VdbeRecordCompare(
  int nKey1, const void *pKey1, /* Left key */
  UnpackedRecord *pPKey2        /* Right key */
){
  int d1;            /* Offset into aKey[] of next data element */
  u32 idx1;          /* Offset into aKey[] of next header element */
  u32 szHdr1;        /* Number of bytes in header */
  int i = 0;
  int nField;
  int rc = 0;
  const unsigned char *aKey1 = (const unsigned char *)pKey1;
  KeyInfo *pKeyInfo;
  Mem mem1;

  pKeyInfo = pPKey2->pKeyInfo;
  mem1.enc = pKeyInfo->enc;
  mem1.db = pKeyInfo->db;
  mem1.flags = 0;
  mem1.u.i = 0;  /* not needed, here to silence compiler warning */
  mem1.zMalloc = 0;
  
  idx1 = getVarint32(aKey1, szHdr1);
  d1 = szHdr1;
  if( pPKey2->flags & UNPACKED_IGNORE_ROWID ){
    szHdr1--;
  }
  nField = pKeyInfo->nField;
  while( idx1<szHdr1 && i<pPKey2->nField ){
    u32 serial_type1;

    /* Read the serial types for the next element in each key. */
    idx1 += getVarint32( aKey1+idx1, serial_type1 );
    if( d1>=nKey1 && sqlite3VdbeSerialTypeLen(serial_type1)>0 ) break;

    /* Extract the values to be compared.
    */
    d1 += sqlite3VdbeSerialGet(&aKey1[d1], serial_type1, &mem1);

    /* Do the comparison
    */
    rc = sqlite3MemCompare(&mem1, &pPKey2->aMem[i],
                           i<nField ? pKeyInfo->aColl[i] : 0);
    if( rc!=0 ){
      break;
    }
    i++;
  }

  /* No memory allocation is ever used on mem1. */
  if( NEVER(mem1.zMalloc) ) sqlite3VdbeMemRelease(&mem1);

  /* If the PREFIX_SEARCH flag is set and all fields except the final
  ** rowid field were equal, then clear the PREFIX_SEARCH flag and set 
  ** pPKey2->rowid to the value of the rowid field in (pKey1, nKey1).
  ** This is used by the OP_IsUnique opcode.
  */
  if( (pPKey2->flags & UNPACKED_PREFIX_SEARCH) && i==(pPKey2->nField-1) ){
    assert( idx1==szHdr1 && rc );
    assert( mem1.flags & MEM_Int );
    pPKey2->flags &= ~UNPACKED_PREFIX_SEARCH;
    pPKey2->rowid = mem1.u.i;
  }

  if( rc==0 ){
    /* rc==0 here means that one of the keys ran out of fields and
    ** all the fields up to that point were equal. If the UNPACKED_INCRKEY
    ** flag is set, then break the tie by treating key2 as larger.
    ** If the UPACKED_PREFIX_MATCH flag is set, then keys with common prefixes
    ** are considered to be equal.  Otherwise, the longer key is the 
    ** larger.  As it happens, the pPKey2 will always be the longer
    ** if there is a difference.
    */
    if( pPKey2->flags & UNPACKED_INCRKEY ){
      rc = -1;
    }else if( pPKey2->flags & UNPACKED_PREFIX_MATCH ){
      /* Leave rc==0 */
    }else if( idx1<szHdr1 ){
      rc = 1;
    }
  }else if( pKeyInfo->aSortOrder && i<pKeyInfo->nField
               && pKeyInfo->aSortOrder[i] ){
    rc = -rc;
  }

  return rc;
}
 

/*
** pCur points at an index entry created using the OP_MakeRecord opcode.
** Read the rowid (the last field in the record) and store it in *rowid.
** Return SQLITE_OK if everything works, or an error code otherwise.
**
** pCur might be pointing to text obtained from a corrupt database file.
** So the content cannot be trusted.  Do appropriate checks on the content.
*/
int sqlite3VdbeIdxRowid(sqlite3 *db, BtCursor *pCur, i64 *rowid){
  i64 nCellKey = 0;
  int rc;
  u32 szHdr;        /* Size of the header */
  u32 typeRowid;    /* Serial type of the rowid */
  u32 lenRowid;     /* Size of the rowid */
  Mem m, v;

  UNUSED_PARAMETER(db);

  /* Get the size of the index entry.  Only indices entries of less
  ** than 2GiB are support - anything large must be database corruption.
  ** Any corruption is detected in sqlite3BtreeParseCellPtr(), though, so
  ** this code can safely assume that nCellKey is 32-bits  
  */
  assert( sqlite3BtreeCursorIsValid(pCur) );
  rc = sqlite3BtreeKeySize(pCur, &nCellKey);
  assert( rc==SQLITE_OK );     /* pCur is always valid so KeySize cannot fail */
  assert( (nCellKey & SQLITE_MAX_U32)==(u64)nCellKey );

  /* Read in the complete content of the index entry */
  memset(&m, 0, sizeof(m));
  rc = sqlite3VdbeMemFromBtree(pCur, 0, (int)nCellKey, 1, &m);
  if( rc ){
    return rc;
  }

  /* The index entry must begin with a header size */
  (void)getVarint32((u8*)m.z, szHdr);
  testcase( szHdr==3 );
  testcase( szHdr==m.n );
  if( unlikely(szHdr<3 || (int)szHdr>m.n) ){
    goto idx_rowid_corruption;
  }

  /* The last field of the index should be an integer - the ROWID.
  ** Verify that the last entry really is an integer. */
  (void)getVarint32((u8*)&m.z[szHdr-1], typeRowid);
  testcase( typeRowid==1 );
  testcase( typeRowid==2 );
  testcase( typeRowid==3 );
  testcase( typeRowid==4 );
  testcase( typeRowid==5 );
  testcase( typeRowid==6 );
  testcase( typeRowid==8 );
  testcase( typeRowid==9 );
  if( unlikely(typeRowid<1 || typeRowid>9 || typeRowid==7) ){
    goto idx_rowid_corruption;
  }
  lenRowid = sqlite3VdbeSerialTypeLen(typeRowid);
  testcase( (u32)m.n==szHdr+lenRowid );
  if( unlikely((u32)m.n<szHdr+lenRowid) ){
    goto idx_rowid_corruption;
  }

  /* Fetch the integer off the end of the index record */
  sqlite3VdbeSerialGet((u8*)&m.z[m.n-lenRowid], typeRowid, &v);
  *rowid = v.u.i;
  sqlite3VdbeMemRelease(&m);
  return SQLITE_OK;

  /* Jump here if database corruption is detected after m has been
  ** allocated.  Free the m object and return SQLITE_CORRUPT. */
idx_rowid_corruption:
  testcase( m.zMalloc!=0 );
  sqlite3VdbeMemRelease(&m);
  return SQLITE_CORRUPT_BKPT;
}

/*
** Compare the key of the index entry that cursor pC is pointing to against
** the key string in pUnpacked.  Write into *pRes a number
** that is negative, zero, or positive if pC is less than, equal to,
** or greater than pUnpacked.  Return SQLITE_OK on success.
**
** pUnpacked is either created without a rowid or is truncated so that it
** omits the rowid at the end.  The rowid at the end of the index entry
** is ignored as well.  Hence, this routine only compares the prefixes 
** of the keys prior to the final rowid, not the entire key.
*/
int sqlite3VdbeIdxKeyCompare(
  VdbeCursor *pC,             /* The cursor to compare against */
  UnpackedRecord *pUnpacked,  /* Unpacked version of key to compare against */
  int *res                    /* Write the comparison result here */
){
  i64 nCellKey = 0;
  int rc;
  BtCursor *pCur = pC->pCursor;
  Mem m;

  assert( sqlite3BtreeCursorIsValid(pCur) );
  rc = sqlite3BtreeKeySize(pCur, &nCellKey);
  assert( rc==SQLITE_OK );    /* pCur is always valid so KeySize cannot fail */
  /* nCellKey will always be between 0 and 0xffffffff because of the say
  ** that btreeParseCellPtr() and sqlite3GetVarint32() are implemented */
  if( nCellKey<=0 || nCellKey>0x7fffffff ){
    *res = 0;
    return SQLITE_CORRUPT;
  }
  memset(&m, 0, sizeof(m));
  rc = sqlite3VdbeMemFromBtree(pC->pCursor, 0, (int)nCellKey, 1, &m);
  if( rc ){
    return rc;
  }
  assert( pUnpacked->flags & UNPACKED_IGNORE_ROWID );
  *res = sqlite3VdbeRecordCompare(m.n, m.z, pUnpacked);
  sqlite3VdbeMemRelease(&m);
  return SQLITE_OK;
}

/*
** This routine sets the value to be returned by subsequent calls to
** sqlite3_changes() on the database handle 'db'. 
*/
void sqlite3VdbeSetChanges(sqlite3 *db, int nChange){
  assert( sqlite3_mutex_held(db->mutex) );
  db->nChange = nChange;
  db->nTotalChange += nChange;
}

/*
** Set a flag in the vdbe to update the change counter when it is finalised
** or reset.
*/
void sqlite3VdbeCountChanges(Vdbe *v){
  v->changeCntOn = 1;
}

/*
** Mark every prepared statement associated with a database connection
** as expired.
**
** An expired statement means that recompilation of the statement is
** recommend.  Statements expire when things happen that make their
** programs obsolete.  Removing user-defined functions or collating
** sequences, or changing an authorization function are the types of
** things that make prepared statements obsolete.
*/
void sqlite3ExpirePreparedStatements(sqlite3 *db){
  Vdbe *p;
  for(p = db->pVdbe; p; p=p->pNext){
    p->expired = 1;
  }
}

/*
** Return the database associated with the Vdbe.
*/
sqlite3 *sqlite3VdbeDb(Vdbe *v){
  return v->db;
}
