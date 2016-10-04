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
** The code in this file implements the function that runs the
** bytecode of a prepared statement.
**
** Various scripts scan this source file in order to generate HTML
** documentation, headers files, or other derived files.  The formatting
** of the code in this file is, therefore, important.  See other comments
** in this file for details.  If in doubt, do not deviate from existing
** commenting and indentation practices when changing or adding code.
*/
#include "sqliteInt.h"
#include "vdbeInt.h"

/*
** Invoke this macro on memory cells just prior to changing the
** value of the cell.  This macro verifies that shallow copies are
** not misused.  A shallow copy of a string or blob just copies a
** pointer to the string or blob, not the content.  If the original
** is changed while the copy is still in use, the string or blob might
** be changed out from under the copy.  This macro verifies that nothing
** like that ever happens.
*/
#ifdef SQLITE_DEBUG
# define memAboutToChange(P,M) sqlite3VdbeMemAboutToChange(P,M)
#else
# define memAboutToChange(P,M)
#endif

/*
** The following global variable is incremented every time a cursor
** moves, either by the OP_SeekXX, OP_Next, or OP_Prev opcodes.  The test
** procedures use this information to make sure that indices are
** working correctly.  This variable has no function other than to
** help verify the correct operation of the library.
*/
#ifdef SQLITE_TEST
int sqlite3_search_count = 0;
#endif

/*
** When this global variable is positive, it gets decremented once before
** each instruction in the VDBE.  When it reaches zero, the u1.isInterrupted
** field of the sqlite3 structure is set in order to simulate an interrupt.
**
** This facility is used for testing purposes only.  It does not function
** in an ordinary build.
*/
#ifdef SQLITE_TEST
int sqlite3_interrupt_count = 0;
#endif

/*
** The next global variable is incremented each type the OP_Sort opcode
** is executed.  The test procedures use this information to make sure that
** sorting is occurring or not occurring at appropriate times.   This variable
** has no function other than to help verify the correct operation of the
** library.
*/
#ifdef SQLITE_TEST
int sqlite3_sort_count = 0;
#endif

/*
** The next global variable records the size of the largest MEM_Blob
** or MEM_Str that has been used by a VDBE opcode.  The test procedures
** use this information to make sure that the zero-blob functionality
** is working correctly.   This variable has no function other than to
** help verify the correct operation of the library.
*/
#ifdef SQLITE_TEST
int sqlite3_max_blobsize = 0;
static void updateMaxBlobsize(Mem *p){
  if( (p->flags & (MEM_Str|MEM_Blob))!=0 && p->n>sqlite3_max_blobsize ){
    sqlite3_max_blobsize = p->n;
  }
}
#endif

/*
** This macro evaluates to true if either the update hook or the preupdate
** hook are enabled for database connect DB.
*/
#ifdef SQLITE_ENABLE_PREUPDATE_HOOK
# define HAS_UPDATE_HOOK(DB) ((DB)->xPreUpdateCallback||(DB)->xUpdateCallback)
#else
# define HAS_UPDATE_HOOK(DB) ((DB)->xUpdateCallback)
#endif

/*
** The next global variable is incremented each time the OP_Found opcode
** is executed. This is used to test whether or not the foreign key
** operation implemented using OP_FkIsZero is working. This variable
** has no function other than to help verify the correct operation of the
** library.
*/
#ifdef SQLITE_TEST
int sqlite3_found_count = 0;
#endif

/*
** Test a register to see if it exceeds the current maximum blob size.
** If it does, record the new maximum blob size.
*/
#if defined(SQLITE_TEST) && !defined(SQLITE_OMIT_BUILTIN_TEST)
# define UPDATE_MAX_BLOBSIZE(P)  updateMaxBlobsize(P)
#else
# define UPDATE_MAX_BLOBSIZE(P)
#endif

/*
** Invoke the VDBE coverage callback, if that callback is defined.  This
** feature is used for test suite validation only and does not appear an
** production builds.
**
** M is an integer, 2 or 3, that indices how many different ways the
** branch can go.  It is usually 2.  "I" is the direction the branch
** goes.  0 means falls through.  1 means branch is taken.  2 means the
** second alternative branch is taken.
**
** iSrcLine is the source code line (from the __LINE__ macro) that
** generated the VDBE instruction.  This instrumentation assumes that all
** source code is in a single file (the amalgamation).  Special values 1
** and 2 for the iSrcLine parameter mean that this particular branch is
** always taken or never taken, respectively.
*/
#if !defined(SQLITE_VDBE_COVERAGE)
# define VdbeBranchTaken(I,M)
#else
# define VdbeBranchTaken(I,M) vdbeTakeBranch(pOp->iSrcLine,I,M)
  static void vdbeTakeBranch(int iSrcLine, u8 I, u8 M){
    if( iSrcLine<=2 && ALWAYS(iSrcLine>0) ){
      M = iSrcLine;
      /* Assert the truth of VdbeCoverageAlwaysTaken() and 
      ** VdbeCoverageNeverTaken() */
      assert( (M & I)==I );
    }else{
      if( sqlite3GlobalConfig.xVdbeBranch==0 ) return;  /*NO_TEST*/
      sqlite3GlobalConfig.xVdbeBranch(sqlite3GlobalConfig.pVdbeBranchArg,
                                      iSrcLine,I,M);
    }
  }
#endif

/*
** Convert the given register into a string if it isn't one
** already. Return non-zero if a malloc() fails.
*/
#define Stringify(P, enc) \
   if(((P)->flags&(MEM_Str|MEM_Blob))==0 && sqlite3VdbeMemStringify(P,enc,0)) \
     { goto no_mem; }

/*
** An ephemeral string value (signified by the MEM_Ephem flag) contains
** a pointer to a dynamically allocated string where some other entity
** is responsible for deallocating that string.  Because the register
** does not control the string, it might be deleted without the register
** knowing it.
**
** This routine converts an ephemeral string into a dynamically allocated
** string that the register itself controls.  In other words, it
** converts an MEM_Ephem string into a string with P.z==P.zMalloc.
*/
#define Deephemeralize(P) \
   if( ((P)->flags&MEM_Ephem)!=0 \
       && sqlite3VdbeMemMakeWriteable(P) ){ goto no_mem;}

/* Return true if the cursor was opened using the OP_OpenSorter opcode. */
#define isSorter(x) ((x)->eCurType==CURTYPE_SORTER)

/*
** Allocate VdbeCursor number iCur.  Return a pointer to it.  Return NULL
** if we run out of memory.
*/
static VdbeCursor *allocateCursor(
  Vdbe *p,              /* The virtual machine */
  int iCur,             /* Index of the new VdbeCursor */
  int nField,           /* Number of fields in the table or index */
  int iDb,              /* Database the cursor belongs to, or -1 */
  u8 eCurType           /* Type of the new cursor */
){
  /* Find the memory cell that will be used to store the blob of memory
  ** required for this VdbeCursor structure. It is convenient to use a 
  ** vdbe memory cell to manage the memory allocation required for a
  ** VdbeCursor structure for the following reasons:
  **
  **   * Sometimes cursor numbers are used for a couple of different
  **     purposes in a vdbe program. The different uses might require
  **     different sized allocations. Memory cells provide growable
  **     allocations.
  **
  **   * When using ENABLE_MEMORY_MANAGEMENT, memory cell buffers can
  **     be freed lazily via the sqlite3_release_memory() API. This
  **     minimizes the number of malloc calls made by the system.
  **
  ** The memory cell for cursor 0 is aMem[0]. The rest are allocated from
  ** the top of the register space.  Cursor 1 is at Mem[p->nMem-1].
  ** Cursor 2 is at Mem[p->nMem-2]. And so forth.
  */
  Mem *pMem = iCur>0 ? &p->aMem[p->nMem-iCur] : p->aMem;

  int nByte;
  VdbeCursor *pCx = 0;
  nByte = 
      ROUND8(sizeof(VdbeCursor)) + 2*sizeof(u32)*nField + 
      (eCurType==CURTYPE_BTREE?sqlite3BtreeCursorSize():0);

  assert( iCur>=0 && iCur<p->nCursor );
  if( p->apCsr[iCur] ){ /*OPTIMIZATION-IF-FALSE*/
    sqlite3VdbeFreeCursor(p, p->apCsr[iCur]);
    p->apCsr[iCur] = 0;
  }
  if( SQLITE_OK==sqlite3VdbeMemClearAndResize(pMem, nByte) ){
    p->apCsr[iCur] = pCx = (VdbeCursor*)pMem->z;
    memset(pCx, 0, sizeof(VdbeCursor));
    pCx->eCurType = eCurType;
    pCx->iDb = iDb;
    pCx->nField = nField;
    pCx->aOffset = &pCx->aType[nField];
    if( eCurType==CURTYPE_BTREE ){
      pCx->uc.pCursor = (BtCursor*)
          &pMem->z[ROUND8(sizeof(VdbeCursor))+2*sizeof(u32)*nField];
      sqlite3BtreeCursorZero(pCx->uc.pCursor);
    }
  }
  return pCx;
}

/*
** Try to convert a value into a numeric representation if we can
** do so without loss of information.  In other words, if the string
** looks like a number, convert it into a number.  If it does not
** look like a number, leave it alone.
**
** If the bTryForInt flag is true, then extra effort is made to give
** an integer representation.  Strings that look like floating point
** values but which have no fractional component (example: '48.00')
** will have a MEM_Int representation when bTryForInt is true.
**
** If bTryForInt is false, then if the input string contains a decimal
** point or exponential notation, the result is only MEM_Real, even
** if there is an exact integer representation of the quantity.
*/
static void applyNumericAffinity(Mem *pRec, int bTryForInt){
  double rValue;
  i64 iValue;
  u8 enc = pRec->enc;
  assert( (pRec->flags & (MEM_Str|MEM_Int|MEM_Real))==MEM_Str );
  if( sqlite3AtoF(pRec->z, &rValue, pRec->n, enc)==0 ) return;
  if( 0==sqlite3Atoi64(pRec->z, &iValue, pRec->n, enc) ){
    pRec->u.i = iValue;
    pRec->flags |= MEM_Int;
  }else{
    pRec->u.r = rValue;
    pRec->flags |= MEM_Real;
    if( bTryForInt ) sqlite3VdbeIntegerAffinity(pRec);
  }
}

/*
** Processing is determine by the affinity parameter:
**
** SQLITE_AFF_INTEGER:
** SQLITE_AFF_REAL:
** SQLITE_AFF_NUMERIC:
**    Try to convert pRec to an integer representation or a 
**    floating-point representation if an integer representation
**    is not possible.  Note that the integer representation is
**    always preferred, even if the affinity is REAL, because
**    an integer representation is more space efficient on disk.
**
** SQLITE_AFF_TEXT:
**    Convert pRec to a text representation.
**
** SQLITE_AFF_BLOB:
**    No-op.  pRec is unchanged.
*/
static void applyAffinity(
  Mem *pRec,          /* The value to apply affinity to */
  char affinity,      /* The affinity to be applied */
  u8 enc              /* Use this text encoding */
){
  if( affinity>=SQLITE_AFF_NUMERIC ){
    assert( affinity==SQLITE_AFF_INTEGER || affinity==SQLITE_AFF_REAL
             || affinity==SQLITE_AFF_NUMERIC );
    if( (pRec->flags & MEM_Int)==0 ){ /*OPTIMIZATION-IF-FALSE*/
      if( (pRec->flags & MEM_Real)==0 ){
        if( pRec->flags & MEM_Str ) applyNumericAffinity(pRec,1);
      }else{
        sqlite3VdbeIntegerAffinity(pRec);
      }
    }
  }else if( affinity==SQLITE_AFF_TEXT ){
    /* Only attempt the conversion to TEXT if there is an integer or real
    ** representation (blob and NULL do not get converted) but no string
    ** representation.  It would be harmless to repeat the conversion if 
    ** there is already a string rep, but it is pointless to waste those
    ** CPU cycles. */
    if( 0==(pRec->flags&MEM_Str) ){ /*OPTIMIZATION-IF-FALSE*/
      if( (pRec->flags&(MEM_Real|MEM_Int)) ){
        sqlite3VdbeMemStringify(pRec, enc, 1);
      }
    }
    pRec->flags &= ~(MEM_Real|MEM_Int);
  }
}

/*
** Try to convert the type of a function argument or a result column
** into a numeric representation.  Use either INTEGER or REAL whichever
** is appropriate.  But only do the conversion if it is possible without
** loss of information and return the revised type of the argument.
*/
int sqlite3_value_numeric_type(sqlite3_value *pVal){
  int eType = sqlite3_value_type(pVal);
  if( eType==SQLITE_TEXT ){
    Mem *pMem = (Mem*)pVal;
    applyNumericAffinity(pMem, 0);
    eType = sqlite3_value_type(pVal);
  }
  return eType;
}

/*
** Exported version of applyAffinity(). This one works on sqlite3_value*, 
** not the internal Mem* type.
*/
void sqlite3ValueApplyAffinity(
  sqlite3_value *pVal, 
  u8 affinity, 
  u8 enc
){
  applyAffinity((Mem *)pVal, affinity, enc);
}

/*
** pMem currently only holds a string type (or maybe a BLOB that we can
** interpret as a string if we want to).  Compute its corresponding
** numeric type, if has one.  Set the pMem->u.r and pMem->u.i fields
** accordingly.
*/
static u16 SQLITE_NOINLINE computeNumericType(Mem *pMem){
  assert( (pMem->flags & (MEM_Int|MEM_Real))==0 );
  assert( (pMem->flags & (MEM_Str|MEM_Blob))!=0 );
  if( sqlite3AtoF(pMem->z, &pMem->u.r, pMem->n, pMem->enc)==0 ){
    return 0;
  }
  if( sqlite3Atoi64(pMem->z, &pMem->u.i, pMem->n, pMem->enc)==SQLITE_OK ){
    return MEM_Int;
  }
  return MEM_Real;
}

/*
** Return the numeric type for pMem, either MEM_Int or MEM_Real or both or
** none.  
**
** Unlike applyNumericAffinity(), this routine does not modify pMem->flags.
** But it does set pMem->u.r and pMem->u.i appropriately.
*/
static u16 numericType(Mem *pMem){
  if( pMem->flags & (MEM_Int|MEM_Real) ){
    return pMem->flags & (MEM_Int|MEM_Real);
  }
  if( pMem->flags & (MEM_Str|MEM_Blob) ){
    return computeNumericType(pMem);
  }
  return 0;
}

#ifdef SQLITE_DEBUG
/*
** Write a nice string representation of the contents of cell pMem
** into buffer zBuf, length nBuf.
*/
void sqlite3VdbeMemPrettyPrint(Mem *pMem, char *zBuf){
  char *zCsr = zBuf;
  int f = pMem->flags;

  static const char *const encnames[] = {"(X)", "(8)", "(16LE)", "(16BE)"};

  if( f&MEM_Blob ){
    int i;
    char c;
    if( f & MEM_Dyn ){
      c = 'z';
      assert( (f & (MEM_Static|MEM_Ephem))==0 );
    }else if( f & MEM_Static ){
      c = 't';
      assert( (f & (MEM_Dyn|MEM_Ephem))==0 );
    }else if( f & MEM_Ephem ){
      c = 'e';
      assert( (f & (MEM_Static|MEM_Dyn))==0 );
    }else{
      c = 's';
    }

    sqlite3_snprintf(100, zCsr, "%c", c);
    zCsr += sqlite3Strlen30(zCsr);
    sqlite3_snprintf(100, zCsr, "%d[", pMem->n);
    zCsr += sqlite3Strlen30(zCsr);
    for(i=0; i<16 && i<pMem->n; i++){
      sqlite3_snprintf(100, zCsr, "%02X", ((int)pMem->z[i] & 0xFF));
      zCsr += sqlite3Strlen30(zCsr);
    }
    for(i=0; i<16 && i<pMem->n; i++){
      char z = pMem->z[i];
      if( z<32 || z>126 ) *zCsr++ = '.';
      else *zCsr++ = z;
    }

    sqlite3_snprintf(100, zCsr, "]%s", encnames[pMem->enc]);
    zCsr += sqlite3Strlen30(zCsr);
    if( f & MEM_Zero ){
      sqlite3_snprintf(100, zCsr,"+%dz",pMem->u.nZero);
      zCsr += sqlite3Strlen30(zCsr);
    }
    *zCsr = '\0';
  }else if( f & MEM_Str ){
    int j, k;
    zBuf[0] = ' ';
    if( f & MEM_Dyn ){
      zBuf[1] = 'z';
      assert( (f & (MEM_Static|MEM_Ephem))==0 );
    }else if( f & MEM_Static ){
      zBuf[1] = 't';
      assert( (f & (MEM_Dyn|MEM_Ephem))==0 );
    }else if( f & MEM_Ephem ){
      zBuf[1] = 'e';
      assert( (f & (MEM_Static|MEM_Dyn))==0 );
    }else{
      zBuf[1] = 's';
    }
    k = 2;
    sqlite3_snprintf(100, &zBuf[k], "%d", pMem->n);
    k += sqlite3Strlen30(&zBuf[k]);
    zBuf[k++] = '[';
    for(j=0; j<15 && j<pMem->n; j++){
      u8 c = pMem->z[j];
      if( c>=0x20 && c<0x7f ){
        zBuf[k++] = c;
      }else{
        zBuf[k++] = '.';
      }
    }
    zBuf[k++] = ']';
    sqlite3_snprintf(100,&zBuf[k], encnames[pMem->enc]);
    k += sqlite3Strlen30(&zBuf[k]);
    zBuf[k++] = 0;
  }
}
#endif

#ifdef SQLITE_DEBUG
/*
** Print the value of a register for tracing purposes:
*/
static void memTracePrint(Mem *p){
  if( p->flags & MEM_Undefined ){
    printf(" undefined");
  }else if( p->flags & MEM_Null ){
    printf(" NULL");
  }else if( (p->flags & (MEM_Int|MEM_Str))==(MEM_Int|MEM_Str) ){
    printf(" si:%lld", p->u.i);
  }else if( p->flags & MEM_Int ){
    printf(" i:%lld", p->u.i);
#ifndef SQLITE_OMIT_FLOATING_POINT
  }else if( p->flags & MEM_Real ){
    printf(" r:%g", p->u.r);
#endif
  }else if( p->flags & MEM_RowSet ){
    printf(" (rowset)");
  }else{
    char zBuf[200];
    sqlite3VdbeMemPrettyPrint(p, zBuf);
    printf(" %s", zBuf);
  }
  if( p->flags & MEM_Subtype ) printf(" subtype=0x%02x", p->eSubtype);
}
static void registerTrace(int iReg, Mem *p){
  printf("REG[%d] = ", iReg);
  memTracePrint(p);
  printf("\n");
}
#endif

#ifdef SQLITE_DEBUG
#  define REGISTER_TRACE(R,M) if(db->flags&SQLITE_VdbeTrace)registerTrace(R,M)
#else
#  define REGISTER_TRACE(R,M)
#endif


#ifdef VDBE_PROFILE

/* 
** hwtime.h contains inline assembler code for implementing 
** high-performance timing routines.
*/
#include "hwtime.h"

#endif

#ifndef NDEBUG
/*
** This function is only called from within an assert() expression. It
** checks that the sqlite3.nTransaction variable is correctly set to
** the number of non-transaction savepoints currently in the 
** linked list starting at sqlite3.pSavepoint.
** 
** Usage:
**
**     assert( checkSavepointCount(db) );
*/
static int checkSavepointCount(sqlite3 *db){
  int n = 0;
  Savepoint *p;
  for(p=db->pSavepoint; p; p=p->pNext) n++;
  assert( n==(db->nSavepoint + db->isTransactionSavepoint) );
  return 1;
}
#endif

/*
** Return the register of pOp->p2 after first preparing it to be
** overwritten with an integer value.
*/
static SQLITE_NOINLINE Mem *out2PrereleaseWithClear(Mem *pOut){
  sqlite3VdbeMemSetNull(pOut);
  pOut->flags = MEM_Int;
  return pOut;
}
static Mem *out2Prerelease(Vdbe *p, VdbeOp *pOp){
  Mem *pOut;
  assert( pOp->p2>0 );
  assert( pOp->p2<=(p->nMem+1 - p->nCursor) );
  pOut = &p->aMem[pOp->p2];
  memAboutToChange(p, pOut);
  if( VdbeMemDynamic(pOut) ){ /*OPTIMIZATION-IF-FALSE*/
    return out2PrereleaseWithClear(pOut);
  }else{
    pOut->flags = MEM_Int;
    return pOut;
  }
}


/*
** Execute as much of a VDBE program as we can.
** This is the core of sqlite3_step().  
*/
int sqlite3VdbeExec(
  Vdbe *p                    /* The VDBE */
){
  Op *aOp = p->aOp;          /* Copy of p->aOp */
  Op *pOp = aOp;             /* Current operation */
#if defined(SQLITE_DEBUG) || defined(VDBE_PROFILE)
  Op *pOrigOp;               /* Value of pOp at the top of the loop */
#endif
#ifdef SQLITE_DEBUG
  int nExtraDelete = 0;      /* Verifies FORDELETE and AUXDELETE flags */
#endif
  int rc = SQLITE_OK;        /* Value to return */
  sqlite3 *db = p->db;       /* The database */
  u8 resetSchemaOnFault = 0; /* Reset schema after an error if positive */
  u8 encoding = ENC(db);     /* The database encoding */
  int iCompare = 0;          /* Result of last comparison */
  unsigned nVmStep = 0;      /* Number of virtual machine steps */
#ifndef SQLITE_OMIT_PROGRESS_CALLBACK
  unsigned nProgressLimit = 0;/* Invoke xProgress() when nVmStep reaches this */
#endif
  Mem *aMem = p->aMem;       /* Copy of p->aMem */
  Mem *pIn1 = 0;             /* 1st input operand */
  Mem *pIn2 = 0;             /* 2nd input operand */
  Mem *pIn3 = 0;             /* 3rd input operand */
  Mem *pOut = 0;             /* Output operand */
  int *aPermute = 0;         /* Permutation of columns for OP_Compare */
  i64 lastRowid = db->lastRowid;  /* Saved value of the last insert ROWID */
#ifdef VDBE_PROFILE
  u64 start;                 /* CPU clock count at start of opcode */
#endif
  /*** INSERT STACK UNION HERE ***/

  assert( p->magic==VDBE_MAGIC_RUN );  /* sqlite3_step() verifies this */
  sqlite3VdbeEnter(p);
  if( p->rc==SQLITE_NOMEM ){
    /* This happens if a malloc() inside a call to sqlite3_column_text() or
    ** sqlite3_column_text16() failed.  */
    goto no_mem;
  }
  assert( p->rc==SQLITE_OK || (p->rc&0xff)==SQLITE_BUSY );
  assert( p->bIsReader || p->readOnly!=0 );
  p->rc = SQLITE_OK;
  p->iCurrentTime = 0;
  assert( p->explain==0 );
  p->pResultSet = 0;
  db->busyHandler.nBusy = 0;
  if( db->u1.isInterrupted ) goto abort_due_to_interrupt;
  sqlite3VdbeIOTraceSql(p);
#ifndef SQLITE_OMIT_PROGRESS_CALLBACK
  if( db->xProgress ){
    u32 iPrior = p->aCounter[SQLITE_STMTSTATUS_VM_STEP];
    assert( 0 < db->nProgressOps );
    nProgressLimit = db->nProgressOps - (iPrior % db->nProgressOps);
  }
#endif
#ifdef SQLITE_DEBUG
  sqlite3BeginBenignMalloc();
  if( p->pc==0
   && (p->db->flags & (SQLITE_VdbeListing|SQLITE_VdbeEQP|SQLITE_VdbeTrace))!=0
  ){
    int i;
    int once = 1;
    sqlite3VdbePrintSql(p);
    if( p->db->flags & SQLITE_VdbeListing ){
      printf("VDBE Program Listing:\n");
      for(i=0; i<p->nOp; i++){
        sqlite3VdbePrintOp(stdout, i, &aOp[i]);
      }
    }
    if( p->db->flags & SQLITE_VdbeEQP ){
      for(i=0; i<p->nOp; i++){
        if( aOp[i].opcode==OP_Explain ){
          if( once ) printf("VDBE Query Plan:\n");
          printf("%s\n", aOp[i].p4.z);
          once = 0;
        }
      }
    }
    if( p->db->flags & SQLITE_VdbeTrace )  printf("VDBE Trace:\n");
  }
  sqlite3EndBenignMalloc();
#endif
  for(pOp=&aOp[p->pc]; 1; pOp++){
    /* Errors are detected by individual opcodes, with an immediate
    ** jumps to abort_due_to_error. */
    assert( rc==SQLITE_OK );

    assert( pOp>=aOp && pOp<&aOp[p->nOp]);
#ifdef VDBE_PROFILE
    start = sqlite3Hwtime();
#endif
    nVmStep++;
#ifdef SQLITE_ENABLE_STMT_SCANSTATUS
    if( p->anExec ) p->anExec[(int)(pOp-aOp)]++;
#endif

    /* Only allow tracing if SQLITE_DEBUG is defined.
    */
#ifdef SQLITE_DEBUG
    if( db->flags & SQLITE_VdbeTrace ){
      sqlite3VdbePrintOp(stdout, (int)(pOp - aOp), pOp);
    }
#endif
      

    /* Check to see if we need to simulate an interrupt.  This only happens
    ** if we have a special test build.
    */
#ifdef SQLITE_TEST
    if( sqlite3_interrupt_count>0 ){
      sqlite3_interrupt_count--;
      if( sqlite3_interrupt_count==0 ){
        sqlite3_interrupt(db);
      }
    }
#endif

    /* Sanity checking on other operands */
#ifdef SQLITE_DEBUG
    {
      u8 opProperty = sqlite3OpcodeProperty[pOp->opcode];
      if( (opProperty & OPFLG_IN1)!=0 ){
        assert( pOp->p1>0 );
        assert( pOp->p1<=(p->nMem+1 - p->nCursor) );
        assert( memIsValid(&aMem[pOp->p1]) );
        assert( sqlite3VdbeCheckMemInvariants(&aMem[pOp->p1]) );
        REGISTER_TRACE(pOp->p1, &aMem[pOp->p1]);
      }
      if( (opProperty & OPFLG_IN2)!=0 ){
        assert( pOp->p2>0 );
        assert( pOp->p2<=(p->nMem+1 - p->nCursor) );
        assert( memIsValid(&aMem[pOp->p2]) );
        assert( sqlite3VdbeCheckMemInvariants(&aMem[pOp->p2]) );
        REGISTER_TRACE(pOp->p2, &aMem[pOp->p2]);
      }
      if( (opProperty & OPFLG_IN3)!=0 ){
        assert( pOp->p3>0 );
        assert( pOp->p3<=(p->nMem+1 - p->nCursor) );
        assert( memIsValid(&aMem[pOp->p3]) );
        assert( sqlite3VdbeCheckMemInvariants(&aMem[pOp->p3]) );
        REGISTER_TRACE(pOp->p3, &aMem[pOp->p3]);
      }
      if( (opProperty & OPFLG_OUT2)!=0 ){
        assert( pOp->p2>0 );
        assert( pOp->p2<=(p->nMem+1 - p->nCursor) );
        memAboutToChange(p, &aMem[pOp->p2]);
      }
      if( (opProperty & OPFLG_OUT3)!=0 ){
        assert( pOp->p3>0 );
        assert( pOp->p3<=(p->nMem+1 - p->nCursor) );
        memAboutToChange(p, &aMem[pOp->p3]);
      }
    }
#endif
#if defined(SQLITE_DEBUG) || defined(VDBE_PROFILE)
    pOrigOp = pOp;
#endif
  
    switch( pOp->opcode ){

/*****************************************************************************
** What follows is a massive switch statement where each case implements a
** separate instruction in the virtual machine.  If we follow the usual
** indentation conventions, each case should be indented by 6 spaces.  But
** that is a lot of wasted space on the left margin.  So the code within
** the switch statement will break with convention and be flush-left. Another
** big comment (similar to this one) will mark the point in the code where
** we transition back to normal indentation.
**
** The formatting of each case is important.  The makefile for SQLite
** generates two C files "opcodes.h" and "opcodes.c" by scanning this
** file looking for lines that begin with "case OP_".  The opcodes.h files
** will be filled with #defines that give unique integer values to each
** opcode and the opcodes.c file is filled with an array of strings where
** each string is the symbolic name for the corresponding opcode.  If the
** case statement is followed by a comment of the form "/# same as ... #/"
** that comment is used to determine the particular value of the opcode.
**
** Other keywords in the comment that follows each case are used to
** construct the OPFLG_INITIALIZER value that initializes opcodeProperty[].
** Keywords include: in1, in2, in3, out2, out3.  See
** the mkopcodeh.awk script for additional information.
**
** Documentation about VDBE opcodes is generated by scanning this file
** for lines of that contain "Opcode:".  That line and all subsequent
** comment lines are used in the generation of the opcode.html documentation
** file.
**
** SUMMARY:
**
**     Formatting is important to scripts that scan this file.
**     Do not deviate from the formatting style currently in use.
**
*****************************************************************************/

/* Opcode:  Goto * P2 * * *
**
** An unconditional jump to address P2.
** The next instruction executed will be 
** the one at index P2 from the beginning of
** the program.
**
** The P1 parameter is not actually used by this opcode.  However, it
** is sometimes set to 1 instead of 0 as a hint to the command-line shell
** that this Goto is the bottom of a loop and that the lines from P2 down
** to the current line should be indented for EXPLAIN output.
*/
case OP_Goto: {             /* jump */
jump_to_p2_and_check_for_interrupt:
  pOp = &aOp[pOp->p2 - 1];

  /* Opcodes that are used as the bottom of a loop (OP_Next, OP_Prev,
  ** OP_VNext, OP_RowSetNext, or OP_SorterNext) all jump here upon
  ** completion.  Check to see if sqlite3_interrupt() has been called
  ** or if the progress callback needs to be invoked. 
  **
  ** This code uses unstructured "goto" statements and does not look clean.
  ** But that is not due to sloppy coding habits. The code is written this
  ** way for performance, to avoid having to run the interrupt and progress
  ** checks on every opcode.  This helps sqlite3_step() to run about 1.5%
  ** faster according to "valgrind --tool=cachegrind" */
check_for_interrupt:
  if( db->u1.isInterrupted ) goto abort_due_to_interrupt;
#ifndef SQLITE_OMIT_PROGRESS_CALLBACK
  /* Call the progress callback if it is configured and the required number
  ** of VDBE ops have been executed (either since this invocation of
  ** sqlite3VdbeExec() or since last time the progress callback was called).
  ** If the progress callback returns non-zero, exit the virtual machine with
  ** a return code SQLITE_ABORT.
  */
  if( db->xProgress!=0 && nVmStep>=nProgressLimit ){
    assert( db->nProgressOps!=0 );
    nProgressLimit = nVmStep + db->nProgressOps - (nVmStep%db->nProgressOps);
    if( db->xProgress(db->pProgressArg) ){
      rc = SQLITE_INTERRUPT;
      goto abort_due_to_error;
    }
  }
#endif
  
  break;
}

/* Opcode:  Gosub P1 P2 * * *
**
** Write the current address onto register P1
** and then jump to address P2.
*/
case OP_Gosub: {            /* jump */
  assert( pOp->p1>0 && pOp->p1<=(p->nMem+1 - p->nCursor) );
  pIn1 = &aMem[pOp->p1];
  assert( VdbeMemDynamic(pIn1)==0 );
  memAboutToChange(p, pIn1);
  pIn1->flags = MEM_Int;
  pIn1->u.i = (int)(pOp-aOp);
  REGISTER_TRACE(pOp->p1, pIn1);

  /* Most jump operations do a goto to this spot in order to update
  ** the pOp pointer. */
jump_to_p2:
  pOp = &aOp[pOp->p2 - 1];
  break;
}

/* Opcode:  Return P1 * * * *
**
** Jump to the next instruction after the address in register P1.  After
** the jump, register P1 becomes undefined.
*/
case OP_Return: {           /* in1 */
  pIn1 = &aMem[pOp->p1];
  assert( pIn1->flags==MEM_Int );
  pOp = &aOp[pIn1->u.i];
  pIn1->flags = MEM_Undefined;
  break;
}

/* Opcode: InitCoroutine P1 P2 P3 * *
**
** Set up register P1 so that it will Yield to the coroutine
** located at address P3.
**
** If P2!=0 then the coroutine implementation immediately follows
** this opcode.  So jump over the coroutine implementation to
** address P2.
**
** See also: EndCoroutine
*/
case OP_InitCoroutine: {     /* jump */
  assert( pOp->p1>0 &&  pOp->p1<=(p->nMem+1 - p->nCursor) );
  assert( pOp->p2>=0 && pOp->p2<p->nOp );
  assert( pOp->p3>=0 && pOp->p3<p->nOp );
  pOut = &aMem[pOp->p1];
  assert( !VdbeMemDynamic(pOut) );
  pOut->u.i = pOp->p3 - 1;
  pOut->flags = MEM_Int;
  if( pOp->p2 ) goto jump_to_p2;
  break;
}

/* Opcode:  EndCoroutine P1 * * * *
**
** The instruction at the address in register P1 is a Yield.
** Jump to the P2 parameter of that Yield.
** After the jump, register P1 becomes undefined.
**
** See also: InitCoroutine
*/
case OP_EndCoroutine: {           /* in1 */
  VdbeOp *pCaller;
  pIn1 = &aMem[pOp->p1];
  assert( pIn1->flags==MEM_Int );
  assert( pIn1->u.i>=0 && pIn1->u.i<p->nOp );
  pCaller = &aOp[pIn1->u.i];
  assert( pCaller->opcode==OP_Yield );
  assert( pCaller->p2>=0 && pCaller->p2<p->nOp );
  pOp = &aOp[pCaller->p2 - 1];
  pIn1->flags = MEM_Undefined;
  break;
}

/* Opcode:  Yield P1 P2 * * *
**
** Swap the program counter with the value in register P1.  This
** has the effect of yielding to a coroutine.
**
** If the coroutine that is launched by this instruction ends with
** Yield or Return then continue to the next instruction.  But if
** the coroutine launched by this instruction ends with
** EndCoroutine, then jump to P2 rather than continuing with the
** next instruction.
**
** See also: InitCoroutine
*/
case OP_Yield: {            /* in1, jump */
  int pcDest;
  pIn1 = &aMem[pOp->p1];
  assert( VdbeMemDynamic(pIn1)==0 );
  pIn1->flags = MEM_Int;
  pcDest = (int)pIn1->u.i;
  pIn1->u.i = (int)(pOp - aOp);
  REGISTER_TRACE(pOp->p1, pIn1);
  pOp = &aOp[pcDest];
  break;
}

/* Opcode:  HaltIfNull  P1 P2 P3 P4 P5
** Synopsis: if r[P3]=null halt
**
** Check the value in register P3.  If it is NULL then Halt using
** parameter P1, P2, and P4 as if this were a Halt instruction.  If the
** value in register P3 is not NULL, then this routine is a no-op.
** The P5 parameter should be 1.
*/
case OP_HaltIfNull: {      /* in3 */
  pIn3 = &aMem[pOp->p3];
  if( (pIn3->flags & MEM_Null)==0 ) break;
  /* Fall through into OP_Halt */
}

/* Opcode:  Halt P1 P2 * P4 P5
**
** Exit immediately.  All open cursors, etc are closed
** automatically.
**
** P1 is the result code returned by sqlite3_exec(), sqlite3_reset(),
** or sqlite3_finalize().  For a normal halt, this should be SQLITE_OK (0).
** For errors, it can be some other value.  If P1!=0 then P2 will determine
** whether or not to rollback the current transaction.  Do not rollback
** if P2==OE_Fail. Do the rollback if P2==OE_Rollback.  If P2==OE_Abort,
** then back out all changes that have occurred during this execution of the
** VDBE, but do not rollback the transaction. 
**
** If P4 is not null then it is an error message string.
**
** P5 is a value between 0 and 4, inclusive, that modifies the P4 string.
**
**    0:  (no change)
**    1:  NOT NULL contraint failed: P4
**    2:  UNIQUE constraint failed: P4
**    3:  CHECK constraint failed: P4
**    4:  FOREIGN KEY constraint failed: P4
**
** If P5 is not zero and P4 is NULL, then everything after the ":" is
** omitted.
**
** There is an implied "Halt 0 0 0" instruction inserted at the very end of
** every program.  So a jump past the last instruction of the program
** is the same as executing Halt.
*/
case OP_Halt: {
  VdbeFrame *pFrame;
  int pcx;

  pcx = (int)(pOp - aOp);
  if( pOp->p1==SQLITE_OK && p->pFrame ){
    /* Halt the sub-program. Return control to the parent frame. */
    pFrame = p->pFrame;
    p->pFrame = pFrame->pParent;
    p->nFrame--;
    sqlite3VdbeSetChanges(db, p->nChange);
    pcx = sqlite3VdbeFrameRestore(pFrame);
    lastRowid = db->lastRowid;
    if( pOp->p2==OE_Ignore ){
      /* Instruction pcx is the OP_Program that invoked the sub-program 
      ** currently being halted. If the p2 instruction of this OP_Halt
      ** instruction is set to OE_Ignore, then the sub-program is throwing
      ** an IGNORE exception. In this case jump to the address specified
      ** as the p2 of the calling OP_Program.  */
      pcx = p->aOp[pcx].p2-1;
    }
    aOp = p->aOp;
    aMem = p->aMem;
    pOp = &aOp[pcx];
    break;
  }
  p->rc = pOp->p1;
  p->errorAction = (u8)pOp->p2;
  p->pc = pcx;
  assert( pOp->p5>=0 && pOp->p5<=4 );
  if( p->rc ){
    if( pOp->p5 ){
      static const char * const azType[] = { "NOT NULL", "UNIQUE", "CHECK",
                                             "FOREIGN KEY" };
      testcase( pOp->p5==1 );
      testcase( pOp->p5==2 );
      testcase( pOp->p5==3 );
      testcase( pOp->p5==4 );
      sqlite3VdbeError(p, "%s constraint failed", azType[pOp->p5-1]);
      if( pOp->p4.z ){
        p->zErrMsg = sqlite3MPrintf(db, "%z: %s", p->zErrMsg, pOp->p4.z);
      }
    }else{
      sqlite3VdbeError(p, "%s", pOp->p4.z);
    }
    sqlite3_log(pOp->p1, "abort at %d in [%s]: %s", pcx, p->zSql, p->zErrMsg);
  }
  rc = sqlite3VdbeHalt(p);
  assert( rc==SQLITE_BUSY || rc==SQLITE_OK || rc==SQLITE_ERROR );
  if( rc==SQLITE_BUSY ){
    p->rc = SQLITE_BUSY;
  }else{
    assert( rc==SQLITE_OK || (p->rc&0xff)==SQLITE_CONSTRAINT );
    assert( rc==SQLITE_OK || db->nDeferredCons>0 || db->nDeferredImmCons>0 );
    rc = p->rc ? SQLITE_ERROR : SQLITE_DONE;
  }
  goto vdbe_return;
}

/* Opcode: Integer P1 P2 * * *
** Synopsis: r[P2]=P1
**
** The 32-bit integer value P1 is written into register P2.
*/
case OP_Integer: {         /* out2 */
  pOut = out2Prerelease(p, pOp);
  pOut->u.i = pOp->p1;
  break;
}

/* Opcode: Int64 * P2 * P4 *
** Synopsis: r[P2]=P4
**
** P4 is a pointer to a 64-bit integer value.
** Write that value into register P2.
*/
case OP_Int64: {           /* out2 */
  pOut = out2Prerelease(p, pOp);
  assert( pOp->p4.pI64!=0 );
  pOut->u.i = *pOp->p4.pI64;
  break;
}

#ifndef SQLITE_OMIT_FLOATING_POINT
/* Opcode: Real * P2 * P4 *
** Synopsis: r[P2]=P4
**
** P4 is a pointer to a 64-bit floating point value.
** Write that value into register P2.
*/
case OP_Real: {            /* same as TK_FLOAT, out2 */
  pOut = out2Prerelease(p, pOp);
  pOut->flags = MEM_Real;
  assert( !sqlite3IsNaN(*pOp->p4.pReal) );
  pOut->u.r = *pOp->p4.pReal;
  break;
}
#endif

/* Opcode: String8 * P2 * P4 *
** Synopsis: r[P2]='P4'
**
** P4 points to a nul terminated UTF-8 string. This opcode is transformed 
** into a String opcode before it is executed for the first time.  During
** this transformation, the length of string P4 is computed and stored
** as the P1 parameter.
*/
case OP_String8: {         /* same as TK_STRING, out2 */
  assert( pOp->p4.z!=0 );
  pOut = out2Prerelease(p, pOp);
  pOp->opcode = OP_String;
  pOp->p1 = sqlite3Strlen30(pOp->p4.z);

#ifndef SQLITE_OMIT_UTF16
  if( encoding!=SQLITE_UTF8 ){
    rc = sqlite3VdbeMemSetStr(pOut, pOp->p4.z, -1, SQLITE_UTF8, SQLITE_STATIC);
    assert( rc==SQLITE_OK || rc==SQLITE_TOOBIG );
    if( SQLITE_OK!=sqlite3VdbeChangeEncoding(pOut, encoding) ) goto no_mem;
    assert( pOut->szMalloc>0 && pOut->zMalloc==pOut->z );
    assert( VdbeMemDynamic(pOut)==0 );
    pOut->szMalloc = 0;
    pOut->flags |= MEM_Static;
    if( pOp->p4type==P4_DYNAMIC ){
      sqlite3DbFree(db, pOp->p4.z);
    }
    pOp->p4type = P4_DYNAMIC;
    pOp->p4.z = pOut->z;
    pOp->p1 = pOut->n;
  }
  testcase( rc==SQLITE_TOOBIG );
#endif
  if( pOp->p1>db->aLimit[SQLITE_LIMIT_LENGTH] ){
    goto too_big;
  }
  assert( rc==SQLITE_OK );
  /* Fall through to the next case, OP_String */
}
  
/* Opcode: String P1 P2 P3 P4 P5
** Synopsis: r[P2]='P4' (len=P1)
**
** The string value P4 of length P1 (bytes) is stored in register P2.
**
** If P3 is not zero and the content of register P3 is equal to P5, then
** the datatype of the register P2 is converted to BLOB.  The content is
** the same sequence of bytes, it is merely interpreted as a BLOB instead
** of a string, as if it had been CAST.  In other words:
**
** if( P3!=0 and reg[P3]==P5 ) reg[P2] := CAST(reg[P2] as BLOB)
*/
case OP_String: {          /* out2 */
  assert( pOp->p4.z!=0 );
  pOut = out2Prerelease(p, pOp);
  pOut->flags = MEM_Str|MEM_Static|MEM_Term;
  pOut->z = pOp->p4.z;
  pOut->n = pOp->p1;
  pOut->enc = encoding;
  UPDATE_MAX_BLOBSIZE(pOut);
#ifndef SQLITE_LIKE_DOESNT_MATCH_BLOBS
  if( pOp->p3>0 ){
    assert( pOp->p3<=(p->nMem+1 - p->nCursor) );
    pIn3 = &aMem[pOp->p3];
    assert( pIn3->flags & MEM_Int );
    if( pIn3->u.i==pOp->p5 ) pOut->flags = MEM_Blob|MEM_Static|MEM_Term;
  }
#endif
  break;
}

/* Opcode: Null P1 P2 P3 * *
** Synopsis: r[P2..P3]=NULL
**
** Write a NULL into registers P2.  If P3 greater than P2, then also write
** NULL into register P3 and every register in between P2 and P3.  If P3
** is less than P2 (typically P3 is zero) then only register P2 is
** set to NULL.
**
** If the P1 value is non-zero, then also set the MEM_Cleared flag so that
** NULL values will not compare equal even if SQLITE_NULLEQ is set on
** OP_Ne or OP_Eq.
*/
case OP_Null: {           /* out2 */
  int cnt;
  u16 nullFlag;
  pOut = out2Prerelease(p, pOp);
  cnt = pOp->p3-pOp->p2;
  assert( pOp->p3<=(p->nMem+1 - p->nCursor) );
  pOut->flags = nullFlag = pOp->p1 ? (MEM_Null|MEM_Cleared) : MEM_Null;
  pOut->n = 0;
  while( cnt>0 ){
    pOut++;
    memAboutToChange(p, pOut);
    sqlite3VdbeMemSetNull(pOut);
    pOut->flags = nullFlag;
    pOut->n = 0;
    cnt--;
  }
  break;
}

/* Opcode: SoftNull P1 * * * *
** Synopsis: r[P1]=NULL
**
** Set register P1 to have the value NULL as seen by the OP_MakeRecord
** instruction, but do not free any string or blob memory associated with
** the register, so that if the value was a string or blob that was
** previously copied using OP_SCopy, the copies will continue to be valid.
*/
case OP_SoftNull: {
  assert( pOp->p1>0 && pOp->p1<=(p->nMem+1 - p->nCursor) );
  pOut = &aMem[pOp->p1];
  pOut->flags = (pOut->flags|MEM_Null)&~MEM_Undefined;
  break;
}

/* Opcode: Blob P1 P2 * P4 *
** Synopsis: r[P2]=P4 (len=P1)
**
** P4 points to a blob of data P1 bytes long.  Store this
** blob in register P2.
*/
case OP_Blob: {                /* out2 */
  assert( pOp->p1 <= SQLITE_MAX_LENGTH );
  pOut = out2Prerelease(p, pOp);
  sqlite3VdbeMemSetStr(pOut, pOp->p4.z, pOp->p1, 0, 0);
  pOut->enc = encoding;
  UPDATE_MAX_BLOBSIZE(pOut);
  break;
}

/* Opcode: Variable P1 P2 * P4 *
** Synopsis: r[P2]=parameter(P1,P4)
**
** Transfer the values of bound parameter P1 into register P2
**
** If the parameter is named, then its name appears in P4.
** The P4 value is used by sqlite3_bind_parameter_name().
*/
case OP_Variable: {            /* out2 */
  Mem *pVar;       /* Value being transferred */

  assert( pOp->p1>0 && pOp->p1<=p->nVar );
  assert( pOp->p4.z==0 || pOp->p4.z==p->azVar[pOp->p1-1] );
  pVar = &p->aVar[pOp->p1 - 1];
  if( sqlite3VdbeMemTooBig(pVar) ){
    goto too_big;
  }
  pOut = out2Prerelease(p, pOp);
  sqlite3VdbeMemShallowCopy(pOut, pVar, MEM_Static);
  UPDATE_MAX_BLOBSIZE(pOut);
  break;
}

/* Opcode: Move P1 P2 P3 * *
** Synopsis: r[P2@P3]=r[P1@P3]
**
** Move the P3 values in register P1..P1+P3-1 over into
** registers P2..P2+P3-1.  Registers P1..P1+P3-1 are
** left holding a NULL.  It is an error for register ranges
** P1..P1+P3-1 and P2..P2+P3-1 to overlap.  It is an error
** for P3 to be less than 1.
*/
case OP_Move: {
  int n;           /* Number of registers left to copy */
  int p1;          /* Register to copy from */
  int p2;          /* Register to copy to */

  n = pOp->p3;
  p1 = pOp->p1;
  p2 = pOp->p2;
  assert( n>0 && p1>0 && p2>0 );
  assert( p1+n<=p2 || p2+n<=p1 );

  pIn1 = &aMem[p1];
  pOut = &aMem[p2];
  do{
    assert( pOut<=&aMem[(p->nMem+1 - p->nCursor)] );
    assert( pIn1<=&aMem[(p->nMem+1 - p->nCursor)] );
    assert( memIsValid(pIn1) );
    memAboutToChange(p, pOut);
    sqlite3VdbeMemMove(pOut, pIn1);
#ifdef SQLITE_DEBUG
    if( pOut->pScopyFrom>=&aMem[p1] && pOut->pScopyFrom<pOut ){
      pOut->pScopyFrom += pOp->p2 - p1;
    }
#endif
    Deephemeralize(pOut);
    REGISTER_TRACE(p2++, pOut);
    pIn1++;
    pOut++;
  }while( --n );
  break;
}

/* Opcode: Copy P1 P2 P3 * *
** Synopsis: r[P2@P3+1]=r[P1@P3+1]
**
** Make a copy of registers P1..P1+P3 into registers P2..P2+P3.
**
** This instruction makes a deep copy of the value.  A duplicate
** is made of any string or blob constant.  See also OP_SCopy.
*/
case OP_Copy: {
  int n;

  n = pOp->p3;
  pIn1 = &aMem[pOp->p1];
  pOut = &aMem[pOp->p2];
  assert( pOut!=pIn1 );
  while( 1 ){
    sqlite3VdbeMemShallowCopy(pOut, pIn1, MEM_Ephem);
    Deephemeralize(pOut);
#ifdef SQLITE_DEBUG
    pOut->pScopyFrom = 0;
#endif
    REGISTER_TRACE(pOp->p2+pOp->p3-n, pOut);
    if( (n--)==0 ) break;
    pOut++;
    pIn1++;
  }
  break;
}

/* Opcode: SCopy P1 P2 * * *
** Synopsis: r[P2]=r[P1]
**
** Make a shallow copy of register P1 into register P2.
**
** This instruction makes a shallow copy of the value.  If the value
** is a string or blob, then the copy is only a pointer to the
** original and hence if the original changes so will the copy.
** Worse, if the original is deallocated, the copy becomes invalid.
** Thus the program must guarantee that the original will not change
** during the lifetime of the copy.  Use OP_Copy to make a complete
** copy.
*/
case OP_SCopy: {            /* out2 */
  pIn1 = &aMem[pOp->p1];
  pOut = &aMem[pOp->p2];
  assert( pOut!=pIn1 );
  sqlite3VdbeMemShallowCopy(pOut, pIn1, MEM_Ephem);
#ifdef SQLITE_DEBUG
  if( pOut->pScopyFrom==0 ) pOut->pScopyFrom = pIn1;
#endif
  break;
}

/* Opcode: IntCopy P1 P2 * * *
** Synopsis: r[P2]=r[P1]
**
** Transfer the integer value held in register P1 into register P2.
**
** This is an optimized version of SCopy that works only for integer
** values.
*/
case OP_IntCopy: {            /* out2 */
  pIn1 = &aMem[pOp->p1];
  assert( (pIn1->flags & MEM_Int)!=0 );
  pOut = &aMem[pOp->p2];
  sqlite3VdbeMemSetInt64(pOut, pIn1->u.i);
  break;
}

/* Opcode: ResultRow P1 P2 * * *
** Synopsis: output=r[P1@P2]
**
** The registers P1 through P1+P2-1 contain a single row of
** results. This opcode causes the sqlite3_step() call to terminate
** with an SQLITE_ROW return code and it sets up the sqlite3_stmt
** structure to provide access to the r(P1)..r(P1+P2-1) values as
** the result row.
*/
case OP_ResultRow: {
  Mem *pMem;
  int i;
  assert( p->nResColumn==pOp->p2 );
  assert( pOp->p1>0 );
  assert( pOp->p1+pOp->p2<=(p->nMem+1 - p->nCursor)+1 );

#ifndef SQLITE_OMIT_PROGRESS_CALLBACK
  /* Run the progress counter just before returning.
  */
  if( db->xProgress!=0
   && nVmStep>=nProgressLimit
   && db->xProgress(db->pProgressArg)!=0
  ){
    rc = SQLITE_INTERRUPT;
    goto abort_due_to_error;
  }
#endif

  /* If this statement has violated immediate foreign key constraints, do
  ** not return the number of rows modified. And do not RELEASE the statement
  ** transaction. It needs to be rolled back.  */
  if( SQLITE_OK!=(rc = sqlite3VdbeCheckFk(p, 0)) ){
    assert( db->flags&SQLITE_CountRows );
    assert( p->usesStmtJournal );
    goto abort_due_to_error;
  }

  /* If the SQLITE_CountRows flag is set in sqlite3.flags mask, then 
  ** DML statements invoke this opcode to return the number of rows 
  ** modified to the user. This is the only way that a VM that
  ** opens a statement transaction may invoke this opcode.
  **
  ** In case this is such a statement, close any statement transaction
  ** opened by this VM before returning control to the user. This is to
  ** ensure that statement-transactions are always nested, not overlapping.
  ** If the open statement-transaction is not closed here, then the user
  ** may step another VM that opens its own statement transaction. This
  ** may lead to overlapping statement transactions.
  **
  ** The statement transaction is never a top-level transaction.  Hence
  ** the RELEASE call below can never fail.
  */
  assert( p->iStatement==0 || db->flags&SQLITE_CountRows );
  rc = sqlite3VdbeCloseStatement(p, SAVEPOINT_RELEASE);
  assert( rc==SQLITE_OK );

  /* Invalidate all ephemeral cursor row caches */
  p->cacheCtr = (p->cacheCtr + 2)|1;

  /* Make sure the results of the current row are \000 terminated
  ** and have an assigned type.  The results are de-ephemeralized as
  ** a side effect.
  */
  pMem = p->pResultSet = &aMem[pOp->p1];
  for(i=0; i<pOp->p2; i++){
    assert( memIsValid(&pMem[i]) );
    Deephemeralize(&pMem[i]);
    assert( (pMem[i].flags & MEM_Ephem)==0
            || (pMem[i].flags & (MEM_Str|MEM_Blob))==0 );
    sqlite3VdbeMemNulTerminate(&pMem[i]);
    REGISTER_TRACE(pOp->p1+i, &pMem[i]);
  }
  if( db->mallocFailed ) goto no_mem;

  if( db->mTrace & SQLITE_TRACE_ROW ){
    db->xTrace(SQLITE_TRACE_ROW, db->pTraceArg, p, 0);
  }

  /* Return SQLITE_ROW
  */
  p->pc = (int)(pOp - aOp) + 1;
  rc = SQLITE_ROW;
  goto vdbe_return;
}

/* Opcode: Concat P1 P2 P3 * *
** Synopsis: r[P3]=r[P2]+r[P1]
**
** Add the text in register P1 onto the end of the text in
** register P2 and store the result in register P3.
** If either the P1 or P2 text are NULL then store NULL in P3.
**
**   P3 = P2 || P1
**
** It is illegal for P1 and P3 to be the same register. Sometimes,
** if P3 is the same register as P2, the implementation is able
** to avoid a memcpy().
*/
case OP_Concat: {           /* same as TK_CONCAT, in1, in2, out3 */
  i64 nByte;

  pIn1 = &aMem[pOp->p1];
  pIn2 = &aMem[pOp->p2];
  pOut = &aMem[pOp->p3];
  assert( pIn1!=pOut );
  if( (pIn1->flags | pIn2->flags) & MEM_Null ){
    sqlite3VdbeMemSetNull(pOut);
    break;
  }
  if( ExpandBlob(pIn1) || ExpandBlob(pIn2) ) goto no_mem;
  Stringify(pIn1, encoding);
  Stringify(pIn2, encoding);
  nByte = pIn1->n + pIn2->n;
  if( nByte>db->aLimit[SQLITE_LIMIT_LENGTH] ){
    goto too_big;
  }
  if( sqlite3VdbeMemGrow(pOut, (int)nByte+2, pOut==pIn2) ){
    goto no_mem;
  }
  MemSetTypeFlag(pOut, MEM_Str);
  if( pOut!=pIn2 ){
    memcpy(pOut->z, pIn2->z, pIn2->n);
  }
  memcpy(&pOut->z[pIn2->n], pIn1->z, pIn1->n);
  pOut->z[nByte]=0;
  pOut->z[nByte+1] = 0;
  pOut->flags |= MEM_Term;
  pOut->n = (int)nByte;
  pOut->enc = encoding;
  UPDATE_MAX_BLOBSIZE(pOut);
  break;
}

/* Opcode: Add P1 P2 P3 * *
** Synopsis: r[P3]=r[P1]+r[P2]
**
** Add the value in register P1 to the value in register P2
** and store the result in register P3.
** If either input is NULL, the result is NULL.
*/
/* Opcode: Multiply P1 P2 P3 * *
** Synopsis: r[P3]=r[P1]*r[P2]
**
**
** Multiply the value in register P1 by the value in register P2
** and store the result in register P3.
** If either input is NULL, the result is NULL.
*/
/* Opcode: Subtract P1 P2 P3 * *
** Synopsis: r[P3]=r[P2]-r[P1]
**
** Subtract the value in register P1 from the value in register P2
** and store the result in register P3.
** If either input is NULL, the result is NULL.
*/
/* Opcode: Divide P1 P2 P3 * *
** Synopsis: r[P3]=r[P2]/r[P1]
**
** Divide the value in register P1 by the value in register P2
** and store the result in register P3 (P3=P2/P1). If the value in 
** register P1 is zero, then the result is NULL. If either input is 
** NULL, the result is NULL.
*/
/* Opcode: Remainder P1 P2 P3 * *
** Synopsis: r[P3]=r[P2]%r[P1]
**
** Compute the remainder after integer register P2 is divided by 
** register P1 and store the result in register P3. 
** If the value in register P1 is zero the result is NULL.
** If either operand is NULL, the result is NULL.
*/
case OP_Add:                   /* same as TK_PLUS, in1, in2, out3 */
case OP_Subtract:              /* same as TK_MINUS, in1, in2, out3 */
case OP_Multiply:              /* same as TK_STAR, in1, in2, out3 */
case OP_Divide:                /* same as TK_SLASH, in1, in2, out3 */
case OP_Remainder: {           /* same as TK_REM, in1, in2, out3 */
  char bIntint;   /* Started out as two integer operands */
  u16 flags;      /* Combined MEM_* flags from both inputs */
  u16 type1;      /* Numeric type of left operand */
  u16 type2;      /* Numeric type of right operand */
  i64 iA;         /* Integer value of left operand */
  i64 iB;         /* Integer value of right operand */
  double rA;      /* Real value of left operand */
  double rB;      /* Real value of right operand */

  pIn1 = &aMem[pOp->p1];
  type1 = numericType(pIn1);
  pIn2 = &aMem[pOp->p2];
  type2 = numericType(pIn2);
  pOut = &aMem[pOp->p3];
  flags = pIn1->flags | pIn2->flags;
  if( (flags & MEM_Null)!=0 ) goto arithmetic_result_is_null;
  if( (type1 & type2 & MEM_Int)!=0 ){
    iA = pIn1->u.i;
    iB = pIn2->u.i;
    bIntint = 1;
    switch( pOp->opcode ){
      case OP_Add:       if( sqlite3AddInt64(&iB,iA) ) goto fp_math;  break;
      case OP_Subtract:  if( sqlite3SubInt64(&iB,iA) ) goto fp_math;  break;
      case OP_Multiply:  if( sqlite3MulInt64(&iB,iA) ) goto fp_math;  break;
      case OP_Divide: {
        if( iA==0 ) goto arithmetic_result_is_null;
        if( iA==-1 && iB==SMALLEST_INT64 ) goto fp_math;
        iB /= iA;
        break;
      }
      default: {
        if( iA==0 ) goto arithmetic_result_is_null;
        if( iA==-1 ) iA = 1;
        iB %= iA;
        break;
      }
    }
    pOut->u.i = iB;
    MemSetTypeFlag(pOut, MEM_Int);
  }else{
    bIntint = 0;
fp_math:
    rA = sqlite3VdbeRealValue(pIn1);
    rB = sqlite3VdbeRealValue(pIn2);
    switch( pOp->opcode ){
      case OP_Add:         rB += rA;       break;
      case OP_Subtract:    rB -= rA;       break;
      case OP_Multiply:    rB *= rA;       break;
      case OP_Divide: {
        /* (double)0 In case of SQLITE_OMIT_FLOATING_POINT... */
        if( rA==(double)0 ) goto arithmetic_result_is_null;
        rB /= rA;
        break;
      }
      default: {
        iA = (i64)rA;
        iB = (i64)rB;
        if( iA==0 ) goto arithmetic_result_is_null;
        if( iA==-1 ) iA = 1;
        rB = (double)(iB % iA);
        break;
      }
    }
#ifdef SQLITE_OMIT_FLOATING_POINT
    pOut->u.i = rB;
    MemSetTypeFlag(pOut, MEM_Int);
#else
    if( sqlite3IsNaN(rB) ){
      goto arithmetic_result_is_null;
    }
    pOut->u.r = rB;
    MemSetTypeFlag(pOut, MEM_Real);
    if( ((type1|type2)&MEM_Real)==0 && !bIntint ){
      sqlite3VdbeIntegerAffinity(pOut);
    }
#endif
  }
  break;

arithmetic_result_is_null:
  sqlite3VdbeMemSetNull(pOut);
  break;
}

/* Opcode: CollSeq P1 * * P4
**
** P4 is a pointer to a CollSeq struct. If the next call to a user function
** or aggregate calls sqlite3GetFuncCollSeq(), this collation sequence will
** be returned. This is used by the built-in min(), max() and nullif()
** functions.
**
** If P1 is not zero, then it is a register that a subsequent min() or
** max() aggregate will set to 1 if the current row is not the minimum or
** maximum.  The P1 register is initialized to 0 by this instruction.
**
** The interface used by the implementation of the aforementioned functions
** to retrieve the collation sequence set by this opcode is not available
** publicly.  Only built-in functions have access to this feature.
*/
case OP_CollSeq: {
  assert( pOp->p4type==P4_COLLSEQ );
  if( pOp->p1 ){
    sqlite3VdbeMemSetInt64(&aMem[pOp->p1], 0);
  }
  break;
}

/* Opcode: Function0 P1 P2 P3 P4 P5
** Synopsis: r[P3]=func(r[P2@P5])
**
** Invoke a user function (P4 is a pointer to a FuncDef object that
** defines the function) with P5 arguments taken from register P2 and
** successors.  The result of the function is stored in register P3.
** Register P3 must not be one of the function inputs.
**
** P1 is a 32-bit bitmask indicating whether or not each argument to the 
** function was determined to be constant at compile time. If the first
** argument was constant then bit 0 of P1 is set. This is used to determine
** whether meta data associated with a user function argument using the
** sqlite3_set_auxdata() API may be safely retained until the next
** invocation of this opcode.
**
** See also: Function, AggStep, AggFinal
*/
/* Opcode: Function P1 P2 P3 P4 P5
** Synopsis: r[P3]=func(r[P2@P5])
**
** Invoke a user function (P4 is a pointer to an sqlite3_context object that
** contains a pointer to the function to be run) with P5 arguments taken
** from register P2 and successors.  The result of the function is stored
** in register P3.  Register P3 must not be one of the function inputs.
**
** P1 is a 32-bit bitmask indicating whether or not each argument to the 
** function was determined to be constant at compile time. If the first
** argument was constant then bit 0 of P1 is set. This is used to determine
** whether meta data associated with a user function argument using the
** sqlite3_set_auxdata() API may be safely retained until the next
** invocation of this opcode.
**
** SQL functions are initially coded as OP_Function0 with P4 pointing
** to a FuncDef object.  But on first evaluation, the P4 operand is
** automatically converted into an sqlite3_context object and the operation
** changed to this OP_Function opcode.  In this way, the initialization of
** the sqlite3_context object occurs only once, rather than once for each
** evaluation of the function.
**
** See also: Function0, AggStep, AggFinal
*/
case OP_Function0: {
  int n;
  sqlite3_context *pCtx;

  assert( pOp->p4type==P4_FUNCDEF );
  n = pOp->p5;
  assert( pOp->p3>0 && pOp->p3<=(p->nMem+1 - p->nCursor) );
  assert( n==0 || (pOp->p2>0 && pOp->p2+n<=(p->nMem+1 - p->nCursor)+1) );
  assert( pOp->p3<pOp->p2 || pOp->p3>=pOp->p2+n );
  pCtx = sqlite3DbMallocRawNN(db, sizeof(*pCtx) + (n-1)*sizeof(sqlite3_value*));
  if( pCtx==0 ) goto no_mem;
  pCtx->pOut = 0;
  pCtx->pFunc = pOp->p4.pFunc;
  pCtx->iOp = (int)(pOp - aOp);
  pCtx->pVdbe = p;
  pCtx->argc = n;
  pOp->p4type = P4_FUNCCTX;
  pOp->p4.pCtx = pCtx;
  pOp->opcode = OP_Function;
  /* Fall through into OP_Function */
}
case OP_Function: {
  int i;
  sqlite3_context *pCtx;

  assert( pOp->p4type==P4_FUNCCTX );
  pCtx = pOp->p4.pCtx;

  /* If this function is inside of a trigger, the register array in aMem[]
  ** might change from one evaluation to the next.  The next block of code
  ** checks to see if the register array has changed, and if so it
  ** reinitializes the relavant parts of the sqlite3_context object */
  pOut = &aMem[pOp->p3];
  if( pCtx->pOut != pOut ){
    pCtx->pOut = pOut;
    for(i=pCtx->argc-1; i>=0; i--) pCtx->argv[i] = &aMem[pOp->p2+i];
  }

  memAboutToChange(p, pCtx->pOut);
#ifdef SQLITE_DEBUG
  for(i=0; i<pCtx->argc; i++){
    assert( memIsValid(pCtx->argv[i]) );
    REGISTER_TRACE(pOp->p2+i, pCtx->argv[i]);
  }
#endif
  MemSetTypeFlag(pCtx->pOut, MEM_Null);
  pCtx->fErrorOrAux = 0;
  db->lastRowid = lastRowid;
  (*pCtx->pFunc->xSFunc)(pCtx, pCtx->argc, pCtx->argv);/* IMP: R-24505-23230 */
  lastRowid = db->lastRowid;  /* Remember rowid changes made by xSFunc */

  /* If the function returned an error, throw an exception */
  if( pCtx->fErrorOrAux ){
    if( pCtx->isError ){
      sqlite3VdbeError(p, "%s", sqlite3_value_text(pCtx->pOut));
      rc = pCtx->isError;
    }
    sqlite3VdbeDeleteAuxData(db, &p->pAuxData, pCtx->iOp, pOp->p1);
    if( rc ) goto abort_due_to_error;
  }

  /* Copy the result of the function into register P3 */
  if( pOut->flags & (MEM_Str|MEM_Blob) ){
    sqlite3VdbeChangeEncoding(pCtx->pOut, encoding);
    if( sqlite3VdbeMemTooBig(pCtx->pOut) ) goto too_big;
  }

  REGISTER_TRACE(pOp->p3, pCtx->pOut);
  UPDATE_MAX_BLOBSIZE(pCtx->pOut);
  break;
}

/* Opcode: BitAnd P1 P2 P3 * *
** Synopsis: r[P3]=r[P1]&r[P2]
**
** Take the bit-wise AND of the values in register P1 and P2 and
** store the result in register P3.
** If either input is NULL, the result is NULL.
*/
/* Opcode: BitOr P1 P2 P3 * *
** Synopsis: r[P3]=r[P1]|r[P2]
**
** Take the bit-wise OR of the values in register P1 and P2 and
** store the result in register P3.
** If either input is NULL, the result is NULL.
*/
/* Opcode: ShiftLeft P1 P2 P3 * *
** Synopsis: r[P3]=r[P2]<<r[P1]
**
** Shift the integer value in register P2 to the left by the
** number of bits specified by the integer in register P1.
** Store the result in register P3.
** If either input is NULL, the result is NULL.
*/
/* Opcode: ShiftRight P1 P2 P3 * *
** Synopsis: r[P3]=r[P2]>>r[P1]
**
** Shift the integer value in register P2 to the right by the
** number of bits specified by the integer in register P1.
** Store the result in register P3.
** If either input is NULL, the result is NULL.
*/
case OP_BitAnd:                 /* same as TK_BITAND, in1, in2, out3 */
case OP_BitOr:                  /* same as TK_BITOR, in1, in2, out3 */
case OP_ShiftLeft:              /* same as TK_LSHIFT, in1, in2, out3 */
case OP_ShiftRight: {           /* same as TK_RSHIFT, in1, in2, out3 */
  i64 iA;
  u64 uA;
  i64 iB;
  u8 op;

  pIn1 = &aMem[pOp->p1];
  pIn2 = &aMem[pOp->p2];
  pOut = &aMem[pOp->p3];
  if( (pIn1->flags | pIn2->flags) & MEM_Null ){
    sqlite3VdbeMemSetNull(pOut);
    break;
  }
  iA = sqlite3VdbeIntValue(pIn2);
  iB = sqlite3VdbeIntValue(pIn1);
  op = pOp->opcode;
  if( op==OP_BitAnd ){
    iA &= iB;
  }else if( op==OP_BitOr ){
    iA |= iB;
  }else if( iB!=0 ){
    assert( op==OP_ShiftRight || op==OP_ShiftLeft );

    /* If shifting by a negative amount, shift in the other direction */
    if( iB<0 ){
      assert( OP_ShiftRight==OP_ShiftLeft+1 );
      op = 2*OP_ShiftLeft + 1 - op;
      iB = iB>(-64) ? -iB : 64;
    }

    if( iB>=64 ){
      iA = (iA>=0 || op==OP_ShiftLeft) ? 0 : -1;
    }else{
      memcpy(&uA, &iA, sizeof(uA));
      if( op==OP_ShiftLeft ){
        uA <<= iB;
      }else{
        uA >>= iB;
        /* Sign-extend on a right shift of a negative number */
        if( iA<0 ) uA |= ((((u64)0xffffffff)<<32)|0xffffffff) << (64-iB);
      }
      memcpy(&iA, &uA, sizeof(iA));
    }
  }
  pOut->u.i = iA;
  MemSetTypeFlag(pOut, MEM_Int);
  break;
}

/* Opcode: AddImm  P1 P2 * * *
** Synopsis: r[P1]=r[P1]+P2
** 
** Add the constant P2 to the value in register P1.
** The result is always an integer.
**
** To force any register to be an integer, just add 0.
*/
case OP_AddImm: {            /* in1 */
  pIn1 = &aMem[pOp->p1];
  memAboutToChange(p, pIn1);
  sqlite3VdbeMemIntegerify(pIn1);
  pIn1->u.i += pOp->p2;
  break;
}

/* Opcode: MustBeInt P1 P2 * * *
** 
** Force the value in register P1 to be an integer.  If the value
** in P1 is not an integer and cannot be converted into an integer
** without data loss, then jump immediately to P2, or if P2==0
** raise an SQLITE_MISMATCH exception.
*/
case OP_MustBeInt: {            /* jump, in1 */
  pIn1 = &aMem[pOp->p1];
  if( (pIn1->flags & MEM_Int)==0 ){
    applyAffinity(pIn1, SQLITE_AFF_NUMERIC, encoding);
    VdbeBranchTaken((pIn1->flags&MEM_Int)==0, 2);
    if( (pIn1->flags & MEM_Int)==0 ){
      if( pOp->p2==0 ){
        rc = SQLITE_MISMATCH;
        goto abort_due_to_error;
      }else{
        goto jump_to_p2;
      }
    }
  }
  MemSetTypeFlag(pIn1, MEM_Int);
  break;
}

#ifndef SQLITE_OMIT_FLOATING_POINT
/* Opcode: RealAffinity P1 * * * *
**
** If register P1 holds an integer convert it to a real value.
**
** This opcode is used when extracting information from a column that
** has REAL affinity.  Such column values may still be stored as
** integers, for space efficiency, but after extraction we want them
** to have only a real value.
*/
case OP_RealAffinity: {                  /* in1 */
  pIn1 = &aMem[pOp->p1];
  if( pIn1->flags & MEM_Int ){
    sqlite3VdbeMemRealify(pIn1);
  }
  break;
}
#endif

#ifndef SQLITE_OMIT_CAST
/* Opcode: Cast P1 P2 * * *
** Synopsis: affinity(r[P1])
**
** Force the value in register P1 to be the type defined by P2.
** 
** <ul>
** <li value="97"> TEXT
** <li value="98"> BLOB
** <li value="99"> NUMERIC
** <li value="100"> INTEGER
** <li value="101"> REAL
** </ul>
**
** A NULL value is not changed by this routine.  It remains NULL.
*/
case OP_Cast: {                  /* in1 */
  assert( pOp->p2>=SQLITE_AFF_BLOB && pOp->p2<=SQLITE_AFF_REAL );
  testcase( pOp->p2==SQLITE_AFF_TEXT );
  testcase( pOp->p2==SQLITE_AFF_BLOB );
  testcase( pOp->p2==SQLITE_AFF_NUMERIC );
  testcase( pOp->p2==SQLITE_AFF_INTEGER );
  testcase( pOp->p2==SQLITE_AFF_REAL );
  pIn1 = &aMem[pOp->p1];
  memAboutToChange(p, pIn1);
  rc = ExpandBlob(pIn1);
  sqlite3VdbeMemCast(pIn1, pOp->p2, encoding);
  UPDATE_MAX_BLOBSIZE(pIn1);
  if( rc ) goto abort_due_to_error;
  break;
}
#endif /* SQLITE_OMIT_CAST */

/* Opcode: Eq P1 P2 P3 P4 P5
** Synopsis: IF r[P3]==r[P1]
**
** Compare the values in register P1 and P3.  If reg(P3)==reg(P1) then
** jump to address P2.  Or if the SQLITE_STOREP2 flag is set in P5, then
** store the result of comparison in register P2.
**
** The SQLITE_AFF_MASK portion of P5 must be an affinity character -
** SQLITE_AFF_TEXT, SQLITE_AFF_INTEGER, and so forth. An attempt is made 
** to coerce both inputs according to this affinity before the
** comparison is made. If the SQLITE_AFF_MASK is 0x00, then numeric
** affinity is used. Note that the affinity conversions are stored
** back into the input registers P1 and P3.  So this opcode can cause
** persistent changes to registers P1 and P3.
**
** Once any conversions have taken place, and neither value is NULL, 
** the values are compared. If both values are blobs then memcmp() is
** used to determine the results of the comparison.  If both values
** are text, then the appropriate collating function specified in
** P4 is used to do the comparison.  If P4 is not specified then
** memcmp() is used to compare text string.  If both values are
** numeric, then a numeric comparison is used. If the two values
** are of different types, then numbers are considered less than
** strings and strings are considered less than blobs.
**
** If SQLITE_NULLEQ is set in P5 then the result of comparison is always either
** true or false and is never NULL.  If both operands are NULL then the result
** of comparison is true.  If either operand is NULL then the result is false.
** If neither operand is NULL the result is the same as it would be if
** the SQLITE_NULLEQ flag were omitted from P5.
**
** If both SQLITE_STOREP2 and SQLITE_KEEPNULL flags are set then the
** content of r[P2] is only changed if the new value is NULL or 0 (false).
** In other words, a prior r[P2] value will not be overwritten by 1 (true).
*/
/* Opcode: Ne P1 P2 P3 P4 P5
** Synopsis: IF r[P3]!=r[P1]
**
** This works just like the Eq opcode except that the jump is taken if
** the operands in registers P1 and P3 are not equal.  See the Eq opcode for
** additional information.
**
** If both SQLITE_STOREP2 and SQLITE_KEEPNULL flags are set then the
** content of r[P2] is only changed if the new value is NULL or 1 (true).
** In other words, a prior r[P2] value will not be overwritten by 0 (false).
*/
/* Opcode: Lt P1 P2 P3 P4 P5
** Synopsis: IF r[P3]<r[P1]
**
** Compare the values in register P1 and P3.  If reg(P3)<reg(P1) then
** jump to address P2.  Or if the SQLITE_STOREP2 flag is set in P5 store
** the result of comparison (0 or 1 or NULL) into register P2.
**
** If the SQLITE_JUMPIFNULL bit of P5 is set and either reg(P1) or
** reg(P3) is NULL then the take the jump.  If the SQLITE_JUMPIFNULL 
** bit is clear then fall through if either operand is NULL.
**
** The SQLITE_AFF_MASK portion of P5 must be an affinity character -
** SQLITE_AFF_TEXT, SQLITE_AFF_INTEGER, and so forth. An attempt is made 
** to coerce both inputs according to this affinity before the
** comparison is made. If the SQLITE_AFF_MASK is 0x00, then numeric
** affinity is used. Note that the affinity conversions are stored
** back into the input registers P1 and P3.  So this opcode can cause
** persistent changes to registers P1 and P3.
**
** Once any conversions have taken place, and neither value is NULL, 
** the values are compared. If both values are blobs then memcmp() is
** used to determine the results of the comparison.  If both values
** are text, then the appropriate collating function specified in
** P4 is  used to do the comparison.  If P4 is not specified then
** memcmp() is used to compare text string.  If both values are
** numeric, then a numeric comparison is used. If the two values
** are of different types, then numbers are considered less than
** strings and strings are considered less than blobs.
*/
/* Opcode: Le P1 P2 P3 P4 P5
** Synopsis: IF r[P3]<=r[P1]
**
** This works just like the Lt opcode except that the jump is taken if
** the content of register P3 is less than or equal to the content of
** register P1.  See the Lt opcode for additional information.
*/
/* Opcode: Gt P1 P2 P3 P4 P5
** Synopsis: IF r[P3]>r[P1]
**
** This works just like the Lt opcode except that the jump is taken if
** the content of register P3 is greater than the content of
** register P1.  See the Lt opcode for additional information.
*/
/* Opcode: Ge P1 P2 P3 P4 P5
** Synopsis: IF r[P3]>=r[P1]
**
** This works just like the Lt opcode except that the jump is taken if
** the content of register P3 is greater than or equal to the content of
** register P1.  See the Lt opcode for additional information.
*/
case OP_Eq:               /* same as TK_EQ, jump, in1, in3 */
case OP_Ne:               /* same as TK_NE, jump, in1, in3 */
case OP_Lt:               /* same as TK_LT, jump, in1, in3 */
case OP_Le:               /* same as TK_LE, jump, in1, in3 */
case OP_Gt:               /* same as TK_GT, jump, in1, in3 */
case OP_Ge: {             /* same as TK_GE, jump, in1, in3 */
  int res, res2;      /* Result of the comparison of pIn1 against pIn3 */
  char affinity;      /* Affinity to use for comparison */
  u16 flags1;         /* Copy of initial value of pIn1->flags */
  u16 flags3;         /* Copy of initial value of pIn3->flags */

  pIn1 = &aMem[pOp->p1];
  pIn3 = &aMem[pOp->p3];
  flags1 = pIn1->flags;
  flags3 = pIn3->flags;
  if( (flags1 | flags3)&MEM_Null ){
    /* One or both operands are NULL */
    if( pOp->p5 & SQLITE_NULLEQ ){
      /* If SQLITE_NULLEQ is set (which will only happen if the operator is
      ** OP_Eq or OP_Ne) then take the jump or not depending on whether
      ** or not both operands are null.
      */
      assert( pOp->opcode==OP_Eq || pOp->opcode==OP_Ne );
      assert( (flags1 & MEM_Cleared)==0 );
      assert( (pOp->p5 & SQLITE_JUMPIFNULL)==0 );
      if( (flags1&MEM_Null)!=0
       && (flags3&MEM_Null)!=0
       && (flags3&MEM_Cleared)==0
      ){
        res = 0;  /* Operands are equal */
      }else{
        res = 1;  /* Operands are not equal */
      }
    }else{
      /* SQLITE_NULLEQ is clear and at least one operand is NULL,
      ** then the result is always NULL.
      ** The jump is taken if the SQLITE_JUMPIFNULL bit is set.
      */
      if( pOp->p5 & SQLITE_STOREP2 ){
        pOut = &aMem[pOp->p2];
        iCompare = 1;    /* Operands are not equal */
        memAboutToChange(p, pOut);
        MemSetTypeFlag(pOut, MEM_Null);
        REGISTER_TRACE(pOp->p2, pOut);
      }else{
        VdbeBranchTaken(2,3);
        if( pOp->p5 & SQLITE_JUMPIFNULL ){
          goto jump_to_p2;
        }
      }
      break;
    }
  }else{
    /* Neither operand is NULL.  Do a comparison. */
    affinity = pOp->p5 & SQLITE_AFF_MASK;
    if( affinity>=SQLITE_AFF_NUMERIC ){
      if( (flags1 | flags3)&MEM_Str ){
        if( (flags1 & (MEM_Int|MEM_Real|MEM_Str))==MEM_Str ){
          applyNumericAffinity(pIn1,0);
          testcase( flags3!=pIn3->flags ); /* Possible if pIn1==pIn3 */
          flags3 = pIn3->flags;
        }
        if( (flags3 & (MEM_Int|MEM_Real|MEM_Str))==MEM_Str ){
          applyNumericAffinity(pIn3,0);
        }
      }
      /* Handle the common case of integer comparison here, as an
      ** optimization, to avoid a call to sqlite3MemCompare() */
      if( (pIn1->flags & pIn3->flags & MEM_Int)!=0 ){
        if( pIn3->u.i > pIn1->u.i ){ res = +1; goto compare_op; }
        if( pIn3->u.i < pIn1->u.i ){ res = -1; goto compare_op; }
        res = 0;
        goto compare_op;
      }
    }else if( affinity==SQLITE_AFF_TEXT ){
      if( (flags1 & MEM_Str)==0 && (flags1 & (MEM_Int|MEM_Real))!=0 ){
        testcase( pIn1->flags & MEM_Int );
        testcase( pIn1->flags & MEM_Real );
        sqlite3VdbeMemStringify(pIn1, encoding, 1);
        testcase( (flags1&MEM_Dyn) != (pIn1->flags&MEM_Dyn) );
        flags1 = (pIn1->flags & ~MEM_TypeMask) | (flags1 & MEM_TypeMask);
        assert( pIn1!=pIn3 );
      }
      if( (flags3 & MEM_Str)==0 && (flags3 & (MEM_Int|MEM_Real))!=0 ){
        testcase( pIn3->flags & MEM_Int );
        testcase( pIn3->flags & MEM_Real );
        sqlite3VdbeMemStringify(pIn3, encoding, 1);
        testcase( (flags3&MEM_Dyn) != (pIn3->flags&MEM_Dyn) );
        flags3 = (pIn3->flags & ~MEM_TypeMask) | (flags3 & MEM_TypeMask);
      }
    }
    assert( pOp->p4type==P4_COLLSEQ || pOp->p4.pColl==0 );
    res = sqlite3MemCompare(pIn3, pIn1, pOp->p4.pColl);
  }
compare_op:
  switch( pOp->opcode ){
    case OP_Eq:    res2 = res==0;     break;
    case OP_Ne:    res2 = res;        break;
    case OP_Lt:    res2 = res<0;      break;
    case OP_Le:    res2 = res<=0;     break;
    case OP_Gt:    res2 = res>0;      break;
    default:       res2 = res>=0;     break;
  }

  /* Undo any changes made by applyAffinity() to the input registers. */
  assert( (pIn1->flags & MEM_Dyn) == (flags1 & MEM_Dyn) );
  pIn1->flags = flags1;
  assert( (pIn3->flags & MEM_Dyn) == (flags3 & MEM_Dyn) );
  pIn3->flags = flags3;

  if( pOp->p5 & SQLITE_STOREP2 ){
    pOut = &aMem[pOp->p2];
    iCompare = res;
    res2 = res2!=0;  /* For this path res2 must be exactly 0 or 1 */
    if( (pOp->p5 & SQLITE_KEEPNULL)!=0 ){
      /* The KEEPNULL flag prevents OP_Eq from overwriting a NULL with 1
      ** and prevents OP_Ne from overwriting NULL with 0.  This flag
      ** is only used in contexts where either:
      **   (1) op==OP_Eq && (r[P2]==NULL || r[P2]==0)
      **   (2) op==OP_Ne && (r[P2]==NULL || r[P2]==1)
      ** Therefore it is not necessary to check the content of r[P2] for
      ** NULL. */
      assert( pOp->opcode==OP_Ne || pOp->opcode==OP_Eq );
      assert( res2==0 || res2==1 );
      testcase( res2==0 && pOp->opcode==OP_Eq );
      testcase( res2==1 && pOp->opcode==OP_Eq );
      testcase( res2==0 && pOp->opcode==OP_Ne );
      testcase( res2==1 && pOp->opcode==OP_Ne );
      if( (pOp->opcode==OP_Eq)==res2 ) break;
    }
    memAboutToChange(p, pOut);
    MemSetTypeFlag(pOut, MEM_Int);
    pOut->u.i = res2;
    REGISTER_TRACE(pOp->p2, pOut);
  }else{
    VdbeBranchTaken(res!=0, (pOp->p5 & SQLITE_NULLEQ)?2:3);
    if( res2 ){
      goto jump_to_p2;
    }
  }
  break;
}

/* Opcode: ElseNotEq * P2 * * *
**
** This opcode must immediately follow an OP_Lt or OP_Gt comparison operator.
** If result of an OP_Eq comparison on the same two operands
** would have be NULL or false (0), then then jump to P2. 
** If the result of an OP_Eq comparison on the two previous operands
** would have been true (1), then fall through.
*/
case OP_ElseNotEq: {       /* same as TK_ESCAPE, jump */
  assert( pOp>aOp );
  assert( pOp[-1].opcode==OP_Lt || pOp[-1].opcode==OP_Gt );
  assert( pOp[-1].p5 & SQLITE_STOREP2 );
  VdbeBranchTaken(iCompare!=0, 2);
  if( iCompare!=0 ) goto jump_to_p2;
  break;
}


/* Opcode: Permutation * * * P4 *
**
** Set the permutation used by the OP_Compare operator to be the array
** of integers in P4.
**
** The permutation is only valid until the next OP_Compare that has
** the OPFLAG_PERMUTE bit set in P5. Typically the OP_Permutation should 
** occur immediately prior to the OP_Compare.
**
** The first integer in the P4 integer array is the length of the array
** and does not become part of the permutation.
*/
case OP_Permutation: {
  assert( pOp->p4type==P4_INTARRAY );
  assert( pOp->p4.ai );
  aPermute = pOp->p4.ai + 1;
  break;
}

/* Opcode: Compare P1 P2 P3 P4 P5
** Synopsis: r[P1@P3] <-> r[P2@P3]
**
** Compare two vectors of registers in reg(P1)..reg(P1+P3-1) (call this
** vector "A") and in reg(P2)..reg(P2+P3-1) ("B").  Save the result of
** the comparison for use by the next OP_Jump instruct.
**
** If P5 has the OPFLAG_PERMUTE bit set, then the order of comparison is
** determined by the most recent OP_Permutation operator.  If the
** OPFLAG_PERMUTE bit is clear, then register are compared in sequential
** order.
**
** P4 is a KeyInfo structure that defines collating sequences and sort
** orders for the comparison.  The permutation applies to registers
** only.  The KeyInfo elements are used sequentially.
**
** The comparison is a sort comparison, so NULLs compare equal,
** NULLs are less than numbers, numbers are less than strings,
** and strings are less than blobs.
*/
case OP_Compare: {
  int n;
  int i;
  int p1;
  int p2;
  const KeyInfo *pKeyInfo;
  int idx;
  CollSeq *pColl;    /* Collating sequence to use on this term */
  int bRev;          /* True for DESCENDING sort order */

  if( (pOp->p5 & OPFLAG_PERMUTE)==0 ) aPermute = 0;
  n = pOp->p3;
  pKeyInfo = pOp->p4.pKeyInfo;
  assert( n>0 );
  assert( pKeyInfo!=0 );
  p1 = pOp->p1;
  p2 = pOp->p2;
#if SQLITE_DEBUG
  if( aPermute ){
    int k, mx = 0;
    for(k=0; k<n; k++) if( aPermute[k]>mx ) mx = aPermute[k];
    assert( p1>0 && p1+mx<=(p->nMem+1 - p->nCursor)+1 );
    assert( p2>0 && p2+mx<=(p->nMem+1 - p->nCursor)+1 );
  }else{
    assert( p1>0 && p1+n<=(p->nMem+1 - p->nCursor)+1 );
    assert( p2>0 && p2+n<=(p->nMem+1 - p->nCursor)+1 );
  }
#endif /* SQLITE_DEBUG */
  for(i=0; i<n; i++){
    idx = aPermute ? aPermute[i] : i;
    assert( memIsValid(&aMem[p1+idx]) );
    assert( memIsValid(&aMem[p2+idx]) );
    REGISTER_TRACE(p1+idx, &aMem[p1+idx]);
    REGISTER_TRACE(p2+idx, &aMem[p2+idx]);
    assert( i<pKeyInfo->nField );
    pColl = pKeyInfo->aColl[i];
    bRev = pKeyInfo->aSortOrder[i];
    iCompare = sqlite3MemCompare(&aMem[p1+idx], &aMem[p2+idx], pColl);
    if( iCompare ){
      if( bRev ) iCompare = -iCompare;
      break;
    }
  }
  aPermute = 0;
  break;
}

/* Opcode: Jump P1 P2 P3 * *
**
** Jump to the instruction at address P1, P2, or P3 depending on whether
** in the most recent OP_Compare instruction the P1 vector was less than
** equal to, or greater than the P2 vector, respectively.
*/
case OP_Jump: {             /* jump */
  if( iCompare<0 ){
    VdbeBranchTaken(0,3); pOp = &aOp[pOp->p1 - 1];
  }else if( iCompare==0 ){
    VdbeBranchTaken(1,3); pOp = &aOp[pOp->p2 - 1];
  }else{
    VdbeBranchTaken(2,3); pOp = &aOp[pOp->p3 - 1];
  }
  break;
}

/* Opcode: And P1 P2 P3 * *
** Synopsis: r[P3]=(r[P1] && r[P2])
**
** Take the logical AND of the values in registers P1 and P2 and
** write the result into register P3.
**
** If either P1 or P2 is 0 (false) then the result is 0 even if
** the other input is NULL.  A NULL and true or two NULLs give
** a NULL output.
*/
/* Opcode: Or P1 P2 P3 * *
** Synopsis: r[P3]=(r[P1] || r[P2])
**
** Take the logical OR of the values in register P1 and P2 and
** store the answer in register P3.
**
** If either P1 or P2 is nonzero (true) then the result is 1 (true)
** even if the other input is NULL.  A NULL and false or two NULLs
** give a NULL output.
*/
case OP_And:              /* same as TK_AND, in1, in2, out3 */
case OP_Or: {             /* same as TK_OR, in1, in2, out3 */
  int v1;    /* Left operand:  0==FALSE, 1==TRUE, 2==UNKNOWN or NULL */
  int v2;    /* Right operand: 0==FALSE, 1==TRUE, 2==UNKNOWN or NULL */

  pIn1 = &aMem[pOp->p1];
  if( pIn1->flags & MEM_Null ){
    v1 = 2;
  }else{
    v1 = sqlite3VdbeIntValue(pIn1)!=0;
  }
  pIn2 = &aMem[pOp->p2];
  if( pIn2->flags & MEM_Null ){
    v2 = 2;
  }else{
    v2 = sqlite3VdbeIntValue(pIn2)!=0;
  }
  if( pOp->opcode==OP_And ){
    static const unsigned char and_logic[] = { 0, 0, 0, 0, 1, 2, 0, 2, 2 };
    v1 = and_logic[v1*3+v2];
  }else{
    static const unsigned char or_logic[] = { 0, 1, 2, 1, 1, 1, 2, 1, 2 };
    v1 = or_logic[v1*3+v2];
  }
  pOut = &aMem[pOp->p3];
  if( v1==2 ){
    MemSetTypeFlag(pOut, MEM_Null);
  }else{
    pOut->u.i = v1;
    MemSetTypeFlag(pOut, MEM_Int);
  }
  break;
}

/* Opcode: Not P1 P2 * * *
** Synopsis: r[P2]= !r[P1]
**
** Interpret the value in register P1 as a boolean value.  Store the
** boolean complement in register P2.  If the value in register P1 is 
** NULL, then a NULL is stored in P2.
*/
case OP_Not: {                /* same as TK_NOT, in1, out2 */
  pIn1 = &aMem[pOp->p1];
  pOut = &aMem[pOp->p2];
  sqlite3VdbeMemSetNull(pOut);
  if( (pIn1->flags & MEM_Null)==0 ){
    pOut->flags = MEM_Int;
    pOut->u.i = !sqlite3VdbeIntValue(pIn1);
  }
  break;
}

/* Opcode: BitNot P1 P2 * * *
** Synopsis: r[P1]= ~r[P1]
**
** Interpret the content of register P1 as an integer.  Store the
** ones-complement of the P1 value into register P2.  If P1 holds
** a NULL then store a NULL in P2.
*/
case OP_BitNot: {             /* same as TK_BITNOT, in1, out2 */
  pIn1 = &aMem[pOp->p1];
  pOut = &aMem[pOp->p2];
  sqlite3VdbeMemSetNull(pOut);
  if( (pIn1->flags & MEM_Null)==0 ){
    pOut->flags = MEM_Int;
    pOut->u.i = ~sqlite3VdbeIntValue(pIn1);
  }
  break;
}

/* Opcode: Once P1 P2 * * *
**
** If the P1 value is equal to the P1 value on the OP_Init opcode at
** instruction 0, then jump to P2.  If the two P1 values differ, then
** set the P1 value on this opcode to equal the P1 value on the OP_Init
** and fall through.
*/
case OP_Once: {             /* jump */
  assert( p->aOp[0].opcode==OP_Init );
  VdbeBranchTaken(p->aOp[0].p1==pOp->p1, 2);
  if( p->aOp[0].p1==pOp->p1 ){
    goto jump_to_p2;
  }else{
    pOp->p1 = p->aOp[0].p1;
  }
  break;
}

/* Opcode: If P1 P2 P3 * *
**
** Jump to P2 if the value in register P1 is true.  The value
** is considered true if it is numeric and non-zero.  If the value
** in P1 is NULL then take the jump if and only if P3 is non-zero.
*/
/* Opcode: IfNot P1 P2 P3 * *
**
** Jump to P2 if the value in register P1 is False.  The value
** is considered false if it has a numeric value of zero.  If the value
** in P1 is NULL then take the jump if and only if P3 is non-zero.
*/
case OP_If:                 /* jump, in1 */
case OP_IfNot: {            /* jump, in1 */
  int c;
  pIn1 = &aMem[pOp->p1];
  if( pIn1->flags & MEM_Null ){
    c = pOp->p3;
  }else{
#ifdef SQLITE_OMIT_FLOATING_POINT
    c = sqlite3VdbeIntValue(pIn1)!=0;
#else
    c = sqlite3VdbeRealValue(pIn1)!=0.0;
#endif
    if( pOp->opcode==OP_IfNot ) c = !c;
  }
  VdbeBranchTaken(c!=0, 2);
  if( c ){
    goto jump_to_p2;
  }
  break;
}

/* Opcode: IsNull P1 P2 * * *
** Synopsis: if r[P1]==NULL goto P2
**
** Jump to P2 if the value in register P1 is NULL.
*/
case OP_IsNull: {            /* same as TK_ISNULL, jump, in1 */
  pIn1 = &aMem[pOp->p1];
  VdbeBranchTaken( (pIn1->flags & MEM_Null)!=0, 2);
  if( (pIn1->flags & MEM_Null)!=0 ){
    goto jump_to_p2;
  }
  break;
}

/* Opcode: NotNull P1 P2 * * *
** Synopsis: if r[P1]!=NULL goto P2
**
** Jump to P2 if the value in register P1 is not NULL.  
*/
case OP_NotNull: {            /* same as TK_NOTNULL, jump, in1 */
  pIn1 = &aMem[pOp->p1];
  VdbeBranchTaken( (pIn1->flags & MEM_Null)==0, 2);
  if( (pIn1->flags & MEM_Null)==0 ){
    goto jump_to_p2;
  }
  break;
}

/* Opcode: Column P1 P2 P3 P4 P5
** Synopsis: r[P3]=PX
**
** Interpret the data that cursor P1 points to as a structure built using
** the MakeRecord instruction.  (See the MakeRecord opcode for additional
** information about the format of the data.)  Extract the P2-th column
** from this record.  If there are less that (P2+1) 
** values in the record, extract a NULL.
**
** The value extracted is stored in register P3.
**
** If the column contains fewer than P2 fields, then extract a NULL.  Or,
** if the P4 argument is a P4_MEM use the value of the P4 argument as
** the result.
**
** If the OPFLAG_CLEARCACHE bit is set on P5 and P1 is a pseudo-table cursor,
** then the cache of the cursor is reset prior to extracting the column.
** The first OP_Column against a pseudo-table after the value of the content
** register has changed should have this bit set.
**
** If the OPFLAG_LENGTHARG and OPFLAG_TYPEOFARG bits are set on P5 when
** the result is guaranteed to only be used as the argument of a length()
** or typeof() function, respectively.  The loading of large blobs can be
** skipped for length() and all content loading can be skipped for typeof().
*/
case OP_Column: {
  int p2;            /* column number to retrieve */
  VdbeCursor *pC;    /* The VDBE cursor */
  BtCursor *pCrsr;   /* The BTree cursor */
  u32 *aOffset;      /* aOffset[i] is offset to start of data for i-th column */
  int len;           /* The length of the serialized data for the column */
  int i;             /* Loop counter */
  Mem *pDest;        /* Where to write the extracted value */
  Mem sMem;          /* For storing the record being decoded */
  const u8 *zData;   /* Part of the record being decoded */
  const u8 *zHdr;    /* Next unparsed byte of the header */
  const u8 *zEndHdr; /* Pointer to first byte after the header */
  u32 offset;        /* Offset into the data */
  u64 offset64;      /* 64-bit offset */
  u32 avail;         /* Number of bytes of available data */
  u32 t;             /* A type code from the record header */
  Mem *pReg;         /* PseudoTable input register */

  pC = p->apCsr[pOp->p1];
  p2 = pOp->p2;

  /* If the cursor cache is stale, bring it up-to-date */
  rc = sqlite3VdbeCursorMoveto(&pC, &p2);
  if( rc ) goto abort_due_to_error;

  assert( pOp->p3>0 && pOp->p3<=(p->nMem+1 - p->nCursor) );
  pDest = &aMem[pOp->p3];
  memAboutToChange(p, pDest);
  assert( pOp->p1>=0 && pOp->p1<p->nCursor );
  assert( pC!=0 );
  assert( p2<pC->nField );
  aOffset = pC->aOffset;
  assert( pC->eCurType!=CURTYPE_VTAB );
  assert( pC->eCurType!=CURTYPE_PSEUDO || pC->nullRow );
  assert( pC->eCurType!=CURTYPE_SORTER );
  pCrsr = pC->uc.pCursor;

  if( pC->cacheStatus!=p->cacheCtr ){                /*OPTIMIZATION-IF-FALSE*/
    if( pC->nullRow ){
      if( pC->eCurType==CURTYPE_PSEUDO ){
        assert( pC->uc.pseudoTableReg>0 );
        pReg = &aMem[pC->uc.pseudoTableReg];
        assert( pReg->flags & MEM_Blob );
        assert( memIsValid(pReg) );
        pC->payloadSize = pC->szRow = avail = pReg->n;
        pC->aRow = (u8*)pReg->z;
      }else{
        sqlite3VdbeMemSetNull(pDest);
        goto op_column_out;
      }
    }else{
      assert( pC->eCurType==CURTYPE_BTREE );
      assert( pCrsr );
      assert( sqlite3BtreeCursorIsValid(pCrsr) );
      pC->payloadSize = sqlite3BtreePayloadSize(pCrsr);
      pC->aRow = sqlite3BtreePayloadFetch(pCrsr, &avail);
      assert( avail<=65536 );  /* Maximum page size is 64KiB */
      if( pC->payloadSize <= (u32)avail ){
        pC->szRow = pC->payloadSize;
      }else if( pC->payloadSize > (u32)db->aLimit[SQLITE_LIMIT_LENGTH] ){
        goto too_big;
      }else{
        pC->szRow = avail;
      }
    }
    pC->cacheStatus = p->cacheCtr;
    pC->iHdrOffset = getVarint32(pC->aRow, offset);
    pC->nHdrParsed = 0;
    aOffset[0] = offset;


    if( avail<offset ){      /*OPTIMIZATION-IF-FALSE*/
      /* pC->aRow does not have to hold the entire row, but it does at least
      ** need to cover the header of the record.  If pC->aRow does not contain
      ** the complete header, then set it to zero, forcing the header to be
      ** dynamically allocated. */
      pC->aRow = 0;
      pC->szRow = 0;

      /* Make sure a corrupt database has not given us an oversize header.
      ** Do this now to avoid an oversize memory allocation.
      **
      ** Type entries can be between 1 and 5 bytes each.  But 4 and 5 byte
      ** types use so much data space that there can only be 4096 and 32 of
      ** them, respectively.  So the maximum header length results from a
      ** 3-byte type for each of the maximum of 32768 columns plus three
      ** extra bytes for the header length itself.  32768*3 + 3 = 98307.
      */
      if( offset > 98307 || offset > pC->payloadSize ){
        rc = SQLITE_CORRUPT_BKPT;
        goto abort_due_to_error;
      }
    }else if( offset>0 ){ /*OPTIMIZATION-IF-TRUE*/
      /* The following goto is an optimization.  It can be omitted and
      ** everything will still work.  But OP_Column is measurably faster
      ** by skipping the subsequent conditional, which is always true.
      */
      zData = pC->aRow;
      assert( pC->nHdrParsed<=p2 );         /* Conditional skipped */
      goto op_column_read_header;
    }
  }

  /* Make sure at least the first p2+1 entries of the header have been
  ** parsed and valid information is in aOffset[] and pC->aType[].
  */
  if( pC->nHdrParsed<=p2 ){
    /* If there is more header available for parsing in the record, try
    ** to extract additional fields up through the p2+1-th field 
    */
    if( pC->iHdrOffset<aOffset[0] ){
      /* Make sure zData points to enough of the record to cover the header. */
      if( pC->aRow==0 ){
        memset(&sMem, 0, sizeof(sMem));
        rc = sqlite3VdbeMemFromBtree(pCrsr, 0, aOffset[0], !pC->isTable, &sMem);
        if( rc!=SQLITE_OK ) goto abort_due_to_error;
        zData = (u8*)sMem.z;
      }else{
        zData = pC->aRow;
      }
  
      /* Fill in pC->aType[i] and aOffset[i] values through the p2-th field. */
    op_column_read_header:
      i = pC->nHdrParsed;
      offset64 = aOffset[i];
      zHdr = zData + pC->iHdrOffset;
      zEndHdr = zData + aOffset[0];
      do{
        if( (t = zHdr[0])<0x80 ){
          zHdr++;
          offset64 += sqlite3VdbeOneByteSerialTypeLen(t);
        }else{
          zHdr += sqlite3GetVarint32(zHdr, &t);
          offset64 += sqlite3VdbeSerialTypeLen(t);
        }
        pC->aType[i++] = t;
        aOffset[i] = (u32)(offset64 & 0xffffffff);
      }while( i<=p2 && zHdr<zEndHdr );

      /* The record is corrupt if any of the following are true:
      ** (1) the bytes of the header extend past the declared header size
      ** (2) the entire header was used but not all data was used
      ** (3) the end of the data extends beyond the end of the record.
      */
      if( (zHdr>=zEndHdr && (zHdr>zEndHdr || offset64!=pC->payloadSize))
       || (offset64 > pC->payloadSize)
      ){
        if( pC->aRow==0 ) sqlite3VdbeMemRelease(&sMem);
        rc = SQLITE_CORRUPT_BKPT;
        goto abort_due_to_error;
      }

      pC->nHdrParsed = i;
      pC->iHdrOffset = (u32)(zHdr - zData);
      if( pC->aRow==0 ) sqlite3VdbeMemRelease(&sMem);
    }else{
      t = 0;
    }

    /* If after trying to extract new entries from the header, nHdrParsed is
    ** still not up to p2, that means that the record has fewer than p2
    ** columns.  So the result will be either the default value or a NULL.
    */
    if( pC->nHdrParsed<=p2 ){
      if( pOp->p4type==P4_MEM ){
        sqlite3VdbeMemShallowCopy(pDest, pOp->p4.pMem, MEM_Static);
      }else{
        sqlite3VdbeMemSetNull(pDest);
      }
      goto op_column_out;
    }
  }else{
    t = pC->aType[p2];
  }

  /* Extract the content for the p2+1-th column.  Control can only
  ** reach this point if aOffset[p2], aOffset[p2+1], and pC->aType[p2] are
  ** all valid.
  */
  assert( p2<pC->nHdrParsed );
  assert( rc==SQLITE_OK );
  assert( sqlite3VdbeCheckMemInvariants(pDest) );
  if( VdbeMemDynamic(pDest) ){
    sqlite3VdbeMemSetNull(pDest);
  }
  assert( t==pC->aType[p2] );
  if( pC->szRow>=aOffset[p2+1] ){
    /* This is the common case where the desired content fits on the original
    ** page - where the content is not on an overflow page */
    zData = pC->aRow + aOffset[p2];
    if( t<12 ){
      sqlite3VdbeSerialGet(zData, t, pDest);
    }else{
      /* If the column value is a string, we need a persistent value, not
      ** a MEM_Ephem value.  This branch is a fast short-cut that is equivalent
      ** to calling sqlite3VdbeSerialGet() and sqlite3VdbeDeephemeralize().
      */
      static const u16 aFlag[] = { MEM_Blob, MEM_Str|MEM_Term };
      pDest->n = len = (t-12)/2;
      pDest->enc = encoding;
      if( pDest->szMalloc < len+2 ){
        pDest->flags = MEM_Null;
        if( sqlite3VdbeMemGrow(pDest, len+2, 0) ) goto no_mem;
      }else{
        pDest->z = pDest->zMalloc;
      }
      memcpy(pDest->z, zData, len);
      pDest->z[len] = 0;
      pDest->z[len+1] = 0;
      pDest->flags = aFlag[t&1];
    }
  }else{
    pDest->enc = encoding;
    /* This branch happens only when content is on overflow pages */
    if( ((pOp->p5 & (OPFLAG_LENGTHARG|OPFLAG_TYPEOFARG))!=0
          && ((t>=12 && (t&1)==0) || (pOp->p5 & OPFLAG_TYPEOFARG)!=0))
     || (len = sqlite3VdbeSerialTypeLen(t))==0
    ){
      /* Content is irrelevant for
      **    1. the typeof() function,
      **    2. the length(X) function if X is a blob, and
      **    3. if the content length is zero.
      ** So we might as well use bogus content rather than reading
      ** content from disk. */
      static u8 aZero[8];  /* This is the bogus content */
      sqlite3VdbeSerialGet(aZero, t, pDest);
    }else{
      rc = sqlite3VdbeMemFromBtree(pCrsr, aOffset[p2], len, !pC->isTable,
                                   pDest);
      if( rc!=SQLITE_OK ) goto abort_due_to_error;
      sqlite3VdbeSerialGet((const u8*)pDest->z, t, pDest);
      pDest->flags &= ~MEM_Ephem;
    }
  }

op_column_out:
  UPDATE_MAX_BLOBSIZE(pDest);
  REGISTER_TRACE(pOp->p3, pDest);
  break;
}

/* Opcode: Affinity P1 P2 * P4 *
** Synopsis: affinity(r[P1@P2])
**
** Apply affinities to a range of P2 registers starting with P1.
**
** P4 is a string that is P2 characters long. The nth character of the
** string indicates the column affinity that should be used for the nth
** memory cell in the range.
*/
case OP_Affinity: {
  const char *zAffinity;   /* The affinity to be applied */
  char cAff;               /* A single character of affinity */

  zAffinity = pOp->p4.z;
  assert( zAffinity!=0 );
  assert( zAffinity[pOp->p2]==0 );
  pIn1 = &aMem[pOp->p1];
  while( (cAff = *(zAffinity++))!=0 ){
    assert( pIn1 <= &p->aMem[(p->nMem+1 - p->nCursor)] );
    assert( memIsValid(pIn1) );
    applyAffinity(pIn1, cAff, encoding);
    pIn1++;
  }
  break;
}

/* Opcode: MakeRecord P1 P2 P3 P4 *
** Synopsis: r[P3]=mkrec(r[P1@P2])
**
** Convert P2 registers beginning with P1 into the [record format]
** use as a data record in a database table or as a key
** in an index.  The OP_Column opcode can decode the record later.
**
** P4 may be a string that is P2 characters long.  The nth character of the
** string indicates the column affinity that should be used for the nth
** field of the index key.
**
** The mapping from character to affinity is given by the SQLITE_AFF_
** macros defined in sqliteInt.h.
**
** If P4 is NULL then all index fields have the affinity BLOB.
*/
case OP_MakeRecord: {
  u8 *zNewRecord;        /* A buffer to hold the data for the new record */
  Mem *pRec;             /* The new record */
  u64 nData;             /* Number of bytes of data space */
  int nHdr;              /* Number of bytes of header space */
  i64 nByte;             /* Data space required for this record */
  i64 nZero;             /* Number of zero bytes at the end of the record */
  int nVarint;           /* Number of bytes in a varint */
  u32 serial_type;       /* Type field */
  Mem *pData0;           /* First field to be combined into the record */
  Mem *pLast;            /* Last field of the record */
  int nField;            /* Number of fields in the record */
  char *zAffinity;       /* The affinity string for the record */
  int file_format;       /* File format to use for encoding */
  int i;                 /* Space used in zNewRecord[] header */
  int j;                 /* Space used in zNewRecord[] content */
  u32 len;               /* Length of a field */

  /* Assuming the record contains N fields, the record format looks
  ** like this:
  **
  ** ------------------------------------------------------------------------
  ** | hdr-size | type 0 | type 1 | ... | type N-1 | data0 | ... | data N-1 | 
  ** ------------------------------------------------------------------------
  **
  ** Data(0) is taken from register P1.  Data(1) comes from register P1+1
  ** and so forth.
  **
  ** Each type field is a varint representing the serial type of the 
  ** corresponding data element (see sqlite3VdbeSerialType()). The
  ** hdr-size field is also a varint which is the offset from the beginning
  ** of the record to data0.
  */
  nData = 0;         /* Number of bytes of data space */
  nHdr = 0;          /* Number of bytes of header space */
  nZero = 0;         /* Number of zero bytes at the end of the record */
  nField = pOp->p1;
  zAffinity = pOp->p4.z;
  assert( nField>0 && pOp->p2>0 && pOp->p2+nField<=(p->nMem+1 - p->nCursor)+1 );
  pData0 = &aMem[nField];
  nField = pOp->p2;
  pLast = &pData0[nField-1];
  file_format = p->minWriteFileFormat;

  /* Identify the output register */
  assert( pOp->p3<pOp->p1 || pOp->p3>=pOp->p1+pOp->p2 );
  pOut = &aMem[pOp->p3];
  memAboutToChange(p, pOut);

  /* Apply the requested affinity to all inputs
  */
  assert( pData0<=pLast );
  if( zAffinity ){
    pRec = pData0;
    do{
      applyAffinity(pRec++, *(zAffinity++), encoding);
      assert( zAffinity[0]==0 || pRec<=pLast );
    }while( zAffinity[0] );
  }

  /* Loop through the elements that will make up the record to figure
  ** out how much space is required for the new record.
  */
  pRec = pLast;
  do{
    assert( memIsValid(pRec) );
    pRec->uTemp = serial_type = sqlite3VdbeSerialType(pRec, file_format, &len);
    if( pRec->flags & MEM_Zero ){
      if( nData ){
        if( sqlite3VdbeMemExpandBlob(pRec) ) goto no_mem;
      }else{
        nZero += pRec->u.nZero;
        len -= pRec->u.nZero;
      }
    }
    nData += len;
    testcase( serial_type==127 );
    testcase( serial_type==128 );
    nHdr += serial_type<=127 ? 1 : sqlite3VarintLen(serial_type);
    if( pRec==pData0 ) break;
    pRec--;
  }while(1);

  /* EVIDENCE-OF: R-22564-11647 The header begins with a single varint
  ** which determines the total number of bytes in the header. The varint
  ** value is the size of the header in bytes including the size varint
  ** itself. */
  testcase( nHdr==126 );
  testcase( nHdr==127 );
  if( nHdr<=126 ){
    /* The common case */
    nHdr += 1;
  }else{
    /* Rare case of a really large header */
    nVarint = sqlite3VarintLen(nHdr);
    nHdr += nVarint;
    if( nVarint<sqlite3VarintLen(nHdr) ) nHdr++;
  }
  nByte = nHdr+nData;
  if( nByte+nZero>db->aLimit[SQLITE_LIMIT_LENGTH] ){
    goto too_big;
  }

  /* Make sure the output register has a buffer large enough to store 
  ** the new record. The output register (pOp->p3) is not allowed to
  ** be one of the input registers (because the following call to
  ** sqlite3VdbeMemClearAndResize() could clobber the value before it is used).
  */
  if( sqlite3VdbeMemClearAndResize(pOut, (int)nByte) ){
    goto no_mem;
  }
  zNewRecord = (u8 *)pOut->z;

  /* Write the record */
  i = putVarint32(zNewRecord, nHdr);
  j = nHdr;
  assert( pData0<=pLast );
  pRec = pData0;
  do{
    serial_type = pRec->uTemp;
    /* EVIDENCE-OF: R-06529-47362 Following the size varint are one or more
    ** additional varints, one per column. */
    i += putVarint32(&zNewRecord[i], serial_type);            /* serial type */
    /* EVIDENCE-OF: R-64536-51728 The values for each column in the record
    ** immediately follow the header. */
    j += sqlite3VdbeSerialPut(&zNewRecord[j], pRec, serial_type); /* content */
  }while( (++pRec)<=pLast );
  assert( i==nHdr );
  assert( j==nByte );

  assert( pOp->p3>0 && pOp->p3<=(p->nMem+1 - p->nCursor) );
  pOut->n = (int)nByte;
  pOut->flags = MEM_Blob;
  if( nZero ){
    pOut->u.nZero = nZero;
    pOut->flags |= MEM_Zero;
  }
  pOut->enc = SQLITE_UTF8;  /* In case the blob is ever converted to text */
  REGISTER_TRACE(pOp->p3, pOut);
  UPDATE_MAX_BLOBSIZE(pOut);
  break;
}

/* Opcode: Count P1 P2 * * *
** Synopsis: r[P2]=count()
**
** Store the number of entries (an integer value) in the table or index 
** opened by cursor P1 in register P2
*/
#ifndef SQLITE_OMIT_BTREECOUNT
case OP_Count: {         /* out2 */
  i64 nEntry;
  BtCursor *pCrsr;

  assert( p->apCsr[pOp->p1]->eCurType==CURTYPE_BTREE );
  pCrsr = p->apCsr[pOp->p1]->uc.pCursor;
  assert( pCrsr );
  nEntry = 0;  /* Not needed.  Only used to silence a warning. */
  rc = sqlite3BtreeCount(pCrsr, &nEntry);
  if( rc ) goto abort_due_to_error;
  pOut = out2Prerelease(p, pOp);
  pOut->u.i = nEntry;
  break;
}
#endif

/* Opcode: Savepoint P1 * * P4 *
**
** Open, release or rollback the savepoint named by parameter P4, depending
** on the value of P1. To open a new savepoint, P1==0. To release (commit) an
** existing savepoint, P1==1, or to rollback an existing savepoint P1==2.
*/
case OP_Savepoint: {
  int p1;                         /* Value of P1 operand */
  char *zName;                    /* Name of savepoint */
  int nName;
  Savepoint *pNew;
  Savepoint *pSavepoint;
  Savepoint *pTmp;
  int iSavepoint;
  int ii;

  p1 = pOp->p1;
  zName = pOp->p4.z;

  /* Assert that the p1 parameter is valid. Also that if there is no open
  ** transaction, then there cannot be any savepoints. 
  */
  assert( db->pSavepoint==0 || db->autoCommit==0 );
  assert( p1==SAVEPOINT_BEGIN||p1==SAVEPOINT_RELEASE||p1==SAVEPOINT_ROLLBACK );
  assert( db->pSavepoint || db->isTransactionSavepoint==0 );
  assert( checkSavepointCount(db) );
  assert( p->bIsReader );

  if( p1==SAVEPOINT_BEGIN ){
    if( db->nVdbeWrite>0 ){
      /* A new savepoint cannot be created if there are active write 
      ** statements (i.e. open read/write incremental blob handles).
      */
      sqlite3VdbeError(p, "cannot open savepoint - SQL statements in progress");
      rc = SQLITE_BUSY;
    }else{
      nName = sqlite3Strlen30(zName);

#ifndef SQLITE_OMIT_VIRTUALTABLE
      /* This call is Ok even if this savepoint is actually a transaction
      ** savepoint (and therefore should not prompt xSavepoint()) callbacks.
      ** If this is a transaction savepoint being opened, it is guaranteed
      ** that the db->aVTrans[] array is empty.  */
      assert( db->autoCommit==0 || db->nVTrans==0 );
      rc = sqlite3VtabSavepoint(db, SAVEPOINT_BEGIN,
                                db->nStatement+db->nSavepoint);
      if( rc!=SQLITE_OK ) goto abort_due_to_error;
#endif

      /* Create a new savepoint structure. */
      pNew = sqlite3DbMallocRawNN(db, sizeof(Savepoint)+nName+1);
      if( pNew ){
        pNew->zName = (char *)&pNew[1];
        memcpy(pNew->zName, zName, nName+1);
    
        /* If there is no open transaction, then mark this as a special
        ** "transaction savepoint". */
        if( db->autoCommit ){
          db->autoCommit = 0;
          db->isTransactionSavepoint = 1;
        }else{
          db->nSavepoint++;
        }

        /* Link the new savepoint into the database handle's list. */
        pNew->pNext = db->pSavepoint;
        db->pSavepoint = pNew;
        pNew->nDeferredCons = db->nDeferredCons;
        pNew->nDeferredImmCons = db->nDeferredImmCons;
      }
    }
  }else{
    iSavepoint = 0;

    /* Find the named savepoint. If there is no such savepoint, then an
    ** an error is returned to the user.  */
    for(
      pSavepoint = db->pSavepoint; 
      pSavepoint && sqlite3StrICmp(pSavepoint->zName, zName);
      pSavepoint = pSavepoint->pNext
    ){
      iSavepoint++;
    }
    if( !pSavepoint ){
      sqlite3VdbeError(p, "no such savepoint: %s", zName);
      rc = SQLITE_ERROR;
    }else if( db->nVdbeWrite>0 && p1==SAVEPOINT_RELEASE ){
      /* It is not possible to release (commit) a savepoint if there are 
      ** active write statements.
      */
      sqlite3VdbeError(p, "cannot release savepoint - "
                          "SQL statements in progress");
      rc = SQLITE_BUSY;
    }else{

      /* Determine whether or not this is a transaction savepoint. If so,
      ** and this is a RELEASE command, then the current transaction 
      ** is committed. 
      */
      int isTransaction = pSavepoint->pNext==0 && db->isTransactionSavepoint;
      if( isTransaction && p1==SAVEPOINT_RELEASE ){
        if( (rc = sqlite3VdbeCheckFk(p, 1))!=SQLITE_OK ){
          goto vdbe_return;
        }
        db->autoCommit = 1;
        if( sqlite3VdbeHalt(p)==SQLITE_BUSY ){
          p->pc = (int)(pOp - aOp);
          db->autoCommit = 0;
          p->rc = rc = SQLITE_BUSY;
          goto vdbe_return;
        }
        db->isTransactionSavepoint = 0;
        rc = p->rc;
      }else{
        int isSchemaChange;
        iSavepoint = db->nSavepoint - iSavepoint - 1;
        if( p1==SAVEPOINT_ROLLBACK ){
          isSchemaChange = (db->flags & SQLITE_InternChanges)!=0;
          for(ii=0; ii<db->nDb; ii++){
            rc = sqlite3BtreeTripAllCursors(db->aDb[ii].pBt,
                                       SQLITE_ABORT_ROLLBACK,
                                       isSchemaChange==0);
            if( rc!=SQLITE_OK ) goto abort_due_to_error;
          }
        }else{
          isSchemaChange = 0;
        }
        for(ii=0; ii<db->nDb; ii++){
          rc = sqlite3BtreeSavepoint(db->aDb[ii].pBt, p1, iSavepoint);
          if( rc!=SQLITE_OK ){
            goto abort_due_to_error;
          }
        }
        if( isSchemaChange ){
          sqlite3ExpirePreparedStatements(db);
          sqlite3ResetAllSchemasOfConnection(db);
          db->flags = (db->flags | SQLITE_InternChanges);
        }
      }
  
      /* Regardless of whether this is a RELEASE or ROLLBACK, destroy all 
      ** savepoints nested inside of the savepoint being operated on. */
      while( db->pSavepoint!=pSavepoint ){
        pTmp = db->pSavepoint;
        db->pSavepoint = pTmp->pNext;
        sqlite3DbFree(db, pTmp);
        db->nSavepoint--;
      }

      /* If it is a RELEASE, then destroy the savepoint being operated on 
      ** too. If it is a ROLLBACK TO, then set the number of deferred 
      ** constraint violations present in the database to the value stored
      ** when the savepoint was created.  */
      if( p1==SAVEPOINT_RELEASE ){
        assert( pSavepoint==db->pSavepoint );
        db->pSavepoint = pSavepoint->pNext;
        sqlite3DbFree(db, pSavepoint);
        if( !isTransaction ){
          db->nSavepoint--;
        }
      }else{
        db->nDeferredCons = pSavepoint->nDeferredCons;
        db->nDeferredImmCons = pSavepoint->nDeferredImmCons;
      }

      if( !isTransaction || p1==SAVEPOINT_ROLLBACK ){
        rc = sqlite3VtabSavepoint(db, p1, iSavepoint);
        if( rc!=SQLITE_OK ) goto abort_due_to_error;
      }
    }
  }
  if( rc ) goto abort_due_to_error;

  break;
}

/* Opcode: AutoCommit P1 P2 * * *
**
** Set the database auto-commit flag to P1 (1 or 0). If P2 is true, roll
** back any currently active btree transactions. If there are any active
** VMs (apart from this one), then a ROLLBACK fails.  A COMMIT fails if
** there are active writing VMs or active VMs that use shared cache.
**
** This instruction causes the VM to halt.
*/
case OP_AutoCommit: {
  int desiredAutoCommit;
  int iRollback;

  desiredAutoCommit = pOp->p1;
  iRollback = pOp->p2;
  assert( desiredAutoCommit==1 || desiredAutoCommit==0 );
  assert( desiredAutoCommit==1 || iRollback==0 );
  assert( db->nVdbeActive>0 );  /* At least this one VM is active */
  assert( p->bIsReader );

  if( desiredAutoCommit!=db->autoCommit ){
    if( iRollback ){
      assert( desiredAutoCommit==1 );
      sqlite3RollbackAll(db, SQLITE_ABORT_ROLLBACK);
      db->autoCommit = 1;
    }else if( desiredAutoCommit && db->nVdbeWrite>0 ){
      /* If this instruction implements a COMMIT and other VMs are writing
      ** return an error indicating that the other VMs must complete first. 
      */
      sqlite3VdbeError(p, "cannot commit transaction - "
                          "SQL statements in progress");
      rc = SQLITE_BUSY;
      goto abort_due_to_error;
    }else if( (rc = sqlite3VdbeCheckFk(p, 1))!=SQLITE_OK ){
      goto vdbe_return;
    }else{
      db->autoCommit = (u8)desiredAutoCommit;
    }
    if( sqlite3VdbeHalt(p)==SQLITE_BUSY ){
      p->pc = (int)(pOp - aOp);
      db->autoCommit = (u8)(1-desiredAutoCommit);
      p->rc = rc = SQLITE_BUSY;
      goto vdbe_return;
    }
    assert( db->nStatement==0 );
    sqlite3CloseSavepoints(db);
    if( p->rc==SQLITE_OK ){
      rc = SQLITE_DONE;
    }else{
      rc = SQLITE_ERROR;
    }
    goto vdbe_return;
  }else{
    sqlite3VdbeError(p,
        (!desiredAutoCommit)?"cannot start a transaction within a transaction":(
        (iRollback)?"cannot rollback - no transaction is active":
                   "cannot commit - no transaction is active"));
         
    rc = SQLITE_ERROR;
    goto abort_due_to_error;
  }
  break;
}

/* Opcode: Transaction P1 P2 P3 P4 P5
**
** Begin a transaction on database P1 if a transaction is not already
** active.
** If P2 is non-zero, then a write-transaction is started, or if a 
** read-transaction is already active, it is upgraded to a write-transaction.
** If P2 is zero, then a read-transaction is started.
**
** P1 is the index of the database file on which the transaction is
** started.  Index 0 is the main database file and index 1 is the
** file used for temporary tables.  Indices of 2 or more are used for
** attached databases.
**
** If a write-transaction is started and the Vdbe.usesStmtJournal flag is
** true (this flag is set if the Vdbe may modify more than one row and may
** throw an ABORT exception), a statement transaction may also be opened.
** More specifically, a statement transaction is opened iff the database
** connection is currently not in autocommit mode, or if there are other
** active statements. A statement transaction allows the changes made by this
** VDBE to be rolled back after an error without having to roll back the
** entire transaction. If no error is encountered, the statement transaction
** will automatically commit when the VDBE halts.
**
** If P5!=0 then this opcode also checks the schema cookie against P3
** and the schema generation counter against P4.
** The cookie changes its value whenever the database schema changes.
** This operation is used to detect when that the cookie has changed
** and that the current process needs to reread the schema.  If the schema
** cookie in P3 differs from the schema cookie in the database header or
** if the schema generation counter in P4 differs from the current
** generation counter, then an SQLITE_SCHEMA error is raised and execution
** halts.  The sqlite3_step() wrapper function might then reprepare the
** statement and rerun it from the beginning.
*/
case OP_Transaction: {
  Btree *pBt;
  int iMeta;
  int iGen;

  assert( p->bIsReader );
  assert( p->readOnly==0 || pOp->p2==0 );
  assert( pOp->p1>=0 && pOp->p1<db->nDb );
  assert( DbMaskTest(p->btreeMask, pOp->p1) );
  if( pOp->p2 && (db->flags & SQLITE_QueryOnly)!=0 ){
    rc = SQLITE_READONLY;
    goto abort_due_to_error;
  }
  pBt = db->aDb[pOp->p1].pBt;

  if( pBt ){
    rc = sqlite3BtreeBeginTrans(pBt, pOp->p2);
    testcase( rc==SQLITE_BUSY_SNAPSHOT );
    testcase( rc==SQLITE_BUSY_RECOVERY );
    if( rc!=SQLITE_OK ){
      if( (rc&0xff)==SQLITE_BUSY ){
        p->pc = (int)(pOp - aOp);
        p->rc = rc;
        goto vdbe_return;
      }
      goto abort_due_to_error;
    }

    if( pOp->p2 && p->usesStmtJournal 
     && (db->autoCommit==0 || db->nVdbeRead>1) 
    ){
      assert( sqlite3BtreeIsInTrans(pBt) );
      if( p->iStatement==0 ){
        assert( db->nStatement>=0 && db->nSavepoint>=0 );
        db->nStatement++; 
        p->iStatement = db->nSavepoint + db->nStatement;
      }

      rc = sqlite3VtabSavepoint(db, SAVEPOINT_BEGIN, p->iStatement-1);
      if( rc==SQLITE_OK ){
        rc = sqlite3BtreeBeginStmt(pBt, p->iStatement);
      }

      /* Store the current value of the database handles deferred constraint
      ** counter. If the statement transaction needs to be rolled back,
      ** the value of this counter needs to be restored too.  */
      p->nStmtDefCons = db->nDeferredCons;
      p->nStmtDefImmCons = db->nDeferredImmCons;
    }

    /* Gather the schema version number for checking:
    ** IMPLEMENTATION-OF: R-03189-51135 As each SQL statement runs, the schema
    ** version is checked to ensure that the schema has not changed since the
    ** SQL statement was prepared.
    */
    sqlite3BtreeGetMeta(pBt, BTREE_SCHEMA_VERSION, (u32 *)&iMeta);
    iGen = db->aDb[pOp->p1].pSchema->iGeneration;
  }else{
    iGen = iMeta = 0;
  }
  assert( pOp->p5==0 || pOp->p4type==P4_INT32 );
  if( pOp->p5 && (iMeta!=pOp->p3 || iGen!=pOp->p4.i) ){
    sqlite3DbFree(db, p->zErrMsg);
    p->zErrMsg = sqlite3DbStrDup(db, "database schema has changed");
    /* If the schema-cookie from the database file matches the cookie 
    ** stored with the in-memory representation of the schema, do
    ** not reload the schema from the database file.
    **
    ** If virtual-tables are in use, this is not just an optimization.
    ** Often, v-tables store their data in other SQLite tables, which
    ** are queried from within xNext() and other v-table methods using
    ** prepared queries. If such a query is out-of-date, we do not want to
    ** discard the database schema, as the user code implementing the
    ** v-table would have to be ready for the sqlite3_vtab structure itself
    ** to be invalidated whenever sqlite3_step() is called from within 
    ** a v-table method.
    */
    if( db->aDb[pOp->p1].pSchema->schema_cookie!=iMeta ){
      sqlite3ResetOneSchema(db, pOp->p1);
    }
    p->expired = 1;
    rc = SQLITE_SCHEMA;
  }
  if( rc ) goto abort_due_to_error;
  break;
}

/* Opcode: ReadCookie P1 P2 P3 * *
**
** Read cookie number P3 from database P1 and write it into register P2.
** P3==1 is the schema version.  P3==2 is the database format.
** P3==3 is the recommended pager cache size, and so forth.  P1==0 is
** the main database file and P1==1 is the database file used to store
** temporary tables.
**
** There must be a read-lock on the database (either a transaction
** must be started or there must be an open cursor) before
** executing this instruction.
*/
case OP_ReadCookie: {               /* out2 */
  int iMeta;
  int iDb;
  int iCookie;

  assert( p->bIsReader );
  iDb = pOp->p1;
  iCookie = pOp->p3;
  assert( pOp->p3<SQLITE_N_BTREE_META );
  assert( iDb>=0 && iDb<db->nDb );
  assert( db->aDb[iDb].pBt!=0 );
  assert( DbMaskTest(p->btreeMask, iDb) );

  sqlite3BtreeGetMeta(db->aDb[iDb].pBt, iCookie, (u32 *)&iMeta);
  pOut = out2Prerelease(p, pOp);
  pOut->u.i = iMeta;
  break;
}

/* Opcode: SetCookie P1 P2 P3 * *
**
** Write the integer value P3 into cookie number P2 of database P1.
** P2==1 is the schema version.  P2==2 is the database format.
** P2==3 is the recommended pager cache 
** size, and so forth.  P1==0 is the main database file and P1==1 is the 
** database file used to store temporary tables.
**
** A transaction must be started before executing this opcode.
*/
case OP_SetCookie: {
  Db *pDb;
  assert( pOp->p2<SQLITE_N_BTREE_META );
  assert( pOp->p1>=0 && pOp->p1<db->nDb );
  assert( DbMaskTest(p->btreeMask, pOp->p1) );
  assert( p->readOnly==0 );
  pDb = &db->aDb[pOp->p1];
  assert( pDb->pBt!=0 );
  assert( sqlite3SchemaMutexHeld(db, pOp->p1, 0) );
  /* See note about index shifting on OP_ReadCookie */
  rc = sqlite3BtreeUpdateMeta(pDb->pBt, pOp->p2, pOp->p3);
  if( pOp->p2==BTREE_SCHEMA_VERSION ){
    /* When the schema cookie changes, record the new cookie internally */
    pDb->pSchema->schema_cookie = pOp->p3;
    db->flags |= SQLITE_InternChanges;
  }else if( pOp->p2==BTREE_FILE_FORMAT ){
    /* Record changes in the file format */
    pDb->pSchema->file_format = pOp->p3;
  }
  if( pOp->p1==1 ){
    /* Invalidate all prepared statements whenever the TEMP database
    ** schema is changed.  Ticket #1644 */
    sqlite3ExpirePreparedStatements(db);
    p->expired = 0;
  }
  if( rc ) goto abort_due_to_error;
  break;
}

/* Opcode: OpenRead P1 P2 P3 P4 P5
** Synopsis: root=P2 iDb=P3
**
** Open a read-only cursor for the database table whose root page is
** P2 in a database file.  The database file is determined by P3. 
** P3==0 means the main database, P3==1 means the database used for 
** temporary tables, and P3>1 means used the corresponding attached
** database.  Give the new cursor an identifier of P1.  The P1
** values need not be contiguous but all P1 values should be small integers.
** It is an error for P1 to be negative.
**
** If P5!=0 then use the content of register P2 as the root page, not
** the value of P2 itself.
**
** There will be a read lock on the database whenever there is an
** open cursor.  If the database was unlocked prior to this instruction
** then a read lock is acquired as part of this instruction.  A read
** lock allows other processes to read the database but prohibits
** any other process from modifying the database.  The read lock is
** released when all cursors are closed.  If this instruction attempts
** to get a read lock but fails, the script terminates with an
** SQLITE_BUSY error code.
**
** The P4 value may be either an integer (P4_INT32) or a pointer to
** a KeyInfo structure (P4_KEYINFO). If it is a pointer to a KeyInfo 
** structure, then said structure defines the content and collating 
** sequence of the index being opened. Otherwise, if P4 is an integer 
** value, it is set to the number of columns in the table.
**
** See also: OpenWrite, ReopenIdx
*/
/* Opcode: ReopenIdx P1 P2 P3 P4 P5
** Synopsis: root=P2 iDb=P3
**
** The ReopenIdx opcode works exactly like ReadOpen except that it first
** checks to see if the cursor on P1 is already open with a root page
** number of P2 and if it is this opcode becomes a no-op.  In other words,
** if the cursor is already open, do not reopen it.
**
** The ReopenIdx opcode may only be used with P5==0 and with P4 being
** a P4_KEYINFO object.  Furthermore, the P3 value must be the same as
** every other ReopenIdx or OpenRead for the same cursor number.
**
** See the OpenRead opcode documentation for additional information.
*/
/* Opcode: OpenWrite P1 P2 P3 P4 P5
** Synopsis: root=P2 iDb=P3
**
** Open a read/write cursor named P1 on the table or index whose root
** page is P2.  Or if P5!=0 use the content of register P2 to find the
** root page.
**
** The P4 value may be either an integer (P4_INT32) or a pointer to
** a KeyInfo structure (P4_KEYINFO). If it is a pointer to a KeyInfo 
** structure, then said structure defines the content and collating 
** sequence of the index being opened. Otherwise, if P4 is an integer 
** value, it is set to the number of columns in the table, or to the
** largest index of any column of the table that is actually used.
**
** This instruction works just like OpenRead except that it opens the cursor
** in read/write mode.  For a given table, there can be one or more read-only
** cursors or a single read/write cursor but not both.
**
** See also OpenRead.
*/
case OP_ReopenIdx: {
  int nField;
  KeyInfo *pKeyInfo;
  int p2;
  int iDb;
  int wrFlag;
  Btree *pX;
  VdbeCursor *pCur;
  Db *pDb;

  assert( pOp->p5==0 || pOp->p5==OPFLAG_SEEKEQ );
  assert( pOp->p4type==P4_KEYINFO );
  pCur = p->apCsr[pOp->p1];
  if( pCur && pCur->pgnoRoot==(u32)pOp->p2 ){
    assert( pCur->iDb==pOp->p3 );      /* Guaranteed by the code generator */
    goto open_cursor_set_hints;
  }
  /* If the cursor is not currently open or is open on a different
  ** index, then fall through into OP_OpenRead to force a reopen */
case OP_OpenRead:
case OP_OpenWrite:

  assert( pOp->opcode==OP_OpenWrite || pOp->p5==0 || pOp->p5==OPFLAG_SEEKEQ );
  assert( p->bIsReader );
  assert( pOp->opcode==OP_OpenRead || pOp->opcode==OP_ReopenIdx
          || p->readOnly==0 );

  if( p->expired ){
    rc = SQLITE_ABORT_ROLLBACK;
    goto abort_due_to_error;
  }

  nField = 0;
  pKeyInfo = 0;
  p2 = pOp->p2;
  iDb = pOp->p3;
  assert( iDb>=0 && iDb<db->nDb );
  assert( DbMaskTest(p->btreeMask, iDb) );
  pDb = &db->aDb[iDb];
  pX = pDb->pBt;
  assert( pX!=0 );
  if( pOp->opcode==OP_OpenWrite ){
    assert( OPFLAG_FORDELETE==BTREE_FORDELETE );
    wrFlag = BTREE_WRCSR | (pOp->p5 & OPFLAG_FORDELETE);
    assert( sqlite3SchemaMutexHeld(db, iDb, 0) );
    if( pDb->pSchema->file_format < p->minWriteFileFormat ){
      p->minWriteFileFormat = pDb->pSchema->file_format;
    }
  }else{
    wrFlag = 0;
  }
  if( pOp->p5 & OPFLAG_P2ISREG ){
    assert( p2>0 );
    assert( p2<=(p->nMem+1 - p->nCursor) );
    pIn2 = &aMem[p2];
    assert( memIsValid(pIn2) );
    assert( (pIn2->flags & MEM_Int)!=0 );
    sqlite3VdbeMemIntegerify(pIn2);
    p2 = (int)pIn2->u.i;
    /* The p2 value always comes from a prior OP_CreateTable opcode and
    ** that opcode will always set the p2 value to 2 or more or else fail.
    ** If there were a failure, the prepared statement would have halted
    ** before reaching this instruction. */
    assert( p2>=2 );
  }
  if( pOp->p4type==P4_KEYINFO ){
    pKeyInfo = pOp->p4.pKeyInfo;
    assert( pKeyInfo->enc==ENC(db) );
    assert( pKeyInfo->db==db );
    nField = pKeyInfo->nField+pKeyInfo->nXField;
  }else if( pOp->p4type==P4_INT32 ){
    nField = pOp->p4.i;
  }
  assert( pOp->p1>=0 );
  assert( nField>=0 );
  testcase( nField==0 );  /* Table with INTEGER PRIMARY KEY and nothing else */
  pCur = allocateCursor(p, pOp->p1, nField, iDb, CURTYPE_BTREE);
  if( pCur==0 ) goto no_mem;
  pCur->nullRow = 1;
  pCur->isOrdered = 1;
  pCur->pgnoRoot = p2;
#ifdef SQLITE_DEBUG
  pCur->wrFlag = wrFlag;
#endif
  rc = sqlite3BtreeCursor(pX, p2, wrFlag, pKeyInfo, pCur->uc.pCursor);
  pCur->pKeyInfo = pKeyInfo;
  /* Set the VdbeCursor.isTable variable. Previous versions of
  ** SQLite used to check if the root-page flags were sane at this point
  ** and report database corruption if they were not, but this check has
  ** since moved into the btree layer.  */  
  pCur->isTable = pOp->p4type!=P4_KEYINFO;

open_cursor_set_hints:
  assert( OPFLAG_BULKCSR==BTREE_BULKLOAD );
  assert( OPFLAG_SEEKEQ==BTREE_SEEK_EQ );
  testcase( pOp->p5 & OPFLAG_BULKCSR );
#ifdef SQLITE_ENABLE_CURSOR_HINTS
  testcase( pOp->p2 & OPFLAG_SEEKEQ );
#endif
  sqlite3BtreeCursorHintFlags(pCur->uc.pCursor,
                               (pOp->p5 & (OPFLAG_BULKCSR|OPFLAG_SEEKEQ)));
  if( rc ) goto abort_due_to_error;
  break;
}

/* Opcode: OpenEphemeral P1 P2 * P4 P5
** Synopsis: nColumn=P2
**
** Open a new cursor P1 to a transient table.
** The cursor is always opened read/write even if 
** the main database is read-only.  The ephemeral
** table is deleted automatically when the cursor is closed.
**
** P2 is the number of columns in the ephemeral table.
** The cursor points to a BTree table if P4==0 and to a BTree index
** if P4 is not 0.  If P4 is not NULL, it points to a KeyInfo structure
** that defines the format of keys in the index.
**
** The P5 parameter can be a mask of the BTREE_* flags defined
** in btree.h.  These flags control aspects of the operation of
** the btree.  The BTREE_OMIT_JOURNAL and BTREE_SINGLE flags are
** added automatically.
*/
/* Opcode: OpenAutoindex P1 P2 * P4 *
** Synopsis: nColumn=P2
**
** This opcode works the same as OP_OpenEphemeral.  It has a
** different name to distinguish its use.  Tables created using
** by this opcode will be used for automatically created transient
** indices in joins.
*/
case OP_OpenAutoindex: 
case OP_OpenEphemeral: {
  VdbeCursor *pCx;
  KeyInfo *pKeyInfo;

  static const int vfsFlags = 
      SQLITE_OPEN_READWRITE |
      SQLITE_OPEN_CREATE |
      SQLITE_OPEN_EXCLUSIVE |
      SQLITE_OPEN_DELETEONCLOSE |
      SQLITE_OPEN_TRANSIENT_DB;
  assert( pOp->p1>=0 );
  assert( pOp->p2>=0 );
  pCx = allocateCursor(p, pOp->p1, pOp->p2, -1, CURTYPE_BTREE);
  if( pCx==0 ) goto no_mem;
  pCx->nullRow = 1;
  pCx->isEphemeral = 1;
  rc = sqlite3BtreeOpen(db->pVfs, 0, db, &pCx->pBt, 
                        BTREE_OMIT_JOURNAL | BTREE_SINGLE | pOp->p5, vfsFlags);
  if( rc==SQLITE_OK ){
    rc = sqlite3BtreeBeginTrans(pCx->pBt, 1);
  }
  if( rc==SQLITE_OK ){
    /* If a transient index is required, create it by calling
    ** sqlite3BtreeCreateTable() with the BTREE_BLOBKEY flag before
    ** opening it. If a transient table is required, just use the
    ** automatically created table with root-page 1 (an BLOB_INTKEY table).
    */
    if( (pKeyInfo = pOp->p4.pKeyInfo)!=0 ){
      int pgno;
      assert( pOp->p4type==P4_KEYINFO );
      rc = sqlite3BtreeCreateTable(pCx->pBt, &pgno, BTREE_BLOBKEY | pOp->p5); 
      if( rc==SQLITE_OK ){
        assert( pgno==MASTER_ROOT+1 );
        assert( pKeyInfo->db==db );
        assert( pKeyInfo->enc==ENC(db) );
        pCx->pKeyInfo = pKeyInfo;
        rc = sqlite3BtreeCursor(pCx->pBt, pgno, BTREE_WRCSR,
                                pKeyInfo, pCx->uc.pCursor);
      }
      pCx->isTable = 0;
    }else{
      rc = sqlite3BtreeCursor(pCx->pBt, MASTER_ROOT, BTREE_WRCSR,
                              0, pCx->uc.pCursor);
      pCx->isTable = 1;
    }
  }
  if( rc ) goto abort_due_to_error;
  pCx->isOrdered = (pOp->p5!=BTREE_UNORDERED);
  break;
}

/* Opcode: SorterOpen P1 P2 P3 P4 *
**
** This opcode works like OP_OpenEphemeral except that it opens
** a transient index that is specifically designed to sort large
** tables using an external merge-sort algorithm.
**
** If argument P3 is non-zero, then it indicates that the sorter may
** assume that a stable sort considering the first P3 fields of each
** key is sufficient to produce the required results.
*/
case OP_SorterOpen: {
  VdbeCursor *pCx;

  assert( pOp->p1>=0 );
  assert( pOp->p2>=0 );
  pCx = allocateCursor(p, pOp->p1, pOp->p2, -1, CURTYPE_SORTER);
  if( pCx==0 ) goto no_mem;
  pCx->pKeyInfo = pOp->p4.pKeyInfo;
  assert( pCx->pKeyInfo->db==db );
  assert( pCx->pKeyInfo->enc==ENC(db) );
  rc = sqlite3VdbeSorterInit(db, pOp->p3, pCx);
  if( rc ) goto abort_due_to_error;
  break;
}

/* Opcode: SequenceTest P1 P2 * * *
** Synopsis: if( cursor[P1].ctr++ ) pc = P2
**
** P1 is a sorter cursor. If the sequence counter is currently zero, jump
** to P2. Regardless of whether or not the jump is taken, increment the
** the sequence value.
*/
case OP_SequenceTest: {
  VdbeCursor *pC;
  assert( pOp->p1>=0 && pOp->p1<p->nCursor );
  pC = p->apCsr[pOp->p1];
  assert( isSorter(pC) );
  if( (pC->seqCount++)==0 ){
    goto jump_to_p2;
  }
  break;
}

/* Opcode: OpenPseudo P1 P2 P3 * *
** Synopsis: P3 columns in r[P2]
**
** Open a new cursor that points to a fake table that contains a single
** row of data.  The content of that one row is the content of memory
** register P2.  In other words, cursor P1 becomes an alias for the 
** MEM_Blob content contained in register P2.
**
** A pseudo-table created by this opcode is used to hold a single
** row output from the sorter so that the row can be decomposed into
** individual columns using the OP_Column opcode.  The OP_Column opcode
** is the only cursor opcode that works with a pseudo-table.
**
** P3 is the number of fields in the records that will be stored by
** the pseudo-table.
*/
case OP_OpenPseudo: {
  VdbeCursor *pCx;

  assert( pOp->p1>=0 );
  assert( pOp->p3>=0 );
  pCx = allocateCursor(p, pOp->p1, pOp->p3, -1, CURTYPE_PSEUDO);
  if( pCx==0 ) goto no_mem;
  pCx->nullRow = 1;
  pCx->uc.pseudoTableReg = pOp->p2;
  pCx->isTable = 1;
  assert( pOp->p5==0 );
  break;
}

/* Opcode: Close P1 * * * *
**
** Close a cursor previously opened as P1.  If P1 is not
** currently open, this instruction is a no-op.
*/
case OP_Close: {
  assert( pOp->p1>=0 && pOp->p1<p->nCursor );
  sqlite3VdbeFreeCursor(p, p->apCsr[pOp->p1]);
  p->apCsr[pOp->p1] = 0;
  break;
}

#ifdef SQLITE_ENABLE_COLUMN_USED_MASK
/* Opcode: ColumnsUsed P1 * * P4 *
**
** This opcode (which only exists if SQLite was compiled with
** SQLITE_ENABLE_COLUMN_USED_MASK) identifies which columns of the
** table or index for cursor P1 are used.  P4 is a 64-bit integer
** (P4_INT64) in which the first 63 bits are one for each of the
** first 63 columns of the table or index that are actually used
** by the cursor.  The high-order bit is set if any column after
** the 64th is used.
*/
case OP_ColumnsUsed: {
  VdbeCursor *pC;
  pC = p->apCsr[pOp->p1];
  assert( pC->eCurType==CURTYPE_BTREE );
  pC->maskUsed = *(u64*)pOp->p4.pI64;
  break;
}
#endif

/* Opcode: SeekGE P1 P2 P3 P4 *
** Synopsis: key=r[P3@P4]
**
** If cursor P1 refers to an SQL table (B-Tree that uses integer keys), 
** use the value in register P3 as the key.  If cursor P1 refers 
** to an SQL index, then P3 is the first in an array of P4 registers 
** that are used as an unpacked index key. 
**
** Reposition cursor P1 so that  it points to the smallest entry that 
** is greater than or equal to the key value. If there are no records 
** greater than or equal to the key and P2 is not zero, then jump to P2.
**
** If the cursor P1 was opened using the OPFLAG_SEEKEQ flag, then this
** opcode will always land on a record that equally equals the key, or
** else jump immediately to P2.  When the cursor is OPFLAG_SEEKEQ, this
** opcode must be followed by an IdxLE opcode with the same arguments.
** The IdxLE opcode will be skipped if this opcode succeeds, but the
** IdxLE opcode will be used on subsequent loop iterations.
**
** This opcode leaves the cursor configured to move in forward order,
** from the beginning toward the end.  In other words, the cursor is
** configured to use Next, not Prev.
**
** See also: Found, NotFound, SeekLt, SeekGt, SeekLe
*/
/* Opcode: SeekGT P1 P2 P3 P4 *
** Synopsis: key=r[P3@P4]
**
** If cursor P1 refers to an SQL table (B-Tree that uses integer keys), 
** use the value in register P3 as a key. If cursor P1 refers 
** to an SQL index, then P3 is the first in an array of P4 registers 
** that are used as an unpacked index key. 
**
** Reposition cursor P1 so that  it points to the smallest entry that 
** is greater than the key value. If there are no records greater than 
** the key and P2 is not zero, then jump to P2.
**
** This opcode leaves the cursor configured to move in forward order,
** from the beginning toward the end.  In other words, the cursor is
** configured to use Next, not Prev.
**
** See also: Found, NotFound, SeekLt, SeekGe, SeekLe
*/
/* Opcode: SeekLT P1 P2 P3 P4 * 
** Synopsis: key=r[P3@P4]
**
** If cursor P1 refers to an SQL table (B-Tree that uses integer keys), 
** use the value in register P3 as a key. If cursor P1 refers 
** to an SQL index, then P3 is the first in an array of P4 registers 
** that are used as an unpacked index key. 
**
** Reposition cursor P1 so that  it points to the largest entry that 
** is less than the key value. If there are no records less than 
** the key and P2 is not zero, then jump to P2.
**
** This opcode leaves the cursor configured to move in reverse order,
** from the end toward the beginning.  In other words, the cursor is
** configured to use Prev, not Next.
**
** See also: Found, NotFound, SeekGt, SeekGe, SeekLe
*/
/* Opcode: SeekLE P1 P2 P3 P4 *
** Synopsis: key=r[P3@P4]
**
** If cursor P1 refers to an SQL table (B-Tree that uses integer keys), 
** use the value in register P3 as a key. If cursor P1 refers 
** to an SQL index, then P3 is the first in an array of P4 registers 
** that are used as an unpacked index key. 
**
** Reposition cursor P1 so that it points to the largest entry that 
** is less than or equal to the key value. If there are no records 
** less than or equal to the key and P2 is not zero, then jump to P2.
**
** This opcode leaves the cursor configured to move in reverse order,
** from the end toward the beginning.  In other words, the cursor is
** configured to use Prev, not Next.
**
** If the cursor P1 was opened using the OPFLAG_SEEKEQ flag, then this
** opcode will always land on a record that equally equals the key, or
** else jump immediately to P2.  When the cursor is OPFLAG_SEEKEQ, this
** opcode must be followed by an IdxGE opcode with the same arguments.
** The IdxGE opcode will be skipped if this opcode succeeds, but the
** IdxGE opcode will be used on subsequent loop iterations.
**
** See also: Found, NotFound, SeekGt, SeekGe, SeekLt
*/
case OP_SeekLT:         /* jump, in3 */
case OP_SeekLE:         /* jump, in3 */
case OP_SeekGE:         /* jump, in3 */
case OP_SeekGT: {       /* jump, in3 */
  int res;           /* Comparison result */
  int oc;            /* Opcode */
  VdbeCursor *pC;    /* The cursor to seek */
  UnpackedRecord r;  /* The key to seek for */
  int nField;        /* Number of columns or fields in the key */
  i64 iKey;          /* The rowid we are to seek to */
  int eqOnly;        /* Only interested in == results */

  assert( pOp->p1>=0 && pOp->p1<p->nCursor );
  assert( pOp->p2!=0 );
  pC = p->apCsr[pOp->p1];
  assert( pC!=0 );
  assert( pC->eCurType==CURTYPE_BTREE );
  assert( OP_SeekLE == OP_SeekLT+1 );
  assert( OP_SeekGE == OP_SeekLT+2 );
  assert( OP_SeekGT == OP_SeekLT+3 );
  assert( pC->isOrdered );
  assert( pC->uc.pCursor!=0 );
  oc = pOp->opcode;
  eqOnly = 0;
  pC->nullRow = 0;
#ifdef SQLITE_DEBUG
  pC->seekOp = pOp->opcode;
#endif

  if( pC->isTable ){
    /* The BTREE_SEEK_EQ flag is only set on index cursors */
    assert( sqlite3BtreeCursorHasHint(pC->uc.pCursor, BTREE_SEEK_EQ)==0 );

    /* The input value in P3 might be of any type: integer, real, string,
    ** blob, or NULL.  But it needs to be an integer before we can do
    ** the seek, so convert it. */
    pIn3 = &aMem[pOp->p3];
    if( (pIn3->flags & (MEM_Int|MEM_Real|MEM_Str))==MEM_Str ){
      applyNumericAffinity(pIn3, 0);
    }
    iKey = sqlite3VdbeIntValue(pIn3);

    /* If the P3 value could not be converted into an integer without
    ** loss of information, then special processing is required... */
    if( (pIn3->flags & MEM_Int)==0 ){
      if( (pIn3->flags & MEM_Real)==0 ){
        /* If the P3 value cannot be converted into any kind of a number,
        ** then the seek is not possible, so jump to P2 */
        VdbeBranchTaken(1,2); goto jump_to_p2;
        break;
      }

      /* If the approximation iKey is larger than the actual real search
      ** term, substitute >= for > and < for <=. e.g. if the search term
      ** is 4.9 and the integer approximation 5:
      **
      **        (x >  4.9)    ->     (x >= 5)
      **        (x <= 4.9)    ->     (x <  5)
      */
      if( pIn3->u.r<(double)iKey ){
        assert( OP_SeekGE==(OP_SeekGT-1) );
        assert( OP_SeekLT==(OP_SeekLE-1) );
        assert( (OP_SeekLE & 0x0001)==(OP_SeekGT & 0x0001) );
        if( (oc & 0x0001)==(OP_SeekGT & 0x0001) ) oc--;
      }

      /* If the approximation iKey is smaller than the actual real search
      ** term, substitute <= for < and > for >=.  */
      else if( pIn3->u.r>(double)iKey ){
        assert( OP_SeekLE==(OP_SeekLT+1) );
        assert( OP_SeekGT==(OP_SeekGE+1) );
        assert( (OP_SeekLT & 0x0001)==(OP_SeekGE & 0x0001) );
        if( (oc & 0x0001)==(OP_SeekLT & 0x0001) ) oc++;
      }
    } 
    rc = sqlite3BtreeMovetoUnpacked(pC->uc.pCursor, 0, (u64)iKey, 0, &res);
    pC->movetoTarget = iKey;  /* Used by OP_Delete */
    if( rc!=SQLITE_OK ){
      goto abort_due_to_error;
    }
  }else{
    /* For a cursor with the BTREE_SEEK_EQ hint, only the OP_SeekGE and
    ** OP_SeekLE opcodes are allowed, and these must be immediately followed
    ** by an OP_IdxGT or OP_IdxLT opcode, respectively, with the same key.
    */
    if( sqlite3BtreeCursorHasHint(pC->uc.pCursor, BTREE_SEEK_EQ) ){
      eqOnly = 1;
      assert( pOp->opcode==OP_SeekGE || pOp->opcode==OP_SeekLE );
      assert( pOp[1].opcode==OP_IdxLT || pOp[1].opcode==OP_IdxGT );
      assert( pOp[1].p1==pOp[0].p1 );
      assert( pOp[1].p2==pOp[0].p2 );
      assert( pOp[1].p3==pOp[0].p3 );
      assert( pOp[1].p4.i==pOp[0].p4.i );
    }

    nField = pOp->p4.i;
    assert( pOp->p4type==P4_INT32 );
    assert( nField>0 );
    r.pKeyInfo = pC->pKeyInfo;
    r.nField = (u16)nField;

    /* The next line of code computes as follows, only faster:
    **   if( oc==OP_SeekGT || oc==OP_SeekLE ){
    **     r.default_rc = -1;
    **   }else{
    **     r.default_rc = +1;
    **   }
    */
    r.default_rc = ((1 & (oc - OP_SeekLT)) ? -1 : +1);
    assert( oc!=OP_SeekGT || r.default_rc==-1 );
    assert( oc!=OP_SeekLE || r.default_rc==-1 );
    assert( oc!=OP_SeekGE || r.default_rc==+1 );
    assert( oc!=OP_SeekLT || r.default_rc==+1 );

    r.aMem = &aMem[pOp->p3];
#ifdef SQLITE_DEBUG
    { int i; for(i=0; i<r.nField; i++) assert( memIsValid(&r.aMem[i]) ); }
#endif
    r.eqSeen = 0;
    rc = sqlite3BtreeMovetoUnpacked(pC->uc.pCursor, &r, 0, 0, &res);
    if( rc!=SQLITE_OK ){
      goto abort_due_to_error;
    }
    if( eqOnly && r.eqSeen==0 ){
      assert( res!=0 );
      goto seek_not_found;
    }
  }
  pC->deferredMoveto = 0;
  pC->cacheStatus = CACHE_STALE;
#ifdef SQLITE_TEST
  sqlite3_search_count++;
#endif
  if( oc>=OP_SeekGE ){  assert( oc==OP_SeekGE || oc==OP_SeekGT );
    if( res<0 || (res==0 && oc==OP_SeekGT) ){
      res = 0;
      rc = sqlite3BtreeNext(pC->uc.pCursor, &res);
      if( rc!=SQLITE_OK ) goto abort_due_to_error;
    }else{
      res = 0;
    }
  }else{
    assert( oc==OP_SeekLT || oc==OP_SeekLE );
    if( res>0 || (res==0 && oc==OP_SeekLT) ){
      res = 0;
      rc = sqlite3BtreePrevious(pC->uc.pCursor, &res);
      if( rc!=SQLITE_OK ) goto abort_due_to_error;
    }else{
      /* res might be negative because the table is empty.  Check to
      ** see if this is the case.
      */
      res = sqlite3BtreeEof(pC->uc.pCursor);
    }
  }
seek_not_found:
  assert( pOp->p2>0 );
  VdbeBranchTaken(res!=0,2);
  if( res ){
    goto jump_to_p2;
  }else if( eqOnly ){
    assert( pOp[1].opcode==OP_IdxLT || pOp[1].opcode==OP_IdxGT );
    pOp++; /* Skip the OP_IdxLt or OP_IdxGT that follows */
  }
  break;
}

/* Opcode: Found P1 P2 P3 P4 *
** Synopsis: key=r[P3@P4]
**
** If P4==0 then register P3 holds a blob constructed by MakeRecord.  If
** P4>0 then register P3 is the first of P4 registers that form an unpacked
** record.
**
** Cursor P1 is on an index btree.  If the record identified by P3 and P4
** is a prefix of any entry in P1 then a jump is made to P2 and
** P1 is left pointing at the matching entry.
**
** This operation leaves the cursor in a state where it can be
** advanced in the forward direction.  The Next instruction will work,
** but not the Prev instruction.
**
** See also: NotFound, NoConflict, NotExists. SeekGe
*/
/* Opcode: NotFound P1 P2 P3 P4 *
** Synopsis: key=r[P3@P4]
**
** If P4==0 then register P3 holds a blob constructed by MakeRecord.  If
** P4>0 then register P3 is the first of P4 registers that form an unpacked
** record.
** 
** Cursor P1 is on an index btree.  If the record identified by P3 and P4
** is not the prefix of any entry in P1 then a jump is made to P2.  If P1 
** does contain an entry whose prefix matches the P3/P4 record then control
** falls through to the next instruction and P1 is left pointing at the
** matching entry.
**
** This operation leaves the cursor in a state where it cannot be
** advanced in either direction.  In other words, the Next and Prev
** opcodes do not work after this operation.
**
** See also: Found, NotExists, NoConflict
*/
/* Opcode: NoConflict P1 P2 P3 P4 *
** Synopsis: key=r[P3@P4]
**
** If P4==0 then register P3 holds a blob constructed by MakeRecord.  If
** P4>0 then register P3 is the first of P4 registers that form an unpacked
** record.
** 
** Cursor P1 is on an index btree.  If the record identified by P3 and P4
** contains any NULL value, jump immediately to P2.  If all terms of the
** record are not-NULL then a check is done to determine if any row in the
** P1 index btree has a matching key prefix.  If there are no matches, jump
** immediately to P2.  If there is a match, fall through and leave the P1
** cursor pointing to the matching row.
**
** This opcode is similar to OP_NotFound with the exceptions that the
** branch is always taken if any part of the search key input is NULL.
**
** This operation leaves the cursor in a state where it cannot be
** advanced in either direction.  In other words, the Next and Prev
** opcodes do not work after this operation.
**
** See also: NotFound, Found, NotExists
*/
case OP_NoConflict:     /* jump, in3 */
case OP_NotFound:       /* jump, in3 */
case OP_Found: {        /* jump, in3 */
  int alreadyExists;
  int takeJump;
  int ii;
  VdbeCursor *pC;
  int res;
  char *pFree;
  UnpackedRecord *pIdxKey;
  UnpackedRecord r;
  char aTempRec[ROUND8(sizeof(UnpackedRecord)) + sizeof(Mem)*4 + 7];

#ifdef SQLITE_TEST
  if( pOp->opcode!=OP_NoConflict ) sqlite3_found_count++;
#endif

  assert( pOp->p1>=0 && pOp->p1<p->nCursor );
  assert( pOp->p4type==P4_INT32 );
  pC = p->apCsr[pOp->p1];
  assert( pC!=0 );
#ifdef SQLITE_DEBUG
  pC->seekOp = pOp->opcode;
#endif
  pIn3 = &aMem[pOp->p3];
  assert( pC->eCurType==CURTYPE_BTREE );
  assert( pC->uc.pCursor!=0 );
  assert( pC->isTable==0 );
  pFree = 0;
  if( pOp->p4.i>0 ){
    r.pKeyInfo = pC->pKeyInfo;
    r.nField = (u16)pOp->p4.i;
    r.aMem = pIn3;
#ifdef SQLITE_DEBUG
    for(ii=0; ii<r.nField; ii++){
      assert( memIsValid(&r.aMem[ii]) );
      assert( (r.aMem[ii].flags & MEM_Zero)==0 || r.aMem[ii].n==0 );
      if( ii ) REGISTER_TRACE(pOp->p3+ii, &r.aMem[ii]);
    }
#endif
    pIdxKey = &r;
  }else{
    pIdxKey = sqlite3VdbeAllocUnpackedRecord(
        pC->pKeyInfo, aTempRec, sizeof(aTempRec), &pFree
    );
    if( pIdxKey==0 ) goto no_mem;
    assert( pIn3->flags & MEM_Blob );
    (void)ExpandBlob(pIn3);
    sqlite3VdbeRecordUnpack(pC->pKeyInfo, pIn3->n, pIn3->z, pIdxKey);
  }
  pIdxKey->default_rc = 0;
  takeJump = 0;
  if( pOp->opcode==OP_NoConflict ){
    /* For the OP_NoConflict opcode, take the jump if any of the
    ** input fields are NULL, since any key with a NULL will not
    ** conflict */
    for(ii=0; ii<pIdxKey->nField; ii++){
      if( pIdxKey->aMem[ii].flags & MEM_Null ){
        takeJump = 1;
        break;
      }
    }
  }
  rc = sqlite3BtreeMovetoUnpacked(pC->uc.pCursor, pIdxKey, 0, 0, &res);
  sqlite3DbFree(db, pFree);
  if( rc!=SQLITE_OK ){
    goto abort_due_to_error;
  }
  pC->seekResult = res;
  alreadyExists = (res==0);
  pC->nullRow = 1-alreadyExists;
  pC->deferredMoveto = 0;
  pC->cacheStatus = CACHE_STALE;
  if( pOp->opcode==OP_Found ){
    VdbeBranchTaken(alreadyExists!=0,2);
    if( alreadyExists ) goto jump_to_p2;
  }else{
    VdbeBranchTaken(takeJump||alreadyExists==0,2);
    if( takeJump || !alreadyExists ) goto jump_to_p2;
  }
  break;
}

/* Opcode: SeekRowid P1 P2 P3 * *
** Synopsis: intkey=r[P3]
**
** P1 is the index of a cursor open on an SQL table btree (with integer
** keys).  If register P3 does not contain an integer or if P1 does not
** contain a record with rowid P3 then jump immediately to P2.  
** Or, if P2 is 0, raise an SQLITE_CORRUPT error. If P1 does contain
** a record with rowid P3 then 
** leave the cursor pointing at that record and fall through to the next
** instruction.
**
** The OP_NotExists opcode performs the same operation, but with OP_NotExists
** the P3 register must be guaranteed to contain an integer value.  With this
** opcode, register P3 might not contain an integer.
**
** The OP_NotFound opcode performs the same operation on index btrees
** (with arbitrary multi-value keys).
**
** This opcode leaves the cursor in a state where it cannot be advanced
** in either direction.  In other words, the Next and Prev opcodes will
** not work following this opcode.
**
** See also: Found, NotFound, NoConflict, SeekRowid
*/
/* Opcode: NotExists P1 P2 P3 * *
** Synopsis: intkey=r[P3]
**
** P1 is the index of a cursor open on an SQL table btree (with integer
** keys).  P3 is an integer rowid.  If P1 does not contain a record with
** rowid P3 then jump immediately to P2.  Or, if P2 is 0, raise an
** SQLITE_CORRUPT error. If P1 does contain a record with rowid P3 then 
** leave the cursor pointing at that record and fall through to the next
** instruction.
**
** The OP_SeekRowid opcode performs the same operation but also allows the
** P3 register to contain a non-integer value, in which case the jump is
** always taken.  This opcode requires that P3 always contain an integer.
**
** The OP_NotFound opcode performs the same operation on index btrees
** (with arbitrary multi-value keys).
**
** This opcode leaves the cursor in a state where it cannot be advanced
** in either direction.  In other words, the Next and Prev opcodes will
** not work following this opcode.
**
** See also: Found, NotFound, NoConflict, SeekRowid
*/
case OP_SeekRowid: {        /* jump, in3 */
  VdbeCursor *pC;
  BtCursor *pCrsr;
  int res;
  u64 iKey;

  pIn3 = &aMem[pOp->p3];
  if( (pIn3->flags & MEM_Int)==0 ){
    applyAffinity(pIn3, SQLITE_AFF_NUMERIC, encoding);
    if( (pIn3->flags & MEM_Int)==0 ) goto jump_to_p2;
  }
  /* Fall through into OP_NotExists */
case OP_NotExists:          /* jump, in3 */
  pIn3 = &aMem[pOp->p3];
  assert( pIn3->flags & MEM_Int );
  assert( pOp->p1>=0 && pOp->p1<p->nCursor );
  pC = p->apCsr[pOp->p1];
  assert( pC!=0 );
#ifdef SQLITE_DEBUG
  pC->seekOp = 0;
#endif
  assert( pC->isTable );
  assert( pC->eCurType==CURTYPE_BTREE );
  pCrsr = pC->uc.pCursor;
  assert( pCrsr!=0 );
  res = 0;
  iKey = pIn3->u.i;
  rc = sqlite3BtreeMovetoUnpacked(pCrsr, 0, iKey, 0, &res);
  assert( rc==SQLITE_OK || res==0 );
  pC->movetoTarget = iKey;  /* Used by OP_Delete */
  pC->nullRow = 0;
  pC->cacheStatus = CACHE_STALE;
  pC->deferredMoveto = 0;
  VdbeBranchTaken(res!=0,2);
  pC->seekResult = res;
  if( res!=0 ){
    assert( rc==SQLITE_OK );
    if( pOp->p2==0 ){
      rc = SQLITE_CORRUPT_BKPT;
    }else{
      goto jump_to_p2;
    }
  }
  if( rc ) goto abort_due_to_error;
  break;
}

/* Opcode: Sequence P1 P2 * * *
** Synopsis: r[P2]=cursor[P1].ctr++
**
** Find the next available sequence number for cursor P1.
** Write the sequence number into register P2.
** The sequence number on the cursor is incremented after this
** instruction.  
*/
case OP_Sequence: {           /* out2 */
  assert( pOp->p1>=0 && pOp->p1<p->nCursor );
  assert( p->apCsr[pOp->p1]!=0 );
  assert( p->apCsr[pOp->p1]->eCurType!=CURTYPE_VTAB );
  pOut = out2Prerelease(p, pOp);
  pOut->u.i = p->apCsr[pOp->p1]->seqCount++;
  break;
}


/* Opcode: NewRowid P1 P2 P3 * *
** Synopsis: r[P2]=rowid
**
** Get a new integer record number (a.k.a "rowid") used as the key to a table.
** The record number is not previously used as a key in the database
** table that cursor P1 points to.  The new record number is written
** written to register P2.
**
** If P3>0 then P3 is a register in the root frame of this VDBE that holds 
** the largest previously generated record number. No new record numbers are
** allowed to be less than this value. When this value reaches its maximum, 
** an SQLITE_FULL error is generated. The P3 register is updated with the '
** generated record number. This P3 mechanism is used to help implement the
** AUTOINCREMENT feature.
*/
case OP_NewRowid: {           /* out2 */
  i64 v;                 /* The new rowid */
  VdbeCursor *pC;        /* Cursor of table to get the new rowid */
  int res;               /* Result of an sqlite3BtreeLast() */
  int cnt;               /* Counter to limit the number of searches */
  Mem *pMem;             /* Register holding largest rowid for AUTOINCREMENT */
  VdbeFrame *pFrame;     /* Root frame of VDBE */

  v = 0;
  res = 0;
  pOut = out2Prerelease(p, pOp);
  assert( pOp->p1>=0 && pOp->p1<p->nCursor );
  pC = p->apCsr[pOp->p1];
  assert( pC!=0 );
  assert( pC->eCurType==CURTYPE_BTREE );
  assert( pC->uc.pCursor!=0 );
  {
    /* The next rowid or record number (different terms for the same
    ** thing) is obtained in a two-step algorithm.
    **
    ** First we attempt to find the largest existing rowid and add one
    ** to that.  But if the largest existing rowid is already the maximum
    ** positive integer, we have to fall through to the second
    ** probabilistic algorithm
    **
    ** The second algorithm is to select a rowid at random and see if
    ** it already exists in the table.  If it does not exist, we have
    ** succeeded.  If the random rowid does exist, we select a new one
    ** and try again, up to 100 times.
    */
    assert( pC->isTable );

#ifdef SQLITE_32BIT_ROWID
#   define MAX_ROWID 0x7fffffff
#else
    /* Some compilers complain about constants of the form 0x7fffffffffffffff.
    ** Others complain about 0x7ffffffffffffffffLL.  The following macro seems
    ** to provide the constant while making all compilers happy.
    */
#   define MAX_ROWID  (i64)( (((u64)0x7fffffff)<<32) | (u64)0xffffffff )
#endif

    if( !pC->useRandomRowid ){
      rc = sqlite3BtreeLast(pC->uc.pCursor, &res);
      if( rc!=SQLITE_OK ){
        goto abort_due_to_error;
      }
      if( res ){
        v = 1;   /* IMP: R-61914-48074 */
      }else{
        assert( sqlite3BtreeCursorIsValid(pC->uc.pCursor) );
        v = sqlite3BtreeIntegerKey(pC->uc.pCursor);
        if( v>=MAX_ROWID ){
          pC->useRandomRowid = 1;
        }else{
          v++;   /* IMP: R-29538-34987 */
        }
      }
    }

#ifndef SQLITE_OMIT_AUTOINCREMENT
    if( pOp->p3 ){
      /* Assert that P3 is a valid memory cell. */
      assert( pOp->p3>0 );
      if( p->pFrame ){
        for(pFrame=p->pFrame; pFrame->pParent; pFrame=pFrame->pParent);
        /* Assert that P3 is a valid memory cell. */
        assert( pOp->p3<=pFrame->nMem );
        pMem = &pFrame->aMem[pOp->p3];
      }else{
        /* Assert that P3 is a valid memory cell. */
        assert( pOp->p3<=(p->nMem+1 - p->nCursor) );
        pMem = &aMem[pOp->p3];
        memAboutToChange(p, pMem);
      }
      assert( memIsValid(pMem) );

      REGISTER_TRACE(pOp->p3, pMem);
      sqlite3VdbeMemIntegerify(pMem);
      assert( (pMem->flags & MEM_Int)!=0 );  /* mem(P3) holds an integer */
      if( pMem->u.i==MAX_ROWID || pC->useRandomRowid ){
        rc = SQLITE_FULL;   /* IMP: R-12275-61338 */
        goto abort_due_to_error;
      }
      if( v<pMem->u.i+1 ){
        v = pMem->u.i + 1;
      }
      pMem->u.i = v;
    }
#endif
    if( pC->useRandomRowid ){
      /* IMPLEMENTATION-OF: R-07677-41881 If the largest ROWID is equal to the
      ** largest possible integer (9223372036854775807) then the database
      ** engine starts picking positive candidate ROWIDs at random until
      ** it finds one that is not previously used. */
      assert( pOp->p3==0 );  /* We cannot be in random rowid mode if this is
                             ** an AUTOINCREMENT table. */
      cnt = 0;
      do{
        sqlite3_randomness(sizeof(v), &v);
        v &= (MAX_ROWID>>1); v++;  /* Ensure that v is greater than zero */
      }while(  ((rc = sqlite3BtreeMovetoUnpacked(pC->uc.pCursor, 0, (u64)v,
                                                 0, &res))==SQLITE_OK)
            && (res==0)
            && (++cnt<100));
      if( rc ) goto abort_due_to_error;
      if( res==0 ){
        rc = SQLITE_FULL;   /* IMP: R-38219-53002 */
        goto abort_due_to_error;
      }
      assert( v>0 );  /* EV: R-40812-03570 */
    }
    pC->deferredMoveto = 0;
    pC->cacheStatus = CACHE_STALE;
  }
  pOut->u.i = v;
  break;
}

/* Opcode: Insert P1 P2 P3 P4 P5
** Synopsis: intkey=r[P3] data=r[P2]
**
** Write an entry into the table of cursor P1.  A new entry is
** created if it doesn't already exist or the data for an existing
** entry is overwritten.  The data is the value MEM_Blob stored in register
** number P2. The key is stored in register P3. The key must
** be a MEM_Int.
**
** If the OPFLAG_NCHANGE flag of P5 is set, then the row change count is
** incremented (otherwise not).  If the OPFLAG_LASTROWID flag of P5 is set,
** then rowid is stored for subsequent return by the
** sqlite3_last_insert_rowid() function (otherwise it is unmodified).
**
** If the OPFLAG_USESEEKRESULT flag of P5 is set and if the result of
** the last seek operation (OP_NotExists or OP_SeekRowid) was a success,
** then this
** operation will not attempt to find the appropriate row before doing
** the insert but will instead overwrite the row that the cursor is
** currently pointing to.  Presumably, the prior OP_NotExists or
** OP_SeekRowid opcode
** has already positioned the cursor correctly.  This is an optimization
** that boosts performance by avoiding redundant seeks.
**
** If the OPFLAG_ISUPDATE flag is set, then this opcode is part of an
** UPDATE operation.  Otherwise (if the flag is clear) then this opcode
** is part of an INSERT operation.  The difference is only important to
** the update hook.
**
** Parameter P4 may point to a Table structure, or may be NULL. If it is 
** not NULL, then the update-hook (sqlite3.xUpdateCallback) is invoked 
** following a successful insert.
**
** (WARNING/TODO: If P1 is a pseudo-cursor and P2 is dynamically
** allocated, then ownership of P2 is transferred to the pseudo-cursor
** and register P2 becomes ephemeral.  If the cursor is changed, the
** value of register P2 will then change.  Make sure this does not
** cause any problems.)
**
** This instruction only works on tables.  The equivalent instruction
** for indices is OP_IdxInsert.
*/
/* Opcode: InsertInt P1 P2 P3 P4 P5
** Synopsis: intkey=P3 data=r[P2]
**
** This works exactly like OP_Insert except that the key is the
** integer value P3, not the value of the integer stored in register P3.
*/
case OP_Insert: 
case OP_InsertInt: {
  Mem *pData;       /* MEM cell holding data for the record to be inserted */
  Mem *pKey;        /* MEM cell holding key  for the record */
  VdbeCursor *pC;   /* Cursor to table into which insert is written */
  int seekResult;   /* Result of prior seek or 0 if no USESEEKRESULT flag */
  const char *zDb;  /* database name - used by the update hook */
  Table *pTab;      /* Table structure - used by update and pre-update hooks */
  int op;           /* Opcode for update hook: SQLITE_UPDATE or SQLITE_INSERT */
  BtreePayload x;   /* Payload to be inserted */

  op = 0;
  pData = &aMem[pOp->p2];
  assert( pOp->p1>=0 && pOp->p1<p->nCursor );
  assert( memIsValid(pData) );
  pC = p->apCsr[pOp->p1];
  assert( pC!=0 );
  assert( pC->eCurType==CURTYPE_BTREE );
  assert( pC->uc.pCursor!=0 );
  assert( pC->isTable );
  assert( pOp->p4type==P4_TABLE || pOp->p4type>=P4_STATIC );
  REGISTER_TRACE(pOp->p2, pData);

  if( pOp->opcode==OP_Insert ){
    pKey = &aMem[pOp->p3];
    assert( pKey->flags & MEM_Int );
    assert( memIsValid(pKey) );
    REGISTER_TRACE(pOp->p3, pKey);
    x.nKey = pKey->u.i;
  }else{
    assert( pOp->opcode==OP_InsertInt );
    x.nKey = pOp->p3;
  }

  if( pOp->p4type==P4_TABLE && HAS_UPDATE_HOOK(db) ){
    assert( pC->isTable );
    assert( pC->iDb>=0 );
    zDb = db->aDb[pC->iDb].zDbSName;
    pTab = pOp->p4.pTab;
    assert( HasRowid(pTab) );
    op = ((pOp->p5 & OPFLAG_ISUPDATE) ? SQLITE_UPDATE : SQLITE_INSERT);
  }else{
    pTab = 0; /* Not needed.  Silence a comiler warning. */
    zDb = 0;  /* Not needed.  Silence a compiler warning. */
  }

#ifdef SQLITE_ENABLE_PREUPDATE_HOOK
  /* Invoke the pre-update hook, if any */
  if( db->xPreUpdateCallback 
   && pOp->p4type==P4_TABLE
   && !(pOp->p5 & OPFLAG_ISUPDATE)
  ){
    sqlite3VdbePreUpdateHook(p, pC, SQLITE_INSERT, zDb, pTab, x.nKey, pOp->p2);
  }
#endif

  if( pOp->p5 & OPFLAG_NCHANGE ) p->nChange++;
  if( pOp->p5 & OPFLAG_LASTROWID ) db->lastRowid = lastRowid = x.nKey;
  if( pData->flags & MEM_Null ){
    x.pData = 0;
    x.nData = 0;
  }else{
    assert( pData->flags & (MEM_Blob|MEM_Str) );
    x.pData = pData->z;
    x.nData = pData->n;
  }
  seekResult = ((pOp->p5 & OPFLAG_USESEEKRESULT) ? pC->seekResult : 0);
  if( pData->flags & MEM_Zero ){
    x.nZero = pData->u.nZero;
  }else{
    x.nZero = 0;
  }
  x.pKey = 0;
  rc = sqlite3BtreeInsert(pC->uc.pCursor, &x,
                          (pOp->p5 & OPFLAG_APPEND)!=0, seekResult
  );
  pC->deferredMoveto = 0;
  pC->cacheStatus = CACHE_STALE;

  /* Invoke the update-hook if required. */
  if( rc ) goto abort_due_to_error;
  if( db->xUpdateCallback && op ){
    db->xUpdateCallback(db->pUpdateArg, op, zDb, pTab->zName, x.nKey);
  }
  break;
}

/* Opcode: Delete P1 P2 P3 P4 P5
**
** Delete the record at which the P1 cursor is currently pointing.
**
** If the OPFLAG_SAVEPOSITION bit of the P5 parameter is set, then
** the cursor will be left pointing at  either the next or the previous
** record in the table. If it is left pointing at the next record, then
** the next Next instruction will be a no-op. As a result, in this case
** it is ok to delete a record from within a Next loop. If 
** OPFLAG_SAVEPOSITION bit of P5 is clear, then the cursor will be
** left in an undefined state.
**
** If the OPFLAG_AUXDELETE bit is set on P5, that indicates that this
** delete one of several associated with deleting a table row and all its
** associated index entries.  Exactly one of those deletes is the "primary"
** delete.  The others are all on OPFLAG_FORDELETE cursors or else are
** marked with the AUXDELETE flag.
**
** If the OPFLAG_NCHANGE flag of P2 (NB: P2 not P5) is set, then the row
** change count is incremented (otherwise not).
**
** P1 must not be pseudo-table.  It has to be a real table with
** multiple rows.
**
** If P4 is not NULL then it points to a Table object. In this case either 
** the update or pre-update hook, or both, may be invoked. The P1 cursor must
** have been positioned using OP_NotFound prior to invoking this opcode in 
** this case. Specifically, if one is configured, the pre-update hook is 
** invoked if P4 is not NULL. The update-hook is invoked if one is configured, 
** P4 is not NULL, and the OPFLAG_NCHANGE flag is set in P2.
**
** If the OPFLAG_ISUPDATE flag is set in P2, then P3 contains the address
** of the memory cell that contains the value that the rowid of the row will
** be set to by the update.
*/
case OP_Delete: {
  VdbeCursor *pC;
  const char *zDb;
  Table *pTab;
  int opflags;

  opflags = pOp->p2;
  assert( pOp->p1>=0 && pOp->p1<p->nCursor );
  pC = p->apCsr[pOp->p1];
  assert( pC!=0 );
  assert( pC->eCurType==CURTYPE_BTREE );
  assert( pC->uc.pCursor!=0 );
  assert( pC->deferredMoveto==0 );

#ifdef SQLITE_DEBUG
  if( pOp->p4type==P4_TABLE && HasRowid(pOp->p4.pTab) && pOp->p5==0 ){
    /* If p5 is zero, the seek operation that positioned the cursor prior to
    ** OP_Delete will have also set the pC->movetoTarget field to the rowid of
    ** the row that is being deleted */
    i64 iKey = sqlite3BtreeIntegerKey(pC->uc.pCursor);
    assert( pC->movetoTarget==iKey );
  }
#endif

  /* If the update-hook or pre-update-hook will be invoked, set zDb to
  ** the name of the db to pass as to it. Also set local pTab to a copy
  ** of p4.pTab. Finally, if p5 is true, indicating that this cursor was
  ** last moved with OP_Next or OP_Prev, not Seek or NotFound, set 
  ** VdbeCursor.movetoTarget to the current rowid.  */
  if( pOp->p4type==P4_TABLE && HAS_UPDATE_HOOK(db) ){
    assert( pC->iDb>=0 );
    assert( pOp->p4.pTab!=0 );
    zDb = db->aDb[pC->iDb].zDbSName;
    pTab = pOp->p4.pTab;
    if( (pOp->p5 & OPFLAG_SAVEPOSITION)!=0 && pC->isTable ){
      pC->movetoTarget = sqlite3BtreeIntegerKey(pC->uc.pCursor);
    }
  }else{
    zDb = 0;   /* Not needed.  Silence a compiler warning. */
    pTab = 0;  /* Not needed.  Silence a compiler warning. */
  }

#ifdef SQLITE_ENABLE_PREUPDATE_HOOK
  /* Invoke the pre-update-hook if required. */
  if( db->xPreUpdateCallback && pOp->p4.pTab && HasRowid(pTab) ){
    assert( !(opflags & OPFLAG_ISUPDATE) || (aMem[pOp->p3].flags & MEM_Int) );
    sqlite3VdbePreUpdateHook(p, pC,
        (opflags & OPFLAG_ISUPDATE) ? SQLITE_UPDATE : SQLITE_DELETE, 
        zDb, pTab, pC->movetoTarget,
        pOp->p3
    );
  }
  if( opflags & OPFLAG_ISNOOP ) break;
#endif
 
  /* Only flags that can be set are SAVEPOISTION and AUXDELETE */ 
  assert( (pOp->p5 & ~(OPFLAG_SAVEPOSITION|OPFLAG_AUXDELETE))==0 );
  assert( OPFLAG_SAVEPOSITION==BTREE_SAVEPOSITION );
  assert( OPFLAG_AUXDELETE==BTREE_AUXDELETE );

#ifdef SQLITE_DEBUG
  if( p->pFrame==0 ){
    if( pC->isEphemeral==0
        && (pOp->p5 & OPFLAG_AUXDELETE)==0
        && (pC->wrFlag & OPFLAG_FORDELETE)==0
      ){
      nExtraDelete++;
    }
    if( pOp->p2 & OPFLAG_NCHANGE ){
      nExtraDelete--;
    }
  }
#endif

  rc = sqlite3BtreeDelete(pC->uc.pCursor, pOp->p5);
  pC->cacheStatus = CACHE_STALE;
  if( rc ) goto abort_due_to_error;

  /* Invoke the update-hook if required. */
  if( opflags & OPFLAG_NCHANGE ){
    p->nChange++;
    if( db->xUpdateCallback && HasRowid(pTab) ){
      db->xUpdateCallback(db->pUpdateArg, SQLITE_DELETE, zDb, pTab->zName,
          pC->movetoTarget);
      assert( pC->iDb>=0 );
    }
  }

  break;
}
/* Opcode: ResetCount * * * * *
**
** The value of the change counter is copied to the database handle
** change counter (returned by subsequent calls to sqlite3_changes()).
** Then the VMs internal change counter resets to 0.
** This is used by trigger programs.
*/
case OP_ResetCount: {
  sqlite3VdbeSetChanges(db, p->nChange);
  p->nChange = 0;
  break;
}

/* Opcode: SorterCompare P1 P2 P3 P4
** Synopsis: if key(P1)!=trim(r[P3],P4) goto P2
**
** P1 is a sorter cursor. This instruction compares a prefix of the
** record blob in register P3 against a prefix of the entry that 
** the sorter cursor currently points to.  Only the first P4 fields
** of r[P3] and the sorter record are compared.
**
** If either P3 or the sorter contains a NULL in one of their significant
** fields (not counting the P4 fields at the end which are ignored) then
** the comparison is assumed to be equal.
**
** Fall through to next instruction if the two records compare equal to
** each other.  Jump to P2 if they are different.
*/
case OP_SorterCompare: {
  VdbeCursor *pC;
  int res;
  int nKeyCol;

  pC = p->apCsr[pOp->p1];
  assert( isSorter(pC) );
  assert( pOp->p4type==P4_INT32 );
  pIn3 = &aMem[pOp->p3];
  nKeyCol = pOp->p4.i;
  res = 0;
  rc = sqlite3VdbeSorterCompare(pC, pIn3, nKeyCol, &res);
  VdbeBranchTaken(res!=0,2);
  if( rc ) goto abort_due_to_error;
  if( res ) goto jump_to_p2;
  break;
};

/* Opcode: SorterData P1 P2 P3 * *
** Synopsis: r[P2]=data
**
** Write into register P2 the current sorter data for sorter cursor P1.
** Then clear the column header cache on cursor P3.
**
** This opcode is normally use to move a record out of the sorter and into
** a register that is the source for a pseudo-table cursor created using
** OpenPseudo.  That pseudo-table cursor is the one that is identified by
** parameter P3.  Clearing the P3 column cache as part of this opcode saves
** us from having to issue a separate NullRow instruction to clear that cache.
*/
case OP_SorterData: {
  VdbeCursor *pC;

  pOut = &aMem[pOp->p2];
  pC = p->apCsr[pOp->p1];
  assert( isSorter(pC) );
  rc = sqlite3VdbeSorterRowkey(pC, pOut);
  assert( rc!=SQLITE_OK || (pOut->flags & MEM_Blob) );
  assert( pOp->p1>=0 && pOp->p1<p->nCursor );
  if( rc ) goto abort_due_to_error;
  p->apCsr[pOp->p3]->cacheStatus = CACHE_STALE;
  break;
}

/* Opcode: RowData P1 P2 * * *
** Synopsis: r[P2]=data
**
** Write into register P2 the complete row data for cursor P1.
** There is no interpretation of the data.  
** It is just copied onto the P2 register exactly as 
** it is found in the database file.
**
** If the P1 cursor must be pointing to a valid row (not a NULL row)
** of a real table, not a pseudo-table.
*/
/* Opcode: RowKey P1 P2 * * *
** Synopsis: r[P2]=key
**
** Write into register P2 the complete row key for cursor P1.
** There is no interpretation of the data.  
** The key is copied onto the P2 register exactly as 
** it is found in the database file.
**
** If the P1 cursor must be pointing to a valid row (not a NULL row)
** of a real table, not a pseudo-table.
*/
case OP_RowKey:
case OP_RowData: {
  VdbeCursor *pC;
  BtCursor *pCrsr;
  u32 n;

  pOut = &aMem[pOp->p2];
  memAboutToChange(p, pOut);

  /* Note that RowKey and RowData are really exactly the same instruction */
  assert( pOp->p1>=0 && pOp->p1<p->nCursor );
  pC = p->apCsr[pOp->p1];
  assert( pC!=0 );
  assert( pC->eCurType==CURTYPE_BTREE );
  assert( isSorter(pC)==0 );
  assert( pC->isTable || pOp->opcode!=OP_RowData );
  assert( pC->isTable==0 || pOp->opcode==OP_RowData );
  assert( pC->nullRow==0 );
  assert( pC->uc.pCursor!=0 );
  pCrsr = pC->uc.pCursor;

  /* The OP_RowKey and OP_RowData opcodes always follow OP_NotExists or
  ** OP_SeekRowid or OP_Rewind/Op_Next with no intervening instructions
  ** that might invalidate the cursor.
  ** If this where not the case, on of the following assert()s
  ** would fail.  Should this ever change (because of changes in the code
  ** generator) then the fix would be to insert a call to
  ** sqlite3VdbeCursorMoveto().
  */
  assert( pC->deferredMoveto==0 );
  assert( sqlite3BtreeCursorIsValid(pCrsr) );
#if 0  /* Not required due to the previous to assert() statements */
  rc = sqlite3VdbeCursorMoveto(pC);
  if( rc!=SQLITE_OK ) goto abort_due_to_error;
#endif

  n = sqlite3BtreePayloadSize(pCrsr);
  if( n>(u32)db->aLimit[SQLITE_LIMIT_LENGTH] ){
    goto too_big;
  }
  testcase( n==0 );
  if( sqlite3VdbeMemClearAndResize(pOut, MAX(n,32)) ){
    goto no_mem;
  }
  pOut->n = n;
  MemSetTypeFlag(pOut, MEM_Blob);
  if( pC->isTable==0 ){
    rc = sqlite3BtreeKey(pCrsr, 0, n, pOut->z);
  }else{
    rc = sqlite3BtreeData(pCrsr, 0, n, pOut->z);
  }
  if( rc ) goto abort_due_to_error;
  pOut->enc = SQLITE_UTF8;  /* In case the blob is ever cast to text */
  UPDATE_MAX_BLOBSIZE(pOut);
  REGISTER_TRACE(pOp->p2, pOut);
  break;
}

/* Opcode: Rowid P1 P2 * * *
** Synopsis: r[P2]=rowid
**
** Store in register P2 an integer which is the key of the table entry that
** P1 is currently point to.
**
** P1 can be either an ordinary table or a virtual table.  There used to
** be a separate OP_VRowid opcode for use with virtual tables, but this
** one opcode now works for both table types.
*/
case OP_Rowid: {                 /* out2 */
  VdbeCursor *pC;
  i64 v;
  sqlite3_vtab *pVtab;
  const sqlite3_module *pModule;

  pOut = out2Prerelease(p, pOp);
  assert( pOp->p1>=0 && pOp->p1<p->nCursor );
  pC = p->apCsr[pOp->p1];
  assert( pC!=0 );
  assert( pC->eCurType!=CURTYPE_PSEUDO || pC->nullRow );
  if( pC->nullRow ){
    pOut->flags = MEM_Null;
    break;
  }else if( pC->deferredMoveto ){
    v = pC->movetoTarget;
#ifndef SQLITE_OMIT_VIRTUALTABLE
  }else if( pC->eCurType==CURTYPE_VTAB ){
    assert( pC->uc.pVCur!=0 );
    pVtab = pC->uc.pVCur->pVtab;
    pModule = pVtab->pModule;
    assert( pModule->xRowid );
    rc = pModule->xRowid(pC->uc.pVCur, &v);
    sqlite3VtabImportErrmsg(p, pVtab);
    if( rc ) goto abort_due_to_error;
#endif /* SQLITE_OMIT_VIRTUALTABLE */
  }else{
    assert( pC->eCurType==CURTYPE_BTREE );
    assert( pC->uc.pCursor!=0 );
    rc = sqlite3VdbeCursorRestore(pC);
    if( rc ) goto abort_due_to_error;
    if( pC->nullRow ){
      pOut->flags = MEM_Null;
      break;
    }
    v = sqlite3BtreeIntegerKey(pC->uc.pCursor);
  }
  pOut->u.i = v;
  break;
}

/* Opcode: NullRow P1 * * * *
**
** Move the cursor P1 to a null row.  Any OP_Column operations
** that occur while the cursor is on the null row will always
** write a NULL.
*/
case OP_NullRow: {
  VdbeCursor *pC;

  assert( pOp->p1>=0 && pOp->p1<p->nCursor );
  pC = p->apCsr[pOp->p1];
  assert( pC!=0 );
  pC->nullRow = 1;
  pC->cacheStatus = CACHE_STALE;
  if( pC->eCurType==CURTYPE_BTREE ){
    assert( pC->uc.pCursor!=0 );
    sqlite3BtreeClearCursor(pC->uc.pCursor);
  }
  break;
}

/* Opcode: Last P1 P2 P3 * *
**
** The next use of the Rowid or Column or Prev instruction for P1 
** will refer to the last entry in the database table or index.
** If the table or index is empty and P2>0, then jump immediately to P2.
** If P2 is 0 or if the table or index is not empty, fall through
** to the following instruction.
**
** This opcode leaves the cursor configured to move in reverse order,
** from the end toward the beginning.  In other words, the cursor is
** configured to use Prev, not Next.
*/
case OP_Last: {        /* jump */
  VdbeCursor *pC;
  BtCursor *pCrsr;
  int res;

  assert( pOp->p1>=0 && pOp->p1<p->nCursor );
  pC = p->apCsr[pOp->p1];
  assert( pC!=0 );
  assert( pC->eCurType==CURTYPE_BTREE );
  pCrsr = pC->uc.pCursor;
  res = 0;
  assert( pCrsr!=0 );
  rc = sqlite3BtreeLast(pCrsr, &res);
  pC->nullRow = (u8)res;
  pC->deferredMoveto = 0;
  pC->cacheStatus = CACHE_STALE;
  pC->seekResult = pOp->p3;
#ifdef SQLITE_DEBUG
  pC->seekOp = OP_Last;
#endif
  if( rc ) goto abort_due_to_error;
  if( pOp->p2>0 ){
    VdbeBranchTaken(res!=0,2);
    if( res ) goto jump_to_p2;
  }
  break;
}


/* Opcode: Sort P1 P2 * * *
**
** This opcode does exactly the same thing as OP_Rewind except that
** it increments an undocumented global variable used for testing.
**
** Sorting is accomplished by writing records into a sorting index,
** then rewinding that index and playing it back from beginning to
** end.  We use the OP_Sort opcode instead of OP_Rewind to do the
** rewinding so that the global variable will be incremented and
** regression tests can determine whether or not the optimizer is
** correctly optimizing out sorts.
*/
case OP_SorterSort:    /* jump */
case OP_Sort: {        /* jump */
#ifdef SQLITE_TEST
  sqlite3_sort_count++;
  sqlite3_search_count--;
#endif
  p->aCounter[SQLITE_STMTSTATUS_SORT]++;
  /* Fall through into OP_Rewind */
}
/* Opcode: Rewind P1 P2 * * *
**
** The next use of the Rowid or Column or Next instruction for P1 
** will refer to the first entry in the database table or index.
** If the table or index is empty, jump immediately to P2.
** If the table or index is not empty, fall through to the following 
** instruction.
**
** This opcode leaves the cursor configured to move in forward order,
** from the beginning toward the end.  In other words, the cursor is
** configured to use Next, not Prev.
*/
case OP_Rewind: {        /* jump */
  VdbeCursor *pC;
  BtCursor *pCrsr;
  int res;

  assert( pOp->p1>=0 && pOp->p1<p->nCursor );
  pC = p->apCsr[pOp->p1];
  assert( pC!=0 );
  assert( isSorter(pC)==(pOp->opcode==OP_SorterSort) );
  res = 1;
#ifdef SQLITE_DEBUG
  pC->seekOp = OP_Rewind;
#endif
  if( isSorter(pC) ){
    rc = sqlite3VdbeSorterRewind(pC, &res);
  }else{
    assert( pC->eCurType==CURTYPE_BTREE );
    pCrsr = pC->uc.pCursor;
    assert( pCrsr );
    rc = sqlite3BtreeFirst(pCrsr, &res);
    pC->deferredMoveto = 0;
    pC->cacheStatus = CACHE_STALE;
  }
  if( rc ) goto abort_due_to_error;
  pC->nullRow = (u8)res;
  assert( pOp->p2>0 && pOp->p2<p->nOp );
  VdbeBranchTaken(res!=0,2);
  if( res ) goto jump_to_p2;
  break;
}

/* Opcode: Next P1 P2 P3 P4 P5
**
** Advance cursor P1 so that it points to the next key/data pair in its
** table or index.  If there are no more key/value pairs then fall through
** to the following instruction.  But if the cursor advance was successful,
** jump immediately to P2.
**
** The Next opcode is only valid following an SeekGT, SeekGE, or
** OP_Rewind opcode used to position the cursor.  Next is not allowed
** to follow SeekLT, SeekLE, or OP_Last.
**
** The P1 cursor must be for a real table, not a pseudo-table.  P1 must have
** been opened prior to this opcode or the program will segfault.
**
** The P3 value is a hint to the btree implementation. If P3==1, that
** means P1 is an SQL index and that this instruction could have been
** omitted if that index had been unique.  P3 is usually 0.  P3 is
** always either 0 or 1.
**
** P4 is always of type P4_ADVANCE. The function pointer points to
** sqlite3BtreeNext().
**
** If P5 is positive and the jump is taken, then event counter
** number P5-1 in the prepared statement is incremented.
**
** See also: Prev, NextIfOpen
*/
/* Opcode: NextIfOpen P1 P2 P3 P4 P5
**
** This opcode works just like Next except that if cursor P1 is not
** open it behaves a no-op.
*/
/* Opcode: Prev P1 P2 P3 P4 P5
**
** Back up cursor P1 so that it points to the previous key/data pair in its
** table or index.  If there is no previous key/value pairs then fall through
** to the following instruction.  But if the cursor backup was successful,
** jump immediately to P2.
**
**
** The Prev opcode is only valid following an SeekLT, SeekLE, or
** OP_Last opcode used to position the cursor.  Prev is not allowed
** to follow SeekGT, SeekGE, or OP_Rewind.
**
** The P1 cursor must be for a real table, not a pseudo-table.  If P1 is
** not open then the behavior is undefined.
**
** The P3 value is a hint to the btree implementation. If P3==1, that
** means P1 is an SQL index and that this instruction could have been
** omitted if that index had been unique.  P3 is usually 0.  P3 is
** always either 0 or 1.
**
** P4 is always of type P4_ADVANCE. The function pointer points to
** sqlite3BtreePrevious().
**
** If P5 is positive and the jump is taken, then event counter
** number P5-1 in the prepared statement is incremented.
*/
/* Opcode: PrevIfOpen P1 P2 P3 P4 P5
**
** This opcode works just like Prev except that if cursor P1 is not
** open it behaves a no-op.
*/
case OP_SorterNext: {  /* jump */
  VdbeCursor *pC;
  int res;

  pC = p->apCsr[pOp->p1];
  assert( isSorter(pC) );
  res = 0;
  rc = sqlite3VdbeSorterNext(db, pC, &res);
  goto next_tail;
case OP_PrevIfOpen:    /* jump */
case OP_NextIfOpen:    /* jump */
  if( p->apCsr[pOp->p1]==0 ) break;
  /* Fall through */
case OP_Prev:          /* jump */
case OP_Next:          /* jump */
  assert( pOp->p1>=0 && pOp->p1<p->nCursor );
  assert( pOp->p5<ArraySize(p->aCounter) );
  pC = p->apCsr[pOp->p1];
  res = pOp->p3;
  assert( pC!=0 );
  assert( pC->deferredMoveto==0 );
  assert( pC->eCurType==CURTYPE_BTREE );
  assert( res==0 || (res==1 && pC->isTable==0) );
  testcase( res==1 );
  assert( pOp->opcode!=OP_Next || pOp->p4.xAdvance==sqlite3BtreeNext );
  assert( pOp->opcode!=OP_Prev || pOp->p4.xAdvance==sqlite3BtreePrevious );
  assert( pOp->opcode!=OP_NextIfOpen || pOp->p4.xAdvance==sqlite3BtreeNext );
  assert( pOp->opcode!=OP_PrevIfOpen || pOp->p4.xAdvance==sqlite3BtreePrevious);

  /* The Next opcode is only used after SeekGT, SeekGE, and Rewind.
  ** The Prev opcode is only used after SeekLT, SeekLE, and Last. */
  assert( pOp->opcode!=OP_Next || pOp->opcode!=OP_NextIfOpen
       || pC->seekOp==OP_SeekGT || pC->seekOp==OP_SeekGE
       || pC->seekOp==OP_Rewind || pC->seekOp==OP_Found);
  assert( pOp->opcode!=OP_Prev || pOp->opcode!=OP_PrevIfOpen
       || pC->seekOp==OP_SeekLT || pC->seekOp==OP_SeekLE
       || pC->seekOp==OP_Last );

  rc = pOp->p4.xAdvance(pC->uc.pCursor, &res);
next_tail:
  pC->cacheStatus = CACHE_STALE;
  VdbeBranchTaken(res==0,2);
  if( rc ) goto abort_due_to_error;
  if( res==0 ){
    pC->nullRow = 0;
    p->aCounter[pOp->p5]++;
#ifdef SQLITE_TEST
    sqlite3_search_count++;
#endif
    goto jump_to_p2_and_check_for_interrupt;
  }else{
    pC->nullRow = 1;
  }
  goto check_for_interrupt;
}

/* Opcode: IdxInsert P1 P2 P3 * P5
** Synopsis: key=r[P2]
**
** Register P2 holds an SQL index key made using the
** MakeRecord instructions.  This opcode writes that key
** into the index P1.  Data for the entry is nil.
**
** P3 is a flag that provides a hint to the b-tree layer that this
** insert is likely to be an append.
**
** If P5 has the OPFLAG_NCHANGE bit set, then the change counter is
** incremented by this instruction.  If the OPFLAG_NCHANGE bit is clear,
** then the change counter is unchanged.
**
** If P5 has the OPFLAG_USESEEKRESULT bit set, then the cursor must have
** just done a seek to the spot where the new entry is to be inserted.
** This flag avoids doing an extra seek.
**
** This instruction only works for indices.  The equivalent instruction
** for tables is OP_Insert.
*/
case OP_SorterInsert:       /* in2 */
case OP_IdxInsert: {        /* in2 */
  VdbeCursor *pC;
  BtreePayload x;

  assert( pOp->p1>=0 && pOp->p1<p->nCursor );
  pC = p->apCsr[pOp->p1];
  assert( pC!=0 );
  assert( isSorter(pC)==(pOp->opcode==OP_SorterInsert) );
  pIn2 = &aMem[pOp->p2];
  assert( pIn2->flags & MEM_Blob );
  if( pOp->p5 & OPFLAG_NCHANGE ) p->nChange++;
  assert( pC->eCurType==CURTYPE_BTREE || pOp->opcode==OP_SorterInsert );
  assert( pC->isTable==0 );
  rc = ExpandBlob(pIn2);
  if( rc ) goto abort_due_to_error;
  if( pOp->opcode==OP_SorterInsert ){
    rc = sqlite3VdbeSorterWrite(pC, pIn2);
  }else{
    x.nKey = pIn2->n;
    x.pKey = pIn2->z;
    rc = sqlite3BtreeInsert(pC->uc.pCursor, &x, pOp->p3, 
        ((pOp->p5 & OPFLAG_USESEEKRESULT) ? pC->seekResult : 0)
        );
    assert( pC->deferredMoveto==0 );
    pC->cacheStatus = CACHE_STALE;
  }
  if( rc) goto abort_due_to_error;
  break;
}

/* Opcode: IdxDelete P1 P2 P3 * *
** Synopsis: key=r[P2@P3]
**
** The content of P3 registers starting at register P2 form
** an unpacked index key. This opcode removes that entry from the 
** index opened by cursor P1.
*/
case OP_IdxDelete: {
  VdbeCursor *pC;
  BtCursor *pCrsr;
  int res;
  UnpackedRecord r;

  assert( pOp->p3>0 );
  assert( pOp->p2>0 && pOp->p2+pOp->p3<=(p->nMem+1 - p->nCursor)+1 );
  assert( pOp->p1>=0 && pOp->p1<p->nCursor );
  pC = p->apCsr[pOp->p1];
  assert( pC!=0 );
  assert( pC->eCurType==CURTYPE_BTREE );
  pCrsr = pC->uc.pCursor;
  assert( pCrsr!=0 );
  assert( pOp->p5==0 );
  r.pKeyInfo = pC->pKeyInfo;
  r.nField = (u16)pOp->p3;
  r.default_rc = 0;
  r.aMem = &aMem[pOp->p2];
  rc = sqlite3BtreeMovetoUnpacked(pCrsr, &r, 0, 0, &res);
  if( rc ) goto abort_due_to_error;
  if( res==0 ){
    rc = sqlite3BtreeDelete(pCrsr, BTREE_AUXDELETE);
    if( rc ) goto abort_due_to_error;
  }
  assert( pC->deferredMoveto==0 );
  pC->cacheStatus = CACHE_STALE;
  break;
}

/* Opcode: Seek P1 * P3 P4 *
** Synopsis: Move P3 to P1.rowid
**
** P1 is an open index cursor and P3 is a cursor on the corresponding
** table.  This opcode does a deferred seek of the P3 table cursor
** to the row that corresponds to the current row of P1.
**
** This is a deferred seek.  Nothing actually happens until
** the cursor is used to read a record.  That way, if no reads
** occur, no unnecessary I/O happens.
**
** P4 may be an array of integers (type P4_INTARRAY) containing
** one entry for each column in the P3 table.  If array entry a(i)
** is non-zero, then reading column a(i)-1 from cursor P3 is 
** equivalent to performing the deferred seek and then reading column i 
** from P1.  This information is stored in P3 and used to redirect
** reads against P3 over to P1, thus possibly avoiding the need to
** seek and read cursor P3.
*/
/* Opcode: IdxRowid P1 P2 * * *
** Synopsis: r[P2]=rowid
**
** Write into register P2 an integer which is the last entry in the record at
** the end of the index key pointed to by cursor P1.  This integer should be
** the rowid of the table entry to which this index entry points.
**
** See also: Rowid, MakeRecord.
*/
case OP_Seek:
case OP_IdxRowid: {              /* out2 */
  VdbeCursor *pC;                /* The P1 index cursor */
  VdbeCursor *pTabCur;           /* The P2 table cursor (OP_Seek only) */
  i64 rowid;                     /* Rowid that P1 current points to */

  assert( pOp->p1>=0 && pOp->p1<p->nCursor );
  pC = p->apCsr[pOp->p1];
  assert( pC!=0 );
  assert( pC->eCurType==CURTYPE_BTREE );
  assert( pC->uc.pCursor!=0 );
  assert( pC->isTable==0 );
  assert( pC->deferredMoveto==0 );
  assert( !pC->nullRow || pOp->opcode==OP_IdxRowid );

  /* The IdxRowid and Seek opcodes are combined because of the commonality
  ** of sqlite3VdbeCursorRestore() and sqlite3VdbeIdxRowid(). */
  rc = sqlite3VdbeCursorRestore(pC);

  /* sqlite3VbeCursorRestore() can only fail if the record has been deleted
  ** out from under the cursor.  That will never happens for an IdxRowid
  ** or Seek opcode */
  if( NEVER(rc!=SQLITE_OK) ) goto abort_due_to_error;

  if( !pC->nullRow ){
    rowid = 0;  /* Not needed.  Only used to silence a warning. */
    rc = sqlite3VdbeIdxRowid(db, pC->uc.pCursor, &rowid);
    if( rc!=SQLITE_OK ){
      goto abort_due_to_error;
    }
    if( pOp->opcode==OP_Seek ){
      assert( pOp->p3>=0 && pOp->p3<p->nCursor );
      pTabCur = p->apCsr[pOp->p3];
      assert( pTabCur!=0 );
      assert( pTabCur->eCurType==CURTYPE_BTREE );
      assert( pTabCur->uc.pCursor!=0 );
      assert( pTabCur->isTable );
      pTabCur->nullRow = 0;
      pTabCur->movetoTarget = rowid;
      pTabCur->deferredMoveto = 1;
      assert( pOp->p4type==P4_INTARRAY || pOp->p4.ai==0 );
      pTabCur->aAltMap = pOp->p4.ai;
      pTabCur->pAltCursor = pC;
    }else{
      pOut = out2Prerelease(p, pOp);
      pOut->u.i = rowid;
      pOut->flags = MEM_Int;
    }
  }else{
    assert( pOp->opcode==OP_IdxRowid );
    sqlite3VdbeMemSetNull(&aMem[pOp->p2]);
  }
  break;
}

/* Opcode: IdxGE P1 P2 P3 P4 P5
** Synopsis: key=r[P3@P4]
**
** The P4 register values beginning with P3 form an unpacked index 
** key that omits the PRIMARY KEY.  Compare this key value against the index 
** that P1 is currently pointing to, ignoring the PRIMARY KEY or ROWID 
** fields at the end.
**
** If the P1 index entry is greater than or equal to the key value
** then jump to P2.  Otherwise fall through to the next instruction.
*/
/* Opcode: IdxGT P1 P2 P3 P4 P5
** Synopsis: key=r[P3@P4]
**
** The P4 register values beginning with P3 form an unpacked index 
** key that omits the PRIMARY KEY.  Compare this key value against the index 
** that P1 is currently pointing to, ignoring the PRIMARY KEY or ROWID 
** fields at the end.
**
** If the P1 index entry is greater than the key value
** then jump to P2.  Otherwise fall through to the next instruction.
*/
/* Opcode: IdxLT P1 P2 P3 P4 P5
** Synopsis: key=r[P3@P4]
**
** The P4 register values beginning with P3 form an unpacked index 
** key that omits the PRIMARY KEY or ROWID.  Compare this key value against
** the index that P1 is currently pointing to, ignoring the PRIMARY KEY or
** ROWID on the P1 index.
**
** If the P1 index entry is less than the key value then jump to P2.
** Otherwise fall through to the next instruction.
*/
/* Opcode: IdxLE P1 P2 P3 P4 P5
** Synopsis: key=r[P3@P4]
**
** The P4 register values beginning with P3 form an unpacked index 
** key that omits the PRIMARY KEY or ROWID.  Compare this key value against
** the index that P1 is currently pointing to, ignoring the PRIMARY KEY or
** ROWID on the P1 index.
**
** If the P1 index entry is less than or equal to the key value then jump
** to P2. Otherwise fall through to the next instruction.
*/
case OP_IdxLE:          /* jump */
case OP_IdxGT:          /* jump */
case OP_IdxLT:          /* jump */
case OP_IdxGE:  {       /* jump */
  VdbeCursor *pC;
  int res;
  UnpackedRecord r;

  assert( pOp->p1>=0 && pOp->p1<p->nCursor );
  pC = p->apCsr[pOp->p1];
  assert( pC!=0 );
  assert( pC->isOrdered );
  assert( pC->eCurType==CURTYPE_BTREE );
  assert( pC->uc.pCursor!=0);
  assert( pC->deferredMoveto==0 );
  assert( pOp->p5==0 || pOp->p5==1 );
  assert( pOp->p4type==P4_INT32 );
  r.pKeyInfo = pC->pKeyInfo;
  r.nField = (u16)pOp->p4.i;
  if( pOp->opcode<OP_IdxLT ){
    assert( pOp->opcode==OP_IdxLE || pOp->opcode==OP_IdxGT );
    r.default_rc = -1;
  }else{
    assert( pOp->opcode==OP_IdxGE || pOp->opcode==OP_IdxLT );
    r.default_rc = 0;
  }
  r.aMem = &aMem[pOp->p3];
#ifdef SQLITE_DEBUG
  { int i; for(i=0; i<r.nField; i++) assert( memIsValid(&r.aMem[i]) ); }
#endif
  res = 0;  /* Not needed.  Only used to silence a warning. */
  rc = sqlite3VdbeIdxKeyCompare(db, pC, &r, &res);
  assert( (OP_IdxLE&1)==(OP_IdxLT&1) && (OP_IdxGE&1)==(OP_IdxGT&1) );
  if( (pOp->opcode&1)==(OP_IdxLT&1) ){
    assert( pOp->opcode==OP_IdxLE || pOp->opcode==OP_IdxLT );
    res = -res;
  }else{
    assert( pOp->opcode==OP_IdxGE || pOp->opcode==OP_IdxGT );
    res++;
  }
  VdbeBranchTaken(res>0,2);
  if( rc ) goto abort_due_to_error;
  if( res>0 ) goto jump_to_p2;
  break;
}

/* Opcode: Destroy P1 P2 P3 * *
**
** Delete an entire database table or index whose root page in the database
** file is given by P1.
**
** The table being destroyed is in the main database file if P3==0.  If
** P3==1 then the table to be clear is in the auxiliary database file
** that is used to store tables create using CREATE TEMPORARY TABLE.
**
** If AUTOVACUUM is enabled then it is possible that another root page
** might be moved into the newly deleted root page in order to keep all
** root pages contiguous at the beginning of the database.  The former
** value of the root page that moved - its value before the move occurred -
** is stored in register P2.  If no page 
** movement was required (because the table being dropped was already 
** the last one in the database) then a zero is stored in register P2.
** If AUTOVACUUM is disabled then a zero is stored in register P2.
**
** See also: Clear
*/
case OP_Destroy: {     /* out2 */
  int iMoved;
  int iDb;

  assert( p->readOnly==0 );
  assert( pOp->p1>1 );
  pOut = out2Prerelease(p, pOp);
  pOut->flags = MEM_Null;
  if( db->nVdbeRead > db->nVDestroy+1 ){
    rc = SQLITE_LOCKED;
    p->errorAction = OE_Abort;
    goto abort_due_to_error;
  }else{
    iDb = pOp->p3;
    assert( DbMaskTest(p->btreeMask, iDb) );
    iMoved = 0;  /* Not needed.  Only to silence a warning. */
    rc = sqlite3BtreeDropTable(db->aDb[iDb].pBt, pOp->p1, &iMoved);
    pOut->flags = MEM_Int;
    pOut->u.i = iMoved;
    if( rc ) goto abort_due_to_error;
#ifndef SQLITE_OMIT_AUTOVACUUM
    if( iMoved!=0 ){
      sqlite3RootPageMoved(db, iDb, iMoved, pOp->p1);
      /* All OP_Destroy operations occur on the same btree */
      assert( resetSchemaOnFault==0 || resetSchemaOnFault==iDb+1 );
      resetSchemaOnFault = iDb+1;
    }
#endif
  }
  break;
}

/* Opcode: Clear P1 P2 P3
**
** Delete all contents of the database table or index whose root page
** in the database file is given by P1.  But, unlike Destroy, do not
** remove the table or index from the database file.
**
** The table being clear is in the main database file if P2==0.  If
** P2==1 then the table to be clear is in the auxiliary database file
** that is used to store tables create using CREATE TEMPORARY TABLE.
**
** If the P3 value is non-zero, then the table referred to must be an
** intkey table (an SQL table, not an index). In this case the row change 
** count is incremented by the number of rows in the table being cleared. 
** If P3 is greater than zero, then the value stored in register P3 is
** also incremented by the number of rows in the table being cleared.
**
** See also: Destroy
*/
case OP_Clear: {
  int nChange;
 
  nChange = 0;
  assert( p->readOnly==0 );
  assert( DbMaskTest(p->btreeMask, pOp->p2) );
  rc = sqlite3BtreeClearTable(
      db->aDb[pOp->p2].pBt, pOp->p1, (pOp->p3 ? &nChange : 0)
  );
  if( pOp->p3 ){
    p->nChange += nChange;
    if( pOp->p3>0 ){
      assert( memIsValid(&aMem[pOp->p3]) );
      memAboutToChange(p, &aMem[pOp->p3]);
      aMem[pOp->p3].u.i += nChange;
    }
  }
  if( rc ) goto abort_due_to_error;
  break;
}

/* Opcode: ResetSorter P1 * * * *
**
** Delete all contents from the ephemeral table or sorter
** that is open on cursor P1.
**
** This opcode only works for cursors used for sorting and
** opened with OP_OpenEphemeral or OP_SorterOpen.
*/
case OP_ResetSorter: {
  VdbeCursor *pC;
 
  assert( pOp->p1>=0 && pOp->p1<p->nCursor );
  pC = p->apCsr[pOp->p1];
  assert( pC!=0 );
  if( isSorter(pC) ){
    sqlite3VdbeSorterReset(db, pC->uc.pSorter);
  }else{
    assert( pC->eCurType==CURTYPE_BTREE );
    assert( pC->isEphemeral );
    rc = sqlite3BtreeClearTableOfCursor(pC->uc.pCursor);
    if( rc ) goto abort_due_to_error;
  }
  break;
}

/* Opcode: CreateTable P1 P2 * * *
** Synopsis: r[P2]=root iDb=P1
**
** Allocate a new table in the main database file if P1==0 or in the
** auxiliary database file if P1==1 or in an attached database if
** P1>1.  Write the root page number of the new table into
** register P2
**
** The difference between a table and an index is this:  A table must
** have a 4-byte integer key and can have arbitrary data.  An index
** has an arbitrary key but no data.
**
** See also: CreateIndex
*/
/* Opcode: CreateIndex P1 P2 * * *
** Synopsis: r[P2]=root iDb=P1
**
** Allocate a new index in the main database file if P1==0 or in the
** auxiliary database file if P1==1 or in an attached database if
** P1>1.  Write the root page number of the new table into
** register P2.
**
** See documentation on OP_CreateTable for additional information.
*/
case OP_CreateIndex:            /* out2 */
case OP_CreateTable: {          /* out2 */
  int pgno;
  int flags;
  Db *pDb;

  pOut = out2Prerelease(p, pOp);
  pgno = 0;
  assert( pOp->p1>=0 && pOp->p1<db->nDb );
  assert( DbMaskTest(p->btreeMask, pOp->p1) );
  assert( p->readOnly==0 );
  pDb = &db->aDb[pOp->p1];
  assert( pDb->pBt!=0 );
  if( pOp->opcode==OP_CreateTable ){
    /* flags = BTREE_INTKEY; */
    flags = BTREE_INTKEY;
  }else{
    flags = BTREE_BLOBKEY;
  }
  rc = sqlite3BtreeCreateTable(pDb->pBt, &pgno, flags);
  if( rc ) goto abort_due_to_error;
  pOut->u.i = pgno;
  break;
}

/* Opcode: ParseSchema P1 * * P4 *
**
** Read and parse all entries from the SQLITE_MASTER table of database P1
** that match the WHERE clause P4. 
**
** This opcode invokes the parser to create a new virtual machine,
** then runs the new virtual machine.  It is thus a re-entrant opcode.
*/
case OP_ParseSchema: {
  int iDb;
  const char *zMaster;
  char *zSql;
  InitData initData;

  /* Any prepared statement that invokes this opcode will hold mutexes
  ** on every btree.  This is a prerequisite for invoking 
  ** sqlite3InitCallback().
  */
#ifdef SQLITE_DEBUG
  for(iDb=0; iDb<db->nDb; iDb++){
    assert( iDb==1 || sqlite3BtreeHoldsMutex(db->aDb[iDb].pBt) );
  }
#endif

  iDb = pOp->p1;
  assert( iDb>=0 && iDb<db->nDb );
  assert( DbHasProperty(db, iDb, DB_SchemaLoaded) );
  /* Used to be a conditional */ {
    zMaster = SCHEMA_TABLE(iDb);
    initData.db = db;
    initData.iDb = pOp->p1;
    initData.pzErrMsg = &p->zErrMsg;
    zSql = sqlite3MPrintf(db,
       "SELECT name, rootpage, sql FROM '%q'.%s WHERE %s ORDER BY rowid",
       db->aDb[iDb].zDbSName, zMaster, pOp->p4.z);
    if( zSql==0 ){
      rc = SQLITE_NOMEM_BKPT;
    }else{
      assert( db->init.busy==0 );
      db->init.busy = 1;
      initData.rc = SQLITE_OK;
      assert( !db->mallocFailed );
      rc = sqlite3_exec(db, zSql, sqlite3InitCallback, &initData, 0);
      if( rc==SQLITE_OK ) rc = initData.rc;
      sqlite3DbFree(db, zSql);
      db->init.busy = 0;
    }
  }
  if( rc ){
    sqlite3ResetAllSchemasOfConnection(db);
    if( rc==SQLITE_NOMEM ){
      goto no_mem;
    }
    goto abort_due_to_error;
  }
  break;  
}

#if !defined(SQLITE_OMIT_ANALYZE)
/* Opcode: LoadAnalysis P1 * * * *
**
** Read the sqlite_stat1 table for database P1 and load the content
** of that table into the internal index hash table.  This will cause
** the analysis to be used when preparing all subsequent queries.
*/
case OP_LoadAnalysis: {
  assert( pOp->p1>=0 && pOp->p1<db->nDb );
  rc = sqlite3AnalysisLoad(db, pOp->p1);
  if( rc ) goto abort_due_to_error;
  break;  
}
#endif /* !defined(SQLITE_OMIT_ANALYZE) */

/* Opcode: DropTable P1 * * P4 *
**
** Remove the internal (in-memory) data structures that describe
** the table named P4 in database P1.  This is called after a table
** is dropped from disk (using the Destroy opcode) in order to keep 
** the internal representation of the
** schema consistent with what is on disk.
*/
case OP_DropTable: {
  sqlite3UnlinkAndDeleteTable(db, pOp->p1, pOp->p4.z);
  break;
}

/* Opcode: DropIndex P1 * * P4 *
**
** Remove the internal (in-memory) data structures that describe
** the index named P4 in database P1.  This is called after an index
** is dropped from disk (using the Destroy opcode)
** in order to keep the internal representation of the
** schema consistent with what is on disk.
*/
case OP_DropIndex: {
  sqlite3UnlinkAndDeleteIndex(db, pOp->p1, pOp->p4.z);
  break;
}

/* Opcode: DropTrigger P1 * * P4 *
**
** Remove the internal (in-memory) data structures that describe
** the trigger named P4 in database P1.  This is called after a trigger
** is dropped from disk (using the Destroy opcode) in order to keep 
** the internal representation of the
** schema consistent with what is on disk.
*/
case OP_DropTrigger: {
  sqlite3UnlinkAndDeleteTrigger(db, pOp->p1, pOp->p4.z);
  break;
}


#ifndef SQLITE_OMIT_INTEGRITY_CHECK
/* Opcode: IntegrityCk P1 P2 P3 P4 P5
**
** Do an analysis of the currently open database.  Store in
** register P1 the text of an error message describing any problems.
** If no problems are found, store a NULL in register P1.
**
** The register P3 contains the maximum number of allowed errors.
** At most reg(P3) errors will be reported.
** In other words, the analysis stops as soon as reg(P1) errors are 
** seen.  Reg(P1) is updated with the number of errors remaining.
**
** The root page numbers of all tables in the database are integers
** stored in P4_INTARRAY argument.
**
** If P5 is not zero, the check is done on the auxiliary database
** file, not the main database file.
**
** This opcode is used to implement the integrity_check pragma.
*/
case OP_IntegrityCk: {
  int nRoot;      /* Number of tables to check.  (Number of root pages.) */
  int *aRoot;     /* Array of rootpage numbers for tables to be checked */
  int nErr;       /* Number of errors reported */
  char *z;        /* Text of the error report */
  Mem *pnErr;     /* Register keeping track of errors remaining */

  assert( p->bIsReader );
  nRoot = pOp->p2;
  aRoot = pOp->p4.ai;
  assert( nRoot>0 );
  assert( aRoot[nRoot]==0 );
  assert( pOp->p3>0 && pOp->p3<=(p->nMem+1 - p->nCursor) );
  pnErr = &aMem[pOp->p3];
  assert( (pnErr->flags & MEM_Int)!=0 );
  assert( (pnErr->flags & (MEM_Str|MEM_Blob))==0 );
  pIn1 = &aMem[pOp->p1];
  assert( pOp->p5<db->nDb );
  assert( DbMaskTest(p->btreeMask, pOp->p5) );
  z = sqlite3BtreeIntegrityCheck(db->aDb[pOp->p5].pBt, aRoot, nRoot,
                                 (int)pnErr->u.i, &nErr);
  pnErr->u.i -= nErr;
  sqlite3VdbeMemSetNull(pIn1);
  if( nErr==0 ){
    assert( z==0 );
  }else if( z==0 ){
    goto no_mem;
  }else{
    sqlite3VdbeMemSetStr(pIn1, z, -1, SQLITE_UTF8, sqlite3_free);
  }
  UPDATE_MAX_BLOBSIZE(pIn1);
  sqlite3VdbeChangeEncoding(pIn1, encoding);
  break;
}
#endif /* SQLITE_OMIT_INTEGRITY_CHECK */

/* Opcode: RowSetAdd P1 P2 * * *
** Synopsis: rowset(P1)=r[P2]
**
** Insert the integer value held by register P2 into a boolean index
** held in register P1.
**
** An assertion fails if P2 is not an integer.
*/
case OP_RowSetAdd: {       /* in1, in2 */
  pIn1 = &aMem[pOp->p1];
  pIn2 = &aMem[pOp->p2];
  assert( (pIn2->flags & MEM_Int)!=0 );
  if( (pIn1->flags & MEM_RowSet)==0 ){
    sqlite3VdbeMemSetRowSet(pIn1);
    if( (pIn1->flags & MEM_RowSet)==0 ) goto no_mem;
  }
  sqlite3RowSetInsert(pIn1->u.pRowSet, pIn2->u.i);
  break;
}

/* Opcode: RowSetRead P1 P2 P3 * *
** Synopsis: r[P3]=rowset(P1)
**
** Extract the smallest value from boolean index P1 and put that value into
** register P3.  Or, if boolean index P1 is initially empty, leave P3
** unchanged and jump to instruction P2.
*/
case OP_RowSetRead: {       /* jump, in1, out3 */
  i64 val;

  pIn1 = &aMem[pOp->p1];
  if( (pIn1->flags & MEM_RowSet)==0 
   || sqlite3RowSetNext(pIn1->u.pRowSet, &val)==0
  ){
    /* The boolean index is empty */
    sqlite3VdbeMemSetNull(pIn1);
    VdbeBranchTaken(1,2);
    goto jump_to_p2_and_check_for_interrupt;
  }else{
    /* A value was pulled from the index */
    VdbeBranchTaken(0,2);
    sqlite3VdbeMemSetInt64(&aMem[pOp->p3], val);
  }
  goto check_for_interrupt;
}

/* Opcode: RowSetTest P1 P2 P3 P4
** Synopsis: if r[P3] in rowset(P1) goto P2
**
** Register P3 is assumed to hold a 64-bit integer value. If register P1
** contains a RowSet object and that RowSet object contains
** the value held in P3, jump to register P2. Otherwise, insert the
** integer in P3 into the RowSet and continue on to the
** next opcode.
**
** The RowSet object is optimized for the case where successive sets
** of integers, where each set contains no duplicates. Each set
** of values is identified by a unique P4 value. The first set
** must have P4==0, the final set P4=-1.  P4 must be either -1 or
** non-negative.  For non-negative values of P4 only the lower 4
** bits are significant.
**
** This allows optimizations: (a) when P4==0 there is no need to test
** the rowset object for P3, as it is guaranteed not to contain it,
** (b) when P4==-1 there is no need to insert the value, as it will
** never be tested for, and (c) when a value that is part of set X is
** inserted, there is no need to search to see if the same value was
** previously inserted as part of set X (only if it was previously
** inserted as part of some other set).
*/
case OP_RowSetTest: {                     /* jump, in1, in3 */
  int iSet;
  int exists;

  pIn1 = &aMem[pOp->p1];
  pIn3 = &aMem[pOp->p3];
  iSet = pOp->p4.i;
  assert( pIn3->flags&MEM_Int );

  /* If there is anything other than a rowset object in memory cell P1,
  ** delete it now and initialize P1 with an empty rowset
  */
  if( (pIn1->flags & MEM_RowSet)==0 ){
    sqlite3VdbeMemSetRowSet(pIn1);
    if( (pIn1->flags & MEM_RowSet)==0 ) goto no_mem;
  }

  assert( pOp->p4type==P4_INT32 );
  assert( iSet==-1 || iSet>=0 );
  if( iSet ){
    exists = sqlite3RowSetTest(pIn1->u.pRowSet, iSet, pIn3->u.i);
    VdbeBranchTaken(exists!=0,2);
    if( exists ) goto jump_to_p2;
  }
  if( iSet>=0 ){
    sqlite3RowSetInsert(pIn1->u.pRowSet, pIn3->u.i);
  }
  break;
}


#ifndef SQLITE_OMIT_TRIGGER

/* Opcode: Program P1 P2 P3 P4 P5
**
** Execute the trigger program passed as P4 (type P4_SUBPROGRAM). 
**
** P1 contains the address of the memory cell that contains the first memory 
** cell in an array of values used as arguments to the sub-program. P2 
** contains the address to jump to if the sub-program throws an IGNORE 
** exception using the RAISE() function. Register P3 contains the address 
** of a memory cell in this (the parent) VM that is used to allocate the 
** memory required by the sub-vdbe at runtime.
**
** P4 is a pointer to the VM containing the trigger program.
**
** If P5 is non-zero, then recursive program invocation is enabled.
*/
case OP_Program: {        /* jump */
  int nMem;               /* Number of memory registers for sub-program */
  int nByte;              /* Bytes of runtime space required for sub-program */
  Mem *pRt;               /* Register to allocate runtime space */
  Mem *pMem;              /* Used to iterate through memory cells */
  Mem *pEnd;              /* Last memory cell in new array */
  VdbeFrame *pFrame;      /* New vdbe frame to execute in */
  SubProgram *pProgram;   /* Sub-program to execute */
  void *t;                /* Token identifying trigger */

  pProgram = pOp->p4.pProgram;
  pRt = &aMem[pOp->p3];
  assert( pProgram->nOp>0 );
  
  /* If the p5 flag is clear, then recursive invocation of triggers is 
  ** disabled for backwards compatibility (p5 is set if this sub-program
  ** is really a trigger, not a foreign key action, and the flag set
  ** and cleared by the "PRAGMA recursive_triggers" command is clear).
  ** 
  ** It is recursive invocation of triggers, at the SQL level, that is 
  ** disabled. In some cases a single trigger may generate more than one 
  ** SubProgram (if the trigger may be executed with more than one different 
  ** ON CONFLICT algorithm). SubProgram structures associated with a
  ** single trigger all have the same value for the SubProgram.token 
  ** variable.  */
  if( pOp->p5 ){
    t = pProgram->token;
    for(pFrame=p->pFrame; pFrame && pFrame->token!=t; pFrame=pFrame->pParent);
    if( pFrame ) break;
  }

  if( p->nFrame>=db->aLimit[SQLITE_LIMIT_TRIGGER_DEPTH] ){
    rc = SQLITE_ERROR;
    sqlite3VdbeError(p, "too many levels of trigger recursion");
    goto abort_due_to_error;
  }

  /* Register pRt is used to store the memory required to save the state
  ** of the current program, and the memory required at runtime to execute
  ** the trigger program. If this trigger has been fired before, then pRt 
  ** is already allocated. Otherwise, it must be initialized.  */
  if( (pRt->flags&MEM_Frame)==0 ){
    /* SubProgram.nMem is set to the number of memory cells used by the 
    ** program stored in SubProgram.aOp. As well as these, one memory
    ** cell is required for each cursor used by the program. Set local
    ** variable nMem (and later, VdbeFrame.nChildMem) to this value.
    */
    nMem = pProgram->nMem + pProgram->nCsr;
    assert( nMem>0 );
    if( pProgram->nCsr==0 ) nMem++;
    nByte = ROUND8(sizeof(VdbeFrame))
              + nMem * sizeof(Mem)
              + pProgram->nCsr * sizeof(VdbeCursor *);
    pFrame = sqlite3DbMallocZero(db, nByte);
    if( !pFrame ){
      goto no_mem;
    }
    sqlite3VdbeMemRelease(pRt);
    pRt->flags = MEM_Frame;
    pRt->u.pFrame = pFrame;

    pFrame->v = p;
    pFrame->nChildMem = nMem;
    pFrame->nChildCsr = pProgram->nCsr;
    pFrame->pc = (int)(pOp - aOp);
    pFrame->aMem = p->aMem;
    pFrame->nMem = p->nMem;
    pFrame->apCsr = p->apCsr;
    pFrame->nCursor = p->nCursor;
    pFrame->aOp = p->aOp;
    pFrame->nOp = p->nOp;
    pFrame->token = pProgram->token;
#ifdef SQLITE_ENABLE_STMT_SCANSTATUS
    pFrame->anExec = p->anExec;
#endif

    pEnd = &VdbeFrameMem(pFrame)[pFrame->nChildMem];
    for(pMem=VdbeFrameMem(pFrame); pMem!=pEnd; pMem++){
      pMem->flags = MEM_Undefined;
      pMem->db = db;
    }
  }else{
    pFrame = pRt->u.pFrame;
    assert( pProgram->nMem+pProgram->nCsr==pFrame->nChildMem 
        || (pProgram->nCsr==0 && pProgram->nMem+1==pFrame->nChildMem) );
    assert( pProgram->nCsr==pFrame->nChildCsr );
    assert( (int)(pOp - aOp)==pFrame->pc );
  }

  p->nFrame++;
  pFrame->pParent = p->pFrame;
  pFrame->lastRowid = lastRowid;
  pFrame->nChange = p->nChange;
  pFrame->nDbChange = p->db->nChange;
  assert( pFrame->pAuxData==0 );
  pFrame->pAuxData = p->pAuxData;
  p->pAuxData = 0;
  p->nChange = 0;
  p->pFrame = pFrame;
  p->aMem = aMem = VdbeFrameMem(pFrame);
  p->nMem = pFrame->nChildMem;
  p->nCursor = (u16)pFrame->nChildCsr;
  p->apCsr = (VdbeCursor **)&aMem[p->nMem];
  p->aOp = aOp = pProgram->aOp;
  p->nOp = pProgram->nOp;
#ifdef SQLITE_ENABLE_STMT_SCANSTATUS
  p->anExec = 0;
#endif
  pOp = &aOp[-1];

  break;
}

/* Opcode: Param P1 P2 * * *
**
** This opcode is only ever present in sub-programs called via the 
** OP_Program instruction. Copy a value currently stored in a memory 
** cell of the calling (parent) frame to cell P2 in the current frames 
** address space. This is used by trigger programs to access the new.* 
** and old.* values.
**
** The address of the cell in the parent frame is determined by adding
** the value of the P1 argument to the value of the P1 argument to the
** calling OP_Program instruction.
*/
case OP_Param: {           /* out2 */
  VdbeFrame *pFrame;
  Mem *pIn;
  pOut = out2Prerelease(p, pOp);
  pFrame = p->pFrame;
  pIn = &pFrame->aMem[pOp->p1 + pFrame->aOp[pFrame->pc].p1];   
  sqlite3VdbeMemShallowCopy(pOut, pIn, MEM_Ephem);
  break;
}

#endif /* #ifndef SQLITE_OMIT_TRIGGER */

#ifndef SQLITE_OMIT_FOREIGN_KEY
/* Opcode: FkCounter P1 P2 * * *
** Synopsis: fkctr[P1]+=P2
**
** Increment a "constraint counter" by P2 (P2 may be negative or positive).
** If P1 is non-zero, the database constraint counter is incremented 
** (deferred foreign key constraints). Otherwise, if P1 is zero, the 
** statement counter is incremented (immediate foreign key constraints).
*/
case OP_FkCounter: {
  if( db->flags & SQLITE_DeferFKs ){
    db->nDeferredImmCons += pOp->p2;
  }else if( pOp->p1 ){
    db->nDeferredCons += pOp->p2;
  }else{
    p->nFkConstraint += pOp->p2;
  }
  break;
}

/* Opcode: FkIfZero P1 P2 * * *
** Synopsis: if fkctr[P1]==0 goto P2
**
** This opcode tests if a foreign key constraint-counter is currently zero.
** If so, jump to instruction P2. Otherwise, fall through to the next 
** instruction.
**
** If P1 is non-zero, then the jump is taken if the database constraint-counter
** is zero (the one that counts deferred constraint violations). If P1 is
** zero, the jump is taken if the statement constraint-counter is zero
** (immediate foreign key constraint violations).
*/
case OP_FkIfZero: {         /* jump */
  if( pOp->p1 ){
    VdbeBranchTaken(db->nDeferredCons==0 && db->nDeferredImmCons==0, 2);
    if( db->nDeferredCons==0 && db->nDeferredImmCons==0 ) goto jump_to_p2;
  }else{
    VdbeBranchTaken(p->nFkConstraint==0 && db->nDeferredImmCons==0, 2);
    if( p->nFkConstraint==0 && db->nDeferredImmCons==0 ) goto jump_to_p2;
  }
  break;
}
#endif /* #ifndef SQLITE_OMIT_FOREIGN_KEY */

#ifndef SQLITE_OMIT_AUTOINCREMENT
/* Opcode: MemMax P1 P2 * * *
** Synopsis: r[P1]=max(r[P1],r[P2])
**
** P1 is a register in the root frame of this VM (the root frame is
** different from the current frame if this instruction is being executed
** within a sub-program). Set the value of register P1 to the maximum of 
** its current value and the value in register P2.
**
** This instruction throws an error if the memory cell is not initially
** an integer.
*/
case OP_MemMax: {        /* in2 */
  VdbeFrame *pFrame;
  if( p->pFrame ){
    for(pFrame=p->pFrame; pFrame->pParent; pFrame=pFrame->pParent);
    pIn1 = &pFrame->aMem[pOp->p1];
  }else{
    pIn1 = &aMem[pOp->p1];
  }
  assert( memIsValid(pIn1) );
  sqlite3VdbeMemIntegerify(pIn1);
  pIn2 = &aMem[pOp->p2];
  sqlite3VdbeMemIntegerify(pIn2);
  if( pIn1->u.i<pIn2->u.i){
    pIn1->u.i = pIn2->u.i;
  }
  break;
}
#endif /* SQLITE_OMIT_AUTOINCREMENT */

/* Opcode: IfPos P1 P2 P3 * *
** Synopsis: if r[P1]>0 then r[P1]-=P3, goto P2
**
** Register P1 must contain an integer.
** If the value of register P1 is 1 or greater, subtract P3 from the
** value in P1 and jump to P2.
**
** If the initial value of register P1 is less than 1, then the
** value is unchanged and control passes through to the next instruction.
*/
case OP_IfPos: {        /* jump, in1 */
  pIn1 = &aMem[pOp->p1];
  assert( pIn1->flags&MEM_Int );
  VdbeBranchTaken( pIn1->u.i>0, 2);
  if( pIn1->u.i>0 ){
    pIn1->u.i -= pOp->p3;
    goto jump_to_p2;
  }
  break;
}

/* Opcode: OffsetLimit P1 P2 P3 * *
** Synopsis: if r[P1]>0 then r[P2]=r[P1]+max(0,r[P3]) else r[P2]=(-1)
**
** This opcode performs a commonly used computation associated with
** LIMIT and OFFSET process.  r[P1] holds the limit counter.  r[P3]
** holds the offset counter.  The opcode computes the combined value
** of the LIMIT and OFFSET and stores that value in r[P2].  The r[P2]
** value computed is the total number of rows that will need to be
** visited in order to complete the query.
**
** If r[P3] is zero or negative, that means there is no OFFSET
** and r[P2] is set to be the value of the LIMIT, r[P1].
**
** if r[P1] is zero or negative, that means there is no LIMIT
** and r[P2] is set to -1. 
**
** Otherwise, r[P2] is set to the sum of r[P1] and r[P3].
*/
case OP_OffsetLimit: {    /* in1, out2, in3 */
  pIn1 = &aMem[pOp->p1];
  pIn3 = &aMem[pOp->p3];
  pOut = out2Prerelease(p, pOp);
  assert( pIn1->flags & MEM_Int );
  assert( pIn3->flags & MEM_Int );
  pOut->u.i = pIn1->u.i<=0 ? -1 : pIn1->u.i+(pIn3->u.i>0?pIn3->u.i:0);
  break;
}

/* Opcode: IfNotZero P1 P2 P3 * *
** Synopsis: if r[P1]!=0 then r[P1]-=P3, goto P2
**
** Register P1 must contain an integer.  If the content of register P1 is
** initially nonzero, then subtract P3 from the value in register P1 and
** jump to P2.  If register P1 is initially zero, leave it unchanged
** and fall through.
*/
case OP_IfNotZero: {        /* jump, in1 */
  pIn1 = &aMem[pOp->p1];
  assert( pIn1->flags&MEM_Int );
  VdbeBranchTaken(pIn1->u.i<0, 2);
  if( pIn1->u.i ){
     pIn1->u.i -= pOp->p3;
     goto jump_to_p2;
  }
  break;
}

/* Opcode: DecrJumpZero P1 P2 * * *
** Synopsis: if (--r[P1])==0 goto P2
**
** Register P1 must hold an integer.  Decrement the value in register P1
** then jump to P2 if the new value is exactly zero.
*/
case OP_DecrJumpZero: {      /* jump, in1 */
  pIn1 = &aMem[pOp->p1];
  assert( pIn1->flags&MEM_Int );
  pIn1->u.i--;
  VdbeBranchTaken(pIn1->u.i==0, 2);
  if( pIn1->u.i==0 ) goto jump_to_p2;
  break;
}


/* Opcode: AggStep0 * P2 P3 P4 P5
** Synopsis: accum=r[P3] step(r[P2@P5])
**
** Execute the step function for an aggregate.  The
** function has P5 arguments.   P4 is a pointer to the FuncDef
** structure that specifies the function.  Register P3 is the
** accumulator.
**
** The P5 arguments are taken from register P2 and its
** successors.
*/
/* Opcode: AggStep * P2 P3 P4 P5
** Synopsis: accum=r[P3] step(r[P2@P5])
**
** Execute the step function for an aggregate.  The
** function has P5 arguments.   P4 is a pointer to an sqlite3_context
** object that is used to run the function.  Register P3 is
** as the accumulator.
**
** The P5 arguments are taken from register P2 and its
** successors.
**
** This opcode is initially coded as OP_AggStep0.  On first evaluation,
** the FuncDef stored in P4 is converted into an sqlite3_context and
** the opcode is changed.  In this way, the initialization of the
** sqlite3_context only happens once, instead of on each call to the
** step function.
*/
case OP_AggStep0: {
  int n;
  sqlite3_context *pCtx;

  assert( pOp->p4type==P4_FUNCDEF );
  n = pOp->p5;
  assert( pOp->p3>0 && pOp->p3<=(p->nMem+1 - p->nCursor) );
  assert( n==0 || (pOp->p2>0 && pOp->p2+n<=(p->nMem+1 - p->nCursor)+1) );
  assert( pOp->p3<pOp->p2 || pOp->p3>=pOp->p2+n );
  pCtx = sqlite3DbMallocRawNN(db, sizeof(*pCtx) + (n-1)*sizeof(sqlite3_value*));
  if( pCtx==0 ) goto no_mem;
  pCtx->pMem = 0;
  pCtx->pFunc = pOp->p4.pFunc;
  pCtx->iOp = (int)(pOp - aOp);
  pCtx->pVdbe = p;
  pCtx->argc = n;
  pOp->p4type = P4_FUNCCTX;
  pOp->p4.pCtx = pCtx;
  pOp->opcode = OP_AggStep;
  /* Fall through into OP_AggStep */
}
case OP_AggStep: {
  int i;
  sqlite3_context *pCtx;
  Mem *pMem;
  Mem t;

  assert( pOp->p4type==P4_FUNCCTX );
  pCtx = pOp->p4.pCtx;
  pMem = &aMem[pOp->p3];

  /* If this function is inside of a trigger, the register array in aMem[]
  ** might change from one evaluation to the next.  The next block of code
  ** checks to see if the register array has changed, and if so it
  ** reinitializes the relavant parts of the sqlite3_context object */
  if( pCtx->pMem != pMem ){
    pCtx->pMem = pMem;
    for(i=pCtx->argc-1; i>=0; i--) pCtx->argv[i] = &aMem[pOp->p2+i];
  }

#ifdef SQLITE_DEBUG
  for(i=0; i<pCtx->argc; i++){
    assert( memIsValid(pCtx->argv[i]) );
    REGISTER_TRACE(pOp->p2+i, pCtx->argv[i]);
  }
#endif

  pMem->n++;
  sqlite3VdbeMemInit(&t, db, MEM_Null);
  pCtx->pOut = &t;
  pCtx->fErrorOrAux = 0;
  pCtx->skipFlag = 0;
  (pCtx->pFunc->xSFunc)(pCtx,pCtx->argc,pCtx->argv); /* IMP: R-24505-23230 */
  if( pCtx->fErrorOrAux ){
    if( pCtx->isError ){
      sqlite3VdbeError(p, "%s", sqlite3_value_text(&t));
      rc = pCtx->isError;
    }
    sqlite3VdbeMemRelease(&t);
    if( rc ) goto abort_due_to_error;
  }else{
    assert( t.flags==MEM_Null );
  }
  if( pCtx->skipFlag ){
    assert( pOp[-1].opcode==OP_CollSeq );
    i = pOp[-1].p1;
    if( i ) sqlite3VdbeMemSetInt64(&aMem[i], 1);
  }
  break;
}

/* Opcode: AggFinal P1 P2 * P4 *
** Synopsis: accum=r[P1] N=P2
**
** Execute the finalizer function for an aggregate.  P1 is
** the memory location that is the accumulator for the aggregate.
**
** P2 is the number of arguments that the step function takes and
** P4 is a pointer to the FuncDef for this function.  The P2
** argument is not used by this opcode.  It is only there to disambiguate
** functions that can take varying numbers of arguments.  The
** P4 argument is only needed for the degenerate case where
** the step function was not previously called.
*/
case OP_AggFinal: {
  Mem *pMem;
  assert( pOp->p1>0 && pOp->p1<=(p->nMem+1 - p->nCursor) );
  pMem = &aMem[pOp->p1];
  assert( (pMem->flags & ~(MEM_Null|MEM_Agg))==0 );
  rc = sqlite3VdbeMemFinalize(pMem, pOp->p4.pFunc);
  if( rc ){
    sqlite3VdbeError(p, "%s", sqlite3_value_text(pMem));
    goto abort_due_to_error;
  }
  sqlite3VdbeChangeEncoding(pMem, encoding);
  UPDATE_MAX_BLOBSIZE(pMem);
  if( sqlite3VdbeMemTooBig(pMem) ){
    goto too_big;
  }
  break;
}

#ifndef SQLITE_OMIT_WAL
/* Opcode: Checkpoint P1 P2 P3 * *
**
** Checkpoint database P1. This is a no-op if P1 is not currently in
** WAL mode. Parameter P2 is one of SQLITE_CHECKPOINT_PASSIVE, FULL,
** RESTART, or TRUNCATE.  Write 1 or 0 into mem[P3] if the checkpoint returns
** SQLITE_BUSY or not, respectively.  Write the number of pages in the
** WAL after the checkpoint into mem[P3+1] and the number of pages
** in the WAL that have been checkpointed after the checkpoint
** completes into mem[P3+2].  However on an error, mem[P3+1] and
** mem[P3+2] are initialized to -1.
*/
case OP_Checkpoint: {
  int i;                          /* Loop counter */
  int aRes[3];                    /* Results */
  Mem *pMem;                      /* Write results here */

  assert( p->readOnly==0 );
  aRes[0] = 0;
  aRes[1] = aRes[2] = -1;
  assert( pOp->p2==SQLITE_CHECKPOINT_PASSIVE
       || pOp->p2==SQLITE_CHECKPOINT_FULL
       || pOp->p2==SQLITE_CHECKPOINT_RESTART
       || pOp->p2==SQLITE_CHECKPOINT_TRUNCATE
  );
  rc = sqlite3Checkpoint(db, pOp->p1, pOp->p2, &aRes[1], &aRes[2]);
  if( rc ){
    if( rc!=SQLITE_BUSY ) goto abort_due_to_error;
    rc = SQLITE_OK;
    aRes[0] = 1;
  }
  for(i=0, pMem = &aMem[pOp->p3]; i<3; i++, pMem++){
    sqlite3VdbeMemSetInt64(pMem, (i64)aRes[i]);
  }    
  break;
};  
#endif

#ifndef SQLITE_OMIT_PRAGMA
/* Opcode: JournalMode P1 P2 P3 * *
**
** Change the journal mode of database P1 to P3. P3 must be one of the
** PAGER_JOURNALMODE_XXX values. If changing between the various rollback
** modes (delete, truncate, persist, off and memory), this is a simple
** operation. No IO is required.
**
** If changing into or out of WAL mode the procedure is more complicated.
**
** Write a string containing the final journal-mode to register P2.
*/
case OP_JournalMode: {    /* out2 */
  Btree *pBt;                     /* Btree to change journal mode of */
  Pager *pPager;                  /* Pager associated with pBt */
  int eNew;                       /* New journal mode */
  int eOld;                       /* The old journal mode */
#ifndef SQLITE_OMIT_WAL
  const char *zFilename;          /* Name of database file for pPager */
#endif

  pOut = out2Prerelease(p, pOp);
  eNew = pOp->p3;
  assert( eNew==PAGER_JOURNALMODE_DELETE 
       || eNew==PAGER_JOURNALMODE_TRUNCATE 
       || eNew==PAGER_JOURNALMODE_PERSIST 
       || eNew==PAGER_JOURNALMODE_OFF
       || eNew==PAGER_JOURNALMODE_MEMORY
       || eNew==PAGER_JOURNALMODE_WAL
       || eNew==PAGER_JOURNALMODE_QUERY
  );
  assert( pOp->p1>=0 && pOp->p1<db->nDb );
  assert( p->readOnly==0 );

  pBt = db->aDb[pOp->p1].pBt;
  pPager = sqlite3BtreePager(pBt);
  eOld = sqlite3PagerGetJournalMode(pPager);
  if( eNew==PAGER_JOURNALMODE_QUERY ) eNew = eOld;
  if( !sqlite3PagerOkToChangeJournalMode(pPager) ) eNew = eOld;

#ifndef SQLITE_OMIT_WAL
  zFilename = sqlite3PagerFilename(pPager, 1);

  /* Do not allow a transition to journal_mode=WAL for a database
  ** in temporary storage or if the VFS does not support shared memory 
  */
  if( eNew==PAGER_JOURNALMODE_WAL
   && (sqlite3Strlen30(zFilename)==0           /* Temp file */
       || !sqlite3PagerWalSupported(pPager))   /* No shared-memory support */
  ){
    eNew = eOld;
  }

  if( (eNew!=eOld)
   && (eOld==PAGER_JOURNALMODE_WAL || eNew==PAGER_JOURNALMODE_WAL)
  ){
    if( !db->autoCommit || db->nVdbeRead>1 ){
      rc = SQLITE_ERROR;
      sqlite3VdbeError(p,
          "cannot change %s wal mode from within a transaction",
          (eNew==PAGER_JOURNALMODE_WAL ? "into" : "out of")
      );
      goto abort_due_to_error;
    }else{
 
      if( eOld==PAGER_JOURNALMODE_WAL ){
        /* If leaving WAL mode, close the log file. If successful, the call
        ** to PagerCloseWal() checkpoints and deletes the write-ahead-log 
        ** file. An EXCLUSIVE lock may still be held on the database file 
        ** after a successful return. 
        */
        rc = sqlite3PagerCloseWal(pPager);
        if( rc==SQLITE_OK ){
          sqlite3PagerSetJournalMode(pPager, eNew);
        }
      }else if( eOld==PAGER_JOURNALMODE_MEMORY ){
        /* Cannot transition directly from MEMORY to WAL.  Use mode OFF
        ** as an intermediate */
        sqlite3PagerSetJournalMode(pPager, PAGER_JOURNALMODE_OFF);
      }
  
      /* Open a transaction on the database file. Regardless of the journal
      ** mode, this transaction always uses a rollback journal.
      */
      assert( sqlite3BtreeIsInTrans(pBt)==0 );
      if( rc==SQLITE_OK ){
        rc = sqlite3BtreeSetVersion(pBt, (eNew==PAGER_JOURNALMODE_WAL ? 2 : 1));
      }
    }
  }
#endif /* ifndef SQLITE_OMIT_WAL */

  if( rc ) eNew = eOld;
  eNew = sqlite3PagerSetJournalMode(pPager, eNew);

  pOut->flags = MEM_Str|MEM_Static|MEM_Term;
  pOut->z = (char *)sqlite3JournalModename(eNew);
  pOut->n = sqlite3Strlen30(pOut->z);
  pOut->enc = SQLITE_UTF8;
  sqlite3VdbeChangeEncoding(pOut, encoding);
  if( rc ) goto abort_due_to_error;
  break;
};
#endif /* SQLITE_OMIT_PRAGMA */

#if !defined(SQLITE_OMIT_VACUUM) && !defined(SQLITE_OMIT_ATTACH)
/* Opcode: Vacuum P1 * * * *
**
** Vacuum the entire database P1.  P1 is 0 for "main", and 2 or more
** for an attached database.  The "temp" database may not be vacuumed.
*/
case OP_Vacuum: {
  assert( p->readOnly==0 );
  rc = sqlite3RunVacuum(&p->zErrMsg, db, pOp->p1);
  if( rc ) goto abort_due_to_error;
  break;
}
#endif

#if !defined(SQLITE_OMIT_AUTOVACUUM)
/* Opcode: IncrVacuum P1 P2 * * *
**
** Perform a single step of the incremental vacuum procedure on
** the P1 database. If the vacuum has finished, jump to instruction
** P2. Otherwise, fall through to the next instruction.
*/
case OP_IncrVacuum: {        /* jump */
  Btree *pBt;

  assert( pOp->p1>=0 && pOp->p1<db->nDb );
  assert( DbMaskTest(p->btreeMask, pOp->p1) );
  assert( p->readOnly==0 );
  pBt = db->aDb[pOp->p1].pBt;
  rc = sqlite3BtreeIncrVacuum(pBt);
  VdbeBranchTaken(rc==SQLITE_DONE,2);
  if( rc ){
    if( rc!=SQLITE_DONE ) goto abort_due_to_error;
    rc = SQLITE_OK;
    goto jump_to_p2;
  }
  break;
}
#endif

/* Opcode: Expire P1 * * * *
**
** Cause precompiled statements to expire.  When an expired statement
** is executed using sqlite3_step() it will either automatically
** reprepare itself (if it was originally created using sqlite3_prepare_v2())
** or it will fail with SQLITE_SCHEMA.
** 
** If P1 is 0, then all SQL statements become expired. If P1 is non-zero,
** then only the currently executing statement is expired.
*/
case OP_Expire: {
  if( !pOp->p1 ){
    sqlite3ExpirePreparedStatements(db);
  }else{
    p->expired = 1;
  }
  break;
}

#ifndef SQLITE_OMIT_SHARED_CACHE
/* Opcode: TableLock P1 P2 P3 P4 *
** Synopsis: iDb=P1 root=P2 write=P3
**
** Obtain a lock on a particular table. This instruction is only used when
** the shared-cache feature is enabled. 
**
** P1 is the index of the database in sqlite3.aDb[] of the database
** on which the lock is acquired.  A readlock is obtained if P3==0 or
** a write lock if P3==1.
**
** P2 contains the root-page of the table to lock.
**
** P4 contains a pointer to the name of the table being locked. This is only
** used to generate an error message if the lock cannot be obtained.
*/
case OP_TableLock: {
  u8 isWriteLock = (u8)pOp->p3;
  if( isWriteLock || 0==(db->flags&SQLITE_ReadUncommitted) ){
    int p1 = pOp->p1; 
    assert( p1>=0 && p1<db->nDb );
    assert( DbMaskTest(p->btreeMask, p1) );
    assert( isWriteLock==0 || isWriteLock==1 );
    rc = sqlite3BtreeLockTable(db->aDb[p1].pBt, pOp->p2, isWriteLock);
    if( rc ){
      if( (rc&0xFF)==SQLITE_LOCKED ){
        const char *z = pOp->p4.z;
        sqlite3VdbeError(p, "database table is locked: %s", z);
      }
      goto abort_due_to_error;
    }
  }
  break;
}
#endif /* SQLITE_OMIT_SHARED_CACHE */

#ifndef SQLITE_OMIT_VIRTUALTABLE
/* Opcode: VBegin * * * P4 *
**
** P4 may be a pointer to an sqlite3_vtab structure. If so, call the 
** xBegin method for that table.
**
** Also, whether or not P4 is set, check that this is not being called from
** within a callback to a virtual table xSync() method. If it is, the error
** code will be set to SQLITE_LOCKED.
*/
case OP_VBegin: {
  VTable *pVTab;
  pVTab = pOp->p4.pVtab;
  rc = sqlite3VtabBegin(db, pVTab);
  if( pVTab ) sqlite3VtabImportErrmsg(p, pVTab->pVtab);
  if( rc ) goto abort_due_to_error;
  break;
}
#endif /* SQLITE_OMIT_VIRTUALTABLE */

#ifndef SQLITE_OMIT_VIRTUALTABLE
/* Opcode: VCreate P1 P2 * * *
**
** P2 is a register that holds the name of a virtual table in database 
** P1. Call the xCreate method for that table.
*/
case OP_VCreate: {
  Mem sMem;          /* For storing the record being decoded */
  const char *zTab;  /* Name of the virtual table */

  memset(&sMem, 0, sizeof(sMem));
  sMem.db = db;
  /* Because P2 is always a static string, it is impossible for the
  ** sqlite3VdbeMemCopy() to fail */
  assert( (aMem[pOp->p2].flags & MEM_Str)!=0 );
  assert( (aMem[pOp->p2].flags & MEM_Static)!=0 );
  rc = sqlite3VdbeMemCopy(&sMem, &aMem[pOp->p2]);
  assert( rc==SQLITE_OK );
  zTab = (const char*)sqlite3_value_text(&sMem);
  assert( zTab || db->mallocFailed );
  if( zTab ){
    rc = sqlite3VtabCallCreate(db, pOp->p1, zTab, &p->zErrMsg);
  }
  sqlite3VdbeMemRelease(&sMem);
  if( rc ) goto abort_due_to_error;
  break;
}
#endif /* SQLITE_OMIT_VIRTUALTABLE */

#ifndef SQLITE_OMIT_VIRTUALTABLE
/* Opcode: VDestroy P1 * * P4 *
**
** P4 is the name of a virtual table in database P1.  Call the xDestroy method
** of that table.
*/
case OP_VDestroy: {
  db->nVDestroy++;
  rc = sqlite3VtabCallDestroy(db, pOp->p1, pOp->p4.z);
  db->nVDestroy--;
  if( rc ) goto abort_due_to_error;
  break;
}
#endif /* SQLITE_OMIT_VIRTUALTABLE */

#ifndef SQLITE_OMIT_VIRTUALTABLE
/* Opcode: VOpen P1 * * P4 *
**
** P4 is a pointer to a virtual table object, an sqlite3_vtab structure.
** P1 is a cursor number.  This opcode opens a cursor to the virtual
** table and stores that cursor in P1.
*/
case OP_VOpen: {
  VdbeCursor *pCur;
  sqlite3_vtab_cursor *pVCur;
  sqlite3_vtab *pVtab;
  const sqlite3_module *pModule;

  assert( p->bIsReader );
  pCur = 0;
  pVCur = 0;
  pVtab = pOp->p4.pVtab->pVtab;
  if( pVtab==0 || NEVER(pVtab->pModule==0) ){
    rc = SQLITE_LOCKED;
    goto abort_due_to_error;
  }
  pModule = pVtab->pModule;
  rc = pModule->xOpen(pVtab, &pVCur);
  sqlite3VtabImportErrmsg(p, pVtab);
  if( rc ) goto abort_due_to_error;

  /* Initialize sqlite3_vtab_cursor base class */
  pVCur->pVtab = pVtab;

  /* Initialize vdbe cursor object */
  pCur = allocateCursor(p, pOp->p1, 0, -1, CURTYPE_VTAB);
  if( pCur ){
    pCur->uc.pVCur = pVCur;
    pVtab->nRef++;
  }else{
    assert( db->mallocFailed );
    pModule->xClose(pVCur);
    goto no_mem;
  }
  break;
}
#endif /* SQLITE_OMIT_VIRTUALTABLE */

#ifndef SQLITE_OMIT_VIRTUALTABLE
/* Opcode: VFilter P1 P2 P3 P4 *
** Synopsis: iplan=r[P3] zplan='P4'
**
** P1 is a cursor opened using VOpen.  P2 is an address to jump to if
** the filtered result set is empty.
**
** P4 is either NULL or a string that was generated by the xBestIndex
** method of the module.  The interpretation of the P4 string is left
** to the module implementation.
**
** This opcode invokes the xFilter method on the virtual table specified
** by P1.  The integer query plan parameter to xFilter is stored in register
** P3. Register P3+1 stores the argc parameter to be passed to the
** xFilter method. Registers P3+2..P3+1+argc are the argc
** additional parameters which are passed to
** xFilter as argv. Register P3+2 becomes argv[0] when passed to xFilter.
**
** A jump is made to P2 if the result set after filtering would be empty.
*/
case OP_VFilter: {   /* jump */
  int nArg;
  int iQuery;
  const sqlite3_module *pModule;
  Mem *pQuery;
  Mem *pArgc;
  sqlite3_vtab_cursor *pVCur;
  sqlite3_vtab *pVtab;
  VdbeCursor *pCur;
  int res;
  int i;
  Mem **apArg;

  pQuery = &aMem[pOp->p3];
  pArgc = &pQuery[1];
  pCur = p->apCsr[pOp->p1];
  assert( memIsValid(pQuery) );
  REGISTER_TRACE(pOp->p3, pQuery);
  assert( pCur->eCurType==CURTYPE_VTAB );
  pVCur = pCur->uc.pVCur;
  pVtab = pVCur->pVtab;
  pModule = pVtab->pModule;

  /* Grab the index number and argc parameters */
  assert( (pQuery->flags&MEM_Int)!=0 && pArgc->flags==MEM_Int );
  nArg = (int)pArgc->u.i;
  iQuery = (int)pQuery->u.i;

  /* Invoke the xFilter method */
  res = 0;
  apArg = p->apArg;
  for(i = 0; i<nArg; i++){
    apArg[i] = &pArgc[i+1];
  }
  rc = pModule->xFilter(pVCur, iQuery, pOp->p4.z, nArg, apArg);
  sqlite3VtabImportErrmsg(p, pVtab);
  if( rc ) goto abort_due_to_error;
  res = pModule->xEof(pVCur);
  pCur->nullRow = 0;
  VdbeBranchTaken(res!=0,2);
  if( res ) goto jump_to_p2;
  break;
}
#endif /* SQLITE_OMIT_VIRTUALTABLE */

#ifndef SQLITE_OMIT_VIRTUALTABLE
/* Opcode: VColumn P1 P2 P3 * *
** Synopsis: r[P3]=vcolumn(P2)
**
** Store the value of the P2-th column of
** the row of the virtual-table that the 
** P1 cursor is pointing to into register P3.
*/
case OP_VColumn: {
  sqlite3_vtab *pVtab;
  const sqlite3_module *pModule;
  Mem *pDest;
  sqlite3_context sContext;

  VdbeCursor *pCur = p->apCsr[pOp->p1];
  assert( pCur->eCurType==CURTYPE_VTAB );
  assert( pOp->p3>0 && pOp->p3<=(p->nMem+1 - p->nCursor) );
  pDest = &aMem[pOp->p3];
  memAboutToChange(p, pDest);
  if( pCur->nullRow ){
    sqlite3VdbeMemSetNull(pDest);
    break;
  }
  pVtab = pCur->uc.pVCur->pVtab;
  pModule = pVtab->pModule;
  assert( pModule->xColumn );
  memset(&sContext, 0, sizeof(sContext));
  sContext.pOut = pDest;
  MemSetTypeFlag(pDest, MEM_Null);
  rc = pModule->xColumn(pCur->uc.pVCur, &sContext, pOp->p2);
  sqlite3VtabImportErrmsg(p, pVtab);
  if( sContext.isError ){
    rc = sContext.isError;
  }
  sqlite3VdbeChangeEncoding(pDest, encoding);
  REGISTER_TRACE(pOp->p3, pDest);
  UPDATE_MAX_BLOBSIZE(pDest);

  if( sqlite3VdbeMemTooBig(pDest) ){
    goto too_big;
  }
  if( rc ) goto abort_due_to_error;
  break;
}
#endif /* SQLITE_OMIT_VIRTUALTABLE */

#ifndef SQLITE_OMIT_VIRTUALTABLE
/* Opcode: VNext P1 P2 * * *
**
** Advance virtual table P1 to the next row in its result set and
** jump to instruction P2.  Or, if the virtual table has reached
** the end of its result set, then fall through to the next instruction.
*/
case OP_VNext: {   /* jump */
  sqlite3_vtab *pVtab;
  const sqlite3_module *pModule;
  int res;
  VdbeCursor *pCur;

  res = 0;
  pCur = p->apCsr[pOp->p1];
  assert( pCur->eCurType==CURTYPE_VTAB );
  if( pCur->nullRow ){
    break;
  }
  pVtab = pCur->uc.pVCur->pVtab;
  pModule = pVtab->pModule;
  assert( pModule->xNext );

  /* Invoke the xNext() method of the module. There is no way for the
  ** underlying implementation to return an error if one occurs during
  ** xNext(). Instead, if an error occurs, true is returned (indicating that 
  ** data is available) and the error code returned when xColumn or
  ** some other method is next invoked on the save virtual table cursor.
  */
  rc = pModule->xNext(pCur->uc.pVCur);
  sqlite3VtabImportErrmsg(p, pVtab);
  if( rc ) goto abort_due_to_error;
  res = pModule->xEof(pCur->uc.pVCur);
  VdbeBranchTaken(!res,2);
  if( !res ){
    /* If there is data, jump to P2 */
    goto jump_to_p2_and_check_for_interrupt;
  }
  goto check_for_interrupt;
}
#endif /* SQLITE_OMIT_VIRTUALTABLE */

#ifndef SQLITE_OMIT_VIRTUALTABLE
/* Opcode: VRename P1 * * P4 *
**
** P4 is a pointer to a virtual table object, an sqlite3_vtab structure.
** This opcode invokes the corresponding xRename method. The value
** in register P1 is passed as the zName argument to the xRename method.
*/
case OP_VRename: {
  sqlite3_vtab *pVtab;
  Mem *pName;

  pVtab = pOp->p4.pVtab->pVtab;
  pName = &aMem[pOp->p1];
  assert( pVtab->pModule->xRename );
  assert( memIsValid(pName) );
  assert( p->readOnly==0 );
  REGISTER_TRACE(pOp->p1, pName);
  assert( pName->flags & MEM_Str );
  testcase( pName->enc==SQLITE_UTF8 );
  testcase( pName->enc==SQLITE_UTF16BE );
  testcase( pName->enc==SQLITE_UTF16LE );
  rc = sqlite3VdbeChangeEncoding(pName, SQLITE_UTF8);
  if( rc ) goto abort_due_to_error;
  rc = pVtab->pModule->xRename(pVtab, pName->z);
  sqlite3VtabImportErrmsg(p, pVtab);
  p->expired = 0;
  if( rc ) goto abort_due_to_error;
  break;
}
#endif

#ifndef SQLITE_OMIT_VIRTUALTABLE
/* Opcode: VUpdate P1 P2 P3 P4 P5
** Synopsis: data=r[P3@P2]
**
** P4 is a pointer to a virtual table object, an sqlite3_vtab structure.
** This opcode invokes the corresponding xUpdate method. P2 values
** are contiguous memory cells starting at P3 to pass to the xUpdate 
** invocation. The value in register (P3+P2-1) corresponds to the 
** p2th element of the argv array passed to xUpdate.
**
** The xUpdate method will do a DELETE or an INSERT or both.
** The argv[0] element (which corresponds to memory cell P3)
** is the rowid of a row to delete.  If argv[0] is NULL then no 
** deletion occurs.  The argv[1] element is the rowid of the new 
** row.  This can be NULL to have the virtual table select the new 
** rowid for itself.  The subsequent elements in the array are 
** the values of columns in the new row.
**
** If P2==1 then no insert is performed.  argv[0] is the rowid of
** a row to delete.
**
** P1 is a boolean flag. If it is set to true and the xUpdate call
** is successful, then the value returned by sqlite3_last_insert_rowid() 
** is set to the value of the rowid for the row just inserted.
**
** P5 is the error actions (OE_Replace, OE_Fail, OE_Ignore, etc) to
** apply in the case of a constraint failure on an insert or update.
*/
case OP_VUpdate: {
  sqlite3_vtab *pVtab;
  const sqlite3_module *pModule;
  int nArg;
  int i;
  sqlite_int64 rowid;
  Mem **apArg;
  Mem *pX;

  assert( pOp->p2==1        || pOp->p5==OE_Fail   || pOp->p5==OE_Rollback 
       || pOp->p5==OE_Abort || pOp->p5==OE_Ignore || pOp->p5==OE_Replace
  );
  assert( p->readOnly==0 );
  pVtab = pOp->p4.pVtab->pVtab;
  if( pVtab==0 || NEVER(pVtab->pModule==0) ){
    rc = SQLITE_LOCKED;
    goto abort_due_to_error;
  }
  pModule = pVtab->pModule;
  nArg = pOp->p2;
  assert( pOp->p4type==P4_VTAB );
  if( ALWAYS(pModule->xUpdate) ){
    u8 vtabOnConflict = db->vtabOnConflict;
    apArg = p->apArg;
    pX = &aMem[pOp->p3];
    for(i=0; i<nArg; i++){
      assert( memIsValid(pX) );
      memAboutToChange(p, pX);
      apArg[i] = pX;
      pX++;
    }
    db->vtabOnConflict = pOp->p5;
    rc = pModule->xUpdate(pVtab, nArg, apArg, &rowid);
    db->vtabOnConflict = vtabOnConflict;
    sqlite3VtabImportErrmsg(p, pVtab);
    if( rc==SQLITE_OK && pOp->p1 ){
      assert( nArg>1 && apArg[0] && (apArg[0]->flags&MEM_Null) );
      db->lastRowid = lastRowid = rowid;
    }
    if( (rc&0xff)==SQLITE_CONSTRAINT && pOp->p4.pVtab->bConstraint ){
      if( pOp->p5==OE_Ignore ){
        rc = SQLITE_OK;
      }else{
        p->errorAction = ((pOp->p5==OE_Replace) ? OE_Abort : pOp->p5);
      }
    }else{
      p->nChange++;
    }
    if( rc ) goto abort_due_to_error;
  }
  break;
}
#endif /* SQLITE_OMIT_VIRTUALTABLE */

#ifndef  SQLITE_OMIT_PAGER_PRAGMAS
/* Opcode: Pagecount P1 P2 * * *
**
** Write the current number of pages in database P1 to memory cell P2.
*/
case OP_Pagecount: {            /* out2 */
  pOut = out2Prerelease(p, pOp);
  pOut->u.i = sqlite3BtreeLastPage(db->aDb[pOp->p1].pBt);
  break;
}
#endif


#ifndef  SQLITE_OMIT_PAGER_PRAGMAS
/* Opcode: MaxPgcnt P1 P2 P3 * *
**
** Try to set the maximum page count for database P1 to the value in P3.
** Do not let the maximum page count fall below the current page count and
** do not change the maximum page count value if P3==0.
**
** Store the maximum page count after the change in register P2.
*/
case OP_MaxPgcnt: {            /* out2 */
  unsigned int newMax;
  Btree *pBt;

  pOut = out2Prerelease(p, pOp);
  pBt = db->aDb[pOp->p1].pBt;
  newMax = 0;
  if( pOp->p3 ){
    newMax = sqlite3BtreeLastPage(pBt);
    if( newMax < (unsigned)pOp->p3 ) newMax = (unsigned)pOp->p3;
  }
  pOut->u.i = sqlite3BtreeMaxPageCount(pBt, newMax);
  break;
}
#endif


/* Opcode: Init P1 P2 * P4 *
** Synopsis: Start at P2
**
** Programs contain a single instance of this opcode as the very first
** opcode.
**
** If tracing is enabled (by the sqlite3_trace()) interface, then
** the UTF-8 string contained in P4 is emitted on the trace callback.
** Or if P4 is blank, use the string returned by sqlite3_sql().
**
** If P2 is not zero, jump to instruction P2.
**
** Increment the value of P1 so that OP_Once opcodes will jump the
** first time they are evaluated for this run.
*/
case OP_Init: {          /* jump */
  char *zTrace;
  int i;

  /* If the P4 argument is not NULL, then it must be an SQL comment string.
  ** The "--" string is broken up to prevent false-positives with srcck1.c.
  **
  ** This assert() provides evidence for:
  ** EVIDENCE-OF: R-50676-09860 The callback can compute the same text that
  ** would have been returned by the legacy sqlite3_trace() interface by
  ** using the X argument when X begins with "--" and invoking
  ** sqlite3_expanded_sql(P) otherwise.
  */
  assert( pOp->p4.z==0 || strncmp(pOp->p4.z, "-" "- ", 3)==0 );
  assert( pOp==p->aOp );  /* Always instruction 0 */

#ifndef SQLITE_OMIT_TRACE
  if( (db->mTrace & (SQLITE_TRACE_STMT|SQLITE_TRACE_LEGACY))!=0
   && !p->doingRerun
   && (zTrace = (pOp->p4.z ? pOp->p4.z : p->zSql))!=0
  ){
#ifndef SQLITE_OMIT_DEPRECATED
    if( db->mTrace & SQLITE_TRACE_LEGACY ){
      void (*x)(void*,const char*) = (void(*)(void*,const char*))db->xTrace;
      char *z = sqlite3VdbeExpandSql(p, zTrace);
      x(db->pTraceArg, z);
      sqlite3_free(z);
    }else
#endif
    {
      (void)db->xTrace(SQLITE_TRACE_STMT, db->pTraceArg, p, zTrace);
    }
  }
#ifdef SQLITE_USE_FCNTL_TRACE
  zTrace = (pOp->p4.z ? pOp->p4.z : p->zSql);
  if( zTrace ){
    int j;
    for(j=0; j<db->nDb; j++){
      if( DbMaskTest(p->btreeMask, j)==0 ) continue;
      sqlite3_file_control(db, db->aDb[j].zDbSName, SQLITE_FCNTL_TRACE, zTrace);
    }
  }
#endif /* SQLITE_USE_FCNTL_TRACE */
#ifdef SQLITE_DEBUG
  if( (db->flags & SQLITE_SqlTrace)!=0
   && (zTrace = (pOp->p4.z ? pOp->p4.z : p->zSql))!=0
  ){
    sqlite3DebugPrintf("SQL-trace: %s\n", zTrace);
  }
#endif /* SQLITE_DEBUG */
#endif /* SQLITE_OMIT_TRACE */
  assert( pOp->p2>0 );
  if( pOp->p1>=sqlite3GlobalConfig.iOnceResetThreshold ){
    for(i=1; i<p->nOp; i++){
      if( p->aOp[i].opcode==OP_Once ) p->aOp[i].p1 = 0;
    }
    pOp->p1 = 0;
  }
  pOp->p1++;
  goto jump_to_p2;
}

#ifdef SQLITE_ENABLE_CURSOR_HINTS
/* Opcode: CursorHint P1 * * P4 *
**
** Provide a hint to cursor P1 that it only needs to return rows that
** satisfy the Expr in P4.  TK_REGISTER terms in the P4 expression refer
** to values currently held in registers.  TK_COLUMN terms in the P4
** expression refer to columns in the b-tree to which cursor P1 is pointing.
*/
case OP_CursorHint: {
  VdbeCursor *pC;

  assert( pOp->p1>=0 && pOp->p1<p->nCursor );
  assert( pOp->p4type==P4_EXPR );
  pC = p->apCsr[pOp->p1];
  if( pC ){
    assert( pC->eCurType==CURTYPE_BTREE );
    sqlite3BtreeCursorHint(pC->uc.pCursor, BTREE_HINT_RANGE,
                           pOp->p4.pExpr, aMem);
  }
  break;
}
#endif /* SQLITE_ENABLE_CURSOR_HINTS */

/* Opcode: Noop * * * * *
**
** Do nothing.  This instruction is often useful as a jump
** destination.
*/
/*
** The magic Explain opcode are only inserted when explain==2 (which
** is to say when the EXPLAIN QUERY PLAN syntax is used.)
** This opcode records information from the optimizer.  It is the
** the same as a no-op.  This opcodesnever appears in a real VM program.
*/
default: {          /* This is really OP_Noop and OP_Explain */
  assert( pOp->opcode==OP_Noop || pOp->opcode==OP_Explain );
  break;
}

/*****************************************************************************
** The cases of the switch statement above this line should all be indented
** by 6 spaces.  But the left-most 6 spaces have been removed to improve the
** readability.  From this point on down, the normal indentation rules are
** restored.
*****************************************************************************/
    }

#ifdef VDBE_PROFILE
    {
      u64 endTime = sqlite3Hwtime();
      if( endTime>start ) pOrigOp->cycles += endTime - start;
      pOrigOp->cnt++;
    }
#endif

    /* The following code adds nothing to the actual functionality
    ** of the program.  It is only here for testing and debugging.
    ** On the other hand, it does burn CPU cycles every time through
    ** the evaluator loop.  So we can leave it out when NDEBUG is defined.
    */
#ifndef NDEBUG
    assert( pOp>=&aOp[-1] && pOp<&aOp[p->nOp-1] );

#ifdef SQLITE_DEBUG
    if( db->flags & SQLITE_VdbeTrace ){
      u8 opProperty = sqlite3OpcodeProperty[pOrigOp->opcode];
      if( rc!=0 ) printf("rc=%d\n",rc);
      if( opProperty & (OPFLG_OUT2) ){
        registerTrace(pOrigOp->p2, &aMem[pOrigOp->p2]);
      }
      if( opProperty & OPFLG_OUT3 ){
        registerTrace(pOrigOp->p3, &aMem[pOrigOp->p3]);
      }
    }
#endif  /* SQLITE_DEBUG */
#endif  /* NDEBUG */
  }  /* The end of the for(;;) loop the loops through opcodes */

  /* If we reach this point, it means that execution is finished with
  ** an error of some kind.
  */
abort_due_to_error:
  if( db->mallocFailed ) rc = SQLITE_NOMEM_BKPT;
  assert( rc );
  if( p->zErrMsg==0 && rc!=SQLITE_IOERR_NOMEM ){
    sqlite3VdbeError(p, "%s", sqlite3ErrStr(rc));
  }
  p->rc = rc;
  sqlite3SystemError(db, rc);
  testcase( sqlite3GlobalConfig.xLog!=0 );
  sqlite3_log(rc, "statement aborts at %d: [%s] %s", 
                   (int)(pOp - aOp), p->zSql, p->zErrMsg);
  sqlite3VdbeHalt(p);
  if( rc==SQLITE_IOERR_NOMEM ) sqlite3OomFault(db);
  rc = SQLITE_ERROR;
  if( resetSchemaOnFault>0 ){
    sqlite3ResetOneSchema(db, resetSchemaOnFault-1);
  }

  /* This is the only way out of this procedure.  We have to
  ** release the mutexes on btrees that were acquired at the
  ** top. */
vdbe_return:
  db->lastRowid = lastRowid;
  testcase( nVmStep>0 );
  p->aCounter[SQLITE_STMTSTATUS_VM_STEP] += (int)nVmStep;
  sqlite3VdbeLeave(p);
  assert( rc!=SQLITE_OK || nExtraDelete==0 
       || sqlite3_strlike("DELETE%",p->zSql,0)!=0 
  );
  return rc;

  /* Jump to here if a string or blob larger than SQLITE_MAX_LENGTH
  ** is encountered.
  */
too_big:
  sqlite3VdbeError(p, "string or blob too big");
  rc = SQLITE_TOOBIG;
  goto abort_due_to_error;

  /* Jump to here if a malloc() fails.
  */
no_mem:
  sqlite3OomFault(db);
  sqlite3VdbeError(p, "out of memory");
  rc = SQLITE_NOMEM_BKPT;
  goto abort_due_to_error;

  /* Jump to here if the sqlite3_interrupt() API sets the interrupt
  ** flag.
  */
abort_due_to_interrupt:
  assert( db->u1.isInterrupted );
  rc = db->mallocFailed ? SQLITE_NOMEM_BKPT : SQLITE_INTERRUPT;
  p->rc = rc;
  sqlite3VdbeError(p, "%s", sqlite3ErrStr(rc));
  goto abort_due_to_error;
}
