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
** a VDBE (or an "sqlite_vm" as it is known to the outside world.)  Prior
** to version 2.8.7, all this code was combined into the vdbe.c source file.
** But that file was getting too big so this subroutines were split out.
*/
#include "sqliteInt.h"
#include "os.h"
#include <ctype.h>
#include "vdbeInt.h"


/*
** When debugging the code generator in a symbolic debugger, one can
** set the sqlite3_vdbe_addop_trace to 1 and all opcodes will be printed
** as they are added to the instruction stream.
*/
#ifndef NDEBUG
int sqlite3_vdbe_addop_trace = 0;
#endif


/*
** Create a new virtual database engine.
*/
Vdbe *sqlite3VdbeCreate(sqlite *db){
  Vdbe *p;
  p = sqliteMalloc( sizeof(Vdbe) );
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
** Turn tracing on or off
*/
void sqlite3VdbeTrace(Vdbe *p, FILE *trace){
  p->trace = trace;
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
**    p1, p2          First two of the three possible operands.
**
** Use the sqlite3VdbeResolveLabel() function to fix an address and
** the sqlite3VdbeChangeP3() function to change the value of the P3
** operand.
*/
int sqlite3VdbeAddOp(Vdbe *p, int op, int p1, int p2){
  int i;
  VdbeOp *pOp;

  i = p->nOp;
  p->nOp++;
  assert( p->magic==VDBE_MAGIC_INIT );
  if( i>=p->nOpAlloc ){
    int oldSize = p->nOpAlloc;
    Op *aNew;
    p->nOpAlloc = p->nOpAlloc*2 + 100;
    aNew = sqliteRealloc(p->aOp, p->nOpAlloc*sizeof(Op));
    if( aNew==0 ){
      p->nOpAlloc = oldSize;
      return 0;
    }
    p->aOp = aNew;
    memset(&p->aOp[oldSize], 0, (p->nOpAlloc-oldSize)*sizeof(Op));
  }
  pOp = &p->aOp[i];
  pOp->opcode = op;
  pOp->p1 = p1;
  if( p2<0 && (-1-p2)<p->nLabel && p->aLabel[-1-p2]>=0 ){
    p2 = p->aLabel[-1-p2];
  }
  pOp->p2 = p2;
  pOp->p3 = 0;
  pOp->p3type = P3_NOTUSED;
#ifndef NDEBUG
  if( sqlite3_vdbe_addop_trace ) sqlite3VdbePrintOp(0, i, &p->aOp[i]);
#endif
  return i;
}

/*
** Add an opcode that includes the p3 value.
*/
int sqlite3VdbeOp3(Vdbe *p, int op, int p1, int p2, const char *zP3, int p3type){
  int addr = sqlite3VdbeAddOp(p, op, p1, p2);
  sqlite3VdbeChangeP3(p, addr, zP3, p3type);
  return addr;
}

/*
** Add multiple opcodes.  The list is terminated by an opcode of 0.
*/
int sqlite3VdbeCode(Vdbe *p, ...){
  int addr;
  va_list ap;
  int opcode, p1, p2;
  va_start(ap, p);
  addr = p->nOp;
  while( (opcode = va_arg(ap,int))!=0 ){
    p1 = va_arg(ap,int);
    p2 = va_arg(ap,int);
    sqlite3VdbeAddOp(p, opcode, p1, p2);
  }
  va_end(ap);
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
*/
int sqlite3VdbeMakeLabel(Vdbe *p){
  int i;
  i = p->nLabel++;
  assert( p->magic==VDBE_MAGIC_INIT );
  if( i>=p->nLabelAlloc ){
    int *aNew;
    p->nLabelAlloc = p->nLabelAlloc*2 + 10;
    aNew = sqliteRealloc( p->aLabel, p->nLabelAlloc*sizeof(p->aLabel[0]));
    if( aNew==0 ){
      sqliteFree(p->aLabel);
    }
    p->aLabel = aNew;
  }
  if( p->aLabel==0 ){
    p->nLabel = 0;
    p->nLabelAlloc = 0;
    return 0;
  }
  p->aLabel[i] = -1;
  return -1-i;
}

/*
** Resolve label "x" to be the address of the next instruction to
** be inserted.  The parameter "x" must have been obtained from
** a prior call to sqlite3VdbeMakeLabel().
*/
void sqlite3VdbeResolveLabel(Vdbe *p, int x){
  int j;
  assert( p->magic==VDBE_MAGIC_INIT );
  if( x<0 && (-x)<=p->nLabel && p->aOp ){
    if( p->aLabel[-1-x]==p->nOp ) return;
    assert( p->aLabel[-1-x]<0 );
    p->aLabel[-1-x] = p->nOp;
    for(j=0; j<p->nOp; j++){
      if( p->aOp[j].p2==x ) p->aOp[j].p2 = p->nOp;
    }
  }
}

/*
** Return the address of the next instruction to be inserted.
*/
int sqlite3VdbeCurrentAddr(Vdbe *p){
  assert( p->magic==VDBE_MAGIC_INIT );
  return p->nOp;
}

/*
** Add a whole list of operations to the operation stack.  Return the
** address of the first operation added.
*/
int sqlite3VdbeAddOpList(Vdbe *p, int nOp, VdbeOpList const *aOp){
  int addr;
  assert( p->magic==VDBE_MAGIC_INIT );
  if( p->nOp + nOp >= p->nOpAlloc ){
    int oldSize = p->nOpAlloc;
    Op *aNew;
    p->nOpAlloc = p->nOpAlloc*2 + nOp + 10;
    aNew = sqliteRealloc(p->aOp, p->nOpAlloc*sizeof(Op));
    if( aNew==0 ){
      p->nOpAlloc = oldSize;
      return 0;
    }
    p->aOp = aNew;
    memset(&p->aOp[oldSize], 0, (p->nOpAlloc-oldSize)*sizeof(Op));
  }
  addr = p->nOp;
  if( nOp>0 ){
    int i;
    VdbeOpList const *pIn = aOp;
    for(i=0; i<nOp; i++, pIn++){
      int p2 = pIn->p2;
      VdbeOp *pOut = &p->aOp[i+addr];
      pOut->opcode = pIn->opcode;
      pOut->p1 = pIn->p1;
      pOut->p2 = p2<0 ? addr + ADDR(p2) : p2;
      pOut->p3 = pIn->p3;
      pOut->p3type = pIn->p3 ? P3_STATIC : P3_NOTUSED;
#ifndef NDEBUG
      if( sqlite3_vdbe_addop_trace ){
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
  assert( p->magic==VDBE_MAGIC_INIT );
  if( p && addr>=0 && p->nOp>addr && p->aOp ){
    p->aOp[addr].p1 = val;
  }
}

/*
** Change the value of the P2 operand for a specific instruction.
** This routine is useful for setting a jump destination.
*/
void sqlite3VdbeChangeP2(Vdbe *p, int addr, int val){
  assert( val>=0 );
  assert( p->magic==VDBE_MAGIC_INIT );
  if( p && addr>=0 && p->nOp>addr && p->aOp ){
    p->aOp[addr].p2 = val;
  }
}

/*
** Change the value of the P3 operand for a specific instruction.
** This routine is useful when a large program is loaded from a
** static array using sqlite3VdbeAddOpList but we want to make a
** few minor changes to the program.
**
** If n>=0 then the P3 operand is dynamic, meaning that a copy of
** the string is made into memory obtained from sqliteMalloc().
** A value of n==0 means copy bytes of zP3 up to and including the
** first null byte.  If n>0 then copy n+1 bytes of zP3.
**
** If n==P3_STATIC  it means that zP3 is a pointer to a constant static
** string and we can just copy the pointer.  n==P3_POINTER means zP3 is
** a pointer to some object other than a string.
**
** If addr<0 then change P3 on the most recently inserted instruction.
*/
void sqlite3VdbeChangeP3(Vdbe *p, int addr, const char *zP3, int n){
  Op *pOp;
  assert( p->magic==VDBE_MAGIC_INIT );
  if( p==0 || p->aOp==0 ) return;
  if( addr<0 || addr>=p->nOp ){
    addr = p->nOp - 1;
    if( addr<0 ) return;
  }
  pOp = &p->aOp[addr];
  if( pOp->p3 && pOp->p3type==P3_DYNAMIC ){
    sqliteFree(pOp->p3);
    pOp->p3 = 0;
  }
  if( zP3==0 ){
    pOp->p3 = 0;
    pOp->p3type = P3_NOTUSED;
  }else if( n<0 ){
    pOp->p3 = (char*)zP3;
    pOp->p3type = n;
  }else{
    sqlite3SetNString(&pOp->p3, zP3, n, 0);
    pOp->p3type = P3_DYNAMIC;
  }
}

/*
** If the P3 operand to the specified instruction appears
** to be a quoted string token, then this procedure removes 
** the quotes.
**
** The quoting operator can be either a grave ascent (ASCII 0x27)
** or a double quote character (ASCII 0x22).  Two quotes in a row
** resolve to be a single actual quote character within the string.
*/
void sqlite3VdbeDequoteP3(Vdbe *p, int addr){
  Op *pOp;
  assert( p->magic==VDBE_MAGIC_INIT );
  if( p->aOp==0 ) return;
  if( addr<0 || addr>=p->nOp ){
    addr = p->nOp - 1;
    if( addr<0 ) return;
  }
  pOp = &p->aOp[addr];
  if( pOp->p3==0 || pOp->p3[0]==0 ) return;
  if( pOp->p3type==P3_POINTER ) return;
  if( pOp->p3type!=P3_DYNAMIC ){
    pOp->p3 = sqliteStrDup(pOp->p3);
    pOp->p3type = P3_DYNAMIC;
  }
  sqlite3Dequote(pOp->p3);
}

/*
** On the P3 argument of the given instruction, change all
** strings of whitespace characters into a single space and
** delete leading and trailing whitespace.
*/
void sqlite3VdbeCompressSpace(Vdbe *p, int addr){
  unsigned char *z;
  int i, j;
  Op *pOp;
  assert( p->magic==VDBE_MAGIC_INIT );
  if( p->aOp==0 || addr<0 || addr>=p->nOp ) return;
  pOp = &p->aOp[addr];
  if( pOp->p3type==P3_POINTER ){
    return;
  }
  if( pOp->p3type!=P3_DYNAMIC ){
    pOp->p3 = sqliteStrDup(pOp->p3);
    pOp->p3type = P3_DYNAMIC;
  }
  z = (unsigned char*)pOp->p3;
  if( z==0 ) return;
  i = j = 0;
  while( isspace(z[i]) ){ i++; }
  while( z[i] ){
    if( isspace(z[i]) ){
      z[j++] = ' ';
      while( isspace(z[++i]) ){}
    }else{
      z[j++] = z[i++];
    }
  }
  while( j>0 && isspace(z[j-1]) ){ j--; }
  z[j] = 0;
}

/*
** Search the current program starting at instruction addr for the given
** opcode and P2 value.  Return the address plus 1 if found and 0 if not
** found.
*/
int sqlite3VdbeFindOp(Vdbe *p, int addr, int op, int p2){
  int i;
  assert( p->magic==VDBE_MAGIC_INIT );
  for(i=addr; i<p->nOp; i++){
    if( p->aOp[i].opcode==op && p->aOp[i].p2==p2 ) return i+1;
  }
  return 0;
}

/*
** Return the opcode for a given address.
*/
VdbeOp *sqlite3VdbeGetOp(Vdbe *p, int addr){
  assert( p->magic==VDBE_MAGIC_INIT );
  assert( addr>=0 && addr<p->nOp );
  return &p->aOp[addr];
}

/*
** The following group or routines are employed by installable functions
** to return their results.
**
** The sqlite3_set_result_string() routine can be used to return a string
** value or to return a NULL.  To return a NULL, pass in NULL for zResult.
** A copy is made of the string before this routine returns so it is safe
** to pass in an ephemeral string.
**
** sqlite3_set_result_error() works like sqlite3_set_result_string() except
** that it signals a fatal error.  The string argument, if any, is the
** error message.  If the argument is NULL a generic substitute error message
** is used.
**
** The sqlite3_set_result_int() and sqlite3_set_result_double() set the return
** value of the user function to an integer or a double.
**
** These routines are defined here in vdbe.c because they depend on knowing
** the internals of the sqlite_func structure which is only defined in 
** this source file.
*/
char *sqlite3_set_result_string(sqlite_func *p, const char *zResult, int n){
  assert( !p->isStep );
  if( p->s.flags & MEM_Dyn ){
    sqliteFree(p->s.z);
  }
  if( zResult==0 ){
    p->s.flags = MEM_Null;
    n = 0;
    p->s.z = 0;
    p->s.n = 0;
  }else{
    if( n<0 ) n = strlen(zResult);
    if( n<NBFS-1 ){
      memcpy(p->s.zShort, zResult, n);
      p->s.zShort[n] = 0;
      p->s.flags = MEM_Str | MEM_Short;
      p->s.z = p->s.zShort;
    }else{
      p->s.z = sqliteMallocRaw( n+1 );
      if( p->s.z ){
        memcpy(p->s.z, zResult, n);
        p->s.z[n] = 0;
      }
      p->s.flags = MEM_Str | MEM_Dyn;
    }
    p->s.n = n+1;
  }
  return p->s.z;
}
void sqlite3_set_result_int(sqlite_func *p, int iResult){
  assert( !p->isStep );
  if( p->s.flags & MEM_Dyn ){
    sqliteFree(p->s.z);
  }
  p->s.i = iResult;
  p->s.flags = MEM_Int;
}
void sqlite3_set_result_double(sqlite_func *p, double rResult){
  assert( !p->isStep );
  if( p->s.flags & MEM_Dyn ){
    sqliteFree(p->s.z);
  }
  p->s.r = rResult;
  p->s.flags = MEM_Real;
}
void sqlite3_set_result_error(sqlite_func *p, const char *zMsg, int n){
  assert( !p->isStep );
  sqlite3_set_result_string(p, zMsg, n);
  p->isError = 1;
}

/*
** Extract the user data from a sqlite_func structure and return a
** pointer to it.
*/
void *sqlite3_user_data(sqlite_func *p){
  assert( p && p->pFunc );
  return p->pFunc->pUserData;
}

/*
** Allocate or return the aggregate context for a user function.  A new
** context is allocated on the first call.  Subsequent calls return the
** same context that was returned on prior calls.
**
** This routine is defined here in vdbe.c because it depends on knowing
** the internals of the sqlite_func structure which is only defined in
** this source file.
*/
void *sqlite3_aggregate_context(sqlite_func *p, int nByte){
  assert( p && p->pFunc && p->pFunc->xStep );
  if( p->pAgg==0 ){
    if( nByte<=NBFS ){
      p->pAgg = (void*)p->s.z;
      memset(p->pAgg, 0, nByte);
    }else{
      p->pAgg = sqliteMalloc( nByte );
    }
  }
  return p->pAgg;
}

/*
** Return the number of times the Step function of a aggregate has been 
** called.
**
** This routine is defined here in vdbe.c because it depends on knowing
** the internals of the sqlite_func structure which is only defined in
** this source file.
*/
int sqlite3_aggregate_count(sqlite_func *p){
  assert( p && p->pFunc && p->pFunc->xStep );
  return p->cnt;
}

#if !defined(NDEBUG) || defined(VDBE_PROFILE)
/*
** Print a single opcode.  This routine is used for debugging only.
*/
void sqlite3VdbePrintOp(FILE *pOut, int pc, Op *pOp){
  char *zP3;
  char zPtr[40];
  if( pOp->p3type==P3_POINTER ){
    sprintf(zPtr, "ptr(%#x)", (int)pOp->p3);
    zP3 = zPtr;
  }else{
    zP3 = pOp->p3;
  }
  if( pOut==0 ) pOut = stdout;
  fprintf(pOut,"%4d %-12s %4d %4d %s\n",
      pc, sqlite3OpcodeNames[pOp->opcode], pOp->p1, pOp->p2, zP3 ? zP3 : "");
  fflush(pOut);
}
#endif

/*
** Give a listing of the program in the virtual machine.
**
** The interface is the same as sqlite3VdbeExec().  But instead of
** running the code, it invokes the callback once for each instruction.
** This feature is used to implement "EXPLAIN".
*/
int sqlite3VdbeList(
  Vdbe *p                   /* The VDBE */
){
  sqlite *db = p->db;
  int i;
  int rc = SQLITE_OK;
  static char *azColumnNames[] = {
     "addr", "opcode", "p1",  "p2",  "p3", 
     "int",  "text",   "int", "int", "text",
     0
  };

  assert( p->popStack==0 );
  assert( p->explain );
  p->azColName = azColumnNames;
  p->azResColumn = p->zArgv;
  for(i=0; i<5; i++) p->zArgv[i] = p->aStack[i].zShort;
  i = p->pc;
  if( i>=p->nOp ){
    p->rc = SQLITE_OK;
    rc = SQLITE_DONE;
  }else if( db->flags & SQLITE_Interrupt ){
    db->flags &= ~SQLITE_Interrupt;
    if( db->magic!=SQLITE_MAGIC_BUSY ){
      p->rc = SQLITE_MISUSE;
    }else{
      p->rc = SQLITE_INTERRUPT;
    }
    rc = SQLITE_ERROR;
    sqlite3SetString(&p->zErrMsg, sqlite3_error_string(p->rc), (char*)0);
  }else{
    sprintf(p->zArgv[0],"%d",i);
    sprintf(p->zArgv[2],"%d", p->aOp[i].p1);
    sprintf(p->zArgv[3],"%d", p->aOp[i].p2);
    if( p->aOp[i].p3type==P3_POINTER ){
      sprintf(p->aStack[4].zShort, "ptr(%#x)", (int)p->aOp[i].p3);
      p->zArgv[4] = p->aStack[4].zShort;
    }else{
      p->zArgv[4] = p->aOp[i].p3;
    }
    p->zArgv[1] = sqlite3OpcodeNames[p->aOp[i].opcode];
    p->pc = i+1;
    p->azResColumn = p->zArgv;
    p->nResColumn = 5;
    p->rc = SQLITE_OK;
    rc = SQLITE_ROW;
  }
  return rc;
}

/*
** Prepare a virtual machine for execution.  This involves things such
** as allocating stack space and initializing the program counter.
** After the VDBE has be prepped, it can be executed by one or more
** calls to sqlite3VdbeExec().  
*/
void sqlite3VdbeMakeReady(
  Vdbe *p,                       /* The VDBE */
  int nVar,                      /* Number of '?' see in the SQL statement */
  int isExplain                  /* True if the EXPLAIN keywords is present */
){
  int n;

  assert( p!=0 );
  assert( p->magic==VDBE_MAGIC_INIT );

  /* Add a HALT instruction to the very end of the program.
  */
  if( p->nOp==0 || (p->aOp && p->aOp[p->nOp-1].opcode!=OP_Halt) ){
    sqlite3VdbeAddOp(p, OP_Halt, 0, 0);
  }

  /* No instruction ever pushes more than a single element onto the
  ** stack.  And the stack never grows on successive executions of the
  ** same loop.  So the total number of instructions is an upper bound
  ** on the maximum stack depth required.
  **
  ** Allocation all the stack space we will ever need.
  */
  if( p->aStack==0 ){
    p->nVar = nVar;
    assert( nVar>=0 );
    n = isExplain ? 10 : p->nOp;
    p->aStack = sqliteMalloc(
      n*(sizeof(p->aStack[0]) + 2*sizeof(char*))     /* aStack and zArgv */
      + p->nVar*sizeof(Mem)                          /* apVar */
    );
    p->zArgv = (char**)&p->aStack[n];
    p->azColName = (char**)&p->zArgv[n];
    p->apVar = (Mem *)&p->azColName[n];
    for(n=0; n<p->nVar; n++){
      p->apVar[n].flags = MEM_Null;
    }
  }

  sqlite3HashInit(&p->agg.hash, SQLITE_HASH_BINARY, 0);
  p->agg.pSearch = 0;
#ifdef MEMORY_DEBUG
  if( sqlite3OsFileExists("vdbe_trace") ){
    p->trace = stdout;
  }
#endif
  p->pTos = &p->aStack[-1];
  p->pc = 0;
  p->rc = SQLITE_OK;
  p->uniqueCnt = 0;
  p->returnDepth = 0;
  p->errorAction = OE_Abort;
  p->undoTransOnError = 0;
  p->popStack =  0;
  p->explain |= isExplain;
  p->magic = VDBE_MAGIC_RUN;
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
** Remove any elements that remain on the sorter for the VDBE given.
*/
void sqlite3VdbeSorterReset(Vdbe *p){
  while( p->pSort ){
    Sorter *pSorter = p->pSort;
    p->pSort = pSorter->pNext;
    sqliteFree(pSorter->zKey);
    sqliteFree(pSorter->pData);
    sqliteFree(pSorter);
  }
}

/*
** Reset an Agg structure.  Delete all its contents. 
**
** For installable aggregate functions, if the step function has been
** called, make sure the finalizer function has also been called.  The
** finalizer might need to free memory that was allocated as part of its
** private context.  If the finalizer has not been called yet, call it
** now.
*/
void sqlite3VdbeAggReset(Agg *pAgg){
  int i;
  HashElem *p;
  for(p = sqliteHashFirst(&pAgg->hash); p; p = sqliteHashNext(p)){
    AggElem *pElem = sqliteHashData(p);
    assert( pAgg->apFunc!=0 );
    for(i=0; i<pAgg->nMem; i++){
      Mem *pMem = &pElem->aMem[i];
      if( pAgg->apFunc[i] && (pMem->flags & MEM_AggCtx)!=0 ){
        sqlite_func ctx;
        ctx.pFunc = pAgg->apFunc[i];
        ctx.s.flags = MEM_Null;
        ctx.pAgg = pMem->z;
        ctx.cnt = pMem->i;
        ctx.isStep = 0;
        ctx.isError = 0;
        (*pAgg->apFunc[i]->xFinalize)(&ctx);
        if( pMem->z!=0 && pMem->z!=pMem->zShort ){
          sqliteFree(pMem->z);
        }
        if( ctx.s.flags & MEM_Dyn ){
          sqliteFree(ctx.s.z);
        }
      }else if( pMem->flags & MEM_Dyn ){
        sqliteFree(pMem->z);
      }
    }
    sqliteFree(pElem);
  }
  sqlite3HashClear(&pAgg->hash);
  sqliteFree(pAgg->apFunc);
  pAgg->apFunc = 0;
  pAgg->pCurrent = 0;
  pAgg->pSearch = 0;
  pAgg->nMem = 0;
}

/*
** Delete a keylist
*/
void sqlite3VdbeKeylistFree(Keylist *p){
  while( p ){
    Keylist *pNext = p->pNext;
    sqliteFree(p);
    p = pNext;
  }
}

/*
** Close a cursor and release all the resources that cursor happens
** to hold.
*/
void sqlite3VdbeCleanupCursor(Cursor *pCx){
  if( pCx->pCursor ){
    sqlite3BtreeCloseCursor(pCx->pCursor);
  }
  if( pCx->pBt ){
    sqlite3BtreeClose(pCx->pBt);
  }
  sqliteFree(pCx->pData);
  sqliteFree(pCx->aType);
  memset(pCx, 0, sizeof(*pCx));
}

/*
** Close all cursors
*/
static void closeAllCursors(Vdbe *p){
  int i;
  for(i=0; i<p->nCursor; i++){
    Cursor *pC = p->apCsr[i];
    sqlite3VdbeCleanupCursor(pC);
    sqliteFree(pC);
  }
  sqliteFree(p->apCsr);
  p->apCsr = 0;
  p->nCursor = 0;
}

/*
** Clean up the VM after execution.
**
** This routine will automatically close any cursors, lists, and/or
** sorters that were left open.  It also deletes the values of
** variables in the aVar[] array.
*/
static void Cleanup(Vdbe *p){
  int i;
  if( p->aStack ){
    Mem *pTos = p->pTos;
    while( pTos>=p->aStack ){
      if( pTos->flags & MEM_Dyn ){
        sqliteFree(pTos->z);
      }
      pTos--;
    }
    p->pTos = pTos;
  }
  closeAllCursors(p);
  if( p->aMem ){
    for(i=0; i<p->nMem; i++){
      if( p->aMem[i].flags & MEM_Dyn ){
        sqliteFree(p->aMem[i].z);
      }
    }
  }
  sqliteFree(p->aMem);
  p->aMem = 0;
  p->nMem = 0;
  if( p->pList ){
    sqlite3VdbeKeylistFree(p->pList);
    p->pList = 0;
  }
  sqlite3VdbeSorterReset(p);
  if( p->pFile ){
    if( p->pFile!=stdin ) fclose(p->pFile);
    p->pFile = 0;
  }
  if( p->azField ){
    sqliteFree(p->azField);
    p->azField = 0;
  }
  p->nField = 0;
  if( p->zLine ){
    sqliteFree(p->zLine);
    p->zLine = 0;
  }
  p->nLineAlloc = 0;
  sqlite3VdbeAggReset(&p->agg);
  if( p->aSet ){
    for(i=0; i<p->nSet; i++){
      sqlite3HashClear(&p->aSet[i].hash);
    }
  }
  sqliteFree(p->aSet);
  p->aSet = 0;
  p->nSet = 0;
  if( p->keylistStack ){
    int ii;
    for(ii = 0; ii < p->keylistStackDepth; ii++){
      sqlite3VdbeKeylistFree(p->keylistStack[ii]);
    }
    sqliteFree(p->keylistStack);
    p->keylistStackDepth = 0;
    p->keylistStack = 0;
  }
  sqliteFree(p->contextStack);
  p->contextStack = 0;
  sqliteFree(p->zErrMsg);
  p->zErrMsg = 0;
}

/*
** Clean up a VDBE after execution but do not delete the VDBE just yet.
** Write any error messages into *pzErrMsg.  Return the result code.
**
** After this routine is run, the VDBE should be ready to be executed
** again.
*/
int sqlite3VdbeReset(Vdbe *p, char **pzErrMsg){
  sqlite *db = p->db;
  int i;

  if( p->magic!=VDBE_MAGIC_RUN && p->magic!=VDBE_MAGIC_HALT ){
    sqlite3SetString(pzErrMsg, sqlite3_error_string(SQLITE_MISUSE), (char*)0);
    return SQLITE_MISUSE;
  }
  if( p->zErrMsg ){
    if( pzErrMsg && *pzErrMsg==0 ){
      *pzErrMsg = p->zErrMsg;
    }else{
      sqliteFree(p->zErrMsg);
    }
    p->zErrMsg = 0;
  }else if( p->rc ){
    sqlite3SetString(pzErrMsg, sqlite3_error_string(p->rc), (char*)0);
  }
  Cleanup(p);
  if( p->rc!=SQLITE_OK ){
    switch( p->errorAction ){
      case OE_Abort: {
        if( !p->undoTransOnError ){
          for(i=0; i<db->nDb; i++){
            if( db->aDb[i].pBt ){
              sqlite3BtreeRollbackStmt(db->aDb[i].pBt);
            }
          }
          break;
        }
        /* Fall through to ROLLBACK */
      }
      case OE_Rollback: {
        sqlite3RollbackAll(db);
        db->flags &= ~SQLITE_InTrans;
        db->onError = OE_Default;
        break;
      }
      default: {
        if( p->undoTransOnError ){
          sqlite3RollbackAll(db);
          db->flags &= ~SQLITE_InTrans;
          db->onError = OE_Default;
        }
        break;
      }
    }
    sqlite3RollbackInternalChanges(db);
  }
  for(i=0; i<db->nDb; i++){
    if( db->aDb[i].pBt && db->aDb[i].inTrans==2 ){
      sqlite3BtreeCommitStmt(db->aDb[i].pBt);
      db->aDb[i].inTrans = 1;
    }
  }
  assert( p->pTos<&p->aStack[p->pc] || sqlite3_malloc_failed==1 );
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
  return p->rc;
}

/*
** Clean up and delete a VDBE after execution.  Return an integer which is
** the result code.  Write any error message text into *pzErrMsg.
*/
int sqlite3VdbeFinalize(Vdbe *p, char **pzErrMsg){
  int rc;
  sqlite *db;

  if( p->magic!=VDBE_MAGIC_RUN && p->magic!=VDBE_MAGIC_HALT ){
    sqlite3SetString(pzErrMsg, sqlite3_error_string(SQLITE_MISUSE), (char*)0);
    return SQLITE_MISUSE;
  }
  db = p->db;
  rc = sqlite3VdbeReset(p, pzErrMsg);
  sqlite3VdbeDelete(p);
  if( db->want_to_close && db->pVdbe==0 ){
    sqlite3_close(db);
  }
  if( rc==SQLITE_SCHEMA ){
    sqlite3ResetInternalSchema(db, 0);
  }
  return rc;
}

/*
** Unbind the value bound to variable $i in virtual machine p. This is the 
** the same as binding a NULL value to the column. If the "i" parameter is
** out of range, then SQLITE_RANGE is returned. Othewise SQLITE_OK.
**
** The error code stored in database p->db is overwritten with the return
** value in any case.
*/
static int vdbeUnbind(Vdbe *p, int i){
  Mem *pVar;
  if( p->magic!=VDBE_MAGIC_RUN || p->pc!=0 ){
    sqlite3Error(p->db, SQLITE_MISUSE, 0);
    return SQLITE_MISUSE;
  }
  if( i<1 || i>p->nVar ){
    sqlite3Error(p->db, SQLITE_RANGE, 0);
    return SQLITE_RANGE;
  }
  i--;
  pVar = &p->apVar[i];
  if( pVar->flags&MEM_Dyn ){
    sqliteFree(pVar->z);
  }
  pVar->flags = MEM_Null;
  sqlite3Error(p->db, SQLITE_OK, 0);
  return SQLITE_OK;
}

/*
** This routine is used to bind text or blob data to an SQL variable (a ?).
** It may also be used to bind a NULL value, by setting zVal to 0. Any
** existing value is unbound.
**
** The error code stored in p->db is overwritten with the return value in
** all cases.
*/
static int vdbeBindBlob(
  Vdbe *p,           /* Virtual machine */
  int i,             /* Var number to bind (numbered from 1 upward) */
  const char *zVal,  /* Pointer to blob of data */
  int bytes,         /* Number of bytes to copy */
  int copy,          /* True to copy the memory, false to copy a pointer */
  int flags          /* Valid combination of MEM_Blob, MEM_Str, MEM_UtfXX */
){
  Mem *pVar;
  int rc;

  rc = vdbeUnbind(p, i);
  if( rc!=SQLITE_OK ){
    return rc;
  }
  pVar = &p->apVar[i-1];

  if( zVal ){
    pVar->n = bytes;
    pVar->flags = flags;
    if( !copy ){
      pVar->z = (char *)zVal;
      pVar->flags |= MEM_Static;
    }else{
      if( bytes>NBFS ){
        pVar->z = (char *)sqliteMalloc(bytes);
        if( !pVar->z ){
          sqlite3Error(p->db, SQLITE_NOMEM, 0);
          return SQLITE_NOMEM;
        }
        pVar->flags |= MEM_Dyn;
      }else{
        pVar->z = pVar->zShort;
        pVar->flags |= MEM_Short;
      }
      memcpy(pVar->z, zVal, bytes);
    }
  }

  return SQLITE_OK;
}

/*
** Bind a 64 bit integer to an SQL statement variable.
*/
int sqlite3_bind_int64(sqlite3_stmt *p, int i, long long int iValue){
  int rc;
  Vdbe *v = (Vdbe *)p;
  rc = vdbeUnbind(v, i);
  if( rc==SQLITE_OK ){
    Mem *pVar = &v->apVar[i-1];
    pVar->flags = MEM_Int;
    pVar->i = iValue;
  }
  return SQLITE_OK;
}

/*
** Bind a 32 bit integer to an SQL statement variable.
*/
int sqlite3_bind_int32(sqlite3_stmt *p, int i, int iValue){
  return sqlite3_bind_int64(p, i, (long long int)iValue);
}

/*
** Bind a double (real) to an SQL statement variable.
*/
int sqlite3_bind_double(sqlite3_stmt *p, int i, double iValue){
  int rc;
  Vdbe *v = (Vdbe *)p;
  rc = vdbeUnbind(v, i);
  if( rc==SQLITE_OK ){
    Mem *pVar = &v->apVar[i-1];
    pVar->flags = MEM_Real;
    pVar->r = iValue;
  }
  return SQLITE_OK;
}

/*
** Bind a NULL value to an SQL statement variable.
*/
int sqlite3_bind_null(sqlite3_stmt* p, int i){
  return vdbeUnbind((Vdbe *)p, i);
}

/*
** Bind a UTF-8 text value to an SQL statement variable.
*/
int sqlite3_bind_text( 
  sqlite3_stmt *p, 
  int i, 
  const char *zData, 
  int nData, 
  int eCopy
){
  int flags = MEM_Str|MEM_Utf8;
  if( zData ){
    if( nData<0 ){
      nData = strlen(zData)+1;
      flags |= MEM_Term;
    }else if( !zData[nData-1] ){
      flags |= MEM_Term;
    }
  }
  return vdbeBindBlob((Vdbe *)p, i, zData, nData, eCopy, flags);
}

/*
** Bind a UTF-16 text value to an SQL statement variable.
*/
int sqlite3_bind_text16(
  sqlite3_stmt *p, 
  int i, 
  const void *zData, 
  int nData, 
  int eCopy
){
  int flags = MEM_Str|MEM_Utf16le|MEM_Utf16be;

  if( zData ){
    /* If nData is less than zero, measure the length of the string. 
    ** manually. In this case the variable will always be null terminated.
    */
    if( nData<0 ){
      nData = sqlite3utf16ByteLen(zData, -1) + 2;
      flags |= MEM_Term;
    }else{
      /* If nData is greater than zero, check if the final character appears
      ** to be a terminator.
      */
      if( !(((u8 *)zData)[nData-1]) && !(((u8 *)zData)[nData-2]) ){
        flags |= MEM_Term;
      }
    }
  }  
 
  return vdbeBindBlob((Vdbe *)p, i, zData, nData, eCopy, flags);
}

/*
** Bind a blob value to an SQL statement variable.
*/
int sqlite3_bind_blob(
  sqlite3_stmt *p, 
  int i, 
  const void *zData, 
  int nData, 
  int eCopy
){
  return vdbeBindBlob((Vdbe *)p, i, zData, nData, eCopy, MEM_Blob);
}

/*
** Set the values of all variables.  Variable $1 in the original SQL will
** be the string azValue[0].  $2 will have the value azValue[1].  And
** so forth.  If a value is out of range (for example $3 when nValue==2)
** then its value will be NULL.
**
** This routine overrides any prior call.
*/
int sqlite3_bind(sqlite_vm *pVm, int i, const char *zVal, int len, int copy){
  return sqlite3_bind_text(pVm, i, zVal, len, copy);
}

/*
** Delete an entire VDBE.
*/
void sqlite3VdbeDelete(Vdbe *p){
  int i;
  if( p==0 ) return;
  Cleanup(p);
  if( p->pPrev ){
    p->pPrev->pNext = p->pNext;
  }else{
    assert( p->db->pVdbe==p );
    p->db->pVdbe = p->pNext;
  }
  if( p->pNext ){
    p->pNext->pPrev = p->pPrev;
  }
  p->pPrev = p->pNext = 0;
  if( p->nOpAlloc==0 ){
    p->aOp = 0;
    p->nOp = 0;
  }
  for(i=0; i<p->nOp; i++){
    if( p->aOp[i].p3type==P3_DYNAMIC ){
      sqliteFree(p->aOp[i].p3);
    }
  }
  for(i=0; i<p->nVar; i++){
    if( p->apVar[i].flags&MEM_Dyn ){
      sqliteFree(p->apVar[i].z);
    }
  }
  sqliteFree(p->aOp);
  sqliteFree(p->aLabel);
  sqliteFree(p->aStack);
  p->magic = VDBE_MAGIC_DEAD;
  sqliteFree(p);
}

/*
** If a MoveTo operation is pending on the given cursor, then do that
** MoveTo now.  Return an error code.  If no MoveTo is pending, this
** routine does nothing and returns SQLITE_OK.
*/
int sqlite3VdbeCursorMoveto(Cursor *p){
  if( p->deferredMoveto ){
    int res;
    extern int sqlite3_search_count;
    assert( p->intKey );
    if( p->intKey ){
      sqlite3BtreeMoveto(p->pCursor, 0, p->movetoTarget, &res);
    }else{
      sqlite3BtreeMoveto(p->pCursor,(char*)&p->movetoTarget,sizeof(i64),&res);
    }
    p->incrKey = 0;
    p->lastRecno = keyToInt(p->movetoTarget);
    p->recnoIsValid = res==0;
    if( res<0 ){
      sqlite3BtreeNext(p->pCursor, &res);
    }
    sqlite3_search_count++;
    p->deferredMoveto = 0;
    p->cacheValid = 0;
  }
  return SQLITE_OK;
}

/*
** The following functions:
**
** sqlite3VdbeSerialType()
** sqlite3VdbeSerialTypeLen()
** sqlite3VdbeSerialRead()
** sqlite3VdbeSerialLen()
** sqlite3VdbeSerialWrite()
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
**      0                     -            Not a type.
**      1                     1            signed integer
**      2                     2            signed integer
**      3                     4            signed integer
**      4                     8            signed integer
**      5                     8            IEEE float
**      6                     0            NULL
**     7..11                               reserved for expansion
**    N>=12 and even       (N-12)/2        BLOB
**    N>=13 and odd        (N-13)/2        text
**
*/

/*
** Return the serial-type for the value stored in pMem.
*/
u64 sqlite3VdbeSerialType(const Mem *pMem){
  int flags = pMem->flags;

  if( flags&MEM_Null ){
    return 6;
  }
  if( flags&MEM_Int ){
    /* Figure out whether to use 1, 2, 4 or 8 bytes. */
    i64 i = pMem->i;
    if( i>=-127 && i<=127 ) return 1;
    if( i>=-32767 && i<=32767 ) return 2;
    if( i>=-2147483647 && i<=2147483647 ) return 3;
    return 4;
  }
  if( flags&MEM_Real ){
    return 5;
  }
  if( flags&MEM_Str ){
    /* We assume that the string is NULL-terminated. We don't store the
    ** NULL-terminator - it is implied by the string storage class.
    */
    assert( pMem->n>0 );
    assert( pMem->z[pMem->n-1]=='\0' );
    return (pMem->n*2 + 11); /* (pMem->n-1)*2 + 13 */
  }
  if( flags&MEM_Blob ){
    return (pMem->n*2 + 12);
  }
  return 0;
}

/*
** Return the length of the data corresponding to the supplied serial-type.
*/
int sqlite3VdbeSerialTypeLen(u64 serial_type){
  assert( serial_type!=0 );
  switch(serial_type){
    case 6: return 0;                  /* NULL */
    case 1: return 1;                  /* 1 byte integer */
    case 2: return 2;                  /* 2 byte integer */
    case 3: return 4;                  /* 4 byte integer */
    case 4: return 8;                  /* 8 byte integer */
    case 5: return 8;                  /* 8 byte float */
  }
  assert( serial_type>=12 );
  return ((serial_type-12)>>1);        /* text or blob */
}

/*
** Write the serialized data blob for the value stored in pMem into 
** buf. It is assumed that the caller has allocated sufficient space.
** Return the number of bytes written.
*/ 
int sqlite3VdbeSerialPut(unsigned char *buf, const Mem *pMem){
  u64 serial_type = sqlite3VdbeSerialType(pMem);
  int len;

  assert( serial_type!=0 );
 
  /* NULL */
  if( serial_type==6 ){
    return 0;
  }
 
  /* Integer */
  if( serial_type<5 ){
    i64 i = pMem->i;
    len = sqlite3VdbeSerialTypeLen(serial_type);
    while( len-- ){
      buf[len] = (i&0xFF);
      i = i >> 8;
    }
    return sqlite3VdbeSerialTypeLen(serial_type);
  }

  /* Float */
  if( serial_type==5 ){
    /* TODO: byte ordering? */
    assert( sizeof(double)==8 );
    memcpy(buf, &pMem->r, 8);
    return 8;
  }
  
  /* String or blob */
  assert( serial_type>=12 );
  len = sqlite3VdbeSerialTypeLen(serial_type);
  memcpy(buf, pMem->z, len);
  return len;
}

/*
** Deserialize the data blob pointed to by buf as serial type serial_type
** and store the result in pMem.  Return the number of bytes read.
*/ 
int sqlite3VdbeSerialGet(const unsigned char *buf, u64 serial_type, Mem *pMem){
  int len;

  assert( serial_type!=0 );

  /* memset(pMem, 0, sizeof(pMem)); */
  pMem->flags = 0;
  pMem->z = 0;

  /* NULL */
  if( serial_type==6 ){
    pMem->flags = MEM_Null;
    return 0;
  }
 
  /* Integer */
  if( serial_type<5 ){
    i64 i = 0;
    int n;
    len = sqlite3VdbeSerialTypeLen(serial_type);

    if( buf[0]&0x80 ){
      for(n=0; n<(8-len); n++){
        i = (i<<8)+0xFF;
      }
    }
    for(n=0; n<len; n++){
      i = i << 8;
      i = i + buf[n];
    }
    pMem->flags = MEM_Int;
    pMem->i = i;
    return sqlite3VdbeSerialTypeLen(serial_type);
  }

  /* Float */
  if( serial_type==5 ){
    /* TODO: byte ordering? */
    assert( sizeof(double)==8 );
    memcpy(&pMem->r, buf, 8);
    pMem->flags = MEM_Real;
    return 8;
  }
  
  /* String or blob */
  assert( serial_type>=12 );
  len = sqlite3VdbeSerialTypeLen(serial_type);
  if( serial_type&0x01 ){
    pMem->flags = MEM_Str;
    pMem->n = len+1;
  }else{
    pMem->flags = MEM_Blob;
    pMem->n = len;
  }

  if( (pMem->n)>NBFS ){
    pMem->z = sqliteMallocRaw( pMem->n );
    if( !pMem->z ){
      return -1;
    }
    pMem->flags |= MEM_Dyn;
  }else{
    pMem->z = pMem->zShort;
    pMem->flags |= MEM_Short;
  }

  memcpy(pMem->z, buf, len); 
  if( pMem->flags&MEM_Str ){
    pMem->z[len] = '\0';
  }

  return len;
}

/*
** Compare the values contained by the two memory cells, returning
** negative, zero or positive if pMem1 is less than, equal to, or greater
** than pMem2. Sorting order is NULL's first, followed by numbers (integers
** and reals) sorted numerically, followed by text ordered by memcmp() and
** finally blob's ordered by memcmp().
**
** Two NULL values are considered equal by this function.
*/
int sqlite3MemCompare(Mem *pMem1, Mem *pMem2){
  int rc;
  int combined_flags = pMem1->flags|pMem2->flags; 
 
  /* If one value is NULL, it is less than the other. If both values
  ** are NULL, return 0.
  */
  if( combined_flags&MEM_Null ){
    return (pMem2->flags&MEM_Null) - (pMem1->flags&MEM_Null);
  }

  /* If one value is a number and the other is not, the number is less.
  ** If both are numbers, compare as reals if one is a real, or as integers
  ** if both values are integers.
  */
  if( combined_flags&(MEM_Int|MEM_Real) ){
    i64 diff;
    if( !(pMem1->flags&(MEM_Int|MEM_Real)) ){
      return 1;
    }
    if( !(pMem2->flags&(MEM_Int|MEM_Real)) ){
      return -1;
    }
    if( combined_flags&MEM_Real ){
      if( pMem1->flags&MEM_Int ){
        pMem1->r = pMem1->i;
      }
      if( pMem2->flags&MEM_Int ){
        pMem2->r = pMem2->i;
      }
      if( pMem1->r < pMem2->r ) return -1;
      if( pMem1->r > pMem2->r ) return 1;
      return 0;
    }
    diff = pMem1->i - pMem2->i;
    return diff<0 ? -1 : diff==0 ? 0 : +1;
  }

  rc = (pMem2->flags&MEM_Null) - (pMem1->flags&MEM_Null);
  if( rc ){
    return rc;
  }

  /* Both values must be strings or blobs. If only one is a string, then
  ** that value is less. Otherwise, compare with memcmp(). If memcmp()
  ** returns 0 and one value is longer than the other, then that value
  ** is greater.
  */
  rc = memcmp(pMem1->z, pMem2->z, (pMem1->n>pMem2->n)?pMem2->n:pMem1->n);
  if( rc ){
    return rc;
  }

  return (pMem1->n - pMem2->n);
}

/*
** The following is the comparison function for (non-integer)
** keys in the btrees.  This function returns negative, zero, or
** positive if the first key is less than, equal to, or greater than
** the second.
**
** This function assumes that each key consists of one or more type/blob
** pairs, encoded using the sqlite3VdbeSerialXXX() functions above. 
**
** Following the type/blob pairs, each key may have a single 0x00 byte
** followed by a varint. A key may only have this traling 0x00/varint
** pair if it has at least as many type/blob pairs as the key it is being
** compared to.
*/
int sqlite3VdbeKeyCompare(
  void *userData,
  int nKey1, const void *pKey1, 
  int nKey2, const void *pKey2
){
  Cursor *pC = (Cursor *)userData;
  int offset1 = 0;
  int offset2 = 0;
  const unsigned char *aKey1 = (const unsigned char *)pKey1;
  const unsigned char *aKey2 = (const unsigned char *)pKey2;
  
  while( offset1<nKey1 && offset2<nKey2 ){
    Mem mem1;
    Mem mem2;
    u64 serial_type1;
    u64 serial_type2;
    int rc;

    /* Read the serial types for the next element in each key. */
    offset1 += sqlite3GetVarint(&aKey1[offset1], &serial_type1);
    offset2 += sqlite3GetVarint(&aKey2[offset2], &serial_type2);

    /* If either of the varints just read in are 0 (not a type), then
    ** this is the end of the keys. The remaining data in each key is
    ** the varint rowid. Compare these as signed integers and return
    ** the result.
    */
    if( !serial_type1 || !serial_type2 ){
      assert( !serial_type1 && !serial_type2 );
      assert( !pC || !pC->incrKey );
      sqlite3GetVarint(&aKey1[offset1], &serial_type1);
      sqlite3GetVarint(&aKey2[offset2], &serial_type2);
      return ( (i64)serial_type1 - (i64)serial_type2 );
    }

    /* Assert that there is enough space left in each key for the blob of
    ** data to go with the serial type just read. This assert may fail if
    ** the file is corrupted.  Then read the value from each key into mem1
    ** and mem2 respectively.
    */
    offset1 += sqlite3VdbeSerialGet(&aKey1[offset1], serial_type1, &mem1);
    offset2 += sqlite3VdbeSerialGet(&aKey2[offset2], serial_type2, &mem2);

    rc = sqlite3MemCompare(&mem1, &mem2);
    if( mem1.flags&MEM_Dyn ){
      sqliteFree(mem1.z);
    }
    if( mem2.flags&MEM_Dyn ){
      sqliteFree(mem2.z);
    }
    if( rc!=0 ){
      return rc;
    }
  }

  /* One of the keys ran out of fields, but all the fields up to that point
  ** were equal. If the incrKey flag is true, then the second key is
  ** treated as larger.
  */
  if( pC && pC->incrKey ){
    assert( offset2==nKey2 );
    return -1;
  }

  if( offset1<nKey1 ){
    return 1;
  }
  if( offset2<nKey2 ){
    return -1;
  }

  return 0;
}

/*
** This function compares the two table row records specified by 
** {nKey1, pKey1} and {nKey2, pKey2}, returning a negative, zero
** or positive integer if {nKey1, pKey1} is less than, equal to or 
** greater than {nKey2, pKey2}.
**
** This function is pretty inefficient and will probably be replaced
** by something else in the near future. It is currently required
** by compound SELECT operators. 
*/
int sqlite3VdbeRowCompare(
  void *userData,
  int nKey1, const void *pKey1, 
  int nKey2, const void *pKey2
){
  Cursor *pC = (Cursor *)userData;
  int offset1 = 0;
  int offset2 = 0;
  int toffset1 = 0;
  int toffset2 = 0;
  int i;
  const unsigned char *aKey1 = (const unsigned char *)pKey1;
  const unsigned char *aKey2 = (const unsigned char *)pKey2;

  assert( pC );
  assert( pC->nField>0 );

  for( i=0; i<pC->nField; i++ ){
    u64 dummy;
    offset1 += sqlite3GetVarint(&aKey1[offset1], &dummy);
    offset2 += sqlite3GetVarint(&aKey1[offset1], &dummy);
  }

  for( i=0; i<pC->nField; i++ ){
    Mem mem1;
    Mem mem2;
    u64 serial_type1;
    u64 serial_type2;
    int rc;

    /* Read the serial types for the next element in each key. */
    toffset1 += sqlite3GetVarint(&aKey1[toffset1], &serial_type1);
    toffset2 += sqlite3GetVarint(&aKey2[toffset2], &serial_type2);

    assert( serial_type1 && serial_type2 );

    /* Assert that there is enough space left in each key for the blob of
    ** data to go with the serial type just read. This assert may fail if
    ** the file is corrupted.  Then read the value from each key into mem1
    ** and mem2 respectively.
    */
    offset1 += sqlite3VdbeSerialGet(&aKey1[offset1], serial_type1, &mem1);
    offset2 += sqlite3VdbeSerialGet(&aKey2[offset2], serial_type2, &mem2);

    rc = sqlite3MemCompare(&mem1, &mem2);
    if( mem1.flags&MEM_Dyn ){
      sqliteFree(mem1.z);
    }
    if( mem2.flags&MEM_Dyn ){
      sqliteFree(mem2.z);
    }
    if( rc!=0 ){
      return rc;
    }
  }

  return 0;
}
  

/*
** pCur points at an index entry. Read the rowid (varint occuring at
** the end of the entry and store it in *rowid. Return SQLITE_OK if
** everything works, or an error code otherwise.
*/
int sqlite3VdbeIdxRowid(BtCursor *pCur, i64 *rowid){
  i64 sz;
  int rc;
  char buf[10];
  int len;
  u64 r;

  rc = sqlite3BtreeKeySize(pCur, &sz);
  if( rc!=SQLITE_OK ){
    return rc;
  }
  len = ((sz>10)?10:sz);

  /* If there are less than 2 bytes in the key, this cannot be
  ** a valid index entry. In practice this comes up for a query
  ** of the sort "SELECT max(x) FROM t1;" when t1 is an empty table
  ** with an index on x. In this case just call the rowid 0.
  */
  if( len<2 ){
    *rowid = 0;
    return SQLITE_OK;
  }

  rc = sqlite3BtreeKey(pCur, sz-len, len, buf);
  if( rc!=SQLITE_OK ){
    return rc;
  }

  len--;
  while( buf[len-1] && --len );

  sqlite3GetVarint(&buf[len], &r);
  *rowid = r;
  return SQLITE_OK;
}

/*
** Compare the key of index entry that cursor pC is point to against
** the key string in pKey (of length nKey).  Write into *pRes a number
** that is negative, zero, or positive if pC is less than, equal to,
** or greater than pKey.  Return SQLITE_OK on success.
*/
int sqlite3VdbeIdxKeyCompare(
  Cursor *pC,                 /* The cursor to compare against */
  int nKey, const u8 *pKey,   /* The key to compare */
  int *res                    /* Write the comparison result here */
){
  unsigned char *pCellKey;
  u64 nCellKey;
  int freeCellKey = 0;
  int rc;
  int len;
  BtCursor *pCur = pC->pCursor;

  sqlite3BtreeKeySize(pCur, &nCellKey);
  if( nCellKey<=0 ){
    *res = 0;
    return SQLITE_OK;
  }

  pCellKey = (unsigned char *)sqlite3BtreeKeyFetch(pCur, nCellKey);
  if( !pCellKey ){
    pCellKey = (unsigned char *)sqliteMallocRaw(nCellKey);
    if( !pCellKey ){
      return SQLITE_NOMEM;
    }
    freeCellKey = 1;
    rc = sqlite3BtreeKey(pCur, 0, nCellKey, pCellKey);
    if( rc!=SQLITE_OK ){
      sqliteFree(pCellKey);
      return rc;
    }
  }
 
  len = nCellKey-2;
  while( pCellKey[len] && --len );

  *res = sqlite3VdbeKeyCompare(pC, len, pCellKey, nKey, pKey);
  
  if( freeCellKey ){
    sqliteFree(pCellKey);
  }
  return SQLITE_OK;
}
