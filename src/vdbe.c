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
** The code in this file implements the Virtual Database Engine (VDBE)
**
** The SQL parser generates a program which is then executed by
** the VDBE to do the work of the SQL statement.  VDBE programs are 
** similar in form to assembly language.  The program consists of
** a linear sequence of operations.  Each operation has an opcode 
** and 3 operands.  Operands P1 and P2 are integers.  Operand P3 
** is a null-terminated string.   The P2 operand must be non-negative.
** Opcodes will typically ignore one or more operands.  Many opcodes
** ignore all three operands.
**
** Computation results are stored on a stack.  Each entry on the
** stack is either an integer or a null-terminated string.  An
** inplicit conversion from one type to the other occurs as necessary.
** 
** Most of the code in this file is taken up by the sqliteVdbeExec()
** function which does the work of interpreting a VDBE program.
** But other routines are also provided to help in building up
** a program instruction by instruction.
**
** $Id: vdbe.c,v 1.38 2000/08/02 12:26:29 drh Exp $
*/
#include "sqliteInt.h"
#include <unistd.h>

/*
** SQL is translated into a sequence of instructions to be
** executed by a virtual machine.  Each instruction is an instance
** of the following structure.
*/
typedef struct VdbeOp Op;

/*
** A cursor is a pointer into a database file.  The database file
** can represent either an SQL table or an SQL index.  Each file is
** a bag of key/data pairs.  The cursor can loop over all key/data
** pairs (in an arbitrary order) or it can retrieve a particular
** key/data pair given a copy of the key.
** 
** Every cursor that the virtual machine has open is represented by an
** instance of the following structure.
*/
struct Cursor {
  DbbeCursor *pCursor;  /* The cursor structure of the backend */
  int index;            /* The next index to extract */
  int keyAsData;        /* The OP_Field command works on key instead of data */
};
typedef struct Cursor Cursor;

/*
** A sorter builds a list of elements to be sorted.  Each element of
** the list is an instance of the following structure.
*/
typedef struct Sorter Sorter;
struct Sorter {
  int nKey;           /* Number of bytes in the key */
  char *zKey;         /* The key by which we will sort */
  int nData;          /* Number of bytes in the data */
  char *pData;        /* The data associated with this key */
  Sorter *pNext;      /* Next in the list */
};

/* 
** Number of buckets used for merge-sort.  
*/
#define NSORT 30

/*
** A single level of the stack is an instance of the following
** structure.  Except, string values are stored on a separate
** list of of pointers to character.  The reason for storing
** strings separately is so that they can be easily passed
** to the callback function.
*/
struct Stack {
  int i;         /* Integer value */
  int n;         /* Number of characters in string value, including '\0' */
  int flags;     /* Some combination of STK_Null, STK_Str, STK_Dyn, etc. */
  double r;      /* Real value */
};
typedef struct Stack Stack;

/*
** Memory cells use the same structure as the stack except that space
** for an arbitrary string is added.
*/
struct Mem {
  Stack s;       /* All values of the memory cell besides string */
  char *z;       /* String value for this memory cell */
};
typedef struct Mem Mem;

/*
** Allowed values for Stack.flags
*/
#define STK_Null      0x0001   /* Value is NULL */
#define STK_Str       0x0002   /* Value is a string */
#define STK_Int       0x0004   /* Value is an integer */
#define STK_Real      0x0008   /* Value is a real number */
#define STK_Dyn       0x0010   /* Need to call sqliteFree() on zStack[*] */

/*
** An Agg structure describes an Aggregator.  Each Agg consists of
** zero or more Aggregator elements (AggElem).  Each AggElem contains
** a key and one or more values.  The values are used in processing
** aggregate functions in a SELECT.  The key is used to implement
** the GROUP BY clause of a select.
*/
typedef struct Agg Agg;
typedef struct AggElem AggElem;
struct Agg {
  int nMem;              /* Number of values stored in each AggElem */
  AggElem *pCurrent;     /* The AggElem currently in focus */
  int nElem;             /* The number of AggElems */
  int nHash;             /* Number of slots in apHash[] */
  AggElem **apHash;      /* A hash array for looking up AggElems by zKey */
  AggElem *pFirst;       /* A list of all AggElems */
};
struct AggElem {
  char *zKey;            /* The key to this AggElem */
  AggElem *pHash;        /* Next AggElem with the same hash on zKey */
  AggElem *pNext;        /* Next AggElem in a list of them all */
  Mem aMem[1];           /* The values for this AggElem */
};

/*
** A Set structure is used for quick testing to see if a value
** is part of a small set.  Sets are used to implement code like
** this:
**            x.y IN ('hi','hoo','hum')
*/
typedef struct Set Set;
typedef struct SetElem SetElem;
struct Set {
  SetElem *pAll;         /* All elements of this set */
  SetElem *apHash[41];   /* A hash array for all elements in this set */
};
struct SetElem {
  SetElem *pHash;        /* Next element with the same hash on zKey */
  SetElem *pNext;        /* Next element in a list of them all */
  char zKey[1];          /* Value of this key */
};

/*
** An instance of the virtual machine
*/
struct Vdbe {
  Dbbe *pBe;          /* Opaque context structure used by DB backend */
  FILE *trace;        /* Write an execution trace here, if not NULL */
  int nOp;            /* Number of instructions in the program */
  int nOpAlloc;       /* Number of slots allocated for aOp[] */
  Op *aOp;            /* Space to hold the virtual machine's program */
  int nLabel;         /* Number of labels used */
  int nLabelAlloc;    /* Number of slots allocated in aLabel[] */
  int *aLabel;        /* Space to hold the labels */
  int tos;            /* Index of top of stack */
  int nStackAlloc;    /* Size of the stack */
  Stack *aStack;      /* The operand stack, except string values */
  char **zStack;      /* Text or binary values of the stack */
  char **azColName;   /* Becomes the 4th parameter to callbacks */
  int nCursor;        /* Number of slots in aCsr[] */
  Cursor *aCsr;       /* On element of this array for each open cursor */
  int nList;          /* Number of slots in apList[] */
  FILE **apList;      /* An open file for each list */
  int nSort;          /* Number of slots in apSort[] */
  Sorter **apSort;    /* An open sorter list */
  FILE *pFile;        /* At most one open file handler */
  int nField;         /* Number of file fields */
  char **azField;     /* Data for each file field */
  char *zLine;        /* A single line from the input file */
  int nLineAlloc;     /* Number of spaces allocated for zLine */
  int nMem;           /* Number of memory locations currently allocated */
  Mem *aMem;          /* The memory locations */
  Agg agg;            /* Aggregate information */
  int nSet;           /* Number of sets allocated */
  Set *aSet;          /* An array of sets */
  int nFetch;         /* Number of OP_Fetch instructions executed */
};

/*
** Create a new virtual database engine.
*/
Vdbe *sqliteVdbeCreate(Dbbe *pBe){
  Vdbe *p;

  p = sqliteMalloc( sizeof(Vdbe) );
  p->pBe = pBe;
  return p;
}

/*
** Turn tracing on or off
*/
void sqliteVdbeTrace(Vdbe *p, FILE *trace){
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
**    p1, p2, p3      Three operands.
**
**    lbl             A symbolic label for this instruction.
**
** Symbolic labels are negative numbers that stand for the address
** of instructions that have yet to be coded.  When the instruction
** is coded, its real address is substituted in the p2 field of
** prior and subsequent instructions that have the lbl value in
** their p2 fields.
*/
int sqliteVdbeAddOp(Vdbe *p, int op, int p1, int p2, const char *p3, int lbl){
  int i, j;

  i = p->nOp;
  p->nOp++;
  if( i>=p->nOpAlloc ){
    int oldSize = p->nOpAlloc;
    p->nOpAlloc = p->nOpAlloc*2 + 10;
    p->aOp = sqliteRealloc(p->aOp, p->nOpAlloc*sizeof(Op));
    if( p->aOp==0 ){
      p->nOp = 0;
      p->nOpAlloc = 0;
      return 0;
    }
    memset(&p->aOp[oldSize], 0, (p->nOpAlloc-oldSize)*sizeof(Op));
  }
  p->aOp[i].opcode = op;
  p->aOp[i].p1 = p1;
  if( p2<0 && (-1-p2)<p->nLabel && p->aLabel[-1-p2]>=0 ){
    p2 = p->aLabel[-1-p2];
  }
  p->aOp[i].p2 = p2;
  if( p3 && p3[0] ){
    p->aOp[i].p3 = sqliteStrDup(p3);
  }else{
    p->aOp[i].p3 = 0;
  }
  if( lbl<0 && (-lbl)<=p->nLabel ){
    p->aLabel[-1-lbl] = i;
    for(j=0; j<i; j++){
      if( p->aOp[j].p2==lbl ) p->aOp[j].p2 = i;
    }
  }
  return i;
}

/*
** Resolve label "x" to be the address of the next instruction to
** be inserted.
*/
void sqliteVdbeResolveLabel(Vdbe *p, int x){
  int j;
  if( x<0 && (-x)<=p->nLabel ){
    p->aLabel[-1-x] = p->nOp;
    for(j=0; j<p->nOp; j++){
      if( p->aOp[j].p2==x ) p->aOp[j].p2 = p->nOp;
    }
  }
}

/*
** Return the address of the next instruction to be inserted.
*/
int sqliteVdbeCurrentAddr(Vdbe *p){
  return p->nOp;
}

/*
** Add a whole list of operations to the operation stack.  Return the
** address of the first operation added.
*/
int sqliteVdbeAddOpList(Vdbe *p, int nOp, VdbeOp const *aOp){
  int addr;
  if( p->nOp + nOp >= p->nOpAlloc ){
    int oldSize = p->nOpAlloc;
    p->nOpAlloc = p->nOpAlloc*2 + nOp + 10;
    p->aOp = sqliteRealloc(p->aOp, p->nOpAlloc*sizeof(Op));
    if( p->aOp==0 ){
      p->nOp = 0;
      p->nOpAlloc = 0;
      return 0;
    }
    memset(&p->aOp[oldSize], 0, (p->nOpAlloc-oldSize)*sizeof(Op));
  }
  addr = p->nOp;
  if( nOp>0 ){
    int i;
    for(i=0; i<nOp; i++){
      int p2 = aOp[i].p2;
      if( p2<0 ) p2 = addr + ADDR(p2);
      sqliteVdbeAddOp(p, aOp[i].opcode, aOp[i].p1, p2, aOp[i].p3, 0);
    }
  }
  return addr;
}

/*
** Change the value of the P3 operand for a specific instruction.
** This routine is useful when a large program is loaded from a
** static array using sqliteVdbeAddOpList but we want to make a
** few minor changes to the program.
*/
void sqliteVdbeChangeP3(Vdbe *p, int addr, const char *zP3, int n){
  if( p && addr>=0 && p->nOp>addr && zP3 ){
    sqliteSetNString(&p->aOp[addr].p3, zP3, n, 0);
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
void sqliteVdbeDequoteP3(Vdbe *p, int addr){
  char *z;
  if( addr<0 || addr>=p->nOp ) return;
  z = p->aOp[addr].p3;
  sqliteDequote(z);
}

/*
** On the P3 argument of the given instruction, change all
** strings of whitespace characters into a single space and
** delete leading and trailing whitespace.
*/
void sqliteVdbeCompressSpace(Vdbe *p, int addr){
  char *z;
  int i, j;
  if( addr<0 || addr>=p->nOp ) return;
  z = p->aOp[addr].p3;
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
  while( i>0 && isspace(z[i-1]) ){
    z[i-1] = 0;
    i--;
  }
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
int sqliteVdbeMakeLabel(Vdbe *p){
  int i;
  i = p->nLabel++;
  if( i>=p->nLabelAlloc ){
    p->nLabelAlloc = p->nLabelAlloc*2 + 10;
    p->aLabel = sqliteRealloc( p->aLabel, p->nLabelAlloc*sizeof(int));
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
** Reset an Agg structure.  Delete all its contents.
*/
static void AggReset(Agg *p){
  int i;
  while( p->pFirst ){
    AggElem *pElem = p->pFirst;
    p->pFirst = pElem->pNext;
    for(i=0; i<p->nMem; i++){
      if( pElem->aMem[i].s.flags & STK_Dyn ){
        sqliteFree(pElem->aMem[i].z);
      }
    }
    sqliteFree(pElem);
  }
  sqliteFree(p->apHash);
  memset(p, 0, sizeof(*p));
}

/*
** Add the given AggElem to the hash array
*/
static void AggEnhash(Agg *p, AggElem *pElem){
  int h = sqliteHashNoCase(pElem->zKey, 0) % p->nHash;
  pElem->pHash = p->apHash[h];
  p->apHash[h] = pElem;
}

/*
** Change the size of the hash array to the amount given.
*/
static void AggRehash(Agg *p, int nHash){
  int size;
  AggElem *pElem;
  if( p->nHash==nHash ) return;
  size = nHash * sizeof(AggElem*);
  p->apHash = sqliteRealloc(p->apHash, size );
  memset(p->apHash, 0, size);
  p->nHash = nHash;
  for(pElem=p->pFirst; pElem; pElem=pElem->pNext){
    AggEnhash(p, pElem);
  }
}

/*
** Insert a new element and make it the current element.  
**
** Return 0 on success and 1 if memory is exhausted.
*/
static int AggInsert(Agg *p, char *zKey){
  AggElem *pElem;
  int i;
  if( p->nHash <= p->nElem*2 ){
    AggRehash(p, p->nElem*2 + 19);
  }
  if( p->nHash==0 ) return 1;
  pElem = sqliteMalloc( sizeof(AggElem) + strlen(zKey) + 1 +
                        (p->nMem-1)*sizeof(pElem->aMem[0]) );
  if( pElem==0 ) return 1;
  pElem->zKey = (char*)&pElem->aMem[p->nMem];
  strcpy(pElem->zKey, zKey);
  AggEnhash(p, pElem);
  pElem->pNext = p->pFirst;
  p->pFirst = pElem;
  p->nElem++;
  p->pCurrent = pElem;
  for(i=0; i<p->nMem; i++){
    pElem->aMem[i].s.flags = STK_Null;
  }
  return 0;
}

/*
** Get the AggElem currently in focus
*/
#define AggInFocus(P)   ((P).pCurrent ? (P).pCurrent : _AggInFocus(&(P)))
static AggElem *_AggInFocus(Agg *p){
  AggElem *pFocus = p->pFirst;
  if( pFocus ){
    p->pCurrent = pFocus;
  }else{
    AggInsert(p,"");
    pFocus = p->pCurrent = p->pFirst;
  }
  return pFocus;
}

/*
** Erase all information from a Set
*/
static void SetClear(Set *p){
  SetElem *pElem, *pNext;
  for(pElem=p->pAll; pElem; pElem=pNext){
    pNext = pElem->pNext;
    sqliteFree(pElem);
  }
  memset(p, 0, sizeof(*p));
}

/*
** Insert a new element into the set
*/
static void SetInsert(Set *p, char *zKey){
  SetElem *pElem;
  int h = sqliteHashNoCase(zKey, 0) % ArraySize(p->apHash);
  for(pElem=p->apHash[h]; pElem; pElem=pElem->pHash){
    if( strcmp(pElem->zKey, zKey)==0 ) return;
  }
  pElem = sqliteMalloc( sizeof(*pElem) + strlen(zKey) );
  if( pElem==0 ) return;
  strcpy(pElem->zKey, zKey);
  pElem->pNext = p->pAll;
  p->pAll = pElem;
  pElem->pHash = p->apHash[h];
  p->apHash[h] = pElem;
}

/*
** Return TRUE if an element is in the set.  Return FALSE if not.
*/
static int SetTest(Set *p, char *zKey){
  SetElem *pElem;
  int h = sqliteHashNoCase(zKey, 0) % ArraySize(p->apHash);
  for(pElem=p->apHash[h]; pElem; pElem=pElem->pHash){
    if( strcmp(pElem->zKey, zKey)==0 ) return 1;
  }
  return 0;
}

/*
** Convert the given stack entity into a string if it isn't one
** already.  Return non-zero if we run out of memory.
**
** NULLs are converted into an empty string.
*/
#define Stringify(P,I) \
   ((P->aStack[I].flags & STK_Str)==0 ? hardStringify(P,I) : 0)
static int hardStringify(Vdbe *p, int i){
  char zBuf[30];
  int fg = p->aStack[i].flags;
  if( fg & STK_Real ){
    sprintf(zBuf,"%g",p->aStack[i].r);
  }else if( fg & STK_Int ){
    sprintf(zBuf,"%d",p->aStack[i].i);
  }else{
    p->zStack[i] = "";
    p->aStack[i].n = 1;
    p->aStack[i].flags |= STK_Str;
    return 0;
  }
  p->zStack[i] = sqliteStrDup(zBuf);
  if( p->zStack[i]==0 ) return 1;
  p->aStack[i].n = strlen(p->zStack[i])+1;
  p->aStack[i].flags |= STK_Str|STK_Dyn;
  return 0;
}

/*
** Release the memory associated with the given stack level
*/
#define Release(P,I)  if((P)->aStack[I].flags&STK_Dyn){ hardRelease(P,I); }
static void hardRelease(Vdbe *p, int i){
  sqliteFree(p->zStack[i]);
  p->zStack[i] = 0;
  p->aStack[i].flags &= ~(STK_Str|STK_Dyn);
}

/*
** Convert the given stack entity into a integer if it isn't one
** already.
**
** Any prior string or real representation is invalidated.  
** NULLs are converted into 0.
*/
#define Integerify(P,I) \
    if(((P)->aStack[(I)].flags&STK_Int)==0){ hardIntegerify(P,I); }
static void hardIntegerify(Vdbe *p, int i){
  if( p->aStack[i].flags & STK_Real ){
    p->aStack[i].i = p->aStack[i].r;
    Release(p, i);
  }else if( p->aStack[i].flags & STK_Str ){
    p->aStack[i].i = atoi(p->zStack[i]);
    Release(p, i);
  }else{
    p->aStack[i].i = 0;
  }
  p->aStack[i].flags = STK_Int;
}

/*
** Get a valid Real representation for the given stack element.
**
** Any prior string or integer representation is retained.
** NULLs are converted into 0.0.
*/
#define Realify(P,I) \
    if(((P)->aStack[(I)].flags&STK_Real)==0){ hardRealify(P,I); }
static void hardRealify(Vdbe *p, int i){
  if( p->aStack[i].flags & STK_Str ){
    p->aStack[i].r = atof(p->zStack[i]);
  }else if( p->aStack[i].flags & STK_Int ){
    p->aStack[i].r = p->aStack[i].i;
  }else{
    p->aStack[i].r = 0.0;
  }
  p->aStack[i].flags |= STK_Real;
}

/*
** Pop the stack N times.  Free any memory associated with the
** popped stack elements.
*/
static void PopStack(Vdbe *p, int N){
  if( p->zStack==0 ) return;
  while( p->tos>=0 && N-->0 ){
    int i = p->tos--;
    if( p->aStack[i].flags & STK_Dyn ){
      sqliteFree(p->zStack[i]);
    }
    p->aStack[i].flags = 0;
    p->zStack[i] = 0;
  }    
}

/*
** Make sure space has been allocated to hold at least N
** stack elements.  Allocate additional stack space if
** necessary.
**
** Return 0 on success and non-zero if there are memory
** allocation errors.
*/
#define NeedStack(P,N) (((P)->nStackAlloc<=(N)) ? hardNeedStack(P,N) : 0)
static int hardNeedStack(Vdbe *p, int N){
  int oldAlloc;
  int i;
  if( N>=p->nStackAlloc ){
    oldAlloc = p->nStackAlloc;
    p->nStackAlloc = N + 20;
    p->aStack = sqliteRealloc(p->aStack, p->nStackAlloc*sizeof(p->aStack[0]));
    p->zStack = sqliteRealloc(p->zStack, p->nStackAlloc*sizeof(char*));
    if( p->aStack==0 || p->zStack==0 ){
      sqliteFree(p->aStack);
      sqliteFree(p->zStack);
      p->aStack = 0;
      p->zStack = 0;
      p->nStackAlloc = 0;
      return 1;
    }
    for(i=oldAlloc; i<p->nStackAlloc; i++){
      p->zStack[i] = 0;
      p->aStack[i].flags = 0;
    }
  }
  return 0;
}

/*
** Clean up the VM after execution.
**
** This routine will automatically close any cursors, list, and/or
** sorters that were left open.
*/
static void Cleanup(Vdbe *p){
  int i;
  PopStack(p, p->tos+1);
  sqliteFree(p->azColName);
  p->azColName = 0;
  for(i=0; i<p->nCursor; i++){
    if( p->aCsr[i].pCursor ){
      sqliteDbbeCloseCursor(p->aCsr[i].pCursor);
      p->aCsr[i].pCursor = 0;
    }
  }
  sqliteFree(p->aCsr);
  p->aCsr = 0;
  p->nCursor = 0;
  for(i=0; i<p->nMem; i++){
    if( p->aMem[i].s.flags & STK_Dyn ){
      sqliteFree(p->aMem[i].z);
    }
  }
  sqliteFree(p->aMem);
  p->aMem = 0;
  p->nMem = 0;
  for(i=0; i<p->nList; i++){
    if( p->apList[i] ){
      sqliteDbbeCloseTempFile(p->pBe, p->apList[i]);
      p->apList[i] = 0;
    }
  }
  sqliteFree(p->apList);
  p->apList = 0;
  p->nList = 0;
  for(i=0; i<p->nSort; i++){
    Sorter *pSorter;
    while( (pSorter = p->apSort[i])!=0 ){
      p->apSort[i] = pSorter->pNext;
      sqliteFree(pSorter->zKey);
      sqliteFree(pSorter->pData);
      sqliteFree(pSorter);
    }
  }
  sqliteFree(p->apSort);
  p->apSort = 0;
  p->nSort = 0;
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
  AggReset(&p->agg);
  for(i=0; i<p->nSet; i++){
    SetClear(&p->aSet[i]);
  }
  sqliteFree(p->aSet);
  p->aSet = 0;
  p->nSet = 0;
}

/*
** Delete an entire VDBE.
*/
void sqliteVdbeDelete(Vdbe *p){
  int i;
  if( p==0 ) return;
  Cleanup(p);
  if( p->nOpAlloc==0 ){
    p->aOp = 0;
    p->nOp = 0;
  }
  for(i=0; i<p->nOp; i++){
    sqliteFree(p->aOp[i].p3);
  }
  sqliteFree(p->aOp);
  sqliteFree(p->aLabel);
  sqliteFree(p->aStack);
  sqliteFree(p->zStack);
  sqliteFree(p);
}

/*
** A translation from opcode numbers to opcode names.  Used for testing
** and debugging only.
**
** If any of the numeric OP_ values for opcodes defined in sqliteVdbe.h
** change, be sure to change this array to match.  You can use the
** "opNames.awk" awk script which is part of the source tree to regenerate
** this array, then copy and paste it into this file, if you want.
*/
static char *zOpName[] = { 0,
  "Open",           "Close",          "Fetch",          "Fcnt",
  "New",            "Put",            "Distinct",       "Found",
  "NotFound",       "Delete",         "Field",          "KeyAsData",
  "Key",            "Rewind",         "Next",           "Destroy",
  "Reorganize",     "ResetIdx",       "NextIdx",        "PutIdx",
  "DeleteIdx",      "MemLoad",        "MemStore",       "ListOpen",
  "ListWrite",      "ListRewind",     "ListRead",       "ListClose",
  "SortOpen",       "SortPut",        "SortMakeRec",    "SortMakeKey",
  "Sort",           "SortNext",       "SortKey",        "SortCallback",
  "SortClose",      "FileOpen",       "FileRead",       "FileField",
  "FileClose",      "AggReset",       "AggFocus",       "AggIncr",
  "AggNext",        "AggSet",         "AggGet",         "SetInsert",
  "SetFound",       "SetNotFound",    "SetClear",       "MakeRecord",
  "MakeKey",        "Goto",           "If",             "Halt",
  "ColumnCount",    "ColumnName",     "Callback",       "Integer",
  "String",         "Null",           "Pop",            "Dup",
  "Pull",           "Add",            "AddImm",         "Subtract",
  "Multiply",       "Divide",         "Min",            "Max",
  "Like",           "Glob",           "Eq",             "Ne",
  "Lt",             "Le",             "Gt",             "Ge",
  "IsNull",         "NotNull",        "Negative",       "And",
  "Or",             "Not",            "Concat",         "Noop",
};

/*
** Given the name of an opcode, return its number.  Return 0 if
** there is no match.
**
** This routine is used for testing and debugging.
*/
int sqliteVdbeOpcode(const char *zName){
  int i;
  for(i=1; i<=OP_MAX; i++){
    if( sqliteStrICmp(zName, zOpName[i])==0 ) return i;
  }
  return 0;
}

/*
** Give a listing of the program in the virtual machine.
**
** The interface is the same as sqliteVdbeExec().  But instead of
** running the code, it invokes the callback once for each instruction.
** This feature is used to implement "EXPLAIN".
*/
int sqliteVdbeList(
  Vdbe *p,                   /* The VDBE */
  sqlite_callback xCallback, /* The callback */
  void *pArg,                /* 1st argument to callback */
  char **pzErrMsg            /* Error msg written here */
){
  int i, rc;
  char *azValue[6];
  char zAddr[20];
  char zP1[20];
  char zP2[20];
  static char *azColumnNames[] = {
     "addr", "opcode", "p1", "p2", "p3", 0
  };

  if( xCallback==0 ) return 0;
  azValue[0] = zAddr;
  azValue[2] = zP1;
  azValue[3] = zP2;
  azValue[5] = 0;
  rc = SQLITE_OK;
  /* if( pzErrMsg ){ *pzErrMsg = 0; } */
  for(i=0; rc==SQLITE_OK && i<p->nOp; i++){
    sprintf(zAddr,"%d",i);
    sprintf(zP1,"%d", p->aOp[i].p1);
    sprintf(zP2,"%d", p->aOp[i].p2);
    azValue[4] = p->aOp[i].p3;
    azValue[1] = zOpName[p->aOp[i].opcode];
    if( xCallback(pArg, 5, azValue, azColumnNames) ){
      rc = SQLITE_ABORT;
    }
  }
  return rc;
}

/*
** The parameters are pointers to the head of two sorted lists
** of Sorter structures.  Merge these two lists together and return
** a single sorted list.  This routine forms the core of the merge-sort
** algorithm.
**
** In the case of a tie, left sorts in front of right.
*/
static Sorter *Merge(Sorter *pLeft, Sorter *pRight){
  Sorter sHead;
  Sorter *pTail;
  pTail = &sHead;
  pTail->pNext = 0;
  while( pLeft && pRight ){
    int c = sqliteSortCompare(pLeft->zKey, pRight->zKey);
    if( c<=0 ){
      pTail->pNext = pLeft;
      pLeft = pLeft->pNext;
    }else{
      pTail->pNext = pRight;
      pRight = pRight->pNext;
    }
    pTail = pTail->pNext;
  }
  if( pLeft ){
    pTail->pNext = pLeft;
  }else if( pRight ){
    pTail->pNext = pRight;
  }
  return sHead.pNext;
}

/*
** Execute the program in the VDBE.
**
** If an error occurs, an error message is written to memory obtained
** from sqliteMalloc() and *pzErrMsg is made to point to that memory.
** The return parameter is the number of errors.
**
** If the callback every returns non-zero, then the program exits
** immediately.  No error message but the function does return SQLITE_ABORT.
**
** A memory allocation error causes this routine to return SQLITE_NOMEM
** and abandon furture processing.
**
** Other fatal errors return SQLITE_ERROR.
**
** If a database file could not be opened because it is locked by
** another database instance, then the xBusy() callback is invoked
** with pBusyArg as its first argument, the name of the table as the
** second argument, and the number of times the open has been attempted
** as the third argument.  The xBusy() callback will typically wait
** for the database file to be openable, then return.  If xBusy()
** returns non-zero, another attempt is made to open the file.  If
** xBusy() returns zero, or if xBusy is NULL, then execution halts
** and this routine returns SQLITE_BUSY.
*/
int sqliteVdbeExec(
  Vdbe *p,                   /* The VDBE */
  sqlite_callback xCallback, /* The callback */
  void *pArg,                /* 1st argument to callback */
  char **pzErrMsg,           /* Error msg written here */
  void *pBusyArg,            /* 1st argument to the busy callback */
  int (*xBusy)(void*,const char*,int)  /* Called when a file is busy */
){
  int pc;                    /* The program counter */
  Op *pOp;                   /* Current operation */
  int rc;                    /* Value to return */
  char zBuf[100];            /* Space to sprintf() and integer */

  p->tos = -1;
  rc = SQLITE_OK;
#ifdef MEMORY_DEBUG
  if( access("vdbe_trace",0)==0 ){
    p->trace = stderr;
  }
#endif
  /* if( pzErrMsg ){ *pzErrMsg = 0; } */
  for(pc=0; rc==SQLITE_OK && pc<p->nOp && pc>=0; pc++){
    pOp = &p->aOp[pc];

    /* Only allow tracing if NDEBUG is not defined.
    */
#ifndef NDEBUG
    if( p->trace ){
      fprintf(p->trace,"%4d %-12s %4d %4d %s\n",
        pc, zOpName[pOp->opcode], pOp->p1, pOp->p2,
           pOp->p3 ? pOp->p3 : "");
    }
#endif

    switch( pOp->opcode ){
      /* Opcode:  Goto P2 * *
      **
      ** An unconditional jump to address P2.
      ** The next instruction executed will be 
      ** the one at index P2 from the beginning of
      ** the program.
      */
      case OP_Goto: {
        pc = pOp->p2 - 1;
        break;
      }

      /* Opcode:  Halt * * *
      **
      ** Exit immediately.  All open DBs, Lists, Sorts, etc are closed
      ** automatically.
      */
      case OP_Halt: {
        pc = p->nOp-1;
        break;
      }

      /* Opcode: Integer P1 * *
      **
      ** The integer value P1 is pushed onto the stack.
      */
      case OP_Integer: {
        int i = ++p->tos;
        if( NeedStack(p, p->tos) ) goto no_mem;
        p->aStack[i].i = pOp->p1;
        p->aStack[i].flags = STK_Int;
        break;
      }

      /* Opcode: String * * P3
      **
      ** The string value P3 is pushed onto the stack.
      */
      case OP_String: {
        int i = ++p->tos;
        char *z;
        if( NeedStack(p, p->tos) ) goto no_mem;
        z = pOp->p3;
        if( z==0 ) z = "";
        p->zStack[i] = z;
        p->aStack[i].n = strlen(z) + 1;
        p->aStack[i].flags = STK_Str;
        break;
      }

      /* Opcode: Null * * *
      **
      ** Push a NULL value onto the stack.
      */
      case OP_Null: {
        int i = ++p->tos;
        if( NeedStack(p, p->tos) ) goto no_mem;
        p->zStack[i] = 0;
        p->aStack[i].flags = STK_Null;
        break;
      }

      /* Opcode: Pop P1 * *
      **
      ** P1 elements are popped off of the top of stack and discarded.
      */
      case OP_Pop: {
        PopStack(p, pOp->p1);
        break;
      }

      /* Opcode: Dup P1 * *
      **
      ** A copy of the P1-th element of the stack 
      ** is made and pushed onto the top of the stack.
      ** The top of the stack is element 0.  So the
      ** instruction "Dup 0 0 0" will make a copy of the
      ** top of the stack.
      */
      case OP_Dup: {
        int i = p->tos - pOp->p1;
        int j = ++p->tos;
        if( i<0 ) goto not_enough_stack;
        if( NeedStack(p, p->tos) ) goto no_mem;
        p->aStack[j] = p->aStack[i];
        if( p->aStack[i].flags & STK_Dyn ){
          p->zStack[j] = sqliteMalloc( p->aStack[j].n );
          if( p->zStack[j]==0 ) goto no_mem;
          memcpy(p->zStack[j], p->zStack[i], p->aStack[j].n);
        }else{
          p->zStack[j] = p->zStack[i];
        }
        break;
      }

      /* Opcode: Pull P1 * *
      **
      ** The P1-th element is removed from its current location on 
      ** the stack and pushed back on top of the stack.  The
      ** top of the stack is element 0, so "Pull 0 0 0" is
      ** a no-op.
      */
      case OP_Pull: {
        int from = p->tos - pOp->p1;
        int to = p->tos;
        int i;
        Stack ts;
        char *tz;
        if( from<0 ) goto not_enough_stack;
        ts = p->aStack[from];
        tz = p->zStack[from];
        for(i=from; i<to; i++){
          p->aStack[i] = p->aStack[i+1];
          p->zStack[i] = p->zStack[i+1];
        }
        p->aStack[to] = ts;
        p->zStack[to] = tz;
        break;
      }

      /* Opcode: ColumnCount P1 * *
      **
      ** Specify the number of column values that will appear in the
      ** array passed as the 4th parameter to the callback.  No checking
      ** is done.  If this value is wrong, a coredump can result.
      */
      case OP_ColumnCount: {
        p->azColName = sqliteRealloc(p->azColName, (pOp->p1+1)*sizeof(char*));
        if( p->azColName==0 ) goto no_mem;
        p->azColName[pOp->p1] = 0;
        break;
      }

      /* Opcode: ColumnName P1 * P3
      **
      ** P3 becomes the P1-th column name (first is 0).  An array of pointers
      ** to all column names is passed as the 4th parameter to the callback.
      ** The ColumnCount opcode must be executed first to allocate space to
      ** hold the column names.  Failure to do this will likely result in
      ** a coredump.
      */
      case OP_ColumnName: {
        p->azColName[pOp->p1] = pOp->p3 ? pOp->p3 : "";
        break;
      }

      /* Opcode: Callback P1 * *
      **
      ** Pop P1 values off the stack and form them into an array.  Then
      ** invoke the callback function using the newly formed array as the
      ** 3rd parameter.
      */
      case OP_Callback: {
        int i = p->tos - pOp->p1 + 1;
        int j;
        if( i<0 ) goto not_enough_stack;
        if( NeedStack(p, p->tos+2) ) goto no_mem;
        for(j=i; j<=p->tos; j++){
          if( (p->aStack[j].flags & STK_Null)==0 ){
            if( Stringify(p, j) ) goto no_mem;
          }
        }
        p->zStack[p->tos+1] = 0;
        if( xCallback!=0 ){
          if( xCallback(pArg, pOp->p1, &p->zStack[i], p->azColName)!=0 ){
            rc = SQLITE_ABORT;
          }
        }
        PopStack(p, pOp->p1);
        break;
      }

      /* Opcode: Concat P1 P2 P3
      **
      ** Look at the first P1 elements of the stack.  Append them all 
      ** together with the lowest element first.  Use P3 as a separator.  
      ** Put the result on the top of the stack.  The original P1 elements
      ** are popped from the stack if P2==0 and retained if P2==1.
      **
      ** If P3 is NULL, then use no separator.  When P1==1, this routine
      ** makes a copy of the top stack element into memory obtained
      ** from sqliteMalloc().
      */
      case OP_Concat: {
        char *zNew;
        int nByte;
        int nField;
        int i, j;
        char *zSep;
        int nSep;

        nField = pOp->p1;
        zSep = pOp->p3;
        if( zSep==0 ) zSep = "";
        nSep = strlen(zSep);
        if( p->tos+1<nField ) goto not_enough_stack;
        nByte = 1 - nSep;
        for(i=p->tos-nField+1; i<=p->tos; i++){
          if( p->aStack[i].flags & STK_Null ){
            nByte += nSep;
          }else{
            if( Stringify(p, i) ) goto no_mem;
            nByte += p->aStack[i].n - 1 + nSep;
          }
        }
        zNew = sqliteMalloc( nByte );
        if( zNew==0 ) goto no_mem;
        j = 0;
        for(i=p->tos-nField+1; i<=p->tos; i++){
          if( (p->aStack[i].flags & STK_Null)==0 ){
            memcpy(&zNew[j], p->zStack[i], p->aStack[i].n-1);
            j += p->aStack[i].n-1;
          }
          if( nSep>0 && i<p->tos ){
            memcpy(&zNew[j], zSep, nSep);
            j += nSep;
          }
        }
        zNew[j] = 0;
        if( pOp->p2==0 ) PopStack(p, nField);
        NeedStack(p, p->tos+1);
        p->tos++;
        p->aStack[p->tos].n = nByte;
        p->aStack[p->tos].flags = STK_Str|STK_Dyn;
        p->zStack[p->tos] = zNew;
        break;
      }

      /* Opcode: Add * * *
      **
      ** Pop the top two elements from the stack, add them together,
      ** and push the result back onto the stack.  If either element
      ** is a string then it is converted to a double using the atof()
      ** function before the addition.
      */
      /* Opcode: Multiply * * *
      **
      ** Pop the top two elements from the stack, multiply them together,
      ** and push the result back onto the stack.  If either element
      ** is a string then it is converted to a double using the atof()
      ** function before the multiplication.
      */
      /* Opcode: Subtract * * *
      **
      ** Pop the top two elements from the stack, subtract the
      ** first (what was on top of the stack) from the second (the
      ** next on stack)
      ** and push the result back onto the stack.  If either element
      ** is a string then it is converted to a double using the atof()
      ** function before the subtraction.
      */
      /* Opcode: Divide * * *
      **
      ** Pop the top two elements from the stack, divide the
      ** first (what was on top of the stack) from the second (the
      ** next on stack)
      ** and push the result back onto the stack.  If either element
      ** is a string then it is converted to a double using the atof()
      ** function before the division.  Division by zero returns NULL.
      */
      case OP_Add:
      case OP_Subtract:
      case OP_Multiply:
      case OP_Divide: {
        int tos = p->tos;
        int nos = tos - 1;
        if( nos<0 ) goto not_enough_stack;
        if( (p->aStack[tos].flags & p->aStack[nos].flags & STK_Int)==STK_Int ){
          int a, b;
          a = p->aStack[tos].i;
          b = p->aStack[nos].i;
          switch( pOp->opcode ){
            case OP_Add:         b += a;       break;
            case OP_Subtract:    b -= a;       break;
            case OP_Multiply:    b *= a;       break;
            default: {
              if( a==0 ) goto divide_by_zero;
              b /= a;
              break;
            }
          }
          PopStack(p, 2);
          p->tos = nos;
          p->aStack[nos].i = b;
          p->aStack[nos].flags = STK_Int;
        }else{
          double a, b;
          Realify(p, tos);
          Realify(p, nos);
          a = p->aStack[tos].r;
          b = p->aStack[nos].r;
          switch( pOp->opcode ){
            case OP_Add:         b += a;       break;
            case OP_Subtract:    b -= a;       break;
            case OP_Multiply:    b *= a;       break;
            default: {
              if( a==0.0 ) goto divide_by_zero;
              b /= a;
              break;
            }
          }
          PopStack(p, 1);
          Release(p, nos);
          p->aStack[nos].r = b;
          p->aStack[nos].flags = STK_Real;
        }
        break;

      divide_by_zero:
        PopStack(p, 2);
        p->tos = nos;
        p->aStack[nos].flags = STK_Null;
        break;
      }

      /* Opcode: Max * * *
      **
      ** Pop the top two elements from the stack then push back the
      ** largest of the two.
      */
      case OP_Max: {
        int tos = p->tos;
        int nos = tos - 1;
        int ft, fn;
        int copy = 0;
        if( nos<0 ) goto not_enough_stack;
        ft = p->aStack[tos].flags;
        fn = p->aStack[nos].flags;
        if( fn & STK_Null ){
          copy = 1;
        }else if( (ft & fn & STK_Int)==STK_Int ){
          copy = p->aStack[nos].i<p->aStack[tos].i;
        }else if( ( (ft|fn) & (STK_Int|STK_Real) ) !=0 ){
          Realify(p, tos);
          Realify(p, nos);
          copy = p->aStack[tos].r>p->aStack[nos].r;
        }else{
          Stringify(p, tos);
          Stringify(p, nos);
          copy = sqliteCompare(p->zStack[tos],p->zStack[nos])>0;
        }
        if( copy ){
          Release(p, nos);
          p->aStack[nos] = p->aStack[tos];
          p->zStack[nos] = p->zStack[tos];
          p->zStack[tos] = 0;
          p->aStack[tos].flags = 0;
        }else{
          Release(p, tos);
        }
        p->tos = nos;
        break;
      }

      /* Opcode: Min * * *
      **
      ** Pop the top two elements from the stack then push back the
      ** smaller of the two. 
      */
      case OP_Min: {
        int tos = p->tos;
        int nos = tos - 1;
        int ft, fn;
        int copy = 0;
        if( nos<0 ) goto not_enough_stack;
        ft = p->aStack[tos].flags;
        fn = p->aStack[nos].flags;
        if( fn & STK_Null ){
          copy = 1;
        }else if( ft & STK_Null ){
          copy = 0;
        }else if( (ft & fn & STK_Int)==STK_Int ){
          copy = p->aStack[nos].i>p->aStack[tos].i;
        }else if( ( (ft|fn) & (STK_Int|STK_Real) ) !=0 ){
          Realify(p, tos);
          Realify(p, nos);
          copy = p->aStack[tos].r<p->aStack[nos].r;
        }else{
          Stringify(p, tos);
          Stringify(p, nos);
          copy = sqliteCompare(p->zStack[tos],p->zStack[nos])<0;
        }
        if( copy ){
          Release(p, nos);
          p->aStack[nos] = p->aStack[tos];
          p->zStack[nos] = p->zStack[tos];
          p->zStack[tos] = 0;
          p->aStack[tos].flags = 0;
        }else{
          Release(p, tos);
        }
        p->tos = nos;
        break;
      }

      /* Opcode: AddImm  P1 * *
      ** 
      ** Add the value P1 to whatever is on top of the stack.
      */
      case OP_AddImm: {
        int tos = p->tos;
        if( tos<0 ) goto not_enough_stack;
        Integerify(p, tos);
        p->aStack[tos].i += pOp->p1;
        break;
      }

      /* Opcode: Eq * P2 *
      **
      ** Pop the top two elements from the stack.  If they are equal, then
      ** jump to instruction P2.  Otherwise, continue to the next instruction.
      */
      /* Opcode: Ne * P2 *
      **
      ** Pop the top two elements from the stack.  If they are not equal, then
      ** jump to instruction P2.  Otherwise, continue to the next instruction.
      */
      /* Opcode: Lt * P2 *
      **
      ** Pop the top two elements from the stack.  If second element (the
      ** next on stack) is less than the first (the top of stack), then
      ** jump to instruction P2.  Otherwise, continue to the next instruction.
      ** In other words, jump if NOS<TOS.
      */
      /* Opcode: Le * P2 *
      **
      ** Pop the top two elements from the stack.  If second element (the
      ** next on stack) is less than or equal to the first (the top of stack),
      ** then jump to instruction P2. In other words, jump if NOS<=TOS.
      */
      /* Opcode: Gt * P2 *
      **
      ** Pop the top two elements from the stack.  If second element (the
      ** next on stack) is greater than the first (the top of stack),
      ** then jump to instruction P2. In other words, jump if NOS>TOS.
      */
      /* Opcode: Ge * P2 *
      **
      ** Pop the top two elements from the stack.  If second element (the next
      ** on stack) is greater than or equal to the first (the top of stack),
      ** then jump to instruction P2. In other words, jump if NOS>=TOS.
      */
      case OP_Eq:
      case OP_Ne:
      case OP_Lt:
      case OP_Le:
      case OP_Gt:
      case OP_Ge: {
        int tos = p->tos;
        int nos = tos - 1;
        int c;
        int ft, fn;
        if( nos<0 ) goto not_enough_stack;
        ft = p->aStack[tos].flags;
        fn = p->aStack[nos].flags;
        if( (ft & fn)==STK_Int ){
          c = p->aStack[nos].i - p->aStack[tos].i;
        }else{
          Stringify(p, tos);
          Stringify(p, nos);
          c = sqliteCompare(p->zStack[nos], p->zStack[tos]);
        }
        switch( pOp->opcode ){
          case OP_Eq:    c = c==0;     break;
          case OP_Ne:    c = c!=0;     break;
          case OP_Lt:    c = c<0;      break;
          case OP_Le:    c = c<=0;     break;
          case OP_Gt:    c = c>0;      break;
          default:       c = c>=0;     break;
        }
        PopStack(p, 2);
        if( c ) pc = pOp->p2-1;
        break;
      }

      /* Opcode: Like P1 P2 *
      **
      ** Pop the top two elements from the stack.  The top-most is a
      ** "like" pattern -- the right operand of the SQL "LIKE" operator.
      ** The lower element is the string to compare against the like
      ** pattern.  Jump to P2 if the two compare, and fall through without
      ** jumping if they do not.  The '%' in the top-most element matches
      ** any sequence of zero or more characters in the lower element.  The
      ** '_' character in the topmost matches any single character of the
      ** lower element.  Case is ignored for this comparison.
      **
      ** If P1 is not zero, the sense of the test is inverted and we
      ** have a "NOT LIKE" operator.  The jump is made if the two values
      ** are different.
      */
      case OP_Like: {
        int tos = p->tos;
        int nos = tos - 1;
        int c;
        if( nos<0 ) goto not_enough_stack;
        Stringify(p, tos);
        Stringify(p, nos);
        c = sqliteLikeCompare(p->zStack[tos], p->zStack[nos]);
        PopStack(p, 2);
        if( pOp->p1 ) c = !c;
        if( c ) pc = pOp->p2-1;
        break;
      }

      /* Opcode: Glob P1 P2 *
      **
      ** Pop the top two elements from the stack.  The top-most is a
      ** "glob" pattern.  The lower element is the string to compare 
      ** against the glob pattern.
      **
      ** Jump to P2 if the two compare, and fall through without
      ** jumping if they do not.  The '*' in the top-most element matches
      ** any sequence of zero or more characters in the lower element.  The
      ** '?' character in the topmost matches any single character of the
      ** lower element.  [...] matches a range of characters.  [^...]
      ** matches any character not in the range.  Case is significant
      ** for globs.
      **
      ** If P1 is not zero, the sense of the test is inverted and we
      ** have a "NOT GLOB" operator.  The jump is made if the two values
      ** are different.
      */
      case OP_Glob: {
        int tos = p->tos;
        int nos = tos - 1;
        int c;
        if( nos<0 ) goto not_enough_stack;
        Stringify(p, tos);
        Stringify(p, nos);
        c = sqliteGlobCompare(p->zStack[tos], p->zStack[nos]);
        PopStack(p, 2);
        if( pOp->p1 ) c = !c;
        if( c ) pc = pOp->p2-1;
        break;
      }

      /* Opcode: And * * *
      **
      ** Pop two values off the stack.  Take the logical AND of the
      ** two values and push the resulting boolean value back onto the
      ** stack. 
      */
      /* Opcode: Or * * *
      **
      ** Pop two values off the stack.  Take the logical OR of the
      ** two values and push the resulting boolean value back onto the
      ** stack. 
      */
      case OP_And:
      case OP_Or: {
        int tos = p->tos;
        int nos = tos - 1;
        int c;
        if( nos<0 ) goto not_enough_stack;
        Integerify(p, tos);
        Integerify(p, nos);
        if( pOp->opcode==OP_And ){
          c = p->aStack[tos].i && p->aStack[nos].i;
        }else{
          c = p->aStack[tos].i || p->aStack[nos].i;
        }
        PopStack(p, 2);
        p->tos++;
        p->aStack[nos].i = c;
        p->aStack[nos].flags = STK_Int;
        break;
      }

      /* Opcode: Negative * * *
      **
      ** Treat the top of the stack as a numeric quantity.  Replace it
      ** with its additive inverse.
      */
      case OP_Negative: {
        int tos;
        if( (tos = p->tos)<0 ) goto not_enough_stack;
        if( p->aStack[tos].flags & STK_Real ){
          Release(p, tos);
          p->aStack[tos].r = -p->aStack[tos].r;
          p->aStack[tos].flags = STK_Real;
        }else if( p->aStack[tos].flags & STK_Int ){
          Release(p, tos);
          p->aStack[tos].i = -p->aStack[tos].i;
          p->aStack[tos].flags = STK_Int;
        }else{
          Realify(p, tos);
          Release(p, tos);
          p->aStack[tos].r = -p->aStack[tos].r;
          p->aStack[tos].flags = STK_Real;
        }
        break;
      }

      /* Opcode: Not * * *
      **
      ** Interpret the top of the stack as a boolean value.  Replace it
      ** with its complement.
      */
      case OP_Not: {
        int tos = p->tos;
        if( p->tos<0 ) goto not_enough_stack;
        Integerify(p, tos);
        Release(p, tos);
        p->aStack[tos].i = !p->aStack[tos].i;
        p->aStack[tos].flags = STK_Int;
        break;
      }

      /* Opcode: Noop * * *
      **
      ** Do nothing.  This instruction is often useful as a jump
      ** destination.
      */
      case OP_Noop: {
        break;
      }

      /* Opcode: If * P2 *
      **
      ** Pop a single boolean from the stack.  If the boolean popped is
      ** true, then jump to p2.  Otherwise continue to the next instruction.
      ** An integer is false if zero and true otherwise.  A string is
      ** false if it has zero length and true otherwise.
      */
      case OP_If: {
        int c;
        if( p->tos<0 ) goto not_enough_stack;
        Integerify(p, p->tos);
        c = p->aStack[p->tos].i;
        PopStack(p, 1);
        if( c ) pc = pOp->p2-1;
        break;
      }

      /* Opcode: IsNull * P2 *
      **
      ** Pop a single value from the stack.  If the value popped is NULL
      ** then jump to p2.  Otherwise continue to the next 
      ** instruction.
      */
      case OP_IsNull: {
        int c;
        if( p->tos<0 ) goto not_enough_stack;
        c = (p->aStack[p->tos].flags & STK_Null)!=0;
        PopStack(p, 1);
        if( c ) pc = pOp->p2-1;
        break;
      }

      /* Opcode: NotNull * P2 *
      **
      ** Pop a single value from the stack.  If the value popped is not an
      ** empty string, then jump to p2.  Otherwise continue to the next 
      ** instruction.
      */
      case OP_NotNull: {
        int c;
        if( p->tos<0 ) goto not_enough_stack;
        c = (p->aStack[p->tos].flags & STK_Null)==0;
        PopStack(p, 1);
        if( c ) pc = pOp->p2-1;
        break;
      }

      /* Opcode: MakeRecord P1 * *
      **
      ** Convert the top P1 entries of the stack into a single entry
      ** suitable for use as a data record in the database.  To do this
      ** all entries (except NULLs) are converted to strings and 
      ** concatenated.  The null-terminators are preserved by the concatation
      ** and serve as a boundry marker between fields.  The lowest entry
      ** on the stack is the first in the concatenation and the top of
      ** the stack is the last.  After all fields are concatenated, an
      ** index header is added.  The index header consists of P1 integers
      ** which hold the offset of the beginning of each field from the
      ** beginning of the completed record including the header.  The
      ** index for NULL entries is 0.
      */
      case OP_MakeRecord: {
        char *zNewRecord;
        int nByte;
        int nField;
        int i, j;
        int addr;

        nField = pOp->p1;
        if( p->tos+1<nField ) goto not_enough_stack;
        nByte = 0;
        for(i=p->tos-nField+1; i<=p->tos; i++){
          if( (p->aStack[i].flags & STK_Null)==0 ){
            if( Stringify(p, i) ) goto no_mem;
            nByte += p->aStack[i].n;
          }
        }
        nByte += sizeof(int)*nField;
        zNewRecord = sqliteMalloc( nByte );
        if( zNewRecord==0 ) goto no_mem;
        j = 0;
        addr = sizeof(int)*nField;
        for(i=p->tos-nField+1; i<=p->tos; i++){
          if( p->aStack[i].flags & STK_Null ){
            int zero = 0;
            memcpy(&zNewRecord[j], (char*)&zero, sizeof(int));
          }else{
            memcpy(&zNewRecord[j], (char*)&addr, sizeof(int));
            addr += p->aStack[i].n;
          }
          j += sizeof(int);
        }
        for(i=p->tos-nField+1; i<=p->tos; i++){
          if( (p->aStack[i].flags & STK_Null)==0 ){
            memcpy(&zNewRecord[j], p->zStack[i], p->aStack[i].n);
            j += p->aStack[i].n;
          }
        }
        PopStack(p, nField);
        NeedStack(p, p->tos+1);
        p->tos++;
        p->aStack[p->tos].n = nByte;
        p->aStack[p->tos].flags = STK_Str | STK_Dyn;
        p->zStack[p->tos] = zNewRecord;
        break;
      }

      /* Opcode: MakeKey P1 P2 *
      **
      ** Convert the top P1 entries of the stack into a single entry suitable
      ** for use as the key in an index or a sort.  The top P1 records are
      ** concatenated with a tab character (ASCII 0x09) used as a record
      ** separator.  The entire concatenation is null-terminated.  The
      ** lowest entry in the stack is the first field and the top of the
      ** stack becomes the last.
      **
      ** If P2 is not zero, then the original entries remain on the stack
      ** and the new key is pushed on top.  If P2 is zero, the original
      ** data is popped off the stack first then the new key is pushed
      ** back in its place.
      **
      ** See also the SortMakeKey opcode.
      */
      case OP_MakeKey: {
        char *zNewKey;
        int nByte;
        int nField;
        int i, j;

        nField = pOp->p1;
        if( p->tos+1<nField ) goto not_enough_stack;
        nByte = 0;
        for(i=p->tos-nField+1; i<=p->tos; i++){
          if( p->aStack[i].flags & STK_Null ){
            nByte++;
          }else{
            if( Stringify(p, i) ) goto no_mem;
            nByte += p->aStack[i].n;
          }
        }
        zNewKey = sqliteMalloc( nByte );
        if( zNewKey==0 ) goto no_mem;
        j = 0;
        for(i=p->tos-nField+1; i<=p->tos; i++){
          if( (p->aStack[i].flags & STK_Null)==0 ){
            memcpy(&zNewKey[j], p->zStack[i], p->aStack[i].n-1);
            j += p->aStack[i].n-1;
          }
          if( i<p->tos ) zNewKey[j++] = '\t';
        }
        zNewKey[j] = 0;
        if( pOp->p2==0 ) PopStack(p, nField);
        NeedStack(p, p->tos+1);
        p->tos++;
        p->aStack[p->tos].n = nByte;
        p->aStack[p->tos].flags = STK_Str|STK_Dyn;
        p->zStack[p->tos] = zNewKey;
        break;
      }

      /* Opcode: Open P1 P2 P3
      **
      ** Open a new cursor for the database file named P3.  Give the
      ** cursor an identifier P1.  The P1 values need not be
      ** contiguous but all P1 values should be small integers.  It is
      ** an error for P1 to be negative.
      **
      ** Open readonly if P2==0 and for reading and writing if P2!=0.
      ** The file is created if it does not already exist and P2!=0.
      ** If there is already another cursor opened with identifier P1,
      ** then the old cursor is closed first.  All cursors are
      ** automatically closed when the VDBE finishes execution.
      **
      ** If P3 is null or an empty string, a temporary database file
      ** is created.  This temporary database file is automatically 
      ** deleted when the cursor is closed.
      */
      case OP_Open: {
        int busy = 0;
        int i = pOp->p1;
        if( i<0 ) goto bad_instruction;
        if( i>=p->nCursor ){
          int j;
          p->aCsr = sqliteRealloc( p->aCsr, (i+1)*sizeof(Cursor) );
          if( p->aCsr==0 ){ p->nCursor = 0; goto no_mem; }
          for(j=p->nCursor; j<=i; j++) p->aCsr[j].pCursor = 0;
          p->nCursor = i+1;
        }else if( p->aCsr[i].pCursor ){
          sqliteDbbeCloseCursor(p->aCsr[i].pCursor);
        }
        do {
          rc = sqliteDbbeOpenCursor(p->pBe,pOp->p3,pOp->p2,&p->aCsr[i].pCursor);
          switch( rc ){
            case SQLITE_BUSY: {
              if( xBusy==0 || (*xBusy)(pBusyArg, pOp->p3, ++busy)==0 ){
                sqliteSetString(pzErrMsg,"table ", pOp->p3, " is locked", 0);
                busy = 0;
              }
              break;
            }
            case SQLITE_PERM: {
              sqliteSetString(pzErrMsg, pOp->p2 ? "write" : "read",
                " permission denied for table ", pOp->p3, 0);
              break;
            }
            case SQLITE_READONLY: {
              sqliteSetString(pzErrMsg,"table ", pOp->p3, 
                 " is readonly", 0);
              break;
            }
            case SQLITE_NOMEM: {
              goto no_mem;
            }
            case SQLITE_OK: {
              busy = 0;
              break;
            }
          }
        }while( busy );
        p->aCsr[i].index = 0;
        p->aCsr[i].keyAsData = 0;
        break;
      }

      /* Opcode: Close P1 * *
      **
      ** Close a cursor previously opened as P1.  If P1 is not
      ** currently open, this instruction is a no-op.
      */
      case OP_Close: {
        int i = pOp->p1;
        if( i>=0 && i<p->nCursor && p->aCsr[i].pCursor ){
          sqliteDbbeCloseCursor(p->aCsr[i].pCursor);
          p->aCsr[i].pCursor = 0;
        }
        break;
      }

      /* Opcode: Fetch P1 * *
      **
      ** Pop the top of the stack and use its value as a key to fetch
      ** a record from cursor P1.  The key/data pair is held
      ** in the P1 cursor until needed.
      */
      case OP_Fetch: {
        int i = pOp->p1;
        int tos = p->tos;
        if( tos<0 ) goto not_enough_stack;
        if( i>=0 && i<p->nCursor && p->aCsr[i].pCursor ){
          if( p->aStack[tos].flags & STK_Int ){
            sqliteDbbeFetch(p->aCsr[i].pCursor, sizeof(int), 
                           (char*)&p->aStack[tos].i);
          }else{
            if( Stringify(p, tos) ) goto no_mem;
            sqliteDbbeFetch(p->aCsr[i].pCursor, p->aStack[tos].n, 
                           p->zStack[tos]);
          }
          p->nFetch++;
        }
        PopStack(p, 1);
        break;
      }

      /* Opcode: Fcnt * * *
      **
      ** Push an integer onto the stack which is the total number of
      ** OP_Fetch opcodes that have been executed by this virtual machine.
      **
      ** This instruction is used to implement the special fcnt() function
      ** in the SQL dialect that SQLite understands.  fcnt() is used for
      ** testing purposes.
      */
      case OP_Fcnt: {
        int i = ++p->tos;
        if( NeedStack(p, p->tos) ) goto no_mem;
        p->aStack[i].i = p->nFetch;
        p->aStack[i].flags = STK_Int;
        break;
      }

      /* Opcode: Distinct P1 P2 *
      **
      ** Use the top of the stack as a key.  If a record with that key
      ** does not exist in file P1, then jump to P2.  If the record
      ** does already exist, then fall thru.  The record is not retrieved.
      ** The key is not popped from the stack.
      **
      ** This operation is similar to NotFound except that this operation
      ** does not pop the key from the stack.
      */
      /* Opcode: Found P1 P2 *
      **
      ** Use the top of the stack as a key.  If a record with that key
      ** does exist in file P1, then jump to P2.  If the record
      ** does not exist, then fall thru.  The record is not retrieved.
      ** The key is popped from the stack.
      */
      /* Opcode: NotFound P1 P2 *
      **
      ** Use the top of the stack as a key.  If a record with that key
      ** does not exist in file P1, then jump to P2.  If the record
      ** does exist, then fall thru.  The record is not retrieved.
      ** The key is popped from the stack.
      **
      ** The difference between this operation and Distinct is that
      ** Distinct does not pop the key from the stack.
      */
      case OP_Distinct:
      case OP_NotFound:
      case OP_Found: {
        int i = pOp->p1;
        int tos = p->tos;
        int alreadyExists = 0;
        if( tos<0 ) goto not_enough_stack;
        if( i>=0 && i<p->nCursor && p->aCsr[i].pCursor ){
          if( p->aStack[tos].flags & STK_Int ){
            alreadyExists = sqliteDbbeTest(p->aCsr[i].pCursor, sizeof(int), 
                                          (char*)&p->aStack[tos].i);
          }else{
            if( Stringify(p, tos) ) goto no_mem;
            alreadyExists = sqliteDbbeTest(p->aCsr[i].pCursor,p->aStack[tos].n, 
                                           p->zStack[tos]);
          }
        }
        if( pOp->opcode==OP_Found ){
          if( alreadyExists ) pc = pOp->p2 - 1;
        }else{
          if( !alreadyExists ) pc = pOp->p2 - 1;
        }
        if( pOp->opcode!=OP_Distinct ){
          PopStack(p, 1);
        }
        break;
      }

      /* Opcode: New P1 * *
      **
      ** Get a new integer key not previous used by the database file
      ** associated with cursor P1 and push it onto the stack.
      */
      case OP_New: {
        int i = pOp->p1;
        int v;
        if( i<0 || i>=p->nCursor || p->aCsr[i].pCursor==0 ){
          v = 0;
        }else{
          v = sqliteDbbeNew(p->aCsr[i].pCursor);
        }
        NeedStack(p, p->tos+1);
        p->tos++;
        p->aStack[p->tos].i = v;
        p->aStack[p->tos].flags = STK_Int;
        break;
      }

      /* Opcode: Put P1 * *
      **
      ** Write an entry into the database file P1.  A new entry is
      ** created if it doesn't already exist, or the data for an existing
      ** entry is overwritten.  The data is the value on the top of the
      ** stack.  The key is the next value down on the stack.  The stack
      ** is popped twice by this instruction.
      */
      case OP_Put: {
        int tos = p->tos;
        int nos = p->tos-1;
        int i = pOp->p1;
        if( nos<0 ) goto not_enough_stack;
        if( i>=0 && i<p->nCursor && p->aCsr[i].pCursor!=0 ){
          char *zKey;
          int nKey;
          if( (p->aStack[nos].flags & STK_Int)==0 ){
            if( Stringify(p, nos) ) goto no_mem;
            nKey = p->aStack[nos].n;
            zKey = p->zStack[nos];
          }else{
            nKey = sizeof(int);
            zKey = (char*)&p->aStack[nos].i;
          }
          sqliteDbbePut(p->aCsr[i].pCursor, nKey, zKey,
                        p->aStack[tos].n, p->zStack[tos]);
        }
        PopStack(p, 2);
        break;
      }

      /* Opcode: Delete P1 * *
      **
      ** The top of the stack is a key.  Remove this key and its data
      ** from database file P1.  Then pop the stack to discard the key.
      */
      case OP_Delete: {
        int tos = p->tos;
        int i = pOp->p1;
        if( tos<0 ) goto not_enough_stack;
        if( i>=0 && i<p->nCursor && p->aCsr[i].pCursor!=0 ){
          char *zKey;
          int nKey;
          if( p->aStack[tos].flags & STK_Int ){
            nKey = sizeof(int);
            zKey = (char*)&p->aStack[tos].i;
          }else{
            if( Stringify(p, tos) ) goto no_mem;
            nKey = p->aStack[tos].n;
            zKey = p->zStack[tos];
          }
          sqliteDbbeDelete(p->aCsr[i].pCursor, nKey, zKey);
        }
        PopStack(p, 1);
        break;
      }

      /* Opcode: KeyAsData P1 P2 *
      **
      ** Turn the key-as-data mode for cursor P1 either on (if P2==1) or
      ** off (if P2==0).  In key-as-data mode, the OP_Field opcode pulls
      ** data off of the key rather than the data.  This is useful for
      ** processing compound selects.
      */
      case OP_KeyAsData: {
        int i = pOp->p1;
        if( i>=0 && i<p->nCursor && p->aCsr[i].pCursor!=0 ){
          p->aCsr[i].keyAsData = pOp->p2;
        }
        break;
      }

      /* Opcode: Field P1 P2 *
      **
      ** Interpret the data in the most recent fetch from cursor P1
      ** is a structure built using the MakeRecord instruction.
      ** Push onto the stack the value of the P2-th field of that
      ** structure.
      ** 
      ** The value pushed is just a pointer to the data in the cursor.
      ** The value will go away the next time a record is fetched from P1,
      ** or when P1 is closed.  Make a copy of the string (using
      ** "Concat 1 0 0") if it needs to persist longer than that.
      **
      ** If the KeyAsData opcode has previously executed on this cursor,
      ** then the field might be extracted from the key rather than the
      ** data.
      **
      ** Viewed from a higher level, this instruction retrieves the
      ** data from a single column in a particular row of an SQL table
      ** file.  Perhaps the name of this instruction should be
      ** "Column" instead of "Field"...
      */
      case OP_Field: {
        int *pAddr;
        int amt;
        int i = pOp->p1;
        int p2 = pOp->p2;
        int tos = ++p->tos;
        DbbeCursor *pCrsr;
        char *z;

        if( NeedStack(p, tos) ) goto no_mem;
        if( i>=0 && i<p->nCursor && (pCrsr = p->aCsr[i].pCursor)!=0 ){
          if( p->aCsr[i].keyAsData ){
            amt = sqliteDbbeKeyLength(pCrsr);
            if( amt<=sizeof(int)*(p2+1) ){
              p->aStack[tos].flags = STK_Null;
              break;
            }
            pAddr = (int*)sqliteDbbeReadKey(pCrsr, sizeof(int)*p2);
            if( *pAddr==0 ){
              p->aStack[tos].flags = STK_Null;
              break;
            }
            z = sqliteDbbeReadKey(pCrsr, *pAddr);
          }else{
            amt = sqliteDbbeDataLength(pCrsr);
            if( amt<=sizeof(int)*(p2+1) ){
              p->aStack[tos].flags = STK_Null;
              break;
            }
            pAddr = (int*)sqliteDbbeReadData(pCrsr, sizeof(int)*p2);
            if( *pAddr==0 ){
              p->aStack[tos].flags = STK_Null;
              break;
            }
            z = sqliteDbbeReadData(pCrsr, *pAddr);
          }
          p->zStack[tos] = z;
          p->aStack[tos].n = strlen(z) + 1;
          p->aStack[tos].flags = STK_Str;
        }
        break;
      }

      /* Opcode: Key P1 * *
      **
      ** Push onto the stack an integer which is the first 4 bytes of the
      ** the key to the current entry in a sequential scan of the database
      ** file P1.  The sequential scan should have been started using the 
      ** Next opcode.
      */
      case OP_Key: {
        int i = pOp->p1;
        int tos = ++p->tos;
        DbbeCursor *pCrsr;

        if( NeedStack(p, p->tos) ) goto no_mem;
        if( i>=0 && i<p->nCursor && (pCrsr = p->aCsr[i].pCursor)!=0 ){
          char *z = sqliteDbbeReadKey(pCrsr, 0);
          if( p->aCsr[i].keyAsData ){
            p->zStack[tos] = z;
            p->aStack[tos].flags = STK_Str;
            p->aStack[tos].n = sqliteDbbeKeyLength(pCrsr);
          }else{
            memcpy(&p->aStack[tos].i, z, sizeof(int));
            p->aStack[tos].flags = STK_Int;
          }
        }
        break;
      }

      /* Opcode: Rewind P1 * *
      **
      ** The next use of the Key or Field or Next instruction for P1 
      ** will refer to the first entry in the database file.
      */
      case OP_Rewind: {
        int i = pOp->p1;
        if( i>=0 && i<p->nCursor && p->aCsr[i].pCursor!=0 ){
          sqliteDbbeRewind(p->aCsr[i].pCursor);
        }
        break;
      }

      /* Opcode: Next P1 P2 *
      **
      ** Advance P1 to the next key/data pair in the file.  Or, if there are no
      ** more key/data pairs, rewind P1 and jump to location P2.
      */
      case OP_Next: {
        int i = pOp->p1;
        if( i>=0 && i<p->nCursor && p->aCsr[i].pCursor!=0 ){
          if( sqliteDbbeNextKey(p->aCsr[i].pCursor)==0 ){
            pc = pOp->p2 - 1;
          }else{
            p->nFetch++;
          }
        }
        break;
      }

      /* Opcode: ResetIdx P1 * *
      **
      ** Begin treating the current data in cursor P1 as a bunch of integer
      ** keys to records of a (separate) SQL table file.  This instruction
      ** causes the new NextIdx instruction push the first integer table
      ** key in the data.
      */
      case OP_ResetIdx: {
        int i = pOp->p1;
        if( i>=0 && i<p->nCursor ){
          p->aCsr[i].index = 0;
        }
        break;
      }

      /* Opcode: NextIdx P1 P2 *
      **
      ** The P1 cursor points to an SQL index.  The data from the most
      ** recent fetch on that cursor consists of a bunch of integers where
      ** each integer is the key to a record in an SQL table file.
      ** This instruction grabs the next integer table key from the data
      ** of P1 and pushes that integer onto the stack.  The first time
      ** this instruction is executed after a fetch, the first integer 
      ** table key is pushed.  Subsequent integer table keys are pushed 
      ** in each subsequent execution of this instruction.
      **
      ** If there are no more integer table keys in the data of P1
      ** when this instruction is executed, then nothing gets pushed and
      ** there is an immediate jump to instruction P2.
      */
      case OP_NextIdx: {
        int i = pOp->p1;
        int tos = ++p->tos;
        DbbeCursor *pCrsr;

        if( NeedStack(p, p->tos) ) goto no_mem;
        p->zStack[tos] = 0;
        if( i>=0 && i<p->nCursor && (pCrsr = p->aCsr[i].pCursor)!=0 ){
          int *aIdx;
          int nIdx;
          int j, k;
          nIdx = sqliteDbbeDataLength(pCrsr)/sizeof(int);
          aIdx = (int*)sqliteDbbeReadData(pCrsr, 0);
          if( nIdx>1 ){
            k = *(aIdx++);
            if( k>nIdx-1 ) k = nIdx-1;
          }else{
            k = nIdx;
          }
          for(j=p->aCsr[i].index; j<k; j++){
            if( aIdx[j]!=0 ){
              p->aStack[tos].i = aIdx[j];
              p->aStack[tos].flags = STK_Int;
              break;
            }
          }
          if( j>=k ){
            j = -1;
            pc = pOp->p2 - 1;
            PopStack(p, 1);
          }
          p->aCsr[i].index = j+1;
        }
        break;
      }

      /* Opcode: PutIdx P1 * *
      **
      ** The top of the stack hold an SQL index key (probably made using the
      ** MakeKey instruction) and next on stack holds an integer which
      ** the key to an SQL table entry.  Locate the record in cursor P1
      ** that has the same key as on the TOS.  Create a new record if
      ** necessary.  Then append the integer table key to the data for that
      ** record and write it back to the P1 file.
      */
      case OP_PutIdx: {
        int i = pOp->p1;
        int tos = p->tos;
        int nos = tos - 1;
        DbbeCursor *pCrsr;
        if( nos<0 ) goto not_enough_stack;
        if( i>=0 && i<p->nCursor && (pCrsr = p->aCsr[i].pCursor)!=0 ){
          int r;
          int newVal;
          Integerify(p, nos);
          newVal = p->aStack[nos].i;
          if( Stringify(p, tos) ) goto no_mem;
          r = sqliteDbbeFetch(pCrsr, p->aStack[tos].n, p->zStack[tos]);
          if( r==0 ){
            /* Create a new record for this index */
            sqliteDbbePut(pCrsr, p->aStack[tos].n, p->zStack[tos],
                          sizeof(int), (char*)&newVal);
          }else{
            /* Extend the existing record */
            int nIdx;
            int *aIdx;
            int k;
            
            nIdx = sqliteDbbeDataLength(pCrsr)/sizeof(int);
            if( nIdx==1 ){
              aIdx = sqliteMalloc( sizeof(int)*4 );
              if( aIdx==0 ) goto no_mem;
              aIdx[0] = 2;
              sqliteDbbeCopyData(pCrsr, 0, sizeof(int), (char*)&aIdx[1]);
              aIdx[2] = newVal;
              sqliteDbbePut(pCrsr, p->aStack[tos].n, p->zStack[tos],
                    sizeof(int)*4, (char*)aIdx);
              sqliteFree(aIdx);
            }else{
              aIdx = (int*)sqliteDbbeReadData(pCrsr, 0);
              k = aIdx[0];
              if( k<nIdx-1 ){
                aIdx[k+1] = newVal;
                aIdx[0]++;
                sqliteDbbePut(pCrsr, p->aStack[tos].n, p->zStack[tos],
                    sizeof(int)*nIdx, (char*)aIdx);
              }else{
                nIdx *= 2;
                aIdx = sqliteMalloc( sizeof(int)*nIdx );
                if( aIdx==0 ) goto no_mem;
                sqliteDbbeCopyData(pCrsr, 0, sizeof(int)*(k+1), (char*)aIdx);
                aIdx[k+1] = newVal;
                aIdx[0]++;
                sqliteDbbePut(pCrsr, p->aStack[tos].n, p->zStack[tos],
                      sizeof(int)*nIdx, (char*)aIdx);
                sqliteFree(aIdx);
              }
            }              
          }
        }
        PopStack(p, 2);
        break;
      }

      /* Opcode: DeleteIdx P1 * *
      **
      ** The top of the stack is a key and next on stack is integer
      ** which is the key to a record in an SQL table.
      ** Locate the record in the cursor P1 (P1 represents an SQL index)
      ** that has the same key as the top of stack.  Then look through
      ** the integer table-keys contained in the data of the P1 record.
      ** Remove the integer table-key that matches the NOS and write the
      ** revised data back to P1 with the same key.
      **
      ** If this routine removes the very last integer table-key from
      ** the P1 data, then the corresponding P1 record is deleted.
      */
      case OP_DeleteIdx: {
        int i = pOp->p1;
        int tos = p->tos;
        int nos = tos - 1;
        DbbeCursor *pCrsr;
        if( nos<0 ) goto not_enough_stack;
        if( i>=0 && i<p->nCursor && (pCrsr = p->aCsr[i].pCursor)!=0 ){
          int *aIdx;
          int nIdx;
          int j, k;
          int r;
          int oldVal;
          Integerify(p, nos);
          oldVal = p->aStack[nos].i;
          if( Stringify(p, tos) ) goto no_mem;
          r = sqliteDbbeFetch(pCrsr, p->aStack[tos].n, p->zStack[tos]);
          if( r==0 ) break;
          nIdx = sqliteDbbeDataLength(pCrsr)/sizeof(int);
          aIdx = (int*)sqliteDbbeReadData(pCrsr, 0);
          if( (nIdx==1 && aIdx[0]==oldVal) || (aIdx[0]==1 && aIdx[1]==oldVal) ){
            sqliteDbbeDelete(pCrsr, p->aStack[tos].n, p->zStack[tos]);
          }else{
            k = aIdx[0];
            for(j=1; j<=k && aIdx[j]!=oldVal; j++){}
            if( j>k ) break;
            aIdx[j] = aIdx[k];
            aIdx[k] = 0;
            aIdx[0]--;
            if( aIdx[0]*3 + 1 < nIdx ){
              nIdx /= 2;
            }
            sqliteDbbePut(pCrsr, p->aStack[tos].n, p->zStack[tos], 
                          sizeof(int)*nIdx, (char*)aIdx);
          }
        }
        PopStack(p, 2);
        break;
      }

      /* Opcode: Destroy * * P3
      **
      ** Drop the disk file whose name is P3.  All key/data pairs in
      ** the file are deleted and the file itself is removed
      ** from the disk.
      */
      case OP_Destroy: {
        sqliteDbbeDropTable(p->pBe, pOp->p3);
        break;
      }

      /* Opcode: Reorganize * * P3
      **
      ** Compress, optimize, and tidy up the GDBM file named by P3.
      */
      case OP_Reorganize: {
        sqliteDbbeReorganizeTable(p->pBe, pOp->p3);
        break;
      }

      /* Opcode: ListOpen P1 * *
      **
      ** Open a file used for temporary storage of integer table keys.  P1
      ** will server as a handle to this temporary file for future
      ** interactions.  If another temporary file with the P1 handle is
      ** already opened, the prior file is closed and a new one opened
      ** in its place.
      */
      case OP_ListOpen: {
        int i = pOp->p1;
        if( i<0 ) goto bad_instruction;
        if( i>=p->nList ){
          int j;
          p->apList = sqliteRealloc( p->apList, (i+1)*sizeof(FILE*) );
          if( p->apList==0 ){ p->nList = 0; goto no_mem; }
          for(j=p->nList; j<=i; j++) p->apList[j] = 0;
          p->nList = i+1;
        }else if( p->apList[i] ){
          sqliteDbbeCloseTempFile(p->pBe, p->apList[i]);
        }
        rc = sqliteDbbeOpenTempFile(p->pBe, &p->apList[i]);
        if( rc!=SQLITE_OK ){
          sqliteSetString(pzErrMsg, "unable to open a temporary file", 0);
        }
        break;
      }

      /* Opcode: ListWrite P1 * *
      **
      ** Write the integer on the top of the stack
      ** into the temporary storage file P1.
      */
      case OP_ListWrite: {
        int i = pOp->p1;
        if( i<0 ) goto bad_instruction;
        if( p->tos<0 ) goto not_enough_stack;
        if( i<p->nList && p->apList[i]!=0 ){
          int val;
          Integerify(p, p->tos);
          val = p->aStack[p->tos].i;
          PopStack(p, 1);
          fwrite(&val, sizeof(int), 1, p->apList[i]);
        }
        break;
      }

      /* Opcode: ListRewind P1 * *
      **
      ** Rewind the temporary buffer P1 back to the beginning.
      */
      case OP_ListRewind: {
        int i = pOp->p1;
        if( i<0 ) goto bad_instruction;
        if( i<p->nList && p->apList[i]!=0 ){
          rewind(p->apList[i]);
        }
        break;
      }

      /* Opcode: ListRead P1 P2 *
      **
      ** Attempt to read an integer from temporary storage buffer P1
      ** and push it onto the stack.  If the storage buffer is empty, 
      ** push nothing but instead jump to P2.
      */
      case OP_ListRead: {
        int i = pOp->p1;
        int val, amt;
        if( i<0 || i>=p->nList || p->apList[i]==0 ) goto bad_instruction;
        amt = fread(&val, sizeof(int), 1, p->apList[i]);
        if( amt==1 ){
          p->tos++;
          if( NeedStack(p, p->tos) ) goto no_mem;
          p->aStack[p->tos].i = val;
          p->aStack[p->tos].flags = STK_Int;
          p->zStack[p->tos] = 0;
        }else{
          pc = pOp->p2 - 1;
        }
        break;
      }

      /* Opcode: ListClose P1 * *
      **
      ** Close the temporary storage buffer and discard its contents.
      */
      case OP_ListClose: {
        int i = pOp->p1;
        if( i<0 ) goto bad_instruction;
        if( i<p->nList && p->apList[i]!=0 ){
          sqliteDbbeCloseTempFile(p->pBe, p->apList[i]);
          p->apList[i] = 0;
        }
        break;
      }

      /* Opcode: SortOpen P1 * *
      **
      ** Create a new sorter with index P1
      */
      case OP_SortOpen: {
        int i = pOp->p1;
        if( i<0 ) goto bad_instruction;
        if( i>=p->nSort ){
          int j;
          p->apSort = sqliteRealloc( p->apSort, (i+1)*sizeof(Sorter*) );
          if( p->apSort==0 ){ p->nSort = 0; goto no_mem; }
          for(j=p->nSort; j<=i; j++) p->apSort[j] = 0;
          p->nSort = i+1;
        }
        break;
      }

      /* Opcode: SortPut P1 * *
      **
      ** The TOS is the key and the NOS is the data.  Pop both from the stack
      ** and put them on the sorter.
      */
      case OP_SortPut: {
        int i = pOp->p1;
        int tos = p->tos;
        int nos = tos - 1;
        Sorter *pSorter;
        if( i<0 || i>=p->nSort ) goto bad_instruction;
        if( tos<1 ) goto not_enough_stack;
        if( Stringify(p, tos) || Stringify(p, nos) ) goto no_mem;
        pSorter = sqliteMalloc( sizeof(Sorter) );
        if( pSorter==0 ) goto no_mem;
        pSorter->pNext = p->apSort[i];
        p->apSort[i] = pSorter;
        pSorter->nKey = p->aStack[tos].n;
        pSorter->zKey = p->zStack[tos];
        pSorter->nData = p->aStack[nos].n;
        pSorter->pData = p->zStack[nos];
        p->aStack[tos].flags = 0;
        p->aStack[nos].flags = 0;
        p->zStack[tos] = 0;
        p->zStack[nos] = 0;
        p->tos -= 2;
        break;
      }

      /* Opcode: SortMakeRec P1 * *
      **
      ** The top P1 elements are the arguments to a callback.  Form these
      ** elements into a single data entry that can be stored on a sorter
      ** using SortPut and later fed to a callback using SortCallback.
      */
      case OP_SortMakeRec: {
        char *z;
        char **azArg;
        int nByte;
        int nField;
        int i, j;

        nField = pOp->p1;
        if( p->tos+1<nField ) goto not_enough_stack;
        nByte = 0;
        for(i=p->tos-nField+1; i<=p->tos; i++){
          if( (p->aStack[i].flags & STK_Null)==0 ){
            if( Stringify(p, i) ) goto no_mem;
            nByte += p->aStack[i].n;
          }
        }
        nByte += sizeof(char*)*(nField+1);
        azArg = sqliteMalloc( nByte );
        if( azArg==0 ) goto no_mem;
        z = (char*)&azArg[nField+1];
        for(j=0, i=p->tos-nField+1; i<=p->tos; i++, j++){
          if( p->aStack[i].flags & STK_Null ){
            azArg[j] = 0;
          }else{
            azArg[j] = z;
            strcpy(z, p->zStack[i]);
            z += p->aStack[i].n;
          }
        }
        PopStack(p, nField);
        NeedStack(p, p->tos+1);
        p->tos++;
        p->aStack[p->tos].n = nByte;
        p->zStack[p->tos] = (char*)azArg;
        p->aStack[p->tos].flags = STK_Str|STK_Dyn;
        break;
      }

      /* Opcode: SortMakeKey P1 * P3
      **
      ** Convert the top few entries of the stack into a sort key.  The
      ** number of stack entries consumed is the number of characters in 
      ** the string P3.  One character from P3 is prepended to each entry.
      ** The first character of P3 is prepended to the element lowest in
      ** the stack and the last character of P3 is appended to the top of
      ** the stack.  All stack entries are separated by a \000 character
      ** in the result.  The whole key is terminated by two \000 characters
      ** in a row.
      **
      ** See also the MakeKey opcode.
      */
      case OP_SortMakeKey: {
        char *zNewKey;
        int nByte;
        int nField;
        int i, j, k;

        nField = strlen(pOp->p3);
        if( p->tos+1<nField ) goto not_enough_stack;
        nByte = 1;
        for(i=p->tos-nField+1; i<=p->tos; i++){
          if( Stringify(p, i) ) goto no_mem;
          nByte += p->aStack[i].n+2;
        }
        zNewKey = sqliteMalloc( nByte );
        if( zNewKey==0 ) goto no_mem;
        j = 0;
        k = 0;
        for(i=p->tos-nField+1; i<=p->tos; i++){
          zNewKey[j++] = pOp->p3[k++];
          memcpy(&zNewKey[j], p->zStack[i], p->aStack[i].n-1);
          j += p->aStack[i].n-1;
          zNewKey[j++] = 0;
        }
        zNewKey[j] = 0;
        PopStack(p, nField);
        NeedStack(p, p->tos+1);
        p->tos++;
        p->aStack[p->tos].n = nByte;
        p->aStack[p->tos].flags = STK_Str|STK_Dyn;
        p->zStack[p->tos] = zNewKey;
        break;
      }

      /* Opcode: Sort P1 * *
      **
      ** Sort all elements on the given sorter.  The algorithm is a
      ** mergesort.
      */
      case OP_Sort: {
        int j;
        j = pOp->p1;
        if( j<0 ) goto bad_instruction;
        if( j<p->nSort ){
          int i;
          Sorter *pElem;
          Sorter *apSorter[NSORT];
          for(i=0; i<NSORT; i++){
            apSorter[i] = 0;
          }
          while( p->apSort[j] ){
            pElem = p->apSort[j];
            p->apSort[j] = pElem->pNext;
            pElem->pNext = 0;
            for(i=0; i<NSORT-1; i++){
              if( apSorter[i]==0 ){
                apSorter[i] = pElem;
                break;
              }else{
                pElem = Merge(apSorter[i], pElem);
                apSorter[i] = 0;
              }
            }
            if( i>=NSORT-1 ){
              apSorter[NSORT-1] = Merge(apSorter[NSORT-1],pElem);
            }
          }
          pElem = 0;
          for(i=0; i<NSORT; i++){
            pElem = Merge(apSorter[i], pElem);
          }
          p->apSort[j] = pElem;
        }
        break;
      }

      /* Opcode: SortNext P1 P2 *
      **
      ** Push the data for the topmost element in the given sorter onto the
      ** stack, then remove the element from the sorter.
      */
      case OP_SortNext: {
        int i = pOp->p1;
        if( i<0 ) goto bad_instruction;
        if( i<p->nSort && p->apSort[i]!=0 ){
          Sorter *pSorter = p->apSort[i];
          p->apSort[i] = pSorter->pNext;
          p->tos++;
          NeedStack(p, p->tos);
          p->zStack[p->tos] = pSorter->pData;
          p->aStack[p->tos].n = pSorter->nData;
          p->aStack[p->tos].flags = STK_Str|STK_Dyn;
          sqliteFree(pSorter->zKey);
          sqliteFree(pSorter);
        }else{
          pc = pOp->p2 - 1;
        }
        break;
      }

      /* Opcode: SortKey P1 * *
      **
      ** Push the key for the topmost element of the sorter onto the stack.
      ** But don't change the sorter an any other way.
      */
      case OP_SortKey: {
        int i = pOp->p1;
        if( i<0 ) goto bad_instruction;
        if( i<p->nSort && p->apSort[i]!=0 ){
          Sorter *pSorter = p->apSort[i];
          p->tos++;
          NeedStack(p, p->tos);
          sqliteSetString(&p->zStack[p->tos], pSorter->zKey, 0);
          p->aStack[p->tos].n = pSorter->nKey;
          p->aStack[p->tos].flags = STK_Str|STK_Dyn;
        }
        break;
      }

      /* Opcode: SortCallback P1 P2 *
      **
      ** The top of the stack contains a callback record built using
      ** the SortMakeRec operation with the same P1 value as this
      ** instruction.  Pop this record from the stack and invoke the
      ** callback on it.
      */
      case OP_SortCallback: {
        int i = p->tos;
        if( i<0 ) goto not_enough_stack;
        if( xCallback!=0 ){
          if( xCallback(pArg, pOp->p1, (char**)p->zStack[i], p->azColName) ){
            rc = SQLITE_ABORT;
          }
        }
        PopStack(p, 1);
        break;
      }

      /* Opcode: SortClose P1 * *
      **
      ** Close the given sorter and remove all its elements.
      */
      case OP_SortClose: {
        Sorter *pSorter;
        int i = pOp->p1;
        if( i<0 ) goto bad_instruction;
        if( i<p->nSort ){
           while( (pSorter = p->apSort[i])!=0 ){
             p->apSort[i] = pSorter->pNext;
             sqliteFree(pSorter->zKey);
             sqliteFree(pSorter->pData);
             sqliteFree(pSorter);
           }
        }
        break;
      }

      /* Opcode: FileOpen * * P3
      **
      ** Open the file named by P3 for reading using the FileRead opcode.
      ** If P3 is "stdin" then open standard input for reading.
      */
      case OP_FileOpen: {
        if( pOp->p3==0 ) goto bad_instruction;
        if( p->pFile ){
          if( p->pFile!=stdin ) fclose(p->pFile);
          p->pFile = 0;
        }
        if( sqliteStrICmp(pOp->p3,"stdin")==0 ){
          p->pFile = stdin;
        }else{
          p->pFile = fopen(pOp->p3, "r");
        }
        if( p->pFile==0 ){
          sqliteSetString(pzErrMsg,"unable to open file: ", pOp->p3, 0);
          rc = SQLITE_ERROR;
          goto cleanup;
        }
        break;
      }

      /* Opcode: FileClose * * *
      **
      ** Close a file previously opened using FileOpen.  This is a no-op
      ** if there is no prior FileOpen call.
      */
      case OP_FileClose: {
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
        break;
      }

      /* Opcode: FileRead P1 P2 P3
      **
      ** Read a single line of input from the open file (the file opened using
      ** FileOpen).  If we reach end-of-file, jump immediately to P2.  If
      ** we are able to get another line, split the line apart using P3 as
      ** a delimiter.  There should be P1 fields.  If the input line contains
      ** more than P1 fields, ignore the excess.  If the input line contains
      ** fewer than P1 fields, assume the remaining fields contain an
      ** empty string.
      */
      case OP_FileRead: {
        int n, eol, nField, i, c, nDelim;
        char *zDelim, *z;
        if( p->pFile==0 ) goto fileread_jump;
        nField = pOp->p1;
        if( nField<=0 ) goto fileread_jump;
        if( nField!=p->nField || p->azField==0 ){
          p->azField = sqliteRealloc(p->azField, sizeof(char*)*nField+1);
          if( p->azField==0 ){
            p->nField = 0;
            goto fileread_jump;
          }
          p->nField = nField;
        }
        n = 0;
        eol = 0;
        while( eol==0 ){
          if( p->zLine==0 || n+200>p->nLineAlloc ){
            p->nLineAlloc = p->nLineAlloc*2 + 300;
            p->zLine = sqliteRealloc(p->zLine, p->nLineAlloc);
            if( p->zLine==0 ){
              p->nLineAlloc = 0;
              goto fileread_jump;
            }
          }
          if( fgets(&p->zLine[n], p->nLineAlloc-n, p->pFile)==0 ){
            eol = 1;
            p->zLine[n] = 0;
          }else{
            while( p->zLine[n] ){ n++; }
            if( n>0 && p->zLine[n-1]=='\n' ){
              n--;
              p->zLine[n] = 0;
              eol = 1;
            }
          }
        }
        if( n==0 ) goto fileread_jump;
        z = p->zLine;
        if( z[0]=='\\' && z[1]=='.' && z[2]==0 ){
          goto fileread_jump;
        }
        zDelim = pOp->p3;
        if( zDelim==0 ) zDelim = "\t";
        c = zDelim[0];
        nDelim = strlen(zDelim);
        p->azField[0] = z;
        for(i=1; *z!=0 && i<=nField; i++){
          int from, to;
          from = to = 0;
          while( z[from] ){
            if( z[from]=='\\' && z[from+1]!=0 ){
              z[to++] = z[from+1];
              from += 2;
              continue;
            }
            if( z[from]==c && strncmp(&z[from],zDelim,nDelim)==0 ) break;
            z[to++] = z[from++];
          }
          if( z[from] ){
            z[to] = 0;
            z += from + nDelim;
            if( i<nField ) p->azField[i] = z;
          }else{
            z[to] = 0;
            z = "";
          }
        }
        while( i<nField ){
          p->azField[i++] = "";
        }
        break;

        /* If we reach end-of-file, or if anything goes wrong, jump here.
        ** This code will cause a jump to P2 */
      fileread_jump:
        pc = pOp->p2 - 1;
        break;
      }

      /* Opcode: FileField P1 * *
      **
      ** Push onto the stack the P1-th field of the most recently read line
      ** from the input file.
      */
      case OP_FileField: {
        int i = pOp->p1;
        char *z;
        if( NeedStack(p, p->tos+1) ) goto no_mem;
        if( i>=0 && i<p->nField && p->azField ){
          z = p->azField[i];
        }else{
          z = 0;
        }
        if( z==0 ) z = "";
        p->tos++;
        p->aStack[p->tos].n = strlen(z) + 1;
        p->zStack[p->tos] = z;
        p->aStack[p->tos].flags = STK_Str;
        break;
      }

      /* Opcode: MemStore P1 * *
      **
      ** Pop a single value of the stack and store that value into memory
      ** location P1.  P1 should be a small integer since space is allocated
      ** for all memory locations between 0 and P1 inclusive.
      */
      case OP_MemStore: {
        int i = pOp->p1;
        int tos = p->tos;
        Mem *pMem;
        char *zOld;
        if( tos<0 ) goto not_enough_stack;
        if( i>=p->nMem ){
          int nOld = p->nMem;
          p->nMem = i + 5;
          p->aMem = sqliteRealloc(p->aMem, p->nMem*sizeof(p->aMem[0]));
          if( p->aMem==0 ) goto no_mem;
          if( nOld<p->nMem ){
            memset(&p->aMem[nOld], 0, sizeof(p->aMem[0])*(p->nMem-nOld));
          }
        }
        pMem = &p->aMem[i];
        if( pMem->s.flags & STK_Dyn ){
          zOld = pMem->z;
        }else{
          zOld = 0;
        }
        pMem->s = p->aStack[tos];
        if( pMem->s.flags & STK_Str ){
          pMem->z = sqliteStrNDup(p->zStack[tos], pMem->s.n);
          pMem->s.flags |= STK_Dyn;
        }
        if( zOld ) sqliteFree(zOld);
        PopStack(p, 1);
        break;
      }

      /* Opcode: MemLoad P1 * *
      **
      ** Push a copy of the value in memory location P1 onto the stack.
      */
      case OP_MemLoad: {
        int tos = ++p->tos;
        int i = pOp->p1;
        if( NeedStack(p, tos) ) goto no_mem;
        if( i<0 || i>=p->nMem ){
          p->aStack[tos].flags = STK_Null;
          p->zStack[tos] = 0;
        }else{
          p->aStack[tos] = p->aMem[i].s;
          if( p->aStack[tos].flags & STK_Str ){
            char *z = sqliteMalloc(p->aStack[tos].n);
            if( z==0 ) goto no_mem;
            memcpy(z, p->aMem[i].z, p->aStack[tos].n);
            p->zStack[tos] = z;
            p->aStack[tos].flags |= STK_Dyn;
          }
        }
        break;
      }

      /* Opcode: AggReset * P2 *
      **
      ** Reset the aggregator so that it no longer contains any data.
      ** Future aggregator elements will contain P2 values each.
      */
      case OP_AggReset: {
        AggReset(&p->agg);
        p->agg.nMem = pOp->p2;
        break;
      }

      /* Opcode: AggFocus * P2 *
      **
      ** Pop the top of the stack and use that as an aggregator key.  If
      ** an aggregator with that same key already exists, then make the
      ** aggregator the current aggregator and jump to P2.  If no aggregator
      ** with the given key exists, create one and make it current but
      ** do not jump.
      **
      ** The order of aggregator opcodes is important.  The order is:
      ** AggReset AggFocus AggNext.  In other words, you must execute
      ** AggReset first, then zero or more AggFocus operations, then
      ** zero or more AggNext operations.  You must not execute an AggFocus
      ** in between an AggNext and an AggReset.
      */
      case OP_AggFocus: {
        int tos = p->tos;
        AggElem *pElem;
        char *zKey;
        int nKey;

        if( tos<0 ) goto not_enough_stack;
        Stringify(p, tos);
        zKey = p->zStack[tos]; 
        nKey = p->aStack[tos].n;
        if( p->agg.nHash<=0 ){
          pElem = 0;
        }else{
          int h = sqliteHashNoCase(zKey, nKey-1) % p->agg.nHash;
          for(pElem=p->agg.apHash[h]; pElem; pElem=pElem->pHash){
            if( strcmp(pElem->zKey, zKey)==0 ) break;
          }
        }
        if( pElem ){
          p->agg.pCurrent = pElem;
          pc = pOp->p2 - 1;
        }else{
          AggInsert(&p->agg, zKey);
        }
        PopStack(p, 1);
        break; 
      }

      /* Opcode: AggIncr P1 P2 *
      **
      ** Increase the integer value in the P2-th field of the aggregate
      ** element current in focus by an amount P1.
      */
      case OP_AggIncr: {
        AggElem *pFocus = AggInFocus(p->agg);
        int i = pOp->p2;
        if( pFocus==0 ) goto no_mem;
        if( i>=0 && i<p->agg.nMem ){
          Mem *pMem = &pFocus->aMem[i];
          if( pMem->s.flags!=STK_Int ){
            if( pMem->s.flags & STK_Int ){
              /* Do nothing */
            }else if( pMem->s.flags & STK_Real ){
              pMem->s.i = pMem->s.r;
            }else if( pMem->s.flags & STK_Str ){
              pMem->s.i = atoi(pMem->z);
            }else{
              pMem->s.i = 0;
            }
            if( pMem->s.flags & STK_Dyn ) sqliteFree(pMem->z);
            pMem->z = 0;
            pMem->s.flags = STK_Int;
          }
          pMem->s.i += pOp->p1;
        }
        break;
      }

      /* Opcode: AggSet * P2 *
      **
      ** Move the top of the stack into the P2-th field of the current
      ** aggregate.  String values are duplicated into new memory.
      */
      case OP_AggSet: {
        AggElem *pFocus = AggInFocus(p->agg);
        int i = pOp->p2;
        int tos = p->tos;
        if( tos<0 ) goto not_enough_stack;
        if( pFocus==0 ) goto no_mem;
        if( i>=0 && i<p->agg.nMem ){
          Mem *pMem = &pFocus->aMem[i];
          char *zOld;
          if( pMem->s.flags & STK_Dyn ){
            zOld = pMem->z;
          }else{
            zOld = 0;
          }
          pMem->s = p->aStack[tos];
          if( pMem->s.flags & STK_Str ){
            pMem->z = sqliteMalloc( p->aStack[tos].n );
            if( pMem->z==0 ) goto no_mem;
            memcpy(pMem->z, p->zStack[tos], pMem->s.n);
            pMem->s.flags |= STK_Str|STK_Dyn;
          }
          if( zOld ) sqliteFree(zOld);
        }
        PopStack(p, 1);
        break;
      }

      /* Opcode: AggGet * P2 *
      **
      ** Push a new entry onto the stack which is a copy of the P2-th field
      ** of the current aggregate.  Strings are not duplicated so
      ** string values will be ephemeral.
      */
      case OP_AggGet: {
        AggElem *pFocus = AggInFocus(p->agg);
        int i = pOp->p2;
        int tos = ++p->tos;
        if( NeedStack(p, tos) ) goto no_mem;
        if( pFocus==0 ) goto no_mem;
        if( i>=0 && i<p->agg.nMem ){
          Mem *pMem = &pFocus->aMem[i];
          p->aStack[tos] = pMem->s;
          p->zStack[tos] = pMem->z;
          p->aStack[tos].flags &= ~STK_Dyn;
        }
        break;
      }

      /* Opcode: AggNext * P2 *
      **
      ** Make the next aggregate value the current aggregate.  The prior
      ** aggregate is deleted.  If all aggregate values have been consumed,
      ** jump to P2.
      **
      ** The order of aggregator opcodes is important.  The order is:
      ** AggReset AggFocus AggNext.  In other words, you must execute
      ** AggReset first, then zero or more AggFocus operations, then
      ** zero or more AggNext operations.  You must not execute an AggFocus
      ** in between an AggNext and an AggReset.
      */
      case OP_AggNext: {
        if( p->agg.nHash ){
          p->agg.nHash = 0;
          sqliteFree(p->agg.apHash);
          p->agg.apHash = 0;
          p->agg.pCurrent = p->agg.pFirst;
        }else if( p->agg.pCurrent==p->agg.pFirst && p->agg.pCurrent!=0 ){
          int i;
          AggElem *pElem = p->agg.pCurrent;
          for(i=0; i<p->agg.nMem; i++){
            if( pElem->aMem[i].s.flags & STK_Dyn ){
              sqliteFree(pElem->aMem[i].z);
            }
          }
          p->agg.pCurrent = p->agg.pFirst = pElem->pNext;
          sqliteFree(pElem);
          p->agg.nElem--;
        }
        if( p->agg.pCurrent==0 ){
          pc = pOp->p2-1;
        }
        break;
      }

      /* Opcode: SetClear P1 * *
      **
      ** Remove all elements from the P1-th Set.
      */
      case OP_SetClear: {
        int i = pOp->p1;
        if( i>=0 && i<p->nSet ){
          SetClear(&p->aSet[i]);
        }
        break;
      }

      /* Opcode: SetInsert P1 * P3
      **
      ** If Set P1 does not exist then create it.  Then insert value
      ** P3 into that set.  If P3 is NULL, then insert the top of the
      ** stack into the set.
      */
      case OP_SetInsert: {
        int i = pOp->p1;
        if( p->nSet<=i ){
          p->aSet = sqliteRealloc(p->aSet, (i+1)*sizeof(p->aSet[0]) );
          if( p->aSet==0 ) goto no_mem;
          memset(&p->aSet[p->nSet], 0, sizeof(p->aSet[0])*(i+1 - p->nSet));
          p->nSet = i+1;
        }
        if( pOp->p3 ){
          SetInsert(&p->aSet[i], pOp->p3);
        }else{
          int tos = p->tos;
          if( tos<0 ) goto not_enough_stack;
          Stringify(p, tos);
          SetInsert(&p->aSet[i], p->zStack[tos]);
          PopStack(p, 1);
        }
        break;
      }

      /* Opcode: SetFound P1 P2 *
      **
      ** Pop the stack once and compare the value popped off with the
      ** contents of set P1.  If the element popped exists in set P1,
      ** then jump to P2.  Otherwise fall through.
      */
      case OP_SetFound: {
        int i = pOp->p1;
        int tos = p->tos;
        if( tos<0 ) goto not_enough_stack;
        Stringify(p, tos);
        if( i>=0 && i<p->nSet && SetTest(&p->aSet[i], p->zStack[tos]) ){
          pc = pOp->p2 - 1;
        }
        PopStack(p, 1);
        break;
      }

      /* Opcode: SetNotFound P1 P2 *
      **
      ** Pop the stack once and compare the value popped off with the
      ** contents of set P1.  If the element popped does not exists in 
      ** set P1, then jump to P2.  Otherwise fall through.
      */
      case OP_SetNotFound: {
        int i = pOp->p1;
        int tos = p->tos;
        if( tos<0 ) goto not_enough_stack;
        Stringify(p, tos);
        if( i>=0 && i<p->nSet && !SetTest(&p->aSet[i], p->zStack[tos]) ){
          pc = pOp->p2 - 1;
        }
        PopStack(p, 1);
        break;
      }

      /* An other opcode is illegal...
      */
      default: {
        sprintf(zBuf,"%d",pOp->opcode);
        sqliteSetString(pzErrMsg, "unknown opcode ", zBuf, 0);
        rc = SQLITE_INTERNAL;
        break;
      }
    }

    /* The following code adds nothing to the actual functionality
    ** of the program.  It is only here for testing and debugging.
    ** On the other hand, it does burn CPU cycles every time through
    ** the evaluator loop.  So we can leave it out when NDEBUG is defined.
    */
#ifndef NDEBUG
    if( pc<-1 || pc>=p->nOp ){
      sqliteSetString(pzErrMsg, "jump destination out of range", 0);
      rc = SQLITE_INTERNAL;
    }
    if( p->trace && p->tos>=0 ){
      int i;
      fprintf(p->trace, "Stack:");
      for(i=p->tos; i>=0 && i>p->tos-5; i--){
        if( p->aStack[i].flags & STK_Null ){
          fprintf(p->trace, " NULL");
        }else if( p->aStack[i].flags & STK_Int ){
          fprintf(p->trace, " i:%d", p->aStack[i].i);
        }else if( p->aStack[i].flags & STK_Real ){
          fprintf(p->trace, " r:%g", p->aStack[i].r);
        }else if( p->aStack[i].flags & STK_Str ){
          if( p->aStack[i].flags & STK_Dyn ){
            fprintf(p->trace, " z:[%.11s]", p->zStack[i]);
          }else{
            fprintf(p->trace, " s:[%.11s]", p->zStack[i]);
          }
        }else{
          fprintf(p->trace, " ???");
        }
      }
      fprintf(p->trace,"\n");
    }
#endif
  }

cleanup:
  Cleanup(p);
  return rc;

  /* Jump to here if a malloc() fails.  It's hard to get a malloc()
  ** to fail on a modern VM computer, so this code is untested.
  */
no_mem:
  Cleanup(p);
  sqliteSetString(pzErrMsg, "out or memory", 0);
  return 1;

  /* Jump to here if a operator is encountered that requires more stack
  ** operands than are currently available on the stack.
  */
not_enough_stack:
  sprintf(zBuf,"%d",pc);
  sqliteSetString(pzErrMsg, "too few operands on stack at ", zBuf, 0);
  rc = SQLITE_INTERNAL;
  goto cleanup;

  /* Jump here if an illegal or illformed instruction is executed.
  */
bad_instruction:
  sprintf(zBuf,"%d",pc);
  sqliteSetString(pzErrMsg, "illegal operation at ", zBuf, 0);
  rc = SQLITE_INTERNAL;
  goto cleanup;
}
