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
** $Id: vdbe.c,v 1.1 2000/05/29 14:26:02 drh Exp $
*/
#include "sqliteInt.h"

/*
** SQL is translated into a sequence of instructions to be
** executed by a virtual machine.  Each instruction is an instance
** of the following structure.
*/
typedef struct VdbeOp Op;

/*
** Every table that the virtual machine has open is represented by an
** instance of the following structure.
*/
struct VdbeTable {
  DbbeTable *pTable;    /* The table structure of the backend */
  int index;            /* The next index to extract */
};
typedef struct VdbeTable VdbeTable;

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
  int *iStack;        /* Integer values of the stack */
  char **zStack;      /* Text or binary values of the stack */
  char **azColName;   /* Becomes the 4th parameter to callbacks */
  int nTable;         /* Number of slots in aTab[] */
  VdbeTable *aTab;    /* On element of this array for each open table */
  int nList;          /* Number of slots in apList[] */
  FILE **apList;      /* An open file for each list */
  int nSort;          /* Number of slots in apSort[] */
  Sorter **apSort;    /* An open sorter list */
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
    sqliteSetString(&p->aOp[i].p3, p3, 0);
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
  int quote;
  int i, j;
  char *z;
  if( addr<0 || addr>=p->nOp ) return;
  z = p->aOp[addr].p3;
  quote = z[0];
  if( quote!='\'' && quote!='"' ) return;
  for(i=1, j=0; z[i]; i++){
    if( z[i]==quote ){
      if( z[i+1]==quote ){
        z[j++] = quote;
        i++;
      }else{
        z[j++] = 0;
        break;
      }
    }else{
      z[j++] = z[i];
    }
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
** Pop the stack N times.  Free any memory associated with the
** popped stack elements.
*/
static void PopStack(Vdbe *p, int N){
  if( p->zStack==0 ) return;
  while( p->tos>=0 && N-->0 ){
    int i = p->tos--;
    sqliteFree(p->zStack[i]);
    p->zStack[i] = 0;
  }    
}

/*
** Clean up the VM after execution.
**
** This routine will automatically close any tables, list, and/or
** sorters that were left open.
*/
static void Cleanup(Vdbe *p){
  int i;
  PopStack(p, p->tos+1);
  sqliteFree(p->azColName);
  p->azColName = 0;
  for(i=0; i<p->nTable; i++){
    if( p->aTab[i].pTable ){
      sqliteDbbeCloseTable(p->aTab[i].pTable);
      p->aTab[i].pTable = 0;
    }
  }
  sqliteFree(p->aTab);
  p->aTab = 0;
  p->nTable = 0;
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
  sqliteFree(p->iStack);
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
  "Open",           "Close",          "Destroy",        "Fetch",
  "New",            "Put",            "Delete",         "Field",
  "Key",            "Rewind",         "Next",           "ResetIdx",
  "NextIdx",        "PutIdx",         "DeleteIdx",      "ListOpen",
  "ListWrite",      "ListRewind",     "ListRead",       "ListClose",
  "SortOpen",       "SortPut",        "SortMakeRec",    "SortMakeKey",
  "Sort",           "SortNext",       "SortKey",        "SortCallback",
  "SortClose",      "MakeRecord",     "MakeKey",        "Goto",
  "If",             "Halt",           "ColumnCount",    "ColumnName",
  "Callback",       "Integer",        "String",         "Pop",
  "Dup",            "Pull",           "Add",            "AddImm",
  "Subtract",       "Multiply",       "Divide",         "Min",
  "Max",            "Eq",             "Ne",             "Lt",
  "Le",             "Gt",             "Ge",             "IsNull",
  "NotNull",        "Negative",       "And",            "Or",
  "Not",            "Concat",         "Noop",         
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
  char *azField[6];
  char zAddr[20];
  char zP1[20];
  char zP2[20];
  static char *azColumnNames[] = {
     "addr", "opcode", "p1", "p2", "p3", 0
  };

  if( xCallback==0 ) return 0;
  azField[0] = zAddr;
  azField[2] = zP1;
  azField[3] = zP2;
  azField[5] = 0;
  rc = 0;
  if( pzErrMsg ){ *pzErrMsg = 0; }
  for(i=0; rc==0 && i<p->nOp; i++){
    sprintf(zAddr,"%d",i);
    sprintf(zP1,"%d", p->aOp[i].p1);
    sprintf(zP2,"%d", p->aOp[i].p2);
    azField[4] = p->aOp[i].p3;
    if( azField[4]==0 ) azField[4] = "";
    azField[1] = zOpName[p->aOp[i].opcode];
    rc = xCallback(pArg, 5, azField, azColumnNames);
  }
  return rc;
}

/*
** Make sure space has been allocated to hold at least N
** stack elements.  Allocate additional stack space if
** necessary.
**
** Return 0 on success and non-zero if there are memory
** allocation errors.
*/
static int NeedStack(Vdbe *p, int N){
  int oldAlloc;
  int i;
  if( N>=p->nStackAlloc ){
    oldAlloc = p->nStackAlloc;
    p->nStackAlloc = N + 20;
    p->iStack = sqliteRealloc(p->iStack, p->nStackAlloc*sizeof(int));
    p->zStack = sqliteRealloc(p->zStack, p->nStackAlloc*sizeof(char*));
    if( p->iStack==0 || p->zStack==0 ){
      sqliteFree(p->iStack);
      sqliteFree(p->zStack);
      p->iStack = 0;
      p->zStack = 0;
      p->nStackAlloc = 0;
      return 1;
    }
    for(i=oldAlloc; i<p->nStackAlloc; i++){
      p->zStack[i] = 0;
    }
  }
  return 0;
}

/*
** Convert the given stack entity into a string if it isn't one
** already.  Return non-zero if we run out of memory.
*/
static int Stringify(Vdbe *p, int i){
  if( p->zStack[i]==0 ){
    char zBuf[30];
    sprintf(zBuf,"%d",p->iStack[i]);
    sqliteSetString(&p->zStack[i], zBuf, 0);
    if( p->zStack[i]==0 ) return 1;
    p->iStack[i] = strlen(p->zStack[i])+1;
  }
  return 0;
}

/*
** Convert the given stack entity into a integer if it isn't one
** already.
*/
static int Integerify(Vdbe *p, int i){
  if( p->zStack[i]!=0 ){
    p->iStack[i] = atoi(p->zStack[i]);
    sqliteFree(p->zStack[i]);
    p->zStack[i] = 0;
  }
  return p->iStack[i];
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
** immediately.  No error message is written but the return value
** from the callback because the return value of this routine.
*/
int sqliteVdbeExec(
  Vdbe *p,                   /* The VDBE */
  sqlite_callback xCallback, /* The callback */
  void *pArg,                /* 1st argument to callback */
  char **pzErrMsg            /* Error msg written here */
){
  int pc;                    /* The program counter */
  Op *pOp;                   /* Current operation */
  int rc;                    /* Value to return */
  char zBuf[100];            /* Space to sprintf() and integer */

  p->tos = -1;
  rc = 0;
  if( pzErrMsg ){ *pzErrMsg = 0; }
  for(pc=0; rc==0 && pc<p->nOp && pc>=0; pc++){
    pOp = &p->aOp[pc];
    if( p->trace ){
      fprintf(p->trace,"%4d %-12s %4d %4d %s\n",
        pc, zOpName[pOp->opcode], pOp->p1, pOp->p2,
           pOp->p3 ? pOp->p3 : "");
    }
    switch( pOp->opcode ){
      /* Opcode:  Goto P2 * *
      **
      ** An unconditional jump to address P2.
      ** The next instruction executed will be 
      ** the one at index P2 from the beginning of
      ** the program.
      */
      case OP_Goto: {
        pc = pOp->p2;
        if( pc<0 || pc>p->nOp ){
          sqliteSetString(pzErrMsg, "jump destination out of range", 0);
          rc = 1;
        }
        pc--;
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
        p->iStack[i] = pOp->p1;
        p->zStack[i] = 0;
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
        p->iStack[i] = strlen(z) + 1;
        sqliteSetString(&p->zStack[i], z, 0);
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
        p->iStack[j] = p->iStack[i];
        if( p->zStack[i] ){
          p->zStack[j] = sqliteMalloc( p->iStack[j] );
          if( p->zStack[j] ) memcpy(p->zStack[j], p->zStack[i], p->iStack[j]);
        }else{
          p->zStack[j] = 0;
        }
        break;
      }

      /* Opcode: Pull P1 * *
      **
      ** The P1-th element is removed its current location on 
      ** the stack and pushed back on top of the stack.  The
      ** top of the stack is element 0, so "Pull 0 0 0" is
      ** a no-op.
      */
      case OP_Pull: {
        int from = p->tos - pOp->p1;
        int to = p->tos;
        int i;
        int ti;
        char *tz;
        if( from<0 ) goto not_enough_stack;
        ti = p->iStack[from];
        tz = p->zStack[from];
        for(i=from; i<to; i++){
          p->iStack[i] = p->iStack[i+1];
          p->zStack[i] = p->zStack[i+1];
        }
        p->iStack[to] = ti;
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
          if( Stringify(p, j) ) goto no_mem;
        }
        p->zStack[p->tos+1] = 0;
        rc = xCallback(pArg, pOp->p1, &p->zStack[i], p->azColName);
        PopStack(p, pOp->p1);
        break;
      }

      /* Opcode: Concat * * *
      **
      ** Pop two elements from the stack.  Append the first (what used
      ** to be the top of stack) to the second (the next on stack) to 
      ** form a new string.  Push the new string back onto the stack.
      */
      case OP_Concat: {
        int tos = p->tos;
        int nos = tos - 1;
        char *z;
        if( nos<0 ) goto not_enough_stack;
        Stringify(p, tos);
        Stringify(p, nos);
        z = 0;
        sqliteSetString(&z, p->zStack[nos], p->zStack[tos], 0);
        PopStack(p, 1);
        sqliteFree(p->zStack[nos]);
        p->zStack[nos] = z;
        p->iStack[nos] = strlen(p->zStack[nos])+1;
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
      ** function before the division.  Division by zero causes the
      ** program to abort with an error.
      */
      case OP_Add:
      case OP_Subtract:
      case OP_Multiply:
      case OP_Divide: {
        int tos = p->tos;
        int nos = tos - 1;
        if( nos<0 ) goto not_enough_stack;
        if( p->zStack[tos]==0 && p->zStack[nos]==0 ){
          int a, b;
          a = p->iStack[tos];
          b = p->iStack[nos];
          switch( pOp->opcode ){
            case OP_Add:         b += a;       break;
            case OP_Subtract:    b -= a;       break;
            case OP_Multiply:    b *= a;       break;
            default: {
              if( a==0 ){ 
                sqliteSetString(pzErrMsg, "division by zero", 0);
                rc = 1;
                goto cleanup;
              }
              b /= a;
              break;
            }
          }
          PopStack(p, 1);
          p->iStack[nos] = b;
        }else{
          double a, b;
          Stringify(p, tos);
          Stringify(p, nos);
          a = atof(p->zStack[tos]);
          b = atof(p->zStack[nos]);
          switch( pOp->opcode ){
            case OP_Add:         b += a;       break;
            case OP_Subtract:    b -= a;       break;
            case OP_Multiply:    b *= a;       break;
            default: {
              if( a==0.0 ){ 
                sqliteSetString(pzErrMsg, "division by zero", 0);
                rc = 1;
                goto cleanup;
              }
              b /= a;
              break;
            }
          }
          sprintf(zBuf,"%g",b);
          PopStack(p, 1);
          sqliteSetString(&p->zStack[nos], zBuf, 0);
          if( p->zStack[nos]==0 ) goto no_mem;
          p->iStack[nos] = strlen(p->zStack[nos]) + 1;
        }
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
        if( nos<0 ) goto not_enough_stack;
        if( p->zStack[tos]==0 && p->zStack[nos]==0 ){
          if( p->iStack[nos]<p->iStack[tos] ){
            p->iStack[nos] = p->iStack[tos];
          }
        }else{
          Stringify(p, tos);
          Stringify(p, nos);
          if( sqliteCompare(p->zStack[nos], p->zStack[tos])<0 ){
            sqliteFree(p->zStack[nos]);
            p->zStack[nos] = p->zStack[tos];
            p->iStack[nos] = p->iStack[tos];
          }
        }
        p->tos--;
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
        if( nos<0 ) goto not_enough_stack;
        if( p->zStack[tos]==0 && p->zStack[nos]==0 ){
          if( p->iStack[nos]>p->iStack[tos] ){
            p->iStack[nos] = p->iStack[tos];
          }
        }else{
          Stringify(p, tos);
          Stringify(p, nos);
          if( sqliteCompare(p->zStack[nos], p->zStack[tos])>0 ){
            sqliteFree(p->zStack[nos]);
            p->zStack[nos] = p->zStack[tos];
            p->iStack[nos] = p->iStack[tos];
          }
        }
        p->tos--;
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
        p->iStack[tos] += pOp->p1;
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
        if( nos<0 ) goto not_enough_stack;
        if( p->zStack[tos]==0 && p->zStack[nos]==0 ){
          int a, b;
          a = p->iStack[tos];
          b = p->iStack[nos];
          switch( pOp->opcode ){
            case OP_Eq:    c = b==a;     break;
            case OP_Ne:    c = b!=a;     break;
            case OP_Lt:    c = b<a;      break;
            case OP_Le:    c = b<=a;     break;
            case OP_Gt:    c = b>a;      break;
            default:       c = b>=a;     break;
          }
        }else{
          Stringify(p, tos);
          Stringify(p, nos);
          c = sqliteCompare(p->zStack[nos], p->zStack[tos]);
          switch( pOp->opcode ){
            case OP_Eq:    c = c==0;     break;
            case OP_Ne:    c = c!=0;     break;
            case OP_Lt:    c = c<0;      break;
            case OP_Le:    c = c<=0;     break;
            case OP_Gt:    c = c>0;      break;
            default:       c = c>=0;     break;
          }
        }
        PopStack(p, 2);
        if( c ) pc = pOp->p2-1;
        break;
      }

      /* Opcode: And * * *
      **
      ** Pop two values off the stack.  Take the logical AND of the
      ** two values and push the resulting boolean value back onto the
      ** stack.  Integers are considered false if zero and true otherwise.
      ** Strings are considered false if their length is zero and true
      ** otherwise.
      */
      /* Opcode: Or * * *
      **
      ** Pop two values off the stack.  Take the logical OR of the
      ** two values and push the resulting boolean value back onto the
      ** stack.  Integers are considered false if zero and true otherwise.
      ** Strings are considered false if their length is zero and true
      ** otherwise.
      */
      case OP_And:
      case OP_Or: {
        int tos = p->tos;
        int nos = tos - 1;
        int x, y, c;
        if( nos<0 ) goto not_enough_stack;
        x = p->zStack[nos] ? p->zStack[nos][0] : p->iStack[nos];
        y = p->zStack[tos] ? p->zStack[tos][0] : p->iStack[tos];
        if( pOp->opcode==OP_And ){
          c = x && y;
        }else{
          c = x || y;
        }
        PopStack(p, 2);
        p->tos++;
        p->iStack[nos] = c;
        break;
      }

      /* Opcode: Negative * * *
      **
      ** Treat the top of the stack as a numeric quantity.  Replace it
      ** with its additive inverse.  If the top of stack is a string,
      ** then it is converted into a number using atof().
      */
      case OP_Negative: {
        int tos;
        if( (tos = p->tos)<0 ) goto not_enough_stack;
        if( p->zStack[tos] ){
          double r = atof(p->zStack[tos]);
          sprintf(zBuf, "%g", -r);
          sqliteSetString(&p->zStack[tos], zBuf, 0);
          p->iStack[tos] = strlen(zBuf) + 1;
        }else{
          p->iStack[tos] = -p->iStack[tos];
        }
        break;
      }

      /* Opcode: Not * * *
      **
      ** Treat the top of the stack as a boolean value.  Replace it
      ** with its complement.  Integers are false if zero and true
      ** otherwise.  Strings are false if zero-length and true otherwise.
      */
      case OP_Not: {
        int c;
        if( p->tos<0 ) goto not_enough_stack;
        c = p->zStack[p->tos] ? p->zStack[p->tos][0] : p->iStack[p->tos];
        PopStack(p, 1);
        p->tos++;
        p->iStack[p->tos] = !c;
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
        c = p->zStack[p->tos] ? p->zStack[p->tos][0] : p->iStack[p->tos];
        PopStack(p, 1);
        if( c ) pc = pOp->p2-1;
        break;
      }

      /* Opcode: IsNull * P2 *
      **
      ** Pop a single value from the stack.  If the value popped is the
      ** empty string, then jump to p2.  Otherwise continue to the next 
      ** instruction.
      */
      case OP_IsNull: {
        int c;
        if( p->tos<0 ) goto not_enough_stack;
        c = p->zStack[p->tos]!=0 && p->zStack[p->tos][0]==0;
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
        c = p->zStack[p->tos]==0 || p->zStack[p->tos][0]!=0;
        PopStack(p, 1);
        if( c ) pc = pOp->p2-1;
        break;
      }

      /* Opcode: MakeRecord P1 * *
      **
      ** Convert the top P1 entries of the stack into a single entry
      ** suitable for use as a data record in the database.  To do this
      ** each entry is converted to a string and all the strings are
      ** concatenated.  The null-terminators are preserved by the concatation
      ** and serve as a boundry marker between fields.  The lowest entry
      ** on the stack is the first in the concatenation and the top of
      ** the stack is the last.  After all fields are concatenated, an
      ** index header is added.  The index header consists of P1 integers
      ** which hold the offset of the beginning of each field from the
      ** beginning of the completed record including the header.
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
          if( Stringify(p, i) ) goto no_mem;
          nByte += p->iStack[i];
        }
        nByte += sizeof(int)*nField;
        zNewRecord = sqliteMalloc( nByte );
        if( zNewRecord==0 ) goto no_mem;
        j = 0;
        addr = sizeof(int)*nField;
        for(i=p->tos-nField+1; i<p->tos; i++){
          memcpy(&zNewRecord[j], (char*)&addr, sizeof(int));
          addr += p->iStack[i];
          j += sizeof(int);
        }
        memcpy(&zNewRecord[j], (char*)&addr, sizeof(int));
        j += sizeof(int);
        for(i=p->tos-nField+1; i<=p->tos; i++){
          memcpy(&zNewRecord[j], p->zStack[i], p->iStack[i]);
          j += p->iStack[i];
        }
        PopStack(p, nField);
        NeedStack(p, p->tos+1);
        p->tos++;
        p->iStack[p->tos] = nByte;
        p->zStack[p->tos] = zNewRecord;
        break;
      }

      /* Opcode: MakeKey P1 * *
      **
      ** Convert the top P1 entries of the stack into a single entry suitable
      ** for use as the key in an index or a sort.  The top P1 records are
      ** concatenated with a tab character (ASCII 0x09) used as a record
      ** separator.  The entire concatenation is null-terminated.  The
      ** lowest entry in the stack is the first field and the top of the
      ** stack becomes the last.
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
          if( Stringify(p, i) ) goto no_mem;
          nByte += p->iStack[i]+1;
        }
        zNewKey = sqliteMalloc( nByte );
        if( zNewKey==0 ) goto no_mem;
        j = 0;
        for(i=p->tos-nField+1; i<=p->tos; i++){
          memcpy(&zNewKey[j], p->zStack[i], p->iStack[i]-1);
          j += p->iStack[i]-1;
          if( i<p->tos ) zNewKey[j++] = '\t';
        }
        zNewKey[j] = 0;
        PopStack(p, nField);
        NeedStack(p, p->tos+1);
        p->tos++;
        p->iStack[p->tos] = nByte;
        p->zStack[p->tos] = zNewKey;
        break;
      }

      /*  Open P1 P3 P2
      **
      ** Open a new database table named P3.  Give it an identifier P1.
      ** Open readonly if P2==0 and for reading and writing if P2!=0.
      ** The table is created if it does not already exist and P2!=0.
      ** If there is already another table opened on P1, then the old
      ** table is closed first.  All tables are automatically closed when
      ** the VDBE finishes execution.  The P1 values need not be
      ** contiguous but all P1 values should be small integers.  It is
      ** an error for P1 to be negative.
      */
      case OP_Open: {
        int i = pOp->p1;
        if( i<0 ) goto bad_instruction;
        if( i>=p->nTable ){
          int j;
          p->aTab = sqliteRealloc( p->aTab, (i+1)*sizeof(VdbeTable) );
          if( p->aTab==0 ){ p->nTable = 0; goto no_mem; }
          for(j=p->nTable; j<=i; j++) p->aTab[j].pTable = 0;
          p->nTable = i+1;
        }else if( p->aTab[i].pTable ){
          sqliteDbbeCloseTable(p->aTab[i].pTable);
        }
        p->aTab[i].pTable = sqliteDbbeOpenTable(p->pBe, pOp->p3, pOp->p2);
        p->aTab[i].index = 0;
        break;
      }

      /* Opcode: Close P1 * *
      **
      ** Close a database table previously opened as P1.  If P1 is not
      ** currently open, this instruction is a no-op.
      */
      case OP_Close: {
        int i = pOp->p1;
        if( i>=0 && i<p->nTable && p->aTab[i].pTable ){
          sqliteDbbeCloseTable(p->aTab[i].pTable);
          p->aTab[i].pTable = 0;
        }
        break;
      }

      /* Opcode: Fetch P1 * *
      **
      ** Pop the top of the stack and use its value as a key to fetch
      ** a record from database table or index P1.  The data is held
      ** in the P1 cursor until needed.  The data is not pushed onto the
      ** stack or anything like that.
      */
      case OP_Fetch: {
        int i = pOp->p1;
        int tos = p->tos;
        if( tos<0 ) goto not_enough_stack;
        if( i>=0 && i<p->nTable && p->aTab[i].pTable ){
          if( p->zStack[tos]==0 ){
            sqliteDbbeFetch(p->aTab[i].pTable, sizeof(int), 
                           (char*)&p->iStack[tos]);
          }else{
            sqliteDbbeFetch(p->aTab[i].pTable, p->iStack[tos], p->zStack[tos]);
          }
        }
        PopStack(p, 1);
        break;
      }

      /* Opcode: New P1 * *
      **
      ** Get a new integer key not previous used by table P1 and
      ** push it onto the stack.
      */
      case OP_New: {
        int i = pOp->p1;
        int v;
        if( i<0 || i>=p->nTable || p->aTab[i].pTable==0 ){
          v = 0;
        }else{
          v = sqliteDbbeNew(p->aTab[i].pTable);
        }
        NeedStack(p, p->tos+1);
        p->tos++;
        p->iStack[p->tos] = v;
        break;
      }

      /* Opcode: Put P1 * *
      **
      ** Write an entry into the database table P1.  A new entry is
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
        if( i>=0 && i<p->nTable && p->aTab[i].pTable!=0 ){
          char *zKey;
          int nKey;
          Stringify(p, tos);
          if( p->zStack[nos]!=0 ){
            nKey = p->iStack[nos];
            zKey = p->zStack[nos];
          }else{
            nKey = sizeof(int);
            zKey = (char*)&p->iStack[nos];
          }
          sqliteDbbePut(p->aTab[i].pTable, nKey, zKey,
                        p->iStack[tos], p->zStack[tos]);
        }
        PopStack(p, 2);
        break;
      }

      /* Opcode: Delete P1 * *
      **
      ** The top of the stack is a key.  Remove this key and its data
      ** from database table P1.  Then pop the stack to discard the key.
      */
      case OP_Delete: {
        int tos = p->tos;
        int i = pOp->p1;
        if( tos<0 ) goto not_enough_stack;
        if( i>=0 && i<p->nTable && p->aTab[i].pTable!=0 ){
          char *zKey;
          int nKey;
          if( p->zStack[tos]!=0 ){
            nKey = p->iStack[tos];
            zKey = p->zStack[tos];
          }else{
            nKey = sizeof(int);
            zKey = (char*)&p->iStack[tos];
          }
          sqliteDbbeDelete(p->aTab[i].pTable, nKey, zKey);
        }
        PopStack(p, 1);
        break;
      }

      /* Opcode: Field P1 P2 *
      **
      ** Push onto the stack the value of the P2-th field from the
      ** most recent Fetch from table P1.
      */
      case OP_Field: {
        int *pAddr;
        int amt;
        int i = pOp->p1;
        int p2 = pOp->p2;
        int tos = ++p->tos;
        DbbeTable *pTab;
        char *z;

        if( NeedStack(p, p->tos) ) goto no_mem;
        if( i>=0 && i<p->nTable && (pTab = p->aTab[i].pTable)!=0 ){
          amt = sqliteDbbeDataLength(pTab);
          if( amt<=sizeof(int)*(p2+1) ){
            sqliteSetString(&p->zStack[tos], "", 0);
            break;
          }
          pAddr = (int*)sqliteDbbeReadData(pTab, sizeof(int)*p2);
          z = sqliteDbbeReadData(pTab, *pAddr);
          sqliteSetString(&p->zStack[tos], z, 0);
          p->iStack[tos] = strlen(z)+1;
        }
        break;
      }

      /* Opcode: Key P1 * *
      **
      ** Push onto the stack an integer which is the first 4 bytes of the
      ** the key to the current entry in a sequential scan of the table P1.
      ** A sequential scan is started using the Next opcode.
      */
      case OP_Key: {
        int i = pOp->p1;
        int tos = ++p->tos;
        DbbeTable *pTab;

        if( NeedStack(p, p->tos) ) goto no_mem;
        if( i>=0 && i<p->nTable && (pTab = p->aTab[i].pTable)!=0 ){
          char *z = sqliteDbbeReadKey(pTab, 0);
          memcpy(&p->iStack[tos], z, sizeof(int));
          p->zStack[tos] = 0;
        }
        break;
      }

      /* Opcode: Rewind P1 * *
      **
      ** The next use of the Key or Field or Next instruction for P1 
      ** will refer to the first entry in the table.
      */
      case OP_Rewind: {
        int i = pOp->p1;
        if( i>=0 && i<p->nTable && p->aTab[i].pTable!=0 ){
          sqliteDbbeRewind(p->aTab[i].pTable);
        }
        break;
      }

      /* Opcode: Next P1 P2 *
      **
      ** Advance P1 to the next entry in the table.  Or, if there are no
      ** more entries, rewind P1 and jump to location P2.
      */
      case OP_Next: {
        int i = pOp->p1;
        if( i>=0 && i<p->nTable && p->aTab[i].pTable!=0 ){
          if( sqliteDbbeNextKey(p->aTab[i].pTable)==0 ){
            pc = pOp->p2;
            if( pc<0 || pc>p->nOp ){
              sqliteSetString(pzErrMsg, "jump destination out of range", 0);
              rc = 1;
            }
            pc--;
          }
        }
        break;
      }

      /* Opcode: ResetIdx P1 * *
      **
      ** Begin treating the current row of table P1 as an index.  The next
      ** NextIdx instruction will refer to the first index in the table.
      */
      case OP_ResetIdx: {
        int i = pOp->p1;
        if( i>=0 && i<p->nTable ){
          p->aTab[i].index = 0;
        }
        break;
      }

      /* Opcode: NextIdx P1 P2 *
      **
      ** Push the next index from the current entry of table P1 onto the
      ** stack and advance the pointer.  If there are no more indices, then
      ** reset the table entry and jump to P2
      */
      case OP_NextIdx: {
        int i = pOp->p1;
        int tos = ++p->tos;
        DbbeTable *pTab;

        if( NeedStack(p, p->tos) ) goto no_mem;
        p->zStack[tos] = 0;
        if( i>=0 && i<p->nTable && (pTab = p->aTab[i].pTable)!=0 ){
          int *aIdx;
          int nIdx;
          int j;
          nIdx = sqliteDbbeDataLength(pTab)/sizeof(int);
          aIdx = (int*)sqliteDbbeReadData(pTab, 0);
          for(j=p->aTab[i].index; j<nIdx; j++){
            if( aIdx[j]!=0 ){
              p->iStack[tos] = aIdx[j];
              break;
            }
          }
          if( j>=nIdx ){
            j = -1;
            pc = pOp->p2;
            if( pc<0 || pc>p->nOp ){
              sqliteSetString(pzErrMsg, "jump destination out of range", 0);
              rc = 1;
            }
            pc--;
          }
          p->aTab[i].index = j+1;
        }
        break;
      }

      /* Opcode: PutIdx P1 * *
      **
      ** The top of the stack hold an index key (proably made using the
      ** MakeKey instruction) and next on stack holds an index value for
      ** a table.  Locate the record in the index P1 that has the key 
      ** and insert the index value into its
      ** data.  Write the results back to the index.
      ** If the key doesn't exist it is created.
      */
      case OP_PutIdx: {
        int i = pOp->p1;
        int tos = p->tos;
        int nos = tos - 1;
        DbbeTable *pTab;
        if( nos<0 ) goto not_enough_stack;
        if( i>=0 && i<p->nTable && (pTab = p->aTab[i].pTable)!=0 ){
          int r;
          int newVal = Integerify(p, nos);
          Stringify(p, tos);
          r = sqliteDbbeFetch(pTab, p->iStack[tos], p->zStack[tos]);
          if( r==0 ){
            /* Create a new record for this index */
            sqliteDbbePut(pTab, p->iStack[tos], p->zStack[tos],
                          sizeof(int), (char*)&newVal);
          }else{
            /* Extend the existing record */
            int nIdx;
            int *aIdx;
            nIdx = sqliteDbbeDataLength(pTab)/sizeof(int);
            aIdx = sqliteMalloc( sizeof(int)*(nIdx+1) );
            if( aIdx==0 ) goto no_mem;
            sqliteDbbeCopyData(pTab, 0, nIdx*sizeof(int), (char*)aIdx);
            aIdx[nIdx] = newVal;
            sqliteDbbePut(pTab, p->iStack[tos], p->zStack[tos],
                          sizeof(int)*(nIdx+1), (char*)aIdx);
            sqliteFree(aIdx);
          }
        }
        PopStack(p, 2);
        break;
      }

      /* Opcode: DeleteIdx P1 * *
      **
      ** The top of the stack is a key and next on stack is an index value.
      ** Locate the record
      ** in index P1 that has the key and remove the index value from its
      ** data.  Write the results back to the table.  If after removing
      ** the index value no more indices remain in the record, then the
      ** record is removed from the table.
      */
      case OP_DeleteIdx: {
        int i = pOp->p1;
        int tos = p->tos;
        int nos = tos - 1;
        DbbeTable *pTab;
        if( nos<0 ) goto not_enough_stack;
        if( i>=0 && i<p->nTable && (pTab = p->aTab[i].pTable)!=0 ){
          int *aIdx;
          int nIdx;
          int j;
          int r;
          int oldVal = Integerify(p, nos);
          Stringify(p, tos);
          r = sqliteDbbeFetch(pTab, p->iStack[tos], p->zStack[tos]);
          if( r==0 ) break;
          nIdx = sqliteDbbeDataLength(pTab)/sizeof(int);
          aIdx = (int*)sqliteDbbeReadData(pTab, 0);
          for(j=0; j<nIdx && aIdx[j]!=oldVal; j++){}
          if( j>=nIdx ) break;
          aIdx[j] = aIdx[nIdx-1];
          if( nIdx==1 ){
            sqliteDbbeDelete(pTab, p->iStack[tos], p->zStack[tos]);
          }else{
            sqliteDbbePut(pTab, p->iStack[tos], p->zStack[tos], 
                          sizeof(int)*(nIdx-1), (char*)aIdx);
          }
        }
        PopStack(p, 2);
        break;
      }

      /* Opcode: Destroy * * P3
      **
      ** Drop the table whose name is P3.  The file that holds this table
      ** is removed from the disk drive.
      */
      case OP_Destroy: {
        sqliteDbbeDropTable(p->pBe, pOp->p3);
        break;
      }

      /* Opcode: ListOpen P1 * *
      **
      ** Open a file used for temporary storage of index numbers.  P1
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
        p->apList[i] = sqliteDbbeOpenTempFile(p->pBe);
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
          int val = Integerify(p, p->tos);
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
      ** and push it onto the stack.  If the storage buffer is empty
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
          p->iStack[p->tos] = val;
          p->zStack[p->tos] = 0;
        }else{
          pc = pOp->p2;
          if( pc<0 || pc>p->nOp ){
            sqliteSetString(pzErrMsg, "jump destination out of range", 0);
            rc = 1;
          }
          pc--;
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
        Sorter *pSorter;
        if( i<0 || i>=p->nSort ) goto bad_instruction;
        if( p->tos<1 ) goto not_enough_stack;
        Stringify(p, p->tos);
        Stringify(p, p->tos-1);
        pSorter = sqliteMalloc( sizeof(Sorter) );
        if( pSorter==0 ) goto no_mem;
        pSorter->pNext = p->apSort[i];
        p->apSort[i] = pSorter;
        pSorter->nKey = p->iStack[p->tos];
        pSorter->zKey = p->zStack[p->tos];
        pSorter->nData = p->iStack[p->tos-1];
        pSorter->pData = p->zStack[p->tos-1];
        p->zStack[p->tos] = p->zStack[p->tos-1] = 0;
        PopStack(p, 2);
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
          if( Stringify(p, i) ) goto no_mem;
          nByte += p->iStack[i];
        }
        nByte += sizeof(char*)*(nField+1);
        azArg = sqliteMalloc( nByte );
        if( azArg==0 ) goto no_mem;
        z = (char*)&azArg[nField+1];
        for(j=0, i=p->tos-nField+1; i<=p->tos; i++, j++){
          azArg[j] = z;
          strcpy(z, p->zStack[i]);
          z += p->iStack[i];
        }
        PopStack(p, nField);
        NeedStack(p, p->tos+1);
        p->tos++;
        p->iStack[p->tos] = nByte;
        p->zStack[p->tos] = (char*)azArg;
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
          nByte += p->iStack[i]+2;
        }
        zNewKey = sqliteMalloc( nByte );
        if( zNewKey==0 ) goto no_mem;
        j = 0;
        k = nField-1;
        for(i=p->tos-nField+1; i<=p->tos; i++){
          zNewKey[j++] = pOp->p3[k--];
          memcpy(&zNewKey[j], p->zStack[i], p->iStack[i]-1);
          j += p->iStack[i]-1;
          zNewKey[j++] = 0;
        }
        zNewKey[j] = 0;
        PopStack(p, nField);
        NeedStack(p, p->tos+1);
        p->tos++;
        p->iStack[p->tos] = nByte;
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
          p->iStack[p->tos] = pSorter->nData;
          sqliteFree(pSorter->zKey);
          sqliteFree(pSorter);
        }else{
          pc = pOp->p2;
          if( pc<0 || pc>p->nOp ){
            sqliteSetString(pzErrMsg, "jump destination out of range", 0);
            rc = 1;
          }
          pc--;
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
          p->iStack[p->tos] = pSorter->nKey;
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
        rc = xCallback(pArg, pOp->p1, (char**)p->zStack[i], p->azColName);
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

      /* An other opcode is illegal...
      */
      default: {
        sprintf(zBuf,"%d",pOp->opcode);
        sqliteSetString(pzErrMsg, "unknown opcode ", zBuf, 0);
        rc = 1;
        break;
      }
    }
    if( p->trace && p->tos>=0 ){
      int i;
      fprintf(p->trace, "Stack:");
      for(i=p->tos; i>=0 && i>p->tos-5; i--){
        if( p->zStack[i] ){
          fprintf(p->trace, " [%.11s]", p->zStack[i]);
        }else{
          fprintf(p->trace, " [%d]", p->iStack[i]);
        }
      }
      fprintf(p->trace,"\n");
    }
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
  rc = 1;
  goto cleanup;

  /* Jump here if an illegal or illformed instruction is executed.
  */
bad_instruction:
  sprintf(zBuf,"%d",pc);
  sqliteSetString(pzErrMsg, "illegal operation at ", zBuf, 0);
  rc = 1;
  goto cleanup;

}
