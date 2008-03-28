/*
** 2007 August 15
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** This file contains code used to implement test interfaces to the
** memory allocation subsystem.
**
** $Id: test_malloc.c,v 1.22 2008/03/28 15:44:10 danielk1977 Exp $
*/
#include "sqliteInt.h"
#include "tcl.h"
#include <stdlib.h>
#include <string.h>
#include <assert.h>

/*
** Transform pointers to text and back again
*/
static void pointerToText(void *p, char *z){
  static const char zHex[] = "0123456789abcdef";
  int i, k;
  unsigned int u;
  sqlite3_uint64 n;
  if( sizeof(n)==sizeof(p) ){
    memcpy(&n, &p, sizeof(p));
  }else if( sizeof(u)==sizeof(p) ){
    memcpy(&u, &p, sizeof(u));
    n = u;
  }else{
    assert( 0 );
  }
  for(i=0, k=sizeof(p)*2-1; i<sizeof(p)*2; i++, k--){
    z[k] = zHex[n&0xf];
    n >>= 4;
  }
  z[sizeof(p)*2] = 0;
}
static int hexToInt(int h){
  if( h>='0' && h<='9' ){
    return h - '0';
  }else if( h>='a' && h<='f' ){
    return h - 'a' + 10;
  }else{
    return -1;
  }
}
static int textToPointer(const char *z, void **pp){
  sqlite3_uint64 n = 0;
  int i;
  unsigned int u;
  for(i=0; i<sizeof(void*)*2 && z[0]; i++){
    int v;
    v = hexToInt(*z++);
    if( v<0 ) return TCL_ERROR;
    n = n*16 + v;
  }
  if( *z!=0 ) return TCL_ERROR;
  if( sizeof(n)==sizeof(*pp) ){
    memcpy(pp, &n, sizeof(n));
  }else if( sizeof(u)==sizeof(*pp) ){
    u = (unsigned int)n;
    memcpy(pp, &u, sizeof(u));
  }else{
    assert( 0 );
  }
  return TCL_OK;
}

/*
** Usage:    sqlite3_malloc  NBYTES
**
** Raw test interface for sqlite3_malloc().
*/
static int test_malloc(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  int nByte;
  void *p;
  char zOut[100];
  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "NBYTES");
    return TCL_ERROR;
  }
  if( Tcl_GetIntFromObj(interp, objv[1], &nByte) ) return TCL_ERROR;
  p = sqlite3_malloc((unsigned)nByte);
  pointerToText(p, zOut);
  Tcl_AppendResult(interp, zOut, NULL);
  return TCL_OK;
}

/*
** Usage:    sqlite3_realloc  PRIOR  NBYTES
**
** Raw test interface for sqlite3_realloc().
*/
static int test_realloc(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  int nByte;
  void *pPrior, *p;
  char zOut[100];
  if( objc!=3 ){
    Tcl_WrongNumArgs(interp, 1, objv, "PRIOR NBYTES");
    return TCL_ERROR;
  }
  if( Tcl_GetIntFromObj(interp, objv[2], &nByte) ) return TCL_ERROR;
  if( textToPointer(Tcl_GetString(objv[1]), &pPrior) ){
    Tcl_AppendResult(interp, "bad pointer: ", Tcl_GetString(objv[1]), (char*)0);
    return TCL_ERROR;
  }
  p = sqlite3_realloc(pPrior, (unsigned)nByte);
  pointerToText(p, zOut);
  Tcl_AppendResult(interp, zOut, NULL);
  return TCL_OK;
}


/*
** Usage:    sqlite3_free  PRIOR
**
** Raw test interface for sqlite3_free().
*/
static int test_free(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  void *pPrior;
  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "PRIOR");
    return TCL_ERROR;
  }
  if( textToPointer(Tcl_GetString(objv[1]), &pPrior) ){
    Tcl_AppendResult(interp, "bad pointer: ", Tcl_GetString(objv[1]), (char*)0);
    return TCL_ERROR;
  }
  sqlite3_free(pPrior);
  return TCL_OK;
}

/*
** These routines are in test_hexio.c
*/
int sqlite3TestHexToBin(const char *, int, char *);
int sqlite3TestBinToHex(char*,int);

/*
** Usage:    memset  ADDRESS  SIZE  HEX
**
** Set a chunk of memory (obtained from malloc, probably) to a
** specified hex pattern.
*/
static int test_memset(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  void *p;
  int size, n, i;
  char *zHex;
  char *zOut;
  char zBin[100];

  if( objc!=4 ){
    Tcl_WrongNumArgs(interp, 1, objv, "ADDRESS SIZE HEX");
    return TCL_ERROR;
  }
  if( textToPointer(Tcl_GetString(objv[1]), &p) ){
    Tcl_AppendResult(interp, "bad pointer: ", Tcl_GetString(objv[1]), (char*)0);
    return TCL_ERROR;
  }
  if( Tcl_GetIntFromObj(interp, objv[2], &size) ){
    return TCL_ERROR;
  }
  if( size<=0 ){
    Tcl_AppendResult(interp, "size must be positive", (char*)0);
    return TCL_ERROR;
  }
  zHex = Tcl_GetStringFromObj(objv[3], &n);
  if( n>sizeof(zBin)*2 ) n = sizeof(zBin)*2;
  n = sqlite3TestHexToBin(zHex, n, zBin);
  if( n==0 ){
    Tcl_AppendResult(interp, "no data", (char*)0);
    return TCL_ERROR;
  }
  zOut = p;
  for(i=0; i<size; i++){
    zOut[i] = zBin[i%n];
  }
  return TCL_OK;
}

/*
** Usage:    memget  ADDRESS  SIZE
**
** Return memory as hexadecimal text.
*/
static int test_memget(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  void *p;
  int size, n;
  char *zBin;
  char zHex[100];

  if( objc!=3 ){
    Tcl_WrongNumArgs(interp, 1, objv, "ADDRESS SIZE");
    return TCL_ERROR;
  }
  if( textToPointer(Tcl_GetString(objv[1]), &p) ){
    Tcl_AppendResult(interp, "bad pointer: ", Tcl_GetString(objv[1]), (char*)0);
    return TCL_ERROR;
  }
  if( Tcl_GetIntFromObj(interp, objv[2], &size) ){
    return TCL_ERROR;
  }
  if( size<=0 ){
    Tcl_AppendResult(interp, "size must be positive", (char*)0);
    return TCL_ERROR;
  }
  zBin = p;
  while( size>0 ){
    if( size>(sizeof(zHex)-1)/2 ){
      n = (sizeof(zHex)-1)/2;
    }else{
      n = size;
    }
    memcpy(zHex, zBin, n);
    zBin += n;
    size -= n;
    sqlite3TestBinToHex(zHex, n);
    Tcl_AppendResult(interp, zHex, (char*)0);
  }
  return TCL_OK;
}

/*
** Usage:    sqlite3_memory_used
**
** Raw test interface for sqlite3_memory_used().
*/
static int test_memory_used(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  Tcl_SetObjResult(interp, Tcl_NewWideIntObj(sqlite3_memory_used()));
  return TCL_OK;
}

/*
** Usage:    sqlite3_memory_highwater ?RESETFLAG?
**
** Raw test interface for sqlite3_memory_highwater().
*/
static int test_memory_highwater(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  int resetFlag = 0;
  if( objc!=1 && objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "?RESET?");
    return TCL_ERROR;
  }
  if( objc==2 ){
    if( Tcl_GetBooleanFromObj(interp, objv[1], &resetFlag) ) return TCL_ERROR;
  } 
  Tcl_SetObjResult(interp, 
     Tcl_NewWideIntObj(sqlite3_memory_highwater(resetFlag)));
  return TCL_OK;
}

/*
** Usage:    sqlite3_memdebug_backtrace DEPTH
**
** Set the depth of backtracing.  If SQLITE_MEMDEBUG is not defined
** then this routine is a no-op.
*/
static int test_memdebug_backtrace(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  int depth;
  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "DEPT");
    return TCL_ERROR;
  }
  if( Tcl_GetIntFromObj(interp, objv[1], &depth) ) return TCL_ERROR;
#ifdef SQLITE_MEMDEBUG
  {
    extern void sqlite3MemdebugBacktrace(int);
    sqlite3MemdebugBacktrace(depth);
  }
#endif
  return TCL_OK;
}

/*
** Usage:    sqlite3_memdebug_dump  FILENAME
**
** Write a summary of unfreed memory to FILENAME.
*/
static int test_memdebug_dump(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "FILENAME");
    return TCL_ERROR;
  }
#if defined(SQLITE_MEMDEBUG) || defined(SQLITE_MEMORY_SIZE) \
     || defined(SQLITE_POW2_MEMORY_SIZE)
  {
    extern void sqlite3MemdebugDump(const char*);
    sqlite3MemdebugDump(Tcl_GetString(objv[1]));
  }
#endif
  return TCL_OK;
}

/*
** Usage:    sqlite3_memdebug_malloc_count
**
** Return the total number of times malloc() has been called.
*/
static int test_memdebug_malloc_count(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  int nMalloc = -1;
  if( objc!=1 ){
    Tcl_WrongNumArgs(interp, 1, objv, "");
    return TCL_ERROR;
  }
#if defined(SQLITE_MEMDEBUG)
  {
    extern int sqlite3MemdebugMallocCount();
    nMalloc = sqlite3MemdebugMallocCount();
  }
#endif
  Tcl_SetObjResult(interp, Tcl_NewIntObj(nMalloc));
  return TCL_OK;
}


/*
** Usage:    sqlite3_memdebug_fail  COUNTER  ?OPTIONS?
**
** where options are:
**
**     -repeat    <count>
**     -benigncnt <varname>
**
** Arrange for a simulated malloc() failure after COUNTER successes.
** If a repeat count is specified, the fault is repeated that many
** times.
**
** Each call to this routine overrides the prior counter value.
** This routine returns the number of simulated failures that have
** happened since the previous call to this routine.
**
** To disable simulated failures, use a COUNTER of -1.
*/
static int test_memdebug_fail(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  int ii;
  int iFail;
  int nRepeat = 1;
  Tcl_Obj *pBenignCnt = 0;
  int nBenign;
  int nFail = 0;

  if( objc<2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "COUNTER ?OPTIONS?");
    return TCL_ERROR;
  }
  if( Tcl_GetIntFromObj(interp, objv[1], &iFail) ) return TCL_ERROR;

  for(ii=2; ii<objc; ii+=2){
    int nOption;
    char *zOption = Tcl_GetStringFromObj(objv[ii], &nOption);
    char *zErr = 0;

    if( nOption>1 && strncmp(zOption, "-repeat", nOption)==0 ){
      if( ii==(objc-1) ){
        zErr = "option requires an argument: ";
      }else{
        if( Tcl_GetIntFromObj(interp, objv[ii+1], &nRepeat) ){
          return TCL_ERROR;
        }
      }
    }else if( nOption>1 && strncmp(zOption, "-benigncnt", nOption)==0 ){
      if( ii==(objc-1) ){
        zErr = "option requires an argument: ";
      }else{
        pBenignCnt = objv[ii+1];
      }
    }else{
      zErr = "unknown option: ";
    }

    if( zErr ){
      Tcl_AppendResult(interp, zErr, zOption, 0);
      return TCL_ERROR;
    }
  }
  
  sqlite3_test_control(-12345); /* Just to stress the test_control interface */
  nBenign = sqlite3_test_control(SQLITE_TESTCTRL_FAULT_BENIGN_FAILURES,
                                 SQLITE_FAULTINJECTOR_MALLOC);
  nFail = sqlite3_test_control(SQLITE_TESTCTRL_FAULT_FAILURES,
                               SQLITE_FAULTINJECTOR_MALLOC);
  sqlite3_test_control(SQLITE_TESTCTRL_FAULT_CONFIG,
                       SQLITE_FAULTINJECTOR_MALLOC, iFail, nRepeat);
  if( pBenignCnt ){
    Tcl_ObjSetVar2(interp, pBenignCnt, 0, Tcl_NewIntObj(nBenign), 0);
  }
  Tcl_SetObjResult(interp, Tcl_NewIntObj(nFail));
  return TCL_OK;
}

/*
** Usage:    sqlite3_memdebug_pending
**
** Return the number of malloc() calls that will succeed before a 
** simulated failure occurs. A negative return value indicates that
** no malloc() failure is scheduled.
*/
static int test_memdebug_pending(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  int nPending;
  if( objc!=1 ){
    Tcl_WrongNumArgs(interp, 1, objv, "");
    return TCL_ERROR;
  }
  nPending = sqlite3_test_control(SQLITE_TESTCTRL_FAULT_PENDING,
                                  SQLITE_FAULTINJECTOR_MALLOC);
  Tcl_SetObjResult(interp, Tcl_NewIntObj(nPending));
  return TCL_OK;
}


/*
** Usage:    sqlite3_memdebug_settitle TITLE
**
** Set a title string stored with each allocation.  The TITLE is
** typically the name of the test that was running when the
** allocation occurred.  The TITLE is stored with the allocation
** and can be used to figure out which tests are leaking memory.
**
** Each title overwrite the previous.
*/
static int test_memdebug_settitle(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  const char *zTitle;
  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "TITLE");
    return TCL_ERROR;
  }
  zTitle = Tcl_GetString(objv[1]);
#ifdef SQLITE_MEMDEBUG
  {
    extern int sqlite3MemdebugSettitle(const char*);
    sqlite3MemdebugSettitle(zTitle);
  }
#endif
  return TCL_OK;
}

#define MALLOC_LOG_FRAMES 10 
static Tcl_HashTable aMallocLog;
static int mallocLogEnabled = 0;

typedef struct MallocLog MallocLog;
struct MallocLog {
  int nCall;
  int nByte;
};

static void test_memdebug_callback(int nByte, int nFrame, void **aFrame){
  if( mallocLogEnabled ){
    MallocLog *pLog;
    Tcl_HashEntry *pEntry;
    int isNew;

    int aKey[MALLOC_LOG_FRAMES];
    int nKey = sizeof(int)*MALLOC_LOG_FRAMES;

    memset(aKey, 0, nKey);
    if( (sizeof(void*)*nFrame)<nKey ){
      nKey = nFrame*sizeof(void*);
    }
    memcpy(aKey, aFrame, nKey);

    pEntry = Tcl_CreateHashEntry(&aMallocLog, (const char *)aKey, &isNew);
    if( isNew ){
      pLog = (MallocLog *)Tcl_Alloc(sizeof(MallocLog));
      memset(pLog, 0, sizeof(MallocLog));
      Tcl_SetHashValue(pEntry, (ClientData)pLog);
    }else{
      pLog = (MallocLog *)Tcl_GetHashValue(pEntry);
    }

    pLog->nCall++;
    pLog->nByte += nByte;
  }
}

static void test_memdebug_log_clear(){
  Tcl_HashSearch search;
  Tcl_HashEntry *pEntry;
  for(
    pEntry=Tcl_FirstHashEntry(&aMallocLog, &search);
    pEntry;
    pEntry=Tcl_NextHashEntry(&search)
  ){
    MallocLog *pLog = (MallocLog *)Tcl_GetHashValue(pEntry);
    Tcl_Free((char *)pLog);
  }
  Tcl_DeleteHashTable(&aMallocLog);
  Tcl_InitHashTable(&aMallocLog, MALLOC_LOG_FRAMES);
}

static int test_memdebug_log(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  static int isInit = 0;
  int iSub;

  static const char *MB_strs[] = { "start", "stop", "dump", "clear", "sync" };
  enum MB_enum { 
      MB_LOG_START, MB_LOG_STOP, MB_LOG_DUMP, MB_LOG_CLEAR, MB_LOG_SYNC 
  };

  if( !isInit ){
#ifdef SQLITE_MEMDEBUG
    extern void sqlite3MemdebugBacktraceCallback(
        void (*xBacktrace)(int, int, void **));
    sqlite3MemdebugBacktraceCallback(test_memdebug_callback);
#endif
    Tcl_InitHashTable(&aMallocLog, MALLOC_LOG_FRAMES);
    isInit = 1;
  }

  if( objc<2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "SUB-COMMAND ...");
  }
  if( Tcl_GetIndexFromObj(interp, objv[1], MB_strs, "sub-command", 0, &iSub) ){
    return TCL_ERROR;
  }

  switch( (enum MB_enum)iSub ){
    case MB_LOG_START:
      mallocLogEnabled = 1;
      break;
    case MB_LOG_STOP:
      mallocLogEnabled = 0;
      break;
    case MB_LOG_DUMP: {
      Tcl_HashSearch search;
      Tcl_HashEntry *pEntry;
      Tcl_Obj *pRet = Tcl_NewObj();

      assert(sizeof(int)==sizeof(void*));

      for(
        pEntry=Tcl_FirstHashEntry(&aMallocLog, &search);
        pEntry;
        pEntry=Tcl_NextHashEntry(&search)
      ){
        Tcl_Obj *apElem[MALLOC_LOG_FRAMES+2];
        MallocLog *pLog = (MallocLog *)Tcl_GetHashValue(pEntry);
        int *aKey = (int *)Tcl_GetHashKey(&aMallocLog, pEntry);
        int ii;
  
        apElem[0] = Tcl_NewIntObj(pLog->nCall);
        apElem[1] = Tcl_NewIntObj(pLog->nByte);
        for(ii=0; ii<MALLOC_LOG_FRAMES; ii++){
          apElem[ii+2] = Tcl_NewIntObj(aKey[ii]);
        }

        Tcl_ListObjAppendElement(interp, pRet,
            Tcl_NewListObj(MALLOC_LOG_FRAMES+2, apElem)
        );
      }

      Tcl_SetObjResult(interp, pRet);
      break;
    }
    case MB_LOG_CLEAR: {
      test_memdebug_log_clear();
      break;
    }

    case MB_LOG_SYNC: {
#ifdef SQLITE_MEMDEBUG
      extern void sqlite3MemdebugSync();
      test_memdebug_log_clear();
      mallocLogEnabled = 1;
      sqlite3MemdebugSync();
#endif
      break;
    }
  }

  return TCL_OK;
}

/*
** Register commands with the TCL interpreter.
*/
int Sqlitetest_malloc_Init(Tcl_Interp *interp){
  static struct {
     char *zName;
     Tcl_ObjCmdProc *xProc;
  } aObjCmd[] = {
     { "sqlite3_malloc",             test_malloc                   },
     { "sqlite3_realloc",            test_realloc                  },
     { "sqlite3_free",               test_free                     },
     { "memset",                     test_memset                   },
     { "memget",                     test_memget                   },
     { "sqlite3_memory_used",        test_memory_used              },
     { "sqlite3_memory_highwater",   test_memory_highwater         },
     { "sqlite3_memdebug_backtrace", test_memdebug_backtrace       },
     { "sqlite3_memdebug_dump",      test_memdebug_dump            },
     { "sqlite3_memdebug_fail",      test_memdebug_fail            },
     { "sqlite3_memdebug_pending",   test_memdebug_pending         },
     { "sqlite3_memdebug_settitle",  test_memdebug_settitle        },
     { "sqlite3_memdebug_malloc_count", test_memdebug_malloc_count },
     { "sqlite3_memdebug_log",       test_memdebug_log },
  };
  int i;
  for(i=0; i<sizeof(aObjCmd)/sizeof(aObjCmd[0]); i++){
    Tcl_CreateObjCommand(interp, aObjCmd[i].zName, aObjCmd[i].xProc, 0, 0);
  }
  return TCL_OK;
}
