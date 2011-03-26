/*
** 2011 March 24
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
** Code for demonstartion virtual table that generates variations
** on an input word at increasing edit distances from the original.
*/
#include "sqlite3.h"
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#ifndef SQLITE_OMIT_VIRTUALTABLE

/*
** Forward declaration of objects used by this implementation
*/
typedef struct fuzzer_vtab fuzzer_vtab;
typedef struct fuzzer_cursor fuzzer_cursor;
typedef struct fuzzer_rule fuzzer_rule;
typedef struct fuzzer_seen fuzzer_seen;
typedef struct fuzzer_stem fuzzer_stem;


/*
** Each transformation rule is stored as an instance of this object.
** All rules are kept on a linked list sorted by rCost.
*/
struct fuzzer_rule {
  fuzzer_rule *pNext;   /* Next rule in order of increasing rCost */
  float rCost;          /* Cost of this transformation */
  char *zFrom;          /* Transform from */
  char zTo[4];          /* Transform to (extra space appended) */
};

/*
** When generating fuzzed words, we have to remember all previously
** generated terms in order to suppress duplicates.  Each previously
** generated term is an instance of the following structure.
*/
struct fuzzer_seen {
  fuzzer_seen *pNext;    /* Next with the same hash */
  char zWord[4];         /* The generated term. */
};

/*
** A stem object is used to generate variants.  
*/
struct fuzzer_stem {
  char *zBasis;           /* Word being fuzzed */
  fuzzer_rule *pRule;     /* Next rule to apply */
  int n;                  /* Apply rule at this character offset */
  float rBaseCost;        /* Base cost of getting to zBasis */
  float rCost;            /* rBaseCost + cost of applying pRule at n */
  fuzzer_stem *pNext;     /* Next stem in rCost order */
};

/* 
** A fuzzer virtual-table object 
*/
struct fuzzer_vtab {
  sqlite3_vtab base;         /* Base class - must be first */
  char *zClassName;          /* Name of this class.  Default: "fuzzer" */
  fuzzer_rule *pRule;        /* All active rules in this fuzzer */
  fuzzer_rule *pNewRule;     /* New rules to add when last cursor expires */
  int nCursor;               /* Number of active cursors */
};

/* A fuzzer cursor object */
struct fuzzer_cursor {
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  float rMax;                /* Maximum cost of any term */
  fuzzer_stem *pStem;        /* Sorted list of stems for generating new terms */
  int nSeen;                 /* Number of terms already generated */
  int nHash;                 /* Number of slots in apHash */
  fuzzer_seen **apHash;      /* Hash table of previously generated terms */
};

/* Methods for the fuzzer module */
static int fuzzerConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  fuzzer_vtab *pNew;
  char *zSql;
  int n;
  if( strcmp(argv[1],"temp")!=0 ){
    *pzErr = sqlite3_mprintf("%s virtual tables must be TEMP", argv[0]);
    return SQLITE_ERROR;
  }
  n = strlen(argv[0]) + 1;
  pNew = sqlite3_malloc( sizeof(*pNew) + n );
  if( pNew==0 ) return SQLITE_NOMEM;
  pNew->zClassName = (char*)&pNew[1];
  memcpy(pNew->zClassName, argv[0], n);
  zSql = sqlite3_mprintf(
     "CREATE TABLE x(word, distance, cFrom, cTo, cost, \"%w\" HIDDEN)",
     argv[2]
  );
  sqlite3_declare_vtab(db, zSql);
  sqlite3_free(zSql);
  memset(pNew, 0, sizeof(*pNew));
  *ppVtab = &pNew->base;
  return SQLITE_OK;
}
/* Note that for this virtual table, the xCreate and xConnect
** methods are identical. */

static int fuzzerDisconnect(sqlite3_vtab *pVtab){
  fuzzer_vtab *p = (fuzzer_vtab*)pVtab;
  assert( p->nCursor==0 );
  do{
    while( p->pRule ){
      fuzzer_rule *pRule = p->pRule;
      p->pRule = pRule->pNext;
      sqlite3_free(pRule);
    }
    p->pRule = p->pNewRule;
    p->pNewRule = 0;
  }while( p->pRule );
  sqlite3_free(p);
  return SQLITE_OK;
}
/* The xDisconnect and xDestroy methods are also the same */

/*
** The two input rule lists are both sorted in order of increasing
** cost.  Merge them together into a single list, sorted by cost, and
** return a pointer to the head of that list.
*/
static fuzzer_rule *fuzzerMergeRules(fuzzer_rule *pA, fuzzer_rule *pB){
  fuzzer_rule head;
  fuzzer_rule *pTail;

  pTail =  &head;
  while( pA && pB ){
    if( pA->rCost<=pB->rCost ){
      pTail->pNext = pA;
      pTail = pA;
      pA = pA->pNext;
    }else{
      pTail->pNext = pB;
      pTail = pB;
      pB = pB->pNext;
    }
  }
  if( pA==0 ){
    pTail->pNext = pB;
  }else{
    pTail->pNext = pA;
  }
  return head.pNext;
}


/*
** Open a new fuzzer cursor.
*/
static int fuzzerOpen(sqlite3_vtab *pVTab, sqlite3_vtab_cursor **ppCursor){
  fuzzer_vtab *p = (fuzzer_vtab*)pVTab;
  fuzzer_cursor *pCur;
  pCur = sqlite3_malloc( sizeof(*pCur) );
  if( pCur==0 ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));
  *ppCursor = &pCur->base;
  if( p->nCursor==0 && p->pNewRule ){
    unsigned int i;
    fuzzer_rule *pX;
    fuzzer_rule *a[15];
    for(i=0; i<sizeof(a)/sizeof(a[0]); i++) a[i] = 0;
    while( (pX = p->pNewRule)!=0 ){
      p->pNewRule = pX->pNext;
      pX->pNext = 0;
      for(i=0; a[i] && i<sizeof(a)/sizeof(a[0])-1; i++){
        pX = fuzzerMergeRules(a[i], pX);
        a[i] = 0;
      }
      a[i] = fuzzerMergeRules(a[i], pX);
    }
    for(pX=a[0], i=1; i<sizeof(a)/sizeof(a[0]); i++){
      pX = fuzzerMergeRules(a[i], pX);
    }
    p->pRule = fuzzerMergeRules(p->pRule, pX);
  }
   
  return SQLITE_OK;
}

/*
** Close a fuzzer cursor.
*/
static int fuzzerClose(sqlite3_vtab_cursor *cur){
  fuzzer_cursor *pCur = (fuzzer_cursor *)cur;
  int i;
  for(i=0; i<pCur->nHash; i++){
    fuzzer_seen *pSeen = pCur->apHash[i];
    while( pSeen ){
      fuzzer_seen *pNext = pSeen->pNext;
      sqlite3_free(pSeen);
      pSeen = pNext;
    }
  }
  sqlite3_free(pCur->apHash);
  while( pCur->pStem ){
    fuzzer_stem *pStem = pCur->pStem;
    pCur->pStem = pStem->pNext;
    sqlite3_free(pStem);
  }
  sqlite3_free(pCur);
  return SQLITE_OK;
}

static int fuzzerNext(sqlite3_vtab_cursor *cur){
  return 0;
}

static int fuzzerFilter(
  sqlite3_vtab_cursor *pVtabCursor, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  fuzzer_cursor *pCur = (fuzzer_cursor *)pVtabCursor;
  return fuzzerNext(pVtabCursor);
}

static int fuzzerColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int i){
  fuzzer_cursor *pCur = (fuzzer_cursor*)cur;
  return SQLITE_OK;
}

static int fuzzerRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  *pRowid = 0;
  return SQLITE_OK;
}

static int fuzzerEof(sqlite3_vtab_cursor *cur){
  fuzzer_cursor *pCur = (fuzzer_cursor*)cur;
  return 1;
}

static int fuzzerBestIndex(sqlite3_vtab *tab, sqlite3_index_info *pIdxInfo){

  return SQLITE_OK;
}

/*
** Disallow all attempts to DELETE or UPDATE.  Only INSERTs are allowed.
**
** On an insert, the cFrom, cTo, and cost columns are used to construct
** a new rule.   All other columns are ignored.  The rule is ignored
** if cFrom and cTo are identical.  A NULL value for cFrom or cTo is
** interpreted as an empty string.  The cost must be positive.
*/
static int fuzzerUpdate(
  sqlite3_vtab *pVTab,
  int argc,
  sqlite3_value **argv,
  sqlite_int64 *pRowid
){
  fuzzer_vtab *p = (fuzzer_vtab*)pVTab;
  fuzzer_rule *pRule;
  const char *zFrom;
  int nFrom;
  const char *zTo;
  int nTo;
  float rCost;
  if( argc!=8 ){
    sqlite3_free(pVTab->zErrMsg);
    pVTab->zErrMsg = sqlite3_mprintf("cannot delete from a %s virtual table",
                                     p->zClassName);
    return SQLITE_CONSTRAINT;
  }
  if( sqlite3_value_type(argv[0])!=SQLITE_NULL ){
    sqlite3_free(pVTab->zErrMsg);
    pVTab->zErrMsg = sqlite3_mprintf("cannot update a %s virtual table",
                                     p->zClassName);
    return SQLITE_CONSTRAINT;
  }
  zFrom = (char*)sqlite3_value_text(argv[4]);
  if( zFrom==0 ) zFrom = "";
  zTo = (char*)sqlite3_value_text(argv[5]);
  if( zTo==0 ) zTo = "";
  if( strcmp(zFrom,zTo)==0 ){
    /* Silently ignore null transformations */
    return SQLITE_OK;
  }
  rCost = (float)sqlite3_value_double(argv[6]);
  if( rCost<=0 ){
    sqlite3_free(pVTab->zErrMsg);
    pVTab->zErrMsg = sqlite3_mprintf("cost must be positive");
    return SQLITE_CONSTRAINT;    
  }
  nFrom = strlen(zFrom)+1;
  nTo = strlen(zTo)+1;
  if( nTo<4 ) nTo = 4;
  pRule = sqlite3_malloc( sizeof(*pRule) + nFrom + nTo - 4 );
  if( pRule==0 ){
    return SQLITE_NOMEM;
  }
  pRule->zFrom = &pRule->zTo[nTo];
  memcpy(pRule->zFrom, zFrom, nFrom);
  memcpy(pRule->zTo, zTo, nTo);
  pRule->rCost = rCost;
  pRule->pNext = p->pNewRule;
  p->pNewRule = pRule;
  return SQLITE_OK;
}

/*
** A virtual table module that provides read-only access to a
** Tcl global variable namespace.
*/
static sqlite3_module fuzzerModule = {
  0,                           /* iVersion */
  fuzzerConnect,
  fuzzerConnect,
  fuzzerBestIndex,
  fuzzerDisconnect, 
  fuzzerDisconnect,
  fuzzerOpen,                  /* xOpen - open a cursor */
  fuzzerClose,                 /* xClose - close a cursor */
  fuzzerFilter,                /* xFilter - configure scan constraints */
  fuzzerNext,                  /* xNext - advance a cursor */
  fuzzerEof,                   /* xEof - check for end of scan */
  fuzzerColumn,                /* xColumn - read data */
  fuzzerRowid,                 /* xRowid - read data */
  fuzzerUpdate,                /* xUpdate - INSERT */
  0,                           /* xBegin */
  0,                           /* xSync */
  0,                           /* xCommit */
  0,                           /* xRollback */
  0,                           /* xFindMethod */
  0,                           /* xRename */
};

#endif /* SQLITE_OMIT_VIRTUALTABLE */


/*
** Register the fuzzer virtual table
*/
int fuzzer_register(sqlite3 *db){
  int rc = SQLITE_OK;
#ifndef SQLITE_OMIT_VIRTUALTABLE
  rc = sqlite3_create_module(db, "fuzzer", &fuzzerModule, 0);
#endif
  return rc;
}

#ifdef SQLITE_TEST
#include <tcl.h>
/*
** Decode a pointer to an sqlite3 object.
*/
extern int getDbPointer(Tcl_Interp *interp, const char *zA, sqlite3 **ppDb);

/*
** Register the echo virtual table module.
*/
static int register_fuzzer_module(
  ClientData clientData, /* Pointer to sqlite3_enable_XXX function */
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int objc,              /* Number of arguments */
  Tcl_Obj *CONST objv[]  /* Command arguments */
){
  sqlite3 *db;
  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "DB");
    return TCL_ERROR;
  }
  if( getDbPointer(interp, Tcl_GetString(objv[1]), &db) ) return TCL_ERROR;
  fuzzer_register(db);
  return TCL_OK;
}


/*
** Register commands with the TCL interpreter.
*/
int Sqlitetestfuzzer_Init(Tcl_Interp *interp){
  static struct {
     char *zName;
     Tcl_ObjCmdProc *xProc;
     void *clientData;
  } aObjCmd[] = {
     { "register_fuzzer_module",   register_fuzzer_module, 0 },
  };
  int i;
  for(i=0; i<sizeof(aObjCmd)/sizeof(aObjCmd[0]); i++){
    Tcl_CreateObjCommand(interp, aObjCmd[i].zName, 
        aObjCmd[i].xProc, aObjCmd[i].clientData, 0);
  }
  return TCL_OK;
}

#endif /* SQLITE_TEST */
