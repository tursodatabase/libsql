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
#include <stdio.h>

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
** Type of the "cost" of an edit operation.  Might be changed to
** "float" or "double" or "sqlite3_int64" in the future.
*/
typedef int fuzzer_cost;


/*
** Each transformation rule is stored as an instance of this object.
** All rules are kept on a linked list sorted by rCost.
*/
struct fuzzer_rule {
  fuzzer_rule *pNext;        /* Next rule in order of increasing rCost */
  fuzzer_cost rCost;         /* Cost of this transformation */
  int nFrom, nTo;            /* Length of the zFrom and zTo strings */
  char *zFrom;               /* Transform from */
  char zTo[4];               /* Transform to (extra space appended) */
};

/*
** A stem object is used to generate variants.  It is also used to record
** previously generated outputs.
**
** Every stem is added to a hash table as it is output.  Generation of
** duplicate stems is suppressed.
**
** Active stems (those that might generate new outputs) are kepts on a linked
** list sorted by increasing cost.  The cost is the sum of rBaseCost and
** pRule->rCost.
*/
struct fuzzer_stem {
  char *zBasis;              /* Word being fuzzed */
  int nBasis;                /* Length of the zBasis string */
  const fuzzer_rule *pRule;  /* Current rule to apply */
  int n;                     /* Apply pRule at this character offset */
  fuzzer_cost rBaseCost;     /* Base cost of getting to zBasis */
  fuzzer_stem *pNext;        /* Next stem in rCost order */
  fuzzer_stem *pHash;        /* Next stem with same hash on zBasis */
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

#define FUZZER_HASH  4001    /* Hash table size */

/* A fuzzer cursor object */
struct fuzzer_cursor {
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  sqlite3_int64 iRowid;      /* The rowid of the current word */
  fuzzer_vtab *pVtab;        /* The virtual table this cursor belongs to */
  fuzzer_cost rLimit;        /* Maximum cost of any term */
  fuzzer_stem *pStem;        /* Sorted list of stems for generating new terms */
  fuzzer_stem *pDone;        /* Stems already processed to completion */
  char *zBuf;                /* Temporary use buffer */
  int nBuf;                  /* Bytes allocated for zBuf */
  fuzzer_rule nullRule;      /* Null rule used first */
  fuzzer_stem *apHash[FUZZER_HASH]; /* Hash of previously generated terms */
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
  sqlite3_declare_vtab(db, "CREATE TABLE x(word,distance,cFrom,cTo,cost)");
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
  pCur->pVtab = p;
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
  p->nCursor++;
  return SQLITE_OK;
}

/*
** Free up all the memory allocated by a cursor.  Set it rLimit to 0
** to indicate that it is at EOF.
*/
static void fuzzerClearCursor(fuzzer_cursor *pCur, int clearHash){
  if( pCur->pStem==0 && pCur->pDone==0 ) clearHash = 0;
  do{
    while( pCur->pStem ){
      fuzzer_stem *pStem = pCur->pStem;
      pCur->pStem = pStem->pNext;
      sqlite3_free(pStem);
    }
    pCur->pStem = pCur->pDone;
    pCur->pDone = 0;
  }while( pCur->pStem );
  pCur->rLimit = (fuzzer_cost)0;
  if( clearHash ) memset(pCur->apHash, 0, sizeof(pCur->apHash));
}

/*
** Close a fuzzer cursor.
*/
static int fuzzerClose(sqlite3_vtab_cursor *cur){
  fuzzer_cursor *pCur = (fuzzer_cursor *)cur;
  fuzzerClearCursor(pCur, 0);
  sqlite3_free(pCur->zBuf);
  pCur->pVtab->nCursor--;
  sqlite3_free(pCur);
  return SQLITE_OK;
}

/*
** Compute the current output term for a fuzzer_stem.
*/
static int fuzzerRender(
  fuzzer_stem *pStem,   /* The stem to be rendered */
  char **pzBuf,         /* Write results into this buffer.  realloc if needed */
  int *pnBuf            /* Size of the buffer */
){
  const fuzzer_rule *pRule = pStem->pRule;
  int n;
  char *z;

  n = pStem->nBasis + pRule->nTo - pRule->nFrom;
  if( (*pnBuf)<n+1 ){
    (*pzBuf) = sqlite3_realloc((*pzBuf), n+100);
    if( (*pzBuf)==0 ) return SQLITE_NOMEM;
    (*pnBuf) = n+100;
  }
  n = pStem->n;
  z = *pzBuf;
  if( n<0 ){
    memcpy(z, pStem->zBasis, pStem->nBasis+1);
  }else{
    memcpy(z, pStem->zBasis, n);
    memcpy(&z[n], pRule->zTo, pRule->nTo);
    memcpy(&z[n+pRule->nTo], &pStem->zBasis[n+pRule->nFrom], 
           pStem->nBasis-n-pRule->nFrom+1);
  }
  return SQLITE_OK;
}

/*
** Compute a hash on zBasis.
*/
static unsigned int fuzzerHash(const char *z){
  unsigned int h = 0;
  while( *z ){ h = (h<<3) ^ (h>>29) ^ *(z++); }
  return h % FUZZER_HASH;
}

/*
** Current cost of a stem
*/
static fuzzer_cost fuzzerCost(fuzzer_stem *pStem){
  return pStem->rBaseCost + pStem->pRule->rCost;
}

#if 0
/*
** Print a description of a fuzzer_stem on stderr.
*/
static void fuzzerStemPrint(
  const char *zPrefix,
  fuzzer_stem *pStem,
  const char *zSuffix
){
  if( pStem->n<0 ){
    fprintf(stderr, "%s[%s](%d)-->self%s",
       zPrefix,
       pStem->zBasis, pStem->rBaseCost,
       zSuffix
    );
  }else{
    char *zBuf = 0;
    int nBuf = 0;
    if( fuzzerRender(pStem, &zBuf, &nBuf)!=SQLITE_OK ) return;
    fprintf(stderr, "%s[%s](%d)-->{%s}(%d)%s",
      zPrefix,
      pStem->zBasis, pStem->rBaseCost, zBuf, fuzzerCost(pStem),
      zSuffix
    );
    sqlite3_free(zBuf);
  }
}
#endif

/*
** Return 1 if the string to which the cursor is point has already
** been emitted.  Return 0 if not.  Return -1 on a memory allocation
** failures.
*/
static int fuzzerSeen(fuzzer_cursor *pCur, fuzzer_stem *pStem){
  unsigned int h;
  fuzzer_stem *pLookup;

  if( fuzzerRender(pStem, &pCur->zBuf, &pCur->nBuf)==SQLITE_NOMEM ){
    return -1;
  }
  h = fuzzerHash(pCur->zBuf);
  pLookup = pCur->apHash[h];
    while( pLookup && strcmp(pLookup->zBasis, pCur->zBuf)!=0 ){
    pLookup = pLookup->pHash;
  }
  return pLookup!=0;
}

/*
** Advance a fuzzer_stem to its next value.   Return 0 if there are
** no more values that can be generated by this fuzzer_stem.  Return
** -1 on a memory allocation failure.
*/
static int fuzzerAdvance(fuzzer_cursor *pCur, fuzzer_stem *pStem){
  const fuzzer_rule *pRule;
  while( (pRule = pStem->pRule)!=0 ){
    while( pStem->n < pStem->nBasis - pRule->nFrom ){
      pStem->n++;
      if( pRule->nFrom==0
       || memcmp(&pStem->zBasis[pStem->n], pRule->zFrom, pRule->nFrom)==0
      ){
        /* Found a rewrite case.  Make sure it is not a duplicate */
        int rc = fuzzerSeen(pCur, pStem);
        if( rc<0 ) return -1;
        if( rc==0 ){
          return 1;
        }
      }
    }
    pStem->n = -1;
    pStem->pRule = pRule->pNext;
    if( pStem->pRule && fuzzerCost(pStem)>pCur->rLimit ) pStem->pRule = 0;
  }
  return 0;
}

/*
** Insert pNew into the list at pList.  Return a pointer to the new
** list.  The insert is done such the pNew is in the correct order
** according to fuzzer_stem.zBaseCost+fuzzer_stem.pRule->rCost.
*/
static fuzzer_stem *fuzzerInsert(fuzzer_stem *pList, fuzzer_stem *pNew){
  fuzzer_cost c1;

  if( pList==0 ){
    pNew->pNext = 0;
    return pNew;
  }
  c1 = fuzzerCost(pNew);
  if( c1 <= fuzzerCost(pList) ){
    pNew->pNext = pList;
    return pNew;
  }else{
    fuzzer_stem *pPrev;
    pPrev = pList;
    while( pPrev->pNext && fuzzerCost(pPrev->pNext)<c1 ){
      pPrev = pPrev->pNext;
    }
    pNew->pNext = pPrev->pNext;
    pPrev->pNext = pNew;
    return pList;
  }
}

/*
** Allocate a new fuzzer_stem.  Add it to the hash table but do not
** link it into either the pCur->pStem or pCur->pDone lists.
*/
static fuzzer_stem *fuzzerNewStem(
  fuzzer_cursor *pCur,
  const char *zWord,
  fuzzer_cost rBaseCost
){
  fuzzer_stem *pNew;
  unsigned int h;

  pNew = sqlite3_malloc( sizeof(*pNew) + strlen(zWord) + 1 );
  if( pNew==0 ) return 0;
  memset(pNew, 0, sizeof(*pNew));
  pNew->zBasis = (char*)&pNew[1];
  pNew->nBasis = strlen(zWord);
  memcpy(pNew->zBasis, zWord, pNew->nBasis+1);
  pNew->pRule = pCur->pVtab->pRule;
  pNew->n = -1;
  pNew->rBaseCost = rBaseCost;
  h = fuzzerHash(pNew->zBasis);
  pNew->pHash = pCur->apHash[h];
  pCur->apHash[h] = pNew;
  return pNew;
}


/*
** Advance a cursor to its next row of output
*/
static int fuzzerNext(sqlite3_vtab_cursor *cur){
  fuzzer_cursor *pCur = (fuzzer_cursor*)cur;
  int rc;
  fuzzer_stem *pStem, *pNew;

  pCur->iRowid++;

  /* Use the element the cursor is currently point to to create
  ** a new stem and insert the new stem into the priority queue.
  */
  pStem = pCur->pStem;
  if( fuzzerCost(pStem)>0 ){
    rc = fuzzerRender(pStem, &pCur->zBuf, &pCur->nBuf);
    if( rc==SQLITE_NOMEM ) return SQLITE_NOMEM;
    pNew = fuzzerNewStem(pCur, pCur->zBuf, fuzzerCost(pStem));
    if( pNew ){
      if( fuzzerAdvance(pCur, pNew)==0 ){
        pNew->pNext = pCur->pDone;
        pCur->pDone = pNew;
      }else{
        pCur->pStem = fuzzerInsert(pStem, pNew);
        if( pCur->pStem==pNew ){
          return SQLITE_OK;
        }
      }
    }else{
      return SQLITE_NOMEM;
    }
  }

  /* Adjust the priority queue so that the first element of the
  ** stem list is the next lowest cost word.
  */
  while( (pStem = pCur->pStem)!=0 ){
    if( fuzzerAdvance(pCur, pStem) ){
      pCur->pStem = pStem = fuzzerInsert(pStem->pNext, pStem);
      if( (rc = fuzzerSeen(pCur, pStem))!=0 ){
        if( rc<0 ) return SQLITE_NOMEM;
        continue;
      }
      return SQLITE_OK;  /* New word found */
    }
    pCur->pStem = pStem->pNext;
    pStem->pNext = pCur->pDone;
    pCur->pDone = pStem;
    if( pCur->pStem ){
      rc = fuzzerSeen(pCur, pCur->pStem);
      if( rc<0 ) return SQLITE_NOMEM;
      if( rc==0 ){
        return SQLITE_OK;
      }
    }
  }

  /* Reach this point only if queue has been exhausted and there is
  ** nothing left to be output. */
  pCur->rLimit = (fuzzer_cost)0;
  return SQLITE_OK;
}

/*
** Called to "rewind" a cursor back to the beginning so that
** it starts its output over again.  Always called at least once
** prior to any fuzzerColumn, fuzzerRowid, or fuzzerEof call.
*/
static int fuzzerFilter(
  sqlite3_vtab_cursor *pVtabCursor, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  fuzzer_cursor *pCur = (fuzzer_cursor *)pVtabCursor;
  const char *zWord = 0;
  fuzzer_stem *pStem;

  fuzzerClearCursor(pCur, 1);
  pCur->rLimit = 2147483647;
  if( idxNum==1 ){
    zWord = (const char*)sqlite3_value_text(argv[0]);
  }else if( idxNum==2 ){
    pCur->rLimit = (fuzzer_cost)sqlite3_value_int(argv[0]);
  }else if( idxNum==3 ){
    zWord = (const char*)sqlite3_value_text(argv[0]);
    pCur->rLimit = (fuzzer_cost)sqlite3_value_int(argv[1]);
  }
  if( zWord==0 ) zWord = "";
  pCur->pStem = pStem = fuzzerNewStem(pCur, zWord, (fuzzer_cost)0);
  if( pStem==0 ) return SQLITE_NOMEM;
  pCur->nullRule.pNext = pCur->pVtab->pRule;
  pCur->nullRule.rCost = 0;
  pCur->nullRule.nFrom = 0;
  pCur->nullRule.nTo = 0;
  pCur->nullRule.zFrom = "";
  pStem->pRule = &pCur->nullRule;
  pStem->n = pStem->nBasis;
  pCur->iRowid = 1;
  return SQLITE_OK;
}

/*
** Only the word and distance columns have values.  All other columns
** return NULL
*/
static int fuzzerColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int i){
  fuzzer_cursor *pCur = (fuzzer_cursor*)cur;
  if( i==0 ){
    /* the "word" column */
    if( fuzzerRender(pCur->pStem, &pCur->zBuf, &pCur->nBuf)==SQLITE_NOMEM ){
      return SQLITE_NOMEM;
    }
    sqlite3_result_text(ctx, pCur->zBuf, -1, SQLITE_TRANSIENT);
  }else if( i==1 ){
    /* the "distance" column */
    sqlite3_result_int(ctx, fuzzerCost(pCur->pStem));
  }else{
    /* All other columns are NULL */
    sqlite3_result_null(ctx);
  }
  return SQLITE_OK;
}

/*
** The rowid.
*/
static int fuzzerRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  fuzzer_cursor *pCur = (fuzzer_cursor*)cur;
  *pRowid = pCur->iRowid;
  return SQLITE_OK;
}

/*
** When the fuzzer_cursor.rLimit value is 0 or less, that is a signal
** that the cursor has nothing more to output.
*/
static int fuzzerEof(sqlite3_vtab_cursor *cur){
  fuzzer_cursor *pCur = (fuzzer_cursor*)cur;
  return pCur->rLimit<=(fuzzer_cost)0;
}

/*
** Search for terms of these forms:
**
**       word MATCH $str
**       distance < $value
**       distance <= $value
**
** The distance< and distance<= are both treated as distance<=.
** The query plan number is as follows:
**
**   0:    None of the terms above are found
**   1:    There is a "word MATCH" term with $str in filter.argv[0].
**   2:    There is a "distance<" term with $value in filter.argv[0].
**   3:    Both "word MATCH" and "distance<" with $str in argv[0] and
**         $value in argv[1].
*/
static int fuzzerBestIndex(sqlite3_vtab *tab, sqlite3_index_info *pIdxInfo){
  int iPlan = 0;
  int iDistTerm = -1;
  int i;
  const struct sqlite3_index_constraint *pConstraint;
  pConstraint = pIdxInfo->aConstraint;
  for(i=0; i<pIdxInfo->nConstraint; i++, pConstraint++){
    if( pConstraint->usable==0 ) continue;
    if( (iPlan & 1)==0 
     && pConstraint->iColumn==0
     && pConstraint->op==SQLITE_INDEX_CONSTRAINT_MATCH
    ){
      iPlan |= 1;
      pIdxInfo->aConstraintUsage[i].argvIndex = 1;
      pIdxInfo->aConstraintUsage[i].omit = 1;
    }
    if( (iPlan & 2)==0
     && pConstraint->iColumn==1
     && (pConstraint->op==SQLITE_INDEX_CONSTRAINT_LT
           || pConstraint->op==SQLITE_INDEX_CONSTRAINT_LE)
    ){
      iPlan |= 2;
      iDistTerm = i;
    }
  }
  if( iPlan==2 ){
    pIdxInfo->aConstraintUsage[iDistTerm].argvIndex = 1;
  }else if( iPlan==3 ){
    pIdxInfo->aConstraintUsage[iDistTerm].argvIndex = 2;
  }
  pIdxInfo->idxNum = iPlan;
  if( pIdxInfo->nOrderBy==1
   && pIdxInfo->aOrderBy[0].iColumn==1
   && pIdxInfo->aOrderBy[0].desc==0
  ){
    pIdxInfo->orderByConsumed = 1;
  }
  pIdxInfo->estimatedCost = (double)10000;
   
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
  fuzzer_cost rCost;
  if( argc!=7 ){
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
  rCost = sqlite3_value_int(argv[6]);
  if( rCost<=0 ){
    sqlite3_free(pVTab->zErrMsg);
    pVTab->zErrMsg = sqlite3_mprintf("cost must be positive");
    return SQLITE_CONSTRAINT;    
  }
  nFrom = strlen(zFrom);
  nTo = strlen(zTo);
  pRule = sqlite3_malloc( sizeof(*pRule) + nFrom + nTo );
  if( pRule==0 ){
    return SQLITE_NOMEM;
  }
  pRule->zFrom = &pRule->zTo[nTo+1];
  pRule->nFrom = nFrom;
  memcpy(pRule->zFrom, zFrom, nFrom+1);
  memcpy(pRule->zTo, zTo, nTo+1);
  pRule->nTo = nTo;
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
