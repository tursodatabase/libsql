/*
** 2018 May 08
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

/*
** Implementation of built-in window function row_number(). Assumes that the
** window frame has been coerced to:
**
**   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
*/
static void row_numberStepFunc(
  sqlite3_context *pCtx, 
  int nArg,
  sqlite3_value **apArg
){
  i64 *p = (i64*)sqlite3_aggregate_context(pCtx, sizeof(*p));
  if( p ) (*p)++;
}
static void row_numberInvFunc(
  sqlite3_context *pCtx, 
  int nArg,
  sqlite3_value **apArg
){
}
static void row_numberValueFunc(sqlite3_context *pCtx){
  i64 *p = (i64*)sqlite3_aggregate_context(pCtx, sizeof(*p));
  sqlite3_result_int64(pCtx, (p ? *p : 0));
}

/*
** Context object type used by rank(), dense_rank(), percent_rank() and
** cume_dist().
*/
struct CallCount {
  i64 nValue;
  i64 nStep;
  i64 nTotal;
};

/*
** Implementation of built-in window function dense_rank().
*/
static void dense_rankStepFunc(
  sqlite3_context *pCtx, 
  int nArg,
  sqlite3_value **apArg
){
  struct CallCount *p;
  p = (struct CallCount*)sqlite3_aggregate_context(pCtx, sizeof(*p));
  if( p ) p->nStep = 1;
}
static void dense_rankInvFunc(
  sqlite3_context *pCtx, 
  int nArg,
  sqlite3_value **apArg
){
}
static void dense_rankValueFunc(sqlite3_context *pCtx){
  struct CallCount *p;
  p = (struct CallCount*)sqlite3_aggregate_context(pCtx, sizeof(*p));
  if( p ){
    if( p->nStep ){
      p->nValue++;
      p->nStep = 0;
    }
    sqlite3_result_int64(pCtx, p->nValue);
  }
}

/*
** Implementation of built-in window function rank().
*/
static void rankStepFunc(
  sqlite3_context *pCtx, 
  int nArg,
  sqlite3_value **apArg
){
  struct CallCount *p;
  p = (struct CallCount*)sqlite3_aggregate_context(pCtx, sizeof(*p));
  if( p ){
    p->nStep++;
    if( p->nValue==0 ){
      p->nValue = p->nStep;
    }
  }
}
static void rankInvFunc(
  sqlite3_context *pCtx, 
  int nArg,
  sqlite3_value **apArg
){
}
static void rankValueFunc(sqlite3_context *pCtx){
  struct CallCount *p;
  p = (struct CallCount*)sqlite3_aggregate_context(pCtx, sizeof(*p));
  if( p ){
    sqlite3_result_int64(pCtx, p->nValue);
    p->nValue = 0;
  }
}

/*
** Implementation of built-in window function percent_rank().
*/
static void percent_rankStepFunc(
  sqlite3_context *pCtx, 
  int nArg,
  sqlite3_value **apArg
){
  struct CallCount *p;
  assert( nArg==1 );

  p = (struct CallCount*)sqlite3_aggregate_context(pCtx, sizeof(*p));
  if( p ){
    if( p->nTotal==0 ){
      p->nTotal = sqlite3_value_int64(apArg[0]);
    }
    p->nStep++;
    if( p->nValue==0 ){
      p->nValue = p->nStep;
    }
  }
}
static void percent_rankInvFunc(
  sqlite3_context *pCtx, 
  int nArg,
  sqlite3_value **apArg
){
}
static void percent_rankValueFunc(sqlite3_context *pCtx){
  struct CallCount *p;
  p = (struct CallCount*)sqlite3_aggregate_context(pCtx, sizeof(*p));
  if( p ){
    if( p->nTotal>1 ){
      double r = (double)(p->nValue-1) / (double)(p->nTotal-1);
      sqlite3_result_double(pCtx, r);
    }else{
      sqlite3_result_double(pCtx, 100.0);
    }
    p->nValue = 0;
  }
}

static void cume_distStepFunc(
  sqlite3_context *pCtx, 
  int nArg,
  sqlite3_value **apArg
){
  struct CallCount *p;
  assert( nArg==1 );

  p = (struct CallCount*)sqlite3_aggregate_context(pCtx, sizeof(*p));
  if( p ){
    if( p->nTotal==0 ){
      p->nTotal = sqlite3_value_int64(apArg[0]);
    }
    p->nStep++;
  }
}
static void cume_distInvFunc(
  sqlite3_context *pCtx, 
  int nArg,
  sqlite3_value **apArg
){
}
static void cume_distValueFunc(sqlite3_context *pCtx){
  struct CallCount *p;
  p = (struct CallCount*)sqlite3_aggregate_context(pCtx, sizeof(*p));
  if( p ){
    double r = (double)(p->nStep) / (double)(p->nTotal);
    sqlite3_result_double(pCtx, r);
  }
}

/*
** Context object for ntile() window function.
*/
struct NtileCtx {
  i64 nTotal;                     /* Total rows in partition */
  i64 nParam;                     /* Parameter passed to ntile(N) */
  i64 iRow;                       /* Current row */
};

/*
** Implementation of ntile(). This assumes that the window frame has
** been coerced to:
**
**   ROWS UNBOUNDED PRECEDING AND CURRENT ROW
*/
static void ntileStepFunc(
  sqlite3_context *pCtx, 
  int nArg,
  sqlite3_value **apArg
){
  struct NtileCtx *p;
  assert( nArg==2 );
  p = (struct NtileCtx*)sqlite3_aggregate_context(pCtx, sizeof(*p));
  if( p ){
    if( p->nTotal==0 ){
      p->nParam = sqlite3_value_int64(apArg[0]);
      p->nTotal = sqlite3_value_int64(apArg[1]);
      if( p->nParam<=0 ){
        sqlite3_result_error(
            pCtx, "argument of ntile must be a positive integer", -1
        );
      }
    }
    p->iRow++;
  }
}
static void ntileInvFunc(
  sqlite3_context *pCtx, 
  int nArg,
  sqlite3_value **apArg
){
}
static void ntileValueFunc(sqlite3_context *pCtx){
  struct NtileCtx *p;
  p = (struct NtileCtx*)sqlite3_aggregate_context(pCtx, sizeof(*p));
  if( p && p->nParam>0 ){
    int nSize = (p->nTotal / p->nParam);
    if( nSize==0 ){
      sqlite3_result_int64(pCtx, p->iRow);
    }else{
      i64 nLarge = p->nTotal - p->nParam*nSize;
      i64 iSmall = nLarge*(nSize+1);
      i64 iRow = p->iRow-1;

      assert( (nLarge*(nSize+1) + (p->nParam-nLarge)*nSize)==p->nTotal );

      if( iRow<iSmall ){
        sqlite3_result_int64(pCtx, 1 + iRow/(nSize+1));
      }else{
        sqlite3_result_int64(pCtx, 1 + nLarge + (iRow-iSmall)/nSize);
      }
    }
  }
}

/*
** Context object for last_value() window function.
*/
struct LastValueCtx {
  sqlite3_value *pVal;
  int nVal;
};

/*
** Implementation of last_value().
*/
static void last_valueStepFunc(
  sqlite3_context *pCtx, 
  int nArg,
  sqlite3_value **apArg
){
  struct LastValueCtx *p;
  p = (struct LastValueCtx *)sqlite3_aggregate_context(pCtx, sizeof(*p));
  if( p ){
    sqlite3_value_free(p->pVal);
    p->pVal = sqlite3_value_dup(apArg[0]);
    p->nVal++;
  }
}
static void last_valueInvFunc(
  sqlite3_context *pCtx, 
  int nArg,
  sqlite3_value **apArg
){
  struct LastValueCtx *p;
  p = (struct LastValueCtx *)sqlite3_aggregate_context(pCtx, sizeof(*p));
  if( p ){
    p->nVal--;
    if( p->nVal==0 ){
      sqlite3_value_free(p->pVal);
      p->pVal = 0;
    }
  }
}
static void last_valueValueFunc(sqlite3_context *pCtx){
  struct LastValueCtx *p;
  p = (struct LastValueCtx *)sqlite3_aggregate_context(pCtx, sizeof(*p));
  if( p && p->pVal ){
    sqlite3_result_value(pCtx, p->pVal);
  }
}
static void last_valueFinalizeFunc(sqlite3_context *pCtx){
  struct LastValueCtx *p;
  p = (struct LastValueCtx *)sqlite3_aggregate_context(pCtx, sizeof(*p));
  if( p && p->pVal ){
    sqlite3_result_value(pCtx, p->pVal);
    sqlite3_value_free(p->pVal);
    p->pVal = 0;
  }
}

/*
** No-op implementations of nth_value(), first_value(), lead() and lag().
** These are all implemented inline using VDBE instructions. 
*/
static void nth_valueStepFunc(sqlite3_context *pCtx, int n, sqlite3_value **a){}
static void nth_valueInvFunc(sqlite3_context *pCtx, int n, sqlite3_value **ap){}
static void nth_valueValueFunc(sqlite3_context *pCtx){}
static void first_valueStepFunc(sqlite3_context *p, int n, sqlite3_value **ap){}
static void first_valueInvFunc(sqlite3_context *p, int n, sqlite3_value **ap){}
static void first_valueValueFunc(sqlite3_context *pCtx){}
static void leadStepFunc(sqlite3_context *pCtx, int n, sqlite3_value **ap){}
static void leadInvFunc(sqlite3_context *pCtx, int n, sqlite3_value **ap){}
static void leadValueFunc(sqlite3_context *pCtx){}
static void lagStepFunc(sqlite3_context *pCtx, int n, sqlite3_value **ap){}
static void lagInvFunc(sqlite3_context *pCtx, int n, sqlite3_value **ap){}
static void lagValueFunc(sqlite3_context *pCtx){}

#define WINDOWFUNC(name,nArg,extra) {                                      \
  nArg, (SQLITE_UTF8|SQLITE_FUNC_WINDOW|extra), 0, 0,                      \
  name ## StepFunc, name ## ValueFunc, name ## ValueFunc,                  \
  name ## InvFunc, #name                                               \
}

#define WINDOWFUNCF(name,nArg,extra) {                                     \
  nArg, (SQLITE_UTF8|SQLITE_FUNC_WINDOW|extra), 0, 0,                      \
  name ## StepFunc, name ## FinalizeFunc, name ## ValueFunc,               \
  name ## InvFunc, #name                                               \
}

/*
** Register those built-in window functions that are not also aggregates.
*/
void sqlite3WindowFunctions(void){
  static FuncDef aWindowFuncs[] = {
    WINDOWFUNC(row_number, 0, 0),
    WINDOWFUNC(dense_rank, 0, 0),
    WINDOWFUNC(rank, 0, 0),
    WINDOWFUNC(percent_rank, 0, SQLITE_FUNC_WINDOW_SIZE),
    WINDOWFUNC(cume_dist, 0, SQLITE_FUNC_WINDOW_SIZE),
    WINDOWFUNC(ntile, 1, SQLITE_FUNC_WINDOW_SIZE),
    WINDOWFUNCF(last_value, 1, 0),
    WINDOWFUNC(nth_value, 2, 0),
    WINDOWFUNC(first_value, 1, 0),
    WINDOWFUNC(lead, 1, 0), WINDOWFUNC(lead, 2, 0), WINDOWFUNC(lead, 3, 0),
    WINDOWFUNC(lag, 1, 0),  WINDOWFUNC(lag, 2, 0),  WINDOWFUNC(lag, 3, 0),
  };
  sqlite3InsertBuiltinFuncs(aWindowFuncs, ArraySize(aWindowFuncs));
}

void sqlite3WindowUpdate(
  Parse *pParse, 
  Window *pList, 
  Window *pWin, 
  FuncDef *pFunc
){
  if( pWin->zName ){
    Window *p;
    for(p=pList; p; p=p->pNextWin){
      if( sqlite3StrICmp(p->zName, pWin->zName)==0 ) break;
    }
    if( p==0 ){
      sqlite3ErrorMsg(pParse, "no such window: %s", pWin->zName);
      return;
    }
    pWin->pPartition = sqlite3ExprListDup(pParse->db, p->pPartition, 0);
    pWin->pOrderBy = sqlite3ExprListDup(pParse->db, p->pOrderBy, 0);
    pWin->pStart = sqlite3ExprDup(pParse->db, p->pStart, 0);
    pWin->pEnd = sqlite3ExprDup(pParse->db, p->pEnd, 0);
    pWin->eStart = p->eStart;
    pWin->eEnd = p->eEnd;
  }
  if( pFunc->funcFlags & SQLITE_FUNC_WINDOW ){
    sqlite3 *db = pParse->db;
    if( pWin->pFilter ){
      sqlite3ErrorMsg(pParse, 
          "FILTER clause may only be used with aggregate window functions"
      );
    }else
    if( pFunc->xSFunc==row_numberStepFunc || pFunc->xSFunc==ntileStepFunc ){
      sqlite3ExprDelete(db, pWin->pStart);
      sqlite3ExprDelete(db, pWin->pEnd);
      pWin->pStart = pWin->pEnd = 0;
      pWin->eType = TK_ROWS;
      pWin->eStart = TK_UNBOUNDED;
      pWin->eEnd = TK_CURRENT;
    }else

    if( pFunc->xSFunc==dense_rankStepFunc || pFunc->xSFunc==rankStepFunc
     || pFunc->xSFunc==percent_rankStepFunc || pFunc->xSFunc==cume_distStepFunc
    ){
      sqlite3ExprDelete(db, pWin->pStart);
      sqlite3ExprDelete(db, pWin->pEnd);
      pWin->pStart = pWin->pEnd = 0;
      pWin->eType = TK_RANGE;
      pWin->eStart = TK_UNBOUNDED;
      pWin->eEnd = TK_CURRENT;
    }
  }
  pWin->pFunc = pFunc;
}

typedef struct WindowRewrite WindowRewrite;
struct WindowRewrite {
  Window *pWin;
  ExprList *pSub;
};

static int selectWindowRewriteSelectCb(Walker *pWalker, Select *pSelect){
  return WRC_Prune;
}

static int selectWindowRewriteExprCb(Walker *pWalker, Expr *pExpr){
  struct WindowRewrite *p = pWalker->u.pRewrite;
  Parse *pParse = pWalker->pParse;

  switch( pExpr->op ){

    case TK_FUNCTION:
      if( pExpr->pWin==0 ){
        break;
      }else{
        Window *pWin;
        for(pWin=p->pWin; pWin; pWin=pWin->pNextWin){
          if( pExpr->pWin==pWin ){
            assert( pWin->pOwner==pExpr );
            return WRC_Prune;
          }
        }
      }
      /* Fall through.  */

    case TK_COLUMN: {
      Expr *pDup = sqlite3ExprDup(pParse->db, pExpr, 0);
      p->pSub = sqlite3ExprListAppend(pParse, p->pSub, pDup);
      if( p->pSub ){
        assert( ExprHasProperty(pExpr, EP_Static)==0 );
        ExprSetProperty(pExpr, EP_Static);
        sqlite3ExprDelete(pParse->db, pExpr);
        ExprClearProperty(pExpr, EP_Static);
        memset(pExpr, 0, sizeof(Expr));

        pExpr->op = TK_COLUMN;
        pExpr->iColumn = p->pSub->nExpr-1;
        pExpr->iTable = p->pWin->iEphCsr;
      }

      break;
    }

    default: /* no-op */
      break;
  }

  return WRC_Continue;
}

static int selectWindowRewriteEList(
  Parse *pParse, 
  Window *pWin,
  ExprList *pEList,               /* Rewrite expressions in this list */
  ExprList **ppSub                /* IN/OUT: Sub-select expression-list */
){
  Walker sWalker;
  WindowRewrite sRewrite;
  int rc;

  memset(&sWalker, 0, sizeof(Walker));
  memset(&sRewrite, 0, sizeof(WindowRewrite));

  sRewrite.pSub = *ppSub;
  sRewrite.pWin = pWin;

  sWalker.pParse = pParse;
  sWalker.xExprCallback = selectWindowRewriteExprCb;
  sWalker.xSelectCallback = selectWindowRewriteSelectCb;
  sWalker.u.pRewrite = &sRewrite;

  rc = sqlite3WalkExprList(&sWalker, pEList);

  *ppSub = sRewrite.pSub;
  return rc;
}

static ExprList *exprListAppendList(
  Parse *pParse,          /* Parsing context */
  ExprList *pList,        /* List to which to append. Might be NULL */
  ExprList *pAppend       /* List of values to append. Might be NULL */
){
  if( pAppend ){
    int i;
    int nInit = pList ? pList->nExpr : 0;
    for(i=0; i<pAppend->nExpr; i++){
      Expr *pDup = sqlite3ExprDup(pParse->db, pAppend->a[i].pExpr, 0);
      pList = sqlite3ExprListAppend(pParse, pList, pDup);
      if( pList ) pList->a[nInit+i].sortOrder = pAppend->a[i].sortOrder;
    }
  }
  return pList;
}

/*
** If the SELECT statement passed as the second argument does not invoke
** any SQL window functions, this function is a no-op. Otherwise, it 
** rewrites the SELECT statement so that window function xStep functions
** are invoked in the correct order. The simplest version of the 
** transformation is:
**
**   SELECT win(args...) OVER (<list1>) FROM <src> ORDER BY <list2>
**
** to
**
**   SELECT win(args...) FROM (
**     SELECT args... FROM <src> ORDER BY <list1>
**   ) ORDER BY <list2>
**
** where <src> may contain WHERE, GROUP BY and HAVING clauses, and <list1>
** is the concatenation of the PARTITION BY and ORDER BY clauses in the
** OVER clause.
**
*/
int sqlite3WindowRewrite(Parse *pParse, Select *p){
  int rc = SQLITE_OK;
  if( p->pWin ){
    Vdbe *v = sqlite3GetVdbe(pParse);
    int i;
    sqlite3 *db = pParse->db;
    Select *pSub = 0;             /* The subquery */
    SrcList *pSrc = p->pSrc;
    Expr *pWhere = p->pWhere;
    ExprList *pGroupBy = p->pGroupBy;
    Expr *pHaving = p->pHaving;
    ExprList *pSort = 0;

    ExprList *pSublist = 0;       /* Expression list for sub-query */
    Window *pMWin = p->pWin;      /* Master window object */
    Window *pWin;                 /* Window object iterator */

    p->pSrc = 0;
    p->pWhere = 0;
    p->pGroupBy = 0;
    p->pHaving = 0;

    /* Assign a cursor number for the ephemeral table used to buffer rows.
    ** The OpenEphemeral instruction is coded later, after it is known how
    ** many columns the table will have.  */
    pMWin->iEphCsr = pParse->nTab++;

    rc = selectWindowRewriteEList(pParse, pMWin, p->pEList, &pSublist);
    if( rc ) return rc;
    rc = selectWindowRewriteEList(pParse, pMWin, p->pOrderBy, &pSublist);
    if( rc ) return rc;
    pMWin->nBufferCol = (pSublist ? pSublist->nExpr : 0);

    /* Create the ORDER BY clause for the sub-select. This is the concatenation
    ** of the window PARTITION and ORDER BY clauses. Append the same 
    ** expressions to the sub-select expression list. They are required to
    ** figure out where boundaries for partitions and sets of peer rows.  */
    pSort = sqlite3ExprListDup(db, pMWin->pPartition, 0);
    if( pMWin->pOrderBy ){
      pSort = exprListAppendList(pParse, pSort, pMWin->pOrderBy);
    }
    pSublist = exprListAppendList(pParse, pSublist, pSort);

    /* Append the arguments passed to each window function to the
    ** sub-select expression list. Also allocate two registers for each
    ** window function - one for the accumulator, another for interim
    ** results.  */
    for(pWin=pMWin; pWin; pWin=pWin->pNextWin){
      pWin->iArgCol = (pSublist ? pSublist->nExpr : 0);
      pSublist = exprListAppendList(pParse, pSublist, pWin->pOwner->x.pList);
      if( pWin->pFilter ){
        Expr *pFilter = sqlite3ExprDup(db, pWin->pFilter, 0);
        pSublist = sqlite3ExprListAppend(pParse, pSublist, pFilter);
      }
      pWin->regAccum = ++pParse->nMem;
      pWin->regResult = ++pParse->nMem;
      sqlite3VdbeAddOp2(v, OP_Null, 0, pWin->regAccum);
    }

    pSub = sqlite3SelectNew(
        pParse, pSublist, pSrc, pWhere, pGroupBy, pHaving, pSort, 0, 0
    );
    p->pSrc = sqlite3SrcListAppend(db, 0, 0, 0);
    if( p->pSrc ){
      int iTab;
      ExprList *pList = 0;
      p->pSrc->a[0].pSelect = pSub;
      sqlite3SrcListAssignCursors(pParse, p->pSrc);
      if( sqlite3ExpandSubquery(pParse, &p->pSrc->a[0]) ){
        rc = SQLITE_NOMEM;
      }else{
        pSub->selFlags |= SF_Expanded;
      }
    }

    sqlite3VdbeAddOp2(v, OP_OpenEphemeral, pMWin->iEphCsr, pSublist->nExpr);
  }

  return rc;
}

void sqlite3WindowDelete(sqlite3 *db, Window *p){
  if( p ){
    sqlite3ExprDelete(db, p->pFilter);
    sqlite3ExprListDelete(db, p->pPartition);
    sqlite3ExprListDelete(db, p->pOrderBy);
    sqlite3ExprDelete(db, p->pEnd);
    sqlite3ExprDelete(db, p->pStart);
    sqlite3DbFree(db, p->zName);
    sqlite3DbFree(db, p);
  }
}

void sqlite3WindowListDelete(sqlite3 *db, Window *p){
  while( p ){
    Window *pNext = p->pNextWin;
    sqlite3WindowDelete(db, p);
    p = pNext;
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
    if( pWin ) pWin->pOwner = p;
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

static void windowAggInit(Parse *pParse, Window *pMWin){
  Window *pWin;
  for(pWin=pMWin; pWin; pWin=pWin->pNextWin){
    Vdbe *v = sqlite3GetVdbe(pParse);
    FuncDef *p = pWin->pFunc;
    if( (p->funcFlags & SQLITE_FUNC_MINMAX) && pWin->eStart!=TK_UNBOUNDED ){
      ExprList *pList = pWin->pOwner->x.pList;
      KeyInfo *pKeyInfo = sqlite3KeyInfoFromExprList(pParse, pList, 0, 0);
      pWin->csrApp = pParse->nTab++;
      pWin->regApp = pParse->nMem+1;
      pParse->nMem += 3;
      if( pKeyInfo && pWin->pFunc->zName[1]=='i' ){
        assert( pKeyInfo->aSortOrder[0]==0 );
        pKeyInfo->aSortOrder[0] = 1;
      }
      sqlite3VdbeAddOp2(v, OP_OpenEphemeral, pWin->csrApp, 2);
      sqlite3VdbeAppendP4(v, pKeyInfo, P4_KEYINFO);
      sqlite3VdbeAddOp2(v, OP_Integer, 0, pWin->regApp+1);
    }
    else if( p->xSFunc==nth_valueStepFunc || p->xSFunc==first_valueStepFunc ){
      /* Allocate two registers at pWin->regApp. These will be used to
      ** store the start and end index of the current frame.  */
      assert( pMWin->iEphCsr );
      pWin->regApp = pParse->nMem+1;
      pWin->csrApp = pParse->nTab++;
      pParse->nMem += 2;
      sqlite3VdbeAddOp2(v, OP_OpenDup, pWin->csrApp, pMWin->iEphCsr);
    }
    else if( p->xSFunc==leadStepFunc || p->xSFunc==lagStepFunc ){
      assert( pMWin->iEphCsr );
      pWin->csrApp = pParse->nTab++;
      sqlite3VdbeAddOp2(v, OP_OpenDup, pWin->csrApp, pMWin->iEphCsr);
    }
  }
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
  windowAggInit(pParse, pWin);
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

static int windowArgCount(Window *pWin){
  ExprList *pList = pWin->pOwner->x.pList;
  return (pList ? pList->nExpr : 0);
}

/*
** Generate VM code to invoke either xStep() (if bInverse is 0) or 
** xInverse (if bInverse is non-zero) for each window function in the 
** linked list starting at pMWin.
*/
static void windowAggStep(
  Parse *pParse, 
  Window *pMWin, 
  int csr,
  int bInverse, 
  int reg,
  int regPartSize                 /* Register containing size of partition */
){
  Vdbe *v = sqlite3GetVdbe(pParse);
  Window *pWin;
  for(pWin=pMWin; pWin; pWin=pWin->pNextWin){
    int flags = pWin->pFunc->funcFlags;
    int regArg;
    int nArg = windowArgCount(pWin);

    if( csr>=0 ){
      int i;
      for(i=0; i<nArg; i++){
        sqlite3VdbeAddOp3(v, OP_Column, csr, pWin->iArgCol+i, reg+i);
      }
      regArg = reg;
      if( flags & SQLITE_FUNC_WINDOW_SIZE ){
        if( nArg==0 ){
          regArg = regPartSize;
        }else{
          sqlite3VdbeAddOp2(v, OP_SCopy, regPartSize, reg+nArg);
        }
        nArg++;
      }
    }else{
      assert( !(flags & SQLITE_FUNC_WINDOW_SIZE) );
      regArg = reg + pWin->iArgCol;
    }

    if( (pWin->pFunc->funcFlags & SQLITE_FUNC_MINMAX) 
      && pWin->eStart!=TK_UNBOUNDED 
    ){
      if( bInverse==0 ){
        sqlite3VdbeAddOp2(v, OP_AddImm, pWin->regApp+1, 1);
        sqlite3VdbeAddOp2(v, OP_SCopy, regArg, pWin->regApp);
        sqlite3VdbeAddOp3(v, OP_MakeRecord, pWin->regApp, 2, pWin->regApp+2);
        sqlite3VdbeAddOp2(v, OP_IdxInsert, pWin->csrApp, pWin->regApp+2);
      }else{
        sqlite3VdbeAddOp4Int(v, OP_SeekGE, pWin->csrApp, 0, regArg, 1);
        sqlite3VdbeAddOp1(v, OP_Delete, pWin->csrApp);
        sqlite3VdbeJumpHere(v, sqlite3VdbeCurrentAddr(v)-2);
      }
    }else if( pWin->regApp ){
      assert( pWin->pFunc->xSFunc==nth_valueStepFunc 
           || pWin->pFunc->xSFunc==first_valueStepFunc 
      );
      assert( bInverse==0 || bInverse==1 );
      sqlite3VdbeAddOp2(v, OP_AddImm, pWin->regApp+1-bInverse, 1);
    }else if( pWin->pFunc->xSFunc==leadStepFunc 
           || pWin->pFunc->xSFunc==lagStepFunc 
    ){
      /* no-op */
    }else{
      int addrIf = 0;
      if( pWin->pFilter ){
        int regTmp;
        assert( nArg==pWin->pOwner->x.pList->nExpr );
        if( csr>0 ){
          regTmp = sqlite3GetTempReg(pParse);
          sqlite3VdbeAddOp3(v, OP_Column, csr, pWin->iArgCol+nArg,regTmp);
        }else{
          regTmp = regArg + nArg;
        }
        addrIf = sqlite3VdbeAddOp3(v, OP_IfNot, regTmp, 0, 1);
        if( csr>0 ){
          sqlite3ReleaseTempReg(pParse, regTmp);
        }
      }
      if( pWin->pFunc->funcFlags & SQLITE_FUNC_NEEDCOLL ){
        CollSeq *pColl;
        pColl = sqlite3ExprNNCollSeq(pParse, pWin->pOwner->x.pList->a[0].pExpr);
        sqlite3VdbeAddOp4(v, OP_CollSeq, 0,0,0, (const char*)pColl, P4_COLLSEQ);
      }
      sqlite3VdbeAddOp3(v, OP_AggStep0, bInverse, regArg, pWin->regAccum);
      sqlite3VdbeAppendP4(v, pWin->pFunc, P4_FUNCDEF);
      sqlite3VdbeChangeP5(v, (u8)nArg);
      if( addrIf ) sqlite3VdbeJumpHere(v, addrIf);
    }
  }
}

static void windowAggFinal(Parse *pParse, Window *pMWin, int bFinal){
  Vdbe *v = sqlite3GetVdbe(pParse);
  Window *pWin;

  for(pWin=pMWin; pWin; pWin=pWin->pNextWin){
    if( (pWin->pFunc->funcFlags & SQLITE_FUNC_MINMAX) 
     && pWin->eStart!=TK_UNBOUNDED 
    ){
      sqlite3VdbeAddOp2(v, OP_Null, 0, pWin->regResult);
      sqlite3VdbeAddOp1(v, OP_Last, pWin->csrApp);
      sqlite3VdbeAddOp3(v, OP_Column, pWin->csrApp, 0, pWin->regResult);
      sqlite3VdbeJumpHere(v, sqlite3VdbeCurrentAddr(v)-2);
      if( bFinal ){
        sqlite3VdbeAddOp1(v, OP_ResetSorter, pWin->csrApp);
      }
    }else if( pWin->regApp ){
    }else{
      if( bFinal==0 ){
        sqlite3VdbeAddOp2(v, OP_Null, 0, pWin->regResult);
      }
      sqlite3VdbeAddOp2(v, OP_AggFinal, pWin->regAccum, windowArgCount(pWin));
      sqlite3VdbeAppendP4(v, pWin->pFunc, P4_FUNCDEF);
      if( bFinal ){
        sqlite3VdbeAddOp2(v, OP_Copy, pWin->regAccum, pWin->regResult);
        sqlite3VdbeAddOp2(v, OP_Null, 0, pWin->regAccum);
      }else{
        sqlite3VdbeChangeP3(v, -1, pWin->regResult);
      }
    }
  }
}

static void windowPartitionCache(
  Parse *pParse,
  Select *p,
  WhereInfo *pWInfo,
  int regFlushPart,
  int lblFlushPart,
  int *pRegSize
){
  Window *pMWin = p->pWin;
  Vdbe *v = sqlite3GetVdbe(pParse);
  Window *pWin;
  int iSubCsr = p->pSrc->a[0].iCursor;
  int nSub = p->pSrc->a[0].pTab->nCol;
  int k;

  int reg = pParse->nMem+1;
  int regRecord = reg+nSub;
  int regRowid = regRecord+1;

  *pRegSize = regRowid;
  pParse->nMem += nSub + 2;

  /* Martial the row returned by the sub-select into an array of 
  ** registers. */
  for(k=0; k<nSub; k++){
    sqlite3VdbeAddOp3(v, OP_Column, iSubCsr, k, reg+k);
  }
  sqlite3VdbeAddOp3(v, OP_MakeRecord, reg, nSub, regRecord);

  /* Check if this is the start of a new partition. If so, call the
  ** flush_partition sub-routine.  */
  if( pMWin->pPartition ){
    int addr;
    ExprList *pPart = pMWin->pPartition;
    int nPart = (pPart ? pPart->nExpr : 0);
    int regNewPart = reg + pMWin->nBufferCol;
    KeyInfo *pKeyInfo = sqlite3KeyInfoFromExprList(pParse, pPart, 0, 0);

    addr = sqlite3VdbeAddOp3(v, OP_Compare, regNewPart, pMWin->regPart,nPart);
    sqlite3VdbeAppendP4(v, (void*)pKeyInfo, P4_KEYINFO);
    sqlite3VdbeAddOp3(v, OP_Jump, addr+2, addr+4, addr+2);
    sqlite3VdbeAddOp3(v, OP_Copy, regNewPart, pMWin->regPart, nPart-1);
    sqlite3VdbeAddOp2(v, OP_Gosub, regFlushPart, lblFlushPart);
  }

  /* Buffer the current row in the ephemeral table. */
  sqlite3VdbeAddOp2(v, OP_NewRowid, pMWin->iEphCsr, regRowid);
  sqlite3VdbeAddOp3(v, OP_Insert, pMWin->iEphCsr, regRecord, regRowid);

  /* End of the input loop */
  sqlite3WhereEnd(pWInfo);

  /* Invoke "flush_partition" to deal with the final (or only) partition */
  sqlite3VdbeAddOp2(v, OP_Gosub, regFlushPart, lblFlushPart);
}

static void windowReturnOneRow(
  Parse *pParse,
  Window *pMWin,
  int regGosub,
  int addrGosub
){
  Vdbe *v = sqlite3GetVdbe(pParse);
  Window *pWin;
  for(pWin=pMWin; pWin; pWin=pWin->pNextWin){
    FuncDef *pFunc = pWin->pFunc;
    if( pFunc->xSFunc==nth_valueStepFunc 
     || pFunc->xSFunc==first_valueStepFunc 
    ){
      int csr = pWin->csrApp;
      int lbl = sqlite3VdbeMakeLabel(v);
      int tmpReg = sqlite3GetTempReg(pParse);
      sqlite3VdbeAddOp2(v, OP_Null, 0, pWin->regResult);

      if( pFunc->xSFunc==nth_valueStepFunc ){
        sqlite3VdbeAddOp3(v, OP_Column, pWin->iEphCsr, pWin->iArgCol+1, tmpReg);
      }else{
        sqlite3VdbeAddOp2(v, OP_Integer, 1, tmpReg);
      }
      sqlite3VdbeAddOp3(v, OP_Add, tmpReg, pWin->regApp, tmpReg);
      sqlite3VdbeAddOp3(v, OP_Gt, pWin->regApp+1, lbl, tmpReg);
      sqlite3VdbeAddOp3(v, OP_SeekRowid, csr, lbl, tmpReg);
      sqlite3VdbeAddOp3(v, OP_Column, csr, pWin->iArgCol, pWin->regResult);
      sqlite3VdbeResolveLabel(v, lbl);
      sqlite3ReleaseTempReg(pParse, tmpReg);
    }
    else if( pFunc->xSFunc==leadStepFunc || pFunc->xSFunc==lagStepFunc ){
      int nArg = pWin->pOwner->x.pList->nExpr;
      int iEph = pWin->iEphCsr;
      int csr = pWin->csrApp;
      int lbl = sqlite3VdbeMakeLabel(v);
      int tmpReg = sqlite3GetTempReg(pParse);

      if( nArg<3 ){
        sqlite3VdbeAddOp2(v, OP_Null, 0, pWin->regResult);
      }else{
        sqlite3VdbeAddOp3(v, OP_Column, iEph, pWin->iArgCol+2, pWin->regResult);
      }
      sqlite3VdbeAddOp2(v, OP_Rowid, iEph, tmpReg);
      if( nArg<2 ){
        int val = (pFunc->xSFunc==leadStepFunc ? 1 : -1);
        sqlite3VdbeAddOp2(v, OP_AddImm, tmpReg, val);
      }else{
        int op = (pFunc->xSFunc==leadStepFunc ? OP_Add : OP_Subtract);
        int tmpReg2 = sqlite3GetTempReg(pParse);
        sqlite3VdbeAddOp3(v, OP_Column, iEph, pWin->iArgCol+1, tmpReg2);
        sqlite3VdbeAddOp3(v, op, tmpReg2, tmpReg, tmpReg);
        sqlite3ReleaseTempReg(pParse, tmpReg2);
      }

      sqlite3VdbeAddOp3(v, OP_SeekRowid, csr, lbl, tmpReg);
      sqlite3VdbeAddOp3(v, OP_Column, csr, pWin->iArgCol, pWin->regResult);
      sqlite3VdbeResolveLabel(v, lbl);
      sqlite3ReleaseTempReg(pParse, tmpReg);
    }
  }
  sqlite3VdbeAddOp2(v, OP_Gosub, regGosub, addrGosub);
}

static void windowReturnRows(
  Parse *pParse,
  Window *pMWin,
  int regCtr,
  int bFinal,
  int regGosub,
  int addrGosub,
  int regInvArg,
  int regInvSize
){
  int addr;
  Vdbe *v = sqlite3GetVdbe(pParse);
  windowAggFinal(pParse, pMWin, 0);
  addr = sqlite3VdbeAddOp3(v, OP_IfPos, regCtr, sqlite3VdbeCurrentAddr(v)+2 ,1);
  sqlite3VdbeAddOp2(v, OP_Goto, 0, 0);
  windowReturnOneRow(pParse, pMWin, regGosub, addrGosub);
  if( regInvArg ){
    windowAggStep(pParse, pMWin, pMWin->iEphCsr, 1, regInvArg, regInvSize);
  }
  sqlite3VdbeAddOp2(v, OP_Next, pMWin->iEphCsr, addr);
  sqlite3VdbeJumpHere(v, addr+1);   /* The OP_Goto */
}

static int windowInitAccum(Parse *pParse, Window *pMWin){
  Vdbe *v = sqlite3GetVdbe(pParse);
  int regArg;
  int nArg = 0;
  Window *pWin;
  for(pWin=pMWin; pWin; pWin=pWin->pNextWin){
    sqlite3VdbeAddOp2(v, OP_Null, 0, pWin->regAccum);
    nArg = MAX(nArg, windowArgCount(pWin));
    if( pWin->pFunc->xSFunc==nth_valueStepFunc
     || pWin->pFunc->xSFunc==first_valueStepFunc 
    ){
      sqlite3VdbeAddOp2(v, OP_Integer, 0, pWin->regApp);
      sqlite3VdbeAddOp2(v, OP_Integer, 0, pWin->regApp+1);
    }
  }
  regArg = pParse->nMem+1;
  pParse->nMem += nArg;
  return regArg;
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
  int nSub = p->pSrc->a[0].pTab->nCol;
  int regFlushPart;               /* Register for "Gosub flush_partition" */
  int lblFlushPart;               /* Label for "Gosub flush_partition" */
  int lblFlushDone;               /* Label for "Gosub flush_partition_done" */

  int regArg;
  int nArg;
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
  int regSize = 0;

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

  /* Allocate register and label for the "flush_partition" sub-routine. */
  regFlushPart = ++pParse->nMem;
  lblFlushPart = sqlite3VdbeMakeLabel(v);
  lblFlushDone = sqlite3VdbeMakeLabel(v);

  regStart = ++pParse->nMem;
  regEnd = ++pParse->nMem;

  windowPartitionCache(pParse, p, pWInfo, regFlushPart, lblFlushPart, &regSize);

  addrGoto = sqlite3VdbeAddOp0(v, OP_Goto);

  /* Start of "flush_partition" */
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
  }

  /* If this is "ROWS <expr1> FOLLOWING AND ROWS <expr2> FOLLOWING", do:
  **
  **   if( regEnd<regStart ){
  **     // The frame always consists of 0 rows
  **     regStart = regSize;
  **   }
  **   regEnd = regEnd - regStart;
  */
  if( pMWin->pEnd && pMWin->pStart && pMWin->eStart==TK_FOLLOWING ){
    assert( pMWin->eEnd==TK_FOLLOWING );
    sqlite3VdbeAddOp3(v, OP_Ge, regStart, sqlite3VdbeCurrentAddr(v)+2, regEnd);
    sqlite3VdbeAddOp2(v, OP_Copy, regSize, regStart);
    sqlite3VdbeAddOp3(v, OP_Subtract, regStart, regEnd, regEnd);
  }

  if( pMWin->pEnd && pMWin->pStart && pMWin->eEnd==TK_PRECEDING ){
    assert( pMWin->eStart==TK_PRECEDING );
    sqlite3VdbeAddOp3(v, OP_Le, regStart, sqlite3VdbeCurrentAddr(v)+3, regEnd);
    sqlite3VdbeAddOp2(v, OP_Copy, regSize, regStart);
    sqlite3VdbeAddOp2(v, OP_Copy, regSize, regEnd);
  }

  /* Initialize the accumulator register for each window function to NULL */
  regArg = windowInitAccum(pParse, pMWin);

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
  windowAggStep(pParse, pMWin, csrEnd, 0, regArg, regSize);
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
  windowReturnOneRow(pParse, pMWin, regGosub, addrGosub);
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
    windowAggStep(pParse, pMWin, csrStart, 1, regArg, regSize);
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
**   flush_partition:
**     Once {
**       OpenDup (iEphCsr -> csrLead)
**     }
**     Integer ctr 0
**     foreach row (csrLead){
**       if( new peer ){
**         AggFinal (xValue)
**         for(i=0; i<ctr; i++){
**           Gosub addrGosub
**           Next iEphCsr
**         }
**         Integer ctr 0
**       }
**       AggStep (csrLead)
**       Incr ctr
**     }
**
**     AggFinal (xFinalize)
**     for(i=0; i<ctr; i++){
**       Gosub addrGosub
**       Next iEphCsr
**     }
**
**     ResetSorter (csr)
**     Return
**
** ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
** RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
** RANGE BETWEEN CURRENT ROW AND CURRENT ROW 
**
**   TODO.
*/
static void windowCodeCacheStep(
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
  int addr;
  ExprList *pPart = pMWin->pPartition;
  ExprList *pOrderBy = pMWin->pOrderBy;
  int nPeer = pOrderBy->nExpr;
  int regNewPeer;

  int addrGoto;                   /* Address of Goto used to jump flush_par.. */
  int addrRewind;                 /* Address of Rewind that starts loop */
  int regFlushPart;
  int lblFlushPart;
  int csrLead;
  int regCtr;
  int regArg;                     /* Register array to martial function args */
  int regSize;
  int nArg;

  assert( (pMWin->eStart==TK_UNBOUNDED && pMWin->eEnd==TK_CURRENT) 
       || (pMWin->eStart==TK_UNBOUNDED && pMWin->eEnd==TK_UNBOUNDED) 
       || (pMWin->eStart==TK_CURRENT && pMWin->eEnd==TK_CURRENT) 
  );

  regNewPeer = pParse->nMem+1;
  pParse->nMem += nPeer;

  /* Allocate register and label for the "flush_partition" sub-routine. */
  regFlushPart = ++pParse->nMem;
  lblFlushPart = sqlite3VdbeMakeLabel(v);

  csrLead = pParse->nTab++;
  regCtr = ++pParse->nMem;

  windowPartitionCache(pParse, p, pWInfo, regFlushPart, lblFlushPart, &regSize);
  addrGoto = sqlite3VdbeAddOp0(v, OP_Goto);

  /* Start of "flush_partition" */
  sqlite3VdbeResolveLabel(v, lblFlushPart);
  sqlite3VdbeAddOp2(v, OP_Once, 0, sqlite3VdbeCurrentAddr(v)+2);
  sqlite3VdbeAddOp2(v, OP_OpenDup, csrLead, pMWin->iEphCsr);

  /* Initialize the accumulator register for each window function to NULL */
  regArg = windowInitAccum(pParse, pMWin);

  sqlite3VdbeAddOp2(v, OP_Integer, 0, regCtr);
  addrRewind = sqlite3VdbeAddOp1(v, OP_Rewind, csrLead);
  sqlite3VdbeAddOp1(v, OP_Rewind, pMWin->iEphCsr);

  if( pOrderBy && pMWin->eEnd==TK_CURRENT ){
    int bCurrent = (pMWin->eEnd==TK_CURRENT && pMWin->eStart==TK_CURRENT);
    int addrJump = 0;             /* Address of OP_Jump below */
    if( pMWin->eType==TK_RANGE ){
      int iOff = pMWin->nBufferCol + (pPart ? pPart->nExpr : 0);
      int regPeer = pMWin->regPart + (pPart ? pPart->nExpr : 0);
      KeyInfo *pKeyInfo = sqlite3KeyInfoFromExprList(pParse, pOrderBy, 0, 0);
      for(k=0; k<nPeer; k++){
        sqlite3VdbeAddOp3(v, OP_Column, csrLead, iOff+k, regNewPeer+k);
      }
      addr = sqlite3VdbeAddOp3(v, OP_Compare, regNewPeer, regPeer, nPeer);
      sqlite3VdbeAppendP4(v, (void*)pKeyInfo, P4_KEYINFO);
      addrJump = sqlite3VdbeAddOp3(v, OP_Jump, addr+2, 0, addr+2);
      sqlite3VdbeAddOp3(v, OP_Copy, regNewPeer, regPeer, nPeer-1);
    }

    windowReturnRows(pParse, pMWin, regCtr, 0, regGosub, addrGosub, 
        (bCurrent ? regArg : 0), (bCurrent ? regSize : 0)
    );
    if( addrJump ) sqlite3VdbeJumpHere(v, addrJump);
  }

  windowAggStep(pParse, pMWin, csrLead, 0, regArg, regSize);
  sqlite3VdbeAddOp2(v, OP_AddImm, regCtr, 1);
  sqlite3VdbeAddOp2(v, OP_Next, csrLead, addrRewind+2);

  windowReturnRows(pParse, pMWin, regCtr, 1, regGosub, addrGosub, 0, 0);

  sqlite3VdbeJumpHere(v, addrRewind);
  sqlite3VdbeJumpHere(v, addrRewind+1);
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
** ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
** 
**   As above, except assume every row is a "new peer".
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

    sqlite3VdbeAddOp2(v, OP_Rewind, pMWin->iEphCsr,sqlite3VdbeCurrentAddr(v)+3);
    sqlite3VdbeAddOp2(v, OP_Gosub, regGosub, addrGosub);
    sqlite3VdbeAddOp2(v, OP_Next, pMWin->iEphCsr, sqlite3VdbeCurrentAddr(v)-1);

    sqlite3VdbeAddOp1(v, OP_ResetSorter, pMWin->iEphCsr);
    sqlite3VdbeAddOp3(
        v, OP_Copy, reg+pMWin->nBufferCol, pMWin->regPart, nPart+nPeer-1
    );

    if( addrJump ) sqlite3VdbeJumpHere(v, addrJump);
  }

  /* Invoke step function for window functions */
  windowAggStep(pParse, pMWin, -1, 0, reg, 0);

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
  sqlite3VdbeAddOp2(v, OP_Rewind, pMWin->iEphCsr,sqlite3VdbeCurrentAddr(v)+3);
  sqlite3VdbeAddOp2(v, OP_Gosub, regGosub, addrGosub);
  sqlite3VdbeAddOp2(v, OP_Next, pMWin->iEphCsr, sqlite3VdbeCurrentAddr(v)-1);
}

Window *sqlite3WindowDup(sqlite3 *db, Expr *pOwner, Window *p){
  Window *pNew = 0;
  if( p ){
    pNew = sqlite3DbMallocZero(db, sizeof(Window));
    if( pNew ){
      pNew->pFilter = sqlite3ExprDup(db, p->pFilter, 0);
      pNew->pPartition = sqlite3ExprListDup(db, p->pPartition, 0);
      pNew->pOrderBy = sqlite3ExprListDup(db, p->pOrderBy, 0);
      pNew->eType = p->eType;
      pNew->eEnd = p->eEnd;
      pNew->eStart = p->eStart;
      pNew->pStart = sqlite3ExprDup(db, pNew->pStart, 0);
      pNew->pEnd = sqlite3ExprDup(db, pNew->pEnd, 0);
      pNew->pOwner = pOwner;
    }
  }
  return pNew;
}

/*
** sqlite3WhereBegin() has already been called for the SELECT statement 
** passed as the second argument when this function is invoked. It generates
** code to populate the Window.regResult register for each window function and
** invoke the sub-routine at instruction addrGosub once for each row.
** This function calls sqlite3WhereEnd() before returning. 
*/
void sqlite3WindowCodeStep(
  Parse *pParse,                  /* Parse context */
  Select *p,                      /* Rewritten SELECT statement */
  WhereInfo *pWInfo,              /* Context returned by sqlite3WhereBegin() */
  int regGosub,                   /* Register for OP_Gosub */
  int addrGosub                   /* OP_Gosub here to return each row */
){
  Window *pMWin = p->pWin;
  Window *pWin;

  /* Call windowCodeRowExprStep() for all window modes *except*:
  **
  **   RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  **   RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  **   RANGE BETWEEN CURRENT ROW AND CURRENT ROW
  **   ROWS  BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  */
  if( (pMWin->eType==TK_ROWS 
   && (pMWin->eStart!=TK_UNBOUNDED||pMWin->eEnd!=TK_CURRENT||!pMWin->pOrderBy))
   || (pMWin->eStart==TK_CURRENT&&pMWin->eEnd==TK_UNBOUNDED&&pMWin->pOrderBy)
  ){
    windowCodeRowExprStep(pParse, p, pWInfo, regGosub, addrGosub);
    return;
  }

  /* Call windowCodeCacheStep() if there is a window function that requires
  ** that the entire partition be cached in a temp table before any rows
  ** are returned.  */
  for(pWin=pMWin; pWin; pWin=pWin->pNextWin){
    FuncDef *pFunc = pWin->pFunc;
    if( (pFunc->funcFlags & SQLITE_FUNC_WINDOW_SIZE)
     || (pFunc->xSFunc==nth_valueStepFunc)
     || (pFunc->xSFunc==first_valueStepFunc)
     || (pFunc->xSFunc==leadStepFunc)
     || (pFunc->xSFunc==lagStepFunc)
    ){
      windowCodeCacheStep(pParse, p, pWInfo, regGosub, addrGosub);
      return;
    }
  }

  /* Otherwise, call windowCodeDefaultStep().  */
  windowCodeDefaultStep(pParse, p, pWInfo, regGosub, addrGosub);
}

