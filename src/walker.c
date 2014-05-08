/*
** 2008 August 16
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains routines used for walking the parser tree for
** an SQL statement.
*/
#include "sqliteInt.h"
#include <stdlib.h>
#include <string.h>


/*
** Walk an expression tree.  Invoke the callback once for each node
** of the expression, while decending.  (In other words, the callback
** is invoked before visiting children.)
**
** The return value from the callback should be one of the WRC_*
** constants to specify how to proceed with the walk.
**
**    WRC_Continue      Continue descending down the tree.
**
**    WRC_Prune         Do not descend into child nodes.  But allow
**                      the walk to continue with sibling nodes.
**
**    WRC_Abort         Do no more callbacks.  Unwind the stack and
**                      return the top-level walk call.
**
** The return value from this routine is WRC_Abort to abandon the tree walk
** and WRC_Continue to continue.
*/
int sqlite3WalkExpr(Walker *pWalker, Expr *pExpr){
  int rc;
  if( pExpr==0 ) return WRC_Continue;
  testcase( ExprHasProperty(pExpr, EP_TokenOnly) );
  testcase( ExprHasProperty(pExpr, EP_Reduced) );
  rc = pWalker->xExprCallback(pWalker, pExpr);
  if( rc==WRC_Continue
              && !ExprHasProperty(pExpr,EP_TokenOnly) ){
    if( sqlite3WalkExpr(pWalker, pExpr->pLeft) ) return WRC_Abort;
    if( sqlite3WalkExpr(pWalker, pExpr->pRight) ) return WRC_Abort;
    if( ExprHasProperty(pExpr, EP_xIsSelect) ){
      if( sqlite3WalkSelect(pWalker, pExpr->x.pSelect) ) return WRC_Abort;
    }else{
      if( sqlite3WalkExprList(pWalker, pExpr->x.pList) ) return WRC_Abort;
    }
  }
  return rc & WRC_Abort;
}

/*
** Call sqlite3WalkExpr() for every expression in list p or until
** an abort request is seen.
*/
int sqlite3WalkExprList(Walker *pWalker, ExprList *p){
  int i;
  struct ExprList_item *pItem;
  if( p ){
    for(i=p->nExpr, pItem=p->a; i>0; i--, pItem++){
      if( sqlite3WalkExpr(pWalker, pItem->pExpr) ) return WRC_Abort;
    }
  }
  return WRC_Continue;
}

/*
** Walk all expressions associated with SELECT statement p.  Do
** not invoke the SELECT callback on p, but do (of course) invoke
** any expr callbacks and SELECT callbacks that come from subqueries.
** Return WRC_Abort or WRC_Continue.
*/
int sqlite3WalkSelectExpr(Walker *pWalker, Select *p){
  if( sqlite3WalkExprList(pWalker, p->pEList) ) return WRC_Abort;
  if( sqlite3WalkExpr(pWalker, p->pWhere) ) return WRC_Abort;
  if( sqlite3WalkExprList(pWalker, p->pGroupBy) ) return WRC_Abort;
  if( sqlite3WalkExpr(pWalker, p->pHaving) ) return WRC_Abort;
  if( sqlite3WalkExprList(pWalker, p->pOrderBy) ) return WRC_Abort;
  if( sqlite3WalkExpr(pWalker, p->pLimit) ) return WRC_Abort;
  if( sqlite3WalkExpr(pWalker, p->pOffset) ) return WRC_Abort;
  return WRC_Continue;
}

/*
** Walk the parse trees associated with all subqueries in the
** FROM clause of SELECT statement p.  Do not invoke the select
** callback on p, but do invoke it on each FROM clause subquery
** and on any subqueries further down in the tree.  Return 
** WRC_Abort or WRC_Continue;
*/
int sqlite3WalkSelectFrom(Walker *pWalker, Select *p){
  SrcList *pSrc;
  int i;
  struct SrcList_item *pItem;

  pSrc = p->pSrc;
  if( ALWAYS(pSrc) ){
    for(i=pSrc->nSrc, pItem=pSrc->a; i>0; i--, pItem++){
      if( sqlite3WalkSelect(pWalker, pItem->pSelect) ){
        return WRC_Abort;
      }
    }
  }
  return WRC_Continue;
} 

/*
** Call sqlite3WalkExpr() for every expression in Select statement p.
** Invoke sqlite3WalkSelect() for subqueries in the FROM clause and
** on the compound select chain, p->pPrior. 
**
** If it is not NULL, the xSelectCallback() callback is invoked before
** the walk of the expressions and FROM clause. The xSelectCallback2()
** method, if it is not NULL, is invoked following the walk of the 
** expressions and FROM clause.
**
** Return WRC_Continue under normal conditions.  Return WRC_Abort if
** there is an abort request.
**
** If the Walker does not have an xSelectCallback() then this routine
** is a no-op returning WRC_Continue.
*/
int sqlite3WalkSelect(Walker *pWalker, Select *p){
  int rc;
  if( p==0 || (pWalker->xSelectCallback==0 && pWalker->xSelectCallback2==0) ){
    return WRC_Continue;
  }
  rc = WRC_Continue;
  pWalker->walkerDepth++;
  while( p ){
    if( pWalker->xSelectCallback ){
       rc = pWalker->xSelectCallback(pWalker, p);
       if( rc ) break;
    }
    if( sqlite3WalkSelectExpr(pWalker, p)
     || sqlite3WalkSelectFrom(pWalker, p)
    ){
      pWalker->walkerDepth--;
      return WRC_Abort;
    }
    if( pWalker->xSelectCallback2 ){
      pWalker->xSelectCallback2(pWalker, p);
    }
    p = p->pPrior;
  }
  pWalker->walkerDepth--;
  return rc & WRC_Abort;
}
