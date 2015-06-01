/*
** 2015-06-01
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
** This file contains C code used to transform FROM-clause subqueries
** in ways that result in faster query evaluation.
*/
#include "sqliteInt.h"

#if !defined(SQLITE_OMIT_SUBQUERY) || !defined(SQLITE_OMIT_VIEW)
/* Forward Declarations */
static void substExprList(sqlite3*, ExprList*, int, ExprList*);
static void substSelect(sqlite3*, Select *, int, ExprList *);

/*
** Scan through the expression pExpr.  Replace every reference to
** a column in table number iTable with a copy of the iColumn-th
** entry in pEList.  (But leave references to the ROWID column 
** unchanged.)
**
** This routine is part of the flattening procedure.  A subquery
** whose result set is defined by pEList appears as entry in the
** FROM clause of a SELECT such that the VDBE cursor assigned to that
** FORM clause entry is iTable.  This routine make the necessary 
** changes to pExpr so that it refers directly to the source table
** of the subquery rather the result set of the subquery.
*/
static Expr *substExpr(
  sqlite3 *db,        /* Report malloc errors to this connection */
  Expr *pExpr,        /* Expr in which substitution occurs */
  int iTable,         /* Table to be substituted */
  ExprList *pEList    /* Substitute expressions */
){
  if( pExpr==0 ) return 0;
  if( pExpr->op==TK_COLUMN && pExpr->iTable==iTable ){
    if( pExpr->iColumn<0 ){
      pExpr->op = TK_NULL;
    }else{
      Expr *pNew;
      assert( pEList!=0 && pExpr->iColumn<pEList->nExpr );
      assert( pExpr->pLeft==0 && pExpr->pRight==0 );
      pNew = sqlite3ExprDup(db, pEList->a[pExpr->iColumn].pExpr, 0);
      sqlite3ExprDelete(db, pExpr);
      pExpr = pNew;
    }
  }else{
    pExpr->pLeft = substExpr(db, pExpr->pLeft, iTable, pEList);
    pExpr->pRight = substExpr(db, pExpr->pRight, iTable, pEList);
    if( ExprHasProperty(pExpr, EP_xIsSelect) ){
      substSelect(db, pExpr->x.pSelect, iTable, pEList);
    }else{
      substExprList(db, pExpr->x.pList, iTable, pEList);
    }
  }
  return pExpr;
}
static void substExprList(
  sqlite3 *db,         /* Report malloc errors here */
  ExprList *pList,     /* List to scan and in which to make substitutes */
  int iTable,          /* Table to be substituted */
  ExprList *pEList     /* Substitute values */
){
  int i;
  if( pList==0 ) return;
  for(i=0; i<pList->nExpr; i++){
    pList->a[i].pExpr = substExpr(db, pList->a[i].pExpr, iTable, pEList);
  }
}
static void substSelect(
  sqlite3 *db,         /* Report malloc errors here */
  Select *p,           /* SELECT statement in which to make substitutions */
  int iTable,          /* Table to be replaced */
  ExprList *pEList     /* Substitute values */
){
  SrcList *pSrc;
  struct SrcList_item *pItem;
  int i;
  if( !p ) return;
  substExprList(db, p->pEList, iTable, pEList);
  substExprList(db, p->pGroupBy, iTable, pEList);
  substExprList(db, p->pOrderBy, iTable, pEList);
  p->pHaving = substExpr(db, p->pHaving, iTable, pEList);
  p->pWhere = substExpr(db, p->pWhere, iTable, pEList);
  substSelect(db, p->pPrior, iTable, pEList);
  pSrc = p->pSrc;
  assert( pSrc );  /* Even for (SELECT 1) we have: pSrc!=0 but pSrc->nSrc==0 */
  if( ALWAYS(pSrc) ){
    for(i=pSrc->nSrc, pItem=pSrc->a; i>0; i--, pItem++){
      substSelect(db, pItem->pSelect, iTable, pEList);
    }
  }
}

/*
** This routine attempts to flatten subqueries as a performance optimization.
** This routine returns 1 if it makes changes and 0 if no flattening occurs.
**
** To understand the concept of flattening, consider the following
** query:
**
**     SELECT a FROM (SELECT x+y AS a FROM t1 WHERE z<100) WHERE a>5
**
** The default way of implementing this query is to execute the
** subquery first and store the results in a temporary table, then
** run the outer query on that temporary table.  This requires two
** passes over the data.  Furthermore, because the temporary table
** has no indices, the WHERE clause on the outer query cannot be
** optimized.
**
** This routine attempts to rewrite queries such as the above into
** a single flat select, like this:
**
**     SELECT x+y AS a FROM t1 WHERE z<100 AND a>5
**
** The code generated for this simplification gives the same result
** but only has to scan the data once.  And because indices might 
** exist on the table t1, a complete scan of the data might be
** avoided.
**
** Flattening is only attempted if all of the following are true:
**
**   (1)  The subquery and the outer query do not both use aggregates.
**
**   (2)  The subquery is not an aggregate or (2a) the outer query is not a join
**        and (2b) the outer query does not use subqueries other than the one
**        FROM-clause subquery that is a candidate for flattening.  (2b is
**        due to ticket [2f7170d73bf9abf80] from 2015-02-09.)
**
**   (3)  The subquery is not the right operand of a left outer join
**        (Originally ticket #306.  Strengthened by ticket #3300)
**
**   (4)  The subquery is not DISTINCT.
**
**  (**)  At one point restrictions (4) and (5) defined a subset of DISTINCT
**        sub-queries that were excluded from this optimization. Restriction 
**        (4) has since been expanded to exclude all DISTINCT subqueries.
**
**   (6)  The subquery does not use aggregates or the outer query is not
**        DISTINCT.
**
**   (7)  The subquery has a FROM clause.  TODO:  For subqueries without
**        A FROM clause, consider adding a FROM close with the special
**        table sqlite_once that consists of a single row containing a
**        single NULL.
**
**   (8)  The subquery does not use LIMIT or the outer query is not a join.
**
**   (9)  The subquery does not use LIMIT or the outer query does not use
**        aggregates.
**
**  (**)  Restriction (10) was removed from the code on 2005-02-05 but we
**        accidently carried the comment forward until 2014-09-15.  Original
**        text: "The subquery does not use aggregates or the outer query does not
**        use LIMIT."
**
**  (11)  The subquery and the outer query do not both have ORDER BY clauses.
**
**  (**)  Not implemented.  Subsumed into restriction (3).  Was previously
**        a separate restriction deriving from ticket #350.
**
**  (13)  The subquery and outer query do not both use LIMIT.
**
**  (14)  The subquery does not use OFFSET.
**
**  (15)  The outer query is not part of a compound select or the
**        subquery does not have a LIMIT clause.
**        (See ticket #2339 and ticket [02a8e81d44]).
**
**  (16)  The outer query is not an aggregate or the subquery does
**        not contain ORDER BY.  (Ticket #2942)  This used to not matter
**        until we introduced the group_concat() function.  
**
**  (17)  The sub-query is not a compound select, or it is a UNION ALL 
**        compound clause made up entirely of non-aggregate queries, and 
**        the parent query:
**
**          * is not itself part of a compound select,
**          * is not an aggregate or DISTINCT query, and
**          * is not a join
**
**        The parent and sub-query may contain WHERE clauses. Subject to
**        rules (11), (13) and (14), they may also contain ORDER BY,
**        LIMIT and OFFSET clauses.  The subquery cannot use any compound
**        operator other than UNION ALL because all the other compound
**        operators have an implied DISTINCT which is disallowed by
**        restriction (4).
**
**        Also, each component of the sub-query must return the same number
**        of result columns. This is actually a requirement for any compound
**        SELECT statement, but all the code here does is make sure that no
**        such (illegal) sub-query is flattened. The caller will detect the
**        syntax error and return a detailed message.
**
**  (18)  If the sub-query is a compound select, then all terms of the
**        ORDER by clause of the parent must be simple references to 
**        columns of the sub-query.
**
**  (19)  The subquery does not use LIMIT or the outer query does not
**        have a WHERE clause.
**
**  (20)  If the sub-query is a compound select, then it must not use
**        an ORDER BY clause.  Ticket #3773.  We could relax this constraint
**        somewhat by saying that the terms of the ORDER BY clause must
**        appear as unmodified result columns in the outer query.  But we
**        have other optimizations in mind to deal with that case.
**
**  (21)  The subquery does not use LIMIT or the outer query is not
**        DISTINCT.  (See ticket [752e1646fc]).
**
**  (22)  The subquery is not a recursive CTE.
**
**  (23)  The parent is not a recursive CTE, or the sub-query is not a
**        compound query. This restriction is because transforming the
**        parent to a compound query confuses the code that handles
**        recursive queries in multiSelect().
**
**  (24)  The subquery is not an aggregate that uses the built-in min() or 
**        or max() functions.  (Without this restriction, a query like:
**        "SELECT x FROM (SELECT max(y), x FROM t1)" would not necessarily
**        return the value X for which Y was maximal.)
**
**
** In this routine, the "p" parameter is a pointer to the outer query.
** The subquery is p->pSrc->a[iFrom].  isAgg is true if the outer query
** uses aggregates and subqueryIsAgg is true if the subquery uses aggregates.
**
** If flattening is not attempted, this routine is a no-op and returns 0.
** If flattening is attempted this routine returns 1.
**
** All of the expression analysis must occur on both the outer query and
** the subquery before this routine runs.
*/
int sqlite3FlattenSubquery(
  Parse *pParse,       /* Parsing context */
  Select *p,           /* The parent or outer SELECT statement */
  int iFrom,           /* Index in p->pSrc->a[] of the inner subquery */
  int isAgg,           /* True if outer SELECT uses aggregate functions */
  int subqueryIsAgg    /* True if the subquery uses aggregate functions */
){
  const char *zSavedAuthContext = pParse->zAuthContext;
  Select *pParent;
  Select *pSub;       /* The inner query or "subquery" */
  Select *pSub1;      /* Pointer to the rightmost select in sub-query */
  SrcList *pSrc;      /* The FROM clause of the outer query */
  SrcList *pSubSrc;   /* The FROM clause of the subquery */
  ExprList *pList;    /* The result set of the outer query */
  int iParent;        /* VDBE cursor number of the pSub result set temp table */
  int i;              /* Loop counter */
  Expr *pWhere;                    /* The WHERE clause */
  struct SrcList_item *pSubitem;   /* The subquery */
  sqlite3 *db = pParse->db;

  /* Check to see if flattening is permitted.  Return 0 if not.
  */
  assert( p!=0 );
  assert( p->pPrior==0 );  /* Unable to flatten compound queries */
  if( OptimizationDisabled(db, SQLITE_QueryFlattener) ) return 0;
  pSrc = p->pSrc;
  assert( pSrc && iFrom>=0 && iFrom<pSrc->nSrc );
  pSubitem = &pSrc->a[iFrom];
  iParent = pSubitem->iCursor;
  pSub = pSubitem->pSelect;
  assert( pSub!=0 );
  if( subqueryIsAgg ){
    if( isAgg ) return 0;                                /* Restriction (1)   */
    if( pSrc->nSrc>1 ) return 0;                         /* Restriction (2a)  */
    if( (p->pWhere && ExprHasProperty(p->pWhere,EP_Subquery))
     || (sqlite3ExprListFlags(p->pEList) & EP_Subquery)!=0
     || (sqlite3ExprListFlags(p->pOrderBy) & EP_Subquery)!=0
    ){
      return 0;                                          /* Restriction (2b)  */
    }
  }
    
  pSubSrc = pSub->pSrc;
  assert( pSubSrc );
  /* Prior to version 3.1.2, when LIMIT and OFFSET had to be simple constants,
  ** not arbitrary expressions, we allowed some combining of LIMIT and OFFSET
  ** because they could be computed at compile-time.  But when LIMIT and OFFSET
  ** became arbitrary expressions, we were forced to add restrictions (13)
  ** and (14). */
  if( pSub->pLimit && p->pLimit ) return 0;              /* Restriction (13) */
  if( pSub->pOffset ) return 0;                          /* Restriction (14) */
  if( (p->selFlags & SF_Compound)!=0 && pSub->pLimit ){
    return 0;                                            /* Restriction (15) */
  }
  if( pSubSrc->nSrc==0 ) return 0;                       /* Restriction (7)  */
  if( pSub->selFlags & SF_Distinct ) return 0;           /* Restriction (5)  */
  if( pSub->pLimit && (pSrc->nSrc>1 || isAgg) ){
     return 0;         /* Restrictions (8)(9) */
  }
  if( (p->selFlags & SF_Distinct)!=0 && subqueryIsAgg ){
     return 0;         /* Restriction (6)  */
  }
  if( p->pOrderBy && pSub->pOrderBy ){
     return 0;                                           /* Restriction (11) */
  }
  if( isAgg && pSub->pOrderBy ) return 0;                /* Restriction (16) */
  if( pSub->pLimit && p->pWhere ) return 0;              /* Restriction (19) */
  if( pSub->pLimit && (p->selFlags & SF_Distinct)!=0 ){
     return 0;         /* Restriction (21) */
  }
  testcase( pSub->selFlags & SF_Recursive );
  testcase( pSub->selFlags & SF_MinMaxAgg );
  if( pSub->selFlags & (SF_Recursive|SF_MinMaxAgg) ){
    return 0; /* Restrictions (22) and (24) */
  }
  if( (p->selFlags & SF_Recursive) && pSub->pPrior ){
    return 0; /* Restriction (23) */
  }

  /* OBSOLETE COMMENT 1:
  ** Restriction 3:  If the subquery is a join, make sure the subquery is 
  ** not used as the right operand of an outer join.  Examples of why this
  ** is not allowed:
  **
  **         t1 LEFT OUTER JOIN (t2 JOIN t3)
  **
  ** If we flatten the above, we would get
  **
  **         (t1 LEFT OUTER JOIN t2) JOIN t3
  **
  ** which is not at all the same thing.
  **
  ** OBSOLETE COMMENT 2:
  ** Restriction 12:  If the subquery is the right operand of a left outer
  ** join, make sure the subquery has no WHERE clause.
  ** An examples of why this is not allowed:
  **
  **         t1 LEFT OUTER JOIN (SELECT * FROM t2 WHERE t2.x>0)
  **
  ** If we flatten the above, we would get
  **
  **         (t1 LEFT OUTER JOIN t2) WHERE t2.x>0
  **
  ** But the t2.x>0 test will always fail on a NULL row of t2, which
  ** effectively converts the OUTER JOIN into an INNER JOIN.
  **
  ** THIS OVERRIDES OBSOLETE COMMENTS 1 AND 2 ABOVE:
  ** Ticket #3300 shows that flattening the right term of a LEFT JOIN
  ** is fraught with danger.  Best to avoid the whole thing.  If the
  ** subquery is the right term of a LEFT JOIN, then do not flatten.
  */
  if( (pSubitem->jointype & JT_OUTER)!=0 ){
    return 0;
  }

  /* Restriction 17: If the sub-query is a compound SELECT, then it must
  ** use only the UNION ALL operator. And none of the simple select queries
  ** that make up the compound SELECT are allowed to be aggregate or distinct
  ** queries.
  */
  if( pSub->pPrior ){
    if( pSub->pOrderBy ){
      return 0;  /* Restriction 20 */
    }
    if( isAgg || (p->selFlags & SF_Distinct)!=0 || pSrc->nSrc!=1 ){
      return 0;
    }
    for(pSub1=pSub; pSub1; pSub1=pSub1->pPrior){
      testcase( (pSub1->selFlags & (SF_Distinct|SF_Aggregate))==SF_Distinct );
      testcase( (pSub1->selFlags & (SF_Distinct|SF_Aggregate))==SF_Aggregate );
      assert( pSub->pSrc!=0 );
      if( (pSub1->selFlags & (SF_Distinct|SF_Aggregate))!=0
       || (pSub1->pPrior && pSub1->op!=TK_ALL) 
       || pSub1->pSrc->nSrc<1
       || pSub->pEList->nExpr!=pSub1->pEList->nExpr
      ){
        return 0;
      }
      testcase( pSub1->pSrc->nSrc>1 );
    }

    /* Restriction 18. */
    if( p->pOrderBy ){
      int ii;
      for(ii=0; ii<p->pOrderBy->nExpr; ii++){
        if( p->pOrderBy->a[ii].u.x.iOrderByCol==0 ) return 0;
      }
    }
  }

  /***** If we reach this point, flattening is permitted. *****/
  SELECTTRACE(1,pParse,p,("flatten %s.%p from term %d\n",
                   pSub->zSelName, pSub, iFrom));

  /* Authorize the subquery */
  pParse->zAuthContext = pSubitem->zName;
  TESTONLY(i =) sqlite3AuthCheck(pParse, SQLITE_SELECT, 0, 0, 0);
  testcase( i==SQLITE_DENY );
  pParse->zAuthContext = zSavedAuthContext;

  /* If the sub-query is a compound SELECT statement, then (by restrictions
  ** 17 and 18 above) it must be a UNION ALL and the parent query must 
  ** be of the form:
  **
  **     SELECT <expr-list> FROM (<sub-query>) <where-clause> 
  **
  ** followed by any ORDER BY, LIMIT and/or OFFSET clauses. This block
  ** creates N-1 copies of the parent query without any ORDER BY, LIMIT or 
  ** OFFSET clauses and joins them to the left-hand-side of the original
  ** using UNION ALL operators. In this case N is the number of simple
  ** select statements in the compound sub-query.
  **
  ** Example:
  **
  **     SELECT a+1 FROM (
  **        SELECT x FROM tab
  **        UNION ALL
  **        SELECT y FROM tab
  **        UNION ALL
  **        SELECT abs(z*2) FROM tab2
  **     ) WHERE a!=5 ORDER BY 1
  **
  ** Transformed into:
  **
  **     SELECT x+1 FROM tab WHERE x+1!=5
  **     UNION ALL
  **     SELECT y+1 FROM tab WHERE y+1!=5
  **     UNION ALL
  **     SELECT abs(z*2)+1 FROM tab2 WHERE abs(z*2)+1!=5
  **     ORDER BY 1
  **
  ** We call this the "compound-subquery flattening".
  */
  for(pSub=pSub->pPrior; pSub; pSub=pSub->pPrior){
    Select *pNew;
    ExprList *pOrderBy = p->pOrderBy;
    Expr *pLimit = p->pLimit;
    Expr *pOffset = p->pOffset;
    Select *pPrior = p->pPrior;
    p->pOrderBy = 0;
    p->pSrc = 0;
    p->pPrior = 0;
    p->pLimit = 0;
    p->pOffset = 0;
    pNew = sqlite3SelectDup(db, p, 0);
    sqlite3SelectSetName(pNew, pSub->zSelName);
    p->pOffset = pOffset;
    p->pLimit = pLimit;
    p->pOrderBy = pOrderBy;
    p->pSrc = pSrc;
    p->op = TK_ALL;
    if( pNew==0 ){
      p->pPrior = pPrior;
    }else{
      pNew->pPrior = pPrior;
      if( pPrior ) pPrior->pNext = pNew;
      pNew->pNext = p;
      p->pPrior = pNew;
      SELECTTRACE(2,pParse,p,
         ("compound-subquery flattener creates %s.%p as peer\n",
         pNew->zSelName, pNew));
    }
    if( db->mallocFailed ) return 1;
  }

  /* Begin flattening the iFrom-th entry of the FROM clause 
  ** in the outer query.
  */
  pSub = pSub1 = pSubitem->pSelect;

  /* Delete the transient table structure associated with the
  ** subquery
  */
  sqlite3DbFree(db, pSubitem->zDatabase);
  sqlite3DbFree(db, pSubitem->zName);
  sqlite3DbFree(db, pSubitem->zAlias);
  pSubitem->zDatabase = 0;
  pSubitem->zName = 0;
  pSubitem->zAlias = 0;
  pSubitem->pSelect = 0;

  /* Defer deleting the Table object associated with the
  ** subquery until code generation is
  ** complete, since there may still exist Expr.pTab entries that
  ** refer to the subquery even after flattening.  Ticket #3346.
  **
  ** pSubitem->pTab is always non-NULL by test restrictions and tests above.
  */
  if( ALWAYS(pSubitem->pTab!=0) ){
    Table *pTabToDel = pSubitem->pTab;
    if( pTabToDel->nRef==1 ){
      Parse *pToplevel = sqlite3ParseToplevel(pParse);
      pTabToDel->pNextZombie = pToplevel->pZombieTab;
      pToplevel->pZombieTab = pTabToDel;
    }else{
      pTabToDel->nRef--;
    }
    pSubitem->pTab = 0;
  }

  /* The following loop runs once for each term in a compound-subquery
  ** flattening (as described above).  If we are doing a different kind
  ** of flattening - a flattening other than a compound-subquery flattening -
  ** then this loop only runs once.
  **
  ** This loop moves all of the FROM elements of the subquery into the
  ** the FROM clause of the outer query.  Before doing this, remember
  ** the cursor number for the original outer query FROM element in
  ** iParent.  The iParent cursor will never be used.  Subsequent code
  ** will scan expressions looking for iParent references and replace
  ** those references with expressions that resolve to the subquery FROM
  ** elements we are now copying in.
  */
  for(pParent=p; pParent; pParent=pParent->pPrior, pSub=pSub->pPrior){
    int nSubSrc;
    u8 jointype = 0;
    pSubSrc = pSub->pSrc;     /* FROM clause of subquery */
    nSubSrc = pSubSrc->nSrc;  /* Number of terms in subquery FROM clause */
    pSrc = pParent->pSrc;     /* FROM clause of the outer query */

    if( pSrc ){
      assert( pParent==p );  /* First time through the loop */
      jointype = pSubitem->jointype;
    }else{
      assert( pParent!=p );  /* 2nd and subsequent times through the loop */
      pSrc = pParent->pSrc = sqlite3SrcListAppend(db, 0, 0, 0);
      if( pSrc==0 ){
        assert( db->mallocFailed );
        break;
      }
    }

    /* The subquery uses a single slot of the FROM clause of the outer
    ** query.  If the subquery has more than one element in its FROM clause,
    ** then expand the outer query to make space for it to hold all elements
    ** of the subquery.
    **
    ** Example:
    **
    **    SELECT * FROM tabA, (SELECT * FROM sub1, sub2), tabB;
    **
    ** The outer query has 3 slots in its FROM clause.  One slot of the
    ** outer query (the middle slot) is used by the subquery.  The next
    ** block of code will expand the out query to 4 slots.  The middle
    ** slot is expanded to two slots in order to make space for the
    ** two elements in the FROM clause of the subquery.
    */
    if( nSubSrc>1 ){
      pParent->pSrc = pSrc = sqlite3SrcListEnlarge(db, pSrc, nSubSrc-1,iFrom+1);
      if( db->mallocFailed ){
        break;
      }
    }

    /* Transfer the FROM clause terms from the subquery into the
    ** outer query.
    */
    for(i=0; i<nSubSrc; i++){
      sqlite3IdListDelete(db, pSrc->a[i+iFrom].pUsing);
      pSrc->a[i+iFrom] = pSubSrc->a[i];
      memset(&pSubSrc->a[i], 0, sizeof(pSubSrc->a[i]));
    }
    pSrc->a[iFrom].jointype = jointype;
  
    /* Now begin substituting subquery result set expressions for 
    ** references to the iParent in the outer query.
    ** 
    ** Example:
    **
    **   SELECT a+5, b*10 FROM (SELECT x*3 AS a, y+10 AS b FROM t1) WHERE a>b;
    **   \                     \_____________ subquery __________/          /
    **    \_____________________ outer query ______________________________/
    **
    ** We look at every expression in the outer query and every place we see
    ** "a" we substitute "x*3" and every place we see "b" we substitute "y+10".
    */
    pList = pParent->pEList;
    for(i=0; i<pList->nExpr; i++){
      if( pList->a[i].zName==0 ){
        char *zName = sqlite3DbStrDup(db, pList->a[i].zSpan);
        sqlite3Dequote(zName);
        pList->a[i].zName = zName;
      }
    }
    substExprList(db, pParent->pEList, iParent, pSub->pEList);
    if( isAgg ){
      substExprList(db, pParent->pGroupBy, iParent, pSub->pEList);
      pParent->pHaving = substExpr(db, pParent->pHaving, iParent, pSub->pEList);
    }
    if( pSub->pOrderBy ){
      /* At this point, any non-zero iOrderByCol values indicate that the
      ** ORDER BY column expression is identical to the iOrderByCol'th
      ** expression returned by SELECT statement pSub. Since these values
      ** do not necessarily correspond to columns in SELECT statement pParent,
      ** zero them before transfering the ORDER BY clause.
      **
      ** Not doing this may cause an error if a subsequent call to this
      ** function attempts to flatten a compound sub-query into pParent
      ** (the only way this can happen is if the compound sub-query is
      ** currently part of pSub->pSrc). See ticket [d11a6e908f].  */
      ExprList *pOrderBy = pSub->pOrderBy;
      for(i=0; i<pOrderBy->nExpr; i++){
        pOrderBy->a[i].u.x.iOrderByCol = 0;
      }
      assert( pParent->pOrderBy==0 );
      assert( pSub->pPrior==0 );
      pParent->pOrderBy = pOrderBy;
      pSub->pOrderBy = 0;
    }else if( pParent->pOrderBy ){
      substExprList(db, pParent->pOrderBy, iParent, pSub->pEList);
    }
    if( pSub->pWhere ){
      pWhere = sqlite3ExprDup(db, pSub->pWhere, 0);
    }else{
      pWhere = 0;
    }
    if( subqueryIsAgg ){
      assert( pParent->pHaving==0 );
      pParent->pHaving = pParent->pWhere;
      pParent->pWhere = pWhere;
      pParent->pHaving = substExpr(db, pParent->pHaving, iParent, pSub->pEList);
      pParent->pHaving = sqlite3ExprAnd(db, pParent->pHaving, 
                                  sqlite3ExprDup(db, pSub->pHaving, 0));
      assert( pParent->pGroupBy==0 );
      pParent->pGroupBy = sqlite3ExprListDup(db, pSub->pGroupBy, 0);
    }else{
      pParent->pWhere = substExpr(db, pParent->pWhere, iParent, pSub->pEList);
      pParent->pWhere = sqlite3ExprAnd(db, pParent->pWhere, pWhere);
    }
  
    /* The flattened query is distinct if either the inner or the
    ** outer query is distinct. 
    */
    pParent->selFlags |= pSub->selFlags & SF_Distinct;
  
    /*
    ** SELECT ... FROM (SELECT ... LIMIT a OFFSET b) LIMIT x OFFSET y;
    **
    ** One is tempted to try to add a and b to combine the limits.  But this
    ** does not work if either limit is negative.
    */
    if( pSub->pLimit ){
      pParent->pLimit = pSub->pLimit;
      pSub->pLimit = 0;
    }
  }

  /* Finially, delete what is left of the subquery and return
  ** success.
  */
  sqlite3SelectDelete(db, pSub1);

#if SELECTTRACE_ENABLED
  if( sqlite3SelectTrace & 0x100 ){
    sqlite3DebugPrintf("After flattening:\n");
    sqlite3TreeViewSelect(0, p, 0);
  }
#endif

  return 1;
}
#endif /* !defined(SQLITE_OMIT_SUBQUERY) || !defined(SQLITE_OMIT_VIEW) */
