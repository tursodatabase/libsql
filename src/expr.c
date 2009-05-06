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
** This file contains routines used for analyzing expressions and
** for generating VDBE code that evaluates expressions in SQLite.
**
** $Id: expr.c,v 1.432 2009/05/06 18:57:10 shane Exp $
*/
#include "sqliteInt.h"

/*
** Return the 'affinity' of the expression pExpr if any.
**
** If pExpr is a column, a reference to a column via an 'AS' alias,
** or a sub-select with a column as the return value, then the 
** affinity of that column is returned. Otherwise, 0x00 is returned,
** indicating no affinity for the expression.
**
** i.e. the WHERE clause expresssions in the following statements all
** have an affinity:
**
** CREATE TABLE t1(a);
** SELECT * FROM t1 WHERE a;
** SELECT a AS b FROM t1 WHERE b;
** SELECT * FROM t1 WHERE (select a from t1);
*/
char sqlite3ExprAffinity(Expr *pExpr){
  int op = pExpr->op;
  if( op==TK_SELECT ){
    assert( pExpr->flags&EP_xIsSelect );
    return sqlite3ExprAffinity(pExpr->x.pSelect->pEList->a[0].pExpr);
  }
#ifndef SQLITE_OMIT_CAST
  if( op==TK_CAST ){
    return sqlite3AffinityType(&pExpr->token);
  }
#endif
  if( (op==TK_AGG_COLUMN || op==TK_COLUMN || op==TK_REGISTER) 
   && pExpr->pTab!=0
  ){
    /* op==TK_REGISTER && pExpr->pTab!=0 happens when pExpr was originally
    ** a TK_COLUMN but was previously evaluated and cached in a register */
    int j = pExpr->iColumn;
    if( j<0 ) return SQLITE_AFF_INTEGER;
    assert( pExpr->pTab && j<pExpr->pTab->nCol );
    return pExpr->pTab->aCol[j].affinity;
  }
  return pExpr->affinity;
}

/*
** Set the collating sequence for expression pExpr to be the collating
** sequence named by pToken.   Return a pointer to the revised expression.
** The collating sequence is marked as "explicit" using the EP_ExpCollate
** flag.  An explicit collating sequence will override implicit
** collating sequences.
*/
Expr *sqlite3ExprSetColl(Parse *pParse, Expr *pExpr, Token *pCollName){
  char *zColl = 0;            /* Dequoted name of collation sequence */
  CollSeq *pColl;
  sqlite3 *db = pParse->db;
  zColl = sqlite3NameFromToken(db, pCollName);
  if( pExpr && zColl ){
    pColl = sqlite3LocateCollSeq(pParse, zColl, -1);
    if( pColl ){
      pExpr->pColl = pColl;
      pExpr->flags |= EP_ExpCollate;
    }
  }
  sqlite3DbFree(db, zColl);
  return pExpr;
}

/*
** Return the default collation sequence for the expression pExpr. If
** there is no default collation type, return 0.
*/
CollSeq *sqlite3ExprCollSeq(Parse *pParse, Expr *pExpr){
  CollSeq *pColl = 0;
  Expr *p = pExpr;
  while( p ){
    int op;
    pColl = p->pColl;
    if( pColl ) break;
    op = p->op;
    if( (op==TK_AGG_COLUMN || op==TK_COLUMN || op==TK_REGISTER) && p->pTab!=0 ){
      /* op==TK_REGISTER && p->pTab!=0 happens when pExpr was originally
      ** a TK_COLUMN but was previously evaluated and cached in a register */
      const char *zColl;
      int j = p->iColumn;
      if( j>=0 ){
        sqlite3 *db = pParse->db;
        zColl = p->pTab->aCol[j].zColl;
        pColl = sqlite3FindCollSeq(db, ENC(db), zColl, -1, 0);
        pExpr->pColl = pColl;
      }
      break;
    }
    if( op!=TK_CAST && op!=TK_UPLUS ){
      break;
    }
    p = p->pLeft;
  }
  if( sqlite3CheckCollSeq(pParse, pColl) ){ 
    pColl = 0;
  }
  return pColl;
}

/*
** pExpr is an operand of a comparison operator.  aff2 is the
** type affinity of the other operand.  This routine returns the
** type affinity that should be used for the comparison operator.
*/
char sqlite3CompareAffinity(Expr *pExpr, char aff2){
  char aff1 = sqlite3ExprAffinity(pExpr);
  if( aff1 && aff2 ){
    /* Both sides of the comparison are columns. If one has numeric
    ** affinity, use that. Otherwise use no affinity.
    */
    if( sqlite3IsNumericAffinity(aff1) || sqlite3IsNumericAffinity(aff2) ){
      return SQLITE_AFF_NUMERIC;
    }else{
      return SQLITE_AFF_NONE;
    }
  }else if( !aff1 && !aff2 ){
    /* Neither side of the comparison is a column.  Compare the
    ** results directly.
    */
    return SQLITE_AFF_NONE;
  }else{
    /* One side is a column, the other is not. Use the columns affinity. */
    assert( aff1==0 || aff2==0 );
    return (aff1 + aff2);
  }
}

/*
** pExpr is a comparison operator.  Return the type affinity that should
** be applied to both operands prior to doing the comparison.
*/
static char comparisonAffinity(Expr *pExpr){
  char aff;
  assert( pExpr->op==TK_EQ || pExpr->op==TK_IN || pExpr->op==TK_LT ||
          pExpr->op==TK_GT || pExpr->op==TK_GE || pExpr->op==TK_LE ||
          pExpr->op==TK_NE );
  assert( pExpr->pLeft );
  aff = sqlite3ExprAffinity(pExpr->pLeft);
  if( pExpr->pRight ){
    aff = sqlite3CompareAffinity(pExpr->pRight, aff);
  }else if( ExprHasProperty(pExpr, EP_xIsSelect) ){
    aff = sqlite3CompareAffinity(pExpr->x.pSelect->pEList->a[0].pExpr, aff);
  }else if( !aff ){
    aff = SQLITE_AFF_NONE;
  }
  return aff;
}

/*
** pExpr is a comparison expression, eg. '=', '<', IN(...) etc.
** idx_affinity is the affinity of an indexed column. Return true
** if the index with affinity idx_affinity may be used to implement
** the comparison in pExpr.
*/
int sqlite3IndexAffinityOk(Expr *pExpr, char idx_affinity){
  char aff = comparisonAffinity(pExpr);
  switch( aff ){
    case SQLITE_AFF_NONE:
      return 1;
    case SQLITE_AFF_TEXT:
      return idx_affinity==SQLITE_AFF_TEXT;
    default:
      return sqlite3IsNumericAffinity(idx_affinity);
  }
}

/*
** Return the P5 value that should be used for a binary comparison
** opcode (OP_Eq, OP_Ge etc.) used to compare pExpr1 and pExpr2.
*/
static u8 binaryCompareP5(Expr *pExpr1, Expr *pExpr2, int jumpIfNull){
  u8 aff = (char)sqlite3ExprAffinity(pExpr2);
  aff = (u8)sqlite3CompareAffinity(pExpr1, aff) | (u8)jumpIfNull;
  return aff;
}

/*
** Return a pointer to the collation sequence that should be used by
** a binary comparison operator comparing pLeft and pRight.
**
** If the left hand expression has a collating sequence type, then it is
** used. Otherwise the collation sequence for the right hand expression
** is used, or the default (BINARY) if neither expression has a collating
** type.
**
** Argument pRight (but not pLeft) may be a null pointer. In this case,
** it is not considered.
*/
CollSeq *sqlite3BinaryCompareCollSeq(
  Parse *pParse, 
  Expr *pLeft, 
  Expr *pRight
){
  CollSeq *pColl;
  assert( pLeft );
  if( pLeft->flags & EP_ExpCollate ){
    assert( pLeft->pColl );
    pColl = pLeft->pColl;
  }else if( pRight && pRight->flags & EP_ExpCollate ){
    assert( pRight->pColl );
    pColl = pRight->pColl;
  }else{
    pColl = sqlite3ExprCollSeq(pParse, pLeft);
    if( !pColl ){
      pColl = sqlite3ExprCollSeq(pParse, pRight);
    }
  }
  return pColl;
}

/*
** Generate the operands for a comparison operation.  Before
** generating the code for each operand, set the EP_AnyAff
** flag on the expression so that it will be able to used a
** cached column value that has previously undergone an
** affinity change.
*/
static void codeCompareOperands(
  Parse *pParse,    /* Parsing and code generating context */
  Expr *pLeft,      /* The left operand */
  int *pRegLeft,    /* Register where left operand is stored */
  int *pFreeLeft,   /* Free this register when done */
  Expr *pRight,     /* The right operand */
  int *pRegRight,   /* Register where right operand is stored */
  int *pFreeRight   /* Write temp register for right operand there */
){
  while( pLeft->op==TK_UPLUS ) pLeft = pLeft->pLeft;
  pLeft->flags |= EP_AnyAff;
  *pRegLeft = sqlite3ExprCodeTemp(pParse, pLeft, pFreeLeft);
  while( pRight->op==TK_UPLUS ) pRight = pRight->pLeft;
  pRight->flags |= EP_AnyAff;
  *pRegRight = sqlite3ExprCodeTemp(pParse, pRight, pFreeRight);
}

/*
** Generate code for a comparison operator.
*/
static int codeCompare(
  Parse *pParse,    /* The parsing (and code generating) context */
  Expr *pLeft,      /* The left operand */
  Expr *pRight,     /* The right operand */
  int opcode,       /* The comparison opcode */
  int in1, int in2, /* Register holding operands */
  int dest,         /* Jump here if true.  */
  int jumpIfNull    /* If true, jump if either operand is NULL */
){
  int p5;
  int addr;
  CollSeq *p4;

  p4 = sqlite3BinaryCompareCollSeq(pParse, pLeft, pRight);
  p5 = binaryCompareP5(pLeft, pRight, jumpIfNull);
  addr = sqlite3VdbeAddOp4(pParse->pVdbe, opcode, in2, dest, in1,
                           (void*)p4, P4_COLLSEQ);
  sqlite3VdbeChangeP5(pParse->pVdbe, (u8)p5);
  if( (p5 & SQLITE_AFF_MASK)!=SQLITE_AFF_NONE ){
    sqlite3ExprCacheAffinityChange(pParse, in1, 1);
    sqlite3ExprCacheAffinityChange(pParse, in2, 1);
  }
  return addr;
}

#if SQLITE_MAX_EXPR_DEPTH>0
/*
** Check that argument nHeight is less than or equal to the maximum
** expression depth allowed. If it is not, leave an error message in
** pParse.
*/
int sqlite3ExprCheckHeight(Parse *pParse, int nHeight){
  int rc = SQLITE_OK;
  int mxHeight = pParse->db->aLimit[SQLITE_LIMIT_EXPR_DEPTH];
  if( nHeight>mxHeight ){
    sqlite3ErrorMsg(pParse, 
       "Expression tree is too large (maximum depth %d)", mxHeight
    );
    rc = SQLITE_ERROR;
  }
  return rc;
}

/* The following three functions, heightOfExpr(), heightOfExprList()
** and heightOfSelect(), are used to determine the maximum height
** of any expression tree referenced by the structure passed as the
** first argument.
**
** If this maximum height is greater than the current value pointed
** to by pnHeight, the second parameter, then set *pnHeight to that
** value.
*/
static void heightOfExpr(Expr *p, int *pnHeight){
  if( p ){
    if( p->nHeight>*pnHeight ){
      *pnHeight = p->nHeight;
    }
  }
}
static void heightOfExprList(ExprList *p, int *pnHeight){
  if( p ){
    int i;
    for(i=0; i<p->nExpr; i++){
      heightOfExpr(p->a[i].pExpr, pnHeight);
    }
  }
}
static void heightOfSelect(Select *p, int *pnHeight){
  if( p ){
    heightOfExpr(p->pWhere, pnHeight);
    heightOfExpr(p->pHaving, pnHeight);
    heightOfExpr(p->pLimit, pnHeight);
    heightOfExpr(p->pOffset, pnHeight);
    heightOfExprList(p->pEList, pnHeight);
    heightOfExprList(p->pGroupBy, pnHeight);
    heightOfExprList(p->pOrderBy, pnHeight);
    heightOfSelect(p->pPrior, pnHeight);
  }
}

/*
** Set the Expr.nHeight variable in the structure passed as an 
** argument. An expression with no children, Expr.pList or 
** Expr.pSelect member has a height of 1. Any other expression
** has a height equal to the maximum height of any other 
** referenced Expr plus one.
*/
static void exprSetHeight(Expr *p){
  int nHeight = 0;
  heightOfExpr(p->pLeft, &nHeight);
  heightOfExpr(p->pRight, &nHeight);
  if( ExprHasProperty(p, EP_xIsSelect) ){
    heightOfSelect(p->x.pSelect, &nHeight);
  }else{
    heightOfExprList(p->x.pList, &nHeight);
  }
  p->nHeight = nHeight + 1;
}

/*
** Set the Expr.nHeight variable using the exprSetHeight() function. If
** the height is greater than the maximum allowed expression depth,
** leave an error in pParse.
*/
void sqlite3ExprSetHeight(Parse *pParse, Expr *p){
  exprSetHeight(p);
  sqlite3ExprCheckHeight(pParse, p->nHeight);
}

/*
** Return the maximum height of any expression tree referenced
** by the select statement passed as an argument.
*/
int sqlite3SelectExprHeight(Select *p){
  int nHeight = 0;
  heightOfSelect(p, &nHeight);
  return nHeight;
}
#else
  #define exprSetHeight(y)
#endif /* SQLITE_MAX_EXPR_DEPTH>0 */

/*
** Construct a new expression node and return a pointer to it.  Memory
** for this node is obtained from sqlite3_malloc().  The calling function
** is responsible for making sure the node eventually gets freed.
*/
Expr *sqlite3Expr(
  sqlite3 *db,            /* Handle for sqlite3DbMallocZero() (may be null) */
  int op,                 /* Expression opcode */
  Expr *pLeft,            /* Left operand */
  Expr *pRight,           /* Right operand */
  const Token *pToken     /* Argument token */
){
  Expr *pNew;
  pNew = sqlite3DbMallocZero(db, sizeof(Expr));
  if( pNew==0 ){
    /* When malloc fails, delete pLeft and pRight. Expressions passed to 
    ** this function must always be allocated with sqlite3Expr() for this 
    ** reason. 
    */
    sqlite3ExprDelete(db, pLeft);
    sqlite3ExprDelete(db, pRight);
    return 0;
  }
  pNew->op = (u8)op;
  pNew->pLeft = pLeft;
  pNew->pRight = pRight;
  pNew->iAgg = -1;
  pNew->span.z = (u8*)"";
  if( pToken ){
    int c;
    assert( pToken->dyn==0 );
    pNew->span = *pToken;
    if( pToken->n>=2 
         && ((c = pToken->z[0])=='\'' || c=='"' || c=='[' || c=='`') ){
      sqlite3TokenCopy(db, &pNew->token, pToken);
      if( pNew->token.z ){
        pNew->token.n = sqlite3Dequote((char*)pNew->token.z);
        assert( pNew->token.n==(unsigned)sqlite3Strlen30((char*)pNew->token.z) );
      }
      if( c=='"' ) pNew->flags |= EP_DblQuoted;
    }else{
      pNew->token = *pToken;
    }
    pNew->token.quoted = 0;
  }else if( pLeft ){
    if( pRight ){
      if( pRight->span.dyn==0 && pLeft->span.dyn==0 ){
        sqlite3ExprSpan(pNew, &pLeft->span, &pRight->span);
      }
      if( pRight->flags & EP_ExpCollate ){
        pNew->flags |= EP_ExpCollate;
        pNew->pColl = pRight->pColl;
      }
    }
    if( pLeft->flags & EP_ExpCollate ){
      pNew->flags |= EP_ExpCollate;
      pNew->pColl = pLeft->pColl;
    }
  }

  exprSetHeight(pNew);
  return pNew;
}

/*
** Works like sqlite3Expr() except that it takes an extra Parse*
** argument and notifies the associated connection object if malloc fails.
*/
Expr *sqlite3PExpr(
  Parse *pParse,          /* Parsing context */
  int op,                 /* Expression opcode */
  Expr *pLeft,            /* Left operand */
  Expr *pRight,           /* Right operand */
  const Token *pToken     /* Argument token */
){
  Expr *p = sqlite3Expr(pParse->db, op, pLeft, pRight, pToken);
  if( p ){
    sqlite3ExprCheckHeight(pParse, p->nHeight);
  }
  return p;
}

/*
** When doing a nested parse, you can include terms in an expression
** that look like this:   #1 #2 ...  These terms refer to registers
** in the virtual machine.  #N is the N-th register.
**
** This routine is called by the parser to deal with on of those terms.
** It immediately generates code to store the value in a memory location.
** The returns an expression that will code to extract the value from
** that memory location as needed.
*/
Expr *sqlite3RegisterExpr(Parse *pParse, Token *pToken){
  Vdbe *v = pParse->pVdbe;
  Expr *p;
  if( pParse->nested==0 ){
    sqlite3ErrorMsg(pParse, "near \"%T\": syntax error", pToken);
    return sqlite3PExpr(pParse, TK_NULL, 0, 0, 0);
  }
  if( v==0 ) return 0;
  p = sqlite3PExpr(pParse, TK_REGISTER, 0, 0, pToken);
  if( p==0 ){
    return 0;  /* Malloc failed */
  }
  p->iTable = atoi((char*)&pToken->z[1]);
  return p;
}

/*
** Join two expressions using an AND operator.  If either expression is
** NULL, then just return the other expression.
*/
Expr *sqlite3ExprAnd(sqlite3 *db, Expr *pLeft, Expr *pRight){
  if( pLeft==0 ){
    return pRight;
  }else if( pRight==0 ){
    return pLeft;
  }else{
    return sqlite3Expr(db, TK_AND, pLeft, pRight, 0);
  }
}

/*
** Set the Expr.span field of the given expression to span all
** text between the two given tokens.  Both tokens must be pointing
** at the same string.
*/
void sqlite3ExprSpan(Expr *pExpr, Token *pLeft, Token *pRight){
  assert( pRight!=0 );
  assert( pLeft!=0 );
  if( pExpr ){
    pExpr->span.z = pLeft->z;
    /* The following assert() may fail when this is called 
    ** via sqlite3PExpr()/sqlite3Expr() from addWhereTerm(). */
    /* assert(pRight->z >= pLeft->z); */
    pExpr->span.n = pRight->n + (unsigned)(pRight->z - pLeft->z);
  }
}

/*
** Construct a new expression node for a function with multiple
** arguments.
*/
Expr *sqlite3ExprFunction(Parse *pParse, ExprList *pList, Token *pToken){
  Expr *pNew;
  sqlite3 *db = pParse->db;
  assert( pToken );
  pNew = sqlite3DbMallocZero(db, sizeof(Expr) );
  if( pNew==0 ){
    sqlite3ExprListDelete(db, pList); /* Avoid memory leak when malloc fails */
    return 0;
  }
  pNew->op = TK_FUNCTION;
  pNew->x.pList = pList;
  assert( !ExprHasProperty(pNew, EP_xIsSelect) );
  assert( pToken->dyn==0 );
  pNew->span = *pToken;
  sqlite3TokenCopy(db, &pNew->token, pToken);
  sqlite3ExprSetHeight(pParse, pNew);
  return pNew;
}

/*
** Assign a variable number to an expression that encodes a wildcard
** in the original SQL statement.  
**
** Wildcards consisting of a single "?" are assigned the next sequential
** variable number.
**
** Wildcards of the form "?nnn" are assigned the number "nnn".  We make
** sure "nnn" is not too be to avoid a denial of service attack when
** the SQL statement comes from an external source.
**
** Wildcards of the form ":aaa" or "$aaa" are assigned the same number
** as the previous instance of the same wildcard.  Or if this is the first
** instance of the wildcard, the next sequenial variable number is
** assigned.
*/
void sqlite3ExprAssignVarNumber(Parse *pParse, Expr *pExpr){
  Token *pToken;
  sqlite3 *db = pParse->db;

  if( pExpr==0 ) return;
  pToken = &pExpr->token;
  assert( pToken->n>=1 );
  assert( pToken->z!=0 );
  assert( pToken->z[0]!=0 );
  if( pToken->n==1 ){
    /* Wildcard of the form "?".  Assign the next variable number */
    pExpr->iTable = ++pParse->nVar;
  }else if( pToken->z[0]=='?' ){
    /* Wildcard of the form "?nnn".  Convert "nnn" to an integer and
    ** use it as the variable number */
    int i;
    pExpr->iTable = i = atoi((char*)&pToken->z[1]);
    testcase( i==0 );
    testcase( i==1 );
    testcase( i==db->aLimit[SQLITE_LIMIT_VARIABLE_NUMBER]-1 );
    testcase( i==db->aLimit[SQLITE_LIMIT_VARIABLE_NUMBER] );
    if( i<1 || i>db->aLimit[SQLITE_LIMIT_VARIABLE_NUMBER] ){
      sqlite3ErrorMsg(pParse, "variable number must be between ?1 and ?%d",
          db->aLimit[SQLITE_LIMIT_VARIABLE_NUMBER]);
    }
    if( i>pParse->nVar ){
      pParse->nVar = i;
    }
  }else{
    /* Wildcards of the form ":aaa" or "$aaa".  Reuse the same variable
    ** number as the prior appearance of the same name, or if the name
    ** has never appeared before, reuse the same variable number
    */
    int i;
    u32 n;
    n = pToken->n;
    for(i=0; i<pParse->nVarExpr; i++){
      Expr *pE;
      if( (pE = pParse->apVarExpr[i])!=0
          && pE->token.n==n
          && memcmp(pE->token.z, pToken->z, n)==0 ){
        pExpr->iTable = pE->iTable;
        break;
      }
    }
    if( i>=pParse->nVarExpr ){
      pExpr->iTable = ++pParse->nVar;
      if( pParse->nVarExpr>=pParse->nVarExprAlloc-1 ){
        pParse->nVarExprAlloc += pParse->nVarExprAlloc + 10;
        pParse->apVarExpr =
            sqlite3DbReallocOrFree(
              db,
              pParse->apVarExpr,
              pParse->nVarExprAlloc*sizeof(pParse->apVarExpr[0])
            );
      }
      if( !db->mallocFailed ){
        assert( pParse->apVarExpr!=0 );
        pParse->apVarExpr[pParse->nVarExpr++] = pExpr;
      }
    }
  } 
  if( !pParse->nErr && pParse->nVar>db->aLimit[SQLITE_LIMIT_VARIABLE_NUMBER] ){
    sqlite3ErrorMsg(pParse, "too many SQL variables");
  }
}

/*
** Clear an expression structure without deleting the structure itself.
** Substructure is deleted.
*/
void sqlite3ExprClear(sqlite3 *db, Expr *p){
  if( p->token.dyn ) sqlite3DbFree(db, (char*)p->token.z);
  if( !ExprHasAnyProperty(p, EP_TokenOnly|EP_SpanToken) ){
    if( p->span.dyn ) sqlite3DbFree(db, (char*)p->span.z);
    if( ExprHasProperty(p, EP_Reduced) ){
      /* Subtrees are part of the same memory allocation when EP_Reduced set */
      if( p->pLeft ) sqlite3ExprClear(db, p->pLeft);
      if( p->pRight ) sqlite3ExprClear(db, p->pRight);
    }else{
      /* Subtrees are separate allocations when EP_Reduced is clear */
      sqlite3ExprDelete(db, p->pLeft);
      sqlite3ExprDelete(db, p->pRight);
    }
    /* x.pSelect and x.pList are always separately allocated */
    if( ExprHasProperty(p, EP_xIsSelect) ){
      sqlite3SelectDelete(db, p->x.pSelect);
    }else{
      sqlite3ExprListDelete(db, p->x.pList);
    }
  }
}

/*
** Recursively delete an expression tree.
*/
void sqlite3ExprDelete(sqlite3 *db, Expr *p){
  if( p==0 ) return;
  sqlite3ExprClear(db, p);
  sqlite3DbFree(db, p);
}

/*
** Return the number of bytes allocated for the expression structure 
** passed as the first argument. This is always one of EXPR_FULLSIZE,
** EXPR_REDUCEDSIZE or EXPR_TOKENONLYSIZE.
*/
static int exprStructSize(Expr *p){
  if( ExprHasProperty(p, EP_TokenOnly) ) return EXPR_TOKENONLYSIZE;
  if( ExprHasProperty(p, EP_SpanToken) ) return EXPR_SPANTOKENSIZE;
  if( ExprHasProperty(p, EP_Reduced) ) return EXPR_REDUCEDSIZE;
  return EXPR_FULLSIZE;
}

/*
** sqlite3ExprDup() has been called to create a copy of expression p with
** the EXPRDUP_XXX flags passed as the second argument. This function 
** returns the space required for the copy of the Expr structure only.
** This is always one of EXPR_FULLSIZE, EXPR_REDUCEDSIZE or EXPR_TOKENONLYSIZE.
*/
static int dupedExprStructSize(Expr *p, int flags){
  int nSize;
  if( 0==(flags&EXPRDUP_REDUCE) ){
    nSize = EXPR_FULLSIZE;
  }else if( p->pLeft || p->pRight || p->pColl || p->x.pList ){
    nSize = EXPR_REDUCEDSIZE;
  }else if( flags&EXPRDUP_SPAN ){
    nSize = EXPR_SPANTOKENSIZE;
  }else{
    nSize = EXPR_TOKENONLYSIZE;
  }
  return nSize;
}

/*
** sqlite3ExprDup() has been called to create a copy of expression p with
** the EXPRDUP_XXX passed as the second argument. This function returns
** the space in bytes required to store the copy of the Expr structure
** and the copies of the Expr.token.z and Expr.span.z (if applicable)
** string buffers.
*/
static int dupedExprNodeSize(Expr *p, int flags){
  int nByte = dupedExprStructSize(p, flags) + (p->token.z ? p->token.n + 1 : 0);
  if( (flags&EXPRDUP_SPAN)!=0
   && (p->token.z!=p->span.z || p->token.n!=p->span.n)
  ){
    nByte += p->span.n;
  }
  return ROUND8(nByte);
}

/*
** Return the number of bytes required to create a duplicate of the 
** expression passed as the first argument. The second argument is a
** mask containing EXPRDUP_XXX flags.
**
** The value returned includes space to create a copy of the Expr struct
** itself and the buffer referred to by Expr.token, if any. If the 
** EXPRDUP_SPAN flag is set, then space to create a copy of the buffer
** refered to by Expr.span is also included.
**
** If the EXPRDUP_REDUCE flag is set, then the return value includes 
** space to duplicate all Expr nodes in the tree formed by Expr.pLeft 
** and Expr.pRight variables (but not for any structures pointed to or 
** descended from the Expr.x.pList or Expr.x.pSelect variables).
*/
static int dupedExprSize(Expr *p, int flags){
  int nByte = 0;
  if( p ){
    nByte = dupedExprNodeSize(p, flags);
    if( flags&EXPRDUP_REDUCE ){
      int f = flags&(~EXPRDUP_SPAN);
      nByte += dupedExprSize(p->pLeft, f) + dupedExprSize(p->pRight, f);
    }
  }
  return nByte;
}

/*
** This function is similar to sqlite3ExprDup(), except that if pzBuffer 
** is not NULL then *pzBuffer is assumed to point to a buffer large enough 
** to store the copy of expression p, the copies of p->token and p->span 
** (if applicable), and the copies of the p->pLeft and p->pRight expressions,
** if any. Before returning, *pzBuffer is set to the first byte passed the
** portion of the buffer copied into by this function.
*/
static Expr *exprDup(sqlite3 *db, Expr *p, int flags, u8 **pzBuffer){
  Expr *pNew = 0;                      /* Value to return */
  if( p ){
    const int isRequireSpan = (flags&EXPRDUP_SPAN);
    const int isReduced = (flags&EXPRDUP_REDUCE);
    u8 *zAlloc;

    assert( pzBuffer==0 || isReduced );

    /* Figure out where to write the new Expr structure. */
    if( pzBuffer ){
      zAlloc = *pzBuffer;
    }else{
      zAlloc = sqlite3DbMallocRaw(db, dupedExprSize(p, flags));
    }
    pNew = (Expr *)zAlloc;

    if( pNew ){
      /* Set nNewSize to the size allocated for the structure pointed to
      ** by pNew. This is either EXPR_FULLSIZE, EXPR_REDUCEDSIZE or
      ** EXPR_TOKENONLYSIZE. nToken is set to the number of bytes consumed
      ** by the copy of the p->token.z string (if any).
      */
      const int nNewSize = dupedExprStructSize(p, flags);
      const int nToken = (p->token.z ? p->token.n + 1 : 0);
      if( isReduced ){
        assert( ExprHasProperty(p, EP_Reduced)==0 );
        memcpy(zAlloc, p, nNewSize);
      }else{
        int nSize = exprStructSize(p);
        memcpy(zAlloc, p, nSize);
        memset(&zAlloc[nSize], 0, EXPR_FULLSIZE-nSize);
      }

      /* Set the EP_Reduced and EP_TokenOnly flags appropriately. */
      pNew->flags &= ~(EP_Reduced|EP_TokenOnly|EP_SpanToken);
      switch( nNewSize ){
        case EXPR_REDUCEDSIZE:   pNew->flags |= EP_Reduced; break;
        case EXPR_TOKENONLYSIZE: pNew->flags |= EP_TokenOnly; break;
        case EXPR_SPANTOKENSIZE: pNew->flags |= EP_SpanToken; break;
      }

      /* Copy the p->token string, if any. */
      if( nToken ){
        unsigned char *zToken = &zAlloc[nNewSize];
        memcpy(zToken, p->token.z, nToken-1);
        zToken[nToken-1] = '\0';
        pNew->token.dyn = 0;
        pNew->token.z = zToken;
      }

      if( 0==((p->flags|pNew->flags) & EP_TokenOnly) ){
        /* Fill in the pNew->span token, if required. */
        if( isRequireSpan ){
          if( p->token.z!=p->span.z || p->token.n!=p->span.n ){
            pNew->span.z = &zAlloc[nNewSize+nToken];
            memcpy((char *)pNew->span.z, p->span.z, p->span.n);
            pNew->span.dyn = 0;
          }else{
            pNew->span.z = pNew->token.z;
            pNew->span.n = pNew->token.n;
          }
        }else{
          pNew->span.z = 0;
          pNew->span.n = 0;
        }
      }

      if( 0==((p->flags|pNew->flags) & (EP_TokenOnly|EP_SpanToken)) ){
        /* Fill in the pNew->x.pSelect or pNew->x.pList member. */
        if( ExprHasProperty(p, EP_xIsSelect) ){
          pNew->x.pSelect = sqlite3SelectDup(db, p->x.pSelect, isReduced);
        }else{
          pNew->x.pList = sqlite3ExprListDup(db, p->x.pList, isReduced);
        }
      }

      /* Fill in pNew->pLeft and pNew->pRight. */
      if( ExprHasAnyProperty(pNew, EP_Reduced|EP_TokenOnly|EP_SpanToken) ){
        zAlloc += dupedExprNodeSize(p, flags);
        if( ExprHasProperty(pNew, EP_Reduced) ){
          pNew->pLeft = exprDup(db, p->pLeft, EXPRDUP_REDUCE, &zAlloc);
          pNew->pRight = exprDup(db, p->pRight, EXPRDUP_REDUCE, &zAlloc);
        }
        if( pzBuffer ){
          *pzBuffer = zAlloc;
        }
      }else if( !ExprHasAnyProperty(p, EP_TokenOnly|EP_SpanToken) ){
        pNew->pLeft = sqlite3ExprDup(db, p->pLeft, 0);
        pNew->pRight = sqlite3ExprDup(db, p->pRight, 0);
      }
    }
  }
  return pNew;
}

/*
** The following group of routines make deep copies of expressions,
** expression lists, ID lists, and select statements.  The copies can
** be deleted (by being passed to their respective ...Delete() routines)
** without effecting the originals.
**
** The expression list, ID, and source lists return by sqlite3ExprListDup(),
** sqlite3IdListDup(), and sqlite3SrcListDup() can not be further expanded 
** by subsequent calls to sqlite*ListAppend() routines.
**
** Any tables that the SrcList might point to are not duplicated.
**
** The flags parameter contains a combination of the EXPRDUP_XXX flags. If
** the EXPRDUP_SPAN flag is set in the argument parameter, then the 
** Expr.span field of the input expression is copied. If EXPRDUP_SPAN is
** clear, then the Expr.span field of the returned expression structure
** is zeroed.
**
** If the EXPRDUP_REDUCE flag is set, then the structure returned is a
** truncated version of the usual Expr structure that will be stored as
** part of the in-memory representation of the database schema.
*/
Expr *sqlite3ExprDup(sqlite3 *db, Expr *p, int flags){
  return exprDup(db, p, flags, 0);
}
void sqlite3TokenCopy(sqlite3 *db, Token *pTo, const Token *pFrom){
  if( pTo->dyn ) sqlite3DbFree(db, (char*)pTo->z);
  if( pFrom->z ){
    pTo->n = pFrom->n;
    pTo->z = (u8*)sqlite3DbStrNDup(db, (char*)pFrom->z, pFrom->n);
    pTo->dyn = 1;
  }else{
    pTo->z = 0;
  }
}
ExprList *sqlite3ExprListDup(sqlite3 *db, ExprList *p, int flags){
  ExprList *pNew;
  struct ExprList_item *pItem, *pOldItem;
  int i;
  if( p==0 ) return 0;
  pNew = sqlite3DbMallocRaw(db, sizeof(*pNew) );
  if( pNew==0 ) return 0;
  pNew->iECursor = 0;
  pNew->nExpr = pNew->nAlloc = p->nExpr;
  pNew->a = pItem = sqlite3DbMallocRaw(db,  p->nExpr*sizeof(p->a[0]) );
  if( pItem==0 ){
    sqlite3DbFree(db, pNew);
    return 0;
  } 
  pOldItem = p->a;
  for(i=0; i<p->nExpr; i++, pItem++, pOldItem++){
    Expr *pNewExpr;
    Expr *pOldExpr = pOldItem->pExpr;
    pItem->pExpr = pNewExpr = sqlite3ExprDup(db, pOldExpr, flags);
    pItem->zName = sqlite3DbStrDup(db, pOldItem->zName);
    pItem->sortOrder = pOldItem->sortOrder;
    pItem->done = 0;
    pItem->iCol = pOldItem->iCol;
    pItem->iAlias = pOldItem->iAlias;
  }
  return pNew;
}

/*
** If cursors, triggers, views and subqueries are all omitted from
** the build, then none of the following routines, except for 
** sqlite3SelectDup(), can be called. sqlite3SelectDup() is sometimes
** called with a NULL argument.
*/
#if !defined(SQLITE_OMIT_VIEW) || !defined(SQLITE_OMIT_TRIGGER) \
 || !defined(SQLITE_OMIT_SUBQUERY)
SrcList *sqlite3SrcListDup(sqlite3 *db, SrcList *p, int flags){
  SrcList *pNew;
  int i;
  int nByte;
  if( p==0 ) return 0;
  nByte = sizeof(*p) + (p->nSrc>0 ? sizeof(p->a[0]) * (p->nSrc-1) : 0);
  pNew = sqlite3DbMallocRaw(db, nByte );
  if( pNew==0 ) return 0;
  pNew->nSrc = pNew->nAlloc = p->nSrc;
  for(i=0; i<p->nSrc; i++){
    struct SrcList_item *pNewItem = &pNew->a[i];
    struct SrcList_item *pOldItem = &p->a[i];
    Table *pTab;
    pNewItem->zDatabase = sqlite3DbStrDup(db, pOldItem->zDatabase);
    pNewItem->zName = sqlite3DbStrDup(db, pOldItem->zName);
    pNewItem->zAlias = sqlite3DbStrDup(db, pOldItem->zAlias);
    pNewItem->jointype = pOldItem->jointype;
    pNewItem->iCursor = pOldItem->iCursor;
    pNewItem->isPopulated = pOldItem->isPopulated;
    pNewItem->zIndex = sqlite3DbStrDup(db, pOldItem->zIndex);
    pNewItem->notIndexed = pOldItem->notIndexed;
    pNewItem->pIndex = pOldItem->pIndex;
    pTab = pNewItem->pTab = pOldItem->pTab;
    if( pTab ){
      pTab->nRef++;
    }
    pNewItem->pSelect = sqlite3SelectDup(db, pOldItem->pSelect, flags);
    pNewItem->pOn = sqlite3ExprDup(db, pOldItem->pOn, flags);
    pNewItem->pUsing = sqlite3IdListDup(db, pOldItem->pUsing);
    pNewItem->colUsed = pOldItem->colUsed;
  }
  return pNew;
}
IdList *sqlite3IdListDup(sqlite3 *db, IdList *p){
  IdList *pNew;
  int i;
  if( p==0 ) return 0;
  pNew = sqlite3DbMallocRaw(db, sizeof(*pNew) );
  if( pNew==0 ) return 0;
  pNew->nId = pNew->nAlloc = p->nId;
  pNew->a = sqlite3DbMallocRaw(db, p->nId*sizeof(p->a[0]) );
  if( pNew->a==0 ){
    sqlite3DbFree(db, pNew);
    return 0;
  }
  for(i=0; i<p->nId; i++){
    struct IdList_item *pNewItem = &pNew->a[i];
    struct IdList_item *pOldItem = &p->a[i];
    pNewItem->zName = sqlite3DbStrDup(db, pOldItem->zName);
    pNewItem->idx = pOldItem->idx;
  }
  return pNew;
}
Select *sqlite3SelectDup(sqlite3 *db, Select *p, int flags){
  Select *pNew;
  if( p==0 ) return 0;
  pNew = sqlite3DbMallocRaw(db, sizeof(*p) );
  if( pNew==0 ) return 0;
  /* Always make a copy of the span for top-level expressions in the
  ** expression list.  The logic in SELECT processing that determines
  ** the names of columns in the result set needs this information */
  pNew->pEList = sqlite3ExprListDup(db, p->pEList, flags|EXPRDUP_SPAN);
  pNew->pSrc = sqlite3SrcListDup(db, p->pSrc, flags);
  pNew->pWhere = sqlite3ExprDup(db, p->pWhere, flags);
  pNew->pGroupBy = sqlite3ExprListDup(db, p->pGroupBy, flags);
  pNew->pHaving = sqlite3ExprDup(db, p->pHaving, flags);
  pNew->pOrderBy = sqlite3ExprListDup(db, p->pOrderBy, flags);
  pNew->op = p->op;
  pNew->pPrior = sqlite3SelectDup(db, p->pPrior, flags);
  pNew->pLimit = sqlite3ExprDup(db, p->pLimit, flags);
  pNew->pOffset = sqlite3ExprDup(db, p->pOffset, flags);
  pNew->iLimit = 0;
  pNew->iOffset = 0;
  pNew->selFlags = p->selFlags & ~SF_UsesEphemeral;
  pNew->pRightmost = 0;
  pNew->addrOpenEphm[0] = -1;
  pNew->addrOpenEphm[1] = -1;
  pNew->addrOpenEphm[2] = -1;
  return pNew;
}
#else
Select *sqlite3SelectDup(sqlite3 *db, Select *p, int flags){
  assert( p==0 );
  return 0;
}
#endif


/*
** Add a new element to the end of an expression list.  If pList is
** initially NULL, then create a new expression list.
*/
ExprList *sqlite3ExprListAppend(
  Parse *pParse,          /* Parsing context */
  ExprList *pList,        /* List to which to append. Might be NULL */
  Expr *pExpr,            /* Expression to be appended */
  Token *pName            /* AS keyword for the expression */
){
  sqlite3 *db = pParse->db;
  if( pList==0 ){
    pList = sqlite3DbMallocZero(db, sizeof(ExprList) );
    if( pList==0 ){
      goto no_mem;
    }
    assert( pList->nAlloc==0 );
  }
  if( pList->nAlloc<=pList->nExpr ){
    struct ExprList_item *a;
    int n = pList->nAlloc*2 + 4;
    a = sqlite3DbRealloc(db, pList->a, n*sizeof(pList->a[0]));
    if( a==0 ){
      goto no_mem;
    }
    pList->a = a;
    pList->nAlloc = sqlite3DbMallocSize(db, a)/sizeof(a[0]);
  }
  assert( pList->a!=0 );
  if( pExpr || pName ){
    struct ExprList_item *pItem = &pList->a[pList->nExpr++];
    memset(pItem, 0, sizeof(*pItem));
    pItem->zName = sqlite3NameFromToken(db, pName);
    pItem->pExpr = pExpr;
    pItem->iAlias = 0;
  }
  return pList;

no_mem:     
  /* Avoid leaking memory if malloc has failed. */
  sqlite3ExprDelete(db, pExpr);
  sqlite3ExprListDelete(db, pList);
  return 0;
}

/*
** If the expression list pEList contains more than iLimit elements,
** leave an error message in pParse.
*/
void sqlite3ExprListCheckLength(
  Parse *pParse,
  ExprList *pEList,
  const char *zObject
){
  int mx = pParse->db->aLimit[SQLITE_LIMIT_COLUMN];
  testcase( pEList && pEList->nExpr==mx );
  testcase( pEList && pEList->nExpr==mx+1 );
  if( pEList && pEList->nExpr>mx ){
    sqlite3ErrorMsg(pParse, "too many columns in %s", zObject);
  }
}

/*
** Delete an entire expression list.
*/
void sqlite3ExprListDelete(sqlite3 *db, ExprList *pList){
  int i;
  struct ExprList_item *pItem;
  if( pList==0 ) return;
  assert( pList->a!=0 || (pList->nExpr==0 && pList->nAlloc==0) );
  assert( pList->nExpr<=pList->nAlloc );
  for(pItem=pList->a, i=0; i<pList->nExpr; i++, pItem++){
    sqlite3ExprDelete(db, pItem->pExpr);
    sqlite3DbFree(db, pItem->zName);
  }
  sqlite3DbFree(db, pList->a);
  sqlite3DbFree(db, pList);
}

/*
** These routines are Walker callbacks.  Walker.u.pi is a pointer
** to an integer.  These routines are checking an expression to see
** if it is a constant.  Set *Walker.u.pi to 0 if the expression is
** not constant.
**
** These callback routines are used to implement the following:
**
**     sqlite3ExprIsConstant()
**     sqlite3ExprIsConstantNotJoin()
**     sqlite3ExprIsConstantOrFunction()
**
*/
static int exprNodeIsConstant(Walker *pWalker, Expr *pExpr){

  /* If pWalker->u.i is 3 then any term of the expression that comes from
  ** the ON or USING clauses of a join disqualifies the expression
  ** from being considered constant. */
  if( pWalker->u.i==3 && ExprHasAnyProperty(pExpr, EP_FromJoin) ){
    pWalker->u.i = 0;
    return WRC_Abort;
  }

  switch( pExpr->op ){
    /* Consider functions to be constant if all their arguments are constant
    ** and pWalker->u.i==2 */
    case TK_FUNCTION:
      if( pWalker->u.i==2 ) return 0;
      /* Fall through */
    case TK_ID:
    case TK_COLUMN:
    case TK_AGG_FUNCTION:
    case TK_AGG_COLUMN:
#ifndef SQLITE_OMIT_SUBQUERY
    case TK_SELECT:
    case TK_EXISTS:
      testcase( pExpr->op==TK_SELECT );
      testcase( pExpr->op==TK_EXISTS );
#endif
      testcase( pExpr->op==TK_ID );
      testcase( pExpr->op==TK_COLUMN );
      testcase( pExpr->op==TK_AGG_FUNCTION );
      testcase( pExpr->op==TK_AGG_COLUMN );
      pWalker->u.i = 0;
      return WRC_Abort;
    default:
      return WRC_Continue;
  }
}
static int selectNodeIsConstant(Walker *pWalker, Select *NotUsed){
  UNUSED_PARAMETER(NotUsed);
  pWalker->u.i = 0;
  return WRC_Abort;
}
static int exprIsConst(Expr *p, int initFlag){
  Walker w;
  w.u.i = initFlag;
  w.xExprCallback = exprNodeIsConstant;
  w.xSelectCallback = selectNodeIsConstant;
  sqlite3WalkExpr(&w, p);
  return w.u.i;
}

/*
** Walk an expression tree.  Return 1 if the expression is constant
** and 0 if it involves variables or function calls.
**
** For the purposes of this function, a double-quoted string (ex: "abc")
** is considered a variable but a single-quoted string (ex: 'abc') is
** a constant.
*/
int sqlite3ExprIsConstant(Expr *p){
  return exprIsConst(p, 1);
}

/*
** Walk an expression tree.  Return 1 if the expression is constant
** that does no originate from the ON or USING clauses of a join.
** Return 0 if it involves variables or function calls or terms from
** an ON or USING clause.
*/
int sqlite3ExprIsConstantNotJoin(Expr *p){
  return exprIsConst(p, 3);
}

/*
** Walk an expression tree.  Return 1 if the expression is constant
** or a function call with constant arguments.  Return and 0 if there
** are any variables.
**
** For the purposes of this function, a double-quoted string (ex: "abc")
** is considered a variable but a single-quoted string (ex: 'abc') is
** a constant.
*/
int sqlite3ExprIsConstantOrFunction(Expr *p){
  return exprIsConst(p, 2);
}

/*
** If the expression p codes a constant integer that is small enough
** to fit in a 32-bit integer, return 1 and put the value of the integer
** in *pValue.  If the expression is not an integer or if it is too big
** to fit in a signed 32-bit integer, return 0 and leave *pValue unchanged.
*/
int sqlite3ExprIsInteger(Expr *p, int *pValue){
  int rc = 0;
  if( p->flags & EP_IntValue ){
    *pValue = p->iTable;
    return 1;
  }
  switch( p->op ){
    case TK_INTEGER: {
      rc = sqlite3GetInt32((char*)p->token.z, pValue);
      break;
    }
    case TK_UPLUS: {
      rc = sqlite3ExprIsInteger(p->pLeft, pValue);
      break;
    }
    case TK_UMINUS: {
      int v;
      if( sqlite3ExprIsInteger(p->pLeft, &v) ){
        *pValue = -v;
        rc = 1;
      }
      break;
    }
    default: break;
  }
  if( rc ){
    p->op = TK_INTEGER;
    p->flags |= EP_IntValue;
    p->iTable = *pValue;
  }
  return rc;
}

/*
** Return TRUE if the given string is a row-id column name.
*/
int sqlite3IsRowid(const char *z){
  if( sqlite3StrICmp(z, "_ROWID_")==0 ) return 1;
  if( sqlite3StrICmp(z, "ROWID")==0 ) return 1;
  if( sqlite3StrICmp(z, "OID")==0 ) return 1;
  return 0;
}

/*
** Return true if the IN operator optimization is enabled and
** the SELECT statement p exists and is of the
** simple form:
**
**     SELECT <column> FROM <table>
**
** If this is the case, it may be possible to use an existing table
** or index instead of generating an epheremal table.
*/
#ifndef SQLITE_OMIT_SUBQUERY
static int isCandidateForInOpt(Select *p){
  SrcList *pSrc;
  ExprList *pEList;
  Table *pTab;
  if( p==0 ) return 0;                   /* right-hand side of IN is SELECT */
  if( p->pPrior ) return 0;              /* Not a compound SELECT */
  if( p->selFlags & (SF_Distinct|SF_Aggregate) ){
      return 0; /* No DISTINCT keyword and no aggregate functions */
  }
  if( p->pGroupBy ) return 0;            /* Has no GROUP BY clause */
  if( p->pLimit ) return 0;              /* Has no LIMIT clause */
  if( p->pOffset ) return 0;
  if( p->pWhere ) return 0;              /* Has no WHERE clause */
  pSrc = p->pSrc;
  assert( pSrc!=0 );
  if( pSrc->nSrc!=1 ) return 0;          /* Single term in FROM clause */
  if( pSrc->a[0].pSelect ) return 0;     /* FROM clause is not a subquery */
  pTab = pSrc->a[0].pTab;
  if( pTab==0 ) return 0;
  if( pTab->pSelect ) return 0;          /* FROM clause is not a view */
  if( IsVirtual(pTab) ) return 0;        /* FROM clause not a virtual table */
  pEList = p->pEList;
  if( pEList->nExpr!=1 ) return 0;       /* One column in the result set */
  if( pEList->a[0].pExpr->op!=TK_COLUMN ) return 0; /* Result is a column */
  return 1;
}
#endif /* SQLITE_OMIT_SUBQUERY */

/*
** This function is used by the implementation of the IN (...) operator.
** It's job is to find or create a b-tree structure that may be used
** either to test for membership of the (...) set or to iterate through
** its members, skipping duplicates.
**
** The cursor opened on the structure (database table, database index 
** or ephermal table) is stored in pX->iTable before this function returns.
** The returned value indicates the structure type, as follows:
**
**   IN_INDEX_ROWID - The cursor was opened on a database table.
**   IN_INDEX_INDEX - The cursor was opened on a database index.
**   IN_INDEX_EPH -   The cursor was opened on a specially created and
**                    populated epheremal table.
**
** An existing structure may only be used if the SELECT is of the simple
** form:
**
**     SELECT <column> FROM <table>
**
** If prNotFound parameter is 0, then the structure will be used to iterate
** through the set members, skipping any duplicates. In this case an
** epheremal table must be used unless the selected <column> is guaranteed
** to be unique - either because it is an INTEGER PRIMARY KEY or it
** is unique by virtue of a constraint or implicit index.
**
** If the prNotFound parameter is not 0, then the structure will be used 
** for fast set membership tests. In this case an epheremal table must 
** be used unless <column> is an INTEGER PRIMARY KEY or an index can 
** be found with <column> as its left-most column.
**
** When the structure is being used for set membership tests, the user
** needs to know whether or not the structure contains an SQL NULL 
** value in order to correctly evaluate expressions like "X IN (Y, Z)".
** If there is a chance that the structure may contain a NULL value at
** runtime, then a register is allocated and the register number written
** to *prNotFound. If there is no chance that the structure contains a
** NULL value, then *prNotFound is left unchanged.
**
** If a register is allocated and its location stored in *prNotFound, then
** its initial value is NULL. If the structure does not remain constant
** for the duration of the query (i.e. the set is a correlated sub-select), 
** the value of the allocated register is reset to NULL each time the 
** structure is repopulated. This allows the caller to use vdbe code 
** equivalent to the following:
**
**   if( register==NULL ){
**     has_null = <test if data structure contains null>
**     register = 1
**   }
**
** in order to avoid running the <test if data structure contains null>
** test more often than is necessary.
*/
#ifndef SQLITE_OMIT_SUBQUERY
int sqlite3FindInIndex(Parse *pParse, Expr *pX, int *prNotFound){
  Select *p;
  int eType = 0;
  int iTab = pParse->nTab++;
  int mustBeUnique = !prNotFound;

  /* The follwing if(...) expression is true if the SELECT is of the 
  ** simple form:
  **
  **     SELECT <column> FROM <table>
  **
  ** If this is the case, it may be possible to use an existing table
  ** or index instead of generating an epheremal table.
  */
  p = (ExprHasProperty(pX, EP_xIsSelect) ? pX->x.pSelect : 0);
  if( isCandidateForInOpt(p) ){
    sqlite3 *db = pParse->db;              /* Database connection */
    Expr *pExpr = p->pEList->a[0].pExpr;   /* Expression <column> */
    int iCol = pExpr->iColumn;             /* Index of column <column> */
    Vdbe *v = sqlite3GetVdbe(pParse);      /* Virtual machine being coded */
    Table *pTab = p->pSrc->a[0].pTab;      /* Table <table>. */
    int iDb;                               /* Database idx for pTab */
   
    /* Code an OP_VerifyCookie and OP_TableLock for <table>. */
    iDb = sqlite3SchemaToIndex(db, pTab->pSchema);
    sqlite3CodeVerifySchema(pParse, iDb);
    sqlite3TableLock(pParse, iDb, pTab->tnum, 0, pTab->zName);

    /* This function is only called from two places. In both cases the vdbe
    ** has already been allocated. So assume sqlite3GetVdbe() is always
    ** successful here.
    */
    assert(v);
    if( iCol<0 ){
      int iMem = ++pParse->nMem;
      int iAddr;
      sqlite3VdbeUsesBtree(v, iDb);

      iAddr = sqlite3VdbeAddOp1(v, OP_If, iMem);
      sqlite3VdbeAddOp2(v, OP_Integer, 1, iMem);

      sqlite3OpenTable(pParse, iTab, iDb, pTab, OP_OpenRead);
      eType = IN_INDEX_ROWID;

      sqlite3VdbeJumpHere(v, iAddr);
    }else{
      Index *pIdx;                         /* Iterator variable */

      /* The collation sequence used by the comparison. If an index is to 
      ** be used in place of a temp-table, it must be ordered according
      ** to this collation sequence.  */
      CollSeq *pReq = sqlite3BinaryCompareCollSeq(pParse, pX->pLeft, pExpr);

      /* Check that the affinity that will be used to perform the 
      ** comparison is the same as the affinity of the column. If
      ** it is not, it is not possible to use any index.
      */
      char aff = comparisonAffinity(pX);
      int affinity_ok = (pTab->aCol[iCol].affinity==aff||aff==SQLITE_AFF_NONE);

      for(pIdx=pTab->pIndex; pIdx && eType==0 && affinity_ok; pIdx=pIdx->pNext){
        if( (pIdx->aiColumn[0]==iCol)
         && (pReq==sqlite3FindCollSeq(db, ENC(db), pIdx->azColl[0], -1, 0))
         && (!mustBeUnique || (pIdx->nColumn==1 && pIdx->onError!=OE_None))
        ){
          int iMem = ++pParse->nMem;
          int iAddr;
          char *pKey;
  
          pKey = (char *)sqlite3IndexKeyinfo(pParse, pIdx);
          iDb = sqlite3SchemaToIndex(db, pIdx->pSchema);
          sqlite3VdbeUsesBtree(v, iDb);

          iAddr = sqlite3VdbeAddOp1(v, OP_If, iMem);
          sqlite3VdbeAddOp2(v, OP_Integer, 1, iMem);
  
          sqlite3VdbeAddOp4(v, OP_OpenRead, iTab, pIdx->tnum, iDb,
                               pKey,P4_KEYINFO_HANDOFF);
          VdbeComment((v, "%s", pIdx->zName));
          eType = IN_INDEX_INDEX;

          sqlite3VdbeJumpHere(v, iAddr);
          if( prNotFound && !pTab->aCol[iCol].notNull ){
            *prNotFound = ++pParse->nMem;
          }
        }
      }
    }
  }

  if( eType==0 ){
    int rMayHaveNull = 0;
    eType = IN_INDEX_EPH;
    if( prNotFound ){
      *prNotFound = rMayHaveNull = ++pParse->nMem;
    }else if( pX->pLeft->iColumn<0 && !ExprHasAnyProperty(pX, EP_xIsSelect) ){
      eType = IN_INDEX_ROWID;
    }
    sqlite3CodeSubselect(pParse, pX, rMayHaveNull, eType==IN_INDEX_ROWID);
  }else{
    pX->iTable = iTab;
  }
  return eType;
}
#endif

/*
** Generate code for scalar subqueries used as an expression
** and IN operators.  Examples:
**
**     (SELECT a FROM b)          -- subquery
**     EXISTS (SELECT a FROM b)   -- EXISTS subquery
**     x IN (4,5,11)              -- IN operator with list on right-hand side
**     x IN (SELECT a FROM b)     -- IN operator with subquery on the right
**
** The pExpr parameter describes the expression that contains the IN
** operator or subquery.
**
** If parameter isRowid is non-zero, then expression pExpr is guaranteed
** to be of the form "<rowid> IN (?, ?, ?)", where <rowid> is a reference
** to some integer key column of a table B-Tree. In this case, use an
** intkey B-Tree to store the set of IN(...) values instead of the usual
** (slower) variable length keys B-Tree.
*/
#ifndef SQLITE_OMIT_SUBQUERY
void sqlite3CodeSubselect(
  Parse *pParse, 
  Expr *pExpr, 
  int rMayHaveNull,
  int isRowid
){
  int testAddr = 0;                       /* One-time test address */
  Vdbe *v = sqlite3GetVdbe(pParse);
  if( v==0 ) return;
  sqlite3ExprCachePush(pParse);

  /* This code must be run in its entirety every time it is encountered
  ** if any of the following is true:
  **
  **    *  The right-hand side is a correlated subquery
  **    *  The right-hand side is an expression list containing variables
  **    *  We are inside a trigger
  **
  ** If all of the above are false, then we can run this code just once
  ** save the results, and reuse the same result on subsequent invocations.
  */
  if( !ExprHasAnyProperty(pExpr, EP_VarSelect) && !pParse->trigStack ){
    int mem = ++pParse->nMem;
    sqlite3VdbeAddOp1(v, OP_If, mem);
    testAddr = sqlite3VdbeAddOp2(v, OP_Integer, 1, mem);
    assert( testAddr>0 || pParse->db->mallocFailed );
  }

  switch( pExpr->op ){
    case TK_IN: {
      char affinity;
      KeyInfo keyInfo;
      int addr;        /* Address of OP_OpenEphemeral instruction */
      Expr *pLeft = pExpr->pLeft;

      if( rMayHaveNull ){
        sqlite3VdbeAddOp2(v, OP_Null, 0, rMayHaveNull);
      }

      affinity = sqlite3ExprAffinity(pLeft);

      /* Whether this is an 'x IN(SELECT...)' or an 'x IN(<exprlist>)'
      ** expression it is handled the same way. A virtual table is 
      ** filled with single-field index keys representing the results
      ** from the SELECT or the <exprlist>.
      **
      ** If the 'x' expression is a column value, or the SELECT...
      ** statement returns a column value, then the affinity of that
      ** column is used to build the index keys. If both 'x' and the
      ** SELECT... statement are columns, then numeric affinity is used
      ** if either column has NUMERIC or INTEGER affinity. If neither
      ** 'x' nor the SELECT... statement are columns, then numeric affinity
      ** is used.
      */
      pExpr->iTable = pParse->nTab++;
      addr = sqlite3VdbeAddOp2(v, OP_OpenEphemeral, pExpr->iTable, !isRowid);
      memset(&keyInfo, 0, sizeof(keyInfo));
      keyInfo.nField = 1;

      if( ExprHasProperty(pExpr, EP_xIsSelect) ){
        /* Case 1:     expr IN (SELECT ...)
        **
        ** Generate code to write the results of the select into the temporary
        ** table allocated and opened above.
        */
        SelectDest dest;
        ExprList *pEList;

        assert( !isRowid );
        sqlite3SelectDestInit(&dest, SRT_Set, pExpr->iTable);
        dest.affinity = (u8)affinity;
        assert( (pExpr->iTable&0x0000FFFF)==pExpr->iTable );
        if( sqlite3Select(pParse, pExpr->x.pSelect, &dest) ){
          return;
        }
        pEList = pExpr->x.pSelect->pEList;
        if( pEList && pEList->nExpr>0 ){ 
          keyInfo.aColl[0] = sqlite3BinaryCompareCollSeq(pParse, pExpr->pLeft,
              pEList->a[0].pExpr);
        }
      }else if( pExpr->x.pList ){
        /* Case 2:     expr IN (exprlist)
        **
        ** For each expression, build an index key from the evaluation and
        ** store it in the temporary table. If <expr> is a column, then use
        ** that columns affinity when building index keys. If <expr> is not
        ** a column, use numeric affinity.
        */
        int i;
        ExprList *pList = pExpr->x.pList;
        struct ExprList_item *pItem;
        int r1, r2, r3;

        if( !affinity ){
          affinity = SQLITE_AFF_NONE;
        }
        keyInfo.aColl[0] = sqlite3ExprCollSeq(pParse, pExpr->pLeft);

        /* Loop through each expression in <exprlist>. */
        r1 = sqlite3GetTempReg(pParse);
        r2 = sqlite3GetTempReg(pParse);
        sqlite3VdbeAddOp2(v, OP_Null, 0, r2);
        for(i=pList->nExpr, pItem=pList->a; i>0; i--, pItem++){
          Expr *pE2 = pItem->pExpr;

          /* If the expression is not constant then we will need to
          ** disable the test that was generated above that makes sure
          ** this code only executes once.  Because for a non-constant
          ** expression we need to rerun this code each time.
          */
          if( testAddr && !sqlite3ExprIsConstant(pE2) ){
            sqlite3VdbeChangeToNoop(v, testAddr-1, 2);
            testAddr = 0;
          }

          /* Evaluate the expression and insert it into the temp table */
          r3 = sqlite3ExprCodeTarget(pParse, pE2, r1);
          if( isRowid ){
            sqlite3VdbeAddOp2(v, OP_MustBeInt, r3, sqlite3VdbeCurrentAddr(v)+2);
            sqlite3VdbeAddOp3(v, OP_Insert, pExpr->iTable, r2, r3);
          }else{
            sqlite3VdbeAddOp4(v, OP_MakeRecord, r3, 1, r2, &affinity, 1);
            sqlite3ExprCacheAffinityChange(pParse, r3, 1);
            sqlite3VdbeAddOp2(v, OP_IdxInsert, pExpr->iTable, r2);
          }
        }
        sqlite3ReleaseTempReg(pParse, r1);
        sqlite3ReleaseTempReg(pParse, r2);
      }
      if( !isRowid ){
        sqlite3VdbeChangeP4(v, addr, (void *)&keyInfo, P4_KEYINFO);
      }
      break;
    }

    case TK_EXISTS:
    case TK_SELECT: {
      /* This has to be a scalar SELECT.  Generate code to put the
      ** value of this select in a memory cell and record the number
      ** of the memory cell in iColumn.
      */
      static const Token one = { (u8*)"1", 0, 0, 1 };
      Select *pSel;
      SelectDest dest;

      assert( ExprHasProperty(pExpr, EP_xIsSelect) );
      pSel = pExpr->x.pSelect;
      sqlite3SelectDestInit(&dest, 0, ++pParse->nMem);
      if( pExpr->op==TK_SELECT ){
        dest.eDest = SRT_Mem;
        sqlite3VdbeAddOp2(v, OP_Null, 0, dest.iParm);
        VdbeComment((v, "Init subquery result"));
      }else{
        dest.eDest = SRT_Exists;
        sqlite3VdbeAddOp2(v, OP_Integer, 0, dest.iParm);
        VdbeComment((v, "Init EXISTS result"));
      }
      sqlite3ExprDelete(pParse->db, pSel->pLimit);
      pSel->pLimit = sqlite3PExpr(pParse, TK_INTEGER, 0, 0, &one);
      if( sqlite3Select(pParse, pSel, &dest) ){
        return;
      }
      pExpr->iColumn = dest.iParm;
      break;
    }
  }

  if( testAddr ){
    sqlite3VdbeJumpHere(v, testAddr-1);
  }
  sqlite3ExprCachePop(pParse, 1);

  return;
}
#endif /* SQLITE_OMIT_SUBQUERY */

/*
** Duplicate an 8-byte value
*/
static char *dup8bytes(Vdbe *v, const char *in){
  char *out = sqlite3DbMallocRaw(sqlite3VdbeDb(v), 8);
  if( out ){
    memcpy(out, in, 8);
  }
  return out;
}

/*
** Generate an instruction that will put the floating point
** value described by z[0..n-1] into register iMem.
**
** The z[] string will probably not be zero-terminated.  But the 
** z[n] character is guaranteed to be something that does not look
** like the continuation of the number.
*/
static void codeReal(Vdbe *v, const char *z, int n, int negateFlag, int iMem){
  assert( z || v==0 || sqlite3VdbeDb(v)->mallocFailed );
  assert( !z || !sqlite3Isdigit(z[n]) );
  UNUSED_PARAMETER(n);
  if( z ){
    double value;
    char *zV;
    sqlite3AtoF(z, &value);
    if( sqlite3IsNaN(value) ){
      sqlite3VdbeAddOp2(v, OP_Null, 0, iMem);
    }else{
      if( negateFlag ) value = -value;
      zV = dup8bytes(v, (char*)&value);
      sqlite3VdbeAddOp4(v, OP_Real, 0, iMem, 0, zV, P4_REAL);
    }
  }
}


/*
** Generate an instruction that will put the integer describe by
** text z[0..n-1] into register iMem.
**
** The z[] string will probably not be zero-terminated.  But the 
** z[n] character is guaranteed to be something that does not look
** like the continuation of the number.
*/
static void codeInteger(Vdbe *v, Expr *pExpr, int negFlag, int iMem){
  const char *z;
  if( pExpr->flags & EP_IntValue ){
    int i = pExpr->iTable;
    if( negFlag ) i = -i;
    sqlite3VdbeAddOp2(v, OP_Integer, i, iMem);
  }else if( (z = (char*)pExpr->token.z)!=0 ){
    int i;
    int n = pExpr->token.n;
    assert( !sqlite3Isdigit(z[n]) );
    if( sqlite3GetInt32(z, &i) ){
      if( negFlag ) i = -i;
      sqlite3VdbeAddOp2(v, OP_Integer, i, iMem);
    }else if( sqlite3FitsIn64Bits(z, negFlag) ){
      i64 value;
      char *zV;
      sqlite3Atoi64(z, &value);
      if( negFlag ) value = -value;
      zV = dup8bytes(v, (char*)&value);
      sqlite3VdbeAddOp4(v, OP_Int64, 0, iMem, 0, zV, P4_INT64);
    }else{
      codeReal(v, z, n, negFlag, iMem);
    }
  }
}

/*
** Clear a cache entry.
*/
static void cacheEntryClear(Parse *pParse, struct yColCache *p){
  if( p->tempReg ){
    if( pParse->nTempReg<ArraySize(pParse->aTempReg) ){
      pParse->aTempReg[pParse->nTempReg++] = p->iReg;
    }
    p->tempReg = 0;
  }
}


/*
** Record in the column cache that a particular column from a
** particular table is stored in a particular register.
*/
void sqlite3ExprCacheStore(Parse *pParse, int iTab, int iCol, int iReg){
  int i;
  int minLru;
  int idxLru;
  struct yColCache *p;

  /* First replace any existing entry */
  for(i=0, p=pParse->aColCache; i<SQLITE_N_COLCACHE; i++, p++){
    if( p->iReg && p->iTable==iTab && p->iColumn==iCol ){
      cacheEntryClear(pParse, p);
      p->iLevel = pParse->iCacheLevel;
      p->iReg = iReg;
      p->affChange = 0;
      p->lru = pParse->iCacheCnt++;
      return;
    }
  }
  if( iReg<=0 ) return;

  /* Find an empty slot and replace it */
  for(i=0, p=pParse->aColCache; i<SQLITE_N_COLCACHE; i++, p++){
    if( p->iReg==0 ){
      p->iLevel = pParse->iCacheLevel;
      p->iTable = iTab;
      p->iColumn = iCol;
      p->iReg = iReg;
      p->affChange = 0;
      p->tempReg = 0;
      p->lru = pParse->iCacheCnt++;
      return;
    }
  }

  /* Replace the last recently used */
  minLru = 0x7fffffff;
  idxLru = -1;
  for(i=0, p=pParse->aColCache; i<SQLITE_N_COLCACHE; i++, p++){
    if( p->lru<minLru ){
      idxLru = i;
      minLru = p->lru;
    }
  }
  if( idxLru>=0 ){
    p = &pParse->aColCache[idxLru];
    p->iLevel = pParse->iCacheLevel;
    p->iTable = iTab;
    p->iColumn = iCol;
    p->iReg = iReg;
    p->affChange = 0;
    p->tempReg = 0;
    p->lru = pParse->iCacheCnt++;
    return;
  }
}

/*
** Indicate that a register is being overwritten.  Purge the register
** from the column cache.
*/
void sqlite3ExprCacheRemove(Parse *pParse, int iReg){
  int i;
  struct yColCache *p;
  for(i=0, p=pParse->aColCache; i<SQLITE_N_COLCACHE; i++, p++){
    if( p->iReg==iReg ){
      cacheEntryClear(pParse, p);
      p->iReg = 0;
    }
  }
}

/*
** Remember the current column cache context.  Any new entries added
** added to the column cache after this call are removed when the
** corresponding pop occurs.
*/
void sqlite3ExprCachePush(Parse *pParse){
  pParse->iCacheLevel++;
}

/*
** Remove from the column cache any entries that were added since the
** the previous N Push operations.  In other words, restore the cache
** to the state it was in N Pushes ago.
*/
void sqlite3ExprCachePop(Parse *pParse, int N){
  int i;
  struct yColCache *p;
  assert( N>0 );
  assert( pParse->iCacheLevel>=N );
  pParse->iCacheLevel -= N;
  for(i=0, p=pParse->aColCache; i<SQLITE_N_COLCACHE; i++, p++){
    if( p->iReg && p->iLevel>pParse->iCacheLevel ){
      cacheEntryClear(pParse, p);
      p->iReg = 0;
    }
  }
}

/*
** Generate code that will extract the iColumn-th column from
** table pTab and store the column value in a register.  An effort
** is made to store the column value in register iReg, but this is
** not guaranteed.  The location of the column value is returned.
**
** There must be an open cursor to pTab in iTable when this routine
** is called.  If iColumn<0 then code is generated that extracts the rowid.
**
** This routine might attempt to reuse the value of the column that
** has already been loaded into a register.  The value will always
** be used if it has not undergone any affinity changes.  But if
** an affinity change has occurred, then the cached value will only be
** used if allowAffChng is true.
*/
int sqlite3ExprCodeGetColumn(
  Parse *pParse,   /* Parsing and code generating context */
  Table *pTab,     /* Description of the table we are reading from */
  int iColumn,     /* Index of the table column */
  int iTable,      /* The cursor pointing to the table */
  int iReg,        /* Store results here */
  int allowAffChng /* True if prior affinity changes are OK */
){
  Vdbe *v = pParse->pVdbe;
  int i;
  struct yColCache *p;

  for(i=0, p=pParse->aColCache; i<SQLITE_N_COLCACHE; i++, p++){
    if( p->iReg>0 && p->iTable==iTable && p->iColumn==iColumn
           && (!p->affChange || allowAffChng) ){
#if 0
      sqlite3VdbeAddOp0(v, OP_Noop);
      VdbeComment((v, "OPT: tab%d.col%d -> r%d", iTable, iColumn, p->iReg));
#endif
      p->lru = pParse->iCacheCnt++;
      p->tempReg = 0;  /* This pins the register, but also leaks it */
      return p->iReg;
    }
  }  
  assert( v!=0 );
  if( iColumn<0 ){
    sqlite3VdbeAddOp2(v, OP_Rowid, iTable, iReg);
  }else if( pTab==0 ){
    sqlite3VdbeAddOp3(v, OP_Column, iTable, iColumn, iReg);
  }else{
    int op = IsVirtual(pTab) ? OP_VColumn : OP_Column;
    sqlite3VdbeAddOp3(v, op, iTable, iColumn, iReg);
    sqlite3ColumnDefault(v, pTab, iColumn);
#ifndef SQLITE_OMIT_FLOATING_POINT
    if( pTab->aCol[iColumn].affinity==SQLITE_AFF_REAL ){
      sqlite3VdbeAddOp1(v, OP_RealAffinity, iReg);
    }
#endif
  }
  sqlite3ExprCacheStore(pParse, iTable, iColumn, iReg);
  return iReg;
}

/*
** Clear all column cache entries.
*/
void sqlite3ExprCacheClear(Parse *pParse){
  int i;
  struct yColCache *p;

  for(i=0, p=pParse->aColCache; i<SQLITE_N_COLCACHE; i++, p++){
    if( p->iReg ){
      cacheEntryClear(pParse, p);
      p->iReg = 0;
    }
  }
}

/*
** Record the fact that an affinity change has occurred on iCount
** registers starting with iStart.
*/
void sqlite3ExprCacheAffinityChange(Parse *pParse, int iStart, int iCount){
  int iEnd = iStart + iCount - 1;
  int i;
  struct yColCache *p;
  for(i=0, p=pParse->aColCache; i<SQLITE_N_COLCACHE; i++, p++){
    int r = p->iReg;
    if( r>=iStart && r<=iEnd ){
      p->affChange = 1;
    }
  }
}

/*
** Generate code to move content from registers iFrom...iFrom+nReg-1
** over to iTo..iTo+nReg-1. Keep the column cache up-to-date.
*/
void sqlite3ExprCodeMove(Parse *pParse, int iFrom, int iTo, int nReg){
  int i;
  struct yColCache *p;
  if( iFrom==iTo ) return;
  sqlite3VdbeAddOp3(pParse->pVdbe, OP_Move, iFrom, iTo, nReg);
  for(i=0, p=pParse->aColCache; i<SQLITE_N_COLCACHE; i++, p++){
    int x = p->iReg;
    if( x>=iFrom && x<iFrom+nReg ){
      p->iReg += iTo-iFrom;
    }
  }
}

/*
** Generate code to copy content from registers iFrom...iFrom+nReg-1
** over to iTo..iTo+nReg-1.
*/
void sqlite3ExprCodeCopy(Parse *pParse, int iFrom, int iTo, int nReg){
  int i;
  if( iFrom==iTo ) return;
  for(i=0; i<nReg; i++){
    sqlite3VdbeAddOp2(pParse->pVdbe, OP_Copy, iFrom+i, iTo+i);
  }
}

/*
** Return true if any register in the range iFrom..iTo (inclusive)
** is used as part of the column cache.
*/
static int usedAsColumnCache(Parse *pParse, int iFrom, int iTo){
  int i;
  struct yColCache *p;
  for(i=0, p=pParse->aColCache; i<SQLITE_N_COLCACHE; i++, p++){
    int r = p->iReg;
    if( r>=iFrom && r<=iTo ) return 1;
  }
  return 0;
}

/*
** If the last instruction coded is an ephemeral copy of any of
** the registers in the nReg registers beginning with iReg, then
** convert the last instruction from OP_SCopy to OP_Copy.
*/
void sqlite3ExprHardCopy(Parse *pParse, int iReg, int nReg){
  int addr;
  VdbeOp *pOp;
  Vdbe *v;

  v = pParse->pVdbe;
  addr = sqlite3VdbeCurrentAddr(v);
  pOp = sqlite3VdbeGetOp(v, addr-1);
  assert( pOp || pParse->db->mallocFailed );
  if( pOp && pOp->opcode==OP_SCopy && pOp->p1>=iReg && pOp->p1<iReg+nReg ){
    pOp->opcode = OP_Copy;
  }
}

/*
** Generate code to store the value of the iAlias-th alias in register
** target.  The first time this is called, pExpr is evaluated to compute
** the value of the alias.  The value is stored in an auxiliary register
** and the number of that register is returned.  On subsequent calls,
** the register number is returned without generating any code.
**
** Note that in order for this to work, code must be generated in the
** same order that it is executed.
**
** Aliases are numbered starting with 1.  So iAlias is in the range
** of 1 to pParse->nAlias inclusive.  
**
** pParse->aAlias[iAlias-1] records the register number where the value
** of the iAlias-th alias is stored.  If zero, that means that the
** alias has not yet been computed.
*/
static int codeAlias(Parse *pParse, int iAlias, Expr *pExpr, int target){
#if 0
  sqlite3 *db = pParse->db;
  int iReg;
  if( pParse->nAliasAlloc<pParse->nAlias ){
    pParse->aAlias = sqlite3DbReallocOrFree(db, pParse->aAlias,
                                 sizeof(pParse->aAlias[0])*pParse->nAlias );
    testcase( db->mallocFailed && pParse->nAliasAlloc>0 );
    if( db->mallocFailed ) return 0;
    memset(&pParse->aAlias[pParse->nAliasAlloc], 0,
           (pParse->nAlias-pParse->nAliasAlloc)*sizeof(pParse->aAlias[0]));
    pParse->nAliasAlloc = pParse->nAlias;
  }
  assert( iAlias>0 && iAlias<=pParse->nAlias );
  iReg = pParse->aAlias[iAlias-1];
  if( iReg==0 ){
    if( pParse->iCacheLevel>0 ){
      iReg = sqlite3ExprCodeTarget(pParse, pExpr, target);
    }else{
      iReg = ++pParse->nMem;
      sqlite3ExprCode(pParse, pExpr, iReg);
      pParse->aAlias[iAlias-1] = iReg;
    }
  }
  return iReg;
#else
  UNUSED_PARAMETER(iAlias);
  return sqlite3ExprCodeTarget(pParse, pExpr, target);
#endif
}

/*
** Generate code into the current Vdbe to evaluate the given
** expression.  Attempt to store the results in register "target".
** Return the register where results are stored.
**
** With this routine, there is no guarantee that results will
** be stored in target.  The result might be stored in some other
** register if it is convenient to do so.  The calling function
** must check the return code and move the results to the desired
** register.
*/
int sqlite3ExprCodeTarget(Parse *pParse, Expr *pExpr, int target){
  Vdbe *v = pParse->pVdbe;  /* The VM under construction */
  int op;                   /* The opcode being coded */
  int inReg = target;       /* Results stored in register inReg */
  int regFree1 = 0;         /* If non-zero free this temporary register */
  int regFree2 = 0;         /* If non-zero free this temporary register */
  int r1, r2, r3, r4;       /* Various register numbers */
  sqlite3 *db;

  db = pParse->db;
  assert( v!=0 || db->mallocFailed );
  assert( target>0 && target<=pParse->nMem );
  if( v==0 ) return 0;

  if( pExpr==0 ){
    op = TK_NULL;
  }else{
    op = pExpr->op;
  }
  switch( op ){
    case TK_AGG_COLUMN: {
      AggInfo *pAggInfo = pExpr->pAggInfo;
      struct AggInfo_col *pCol = &pAggInfo->aCol[pExpr->iAgg];
      if( !pAggInfo->directMode ){
        assert( pCol->iMem>0 );
        inReg = pCol->iMem;
        break;
      }else if( pAggInfo->useSortingIdx ){
        sqlite3VdbeAddOp3(v, OP_Column, pAggInfo->sortingIdx,
                              pCol->iSorterColumn, target);
        break;
      }
      /* Otherwise, fall thru into the TK_COLUMN case */
    }
    case TK_COLUMN: {
      if( pExpr->iTable<0 ){
        /* This only happens when coding check constraints */
        assert( pParse->ckBase>0 );
        inReg = pExpr->iColumn + pParse->ckBase;
      }else{
        testcase( (pExpr->flags & EP_AnyAff)!=0 );
        inReg = sqlite3ExprCodeGetColumn(pParse, pExpr->pTab,
                                 pExpr->iColumn, pExpr->iTable, target,
                                 pExpr->flags & EP_AnyAff);
      }
      break;
    }
    case TK_INTEGER: {
      codeInteger(v, pExpr, 0, target);
      break;
    }
    case TK_FLOAT: {
      codeReal(v, (char*)pExpr->token.z, pExpr->token.n, 0, target);
      break;
    }
    case TK_STRING: {
      sqlite3VdbeAddOp4(v, OP_String8, 0, target, 0,
                        (char*)pExpr->token.z, pExpr->token.n);
      break;
    }
    case TK_NULL: {
      sqlite3VdbeAddOp2(v, OP_Null, 0, target);
      break;
    }
#ifndef SQLITE_OMIT_BLOB_LITERAL
    case TK_BLOB: {
      int n;
      const char *z;
      char *zBlob;
      assert( pExpr->token.n>=3 );
      assert( pExpr->token.z[0]=='x' || pExpr->token.z[0]=='X' );
      assert( pExpr->token.z[1]=='\'' );
      assert( pExpr->token.z[pExpr->token.n-1]=='\'' );
      n = pExpr->token.n - 3;
      z = (char*)pExpr->token.z + 2;
      zBlob = sqlite3HexToBlob(sqlite3VdbeDb(v), z, n);
      sqlite3VdbeAddOp4(v, OP_Blob, n/2, target, 0, zBlob, P4_DYNAMIC);
      break;
    }
#endif
    case TK_VARIABLE: {
      int iPrior;
      VdbeOp *pOp;
      if( pExpr->token.n<=1
         && (iPrior = sqlite3VdbeCurrentAddr(v)-1)>=0
         && (pOp = sqlite3VdbeGetOp(v, iPrior))->opcode==OP_Variable
         && pOp->p1+pOp->p3==pExpr->iTable
         && pOp->p2+pOp->p3==target
         && pOp->p4.z==0
      ){
        /* If the previous instruction was a copy of the previous unnamed
        ** parameter into the previous register, then simply increment the
        ** repeat count on the prior instruction rather than making a new
        ** instruction.
        */
        pOp->p3++;
      }else{
        sqlite3VdbeAddOp3(v, OP_Variable, pExpr->iTable, target, 1);
        if( pExpr->token.n>1 ){
          sqlite3VdbeChangeP4(v, -1, (char*)pExpr->token.z, pExpr->token.n);
        }
      }
      break;
    }
    case TK_REGISTER: {
      inReg = pExpr->iTable;
      break;
    }
    case TK_AS: {
      inReg = codeAlias(pParse, pExpr->iTable, pExpr->pLeft, target);
      break;
    }
#ifndef SQLITE_OMIT_CAST
    case TK_CAST: {
      /* Expressions of the form:   CAST(pLeft AS token) */
      int aff, to_op;
      inReg = sqlite3ExprCodeTarget(pParse, pExpr->pLeft, target);
      aff = sqlite3AffinityType(&pExpr->token);
      to_op = aff - SQLITE_AFF_TEXT + OP_ToText;
      assert( to_op==OP_ToText    || aff!=SQLITE_AFF_TEXT    );
      assert( to_op==OP_ToBlob    || aff!=SQLITE_AFF_NONE    );
      assert( to_op==OP_ToNumeric || aff!=SQLITE_AFF_NUMERIC );
      assert( to_op==OP_ToInt     || aff!=SQLITE_AFF_INTEGER );
      assert( to_op==OP_ToReal    || aff!=SQLITE_AFF_REAL    );
      testcase( to_op==OP_ToText );
      testcase( to_op==OP_ToBlob );
      testcase( to_op==OP_ToNumeric );
      testcase( to_op==OP_ToInt );
      testcase( to_op==OP_ToReal );
      if( inReg!=target ){
        sqlite3VdbeAddOp2(v, OP_SCopy, inReg, target);
        inReg = target;
      }
      sqlite3VdbeAddOp1(v, to_op, inReg);
      testcase( usedAsColumnCache(pParse, inReg, inReg) );
      sqlite3ExprCacheAffinityChange(pParse, inReg, 1);
      break;
    }
#endif /* SQLITE_OMIT_CAST */
    case TK_LT:
    case TK_LE:
    case TK_GT:
    case TK_GE:
    case TK_NE:
    case TK_EQ: {
      assert( TK_LT==OP_Lt );
      assert( TK_LE==OP_Le );
      assert( TK_GT==OP_Gt );
      assert( TK_GE==OP_Ge );
      assert( TK_EQ==OP_Eq );
      assert( TK_NE==OP_Ne );
      testcase( op==TK_LT );
      testcase( op==TK_LE );
      testcase( op==TK_GT );
      testcase( op==TK_GE );
      testcase( op==TK_EQ );
      testcase( op==TK_NE );
      codeCompareOperands(pParse, pExpr->pLeft, &r1, &regFree1,
                                  pExpr->pRight, &r2, &regFree2);
      codeCompare(pParse, pExpr->pLeft, pExpr->pRight, op,
                  r1, r2, inReg, SQLITE_STOREP2);
      testcase( regFree1==0 );
      testcase( regFree2==0 );
      break;
    }
    case TK_AND:
    case TK_OR:
    case TK_PLUS:
    case TK_STAR:
    case TK_MINUS:
    case TK_REM:
    case TK_BITAND:
    case TK_BITOR:
    case TK_SLASH:
    case TK_LSHIFT:
    case TK_RSHIFT: 
    case TK_CONCAT: {
      assert( TK_AND==OP_And );
      assert( TK_OR==OP_Or );
      assert( TK_PLUS==OP_Add );
      assert( TK_MINUS==OP_Subtract );
      assert( TK_REM==OP_Remainder );
      assert( TK_BITAND==OP_BitAnd );
      assert( TK_BITOR==OP_BitOr );
      assert( TK_SLASH==OP_Divide );
      assert( TK_LSHIFT==OP_ShiftLeft );
      assert( TK_RSHIFT==OP_ShiftRight );
      assert( TK_CONCAT==OP_Concat );
      testcase( op==TK_AND );
      testcase( op==TK_OR );
      testcase( op==TK_PLUS );
      testcase( op==TK_MINUS );
      testcase( op==TK_REM );
      testcase( op==TK_BITAND );
      testcase( op==TK_BITOR );
      testcase( op==TK_SLASH );
      testcase( op==TK_LSHIFT );
      testcase( op==TK_RSHIFT );
      testcase( op==TK_CONCAT );
      r1 = sqlite3ExprCodeTemp(pParse, pExpr->pLeft, &regFree1);
      r2 = sqlite3ExprCodeTemp(pParse, pExpr->pRight, &regFree2);
      sqlite3VdbeAddOp3(v, op, r2, r1, target);
      testcase( regFree1==0 );
      testcase( regFree2==0 );
      break;
    }
    case TK_UMINUS: {
      Expr *pLeft = pExpr->pLeft;
      assert( pLeft );
      if( pLeft->op==TK_FLOAT ){
        codeReal(v, (char*)pLeft->token.z, pLeft->token.n, 1, target);
      }else if( pLeft->op==TK_INTEGER ){
        codeInteger(v, pLeft, 1, target);
      }else{
        regFree1 = r1 = sqlite3GetTempReg(pParse);
        sqlite3VdbeAddOp2(v, OP_Integer, 0, r1);
        r2 = sqlite3ExprCodeTemp(pParse, pExpr->pLeft, &regFree2);
        sqlite3VdbeAddOp3(v, OP_Subtract, r2, r1, target);
        testcase( regFree2==0 );
      }
      inReg = target;
      break;
    }
    case TK_BITNOT:
    case TK_NOT: {
      assert( TK_BITNOT==OP_BitNot );
      assert( TK_NOT==OP_Not );
      testcase( op==TK_BITNOT );
      testcase( op==TK_NOT );
      r1 = sqlite3ExprCodeTemp(pParse, pExpr->pLeft, &regFree1);
      testcase( regFree1==0 );
      inReg = target;
      sqlite3VdbeAddOp2(v, op, r1, inReg);
      break;
    }
    case TK_ISNULL:
    case TK_NOTNULL: {
      int addr;
      assert( TK_ISNULL==OP_IsNull );
      assert( TK_NOTNULL==OP_NotNull );
      testcase( op==TK_ISNULL );
      testcase( op==TK_NOTNULL );
      sqlite3VdbeAddOp2(v, OP_Integer, 1, target);
      r1 = sqlite3ExprCodeTemp(pParse, pExpr->pLeft, &regFree1);
      testcase( regFree1==0 );
      addr = sqlite3VdbeAddOp1(v, op, r1);
      sqlite3VdbeAddOp2(v, OP_AddImm, target, -1);
      sqlite3VdbeJumpHere(v, addr);
      break;
    }
    case TK_AGG_FUNCTION: {
      AggInfo *pInfo = pExpr->pAggInfo;
      if( pInfo==0 ){
        sqlite3ErrorMsg(pParse, "misuse of aggregate: %T",
            &pExpr->span);
      }else{
        inReg = pInfo->aFunc[pExpr->iAgg].iMem;
      }
      break;
    }
    case TK_CONST_FUNC:
    case TK_FUNCTION: {
      ExprList *pFarg;       /* List of function arguments */
      int nFarg;             /* Number of function arguments */
      FuncDef *pDef;         /* The function definition object */
      int nId;               /* Length of the function name in bytes */
      const char *zId;       /* The function name */
      int constMask = 0;     /* Mask of function arguments that are constant */
      int i;                 /* Loop counter */
      u8 enc = ENC(db);      /* The text encoding used by this database */
      CollSeq *pColl = 0;    /* A collating sequence */

      assert( !ExprHasProperty(pExpr, EP_xIsSelect) );
      testcase( op==TK_CONST_FUNC );
      testcase( op==TK_FUNCTION );
      if( ExprHasAnyProperty(pExpr, EP_TokenOnly|EP_SpanToken) ){
        pFarg = 0;
      }else{
        pFarg = pExpr->x.pList;
      }
      nFarg = pFarg ? pFarg->nExpr : 0;
      zId = (char*)pExpr->token.z;
      nId = pExpr->token.n;
      pDef = sqlite3FindFunction(db, zId, nId, nFarg, enc, 0);
      assert( pDef!=0 );
      if( pFarg ){
        r1 = sqlite3GetTempRange(pParse, nFarg);
        sqlite3ExprCodeExprList(pParse, pFarg, r1, 1);
      }else{
        r1 = 0;
      }
#ifndef SQLITE_OMIT_VIRTUALTABLE
      /* Possibly overload the function if the first argument is
      ** a virtual table column.
      **
      ** For infix functions (LIKE, GLOB, REGEXP, and MATCH) use the
      ** second argument, not the first, as the argument to test to
      ** see if it is a column in a virtual table.  This is done because
      ** the left operand of infix functions (the operand we want to
      ** control overloading) ends up as the second argument to the
      ** function.  The expression "A glob B" is equivalent to 
      ** "glob(B,A).  We want to use the A in "A glob B" to test
      ** for function overloading.  But we use the B term in "glob(B,A)".
      */
      if( nFarg>=2 && (pExpr->flags & EP_InfixFunc) ){
        pDef = sqlite3VtabOverloadFunction(db, pDef, nFarg, pFarg->a[1].pExpr);
      }else if( nFarg>0 ){
        pDef = sqlite3VtabOverloadFunction(db, pDef, nFarg, pFarg->a[0].pExpr);
      }
#endif
      for(i=0; i<nFarg && i<32; i++){
        if( sqlite3ExprIsConstant(pFarg->a[i].pExpr) ){
          constMask |= (1<<i);
        }
        if( (pDef->flags & SQLITE_FUNC_NEEDCOLL)!=0 && !pColl ){
          pColl = sqlite3ExprCollSeq(pParse, pFarg->a[i].pExpr);
        }
      }
      if( pDef->flags & SQLITE_FUNC_NEEDCOLL ){
        if( !pColl ) pColl = db->pDfltColl; 
        sqlite3VdbeAddOp4(v, OP_CollSeq, 0, 0, 0, (char *)pColl, P4_COLLSEQ);
      }
      sqlite3VdbeAddOp4(v, OP_Function, constMask, r1, target,
                        (char*)pDef, P4_FUNCDEF);
      sqlite3VdbeChangeP5(v, (u8)nFarg);
      if( nFarg ){
        sqlite3ReleaseTempRange(pParse, r1, nFarg);
      }
      sqlite3ExprCacheAffinityChange(pParse, r1, nFarg);
      break;
    }
#ifndef SQLITE_OMIT_SUBQUERY
    case TK_EXISTS:
    case TK_SELECT: {
      testcase( op==TK_EXISTS );
      testcase( op==TK_SELECT );
      if( pExpr->iColumn==0 ){
        sqlite3CodeSubselect(pParse, pExpr, 0, 0);
      }
      inReg = pExpr->iColumn;
      break;
    }
    case TK_IN: {
      int rNotFound = 0;
      int rMayHaveNull = 0;
      int j2, j3, j4, j5;
      char affinity;
      int eType;

      VdbeNoopComment((v, "begin IN expr r%d", target));
      eType = sqlite3FindInIndex(pParse, pExpr, &rMayHaveNull);
      if( rMayHaveNull ){
        rNotFound = ++pParse->nMem;
      }

      /* Figure out the affinity to use to create a key from the results
      ** of the expression. affinityStr stores a static string suitable for
      ** P4 of OP_MakeRecord.
      */
      affinity = comparisonAffinity(pExpr);


      /* Code the <expr> from "<expr> IN (...)". The temporary table
      ** pExpr->iTable contains the values that make up the (...) set.
      */
      sqlite3ExprCachePush(pParse);
      sqlite3ExprCode(pParse, pExpr->pLeft, target);
      j2 = sqlite3VdbeAddOp1(v, OP_IsNull, target);
      if( eType==IN_INDEX_ROWID ){
        j3 = sqlite3VdbeAddOp1(v, OP_MustBeInt, target);
        j4 = sqlite3VdbeAddOp3(v, OP_NotExists, pExpr->iTable, 0, target);
        sqlite3VdbeAddOp2(v, OP_Integer, 1, target);
        j5 = sqlite3VdbeAddOp0(v, OP_Goto);
        sqlite3VdbeJumpHere(v, j3);
        sqlite3VdbeJumpHere(v, j4);
        sqlite3VdbeAddOp2(v, OP_Integer, 0, target);
      }else{
        r2 = regFree2 = sqlite3GetTempReg(pParse);

        /* Create a record and test for set membership. If the set contains
        ** the value, then jump to the end of the test code. The target
        ** register still contains the true (1) value written to it earlier.
        */
        sqlite3VdbeAddOp4(v, OP_MakeRecord, target, 1, r2, &affinity, 1);
        sqlite3VdbeAddOp2(v, OP_Integer, 1, target);
        j5 = sqlite3VdbeAddOp3(v, OP_Found, pExpr->iTable, 0, r2);

        /* If the set membership test fails, then the result of the 
        ** "x IN (...)" expression must be either 0 or NULL. If the set
        ** contains no NULL values, then the result is 0. If the set 
        ** contains one or more NULL values, then the result of the
        ** expression is also NULL.
        */
        if( rNotFound==0 ){
          /* This branch runs if it is known at compile time (now) that 
          ** the set contains no NULL values. This happens as the result
          ** of a "NOT NULL" constraint in the database schema. No need
          ** to test the data structure at runtime in this case.
          */
          sqlite3VdbeAddOp2(v, OP_Integer, 0, target);
        }else{
          /* This block populates the rNotFound register with either NULL
          ** or 0 (an integer value). If the data structure contains one
          ** or more NULLs, then set rNotFound to NULL. Otherwise, set it
          ** to 0. If register rMayHaveNull is already set to some value
          ** other than NULL, then the test has already been run and 
          ** rNotFound is already populated.
          */
          static const char nullRecord[] = { 0x02, 0x00 };
          j3 = sqlite3VdbeAddOp1(v, OP_NotNull, rMayHaveNull);
          sqlite3VdbeAddOp2(v, OP_Null, 0, rNotFound);
          sqlite3VdbeAddOp4(v, OP_Blob, 2, rMayHaveNull, 0, 
                             nullRecord, P4_STATIC);
          j4 = sqlite3VdbeAddOp3(v, OP_Found, pExpr->iTable, 0, rMayHaveNull);
          sqlite3VdbeAddOp2(v, OP_Integer, 0, rNotFound);
          sqlite3VdbeJumpHere(v, j4);
          sqlite3VdbeJumpHere(v, j3);

          /* Copy the value of register rNotFound (which is either NULL or 0)
          ** into the target register. This will be the result of the
          ** expression.
          */
          sqlite3VdbeAddOp2(v, OP_Copy, rNotFound, target);
        }
      }
      sqlite3VdbeJumpHere(v, j2);
      sqlite3VdbeJumpHere(v, j5);
      sqlite3ExprCachePop(pParse, 1);
      VdbeComment((v, "end IN expr r%d", target));
      break;
    }
#endif
    /*
    **    x BETWEEN y AND z
    **
    ** This is equivalent to
    **
    **    x>=y AND x<=z
    **
    ** X is stored in pExpr->pLeft.
    ** Y is stored in pExpr->pList->a[0].pExpr.
    ** Z is stored in pExpr->pList->a[1].pExpr.
    */
    case TK_BETWEEN: {
      Expr *pLeft = pExpr->pLeft;
      struct ExprList_item *pLItem = pExpr->x.pList->a;
      Expr *pRight = pLItem->pExpr;

      codeCompareOperands(pParse, pLeft, &r1, &regFree1,
                                  pRight, &r2, &regFree2);
      testcase( regFree1==0 );
      testcase( regFree2==0 );
      r3 = sqlite3GetTempReg(pParse);
      r4 = sqlite3GetTempReg(pParse);
      codeCompare(pParse, pLeft, pRight, OP_Ge,
                  r1, r2, r3, SQLITE_STOREP2);
      pLItem++;
      pRight = pLItem->pExpr;
      sqlite3ReleaseTempReg(pParse, regFree2);
      r2 = sqlite3ExprCodeTemp(pParse, pRight, &regFree2);
      testcase( regFree2==0 );
      codeCompare(pParse, pLeft, pRight, OP_Le, r1, r2, r4, SQLITE_STOREP2);
      sqlite3VdbeAddOp3(v, OP_And, r3, r4, target);
      sqlite3ReleaseTempReg(pParse, r3);
      sqlite3ReleaseTempReg(pParse, r4);
      break;
    }
    case TK_UPLUS: {
      inReg = sqlite3ExprCodeTarget(pParse, pExpr->pLeft, target);
      break;
    }

    /*
    ** Form A:
    **   CASE x WHEN e1 THEN r1 WHEN e2 THEN r2 ... WHEN eN THEN rN ELSE y END
    **
    ** Form B:
    **   CASE WHEN e1 THEN r1 WHEN e2 THEN r2 ... WHEN eN THEN rN ELSE y END
    **
    ** Form A is can be transformed into the equivalent form B as follows:
    **   CASE WHEN x=e1 THEN r1 WHEN x=e2 THEN r2 ...
    **        WHEN x=eN THEN rN ELSE y END
    **
    ** X (if it exists) is in pExpr->pLeft.
    ** Y is in pExpr->pRight.  The Y is also optional.  If there is no
    ** ELSE clause and no other term matches, then the result of the
    ** exprssion is NULL.
    ** Ei is in pExpr->pList->a[i*2] and Ri is pExpr->pList->a[i*2+1].
    **
    ** The result of the expression is the Ri for the first matching Ei,
    ** or if there is no matching Ei, the ELSE term Y, or if there is
    ** no ELSE term, NULL.
    */
    case TK_CASE: {
      int endLabel;                     /* GOTO label for end of CASE stmt */
      int nextCase;                     /* GOTO label for next WHEN clause */
      int nExpr;                        /* 2x number of WHEN terms */
      int i;                            /* Loop counter */
      ExprList *pEList;                 /* List of WHEN terms */
      struct ExprList_item *aListelem;  /* Array of WHEN terms */
      Expr opCompare;                   /* The X==Ei expression */
      Expr cacheX;                      /* Cached expression X */
      Expr *pX;                         /* The X expression */
      Expr *pTest = 0;                  /* X==Ei (form A) or just Ei (form B) */
      VVA_ONLY( int iCacheLevel = pParse->iCacheLevel; )

      assert( !ExprHasProperty(pExpr, EP_xIsSelect) && pExpr->x.pList );
      assert((pExpr->x.pList->nExpr % 2) == 0);
      assert(pExpr->x.pList->nExpr > 0);
      pEList = pExpr->x.pList;
      aListelem = pEList->a;
      nExpr = pEList->nExpr;
      endLabel = sqlite3VdbeMakeLabel(v);
      if( (pX = pExpr->pLeft)!=0 ){
        cacheX = *pX;
        testcase( pX->op==TK_COLUMN || pX->op==TK_REGISTER );
        cacheX.iTable = sqlite3ExprCodeTemp(pParse, pX, &regFree1);
        testcase( regFree1==0 );
        cacheX.op = TK_REGISTER;
        opCompare.op = TK_EQ;
        opCompare.pLeft = &cacheX;
        pTest = &opCompare;
      }
      for(i=0; i<nExpr; i=i+2){
        sqlite3ExprCachePush(pParse);
        if( pX ){
          assert( pTest!=0 );
          opCompare.pRight = aListelem[i].pExpr;
        }else{
          pTest = aListelem[i].pExpr;
        }
        nextCase = sqlite3VdbeMakeLabel(v);
        testcase( pTest->op==TK_COLUMN || pTest->op==TK_REGISTER );
        sqlite3ExprIfFalse(pParse, pTest, nextCase, SQLITE_JUMPIFNULL);
        testcase( aListelem[i+1].pExpr->op==TK_COLUMN );
        testcase( aListelem[i+1].pExpr->op==TK_REGISTER );
        sqlite3ExprCode(pParse, aListelem[i+1].pExpr, target);
        sqlite3VdbeAddOp2(v, OP_Goto, 0, endLabel);
        sqlite3ExprCachePop(pParse, 1);
        sqlite3VdbeResolveLabel(v, nextCase);
      }
      if( pExpr->pRight ){
        sqlite3ExprCachePush(pParse);
        sqlite3ExprCode(pParse, pExpr->pRight, target);
        sqlite3ExprCachePop(pParse, 1);
      }else{
        sqlite3VdbeAddOp2(v, OP_Null, 0, target);
      }
      assert( db->mallocFailed || pParse->nErr>0 
           || pParse->iCacheLevel==iCacheLevel );
      sqlite3VdbeResolveLabel(v, endLabel);
      break;
    }
#ifndef SQLITE_OMIT_TRIGGER
    case TK_RAISE: {
      if( !pParse->trigStack ){
        sqlite3ErrorMsg(pParse,
                       "RAISE() may only be used within a trigger-program");
        return 0;
      }
      if( pExpr->affinity!=OE_Ignore ){
         assert( pExpr->affinity==OE_Rollback ||
                 pExpr->affinity == OE_Abort ||
                 pExpr->affinity == OE_Fail );
         sqlite3VdbeAddOp4(v, OP_Halt, SQLITE_CONSTRAINT, pExpr->affinity, 0,
                        (char*)pExpr->token.z, pExpr->token.n);
      } else {
         assert( pExpr->affinity == OE_Ignore );
         sqlite3VdbeAddOp2(v, OP_ContextPop, 0, 0);
         sqlite3VdbeAddOp2(v, OP_Goto, 0, pParse->trigStack->ignoreJump);
         VdbeComment((v, "raise(IGNORE)"));
      }
      break;
    }
#endif
  }
  sqlite3ReleaseTempReg(pParse, regFree1);
  sqlite3ReleaseTempReg(pParse, regFree2);
  return inReg;
}

/*
** Generate code to evaluate an expression and store the results
** into a register.  Return the register number where the results
** are stored.
**
** If the register is a temporary register that can be deallocated,
** then write its number into *pReg.  If the result register is not
** a temporary, then set *pReg to zero.
*/
int sqlite3ExprCodeTemp(Parse *pParse, Expr *pExpr, int *pReg){
  int r1 = sqlite3GetTempReg(pParse);
  int r2 = sqlite3ExprCodeTarget(pParse, pExpr, r1);
  if( r2==r1 ){
    *pReg = r1;
  }else{
    sqlite3ReleaseTempReg(pParse, r1);
    *pReg = 0;
  }
  return r2;
}

/*
** Generate code that will evaluate expression pExpr and store the
** results in register target.  The results are guaranteed to appear
** in register target.
*/
int sqlite3ExprCode(Parse *pParse, Expr *pExpr, int target){
  int inReg;

  assert( target>0 && target<=pParse->nMem );
  inReg = sqlite3ExprCodeTarget(pParse, pExpr, target);
  assert( pParse->pVdbe || pParse->db->mallocFailed );
  if( inReg!=target && pParse->pVdbe ){
    sqlite3VdbeAddOp2(pParse->pVdbe, OP_SCopy, inReg, target);
  }
  return target;
}

/*
** Generate code that evalutes the given expression and puts the result
** in register target.
**
** Also make a copy of the expression results into another "cache" register
** and modify the expression so that the next time it is evaluated,
** the result is a copy of the cache register.
**
** This routine is used for expressions that are used multiple 
** times.  They are evaluated once and the results of the expression
** are reused.
*/
int sqlite3ExprCodeAndCache(Parse *pParse, Expr *pExpr, int target){
  Vdbe *v = pParse->pVdbe;
  int inReg;
  inReg = sqlite3ExprCode(pParse, pExpr, target);
  assert( target>0 );
  if( pExpr->op!=TK_REGISTER ){  
    int iMem;
    iMem = ++pParse->nMem;
    sqlite3VdbeAddOp2(v, OP_Copy, inReg, iMem);
    pExpr->iTable = iMem;
    pExpr->op = TK_REGISTER;
  }
  return inReg;
}

/*
** Return TRUE if pExpr is an constant expression that is appropriate
** for factoring out of a loop.  Appropriate expressions are:
**
**    *  Any expression that evaluates to two or more opcodes.
**
**    *  Any OP_Integer, OP_Real, OP_String, OP_Blob, OP_Null, 
**       or OP_Variable that does not need to be placed in a 
**       specific register.
**
** There is no point in factoring out single-instruction constant
** expressions that need to be placed in a particular register.  
** We could factor them out, but then we would end up adding an
** OP_SCopy instruction to move the value into the correct register
** later.  We might as well just use the original instruction and
** avoid the OP_SCopy.
*/
static int isAppropriateForFactoring(Expr *p){
  if( !sqlite3ExprIsConstantNotJoin(p) ){
    return 0;  /* Only constant expressions are appropriate for factoring */
  }
  if( (p->flags & EP_FixedDest)==0 ){
    return 1;  /* Any constant without a fixed destination is appropriate */
  }
  while( p->op==TK_UPLUS ) p = p->pLeft;
  switch( p->op ){
#ifndef SQLITE_OMIT_BLOB_LITERAL
    case TK_BLOB:
#endif
    case TK_VARIABLE:
    case TK_INTEGER:
    case TK_FLOAT:
    case TK_NULL:
    case TK_STRING: {
      testcase( p->op==TK_BLOB );
      testcase( p->op==TK_VARIABLE );
      testcase( p->op==TK_INTEGER );
      testcase( p->op==TK_FLOAT );
      testcase( p->op==TK_NULL );
      testcase( p->op==TK_STRING );
      /* Single-instruction constants with a fixed destination are
      ** better done in-line.  If we factor them, they will just end
      ** up generating an OP_SCopy to move the value to the destination
      ** register. */
      return 0;
    }
    case TK_UMINUS: {
       if( p->pLeft->op==TK_FLOAT || p->pLeft->op==TK_INTEGER ){
         return 0;
       }
       break;
    }
    default: {
      break;
    }
  }
  return 1;
}

/*
** If pExpr is a constant expression that is appropriate for
** factoring out of a loop, then evaluate the expression
** into a register and convert the expression into a TK_REGISTER
** expression.
*/
static int evalConstExpr(Walker *pWalker, Expr *pExpr){
  Parse *pParse = pWalker->pParse;
  switch( pExpr->op ){
    case TK_REGISTER: {
      return 1;
    }
    case TK_FUNCTION:
    case TK_AGG_FUNCTION:
    case TK_CONST_FUNC: {
      /* The arguments to a function have a fixed destination.
      ** Mark them this way to avoid generated unneeded OP_SCopy
      ** instructions. 
      */
      ExprList *pList = pExpr->x.pList;
      assert( !ExprHasProperty(pExpr, EP_xIsSelect) );
      if( pList ){
        int i = pList->nExpr;
        struct ExprList_item *pItem = pList->a;
        for(; i>0; i--, pItem++){
          if( pItem->pExpr ) pItem->pExpr->flags |= EP_FixedDest;
        }
      }
      break;
    }
  }
  if( isAppropriateForFactoring(pExpr) ){
    int r1 = ++pParse->nMem;
    int r2;
    r2 = sqlite3ExprCodeTarget(pParse, pExpr, r1);
    if( r1!=r2 ) sqlite3ReleaseTempReg(pParse, r1);
    pExpr->op = TK_REGISTER;
    pExpr->iTable = r2;
    return WRC_Prune;
  }
  return WRC_Continue;
}

/*
** Preevaluate constant subexpressions within pExpr and store the
** results in registers.  Modify pExpr so that the constant subexpresions
** are TK_REGISTER opcodes that refer to the precomputed values.
*/
void sqlite3ExprCodeConstants(Parse *pParse, Expr *pExpr){
  Walker w;
  w.xExprCallback = evalConstExpr;
  w.xSelectCallback = 0;
  w.pParse = pParse;
  sqlite3WalkExpr(&w, pExpr);
}


/*
** Generate code that pushes the value of every element of the given
** expression list into a sequence of registers beginning at target.
**
** Return the number of elements evaluated.
*/
int sqlite3ExprCodeExprList(
  Parse *pParse,     /* Parsing context */
  ExprList *pList,   /* The expression list to be coded */
  int target,        /* Where to write results */
  int doHardCopy     /* Make a hard copy of every element */
){
  struct ExprList_item *pItem;
  int i, n;
  assert( pList!=0 );
  assert( target>0 );
  n = pList->nExpr;
  for(pItem=pList->a, i=0; i<n; i++, pItem++){
    if( pItem->iAlias ){
      int iReg = codeAlias(pParse, pItem->iAlias, pItem->pExpr, target+i);
      Vdbe *v = sqlite3GetVdbe(pParse);
      if( iReg!=target+i ){
        sqlite3VdbeAddOp2(v, OP_SCopy, iReg, target+i);
      }
    }else{
      sqlite3ExprCode(pParse, pItem->pExpr, target+i);
    }
    if( doHardCopy ){
      sqlite3ExprHardCopy(pParse, target, n);
    }
  }
  return n;
}

/*
** Generate code for a boolean expression such that a jump is made
** to the label "dest" if the expression is true but execution
** continues straight thru if the expression is false.
**
** If the expression evaluates to NULL (neither true nor false), then
** take the jump if the jumpIfNull flag is SQLITE_JUMPIFNULL.
**
** This code depends on the fact that certain token values (ex: TK_EQ)
** are the same as opcode values (ex: OP_Eq) that implement the corresponding
** operation.  Special comments in vdbe.c and the mkopcodeh.awk script in
** the make process cause these values to align.  Assert()s in the code
** below verify that the numbers are aligned correctly.
*/
void sqlite3ExprIfTrue(Parse *pParse, Expr *pExpr, int dest, int jumpIfNull){
  Vdbe *v = pParse->pVdbe;
  int op = 0;
  int regFree1 = 0;
  int regFree2 = 0;
  int r1, r2;

  assert( jumpIfNull==SQLITE_JUMPIFNULL || jumpIfNull==0 );
  if( v==0 || pExpr==0 ) return;
  op = pExpr->op;
  switch( op ){
    case TK_AND: {
      int d2 = sqlite3VdbeMakeLabel(v);
      testcase( jumpIfNull==0 );
      sqlite3ExprCachePush(pParse);
      sqlite3ExprIfFalse(pParse, pExpr->pLeft, d2,jumpIfNull^SQLITE_JUMPIFNULL);
      sqlite3ExprIfTrue(pParse, pExpr->pRight, dest, jumpIfNull);
      sqlite3VdbeResolveLabel(v, d2);
      sqlite3ExprCachePop(pParse, 1);
      break;
    }
    case TK_OR: {
      testcase( jumpIfNull==0 );
      sqlite3ExprIfTrue(pParse, pExpr->pLeft, dest, jumpIfNull);
      sqlite3ExprIfTrue(pParse, pExpr->pRight, dest, jumpIfNull);
      break;
    }
    case TK_NOT: {
      testcase( jumpIfNull==0 );
      sqlite3ExprIfFalse(pParse, pExpr->pLeft, dest, jumpIfNull);
      break;
    }
    case TK_LT:
    case TK_LE:
    case TK_GT:
    case TK_GE:
    case TK_NE:
    case TK_EQ: {
      assert( TK_LT==OP_Lt );
      assert( TK_LE==OP_Le );
      assert( TK_GT==OP_Gt );
      assert( TK_GE==OP_Ge );
      assert( TK_EQ==OP_Eq );
      assert( TK_NE==OP_Ne );
      testcase( op==TK_LT );
      testcase( op==TK_LE );
      testcase( op==TK_GT );
      testcase( op==TK_GE );
      testcase( op==TK_EQ );
      testcase( op==TK_NE );
      testcase( jumpIfNull==0 );
      codeCompareOperands(pParse, pExpr->pLeft, &r1, &regFree1,
                                  pExpr->pRight, &r2, &regFree2);
      codeCompare(pParse, pExpr->pLeft, pExpr->pRight, op,
                  r1, r2, dest, jumpIfNull);
      testcase( regFree1==0 );
      testcase( regFree2==0 );
      break;
    }
    case TK_ISNULL:
    case TK_NOTNULL: {
      assert( TK_ISNULL==OP_IsNull );
      assert( TK_NOTNULL==OP_NotNull );
      testcase( op==TK_ISNULL );
      testcase( op==TK_NOTNULL );
      r1 = sqlite3ExprCodeTemp(pParse, pExpr->pLeft, &regFree1);
      sqlite3VdbeAddOp2(v, op, r1, dest);
      testcase( regFree1==0 );
      break;
    }
    case TK_BETWEEN: {
      /*    x BETWEEN y AND z
      **
      ** Is equivalent to 
      **
      **    x>=y AND x<=z
      **
      ** Code it as such, taking care to do the common subexpression
      ** elementation of x.
      */
      Expr exprAnd;
      Expr compLeft;
      Expr compRight;
      Expr exprX;

      assert( !ExprHasProperty(pExpr, EP_xIsSelect) );
      exprX = *pExpr->pLeft;
      exprAnd.op = TK_AND;
      exprAnd.pLeft = &compLeft;
      exprAnd.pRight = &compRight;
      compLeft.op = TK_GE;
      compLeft.pLeft = &exprX;
      compLeft.pRight = pExpr->x.pList->a[0].pExpr;
      compRight.op = TK_LE;
      compRight.pLeft = &exprX;
      compRight.pRight = pExpr->x.pList->a[1].pExpr;
      exprX.iTable = sqlite3ExprCodeTemp(pParse, &exprX, &regFree1);
      testcase( regFree1==0 );
      exprX.op = TK_REGISTER;
      testcase( jumpIfNull==0 );
      sqlite3ExprIfTrue(pParse, &exprAnd, dest, jumpIfNull);
      break;
    }
    default: {
      r1 = sqlite3ExprCodeTemp(pParse, pExpr, &regFree1);
      sqlite3VdbeAddOp3(v, OP_If, r1, dest, jumpIfNull!=0);
      testcase( regFree1==0 );
      testcase( jumpIfNull==0 );
      break;
    }
  }
  sqlite3ReleaseTempReg(pParse, regFree1);
  sqlite3ReleaseTempReg(pParse, regFree2);  
}

/*
** Generate code for a boolean expression such that a jump is made
** to the label "dest" if the expression is false but execution
** continues straight thru if the expression is true.
**
** If the expression evaluates to NULL (neither true nor false) then
** jump if jumpIfNull is SQLITE_JUMPIFNULL or fall through if jumpIfNull
** is 0.
*/
void sqlite3ExprIfFalse(Parse *pParse, Expr *pExpr, int dest, int jumpIfNull){
  Vdbe *v = pParse->pVdbe;
  int op = 0;
  int regFree1 = 0;
  int regFree2 = 0;
  int r1, r2;

  assert( jumpIfNull==SQLITE_JUMPIFNULL || jumpIfNull==0 );
  if( v==0 || pExpr==0 ) return;

  /* The value of pExpr->op and op are related as follows:
  **
  **       pExpr->op            op
  **       ---------          ----------
  **       TK_ISNULL          OP_NotNull
  **       TK_NOTNULL         OP_IsNull
  **       TK_NE              OP_Eq
  **       TK_EQ              OP_Ne
  **       TK_GT              OP_Le
  **       TK_LE              OP_Gt
  **       TK_GE              OP_Lt
  **       TK_LT              OP_Ge
  **
  ** For other values of pExpr->op, op is undefined and unused.
  ** The value of TK_ and OP_ constants are arranged such that we
  ** can compute the mapping above using the following expression.
  ** Assert()s verify that the computation is correct.
  */
  op = ((pExpr->op+(TK_ISNULL&1))^1)-(TK_ISNULL&1);

  /* Verify correct alignment of TK_ and OP_ constants
  */
  assert( pExpr->op!=TK_ISNULL || op==OP_NotNull );
  assert( pExpr->op!=TK_NOTNULL || op==OP_IsNull );
  assert( pExpr->op!=TK_NE || op==OP_Eq );
  assert( pExpr->op!=TK_EQ || op==OP_Ne );
  assert( pExpr->op!=TK_LT || op==OP_Ge );
  assert( pExpr->op!=TK_LE || op==OP_Gt );
  assert( pExpr->op!=TK_GT || op==OP_Le );
  assert( pExpr->op!=TK_GE || op==OP_Lt );

  switch( pExpr->op ){
    case TK_AND: {
      testcase( jumpIfNull==0 );
      sqlite3ExprIfFalse(pParse, pExpr->pLeft, dest, jumpIfNull);
      sqlite3ExprIfFalse(pParse, pExpr->pRight, dest, jumpIfNull);
      break;
    }
    case TK_OR: {
      int d2 = sqlite3VdbeMakeLabel(v);
      testcase( jumpIfNull==0 );
      sqlite3ExprCachePush(pParse);
      sqlite3ExprIfTrue(pParse, pExpr->pLeft, d2, jumpIfNull^SQLITE_JUMPIFNULL);
      sqlite3ExprIfFalse(pParse, pExpr->pRight, dest, jumpIfNull);
      sqlite3VdbeResolveLabel(v, d2);
      sqlite3ExprCachePop(pParse, 1);
      break;
    }
    case TK_NOT: {
      sqlite3ExprIfTrue(pParse, pExpr->pLeft, dest, jumpIfNull);
      break;
    }
    case TK_LT:
    case TK_LE:
    case TK_GT:
    case TK_GE:
    case TK_NE:
    case TK_EQ: {
      testcase( op==TK_LT );
      testcase( op==TK_LE );
      testcase( op==TK_GT );
      testcase( op==TK_GE );
      testcase( op==TK_EQ );
      testcase( op==TK_NE );
      testcase( jumpIfNull==0 );
      codeCompareOperands(pParse, pExpr->pLeft, &r1, &regFree1,
                                  pExpr->pRight, &r2, &regFree2);
      codeCompare(pParse, pExpr->pLeft, pExpr->pRight, op,
                  r1, r2, dest, jumpIfNull);
      testcase( regFree1==0 );
      testcase( regFree2==0 );
      break;
    }
    case TK_ISNULL:
    case TK_NOTNULL: {
      testcase( op==TK_ISNULL );
      testcase( op==TK_NOTNULL );
      r1 = sqlite3ExprCodeTemp(pParse, pExpr->pLeft, &regFree1);
      sqlite3VdbeAddOp2(v, op, r1, dest);
      testcase( regFree1==0 );
      break;
    }
    case TK_BETWEEN: {
      /*    x BETWEEN y AND z
      **
      ** Is equivalent to 
      **
      **    x>=y AND x<=z
      **
      ** Code it as such, taking care to do the common subexpression
      ** elementation of x.
      */
      Expr exprAnd;
      Expr compLeft;
      Expr compRight;
      Expr exprX;

      assert( !ExprHasProperty(pExpr, EP_xIsSelect) );
      exprX = *pExpr->pLeft;
      exprAnd.op = TK_AND;
      exprAnd.pLeft = &compLeft;
      exprAnd.pRight = &compRight;
      compLeft.op = TK_GE;
      compLeft.pLeft = &exprX;
      compLeft.pRight = pExpr->x.pList->a[0].pExpr;
      compRight.op = TK_LE;
      compRight.pLeft = &exprX;
      compRight.pRight = pExpr->x.pList->a[1].pExpr;
      exprX.iTable = sqlite3ExprCodeTemp(pParse, &exprX, &regFree1);
      testcase( regFree1==0 );
      exprX.op = TK_REGISTER;
      testcase( jumpIfNull==0 );
      sqlite3ExprIfFalse(pParse, &exprAnd, dest, jumpIfNull);
      break;
    }
    default: {
      r1 = sqlite3ExprCodeTemp(pParse, pExpr, &regFree1);
      sqlite3VdbeAddOp3(v, OP_IfNot, r1, dest, jumpIfNull!=0);
      testcase( regFree1==0 );
      testcase( jumpIfNull==0 );
      break;
    }
  }
  sqlite3ReleaseTempReg(pParse, regFree1);
  sqlite3ReleaseTempReg(pParse, regFree2);
}

/*
** Do a deep comparison of two expression trees.  Return TRUE (non-zero)
** if they are identical and return FALSE if they differ in any way.
**
** Sometimes this routine will return FALSE even if the two expressions
** really are equivalent.  If we cannot prove that the expressions are
** identical, we return FALSE just to be safe.  So if this routine
** returns false, then you do not really know for certain if the two
** expressions are the same.  But if you get a TRUE return, then you
** can be sure the expressions are the same.  In the places where
** this routine is used, it does not hurt to get an extra FALSE - that
** just might result in some slightly slower code.  But returning
** an incorrect TRUE could lead to a malfunction.
*/
int sqlite3ExprCompare(Expr *pA, Expr *pB){
  int i;
  if( pA==0||pB==0 ){
    return pB==pA;
  }
  if( ExprHasProperty(pA, EP_xIsSelect) || ExprHasProperty(pB, EP_xIsSelect) ){
    return 0;
  }
  if( (pA->flags & EP_Distinct)!=(pB->flags & EP_Distinct) ) return 0;
  if( pA->op!=pB->op ) return 0;
  if( !sqlite3ExprCompare(pA->pLeft, pB->pLeft) ) return 0;
  if( !sqlite3ExprCompare(pA->pRight, pB->pRight) ) return 0;

  if( pA->x.pList && pB->x.pList ){
    if( pA->x.pList->nExpr!=pB->x.pList->nExpr ) return 0;
    for(i=0; i<pA->x.pList->nExpr; i++){
      Expr *pExprA = pA->x.pList->a[i].pExpr;
      Expr *pExprB = pB->x.pList->a[i].pExpr;
      if( !sqlite3ExprCompare(pExprA, pExprB) ) return 0;
    }
  }else if( pA->x.pList || pB->x.pList ){
    return 0;
  }

  if( pA->iTable!=pB->iTable || pA->iColumn!=pB->iColumn ) return 0;
  if( pA->op!=TK_COLUMN && pA->token.z ){
    if( pB->token.z==0 ) return 0;
    if( pB->token.n!=pA->token.n ) return 0;
    if( sqlite3StrNICmp((char*)pA->token.z,(char*)pB->token.z,pB->token.n)!=0 ){
      return 0;
    }
  }
  return 1;
}


/*
** Add a new element to the pAggInfo->aCol[] array.  Return the index of
** the new element.  Return a negative number if malloc fails.
*/
static int addAggInfoColumn(sqlite3 *db, AggInfo *pInfo){
  int i;
  pInfo->aCol = sqlite3ArrayAllocate(
       db,
       pInfo->aCol,
       sizeof(pInfo->aCol[0]),
       3,
       &pInfo->nColumn,
       &pInfo->nColumnAlloc,
       &i
  );
  return i;
}    

/*
** Add a new element to the pAggInfo->aFunc[] array.  Return the index of
** the new element.  Return a negative number if malloc fails.
*/
static int addAggInfoFunc(sqlite3 *db, AggInfo *pInfo){
  int i;
  pInfo->aFunc = sqlite3ArrayAllocate(
       db, 
       pInfo->aFunc,
       sizeof(pInfo->aFunc[0]),
       3,
       &pInfo->nFunc,
       &pInfo->nFuncAlloc,
       &i
  );
  return i;
}    

/*
** This is the xExprCallback for a tree walker.  It is used to
** implement sqlite3ExprAnalyzeAggregates().  See sqlite3ExprAnalyzeAggregates
** for additional information.
*/
static int analyzeAggregate(Walker *pWalker, Expr *pExpr){
  int i;
  NameContext *pNC = pWalker->u.pNC;
  Parse *pParse = pNC->pParse;
  SrcList *pSrcList = pNC->pSrcList;
  AggInfo *pAggInfo = pNC->pAggInfo;

  switch( pExpr->op ){
    case TK_AGG_COLUMN:
    case TK_COLUMN: {
      testcase( pExpr->op==TK_AGG_COLUMN );
      testcase( pExpr->op==TK_COLUMN );
      /* Check to see if the column is in one of the tables in the FROM
      ** clause of the aggregate query */
      if( pSrcList ){
        struct SrcList_item *pItem = pSrcList->a;
        for(i=0; i<pSrcList->nSrc; i++, pItem++){
          struct AggInfo_col *pCol;
          if( pExpr->iTable==pItem->iCursor ){
            /* If we reach this point, it means that pExpr refers to a table
            ** that is in the FROM clause of the aggregate query.  
            **
            ** Make an entry for the column in pAggInfo->aCol[] if there
            ** is not an entry there already.
            */
            int k;
            pCol = pAggInfo->aCol;
            for(k=0; k<pAggInfo->nColumn; k++, pCol++){
              if( pCol->iTable==pExpr->iTable &&
                  pCol->iColumn==pExpr->iColumn ){
                break;
              }
            }
            if( (k>=pAggInfo->nColumn)
             && (k = addAggInfoColumn(pParse->db, pAggInfo))>=0 
            ){
              pCol = &pAggInfo->aCol[k];
              pCol->pTab = pExpr->pTab;
              pCol->iTable = pExpr->iTable;
              pCol->iColumn = pExpr->iColumn;
              pCol->iMem = ++pParse->nMem;
              pCol->iSorterColumn = -1;
              pCol->pExpr = pExpr;
              if( pAggInfo->pGroupBy ){
                int j, n;
                ExprList *pGB = pAggInfo->pGroupBy;
                struct ExprList_item *pTerm = pGB->a;
                n = pGB->nExpr;
                for(j=0; j<n; j++, pTerm++){
                  Expr *pE = pTerm->pExpr;
                  if( pE->op==TK_COLUMN && pE->iTable==pExpr->iTable &&
                      pE->iColumn==pExpr->iColumn ){
                    pCol->iSorterColumn = j;
                    break;
                  }
                }
              }
              if( pCol->iSorterColumn<0 ){
                pCol->iSorterColumn = pAggInfo->nSortingColumn++;
              }
            }
            /* There is now an entry for pExpr in pAggInfo->aCol[] (either
            ** because it was there before or because we just created it).
            ** Convert the pExpr to be a TK_AGG_COLUMN referring to that
            ** pAggInfo->aCol[] entry.
            */
            pExpr->pAggInfo = pAggInfo;
            pExpr->op = TK_AGG_COLUMN;
            pExpr->iAgg = k;
            break;
          } /* endif pExpr->iTable==pItem->iCursor */
        } /* end loop over pSrcList */
      }
      return WRC_Prune;
    }
    case TK_AGG_FUNCTION: {
      /* The pNC->nDepth==0 test causes aggregate functions in subqueries
      ** to be ignored */
      if( pNC->nDepth==0 ){
        /* Check to see if pExpr is a duplicate of another aggregate 
        ** function that is already in the pAggInfo structure
        */
        struct AggInfo_func *pItem = pAggInfo->aFunc;
        for(i=0; i<pAggInfo->nFunc; i++, pItem++){
          if( sqlite3ExprCompare(pItem->pExpr, pExpr) ){
            break;
          }
        }
        if( i>=pAggInfo->nFunc ){
          /* pExpr is original.  Make a new entry in pAggInfo->aFunc[]
          */
          u8 enc = ENC(pParse->db);
          i = addAggInfoFunc(pParse->db, pAggInfo);
          if( i>=0 ){
            assert( !ExprHasProperty(pExpr, EP_xIsSelect) );
            pItem = &pAggInfo->aFunc[i];
            pItem->pExpr = pExpr;
            pItem->iMem = ++pParse->nMem;
            pItem->pFunc = sqlite3FindFunction(pParse->db,
                   (char*)pExpr->token.z, pExpr->token.n,
                   pExpr->x.pList ? pExpr->x.pList->nExpr : 0, enc, 0);
            if( pExpr->flags & EP_Distinct ){
              pItem->iDistinct = pParse->nTab++;
            }else{
              pItem->iDistinct = -1;
            }
          }
        }
        /* Make pExpr point to the appropriate pAggInfo->aFunc[] entry
        */
        pExpr->iAgg = i;
        pExpr->pAggInfo = pAggInfo;
        return WRC_Prune;
      }
    }
  }
  return WRC_Continue;
}
static int analyzeAggregatesInSelect(Walker *pWalker, Select *pSelect){
  NameContext *pNC = pWalker->u.pNC;
  if( pNC->nDepth==0 ){
    pNC->nDepth++;
    sqlite3WalkSelect(pWalker, pSelect);
    pNC->nDepth--;
    return WRC_Prune;
  }else{
    return WRC_Continue;
  }
}

/*
** Analyze the given expression looking for aggregate functions and
** for variables that need to be added to the pParse->aAgg[] array.
** Make additional entries to the pParse->aAgg[] array as necessary.
**
** This routine should only be called after the expression has been
** analyzed by sqlite3ResolveExprNames().
*/
void sqlite3ExprAnalyzeAggregates(NameContext *pNC, Expr *pExpr){
  Walker w;
  w.xExprCallback = analyzeAggregate;
  w.xSelectCallback = analyzeAggregatesInSelect;
  w.u.pNC = pNC;
  sqlite3WalkExpr(&w, pExpr);
}

/*
** Call sqlite3ExprAnalyzeAggregates() for every expression in an
** expression list.  Return the number of errors.
**
** If an error is found, the analysis is cut short.
*/
void sqlite3ExprAnalyzeAggList(NameContext *pNC, ExprList *pList){
  struct ExprList_item *pItem;
  int i;
  if( pList ){
    for(pItem=pList->a, i=0; i<pList->nExpr; i++, pItem++){
      sqlite3ExprAnalyzeAggregates(pNC, pItem->pExpr);
    }
  }
}

/*
** Allocate a single new register for use to hold some intermediate result.
*/
int sqlite3GetTempReg(Parse *pParse){
  if( pParse->nTempReg==0 ){
    return ++pParse->nMem;
  }
  return pParse->aTempReg[--pParse->nTempReg];
}

/*
** Deallocate a register, making available for reuse for some other
** purpose.
**
** If a register is currently being used by the column cache, then
** the dallocation is deferred until the column cache line that uses
** the register becomes stale.
*/
void sqlite3ReleaseTempReg(Parse *pParse, int iReg){
  if( iReg && pParse->nTempReg<ArraySize(pParse->aTempReg) ){
    int i;
    struct yColCache *p;
    for(i=0, p=pParse->aColCache; i<SQLITE_N_COLCACHE; i++, p++){
      if( p->iReg==iReg ){
        p->tempReg = 1;
        return;
      }
    }
    pParse->aTempReg[pParse->nTempReg++] = iReg;
  }
}

/*
** Allocate or deallocate a block of nReg consecutive registers
*/
int sqlite3GetTempRange(Parse *pParse, int nReg){
  int i, n;
  i = pParse->iRangeReg;
  n = pParse->nRangeReg;
  if( nReg<=n && !usedAsColumnCache(pParse, i, i+n-1) ){
    pParse->iRangeReg += nReg;
    pParse->nRangeReg -= nReg;
  }else{
    i = pParse->nMem+1;
    pParse->nMem += nReg;
  }
  return i;
}
void sqlite3ReleaseTempRange(Parse *pParse, int iReg, int nReg){
  if( nReg>pParse->nRangeReg ){
    pParse->nRangeReg = nReg;
    pParse->iRangeReg = iReg;
  }
}
