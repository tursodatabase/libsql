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
** $Id: expr.c,v 1.126 2004/05/21 02:14:25 drh Exp $
*/
#include "sqliteInt.h"
#include <ctype.h>

char const *sqlite3AffinityString(char affinity){
  switch( affinity ){
    case SQLITE_AFF_INTEGER: return "i";
    case SQLITE_AFF_NUMERIC: return "n";
    case SQLITE_AFF_TEXT:    return "t";
    case SQLITE_AFF_NONE:    return "o";
    default:
      assert(0);
  }
}


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
  if( pExpr->op==TK_AS ){
    return sqlite3ExprAffinity(pExpr->pLeft);
  }
  if( pExpr->op==TK_SELECT ){
    return sqlite3ExprAffinity(pExpr->pSelect->pEList->a[0].pExpr);
  }
  return pExpr->affinity;
}

/*
** pExpr is the left operand of a comparison operator.  aff2 is the
** type affinity of the right operand.  This routine returns the
** type affinity that should be used for the comparison operator.
*/
char sqlite3CompareAffinity(Expr *pExpr, char aff2){
  char aff1 = sqlite3ExprAffinity(pExpr);
  if( aff1 && aff2 ){
    /* Both sides of the comparison are columns. If one has numeric or
    ** integer affinity, use that. Otherwise use no affinity.
    */
    if( aff1==SQLITE_AFF_INTEGER || aff2==SQLITE_AFF_INTEGER ){
      return SQLITE_AFF_INTEGER;
    }else if( aff1==SQLITE_AFF_NUMERIC || aff2==SQLITE_AFF_NUMERIC ){
      return SQLITE_AFF_NUMERIC;
    }else{
      return SQLITE_AFF_NONE;
    }
  }else if( !aff1 && !aff2 ){
    /* Neither side of the comparison is a column. Use numeric affinity
    ** for the comparison.
    */
    return SQLITE_AFF_NUMERIC;
  }else{
    /* One side is a column, the other is not. Use the columns affinity. */
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
  }
  else if( pExpr->pSelect ){
    aff = sqlite3CompareAffinity(pExpr->pSelect->pEList->a[0].pExpr, aff);
  }
  else if( !aff ){
    aff = SQLITE_AFF_NUMERIC;
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
  return 
    (aff==SQLITE_AFF_NONE) ||
    (aff==SQLITE_AFF_NUMERIC && idx_affinity==SQLITE_AFF_INTEGER) ||
    (aff==SQLITE_AFF_INTEGER && idx_affinity==SQLITE_AFF_NUMERIC) ||
    (aff==idx_affinity);
}

/*
** Return the P1 value that should be used for a binary comparison
** opcode (OP_Eq, OP_Ge etc.) used to compare pExpr1 and pExpr2.
** If jumpIfNull is true, then set the low byte of the returned
** P1 value to tell the opcode to jump if either expression
** evaluates to NULL.
*/
static int binaryCompareP1(Expr *pExpr1, Expr *pExpr2, int jumpIfNull){
  char aff = sqlite3ExprAffinity(pExpr2);
  return (((int)sqlite3CompareAffinity(pExpr1, aff))<<8)+(jumpIfNull?1:0);
}

/*
** Construct a new expression node and return a pointer to it.  Memory
** for this node is obtained from sqliteMalloc().  The calling function
** is responsible for making sure the node eventually gets freed.
*/
Expr *sqlite3Expr(int op, Expr *pLeft, Expr *pRight, Token *pToken){
  Expr *pNew;
  pNew = sqliteMalloc( sizeof(Expr) );
  if( pNew==0 ){
    /* When malloc fails, we leak memory from pLeft and pRight */
    return 0;
  }
  pNew->op = op;
  pNew->pLeft = pLeft;
  pNew->pRight = pRight;
  if( pToken ){
    assert( pToken->dyn==0 );
    pNew->token = *pToken;
    pNew->span = *pToken;
  }else{
    assert( pNew->token.dyn==0 );
    assert( pNew->token.z==0 );
    assert( pNew->token.n==0 );
    if( pLeft && pRight ){
      sqlite3ExprSpan(pNew, &pLeft->span, &pRight->span);
    }else{
      pNew->span = pNew->token;
    }
  }
  return pNew;
}

/*
** Set the Expr.span field of the given expression to span all
** text between the two given tokens.
*/
void sqlite3ExprSpan(Expr *pExpr, Token *pLeft, Token *pRight){
  assert( pRight!=0 );
  assert( pLeft!=0 );
  /* Note: pExpr might be NULL due to a prior malloc failure */
  if( pExpr && pRight->z && pLeft->z ){
    if( pLeft->dyn==0 && pRight->dyn==0 ){
      pExpr->span.z = pLeft->z;
      pExpr->span.n = pRight->n + Addr(pRight->z) - Addr(pLeft->z);
    }else{
      pExpr->span.z = 0;
    }
  }
}

/*
** Construct a new expression node for a function with multiple
** arguments.
*/
Expr *sqlite3ExprFunction(ExprList *pList, Token *pToken){
  Expr *pNew;
  pNew = sqliteMalloc( sizeof(Expr) );
  if( pNew==0 ){
    /* sqlite3ExprListDelete(pList); // Leak pList when malloc fails */
    return 0;
  }
  pNew->op = TK_FUNCTION;
  pNew->pList = pList;
  if( pToken ){
    assert( pToken->dyn==0 );
    pNew->token = *pToken;
  }else{
    pNew->token.z = 0;
  }
  pNew->span = pNew->token;
  return pNew;
}

/*
** Recursively delete an expression tree.
*/
void sqlite3ExprDelete(Expr *p){
  if( p==0 ) return;
  if( p->span.dyn ) sqliteFree((char*)p->span.z);
  if( p->token.dyn ) sqliteFree((char*)p->token.z);
  sqlite3ExprDelete(p->pLeft);
  sqlite3ExprDelete(p->pRight);
  sqlite3ExprListDelete(p->pList);
  sqlite3SelectDelete(p->pSelect);
  sqliteFree(p);
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
*/
Expr *sqlite3ExprDup(Expr *p){
  Expr *pNew;
  if( p==0 ) return 0;
  pNew = sqliteMallocRaw( sizeof(*p) );
  if( pNew==0 ) return 0;
  memcpy(pNew, p, sizeof(*pNew));
  if( p->token.z!=0 ){
    pNew->token.z = sqliteStrDup(p->token.z);
    pNew->token.dyn = 1;
  }else{
    assert( pNew->token.z==0 );
  }
  pNew->span.z = 0;
  pNew->pLeft = sqlite3ExprDup(p->pLeft);
  pNew->pRight = sqlite3ExprDup(p->pRight);
  pNew->pList = sqlite3ExprListDup(p->pList);
  pNew->pSelect = sqlite3SelectDup(p->pSelect);
  return pNew;
}
void sqlite3TokenCopy(Token *pTo, Token *pFrom){
  if( pTo->dyn ) sqliteFree((char*)pTo->z);
  if( pFrom->z ){
    pTo->n = pFrom->n;
    pTo->z = sqliteStrNDup(pFrom->z, pFrom->n);
    pTo->dyn = 1;
  }else{
    pTo->z = 0;
  }
}
ExprList *sqlite3ExprListDup(ExprList *p){
  ExprList *pNew;
  struct ExprList_item *pItem;
  int i;
  if( p==0 ) return 0;
  pNew = sqliteMalloc( sizeof(*pNew) );
  if( pNew==0 ) return 0;
  pNew->nExpr = pNew->nAlloc = p->nExpr;
  pNew->a = pItem = sqliteMalloc( p->nExpr*sizeof(p->a[0]) );
  if( pItem==0 ) return 0;  /* Leaks memory after a malloc failure */
  for(i=0; i<p->nExpr; i++, pItem++){
    Expr *pNewExpr, *pOldExpr;
    pItem->pExpr = pNewExpr = sqlite3ExprDup(pOldExpr = p->a[i].pExpr);
    if( pOldExpr->span.z!=0 && pNewExpr ){
      /* Always make a copy of the span for top-level expressions in the
      ** expression list.  The logic in SELECT processing that determines
      ** the names of columns in the result set needs this information */
      sqlite3TokenCopy(&pNewExpr->span, &pOldExpr->span);
    }
    assert( pNewExpr==0 || pNewExpr->span.z!=0 
            || pOldExpr->span.z==0 || sqlite3_malloc_failed );
    pItem->zName = sqliteStrDup(p->a[i].zName);
    pItem->sortOrder = p->a[i].sortOrder;
    pItem->isAgg = p->a[i].isAgg;
    pItem->done = 0;
  }
  return pNew;
}
SrcList *sqlite3SrcListDup(SrcList *p){
  SrcList *pNew;
  int i;
  int nByte;
  if( p==0 ) return 0;
  nByte = sizeof(*p) + (p->nSrc>0 ? sizeof(p->a[0]) * (p->nSrc-1) : 0);
  pNew = sqliteMallocRaw( nByte );
  if( pNew==0 ) return 0;
  pNew->nSrc = pNew->nAlloc = p->nSrc;
  for(i=0; i<p->nSrc; i++){
    struct SrcList_item *pNewItem = &pNew->a[i];
    struct SrcList_item *pOldItem = &p->a[i];
    pNewItem->zDatabase = sqliteStrDup(pOldItem->zDatabase);
    pNewItem->zName = sqliteStrDup(pOldItem->zName);
    pNewItem->zAlias = sqliteStrDup(pOldItem->zAlias);
    pNewItem->jointype = pOldItem->jointype;
    pNewItem->iCursor = pOldItem->iCursor;
    pNewItem->pTab = 0;
    pNewItem->pSelect = sqlite3SelectDup(pOldItem->pSelect);
    pNewItem->pOn = sqlite3ExprDup(pOldItem->pOn);
    pNewItem->pUsing = sqlite3IdListDup(pOldItem->pUsing);
  }
  return pNew;
}
IdList *sqlite3IdListDup(IdList *p){
  IdList *pNew;
  int i;
  if( p==0 ) return 0;
  pNew = sqliteMallocRaw( sizeof(*pNew) );
  if( pNew==0 ) return 0;
  pNew->nId = pNew->nAlloc = p->nId;
  pNew->a = sqliteMallocRaw( p->nId*sizeof(p->a[0]) );
  if( pNew->a==0 ) return 0;
  for(i=0; i<p->nId; i++){
    struct IdList_item *pNewItem = &pNew->a[i];
    struct IdList_item *pOldItem = &p->a[i];
    pNewItem->zName = sqliteStrDup(pOldItem->zName);
    pNewItem->idx = pOldItem->idx;
  }
  return pNew;
}
Select *sqlite3SelectDup(Select *p){
  Select *pNew;
  if( p==0 ) return 0;
  pNew = sqliteMallocRaw( sizeof(*p) );
  if( pNew==0 ) return 0;
  pNew->isDistinct = p->isDistinct;
  pNew->pEList = sqlite3ExprListDup(p->pEList);
  pNew->pSrc = sqlite3SrcListDup(p->pSrc);
  pNew->pWhere = sqlite3ExprDup(p->pWhere);
  pNew->pGroupBy = sqlite3ExprListDup(p->pGroupBy);
  pNew->pHaving = sqlite3ExprDup(p->pHaving);
  pNew->pOrderBy = sqlite3ExprListDup(p->pOrderBy);
  pNew->op = p->op;
  pNew->pPrior = sqlite3SelectDup(p->pPrior);
  pNew->nLimit = p->nLimit;
  pNew->nOffset = p->nOffset;
  pNew->zSelect = 0;
  pNew->iLimit = -1;
  pNew->iOffset = -1;
  return pNew;
}


/*
** Add a new element to the end of an expression list.  If pList is
** initially NULL, then create a new expression list.
*/
ExprList *sqlite3ExprListAppend(ExprList *pList, Expr *pExpr, Token *pName){
  if( pList==0 ){
    pList = sqliteMalloc( sizeof(ExprList) );
    if( pList==0 ){
      /* sqlite3ExprDelete(pExpr); // Leak memory if malloc fails */
      return 0;
    }
    assert( pList->nAlloc==0 );
  }
  if( pList->nAlloc<=pList->nExpr ){
    pList->nAlloc = pList->nAlloc*2 + 4;
    pList->a = sqliteRealloc(pList->a, pList->nAlloc*sizeof(pList->a[0]));
    if( pList->a==0 ){
      /* sqlite3ExprDelete(pExpr); // Leak memory if malloc fails */
      pList->nExpr = pList->nAlloc = 0;
      return pList;
    }
  }
  assert( pList->a!=0 );
  if( pExpr || pName ){
    struct ExprList_item *pItem = &pList->a[pList->nExpr++];
    memset(pItem, 0, sizeof(*pItem));
    pItem->pExpr = pExpr;
    if( pName ){
      sqlite3SetNString(&pItem->zName, pName->z, pName->n, 0);
      sqlite3Dequote(pItem->zName);
    }
  }
  return pList;
}

/*
** Delete an entire expression list.
*/
void sqlite3ExprListDelete(ExprList *pList){
  int i;
  if( pList==0 ) return;
  assert( pList->a!=0 || (pList->nExpr==0 && pList->nAlloc==0) );
  assert( pList->nExpr<=pList->nAlloc );
  for(i=0; i<pList->nExpr; i++){
    sqlite3ExprDelete(pList->a[i].pExpr);
    sqliteFree(pList->a[i].zName);
  }
  sqliteFree(pList->a);
  sqliteFree(pList);
}

/*
** Walk an expression tree.  Return 1 if the expression is constant
** and 0 if it involves variables.
**
** For the purposes of this function, a double-quoted string (ex: "abc")
** is considered a variable but a single-quoted string (ex: 'abc') is
** a constant.
*/
int sqlite3ExprIsConstant(Expr *p){
  switch( p->op ){
    case TK_ID:
    case TK_COLUMN:
    case TK_DOT:
    case TK_FUNCTION:
      return 0;
    case TK_NULL:
    case TK_STRING:
    case TK_INTEGER:
    case TK_FLOAT:
    case TK_VARIABLE:
      return 1;
    default: {
      if( p->pLeft && !sqlite3ExprIsConstant(p->pLeft) ) return 0;
      if( p->pRight && !sqlite3ExprIsConstant(p->pRight) ) return 0;
      if( p->pList ){
        int i;
        for(i=0; i<p->pList->nExpr; i++){
          if( !sqlite3ExprIsConstant(p->pList->a[i].pExpr) ) return 0;
        }
      }
      return p->pLeft!=0 || p->pRight!=0 || (p->pList && p->pList->nExpr>0);
    }
  }
  return 0;
}

/*
** If the given expression codes a constant integer that is small enough
** to fit in a 32-bit integer, return 1 and put the value of the integer
** in *pValue.  If the expression is not an integer or if it is too big
** to fit in a signed 32-bit integer, return 0 and leave *pValue unchanged.
*/
int sqlite3ExprIsInteger(Expr *p, int *pValue){
  switch( p->op ){
    case TK_INTEGER: {
      if( sqlite3GetInt32(p->token.z, pValue) ){
        return 1;
      }
      break;
    }
    case TK_STRING: {
      const char *z = p->token.z;
      int n = p->token.n;
      if( n>0 && z[0]=='-' ){ z++; n--; }
      while( n>0 && *z && isdigit(*z) ){ z++; n--; }
      if( n==0 && sqlite3GetInt32(p->token.z, pValue) ){
        return 1;
      }
      break;
    }
    case TK_UPLUS: {
      return sqlite3ExprIsInteger(p->pLeft, pValue);
    }
    case TK_UMINUS: {
      int v;
      if( sqlite3ExprIsInteger(p->pLeft, &v) ){
        *pValue = -v;
        return 1;
      }
      break;
    }
    default: break;
  }
  return 0;
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
** Given the name of a column of the form X.Y.Z or Y.Z or just Z, look up
** that name in the set of source tables in pSrcList and make the pExpr 
** expression node refer back to that source column.  The following changes
** are made to pExpr:
**
**    pExpr->iDb           Set the index in db->aDb[] of the database holding
**                         the table.
**    pExpr->iTable        Set to the cursor number for the table obtained
**                         from pSrcList.
**    pExpr->iColumn       Set to the column number within the table.
**    pExpr->dataType      Set to the appropriate data type for the column.
**    pExpr->op            Set to TK_COLUMN.
**    pExpr->pLeft         Any expression this points to is deleted
**    pExpr->pRight        Any expression this points to is deleted.
**
** The pDbToken is the name of the database (the "X").  This value may be
** NULL meaning that name is of the form Y.Z or Z.  Any available database
** can be used.  The pTableToken is the name of the table (the "Y").  This
** value can be NULL if pDbToken is also NULL.  If pTableToken is NULL it
** means that the form of the name is Z and that columns from any table
** can be used.
**
** If the name cannot be resolved unambiguously, leave an error message
** in pParse and return non-zero.  Return zero on success.
*/
static int lookupName(
  Parse *pParse,      /* The parsing context */
  Token *pDbToken,     /* Name of the database containing table, or NULL */
  Token *pTableToken,  /* Name of table containing column, or NULL */
  Token *pColumnToken, /* Name of the column. */
  SrcList *pSrcList,   /* List of tables used to resolve column names */
  ExprList *pEList,    /* List of expressions used to resolve "AS" */
  Expr *pExpr          /* Make this EXPR node point to the selected column */
){
  char *zDb = 0;       /* Name of the database.  The "X" in X.Y.Z */
  char *zTab = 0;      /* Name of the table.  The "Y" in X.Y.Z or Y.Z */
  char *zCol = 0;      /* Name of the column.  The "Z" */
  int i, j;            /* Loop counters */
  int cnt = 0;         /* Number of matching column names */
  int cntTab = 0;      /* Number of matching table names */
  sqlite *db = pParse->db;  /* The database */

  assert( pColumnToken && pColumnToken->z ); /* The Z in X.Y.Z cannot be NULL */
  if( pDbToken && pDbToken->z ){
    zDb = sqliteStrNDup(pDbToken->z, pDbToken->n);
    sqlite3Dequote(zDb);
  }else{
    zDb = 0;
  }
  if( pTableToken && pTableToken->z ){
    zTab = sqliteStrNDup(pTableToken->z, pTableToken->n);
    sqlite3Dequote(zTab);
  }else{
    assert( zDb==0 );
    zTab = 0;
  }
  zCol = sqliteStrNDup(pColumnToken->z, pColumnToken->n);
  sqlite3Dequote(zCol);
  if( sqlite3_malloc_failed ){
    return 1;  /* Leak memory (zDb and zTab) if malloc fails */
  }
  assert( zTab==0 || pEList==0 );

  pExpr->iTable = -1;
  for(i=0; i<pSrcList->nSrc; i++){
    struct SrcList_item *pItem = &pSrcList->a[i];
    Table *pTab = pItem->pTab;
    Column *pCol;

    if( pTab==0 ) continue;
    assert( pTab->nCol>0 );
    if( zTab ){
      if( pItem->zAlias ){
        char *zTabName = pItem->zAlias;
        if( sqlite3StrICmp(zTabName, zTab)!=0 ) continue;
      }else{
        char *zTabName = pTab->zName;
        if( zTabName==0 || sqlite3StrICmp(zTabName, zTab)!=0 ) continue;
        if( zDb!=0 && sqlite3StrICmp(db->aDb[pTab->iDb].zName, zDb)!=0 ){
          continue;
        }
      }
    }
    if( 0==(cntTab++) ){
      pExpr->iTable = pItem->iCursor;
      pExpr->iDb = pTab->iDb;
    }
    for(j=0, pCol=pTab->aCol; j<pTab->nCol; j++, pCol++){
      if( sqlite3StrICmp(pCol->zName, zCol)==0 ){
        cnt++;
        pExpr->iTable = pItem->iCursor;
        pExpr->iDb = pTab->iDb;
        /* Substitute the rowid (column -1) for the INTEGER PRIMARY KEY */
        pExpr->iColumn = j==pTab->iPKey ? -1 : j;
        pExpr->affinity = pTab->aCol[j].affinity;
        break;
      }
    }
  }

  /* If we have not already resolved the name, then maybe 
  ** it is a new.* or old.* trigger argument reference
  */
  if( zDb==0 && zTab!=0 && cnt==0 && pParse->trigStack!=0 ){
    TriggerStack *pTriggerStack = pParse->trigStack;
    Table *pTab = 0;
    if( pTriggerStack->newIdx != -1 && sqlite3StrICmp("new", zTab) == 0 ){
      pExpr->iTable = pTriggerStack->newIdx;
      assert( pTriggerStack->pTab );
      pTab = pTriggerStack->pTab;
    }else if( pTriggerStack->oldIdx != -1 && sqlite3StrICmp("old", zTab) == 0 ){
      pExpr->iTable = pTriggerStack->oldIdx;
      assert( pTriggerStack->pTab );
      pTab = pTriggerStack->pTab;
    }

    if( pTab ){ 
      int j;
      Column *pCol = pTab->aCol;
      
      pExpr->iDb = pTab->iDb;
      cntTab++;
      for(j=0; j < pTab->nCol; j++, pCol++) {
        if( sqlite3StrICmp(pCol->zName, zCol)==0 ){
          cnt++;
          pExpr->iColumn = j==pTab->iPKey ? -1 : j;
          pExpr->affinity = pTab->aCol[j].affinity;
          break;
        }
      }
    }
  }

  /*
  ** Perhaps the name is a reference to the ROWID
  */
  if( cnt==0 && cntTab==1 && sqlite3IsRowid(zCol) ){
    cnt = 1;
    pExpr->iColumn = -1;
    pExpr->affinity = SQLITE_AFF_INTEGER;
  }

  /*
  ** If the input is of the form Z (not Y.Z or X.Y.Z) then the name Z
  ** might refer to an result-set alias.  This happens, for example, when
  ** we are resolving names in the WHERE clause of the following command:
  **
  **     SELECT a+b AS x FROM table WHERE x<10;
  **
  ** In cases like this, replace pExpr with a copy of the expression that
  ** forms the result set entry ("a+b" in the example) and return immediately.
  ** Note that the expression in the result set should have already been
  ** resolved by the time the WHERE clause is resolved.
  */
  if( cnt==0 && pEList!=0 ){
    for(j=0; j<pEList->nExpr; j++){
      char *zAs = pEList->a[j].zName;
      if( zAs!=0 && sqlite3StrICmp(zAs, zCol)==0 ){
        assert( pExpr->pLeft==0 && pExpr->pRight==0 );
        pExpr->op = TK_AS;
        pExpr->iColumn = j;
        pExpr->pLeft = sqlite3ExprDup(pEList->a[j].pExpr);
        sqliteFree(zCol);
        assert( zTab==0 && zDb==0 );
        return 0;
      }
    } 
  }

  /*
  ** If X and Y are NULL (in other words if only the column name Z is
  ** supplied) and the value of Z is enclosed in double-quotes, then
  ** Z is a string literal if it doesn't match any column names.  In that
  ** case, we need to return right away and not make any changes to
  ** pExpr.
  */
  if( cnt==0 && zTab==0 && pColumnToken->z[0]=='"' ){
    sqliteFree(zCol);
    return 0;
  }

  /*
  ** cnt==0 means there was not match.  cnt>1 means there were two or
  ** more matches.  Either way, we have an error.
  */
  if( cnt!=1 ){
    char *z = 0;
    char *zErr;
    zErr = cnt==0 ? "no such column: %s" : "ambiguous column name: %s";
    if( zDb ){
      sqlite3SetString(&z, zDb, ".", zTab, ".", zCol, 0);
    }else if( zTab ){
      sqlite3SetString(&z, zTab, ".", zCol, 0);
    }else{
      z = sqliteStrDup(zCol);
    }
    sqlite3ErrorMsg(pParse, zErr, z);
    sqliteFree(z);
  }

  /* Clean up and return
  */
  sqliteFree(zDb);
  sqliteFree(zTab);
  sqliteFree(zCol);
  sqlite3ExprDelete(pExpr->pLeft);
  pExpr->pLeft = 0;
  sqlite3ExprDelete(pExpr->pRight);
  pExpr->pRight = 0;
  pExpr->op = TK_COLUMN;
  sqlite3AuthRead(pParse, pExpr, pSrcList);
  return cnt!=1;
}

/*
** This routine walks an expression tree and resolves references to
** table columns.  Nodes of the form ID.ID or ID resolve into an
** index to the table in the table list and a column offset.  The 
** Expr.opcode for such nodes is changed to TK_COLUMN.  The Expr.iTable
** value is changed to the index of the referenced table in pTabList
** plus the "base" value.  The base value will ultimately become the
** VDBE cursor number for a cursor that is pointing into the referenced
** table.  The Expr.iColumn value is changed to the index of the column 
** of the referenced table.  The Expr.iColumn value for the special
** ROWID column is -1.  Any INTEGER PRIMARY KEY column is tried as an
** alias for ROWID.
**
** We also check for instances of the IN operator.  IN comes in two
** forms:
**
**           expr IN (exprlist)
** and
**           expr IN (SELECT ...)
**
** The first form is handled by creating a set holding the list
** of allowed values.  The second form causes the SELECT to generate 
** a temporary table.
**
** This routine also looks for scalar SELECTs that are part of an expression.
** If it finds any, it generates code to write the value of that select
** into a memory cell.
**
** Unknown columns or tables provoke an error.  The function returns
** the number of errors seen and leaves an error message on pParse->zErrMsg.
*/
int sqlite3ExprResolveIds(
  Parse *pParse,     /* The parser context */
  SrcList *pSrcList, /* List of tables used to resolve column names */
  ExprList *pEList,  /* List of expressions used to resolve "AS" */
  Expr *pExpr        /* The expression to be analyzed. */
){
  int i;

  if( pExpr==0 || pSrcList==0 ) return 0;
  for(i=0; i<pSrcList->nSrc; i++){
    assert( pSrcList->a[i].iCursor>=0 && pSrcList->a[i].iCursor<pParse->nTab );
  }
  switch( pExpr->op ){
    /* Double-quoted strings (ex: "abc") are used as identifiers if
    ** possible.  Otherwise they remain as strings.  Single-quoted
    ** strings (ex: 'abc') are always string literals.
    */
    case TK_STRING: {
      if( pExpr->token.z[0]=='\'' ) break;
      /* Fall thru into the TK_ID case if this is a double-quoted string */
    }
    /* A lone identifier is the name of a columnd.
    */
    case TK_ID: {
      if( lookupName(pParse, 0, 0, &pExpr->token, pSrcList, pEList, pExpr) ){
        return 1;
      }
      break; 
    }
  
    /* A table name and column name:     ID.ID
    ** Or a database, table and column:  ID.ID.ID
    */
    case TK_DOT: {
      Token *pColumn;
      Token *pTable;
      Token *pDb;
      Expr *pRight;

      pRight = pExpr->pRight;
      if( pRight->op==TK_ID ){
        pDb = 0;
        pTable = &pExpr->pLeft->token;
        pColumn = &pRight->token;
      }else{
        assert( pRight->op==TK_DOT );
        pDb = &pExpr->pLeft->token;
        pTable = &pRight->pLeft->token;
        pColumn = &pRight->pRight->token;
      }
      if( lookupName(pParse, pDb, pTable, pColumn, pSrcList, 0, pExpr) ){
        return 1;
      }
      break;
    }

    case TK_IN: {
      char affinity;
      Vdbe *v = sqlite3GetVdbe(pParse);
      KeyInfo keyInfo;

      if( v==0 ) return 1;
      if( sqlite3ExprResolveIds(pParse, pSrcList, pEList, pExpr->pLeft) ){
        return 1;
      }
      affinity = sqlite3ExprAffinity(pExpr->pLeft);

      /* Whether this is an 'x IN(SELECT...)' or an 'x IN(<exprlist>)'
      ** expression it is handled the same way. A temporary table is 
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
      memset(&keyInfo, 0, sizeof(keyInfo));
      keyInfo.nField = 1;
      keyInfo.aColl[0] = pParse->db->pDfltColl;
      sqlite3VdbeOp3(v, OP_OpenTemp, pExpr->iTable, 0, \
           (char*)&keyInfo, P3_KEYINFO);

      if( pExpr->pSelect ){
        /* Case 1:     expr IN (SELECT ...)
        **
        ** Generate code to write the results of the select into the temporary
        ** table allocated and opened above.
        */
        int iParm = pExpr->iTable +  (((int)affinity)<<16);
        assert( (pExpr->iTable&0x0000FFFF)==pExpr->iTable );
        sqlite3Select(pParse, pExpr->pSelect, SRT_Set, iParm, 0, 0, 0, 0);
      }else if( pExpr->pList ){
        /* Case 2:     expr IN (exprlist)
        **
	** For each expression, build an index key from the evaluation and
        ** store it in the temporary table. If <expr> is a column, then use
        ** that columns affinity when building index keys. If <expr> is not
        ** a column, use numeric affinity.
        */
        int i;
        char const *affStr;
        if( !affinity ){
          affinity = SQLITE_AFF_NUMERIC;
        }
        affStr = sqlite3AffinityString(affinity);

        /* Loop through each expression in <exprlist>. */
        for(i=0; i<pExpr->pList->nExpr; i++){
          Expr *pE2 = pExpr->pList->a[i].pExpr;

          /* Check that the expression is constant and valid. */
          if( !sqlite3ExprIsConstant(pE2) ){
            sqlite3ErrorMsg(pParse,
              "right-hand side of IN operator must be constant");
            return 1;
          }
          if( sqlite3ExprCheck(pParse, pE2, 0, 0) ){
            return 1;
          }

          /* Evaluate the expression and insert it into the temp table */
          sqlite3ExprCode(pParse, pE2);
          sqlite3VdbeOp3(v, OP_MakeKey, 1, 0, affStr, P3_STATIC);
          sqlite3VdbeAddOp(v, OP_String, 0, 0);
          sqlite3VdbeAddOp(v, OP_PutStrKey, pExpr->iTable, 0);
        }
      }
      break;
    }

    case TK_SELECT: {
      /* This has to be a scalar SELECT.  Generate code to put the
      ** value of this select in a memory cell and record the number
      ** of the memory cell in iColumn.
      */
      pExpr->iColumn = pParse->nMem++;
      if(sqlite3Select(pParse, pExpr->pSelect, SRT_Mem,pExpr->iColumn,0,0,0,0)){
        return 1;
      }
      break;
    }

    /* For all else, just recursively walk the tree */
    default: {
      if( pExpr->pLeft
      && sqlite3ExprResolveIds(pParse, pSrcList, pEList, pExpr->pLeft) ){
        return 1;
      }
      if( pExpr->pRight 
      && sqlite3ExprResolveIds(pParse, pSrcList, pEList, pExpr->pRight) ){
        return 1;
      }
      if( pExpr->pList ){
        int i;
        ExprList *pList = pExpr->pList;
        for(i=0; i<pList->nExpr; i++){
          Expr *pArg = pList->a[i].pExpr;
          if( sqlite3ExprResolveIds(pParse, pSrcList, pEList, pArg) ){
            return 1;
          }
        }
      }
    }
  }
  return 0;
}

/*
** pExpr is a node that defines a function of some kind.  It might
** be a syntactic function like "count(x)" or it might be a function
** that implements an operator, like "a LIKE b".  
**
** This routine makes *pzName point to the name of the function and 
** *pnName hold the number of characters in the function name.
*/
static void getFunctionName(Expr *pExpr, const char **pzName, int *pnName){
  switch( pExpr->op ){
    case TK_FUNCTION: {
      *pzName = pExpr->token.z;
      *pnName = pExpr->token.n;
      break;
    }
    case TK_LIKE: {
      *pzName = "like";
      *pnName = 4;
      break;
    }
    case TK_GLOB: {
      *pzName = "glob";
      *pnName = 4;
      break;
    }
    default: {
      *pzName = "can't happen";
      *pnName = 12;
      break;
    }
  }
}

/*
** Error check the functions in an expression.  Make sure all
** function names are recognized and all functions have the correct
** number of arguments.  Leave an error message in pParse->zErrMsg
** if anything is amiss.  Return the number of errors.
**
** if pIsAgg is not null and this expression is an aggregate function
** (like count(*) or max(value)) then write a 1 into *pIsAgg.
*/
int sqlite3ExprCheck(Parse *pParse, Expr *pExpr, int allowAgg, int *pIsAgg){
  int nErr = 0;
  if( pExpr==0 ) return 0;
  switch( pExpr->op ){
    case TK_GLOB:
    case TK_LIKE:
    case TK_FUNCTION: {
      int n = pExpr->pList ? pExpr->pList->nExpr : 0;  /* Number of arguments */
      int no_such_func = 0;       /* True if no such function exists */
      int wrong_num_args = 0;     /* True if wrong number of arguments */
      int is_agg = 0;             /* True if is an aggregate function */
      int i;
      int nId;                    /* Number of characters in function name */
      const char *zId;            /* The function name. */
      FuncDef *pDef;

      getFunctionName(pExpr, &zId, &nId);
      pDef = sqlite3FindFunction(pParse->db, zId, nId, n, 0);
      if( pDef==0 ){
        pDef = sqlite3FindFunction(pParse->db, zId, nId, -1, 0);
        if( pDef==0 ){
          no_such_func = 1;
        }else{
          wrong_num_args = 1;
        }
      }else{
        is_agg = pDef->xFunc==0;
      }
      if( is_agg && !allowAgg ){
        sqlite3ErrorMsg(pParse, "misuse of aggregate function %.*s()", nId, zId);
        nErr++;
        is_agg = 0;
      }else if( no_such_func ){
        sqlite3ErrorMsg(pParse, "no such function: %.*s", nId, zId);
        nErr++;
      }else if( wrong_num_args ){
        sqlite3ErrorMsg(pParse,"wrong number of arguments to function %.*s()",
             nId, zId);
        nErr++;
      }
      if( is_agg ){
        pExpr->op = TK_AGG_FUNCTION;
        if( pIsAgg ) *pIsAgg = 1;
      }
      for(i=0; nErr==0 && i<n; i++){
        nErr = sqlite3ExprCheck(pParse, pExpr->pList->a[i].pExpr,
                               allowAgg && !is_agg, pIsAgg);
      }
      /** TODO:  Compute pExpr->affinity based on the expected return
      ** type of the function */
    }
    default: {
      if( pExpr->pLeft ){
        nErr = sqlite3ExprCheck(pParse, pExpr->pLeft, allowAgg, pIsAgg);
      }
      if( nErr==0 && pExpr->pRight ){
        nErr = sqlite3ExprCheck(pParse, pExpr->pRight, allowAgg, pIsAgg);
      }
      if( nErr==0 && pExpr->pList ){
        int n = pExpr->pList->nExpr;
        int i;
        for(i=0; nErr==0 && i<n; i++){
          Expr *pE2 = pExpr->pList->a[i].pExpr;
          nErr = sqlite3ExprCheck(pParse, pE2, allowAgg, pIsAgg);
        }
      }
      break;
    }
  }
  return nErr;
}

/*
** Return one of the SQLITE_AFF_* affinity types that indicates the likely
** data type of the result of the given expression.
**
** Not every expression has a fixed type.  If the type cannot be determined
** at compile-time, then try to return the type affinity if the expression
** is a column.  Otherwise just return SQLITE_AFF_NONE.
**
** The sqlite3ExprResolveIds() and sqlite3ExprCheck() routines must have
** both been called on the expression before it is passed to this routine.
*/
int sqlite3ExprType(Expr *p){
  if( p==0 ) return SQLITE_AFF_NONE;
  while( p ) switch( p->op ){
    case TK_CONCAT:
    case TK_STRING:
      return SQLITE_AFF_TEXT;

    case TK_AS:
      p = p->pLeft;
      break;

    case TK_VARIABLE:
    case TK_NULL:
      return SQLITE_AFF_NONE;

    case TK_SELECT:   /*** FIX ME ****/
    case TK_COLUMN:   /*** FIX ME ****/
    case TK_CASE:     /*** FIX ME ****/

    default:
      return SQLITE_AFF_NUMERIC;
  }
  return SQLITE_AFF_NONE;
}

/*
** Generate an instruction that will put the integer describe by
** text z[0..n-1] on the stack.
*/
static void codeInteger(Vdbe *v, const char *z, int n){
  int i;
  if( sqlite3GetInt32(z, &i) || (i=0, sqlite3FitsIn64Bits(z))!=0 ){
    sqlite3VdbeOp3(v, OP_Integer, i, 0, z, n);
  }else{
    sqlite3VdbeOp3(v, OP_Real, 0, 0, z, n);
  }
}

/*
** Generate code into the current Vdbe to evaluate the given
** expression and leave the result on the top of stack.
*/
void sqlite3ExprCode(Parse *pParse, Expr *pExpr){
  Vdbe *v = pParse->pVdbe;
  int op;
  if( v==0 || pExpr==0 ) return;
  switch( pExpr->op ){
    case TK_PLUS:     op = OP_Add;      break;
    case TK_MINUS:    op = OP_Subtract; break;
    case TK_STAR:     op = OP_Multiply; break;
    case TK_SLASH:    op = OP_Divide;   break;
    case TK_AND:      op = OP_And;      break;
    case TK_OR:       op = OP_Or;       break;
    case TK_LT:       op = OP_Lt;       break;
    case TK_LE:       op = OP_Le;       break;
    case TK_GT:       op = OP_Gt;       break;
    case TK_GE:       op = OP_Ge;       break;
    case TK_NE:       op = OP_Ne;       break;
    case TK_EQ:       op = OP_Eq;       break;
    case TK_ISNULL:   op = OP_IsNull;   break;
    case TK_NOTNULL:  op = OP_NotNull;  break;
    case TK_NOT:      op = OP_Not;      break;
    case TK_UMINUS:   op = OP_Negative; break;
    case TK_BITAND:   op = OP_BitAnd;   break;
    case TK_BITOR:    op = OP_BitOr;    break;
    case TK_BITNOT:   op = OP_BitNot;   break;
    case TK_LSHIFT:   op = OP_ShiftLeft;  break;
    case TK_RSHIFT:   op = OP_ShiftRight; break;
    case TK_REM:      op = OP_Remainder;  break;
    case TK_FLOAT:    op = OP_Real;       break;
    case TK_STRING:   op = OP_String;     break;
    default: break;
  }
  switch( pExpr->op ){
    case TK_COLUMN: {
      if( pParse->useAgg ){
        sqlite3VdbeAddOp(v, OP_AggGet, 0, pExpr->iAgg);
      }else if( pExpr->iColumn>=0 ){
        sqlite3VdbeAddOp(v, OP_Column, pExpr->iTable, pExpr->iColumn);
      }else{
        sqlite3VdbeAddOp(v, OP_Recno, pExpr->iTable, 0);
      }
      break;
    }
    case TK_INTEGER: {
      codeInteger(v, pExpr->token.z, pExpr->token.n);
      break;
    }
    case TK_FLOAT:
    case TK_STRING: {
      sqlite3VdbeOp3(v, op, 0, 0, pExpr->token.z, pExpr->token.n);
      sqlite3VdbeDequoteP3(v, -1);
      break;
    }
    case TK_NULL: {
      sqlite3VdbeAddOp(v, OP_String, 0, 0);
      break;
    }
    case TK_VARIABLE: {
      sqlite3VdbeAddOp(v, OP_Variable, pExpr->iTable, 0);
      break;
    }
    case TK_LT:
    case TK_LE:
    case TK_GT:
    case TK_GE:
    case TK_NE:
    case TK_EQ: {
      int p1 = binaryCompareP1(pExpr->pLeft, pExpr->pRight, 0);
      sqlite3ExprCode(pParse, pExpr->pLeft);
      sqlite3ExprCode(pParse, pExpr->pRight);
      sqlite3VdbeAddOp(v, op, p1, 0);
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
    case TK_SLASH: {
      sqlite3ExprCode(pParse, pExpr->pLeft);
      sqlite3ExprCode(pParse, pExpr->pRight);
      sqlite3VdbeAddOp(v, op, 0, 0);
      break;
    }
    case TK_LSHIFT:
    case TK_RSHIFT: {
      sqlite3ExprCode(pParse, pExpr->pRight);
      sqlite3ExprCode(pParse, pExpr->pLeft);
      sqlite3VdbeAddOp(v, op, 0, 0);
      break;
    }
    case TK_CONCAT: {
      sqlite3ExprCode(pParse, pExpr->pLeft);
      sqlite3ExprCode(pParse, pExpr->pRight);
      sqlite3VdbeAddOp(v, OP_Concat, 2, 0);
      break;
    }
    case TK_UMINUS: {
      Expr *pLeft = pExpr->pLeft;
      assert( pLeft );
      if( pLeft->op==TK_FLOAT || pLeft->op==TK_INTEGER ){
        Token *p = &pLeft->token;
        char *z = sqliteMalloc( p->n + 2 );
        sprintf(z, "-%.*s", p->n, p->z);
        if( pLeft->op==TK_FLOAT ){
          sqlite3VdbeOp3(v, OP_Real, 0, 0, z, p->n+1);
        }else{
          codeInteger(v, z, p->n+1);
        }
        sqliteFree(z);
        break;
      }
      /* Fall through into TK_NOT */
    }
    case TK_BITNOT:
    case TK_NOT: {
      sqlite3ExprCode(pParse, pExpr->pLeft);
      sqlite3VdbeAddOp(v, op, 0, 0);
      break;
    }
    case TK_ISNULL:
    case TK_NOTNULL: {
      int dest;
      sqlite3VdbeAddOp(v, OP_Integer, 1, 0);
      sqlite3ExprCode(pParse, pExpr->pLeft);
      dest = sqlite3VdbeCurrentAddr(v) + 2;
      sqlite3VdbeAddOp(v, op, 1, dest);
      sqlite3VdbeAddOp(v, OP_AddImm, -1, 0);
    }
    break;
    case TK_AGG_FUNCTION: {
      sqlite3VdbeAddOp(v, OP_AggGet, 0, pExpr->iAgg);
      break;
    }
    case TK_GLOB:
    case TK_LIKE:
    case TK_FUNCTION: {
      ExprList *pList = pExpr->pList;
      int nExpr = pList ? pList->nExpr : 0;
      FuncDef *pDef;
      int nId;
      const char *zId;
      getFunctionName(pExpr, &zId, &nId);
      pDef = sqlite3FindFunction(pParse->db, zId, nId, nExpr, 0);
      assert( pDef!=0 );
      nExpr = sqlite3ExprCodeExprList(pParse, pList, pDef->includeTypes);
      /* FIX ME: The following is a temporary hack. */
      if( 0==sqlite3StrNICmp(zId, "classof", nId) ){
        assert( nExpr==1 );
        sqlite3VdbeAddOp(v, OP_Class, nExpr, 0);
      }else{
        sqlite3VdbeOp3(v, OP_Function, nExpr, 0, (char*)pDef, P3_POINTER);
      }
      break;
    }
    case TK_SELECT: {
      sqlite3VdbeAddOp(v, OP_MemLoad, pExpr->iColumn, 0);
      break;
    }
    case TK_IN: {
      int addr;
      char const *affStr;

      /* Figure out the affinity to use to create a key from the results
      ** of the expression. affinityStr stores a static string suitable for
      ** P3 of OP_MakeKey.
      */
      affStr = sqlite3AffinityString(comparisonAffinity(pExpr));

      sqlite3VdbeAddOp(v, OP_Integer, 1, 0);

      /* Code the <expr> from "<expr> IN (...)". The temporary table
      ** pExpr->iTable contains the values that make up the (...) set.
      */
      sqlite3ExprCode(pParse, pExpr->pLeft);
      addr = sqlite3VdbeCurrentAddr(v);
      sqlite3VdbeAddOp(v, OP_NotNull, -1, addr+4);            /* addr + 0 */
      sqlite3VdbeAddOp(v, OP_Pop, 2, 0);
      sqlite3VdbeAddOp(v, OP_String, 0, 0);
      sqlite3VdbeAddOp(v, OP_Goto, 0, addr+7);
      sqlite3VdbeOp3(v, OP_MakeKey, 1, 0, affStr, P3_STATIC); /* addr + 4 */
      sqlite3VdbeAddOp(v, OP_Found, pExpr->iTable, addr+7);
      sqlite3VdbeAddOp(v, OP_AddImm, -1, 0);                  /* addr + 6 */

      break;
    }
    case TK_BETWEEN: {
      sqlite3ExprCode(pParse, pExpr->pLeft);
      sqlite3VdbeAddOp(v, OP_Dup, 0, 0);
      sqlite3ExprCode(pParse, pExpr->pList->a[0].pExpr);
      sqlite3VdbeAddOp(v, OP_Ge, 0, 0);
      sqlite3VdbeAddOp(v, OP_Pull, 1, 0);
      sqlite3ExprCode(pParse, pExpr->pList->a[1].pExpr);
      sqlite3VdbeAddOp(v, OP_Le, 0, 0);
      sqlite3VdbeAddOp(v, OP_And, 0, 0);
      break;
    }
    case TK_UPLUS:
    case TK_AS: {
      sqlite3ExprCode(pParse, pExpr->pLeft);
      break;
    }
    case TK_CASE: {
      int expr_end_label;
      int jumpInst;
      int addr;
      int nExpr;
      int i;

      assert(pExpr->pList);
      assert((pExpr->pList->nExpr % 2) == 0);
      assert(pExpr->pList->nExpr > 0);
      nExpr = pExpr->pList->nExpr;
      expr_end_label = sqlite3VdbeMakeLabel(v);
      if( pExpr->pLeft ){
        sqlite3ExprCode(pParse, pExpr->pLeft);
      }
      for(i=0; i<nExpr; i=i+2){
        sqlite3ExprCode(pParse, pExpr->pList->a[i].pExpr);
        if( pExpr->pLeft ){
          sqlite3VdbeAddOp(v, OP_Dup, 1, 1);
          jumpInst = sqlite3VdbeAddOp(v, OP_Ne, 1, 0);
          sqlite3VdbeAddOp(v, OP_Pop, 1, 0);
        }else{
          jumpInst = sqlite3VdbeAddOp(v, OP_IfNot, 1, 0);
        }
        sqlite3ExprCode(pParse, pExpr->pList->a[i+1].pExpr);
        sqlite3VdbeAddOp(v, OP_Goto, 0, expr_end_label);
        addr = sqlite3VdbeCurrentAddr(v);
        sqlite3VdbeChangeP2(v, jumpInst, addr);
      }
      if( pExpr->pLeft ){
        sqlite3VdbeAddOp(v, OP_Pop, 1, 0);
      }
      if( pExpr->pRight ){
        sqlite3ExprCode(pParse, pExpr->pRight);
      }else{
        sqlite3VdbeAddOp(v, OP_String, 0, 0);
      }
      sqlite3VdbeResolveLabel(v, expr_end_label);
      break;
    }
    case TK_RAISE: {
      if( !pParse->trigStack ){
        sqlite3ErrorMsg(pParse,
                       "RAISE() may only be used within a trigger-program");
        pParse->nErr++;
	return;
      }
      if( pExpr->iColumn == OE_Rollback ||
	  pExpr->iColumn == OE_Abort ||
	  pExpr->iColumn == OE_Fail ){
	  sqlite3VdbeOp3(v, OP_Halt, SQLITE_CONSTRAINT, pExpr->iColumn,
                           pExpr->token.z, pExpr->token.n);
	  sqlite3VdbeDequoteP3(v, -1);
      } else {
	  assert( pExpr->iColumn == OE_Ignore );
	  sqlite3VdbeOp3(v, OP_Goto, 0, pParse->trigStack->ignoreJump,
                           "(IGNORE jump)", 0);
      }
    }
    break;
  }
}

/*
** Generate code that pushes the value of every element of the given
** expression list onto the stack.  If the includeTypes flag is true,
** then also push a string that is the datatype of each element onto
** the stack after the value.
**
** Return the number of elements pushed onto the stack.
*/
int sqlite3ExprCodeExprList(
  Parse *pParse,     /* Parsing context */
  ExprList *pList,   /* The expression list to be coded */
  int includeTypes   /* TRUE to put datatypes on the stack too */
){
  struct ExprList_item *pItem;
  int i, n;
  Vdbe *v;
  if( pList==0 ) return 0;
  v = sqlite3GetVdbe(pParse);
  n = pList->nExpr;
  for(pItem=pList->a, i=0; i<n; i++, pItem++){
    sqlite3ExprCode(pParse, pItem->pExpr);
    if( includeTypes ){
      /** DEPRECATED.  This will go away with the new function interface **/
      sqlite3VdbeOp3(v, OP_String, 0, 0, "numeric", P3_STATIC);
    }
  }
  return includeTypes ? n*2 : n;
}

/*
** Generate code for a boolean expression such that a jump is made
** to the label "dest" if the expression is true but execution
** continues straight thru if the expression is false.
**
** If the expression evaluates to NULL (neither true nor false), then
** take the jump if the jumpIfNull flag is true.
*/
void sqlite3ExprIfTrue(Parse *pParse, Expr *pExpr, int dest, int jumpIfNull){
  Vdbe *v = pParse->pVdbe;
  int op = 0;
  if( v==0 || pExpr==0 ) return;
  switch( pExpr->op ){
    case TK_LT:       op = OP_Lt;       break;
    case TK_LE:       op = OP_Le;       break;
    case TK_GT:       op = OP_Gt;       break;
    case TK_GE:       op = OP_Ge;       break;
    case TK_NE:       op = OP_Ne;       break;
    case TK_EQ:       op = OP_Eq;       break;
    case TK_ISNULL:   op = OP_IsNull;   break;
    case TK_NOTNULL:  op = OP_NotNull;  break;
    default:  break;
  }
  switch( pExpr->op ){
    case TK_AND: {
      int d2 = sqlite3VdbeMakeLabel(v);
      sqlite3ExprIfFalse(pParse, pExpr->pLeft, d2, !jumpIfNull);
      sqlite3ExprIfTrue(pParse, pExpr->pRight, dest, jumpIfNull);
      sqlite3VdbeResolveLabel(v, d2);
      break;
    }
    case TK_OR: {
      sqlite3ExprIfTrue(pParse, pExpr->pLeft, dest, jumpIfNull);
      sqlite3ExprIfTrue(pParse, pExpr->pRight, dest, jumpIfNull);
      break;
    }
    case TK_NOT: {
      sqlite3ExprIfFalse(pParse, pExpr->pLeft, dest, jumpIfNull);
      break;
    }
    case TK_LT:
    case TK_LE:
    case TK_GT:
    case TK_GE:
    case TK_NE:
    case TK_EQ: {
      int p1 = binaryCompareP1(pExpr->pLeft, pExpr->pRight, jumpIfNull);
      sqlite3ExprCode(pParse, pExpr->pLeft);
      sqlite3ExprCode(pParse, pExpr->pRight);
      sqlite3VdbeAddOp(v, op, p1, dest);
      break;
    }
    case TK_ISNULL:
    case TK_NOTNULL: {
      sqlite3ExprCode(pParse, pExpr->pLeft);
      sqlite3VdbeAddOp(v, op, 1, dest);
      break;
    }
#if 0
    case TK_IN: {
      int addr;
      sqlite3ExprCode(pParse, pExpr->pLeft);
      addr = sqlite3VdbeCurrentAddr(v);
      sqlite3VdbeAddOp(v, OP_NotNull, -1, addr+3);
      sqlite3VdbeAddOp(v, OP_Pop, 1, 0);
      sqlite3VdbeAddOp(v, OP_Goto, 0, jumpIfNull ? dest : addr+4);
      if( pExpr->pSelect ){
        sqlite3VdbeAddOp(v, OP_Found, pExpr->iTable, dest);
      }else{
        sqlite3VdbeAddOp(v, OP_SetFound, pExpr->iTable, dest);
      }
      break;
    }
#endif
    case TK_BETWEEN: {
      int addr;
      sqlite3ExprCode(pParse, pExpr->pLeft);
      sqlite3VdbeAddOp(v, OP_Dup, 0, 0);
      sqlite3ExprCode(pParse, pExpr->pList->a[0].pExpr);
      addr = sqlite3VdbeAddOp(v, OP_Lt, !jumpIfNull, 0);
      sqlite3ExprCode(pParse, pExpr->pList->a[1].pExpr);
      sqlite3VdbeAddOp(v, OP_Le, jumpIfNull, dest);
      sqlite3VdbeAddOp(v, OP_Integer, 0, 0);
      sqlite3VdbeChangeP2(v, addr, sqlite3VdbeCurrentAddr(v));
      sqlite3VdbeAddOp(v, OP_Pop, 1, 0);
      break;
    }
    default: {
      sqlite3ExprCode(pParse, pExpr);
      sqlite3VdbeAddOp(v, OP_If, jumpIfNull, dest);
      break;
    }
  }
}

/*
** Generate code for a boolean expression such that a jump is made
** to the label "dest" if the expression is false but execution
** continues straight thru if the expression is true.
**
** If the expression evaluates to NULL (neither true nor false) then
** jump if jumpIfNull is true or fall through if jumpIfNull is false.
*/
void sqlite3ExprIfFalse(Parse *pParse, Expr *pExpr, int dest, int jumpIfNull){
  Vdbe *v = pParse->pVdbe;
  int op = 0;
  if( v==0 || pExpr==0 ) return;
  switch( pExpr->op ){
    case TK_LT:       op = OP_Ge;       break;
    case TK_LE:       op = OP_Gt;       break;
    case TK_GT:       op = OP_Le;       break;
    case TK_GE:       op = OP_Lt;       break;
    case TK_NE:       op = OP_Eq;       break;
    case TK_EQ:       op = OP_Ne;       break;
    case TK_ISNULL:   op = OP_NotNull;  break;
    case TK_NOTNULL:  op = OP_IsNull;   break;
    default:  break;
  }
  switch( pExpr->op ){
    case TK_AND: {
      sqlite3ExprIfFalse(pParse, pExpr->pLeft, dest, jumpIfNull);
      sqlite3ExprIfFalse(pParse, pExpr->pRight, dest, jumpIfNull);
      break;
    }
    case TK_OR: {
      int d2 = sqlite3VdbeMakeLabel(v);
      sqlite3ExprIfTrue(pParse, pExpr->pLeft, d2, !jumpIfNull);
      sqlite3ExprIfFalse(pParse, pExpr->pRight, dest, jumpIfNull);
      sqlite3VdbeResolveLabel(v, d2);
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
      int p1 = binaryCompareP1(pExpr->pLeft, pExpr->pRight, jumpIfNull);
      sqlite3ExprCode(pParse, pExpr->pLeft);
      sqlite3ExprCode(pParse, pExpr->pRight);
      sqlite3VdbeAddOp(v, op, p1, dest);
      break;
    }
    case TK_ISNULL:
    case TK_NOTNULL: {
      sqlite3ExprCode(pParse, pExpr->pLeft);
      sqlite3VdbeAddOp(v, op, 1, dest);
      break;
    }
#if 0
    case TK_IN: {
      int addr;
      sqlite3ExprCode(pParse, pExpr->pLeft);
      addr = sqlite3VdbeCurrentAddr(v);
      sqlite3VdbeAddOp(v, OP_NotNull, -1, addr+3);
      sqlite3VdbeAddOp(v, OP_Pop, 1, 0);
      sqlite3VdbeAddOp(v, OP_Goto, 0, jumpIfNull ? dest : addr+4);
      if( pExpr->pSelect ){
        sqlite3VdbeAddOp(v, OP_NotFound, pExpr->iTable, dest);
      }else{
        sqlite3VdbeAddOp(v, OP_SetNotFound, pExpr->iTable, dest);
      }
      break;
    }
#endif
    case TK_BETWEEN: {
      int addr;
      sqlite3ExprCode(pParse, pExpr->pLeft);
      sqlite3VdbeAddOp(v, OP_Dup, 0, 0);
      sqlite3ExprCode(pParse, pExpr->pList->a[0].pExpr);
      addr = sqlite3VdbeCurrentAddr(v);
      sqlite3VdbeAddOp(v, OP_Ge, !jumpIfNull, addr+3);
      sqlite3VdbeAddOp(v, OP_Pop, 1, 0);
      sqlite3VdbeAddOp(v, OP_Goto, 0, dest);
      sqlite3ExprCode(pParse, pExpr->pList->a[1].pExpr);
      sqlite3VdbeAddOp(v, OP_Gt, jumpIfNull, dest);
      break;
    }
    default: {
      sqlite3ExprCode(pParse, pExpr);
      sqlite3VdbeAddOp(v, OP_IfNot, jumpIfNull, dest);
      break;
    }
  }
}

/*
** Do a deep comparison of two expression trees.  Return TRUE (non-zero)
** if they are identical and return FALSE if they differ in any way.
*/
int sqlite3ExprCompare(Expr *pA, Expr *pB){
  int i;
  if( pA==0 ){
    return pB==0;
  }else if( pB==0 ){
    return 0;
  }
  if( pA->op!=pB->op ) return 0;
  if( !sqlite3ExprCompare(pA->pLeft, pB->pLeft) ) return 0;
  if( !sqlite3ExprCompare(pA->pRight, pB->pRight) ) return 0;
  if( pA->pList ){
    if( pB->pList==0 ) return 0;
    if( pA->pList->nExpr!=pB->pList->nExpr ) return 0;
    for(i=0; i<pA->pList->nExpr; i++){
      if( !sqlite3ExprCompare(pA->pList->a[i].pExpr, pB->pList->a[i].pExpr) ){
        return 0;
      }
    }
  }else if( pB->pList ){
    return 0;
  }
  if( pA->pSelect || pB->pSelect ) return 0;
  if( pA->iTable!=pB->iTable || pA->iColumn!=pB->iColumn ) return 0;
  if( pA->token.z ){
    if( pB->token.z==0 ) return 0;
    if( pB->token.n!=pA->token.n ) return 0;
    if( sqlite3StrNICmp(pA->token.z, pB->token.z, pB->token.n)!=0 ) return 0;
  }
  return 1;
}

/*
** Add a new element to the pParse->aAgg[] array and return its index.
*/
static int appendAggInfo(Parse *pParse){
  if( (pParse->nAgg & 0x7)==0 ){
    int amt = pParse->nAgg + 8;
    AggExpr *aAgg = sqliteRealloc(pParse->aAgg, amt*sizeof(pParse->aAgg[0]));
    if( aAgg==0 ){
      return -1;
    }
    pParse->aAgg = aAgg;
  }
  memset(&pParse->aAgg[pParse->nAgg], 0, sizeof(pParse->aAgg[0]));
  return pParse->nAgg++;
}

/*
** Analyze the given expression looking for aggregate functions and
** for variables that need to be added to the pParse->aAgg[] array.
** Make additional entries to the pParse->aAgg[] array as necessary.
**
** This routine should only be called after the expression has been
** analyzed by sqlite3ExprResolveIds() and sqlite3ExprCheck().
**
** If errors are seen, leave an error message in zErrMsg and return
** the number of errors.
*/
int sqlite3ExprAnalyzeAggregates(Parse *pParse, Expr *pExpr){
  int i;
  AggExpr *aAgg;
  int nErr = 0;

  if( pExpr==0 ) return 0;
  switch( pExpr->op ){
    case TK_COLUMN: {
      aAgg = pParse->aAgg;
      for(i=0; i<pParse->nAgg; i++){
        if( aAgg[i].isAgg ) continue;
        if( aAgg[i].pExpr->iTable==pExpr->iTable
         && aAgg[i].pExpr->iColumn==pExpr->iColumn ){
          break;
        }
      }
      if( i>=pParse->nAgg ){
        i = appendAggInfo(pParse);
        if( i<0 ) return 1;
        pParse->aAgg[i].isAgg = 0;
        pParse->aAgg[i].pExpr = pExpr;
      }
      pExpr->iAgg = i;
      break;
    }
    case TK_AGG_FUNCTION: {
      aAgg = pParse->aAgg;
      for(i=0; i<pParse->nAgg; i++){
        if( !aAgg[i].isAgg ) continue;
        if( sqlite3ExprCompare(aAgg[i].pExpr, pExpr) ){
          break;
        }
      }
      if( i>=pParse->nAgg ){
        i = appendAggInfo(pParse);
        if( i<0 ) return 1;
        pParse->aAgg[i].isAgg = 1;
        pParse->aAgg[i].pExpr = pExpr;
        pParse->aAgg[i].pFunc = sqlite3FindFunction(pParse->db,
             pExpr->token.z, pExpr->token.n,
             pExpr->pList ? pExpr->pList->nExpr : 0, 0);
      }
      pExpr->iAgg = i;
      break;
    }
    default: {
      if( pExpr->pLeft ){
        nErr = sqlite3ExprAnalyzeAggregates(pParse, pExpr->pLeft);
      }
      if( nErr==0 && pExpr->pRight ){
        nErr = sqlite3ExprAnalyzeAggregates(pParse, pExpr->pRight);
      }
      if( nErr==0 && pExpr->pList ){
        int n = pExpr->pList->nExpr;
        int i;
        for(i=0; nErr==0 && i<n; i++){
          nErr = sqlite3ExprAnalyzeAggregates(pParse, pExpr->pList->a[i].pExpr);
        }
      }
      break;
    }
  }
  return nErr;
}

/*
** Locate a user function given a name and a number of arguments.
** Return a pointer to the FuncDef structure that defines that
** function, or return NULL if the function does not exist.
**
** If the createFlag argument is true, then a new (blank) FuncDef
** structure is created and liked into the "db" structure if a
** no matching function previously existed.  When createFlag is true
** and the nArg parameter is -1, then only a function that accepts
** any number of arguments will be returned.
**
** If createFlag is false and nArg is -1, then the first valid
** function found is returned.  A function is valid if either xFunc
** or xStep is non-zero.
*/
FuncDef *sqlite3FindFunction(
  sqlite *db,        /* An open database */
  const char *zName, /* Name of the function.  Not null-terminated */
  int nName,         /* Number of characters in the name */
  int nArg,          /* Number of arguments.  -1 means any number */
  int createFlag     /* Create new entry if true and does not otherwise exist */
){
  FuncDef *pFirst, *p, *pMaybe;
  pFirst = p = (FuncDef*)sqlite3HashFind(&db->aFunc, zName, nName);
  if( p && !createFlag && nArg<0 ){
    while( p && p->xFunc==0 && p->xStep==0 ){ p = p->pNext; }
    return p;
  }
  pMaybe = 0;
  while( p && p->nArg!=nArg ){
    if( p->nArg<0 && !createFlag && (p->xFunc || p->xStep) ) pMaybe = p;
    p = p->pNext;
  }
  if( p && !createFlag && p->xFunc==0 && p->xStep==0 ){
    return 0;
  }
  if( p==0 && pMaybe ){
    assert( createFlag==0 );
    return pMaybe;
  }
  if( p==0 && createFlag && (p = sqliteMalloc(sizeof(*p)))!=0 ){
    p->nArg = nArg;
    p->pNext = pFirst;
    p->dataType = pFirst ? pFirst->dataType : SQLITE_NUMERIC;
    sqlite3HashInsert(&db->aFunc, zName, nName, (void*)p);
  }
  return p;
}
