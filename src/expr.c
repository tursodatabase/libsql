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
** This file contains routines used for analyzing expressions and
** for generating VDBE code that evaluates expressions.
**
** $Id: expr.c,v 1.18 2000/06/21 13:59:11 drh Exp $
*/
#include "sqliteInt.h"

/*
** Walk an expression tree.  Return 1 if the expression is constant
** and 0 if it involves variables.
*/
static int isConstant(Expr *p){
  switch( p->op ){
    case TK_ID:
    case TK_COLUMN:
    case TK_DOT:
      return 0;
    default: {
      if( p->pLeft && !isConstant(p->pLeft) ) return 0;
      if( p->pRight && !isConstant(p->pRight) ) return 0;
      if( p->pList ){
        int i;
        for(i=0; i<p->pList->nExpr; i++){
          if( !isConstant(p->pList->a[i].pExpr) ) return 0;
        }
      }
      break;
    }
  }
  return 1;
}

/*
** Walk the expression tree and process operators of the form:
**
**       expr IN (SELECT ...)
**
** These operators have to be processed before column names are
** resolved because each such operator increments pParse->nTab
** to reserve cursor numbers for its own use.  But pParse->nTab
** needs to be constant once we begin resolving column names.
**
** Actually, the processing of IN-SELECT is only started by this
** routine.  This routine allocates a cursor number to the IN-SELECT
** and then moves on.  The code generation is done by 
** sqliteExprResolveIds() which must be called afterwards.
*/
void sqliteExprResolveInSelect(Parse *pParse, Expr *pExpr){
  if( pExpr==0 ) return;
  if( pExpr->op==TK_IN && pExpr->pSelect!=0 ){
    pExpr->iTable = pParse->nTab++;
  }else{
    if( pExpr->pLeft ) sqliteExprResolveInSelect(pParse, pExpr->pLeft);
    if( pExpr->pRight ) sqliteExprResolveInSelect(pParse, pExpr->pRight);
    if( pExpr->pList ){
      int i;
      ExprList *pList = pExpr->pList;
      for(i=0; i<pList->nExpr; i++){
        sqliteExprResolveInSelect(pParse, pList->a[i].pExpr);
      }
    }
  }
}

/*
** This routine walks an expression tree and resolves references to
** table columns.  Nodes of the form ID.ID or ID resolve into an
** index to the table in the table list and a column offset.  The opcode
** for such nodes is changed to TK_COLUMN.  The iTable value is changed
** to the index of the referenced table in pTabList plus the pParse->nTab
** value.  The iColumn value is changed to the index of the column of the 
** referenced table.
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
int sqliteExprResolveIds(Parse *pParse, IdList *pTabList, Expr *pExpr){
  if( pExpr==0 ) return 0;
  switch( pExpr->op ){
    /* A lone identifier */
    case TK_ID: {
      int cnt = 0;   /* Number of matches */
      int i;         /* Loop counter */
      char *z = sqliteStrNDup(pExpr->token.z, pExpr->token.n);
      for(i=0; i<pTabList->nId; i++){
        int j;
        Table *pTab = pTabList->a[i].pTab;
        if( pTab==0 ) continue;
        for(j=0; j<pTab->nCol; j++){
          if( sqliteStrICmp(pTab->aCol[j].zName, z)==0 ){
            cnt++;
            pExpr->iTable = i + pParse->nTab;
            pExpr->iColumn = j;
          }
        }
      }
      sqliteFree(z);
      if( cnt==0 ){
        sqliteSetNString(&pParse->zErrMsg, "no such column: ", -1,  
          pExpr->token.z, pExpr->token.n, 0);
        pParse->nErr++;
        return 1;
      }else if( cnt>1 ){
        sqliteSetNString(&pParse->zErrMsg, "ambiguous column name: ", -1,  
          pExpr->token.z, pExpr->token.n, 0);
        pParse->nErr++;
        return 1;
      }
      pExpr->op = TK_COLUMN;
      break; 
    }
  
    /* A table name and column name:  ID.ID */
    case TK_DOT: {
      int cnt = 0;             /* Number of matches */
      int i;                   /* Loop counter */
      Expr *pLeft, *pRight;    /* Left and right subbranches of the expr */
      char *zLeft, *zRight;    /* Text of an identifier */

      pLeft = pExpr->pLeft;
      pRight = pExpr->pRight;
      assert( pLeft && pLeft->op==TK_ID );
      assert( pRight && pRight->op==TK_ID );
      zLeft = sqliteStrNDup(pLeft->token.z, pLeft->token.n);
      zRight = sqliteStrNDup(pRight->token.z, pRight->token.n);
      for(i=0; i<pTabList->nId; i++){
        int j;
        char *zTab;
        Table *pTab = pTabList->a[i].pTab;
        if( pTab==0 ) continue;
        if( pTabList->a[i].zAlias ){
          zTab = pTabList->a[i].zAlias;
        }else{
          zTab = pTab->zName;
        }
        if( sqliteStrICmp(zTab, zLeft)!=0 ) continue;
        for(j=0; j<pTab->nCol; j++){
          if( sqliteStrICmp(pTab->aCol[j].zName, zRight)==0 ){
            cnt++;
            pExpr->iTable = i + pParse->nTab;
            pExpr->iColumn = j;
          }
        }
      }
      sqliteFree(zLeft);
      sqliteFree(zRight);
      if( cnt==0 ){
        sqliteSetNString(&pParse->zErrMsg, "no such column: ", -1,  
          pLeft->token.z, pLeft->token.n, ".", 1, 
          pRight->token.z, pRight->token.n, 0);
        pParse->nErr++;
        return 1;
      }else if( cnt>1 ){
        sqliteSetNString(&pParse->zErrMsg, "ambiguous column name: ", -1,  
          pLeft->token.z, pLeft->token.n, ".", 1,
          pRight->token.z, pRight->token.n, 0);
        pParse->nErr++;
        return 1;
      }
      sqliteExprDelete(pLeft);
      pExpr->pLeft = 0;
      sqliteExprDelete(pRight);
      pExpr->pRight = 0;
      pExpr->op = TK_COLUMN;
      break;
    }

    case TK_IN: {
      Vdbe *v = sqliteGetVdbe(pParse);
      if( v==0 ) return 1;
      if( sqliteExprResolveIds(pParse, pTabList, pExpr->pLeft) ){
        return 1;
      }
      if( pExpr->pSelect ){
        /* Case 1:     expr IN (SELECT ...)
        **
        ** Generate code to write the results of the select into a temporary
        ** table.  The cursor number of the temporary table has already
        ** been put in iTable by sqliteExprResolveInSelect().
        */
        sqliteVdbeAddOp(v, OP_Open, pExpr->iTable, 1, 0, 0);
        if( sqliteSelect(pParse, pExpr->pSelect, SRT_Set, pExpr->iTable) );
      }else if( pExpr->pList ){
        /* Case 2:     expr IN (exprlist)
        **
        ** Create a set to put the exprlist values in.  The Set id is stored
        ** in iTable.
        */
        int i, iSet;
        for(i=0; i<pExpr->pList->nExpr; i++){
          Expr *pE2 = pExpr->pList->a[i].pExpr;
          if( !isConstant(pE2) ){
            sqliteSetString(&pParse->zErrMsg,
              "right-hand side of IN operator must be constant", 0);
            pParse->nErr++;
            return 1;
          }
          if( sqliteExprCheck(pParse, pE2, 0, 0) ){
            return 1;
          }
        }
        iSet = pExpr->iTable = pParse->nSet++;
        for(i=0; i<pExpr->pList->nExpr; i++){
          Expr *pE2 = pExpr->pList->a[i].pExpr;
          switch( pE2->op ){
            case TK_FLOAT:
            case TK_INTEGER:
            case TK_STRING: {
              int addr = sqliteVdbeAddOp(v, OP_SetInsert, iSet, 0, 0, 0);
              sqliteVdbeChangeP3(v, addr, pE2->token.z, pE2->token.n);
              sqliteVdbeDequoteP3(v, addr);
              break;
            }
            default: {
              sqliteExprCode(pParse, pE2);
              sqliteVdbeAddOp(v, OP_SetInsert, iSet, 0, 0, 0);
              break;
            }
          }
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
      if( sqliteSelect(pParse, pExpr->pSelect, SRT_Mem, pExpr->iColumn) ){
        return 1;
      }
      break;
    }

    /* For all else, just recursively walk the tree */
    default: {
      if( pExpr->pLeft
      && sqliteExprResolveIds(pParse, pTabList, pExpr->pLeft) ){
        return 1;
      }
      if( pExpr->pRight 
      && sqliteExprResolveIds(pParse, pTabList, pExpr->pRight) ){
        return 1;
      }
      if( pExpr->pList ){
        int i;
        ExprList *pList = pExpr->pList;
        for(i=0; i<pList->nExpr; i++){
          if( sqliteExprResolveIds(pParse, pTabList, pList->a[i].pExpr) ){
            return 1;
          }
        }
      }
    }
  }
  return 0;
}

#if 0 /* NOT USED */
/*
** Compare a token against a string.  Return TRUE if they match.
*/
static int sqliteTokenCmp(Token *pToken, const char *zStr){
  int n = strlen(zStr);
  if( n!=pToken->n ) return 0;
  return sqliteStrNICmp(pToken->z, zStr, n)==0;
}
#endif

/*
** Convert a function name into its integer identifier.  Return the
** identifier.  Return FN_Unknown if the function name is unknown.
*/
int sqliteFuncId(Token *pToken){
  static const struct {
     char *zName;
     int len;
     int id;
  } aFunc[] = {
     { "count",  5, FN_Count },
     { "min",    3, FN_Min   },
     { "max",    3, FN_Max   },
     { "sum",    3, FN_Sum   },
     { "avg",    3, FN_Avg   },
     { "fcnt",   4, FN_Fcnt  },  /* Used for testing only */
  };
  int i;
  for(i=0; i<ArraySize(aFunc); i++){
    if( aFunc[i].len==pToken->n 
     && sqliteStrNICmp(pToken->z, aFunc[i].zName, aFunc[i].len)==0 ){
       return aFunc[i].id;
    }
  }
  return FN_Unknown;
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
int sqliteExprCheck(Parse *pParse, Expr *pExpr, int allowAgg, int *pIsAgg){
  int nErr = 0;
  if( pExpr==0 ) return 0;
  switch( pExpr->op ){
    case TK_FUNCTION: {
      int id = sqliteFuncId(&pExpr->token);
      int n = pExpr->pList ? pExpr->pList->nExpr : 0;
      int no_such_func = 0;
      int too_many_args = 0;
      int too_few_args = 0;
      int is_agg = 0;
      int i;
      pExpr->iColumn = id;
      switch( id ){
        case FN_Unknown: { 
          no_such_func = 1;
          break;
        }
        case FN_Count: { 
          no_such_func = !allowAgg;
          too_many_args = n>1;
          is_agg = 1;
          break;
        }
        case FN_Max:
        case FN_Min: {
          too_few_args = allowAgg ? n<1 : n<2;
          is_agg = n==1;
          break;
        }
        case FN_Avg:
        case FN_Sum: {
          no_such_func = !allowAgg;
          too_many_args = n>1;
          too_few_args = n<1;
          is_agg = 1;
          break;
        }
        /* The "fcnt(*)" function always returns the number of fetch
        ** operations that have occurred so far while processing the
        ** SQL statement.  This information can be used by test procedures
        ** to verify that indices are being used properly to minimize
        ** searching.  All arguments to fcnt() are ignored.  fcnt() has
        ** no use (other than testing) that we are aware of.
        */
        case FN_Fcnt: {
          n = 0;
          break;
        }
        default: break;
      }
      if( no_such_func ){
        sqliteSetNString(&pParse->zErrMsg, "no such function: ", -1,
           pExpr->token.z, pExpr->token.n, 0);
        pParse->nErr++;
        nErr++;
      }else if( too_many_args ){
        sqliteSetNString(&pParse->zErrMsg, "too many arguments to function ",-1,
           pExpr->token.z, pExpr->token.n, "()", 2, 0);
        pParse->nErr++;
        nErr++;
      }else if( too_few_args ){
        sqliteSetNString(&pParse->zErrMsg, "too few arguments to function ",-1,
           pExpr->token.z, pExpr->token.n, "()", 2, 0);
        pParse->nErr++;
        nErr++;
      }
      if( is_agg ) pExpr->op = TK_AGG_FUNCTION;
      if( is_agg && pIsAgg ) *pIsAgg = 1;
      for(i=0; nErr==0 && i<n; i++){
        nErr = sqliteExprCheck(pParse, pExpr->pList->a[i].pExpr,
                               allowAgg && !is_agg, pIsAgg);
      }
    }
    default: {
      if( pExpr->pLeft ){
        nErr = sqliteExprCheck(pParse, pExpr->pLeft, allowAgg, pIsAgg);
      }
      if( nErr==0 && pExpr->pRight ){
        nErr = sqliteExprCheck(pParse, pExpr->pRight, allowAgg, pIsAgg);
      }
      if( nErr==0 && pExpr->pList ){
        int n = pExpr->pList->nExpr;
        int i;
        for(i=0; nErr==0 && i<n; i++){
          Expr *pE2 = pExpr->pList->a[i].pExpr;
          nErr = sqliteExprCheck(pParse, pE2, allowAgg, pIsAgg);
        }
      }
      break;
    }
  }
  return nErr;
}

/*
** Generate code into the current Vdbe to evaluate the given
** expression and leave the result on the top of stack.
*/
void sqliteExprCode(Parse *pParse, Expr *pExpr){
  Vdbe *v = pParse->pVdbe;
  int op;
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
    case TK_LIKE:     op = OP_Like;     break;
    case TK_GLOB:     op = OP_Glob;     break;
    case TK_ISNULL:   op = OP_IsNull;   break;
    case TK_NOTNULL:  op = OP_NotNull;  break;
    case TK_NOT:      op = OP_Not;      break;
    case TK_UMINUS:   op = OP_Negative; break;
    default: break;
  }
  switch( pExpr->op ){
    case TK_COLUMN: {
      if( pParse->useAgg ){
        sqliteVdbeAddOp(v, OP_AggGet, 0, pExpr->iAgg, 0, 0);
      }else{
        sqliteVdbeAddOp(v, OP_Field, pExpr->iTable, pExpr->iColumn, 0, 0);
      }
      break;
    }
    case TK_INTEGER: {
      int i = atoi(pExpr->token.z);
      sqliteVdbeAddOp(v, OP_Integer, i, 0, 0, 0);
      break;
    }
    case TK_FLOAT: {
      int addr = sqliteVdbeAddOp(v, OP_String, 0, 0, 0, 0);
      sqliteVdbeChangeP3(v, addr, pExpr->token.z, pExpr->token.n);
      break;
    }
    case TK_STRING: {
      int addr = sqliteVdbeAddOp(v, OP_String, 0, 0, 0, 0);
      sqliteVdbeChangeP3(v, addr, pExpr->token.z, pExpr->token.n);
      sqliteVdbeDequoteP3(v, addr);
      break;
    }
    case TK_NULL: {
      sqliteVdbeAddOp(v, OP_Null, 0, 0, 0, 0);
      break;
    }
    case TK_AND:
    case TK_OR:
    case TK_PLUS:
    case TK_STAR:
    case TK_MINUS:
    case TK_SLASH: {
      sqliteExprCode(pParse, pExpr->pLeft);
      sqliteExprCode(pParse, pExpr->pRight);
      sqliteVdbeAddOp(v, op, 0, 0, 0, 0);
      break;
    }
    case TK_CONCAT: {
      sqliteExprCode(pParse, pExpr->pLeft);
      sqliteExprCode(pParse, pExpr->pRight);
      sqliteVdbeAddOp(v, OP_Concat, 2, 0, 0, 0);
      break;
    }
    case TK_LT:
    case TK_LE:
    case TK_GT:
    case TK_GE:
    case TK_NE:
    case TK_EQ: 
    case TK_LIKE: 
    case TK_GLOB: {
      int dest;
      sqliteVdbeAddOp(v, OP_Integer, 1, 0, 0, 0);
      sqliteExprCode(pParse, pExpr->pLeft);
      sqliteExprCode(pParse, pExpr->pRight);
      dest = sqliteVdbeCurrentAddr(v) + 2;
      sqliteVdbeAddOp(v, op, 0, dest, 0, 0);
      sqliteVdbeAddOp(v, OP_AddImm, -1, 0, 0, 0);
      break;
    }
    case TK_UMINUS: {
      assert( pExpr->pLeft );
      if( pExpr->pLeft->op==TK_INTEGER ){
        int i = atoi(pExpr->pLeft->token.z);
        sqliteVdbeAddOp(v, OP_Integer, -i, 0, 0, 0);
        break;
      }else if( pExpr->pLeft->op==TK_FLOAT ){
        Token *p = &pExpr->pLeft->token;
        char *z = sqliteMalloc( p->n + 2 );
        sprintf(z, "-%.*s", p->n, p->z);
        sqliteVdbeAddOp(v, OP_String, 0, 0, z, 0);
        sqliteFree(z);
        break;
      }
      /* Fall through into TK_NOT */
    }
    case TK_NOT: {
      sqliteExprCode(pParse, pExpr->pLeft);
      sqliteVdbeAddOp(v, op, 0, 0, 0, 0);
      break;
    }
    case TK_ISNULL:
    case TK_NOTNULL: {
      int dest;
      sqliteVdbeAddOp(v, OP_Integer, 1, 0, 0, 0);
      sqliteExprCode(pParse, pExpr->pLeft);
      dest = sqliteVdbeCurrentAddr(v) + 2;
      sqliteVdbeAddOp(v, op, 0, dest, 0, 0);
      sqliteVdbeAddOp(v, OP_AddImm, -1, 0, 0, 0);
      break;
    }
    case TK_AGG_FUNCTION: {
      sqliteVdbeAddOp(v, OP_AggGet, 0, pExpr->iAgg, 0, 0);
      if( pExpr->iColumn==FN_Avg ){
        assert( pParse->iAggCount>=0 && pParse->iAggCount<pParse->nAgg );
        sqliteVdbeAddOp(v, OP_AggGet, 0, pParse->iAggCount, 0, 0);
        sqliteVdbeAddOp(v, OP_Divide, 0, 0, 0, 0);
      }
      break;
    }
    case TK_FUNCTION: {
      int id = pExpr->iColumn;
      int op;
      int i;
      ExprList *pList = pExpr->pList;
      if( id==FN_Fcnt ){
        sqliteVdbeAddOp(v, OP_Fcnt, 0, 0, 0, 0);
        break;
      }
      op = id==FN_Min ? OP_Min : OP_Max;
      for(i=0; i<pList->nExpr; i++){
        sqliteExprCode(pParse, pList->a[i].pExpr);
        if( i>0 ){
          sqliteVdbeAddOp(v, op, 0, 0, 0, 0);
        }
      }
      break;
    }
    case TK_SELECT: {
      sqliteVdbeAddOp(v, OP_MemLoad, pExpr->iColumn, 0, 0, 0);
      break;
    }
    case TK_IN: {
      int addr;
      sqliteVdbeAddOp(v, OP_Integer, 1, 0, 0, 0);
      sqliteExprCode(pParse, pExpr->pLeft);
      addr = sqliteVdbeCurrentAddr(v);
      if( pExpr->pSelect ){
        sqliteVdbeAddOp(v, OP_Found, pExpr->iTable, addr+2, 0, 0);
      }else{
        sqliteVdbeAddOp(v, OP_SetFound, pExpr->iTable, addr+2, 0, 0);
      }
      sqliteVdbeAddOp(v, OP_AddImm, -1, 0, 0, 0);
      break;
    }
    case TK_BETWEEN: {
      int lbl = sqliteVdbeMakeLabel(v);
      sqliteVdbeAddOp(v, OP_Integer, 0, 0, 0, 0);
      sqliteExprIfFalse(pParse, pExpr, lbl);
      sqliteVdbeAddOp(v, OP_AddImm, 1, 0, 0, 0);
      sqliteVdbeResolveLabel(v, lbl);
      break;
    }
  }
  return;
}

/*
** Generate code for a boolean expression such that a jump is made
** to the label "dest" if the expression is true but execution
** continues straight thru if the expression is false.
*/
void sqliteExprIfTrue(Parse *pParse, Expr *pExpr, int dest){
  Vdbe *v = pParse->pVdbe;
  int op = 0;
  switch( pExpr->op ){
    case TK_LT:       op = OP_Lt;       break;
    case TK_LE:       op = OP_Le;       break;
    case TK_GT:       op = OP_Gt;       break;
    case TK_GE:       op = OP_Ge;       break;
    case TK_NE:       op = OP_Ne;       break;
    case TK_EQ:       op = OP_Eq;       break;
    case TK_LIKE:     op = OP_Like;     break;
    case TK_GLOB:     op = OP_Glob;     break;
    case TK_ISNULL:   op = OP_IsNull;   break;
    case TK_NOTNULL:  op = OP_NotNull;  break;
    default:  break;
  }
  switch( pExpr->op ){
    case TK_AND: {
      int d2 = sqliteVdbeMakeLabel(v);
      sqliteExprIfFalse(pParse, pExpr->pLeft, d2);
      sqliteExprIfTrue(pParse, pExpr->pRight, dest);
      sqliteVdbeResolveLabel(v, d2);
      break;
    }
    case TK_OR: {
      sqliteExprIfTrue(pParse, pExpr->pLeft, dest);
      sqliteExprIfTrue(pParse, pExpr->pRight, dest);
      break;
    }
    case TK_NOT: {
      sqliteExprIfFalse(pParse, pExpr->pLeft, dest);
      break;
    }
    case TK_LT:
    case TK_LE:
    case TK_GT:
    case TK_GE:
    case TK_NE:
    case TK_EQ:
    case TK_LIKE:
    case TK_GLOB: {
      sqliteExprCode(pParse, pExpr->pLeft);
      sqliteExprCode(pParse, pExpr->pRight);
      sqliteVdbeAddOp(v, op, 0, dest, 0, 0);
      break;
    }
    case TK_ISNULL:
    case TK_NOTNULL: {
      sqliteExprCode(pParse, pExpr->pLeft);
      sqliteVdbeAddOp(v, op, 0, dest, 0, 0);
      break;
    }
    case TK_IN: {
      sqliteExprCode(pParse, pExpr->pLeft);
      if( pExpr->pSelect ){
        sqliteVdbeAddOp(v, OP_Found, pExpr->iTable, dest, 0, 0);
      }else{
        sqliteVdbeAddOp(v, OP_SetFound, pExpr->iTable, dest, 0, 0);
      }
      break;
    }
    case TK_BETWEEN: {
      int lbl = sqliteVdbeMakeLabel(v);
      sqliteExprCode(pParse, pExpr->pLeft);
      sqliteVdbeAddOp(v, OP_Dup, 0, 0, 0, 0);
      sqliteExprCode(pParse, pExpr->pList->a[0].pExpr);
      sqliteVdbeAddOp(v, OP_Lt, 0, lbl, 0, 0);
      sqliteExprCode(pParse, pExpr->pList->a[1].pExpr);
      sqliteVdbeAddOp(v, OP_Le, 0, dest, 0, 0);
      sqliteVdbeAddOp(v, OP_Integer, 0, 0, 0, 0);
      sqliteVdbeAddOp(v, OP_Pop, 1, 0, 0, lbl);
      break;
    }
    default: {
      sqliteExprCode(pParse, pExpr);
      sqliteVdbeAddOp(v, OP_If, 0, dest, 0, 0);
      break;
    }
  }
}

/*
** Generate code for boolean expression such that a jump is made
** to the label "dest" if the expression is false but execution
** continues straight thru if the expression is true.
*/
void sqliteExprIfFalse(Parse *pParse, Expr *pExpr, int dest){
  Vdbe *v = pParse->pVdbe;
  int op = 0;
  switch( pExpr->op ){
    case TK_LT:       op = OP_Ge;       break;
    case TK_LE:       op = OP_Gt;       break;
    case TK_GT:       op = OP_Le;       break;
    case TK_GE:       op = OP_Lt;       break;
    case TK_NE:       op = OP_Eq;       break;
    case TK_EQ:       op = OP_Ne;       break;
    case TK_LIKE:     op = OP_Like;     break;
    case TK_GLOB:     op = OP_Glob;     break;
    case TK_ISNULL:   op = OP_NotNull;  break;
    case TK_NOTNULL:  op = OP_IsNull;   break;
    default:  break;
  }
  switch( pExpr->op ){
    case TK_AND: {
      sqliteExprIfFalse(pParse, pExpr->pLeft, dest);
      sqliteExprIfFalse(pParse, pExpr->pRight, dest);
      break;
    }
    case TK_OR: {
      int d2 = sqliteVdbeMakeLabel(v);
      sqliteExprIfTrue(pParse, pExpr->pLeft, d2);
      sqliteExprIfFalse(pParse, pExpr->pRight, dest);
      sqliteVdbeResolveLabel(v, d2);
      break;
    }
    case TK_NOT: {
      sqliteExprIfTrue(pParse, pExpr->pLeft, dest);
      break;
    }
    case TK_LT:
    case TK_LE:
    case TK_GT:
    case TK_GE:
    case TK_NE:
    case TK_EQ: {
      sqliteExprCode(pParse, pExpr->pLeft);
      sqliteExprCode(pParse, pExpr->pRight);
      sqliteVdbeAddOp(v, op, 0, dest, 0, 0);
      break;
    }
    case TK_LIKE:
    case TK_GLOB: {
      sqliteExprCode(pParse, pExpr->pLeft);
      sqliteExprCode(pParse, pExpr->pRight);
      sqliteVdbeAddOp(v, op, 1, dest, 0, 0);
      break;
    }
    case TK_ISNULL:
    case TK_NOTNULL: {
      sqliteExprCode(pParse, pExpr->pLeft);
      sqliteVdbeAddOp(v, op, 0, dest, 0, 0);
      break;
    }
    case TK_IN: {
      sqliteExprCode(pParse, pExpr->pLeft);
      if( pExpr->pSelect ){
        sqliteVdbeAddOp(v, OP_NotFound, pExpr->iTable, dest, 0, 0);
      }else{
        sqliteVdbeAddOp(v, OP_SetNotFound, pExpr->iTable, dest, 0, 0);
      }
      break;
    }
    case TK_BETWEEN: {
      int addr;
      sqliteExprCode(pParse, pExpr->pLeft);
      sqliteVdbeAddOp(v, OP_Dup, 0, 0, 0, 0);
      sqliteExprCode(pParse, pExpr->pList->a[0].pExpr);
      addr = sqliteVdbeCurrentAddr(v);
      sqliteVdbeAddOp(v, OP_Ge, 0, addr+3, 0, 0);
      sqliteVdbeAddOp(v, OP_Pop, 1, 0, 0, 0);
      sqliteVdbeAddOp(v, OP_Goto, 0, dest, 0, 0);
      sqliteExprCode(pParse, pExpr->pList->a[1].pExpr);
      sqliteVdbeAddOp(v, OP_Gt, 0, dest, 0, 0);
      break;
    }
    default: {
      sqliteExprCode(pParse, pExpr);
      sqliteVdbeAddOp(v, OP_Not, 0, 0, 0, 0);
      sqliteVdbeAddOp(v, OP_If, 0, dest, 0, 0);
      break;
    }
  }
}

/*
** Do a deep comparison of two expression trees.  Return TRUE (non-zero)
** if they are identical and return FALSE if they differ in any way.
*/
int sqliteExprCompare(Expr *pA, Expr *pB){
  int i;
  if( pA==0 ){
    return pB==0;
  }else if( pB==0 ){
    return 0;
  }
  if( pA->op!=pB->op ) return 0;
  if( !sqliteExprCompare(pA->pLeft, pB->pLeft) ) return 0;
  if( !sqliteExprCompare(pA->pRight, pB->pRight) ) return 0;
  if( pA->pList ){
    if( pB->pList==0 ) return 0;
    if( pA->pList->nExpr!=pB->pList->nExpr ) return 0;
    for(i=0; i<pA->pList->nExpr; i++){
      if( !sqliteExprCompare(pA->pList->a[i].pExpr, pB->pList->a[i].pExpr) ){
        return 0;
      }
    }
  }else if( pB->pList ){
    return 0;
  }
  if( pA->pSelect || pB->pSelect ) return 0;
  if( pA->token.z ){
    if( pB->token.z==0 ) return 0;
    if( pB->token.n!=pA->token.n ) return 0;
    if( sqliteStrNICmp(pA->token.z, pB->token.z, pA->token.n)!=0 ) return 0;
  }
  return 1;
}

/*
** Add a new element to the pParse->aAgg[] array and return its index.
*/
static int appendAggInfo(Parse *pParse){
  if( (pParse->nAgg & 0x7)==0 ){
    int amt = pParse->nAgg + 8;
    pParse->aAgg = sqliteRealloc(pParse->aAgg, amt*sizeof(pParse->aAgg[0]));
    if( pParse->aAgg==0 ){
      sqliteSetString(&pParse->zErrMsg, "out of memory", 0);
      pParse->nErr++;
      return -1;
    }
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
** analyzed by sqliteExprResolveIds() and sqliteExprCheck().
**
** If errors are seen, leave an error message in zErrMsg and return
** the number of errors.
*/
int sqliteExprAnalyzeAggregates(Parse *pParse, Expr *pExpr){
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
      if( pExpr->iColumn==FN_Count || pExpr->iColumn==FN_Avg ){
        if( pParse->iAggCount>=0 ){
          i = pParse->iAggCount;
        }else{
          i = appendAggInfo(pParse);
          if( i<0 ) return 1;
          pParse->aAgg[i].isAgg = 1;
          pParse->aAgg[i].pExpr = 0;
          pParse->iAggCount = i;
        }
        if( pExpr->iColumn==FN_Count ){
          pExpr->iAgg = i;
          break;
        }
      }
      aAgg = pParse->aAgg;
      for(i=0; i<pParse->nAgg; i++){
        if( !aAgg[i].isAgg ) continue;
        if( sqliteExprCompare(aAgg[i].pExpr, pExpr) ){
          break;
        }
      }
      if( i>=pParse->nAgg ){
        i = appendAggInfo(pParse);
        if( i<0 ) return 1;
        pParse->aAgg[i].isAgg = 1;
        pParse->aAgg[i].pExpr = pExpr;
      }
      pExpr->iAgg = i;
      break;
    }
    default: {
      if( pExpr->pLeft ){
        nErr = sqliteExprAnalyzeAggregates(pParse, pExpr->pLeft);
      }
      if( nErr==0 && pExpr->pRight ){
        nErr = sqliteExprAnalyzeAggregates(pParse, pExpr->pRight);
      }
      if( nErr==0 && pExpr->pList ){
        int n = pExpr->pList->nExpr;
        int i;
        for(i=0; nErr==0 && i<n; i++){
          nErr = sqliteExprAnalyzeAggregates(pParse, pExpr->pList->a[i].pExpr);
        }
      }
      break;
    }
  }
  return nErr;
}
