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
** This file contains C code routines that are called by the parser
** to handle INSERT statements.
**
** $Id: insert.c,v 1.11 2000/06/21 13:59:12 drh Exp $
*/
#include "sqliteInt.h"

/*
** This routine is call to handle SQL of the following forms:
**
**    insert into TABLE (IDLIST) values(EXPRLIST)
**    insert into TABLE (IDLIST) select
**
** The IDLIST following the table name is always optional.  If omitted,
** then a list of all columns for the table is substituted.  The IDLIST
** appears in the pColumn parameter.  pColumn is NULL if IDLIST is omitted.
**
** The pList parameter holds EXPRLIST in the first form of the INSERT
** statement above, and pSelect is NULL.  For the second form, pList is
** NULL and pSelect is a pointer to the select statement used to generate
** data for the insert.
*/
void sqliteInsert(
  Parse *pParse,        /* Parser context */
  Token *pTableName,    /* Name of table into which we are inserting */
  ExprList *pList,      /* List of values to be inserted */
  Select *pSelect,      /* A SELECT statement to use as the data source */
  IdList *pColumn       /* Column names corresponding to IDLIST. */
){
  Table *pTab;          /* The table to insert into */
  char *zTab;           /* Name of the table into which we are inserting */
  int i, j, idx;        /* Loop counters */
  Vdbe *v;              /* Generate code into this virtual machine */
  Index *pIdx;          /* For looping over indices of the table */
  int srcTab;           /* Date comes from this temporary cursor if >=0 */
  int nColumn;          /* Number of columns in the data */
  int base;             /* First available cursor */
  int iCont, iBreak;    /* Beginning and end of the loop over srcTab */

  /* Locate the table into which we will be inserting new information.
  */
  zTab = sqliteTableNameFromToken(pTableName);
  pTab = sqliteFindTable(pParse->db, zTab);
  sqliteFree(zTab);
  if( pTab==0 ){
    sqliteSetNString(&pParse->zErrMsg, "no such table: ", 0, 
        pTableName->z, pTableName->n, 0);
    pParse->nErr++;
    goto insert_cleanup;
  }
  if( pTab->readOnly ){
    sqliteSetString(&pParse->zErrMsg, "table ", pTab->zName,
        " may not be modified", 0);
    pParse->nErr++;
    goto insert_cleanup;
  }

  /* Allocate a VDBE
  */
  v = sqliteGetVdbe(pParse);
  if( v==0 ) goto insert_cleanup;

  /* Figure out how many columns of data are supplied.  If the data
  ** is comming from a SELECT statement, then this step has to generate
  ** all the code to implement the SELECT statement and leave the data
  ** in a temporary table.  If data is coming from an expression list,
  ** then we just have to count the number of expressions.
  */
  if( pSelect ){
    int rc;
    srcTab = pParse->nTab++;
    sqliteVdbeAddOp(v, OP_Open, srcTab, 1, 0, 0);
    rc = sqliteSelect(pParse, pSelect, SRT_Table, srcTab);
    if( rc ) goto insert_cleanup;
    assert( pSelect->pEList );
    nColumn = pSelect->pEList->nExpr;
  }else{
    srcTab = -1;
    assert( pList );
    nColumn = pList->nExpr;
  }

  /* Make sure the number of columns in the source data matches the number
  ** of columns to be inserted into the table.
  */
  if( pColumn==0 && nColumn!=pTab->nCol ){
    char zNum1[30];
    char zNum2[30];
    sprintf(zNum1,"%d", nColumn);
    sprintf(zNum2,"%d", pTab->nCol);
    sqliteSetString(&pParse->zErrMsg, "table ", pTab->zName,
       " has ", zNum2, " columns but ",
       zNum1, " values were supplied", 0);
    pParse->nErr++;
    goto insert_cleanup;
  }
  if( pColumn!=0 && nColumn!=pColumn->nId ){
    char zNum1[30];
    char zNum2[30];
    sprintf(zNum1,"%d", nColumn);
    sprintf(zNum2,"%d", pColumn->nId);
    sqliteSetString(&pParse->zErrMsg, zNum1, " values for ",
       zNum2, " columns", 0);
    pParse->nErr++;
    goto insert_cleanup;
  }

  /* If the INSERT statement included an IDLIST term, then make sure
  ** all elements of the IDLIST really are columns of the table and 
  ** remember the column indices.
  */
  if( pColumn ){
    for(i=0; i<pColumn->nId; i++){
      pColumn->a[i].idx = -1;
    }
    for(i=0; i<pColumn->nId; i++){
      for(j=0; j<pTab->nCol; j++){
        if( sqliteStrICmp(pColumn->a[i].zName, pTab->aCol[j].zName)==0 ){
          pColumn->a[i].idx = j;
          break;
        }
      }
      if( j>=pTab->nCol ){
        sqliteSetString(&pParse->zErrMsg, "table ", pTab->zName,
           " has no column named ", pColumn->a[i].zName, 0);
        pParse->nErr++;
        goto insert_cleanup;
      }
    }
  }

  /* Open cursors into the table that is received the new data and
  ** all indices of that table.
  */
  base = pParse->nTab;
  sqliteVdbeAddOp(v, OP_Open, base, 1, pTab->zName, 0);
  for(idx=1, pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext, idx++){
    sqliteVdbeAddOp(v, OP_Open, idx+base, 1, pIdx->zName, 0);
  }

  /* If the data source is a SELECT statement, then we have to create
  ** a loop because there might be multiple rows of data.  If the data
  ** source is an expression list, then exactly one row will be inserted
  ** and the loop is not used.
  */
  if( srcTab>=0 ){
    sqliteVdbeAddOp(v, OP_Rewind, srcTab, 0, 0, 0);
    iBreak = sqliteVdbeMakeLabel(v);
    iCont = sqliteVdbeAddOp(v, OP_Next, srcTab, iBreak, 0, 0);
  }

  /* Create a new entry in the table and fill it with data.
  */
  sqliteVdbeAddOp(v, OP_New, 0, 0, 0, 0);
  if( pTab->pIndex ){
    sqliteVdbeAddOp(v, OP_Dup, 0, 0, 0, 0);
  }
  for(i=0; i<pTab->nCol; i++){
    if( pColumn==0 ){
      j = i;
    }else{
      for(j=0; j<pColumn->nId; j++){
        if( pColumn->a[j].idx==i ) break;
      }
    }
    if( pColumn && j>=pColumn->nId ){
      char *zDflt = pTab->aCol[i].zDflt;
      if( zDflt==0 ){
        sqliteVdbeAddOp(v, OP_Null, 0, 0, 0, 0);
      }else{
        sqliteVdbeAddOp(v, OP_String, 0, 0, zDflt, 0);
      }
    }else if( srcTab>=0 ){
      sqliteVdbeAddOp(v, OP_Field, srcTab, i, 0, 0); 
    }else{
      sqliteExprCode(pParse, pList->a[j].pExpr);
    }
  }
  sqliteVdbeAddOp(v, OP_MakeRecord, pTab->nCol, 0, 0, 0);
  sqliteVdbeAddOp(v, OP_Put, base, 0, 0, 0);

  /* Create appropriate entries for the new data row in all indices
  ** of the table.
  */
  for(idx=1, pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext, idx++){
    if( pIdx->pNext ){
      sqliteVdbeAddOp(v, OP_Dup, 0, 0, 0, 0);
    }
    for(i=0; i<pIdx->nColumn; i++){
      int idx = pIdx->aiColumn[i];
      if( pColumn==0 ){
        j = idx;
      }else{
        for(j=0; j<pColumn->nId; j++){
          if( pColumn->a[j].idx==idx ) break;
        }
      }
      if( pColumn && j>=pColumn->nId ){
        char *zDflt = pTab->aCol[idx].zDflt;
        if( zDflt==0 ){
          sqliteVdbeAddOp(v, OP_Null, 0, 0, 0, 0);
        }else{
          sqliteVdbeAddOp(v, OP_String, 0, 0, zDflt, 0);
        }
      }else if( srcTab>=0 ){
        sqliteVdbeAddOp(v, OP_Field, srcTab, idx, 0, 0); 
      }else{
        sqliteExprCode(pParse, pList->a[j].pExpr);
      }
    }
    sqliteVdbeAddOp(v, OP_MakeKey, pIdx->nColumn, 0, 0, 0);
    sqliteVdbeAddOp(v, OP_PutIdx, idx+base, 0, 0, 0);
  }

  /* The bottom of the loop, if the data source is a SELECT statement
  */
  if( srcTab>=0 ){
    sqliteVdbeAddOp(v, OP_Goto, 0, iCont, 0, 0);
    sqliteVdbeAddOp(v, OP_Noop, 0, 0, 0, iBreak);
  }

insert_cleanup:
  if( pList ) sqliteExprListDelete(pList);
  if( pSelect ) sqliteSelectDelete(pSelect);
  sqliteIdListDelete(pColumn);
}
