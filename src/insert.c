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
** $Id: insert.c,v 1.8 2000/06/07 15:11:27 drh Exp $
*/
#include "sqliteInt.h"

/*
** This routine is call to handle SQL of the following form:
**
**    insert into TABLE (IDLIST) values(EXPRLIST)
**
** The parameters are the table name and the expression list.
*/
void sqliteInsert(
  Parse *pParse,        /* Parser context */
  Token *pTableName,    /* Name of table into which we are inserting */
  ExprList *pList,      /* List of values to be inserted */
  Select *pSelect,      /* A SELECT statement to use as the data source */
  IdList *pField        /* Field name corresponding to pList.  Might be NULL */
){
  Table *pTab;          /* The table to insert into */
  char *zTab;           /* Name of the table into which we are inserting */
  int i, j, idx;        /* Loop counters */
  Vdbe *v;              /* Generate code into this virtual machine */
  Index *pIdx;          /* For looping over indices of the table */
  int srcTab;           /* Date comes from this temporary cursor if >=0 */
  int nField;           /* Number of columns in the data */
  int base;             /* First available cursor */
  int iCont, iBreak;    /* Beginning and end of the loop over srcTab */

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
  v = pParse->pVdbe;
  if( v==0 ){
    v = pParse->pVdbe = sqliteVdbeCreate(pParse->db->pBe);
  }
  if( v==0 ) goto insert_cleanup;
  if( pSelect ){
    int rc;
    srcTab = pParse->nTab++;
    sqliteVdbeAddOp(v, OP_Open, srcTab, 1, 0, 0);
    rc = sqliteSelect(pParse, pSelect, SRT_Table, srcTab);
    if( rc ) goto insert_cleanup;
    assert( pSelect->pEList );
    nField = pSelect->pEList->nExpr;
  }else{
    srcTab = -1;
    assert( pList );
    nField = pList->nExpr;
  }
  if( pField==0 && nField!=pTab->nCol ){
    char zNum1[30];
    char zNum2[30];
    sprintf(zNum1,"%d", nField);
    sprintf(zNum2,"%d", pTab->nCol);
    sqliteSetString(&pParse->zErrMsg, "table ", pTab->zName,
       " has ", zNum2, " columns but ",
       zNum1, " values were supplied", 0);
    pParse->nErr++;
    goto insert_cleanup;
  }
  if( pField!=0 && nField!=pField->nId ){
    char zNum1[30];
    char zNum2[30];
    sprintf(zNum1,"%d", nField);
    sprintf(zNum2,"%d", pField->nId);
    sqliteSetString(&pParse->zErrMsg, zNum1, " values for ",
       zNum2, " columns", 0);
    pParse->nErr++;
    goto insert_cleanup;
  }
  if( pField ){
    for(i=0; i<pField->nId; i++){
      pField->a[i].idx = -1;
    }
    for(i=0; i<pField->nId; i++){
      for(j=0; j<pTab->nCol; j++){
        if( sqliteStrICmp(pField->a[i].zName, pTab->aCol[j].zName)==0 ){
          pField->a[i].idx = j;
          break;
        }
      }
      if( j>=pTab->nCol ){
        sqliteSetString(&pParse->zErrMsg, "table ", pTab->zName,
           " has no column named ", pField->a[i].zName, 0);
        pParse->nErr++;
        goto insert_cleanup;
      }
    }
  }
  base = pParse->nTab;
  sqliteVdbeAddOp(v, OP_Open, base, 1, pTab->zName, 0);
  for(idx=1, pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext, idx++){
    sqliteVdbeAddOp(v, OP_Open, idx+base, 1, pIdx->zName, 0);
  }
  if( srcTab>=0 ){
    sqliteVdbeAddOp(v, OP_Rewind, srcTab, 0, 0, 0);
    iBreak = sqliteVdbeMakeLabel(v);
    iCont = sqliteVdbeAddOp(v, OP_Next, srcTab, iBreak, 0, 0);
  }
  sqliteVdbeAddOp(v, OP_New, 0, 0, 0, 0);
  if( pTab->pIndex ){
    sqliteVdbeAddOp(v, OP_Dup, 0, 0, 0, 0);
  }
  for(i=0; i<pTab->nCol; i++){
    if( pField==0 ){
      j = i;
    }else{
      for(j=0; j<pField->nId; j++){
        if( pField->a[j].idx==i ) break;
      }
    }
    if( pField && j>=pField->nId ){
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
  /* sqliteVdbeAddOp(v, OP_Close, 0, 0, 0, 0); */
  for(idx=1, pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext, idx++){
    if( pIdx->pNext ){
      sqliteVdbeAddOp(v, OP_Dup, 0, 0, 0, 0);
    }
    for(i=0; i<pIdx->nField; i++){
      int idx = pIdx->aiField[i];
      if( pField==0 ){
        j = idx;
      }else{
        for(j=0; j<pField->nId; j++){
          if( pField->a[j].idx==idx ) break;
        }
      }
      if( pField && j>=pField->nId ){
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
    sqliteVdbeAddOp(v, OP_MakeKey, pIdx->nField, 0, 0, 0);
    sqliteVdbeAddOp(v, OP_PutIdx, idx+base, 0, 0, 0);
    /* sqliteVdbeAddOp(v, OP_Close, idx, 0, 0, 0); */
  }
  if( srcTab>=0 ){
    sqliteVdbeAddOp(v, OP_Goto, 0, iCont, 0, 0);
    sqliteVdbeAddOp(v, OP_Noop, 0, 0, 0, iBreak);
  }

insert_cleanup:
  if( pList ) sqliteExprListDelete(pList);
  if( pSelect ) sqliteSelectDelete(pSelect);
  sqliteIdListDelete(pField);
}
