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
** This file contains C code routines that are called by the parser
** to handle INSERT statements in SQLite.
**
** $Id: insert.c,v 1.35 2002/01/29 23:07:02 drh Exp $
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
  IdList *pColumn,      /* Column names corresponding to IDLIST. */
  int onError           /* How to handle constraint errors */
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
  sqlite *db;           /* The main database structure */
  int openOp;           /* Opcode used to open cursors */
  int keyColumn = -1;   /* Column that is the INTEGER PRIMARY KEY */
  int endOfLoop;        /* Label for the end of the insertion loop */

  if( pParse->nErr || sqlite_malloc_failed ) goto insert_cleanup;
  db = pParse->db;

  /* Locate the table into which we will be inserting new information.
  */
  zTab = sqliteTableNameFromToken(pTableName);
  if( zTab==0 ) goto insert_cleanup;
  pTab = sqliteFindTable(db, zTab);
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
  if( (db->flags & SQLITE_InTrans)==0 ){
    sqliteVdbeAddOp(v, OP_Transaction, 0, 0);
    sqliteVdbeAddOp(v, OP_VerifyCookie, db->schema_cookie, 0);
    pParse->schemaVerified = 1;
  }

  /* Figure out how many columns of data are supplied.  If the data
  ** is coming from a SELECT statement, then this step has to generate
  ** all the code to implement the SELECT statement and leave the data
  ** in a temporary table.  If data is coming from an expression list,
  ** then we just have to count the number of expressions.
  */
  if( pSelect ){
    int rc;
    srcTab = pParse->nTab++;
    sqliteVdbeAddOp(v, OP_OpenTemp, srcTab, 0);
    rc = sqliteSelect(pParse, pSelect, SRT_Table, srcTab);
    if( rc || pParse->nErr || sqlite_malloc_failed ) goto insert_cleanup;
    assert( pSelect->pEList );
    nColumn = pSelect->pEList->nExpr;
  }else{
    assert( pList!=0 );
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
  **
  ** If the table has an INTEGER PRIMARY KEY column and that column
  ** is named in the IDLIST, then record in the keyColumn variable
  ** the index into IDLIST of the primary key column.  keyColumn is
  ** the index of the primary key as it appears in IDLIST, not as
  ** is appears in the original table.  (The index of the primary
  ** key in the original table is pTab->iPKey.)
  */
  if( pColumn ){
    for(i=0; i<pColumn->nId; i++){
      pColumn->a[i].idx = -1;
    }
    for(i=0; i<pColumn->nId; i++){
      for(j=0; j<pTab->nCol; j++){
        if( sqliteStrICmp(pColumn->a[i].zName, pTab->aCol[j].zName)==0 ){
          pColumn->a[i].idx = j;
          if( j==pTab->iPKey ){
            keyColumn = i;
          }
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

  /* If there is no IDLIST term but the table has an integer primary
  ** key, the set the keyColumn variable to the primary key column index
  ** in the original table definition.
  */
  if( pColumn==0 ){
    keyColumn = pTab->iPKey;
  }

  /* Open cursors into the table that is received the new data and
  ** all indices of that table.
  */
  base = pParse->nTab;
  openOp = pTab->isTemp ? OP_OpenWrAux : OP_OpenWrite;
  sqliteVdbeAddOp(v, openOp, base, pTab->tnum);
  sqliteVdbeChangeP3(v, -1, pTab->zName, P3_STATIC);
  for(idx=1, pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext, idx++){
    sqliteVdbeAddOp(v, openOp, idx+base, pIdx->tnum);
    sqliteVdbeChangeP3(v, -1, pIdx->zName, P3_STATIC);
  }

  /* If the data source is a SELECT statement, then we have to create
  ** a loop because there might be multiple rows of data.  If the data
  ** source is an expression list, then exactly one row will be inserted
  ** and the loop is not used.
  */
  if( srcTab>=0 ){
    if( db->flags & SQLITE_CountRows ){
      sqliteVdbeAddOp(v, OP_Integer, 0, 0);  /* Initialize the row count */
    }
    iBreak = sqliteVdbeMakeLabel(v);
    sqliteVdbeAddOp(v, OP_Rewind, srcTab, iBreak);
    iCont = sqliteVdbeCurrentAddr(v);
  }

  /* Push the record number for the new entry onto the stack.  The
  ** record number is a randomly generate integer created by NewRecno
  ** except when the table has an INTEGER PRIMARY KEY column, in which
  ** case the record number is the same as that column.  May a copy
  ** because sqliteGenerateConstraintChecks() requires two copies of
  ** the record number.
  */
  if( keyColumn>=0 ){
    if( srcTab>=0 ){
      sqliteVdbeAddOp(v, OP_Column, srcTab, keyColumn);
    }else{
      sqliteExprCode(pParse, pList->a[keyColumn].pExpr);
    }
    sqliteVdbeAddOp(v, OP_MustBeInt, 0, 0);
  }else{
    sqliteVdbeAddOp(v, OP_NewRecno, base, 0);
  }
  sqliteVdbeAddOp(v, OP_Dup, 0, 0);

  /* Push onto the stack, data for all columns of the new entry, beginning
  ** with the first column.
  */
  for(i=0; i<pTab->nCol; i++){
    if( i==pTab->iPKey ){
      /* The value of the INTEGER PRIMARY KEY column is always a NULL.
      ** Whenever this column is read, the record number will be substituted
      ** in its place.  So will fill this column with a NULL to avoid
      ** taking up data space with information that will never be used. */
      sqliteVdbeAddOp(v, OP_String, 0, 0);
      continue;
    }
    if( pColumn==0 ){
      j = i;
    }else{
      for(j=0; j<pColumn->nId; j++){
        if( pColumn->a[j].idx==i ) break;
      }
    }
    if( pColumn && j>=pColumn->nId ){
      sqliteVdbeAddOp(v, OP_String, 0, 0);
      sqliteVdbeChangeP3(v, -1, pTab->aCol[i].zDflt, P3_STATIC);
    }else if( srcTab>=0 ){
      sqliteVdbeAddOp(v, OP_Column, srcTab, i); 
    }else{
      sqliteExprCode(pParse, pList->a[j].pExpr);
    }
  }

  /* Generate code to check constraints and generate index keys and
  ** do the insertion.
  */
  endOfLoop = sqliteVdbeMakeLabel(v);
  sqliteGenerateConstraintChecks(pParse, pTab, base, 0,1,onError,endOfLoop,0);
  sqliteCompleteInsertion(pParse, pTab, base, 0, 1);

  /* If inserting from a SELECT, keep a count of the number of
  ** rows inserted.
  */
  if( srcTab>=0 && (db->flags & SQLITE_CountRows)!=0 ){
    sqliteVdbeAddOp(v, OP_AddImm, 1, 0);
  }

  /* The bottom of the loop, if the data source is a SELECT statement
  */
  sqliteVdbeResolveLabel(v, endOfLoop);
  if( srcTab>=0 ){
    sqliteVdbeAddOp(v, OP_Next, srcTab, iCont);
    sqliteVdbeResolveLabel(v, iBreak);
    sqliteVdbeAddOp(v, OP_Close, srcTab, 0);
  }
  sqliteVdbeAddOp(v, OP_Close, base, 0);
  for(idx=1, pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext, idx++){
    sqliteVdbeAddOp(v, OP_Close, idx+base, 0);
  }
  if( (db->flags & SQLITE_InTrans)==0 ){
    sqliteVdbeAddOp(v, OP_Commit, 0, 0);
  }

  /*
  ** Return the number of rows inserted.
  */
  if( db->flags & SQLITE_CountRows ){
    sqliteVdbeAddOp(v, OP_ColumnCount, 1, 0);
    sqliteVdbeAddOp(v, OP_ColumnName, 0, 0);
    sqliteVdbeChangeP3(v, -1, "rows inserted", P3_STATIC);
    if( srcTab<0 ){
      sqliteVdbeAddOp(v, OP_Integer, 1, 0);
    }
    sqliteVdbeAddOp(v, OP_Callback, 1, 0);
  }

insert_cleanup:
  if( pList ) sqliteExprListDelete(pList);
  if( pSelect ) sqliteSelectDelete(pSelect);
  sqliteIdListDelete(pColumn);
}

/*
** Generate code to do a constraint check prior to an INSERT or an UPDATE.
**
** When this routine is called, the stack contains (from bottom to top)
** the following values:
**
**    1.  The recno of the row to be updated before it is updated.
**
**    2.  The recno of the row after the update.  (This is usually the
**        same as (1) but can be different if an UPDATE changes an
**        INTEGER PRIMARY KEY column.)
**
**    3.  The data in the first column of the entry after the update.
**
**    i.  Data from middle columns...
**
**    N.  The data in the last column of the entry after the update.
**
** The old recno shown as entry (1) above is omitted if the recnoChng
** parameter is 0.  recnoChange is true if the record number is changing
** and false if not.
**
** The code generated by this routine pushes additional entries onto
** the stack which are the keys for new index entries for the new record.
** The order of index keys is the same as the order of the indices on
** the pTable->pIndex list.  A key is only created for index i if 
** aIdxUsed!=0 and aIdxUsed[i]!=0.
**
** This routine also generates code to check constraints.  NOT NULL,
** CHECK, and UNIQUE constraints are all checked.  If a constraint fails,
** then the appropriate action is performed.  The default action is to
** execute OP_Halt to abort the transaction and cause sqlite_exec() to
** return SQLITE_CONSTRAINT.  This is the so-called "ABORT" action.
** Other actions are REPLACE and IGNORE.  The following table summarizes
** what happens.
**
**  Constraint type  Action       What Happens
**  ---------------  ----------   ----------------------------------------
**  any              ABORT        The current transaction is rolled back and
**                                sqlite_exec() returns immediately with a
**                                return code of SQLITE_CONSTRAINT.
**
**  any              IGNORE       The record number and data is popped from
**                                the stack and there is an immediate jump
**                                to label ignoreDest.
**
**  NOT NULL         REPLACE      The NULL value is replace by the default
**                                value for that column.  If the default value
**                                is NULL, the action is the same as ABORT.
**
**  UNIQUE           REPLACE      The other row that conflicts with the row
**                                being inserted is removed.
**
**  CHECK            REPLACE      Illegal.  The results in an exception.
**
** The action to take is determined by the constraint itself if
** overrideError is OE_Default.  Otherwise, overrideError determines
** which action to use.
**
** The calling routine must an open read/write cursor for pTab with
** cursor number "base".  All indices of pTab must also have open
** read/write cursors with cursor number base+i for the i-th cursor.
** Except, if there is no possibility of a REPLACE action then
** cursors do not need to be open for indices where aIdxUsed[i]==0.
**
** If the isUpdate flag is true, it means that the "base" cursor is
** initially pointing to an entry that is being updated.  The isUpdate
** flag causes extra code to be generated so that the "base" cursor
** is still pointing at the same entry after the routine returns.
** Without the isUpdate flag, the "base" cursor might be moved.
*/
void sqliteGenerateConstraintChecks(
  Parse *pParse,      /* The parser context */
  Table *pTab,        /* the table into which we are inserting */
  int base,           /* Index of a read/write cursor pointing at pTab */
  char *aIdxUsed,     /* Which indices are used.  NULL means all are used */
  int recnoChng,      /* True if the record number will change */
  int overrideError,  /* Override onError to this if not OE_Default */
  int ignoreDest,     /* Jump to this label on an OE_Ignore resolution */
  int isUpdate        /* True for UPDATE, False for INSERT */
){
  int i;
  Vdbe *v;
  int nCol;
  int onError;
  int addr;
  int extra;
  int iCur;
  Index *pIdx;
  int seenReplace = 0;
  int jumpInst;
  int contAddr;

  v = sqliteGetVdbe(pParse);
  assert( v!=0 );
  nCol = pTab->nCol;
  recnoChng = (recnoChng!=0);  /* Must be either 1 or 0 */

  /* Test all NOT NULL constraints.
  */
  for(i=0; i<nCol; i++){
    if( i==pTab->iPKey ){
      /* Fix me: Make sure the INTEGER PRIMARY KEY is not NULL. */
      continue;
    }
    onError = pTab->aCol[i].notNull;
    if( onError==OE_None ) continue;
    if( overrideError!=OE_Default ){
      onError = overrideError;
    }
    if( onError==OE_Replace && pTab->aCol[i].zDflt==0 ){
      onError = OE_Abort;
    }
    addr = sqliteVdbeAddOp(v, OP_Dup, nCol-i, 1);
    sqliteVdbeAddOp(v, OP_NotNull, 0, addr+1+(onError!=OE_Abort));
    switch( onError ){
      case OE_Abort: {
        sqliteVdbeAddOp(v, OP_Halt, SQLITE_CONSTRAINT, 0);
        break;
      }
      case OE_Ignore: {
        sqliteVdbeAddOp(v, OP_Pop, nCol+1+recnoChng, 0);
        sqliteVdbeAddOp(v, OP_Goto, 0, ignoreDest);
        break;
      }
      case OE_Replace: {
        sqliteVdbeAddOp(v, OP_String, 0, 0);
        sqliteVdbeChangeP3(v, -1, pTab->aCol[i].zDflt, P3_STATIC);
        sqliteVdbeAddOp(v, OP_Push, nCol-i, 0);
        break;
      }
      default: assert(0);
    }
  }

  /* Test all CHECK constraints
  */

  /* Test all UNIQUE constraints.  Add index records as we go.
  */
  if( recnoChng && pTab->iPKey>=0 && pTab->keyConf!=OE_Replace 
      && overrideError!=OE_Replace ){
    sqliteVdbeAddOp(v, OP_Dup, nCol, 1);
    jumpInst = sqliteVdbeAddOp(v, OP_NotExists, base, 0);
    onError = pTab->keyConf;
    if( overrideError!=OE_Default ){
      onError = overrideError;
    }
    switch( onError ){
      case OE_Abort: {
        sqliteVdbeAddOp(v, OP_Halt, SQLITE_CONSTRAINT, 0);
        break;
      }
      case OE_Ignore: {
        sqliteVdbeAddOp(v, OP_Pop, nCol+2, 0);
        sqliteVdbeAddOp(v, OP_Goto, 0, ignoreDest);
        break;
      }
      default: assert(0);
    }
    contAddr = sqliteVdbeCurrentAddr(v);
    sqliteVdbeChangeP2(v, jumpInst, contAddr);
    if( isUpdate ){
      sqliteVdbeAddOp(v, OP_Dup, nCol+1, 1);
      sqliteVdbeAddOp(v, OP_MoveTo, base, 0);
    }
  }
  extra = 0;
  for(extra=(-1), iCur=0, pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext, iCur++){
    if( aIdxUsed && aIdxUsed[iCur]==0 ) continue;
    extra++;    
    sqliteVdbeAddOp(v, OP_Dup, nCol+extra, 1);
    for(i=0; i<pIdx->nColumn; i++){
      int idx = pIdx->aiColumn[i];
      if( idx==pTab->iPKey ){
        sqliteVdbeAddOp(v, OP_Dup, i+extra+nCol+1, 1);
      }else{
        sqliteVdbeAddOp(v, OP_Dup, i+extra+nCol-idx, 1);
      }
    }
    sqliteVdbeAddOp(v, OP_MakeIdxKey, pIdx->nColumn, 0);
    onError = pIdx->onError;
    if( onError==OE_None ) continue;
    if( overrideError!=OE_Default ){
      onError = overrideError;
    }
    sqliteVdbeAddOp(v, OP_Dup, extra+nCol+2, 1);
    jumpInst = sqliteVdbeAddOp(v, OP_IsUnique, base+iCur+1, 0);
    switch( onError ){
      case OE_Abort: {
        sqliteVdbeAddOp(v, OP_Halt, SQLITE_CONSTRAINT, 0);
        break;
      }
      case OE_Ignore: {
        assert( seenReplace==0 );
        sqliteVdbeAddOp(v, OP_Pop, nCol+extra+2+recnoChng, 0);
        sqliteVdbeAddOp(v, OP_Goto, 0, ignoreDest);
        break;
      }
      case OE_Replace: {
        sqliteVdbeAddOp(v, OP_MoveTo, base, 0);
        sqliteGenerateRowDelete(v, pTab, base);
        if( isUpdate ){
          sqliteVdbeAddOp(v, OP_Dup, nCol+extra+recnoChng, 1);
          sqliteVdbeAddOp(v, OP_MoveTo, base, 0);
        }
        seenReplace = 1;
        break;
      }
      default: assert(0);
    }
    contAddr = sqliteVdbeCurrentAddr(v);
    sqliteVdbeChangeP2(v, jumpInst, contAddr);
  }
}

/*
** This routine generates code to finish the INSERT or UPDATE operation
** that was started by a prior call to sqliteGenerateConstraintChecks.
** The stack must contain keys for all active indices followed by data
** and the recno for the new entry.  This routine creates the new
** entries in all indices and in the main table.
**
** The arguments to this routine should be the same as the first five
** arguments to sqliteGenerateConstraintChecks.
*/
void sqliteCompleteInsertion(
  Parse *pParse,      /* The parser context */
  Table *pTab,        /* the table into which we are inserting */
  int base,           /* Index of a read/write cursor pointing at pTab */
  char *aIdxUsed,     /* Which indices are used.  NULL means all are used */
  int recnoChng       /* True if the record number changed */
){
  int i;
  Vdbe *v;
  int nIdx;
  Index *pIdx;

  v = sqliteGetVdbe(pParse);
  assert( v!=0 );
  for(nIdx=0, pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext, nIdx++){}
  for(i=nIdx-1; i>=0; i--){
    if( aIdxUsed && aIdxUsed[i]==0 ) continue;
    sqliteVdbeAddOp(v, OP_IdxPut, base+i+1, 0);
  }
  sqliteVdbeAddOp(v, OP_MakeRecord, pTab->nCol, 0);
  sqliteVdbeAddOp(v, OP_PutIntKey, base, 0);
  if( recnoChng ){
    sqliteVdbeAddOp(v, OP_Pop, 1, 0);
  }
}
