/*
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains code used by the compiler to add foreign key
** support to compiled SQL statements.
*/
#include "sqliteInt.h"

#ifndef SQLITE_OMIT_FOREIGN_KEY
#ifndef SQLITE_OMIT_TRIGGER

/*
** Deferred and Immediate FKs
** --------------------------
**
** Foreign keys in SQLite come in two flavours: deferred and immediate.
** If an immediate foreign key constraint is violated, an OP_Halt is 
** executed and the current statement transaction rolled back. If a 
** deferred foreign key constraint is violated, no action is taken 
** immediately. However if the application attempts to commit the 
** transaction before fixing the constraint violation, the attempt fails.
**
** Deferred constraints are implemented using a simple counter associated
** with the database handle. The counter is set to zero each time a 
** database transaction is opened. Each time a statement is executed 
** that causes a foreign key violation, the counter is incremented. Each
** time a statement is executed that removes an existing violation from
** the database, the counter is decremented. When the transaction is
** committed, the commit fails if the current value of the counter is
** greater than zero. This scheme has two big drawbacks:
**
**   * When a commit fails due to a deferred foreign key constraint, 
**     there is no way to tell which foreign constraint is not satisfied,
**     or which row it is not satisfied for.
**
**   * If the database contains foreign key violations when the 
**     transaction is opened, this may cause the mechanism to malfunction.
**
** Despite these problems, this approach is adopted as it seems simpler
** than the alternatives.
**
** INSERT operations:
**
**   I.1) For each FK for which the table is the referencing table, search
**        the referenced table for a match. If none is found, throw an 
**        exception for an immediate FK, or increment the counter for a
**        deferred FK.
**
**   I.2) For each deferred FK for which the table is the referenced table, 
**        search the referencing table for rows that correspond to the new
**        row in the referenced table. Decrement the counter for each row
**        found (as the constraint is now satisfied).
**
** DELETE operations:
**
**   D.1) For each deferred FK for which the table is the referencing table, 
**        search the referenced table for a row that corresponds to the 
**        deleted row in the referencing table. If such a row is not found, 
**        decrement the counter.
**
**   D.2) For each FK for which the table is the referenced table, search 
**        the referencing table for rows that correspond to the deleted row 
**        in the referenced table. For each found, throw an exception for an
**        immediate FK, or increment the counter for a deferred FK.
**
** UPDATE operations:
**
**   An UPDATE command requires that all 4 steps above are taken, but only
**   for FK constraints for which the affected columns are actually 
**   modified (values must be compared at runtime).
**
** Note that I.1 and D.1 are very similar operations, as are I.2 and D.2.
** This simplifies the implementation a bit.
**
** For the purposes of immediate FK constraints, the OR REPLACE conflict
** resolution is considered to delete rows before the new row is inserted.
** If a delete caused by OR REPLACE violates an FK constraint, an exception
** is thrown, even if the FK constraint would be satisfied after the new 
** row is inserted.
**
** TODO: How should dropping a table be handled? How should renaming a 
** table be handled?
*/

/*
** Query API Notes
** ---------------
**
** Before coding an UPDATE or DELETE row operation, the code-generator
** for those two operations needs to know whether or not the operation
** requires any FK processing and, if so, which columns of the original
** row are required by the FK processing VDBE code (i.e. if FKs were
** implemented using triggers, which of the old.* columns would be 
** accessed). No information is required by the code-generator before
** coding an INSERT operation.
**
*/

/*
** VDBE Calling Convention
** -----------------------
**
** Example:
**
**   For the following INSERT statement:
**
**     CREATE TABLE t1(a, b INTEGER PRIMARY KEY, c);
**     INSERT INTO t1 VALUES(1, 2, 3.1);
**
**   Register (x):        2    (type integer)
**   Register (x+1):      1    (type integer)
**   Register (x+2):      NULL (type NULL)
**   Register (x+3):      3.1  (type real)
*/

/*
** ON UPDATE and ON DELETE clauses
** -------------------------------
*/

/*
** Externally accessible module functions
** --------------------------------------
**
**   sqlite3FkRequired()
**   sqlite3FkOldmask()
**
**   sqlite3FkCheck()
**   sqlite3FkActions()
**
**   sqlite3FkDelete()
** 
*/

/*
** A foreign key constraint requires that the key columns in the referenced
** table are collectively subject to a UNIQUE or PRIMARY KEY constraint.
** Given that pTo is the referenced table for foreign key constraint
** pFKey, check that the columns in pTo are indeed subject to a such a
** constraint. If they are not, return non-zero and leave an error in pParse.
**
** If an error does not occur, return zero.
*/
static int locateFkeyIndex(
  Parse *pParse,                  /* Parse context to store any error in */
  Table *pTo,                     /* Referenced table */
  FKey *pFKey,                    /* Foreign key to find index for */
  Index **ppIdx,                  /* OUT: Unique index on referenced table */
  int **paiCol                    /* OUT: Map of index columns in pFKey */
){
  Index *pIdx = 0;
  int *aiCol = 0;
  int nCol = pFKey->nCol;
  char *zFirst = pFKey->aCol[0].zCol;

  /* The caller is responsible for zeroing output parameters. */
  assert( ppIdx && *ppIdx==0 );
  assert( !paiCol || *paiCol==0 );

  /* If this is a non-composite (single column) foreign key, check if it 
  ** maps to the INTEGER PRIMARY KEY of table pTo. If so, leave *ppIdx 
  ** and *paiCol set to zero and return early. 
  **
  ** Otherwise, for a composite foreign key (more than one column), allocate
  ** space for the aiCol array (returned via output parameter *paiCol).
  ** Non-composite foreign keys do not require the aiCol array.
  */
  if( nCol==1 ){
    /* The FK maps to the IPK if any of the following are true:
    **
    **   1) The FK is explicitly mapped to "rowid", "oid" or "_rowid_", or
    **   2) There is an explicit INTEGER PRIMARY KEY column and the FK is
    **      implicitly mapped to the primary key of table pTo, or
    **   3) The FK is explicitly mapped to a column declared as INTEGER
    **      PRIMARY KEY.
    */
    if( zFirst && sqlite3IsRowid(zFirst) ) return 0;
    if( pTo->iPKey>=0 ){
      if( !zFirst ) return 0;
      if( !sqlite3StrICmp(pTo->aCol[pTo->iPKey].zName, zFirst) ) return 0;
    }
  }else if( paiCol ){
    assert( nCol>1 );
    aiCol = (int *)sqlite3DbMallocRaw(pParse->db, nCol*sizeof(int));
    if( !aiCol ) return 1;
    *paiCol = aiCol;
  }

  for(pIdx=pTo->pIndex; pIdx; pIdx=pIdx->pNext){
    if( pIdx->nColumn==nCol && pIdx->onError!=OE_None ){ 
      /* pIdx is a UNIQUE index (or a PRIMARY KEY) and has the right number
      ** of columns. If each indexed column corresponds to a foreign key
      ** column of pFKey, then this index is a winner.  */

      if( zFirst==0 ){
        /* If zFirst is NULL, then this foreign key is implicitly mapped to 
        ** the PRIMARY KEY of table pTo. The PRIMARY KEY index may be 
        ** identified by the test (Index.autoIndex==2).  */
        if( pIdx->autoIndex==2 ){
          if( aiCol ) memcpy(aiCol, pIdx->aiColumn, sizeof(int)*nCol);
          break;
        }
      }else{
        /* If zFirst is non-NULL, then this foreign key was declared to
        ** map to an explicit list of columns in table pTo. Check if this
        ** index matches those columns.  */
        int i, j;
        for(i=0; i<nCol; i++){
          char *zIdxCol = pTo->aCol[pIdx->aiColumn[i]].zName;
          for(j=0; j<nCol; j++){
            if( sqlite3StrICmp(pFKey->aCol[j].zCol, zIdxCol)==0 ){
              if( aiCol ) aiCol[i] = pFKey->aCol[j].iFrom;
              break;
            }
          }
          if( j==nCol ) break;
        }
        if( i==nCol ) break;      /* pIdx is usable */
      }
    }
  }

  if( pParse && !pIdx ){
    sqlite3ErrorMsg(pParse, "foreign key mismatch");
    sqlite3DbFree(pParse->db, aiCol);
    return 1;
  }

  *ppIdx = pIdx;
  return 0;
}

static void fkCheckReference(
  Parse *pParse,        /* Parse context */
  int iDb,              /* Index of database housing pTab */
  Table *pTab,          /* Table referenced by FK pFKey */
  Index *pIdx,          /* Index ensuring uniqueness of FK in pTab */
  FKey *pFKey,          /* Foreign key to check */
  int *aiCol,           /* Map from FK column to referencing table column */
  int regData,          /* Address of array containing referencing row */
  int nIncr             /* If deferred FK, increment counter by this */
){
  int i;
  Vdbe *v = sqlite3GetVdbe(pParse);
  int iCur = pParse->nTab - 1;
  int iOk = sqlite3VdbeMakeLabel(v);

  assert( pFKey->isDeferred || nIncr==1 );

  /* Check if any of the key columns in the referencing table are 
  ** NULL. If any are, then the constraint is satisfied. No need
  ** to search for a matching row in the referenced table.  */
  for(i=0; i<pFKey->nCol; i++){
    int iReg = aiCol[i] + regData + 1;
    sqlite3VdbeAddOp2(v, OP_IsNull, iReg, iOk);
  }

  if( pIdx==0 ){
    /* If pIdx is NULL, then the foreign key constraint references the
    ** INTEGER PRIMARY KEY column in the referenced table (table pTab).  */
    int iReg = pFKey->aCol[0].iFrom + regData + 1;
    sqlite3OpenTable(pParse, iCur, iDb, pTab, OP_OpenRead);
    sqlite3VdbeAddOp3(v, OP_NotExists, iCur, 0, iReg);
    sqlite3VdbeAddOp2(v, OP_Goto, 0, iOk);
    sqlite3VdbeJumpHere(v, sqlite3VdbeCurrentAddr(v)-2);
  }else{
    int regRec = sqlite3GetTempReg(pParse);
    KeyInfo *pKey = sqlite3IndexKeyinfo(pParse, pIdx);

    sqlite3VdbeAddOp3(v, OP_OpenRead, iCur, pIdx->tnum, iDb);
    sqlite3VdbeChangeP4(v, -1, (char*)pKey, P4_KEYINFO_HANDOFF);

    if( pFKey->nCol>1 ){
      int nCol = pFKey->nCol;
      int regTemp = sqlite3GetTempRange(pParse, nCol);
      for(i=0; i<nCol; i++){ 
        sqlite3VdbeAddOp2(v, OP_SCopy, aiCol[i]+1+regData, regTemp+i);
      }
      sqlite3VdbeAddOp3(v, OP_MakeRecord, regTemp, nCol, regRec);
      sqlite3ReleaseTempRange(pParse, regTemp, nCol);
    }else{
      int iReg = aiCol[0] + regData + 1;
      sqlite3VdbeAddOp3(v, OP_MakeRecord, iReg, 1, regRec);
      sqlite3IndexAffinityStr(v, pIdx);
    }

    sqlite3VdbeAddOp3(v, OP_Found, iCur, iOk, regRec);
    sqlite3ReleaseTempReg(pParse, regRec);
  }

  if( pFKey->isDeferred ){
    assert( nIncr==1 || nIncr==-1 );
    sqlite3VdbeAddOp1(v, OP_DeferredCons, nIncr);
  }else{
    sqlite3HaltConstraint(
        pParse, OE_Abort, "foreign key constraint failed", P4_STATIC
    );
  }

  sqlite3VdbeResolveLabel(v, iOk);
}

static void fkScanReferences(
  Parse *pParse,                  /* Parse context */
  SrcList *pSrc,                  /* SrcList containing the table to scan */
  Index *pIdx,                    /* Foreign key index */
  FKey *pFKey,                    /* Foreign key relationship */
  int *aiCol,                     /* Map from FK to referenced table columns */
  int regData,                    /* Referenced table data starts here */
  int nIncr                       /* Amount to increment deferred counter by */
){
  sqlite3 *db = pParse->db;       /* Database handle */
  int i;                          /* Iterator variable */
  Expr *pWhere = 0;               /* WHERE clause to scan with */
  NameContext sNameContext;       /* Context used to resolve WHERE clause */
  WhereInfo *pWInfo;              /* Context used by sqlite3WhereXXX() */

  for(i=0; i<pFKey->nCol; i++){
    Expr *pLeft;                  /* Value from deleted row */
    Expr *pRight;                 /* Column ref to referencing table */
    Expr *pEq;                    /* Expression (pLeft = pRight) */
    int iCol;                     /* Index of column in referencing table */ 
    const char *zCol;             /* Name of column in referencing table */

    pLeft = sqlite3Expr(db, TK_REGISTER, 0);
    if( pLeft ){
      pLeft->iTable = (pIdx ? (regData+pIdx->aiColumn[i]+1) : regData);
    }
    iCol = aiCol ? aiCol[i] : pFKey->aCol[0].iFrom;
    if( iCol<0 ){
      zCol = "rowid";
    }else{
      zCol = pFKey->pFrom->aCol[iCol].zName;
    }
    pRight = sqlite3Expr(db, TK_ID, zCol);
    pEq = sqlite3PExpr(pParse, TK_EQ, pLeft, pRight, 0);
    pWhere = sqlite3ExprAnd(db, pWhere, pEq);
  }

  /* Resolve the references in the WHERE clause. */
  memset(&sNameContext, 0, sizeof(NameContext));
  sNameContext.pSrcList = pSrc;
  sNameContext.pParse = pParse;
  sqlite3ResolveExprNames(&sNameContext, pWhere);

  /* Create VDBE to loop through the entries in pSrc that match the WHERE
  ** clause. If the constraint is not deferred, throw an exception for
  ** each row found. Otherwise, for deferred constraints, increment the
  ** deferred constraint counter by nIncr for each row selected.  */
  pWInfo = sqlite3WhereBegin(pParse, pSrc, pWhere, 0, 0);
  if( pFKey->isDeferred && nIncr ){
    assert( nIncr==1 || nIncr==-1 );
    sqlite3VdbeAddOp1(pParse->pVdbe, OP_DeferredCons, nIncr);
  }else{
    assert( nIncr==1 || nIncr==0 );
    sqlite3HaltConstraint(
      pParse, OE_Abort, "foreign key constraint failed", P4_STATIC
    );
  }
  if( pWInfo ){
    sqlite3WhereEnd(pWInfo);
  }

  /* Clean up the WHERE clause constructed above. */
  sqlite3ExprDelete(db, pWhere);
}

/*
** This function returns a pointer to the head of a linked list of FK
** constraints that refer to the table passed as an argument. For example,
** given the following schema:
**
**   CREATE TABLE t1(a PRIMARY KEY);
**   CREATE TABLE t2(b REFERENCES t1(a);
**
** Calling this function with table "t1" as an argument returns a pointer
** to the FKey structure representing the foreign key constraint on table
** "t2". Calling this function with "t2" as the argument would return a
** NULL pointer (as there are no FK constraints that refer to t2).
*/
static FKey *fkRefering(Table *pTab){
  int nName = sqlite3Strlen30(pTab->zName);
  return (FKey *)sqlite3HashFind(&pTab->pSchema->fkeyHash, pTab->zName, nName);
}

static void fkTriggerDelete(sqlite3 *dbMem, Trigger *p){
  if( p ){
    TriggerStep *pStep = p->step_list;
    sqlite3ExprDelete(dbMem, pStep->pWhere);
    sqlite3ExprListDelete(dbMem, pStep->pExprList);
    sqlite3ExprDelete(dbMem, p->pWhen);
    sqlite3DbFree(dbMem, p);
  }
}

void sqlite3FkCheck(
  Parse *pParse,                  /* Parse context */
  Table *pTab,                    /* Row is being deleted from this table */ 
  ExprList *pChanges,             /* Changed columns if this is an UPDATE */
  int regOld,                     /* Previous row data is stored here */
  int regNew                      /* New row data is stored here */
){
  sqlite3 *db = pParse->db;       /* Database handle */
  Vdbe *v;                        /* VM to write code to */
  FKey *pFKey;                    /* Used to iterate through FKs */
  int iDb;                        /* Index of database containing pTab */
  const char *zDb;                /* Name of database containing pTab */

  assert( ( pChanges &&  regOld &&  regNew)           /* UPDATE operation */
       || (!pChanges && !regOld &&  regNew)           /* INSERT operation */
       || (!pChanges &&  regOld && !regNew)           /* DELETE operation */
  );

  /* If foreign-keys are disabled, this function is a no-op. */
  if( (db->flags&SQLITE_ForeignKeys)==0 ) return;

  v = sqlite3GetVdbe(pParse);
  iDb = sqlite3SchemaToIndex(db, pTab->pSchema);
  zDb = db->aDb[iDb].zName;

  /* Loop through all the foreign key constraints attached to the table. */
  for(pFKey=pTab->pFKey; pFKey; pFKey=pFKey->pNextFrom){
    Table *pTo;                   /* Table referenced by this FK */
    Index *pIdx = 0;              /* Index on key columns in pTo */
    int *aiFree = 0;
    int *aiCol;
    int iCol;
    int i;

    if( pFKey->isDeferred==0 && regNew==0 ) continue;

    /* Find the table this foreign key references. Also find a unique 
    ** index on the referenced table that corresponds to the key columns. 
    ** If either of these things cannot be located, set an error in pParse
    ** and return early.  */
    pTo = sqlite3LocateTable(pParse, 0, pFKey->zTo, zDb);
    if( !pTo || locateFkeyIndex(pParse, pTo, pFKey, &pIdx, &aiFree) ) return;
    assert( pFKey->nCol==1 || (aiFree && pIdx) );

    /* If the key does not overlap with the pChanges list, skip this FK. */
    if( pChanges ){
      /* TODO */
    }

    if( aiFree ){
      aiCol = aiFree;
    }else{
      iCol = pFKey->aCol[0].iFrom;
      aiCol = &iCol;
    }
    for(i=0; i<pFKey->nCol; i++){
      if( aiCol[i]==pTab->iPKey ){
        aiCol[i] = -1;
      }
    }

    /* Take a shared-cache advisory read-lock on the referenced table.
    ** Allocate a cursor to use to search the unique index on the FK 
    ** columns in the referenced table.  */
    sqlite3TableLock(pParse, iDb, pTo->tnum, 0, pTo->zName);
    pParse->nTab++;

    if( regOld!=0 && pFKey->isDeferred ){
      fkCheckReference(pParse, iDb, pTo, pIdx, pFKey, aiCol, regOld, -1);
    }
    if( regNew!=0 ){
      fkCheckReference(pParse, iDb, pTo, pIdx, pFKey, aiCol, regNew, +1);
    }

    sqlite3DbFree(db, aiFree);
  }

  /* Loop through all the foreign key constraints that refer to this table */
  for(pFKey = fkRefering(pTab); pFKey; pFKey=pFKey->pNextTo){
    int iGoto;                    /* Address of OP_Goto instruction */
    Index *pIdx = 0;              /* Foreign key index for pFKey */
    SrcList *pSrc;
    int *aiCol = 0;

    /* For immediate constraints, skip this scan if:
    **
    **   1) this is an INSERT operation, or
    **   2) an UPDATE operation and the FK action is a trigger-action, or
    **   3) a DELETE operation and the FK action is a trigger-action.
    **
    ** A "trigger-action" is one of CASCADE, SET DEFAULT or SET NULL.
    */
    if( pFKey->isDeferred==0 ){
      if( regOld==0 ) continue;                                     /* 1 */
      if( regNew!=0 && pFKey->updateConf>OE_Restrict ) continue;    /* 2 */
      if( regNew==0 && pFKey->deleteConf>OE_Restrict ) continue;    /* 3 */
    }

    if( locateFkeyIndex(pParse, pTab, pFKey, &pIdx, &aiCol) ) return;
    assert( aiCol || pFKey->nCol==1 );

    /* Check if this update statement has modified any of the key columns
    ** for this foreign key constraint. If it has not, there is no need
    ** to search the referencing table for rows in violation. This is
    ** just an optimization. Things would work fine without this check.  */
    if( pChanges ){
      /* TODO */
    }

    /* Create a SrcList structure containing a single table (the table 
    ** the foreign key that refers to this table is attached to). This
    ** is required for the sqlite3WhereXXX() interface.  */
    pSrc = sqlite3SrcListAppend(db, 0, 0, 0);
    if( pSrc ){
      pSrc->a->pTab = pFKey->pFrom;
      pSrc->a->pTab->nRef++;
      pSrc->a->iCursor = pParse->nTab++;
  
      /* If this is an UPDATE, and none of the columns associated with this
      ** FK have been modified, do not scan the referencing table. Unlike
      ** the compile-time test implemented above, this is not just an 
      ** optimization. It is required so that immediate foreign keys do not 
      ** throw exceptions when the user executes a statement like:
      **
      **     UPDATE refd_table SET refd_column = refd_column
      */
      if( pChanges ){
        int i;
        int iJump = sqlite3VdbeCurrentAddr(v) + pFKey->nCol + 1;
        for(i=0; i<pFKey->nCol; i++){
          int iOff = (pIdx ? pIdx->aiColumn[i] : -1) + 1;
          sqlite3VdbeAddOp3(v, OP_Ne, regOld+iOff, iJump, regNew+iOff);
        }
        iGoto = sqlite3VdbeAddOp0(v, OP_Goto);
      }
  
      if( regNew!=0 && pFKey->isDeferred ){
        fkScanReferences(pParse, pSrc, pIdx, pFKey, aiCol, regNew, -1);
      }
      if( regOld!=0 ){
        /* If there is a RESTRICT action configured for the current operation
        ** on the referenced table of this FK, then throw an exception 
        ** immediately if the FK constraint is violated, even if this is a
        ** deferred trigger. That's what RESTRICT means. To defer checking
        ** the constraint, the FK should specify NO ACTION (represented
        ** using OE_None). NO ACTION is the default.  */
        fkScanReferences(pParse, pSrc, pIdx, pFKey, aiCol, regOld, 
            (pChanges!=0 && pFKey->updateConf!=OE_Restrict)
         || (pChanges==0 && pFKey->deleteConf!=OE_Restrict)
        );
      }
  
      if( pChanges ){
        sqlite3VdbeJumpHere(v, iGoto);
      }
      sqlite3SrcListDelete(db, pSrc);
    }
    sqlite3DbFree(db, aiCol);
  }
}

#define COLUMN_MASK(x) (((x)>31) ? 0xffffffff : ((u32)1<<(x)))

/*
** This function is called before generating code to update or delete a 
** row contained in table pTab. If the operation is an update, then 
** pChanges is a pointer to the list of columns to modify. If this is a 
** delete, then pChanges is NULL.
*/
u32 sqlite3FkOldmask(
  Parse *pParse,                  /* Parse context */
  Table *pTab,                    /* Table being modified */
  ExprList *pChanges              /* Non-NULL for UPDATE operations */
){
  u32 mask = 0;
  if( pParse->db->flags&SQLITE_ForeignKeys ){
    FKey *p;
    int i;
    for(p=pTab->pFKey; p; p=p->pNextFrom){
      if( pChanges || p->isDeferred ){
        for(i=0; i<p->nCol; i++) mask |= COLUMN_MASK(p->aCol[i].iFrom);
      }
    }
    for(p=fkRefering(pTab); p; p=p->pNextTo){
      Index *pIdx = 0;
      locateFkeyIndex(0, pTab, p, &pIdx, 0);
      if( pIdx ){
        for(i=0; i<pIdx->nColumn; i++) mask |= COLUMN_MASK(pIdx->aiColumn[i]);
      }
    }
  }
  return mask;
}

/*
** This function is called before generating code to update or delete a 
** row contained in table pTab. If the operation is an update, then 
** pChanges is a pointer to the list of columns to modify. If this is a 
** delete, then pChanges is NULL.
**
** If any foreign key processing will be required, this function returns
** true. If there is no foreign key related processing, this function 
** returns false.
*/
int sqlite3FkRequired(
  Parse *pParse,                  /* Parse context */
  Table *pTab,                    /* Table being modified */
  ExprList *pChanges              /* Non-NULL for UPDATE operations */
){
  if( pParse->db->flags&SQLITE_ForeignKeys ){
    FKey *p;
    for(p=pTab->pFKey; p; p=p->pNextFrom){
      if( pChanges || p->isDeferred ) return 1;
    }
    if( fkRefering(pTab) ) return 1;
  }
  return 0;
}

static Trigger *fkActionTrigger(
  Parse *pParse,
  Table *pTab,                    /* Table being updated or deleted from */
  FKey *pFKey,                    /* Foreign key to get action for */
  ExprList *pChanges              /* Change-list for UPDATE, NULL for DELETE */
){
  sqlite3 *db = pParse->db;       /* Database handle */
  int action;                     /* One of OE_None, OE_Cascade etc. */
  Trigger *pTrigger;              /* Trigger definition to return */

  if( pChanges ){
    action = pFKey->updateConf;
    pTrigger = pFKey->pOnUpdate;
  }else{
    action = pFKey->deleteConf;
    pTrigger = pFKey->pOnDelete;
  }

  assert( OE_SetNull>OE_Restrict && OE_SetDflt>OE_Restrict );
  assert( OE_Cascade>OE_Restrict && OE_None<OE_Restrict );

  if( action>OE_Restrict && !pTrigger ){
    u8 enableLookaside;           /* Copy of db->lookaside.bEnabled */
    char const *zFrom;            /* Name of referencing table */
    int nFrom;                    /* Length in bytes of zFrom */
    Index *pIdx = 0;              /* Parent key index for this FK */
    int *aiCol = 0;               /* child table cols -> parent key cols */
    TriggerStep *pStep;           /* First (only) step of trigger program */
    Expr *pWhere = 0;             /* WHERE clause of trigger step */
    ExprList *pList = 0;          /* Changes list if ON UPDATE CASCADE */
    int i;                        /* Iterator variable */
    Expr *pWhen = 0;              /* WHEN clause for the trigger */

    if( locateFkeyIndex(pParse, pTab, pFKey, &pIdx, &aiCol) ) return 0;
    assert( aiCol || pFKey->nCol==1 );

    for(i=0; i<pFKey->nCol; i++){
      Token tOld = { "old", 3 };  /* Literal "old" token */
      Token tNew = { "new", 3 };  /* Literal "new" token */
      Token tFromCol;             /* Name of column in referencing table */
      Token tToCol;               /* Name of column in referenced table */
      int iFromCol;               /* Idx of column in referencing table */
      Expr *pEq;                  /* tFromCol = OLD.tToCol */

      iFromCol = aiCol ? aiCol[i] : pFKey->aCol[0].iFrom;
      tToCol.z = pIdx ? pTab->aCol[pIdx->aiColumn[i]].zName : "oid";
      tFromCol.z = iFromCol<0 ? "oid" : pFKey->pFrom->aCol[iFromCol].zName;

      tToCol.n = sqlite3Strlen30(tToCol.z);
      tFromCol.n = sqlite3Strlen30(tFromCol.z);

      /* Create the expression "zFromCol = OLD.zToCol" */
      pEq = sqlite3PExpr(pParse, TK_EQ,
          sqlite3PExpr(pParse, TK_ID, 0, 0, &tFromCol),
          sqlite3PExpr(pParse, TK_DOT, 
            sqlite3PExpr(pParse, TK_ID, 0, 0, &tOld),
            sqlite3PExpr(pParse, TK_ID, 0, 0, &tToCol)
          , 0)
      , 0);
      pWhere = sqlite3ExprAnd(db, pWhere, pEq);

      /* For ON UPDATE, construct the next term of the WHEN clause.
      ** The final WHEN clause will be like this:
      **
      **    WHEN NOT(old.col1 IS new.col1 AND ... AND old.colN IS new.colN)
      */
      if( pChanges ){
        pEq = sqlite3PExpr(pParse, TK_IS,
            sqlite3PExpr(pParse, TK_DOT, 
              sqlite3PExpr(pParse, TK_ID, 0, 0, &tOld),
              sqlite3PExpr(pParse, TK_ID, 0, 0, &tToCol),
              0),
            sqlite3PExpr(pParse, TK_DOT, 
              sqlite3PExpr(pParse, TK_ID, 0, 0, &tNew),
              sqlite3PExpr(pParse, TK_ID, 0, 0, &tToCol),
              0),
            0);
        pWhen = sqlite3ExprAnd(db, pWhen, pEq);
      }
  
      if( action!=OE_Cascade || pChanges ){
        Expr *pNew;
        if( action==OE_Cascade ){
          pNew = sqlite3PExpr(pParse, TK_DOT, 
            sqlite3PExpr(pParse, TK_ID, 0, 0, &tNew),
            sqlite3PExpr(pParse, TK_ID, 0, 0, &tToCol)
          , 0);
        }else if( action==OE_SetDflt ){
          Expr *pDflt = pFKey->pFrom->aCol[iFromCol].pDflt;
          if( pDflt ){
            pNew = sqlite3ExprDup(db, pDflt, 0);
          }else{
            pNew = sqlite3PExpr(pParse, TK_NULL, 0, 0, 0);
          }
        }else{
          pNew = sqlite3PExpr(pParse, TK_NULL, 0, 0, 0);
        }
        pList = sqlite3ExprListAppend(pParse, pList, pNew);
        sqlite3ExprListSetName(pParse, pList, &tFromCol, 0);
      }
    }
    sqlite3DbFree(db, aiCol);

    /* If pTab->dbMem==0, then the table may be part of a shared-schema.
    ** Disable the lookaside buffer before allocating space for the
    ** trigger definition in this case.  */
    enableLookaside = db->lookaside.bEnabled;
    if( pTab->dbMem==0 ){
      db->lookaside.bEnabled = 0;
    }

    zFrom = pFKey->pFrom->zName;
    nFrom = sqlite3Strlen30(zFrom);
    pTrigger = (Trigger *)sqlite3DbMallocZero(db, 
        sizeof(Trigger) +         /* struct Trigger */
        sizeof(TriggerStep) +     /* Single step in trigger program */
        nFrom + 1                 /* Space for pStep->target.z */
    );
    if( pTrigger ){
      pStep = pTrigger->step_list = (TriggerStep *)&pTrigger[1];
      pStep->target.z = (char *)&pStep[1];
      pStep->target.n = nFrom;
      memcpy((char *)pStep->target.z, zFrom, nFrom);
  
      pStep->pWhere = sqlite3ExprDup(db, pWhere, EXPRDUP_REDUCE);
      pStep->pExprList = sqlite3ExprListDup(db, pList, EXPRDUP_REDUCE);
      if( pWhen ){
        pWhen = sqlite3PExpr(pParse, TK_NOT, pWhen, 0, 0);
        pTrigger->pWhen = sqlite3ExprDup(db, pWhen, EXPRDUP_REDUCE);
      }
    }

    /* Re-enable the lookaside buffer, if it was disabled earlier. */
    db->lookaside.bEnabled = enableLookaside;

    sqlite3ExprDelete(db, pWhere);
    sqlite3ExprDelete(db, pWhen);
    sqlite3ExprListDelete(db, pList);
    if( db->mallocFailed==1 ){
      fkTriggerDelete(db, pTrigger);
      return 0;
    }

    pStep->op = (action!=OE_Cascade || pChanges) ? TK_UPDATE : TK_DELETE;
    pStep->pTrig = pTrigger;
    pTrigger->pSchema = pTab->pSchema;
    pTrigger->pTabSchema = pTab->pSchema;

    if( pChanges ){
      pFKey->pOnUpdate = pTrigger;
      pTrigger->op = TK_UPDATE;
      pStep->op = TK_UPDATE;
    }else{
      pFKey->pOnDelete = pTrigger;
      pTrigger->op = TK_DELETE;
      pStep->op = (action==OE_Cascade)?TK_DELETE:TK_UPDATE;
    }
  }

  return pTrigger;
}

/*
** This function is called when deleting or updating a row to implement
** any required CASCADE, SET NULL or SET DEFAULT actions.
*/
void sqlite3FkActions(
  Parse *pParse,                  /* Parse context */
  Table *pTab,                    /* Table being updated or deleted from */
  ExprList *pChanges,             /* Change-list for UPDATE, NULL for DELETE */
  int regOld                      /* Address of array containing old row */
){
  /* If foreign-key support is enabled, iterate through all FKs that 
  ** refer to table pTab. If there is an action associated with the FK 
  ** for this operation (either update or delete), invoke the associated 
  ** trigger sub-program.  */
  if( pParse->db->flags&SQLITE_ForeignKeys ){
    FKey *pFKey;                  /* Iterator variable */
    for(pFKey = fkRefering(pTab); pFKey; pFKey=pFKey->pNextTo){
      Trigger *pAction = fkActionTrigger(pParse, pTab, pFKey, pChanges);
      if( pAction ){
        sqlite3CodeRowTriggerDirect(pParse, pAction, pTab, regOld, OE_Abort, 0);
      }
    }
  }
}

#endif /* ifndef SQLITE_OMIT_TRIGGER */

/*
** Free all memory associated with foreign key definitions attached to
** table pTab. Remove the deleted foreign keys from the Schema.fkeyHash
** hash table.
*/
void sqlite3FkDelete(Table *pTab){
  FKey *pFKey;                    /* Iterator variable */
  FKey *pNext;                    /* Copy of pFKey->pNextFrom */

  for(pFKey=pTab->pFKey; pFKey; pFKey=pNext){

    /* Remove the FK from the fkeyHash hash table. */
    if( pFKey->pPrevTo ){
      pFKey->pPrevTo->pNextTo = pFKey->pNextTo;
    }else{
      void *data = (void *)pFKey->pNextTo;
      const char *z = (data ? pFKey->pNextTo->zTo : pFKey->zTo);
      sqlite3HashInsert(&pTab->pSchema->fkeyHash, z, sqlite3Strlen30(z), data);
    }
    if( pFKey->pNextTo ){
      pFKey->pNextTo->pPrevTo = pFKey->pPrevTo;
    }

    /* Delete any triggers created to implement actions for this FK. */
#ifndef SQLITE_OMIT_TRIGGER
    fkTriggerDelete(pTab->dbMem, pFKey->pOnDelete);
    fkTriggerDelete(pTab->dbMem, pFKey->pOnUpdate);
#endif

    /* Delete the memory allocated for the FK structure. */
    pNext = pFKey->pNextFrom;
    sqlite3DbFree(pTab->dbMem, pFKey);
  }
}
#endif /* ifndef SQLITE_OMIT_FOREIGN_KEY */
