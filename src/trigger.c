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
*
*/
#include "sqliteInt.h"

/*
** This is called by the parser when it sees a CREATE TRIGGER statement. See
** comments surrounding struct Trigger in sqliteInt.h for a description of 
** how triggers are stored.
*/
void sqliteCreateTrigger(
  Parse *pParse,      /* The parse context of the CREATE TRIGGER statement */
  Token *pName,       /* The name of the trigger */
  int tr_tm,          /* One of TK_BEFORE, TK_AFTER */
  int op,             /* One of TK_INSERT, TK_UPDATE, TK_DELETE */
  IdList *pColumns,   /* column list if this is an UPDATE OF trigger */
  Token *pTableName,  /* The name of the table/view the trigger applies to */
  int foreach,        /* One of TK_ROW or TK_STATEMENT */
  Expr *pWhen,        /* WHEN clause */
  TriggerStep *pStepList, /* The triggered program */
  char const *zData,  /* The string data to make persistent */
  int zDataLen
){
  Trigger *nt;
  Table   *tab;
  int offset;
  TriggerStep *ss;

  /* Check that: 
  ** 1. the trigger name does not already exist.
  ** 2. the table (or view) does exist.
  */
  {
    char *tmp_str = sqliteStrNDup(pName->z, pName->n);
    if( sqliteHashFind(&(pParse->db->trigHash), tmp_str, pName->n + 1) ){
      sqliteSetNString(&pParse->zErrMsg, "trigger ", -1,
          pName->z, pName->n, " already exists", -1, 0);
      sqliteFree(tmp_str);
      pParse->nErr++;
      goto trigger_cleanup;
    }
    sqliteFree(tmp_str);
  }
  {
    char *tmp_str = sqliteStrNDup(pTableName->z, pTableName->n);
    tab = sqliteFindTable(pParse->db, tmp_str);
    sqliteFree(tmp_str);
    if( !tab ){
      sqliteSetNString(&pParse->zErrMsg, "no such table: ", -1,
          pTableName->z, pTableName->n, 0);
      pParse->nErr++;
      goto trigger_cleanup;
    }
  }

  /* Build the Trigger object */
  nt = (Trigger*)sqliteMalloc(sizeof(Trigger));
  nt->name = sqliteStrNDup(pName->z, pName->n);
  nt->table = sqliteStrNDup(pTableName->z, pTableName->n);
  nt->op = op;
  nt->tr_tm = tr_tm;
  nt->pWhen = pWhen;
  nt->pColumns = pColumns;
  nt->foreach = foreach;
  nt->step_list = pStepList;
  nt->isCommit = 0;

  nt->strings = sqliteStrNDup(zData, zDataLen);
  offset = (int)(nt->strings - zData);

  sqliteExprMoveStrings(nt->pWhen, offset);

  ss = nt->step_list;
  while (ss) {
    sqliteSelectMoveStrings(ss->pSelect, offset);
    if (ss->target.z) ss->target.z += offset;
    sqliteExprMoveStrings(ss->pWhere, offset);
    sqliteExprListMoveStrings(ss->pExprList, offset);

    ss = ss->pNext;
  }

  /* if we are not initializing, and this trigger is not on a TEMP table, 
  ** build the sqlite_master entry
  */
  if( !pParse->initFlag && !tab->isTemp ){

    /* Make an entry in the sqlite_master table */
    sqliteBeginWriteOperation(pParse);

    sqliteVdbeAddOp(pParse->pVdbe,        OP_OpenWrite, 0, 2);
    sqliteVdbeChangeP3(pParse->pVdbe, -1, MASTER_NAME,           P3_STATIC);
    sqliteVdbeAddOp(pParse->pVdbe,        OP_NewRecno,  0, 0);
    sqliteVdbeAddOp(pParse->pVdbe,        OP_String,    0, 0);
    sqliteVdbeChangeP3(pParse->pVdbe, -1, "trigger",             P3_STATIC);
    sqliteVdbeAddOp(pParse->pVdbe,        OP_String,    0, 0);
    sqliteVdbeChangeP3(pParse->pVdbe, -1, nt->name,        0); 
    sqliteVdbeAddOp(pParse->pVdbe,        OP_String,    0, 0);
    sqliteVdbeChangeP3(pParse->pVdbe, -1, nt->table,        0); 
    sqliteVdbeAddOp(pParse->pVdbe,        OP_Integer,    0, 0);
    sqliteVdbeAddOp(pParse->pVdbe,        OP_String,    0, 0);
    sqliteVdbeChangeP3(pParse->pVdbe, -1, nt->strings,     0);
    sqliteVdbeAddOp(pParse->pVdbe,        OP_MakeRecord, 5, 0);
    sqliteVdbeAddOp(pParse->pVdbe,        OP_PutIntKey, 0, 1);

    /* Change the cookie, since the schema is changed */
    sqliteChangeCookie(pParse->db);
    sqliteVdbeAddOp(pParse->pVdbe, OP_Integer, pParse->db->next_cookie, 0);
    sqliteVdbeAddOp(pParse->pVdbe, OP_SetCookie, 0, 0);

    sqliteVdbeAddOp(pParse->pVdbe,        OP_Close,     0, 0);

    sqliteEndWriteOperation(pParse);
  }

  if( !pParse->explain ){
    /* Stick it in the hash-table */
    sqliteHashInsert(&(pParse->db->trigHash), nt->name, pName->n + 1, nt);

    /* Attach it to the table object */
    nt->pNext = tab->pTrigger;
    tab->pTrigger = nt;
    return;
  } else {
    sqliteFree(nt->strings);
    sqliteFree(nt->name);
    sqliteFree(nt->table);
    sqliteFree(nt);
  }

trigger_cleanup:

  sqliteIdListDelete(pColumns);
  sqliteExprDelete(pWhen);
  {
    TriggerStep * pp;
    TriggerStep * nn;

    pp = pStepList;
    while (pp) {
      nn = pp->pNext;
      sqliteExprDelete(pp->pWhere);
      sqliteExprListDelete(pp->pExprList);
      sqliteSelectDelete(pp->pSelect);
      sqliteIdListDelete(pp->pIdList);
      sqliteFree(pp);
      pp = nn;
    }
  }
}

TriggerStep *sqliteTriggerSelectStep(Select * pSelect)
{
  TriggerStep *pTriggerStep = sqliteMalloc(sizeof(TriggerStep));

  pTriggerStep->op = TK_SELECT;
  pTriggerStep->pSelect = pSelect;
  pTriggerStep->orconf = OE_Default;

  return pTriggerStep;
}

TriggerStep *sqliteTriggerInsertStep(
  Token *pTableName,
  IdList *pColumn,
  ExprList *pEList,
  Select *pSelect,
  int orconf
){
  TriggerStep *pTriggerStep = sqliteMalloc(sizeof(TriggerStep));

  assert(pEList == 0 || pSelect == 0);
  assert(pEList != 0 || pSelect != 0);

  pTriggerStep->op = TK_INSERT;
  pTriggerStep->pSelect = pSelect;
  pTriggerStep->target  = *pTableName;
  pTriggerStep->pIdList = pColumn;
  pTriggerStep->pExprList = pEList;
  pTriggerStep->orconf = orconf;

  return pTriggerStep;
}

TriggerStep *sqliteTriggerUpdateStep(
  Token *pTableName, 
  ExprList *pEList, 
  Expr *pWhere, 
  int orconf)
{
  TriggerStep *pTriggerStep = sqliteMalloc(sizeof(TriggerStep));

  pTriggerStep->op = TK_UPDATE;
  pTriggerStep->target  = *pTableName;
  pTriggerStep->pExprList = pEList;
  pTriggerStep->pWhere = pWhere;
  pTriggerStep->orconf = orconf;

  return pTriggerStep;
}

TriggerStep *sqliteTriggerDeleteStep(Token *pTableName, Expr *pWhere)
{
  TriggerStep * pTriggerStep = sqliteMalloc(sizeof(TriggerStep));

  pTriggerStep->op = TK_DELETE;
  pTriggerStep->target  = *pTableName;
  pTriggerStep->pWhere = pWhere;
  pTriggerStep->orconf = OE_Default;

  return pTriggerStep;
}

/* 
** Recursively delete a Trigger structure
*/
void sqliteDeleteTrigger(Trigger *pTrigger)
{
  TriggerStep *pTriggerStep;

  pTriggerStep = pTrigger->step_list;
  while (pTriggerStep) {
    TriggerStep * pTmp = pTriggerStep;
    pTriggerStep = pTriggerStep->pNext;

    sqliteExprDelete(pTmp->pWhere);
    sqliteExprListDelete(pTmp->pExprList);
    sqliteSelectDelete(pTmp->pSelect);
    sqliteIdListDelete(pTmp->pIdList);

    sqliteFree(pTmp);
  }

  sqliteFree(pTrigger->name);
  sqliteFree(pTrigger->table);
  sqliteExprDelete(pTrigger->pWhen);
  sqliteIdListDelete(pTrigger->pColumns);
  sqliteFree(pTrigger->strings);
  sqliteFree(pTrigger);
}

/*
 * This function is called to drop a trigger from the database schema. 
 *
 * This may be called directly from the parser, or from within 
 * sqliteDropTable(). In the latter case the "nested" argument is true.
 *
 * Note that this function does not delete the trigger entirely. Instead it
 * removes it from the internal schema and places it in the trigDrop hash 
 * table. This is so that the trigger can be restored into the database schema
 * if the transaction is rolled back.
 */
void sqliteDropTrigger(Parse *pParse, Token *pName, int nested)
{
  char *zName;
  Trigger *pTrigger;
  Table   *pTable;

  zName = sqliteStrNDup(pName->z, pName->n);

  /* ensure that the trigger being dropped exists */
  pTrigger = sqliteHashFind(&(pParse->db->trigHash), zName, pName->n + 1); 
  if( !pTrigger ){
    sqliteSetNString(&pParse->zErrMsg, "no such trigger: ", -1,
        zName, -1, 0);
    sqliteFree(zName);
    return;
  }

  /*
   * If this is not an "explain", do the following:
   * 1. Remove the trigger from its associated table structure
   * 2. Move the trigger from the trigHash hash to trigDrop
   */
  if( !pParse->explain ){
    /* 1 */
    pTable = sqliteFindTable(pParse->db, pTrigger->table);
    assert(pTable);
    if( pTable->pTrigger == pTrigger ){
      pTable->pTrigger = pTrigger->pNext;
    } else {
      Trigger *cc = pTable->pTrigger;
      while( cc ){ 
        if( cc->pNext == pTrigger ){
          cc->pNext = cc->pNext->pNext;
          break;
        }
        cc = cc->pNext;
      }
      assert(cc);
    }

    /* 2 */
    sqliteHashInsert(&(pParse->db->trigHash), zName, 
        pName->n + 1, NULL);
    sqliteHashInsert(&(pParse->db->trigDrop), pTrigger->name, 
        pName->n + 1, pTrigger);
  }

  /* Unless this is a trigger on a TEMP TABLE, generate code to destroy the
   * database record of the trigger */
  if( !pTable->isTemp ){
    int base;
    static VdbeOp dropTrigger[] = {
      { OP_OpenWrite,  0, 2,        MASTER_NAME},
      { OP_Rewind,     0, ADDR(9),  0},
      { OP_String,     0, 0,        0}, /* 2 */
      { OP_MemStore,   1, 1,        0},
      { OP_MemLoad,    1, 0,        0}, /* 4 */
      { OP_Column,     0, 1,        0},
      { OP_Ne,         0, ADDR(8),  0},
      { OP_Delete,     0, 0,        0},
      { OP_Next,       0, ADDR(4),  0}, /* 8 */
      { OP_Integer,    0, 0,        0}, /* 9 */
      { OP_SetCookie,  0, 0,        0},
      { OP_Close,      0, 0,        0},
    };

    if( !nested ){
      sqliteBeginWriteOperation(pParse);
    }
    base = sqliteVdbeAddOpList(pParse->pVdbe, 
        ArraySize(dropTrigger), dropTrigger);
    sqliteVdbeChangeP3(pParse->pVdbe, base+2, zName, 0);
    if( !nested ){
      sqliteChangeCookie(pParse->db);
    }
    sqliteVdbeChangeP1(pParse->pVdbe, base+9, pParse->db->next_cookie);
    if( !nested ){
      sqliteEndWriteOperation(pParse);
    }
  }

  sqliteFree(zName);
}

static int checkColumnOverLap(IdList * pIdList, ExprList * pEList)
{
  int i, e;
  if (!pIdList) return 1;
  if (!pEList) return 1;

  for (i = 0; i < pIdList->nId; i++) 
    for (e = 0; e < pEList->nExpr; e++) 
      if (!sqliteStrICmp(pIdList->a[i].zName, pEList->a[e].zName))
        return 1;

  return 0; 
}

/* A global variable that is TRUE if we should always set up temp tables for
 * for triggers, even if there are no triggers to code. This is used to test 
 * how much overhead the triggers algorithm is causing.
 *
 * This flag can be set or cleared using the "trigger_overhead_test" pragma.
 * The pragma is not documented since it is not really part of the interface
 * to SQLite, just the test procedure.
*/
int always_code_trigger_setup = 0;

/*
 * Returns true if a trigger matching op, tr_tm and foreach that is NOT already
 * on the Parse objects trigger-stack (to prevent recursive trigger firing) is
 * found in the list specified as pTrigger.
 */
int sqliteTriggersExist(
  Parse *pParse, 
  Trigger *pTrigger,
  int op,                 /* one of TK_DELETE, TK_INSERT, TK_UPDATE */
  int tr_tm,              /* one of TK_BEFORE, TK_AFTER */
  int foreach,            /* one of TK_ROW or TK_STATEMENT */
  ExprList *pChanges)
{
  Trigger * pTriggerCursor;

  if( always_code_trigger_setup ){
    return 1;
  }

  pTriggerCursor = pTrigger;
  while( pTriggerCursor ){
    if( pTriggerCursor->op == op && 
	pTriggerCursor->tr_tm == tr_tm && 
	pTriggerCursor->foreach == foreach &&
	checkColumnOverLap(pTriggerCursor->pColumns, pChanges) ){
      TriggerStack * ss;
      ss = pParse->trigStack;
      while (ss && ss->pTrigger != pTrigger) ss = ss->pNext;
      if (!ss) return 1;
    }
    pTriggerCursor = pTriggerCursor->pNext;
  }

  return 0;
}

static int codeTriggerProgram(
  Parse *pParse, 
  TriggerStep *pStepList, 
  int orconfin
){
  TriggerStep * pTriggerStep = pStepList;
  int orconf;

  while( pTriggerStep ){
    int saveNTab = pParse->nTab;
    orconf = (orconfin == OE_Default)?pTriggerStep->orconf:orconfin;
    pParse->trigStack->orconf = orconf;
    switch( pTriggerStep->op ){
      case TK_SELECT: {
        int tmp_tbl = pParse->nTab++;
	sqliteVdbeAddOp(pParse->pVdbe, OP_OpenTemp, tmp_tbl, 0);
	sqliteVdbeAddOp(pParse->pVdbe, OP_KeyAsData, tmp_tbl, 1);
	sqliteSelect(pParse, pTriggerStep->pSelect, SRT_Union, 
	    tmp_tbl, 0, 0, 0);
	sqliteVdbeAddOp(pParse->pVdbe, OP_Close, tmp_tbl, 0);
	pParse->nTab--;
	break;
      }
      case TK_UPDATE: {
        sqliteVdbeAddOp(pParse->pVdbe, OP_PushList, 0, 0);
        sqliteUpdate(pParse, &pTriggerStep->target, 
        sqliteExprListDup(pTriggerStep->pExprList), 
        sqliteExprDup(pTriggerStep->pWhere), orconf);
        sqliteVdbeAddOp(pParse->pVdbe, OP_PopList, 0, 0);
        break;
      }
      case TK_INSERT: {
        sqliteInsert(pParse, &pTriggerStep->target, 
        sqliteExprListDup(pTriggerStep->pExprList), 
        sqliteSelectDup(pTriggerStep->pSelect), 
        sqliteIdListDup(pTriggerStep->pIdList), orconf);
        break;
      }
      case TK_DELETE: {
        sqliteVdbeAddOp(pParse->pVdbe, OP_PushList, 0, 0);
        sqliteDeleteFrom(pParse, &pTriggerStep->target, 
	    sqliteExprDup(pTriggerStep->pWhere));
        sqliteVdbeAddOp(pParse->pVdbe, OP_PopList, 0, 0);
        break;
      }
      default:
        assert(0);
    } 
    pParse->nTab = saveNTab;
    pTriggerStep = pTriggerStep->pNext;
  }

  return 0;
}

/*
** This is called to code FOR EACH ROW triggers.
**
** When the code that this function generates is executed, the following 
** must be true:
** 1. NO vdbe cursors must be open.
** 2. If the triggers being coded are ON INSERT or ON UPDATE triggers, then
**    a temporary vdbe cursor (index newIdx) must be open and pointing at
**    a row containing values to be substituted for new.* expressions in the
**    trigger program(s).
** 3. If the triggers being coded are ON DELETE or ON UPDATE triggers, then
**    a temporary vdbe cursor (index oldIdx) must be open and pointing at
**    a row containing values to be substituted for old.* expressions in the
**    trigger program(s).
**
*/
int sqliteCodeRowTrigger(
  Parse *pParse,       /* Parse context */
  int op,              /* One of TK_UPDATE, TK_INSERT, TK_DELETE */
  ExprList *pChanges,  /* Changes list for any UPDATE OF triggers */
  int tr_tm,           /* One of TK_BEFORE, TK_AFTER */
  Table *pTab,         /* The table to code triggers from */
  int newIdx,          /* The indice of the "new" row to access */
  int oldIdx,          /* The indice of the "old" row to access */
  int orconf)          /* ON CONFLICT policy */
{
  Trigger * pTrigger;
  TriggerStack * pTriggerStack;

  assert(op == TK_UPDATE || op == TK_INSERT || op == TK_DELETE);
  assert(tr_tm == TK_BEFORE || tr_tm == TK_AFTER);

  assert(newIdx != -1 || oldIdx != -1);

  pTrigger = pTab->pTrigger;
  while (pTrigger) {
    int fire_this = 0;

    /* determine whether we should code this trigger */
    if (pTrigger->op == op && pTrigger->tr_tm == tr_tm && 
        pTrigger->foreach == TK_ROW) {
      fire_this = 1;
      pTriggerStack = pParse->trigStack;
      while (pTriggerStack) {
        if (pTriggerStack->pTrigger == pTrigger) fire_this = 0;
        pTriggerStack = pTriggerStack->pNext;
      }
      if (op == TK_UPDATE && pTrigger->pColumns &&
          !checkColumnOverLap(pTrigger->pColumns, pChanges))
        fire_this = 0;
    }

    if (fire_this) {
      int endTrigger;
      IdList dummyTablist;
      Expr * whenExpr;

      dummyTablist.nId = 0;
      dummyTablist.a = 0;

      /* Push an entry on to the trigger stack */
      pTriggerStack = sqliteMalloc(sizeof(TriggerStack));
      pTriggerStack->pTrigger = pTrigger;
      pTriggerStack->newIdx = newIdx;
      pTriggerStack->oldIdx = oldIdx;
      pTriggerStack->pTab = pTab;
      pTriggerStack->pNext = pParse->trigStack;
      pParse->trigStack = pTriggerStack;

      /* code the WHEN clause */
      endTrigger = sqliteVdbeMakeLabel(pParse->pVdbe);
      whenExpr = sqliteExprDup(pTrigger->pWhen);
      if (sqliteExprResolveIds(pParse, 0, &dummyTablist, 0, whenExpr)) {
        pParse->trigStack = pParse->trigStack->pNext;
        sqliteFree(pTriggerStack);
        sqliteExprDelete(whenExpr);
        return 1;
      }
      sqliteExprIfFalse(pParse, whenExpr, endTrigger);
      sqliteExprDelete(whenExpr);

      codeTriggerProgram(pParse, pTrigger->step_list, orconf); 

      /* Pop the entry off the trigger stack */
      pParse->trigStack = pParse->trigStack->pNext;
      sqliteFree(pTriggerStack);

      sqliteVdbeResolveLabel(pParse->pVdbe, endTrigger);
    }
    pTrigger = pTrigger->pNext;
  }

  return 0;
}

/*
 * This function is called to code ON UPDATE and ON DELETE triggers on 
 * views. 
 *
 * This function deletes the data pointed at by the pWhere and pChanges
 * arguments before it completes.
 */
void sqliteViewTriggers(
  Parse *pParse, 
  Table *pTab,         /* The view to code triggers on */
  Expr *pWhere,        /* The WHERE clause of the statement causing triggers*/
  int orconf,          /* The ON CONFLICT policy specified as part of the
			  statement causing these triggers */
  ExprList *pChanges   /* If this is an statement causing triggers to fire
			  is an UPDATE, then this list holds the columns
			  to update and the expressions to update them to.
			  See comments for sqliteUpdate(). */
){
  int oldIdx = -1;
  int newIdx = -1;
  int *aXRef = 0;   
  Vdbe *v;
  int endOfLoop;
  int startOfLoop;
  Select theSelect;
  Token tblNameToken;

  assert(pTab->pSelect);

  tblNameToken.z = pTab->zName;
  tblNameToken.n = strlen(pTab->zName);

  theSelect.isDistinct = 0;
  theSelect.pEList = sqliteExprListAppend(0, sqliteExpr(TK_ALL, 0, 0, 0), 0);
  theSelect.pSrc   = sqliteIdListAppend(0, &tblNameToken);
  theSelect.pWhere = pWhere;    pWhere = 0;
  theSelect.pGroupBy = 0;
  theSelect.pHaving = 0;
  theSelect.pOrderBy = 0;
  theSelect.op = TK_SELECT; /* ?? */
  theSelect.pPrior = 0;
  theSelect.nLimit = -1;
  theSelect.nOffset = -1;
  theSelect.zSelect = 0;
  theSelect.base = 0;

  v = sqliteGetVdbe(pParse);
  assert(v);
  sqliteBeginMultiWriteOperation(pParse);

  /* Allocate temp tables */
  oldIdx = pParse->nTab++;
  sqliteVdbeAddOp(v, OP_OpenTemp, oldIdx, 0);
  if( pChanges ){
    newIdx = pParse->nTab++;
    sqliteVdbeAddOp(v, OP_OpenTemp, newIdx, 0);
  }

  /* Snapshot the view */
  if( sqliteSelect(pParse, &theSelect, SRT_Table, oldIdx, 0, 0, 0) ){
    goto trigger_cleanup;
  }

  /* loop thru the view snapshot, executing triggers for each row */
  endOfLoop = sqliteVdbeMakeLabel(v);
  sqliteVdbeAddOp(v, OP_Rewind, oldIdx, endOfLoop);

  /* Loop thru the view snapshot, executing triggers for each row */
  startOfLoop = sqliteVdbeCurrentAddr(v);

  /* Build the updated row if required */
  if( pChanges ){
    int ii, jj;

    aXRef = sqliteMalloc( sizeof(int) * pTab->nCol );
    if( aXRef==0 ) goto trigger_cleanup;
    for(ii = 0; ii < pTab->nCol; ii++){
      aXRef[ii] = -1;
    }

    for(ii=0; ii<pChanges->nExpr; ii++){
      int jj;
      if( sqliteExprResolveIds(pParse, oldIdx, theSelect.pSrc , 0, 
            pChanges->a[ii].pExpr) )
        goto trigger_cleanup;

      if( sqliteExprCheck(pParse, pChanges->a[ii].pExpr, 0, 0) )
        goto trigger_cleanup;

      for(jj=0; jj<pTab->nCol; jj++){
        if( sqliteStrICmp(pTab->aCol[jj].zName, pChanges->a[ii].zName)==0 ){
          aXRef[jj] = ii;
          break;
        }
      }
      if( jj>=pTab->nCol ){
        sqliteSetString(&pParse->zErrMsg, "no such column: ", 
            pChanges->a[ii].zName, 0);
        pParse->nErr++;
        goto trigger_cleanup;
      }
    }

    sqliteVdbeAddOp(v, OP_Integer, 13, 0);

    for(ii = 0; ii<pTab->nCol; ii++){
      if( aXRef[ii] < 0 ){ 
        sqliteVdbeAddOp(v, OP_Column, oldIdx, ii);
      } else {
        sqliteExprCode(pParse, pChanges->a[aXRef[ii]].pExpr);
      }
    }

    sqliteVdbeAddOp(v, OP_MakeRecord, pTab->nCol, 0);
    sqliteVdbeAddOp(v, OP_PutIntKey, newIdx, 0);
    sqliteVdbeAddOp(v, OP_Rewind, newIdx, 0);

    sqliteCodeRowTrigger(pParse, TK_UPDATE, pChanges, TK_BEFORE, 
        pTab, newIdx, oldIdx, orconf);
    sqliteCodeRowTrigger(pParse, TK_UPDATE, pChanges, TK_AFTER, 
        pTab, newIdx, oldIdx, orconf);
  } else {
    sqliteCodeRowTrigger(pParse, TK_DELETE, 0, TK_BEFORE, pTab, -1, oldIdx, 
        orconf);
    sqliteCodeRowTrigger(pParse, TK_DELETE, 0, TK_AFTER, pTab, -1, oldIdx, 
        orconf);
  }

  sqliteVdbeAddOp(v, OP_Next, oldIdx, startOfLoop);

  sqliteVdbeResolveLabel(v, endOfLoop);
  sqliteEndWriteOperation(pParse);

trigger_cleanup:
  sqliteFree(aXRef);
  sqliteExprListDelete(pChanges);
  sqliteExprDelete(pWhere);
  sqliteExprListDelete(theSelect.pEList);
  sqliteIdListDelete(theSelect.pSrc);
  sqliteExprDelete(theSelect.pWhere);
  return;
}
