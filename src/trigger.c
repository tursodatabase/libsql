/*
** All copyright on this work is disclaimed by the author.
*/
#include "sqliteInt.h"

/*
** This is called by the parser when it sees a CREATE TRIGGER statement
*/
void sqliteCreateTrigger(
  Parse *pParse,      /* The parse context of the CREATE TRIGGER statement */
  Token *nm,          /* The name of the trigger */
  int tr_tm,          /* One of TK_BEFORE, TK_AFTER */
  int op,             /* One of TK_INSERT, TK_UPDATE, TK_DELETE */
  IdList *cols,       /* column list if this is an UPDATE OF trigger */
  Token *tbl,         /* The name of the table/view the trigger applies to */
  int foreach,        /* One of TK_ROW or TK_STATEMENT */
  Expr *pWhen,        /* WHEN clause */
  TriggerStep *steps, /* The triggered program */
  char const *cc,     /* The string data to make persistent */
  int len
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
    char *tmp_str = sqliteStrNDup(nm->z, nm->n);
    if( sqliteHashFind(&(pParse->db->trigHash), tmp_str, nm->n + 1) ){
      sqliteSetNString(&pParse->zErrMsg, "trigger ", -1,
          nm->z, nm->n, " already exists", -1, 0);
      sqliteFree(tmp_str);
      pParse->nErr++;
      goto trigger_cleanup;
    }
    sqliteFree(tmp_str);
  }
  {
    char *tmp_str = sqliteStrNDup(tbl->z, tbl->n);
    tab = sqliteFindTable(pParse->db, tmp_str);
    sqliteFree(tmp_str);
    if( !tab ){
      sqliteSetNString(&pParse->zErrMsg, "no such table: ", -1,
          tbl->z, tbl->n, 0);
      pParse->nErr++;
      goto trigger_cleanup;
    }
  }

  /* Build the Trigger object */
  nt = (Trigger*)sqliteMalloc(sizeof(Trigger));
  nt->name = sqliteStrNDup(nm->z, nm->n);
  nt->table = sqliteStrNDup(tbl->z, tbl->n);
  nt->op = op;
  nt->tr_tm = tr_tm;
  nt->pWhen = pWhen;
  nt->pColumns = cols;
  nt->foreach = foreach;
  nt->step_list = steps;
  nt->isCommit = 0;

  nt->strings = sqliteStrNDup(cc, len);
  offset = (int)(nt->strings - cc);

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

  if (!pParse->explain) {
    /* Stick it in the hash-table */
    sqliteHashInsert(&(pParse->db->trigHash), nt->name, nm->n + 1, nt);

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

  sqliteIdListDelete(cols);
  sqliteExprDelete(pWhen);
  {
    TriggerStep * pp;
    TriggerStep * nn;

    pp = steps;
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

  TriggerStep * 
sqliteTriggerSelectStep(Select * s)
{
  TriggerStep * tt = sqliteMalloc(sizeof(TriggerStep));

  tt->op = TK_SELECT;
  tt->pSelect = s;
  tt->orconf = OE_Default;

  return tt;
}

TriggerStep * 
sqliteTriggerInsertStep(Token * tbl, IdList * col, ExprList * val, Select * s, int orconf)
{
  TriggerStep * tt = sqliteMalloc(sizeof(TriggerStep));

  assert(val == 0 || s == 0);
  assert(val != 0 || s != 0);

  tt->op = TK_INSERT;
  tt->pSelect = s;
  tt->target  = *tbl;
  tt->pIdList = col;
  tt->pExprList = val;
  tt->orconf = orconf;

  return tt;
}

TriggerStep * 
sqliteTriggerUpdateStep(Token * tbl, ExprList * val, Expr * w, int orconf)
{
  TriggerStep * tt = sqliteMalloc(sizeof(TriggerStep));

  tt->op = TK_UPDATE;
  tt->target  = *tbl;
  tt->pExprList = val;
  tt->pWhere = w;
  tt->orconf = orconf;

  return tt;
}

TriggerStep * 
sqliteTriggerDeleteStep(Token * tbl, Expr * w)
{
  TriggerStep * tt = sqliteMalloc(sizeof(TriggerStep));

  tt->op = TK_DELETE;
  tt->target  = *tbl;
  tt->pWhere = w;
  tt->orconf = OE_Default;

  return tt;
}


/* This does a recursive delete of the trigger structure */
void sqliteDeleteTrigger(Trigger * tt)
{
  TriggerStep * ts, * tc;
  ts = tt->step_list;

  while (ts) {
    tc = ts;
    ts = ts->pNext;

    sqliteExprDelete(tc->pWhere);
    sqliteExprListDelete(tc->pExprList);
    sqliteSelectDelete(tc->pSelect);
    sqliteIdListDelete(tc->pIdList);

    sqliteFree(tc);
  }

  sqliteFree(tt->name);
  sqliteFree(tt->table);
  sqliteExprDelete(tt->pWhen);
  sqliteIdListDelete(tt->pColumns);
  sqliteFree(tt->strings);
  sqliteFree(tt);
}

/*
 * "nested" is true if this is begin called as the result of a DROP TABLE
 */
void sqliteDropTrigger(Parse *pParse, Token * trigname, int nested)
{
  char * tmp_name;
  Trigger * trig;
  Table   * tbl;

  tmp_name = sqliteStrNDup(trigname->z, trigname->n);

  /* ensure that the trigger being dropped exists */
  trig = sqliteHashFind(&(pParse->db->trigHash), tmp_name, trigname->n + 1); 
  if (!trig) {
    sqliteSetNString(&pParse->zErrMsg, "no such trigger: ", -1,
        tmp_name, -1, 0);
    sqliteFree(tmp_name);
    return;
  }

  /*
   * If this is not an "explain", do the following:
   * 1. Remove the trigger from its associated table structure
   * 2. Move the trigger from the trigHash hash to trigDrop
   */
  if (!pParse->explain) {
    /* 1 */
    tbl = sqliteFindTable(pParse->db, trig->table);
    assert(tbl);
    if (tbl->pTrigger == trig) 
      tbl->pTrigger = trig->pNext;
    else {
      Trigger * cc = tbl->pTrigger;
      while (cc) {
        if (cc->pNext == trig) {
          cc->pNext = cc->pNext->pNext;
          break;
        }
        cc = cc->pNext;
      }
      assert(cc);
    }

    /* 2 */
    sqliteHashInsert(&(pParse->db->trigHash), tmp_name, 
        trigname->n + 1, NULL);
    sqliteHashInsert(&(pParse->db->trigDrop), trig->name, 
        trigname->n + 1, trig);
  }

  /* Unless this is a trigger on a TEMP TABLE, generate code to destroy the
   * database record of the trigger */
  if (!tbl->isTemp) {
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
    sqliteVdbeChangeP3(pParse->pVdbe, base+2, tmp_name, 0);
    if( !nested ){
      sqliteChangeCookie(pParse->db);
    }
    sqliteVdbeChangeP1(pParse->pVdbe, base+9, pParse->db->next_cookie);
    if( !nested ){
      sqliteEndWriteOperation(pParse);
    }
  }

  sqliteFree(tmp_name);
}

static int checkColumnOverLap(IdList * ii, ExprList * ee)
{
  int i, e;
  if (!ii) return 1;
  if (!ee) return 1;

  for (i = 0; i < ii->nId; i++) 
    for (e = 0; e < ee->nExpr; e++) 
      if (!sqliteStrICmp(ii->a[i].zName, ee->a[e].zName))
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
    Parse * pParse, 
    Trigger * pTrigger,
    int op,                 /* one of TK_DELETE, TK_INSERT, TK_UPDATE */
    int tr_tm,              /* one of TK_BEFORE, TK_AFTER */
    int foreach,            /* one of TK_ROW or TK_STATEMENT */
    ExprList * pChanges)
{
  Trigger * tt;

  if (always_code_trigger_setup) return 1;

  tt = pTrigger;
  while (tt) {
    if (tt->op == op && tt->tr_tm == tr_tm && tt->foreach == foreach &&
        checkColumnOverLap(tt->pColumns, pChanges)) {
      TriggerStack * ss;
      ss = pParse->trigStack;
      while (ss && ss->pTrigger != pTrigger) ss = ss->pNext;
      if (!ss) return 1;
    }
    tt = tt->pNext;
  }

  return 0;
}

static int codeTriggerProgram(
        Parse *pParse,
        TriggerStep * program,
        int onError)
{
    TriggerStep * step = program;
    int orconf;

    while (step) {
        int saveNTab = pParse->nTab;
        orconf = (onError == OE_Default)?step->orconf:onError;
        pParse->trigStack->orconf = orconf;
        switch(step->op) {
            case TK_SELECT: {
                int tmp_tbl = pParse->nTab++;
                sqliteVdbeAddOp(pParse->pVdbe, OP_OpenTemp, tmp_tbl, 0);
                sqliteVdbeAddOp(pParse->pVdbe, OP_KeyAsData, tmp_tbl, 1);
                sqliteSelect(pParse, step->pSelect, 
                        SRT_Union, tmp_tbl, 0, 0, 0);
                sqliteVdbeAddOp(pParse->pVdbe, OP_Close, tmp_tbl, 0);
                pParse->nTab--;
                break;
                            }
            case TK_UPDATE: {
                sqliteVdbeAddOp(pParse->pVdbe, OP_PushList, 0, 0);
                sqliteUpdate(pParse, &step->target, 
                        sqliteExprListDup(step->pExprList), 
                        sqliteExprDup(step->pWhere), orconf);
                sqliteVdbeAddOp(pParse->pVdbe, OP_PopList, 0, 0);
                break;
                            }
            case TK_INSERT: {
                sqliteInsert(pParse, &step->target, 
                        sqliteExprListDup(step->pExprList), 
                        sqliteSelectDup(step->pSelect), 
                        sqliteIdListDup(step->pIdList), orconf);
                break;
                            }
            case TK_DELETE: {
                sqliteVdbeAddOp(pParse->pVdbe, OP_PushList, 0, 0);
                sqliteDeleteFrom(pParse, &step->target, 
                        sqliteExprDup(step->pWhere)
                        );
                sqliteVdbeAddOp(pParse->pVdbe, OP_PopList, 0, 0);
                break;
                            }
            default:
                            assert(0);
        } 
        pParse->nTab = saveNTab;
        step = step->pNext;
    }

    return 0;
}

int sqliteCodeRowTrigger(
        Parse * pParse,  /* Parse context */
        int op,          /* One of TK_UPDATE, TK_INSERT, TK_DELETE */
        ExprList * changes, /* Changes list for any UPDATE OF triggers */
        int tr_tm,       /* One of TK_BEFORE, TK_AFTER */
        Table * tbl,     /* The table to code triggers from */
        int newTable,    /* The indice of the "new" row to access */
        int oldTable,    /* The indice of the "old" row to access */
        int onError)     /* ON CONFLICT policy */
{
  Trigger * pTrigger;
  TriggerStack * pTriggerStack;


  assert(op == TK_UPDATE || op == TK_INSERT || op == TK_DELETE);
  assert(tr_tm == TK_BEFORE || tr_tm == TK_AFTER);

  assert(newTable != -1 || oldTable != -1);

  pTrigger = tbl->pTrigger;
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
          !checkColumnOverLap(pTrigger->pColumns, changes))
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
      pTriggerStack->newIdx = newTable;
      pTriggerStack->oldIdx = oldTable;
      pTriggerStack->pTab = tbl;
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

      codeTriggerProgram(pParse, pTrigger->step_list, onError); 

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
 * Handle UPDATE and DELETE triggers on views
 */
void sqliteViewTriggers(Parse *pParse, Table *pTab, 
    Expr * pWhere, int onError, ExprList * pChanges)
{
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
  if (pChanges) {
    newIdx = pParse->nTab++;
    sqliteVdbeAddOp(v, OP_OpenTemp, newIdx, 0);
  }

  /* Snapshot the view */
  if (sqliteSelect(pParse, &theSelect, SRT_Table, oldIdx, 0, 0, 0)) {
    goto trigger_cleanup;
  }

  /* loop thru the view snapshot, executing triggers for each row */
  endOfLoop = sqliteVdbeMakeLabel(v);
  sqliteVdbeAddOp(v, OP_Rewind, oldIdx, endOfLoop);

  /* Loop thru the view snapshot, executing triggers for each row */
  startOfLoop = sqliteVdbeCurrentAddr(v);

  /* Build the updated row if required */
  if (pChanges) {
    int ii, jj;

    aXRef = sqliteMalloc( sizeof(int) * pTab->nCol );
    if( aXRef==0 ) goto trigger_cleanup;
    for (ii = 0; ii < pTab->nCol; ii++)
      aXRef[ii] = -1;

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

    for (ii = 0; ii < pTab->nCol; ii++)
      if( aXRef[ii] < 0 ) 
        sqliteVdbeAddOp(v, OP_Column, oldIdx, ii);
      else
        sqliteExprCode(pParse, pChanges->a[aXRef[ii]].pExpr);

    sqliteVdbeAddOp(v, OP_MakeRecord, pTab->nCol, 0);
    sqliteVdbeAddOp(v, OP_PutIntKey, newIdx, 0);
    sqliteVdbeAddOp(v, OP_Rewind, newIdx, 0);

    sqliteCodeRowTrigger(pParse, TK_UPDATE, pChanges, TK_BEFORE, 
        pTab, newIdx, oldIdx, onError);
    sqliteCodeRowTrigger(pParse, TK_UPDATE, pChanges, TK_AFTER, 
        pTab, newIdx, oldIdx, onError);
  } else {
    sqliteCodeRowTrigger(pParse, TK_DELETE, 0, TK_BEFORE, pTab, -1, oldIdx, 
        onError);
    sqliteCodeRowTrigger(pParse, TK_DELETE, 0, TK_AFTER, pTab, -1, oldIdx, 
        onError);
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
