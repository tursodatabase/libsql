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
** Code for testing the btree.c module in SQLite.  This code
** is not included in the SQLite library.  It is used for automated
** testing of the SQLite library.
**
** $Id: test3.c,v 1.43 2004/06/04 06:22:02 danielk1977 Exp $
*/
#include "sqliteInt.h"
#include "pager.h"
#include "btree.h"
#include "tcl.h"
#include <stdlib.h>
#include <string.h>

/*
** Interpret an SQLite error number
*/
static char *errorName(int rc){
  char *zName;
  switch( rc ){
    case SQLITE_OK:         zName = "SQLITE_OK";          break;
    case SQLITE_ERROR:      zName = "SQLITE_ERROR";       break;
    case SQLITE_INTERNAL:   zName = "SQLITE_INTERNAL";    break;
    case SQLITE_PERM:       zName = "SQLITE_PERM";        break;
    case SQLITE_ABORT:      zName = "SQLITE_ABORT";       break;
    case SQLITE_BUSY:       zName = "SQLITE_BUSY";        break;
    case SQLITE_NOMEM:      zName = "SQLITE_NOMEM";       break;
    case SQLITE_READONLY:   zName = "SQLITE_READONLY";    break;
    case SQLITE_INTERRUPT:  zName = "SQLITE_INTERRUPT";   break;
    case SQLITE_IOERR:      zName = "SQLITE_IOERR";       break;
    case SQLITE_CORRUPT:    zName = "SQLITE_CORRUPT";     break;
    case SQLITE_NOTFOUND:   zName = "SQLITE_NOTFOUND";    break;
    case SQLITE_FULL:       zName = "SQLITE_FULL";        break;
    case SQLITE_CANTOPEN:   zName = "SQLITE_CANTOPEN";    break;
    case SQLITE_PROTOCOL:   zName = "SQLITE_PROTOCOL";    break;
    case SQLITE_EMPTY:      zName = "SQLITE_EMPTY";       break;
    default:                zName = "SQLITE_Unknown";     break;
  }
  return zName;
}

/*
** Usage:   btree_open FILENAME NCACHE FLAGS
**
** Open a new database
*/
static int btree_open(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  const char **argv      /* Text of each argument */
){
  Btree *pBt;
  int rc, nCache, flags;
  char zBuf[100];
  if( argc!=4 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " FILENAME NCACHE FLAGS\"", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[2], &nCache) ) return TCL_ERROR;
  if( Tcl_GetInt(interp, argv[3], &flags) ) return TCL_ERROR;
  rc = sqlite3BtreeOpen(argv[1], &pBt, nCache, flags, 0);
  if( rc!=SQLITE_OK ){
    Tcl_AppendResult(interp, errorName(rc), 0);
    return TCL_ERROR;
  }
  sprintf(zBuf,"%p", pBt);
  if( strncmp(zBuf,"0x",2) ){
    sprintf(zBuf, "0x%p", pBt);
  }
  Tcl_AppendResult(interp, zBuf, 0);
  return TCL_OK;
}

/*
** Usage:   btree_close ID
**
** Close the given database.
*/
static int btree_close(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  const char **argv      /* Text of each argument */
){
  Btree *pBt;
  int rc;
  if( argc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " ID\"", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], (int*)&pBt) ) return TCL_ERROR;
  rc = sqlite3BtreeClose(pBt);
  if( rc!=SQLITE_OK ){
    Tcl_AppendResult(interp, errorName(rc), 0);
    return TCL_ERROR;
  }
  return TCL_OK;
}

/*
** Usage:   btree_begin_transaction ID
**
** Start a new transaction
*/
static int btree_begin_transaction(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  const char **argv      /* Text of each argument */
){
  Btree *pBt;
  int rc;
  if( argc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " ID\"", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], (int*)&pBt) ) return TCL_ERROR;
  rc = sqlite3BtreeBeginTrans(pBt, 1, 0);
  if( rc!=SQLITE_OK ){
    Tcl_AppendResult(interp, errorName(rc), 0);
    return TCL_ERROR;
  }
  return TCL_OK;
}

/*
** Usage:   btree_rollback ID
**
** Rollback changes
*/
static int btree_rollback(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  const char **argv      /* Text of each argument */
){
  Btree *pBt;
  int rc;
  if( argc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " ID\"", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], (int*)&pBt) ) return TCL_ERROR;
  rc = sqlite3BtreeRollback(pBt);
  if( rc!=SQLITE_OK ){
    Tcl_AppendResult(interp, errorName(rc), 0);
    return TCL_ERROR;
  }
  return TCL_OK;
}

/*
** Usage:   btree_commit ID
**
** Commit all changes
*/
static int btree_commit(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  const char **argv      /* Text of each argument */
){
  Btree *pBt;
  int rc;
  if( argc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " ID\"", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], (int*)&pBt) ) return TCL_ERROR;
  rc = sqlite3BtreeCommit(pBt);
  if( rc!=SQLITE_OK ){
    Tcl_AppendResult(interp, errorName(rc), 0);
    return TCL_ERROR;
  }
  return TCL_OK;
}

/*
** Usage:   btree_begin_statement ID
**
** Start a new statement transaction
*/
static int btree_begin_statement(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  const char **argv      /* Text of each argument */
){
  Btree *pBt;
  int rc;
  if( argc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " ID\"", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], (int*)&pBt) ) return TCL_ERROR;
  rc = sqlite3BtreeBeginStmt(pBt);
  if( rc!=SQLITE_OK ){
    Tcl_AppendResult(interp, errorName(rc), 0);
    return TCL_ERROR;
  }
  return TCL_OK;
}

/*
** Usage:   btree_rollback_statement ID
**
** Rollback changes
*/
static int btree_rollback_statement(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  const char **argv      /* Text of each argument */
){
  Btree *pBt;
  int rc;
  if( argc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " ID\"", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], (int*)&pBt) ) return TCL_ERROR;
  rc = sqlite3BtreeRollbackStmt(pBt);
  if( rc!=SQLITE_OK ){
    Tcl_AppendResult(interp, errorName(rc), 0);
    return TCL_ERROR;
  }
  return TCL_OK;
}

/*
** Usage:   btree_commit_statement ID
**
** Commit all changes
*/
static int btree_commit_statement(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  const char **argv      /* Text of each argument */
){
  Btree *pBt;
  int rc;
  if( argc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " ID\"", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], (int*)&pBt) ) return TCL_ERROR;
  rc = sqlite3BtreeCommitStmt(pBt);
  if( rc!=SQLITE_OK ){
    Tcl_AppendResult(interp, errorName(rc), 0);
    return TCL_ERROR;
  }
  return TCL_OK;
}

/*
** Usage:   btree_create_table ID FLAGS
**
** Create a new table in the database
*/
static int btree_create_table(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  const char **argv      /* Text of each argument */
){
  Btree *pBt;
  int rc, iTable, flags;
  char zBuf[30];
  if( argc!=3 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " ID FLAGS\"", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], (int*)&pBt) ) return TCL_ERROR;
  if( Tcl_GetInt(interp, argv[2], &flags) ) return TCL_ERROR;
  rc = sqlite3BtreeCreateTable(pBt, &iTable, flags);
  if( rc!=SQLITE_OK ){
    Tcl_AppendResult(interp, errorName(rc), 0);
    return TCL_ERROR;
  }
  sprintf(zBuf, "%d", iTable);
  Tcl_AppendResult(interp, zBuf, 0);
  return TCL_OK;
}

/*
** Usage:   btree_drop_table ID TABLENUM
**
** Delete an entire table from the database
*/
static int btree_drop_table(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  const char **argv      /* Text of each argument */
){
  Btree *pBt;
  int iTable;
  int rc;
  if( argc!=3 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " ID TABLENUM\"", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], (int*)&pBt) ) return TCL_ERROR;
  if( Tcl_GetInt(interp, argv[2], &iTable) ) return TCL_ERROR;
  rc = sqlite3BtreeDropTable(pBt, iTable);
  if( rc!=SQLITE_OK ){
    Tcl_AppendResult(interp, errorName(rc), 0);
    return TCL_ERROR;
  }
  return TCL_OK;
}

/*
** Usage:   btree_clear_table ID TABLENUM
**
** Remove all entries from the given table but keep the table around.
*/
static int btree_clear_table(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  const char **argv      /* Text of each argument */
){
  Btree *pBt;
  int iTable;
  int rc;
  if( argc!=3 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " ID TABLENUM\"", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], (int*)&pBt) ) return TCL_ERROR;
  if( Tcl_GetInt(interp, argv[2], &iTable) ) return TCL_ERROR;
  rc = sqlite3BtreeClearTable(pBt, iTable);
  if( rc!=SQLITE_OK ){
    Tcl_AppendResult(interp, errorName(rc), 0);
    return TCL_ERROR;
  }
  return TCL_OK;
}

#define SQLITE_N_BTREE_META 16

/*
** Usage:   btree_get_meta ID
**
** Return meta data
*/
static int btree_get_meta(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  const char **argv      /* Text of each argument */
){
  Btree *pBt;
  int rc;
  int i;
  if( argc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " ID\"", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], (int*)&pBt) ) return TCL_ERROR;
  for(i=0; i<SQLITE_N_BTREE_META; i++){
    char zBuf[30];
    unsigned int v;
    rc = sqlite3BtreeGetMeta(pBt, i, &v);
    if( rc!=SQLITE_OK ){
      Tcl_AppendResult(interp, errorName(rc), 0);
      return TCL_ERROR;
    }
    sprintf(zBuf,"%d",v);
    Tcl_AppendElement(interp, zBuf);
  }
  return TCL_OK;
}

/*
** Usage:   btree_update_meta ID METADATA...
**
** Return meta data
*/
static int btree_update_meta(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  const char **argv      /* Text of each argument */
){
  Btree *pBt;
  int rc;
  int i;
  int aMeta[SQLITE_N_BTREE_META];

  if( argc!=1+SQLITE_N_BTREE_META ){
    char zBuf[30];
    sprintf(zBuf,"%d",SQLITE_N_BTREE_META);
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " ID METADATA...\" (METADATA is ", zBuf, " integers)", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], (int*)&pBt) ) return TCL_ERROR;
  for(i=1; i<SQLITE_N_BTREE_META; i++){
    if( Tcl_GetInt(interp, argv[i+1], &aMeta[i]) ) return TCL_ERROR;
  }
  for(i=1; i<SQLITE_N_BTREE_META; i++){
    rc = sqlite3BtreeUpdateMeta(pBt, i, aMeta[i]);
    if( rc!=SQLITE_OK ){
      Tcl_AppendResult(interp, errorName(rc), 0);
      return TCL_ERROR;
    }
  }
  return TCL_OK;
}

/*
** Usage:   btree_page_dump ID PAGENUM
**
** Print a disassembly of a page on standard output
*/
static int btree_page_dump(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  const char **argv      /* Text of each argument */
){
  Btree *pBt;
  int iPage;
  int rc;

  if( argc!=3 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " ID\"", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], (int*)&pBt) ) return TCL_ERROR;
  if( Tcl_GetInt(interp, argv[2], &iPage) ) return TCL_ERROR;
  rc = sqlite3BtreePageDump(pBt, iPage, 0);
  if( rc!=SQLITE_OK ){
    Tcl_AppendResult(interp, errorName(rc), 0);
    return TCL_ERROR;
  }
  return TCL_OK;
}

/*
** Usage:   btree_tree_dump ID PAGENUM
**
** Print a disassembly of a page and all its child pages on standard output
*/
static int btree_tree_dump(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  const char **argv      /* Text of each argument */
){
  Btree *pBt;
  int iPage;
  int rc;

  if( argc!=3 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " ID\"", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], (int*)&pBt) ) return TCL_ERROR;
  if( Tcl_GetInt(interp, argv[2], &iPage) ) return TCL_ERROR;
  rc = sqlite3BtreePageDump(pBt, iPage, 1);
  if( rc!=SQLITE_OK ){
    Tcl_AppendResult(interp, errorName(rc), 0);
    return TCL_ERROR;
  }
  return TCL_OK;
}

/*
** Usage:   btree_pager_stats ID
**
** Returns pager statistics
*/
static int btree_pager_stats(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  const char **argv      /* Text of each argument */
){
  Btree *pBt;
  int i;
  int *a;

  if( argc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " ID\"", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], (int*)&pBt) ) return TCL_ERROR;
  a = sqlite3pager_stats(sqlite3BtreePager(pBt));
  for(i=0; i<9; i++){
    static char *zName[] = {
      "ref", "page", "max", "size", "state", "err",
      "hit", "miss", "ovfl",
    };
    char zBuf[100];
    Tcl_AppendElement(interp, zName[i]);
    sprintf(zBuf,"%d",a[i]);
    Tcl_AppendElement(interp, zBuf);
  }
  return TCL_OK;
}

/*
** Usage:   btree_pager_ref_dump ID
**
** Print out all outstanding pages.
*/
static int btree_pager_ref_dump(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  const char **argv      /* Text of each argument */
){
  Btree *pBt;

  if( argc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " ID\"", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], (int*)&pBt) ) return TCL_ERROR;
  sqlite3pager_refdump(sqlite3BtreePager(pBt));
  return TCL_OK;
}

/*
** Usage:   btree_integrity_check ID ROOT ...
**
** Look through every page of the given BTree file to verify correct
** formatting and linkage.  Return a line of text for each problem found.
** Return an empty string if everything worked.
*/
static int btree_integrity_check(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  const char **argv      /* Text of each argument */
){
  Btree *pBt;
  char *zResult;
  int nRoot;
  int *aRoot;
  int i;

  if( argc<3 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " ID ROOT ...\"", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], (int*)&pBt) ) return TCL_ERROR;
  nRoot = argc-2;
  aRoot = malloc( sizeof(int)*(argc-2) );
  for(i=0; i<argc-2; i++){
    if( Tcl_GetInt(interp, argv[i+2], &aRoot[i]) ) return TCL_ERROR;
  }
  zResult = sqlite3BtreeIntegrityCheck(pBt, aRoot, nRoot);
  if( zResult ){
    Tcl_AppendResult(interp, zResult, 0);
    sqliteFree(zResult); 
  }
  return TCL_OK;
}

/*
** Usage:   btree_cursor_list ID
**
** Print information about all cursors to standard output for debugging.
*/
static int btree_cursor_list(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  const char **argv      /* Text of each argument */
){
  Btree *pBt;

  if( argc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " ID\"", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], (int*)&pBt) ) return TCL_ERROR;
  sqlite3BtreeCursorList(pBt);
  return SQLITE_OK;
}

/*
** Usage:   btree_cursor ID TABLENUM WRITEABLE
**
** Create a new cursor.  Return the ID for the cursor.
*/
static int btree_cursor(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  const char **argv      /* Text of each argument */
){
  Btree *pBt;
  int iTable;
  BtCursor *pCur;
  int rc;
  int wrFlag;
  char zBuf[30];

  if( argc!=4 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " ID TABLENUM WRITEABLE\"", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], (int*)&pBt) ) return TCL_ERROR;
  if( Tcl_GetInt(interp, argv[2], &iTable) ) return TCL_ERROR;
  if( Tcl_GetBoolean(interp, argv[3], &wrFlag) ) return TCL_ERROR;
  rc = sqlite3BtreeCursor(pBt, iTable, wrFlag, 0, 0, &pCur);
  if( rc ){
    Tcl_AppendResult(interp, errorName(rc), 0);
    return TCL_ERROR;
  }
  sprintf(zBuf,"0x%x", (int)pCur);
  Tcl_AppendResult(interp, zBuf, 0);
  return SQLITE_OK;
}

/*
** Usage:   btree_close_cursor ID
**
** Close a cursor opened using btree_cursor.
*/
static int btree_close_cursor(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  const char **argv      /* Text of each argument */
){
  BtCursor *pCur;
  int rc;

  if( argc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " ID\"", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], (int*)&pCur) ) return TCL_ERROR;
  rc = sqlite3BtreeCloseCursor(pCur);
  if( rc ){
    Tcl_AppendResult(interp, errorName(rc), 0);
    return TCL_ERROR;
  }
  return SQLITE_OK;
}

/*
** Usage:   btree_move_to ID KEY
**
** Move the cursor to the entry with the given key.
*/
static int btree_move_to(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  const char **argv      /* Text of each argument */
){
  BtCursor *pCur;
  int rc;
  int res;
  char zBuf[20];

  if( argc!=3 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " ID KEY\"", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], (int*)&pCur) ) return TCL_ERROR;
  if( sqlite3BtreeFlags(pCur) & BTREE_INTKEY ){
    int iKey;
    if( Tcl_GetInt(interp, argv[2], &iKey) ) return TCL_ERROR;
    rc = sqlite3BtreeMoveto(pCur, 0, iKey, &res);
  }else{
    rc = sqlite3BtreeMoveto(pCur, argv[2], strlen(argv[2]), &res);  
  }
  if( rc ){
    Tcl_AppendResult(interp, errorName(rc), 0);
    return TCL_ERROR;
  }
  if( res<0 ) res = -1;
  if( res>0 ) res = 1;
  sprintf(zBuf,"%d",res);
  Tcl_AppendResult(interp, zBuf, 0);
  return SQLITE_OK;
}

/*
** Usage:   btree_delete ID
**
** Delete the entry that the cursor is pointing to
*/
static int btree_delete(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  const char **argv      /* Text of each argument */
){
  BtCursor *pCur;
  int rc;

  if( argc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " ID\"", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], (int*)&pCur) ) return TCL_ERROR;
  rc = sqlite3BtreeDelete(pCur);
  if( rc ){
    Tcl_AppendResult(interp, errorName(rc), 0);
    return TCL_ERROR;
  }
  return SQLITE_OK;
}

/*
** Usage:   btree_insert ID KEY DATA
**
** Create a new entry with the given key and data.  If an entry already
** exists with the same key the old entry is overwritten.
*/
static int btree_insert(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  const char **argv      /* Text of each argument */
){
  BtCursor *pCur;
  int rc;

  if( argc!=4 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " ID KEY DATA\"", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], (int*)&pCur) ) return TCL_ERROR;
  if( sqlite3BtreeFlags(pCur) & BTREE_INTKEY ){
/*
    int iKey;
    if( Tcl_GetInt(interp, argv[2], &iKey) ) return TCL_ERROR;
*/
    i64 iKey;
    Tcl_Obj *obj = Tcl_NewStringObj(argv[2], -1);
    Tcl_IncrRefCount(obj);
    if( Tcl_GetWideIntFromObj(interp, obj, &iKey) ) return TCL_ERROR;
    Tcl_DecrRefCount(obj);

    rc = sqlite3BtreeInsert(pCur, 0, iKey, argv[3], strlen(argv[3]));
  }else{
    rc = sqlite3BtreeInsert(pCur, argv[2], strlen(argv[2]),
                         argv[3], strlen(argv[3]));
  }
  if( rc ){
    Tcl_AppendResult(interp, errorName(rc), 0);
    return TCL_ERROR;
  }
  return SQLITE_OK;
}

/*
** Usage:   btree_next ID
**
** Move the cursor to the next entry in the table.  Return 0 on success
** or 1 if the cursor was already on the last entry in the table or if
** the table is empty.
*/
static int btree_next(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  const char **argv      /* Text of each argument */
){
  BtCursor *pCur;
  int rc;
  int res = 0;
  char zBuf[100];

  if( argc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " ID\"", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], (int*)&pCur) ) return TCL_ERROR;
  rc = sqlite3BtreeNext(pCur, &res);
  if( rc ){
    Tcl_AppendResult(interp, errorName(rc), 0);
    return TCL_ERROR;
  }
  sprintf(zBuf,"%d",res);
  Tcl_AppendResult(interp, zBuf, 0);
  return SQLITE_OK;
}

/*
** Usage:   btree_prev ID
**
** Move the cursor to the previous entry in the table.  Return 0 on
** success and 1 if the cursor was already on the first entry in
** the table or if the table was empty.
*/
static int btree_prev(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  const char **argv      /* Text of each argument */
){
  BtCursor *pCur;
  int rc;
  int res = 0;
  char zBuf[100];

  if( argc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " ID\"", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], (int*)&pCur) ) return TCL_ERROR;
  rc = sqlite3BtreePrevious(pCur, &res);
  if( rc ){
    Tcl_AppendResult(interp, errorName(rc), 0);
    return TCL_ERROR;
  }
  sprintf(zBuf,"%d",res);
  Tcl_AppendResult(interp, zBuf, 0);
  return SQLITE_OK;
}

/*
** Usage:   btree_first ID
**
** Move the cursor to the first entry in the table.  Return 0 if the
** cursor was left point to something and 1 if the table is empty.
*/
static int btree_first(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  const char **argv      /* Text of each argument */
){
  BtCursor *pCur;
  int rc;
  int res = 0;
  char zBuf[100];

  if( argc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " ID\"", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], (int*)&pCur) ) return TCL_ERROR;
  rc = sqlite3BtreeFirst(pCur, &res);
  if( rc ){
    Tcl_AppendResult(interp, errorName(rc), 0);
    return TCL_ERROR;
  }
  sprintf(zBuf,"%d",res);
  Tcl_AppendResult(interp, zBuf, 0);
  return SQLITE_OK;
}

/*
** Usage:   btree_last ID
**
** Move the cursor to the last entry in the table.  Return 0 if the
** cursor was left point to something and 1 if the table is empty.
*/
static int btree_last(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  const char **argv      /* Text of each argument */
){
  BtCursor *pCur;
  int rc;
  int res = 0;
  char zBuf[100];

  if( argc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " ID\"", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], (int*)&pCur) ) return TCL_ERROR;
  rc = sqlite3BtreeLast(pCur, &res);
  if( rc ){
    Tcl_AppendResult(interp, errorName(rc), 0);
    return TCL_ERROR;
  }
  sprintf(zBuf,"%d",res);
  Tcl_AppendResult(interp, zBuf, 0);
  return SQLITE_OK;
}

/*
** Usage:   btree_eof ID
**
** Return TRUE if the given cursor is not pointing at a valid entry.
** Return FALSE if the cursor does point to a valid entry.
*/
static int btree_eof(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  const char **argv      /* Text of each argument */
){
  BtCursor *pCur;
  char zBuf[50];

  if( argc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " ID\"", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], (int*)&pCur) ) return TCL_ERROR;
  sprintf(zBuf, "%d", sqlite3BtreeEof(pCur));
  Tcl_AppendResult(interp, zBuf, 0);
  return SQLITE_OK;
}

/*
** Usage:   btree_keysize ID
**
** Return the number of bytes of key.  For an INTKEY table, this
** returns the key itself.
*/
static int btree_keysize(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  const char **argv      /* Text of each argument */
){
  BtCursor *pCur;
  u64 n;
  char zBuf[50];

  if( argc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " ID\"", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], (int*)&pCur) ) return TCL_ERROR;
  sqlite3BtreeKeySize(pCur, &n);
  sprintf(zBuf, "%llu", n);
  Tcl_AppendResult(interp, zBuf, 0);
  return SQLITE_OK;
}

/*
** Usage:   btree_key ID
**
** Return the key for the entry at which the cursor is pointing.
*/
static int btree_key(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  const char **argv      /* Text of each argument */
){
  BtCursor *pCur;
  int rc;
  u64 n;
  char *zBuf;

  if( argc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " ID\"", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], (int*)&pCur) ) return TCL_ERROR;
  sqlite3BtreeKeySize(pCur, &n);
  if( sqlite3BtreeFlags(pCur) & BTREE_INTKEY ){
    char zBuf2[60];
    sprintf(zBuf2, "%llu", n);
    Tcl_AppendResult(interp, zBuf2, 0);
  }else{
    zBuf = malloc( n+1 );
    rc = sqlite3BtreeKey(pCur, 0, n, zBuf);
    if( rc ){
      Tcl_AppendResult(interp, errorName(rc), 0);
      return TCL_ERROR;
    }
    zBuf[n] = 0;
    Tcl_AppendResult(interp, zBuf, 0);
    free(zBuf);
  }
  return SQLITE_OK;
}

/*
** Usage:   btree_data ID
**
** Return the data for the entry at which the cursor is pointing.
*/
static int btree_data(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  const char **argv      /* Text of each argument */
){
  BtCursor *pCur;
  int rc;
  u32 n;
  char *zBuf;

  if( argc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " ID\"", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], (int*)&pCur) ) return TCL_ERROR;
  sqlite3BtreeDataSize(pCur, &n);
  zBuf = malloc( n+1 );
  rc = sqlite3BtreeData(pCur, 0, n, zBuf);
  if( rc ){
    Tcl_AppendResult(interp, errorName(rc), 0);
    return TCL_ERROR;
  }
  zBuf[n] = 0;
  Tcl_AppendResult(interp, zBuf, 0);
  free(zBuf);
  return SQLITE_OK;
}

/*
** Usage:   btree_fetch_key ID AMT
**
** Use the sqlite3BtreeKeyFetch() routine to get AMT bytes of the key.
** If sqlite3BtreeKeyFetch() fails, return an empty string.
*/
static int btree_fetch_key(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  const char **argv      /* Text of each argument */
){
  BtCursor *pCur;
  int n;
  int amt;
  u64 nKey;
  const char *zBuf;
  char zStatic[1000];

  if( argc!=3 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " ID AMT\"", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], (int*)&pCur) ) return TCL_ERROR;
  if( Tcl_GetInt(interp, argv[2], &n) ) return TCL_ERROR;
  sqlite3BtreeKeySize(pCur, &nKey);
  zBuf = sqlite3BtreeKeyFetch(pCur, &amt);
  if( zBuf && amt>=n ){
    assert( nKey<sizeof(zStatic) );
    if( n>0 ) nKey = n;
    memcpy(zStatic, zBuf, (int)nKey); 
    zStatic[nKey] = 0;
    Tcl_AppendResult(interp, zStatic, 0);
  }
  return TCL_OK;
}

/*
** Usage:   btree_fetch_data ID AMT
**
** Use the sqlite3BtreeDataFetch() routine to get AMT bytes of the key.
** If sqlite3BtreeDataFetch() fails, return an empty string.
*/
static int btree_fetch_data(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  const char **argv      /* Text of each argument */
){
  BtCursor *pCur;
  int n;
  int amt;
  u32 nData;
  const char *zBuf;
  char zStatic[1000];

  if( argc!=3 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " ID AMT\"", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], (int*)&pCur) ) return TCL_ERROR;
  if( Tcl_GetInt(interp, argv[2], &n) ) return TCL_ERROR;
  sqlite3BtreeDataSize(pCur, &nData);
  zBuf = sqlite3BtreeDataFetch(pCur, &amt);
  if( zBuf && amt>=n ){
    assert( nData<sizeof(zStatic) );
    if( n>0 ) nData = n;
    memcpy(zStatic, zBuf, (int)nData); 
    zStatic[nData] = 0;
    Tcl_AppendResult(interp, zStatic, 0);
  }
  return TCL_OK;
}

/*
** Usage:   btree_payload_size ID
**
** Return the number of bytes of payload
*/
static int btree_payload_size(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  const char **argv      /* Text of each argument */
){
  BtCursor *pCur;
  int n2;
  u64 n1;
  char zBuf[50];

  if( argc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " ID\"", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], (int*)&pCur) ) return TCL_ERROR;
  if( sqlite3BtreeFlags(pCur) & BTREE_INTKEY ){
    n1 = 0;
  }else{
    sqlite3BtreeKeySize(pCur, &n1);
  }
  sqlite3BtreeDataSize(pCur, &n2);
  sprintf(zBuf, "%d", (int)(n1+n2));
  Tcl_AppendResult(interp, zBuf, 0);
  return SQLITE_OK;
}

/*
** Usage:   btree_cursor_info ID
**
** Return eight integers containing information about the entry the
** cursor is pointing to:
**
**   aResult[0] =  The page number
**   aResult[1] =  The entry number
**   aResult[2] =  Total number of entries on this page
**   aResult[3] =  Size of this entry
**   aResult[4] =  Number of free bytes on this page
**   aResult[5] =  Number of free blocks on the page
**   aResult[6] =  Page number of the left child of this entry
**   aResult[7] =  Page number of the right child for the whole page
*/
static int btree_cursor_info(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  const char **argv      /* Text of each argument */
){
  BtCursor *pCur;
  int rc;
  int i, j;
  int aResult[8];
  char zBuf[400];

  if( argc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " ID\"", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], (int*)&pCur) ) return TCL_ERROR;
  rc = sqlite3BtreeCursorInfo(pCur, aResult);
  if( rc ){
    Tcl_AppendResult(interp, errorName(rc), 0);
    return TCL_ERROR;
  }
  j = 0;
  for(i=0; i<sizeof(aResult)/sizeof(aResult[0]); i++){
    sprintf(&zBuf[j]," %d", aResult[i]);
    j += strlen(&zBuf[j]);
  }
  Tcl_AppendResult(interp, &zBuf[1], 0);
  return SQLITE_OK;
}

/*
** The command is provided for the purpose of setting breakpoints.
** in regression test scripts.
**
** By setting a GDB breakpoint on this procedure and executing the
** btree_breakpoint command in a test script, we can stop GDB at
** the point in the script where the btree_breakpoint command is
** inserted.  This is useful for debugging.
*/
static int btree_breakpoint(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  const char **argv      /* Text of each argument */
){
  return TCL_OK;
}

/*
** usage:   varint_test  START  MULTIPLIER  COUNT  INCREMENT
**
** This command tests the sqlite3PutVarint() and sqlite3GetVarint()
** routines, both for accuracy and for speed.
**
** An integer is written using PutVarint() and read back with
** GetVarint() and varified to be unchanged.  This repeats COUNT
** times.  The first integer is START*MULTIPLIER.  Each iteration
** increases the integer by INCREMENT.
**
** This command returns nothing if it works.  It returns an error message
** if something goes wrong.
*/
static int btree_varint_test(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  const char **argv      /* Text of each argument */
){
  u32 start, mult, count, incr;
  u64 in, out;
  int n1, n2, i, j;
  unsigned char zBuf[100];
  if( argc!=5 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " START MULTIPLIER COUNT INCREMENT\"", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], (int*)&start) ) return TCL_ERROR;
  if( Tcl_GetInt(interp, argv[2], (int*)&mult) ) return TCL_ERROR;
  if( Tcl_GetInt(interp, argv[3], (int*)&count) ) return TCL_ERROR;
  if( Tcl_GetInt(interp, argv[4], (int*)&incr) ) return TCL_ERROR;
  in = start;
  in *= mult;
  for(i=0; i<count; i++){
    char zErr[200];
    n1 = sqlite3PutVarint(zBuf, in);
    if( n1>9 || n1<1 ){
      sprintf(zErr, "PutVarint returned %d - should be between 1 and 9", n1);
      Tcl_AppendResult(interp, zErr, 0);
      return TCL_ERROR;
    }
    n2 = sqlite3GetVarint(zBuf, &out);
    if( n1!=n2 ){
      sprintf(zErr, "PutVarint returned %d and GetVarint returned %d", n1, n2);
      Tcl_AppendResult(interp, zErr, 0);
      return TCL_ERROR;
    }
    if( in!=out ){
      sprintf(zErr, "Wrote 0x%016llx and got back 0x%016llx", in, out);
      Tcl_AppendResult(interp, zErr, 0);
      return TCL_ERROR;
    }

    /* In order to get realistic timings, run getVarint 19 more times.
    ** This is because getVarint is called about 20 times more often
    ** than putVarint.
    */
    for(j=0; j<19; j++){
      sqlite3GetVarint(zBuf, &out);
    }
    in += incr;
  }
  return TCL_OK;
}

/*
** Register commands with the TCL interpreter.
*/
int Sqlitetest3_Init(Tcl_Interp *interp){
  extern int sqlite3_btree_trace;
  static struct {
     char *zName;
     Tcl_CmdProc *xProc;
  } aCmd[] = {
     { "btree_open",               (Tcl_CmdProc*)btree_open               },
     { "btree_close",              (Tcl_CmdProc*)btree_close              },
     { "btree_begin_transaction",  (Tcl_CmdProc*)btree_begin_transaction  },
     { "btree_commit",             (Tcl_CmdProc*)btree_commit             },
     { "btree_rollback",           (Tcl_CmdProc*)btree_rollback           },
     { "btree_create_table",       (Tcl_CmdProc*)btree_create_table       },
     { "btree_drop_table",         (Tcl_CmdProc*)btree_drop_table         },
     { "btree_clear_table",        (Tcl_CmdProc*)btree_clear_table        },
     { "btree_get_meta",           (Tcl_CmdProc*)btree_get_meta           },
     { "btree_update_meta",        (Tcl_CmdProc*)btree_update_meta        },
     { "btree_page_dump",          (Tcl_CmdProc*)btree_page_dump          },
     { "btree_tree_dump",          (Tcl_CmdProc*)btree_tree_dump          },
     { "btree_pager_stats",        (Tcl_CmdProc*)btree_pager_stats        },
     { "btree_pager_ref_dump",     (Tcl_CmdProc*)btree_pager_ref_dump     },
     { "btree_cursor",             (Tcl_CmdProc*)btree_cursor             },
     { "btree_close_cursor",       (Tcl_CmdProc*)btree_close_cursor       },
     { "btree_move_to",            (Tcl_CmdProc*)btree_move_to            },
     { "btree_delete",             (Tcl_CmdProc*)btree_delete             },
     { "btree_insert",             (Tcl_CmdProc*)btree_insert             },
     { "btree_next",               (Tcl_CmdProc*)btree_next               },
     { "btree_prev",               (Tcl_CmdProc*)btree_prev               },
     { "btree_eof",                (Tcl_CmdProc*)btree_eof                },
     { "btree_keysize",            (Tcl_CmdProc*)btree_keysize            },
     { "btree_key",                (Tcl_CmdProc*)btree_key                },
     { "btree_data",               (Tcl_CmdProc*)btree_data               },
     { "btree_fetch_key",          (Tcl_CmdProc*)btree_fetch_key          },
     { "btree_fetch_data",         (Tcl_CmdProc*)btree_fetch_data         },
     { "btree_payload_size",       (Tcl_CmdProc*)btree_payload_size       },
     { "btree_first",              (Tcl_CmdProc*)btree_first              },
     { "btree_last",               (Tcl_CmdProc*)btree_last               },
     { "btree_cursor_info",        (Tcl_CmdProc*)btree_cursor_info        },
     { "btree_cursor_list",        (Tcl_CmdProc*)btree_cursor_list        },
     { "btree_integrity_check",    (Tcl_CmdProc*)btree_integrity_check    },
     { "btree_breakpoint",         (Tcl_CmdProc*)btree_breakpoint         },
     { "btree_varint_test",        (Tcl_CmdProc*)btree_varint_test        },
     { "btree_begin_statement",    (Tcl_CmdProc*)btree_begin_statement    },
     { "btree_commit_statement",   (Tcl_CmdProc*)btree_commit_statement   },
     { "btree_rollback_statement", (Tcl_CmdProc*)btree_rollback_statement },
  };
  int i;

  for(i=0; i<sizeof(aCmd)/sizeof(aCmd[0]); i++){
    Tcl_CreateCommand(interp, aCmd[i].zName, aCmd[i].xProc, 0, 0);
  }
  Tcl_LinkVar(interp, "pager_refinfo_enable", (char*)&pager3_refinfo_enable,
     TCL_LINK_INT);
  Tcl_LinkVar(interp, "btree_trace", (char*)&sqlite3_btree_trace,
     TCL_LINK_INT);
  return TCL_OK;
}
