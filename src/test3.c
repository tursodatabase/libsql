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
** $Id: test3.c,v 1.15 2002/06/26 20:06:06 drh Exp $
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
    default:                zName = "SQLITE_Unknown";     break;
  }
  return zName;
}

/*
** Usage:   btree_open FILENAME
**
** Open a new database
*/
static int btree_open(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  char **argv            /* Text of each argument */
){
  Btree *pBt;
  int rc;
  char zBuf[100];
  if( argc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " FILENAME\"", 0);
    return TCL_ERROR;
  }
  rc = sqliteBtreeOpen(argv[1], 0666, 10, &pBt);
  if( rc!=SQLITE_OK ){
    Tcl_AppendResult(interp, errorName(rc), 0);
    return TCL_ERROR;
  }
  sprintf(zBuf,"%p", pBt);
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
  char **argv            /* Text of each argument */
){
  Btree *pBt;
  int rc;
  if( argc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " ID\"", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], (int*)&pBt) ) return TCL_ERROR;
  rc = sqliteBtreeClose(pBt);
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
  char **argv            /* Text of each argument */
){
  Btree *pBt;
  int rc;
  if( argc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " ID\"", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], (int*)&pBt) ) return TCL_ERROR;
  rc = sqliteBtreeBeginTrans(pBt);
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
  char **argv            /* Text of each argument */
){
  Btree *pBt;
  int rc;
  if( argc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " ID\"", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], (int*)&pBt) ) return TCL_ERROR;
  rc = sqliteBtreeRollback(pBt);
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
  char **argv            /* Text of each argument */
){
  Btree *pBt;
  int rc;
  if( argc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " ID\"", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], (int*)&pBt) ) return TCL_ERROR;
  rc = sqliteBtreeCommit(pBt);
  if( rc!=SQLITE_OK ){
    Tcl_AppendResult(interp, errorName(rc), 0);
    return TCL_ERROR;
  }
  return TCL_OK;
}

/*
** Usage:   btree_create_table ID
**
** Create a new table in the database
*/
static int btree_create_table(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  char **argv            /* Text of each argument */
){
  Btree *pBt;
  int rc, iTable;
  char zBuf[30];
  if( argc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " ID\"", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], (int*)&pBt) ) return TCL_ERROR;
  rc = sqliteBtreeCreateTable(pBt, &iTable);
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
  char **argv            /* Text of each argument */
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
  rc = sqliteBtreeDropTable(pBt, iTable);
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
  char **argv            /* Text of each argument */
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
  rc = sqliteBtreeClearTable(pBt, iTable);
  if( rc!=SQLITE_OK ){
    Tcl_AppendResult(interp, errorName(rc), 0);
    return TCL_ERROR;
  }
  return TCL_OK;
}

/*
** Usage:   btree_get_meta ID
**
** Return meta data
*/
static int btree_get_meta(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  char **argv            /* Text of each argument */
){
  Btree *pBt;
  int rc;
  int i;
  int aMeta[SQLITE_N_BTREE_META];
  if( argc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " ID\"", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], (int*)&pBt) ) return TCL_ERROR;
  rc = sqliteBtreeGetMeta(pBt, aMeta);
  if( rc!=SQLITE_OK ){
    Tcl_AppendResult(interp, errorName(rc), 0);
    return TCL_ERROR;
  }
  for(i=0; i<SQLITE_N_BTREE_META; i++){
    char zBuf[30];
    sprintf(zBuf,"%d",aMeta[i]);
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
  char **argv            /* Text of each argument */
){
  Btree *pBt;
  int rc;
  int i;
  int aMeta[SQLITE_N_BTREE_META];

  if( argc!=2+SQLITE_N_BTREE_META ){
    char zBuf[30];
    sprintf(zBuf,"%d",SQLITE_N_BTREE_META);
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " ID METADATA...\" (METADATA is ", zBuf, " integers)", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], (int*)&pBt) ) return TCL_ERROR;
  for(i=0; i<SQLITE_N_BTREE_META; i++){
    if( Tcl_GetInt(interp, argv[i+2], &aMeta[i]) ) return TCL_ERROR;
  }
  rc = sqliteBtreeUpdateMeta(pBt, aMeta);
  if( rc!=SQLITE_OK ){
    Tcl_AppendResult(interp, errorName(rc), 0);
    return TCL_ERROR;
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
  char **argv            /* Text of each argument */
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
  rc = sqliteBtreePageDump(pBt, iPage, 0);
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
  char **argv            /* Text of each argument */
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
  rc = sqliteBtreePageDump(pBt, iPage, 1);
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
  char **argv            /* Text of each argument */
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
  a = sqlitepager_stats(sqliteBtreePager(pBt));
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
  char **argv            /* Text of each argument */
){
  Btree *pBt;

  if( argc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " ID\"", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], (int*)&pBt) ) return TCL_ERROR;
  sqlitepager_refdump(sqliteBtreePager(pBt));
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
  char **argv            /* Text of each argument */
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
  zResult = sqliteBtreeIntegrityCheck(pBt, aRoot, nRoot);
  if( zResult ){
    Tcl_AppendResult(interp, zResult, 0);
    free(zResult); 
  }
  return TCL_OK;
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
  char **argv            /* Text of each argument */
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
  rc = sqliteBtreeCursor(pBt, iTable, wrFlag, &pCur);
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
  char **argv            /* Text of each argument */
){
  BtCursor *pCur;
  int rc;

  if( argc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " ID\"", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], (int*)&pCur) ) return TCL_ERROR;
  rc = sqliteBtreeCloseCursor(pCur);
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
  char **argv            /* Text of each argument */
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
  rc = sqliteBtreeMoveto(pCur, argv[2], strlen(argv[2]), &res);  
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
  char **argv            /* Text of each argument */
){
  BtCursor *pCur;
  int rc;

  if( argc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " ID\"", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], (int*)&pCur) ) return TCL_ERROR;
  rc = sqliteBtreeDelete(pCur);
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
  char **argv            /* Text of each argument */
){
  BtCursor *pCur;
  int rc;

  if( argc!=4 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " ID KEY DATA\"", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], (int*)&pCur) ) return TCL_ERROR;
  rc = sqliteBtreeInsert(pCur, argv[2], strlen(argv[2]),
                         argv[3], strlen(argv[3]));
  if( rc ){
    Tcl_AppendResult(interp, errorName(rc), 0);
    return TCL_ERROR;
  }
  return SQLITE_OK;
}

/*
** Usage:   btree_next ID
**
** Move the cursor to the next entry in the table.
*/
static int btree_next(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  char **argv            /* Text of each argument */
){
  BtCursor *pCur;
  int rc;

  if( argc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " ID\"", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], (int*)&pCur) ) return TCL_ERROR;
  rc = sqliteBtreeNext(pCur, 0);
  if( rc ){
    Tcl_AppendResult(interp, errorName(rc), 0);
    return TCL_ERROR;
  }
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
  char **argv            /* Text of each argument */
){
  BtCursor *pCur;
  int rc;
  int n;
  char *zBuf;

  if( argc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " ID\"", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], (int*)&pCur) ) return TCL_ERROR;
  sqliteBtreeKeySize(pCur, &n);
  zBuf = malloc( n+1 );
  rc = sqliteBtreeKey(pCur, 0, n, zBuf);
  if( rc!=n ){
    char zMsg[100];
    free(zBuf);
    sprintf(zMsg, "truncated key: got %d of %d bytes", rc, n);
    Tcl_AppendResult(interp, zMsg, 0);
    return TCL_ERROR;
  }
  zBuf[n] = 0;
  Tcl_AppendResult(interp, zBuf, 0);
  free(zBuf);
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
  char **argv            /* Text of each argument */
){
  BtCursor *pCur;
  int rc;
  int n;
  char *zBuf;

  if( argc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " ID\"", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], (int*)&pCur) ) return TCL_ERROR;
  sqliteBtreeDataSize(pCur, &n);
  zBuf = malloc( n+1 );
  rc = sqliteBtreeData(pCur, 0, n, zBuf);
  if( rc!=n ){
    char zMsg[100];
    free(zBuf);
    sprintf(zMsg, "truncated data: got %d of %d bytes", rc, n);
    Tcl_AppendResult(interp, zMsg, 0);
    return TCL_ERROR;
  }
  zBuf[n] = 0;
  Tcl_AppendResult(interp, zBuf, 0);
  free(zBuf);
  return SQLITE_OK;
}

/*
** Usage:   btree_cursor_dump ID
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
static int btree_cursor_dump(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  char **argv            /* Text of each argument */
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
  rc = sqliteBtreeCursorDump(pCur, aResult);
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
** Register commands with the TCL interpreter.
*/
int Sqlitetest3_Init(Tcl_Interp *interp){
  Tcl_CreateCommand(interp, "btree_open", btree_open, 0, 0);
  Tcl_CreateCommand(interp, "btree_close", btree_close, 0, 0);
  Tcl_CreateCommand(interp, "btree_begin_transaction",
      btree_begin_transaction, 0, 0);
  Tcl_CreateCommand(interp, "btree_commit", btree_commit, 0, 0);
  Tcl_CreateCommand(interp, "btree_rollback", btree_rollback, 0, 0);
  Tcl_CreateCommand(interp, "btree_create_table", btree_create_table, 0, 0);
  Tcl_CreateCommand(interp, "btree_drop_table", btree_drop_table, 0, 0);
  Tcl_CreateCommand(interp, "btree_clear_table", btree_clear_table, 0, 0);
  Tcl_CreateCommand(interp, "btree_get_meta", btree_get_meta, 0, 0);
  Tcl_CreateCommand(interp, "btree_update_meta", btree_update_meta, 0, 0);
  Tcl_CreateCommand(interp, "btree_page_dump", btree_page_dump, 0, 0);
  Tcl_CreateCommand(interp, "btree_tree_dump", btree_tree_dump, 0, 0);
  Tcl_CreateCommand(interp, "btree_pager_stats", btree_pager_stats, 0, 0);
  Tcl_CreateCommand(interp, "btree_pager_ref_dump", btree_pager_ref_dump, 0, 0);
  Tcl_CreateCommand(interp, "btree_cursor", btree_cursor, 0, 0);
  Tcl_CreateCommand(interp, "btree_close_cursor", btree_close_cursor, 0, 0);
  Tcl_CreateCommand(interp, "btree_move_to", btree_move_to, 0, 0);
  Tcl_CreateCommand(interp, "btree_delete", btree_delete, 0, 0);
  Tcl_CreateCommand(interp, "btree_insert", btree_insert, 0, 0);
  Tcl_CreateCommand(interp, "btree_next", btree_next, 0, 0);
  Tcl_CreateCommand(interp, "btree_key", btree_key, 0, 0);
  Tcl_CreateCommand(interp, "btree_data", btree_data, 0, 0);
  Tcl_CreateCommand(interp, "btree_cursor_dump", btree_cursor_dump, 0, 0);
  Tcl_CreateCommand(interp, "btree_integrity_check", btree_integrity_check,0,0);
  Tcl_LinkVar(interp, "pager_refinfo_enable", (char*)&pager_refinfo_enable,
     TCL_LINK_INT);
  return TCL_OK;
}
