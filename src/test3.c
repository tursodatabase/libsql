/*
** Copyright (c) 2001 D. Richard Hipp
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
** Code for testing the btree.c module in SQLite.  This code
** is not included in the SQLite library.  It is used for automated
** testing of the SQLite library.
**
** $Id: test3.c,v 1.1 2001/06/02 02:40:57 drh Exp $
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
  BTree *pBt;
  int nPage;
  int rc;
  char zBuf[100];
  if( argc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " FILENAME\"", 0);
    return TCL_ERROR;
  }
  rc = sqliteBtreeOpen(argv[1], 0666, &pBt);
  if( rc!=SQLITE_OK ){
    Tcl_AppendResult(interp, errorName(rc), 0);
    return TCL_ERROR;
  }
  sprintf(zBuf,"0x%x",(int)pBt);
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
  Btree *pBt
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
  Pager *pPager;
  int iTable;
  char zBuf[100];
  if( argc!=3 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " ID TABLENUM\"", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], (int*)&pBt) ) return TCL_ERROR;
  if( Tcl_GetInt(interp, argv[2], &iTable ) return TCL_ERROR;
  rc = sqliteBtreeDropTable(pBt, iTable);
  if( rc!=SQLITE_OK ){
    Tcl_AppendResult(interp, errorName(rc), 0);
    return TCL_ERROR;
  }
  return TCL_OK;
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
  return TCL_OK;
}
