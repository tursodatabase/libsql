/*
** 2008 Jan 22
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** $Id: fault.c,v 1.10 2008/06/22 12:37:58 drh Exp $
*/

/*
** This file contains code to support the concept of "benign" 
** malloc failures (when the xMalloc() or xRealloc() method of the
** sqlite3_mem_methods structure fails to allocate a block of memory
** and returns 0). 
**
** Most malloc failures are non-benign. After they occur, SQLite
** abandons the current operation and returns an error code (usually
** SQLITE_NOMEM) to the user. However, sometimes a fault is not necessarily
** fatal. For example, if a malloc fails while resizing a hash table, this 
** is completely recoverable simply by not carrying out the resize. The 
** hash table will continue to function normally.  So a malloc failure 
** during a hash table resize is a benign fault.
*/

#include "sqliteInt.h"

#ifndef SQLITE_OMIT_BUILTIN_TEST

/*
** Global variables.
*/
static struct BenignMallocHooks {
  void (*xBenignBegin)(void);
  void (*xBenignEnd)(void);
} hooks;

/*
** Register hooks to call when sqlite3BeginBenignMalloc() and
** sqlite3EndBenignMalloc() are called, respectively.
*/
void sqlite3BenignMallocHooks(
  void (*xBenignBegin)(void),
  void (*xBenignEnd)(void)
){
  hooks.xBenignBegin = xBenignBegin;
  hooks.xBenignEnd = xBenignEnd;
}

/*
** This (sqlite3EndBenignMalloc()) is called by SQLite code to indicate that
** subsequent malloc failures are benign. A call to sqlite3EndBenignMalloc()
** indicates that subsequent malloc failures are non-benign.
*/
void sqlite3BeginBenignMalloc(void){
  if( hooks.xBenignBegin ){
    hooks.xBenignBegin();
  }
}
void sqlite3EndBenignMalloc(void){
  if( hooks.xBenignEnd ){
    hooks.xBenignEnd();
  }
}

#endif   /* #ifndef SQLITE_OMIT_BUILTIN_TEST */
