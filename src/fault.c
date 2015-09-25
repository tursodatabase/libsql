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
** The default xBenignCtrl function is a no-op
*/
static void sqlite3BenignCtrlNoop(int eOp){
  (void)eOp;
}

/*
** Global variable:  Pointer to the benign malloc control interface.
*/
static void (*sqlite3xBenignCtrl)(int) = sqlite3BenignCtrlNoop;

/*
** Register a pointer to the benign-malloc control interface function.
** If the argument is a NULL pointer, register the default no-op controller.
*/
void sqlite3BenignMallocHooks(void (*xBenignCtrl)(int)){
  sqlite3xBenignCtrl = xBenignCtrl ? xBenignCtrl : sqlite3BenignCtrlNoop;
}

/*
** The sqlite3BeginBenignMalloc() and sqlite3EndBenignMalloc() calls bracket
** sections of code for which malloc failures are non-fatal.  
*/
void sqlite3BeginBenignMalloc(void){
  sqlite3xBenignCtrl(1);
}
void sqlite3EndBenignMalloc(void){
  sqlite3xBenignCtrl(0);
}

/*
** The sqlite3PreviousBenignMalloc() call indicates that the previous
** malloc call (which must have failed) was a benign failure.
*/
void sqlite3PreviousBenignMalloc(void){
  sqlite3xBenignCtrl(2);
}

#endif   /* #ifndef SQLITE_OMIT_BUILTIN_TEST */
