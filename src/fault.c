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
** malloc failures. 
**
** $Id: fault.c,v 1.8 2008/06/20 11:05:38 danielk1977 Exp $
*/

#include "sqliteInt.h"

#ifndef SQLITE_OMIT_BUILTIN_TEST

/*
** If zero, malloc() failures are non-benign. If non-zero, benign.
*/
static int memfault_is_benign = 0;

/*
** Return true if a malloc failures are currently considered to be
** benign. A benign fault does not affect the operation of sqlite.
** By constrast a non-benign fault causes sqlite to fail the current 
** operation and return SQLITE_NOMEM to the user.
*/
int sqlite3FaultIsBenign(void){
  return memfault_is_benign;
}

/* 
** After this routine causes subsequent malloc faults to be either 
** benign or hard (not benign), according to the "enable" parameter.
**
** Most faults are hard.  In other words, most faults cause
** an error to be propagated back up to the application interface.
** However, sometimes a fault is easily recoverable.  For example,
** if a malloc fails while resizing a hash table, this is completely
** recoverable simply by not carrying out the resize.  The hash table
** will continue to function normally.  So a malloc failure during
** a hash table resize is a benign fault.  
*/
void sqlite3FaultBeginBenign(int id){
  memfault_is_benign++;
}
void sqlite3FaultEndBenign(int id){
  memfault_is_benign--;
}

#endif   /* #ifndef SQLITE_OMIT_BUILTIN_TEST */

