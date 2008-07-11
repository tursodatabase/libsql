/*
** 2008 June 18
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
** This module implements the sqlite3_status() interface and related
** functionality.
**
** $Id: status.c,v 1.3 2008/07/11 16:15:18 drh Exp $
*/
#include "sqliteInt.h"

/*
** Variables in which to record status information.
*/
static struct {
  int nowValue[6];         /* Current value */
  int mxValue[6];          /* Maximum value */
} sqlite3Stat;


/*
** Reset the status records.  This routine is called by
** sqlite3_initialize().
*/
void sqlite3StatusReset(void){
  memset(&sqlite3Stat, 0, sizeof(sqlite3Stat));
}

/*
** Return the current value of a status parameter.
*/
int sqlite3StatusValue(int op){
  assert( op>=0 && op<ArraySize(sqlite3Stat.nowValue) );
  return sqlite3Stat.nowValue[op];
}

/*
** Add N to the value of a status record.  It is assumed that the
** caller holds appropriate locks.
*/
void sqlite3StatusAdd(int op, int N){
  assert( op>=0 && op<ArraySize(sqlite3Stat.nowValue) );
  sqlite3Stat.nowValue[op] += N;
  if( sqlite3Stat.nowValue[op]>sqlite3Stat.mxValue[op] ){
    sqlite3Stat.mxValue[op] = sqlite3Stat.nowValue[op];
  }
}

/*
** Set the value of a status to X.
*/
void sqlite3StatusSet(int op, int X){
  assert( op>=0 && op<ArraySize(sqlite3Stat.nowValue) );
  sqlite3Stat.nowValue[op] = X;
  if( sqlite3Stat.nowValue[op]>sqlite3Stat.mxValue[op] ){
    sqlite3Stat.mxValue[op] = sqlite3Stat.nowValue[op];
  }
}

/*
** Query status information.
**
** This implementation assumes that reading or writing an aligned
** 32-bit integer is an atomic operation.  If that assumption is not true,
** then this routine is not threadsafe.
*/
int sqlite3_status(int op, int *pCurrent, int *pHighwater, int resetFlag){
  if( op<0 || op>=ArraySize(sqlite3Stat.nowValue) ){
    return SQLITE_MISUSE;
  }
  *pCurrent = sqlite3Stat.nowValue[op];
  *pHighwater = sqlite3Stat.mxValue[op];
  if( resetFlag ){
    sqlite3Stat.mxValue[op] = sqlite3Stat.nowValue[op];
  }
  return SQLITE_OK;
}
