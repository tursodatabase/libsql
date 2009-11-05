/*
** 2009 March 26
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
******************************************************************************
**
** This header file is used by programs that want to link against the
** CSV Virtual Table extention.
**
** All it does is declare the sqlite3CsvInit() interface.
*/
#include "sqlite3.h"

#ifdef __cplusplus
extern "C" {
#endif  /* __cplusplus */

int sqlite3CsvInit(sqlite3 *db);

#ifdef __cplusplus
}  /* extern "C" */
#endif  /* __cplusplus */
