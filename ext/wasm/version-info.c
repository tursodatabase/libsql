/*
** 2022-10-16
**
** The author disclaims copyright to this source code.  In place of a
** legal notice, here is a blessing:
**
** *   May you do good and not evil.
** *   May you find forgiveness for yourself and forgive others.
** *   May you share freely, never taking more than you give.
**
*************************************************************************
** This file simply outputs sqlite3 version information in JSON form,
** intended for embedding in the sqlite3 JS API build.
*/
#include <stdio.h>
#include "sqlite3.h"
int main(int argc, char const * const * argv){
  if(argc || argv){/*unused*/}
  printf("{\"libVersion\": \"%s\", "
         "\"libVersionNumber\": %d, "
         "\"sourceId\": \"%s\"}"/*missing newline is intentional*/,
         SQLITE_VERSION,
         SQLITE_VERSION_NUMBER,
         SQLITE_SOURCE_ID);
  return 0;
}
