/*
** 2002 February 23
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains the C functions that implement various SQL
** functions of SQLite.  
**
** There is only one exported symbol in this file - the function
** sqliteRegisterBuildinFunctions() found at the bottom of the file.
** All other code has file scope.
**
** $Id: func.c,v 1.1 2002/02/24 01:55:17 drh Exp $
*/
#include <ctype.h>
#include "sqlite.h"

/*
** Implementation of the upper() and lower() SQL functions.
*/
static void upperFunc(void *context, int argc, const char **argv){
  char *z;
  int i;
  if( argc<1 || argv[0]==0 ) return;
  z = sqlite_set_result_string(context, argv[0], -1);
  if( z==0 ) return;
  for(i=0; z[i]; i++){
    if( islower(z[i]) ) z[i] = toupper(z[i]);
  }
}
static void lowerFunc(void *context, int argc, const char **argv){
  char *z;
  int i;
  if( argc<1 || argv[0]==0 ) return;
  z = sqlite_set_result_string(context, argv[0], -1);
  if( z==0 ) return;
  for(i=0; z[i]; i++){
    if( isupper(z[i]) ) z[i] = tolower(z[i]);
  }
}

/*
** This file registered all of the above C functions as SQL
** functions.
*/
void sqliteRegisterBuildinFunctions(sqlite *db){
  sqlite_create_function(db, "upper", 1, upperFunc);
  sqlite_create_function(db, "lower", 1, lowerFunc);
}
