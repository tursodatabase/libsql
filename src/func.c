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
** $Id: func.c,v 1.4 2002/02/27 19:00:22 drh Exp $
*/
#include <ctype.h>
#include <math.h>
#include <stdlib.h>
#include "sqlite.h"

/*
** Implementation of the upper() and lower() SQL functions.
*/
static void upperFunc(sqlite_func *context, int argc, const char **argv){
  char *z;
  int i;
  if( argc<1 || argv[0]==0 ) return;
  z = sqlite_set_result_string(context, argv[0], -1);
  if( z==0 ) return;
  for(i=0; z[i]; i++){
    if( islower(z[i]) ) z[i] = toupper(z[i]);
  }
}
static void lowerFunc(sqlite_func *context, int argc, const char **argv){
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
** An instance of the following structure holds the context of a
** variance or standard deviation computation.
*/
typedef struct StdDevCtx StdDevCtx;
struct StdDevCtx {
  double sum;     /* Sum of terms */
  double sum2;    /* Sum of the squares of terms */
  int n;          /* Number of terms seen so far */
};

/*
** Routines used to compute the standard deviation as an aggregate.
*/
static void stdDevStep(sqlite_func *context, int argc, const char **argv){
  StdDevCtx *p;
  double x;
  if( argc<1 ) return;
  p = sqlite_aggregate_context(context, sizeof(*p));
  if( p==0 ) return;
  x = atof(argv[0]);
  p->sum += x;
  p->sum2 += x*x;
  p->n++;
}
static void stdDevFinalize(sqlite_func *context){
  StdDevCtx *p = sqlite_aggregate_context(context, sizeof(*p));
  if( p && p->n>1 ){
    double rN = p->n;
    sqlite_set_result_double(context, 
       sqrt((p->sum2 - p->sum*p->sum/rN)/(rN-1.0)));
  }
}

/*
** This function registered all of the above C functions as SQL
** functions.  This should be the only routine in this file with
** external linkage.
*/
void sqliteRegisterBuildinFunctions(sqlite *db){
  sqlite_create_function(db, "upper", 1, upperFunc, 0);
  sqlite_create_function(db, "lower", 1, lowerFunc, 0);
  sqlite_create_aggregate(db, "stddev", 1, stdDevStep, stdDevFinalize, 0);
}
