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
** $Id: func.c,v 1.2 2002/02/24 17:12:54 drh Exp $
*/
#include <ctype.h>
#include <math.h>
#include <stdlib.h>
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
** An instance of the following structure holds the context of a
** standard deviation computation.
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
static void *stdDevStep(void *stddev, int argc, char **argv){
  StdDevCtx *p;
  double x;
  if( argc<1 ) return 0;
  if( stddev==0 ){
    p = malloc( sizeof(*p) );
    p->n = 0;
    p->sum = 0.0;
    p->sum2 = 0.0;
  }else{
    p = (StdDevCtx*)stddev;
  }
  x = atof(argv[0]);
  p->sum += x;
  p->sum2 += x*x;
  p->n++;
  return p;
}
static void stdDevFinalize(void *stddev, void *context){
  StdDevCtx *p = (StdDevCtx*)stddev;
  if( context && p && p->n>1 ){
    double rN = p->n;
    sqlite_set_result_double(context, 
       sqrt((p->sum2 - p->sum*p->sum/rN)/(rN-1.0)));
  }
  if( stddev ) free(stddev);
}

/*
** This file registered all of the above C functions as SQL
** functions.
*/
void sqliteRegisterBuildinFunctions(sqlite *db){
  sqlite_create_function(db, "upper", 1, upperFunc);
  sqlite_create_function(db, "lower", 1, lowerFunc);
  sqlite_create_aggregate(db, "stddev", 1, stdDevStep, stdDevFinalize);
}
