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
** $Id: func.c,v 1.8 2002/02/28 01:46:13 drh Exp $
*/
#include <ctype.h>
#include <math.h>
#include <stdlib.h>
#include <assert.h>
#include "sqliteInt.h"

/*
** Implementation of the non-aggregate min() and max() functions
*/
static void minFunc(sqlite_func *context, int argc, const char **argv){
  const char *zBest; 
  int i;

  zBest = argv[0];
  for(i=1; i<argc; i++){
    if( sqliteCompare(argv[i], zBest)<0 ){
      zBest = argv[i];
    }
  }
  sqlite_set_result_string(context, zBest, -1);
}
static void maxFunc(sqlite_func *context, int argc, const char **argv){
  const char *zBest; 
  int i;

  zBest = argv[0];
  for(i=1; i<argc; i++){
    if( sqliteCompare(argv[i], zBest)>0 ){
      zBest = argv[i];
    }
  }
  sqlite_set_result_string(context, zBest, -1);
}

/*
** Implementation of the length() function
*/
static void lengthFunc(sqlite_func *context, int argc, const char **argv){
  const char *z;
  int len;

  assert( argc==1 );
  z = argv[0];
  if( z==0 ){
    len = 0;
  }else{
#ifdef SQLITE_UTF8
    for(len=0; *z; z++){ if( (0xc0&*z)!=0x80 ) len++; }
#else
    len = strlen(z);
#endif
  }
  sqlite_set_result_int(context, len);
}

/*
** Implementation of the abs() function
*/
static void absFunc(sqlite_func *context, int argc, const char **argv){
  const char *z;
  assert( argc==1 );
  z = argv[0];
  if( z && z[0]=='-' && isdigit(z[1]) ) z++;
  sqlite_set_result_string(context, z, -1);
}

/*
** Implementation of the substr() function
*/
static void substrFunc(sqlite_func *context, int argc, const char **argv){
  const char *z;
#ifdef SQLITE_UTF8
  const char *z2;
  int i;
#endif
  int p1, p2, len;
  assert( argc==3 );
  z = argv[0];
  if( z==0 ) return;
  p1 = atoi(argv[1]?argv[1]:0);
  p2 = atoi(argv[2]?argv[2]:0);
#ifdef SQLITE_UTF8
  for(len=0, z2=z; *z2; z2++){ if( (0xc0&*z)!=0x80 ) len++; }
#else
  len = strlen(z);
#endif
  if( p1<0 ){
    p1 = len-p1;
  }else if( p1>0 ){
    p1--;
  }
  if( p1+p2>len ){
    p2 = len-p1;
  }
#ifdef SQLITE_UTF8
  for(i=0; i<p1; i++){
    assert( z[i] );
    if( (z[i]&0xc0)!=0x80 ) p1++;
  }
  for(; i<p1+p2; i++){
    assert( z[i] );
    if( (z[i]&0xc0)!=0x80 ) p2++;
  }
#endif
  sqlite_set_result_string(context, &z[p1], p2);
}

/*
** Implementation of the round() function
*/
static void roundFunc(sqlite_func *context, int argc, const char **argv){
  int n;
  double r;
  char zBuf[100];
  assert( argc==1 || argc==2 );
  n = argc==2 && argv[1] ? atoi(argv[1]) : 0;
  if( n>30 ) n = 30;
  if( n<0 ) n = 0;
  r = argv[0] ? atof(argv[0]) : 0.0;
  sprintf(zBuf,"%.*f",n,r);
  sqlite_set_result_string(context, zBuf, -1);
}

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
** Implementation of the IFNULL() and NVL() functions.  (both do the
** same thing.  They return their first argument if it is not NULL or
** their second argument if the first is NULL.
*/
static void ifnullFunc(sqlite_func *context, int argc, const char **argv){
  const char *z;
  assert( argc==2 );
  z = argv[0] ? argv[0] : argv[1];
  sqlite_set_result_string(context, z, -1);
}

/*
** An instance of the following structure holds the context of a
** sum() or avg() aggregate computation.
*/
typedef struct SumCtx SumCtx;
struct SumCtx {
  double sum;     /* Sum of terms */
};

/*
** Routines used to compute the sum or average.
*/
static void sumStep(sqlite_func *context, int argc, const char **argv){
  SumCtx *p;
  double x;
  if( argc<1 ) return;
  p = sqlite_aggregate_context(context, sizeof(*p));
  if( p==0 ) return;
  x = argv[0] ? atof(argv[0]) : 0.0;
  p->sum += x;
}
static void sumFinalize(sqlite_func *context){
  SumCtx *p;
  p = sqlite_aggregate_context(context, sizeof(*p));
  if( p ){
    sqlite_set_result_double(context, p->sum);
  }
}
static void avgFinalize(sqlite_func *context){
  SumCtx *p;
  double rN;
  p = sqlite_aggregate_context(context, sizeof(*p));
  rN = sqlite_aggregate_count(context);
  if( p && rN>0.0 ){
    sqlite_set_result_double(context, p->sum/rN);
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
  x = argv[0] ? atof(argv[0]) : 0.0;
  p->sum += x;
  p->sum2 += x*x;
}
static void stdDevFinalize(sqlite_func *context){
  double rN = sqlite_aggregate_count(context);
  StdDevCtx *p = sqlite_aggregate_context(context, sizeof(*p));
  if( p && rN>1.0 ){
    sqlite_set_result_double(context, 
       sqrt((p->sum2 - p->sum*p->sum/rN)/(rN-1.0)));
  }
}

/*
** The following structure keeps track of state information for the
** count() aggregate function.
*/
typedef struct CountCtx CountCtx;
struct CountCtx {
  int n;
};

/*
** Routines to implement the count() aggregate function.
*/
static void countStep(sqlite_func *context, int argc, const char **argv){
  CountCtx *p;
  p = sqlite_aggregate_context(context, sizeof(*p));
  if( (argc==0 || argv[0]) && p ){
    p->n++;
  }
}   
static void countFinalize(sqlite_func *context){
  CountCtx *p;
  p = sqlite_aggregate_context(context, sizeof(*p));
  sqlite_set_result_int(context, p ? p->n : 0);
}

/*
** This function tracks state information for the min() and max()
** aggregate functions.
*/
typedef struct MinMaxCtx MinMaxCtx;
struct MinMaxCtx {
  char *z;         /* The best so far */
  char zBuf[28];   /* Space that can be used for storage */
};

/*
** Routines to implement min() and max() aggregate functions.
*/
static void minStep(sqlite_func *context, int argc, const char **argv){
  MinMaxCtx *p;
  p = sqlite_aggregate_context(context, sizeof(*p));
  if( p==0 || argc<1 ) return;
  if( sqlite_aggregate_count(context)==1 || sqliteCompare(argv[0],p->z)<0 ){
    if( p->z && p->z!=p->zBuf ){
      sqliteFree(p->z);
    }
    if( argv[0] ){
      int len = strlen(argv[0]);
      if( len < sizeof(p->zBuf) ){
        p->z = p->zBuf;
      }else{
        p->z = sqliteMalloc( len+1 );
        if( p->z==0 ) return;
      }
      strcpy(p->z, argv[0]);
    }else{
      p->z = 0;
    }
  }
}
static void maxStep(sqlite_func *context, int argc, const char **argv){
  MinMaxCtx *p;
  p = sqlite_aggregate_context(context, sizeof(*p));
  if( p==0 || argc<1 ) return;
  if( sqlite_aggregate_count(context)==1 || sqliteCompare(argv[0],p->z)>0 ){
    if( p->z && p->z!=p->zBuf ){
      sqliteFree(p->z);
    }
    if( argv[0] ){
      int len = strlen(argv[0]);
      if( len < sizeof(p->zBuf) ){
        p->z = p->zBuf;
      }else{
        p->z = sqliteMalloc( len+1 );
        if( p->z==0 ) return;
      }
      strcpy(p->z, argv[0]);
    }else{
      p->z = 0;
    }
  }
}
static void minMaxFinalize(sqlite_func *context){
  MinMaxCtx *p;
  p = sqlite_aggregate_context(context, sizeof(*p));
  if( p && p->z ){
    sqlite_set_result_string(context, p->z, strlen(p->z));
  }
  if( p && p->z && p->z!=p->zBuf ){
    sqliteFree(p->z);
  }
}

/*
** This function registered all of the above C functions as SQL
** functions.  This should be the only routine in this file with
** external linkage.
*/
void sqliteRegisterBuildinFunctions(sqlite *db){
  static struct {
     char *zName;
     int nArg;
     void (*xFunc)(sqlite_func*,int,const char**);
  } aFuncs[] = {
    { "min",  -1,  minFunc    },
    { "max",  -1,  maxFunc    },
    { "length", 1, lengthFunc },
    { "substr", 3, substrFunc },
    { "abs",    1, absFunc    },
    { "round",  1, roundFunc  },
    { "round",  2, roundFunc  },
    { "upper",  1, upperFunc  },
    { "lower",  1, lowerFunc  },
    { "ifnull", 2, ifnullFunc },
    { "nvl",    2, ifnullFunc },
  };
  static struct {
    char *zName;
    int nArg;
    void (*xStep)(sqlite_func*,int,const char**);
    void (*xFinalize)(sqlite_func*);
  } aAggs[] = {
    { "min",    1, minStep,      minMaxFinalize },
    { "max",    1, maxStep,      minMaxFinalize },
    { "sum",    1, sumStep,      sumFinalize    },
    { "avg",    1, sumStep,      avgFinalize    },
    { "count",  0, countStep,    countFinalize  },
    { "count",  1, countStep,    countFinalize  },
    { "stddev", 1, stdDevStep,   stdDevFinalize },
  };
  int i;

  for(i=0; i<sizeof(aFuncs)/sizeof(aFuncs[0]); i++){
    sqlite_create_function(db, aFuncs[i].zName,
           aFuncs[i].nArg, aFuncs[i].xFunc, 0);
  }
  for(i=0; i<sizeof(aAggs)/sizeof(aAggs[0]); i++){
    sqlite_create_aggregate(db, aAggs[i].zName,
           aAggs[i].nArg, aAggs[i].xStep, aAggs[i].xFinalize, 0);
  }
}
