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
** $Id: func.c,v 1.53 2004/05/25 01:13:21 danielk1977 Exp $
*/
#include <ctype.h>
#include <math.h>
#include <stdlib.h>
#include <assert.h>
#include "sqliteInt.h"
#include "vdbeInt.h"
#include "os.h"

/*
** Implementation of the non-aggregate min() and max() functions
*/
static void minmaxFunc(sqlite_func *context, int argc, sqlite3_value **argv){
  const char *zBest; 
  int i;
  int (*xCompare)(const char*, const char*);
  int mask;    /* 0 for min() or 0xffffffff for max() */
  const char *zArg;

  if( argc==0 ) return;
  mask = (int)sqlite3_user_data(context);
  zBest = sqlite3_value_data(argv[0]);
  if( zBest==0 ) return;
  zArg = sqlite3_value_data(argv[1]);
  if( zArg[0]=='n' ){
    xCompare = sqlite3Compare;
  }else{
    xCompare = strcmp;
  }
  for(i=2; i<argc; i+=2){
    zArg = sqlite3_value_data(argv[i]);
    if( zArg==0 ) return;
    if( (xCompare(zArg, zBest)^mask)<0 ){
      zBest = zArg;
    }
  }
  sqlite3_set_result_string(context, zBest, -1);
}

/*
** Return the type of the argument.
*/
static void typeofFunc(sqlite_func *context, int argc, sqlite3_value **argv){
  const char *z = 0;
  assert( argc==2 );
  switch( sqlite3_value_type(argv[0]) ){
    case SQLITE3_NULL: z = "null" ; break;
    case SQLITE3_INTEGER: z = "integer" ; break;
    case SQLITE3_TEXT: z = "text" ; break;
    case SQLITE3_FLOAT: z = "real" ; break;
    case SQLITE3_BLOB: z = "blob" ; break;
  }
  sqlite3_set_result_string(context, z, -1);
}

/*
** Implementation of the length() function
*/
static void lengthFunc(sqlite_func *context, int argc, sqlite3_value **argv){
  const char *z;
  int len;

  assert( argc==1 );
  z = sqlite3_value_data(argv[0]);
  if( z==0 ) return;
#ifdef SQLITE_UTF8
  for(len=0; *z; z++){ if( (0xc0&*z)!=0x80 ) len++; }
#else
  len = strlen(z);
#endif
  sqlite3_set_result_int(context, len);
}

/*
** Implementation of the abs() function
*/
static void absFunc(sqlite_func *context, int argc, sqlite3_value **argv){
  const char *z;
  assert( argc==1 );
  z = sqlite3_value_data(argv[0]);
  if( z==0 ) return;
  if( z[0]=='-' && isdigit(z[1]) ) z++;
  sqlite3_set_result_string(context, z, -1);
}

/*
** Implementation of the substr() function
*/
static void substrFunc(sqlite_func *context, int argc, sqlite3_value **argv){
  const char *z;
#ifdef SQLITE_UTF8
  const char *z2;
  int i;
#endif
  int p1, p2, len;
  assert( argc==3 );
  z = sqlite3_value_data(argv[0]);
  if( z==0 ) return;
  p1 = sqlite3_value_int(argv[1]);
  p2 = sqlite3_value_int(argv[2]);
#ifdef SQLITE_UTF8
  for(len=0, z2=z; *z2; z2++){ if( (0xc0&*z2)!=0x80 ) len++; }
#else
  len = strlen(z);
#endif
  if( p1<0 ){
    p1 += len;
    if( p1<0 ){
      p2 += p1;
      p1 = 0;
    }
  }else if( p1>0 ){
    p1--;
  }
  if( p1+p2>len ){
    p2 = len-p1;
  }
#ifdef SQLITE_UTF8
  for(i=0; i<p1 && z[i]; i++){
    if( (z[i]&0xc0)==0x80 ) p1++;
  }
  while( z[i] && (z[i]&0xc0)==0x80 ){ i++; p1++; }
  for(; i<p1+p2 && z[i]; i++){
    if( (z[i]&0xc0)==0x80 ) p2++;
  }
  while( z[i] && (z[i]&0xc0)==0x80 ){ i++; p2++; }
#endif
  if( p2<0 ) p2 = 0;
  sqlite3_set_result_string(context, &z[p1], p2);
}

/*
** Implementation of the round() function
*/
static void roundFunc(sqlite_func *context, int argc, sqlite3_value **argv){
  int n = 0;
  double r;
  char zBuf[100];
  assert( argc==1 || argc==2 );
  if( argc==2 ){
    if( SQLITE3_NULL==sqlite3_value_type(argv[1]) ) return;
    n = sqlite3_value_int(argv[1]);
    if( n>30 ) n = 30;
    if( n<0 ) n = 0;
  }
  if( SQLITE3_NULL==sqlite3_value_type(argv[0]) ) return;
  r = sqlite3_value_float(argv[0]);
  sprintf(zBuf,"%.*f",n,r);
  sqlite3_set_result_string(context, zBuf, -1);
}

/*
** Implementation of the upper() and lower() SQL functions.
*/
static void upperFunc(sqlite_func *context, int argc, sqlite3_value **argv){
  char *z;
  int i;
  if( argc<1 ) return;
  z = sqlite3_set_result_string(context, sqlite3_value_data(argv[0]), -1);
  if( z==0 ) return;
  for(i=0; z[i]; i++){
    if( islower(z[i]) ) z[i] = toupper(z[i]);
  }
}
static void lowerFunc(sqlite_func *context, int argc, sqlite3_value **argv){
  char *z;
  int i;
  if( argc<1 ) return;
  z = sqlite3_set_result_string(context, sqlite3_value_data(argv[0]), -1);
  if( z==0 ) return;
  for(i=0; z[i]; i++){
    if( isupper(z[i]) ) z[i] = tolower(z[i]);
  }
}

/*
** Implementation of the IFNULL(), NVL(), and COALESCE() functions.  
** All three do the same thing.  They return the first non-NULL
** argument.
*/
static void ifnullFunc(sqlite_func *context, int argc, sqlite3_value **argv){
  int i;
  for(i=0; i<argc; i++){
    if( SQLITE3_NULL!=sqlite3_value_type(argv[i]) ){
      sqlite3_set_result_string(context, sqlite3_value_data(argv[i]), -1);
      break;
    }
  }
}

/*
** Implementation of random().  Return a random integer.  
*/
static void randomFunc(sqlite_func *context, int argc, sqlite3_value **argv){
  int r;
  sqlite3Randomness(sizeof(r), &r);
  sqlite3_set_result_int(context, r);
}

/*
** Implementation of the last_insert_rowid() SQL function.  The return
** value is the same as the sqlite3_last_insert_rowid() API function.
*/
static void last_insert_rowid(
  sqlite_func *context, 
  int arg, 
  sqlite3_value **argv
){
  sqlite *db = sqlite3_user_data(context);
  sqlite3_set_result_int(context, sqlite3_last_insert_rowid(db));
}

/*
** Implementation of the change_count() SQL function.  The return
** value is the same as the sqlite3_changes() API function.
*/
static void change_count(sqlite_func *context, int arg, sqlite3_value **argv){
  sqlite *db = sqlite3_user_data(context);
  sqlite3_set_result_int(context, sqlite3_changes(db));
}

/*
** Implementation of the last_statement_change_count() SQL function.  The
** return value is the same as the sqlite3_last_statement_changes() API
** function.
*/
static void last_statement_change_count(
  sqlite_func *context, 
  int arg,
  sqlite3_value **argv
){
  sqlite *db = sqlite3_user_data(context);
  sqlite3_set_result_int(context, sqlite3_last_statement_changes(db));
}

/*
** Implementation of the like() SQL function.  This function implements
** the build-in LIKE operator.  The first argument to the function is the
** string and the second argument is the pattern.  So, the SQL statements:
**
**       A LIKE B
**
** is implemented as like(A,B).
*/
static void likeFunc(
  sqlite_func *context, 
  int argc, 
  sqlite3_value **argv
){
  const unsigned char *zA = sqlite3_value_data(argv[0]);
  const unsigned char *zB = sqlite3_value_data(argv[1]);
  if( zA && zB ){
    sqlite3_set_result_int(context, sqlite3LikeCompare(zA, zB));
  }
}

/*
** Implementation of the glob() SQL function.  This function implements
** the build-in GLOB operator.  The first argument to the function is the
** string and the second argument is the pattern.  So, the SQL statements:
**
**       A GLOB B
**
** is implemented as glob(A,B).
*/
static void globFunc(sqlite_func *context, int arg, sqlite3_value **argv){
  const unsigned char *zA = sqlite3_value_data(argv[0]);
  const unsigned char *zB = sqlite3_value_data(argv[1]);
  if( zA && zB ){
    sqlite3_set_result_int(context, sqlite3GlobCompare(zA, zB));
  }
}

/*
** Implementation of the NULLIF(x,y) function.  The result is the first
** argument if the arguments are different.  The result is NULL if the
** arguments are equal to each other.
*/
static void nullifFunc(sqlite_func *context, int argc, sqlite3_value **argv){
  const unsigned char *zX = sqlite3_value_data(argv[0]);
  const unsigned char *zY = sqlite3_value_data(argv[1]);
  if( zX!=0 && sqlite3Compare(zX, zY)!=0 ){
    sqlite3_set_result_string(context, zX, -1);
  }
}

/*
** Implementation of the VERSION(*) function.  The result is the version
** of the SQLite library that is running.
*/
static void versionFunc(sqlite_func *context, int argc, sqlite3_value **argv){
  sqlite3_set_result_string(context, sqlite3_version, -1);
}

/*
** EXPERIMENTAL - This is not an official function.  The interface may
** change.  This function may disappear.  Do not write code that depends
** on this function.
**
** Implementation of the QUOTE() function.  This function takes a single
** argument.  If the argument is numeric, the return value is the same as
** the argument.  If the argument is NULL, the return value is the string
** "NULL".  Otherwise, the argument is enclosed in single quotes with
** single-quote escapes.
*/
static void quoteFunc(sqlite_func *context, int argc, sqlite3_value **argv){
  const char *zArg = sqlite3_value_data(argv[0]);
  if( argc<1 ) return;
  if( zArg==0 ){
    sqlite3_set_result_string(context, "NULL", 4);
  }else if( sqlite3IsNumber(zArg, 0, TEXT_Utf8) ){
    sqlite3_set_result_string(context, zArg, -1);
  }else{
    int i,j,n;
    char *z;
    for(i=n=0; zArg[i]; i++){ if( zArg[i]=='\'' ) n++; }
    z = sqliteMalloc( i+n+3 );
    if( z==0 ) return;
    z[0] = '\'';
    for(i=0, j=1; zArg[i]; i++){
      z[j++] = zArg[i];
      if( zArg[i]=='\'' ){
        z[j++] = '\'';
      }
    }
    z[j++] = '\'';
    z[j] = 0;
    sqlite3_set_result_string(context, z, j);
    sqliteFree(z);
  }
}

#ifdef SQLITE_SOUNDEX
/*
** Compute the soundex encoding of a word.
*/
static void soundexFunc(sqlite_func *context, int argc, sqlite3_value **argv){
  char zResult[8];
  const char *zIn;
  int i, j;
  static const unsigned char iCode[] = {
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 1, 2, 3, 0, 1, 2, 0, 0, 2, 2, 4, 5, 5, 0,
    1, 2, 6, 2, 3, 0, 1, 0, 2, 0, 2, 0, 0, 0, 0, 0,
    0, 0, 1, 2, 3, 0, 1, 2, 0, 0, 2, 2, 4, 5, 5, 0,
    1, 2, 6, 2, 3, 0, 1, 0, 2, 0, 2, 0, 0, 0, 0, 0,
  };
  assert( argc==1 );
  zIn = sqlite3_value_data(argv[0]);
  for(i=0; zIn[i] && !isalpha(zIn[i]); i++){}
  if( zIn[i] ){
    zResult[0] = toupper(zIn[i]);
    for(j=1; j<4 && zIn[i]; i++){
      int code = iCode[zIn[i]&0x7f];
      if( code>0 ){
        zResult[j++] = code + '0';
      }
    }
    while( j<4 ){
      zResult[j++] = '0';
    }
    zResult[j] = 0;
    sqlite3_set_result_string(context, zResult, 4);
  }else{
    sqlite3_set_result_string(context, "?000", 4);
  }
}
#endif

#ifdef SQLITE_TEST
/*
** This function generates a string of random characters.  Used for
** generating test data.
*/
static void randStr(sqlite_func *context, int argc, sqlite3_value **argv){
  static const unsigned char zSrc[] = 
     "abcdefghijklmnopqrstuvwxyz"
     "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
     "0123456789"
     ".-!,:*^+=_|?/<> ";
  int iMin, iMax, n, r, i;
  unsigned char zBuf[1000];
  if( argc>=1 ){
    iMin = atoi(sqlite3_value_data(argv[0]));
    if( iMin<0 ) iMin = 0;
    if( iMin>=sizeof(zBuf) ) iMin = sizeof(zBuf)-1;
  }else{
    iMin = 1;
  }
  if( argc>=2 ){
    iMax = atoi(sqlite3_value_data(argv[1]));
    if( iMax<iMin ) iMax = iMin;
    if( iMax>=sizeof(zBuf) ) iMax = sizeof(zBuf)-1;
  }else{
    iMax = 50;
  }
  n = iMin;
  if( iMax>iMin ){
    sqlite3Randomness(sizeof(r), &r);
    r &= 0x7fffffff;
    n += r%(iMax + 1 - iMin);
  }
  assert( n<sizeof(zBuf) );
  sqlite3Randomness(n, zBuf);
  for(i=0; i<n; i++){
    zBuf[i] = zSrc[zBuf[i]%(sizeof(zSrc)-1)];
  }
  zBuf[n] = 0;
  sqlite3_set_result_string(context, zBuf, n);
}
#endif

/*
** An instance of the following structure holds the context of a
** sum() or avg() aggregate computation.
*/
typedef struct SumCtx SumCtx;
struct SumCtx {
  double sum;     /* Sum of terms */
  int cnt;        /* Number of elements summed */
};

/*
** Routines used to compute the sum or average.
*/
static void sumStep(sqlite_func *context, int argc, sqlite3_value **argv){
  SumCtx *p;
  if( argc<1 ) return;
  p = sqlite3_aggregate_context(context, sizeof(*p));
  if( p && SQLITE3_NULL!=sqlite3_value_type(argv[0]) ){
    p->sum += sqlite3_value_float(argv[0]);
    p->cnt++;
  }
}
static void sumFinalize(sqlite_func *context){
  SumCtx *p;
  p = sqlite3_aggregate_context(context, sizeof(*p));
  sqlite3_set_result_double(context, p ? p->sum : 0.0);
}
static void avgFinalize(sqlite_func *context){
  SumCtx *p;
  p = sqlite3_aggregate_context(context, sizeof(*p));
  if( p && p->cnt>0 ){
    sqlite3_set_result_double(context, p->sum/(double)p->cnt);
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
  int cnt;        /* Number of terms counted */
};

#if 0   /* Omit because math library is required */
/*
** Routines used to compute the standard deviation as an aggregate.
*/
static void stdDevStep(sqlite_func *context, int argc, const char **argv){
  StdDevCtx *p;
  double x;
  if( argc<1 ) return;
  p = sqlite3_aggregate_context(context, sizeof(*p));
  if( p && argv[0] ){
    x = sqlite3AtoF(argv[0], 0);
    p->sum += x;
    p->sum2 += x*x;
    p->cnt++;
  }
}
static void stdDevFinalize(sqlite_func *context){
  double rN = sqlite3_aggregate_count(context);
  StdDevCtx *p = sqlite3_aggregate_context(context, sizeof(*p));
  if( p && p->cnt>1 ){
    double rCnt = cnt;
    sqlite3_set_result_double(context, 
       sqrt((p->sum2 - p->sum*p->sum/rCnt)/(rCnt-1.0)));
  }
}
#endif

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
static void countStep(sqlite_func *context, int argc, sqlite3_value **argv){
  CountCtx *p;
  p = sqlite3_aggregate_context(context, sizeof(*p));
  if( (argc==0 || SQLITE3_NULL!=sqlite3_value_type(argv[0])) && p ){
    p->n++;
  }
}   
static void countFinalize(sqlite_func *context){
  CountCtx *p;
  p = sqlite3_aggregate_context(context, sizeof(*p));
  sqlite3_set_result_int(context, p ? p->n : 0);
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
static void minmaxStep(sqlite_func *context, int argc, sqlite3_value **argv){
  int max = 0;
  int cmp = 0;
  Mem *pArg  = (Mem *)argv[0];
  Mem *pBest = (Mem *)sqlite3_aggregate_context(context, sizeof(*pBest));

  if( SQLITE3_NULL==sqlite3_value_type(argv[0]) ) return;

  if( pBest->flags ){
    max = ((sqlite3_user_data(context)==(void *)-1)?1:0);
    cmp = sqlite3MemCompare(pBest, pArg, 0);
    if( (max && cmp<0) || (!max && cmp>0) ){
      sqlite3MemCopy(pBest, pArg);
    }
  }else{
    sqlite3MemCopy(pBest, pArg);
  }
}
static void minMaxFinalize(sqlite_func *context){
  sqlite3_value *pRes;
  pRes = (sqlite3_value *)sqlite3_aggregate_context(context, sizeof(Mem));
  
  if( pRes->flags ){
    switch( sqlite3_value_type(pRes) ){
      case SQLITE3_INTEGER: 
        sqlite3_set_result_int(context, sqlite3_value_int(pRes));
        break;
      case SQLITE3_FLOAT: 
        sqlite3_set_result_double(context, sqlite3_value_float(pRes));
      case SQLITE3_TEXT: 
      case SQLITE3_BLOB: 
        sqlite3_set_result_string(context,
            sqlite3_value_data(pRes),
            sqlite3_value_bytes(pRes));
        break;
      case SQLITE3_NULL: 
      default:
        assert(0);
    }
  }
}

/*
** This function registered all of the above C functions as SQL
** functions.  This should be the only routine in this file with
** external linkage.
*/
void sqlite3RegisterBuiltinFunctions(sqlite *db){
  static struct {
     char *zName;
     signed char nArg;
     signed char dataType;
     u8 argType;               /* 0: none.  1: db  2: (-1) */
     void (*xFunc)(sqlite_func*,int,sqlite3_value **);
  } aFuncs[] = {
    { "min",       -1, SQLITE_ARGS,    0, minmaxFunc },
    { "min",        0, 0,              0, 0          },
    { "max",       -1, SQLITE_ARGS,    2, minmaxFunc },
    { "max",        0, 0,              2, 0          },
    { "typeof",     1, SQLITE_TEXT,    0, typeofFunc },
    { "classof",    1, SQLITE_TEXT,    0, typeofFunc }, /* FIX ME: hack */
    { "length",     1, SQLITE_NUMERIC, 0, lengthFunc },
    { "substr",     3, SQLITE_TEXT,    0, substrFunc },
    { "abs",        1, SQLITE_NUMERIC, 0, absFunc    },
    { "round",      1, SQLITE_NUMERIC, 0, roundFunc  },
    { "round",      2, SQLITE_NUMERIC, 0, roundFunc  },
    { "upper",      1, SQLITE_TEXT,    0, upperFunc  },
    { "lower",      1, SQLITE_TEXT,    0, lowerFunc  },
    { "coalesce",  -1, SQLITE_ARGS,    0, ifnullFunc },
    { "coalesce",   0, 0,              0, 0          },
    { "coalesce",   1, 0,              0, 0          },
    { "ifnull",     2, SQLITE_ARGS,    0, ifnullFunc },
    { "random",    -1, SQLITE_NUMERIC, 0, randomFunc },
    { "like",       2, SQLITE_NUMERIC, 0, likeFunc   },
    { "glob",       2, SQLITE_NUMERIC, 0, globFunc   },
    { "nullif",     2, SQLITE_ARGS,    0, nullifFunc },
    { "sqlite_version",0,SQLITE_TEXT,  0, versionFunc},
    { "quote",      1, SQLITE_ARGS,    0, quoteFunc  },
    { "last_insert_rowid", 0, SQLITE_NUMERIC, 1, last_insert_rowid },
    { "change_count",      0, SQLITE_NUMERIC, 1, change_count      },
    { "last_statement_change_count",
                           0, SQLITE_NUMERIC, 1, last_statement_change_count },
#ifdef SQLITE_SOUNDEX
    { "soundex",    1, SQLITE_TEXT,    0, soundexFunc},
#endif
#ifdef SQLITE_TEST
    { "randstr",    2, SQLITE_TEXT,    0, randStr    },
#endif
  };
  static struct {
    char *zName;
    signed char nArg;
    signed char dataType;
    u8 argType;
    void (*xStep)(sqlite_func*,int,sqlite3_value**);
    void (*xFinalize)(sqlite_func*);
  } aAggs[] = {
    { "min",    1, 0,              0, minmaxStep,   minMaxFinalize },
    { "max",    1, 0,              2, minmaxStep,   minMaxFinalize },
    { "sum",    1, SQLITE_NUMERIC, 0, sumStep,      sumFinalize    },
    { "avg",    1, SQLITE_NUMERIC, 0, sumStep,      avgFinalize    },
    { "count",  0, SQLITE_NUMERIC, 0, countStep,    countFinalize  },
    { "count",  1, SQLITE_NUMERIC, 0, countStep,    countFinalize  },
#if 0
    { "stddev", 1, SQLITE_NUMERIC, 0, stdDevStep,   stdDevFinalize },
#endif
  };
  static const char *azTypeFuncs[] = { "min", "max", "typeof" };
  int i;

  for(i=0; i<sizeof(aFuncs)/sizeof(aFuncs[0]); i++){
    void *pArg = aFuncs[i].argType==2 ? (void*)(-1) : db;
    sqlite3_create_function(db, aFuncs[i].zName,
           aFuncs[i].nArg, aFuncs[i].xFunc, pArg);
    if( aFuncs[i].xFunc ){
      sqlite3_function_type(db, aFuncs[i].zName, aFuncs[i].dataType);
    }
  }
  for(i=0; i<sizeof(aAggs)/sizeof(aAggs[0]); i++){
    void *pArg = aAggs[i].argType==2 ? (void*)(-1) : db;
    sqlite3_create_aggregate(db, aAggs[i].zName,
           aAggs[i].nArg, aAggs[i].xStep, aAggs[i].xFinalize, pArg);
    sqlite3_function_type(db, aAggs[i].zName, aAggs[i].dataType);
  }

  for(i=0; i<sizeof(azTypeFuncs)/sizeof(azTypeFuncs[0]); i++){
    int n = strlen(azTypeFuncs[i]);
    FuncDef *p = sqlite3HashFind(&db->aFunc, azTypeFuncs[i], n);
    while( p ){
      p->includeTypes = 1;
      p = p->pNext;
    }
  }
  sqlite3RegisterDateTimeFunctions(db);
}



