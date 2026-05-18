/*
** 2013-05-28
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
** This file contains code to implement the percentile(Y,P) SQL function
** and similar as described below:
**
**   (1)  The percentile(Y,P) function is an aggregate function taking
**        exactly two arguments.
**
**   (2)  If the P argument to percentile(Y,P) is not the same for every
**        row in the aggregate then an error is thrown.  The word "same"
**        in the previous sentence means that the value differ by less
**        than 0.001.
**
**   (3)  If the P argument to percentile(Y,P) evaluates to anything other
**        than a number in the range of 0.0 to 100.0 inclusive then an
**        error is thrown.
**
**   (4)  If any Y argument to percentile(Y,P) evaluates to a value that
**        is not NULL and is not numeric then an error is thrown.
**
**   (5)  If any Y argument to percentile(Y,P) evaluates to plus or minus
**        infinity then an error is thrown.  (SQLite always interprets NaN
**        values as NULL.)
**
**   (6)  Both Y and P in percentile(Y,P) can be arbitrary expressions,
**        including CASE WHEN expressions.
**
**   (7)  The percentile(Y,P) aggregate is able to handle inputs of at least
**        one million (1,000,000) rows.
**
**   (8)  If there are no non-NULL values for Y, then percentile(Y,P)
**        returns NULL.
**
**   (9)  If there is exactly one non-NULL value for Y, the percentile(Y,P)
**        returns the one Y value.
**
**  (10)  If there N non-NULL values of Y where N is two or more and
**        the Y values are ordered from least to greatest and a graph is
**        drawn from 0 to N-1 such that the height of the graph at J is
**        the J-th Y value and such that straight lines are drawn between
**        adjacent Y values, then the percentile(Y,P) function returns
**        the height of the graph at P*(N-1)/100.
**
**  (11)  The percentile(Y,P) function always returns either a floating
**        point number or NULL.
**
**  (12)  The percentile(Y,P) is implemented as a single C99 source-code
**        file that compiles into a shared-library or DLL that can be loaded
**        into SQLite using the sqlite3_load_extension() interface.
**
**  (13)  A separate median(Y) function is the equivalent percentile(Y,50).
**
**  (14)  A separate percentile_cont(Y,P) function is equivalent to
**        percentile(Y,P/100.0).  In other words, the fraction value in
**        the second argument is in the range of 0 to 1 instead of 0 to 100.
**
**  (15)  A separate percentile_disc(Y,P) function is like
**        percentile_cont(Y,P) except that instead of returning the weighted
**        average of the nearest two input values, it returns the next lower
**        value.  So the percentile_disc(Y,P) will always return a value
**        that was one of the inputs.
**
**  (16)  All of median(), percentile(Y,P), percentile_cont(Y,P) and
**        percentile_disc(Y,P) can be used as window functions.
**
** Differences from standard SQL:
**
**  *  The percentile_cont(X,P) function is equivalent to the following in
**     standard SQL:
**
**         (percentile_cont(P) WITHIN GROUP (ORDER BY X))
**
**     The SQLite syntax is much more compact.  The standard SQL syntax
**     is also supported if SQLite is compiled with the
**     -DSQLITE_ENABLE_ORDERED_SET_AGGREGATES option.
**
**  *  No median(X) function exists in the SQL standard.  App developers
**     are expected to write "percentile_cont(0.5)WITHIN GROUP(ORDER BY X)".
**
**  *  No percentile(Y,P) function exists in the SQL standard.  Instead of
**     percential(Y,P), developers must write this:
**     "percentile_cont(P/100.0) WITHIN GROUP (ORDER BY Y)".  Note that
**     the fraction parameter to percentile() goes from 0 to 100 whereas
**     the fraction parameter in SQL standard percentile_cont() goes from
**     0 to 1.
**
** Implementation notes as of 2024-08-31:
**
**  *  The regular aggregate-function versions of these routines work
**     by accumulating all values in an array of doubles, then sorting
**     that array using quicksort before computing the answer. Thus
**     the runtime is O(NlogN) where N is the number of rows of input.
**
**  *  For the window-function versions of these routines, the array of
**     inputs is sorted as soon as the first value is computed.  Thereafter,
**     the array is kept in sorted order using an insert-sort.  This
**     results in O(N*K) performance where K is the size of the window.
**     One can imagine alternative implementations that give O(N*logN*logK)
**     performance, but they require more complex logic and data structures.
**     The developers have elected to keep the asymptotically slower
**     algorithm for now, for simplicity, under the theory that window
**     functions are seldom used and when they are, the window size K is
**     often small.  The developers might revisit that decision later,
**     should the need arise.
*/
#if defined(SQLITE3_H)
  /* no-op */
#elif defined(SQLITE_STATIC_PERCENTILE)
#  include "sqlite3.h"
#else
#  include "sqlite3ext.h"
   SQLITE_EXTENSION_INIT1
#endif
#include <assert.h>
#include <string.h>
#include <stdlib.h>

/* The following object is the group context for a single percentile()
** aggregate.  Remember all input Y values until the very end.
** Those values are accumulated in the Percentile.a[] array.
*/
typedef struct Percentile Percentile;
struct Percentile {
  unsigned nAlloc;     /* Number of slots allocated for a[] */
  unsigned nUsed;      /* Number of slots actually used in a[] */
  char bSorted;        /* True if a[] is already in sorted order */
  char bKeepSorted;    /* True if advantageous to keep a[] sorted */
  char bPctValid;      /* True if rPct is valid */
  double rPct;         /* Fraction.  0.0 to 1.0 */
  double *a;           /* Array of Y values */
};

/* Details of each function in the percentile family */
typedef struct PercentileFunc PercentileFunc;
struct PercentileFunc {
  const char *zName;   /* Function name */
  char nArg;           /* Number of arguments */
  char mxFrac;         /* Maximum value of the "fraction" input */
  char bDiscrete;      /* True for percentile_disc() */
};
static const PercentileFunc aPercentFunc[] = {
  { "median",           1,   1, 0 },
  { "percentile",       2, 100, 0 },
  { "percentile_cont",  2,   1, 0 },
  { "percentile_disc",  2,   1, 1 },
};

/*
** Return TRUE if the input floating-point number is an infinity.
*/
static int percentIsInfinity(double r){
  sqlite3_uint64 u;
  assert( sizeof(u)==sizeof(r) );
  memcpy(&u, &r, sizeof(u));
  return ((u>>52)&0x7ff)==0x7ff;
}

/*
** Return TRUE if two doubles differ by 0.001 or less.
*/
static int percentSameValue(double a, double b){
  a -= b;
  return a>=-0.001 && a<=0.001;
}

/*
** Search p (which must have p->bSorted) looking for an entry with
** value y.  Return the index of that entry.
**
** If bExact is true, return -1 if the entry is not found.
**
** If bExact is false, return the index at which a new entry with
** value y should be insert in order to keep the values in sorted
** order.  The smallest return value in this case will be 0, and
** the largest return value will be p->nUsed.
*/
static int percentBinarySearch(Percentile *p, double y, int bExact){
  int iFirst = 0;              /* First element of search range */
  int iLast = p->nUsed - 1;    /* Last element of search range */
  while( iLast>=iFirst ){
    int iMid = (iFirst+iLast)/2;
    double x = p->a[iMid];
    if( x<y ){
      iFirst = iMid + 1;
    }else if( x>y ){
      iLast = iMid - 1;
    }else{
      return iMid;
    }
  }
  if( bExact ) return -1;
  return iFirst;
}

/*
** Generate an error for a percentile function.
**
** The error format string must have exactly one occurrance of "%%s()"
** (with two '%' characters).  That substring will be replaced by the name
** of the function.
*/
static void percentError(sqlite3_context *pCtx, const char *zFormat, ...){
  PercentileFunc *pFunc = (PercentileFunc*)sqlite3_user_data(pCtx);
  char *zMsg1;
  char *zMsg2;
  va_list ap;

  va_start(ap, zFormat);
  zMsg1 = sqlite3_vmprintf(zFormat, ap);
  va_end(ap);
  zMsg2 = zMsg1 ? sqlite3_mprintf(zMsg1, pFunc->zName) : 0;
  sqlite3_result_error(pCtx, zMsg2, -1);
  sqlite3_free(zMsg1);
  sqlite3_free(zMsg2);
}

/*
** The "step" function for percentile(Y,P) is called once for each
** input row.
*/
static void percentStep(sqlite3_context *pCtx, int argc, sqlite3_value **argv){
  Percentile *p;
  double rPct;
  int eType;
  double y;
  assert( argc==2 || argc==1 );

  if( argc==1 ){
    /* Requirement 13:  median(Y) is the same as percentile(Y,50). */
    rPct = 0.5;
  }else{
    /* Requirement 3:  P must be a number between 0 and 100 */
    PercentileFunc *pFunc = (PercentileFunc*)sqlite3_user_data(pCtx);
    eType = sqlite3_value_numeric_type(argv[1]);
    rPct = sqlite3_value_double(argv[1])/(double)pFunc->mxFrac;
    if( (eType!=SQLITE_INTEGER && eType!=SQLITE_FLOAT)
     || rPct<0.0 || rPct>1.0
    ){
      percentError(pCtx, "the fraction argument to %%s()"
                        " is not between 0.0 and %.1f",
                        (double)pFunc->mxFrac);
      return;
    }
  }

  /* Allocate the session context. */
  p = (Percentile*)sqlite3_aggregate_context(pCtx, sizeof(*p));
  if( p==0 ) return;

  /* Remember the P value.  Throw an error if the P value is different
  ** from any prior row, per Requirement (2). */
  if( !p->bPctValid ){
    p->rPct = rPct;
    p->bPctValid = 1;
  }else if( !percentSameValue(p->rPct,rPct) ){
    percentError(pCtx, "the fraction argument to %%s()"
                      " is not the same for all input rows");
    return;
  }

  /* Ignore rows for which Y is NULL */
  eType = sqlite3_value_type(argv[0]);
  if( eType==SQLITE_NULL ) return;

  /* If not NULL, then Y must be numeric.  Otherwise throw an error.
  ** Requirement 4 */
  if( eType!=SQLITE_INTEGER && eType!=SQLITE_FLOAT ){
    percentError(pCtx, "input to %%s() is not numeric");
    return;
  }

  /* Throw an error if the Y value is infinity or NaN */
  y = sqlite3_value_double(argv[0]);
  if( percentIsInfinity(y) ){
    percentError(pCtx, "Inf input to %%s()");
    return;
  }

  /* Allocate and store the Y */
  if( p->nUsed>=p->nAlloc ){
    unsigned n = p->nAlloc*2 + 250;
    double *a = sqlite3_realloc64(p->a, sizeof(double)*n);
    if( a==0 ){
      sqlite3_free(p->a);
      memset(p, 0, sizeof(*p));
      sqlite3_result_error_nomem(pCtx);
      return;
    }
    p->nAlloc = n;
    p->a = a;
  }
  if( p->nUsed==0 ){
    p->a[p->nUsed++] = y;
    p->bSorted = 1;
  }else if( !p->bSorted || y>=p->a[p->nUsed-1] ){
    p->a[p->nUsed++] = y;
  }else if( p->bKeepSorted ){
    int i;
    i = percentBinarySearch(p, y, 0);
    if( i<(int)p->nUsed ){
      memmove(&p->a[i+1], &p->a[i], (p->nUsed-i)*sizeof(p->a[0]));
    }
    p->a[i] = y;
    p->nUsed++;
  }else{
    p->a[p->nUsed++] = y;
    p->bSorted = 0;
  }
}

/*
** Interchange two doubles.
*/
#define SWAP_DOUBLE(X,Y)  {double ttt=(X);(X)=(Y);(Y)=ttt;}

/*
** Sort an array of doubles.
**
** Algorithm: quicksort
**
** This is implemented separately rather than using the qsort() routine
** from the standard library because:
**
**    (1)  To avoid a dependency on qsort()
**    (2)  To avoid the function call to the comparison routine for each
**         comparison.
*/
static void percentSort(double *a, unsigned int n){
  int iLt;  /* Entries before a[iLt] are less than rPivot */
  int iGt;  /* Entries at or after a[iGt] are greater than rPivot */
  int i;         /* Loop counter */
  double rPivot; /* The pivot value */
  
  assert( n>=2 );
  if( a[0]>a[n-1] ){
    SWAP_DOUBLE(a[0],a[n-1])
  }
  if( n==2 ) return;
  iGt = n-1;
  i = n/2;
  if( a[0]>a[i] ){
    SWAP_DOUBLE(a[0],a[i])
  }else if( a[i]>a[iGt] ){
    SWAP_DOUBLE(a[i],a[iGt])
  }
  if( n==3 ) return;
  rPivot = a[i];
  iLt = i = 1;
  do{
    if( a[i]<rPivot ){
      if( i>iLt ) SWAP_DOUBLE(a[i],a[iLt])
      iLt++;
      i++;
    }else if( a[i]>rPivot ){
      do{
        iGt--;
      }while( iGt>i && a[iGt]>rPivot );
      SWAP_DOUBLE(a[i],a[iGt])
    }else{
      i++;
    }
  }while( i<iGt );
  if( iLt>=2 ) percentSort(a, iLt);
  if( n-iGt>=2 ) percentSort(a+iGt, n-iGt);
    
/* Uncomment for testing */
#if 0
  for(i=0; i<n-1; i++){
    assert( a[i]<=a[i+1] );
  }
#endif
}


/*
** The "inverse" function for percentile(Y,P) is called to remove a
** row that was previously inserted by "step".
*/
static void percentInverse(sqlite3_context *pCtx,int argc,sqlite3_value **argv){
  Percentile *p;
  int eType;
  double y;
  int i;
  assert( argc==2 || argc==1 );

  /* Allocate the session context. */
  p = (Percentile*)sqlite3_aggregate_context(pCtx, sizeof(*p));
  assert( p!=0 );

  /* Ignore rows for which Y is NULL */
  eType = sqlite3_value_type(argv[0]);
  if( eType==SQLITE_NULL ) return;

  /* If not NULL, then Y must be numeric.  Otherwise throw an error.
  ** Requirement 4 */
  if( eType!=SQLITE_INTEGER && eType!=SQLITE_FLOAT ){
    return;
  }

  /* Ignore the Y value if it is infinity or NaN */
  y = sqlite3_value_double(argv[0]);
  if( percentIsInfinity(y) ){
    return;
  }
  if( p->bSorted==0 ){
    assert( p->nUsed>1 );
    percentSort(p->a, p->nUsed);
    p->bSorted = 1;
  }
  p->bKeepSorted = 1;

  /* Find and remove the row */
  i = percentBinarySearch(p, y, 1);
  if( i>=0 ){
    p->nUsed--;
    if( i<(int)p->nUsed ){
      memmove(&p->a[i], &p->a[i+1], (p->nUsed - i)*sizeof(p->a[0]));
    }
  }
}

/*
** Compute the final output of percentile().  Clean up all allocated
** memory if and only if bIsFinal is true.
*/
static void percentCompute(sqlite3_context *pCtx, int bIsFinal){
  Percentile *p;
  PercentileFunc *pFunc = (PercentileFunc*)sqlite3_user_data(pCtx);
  unsigned i1, i2;
  double v1, v2;
  double ix, vx;
  p = (Percentile*)sqlite3_aggregate_context(pCtx, 0);
  if( p==0 ) return;
  if( p->a==0 ) return;
  if( p->nUsed ){
    if( p->bSorted==0 ){
      assert( p->nUsed>1 );
      percentSort(p->a, p->nUsed);
      p->bSorted = 1;
    }
    ix = p->rPct*(p->nUsed-1);
    i1 = (unsigned)ix;
    if( pFunc->bDiscrete ){
      vx = p->a[i1];
    }else{
      i2 = ix==(double)i1 || i1==p->nUsed-1 ? i1 : i1+1;
      v1 = p->a[i1];
      v2 = p->a[i2];
      vx = v1 + (v2-v1)*(ix-i1);
    }
    sqlite3_result_double(pCtx, vx);
  }
  if( bIsFinal ){
    sqlite3_free(p->a);
    memset(p, 0, sizeof(*p));
  }else{
    p->bKeepSorted = 1;
  }
}
static void percentFinal(sqlite3_context *pCtx){
  percentCompute(pCtx, 1);
}
static void percentValue(sqlite3_context *pCtx){
  percentCompute(pCtx, 0);
}

#if defined(_WIN32) && !defined(SQLITE3_H) && !defined(SQLITE_STATIC_PERCENTILE)
__declspec(dllexport)
#endif
int sqlite3_percentile_init(
  sqlite3 *db, 
  char **pzErrMsg, 
  const sqlite3_api_routines *pApi
){
  int rc = SQLITE_OK;
  unsigned int i;
#ifdef SQLITE3EXT_H
  SQLITE_EXTENSION_INIT2(pApi);
#else
  (void)pApi;      /* Unused parameter */
#endif
  (void)pzErrMsg;  /* Unused parameter */
  for(i=0; i<sizeof(aPercentFunc)/sizeof(aPercentFunc[0]); i++){
    rc = sqlite3_create_window_function(db,
            aPercentFunc[i].zName,
            aPercentFunc[i].nArg,
            SQLITE_UTF8|SQLITE_INNOCUOUS|SQLITE_SELFORDER1,
            (void*)&aPercentFunc[i],
            percentStep, percentFinal, percentValue, percentInverse, 0);
    if( rc ) break;
  }
  return rc;
}
