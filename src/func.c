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
** $Id: func.c,v 1.29 2003/08/20 01:03:34 drh Exp $
*/
#include <ctype.h>
#include <math.h>
#include <stdlib.h>
#include <assert.h>
#include "sqliteInt.h"
#include "os.h"

/*
** Implementation of the non-aggregate min() and max() functions
*/
static void minFunc(sqlite_func *context, int argc, const char **argv){
  const char *zBest; 
  int i;

  if( argc==0 ) return;
  zBest = argv[0];
  if( zBest==0 ) return;
  for(i=1; i<argc; i++){
    if( argv[i]==0 ) return;
    if( sqliteCompare(argv[i], zBest)<0 ){
      zBest = argv[i];
    }
  }
  sqlite_set_result_string(context, zBest, -1);
}
static void maxFunc(sqlite_func *context, int argc, const char **argv){
  const char *zBest; 
  int i;

  if( argc==0 ) return;
  zBest = argv[0];
  if( zBest==0 ) return;
  for(i=1; i<argc; i++){
    if( argv[i]==0 ) return;
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
  if( z==0 ) return;
#ifdef SQLITE_UTF8
  for(len=0; *z; z++){ if( (0xc0&*z)!=0x80 ) len++; }
#else
  len = strlen(z);
#endif
  sqlite_set_result_int(context, len);
}

/*
** Implementation of the abs() function
*/
static void absFunc(sqlite_func *context, int argc, const char **argv){
  const char *z;
  assert( argc==1 );
  z = argv[0];
  if( z==0 ) return;
  if( z[0]=='-' && isdigit(z[1]) ) z++;
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
  for(i=0; i<p1; i++){
    assert( z[i] );
    if( (z[i]&0xc0)==0x80 ) p1++;
  }
  while( z[i] && (z[i]&0xc0)==0x80 ){ i++; p1++; }
  for(; i<p1+p2; i++){
    assert( z[i] );
    if( (z[i]&0xc0)==0x80 ) p2++;
  }
  while( z[i] && (z[i]&0xc0)==0x80 ){ i++; p2++; }
#endif
  if( p2<0 ) p2 = 0;
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
  if( argv[0]==0 || (argc==2 && argv[1]==0) ) return;
  n = argc==2 ? atoi(argv[1]) : 0;
  if( n>30 ) n = 30;
  if( n<0 ) n = 0;
  r = atof(argv[0]);
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
** Implementation of the IFNULL(), NVL(), and COALESCE() functions.  
** All three do the same thing.  They return the first argument
** non-NULL argument.
*/
static void ifnullFunc(sqlite_func *context, int argc, const char **argv){
  int i;
  for(i=0; i<argc; i++){
    if( argv[i] ){
      sqlite_set_result_string(context, argv[i], -1);
      break;
    }
  }
}

/*
** Implementation of random().  Return a random integer.  
*/
static void randomFunc(sqlite_func *context, int argc, const char **argv){
  sqlite_set_result_int(context, sqliteRandomInteger());
}

/*
** Implementation of the last_insert_rowid() SQL function.  The return
** value is the same as the sqlite_last_insert_rowid() API function.
*/
static void last_insert_rowid(sqlite_func *context, int arg, const char **argv){
  sqlite *db = sqlite_user_data(context);
  sqlite_set_result_int(context, sqlite_last_insert_rowid(db));
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
static void likeFunc(sqlite_func *context, int arg, const char **argv){
  if( argv[0]==0 || argv[1]==0 ) return;
  sqlite_set_result_int(context, sqliteLikeCompare(argv[0], argv[1]));
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
static void globFunc(sqlite_func *context, int arg, const char **argv){
  if( argv[0]==0 || argv[1]==0 ) return;
  sqlite_set_result_int(context, sqliteGlobCompare(argv[0], argv[1]));
}

/*
** Implementation of the NULLIF(x,y) function.  The result is the first
** argument if the arguments are different.  The result is NULL if the
** arguments are equal to each other.
*/
static void nullifFunc(sqlite_func *context, int argc, const char **argv){
  if( argv[0]!=0 && sqliteCompare(argv[0],argv[1])!=0 ){
    sqlite_set_result_string(context, argv[0], -1);
  }
}

/*
** Implementation of the VERSION(*) function.  The result is the version
** of the SQLite library that is running.
*/
static void versionFunc(sqlite_func *context, int argc, const char **argv){
  sqlite_set_result_string(context, sqlite_version, -1);
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
static void quoteFunc(sqlite_func *context, int argc, const char **argv){
  if( argc<1 ) return;
  if( argv[0]==0 ){
    sqlite_set_result_string(context, "NULL", 4);
  }else if( sqliteIsNumber(argv[0]) ){
    sqlite_set_result_string(context, argv[0], -1);
  }else{
    int i,j,n;
    char *z;
    for(i=n=0; argv[0][i]; i++){ if( argv[0][i]=='\'' ) n++; }
    z = sqliteMalloc( i+n+3 );
    if( z==0 ) return;
    z[0] = '\'';
    for(i=0, j=1; argv[0][i]; i++){
      z[j++] = argv[0][i];
      if( argv[0][i]=='\'' ){
        z[j++] = '\'';
      }
    }
    z[j++] = '\'';
    z[j] = 0;
    sqlite_set_result_string(context, z, j);
    sqliteFree(z);
  }
}

#ifdef SQLITE_SOUNDEX
/*
** Compute the soundex encoding of a word.
*/
static void soundexFunc(sqlite_func *context, int argc, const char **argv){
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
  zIn = argv[0];
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
    sqlite_set_result_string(context, zResult, 4);
  }else{
    sqlite_set_result_string(context, "?000", 4);
  }
}
#endif

#ifdef SQLITE_TEST
/*
** This function generates a string of random characters.  Used for
** generating test data.
*/
static void randStr(sqlite_func *context, int argc, const char **argv){
  static const char zSrc[] = 
     "abcdefghijklmnopqrstuvwxyz"
     "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
     "0123456789"
     ".-!,:*^+=_|?/<> ";
  int iMin, iMax, n, r, i;
  char zBuf[1000];
  if( argc>=1 ){
    iMin = atoi(argv[0]);
    if( iMin<0 ) iMin = 0;
    if( iMin>=sizeof(zBuf) ) iMin = sizeof(zBuf)-1;
  }else{
    iMin = 1;
  }
  if( argc>=2 ){
    iMax = atoi(argv[1]);
    if( iMax<iMin ) iMax = iMin;
    if( iMax>=sizeof(zBuf) ) iMax = sizeof(zBuf);
  }else{
    iMax = 50;
  }
  n = iMin;
  if( iMax>iMin ){
    r = sqliteRandomInteger() & 0x7fffffff;
    n += r%(iMax + 1 - iMin);
  }
  r = 0;
  for(i=0; i<n; i++){
    r = (r + sqliteRandomByte())% (sizeof(zSrc)-1);
    zBuf[i] = zSrc[r];
  }
  zBuf[n] = 0;
  sqlite_set_result_string(context, zBuf, n);
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
static void sumStep(sqlite_func *context, int argc, const char **argv){
  SumCtx *p;
  if( argc<1 ) return;
  p = sqlite_aggregate_context(context, sizeof(*p));
  if( p && argv[0] ){
    p->sum += atof(argv[0]);
    p->cnt++;
  }
}
static void sumFinalize(sqlite_func *context){
  SumCtx *p;
  p = sqlite_aggregate_context(context, sizeof(*p));
  sqlite_set_result_double(context, p ? p->sum : 0.0);
}
static void avgFinalize(sqlite_func *context){
  SumCtx *p;
  p = sqlite_aggregate_context(context, sizeof(*p));
  if( p && p->cnt>0 ){
    sqlite_set_result_double(context, p->sum/(double)p->cnt);
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
  p = sqlite_aggregate_context(context, sizeof(*p));
  if( p && argv[0] ){
    x = atof(argv[0]);
    p->sum += x;
    p->sum2 += x*x;
    p->cnt++;
  }
}
static void stdDevFinalize(sqlite_func *context){
  double rN = sqlite_aggregate_count(context);
  StdDevCtx *p = sqlite_aggregate_context(context, sizeof(*p));
  if( p && p->cnt>1 ){
    double rCnt = cnt;
    sqlite_set_result_double(context, 
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
  if( p==0 || argc<1 || argv[0]==0 ) return;
  if( p->z==0 || sqliteCompare(argv[0],p->z)<0 ){
    int len;
    if( p->z && p->z!=p->zBuf ){
      sqliteFree(p->z);
    }
    len = strlen(argv[0]);
    if( len < sizeof(p->zBuf) ){
      p->z = p->zBuf;
    }else{
      p->z = sqliteMalloc( len+1 );
      if( p->z==0 ) return;
    }
    strcpy(p->z, argv[0]);
  }
}
static void maxStep(sqlite_func *context, int argc, const char **argv){
  MinMaxCtx *p;
  p = sqlite_aggregate_context(context, sizeof(*p));
  if( p==0 || argc<1 || argv[0]==0 ) return;
  if( p->z==0 || sqliteCompare(argv[0],p->z)>0 ){
    int len;
    if( p->z && p->z!=p->zBuf ){
      sqliteFree(p->z);
    }
    len = strlen(argv[0]);
    if( len < sizeof(p->zBuf) ){
      p->z = p->zBuf;
    }else{
      p->z = sqliteMalloc( len+1 );
      if( p->z==0 ) return;
    }
    strcpy(p->z, argv[0]);
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

/****************************************************************************
** Time and date functions.
**
** SQLite processes all times and dates as Julian Day numbers.  The
** dates and times are stored as the number of days since noon
** in Greenwich on November 24, 4714 B.C. according to the Gregorian
** calendar system.
**
** This implement requires years to be expressed as a 4-digit number
** which means that only dates between 0000-01-01 and 9999-12-31 can
** be represented, even though julian day numbers allow a much wider
** range of dates.
**
** The Gregorian calendar system is used for all dates and times,
** even those that predate the Gregorian calendar.  Historians usually
** use the Julian calendar for dates prior to 1582-10-15 and for some
** dates afterwards, depending on locale.  Beware of this difference.
**
** The conversion algorithms are implemented based on descriptions
** in the following text:
**
**      Jean Meeus
**      Astronomical Algorithms, 2nd Edition, 1998
**      ISBM 0-943396-61-1
**      Willmann-Bell, Inc
**      Richmond, Virginia (USA)
*/
#ifndef SQLITE_OMIT_DATETIME_FUNCS

/*
** Convert N digits from zDate into an integer.  Return
** -1 if zDate does not begin with N digits.
*/
static int getDigits(const char *zDate, int N){
  int val = 0;
  while( N-- ){
    if( !isdigit(*zDate) ) return -1;
    val = val*10 + *zDate - '0';
    zDate++;
  }
  return val;
}

/*
** Parse times of the form HH:MM:SS or HH:MM.  Store the
** result (in days) in *prJD.
**
** Return 1 if there is a parsing error and 0 on success.
*/
static int parseHhMmSs(const char *zDate, double *prJD){
  int h, m, s;
  h = getDigits(zDate, 2);
  if( h<0 || zDate[2]!=':' ) return 1;
  zDate += 3;
  m = getDigits(zDate, 2);
  if( m<0 || m>59 ) return 1;
  zDate += 2;
  if( *zDate==':' ){
    s = getDigits(&zDate[1], 2);
    if( s<0 || s>59 ) return 1;
    zDate += 3;
  }else{
    s = 0;
  }
  while( isspace(*zDate) ){ zDate++; }
  *prJD = (h*3600.0 + m*60.0 + s)/86400.0;
  return 0;
}

/*
** Parse dates of the form
**
**     YYYY-MM-DD HH:MM:SS
**     YYYY-MM-DD HH:MM
**     YYYY-MM-DD
**
** Write the result as a julian day number in *prJD.  Return 0
** on success and 1 if the input string is not a well-formed
** date.
*/
static int parseYyyyMmDd(const char *zDate, double *prJD){
  int Y, M, D;
  double rTime;
  int A, B, X1, X2;

  Y = getDigits(zDate, 4);
  if( Y<0 || zDate[4]!='-' ) return 1;
  zDate += 5;
  M = getDigits(zDate, 2);
  if( M<=0 || M>12 || zDate[2]!='-' ) return 1;
  zDate += 3;
  D = getDigits(zDate, 2);
  if( D<=0 || D>31 ) return 1;
  zDate += 2;
  while( isspace(*zDate) ){ zDate++; }
  if( isdigit(*zDate) ){
    if( parseHhMmSs(zDate, &rTime) ) return 1;
  }else if( *zDate==0 ){
    rTime = 0.0;
  }else{ 
    return 1;
  }

  /* The year, month, and day are now stored in Y, M, and D.  Convert
  ** these into the Julian Day number.  See Meeus page 61.
  */
  if( M<=2 ){
    Y--;
    M += 12;
  }
  A = Y/100;
  B = 2 - A + (A/4);
  X1 = 365.25*(Y+4716);
  X2 = 30.6001*(M+1);
  *prJD = X1 + X2 + D + B - 1524.5 + rTime;
  return 0;
}

/*
** Attempt to parse the given string into a Julian Day Number.  Return
** the number of errors.
**
** The following are acceptable forms for the input string:
**
**      YYYY-MM-DD
**      YYYY-MM-DD HH:MM
**      YYYY-MM-DD HH:MM:SS
**      HH:MM
**      HH:MM:SS
**      DDDD.DD 
**      now
*/
static int parseDateOrTime(const char *zDate, double *prJD){
  int i;
  for(i=0; isdigit(zDate[i]); i++){}
  if( i==4 && zDate[i]=='-' ){
    return parseYyyyMmDd(zDate, prJD);
  }else if( i==2 && zDate[i]==':' ){
    return parseHhMmSs(zDate, prJD);
  }else if( i==0 && sqliteStrICmp(zDate,"now")==0 ){
    return sqliteOsCurrentTime(prJD);
  }else if( sqliteIsNumber(zDate) ){
    *prJD = atof(zDate);
    return 0;
  }
  return 1;
}

/*
** A structure for holding date and time.
*/
typedef struct DateTime DateTime;
struct DateTime {
  double rJD;    /* The julian day number */
  int Y, M, D;   /* Year, month, and day */
  int h, m, s;   /* Hour minute and second */
};

/*
** Break up a julian day number into year, month, day, hour, minute, second.
** This function assume the Gregorian calendar - even for dates prior
** to the invention of the Gregorian calendar in 1582.
**
** See Meeus page 63.
**
** If mode==1 only the year, month, and day are computed.  If mode==2
** then only the hour, minute, and second are computed.  If mode==3 then
** everything is computed.  If mode==0, this routine is a no-op.
*/
static void decomposeDate(DateTime *p, int mode){
  int Z;
  Z = p->rJD + 0.5;
  if( mode & 1 ){
    int A, B, C, D, E, X1;
    A = (Z - 1867216.25)/36524.25;
    A = Z + 1 + A - (A/4);
    B = A + 1524;
    C = (B - 122.1)/365.25;
    D = 365.25*C;
    E = (B-D)/30.6001;
    X1 = 30.6001*E;
    p->D = B - D - X1;
    p->M = E<14 ? E-1 : E-13;
    p->Y = p->M>2 ? C - 4716 : C - 4715;
  }
  if( mode & 2 ){
    p->s = (p->rJD + 0.5 - Z)*86400.0;
    p->h = p->s/3600;
    p->s -= p->h*3600;
    p->m = p->s/60;
    p->s -= p->m*60;
  }
}

/*
** Check to see that all arguments are valid date strings.  If any 
** argument is not a valid date string, return 0.  If all arguments
** are valid, return 1 and write into *p->rJD the sum of the julian day
** numbers for all date strings.
**
** A "valid" date string is one that is accepted by parseDateOrTime().
**
** The mode argument is passed through to decomposeDate() in order to
** fill in the year, month, day, hour, minute, and second of the *p
** structure, if desired.
*/
static int isDate(int argc, const char **argv, DateTime *p, int mode){
  double r;
  int i;
  p->rJD = 0.0;
  for(i=0; i<argc; i++){
    if( argv[i]==0 ) return 0;
    if( parseDateOrTime(argv[i], &r) ) return 0;
    p->rJD += r;
  }
  decomposeDate(p, mode);
  return 1;
}


/*
** The following routines implement the various date and time functions
** of SQLite.
*/
static void juliandayFunc(sqlite_func *context, int argc, const char **argv){
  DateTime x;
  if( isDate(argc, argv, &x, 0) ){
    sqlite_set_result_double(context, x.rJD);
  }
}
static void timestampFunc(sqlite_func *context, int argc, const char **argv){
  DateTime x;
  if( isDate(argc, argv, &x, 3) ){
    char zBuf[100];
    sprintf(zBuf, "%04d-%02d-%02d %02d:%02d:%02d",x.Y, x.M, x.D, x.h, x.m, x.s);
    sqlite_set_result_string(context, zBuf, -1);
  }
}
static void timeFunc(sqlite_func *context, int argc, const char **argv){
  DateTime x;
  if( isDate(argc, argv, &x, 2) ){
    char zBuf[100];
    sprintf(zBuf, "%02d:%02d:%02d", x.h, x.m, x.s);
    sqlite_set_result_string(context, zBuf, -1);
  }
}
static void dateFunc(sqlite_func *context, int argc, const char **argv){
  DateTime x;
  if( isDate(argc, argv, &x, 1) ){
    char zBuf[100];
    sprintf(zBuf, "%04d-%02d-%02d", x.Y, x.M, x.D);
    sqlite_set_result_string(context, zBuf, -1);
  }
}
static void yearFunc(sqlite_func *context, int argc, const char **argv){
  DateTime x;
  if( isDate(argc, argv, &x, 1) ){
    sqlite_set_result_int(context, x.Y);
  }
}
static void monthFunc(sqlite_func *context, int argc, const char **argv){
  DateTime x;
  if( isDate(argc, argv, &x, 1) ){
    sqlite_set_result_int(context, x.M);
  }
}
static void dayofweekFunc(sqlite_func *context, int argc, const char **argv){
  DateTime x;
  if( isDate(argc, argv, &x, 0) ){
    int Z = x.rJD + 1.5;
    sqlite_set_result_int(context, Z % 7);
  }
}
static void dayofmonthFunc(sqlite_func *context, int argc, const char **argv){
  DateTime x;
  if( isDate(argc, argv, &x, 1) ){
    sqlite_set_result_int(context, x.D);
  }
}
static void secondFunc(sqlite_func *context, int argc, const char **argv){
  DateTime x;
  if( isDate(argc, argv, &x, 2) ){
    sqlite_set_result_int(context, x.s);
  }
}
static void minuteFunc(sqlite_func *context, int argc, const char **argv){
  DateTime x;
  if( isDate(argc, argv, &x, 2) ){
    sqlite_set_result_int(context, x.m);
  }
}
static void hourFunc(sqlite_func *context, int argc, const char **argv){
  DateTime x;
  if( isDate(argc, argv, &x, 2) ){
    sqlite_set_result_int(context, x.h);
  }
}
#endif /* !defined(SQLITE_OMIT_DATETIME_FUNCS) */
/***************************************************************************/

/*
** This function registered all of the above C functions as SQL
** functions.  This should be the only routine in this file with
** external linkage.
*/
void sqliteRegisterBuiltinFunctions(sqlite *db){
  static struct {
     char *zName;
     int nArg;
     int dataType;
     void (*xFunc)(sqlite_func*,int,const char**);
  } aFuncs[] = {
    { "min",       -1, SQLITE_ARGS,    minFunc    },
    { "min",        0, 0,              0          },
    { "max",       -1, SQLITE_ARGS,    maxFunc    },
    { "max",        0, 0,              0          },
    { "length",     1, SQLITE_NUMERIC, lengthFunc },
    { "substr",     3, SQLITE_TEXT,    substrFunc },
    { "abs",        1, SQLITE_NUMERIC, absFunc    },
    { "round",      1, SQLITE_NUMERIC, roundFunc  },
    { "round",      2, SQLITE_NUMERIC, roundFunc  },
    { "upper",      1, SQLITE_TEXT,    upperFunc  },
    { "lower",      1, SQLITE_TEXT,    lowerFunc  },
    { "coalesce",  -1, SQLITE_ARGS,    ifnullFunc },
    { "coalesce",   0, 0,              0          },
    { "coalesce",   1, 0,              0          },
    { "ifnull",     2, SQLITE_ARGS,    ifnullFunc },
    { "random",    -1, SQLITE_NUMERIC, randomFunc },
    { "like",       2, SQLITE_NUMERIC, likeFunc   },
    { "glob",       2, SQLITE_NUMERIC, globFunc   },
    { "nullif",     2, SQLITE_ARGS,    nullifFunc },
    { "sqlite_version",0,SQLITE_TEXT,  versionFunc},
    { "quote",      1, SQLITE_ARGS,    quoteFunc  },
#ifndef SQLITE_OMIT_DATETIME_FUNCS
    { "julianday", -1, SQLITE_NUMERIC, juliandayFunc   },
    { "timestamp", -1, SQLITE_TEXT,    timestampFunc   },
    { "time",      -1, SQLITE_TEXT,    timeFunc        },
    { "date",      -1, SQLITE_TEXT,    dateFunc        },
    { "year",      -1, SQLITE_NUMERIC, yearFunc        },
    { "month",     -1, SQLITE_NUMERIC, monthFunc       },
    { "dayofmonth",-1, SQLITE_NUMERIC, dayofmonthFunc  },
    { "dayofweek", -1, SQLITE_NUMERIC, dayofweekFunc   },
    { "hour",      -1, SQLITE_NUMERIC, hourFunc        },
    { "minute",    -1, SQLITE_NUMERIC, minuteFunc      },
    { "second",    -1, SQLITE_NUMERIC, secondFunc      },
#endif
#ifdef SQLITE_SOUNDEX
    { "soundex",    1, SQLITE_TEXT,    soundexFunc},
#endif
#ifdef SQLITE_TEST
    { "randstr",    2, SQLITE_TEXT,    randStr    },
#endif
  };
  static struct {
    char *zName;
    int nArg;
    int dataType;
    void (*xStep)(sqlite_func*,int,const char**);
    void (*xFinalize)(sqlite_func*);
  } aAggs[] = {
    { "min",    1, 0,              minStep,      minMaxFinalize },
    { "max",    1, 0,              maxStep,      minMaxFinalize },
    { "sum",    1, SQLITE_NUMERIC, sumStep,      sumFinalize    },
    { "avg",    1, SQLITE_NUMERIC, sumStep,      avgFinalize    },
    { "count",  0, SQLITE_NUMERIC, countStep,    countFinalize  },
    { "count",  1, SQLITE_NUMERIC, countStep,    countFinalize  },
#if 0
    { "stddev", 1, SQLITE_NUMERIC, stdDevStep,   stdDevFinalize },
#endif
  };
  int i;

  for(i=0; i<sizeof(aFuncs)/sizeof(aFuncs[0]); i++){
    sqlite_create_function(db, aFuncs[i].zName,
           aFuncs[i].nArg, aFuncs[i].xFunc, 0);
    if( aFuncs[i].xFunc ){
      sqlite_function_type(db, aFuncs[i].zName, aFuncs[i].dataType);
    }
  }
  sqlite_create_function(db, "last_insert_rowid", 0, 
           last_insert_rowid, db);
  sqlite_function_type(db, "last_insert_rowid", SQLITE_NUMERIC);
  for(i=0; i<sizeof(aAggs)/sizeof(aAggs[0]); i++){
    sqlite_create_aggregate(db, aAggs[i].zName,
           aAggs[i].nArg, aAggs[i].xStep, aAggs[i].xFinalize, 0);
    sqlite_function_type(db, aAggs[i].zName, aAggs[i].dataType);
  }
}
