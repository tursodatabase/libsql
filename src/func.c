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
** $Id: func.c,v 1.76 2004/06/24 00:20:05 danielk1977 Exp $
*/
#include <ctype.h>
#include <math.h>
#include <stdlib.h>
#include <assert.h>
#include "sqliteInt.h"
#include "vdbeInt.h"
#include "os.h"

static CollSeq *sqlite3GetFuncCollSeq(sqlite3_context *context){
  return context->pColl;
}

/*
** Implementation of the non-aggregate min() and max() functions
*/
static void minmaxFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  int i;
  int mask;    /* 0 for min() or 0xffffffff for max() */
  int iBest;
  CollSeq *pColl;

  if( argc==0 ) return;
  mask = (int)sqlite3_user_data(context);
  pColl = sqlite3GetFuncCollSeq(context);
  assert( pColl );
  assert( mask==-1 || mask==0 );
  iBest = 0;
  if( sqlite3_value_type(argv[0])==SQLITE_NULL ) return;
  for(i=1; i<argc; i++){
    if( sqlite3_value_type(argv[i])==SQLITE_NULL ) return;
    if( (sqlite3MemCompare(argv[iBest], argv[i], pColl)^mask)>=0 ){
      iBest = i;
    }
  }
  sqlite3_result_value(context, argv[iBest]);
}

/*
** Return the type of the argument.
*/
static void typeofFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  const char *z = 0;
  switch( sqlite3_value_type(argv[0]) ){
    case SQLITE_NULL:    z = "null";    break;
    case SQLITE_INTEGER: z = "integer"; break;
    case SQLITE_TEXT:    z = "text";    break;
    case SQLITE_FLOAT:   z = "real";    break;
    case SQLITE_BLOB:    z = "blob";    break;
  }
  sqlite3_result_text(context, z, -1, SQLITE_STATIC);
}

/*
** Implementation of the length() function
*/
static void lengthFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  int len;

  assert( argc==1 );
  switch( sqlite3_value_type(argv[0]) ){
    case SQLITE_BLOB:
    case SQLITE_INTEGER:
    case SQLITE_FLOAT: {
      sqlite3_result_int(context, sqlite3_value_bytes(argv[0]));
      break;
    }
    case SQLITE_TEXT: {
      const char *z = sqlite3_value_text(argv[0]);
      for(len=0; *z; z++){ if( (0xc0&*z)!=0x80 ) len++; }
      sqlite3_result_int(context, len);
      break;
    }
    default: {
      sqlite3_result_null(context);
      break;
    }
  }
}

/*
** Implementation of the abs() function
*/
static void absFunc(sqlite3_context *context, int argc, sqlite3_value **argv){
  assert( argc==1 );
  switch( sqlite3_value_type(argv[0]) ){
    case SQLITE_INTEGER: {
      i64 iVal = sqlite3_value_int64(argv[0]);
      if( iVal<0 ) iVal = iVal * -1;
      sqlite3_result_int64(context, iVal);
      break;
    }
    case SQLITE_NULL: {
      sqlite3_result_null(context);
      break;
    }
    default: {
      double rVal = sqlite3_value_double(argv[0]);
      if( rVal<0 ) rVal = rVal * -1.0;
      sqlite3_result_double(context, rVal);
      break;
    }
  }
}

/*
** Implementation of the substr() function
*/
static void substrFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  const char *z;
  const char *z2;
  int i;
  int p1, p2, len;

  assert( argc==3 );
  z = sqlite3_value_text(argv[0]);
  if( z==0 ) return;
  p1 = sqlite3_value_int(argv[1]);
  p2 = sqlite3_value_int(argv[2]);
  for(len=0, z2=z; *z2; z2++){ if( (0xc0&*z2)!=0x80 ) len++; }
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
  for(i=0; i<p1 && z[i]; i++){
    if( (z[i]&0xc0)==0x80 ) p1++;
  }
  while( z[i] && (z[i]&0xc0)==0x80 ){ i++; p1++; }
  for(; i<p1+p2 && z[i]; i++){
    if( (z[i]&0xc0)==0x80 ) p2++;
  }
  while( z[i] && (z[i]&0xc0)==0x80 ){ i++; p2++; }
  if( p2<0 ) p2 = 0;
  sqlite3_result_text(context, &z[p1], p2, SQLITE_TRANSIENT);
}

/*
** Implementation of the round() function
*/
static void roundFunc(sqlite3_context *context, int argc, sqlite3_value **argv){
  int n = 0;
  double r;
  char zBuf[100];
  assert( argc==1 || argc==2 );
  if( argc==2 ){
    if( SQLITE_NULL==sqlite3_value_type(argv[1]) ) return;
    n = sqlite3_value_int(argv[1]);
    if( n>30 ) n = 30;
    if( n<0 ) n = 0;
  }
  if( SQLITE_NULL==sqlite3_value_type(argv[0]) ) return;
  r = sqlite3_value_double(argv[0]);
  sprintf(zBuf,"%.*f",n,r);
  sqlite3_result_text(context, zBuf, -1, SQLITE_TRANSIENT);
}

/*
** Implementation of the upper() and lower() SQL functions.
*/
static void upperFunc(sqlite3_context *context, int argc, sqlite3_value **argv){
  char *z;
  int i;
  if( argc<1 || SQLITE_NULL==sqlite3_value_type(argv[0]) ) return;
  z = sqliteMalloc(sqlite3_value_bytes(argv[0])+1);
  if( z==0 ) return;
  strcpy(z, sqlite3_value_text(argv[0]));
  for(i=0; z[i]; i++){
    if( islower(z[i]) ) z[i] = toupper(z[i]);
  }
  sqlite3_result_text(context, z, -1, SQLITE_TRANSIENT);
  sqliteFree(z);
}
static void lowerFunc(sqlite3_context *context, int argc, sqlite3_value **argv){
  char *z;
  int i;
  if( argc<1 || SQLITE_NULL==sqlite3_value_type(argv[0]) ) return;
  z = sqliteMalloc(sqlite3_value_bytes(argv[0])+1);
  if( z==0 ) return;
  strcpy(z, sqlite3_value_text(argv[0]));
  for(i=0; z[i]; i++){
    if( isupper(z[i]) ) z[i] = tolower(z[i]);
  }
  sqlite3_result_text(context, z, -1, SQLITE_TRANSIENT);
  sqliteFree(z);
}

/*
** Implementation of the IFNULL(), NVL(), and COALESCE() functions.  
** All three do the same thing.  They return the first non-NULL
** argument.
*/
static void ifnullFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  int i;
  for(i=0; i<argc; i++){
    if( SQLITE_NULL!=sqlite3_value_type(argv[i]) ){
      sqlite3_result_value(context, argv[i]);
      break;
    }
  }
}

/*
** Implementation of random().  Return a random integer.  
*/
static void randomFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  int r;
  sqlite3Randomness(sizeof(r), &r);
  sqlite3_result_int(context, r);
}

/*
** Implementation of the last_insert_rowid() SQL function.  The return
** value is the same as the sqlite3_last_insert_rowid() API function.
*/
static void last_insert_rowid(
  sqlite3_context *context, 
  int arg, 
  sqlite3_value **argv
){
  sqlite *db = sqlite3_user_data(context);
  sqlite3_result_int64(context, sqlite3_last_insert_rowid(db));
}

/*
** Implementation of the changes() SQL function.  The return value is the
** same as the sqlite3_changes() API function.
*/
static void changes(
  sqlite3_context *context,
  int arg,
  sqlite3_value **argv
){
  sqlite *db = sqlite3_user_data(context);
  sqlite3_result_int(context, sqlite3_changes(db));
}

/*
** Implementation of the total_changes() SQL function.  The return value is
** the same as the sqlite3_total_changes() API function.
*/
static void total_changes(
  sqlite3_context *context,
  int arg,
  sqlite3_value **argv
){
  sqlite *db = sqlite3_user_data(context);
  sqlite3_result_int(context, sqlite3_total_changes(db));
}

#if 0

/*
** A LIKE pattern compiles to an instance of the following structure. Refer
** to the comment for compileLike() function for details.
*/
struct LikePattern {
  int nState;
  struct LikeState {
    int val;           /* Unicode codepoint or -1 for any char i.e. '_' */
    int failstate;     /* State to jump to if next char is not val */
  } aState[1];
};
typedef struct LikePattern LikePattern;

void deleteLike(void *pLike){
  sqliteFree(pLike);
}
/* #define TRACE_LIKE */
#if defined(TRACE_LIKE) && !defined(NDEBUG)
char *dumpLike(LikePattern *pLike){
  int i;
  int k = 0;
  char *zBuf = (char *)sqliteMalloc(pLike->nState*40);
  
  k += sprintf(&zBuf[k], "%d states - ", pLike->nState);
  for(i=0; i<pLike->nState; i++){
    k += sprintf(&zBuf[k], " %d:(%d, %d)", i, pLike->aState[i].val,
        pLike->aState[i].failstate);
  }
  return zBuf;
}
#endif

/*
** This function compiles an SQL 'LIKE' pattern into a state machine, 
** represented by a LikePattern structure.
**
** Each state of the state-machine has two attributes, 'val' and
** 'failstate'. The val attribute is either the value of a unicode 
** codepoint, or -1, indicating a '_' wildcard (match any single
** character). The failstate is either the number of another state
** or -1, indicating jump to 'no match'.
**
** To see if a string matches a pattern the pattern is
** compiled to a state machine that is executed according to the algorithm
** below. The string is assumed to be terminated by a 'NUL' character
** (unicode codepoint 0).
**
** 1   S = 0
** 2   DO 
** 3       C = <Next character from input string>
** 4       IF( C matches <State S val> )
** 5           S = S+1
** 6       ELSE IF( S != <State S failstate> )
** 7           S = <State S failstate>
** 8           <Rewind Input string 1 character>
** 9   WHILE( (C != NUL) AND (S != FAILED) )
** 10
** 11  IF( S == <number of states> )
** 12      RETURN MATCH
** 13  ELSE
** 14      RETURN NO-MATCH
**       
** In practice there is a small optimization to avoid the <Rewind>
** operation in line 8 of the description above.
**
** For example, the following pattern, 'X%ABabc%_Y' is compiled to
** the state machine below.
**
** State    Val          FailState
** -------------------------------
** 0        120 (x)      -1 (NO MATCH)
** 1        97  (a)      1
** 2        98  (b)      1
** 3        97  (a)      1
** 4        98  (b)      2
** 5        99  (c)      3
** 6        -1  (_)      6
** 7        121 (y)      7
** 8        0   (NUL)    7
**
** The algorithms implemented to compile and execute the state machine were
** first presented in "Fast pattern matching in strings", Knuth, Morris and
** Pratt, 1977.
**       
*/
LikePattern *compileLike(sqlite3_value *pPattern, u8 enc){
  LikePattern *pLike;
  struct LikeState *aState;
  int pc_state = -1;    /* State number of previous '%' wild card */
  int n = 0;
  int c;

  int offset = 0;
  const char *zLike;
 
  if( enc==SQLITE_UTF8 ){
    zLike = sqlite3_value_text(pPattern);
    n = sqlite3_value_bytes(pPattern) + 1;
  }else{
    zLike = sqlite3_value_text16(pPattern);
    n = sqlite3_value_bytes16(pPattern)/2 + 1;
  }

  pLike = (LikePattern *)
      sqliteMalloc(sizeof(LikePattern)+n*sizeof(struct LikeState));
  aState = pLike->aState;

  n = 0;
  do {
    c = sqlite3ReadUniChar(zLike, &offset, &enc, 1);
    if( c==95 ){        /* A '_' wildcard */
      aState[n].val = -1;
      n++;
    }else if( c==37 ){  /* A '%' wildcard */
      aState[n].failstate = n;
      pc_state = n;
    }else{              /* A regular character */
      aState[n].val = c;

      assert( pc_state<=n );
      if( pc_state<0 ){
        aState[n].failstate = -1;
      }else if( pc_state==n ){
        if( c ){
          aState[n].failstate = pc_state;
        }else{
          aState[n].failstate = -2;
        }
      }else{
        int k = pLike->aState[n-1].failstate;
        while( k>pc_state && aState[k+1].val!=-1 && aState[k+1].val!=c ){
          k = aState[k].failstate;
        }
        if( k!=pc_state && aState[k+1].val==c ){
          assert( k==pc_state );
          k++;
        }
        aState[n].failstate = k;
      }
      n++;
    }
  }while( c );
  pLike->nState = n;
#if defined(TRACE_LIKE) && !defined(NDEBUG)
  {
    char *zCompiled = dumpLike(pLike);
    printf("Pattern=\"%s\" Compiled=\"%s\"\n", zPattern, zCompiled);
    sqliteFree(zCompiled);
  }
#endif
  return pLike;
}

/*
** Implementation of the like() SQL function.  This function implements
** the build-in LIKE operator.  The first argument to the function is the
** pattern and the second argument is the string.  So, the SQL statements:
**
**       A LIKE B
**
** is implemented as like(B,A).
**
** If the pointer retrieved by via a call to sqlite3_user_data() is
** not NULL, then this function uses UTF-16. Otherwise UTF-8.
*/
static void likeFunc(
  sqlite3_context *context, 
  int argc, 
  sqlite3_value **argv
){
  register int c;
  u8 enc;
  int offset = 0;
  const unsigned char *zString;
  LikePattern *pLike = sqlite3_get_auxdata(context, 0); 
  struct LikeState *aState;
  register struct LikeState *pState;

  /* If either argument is NULL, the result is NULL */
  if( sqlite3_value_type(argv[1])==SQLITE_NULL || 
      sqlite3_value_type(argv[0])==SQLITE_NULL ){
    return;
  }

  /* If the user-data pointer is NULL, use UTF-8. Otherwise UTF-16. */
  if( sqlite3_user_data(context) ){
    enc = SQLITE_UTF16NATIVE;
    zString = (const unsigned char *)sqlite3_value_text16(argv[1]);
    assert(0);
  }else{
    enc = SQLITE_UTF8;
    zString = sqlite3_value_text(argv[1]);
  }

  /* If the LIKE pattern has not been compiled, compile it now. */
  if( !pLike ){
    pLike = compileLike(argv[0], enc);
    if( !pLike ){
      sqlite3_result_error(context, "out of memory", -1);
      return;
    }
    sqlite3_set_auxdata(context, 0, pLike, deleteLike);
  }
  aState = pLike->aState;
  pState = aState;

  do {
    if( enc==SQLITE_UTF8 ){
      c = zString[offset++];
      if( c&0x80 ){
        offset--;
        c = sqlite3ReadUniChar(zString, &offset, &enc, 1);
      }
    }else{
      c = sqlite3ReadUniChar(zString, &offset, &enc, 1);
    }

skip_read:

#if defined(TRACE_LIKE) && !defined(NDEBUG)
    printf("State=%d:(%d, %d) Input=%d\n", 
        (aState - pState), pState->val, pState->failstate, c);
#endif

    if( pState->val==-1 || pState->val==c ){
      pState++;
    }else{
      struct LikeState *pFailState = &aState[pState->failstate];
      if( pState!=pFailState ){
        pState = pFailState;
        if( c && pState>=aState ) goto skip_read;
      }
    }
  }while( c && pState>=aState );

  if( (pState-aState)==pLike->nState || (pState-aState)<-1 ){
    sqlite3_result_int(context, 1);
  }else{
    sqlite3_result_int(context, 0);
  }
}
#endif

/*
** Implementation of the like() SQL function.  This function implements
** the build-in LIKE operator.  The first argument to the function is the
** pattern and the second argument is the string.  So, the SQL statements:
**
**       A LIKE B
**
** is implemented as like(B,A).
**
** If the pointer retrieved by via a call to sqlite3_user_data() is
** not NULL, then this function uses UTF-16. Otherwise UTF-8.
*/
static void likeFunc(
  sqlite3_context *context, 
  int argc, 
  sqlite3_value **argv
){
  const unsigned char *zA = sqlite3_value_text(argv[0]);
  const unsigned char *zB = sqlite3_value_text(argv[1]);
  if( zA && zB ){
    sqlite3_result_int(context, sqlite3utf8LikeCompare(zA, zB));
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
static void globFunc(sqlite3_context *context, int arg, sqlite3_value **argv){
  const unsigned char *zA = sqlite3_value_text(argv[0]);
  const unsigned char *zB = sqlite3_value_text(argv[1]);
  if( zA && zB ){
    sqlite3_result_int(context, sqlite3GlobCompare(zA, zB));
  }
}

/*
** Implementation of the NULLIF(x,y) function.  The result is the first
** argument if the arguments are different.  The result is NULL if the
** arguments are equal to each other.
*/
static void nullifFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  CollSeq *pColl = sqlite3GetFuncCollSeq(context);
  if( sqlite3MemCompare(argv[0], argv[1], pColl)!=0 ){
    sqlite3_result_value(context, argv[0]);
  }
}

/*
** Implementation of the VERSION(*) function.  The result is the version
** of the SQLite library that is running.
*/
static void versionFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  sqlite3_result_text(context, sqlite3_version, -1, SQLITE_STATIC);
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
static void quoteFunc(sqlite3_context *context, int argc, sqlite3_value **argv){
  if( argc<1 ) return;
  switch( sqlite3_value_type(argv[0]) ){
    case SQLITE_NULL: {
      sqlite3_result_text(context, "NULL", 4, SQLITE_STATIC);
      break;
    }
    case SQLITE_INTEGER:
    case SQLITE_FLOAT: {
      sqlite3_result_value(context, argv[0]);
      break;
    }
    case SQLITE_BLOB: {
      static const char hexdigits[] = { 
        '0', '1', '2', '3', '4', '5', '6', '7',
        '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' 
      };
      char *zText = 0;
      int nBlob = sqlite3_value_bytes(argv[0]);
      char const *zBlob = sqlite3_value_blob(argv[0]);

      zText = (char *)sqliteMalloc((2*nBlob)+4); 
      if( !zText ){
        sqlite3_result_error(context, "out of memory", -1);
      }else{
        int i;
        for(i=0; i<nBlob; i++){
          zText[(i*2)+2] = hexdigits[(zBlob[i]>>4)&0x0F];
          zText[(i*2)+3] = hexdigits[(zBlob[i])&0x0F];
        }
        zText[(nBlob*2)+2] = '\'';
        zText[(nBlob*2)+3] = '\0';
        zText[0] = 'X';
        zText[1] = '\'';
        sqlite3_result_text(context, zText, -1, SQLITE_TRANSIENT);
        sqliteFree(zText);
      }
      break;
    }
    case SQLITE_TEXT: {
      int i,j,n;
      const char *zArg = sqlite3_value_text(argv[0]);
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
      sqlite3_result_text(context, z, j, SQLITE_TRANSIENT);
      sqliteFree(z);
    }
  }
}

#ifdef SQLITE_SOUNDEX
/*
** Compute the soundex encoding of a word.
*/
static void soundexFunc(sqlite3_context *context, int argc, sqlite3_value **argv){
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
  zIn = sqlite3_value_text(argv[0]);
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
    sqlite3_result_text(context, zResult, 4, SQLITE_TRANSIENT);
  }else{
    sqlite3_result_text(context, "?000", 4, SQLITE_STATIC);
  }
}
#endif

#ifdef SQLITE_TEST
/*
** This function generates a string of random characters.  Used for
** generating test data.
*/
static void randStr(sqlite3_context *context, int argc, sqlite3_value **argv){
  static const unsigned char zSrc[] = 
     "abcdefghijklmnopqrstuvwxyz"
     "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
     "0123456789"
     ".-!,:*^+=_|?/<> ";
  int iMin, iMax, n, r, i;
  unsigned char zBuf[1000];
  if( argc>=1 ){
    iMin = sqlite3_value_int(argv[0]);
    if( iMin<0 ) iMin = 0;
    if( iMin>=sizeof(zBuf) ) iMin = sizeof(zBuf)-1;
  }else{
    iMin = 1;
  }
  if( argc>=2 ){
    iMax = sqlite3_value_int(argv[1]);
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
  sqlite3_result_text(context, zBuf, n, SQLITE_TRANSIENT);
}
#endif /* SQLITE_TEST */

#ifdef SQLITE_TEST
/*
** The following two SQL functions are used to test returning a text
** result with a destructor. Function 'test_destructor' takes one argument
** and returns the same argument interpreted as TEXT. A destructor is
** passed with the sqlite3_result_text() call.
**
** SQL function 'test_destructor_count' returns the number of outstanding 
** allocations made by 'test_destructor';
**
** WARNING: Not threadsafe.
*/
static int test_destructor_count_var = 0;
static void destructor(void *p){
  char *zVal = (char *)p;
  assert(zVal);
  zVal--;
  sqliteFree(zVal);
  test_destructor_count_var--;
}
static void test_destructor(
  sqlite3_context *pCtx, 
  int nArg,
  sqlite3_value **argv
){
  char *zVal;
  test_destructor_count_var++;
  assert( nArg==1 );
  if( sqlite3_value_type(argv[0])==SQLITE_NULL ) return;
  zVal = sqliteMalloc(sqlite3_value_bytes(argv[0]) + 2);
  assert( zVal );
  zVal++;
  strcpy(zVal, sqlite3_value_text(argv[0]));
  sqlite3_result_text(pCtx, zVal, -1, destructor);
}
static void test_destructor_count(
  sqlite3_context *pCtx, 
  int nArg,
  sqlite3_value **argv
){
  sqlite3_result_int(pCtx, test_destructor_count_var);
}
#endif /* SQLITE_TEST */

#ifdef SQLITE_TEST
/*
** Routines for testing the sqlite3_get_auxdata() and sqlite3_set_auxdata()
** interface.
**
** The test_auxdata() SQL function attempts to register each of its arguments
** as auxiliary data.  If there are no prior registrations of aux data for
** that argument (meaning the argument is not a constant or this is its first
** call) then the result for that argument is 0.  If there is a prior
** registration, the result for that argument is 1.  The overall result
** is the individual argument results separated by spaces.
*/
static void free_test_auxdata(void *p) {sqliteFree(p);}
static void test_auxdata(
  sqlite3_context *pCtx, 
  int nArg,
  sqlite3_value **argv
){
  int i;
  char *zRet = sqliteMalloc(nArg*2);
  if( !zRet ) return;
  for(i=0; i<nArg; i++){
    char const *z = sqlite3_value_text(argv[i]);
    if( z ){
      char *zAux = sqlite3_get_auxdata(pCtx, i);
      if( zAux ){
        zRet[i*2] = '1';
        if( strcmp(zAux, z) ){
          sqlite3_result_error(pCtx, "Auxilary data corruption", -1);
          return;
        }
      }else{
        zRet[i*2] = '0';
        zAux = sqliteStrDup(z);
        sqlite3_set_auxdata(pCtx, i, zAux, free_test_auxdata);
      }
      zRet[i*2+1] = ' ';
    }
  }
  sqlite3_result_text(pCtx, zRet, 2*nArg-1, free_test_auxdata);
}
#endif /* SQLITE_TEST */

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
static void sumStep(sqlite3_context *context, int argc, sqlite3_value **argv){
  SumCtx *p;
  if( argc<1 ) return;
  p = sqlite3_aggregate_context(context, sizeof(*p));
  if( p && SQLITE_NULL!=sqlite3_value_type(argv[0]) ){
    p->sum += sqlite3_value_double(argv[0]);
    p->cnt++;
  }
}
static void sumFinalize(sqlite3_context *context){
  SumCtx *p;
  p = sqlite3_aggregate_context(context, sizeof(*p));
  sqlite3_result_double(context, p ? p->sum : 0.0);
}
static void avgFinalize(sqlite3_context *context){
  SumCtx *p;
  p = sqlite3_aggregate_context(context, sizeof(*p));
  if( p && p->cnt>0 ){
    sqlite3_result_double(context, p->sum/(double)p->cnt);
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
static void stdDevStep(sqlite3_context *context, int argc, const char **argv){
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
static void stdDevFinalize(sqlite3_context *context){
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
static void countStep(sqlite3_context *context, int argc, sqlite3_value **argv){
  CountCtx *p;
  p = sqlite3_aggregate_context(context, sizeof(*p));
  if( (argc==0 || SQLITE_NULL!=sqlite3_value_type(argv[0])) && p ){
    p->n++;
  }
}   
static void countFinalize(sqlite3_context *context){
  CountCtx *p;
  p = sqlite3_aggregate_context(context, sizeof(*p));
  sqlite3_result_int(context, p ? p->n : 0);
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
static void minmaxStep(sqlite3_context *context, int argc, sqlite3_value **argv){
  int max = 0;
  int cmp = 0;
  Mem *pArg  = (Mem *)argv[0];
  Mem *pBest = (Mem *)sqlite3_aggregate_context(context, sizeof(*pBest));
  if( !pBest ) return;

  if( pBest->flags ){
    CollSeq *pColl = sqlite3GetFuncCollSeq(context);
    /* This step function is used for both the min() and max() aggregates,
    ** the only difference between the two being that the sense of the
    ** comparison is inverted. For the max() aggregate, the
    ** sqlite3_user_data() function returns (void *)-1. For min() it
    ** returns (void *)db, where db is the sqlite3* database pointer.
    ** Therefore the next statement sets variable 'max' to 1 for the max()
    ** aggregate, or 0 for min().
    */
    max = ((sqlite3_user_data(context)==(void *)-1)?1:0);
    cmp = sqlite3MemCompare(pBest, pArg, pColl);
    if( (max && cmp<0) || (!max && cmp>0) ){
      sqlite3VdbeMemCopy(pBest, pArg);
    }
  }else{
    sqlite3VdbeMemCopy(pBest, pArg);
  }
}
static void minMaxFinalize(sqlite3_context *context){
  sqlite3_value *pRes;
  pRes = (sqlite3_value *)sqlite3_aggregate_context(context, sizeof(Mem));
  if( pRes->flags ){
    sqlite3_result_value(context, pRes);
  }
  sqlite3VdbeMemRelease(pRes);
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
     u8 argType;               /* 0: none.  1: db  2: (-1) */
     u8 eTextRep;              /* 1: UTF-16.  0: UTF-8 */
     u8 needCollSeq;
     void (*xFunc)(sqlite3_context*,int,sqlite3_value **);
  } aFuncs[] = {
    { "min",                        -1, 0, SQLITE_UTF8, 1, minmaxFunc },
    { "min",                         0, 0, SQLITE_UTF8, 1, 0          },
    { "max",                        -1, 2, SQLITE_UTF8, 1, minmaxFunc },
    { "max",                         0, 2, SQLITE_UTF8, 1, 0          },
    { "typeof",                      1, 0, SQLITE_UTF8, 0, typeofFunc },
    { "length",                      1, 0, SQLITE_UTF8, 0, lengthFunc },
    { "substr",                      3, 0, SQLITE_UTF8, 0, substrFunc },
    { "abs",                         1, 0, SQLITE_UTF8, 0, absFunc    },
    { "round",                       1, 0, SQLITE_UTF8, 0, roundFunc  },
    { "round",                       2, 0, SQLITE_UTF8, 0, roundFunc  },
    { "upper",                       1, 0, SQLITE_UTF8, 0, upperFunc  },
    { "lower",                       1, 0, SQLITE_UTF8, 0, lowerFunc  },
    { "coalesce",                   -1, 0, SQLITE_UTF8, 0, ifnullFunc },
    { "coalesce",                    0, 0, SQLITE_UTF8, 0, 0          },
    { "coalesce",                    1, 0, SQLITE_UTF8, 0, 0          },
    { "ifnull",                      2, 0, SQLITE_UTF8, 1, ifnullFunc },
    { "random",                     -1, 0, SQLITE_UTF8, 0, randomFunc },
    { "like",                        2, 0, SQLITE_UTF8, 0, likeFunc   },
/* { "like",                        2, 2, SQLITE_UTF16,0, likeFunc   }, */
    { "glob",                        2, 0, SQLITE_UTF8, 0, globFunc   },
    { "nullif",                      2, 0, SQLITE_UTF8, 0, nullifFunc },
    { "sqlite_version",              0, 0, SQLITE_UTF8, 0, versionFunc},
    { "quote",                       1, 0, SQLITE_UTF8, 0, quoteFunc  },
    { "last_insert_rowid",           0, 1, SQLITE_UTF8, 0, last_insert_rowid },
    { "changes",                     0, 1, SQLITE_UTF8, 0, changes    },
    { "total_changes",               0, 1, SQLITE_UTF8, 0, total_changes },
#ifdef SQLITE_SOUNDEX
    { "soundex",                     1, 0, SQLITE_UTF8, 0, soundexFunc},
#endif
#ifdef SQLITE_TEST
    { "randstr",                     2, 0, SQLITE_UTF8, 0, randStr    },
    { "test_destructor",             1, 0, SQLITE_UTF8, 0, test_destructor},
    { "test_destructor_count", 0, 0, SQLITE_UTF8, 0, test_destructor_count},
    { "test_auxdata",               -1, 0, SQLITE_UTF8, 0, test_auxdata},
#endif
  };
  static struct {
    char *zName;
    signed char nArg;
    u8 argType;
    u8 needCollSeq;
    void (*xStep)(sqlite3_context*,int,sqlite3_value**);
    void (*xFinalize)(sqlite3_context*);
  } aAggs[] = {
    { "min",    1, 0, 1, minmaxStep,   minMaxFinalize },
    { "max",    1, 2, 1, minmaxStep,   minMaxFinalize },
    { "sum",    1, 0, 0, sumStep,      sumFinalize    },
    { "avg",    1, 0, 0, sumStep,      avgFinalize    },
    { "count",  0, 0, 0, countStep,    countFinalize  },
    { "count",  1, 0, 0, countStep,    countFinalize  },
#if 0
    { "stddev", 1, 0, stdDevStep,   stdDevFinalize },
#endif
  };
  int i;

  for(i=0; i<sizeof(aFuncs)/sizeof(aFuncs[0]); i++){
    void *pArg = 0;
    switch( aFuncs[i].argType ){
      case 1: pArg = db; break;
      case 2: pArg = (void *)(-1); break;
    }
    sqlite3_create_function(db, aFuncs[i].zName, aFuncs[i].nArg,
        aFuncs[i].eTextRep, pArg, aFuncs[i].xFunc, 0, 0);
    if( aFuncs[i].needCollSeq ){
      FuncDef *pFunc = sqlite3FindFunction(db, aFuncs[i].zName, 
          strlen(aFuncs[i].zName), aFuncs[i].nArg, aFuncs[i].eTextRep, 0);
      if( pFunc && aFuncs[i].needCollSeq ){
        pFunc->needCollSeq = 1;
      }
    }
  }
  for(i=0; i<sizeof(aAggs)/sizeof(aAggs[0]); i++){
    void *pArg = 0;
    switch( aAggs[i].argType ){
      case 1: pArg = db; break;
      case 2: pArg = (void *)(-1); break;
    }
    sqlite3_create_function(db, aAggs[i].zName, aAggs[i].nArg, SQLITE_UTF8, 
        pArg, 0, aAggs[i].xStep, aAggs[i].xFinalize);
    if( aAggs[i].needCollSeq ){
      FuncDef *pFunc = sqlite3FindFunction( db, aAggs[i].zName,
          strlen(aAggs[i].zName), aAggs[i].nArg, SQLITE_UTF8, 0);
      if( pFunc && aAggs[i].needCollSeq ){
        pFunc->needCollSeq = 1;
      }
    }
  }
  sqlite3RegisterDateTimeFunctions(db);
}
