/*
** 2001 September 15
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** Utility functions used throughout sqlite.
**
** This file contains functions for allocating memory, comparing
** strings, and stuff like that.
**
** $Id: util.c,v 1.249 2009/03/01 22:29:20 drh Exp $
*/
#include "sqliteInt.h"
#include <stdarg.h>

/*
** Routine needed to support the testcase() macro.
*/
#ifdef SQLITE_COVERAGE_TEST
void sqlite3Coverage(int x){
  static int dummy = 0;
  dummy += x;
}
#endif

/*
** Routine needed to support the ALWAYS() and NEVER() macros.
**
** The argument to ALWAYS() should always be true and the argument
** to NEVER() should always be false.  If either is not the case
** then this routine is called in order to throw an error.
**
** This routine only exists if assert() is operational.  It always
** throws an assert on its first invocation.  The variable has a long
** name to help the assert() message be more readable.  The variable
** is used to prevent a too-clever optimizer from optimizing out the
** entire call.
*/
#ifndef NDEBUG
int sqlite3Assert(void){
  static volatile int ALWAYS_was_false_or_NEVER_was_true = 0;
  assert( ALWAYS_was_false_or_NEVER_was_true );      /* Always fails */
  return ALWAYS_was_false_or_NEVER_was_true++;       /* Not Reached */
}
#endif

/*
** Return true if the floating point value is Not a Number (NaN).
*/
int sqlite3IsNaN(double x){
  /* This NaN test sometimes fails if compiled on GCC with -ffast-math.
  ** On the other hand, the use of -ffast-math comes with the following
  ** warning:
  **
  **      This option [-ffast-math] should never be turned on by any
  **      -O option since it can result in incorrect output for programs
  **      which depend on an exact implementation of IEEE or ISO 
  **      rules/specifications for math functions.
  **
  ** Under MSVC, this NaN test may fail if compiled with a floating-
  ** point precision mode other than /fp:precise.  From the MSDN 
  ** documentation:
  **
  **      The compiler [with /fp:precise] will properly handle comparisons 
  **      involving NaN. For example, x != x evaluates to true if x is NaN 
  **      ...
  */
#ifdef __FAST_MATH__
# error SQLite will not work correctly with the -ffast-math option of GCC.
#endif
  volatile double y = x;
  volatile double z = y;
  return y!=z;
}

/*
** Compute a string length that is limited to what can be stored in
** lower 30 bits of a 32-bit signed integer.
*/
int sqlite3Strlen30(const char *z){
  const char *z2 = z;
  while( *z2 ){ z2++; }
  return 0x3fffffff & (int)(z2 - z);
}

/*
** Return the length of a string, except do not allow the string length
** to exceed the SQLITE_LIMIT_LENGTH setting.
*/
int sqlite3Strlen(sqlite3 *db, const char *z){
  const char *z2 = z;
  int len;
  int x;
  while( *z2 ){ z2++; }
  x = (int)(z2 - z);
  len = 0x7fffffff & x;
  if( len!=x || len > db->aLimit[SQLITE_LIMIT_LENGTH] ){
    return db->aLimit[SQLITE_LIMIT_LENGTH];
  }else{
    return len;
  }
}

/*
** Set the most recent error code and error string for the sqlite
** handle "db". The error code is set to "err_code".
**
** If it is not NULL, string zFormat specifies the format of the
** error string in the style of the printf functions: The following
** format characters are allowed:
**
**      %s      Insert a string
**      %z      A string that should be freed after use
**      %d      Insert an integer
**      %T      Insert a token
**      %S      Insert the first element of a SrcList
**
** zFormat and any string tokens that follow it are assumed to be
** encoded in UTF-8.
**
** To clear the most recent error for sqlite handle "db", sqlite3Error
** should be called with err_code set to SQLITE_OK and zFormat set
** to NULL.
*/
void sqlite3Error(sqlite3 *db, int err_code, const char *zFormat, ...){
  if( db && (db->pErr || (db->pErr = sqlite3ValueNew(db))!=0) ){
    db->errCode = err_code;
    if( zFormat ){
      char *z;
      va_list ap;
      va_start(ap, zFormat);
      z = sqlite3VMPrintf(db, zFormat, ap);
      va_end(ap);
      sqlite3ValueSetStr(db->pErr, -1, z, SQLITE_UTF8, SQLITE_DYNAMIC);
    }else{
      sqlite3ValueSetStr(db->pErr, 0, 0, SQLITE_UTF8, SQLITE_STATIC);
    }
  }
}

/*
** Add an error message to pParse->zErrMsg and increment pParse->nErr.
** The following formatting characters are allowed:
**
**      %s      Insert a string
**      %z      A string that should be freed after use
**      %d      Insert an integer
**      %T      Insert a token
**      %S      Insert the first element of a SrcList
**
** This function should be used to report any error that occurs whilst
** compiling an SQL statement (i.e. within sqlite3_prepare()). The
** last thing the sqlite3_prepare() function does is copy the error
** stored by this function into the database handle using sqlite3Error().
** Function sqlite3Error() should be used during statement execution
** (sqlite3_step() etc.).
*/
void sqlite3ErrorMsg(Parse *pParse, const char *zFormat, ...){
  va_list ap;
  sqlite3 *db = pParse->db;
  pParse->nErr++;
  sqlite3DbFree(db, pParse->zErrMsg);
  va_start(ap, zFormat);
  pParse->zErrMsg = sqlite3VMPrintf(db, zFormat, ap);
  va_end(ap);
  if( pParse->rc==SQLITE_OK ){
    pParse->rc = SQLITE_ERROR;
  }
}

/*
** Clear the error message in pParse, if any
*/
void sqlite3ErrorClear(Parse *pParse){
  sqlite3DbFree(pParse->db, pParse->zErrMsg);
  pParse->zErrMsg = 0;
  pParse->nErr = 0;
}

/*
** Convert an SQL-style quoted string into a normal string by removing
** the quote characters.  The conversion is done in-place.  If the
** input does not begin with a quote character, then this routine
** is a no-op.
**
** 2002-Feb-14: This routine is extended to remove MS-Access style
** brackets from around identifers.  For example:  "[a-b-c]" becomes
** "a-b-c".
*/
void sqlite3Dequote(char *z){
  char quote;
  int i, j;
  if( z==0 ) return;
  quote = z[0];
  switch( quote ){
    case '\'':  break;
    case '"':   break;
    case '`':   break;                /* For MySQL compatibility */
    case '[':   quote = ']';  break;  /* For MS SqlServer compatibility */
    default:    return;
  }
  for(i=1, j=0; z[i]; i++){
    if( z[i]==quote ){
      if( z[i+1]==quote ){
        z[j++] = quote;
        i++;
      }else{
        z[j++] = 0;
        break;
      }
    }else{
      z[j++] = z[i];
    }
  }
}

/* Convenient short-hand */
#define UpperToLower sqlite3UpperToLower

/*
** Some systems have stricmp().  Others have strcasecmp().  Because
** there is no consistency, we will define our own.
*/
int sqlite3StrICmp(const char *zLeft, const char *zRight){
  register unsigned char *a, *b;
  a = (unsigned char *)zLeft;
  b = (unsigned char *)zRight;
  while( *a!=0 && UpperToLower[*a]==UpperToLower[*b]){ a++; b++; }
  return UpperToLower[*a] - UpperToLower[*b];
}
int sqlite3StrNICmp(const char *zLeft, const char *zRight, int N){
  register unsigned char *a, *b;
  a = (unsigned char *)zLeft;
  b = (unsigned char *)zRight;
  while( N-- > 0 && *a!=0 && UpperToLower[*a]==UpperToLower[*b]){ a++; b++; }
  return N<0 ? 0 : UpperToLower[*a] - UpperToLower[*b];
}

/*
** Return TRUE if z is a pure numeric string.  Return FALSE if the
** string contains any character which is not part of a number. If
** the string is numeric and contains the '.' character, set *realnum
** to TRUE (otherwise FALSE).
**
** An empty string is considered non-numeric.
*/
int sqlite3IsNumber(const char *z, int *realnum, u8 enc){
  int incr = (enc==SQLITE_UTF8?1:2);
  if( enc==SQLITE_UTF16BE ) z++;
  if( *z=='-' || *z=='+' ) z += incr;
  if( !sqlite3Isdigit(*z) ){
    return 0;
  }
  z += incr;
  if( realnum ) *realnum = 0;
  while( sqlite3Isdigit(*z) ){ z += incr; }
  if( *z=='.' ){
    z += incr;
    if( !sqlite3Isdigit(*z) ) return 0;
    while( sqlite3Isdigit(*z) ){ z += incr; }
    if( realnum ) *realnum = 1;
  }
  if( *z=='e' || *z=='E' ){
    z += incr;
    if( *z=='+' || *z=='-' ) z += incr;
    if( !sqlite3Isdigit(*z) ) return 0;
    while( sqlite3Isdigit(*z) ){ z += incr; }
    if( realnum ) *realnum = 1;
  }
  return *z==0;
}

/*
** The string z[] is an ascii representation of a real number.
** Convert this string to a double.
**
** This routine assumes that z[] really is a valid number.  If it
** is not, the result is undefined.
**
** This routine is used instead of the library atof() function because
** the library atof() might want to use "," as the decimal point instead
** of "." depending on how locale is set.  But that would cause problems
** for SQL.  So this routine always uses "." regardless of locale.
*/
int sqlite3AtoF(const char *z, double *pResult){
#ifndef SQLITE_OMIT_FLOATING_POINT
  int sign = 1;
  const char *zBegin = z;
  LONGDOUBLE_TYPE v1 = 0.0;
  int nSignificant = 0;
  while( sqlite3Isspace(*z) ) z++;
  if( *z=='-' ){
    sign = -1;
    z++;
  }else if( *z=='+' ){
    z++;
  }
  while( z[0]=='0' ){
    z++;
  }
  while( sqlite3Isdigit(*z) ){
    v1 = v1*10.0 + (*z - '0');
    z++;
    nSignificant++;
  }
  if( *z=='.' ){
    LONGDOUBLE_TYPE divisor = 1.0;
    z++;
    if( nSignificant==0 ){
      while( z[0]=='0' ){
        divisor *= 10.0;
        z++;
      }
    }
    while( sqlite3Isdigit(*z) ){
      if( nSignificant<18 ){
        v1 = v1*10.0 + (*z - '0');
        divisor *= 10.0;
        nSignificant++;
      }
      z++;
    }
    v1 /= divisor;
  }
  if( *z=='e' || *z=='E' ){
    int esign = 1;
    int eval = 0;
    LONGDOUBLE_TYPE scale = 1.0;
    z++;
    if( *z=='-' ){
      esign = -1;
      z++;
    }else if( *z=='+' ){
      z++;
    }
    while( sqlite3Isdigit(*z) ){
      eval = eval*10 + *z - '0';
      z++;
    }
    while( eval>=64 ){ scale *= 1.0e+64; eval -= 64; }
    while( eval>=16 ){ scale *= 1.0e+16; eval -= 16; }
    while( eval>=4 ){ scale *= 1.0e+4; eval -= 4; }
    while( eval>=1 ){ scale *= 1.0e+1; eval -= 1; }
    if( esign<0 ){
      v1 /= scale;
    }else{
      v1 *= scale;
    }
  }
  *pResult = (double)(sign<0 ? -v1 : v1);
  return (int)(z - zBegin);
#else
  return sqlite3Atoi64(z, pResult);
#endif /* SQLITE_OMIT_FLOATING_POINT */
}

/*
** Compare the 19-character string zNum against the text representation
** value 2^63:  9223372036854775808.  Return negative, zero, or positive
** if zNum is less than, equal to, or greater than the string.
**
** Unlike memcmp() this routine is guaranteed to return the difference
** in the values of the last digit if the only difference is in the
** last digit.  So, for example,
**
**      compare2pow63("9223372036854775800")
**
** will return -8.
*/
static int compare2pow63(const char *zNum){
  int c;
  c = memcmp(zNum,"922337203685477580",18);
  if( c==0 ){
    c = zNum[18] - '8';
  }
  return c;
}


/*
** Return TRUE if zNum is a 64-bit signed integer and write
** the value of the integer into *pNum.  If zNum is not an integer
** or is an integer that is too large to be expressed with 64 bits,
** then return false.
**
** When this routine was originally written it dealt with only
** 32-bit numbers.  At that time, it was much faster than the
** atoi() library routine in RedHat 7.2.
*/
int sqlite3Atoi64(const char *zNum, i64 *pNum){
  i64 v = 0;
  int neg;
  int i, c;
  const char *zStart;
  while( sqlite3Isspace(*zNum) ) zNum++;
  if( *zNum=='-' ){
    neg = 1;
    zNum++;
  }else if( *zNum=='+' ){
    neg = 0;
    zNum++;
  }else{
    neg = 0;
  }
  zStart = zNum;
  while( zNum[0]=='0' ){ zNum++; } /* Skip over leading zeros. Ticket #2454 */
  for(i=0; (c=zNum[i])>='0' && c<='9'; i++){
    v = v*10 + c - '0';
  }
  *pNum = neg ? -v : v;
  if( c!=0 || (i==0 && zStart==zNum) || i>19 ){
    /* zNum is empty or contains non-numeric text or is longer
    ** than 19 digits (thus guaranting that it is too large) */
    return 0;
  }else if( i<19 ){
    /* Less than 19 digits, so we know that it fits in 64 bits */
    return 1;
  }else{
    /* 19-digit numbers must be no larger than 9223372036854775807 if positive
    ** or 9223372036854775808 if negative.  Note that 9223372036854665808
    ** is 2^63. */
    return compare2pow63(zNum)<neg;
  }
}

/*
** The string zNum represents an integer.  There might be some other
** information following the integer too, but that part is ignored.
** If the integer that the prefix of zNum represents will fit in a
** 64-bit signed integer, return TRUE.  Otherwise return FALSE.
**
** This routine returns FALSE for the string -9223372036854775808 even that
** that number will, in theory fit in a 64-bit integer.  Positive
** 9223373036854775808 will not fit in 64 bits.  So it seems safer to return
** false.
*/
int sqlite3FitsIn64Bits(const char *zNum, int negFlag){
  int i, c;
  int neg = 0;
  if( *zNum=='-' ){
    neg = 1;
    zNum++;
  }else if( *zNum=='+' ){
    zNum++;
  }
  if( negFlag ) neg = 1-neg;
  while( *zNum=='0' ){
    zNum++;   /* Skip leading zeros.  Ticket #2454 */
  }
  for(i=0; (c=zNum[i])>='0' && c<='9'; i++){}
  if( i<19 ){
    /* Guaranteed to fit if less than 19 digits */
    return 1;
  }else if( i>19 ){
    /* Guaranteed to be too big if greater than 19 digits */
    return 0;
  }else{
    /* Compare against 2^63. */
    return compare2pow63(zNum)<neg;
  }
}

/*
** If zNum represents an integer that will fit in 32-bits, then set
** *pValue to that integer and return true.  Otherwise return false.
**
** Any non-numeric characters that following zNum are ignored.
** This is different from sqlite3Atoi64() which requires the
** input number to be zero-terminated.
*/
int sqlite3GetInt32(const char *zNum, int *pValue){
  sqlite_int64 v = 0;
  int i, c;
  int neg = 0;
  if( zNum[0]=='-' ){
    neg = 1;
    zNum++;
  }else if( zNum[0]=='+' ){
    zNum++;
  }
  while( zNum[0]=='0' ) zNum++;
  for(i=0; i<11 && (c = zNum[i] - '0')>=0 && c<=9; i++){
    v = v*10 + c;
  }

  /* The longest decimal representation of a 32 bit integer is 10 digits:
  **
  **             1234567890
  **     2^31 -> 2147483648
  */
  if( i>10 ){
    return 0;
  }
  if( v-neg>2147483647 ){
    return 0;
  }
  if( neg ){
    v = -v;
  }
  *pValue = (int)v;
  return 1;
}

/*
** The variable-length integer encoding is as follows:
**
** KEY:
**         A = 0xxxxxxx    7 bits of data and one flag bit
**         B = 1xxxxxxx    7 bits of data and one flag bit
**         C = xxxxxxxx    8 bits of data
**
**  7 bits - A
** 14 bits - BA
** 21 bits - BBA
** 28 bits - BBBA
** 35 bits - BBBBA
** 42 bits - BBBBBA
** 49 bits - BBBBBBA
** 56 bits - BBBBBBBA
** 64 bits - BBBBBBBBC
*/

/*
** Write a 64-bit variable-length integer to memory starting at p[0].
** The length of data write will be between 1 and 9 bytes.  The number
** of bytes written is returned.
**
** A variable-length integer consists of the lower 7 bits of each byte
** for all bytes that have the 8th bit set and one byte with the 8th
** bit clear.  Except, if we get to the 9th byte, it stores the full
** 8 bits and is the last byte.
*/
int sqlite3PutVarint(unsigned char *p, u64 v){
  int i, j, n;
  u8 buf[10];
  if( v & (((u64)0xff000000)<<32) ){
    p[8] = (u8)v;
    v >>= 8;
    for(i=7; i>=0; i--){
      p[i] = (u8)((v & 0x7f) | 0x80);
      v >>= 7;
    }
    return 9;
  }    
  n = 0;
  do{
    buf[n++] = (u8)((v & 0x7f) | 0x80);
    v >>= 7;
  }while( v!=0 );
  buf[0] &= 0x7f;
  assert( n<=9 );
  for(i=0, j=n-1; j>=0; j--, i++){
    p[i] = buf[j];
  }
  return n;
}

/*
** This routine is a faster version of sqlite3PutVarint() that only
** works for 32-bit positive integers and which is optimized for
** the common case of small integers.  A MACRO version, putVarint32,
** is provided which inlines the single-byte case.  All code should use
** the MACRO version as this function assumes the single-byte case has
** already been handled.
*/
int sqlite3PutVarint32(unsigned char *p, u32 v){
#ifndef putVarint32
  if( (v & ~0x7f)==0 ){
    p[0] = v;
    return 1;
  }
#endif
  if( (v & ~0x3fff)==0 ){
    p[0] = (u8)((v>>7) | 0x80);
    p[1] = (u8)(v & 0x7f);
    return 2;
  }
  return sqlite3PutVarint(p, v);
}

/*
** Read a 64-bit variable-length integer from memory starting at p[0].
** Return the number of bytes read.  The value is stored in *v.
*/
u8 sqlite3GetVarint(const unsigned char *p, u64 *v){
  u32 a,b,s;

  a = *p;
  /* a: p0 (unmasked) */
  if (!(a&0x80))
  {
    *v = a;
    return 1;
  }

  p++;
  b = *p;
  /* b: p1 (unmasked) */
  if (!(b&0x80))
  {
    a &= 0x7f;
    a = a<<7;
    a |= b;
    *v = a;
    return 2;
  }

  p++;
  a = a<<14;
  a |= *p;
  /* a: p0<<14 | p2 (unmasked) */
  if (!(a&0x80))
  {
    a &= (0x7f<<14)|(0x7f);
    b &= 0x7f;
    b = b<<7;
    a |= b;
    *v = a;
    return 3;
  }

  /* CSE1 from below */
  a &= (0x7f<<14)|(0x7f);
  p++;
  b = b<<14;
  b |= *p;
  /* b: p1<<14 | p3 (unmasked) */
  if (!(b&0x80))
  {
    b &= (0x7f<<14)|(0x7f);
    /* moved CSE1 up */
    /* a &= (0x7f<<14)|(0x7f); */
    a = a<<7;
    a |= b;
    *v = a;
    return 4;
  }

  /* a: p0<<14 | p2 (masked) */
  /* b: p1<<14 | p3 (unmasked) */
  /* 1:save off p0<<21 | p1<<14 | p2<<7 | p3 (masked) */
  /* moved CSE1 up */
  /* a &= (0x7f<<14)|(0x7f); */
  b &= (0x7f<<14)|(0x7f);
  s = a;
  /* s: p0<<14 | p2 (masked) */

  p++;
  a = a<<14;
  a |= *p;
  /* a: p0<<28 | p2<<14 | p4 (unmasked) */
  if (!(a&0x80))
  {
    /* we can skip these cause they were (effectively) done above in calc'ing s */
    /* a &= (0x7f<<28)|(0x7f<<14)|(0x7f); */
    /* b &= (0x7f<<14)|(0x7f); */
    b = b<<7;
    a |= b;
    s = s>>18;
    *v = ((u64)s)<<32 | a;
    return 5;
  }

  /* 2:save off p0<<21 | p1<<14 | p2<<7 | p3 (masked) */
  s = s<<7;
  s |= b;
  /* s: p0<<21 | p1<<14 | p2<<7 | p3 (masked) */

  p++;
  b = b<<14;
  b |= *p;
  /* b: p1<<28 | p3<<14 | p5 (unmasked) */
  if (!(b&0x80))
  {
    /* we can skip this cause it was (effectively) done above in calc'ing s */
    /* b &= (0x7f<<28)|(0x7f<<14)|(0x7f); */
    a &= (0x7f<<14)|(0x7f);
    a = a<<7;
    a |= b;
    s = s>>18;
    *v = ((u64)s)<<32 | a;
    return 6;
  }

  p++;
  a = a<<14;
  a |= *p;
  /* a: p2<<28 | p4<<14 | p6 (unmasked) */
  if (!(a&0x80))
  {
    a &= (0x1f<<28)|(0x7f<<14)|(0x7f);
    b &= (0x7f<<14)|(0x7f);
    b = b<<7;
    a |= b;
    s = s>>11;
    *v = ((u64)s)<<32 | a;
    return 7;
  }

  /* CSE2 from below */
  a &= (0x7f<<14)|(0x7f);
  p++;
  b = b<<14;
  b |= *p;
  /* b: p3<<28 | p5<<14 | p7 (unmasked) */
  if (!(b&0x80))
  {
    b &= (0x1f<<28)|(0x7f<<14)|(0x7f);
    /* moved CSE2 up */
    /* a &= (0x7f<<14)|(0x7f); */
    a = a<<7;
    a |= b;
    s = s>>4;
    *v = ((u64)s)<<32 | a;
    return 8;
  }

  p++;
  a = a<<15;
  a |= *p;
  /* a: p4<<29 | p6<<15 | p8 (unmasked) */

  /* moved CSE2 up */
  /* a &= (0x7f<<29)|(0x7f<<15)|(0xff); */
  b &= (0x7f<<14)|(0x7f);
  b = b<<8;
  a |= b;

  s = s<<4;
  b = p[-4];
  b &= 0x7f;
  b = b>>3;
  s |= b;

  *v = ((u64)s)<<32 | a;

  return 9;
}

/*
** Read a 32-bit variable-length integer from memory starting at p[0].
** Return the number of bytes read.  The value is stored in *v.
** A MACRO version, getVarint32, is provided which inlines the 
** single-byte case.  All code should use the MACRO version as 
** this function assumes the single-byte case has already been handled.
*/
u8 sqlite3GetVarint32(const unsigned char *p, u32 *v){
  u32 a,b;

  a = *p;
  /* a: p0 (unmasked) */
#ifndef getVarint32
  if (!(a&0x80))
  {
    *v = a;
    return 1;
  }
#endif

  p++;
  b = *p;
  /* b: p1 (unmasked) */
  if (!(b&0x80))
  {
    a &= 0x7f;
    a = a<<7;
    *v = a | b;
    return 2;
  }

  p++;
  a = a<<14;
  a |= *p;
  /* a: p0<<14 | p2 (unmasked) */
  if (!(a&0x80))
  {
    a &= (0x7f<<14)|(0x7f);
    b &= 0x7f;
    b = b<<7;
    *v = a | b;
    return 3;
  }

  p++;
  b = b<<14;
  b |= *p;
  /* b: p1<<14 | p3 (unmasked) */
  if (!(b&0x80))
  {
    b &= (0x7f<<14)|(0x7f);
    a &= (0x7f<<14)|(0x7f);
    a = a<<7;
    *v = a | b;
    return 4;
  }

  p++;
  a = a<<14;
  a |= *p;
  /* a: p0<<28 | p2<<14 | p4 (unmasked) */
  if (!(a&0x80))
  {
    a &= (0x1f<<28)|(0x7f<<14)|(0x7f);
    b &= (0x1f<<28)|(0x7f<<14)|(0x7f);
    b = b<<7;
    *v = a | b;
    return 5;
  }

  /* We can only reach this point when reading a corrupt database
  ** file.  In that case we are not in any hurry.  Use the (relatively
  ** slow) general-purpose sqlite3GetVarint() routine to extract the
  ** value. */
  {
    u64 v64;
    u8 n;

    p -= 4;
    n = sqlite3GetVarint(p, &v64);
    assert( n>5 && n<=9 );
    *v = (u32)v64;
    return n;
  }
}

/*
** Return the number of bytes that will be needed to store the given
** 64-bit integer.
*/
int sqlite3VarintLen(u64 v){
  int i = 0;
  do{
    i++;
    v >>= 7;
  }while( v!=0 && i<9 );
  return i;
}


/*
** Read or write a four-byte big-endian integer value.
*/
u32 sqlite3Get4byte(const u8 *p){
  return (p[0]<<24) | (p[1]<<16) | (p[2]<<8) | p[3];
}
void sqlite3Put4byte(unsigned char *p, u32 v){
  p[0] = (u8)(v>>24);
  p[1] = (u8)(v>>16);
  p[2] = (u8)(v>>8);
  p[3] = (u8)v;
}



#if !defined(SQLITE_OMIT_BLOB_LITERAL) || defined(SQLITE_HAS_CODEC)
/*
** Translate a single byte of Hex into an integer.
** This routinen only works if h really is a valid hexadecimal
** character:  0..9a..fA..F
*/
static u8 hexToInt(int h){
  assert( (h>='0' && h<='9') ||  (h>='a' && h<='f') ||  (h>='A' && h<='F') );
#ifdef SQLITE_ASCII
  h += 9*(1&(h>>6));
#endif
#ifdef SQLITE_EBCDIC
  h += 9*(1&~(h>>4));
#endif
  return (u8)(h & 0xf);
}
#endif /* !SQLITE_OMIT_BLOB_LITERAL || SQLITE_HAS_CODEC */

#if !defined(SQLITE_OMIT_BLOB_LITERAL) || defined(SQLITE_HAS_CODEC)
/*
** Convert a BLOB literal of the form "x'hhhhhh'" into its binary
** value.  Return a pointer to its binary value.  Space to hold the
** binary value has been obtained from malloc and must be freed by
** the calling routine.
*/
void *sqlite3HexToBlob(sqlite3 *db, const char *z, int n){
  char *zBlob;
  int i;

  zBlob = (char *)sqlite3DbMallocRaw(db, n/2 + 1);
  n--;
  if( zBlob ){
    for(i=0; i<n; i+=2){
      zBlob[i/2] = (hexToInt(z[i])<<4) | hexToInt(z[i+1]);
    }
    zBlob[i/2] = 0;
  }
  return zBlob;
}
#endif /* !SQLITE_OMIT_BLOB_LITERAL || SQLITE_HAS_CODEC */


/*
** Change the sqlite.magic from SQLITE_MAGIC_OPEN to SQLITE_MAGIC_BUSY.
** Return an error (non-zero) if the magic was not SQLITE_MAGIC_OPEN
** when this routine is called.
**
** This routine is called when entering an SQLite API.  The SQLITE_MAGIC_OPEN
** value indicates that the database connection passed into the API is
** open and is not being used by another thread.  By changing the value
** to SQLITE_MAGIC_BUSY we indicate that the connection is in use.
** sqlite3SafetyOff() below will change the value back to SQLITE_MAGIC_OPEN
** when the API exits. 
**
** This routine is a attempt to detect if two threads use the
** same sqlite* pointer at the same time.  There is a race 
** condition so it is possible that the error is not detected.
** But usually the problem will be seen.  The result will be an
** error which can be used to debug the application that is
** using SQLite incorrectly.
**
** Ticket #202:  If db->magic is not a valid open value, take care not
** to modify the db structure at all.  It could be that db is a stale
** pointer.  In other words, it could be that there has been a prior
** call to sqlite3_close(db) and db has been deallocated.  And we do
** not want to write into deallocated memory.
*/
#ifdef SQLITE_DEBUG
int sqlite3SafetyOn(sqlite3 *db){
  if( db->magic==SQLITE_MAGIC_OPEN ){
    db->magic = SQLITE_MAGIC_BUSY;
    assert( sqlite3_mutex_held(db->mutex) );
    return 0;
  }else if( db->magic==SQLITE_MAGIC_BUSY ){
    db->magic = SQLITE_MAGIC_ERROR;
    db->u1.isInterrupted = 1;
  }
  return 1;
}
#endif

/*
** Change the magic from SQLITE_MAGIC_BUSY to SQLITE_MAGIC_OPEN.
** Return an error (non-zero) if the magic was not SQLITE_MAGIC_BUSY
** when this routine is called.
*/
#ifdef SQLITE_DEBUG
int sqlite3SafetyOff(sqlite3 *db){
  if( db->magic==SQLITE_MAGIC_BUSY ){
    db->magic = SQLITE_MAGIC_OPEN;
    assert( sqlite3_mutex_held(db->mutex) );
    return 0;
  }else{
    db->magic = SQLITE_MAGIC_ERROR;
    db->u1.isInterrupted = 1;
    return 1;
  }
}
#endif

/*
** Check to make sure we have a valid db pointer.  This test is not
** foolproof but it does provide some measure of protection against
** misuse of the interface such as passing in db pointers that are
** NULL or which have been previously closed.  If this routine returns
** 1 it means that the db pointer is valid and 0 if it should not be
** dereferenced for any reason.  The calling function should invoke
** SQLITE_MISUSE immediately.
**
** sqlite3SafetyCheckOk() requires that the db pointer be valid for
** use.  sqlite3SafetyCheckSickOrOk() allows a db pointer that failed to
** open properly and is not fit for general use but which can be
** used as an argument to sqlite3_errmsg() or sqlite3_close().
*/
int sqlite3SafetyCheckOk(sqlite3 *db){
  u32 magic;
  if( db==0 ) return 0;
  magic = db->magic;
  if( magic!=SQLITE_MAGIC_OPEN &&
      magic!=SQLITE_MAGIC_BUSY ) return 0;
  return 1;
}
int sqlite3SafetyCheckSickOrOk(sqlite3 *db){
  u32 magic;
  if( db==0 ) return 0;
  magic = db->magic;
  if( magic!=SQLITE_MAGIC_SICK &&
      magic!=SQLITE_MAGIC_OPEN &&
      magic!=SQLITE_MAGIC_BUSY ) return 0;
  return 1;
}
