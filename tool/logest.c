/*
** 2013-06-10
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains a simple command-line utility for converting from
** integers and LogEst values and back again and for doing simple
** arithmetic operations (multiple and add) on LogEst values.
**
** Usage:
**
**      ./LogEst ARGS
**
** Arguments:
**
**    'x'    Multiple the top two elements of the stack
**    '+'    Add the top two elements of the stack
**    NUM    Convert NUM from integer to LogEst and push onto the stack
**   ^NUM    Interpret NUM as a LogEst and push onto stack.
**
** Examples:
**
** To convert 123 from LogEst to integer:
** 
**         ./LogEst ^123
**
** To convert 123456 from integer to LogEst:
**
**         ./LogEst 123456
**
*/
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <assert.h>
#include <string.h>
#include "sqlite3.h"

typedef short int LogEst;  /* 10 times log2() */

LogEst logEstMultiply(LogEst a, LogEst b){ return a+b; }
LogEst logEstAdd(LogEst a, LogEst b){
  static const unsigned char x[] = {
     10, 10,                         /* 0,1 */
      9, 9,                          /* 2,3 */
      8, 8,                          /* 4,5 */
      7, 7, 7,                       /* 6,7,8 */
      6, 6, 6,                       /* 9,10,11 */
      5, 5, 5,                       /* 12-14 */
      4, 4, 4, 4,                    /* 15-18 */
      3, 3, 3, 3, 3, 3,              /* 19-24 */
      2, 2, 2, 2, 2, 2, 2,           /* 25-31 */
  };
  if( a<b ){ LogEst t = a; a = b; b = t; }
  if( a>b+49 ) return a;
  if( a>b+31 ) return a+1;
  return a+x[a-b];
}
LogEst logEstFromInteger(sqlite3_uint64 x){
  static LogEst a[] = { 0, 2, 3, 5, 6, 7, 8, 9 };
  LogEst y = 40;
  if( x<8 ){
    if( x<2 ) return 0;
    while( x<8 ){  y -= 10; x <<= 1; }
  }else{
    while( x>255 ){ y += 40; x >>= 4; }
    while( x>15 ){  y += 10; x >>= 1; }
  }
  return a[x&7] + y - 10;
}
static sqlite3_uint64 logEstToInt(LogEst x){
  sqlite3_uint64 n;
  if( x<10 ) return 1;
  n = x%10;
  x /= 10;
  if( n>=5 ) n -= 2;
  else if( n>=1 ) n -= 1;
  if( x>=3 ) return (n+8)<<(x-3);
  return (n+8)>>(3-x);
}
static LogEst logEstFromDouble(double x){
  sqlite3_uint64 a;
  LogEst e;
  assert( sizeof(x)==8 && sizeof(a)==8 );
  if( x<=0.0 ) return -32768;
  if( x<1.0 ) return -logEstFromDouble(1/x);
  if( x<1024.0 ) return logEstFromInteger((sqlite3_uint64)(1024.0*x)) - 100;
  if( x<=2000000000.0 ) return logEstFromInteger((sqlite3_uint64)x);
  memcpy(&a, &x, 8);
  e = (a>>52) - 1022;
  return e*10;
}

int isFloat(const char *z){
  while( z[0] ){
    if( z[0]=='.' || z[0]=='E' || z[0]=='e' ) return 1;
    z++;
  }
  return 0;
}

int main(int argc, char **argv){
  int i;
  int n = 0;
  LogEst a[100];
  for(i=1; i<argc; i++){
    const char *z = argv[i];
    if( z[0]=='+' ){
      if( n>=2 ){
        a[n-2] = logEstAdd(a[n-2],a[n-1]);
        n--;
      }
    }else if( z[0]=='x' ){
      if( n>=2 ){
        a[n-2] = logEstMultiply(a[n-2],a[n-1]);
        n--;
      }
    }else if( z[0]=='^' ){
      a[n++] = atoi(z+1);
    }else if( isFloat(z) ){
      a[n++] = logEstFromDouble(atof(z));
    }else{
      a[n++] = logEstFromInteger(atoi(z));
    }
  }
  for(i=n-1; i>=0; i--){
    if( a[i]<0 ){
      printf("%d (%f)\n", a[i], 1.0/(double)logEstToInt(-a[i]));
    }else{
      sqlite3_uint64 x = logEstToInt(a[i]+100)*100/1024;
      printf("%d (%lld.%02lld)\n", a[i], x/100, x%100);
    }
  }
  return 0;
}
