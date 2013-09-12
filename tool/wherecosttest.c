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
** integers and WhereCost values and back again and for doing simple
** arithmetic operations (multiple and add) on WhereCost values.
**
** Usage:
**
**      ./wherecosttest ARGS
**
** Arguments:
**
**    'x'    Multiple the top two elements of the stack
**    '+'    Add the top two elements of the stack
**    NUM    Convert NUM from integer to WhereCost and push onto the stack
**   ^NUM    Interpret NUM as a WhereCost and push onto stack.
**
** Examples:
**
** To convert 123 from WhereCost to integer:
** 
**         ./wherecosttest ^123
**
** To convert 123456 from integer to WhereCost:
**
**         ./wherecosttest 123456
**
*/
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>

typedef unsigned short int WhereCost;  /* 10 times log2() */

WhereCost whereCostMultiply(WhereCost a, WhereCost b){ return a+b; }
WhereCost whereCostAdd(WhereCost a, WhereCost b){
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
  if( a<b ){ WhereCost t = a; a = b; b = t; }
  if( a>b+49 ) return a;
  if( a>b+31 ) return a+1;
  return a+x[a-b];
}
WhereCost whereCostFromInteger(int x){
  static WhereCost a[] = { 0, 2, 3, 5, 6, 7, 8, 9 };
  WhereCost y = 40;
  if( x<8 ){
    if( x<2 ) return 0;
    while( x<8 ){  y -= 10; x <<= 1; }
  }else{
    while( x>255 ){ y += 40; x >>= 4; }
    while( x>15 ){  y += 10; x >>= 1; }
  }
  return a[x&7] + y - 10;
}
static unsigned long int whereCostToInt(WhereCost x){
  unsigned long int n;
  if( x<10 ) return 1;
  n = x%10;
  x /= 10;
  if( n>=5 ) n -= 2;
  else if( n>=1 ) n -= 1;
  if( x>=3 ) return (n+8)<<(x-3);
  return (n+8)>>(3-x);
}

int main(int argc, char **argv){
  int i;
  int n = 0;
  WhereCost a[100];
  for(i=1; i<argc; i++){
    const char *z = argv[i];
    if( z[0]=='+' ){
      if( n>=2 ){
        a[n-2] = whereCostAdd(a[n-2],a[n-1]);
        n--;
      }
    }else if( z[0]=='x' ){
      if( n>=2 ){
        a[n-2] = whereCostMultiply(a[n-2],a[n-1]);
        n--;
      }
    }else if( z[0]=='^' ){
      a[n++] = atoi(z+1);
    }else{
      a[n++] = whereCostFromInteger(atoi(z));
    }
  }
  for(i=n-1; i>=0; i--){
    printf("%d (%lu)\n", a[i], whereCostToInt(a[i]));
  }
  return 0;
}
