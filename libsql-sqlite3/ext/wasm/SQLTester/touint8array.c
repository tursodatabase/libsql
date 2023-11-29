/*
** 2023-08-29
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains a tool for writing out the contents of stdin as
** a comma-separated list of numbers, one per byte.
*/

#include <stdio.h>
int main(int argc, char const **argv){
  int i;
  int rc = 0, colWidth = 30;
  int ch;
  printf("[");
  for( i=0; EOF!=(ch = fgetc(stdin)); ++i ){
    if( 0!=i ) printf(",");
    if( i && 0==(i%colWidth) ) puts("");
    printf("%d",ch);
  }
  printf("]");
  return rc;
}
