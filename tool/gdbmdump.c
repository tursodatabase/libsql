/*
** Copyright (c) 1999, 2000 D. Richard Hipp
**
** This program is free software; you can redistribute it and/or
** modify it under the terms of the GNU General Public
** License as published by the Free Software Foundation; either
** version 2 of the License, or (at your option) any later version.
**
** This program is distributed in the hope that it will be useful,
** but WITHOUT ANY WARRANTY; without even the implied warranty of
** MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
** General Public License for more details.
** 
** You should have received a copy of the GNU General Public
** License along with this library; if not, write to the
** Free Software Foundation, Inc., 59 Temple Place - Suite 330,
** Boston, MA  02111-1307, USA.
**
** Author contact information:
**   drh@hwaci.com
**   http://www.hwaci.com/drh/
**
*************************************************************************
** A utility to dump the entire contents of a GDBM table in a 
** readable format.
*/
#include <stdio.h>
#include <ctype.h>
#include <gdbm.h>
#include <stdlib.h>

static void print_data(char *zPrefix, datum p){
  int i, j;

  printf("%-5s: ", zPrefix);
  for(i=0; i<p.dsize; i+=20){
    for(j=i; j<p.dsize && j<i+20; j++){
      printf("%02x", 0xff & p.dptr[j]);
      if( (j&3)==3 ) printf(" ");
    }
    while( j<i+20 ){
      printf("  ");
      if( (j&3)==3 ) printf(" ");
      j++;
    }
    printf(" ");
    for(j=i; j<p.dsize && j<i+20; j++){
      int c = p.dptr[j];
      if( !isprint(c) ){ c = '.'; }
      putchar(c);
    }
    printf("\n");
    if( i+20<p.dsize ) printf("       ");
  }
}

static int gdbm_dump(char *zFilename){
  GDBM_FILE p;
  datum data, key, next;

  p = gdbm_open(zFilename, 0, GDBM_READER, 0, 0);
  if( p==0 ){
    fprintf(stderr,"can't open file \"%s\"\n", zFilename);
    return 1;
  }
  key = gdbm_firstkey(p);
  while( key.dptr ){
    print_data("key",key);
    data = gdbm_fetch(p, key);
    if( data.dptr ){
      print_data("data",data);
      free( data.dptr );
    }
    next = gdbm_nextkey(p, key);
    free( key.dptr );
    key = next;
    printf("\n");
  }
  gdbm_close(p);
  return 0;
}

int main(int argc, char **argv){
  int i;
  int nErr = 0;
  for(i=1; i<argc; i++){
    nErr += gdbm_dump(argv[i]);
  }
  return nErr;
}
