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
** A utility print statistics about the content of a GDBM database.
*/
#include <stdio.h>
#include <ctype.h>
#include <gdbm.h>
#include <stdlib.h>

static int bins[] = {
  4, 8, 12, 16, 24,
  32, 40, 48, 56, 64,
  80, 96, 112, 128, 144, 160, 176, 192, 208, 224, 240, 256,
  288, 320, 352, 384, 416, 448, 480, 512, 1024, 2048, 4096, 8192,
  16384, 32768, 65536,
};
#define NBIN (sizeof(bins)/sizeof(bins[0])+1)

static int gAllSize[NBIN];
static int gCount;
static int gTotal;
static int gMax;
static int gKey;
static int gMaxKey;

static int gdbm_stat(char *zFilename){
  GDBM_FILE p;
  datum data, key, next;
  int nEntry = 0;
  int keyTotal = 0;
  int dataTotal = 0;
  int allTotal = 0;
  int lMax = 0;
  int keySize[NBIN], dataSize[NBIN], allSize[NBIN];
  int i, priorSize;

  p = gdbm_open(zFilename, 0, GDBM_READER, 0, 0);
  if( p==0 ){
    fprintf(stderr,"can't open file \"%s\"\n", zFilename);
    return 1;
  }
  for(i=0; i<NBIN; i++){
    keySize[i] = 0;
    dataSize[i] = 0;
    allSize[i] = 0;
  }
  key = gdbm_firstkey(p);
  while( key.dptr ){
    int all;
    nEntry++;
    gCount++;
    keyTotal += key.dsize;
    for(i=0; i<NBIN-1 && key.dsize>bins[i]; i++){}
    keySize[i]++;
    gKey += key.dsize;
    if( key.dsize>gMaxKey ) gMaxKey = key.dsize;
    data = gdbm_fetch(p, key);
    if( data.dptr==0 ) data.dsize = 0;
    dataTotal += data.dsize;
    for(i=0; i<NBIN-1 && data.dsize>bins[i]; i++){}
    dataSize[i]++;
    all = key.dsize + data.dsize;
    allTotal += all;
    gTotal += all;
    if( all>gMax ) gMax = all;
    if( all>lMax ) lMax = all;
    for(i=0; i<NBIN-1 && all>bins[i]; i++){}
    allSize[i]++;
    gAllSize[i]++;
    next = gdbm_nextkey(p, key);
    free( key.dptr );
    key = next;
  }
  gdbm_close(p);
  printf("%s:\n", zFilename);
  printf("  entries: %d\n", nEntry);
  printf("  keysize: %d (%d per entry)\n", 
       keyTotal, nEntry>0 ? (keyTotal+nEntry-1)/nEntry : 0);
  printf("  datasize: %d (%d per entry)\n",
       dataTotal, nEntry>0 ? (dataTotal+nEntry-1)/nEntry : 0);
  printf("  size: %d (%d per entry)\n",
       allTotal, nEntry>0 ? (allTotal+nEntry-1)/nEntry : 0);
  priorSize = 0;
  for(i=0; i<NBIN-1; i++){
    if( keySize[i]==0 && dataSize[i]==0 ) continue;
    printf("%5d..%-5d   %7d  %7d  %7d\n", priorSize, bins[i], keySize[i],
      dataSize[i], allSize[i]);
    priorSize = bins[i]+1;
  }
  if( keySize[NBIN-1]>0 || dataSize[NBIN-1]>0 ){
    printf("%5d..%-5d   %7d  %7d  %7d\n", priorSize, lMax,
       keySize[NBIN-1], dataSize[NBIN-1], allSize[NBIN-1]);
  }
  return 0;
}

int main(int argc, char **argv){
  int i, ps, sum;
  int nErr = 0;
  for(i=1; i<argc; i++){
    nErr += gdbm_stat(argv[i]);
  }
  printf("*****************************************************************\n");
  printf("Entries:      %d\n", gCount);
  printf("Size:         %d\n", gTotal);
  printf("Avg Size:     %d\n", gCount>0 ? (gTotal + gCount - 1)/gCount : 0);
  printf("Key Size:     %d\n", gKey);
  printf("Avg Key Size: %d\n", gCount>0 ? (gKey + gCount - 1)/gCount : 0);
  printf("Max Key Size: %d\n\n", gMaxKey);
  ps = 0;
  sum = 0;
  for(i=0; i<NBIN-1; i++){
    if( gAllSize[i]==0 ) continue;
    sum += gAllSize[i];
    printf("%5d..%-5d   %8d  %3d%%\n", 
      ps, bins[i], gAllSize[i], sum*100/gCount);
    ps = bins[i]+1;
  }
  if( gAllSize[NBIN-1]>0 ){
    printf("%5d..%-5d   %8d  100%%\n", ps, gMax, gAllSize[NBIN-1]);
  }
  return nErr;
}
