/*
** Copyright 2008 D. Richard Hipp and Hipp, Wyrick & Company, Inc.
** All Rights Reserved
**
******************************************************************************
**
** This file implements a stand-alone utility program that converts
** a binary file (usually an SQLite database) into a text format that
** is compact and friendly to human-readers.
**
** Usage:
**
**         dbtotxt [OPTIONS] FILENAME
**
** where OPTIONS are zero or more of:
**
**    --for-cli          prepending '.open --hexdb' to the output
**
**    --script           The input file is expected to start with a
**                       zero-terminated SQL string.  Output the
**                       ".open --hexdb" header, then the database
**                       then the SQL.
**
**    --pagesize N       set the database page size for later reading
**
** The translation of the database appears on standard output.  If the
** --pagesize command-line option is omitted, then the page size is taken
** from the database header.
**
** Compactness is achieved by suppressing lines of all zero bytes.  This
** works well at compressing test databases that are mostly empty.  But
** the output will probably be lengthy for a real database containing lots
** of real content.  For maximum compactness, it is suggested that test
** databases be constructed with "zeroblob()" rather than "randomblob()"
** used for filler content and with "PRAGMA secure_delete=ON" selected to
** zero-out deleted content.
*/
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
 
/* Return true if the line is all zeros */
static int allZero(unsigned char *aLine){
  int i;
  for(i=0; i<16 && aLine[i]==0; i++){}
  return i==16;
}

int main(int argc, char **argv){
  int pgsz = 0;               /* page size */
  int forCli = 0;             /* whether to prepend with .open */
  int bSQL = 0;               /* Expect and SQL prefix */
  long szFile;                /* Size of the input file in bytes */
  FILE *in;                   /* Input file */
  int nSQL;                   /* Number of bytes of script */
  int i, j;                   /* Loop counters */
  int nErr = 0;               /* Number of errors */
  const char *zInputFile = 0; /* Name of the input file */
  const char *zBaseName = 0;  /* Base name of the file */
  int lastPage = 0;           /* Last page number shown */
  int iPage;                  /* Current page number */
  unsigned char *aData = 0;   /* All data */
  unsigned char *aLine;       /* A single line of the file */
  unsigned char *aHdr;        /* File header */
  unsigned char bShow[256];   /* Characters ok to display */
  memset(bShow, '.', sizeof(bShow));
  for(i=' '; i<='~'; i++){
    if( i!='{' && i!='}' && i!='"' && i!='\\' ) bShow[i] = (unsigned char)i;
  }
  for(i=1; i<argc; i++){
    if( argv[i][0]=='-' ){
      const char *z = argv[i];
      z++;
      if( z[0]=='-' ) z++;
      if( strcmp(z,"pagesize")==0 ){
        i++;
        pgsz = atoi(argv[i]);
        if( pgsz<512 || pgsz>65536 || (pgsz&(pgsz-1))!=0 ){
          fprintf(stderr, "Page size must be a power of two between"
                          " 512 and 65536.\n");
          nErr++;
        }
        continue;
      }else if( strcmp(z,"for-cli")==0 ){
        forCli = 1;
        continue;
      }else if( strcmp(z,"script")==0 ){
        forCli = 1;
        bSQL = 1;
        continue;
      }
      fprintf(stderr, "Unknown option: %s\n", argv[i]);
      nErr++;
    }else if( zInputFile ){
      fprintf(stderr, "Already using a different input file: [%s]\n", argv[i]);
      nErr++;
    }else{
      zInputFile = argv[i];
    }
  }
  if( zInputFile==0 ){
    fprintf(stderr, "No input file specified.\n");
    nErr++;
  }
  if( nErr ){
    fprintf(stderr, 
       "Usage: %s [--pagesize N] [--script] [--for-cli] FILENAME\n", argv[0]);
    exit(1);
  }
  in = fopen(zInputFile, "rb");
  if( in==0 ){
    fprintf(stderr, "Cannot open input file [%s]\n", zInputFile);
    exit(1);
  }
  fseek(in, 0, SEEK_END);
  szFile = ftell(in);
  rewind(in);
  if( szFile<100 ){
    fprintf(stderr, "File too short. Minimum size is 100 bytes.\n");
    exit(1);
  }
  aData = malloc( szFile+16 );
  if( aData==0 ){
    fprintf(stderr, "Failed to allocate %ld bytes\n", szFile);
    exit(1);
  }
  if( fread(aData, szFile, 1, in)!=1 ){
    fprintf(stderr, "Cannot read file info memory\n");
    exit(1);
  }
  memset(aData+szFile, 0, 16);
  fclose(in);
  if( bSQL ){
    for(i=0; i<szFile && aData[i]!=0; i++){}
    if( i==szFile ){
      fprintf(stderr, "No zero terminator on SQL script\n");
      exit(1);
    }
    nSQL = i+1;
    if( szFile - nSQL<100 ){
      fprintf(stderr, "Less than 100 bytes in the database\n");
      exit(1);
    }
  }else{
    nSQL = 0;
  }
  aHdr = aData + nSQL;
  if( pgsz==0 ){
    pgsz = (aHdr[16]<<8) | aHdr[17];
    if( pgsz==1 ) pgsz = 65536;
    if( pgsz<512 || (pgsz&(pgsz-1))!=0 ){
      fprintf(stderr, "Invalid page size in header: %d\n", pgsz);
      exit(1);
    }
  }
  zBaseName = zInputFile;
  for(i=0; zInputFile[i]; i++){
    if( zInputFile[i]=='/' && zInputFile[i+1]!=0 ) zBaseName = zInputFile+i+1;
  }
  if( forCli ){
    printf(".open --hexdb\n");
  }
  printf("| size %d pagesize %d filename %s\n",(int)szFile,pgsz,zBaseName);
  for(i=nSQL; i<szFile; i+=16){
    aLine = aData+i;
    if( allZero(aLine) ) continue;
    iPage = i/pgsz + 1;
    if( lastPage!=iPage ){
      printf("| page %d offset %d\n", iPage, (iPage-1)*pgsz);
      lastPage = iPage;
    }
    printf("|  %5d:", i-(iPage-1)*pgsz);
    for(j=0; j<16; j++) printf(" %02x", aLine[j]);
    printf("   ");
    for(j=0; j<16; j++){
      unsigned char c = (unsigned char)aLine[j];
      fputc( bShow[c], stdout);
    }
    fputc('\n', stdout);
  }
  printf("| end %s\n", zBaseName);
  if( nSQL>0 ){
    printf("%s\n", aData);
  }
  free( aData );
  return 0;
}
