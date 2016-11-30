/*
** This is a test interface for the ossfuzz.c module.  The ossfuzz.c module
** is an adaptor for OSS-FUZZ.  (https://github.com/google/oss-fuzz)
**
** This program links against ossfuzz.c.  It reads files named on the
** command line and passes them one by one into ossfuzz.c.
*/
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include "sqlite3.h"

/*
** The entry point in ossfuzz.c that this routine will be calling
*/
int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size);


/*
** Read files named on the command-line and invoke the fuzzer for
** each one.
*/
int main(int argc, char **argv){
  FILE *in;
  int i;
  int nErr = 0;
  uint8_t *zBuf = 0;
  size_t sz;

  for(i=1; i<argc; i++){
    const char *zFilename = argv[i];
    in = fopen(zFilename, "rb");
    if( in==0 ){
      fprintf(stderr, "cannot open \"%s\"\n", zFilename);
      nErr++;
      continue;
    }
    fseek(in, 0, SEEK_END);
    sz = ftell(in);
    rewind(in);
    zBuf = realloc(zBuf, sz);
    if( zBuf==0 ){
      fprintf(stderr, "cannot malloc() for %d bytes\n", (int)sz);
      exit(1);
    }
    if( fread(zBuf, sz, 1, in)!=1 ){
      fprintf(stderr, "cannot read %d bytes from \"%s\"\n",
                       (int)sz, zFilename);
      nErr++;
    }else{
      printf("%s... ", zFilename);
      fflush(stdout);
      (void)LLVMFuzzerTestOneInput(zBuf, sz);
      printf("ok\n");
    }
    fclose(in);
  }
  free(zBuf);
  return nErr;
}
