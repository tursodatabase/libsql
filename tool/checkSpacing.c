/*
** This program checks for formatting problems in source code:
**
**    *  Any use of tab characters
**    *  White space at the end of a line
**    *  Blank lines at the end of a file
**
** Any violations are reported.
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void checkSpacing(const char *zFile, int crok){
  FILE *in = fopen(zFile, "rb");
  int i;
  int seenSpace;
  int seenTab;
  int ln = 0;
  int lastNonspace = 0;
  char zLine[2000];
  if( in==0 ){
    printf("cannot open %s\n", zFile);
    return;
  }
  while( fgets(zLine, sizeof(zLine), in) ){
    seenSpace = 0;
    seenTab = 0;
    ln++;
    for(i=0; zLine[i]; i++){
      if( zLine[i]=='\t' && seenTab==0 ){
        printf("%s:%d: tab (\\t) character\n", zFile, ln);
        seenTab = 1;
      }else if( zLine[i]=='\r' ){
        if( !crok ){
          printf("%s:%d: carriage-return (\\r) character\n", zFile, ln);
        }
      }else if( zLine[i]==' ' ){
        seenSpace = 1;
      }else if( zLine[i]!='\n' ){
        lastNonspace = ln;
        seenSpace = 0;
      }
    }
    if( seenSpace ){
      printf("%s:%d: whitespace at end-of-line\n", zFile, ln);
    }
  }
  fclose(in);
  if( lastNonspace<ln ){
    printf("%s:%d: blank lines at end of file (%d)\n",
        zFile, ln, ln - lastNonspace);
  }
}

int main(int argc, char **argv){
  int i;
  int crok = 0;
  for(i=1; i<argc; i++){
    if( strcmp(argv[i], "--crok")==0 ){
      crok = 1;
    }else{
      checkSpacing(argv[i], crok);
    }
  }
  return 0;
}
