/*
** A utility for printing all or part of an SQLite database file.
*/
#include <stdio.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>


static int pagesize = 1024;     /* Size of a database page */
static int db = -1;             /* File descriptor for reading the DB */
static int mxPage = 0;          /* Last page number */
static int perLine = 16;        /* HEX elements to print per line */

typedef long long int i64;      /* Datatype for 64-bit integers */


/*
** Convert the var-int format into i64.  Return the number of bytes
** in the var-int.  Write the var-int value into *pVal.
*/
static int decodeVarint(const unsigned char *z, i64 *pVal){
  i64 v = 0;
  int i;
  for(i=0; i<8; i++){
    v = (v<<7) + (z[i]&0x7f);
    if( (z[i]&0x80)==0 ){ *pVal = v; return i+1; }
  }
  v = (v<<8) + (z[i]&0xff);
  *pVal = v;
  return 9;
}

/* Report an out-of-memory error and die.
*/
static void out_of_memory(void){
  fprintf(stderr,"Out of memory...\n");
  exit(1);
}

/*
** Read content from the file.
**
** Space to hold the content is obtained from malloc() and needs to be
** freed by the caller.
*/
static unsigned char *getContent(int ofst, int nByte){
  unsigned char *aData;
  aData = malloc(nByte);
  if( aData==0 ) out_of_memory();
  lseek(db, ofst, SEEK_SET);
  read(db, aData, nByte);
  return aData;
}

/*
** Print a range of bytes as hex and as ascii.
*/
static unsigned char *print_byte_range(
  int ofst,          /* First byte in the range of bytes to print */
  int nByte,         /* Number of bytes to print */
  int printOfst      /* Add this amount to the index on the left column */
){
  unsigned char *aData;
  int i, j;
  const char *zOfstFmt;

  if( ((printOfst+nByte)&~0xfff)==0 ){
    zOfstFmt = " %03x: ";
  }else if( ((printOfst+nByte)&~0xffff)==0 ){
    zOfstFmt = " %04x: ";
  }else if( ((printOfst+nByte)&~0xfffff)==0 ){
    zOfstFmt = " %05x: ";
  }else if( ((printOfst+nByte)&~0xffffff)==0 ){
    zOfstFmt = " %06x: ";
  }else{
    zOfstFmt = " %08x: ";
  }

  aData = getContent(ofst, nByte);
  for(i=0; i<nByte; i += perLine){
    fprintf(stdout, zOfstFmt, i+printOfst);
    for(j=0; j<perLine; j++){
      if( i+j>nByte ){
        fprintf(stdout, "   ");
      }else{
        fprintf(stdout,"%02x ", aData[i+j]);
      }
    }
    for(j=0; j<perLine; j++){
      if( i+j>nByte ){
        fprintf(stdout, " ");
      }else{
        fprintf(stdout,"%c", isprint(aData[i+j]) ? aData[i+j] : '.');
      }
    }
    fprintf(stdout,"\n");
  }
  return aData;
}

/*
** Print an entire page of content as hex
*/
static print_page(int iPg){
  int iStart;
  unsigned char *aData;
  iStart = (iPg-1)*pagesize;
  fprintf(stdout, "Page %d:   (offsets 0x%x..0x%x)\n",
          iPg, iStart, iStart+pagesize-1);
  aData = print_byte_range(iStart, pagesize, 0);
  free(aData);
}

/* Print a line of decode output showing a 4-byte integer.
*/
static print_decode_line(
  unsigned char *aData,      /* Content being decoded */
  int ofst, int nByte,       /* Start and size of decode */
  const char *zMsg           /* Message to append */
){
  int i, j;
  int val = aData[ofst];
  char zBuf[100];
  sprintf(zBuf, " %03x: %02x", ofst, aData[ofst]);
  i = strlen(zBuf);
  for(j=1; j<4; j++){
    if( j>=nByte ){
      sprintf(&zBuf[i], "   ");
    }else{
      sprintf(&zBuf[i], " %02x", aData[ofst+j]);
      val = val*256 + aData[ofst+j];
    }
    i += strlen(&zBuf[i]);
  }
  sprintf(&zBuf[i], "   %9d", val);
  printf("%s  %s\n", zBuf, zMsg);
}

/*
** Decode the database header.
*/
static void print_db_header(void){
  unsigned char *aData;
  aData = print_byte_range(0, 100, 0);
  printf("Decoded:\n");
  print_decode_line(aData, 16, 2, "Database page size");
  print_decode_line(aData, 18, 1, "File format write version");
  print_decode_line(aData, 19, 1, "File format read version");
  print_decode_line(aData, 20, 1, "Reserved space at end of page");
  print_decode_line(aData, 24, 4, "File change counter");
  print_decode_line(aData, 28, 4, "Size of database in pages");
  print_decode_line(aData, 32, 4, "Page number of first freelist page");
  print_decode_line(aData, 36, 4, "Number of freelist pages");
  print_decode_line(aData, 40, 4, "Schema cookie");
  print_decode_line(aData, 44, 4, "Schema format version");
  print_decode_line(aData, 48, 4, "Default page cache size");
  print_decode_line(aData, 52, 4, "Largest auto-vac root page");
  print_decode_line(aData, 56, 4, "Text encoding");
  print_decode_line(aData, 60, 4, "User version");
  print_decode_line(aData, 64, 4, "Incremental-vacuum mode");
  print_decode_line(aData, 68, 4, "meta[7]");
  print_decode_line(aData, 72, 4, "meta[8]");
  print_decode_line(aData, 76, 4, "meta[9]");
  print_decode_line(aData, 80, 4, "meta[10]");
  print_decode_line(aData, 84, 4, "meta[11]");
  print_decode_line(aData, 88, 4, "meta[12]");
  print_decode_line(aData, 92, 4, "Change counter for version number");
  print_decode_line(aData, 96, 4, "SQLite version number");
}

/*
** Create a description for a single cell.
*/
static int describeCell(unsigned char cType, unsigned char *a, char **pzDesc){
  int i;
  int nDesc = 0;
  int n = 0;
  int leftChild;
  i64 nPayload;
  i64 rowid;
  static char zDesc[100];
  i = 0;
  if( cType<=5 ){
    leftChild = ((a[0]*256 + a[1])*256 + a[2])*256 + a[3];
    a += 4;
    n += 4;
    sprintf(zDesc, "left-child: %d ", leftChild);
    nDesc = strlen(zDesc);
  }
  if( cType!=5 ){
    i = decodeVarint(a, &nPayload);
    a += i;
    n += i;
    sprintf(&zDesc[nDesc], "sz: %lld ", nPayload);
    nDesc += strlen(&zDesc[nDesc]);
  }
  if( cType==5 || cType==13 ){
    i = decodeVarint(a, &rowid);
    a += i;
    n += i;
    sprintf(&zDesc[nDesc], "rowid: %lld ", rowid);
    nDesc += strlen(&zDesc[nDesc]);
  }
  *pzDesc = zDesc;
  return n;
}

/*
** Decode a btree page
*/
static void decode_btree_page(unsigned char *a, int pgno, int hdrSize){
  const char *zType = "unknown";
  int nCell;
  int i;
  int iCellPtr;
  switch( a[0] ){
    case 2:  zType = "index interior node";  break;
    case 5:  zType = "table interior node";  break;
    case 10: zType = "index leaf";           break;
    case 13: zType = "table leaf";           break;
  }
  printf("Decode of btree page %d:\n", pgno);
  print_decode_line(a, 0, 1, zType);
  print_decode_line(a, 1, 2, "Offset to first freeblock");
  print_decode_line(a, 3, 2, "Number of cells on this page");
  nCell = a[3]*256 + a[4];
  print_decode_line(a, 5, 2, "Offset to cell content area");
  print_decode_line(a, 7, 1, "Fragmented byte count");
  if( a[0]==2 || a[0]==5 ){
    print_decode_line(a, 8, 4, "Right child");
    iCellPtr = 12;
  }else{
    iCellPtr = 8;
  }
  for(i=0; i<nCell; i++){
    int cofst = iCellPtr + i*2;
    char *zDesc;
    cofst = a[cofst]*256 + a[cofst+1];
    describeCell(a[0], &a[cofst-hdrSize], &zDesc);
    printf(" %03x: cell[%d] %s\n", cofst, i, zDesc);
  }
}

int main(int argc, char **argv){
  struct stat sbuf;
  unsigned char zPgSz[2];
  if( argc<2 ){
    fprintf(stderr,"Usage: %s FILENAME ?PAGE? ...\n", argv[0]);
    exit(1);
  }
  db = open(argv[1], O_RDONLY);
  if( db<0 ){
    fprintf(stderr,"%s: can't open %s\n", argv[0], argv[1]);
    exit(1);
  }
  zPgSz[0] = 0;
  zPgSz[1] = 0;
  lseek(db, 16, SEEK_SET);
  read(db, zPgSz, 2);
  pagesize = zPgSz[0]*256 + zPgSz[1];
  if( pagesize==0 ) pagesize = 1024;
  printf("Pagesize: %d\n", pagesize);
  fstat(db, &sbuf);
  mxPage = sbuf.st_size/pagesize;
  printf("Available pages: 1..%d\n", mxPage);
  if( argc==2 ){
    int i;
    for(i=1; i<=mxPage; i++) print_page(i);
  }else{
    int i;
    for(i=2; i<argc; i++){
      int iStart, iEnd;
      char *zLeft;
      if( strcmp(argv[i], "dbheader")==0 ){
        print_db_header();
        continue;
      }
      if( !isdigit(argv[i][0]) ){
        fprintf(stderr, "%s: unknown option: [%s]\n", argv[0], argv[i]);
        continue;
      }
      iStart = strtol(argv[i], &zLeft, 0);
      if( zLeft && strcmp(zLeft,"..end")==0 ){
        iEnd = mxPage;
      }else if( zLeft && zLeft[0]=='.' && zLeft[1]=='.' ){
        iEnd = strtol(&zLeft[2], 0, 0);
      }else if( zLeft && zLeft[0]=='b' ){
        int ofst, nByte, hdrSize;
        unsigned char *a;
        if( iStart==1 ){
          ofst = hdrSize = 100;
          nByte = pagesize-100;
        }else{
          hdrSize = 0;
          ofst = (iStart-1)*pagesize;
          nByte = pagesize;
        }
        a = getContent(ofst, nByte);
        decode_btree_page(a, iStart, hdrSize);
        free(a);
        continue;
      }else{
        iEnd = iStart;
      }
      if( iStart<1 || iEnd<iStart || iEnd>mxPage ){
        fprintf(stderr,
          "Page argument should be LOWER?..UPPER?.  Range 1 to %d\n",
          mxPage);
        exit(1);
      }
      while( iStart<=iEnd ){
        print_page(iStart);
        iStart++;
      }
    }
  }
  close(db);
}
