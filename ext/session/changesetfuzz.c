/*
** 2018-11-01
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains code to implement the "changesetfuzz" command 
** line utility for fuzzing changeset blobs without corrupting them.
*/


/************************************************************************
** USAGE:
**
** This program may be invoked in two ways:
**
**   changesetfuzz INPUT
**   changesetfuzz INPUT SEED N
**
** Argument INPUT must be the name of a file containing a binary changeset.
** In the first form above, this program outputs a human-readable version
** of the same changeset. This is chiefly for debugging.
**
** In the second form, arguments SEED and N must both be integers. In this
** case, this program writes N binary changesets to disk. Each output
** changeset is a slightly modified - "fuzzed" - version of the input. 
** The output changesets are written to files name "INPUT-$n", where $n is 
** an integer between 0 and N-1, inclusive. Output changesets are always
** well-formed. Parameter SEED is used to seed the PRNG - any two 
** invocations of this program with the same SEED and input changeset create
** the same N output changesets.
**
** The ways in which an input changeset may be fuzzed are as follows:
**
**   1. Any two values within the changeset may be exchanged.
**
**   2. Any TEXT, BLOB, INTEGER or REAL value within the changeset 
**      may have a single bit of its content flipped.
**
**   3. Any value within a changeset may be replaced by a pseudo-randomly
**      generated value.
**
** The above operations never set a PRIMARY KEY column to NULL. Nor do they
** set any value to "undefined", or replace any "undefined" value with
** another. Any such operation risks producing a changeset that is not 
** well-formed.
**
**   4. A single change may be duplicated.
**
**   5. A single change may be removed, so long as this does not mean that
**      there are zero changes following a table-header within the changeset.
**
**   6. A single change may have its type (INSERT, DELETE, UPDATE) changed.
**      If an INSERT is changed to a DELETE (or vice versa), the type is
**      simply changed - no other modifications are required. If an INSERT
**      or DELETE is changed to an UPDATE, then the single record is duplicated
**      (as both the old.* and new.* records of the new UPDATE change). If an
**      UPDATE is changed to a DELETE or INSERT, the new.* record is discarded
**      and any "undefined" fields replaced with pseudo-randomly generated
**      values. 
**
**   7. An UPDATE change that modifies N table columns may be modified so
**      that it updates N-1 columns, so long as (N>1).
**
*/

#include "sqlite3.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <ctype.h>

#define FUZZ_VALUE_SUB     1      /* Replace one value with a copy of another */
#define FUZZ_VALUE_MOD     2      /* Modify content by 1 bit */
#define FUZZ_VALUE_RND     3      /* Replace with pseudo-random value */

#define FUZZ_CHANGE_DUP    4
#define FUZZ_CHANGE_DEL    5
#define FUZZ_CHANGE_TYPE   6
#define FUZZ_CHANGE_FIELD  7

#if 0
#define FUZZ_COLUMN_ADD    1      /* Add column to table definition */
#define FUZZ_COLUMN_DEL    2      /* Remove column from table definition */
#define FUZZ_PK_ADD        3      /* Add a PK column */
#define FUZZ_PK_DEL        4      /* Delete a PK column */
#define FUZZ_NAME_CHANGE   5      /* Change a table name */
#endif



typedef unsigned char u8;
typedef sqlite3_uint64 u64;
typedef sqlite3_int64 i64;
typedef unsigned int u32;

/*
** Show a usage message on stderr then quit.
*/
static void usage(const char *argv0){
  fprintf(stderr, "Usage: %s FILENAME ?SEED N?\n", argv0);
  exit(1);
}

/*
** Read the content of a disk file into an in-memory buffer
*/
static void fuzzReadFile(const char *zFilename, int *pSz, void **ppBuf){
  FILE *f;
  int sz;
  void *pBuf;
  f = fopen(zFilename, "rb");
  if( f==0 ){
    fprintf(stderr, "cannot open \"%s\" for reading\n", zFilename);
    exit(1);
  }
  fseek(f, 0, SEEK_END);
  sz = (int)ftell(f);
  rewind(f);
  pBuf = sqlite3_malloc( sz ? sz : 1 );
  if( pBuf==0 ){
    fprintf(stderr, "cannot allocate %d to hold content of \"%s\"\n",
            sz, zFilename);
    exit(1);
  }
  if( sz>0 ){
    if( fread(pBuf, sz, 1, f)!=1 ){
      fprintf(stderr, "cannot read all %d bytes of \"%s\"\n", sz, zFilename);
      exit(1);
    }
    fclose(f);
  }
  *pSz = sz;
  *ppBuf = pBuf;
}

static void fuzzWriteFile(const char *zFilename, void *pBuf, int nBuf){
  FILE *f;
  f = fopen(zFilename, "wb");
  if( f==0 ){
    fprintf(stderr, "cannot open \"%s\" for writing\n", zFilename);
    exit(1);
  }
  if( fwrite(pBuf, nBuf, 1, f)!=1 ){
    fprintf(stderr, "cannot write to \"%s\"\n", zFilename);
    exit(1);
  }
  fclose(f);
}

static int fuzzCorrupt(){
  return SQLITE_CORRUPT;
}

/*************************************************************************
** The following block is a copy of the implementation of SQLite function
** sqlite3_randomness. This version has two important differences:
**
**   1. It always uses the same seed. So the sequence of random data output
**      is the same for every run of the program.
**
**   2. It is not threadsafe.
*/
static struct sqlite3PrngType {
  unsigned char i, j;             /* State variables */
  unsigned char s[256];           /* State variables */
} sqlite3Prng = {
    0xAF, 0x28,
  {
    0x71, 0xF5, 0xB4, 0x6E, 0x80, 0xAB, 0x1D, 0xB8, 
    0xFB, 0xB7, 0x49, 0xBF, 0xFF, 0x72, 0x2D, 0x14, 
    0x79, 0x09, 0xE3, 0x78, 0x76, 0xB0, 0x2C, 0x0A, 
    0x8E, 0x23, 0xEE, 0xDF, 0xE0, 0x9A, 0x2F, 0x67, 
    0xE1, 0xBE, 0x0E, 0xA7, 0x08, 0x97, 0xEB, 0x77, 
    0x78, 0xBA, 0x9D, 0xCA, 0x49, 0x4C, 0x60, 0x9A, 
    0xF6, 0xBD, 0xDA, 0x7F, 0xBC, 0x48, 0x58, 0x52, 
    0xE5, 0xCD, 0x83, 0x72, 0x23, 0x52, 0xFF, 0x6D, 
    0xEF, 0x0F, 0x82, 0x29, 0xA0, 0x83, 0x3F, 0x7D, 
    0xA4, 0x88, 0x31, 0xE7, 0x88, 0x92, 0x3B, 0x9B, 
    0x3B, 0x2C, 0xC2, 0x4C, 0x71, 0xA2, 0xB0, 0xEA, 
    0x36, 0xD0, 0x00, 0xF1, 0xD3, 0x39, 0x17, 0x5D, 
    0x2A, 0x7A, 0xE4, 0xAD, 0xE1, 0x64, 0xCE, 0x0F, 
    0x9C, 0xD9, 0xF5, 0xED, 0xB0, 0x22, 0x5E, 0x62, 
    0x97, 0x02, 0xA3, 0x8C, 0x67, 0x80, 0xFC, 0x88, 
    0x14, 0x0B, 0x15, 0x10, 0x0F, 0xC7, 0x40, 0xD4, 
    0xF1, 0xF9, 0x0E, 0x1A, 0xCE, 0xB9, 0x1E, 0xA1, 
    0x72, 0x8E, 0xD7, 0x78, 0x39, 0xCD, 0xF4, 0x5D, 
    0x2A, 0x59, 0x26, 0x34, 0xF2, 0x73, 0x0B, 0xA0, 
    0x02, 0x51, 0x2C, 0x03, 0xA3, 0xA7, 0x43, 0x13, 
    0xE8, 0x98, 0x2B, 0xD2, 0x53, 0xF8, 0xEE, 0x91, 
    0x7D, 0xE7, 0xE3, 0xDA, 0xD5, 0xBB, 0xC0, 0x92, 
    0x9D, 0x98, 0x01, 0x2C, 0xF9, 0xB9, 0xA0, 0xEB, 
    0xCF, 0x32, 0xFA, 0x01, 0x49, 0xA5, 0x1D, 0x9A, 
    0x76, 0x86, 0x3F, 0x40, 0xD4, 0x89, 0x8F, 0x9C, 
    0xE2, 0xE3, 0x11, 0x31, 0x37, 0xB2, 0x49, 0x28, 
    0x35, 0xC0, 0x99, 0xB6, 0xD0, 0xBC, 0x66, 0x35, 
    0xF7, 0x83, 0x5B, 0xD7, 0x37, 0x1A, 0x2B, 0x18, 
    0xA6, 0xFF, 0x8D, 0x7C, 0x81, 0xA8, 0xFC, 0x9E, 
    0xC4, 0xEC, 0x80, 0xD0, 0x98, 0xA7, 0x76, 0xCC, 
    0x9C, 0x2F, 0x7B, 0xFF, 0x8E, 0x0E, 0xBB, 0x90, 
    0xAE, 0x13, 0x06, 0xF5, 0x1C, 0x4E, 0x52, 0xF7
  }
};

/* 
** Generate and return single random byte 
*/
static unsigned char fuzzRandomByte(void){
  unsigned char t;
  sqlite3Prng.i++;
  t = sqlite3Prng.s[sqlite3Prng.i];
  sqlite3Prng.j += t;
  sqlite3Prng.s[sqlite3Prng.i] = sqlite3Prng.s[sqlite3Prng.j];
  sqlite3Prng.s[sqlite3Prng.j] = t;
  t += sqlite3Prng.s[sqlite3Prng.i];
  return sqlite3Prng.s[t];
}

/*
** Return N random bytes.
*/
static void fuzzRandomBlob(int nBuf, unsigned char *zBuf){
  int i;
  for(i=0; i<nBuf; i++){
    zBuf[i] = fuzzRandomByte();
  }
}

/*
** Return a random integer between 0 and nRange (not inclusive).
*/
static unsigned int fuzzRandomInt(unsigned int nRange){
  unsigned int ret;
  assert( nRange>0 );
  fuzzRandomBlob(sizeof(ret), (unsigned char*)&ret);
  return (ret % nRange);
}

static u64 fuzzRandomU64(){
  u64 ret;
  fuzzRandomBlob(sizeof(ret), (unsigned char*)&ret);
  return ret;
}

static void fuzzRandomSeed(unsigned int iSeed){
  int i;
  for(i=0; i<256; i+=4){
    sqlite3Prng.s[i] ^= ((iSeed >> 24) & 0xFF);
    sqlite3Prng.s[i+1] ^= ((iSeed >> 16) & 0xFF);
    sqlite3Prng.s[i+2] ^= ((iSeed >>  8) & 0xFF);
    sqlite3Prng.s[i+3] ^= ((iSeed >>  0) & 0xFF);
  }
}

/*
** End of code for generating pseudo-random values.
*************************************************************************/

typedef struct FuzzChangeset FuzzChangeset;
typedef struct FuzzChangesetGroup FuzzChangesetGroup;
typedef struct FuzzChange FuzzChange;

#define FUZZER_AVAL_SZ 512

/* 
** Object containing partially parsed changeset.
*/
struct FuzzChangeset {
  FuzzChangesetGroup **apGroup;   /* Array of groups in changeset */
  int nGroup;                     /* Number of items in list pGroup */
  u8 *aVal[FUZZER_AVAL_SZ];       /* Array of first few values in changeset */
  int nVal;                       /* Number of used slots in aVal[] */
  int nChange;                    /* Number of changes in changeset */
  int nUpdate;                    /* Number of UPDATE changes in changeset */
};

struct FuzzChangesetGroup {
  const char *zTab;               /* Name of table */
  int nCol;                       /* Number of columns in table */
  u8 *aPK;                        /* PK array for this table */
  u8 *aChange;                    /* Buffer containing array of changes */
  int szChange;                   /* Size of buffer aChange[] in bytes */
  int nChange;                    /* Number of changes in buffer aChange[] */
  FuzzChangesetGroup *pNextGroup;
};

/*
** Description of a fuzz change to be applied to a changeset.
*/
struct FuzzChange {
  int eType;                      /* One of the FUZZ_* constants above */
  int iChange;                    /* Change to modify */
  u8 *pSub1;
  u8 *pSub2;
  u8 aSub[128];                   /* Substitute value */

  int iCurrent;                   /* Current change number */
};

static void *fuzzMalloc(int nByte){
  void *pRet = sqlite3_malloc(nByte);
  if( pRet ){
    memset(pRet, 0, nByte);
  }
  return pRet;
}

static void fuzzFree(void *p){
  sqlite3_free(p);
}

static int fuzzGetVarint(u8 *p, int *pnVal){
  int i;
  sqlite3_uint64 nVal = 0;
  for(i=0; i<9; i++){
    nVal = (nVal<<7) + (p[i] & 0x7F);
    if( (p[i] & 0x80)==0 ){
      i++;
      break;
    }
  }
  *pnVal = (int)nVal;
  return i;
}

static int fuzzPutVarint(u8 *p, int nVal){
  assert( nVal>0 && nVal<2097152 );
  if( nVal<128 ){
    p[0] = nVal;
    return 1;
  }
  if( nVal<16384 ){
    p[0] = ((nVal >> 7) & 0x7F) | 0x80;
    p[1] = (nVal & 0x7F);
    return 2;
  }

  p[0] = ((nVal >> 14) & 0x7F) | 0x80;
  p[1] = ((nVal >> 7) & 0x7F) | 0x80;
  p[2] = (nVal & 0x7F);
  return 3;
}

/* Load an unaligned and unsigned 32-bit integer */
#define FUZZ_UINT32(x) (((u32)(x)[0]<<24)|((x)[1]<<16)|((x)[2]<<8)|(x)[3])

/*
** Read a 64-bit big-endian integer value from buffer aRec[]. Return
** the value read.
*/
static sqlite3_int64 fuzzGetI64(u8 *aRec){
  u64 x = FUZZ_UINT32(aRec);
  u32 y = FUZZ_UINT32(aRec+4);
  x = (x<<32) + y;
  return (sqlite3_int64)x;
}

static void fuzzPutU64(u8 *aRec, u64 iVal){
  aRec[0] = (iVal>>56) & 0xFF;
  aRec[1] = (iVal>>48) & 0xFF;
  aRec[2] = (iVal>>40) & 0xFF;
  aRec[3] = (iVal>>32) & 0xFF;
  aRec[4] = (iVal>>24) & 0xFF;
  aRec[5] = (iVal>>16) & 0xFF;
  aRec[6] = (iVal>> 8) & 0xFF;
  aRec[7] = (iVal)     & 0xFF;
}

static int fuzzParseHeader(u8 **ppHdr, u8 *pEnd, FuzzChangesetGroup **ppGrp){
  int rc = SQLITE_OK;
  FuzzChangesetGroup *pGrp;

  assert( pEnd>(*ppHdr) );
  pGrp = (FuzzChangesetGroup*)fuzzMalloc(sizeof(FuzzChangesetGroup));
  if( !pGrp ){
    rc = SQLITE_NOMEM;
  }else{
    u8 *p = *ppHdr;
    if( p[0]!='T' ){
      rc = fuzzCorrupt();
    }else{
      p++;
      p += fuzzGetVarint(p, &pGrp->nCol);
      pGrp->aPK = p;
      p += pGrp->nCol;
      pGrp->zTab = (const char*)p;
      p = &p[strlen(p)+1];

      if( p>=pEnd ){
        rc = fuzzCorrupt();
      }
    }
    *ppHdr = p;
  }

  if( rc!=SQLITE_OK ){
    fuzzFree(pGrp);
    pGrp = 0;
  }

  *ppGrp = pGrp;
  return rc;
}

static int fuzzChangeSize(u8 *p, int *pSz){
  u8 eType = p[0];
  switch( eType ){
    case 0x00:                    /* undefined */
    case 0x05:                    /* null */
      *pSz = 1;
      break;

    case 0x01:                    /* integer */
    case 0x02:                    /* real */
      *pSz = 9;
      break;

    case 0x03:                    /* text */
    case 0x04: {                  /* blob */
      int nTxt;
      int sz;
      sz = fuzzGetVarint(&p[1], &nTxt);
      *pSz = 1 + sz + nTxt;
      break;
    }

    default:
      return fuzzCorrupt();
  }
  return SQLITE_OK;
}

static int fuzzParseRecord(u8 **ppRec, u8 *pEnd, FuzzChangeset *pParse){
  int rc = SQLITE_OK;
  int nCol = pParse->apGroup[pParse->nGroup-1]->nCol;
  int i;
  u8 *p = *ppRec;

  for(i=0; rc==SQLITE_OK && i<nCol && p<pEnd; i++){
    int sz;
    if( pParse->nVal<FUZZER_AVAL_SZ ){
      pParse->aVal[pParse->nVal++] = p;
    }
    rc = fuzzChangeSize(p, &sz);
    p += sz;
  }

  if( rc==SQLITE_OK && i<nCol ){
    rc = fuzzCorrupt();
  }

  *ppRec = p;
  return rc;
}

static int fuzzParseChanges(u8 **ppData, u8 *pEnd, FuzzChangeset *pParse){
  FuzzChangesetGroup *pGrp = pParse->apGroup[pParse->nGroup-1];
  int rc = SQLITE_OK;
  u8 *p = *ppData;

  pGrp->aChange = p;
  while( rc==SQLITE_OK && p<pEnd && p[0]!='T' ){
    u8 eOp = p[0];
    u8 bIndirect = p[1];

    p += 2;
    if( eOp==SQLITE_UPDATE ){
      pParse->nUpdate++;
      rc = fuzzParseRecord(&p, pEnd, pParse);
    }else if( eOp!=SQLITE_INSERT && eOp!=SQLITE_DELETE ){
      rc = fuzzCorrupt();
    }
    if( rc==SQLITE_OK ){
      rc = fuzzParseRecord(&p, pEnd, pParse);
    }
    pGrp->nChange++;
    pParse->nChange++;
  }
  pGrp->szChange = p - pGrp->aChange;

  *ppData = p;
  return rc;
}

static int fuzzParseChangeset(
  u8 *pChangeset,                 /* Buffer containing changeset */
  int nChangeset,                 /* Size of buffer in bytes */
  FuzzChangeset *pParse           /* OUT: Results of parse */
){
  u8 *pEnd = &pChangeset[nChangeset];
  u8 *p = pChangeset;
  int rc = SQLITE_OK;

  memset(pParse, 0, sizeof(FuzzChangeset));

  while( rc==SQLITE_OK && p<pEnd ){
    FuzzChangesetGroup *pGrp = 0;

    /* Read a table-header from the changeset */
    rc = fuzzParseHeader(&p, pEnd, &pGrp);
    assert( (rc==SQLITE_OK)==(pGrp!=0) );

    /* If the table-header was successfully parsed, link the new change-group
    ** into the linked list and parse the associated array of changes. */
    if( rc==SQLITE_OK ){
      FuzzChangesetGroup **apNew = (FuzzChangesetGroup**)sqlite3_realloc(
          pParse->apGroup, sizeof(FuzzChangesetGroup*)*(pParse->nGroup+1)
      );
      if( apNew==0 ){
        rc = SQLITE_NOMEM;
      }else{
        apNew[pParse->nGroup] = pGrp;
        pParse->apGroup = apNew;
        pParse->nGroup++;
      }
      rc = fuzzParseChanges(&p, pEnd, pParse);
    }
  }

  return rc;
}

static int fuzzPrintRecord(FuzzChangesetGroup *pGrp, u8 **ppRec){
  int rc = SQLITE_OK;
  u8 *p = *ppRec;
  int i;
  const char *zPre = " (";

  for(i=0; i<pGrp->nCol; i++){
    u8 eType = p++[0];
    switch( eType ){
      case 0x00:                    /* undefined */
        printf("%sn/a", zPre);
        break;

      case 0x01: {                  /* integer */
        sqlite3_int64 iVal = 0;
        iVal = fuzzGetI64(p);
        printf("%s%lld", zPre, iVal);
        p += 8;
        break;
      }

      case 0x02: {                  /* real */
        sqlite3_int64 iVal = 0;
        double fVal = 0.0;
        iVal = fuzzGetI64(p);
        memcpy(&fVal, &iVal, 8);
        printf("%s%f", zPre, fVal);
        p += 8;
        break;
      }

      case 0x03:                    /* text */
      case 0x04: {                  /* blob */
        int nTxt;
        int sz;
        int i;
        p += fuzzGetVarint(p, &nTxt);
        printf("%s%s", zPre, eType==0x03 ? "'" : "X'");
        for(i=0; i<nTxt; i++){
          if( eType==0x03 ){
            printf("%c", p[i]);
          }else{
            char aHex[16] = {'0', '1', '2', '3', '4', '5', '6', '7',
                             '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'
            };
            printf("%c", aHex[ p[i]>>4 ]);
            printf("%c", aHex[ p[i] & 0x0F ]);
          }
        }
        printf("'");
        p += nTxt;
        break;
      }

      case 0x05:                    /* null */
        printf("%sNULL", zPre);
        break;
    }
    zPre = ", ";
  }
  printf(")");

  *ppRec = p;
  return rc;
}

static int fuzzPrintGroup(FuzzChangesetGroup *pGrp){
  int i;
  u8 *p;

  /* The table header */
  printf("TABLE:  %s nCol=%d aPK=", pGrp->zTab, pGrp->nCol);
  for(i=0; i<pGrp->nCol; i++){
    printf("%d", (int)pGrp->aPK[i]);
  }
  printf("\n");

  /* The array of changes */
  p = pGrp->aChange;
  for(i=0; i<pGrp->nChange; i++){
    u8 eType = p[0];
    u8 bIndirect = p[1];
    printf("%s (ind=%d):",
        (eType==SQLITE_INSERT) ? "INSERT" :
        (eType==SQLITE_DELETE ? "DELETE" : "UPDATE"),
        bIndirect
    );
    p += 2;

    if( eType==SQLITE_UPDATE ){
      fuzzPrintRecord(pGrp, &p);
    }
    fuzzPrintRecord(pGrp, &p);
    printf("\n");
  }
}

static int fuzzSelectChange(FuzzChangeset *pParse, FuzzChange *pChange){
  int iSub;

  memset(pChange, 0, sizeof(FuzzChange));
  pChange->eType = fuzzRandomInt(7) + FUZZ_VALUE_SUB;

  assert( pChange->eType==FUZZ_VALUE_SUB
       || pChange->eType==FUZZ_VALUE_MOD
       || pChange->eType==FUZZ_VALUE_RND
       || pChange->eType==FUZZ_CHANGE_DUP
       || pChange->eType==FUZZ_CHANGE_DEL
       || pChange->eType==FUZZ_CHANGE_TYPE
       || pChange->eType==FUZZ_CHANGE_FIELD
  );

  pChange->iChange = fuzzRandomInt(pParse->nChange);
  if( pChange->eType==FUZZ_CHANGE_FIELD ){
    if( pParse->nUpdate==0 ) return -1;
    pChange->iChange = fuzzRandomInt(pParse->nUpdate);
  }

  if( pChange->eType==FUZZ_VALUE_SUB 
   || pChange->eType==FUZZ_VALUE_MOD 
   || pChange->eType==FUZZ_VALUE_RND 
  ){
    iSub = fuzzRandomInt(pParse->nVal);
    pChange->pSub1 = pParse->aVal[iSub];
    if( pChange->eType==FUZZ_VALUE_SUB ){
      iSub = fuzzRandomInt(pParse->nVal);
      pChange->pSub2 = pParse->aVal[iSub];
    }else{
      pChange->pSub2 = pChange->aSub;
    }

    if( pChange->eType==FUZZ_VALUE_RND ){
      pChange->aSub[0] = (u8)(fuzzRandomInt(5) + 1);
      switch( pChange->aSub[0] ){
        case 0x01: {                  /* integer */
          u64 iVal = fuzzRandomU64();
          fuzzPutU64(&pChange->aSub[1], iVal);
          break;
        }

        case 0x02: {                  /* real */
          u64 iVal1 = fuzzRandomU64();
          u64 iVal2 = fuzzRandomU64();
          double d = (double)iVal1 / (double)iVal2;
          memcpy(&iVal1, &d, sizeof(iVal1));
          fuzzPutU64(&pChange->aSub[1], iVal1);
          break;
        }

        case 0x03:                    /* text */
        case 0x04: {                  /* blob */
          int nByte = fuzzRandomInt(48);
          pChange->aSub[1] = nByte;
          fuzzRandomBlob(nByte, &pChange->aSub[2]);
          if( pChange->aSub[0]==0x03 ){
            int i;
            for(i=0; i<nByte; i++){
              pChange->aSub[2+i] &= 0x7F;
            }
          }
          break;
        }
      }
    }
    if( pChange->eType==FUZZ_VALUE_MOD ){
      int sz;
      int iMod = -1;
      fuzzChangeSize(pChange->pSub1, &sz);
      memcpy(pChange->aSub, pChange->pSub1, sz);
      switch( pChange->aSub[0] ){
        case 0x01:
        case 0x02:
          iMod = fuzzRandomInt(8) + 1;
          break;

        case 0x03:                    /* text */
        case 0x04: {                  /* blob */
          int nByte;
          int iFirst = 1 + fuzzGetVarint(&pChange->aSub[1], &nByte);
          if( nByte>0 ){
            iMod = fuzzRandomInt(nByte) + iFirst;
          }
          break;
        }
      }

      if( iMod>=0 ){
        u8 mask = (1 << fuzzRandomInt(8 - (pChange->aSub[0]==0x03)));
        pChange->aSub[iMod] ^= mask;
      }
    }
  }

  return SQLITE_OK;
}

static int fuzzCopyChange(
  FuzzChangeset *pParse,
  FuzzChangesetGroup *pGrp, 
  FuzzChange *pFuzz,
  u8 **pp, u8 **ppOut             /* IN/OUT: Input and output pointers */
){
  u8 *p = *pp;
  u8 *pOut = *ppOut;
  u8 eType = p++[0];
  int iRec;
  int nRec = (eType==SQLITE_UPDATE ? 2 : 1);
  int iUndef = -1;

  u8 eNew = eType;
  if( pFuzz->iCurrent==pFuzz->iChange && pFuzz->eType==FUZZ_CHANGE_TYPE ){
    switch( eType ){
      case SQLITE_INSERT:
        eNew = SQLITE_DELETE;
        break;
      case SQLITE_DELETE:
        eNew = SQLITE_UPDATE;
        break;
      case SQLITE_UPDATE:
        eNew = SQLITE_INSERT;
        break;
    }
  }

  if( pFuzz->iCurrent==pFuzz->iChange 
   && pFuzz->eType==FUZZ_CHANGE_FIELD && eType==SQLITE_UPDATE
  ){
    int sz;
    int i;
    int nDef = 0;
    u8 *pCsr = p+1;
    for(i=0; i<pGrp->nCol; i++){
      if( pCsr[0] && pGrp->aPK[i]==0 ) nDef++;
      fuzzChangeSize(pCsr, &sz);
      pCsr += sz;
    }
    if( nDef<=1 ) return -1;
    nDef = fuzzRandomInt(nDef);
    for(i=0; i<pGrp->nCol; i++){
      if( pCsr[0] && pGrp->aPK[i]==0 ){
        if( nDef==0 ) iUndef = i;
        nDef--;
      }
      fuzzChangeSize(pCsr, &sz);
      pCsr += sz;
    }
  }

  /* Copy the change type and indirect flag */
  *(pOut++) = eNew;
  *(pOut++) = *(p++);
  for(iRec=0; iRec<nRec; iRec++){
    int i;
    for(i=0; i<pGrp->nCol; i++){
      int sz;
      u8 *pCopy = p;

      if( p==pFuzz->pSub1 ){
        pCopy = pFuzz->pSub2;
      }else if( p==pFuzz->pSub2 ){
        pCopy = pFuzz->pSub1;
      }else if( i==iUndef ){
        pCopy = "\0";
      }

      if( pCopy[0]==0x00 && eNew!=eType && eType==SQLITE_UPDATE && iRec==0 ){
        while( pCopy[0]==0x00 ){
          pCopy = pParse->aVal[fuzzRandomInt(pParse->nVal)];
        }
      }else if( p[0]==0x00 && pCopy[0]!=0x00 ){
        return -1;
      }else{
        if( pGrp->aPK[i]>0 && pCopy[0]==0x05 ) return -1;
      }

      if( eNew==eType || eType!=SQLITE_UPDATE || iRec==0 ){
        fuzzChangeSize(pCopy, &sz);
        memcpy(pOut, pCopy, sz);
        pOut += sz;
      }

      fuzzChangeSize(p, &sz);
      p += sz;
    }
  }

  if( pFuzz->iCurrent==pFuzz->iChange ){
    if( pFuzz->eType==FUZZ_CHANGE_DUP ){
      int nByte = pOut - (*ppOut);
      memcpy(pOut, *ppOut, nByte);
      pOut += nByte;
    }
    if( pFuzz->eType==FUZZ_CHANGE_DEL ){
      if( pGrp->nChange==1 ) return -1;
      pOut = *ppOut;
    }
    if( eNew!=eType && eNew==SQLITE_UPDATE ){
      int i;
      u8 *pCsr = (*ppOut) + 2;
      for(i=0; i<pGrp->nCol; i++){
        int sz;
        u8 *pCopy = pCsr;
        if( pGrp->aPK[i] ) pCopy = "\0";
        fuzzChangeSize(pCopy, &sz);
        memcpy(pOut, pCopy, sz);
        pOut += sz;
        fuzzChangeSize(pCsr, &sz);
        pCsr += sz;
      }
    }
  }

  *pp = p;
  *ppOut = pOut;
  pFuzz->iCurrent += (eType==SQLITE_UPDATE || pFuzz->eType!=FUZZ_CHANGE_FIELD);
  return SQLITE_OK;
}

static int fuzzDoOneFuzz(
  const char *zOut,               /* Filename to write modified changeset to */
  u8 *pBuf,                       /* Buffer to use for modified changeset */
  FuzzChangeset *pParse           /* Parse of input changeset */
){
  FuzzChange change;
  int iGrp;
  int rc = -1;

  while( rc<0 ){
    u8 *pOut = pBuf;
    rc = fuzzSelectChange(pParse, &change);
    for(iGrp=0; rc==SQLITE_OK && iGrp<pParse->nGroup; iGrp++){
      FuzzChangesetGroup *pGrp = pParse->apGroup[iGrp];
      int nTab = strlen(pGrp->zTab) + 1;
      u8 *p;
      int i;

      /* Output a table header */
      pOut++[0] = 'T';
      pOut += fuzzPutVarint(pOut, pGrp->nCol);
      memcpy(pOut, pGrp->aPK, pGrp->nCol);
      pOut += pGrp->nCol;
      memcpy(pOut, pGrp->zTab, nTab);
      pOut += nTab;

      /* Output the change array */
      p = pGrp->aChange;
      for(i=0; rc==SQLITE_OK && i<pGrp->nChange; i++){
        rc = fuzzCopyChange(pParse, pGrp, &change, &p, &pOut);
      }
    }
    if( rc==SQLITE_OK ){
      fuzzWriteFile(zOut, pBuf, pOut-pBuf);
    }
  }

  return rc;
}

int main(int argc, char **argv){
  int nRepeat = 0;                /* Number of output files */
  int iSeed = 0;                  /* Value of PRNG seed */
  const char *zInput;             /* Name of input file */
  void *pChangeset = 0;           /* Input changeset */
  int nChangeset = 0;             /* Size of input changeset in bytes */
  int i;                          /* Current output file */
  FuzzChangeset changeset;        /* Partially parsed changeset */
  int rc;
  u8 *pBuf = 0;

  if( argc!=4 && argc!=2 ) usage(argv[0]);
  zInput = argv[1];

  fuzzReadFile(zInput, &nChangeset, &pChangeset);
  rc = fuzzParseChangeset(pChangeset, nChangeset, &changeset);

  if( rc==SQLITE_OK ){
    if( argc==2 ){
      for(i=0; i<changeset.nGroup; i++){
        fuzzPrintGroup(changeset.apGroup[i]);
      }
    }else{
      pBuf = (u8*)fuzzMalloc(nChangeset*2 + 1024);
      if( pBuf==0 ){
        rc = SQLITE_NOMEM;
      }else{
        iSeed = atoi(argv[2]);
        nRepeat = atoi(argv[3]);
        fuzzRandomSeed((unsigned int)iSeed);
        for(i=0; rc==SQLITE_OK && i<nRepeat; i++){
          char *zOut = sqlite3_mprintf("%s-%d", zInput, i);
          fuzzDoOneFuzz(zOut, pBuf, &changeset);
          sqlite3_free(zOut);
        }
        fuzzFree(pBuf);
      }
    }
  }

  if( rc!=SQLITE_OK ){
    fprintf(stderr, "error while processing changeset: %d\n", rc);
  }
  return rc;
}

