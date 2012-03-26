/*
** This program is a debugging and analysis utility that displays
** information about an FTS3 or FTS4 index.
**
** Link this program against the SQLite3 amalgamation with the
** SQLITE_ENABLE_FTS4 compile-time option.  Then run it as:
**
**    fts3view DATABASE
**
** to get a list of all FTS3/4 tables in DATABASE, or do
**
**    fts3view DATABASE TABLE COMMAND ....
**
** to see various aspects of the TABLE table.  Type fts3view with no
** arguments for a list of available COMMANDs.
*/
#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include "sqlite3.h"

/*
** Extra command-line arguments:
*/
int nExtra;
char **azExtra;

/*
** Look for a command-line argument.
*/
const char *findOption(const char *zName, int hasArg, const char *zDefault){
  int i;
  const char *zResult = zDefault;
  for(i=0; i<nExtra; i++){
    const char *z = azExtra[i];
    while( z[0]=='-' ) z++;
    if( strcmp(z, zName)==0 ){
      int j = 1;
      if( hasArg==0 || i==nExtra-1 ) j = 0;
      zResult = azExtra[i+j];
      while( i+j<nExtra ){
        azExtra[i] = azExtra[i+j+1];
        i++;
      }
      break;
    }
  }
  return zResult;       
}


/*
** Prepare an SQL query
*/
static sqlite3_stmt *prepare(sqlite3 *db, const char *zFormat, ...){
  va_list ap;
  char *zSql;
  sqlite3_stmt *pStmt;
  int rc;

  va_start(ap, zFormat);
  zSql = sqlite3_vmprintf(zFormat, ap);
  va_end(ap);
  rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
  if( rc ){
    fprintf(stderr, "Error: %s\nSQL: %s\n", sqlite3_errmsg(db), zSql);
    exit(1);
  }
  sqlite3_free(zSql);
  return pStmt;
}

/*
** Run an SQL statement
*/
static void runSql(sqlite3 *db, const char *zFormat, ...){
  va_list ap;
  char *zSql;

  va_start(ap, zFormat);
  zSql = sqlite3_vmprintf(zFormat, ap);
  sqlite3_exec(db, zSql, 0, 0, 0);
  va_end(ap);
}

/*
** Show the table schema
*/
static void showSchema(sqlite3 *db, const char *zTab){
  sqlite3_stmt *pStmt;
  pStmt = prepare(db,
            "SELECT sql FROM sqlite_master"
            " WHERE name LIKE '%q%%'"
            " ORDER BY 1",
            zTab);
  while( sqlite3_step(pStmt)==SQLITE_ROW ){
    printf("%s;\n", sqlite3_column_text(pStmt, 0));
  }
  sqlite3_finalize(pStmt);
  pStmt = prepare(db, "PRAGMA page_size");
  while( sqlite3_step(pStmt)==SQLITE_ROW ){
    printf("PRAGMA page_size=%s;\n", sqlite3_column_text(pStmt, 0));
  }
  sqlite3_finalize(pStmt);
  pStmt = prepare(db, "PRAGMA journal_mode");
  while( sqlite3_step(pStmt)==SQLITE_ROW ){
    printf("PRAGMA journal_mode=%s;\n", sqlite3_column_text(pStmt, 0));
  }
  sqlite3_finalize(pStmt);
}

/* 
** Read a 64-bit variable-length integer from memory starting at p[0].
** Return the number of bytes read, or 0 on error.
** The value is stored in *v.
*/
int getVarint(const unsigned char *p, sqlite_int64 *v){
  const unsigned char *q = p;
  sqlite_uint64 x = 0, y = 1;
  while( (*q&0x80)==0x80 && q-(unsigned char *)p<9 ){
    x += y * (*q++ & 0x7f);
    y <<= 7;
  }
  x += y * (*q++);
  *v = (sqlite_int64) x;
  return (int) (q - (unsigned char *)p);
}


/* Show the content of the %_stat table
*/
static void showStat(sqlite3 *db, const char *zTab){
  sqlite3_stmt *pStmt;
  pStmt = prepare(db, "SELECT id, value FROM '%q_stat'", zTab);
  while( sqlite3_step(pStmt)==SQLITE_ROW ){
    printf("stat[%d] =", sqlite3_column_int(pStmt, 0));
    switch( sqlite3_column_type(pStmt, 1) ){
      case SQLITE_INTEGER: {
        printf(" %d\n", sqlite3_column_int(pStmt, 1));
        break;
      }
      case SQLITE_BLOB: {
        unsigned char *x = (unsigned char*)sqlite3_column_blob(pStmt, 1);
        int len = sqlite3_column_bytes(pStmt, 1);
        int i = 0;
        sqlite3_int64 v;
        while( i<len ){
          i += getVarint(x, &v);
          printf(" %lld", v);
        }
        printf("\n");
        break;
      }
    }
  }
  sqlite3_finalize(pStmt);
}

/*
** Report on the vocabulary.  This creates an fts4aux table with a random
** name, but deletes it in the end.
*/
static void showVocabulary(sqlite3 *db, const char *zTab){
  char *zAux;
  sqlite3_uint64 r;
  sqlite3_stmt *pStmt;
  int nDoc = 0;
  int nToken = 0;
  int nOccurrence = 0;
  int nTop;
  int n, i;

  sqlite3_randomness(sizeof(r), &r);
  zAux = sqlite3_mprintf("viewer_%llx", zTab, r);
  runSql(db, "BEGIN");
  pStmt = prepare(db, "SELECT count(*) FROM %Q", zTab);
  while( sqlite3_step(pStmt)==SQLITE_ROW ){
    nDoc = sqlite3_column_int(pStmt, 0);
  }
  sqlite3_finalize(pStmt);
  printf("Number of documents...................... %9d\n", nDoc);

  runSql(db, "CREATE VIRTUAL TABLE %s USING fts4aux(%Q)", zAux, zTab);
  pStmt = prepare(db, 
             "SELECT count(*), sum(occurrences) FROM %s WHERE col='*'",
             zAux);
  while( sqlite3_step(pStmt)==SQLITE_ROW ){
    nToken = sqlite3_column_int(pStmt, 0);
    nOccurrence = sqlite3_column_int(pStmt, 1);
  }
  sqlite3_finalize(pStmt);
  printf("Total tokens in all documents............ %9d\n", nOccurrence);
  printf("Total number of distinct tokens.......... %9d\n", nToken);
  if( nToken==0 ) goto end_vocab;

  n = 0;
  pStmt = prepare(db, "SELECT count(*) FROM %s"
                      " WHERE col='*' AND occurrences==1", zAux);
  while( sqlite3_step(pStmt)==SQLITE_ROW ){
    n = sqlite3_column_int(pStmt, 0);
  }
  sqlite3_finalize(pStmt);
  printf("Tokens used exactly once................. %9d %5.2f%%\n",
          n, n*100.0/nToken);

  n = 0;
  pStmt = prepare(db, "SELECT count(*) FROM %s"
                      " WHERE col='*' AND documents==1", zAux);
  while( sqlite3_step(pStmt)==SQLITE_ROW ){
    n = sqlite3_column_int(pStmt, 0);
  }
  sqlite3_finalize(pStmt);
  printf("Tokens used in only one document......... %9d %5.2f%%\n",
          n, n*100.0/nToken);

  if( nDoc>=2000 ){
    n = 0;
    pStmt = prepare(db, "SELECT count(*) FROM %s"
                        " WHERE col='*' AND occurrences<=%d", zAux, nDoc/1000);
    while( sqlite3_step(pStmt)==SQLITE_ROW ){
      n = sqlite3_column_int(pStmt, 0);
    }
    sqlite3_finalize(pStmt);
    printf("Tokens used in 0.1%% or less of docs...... %9d %5.2f%%\n",
            n, n*100.0/nToken);
  }

  if( nDoc>=200 ){
    n = 0;
    pStmt = prepare(db, "SELECT count(*) FROM %s"
                        " WHERE col='*' AND occurrences<=%d", zAux, nDoc/100);
    while( sqlite3_step(pStmt)==SQLITE_ROW ){
      n = sqlite3_column_int(pStmt, 0);
    }
    sqlite3_finalize(pStmt);
    printf("Tokens used in 1%% or less of docs........ %9d %5.2f%%\n",
            n, n*100.0/nToken);
  }

  nTop = atoi(findOption("top", 1, "25"));
  printf("The %d most common tokens:\n", nTop);
  pStmt = prepare(db,
            "SELECT term, documents FROM %s"
            " WHERE col='*'"
            " ORDER BY documents DESC, term"
            " LIMIT %d", zAux, nTop);
  i = 0;
  while( sqlite3_step(pStmt)==SQLITE_ROW ){
    i++;
    n = sqlite3_column_int(pStmt, 1);
    printf("  %2d. %-30s %9d docs %5.2f%%\n", i,
      sqlite3_column_text(pStmt, 0), n, n*100.0/nDoc);
  }
  sqlite3_finalize(pStmt);

end_vocab:
  runSql(db, "ROLLBACK");
  sqlite3_free(zAux);
}

/*
** Report on the number and sizes of segments
*/
static void showSegmentStats(sqlite3 *db, const char *zTab){
  sqlite3_stmt *pStmt;
  int nSeg = 0;
  sqlite3_int64 szSeg = 0, mxSeg = 0;
  int nIdx = 0;
  sqlite3_int64 szIdx = 0, mxIdx = 0;
  int nRoot = 0;
  sqlite3_int64 szRoot = 0, mxRoot = 0;
  sqlite3_int64 mx;
  int nLeaf;
  int n;
  int pgsz;
  int mxLevel;
  int i;

  pStmt = prepare(db,
                  "SELECT count(*), sum(length(block)), max(length(block))"
                  " FROM '%q_segments'",
                  zTab);
  while( sqlite3_step(pStmt)==SQLITE_ROW ){
    nSeg = sqlite3_column_int(pStmt, 0);
    szSeg = sqlite3_column_int64(pStmt, 1);
    mxSeg = sqlite3_column_int64(pStmt, 2);
  }
  sqlite3_finalize(pStmt);
  pStmt = prepare(db,
            "SELECT count(*), sum(length(block)), max(length(block))"
            "  FROM '%q_segments' a JOIN '%q_segdir' b"
            " WHERE a.blockid BETWEEN b.leaves_end_block+1 AND b.end_block",
            zTab, zTab);
  while( sqlite3_step(pStmt)==SQLITE_ROW ){
    nIdx = sqlite3_column_int(pStmt, 0);
    szIdx = sqlite3_column_int64(pStmt, 1);
    mxIdx = sqlite3_column_int64(pStmt, 2);
  }
  sqlite3_finalize(pStmt);
  pStmt = prepare(db,
            "SELECT count(*), sum(length(root)), max(length(root))"
            "  FROM '%q_segdir'",
            zTab);
  while( sqlite3_step(pStmt)==SQLITE_ROW ){
    nRoot = sqlite3_column_int(pStmt, 0);
    szRoot = sqlite3_column_int64(pStmt, 1);
    mxRoot = sqlite3_column_int64(pStmt, 2);
  }
  sqlite3_finalize(pStmt);

  printf("Number of segments....................... %9d\n", nSeg+nRoot);
  printf("Number of leaf segments.................. %9d\n", nSeg-nIdx);
  printf("Number of index segments................. %9d\n", nIdx);
  printf("Number of root segments.................. %9d\n", nRoot);
  printf("Total size of all segments............... %9lld\n", szSeg+szRoot);
  printf("Total size of all leaf segments.......... %9lld\n", szSeg-szIdx);
  printf("Total size of all index segments......... %9lld\n", szIdx);
  printf("Total size of all root segments.......... %9lld\n", szRoot);
  if( nSeg>0 ){
    printf("Average size of all segments............. %11.1f\n",
            (double)(szSeg+szRoot)/(double)(nSeg+nRoot));
    printf("Average size of leaf segments............ %11.1f\n",
            (double)(szSeg-szIdx)/(double)(nSeg-nIdx));
  }
  if( nIdx>0 ){
    printf("Average size of index segments........... %11.1f\n",
            (double)szIdx/(double)nIdx);
  }
  if( nRoot>0 ){
    printf("Average size of root segments............ %11.1f\n",
            (double)szRoot/(double)nRoot);
  }
  mx = mxSeg;
  if( mx<mxRoot ) mx = mxRoot;
  printf("Maximum segment size..................... %9lld\n", mx);
  printf("Maximum index segment size............... %9lld\n", mxIdx);
  printf("Maximum root segment size................ %9lld\n", mxRoot);

  pStmt = prepare(db, "PRAGMA page_size");
  pgsz = 1024;
  while( sqlite3_step(pStmt)==SQLITE_ROW ){
    pgsz = sqlite3_column_int(pStmt, 0);
  }
  sqlite3_finalize(pStmt);
  printf("Database page size....................... %9d\n", pgsz);
  pStmt = prepare(db,
            "SELECT count(*)"
            "  FROM '%q_segments' a JOIN '%q_segdir' b"
            " WHERE a.blockid BETWEEN b.start_block AND b.leaves_end_block"
            "   AND length(a.block)>%d",
            zTab, zTab, pgsz-45);
  n = 0;
  while( sqlite3_step(pStmt)==SQLITE_ROW ){
    n = sqlite3_column_int(pStmt, 0);
  }
  sqlite3_finalize(pStmt);
  nLeaf = nSeg - nIdx;
  printf("Leaf segments larger than %5d bytes.... %9d   %5.2f%%\n",
         pgsz-45, n, n*100.0/nLeaf);

  pStmt = prepare(db, "SELECT max(level%%1024) FROM '%q_segdir'", zTab);
  mxLevel = 0;
  while( sqlite3_step(pStmt)==SQLITE_ROW ){
    mxLevel = sqlite3_column_int(pStmt, 0);
  }
  sqlite3_finalize(pStmt);

  for(i=0; i<=mxLevel; i++){
    pStmt = prepare(db,
           "SELECT count(*), sum(len), avg(len), max(len), sum(len>%d),"
           "       count(distinct idx)"
           "  FROM (SELECT length(a.block) AS len, idx"
           "          FROM '%q_segments' a JOIN '%q_segdir' b"
           "         WHERE (a.blockid BETWEEN b.start_block"
                                       " AND b.leaves_end_block)"
           "           AND (b.level%%1024)==%d)",
           pgsz-45, zTab, zTab, i);
    if( sqlite3_step(pStmt)==SQLITE_ROW
     && (nLeaf = sqlite3_column_int(pStmt, 0))>0
    ){
      int nIdx = sqlite3_column_int(pStmt, 5);
      sqlite3_int64 sz;
      printf("For level %d:\n", i);
      printf("  Number of indexes...................... %9d\n", nIdx);
      printf("  Number of leaf segments................ %9d\n", nLeaf);
      if( nIdx>1 ){
        printf("  Average leaf segments per index........ %11.1f\n",
               (double)nLeaf/(double)nIdx);
      }
      printf("  Total size of all leaf segments........ %9lld\n",
             (sz = sqlite3_column_int64(pStmt, 1)));
      printf("  Average size of leaf segments.......... %11.1f\n",
             sqlite3_column_double(pStmt, 2));
      if( nIdx>1 ){
        printf("  Average leaf segment size per index.... %11.1f\n",
               (double)sz/(double)nIdx);
      }
      printf("  Maximum leaf segment size.............. %9lld\n",
             sqlite3_column_int64(pStmt, 3));
      n = sqlite3_column_int(pStmt, 4);
      printf("  Leaf segments larger than %5d bytes.. %9d   %5.2f%%\n",
             pgsz-45, n, n*100.0/nLeaf);
    }
    sqlite3_finalize(pStmt);
  }
}

/*
** Print a single "tree" line of the segdir map output.
*/
static void printTreeLine(sqlite3_int64 iLower, sqlite3_int64 iUpper){
  printf("                 tree   %9lld", iLower);
  if( iUpper>iLower ){
    printf(" thru %9lld  (%lld blocks)", iUpper, iUpper-iLower+1);
  }
  printf("\n");
}

/*
** Show a map of segments derived from the %_segdir table.
*/
static void showSegdirMap(sqlite3 *db, const char *zTab){
  int mxIndex, iIndex;
  sqlite3_stmt *pStmt = 0;
  sqlite3_stmt *pStmt2 = 0;
  int prevLevel;

  pStmt = prepare(db, "SELECT max(level/1024) FROM '%q_segdir'", zTab);
  if( sqlite3_step(pStmt)==SQLITE_ROW ){
    mxIndex = sqlite3_column_int(pStmt, 0);
  }else{
    mxIndex = 0;
  }
  sqlite3_finalize(pStmt);

  printf("Number of inverted indices............... %3d\n", mxIndex+1);
  pStmt = prepare(db,
    "SELECT level, idx, start_block, leaves_end_block, end_block"
    "  FROM '%q_segdir'"
    " WHERE level/1024==?"
    " ORDER BY level DESC, idx",
    zTab);
  pStmt2 = prepare(db,
    "SELECT blockid FROM '%q_segments'"
    " WHERE blockid BETWEEN ? AND ? ORDER BY blockid",
    zTab);
  for(iIndex=0; iIndex<=mxIndex; iIndex++){
    if( mxIndex>0 ){
      printf("**************************** Index %d "
             "****************************\n", iIndex);
    }
    sqlite3_bind_int(pStmt, 1, iIndex);
    prevLevel = -1;
    while( sqlite3_step(pStmt)==SQLITE_ROW ){
      int iLevel = sqlite3_column_int(pStmt, 0)%1024;
      int iIdx = sqlite3_column_int(pStmt, 1);
      sqlite3_int64 iStart = sqlite3_column_int64(pStmt, 2);
      sqlite3_int64 iLEnd = sqlite3_column_int64(pStmt, 3);
      sqlite3_int64 iEnd = sqlite3_column_int64(pStmt, 4);
      if( iLevel!=prevLevel ){
        printf("level %2d idx %2d", iLevel, iIdx);
        prevLevel = iLevel;
      }else{
        printf("         idx %2d", iIdx);
      }
      if( iLEnd>iStart ){
        sqlite3_int64 iLower, iPrev, iX;
        printf("  leaves %9lld thru %9lld  (%lld blocks)\n",
               iStart, iLEnd, iLEnd - iStart + 1);
        if( iLEnd+1<=iEnd ){
          sqlite3_bind_int64(pStmt2, 1, iLEnd+1);
          sqlite3_bind_int64(pStmt2, 2, iEnd);
          iLower = -1;        
          while( sqlite3_step(pStmt2)==SQLITE_ROW ){
            iX = sqlite3_column_int64(pStmt2, 0);
            if( iLower<0 ){
              iLower = iPrev = iX;
            }else if( iX==iPrev+1 ){
              iPrev = iX;
            }else{
              printTreeLine(iLower, iPrev);
              iLower = iPrev = iX;
            }
          }
          sqlite3_reset(pStmt2);
          if( iLower>=0 ) printTreeLine(iLower, iPrev);
        }
      }else{
        printf("  root only\n");
      }
    }
    sqlite3_reset(pStmt);
  }
  sqlite3_finalize(pStmt);
  sqlite3_finalize(pStmt2);
}


static void usage(const char *argv0){
  fprintf(stderr, "Usage: %s DATABASE\n"
                  "   or: %s DATABASE FTS3TABLE ARGS...\n", argv0, argv0);
  fprintf(stderr,
    "ARGS:\n"
    "  schema                        FTS table schema\n"
    "  segdir                        directory of segments\n"
    "  segment-stats                 information about segment sizes\n"
    "  stat                          content of the %%_stat table\n"
    "  vocabulary --top N            information on the document vocabulary\n"
  );
  exit(1);
}

int main(int argc, char **argv){
  sqlite3 *db;
  int rc;
  const char *zTab;
  const char *zCmd;
  if( argc<2 ) usage(argv[0]);
  rc = sqlite3_open(argv[1], &db);
  if( rc ){
    fprintf(stderr, "Cannot open %s\n", argv[1]);
    exit(1);
  }
  if( argc==2 ){
    sqlite3_stmt *pStmt;
    int cnt = 0;
    pStmt = prepare(db, "SELECT b.sql"
                        "  FROM sqlite_master a, sqlite_master b"
                        " WHERE a.name GLOB '*_segdir'"
                        "   AND b.name=substr(a.name,1,length(a.name)-7)"
                        " ORDER BY 1");
    while( sqlite3_step(pStmt)==SQLITE_ROW ){
      cnt++;
      printf("%s;\n", sqlite3_column_text(pStmt, 0));
    }
    sqlite3_finalize(pStmt);
    if( cnt==0 ){
      printf("/* No FTS3/4 tables found in database %s */\n", argv[1]);
    }
    return 0;
  }
  if( argc<4 ) usage(argv[0]);
  zTab = argv[2];
  zCmd = argv[3];
  nExtra = argc-4;
  azExtra = argv+4;
  if( strcmp(zCmd,"schema")==0 ){
    showSchema(db, zTab);
  }else if( strcmp(zCmd,"segdir")==0 ){
    showSegdirMap(db, zTab);
  }else if( strcmp(zCmd,"segment-stats")==0 ){
    showSegmentStats(db, zTab);
  }else if( strcmp(zCmd,"stat")==0 ){
    showStat(db, zTab);
  }else if( strcmp(zCmd,"vocabulary")==0 ){
    showVocabulary(db, zTab);
  }else{
    usage(argv[0]);
  }
  return 0; 
}
