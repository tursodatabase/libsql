/*
** This C program extracts all "words" from an input document and adds them
** to an SQLite database.  A "word" is any contiguous sequence of alphabetic
** characters.  All digits, punctuation, and whitespace characters are 
** word separators.  The database stores a single entry for each distinct
** word together with a count of the number of occurrences of that word.
** A fresh database is created automatically on each run.
**
**     wordcount DATABASE INPUTFILE
**
** The INPUTFILE name can be omitted, in which case input it taken from
** standard input.
**
** Option:
**
**     --without-rowid      Use a WITHOUT ROWID table to store the words.
**     --insert             Use INSERT mode (the default)
**     --replace            Use REPLACE mode
**     --select             Use SELECT mode
**     --update             Use UPDATE mode
**     --nocase             Add the NOCASE collating sequence to the words.
**     --trace              Enable sqlite3_trace() output.
**
** Modes:
**
** Insert mode means:
**    (1) INSERT OR IGNORE INTO wordcount VALUES($new,1)
**    (2) UPDATE wordcount SET cnt=cnt+1 WHERE word=$new -- if (1) is a noop
**
** Update mode means:
**    (1) INSERT OR IGNORE INTO wordcount VALUES($new,0)
**    (2) UPDATE wordcount SET cnt=cnt+1 WHERE word=$new
**
** Replace mode means:
**    (1) REPLACE INTO wordcount
**        VALUES($new,ifnull((SELECT cnt FROM wordcount WHERE word=$new),0)+1);
**
** Select mode modes:
**    (1) SELECT 1 FROM wordcount WHERE word=$newword
**    (2) INSERT INTO wordcount VALUES($new,1) -- if (1) returns nothing
**    (3) UPDATE wordcount SET cnt=cnt+1 WHERE word=$new  --if (1) return TRUE
**
******************************************************************************
**
** Compile as follows:
**
**    gcc -I. wordcount.c sqlite3.c -ldl -lpthreads
**
** Or:
**
**    gcc -I. -DSQLITE_THREADSAFE=0 -DSQLITE_OMIT_LOAD_EXTENSION \
**        wordcount.c sqlite3.c
*/
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <stdlib.h>
#include <stdarg.h>
#include "sqlite3.h"

/* Print an error message and exit */
static void fatal_error(const char *zMsg, ...){
  va_list ap;
  va_start(ap, zMsg);
  vfprintf(stderr, zMsg, ap);
  va_end(ap);
  exit(1);
}

/* The sqlite3_trace() callback function */
static void traceCallback(void *NotUsed, const char *zSql){
  printf("%s;\n", zSql);
}

/* Define operating modes */
#define MODE_INSERT     0
#define MODE_REPLACE    1
#define MODE_SELECT     2
#define MODE_UPDATE     3

int main(int argc, char **argv){
  const char *zFileToRead = 0;  /* Input file.  NULL for stdin */
  const char *zDbName = 0;      /* Name of the database file to create */
  int useWithoutRowid = 0;      /* True for --without-rowid */
  int iMode = MODE_INSERT;      /* One of MODE_xxxxx */
  int useNocase = 0;            /* True for --nocase */
  int doTrace = 0;              /* True for --trace */
  int i, j;                     /* Loop counters */
  sqlite3 *db;                  /* The SQLite database connection */
  char *zSql;                   /* Constructed SQL statement */
  sqlite3_stmt *pInsert = 0;    /* The INSERT statement */
  sqlite3_stmt *pUpdate = 0;    /* The UPDATE statement */
  sqlite3_stmt *pSelect = 0;    /* The SELECT statement */
  FILE *in;                     /* The open input file */
  int rc;                       /* Return code from an SQLite interface */
  int iCur, iHiwtr;             /* Statistics values, current and "highwater" */
  char zInput[2000];            /* A single line of input */

  /* Process command-line arguments */
  for(i=1; i<argc; i++){
    const char *z = argv[i];
    if( z[0]=='-' ){
      do{ z++; }while( z[0]=='-' );
      if( strcmp(z,"without-rowid")==0 ){
        useWithoutRowid = 1;
      }else if( strcmp(z,"replace")==0 ){
        iMode = MODE_REPLACE;
      }else if( strcmp(z,"select")==0 ){
        iMode = MODE_SELECT;
      }else if( strcmp(z,"insert")==0 ){
        iMode = MODE_INSERT;
      }else if( strcmp(z,"update")==0 ){
        iMode = MODE_UPDATE;
      }else if( strcmp(z,"nocase")==0 ){
        useNocase = 1;
      }else if( strcmp(z,"trace")==0 ){
        doTrace = 1;
      }else{
        fatal_error("unknown option: %s\n", argv[i]);
      }
    }else if( zDbName==0 ){
      zDbName = argv[i];
    }else if( zFileToRead==0 ){
      zFileToRead = argv[i];
    }else{
      fatal_error("surplus argument: %s\n", argv[i]);
    }
  }
  if( zDbName==0 ){
    fatal_error("Usage: %s [--options] DATABASE [INPUTFILE]\n", argv[0]);
  }

  /* Open the database and the input file */
  if( sqlite3_open(zDbName, &db) ){
    fatal_error("Cannot open database file: %s\n", zDbName);
  }
  if( zFileToRead ){
    in = fopen(zFileToRead, "rb");
    if( in==0 ){
      fatal_error("Could not open input file \"%s\"\n", zFileToRead);
    }
  }else{
    in = stdin;
  }

  /* Construct the "wordcount" table into which to put the words */
  if( doTrace ) sqlite3_trace(db, traceCallback, 0);
  if( sqlite3_exec(db, "BEGIN IMMEDIATE", 0, 0, 0) ){
    fatal_error("Could not start a transaction\n");
  }
  zSql = sqlite3_mprintf(
     "CREATE TABLE wordcount(\n"
     "  word TEXT PRIMARY KEY COLLATE %s,\n"
     "  cnt INTEGER\n"
     ")%s",
     useNocase ? "nocase" : "binary",
     useWithoutRowid ? " WITHOUT ROWID" : ""
  );
  if( zSql==0 ) fatal_error("out of memory\n");
  rc = sqlite3_exec(db, zSql, 0, 0, 0);
  if( rc ) fatal_error("Could not create the wordcount table: %s.\n",
                       sqlite3_errmsg(db));
  sqlite3_free(zSql);

  /* Prepare SQL statements that will be needed */
  if( iMode==MODE_SELECT ){
    rc = sqlite3_prepare_v2(db,
          "SELECT 1 FROM wordcount WHERE word=?1",
          -1, &pSelect, 0);
    if( rc ) fatal_error("Could not prepare the SELECT statement: %s\n",
                          sqlite3_errmsg(db));
    rc = sqlite3_prepare_v2(db,
          "INSERT INTO wordcount(word,cnt) VALUES(?1,1)",
          -1, &pInsert, 0);
    if( rc ) fatal_error("Could not prepare the INSERT statement: %s\n",
                         sqlite3_errmsg(db));
  }
  if( iMode==MODE_SELECT || iMode==MODE_UPDATE || iMode==MODE_INSERT ){
    rc = sqlite3_prepare_v2(db,
          "UPDATE wordcount SET cnt=cnt+1 WHERE word=?1",
          -1, &pUpdate, 0);
    if( rc ) fatal_error("Could not prepare the UPDATE statement: %s\n",
                         sqlite3_errmsg(db));
  }
  if( iMode==MODE_INSERT ){
    rc = sqlite3_prepare_v2(db,
          "INSERT OR IGNORE INTO wordcount(word,cnt) VALUES(?1,1)",
          -1, &pInsert, 0);
    if( rc ) fatal_error("Could not prepare the INSERT statement: %s\n",
                         sqlite3_errmsg(db));
  }
  if( iMode==MODE_UPDATE ){
    rc = sqlite3_prepare_v2(db,
          "INSERT OR IGNORE INTO wordcount(word,cnt) VALUES(?1,0)",
          -1, &pInsert, 0);
    if( rc ) fatal_error("Could not prepare the INSERT statement: %s\n",
                         sqlite3_errmsg(db));
  }
  if( iMode==MODE_REPLACE ){
    rc = sqlite3_prepare_v2(db,
          "REPLACE INTO wordcount(word,cnt)"
          "VALUES(?1,coalesce((SELECT cnt FROM wordcount WHERE word=?1),0)+1)",
          -1, &pInsert, 0);
    if( rc ) fatal_error("Could not prepare the REPLACE statement: %s\n",
                          sqlite3_errmsg(db));
  }

  /* Process the input file */
  while( fgets(zInput, sizeof(zInput), in) ){
    for(i=0; zInput[i]; i++){
      if( !isalpha(zInput[i]) ) continue;
      for(j=i+1; isalpha(zInput[j]); j++){}

      /* Found a new word at zInput[i] that is j-i bytes long. 
      ** Process it into the wordcount table.  */
      if( iMode==MODE_SELECT ){
        sqlite3_bind_text(pSelect, 1, zInput+i, j-i, SQLITE_STATIC);
        rc = sqlite3_step(pSelect);
        sqlite3_reset(pSelect);
        if( rc==SQLITE_ROW ){
          sqlite3_bind_text(pUpdate, 1, zInput+i, j-i, SQLITE_STATIC);
          if( sqlite3_step(pUpdate)!=SQLITE_DONE ){
            fatal_error("UPDATE failed: %s\n", sqlite3_errmsg(db));
          }
          sqlite3_reset(pUpdate);
        }else if( rc==SQLITE_DONE ){
          sqlite3_bind_text(pInsert, 1, zInput+i, j-i, SQLITE_STATIC);
          if( sqlite3_step(pInsert)!=SQLITE_DONE ){
            fatal_error("Insert failed: %s\n", sqlite3_errmsg(db));
          }
          sqlite3_reset(pInsert);
        }else{
          fatal_error("SELECT failed: %s\n", sqlite3_errmsg(db));
        }
      }else{
        sqlite3_bind_text(pInsert, 1, zInput+i, j-i, SQLITE_STATIC);
        if( sqlite3_step(pInsert)!=SQLITE_DONE ){
          fatal_error("INSERT failed: %s\n", sqlite3_errmsg(db));
        }
        sqlite3_reset(pInsert);
        if( iMode==MODE_UPDATE
         || (iMode==MODE_INSERT && sqlite3_changes(db)==0)
        ){
          sqlite3_bind_text(pUpdate, 1, zInput+i, j-i, SQLITE_STATIC);
          if( sqlite3_step(pUpdate)!=SQLITE_DONE ){
            fatal_error("UPDATE failed: %s\n", sqlite3_errmsg(db));
          }
          sqlite3_reset(pUpdate);
        }
      }
      i = j-1;
    }
  }
  sqlite3_exec(db, "COMMIT", 0, 0, 0);
  if( zFileToRead ) fclose(in);
  sqlite3_finalize(pInsert);
  sqlite3_finalize(pUpdate);
  sqlite3_finalize(pSelect);

  /* Database connection statistics printed after both prepared statements
  ** have been finalized */
  sqlite3_db_status(db, SQLITE_DBSTATUS_LOOKASIDE_USED, &iCur, &iHiwtr, 0);
  printf("-- Lookaside Slots Used:        %d (max %d)\n", iCur,iHiwtr);
  sqlite3_db_status(db, SQLITE_DBSTATUS_LOOKASIDE_HIT, &iCur, &iHiwtr, 0);
  printf("-- Successful lookasides:       %d\n", iHiwtr);
  sqlite3_db_status(db, SQLITE_DBSTATUS_LOOKASIDE_MISS_SIZE, &iCur, &iHiwtr, 0);
  printf("-- Lookaside size faults:       %d\n", iHiwtr);
  sqlite3_db_status(db, SQLITE_DBSTATUS_LOOKASIDE_MISS_FULL, &iCur, &iHiwtr, 0);
  printf("-- Lookaside OOM faults:        %d\n", iHiwtr);
  sqlite3_db_status(db, SQLITE_DBSTATUS_CACHE_USED, &iCur, &iHiwtr, 0);
  printf("-- Pager Heap Usage:            %d bytes\n", iCur);
  sqlite3_db_status(db, SQLITE_DBSTATUS_CACHE_HIT, &iCur, &iHiwtr, 1);
  printf("-- Page cache hits:             %d\n", iCur);
  sqlite3_db_status(db, SQLITE_DBSTATUS_CACHE_MISS, &iCur, &iHiwtr, 1);
  printf("-- Page cache misses:           %d\n", iCur); 
  sqlite3_db_status(db, SQLITE_DBSTATUS_CACHE_WRITE, &iCur, &iHiwtr, 1);
  printf("-- Page cache writes:           %d\n", iCur); 
  sqlite3_db_status(db, SQLITE_DBSTATUS_SCHEMA_USED, &iCur, &iHiwtr, 0);
  printf("-- Schema Heap Usage:           %d bytes\n", iCur); 
  sqlite3_db_status(db, SQLITE_DBSTATUS_STMT_USED, &iCur, &iHiwtr, 0);
  printf("-- Statement Heap Usage:        %d bytes\n", iCur); 

  sqlite3_close(db);

  /* Global memory usage statistics printed after the database connection
  ** has closed.  Memory usage should be zero at this point. */
  sqlite3_status(SQLITE_STATUS_MEMORY_USED, &iCur, &iHiwtr, 0);
  printf("-- Memory Used (bytes):         %d (max %d)\n", iCur,iHiwtr);
  sqlite3_status(SQLITE_STATUS_MALLOC_COUNT, &iCur, &iHiwtr, 0);
  printf("-- Outstanding Allocations:     %d (max %d)\n", iCur,iHiwtr);
  sqlite3_status(SQLITE_STATUS_PAGECACHE_OVERFLOW, &iCur, &iHiwtr, 0);
  printf("-- Pcache Overflow Bytes:       %d (max %d)\n", iCur,iHiwtr);
  sqlite3_status(SQLITE_STATUS_SCRATCH_OVERFLOW, &iCur, &iHiwtr, 0);
  printf("-- Scratch Overflow Bytes:      %d (max %d)\n", iCur,iHiwtr);
  sqlite3_status(SQLITE_STATUS_MALLOC_SIZE, &iCur, &iHiwtr, 0);
  printf("-- Largest Allocation:          %d bytes\n",iHiwtr);
  sqlite3_status(SQLITE_STATUS_PAGECACHE_SIZE, &iCur, &iHiwtr, 0);
  printf("-- Largest Pcache Allocation:   %d bytes\n",iHiwtr);
  sqlite3_status(SQLITE_STATUS_SCRATCH_SIZE, &iCur, &iHiwtr, 0);
  printf("-- Largest Scratch Allocation:  %d bytes\n", iHiwtr);

  return 0;
}
