/*
** 2004 January 13
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file implements a simple standalone program used to test whether
** or not the SQLite library is threadsafe.
**
** This file is NOT part of the standard SQLite library.  It is used for
** testing only.
*/
#include <stdio.h>
#include <unistd.h>
#include <pthread.h>
#include <string.h>
#include <stdlib.h>
#include "sqlite.h"

/*
** Name of the database
*/
#define DB_FILE "test.db"

/* 
** When this variable becomes non-zero, all threads stop
** what they are doing.
*/
volatile int all_stop = 0;

/* 
** Callback from the integrity check.  If the result is anything other
** than "ok" it means the integrity check has failed.  Set the "all_stop"
** global variable to stop all other activity.  Print the error message
** or print OK if the string "ok" is seen.
*/
int check_callback(void *notUsed, int argc, char **argv, char **notUsed2){
  if( strcmp(argv[0],"ok") ){
    all_stop = 1;
    fprintf(stderr,"pid=%d. %s\n", getpid(), argv[0]);
  }else{
    /* fprintf(stderr,"pid=%d. OK\n", getpid()); */
  }
  return 0;
}

/*
** Do an integrity check on the database.  If the first integrity check
** fails, try it a second time.
*/
int integrity_check(sqlite *db){
  int rc;
  if( all_stop ) return 0;
  /* fprintf(stderr,"pid=%d: CHECK\n", getpid()); */
  rc = sqlite3_exec(db, "pragma integrity_check", check_callback, 0, 0);
  if( rc!=SQLITE_OK && rc!=SQLITE_BUSY ){
    fprintf(stderr,"pid=%d, Integrity check returns %d\n", getpid(), rc);
  }
  if( all_stop ){
    sqlite3_exec(db, "pragma integrity_check", check_callback, 0, 0);
  }
  return 0;
}

/*
** This is the worker thread
*/
void *worker(void *notUsed){
  sqlite *db;
  int rc;
  int cnt = 0;
  while( !all_stop && cnt++<10000 ){
    if( cnt%1000==0 ) printf("pid=%d: %d\n", getpid(), cnt);
    while( (sqlite3_open(DB_FILE, &db))!=SQLITE_OK ) sched_yield();
    sqlite3_exec(db, "PRAGMA synchronous=OFF", 0, 0, 0);
    integrity_check(db);
    if( all_stop ){ sqlite3_close(db); break; }
    /* fprintf(stderr, "pid=%d: BEGIN\n", getpid()); */
    rc = sqlite3_exec(db, "INSERT INTO t1 VALUES('bogus data')", 0, 0, 0);
    /* fprintf(stderr, "pid=%d: END rc=%d\n", getpid(), rc); */
    sqlite3_close(db);
  }
  return 0;
}

/*
** Initialize the database and start the threads
*/
int main(int argc, char **argv){
  sqlite *db;
  int i, rc;
  pthread_t aThread[5];

  if( strcmp(DB_FILE,":memory:") ){
    char *zJournal = sqlite3_mprintf("%s-journal", DB_FILE);
    unlink(DB_FILE);
    unlink(zJournal);
    free(zJournal);
  }  
  sqlite3_open(DB_FILE, &db);
  if( db==0 ){
    fprintf(stderr,"unable to initialize database\n");
    exit(1);
  }
  rc = sqlite3_exec(db, "CREATE TABLE t1(x);", 0,0,0);
  if( rc ){
    fprintf(stderr,"cannot create table t1: %d\n", rc);
    exit(1);
  }
  sqlite3_close(db);
  for(i=0; i<sizeof(aThread)/sizeof(aThread[0]); i++){
    pthread_create(&aThread[i], 0, worker, 0);
  }
  for(i=0; i<sizeof(aThread)/sizeof(aThread[i]); i++){
    pthread_join(aThread[i], 0);
  }
  if( !all_stop ){
    printf("Everything seems ok.\n");
    return 0;
  }else{
    printf("We hit an error.\n");
    return 1;
  }
}
