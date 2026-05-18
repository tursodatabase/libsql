/*
** The program demonstrates how a child process created using fork()
** can continue to use SQLite for a database that the parent had opened and
** and was writing into when the fork() occurred.
**
** This program executes the following steps:
**
**   1.  Create a new database file.  Open it, and populate it.
**   2.  Start a transaction and make changes.
**       ^-- close the transaction prior to fork() if --commit-before-fork
**   3.  Fork()
**   4.  In the child, close the database connection.  Special procedures
**       are needed to close the database connection in the child.  See the
**       implementation below.
**   5.  Commit the transaction in the parent.
**   6.  Verify that the transaction committed in the parent.
**   7.  In the child, after a delay to allow time for (5) and (6),
**       open a new database connection and verify that the transaction
**       committed by (5) is seen.
**   8.  Add make further changes and commit them in the child, using the
**       new database connection.
**   9.  In the parent, after a delay to account for (8), verify that
**       the new transaction added by (8) can be seen.
**
** Usage:
**
**    fork-test FILENAME [options]
**
** Options:
**
**    --wal                       Run the database in WAL mode
**    --vfstrace                  Enable VFS tracing for debugging
**    --commit-before-fork        COMMIT prior to the fork() in step 3
**    --delay-after-4 N           Pause for N seconds after step 4
**
** How To Compile:
**
**   gcc -O0 -g -Wall -I$(SQLITESRC) \
**         f1.c $(SQLITESRC)/ext/misc/vfstrace.c $(SQLITESRC/sqlite3.c \
**         -ldl -lpthread -lm
**
** Test procedure:
**
**   (1)  Run "fork-test x1.db".  Verify no I/O errors occur and that
**        both parent and child see all three rows in the t1 table.
**
**   (2)  Repeat (1) adding the --wal option.
**
**   (3)  Repeat (1) and (2) adding the --commit-before-fork option.
**
**   (4)  Repeat all prior steps adding the --delay-after-4 option with
**        a timeout of 15 seconds or so.  Then, while both parent and child
**        are paused, run the CLI against the x1.db database from a separate
**        window and verify that all the correct file locks are still working
**        correctly.
**
** Take-Aways:
**
**   *   If a process has open SQLite database connections when it fork()s,
**       the child can call exec() and all it well.  Nothing special needs
**       to happen.
**
**   *   If a process has open SQLite database connections when it fork()s,
**       the child can do anything that does not involve using SQLite without
**       causing problems in the parent.  No special actions are needed in
**       the child.
**
**   *   If a process has open SQLite database connections when it fork()s,
**       the child can call sqlite3_close() on those database connections
**       as long as there were no pending write transactions when the fork()
**       occurred.
**
**   *   If a process has open SQLite database connections that are in the
**       middle of a write transaction and then the processes fork()s, the
**       child process should close the database connections using the
**       procedures demonstrated in Step 4 below before trying to do anything
**       else with SQLite.
**
**   *   If a child process can safely close SQLite database connections that
**       it inherited via fork() using the procedures shown in Step 4 below
**       even if the database connections were not involved in a write
**       transaction at the time of the fork().  The special procedures are
**       required if a write transaction was active.  They are optional
**       otherwise.  No harm results from using the special procedures when
**       they are not necessary.
**
**   *   Child processes that use SQLite should open their own database
**       connections.  They should not attempt to use a database connection
**       that is inherited from the parent.
*/
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>
#include "sqlite3.h"

/*
** Process ID of the parent
*/
static pid_t parentPid = 0;

/*
** Return either "parent" or "child", as appropriate.
*/
static const char *whoAmI(void){
  return getpid()==parentPid ? "parent" : "child";
}

/*
** This is an sqlite3_exec() callback routine that prints all results.
*/
static int execCallback(void *pNotUsed, int nCol, char **aVal, char **aCol){
  int i;
  const char *zWho = whoAmI();
  for(i=0; i<nCol; i++){
    const char *zVal = aVal[i];
    const char *zCol = aCol[i];
    if( zVal==0 ) zVal = "NULL";
    if( zCol==0 ) zCol = "NULL";
    printf("%s: %s = %s\n", zWho, zCol, zVal);
    fflush(stdout);
  }
  return 0;
}

/*
** Execute one or more SQL statements.
*/
static void sqlExec(sqlite3 *db, const char *zSql, int bCallback){
  int rc;
  char *zErr = 0;
  printf("%s: %s\n", whoAmI(), zSql);
  fflush(stdout);
  rc = sqlite3_exec(db, zSql, bCallback ? execCallback : 0, 0, &zErr);
  if( rc || zErr!=0 ){
    printf("%s: %s: rc=%d: %s\n", 
      whoAmI(), zSql, rc, zErr);
    exit(1);
  }
}

/*
** Trace callback for the vfstrace extension.
*/
static int vfsTraceCallback(const char *zMsg, void *pNotUsed){
  printf("%s: %s", whoAmI(), zMsg);
  fflush(stdout);
  return 0;
}

/* External VFS module provided by ext/misc/vfstrace.c
*/
extern int vfstrace_register(
  const char *zTraceName,         // Name of the newly constructed VFS
  const char *zOldVfsName,        // Name of the underlying VFS
  int (*xOut)(const char*,void*), // Output routine.  ex: fputs
  void *pOutArg,                  // 2nd argument to xOut.  ex: stderr
  int makeDefault                 // Make the new VFS the default
);


int main(int argc, char **argv){
  sqlite3 *db;
  int rc;
  int i;
  int useWal = 0;
  const char *zFilename = 0;
  pid_t child = 0, c2;
  int status;
  int bCommitBeforeFork = 0;
  int nDelayAfter4 = 0;

  for(i=1; i<argc; i++){
    const char *z = argv[i];
    if( z[0]=='-' && z[1]=='-' && z[2]!=0 ) z++;
    if( strcmp(z, "-wal")==0 ){
      useWal = 1;
    }else if( strcmp(z, "-commit-before-fork")==0 ){
      bCommitBeforeFork = 1;
    }else if( strcmp(z, "-delay-after-4")==0 && i+1<argc ){
      i++;
      nDelayAfter4 = atoi(argv[i]);
    }else if( strcmp(z, "-vfstrace")==0 ){
      vfstrace_register("vfstrace", 0, vfsTraceCallback, 0, 1);
    }else if( z[0]=='-' ){
      printf("unknown option: \"%s\"\n", argv[i]);
      exit(1);
    }else if( zFilename!=0 ){
      printf("unknown argument: \"%s\"\n", argv[i]);
      exit(1);
    }else{
      zFilename = argv[i];
    }
  }
  if( zFilename==0 ){
    printf("Usage: %s FILENAME\n", argv[0]);
    return 1;
  }

  /**  Step 1 **/
  printf("Step 1:\n");
  parentPid = getpid();
  unlink(zFilename);
  rc = sqlite3_open(zFilename, &db);
  if( rc ){
    printf("sqlite3_open() returns %d\n", rc);
    exit(1);
  }
  if( useWal ){
    sqlExec(db, "PRAGMA journal_mode=WAL;", 0);
  }
  sqlExec(db, "CREATE TABLE t1(x);", 0);
  sqlExec(db, "INSERT INTO t1 VALUES('First row');", 0);
  sqlExec(db, "SELECT x FROM t1;", 1);

  /**  Step 2 **/
  printf("Step 2:\n");
  sqlExec(db, "BEGIN IMMEDIATE;", 0);
  sqlExec(db, "INSERT INTO t1 VALUES('Second row');", 0);
  sqlExec(db, "SELECT x FROM t1;", 1);
  if( bCommitBeforeFork ) sqlExec(db, "COMMIT", 0);

  /**  Step 3 **/
  printf("Step 3:\n"); fflush(stdout);
  child = fork();
  if( child!=0 ){
    printf("Parent = %d\nChild = %d\n", getpid(), child);
  }

  /**  Step 4 **/
  if( child==0 ){
    int k;
    printf("Step 4:\n"); fflush(stdout);

    /***********************************************************************
    ** The following block of code closes the database connection without
    ** rolling back or changing any files on disk.  This is necessary to
    ** preservce the pending transaction in the parent. 
    */
    for(k=0; 1/*exit-by-break*/; k++){
      const char *zDbName = sqlite3_db_name(db, k);
      sqlite3_file *pJrnl = 0;
      if( k==1 ) continue;
      if( zDbName==0 ) break;
      sqlite3_file_control(db, zDbName, SQLITE_FCNTL_NULL_IO, 0);
      sqlite3_file_control(db, zDbName, SQLITE_FCNTL_JOURNAL_POINTER, &pJrnl);
      if( pJrnl && pJrnl->pMethods && pJrnl->pMethods->xFileControl ){
        pJrnl->pMethods->xFileControl(pJrnl, SQLITE_FCNTL_NULL_IO, 0);
      }
    }    
    sqlite3_close(db);
    /*
    ** End of special close procedures for SQLite database connections
    ** inherited via fork().
    ***********************************************************************/

    printf("%s: database connection closed\n", whoAmI()); fflush(stdout);
  }else{
    /* Pause the parent briefly to give the child a chance to close its
    ** database connection */
    sleep(1);
  }

  if( nDelayAfter4>0 ){
    printf("%s: Delay for %d seconds\n", whoAmI(), nDelayAfter4);
    fflush(stdout);
    sleep(nDelayAfter4);
    printf("%s: Continue after %d delay\n", whoAmI(), nDelayAfter4);
    fflush(stdout);
  }

  /**  Step 5 **/
  if( child!=0 ){
    printf("Step 5:\n");
    if( !bCommitBeforeFork ) sqlExec(db, "COMMIT", 0);
    sqlExec(db, "SELECT x FROM t1;", 1);
  }    
  

  /** Step 7 **/
  if( child==0 ){
    sleep(2);
    printf("Steps 7 and 8:\n");
    rc = sqlite3_open(zFilename, &db);
    if( rc ){
      printf("Child unable to reopen the database.  rc = %d\n", rc);
      exit(1);
    }
    sqlExec(db, "SELECT * FROM t1;", 1);

    /** Step 8 **/
    sqlExec(db, "INSERT INTO t1 VALUES('Third row');", 0);
    sqlExec(db, "SELECT * FROM t1;", 1);
    sleep(1);
    return 0;
  }
  c2 = wait(&status);
  printf("Process %d finished with status %d\n", c2, status);

  /** Step 9 */
  if( child!=0 ){
    printf("Step 9:\n");
    sqlExec(db, "SELECT * FROM t1;", 1);
  }

  return 0;
}
