/*
** 2017 June 7
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** Simple multi-threaded server used for informal testing of concurrency
** between connections in different threads. Listens for tcp/ip connections
** on port 9999 of the 127.0.0.1 interface only. To build:
**
**   gcc -g $(TOP)/tool/tserver.c sqlite3.o -lpthread -o tserver
**
** To run using "x.db" as the db file:
**
**   ./tserver x.db
**
** To connect, open a client socket on port 9999 and start sending commands.
** Commands are either SQL - which must be terminated by a semi-colon, or
** dot-commands, which must be terminated by a newline. If an SQL statement
** is seen, it is prepared and added to an internal list.
**
** Dot-commands are:
**
**   .list                    Display all SQL statements in the list.
**   .quit                    Disconnect.
**   .run                     Run all SQL statements in the list.
**   .repeats N               Configure the number of repeats per ".run".
**   .seconds N               Configure the number of seconds to ".run" for.
**   .mutex_commit            Add a "COMMIT" protected by a g.commit_mutex
**                            to the current SQL.
**   .stop                    Stop the tserver process - exit(0).
**   .checkpoint N
**   .integrity_check
**
** Example input:
**
**   BEGIN;
**     INSERT INTO t1 VALUES(randomblob(10), randomblob(100));
**     INSERT INTO t1 VALUES(randomblob(10), randomblob(100));
**     INSERT INTO t1 VALUES(randomblob(10), randomblob(100));
**   COMMIT;
**   .repeats 100000
**   .run
**
*/
#define TSERVER_PORTNUMBER 9999

#include <arpa/inet.h>
#include <assert.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include "sqlite3.h"

#define TSERVER_DEFAULT_CHECKPOINT_THRESHOLD 3900

/* Global variables */
struct TserverGlobal {
  char *zDatabaseName;             /* Database used by this server */
  char *zVfs;
  sqlite3_mutex *commit_mutex;
  sqlite3 *db;                     /* Global db handle */

  /* The following use native pthreads instead of a portable interface. This
  ** is because a condition variable, as well as a mutex, is required.  */
  pthread_mutex_t ckpt_mutex;
  pthread_cond_t ckpt_cond;
  int nThreshold;                  /* Checkpoint when wal is this large */
  int bCkptRequired;               /* True if wal checkpoint is required */
  int nRun;                        /* Number of clients in ".run" */
  int nWait;                       /* Number of clients waiting on ckpt_cond */
};

static struct TserverGlobal g = {0};

typedef struct ClientSql ClientSql;
struct ClientSql {
  sqlite3_stmt *pStmt;
  int flags;
};

#define TSERVER_CLIENTSQL_MUTEX     0x0001
#define TSERVER_CLIENTSQL_INTEGRITY 0x0002

typedef struct ClientCtx ClientCtx;
struct ClientCtx {
  sqlite3 *db;                    /* Database handle for this client */
  int fd;                         /* Client fd */
  int nRepeat;                    /* Number of times to repeat SQL */
  int nSecond;                    /* Number of seconds to run for */
  ClientSql *aPrepare;            /* Array of prepared statements */
  int nPrepare;                   /* Valid size of apPrepare[] */
  int nAlloc;                     /* Allocated size of apPrepare[] */

  int nClientThreshold;           /* Threshold for checkpointing */
  int bClientCkptRequired;        /* True to do a checkpoint */
};

static int is_eol(int i){
  return (i=='\n' || i=='\r');
}
static int is_whitespace(int i){
  return (i==' ' || i=='\t' || is_eol(i));
}

/*
** Implementation of SQL scalar function usleep().
*/
static void usleepFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  int nUs;
  sqlite3_vfs *pVfs = (sqlite3_vfs*)sqlite3_user_data(context);
  assert( argc==1 );
  nUs = sqlite3_value_int64(argv[0]);
  pVfs->xSleep(pVfs, nUs);
}

static void trim_string(const char **pzStr, int *pnStr){
  const char *zStr = *pzStr;
  int nStr = *pnStr;

  while( nStr>0 && is_whitespace(zStr[0]) ){
    zStr++;
    nStr--;
  }
  while( nStr>0 && is_whitespace(zStr[nStr-1]) ){
    nStr--;
  }

  *pzStr = zStr;
  *pnStr = nStr;
}

static int send_message(ClientCtx *p, const char *zFmt, ...){
  char *zMsg;
  va_list ap;                             /* Vararg list */
  va_start(ap, zFmt);
  int res = -1;

  zMsg = sqlite3_vmprintf(zFmt, ap);
  if( zMsg ){
    res = write(p->fd, zMsg, strlen(zMsg));
  }
  sqlite3_free(zMsg);
  va_end(ap);

  return (res<0);
}

static int handle_some_sql(ClientCtx *p, const char *zSql, int nSql){
  const char *zTail = zSql;
  int nTail = nSql;
  int rc = SQLITE_OK;

  while( rc==SQLITE_OK ){
    if( p->nPrepare>=p->nAlloc ){
      int nByte = (p->nPrepare+32) * sizeof(ClientSql);
      ClientSql *aNew = sqlite3_realloc(p->aPrepare, nByte);
      if( aNew ){
        memset(&aNew[p->nPrepare], 0, sizeof(ClientSql)*32);
        p->aPrepare = aNew;
        p->nAlloc = p->nPrepare+32;
      }else{
        rc = SQLITE_NOMEM;
        break;
      }
    }
    rc = sqlite3_prepare_v2(
        p->db, zTail, nTail, &p->aPrepare[p->nPrepare].pStmt, &zTail
    );
    if( rc!=SQLITE_OK ){
      send_message(p, "error - %s (eec=%d)\n", sqlite3_errmsg(p->db),
          sqlite3_extended_errcode(p->db)
      );
      rc = 1;
      break;
    }
    if( p->aPrepare[p->nPrepare].pStmt==0 ){
      break;
    }
    p->nPrepare++;
    nTail = nSql - (zTail-zSql);
    rc = send_message(p, "ok (%d SQL statements)\n", p->nPrepare);
  }

  return rc;
}

/*
** Return a micro-seconds resolution timer.
*/
static sqlite3_int64 get_timer(void){
  struct timeval t;
  gettimeofday(&t, 0);
  return (sqlite3_int64)t.tv_usec + ((sqlite3_int64)t.tv_sec * 1000000);
}

static void clear_sql(ClientCtx *p){
  int j;
  for(j=0; j<p->nPrepare; j++){
    sqlite3_finalize(p->aPrepare[j].pStmt);
  }
  p->nPrepare = 0;
}

/*
** The sqlite3_wal_hook() callback used by all client database connections.
*/
static int clientWalHook(void *pArg, sqlite3 *db, const char *zDb, int nFrame){
  if( g.nThreshold>0 ){
    if( nFrame>=g.nThreshold ){
      g.bCkptRequired = 1;
    }
  }else{
    ClientCtx *pCtx = (ClientCtx*)pArg;
    if( pCtx->nClientThreshold && nFrame>=pCtx->nClientThreshold ){
      pCtx->bClientCkptRequired = 1;
    }
  }
  return SQLITE_OK;
}

static int handle_run_command(ClientCtx *p){
  int i, j;
  int nBusy = 0;
  sqlite3_int64 t0 = get_timer();
  sqlite3_int64 t1 = t0;
  sqlite3_int64 tCommit = 0;
  int nT1 = 0;
  int nTBusy1 = 0;
  int rc = SQLITE_OK;

  pthread_mutex_lock(&g.ckpt_mutex);
  g.nRun++;
  pthread_mutex_unlock(&g.ckpt_mutex);

  for(j=0; (p->nRepeat<=0 || j<p->nRepeat) && rc==SQLITE_OK; j++){
    sqlite3_int64 t2;

    for(i=0; i<p->nPrepare && rc==SQLITE_OK; i++){
      sqlite3_stmt *pStmt = p->aPrepare[i].pStmt;

      /* If the MUTEX flag is set, grab g.commit_mutex before executing
      ** the SQL statement (which is always "COMMIT" in this case). */
      if( p->aPrepare[i].flags & TSERVER_CLIENTSQL_MUTEX ){
        sqlite3_mutex_enter(g.commit_mutex);
        tCommit -= get_timer();
      }

      /* Execute the statement */
      if( p->aPrepare[i].flags & TSERVER_CLIENTSQL_INTEGRITY ){
        sqlite3_step(pStmt);
        if( sqlite3_stricmp("ok", (const char*)sqlite3_column_text(pStmt, 0)) ){
          send_message(p, "error - integrity_check failed: %s\n", 
              sqlite3_column_text(pStmt, 0)
          );
        }
        sqlite3_reset(pStmt);
      }
      while( sqlite3_step(pStmt)==SQLITE_ROW );
      rc = sqlite3_reset(pStmt);

      /* Relinquish the g.commit_mutex mutex if required. */
      if( p->aPrepare[i].flags & TSERVER_CLIENTSQL_MUTEX ){
        tCommit += get_timer();
        sqlite3_mutex_leave(g.commit_mutex);
      }

      if( (rc & 0xFF)==SQLITE_BUSY ){
        if( sqlite3_get_autocommit(p->db)==0 ){
          sqlite3_exec(p->db, "ROLLBACK", 0, 0, 0);
        }
        nBusy++;
        rc = SQLITE_OK;
        break;
      }
      else if( rc!=SQLITE_OK ){
        send_message(p, "error - %s (eec=%d)\n", sqlite3_errmsg(p->db),
            sqlite3_extended_errcode(p->db)
        );
      }
    }

    t2 = get_timer();
    if( t2>=(t1+1000000) ){
      sqlite3_int64 nUs = (t2 - t1);
      sqlite3_int64 nDone = (j+1 - nBusy - nT1);

      rc = send_message(
          p, "(%d done @ %lld per second, %d busy)\n", 
          (int)nDone, (1000000*nDone + nUs/2) / nUs, nBusy - nTBusy1
      );
      t1 = t2;
      nT1 = j+1 - nBusy;
      nTBusy1 = nBusy;
      if( p->nSecond>0 && ((sqlite3_int64)p->nSecond*1000000)<=t1-t0 ) break;
    }

    /* Global checkpoint handling. */
    if( g.nThreshold>0 ){
      pthread_mutex_lock(&g.ckpt_mutex);
      if( rc==SQLITE_OK && g.bCkptRequired ){
        if( g.nWait==g.nRun-1 ){
          /* All other clients are already waiting on the condition variable.
          ** Run the checkpoint, signal the condition and move on.  */
          rc = sqlite3_wal_checkpoint(p->db, "main");
          g.bCkptRequired = 0;
          pthread_cond_broadcast(&g.ckpt_cond);
        }else{
          assert( g.nWait<g.nRun-1 );
          g.nWait++;
          pthread_cond_wait(&g.ckpt_cond, &g.ckpt_mutex);
          g.nWait--;
        }
      }
      pthread_mutex_unlock(&g.ckpt_mutex);
    }

    if( rc==SQLITE_OK && p->bClientCkptRequired ){
      rc = sqlite3_wal_checkpoint(p->db, "main");
      if( rc==SQLITE_BUSY ) rc = SQLITE_OK;
      assert( rc==SQLITE_OK );
      p->bClientCkptRequired = 0;
    }
  }

  if( rc==SQLITE_OK ){
    int nMs = (get_timer() - t0) / 1000;
    send_message(p, "ok (%d/%d SQLITE_BUSY)\n", nBusy, j);
    if( p->nRepeat<=0 ){
      send_message(p, "### ok %d busy %d ms %d commit-ms %d\n", 
          j-nBusy, nBusy, nMs, (int)(tCommit / 1000)
      );
    }
  }
  clear_sql(p);

  pthread_mutex_lock(&g.ckpt_mutex);
  g.nRun--;
  pthread_mutex_unlock(&g.ckpt_mutex);

  return rc;
}

static int handle_dot_command(ClientCtx *p, const char *zCmd, int nCmd){
  int n;
  int rc = 0;
  const char *z = &zCmd[1];
  const char *zArg;
  int nArg;

  assert( zCmd[0]=='.' );
  for(n=0; n<(nCmd-1); n++){
    if( is_whitespace(z[n]) ) break;
  }

  zArg = &z[n];
  nArg = nCmd-n;
  trim_string(&zArg, &nArg);

  if( n>=1 && n<=4 && 0==strncmp(z, "list", n) ){
    int i;
    for(i=0; rc==0 && i<p->nPrepare; i++){
      const char *zSql = sqlite3_sql(p->aPrepare[i].pStmt);
      int nSql = strlen(zSql);
      trim_string(&zSql, &nSql);
      rc = send_message(p, "%d: %.*s\n", i, nSql, zSql);
    }
  }

  else if( n>=1 && n<=4 && 0==strncmp(z, "quit", n) ){
    rc = -1;
  }

  else if( n>=2 && n<=7 && 0==strncmp(z, "repeats", n) ){
    if( nArg ){
      p->nRepeat = strtol(zArg, 0, 0);
      if( p->nRepeat>0 ) p->nSecond = 0;
    }
    rc = send_message(p, "ok (repeat=%d)\n", p->nRepeat);
  }

  else if( n>=2 && n<=3 && 0==strncmp(z, "run", n) ){
    rc = handle_run_command(p);
  }

  else if( n>=2 && n<=7 && 0==strncmp(z, "seconds", n) ){
    if( nArg ){
      p->nSecond = strtol(zArg, 0, 0);
      if( p->nSecond>0 ) p->nRepeat = 0;
    }
    rc = send_message(p, "ok (seconds=%d)\n", p->nSecond);
  }

  else if( n>=1 && n<=12 && 0==strncmp(z, "mutex_commit", n) ){
    rc = handle_some_sql(p, "COMMIT;", 7);
    if( rc==SQLITE_OK ){
      p->aPrepare[p->nPrepare-1].flags |= TSERVER_CLIENTSQL_MUTEX;
    }
  }

  else if( n>=1 && n<=10 && 0==strncmp(z, "checkpoint", n) ){
    if( nArg ){
      p->nClientThreshold = strtol(zArg, 0, 0);
    }
    rc = send_message(p, "ok (checkpoint=%d)\n", p->nClientThreshold);
  }

  else if( n>=2 && n<=4 && 0==strncmp(z, "stop", n) ){
    sqlite3_close(g.db);
    exit(0);
  }

  else if( n>=2 && n<=15 && 0==strncmp(z, "integrity_check", n) ){
    rc = handle_some_sql(p, "PRAGMA integrity_check;", 23);
    if( rc==SQLITE_OK ){
      p->aPrepare[p->nPrepare-1].flags |= TSERVER_CLIENTSQL_INTEGRITY;
    }
  }

  else{
    send_message(p, 
        "unrecognized dot command: %.*s\n"
        "should be \"list\", \"run\", \"repeats\", \"mutex_commit\", "
        "\"checkpoint\", \"integrity_check\" or \"seconds\"\n", n, z
    );
    rc = 1;
  }

  return rc;
}

static void *handle_client(void *pArg){
  char zCmd[32*1024];             /* Read buffer */
  int nCmd = 0;                   /* Valid bytes in zCmd[] */
  int res;                        /* Result of read() call */
  int rc = SQLITE_OK;

  ClientCtx ctx;
  memset(&ctx, 0, sizeof(ClientCtx));

  ctx.fd = (int)(intptr_t)pArg;
  ctx.nRepeat = 1;
  rc = sqlite3_open_v2(g.zDatabaseName, &ctx.db,
      SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE, g.zVfs
  );
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "sqlite3_open(): %s\n", sqlite3_errmsg(ctx.db));
    return 0;
  }
  sqlite3_create_function(
      ctx.db, "usleep", 1, SQLITE_UTF8, (void*)sqlite3_vfs_find(0), 
      usleepFunc, 0, 0
  );

  /* Register the wal-hook with the new client connection */
  sqlite3_wal_hook(ctx.db, clientWalHook, (void*)&ctx);

  while( rc==SQLITE_OK ){
    int i;
    int iStart;
    int nConsume;
    res = read(ctx.fd, &zCmd[nCmd], sizeof(zCmd)-nCmd-1);
    if( res<=0 ) break;
    nCmd += res;
    if( nCmd>=sizeof(zCmd)-1 ){
      fprintf(stderr, "oversized (>32KiB) message\n");
      res = 0;
      break;
    }
    zCmd[nCmd] = '\0';

    do {
      nConsume = 0;

      /* Gobble up any whitespace */
      iStart = 0;
      while( is_whitespace(zCmd[iStart]) ) iStart++;

      if( zCmd[iStart]=='.' ){
        /* This is a dot-command. Search for end-of-line. */
        for(i=iStart; i<nCmd; i++){
          if( is_eol(zCmd[i]) ){
            rc = handle_dot_command(&ctx, &zCmd[iStart], i-iStart);
            nConsume = i+1;
            break;
          }
        }
      }else{

        int iSemi;
        char c = 0;
        for(iSemi=iStart; iSemi<nCmd; iSemi++){
          if( zCmd[iSemi]==';' ){
            c = zCmd[iSemi+1];
            zCmd[iSemi+1] = '\0';
            break;
          }
        }

        if( iSemi<nCmd ){
          if( sqlite3_complete(zCmd) ){
            rc = handle_some_sql(&ctx, zCmd, iSemi+1);
            nConsume = iSemi+1;
          }

          if( c ){
            zCmd[iSemi+1] = c;
          }
        }
      }

      if( nConsume>0 ){
        nCmd = nCmd-nConsume;
        if( nCmd>0 ){
          memmove(zCmd, &zCmd[nConsume], nCmd);
        }
      }
    }while( rc==SQLITE_OK && nConsume>0 );
  }

  fprintf(stdout, "Client %d disconnects (rc=%d)\n", ctx.fd, rc);
  fflush(stdout);
  close(ctx.fd);
  clear_sql(&ctx);
  sqlite3_free(ctx.aPrepare);
  sqlite3_close(ctx.db);
  return 0;
} 

static void usage(const char *zExec){
  fprintf(stderr, "Usage: %s ?-vfs VFS? DATABASE\n", zExec);
  exit(1);
}

int main(int argc, char *argv[]) {
  int sfd;
  int rc;
  int yes = 1;
  struct sockaddr_in server;
  int i;

  /* Ignore SIGPIPE. Otherwise the server exits if a client disconnects
  ** abruptly.  */
  signal(SIGPIPE, SIG_IGN);

  g.nThreshold = TSERVER_DEFAULT_CHECKPOINT_THRESHOLD;
  if( (argc%2) ) usage(argv[0]);
  for(i=1; i<(argc-1); i+=2){
    int n = strlen(argv[i]);
    if( n>=2 && 0==sqlite3_strnicmp("-walautocheckpoint", argv[i], n) ){
      g.nThreshold = strtol(argv[i+1], 0, 0);
    }else 
    if( n>=2 && 0==sqlite3_strnicmp("-vfs", argv[i], n) ){
      g.zVfs = argv[i+1];
    }
  }
  g.zDatabaseName = argv[argc-1];

  g.commit_mutex = sqlite3_mutex_alloc(SQLITE_MUTEX_FAST);
  pthread_mutex_init(&g.ckpt_mutex, 0);
  pthread_cond_init(&g.ckpt_cond, 0);

  rc = sqlite3_open_v2(g.zDatabaseName, &g.db,
      SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE, g.zVfs
  );
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "sqlite3_open(): %s\n", sqlite3_errmsg(g.db));
    return 1;
  }

  rc = sqlite3_exec(g.db, "SELECT * FROM sqlite_master", 0, 0, 0);
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "sqlite3_exec(): %s\n", sqlite3_errmsg(g.db));
    return 1;
  }

  sfd = socket(AF_INET, SOCK_STREAM, 0);
  if( sfd<0 ){
    fprintf(stderr, "socket() failed\n");
    return 1;
  }

  rc = setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
  if( rc<0 ){
    perror("setsockopt");
    return 1;
  }

  memset(&server, 0, sizeof(server));
  server.sin_family = AF_INET;
  server.sin_addr.s_addr = inet_addr("127.0.0.1");
  server.sin_port = htons(TSERVER_PORTNUMBER);

  rc = bind(sfd, (struct sockaddr *)&server, sizeof(struct sockaddr));
  if( rc<0 ){
    fprintf(stderr, "bind() failed\n");
    return 1;
  }

  rc = listen(sfd, 8);
  if( rc<0 ){
    fprintf(stderr, "listen() failed\n");
    return 1;
  }

  while( 1 ){
    pthread_t tid;
    int cfd = accept(sfd, NULL, NULL);
    if( cfd<0 ){
      perror("accept()");
      return 1;
    }

    fprintf(stdout, "Client %d connects\n", cfd);
    fflush(stdout);
    rc = pthread_create(&tid, NULL, handle_client, (void*)(intptr_t)cfd);
    if( rc!=0 ){
      perror("pthread_create()");
      return 1;
    }

    pthread_detach(tid);
  }

  return 0;
}
