/*
** 2023-06-21
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
** This file implements an extension that uses the SQLITE_CONFIG_PCACHE2
** mechanism to add a tracing layer on top of pluggable page cache of
** SQLite.  If this extension is registered prior to sqlite3_initialize(),
** it will cause all page cache activities to be logged on standard output,
** or to some other FILE specified by the initializer.
**
** This file needs to be compiled into the application that uses it.
**
** This extension is used to implement the --pcachetrace option of the
** command-line shell.
*/
#include <assert.h>
#include <string.h>
#include <stdio.h>

/* The original page cache routines */
static sqlite3_pcache_methods2 pcacheBase;
static FILE *pcachetraceOut;

/* Methods that trace pcache activity */
static int pcachetraceInit(void *pArg){
  int nRes;
  if( pcachetraceOut ){
    fprintf(pcachetraceOut, "PCACHETRACE: xInit(%p)\n", pArg);
  }
  nRes = pcacheBase.xInit(pArg);
  if( pcachetraceOut ){
    fprintf(pcachetraceOut, "PCACHETRACE: xInit(%p) -> %d\n", pArg, nRes);
  }
  return nRes;
}
static void pcachetraceShutdown(void *pArg){
  if( pcachetraceOut ){
    fprintf(pcachetraceOut, "PCACHETRACE: xShutdown(%p)\n", pArg);
  }
  pcacheBase.xShutdown(pArg);
}
static sqlite3_pcache *pcachetraceCreate(int szPage, int szExtra, int bPurge){
  sqlite3_pcache *pRes;
  if( pcachetraceOut ){
    fprintf(pcachetraceOut, "PCACHETRACE: xCreate(%d,%d,%d)\n",
            szPage, szExtra, bPurge);
  }
  pRes = pcacheBase.xCreate(szPage, szExtra, bPurge);
  if( pcachetraceOut ){
    fprintf(pcachetraceOut, "PCACHETRACE: xCreate(%d,%d,%d) -> %p\n",
            szPage, szExtra, bPurge, pRes);
  }
  return pRes;
}
static void pcachetraceCachesize(sqlite3_pcache *p, int nCachesize){
  if( pcachetraceOut ){
    fprintf(pcachetraceOut, "PCACHETRACE: xCachesize(%p, %d)\n", p, nCachesize);
  }
  pcacheBase.xCachesize(p, nCachesize);
}
static int pcachetracePagecount(sqlite3_pcache *p){
  int nRes;
  if( pcachetraceOut ){
    fprintf(pcachetraceOut, "PCACHETRACE: xPagecount(%p)\n", p);
  }
  nRes = pcacheBase.xPagecount(p);
  if( pcachetraceOut ){
    fprintf(pcachetraceOut, "PCACHETRACE: xPagecount(%p) -> %d\n", p, nRes);
  }
  return nRes;
}
static sqlite3_pcache_page *pcachetraceFetch(
  sqlite3_pcache *p,
  unsigned key,
  int crFg
){
  sqlite3_pcache_page *pRes;
  if( pcachetraceOut ){
    fprintf(pcachetraceOut, "PCACHETRACE: xFetch(%p,%u,%d)\n", p, key, crFg);
  }
  pRes = pcacheBase.xFetch(p, key, crFg);
  if( pcachetraceOut ){
    fprintf(pcachetraceOut, "PCACHETRACE: xFetch(%p,%u,%d) -> %p\n",
            p, key, crFg, pRes);
  }
  return pRes;
}
static void pcachetraceUnpin(
  sqlite3_pcache *p,
  sqlite3_pcache_page *pPg,
  int bDiscard
){
  if( pcachetraceOut ){
    fprintf(pcachetraceOut, "PCACHETRACE: xUnpin(%p, %p, %d)\n",
            p, pPg, bDiscard);
  }
  pcacheBase.xUnpin(p, pPg, bDiscard);
}
static void pcachetraceRekey(
  sqlite3_pcache *p,
  sqlite3_pcache_page *pPg,
  unsigned oldKey,
  unsigned newKey
){
  if( pcachetraceOut ){
    fprintf(pcachetraceOut, "PCACHETRACE: xRekey(%p, %p, %u, %u)\n",
        p, pPg, oldKey, newKey);
  }
  pcacheBase.xRekey(p, pPg, oldKey, newKey);
}
static void pcachetraceTruncate(sqlite3_pcache *p, unsigned n){
  if( pcachetraceOut ){
    fprintf(pcachetraceOut, "PCACHETRACE: xTruncate(%p, %u)\n", p, n);
  }
  pcacheBase.xTruncate(p, n);
}
static void pcachetraceDestroy(sqlite3_pcache *p){
  if( pcachetraceOut ){
    fprintf(pcachetraceOut, "PCACHETRACE: xDestroy(%p)\n", p);
  }
  pcacheBase.xDestroy(p);
}
static void pcachetraceShrink(sqlite3_pcache *p){
  if( pcachetraceOut ){
    fprintf(pcachetraceOut, "PCACHETRACE: xShrink(%p)\n", p);
  }
  pcacheBase.xShrink(p);
}

/* The substitute pcache methods */
static sqlite3_pcache_methods2 ersaztPcacheMethods = {
  0,
  0,
  pcachetraceInit,
  pcachetraceShutdown,
  pcachetraceCreate,
  pcachetraceCachesize,
  pcachetracePagecount,
  pcachetraceFetch,
  pcachetraceUnpin,
  pcachetraceRekey,
  pcachetraceTruncate,
  pcachetraceDestroy,
  pcachetraceShrink
};

/* Begin tracing memory allocations to out. */
int sqlite3PcacheTraceActivate(FILE *out){
  int rc = SQLITE_OK;
  if( pcacheBase.xFetch==0 ){
    rc = sqlite3_config(SQLITE_CONFIG_GETPCACHE2, &pcacheBase);
    if( rc==SQLITE_OK ){
      rc = sqlite3_config(SQLITE_CONFIG_PCACHE2, &ersaztPcacheMethods);
    }
  }
  pcachetraceOut = out;
  return rc;
}

/* Deactivate memory tracing */
int sqlite3PcacheTraceDeactivate(void){
  int rc = SQLITE_OK;
  if( pcacheBase.xFetch!=0 ){
    rc = sqlite3_config(SQLITE_CONFIG_PCACHE2, &pcacheBase);
    if( rc==SQLITE_OK ){
      memset(&pcacheBase, 0, sizeof(pcacheBase));
    }
  }
  pcachetraceOut = 0;
  return rc;
}
