/*
** 2017 April 24
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
*/

#ifdef SQLITE_SERVER_EDITION

#ifndef SQLITE_SERVER_H
#define SQLITE_SERVER_H


typedef struct Server Server;

typedef struct ServerPage ServerPage;
struct ServerPage {
  Pgno pgno;                      /* Page number for this record */
  int nData;                      /* Size of aData[] in bytes */
  u8 *aData;
  ServerPage *pNext;

  int iCommitId;
  ServerPage *pHashNext;
  ServerPage *pHashPrev;
};

int sqlite3ServerConnect(Pager *pPager, int eServer, Server **ppOut);
void sqlite3ServerDisconnect(Server *p, sqlite3_file *dbfd);

int sqlite3ServerBegin(Server *p, int bReadonly);
int sqlite3ServerPreCommit(Server*, ServerPage*);
int sqlite3ServerEnd(Server *p);

int sqlite3ServerEndWrite(Server *p);

int sqlite3ServerLock(Server *p, Pgno pgno, int bWrite, int bBlock);

ServerPage *sqlite3ServerBuffer(Server*);

int sqlite3ServerIsSingleProcess(Server*);

/* For "BEGIN READONLY" clients. */
int sqlite3ServerIsReadonly(Server*);
void sqlite3ServerReadPage(Server*, Pgno, u8**);
void sqlite3ServerEndReadPage(Server*, Pgno);

#endif /* SQLITE_SERVER_H */
#endif /* SQLITE_SERVER_EDITION */

