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

int sqlite3ServerConnect(Pager *pPager, Server **ppOut, int *piClient);

void sqlite3ServerDisconnect(Server *p, sqlite3_file *dbfd);

int sqlite3ServerBegin(Server *p);
int sqlite3ServerEnd(Server *p);
int sqlite3ServerReleaseWriteLocks(Server *p);

int sqlite3ServerLock(Server *p, Pgno pgno, int bWrite, int bBlock);

#endif /* SQLITE_SERVER_H */

#endif /* SQLITE_SERVER_EDITION */
