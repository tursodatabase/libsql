
#ifndef __SQLITEASYNC_H_
#define __SQLITEASYNC_H_ 1

#define SQLITEASYNC_VFSNAME "sqlite3async"

/*
** Install the asynchronous IO VFS.
*/ 
int sqlite3async_initialize(const char *zParent, int isDefault);

/*
** Uninstall the asynchronous IO VFS.
*/ 
void sqlite3async_shutdown();

/*
** Process events on the write-queue.
*/
void sqlite3async_run();

/*
** Control/configure the asynchronous IO system.
*/
int sqlite3async_control(int op, ...);

/*
** Values that can be used as the first argument to sqlite3async_control().
*/
#define SQLITEASYNC_HALT       1
#define SQLITEASYNC_DELAY      2
#define SQLITEASYNC_GET_HALT   3
#define SQLITEASYNC_GET_DELAY  4

/*
** If the first argument to sqlite3async_control() is SQLITEASYNC_HALT,
** the second argument should be one of the following.
*/
#define SQLITEASYNC_HALT_NEVER 0       /* Never halt (default value) */
#define SQLITEASYNC_HALT_NOW   1       /* Halt as soon as possible */
#define SQLITEASYNC_HALT_IDLE  2       /* Halt when write-queue is empty */

#endif        /* ifndef __SQLITEASYNC_H_ */

