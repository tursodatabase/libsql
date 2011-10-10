/*
 *  sqlite3_private.h
 */

#ifndef _SQLITE3_PRIVATE_H
#define _SQLITE3_PRIVATE_H

#define SQLITE_LOCKSTATE_OFF    0
#define SQLITE_LOCKSTATE_ON     1
#define SQLITE_LOCKSTATE_NOTADB 2
#define SQLITE_LOCKSTATE_ERROR  -1

#define SQLITE_LOCKSTATE_ANYPID -1

/* 
** Test a file path for sqlite locks held by a process ID (-1 = any PID). 
** Returns one of the following integer codes:
** 
**   SQLITE_LOCKSTATE_OFF    no active sqlite file locks match the specified pid
**   SQLITE_LOCKSTATE_ON     active sqlite file locks match the specified pid
**   SQLITE_LOCKSTATE_NOTADB path points to a file that is not an sqlite db file
**   SQLITE_LOCKSTATE_ERROR  path was not vaild or was unreadable
**
** There is no support for identifying db files encrypted via SEE encryption
** currently.  Zero byte files are tested for sqlite locks, but if no sqlite 
** locks are present then SQLITE_LOCKSTATE_NOTADB is returned.
*/
extern int _sqlite3_lockstate(const char *path, pid_t pid);

/*
** Test an open database connection for sqlite locks held by a process ID,
** if a process has an open database connection this will avoid trashing file
** locks by re-using open file descriptors for the database file and support
** files (-shm)
*/
#define SQLITE_FCNTL_LOCKSTATE_PID          103

/*
** Pass the SQLITE_TRUNCATE_DATABASE operation code to sqlite3_file_control() 
** to truncate a database and its associated journal file to zero length.
*/
#define SQLITE_FCNTL_TRUNCATE_DATABASE      101
#define SQLITE_TRUNCATE_DATABASE            SQLITE_FCNTL_TRUNCATE_DATABASE

/*
** Pass the SQLITE_REPLACE_DATABASE operation code to sqlite3_file_control() 
** and a sqlite3 pointer to another open database file to safely copy the 
** contents of that database file into the receiving database.
*/
#define SQLITE_FCNTL_REPLACE_DATABASE       102
#define SQLITE_REPLACE_DATABASE             SQLITE_FCNTL_REPLACE_DATABASE

#endif
