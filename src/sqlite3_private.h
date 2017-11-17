/*
 *  sqlite3_private.h
 */

#ifndef _SQLITE3_PRIVATE_H
#define _SQLITE3_PRIVATE_H

#ifdef __cplusplus
extern "C" {
#endif

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
** to truncate a database and its associated journal file to zero length.  The 
** SQLITE_TRUNCATE_* flags represent optional flags to safely initialize an
** empty database in the place of the truncated database, the flags are passed 
** into sqlite3_file_control via the fourth argument using a pointer to an integer
** configured with the ORed flags.  If the fourth argument is NULL, the default 
** behavior is applied and the database file is truncated to zero bytes, a rollback 
** journal (if present) is unlinked, a WAL journal (if present) is truncated to zero 
** bytes and the first few bytes of the -shm file is scrambled to trigger existing
** connections to rebuild the index from the database file contents.
*/
#define SQLITE_FCNTL_TRUNCATE_DATABASE      101
#define SQLITE_TRUNCATE_DATABASE            SQLITE_FCNTL_TRUNCATE_DATABASE
#define SQLITE_TRUNCATE_INITIALIZE_HEADER_MASK    (0x7F<<0)
#define SQLITE_TRUNCATE_JOURNALMODE_WAL           (0x1<<0)
#define SQLITE_TRUNCATE_AUTOVACUUM_MASK           (0x3<<2)
#define SQLITE_TRUNCATE_AUTOVACUUM_OFF            (0x1<<2)
#define SQLITE_TRUNCATE_AUTOVACUUM_FULL           (0x2<<2)
#define SQLITE_TRUNCATE_AUTOVACUUM_INCREMENTAL    (0x3<<2)
#define SQLITE_TRUNCATE_PAGESIZE_MASK             (0x7<<4)
#define SQLITE_TRUNCATE_PAGESIZE_1024             (0x1<<4)
#define SQLITE_TRUNCATE_PAGESIZE_2048             (0x2<<4)
#define SQLITE_TRUNCATE_PAGESIZE_4096             (0x3<<4)
#define SQLITE_TRUNCATE_PAGESIZE_8192             (0x4<<4)
#define SQLITE_TRUNCATE_FORCE                     (0x1<<7)

/*
** Pass the SQLITE_REPLACE_DATABASE operation code to sqlite3_file_control()
** and a sqlite3 pointer to another open database file to safely copy the 
** contents of that database file into the receiving database.
*/
#define SQLITE_FCNTL_REPLACE_DATABASE       102
#define SQLITE_REPLACE_DATABASE             SQLITE_FCNTL_REPLACE_DATABASE

#ifdef __cplusplus
}  /* End of the 'extern "C"' block */
#endif
#endif
