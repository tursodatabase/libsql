/*
 *  sqlite3_private.h
 */

#ifndef _SQLITE3_PRIVATE_H
#define _SQLITE3_PRIVATE_H

/*
** Pass the SQLITE_TRUNCATE_DATABASE operation code to sqlite3_file_control() 
** to truncate a database and its associated journal file to zero length.
*/
#define SQLITE_TRUNCATE_DATABASE      101

/*
** Pass the SQLITE_REPLACE_DATABASE operation code to sqlite3_file_control() 
** and a sqlite3 pointer to another open database file to safely copy the 
** contents of that database file into the receiving database.
*/
#define SQLITE_REPLACE_DATABASE       102

#endif
