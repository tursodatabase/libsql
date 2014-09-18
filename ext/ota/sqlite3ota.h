/*
** 2014 August 30
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
** This file contains the public interface for the OTA extension. 
*/

/*
** SUMMARY
**
** Writing a transaction containing a large number of operations on 
** b-tree indexes that are collectively larger than the available cache
** memory can be very inefficient. 
**
** The problem is that in order to update a b-tree, the leaf page (at least)
** containing the entry being inserted or deleted must be modified. If the
** working set of leaves is larger than the available cache memory, then a 
** single leaf that is modified more than once as part of the transaction 
** may be loaded from or written to the persistent media more than once. 
** Additionally, because the index updates are likely to be applied in
** random order, access to pages within the databse is also likely to be in 
** random order, which is itself quite inefficient.
**
** One way to improve the situation is to sort the operations on each index
** by index key before applying them to the b-tree. This leads to an IO
** pattern that resembles a single linear scan through the index b-tree,
** and all but guarantees each modified leaf page is loaded and stored 
** exactly once. SQLite uses this trick to improve the performance of
** CREATE INDEX commands. This extension allows it to be used to improve
** the performance of large transactions on existing databases.
**
** Additionally, this extension allows the work involved in writing the 
** large transaction to be broken down into sub-transactions performed 
** sequentially by separate processes. This is useful if the system cannot 
** guarantee that a single update process may run for long enough to apply 
** the entire update, for example because the update is running on a mobile
** device that is frequently rebooted. Even after the writer process has 
** committed one or more sub-transactions, other database clients continue
** to read from the original database snapshot. In other words, partially 
** applied transactions are not visible to other clients. 
**
** "OTA" stands for "Over The Air" update. As in a large database update
** transmitted via a wireless network to a mobile device. A transaction
** applied using this extension is hence refered to as an "OTA update".
**
**
** LIMITATIONS
**
** An "OTA update" transaction is subject to the following limitations:
**
**   * The transaction must consist of INSERT, UPDATE and DELETE operations
**     only.
**
**   * INSERT statements may not use any default values.
**
**   * UPDATE and DELETE statements must identify their target rows by
**     real PRIMARY KEY values - i.e. INTEGER PRIMARY KEY columns or 
**     by the PRIMARY KEY columns of WITHOUT ROWID tables.
**
**   * UPDATE statements may not modify real PRIMARY KEY columns.
**
**   * No triggers will be fired.
**
**   * No foreign key violations are detected or reported.
**
**   * No constraint handling mode except for "OR ROLLBACK" is supported.
**
**
** PREPARATION
**
** An "OTA update" is stored as a separate SQLite database. A database
** containing an OTA update is an "OTA database". For each table in the 
** target database to be updated, the OTA database should contain a table
** named "data_<target name>" containing the same set of columns as the
** target table, and one more - "ota_control". The data_% table should 
** have no PRIMARY KEY or UNIQUE constraints, but each column should have
** the same type as the corresponding column in the target database.
** The "ota_control" column should have no type at all. For example, if
** the target database contains:
**
**   CREATE TABLE t1(a INTEGER PRIMARY KEY, b TEXT, c UNIQUE);
**
** Then the OTA database should contain:
**
**   CREATE TABLE data_t1(a INTEGER, b TEXT, c, ota_control);
**
** The order of the columns in the data_% table does not matter.
**
** For each row to INSERT into the target database as part of the OTA 
** update, the corresponding data_% table should contain a single record
** with the "ota_control" column set to contain integer value 0. The
** other columns should be set to the values that make up the new record 
** to insert. 
**
** If the target database table has an INTEGER PRIMARY KEY and there are
** one or more auxiliary indexes, it is not possible to insert a NULL value
** into the IPK column. Attempting to do so results in an SQLITE_MISMATCH
** error.
**
** For each row to DELETE from the target database as part of the OTA 
** update, the corresponding data_% table should contain a single record
** with the "ota_control" column set to contain integer value 1. The
** real primary key values of the row to delete should be stored in the
** corresponding columns of the data_% table. The values stored in the
** other columns are not used.
**
** For each row to UPDATE from the target database as part of the OTA 
** update, the corresponding data_% table should contain a single record
** with the "ota_control" column set to contain a value of type text.
** The real primary key values identifying the row to update should be 
** stored in the corresponding columns of the data_% table row, as should
** the new values of all columns being update. The text value in the 
** "ota_control" column must contain the same number of characters as
** there are column in the target database table, and must consist entirely
** of "x" and "." characters. For each column that is being updated,
** the corresponding character is set to "x". For those that remain as
** they are, the corresponding character of the ota_control value should
** be set to ".". For example, given the tables above, the update 
** statement:
**
**   UPDATE t1 SET c = 'usa' WHERE a = 4;
**
** is represented by the data_t1 row created by:
**
**   INSERT INTO data_t1(a, b, c, ota_control) VALUES(4, NULL, 'usa', '..x');
**
**
** USAGE
**
** The API declared below allows an application to apply an OTA update 
** stored on disk to an existing target database. Essentially, the 
** application:
**
**     1) Opens an OTA handle using the sqlite3ota_open() function.
**
**     2) Calls the sqlite3ota_step() function one or more times on
**        the new handle. Each call to sqlite3ota_step() performs a single
**        b-tree operation, so thousands of calls may be required to apply 
**        a complete update.
**
**     3) Calls sqlite3ota_close() to close the OTA update handle. If
**        sqlite3ota_step() has been called enough times to completely
**        apply the update to the target database, then it is committed
**        and made visible to other database clients at this point. 
**        Otherwise, the state of the OTA update application is saved
**        in the OTA database for later resumption.
**
** See comments below for more detail on APIs.
**
** If an update is only partially applied to the target database by the
** time sqlite3ota_close() is called, various state information is saved 
** within the OTA database. This allows subsequent processes to automatically
** resume the OTA update from where it left off.
**
** To remove all OTA extension state information, returning an OTA database 
** to its original contents, it is sufficient to drop all tables that begin
** with the prefix "ota_"
*/

#ifndef _SQLITE3OTA_H
#define _SQLITE3OTA_H

#include <sqlite3.h>              /* Required for error code definitions */

typedef struct sqlite3ota sqlite3ota;

/*
** Open an OTA handle.
**
** Argument zTarget is the path to the target database. Argument zOta is
** the path to the OTA database. Each call to this function must be matched
** by a call to sqlite3ota_close().
*/
sqlite3ota *sqlite3ota_open(const char *zTarget, const char *zOta);

/*
** Do some work towards applying the OTA update to the target db. 
**
** Return SQLITE_DONE if the update has been completely applied, or 
** SQLITE_OK if no error occurs but there remains work to do to apply
** the OTA update. If an error does occur, some other error code is 
** returned. 
**
** Once a call to sqlite3ota_step() has returned a value other than
** SQLITE_OK, all subsequent calls on the same OTA handle are no-ops
** that immediately return the same value.
*/
int sqlite3ota_step(sqlite3ota *pOta);

/*
** Close an OTA handle. 
**
** If the OTA update has been completely applied, commit it to the target 
** database. Otherwise, assuming no error has occurred, save the current 
** state of the OTA update appliation to the OTA database.
**
** If an error has already occurred as part of an sqlite3ota_step()
** or sqlite3ota_open() call, or if one occurs within this function, an
** SQLite error code is returned. Additionally, *pzErrmsg may be set to
** point to a buffer containing a utf-8 formatted English language error
** message. It is the responsibility of the caller to eventually free any 
** such buffer using sqlite3_free().
**
** Otherwise, if no error occurs, this function returns SQLITE_OK if the
** update has been partially applied, or SQLITE_DONE if it has been 
** completely applied.
*/
int sqlite3ota_close(sqlite3ota *pOta, char **pzErrmsg);

/*
** Return the total number of key-value operations (inserts, deletes or 
** updates) that have been performed on the target database since the
** current OTA update was started.
*/
sqlite3_int64 sqlite3ota_progress(sqlite3ota *pOta);

#endif /* _SQLITE3OTA_H */

