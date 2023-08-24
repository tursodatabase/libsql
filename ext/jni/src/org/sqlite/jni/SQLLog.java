/*
** 2023-08-23
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file is part of the JNI bindings for the sqlite3 C API.
*/
package org.sqlite.jni;

/**
   A callback for use with sqlite3_config(SQLLog).
*/
public interface SQLLog {
  /**
     Must function as described for sqlite3_config(SQLITE_CONFIG_SQLLOG)
     callback, with the slight signature change.
  */
  void xSqllog(sqlite3 db, String msg, int msgType );
}
