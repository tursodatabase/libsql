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
   A callback for use with sqlite3_preupdate_hook().
*/
public interface PreUpdateHook {
  /**
     Must function as described for the sqlite3_preupdate_hook().
     callback, with the slight signature change.

     Must not throw. Any exceptions may emit debugging messages and
     will be suppressed.
  */
  void xPreUpdate(sqlite3 db, int op, String dbName, String dbTable,
                  long iKey1, long iKey2 );
}
