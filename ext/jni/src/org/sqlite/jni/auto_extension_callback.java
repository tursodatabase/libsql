/*
** 2023-08-25
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
   A callback for use with the sqlite3_auto_extension() family of
   APIs.
*/
public interface auto_extension_callback extends sqlite3_callback_proxy {
  /**
     Must function as described for a C-level
     sqlite3_auto_extension() callback, with the caveat that the
     signature is shorter.

     This callback may throw and the exception's error message will
     be set as the db's error string.

     Tips for implementations:

     - Opening a database from an auto-extension handler will lead to
     an endless recursion of the auto-handler triggering itself
     indirectly for each newly-opened database.

     - If this routine is stateful, it may be useful to make the
     overridden method synchronized.

     - Results are undefined if db is closed by an auto-extension.
  */
  int call(sqlite3 db);
}
