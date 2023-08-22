/*
** 2023-08-05
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
   A callback for use with sqlite3_auto_extension().
*/
public interface AutoExtension {
  /**
     Must function as described for a sqlite3_auto_extension()
     callback, with the caveat that the signature is more limited.

     As an exception (as it were) to the callbacks-must-not-throw
     rule, AutoExtensions may throw and the exception's error message
     will be set as the db's error string.

     Hints for implementations:

     - Opening a database from an auto-extension handler will lead to
       an endless recursion of the auto-handler triggering itself
       indirectly for each newly-opened database.

     - If this routine is stateful, it is a good idea to make the
       overridden method synchronized.

     - Results are undefined if db is closed by an auto-extension.
  */
  int xEntryPoint(sqlite3 db);
}
