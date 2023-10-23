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
package org.sqlite.jni.capi;

/**
   Callback for use with {@link CApi#sqlite3_preupdate_hook}.
*/
public interface PreupdateHookCallback extends CallbackProxy {
  /**
     Must function as described for the C-level sqlite3_preupdate_hook()
     callback.
  */
  void call(sqlite3 db, int op, String dbName, String dbTable,
            long iKey1, long iKey2 );
}
