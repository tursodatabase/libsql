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
package org.sqlite.jni.capi;

/**
   A callback for use with sqlite3_config().
*/
public interface ConfigSqlLogCallback {
  /**
     Must function as described for a C-level callback for
     {@link CApi#sqlite3_config(ConfigSqlLogCallback)}, with the slight signature change.
  */
  void call(sqlite3 db, String msg, int msgType );
}
