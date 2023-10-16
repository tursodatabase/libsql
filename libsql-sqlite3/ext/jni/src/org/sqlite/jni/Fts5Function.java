/*
** 2023-08-04
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
   Fts5Function is used in conjunction with the
   sqlite3_create_fts_function() JNI-bound API to give that native code
   access to the callback functions needed in order to implement
   FTS5 auxiliary functions in Java.
*/
public abstract class Fts5Function {

  public abstract void xFunction(Fts5ExtensionApi pApi, Fts5Context pFts,
                                 sqlite3_context pCtx, sqlite3_value argv[]);
  public void xDestroy() {}
}
