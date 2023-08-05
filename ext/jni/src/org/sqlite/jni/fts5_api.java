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
   INCOMPLETE AND COMPLETELY UNTESTED.

   A wrapper for communicating C-level (fts5_api*) instances with
   Java. These wrappers do not own their associated pointer, they
   simply provide a type-safe way to communicate it between Java and C
   via JNI.
*/
public final class fts5_api extends NativePointerHolder<fts5_api> {
  /* Only invoked by JNI */
  private fts5_api(){}

  /**
     Returns the fts5_api instance associated with the given db, or
     null if something goes horribly wrong.
  */
  public static native fts5_api getInstanceForDb(@NotNull sqlite3 db);
}
