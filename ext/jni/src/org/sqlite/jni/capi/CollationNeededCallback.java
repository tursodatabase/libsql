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
   Callback for use with {@link CApi#sqlite3_collation_needed}.
*/
public interface CollationNeededCallback extends CallbackProxy {
  /**
     Has the same semantics as the C-level sqlite3_create_collation()
     callback.

     <p>Because the C API has no mechanism for reporting errors
     from this callbacks, any exceptions thrown by this callback
     are suppressed.
  */
  void call(sqlite3 db, int eTextRep, String collationName);
}
