/*
** 2023-07-30
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
   Callback proxy for use with sqlite3_collation_needed().
*/
public interface CollationNeeded {
  /**
     Has the same semantics as the C-level sqlite3_create_collation()
     callback.  Must not throw.

     Pedantic note: the first argument to this function will always be
     the same object reference which was passed to sqlite3_open() or
     sqlite3_open_v2(), even if the client has managed to create other
     Java-side references to the same C-level object.
  */
  int xCollationNeeded(sqlite3 db, int eTextRep, String collationName);
}
