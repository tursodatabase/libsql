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
     Works as documented for the sqlite3_create_collation() callback.
     Must not throw.

     Achtung: the first argument to this function is not guaranteed to
     be the same object upon which ealier DB operations have been
     performed, e.g. not the one passed to sqlite3_collation_needed(),
     but it will refer to the same underlying C-level database
     pointer. This quirk is a side effect of how per-db state is
     managed in the JNI layer.
  */
  int xCollationNeeded(sqlite3 db, int eTextRep, String collationName);
}
