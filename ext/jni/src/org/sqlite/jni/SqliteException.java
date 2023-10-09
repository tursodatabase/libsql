/*
** 2023-10-09
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
import static org.sqlite.jni.CApi.*;

/**
   A wrapper for communicating C-level (sqlite3*) instances with
   Java. These wrappers do not own their associated pointer, they
   simply provide a type-safe way to communicate it between Java
   and C via JNI.
*/
public final class SqliteException extends java.lang.RuntimeException {

  public SqliteException(String msg){
    super(msg);
  }

  public SqliteException(int sqlite3ResultCode){
    super(sqlite3_errstr(sqlite3ResultCode));
  }

  public SqliteException(sqlite3 db){
    super(sqlite3_errmsg(db));
    db.close();
  }

  public SqliteException(Sqlite db){
    this(db.dbHandle());
  }

}
