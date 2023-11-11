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
** This file is part of the wrapper1 interface for sqlite3.
*/
package org.sqlite.jni.wrapper1;
import org.sqlite.jni.capi.CApi;
import org.sqlite.jni.capi.sqlite3;

/**
   A wrapper for communicating C-level (sqlite3*) instances with
   Java. These wrappers do not own their associated pointer, they
   simply provide a type-safe way to communicate it between Java
   and C via JNI.
*/
public final class SqliteException extends java.lang.RuntimeException {
  private int errCode = CApi.SQLITE_ERROR;
  private int xerrCode = CApi.SQLITE_ERROR;
  private int errOffset = -1;
  private int sysErrno = 0;

  /**
     Records the given error string and uses SQLITE_ERROR for both the
     error code and extended error code.
  */
  public SqliteException(String msg){
    super(msg);
  }

  /**
     Uses sqlite3_errstr(sqlite3ResultCode) for the error string and
     sets both the error code and extended error code to the given
     value. This approach includes no database-level information and
     systemErrno() will be 0, so is intended only for use with sqlite3
     APIs for which a result code is not an error but which the
     higher-level wrapper should treat as one.
  */
  public SqliteException(int sqlite3ResultCode){
    super(CApi.sqlite3_errstr(sqlite3ResultCode));
    errCode = xerrCode = sqlite3ResultCode;
  }

  /**
     Records the current error state of db (which must not be null and
     must refer to an opened db object). Note that this does not close
     the db.

     Design note: closing the db on error is really only useful during
     a failed db-open operation, and the place(s) where that can
     happen are inside this library, not client-level code.
  */
  SqliteException(sqlite3 db){
    super(CApi.sqlite3_errmsg(db));
    errCode = CApi.sqlite3_errcode(db);
    xerrCode = CApi.sqlite3_extended_errcode(db);
    errOffset = CApi.sqlite3_error_offset(db);
    sysErrno = CApi.sqlite3_system_errno(db);
  }

  /**
     Records the current error state of db (which must not be null and
     must refer to an open database).
  */
  public SqliteException(Sqlite db){
    this(db.nativeHandle());
  }

  public SqliteException(Sqlite.Stmt stmt){
    this(stmt.getDb());
  }

  public int errcode(){ return errCode; }
  public int extendedErrcode(){ return xerrCode; }
  public int errorOffset(){ return errOffset; }
  public int systemErrno(){ return sysErrno; }

}
