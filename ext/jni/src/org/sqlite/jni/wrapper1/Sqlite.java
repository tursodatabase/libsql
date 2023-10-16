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
import java.nio.charset.StandardCharsets;
import static org.sqlite.jni.capi.CApi.*;
import org.sqlite.jni.capi.CApi;
import org.sqlite.jni.capi.sqlite3;
import org.sqlite.jni.capi.sqlite3_stmt;
import org.sqlite.jni.capi.OutputPointer;

/**
   This class represents a database connection, analog to the C-side
   sqlite3 class but with added argument validation, exceptions, and
   similar "smoothing of sharp edges" to make the API safe to use from
   Java. It also acts as a namespace for other types for which
   individual instances are tied to a specific database connection.
*/
public final class Sqlite implements AutoCloseable  {
  private sqlite3 db;

  //! Used only by the open() factory functions.
  private Sqlite(sqlite3 db){
    this.db = db;
  }

  /**
     Returns a newly-opened db connection or throws SqliteException if
     opening fails. All arguments are as documented for
     sqlite3_open_v2().

     Design question: do we want static factory functions or should
     this be reformulated as a constructor?
  */
  public static Sqlite open(String filename, int flags, String vfsName){
    final OutputPointer.sqlite3 out = new OutputPointer.sqlite3();
    final int rc = sqlite3_open_v2(filename, out, flags, vfsName);
    final sqlite3 n = out.take();
    if( 0!=rc ){
      if( null==n ) throw new SqliteException(rc);
      final SqliteException ex = new SqliteException(n);
      n.close();
      throw ex;
    }
    return new Sqlite(n);
  }

  public static Sqlite open(String filename, int flags){
    return open(filename, flags, null);
  }

  public static Sqlite open(String filename){
    return open(filename, SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE, null);
  }

  @Override public void close(){
    if(null!=this.db){
      this.db.close();
      this.db = null;
    }
  }

  /**
     Returns this object's underlying native db handle, or null if
     this instance has been closed. This is very specifically not
     public.
  */
  sqlite3 nativeHandle(){ return this.db; }

  private sqlite3 affirmOpen(){
    if( null==db || 0==db.getNativePointer() ){
      throw new IllegalArgumentException("This database instance is closed.");
    }
    return this.db;
  }

  // private byte[] stringToUtf8(String s){
  //   return s==null ? null : s.getBytes(StandardCharsets.UTF_8);
  // }

  private void affirmRcOk(int rc){
    if( 0!=rc ){
      throw new SqliteException(db);
    }
  }

  /**
     Corresponds to the sqlite3_stmt class. Use Sqlite.prepare() to
     create new instances.
  */
  public final class Stmt implements AutoCloseable {
    private Sqlite _db = null;
    private sqlite3_stmt stmt = null;
    /** Only called by the prepare() factory functions. */
    Stmt(Sqlite db, sqlite3_stmt stmt){
      this._db = db;
      this.stmt = stmt;
    }

    sqlite3_stmt nativeHandle(){
      return stmt;
    }

    private sqlite3_stmt affirmOpen(){
      if( null==stmt || 0==stmt.getNativePointer() ){
        throw new IllegalArgumentException("This Stmt has been finalized.");
      }
      return stmt;
    }

    /**
       Corresponds to sqlite3_finalize(), but we cannot override the
       name finalize() here because this one requires a different
       signature. It does not throw on error here because "destructors
       do not throw." If it returns non-0, the object is still
       finalized.
    */
    public int finalizeStmt(){
      int rc = 0;
      if( null!=stmt ){
        sqlite3_finalize(stmt);
        stmt = null;
      }
      return rc;
    }

    @Override public void close(){
      finalizeStmt();
    }

    /**
       Throws if rc is any value other than 0, SQLITE_ROW, or
       SQLITE_DONE, else returns rc.
    */
    private int checkRc(int rc){
      switch(rc){
        case 0:
        case SQLITE_ROW:
        case SQLITE_DONE: return rc;
        default:
          throw new SqliteException(this);
      }
    }

    /**
       Works like sqlite3_step() but throws SqliteException for any
       result other than 0, SQLITE_ROW, or SQLITE_DONE.
    */
    public int step(){
      return checkRc(sqlite3_step(affirmOpen()));
    }

    public Sqlite db(){ return this._db; }

    /**
       Works like sqlite3_reset() but throws on error.
    */
    public void reset(){
      checkRc(sqlite3_reset(affirmOpen()));
    }

    public void clearBindings(){
      sqlite3_clear_bindings( affirmOpen() );
    }
  }


  /**
     prepare() TODOs include:

     - overloads taking byte[] and ByteBuffer.

     - multi-statement processing, like CApi.sqlite3_prepare_multi()
     but using a callback specific to the higher-level Stmt class
     rather than the sqlite3_stmt class.
  */
  public Stmt prepare(String sql, int prepFlags){
    final OutputPointer.sqlite3_stmt out = new OutputPointer.sqlite3_stmt();
    final int rc = sqlite3_prepare_v3(affirmOpen(), sql, prepFlags, out);
    affirmRcOk(rc);
    return new Stmt(this, out.take());
  }

  public Stmt prepare(String sql){
    return prepare(sql, 0);
  }

  public void createFunction(String name, int nArg, int eTextRep, ScalarFunction f ){
    int rc = CApi.sqlite3_create_function(affirmOpen(), name, nArg, eTextRep,
                                           new SqlFunction.ScalarAdapter(f));
    if( 0!=rc ) throw new SqliteException(db);
  }

  public void createFunction(String name, int nArg, ScalarFunction f){
    this.createFunction(name, nArg, CApi.SQLITE_UTF8, f);
  }

  public void createFunction(String name, int nArg, int eTextRep, AggregateFunction f ){
    int rc = CApi.sqlite3_create_function(affirmOpen(), name, nArg, eTextRep,
                                           new SqlFunction.AggregateAdapter(f));
    if( 0!=rc ) throw new SqliteException(db);
  }

  public void createFunction(String name, int nArg, AggregateFunction f){
    this.createFunction(name, nArg, CApi.SQLITE_UTF8, f);
  }

}
