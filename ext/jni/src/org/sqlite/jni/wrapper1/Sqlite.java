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

  private sqlite3 thisDb(){
    if( null==db || 0==db.getNativePointer() ){
      throw new IllegalArgumentException("This database instance is closed.");
    }
    return this.db;
  }

  // private byte[] stringToUtf8(String s){
  //   return s==null ? null : s.getBytes(StandardCharsets.UTF_8);
  // }

  /**
     If rc!=0, throws an SqliteException. If this db is currently
     opened, the error state is extracted from it, else only the
     string form of rc is used.
  */
  private void affirmRcOk(int rc){
    if( 0!=rc ){
      if( null==db ) throw new SqliteException(rc);
      else throw new SqliteException(db);
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
    final int rc = sqlite3_prepare_v3(thisDb(), sql, prepFlags, out);
    affirmRcOk(rc);
    return new Stmt(this, out.take());
  }

  public Stmt prepare(String sql){
    return prepare(sql, 0);
  }

  public void createFunction(String name, int nArg, int eTextRep, ScalarFunction f ){
    int rc = CApi.sqlite3_create_function(thisDb(), name, nArg, eTextRep,
                                           new SqlFunction.ScalarAdapter(f));
    if( 0!=rc ) throw new SqliteException(db);
  }

  public void createFunction(String name, int nArg, ScalarFunction f){
    this.createFunction(name, nArg, CApi.SQLITE_UTF8, f);
  }

  public void createFunction(String name, int nArg, int eTextRep, AggregateFunction f ){
    int rc = CApi.sqlite3_create_function(thisDb(), name, nArg, eTextRep,
                                           new SqlFunction.AggregateAdapter(f));
    if( 0!=rc ) throw new SqliteException(db);
  }

  public void createFunction(String name, int nArg, AggregateFunction f){
    this.createFunction(name, nArg, CApi.SQLITE_UTF8, f);
  }

  /**
     Corresponds to the sqlite3_stmt class. Use Sqlite.prepare() to
     create new instances.
  */
  public final class Stmt implements AutoCloseable {
    private Sqlite _db = null;
    private sqlite3_stmt stmt = null;
    /**
       We save the result column count in order to prevent having to
       call into C to fetch that value every time we need to check
       that value for the columnXyz() methods.
    */
    private final int resultColCount;

    /** Only called by the prepare() factory functions. */
    Stmt(Sqlite db, sqlite3_stmt stmt){
      this._db = db;
      this.stmt = stmt;
      this.resultColCount = CApi.sqlite3_column_count(stmt);
    }

    sqlite3_stmt nativeHandle(){
      return stmt;
    }

    /**
       If this statement is still opened, its low-level handle is
       returned, eelse an IllegalArgumentException is thrown.
    */
    private sqlite3_stmt thisStmt(){
      if( null==stmt || 0==stmt.getNativePointer() ){
        throw new IllegalArgumentException("This Stmt has been finalized.");
      }
      return stmt;
    }

    /** Throws if n is out of range of this.resultColCount. Intended
        to be used by the columnXyz() methods. */
    private sqlite3_stmt checkColIndex(int n){
      if(n<0 || n>=this.resultColCount){
        throw new IllegalArgumentException("Column index "+n+" is out of range.");
      }
      return thisStmt();
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
       SQLITE_DONE, else returns rc. Error state for the exception is
       extracted from this statement object (if it's opened) or the
       string form of rc.
    */
    private int checkRc(int rc){
      switch(rc){
        case 0:
        case SQLITE_ROW:
        case SQLITE_DONE: return rc;
        default:
          if( null==stmt ) throw new SqliteException(rc);
          else throw new SqliteException(this);
      }
    }

    /**
       Works like sqlite3_step() but throws SqliteException for any
       result other than 0, SQLITE_ROW, or SQLITE_DONE.
    */
    public int step(){
      return checkRc(sqlite3_step(thisStmt()));
      /*
        Potential signature change TODO:

        boolean step()

        Returning true for SQLITE_ROW and false for anything else.
        Those semantics have proven useful in the WASM/JS bindings.
      */
    }

    public Sqlite db(){ return this._db; }

    /**
       Works like sqlite3_reset() but throws on error.
    */
    public void reset(){
      checkRc(CApi.sqlite3_reset(thisStmt()));
    }

    public void clearBindings(){
      CApi.sqlite3_clear_bindings( thisStmt() );
    }
    public void bindInt(int ndx, int val){
      checkRc(CApi.sqlite3_bind_int(thisStmt(), ndx, val));
    }
    public void bindInt64(int ndx, long val){
      checkRc(CApi.sqlite3_bind_int64(thisStmt(), ndx, val));
    }
    public void bindDouble(int ndx, double val){
      checkRc(CApi.sqlite3_bind_double(thisStmt(), ndx, val));
    }
    public void bindObject(int ndx, Object o){
      checkRc(CApi.sqlite3_bind_java_object(thisStmt(), ndx, o));
    }
    public void bindNull(int ndx){
      checkRc(CApi.sqlite3_bind_null(thisStmt(), ndx));
    }
    public int bindParameterCount(){
      return CApi.sqlite3_bind_parameter_count(thisStmt());
    }
    public int bindParameterIndex(String paramName){
      return CApi.sqlite3_bind_parameter_index(thisStmt(), paramName);
    }
    public String bindParameterName(int ndx){
      return CApi.sqlite3_bind_parameter_name(thisStmt(), ndx);
    }
    public void bindText(int ndx, byte[] utf8){
      checkRc(CApi.sqlite3_bind_text(thisStmt(), ndx, utf8));
    }
    public void bindText(int ndx, String asUtf8){
      checkRc(CApi.sqlite3_bind_text(thisStmt(), ndx, asUtf8));
    }
    public void bindText16(int ndx, byte[] utf16){
      checkRc(CApi.sqlite3_bind_text16(thisStmt(), ndx, utf16));
    }
    public void bindText16(int ndx, String txt){
      checkRc(CApi.sqlite3_bind_text16(thisStmt(), ndx, txt));
    }
    public void bindZeroBlob(int ndx, int n){
      checkRc(CApi.sqlite3_bind_zeroblob(thisStmt(), ndx, n));
    }
    public void bindBlob(int ndx, byte[] bytes){
      checkRc(CApi.sqlite3_bind_blob(thisStmt(), ndx, bytes));
    }

    public byte[] columnBlob(int ndx){
      return CApi.sqlite3_column_blob( checkColIndex(ndx), ndx );
    }
    public byte[] columnText(int ndx){
      return CApi.sqlite3_column_text( checkColIndex(ndx), ndx );
    }
    public String columnText16(int ndx){
      return CApi.sqlite3_column_text16( checkColIndex(ndx), ndx );
    }
    public int columnBytes(int ndx){
      return CApi.sqlite3_column_bytes( checkColIndex(ndx), ndx );
    }
    public int columnBytes16(int ndx){
      return CApi.sqlite3_column_bytes16( checkColIndex(ndx), ndx );
    }
    public int columnInt(int ndx){
      return CApi.sqlite3_column_int( checkColIndex(ndx), ndx );
    }
    public long columnInt64(int ndx){
      return CApi.sqlite3_column_int64( checkColIndex(ndx), ndx );
    }
    public double columnDouble(int ndx){
      return CApi.sqlite3_column_double( checkColIndex(ndx), ndx );
    }
    public int columnType(int ndx){
      return CApi.sqlite3_column_type( checkColIndex(ndx), ndx );
    }
    public String columnDeclType(int ndx){
      return CApi.sqlite3_column_decltype( checkColIndex(ndx), ndx );
    }
    public int columnCount(){
      return resultColCount;
    }
    public int columnDataCount(){
      return CApi.sqlite3_data_count( thisStmt() );
    }
    public Object columnObject(int ndx){
      return CApi.sqlite3_column_java_object( checkColIndex(ndx), ndx );
    }
    public <T> T columnObject(int ndx, Class<T> type){
      return CApi.sqlite3_column_java_object( checkColIndex(ndx), ndx, type );
    }
    public String columnName(int ndx){
      return CApi.sqlite3_column_name( checkColIndex(ndx), ndx );
    }
    public String columnDatabaseName(int ndx){
      return CApi.sqlite3_column_database_name( checkColIndex(ndx), ndx );
    }
    public String columnOriginName(int ndx){
      return CApi.sqlite3_column_origin_name( checkColIndex(ndx), ndx );
    }
    public String columnTableName(int ndx){
      return CApi.sqlite3_column_table_name( checkColIndex(ndx), ndx );
    }
  } /* Stmt class */

}
