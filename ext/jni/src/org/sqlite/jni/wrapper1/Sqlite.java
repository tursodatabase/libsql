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

  public static final int OPEN_READWRITE = CApi.SQLITE_OPEN_READWRITE;
  public static final int OPEN_CREATE = CApi.SQLITE_OPEN_CREATE;
  public static final int OPEN_EXRESCODE = CApi.SQLITE_OPEN_EXRESCODE;
  public static final int TXN_NONE = CApi.SQLITE_TXN_NONE;
  public static final int TXN_READ = CApi.SQLITE_TXN_READ;
  public static final int TXN_WRITE = CApi.SQLITE_TXN_WRITE;

  public static final int STATUS_MEMORY_USED = CApi.SQLITE_STATUS_MEMORY_USED;
  public static final int STATUS_PAGECACHE_USED = CApi.SQLITE_STATUS_PAGECACHE_USED;
  public static final int STATUS_PAGECACHE_OVERFLOW = CApi.SQLITE_STATUS_PAGECACHE_OVERFLOW;
  public static final int STATUS_MALLOC_SIZE = CApi.SQLITE_STATUS_MALLOC_SIZE;
  public static final int STATUS_PARSER_STACK = CApi.SQLITE_STATUS_PARSER_STACK;
  public static final int STATUS_PAGECACHE_SIZE = CApi.SQLITE_STATUS_PAGECACHE_SIZE;
  public static final int STATUS_MALLOC_COUNT = CApi.SQLITE_STATUS_MALLOC_COUNT;

  public static final int LIMIT_LENGTH = CApi.SQLITE_LIMIT_LENGTH;
  public static final int LIMIT_SQL_LENGTH = CApi.SQLITE_LIMIT_SQL_LENGTH;
  public static final int LIMIT_COLUMN = CApi.SQLITE_LIMIT_COLUMN;
  public static final int LIMIT_EXPR_DEPTH = CApi.SQLITE_LIMIT_EXPR_DEPTH;
  public static final int LIMIT_COMPOUND_SELECT = CApi.SQLITE_LIMIT_COMPOUND_SELECT;
  public static final int LIMIT_VDBE_OP = CApi.SQLITE_LIMIT_VDBE_OP;
  public static final int LIMIT_FUNCTION_ARG = CApi.SQLITE_LIMIT_FUNCTION_ARG;
  public static final int LIMIT_ATTACHED = CApi.SQLITE_LIMIT_ATTACHED;
  public static final int LIMIT_LIKE_PATTERN_LENGTH = CApi.SQLITE_LIMIT_LIKE_PATTERN_LENGTH;
  public static final int LIMIT_VARIABLE_NUMBER = CApi.SQLITE_LIMIT_VARIABLE_NUMBER;
  public static final int LIMIT_TRIGGER_DEPTH = CApi.SQLITE_LIMIT_TRIGGER_DEPTH;
  public static final int LIMIT_WORKER_THREADS = CApi.SQLITE_LIMIT_WORKER_THREADS;

  public static final int PREPARE_PERSISTENT = CApi.SQLITE_PREPARE_PERSISTENT;
  public static final int PREPARE_NORMALIZE = CApi.SQLITE_PREPARE_NORMALIZE;
  public static final int PREPARE_NO_VTAB = CApi.SQLITE_PREPARE_NO_VTAB;

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

  public static String libVersion(){
    return CApi.sqlite3_libversion();
  }

  public static int libVersionNumber(){
    return CApi.sqlite3_libversion_number();
  }

  public static String libSourceId(){
    return CApi.sqlite3_sourceid();
  }

  /**
     As per sqlite3_status64(), but returns its current and high-water
     results as a two-element array. Throws if the first argument is
     not one of the STATUS_... constants.
  */
  public long[] libStatus(int op, boolean resetStats){
    org.sqlite.jni.capi.OutputPointer.Int64 pCurrent =
      new org.sqlite.jni.capi.OutputPointer.Int64();
    org.sqlite.jni.capi.OutputPointer.Int64 pHighwater =
      new org.sqlite.jni.capi.OutputPointer.Int64();
    final int rc = CApi.sqlite3_status64(op, pCurrent, pHighwater, resetStats);
    checkRc(rc);
    return new long[] {pCurrent.value, pHighwater.value};
  }

  @Override public void close(){
    if(null!=this.db){
      this.db.close();
      this.db = null;
    }
  }

  /**
     Returns the value of the native library's build-time value of the
     SQLITE_THREADSAFE build option.
  */
  public static int libThreadsafe(){
    return CApi.sqlite3_threadsafe();
  }

  /**
     Analog to sqlite3_compileoption_get().
  */
  public static String compileOptionGet(int n){
    return CApi.sqlite3_compileoption_get(n);
  }

  /**
     Analog to sqlite3_compileoption_used().
  */
  public static boolean compileOptionUsed(String optName){
    return CApi.sqlite3_compileoption_used(optName);
  }

  /**
     Analog to sqlite3_complete().
  */
  public static boolean isCompleteStatement(String sql){
    switch(CApi.sqlite3_complete(sql)){
      case 0: return false;
      case CApi.SQLITE_MISUSE:
        throw new IllegalArgumentException("Input may not be null.");
      case CApi.SQLITE_NOMEM:
        throw new OutOfMemoryError();
      default:
        return true;
    }
  }

  public static int keywordCount(){
    return CApi.sqlite3_keyword_count();
  }

  public static boolean keywordCheck(String word){
    return CApi.sqlite3_keyword_check(word);
  }

  public static String keywordName(int index){
    return CApi.sqlite3_keyword_name(index);
  }

  public static boolean strglob(String glob, String txt){
    return 0==CApi.sqlite3_strglob(glob, txt);
  }

  public static boolean strlike(String glob, String txt, char escChar){
    return 0==CApi.sqlite3_strlike(glob, txt, escChar);
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
     opened and has non-0 sqlite3_errcode(), the error state is
     extracted from it, else only the string form of rc is used. It is
     the caller's responsibility to filter out non-error codes such as
     SQLITE_ROW and SQLITE_DONE before calling this.

     As a special case, if rc is SQLITE_NOMEM, an OutOfMemoryError is
     thrown.
  */
  private void checkRc(int rc){
    if( 0!=rc ){
      if( CApi.SQLITE_NOMEM==rc ){
        throw new OutOfMemoryError();
      }else if( null==db || 0==sqlite3_errcode(db)){
        throw new SqliteException(rc);
      }else{
        throw new SqliteException(db);
      }
    }
  }

  /**
     prepFlags must be 0 or a bitmask of the PREPARE_... constants.

     prepare() TODOs include:

     - overloads taking byte[] and ByteBuffer.

     - multi-statement processing, like CApi.sqlite3_prepare_multi()
     but using a callback specific to the higher-level Stmt class
     rather than the sqlite3_stmt class.
  */
  public Stmt prepare(String sql, int prepFlags){
    final OutputPointer.sqlite3_stmt out = new OutputPointer.sqlite3_stmt();
    final int rc = sqlite3_prepare_v3(thisDb(), sql, prepFlags, out);
    checkRc(rc);
    final sqlite3_stmt q = out.take();
    if( null==q ){
      /* The C-level API treats input which is devoid of SQL
         statements (e.g. all comments or an empty string) as success
         but returns a NULL sqlite3_stmt object. In higher-level APIs,
         wrapping a "successful NULL" object that way is tedious to
         use because it forces clients and/or wrapper-level code to
         check for that unusual case. In practice, higher-level
         bindings are generally better-served by treating empty SQL
         input as an error. */
      throw new IllegalArgumentException("Input contains no SQL statements.");
    }
    return new Stmt(this, q);
  }

  public Stmt prepare(String sql){
    return prepare(sql, 0);
  }

  public void createFunction(String name, int nArg, int eTextRep, ScalarFunction f){
    int rc = CApi.sqlite3_create_function(thisDb(), name, nArg, eTextRep,
                                           new SqlFunction.ScalarAdapter(f));
    if( 0!=rc ) throw new SqliteException(db);
  }

  public void createFunction(String name, int nArg, ScalarFunction f){
    this.createFunction(name, nArg, CApi.SQLITE_UTF8, f);
  }

  public void createFunction(String name, int nArg, int eTextRep, AggregateFunction f){
    int rc = CApi.sqlite3_create_function(thisDb(), name, nArg, eTextRep,
                                           new SqlFunction.AggregateAdapter(f));
    if( 0!=rc ) throw new SqliteException(db);
  }

  public void createFunction(String name, int nArg, AggregateFunction f){
    this.createFunction(name, nArg, CApi.SQLITE_UTF8, f);
  }

  public void createFunction(String name, int nArg, int eTextRep, WindowFunction f){
    int rc = CApi.sqlite3_create_function(thisDb(), name, nArg, eTextRep,
                                          new SqlFunction.WindowAdapter(f));
    if( 0!=rc ) throw new SqliteException(db);
  }

  public void createFunction(String name, int nArg, WindowFunction f){
    this.createFunction(name, nArg, CApi.SQLITE_UTF8, f);
  }

  public long changes(){
    return CApi.sqlite3_changes64(thisDb());
  }

  public long totalChanges(){
    return CApi.sqlite3_total_changes64(thisDb());
  }

  public long lastInsertRowId(){
    return CApi.sqlite3_last_insert_rowid(thisDb());
  }

  public void setLastInsertRowId(long rowId){
    CApi.sqlite3_set_last_insert_rowid(thisDb(), rowId);
  }

  public void interrupt(){
    CApi.sqlite3_interrupt(thisDb());
  }

  public boolean isInterrupted(){
    return CApi.sqlite3_is_interrupted(thisDb());
  }

  public boolean isAutoCommit(){
    return CApi.sqlite3_get_autocommit(thisDb());
  }

  public void setBusyTimeout(int ms){
    checkRc(CApi.sqlite3_busy_timeout(thisDb(), ms));
  }

  /**
     Analog to sqlite3_txn_state(). Returns one of TXN_NONE, TXN_READ,
     or TXN_WRITE to denote this database's current transaction state
     for the given schema name (or the most restrictive state of any
     schema if zSchema is null).
  */
  public int transactionState(String zSchema){
    return CApi.sqlite3_txn_state(thisDb(), zSchema);
  }

  /**
     Analog to sqlite3_db_name(). Returns null if passed an unknown
     index.
  */
  public String dbName(int dbNdx){
    return CApi.sqlite3_db_name(thisDb(), dbNdx);
  }

  /**
     Analog to sqlite3_db_filename(). Returns null if passed an
     unknown db name.
  */
  public String dbFileName(String dbName){
    return CApi.sqlite3_db_filename(thisDb(), dbName);
  }

  /**
     Analog to the variant of sqlite3_db_config() for configuring the
     SQLITE_DBCONFIG_MAINDBNAME option. Throws on error.
  */
  public void setMainDbName(String name){
    checkRc(
      CApi.sqlite3_db_config(thisDb(), CApi.SQLITE_DBCONFIG_MAINDBNAME,
                             name)
    );
  }

  /**
     Analog to sqlite3_db_readonly() but throws an SqliteException
     with result code SQLITE_NOTFOUND if given an unknown database
     name.
  */
  public boolean readOnly(String dbName){
    final int rc = CApi.sqlite3_db_readonly(thisDb(), dbName);
    if( 0==rc ) return false;
    else if( rc>0 ) return true;
    throw new SqliteException(CApi.SQLITE_NOTFOUND);
  }

  /**
     Analog to sqlite3_db_release_memory().
  */
  public void releaseMemory(){
    CApi.sqlite3_db_release_memory(thisDb());
  }

  /**
     Analog to sqlite3_release_memory().
  */
  public static int releaseMemory(int n){
    return CApi.sqlite3_release_memory(n);
  }

  /**
     Analog to sqlite3_limit(). limitId must be one of the
     LIMIT_... constants.

     Returns the old limit for the given option. If newLimit is
     negative, it returns the old limit without modifying the limit.

     If sqlite3_limit() returns a negative value, this function throws
     an SqliteException with the SQLITE_RANGE result code but no
     further error info (because that case does not qualify as a
     db-level error). Such errors may indicate an invalid argument
     value or an invalid range for newLimit (the underlying function
     does not differentiate between those).
  */
  public int limit(int limitId, int newLimit){
    final int rc = CApi.sqlite3_limit(thisDb(), limitId, newLimit);
    if( rc<0 ){
      throw new SqliteException(CApi.SQLITE_RANGE);
    }
    return rc;
  }

  /**
     Analog to sqlite3_errstr().
  */
  static String errstr(int resultCode){
    return CApi.sqlite3_errstr(resultCode);
  }

  /**
     A wrapper object for use with tableColumnMetadata().  They are
     created and populated only via that interface.
  */
  public final class TableColumnMetadata {
    Boolean pNotNull = null;
    Boolean pPrimaryKey = null;
    Boolean pAutoinc = null;
    String pzCollSeq = null;
    String pzDataType = null;

    private TableColumnMetadata(){}

    public String getDataType(){ return pzDataType; }
    public String getCollation(){ return pzCollSeq; }
    public boolean isNotNull(){ return pNotNull; }
    public boolean isPrimaryKey(){ return pPrimaryKey; }
    public boolean isAutoincrement(){ return pAutoinc; }
  }

  /**
     Returns data about a database, table, and (optionally) column
     (which may be null), as per sqlite3_table_column_metadata().
     Throws if passed invalid arguments, else returns the result as a
     new TableColumnMetadata object.
  */
  TableColumnMetadata tableColumnMetadata(
    String zDbName, String zTableName, String zColumnName
  ){
    org.sqlite.jni.capi.OutputPointer.String pzDataType
      = new org.sqlite.jni.capi.OutputPointer.String();
    org.sqlite.jni.capi.OutputPointer.String pzCollSeq
      = new org.sqlite.jni.capi.OutputPointer.String();
    org.sqlite.jni.capi.OutputPointer.Bool pNotNull
      = new org.sqlite.jni.capi.OutputPointer.Bool();
    org.sqlite.jni.capi.OutputPointer.Bool pPrimaryKey
      = new org.sqlite.jni.capi.OutputPointer.Bool();
    org.sqlite.jni.capi.OutputPointer.Bool pAutoinc
      = new org.sqlite.jni.capi.OutputPointer.Bool();
    final int rc = CApi.sqlite3_table_column_metadata(
      thisDb(), zDbName, zTableName, zColumnName,
      pzDataType, pzCollSeq, pNotNull, pPrimaryKey, pAutoinc
    );
    checkRc(rc);
    TableColumnMetadata rv = new TableColumnMetadata();
    rv.pzDataType = pzDataType.value;
    rv.pzCollSeq = pzCollSeq.value;
    rv.pNotNull = pNotNull.value;
    rv.pPrimaryKey = pPrimaryKey.value;
    rv.pAutoinc = pAutoinc.value;
    return rv;
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

       Design note: if this is final then we cannot zero it in
       finalizeStmt().
    */
    private int resultColCount;

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
       finalized, but the result code is an indication that something
       went wrong in a prior call into the statement's API, as
       documented for sqlite3_finalize().
    */
    public int finalizeStmt(){
      int rc = 0;
      if( null!=stmt ){
        sqlite3_finalize(stmt);
        stmt = null;
        _db = null;
        resultColCount = 0;
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
       Works like sqlite3_step() but returns true for SQLITE_ROW,
       false for SQLITE_DONE, and throws SqliteException for any other
       result.
    */
    public boolean step(){
      switch(checkRc(sqlite3_step(thisStmt()))){
        case CApi.SQLITE_ROW: return true;
        case CApi.SQLITE_DONE: return false;
        default:
          throw new IllegalStateException(
            "This \"cannot happen\": all possible result codes were checked already."
          );
      }
      /*
        Potential signature change TODO:

        boolean step()

        Returning true for SQLITE_ROW and false for anything else.
        Those semantics have proven useful in the WASM/JS bindings.
      */
    }

    /**
       Returns the Sqlite which prepared this statement, or null if
       this statement has been finalized.
    */
    public Sqlite db(){ return this._db; }

    /**
       Works like sqlite3_reset() but throws on error.
    */
    public void reset(){
      checkRc(CApi.sqlite3_reset(thisStmt()));
    }

    public boolean isBusy(){
      return CApi.sqlite3_stmt_busy(thisStmt());
    }

    public boolean isReadOnly(){
      return CApi.sqlite3_stmt_readonly(thisStmt());
    }

    public String sql(){
      return CApi.sqlite3_sql(thisStmt());
    }

    public String expandedSql(){
      return CApi.sqlite3_expanded_sql(thisStmt());
    }

    /**
       Analog to sqlite3_stmt_explain() but throws if op is invalid.
    */
    public void explain(int op){
      checkRc(CApi.sqlite3_stmt_explain(thisStmt(), op));
    }

    /**
       Analog to sqlite3_stmt_isexplain().
    */
    public int isExplain(){
      return CApi.sqlite3_stmt_isexplain(thisStmt());
    }

    /**
       Analog to sqlite3_normalized_sql(), returning null if the
       library is built without the SQLITE_ENABLE_NORMALIZE flag.
    */
    public String normalizedSql(){
      return CApi.sqlite3_normalized_sql(thisStmt());
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
    public void bindText16(int ndx, String asUtf16){
      checkRc(CApi.sqlite3_bind_text16(thisStmt(), ndx, asUtf16));
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
