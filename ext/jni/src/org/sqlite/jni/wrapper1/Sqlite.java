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
import org.sqlite.jni.capi.CApi;
import org.sqlite.jni.capi.sqlite3;
import org.sqlite.jni.capi.sqlite3_stmt;
import org.sqlite.jni.capi.sqlite3_backup;
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

  public static final int DBSTATUS_LOOKASIDE_USED = CApi.SQLITE_DBSTATUS_LOOKASIDE_USED;
  public static final int DBSTATUS_CACHE_USED = CApi.SQLITE_DBSTATUS_CACHE_USED;
  public static final int DBSTATUS_SCHEMA_USED = CApi.SQLITE_DBSTATUS_SCHEMA_USED;
  public static final int DBSTATUS_STMT_USED = CApi.SQLITE_DBSTATUS_STMT_USED;
  public static final int DBSTATUS_LOOKASIDE_HIT = CApi.SQLITE_DBSTATUS_LOOKASIDE_HIT;
  public static final int DBSTATUS_LOOKASIDE_MISS_SIZE = CApi.SQLITE_DBSTATUS_LOOKASIDE_MISS_SIZE;
  public static final int DBSTATUS_LOOKASIDE_MISS_FULL = CApi.SQLITE_DBSTATUS_LOOKASIDE_MISS_FULL;
  public static final int DBSTATUS_CACHE_HIT = CApi.SQLITE_DBSTATUS_CACHE_HIT;
  public static final int DBSTATUS_CACHE_MISS = CApi.SQLITE_DBSTATUS_CACHE_MISS;
  public static final int DBSTATUS_CACHE_WRITE = CApi.SQLITE_DBSTATUS_CACHE_WRITE;
  public static final int DBSTATUS_DEFERRED_FKS = CApi.SQLITE_DBSTATUS_DEFERRED_FKS;
  public static final int DBSTATUS_CACHE_USED_SHARED = CApi.SQLITE_DBSTATUS_CACHE_USED_SHARED;
  public static final int DBSTATUS_CACHE_SPILL = CApi.SQLITE_DBSTATUS_CACHE_SPILL;

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

  public static final int TRACE_STMT = CApi.SQLITE_TRACE_STMT;
  public static final int TRACE_PROFILE = CApi.SQLITE_TRACE_PROFILE;
  public static final int TRACE_ROW = CApi.SQLITE_TRACE_ROW;
  public static final int TRACE_CLOSE = CApi.SQLITE_TRACE_CLOSE;
  public static final int TRACE_ALL = TRACE_STMT | TRACE_PROFILE | TRACE_ROW | TRACE_CLOSE;

  public static final int DBCONFIG_ENABLE_FKEY = CApi.SQLITE_DBCONFIG_ENABLE_FKEY;
  public static final int DBCONFIG_ENABLE_TRIGGER = CApi.SQLITE_DBCONFIG_ENABLE_TRIGGER;
  public static final int DBCONFIG_ENABLE_FTS3_TOKENIZER = CApi.SQLITE_DBCONFIG_ENABLE_FTS3_TOKENIZER;
  public static final int DBCONFIG_ENABLE_LOAD_EXTENSION = CApi.SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION;
  public static final int DBCONFIG_NO_CKPT_ON_CLOSE = CApi.SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE;
  public static final int DBCONFIG_ENABLE_QPSG = CApi.SQLITE_DBCONFIG_ENABLE_QPSG;
  public static final int DBCONFIG_TRIGGER_EQP = CApi.SQLITE_DBCONFIG_TRIGGER_EQP;
  public static final int DBCONFIG_RESET_DATABASE = CApi.SQLITE_DBCONFIG_RESET_DATABASE;
  public static final int DBCONFIG_DEFENSIVE = CApi.SQLITE_DBCONFIG_DEFENSIVE;
  public static final int DBCONFIG_WRITABLE_SCHEMA = CApi.SQLITE_DBCONFIG_WRITABLE_SCHEMA;
  public static final int DBCONFIG_LEGACY_ALTER_TABLE = CApi.SQLITE_DBCONFIG_LEGACY_ALTER_TABLE;
  public static final int DBCONFIG_DQS_DML = CApi.SQLITE_DBCONFIG_DQS_DML;
  public static final int DBCONFIG_DQS_DDL = CApi.SQLITE_DBCONFIG_DQS_DDL;
  public static final int DBCONFIG_ENABLE_VIEW = CApi.SQLITE_DBCONFIG_ENABLE_VIEW;
  public static final int DBCONFIG_LEGACY_FILE_FORMAT = CApi.SQLITE_DBCONFIG_LEGACY_FILE_FORMAT;
  public static final int DBCONFIG_TRUSTED_SCHEMA = CApi.SQLITE_DBCONFIG_TRUSTED_SCHEMA;
  public static final int DBCONFIG_STMT_SCANSTATUS = CApi.SQLITE_DBCONFIG_STMT_SCANSTATUS;
  public static final int DBCONFIG_REVERSE_SCANORDER = CApi.SQLITE_DBCONFIG_REVERSE_SCANORDER;

  public static final int UTF8 = CApi.SQLITE_UTF8;
  public static final int UTF16 = CApi.SQLITE_UTF16;
  public static final int UTF16LE = CApi.SQLITE_UTF16LE;
  public static final int UTF16BE = CApi.SQLITE_UTF16BE;
  /* We elide the UTF16_ALIGNED from this interface because it
     is irrelevant for the Java interface. */

  public static final int DONE = CApi.SQLITE_DONE;
  public static final int BUSY = CApi.SQLITE_BUSY;
  public static final int LOCKED = CApi.SQLITE_LOCKED;

  //! Used only by the open() factory functions.
  private Sqlite(sqlite3 db){
    this.db = db;
  }

  /** Maps org.sqlite.jni.capi.sqlite3 to Sqlite instances. */
  private static final java.util.Map<org.sqlite.jni.capi.sqlite3, Sqlite> nativeToWrapper
    = new java.util.HashMap<>();

  /**
     Returns the Sqlite object associated with the given sqlite3
     object, or null if there is no such mapping.
  */
  static Sqlite fromNative(sqlite3 low){
    synchronized(nativeToWrapper){
      return nativeToWrapper.get(low);
    }
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
    final int rc = CApi.sqlite3_open_v2(filename, out, flags, vfsName);
    final sqlite3 n = out.take();
    if( 0!=rc ){
      if( null==n ) throw new SqliteException(rc);
      final SqliteException ex = new SqliteException(n);
      n.close();
      throw ex;
    }
    final Sqlite rv = new Sqlite(n);
    synchronized(nativeToWrapper){
      nativeToWrapper.put(n, rv);
    }
    runAutoExtensions(rv);
    return rv;
  }

  public static Sqlite open(String filename, int flags){
    return open(filename, flags, null);
  }

  public static Sqlite open(String filename){
    return open(filename, OPEN_READWRITE|OPEN_CREATE, null);
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
     Output object for use with status() and libStatus().
  */
  public static final class Status {
    /** The current value for the requested status() or libStatus() metric. */
    long current;
    /** The peak value for the requested status() or libStatus() metric. */
    long peak;
  };

  /**
     As per sqlite3_status64(), but returns its current and high-water
     results as a Status object. Throws if the first argument is
     not one of the STATUS_... constants.
  */
  public static Status libStatus(int op, boolean resetStats){
    org.sqlite.jni.capi.OutputPointer.Int64 pCurrent =
      new org.sqlite.jni.capi.OutputPointer.Int64();
    org.sqlite.jni.capi.OutputPointer.Int64 pHighwater =
      new org.sqlite.jni.capi.OutputPointer.Int64();
    checkRc2( CApi.sqlite3_status64(op, pCurrent, pHighwater, resetStats) );
    final Status s = new Status();
    s.current = pCurrent.value;
    s.peak = pHighwater.value;
    return s;
  }

  /**
     As per sqlite3_status64(), but returns its current and high-water
     results as a Status object. Throws if the first argument is
     not one of the DBSTATUS_... constants or on any other misuse.
  */
  public Status status(int op, boolean resetStats){
    org.sqlite.jni.capi.OutputPointer.Int32 pCurrent =
      new org.sqlite.jni.capi.OutputPointer.Int32();
    org.sqlite.jni.capi.OutputPointer.Int32 pHighwater =
      new org.sqlite.jni.capi.OutputPointer.Int32();
    checkRc( CApi.sqlite3_db_status(thisDb(), op, pCurrent, pHighwater, resetStats) );
    final Status s = new Status();
    s.current = pCurrent.value;
    s.peak = pHighwater.value;
    return s;
  }

  @Override public void close(){
    if(null!=this.db){
      synchronized(nativeToWrapper){
        nativeToWrapper.remove(this.db);
      }
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
      }else if( null==db || 0==CApi.sqlite3_errcode(db)){
        throw new SqliteException(rc);
      }else{
        throw new SqliteException(db);
      }
    }
  }

  /**
     Like checkRc() but behaves as if that function were
     called with a null db object.
  */
  private static void checkRc2(int rc){
    if( 0!=rc ){
      if( CApi.SQLITE_NOMEM==rc ){
        throw new OutOfMemoryError();
      }else{
        throw new SqliteException(rc);
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
    final int rc = CApi.sqlite3_prepare_v3(thisDb(), sql, prepFlags, out);
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
     Analog to sqlite3_db_config() for the call forms which take one
     of the boolean-type db configuration flags (namely the
     DBCONFIG_... constants defined in this class). On success it
     returns the result of that underlying call. Throws on error.
  */
  public boolean dbConfig(int op, boolean on){
    org.sqlite.jni.capi.OutputPointer.Int32 pOut =
      new org.sqlite.jni.capi.OutputPointer.Int32();
    checkRc( CApi.sqlite3_db_config(thisDb(), op, on ? 1 : 0, pOut) );
    return pOut.get()!=0;
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
  public static int libReleaseMemory(int n){
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

  public interface TraceCallback {
    /**
       Called by sqlite3 for various tracing operations, as per
       sqlite3_trace_v2(). Note that this interface elides the 2nd
       argument to the native trace callback, as that role is better
       filled by instance-local state.

       <p>These callbacks may throw, in which case their exceptions are
       converted to C-level error information.

       <p>The 2nd argument to this function, if non-null, will be a an
       Sqlite or Sqlite.Stmt object, depending on the first argument
       (see below).

       <p>The final argument to this function is the "X" argument
       documented for sqlite3_trace() and sqlite3_trace_v2(). Its type
       depends on value of the first argument:

       <p>- SQLITE_TRACE_STMT: pNative is a Sqlite.Stmt. pX is a String
       containing the prepared SQL.

       <p>- SQLITE_TRACE_PROFILE: pNative is a sqlite3_stmt. pX is a Long
       holding an approximate number of nanoseconds the statement took
       to run.

       <p>- SQLITE_TRACE_ROW: pNative is a sqlite3_stmt. pX is null.

       <p>- SQLITE_TRACE_CLOSE: pNative is a sqlite3. pX is null.
    */
    void call(int traceFlag, Object pNative, Object pX);
  }

  /**
     Analog to sqlite3_trace_v2(). traceMask must be a mask of the
     TRACE_...  constants. Pass a null callback to remove tracing.

     Throws on error.
  */
  public void trace(int traceMask, TraceCallback callback){
    final Sqlite self = this;
    final org.sqlite.jni.capi.TraceV2Callback tc =
      (null==callback) ? null : new org.sqlite.jni.capi.TraceV2Callback(){
          @SuppressWarnings("unchecked")
          @Override public int call(int flag, Object pNative, Object pX){
            switch(flag){
              case TRACE_ROW:
              case TRACE_PROFILE:
              case TRACE_STMT:
                callback.call(flag, Sqlite.Stmt.fromNative((sqlite3_stmt)pNative), pX);
                break;
              case TRACE_CLOSE:
                callback.call(flag, self, pX);
                break;
            }
            return 0;
          }
        };
    checkRc( CApi.sqlite3_trace_v2(thisDb(), traceMask, tc) );
  };

  /**
     Corresponds to the sqlite3_stmt class. Use Sqlite.prepare() to
     create new instances.
  */
  public static final class Stmt implements AutoCloseable {
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
      synchronized(nativeToWrapper){
        nativeToWrapper.put(this.stmt, this);
      }
    }

    sqlite3_stmt nativeHandle(){
      return stmt;
    }

    /** Maps org.sqlite.jni.capi.sqlite3_stmt to Stmt instances. */
    private static final java.util.Map<org.sqlite.jni.capi.sqlite3_stmt, Stmt> nativeToWrapper
      = new java.util.HashMap<>();

    /**
       Returns the Stmt object associated with the given sqlite3_stmt
       object, or null if there is no such mapping.
    */
    static Stmt fromNative(sqlite3_stmt low){
      synchronized(nativeToWrapper){
        return nativeToWrapper.get(low);
      }
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
        synchronized(nativeToWrapper){
          nativeToWrapper.remove(this.stmt);
        }
        CApi.sqlite3_finalize(stmt);
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
        case CApi.SQLITE_ROW:
        case CApi.SQLITE_DONE: return rc;
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
      switch(checkRc(CApi.sqlite3_step(thisStmt()))){
        case CApi.SQLITE_ROW: return true;
        case CApi.SQLITE_DONE: return false;
        default:
          throw new IllegalStateException(
            "This \"cannot happen\": all possible result codes were checked already."
          );
      }
    }

    /**
       Returns the Sqlite which prepared this statement, or null if
       this statement has been finalized.
    */
    public Sqlite getDb(){ return this._db; }

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

  /**
     Interface for auto-extensions, as per the
     sqlite3_auto_extension() API.

     Design note: the chicken/egg timing of auto-extension execution
     requires that this feature be entirely re-implemented in Java
     because the C-level API has no access to the Sqlite type so
     cannot pass on an object of that type while the database is being
     opened.  One side effect of this reimplementation is that this
     class's list of auto-extensions is 100% independent of the
     C-level list so, e.g., clearAutoExtensions() will have no effect
     on auto-extensions added via the C-level API and databases opened
     from that level of API will not be passed to this level's
     AutoExtension instances.
  */
  public interface AutoExtension {
    public void call(Sqlite db);
  }

  private static final java.util.Set<AutoExtension> autoExtensions =
    new java.util.LinkedHashSet<>();

  /**
     Passes db to all auto-extensions. If any one of them throws,
     db.close() is called before the exception is propagated.
  */
  private static void runAutoExtensions(Sqlite db){
    AutoExtension list[];
    synchronized(autoExtensions){
      /* Avoid that modifications to the AutoExtension list from within
         auto-extensions affect this execution of this list. */
      list = autoExtensions.toArray(new AutoExtension[0]);
    }
    try {
      for( AutoExtension ax : list ) ax.call(db);
    }catch(Exception e){
      db.close();
      throw e;
    }
  }

  /**
     Analog to sqlite3_auto_extension(), adds the given object to the
     list of auto-extensions if it is not already in that list. The
     given object will be run as part of Sqlite.open(), and passed the
     being-opened database. If the extension throws then open() will
     fail.

     This API does not guaranty whether or not manipulations made to
     the auto-extension list from within auto-extension callbacks will
     affect the current traversal of the auto-extension list.  Whether
     or not they do is unspecified and subject to change between
     versions. e.g. if an AutoExtension calls addAutoExtension(),
     whether or not the new extension will be run on the being-opened
     database is undefined.

     Note that calling Sqlite.open() from an auto-extension will
     necessarily result in recursion loop and (eventually) a stack
     overflow.
  */
  public static void addAutoExtension( AutoExtension e ){
    if( null==e ){
      throw new IllegalArgumentException("AutoExtension may not be null.");
    }
    synchronized(autoExtensions){
      autoExtensions.add(e);
    }
  }

  /**
     Removes the given object from the auto-extension list if it is in
     that list, otherwise this has no side-effects beyond briefly
     locking that list.
  */
  public static void removeAutoExtension( AutoExtension e ){
    synchronized(autoExtensions){
      autoExtensions.remove(e);
    }
  }

  /**
     Removes all auto-extensions which were added via addAutoExtension().
  */
  public static void clearAutoExtensions(){
    synchronized(autoExtensions){
      autoExtensions.clear();
    }
  }

  /**
     Encapsulates state related to the sqlite3 backup API. Use
     Sqlite.initBackup() to create new instances.
  */
  public static final class Backup implements AutoCloseable {
    private sqlite3_backup b = null;
    private Sqlite dbTo = null;
    private Sqlite dbFrom = null;

    Backup(Sqlite dbDest, String schemaDest,Sqlite dbSrc, String schemaSrc){
      this.dbTo = dbDest;
      this.dbFrom = dbSrc;
      b = CApi.sqlite3_backup_init(dbDest.nativeHandle(), schemaDest,
                                   dbSrc.nativeHandle(), schemaSrc);
      if(null==b) toss();
    }

    private void toss(){
      int rc = CApi.sqlite3_errcode(dbTo.nativeHandle());
      if(0!=rc) throw new SqliteException(dbTo);
      rc = CApi.sqlite3_errcode(dbFrom.nativeHandle());
      if(0!=rc) throw new SqliteException(dbFrom);
      throw new SqliteException(CApi.SQLITE_ERROR);
    }

    private sqlite3_backup getNative(){
      if( null==b ) throw new IllegalStateException("This Backup is already closed.");
      return b;
    }
    /**
       If this backup is still active, this completes the backup and
       frees its native resources, otherwise it this is a no-op.
    */
    public void finish(){
      if( null!=b ){
        CApi.sqlite3_backup_finish(b);
        b = null;
        dbTo = null;
        dbFrom = null;
      }
    }

    /** Equivalent to finish(). */
    @Override public void close(){
      this.finish();
    }

    /**
       Analog to sqlite3_backup_step(). Returns 0 if stepping succeeds
       or, Sqlite.DONE if the end is reached, Sqlite.BUSY if one of
       the databases is busy, Sqlite.LOCKED if one of the databases is
       locked, and throws for any other result code or if this object
       has been closed. Note that BUSY and LOCKED are not necessarily
       permanent errors, so do not trigger an exception.
    */
    public int step(int pageCount){
      final int rc = CApi.sqlite3_backup_step(getNative(), pageCount);
      switch(rc){
        case 0:
        case Sqlite.DONE:
        case Sqlite.BUSY:
        case Sqlite.LOCKED:
          return rc;
        default:
          toss();
          return CApi.SQLITE_ERROR/*not reached*/;
      }
    }

    /**
       Analog to sqlite3_backup_pagecount().
    */
    public int pageCount(){
      return CApi.sqlite3_backup_pagecount(getNative());
    }

    /**
       Analog to sqlite3_backup_remaining().
    */
    public int remaining(){
      return CApi.sqlite3_backup_remaining(getNative());
    }
  }

  /**
     Analog to sqlite3_backup_init(). If schemaSrc is null, "main" is
     assumed. Throws if either this db or dbSrc (the source db) are
     not opened, if either of schemaDest or schemaSrc are null, or if
     the underlying call to sqlite3_backup_init() fails.

     The returned object must eventually be cleaned up by either
     arranging for it to be auto-closed (e.g. using
     try-with-resources) or by calling its finish() method.
  */
  public Backup initBackup(String schemaDest, Sqlite dbSrc, String schemaSrc){
    thisDb();
    dbSrc.thisDb();
    if( null==schemaSrc || null==schemaDest ){
      throw new IllegalArgumentException(
        "Neither the source nor destination schema name may be null."
      );
    }
    return new Backup(this, schemaDest, dbSrc, schemaSrc);
  }


  /**
     Callback type for use with createCollation().
   */
  public interface Collation {
    /**
       Called by the SQLite core to compare inputs. Implementations
       must compare its two arguments using memcmp(3) semantics.

       Warning: the SQLite core has no mechanism for reporting errors
       from custom collations and its workflow does not accommodate
       propagation of exceptions from callbacks. Any exceptions thrown
       from collations will be silently supressed and sorting results
       will be unpredictable.
    */
    int call(byte[] lhs, byte[] rhs);
  }

  /**
     Analog to sqlite3_create_collation().

     Throws if name is null or empty, c is null, or the encoding flag
     is invalid. The encoding must be one of the UTF8, UTF16, UTF16LE,
     or UTF16BE constants.
  */
  public void createCollation(String name, int encoding, Collation c){
    thisDb();
    if( null==name || 0==name.length()){
      throw new IllegalArgumentException("Collation name may not be null or empty.");
    }
    if( null==c ){
      throw new IllegalArgumentException("Collation may not be null.");
    }
    switch(encoding){
      case UTF8:
      case UTF16:
      case UTF16LE:
      case UTF16BE:
        break;
      default:
        throw new IllegalArgumentException("Invalid Collation encoding.");
    }
    checkRc(
      CApi.sqlite3_create_collation(
        thisDb(), name, encoding, new org.sqlite.jni.capi.CollationCallback(){
            @Override public int call(byte[] lhs, byte[] rhs){
              try{return c.call(lhs, rhs);}
              catch(Exception e){return 0;}
            }
            @Override public void xDestroy(){}
          }
      )
    );
  }

  /**
     Callback for use with onCollationNeeded().
  */
  public interface CollationNeeded {
    /**
       Must behave as documented for the callback for
       sqlite3_collation_needed().

       Warning: the C API has no mechanism for reporting or
       propagating errors from this callback, so any exceptions it
       throws are suppressed.
    */
    void call(Sqlite db, int encoding, String collationName);
  }

  /**
     Sets up the given object to be called by the SQLite core when it
     encounters a collation name which it does not know. Pass a null
     object to disconnect the object from the core. This replaces any
     existing collation-needed loader, or is a no-op if the given
     object is already registered. Throws if registering the loader
     fails.
  */
  public void onCollationNeeded( CollationNeeded cn ){
    org.sqlite.jni.capi.CollationNeededCallback cnc = null;
    if( null!=cn ){
      cnc = new org.sqlite.jni.capi.CollationNeededCallback(){
          @Override public void call(sqlite3 db, int encoding, String collationName){
            final Sqlite xdb = Sqlite.fromNative(db);
            if(null!=xdb) cn.call(xdb, encoding, collationName);
          }
        };
    }
    checkRc( CApi.sqlite3_collation_needed(thisDb(), cnc) );
  }

  /**
     Callback for use with busyHandler().
  */
  public interface BusyHandler {
    /**
       Must function as documented for the C-level
       sqlite3_busy_handler() callback argument, minus the (void*)
       argument the C-level function requires.

       If this function throws, it is translated to a database-level
       error.
    */
    int call(int n);
  }

  /**
     Analog to sqlite3_busy_timeout().
  */
  public void setBusyTimeout(int ms){
    checkRc(CApi.sqlite3_busy_timeout(thisDb(), ms));
  }

  /**
     Analog to sqlite3_busy_handler(). If b is null then any
     current handler is cleared.
  */
  void setBusyHandler( BusyHandler b ){
    org.sqlite.jni.capi.BusyHandlerCallback bhc = null;
    if( null!=b ){
      bhc = new org.sqlite.jni.capi.BusyHandlerCallback(){
          @Override public int call(int n){
            return b.call(n);
          }
        };
    }
    checkRc( CApi.sqlite3_busy_handler(thisDb(), bhc) );
  }
}
