/*
** 2023-07-21
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file declares JNI bindings for the sqlite3 C API.
*/
package org.sqlite.jni;
import java.nio.charset.StandardCharsets;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;

/**
   This annotation is for flagging parameters which may legally be
   null, noting that they may behave differently if passed null but
   are prepared to expect null as a value.

   This annotation is solely for the reader's information.
*/
@Documented
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.PARAMETER)
@interface Nullable{}

/**
   This annotation is for flagging parameters which may not legally be
   null. Note that the C-style API does _not_ throw any
   NullPointerExceptions on its own because it has a no-throw policy
   in order to retain its C-style semantics.

   This annotation is solely for the reader's information. No policy
   is in place to programmatically ensure that NotNull is conformed to
   in client code.
*/
@Documented
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.PARAMETER)
@interface NotNull{}

/**
  This class contains the entire sqlite3 JNI API binding.  For
  client-side use, a static import is recommended:

  ```
  import static org.sqlite.jni.SQLite3Jni.*;
  ```

  The C-side part can be found in sqlite3-jni.c.


  Only functions which materially differ from their C counterparts
  are documented here. The C documetation is otherwise applicable
  here:

  https://sqlite.org/c3ref/intro.html

  A handful of Java-specific APIs have been added.


  ******************************************************************
  *** Warning regarding Java's Modified UTF-8 vs standard UTF-8: ***
  ******************************************************************

  SQLite internally uses UTF-8 encoding, whereas Java natively uses
  UTF-16.  Java JNI has routines for converting to and from UTF-8,
  _but_ JNI uses what its docs call modified UTF-8 (see links below)
  Care must be taken when converting Java strings to or from standard
  UTF-8 to ensure that the proper conversion is performed. In short,
  Java's `String.getBytes(StandardCharsets.UTF_8)` performs the proper
  conversion in Java, and there are no JNI C APIs for that conversion
  (JNI's `NewStringUTF()` requires its input to be in MUTF-8).

  The known consequences and limitations this discrepancy places on
  the SQLite3 JNI binding include:

  - Any functions which return client-side data from a database
    take extra care to perform proper conversion, at the cost of
    efficiency.

  - Functions which return database identifiers require those
    identifiers to have identical representations in UTF-8 and
    MUTF-8. They do not perform such conversions (A) because of the
    much lower risk of an encoding discrepancy and (B) to avoid
    significant extra code involved (see both the Java- and C-side
    implementations of sqlite3_db_filename() for an example).  Names
    of databases, tables, columns, collations, and functions MUST NOT
    contain characters which differ in MUTF-8 and UTF-8, or certain
    APIs will mis-translate them on their way between languages
    (possibly leading to a crash).

  - C functions which take C-style strings without a length argument
    require special care when taking input from Java. In particular,
    Java strings converted to byte arrays for encoding purposes are
    not NUL-terminated, and conversion to a Java byte array must be
    careful to add one. Functions which take a length do not require
    this. Search the SQLite3Jni class for "\0" for many examples.

  - Similarly, C-side code which deals with strings which might not be
    NUL-terminated (e.g. while tokenizing in FTS5-related code) cannot
    use JNI's new-string functions to return them to Java because none
    of those APIs take a string-length argument. Such cases must
    return byte arrays instead of strings.

  Further reading:

  - https://stackoverflow.com/questions/57419723
  - https://stackoverflow.com/questions/7921016
  - https://itecnote.com/tecnote/java-getting-true-utf-8-characters-in-java-jni/
  - https://docs.oracle.com/javase/8/docs/api/java/lang/Character.html#unicode
  - https://docs.oracle.com/javase/8/docs/api/java/io/DataInput.html#modified-utf-8

*/
public final class SQLite3Jni {
  static {
    System.loadLibrary("sqlite3-jni");
  }
  //! Not used
  private SQLite3Jni(){}
  //! Called from static init code.
  private static native void init();

  /**
     Each thread which uses the SQLite3 JNI APIs should call
     uncacheJniEnv() when it is done with the library - either right
     before it terminates or when it is finished using the SQLite API.
     This will clean up any cached per-JNIEnv info. Calling into the
     library again after that "should" re-initialize the cache on
     demand, but that's untested.

     This call forcibly wipes out all cached information for the
     current JNIEnv, a side-effect of which is that behavior is
     undefined if any database objects are (A) still active at the
     time it is called _and_ (B) calls are subsequently made into the
     library with such a database. Doing so will, at best, lead to a
     crash.  Azt worst, it will lead to the db possibly misbehaving
     because some of its Java-bound state has been cleared. There is
     no immediate harm in (A) so long as condition (B) is not met.
     This process does _not_ actually close any databases or finalize
     any prepared statements.  For proper library behavior, and to
     avoid C-side leaks, be sure to close them before calling this
     function.

     Calling this from the main application thread is not strictly
     required but is "polite." Additional threads must call this
     before ending or they will leak cache entries in the C heap,
     which in turn may keep numerous Java-side global references
     active.

     This routine returns false without side effects if the current
     JNIEnv is not cached, else returns true, but this information is
     primarily for testing of the JNI bindings and is not information
     which client-level code should use to make any informed
     decisions.
  */
  public static synchronized native boolean uncacheJniEnv();

  //////////////////////////////////////////////////////////////////////
  // Maintenance reminder: please keep the sqlite3_.... functions
  // alphabetized.  The SQLITE_... values. on the other hand, are
  // grouped by category.


  /**
     Functions almost as documented for the C API, with these
     exceptions:

     - The callback interface is more limited because of
       cross-language differences. Specifically, auto-extensions do
       not have access to the sqlite3_api object which native
       auto-extensions do.

     - If an auto-extension opens a db, thereby triggering recursion
       in the auto-extension handler, it will fail with a message
       explaining that recursion is not permitted.

     - All of the other auto extension routines will fail without side
       effects if invoked from within the execution of an
       auto-extension. i.e. auto extensions can neither be added,
       removed, nor cleared while one registered with this function is
       running. Auto-extensions registered directly with the library
       via C code, as opposed to indirectly via Java, do not have that
       limitation.

     See the AutoExtension class docs for more information.

     Achtung: it is as yet unknown whether auto extensions registered
     from one JNIEnv (thread) can be safely called from another.
  */
  public static synchronized native int sqlite3_auto_extension(@NotNull AutoExtension callback);

  public static int sqlite3_bind_blob(
    @NotNull sqlite3_stmt stmt, int ndx, @Nullable byte[] data
  ){
    return (null == data)
      ? sqlite3_bind_null(stmt, ndx)
      : sqlite3_bind_blob(stmt, ndx, data, data.length);
  }

  private static synchronized native int sqlite3_bind_blob(
    @NotNull sqlite3_stmt stmt, int ndx, @Nullable byte[] data, int n
  );

  public static synchronized native int sqlite3_bind_double(
    @NotNull sqlite3_stmt stmt, int ndx, double v
  );

  public static synchronized native int sqlite3_bind_int(
    @NotNull sqlite3_stmt stmt, int ndx, int v
  );

  public static synchronized native int sqlite3_bind_int64(
    @NotNull sqlite3_stmt stmt, int ndx, long v
  );

  public static synchronized native int sqlite3_bind_null(
    @NotNull sqlite3_stmt stmt, int ndx
  );

  public static synchronized native int sqlite3_bind_parameter_count(
    @NotNull sqlite3_stmt stmt
  );


  /** A level of indirection required to ensure that the input to the
      C-level function of the same name is a NUL-terminated UTF-8
      string. */
  private static synchronized native int sqlite3_bind_parameter_index(
    @NotNull sqlite3_stmt stmt, byte[] paramName
  );

  public static int sqlite3_bind_parameter_index(
    @NotNull sqlite3_stmt stmt, @NotNull String paramName
  ){
    final byte[] utf8 = (paramName+"\0").getBytes(StandardCharsets.UTF_8);
    return sqlite3_bind_parameter_index(stmt, utf8);
  }

  public static int sqlite3_bind_text(
    @NotNull sqlite3_stmt stmt, int ndx, @Nullable String data
  ){
    if(null == data) return sqlite3_bind_null(stmt, ndx);
    final byte[] utf8 = data.getBytes(StandardCharsets.UTF_8);
    return sqlite3_bind_text(stmt, ndx, utf8, utf8.length);
  }

  public static int sqlite3_bind_text(
    @NotNull sqlite3_stmt stmt, int ndx, @Nullable byte[] data
  ){
    return (null == data)
      ? sqlite3_bind_null(stmt, ndx)
      : sqlite3_bind_text(stmt, ndx, data, data.length);
  }

  /**
     Works like the C-level sqlite3_bind_text() but (A) assumes
     SQLITE_TRANSIENT for the final parameter and (B) behaves like
     sqlite3_bind_null() if the data argument is null.
  */
  private static synchronized native int sqlite3_bind_text(
    @NotNull sqlite3_stmt stmt, int ndx, @Nullable byte[] data, int maxBytes
  );

  public static synchronized native int sqlite3_bind_zeroblob(
    @NotNull sqlite3_stmt stmt, int ndx, int n
  );

  public static synchronized native int sqlite3_bind_zeroblob64(
    @NotNull sqlite3_stmt stmt, int ndx, long n
  );

  /**
     As for the C-level function of the same name, with a BusyHandler
     instance in place of a callback function. Pass it a null handler
     to clear the busy handler. Calling this multiple times with the
     same object is a no-op on the second and subsequent calls.
  */
  public static synchronized native int sqlite3_busy_handler(
    @NotNull sqlite3 db, @Nullable BusyHandler handler
  );

  public static synchronized native int sqlite3_busy_timeout(
    @NotNull sqlite3 db, int ms
  );

  /**
     Works like the C API except that it returns false, without side
     effects, if auto extensions are currently running. (The JNI-level
     list of extensions cannot be manipulated while it is being traversed.)
  */
  public static synchronized native boolean sqlite3_cancel_auto_extension(
    @NotNull AutoExtension ax
  );

  public static synchronized native int sqlite3_changes(
    @NotNull sqlite3 db
  );

  public static synchronized native long sqlite3_changes64(
    @NotNull sqlite3 db
  );

  public static synchronized native int sqlite3_clear_bindings(
    @NotNull sqlite3_stmt stmt
  );

  public static synchronized native int sqlite3_close(
    @NotNull sqlite3 db
  );

  public static synchronized native int sqlite3_close_v2(
    @NotNull sqlite3 db
  );

  public static synchronized native byte[] sqlite3_column_blob(
    @NotNull sqlite3_stmt stmt, int ndx
  );

  public static synchronized native int sqlite3_column_bytes(
    @NotNull sqlite3_stmt stmt, int ndx
  );

  public static synchronized native int sqlite3_column_bytes16(
    @NotNull sqlite3_stmt stmt, int ndx
  );

  public static synchronized native int sqlite3_column_count(
    @NotNull sqlite3_stmt stmt
  );

  public static synchronized native double sqlite3_column_double(
    @NotNull sqlite3_stmt stmt, int ndx
  );

  public static synchronized native int sqlite3_column_int(
    @NotNull sqlite3_stmt stmt, int ndx
  );

  public static synchronized native long sqlite3_column_int64(
    @NotNull sqlite3_stmt stmt, int ndx
  );

  public static synchronized native String sqlite3_column_name(
    @NotNull sqlite3_stmt stmt, int ndx
  );

  public static synchronized native String sqlite3_column_database_name(
    @NotNull sqlite3_stmt stmt, int ndx
  );

  /**
     Column counterpart of sqlite3_value_java_object().
  */
  public static Object sqlite3_column_java_object(
    @NotNull sqlite3_stmt stmt, int ndx
  ){
    Object rv = null;
    sqlite3_value v = sqlite3_column_value(stmt, ndx);
    if(null!=v){
      v = sqlite3_value_dupe(v) /* we need a "protected" value */;
      if(null!=v){
        rv = sqlite3_value_java_object(v);
        sqlite3_value_free(v);
      }
    }
    return rv;
  }

  /**
     Column counterpart of sqlite3_value_java_casted().
  */
  @SuppressWarnings("unchecked")
  public static <T> T sqlite3_column_java_casted(
    @NotNull sqlite3_stmt stmt, int ndx, @NotNull Class<T> type
  ){
    final Object o = sqlite3_column_java_object(stmt, ndx);
    return type.isInstance(o) ? (T)o : null;
  }

  public static synchronized native String sqlite3_column_origin_name(
    @NotNull sqlite3_stmt stmt, int ndx
  );

  public static synchronized native String sqlite3_column_table_name(
    @NotNull sqlite3_stmt stmt, int ndx
  );

  /**
     To extract _standard_ UTF-8, use sqlite3_column_text().
     This API includes no functions for working with Java's Modified
     UTF-8.
  */
  public static synchronized native String sqlite3_column_text16(
    @NotNull sqlite3_stmt stmt, int ndx
  );

  /**
     Returns the given column's contents as UTF-8-encoded (not MUTF-8) text.
     Use sqlite3_column_text16() to fetch the text
  */
  public static synchronized native byte[] sqlite3_column_text(
    @NotNull sqlite3_stmt stmt, int ndx
  );

  // The real utility of this function is questionable.
  // /**
  //    Returns a Java value representation based on the value of
  //    sqlite_value_type(). For integer types it returns either Integer
  //    or Long, depending on whether the value will fit in an
  //    Integer. For floating-point values it always returns type Double.

  //    If the column was bound using sqlite3_result_java_object() then
  //    that value, as an Object, is returned.
  // */
  // public static Object sqlite3_column_to_java(@NotNull sqlite3_stmt stmt,
  //                                             int ndx){
  //   sqlite3_value v = sqlite3_column_value(stmt, ndx);
  //   Object rv = null;
  //   if(null == v) return v;
  //   v = sqlite3_value_dup(v)/*need a protected value*/;
  //   if(null == v) return v /* OOM error in C */;
  //   if(112/* 'p' */ == sqlite3_value_subtype(v)){
  //     rv = sqlite3_value_java_object(v);
  //   }else{
  //     switch(sqlite3_value_type(v)){
  //       case SQLITE_INTEGER: {
  //         final long i = sqlite3_value_int64(v);
  //         rv = (i<=0x7fffffff && i>=-0x7fffffff-1)
  //           ? new Integer((int)i) : new Long(i);
  //         break;
  //       }
  //       case SQLITE_FLOAT: rv = new Double(sqlite3_value_double(v)); break;
  //       case SQLITE_BLOB: rv = sqlite3_value_blob(v); break;
  //       case SQLITE_TEXT: rv = sqlite3_value_text(v); break;
  //       default: break;
  //     }
  //   }
  //   sqlite3_value_free(v);
  //   return rv;
  // }

  public static synchronized native int sqlite3_column_type(
    @NotNull sqlite3_stmt stmt, int ndx
  );

  public static synchronized native sqlite3_value sqlite3_column_value(
    @NotNull sqlite3_stmt stmt, int ndx
  );

  /**
     This functions like C's sqlite3_collation_needed16() because
     Java's string type is compatible with that interface.
  */
  public static synchronized native int sqlite3_collation_needed(
    @NotNull sqlite3 db, @Nullable CollationNeeded callback
  );

  /**
     Returns the db handle passed to sqlite3_open() or
     sqlite3_open_v2(), as opposed to a new wrapper object.
  */
  public static synchronized native sqlite3 sqlite3_context_db_handle(
    @NotNull sqlite3_context cx
  );

  public static synchronized native CommitHook sqlite3_commit_hook(
    @NotNull sqlite3 db, @Nullable CommitHook hook
  );

  public static native String sqlite3_compileoption_get(
    int n
  );

  public static native boolean sqlite3_compileoption_used(
    @NotNull String optName
  );

  public static synchronized native int sqlite3_create_collation(
    @NotNull sqlite3 db, @NotNull String name, int eTextRep,
    @NotNull Collation col
  );

  /**
     The Java counterpart to the C-native sqlite3_create_function(),
     sqlite3_create_function_v2(), and
     sqlite3_create_window_function(). Which one it behaves like
     depends on which methods the final argument implements. See
     SQLFunction's inner classes (Scalar, Aggregate<T>, and Window<T>)
     for details.
   */
  public static synchronized native int sqlite3_create_function(
    @NotNull sqlite3 db, @NotNull String functionName,
    int nArg, int eTextRep, @NotNull SQLFunction func
  );

  public static synchronized native int sqlite3_data_count(
    @NotNull sqlite3_stmt stmt
  );

  public static synchronized native String sqlite3_db_filename(
    @NotNull sqlite3 db, @NotNull String dbName
  );

  /**
     Overload for sqlite3_db_config() calls which take (int,int*)
     variadic arguments. Returns SQLITE_MISUSE if op is not one of the
     SQLITE_DBCONFIG_... options which uses this call form.
  */
  public static synchronized native int sqlite3_db_config(
    @NotNull sqlite3 db, int op, int onOff, @Nullable OutputPointer.Int32 out
  );

  /**
     Overload for sqlite3_db_config() calls which take a (const char*)
     variadic argument. As of SQLite3 v3.43 the only such option is
     SQLITE_DBCONFIG_MAINDBNAME. Returns SQLITE_MISUSE if op is not
     SQLITE_DBCONFIG_MAINDBNAME, but that set of options may be
     extended in future versions.
  */
  public static synchronized native int sqlite3_db_config(
    @NotNull sqlite3 db, int op, @NotNull String val
  );

  public static synchronized native int sqlite3_db_status(
    @NotNull sqlite3 db, int op, @NotNull OutputPointer.Int32 pCurrent,
    @NotNull OutputPointer.Int32 pHighwater, boolean reset
  );

  public static synchronized native int sqlite3_errcode(@NotNull sqlite3 db);

  public static synchronized native String sqlite3_expanded_sql(@NotNull sqlite3_stmt stmt);

  public static synchronized native int sqlite3_extended_errcode(@NotNull sqlite3 db);

  public static synchronized native boolean sqlite3_extended_result_codes(
    @NotNull sqlite3 db, boolean onoff
  );

  public static synchronized native String sqlite3_errmsg(@NotNull sqlite3 db);

  public static synchronized native String sqlite3_errstr(int resultCode);

  /**
     Note that the offset values assume UTF-8-encoded SQL.
  */
  public static synchronized native int sqlite3_error_offset(@NotNull sqlite3 db);

  public static synchronized native int sqlite3_finalize(@NotNull sqlite3_stmt stmt);

  public static synchronized native int sqlite3_initialize();

  public static synchronized native long sqlite3_last_insert_rowid(@NotNull sqlite3 db);

  public static synchronized native String sqlite3_libversion();

  public static synchronized native int sqlite3_libversion_number();

  /**
     Works like its C counterpart and makes the native pointer of the
     underling (sqlite3*) object available via
     ppDb.getNativePointer(). That pointer is necessary for looking up
     the JNI-side native, but clients need not pay it any
     heed. Passing the object to sqlite3_close() or sqlite3_close_v2()
     will clear that pointer mapping.

     Recall that even if opening fails, the output pointer might be
     non-null. Any error message about the failure will be in that
     object and it is up to the caller to sqlite3_close() that
     db handle.

     Pedantic note: though any number of Java-level sqlite3 objects
     may refer to/wrap a single C-level (sqlite3*), the JNI internals
     take a reference to the object which is passed to sqlite3_open()
     or sqlite3_open_v2() so that they have a predictible object to
     pass to, e.g., the sqlite3_collation_needed() callback.
  */
  public static synchronized native int sqlite3_open(
    @Nullable String filename, @NotNull OutputPointer.sqlite3 ppDb
  );

  public static synchronized native int sqlite3_open_v2(
    @Nullable String filename, @NotNull OutputPointer.sqlite3 ppDb,
    int flags, @Nullable String zVfs
  );

  /**
     The sqlite3_prepare() family of functions require slightly
     different signatures than their native counterparts, but
     overloading allows us to install several convenience forms.

     All of them which take their SQL in the form of a byte[] require
     that it be in UTF-8 encoding unless explicitly noted otherwise.

     The forms which take a "tail" output pointer return (via that
     output object) the index into their SQL byte array at which the
     end of the first SQL statement processed by the call was
     found. That's fundamentally how the C APIs work but making use of
     that value requires more copying of the input SQL into
     consecutively smaller arrays in order to consume all of
     it. (There is an example of doing that in this project's Tester1
     class.) For that vast majority of uses, that capability is not
     necessary, however, and overloads are provided which gloss over
     that.
  */
  private static synchronized native int sqlite3_prepare(
    @NotNull sqlite3 db, @NotNull byte[] sqlUtf8, int maxBytes,
    @NotNull OutputPointer.sqlite3_stmt outStmt,
    @Nullable OutputPointer.Int32 pTailOffset
  );

  public static int sqlite3_prepare(
    @NotNull sqlite3 db, @NotNull byte[] sqlUtf8,
    @NotNull OutputPointer.sqlite3_stmt outStmt,
    @Nullable OutputPointer.Int32 pTailOffset
  ){
    return sqlite3_prepare(db, sqlUtf8, sqlUtf8.length, outStmt, pTailOffset);
  }

  public static int sqlite3_prepare(
    @NotNull sqlite3 db, @NotNull byte[] sqlUtf8,
    @NotNull OutputPointer.sqlite3_stmt outStmt
  ){
    return sqlite3_prepare(db, sqlUtf8, sqlUtf8.length, outStmt, null);
  }

  public static int sqlite3_prepare(
    @NotNull sqlite3 db, @NotNull String sql,
    @NotNull OutputPointer.sqlite3_stmt outStmt
  ){
    final byte[] utf8 = sql.getBytes(StandardCharsets.UTF_8);
    return sqlite3_prepare(db, utf8, utf8.length, outStmt, null);
  }

  private static synchronized native int sqlite3_prepare_v2(
    @NotNull sqlite3 db, @NotNull byte[] sqlUtf8, int maxBytes,
    @NotNull OutputPointer.sqlite3_stmt outStmt,
    @Nullable OutputPointer.Int32 pTailOffset
  );

  public static int sqlite3_prepare_v2(
    @NotNull sqlite3 db, @NotNull byte[] sqlUtf8,
    @NotNull OutputPointer.sqlite3_stmt outStmt,
    @Nullable OutputPointer.Int32 pTailOffset
  ){
    return sqlite3_prepare_v2(db, sqlUtf8, sqlUtf8.length, outStmt, pTailOffset);
  }

  public static int sqlite3_prepare_v2(
    @NotNull sqlite3 db, @NotNull byte[] sqlUtf8,
    @NotNull OutputPointer.sqlite3_stmt outStmt
  ){
    return sqlite3_prepare_v2(db, sqlUtf8, sqlUtf8.length, outStmt, null);
  }

  public static int sqlite3_prepare_v2(
    @NotNull sqlite3 db, @NotNull String sql,
    @NotNull OutputPointer.sqlite3_stmt outStmt
  ){
    final byte[] utf8 = sql.getBytes(StandardCharsets.UTF_8);
    return sqlite3_prepare_v2(db, utf8, utf8.length, outStmt, null);
  }

  private static synchronized native int sqlite3_prepare_v3(
    @NotNull sqlite3 db, @NotNull byte[] sqlUtf8, int maxBytes,
    int prepFlags, @NotNull OutputPointer.sqlite3_stmt outStmt,
    @Nullable OutputPointer.Int32 pTailOffset
  );

  public static int sqlite3_prepare_v3(
    @NotNull sqlite3 db, @NotNull byte[] sqlUtf8, int prepFlags,
    @NotNull OutputPointer.sqlite3_stmt outStmt,
    @Nullable OutputPointer.Int32 pTailOffset
  ){
    return sqlite3_prepare_v3(db, sqlUtf8, sqlUtf8.length, prepFlags, outStmt, pTailOffset);
  }

  public static int sqlite3_prepare_v3(
    @NotNull sqlite3 db, @NotNull byte[] sqlUtf8, int prepFlags,
    @NotNull OutputPointer.sqlite3_stmt outStmt
  ){
    return sqlite3_prepare_v3(db, sqlUtf8, sqlUtf8.length, prepFlags, outStmt, null);
  }

  public static int sqlite3_prepare_v3(
    @NotNull sqlite3 db, @NotNull String sql, int prepFlags,
    @NotNull OutputPointer.sqlite3_stmt outStmt
  ){
    final byte[] utf8 = sql.getBytes(StandardCharsets.UTF_8);
    return sqlite3_prepare_v3(db, utf8, utf8.length, prepFlags, outStmt, null);
  }

  public static synchronized native void sqlite3_progress_handler(
    @NotNull sqlite3 db, int n, @Nullable ProgressHandler h
  );

  //TODO??? void *sqlite3_preupdate_hook(...) and friends

  public static synchronized native int sqlite3_reset(@NotNull sqlite3_stmt stmt);

  /**
     Works like the C API except that it has no side effects if auto
     extensions are currently running. (The JNI-level list of
     extensions cannot be manipulated while it is being traversed.)
  */
  public static synchronized native void sqlite3_reset_auto_extension();

  public static synchronized native void sqlite3_result_double(
    @NotNull sqlite3_context cx, double v
  );

  /**
     The main sqlite3_result_error() impl of which all others are
     proxies. eTextRep must be one of SQLITE_UTF8 or SQLITE_UTF16 and
     msg must be encoded correspondingly. Any other eTextRep value
     results in the C-level sqlite3_result_error() being called with
     a complaint about the invalid argument.
  */
  private static synchronized native void sqlite3_result_error(
    @NotNull sqlite3_context cx, @Nullable byte[] msg,
    int eTextRep
  );

  public static void sqlite3_result_error(
    @NotNull sqlite3_context cx, @NotNull byte[] utf8
  ){
    sqlite3_result_error(cx, utf8, SQLITE_UTF8);
  }

  public static void sqlite3_result_error(
    @NotNull sqlite3_context cx, @NotNull String msg
  ){
    final byte[] utf8 = (msg+"\0").getBytes(StandardCharsets.UTF_8);
    sqlite3_result_error(cx, utf8, SQLITE_UTF8);
  }

  public static void sqlite3_result_error16(
    @NotNull sqlite3_context cx, @Nullable byte[] utf16
  ){
    sqlite3_result_error(cx, utf16, SQLITE_UTF16);
  }

  public static void sqlite3_result_error16(
    @NotNull sqlite3_context cx, @NotNull String msg
  ){
    final byte[] utf8 = (msg+"\0").getBytes(StandardCharsets.UTF_16);
    sqlite3_result_error(cx, utf8, SQLITE_UTF16);
  }

  public static void sqlite3_result_error(
    @NotNull sqlite3_context cx, @NotNull Exception e
  ){
    sqlite3_result_error(cx, e.getMessage());
  }

  public static void sqlite3_result_error16(
    @NotNull sqlite3_context cx, @NotNull Exception e
  ){
    sqlite3_result_error16(cx, e.getMessage());
  }

  public static synchronized native void sqlite3_result_error_toobig(
    @NotNull sqlite3_context cx
  );

  public static synchronized native void sqlite3_result_error_nomem(
    @NotNull sqlite3_context cx
  );

  public static synchronized native void sqlite3_result_error_code(
    @NotNull sqlite3_context cx, int c
  );

  public static synchronized native void sqlite3_result_null(
    @NotNull sqlite3_context cx
  );

  public static synchronized native void sqlite3_result_int(
    @NotNull sqlite3_context cx, int v
  );

  public static synchronized native void sqlite3_result_int64(
    @NotNull sqlite3_context cx, long v
  );

  /**
     Binds the SQL result to the given object, or
     sqlite3_result_null() if o is null. Use
     sqlite3_value_java_object() or sqlite3_column_java_object() to
     fetch it.

     This is implemented in terms of sqlite3_result_pointer(), but
     that function is not exposed to JNI because its 3rd argument must
     be a constant string (the library does not copy it), which we
     cannot implement cross-language here unless, in the JNI layer, we
     allocate such strings and store them somewhere for long-term use
     (leaking them more likely than not). Even then, passing around a
     pointer via Java like that has little practical use.

     Note that there is no sqlite3_bind_java_object() counterpart.
  */
  public static synchronized native void sqlite3_result_java_object(
    @NotNull sqlite3_context cx, @NotNull Object o
  );

  public static void sqlite3_result_set(
    @NotNull sqlite3_context cx, @NotNull Integer v
  ){
    sqlite3_result_int(cx, v);
  }

  public static void sqlite3_result_set(
    @NotNull sqlite3_context cx, int v
  ){
    sqlite3_result_int(cx, v);
  }

  public static void sqlite3_result_set(
    @NotNull sqlite3_context cx, @NotNull Boolean v
  ){
    sqlite3_result_int(cx, v ? 1 : 0);
  }

  public static void sqlite3_result_set(
    @NotNull sqlite3_context cx, boolean v
  ){
    sqlite3_result_int(cx, v ? 1 : 0);
  }

  public static void sqlite3_result_set(
    @NotNull sqlite3_context cx, @NotNull Long v
  ){
    sqlite3_result_int64(cx, v);
  }

  public static void sqlite3_result_set(
    @NotNull sqlite3_context cx, long v
  ){
    sqlite3_result_int64(cx, v);
  }

  public static void sqlite3_result_set(
    @NotNull sqlite3_context cx, @NotNull Double v
  ){
    sqlite3_result_double(cx, v);
  }

  public static void sqlite3_result_set(
    @NotNull sqlite3_context cx, double v
  ){
    sqlite3_result_double(cx, v);
  }

  public static void sqlite3_result_set(
    @NotNull sqlite3_context cx, @Nullable String v
  ){
    sqlite3_result_text(cx, v);
  }

  public static synchronized native void sqlite3_result_value(
    @NotNull sqlite3_context cx, @NotNull sqlite3_value v
  );

  public static synchronized native void sqlite3_result_zeroblob(
    @NotNull sqlite3_context cx, int n
  );

  public static synchronized native int sqlite3_result_zeroblob64(
    @NotNull sqlite3_context cx, long n
  );

  private static synchronized native void sqlite3_result_blob(
    @NotNull sqlite3_context cx, @Nullable byte[] blob, int maxLen
  );

  public static void sqlite3_result_blob(
    @NotNull sqlite3_context cx, @Nullable byte[] blob
  ){
    sqlite3_result_blob(cx, blob, (int)(null==blob ? 0 : blob.length));
  }

  /**
     Binds the given text using C's sqlite3_result_blob64() unless:

     - blob is null ==> sqlite3_result_null()

     - blob is too large ==> sqlite3_result_error_toobig()

     If maxLen is larger than blob.length, it is truncated to that
     value. If it is negative, results are undefined.
  */
  private static synchronized native void sqlite3_result_blob64(
    @NotNull sqlite3_context cx, @Nullable byte[] blob, long maxLen
  );

  public static void sqlite3_result_blob64(
    @NotNull sqlite3_context cx, @Nullable byte[] blob
  ){
    sqlite3_result_blob64(cx, blob, (long)(null==blob ? 0 : blob.length));
  }

  private static synchronized native void sqlite3_result_text(
    @NotNull sqlite3_context cx, @Nullable byte[] text, int maxLen
  );

  public static void sqlite3_result_text(
    @NotNull sqlite3_context cx, @Nullable byte[] text
  ){
    sqlite3_result_text(cx, text, null==text ? 0 : text.length);
  }

  public static void sqlite3_result_text(
    @NotNull sqlite3_context cx, @Nullable String text
  ){
    if(null == text) sqlite3_result_null(cx);
    else{
      final byte[] utf8 = text.getBytes(StandardCharsets.UTF_8);
      sqlite3_result_text(cx, utf8, utf8.length);
    }
  }

  /**
     Binds the given text using C's sqlite3_result_text64() unless:

     - text is null ==> sqlite3_result_null()

     - text is too large ==> sqlite3_result_error_toobig()

     - The `encoding` argument has an invalid value ==>
       sqlite3_result_error_code() with SQLITE_FORMAT

     If maxLength (in bytes, not characters) is larger than
     text.length, it is silently truncated to text.length. If it is
     negative, results are undefined.
  */
  private static synchronized native void sqlite3_result_text64(
    @NotNull sqlite3_context cx, @Nullable byte[] text,
    long maxLength, int encoding
  );

  public static synchronized native int sqlite3_status(
    int op, @NotNull OutputPointer.Int32 pCurrent,
    @NotNull OutputPointer.Int32 pHighwater, boolean reset
  );

  public static synchronized native int sqlite3_status64(
    int op, @NotNull OutputPointer.Int64 pCurrent,
    @NotNull OutputPointer.Int64 pHighwater, boolean reset
  );

  /**
     Sets the current UDF result to the given bytes, which are assumed
     be encoded in UTF-16 using the platform's byte order.
  */
  public static void sqlite3_result_text16(
    @NotNull sqlite3_context cx, @Nullable byte[] text
  ){
    sqlite3_result_text64(cx, text, text.length, SQLITE_UTF16);
  }

  public static void sqlite3_result_text16(
    @NotNull sqlite3_context cx, @Nullable String text
  ){
    if(null == text) sqlite3_result_null(cx);
    else{
      final byte[] b = text.getBytes(StandardCharsets.UTF_16);
      sqlite3_result_text64(cx, b, b.length, SQLITE_UTF16);
    }
  }

  /**
     Sets the current UDF result to the given bytes, which are assumed
     be encoded in UTF-16LE.
  */
  public static void sqlite3_result_text16le(
    @NotNull sqlite3_context cx, @Nullable String text
  ){
    if(null == text) sqlite3_result_null(cx);
    else{
      final byte[] b = text.getBytes(StandardCharsets.UTF_16LE);
      sqlite3_result_text64(cx, b, b.length, SQLITE_UTF16LE);
    }
  }

  /**
     Sets the current UDF result to the given bytes, which are assumed
     be encoded in UTF-16BE.
  */
  public static void sqlite3_result_text16be(
    @NotNull sqlite3_context cx, @Nullable byte[] text
  ){
    sqlite3_result_text64(cx, text, text.length, SQLITE_UTF16BE);
  }

  public static void sqlite3_result_text16be(
    @NotNull sqlite3_context cx, @NotNull String text
  ){
    final byte[] b = text.getBytes(StandardCharsets.UTF_16BE);
    sqlite3_result_text64(cx, b, b.length, SQLITE_UTF16BE);
  }

  public static synchronized native RollbackHook sqlite3_rollback_hook(
    @NotNull sqlite3 db, @Nullable RollbackHook hook
  );

  //! Sets or unsets (if auth is null) the current authorizer.
  public static synchronized native int sqlite3_set_authorizer(
    @NotNull sqlite3 db, @Nullable Authorizer auth
  );

  public static synchronized native void sqlite3_set_last_insert_rowid(
    @NotNull sqlite3 db, long rowid
  );

  public static synchronized native int sqlite3_sleep(int ms);

  public static synchronized native String sqlite3_sourceid();

  public static synchronized native String sqlite3_sql(@NotNull sqlite3_stmt stmt);

  public static synchronized native int sqlite3_step(@NotNull sqlite3_stmt stmt);

  /**
     Internal impl of the public sqlite3_strglob() method. Neither argument
     may be NULL and both _MUST_ be NUL-terminated.
  */
  private static synchronized native int sqlite3_strglob(
    @NotNull byte[] glob, @NotNull byte[] txt
  );

  public static int sqlite3_strglob(
    @NotNull String glob, @NotNull String txt
  ){
    return sqlite3_strglob(
      (glob+"\0").getBytes(StandardCharsets.UTF_8),
      (txt+"\0").getBytes(StandardCharsets.UTF_8)
    );
  }

  /**
     Internal impl of the public sqlite3_strlike() method. Neither
     argument may be NULL and both _MUST_ be NUL-terminated.
  */
  private static synchronized native int sqlite3_strlike(
    @NotNull byte[] glob, @NotNull byte[] txt, int escChar
  );

  public static int sqlite3_strlike(
    @NotNull String glob, @NotNull String txt, char escChar
  ){
    return sqlite3_strlike(
      (glob+"\0").getBytes(StandardCharsets.UTF_8),
      (txt+"\0").getBytes(StandardCharsets.UTF_8),
      (int)escChar
    );
  }

  public static synchronized native int sqlite3_threadsafe();

  public static synchronized native int sqlite3_total_changes(@NotNull sqlite3 db);

  public static synchronized native long sqlite3_total_changes64(@NotNull sqlite3 db);

  /**
     Works like C's sqlite3_trace_v2() except that the 3rd argument to that
     function is elided here because the roles of that functions' 3rd and 4th
     arguments are encapsulated in the final argument to this function.

     Unlike the C API, which is documented as always returning 0, this
     implementation returns SQLITE_NOMEM if allocation of per-db
     mapping state fails and SQLITE_ERROR if the given callback object
     cannot be processed propertly (i.e. an internal error).
  */
  public static synchronized native int sqlite3_trace_v2(
    @NotNull sqlite3 db, int traceMask, @Nullable Tracer tracer
  );

  public static synchronized native UpdateHook sqlite3_update_hook(
    sqlite3 db, UpdateHook hook
  );

  public static synchronized native byte[] sqlite3_value_blob(@NotNull sqlite3_value v);

  public static synchronized native int sqlite3_value_bytes(@NotNull sqlite3_value v);

  public static synchronized native int sqlite3_value_bytes16(@NotNull sqlite3_value v);

  public static synchronized native double sqlite3_value_double(@NotNull sqlite3_value v);

  public static synchronized native sqlite3_value sqlite3_value_dupe(
    @NotNull sqlite3_value v
  );

  public static synchronized native int sqlite3_value_encoding(@NotNull sqlite3_value v);

  public static synchronized native void sqlite3_value_free(@Nullable sqlite3_value v);

  public static synchronized native int sqlite3_value_int(@NotNull sqlite3_value v);

  public static synchronized native long sqlite3_value_int64(@NotNull sqlite3_value v);

  /**
     If the given value was set using sqlite3_result_java_value() then
     this function returns that object, else it returns null.

     It is up to the caller to inspect the object to determine its
     type, and cast it if necessary.
  */
  public static synchronized native Object sqlite3_value_java_object(
    @NotNull sqlite3_value v
  );

  /**
     A variant of sqlite3_value_java_object() which returns the
     fetched object cast to T if the object is an instance of the
     given Class. It returns null in all other cases.
  */
  @SuppressWarnings("unchecked")
  public static <T> T sqlite3_value_java_casted(@NotNull sqlite3_value v,
                                                @NotNull Class<T> type){
    final Object o = sqlite3_value_java_object(v);
    return type.isInstance(o) ? (T)o : null;
  }

  /**
     See sqlite3_column_text() for notes about encoding conversions.
     See sqlite3_value_text_utf8() for how to extract text in standard
     UTF-8.
  */
  public static synchronized native String sqlite3_value_text(@NotNull sqlite3_value v);

  /**
     The sqlite3_value counterpart of sqlite3_column_text_utf8().
  */
  public static synchronized native byte[] sqlite3_value_text_utf8(@NotNull sqlite3_value v);

  public static synchronized native byte[] sqlite3_value_text16(@NotNull sqlite3_value v);

  public static synchronized native byte[] sqlite3_value_text16le(@NotNull sqlite3_value v);

  public static synchronized native byte[] sqlite3_value_text16be(@NotNull sqlite3_value v);

  public static synchronized native int sqlite3_value_type(@NotNull sqlite3_value v);

  public static synchronized native int sqlite3_value_numeric_type(@NotNull sqlite3_value v);

  public static synchronized native int sqlite3_value_nochange(@NotNull sqlite3_value v);

  public static synchronized native int sqlite3_value_frombind(@NotNull sqlite3_value v);

  public static synchronized native int sqlite3_value_subtype(@NotNull sqlite3_value v);

  /**
     Cleans up all per-JNIEnv and per-db state managed by the library
     then calls the C-native sqlite3_shutdown().
  */
  public static synchronized native int sqlite3_shutdown();

  /**
     This is NOT part of the public API. It exists solely as a place
     to hook in arbitrary C-side code during development and testing
     of this library.
  */
  public static synchronized native void sqlite3_do_something_for_developer();

  //////////////////////////////////////////////////////////////////////
  // SQLITE_... constants follow...

  // version info
  public static final int SQLITE_VERSION_NUMBER = sqlite3_libversion_number();
  public static final String SQLITE_VERSION = sqlite3_libversion();
  public static final String SQLITE_SOURCE_ID = sqlite3_sourceid();

  //! Feature flags which are initialized at lib startup. Necessarily
  // non-final so that lib init can fill out the proper values,
  // but modifying them from client code has no effect.
  public static boolean SQLITE_ENABLE_FTS5 = false;

  // access
  public static final int SQLITE_ACCESS_EXISTS = 0;
  public static final int SQLITE_ACCESS_READWRITE = 1;
  public static final int SQLITE_ACCESS_READ = 2;

  // authorizer
  public static final int SQLITE_DENY = 1;
  public static final int SQLITE_IGNORE = 2;
  public static final int SQLITE_CREATE_INDEX = 1;
  public static final int SQLITE_CREATE_TABLE = 2;
  public static final int SQLITE_CREATE_TEMP_INDEX = 3;
  public static final int SQLITE_CREATE_TEMP_TABLE = 4;
  public static final int SQLITE_CREATE_TEMP_TRIGGER = 5;
  public static final int SQLITE_CREATE_TEMP_VIEW = 6;
  public static final int SQLITE_CREATE_TRIGGER = 7;
  public static final int SQLITE_CREATE_VIEW = 8;
  public static final int SQLITE_DELETE = 9;
  public static final int SQLITE_DROP_INDEX = 10;
  public static final int SQLITE_DROP_TABLE = 11;
  public static final int SQLITE_DROP_TEMP_INDEX = 12;
  public static final int SQLITE_DROP_TEMP_TABLE = 13;
  public static final int SQLITE_DROP_TEMP_TRIGGER = 14;
  public static final int SQLITE_DROP_TEMP_VIEW = 15;
  public static final int SQLITE_DROP_TRIGGER = 16;
  public static final int SQLITE_DROP_VIEW = 17;
  public static final int SQLITE_INSERT = 18;
  public static final int SQLITE_PRAGMA = 19;
  public static final int SQLITE_READ = 20;
  public static final int SQLITE_SELECT = 21;
  public static final int SQLITE_TRANSACTION = 22;
  public static final int SQLITE_UPDATE = 23;
  public static final int SQLITE_ATTACH = 24;
  public static final int SQLITE_DETACH = 25;
  public static final int SQLITE_ALTER_TABLE = 26;
  public static final int SQLITE_REINDEX = 27;
  public static final int SQLITE_ANALYZE = 28;
  public static final int SQLITE_CREATE_VTABLE = 29;
  public static final int SQLITE_DROP_VTABLE = 30;
  public static final int SQLITE_FUNCTION = 31;
  public static final int SQLITE_SAVEPOINT = 32;
  public static final int SQLITE_RECURSIVE = 33;

  // blob finalizers: these should, because they are treated as
  // special pointer values in C, ideally have the same sizeof() as
  // the platform's (void*), but we can't know that size from here.
  public static final long SQLITE_STATIC = 0;
  public static final long SQLITE_TRANSIENT = -1;

  // changeset
  public static final int SQLITE_CHANGESETSTART_INVERT = 2;
  public static final int SQLITE_CHANGESETAPPLY_NOSAVEPOINT = 1;
  public static final int SQLITE_CHANGESETAPPLY_INVERT = 2;
  public static final int SQLITE_CHANGESETAPPLY_IGNORENOOP = 4;
  public static final int SQLITE_CHANGESET_DATA = 1;
  public static final int SQLITE_CHANGESET_NOTFOUND = 2;
  public static final int SQLITE_CHANGESET_CONFLICT = 3;
  public static final int SQLITE_CHANGESET_CONSTRAINT = 4;
  public static final int SQLITE_CHANGESET_FOREIGN_KEY = 5;
  public static final int SQLITE_CHANGESET_OMIT = 0;
  public static final int SQLITE_CHANGESET_REPLACE = 1;
  public static final int SQLITE_CHANGESET_ABORT = 2;

  // config
  public static final int SQLITE_CONFIG_SINGLETHREAD = 1;
  public static final int SQLITE_CONFIG_MULTITHREAD = 2;
  public static final int SQLITE_CONFIG_SERIALIZED = 3;
  public static final int SQLITE_CONFIG_MALLOC = 4;
  public static final int SQLITE_CONFIG_GETMALLOC = 5;
  public static final int SQLITE_CONFIG_SCRATCH = 6;
  public static final int SQLITE_CONFIG_PAGECACHE = 7;
  public static final int SQLITE_CONFIG_HEAP = 8;
  public static final int SQLITE_CONFIG_MEMSTATUS = 9;
  public static final int SQLITE_CONFIG_MUTEX = 10;
  public static final int SQLITE_CONFIG_GETMUTEX = 11;
  public static final int SQLITE_CONFIG_LOOKASIDE = 13;
  public static final int SQLITE_CONFIG_PCACHE = 14;
  public static final int SQLITE_CONFIG_GETPCACHE = 15;
  public static final int SQLITE_CONFIG_LOG = 16;
  public static final int SQLITE_CONFIG_URI = 17;
  public static final int SQLITE_CONFIG_PCACHE2 = 18;
  public static final int SQLITE_CONFIG_GETPCACHE2 = 19;
  public static final int SQLITE_CONFIG_COVERING_INDEX_SCAN = 20;
  public static final int SQLITE_CONFIG_SQLLOG = 21;
  public static final int SQLITE_CONFIG_MMAP_SIZE = 22;
  public static final int SQLITE_CONFIG_WIN32_HEAPSIZE = 23;
  public static final int SQLITE_CONFIG_PCACHE_HDRSZ = 24;
  public static final int SQLITE_CONFIG_PMASZ = 25;
  public static final int SQLITE_CONFIG_STMTJRNL_SPILL = 26;
  public static final int SQLITE_CONFIG_SMALL_MALLOC = 27;
  public static final int SQLITE_CONFIG_SORTERREF_SIZE = 28;
  public static final int SQLITE_CONFIG_MEMDB_MAXSIZE = 29;

  // data types
  public static final int SQLITE_INTEGER = 1;
  public static final int SQLITE_FLOAT = 2;
  public static final int SQLITE_TEXT = 3;
  public static final int SQLITE_BLOB = 4;
  public static final int SQLITE_NULL = 5;

  // db config
  public static final int SQLITE_DBCONFIG_MAINDBNAME = 1000;
  public static final int SQLITE_DBCONFIG_LOOKASIDE = 1001;
  public static final int SQLITE_DBCONFIG_ENABLE_FKEY = 1002;
  public static final int SQLITE_DBCONFIG_ENABLE_TRIGGER = 1003;
  public static final int SQLITE_DBCONFIG_ENABLE_FTS3_TOKENIZER = 1004;
  public static final int SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION = 1005;
  public static final int SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE = 1006;
  public static final int SQLITE_DBCONFIG_ENABLE_QPSG = 1007;
  public static final int SQLITE_DBCONFIG_TRIGGER_EQP = 1008;
  public static final int SQLITE_DBCONFIG_RESET_DATABASE = 1009;
  public static final int SQLITE_DBCONFIG_DEFENSIVE = 1010;
  public static final int SQLITE_DBCONFIG_WRITABLE_SCHEMA = 1011;
  public static final int SQLITE_DBCONFIG_LEGACY_ALTER_TABLE = 1012;
  public static final int SQLITE_DBCONFIG_DQS_DML = 1013;
  public static final int SQLITE_DBCONFIG_DQS_DDL = 1014;
  public static final int SQLITE_DBCONFIG_ENABLE_VIEW = 1015;
  public static final int SQLITE_DBCONFIG_LEGACY_FILE_FORMAT = 1016;
  public static final int SQLITE_DBCONFIG_TRUSTED_SCHEMA = 1017;
  public static final int SQLITE_DBCONFIG_STMT_SCANSTATUS = 1018;
  public static final int SQLITE_DBCONFIG_REVERSE_SCANORDER = 1019;
  public static final int SQLITE_DBCONFIG_MAX = 1019;

  // db status
  public static final int SQLITE_DBSTATUS_LOOKASIDE_USED = 0;
  public static final int SQLITE_DBSTATUS_CACHE_USED = 1;
  public static final int SQLITE_DBSTATUS_SCHEMA_USED = 2;
  public static final int SQLITE_DBSTATUS_STMT_USED = 3;
  public static final int SQLITE_DBSTATUS_LOOKASIDE_HIT = 4;
  public static final int SQLITE_DBSTATUS_LOOKASIDE_MISS_SIZE = 5;
  public static final int SQLITE_DBSTATUS_LOOKASIDE_MISS_FULL = 6;
  public static final int SQLITE_DBSTATUS_CACHE_HIT = 7;
  public static final int SQLITE_DBSTATUS_CACHE_MISS = 8;
  public static final int SQLITE_DBSTATUS_CACHE_WRITE = 9;
  public static final int SQLITE_DBSTATUS_DEFERRED_FKS = 10;
  public static final int SQLITE_DBSTATUS_CACHE_USED_SHARED = 11;
  public static final int SQLITE_DBSTATUS_CACHE_SPILL = 12;
  public static final int SQLITE_DBSTATUS_MAX = 12;

  // encodings
  public static final int SQLITE_UTF8 = 1;
  public static final int SQLITE_UTF16LE = 2;
  public static final int SQLITE_UTF16BE = 3;
  public static final int SQLITE_UTF16 = 4;
  public static final int SQLITE_UTF16_ALIGNED = 8;

  // fcntl
  public static final int SQLITE_FCNTL_LOCKSTATE = 1;
  public static final int SQLITE_FCNTL_GET_LOCKPROXYFILE = 2;
  public static final int SQLITE_FCNTL_SET_LOCKPROXYFILE = 3;
  public static final int SQLITE_FCNTL_LAST_ERRNO = 4;
  public static final int SQLITE_FCNTL_SIZE_HINT = 5;
  public static final int SQLITE_FCNTL_CHUNK_SIZE = 6;
  public static final int SQLITE_FCNTL_FILE_POINTER = 7;
  public static final int SQLITE_FCNTL_SYNC_OMITTED = 8;
  public static final int SQLITE_FCNTL_WIN32_AV_RETRY = 9;
  public static final int SQLITE_FCNTL_PERSIST_WAL = 10;
  public static final int SQLITE_FCNTL_OVERWRITE = 11;
  public static final int SQLITE_FCNTL_VFSNAME = 12;
  public static final int SQLITE_FCNTL_POWERSAFE_OVERWRITE = 13;
  public static final int SQLITE_FCNTL_PRAGMA = 14;
  public static final int SQLITE_FCNTL_BUSYHANDLER = 15;
  public static final int SQLITE_FCNTL_TEMPFILENAME = 16;
  public static final int SQLITE_FCNTL_MMAP_SIZE = 18;
  public static final int SQLITE_FCNTL_TRACE = 19;
  public static final int SQLITE_FCNTL_HAS_MOVED = 20;
  public static final int SQLITE_FCNTL_SYNC = 21;
  public static final int SQLITE_FCNTL_COMMIT_PHASETWO = 22;
  public static final int SQLITE_FCNTL_WIN32_SET_HANDLE = 23;
  public static final int SQLITE_FCNTL_WAL_BLOCK = 24;
  public static final int SQLITE_FCNTL_ZIPVFS = 25;
  public static final int SQLITE_FCNTL_RBU = 26;
  public static final int SQLITE_FCNTL_VFS_POINTER = 27;
  public static final int SQLITE_FCNTL_JOURNAL_POINTER = 28;
  public static final int SQLITE_FCNTL_WIN32_GET_HANDLE = 29;
  public static final int SQLITE_FCNTL_PDB = 30;
  public static final int SQLITE_FCNTL_BEGIN_ATOMIC_WRITE = 31;
  public static final int SQLITE_FCNTL_COMMIT_ATOMIC_WRITE = 32;
  public static final int SQLITE_FCNTL_ROLLBACK_ATOMIC_WRITE = 33;
  public static final int SQLITE_FCNTL_LOCK_TIMEOUT = 34;
  public static final int SQLITE_FCNTL_DATA_VERSION = 35;
  public static final int SQLITE_FCNTL_SIZE_LIMIT = 36;
  public static final int SQLITE_FCNTL_CKPT_DONE = 37;
  public static final int SQLITE_FCNTL_RESERVE_BYTES = 38;
  public static final int SQLITE_FCNTL_CKPT_START = 39;
  public static final int SQLITE_FCNTL_EXTERNAL_READER = 40;
  public static final int SQLITE_FCNTL_CKSM_FILE = 41;
  public static final int SQLITE_FCNTL_RESET_CACHE = 42;

  // flock
  public static final int SQLITE_LOCK_NONE = 0;
  public static final int SQLITE_LOCK_SHARED = 1;
  public static final int SQLITE_LOCK_RESERVED = 2;
  public static final int SQLITE_LOCK_PENDING = 3;
  public static final int SQLITE_LOCK_EXCLUSIVE = 4;

  // iocap
  public static final int SQLITE_IOCAP_ATOMIC = 1;
  public static final int SQLITE_IOCAP_ATOMIC512 = 2;
  public static final int SQLITE_IOCAP_ATOMIC1K = 4;
  public static final int SQLITE_IOCAP_ATOMIC2K = 8;
  public static final int SQLITE_IOCAP_ATOMIC4K = 16;
  public static final int SQLITE_IOCAP_ATOMIC8K = 32;
  public static final int SQLITE_IOCAP_ATOMIC16K = 64;
  public static final int SQLITE_IOCAP_ATOMIC32K = 128;
  public static final int SQLITE_IOCAP_ATOMIC64K = 256;
  public static final int SQLITE_IOCAP_SAFE_APPEND = 512;
  public static final int SQLITE_IOCAP_SEQUENTIAL = 1024;
  public static final int SQLITE_IOCAP_UNDELETABLE_WHEN_OPEN = 2048;
  public static final int SQLITE_IOCAP_POWERSAFE_OVERWRITE = 4096;
  public static final int SQLITE_IOCAP_IMMUTABLE = 8192;
  public static final int SQLITE_IOCAP_BATCH_ATOMIC = 16384;

  // limits. These get injected at init-time so that they stay in sync
  // with the compile-time options. This unfortunately means they are
  // not final, but keeping them in sync with their C values seems
  // more important than protecting users from assigning to these
  // (with unpredictable results).
  public static int SQLITE_MAX_ALLOCATION_SIZE = -1;
  public static int SQLITE_LIMIT_LENGTH = -1;
  public static int SQLITE_MAX_LENGTH = -1;
  public static int SQLITE_LIMIT_SQL_LENGTH = -1;
  public static int SQLITE_MAX_SQL_LENGTH = -1;
  public static int SQLITE_LIMIT_COLUMN = -1;
  public static int SQLITE_MAX_COLUMN = -1;
  public static int SQLITE_LIMIT_EXPR_DEPTH = -1;
  public static int SQLITE_MAX_EXPR_DEPTH = -1;
  public static int SQLITE_LIMIT_COMPOUND_SELECT = -1;
  public static int SQLITE_MAX_COMPOUND_SELECT = -1;
  public static int SQLITE_LIMIT_VDBE_OP = -1;
  public static int SQLITE_MAX_VDBE_OP = -1;
  public static int SQLITE_LIMIT_FUNCTION_ARG = -1;
  public static int SQLITE_MAX_FUNCTION_ARG = -1;
  public static int SQLITE_LIMIT_ATTACHED = -1;
  public static int SQLITE_MAX_ATTACHED = -1;
  public static int SQLITE_LIMIT_LIKE_PATTERN_LENGTH = -1;
  public static int SQLITE_MAX_LIKE_PATTERN_LENGTH = -1;
  public static int SQLITE_LIMIT_VARIABLE_NUMBER = -1;
  public static int SQLITE_MAX_VARIABLE_NUMBER = -1;
  public static int SQLITE_LIMIT_TRIGGER_DEPTH = -1;
  public static int SQLITE_MAX_TRIGGER_DEPTH = -1;
  public static int SQLITE_LIMIT_WORKER_THREADS = -1;
  public static int SQLITE_MAX_WORKER_THREADS = -1;

  // open flags
  public static final int SQLITE_OPEN_READONLY = 1;
  public static final int SQLITE_OPEN_READWRITE = 2;
  public static final int SQLITE_OPEN_CREATE = 4;
  public static final int SQLITE_OPEN_URI = 64;
  public static final int SQLITE_OPEN_MEMORY = 128;
  public static final int SQLITE_OPEN_NOMUTEX = 32768;
  public static final int SQLITE_OPEN_FULLMUTEX = 65536;
  public static final int SQLITE_OPEN_SHAREDCACHE = 131072;
  public static final int SQLITE_OPEN_PRIVATECACHE = 262144;
  public static final int SQLITE_OPEN_EXRESCODE = 33554432;
  public static final int SQLITE_OPEN_NOFOLLOW = 16777216;
  public static final int SQLITE_OPEN_MAIN_DB = 256;
  public static final int SQLITE_OPEN_MAIN_JOURNAL = 2048;
  public static final int SQLITE_OPEN_TEMP_DB = 512;
  public static final int SQLITE_OPEN_TEMP_JOURNAL = 4096;
  public static final int SQLITE_OPEN_TRANSIENT_DB = 1024;
  public static final int SQLITE_OPEN_SUBJOURNAL = 8192;
  public static final int SQLITE_OPEN_SUPER_JOURNAL = 16384;
  public static final int SQLITE_OPEN_WAL = 524288;
  public static final int SQLITE_OPEN_DELETEONCLOSE = 8;
  public static final int SQLITE_OPEN_EXCLUSIVE = 16;

  // prepare flags
  public static final int SQLITE_PREPARE_PERSISTENT = 1;
  public static final int SQLITE_PREPARE_NORMALIZE = 2;
  public static final int SQLITE_PREPARE_NO_VTAB = 4;

  // result codes
  public static final int SQLITE_OK = 0;
  public static final int SQLITE_ERROR = 1;
  public static final int SQLITE_INTERNAL = 2;
  public static final int SQLITE_PERM = 3;
  public static final int SQLITE_ABORT = 4;
  public static final int SQLITE_BUSY = 5;
  public static final int SQLITE_LOCKED = 6;
  public static final int SQLITE_NOMEM = 7;
  public static final int SQLITE_READONLY = 8;
  public static final int SQLITE_INTERRUPT = 9;
  public static final int SQLITE_IOERR = 10;
  public static final int SQLITE_CORRUPT = 11;
  public static final int SQLITE_NOTFOUND = 12;
  public static final int SQLITE_FULL = 13;
  public static final int SQLITE_CANTOPEN = 14;
  public static final int SQLITE_PROTOCOL = 15;
  public static final int SQLITE_EMPTY = 16;
  public static final int SQLITE_SCHEMA = 17;
  public static final int SQLITE_TOOBIG = 18;
  public static final int SQLITE_CONSTRAINT = 19;
  public static final int SQLITE_MISMATCH = 20;
  public static final int SQLITE_MISUSE = 21;
  public static final int SQLITE_NOLFS = 22;
  public static final int SQLITE_AUTH = 23;
  public static final int SQLITE_FORMAT = 24;
  public static final int SQLITE_RANGE = 25;
  public static final int SQLITE_NOTADB = 26;
  public static final int SQLITE_NOTICE = 27;
  public static final int SQLITE_WARNING = 28;
  public static final int SQLITE_ROW = 100;
  public static final int SQLITE_DONE = 101;
  public static final int SQLITE_ERROR_MISSING_COLLSEQ = 257;
  public static final int SQLITE_ERROR_RETRY = 513;
  public static final int SQLITE_ERROR_SNAPSHOT = 769;
  public static final int SQLITE_IOERR_READ = 266;
  public static final int SQLITE_IOERR_SHORT_READ = 522;
  public static final int SQLITE_IOERR_WRITE = 778;
  public static final int SQLITE_IOERR_FSYNC = 1034;
  public static final int SQLITE_IOERR_DIR_FSYNC = 1290;
  public static final int SQLITE_IOERR_TRUNCATE = 1546;
  public static final int SQLITE_IOERR_FSTAT = 1802;
  public static final int SQLITE_IOERR_UNLOCK = 2058;
  public static final int SQLITE_IOERR_RDLOCK = 2314;
  public static final int SQLITE_IOERR_DELETE = 2570;
  public static final int SQLITE_IOERR_BLOCKED = 2826;
  public static final int SQLITE_IOERR_NOMEM = 3082;
  public static final int SQLITE_IOERR_ACCESS = 3338;
  public static final int SQLITE_IOERR_CHECKRESERVEDLOCK = 3594;
  public static final int SQLITE_IOERR_LOCK = 3850;
  public static final int SQLITE_IOERR_CLOSE = 4106;
  public static final int SQLITE_IOERR_DIR_CLOSE = 4362;
  public static final int SQLITE_IOERR_SHMOPEN = 4618;
  public static final int SQLITE_IOERR_SHMSIZE = 4874;
  public static final int SQLITE_IOERR_SHMLOCK = 5130;
  public static final int SQLITE_IOERR_SHMMAP = 5386;
  public static final int SQLITE_IOERR_SEEK = 5642;
  public static final int SQLITE_IOERR_DELETE_NOENT = 5898;
  public static final int SQLITE_IOERR_MMAP = 6154;
  public static final int SQLITE_IOERR_GETTEMPPATH = 6410;
  public static final int SQLITE_IOERR_CONVPATH = 6666;
  public static final int SQLITE_IOERR_VNODE = 6922;
  public static final int SQLITE_IOERR_AUTH = 7178;
  public static final int SQLITE_IOERR_BEGIN_ATOMIC = 7434;
  public static final int SQLITE_IOERR_COMMIT_ATOMIC = 7690;
  public static final int SQLITE_IOERR_ROLLBACK_ATOMIC = 7946;
  public static final int SQLITE_IOERR_DATA = 8202;
  public static final int SQLITE_IOERR_CORRUPTFS = 8458;
  public static final int SQLITE_LOCKED_SHAREDCACHE = 262;
  public static final int SQLITE_LOCKED_VTAB = 518;
  public static final int SQLITE_BUSY_RECOVERY = 261;
  public static final int SQLITE_BUSY_SNAPSHOT = 517;
  public static final int SQLITE_BUSY_TIMEOUT = 773;
  public static final int SQLITE_CANTOPEN_NOTEMPDIR = 270;
  public static final int SQLITE_CANTOPEN_ISDIR = 526;
  public static final int SQLITE_CANTOPEN_FULLPATH = 782;
  public static final int SQLITE_CANTOPEN_CONVPATH = 1038;
  public static final int SQLITE_CANTOPEN_SYMLINK = 1550;
  public static final int SQLITE_CORRUPT_VTAB = 267;
  public static final int SQLITE_CORRUPT_SEQUENCE = 523;
  public static final int SQLITE_CORRUPT_INDEX = 779;
  public static final int SQLITE_READONLY_RECOVERY = 264;
  public static final int SQLITE_READONLY_CANTLOCK = 520;
  public static final int SQLITE_READONLY_ROLLBACK = 776;
  public static final int SQLITE_READONLY_DBMOVED = 1032;
  public static final int SQLITE_READONLY_CANTINIT = 1288;
  public static final int SQLITE_READONLY_DIRECTORY = 1544;
  public static final int SQLITE_ABORT_ROLLBACK = 516;
  public static final int SQLITE_CONSTRAINT_CHECK = 275;
  public static final int SQLITE_CONSTRAINT_COMMITHOOK = 531;
  public static final int SQLITE_CONSTRAINT_FOREIGNKEY = 787;
  public static final int SQLITE_CONSTRAINT_FUNCTION = 1043;
  public static final int SQLITE_CONSTRAINT_NOTNULL = 1299;
  public static final int SQLITE_CONSTRAINT_PRIMARYKEY = 1555;
  public static final int SQLITE_CONSTRAINT_TRIGGER = 1811;
  public static final int SQLITE_CONSTRAINT_UNIQUE = 2067;
  public static final int SQLITE_CONSTRAINT_VTAB = 2323;
  public static final int SQLITE_CONSTRAINT_ROWID = 2579;
  public static final int SQLITE_CONSTRAINT_PINNED = 2835;
  public static final int SQLITE_CONSTRAINT_DATATYPE = 3091;
  public static final int SQLITE_NOTICE_RECOVER_WAL = 283;
  public static final int SQLITE_NOTICE_RECOVER_ROLLBACK = 539;
  public static final int SQLITE_WARNING_AUTOINDEX = 284;
  public static final int SQLITE_AUTH_USER = 279;
  public static final int SQLITE_OK_LOAD_PERMANENTLY = 256;

  // serialize
  public static final int SQLITE_SERIALIZE_NOCOPY = 1;
  public static final int SQLITE_DESERIALIZE_FREEONCLOSE = 1;
  public static final int SQLITE_DESERIALIZE_READONLY = 4;
  public static final int SQLITE_DESERIALIZE_RESIZEABLE = 2;

  // session
  public static final int SQLITE_SESSION_CONFIG_STRMSIZE = 1;
  public static final int SQLITE_SESSION_OBJCONFIG_SIZE = 1;

  // sqlite3 status
  public static final int SQLITE_STATUS_MEMORY_USED = 0;
  public static final int SQLITE_STATUS_PAGECACHE_USED = 1;
  public static final int SQLITE_STATUS_PAGECACHE_OVERFLOW = 2;
  public static final int SQLITE_STATUS_MALLOC_SIZE = 5;
  public static final int SQLITE_STATUS_PARSER_STACK = 6;
  public static final int SQLITE_STATUS_PAGECACHE_SIZE = 7;
  public static final int SQLITE_STATUS_MALLOC_COUNT = 9;

  // stmt status
  public static final int SQLITE_STMTSTATUS_FULLSCAN_STEP = 1;
  public static final int SQLITE_STMTSTATUS_SORT = 2;
  public static final int SQLITE_STMTSTATUS_AUTOINDEX = 3;
  public static final int SQLITE_STMTSTATUS_VM_STEP = 4;
  public static final int SQLITE_STMTSTATUS_REPREPARE = 5;
  public static final int SQLITE_STMTSTATUS_RUN = 6;
  public static final int SQLITE_STMTSTATUS_FILTER_MISS = 7;
  public static final int SQLITE_STMTSTATUS_FILTER_HIT = 8;
  public static final int SQLITE_STMTSTATUS_MEMUSED = 99;

  // sync flags
  public static final int SQLITE_SYNC_NORMAL = 2;
  public static final int SQLITE_SYNC_FULL = 3;
  public static final int SQLITE_SYNC_DATAONLY = 16;

  // tracing flags
  public static final int SQLITE_TRACE_STMT = 1;
  public static final int SQLITE_TRACE_PROFILE = 2;
  public static final int SQLITE_TRACE_ROW = 4;
  public static final int SQLITE_TRACE_CLOSE = 8;

  // transaction state
  public static final int SQLITE_TXN_NONE = 0;
  public static final int SQLITE_TXN_READ = 1;
  public static final int SQLITE_TXN_WRITE = 2;

  // udf flags
  public static final int SQLITE_DETERMINISTIC = 2048;
  public static final int SQLITE_DIRECTONLY = 524288;
  public static final int SQLITE_INNOCUOUS = 2097152;

  // virtual tables
  public static final int SQLITE_INDEX_SCAN_UNIQUE = 1;
  public static final int SQLITE_INDEX_CONSTRAINT_EQ = 2;
  public static final int SQLITE_INDEX_CONSTRAINT_GT = 4;
  public static final int SQLITE_INDEX_CONSTRAINT_LE = 8;
  public static final int SQLITE_INDEX_CONSTRAINT_LT = 16;
  public static final int SQLITE_INDEX_CONSTRAINT_GE = 32;
  public static final int SQLITE_INDEX_CONSTRAINT_MATCH = 64;
  public static final int SQLITE_INDEX_CONSTRAINT_LIKE = 65;
  public static final int SQLITE_INDEX_CONSTRAINT_GLOB = 66;
  public static final int SQLITE_INDEX_CONSTRAINT_REGEXP = 67;
  public static final int SQLITE_INDEX_CONSTRAINT_NE = 68;
  public static final int SQLITE_INDEX_CONSTRAINT_ISNOT = 69;
  public static final int SQLITE_INDEX_CONSTRAINT_ISNOTNULL = 70;
  public static final int SQLITE_INDEX_CONSTRAINT_ISNULL = 71;
  public static final int SQLITE_INDEX_CONSTRAINT_IS = 72;
  public static final int SQLITE_INDEX_CONSTRAINT_LIMIT = 73;
  public static final int SQLITE_INDEX_CONSTRAINT_OFFSET = 74;
  public static final int SQLITE_INDEX_CONSTRAINT_FUNCTION = 150;
  public static final int SQLITE_VTAB_CONSTRAINT_SUPPORT = 1;
  public static final int SQLITE_VTAB_INNOCUOUS = 2;
  public static final int SQLITE_VTAB_DIRECTONLY = 3;
  public static final int SQLITE_VTAB_USES_ALL_SCHEMAS = 4;
  public static final int SQLITE_ROLLBACK = 1;
  public static final int SQLITE_FAIL = 3;
  public static final int SQLITE_REPLACE = 5;
  static {
    // This MUST come after the SQLITE_MAX_... values or else
    // attempting to modify them silently fails.
    init();
  }
}
