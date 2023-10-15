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
import org.sqlite.jni.annotation.*;
import java.util.Arrays;

/**
  This class contains the entire C-style sqlite3 JNI API binding,
  minus a few bits and pieces declared in other files. For client-side
  use, a static import is recommended:

  <pre>{@code
  import static org.sqlite.jni.CApi.*;
  }</pre>

  <p>The C-side part can be found in sqlite3-jni.c.

  <p>This class is package-private in order to keep Java clients from
  having direct access to the low-level C-style APIs, a design
  decision made by Java developers based on the C-style API being
  riddled with opportunities for Java developers to proverbially shoot
  themselves in the foot with. Third-party copies of this code may
  eliminate that guard by simply changing this class from
  package-private to public. Its methods which are intended to be
  exposed that way are all public.

  <p>Only functions which materially differ from their C counterparts
  are documented here, and only those material differences are
  documented. The C documentation is otherwise applicable for these
  APIs:

  <p><a href="https://sqlite.org/c3ref/intro.html">https://sqlite.org/c3ref/intro.html</a>

  <p>A handful of Java-specific APIs have been added which are
  documented here. A number of convenience overloads are provided
  which are not documented but whose semantics map 1-to-1 in an
  intuitive manner. e.g. {@link
  #sqlite3_result_set(sqlite3_context,int)} is equivalent to {@link
  #sqlite3_result_int}, and sqlite3_result_set() has many
  type-specific overloads.

  <p>Notes regarding Java's Modified UTF-8 vs standard UTF-8:

  <p>SQLite internally uses UTF-8 encoding, whereas Java natively uses
  UTF-16.  Java JNI has routines for converting to and from UTF-8,
  but JNI uses what its docs call modified UTF-8 (see links below)
  Care must be taken when converting Java strings to or from standard
  UTF-8 to ensure that the proper conversion is performed. In short,
  Java's {@code String.getBytes(StandardCharsets.UTF_8)} performs the proper
  conversion in Java, and there are no JNI C APIs for that conversion
  (JNI's {@code NewStringUTF()} requires its input to be in MUTF-8).

  <p>The known consequences and limitations this discrepancy places on
  the SQLite3 JNI binding include:

  <ul>

  <li>C functions which take C-style strings without a length argument
  require special care when taking input from Java. In particular,
  Java strings converted to byte arrays for encoding purposes are not
  NUL-terminated, and conversion to a Java byte array must sometimes
  be careful to add one. Functions which take a length do not require
  this so long as the length is provided. Search the CApi class
  for "\0" for many examples.

  </ul>

  <p>Further reading:

  <p><a href="https://stackoverflow.com/questions/57419723">https://stackoverflow.com/questions/57419723</a>
  <p><a href="https://stackoverflow.com/questions/7921016">https://stackoverflow.com/questions/7921016</a>
  <p><a href="https://itecnote.com/tecnote/java-getting-true-utf-8-characters-in-java-jni/">https://itecnote.com/tecnote/java-getting-true-utf-8-characters-in-java-jni/</a>
  <p><a href="https://docs.oracle.com/javase/8/docs/api/java/lang/Character.html#unicode">https://docs.oracle.com/javase/8/docs/api/java/lang/Character.html#unicode</a>
  <p><a href="https://docs.oracle.com/javase/8/docs/api/java/io/DataInput.html#modified-utf-8">https://docs.oracle.com/javase/8/docs/api/java/io/DataInput.html#modified-utf-8</a>

*/
final class CApi {
  static {
    System.loadLibrary("sqlite3-jni");
  }
  //! Not used
  private CApi(){}
  //! Called from static init code.
  private static native void init();

  /**
     Returns a nul-terminated copy of s as a UTF-8-encoded byte array,
     or null if s is null.
  */
  private static byte[] nulTerminateUtf8(String s){
    return null==s ? null : (s+"\0").getBytes(StandardCharsets.UTF_8);
  }

  /**
     Each thread which uses the SQLite3 JNI APIs should call
     sqlite3_jni_uncache_thread() when it is done with the library -
     either right before it terminates or when it finishes using the
     SQLite API.  This will clean up any cached per-thread info.

     <p>This process does not close any databases or finalize
     any prepared statements because their ownership does not depend on
     a given thread.  For proper library behavior, and to
     avoid C-side leaks, be sure to finalize all statements and close
     all databases before calling this function.

     <p>Calling this from the main application thread is not strictly
     required. Additional threads must call this before ending or they
     will leak cache entries in the C heap, which in turn may keep
     numerous Java-side global references active.

     <p>This routine returns false without side effects if the current
     JNIEnv is not cached, else returns true, but this information is
     primarily for testing of the JNI bindings and is not information
     which client-level code can use to make any informed decisions.
  */
  public static native boolean sqlite3_java_uncache_thread();

  //////////////////////////////////////////////////////////////////////
  // Maintenance reminder: please keep the sqlite3_.... functions
  // alphabetized.  The SQLITE_... values. on the other hand, are
  // grouped by category.

  /**
     Functions exactly like the native form except that (A) the 2nd
     argument is a boolean instead of an int and (B) the returned
     value is not a pointer address and is only intended for use as a
     per-UDF-call lookup key in a higher-level data structure.

     <p>Passing a true second argument is analogous to passing some
     unspecified small, non-0 positive value to the C API and passing
     false is equivalent to passing 0 to the C API.

     <p>Like the C API, it returns 0 if allocation fails or if
     initialize is false and no prior aggregate context was allocated
     for cx.  If initialize is true then it returns 0 only on
     allocation error. In all casses, 0 is considered the sentinel
     "not a key" value.
  */
  public static native long sqlite3_aggregate_context(sqlite3_context cx, boolean initialize);

  /**
     Functions almost as documented for the C API, with these
     exceptions:

     <p>- The callback interface is shorter because of
     cross-language differences. Specifically, 3rd argument to the C
     auto-extension callback interface is unnecessary here.

     <p>The C API docs do not specifically say so, but if the list of
     auto-extensions is manipulated from an auto-extension, it is
     undefined which, if any, auto-extensions will subsequently
     execute for the current database. That is, doing so will result
     in unpredictable, but not undefined, behavior.

     <p>See the AutoExtension class docs for more information.
  */
  public static native int sqlite3_auto_extension(@NotNull AutoExtensionCallback callback);

  static native int sqlite3_backup_finish(@NotNull long ptrToBackup);

  public static int sqlite3_backup_finish(@NotNull sqlite3_backup b){
    return sqlite3_backup_finish(b.clearNativePointer());
  }

  static native sqlite3_backup sqlite3_backup_init(
    @NotNull long ptrToDbDest, @NotNull String destTableName,
    @NotNull long ptrToDbSrc, @NotNull String srcTableName
  );

  public static sqlite3_backup sqlite3_backup_init(
    @NotNull sqlite3 dbDest, @NotNull String destTableName,
    @NotNull sqlite3 dbSrc, @NotNull String srcTableName
  ){
    return sqlite3_backup_init( dbDest.getNativePointer(), destTableName,
                                dbSrc.getNativePointer(), srcTableName );
  }

  static native int sqlite3_backup_pagecount(@NotNull long ptrToBackup);

  public static int sqlite3_backup_pagecount(@NotNull sqlite3_backup b){
    return sqlite3_backup_pagecount(b.getNativePointer());
  }

  static native int sqlite3_backup_remaining(@NotNull long ptrToBackup);

  public static int sqlite3_backup_remaining(@NotNull sqlite3_backup b){
    return sqlite3_backup_remaining(b.getNativePointer());
  }

  static native int sqlite3_backup_step(@NotNull long ptrToBackup, int nPage);

  public static int sqlite3_backup_step(@NotNull sqlite3_backup b, int nPage){
    return sqlite3_backup_step(b.getNativePointer(), nPage);
  }

  static native int sqlite3_bind_blob(
    @NotNull long ptrToStmt, int ndx, @Nullable byte[] data, int n
  );

  /**
     Results are undefined if data is not null and n<0 || n>=data.length.
  */
  public static int sqlite3_bind_blob(
    @NotNull sqlite3_stmt stmt, int ndx, @Nullable byte[] data, int n
  ){
    return sqlite3_bind_blob(stmt.getNativePointer(), ndx, data, n);
  }

  public static int sqlite3_bind_blob(
    @NotNull sqlite3_stmt stmt, int ndx, @Nullable byte[] data
  ){
    return (null==data)
      ? sqlite3_bind_null(stmt.getNativePointer(), ndx)
      : sqlite3_bind_blob(stmt.getNativePointer(), ndx, data, data.length);
  }

  static native int sqlite3_bind_double(
    @NotNull long ptrToStmt, int ndx, double v
  );

  public static int sqlite3_bind_double(
    @NotNull sqlite3_stmt stmt, int ndx, double v
  ){
    return sqlite3_bind_double(stmt.getNativePointer(), ndx, v);
  }

  static native int sqlite3_bind_int(
    @NotNull long ptrToStmt, int ndx, int v
  );

  public static int sqlite3_bind_int(
    @NotNull sqlite3_stmt stmt, int ndx, int v
  ){
    return sqlite3_bind_int(stmt.getNativePointer(), ndx, v);
  }

  static native int sqlite3_bind_int64(
    @NotNull long ptrToStmt, int ndx, long v
  );

  public static int sqlite3_bind_int64(@NotNull sqlite3_stmt stmt, int ndx, long v){
    return sqlite3_bind_int64( stmt.getNativePointer(), ndx, v );
  }

  static native int sqlite3_bind_java_object(
    @NotNull long ptrToStmt, int ndx, @Nullable Object o
  );

  /**
     Binds the given object at the given index. If o is null then this behaves like
     sqlite3_bind_null().

     @see #sqlite3_result_java_object
  */
  public static int sqlite3_bind_java_object(
    @NotNull sqlite3_stmt stmt, int ndx, @Nullable Object o
  ){
    return sqlite3_bind_java_object(stmt.getNativePointer(), ndx, o);
  }

  static native int sqlite3_bind_null(@NotNull long ptrToStmt, int ndx);

  public static int sqlite3_bind_null(@NotNull sqlite3_stmt stmt, int ndx){
    return sqlite3_bind_null(stmt.getNativePointer(), ndx);
  }

  static native int sqlite3_bind_parameter_count(@NotNull long ptrToStmt);

  public static int sqlite3_bind_parameter_count(@NotNull sqlite3_stmt stmt){
    return sqlite3_bind_parameter_count(stmt.getNativePointer());
  }

  /**
     Requires that paramName be a NUL-terminated UTF-8 string.

     This overload is private because: (A) to keep users from
     inadvertently passing non-NUL-terminated byte arrays (an easy
     thing to do). (B) it is cheaper to NUL-terminate the
     String-to-byte-array conversion in the public-facing Java-side
     overload than to do that in C, so that signature is the
     public-facing one.
  */
  private static native int sqlite3_bind_parameter_index(
    @NotNull long ptrToStmt, @NotNull byte[] paramName
  );

  public static int sqlite3_bind_parameter_index(
    @NotNull sqlite3_stmt stmt, @NotNull String paramName
  ){
    final byte[] utf8 = nulTerminateUtf8(paramName);
    return null==utf8 ? 0 : sqlite3_bind_parameter_index(stmt.getNativePointer(), utf8);
  }

  static native String sqlite3_bind_parameter_name(
    @NotNull long ptrToStmt, int index
  );

  public static String sqlite3_bind_parameter_name(@NotNull sqlite3_stmt stmt, int index){
    return sqlite3_bind_parameter_name(stmt.getNativePointer(), index);
  }

  static native int sqlite3_bind_text(
    @NotNull long ptrToStmt, int ndx, @Nullable byte[] utf8, int maxBytes
  );

  /**
     Works like the C-level sqlite3_bind_text() but assumes
     SQLITE_TRANSIENT for the final C API parameter. The byte array is
     assumed to be in UTF-8 encoding.

     <p>Results are undefined if data is not null and
     maxBytes>=utf8.length. If maxBytes is negative then results are
     undefined if data is not null and does not contain a NUL byte.
  */
  public static int sqlite3_bind_text(
    @NotNull sqlite3_stmt stmt, int ndx, @Nullable byte[] utf8, int maxBytes
  ){
    return sqlite3_bind_text(stmt.getNativePointer(), ndx, utf8, maxBytes);
  }

  /**
     Converts data, if not null, to a UTF-8-encoded byte array and
     binds it as such, returning the result of the C-level
     sqlite3_bind_null() or sqlite3_bind_text().
  */
  public static int sqlite3_bind_text(
    @NotNull sqlite3_stmt stmt, int ndx, @Nullable String data
  ){
    if( null==data ) return sqlite3_bind_null(stmt.getNativePointer(), ndx);
    final byte[] utf8 = data.getBytes(StandardCharsets.UTF_8);
    return sqlite3_bind_text(stmt.getNativePointer(), ndx, utf8, utf8.length);
  }

  /**
     Requires that utf8 be null or in UTF-8 encoding.
  */
  public static int sqlite3_bind_text(
    @NotNull sqlite3_stmt stmt, int ndx, @Nullable byte[] utf8
  ){
    return (null == utf8)
      ? sqlite3_bind_null(stmt.getNativePointer(), ndx)
      : sqlite3_bind_text(stmt.getNativePointer(), ndx, utf8, utf8.length);
  }

  static native int sqlite3_bind_text16(
    @NotNull long ptrToStmt, int ndx, @Nullable byte[] data, int maxBytes
  );

  /**
     Identical to the sqlite3_bind_text() overload with the same
     signature but requires that its input be encoded in UTF-16 in
     platform byte order.
  */
  public static int sqlite3_bind_text16(
    @NotNull sqlite3_stmt stmt, int ndx, @Nullable byte[] data, int maxBytes
  ){
    return sqlite3_bind_text16(stmt.getNativePointer(), ndx, data, maxBytes);
  }

  /**
     Converts its string argument to UTF-16 and binds it as such, returning
     the result of the C-side function of the same name. The 3rd argument
     may be null.
  */
  public static int sqlite3_bind_text16(
    @NotNull sqlite3_stmt stmt, int ndx, @Nullable String data
  ){
    if(null == data) return sqlite3_bind_null(stmt, ndx);
    final byte[] bytes = data.getBytes(StandardCharsets.UTF_16);
    return sqlite3_bind_text16(stmt.getNativePointer(), ndx, bytes, bytes.length);
  }

  /**
     Requires that data be null or in UTF-16 encoding in platform byte
     order. Returns the result of the C-level sqlite3_bind_null() or
     sqlite3_bind_text16().
  */
  public static int sqlite3_bind_text16(
    @NotNull sqlite3_stmt stmt, int ndx, @Nullable byte[] data
  ){
    return (null == data)
      ? sqlite3_bind_null(stmt.getNativePointer(), ndx)
      : sqlite3_bind_text16(stmt.getNativePointer(), ndx, data, data.length);
  }

  static native int sqlite3_bind_value(@NotNull long ptrToStmt, int ndx, long ptrToValue);

  /**
     Functions like the C-level sqlite3_bind_value(), or
     sqlite3_bind_null() if val is null.
  */
  public static int sqlite3_bind_value(@NotNull sqlite3_stmt stmt, int ndx, sqlite3_value val){
    return sqlite3_bind_value(stmt.getNativePointer(), ndx,
                              null==val ? 0L : val.getNativePointer());
  }

  static native int sqlite3_bind_zeroblob(@NotNull long ptrToStmt, int ndx, int n);

  public static int sqlite3_bind_zeroblob(@NotNull sqlite3_stmt stmt, int ndx, int n){
    return sqlite3_bind_zeroblob(stmt.getNativePointer(), ndx, n);
  }

  static native int sqlite3_bind_zeroblob64(
    @NotNull long ptrToStmt, int ndx, long n
  );

  public static int sqlite3_bind_zeroblob64(@NotNull sqlite3_stmt stmt, int ndx, long n){
    return sqlite3_bind_zeroblob64(stmt.getNativePointer(), ndx, n);
  }

  static native int sqlite3_blob_bytes(@NotNull long ptrToBlob);

  public static int sqlite3_blob_bytes(@NotNull sqlite3_blob blob){
    return sqlite3_blob_bytes(blob.getNativePointer());
  }

  static native int sqlite3_blob_close(@Nullable long ptrToBlob);

  public static int sqlite3_blob_close(@Nullable sqlite3_blob blob){
    return sqlite3_blob_close(blob.clearNativePointer());
  }

  static native int sqlite3_blob_open(
    @NotNull long ptrToDb, @NotNull String dbName,
    @NotNull String tableName, @NotNull String columnName,
    long iRow, int flags, @NotNull OutputPointer.sqlite3_blob out
  );

  public static int sqlite3_blob_open(
    @NotNull sqlite3 db, @NotNull String dbName,
    @NotNull String tableName, @NotNull String columnName,
    long iRow, int flags, @NotNull OutputPointer.sqlite3_blob out
  ){
    return sqlite3_blob_open(db.getNativePointer(), dbName, tableName,
                             columnName, iRow, flags, out);
  }

  /**
     Convenience overload.
  */
  public static sqlite3_blob sqlite3_blob_open(
    @NotNull sqlite3 db, @NotNull String dbName,
    @NotNull String tableName, @NotNull String columnName,
    long iRow, int flags ){
    final OutputPointer.sqlite3_blob out = new OutputPointer.sqlite3_blob();
    sqlite3_blob_open(db.getNativePointer(), dbName, tableName, columnName,
                      iRow, flags, out);
    return out.take();
  };

  static native int sqlite3_blob_read(
    @NotNull long ptrToBlob, @NotNull byte[] target, int iOffset
  );

  public static int sqlite3_blob_read(
    @NotNull sqlite3_blob b, @NotNull byte[] target, int iOffset
  ){
    return sqlite3_blob_read(b.getNativePointer(), target, iOffset);
  }

  static native int sqlite3_blob_reopen(
    @NotNull long ptrToBlob, long newRowId
  );

  public static int sqlite3_blob_reopen(@NotNull sqlite3_blob b, long newRowId){
    return sqlite3_blob_reopen(b.getNativePointer(), newRowId);
  }

  static native int sqlite3_blob_write(
    @NotNull long ptrToBlob, @NotNull byte[] bytes, int iOffset
  );

  public static int sqlite3_blob_write(
    @NotNull sqlite3_blob b, @NotNull byte[] bytes, int iOffset
  ){
    return sqlite3_blob_write(b.getNativePointer(), bytes, iOffset);
  }

  static native int sqlite3_busy_handler(
    @NotNull long ptrToDb, @Nullable BusyHandlerCallback handler
  );

  /**
     As for the C-level function of the same name, with a
     BusyHandlerCallback instance in place of a callback
     function. Pass it a null handler to clear the busy handler.
  */
  public static int sqlite3_busy_handler(
    @NotNull sqlite3 db, @Nullable BusyHandlerCallback handler
  ){
    return sqlite3_busy_handler(db.getNativePointer(), handler);
  }

  static native int sqlite3_busy_timeout(@NotNull long ptrToDb, int ms);

  public static int sqlite3_busy_timeout(@NotNull sqlite3 db, int ms){
    return sqlite3_busy_timeout(db.getNativePointer(), ms);
  }

  public static native boolean sqlite3_cancel_auto_extension(
    @NotNull AutoExtensionCallback ax
  );

  static native int sqlite3_changes(@NotNull long ptrToDb);

  public static int sqlite3_changes(@NotNull sqlite3 db){
    return sqlite3_changes(db.getNativePointer());
  }

  static native long sqlite3_changes64(@NotNull long ptrToDb);

  public static long sqlite3_changes64(@NotNull sqlite3 db){
    return sqlite3_changes64(db.getNativePointer());
  }

  static native int sqlite3_clear_bindings(@NotNull long ptrToStmt);

  public static int sqlite3_clear_bindings(@NotNull sqlite3_stmt stmt){
    return sqlite3_clear_bindings(stmt.getNativePointer());
  }

  static native int sqlite3_close(@Nullable long ptrToDb);

  public static int sqlite3_close(@Nullable sqlite3 db){
    int rc = 0;
    if( null!=db ){
      rc = sqlite3_close(db.getNativePointer());
      if( 0==rc ) db.clearNativePointer();
    }
    return rc;
  }

  static native int sqlite3_close_v2(@Nullable long ptrToDb);

  public static int sqlite3_close_v2(@Nullable sqlite3 db){
    return db==null ? 0 : sqlite3_close_v2(db.clearNativePointer());
  }

  public static native byte[] sqlite3_column_blob(
    @NotNull sqlite3_stmt stmt, int ndx
  );

  static native int sqlite3_column_bytes(@NotNull long ptrToStmt, int ndx);

  public static int sqlite3_column_bytes(@NotNull sqlite3_stmt stmt, int ndx){
    return sqlite3_column_bytes(stmt.getNativePointer(), ndx);
  }

  static native int sqlite3_column_bytes16(@NotNull long ptrToStmt, int ndx);

  public static int sqlite3_column_bytes16(@NotNull sqlite3_stmt stmt, int ndx){
    return sqlite3_column_bytes16(stmt.getNativePointer(), ndx);
  }

  static native int sqlite3_column_count(@NotNull long ptrToStmt);

  public static int sqlite3_column_count(@NotNull sqlite3_stmt stmt){
    return sqlite3_column_count(stmt.getNativePointer());
  }

  static native String sqlite3_column_decltype(@NotNull long ptrToStmt, int ndx);

  public static String sqlite3_column_decltype(@NotNull sqlite3_stmt stmt, int ndx){
    return sqlite3_column_decltype(stmt.getNativePointer(), ndx);
  }

  public static native double sqlite3_column_double(
    @NotNull sqlite3_stmt stmt, int ndx
  );

  public static native int sqlite3_column_int(
    @NotNull sqlite3_stmt stmt, int ndx
  );

  public static native long sqlite3_column_int64(
    @NotNull sqlite3_stmt stmt, int ndx
  );

  static native String sqlite3_column_name(@NotNull long ptrToStmt, int ndx);

  public static String sqlite3_column_name(@NotNull sqlite3_stmt stmt, int ndx){
    return sqlite3_column_name(stmt.getNativePointer(), ndx);
  }

  static native String sqlite3_column_database_name(@NotNull long ptrToStmt, int ndx);

  public static String sqlite3_column_database_name(@NotNull sqlite3_stmt stmt, int ndx){
    return sqlite3_column_database_name(stmt.getNativePointer(), ndx);
  }

  static native String sqlite3_column_origin_name(@NotNull long ptrToStmt, int ndx);

  public static String sqlite3_column_origin_name(@NotNull sqlite3_stmt stmt, int ndx){
    return sqlite3_column_origin_name(stmt.getNativePointer(), ndx);
  }

  static native String sqlite3_column_table_name(@NotNull long ptrToStmt, int ndx);

  public static String sqlite3_column_table_name(@NotNull sqlite3_stmt stmt, int ndx){
    return sqlite3_column_table_name(stmt.getNativePointer(), ndx);
  }

  /**
     Functions identially to the C API, and this note is just to
     stress that the returned bytes are encoded as UTF-8. It returns
     null if the underlying C-level sqlite3_column_text() returns NULL
     or on allocation error.

     @see #sqlite3_column_text16(sqlite3_stmt,int)
  */
  public static native byte[] sqlite3_column_text(
    @NotNull sqlite3_stmt stmt, int ndx
  );

  public static native String sqlite3_column_text16(
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
  //       case SQLITE_TEXT: rv = sqlite3_value_text16(v); break;
  //       default: break;
  //     }
  //   }
  //   sqlite3_value_free(v);
  //   return rv;
  // }

  static native int sqlite3_column_type(@NotNull long ptrToStmt, int ndx);

  public static int sqlite3_column_type(@NotNull sqlite3_stmt stmt, int ndx){
    return sqlite3_column_type(stmt.getNativePointer(), ndx);
  }

  public static native sqlite3_value sqlite3_column_value(
    @NotNull sqlite3_stmt stmt, int ndx
  );

  static native int sqlite3_collation_needed(
    @NotNull long ptrToDb, @Nullable CollationNeededCallback callback
  );

  /**
     This functions like C's sqlite3_collation_needed16() because
     Java's string type is inherently compatible with that interface.
  */
  public static int sqlite3_collation_needed(
    @NotNull sqlite3 db, @Nullable CollationNeededCallback callback
  ){
    return sqlite3_collation_needed(db.getNativePointer(), callback);
  }

  static native CommitHookCallback sqlite3_commit_hook(
    @NotNull long ptrToDb, @Nullable CommitHookCallback hook
  );

  public static CommitHookCallback sqlite3_commit_hook(
    @NotNull sqlite3 db, @Nullable CommitHookCallback hook
  ){
    return sqlite3_commit_hook(db.getNativePointer(), hook);
  }

  public static native String sqlite3_compileoption_get(int n);

  public static native boolean sqlite3_compileoption_used(String optName);

  /**
     This implementation is private because it's too easy to pass it
     non-NUL-terminated byte arrays from client code.
  */
  private static native int sqlite3_complete(
    @NotNull byte[] nulTerminatedUtf8Sql
  );

  /**
     Unlike the C API, this returns SQLITE_MISUSE if its argument is
     null (as opposed to invoking UB).
  */
  public static int sqlite3_complete(@NotNull String sql){
    return sqlite3_complete( nulTerminateUtf8(sql) );
  }


  /**
     <p>Works like in the C API with the exception that it only supports
     the following subset of configution flags:

     <p>SQLITE_CONFIG_SINGLETHREAD
     SQLITE_CONFIG_MULTITHREAD
     SQLITE_CONFIG_SERIALIZED

     <p>Others may be added in the future. It returns SQLITE_MISUSE if
     given an argument it does not handle.

     <p>Note that sqlite3_config() is not threadsafe with regards to
     the rest of the library. This must not be called when any other
     library APIs are being called.
  */
  public static native int sqlite3_config(int op);

  /**
     If the native library was built with SQLITE_ENABLE_SQLLOG defined
     then this acts as a proxy for C's
     sqlite3_config(SQLITE_ENABLE_SQLLOG,...). This sets or clears the
     logger. If installation of a logger fails, any previous logger is
     retained.

     <p>If not built with SQLITE_ENABLE_SQLLOG defined, this returns
     SQLITE_MISUSE.

     <p>Note that sqlite3_config() is not threadsafe with regards to
     the rest of the library. This must not be called when any other
     library APIs are being called.
  */
  public static native int sqlite3_config( @Nullable ConfigSqllogCallback logger );

  /**
     The sqlite3_config() overload for handling the SQLITE_CONFIG_LOG
     option.
  */
  public static native int sqlite3_config( @Nullable ConfigLogCallback logger );

  /**
     Unlike the C API, this returns null if its argument is
     null (as opposed to invoking UB).
  */
  public static native sqlite3 sqlite3_context_db_handle(
    @NotNull sqlite3_context cx
  );

  public static native int sqlite3_create_collation(
    @NotNull sqlite3 db, @NotNull String name, int eTextRep,
    @NotNull CollationCallback col
  );

  /**
     The Java counterpart to the C-native sqlite3_create_function(),
     sqlite3_create_function_v2(), and
     sqlite3_create_window_function(). Which one it behaves like
     depends on which methods the final argument implements. See
     SQLFunction's subclasses (ScalarFunction, AggregateFunction<T>,
     and WindowFunction<T>) for details.

     <p>Unlike the C API, this returns SQLITE_MISUSE null if its db or
     functionName arguments are null (as opposed to invoking UB).
  */
  public static native int sqlite3_create_function(
    @NotNull sqlite3 db, @NotNull String functionName,
    int nArg, int eTextRep, @NotNull SQLFunction func
  );

  static native int sqlite3_data_count(@NotNull long ptrToStmt);

  public static int sqlite3_data_count(@NotNull sqlite3_stmt stmt){
    return sqlite3_data_count(stmt.getNativePointer());
  }

  /**
     Overload for sqlite3_db_config() calls which take (int,int*)
     variadic arguments. Returns SQLITE_MISUSE if op is not one of the
     SQLITE_DBCONFIG_... options which uses this call form.

     <p>Unlike the C API, this returns SQLITE_MISUSE if its db argument
     are null (as opposed to invoking UB).
  */
  public static native int sqlite3_db_config(
    @NotNull sqlite3 db, int op, int onOff, @Nullable OutputPointer.Int32 out
  );

  /**
     Overload for sqlite3_db_config() calls which take a (const char*)
     variadic argument. As of SQLite3 v3.43 the only such option is
     SQLITE_DBCONFIG_MAINDBNAME. Returns SQLITE_MISUSE if op is not
     SQLITE_DBCONFIG_MAINDBNAME, but that set of options may be
     extended in future versions.
  */
  public static native int sqlite3_db_config(
    @NotNull sqlite3 db, int op, @NotNull String val
  );

  private static native String sqlite3_db_name(@NotNull long ptrToDb, int ndx);

  public static String sqlite3_db_name(@NotNull sqlite3 db, int ndx){
    return null==db ? null : sqlite3_db_name(db.getNativePointer(), ndx);
  }


  public static native String sqlite3_db_filename(
    @NotNull sqlite3 db, @NotNull String dbName
  );

  public static native sqlite3 sqlite3_db_handle(@NotNull sqlite3_stmt stmt);

  public static native int sqlite3_db_readonly(@NotNull sqlite3 db, String dbName);

  public static native int sqlite3_db_release_memory(sqlite3 db);

  public static native int sqlite3_db_status(
    @NotNull sqlite3 db, int op, @NotNull OutputPointer.Int32 pCurrent,
    @NotNull OutputPointer.Int32 pHighwater, boolean reset
  );

  public static native int sqlite3_errcode(@NotNull sqlite3 db);

  public static native String sqlite3_errmsg(@NotNull sqlite3 db);

  static native int sqlite3_error_offset(@NotNull long ptrToDb);

  /**
     Note that the returned byte offset values assume UTF-8-encoded
     inputs, so won't always match character offsets in Java Strings.
  */
  public static int sqlite3_error_offset(@NotNull sqlite3 db){
    return sqlite3_error_offset(db.getNativePointer());
  }

  public static native String sqlite3_errstr(int resultCode);

  public static native String sqlite3_expanded_sql(@NotNull sqlite3_stmt stmt);

  static native int sqlite3_extended_errcode(@NotNull long ptrToDb);

  public static int sqlite3_extended_errcode(@NotNull sqlite3 db){
    return sqlite3_extended_errcode(db.getNativePointer());
  }

  public static native boolean sqlite3_extended_result_codes(
    @NotNull sqlite3 db, boolean onoff
  );

  static native boolean sqlite3_get_autocommit(@NotNull long ptrToDb);

  public static boolean sqlite3_get_autocommit(@NotNull sqlite3 db){
    return sqlite3_get_autocommit(db.getNativePointer());
  }

  public static native Object sqlite3_get_auxdata(
    @NotNull sqlite3_context cx, int n
  );

  static native int sqlite3_finalize(long ptrToStmt);

  public static int sqlite3_finalize(@NotNull sqlite3_stmt stmt){
    return null==stmt ? 0 : sqlite3_finalize(stmt.clearNativePointer());
  }

  public static native int sqlite3_initialize();

  public static native void sqlite3_interrupt(@NotNull sqlite3 db);

  public static native boolean sqlite3_is_interrupted(@NotNull sqlite3 db);

  public static native boolean sqlite3_keyword_check(@NotNull String word);

  public static native int sqlite3_keyword_count();

  public static native String sqlite3_keyword_name(int index);


  public static native long sqlite3_last_insert_rowid(@NotNull sqlite3 db);

  public static native String sqlite3_libversion();

  public static native int sqlite3_libversion_number();

  public static native int sqlite3_limit(@NotNull sqlite3 db, int id, int newVal);

  /**
     Only available if built with SQLITE_ENABLE_NORMALIZE. If not, it always
     returns null.
  */
  public static native String sqlite3_normalized_sql(@NotNull sqlite3_stmt stmt);

  /**
     Works like its C counterpart and makes the native pointer of the
     underling (sqlite3*) object available via
     ppDb.getNativePointer(). That pointer is necessary for looking up
     the JNI-side native, but clients need not pay it any
     heed. Passing the object to sqlite3_close() or sqlite3_close_v2()
     will clear that pointer mapping.

     <p>Recall that even if opening fails, the output pointer might be
     non-null. Any error message about the failure will be in that
     object and it is up to the caller to sqlite3_close() that
     db handle.
  */
  public static native int sqlite3_open(
    @Nullable String filename, @NotNull OutputPointer.sqlite3 ppDb
  );

  /**
     Convenience overload which returns its db handle directly. The returned
     object might not have been successfully opened: use sqlite3_errcode() to
     check whether it is in an error state.

     <p>Ownership of the returned value is passed to the caller, who must eventually
     pass it to sqlite3_close() or sqlite3_close_v2().
  */
  public static sqlite3 sqlite3_open(@Nullable String filename){
    final OutputPointer.sqlite3 out = new OutputPointer.sqlite3();
    sqlite3_open(filename, out);
    return out.take();
  };

  public static native int sqlite3_open_v2(
    @Nullable String filename, @NotNull OutputPointer.sqlite3 ppDb,
    int flags, @Nullable String zVfs
  );

  /**
     Has the same semantics as the sqlite3-returning sqlite3_open()
     but uses sqlite3_open_v2() instead of sqlite3_open().
  */
  public static sqlite3 sqlite3_open_v2(@Nullable String filename, int flags,
                                        @Nullable String zVfs){
    final OutputPointer.sqlite3 out = new OutputPointer.sqlite3();
    sqlite3_open_v2(filename, out, flags, zVfs);
    return out.take();
  };

  /**
     The sqlite3_prepare() family of functions require slightly
     different signatures than their native counterparts, but (A) they
     retain functionally equivalent semantics and (B) overloading
     allows us to install several convenience forms.

     <p>All of them which take their SQL in the form of a byte[] require
     that it be in UTF-8 encoding unless explicitly noted otherwise.

     <p>The forms which take a "tail" output pointer return (via that
     output object) the index into their SQL byte array at which the
     end of the first SQL statement processed by the call was
     found. That's fundamentally how the C APIs work but making use of
     that value requires more copying of the input SQL into
     consecutively smaller arrays in order to consume all of
     it. (There is an example of doing that in this project's Tester1
     class.) For that vast majority of uses, that capability is not
     necessary, however, and overloads are provided which gloss over
     that.

     <p>Results are undefined if maxBytes>=sqlUtf8.length.

     <p>This routine is private because its maxBytes value is not
     strictly necessary in the Java interface, as sqlUtf8.length tells
     us the information we need. Making this public would give clients
     more ways to shoot themselves in the foot without providing any
     real utility.
  */
  private static native int sqlite3_prepare(
    @NotNull long ptrToDb, @NotNull byte[] sqlUtf8, int maxBytes,
    @NotNull OutputPointer.sqlite3_stmt outStmt,
    @Nullable OutputPointer.Int32 pTailOffset
  );

  /**
     Works like the canonical sqlite3_prepare() but its "tail" output
     argument is returned as the index offset into the given
     UTF-8-encoded byte array at which SQL parsing stopped. The
     semantics are otherwise identical to the C API counterpart.

     <p>Several overloads provided simplified call signatures.
  */
  public static int sqlite3_prepare(
    @NotNull sqlite3 db, @NotNull byte[] sqlUtf8,
    @NotNull OutputPointer.sqlite3_stmt outStmt,
    @Nullable OutputPointer.Int32 pTailOffset
  ){
    return sqlite3_prepare(db.getNativePointer(), sqlUtf8, sqlUtf8.length,
                           outStmt, pTailOffset);
  }

  public static int sqlite3_prepare(
    @NotNull sqlite3 db, @NotNull byte[] sqlUtf8,
    @NotNull OutputPointer.sqlite3_stmt outStmt
  ){
    return sqlite3_prepare(db.getNativePointer(), sqlUtf8, sqlUtf8.length,
                           outStmt, null);
  }

  public static int sqlite3_prepare(
    @NotNull sqlite3 db, @NotNull String sql,
    @NotNull OutputPointer.sqlite3_stmt outStmt
  ){
    final byte[] utf8 = sql.getBytes(StandardCharsets.UTF_8);
    return sqlite3_prepare(db.getNativePointer(), utf8, utf8.length,
                           outStmt, null);
  }

  /**
     Convenience overload which returns its statement handle directly,
     or null on error or when reading only whitespace or
     comments. sqlite3_errcode() can be used to determine whether
     there was an error or the input was empty. Ownership of the
     returned object is passed to the caller, who must eventually pass
     it to sqlite3_finalize().
  */
  public static sqlite3_stmt sqlite3_prepare(
    @NotNull sqlite3 db, @NotNull String sql
  ){
    final OutputPointer.sqlite3_stmt out = new OutputPointer.sqlite3_stmt();
    sqlite3_prepare(db, sql, out);
    return out.take();
  }
  /**
     @see #sqlite3_prepare
  */
  private static native int sqlite3_prepare_v2(
    @NotNull long ptrToDb, @NotNull byte[] sqlUtf8, int maxBytes,
    @NotNull OutputPointer.sqlite3_stmt outStmt,
    @Nullable OutputPointer.Int32 pTailOffset
  );

  /**
     Works like the canonical sqlite3_prepare_v2() but its "tail"
     output paramter is returned as the index offset into the given
     byte array at which SQL parsing stopped.
  */
  public static int sqlite3_prepare_v2(
    @NotNull sqlite3 db, @NotNull byte[] sqlUtf8,
    @NotNull OutputPointer.sqlite3_stmt outStmt,
    @Nullable OutputPointer.Int32 pTailOffset
  ){
    return sqlite3_prepare_v2(db.getNativePointer(), sqlUtf8, sqlUtf8.length,
                              outStmt, pTailOffset);
  }

  public static int sqlite3_prepare_v2(
    @NotNull sqlite3 db, @NotNull byte[] sqlUtf8,
    @NotNull OutputPointer.sqlite3_stmt outStmt
  ){
    return sqlite3_prepare_v2(db.getNativePointer(), sqlUtf8, sqlUtf8.length,
                              outStmt, null);
  }

  public static int sqlite3_prepare_v2(
    @NotNull sqlite3 db, @NotNull String sql,
    @NotNull OutputPointer.sqlite3_stmt outStmt
  ){
    final byte[] utf8 = sql.getBytes(StandardCharsets.UTF_8);
    return sqlite3_prepare_v2(db.getNativePointer(), utf8, utf8.length,
                              outStmt, null);
  }

  /**
     Works identically to the sqlite3_stmt-returning sqlite3_prepare()
     but uses sqlite3_prepare_v2().
  */
  public static sqlite3_stmt sqlite3_prepare_v2(
    @NotNull sqlite3 db, @NotNull String sql
  ){
    final OutputPointer.sqlite3_stmt out = new OutputPointer.sqlite3_stmt();
    sqlite3_prepare_v2(db, sql, out);
    return out.take();
  }

  /**
     @see #sqlite3_prepare
  */
  private static native int sqlite3_prepare_v3(
    @NotNull long ptrToDb, @NotNull byte[] sqlUtf8, int maxBytes,
    int prepFlags, @NotNull OutputPointer.sqlite3_stmt outStmt,
    @Nullable OutputPointer.Int32 pTailOffset
  );

  /**
     Works like the canonical sqlite3_prepare_v2() but its "tail"
     output paramter is returned as the index offset into the given
     byte array at which SQL parsing stopped.
  */
  public static int sqlite3_prepare_v3(
    @NotNull sqlite3 db, @NotNull byte[] sqlUtf8, int prepFlags,
    @NotNull OutputPointer.sqlite3_stmt outStmt,
    @Nullable OutputPointer.Int32 pTailOffset
  ){
    return sqlite3_prepare_v3(db.getNativePointer(), sqlUtf8, sqlUtf8.length,
                              prepFlags, outStmt, pTailOffset);
  }

  /**
     Convenience overload which elides the seldom-used pTailOffset
     parameter.
  */
  public static int sqlite3_prepare_v3(
    @NotNull sqlite3 db, @NotNull byte[] sqlUtf8, int prepFlags,
    @NotNull OutputPointer.sqlite3_stmt outStmt
  ){
    return sqlite3_prepare_v3(db.getNativePointer(), sqlUtf8, sqlUtf8.length,
                              prepFlags, outStmt, null);
  }

  /**
     Convenience overload which elides the seldom-used pTailOffset
     parameter and converts the given string to UTF-8 before passing
     it on.
  */
  public static int sqlite3_prepare_v3(
    @NotNull sqlite3 db, @NotNull String sql, int prepFlags,
    @NotNull OutputPointer.sqlite3_stmt outStmt
  ){
    final byte[] utf8 = sql.getBytes(StandardCharsets.UTF_8);
    return sqlite3_prepare_v3(db.getNativePointer(), utf8, utf8.length,
                              prepFlags, outStmt, null);
  }

  /**
     Works identically to the sqlite3_stmt-returning sqlite3_prepare()
     but uses sqlite3_prepare_v3().
  */
  public static sqlite3_stmt sqlite3_prepare_v3(
    @NotNull sqlite3 db, @NotNull String sql, int prepFlags
  ){
    final OutputPointer.sqlite3_stmt out = new OutputPointer.sqlite3_stmt();
    sqlite3_prepare_v3(db, sql, prepFlags, out);
    return out.take();
  }

  /**
     A convenience wrapper around sqlite3_prepare_v3() which accepts
     an arbitrary amount of input provided as a UTF-8-encoded byte
     array.  It loops over the input bytes looking for
     statements. Each one it finds is passed to p.call(), passing
     ownership of it to that function. If p.call() returns 0, looping
     continues, else the loop stops.

     <p>If p.call() throws, the exception is propagated.

     <p>How each statement is handled, including whether it is finalized
     or not, is up to the callback object. e.g. the callback might
     collect them for later use. If it does not collect them then it
     must finalize them. See PrepareMultiCallback.Finalize for a
     simple proxy which does that.
  */
  public static int sqlite3_prepare_multi(
    @NotNull sqlite3 db, @NotNull byte[] sqlUtf8,
    int preFlags,
    @NotNull PrepareMultiCallback p){
    final OutputPointer.Int32 oTail = new OutputPointer.Int32();
    int pos = 0, n = 1;
    byte[] sqlChunk = sqlUtf8;
    int rc = 0;
    final OutputPointer.sqlite3_stmt outStmt = new OutputPointer.sqlite3_stmt();
    while(0==rc && pos<sqlChunk.length){
      sqlite3_stmt stmt = null;
      if(pos > 0){
        sqlChunk = Arrays.copyOfRange(sqlChunk, pos,
                                      sqlChunk.length);
      }
      if( 0==sqlChunk.length ) break;
      rc = sqlite3_prepare_v3(db, sqlChunk, preFlags, outStmt, oTail);
      if( 0!=rc ) break;
      pos = oTail.value;
      stmt = outStmt.take();
      if( null == stmt ){
        // empty statement was parsed.
        continue;
      }
      rc = p.call(stmt);
    }
    return rc;
  }

  /**
     Convenience overload which accepts its SQL as a String and uses
     no statement-preparation flags.
  */
  public static int sqlite3_prepare_multi(
    @NotNull sqlite3 db, @NotNull byte[] sqlUtf8,
    @NotNull PrepareMultiCallback p){
    return sqlite3_prepare_multi(db, sqlUtf8, 0, p);
  }

  /**
     Convenience overload which accepts its SQL as a String.
  */
  public static int sqlite3_prepare_multi(
    @NotNull sqlite3 db, @NotNull String sql, int prepFlags,
    @NotNull PrepareMultiCallback p){
    return sqlite3_prepare_multi(
      db, sql.getBytes(StandardCharsets.UTF_8), prepFlags, p
    );
  }

  /**
     Convenience overload which accepts its SQL as a String and uses
     no statement-preparation flags.
  */
  public static int sqlite3_prepare_multi(
    @NotNull sqlite3 db, @NotNull String sql,
    @NotNull PrepareMultiCallback p){
    return sqlite3_prepare_multi(db, sql, 0, p);
  }

  /**
     Convenience overload which accepts its SQL as a String
     array. They will be concatenated together as-is, with no
     separator, and passed on to one of the other overloads.
  */
  public static int sqlite3_prepare_multi(
    @NotNull sqlite3 db, @NotNull String[] sql, int prepFlags,
    @NotNull PrepareMultiCallback p){
    return sqlite3_prepare_multi(db, String.join("",sql), prepFlags, p);
  }

  /**
     Convenience overload which uses no statement-preparation flags.
  */
  public static int sqlite3_prepare_multi(
    @NotNull sqlite3 db, @NotNull String[] sql,
    @NotNull PrepareMultiCallback p){
    return sqlite3_prepare_multi(db, sql, 0, p);
  }

  static native int sqlite3_preupdate_blobwrite(@NotNull long ptrToDb);

  /**
     If the C API was built with SQLITE_ENABLE_PREUPDATE_HOOK defined, this
     acts as a proxy for C's sqlite3_preupdate_blobwrite(), else it returns
     SQLITE_MISUSE with no side effects.
  */
  public static int sqlite3_preupdate_blobwrite(@NotNull sqlite3 db){
    return sqlite3_preupdate_blobwrite(db.getNativePointer());
  }

  static native int sqlite3_preupdate_count(@NotNull long ptrToDb);

  /**
     If the C API was built with SQLITE_ENABLE_PREUPDATE_HOOK defined, this
     acts as a proxy for C's sqlite3_preupdate_count(), else it returns
     SQLITE_MISUSE with no side effects.
  */
  public static int sqlite3_preupdate_count(@NotNull sqlite3 db){
    return sqlite3_preupdate_count(db.getNativePointer());
  }

  static native int sqlite3_preupdate_depth(@NotNull long ptrToDb);

  /**
     If the C API was built with SQLITE_ENABLE_PREUPDATE_HOOK defined, this
     acts as a proxy for C's sqlite3_preupdate_depth(), else it returns
     SQLITE_MISUSE with no side effects.
  */
  public static int sqlite3_preupdate_depth(@NotNull sqlite3 db){
    return sqlite3_preupdate_depth(db.getNativePointer());
  }

  static native PreupdateHookCallback sqlite3_preupdate_hook(
    @NotNull long ptrToDb, @Nullable PreupdateHookCallback hook
  );

  /**
     If the C API was built with SQLITE_ENABLE_PREUPDATE_HOOK defined, this
     acts as a proxy for C's sqlite3_preupdate_hook(), else it returns null
     with no side effects.
  */
  public static PreupdateHookCallback sqlite3_preupdate_hook(
    @NotNull sqlite3 db, @Nullable PreupdateHookCallback hook
  ){
    return sqlite3_preupdate_hook(db.getNativePointer(), hook);
  }

  static native int sqlite3_preupdate_new(@NotNull long ptrToDb, int col,
                                                 @NotNull OutputPointer.sqlite3_value out);

  /**
     If the C API was built with SQLITE_ENABLE_PREUPDATE_HOOK defined,
     this acts as a proxy for C's sqlite3_preupdate_new(), else it
     returns SQLITE_MISUSE with no side effects.
  */
  public static int sqlite3_preupdate_new(@NotNull sqlite3 db, int col,
                                          @NotNull OutputPointer.sqlite3_value out){
    return sqlite3_preupdate_new(db.getNativePointer(), col, out);
  }

  /**
     Convenience wrapper for the 3-arg sqlite3_preupdate_new() which returns
     null on error.
  */
  public static sqlite3_value sqlite3_preupdate_new(@NotNull sqlite3 db, int col){
    final OutputPointer.sqlite3_value out = new OutputPointer.sqlite3_value();
    sqlite3_preupdate_new(db.getNativePointer(), col, out);
    return out.take();
  }

  static native int sqlite3_preupdate_old(@NotNull long ptrToDb, int col,
                                                 @NotNull OutputPointer.sqlite3_value out);

  /**
     If the C API was built with SQLITE_ENABLE_PREUPDATE_HOOK defined,
     this acts as a proxy for C's sqlite3_preupdate_old(), else it
     returns SQLITE_MISUSE with no side effects.
  */
  public static int sqlite3_preupdate_old(@NotNull sqlite3 db, int col,
                                          @NotNull OutputPointer.sqlite3_value out){
    return sqlite3_preupdate_old(db.getNativePointer(), col, out);
  }

  /**
     Convenience wrapper for the 3-arg sqlite3_preupdate_old() which returns
     null on error.
  */
  public static sqlite3_value sqlite3_preupdate_old(@NotNull sqlite3 db, int col){
    final OutputPointer.sqlite3_value out = new OutputPointer.sqlite3_value();
    sqlite3_preupdate_old(db.getNativePointer(), col, out);
    return out.take();
  }

  public static native void sqlite3_progress_handler(
    @NotNull sqlite3 db, int n, @Nullable ProgressHandlerCallback h
  );

  public static native void sqlite3_randomness(byte[] target);

  public static native int sqlite3_release_memory(int n);

  public static native int sqlite3_reset(@NotNull sqlite3_stmt stmt);

  /**
     Works like the C API except that it has no side effects if auto
     extensions are currently running. (The JNI-level list of
     extensions cannot be manipulated while it is being traversed.)
  */
  public static native void sqlite3_reset_auto_extension();

  public static native void sqlite3_result_double(
    @NotNull sqlite3_context cx, double v
  );

  /**
     The main sqlite3_result_error() impl of which all others are
     proxies. eTextRep must be one of SQLITE_UTF8 or SQLITE_UTF16 and
     msg must be encoded correspondingly. Any other eTextRep value
     results in the C-level sqlite3_result_error() being called with a
     complaint about the invalid argument.
  */
  static native void sqlite3_result_error(
    @NotNull sqlite3_context cx, @NotNull byte[] msg, int eTextRep
  );

  public static void sqlite3_result_error(
    @NotNull sqlite3_context cx, @NotNull byte[] utf8
  ){
    sqlite3_result_error(cx, utf8, SQLITE_UTF8);
  }

  public static void sqlite3_result_error(
    @NotNull sqlite3_context cx, @NotNull String msg
  ){
    final byte[] utf8 = msg.getBytes(StandardCharsets.UTF_8);
    sqlite3_result_error(cx, utf8, SQLITE_UTF8);
  }

  public static void sqlite3_result_error16(
    @NotNull sqlite3_context cx, @NotNull byte[] utf16
  ){
    sqlite3_result_error(cx, utf16, SQLITE_UTF16);
  }

  public static void sqlite3_result_error16(
    @NotNull sqlite3_context cx, @NotNull String msg
  ){
    final byte[] utf16 = msg.getBytes(StandardCharsets.UTF_16);
    sqlite3_result_error(cx, utf16, SQLITE_UTF16);
  }

  /**
     Equivalent to passing e.toString() to {@link
     #sqlite3_result_error(sqlite3_context,String)}.  Note that
     toString() is used instead of getMessage() because the former
     prepends the exception type name to the message.
  */
  public static void sqlite3_result_error(
    @NotNull sqlite3_context cx, @NotNull Exception e
  ){
    sqlite3_result_error(cx, e.toString());
  }

  public static native void sqlite3_result_error_toobig(
    @NotNull sqlite3_context cx
  );

  public static native void sqlite3_result_error_nomem(
    @NotNull sqlite3_context cx
  );

  public static native void sqlite3_result_error_code(
    @NotNull sqlite3_context cx, int c
  );

  public static native void sqlite3_result_null(
    @NotNull sqlite3_context cx
  );

  public static native void sqlite3_result_int(
    @NotNull sqlite3_context cx, int v
  );

  public static native void sqlite3_result_int64(
    @NotNull sqlite3_context cx, long v
  );

  /**
     Binds the SQL result to the given object, or {@link
     #sqlite3_result_null} if {@code o} is null. Use {@link
     #sqlite3_value_java_object} to fetch it.

     <p>This is implemented in terms of C's sqlite3_result_pointer(),
     but that function is not exposed to JNI because (A)
     cross-language semantic mismatch and (B) Java doesn't need that
     argument for its intended purpose (type safety).

     <p>Note that there is no sqlite3_column_java_object(), as the
     C-level API has no sqlite3_column_pointer() to proxy.

     @see #sqlite3_value_java_object
     @see #sqlite3_bind_java_object
  */
  public static native void sqlite3_result_java_object(
    @NotNull sqlite3_context cx, @NotNull Object o
  );

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
    @NotNull sqlite3_context cx, @NotNull Integer v
  ){
    sqlite3_result_int(cx, v);
  }

  public static void sqlite3_result_set(@NotNull sqlite3_context cx, int v){
    sqlite3_result_int(cx, v);
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
    @NotNull sqlite3_context cx, @Nullable String v
  ){
    if( null==v ) sqlite3_result_null(cx);
    else sqlite3_result_text(cx, v);
  }

  public static void sqlite3_result_set(
    @NotNull sqlite3_context cx, @Nullable byte[] blob
  ){
    if( null==blob ) sqlite3_result_null(cx);
    else sqlite3_result_blob(cx, blob, blob.length);
  }

  public static native void sqlite3_result_value(
    @NotNull sqlite3_context cx, @NotNull sqlite3_value v
  );

  public static native void sqlite3_result_zeroblob(
    @NotNull sqlite3_context cx, int n
  );

  public static native int sqlite3_result_zeroblob64(
    @NotNull sqlite3_context cx, long n
  );

  /**
     This overload is private because its final parameter is arguably
     unnecessary in Java.
  */
  private static native void sqlite3_result_blob(
    @NotNull sqlite3_context cx, @Nullable byte[] blob, int maxLen
  );

  public static void sqlite3_result_blob(
    @NotNull sqlite3_context cx, @Nullable byte[] blob
  ){
    sqlite3_result_blob(cx, blob, (int)(null==blob ? 0 : blob.length));
  }

  /**
     Binds the given text using C's sqlite3_result_blob64() unless:

     <ul>

     <li>@param blob is null: translates to sqlite3_result_null()</li>

     <li>@param blob is too large: translates to
     sqlite3_result_error_toobig()</li>

     </ul>

     <p>If @param maxLen is larger than blob.length, it is truncated
     to that value. If it is negative, results are undefined.</p>

     <p>This overload is private because its final parameter is
     arguably unnecessary in Java.</p>
  */
  private static native void sqlite3_result_blob64(
    @NotNull sqlite3_context cx, @Nullable byte[] blob, long maxLen
  );

  public static void sqlite3_result_blob64(
    @NotNull sqlite3_context cx, @Nullable byte[] blob
  ){
    sqlite3_result_blob64(cx, blob, (long)(null==blob ? 0 : blob.length));
  }

  /**
     This overload is private because its final parameter is
     arguably unnecessary in Java.
  */
  private static native void sqlite3_result_text(
    @NotNull sqlite3_context cx, @Nullable byte[] utf8, int maxLen
  );

  public static void sqlite3_result_text(
    @NotNull sqlite3_context cx, @Nullable byte[] utf8
  ){
    sqlite3_result_text(cx, utf8, null==utf8 ? 0 : utf8.length);
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

     <ul>

     <li>text is null: translates to a call to sqlite3_result_null()</li>

     <li>text is too large: translates to a call to
     {@link #sqlite3_result_error_toobig}</li>

     <li>The @param encoding argument has an invalid value: translates to
     {@link sqlite3_result_error_code} with code SQLITE_FORMAT.</li>

     </ul>

     If maxLength (in bytes, not characters) is larger than
     text.length, it is silently truncated to text.length. If it is
     negative, results are undefined. If text is null, the subsequent
     arguments are ignored.

     This overload is private because its maxLength parameter is
     arguably unnecessary in Java.
  */
  private static native void sqlite3_result_text64(
    @NotNull sqlite3_context cx, @Nullable byte[] text,
    long maxLength, int encoding
  );

  /**
     Sets the current UDF result to the given bytes, which are assumed
     be encoded in UTF-16 using the platform's byte order.
  */
  public static void sqlite3_result_text16(
    @NotNull sqlite3_context cx, @Nullable byte[] utf16
  ){
    sqlite3_result_text64(cx, utf16, utf16.length, SQLITE_UTF16);
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

  static native RollbackHookCallback sqlite3_rollback_hook(
    @NotNull long ptrToDb, @Nullable RollbackHookCallback hook
  );

  public static RollbackHookCallback sqlite3_rollback_hook(
    @NotNull sqlite3 db, @Nullable RollbackHookCallback hook
  ){
    return sqlite3_rollback_hook(db.getNativePointer(), hook);
  }

  public static native int sqlite3_set_authorizer(
    @NotNull sqlite3 db, @Nullable AuthorizerCallback auth
  );

  public static native void sqlite3_set_auxdata(
    @NotNull sqlite3_context cx, int n, @Nullable Object data
  );

  public static native void sqlite3_set_last_insert_rowid(
    @NotNull sqlite3 db, long rowid
  );


  /**
     In addition to calling the C-level sqlite3_shutdown(), the JNI
     binding also cleans up all stale per-thread state managed by the
     library, as well as any registered auto-extensions, and frees up
     various bits of memory. Calling this while database handles or
     prepared statements are still active will leak resources. Trying
     to use those objects after this routine is called invoked
     undefined behavior.
  */
  public static synchronized native int sqlite3_shutdown();

  public static native int sqlite3_sleep(int ms);

  public static native String sqlite3_sourceid();

  public static native String sqlite3_sql(@NotNull sqlite3_stmt stmt);

  //! Consider removing this. We can use sqlite3_status64() instead,
  // or use that one's impl with this one's name.
  public static native int sqlite3_status(
    int op, @NotNull OutputPointer.Int32 pCurrent,
    @NotNull OutputPointer.Int32 pHighwater, boolean reset
  );

  public static native int sqlite3_status64(
    int op, @NotNull OutputPointer.Int64 pCurrent,
    @NotNull OutputPointer.Int64 pHighwater, boolean reset
  );

  public static native int sqlite3_step(@NotNull sqlite3_stmt stmt);

  public static native boolean sqlite3_stmt_busy(@NotNull sqlite3_stmt stmt);

  static native int sqlite3_stmt_explain(@NotNull long ptrToStmt, int op);

  public static int sqlite3_stmt_explain(@NotNull sqlite3_stmt stmt, int op){
    return sqlite3_stmt_explain(stmt.getNativePointer(), op);
  }

  static native int sqlite3_stmt_isexplain(@NotNull long ptrToStmt);

  public static int sqlite3_stmt_isexplain(@NotNull sqlite3_stmt stmt){
    return sqlite3_stmt_isexplain(stmt.getNativePointer());
  }

  public static native boolean sqlite3_stmt_readonly(@NotNull sqlite3_stmt stmt);

  public static native int sqlite3_stmt_status(
    @NotNull sqlite3_stmt stmt, int op, boolean reset
  );

  /**
     Internal impl of the public sqlite3_strglob() method. Neither
     argument may be null and both must be NUL-terminated UTF-8.

     This overload is private because: (A) to keep users from
     inadvertently passing non-NUL-terminated byte arrays (an easy
     thing to do). (B) it is cheaper to NUL-terminate the
     String-to-byte-array conversion in the Java implementation
     (sqlite3_strglob(String,String)) than to do that in C, so that
     signature is the public-facing one.
  */
  private static native int sqlite3_strglob(
    @NotNull byte[] glob, @NotNull byte[] nullTerminatedUtf8
  );

  public static int sqlite3_strglob(
    @NotNull String glob, @NotNull String txt
  ){
    return sqlite3_strglob(nulTerminateUtf8(glob),
                           nulTerminateUtf8(txt));
  }

  /**
     The LIKE counterpart of the private sqlite3_strglob() method.
  */
  private static native int sqlite3_strlike(
    @NotNull byte[] glob, @NotNull byte[] nullTerminatedUtf8,
    int escChar
  );

  public static int sqlite3_strlike(
    @NotNull String glob, @NotNull String txt, char escChar
  ){
    return sqlite3_strlike(nulTerminateUtf8(glob),
                           nulTerminateUtf8(txt),
                           (int)escChar);
  }

  static native int sqlite3_system_errno(@NotNull long ptrToDb);

  public static int sqlite3_system_errno(@NotNull sqlite3 db){
    return sqlite3_system_errno(db.getNativePointer());
  }

  public static native int sqlite3_table_column_metadata(
    @NotNull sqlite3 db, @NotNull String zDbName,
    @NotNull String zTableName, @NotNull String zColumnName,
    @Nullable OutputPointer.String pzDataType,
    @Nullable OutputPointer.String pzCollSeq,
    @Nullable OutputPointer.Bool pNotNull,
    @Nullable OutputPointer.Bool pPrimaryKey,
    @Nullable OutputPointer.Bool pAutoinc
  );

  /**
     Convenience overload which returns its results via a single
     output object. If this function returns non-0 (error), the the
     contents of the output object are not modified.
  */
  public static int sqlite3_table_column_metadata(
    @NotNull sqlite3 db, @NotNull String zDbName,
    @NotNull String zTableName, @NotNull String zColumnName,
    @NotNull TableColumnMetadata out){
    return sqlite3_table_column_metadata(
      db, zDbName, zTableName, zColumnName,
      out.pzDataType, out.pzCollSeq, out.pNotNull,
      out.pPrimaryKey, out.pAutoinc);
  }

  /**
     Convenience overload which returns the column metadata object on
     success and null on error.
  */
  public static TableColumnMetadata sqlite3_table_column_metadata(
    @NotNull sqlite3 db, @NotNull String zDbName,
    @NotNull String zTableName, @NotNull String zColumnName){
    final TableColumnMetadata out = new TableColumnMetadata();
    return 0==sqlite3_table_column_metadata(
      db, zDbName, zTableName, zColumnName, out
    ) ? out : null;
  }

  public static native int sqlite3_threadsafe();

  static native int sqlite3_total_changes(@NotNull long ptrToDb);

  public static int sqlite3_total_changes(@NotNull sqlite3 db){
    return sqlite3_total_changes(db.getNativePointer());
  }

  static native long sqlite3_total_changes64(@NotNull long ptrToDb);

  public static long sqlite3_total_changes64(@NotNull sqlite3 db){
    return sqlite3_total_changes64(db.getNativePointer());
  }

  /**
     Works like C's sqlite3_trace_v2() except that the 3rd argument to that
     function is elided here because the roles of that functions' 3rd and 4th
     arguments are encapsulated in the final argument to this function.

     <p>Unlike the C API, which is documented as always returning 0,
     this implementation returns non-0 if initialization of the tracer
     mapping state fails (e.g. on OOM).
  */
  public static native int sqlite3_trace_v2(
    @NotNull sqlite3 db, int traceMask, @Nullable TraceV2Callback tracer
  );

  public static native int sqlite3_txn_state(
    @NotNull sqlite3 db, @Nullable String zSchema
  );

  static native UpdateHookCallback sqlite3_update_hook(
    @NotNull long ptrToDb, @Nullable UpdateHookCallback hook
  );

  public static UpdateHookCallback sqlite3_update_hook(
    @NotNull sqlite3 db, @Nullable UpdateHookCallback hook
  ){
    return sqlite3_update_hook(db.getNativePointer(), hook);
  }

  /*
     Note that:

     void * sqlite3_user_data(sqlite3_context*)

     Is not relevant in the JNI binding, as its feature is replaced by
     the ability to pass an object, including any relevant state, to
     sqlite3_create_function().
  */

  static native byte[] sqlite3_value_blob(@NotNull long ptrToValue);

  public static byte[] sqlite3_value_blob(@NotNull sqlite3_value v){
    return sqlite3_value_blob(v.getNativePointer());
  }

  static native int sqlite3_value_bytes(@NotNull long ptrToValue);

  public static int sqlite3_value_bytes(@NotNull sqlite3_value v){
    return sqlite3_value_bytes(v.getNativePointer());
  }

  static native int sqlite3_value_bytes16(@NotNull long ptrToValue);

  public static int sqlite3_value_bytes16(@NotNull sqlite3_value v){
    return sqlite3_value_bytes16(v.getNativePointer());
  }

  static native double sqlite3_value_double(@NotNull long ptrToValue);

  public static double sqlite3_value_double(@NotNull sqlite3_value v){
    return sqlite3_value_double(v.getNativePointer());
  }

  static native sqlite3_value sqlite3_value_dup(@NotNull long ptrToValue);

  public static sqlite3_value sqlite3_value_dup(@NotNull sqlite3_value v){
    return sqlite3_value_dup(v.getNativePointer());
  }

  static native int sqlite3_value_encoding(@NotNull long ptrToValue);

  public static int sqlite3_value_encoding(@NotNull sqlite3_value v){
    return sqlite3_value_encoding(v.getNativePointer());
  }

  static native void sqlite3_value_free(@Nullable long ptrToValue);

  public static void sqlite3_value_free(@Nullable sqlite3_value v){
    sqlite3_value_free(v.getNativePointer());
  }

  static native boolean sqlite3_value_frombind(@NotNull long ptrToValue);

  public static boolean sqlite3_value_frombind(@NotNull sqlite3_value v){
    return sqlite3_value_frombind(v.getNativePointer());
  }

  static native int sqlite3_value_int(@NotNull long ptrToValue);

  public static int sqlite3_value_int(@NotNull sqlite3_value v){
    return sqlite3_value_int(v.getNativePointer());
  }

  static native long sqlite3_value_int64(@NotNull long ptrToValue);

  public static long sqlite3_value_int64(@NotNull sqlite3_value v){
    return sqlite3_value_int64(v.getNativePointer());
  }

  static native Object sqlite3_value_java_object(@NotNull long ptrToValue);

  /**
     If the given value was set using {@link
     #sqlite3_result_java_object} then this function returns that
     object, else it returns null.

     <p>It is up to the caller to inspect the object to determine its
     type, and cast it if necessary.
  */
  public static Object sqlite3_value_java_object(@NotNull sqlite3_value v){
    return sqlite3_value_java_object(v.getNativePointer());
  }

  /**
     A variant of sqlite3_value_java_object() which returns the
     fetched object cast to T if the object is an instance of the
     given Class, else it returns null.
  */
  @SuppressWarnings("unchecked")
  public static <T> T sqlite3_value_java_casted(@NotNull sqlite3_value v,
                                                @NotNull Class<T> type){
    final Object o = sqlite3_value_java_object(v);
    return type.isInstance(o) ? (T)o : null;
  }

  static native int sqlite3_value_nochange(@NotNull long ptrToValue);

  public static int sqlite3_value_nochange(@NotNull sqlite3_value v){
    return sqlite3_value_nochange(v.getNativePointer());
  }

  static native int sqlite3_value_numeric_type(@NotNull long ptrToValue);

  public static int sqlite3_value_numeric_type(@NotNull sqlite3_value v){
    return sqlite3_value_numeric_type(v.getNativePointer());
  }

  static native int sqlite3_value_subtype(@NotNull long ptrToValue);

  public static int sqlite3_value_subtype(@NotNull sqlite3_value v){
    return sqlite3_value_subtype(v.getNativePointer());
  }

  static native byte[] sqlite3_value_text(@NotNull long ptrToValue);

  /**
     Functions identially to the C API, and this note is just to
     stress that the returned bytes are encoded as UTF-8. It returns
     null if the underlying C-level sqlite3_value_text() returns NULL
     or on allocation error.
  */
  public static byte[] sqlite3_value_text(@NotNull sqlite3_value v){
    return sqlite3_value_text(v.getNativePointer());
  }

  static native String sqlite3_value_text16(@NotNull long ptrToValue);

  public static String sqlite3_value_text16(@NotNull sqlite3_value v){
    return sqlite3_value_text16(v.getNativePointer());
  }

  static native int sqlite3_value_type(@NotNull long ptrToValue);

  public static int sqlite3_value_type(@NotNull sqlite3_value v){
    return sqlite3_value_type(v.getNativePointer());
  }

  /**
     This is NOT part of the public API. It exists solely as a place
     for this code's developers to collect internal metrics and such.
     It has no stable interface. It may go way or change behavior at
     any time.
  */
  public static native void sqlite3_jni_internal_details();

  //////////////////////////////////////////////////////////////////////
  // SQLITE_... constants follow...

  // version info
  public static final int SQLITE_VERSION_NUMBER = sqlite3_libversion_number();
  public static final String SQLITE_VERSION = sqlite3_libversion();
  public static final String SQLITE_SOURCE_ID = sqlite3_sourceid();

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

  // limits
  public static final int SQLITE_LIMIT_LENGTH = 0;
  public static final int SQLITE_LIMIT_SQL_LENGTH = 1;
  public static final int SQLITE_LIMIT_COLUMN = 2;
  public static final int SQLITE_LIMIT_EXPR_DEPTH = 3;
  public static final int SQLITE_LIMIT_COMPOUND_SELECT = 4;
  public static final int SQLITE_LIMIT_VDBE_OP = 5;
  public static final int SQLITE_LIMIT_FUNCTION_ARG = 6;
  public static final int SQLITE_LIMIT_ATTACHED = 7;
  public static final int SQLITE_LIMIT_LIKE_PATTERN_LENGTH = 8;
  public static final int SQLITE_LIMIT_VARIABLE_NUMBER = 9;
  public static final int SQLITE_LIMIT_TRIGGER_DEPTH = 10;
  public static final int SQLITE_LIMIT_WORKER_THREADS = 11;

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
