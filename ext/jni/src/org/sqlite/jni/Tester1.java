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
** This file contains a set of tests for the sqlite3 JNI bindings.
*/
package org.sqlite.jni;
import static org.sqlite.jni.SQLite3Jni.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class Tester1 {
  private static final class Metrics {
    int dbOpen;
  }

  static final Metrics metrics = new Metrics();
  private static final OutputPointer.sqlite3_stmt outStmt
    = new OutputPointer.sqlite3_stmt();

  public static void out(Object val){
    System.out.print(val);
  }

  public static void outln(Object val){
    System.out.println(val);
  }

  @SuppressWarnings("unchecked")
  public static void out(Object... vals){
    int n = 0;
    for(Object v : vals) out((n++>0 ? " " : "")+v);
  }

  @SuppressWarnings("unchecked")
  public static void outln(Object... vals){
    out(vals); out("\n");
  }

  static int affirmCount = 0;
  public static void affirm(Boolean v){
    ++affirmCount;
    assert( v /* prefer assert over exception if it's enabled because
                 the JNI layer sometimes has to suppress exceptions. */);
    if( !v ) throw new RuntimeException("Assertion failed.");
  }

  private static void test1(){
    outln("libversion_number:",
          sqlite3_libversion_number()
          + "\n"
          + sqlite3_libversion()
          + "\n"
          + SQLITE_SOURCE_ID);
    affirm(sqlite3_libversion_number() == SQLITE_VERSION_NUMBER);
    //outln("threadsafe = "+sqlite3_threadsafe());
    affirm(SQLITE_MAX_LENGTH > 0);
    affirm(SQLITE_MAX_TRIGGER_DEPTH>0);
  }

  public static sqlite3 createNewDb(){
    final OutputPointer.sqlite3 out = new OutputPointer.sqlite3();
    int rc = sqlite3_open(":memory:", out);
    ++metrics.dbOpen;
    sqlite3 db = out.take();
    if( 0!=rc ){
      final String msg = db.getNativePointer()==0
        ? sqlite3_errstr(rc)
        : sqlite3_errmsg(db);
      throw new RuntimeException("Opening db failed: "+msg);
    }
    affirm( null == out.get() );
    affirm( 0 != db.getNativePointer() );
    rc = sqlite3_busy_timeout(db, 2000);
    affirm( 0 == rc );
    return db;
  }

  public static void execSql(sqlite3 db, String[] sql){
    execSql(db, String.join("", sql));
  }

  public static int execSql(sqlite3 db, boolean throwOnError, String sql){
    OutputPointer.Int32 oTail = new OutputPointer.Int32();
    final byte[] sqlUtf8 = sql.getBytes(StandardCharsets.UTF_8);
    int pos = 0, n = 1;
    byte[] sqlChunk = sqlUtf8;
    int rc = 0;
    sqlite3_stmt stmt = null;
    while(pos < sqlChunk.length){
      if(pos > 0){
        sqlChunk = Arrays.copyOfRange(sqlChunk, pos,
                                      sqlChunk.length);
      }
      if( 0==sqlChunk.length ) break;
      rc = sqlite3_prepare_v2(db, sqlChunk, outStmt, oTail);
      if(throwOnError) affirm(0 == rc);
      else if( 0!=rc ) break;
      pos = oTail.value;
      stmt = outStmt.take();
      if( null == stmt ){
        // empty statement was parsed.
        continue;
      }
      affirm(0 != stmt.getNativePointer());
      while( SQLITE_ROW == (rc = sqlite3_step(stmt)) ){
      }
      sqlite3_finalize(stmt);
      affirm(0 == stmt.getNativePointer());
      if(0!=rc && SQLITE_ROW!=rc && SQLITE_DONE!=rc){
        break;
      }
    }
    sqlite3_finalize(stmt);
    if(SQLITE_ROW==rc || SQLITE_DONE==rc) rc = 0;
    if( 0!=rc && throwOnError){
      throw new RuntimeException("db op failed with rc="
                                 +rc+": "+sqlite3_errmsg(db));
    }
    return rc;
  }

  public static void execSql(sqlite3 db, String sql){
    execSql(db, true, sql);
  }

  public static sqlite3_stmt prepare(sqlite3 db, String sql){
    outStmt.clear();
    int rc = sqlite3_prepare(db, sql, outStmt);
    affirm( 0 == rc );
    final sqlite3_stmt rv = outStmt.take();
    affirm( null == outStmt.get() );
    affirm( 0 != rv.getNativePointer() );
    return rv;
  }

  private static void testCompileOption(){
    int i = 0;
    String optName;
    outln("compile options:");
    for( ; null != (optName = sqlite3_compileoption_get(i)); ++i){
      outln("\t"+optName+"\t (used="+
            sqlite3_compileoption_used(optName)+")");
    }

  }

  private static void testOpenDb1(){
    final OutputPointer.sqlite3 out = new OutputPointer.sqlite3();
    int rc = sqlite3_open(":memory:", out);
    ++metrics.dbOpen;
    sqlite3 db = out.get();
    affirm(0 == rc);
    affirm(0 < db.getNativePointer());
    sqlite3_db_config(db, SQLITE_DBCONFIG_DEFENSIVE, 1, null)
      /* This function has different mangled names in jdk8 vs jdk19,
         and this call is here to ensure that the build fails
         if it cannot find both names. */;
    sqlite3_close_v2(db);
    affirm(0 == db.getNativePointer());
  }

  private static void testOpenDb2(){
    final OutputPointer.sqlite3 out = new OutputPointer.sqlite3();
    int rc = sqlite3_open_v2(":memory:", out,
                             SQLITE_OPEN_READWRITE
                             | SQLITE_OPEN_CREATE, null);
    ++metrics.dbOpen;
    affirm(0 == rc);
    sqlite3 db = out.get();
    affirm(0 < db.getNativePointer());
    sqlite3_close_v2(db);
    affirm(0 == db.getNativePointer());
  }

  private static void testPrepare123(){
    sqlite3 db = createNewDb();
    int rc;
    rc = sqlite3_prepare(db, "CREATE TABLE t1(a);", outStmt);
    affirm(0 == rc);
    sqlite3_stmt stmt = outStmt.get();
    affirm(0 != stmt.getNativePointer());
    rc = sqlite3_step(stmt);
    affirm(SQLITE_DONE == rc);
    sqlite3_finalize(stmt);
    affirm(0 == stmt.getNativePointer());

    { /* Demonstrate how to use the "zTail" option of
         sqlite3_prepare() family of functions. */
      OutputPointer.Int32 oTail = new OutputPointer.Int32();
      final byte[] sqlUtf8 =
        "CREATE TABLE t2(a); INSERT INTO t2(a) VALUES(1),(2),(3)"
        .getBytes(StandardCharsets.UTF_8);
      int pos = 0, n = 1;
      byte[] sqlChunk = sqlUtf8;
      while(pos < sqlChunk.length){
        if(pos > 0){
          sqlChunk = Arrays.copyOfRange(sqlChunk, pos, sqlChunk.length);
        }
        //outln("SQL chunk #"+n+" length = "+sqlChunk.length+", pos = "+pos);
        if( 0==sqlChunk.length ) break;
        rc = sqlite3_prepare_v2(db, sqlChunk, outStmt, oTail);
        affirm(0 == rc);
        stmt = outStmt.get();
        pos = oTail.value;
        /*outln("SQL tail pos = "+pos+". Chunk = "+
              (new String(Arrays.copyOfRange(sqlChunk,0,pos),
              StandardCharsets.UTF_8)));*/
        switch(n){
          case 1: affirm(19 == pos); break;
          case 2: affirm(36 == pos); break;
          default: affirm( false /* can't happen */ );

        }
        ++n;
        affirm(0 != stmt.getNativePointer());
        rc = sqlite3_step(stmt);
        affirm(SQLITE_DONE == rc);
        sqlite3_finalize(stmt);
        affirm(0 == stmt.getNativePointer());
      }
    }


    rc = sqlite3_prepare_v3(db, "INSERT INTO t2(a) VALUES(1),(2),(3)",
                            SQLITE_PREPARE_NORMALIZE, outStmt);
    affirm(0 == rc);
    stmt = outStmt.get();
    affirm(0 != stmt.getNativePointer());
    sqlite3_finalize(stmt);
    affirm(0 == stmt.getNativePointer() );
    sqlite3_close_v2(db);
  }

  private static void testBindFetchInt(){
    sqlite3 db = createNewDb();
    execSql(db, "CREATE TABLE t(a)");

    sqlite3_stmt stmt = prepare(db, "INSERT INTO t(a) VALUES(:a);");
    affirm(1 == sqlite3_bind_parameter_count(stmt));
    final int paramNdx = sqlite3_bind_parameter_index(stmt, ":a");
    affirm(1 == paramNdx);
    int total1 = 0;
    long rowid = -1;
    int changes = sqlite3_changes(db);
    int changesT = sqlite3_total_changes(db);
    long changes64 = sqlite3_changes64(db);
    long changesT64 = sqlite3_total_changes64(db);
    int rc;
    for(int i = 99; i < 102; ++i ){
      total1 += i;
      rc = sqlite3_bind_int(stmt, paramNdx, i);
      affirm(0 == rc);
      rc = sqlite3_step(stmt);
      sqlite3_reset(stmt);
      affirm(SQLITE_DONE == rc);
      long x = sqlite3_last_insert_rowid(db);
      affirm(x > rowid);
      rowid = x;
    }
    sqlite3_finalize(stmt);
    affirm(300 == total1);
    affirm(sqlite3_changes(db) > changes);
    affirm(sqlite3_total_changes(db) > changesT);
    affirm(sqlite3_changes64(db) > changes64);
    affirm(sqlite3_total_changes64(db) > changesT64);
    stmt = prepare(db, "SELECT a FROM t ORDER BY a DESC;");
    int total2 = 0;
    while( SQLITE_ROW == sqlite3_step(stmt) ){
      total2 += sqlite3_column_int(stmt, 0);
      sqlite3_value sv = sqlite3_column_value(stmt, 0);
      affirm( null != sv );
      affirm( 0 != sv.getNativePointer() );
      affirm( SQLITE_INTEGER == sqlite3_value_type(sv) );
    }
    sqlite3_finalize(stmt);
    affirm(total1 == total2);
    sqlite3_close_v2(db);
    affirm(0 == db.getNativePointer());
  }

  private static void testBindFetchInt64(){
    sqlite3 db = createNewDb();
    execSql(db, "CREATE TABLE t(a)");
    sqlite3_stmt stmt = prepare(db, "INSERT INTO t(a) VALUES(?);");
    long total1 = 0;
    for(long i = 0xffffffff; i < 0xffffffff + 3; ++i ){
      total1 += i;
      sqlite3_bind_int64(stmt, 1, i);
      sqlite3_step(stmt);
      sqlite3_reset(stmt);
    }
    sqlite3_finalize(stmt);
    stmt = prepare(db, "SELECT a FROM t ORDER BY a DESC;");
    long total2 = 0;
    while( SQLITE_ROW == sqlite3_step(stmt) ){
      total2 += sqlite3_column_int64(stmt, 0);
    }
    sqlite3_finalize(stmt);
    affirm(total1 == total2);
    sqlite3_close_v2(db);
  }

  private static void testBindFetchDouble(){
    sqlite3 db = createNewDb();
    execSql(db, "CREATE TABLE t(a)");
    sqlite3_stmt stmt = prepare(db, "INSERT INTO t(a) VALUES(?);");
    double total1 = 0;
    for(double i = 1.5; i < 5.0; i = i + 1.0 ){
      total1 += i;
      sqlite3_bind_double(stmt, 1, i);
      sqlite3_step(stmt);
      sqlite3_reset(stmt);
    }
    sqlite3_finalize(stmt);
    stmt = prepare(db, "SELECT a FROM t ORDER BY a DESC;");
    double total2 = 0;
    int counter = 0;
    while( SQLITE_ROW == sqlite3_step(stmt) ){
      ++counter;
      total2 += sqlite3_column_double(stmt, 0);
    }
    affirm(4 == counter);
    sqlite3_finalize(stmt);
    affirm(total2<=total1+0.01 && total2>=total1-0.01);
    sqlite3_close_v2(db);
  }

  private static void testBindFetchText(){
    sqlite3 db = createNewDb();
    execSql(db, "CREATE TABLE t(a)");
    sqlite3_stmt stmt = prepare(db, "INSERT INTO t(a) VALUES(?);");
    String[] list1 = { "hell🤩", "w😃rld", "!" };
    int rc;
    for( String e : list1 ){
      rc = sqlite3_bind_text(stmt, 1, e);
      affirm(0 == rc);
      rc = sqlite3_step(stmt);
      affirm(SQLITE_DONE==rc);
      sqlite3_reset(stmt);
    }
    sqlite3_finalize(stmt);
    stmt = prepare(db, "SELECT a FROM t ORDER BY a DESC;");
    StringBuilder sbuf = new StringBuilder();
    int n = 0;
    while( SQLITE_ROW == sqlite3_step(stmt) ){
      String txt = sqlite3_column_text16(stmt, 0);
      //outln("txt = "+txt);
      sbuf.append( txt );
      ++n;
    }
    sqlite3_finalize(stmt);
    affirm(3 == n);
    affirm("w😃rldhell🤩!".equals(sbuf.toString()));
    sqlite3_close_v2(db);
  }

  private static void testBindFetchBlob(){
    sqlite3 db = createNewDb();
    execSql(db, "CREATE TABLE t(a)");
    sqlite3_stmt stmt = prepare(db, "INSERT INTO t(a) VALUES(?);");
    byte[] list1 = { 0x32, 0x33, 0x34 };
    int rc = sqlite3_bind_blob(stmt, 1, list1);
    affirm( 0==rc );
    rc = sqlite3_step(stmt);
    affirm(SQLITE_DONE == rc);
    sqlite3_finalize(stmt);
    stmt = prepare(db, "SELECT a FROM t ORDER BY a DESC;");
    int n = 0;
    int total = 0;
    while( SQLITE_ROW == sqlite3_step(stmt) ){
      byte[] blob = sqlite3_column_blob(stmt, 0);
      affirm(3 == blob.length);
      int i = 0;
      for(byte b : blob){
        affirm(b == list1[i++]);
        total += b;
      }
      ++n;
    }
    sqlite3_finalize(stmt);
    affirm(1 == n);
    affirm(total == 0x32 + 0x33 + 0x34);
    sqlite3_close_v2(db);
  }

  private static void testSql(){
    sqlite3 db = createNewDb();
    sqlite3_stmt stmt = prepare(db, "SELECT 1");
    affirm( "SELECT 1".equals(sqlite3_sql(stmt)) );
    sqlite3_finalize(stmt);
    stmt = prepare(db, "SELECT ?");
    sqlite3_bind_text(stmt, 1, "hell😃");
    affirm( "SELECT 'hell😃'".equals(sqlite3_expanded_sql(stmt)) );
    sqlite3_finalize(stmt);
  }

  private static void testCollation(){
    final sqlite3 db = createNewDb();
    execSql(db, "CREATE TABLE t(a); INSERT INTO t(a) VALUES('a'),('b'),('c')");
    final ValueHolder<Boolean> xDestroyCalled = new ValueHolder<>(false);
    final Collation myCollation = new Collation() {
        private String myState =
          "this is local state. There is much like it, but this is mine.";
        @Override
        // Reverse-sorts its inputs...
        public int xCompare(byte[] lhs, byte[] rhs){
          int len = lhs.length > rhs.length ? rhs.length : lhs.length;
          int c = 0, i = 0;
          for(i = 0; i < len; ++i){
            c = lhs[i] - rhs[i];
            if(0 != c) break;
          }
          if(0==c){
            if(i < lhs.length) c = 1;
            else if(i < rhs.length) c = -1;
          }
          return -c;
        }
        @Override
        public void xDestroy() {
          // Just demonstrates that xDestroy is called.
          xDestroyCalled.value = true;
        }
      };
    final CollationNeeded collLoader = new CollationNeeded(){
        public int xCollationNeeded(sqlite3 dbArg, int eTextRep, String collationName){
          affirm(dbArg == db/* as opposed to a temporary object*/);
          return sqlite3_create_collation(dbArg, "reversi", eTextRep, myCollation);
        }
      };
    int rc = sqlite3_collation_needed(db, collLoader);
    affirm( 0 == rc );
    rc = sqlite3_collation_needed(db, collLoader);
    affirm( 0 == rc /* Installing the same object again is a no-op */);
    sqlite3_stmt stmt = prepare(db, "SELECT a FROM t ORDER BY a COLLATE reversi");
    int counter = 0;
    while( SQLITE_ROW == sqlite3_step(stmt) ){
      final String val = sqlite3_column_text16(stmt, 0);
      ++counter;
      //outln("REVERSI'd row#"+counter+": "+val);
      switch(counter){
        case 1: affirm("c".equals(val)); break;
        case 2: affirm("b".equals(val)); break;
        case 3: affirm("a".equals(val)); break;
      }
    }
    affirm(3 == counter);
    sqlite3_finalize(stmt);
    stmt = prepare(db, "SELECT a FROM t ORDER BY a");
    counter = 0;
    while( SQLITE_ROW == sqlite3_step(stmt) ){
      final String val = sqlite3_column_text16(stmt, 0);
      ++counter;
      //outln("Non-REVERSI'd row#"+counter+": "+val);
      switch(counter){
        case 3: affirm("c".equals(val)); break;
        case 2: affirm("b".equals(val)); break;
        case 1: affirm("a".equals(val)); break;
      }
    }
    affirm(3 == counter);
    sqlite3_finalize(stmt);
    affirm(!xDestroyCalled.value);
    rc = sqlite3_collation_needed(db, null);
    affirm( 0 == rc );
    sqlite3_close_v2(db);
    affirm(xDestroyCalled.value);
  }

  private static void testToUtf8(){
    /**
       Java docs seem contradictory, claiming to use "modified UTF-8"
       encoding while also claiming to export using RFC 2279:

       https://docs.oracle.com/javase/8/docs/api/java/nio/charset/Charset.html

       Let's ensure that we can convert to standard UTF-8 in Java code
       (noting that the JNI native API has no way to do this).
    */
    final byte[] ba = "a \0 b".getBytes(StandardCharsets.UTF_8);
    affirm( 5 == ba.length /* as opposed to 6 in modified utf-8 */);
  }

  private static void testStatus(){
    final OutputPointer.Int64 cur64 = new OutputPointer.Int64();
    final OutputPointer.Int64 high64 = new OutputPointer.Int64();
    final OutputPointer.Int32 cur32 = new OutputPointer.Int32();
    final OutputPointer.Int32 high32 = new OutputPointer.Int32();
    final sqlite3 db = createNewDb();
    execSql(db, "create table t(a); insert into t values(1),(2),(3)");

    int rc = sqlite3_status(SQLITE_STATUS_MEMORY_USED, cur32, high32, false);
    affirm( 0 == rc );
    affirm( cur32.value > 0 );
    affirm( high32.value >= cur32.value );

    rc = sqlite3_status64(SQLITE_STATUS_MEMORY_USED, cur64, high64, false);
    affirm( 0 == rc );
    affirm( cur64.value > 0 );
    affirm( high64.value >= cur64.value );

    cur32.value = 0;
    high32.value = 1;
    rc = sqlite3_db_status(db, SQLITE_DBSTATUS_SCHEMA_USED, cur32, high32, false);
    affirm( 0 == rc );
    affirm( cur32.value > 0 );
    affirm( high32.value == 0 /* always 0 for SCHEMA_USED */ );

    sqlite3_close_v2(db);
  }

  private static void testUdf1(){
    final sqlite3 db = createNewDb();
    // These ValueHolders are just to confirm that the func did what we want...
    final ValueHolder<Boolean> xDestroyCalled = new ValueHolder<>(false);
    final ValueHolder<Integer> xFuncAccum = new ValueHolder<>(0);

    // Create an SQLFunction instance using one of its 3 subclasses:
    // Scalar, Aggregate, or Window:
    SQLFunction func =
      // Each of the 3 subclasses requires a different set of
      // functions, all of which must be implemented.  Anonymous
      // classes are a convenient way to implement these.
      new SQLFunction.Scalar(){
        public void xFunc(sqlite3_context cx, sqlite3_value[] args){
          affirm(db == sqlite3_context_db_handle(cx));
          int result = 0;
          for( sqlite3_value v : args ) result += sqlite3_value_int(v);
          xFuncAccum.value += result;// just for post-run testing
          sqlite3_result_int(cx, result);
        }
        /* OPTIONALLY override xDestroy... */
        public void xDestroy(){
          xDestroyCalled.value = true;
        }
      };

    // Register and use the function...
    int rc = sqlite3_create_function(db, "myfunc", -1, SQLITE_UTF8, func);
    affirm(0 == rc);
    affirm(0 == xFuncAccum.value);
    final sqlite3_stmt stmt = prepare(db, "SELECT myfunc(1,2,3)");
    int n = 0;
    while( SQLITE_ROW == sqlite3_step(stmt) ){
      affirm( 6 == sqlite3_column_int(stmt, 0) );
      ++n;
    }
    sqlite3_finalize(stmt);
    affirm(1 == n);
    affirm(6 == xFuncAccum.value);
    affirm( !xDestroyCalled.value );
    sqlite3_close_v2(db);
    affirm( xDestroyCalled.value );
  }

  private static void testUdfJavaObject(){
    final sqlite3 db = createNewDb();
    final ValueHolder<sqlite3> testResult = new ValueHolder<>(db);
    final SQLFunction func = new SQLFunction.Scalar(){
        public void xFunc(sqlite3_context cx, sqlite3_value args[]){
          sqlite3_result_java_object(cx, testResult.value);
        }
      };
    int rc = sqlite3_create_function(db, "myfunc", -1, SQLITE_UTF8, func);
    affirm(0 == rc);
    final sqlite3_stmt stmt = prepare(db, "select myfunc()");
    affirm( 0 != stmt.getNativePointer() );
    affirm( testResult.value == db );
    int n = 0;
    if( SQLITE_ROW == sqlite3_step(stmt) ){
      final sqlite3_value v = sqlite3_column_value(stmt, 0);
      affirm( testResult.value == sqlite3_value_java_object(v) );
      affirm( testResult.value == sqlite3_value_java_casted(v, sqlite3.class) );
      affirm( testResult.value ==
              sqlite3_value_java_casted(v, testResult.value.getClass()) );
      affirm( testResult.value == sqlite3_value_java_casted(v, Object.class) );
      affirm( null == sqlite3_value_java_casted(v, String.class) );
      ++n;
    }
    sqlite3_finalize(stmt);
    affirm( 1 == n );
    sqlite3_close_v2(db);
  }

  private static void testUdfAggregate(){
    final sqlite3 db = createNewDb();
    final ValueHolder<Boolean> xFinalNull =
      // To confirm that xFinal() is called with no aggregate state
      // when the corresponding result set is empty.
      new ValueHolder<>(false);
    SQLFunction func = new SQLFunction.Aggregate<Integer>(){
        @Override
        public void xStep(sqlite3_context cx, sqlite3_value[] args){
          this.getAggregateState(cx, 0).value += sqlite3_value_int(args[0]);
        }
        @Override
        public void xFinal(sqlite3_context cx){
          final Integer v = this.takeAggregateState(cx);
          if(null == v){
            xFinalNull.value = true;
            sqlite3_result_null(cx);
          }else{
            sqlite3_result_int(cx, v);
          }
        }
      };
    execSql(db, "CREATE TABLE t(a); INSERT INTO t(a) VALUES(1),(2),(3)");
    int rc = sqlite3_create_function(db, "myfunc", 1, SQLITE_UTF8, func);
    affirm(0 == rc);
    sqlite3_stmt stmt = prepare(db, "select myfunc(a), myfunc(a+10) from t");
    int n = 0;
    if( SQLITE_ROW == sqlite3_step(stmt) ){
      final int v = sqlite3_column_int(stmt, 0);
      affirm( 6 == v );
      ++n;
    }
    affirm(!xFinalNull.value);
    sqlite3_reset(stmt);
    // Ensure that the accumulator is reset...
    n = 0;
    if( SQLITE_ROW == sqlite3_step(stmt) ){
      final int v = sqlite3_column_int(stmt, 0);
      affirm( 6 == v );
      ++n;
    }
    sqlite3_finalize(stmt);
    affirm( 1==n );

    stmt = prepare(db, "select myfunc(a), myfunc(a+a) from t order by a");
    n = 0;
    while( SQLITE_ROW == sqlite3_step(stmt) ){
      final int c0 = sqlite3_column_int(stmt, 0);
      final int c1 = sqlite3_column_int(stmt, 1);
      ++n;
      affirm( 6 == c0 );
      affirm( 12 == c1 );
    }
    affirm( 1 == n );
    affirm(!xFinalNull.value);
    sqlite3_finalize(stmt);

    execSql(db, "SELECT myfunc(1) WHERE 0");
    affirm(xFinalNull.value);
    sqlite3_close_v2(db);
  }

  private static void testUdfWindow(){
    final sqlite3 db = createNewDb();
    /* Example window function, table, and results taken from:
       https://sqlite.org/windowfunctions.html#udfwinfunc */
    final SQLFunction func = new SQLFunction.Window<Integer>(){

        private void xStepInverse(sqlite3_context cx, int v){
          this.getAggregateState(cx,0).value += v;
        }
        @Override public void xStep(sqlite3_context cx, sqlite3_value[] args){
          this.xStepInverse(cx, sqlite3_value_int(args[0]));
        }
        @Override public void xInverse(sqlite3_context cx, sqlite3_value[] args){
          this.xStepInverse(cx, -sqlite3_value_int(args[0]));
        }

        private void xFinalValue(sqlite3_context cx, Integer v){
          if(null == v) sqlite3_result_null(cx);
          else sqlite3_result_int(cx, v);
        }
        @Override public void xFinal(sqlite3_context cx){
          xFinalValue(cx, this.takeAggregateState(cx));
        }
        @Override public void xValue(sqlite3_context cx){
          xFinalValue(cx, this.getAggregateState(cx,null).value);
        }
      };
    int rc = sqlite3_create_function(db, "winsumint", 1, SQLITE_UTF8, func);
    affirm( 0 == rc );
    execSql(db, new String[] {
        "CREATE TEMP TABLE twin(x, y); INSERT INTO twin VALUES",
        "('a', 4),('b', 5),('c', 3),('d', 8),('e', 1)"
      });
    final sqlite3_stmt stmt = prepare(db,
                         "SELECT x, winsumint(y) OVER ("+
                         "ORDER BY x ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING"+
                         ") AS sum_y "+
                         "FROM twin ORDER BY x;");
    affirm( 0 == rc );
    int n = 0;
    while( SQLITE_ROW == sqlite3_step(stmt) ){
      final String s = sqlite3_column_text16(stmt, 0);
      final int i = sqlite3_column_int(stmt, 1);
      switch(++n){
        case 1: affirm( "a".equals(s) && 9==i ); break;
        case 2: affirm( "b".equals(s) && 12==i ); break;
        case 3: affirm( "c".equals(s) && 16==i ); break;
        case 4: affirm( "d".equals(s) && 12==i ); break;
        case 5: affirm( "e".equals(s) && 9==i ); break;
        default: affirm( false /* cannot happen */ );
      }
    }
    sqlite3_finalize(stmt);
    affirm( 5 == n );
    sqlite3_close_v2(db);
  }

  private static void listBoundMethods(){
    if(false){
      final java.lang.reflect.Field[] declaredFields =
        SQLite3Jni.class.getDeclaredFields();
      outln("Bound constants:\n");
      for(java.lang.reflect.Field field : declaredFields) {
        if(java.lang.reflect.Modifier.isStatic(field.getModifiers())) {
          outln("\t"+field.getName());
        }
      }
    }
    final java.lang.reflect.Method[] declaredMethods =
      SQLite3Jni.class.getDeclaredMethods();
    final java.util.List<String> funcList = new java.util.ArrayList<>();
    for(java.lang.reflect.Method m : declaredMethods){
      if((m.getModifiers() & java.lang.reflect.Modifier.STATIC) != 0){
        final String name = m.getName();
        if(name.startsWith("sqlite3_")){
          funcList.add(name);
        }
      }
    }
    int count = 0;
    java.util.Collections.sort(funcList);
    for(String n : funcList){
      ++count;
      outln("\t"+n+"()");
    }
    outln(count+" functions named sqlite3_*.");
  }

  private static void testTrace(){
    final sqlite3 db = createNewDb();
    final ValueHolder<Integer> counter = new ValueHolder<>(0);
    /* Ensure that characters outside of the UTF BMP survive the trip
       from Java to sqlite3 and back to Java. (At no small efficiency
       penalty.) */
    final String nonBmpChar = "😃";
    sqlite3_trace_v2(
      db, SQLITE_TRACE_STMT | SQLITE_TRACE_PROFILE
          | SQLITE_TRACE_ROW | SQLITE_TRACE_CLOSE,
      new Tracer(){
        public int xCallback(int traceFlag, Object pNative, Object x){
          ++counter.value;
          //outln("TRACE "+traceFlag+" pNative = "+pNative.getClass().getName());
          switch(traceFlag){
            case SQLITE_TRACE_STMT:
              affirm(pNative instanceof sqlite3_stmt);
              //outln("TRACE_STMT sql = "+x);
              affirm(x instanceof String);
              affirm( ((String)x).indexOf(nonBmpChar) > 0 );
              break;
            case SQLITE_TRACE_PROFILE:
              affirm(pNative instanceof sqlite3_stmt);
              affirm(x instanceof Long);
              //outln("TRACE_PROFILE time = "+x);
              break;
            case SQLITE_TRACE_ROW:
              affirm(pNative instanceof sqlite3_stmt);
              affirm(null == x);
              //outln("TRACE_ROW = "+sqlite3_column_text16((sqlite3_stmt)pNative, 0));
              break;
            case SQLITE_TRACE_CLOSE:
              affirm(pNative instanceof sqlite3);
              affirm(null == x);
              break;
            default:
              affirm(false /*cannot happen*/);
              break;
          }
          return 0;
        }
      });
    execSql(db, "SELECT coalesce(null,null,'"+nonBmpChar+"'); "+
            "SELECT 'w"+nonBmpChar+"orld'");
    affirm( 6 == counter.value );
    sqlite3_close_v2(db);
    affirm( 7 == counter.value );
  }

  private static void testBusy(){
    final String dbName = "_busy-handler.db";
    final OutputPointer.sqlite3 outDb = new OutputPointer.sqlite3();

    int rc = sqlite3_open(dbName, outDb);
    ++metrics.dbOpen;
    affirm( 0 == rc );
    final sqlite3 db1 = outDb.get();
    execSql(db1, "CREATE TABLE IF NOT EXISTS t(a)");
    rc = sqlite3_open(dbName, outDb);
    ++metrics.dbOpen;
    affirm( 0 == rc );
    affirm( outDb.get() != db1 );
    final sqlite3 db2 = outDb.get();
    rc = sqlite3_db_config(db1, SQLITE_DBCONFIG_MAINDBNAME, "foo");
    affirm( sqlite3_db_filename(db1, "foo").endsWith(dbName) );

    final ValueHolder<Boolean> xDestroyed = new ValueHolder<>(false);
    final ValueHolder<Integer> xBusyCalled = new ValueHolder<>(0);
    BusyHandler handler = new BusyHandler(){
        @Override public int xCallback(int n){
          //outln("busy handler #"+n);
          return n > 2 ? 0 : ++xBusyCalled.value;
        }
        @Override public void xDestroy(){
          xDestroyed.value = true;
        }
      };
    rc = sqlite3_busy_handler(db2, handler);
    affirm(0 == rc);

    // Force a locked condition...
    execSql(db1, "BEGIN EXCLUSIVE");
    affirm(!xDestroyed.value);
    rc = sqlite3_prepare_v2(db2, "SELECT * from t", outStmt);
    affirm( SQLITE_BUSY == rc);
    assert( null == outStmt.get() );
    affirm( 3 == xBusyCalled.value );
    sqlite3_close_v2(db1);
    affirm(!xDestroyed.value);
    sqlite3_close_v2(db2);
    affirm(xDestroyed.value);
    try{
      final java.io.File f = new java.io.File(dbName);
      f.delete();
    }catch(Exception e){
      /* ignore */
    }
  }

  private static void testProgress(){
    final sqlite3 db = createNewDb();
    final ValueHolder<Integer> counter = new ValueHolder<>(0);
    sqlite3_progress_handler(db, 1, new ProgressHandler(){
        public int xCallback(){
          ++counter.value;
          return 0;
        }
      });
    execSql(db, "SELECT 1; SELECT 2;");
    affirm( counter.value > 0 );
    int nOld = counter.value;
    sqlite3_progress_handler(db, 0, null);
    execSql(db, "SELECT 1; SELECT 2;");
    affirm( nOld == counter.value );
    sqlite3_close_v2(db);
  }

  private static void testCommitHook(){
    final sqlite3 db = createNewDb();
    final ValueHolder<Integer> counter = new ValueHolder<>(0);
    final ValueHolder<Integer> hookResult = new ValueHolder<>(0);
    final CommitHook theHook = new CommitHook(){
        public int xCommitHook(){
          ++counter.value;
          return hookResult.value;
        }
      };
    CommitHook oldHook = sqlite3_commit_hook(db, theHook);
    affirm( null == oldHook );
    execSql(db, "CREATE TABLE t(a); INSERT INTO t(a) VALUES('a'),('b'),('c')");
    affirm( 2 == counter.value );
    execSql(db, "BEGIN; SELECT 1; SELECT 2; COMMIT;");
    affirm( 2 == counter.value /* NOT invoked if no changes are made */ );
    execSql(db, "BEGIN; update t set a='d' where a='c'; COMMIT;");
    affirm( 3 == counter.value );
    oldHook = sqlite3_commit_hook(db, theHook);
    affirm( theHook == oldHook );
    execSql(db, "BEGIN; update t set a='e' where a='d'; COMMIT;");
    affirm( 4 == counter.value );
    oldHook = sqlite3_commit_hook(db, null);
    affirm( theHook == oldHook );
    execSql(db, "BEGIN; update t set a='f' where a='e'; COMMIT;");
    affirm( 4 == counter.value );
    oldHook = sqlite3_commit_hook(db, null);
    affirm( null == oldHook );
    execSql(db, "BEGIN; update t set a='g' where a='f'; COMMIT;");
    affirm( 4 == counter.value );

    final CommitHook newHook = new CommitHook(){
        public int xCommitHook(){return 0;}
      };
    oldHook = sqlite3_commit_hook(db, newHook);
    affirm( null == oldHook );
    execSql(db, "BEGIN; update t set a='h' where a='g'; COMMIT;");
    affirm( 4 == counter.value );
    oldHook = sqlite3_commit_hook(db, theHook);
    affirm( newHook == oldHook );
    execSql(db, "BEGIN; update t set a='i' where a='h'; COMMIT;");
    affirm( 5 == counter.value );
    hookResult.value = SQLITE_ERROR;
    int rc = execSql(db, false, "BEGIN; update t set a='j' where a='i'; COMMIT;");
    affirm( SQLITE_CONSTRAINT == rc );
    affirm( 6 == counter.value );
    sqlite3_close_v2(db);
  }

  private static void testUpdateHook(){
    final sqlite3 db = createNewDb();
    final ValueHolder<Integer> counter = new ValueHolder<>(0);
    final ValueHolder<Integer> expectedOp = new ValueHolder<>(0);
    final UpdateHook theHook = new UpdateHook(){
        @SuppressWarnings("unchecked")
        public void xUpdateHook(int opId, String dbName, String tableName, long rowId){
          ++counter.value;
          if( 0!=expectedOp.value ){
            affirm( expectedOp.value == opId );
          }
        }
      };
    UpdateHook oldHook = sqlite3_update_hook(db, theHook);
    affirm( null == oldHook );
    expectedOp.value = SQLITE_INSERT;
    execSql(db, "CREATE TABLE t(a); INSERT INTO t(a) VALUES('a'),('b'),('c')");
    affirm( 3 == counter.value );
    expectedOp.value = SQLITE_UPDATE;
    execSql(db, "update t set a='d' where a='c';");
    affirm( 4 == counter.value );
    oldHook = sqlite3_update_hook(db, theHook);
    affirm( theHook == oldHook );
    expectedOp.value = SQLITE_DELETE;
    execSql(db, "DELETE FROM t where a='d'");
    affirm( 5 == counter.value );
    oldHook = sqlite3_update_hook(db, null);
    affirm( theHook == oldHook );
    execSql(db, "update t set a='e' where a='b';");
    affirm( 5 == counter.value );
    oldHook = sqlite3_update_hook(db, null);
    affirm( null == oldHook );

    final UpdateHook newHook = new UpdateHook(){
        public void xUpdateHook(int opId, String dbName, String tableName, long rowId){
        }
      };
    oldHook = sqlite3_update_hook(db, newHook);
    affirm( null == oldHook );
    execSql(db, "update t set a='h' where a='a'");
    affirm( 5 == counter.value );
    oldHook = sqlite3_update_hook(db, theHook);
    affirm( newHook == oldHook );
    expectedOp.value = SQLITE_UPDATE;
    execSql(db, "update t set a='i' where a='h'");
    affirm( 6 == counter.value );
    sqlite3_close_v2(db);
  }

  private static void testRollbackHook(){
    final sqlite3 db = createNewDb();
    final ValueHolder<Integer> counter = new ValueHolder<>(0);
    final RollbackHook theHook = new RollbackHook(){
        public void xRollbackHook(){
          ++counter.value;
        }
      };
    RollbackHook oldHook = sqlite3_rollback_hook(db, theHook);
    affirm( null == oldHook );
    execSql(db, "CREATE TABLE t(a); INSERT INTO t(a) VALUES('a'),('b'),('c')");
    affirm( 0 == counter.value );
    execSql(db, false, "BEGIN; SELECT 1; SELECT 2; ROLLBACK;");
    affirm( 1 == counter.value /* contra to commit hook, is invoked if no changes are made */ );

    final RollbackHook newHook = new RollbackHook(){
        public void xRollbackHook(){return;}
      };
    oldHook = sqlite3_rollback_hook(db, newHook);
    affirm( theHook == oldHook );
    execSql(db, false, "BEGIN; SELECT 1; ROLLBACK;");
    affirm( 1 == counter.value );
    oldHook = sqlite3_rollback_hook(db, theHook);
    affirm( newHook == oldHook );
    execSql(db, false, "BEGIN; SELECT 1; ROLLBACK;");
    affirm( 2 == counter.value );
    int rc = execSql(db, false, "BEGIN; SELECT 1; ROLLBACK;");
    affirm( 0 == rc );
    affirm( 3 == counter.value );
    sqlite3_close_v2(db);
  }

  /**
     If FTS5 is available, runs FTS5 tests, else returns with no side
     effects. If it is available but loading of the FTS5 bits fails,
     it throws.
  */
  @SuppressWarnings("unchecked")
  private static void testFts5() throws Exception {
    if( !SQLITE_ENABLE_FTS5 ){
      outln("SQLITE_ENABLE_FTS5 is not set. Skipping FTS5 tests.");
      return;
    }
    Exception err = null;
    try {
      Class t = Class.forName("org.sqlite.jni.TesterFts5");
      java.lang.reflect.Constructor ctor = t.getConstructor();
      ctor.setAccessible(true);
      ctor.newInstance() /* will run all tests */;
    }catch(ClassNotFoundException e){
      outln("FTS5 classes not loaded.");
      err = e;
    }catch(NoSuchMethodException e){
      outln("FTS5 tester ctor not found.");
      err = e;
    }catch(Exception e){
      outln("Instantiation of FTS5 tester threw.");
      err = e;
    }
    if( null != err ){
      outln("Exception: "+err);
      err.printStackTrace();
      throw err;
    }
  }

  private static void testAuthorizer(){
    final sqlite3 db = createNewDb();
    final ValueHolder<Integer> counter = new ValueHolder<>(0);
    final ValueHolder<Integer> authRc = new ValueHolder<>(0);
    final Authorizer auth = new Authorizer(){
        public int xAuth(int op, String s0, String s1, String s2, String s3){
          ++counter.value;
          //outln("xAuth(): "+s0+" "+s1+" "+s2+" "+s3);
          return authRc.value;
        }
      };
    execSql(db, "CREATE TABLE t(a); INSERT INTO t(a) VALUES('a'),('b'),('c')");
    sqlite3_set_authorizer(db, auth);
    execSql(db, "UPDATE t SET a=1");
    affirm( 1 == counter.value );
    authRc.value = SQLITE_DENY;
    int rc = execSql(db, false, "UPDATE t SET a=2");
    affirm( SQLITE_AUTH==rc );
    // TODO: expand these tests considerably
    sqlite3_close(db);
  }

  private static void testAutoExtension(){
    final ValueHolder<Integer> val = new ValueHolder<>(0);
    final ValueHolder<String> toss = new ValueHolder<>(null);
    final AutoExtension ax = new AutoExtension(){
        public synchronized int xEntryPoint(sqlite3 db){
          ++val.value;
          if( null!=toss.value ){
            throw new RuntimeException(toss.value);
          }
          return 0;
        }
      };
    int rc = sqlite3_auto_extension( ax );
    affirm( 0==rc );
    sqlite3_close(createNewDb());
    affirm( 1==val.value );
    sqlite3_close(createNewDb());
    affirm( 2==val.value );
    sqlite3_reset_auto_extension();
    sqlite3_close(createNewDb());
    affirm( 2==val.value );
    rc = sqlite3_auto_extension( ax );
    affirm( 0==rc );
    // Must not add a new entry
    rc = sqlite3_auto_extension( ax );
    affirm( 0==rc );
    sqlite3_close( createNewDb() );
    affirm( 3==val.value );
    affirm( sqlite3_cancel_auto_extension(ax) );
    affirm( !sqlite3_cancel_auto_extension(ax) );
    sqlite3_close(createNewDb());
    affirm( 3==val.value );
    rc = sqlite3_auto_extension( ax );
    affirm( 0==rc );
    Exception err = null;
    toss.value = "Throwing from AutoExtension.";
    try{
      createNewDb();
    }catch(Exception e){
      err = e;
    }
    affirm( err!=null );
    affirm( err.getMessage().indexOf(toss.value)>0 );
    affirm( sqlite3_cancel_auto_extension(ax) );
  }

  private static void testSleep(){
    out("Sleeping briefly... ");
    sqlite3_sleep(600);
    outln("Woke up.");
  }

  public static void main(String[] args) throws Exception {
    final long timeStart = System.nanoTime();
    test1();
    if(false) testCompileOption();
    final java.util.List<String> liArgs =
      java.util.Arrays.asList(args);
    testOpenDb1();
    testOpenDb2();
    testPrepare123();
    testBindFetchInt();
    testBindFetchInt64();
    testBindFetchDouble();
    testBindFetchText();
    testBindFetchBlob();
    testSql();
    testCollation();
    testToUtf8();
    testStatus();
    testUdf1();
    testUdfJavaObject();
    testUdfAggregate();
    testUdfWindow();
    testTrace();
    testBusy();
    testProgress();
    testCommitHook();
    testRollbackHook();
    testUpdateHook();
    testAuthorizer();
    testFts5();
    testAutoExtension();
    //testSleep();
    if(liArgs.indexOf("-v")>0){
      sqlite3_do_something_for_developer();
      //listBoundMethods();
    }
    final long timeEnd = System.nanoTime();
    affirm( SQLite3Jni.uncacheJniEnv() );
    affirm( !SQLite3Jni.uncacheJniEnv() );
    outln("Tests done. Metrics:");
    outln("\tAssertions checked: "+affirmCount);
    outln("\tDatabases opened: "+metrics.dbOpen);

    int nMethods = 0;
    int nNatives = 0;
    final java.lang.reflect.Method[] declaredMethods =
      SQLite3Jni.class.getDeclaredMethods();
    for(java.lang.reflect.Method m : declaredMethods){
      int mod = m.getModifiers();
      if( 0!=(mod & java.lang.reflect.Modifier.STATIC) ){
        final String name = m.getName();
        if(name.startsWith("sqlite3_")){
          ++nMethods;
          if( 0!=(mod & java.lang.reflect.Modifier.NATIVE) ){
            ++nNatives;
          }
        }
      }
    }
    outln("\tSQLite3Jni sqlite3_*() methods: "+
          nNatives+" native methods and "+
          (nMethods - nNatives)+" Java impls");
    outln("\tTotal time = "
          +((timeEnd - timeStart)/1000000.0)+"ms");
  }
}
