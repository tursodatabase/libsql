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

  private static <T> void out(T val){
    System.out.print(val);
  }

  private static <T> void outln(T val){
    System.out.println(val);
  }

  private static int affirmCount = 0;
  private static void affirm(Boolean v){
    ++affirmCount;
    if( !v ) throw new RuntimeException("Assertion failed.");
  }

  private static void test1(){
    outln("libversion_number: "
          + sqlite3_libversion_number()
          + "\n"
          + sqlite3_libversion()
          + "\n"
          + SQLITE_SOURCE_ID);
    affirm(sqlite3_libversion_number() == SQLITE_VERSION_NUMBER);
    //outln("threadsafe = "+sqlite3_threadsafe());
    affirm(SQLITE_MAX_LENGTH > 0);
    affirm(SQLITE_MAX_TRIGGER_DEPTH>0);
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

  private static void execSql(sqlite3 db, String[] sql){
    execSql(db, String.join("", sql));
  }
  private static void execSql(sqlite3 db, String sql){
      OutputPointer.Int32 oTail = new OutputPointer.Int32();
      final byte[] sqlUtf8 = sql.getBytes(StandardCharsets.UTF_8);
      int pos = 0, n = 1;
      byte[] sqlChunk = sqlUtf8;
      sqlite3_stmt stmt = new sqlite3_stmt();
      while(pos < sqlChunk.length){
        if(pos > 0){
          sqlChunk = Arrays.copyOfRange(sqlChunk, pos,
                                        sqlChunk.length);
        }
        if( 0==sqlChunk.length ) break;
        int rc = sqlite3_prepare_v2(db, sqlChunk, stmt, oTail);
        affirm(0 == rc);
        pos = oTail.getValue();
        affirm(0 != stmt.getNativePointer());
        rc = sqlite3_step(stmt);
        sqlite3_finalize(stmt);
        affirm(0 == stmt.getNativePointer());
        if(0!=rc && SQLITE_ROW!=rc && SQLITE_DONE!=rc){
          throw new RuntimeException("db op failed with rc="+rc);
        }
      }
  }
  private static void testOpenDb1(){
      sqlite3 db = new sqlite3();
      affirm(0 == db.getNativePointer());
      int rc = sqlite3_open(":memory:", db);
      affirm(0 == rc);
      affirm(0 < db.getNativePointer());
      sqlite3_close(db);
      affirm(0 == db.getNativePointer());
  }

  private static void testOpenDb2(){
    sqlite3 db = new sqlite3();
    affirm(0 == db.getNativePointer());
    int rc = sqlite3_open_v2(":memory:", db,
                             SQLITE_OPEN_READWRITE
                             | SQLITE_OPEN_CREATE, null);
    affirm(0 == rc);
    affirm(0 < db.getNativePointer());
    sqlite3_close_v2(db);
    affirm(0 == db.getNativePointer());
  }

  private static sqlite3 createNewDb(){
    sqlite3 db = new sqlite3();
    affirm(0 == db.getNativePointer());
    int rc = sqlite3_open(":memory:", db);
    affirm(0 == rc);
    affirm(0 != db.getNativePointer());
    rc = sqlite3_busy_timeout(db, 2000);
    affirm( 0 == rc );
    return db;
  }

  private static void testPrepare123(){
    sqlite3 db = createNewDb();
    int rc;
    sqlite3_stmt stmt = new sqlite3_stmt();
    affirm(0 == stmt.getNativePointer());
    rc = sqlite3_prepare(db, "CREATE TABLE t1(a);", stmt);
    affirm(0 == rc);
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
        rc = sqlite3_prepare_v2(db, sqlChunk, stmt, oTail);
        affirm(0 == rc);
        pos = oTail.getValue();
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
                            SQLITE_PREPARE_NORMALIZE, stmt);
    affirm(0 == rc);
    affirm(0 != stmt.getNativePointer());
    sqlite3_finalize(stmt);
    affirm(0 == stmt.getNativePointer() );
    sqlite3_close_v2(db);
  }

  private static void testBindFetchInt(){
    sqlite3 db = createNewDb();
    execSql(db, "CREATE TABLE t(a)");

    sqlite3_stmt stmt = new sqlite3_stmt();
    int rc = sqlite3_prepare(db, "INSERT INTO t(a) VALUES(:a);", stmt);
    affirm(0 == rc);
    affirm(1 == sqlite3_bind_parameter_count(stmt));
    final int paramNdx = sqlite3_bind_parameter_index(stmt, ":a");
    affirm(1 == paramNdx);
    int total1 = 0;
    long rowid = -1;
    int changes = sqlite3_changes(db);
    int changesT = sqlite3_total_changes(db);
    long changes64 = sqlite3_changes64(db);
    long changesT64 = sqlite3_total_changes64(db);
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
    affirm(total1 > 0);
    affirm(sqlite3_changes(db) > changes);
    affirm(sqlite3_total_changes(db) > changesT);
    affirm(sqlite3_changes64(db) > changes64);
    affirm(sqlite3_total_changes64(db) > changesT64);
    rc = sqlite3_prepare(db, "SELECT a FROM t ORDER BY a DESC;", stmt);
    affirm(0 == rc);
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
    sqlite3_stmt stmt = new sqlite3_stmt();
    int rc = sqlite3_prepare(db, "INSERT INTO t(a) VALUES(?);", stmt);
    long total1 = 0;
    for(long i = 0xffffffff; i < 0xffffffff + 3; ++i ){
      total1 += i;
      sqlite3_bind_int64(stmt, 1, i);
      sqlite3_step(stmt);
      sqlite3_reset(stmt);
    }
    sqlite3_finalize(stmt);
    rc = sqlite3_prepare(db, "SELECT a FROM t ORDER BY a DESC;", stmt);
    affirm(0 == rc);
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
    sqlite3_stmt stmt = new sqlite3_stmt();
    int rc = sqlite3_prepare(db, "INSERT INTO t(a) VALUES(?);", stmt);
    double total1 = 0;
    for(double i = 1.5; i < 5.0; i = i + 1.0 ){
      total1 += i;
      sqlite3_bind_double(stmt, 1, i);
      sqlite3_step(stmt);
      sqlite3_reset(stmt);
    }
    sqlite3_finalize(stmt);
    rc = sqlite3_prepare(db, "SELECT a FROM t ORDER BY a DESC;", stmt);
    affirm(0 == rc);
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
    sqlite3_stmt stmt = new sqlite3_stmt();
    int rc = sqlite3_prepare(db, "INSERT INTO t(a) VALUES(?);", stmt);
    String list1[] = { "hell🤩", "w😃rld", "!" };
    for( String e : list1 ){
      rc = sqlite3_bind_text(stmt, 1, e);
      affirm(0 == rc);
      rc = sqlite3_step(stmt);
      affirm(SQLITE_DONE==rc);
      sqlite3_reset(stmt);
    }
    sqlite3_finalize(stmt);
    rc = sqlite3_prepare(db, "SELECT a FROM t ORDER BY a DESC;", stmt);
    affirm(0 == rc);
    StringBuffer sbuf = new StringBuffer();
    int n = 0;
    while( SQLITE_ROW == sqlite3_step(stmt) ){
      String txt = sqlite3_column_text(stmt, 0);
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
    sqlite3_stmt stmt = new sqlite3_stmt();
    int rc = sqlite3_prepare(db, "INSERT INTO t(a) VALUES(?);", stmt);
    byte list1[] = { 0x32, 0x33, 0x34 };
    rc = sqlite3_bind_blob(stmt, 1, list1);
    rc = sqlite3_step(stmt);
    affirm(SQLITE_DONE == rc);
    sqlite3_finalize(stmt);
    rc = sqlite3_prepare(db, "SELECT a FROM t ORDER BY a DESC;", stmt);
    affirm(0 == rc);
    int n = 0;
    int total = 0;
    while( SQLITE_ROW == sqlite3_step(stmt) ){
      byte blob[] = sqlite3_column_blob(stmt, 0);
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

  private static void testCollation(){
    sqlite3 db = createNewDb();
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
    int rc = sqlite3_create_collation(db, "reversi", SQLITE_UTF8, myCollation);
    affirm(0 == rc);
    sqlite3_stmt stmt = new sqlite3_stmt();
    sqlite3_prepare(db, "SELECT a FROM t ORDER BY a COLLATE reversi", stmt);
    int counter = 0;
    while( SQLITE_ROW == sqlite3_step(stmt) ){
      final String val = sqlite3_column_text(stmt, 0);
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
    sqlite3_prepare(db, "SELECT a FROM t ORDER BY a", stmt);
    counter = 0;
    while( SQLITE_ROW == sqlite3_step(stmt) ){
      final String val = sqlite3_column_text(stmt, 0);
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
    sqlite3_close(db);
    affirm(xDestroyCalled.value);
  }

  private static void testToUtf8(){
    /**
       Java docs seem contradictory, claiming to use "modified UTF-8"
       encoding while also claiming to export using RFC 2279:

       https://docs.oracle.com/javase/8/docs/api/java/nio/charset/Charset.html
    */
    final byte[] ba = "a \0 b".getBytes(StandardCharsets.UTF_8);
    //out("\"a NUL b\" via getBytes(): ");
    affirm( 5 == ba.length /* as opposed to 6 in modified utf-8 */);
    //for( byte b : ba ) out( ""+b );
    //outln("");
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
      // classes are a convenient way to implement these, though the
      // result is possibly somewhat noisy for those not at home in
      // Java...
      new SQLFunction.Scalar(){
        public void xFunc(sqlite3_context cx, sqlite3_value args[]){
          affirm(db.getNativePointer()
                   == sqlite3_context_db_handle(cx).getNativePointer());
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
    execSql(db, "SELECT myfunc(1,2,3)");
    affirm(6 == xFuncAccum.value);
    affirm( !xDestroyCalled.value );
    sqlite3_close(db);
    affirm( xDestroyCalled.value );
  }

  private static void testUdfJavaObject(){
    final sqlite3 db = createNewDb();
    final ValueHolder<Long> testResult = new ValueHolder<>(42L);
    SQLFunction func = new SQLFunction.Scalar(){
        public void xFunc(sqlite3_context cx, sqlite3_value args[]){
          sqlite3_result_java_object(cx, testResult.value);
        }
      };
    int rc = sqlite3_create_function(db, "myfunc", -1, SQLITE_UTF8, func);
    affirm(0 == rc);
    sqlite3_stmt stmt = new sqlite3_stmt();
    sqlite3_prepare(db, "select myfunc()", stmt);
    affirm( 0 != stmt.getNativePointer() );
    int n = 0;
    if( SQLITE_ROW == sqlite3_step(stmt) ){
      sqlite3_value v = sqlite3_column_value(stmt, 0);
      affirm( testResult.value == sqlite3_value_java_object(v) );
      affirm( testResult.value == sqlite3_value_java_casted(v, Long.class) );
      affirm( testResult.value ==
              sqlite3_value_java_casted(v, testResult.value.getClass()) );
      affirm( null == sqlite3_value_java_casted(v, Double.class) );
      ++n;
    }
    sqlite3_finalize(stmt);
    affirm( 1 == n );
    sqlite3_close(db);
  }

  private static void testUdfAggregate(){
    final sqlite3 db = createNewDb();
    SQLFunction func = new SQLFunction.Aggregate<Integer>(){
        @Override
        public void xStep(sqlite3_context cx, sqlite3_value args[]){
          this.getAggregateState(cx, 0).value += sqlite3_value_int(args[0]);
        }
        @Override
        public void xFinal(sqlite3_context cx){
          final Integer v = this.takeAggregateState(cx);
          if(null == v) sqlite3_result_null(cx);
          else sqlite3_result_int(cx, v);
        }
      };
    execSql(db, "CREATE TABLE t(a); INSERT INTO t(a) VALUES(1),(2),(3)");
    int rc = sqlite3_create_function(db, "myfunc", 1, SQLITE_UTF8, func);
    affirm(0 == rc);
    sqlite3_stmt stmt = new sqlite3_stmt();
    sqlite3_prepare(db, "select myfunc(a), myfunc(a+10) from t", stmt);
    affirm( 0 != stmt.getNativePointer() );
    int n = 0;
    if( SQLITE_ROW == sqlite3_step(stmt) ){
      final int v = sqlite3_column_int(stmt, 0);
      affirm( 6 == v );
      ++n;
    }
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

    rc = sqlite3_prepare(db, "select myfunc(a), myfunc(a+a) from t order by a",
                         stmt);
    affirm( 0 == rc );
    n = 0;
    while( SQLITE_ROW == sqlite3_step(stmt) ){
      final int c0 = sqlite3_column_int(stmt, 0);
      final int c1 = sqlite3_column_int(stmt, 1);
      ++n;
      affirm( 6 == c0 );
      affirm( 12 == c1 );
    }
    affirm( 1 == n );
    sqlite3_finalize(stmt);
    sqlite3_close(db);
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
    sqlite3_stmt stmt = new sqlite3_stmt();
    rc = sqlite3_prepare(db,
                         "SELECT x, winsumint(y) OVER ("+
                         "ORDER BY x ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING"+
                         ") AS sum_y "+
                         "FROM twin ORDER BY x;", stmt);
    affirm( 0 == rc );
    int n = 0;
    while( SQLITE_ROW == sqlite3_step(stmt) ){
      final String s = sqlite3_column_text(stmt, 0);
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
    sqlite3_close(db);
  }

  private static void listBoundMethods(){
    //public static List<Field> getStatics(Class<?> clazz) {
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
    sqlite3_trace_v2(
      db, SQLITE_TRACE_STMT | SQLITE_TRACE_PROFILE
          | SQLITE_TRACE_ROW | SQLITE_TRACE_CLOSE,
      new Tracer(){
        public int xCallback(int traceFlag, long pNative, Object x){
          ++counter.value;
          //outln("Trace #"+counter.value+" flag="+traceFlag+": "+x);
          switch(traceFlag){
            case SQLITE_TRACE_STMT:
              // pNative ==> sqlite3_stmt
              affirm(x instanceof String); break;
            case SQLITE_TRACE_PROFILE:
              // pNative ==> sqlite3_stmt
              affirm(x instanceof Long); break;
            case SQLITE_TRACE_ROW:
              // pNative ==> sqlite3_stmt
            case SQLITE_TRACE_CLOSE:
              // pNative ==> sqlite3
              affirm(null == x);
          }
          return 0;
        }
      });
    execSql(db, "SELECT 1; SELECT 2");
    affirm( 6 == counter.value );
    sqlite3_close(db);
    affirm( 7 == counter.value );
  }

  private static void testBusy(){
    outln("testBusy()...");
    final String dbName = "_busy-handler.db";
    final sqlite3 db1 = new sqlite3();
    final sqlite3 db2 = new sqlite3();

    int rc = sqlite3_open(dbName, db1);
    affirm( 0 == rc );
    execSql(db1, "CREATE TABLE IF NOT EXISTS t(a)");
    rc = sqlite3_open(dbName, db2);
    affirm( 0 == rc );

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
    affirm( false == xDestroyed.value );
    sqlite3_stmt stmt = new sqlite3_stmt();
    rc = sqlite3_prepare(db2, "SELECT * from t", stmt);
    affirm( SQLITE_BUSY == rc);
    affirm( 3 == xBusyCalled.value );
    sqlite3_finalize(stmt);
    sqlite3_close(db1);
    affirm( false == xDestroyed.value );
    sqlite3_close(db2);
    affirm( true == xDestroyed.value );
    try{
      final java.io.File f = new java.io.File(dbName);
      f.delete();
    }catch(Exception e){
      /* ignore */
    }
  }

  private static void testSleep(){
    out("Sleeping briefly... ");
    sqlite3_sleep(600);
    outln("Woke up.");
  }

  public static void main(String[] args){
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
    testCollation();
    testToUtf8();
    testUdf1();
    testUdfJavaObject();
    testUdfAggregate();
    testUdfWindow();
    testTrace();
    testBusy();
    testSleep();
    if(liArgs.indexOf("-v")>0){
      listBoundMethods();
    }
    outln("Tests done. "+affirmCount+" assertion checked.");
  }
}
