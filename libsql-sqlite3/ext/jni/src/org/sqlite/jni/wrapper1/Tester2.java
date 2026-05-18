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
** This file contains a set of tests for the sqlite3 JNI bindings.
*/
package org.sqlite.jni.wrapper1;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.sqlite.jni.capi.CApi;

/**
   An annotation for Tester2 tests which we do not want to run in
   reflection-driven test mode because either they are not suitable
   for multi-threaded threaded mode or we have to control their execution
   order.
*/
@java.lang.annotation.Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
@java.lang.annotation.Target({java.lang.annotation.ElementType.METHOD})
@interface ManualTest{}
/**
   Annotation for Tester2 tests which mark those which must be skipped
   in multi-threaded mode.
*/
@java.lang.annotation.Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
@java.lang.annotation.Target({java.lang.annotation.ElementType.METHOD})
@interface SingleThreadOnly{}

public class Tester2 implements Runnable {
  //! True when running in multi-threaded mode.
  private static boolean mtMode = false;
  //! True to sleep briefly between tests.
  private static boolean takeNaps = false;
  //! True to shuffle the order of the tests.
  private static boolean shuffle = false;
  //! True to dump the list of to-run tests to stdout.
  private static int listRunTests = 0;
  //! True to squelch all out() and outln() output.
  private static boolean quietMode = false;
  //! Total number of runTests() calls.
  private static int nTestRuns = 0;
  //! List of test*() methods to run.
  private static List<java.lang.reflect.Method> testMethods = null;
  //! List of exceptions collected by run()
  private static final List<Exception> listErrors = new ArrayList<>();
  private static final class Metrics {
    //! Number of times createNewDb() (or equivalent) is invoked.
    volatile int dbOpen = 0;
  }

  //! Instance ID.
  private final Integer tId;

  Tester2(Integer id){
    tId = id;
  }

  static final Metrics metrics = new Metrics();

  public static synchronized void outln(){
    if( !quietMode ){
      System.out.println();
    }
  }

  public static synchronized void outPrefix(){
    if( !quietMode ){
      System.out.print(Thread.currentThread().getName()+": ");
    }
  }

  public static synchronized void outln(Object val){
    if( !quietMode ){
      outPrefix();
      System.out.println(val);
    }
  }

  public static synchronized void out(Object val){
    if( !quietMode ){
      System.out.print(val);
    }
  }

  @SuppressWarnings("unchecked")
  public static synchronized void out(Object... vals){
    if( !quietMode ){
      outPrefix();
      for(Object v : vals) out(v);
    }
  }

  @SuppressWarnings("unchecked")
  public static synchronized void outln(Object... vals){
    if( !quietMode ){
      out(vals); out("\n");
    }
  }

  static volatile int affirmCount = 0;
  public static synchronized int affirm(Boolean v, String comment){
    ++affirmCount;
    if( false ) assert( v /* prefer assert over exception if it's enabled because
                 the JNI layer sometimes has to suppress exceptions,
                 so they might be squelched on their way back to the
                 top. */);
    if( !v ) throw new RuntimeException(comment);
    return affirmCount;
  }

  public static void affirm(Boolean v){
    affirm(v, "Affirmation failed.");
  }


  public static void execSql(Sqlite db, String sql[]){
    execSql(db, String.join("", sql));
  }

  /**
     Executes all SQL statements in the given string. If throwOnError
     is true then it will throw for any prepare/step errors, else it
     will return the corresponding non-0 result code.
  */
  public static int execSql(Sqlite dbw, boolean throwOnError, String sql){
    final ValueHolder<Integer> rv = new ValueHolder<>(0);
    final Sqlite.PrepareMulti pm = new Sqlite.PrepareMulti(){
        @Override public void call(Sqlite.Stmt stmt){
          try{
            while( Sqlite.ROW == (rv.value = stmt.step(throwOnError)) ){}
          }
          finally{ stmt.finalizeStmt(); }
        }
      };
    try {
      dbw.prepareMulti(sql, pm);
    }catch(SqliteException se){
      if( throwOnError ){
        throw se;
      }else{
        /* This error (likely) happened in the prepare() phase and we
           need to preempt it. */
        rv.value = se.errcode();
      }
    }
    return (rv.value==Sqlite.DONE) ? 0 : rv.value;
  }

  static void execSql(Sqlite db, String sql){
    execSql(db, true, sql);
  }

  @SingleThreadOnly /* because it's thread-agnostic */
  private void test1(){
    affirm(Sqlite.libVersionNumber() == CApi.SQLITE_VERSION_NUMBER);
  }

  private void nap() throws InterruptedException {
    if( takeNaps ){
      Thread.sleep(java.util.concurrent.ThreadLocalRandom.current().nextInt(3, 17), 0);
    }
  }

  Sqlite openDb(String name){
    final Sqlite db = Sqlite.open(name, Sqlite.OPEN_READWRITE|
                                  Sqlite.OPEN_CREATE|
                                  Sqlite.OPEN_EXRESCODE);
    ++metrics.dbOpen;
    return db;
  }

  Sqlite openDb(){ return openDb(":memory:"); }

  void testOpenDb1(){
    Sqlite db = openDb();
    affirm( 0!=db.nativeHandle().getNativePointer() );
    affirm( "main".equals( db.dbName(0) ) );
    db.setMainDbName("foo");
    affirm( "foo".equals( db.dbName(0) ) );
    affirm( db.dbConfig(Sqlite.DBCONFIG_DEFENSIVE, true)
      /* The underlying function has different mangled names in jdk8
         vs jdk19, and this call is here to ensure that the build
         fails if it cannot find both names. */ );
    affirm( !db.dbConfig(Sqlite.DBCONFIG_DEFENSIVE, false) );
    SqliteException ex = null;
    try{ db.dbConfig(0, false); }
    catch(SqliteException e){ ex = e; }
    affirm( null!=ex );
    ex = null;
    db.close();
    affirm( null==db.nativeHandle() );

    try{ db = openDb("/no/such/dir/.../probably"); }
    catch(SqliteException e){ ex = e; }
    affirm( ex!=null );
    affirm( ex.errcode() != 0 );
    affirm( ex.extendedErrcode() != 0 );
    affirm( ex.errorOffset() < 0 );
    // there's no reliable way to predict what ex.systemErrno() might be
  }

  void testPrepare1(){
    try (Sqlite db = openDb()) {
      Sqlite.Stmt stmt = db.prepare("SELECT ?1");
      Exception e = null;
      affirm( null!=stmt.nativeHandle() );
      affirm( db == stmt.getDb() );
      affirm( 1==stmt.bindParameterCount() );
      affirm( "?1".equals(stmt.bindParameterName(1)) );
      affirm( null==stmt.bindParameterName(2) );
      stmt.bindInt64(1, 1);
      stmt.bindDouble(1, 1.1);
      stmt.bindObject(1, db);
      stmt.bindNull(1);
      stmt.bindText(1, new byte[] {32,32,32});
      stmt.bindText(1, "123");
      stmt.bindText16(1, "123".getBytes(StandardCharsets.UTF_16));
      stmt.bindText16(1, "123");
      stmt.bindZeroBlob(1, 8);
      stmt.bindBlob(1, new byte[] {1,2,3,4});
      stmt.bindInt(1, 17);
      try{ stmt.bindInt(2,1); }
      catch(Exception ex){ e = ex; }
      affirm( null!=e );
      e = null;
      affirm( stmt.step() );
      try{ stmt.columnInt(1); }
      catch(Exception ex){ e = ex; }
      affirm( null!=e );
      e = null;
      affirm( 17 == stmt.columnInt(0) );
      affirm( 17L == stmt.columnInt64(0) );
      affirm( 17.0 == stmt.columnDouble(0) );
      affirm( "17".equals(stmt.columnText16(0)) );
      affirm( !stmt.step() );
      stmt.reset();
      affirm( Sqlite.ROW==stmt.step(false) );
      affirm( !stmt.step() );
      affirm( 0 == stmt.finalizeStmt() );
      affirm( null==stmt.nativeHandle() );

      stmt = db.prepare("SELECT ?");
      stmt.bindObject(1, db);
      affirm( Sqlite.ROW == stmt.step(false) );
      affirm( db==stmt.columnObject(0) );
      affirm( db==stmt.columnObject(0, Sqlite.class ) );
      affirm( null==stmt.columnObject(0, Sqlite.Stmt.class ) );
      affirm( 0==stmt.finalizeStmt() )
        /* getting a non-0 out of sqlite3_finalize() is tricky */;
      affirm( null==stmt.nativeHandle() );
    }
  }

  void testUdfScalar(){
    final ValueHolder<Integer> xDestroyCalled = new ValueHolder<>(0);
    try (Sqlite db = openDb()) {
      execSql(db, "create table t(a); insert into t(a) values(1),(2),(3)");
      final ValueHolder<Integer> vh = new ValueHolder<>(0);
      final ScalarFunction f = new ScalarFunction(){
          public void xFunc(SqlFunction.Arguments args){
            affirm( db == args.getDb() );
            for( SqlFunction.Arguments.Arg arg : args ){
              vh.value += arg.getInt();
            }
            args.resultInt(vh.value);
          }
          public void xDestroy(){
            ++xDestroyCalled.value;
          }
        };
      db.createFunction("myfunc", -1, f);
      Sqlite.Stmt q = db.prepare("select myfunc(1,2,3)");
      affirm( q.step() );
      affirm( 6 == vh.value );
      affirm( 6 == q.columnInt(0) );
      q.finalizeStmt();
      affirm( 0 == xDestroyCalled.value );
      vh.value = 0;
      q = db.prepare("select myfunc(-1,-2,-3)");
      affirm( q.step() );
      affirm( -6 == vh.value );
      affirm( -6 == q.columnInt(0) );
      affirm( 0 == xDestroyCalled.value );
      q.finalizeStmt();
    }
    affirm( 1 == xDestroyCalled.value );
  }

  void testUdfAggregate(){
    final ValueHolder<Integer> xDestroyCalled = new ValueHolder<>(0);
    Sqlite.Stmt q = null;
    try (Sqlite db = openDb()) {
      execSql(db, "create table t(a); insert into t(a) values(1),(2),(3)");
      final AggregateFunction f = new AggregateFunction<Integer>(){
          public void xStep(SqlFunction.Arguments args){
            final ValueHolder<Integer> agg = this.getAggregateState(args, 0);
            for( SqlFunction.Arguments.Arg arg : args ){
              agg.value += arg.getInt();
            }
          }
          public void xFinal(SqlFunction.Arguments args){
            final Integer v = this.takeAggregateState(args);
            if( null==v ) args.resultNull();
            else args.resultInt(v);
          }
          public void xDestroy(){
            ++xDestroyCalled.value;
          }
        };
      db.createFunction("summer", 1, f);
      q = db.prepare(
        "with cte(v) as ("+
        "select 3 union all select 5 union all select 7"+
        ") select summer(v), summer(v+1) from cte"
        /* ------------------^^^^^^^^^^^ ensures that we're handling
           sqlite3_aggregate_context() properly. */
      );
      affirm( q.step() );
      affirm( 15==q.columnInt(0) );
      q.finalizeStmt();
      q = null;
      affirm( 0 == xDestroyCalled.value );
      db.createFunction("summerN", -1, f);

      q = db.prepare("select summerN(1,8,9), summerN(2,3,4)");
      affirm( q.step() );
      affirm( 18==q.columnInt(0) );
      affirm( 9==q.columnInt(1) );
      q.finalizeStmt();
      q = null;

    }/*db*/
    finally{
      if( null!=q ) q.finalizeStmt();
    }
    affirm( 2 == xDestroyCalled.value
            /* because we've bound the same instance twice */ );
  }

  private void testUdfWindow(){
    final Sqlite db = openDb();
    /* Example window function, table, and results taken from:
       https://sqlite.org/windowfunctions.html#udfwinfunc */
    final WindowFunction func = new WindowFunction<Integer>(){
        //! Impl of xStep() and xInverse()
        private void xStepInverse(SqlFunction.Arguments args, int v){
          this.getAggregateState(args,0).value += v;
        }
        @Override public void xStep(SqlFunction.Arguments args){
          this.xStepInverse(args, args.getInt(0));
        }
        @Override public void xInverse(SqlFunction.Arguments args){
          this.xStepInverse(args, -args.getInt(0));
        }
        //! Impl of xFinal() and xValue()
        private void xFinalValue(SqlFunction.Arguments args, Integer v){
          if(null == v) args.resultNull();
          else args.resultInt(v);
        }
        @Override public void xFinal(SqlFunction.Arguments args){
          xFinalValue(args, this.takeAggregateState(args));
          affirm( null == this.getAggregateState(args,null).value );
        }
        @Override public void xValue(SqlFunction.Arguments args){
          xFinalValue(args, this.getAggregateState(args,null).value);
        }
      };
    db.createFunction("winsumint", 1, func);
    execSql(db, new String[] {
        "CREATE TEMP TABLE twin(x, y); INSERT INTO twin VALUES",
        "('a', 4),('b', 5),('c', 3),('d', 8),('e', 1)"
      });
    final Sqlite.Stmt stmt = db.prepare(
      "SELECT x, winsumint(y) OVER ("+
      "ORDER BY x ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING"+
      ") AS sum_y "+
      "FROM twin ORDER BY x;"
    );
    int n = 0;
    while( stmt.step() ){
      final String s = stmt.columnText16(0);
      final int i = stmt.columnInt(1);
      switch(++n){
        case 1: affirm( "a".equals(s) && 9==i ); break;
        case 2: affirm( "b".equals(s) && 12==i ); break;
        case 3: affirm( "c".equals(s) && 16==i ); break;
        case 4: affirm( "d".equals(s) && 12==i ); break;
        case 5: affirm( "e".equals(s) && 9==i ); break;
        default: affirm( false /* cannot happen */ );
      }
    }
    stmt.close();
    affirm( 5 == n );
    db.close();
  }

  private void testKeyword(){
    final int n = Sqlite.keywordCount();
    affirm( n>0 );
    affirm( !Sqlite.keywordCheck("_nope_") );
    affirm( Sqlite.keywordCheck("seLect") );
    affirm( null!=Sqlite.keywordName(0) );
    affirm( null!=Sqlite.keywordName(n-1) );
    affirm( null==Sqlite.keywordName(n) );
  }


  private void testExplain(){
    final Sqlite db = openDb();
    Sqlite.Stmt q = db.prepare("SELECT 1");
    affirm( 0 == q.isExplain() );
    q.explain(0);
    affirm( 0 == q.isExplain() );
    q.explain(1);
    affirm( 1 == q.isExplain() );
    q.explain(2);
    affirm( 2 == q.isExplain() );
    Exception ex = null;
    try{
      q.explain(-1);
    }catch(Exception e){
      ex = e;
    }
    affirm( ex instanceof SqliteException );
    q.finalizeStmt();
    db.close();
  }


  private void testTrace(){
    final Sqlite db = openDb();
    final ValueHolder<Integer> counter = new ValueHolder<>(0);
    /* Ensure that characters outside of the UTF BMP survive the trip
       from Java to sqlite3 and back to Java. (At no small efficiency
       penalty.) */
    final String nonBmpChar = "😃";
    db.trace(
      Sqlite.TRACE_ALL,
      new Sqlite.TraceCallback(){
        @Override public void call(int traceFlag, Object pNative, Object x){
          ++counter.value;
          //outln("TRACE "+traceFlag+" pNative = "+pNative.getClass().getName());
          switch(traceFlag){
            case Sqlite.TRACE_STMT:
              affirm(pNative instanceof Sqlite.Stmt);
              //outln("TRACE_STMT sql = "+x);
              affirm(x instanceof String);
              affirm( ((String)x).indexOf(nonBmpChar) > 0 );
              break;
            case Sqlite.TRACE_PROFILE:
              affirm(pNative instanceof Sqlite.Stmt);
              affirm(x instanceof Long);
              //outln("TRACE_PROFILE time = "+x);
              break;
            case Sqlite.TRACE_ROW:
              affirm(pNative instanceof Sqlite.Stmt);
              affirm(null == x);
              //outln("TRACE_ROW = "+sqlite3_column_text16((sqlite3_stmt)pNative, 0));
              break;
            case Sqlite.TRACE_CLOSE:
              affirm(pNative instanceof Sqlite);
              affirm(null == x);
              break;
            default:
              affirm(false /*cannot happen*/);
              break;
          }
        }
      });
    execSql(db, "SELECT coalesce(null,null,'"+nonBmpChar+"'); "+
            "SELECT 'w"+nonBmpChar+"orld'");
    affirm( 6 == counter.value );
    db.close();
    affirm( 7 == counter.value );
  }

  private void testStatus(){
    final Sqlite db = openDb();
    execSql(db, "create table t(a); insert into t values(1),(2),(3)");

    Sqlite.Status s = Sqlite.libStatus(Sqlite.STATUS_MEMORY_USED, false);
    affirm( s.current > 0 );
    affirm( s.peak >= s.current );

    s = db.status(Sqlite.DBSTATUS_SCHEMA_USED, false);
    affirm( s.current > 0 );
    affirm( s.peak == 0 /* always 0 for SCHEMA_USED */ );

    db.close();
  }

  @SingleThreadOnly /* because multiple threads legitimately make these
                       results unpredictable */
  private synchronized void testAutoExtension(){
    final ValueHolder<Integer> val = new ValueHolder<>(0);
    final ValueHolder<String> toss = new ValueHolder<>(null);
    final Sqlite.AutoExtension ax = new Sqlite.AutoExtension(){
        @Override public void call(Sqlite db){
          ++val.value;
          if( null!=toss.value ){
            throw new RuntimeException(toss.value);
          }
        }
      };
    Sqlite.addAutoExtension(ax);
    openDb().close();
    affirm( 1==val.value );
    openDb().close();
    affirm( 2==val.value );
    Sqlite.clearAutoExtensions();
    openDb().close();
    affirm( 2==val.value );

    Sqlite.addAutoExtension( ax );
    Sqlite.addAutoExtension( ax ); // Must not add a second entry
    Sqlite.addAutoExtension( ax ); // or a third one
    openDb().close();
    affirm( 3==val.value );

    Sqlite db = openDb();
    affirm( 4==val.value );
    execSql(db, "ATTACH ':memory:' as foo");
    affirm( 4==val.value, "ATTACH uses the same connection, not sub-connections." );
    db.close();
    db = null;

    Sqlite.removeAutoExtension(ax);
    openDb().close();
    affirm( 4==val.value );
    Sqlite.addAutoExtension(ax);
    Exception err = null;
    toss.value = "Throwing from auto_extension.";
    try{
      openDb();
    }catch(Exception e){
      err = e;
    }
    affirm( err!=null );
    affirm( err.getMessage().contains(toss.value) );
    toss.value = null;

    val.value = 0;
    final Sqlite.AutoExtension ax2 = new Sqlite.AutoExtension(){
        @Override public void call(Sqlite db){
          ++val.value;
        }
      };
    Sqlite.addAutoExtension(ax2);
    openDb().close();
    affirm( 2 == val.value );
    Sqlite.removeAutoExtension(ax);
    openDb().close();
    affirm( 3 == val.value );
    Sqlite.addAutoExtension(ax);
    openDb().close();
    affirm( 5 == val.value );
    Sqlite.removeAutoExtension(ax2);
    openDb().close();
    affirm( 6 == val.value );
    Sqlite.addAutoExtension(ax2);
    openDb().close();
    affirm( 8 == val.value );

    Sqlite.clearAutoExtensions();
    openDb().close();
    affirm( 8 == val.value );
  }

  private void testBackup(){
    final Sqlite dbDest = openDb();

    try (Sqlite dbSrc = openDb()) {
      execSql(dbSrc, new String[]{
          "pragma page_size=512; VACUUM;",
          "create table t(a);",
          "insert into t(a) values(1),(2),(3);"
        });
      Exception e = null;
      try {
        dbSrc.initBackup("main",dbSrc,"main");
      }catch(Exception x){
        e = x;
      }
      affirm( e instanceof SqliteException );
      e = null;
      try (Sqlite.Backup b = dbDest.initBackup("main",dbSrc,"main")) {
        affirm( null!=b );
        int rc;
        while( Sqlite.DONE!=(rc = b.step(1)) ){
          affirm( 0==rc );
        }
        affirm( b.pageCount() > 0 );
        b.finish();
      }
    }

    try (Sqlite.Stmt q = dbDest.prepare("SELECT sum(a) from t")) {
      q.step();
      affirm( q.columnInt(0) == 6 );
    }
    dbDest.close();
  }

  private void testCollation(){
    final Sqlite db = openDb();
    execSql(db, "CREATE TABLE t(a); INSERT INTO t(a) VALUES('a'),('b'),('c')");
    final Sqlite.Collation myCollation = new Sqlite.Collation() {
        private final String myState =
          "this is local state. There is much like it, but this is mine.";
        @Override
        // Reverse-sorts its inputs...
        public int call(byte[] lhs, byte[] rhs){
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
      };
    final Sqlite.CollationNeeded collLoader = new Sqlite.CollationNeeded(){
        @Override
        public void call(Sqlite dbArg, int eTextRep, String collationName){
          affirm(dbArg == db);
          db.createCollation("reversi", eTextRep, myCollation);
        }
      };
    db.onCollationNeeded(collLoader);
    Sqlite.Stmt stmt = db.prepare("SELECT a FROM t ORDER BY a COLLATE reversi");
    int counter = 0;
    while( stmt.step() ){
      final String val = stmt.columnText16(0);
      ++counter;
      switch(counter){
        case 1: affirm("c".equals(val)); break;
        case 2: affirm("b".equals(val)); break;
        case 3: affirm("a".equals(val)); break;
      }
    }
    affirm(3 == counter);
    stmt.finalizeStmt();
    stmt = db.prepare("SELECT a FROM t ORDER BY a");
    counter = 0;
    while( stmt.step() ){
      final String val = stmt.columnText16(0);
      ++counter;
      //outln("Non-REVERSI'd row#"+counter+": "+val);
      switch(counter){
        case 3: affirm("c".equals(val)); break;
        case 2: affirm("b".equals(val)); break;
        case 1: affirm("a".equals(val)); break;
      }
    }
    affirm(3 == counter);
    stmt.finalizeStmt();
    db.onCollationNeeded(null);
    db.close();
  }

  @SingleThreadOnly /* because threads inherently break this test */
  private void testBusy(){
    final String dbName = "_busy-handler.db";
    try{
      Sqlite db1 = openDb(dbName);
      ++metrics.dbOpen;
      execSql(db1, "CREATE TABLE IF NOT EXISTS t(a)");
      Sqlite db2 = openDb(dbName);
      ++metrics.dbOpen;

      final ValueHolder<Integer> xBusyCalled = new ValueHolder<>(0);
      Sqlite.BusyHandler handler = new Sqlite.BusyHandler(){
          @Override public int call(int n){
            return n > 2 ? 0 : ++xBusyCalled.value;
          }
        };
      db2.setBusyHandler(handler);

      // Force a locked condition...
      execSql(db1, "BEGIN EXCLUSIVE");
      int rc = 0;
      SqliteException ex = null;
      try{
        db2.prepare("SELECT * from t");
      }catch(SqliteException x){
        ex = x;
      }
      affirm( null!=ex );
      affirm( Sqlite.BUSY == ex.errcode() );
      affirm( 3 == xBusyCalled.value );
      db1.close();
      db2.close();
    }finally{
      try{(new java.io.File(dbName)).delete();}
      catch(Exception e){/* ignore */}
    }
  }

  private void testCommitHook(){
    final Sqlite db = openDb();
    final ValueHolder<Integer> counter = new ValueHolder<>(0);
    final ValueHolder<Integer> hookResult = new ValueHolder<>(0);
    final Sqlite.CommitHook theHook = new Sqlite.CommitHook(){
        @Override public int call(){
          ++counter.value;
          return hookResult.value;
        }
      };
    Sqlite.CommitHook oldHook = db.setCommitHook(theHook);
    affirm( null == oldHook );
    execSql(db, "CREATE TABLE t(a); INSERT INTO t(a) VALUES('a'),('b'),('c')");
    affirm( 2 == counter.value );
    execSql(db, "BEGIN; SELECT 1; SELECT 2; COMMIT;");
    affirm( 2 == counter.value /* NOT invoked if no changes are made */ );
    execSql(db, "BEGIN; update t set a='d' where a='c'; COMMIT;");
    affirm( 3 == counter.value );
    oldHook = db.setCommitHook(theHook);
    affirm( theHook == oldHook );
    execSql(db, "BEGIN; update t set a='e' where a='d'; COMMIT;");
    affirm( 4 == counter.value );
    oldHook = db.setCommitHook(null);
    affirm( theHook == oldHook );
    execSql(db, "BEGIN; update t set a='f' where a='e'; COMMIT;");
    affirm( 4 == counter.value );
    oldHook = db.setCommitHook(null);
    affirm( null == oldHook );
    execSql(db, "BEGIN; update t set a='g' where a='f'; COMMIT;");
    affirm( 4 == counter.value );

    final Sqlite.CommitHook newHook = new Sqlite.CommitHook(){
        @Override public int call(){return 0;}
      };
    oldHook = db.setCommitHook(newHook);
    affirm( null == oldHook );
    execSql(db, "BEGIN; update t set a='h' where a='g'; COMMIT;");
    affirm( 4 == counter.value );
    oldHook = db.setCommitHook(theHook);
    affirm( newHook == oldHook );
    execSql(db, "BEGIN; update t set a='i' where a='h'; COMMIT;");
    affirm( 5 == counter.value );
    hookResult.value = Sqlite.ERROR;
    int rc = execSql(db, false, "BEGIN; update t set a='j' where a='i'; COMMIT;");
    affirm( Sqlite.CONSTRAINT_COMMITHOOK == rc );
    affirm( 6 == counter.value );
    db.close();
  }

  private void testRollbackHook(){
    final Sqlite db = openDb();
    final ValueHolder<Integer> counter = new ValueHolder<>(0);
    final Sqlite.RollbackHook theHook = new Sqlite.RollbackHook(){
        @Override public void call(){
          ++counter.value;
        }
      };
    Sqlite.RollbackHook oldHook = db.setRollbackHook(theHook);
    affirm( null == oldHook );
    execSql(db, "CREATE TABLE t(a); INSERT INTO t(a) VALUES('a'),('b'),('c')");
    affirm( 0 == counter.value );
    execSql(db, false, "BEGIN; SELECT 1; SELECT 2; ROLLBACK;");
    affirm( 1 == counter.value /* contra to commit hook, is invoked if no changes are made */ );

    final Sqlite.RollbackHook newHook = new Sqlite.RollbackHook(){
        @Override public void call(){}
      };
    oldHook = db.setRollbackHook(newHook);
    affirm( theHook == oldHook );
    execSql(db, false, "BEGIN; SELECT 1; ROLLBACK;");
    affirm( 1 == counter.value );
    oldHook = db.setRollbackHook(theHook);
    affirm( newHook == oldHook );
    execSql(db, false, "BEGIN; SELECT 1; ROLLBACK;");
    affirm( 2 == counter.value );
    int rc = execSql(db, false, "BEGIN; SELECT 1; ROLLBACK;");
    affirm( 0 == rc );
    affirm( 3 == counter.value );
    db.close();
  }

  private void testUpdateHook(){
    final Sqlite db = openDb();
    final ValueHolder<Integer> counter = new ValueHolder<>(0);
    final ValueHolder<Integer> expectedOp = new ValueHolder<>(0);
    final Sqlite.UpdateHook theHook = new Sqlite.UpdateHook(){
        @Override
        public void call(int opId, String dbName, String tableName, long rowId){
          ++counter.value;
          if( 0!=expectedOp.value ){
            affirm( expectedOp.value == opId );
          }
        }
      };
    Sqlite.UpdateHook oldHook = db.setUpdateHook(theHook);
    affirm( null == oldHook );
    expectedOp.value = Sqlite.INSERT;
    execSql(db, "CREATE TABLE t(a); INSERT INTO t(a) VALUES('a'),('b'),('c')");
    affirm( 3 == counter.value );
    expectedOp.value = Sqlite.UPDATE;
    execSql(db, "update t set a='d' where a='c';");
    affirm( 4 == counter.value );
    oldHook = db.setUpdateHook(theHook);
    affirm( theHook == oldHook );
    expectedOp.value = Sqlite.DELETE;
    execSql(db, "DELETE FROM t where a='d'");
    affirm( 5 == counter.value );
    oldHook = db.setUpdateHook(null);
    affirm( theHook == oldHook );
    execSql(db, "update t set a='e' where a='b';");
    affirm( 5 == counter.value );
    oldHook = db.setUpdateHook(null);
    affirm( null == oldHook );

    final Sqlite.UpdateHook newHook = new Sqlite.UpdateHook(){
        @Override public void call(int opId, String dbName, String tableName, long rowId){
        }
      };
    oldHook = db.setUpdateHook(newHook);
    affirm( null == oldHook );
    execSql(db, "update t set a='h' where a='a'");
    affirm( 5 == counter.value );
    oldHook = db.setUpdateHook(theHook);
    affirm( newHook == oldHook );
    expectedOp.value = Sqlite.UPDATE;
    execSql(db, "update t set a='i' where a='h'");
    affirm( 6 == counter.value );
    db.close();
  }

  private void testProgress(){
    final Sqlite db = openDb();
    final ValueHolder<Integer> counter = new ValueHolder<>(0);
    db.setProgressHandler(1, new Sqlite.ProgressHandler(){
        @Override public int call(){
          ++counter.value;
          return 0;
        }
      });
    execSql(db, "SELECT 1; SELECT 2;");
    affirm( counter.value > 0 );
    int nOld = counter.value;
    db.setProgressHandler(0, null);
    execSql(db, "SELECT 1; SELECT 2;");
    affirm( nOld == counter.value );
    db.close();
  }

  private void testAuthorizer(){
    final Sqlite db = openDb();
    final ValueHolder<Integer> counter = new ValueHolder<>(0);
    final ValueHolder<Integer> authRc = new ValueHolder<>(0);
    final Sqlite.Authorizer auth = new Sqlite.Authorizer(){
        public int call(int op, String s0, String s1, String s2, String s3){
          ++counter.value;
          //outln("xAuth(): "+s0+" "+s1+" "+s2+" "+s3);
          return authRc.value;
        }
      };
    execSql(db, "CREATE TABLE t(a); INSERT INTO t(a) VALUES('a'),('b'),('c')");
    db.setAuthorizer(auth);
    execSql(db, "UPDATE t SET a=1");
    affirm( 1 == counter.value );
    authRc.value = Sqlite.DENY;
    int rc = execSql(db, false, "UPDATE t SET a=2");
    affirm( Sqlite.AUTH==rc );
    db.setAuthorizer(null);
    rc = execSql(db, false, "UPDATE t SET a=2");
    affirm( 0==rc );
    db.close();
  }

  private void testBlobOpen(){
    final Sqlite db = openDb();

    execSql(db, "CREATE TABLE T(a BLOB);"
            +"INSERT INTO t(rowid,a) VALUES(1, 'def'),(2, 'XYZ');"
    );
    Sqlite.Blob b = db.blobOpen("main", "t", "a",
                                db.lastInsertRowId(), true);
    affirm( 3==b.bytes() );
    b.write(new byte[] {100, 101, 102 /*"DEF"*/}, 0);
    b.close();
    Sqlite.Stmt stmt = db.prepare("SELECT length(a), a FROM t ORDER BY a");
    affirm( stmt.step() );
    affirm( 3 == stmt.columnInt(0) );
    affirm( "def".equals(stmt.columnText16(1)) );
    stmt.finalizeStmt();

    b = db.blobOpen("main", "t", "a", db.lastInsertRowId(), false);
    final byte[] tgt = new byte[3];
    b.read( tgt, 0 );
    affirm( 100==tgt[0] && 101==tgt[1] && 102==tgt[2], "DEF" );
    execSql(db,"UPDATE t SET a=zeroblob(10) WHERE rowid=2");
    b.close();
    b = db.blobOpen("main", "t", "a", db.lastInsertRowId(), true);
    byte[] bw = new byte[]{
      0, 1, 2, 3, 4, 5, 6, 7, 8, 9
    };
    b.write(bw, 0);
    byte[] br = new byte[10];
    b.read(br, 0);
    for( int i = 0; i < br.length; ++i ){
      affirm(bw[i] == br[i]);
    }
    b.close();
    db.close();
  }

  void testPrepareMulti(){
    final ValueHolder<Integer> fCount = new ValueHolder<>(0);
    final ValueHolder<Integer> mCount = new ValueHolder<>(0);
    try (Sqlite db = openDb()) {
      execSql(db, "create table t(a); insert into t(a) values(1),(2),(3)");
      db.createFunction("counter", -1, new ScalarFunction(){
          @Override public void xFunc(SqlFunction.Arguments args){
            ++fCount.value;
            args.resultNull();
          }
        }
      );
      final Sqlite.PrepareMulti pm = new Sqlite.PrepareMultiFinalize(
        new Sqlite.PrepareMulti() {
          @Override public void call(Sqlite.Stmt q){
            ++mCount.value;
            while(q.step()){}
          }
        }
      );
      final String sql = "select counter(*) from t;"+
        "select counter(*) from t; /* comment */"+
        "select counter(*) from t; -- comment\n"
        ;
      db.prepareMulti(sql, pm);
    }
    affirm( 3 == mCount.value );
    affirm( 9 == fCount.value );
  }


  /* Copy/paste/rename this to add new tests. */
  private void _testTemplate(){
    try (Sqlite db = openDb()) {
      Sqlite.Stmt stmt = db.prepare("SELECT 1");
      stmt.finalizeStmt();
    }
  }

  private void runTests(boolean fromThread) throws Exception {
    List<java.lang.reflect.Method> mlist = testMethods;
    affirm( null!=mlist );
    if( shuffle ){
      mlist = new ArrayList<>( testMethods.subList(0, testMethods.size()) );
      java.util.Collections.shuffle(mlist);
    }
    if( (!fromThread && listRunTests>0) || listRunTests>1 ){
      synchronized(this.getClass()){
        if( !fromThread ){
          out("Initial test"," list: ");
          for(java.lang.reflect.Method m : testMethods){
            out(m.getName()+" ");
          }
          outln();
          outln("(That list excludes some which are hard-coded to run.)");
        }
        out("Running"," tests: ");
        for(java.lang.reflect.Method m : mlist){
          out(m.getName()+" ");
        }
        outln();
      }
    }
    for(java.lang.reflect.Method m : mlist){
      nap();
      try{
        m.invoke(this);
      }catch(java.lang.reflect.InvocationTargetException e){
        outln("FAILURE: ",m.getName(),"(): ", e.getCause());
        throw e;
      }
    }
    synchronized( this.getClass() ){
      ++nTestRuns;
    }
  }

  public void run() {
    try {
      runTests(0!=this.tId);
    }catch(Exception e){
      synchronized( listErrors ){
        listErrors.add(e);
      }
    }finally{
      Sqlite.uncacheThread();
    }
  }

  /**
     Runs the basic sqlite3 JNI binding sanity-check suite.

     CLI flags:

     -q|-quiet: disables most test output.

     -t|-thread N: runs the tests in N threads
      concurrently. Default=1.

     -r|-repeat N: repeats the tests in a loop N times, each one
      consisting of the -thread value's threads.

     -shuffle: randomizes the order of most of the test functions.

     -naps: sleep small random intervals between tests in order to add
     some chaos for cross-thread contention.

     -list-tests: outputs the list of tests being run, minus some
      which are hard-coded. In multi-threaded mode, use this twice to
      to emit the list run by each thread (which may differ from the initial
      list, in particular if -shuffle is used).

     -fail: forces an exception to be thrown during the test run.  Use
     with -shuffle to make its appearance unpredictable.

     -v: emit some developer-mode info at the end.
  */
  public static void main(String[] args) throws Exception {
    int nThread = 1;
    int nRepeat = 1;
    boolean doSomethingForDev = false;
    boolean forceFail = false;
    boolean sqlLog = false;
    boolean configLog = false;
    boolean squelchTestOutput = false;
    for( int i = 0; i < args.length; ){
      String arg = args[i++];
      if(arg.startsWith("-")){
        arg = arg.replaceFirst("-+","");
        if(arg.equals("v")){
          doSomethingForDev = true;
          //listBoundMethods();
        }else if(arg.equals("t") || arg.equals("thread")){
          nThread = Integer.parseInt(args[i++]);
        }else if(arg.equals("r") || arg.equals("repeat")){
          nRepeat = Integer.parseInt(args[i++]);
        }else if(arg.equals("shuffle")){
          shuffle = true;
        }else if(arg.equals("list-tests")){
          ++listRunTests;
        }else if(arg.equals("fail")){
          forceFail = true;
        }else if(arg.equals("sqllog")){
          sqlLog = true;
        }else if(arg.equals("configlog")){
          configLog = true;
        }else if(arg.equals("naps")){
          takeNaps = true;
        }else if(arg.equals("q") || arg.equals("quiet")){
          squelchTestOutput = true;
        }else{
          throw new IllegalArgumentException("Unhandled flag:"+arg);
        }
      }
    }

    if( sqlLog ){
      if( Sqlite.compileOptionUsed("ENABLE_SQLLOG") ){
        Sqlite.libConfigSqlLog( new Sqlite.ConfigSqlLog() {
            @Override public void call(Sqlite db, String msg, int op){
              switch(op){
                case 0: outln("Opening db: ",db); break;
                case 1: outln("SQL ",db,": ",msg); break;
                case 2: outln("Closing db: ",db); break;
              }
            }
          }
        );
      }else{
        outln("WARNING: -sqllog is not active because library was built ",
              "without SQLITE_ENABLE_SQLLOG.");
      }
    }
    if( configLog ){
      Sqlite.libConfigLog( new Sqlite.ConfigLog() {
          @Override public void call(int code, String msg){
            outln("ConfigLog: ",Sqlite.errstr(code),": ", msg);
          }
        }
      );
    }

    quietMode = squelchTestOutput;
    outln("If you just saw warning messages regarding CallStaticObjectMethod, ",
          "you are very likely seeing the side effects of a known openjdk8 ",
          "bug. It is unsightly but does not affect the library.");

    {
      // Build list of tests to run from the methods named test*().
      testMethods = new ArrayList<>();
      int nSkipped = 0;
      for(final java.lang.reflect.Method m : Tester2.class.getDeclaredMethods()){
        final String name = m.getName();
        if( name.equals("testFail") ){
          if( forceFail ){
            testMethods.add(m);
          }
        }else if( !m.isAnnotationPresent( ManualTest.class ) ){
          if( nThread>1 && m.isAnnotationPresent( SingleThreadOnly.class ) ){
            if( 0==nSkipped++ ){
              out("Skipping tests in multi-thread mode:");
            }
            out(" "+name+"()");
          }else if( name.startsWith("test") ){
            testMethods.add(m);
          }
        }
      }
      if( nSkipped>0 ) out("\n");
    }

    final long timeStart = System.currentTimeMillis();
    outln("libversion_number: ",
          Sqlite.libVersionNumber(),"\n",
          Sqlite.libVersion(),"\n",Sqlite.libSourceId(),"\n",
          "SQLITE_THREADSAFE=",CApi.sqlite3_threadsafe());
    final boolean showLoopCount = (nRepeat>1 && nThread>1);
    if( showLoopCount ){
      outln("Running ",nRepeat," loop(s) with ",nThread," thread(s) each.");
    }
    if( takeNaps ) outln("Napping between tests is enabled.");
    int nLoop = 0;
    for( int n = 0; n < nRepeat; ++n ){
      ++nLoop;
      if( showLoopCount ) out((1==nLoop ? "" : " ")+nLoop);
      if( nThread<=1 ){
        new Tester2(0).runTests(false);
        continue;
      }
      Tester2.mtMode = true;
      final ExecutorService ex = Executors.newFixedThreadPool( nThread );
      for( int i = 0; i < nThread; ++i ){
        ex.submit( new Tester2(i), i );
      }
      ex.shutdown();
      try{
        ex.awaitTermination(nThread*200, java.util.concurrent.TimeUnit.MILLISECONDS);
        ex.shutdownNow();
      }catch (InterruptedException ie){
        ex.shutdownNow();
        Thread.currentThread().interrupt();
      }
      if( !listErrors.isEmpty() ){
        quietMode = false;
        outln("TEST ERRORS:");
        Exception err = null;
        for( Exception e : listErrors ){
          e.printStackTrace();
          if( null==err ) err = e;
        }
        if( null!=err ) throw err;
      }
    }
    if( showLoopCount ) outln();
    quietMode = false;

    final long timeEnd = System.currentTimeMillis();
    outln("Tests done. Metrics across ",nTestRuns," total iteration(s):");
    outln("\tAssertions checked: ",affirmCount);
    outln("\tDatabases opened: ",metrics.dbOpen);
    if( doSomethingForDev ){
      CApi.sqlite3_jni_internal_details();
    }
    affirm( 0==Sqlite.libReleaseMemory(1) );
    CApi.sqlite3_shutdown();
    int nMethods = 0;
    int nNatives = 0;
    int nCanonical = 0;
    final java.lang.reflect.Method[] declaredMethods =
      CApi.class.getDeclaredMethods();
    for(java.lang.reflect.Method m : declaredMethods){
      final int mod = m.getModifiers();
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
    outln("\tCApi.sqlite3_*() methods: "+
          nMethods+" total, with "+
          nNatives+" native, "+
          (nMethods - nNatives)+" Java"
    );
    outln("\tTotal test time = "
          +(timeEnd - timeStart)+"ms");
  }
}
