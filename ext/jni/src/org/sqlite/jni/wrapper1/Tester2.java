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
//import static org.sqlite.jni.capi.CApi.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.sqlite.jni.capi.*;

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
  private static boolean listRunTests = false;
  //! True to squelch all out() and outln() output.
  private static boolean quietMode = false;
  //! Total number of runTests() calls.
  private static int nTestRuns = 0;
  //! List of test*() methods to run.
  private static List<java.lang.reflect.Method> testMethods = null;
  //! List of exceptions collected by run()
  private static List<Exception> listErrors = new ArrayList<>();
  private static final class Metrics {
    //! Number of times createNewDb() (or equivalent) is invoked.
    volatile int dbOpen = 0;
  }

  //! Instance ID.
  private Integer tId;

  Tester2(Integer id){
    tId = id;
  }

  static final Metrics metrics = new Metrics();

  public static synchronized void outln(){
    if( !quietMode ){
      System.out.println("");
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


  public static void execSql(Sqlite db, String[] sql){
    execSql(db, String.join("", sql));
  }

  public static int execSql(Sqlite dbw, boolean throwOnError, String sql){
    final sqlite3 db = dbw.nativeHandle();
    OutputPointer.Int32 oTail = new OutputPointer.Int32();
    final byte[] sqlUtf8 = sql.getBytes(StandardCharsets.UTF_8);
    int pos = 0, n = 1;
    byte[] sqlChunk = sqlUtf8;
    int rc = 0;
    sqlite3_stmt stmt = null;
    final OutputPointer.sqlite3_stmt outStmt = new OutputPointer.sqlite3_stmt();
    while(pos < sqlChunk.length){
      if(pos > 0){
        sqlChunk = Arrays.copyOfRange(sqlChunk, pos,
                                      sqlChunk.length);
      }
      if( 0==sqlChunk.length ) break;
      rc = CApi.sqlite3_prepare_v2(db, sqlChunk, outStmt, oTail);
      if(throwOnError) affirm(0 == rc);
      else if( 0!=rc ) break;
      pos = oTail.value;
      stmt = outStmt.take();
      if( null == stmt ){
        // empty statement was parsed.
        continue;
      }
      affirm(0 != stmt.getNativePointer());
      while( CApi.SQLITE_ROW == (rc = CApi.sqlite3_step(stmt)) ){
      }
      CApi.sqlite3_finalize(stmt);
      affirm(0 == stmt.getNativePointer());
      if(0!=rc && CApi.SQLITE_ROW!=rc && CApi.SQLITE_DONE!=rc){
        break;
      }
    }
    CApi.sqlite3_finalize(stmt);
    if(CApi.SQLITE_ROW==rc || CApi.SQLITE_DONE==rc) rc = 0;
    if( 0!=rc && throwOnError){
      throw new SqliteException(db);
    }
    return rc;
  }

  static void execSql(Sqlite db, String sql){
    execSql(db, true, sql);
  }

  @SingleThreadOnly /* because it's thread-agnostic */
  private void test1(){
    affirm(CApi.sqlite3_libversion_number() == CApi.SQLITE_VERSION_NUMBER);
  }

  /* Copy/paste/rename this to add new tests. */
  private void _testTemplate(){
    //final sqlite3 db = createNewDb();
    //sqlite3_stmt stmt = prepare(db,"SELECT 1");
    //sqlite3_finalize(stmt);
    //sqlite3_close_v2(db);
  }

  private void nap() throws InterruptedException {
    if( takeNaps ){
      Thread.sleep(java.util.concurrent.ThreadLocalRandom.current().nextInt(3, 17), 0);
    }
  }

  Sqlite openDb(String name){
    final Sqlite db = Sqlite.open(name, CApi.SQLITE_OPEN_READWRITE|
                                  CApi.SQLITE_OPEN_CREATE|
                                  CApi.SQLITE_OPEN_EXRESCODE);
    ++metrics.dbOpen;
    return db;
  }

  Sqlite openDb(){ return openDb(":memory:"); }

  void testOpenDb1(){
    Sqlite db = openDb();
    affirm( 0!=db.nativeHandle().getNativePointer() );
    db.close();
    affirm( null==db.nativeHandle() );

    SqliteException ex = null;
    try {
      db = openDb("/no/such/dir/.../probably");
    }catch(SqliteException e){
      ex = e;
    }
    affirm( ex!=null );
    affirm( ex.errcode() != 0 );
    affirm( ex.extendedErrcode() != 0 );
    affirm( ex.errorOffset() < 0 );
    // there's no reliable way to predict what ex.systemErrno() might be
  }

  void testPrepare1(){
    try (Sqlite db = openDb()) {
      Sqlite.Stmt stmt = db.prepare("SELECT 1");
      affirm( null!=stmt.nativeHandle() );
      affirm( CApi.SQLITE_ROW == stmt.step() );
      affirm( CApi.SQLITE_DONE == stmt.step() );
      stmt.reset();
      affirm( CApi.SQLITE_ROW == stmt.step() );
      affirm( CApi.SQLITE_DONE == stmt.step() );
      affirm( 0 == stmt.finalizeStmt() );
      affirm( null==stmt.nativeHandle() );

      stmt = db.prepare("SELECT 1");
      affirm( CApi.SQLITE_ROW == stmt.step() );
      affirm( 0 == stmt.finalizeStmt() )
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
            for( SqlFunction.Arguments.Arg arg : args ){
              vh.value += arg.getInt();
            }
          }
          public void xDestroy(){
            ++xDestroyCalled.value;
          }
        };
      db.createFunction("myfunc", -1, f);
      execSql(db, "select myfunc(1,2,3)");
      affirm( 6 == vh.value );
      vh.value = 0;
      execSql(db, "select myfunc(-1,-2,-3)");
      affirm( -6 == vh.value );
      affirm( 0 == xDestroyCalled.value );
    }
    affirm( 1 == xDestroyCalled.value );
  }

  void testUdfAggregate(){
    final ValueHolder<Integer> xDestroyCalled = new ValueHolder<>(0);
    final ValueHolder<Integer> vh = new ValueHolder<>(0);
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
            vh.value = v;
          }
          public void xDestroy(){
            ++xDestroyCalled.value;
          }
        };
      db.createFunction("myagg", -1, f);
      execSql(db, "select myagg(a) from t");
      affirm( 6 == vh.value );
      affirm( 0 == xDestroyCalled.value );
    }
    affirm( 1 == xDestroyCalled.value );
  }

  private void runTests(boolean fromThread) throws Exception {
    List<java.lang.reflect.Method> mlist = testMethods;
    affirm( null!=mlist );
    if( shuffle ){
      mlist = new ArrayList<>( testMethods.subList(0, testMethods.size()) );
      java.util.Collections.shuffle(mlist);
    }
    if( listRunTests ){
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
      affirm( CApi.sqlite3_java_uncache_thread() );
      affirm( !CApi.sqlite3_java_uncache_thread() );
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
      which are hard-coded. This is noisy in multi-threaded mode.

     -fail: forces an exception to be thrown during the test run.  Use
     with -shuffle to make its appearance unpredictable.

     -v: emit some developer-mode info at the end.
  */
  public static void main(String[] args) throws Exception {
    Integer nThread = 1;
    boolean doSomethingForDev = false;
    Integer nRepeat = 1;
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
          listRunTests = true;
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
      if( CApi.sqlite3_compileoption_used("ENABLE_SQLLOG") ){
        final ConfigSqllogCallback log = new ConfigSqllogCallback() {
            @Override public void call(sqlite3 db, String msg, int op){
              switch(op){
                case 0: outln("Opening db: ",db); break;
                case 1: outln("SQL ",db,": ",msg); break;
                case 2: outln("Closing db: ",db); break;
              }
            }
          };
        int rc = CApi.sqlite3_config( log );
        affirm( 0==rc );
        rc = CApi.sqlite3_config( (ConfigSqllogCallback)null );
        affirm( 0==rc );
        rc = CApi.sqlite3_config( log );
        affirm( 0==rc );
      }else{
        outln("WARNING: -sqllog is not active because library was built ",
              "without SQLITE_ENABLE_SQLLOG.");
      }
    }
    if( configLog ){
      final ConfigLogCallback log = new ConfigLogCallback() {
          @Override public void call(int code, String msg){
            outln("ConfigLogCallback: ",ResultCode.getEntryForInt(code),": ", msg);
          };
        };
      int rc = CApi.sqlite3_config( log );
      affirm( 0==rc );
      rc = CApi.sqlite3_config( (ConfigLogCallback)null );
      affirm( 0==rc );
      rc = CApi.sqlite3_config( log );
      affirm( 0==rc );
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
    int nLoop = 0;
    switch( CApi.sqlite3_threadsafe() ){ /* Sanity checking */
      case 0:
        affirm( CApi.SQLITE_ERROR==CApi.sqlite3_config( CApi.SQLITE_CONFIG_SINGLETHREAD ),
                "Could not switch to single-thread mode." );
        affirm( CApi.SQLITE_ERROR==CApi.sqlite3_config( CApi.SQLITE_CONFIG_MULTITHREAD ),
                "Could switch to multithread mode."  );
        affirm( CApi.SQLITE_ERROR==CApi.sqlite3_config( CApi.SQLITE_CONFIG_SERIALIZED ),
                "Could not switch to serialized threading mode."  );
        outln("This is a single-threaded build. Not using threads.");
        nThread = 1;
        break;
      case 1:
      case 2:
        affirm( 0==CApi.sqlite3_config( CApi.SQLITE_CONFIG_SINGLETHREAD ),
                "Could not switch to single-thread mode." );
        affirm( 0==CApi.sqlite3_config( CApi.SQLITE_CONFIG_MULTITHREAD ),
                "Could not switch to multithread mode."  );
        affirm( 0==CApi.sqlite3_config( CApi.SQLITE_CONFIG_SERIALIZED ),
                "Could not switch to serialized threading mode."  );
        break;
      default:
        affirm( false, "Unhandled SQLITE_THREADSAFE value." );
    }
    outln("libversion_number: ",
          CApi.sqlite3_libversion_number(),"\n",
          CApi.sqlite3_libversion(),"\n",CApi.SQLITE_SOURCE_ID,"\n",
          "SQLITE_THREADSAFE=",CApi.sqlite3_threadsafe());
    final boolean showLoopCount = (nRepeat>1 && nThread>1);
    if( showLoopCount ){
      outln("Running ",nRepeat," loop(s) with ",nThread," thread(s) each.");
    }
    if( takeNaps ) outln("Napping between tests is enabled.");
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
    affirm( 0==CApi.sqlite3_release_memory(1) );
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
