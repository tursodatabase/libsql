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
package org.sqlite.jni;
import static org.sqlite.jni.CApi.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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

  @SingleThreadOnly /* because it's thread-agnostic */
  private void test1(){
    affirm(sqlite3_libversion_number() == SQLITE_VERSION_NUMBER);
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
    return Sqlite.open(name, SQLITE_OPEN_READWRITE|
                       SQLITE_OPEN_CREATE|
                       SQLITE_OPEN_EXRESCODE);
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
      affirm( SQLITE_ROW == stmt.step() );
      affirm( SQLITE_DONE == stmt.step() );
      stmt.reset();
      affirm( SQLITE_ROW == stmt.step() );
      affirm( SQLITE_DONE == stmt.step() );
      affirm( 0 == stmt.finalizeStmt() );
      affirm( null==stmt.nativeHandle() );

      stmt = db.prepare("SELECT 1");
      affirm( SQLITE_ROW == stmt.step() );
      affirm( 0 == stmt.finalizeStmt() )
        /* getting a non-0 out of sqlite3_finalize() is tricky */;
      affirm( null==stmt.nativeHandle() );
    }
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
      affirm( sqlite3_java_uncache_thread() );
      affirm( !sqlite3_java_uncache_thread() );
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
      if( sqlite3_compileoption_used("ENABLE_SQLLOG") ){
        final ConfigSqllogCallback log = new ConfigSqllogCallback() {
            @Override public void call(sqlite3 db, String msg, int op){
              switch(op){
                case 0: outln("Opening db: ",db); break;
                case 1: outln("SQL ",db,": ",msg); break;
                case 2: outln("Closing db: ",db); break;
              }
            }
          };
        int rc = sqlite3_config( log );
        affirm( 0==rc );
        rc = sqlite3_config( (ConfigSqllogCallback)null );
        affirm( 0==rc );
        rc = sqlite3_config( log );
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
      int rc = sqlite3_config( log );
      affirm( 0==rc );
      rc = sqlite3_config( (ConfigLogCallback)null );
      affirm( 0==rc );
      rc = sqlite3_config( log );
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
    switch( sqlite3_threadsafe() ){ /* Sanity checking */
      case 0:
        affirm( SQLITE_ERROR==sqlite3_config( SQLITE_CONFIG_SINGLETHREAD ),
                "Could not switch to single-thread mode." );
        affirm( SQLITE_ERROR==sqlite3_config( SQLITE_CONFIG_MULTITHREAD ),
                "Could switch to multithread mode."  );
        affirm( SQLITE_ERROR==sqlite3_config( SQLITE_CONFIG_SERIALIZED ),
                "Could not switch to serialized threading mode."  );
        outln("This is a single-threaded build. Not using threads.");
        nThread = 1;
        break;
      case 1:
      case 2:
        affirm( 0==sqlite3_config( SQLITE_CONFIG_SINGLETHREAD ),
                "Could not switch to single-thread mode." );
        affirm( 0==sqlite3_config( SQLITE_CONFIG_MULTITHREAD ),
                "Could not switch to multithread mode."  );
        affirm( 0==sqlite3_config( SQLITE_CONFIG_SERIALIZED ),
                "Could not switch to serialized threading mode."  );
        break;
      default:
        affirm( false, "Unhandled SQLITE_THREADSAFE value." );
    }
    outln("libversion_number: ",
          sqlite3_libversion_number(),"\n",
          sqlite3_libversion(),"\n",SQLITE_SOURCE_ID,"\n",
          "SQLITE_THREADSAFE=",sqlite3_threadsafe());
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
      sqlite3_jni_internal_details();
    }
    affirm( 0==sqlite3_release_memory(1) );
    sqlite3_shutdown();
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
