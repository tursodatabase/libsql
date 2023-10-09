/*
** 2023-08-08
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains the main application entry pointer for the
** SQLTester framework.
*/
package org.sqlite.jni;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.nio.charset.StandardCharsets;
import java.util.regex.*;
import static org.sqlite.jni.CApi.*;

/**
   Modes for how to escape (or not) column values and names from
   SQLTester.execSql() to the result buffer output.
*/
enum ResultBufferMode {
  //! Do not append to result buffer
  NONE,
  //! Append output escaped.
  ESCAPED,
  //! Append output as-is
  ASIS
};

/**
   Modes to specify how to emit multi-row output from
   SQLTester.execSql() to the result buffer.
*/
enum ResultRowMode {
  //! Keep all result rows on one line, space-separated.
  ONELINE,
  //! Add a newline between each result row.
  NEWLINE
};

/**
   Base exception type for test-related failures.
*/
class SQLTesterException extends RuntimeException {
  private boolean bFatal = false;

  SQLTesterException(String msg){
    super(msg);
  }

  protected SQLTesterException(String msg, boolean fatal){
    super(msg);
    bFatal = fatal;
  }

  /**
     Indicates whether the framework should consider this exception
     type as immediately fatal to the test run or not.
  */
  final boolean isFatal(){ return bFatal; }
}

class DbException extends SQLTesterException {
  DbException(sqlite3 db, int rc, boolean closeDb){
    super("DB error #"+rc+": "+sqlite3_errmsg(db),true);
    if( closeDb ) sqlite3_close_v2(db);
  }
  DbException(sqlite3 db, int rc){
    this(db, rc, false);
  }
}

/**
   Generic test-failed exception.
 */
class TestScriptFailed extends SQLTesterException {
  public TestScriptFailed(TestScript ts, String msg){
    super(ts.getOutputPrefix()+": "+msg, true);
  }
}

/**
   Thrown when an unknown test command is encountered in a script.
*/
class UnknownCommand extends SQLTesterException {
  public UnknownCommand(TestScript ts, String cmd){
    super(ts.getOutputPrefix()+": unknown command: "+cmd, false);
  }
}

/**
   Thrown when an "incompatible directive" is found in a script.  This
   can be the presence of a C-preprocessor construct, specific
   metadata tags within a test script's header, or specific test
   constructs which are incompatible with this particular
   implementation.
*/
class IncompatibleDirective extends SQLTesterException {
  public IncompatibleDirective(TestScript ts, String line){
    super(ts.getOutputPrefix()+": incompatible directive: "+line, false);
  }
}

/**
   Console output utility class.
*/
class Outer {
  private int verbosity = 0;

  static void out(Object val){
    System.out.print(val);
  }

  Outer out(Object... vals){
    for(Object v : vals) out(v);
    return this;
  }

  Outer outln(Object... vals){
    out(vals).out("\n");
    return this;
  }

  Outer verbose(Object... vals){
    if(verbosity>0){
      out("VERBOSE",(verbosity>1 ? "+: " : ": ")).outln(vals);
    }
    return this;
  }

  void setVerbosity(int level){
    verbosity = level;
  }

  int getVerbosity(){
    return verbosity;
  }

  public boolean isVerbose(){return verbosity > 0;}

}

/**
   <p>This class provides an application which aims to implement the
   rudimentary SQL-driven test tool described in the accompanying
   {@code test-script-interpreter.md}.

   <p>This class is an internal testing tool, not part of the public
   interface but is (A) in the same package as the library because
   access permissions require it to be so and (B) the JDK8 javadoc
   offers no way to filter individual classes out of the doc
   generation process (it can only exclude packages, but see (A)).

   <p>An instance of this application provides a core set of services
   which TestScript instances use for processing testing logic.
   TestScripts, in turn, delegate the concrete test work to Command
   objects, which the TestScript parses on their behalf.
*/
public class SQLTester {
  //! List of input script files.
  private final java.util.List<String> listInFiles = new ArrayList<>();
  //! Console output utility.
  private final Outer outer = new Outer();
  //! Test input buffer.
  private final StringBuilder inputBuffer = new StringBuilder();
  //! Test result buffer.
  private final StringBuilder resultBuffer = new StringBuilder();
  //! Buffer for REQUIRED_PROPERTIES pragmas.
  private final StringBuilder dbInitSql = new StringBuilder();
  //! Output representation of SQL NULL.
  private String nullView = "nil";
  //! Total tests run.
  private int nTotalTest = 0;
  //! Total test script files run.
  private int nTestFile = 0;
  //! Number of scripts which were aborted.
  private int nAbortedScript = 0;
  //! Incremented by test case handlers
  private int nTest = 0;
  //! True to enable column name output from execSql()
  private boolean emitColNames;
  //! True to keep going regardless of how a test fails.
  private boolean keepGoing = false;
  //! The list of available db handles.
  private final sqlite3[] aDb = new sqlite3[7];
  //! Index into aDb of the current db.
  private int iCurrentDb = 0;
  //! Name of the default db, re-created for each script.
  private final String initialDbName = "test.db";


  public SQLTester(){
    reset();
  }

  void setVerbosity(int level){
    this.outer.setVerbosity( level );
  }
  int getVerbosity(){
    return this.outer.getVerbosity();
  }
  boolean isVerbose(){
    return this.outer.isVerbose();
  }

  void outputColumnNames(boolean b){ emitColNames = b; }

  void verbose(Object... vals){
    outer.verbose(vals);
  }

  void outln(Object... vals){
    outer.outln(vals);
  }

  void out(Object... vals){
    outer.out(vals);
  }

  //! Adds the given test script to the to-test list.
  public void addTestScript(String filename){
    listInFiles.add(filename);
    //verbose("Added file ",filename);
  }

  private void setupInitialDb() throws DbException {
    if( null==aDb[0] ){
      Util.unlink(initialDbName);
      openDb(0, initialDbName, true);
    }else{
      outln("WARNING: setupInitialDb() unexpectedly ",
            "triggered while it is opened.");
    }
  }

  static final String[] startEmoji = {
    "🚴", "🏄", "🏇", "🤸", "⛹", "🏊", "⛷", "🧗", "🏋"
  };
  static final int nStartEmoji = startEmoji.length;
  static int iStartEmoji = 0;

  private static String nextStartEmoji(){
    return startEmoji[iStartEmoji++ % nStartEmoji];
  }

  public void runTests() throws Exception {
    final long tStart = System.currentTimeMillis();
    for(String f : listInFiles){
      reset();
      ++nTestFile;
      final TestScript ts = new TestScript(f);
      outln(nextStartEmoji(), " starting [",f,"]");
      boolean threw = false;
      final long timeStart = System.currentTimeMillis();
      try{
        ts.run(this);
      }catch(SQLTesterException e){
        threw = true;
        outln("🔥EXCEPTION: ",e.getClass().getSimpleName(),": ",e.getMessage());
        ++nAbortedScript;
        if( keepGoing ) outln("Continuing anyway becaure of the keep-going option.");
        else if( e.isFatal() ) throw e;
      }finally{
        final long timeEnd = System.currentTimeMillis();
        outln("🏁",(threw ? "❌" : "✅")," ",nTest," test(s) in ",
              (timeEnd-timeStart),"ms.");
      }
    }
    final long tEnd = System.currentTimeMillis();
    outln("Total run-time: ",(tEnd-tStart),"ms");
    Util.unlink(initialDbName);
  }

  private StringBuilder clearBuffer(StringBuilder b){
    b.setLength(0);;
    return b;
  }

  StringBuilder clearInputBuffer(){
    return clearBuffer(inputBuffer);
  }

  StringBuilder clearResultBuffer(){
    return clearBuffer(resultBuffer);
  }

  StringBuilder getInputBuffer(){ return inputBuffer; }

  void appendInput(String n, boolean addNL){
    inputBuffer.append(n);
    if(addNL) inputBuffer.append('\n');
  }

  void appendResult(String n, boolean addNL){
    resultBuffer.append(n);
    if(addNL) resultBuffer.append('\n');
  }

  void appendDbInitSql(String n) throws DbException {
    dbInitSql.append(n).append('\n');
    if( null!=getCurrentDb() ){
      //outln("RUNNING DB INIT CODE: ",n);
      execSql(null, true, ResultBufferMode.NONE, null, n);
    }
  }
  String getDbInitSql(){ return dbInitSql.toString(); }

  String getInputText(){ return inputBuffer.toString(); }

  String getResultText(){ return resultBuffer.toString(); }

  private String takeBuffer(StringBuilder b){
    final String rc = b.toString();
    clearBuffer(b);
    return rc;
  }

  String takeInputBuffer(){ return takeBuffer(inputBuffer); }

  String takeResultBuffer(){ return takeBuffer(resultBuffer); }

  int getCurrentDbId(){ return iCurrentDb; }

  SQLTester affirmDbId(int n) throws IndexOutOfBoundsException {
    if(n<0 || n>=aDb.length){
      throw new IndexOutOfBoundsException("illegal db number: "+n);
    }
    return this;
  }

  sqlite3 setCurrentDb(int n) throws Exception{
    affirmDbId(n);
    iCurrentDb = n;
    return this.aDb[n];
  }

  sqlite3 getCurrentDb(){ return aDb[iCurrentDb]; }

  sqlite3 getDbById(int id) throws Exception{
    return affirmDbId(id).aDb[id];
  }

  void closeDb(int id) {
    final sqlite3 db = affirmDbId(id).aDb[id];
    if( null != db ){
      sqlite3_close_v2(db);
      aDb[id] = null;
    }
  }

  void closeDb() { closeDb(iCurrentDb); }

  void closeAllDbs(){
    for(int i = 0; i<aDb.length; ++i){
      sqlite3_close_v2(aDb[i]);
      aDb[i] = null;
    }
  }

  sqlite3 openDb(String name, boolean createIfNeeded) throws DbException {
    closeDb();
    int flags = SQLITE_OPEN_READWRITE;
    if( createIfNeeded ) flags |= SQLITE_OPEN_CREATE;
    final OutputPointer.sqlite3 out = new OutputPointer.sqlite3();
    int rc = sqlite3_open_v2(name, out, flags, null);
    final sqlite3 db = out.take();
    if( 0==rc && dbInitSql.length() > 0){
      //outln("RUNNING DB INIT CODE: ",dbInitSql.toString());
      rc = execSql(db, false, ResultBufferMode.NONE,
                   null, dbInitSql.toString());
    }
    if( 0!=rc ){
      throw new DbException(db, rc, true);
    }
    return aDb[iCurrentDb] = db;
  }

  sqlite3 openDb(int slot, String name, boolean createIfNeeded) throws DbException {
    affirmDbId(slot);
    iCurrentDb = slot;
    return openDb(name, createIfNeeded);
  }

  /**
     Resets all tester context state except for that related to
     tracking running totals.
  */
  void reset(){
    clearInputBuffer();
    clearResultBuffer();
    clearBuffer(dbInitSql);
    closeAllDbs();
    nTest = 0;
    nullView = "nil";
    emitColNames = false;
    iCurrentDb = 0;
    //dbInitSql.append("SELECT 1;");
  }

  void setNullValue(String v){nullView = v;}

  /**
     If true, encountering an unknown command in a script causes the
     remainder of the script to be skipped, rather than aborting the
     whole script run.
  */
  boolean skipUnknownCommands(){
    // Currently hard-coded. Potentially a flag someday.
    return true;
  }

  void incrementTestCounter(){ ++nTest; ++nTotalTest; }

  //! "Special" characters - we have to escape output if it contains any.
  static final Pattern patternSpecial = Pattern.compile(
    "[\\x00-\\x20\\x22\\x5c\\x7b\\x7d]"
  );
  //! Either of '{' or '}'.
  static final Pattern patternSquiggly = Pattern.compile("[{}]");

  /**
     Returns v or some escaped form of v, as defined in the tester's
     spec doc.
  */
  String escapeSqlValue(String v){
    if( "".equals(v) ) return "{}";
    Matcher m = patternSpecial.matcher(v);
    if( !m.find() ){
      return v  /* no escaping needed */;
    }
    m = patternSquiggly.matcher(v);
    if( !m.find() ){
      return "{"+v+"}";
    }
    final StringBuilder sb = new StringBuilder("\"");
    final int n = v.length();
    for(int i = 0; i < n; ++i){
      final char ch = v.charAt(i);
      switch(ch){
        case '\\': sb.append("\\\\"); break;
        case '"': sb.append("\\\""); break;
        default:
          //verbose("CHAR ",(int)ch," ",ch," octal=",String.format("\\%03o", (int)ch));
          if( (int)ch < 32 ) sb.append(String.format("\\%03o", (int)ch));
          else sb.append(ch);
          break;
      }
    }
    sb.append("\"");
    return sb.toString();
  }

  private void appendDbErr(sqlite3 db, StringBuilder sb, int rc){
    sb.append(org.sqlite.jni.ResultCode.getEntryForInt(rc)).append(' ');
    final String msg = escapeSqlValue(sqlite3_errmsg(db));
    if( '{' == msg.charAt(0) ){
      sb.append(msg);
    }else{
      sb.append('{').append(msg).append('}');
    }
  }

  /**
     Runs SQL on behalf of test commands and outputs the results following
     the very specific rules of the test framework.

     If db is null, getCurrentDb() is assumed. If throwOnError is true then
     any db-side error will result in an exception, else they result in
     the db's result code.

     appendMode specifies how/whether to append results to the result
     buffer. rowMode specifies whether to output all results in a
     single line or one line per row. If appendMode is
     ResultBufferMode.NONE then rowMode is ignored and may be null.
  */
  public int execSql(sqlite3 db, boolean throwOnError,
                     ResultBufferMode appendMode, ResultRowMode rowMode,
                     String sql) throws SQLTesterException {
    if( null==db && null==aDb[0] ){
      // Delay opening of the initial db to enable tests to change its
      // name and inject on-connect code via, e.g., the MEMDB
      // directive.  this setup as the potential to misinteract with
      // auto-extension timing and must be done carefully.
      setupInitialDb();
    }
    final OutputPointer.Int32 oTail = new OutputPointer.Int32();
    final OutputPointer.sqlite3_stmt outStmt = new OutputPointer.sqlite3_stmt();
    final byte[] sqlUtf8 = sql.getBytes(StandardCharsets.UTF_8);
    if( null==db ) db = getCurrentDb();
    int pos = 0, n = 1;
    byte[] sqlChunk = sqlUtf8;
    int rc = 0;
    sqlite3_stmt stmt = null;
    int spacing = 0 /* emit a space for --result if>0 */ ;
    final StringBuilder sb = (ResultBufferMode.NONE==appendMode)
      ? null : resultBuffer;
    //outln("sqlChunk len= = ",sqlChunk.length);
    try{
      while(pos < sqlChunk.length){
        if(pos > 0){
          sqlChunk = Arrays.copyOfRange(sqlChunk, pos,
                                        sqlChunk.length);
        }
        if( 0==sqlChunk.length ) break;
        rc = sqlite3_prepare_v2(db, sqlChunk, outStmt, oTail);
        /*outln("PREPARE rc ",rc," oTail=",oTail.get(),": ",
          new String(sqlChunk,StandardCharsets.UTF_8),"\n<EOSQL>");*/
        if( 0!=rc ){
          if(throwOnError){
            throw new DbException(db, rc);
          }else if( null!=sb ){
            appendDbErr(db, sb, rc);
          }
          break;
        }
        pos = oTail.value;
        stmt = outStmt.take();
        if( null == stmt ){
          // empty statement was parsed.
          continue;
        }
        if( null!=sb ){
          // Add the output to the result buffer...
          final int nCol = sqlite3_column_count(stmt);
          String colName = null, val = null;
          while( SQLITE_ROW == (rc = sqlite3_step(stmt)) ){
            for(int i = 0; i < nCol; ++i){
              if( spacing++ > 0 ) sb.append(' ');
              if( emitColNames ){
                colName = sqlite3_column_name(stmt, i);
                switch(appendMode){
                  case ASIS:
                    sb.append( colName );
                    break;
                  case ESCAPED:
                    sb.append( escapeSqlValue(colName) );
                    break;
                  default:
                    throw new SQLTesterException("Unhandled ResultBufferMode: "+appendMode);
                }
                sb.append(' ');
              }
              val = sqlite3_column_text16(stmt, i);
              if( null==val ){
                sb.append( nullView );
                continue;
              }
              switch(appendMode){
                case ASIS:
                  sb.append( val );
                  break;
                case ESCAPED:
                  sb.append( escapeSqlValue(val) );
                  break;
                default:
                  throw new SQLTesterException("Unhandled ResultBufferMode: "+appendMode);
              }
            }
            if( ResultRowMode.NEWLINE == rowMode ){
              spacing = 0;
              sb.append('\n');
            }
          }
        }else{
          while( SQLITE_ROW == (rc = sqlite3_step(stmt)) ){}
        }
        sqlite3_finalize(stmt);
        stmt = null;
        if(SQLITE_ROW==rc || SQLITE_DONE==rc) rc = 0;
        else if( rc!=0 ){
          if( null!=sb ){
            appendDbErr(db, sb, rc);
          }
          break;
        }
      }
    }finally{
      sqlite3_reset(stmt
        /* In order to trigger an exception in the
           INSERT...RETURNING locking scenario:
           https://sqlite.org/forum/forumpost/36f7a2e7494897df */);
      sqlite3_finalize(stmt);
    }
    if( 0!=rc && throwOnError ){
      throw new DbException(db, rc);
    }
    return rc;
  }

  public static void main(String[] argv) throws Exception{
    installCustomExtensions();
    boolean dumpInternals = false;
    final SQLTester t = new SQLTester();
    for(String a : argv){
      if(a.startsWith("-")){
        final String flag = a.replaceFirst("-+","");
        if( flag.equals("verbose") ){
          // Use --verbose up to 3 times
          t.setVerbosity(t.getVerbosity() + 1);
        }else if( flag.equals("keep-going") ){
          t.keepGoing = true;
        }else if( flag.equals("internals") ){
          dumpInternals = true;
        }else{
          throw new IllegalArgumentException("Unhandled flag: "+flag);
        }
        continue;
      }
      t.addTestScript(a);
    }
    final AutoExtensionCallback ax = new AutoExtensionCallback() {
        private final SQLTester tester = t;
        @Override public int call(sqlite3 db){
          final String init = tester.getDbInitSql();
          if( !init.isEmpty() ){
            tester.execSql(db, true, ResultBufferMode.NONE, null, init);
          }
          return 0;
        }
      };
    sqlite3_auto_extension(ax);
    try {
      t.runTests();
    }finally{
      sqlite3_cancel_auto_extension(ax);
      t.outln("Processed ",t.nTotalTest," test(s) in ",t.nTestFile," file(s).");
      if( t.nAbortedScript > 0 ){
        t.outln("Aborted ",t.nAbortedScript," script(s).");
      }
      if( dumpInternals ){
        sqlite3_jni_internal_details();
      }
    }
  }

  /**
     Internal impl of the public strglob() method. Neither argument
     may be NULL and both _MUST_ be NUL-terminated.
  */
  private static native int strglob(byte[] glob, byte[] txt);

  /**
     Works essentially the same as sqlite3_strglob() except that the
     glob character '#' matches a sequence of one or more digits.  It
     does not match when it appears at the start or middle of a series
     of digits, e.g. "#23" or "1#3", but will match at the end,
     e.g. "12#".
  */
  static int strglob(String glob, String txt){
    return strglob(
      (glob+"\0").getBytes(StandardCharsets.UTF_8),
      (txt+"\0").getBytes(StandardCharsets.UTF_8)
    );
  }

  /**
     Sets up C-side components needed by the test framework. This must
     not be called until main() is triggered so that it does not
     interfere with library clients who don't use this class.
  */
  static native void installCustomExtensions();
  static {
    System.loadLibrary("sqlite3-jni")
      /* Interestingly, when SQLTester is the main app, we have to
         load that lib from here. The same load from CApi does
         not happen early enough. Without this,
         installCustomExtensions() is an unresolved symbol. */;
  }

}

/**
   General utilities for the SQLTester bits.
*/
final class Util {

  //! Throws a new T, appending all msg args into a string for the message.
  static void toss(Class<? extends Exception> errorType, Object... msg) throws Exception {
    StringBuilder sb = new StringBuilder();
    for(Object s : msg) sb.append(s);
    final java.lang.reflect.Constructor<? extends Exception> ctor =
      errorType.getConstructor(String.class);
    throw ctor.newInstance(sb.toString());
  }

  static void toss(Object... msg) throws Exception{
    toss(RuntimeException.class, msg);
  }

  //! Tries to delete the given file, silently ignoring failure.
  static void unlink(String filename){
    try{
      final java.io.File f = new java.io.File(filename);
      f.delete();
    }catch(Exception e){
      /* ignore */
    }
  }

  /**
     Appends all entries in argv[1..end] into a space-separated
     string, argv[0] is not included because it's expected to be a
     command name.
  */
  static String argvToString(String[] argv){
    StringBuilder sb = new StringBuilder();
    for(int i = 1; i < argv.length; ++i ){
      if( i>1 ) sb.append(" ");
      sb.append( argv[i] );
    }
    return sb.toString();
  }

}

/**
   Base class for test script commands. It provides a set of utility
   APIs for concrete command implementations.

   Each subclass must have a public no-arg ctor and must implement
   the process() method which is abstract in this class.

   Commands are intended to be stateless, except perhaps for counters
   and similar internals. Specifically, no state which changes the
   behavior between any two invocations of process() should be
   retained.
*/
abstract class Command {
  protected Command(){}

  /**
     Must process one command-unit of work and either return
     (on success) or throw (on error).

     The first two arguments specify the context of the test. The TestScript
     provides the content of the test and the SQLTester providers the sandbox
     in which that script is being evaluated.

     argv is a list with the command name followed by any arguments to
     that command. The argcCheck() method from this class provides
     very basic argc validation.
  */
  public abstract void process(
    SQLTester st, TestScript ts, String[] argv
  ) throws Exception;

  /**
     If argv.length-1 (-1 because the command's name is in argv[0]) does not
     fall in the inclusive range (min,max) then this function throws. Use
     a max value of -1 to mean unlimited.
  */
  protected final void argcCheck(TestScript ts, String[] argv, int min, int max) throws Exception{
    int argc = argv.length-1;
    if(argc<min || (max>=0 && argc>max)){
      if( min==max ){
        ts.toss(argv[0]," requires exactly ",min," argument(s)");
      }else if(max>0){
        ts.toss(argv[0]," requires ",min,"-",max," arguments.");
      }else{
        ts.toss(argv[0]," requires at least ",min," arguments.");
      }
    }
  }

  /**
     Equivalent to argcCheck(argv,argc,argc).
  */
  protected final void argcCheck(TestScript ts, String[] argv, int argc) throws Exception{
    argcCheck(ts, argv, argc, argc);
  }
}

//! --close command
class CloseDbCommand extends Command {
  public void process(SQLTester t, TestScript ts, String[] argv) throws Exception{
    argcCheck(ts,argv,0,1);
    Integer id;
    if(argv.length>1){
      String arg = argv[1];
      if("all".equals(arg)){
        t.closeAllDbs();
        return;
      }
      else{
        id = Integer.parseInt(arg);
      }
    }else{
      id = t.getCurrentDbId();
    }
    t.closeDb(id);
  }
}

//! --column-names command
class ColumnNamesCommand extends Command {
  public void process(
    SQLTester st, TestScript ts, String[] argv
  ) throws Exception{
    argcCheck(ts,argv,1);
    st.outputColumnNames( Integer.parseInt(argv[1])!=0 );
  }
}

//! --db command
class DbCommand extends Command {
  public void process(SQLTester t, TestScript ts, String[] argv) throws Exception{
    argcCheck(ts,argv,1);
    t.setCurrentDb( Integer.parseInt(argv[1]) );
  }
}

//! --glob command
class GlobCommand extends Command {
  private boolean negate = false;
  public GlobCommand(){}
  protected GlobCommand(boolean negate){ this.negate = negate; }

  public void process(SQLTester t, TestScript ts, String[] argv) throws Exception{
    argcCheck(ts,argv,1,-1);
    t.incrementTestCounter();
    final String sql = t.takeInputBuffer();
    int rc = t.execSql(null, true, ResultBufferMode.ESCAPED,
                       ResultRowMode.ONELINE, sql);
    final String result = t.getResultText();
    final String sArgs = Util.argvToString(argv);
    //t2.verbose2(argv[0]," rc = ",rc," result buffer:\n", result,"\nargs:\n",sArgs);
    final String glob = Util.argvToString(argv);
    rc = SQLTester.strglob(glob, result);
    if( (negate && 0==rc) || (!negate && 0!=rc) ){
      ts.toss(argv[0], " mismatch: ", glob," vs input: ",result);
    }
  }
}

//! --json command
class JsonCommand extends ResultCommand {
  public JsonCommand(){ super(ResultBufferMode.ASIS); }
}

//! --json-block command
class JsonBlockCommand extends TableResultCommand {
  public JsonBlockCommand(){ super(true); }
}

//! --new command
class NewDbCommand extends OpenDbCommand {
  public NewDbCommand(){ super(true); }
}

//! Placeholder dummy/no-op commands
class NoopCommand extends Command {
  public void process(SQLTester t, TestScript ts, String[] argv) throws Exception{
  }
}

//! --notglob command
class NotGlobCommand extends GlobCommand {
  public NotGlobCommand(){
    super(true);
  }
}

//! --null command
class NullCommand extends Command {
  public void process(
    SQLTester st, TestScript ts, String[] argv
  ) throws Exception{
    argcCheck(ts,argv,1);
    st.setNullValue( argv[1] );
  }
}

//! --open command
class OpenDbCommand extends Command {
  private boolean createIfNeeded = false;
  public OpenDbCommand(){}
  protected OpenDbCommand(boolean c){createIfNeeded = c;}
  public void process(SQLTester t, TestScript ts, String[] argv) throws Exception{
    argcCheck(ts,argv,1);
    t.openDb(argv[1], createIfNeeded);
  }
}

//! --print command
class PrintCommand extends Command {
  public void process(
    SQLTester st, TestScript ts, String[] argv
  ) throws Exception{
    st.out(ts.getOutputPrefix(),": ");
    if( 1==argv.length ){
      st.out( st.getInputText() );
    }else{
      st.outln( Util.argvToString(argv) );
    }
  }
}

//! --result command
class ResultCommand extends Command {
  private final ResultBufferMode bufferMode;
  protected ResultCommand(ResultBufferMode bm){ bufferMode = bm; }
  public ResultCommand(){ this(ResultBufferMode.ESCAPED); }
  public void process(SQLTester t, TestScript ts, String[] argv) throws Exception{
    argcCheck(ts,argv,0,-1);
    t.incrementTestCounter();
    final String sql = t.takeInputBuffer();
    //ts.verbose2(argv[0]," SQL =\n",sql);
    int rc = t.execSql(null, false, bufferMode, ResultRowMode.ONELINE, sql);
    final String result = t.getResultText().trim();
    final String sArgs = argv.length>1 ? Util.argvToString(argv) : "";
    if( !result.equals(sArgs) ){
      t.outln(argv[0]," FAILED comparison. Result buffer:\n",
              result,"\nExpected result:\n",sArgs);
      ts.toss(argv[0]+" comparison failed.");
    }
  }
}

//! --run command
class RunCommand extends Command {
  public void process(SQLTester t, TestScript ts, String[] argv) throws Exception{
    argcCheck(ts,argv,0,1);
    final sqlite3 db = (1==argv.length)
      ? t.getCurrentDb() : t.getDbById( Integer.parseInt(argv[1]) );
    final String sql = t.takeInputBuffer();
    final int rc = t.execSql(db, false, ResultBufferMode.NONE,
                             ResultRowMode.ONELINE, sql);
    if( 0!=rc && t.isVerbose() ){
      String msg = sqlite3_errmsg(db);
      ts.verbose1(argv[0]," non-fatal command error #",rc,": ",
                  msg,"\nfor SQL:\n",sql);
    }
  }
}

//! --tableresult command
class TableResultCommand extends Command {
  private final boolean jsonMode;
  protected TableResultCommand(boolean jsonMode){ this.jsonMode = jsonMode; }
  public TableResultCommand(){ this(false); }
  public void process(SQLTester t, TestScript ts, String[] argv) throws Exception{
    argcCheck(ts,argv,0);
    t.incrementTestCounter();
    String body = ts.fetchCommandBody(t);
    if( null==body ) ts.toss("Missing ",argv[0]," body.");
    body = body.trim();
    if( !body.endsWith("\n--end") ){
      ts.toss(argv[0], " must be terminated with --end.");
    }else{
      body = body.substring(0, body.length()-6);
    }
    final String[] globs = body.split("\\s*\\n\\s*");
    if( globs.length < 1 ){
      ts.toss(argv[0], " requires 1 or more ",
              (jsonMode ? "json snippets" : "globs"),".");
    }
    final String sql = t.takeInputBuffer();
    t.execSql(null, true,
              jsonMode ? ResultBufferMode.ASIS : ResultBufferMode.ESCAPED,
              ResultRowMode.NEWLINE, sql);
    final String rbuf = t.getResultText();
    final String[] res = rbuf.split("\n");
    if( res.length != globs.length ){
      ts.toss(argv[0], " failure: input has ", res.length,
              " row(s) but expecting ",globs.length);
    }
    for(int i = 0; i < res.length; ++i){
      final String glob = globs[i].replaceAll("\\s+"," ").trim();
      //ts.verbose2(argv[0]," <<",glob,">> vs <<",res[i],">>");
      if( jsonMode ){
        if( !glob.equals(res[i]) ){
          ts.toss(argv[0], " json <<",glob, ">> does not match: <<",
                  res[i],">>");
        }
      }else if( 0 != SQLTester.strglob(glob, res[i]) ){
        ts.toss(argv[0], " glob <<",glob,">> does not match: <<",res[i],">>");
      }
    }
  }
}

//! --testcase command
class TestCaseCommand extends Command {
  public void process(SQLTester t, TestScript ts, String[] argv) throws Exception{
    argcCheck(ts,argv,1);
    ts.setTestCaseName(argv[1]);
    t.clearResultBuffer();
    t.clearInputBuffer();
  }
}

//! --verbosity command
class VerbosityCommand extends Command {
  public void process(SQLTester t, TestScript ts, String[] argv) throws Exception{
    argcCheck(ts,argv,1);
    ts.setVerbosity( Integer.parseInt(argv[1]) );
  }
}

class CommandDispatcher {

  private static java.util.Map<String,Command> commandMap =
    new java.util.HashMap<>();

  /**
     Returns a (cached) instance mapped to name, or null if no match
     is found.
  */
  static Command getCommandByName(String name){
    Command rv = commandMap.get(name);
    if( null!=rv ) return rv;
    switch(name){
      case "close":        rv = new CloseDbCommand(); break;
      case "column-names": rv = new ColumnNamesCommand(); break;
      case "db":           rv = new DbCommand(); break;
      case "glob":         rv = new GlobCommand(); break;
      case "json":         rv = new JsonCommand(); break;
      case "json-block":   rv = new JsonBlockCommand(); break;
      case "new":          rv = new NewDbCommand(); break;
      case "notglob":      rv = new NotGlobCommand(); break;
      case "null":         rv = new NullCommand(); break;
      case "oom":          rv = new NoopCommand(); break;
      case "open":         rv = new OpenDbCommand(); break;
      case "print":        rv = new PrintCommand(); break;
      case "result":       rv = new ResultCommand(); break;
      case "run":          rv = new RunCommand(); break;
      case "tableresult":  rv = new TableResultCommand(); break;
      case "testcase":     rv = new TestCaseCommand(); break;
      case "verbosity":    rv = new VerbosityCommand(); break;
      default: rv = null; break;
    }
    if( null!=rv ) commandMap.put(name, rv);
    return rv;
  }

  /**
     Treats argv[0] as a command name, looks it up with
     getCommandByName(), and calls process() on that instance, passing
     it arguments given to this function.
  */
  static void dispatch(SQLTester tester, TestScript ts, String[] argv) throws Exception{
    final Command cmd = getCommandByName(argv[0]);
    if(null == cmd){
      throw new UnknownCommand(ts, argv[0]);
    }
    cmd.process(tester, ts, argv);
  }
}


/**
   This class represents a single test script. It handles (or
   delegates) its the reading-in and parsing, but the details of
   evaluation are delegated elsewhere.
*/
class TestScript {
  //! input file
  private String filename = null;
  //! Name pulled from the SCRIPT_MODULE_NAME directive of the file
  private String moduleName = null;
  //! Current test case name.
  private String testCaseName = null;
  //! Content buffer state.
  private final Cursor cur = new Cursor();
  //! Utility for console output.
  private final Outer outer = new Outer();

  //! File content and parse state.
  private static final class Cursor {
    private final StringBuilder sb = new StringBuilder();
    byte[] src = null;
    //! Current position in this.src.
    int pos = 0;
    //! Current line number. Starts at 0 for internal reasons and will
    // line up with 1-based reality once parsing starts.
    int lineNo = 0 /* yes, zero */;
    //! Putback value for this.pos.
    int putbackPos = 0;
    //! Putback line number
    int putbackLineNo = 0;
    //! Peeked-to pos, used by peekLine() and consumePeeked().
    int peekedPos = 0;
    //! Peeked-to line number.
    int peekedLineNo = 0;

    //! Restore parsing state to the start of the stream.
    void rewind(){
      sb.setLength(0);
      pos = lineNo = putbackPos = putbackLineNo = peekedPos = peekedLineNo = 0
        /* kinda missing memset() about now. */;
    }
  }

  private byte[] readFile(String filename) throws Exception {
    return java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(filename));
  }

  /**
     Initializes the script with the content of the given file.
     Throws if it cannot read the file.
  */
  public TestScript(String filename) throws Exception{
    this.filename = filename;
    setVerbosity(2);
    cur.src = readFile(filename);
  }

  public String getFilename(){
    return filename;
  }

  public String getModuleName(){
    return moduleName;
  }

  /**
     Verbosity level 0 produces no debug/verbose output. Level 1 produces
     some and level 2 produces more.
   */
  public void setVerbosity(int level){
    outer.setVerbosity(level);
  }

  public String getOutputPrefix(){
    String rc = "["+(moduleName==null ? "<unnamed>" : moduleName)+"]";
    if( null!=testCaseName ) rc += "["+testCaseName+"]";
    if( null!=filename ) rc += "["+filename+"]";
    return rc + " line "+ cur.lineNo;
  }

  static final String[] verboseLabel = {"🔈",/*"🔉",*/"🔊","📢"};
  //! Output vals only if level<=current verbosity level.
  private TestScript verboseN(int level, Object... vals){
    final int verbosity = outer.getVerbosity();
    if(verbosity>=level){
      outer.out( verboseLabel[level-1], getOutputPrefix(), " ",level,": "
      ).outln(vals);
    }
    return this;
  }

  TestScript verbose1(Object... vals){return verboseN(1,vals);}
  TestScript verbose2(Object... vals){return verboseN(2,vals);}
  TestScript verbose3(Object... vals){return verboseN(3,vals);}

  private void reset(){
    testCaseName = null;
    cur.rewind();
  }

  void setTestCaseName(String n){ testCaseName = n; }

  /**
     Returns the next line from the buffer, minus the trailing EOL.

     Returns null when all input is consumed. Throws if it reads
     illegally-encoded input, e.g. (non-)characters in the range
     128-256.
  */
  String getLine(){
    if( cur.pos==cur.src.length ){
      return null /* EOF */;
    }
    cur.putbackPos = cur.pos;
    cur.putbackLineNo = cur.lineNo;
    cur.sb.setLength(0);
    final boolean skipLeadingWs = false;
    byte b = 0, prevB = 0;
    int i = cur.pos;
    if(skipLeadingWs) {
      /* Skip any leading spaces, including newlines. This will eliminate
         blank lines. */
      for(; i < cur.src.length; ++i, prevB=b){
        b = cur.src[i];
        switch((int)b){
          case 32/*space*/: case 9/*tab*/: case 13/*CR*/: continue;
          case 10/*NL*/: ++cur.lineNo; continue;
          default: break;
        }
        break;
      }
      if( i==cur.src.length ){
        return null /* EOF */;
      }
    }
    boolean doBreak = false;
    final byte[] aChar = {0,0,0,0} /* multi-byte char buffer */;
    int nChar = 0 /* number of bytes in the char */;
    for(; i < cur.src.length && !doBreak; ++i){
      b = cur.src[i];
      switch( (int)b ){
        case 13/*CR*/: continue;
        case 10/*NL*/:
          ++cur.lineNo;
          if(cur.sb.length()>0) doBreak = true;
          // Else it's an empty string
          break;
        default:
          /* Multi-byte chars need to be gathered up and appended at
             one time. Appending individual bytes to the StringBuffer
             appends their integer value. */
          nChar = 1;
          switch( b & 0xF0 ){
            case 0xC0: nChar = 2; break;
            case 0xE0: nChar = 3; break;
            case 0xF0: nChar = 4; break;
            default:
              if( b > 127 ) this.toss("Invalid character (#"+(int)b+").");
              break;
          }
          if( 1==nChar ){
            cur.sb.append((char)b);
          }else{
            for(int x = 0; x < nChar; ++x) aChar[x] = cur.src[i+x];
            cur.sb.append(new String(Arrays.copyOf(aChar, nChar),
                                      StandardCharsets.UTF_8));
            i += nChar-1;
          }
          break;
      }
    }
    cur.pos = i;
    final String rv = cur.sb.toString();
    if( i==cur.src.length && 0==rv.length() ){
      return null /* EOF */;
    }
    return rv;
  }/*getLine()*/

  /**
     Fetches the next line then resets the cursor to its pre-call
     state. consumePeeked() can be used to consume this peeked line
     without having to re-parse it.
  */
  String peekLine(){
    final int oldPos = cur.pos;
    final int oldPB = cur.putbackPos;
    final int oldPBL = cur.putbackLineNo;
    final int oldLine = cur.lineNo;
    try{ return getLine(); }
    finally{
      cur.peekedPos = cur.pos;
      cur.peekedLineNo = cur.lineNo;
      cur.pos = oldPos;
      cur.lineNo = oldLine;
      cur.putbackPos = oldPB;
      cur.putbackLineNo = oldPBL;
    }
  }

  /**
     Only valid after calling peekLine() and before calling getLine().
     This places the cursor to the position it would have been at had
     the peekLine() had been fetched with getLine().
  */
  void consumePeeked(){
    cur.pos = cur.peekedPos;
    cur.lineNo = cur.peekedLineNo;
  }

  /**
     Restores the cursor to the position it had before the previous
     call to getLine().
  */
  void putbackLine(){
    cur.pos = cur.putbackPos;
    cur.lineNo = cur.putbackLineNo;
  }

  private boolean checkRequiredProperties(SQLTester t, String[] props) throws SQLTesterException{
    if( true ) return false;
    int nOk = 0;
    for(String rp : props){
      verbose1("REQUIRED_PROPERTIES: ",rp);
      switch(rp){
        case "RECURSIVE_TRIGGERS":
          t.appendDbInitSql("pragma recursive_triggers=on;");
          ++nOk;
          break;
        case "TEMPSTORE_FILE":
          /* This _assumes_ that the lib is built with SQLITE_TEMP_STORE=1 or 2,
             which we just happen to know is the case */
          t.appendDbInitSql("pragma temp_store=1;");
          ++nOk;
          break;
        case "TEMPSTORE_MEM":
          /* This _assumes_ that the lib is built with SQLITE_TEMP_STORE=1 or 2,
             which we just happen to know is the case */
          t.appendDbInitSql("pragma temp_store=0;");
          ++nOk;
          break;
        case "AUTOVACUUM":
          t.appendDbInitSql("pragma auto_vacuum=full;");
          ++nOk;
        case "INCRVACUUM":
          t.appendDbInitSql("pragma auto_vacuum=incremental;");
          ++nOk;
        default:
          break;
      }
    }
    return props.length == nOk;
  }

  private static final Pattern patternRequiredProperties =
    Pattern.compile(" REQUIRED_PROPERTIES:[ \\t]*(\\S.*)\\s*$");
  private static final Pattern patternScriptModuleName =
    Pattern.compile(" SCRIPT_MODULE_NAME:[ \\t]*(\\S+)\\s*$");
  private static final Pattern patternMixedModuleName =
    Pattern.compile(" ((MIXED_)?MODULE_NAME):[ \\t]*(\\S+)\\s*$");
  private static final Pattern patternCommand =
    Pattern.compile("^--(([a-z-]+)( .*)?)$");

  /**
     Looks for "directives." If a compatible one is found, it is
     processed and this function returns. If an incompatible one is found,
     a description of it is returned and processing of the test must
     end immediately.
  */
  private void checkForDirective(
    SQLTester tester, String line
  ) throws IncompatibleDirective {
    if(line.startsWith("#")){
      throw new IncompatibleDirective(this, "C-preprocessor input: "+line);
    }else if(line.startsWith("---")){
      new IncompatibleDirective(this, "triple-dash: "+line);
    }
    Matcher m = patternScriptModuleName.matcher(line);
    if( m.find() ){
      moduleName = m.group(1);
      return;
    }
    m = patternRequiredProperties.matcher(line);
    if( m.find() ){
      final String rp = m.group(1);
      if( ! checkRequiredProperties( tester, rp.split("\\s+") ) ){
        throw new IncompatibleDirective(this, "REQUIRED_PROPERTIES: "+rp);
      }
    }
    m = patternMixedModuleName.matcher(line);
    if( m.find() ){
      throw new IncompatibleDirective(this, m.group(1)+": "+m.group(3));
    }
    if( line.indexOf("\n|")>=0 ){
      throw new IncompatibleDirective(this, "newline-pipe combination.");
    }
    return;
  }

  boolean isCommandLine(String line, boolean checkForImpl){
    final Matcher m = patternCommand.matcher(line);
    boolean rc = m.find();
    if( rc && checkForImpl ){
      rc = null!=CommandDispatcher.getCommandByName(m.group(2));
    }
    return rc;
  }

  /**
     If line looks like a command, returns an argv for that command
     invocation, else returns null.
  */
  String[] getCommandArgv(String line){
    final Matcher m = patternCommand.matcher(line);
    return m.find() ? m.group(1).trim().split("\\s+") : null;
  }

  /**
     Fetches lines until the next recognized command. Throws if
     checkForDirective() does.  Returns null if there is no input or
     it's only whitespace. The returned string retains all whitespace.

     Note that "subcommands", --command-like constructs in the body
     which do not match a known command name are considered to be
     content, not commands.
  */
  String fetchCommandBody(SQLTester tester){
    final StringBuilder sb = new StringBuilder();
    String line;
    while( (null != (line = peekLine())) ){
      checkForDirective(tester, line);
      if( isCommandLine(line, true) ) break;
      else {
        sb.append(line).append("\n");
        consumePeeked();
      }
    }
    line = sb.toString();
    return line.trim().isEmpty() ? null : line;
  }

  private void processCommand(SQLTester t, String[] argv) throws Exception{
    verbose1("running command: ",argv[0], " ", Util.argvToString(argv));
    if(outer.getVerbosity()>1){
      final String input = t.getInputText();
      if( !input.isEmpty() ) verbose3("Input buffer = ",input);
    }
    CommandDispatcher.dispatch(t, this, argv);
  }

  void toss(Object... msg) throws TestScriptFailed {
    StringBuilder sb = new StringBuilder();
    for(Object s : msg) sb.append(s);
    throw new TestScriptFailed(this, sb.toString());
  }

  /**
     Runs this test script in the context of the given tester object.
  */
  public boolean run(SQLTester tester) throws Exception {
    reset();
    setVerbosity(tester.getVerbosity());
    String line, directive;
    String[] argv;
    while( null != (line = getLine()) ){
      verbose3("input line: ",line);
      checkForDirective(tester, line);
      argv = getCommandArgv(line);
      if( null!=argv ){
        processCommand(tester, argv);
        continue;
      }
      tester.appendInput(line,true);
    }
    return true;
  }
}
