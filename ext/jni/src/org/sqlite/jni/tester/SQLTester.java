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
package org.sqlite.jni.tester;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.nio.charset.StandardCharsets;
import java.util.regex.*;
import org.sqlite.jni.*;
import static org.sqlite.jni.SQLite3Jni.*;

class TestFailure extends RuntimeException {
  public TestFailure(String msg){
    super(msg);
  }
}

class SkipTestRemainder extends RuntimeException {
  public TestScript testScript;
  public SkipTestRemainder(TestScript ts){
    super("Skipping remainder of "+ts.getName());
    testScript = ts;
  }
}

/**
   Modes for how to handle SQLTester.execSql()'s
   result output.
 */
enum ResultBufferMode {
  //! Do not append to result buffer
  NONE,
  //! Append output escaped.
  ESCAPED,
  //! Append output as-is
  ASIS
};

enum ResultRowMode {
  //! Keep all result rows on one line, space-separated.
  ONELINE,
  //! Add a newline between each result row.
  NEWLINE
};

/**
   This class provides an application which aims to implement the
   rudimentary SQL-driven test tool described in the accompanying
   test-script-interpreter.md.

   This is a work in progress.
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
  private String nullView;
  private int nTotalTest = 0;
  private int nTestFile = 0;
  private int nAbortedScript = 0;
  private int nTest;
  private final sqlite3[] aDb = new sqlite3[7];
  private int iCurrentDb = 0;
  private final String initialDbName = "test.db";
  private TestScript currentScript;
  private int verbosity = 0;

  public SQLTester(){
    reset();
  }

  public void setVerbose(int level){
    verbosity = level;
    this.outer.setVerbose( level!=0 );
  }
  public int getVerbosity(){
    return verbosity;
  }
  public boolean isVerbose(){
    return verbosity>0;
  }

  @SuppressWarnings("unchecked")
  public void verbose(Object... vals){
    if( verbosity > 0 ) outer.verbose(vals);
  }

  @SuppressWarnings("unchecked")
  public void outln(Object... vals){
    outer.outln(vals);
  }

  @SuppressWarnings("unchecked")
  public void out(Object... vals){
    outer.out(vals);
  }

  //! Adds the given test script to the to-test list.
  public void addTestScript(String filename){
    listInFiles.add(filename);
    //verbose("Added file ",filename);
  }

  public void setupInitialDb() throws Exception {
    Util.unlink(initialDbName);
    openDb(0, initialDbName, true);
  }

  TestScript getCurrentScript(){
    return currentScript;
  }

  public void runTests() throws Exception {
    // process each input file
    try {
      for(String f : listInFiles){
        reset();
        setupInitialDb();
        ++nTestFile;
        final TestScript ts = new TestScript(f);
        currentScript = ts;
        outln("----->>>>> ",ts.getModuleName()," [",ts.getName(),"]");
        if( ts.isIgnored() ){
          outln("WARNING: skipping [",ts.getModuleName(),"]: ",
                ts.getIgnoredReason());
          continue;
        }else{
          try{
            ts.run(this);
          }catch(SkipTestRemainder e){
            /* not an error */
            ++nAbortedScript;
          }
        }
        outln("<<<<<----- ",ts.getModuleName(),": ",nTest," test(s)");
      }
    }finally{
      currentScript = null;
    }
    Util.unlink(initialDbName);
  }

  private StringBuilder clearBuffer(StringBuilder b){
    b.delete(0, b.length());
    return b;
  }

  StringBuilder clearInputBuffer(){
    return clearBuffer(inputBuffer);
  }

  StringBuilder clearResultBuffer(){
    return clearBuffer(resultBuffer);
  }

  StringBuilder getInputBuffer(){ return inputBuffer; }

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
      throw new IndexOutOfBoundsException("illegal db number.");
    }
    return this;
  }

  sqlite3 setCurrentDb(int n) throws Exception{
    return affirmDbId(n).aDb[n];
  }

  sqlite3 getCurrentDb(){ return aDb[iCurrentDb]; }

  sqlite3 getDbById(int id) throws Exception{
    return affirmDbId(id).aDb[iCurrentDb];
  }

  void closeDb(int id) throws Exception{
    final sqlite3 db = affirmDbId(id).aDb[id];
    if( null != db ){
      sqlite3_close_v2(db);
      aDb[id] = null;
    }
  }

  void closeDb() throws Exception { closeDb(iCurrentDb); }

  void closeAllDbs(){
    for(int i = 0; i<aDb.length; ++i){
      sqlite3_close_v2(aDb[i]);
      aDb[i] = null;
    }
  }

  sqlite3 openDb(String name, boolean createIfNeeded) throws Exception {
    closeDb();
    int flags = SQLITE_OPEN_READWRITE;
    if( createIfNeeded ) flags |= SQLITE_OPEN_CREATE;
    final OutputPointer.sqlite3 out = new OutputPointer.sqlite3();
    int rc = sqlite3_open_v2(name, out, flags, null);
    final sqlite3 db = out.getValue();
    if( 0!=rc ){
      final String msg = sqlite3_errmsg(db);
      sqlite3_close(db);
      Util.toss(TestFailure.class, "db open failed with code ",
                rc," and message: ",msg);
    }
    return aDb[iCurrentDb] = db;
  }

  sqlite3 openDb(int slot, String name, boolean createIfNeeded) throws Exception {
    affirmDbId(slot);
    iCurrentDb = slot;
    return openDb(name, createIfNeeded);
  }

  /**
     Resets all tester context state except for that related to
     tracking running totals.
  */
  void reset(){
    nTest = 0;
    nullView = "nil";
    clearInputBuffer();
    closeAllDbs();
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

  static final Pattern patternSpecial = Pattern.compile(
    "[\\x00-\\x20\\x22\\x5c\\x7b\\x7d]", Pattern.MULTILINE
  );
  static final Pattern patternSquiggly = Pattern.compile("[{}]", Pattern.MULTILINE);

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
    sb.append(org.sqlite.jni.ResultCode.getEntryForInt(rc))
      .append(" {")
      .append(escapeSqlValue(sqlite3_errmsg(db)))
      .append("}");
  }

  public int execSql(sqlite3 db, boolean throwOnError,
                     ResultBufferMode appendMode,
                     ResultRowMode lineMode,
                     String sql) throws Exception {
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
    while(pos < sqlChunk.length){
      if(pos > 0){
        sqlChunk = Arrays.copyOfRange(sqlChunk, pos,
                                      sqlChunk.length);
      }
      if( 0==sqlChunk.length ) break;
      rc = sqlite3_prepare_v2(db, sqlChunk, outStmt, oTail);
      /*outln("PREPARE rc ",rc," oTail=",oTail.getValue(),": ",
        new String(sqlChunk,StandardCharsets.UTF_8),"\n<EOSQL>");*/
      if( 0!=rc ){
        if(throwOnError){
          Util.toss(RuntimeException.class, "db op failed with rc="
                    +rc+": "+sqlite3_errmsg(db));
        }else if( null!=sb ){
          appendDbErr(db, sb, rc);
        }
        break;
      }
      pos = oTail.getValue();
      stmt = outStmt.getValue();
      if( null == stmt ){
        // empty statement was parsed.
        continue;
      }
      if( null!=sb ){
        // Add the output to the result buffer...
        final int nCol = sqlite3_column_count(stmt);
        while( SQLITE_ROW == (rc = sqlite3_step(stmt)) ){
          for(int i = 0; i < nCol; ++i){
            if( spacing++ > 0 ) sb.append(' ');
            String val = sqlite3_column_text16(stmt, i);
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
                Util.toss(RuntimeException.class, "Unhandled ResultBufferMode.");
            }
          }
          if( ResultRowMode.NEWLINE == lineMode ){
            spacing = 0;
            sb.append('\n');
          }
        }
      }else{
        while( SQLITE_ROW == (rc = sqlite3_step(stmt)) ){}
      }
      sqlite3_finalize(stmt);
      if(SQLITE_ROW==rc || SQLITE_DONE==rc) rc = 0;
      else if( rc!=0 ){
        if( null!=sb ){
          appendDbErr(db, sb, rc);
        }
        break;
      }
    }
    sqlite3_finalize(stmt);
    if( 0!=rc && throwOnError ){
      Util.toss(RuntimeException.class, "db op failed with rc="
                +rc+": "+sqlite3_errmsg(db));
    }
    return rc;
  }

  public static void main(String[] argv) throws Exception{
    final SQLTester t = new SQLTester();
    for(String a : argv){
      if(a.startsWith("-")){
        final String flag = a.replaceFirst("-+","");
        if( flag.equals("verbose") ){
          ++t.verbosity;
        }else{
          throw new IllegalArgumentException("Unhandled flag: "+flag);
        }
        continue;
      }
      t.addTestScript(a);
    }
    t.runTests();
    t.outln("Processed ",t.nTotalTest," test(s) in ",t.nTestFile," file(s).");
    if( t.nAbortedScript > 0 ){
      t.outln("Aborted ",t.nAbortedScript," script(s).");
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
  public static int strglob(String glob, String txt){
    return strglob(
      (glob+"\0").getBytes(StandardCharsets.UTF_8),
      (txt+"\0").getBytes(StandardCharsets.UTF_8)
    );
  }

  private static native void installCustomExtensions();
  static {
    System.loadLibrary("sqlite3-jni")
      /* Interestingly, when SQLTester is the main app, we have to
         load that lib from here. The same load from SQLite3Jni does
         not happen early enough. Without this,
         installCustomExtensions() is an unresolved symbol. */;
    installCustomExtensions();
  }

}

/**
   Base class for test script commands. It provides a set of utility
   APIs for concrete command implementations.

   Each subclass must have a public no-arg ctor and must implement
   the process() method which is abstract in this class.

   Commands are intended to be stateless, except perhaps for counters
   and similar internals. No state which changes the behavior between
   any two invocations of process() should be retained.
*/
abstract class Command {
  protected Command(){}

  /**
     Must process one command-unit of work and either return
     (on success) or throw (on error).

     The first argument is the context of the test.

     argv is a list with the command name followed by any arguments to
     that command. The argcCheck() method from this class provides
     very basic argc validation.

     The content is any text content which was specified after the
     command, or null if there is null. Any command which does not
     permit content must pass that argument to affirmNoContent() in
     their constructor (or perform an equivalent check). Similary,
     those which require content must pass it to affirmHasContent()
     (or equivalent).
  */
  public abstract void process(SQLTester tester, String[] argv, String content) throws Exception;

  /**
     If argv.length-1 (-1 because the command's name is in argv[0]) does not
     fall in the inclusive range (min,max) then this function throws. Use
     a max value of -1 to mean unlimited.
  */
  protected final void argcCheck(String[] argv, int min, int max) throws Exception{
    int argc = argv.length-1;
    if(argc<min || (max>=0 && argc>max)){
      if( min==max ){
        Util.badArg(argv[0]," requires exactly ",min," argument(s)");
      }else if(max>0){
        Util.badArg(argv[0]," requires ",min,"-",max," arguments.");
      }else{
        Util.badArg(argv[0]," requires at least ",min," arguments.");
      }
    }
  }

  /**
     Equivalent to argcCheck(argv,argc,argc).
  */
  protected final void argcCheck(String[] argv, int argc) throws Exception{
    argcCheck(argv, argc, argc);
  }

  //! Throws if content is not null.
  protected void affirmNoContent(String content) throws Exception{
    if(null != content){
      Util.badArg(this.getClass().getName()," does not accept content ",
                  "but got:\n",content);
    }
  }

  //! Throws if content is null.
  protected void affirmHasContent(String content) throws Exception{
    if(null == content){
      Util.badArg(this.getClass().getName()," requires content.");
    }
  }
}

class CloseDbCommand extends Command {
  public void process(SQLTester t, String[] argv, String content) throws Exception{
    argcCheck(argv,0,1);
    affirmNoContent(content);
    Integer id;
    if(argv.length>1){
      String arg = argv[1];
      if("all".equals(arg)){
        //t.verbose(argv[0]," all dbs");
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
    //t.verbose(argv[0]," db ",id);
  }
}

//! --db command
class DbCommand extends Command {
  public void process(SQLTester t, String[] argv, String content) throws Exception{
    argcCheck(argv,1);
    affirmNoContent(content);
    final sqlite3 db = t.setCurrentDb( Integer.parseInt(argv[1]) );
    //t.verbose(argv[0]," set db to ",db);
  }
}

//! --glob command
class GlobCommand extends Command {
  private boolean negate = false;
  public GlobCommand(){}
  protected GlobCommand(boolean negate){ this.negate = negate; }

  public void process(SQLTester t, String[] argv, String content) throws Exception{
    argcCheck(argv,1);
    affirmNoContent(content);
    t.incrementTestCounter();
    final String sql = t.takeInputBuffer();
    //t.verbose(argv[0]," SQL =\n",sql);
    int rc = t.execSql(null, true, ResultBufferMode.ESCAPED,
                       ResultRowMode.ONELINE, sql);
    final String result = t.getResultText().trim();
    final String sArgs = Util.argvToString(argv);
    //t.verbose(argv[0]," rc = ",rc," result buffer:\n", result,"\nargs:\n",sArgs);
    final String glob = argv[1];
    rc = SQLTester.strglob(glob, result);
    if( (negate && 0==rc) || (!negate && 0!=rc) ){
      Util.toss(TestFailure.class, argv[0], " mismatch: ",
                glob," vs input: ",result);
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
class NewDbCommand extends Command {
  public void process(SQLTester t, String[] argv, String content) throws Exception{
    argcCheck(argv,1);
    affirmNoContent(content);
    String fname = argv[1];
    Util.unlink(fname);
    final sqlite3 db = t.openDb(fname, true);
    //t.verbose(argv[0]," db ",db);
  }
}

//! Placeholder dummy/no-op commands
class NoopCommand extends Command {
  public void process(SQLTester t, String[] argv, String content) throws Exception{
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
  public void process(SQLTester t, String[] argv, String content) throws Exception{
    argcCheck(argv,1);
    affirmNoContent(content);
    t.setNullValue(argv[1]);
    //t.verbose(argv[0]," ",argv[1]);
  }
}

//! --open command
class OpenDbCommand extends Command {
  public void process(SQLTester t, String[] argv, String content) throws Exception{
    argcCheck(argv,1);
    affirmNoContent(content);
    String fname = argv[1];
    final sqlite3 db = t.openDb(fname, false);
    //t.verbose(argv[0]," db ",db);
  }
}

//! --print command
class PrintCommand extends Command {
  public void process(SQLTester t, String[] argv, String content) throws Exception{
    if( 1==argv.length && null==content ){
      Util.badArg(argv[0]," requires at least 1 argument or body content.");
    }
    if( argv.length > 1 ) t.outln("\t",Util.argvToString(argv));
    if( null!=content ) t.outln(content.replaceAll("(?m)^", "\t"));
  }
}

//! --result command
class ResultCommand extends Command {
  private final ResultBufferMode bufferMode;
  protected ResultCommand(ResultBufferMode bm){ bufferMode = bm; }
  public ResultCommand(){ this(ResultBufferMode.ESCAPED); }
  public void process(SQLTester t, String[] argv, String content) throws Exception{
    argcCheck(argv,0,-1);
    affirmNoContent(content);
    t.incrementTestCounter();
    final String sql = t.takeInputBuffer();
    //t.verbose(argv[0]," SQL =\n",sql);
    int rc = t.execSql(null, true, bufferMode, ResultRowMode.ONELINE, sql);
    final String result = t.getResultText().trim();
    final String sArgs = argv.length>1 ? Util.argvToString(argv) : "";
    if( !result.equals(sArgs) ){
      t.outln(argv[0]," FAILED comparison. Result buffer:\n",
              result,"\nargs:\n",sArgs);
      Util.toss(TestFailure.class, argv[0]," comparison failed.");
    }
  }
}

//! --run command
class RunCommand extends Command {
  public void process(SQLTester t, String[] argv, String content) throws Exception{
    argcCheck(argv,0,1);
    affirmHasContent(content);
    final sqlite3 db = (1==argv.length)
      ? t.getCurrentDb() : t.getDbById( Integer.parseInt(argv[1]) );
    int rc = t.execSql(db, false, ResultBufferMode.NONE,
                       ResultRowMode.ONELINE, content);
    if( 0!=rc && t.isVerbose() ){
      String msg = sqlite3_errmsg(db);
      t.verbose(argv[0]," non-fatal command error #",rc,": ",
                msg,"\nfor SQL:\n",content);
    }
  }
}

//! --tableresult command
class TableResultCommand extends Command {
  private final boolean jsonMode;
  protected TableResultCommand(boolean jsonMode){ this.jsonMode = jsonMode; }
  public TableResultCommand(){ this(false); }
  public void process(SQLTester t, String[] argv, String content) throws Exception{
    argcCheck(argv,0);
    affirmHasContent(content);
    t.incrementTestCounter();
    if( !content.endsWith("\n--end") ){
      Util.toss(TestFailure.class, argv[0], " must be terminated with --end.");
    }else{
      int n = content.length();
      content = content.substring(0, n-6);
    }
    final String[] globs = content.split("\\s*\\n\\s*");
    if( globs.length < 1 ){
      Util.toss(TestFailure.class, argv[0], " requires 1 or more ",
                (jsonMode ? "json snippets" : "globs"),".");
    }
    final String sql = t.takeInputBuffer();
    t.execSql(null, true,
              jsonMode ? ResultBufferMode.ASIS : ResultBufferMode.ESCAPED,
              ResultRowMode.NEWLINE, sql);
    final String rbuf = t.getResultText();
    final String[] res = rbuf.split("\n");
    if( res.length != globs.length ){
      Util.toss(TestFailure.class, argv[0], " failure: input has ",
                res.length," row(s) but expecting ",globs.length);
    }
    for(int i = 0; i < res.length; ++i){
      final String glob = globs[i].replaceAll("\\s+"," ");
      //t.verbose(argv[0]," <<",glob,">> vs <<",res[i],">>");
      if( jsonMode ){
        if( !glob.equals(res[i]) ){
          Util.toss(TestFailure.class, argv[0], " json <<",glob,
                  ">> does not match: <<",res[i],">>");
        }
      }else if( 0 != SQLTester.strglob(glob, res[i]) ){
        Util.toss(TestFailure.class, argv[0], " glob <<",glob,
                  ">> does not match: <<",res[i],">>");
      }
    }
  }
}

//! --testcase command
class TestCaseCommand extends Command {
  public void process(SQLTester t, String[] argv, String content) throws Exception{
    argcCheck(argv,1);
    affirmHasContent(content);
    // TODO: do something with the test name
    t.clearResultBuffer();
    t.clearInputBuffer().append(content);
    //t.verbose(argv[0]," input buffer: ",content);
  }
}

/**
   Helper for dispatching Command instances.
*/
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
      case "close":       rv = new CloseDbCommand(); break;
      case "db":          rv = new DbCommand(); break;
      case "glob":        rv = new GlobCommand(); break;
      case "json":        rv = new JsonCommand(); break;
      case "json-block":  rv = new JsonBlockCommand(); break;
      case "new":         rv = new NewDbCommand(); break;
      case "notglob":     rv = new NotGlobCommand(); break;
      case "null":        rv = new NullCommand(); break;
      case "oom":         rv = new NoopCommand(); break;
      case "open":        rv = new OpenDbCommand(); break;
      case "print":       rv = new PrintCommand(); break;
      case "result":      rv = new ResultCommand(); break;
      case "run":         rv = new RunCommand(); break;
      case "tableresult": rv = new TableResultCommand(); break;
      case "testcase":    rv = new TestCaseCommand(); break;
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
  static void dispatch(SQLTester tester, String[] argv, String content) throws Exception{
    final Command cmd = getCommandByName(argv[0]);
    if(null == cmd){
      final TestScript ts = tester.getCurrentScript();
      if( tester.skipUnknownCommands() ){
        tester.outln("WARNING: skipping remainder of [",ts.getModuleName(),
                     "] because it contains unknown command '",argv[0],"'.");
        throw new SkipTestRemainder(ts);
      }
      Util.toss(IllegalArgumentException.class,
                "No command handler found for '"+argv[0]+"' in ",
                ts.getName());
    }
    //tester.verbose("Running ",argv[0]," with:\n", content);
    cmd.process(tester, argv, content);
  }
}

/**
   General utilities for the SQLTester bits.
*/
final class Util {

  //! Throws a new T, appending all msg args into a string for the message.
  public static void toss(Class<? extends Exception> errorType, Object... msg) throws Exception {
    StringBuilder sb = new StringBuilder();
    for(Object s : msg) sb.append(s);
    final java.lang.reflect.Constructor<? extends Exception> ctor =
      errorType.getConstructor(String.class);
    throw ctor.newInstance(sb.toString());
  }

  public static void toss(Object... msg) throws Exception{
    toss(RuntimeException.class, msg);
  }

  public static void badArg(Object... msg) throws Exception{
    toss(IllegalArgumentException.class, msg);
  }

  //! Tries to delete the given file, silently ignoring failure.
  public static void unlink(String filename){
    try{
      final java.io.File f = new java.io.File(filename);
      f.delete();
    }catch(Exception e){
      /* ignore */
    }
  }

  /**
     Appends all entries in argv[1..end] into a space-separated
     string, argv[0] is not included because it's expected to
     be a command name.
  */
  public static String argvToString(String[] argv){
    StringBuilder sb = new StringBuilder();
    for(int i = 1; i < argv.length; ++i ){
      if( i>1 ) sb.append(" ");
      sb.append( argv[i] );
    }
    return sb.toString();
  }

}
