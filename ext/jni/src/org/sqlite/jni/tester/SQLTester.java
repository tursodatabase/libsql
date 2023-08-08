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
import org.sqlite.jni.*;
import static org.sqlite.jni.SQLite3Jni.*;

class TestFailure extends RuntimeException {
  public TestFailure(String msg){
    super(msg);
  }
}
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
  private int nTest;
  private final sqlite3[] aDb = new sqlite3[7];
  private int iCurrentDb = 0;
  private final String initialDbName = "test.db";

  public SQLTester(){
    reset();
  }

  public void setVerbose(boolean b){
    this.outer.setVerbose(b);
  }
  public boolean isVerbose(){
    return this.outer.isVerbose();
  }

  @SuppressWarnings("unchecked")
  public void verbose(Object... vals){
    outer.verbose(vals);
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
    verbose("Added file ",filename);
  }

  public void setupInitialDb() throws Exception {
    Util.unlink(initialDbName);
    openDb(0, initialDbName, true);
  }

  public void runTests() throws Exception {
    // process each input file
    outln("Verbose = ",outer.isVerbose());
    for(String f : listInFiles){
      reset();
      setupInitialDb();
      ++nTestFile;
      final TestScript ts = new TestScript(f);
      outln("---------> Test ",ts.getName()," ...");
      ts.run(this);
      outln("<--------- ",nTest," test(s) in ",f);
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

  String getInputBufferText(){ return inputBuffer.toString(); }

  String getResultBufferText(){ return resultBuffer.toString(); }

  private String takeBuffer(StringBuilder b){
    final String rc = b.toString();
    clearBuffer(b);
    return rc;
  }

  String takeInputBuffer(){ return takeBuffer(inputBuffer); }

  String takeResultBuffer(){ return takeBuffer(resultBuffer); }

  int getCurrentDbId(){ return iCurrentDb; }

  SQLTester affirmDbId(int n) throws Exception{
    if(n<0 || n>=aDb.length){
      Util.toss(IllegalArgumentException.class,"illegal db number.");
    }
    return this;
  }

  sqlite3 setCurrentDb(int n) throws Exception{
    return affirmDbId(n).aDb[n];
  }

  sqlite3 getCurrentDb(){ return aDb[iCurrentDb]; }

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
      Util.toss(TestFailure.class, "db open failed with code",
                rc,"and message:",msg);
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

  void incrementTestCounter(){ ++nTest; ++nTotalTest; }

  String escapeSqlValue(String v){
    // TODO: implement the escaping rules
    return v;
  }

  private void appendDbErr(sqlite3 db, StringBuilder sb, int rc){
    sb.append(org.sqlite.jni.ResultCode.getEntryForInt(rc))
      .append(" {")
      .append(escapeSqlValue(sqlite3_errmsg(db)))
      .append("}");
  }

  public int execSql(sqlite3 db, boolean throwOnError,
                     boolean appendToResult, String sql) throws Exception {
    final OutputPointer.Int32 oTail = new OutputPointer.Int32();
    final OutputPointer.sqlite3_stmt outStmt = new OutputPointer.sqlite3_stmt();
    final byte[] sqlUtf8 = sql.getBytes(StandardCharsets.UTF_8);
    if( null==db ) db = getCurrentDb();
    int pos = 0, n = 1;
    byte[] sqlChunk = sqlUtf8;
    int rc = 0;
    sqlite3_stmt stmt = null;
    int spacing = 0 /* emit a space for --result if>0 */ ;
    final StringBuilder sb = appendToResult ? resultBuffer : null;
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
            sb.append( escapeSqlValue(val) );
          }
          //sb.append('\n');
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
          t.setVerbose(true);
        }else if( flag.equals("quiet") ) {
          t.setVerbose(false);
        }else{
          throw new IllegalArgumentException("Unhandled flag: "+flag);
        }
        continue;
      }
      t.addTestScript(a);
    }
    t.runTests();
    t.outer.outln("Processed ",t.nTotalTest," test(s) in ",t.nTestFile," file(s).");
  }
}

/**
   Base class for test script commands. It provides a set of utility
   APIs for concrete command implementations.

   Each subclass must have a ctor with this signature:

   (SQLTester testContext, String[] argv, String content) throws Exception

   argv is a list with the command name followed by any
   arguments to that command. The argcCheck() method provides
   very basic argc validation.

   The content is any text content which was specified after the
   command, or null if there is null. Any command which does not
   permit content must pass that argument to affirmNoContent() in
   their constructor. Similary, those which require content should
   pass it to affirmHasContent().

   For simplicity, instantiating the test is intended to execute it,
   as opposed to delaying execution until a method devoted to that.

   Tests must throw on error.
*/
class Command {
  protected Command(){}

  protected final void argcCheck(String[] argv, int min, int max) throws Exception{
    int argc = argv.length-1;
    if(max<0) max = 99999999;
    if(argc<min || argc>max){
      if( min==max ) Util.badArg(argv[0],"requires exactly",min,"argument(s)");
      else Util.badArg(argv[0],"requires",min,"-",max,"arguments.");
    }
  }

  protected final void argcCheck(String[] argv, int argc) throws Exception{
    argcCheck(argv, argc, argc);
  }

  //! Throws if content is not null.
  protected void affirmNoContent(String content) throws Exception{
    if(null != content){
      Util.badArg(this.getClass().getName(),"does not accept content.");
    }
  }

  //! Throws if content is null.
  protected void affirmHasContent(String content) throws Exception{
    if(null == content){
      Util.badArg(this.getClass().getName(),"requires content.");
    }
  }
}

class CloseDbCommand extends Command {
  public CloseDbCommand(SQLTester t, String[] argv, String content) throws Exception{
    argcCheck(argv,0,1);
    affirmNoContent(content);
    Integer id;
    if(argv.length>1){
      String arg = argv[1];
      if("all".equals(arg)){
        t.verbose(argv[0]," all dbs");
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
    t.verbose(argv[0]," db ",id);
  }
}

class DbCommand extends Command {
  public DbCommand(SQLTester t, String[] argv, String content) throws Exception{
    argcCheck(argv,1);
    affirmNoContent(content);
    final sqlite3 db = t.setCurrentDb( Integer.parseInt(argv[1]) );
    t.verbose(argv[0]," set db to ",db);
  }
}

class GlobCommand extends Command {
  protected GlobCommand(boolean negate, SQLTester t,
                        String[] argv, String content) throws Exception{
    argcCheck(argv,1);
    affirmNoContent(content);
    final String glob = argv[1].replace("#","[0-9]");
    t.verbose(argv[0]," is TODO. Pattern = ",glob);
  }
  public GlobCommand(SQLTester t, String[] argv, String content) throws Exception{
    this(false, t, argv, content);
  }
}

class NewDbCommand extends Command {
  public NewDbCommand(SQLTester t, String[] argv, String content) throws Exception{
    argcCheck(argv,1);
    affirmNoContent(content);
    String fname = argv[1];
    Util.unlink(fname);
    final sqlite3 db = t.openDb(fname, true);
    t.verbose(argv[0]," db ",db);
  }
}

class NoopCommand extends Command {
  public NoopCommand(SQLTester t, String[] argv, String content) throws Exception{
  }
}

class NotGlobCommand extends GlobCommand {
  public NotGlobCommand(SQLTester t, String[] argv, String content) throws Exception{
    super(true, t, argv, content);
  }
}

class NullCommand extends Command {
  public NullCommand(SQLTester t, String[] argv, String content) throws Exception{
    argcCheck(argv,1);
    affirmNoContent(content);
    t.setNullValue(argv[1]);
    //t.verbose(argv[0]," ",argv[1]);
  }
}

class OpenDbCommand extends Command {
  public OpenDbCommand(SQLTester t, String[] argv, String content) throws Exception{
    argcCheck(argv,1);
    affirmNoContent(content);
    String fname = argv[1];
    final sqlite3 db = t.openDb(fname, false);
    t.verbose(argv[0]," db ",db);
  }
}


class PrintCommand extends Command {
  public PrintCommand(SQLTester t, String[] argv, String content) throws Exception{
    argcCheck(argv,0);
    t.outln(content);
  }
}

class ResultCommand extends Command {
  public ResultCommand(SQLTester t, String[] argv, String content) throws Exception{
    argcCheck(argv,1,-1);
    affirmNoContent(content);
    t.incrementTestCounter();
    final String sql = t.takeInputBuffer();
    //t.verbose(argv[0]," SQL =\n",sql);
    int rc = t.execSql(null, true, true, sql);
    final String result = t.getResultBufferText().trim();
    StringBuilder sbExpect = new StringBuilder();
    for(int i = 1; i < argv.length; ++i ){
      if( i>1 ) sbExpect.append(" ");
      sbExpect.append( argv[i] );
    }
    final String sArgs = sbExpect.toString();
    //t.verbose(argv[0]," rc = ",rc," result buffer:\n", result,"\nargs:\n",sArgs);
    if( !result.equals(sArgs) ){
      Util.toss(TestFailure.class, argv[0]," comparison failed.");
    }
  }
}

class RunCommand extends Command {
  public RunCommand(SQLTester t, String[] argv, String content) throws Exception{
    argcCheck(argv,0);
    affirmHasContent(content);
    int rc = t.execSql(null, false, false, content);
    if( 0!=rc ){
      sqlite3 db = t.getCurrentDb();
      String msg = sqlite3_errmsg(db);
      t.verbose(argv[0]," non-fatal command error #",rc,": ",
                msg,"\nfor SQL:\n",content);
    }
  }
}

class TestCaseCommand extends Command {
  public TestCaseCommand(SQLTester t, String[] argv, String content) throws Exception{
    argcCheck(argv,1);
    affirmHasContent(content);
    // TODO: do something with the test name
    t.clearResultBuffer();
    t.clearInputBuffer().append(content);
    //t.verbose(argv[0]," input buffer: ",content);
  }
}

class CommandDispatcher {

  static Class getCommandByName(String name){
    switch(name){
      case "close":    return CloseDbCommand.class;
      case "db":       return DbCommand.class;
      case "glob":     return GlobCommand.class;
      case "new":      return NewDbCommand.class;
      case "notglob":  return NotGlobCommand.class;
      case "null":     return NullCommand.class;
      case "oom":      return NoopCommand.class;
      case "open":     return OpenDbCommand.class;
      case "print":    return PrintCommand.class;
      case "result":   return ResultCommand.class;
      case "run":      return RunCommand.class;
      case "testcase": return TestCaseCommand.class;
      default: return null;
    }
  }

  @SuppressWarnings("unchecked")
  static void dispatch(SQLTester tester, String[] argv, String content) throws Exception{
    final Class cmdClass = getCommandByName(argv[0]);
    if(null == cmdClass){
      Util.toss(IllegalArgumentException.class,
           "No command handler found for '"+argv[0]+"'");
    }
    final java.lang.reflect.Constructor<Command> ctor =
      cmdClass.getConstructor(SQLTester.class, String[].class, String.class);
    try{
      //tester.verbose("Running ",argv[0]," with:\n", content);
      ctor.newInstance(tester, argv, content);
    }catch(java.lang.reflect.InvocationTargetException e){
      throw (Exception)e.getCause();
    }
  }
}

final class Util {
  public static void toss(Class<? extends Exception> errorType, Object... msg) throws Exception {
    StringBuilder sb = new StringBuilder();
    int i = 0;
    for(Object s : msg) sb.append(((0==i++) ? "" : " ")+s);
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

  public static void unlink(String filename){
    try{
      final java.io.File f = new java.io.File(filename);
      f.delete();
    }catch(Exception e){
      /* ignore */
    }
  }
}
