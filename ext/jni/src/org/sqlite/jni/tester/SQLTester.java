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
import org.sqlite.jni.*;
import static org.sqlite.jni.SQLite3Jni.*;

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

  public SQLTester(){
    reset();
  }

  public void setVerbose(boolean b){
    this.outer.setVerbose(b);
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
    verbose("Added file",filename);
  }

  public void runTests() throws Exception {
    // process each input file
    outln("Verbose =",outer.isVerbose());
    for(String f : listInFiles){
      reset();
      ++nTestFile;
      final TestScript ts = new TestScript(f);
      outln("---------> Test",ts.getName(),"...");
      ts.run(this);
      outln("<---------",nTest,"test(s) in",f);
    }
  }

  private StringBuilder resetBuffer(StringBuilder b){
    b.delete(0, b.length());
    return b;
  }

  StringBuilder resetInputBuffer(){
    return resetBuffer(inputBuffer);
  }

  StringBuilder resetResultBuffer(){
    return resetBuffer(resultBuffer);
  }

  StringBuilder getInputBuffer(){ return inputBuffer; }

  String getInputBufferText(){ return inputBuffer.toString(); }

  private String takeBuffer(StringBuilder b){
    final String rc = b.toString();
    resetBuffer(b);
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
      Util.toss("db open failed with code",rc,"and message:",msg);
    }
    return aDb[iCurrentDb] = db;
  }

  /**
     Resets all tester context state except for that related to
     tracking running totals.
  */
  void reset(){
    nTest = 0;
    nullView = "nil";
    resetInputBuffer();
    closeAllDbs();
  }

  void setNullValue(String v){nullView = v;}

  void incrementTestCounter(){ ++nTest; ++nTotalTest; }

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
    t.outer.outln("Processed",t.nTotalTest,"test(s) in",t.nTestFile,"file(s).");
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
        t.verbose(argv[0],"all dbs");
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
    t.verbose(argv[0],"db",id);
  }
}

class DbCommand extends Command {
  public DbCommand(SQLTester t, String[] argv, String content) throws Exception{
    argcCheck(argv,1);
    affirmNoContent(content);
    final sqlite3 db = t.setCurrentDb( Integer.parseInt(argv[1]) );
    t.verbose(argv[0],"set db to",db);
  }
}

class GlobCommand extends Command {
  protected GlobCommand(boolean negate, SQLTester t,
                        String[] argv, String content) throws Exception{
    argcCheck(argv,1);
    affirmNoContent(content);
    final String glob = argv[1].replace("#","[0-9]");
    t.verbose(argv[0],"is TODO. Pattern =",glob);
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
    t.verbose(argv[0],"db",db);
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
    //t.verbose(argv[0],argv[1]);
  }
}

class OpenDbCommand extends Command {
  public OpenDbCommand(SQLTester t, String[] argv, String content) throws Exception{
    argcCheck(argv,1);
    affirmNoContent(content);
    String fname = argv[1];
    Util.unlink(fname);
    final sqlite3 db = t.openDb(fname, false);
    t.verbose(argv[0],"db",db);
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
    argcCheck(argv,0);
    //t.verbose(argv[0],"command is TODO");
    t.incrementTestCounter();
  }
}

class TestCaseCommand extends Command {
  public TestCaseCommand(SQLTester t, String[] argv, String content) throws Exception{
    argcCheck(argv,1);
    affirmHasContent(content);
    t.resetInputBuffer();
    t.resetResultBuffer().append(content);
    t.verbose(argv[0],"result buffer:",content);
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
      case "testcase": return TestCaseCommand.class;
      default: return null;
    }
  }

  @SuppressWarnings("unchecked")
  static void dispatch(SQLTester tester, String[] argv, String content) throws Exception{
    final Class cmdClass = getCommandByName(argv[0]);
    if(null == cmdClass){
      throw new IllegalArgumentException(
        "No command handler found for '"+argv[0]+"'"
      );
    }
    final java.lang.reflect.Constructor<Command> ctor =
      cmdClass.getConstructor(SQLTester.class, String[].class, String.class);
    //tester.verbose("Running",argv[0],"...");
    ctor.newInstance(tester, argv, content);
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
