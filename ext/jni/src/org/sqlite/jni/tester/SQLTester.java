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
  private final Outer outer = new Outer();
  private final StringBuilder inputBuffer = new StringBuilder();
  private String nullView;
  private int nTotalTest = 0;
  private int nTestFile = 0;
  private int nTest;
  private sqlite3[] aDb = {};

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
    for(String f : listInFiles){
      reset();
      ++nTestFile;
      final TestScript ts = new TestScript(f);
      ts.setVerbose(this.outer.getVerbose());
      verbose(">>> Test",ts.getName(),"...");
      ts.run(this);
      verbose("<<< Ran",nTest,"test(s) in",f);
    }
  }

  private void resetDbs(){
    for(sqlite3 db : aDb) sqlite3_close_v2(db);
  }

  StringBuilder resetInputBuffer(){
    inputBuffer.delete(0, inputBuffer.length());
    return inputBuffer;
  }

  StringBuilder getInputBuffer(){
    return inputBuffer;
  }

  String getInputBufferText(){
    return inputBuffer.toString();
  }

  String takeInputBuffer(){
    final String rc = inputBuffer.toString();
    resetInputBuffer();
    return rc;
  }

  void reset(){
    nTest = 0;
    nullView = "nil";
    resetInputBuffer();
    resetDbs();
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
      }
      t.addTestScript(a);
    }
    t.runTests();
    t.outer.outln("Processed",t.nTotalTest,"test(s) in",t.nTestFile,"file(s).");
  }
}

/**
   Base class for test script commands.

   Each subclass must have a ctor with this signature:

   (SQLTester testContext, String[] argv, String content) throws Exception

   argv is a list with the command name followed by any
   arguments to that command. The argcCheck() method provides
   very basic argc validation.

   The content is any text content which was specified after the
   command. Any command which does not permit content must pass that
   argument to affirmNoContent() in their constructor.

   Tests must throw on error.
*/
class Command {
  protected SQLTester tester;
  Command(SQLTester t){tester = t;}

  protected final void toss(Class<? extends Exception> errorType, Object... msg) throws Exception {
    StringBuilder sb = new StringBuilder();
    int i = 0;
    for(Object s : msg) sb.append(((0==i++) ? "" : " ")+s);
    final java.lang.reflect.Constructor<? extends Exception> ctor =
      errorType.getConstructor(String.class);
    throw ctor.newInstance(sb.toString());
  }

  protected final void toss(Object... msg) throws Exception{
    toss(RuntimeException.class, msg);
  }

  protected final void badArg(Object... msg) throws Exception{
    toss(IllegalArgumentException.class, msg);
  }

  protected final void argcCheck(String[] argv, int min, int max) throws Exception{
    int argc = argv.length-1;
    if(argc<min || argc>max){
      if( min==max ) badArg(argv[0],"requires exactly",min,"argument(s)");
      else badArg(argv[0],"requires",min,"-",max,"arguments.");
    }
  }

  protected final void argcCheck(String[] argv, int argc) throws Exception{
    argcCheck(argv, argc, argc);
  }

  protected void affirmNoContent(String content) throws Exception{
    if(null != content){
      badArg(this.getClass().getName(),"does not accept content.");
    }
  }
}

class DbCommand extends Command {
  public DbCommand(SQLTester t, String[] argv, String content) throws Exception{
    super(t);
    argcCheck(argv,1);
    affirmNoContent(content);
    //t.verbose(argv[0],argv[1]);
  }
}

class NullCommand extends Command {
  public NullCommand(SQLTester t, String[] argv, String content) throws Exception{
    super(t);
    argcCheck(argv,1);
    affirmNoContent(content);
    t.setNullValue(argv[1]);
    //t.verbose(argv[0],argv[1]);
  }
}

class PrintCommand extends Command {
  public PrintCommand(SQLTester t, String[] argv, String content) throws Exception{
    super(t);
    argcCheck(argv,0);
    t.outln(content);
  }
}

class ResultCommand extends Command {
  public ResultCommand(SQLTester t, String[] argv, String content) throws Exception{
    super(t);
    argcCheck(argv,0);
    //t.verbose(argv[0],"command is TODO");
    t.incrementTestCounter();
  }
}

class TestCaseCommand extends Command {
  public TestCaseCommand(SQLTester t, String[] argv, String content) throws Exception{
    super(t);
    argcCheck(argv,1);
    //t.verbose(argv[0],argv[1]);
  }
}

class CommandDispatcher {
  static Class getCommandByName(String name){
    switch(name){
      case "db": return DbCommand.class;
      case "null": return NullCommand.class;
      case "print": return PrintCommand.class;
      case "result": return ResultCommand.class;
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
