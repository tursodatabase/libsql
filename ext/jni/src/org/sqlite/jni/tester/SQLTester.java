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
  private int totalTestCount = 0;
  private int testCount;

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

  //! Adds the given test script to the to-test list.
  public void addTestScript(String filename){
    listInFiles.add(filename);
    verbose("Added file",filename);
  }

  public void runTests() throws Exception {
    // process each input file
    for(String f : listInFiles){
      reset();
      final TestScript ts = new TestScript(f);
      ts.setVerbose(this.outer.getVerbose());
      verbose("Test",ts.getName(),"...");
      ts.run(this);
      verbose("Ran",testCount,"test(s).");
    }
  }

  void resetInputBuffer(){
    inputBuffer.delete(0, this.inputBuffer.length());
  }

  String getInputBuffer(){
    return inputBuffer.toString();
  }

  String takeInputBuffer(){
    final String rc = inputBuffer.toString();
    resetInputBuffer();
    return rc;
  }

  void reset(){
    testCount = 0;
    nullView = "nil";
    resetInputBuffer();
  }

  void setNullValue(String v){nullView = v;}

  void incrementTestCounter(){
    ++testCount;
    ++totalTestCount;
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
      }
      t.addTestScript(a);
    }
    t.runTests();
  }
}

class Command {
  protected SQLTester tester;
  Command(SQLTester t){tester = t;}

  protected final void badArg(Object... msg){
    StringBuilder sb = new StringBuilder();
    int i = 0;
    for(Object s : msg) sb.append(((0==i++) ? "" : " ")+s);
    throw new IllegalArgumentException(sb.toString());
  }

  protected final void argcCheck(String[] argv, int min, int max){
    int argc = argv.length-1;
    if(argc<min || argc>max){
      if( min==max ) badArg(argv[0],"requires exactly",min,"argument(s)");
      else badArg(argv[0],"requires",min,"-",max,"arguments.");
    }
  }

  protected final void argcCheck(String[] argv, int argc){
    argcCheck(argv, argc, argc);
  }

  protected void affirmNoContent(String content){
    if(null != content){
      badArg(this.getClass().getName(),"does not accept content.");
    }
  }
}

class DbCommand extends Command {
  public DbCommand(SQLTester t, String[] argv, String content){
    super(t);
    argcCheck(argv,1);
    affirmNoContent(content);
    //t.verbose(argv[0],argv[1]);
  }
}

class NullCommand extends Command {
  public NullCommand(SQLTester t, String[] argv, String content){
    super(t);
    argcCheck(argv,1);
    affirmNoContent(content);
    t.setNullValue(argv[1]);
    //t.verbose(argv[0],argv[1]);
  }
}

class ResultCommand extends Command {
  public ResultCommand(SQLTester t, String[] argv, String content){
    super(t);
    argcCheck(argv,0);
    t.verbose(argv[0],"command is TODO");
    t.incrementTestCounter();
  }
}

class TestCaseCommand extends Command {
  public TestCaseCommand(SQLTester t, String[] argv, String content){
    super(t);
    argcCheck(argv,1);
    t.verbose(argv[0],argv[1]);
  }
}

class CommandDispatcher {
  static Class getCommandByName(String name){
    switch(name){
      case "db": return DbCommand.class;
      case "null": return NullCommand.class;
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
