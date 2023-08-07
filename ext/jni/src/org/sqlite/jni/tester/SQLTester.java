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

  public SQLTester(){
  }

  public void setVerbose(boolean b){
    this.outer.setVerbose(b);
  }

  @SuppressWarnings("unchecked")
  public <T> void verbose(T... vals){
    this.outer.verbose(vals);
  }

  @SuppressWarnings("unchecked")
  public <T> void outln(T... vals){
    this.outer.outln(vals);
  }

  //! Adds the given test script to the to-test list.
  public void addTestScript(String filename){
    listInFiles.add(filename);
    verbose("Added file",filename);
  }

  public void runTests() throws Exception {
    // process each input file
    for(String f : listInFiles){
      this.reset();
      final TestScript ts = new TestScript(f);
      ts.setVerbose(this.outer.getVerbose());
      verbose("Test",ts.getName(),"...");
      ts.run(this);
    }
  }

  void resetInputBuffer(){
    this.inputBuffer.delete(0, this.inputBuffer.length());
  }

  void reset(){
    this.resetInputBuffer();
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

abstract class Command {
  private SQLTester tester;
  Command(SQLTester t){tester = t;}
  public SQLTester getTester(){return tester;}

  public abstract void process(String[] argv, String content);
}

class NullCommand extends Command {
  public NullCommand(SQLTester t){super(t);}

  public void process(String[] argv, String content){
  }
}

class TestCaseCommand extends Command {
  public TestCaseCommand(SQLTester t){super(t);}

  public void process(String[] argv, String content){
  }
}

class ResultCommand extends Command {
  public ResultCommand(SQLTester t){super(t);}

  public void process(String[] argv, String content){
  }
}

class CommandFactory {
  static Command getCommandByName(SQLTester t, String name){
    switch(name){
      case "null": return new NullCommand(t);
      case "result": return new ResultCommand(t);
      case "testcase": return new TestCaseCommand(t);
      default: return null;
    }
  }
}
