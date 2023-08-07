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
  private java.util.List<String> listInFiles = new ArrayList<>();
  private boolean isVerbose = true;

  public SQLTester(){
  }

  public void setVerbose(boolean b){
    isVerbose = b;
  }

  public static <T> void out(T val){
    System.out.print(val);
  }

  public static <T> void outln(T val){
    System.out.println(val);
  }

  @SuppressWarnings("unchecked")
  public static <T> void out(T... vals){
    int n = 0;
    for(T v : vals) out((n++>0 ? " " : "")+v);
  }

  @SuppressWarnings("unchecked")
  public static <T> void outln(T... vals){
    out(vals);
    out("\n");
  }

  @SuppressWarnings("unchecked")
  private <T> void verbose(T... vals){
    if(isVerbose) outln(vals);
  }

  //! Adds the given test script to the to-test list.
  public void addTestScript(String filename){
    listInFiles.add(filename);
    verbose("Added file",filename);
  }

  public void runTests() throws Exception {
    // process each input file
    for(String f : listInFiles){
      verbose("Running test script",f);
      final TestScript ts = new TestScript(f);
    }
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
