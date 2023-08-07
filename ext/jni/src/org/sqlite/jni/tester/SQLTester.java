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

  public SQLTester(){
  }

  public void setVerbose(boolean b){
    this.outer.setVerbose(b);
  }

  @SuppressWarnings("unchecked")
  private <T> void verbose(T... vals){
    this.outer.verbose(vals);
  }

  //! Adds the given test script to the to-test list.
  public void addTestScript(String filename){
    listInFiles.add(filename);
    verbose("Added file",filename);
  }

  public void runTests() throws Exception {
    // process each input file
    for(String f : listInFiles){
      final TestScript ts = new TestScript(f);
      ts.setVerbose(this.outer.getVerbose());
      verbose("Test",ts.getName(),"...");
      ts.dump();
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
