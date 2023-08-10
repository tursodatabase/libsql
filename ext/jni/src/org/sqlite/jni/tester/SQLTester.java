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

class SQLTesterException extends RuntimeException {
  public SQLTesterException(String msg){
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
  private int nAbortedScript = 0;
  private int nTest;
  private boolean emitColNames;
  private final sqlite3[] aDb = new sqlite3[7];
  private int iCurrentDb = 0;
  private final String initialDbName = "test.db";
  private TestScript currentScript;

  public SQLTester(){
    reset();
  }

  public void setVerbosity(int level){
    this.outer.setVerbosity( level );
  }
  public int getVerbosity(){
    return this.outer.getVerbosity();
  }
  public boolean isVerbose(){
    return this.outer.isVerbose();
  }

  void outputColumnNames(boolean b){ emitColNames = b; }

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
    //verbose("Added file ",filename);
  }

  public void setupInitialDb() throws Exception {
    Util.unlink(initialDbName);
    openDb(0, initialDbName, true);
  }

  TestScript getCurrentScript(){
    return currentScript;
  }

  private void runTests() throws Exception {
    for(String f : listInFiles){
      reset();
      setupInitialDb();
      ++nTestFile;
      final TestScript ts = new TestScript(f);
      outln("----->>>>> running [",f,"]");
      try{
        ts.run(this);
      }catch(UnknownCommand e){
        /* currently not fatal */
        outln(e);
        ++nAbortedScript;
      }catch(IncompatibleDirective e){
        /* not fatal */
        outln(e);
        ++nAbortedScript;
      }catch(Exception e){
        ++nAbortedScript;
        throw e;
      }finally{
        outln("<<<<<----- ",nTest," test(s) in ",ts.getFilename());
      }
    }
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
    return affirmDbId(id).aDb[id];
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
      Util.toss(SQLTesterException.class, "db open failed with code ",
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
    sb.append(org.sqlite.jni.ResultCode.getEntryForInt(rc)).append(' ');
    final String msg = escapeSqlValue(sqlite3_errmsg(db));
    if( '{' == msg.charAt(0) ){
      sb.append(msg);
    }else{
      sb.append('{').append(msg).append('}');
    }
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
    try{
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
                    Util.toss(RuntimeException.class, "Unhandled ResultBufferMode.");
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
      sqlite3_finalize(stmt);
    }
    if( 0!=rc && throwOnError ){
      Util.toss(RuntimeException.class, "db op failed with rc="
                +rc+": "+sqlite3_errmsg(db));
    }
    return rc;
  }

  public static void main(String[] argv) throws Exception{
    final SQLTester t = new SQLTester();
    boolean v2 = false;
    for(String a : argv){
      if(a.startsWith("-")){
        final String flag = a.replaceFirst("-+","");
        if( flag.equals("verbose") ){
          t.setVerbosity(t.getVerbosity() + 1);
        }else{
          throw new IllegalArgumentException("Unhandled flag: "+flag);
        }
        continue;
      }
      t.addTestScript(a);
    }
    try {
      t.runTests();
    }finally{
      t.outln("Processed ",t.nTotalTest," test(s) in ",t.nTestFile," file(s).");
      if( t.nAbortedScript > 0 ){
        t.outln("Aborted ",t.nAbortedScript," script(s).");
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
