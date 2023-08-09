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
** This file contains the TestScript2 part of the SQLTester framework.
*/
package org.sqlite.jni.tester;
//import java.util.regex.*;
import java.util.Arrays;
import java.nio.charset.StandardCharsets;

/**
   This class represents a single test script. It handles (or
   delegates) its the reading-in and parsing, but the details of
   evaluation are delegated elsewhere.
*/
class TestScript2 {
  private String filename = null;
  private final Cursor curs = new Cursor();
  private final Outer outer = new Outer();

  private static final class Cursor {
    private final StringBuilder sb = new StringBuilder();
    byte[] src = null;
    int pos = 0;
    int lineNo = 1;
    boolean inComment = false;

    void reset(){
      sb.setLength(0); pos = 0; lineNo = 1; inComment = false;
    }
  }

  private byte[] readFile(String filename) throws Exception {
    return java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(filename));
  }

  /**
     Initializes the script with the content of the given file.
     Throws if it cannot read the file.
  */
  public TestScript2(String filename) throws Exception{
    this.filename = filename;
    setVerbosity(2);
    curs.src = readFile(filename);
  }

  public String getFilename(){
    return filename;
  }

  public void setVerbosity(int level){
    outer.setVerbosity(level);
  }

  @SuppressWarnings("unchecked")
  private <T> TestScript2 verbose(T... vals){
    outer.verbose(vals);
    return this;
  }

  @SuppressWarnings("unchecked")
  private void tossSyntax(Object... msg){
    StringBuilder sb = new StringBuilder();
    sb.append(this.filename).append(":").append(curs.lineNo).
      append(": ");
    for(Object o : msg) sb.append(o);
    throw new RuntimeException(sb.toString());
  }

  private void reset(){
    curs.reset();
  }

  /**
     Returns the next line from the buffer, minus the trailing EOL.
     If skipLeadingWs is true then all leading whitespace (including
     blank links) is skipped over and will not appear in the resulting
     string.

     Returns null when all input is consumed. Throws if it reads
     illegally-encoded input, e.g. (non-)characters in the range
     128-256.
  */
  String getLine(boolean skipLeadingWs){
    curs.sb.setLength(0);
    byte b = 0, prevB = 0;
    int i = curs.pos;
    if(skipLeadingWs) {
      /* Skip any leading spaces, including newlines. This will eliminate
         blank lines. */
      for(; i < curs.src.length; ++i, prevB=b){
        b = curs.src[i];
        switch((int)b){
          case 32/*space*/: case 9/*tab*/: case 13/*CR*/: continue;
          case 10/*NL*/: ++curs.lineNo; continue;
          default: break;
        }
        break;
      }
    }
    if( i==curs.src.length ){
      return null /* EOF */;
    }
    boolean doBreak = false;
    final byte[] aChar = {0,0,0,0} /* multi-byte char buffer */;
    int nChar = 0 /* number of bytes in the char */;
    for(; i < curs.src.length && !doBreak; ++i){
      b = curs.src[i];
      switch( (int)b ){
        case 13/*CR*/: continue;
        case 10/*NL*/:
          ++curs.lineNo;
          if(curs.sb.length()>0) doBreak = true;
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
              if( b > 127 ) tossSyntax("Invalid character (#"+(int)b+").");
              break;
          }
          if( 1==nChar ){
            curs.sb.append((char)b);
          }else{
            for(int x = 0; x < nChar; ++x) aChar[x] = curs.src[i+x];
            curs.sb.append(new String(Arrays.copyOf(aChar, nChar),
                                      StandardCharsets.UTF_8));
            i += nChar-1;
          }
          break;
      }
    }
    curs.pos = i;
    if( 0==curs.sb.length() && i==curs.src.length ){
      return null /* EOF */;
    }
    return curs.sb.toString();
  }/*getLine()*/

  /**
     Runs this test script in the context of the given tester object.
  */
  @SuppressWarnings("unchecked")
  public void run(SQLTester tester) throws Exception {
    reset();
    setVerbosity(tester.getVerbosity());
    String line;
    while( null != (line = getLine(false)) ){
      verbose("LINE #",curs.lineNo-1,": ",line);
    }
  }
}
