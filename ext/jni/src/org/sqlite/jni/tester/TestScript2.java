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
import java.util.Arrays;
import java.nio.charset.StandardCharsets;
import java.util.regex.*;

class SQLTestException extends RuntimeException {
  public SQLTestException(String msg){
    super(msg);
  }
}

class SkipTestRemainder2 extends SQLTestException {
  public SkipTestRemainder2(TestScript2 ts){
    super(ts.getOutputPrefix()+": skipping remainder");
  }
}

class IncompatibleDirective extends SQLTestException {
  public IncompatibleDirective(TestScript2 ts, String line){
    super(ts.getOutputPrefix()+": incompatible directive: "+line);
  }
}

class UnknownCommand extends SQLTestException {
  public UnknownCommand(TestScript2 ts, String line){
    super(ts.getOutputPrefix()+": unknown command: "+line);
  }
}

abstract class Command2 {
  protected Command2(){}

  public abstract void process(
    SQLTester st, TestScript2 ts, String[] argv
  ) throws Exception;

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
}

class PrintCommand2 extends Command2 {
  public void process(
    SQLTester st, TestScript2 ts, String[] argv
  ) throws Exception{
    st.out(ts.getOutputPrefix(),": ");
    if( 1==argv.length ){
      st.outln( st.getInputText() );
    }else{
      st.outln( Util.argvToString(argv) );
    }
    final String body = ts.fetchCommandBody();
    if( null!=body ){
      st.out(body,"\n");
    }
  }
}

class CommandDispatcher2 {

  private static java.util.Map<String,Command2> commandMap =
    new java.util.HashMap<>();

  /**
     Returns a (cached) instance mapped to name, or null if no match
     is found.
  */
  static Command2 getCommandByName(String name){
    Command2 rv = commandMap.get(name);
    if( null!=rv ) return rv;
    switch(name){
      case "print":       rv = new PrintCommand2(); break;
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
  static void dispatch(SQLTester tester, TestScript2 ts, String[] argv) throws Exception{
    final Command2 cmd = getCommandByName(argv[0]);
    if(null == cmd){
      if( tester.skipUnknownCommands() ){
        ts.warn("skipping remainder because of unknown command '",argv[0],"'.");
        throw new SkipTestRemainder2(ts);
      }
      Util.toss(IllegalArgumentException.class,
                ts.getOutputPrefix()+": no command handler found for '"+argv[0]+"'.");
    }
    cmd.process(tester, ts, argv);
  }
}


/**
   This class represents a single test script. It handles (or
   delegates) its the reading-in and parsing, but the details of
   evaluation are delegated elsewhere.
*/
class TestScript2 {
  private String filename = null;
  private String moduleName = null;
  private final Cursor cur = new Cursor();
  private final Outer outer = new Outer();

  private static final class Cursor {
    private final StringBuilder sb = new StringBuilder();
    byte[] src = null;
    int pos = 0;
    int putbackPos = 0;
    int putbackLineNo = 0;
    int lineNo = 0 /* yes, zero */;
    int peekedPos = 0;
    int peekedLineNo = 0;
    boolean inComment = false;

    void reset(){
      sb.setLength(0); pos = 0; lineNo = 0/*yes, zero*/; inComment = false;
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
    cur.src = readFile(filename);
  }

  public String getFilename(){
    return filename;
  }

  public String getModuleName(){
    return moduleName;
  }

  public void setVerbosity(int level){
    outer.setVerbosity(level);
  }

  public String getOutputPrefix(){
    return "["+(moduleName==null ? filename : moduleName)+"] line "+
      cur.lineNo;
  }

  @SuppressWarnings("unchecked")
  private TestScript2 verbose(Object... vals){
    final int verbosity = outer.getVerbosity();
    if(verbosity>0){
      outer.out("VERBOSE",(verbosity>1 ? "+ " : " "),
                getOutputPrefix(),": ");
      outer.outln(vals);
    }
    return this;
  }

  @SuppressWarnings("unchecked")
  public TestScript2 warn(Object... vals){
    outer.out("WARNING ", getOutputPrefix(),": ");
    outer.outln(vals);
    return this;
  }

  @SuppressWarnings("unchecked")
  private void tossSyntax(Object... msg){
    StringBuilder sb = new StringBuilder();
    sb.append(this.filename).append(":").append(cur.lineNo).
      append(": ");
    for(Object o : msg) sb.append(o);
    throw new RuntimeException(sb.toString());
  }

  private void reset(){
    cur.reset();
  }


  /**
     Returns the next line from the buffer, minus the trailing EOL.

     Returns null when all input is consumed. Throws if it reads
     illegally-encoded input, e.g. (non-)characters in the range
     128-256.
  */
  String getLine(){
    if( cur.pos==cur.src.length ){
      return null /* EOF */;
    }
    cur.putbackPos = cur.pos;
    cur.putbackLineNo = cur.lineNo;
    cur.sb.setLength(0);
    final boolean skipLeadingWs = false;
    byte b = 0, prevB = 0;
    int i = cur.pos;
    if(skipLeadingWs) {
      /* Skip any leading spaces, including newlines. This will eliminate
         blank lines. */
      for(; i < cur.src.length; ++i, prevB=b){
        b = cur.src[i];
        switch((int)b){
          case 32/*space*/: case 9/*tab*/: case 13/*CR*/: continue;
          case 10/*NL*/: ++cur.lineNo; continue;
          default: break;
        }
        break;
      }
      if( i==cur.src.length ){
        return null /* EOF */;
      }
    }
    boolean doBreak = false;
    final byte[] aChar = {0,0,0,0} /* multi-byte char buffer */;
    int nChar = 0 /* number of bytes in the char */;
    for(; i < cur.src.length && !doBreak; ++i){
      b = cur.src[i];
      switch( (int)b ){
        case 13/*CR*/: continue;
        case 10/*NL*/:
          ++cur.lineNo;
          if(cur.sb.length()>0) doBreak = true;
          // Else it's an empty string
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
            cur.sb.append((char)b);
          }else{
            for(int x = 0; x < nChar; ++x) aChar[x] = cur.src[i+x];
            cur.sb.append(new String(Arrays.copyOf(aChar, nChar),
                                      StandardCharsets.UTF_8));
            i += nChar-1;
          }
          break;
      }
    }
    cur.pos = i;
    final String rv = cur.sb.toString();
    if( i==cur.src.length && 0==rv.length() ){
      return null /* EOF */;
    }
    return rv;
  }/*getLine()*/

  /**
     Fetches the next line then resets the cursor to its pre-call
     state. consumePeeked() can be used to consume this peeked line
     without having to re-parse it.
  */
  public String peekLine(){
    final int oldPos = cur.pos;
    final int oldPB = cur.putbackPos;
    final int oldPBL = cur.putbackLineNo;
    final int oldLine = cur.lineNo;
    final String rc = getLine();
    cur.peekedPos = cur.pos;
    cur.peekedLineNo = cur.lineNo;
    cur.pos = oldPos;
    cur.lineNo = oldLine;
    cur.putbackPos = oldPB;
    cur.putbackLineNo = oldPBL;
    return rc;
  }

  /**
     Only valid after calling peekLine() and before calling getLine().
     This places the cursor to the position it would have been at had
     the peekLine() had been fetched with getLine().
  */
  public void consumePeeked(){
    cur.pos = cur.peekedPos;
    cur.lineNo = cur.peekedLineNo;
  }

  /**
     Restores the cursor to the position it had before the previous
     call to getLine().
  */
  public void putbackLine(){
    cur.pos = cur.putbackPos;
    cur.lineNo = cur.putbackLineNo;
  }

  private static final Pattern patternRequiredProperties =
    Pattern.compile(" REQUIRED_PROPERTIES:[ \\t]*(\\S.*)\\s*$");
  private static final Pattern patternScriptModuleName =
    Pattern.compile(" SCRIPT_MODULE_NAME:[ \\t]*(\\S+)\\s*$");
  private static final Pattern patternMixedModuleName =
    Pattern.compile(" ((MIXED_)?MODULE_NAME):[ \\t]*(\\S+)\\s*$");
  private static final Pattern patternCommand =
    Pattern.compile("^--(([a-z-]+)( .*)?)$");

  /**
     Looks for "directives." If a compatible one is found, it is
     processed and this function returns. If an incompatible one is found,
     a description of it is returned and processing of the test must
     end immediately.
  */
  private void checkForDirective(String line) throws IncompatibleDirective {
    if(line.startsWith("#")){
      throw new IncompatibleDirective(this, "C-preprocessor input: "+line);
    }else if(line.startsWith("---")){
      new IncompatibleDirective(this, "Triple-dash: "+line);
    }
    Matcher m = patternScriptModuleName.matcher(line);
    if( m.find() ){
      moduleName = m.group(1);
      return;
    }
    m = patternRequiredProperties.matcher(line);
    if( m.find() ){
      throw new IncompatibleDirective(this, "REQUIRED_PROPERTIES: "+m.group(1));
    }
    m = patternMixedModuleName.matcher(line);
    if( m.find() ){
      throw new IncompatibleDirective(this, m.group(1)+": "+m.group(3));
    }
    return;
  }

  public boolean isCommandLine(String line){
    final Matcher m = patternCommand.matcher(line);
    return m.find();
  }

  /**
     If line looks like a command, returns an argv for that command
     invocation, else returns null.
  */
  public String[] getCommandArgv(String line){
    final Matcher m = patternCommand.matcher(line);
    return m.find() ? m.group(1).trim().split("\\s+") : null;
  }

  /**
     Fetches lines until the next command. Throws if
     checkForDirective() does.  Returns null if there is no input or
     it's only whitespace. The returned string is trim()'d of
     leading/trailing whitespace.
  */
  public String fetchCommandBody(){
    final StringBuilder sb = new StringBuilder();
    String line;
    while( (null != (line = peekLine())) ){
      checkForDirective(line);
      if( !isCommandLine(line) ){
        sb.append(line).append("\n");
        consumePeeked();
      }else{
        break;
      }
    }
    line = sb.toString().trim();
    return line.isEmpty() ? null : line;
  }

  public void processCommand(SQLTester t, String[] argv) throws Exception{
    //verbose("got argv: ",argv[0], " ", Util.argvToString(argv));
    //verbose("Input buffer = ",t.getInputBuffer());
    CommandDispatcher2.dispatch(t, this, argv);
  }

  /**
     Runs this test script in the context of the given tester object.
  */
  @SuppressWarnings("unchecked")
  public boolean run(SQLTester tester) throws Exception {
    reset();
    setVerbosity(tester.getVerbosity());
    String line, directive;
    String[] argv;
    while( null != (line = getLine()) ){
      //verbose(line);
      checkForDirective(line);
      argv = getCommandArgv(line);
      if( null!=argv ){
        processCommand(tester, argv);
        continue;
      }
      tester.appendInput(line,true);
    }
    return true;
  }
}
