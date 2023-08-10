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
** This file contains the TestScript part of the SQLTester framework.
*/
package org.sqlite.jni.tester;
import static org.sqlite.jni.SQLite3Jni.*;
import org.sqlite.jni.sqlite3;
import java.util.Arrays;
import java.nio.charset.StandardCharsets;
import java.util.regex.*;

class TestScriptFailed extends SQLTesterException {
  public TestScriptFailed(TestScript ts, String msg){
    super(ts.getOutputPrefix()+": "+msg);
  }
}

class UnknownCommand extends SQLTesterException {
  public UnknownCommand(TestScript ts, String cmd){
    super(ts.getOutputPrefix()+": unknown command: "+cmd);
  }
}

class IncompatibleDirective extends SQLTesterException {
  public IncompatibleDirective(TestScript ts, String line){
    super(ts.getOutputPrefix()+": incompatible directive: "+line);
  }
}

/**
   Base class for test script commands. It provides a set of utility
   APIs for concrete command implementations.

   Each subclass must have a public no-arg ctor and must implement
   the process() method which is abstract in this class.

   Commands are intended to be stateless, except perhaps for counters
   and similar internals. Specifically, no state which changes the
   behavior between any two invocations of process() should be
   retained.
*/
abstract class Command {
  protected Command(){}

  /**
     Must process one command-unit of work and either return
     (on success) or throw (on error).

     The first two arguments specify the context of the test.

     argv is a list with the command name followed by any arguments to
     that command. The argcCheck() method from this class provides
     very basic argc validation.
  */
  public abstract void process(
    SQLTester st, TestScript ts, String[] argv
  ) throws Exception;

  /**
     If argv.length-1 (-1 because the command's name is in argv[0]) does not
     fall in the inclusive range (min,max) then this function throws. Use
     a max value of -1 to mean unlimited.
  */
  protected final void argcCheck(TestScript ts, String[] argv, int min, int max) throws Exception{
    int argc = argv.length-1;
    if(argc<min || (max>=0 && argc>max)){
      if( min==max ){
        ts.toss(argv[0]," requires exactly ",min," argument(s)");
      }else if(max>0){
        ts.toss(argv[0]," requires ",min,"-",max," arguments.");
      }else{
        ts.toss(argv[0]," requires at least ",min," arguments.");
      }
    }
  }

  /**
     Equivalent to argcCheck(argv,argc,argc).
  */
  protected final void argcCheck(TestScript ts, String[] argv, int argc) throws Exception{
    argcCheck(ts, argv, argc, argc);
  }
}

//! --close command
class CloseDbCommand extends Command {
  public void process(SQLTester t, TestScript ts, String[] argv) throws Exception{
    argcCheck(ts,argv,0,1);
    Integer id;
    if(argv.length>1){
      String arg = argv[1];
      if("all".equals(arg)){
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
  }
}

//! --column-names command
class ColumnNamesCommand extends Command {
  public void process(
    SQLTester st, TestScript ts, String[] argv
  ) throws Exception{
    argcCheck(ts,argv,1);
    st.outputColumnNames( Integer.parseInt(argv[1])!=0 );
  }
}

//! --db command
class DbCommand extends Command {
  public void process(SQLTester t, TestScript ts, String[] argv) throws Exception{
    argcCheck(ts,argv,1);
    t.setCurrentDb( Integer.parseInt(argv[1]) );
  }
}

//! --glob command
class GlobCommand extends Command {
  private boolean negate = false;
  public GlobCommand(){}
  protected GlobCommand(boolean negate){ this.negate = negate; }

  public void process(SQLTester t, TestScript ts, String[] argv) throws Exception{
    argcCheck(ts,argv,1,-1);
    t.incrementTestCounter();
    final String sql = t.takeInputBuffer();
    int rc = t.execSql(null, true, ResultBufferMode.ESCAPED,
                       ResultRowMode.ONELINE, sql);
    final String result = t.getResultText();
    final String sArgs = Util.argvToString(argv);
    //t.verbose(argv[0]," rc = ",rc," result buffer:\n", result,"\nargs:\n",sArgs);
    final String glob = Util.argvToString(argv);
    rc = SQLTester.strglob(glob, result);
    if( (negate && 0==rc) || (!negate && 0!=rc) ){
      ts.toss(argv[0], " mismatch: ", glob," vs input: ",result);
    }
  }
}

//! --json command
class JsonCommand extends ResultCommand {
  public JsonCommand(){ super(ResultBufferMode.ASIS); }
}

//! --json-block command
class JsonBlockCommand extends TableResultCommand {
  public JsonBlockCommand(){ super(true); }
}

//! --new command
class NewDbCommand extends OpenDbCommand {
  public NewDbCommand(){ super(true); }
}

//! Placeholder dummy/no-op commands
class NoopCommand extends Command {
  public void process(SQLTester t, TestScript ts, String[] argv) throws Exception{
  }
}

//! --notglob command
class NotGlobCommand extends GlobCommand {
  public NotGlobCommand(){
    super(true);
  }
}

//! --null command
class NullCommand extends Command {
  public void process(
    SQLTester st, TestScript ts, String[] argv
  ) throws Exception{
    argcCheck(ts,argv,1);
    st.setNullValue( argv[1] );
  }
}

//! --open command
class OpenDbCommand extends Command {
  private boolean createIfNeeded = false;
  public OpenDbCommand(){}
  protected OpenDbCommand(boolean c){createIfNeeded = c;}
  public void process(SQLTester t, TestScript ts, String[] argv) throws Exception{
    argcCheck(ts,argv,1);
    t.openDb(argv[1], createIfNeeded);
  }
}

//! --print command
class PrintCommand extends Command {
  public void process(
    SQLTester st, TestScript ts, String[] argv
  ) throws Exception{
    st.out(ts.getOutputPrefix(),": ");
    if( 1==argv.length ){
      st.out( st.getInputText() );
    }else{
      st.outln( Util.argvToString(argv) );
    }
  }
}

//! --result command
class ResultCommand extends Command {
  private final ResultBufferMode bufferMode;
  protected ResultCommand(ResultBufferMode bm){ bufferMode = bm; }
  public ResultCommand(){ this(ResultBufferMode.ESCAPED); }
  public void process(SQLTester t, TestScript ts, String[] argv) throws Exception{
    argcCheck(ts,argv,0,-1);
    t.incrementTestCounter();
    final String sql = t.takeInputBuffer();
    //t.verbose(argv[0]," SQL =\n",sql);
    int rc = t.execSql(null, false, bufferMode, ResultRowMode.ONELINE, sql);
    final String result = t.getResultText().trim();
    final String sArgs = argv.length>1 ? Util.argvToString(argv) : "";
    if( !result.equals(sArgs) ){
      t.outln(argv[0]," FAILED comparison. Result buffer:\n",
              result,"\nargs:\n",sArgs);
      ts.toss(argv[0]+" comparison failed.");
    }
  }
}

//! --run command
class RunCommand extends Command {
  public void process(SQLTester t, TestScript ts, String[] argv) throws Exception{
    argcCheck(ts,argv,0,1);
    final sqlite3 db = (1==argv.length)
      ? t.getCurrentDb() : t.getDbById( Integer.parseInt(argv[1]) );
    final String sql = t.takeInputBuffer();
    int rc = t.execSql(db, false, ResultBufferMode.NONE,
                       ResultRowMode.ONELINE, sql);
    if( 0!=rc && t.isVerbose() ){
      String msg = sqlite3_errmsg(db);
      t.verbose(argv[0]," non-fatal command error #",rc,": ",
                msg,"\nfor SQL:\n",sql);
    }
  }
}

//! --tableresult command
class TableResultCommand extends Command {
  private final boolean jsonMode;
  protected TableResultCommand(boolean jsonMode){ this.jsonMode = jsonMode; }
  public TableResultCommand(){ this(false); }
  public void process(SQLTester t, TestScript ts, String[] argv) throws Exception{
    argcCheck(ts,argv,0);
    t.incrementTestCounter();
    String body = ts.fetchCommandBody();
    if( null==body ) ts.toss("Missing ",argv[0]," body.");
    body = body.trim();
    if( !body.endsWith("\n--end") ){
      ts.toss(argv[0], " must be terminated with --end.");
    }else{
      int n = body.length();
      body = body.substring(0, n-6);
    }
    final String[] globs = body.split("\\s*\\n\\s*");
    if( globs.length < 1 ){
      ts.toss(argv[0], " requires 1 or more ",
              (jsonMode ? "json snippets" : "globs"),".");
    }
    final String sql = t.takeInputBuffer();
    t.execSql(null, true,
              jsonMode ? ResultBufferMode.ASIS : ResultBufferMode.ESCAPED,
              ResultRowMode.NEWLINE, sql);
    final String rbuf = t.getResultText();
    final String[] res = rbuf.split("\n");
    if( res.length != globs.length ){
      ts.toss(argv[0], " failure: input has ", res.length,
              " row(s) but expecting ",globs.length);
    }
    for(int i = 0; i < res.length; ++i){
      final String glob = globs[i].replaceAll("\\s+"," ").trim();
      //t.verbose(argv[0]," <<",glob,">> vs <<",res[i],">>");
      if( jsonMode ){
        if( !glob.equals(res[i]) ){
          ts.toss(argv[0], " json <<",glob, ">> does not match: <<",
                  res[i],">>");
        }
      }else if( 0 != SQLTester.strglob(glob, res[i]) ){
        ts.toss(argv[0], " glob <<",glob,">> does not match: <<",res[i],">>");
      }
    }
  }
}

//! --testcase command
class TestCaseCommand extends Command {
  public void process(SQLTester t, TestScript ts, String[] argv) throws Exception{
    argcCheck(ts,argv,1);
    // TODO?: do something with the test name
    t.clearResultBuffer();
    t.clearInputBuffer();
  }
}

class CommandDispatcher2 {

  private static java.util.Map<String,Command> commandMap =
    new java.util.HashMap<>();

  /**
     Returns a (cached) instance mapped to name, or null if no match
     is found.
  */
  static Command getCommandByName(String name){
    Command rv = commandMap.get(name);
    if( null!=rv ) return rv;
    switch(name){
      case "close":       rv = new CloseDbCommand(); break;
      case "column-names":rv = new ColumnNamesCommand(); break;
      case "db":          rv = new DbCommand(); break;
      case "glob":        rv = new GlobCommand(); break;
      case "json":        rv = new JsonCommand(); break;
      case "json-block":  rv = new JsonBlockCommand(); break;
      case "new":         rv = new NewDbCommand(); break;
      case "notglob":     rv = new NotGlobCommand(); break;
      case "null":        rv = new NullCommand(); break;
      case "oom":         rv = new NoopCommand(); break;
      case "open":        rv = new OpenDbCommand(); break;
      case "print":       rv = new PrintCommand(); break;
      case "result":      rv = new ResultCommand(); break;
      case "run":         rv = new RunCommand(); break;
      case "tableresult": rv = new TableResultCommand(); break;
      case "testcase":    rv = new TestCaseCommand(); break;
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
  static void dispatch(SQLTester tester, TestScript ts, String[] argv) throws Exception{
    final Command cmd = getCommandByName(argv[0]);
    if(null == cmd){
      throw new UnknownCommand(ts, argv[0]);
    }
    cmd.process(tester, ts, argv);
  }
}


/**
   This class represents a single test script. It handles (or
   delegates) its the reading-in and parsing, but the details of
   evaluation are delegated elsewhere.
*/
class TestScript {
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
  public TestScript(String filename) throws Exception{
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
  private TestScript verboseN(int level, Object... vals){
    final int verbosity = outer.getVerbosity();
    if(verbosity>=level){
      outer.out("VERBOSE",(verbosity>1 ? "+ " : " "),
                getOutputPrefix(),": ");
      outer.outln(vals);
    }
    return this;
  }

  private TestScript verbose1(Object... vals){return verboseN(1,vals);}
  private TestScript verbose2(Object... vals){return verboseN(2,vals);}

  @SuppressWarnings("unchecked")
  public TestScript warn(Object... vals){
    outer.out("WARNING ", getOutputPrefix(),": ");
    outer.outln(vals);
    return this;
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
              if( b > 127 ) this.toss("Invalid character (#"+(int)b+").");
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
  String peekLine(){
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
  void consumePeeked(){
    cur.pos = cur.peekedPos;
    cur.lineNo = cur.peekedLineNo;
  }

  /**
     Restores the cursor to the position it had before the previous
     call to getLine().
  */
  void putbackLine(){
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
      new IncompatibleDirective(this, "triple-dash: "+line);
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
    if( line.indexOf("\n|")>=0 ){
      throw new IncompatibleDirective(this, "newline-pipe combination.");
    }
    return;
  }

  boolean isCommandLine(String line, boolean checkForImpl){
    final Matcher m = patternCommand.matcher(line);
    boolean rc = m.find();
    if( rc && checkForImpl ){
      rc = null!=CommandDispatcher2.getCommandByName(m.group(2));
    }
    return rc;
  }

  /**
     If line looks like a command, returns an argv for that command
     invocation, else returns null.
  */
  String[] getCommandArgv(String line){
    final Matcher m = patternCommand.matcher(line);
    return m.find() ? m.group(1).trim().split("\\s+") : null;
  }

  /**
     Fetches lines until the next recognized command. Throws if
     checkForDirective() does.  Returns null if there is no input or
     it's only whitespace. The returned string retains all whitespace.

     Note that "subcommands", --command-like constructs in the body
     which do not match a known command name are considered to be
     content, not commands.
  */
  String fetchCommandBody(){
    final StringBuilder sb = new StringBuilder();
    String line;
    while( (null != (line = peekLine())) ){
      checkForDirective(line);
      if( !isCommandLine(line, true) ){
        sb.append(line).append("\n");
        consumePeeked();
      }else{
        break;
      }
    }
    line = sb.toString();
    return line.trim().isEmpty() ? null : line;
  }

  private void processCommand(SQLTester t, String[] argv) throws Exception{
    verbose1("running command: ",argv[0], " ", Util.argvToString(argv));
    if(outer.getVerbosity()>1){
      final String input = t.getInputText();
      if( !input.isEmpty() ) verbose2("Input buffer = ",input);
    }
    CommandDispatcher2.dispatch(t, this, argv);
  }

  void toss(Object... msg) throws TestScriptFailed {
    StringBuilder sb = new StringBuilder();
    for(Object s : msg) sb.append(s);
    throw new TestScriptFailed(this, sb.toString());
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
