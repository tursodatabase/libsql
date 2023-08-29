/*
** 2023-08-29
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
** JS implementation of the SQLTester framework.
*/

// UNDER CONSTRUCTION. Still being ported from the Java impl.

import sqlite3ApiInit from '/jswasm/sqlite3.mjs';

const sqlite3 = await sqlite3ApiInit();

const log = (...args)=>{
  console.log('SQLTester:',...args);
};

// Return a new enum entry value
const newE = ()=>Object.create(null);

const newObj = (props)=>Object.assign(newE(), props);

/**
   Modes for how to escape (or not) column values and names from
   SQLTester.execSql() to the result buffer output.
*/
const ResultBufferMode = Object.assign(Object.create(null),{
  //! Do not append to result buffer
  NONE: newE(),
  //! Append output escaped.
  ESCAPED: newE(),
  //! Append output as-is
  ASIS: newE()
});

/**
   Modes to specify how to emit multi-row output from
   SQLTester.execSql() to the result buffer.
*/
const ResultRowMode = newObj({
  //! Keep all result rows on one line, space-separated.
  ONLINE: newE(),
  //! Add a newline between each result row.
  NEWLINE: newE()
});

class SQLTesterException extends globalThis.Error {
  constructor(...args){
    super(args.join(''));
  }
  isFatal() { return false; }
}

SQLTesterException.toss = (...args)=>{
  throw new SQLTesterException(...args);
}

class DbException extends SQLTesterException {
  constructor(...args){
    super(...args);
    //TODO...
    //const db = args[0];
    //if( db instanceof sqlite3.oo1.DB )
  }
  isFatal() { return true; }
}

class TestScriptFailed extends SQLTesterException {
  constructor(testScript, ...args){
    super(testScript.getPutputPrefix(),': ',...args);
  }
  isFatal() { return true; }
}

class UnknownCommand extends SQLTesterException {
  constructor(...args){
    super(...args);
  }
}

class IncompatibleDirective extends SQLTesterException {
  constructor(...args){
    super(...args);
  }
}

const toss = (errType, ...args)=>{
  if( !(errType instanceof SQLTesterException)){
    args.unshift(errType);
    errType = SQLTesterException;
  }
  throw new errType(...args);
};

const __utf8Decoder = new TextDecoder();
const __utf8Encoder = new TextEncoder('utf-8');
const __SAB = ('undefined'===typeof globalThis.SharedArrayBuffer)
      ? function(){} : globalThis.SharedArrayBuffer;

const Util = newObj({
  toss,

  unlink: function(fn){
    return 0==sqlite3.wasm.sqlite3_wasm_vfs_unlink(0,fn);
  },

  argvToString: (list)=>list.join(" "),

  utf8Decode: function(arrayBuffer, begin, end){
    return __utf8Decoder.decode(
      (arrayBuffer.buffer instanceof __SAB)
        ? arrayBuffer.slice(begin, end)
        : arrayBuffer.subarray(begin, end)
    );
  },

  utf8Encode: (str)=>__utf8Encoder.encode(str)
})/*Util*/;

class Outer {
  #lnBuf = [];
  #verbosity = 0;
  #logger = console.log.bind(console);

  constructor(){
  }

  out(...args){
    if(!this.#lnBuf.length && this.getOutputPrefix ){
      this.#lnBuf.push(this.getOutputPrefix());
    }
    this.#lnBuf.push(...args);
    return this;
  }
  outln(...args){
    if(!this.#lnBuf.length && this.getOutputPrefix ){
      this.#lnBuf.push(this.getOutputPrefix());
    }
    this.#lnBuf.push(...args,'\n');
    this.#logger(this.#lnBuf.join(''));
    this.#lnBuf.length = 0;
    return this;
  }

  setOutputPrefix( func ){
    this.getOutputPrefix = func;
    return this;
  }

  verboseN(lvl, argv){
    if( this.#verbosity>=lvl ){
      this.outln('VERBOSE ',lvl,': ',...argv);
    }
  }
  verbose1(...args){ return this.verboseN(1,args); }
  verbose2(...args){ return this.verboseN(2,args); }
  verbose3(...args){ return this.verboseN(3,args); }

  verbosity(){
    let rc;
    if(arguments.length){
      rc = this.#verbosity;
      this.#verbosity = arguments[0];
    }else{
      rc = this.#verbosity;
    }
    return rc;
  }

}/*Outer*/

class SQLTester {

  #outer = new Outer().setOutputPrefix( ()=>'SQLTester: ' );
  #aFiles = [];
  #inputBuffer = [];
  #resultBuffer = [];
  #nullView = "nil";
  #metrics = newObj({
    nTotalTest: 0, nTestFile: 0, nAbortedScript: 0
  });
  #emitColNames = false;
  #keepGoing = false;
  #aDb = [];
  #db = newObj({
    list: [],
    iCurrent: 0,
    initialDbName: "test.db",
  });

  constructor(){
  }

  appendInput(line, addNL){
    this.#inputBuffer.push(line);
    if( addNL ) this.#inputBuffer.push('\n');
  }
  appendResult(line, addNL){
    this.#resultBuffer.push(line);
    if( addNL ) this.#resultBuffer.push('\n');
  }

  clearInputBuffer(){
    this.#inputBuffer.length = 0;
    return this.#inputBuffer;
  }
  clearResultBuffer(){
    this.#resultBuffer.length = 0;
    return this.#resultBuffer;
  }

  getInputText(){ return this.#inputBuffer.join(''); }
  getResultText(){ return this.#resultBuffer.join(''); }

  verbosity(...args){ return this.#outer.verbosity(...args); }

}/*SQLTester*/

class Command {
  constructor(){
  }

  process(sqlTester,testScript,argv){
    SQLTesterException.toss("process() must be overridden");
  }

  argcCheck(testScript,argv,min,max){
    const argc = argv.length-1;
    if(argc<min || (max>=0 && argc>max)){
      if( min==max ){
        testScript.toss(argv[0]," requires exactly ",min," argument(s)");
      }else if(max>0){
        testScript.toss(argv[0]," requires ",min,"-",max," arguments.");
      }else{
        testScript.toss(argv[0]," requires at least ",min," arguments.");
      }
    }
  }
}

class TestCase extends Command {

  process(tester, script, argv){
    this.argcCheck(script, argv,1);
    script.testCaseName(argv[1]);
    tester.clearResultBuffer();
    tester.clearInputBuffer();
  }
}

class Cursor {
  src;
  sb = [];
  pos = 0;
  //! Current line number. Starts at 0 for internal reasons and will
  // line up with 1-based reality once parsing starts.
  lineNo = 0 /* yes, zero */;
  //! Putback value for this.pos.
  putbackPos = 0;
  //! Putback line number
  putbackLineNo = 0;
  //! Peeked-to pos, used by peekLine() and consumePeeked().
  peekedPos = 0;
  //! Peeked-to line number.
  peekedLineNo = 0;

  constructor(){
  }

  //! Restore parsing state to the start of the stream.
  rewind(){
    this.sb.length = this.pos = this.lineNo
      = this.putbackPos = this.putbackLineNo
      = this.peekedPos = this.peekedLineNo = 0;
  }
}

const Rx = newObj({
  requiredProperties: / REQUIRED_PROPERTIES:[ \t]*(\S.*)\s*$/,
  scriptModuleName: / SCRIPT_MODULE_NAME:[ \t]*(\S+)\s*$/,
  mixedModuleName: / ((MIXED_)?MODULE_NAME):[ \t]*(\S+)\s*$/,
  command: /^--(([a-z-]+)( .*)?)$/
});

class TestScript {
  #cursor = new Cursor();
  #moduleName = null;
  #filename = null;
  #testCaseName = null;
  #outer = new Outer().setOutputPrefix( ()=>this.getOutputPrefix() );

  constructor(...args){
    let content, filename;
    if( 2 == args.length ){
      filename = args[0];
      content = args[1];
    }else{
      content = args[0];
    }
    this.#filename = filename;
    this.#cursor.src = content;
    this.#outer.outputPrefix = ()=>this.getOutputPrefix();
  }

  testCaseName(){
    return (0==arguments.length)
      ? this.#testCaseName : (this.#testCaseName = arguments[0]);
  }

  getOutputPrefix() {
    let rc =  "["+(this.#moduleName || this.#filename)+"]";
    if( this.#testCaseName ) rc += "["+this.#testCaseName+"]";
    return rc + " line "+ this.#cursor.lineNo +" ";
  }

  reset(){
    this.#testCaseName = null;
    this.#cursor.rewind();
    return this;
  }

  toss(...args){
    throw new TestScriptFailed(this,...args);
  }

  #checkForDirective(tester,line){
    //todo
  }

  #getCommandArgv(line){
    const m = Rx.command.exec(line);
    return m ? m[1].trim().split(/\s+/) : null;
  }

  run(tester){
    this.reset();
    this.#outer.verbosity(tester.verbosity());
    let line, directive, argv = [];
    while( null != (line = this.getLine()) ){
      this.verbose3("input line: ",line);
      this.#checkForDirective(tester, line);
      argv = this.#getCommandArgv(line);
      if( argv ){
        this.#processCommand(tester, argv);
        continue;
      }
      tester.appendInput(line,true);
    }
    return true;
  }

  #processCommand(tester, argv){
    this.verbose1("running command: ",argv[0], " ", Util.argvToString(argv));
    if(this.#outer.verbosity()>1){
      const input = tester.getInputText();
      if( !!input ) this.verbose3("Input buffer = ",input);
    }
    CommandDispatcher.dispatch(tester, this, argv);
  }

  getLine(){
    const cur = this.#cursor;
    if( cur.pos==cur.src.byteLength ){
      return null/*EOF*/;
    }
    cur.putbackPos = cur.pos;
    cur.putbackLineNo = cur.lineNo;
    cur.sb.length = 0;
    let b = 0, prevB = 0, i = cur.pos;
    let doBreak = false;
    let nChar = 0 /* number of bytes in the aChar char */;
    const end = cur.src.byteLength;
    for(; i < end && !doBreak; ++i){
      b = cur.src[i];
      switch( b ){
        case 13/*CR*/: continue;
        case 10/*NL*/:
          ++cur.lineNo;
          if(cur.sb.length>0) doBreak = true;
          // Else it's an empty string
          break;
        default:{
          /* Multi-byte chars need to be gathered up and appended at
             one time so that we can get them as string objects. */
          nChar = 1;
          switch( b & 0xF0 ){
            case 0xC0: nChar = 2; break;
            case 0xE0: nChar = 3; break;
            case 0xF0: nChar = 4; break;
            default:
              if( b > 127 ) this.toss("Invalid character (#"+b+").");
              break;
          }
          if( 1==nChar ){
            cur.sb.push(String.fromCharCode(b));
          }else{
            const aChar = [] /* multi-byte char buffer */;
            for(let x = 0; (x < nChar) && (i+x < end); ++x) aChar[x] = cur.src[i+x];
            cur.sb.push(
              Util.utf8Decode( new Uint8Array(aChar) )
            );
            i += nChar-1;
          }
          break;
        }
      }
    }
    cur.pos = i;
    const rv = cur.sb.join('');
    if( i==cur.src.byteLength && 0==rv.length ){
      return null /* EOF */;
    }
    return rv;
  }/*getLine()*/

  /**
     Fetches the next line then resets the cursor to its pre-call
     state. consumePeeked() can be used to consume this peeked line
     without having to re-parse it.
  */
  peekLine(){
    const cur = this.#cursor;
    const oldPos = cur.pos;
    const oldPB = cur.putbackPos;
    const oldPBL = cur.putbackLineNo;
    const oldLine = cur.lineNo;
    const rc = this.getLine();
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
  consumePeeked(){
    const cur = this.#cursor;
    cur.pos = cur.peekedPos;
    cur.lineNo = cur.peekedLineNo;
  }

  /**
     Restores the cursor to the position it had before the previous
     call to getLine().
  */
  putbackLine(){
    const cur = this.#cursor;
    cur.pos = cur.putbackPos;
    cur.lineNo = cur.putbackLineNo;
  }

  verbose1(...args){ return this.#outer.verboseN(1,args); }
  verbose2(...args){ return this.#outer.verboseN(2,args); }
  verbose3(...args){ return this.#outer.verboseN(3,args); }
  verbosity(...args){ return this.#outer.verbosity(...args); }

}/*TestScript*/;

class CommandDispatcher {
  static map = newObj();

  static getCommandByName(name){
    let rv = CommandDispatcher.map[name];
    if( rv ) return rv;
    switch(name){
        //todo: map name to Command instance
      case "testcase": rv = new TestCase(); break;
    }
    if( rv ){
      CommandDispatcher.map[name] = rv;
    }
    return rv;
  }

  static dispatch(tester, testScript, argv){
    const cmd = CommandDispatcher.getCommandByName(argv[0]);
    if( !cmd ){
      toss(UnknownCommand,argv[0],' ',testScript.getOutputPrefix());
    }
    cmd.process(tester, testScript, argv);
  }
}/*CommandDispatcher*/

const namespace = newObj({
  Command,
  DbException,
  IncompatibleDirective,
  Outer,
  SQLTester,
  SQLTesterException,
  TestScript,
  TestScriptFailed,
  UnknownCommand,
  Util
});

export {namespace as default};
