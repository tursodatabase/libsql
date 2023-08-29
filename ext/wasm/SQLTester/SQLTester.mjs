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
    super(args.join(' '));
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
  constructor(...args){
    super(...args);
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

  out(...args){
    this.#lnBuf.append(...args);
    return this;
  }
  outln(...args){
    this.#lnBuf.append(...args,'\n');
    this.logger(this.#lnBuf.join(''));
    this.#lnBuf.length = 0;
    return this;
  }

  #verboseN(lvl, argv){
    if( this.#verbosity>=lvl ){
      const pre = this.getOutputPrefix ? this.getOutputPrefix() : '';
      this.outln('VERBOSE ',lvl,' ',pre,': ',...argv);
    }
  }
  verbose1(...args){ return this.#verboseN(1,args); }
  verbose2(...args){ return this.#verboseN(2,args); }
  verbose3(...args){ return this.#verboseN(3,args); }

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
  SQLTester(){}

  #aFiles = [];
  #inputBuffer = [];
  #outputBuffer = [];
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

}/*SQLTester*/

class Command {
  Command(){
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

class Cursor {
  src;
  buffer = [];
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

  //! Restore parsing state to the start of the stream.
  rewind(){
    this.buffer.length = 0;
    this.pos = this.lineNo = this.putbackPos =
      this.putbackLineNo = this.peekedPos = this.peekedLineNo = 0;
  }
}

class TestScript {
  #cursor = new Cursor();
  #verbosity = 0;
  #moduleName = null;
  #filename = null;
  #testCaseName = null;
  #outer = new Outer();
  #verboseN(lvl, argv){
    if( this.#verbosity>=lvl ){
      this.outln('VERBOSE ',lvl,': ',...argv);
    }
  }

  verbose1(...args){ return this.#verboseN(1,args); }
  verbose2(...args){ return this.#verboseN(2,args); }
  verbose3(...args){ return this.#verboseN(3,args); }

  TestScript(content){
    this.cursor.src = content;
    this.outer.outputPrefix = ()=>this.getOutputPrefix();
  }

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

  getOutputPrefix() {
    const rc =  "["+(this.moduleName || this.filename)+"]";
    if( this.testCaseName ) rc += "["+this.testCaseName+"]";
    return rc + " line "+ this.cur.lineNo;
  }

  toss(...args){
    Util.toss(this.getOutputPrefix()+":",TestScriptFailed,...args)
  }

}/*TestScript*/;


const namespace = newObj({
  SQLTester: new SQLTester(),
  DbException,
  IncompatibleDirective,
  SQLTesterException,
  TestScriptFailed,
  UnknownCommand
});


export {namespace as default};
