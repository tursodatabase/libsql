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
** This file contains the main application entry pointer for the JS
** implementation of the SQLTester framework.
**
** This version is not well-documented because it's a direct port of
** the Java implementation, which is documented: in the main SQLite3
** source tree, see ext/jni/src/org/sqlite/jni/capi/SQLTester.java.
*/

import sqlite3ApiInit from '/jswasm/sqlite3.mjs';

const sqlite3 = await sqlite3ApiInit();

const log = (...args)=>{
  console.log('SQLTester:',...args);
};

/**
   Try to install vfsName as the new default VFS. Once this succeeds
   (returns true) then it becomes a no-op on future calls. Throws if
   VFS registration as the default VFS fails but has no side effects
   if vfsName is not currently registered.
*/
const tryInstallVfs = function f(vfsName){
  if(f.vfsName) return false;
  const pVfs = sqlite3.capi.sqlite3_vfs_find(vfsName);
  if(pVfs){
    log("Installing",'"'+vfsName+'"',"as default VFS.");
    const rc = sqlite3.capi.sqlite3_vfs_register(pVfs, 1);
    if(rc){
      sqlite3.SQLite3Error.toss(rc,"While trying to register",vfsName,"vfs.");
    }
    f.vfsName = vfsName;
  }
  return !!pVfs;
};
tryInstallVfs.vfsName = undefined;

if( 0 && globalThis.WorkerGlobalScope ){
  // Try OPFS storage, if available...
  if( 1 && sqlite3.oo1.OpfsDb ){
    /* Really slow with these tests */
    tryInstallVfs("opfs");
  }else if( sqlite3.installOpfsSAHPoolVfs ){
    await sqlite3.installOpfsSAHPoolVfs({
      clearOnInit: true,
      initialCapacity: 15,
      name: 'opfs-SQLTester'
    }).then(pool=>{
      tryInstallVfs(pool.vfsName);
    }).catch(e=>{
      log("OpfsSAHPool could not load:",e);
    });
  }
}
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
  constructor(testScript, ...args){
    if(testScript){
      super( [testScript.getOutputPrefix()+": ", ...args].join('') );
    }else{
      super( args.join('') );
    }
    this.name = 'SQLTesterException';
  }
  isFatal() { return false; }
}

SQLTesterException.toss = (...args)=>{
  throw new SQLTesterException(...args);
}

class DbException extends SQLTesterException {
  constructor(testScript, pDb, rc, closeDb=false){
    super(testScript, "DB error #"+rc+": "+sqlite3.capi.sqlite3_errmsg(pDb));
    this.name = 'DbException';
    if( closeDb ) sqlite3.capi.sqlite3_close_v2(pDb);
  }
  isFatal() { return true; }
}

class TestScriptFailed extends SQLTesterException {
  constructor(testScript, ...args){
    super(testScript,...args);
    this.name = 'TestScriptFailed';
  }
  isFatal() { return true; }
}

class UnknownCommand extends SQLTesterException {
  constructor(testScript, cmdName){
    super(testScript, cmdName);
    this.name = 'UnknownCommand';
  }
  isFatal() { return true; }
}

class IncompatibleDirective extends SQLTesterException {
  constructor(testScript, ...args){
    super(testScript,...args);
    this.name = 'IncompatibleDirective';
  }
}

//! For throwing where an expression is required.
const toss = (errType, ...args)=>{
  throw new errType(...args);
};

const __utf8Decoder = new TextDecoder();
const __utf8Encoder = new TextEncoder('utf-8');
//! Workaround for Util.utf8Decode()
const __SAB = ('undefined'===typeof globalThis.SharedArrayBuffer)
      ? function(){} : globalThis.SharedArrayBuffer;


/* Frequently-reused regexes. */
const Rx = newObj({
  requiredProperties: / REQUIRED_PROPERTIES:[ \t]*(\S.*)\s*$/,
  scriptModuleName: / SCRIPT_MODULE_NAME:[ \t]*(\S+)\s*$/,
  mixedModuleName: / ((MIXED_)?MODULE_NAME):[ \t]*(\S+)\s*$/,
  command: /^--(([a-z-]+)( .*)?)$/,
  //! "Special" characters - we have to escape output if it contains any.
  special: /[\x00-\x20\x22\x5c\x7b\x7d]/,
  squiggly: /[{}]/
});



const Util = newObj({
  toss,

  unlink: function f(fn){
    if(!f.unlink){
      f.unlink = sqlite3.wasm.xWrap('sqlite3__wasm_vfs_unlink','int',
                                    ['*','string']);
    }
    return 0==f.unlink(0,fn);
  },

  argvToString: (list)=>{
    const m = [...list];
    m.shift() /* strip command name */;
    return m.join(" ")
  },

  utf8Decode: function(arrayBuffer, begin, end){
    return __utf8Decoder.decode(
      (arrayBuffer.buffer instanceof __SAB)
        ? arrayBuffer.slice(begin, end)
        : arrayBuffer.subarray(begin, end)
    );
  },

  utf8Encode: (str)=>__utf8Encoder.encode(str),

  strglob: sqlite3.wasm.xWrap('sqlite3__wasm_SQLTester_strglob','int',
                              ['string','string'])
})/*Util*/;

/**
   Output logger utility.
*/
class Outer {
  #lnBuf = [];
  #verbosity = 0;
  #logger = console.log.bind(console);

  constructor(func){
    if(func) this.setFunc(func);
  }

  logger(...args){
    if(args.length){
      this.#logger = args[0];
      return this;
    }
    return this.#logger;
  }

  out(...args){
    if( this.getOutputPrefix && !this.#lnBuf.length ){
      this.#lnBuf.push(this.getOutputPrefix());
    }
    this.#lnBuf.push(...args);
    return this;
  }

  #outlnImpl(vLevel, ...args){
    if( this.getOutputPrefix && !this.#lnBuf.length ){
      this.#lnBuf.push(this.getOutputPrefix());
    }
    this.#lnBuf.push(...args,'\n');
    const msg = this.#lnBuf.join('');
    this.#lnBuf.length = 0;
    this.#logger(msg);
    return this;
  }

  outln(...args){
    return this.#outlnImpl(0,...args);
  }

  outputPrefix(){
    if( 0==arguments.length ){
      return (this.getOutputPrefix
              ? (this.getOutputPrefix() ?? '') : '');
    }else{
      this.getOutputPrefix = arguments[0];
      return this;
    }
  }

  static #verboseLabel = ["🔈",/*"🔉",*/"🔊","📢"];
  verboseN(lvl, args){
    if( this.#verbosity>=lvl ){
      this.#outlnImpl(lvl, Outer.#verboseLabel[lvl-1],': ',...args);
    }
  }
  verbose1(...args){ return this.verboseN(1,args); }
  verbose2(...args){ return this.verboseN(2,args); }
  verbose3(...args){ return this.verboseN(3,args); }

  verbosity(){
    const rc = this.#verbosity;
    if(arguments.length) this.#verbosity = +arguments[0];
    return rc;
  }

}/*Outer*/

class SQLTester {

  //! Console output utility.
  #outer = new Outer().outputPrefix( ()=>'SQLTester: ' );
  //! List of input scripts.
  #aScripts = [];
  //! Test input buffer.
  #inputBuffer = [];
  //! Test result buffer.
  #resultBuffer = [];
  //! Output representation of SQL NULL.
  #nullView;
  metrics = newObj({
    //! Total tests run
    nTotalTest: 0,
    //! Total test script files run
    nTestFile: 0,
    //! Test-case count for to the current TestScript
    nTest: 0,
    //! Names of scripts which were aborted.
    failedScripts: []
  });
  #emitColNames = false;
  //! True to keep going regardless of how a test fails.
  #keepGoing = false;
  #db = newObj({
    //! The list of available db handles.
    list: new Array(7),
    //! Index into this.list of the current db.
    iCurrentDb: 0,
    //! Name of the default db, re-created for each script.
    initialDbName: "test.db",
    //! Buffer for REQUIRED_PROPERTIES pragmas.
    initSql: ['select 1;'],
    //! (sqlite3*) to the current db.
    currentDb: function(){
      return this.list[this.iCurrentDb];
    }
  });

  constructor(){
    this.reset();
  }

  outln(...args){ return this.#outer.outln(...args); }
  out(...args){ return this.#outer.out(...args); }
  outer(...args){
    if(args.length){
      this.#outer = args[0];
      return this;
    }
    return this.#outer;
  }
  verbose1(...args){ return this.#outer.verboseN(1,args); }
  verbose2(...args){ return this.#outer.verboseN(2,args); }
  verbose3(...args){ return this.#outer.verboseN(3,args); }
  verbosity(...args){
    const rc = this.#outer.verbosity(...args);
    return args.length ? this : rc;
  }
  setLogger(func){
    this.#outer.logger(func);
    return this;
  }

  incrementTestCounter(){
    ++this.metrics.nTotalTest;
    ++this.metrics.nTest;
  }

  reset(){
    this.clearInputBuffer();
    this.clearResultBuffer();
    this.#clearBuffer(this.#db.initSql);
    this.closeAllDbs();
    this.metrics.nTest = 0;
    this.#nullView = "nil";
    this.#emitColNames = false;
    this.#db.iCurrentDb = 0;
    //this.#db.initSql.push("SELECT 1;");
  }

  appendInput(line, addNL){
    this.#inputBuffer.push(line);
    if( addNL ) this.#inputBuffer.push('\n');
  }
  appendResult(line, addNL){
    this.#resultBuffer.push(line);
    if( addNL ) this.#resultBuffer.push('\n');
  }
  appendDbInitSql(sql){
    this.#db.initSql.push(sql);
    if( this.currentDb() ){
      this.execSql(null, true, ResultBufferMode.NONE, null, sql);
    }
  }

  #runInitSql(pDb){
    let rc = 0;
    for(const sql of this.#db.initSql){
      this.#outer.verbose2("RUNNING DB INIT CODE: ",sql);
      rc = this.execSql(pDb, false, ResultBufferMode.NONE, null, sql);
      if( rc ) break;
    }
    return rc;
  }

#clearBuffer(buffer){
    buffer.length = 0;
    return buffer;
  }

  clearInputBuffer(){ return this.#clearBuffer(this.#inputBuffer); }
  clearResultBuffer(){return this.#clearBuffer(this.#resultBuffer); }

  getInputText(){ return this.#inputBuffer.join(''); }
  getResultText(){ return this.#resultBuffer.join(''); }

  #takeBuffer(buffer){
    const s = buffer.join('');
    buffer.length = 0;
    return s;
  }

  takeInputBuffer(){
    return this.#takeBuffer(this.#inputBuffer);
  }
  takeResultBuffer(){
    return this.#takeBuffer(this.#resultBuffer);
  }

  nullValue(){
    return (0==arguments.length)
      ? this.#nullView
      : (this.#nullView = ''+arguments[0]);
  }

  outputColumnNames(){
    return (0==arguments.length)
      ? this.#emitColNames
      : (this.#emitColNames = !!arguments[0]);
  }

  currentDbId(){
    return (0==arguments.length)
      ? this.#db.iCurrentDb
      : (this.#affirmDbId(arguments[0]).#db.iCurrentDb = arguments[0]);
  }

  #affirmDbId(id){
    if(id<0 || id>=this.#db.list.length){
      toss(SQLTesterException, "Database index ",id," is out of range.");
    }
    return this;
  }

  currentDb(...args){
    if( 0!=args.length ){
      this.#affirmDbId(id).#db.iCurrentDb = id;
    }
    return this.#db.currentDb();
  }

  getDbById(id){
    return this.#affirmDbId(id).#db.list[id];
  }

  getCurrentDb(){ return this.#db.list[this.#db.iCurrentDb]; }


  closeDb(id) {
    if( 0==arguments.length ){
      id = this.#db.iCurrentDb;
    }
    const pDb = this.#affirmDbId(id).#db.list[id];
    if( pDb ){
      sqlite3.capi.sqlite3_close_v2(pDb);
      this.#db.list[id] = null;
    }
  }

  closeAllDbs(){
    for(let i = 0; i<this.#db.list.length; ++i){
      if(this.#db.list[i]){
        sqlite3.capi.sqlite3_close_v2(this.#db.list[i]);
        this.#db.list[i] = null;
      }
    }
    this.#db.iCurrentDb = 0;
  }

  openDb(name, createIfNeeded){
    if( 3===arguments.length ){
      const slot = arguments[0];
      this.#affirmDbId(slot).#db.iCurrentDb = slot;
      name = arguments[1];
      createIfNeeded = arguments[2];
    }
    this.closeDb();
    const capi = sqlite3.capi, wasm = sqlite3.wasm;
    let pDb = 0;
    let flags = capi.SQLITE_OPEN_READWRITE;
    if( createIfNeeded ) flags |= capi.SQLITE_OPEN_CREATE;
    try{
      let rc;
      wasm.pstack.call(function(){
        let ppOut = wasm.pstack.allocPtr();
        rc = sqlite3.capi.sqlite3_open_v2(name, ppOut, flags, null);
        pDb = wasm.peekPtr(ppOut);
      });
      let sql;
      if( 0==rc && this.#db.initSql.length > 0){
        rc = this.#runInitSql(pDb);
      }
      if( 0!=rc ){
        sqlite3.SQLite3Error.toss(
          rc,
          "sqlite3 result code",rc+":",
          (pDb ? sqlite3.capi.sqlite3_errmsg(pDb)
           : sqlite3.capi.sqlite3_errstr(rc))
        );
      }
      return this.#db.list[this.#db.iCurrentDb] = pDb;
    }catch(e){
      sqlite3.capi.sqlite3_close_v2(pDb);
      throw e;
    }
  }

  addTestScript(ts){
    if( 2===arguments.length ){
      ts = new TestScript(arguments[0], arguments[1]);
    }else if(ts instanceof Uint8Array){
      ts = new TestScript('<unnamed>', ts);
    }else if('string' === typeof arguments[1]){
      ts = new TestScript('<unnamed>', Util.utf8Encode(arguments[1]));
    }
    if( !(ts instanceof TestScript) ){
      Util.toss(SQLTesterException, "Invalid argument type for addTestScript()");
    }
    this.#aScripts.push(ts);
    return this;
  }

  runTests(){
    this.outln("SQLite version ", sqlite3.capi.sqlite3_libversion()," with ",
               sqlite3.wasm.ptr.size, "-byte WASM pointers");
    const tStart = (new Date()).getTime();
    let isVerbose = this.verbosity();
    this.metrics.failedScripts.length = 0;
    this.metrics.nTotalTest = 0;
    this.metrics.nTestFile = 0;
    for(const ts of this.#aScripts){
      this.reset();
      ++this.metrics.nTestFile;
      let threw = false;
      const timeStart = (new Date()).getTime();
      let msgTail = '';
      try{
        ts.run(this);
      }catch(e){
        if(e instanceof SQLTesterException){
          threw = true;
          this.outln("🔥EXCEPTION: ",e);
          this.metrics.failedScripts.push({script: ts.filename(), message:e.toString()});
          if( this.#keepGoing ){
            this.outln("Continuing anyway because of the keep-going option.");
          }else if( e.isFatal() ){
            throw e;
          }
        }else{
          throw e;
        }
      }finally{
        const timeEnd = (new Date()).getTime();
        this.out("🏁", (threw ? "❌" : "✅"), " ",
                 this.metrics.nTest, " test(s) in ",
                 (timeEnd-timeStart),"ms. ");
        const mod = ts.moduleName();
        if( mod ){
          this.out( "[",mod,"] " );
        }
        this.outln(ts.filename());
      }
    }
    const tEnd = (new Date()).getTime();
    Util.unlink(this.#db.initialDbName);
    this.outln("Took ",(tEnd-tStart),"ms. Test count = ",
               this.metrics.nTotalTest,", script count = ",
               this.#aScripts.length,(
                 this.metrics.failedScripts.length
                   ? ", failed scripts = "+this.metrics.failedScripts.length
                   : ""
               )
              );
    return this;
  }

  #setupInitialDb(){
    if( !this.#db.list[0] ){
      Util.unlink(this.#db.initialDbName);
      this.openDb(0, this.#db.initialDbName, true);
    }else{
      this.#outer.outln("WARNING: setupInitialDb() was unexpectedly ",
                        "triggered while it is opened.");
    }
  }

  #escapeSqlValue(v){
    if( !v ) return "{}";
    if( !Rx.special.test(v) ){
      return v  /* no escaping needed */;
    }
    if( !Rx.squiggly.test(v) ){
      return "{"+v+"}";
    }
    const sb = ["\""];
    const n = v.length;
    for(let i = 0; i < n; ++i){
      const ch = v.charAt(i);
      switch(ch){
        case '\\': sb.push("\\\\"); break;
        case '"': sb.push("\\\""); break;
        default:{
          //verbose("CHAR ",(int)ch," ",ch," octal=",String.format("\\%03o", (int)ch));
          const ccode = ch.charCodeAt(i);
          if( ccode < 32 ) sb.push('\\',ccode.toString(8),'o');
          else sb.push(ch);
          break;
        }
      }
    }
    sb.push("\"");
    return sb.join('');
  }

  #appendDbErr(pDb, sb, rc){
    sb.push(sqlite3.capi.sqlite3_js_rc_str(rc), ' ');
    const msg = this.#escapeSqlValue(sqlite3.capi.sqlite3_errmsg(pDb));
    if( '{' === msg.charAt(0) ){
      sb.push(msg);
    }else{
      sb.push('{', msg, '}');
    }
  }

  #checkDbRc(pDb,rc){
    sqlite3.oo1.DB.checkRc(pDb, rc);
  }

  execSql(pDb, throwOnError, appendMode, rowMode, sql){
    if( !pDb && !this.#db.list[0] ){
      this.#setupInitialDb();
    }
    if( !pDb ) pDb = this.#db.currentDb();
    const wasm = sqlite3.wasm, capi = sqlite3.capi;
    sql = (sql instanceof Uint8Array)
      ? sql
      : Util.utf8Encode(capi.sqlite3_js_sql_to_string(sql));
    const self = this;
    const sb = (ResultBufferMode.NONE===appendMode) ? null : this.#resultBuffer;
    let rc = 0;
    wasm.scopedAllocCall(function(){
      let sqlByteLen = Number(sql.byteLength);
      const ppStmt = wasm.scopedAlloc(
        (2 * wasm.ptr.size) /* output (sqlite3_stmt**) arg and pzTail */
        + (sqlByteLen + 1/* SQL + NUL */)
      );
      const pzTail = wasm.ptr.add(ppStmt, wasm.ptr.size) /* final arg to sqlite3_prepare_v2() */;
      let pSql = wasm.ptr.add(pzTail, wasm.ptr.size);
      const pSqlEnd = wasm.ptr.add(pSql, sqlByteLen);
      wasm.heap8().set(sql, Number(pSql));
      wasm.poke8(pSqlEnd, 0/*NUL terminator*/);
      let pos = 0, n = 1, spacing = 0;
      while( pSql && wasm.peek8(pSql) ){
        wasm.pokePtr([ppStmt, pzTail], 0);
        rc = capi.sqlite3_prepare_v3(
          pDb, pSql, sqlByteLen, 0, ppStmt, pzTail
        );
        if( 0!==rc ){
          if(throwOnError){
            throw new DbException(self, pDb, rc);
          }else if( sb ){
            self.#appendDbErr(pDb, sb, rc);
          }
          break;
        }
        const pStmt = wasm.peekPtr(ppStmt);
        pSql = wasm.peekPtr(pzTail);
        sqlByteLen = Number(pSqlEnd - pSql);
        if(!pStmt) continue /* only whitespace or comments */;
        if( sb ){
          const nCol = capi.sqlite3_column_count(pStmt);
          let colName, val;
          while( capi.SQLITE_ROW === (rc = capi.sqlite3_step(pStmt)) ) {
            for( let i=0; i < nCol; ++i ){
              if( spacing++ > 0 ) sb.push(' ');
              if( self.#emitColNames ){
                colName = capi.sqlite3_column_name(pStmt, i);
                switch(appendMode){
                  case ResultBufferMode.ASIS: sb.push( colName ); break;
                  case ResultBufferMode.ESCAPED:
                    sb.push( self.#escapeSqlValue(colName) );
                    break;
                  default:
                    self.toss("Unhandled ResultBufferMode.");
                }
                sb.push(' ');
              }
              val = capi.sqlite3_column_text(pStmt, i);
              if( null===val ){
                sb.push( self.#nullView );
                continue;
              }
              switch(appendMode){
                case ResultBufferMode.ASIS: sb.push( val ); break;
                case ResultBufferMode.ESCAPED:
                  sb.push( self.#escapeSqlValue(val) );
                  break;
              }
            }/* column loop */
            if( ResultRowMode.NEWLINE === rowMode ){
              spacing = 0;
              sb.push('\n');
            }
          }/* row loop */
        }else{ // no output but possibly other side effects
          while( capi.SQLITE_ROW === (rc = capi.sqlite3_step(pStmt)) ) {}
        }
        capi.sqlite3_finalize(pStmt);
        if( capi.SQLITE_ROW===rc || capi.SQLITE_DONE===rc) rc = 0;
        else if( rc!=0 ){
          if( sb ){
            self.#appendDbErr(pDb, sb, rc);
          }
          break;
        }
      }/* SQL script loop */;
    })/*scopedAllocCall()*/;
    return rc;
  }

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

class TestScript {
  #cursor = new Cursor();
  #moduleName = null;
  #filename = null;
  #testCaseName = null;
  #outer = new Outer().outputPrefix( ()=>this.getOutputPrefix()+': ' );

  constructor(...args){
    let content, filename;
    if( 2 == args.length ){
      filename = args[0];
      content = args[1];
    }else if( 1 == args.length ){
      if(args[0] instanceof Object){
        const o = args[0];
        filename = o.name;
        content = o.content;
      }else{
        content = args[0];
      }
    }
    if(!(content instanceof Uint8Array)){
      if('string' === typeof content){
        content = Util.utf8Encode(content);
      }else if((content instanceof ArrayBuffer)
               ||(content instanceof Array)){
        content = new Uint8Array(content);
      }else{
        toss(Error, "Invalid content type for TestScript constructor.");
      }
    }
    this.#filename = filename;
    this.#cursor.src = content;
  }

  moduleName(){
    return (0==arguments.length)
      ? this.#moduleName : (this.#moduleName = arguments[0]);
  }

  testCaseName(){
    return (0==arguments.length)
      ? this.#testCaseName : (this.#testCaseName = arguments[0]);
  }
  filename(){
    return (0==arguments.length)
      ? this.#filename : (this.#filename = arguments[0]);
  }

  getOutputPrefix() {
    let rc =  "["+(this.#moduleName || '<unnamed>')+"]";
    if( this.#testCaseName ) rc += "["+this.#testCaseName+"]";
    if( this.#filename ) rc += '['+this.#filename+']';
    return rc + " line "+ this.#cursor.lineNo;
  }

  reset(){
    this.#testCaseName = null;
    this.#cursor.rewind();
    return this;
  }

  toss(...args){
    throw new TestScriptFailed(this,...args);
  }

  verbose1(...args){ return this.#outer.verboseN(1,args); }
  verbose2(...args){ return this.#outer.verboseN(2,args); }
  verbose3(...args){ return this.#outer.verboseN(3,args); }
  verbosity(...args){
    const rc = this.#outer.verbosity(...args);
    return args.length ? this : rc;
  }

  #checkRequiredProperties(tester, props){
    if(true) return false;
    let nOk = 0;
    for(const rp of props){
      this.verbose2("REQUIRED_PROPERTIES: ",rp);
      switch(rp){
        case "RECURSIVE_TRIGGERS":
          tester.appendDbInitSql("pragma recursive_triggers=on;");
          ++nOk;
          break;
        case "TEMPSTORE_FILE":
          /* This _assumes_ that the lib is built with SQLITE_TEMP_STORE=1 or 2,
             which we just happen to know is the case */
          tester.appendDbInitSql("pragma temp_store=1;");
          ++nOk;
          break;
        case "TEMPSTORE_MEM":
          /* This _assumes_ that the lib is built with SQLITE_TEMP_STORE=1 or 2,
             which we just happen to know is the case */
          tester.appendDbInitSql("pragma temp_store=0;");
          ++nOk;
          break;
        case "AUTOVACUUM":
          tester.appendDbInitSql("pragma auto_vacuum=full;");
          ++nOk;
          break;
        case "INCRVACUUM":
          tester.appendDbInitSql("pragma auto_vacuum=incremental;");
          ++nOk;
        default:
          break;
      }
    }
    return props.length == nOk;
  }

  #checkForDirective(tester,line){
    if(line.startsWith("#")){
      throw new IncompatibleDirective(this, "C-preprocessor input: "+line);
    }else if(line.startsWith("---")){
      throw new IncompatibleDirective(this, "triple-dash: ",line);
    }
    let m = Rx.scriptModuleName.exec(line);
    if( m ){
      this.#moduleName = m[1];
      return;
    }
    m = Rx.requiredProperties.exec(line);
    if( m ){
      const rp = m[1];
      if( !this.#checkRequiredProperties( tester, rp.split(/\s+/).filter(v=>!!v) ) ){
        throw new IncompatibleDirective(this, "REQUIRED_PROPERTIES: "+rp);
      }
    }

    m = Rx.mixedModuleName.exec(line);
    if( m ){
      throw new IncompatibleDirective(this, m[1]+": "+m[3]);
    }
    if( line.indexOf("\n|")>=0 ){
      throw new IncompatibleDirective(this, "newline-pipe combination.");
    }

  }

  #getCommandArgv(line){
    const m = Rx.command.exec(line);
    return m ? m[1].trim().split(/\s+/) : null;
  }


  #isCommandLine(line, checkForImpl){
    let m = Rx.command.exec(line);
    if( m && checkForImpl ){
      m = !!CommandDispatcher.getCommandByName(m[2]);
    }
    return !!m;
  }

  fetchCommandBody(tester){
    const sb = [];
    let line;
    while( (null !== (line = this.peekLine())) ){
      this.#checkForDirective(tester, line);
      if( this.#isCommandLine(line, true) ) break;
      sb.push(line,"\n");
      this.consumePeeked();
    }
    line = sb.join('');
    return !!line.trim() ? line : null;
  }

  run(tester){
    this.reset();
    this.#outer.verbosity( tester.verbosity() );
    this.#outer.logger( tester.outer().logger() );
    let line, directive, argv = [];
    while( null != (line = this.getLine()) ){
      this.verbose3("run() input line: ",line);
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
    this.verbose2("processCommand(): ",argv[0], " ", Util.argvToString(argv));
    if(this.#outer.verbosity()>1){
      const input = tester.getInputText();
      this.verbose3("processCommand() input buffer = ",input);
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
    try {
      return this.getLine();
    }finally{
      cur.peekedPos = cur.pos;
      cur.peekedLineNo = cur.lineNo;
      cur.pos = oldPos;
      cur.lineNo = oldLine;
      cur.putbackPos = oldPB;
      cur.putbackLineNo = oldPBL;
    }
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

}/*TestScript*/;

//! --close command
class CloseDbCommand extends Command {
  process(t, ts, argv){
    this.argcCheck(ts,argv,0,1);
    let id;
    if(argv.length>1){
      const arg = argv[1];
      if( "all" === arg ){
        t.closeAllDbs();
        return;
      }
      else{
        id = parseInt(arg);
      }
    }else{
      id = t.currentDbId();
    }
    t.closeDb(id);
  }
}

//! --column-names command
class ColumnNamesCommand extends Command {
  process( st, ts, argv ){
    this.argcCheck(ts,argv,1);
    st.outputColumnNames( !!parseInt(argv[1]) );
  }
}

//! --db command
class DbCommand extends Command {
  process(t, ts, argv){
    this.argcCheck(ts,argv,1);
    t.currentDbId( parseInt(argv[1]) );
  }
}

//! --glob command
class GlobCommand extends Command {
  #negate = false;
  constructor(negate=false){
    super();
    this.#negate = negate;
  }

  process(t, ts, argv){
    this.argcCheck(ts,argv,1,-1);
    t.incrementTestCounter();
    const sql = t.takeInputBuffer();
    let rc = t.execSql(null, true, ResultBufferMode.ESCAPED,
                       ResultRowMode.ONELINE, sql);
    const result = t.getResultText();
    const sArgs = Util.argvToString(argv);
    //t2.verbose2(argv[0]," rc = ",rc," result buffer:\n", result,"\nargs:\n",sArgs);
    const glob = Util.argvToString(argv);
    rc = Util.strglob(glob, result);
    if( (this.#negate && 0===rc) || (!this.#negate && 0!==rc) ){
      ts.toss(argv[0], " mismatch: ", glob," vs input: ",result);
    }
  }
}

//! --notglob command
class NotGlobCommand extends GlobCommand {
  constructor(){super(true);}
}

//! --open command
class OpenDbCommand extends Command {
  #createIfNeeded = false;
  constructor(createIfNeeded=false){
    super();
    this.#createIfNeeded = createIfNeeded;
  }
  process(t, ts, argv){
    this.argcCheck(ts,argv,1);
    t.openDb(argv[1], this.#createIfNeeded);
  }
}

//! --new command
class NewDbCommand extends OpenDbCommand {
  constructor(){ super(true); }
}

//! Placeholder dummy/no-op commands
class NoopCommand extends Command {
  process(t, ts, argv){}
}

//! --null command
class NullCommand extends Command {
  process(st, ts, argv){
    this.argcCheck(ts,argv,1);
    st.nullValue( argv[1] );
  }
}

//! --print command
class PrintCommand extends Command {
  process(st, ts, argv){
    st.out(ts.getOutputPrefix(),': ');
    if( 1==argv.length ){
      st.out( st.getInputText() );
    }else{
      st.outln( Util.argvToString(argv) );
    }
  }
}

//! --result command
class ResultCommand extends Command {
  #bufferMode;
  constructor(resultBufferMode = ResultBufferMode.ESCAPED){
    super();
    this.#bufferMode = resultBufferMode;
  }
  process(t, ts, argv){
    this.argcCheck(ts,argv,0,-1);
    t.incrementTestCounter();
    const sql = t.takeInputBuffer();
    //ts.verbose2(argv[0]," SQL =\n",sql);
    t.execSql(null, false, this.#bufferMode, ResultRowMode.ONELINE, sql);
    const result = t.getResultText().trim();
    const sArgs = argv.length>1 ? Util.argvToString(argv) : "";
    if( result !== sArgs ){
      t.outln(argv[0]," FAILED comparison. Result buffer:\n",
              result,"\nExpected result:\n",sArgs);
      ts.toss(argv[0]+" comparison failed.");
    }
  }
}

//! --json command
class JsonCommand extends ResultCommand {
  constructor(){ super(ResultBufferMode.ASIS); }
}

//! --run command
class RunCommand extends Command {
  process(t, ts, argv){
    this.argcCheck(ts,argv,0,1);
    const pDb = (1==argv.length)
      ? t.currentDb() : t.getDbById( parseInt(argv[1]) );
    const sql = t.takeInputBuffer();
    const rc = t.execSql(pDb, false, ResultBufferMode.NONE,
                       ResultRowMode.ONELINE, sql);
    if( 0!==rc && t.verbosity()>0 ){
      const msg = sqlite3.capi.sqlite3_errmsg(pDb);
      ts.verbose2(argv[0]," non-fatal command error #",rc,": ",
                  msg,"\nfor SQL:\n",sql);
    }
  }
}

//! --tableresult command
class TableResultCommand extends Command {
  #jsonMode;
  constructor(jsonMode=false){
    super();
    this.#jsonMode = jsonMode;
  }
  process(t, ts, argv){
    this.argcCheck(ts,argv,0);
    t.incrementTestCounter();
    let body = ts.fetchCommandBody(t);
    if( null===body ) ts.toss("Missing ",argv[0]," body.");
    body = body.trim();
    if( !body.endsWith("\n--end") ){
      ts.toss(argv[0], " must be terminated with --end\\n");
    }else{
      body = body.substring(0, body.length-6);
    }
    const globs = body.split(/\s*\n\s*/);
    if( globs.length < 1 ){
      ts.toss(argv[0], " requires 1 or more ",
              (this.#jsonMode ? "json snippets" : "globs"),".");
    }
    const sql = t.takeInputBuffer();
    t.execSql(null, true,
              this.#jsonMode ? ResultBufferMode.ASIS : ResultBufferMode.ESCAPED,
              ResultRowMode.NEWLINE, sql);
    const rbuf = t.getResultText().trim();
    const res = rbuf.split(/\r?\n/);
    if( res.length !== globs.length ){
      ts.toss(argv[0], " failure: input has ", res.length,
              " row(s) but expecting ",globs.length);
    }
    for(let i = 0; i < res.length; ++i){
      const glob = globs[i].replaceAll(/\s+/g," ").trim();
      //ts.verbose2(argv[0]," <<",glob,">> vs <<",res[i],">>");
      if( this.#jsonMode ){
        if( glob!==res[i] ){
          ts.toss(argv[0], " json <<",glob, ">> does not match: <<",
                  res[i],">>");
        }
      }else if( 0!=Util.strglob(glob, res[i]) ){
        ts.toss(argv[0], " glob <<",glob,">> does not match: <<",res[i],">>");
      }
    }
  }
}

//! --json-block command
class JsonBlockCommand extends TableResultCommand {
  constructor(){ super(true); }
}

//! --testcase command
class TestCaseCommand extends Command {
  process(tester, script, argv){
    this.argcCheck(script, argv,1);
    script.testCaseName(argv[1]);
    tester.clearResultBuffer();
    tester.clearInputBuffer();
  }
}


//! --verbosity command
class VerbosityCommand extends Command {
  process(t, ts, argv){
    this.argcCheck(ts,argv,1);
    ts.verbosity( parseInt(argv[1]) );
  }
}

class CommandDispatcher {
  static map = newObj();

  static getCommandByName(name){
    let rv = CommandDispatcher.map[name];
    if( rv ) return rv;
    switch(name){
      case "close":        rv = new CloseDbCommand(); break;
      case "column-names": rv = new ColumnNamesCommand(); break;
      case "db":           rv = new DbCommand(); break;
      case "glob":         rv = new GlobCommand(); break;
      case "json":         rv = new JsonCommand(); break;
      case "json-block":   rv = new JsonBlockCommand(); break;
      case "new":          rv = new NewDbCommand(); break;
      case "notglob":      rv = new NotGlobCommand(); break;
      case "null":         rv = new NullCommand(); break;
      case "oom":          rv = new NoopCommand(); break;
      case "open":         rv = new OpenDbCommand(); break;
      case "print":        rv = new PrintCommand(); break;
      case "result":       rv = new ResultCommand(); break;
      case "run":          rv = new RunCommand(); break;
      case "tableresult":  rv = new TableResultCommand(); break;
      case "testcase":     rv = new TestCaseCommand(); break;
      case "verbosity":    rv = new VerbosityCommand(); break;
    }
    if( rv ){
      CommandDispatcher.map[name] = rv;
    }
    return rv;
  }

  static dispatch(tester, testScript, argv){
    const cmd = CommandDispatcher.getCommandByName(argv[0]);
    if( !cmd ){
      toss(UnknownCommand,testScript,argv[0]);
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
  Util,
  sqlite3
});

export {namespace as default};
