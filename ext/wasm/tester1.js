/*
  2022-10-12

  The author disclaims copyright to this source code.  In place of a
  legal notice, here is a blessing:

  *   May you do good and not evil.
  *   May you find forgiveness for yourself and forgive others.
  *   May you share freely, never taking more than you give.

  ***********************************************************************

  Main functional and regression tests for the sqlite3 WASM API.

  This mini-framework works like so:

  This script adds a series of test groups, each of which contains an
  arbitrary number of tests, into a queue. After loading of the
  sqlite3 WASM/JS module is complete, that queue is processed. If any
  given test fails, the whole thing fails. This script is built such
  that it can run from the main UI thread or worker thread. Test
  groups and individual tests can be assigned a predicate function
  which determines whether to run them or not, and this is
  specifically intended to be used to toggle certain tests on or off
  for the main/worker threads.

  Each test group defines a state object which gets applied as each
  test function's `this`. Test functions can use that to, e.g., set up
  a db in an early test and close it in a later test. Each test gets
  passed the sqlite3 namespace object as its only argument.
*/
'use strict';
(function(){
  /**
     Set up our output channel differently depending
     on whether we are running in a worker thread or
     the main (UI) thread.
  */
  let logClass;
  /* Predicate for tests/groups. */
  const isUIThread = ()=>(self.window===self && self.document);
  /* Predicate for tests/groups. */
  const isWorker = ()=>!isUIThread();
  /* Predicate for tests/groups. */
  const testIsTodo = ()=>false;
  const haveJaccwabytTests = function(){
    return !!wasm.exports.jaccwabyt_test_int64_max;
  };
  const mapToString = (v)=>{
    switch(typeof v){
        case 'number': case 'string': case 'boolean':
        case 'undefined':
          return ''+v;
        default: break;
    }
    if(null===v) return 'null';
    if(v instanceof Error){
      v = {
        message: v.message,
        stack: v.stack,
        errorClass: v.name
      };
    }
    return JSON.stringify(v,undefined,2);
  };
  const normalizeArgs = (args)=>args.map(mapToString);
  if( isUIThread() ){
    console.log("Running in UI thread.");
    const logTarget = document.querySelector('#test-output');
    logClass = function(cssClass,...args){
      const ln = document.createElement('div');
      if(cssClass) ln.classList.add(cssClass);
      ln.append(document.createTextNode(normalizeArgs(args).join(' ')));
      logTarget.append(ln);
    };
  }else{ /* Worker thread */
    console.log("Running Worker thread.");
    logClass = function(cssClass,...args){
      postMessage({
        type:'log',
        payload:{cssClass, args: normalizeArgs(args)}
      });
    };
  }
  const log = (...args)=>{
    //console.log(...args);
    logClass('',...args);
  }
  const warn = (...args)=>{
    console.warn(...args);
    logClass('warning',...args);
  }
  const error = (...args)=>{
    console.error(...args);
    logClass('error',...args);
  };

  const toss = (...args)=>{
    error(...args);
    throw new Error(args.join(' '));
  };
  const tossQuietly = (...args)=>{
    throw new Error(args.join(' '));
  };

  /**
     Helpers for writing sqlite3-specific tests.
  */
  const TestUtil = {
    /** Running total of the number of tests run via
        this API. */
    counter: 0,
    /* Separator line for log messages. */
    separator: '------------------------------------------------------------',
    /**
       If expr is a function, it is called and its result
       is returned, coerced to a bool, else expr, coerced to
       a bool, is returned.
    */
    toBool: function(expr){
      return (expr instanceof Function) ? !!expr() : !!expr;
    },
    /** abort() if expr is false. If expr is a function, it
        is called and its result is evaluated.
    */
    assert: function f(expr, msg){
      if(!f._){
        f._ = ('undefined'===typeof abort
               ? (msg)=>{throw new Error(msg)}
               : abort);
      }
      ++this.counter;
      if(!this.toBool(expr)){
        f._(msg || "Assertion failed.");
      }
      return this;
    },
    /** Identical to assert() but throws instead of calling
        abort(). */
    affirm: function(expr, msg){
      ++this.counter;
      if(!this.toBool(expr)) throw new Error(msg || "Affirmation failed.");
      return this;
    },
    /** Calls f() and squelches any exception it throws. If it
        does not throw, this function throws. */
    mustThrow: function(f, msg){
      ++this.counter;
      let err;
      try{ f(); } catch(e){err=e;}
      if(!err) throw new Error(msg || "Expected exception.");
      return this;
    },
    /**
       Works like mustThrow() but expects filter to be a regex,
       function, or string to match/filter the resulting exception
       against. If f() does not throw, this test fails and an Error is
       thrown. If filter is a regex, the test passes if
       filter.test(error.message) passes. If it's a function, the test
       passes if filter(error) returns truthy. If it's a string, the
       test passes if the filter matches the exception message
       precisely. In all other cases the test fails, throwing an
       Error.

       If it throws, msg is used as the error report unless it's falsy,
       in which case a default is used.
    */
    mustThrowMatching: function(f, filter, msg){
      ++this.counter;
      let err;
      try{ f(); } catch(e){err=e;}
      if(!err) throw new Error(msg || "Expected exception.");
      let pass = false;
      if(filter instanceof RegExp) pass = filter.test(err.message);
      else if(filter instanceof Function) pass = filter(err);
      else if('string' === typeof filter) pass = (err.message === filter);
      if(!pass){
        throw new Error(msg || ("Filter rejected this exception: "+err.message));
      }
      return this;
    },
    /** Throws if expr is truthy or expr is a function and expr()
        returns truthy. */
    throwIf: function(expr, msg){
      ++this.counter;
      if(this.toBool(expr)) throw new Error(msg || "throwIf() failed");
      return this;
    },
    /** Throws if expr is falsy or expr is a function and expr()
        returns falsy. */
    throwUnless: function(expr, msg){
      ++this.counter;
      if(!this.toBool(expr)) throw new Error(msg || "throwUnless() failed");
      return this;
    },
    eqApprox: (v1,v2,factor=0.05)=>(v1>=(v2-factor) && v1<=(v2+factor)),
    TestGroup: (function(){
      let groupCounter = 0;
      const TestGroup = function(name, predicate){
        this.number = ++groupCounter;
        this.name = name;
        this.predicate = predicate;
        this.tests = [];
      };
      TestGroup.prototype = {
        addTest: function(testObj){
          this.tests.push(testObj);
          return this;
        },
        run: async function(sqlite3){
          if(this.predicate && !this.predicate()){
            log("SKIPPING test group #"+this.number,this.name);
            return;
          }
          log(TestUtil.separator);
          logClass('group-start',"Group #"+this.number+':',this.name);
          const indent = '....';
          const assertCount = TestUtil.counter;
          const groupState = Object.create(null);
          const skipped = [];
          for(let i in this.tests){
            const t = this.tests[i];
            const n = this.number+"."+i;
            if(t.predicate && !t.predicate()){
              logClass('warning',indent, n+': SKIPPING',t.name);
              skipped.push( n+': '+t.name );
            }else{
              const tc = TestUtil.counter
              log(indent, n+":", t.name);
              await t.test.call(groupState, sqlite3);
              //log(indent, indent, 'assertion count:',TestUtil.counter - tc);
            }
          }
          logClass('green',
                   "Group #"+this.number,"assertion count:",(TestUtil.counter - assertCount));
          if(skipped.length){
            log("SKIPPED test(s) in group",this.number+":",skipped);
          }
        }
      };
      return TestGroup;
    })()/*TestGroup*/,
    testGroups: [],
    currentTestGroup: undefined,
    addGroup: function(name, predicate){
      this.testGroups.push( this.currentTestGroup = new this.TestGroup(name) );
      return this;
    },
    addTest: function(name, callback){
      let predicate;
      if(1===arguments.length){
        const opt = arguments[0];
        predicate = opt.predicate;
        name = opt.name;
        callback = opt.test;
      }
      this.currentTestGroup.addTest({
        name, predicate, test: callback
      });
      return this;
    },
    runTests: async function(sqlite3){
      return new Promise(async function(pok,pnok){
        try {
          for(let g of this.testGroups){
            await g.run(sqlite3);
          }
          log(TestUtil.separator);
          log("Done running tests. Total assertion count:",TestUtil.counter);
          pok();
        }catch(e){
          error(e);
          pnok(e);
        }
      }.bind(this));
    }
  }/*TestUtil*/;
  const T = TestUtil;
  T.g = T.addGroup;
  T.t = T.addTest;
  let capi, wasm/*assigned after module init*/;
  ////////////////////////////////////////////////////////////////////////
  // End of infrastructure setup. Now define the tests...
  ////////////////////////////////////////////////////////////////////////
  
  T.g('Basic sanity checks')
    .t('Namespace object checks', function(sqlite3){
      const wasmCtypes = wasm.ctype;
      T.assert(wasmCtypes.structs[0].name==='sqlite3_vfs').
        assert(wasmCtypes.structs[0].members.szOsFile.sizeof>=4).
        assert(wasmCtypes.structs[1/*sqlite3_io_methods*/
                                 ].members.xFileSize.offset>0);
      [ /* Spot-check a handful of constants to make sure they got installed... */
        'SQLITE_SCHEMA','SQLITE_NULL','SQLITE_UTF8',
        'SQLITE_STATIC', 'SQLITE_DIRECTONLY',
        'SQLITE_OPEN_CREATE', 'SQLITE_OPEN_DELETEONCLOSE'
      ].forEach((k)=>T.assert('number' === typeof capi[k]));
      [/* Spot-check a few of the WASM API methods. */
        'alloc', 'dealloc', 'installFunction'
      ].forEach((k)=>T.assert(wasm[k] instanceof Function));

      T.assert(capi.sqlite3_errstr(capi.SQLITE_IOERR_ACCESS).indexOf("I/O")>=0).
        assert(capi.sqlite3_errstr(capi.SQLITE_CORRUPT).indexOf('malformed')>0).
        assert(capi.sqlite3_errstr(capi.SQLITE_OK) === 'not an error');

      try {
        throw new sqlite3.WasmAllocError;
      }catch(e){
        T.assert(e instanceof Error)
          .assert(e instanceof sqlite3.WasmAllocError);
      }
    })
    .t('strglob/strlike', function(sqlite3){
      T.assert(0===capi.sqlite3_strglob("*.txt", "foo.txt")).
        assert(0!==capi.sqlite3_strglob("*.txt", "foo.xtx")).
        assert(0===capi.sqlite3_strlike("%.txt", "foo.txt", 0)).
        assert(0!==capi.sqlite3_strlike("%.txt", "foo.xtx", 0));
    })

    .t('sqlite3.capi.wasm.pstack', function(sqlite3){
      const w = sqlite3.capi.wasm, P = w.pstack;
      const isAllocErr = (e)=>e instanceof sqlite3.WasmAllocError;
      const stack = P.pointer;
      T.assert(0===stack % 8 /* must be 8-byte aligned */);
      try{
        const remaining = P.remaining;
        T.assert(P.quota >= 4096)
          .assert(remaining === P.quota)
          .mustThrowMatching(()=>P.alloc(0), isAllocErr)
          .mustThrowMatching(()=>P.alloc(-1), isAllocErr);
        let p1 = P.alloc(12);
        T.assert(p1 === stack - 16/*8-byte aligned*/)
          .assert(P.pointer === p1);
        let p2 = P.alloc(7);
        T.assert(p2 === p1-8/*8-byte aligned, stack grows downwards*/)
          .mustThrowMatching(()=>P.alloc(remaining), isAllocErr)
          .assert(24 === stack - p2)
          .assert(P.pointer === p2);
        let n = remaining - (stack - p2);
        let p3 = P.alloc(n);
        T.assert(p3 === stack-remaining)
          .mustThrowMatching(()=>P.alloc(1), isAllocErr);
      }finally{
        P.restore(stack);
      }

      T.assert(P.pointer === stack);
      try {
        const [p1, p2, p3] = P.allocChunks(3,4);
        T.assert(P.pointer === stack-16/*always rounded to multiple of 8*/)
          .assert(p2 === p1 + 4)
          .assert(p3 === p2 + 4);
        T.mustThrowMatching(()=>P.allocChunks(1024, 1024 * 16),
                            (e)=>e instanceof sqlite3.WasmAllocError)
      }finally{
        P.restore(stack);
      }

      T.assert(P.pointer === stack);
      try {
        let [p1, p2, p3] = P.allocPtr(3,false);
        let sPos = stack-16/*always rounded to multiple of 8*/;
        T.assert(P.pointer === sPos)
          .assert(p2 === p1 + 4)
          .assert(p3 === p2 + 4);
        [p1, p2, p3] = P.allocPtr(3);
        T.assert(P.pointer === sPos-24/*3 x 8 bytes*/)
          .assert(p2 === p1 + 8)
          .assert(p3 === p2 + 8);
        p1 = P.allocPtr();
        T.assert('number'===typeof p1);
      }finally{
        P.restore(stack);
      }
    }/*pstack tests*/)
  ;/*end of basic sanity checks*/

  ////////////////////////////////////////////////////////////////////////
  T.g('sqlite3.oo1 sanity checks')
    .t('Create db', function(sqlite3){
      const db = this.db = new sqlite3.oo1.DB();
      T.assert(Number.isInteger(db.pointer)).
        mustThrowMatching(()=>db.pointer=1, /read-only/).
        assert(0===sqlite3.capi.sqlite3_extended_result_codes(db.pointer,1)).
        assert('main'===db.dbName(0));

      // Custom db error message handling via sqlite3_prepare_v2/v3()
      let rc = capi.sqlite3_prepare_v3(db.pointer, {/*invalid*/}, -1, 0, null, null);
      T.assert(capi.SQLITE_MISUSE === rc)
        .assert(0 === capi.sqlite3_errmsg(db.pointer).indexOf("Invalid SQL"));
    })
    .t('DB.Stmt sanity checks', function(S){
      let pId;
      let st = this.db.prepare(
        new TextEncoder('utf-8').encode("select 3 as a")
      );
      //debug("statement =",st);
      try {
        T.assert(Number.isInteger(st.pointer))
          .mustThrowMatching(()=>st.pointer=1, /read-only/)
          .assert(1===this.db.openStatementCount())
          .assert(!st._mayGet)
          .assert('a' === st.getColumnName(0))
          .assert(1===st.columnCount)
          .assert(0===st.parameterCount)
          .mustThrow(()=>st.bind(1,null))
          .assert(true===st.step())
          .assert(3 === st.get(0))
          .mustThrow(()=>st.get(1))
          .mustThrow(()=>st.get(0,~capi.SQLITE_INTEGER))
          .assert(3 === st.get(0,capi.SQLITE_INTEGER))
          .assert(3 === st.getInt(0))
          .assert('3' === st.get(0,capi.SQLITE_TEXT))
          .assert('3' === st.getString(0))
          .assert(3.0 === st.get(0,capi.SQLITE_FLOAT))
          .assert(3.0 === st.getFloat(0))
          .assert(3 === st.get({}).a)
          .assert(3 === st.get([])[0])
          .assert(3 === st.getJSON(0))
          .assert(st.get(0,capi.SQLITE_BLOB) instanceof Uint8Array)
          .assert(1===st.get(0,capi.SQLITE_BLOB).length)
          .assert(st.getBlob(0) instanceof Uint8Array)
          .assert('3'.charCodeAt(0) === st.getBlob(0)[0])
          .assert(st._mayGet)
          .assert(false===st.step())
          .assert(!st._mayGet)
        ;
        pId = st.pointer;
        T.assert(0===capi.sqlite3_strglob("*.txt", "foo.txt")).
          assert(0!==capi.sqlite3_strglob("*.txt", "foo.xtx")).
          assert(0===capi.sqlite3_strlike("%.txt", "foo.txt", 0)).
          assert(0!==capi.sqlite3_strlike("%.txt", "foo.xtx", 0));
      }finally{
        st.finalize();
      }
      T.assert(!st.pointer)
        .assert(0===this.db.openStatementCount());
    })
    .t('Table t', function(sqlite3){
      const db = this.db;
      let list = [];
      db.exec({
        sql:['CREATE TABLE t(a,b);',
             "INSERT INTO t(a,b) VALUES(1,2),(3,4),",
             "(?,?),('blob',X'6869')"/*intentionally missing semicolon to test for
                                       off-by-one bug in string-to-WASM conversion*/],
        saveSql: list,
        bind: [5,6]
      });
      //debug("Exec'd SQL:", list);
      T.assert(2 === list.length)
        .assert('string'===typeof list[1])
        .assert(4===db.changes());
      if(capi.wasm.bigIntEnabled){
        T.assert(4n===db.changes(false,true));
      }
      let blob = db.selectValue("select b from t where a='blob'");
      T.assert(blob instanceof Uint8Array).
        assert(0x68===blob[0] && 0x69===blob[1]);
      blob = null;
      let counter = 0, colNames = [];
      list.length = 0;
      db.exec(new TextEncoder('utf-8').encode("SELECT a a, b b FROM t"),{
        rowMode: 'object',
        resultRows: list,
        columnNames: colNames,
        callback: function(row,stmt){
          ++counter;
          T.assert((row.a%2 && row.a<6) || 'blob'===row.a);
        }
      });
      T.assert(2 === colNames.length)
        .assert('a' === colNames[0])
        .assert(4 === counter)
        .assert(4 === list.length);
      list.length = 0;
      db.exec("SELECT a a, b b FROM t",{
        rowMode: 'array',
        callback: function(row,stmt){
          ++counter;
          T.assert(Array.isArray(row))
            .assert((0===row[1]%2 && row[1]<7)
                    || (row[1] instanceof Uint8Array));
        }
      });
      T.assert(8 === counter);
      T.assert(Number.MIN_SAFE_INTEGER ===
               db.selectValue("SELECT "+Number.MIN_SAFE_INTEGER)).
        assert(Number.MAX_SAFE_INTEGER ===
               db.selectValue("SELECT "+Number.MAX_SAFE_INTEGER));
      if(capi.wasm.bigIntEnabled && haveJaccwabytTests()){
        const mI = capi.wasm.xCall('jaccwabyt_test_int64_max');
        const b = BigInt(Number.MAX_SAFE_INTEGER * 2);
        T.assert(b === db.selectValue("SELECT "+b)).
          assert(b === db.selectValue("SELECT ?", b)).
          assert(mI == db.selectValue("SELECT $x", {$x:mI}));
      }else{
        /* Curiously, the JS spec seems to be off by one with the definitions
           of MIN/MAX_SAFE_INTEGER:

           https://github.com/emscripten-core/emscripten/issues/17391 */
        T.mustThrow(()=>db.selectValue("SELECT "+(Number.MAX_SAFE_INTEGER+1))).
          mustThrow(()=>db.selectValue("SELECT "+(Number.MIN_SAFE_INTEGER-1)));
      }

      let st = db.prepare("update t set b=:b where a='blob'");
      try {
        const ndx = st.getParamIndex(':b');
        T.assert(1===ndx);
        st.bindAsBlob(ndx, "ima blob").reset(true);
      } finally {
        st.finalize();
      }

      try {
        db.prepare("/*empty SQL*/");
        toss("Must not be reached.");
      }catch(e){
        T.assert(e instanceof sqlite3.SQLite3Error)
          .assert(0==e.message.indexOf('Cannot prepare empty'));
      }
    })

    .t('Scalar UDFs', function(sqlite3){
      const db = this.db;
      db.createFunction("foo",(pCx,a,b)=>a+b);
      T.assert(7===db.selectValue("select foo(3,4)")).
        assert(5===db.selectValue("select foo(3,?)",2)).
        assert(5===db.selectValue("select foo(?,?2)",[1,4])).
        assert(5===db.selectValue("select foo($a,$b)",{$a:0,$b:5}));
      db.createFunction("bar", {
        arity: -1,
        callback: function(pCx){
          var rc = 0;
          for(let i = 1; i < arguments.length; ++i) rc += arguments[i];
          return rc;
        }
      }).createFunction({
        name: "asis",
        callback: (pCx,arg)=>arg
      });
      T.assert(0===db.selectValue("select bar()")).
        assert(1===db.selectValue("select bar(1)")).
        assert(3===db.selectValue("select bar(1,2)")).
        assert(-1===db.selectValue("select bar(1,2,-4)")).
        assert('hi' === db.selectValue("select asis('hi')")).
        assert('hi' === db.selectValue("select ?",'hi')).
        assert(null === db.selectValue("select null")).
        assert(null === db.selectValue("select asis(null)")).
        assert(1 === db.selectValue("select ?",1)).
        assert(2 === db.selectValue("select ?",[2])).
        assert(3 === db.selectValue("select $a",{$a:3})).
        assert(T.eqApprox(3.1,db.selectValue("select 3.0 + 0.1"))).
        assert(T.eqApprox(1.3,db.selectValue("select asis(1 + 0.3)")));

      let blobArg = new Uint8Array(2);
      blobArg.set([0x68, 0x69], 0);
      let blobRc = db.selectValue("select asis(?1)", blobArg);
      T.assert(blobRc instanceof Uint8Array).
        assert(2 === blobRc.length).
        assert(0x68==blobRc[0] && 0x69==blobRc[1]);
      blobRc = db.selectValue("select asis(X'6869')");
      T.assert(blobRc instanceof Uint8Array).
        assert(2 === blobRc.length).
        assert(0x68==blobRc[0] && 0x69==blobRc[1]);

      blobArg = new Int8Array(2);
      blobArg.set([0x68, 0x69]);
      //debug("blobArg=",blobArg);
      blobRc = db.selectValue("select asis(?1)", blobArg);
      T.assert(blobRc instanceof Uint8Array).
        assert(2 === blobRc.length);
      //debug("blobRc=",blobRc);
      T.assert(0x68==blobRc[0] && 0x69==blobRc[1]);
    })

    .t({
      name: 'Aggregate UDFs (tests are TODO)',
      predicate: testIsTodo
    })

    .t({
      name: 'Window UDFs (tests are TODO)',
      predicate: testIsTodo
    })

    .t("ATTACH", function(){
      const db = this.db;
      const resultRows = [];
      db.exec({
        sql:new TextEncoder('utf-8').encode([
          // ^^^ testing string-vs-typedarray handling in exec()
          "attach 'session' as foo;" /* name 'session' is magic for kvvfs! */,
          "create table foo.bar(a);",
          "insert into foo.bar(a) values(1),(2),(3);",
          "select a from foo.bar order by a;"
        ].join('')),
        rowMode: 0,
        resultRows
      });
      T.assert(3===resultRows.length)
        .assert(2===resultRows[1]);
      T.assert(2===db.selectValue('select a from foo.bar where a>1 order by a'));
      let colCount = 0, rowCount = 0;
      const execCallback = function(pVoid, nCols, aVals, aNames){
        colCount = nCols;
        ++rowCount;
        T.assert(2===aVals.length)
          .assert(2===aNames.length)
          .assert(+(aVals[1]) === 2 * +(aVals[0]));
      };
      let rc = capi.sqlite3_exec(
        db.pointer, "select a, a*2 from foo.bar", execCallback,
        0, 0
      );
      T.assert(0===rc).assert(3===rowCount).assert(2===colCount);
      rc = capi.sqlite3_exec(
        db.pointer, "select a from foo.bar", ()=>{
          tossQuietly("Testing throwing from exec() callback.");
        }, 0, 0
      );
      T.assert(capi.SQLITE_ABORT === rc);
      db.exec("detach foo");
      T.mustThrow(()=>db.exec("select * from foo.bar"));
    })

    .t({
      name: 'Jaccwabyt-specific int-pointer tests (if compiled in)',
      predicate: haveJaccwabytTests,
      test: function(){
        const w = wasm, db = this.db;
        const stack = w.scopedAllocPush();
        let ptrInt;
        const origValue = 512;
        const ptrValType = 'i32';
        try{
          ptrInt = w.scopedAlloc(4);
          w.setMemValue(ptrInt,origValue, ptrValType);
          const cf = w.xGet('jaccwabyt_test_intptr');
          const oldPtrInt = ptrInt;
          //log('ptrInt',ptrInt);
          //log('getMemValue(ptrInt)',w.getMemValue(ptrInt));
          T.assert(origValue === w.getMemValue(ptrInt, ptrValType));
          const rc = cf(ptrInt);
          //log('cf(ptrInt)',rc);
          //log('ptrInt',ptrInt);
          //log('getMemValue(ptrInt)',w.getMemValue(ptrInt,ptrValType));
          T.assert(2*origValue === rc).
            assert(rc === w.getMemValue(ptrInt,ptrValType)).
            assert(oldPtrInt === ptrInt);
          const pi64 = w.scopedAlloc(8)/*ptr to 64-bit integer*/;
          const o64 = 0x010203040506/*>32-bit integer*/;
          const ptrType64 = 'i64';
          if(w.bigIntEnabled){
            w.setMemValue(pi64, o64, ptrType64);
            //log("pi64 =",pi64, "o64 = 0x",o64.toString(16), o64);
            const v64 = ()=>w.getMemValue(pi64,ptrType64)
            //log("getMemValue(pi64)",v64());
            T.assert(v64() == o64);
            //T.assert(o64 === w.getMemValue(pi64, ptrType64));
            const cf64w = w.xGet('jaccwabyt_test_int64ptr');
            cf64w(pi64);
            //log("getMemValue(pi64)",v64());
            T.assert(v64() == BigInt(2 * o64));
            cf64w(pi64);
            T.assert(v64() == BigInt(4 * o64));

            const biTimes2 = w.xGet('jaccwabyt_test_int64_times2');
            T.assert(BigInt(2 * o64) ===
                     biTimes2(BigInt(o64)/*explicit conv. required to avoid TypeError
                                           in the call :/ */));

            const pMin = w.scopedAlloc(16);
            const pMax = pMin + 8;
            const g64 = (p)=>w.getMemValue(p,ptrType64);
            w.setMemValue(pMin, 0, ptrType64);
            w.setMemValue(pMax, 0, ptrType64);
            const minMaxI64 = [
              w.xCall('jaccwabyt_test_int64_min'),
              w.xCall('jaccwabyt_test_int64_max')
            ];
            T.assert(minMaxI64[0] < BigInt(Number.MIN_SAFE_INTEGER)).
              assert(minMaxI64[1] > BigInt(Number.MAX_SAFE_INTEGER));
            //log("int64_min/max() =",minMaxI64, typeof minMaxI64[0]);
            w.xCall('jaccwabyt_test_int64_minmax', pMin, pMax);
            T.assert(g64(pMin) === minMaxI64[0], "int64 mismatch").
              assert(g64(pMax) === minMaxI64[1], "int64 mismatch");
            //log("pMin",g64(pMin), "pMax",g64(pMax));
            w.setMemValue(pMin, minMaxI64[0], ptrType64);
            T.assert(g64(pMin) === minMaxI64[0]).
              assert(minMaxI64[0] === db.selectValue("select ?",g64(pMin))).
              assert(minMaxI64[1] === db.selectValue("select ?",g64(pMax)));
            const rxRange = /too big/;
            T.mustThrowMatching(()=>{db.prepare("select ?").bind(minMaxI64[0] - BigInt(1))},
                                rxRange).
              mustThrowMatching(()=>{db.prepare("select ?").bind(minMaxI64[1] + BigInt(1))},
                                (e)=>rxRange.test(e.message));
          }else{
            log("No BigInt support. Skipping related tests.");
            log("\"The problem\" here is that we can manipulate, at the byte level,",
                "heap memory to set 64-bit values, but we can't get those values",
                "back into JS because of the lack of 64-bit integer support.");
          }
        }finally{
          const x = w.scopedAlloc(1), y = w.scopedAlloc(1), z = w.scopedAlloc(1);
          //log("x=",x,"y=",y,"z=",z); // just looking at the alignment
          w.scopedAllocPop(stack);
        }
      }
    }/* jaccwabyt-specific tests */)

    .t('Close db', function(){
      T.assert(this.db).assert(Number.isInteger(this.db.pointer));
      wasm.exports.sqlite3_wasm_db_reset(this.db.pointer);
      this.db.close();
      T.assert(!this.db.pointer);
    })
  ;/* end of oo1 checks */

  ////////////////////////////////////////////////////////////////////////
  log("Loading and initializing sqlite3 WASM module...");
  if(!isUIThread()){
    importScripts("sqlite3.js");
  }
  self.sqlite3InitModule({
    print: log,
    printErr: error
  }).then(function(sqlite3){
    //console.log('sqlite3 =',sqlite3);
    log("Done initializing WASM/JS bits. Running tests...");
    capi = sqlite3.capi;
    wasm = capi.wasm;
    log("sqlite3 version:",capi.sqlite3_libversion(),
        capi.sqlite3_sourceid());
    log("BigInt/int64 support is",(wasm.bigIntEnabled ? "enabled" : "disabled"));
    if(haveJaccwabytTests()){
      log("Jaccwabyt test C code found. Jaccwabyt-specific low-level tests.");
    }
    TestUtil.runTests(sqlite3);
  });
})();
