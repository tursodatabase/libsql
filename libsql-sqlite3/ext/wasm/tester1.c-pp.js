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
  for the main/worker threads or the availability (or not) of
  optional features such as int64 support.

  Each test group defines a single state object which gets applied as
  the test functions' `this` for all tests in that group. Test
  functions can use that to, e.g., set up a db in an early test and
  close it in a later test. Each test gets passed the sqlite3
  namespace object as its only argument.
*/
/*
   This file is intended to be processed by c-pp to inject (or not)
   code specific to ES6 modules which is illegal in non-module code.

   Non-ES6 module build and ES6 module for the main-thread:

     ./c-pp -f tester1.c-pp.js -o tester1.js

   ES6 worker module build:

     ./c-pp -f tester1.c-pp.js -o tester1-esm.js -Dtarget=es6-module
*/
//#if target=es6-module
import {default as sqlite3InitModule} from './jswasm/sqlite3.mjs';
globalThis.sqlite3InitModule = sqlite3InitModule;
//#else
'use strict';
//#endif
(function(self){
  /**
     Set up our output channel differently depending
     on whether we are running in a worker thread or
     the main (UI) thread.
  */
  let logClass;
  /* Predicate for tests/groups. */
  const isUIThread = ()=>(globalThis.window===self && globalThis.document);
  /* Predicate for tests/groups. */
  const isWorker = ()=>!isUIThread();
  /* Predicate for tests/groups. */
  const testIsTodo = ()=>false;
  const haveWasmCTests = ()=>{
    return !!wasm.exports.sqlite3_wasm_test_intptr;
  };
  const hasOpfs = ()=>{
    return globalThis.FileSystemHandle
      && globalThis.FileSystemDirectoryHandle
      && globalThis.FileSystemFileHandle
      && globalThis.FileSystemFileHandle.prototype.createSyncAccessHandle
      && navigator?.storage?.getDirectory;
  };

  {
    const mapToString = (v)=>{
      switch(typeof v){
          case 'number': case 'string': case 'boolean':
          case 'undefined': case 'bigint':
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
      console.log("Running in the UI thread.");
      const logTarget = document.querySelector('#test-output');
      logClass = function(cssClass,...args){
        const ln = document.createElement('div');
        if(cssClass){
          for(const c of (Array.isArray(cssClass) ? cssClass : [cssClass])){
            ln.classList.add(c);
          }
        }
        ln.append(document.createTextNode(normalizeArgs(args).join(' ')));
        logTarget.append(ln);
      };
      const cbReverse = document.querySelector('#cb-log-reverse');
      //cbReverse.setAttribute('checked','checked');
      const cbReverseKey = 'tester1:cb-log-reverse';
      const cbReverseIt = ()=>{
        logTarget.classList[cbReverse.checked ? 'add' : 'remove']('reverse');
        //localStorage.setItem(cbReverseKey, cbReverse.checked ? 1 : 0);
      };
      cbReverse.addEventListener('change', cbReverseIt, true);
      /*if(localStorage.getItem(cbReverseKey)){
        cbReverse.checked = !!(+localStorage.getItem(cbReverseKey));
      }*/
      cbReverseIt();
    }else{ /* Worker thread */
      console.log("Running in a Worker thread.");
      logClass = function(cssClass,...args){
        postMessage({
          type:'log',
          payload:{cssClass, args: normalizeArgs(args)}
        });
      };
    }
  }
  const reportFinalTestStatus = function(pass){
    if(isUIThread()){
      let e = document.querySelector('#color-target');
      e.classList.add(pass ? 'tests-pass' : 'tests-fail');
      e = document.querySelector('title');
      e.innerText = (pass ? 'PASS' : 'FAIL') + ': ' + e.innerText;
    }else{
      postMessage({type:'test-result', payload:{pass}});
    }
  };
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

  const roundMs = (ms)=>Math.round(ms*100)/100;

  /**
     Helpers for writing sqlite3-specific tests.
  */
  const TestUtil = {
    /** Running total of the number of tests run via
        this API. */
    counter: 0,
    /**
       If expr is a function, it is called and its result
       is returned, coerced to a bool, else expr, coerced to
       a bool, is returned.
    */
    toBool: function(expr){
      return (expr instanceof Function) ? !!expr() : !!expr;
    },
    /** Throws if expr is false. If expr is a function, it is called
        and its result is evaluated. If passed multiple arguments,
        those after the first are a message string which get applied
        as an exception message if the assertion fails. The message
        arguments are concatenated together with a space between each.
    */
    assert: function f(expr, ...msg){
      ++this.counter;
      if(!this.toBool(expr)){
        throw new Error(msg.length ? msg.join(' ') : "Assertion failed.");
      }
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
          logClass('group-start',"Group #"+this.number+':',this.name);
          if(this.predicate){
            const p = this.predicate(sqlite3);
            if(!p || 'string'===typeof p){
              logClass(['warning','skipping-group'],
                       "SKIPPING group:", p ? p : "predicate says to" );
              return;
            }
          }
          const assertCount = TestUtil.counter;
          const groupState = Object.create(null);
          const skipped = [];
          let runtime = 0, i = 0;
          for(const t of this.tests){
            ++i;
            const n = this.number+"."+i;
            logClass('one-test-line', n+":", t.name);
            if(t.predicate){
              const p = t.predicate(sqlite3);
              if(!p || 'string'===typeof p){
                logClass(['warning','skipping-test'],
                         "SKIPPING:", p ? p : "predicate says to" );
                skipped.push( n+': '+t.name );
                continue;
              }
            }
            const tc = TestUtil.counter, now = performance.now();
            let rc = t.test.call(groupState, sqlite3);
            /*if(rc instanceof Promise){
              rc = rc.catch((e)=>{
                error("Test failure:",e);
                throw e;
              });
            }*/
            await rc;
            const then = performance.now();
            runtime += then - now;
            logClass(['faded','one-test-summary'],
                     TestUtil.counter - tc, 'assertion(s) in',
                     roundMs(then-now),'ms');
          }
          logClass(['green','group-end'],
                   "#"+this.number+":",
                   (TestUtil.counter - assertCount),
                   "assertion(s) in",roundMs(runtime),"ms");
          if(0 && skipped.length){
            logClass('warning',"SKIPPED test(s) in group",this.number+":",skipped);
          }
        }
      };
      return TestGroup;
    })()/*TestGroup*/,
    testGroups: [],
    currentTestGroup: undefined,
    addGroup: function(name, predicate){
      this.testGroups.push( this.currentTestGroup =
                            new this.TestGroup(name, predicate) );
      return this;
    },
    addTest: function(name, callback){
      let predicate;
      if(1===arguments.length){
        this.currentTestGroup.addTest(arguments[0]);
      }else{
        this.currentTestGroup.addTest({
          name, predicate, test: callback
        });
      }
      return this;
    },
    runTests: async function(sqlite3){
      return new Promise(async function(pok,pnok){
        try {
          let runtime = 0;
          for(let g of this.testGroups){
            const now = performance.now();
            await g.run(sqlite3);
            runtime += performance.now() - now;
          }
          logClass(['strong','green','full-test-summary'],
                   "Done running tests.",TestUtil.counter,"assertions in",
                   roundMs(runtime),'ms');
          pok();
          reportFinalTestStatus(true);
        }catch(e){
          error(e);
          pnok(e);
          reportFinalTestStatus(false);
        }
      }.bind(this));
    }
  }/*TestUtil*/;
  const T = TestUtil;
  T.g = T.addGroup;
  T.t = T.addTest;
  let capi, wasm/*assigned after module init*/;
  const sahPoolConfig = {
    name: 'opfs-sahpool-tester1',
    clearOnInit: true,
    initialCapacity: 6
  };
  ////////////////////////////////////////////////////////////////////////
  // End of infrastructure setup. Now define the tests...
  ////////////////////////////////////////////////////////////////////////

  ////////////////////////////////////////////////////////////////////
  T.g('Basic sanity checks')
    .t({
      name:'sqlite3_config()',
      test:function(sqlite3){
        for(const k of [
          'SQLITE_CONFIG_GETMALLOC', 'SQLITE_CONFIG_URI'
        ]){
          T.assert(capi[k] > 0);
        }
        T.assert(capi.SQLITE_MISUSE===capi.sqlite3_config(
          capi.SQLITE_CONFIG_URI, 1
        ), "MISUSE because the library has already been initialized.");
        T.assert(capi.SQLITE_MISUSE === capi.sqlite3_config(
          // not enough args
          capi.SQLITE_CONFIG_GETMALLOC
        ));
        T.assert(capi.SQLITE_NOTFOUND === capi.sqlite3_config(
          // unhandled-in-JS config option
          capi.SQLITE_CONFIG_GETMALLOC, 1
        ));
        if(0){
          log("We cannot _fully_ test sqlite3_config() after the library",
              "has been initialized (which it necessarily has been to",
              "set up various bindings) and we cannot shut it down ",
              "without losing the VFS registrations.");
          T.assert(0 === capi.sqlite3_config(
            capi.SQLITE_CONFIG_URI, 1
          ));
        }
      }
    })/*sqlite3_config()*/

  ////////////////////////////////////////////////////////////////////
    .t({
      name: "JS wasm-side allocator",
      test: function(sqlite3){
        if(sqlite3.config.useStdAlloc){
          warn("Using system allocator. This violates the docs and",
               "may cause grief with certain APIs",
               "(e.g. sqlite3_deserialize()).");
          T.assert(wasm.alloc.impl === wasm.exports.malloc)
            .assert(wasm.dealloc === wasm.exports.free)
            .assert(wasm.realloc.impl === wasm.exports.realloc);
        }else{
          T.assert(wasm.alloc.impl === wasm.exports.sqlite3_malloc)
            .assert(wasm.dealloc === wasm.exports.sqlite3_free)
            .assert(wasm.realloc.impl === wasm.exports.sqlite3_realloc);
        }
      }
    })
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
          .assert(e instanceof sqlite3.WasmAllocError)
          .assert("Allocation failed." === e.message);
      }
      try {
        throw new sqlite3.WasmAllocError("test",{
          cause: 3
        });
      }catch(e){
        T.assert(3 === e.cause)
          .assert("test" === e.message);
      }
      try {throw new sqlite3.WasmAllocError("test","ing",".")}
      catch(e){T.assert("test ing ." === e.message)}

      try{ throw new sqlite3.SQLite3Error(capi.SQLITE_SCHEMA) }
      catch(e){
        T.assert('SQLITE_SCHEMA' === e.message)
          .assert(capi.SQLITE_SCHEMA === e.resultCode);
      }
      try{ sqlite3.SQLite3Error.toss(capi.SQLITE_CORRUPT,{cause: true}) }
      catch(e){
        T.assert('SQLITE_CORRUPT' === e.message)
          .assert(capi.SQLITE_CORRUPT === e.resultCode)
          .assert(true===e.cause);
      }
      try{ sqlite3.SQLite3Error.toss("resultCode check") }
      catch(e){
        T.assert(capi.SQLITE_ERROR === e.resultCode)
          .assert('resultCode check' === e.message);        
      }
    })
  ////////////////////////////////////////////////////////////////////
    .t('strglob/strlike', function(sqlite3){
      T.assert(0===capi.sqlite3_strglob("*.txt", "foo.txt")).
        assert(0!==capi.sqlite3_strglob("*.txt", "foo.xtx")).
        assert(0===capi.sqlite3_strlike("%.txt", "foo.txt", 0)).
        assert(0!==capi.sqlite3_strlike("%.txt", "foo.xtx", 0));
    })

  ////////////////////////////////////////////////////////////////////
  ;/*end of basic sanity checks*/

  ////////////////////////////////////////////////////////////////////
  T.g('C/WASM Utilities')
    .t('sqlite3.wasm namespace', function(sqlite3){
      // TODO: break this into smaller individual test functions.
      const w = wasm;
      const chr = (x)=>x.charCodeAt(0);
      //log("heap getters...");
      {
        const li = [8, 16, 32];
        if(w.bigIntEnabled) li.push(64);
        for(const n of li){
          const bpe = n/8;
          const s = w.heapForSize(n,false);
          T.assert(bpe===s.BYTES_PER_ELEMENT).
            assert(w.heapForSize(s.constructor) === s);
          const u = w.heapForSize(n,true);
          T.assert(bpe===u.BYTES_PER_ELEMENT).
            assert(s!==u).
            assert(w.heapForSize(u.constructor) === u);
        }
      }

      // alloc(), realloc(), allocFromTypedArray()
      {
        let m = w.alloc(14);
        let m2 = w.realloc(m, 16);
        T.assert(m === m2/* because of alignment */);
        T.assert(0 === w.realloc(m, 0));
        m = m2 = 0;

        // Check allocation limits and allocator's responses...
        T.assert('number' === typeof sqlite3.capi.SQLITE_MAX_ALLOCATION_SIZE);
        if(!sqlite3.config.useStdAlloc){
          const tooMuch = sqlite3.capi.SQLITE_MAX_ALLOCATION_SIZE + 1,
                isAllocErr = (e)=>e instanceof sqlite3.WasmAllocError;
          T.mustThrowMatching(()=>w.alloc(tooMuch), isAllocErr)
            .assert(0 === w.alloc.impl(tooMuch))
            .mustThrowMatching(()=>w.realloc(0, tooMuch), isAllocErr)
            .assert(0 === w.realloc.impl(0, tooMuch));
        }

        // Check allocFromTypedArray()...
        const byteList = [11,22,33]
        const u = new Uint8Array(byteList);
        m = w.allocFromTypedArray(u);
        for(let i = 0; i < u.length; ++i){
          T.assert(u[i] === byteList[i])
            .assert(u[i] === w.peek8(m + i));
        }
        w.dealloc(m);
        m = w.allocFromTypedArray(u.buffer);
        for(let i = 0; i < u.length; ++i){
          T.assert(u[i] === byteList[i])
            .assert(u[i] === w.peek8(m + i));
        }

        w.dealloc(m);
        T.mustThrowMatching(
          ()=>w.allocFromTypedArray(1),
          'Value is not of a supported TypedArray type.'
        );
      }

      { // Test peekXYZ()/pokeXYZ()...
        const m = w.alloc(8);
        T.assert( 17 === w.poke8(m,17).peek8(m) )
          .assert( 31987 === w.poke16(m,31987).peek16(m) )
          .assert( 345678 === w.poke32(m,345678).peek32(m) )
          .assert(
            T.eqApprox( 345678.9, w.poke32f(m,345678.9).peek32f(m) )
          ).assert(
            T.eqApprox( 4567890123.4, w.poke64f(m, 4567890123.4).peek64f(m) )
          );
        if(w.bigIntEnabled){
          T.assert(
            BigInt(Number.MAX_SAFE_INTEGER) ===
              w.poke64(m, Number.MAX_SAFE_INTEGER).peek64(m)
          );
        }
        w.dealloc(m);
      }

      // isPtr32()
      {
        const ip = w.isPtr32;
        T.assert(ip(0))
          .assert(!ip(-1))
          .assert(!ip(1.1))
          .assert(!ip(0xffffffff))
          .assert(ip(0x7fffffff))
          .assert(!ip())
          .assert(!ip(null)/*might change: under consideration*/)
        ;
      }

      //log("jstrlen()...");
      {
        T.assert(3 === w.jstrlen("abc")).assert(4 === w.jstrlen("äbc"));
      }

      //log("jstrcpy()...");
      {
        const fillChar = 10;
        let ua = new Uint8Array(8), rc,
            refill = ()=>ua.fill(fillChar);
        refill();
        rc = w.jstrcpy("hello", ua);
        T.assert(6===rc).assert(0===ua[5]).assert(chr('o')===ua[4]);
        refill();
        ua[5] = chr('!');
        rc = w.jstrcpy("HELLO", ua, 0, -1, false);
        T.assert(5===rc).assert(chr('!')===ua[5]).assert(chr('O')===ua[4]);
        refill();
        rc = w.jstrcpy("the end", ua, 4);
        //log("rc,ua",rc,ua);
        T.assert(4===rc).assert(0===ua[7]).
          assert(chr('e')===ua[6]).assert(chr('t')===ua[4]);
        refill();
        rc = w.jstrcpy("the end", ua, 4, -1, false);
        T.assert(4===rc).assert(chr(' ')===ua[7]).
          assert(chr('e')===ua[6]).assert(chr('t')===ua[4]);
        refill();
        rc = w.jstrcpy("", ua, 0, 1, true);
        //log("rc,ua",rc,ua);
        T.assert(1===rc).assert(0===ua[0]);
        refill();
        rc = w.jstrcpy("x", ua, 0, 1, true);
        //log("rc,ua",rc,ua);
        T.assert(1===rc).assert(0===ua[0]);
        refill();
        rc = w.jstrcpy('äbä', ua, 0, 1, true);
        T.assert(1===rc, 'Must not write partial multi-byte char.')
          .assert(0===ua[0]);
        refill();
        rc = w.jstrcpy('äbä', ua, 0, 2, true);
        T.assert(1===rc, 'Must not write partial multi-byte char.')
          .assert(0===ua[0]);
        refill();
        rc = w.jstrcpy('äbä', ua, 0, 2, false);
        T.assert(2===rc).assert(fillChar!==ua[1]).assert(fillChar===ua[2]);
      }/*jstrcpy()*/

      //log("cstrncpy()...");
      {
        const scope = w.scopedAllocPush();
        try {
          let cStr = w.scopedAllocCString("hello");
          const n = w.cstrlen(cStr);
          let cpy = w.scopedAlloc(n+10);
          let rc = w.cstrncpy(cpy, cStr, n+10);
          T.assert(n+1 === rc).
            assert("hello" === w.cstrToJs(cpy)).
            assert(chr('o') === w.peek8(cpy+n-1)).
            assert(0 === w.peek8(cpy+n));
          let cStr2 = w.scopedAllocCString("HI!!!");
          rc = w.cstrncpy(cpy, cStr2, 3);
          T.assert(3===rc).
            assert("HI!lo" === w.cstrToJs(cpy)).
            assert(chr('!') === w.peek8(cpy+2)).
            assert(chr('l') === w.peek8(cpy+3));
        }finally{
          w.scopedAllocPop(scope);
        }
      }

      //log("jstrToUintArray()...");
      {
        let a = w.jstrToUintArray("hello", false);
        T.assert(5===a.byteLength).assert(chr('o')===a[4]);
        a = w.jstrToUintArray("hello", true);
        T.assert(6===a.byteLength).assert(chr('o')===a[4]).assert(0===a[5]);
        a = w.jstrToUintArray("äbä", false);
        T.assert(5===a.byteLength).assert(chr('b')===a[2]);
        a = w.jstrToUintArray("äbä", true);
        T.assert(6===a.byteLength).assert(chr('b')===a[2]).assert(0===a[5]);
      }

      //log("allocCString()...");
      {
        const jstr = "hällo, world!";
        const [cstr, n] = w.allocCString(jstr, true);
        T.assert(14 === n)
          .assert(0===w.peek8(cstr+n))
          .assert(chr('!')===w.peek8(cstr+n-1));
        w.dealloc(cstr);
      }

      //log("scopedAlloc() and friends...");
      {
        const alloc = w.alloc, dealloc = w.dealloc;
        w.alloc = w.dealloc = null;
        T.assert(!w.scopedAlloc.level)
          .mustThrowMatching(()=>w.scopedAlloc(1), /^No scopedAllocPush/)
          .mustThrowMatching(()=>w.scopedAllocPush(), /missing alloc/);
        w.alloc = alloc;
        T.mustThrowMatching(()=>w.scopedAllocPush(), /missing alloc/);
        w.dealloc = dealloc;
        T.mustThrowMatching(()=>w.scopedAllocPop(), /^Invalid state/)
          .mustThrowMatching(()=>w.scopedAlloc(1), /^No scopedAllocPush/)
          .mustThrowMatching(()=>w.scopedAlloc.level=0, /read-only/);
        const asc = w.scopedAllocPush();
        let asc2;
        try {
          const p1 = w.scopedAlloc(16),
                p2 = w.scopedAlloc(16);
          T.assert(1===w.scopedAlloc.level)
            .assert(Number.isFinite(p1))
            .assert(Number.isFinite(p2))
            .assert(asc[0] === p1)
            .assert(asc[1]===p2);
          asc2 = w.scopedAllocPush();
          const p3 = w.scopedAlloc(16);
          T.assert(2===w.scopedAlloc.level)
            .assert(Number.isFinite(p3))
            .assert(2===asc.length)
            .assert(p3===asc2[0]);

          const [z1, z2, z3] = w.scopedAllocPtr(3);
          T.assert('number'===typeof z1).assert(z2>z1).assert(z3>z2)
            .assert(0===w.peek32(z1), 'allocPtr() must zero the targets')
            .assert(0===w.peek32(z3));
        }finally{
          // Pop them in "incorrect" order to make sure they behave:
          w.scopedAllocPop(asc);
          T.assert(0===asc.length);
          T.mustThrowMatching(()=>w.scopedAllocPop(asc),
                              /^Invalid state object/);
          if(asc2){
            T.assert(2===asc2.length,'Should be p3 and z1');
            w.scopedAllocPop(asc2);
            T.assert(0===asc2.length);
            T.mustThrowMatching(()=>w.scopedAllocPop(asc2),
                                /^Invalid state object/);
          }
        }
        T.assert(0===w.scopedAlloc.level);
        w.scopedAllocCall(function(){
          T.assert(1===w.scopedAlloc.level);
          const [cstr, n] = w.scopedAllocCString("hello, world", true);
          T.assert(12 === n)
            .assert(0===w.peek8(cstr+n))
            .assert(chr('d')===w.peek8(cstr+n-1));
        });
      }/*scopedAlloc()*/

      //log("xCall()...");
      {
        const pJson = w.xCall('sqlite3_wasm_enum_json');
        T.assert(Number.isFinite(pJson)).assert(w.cstrlen(pJson)>300);
      }

      //log("xWrap()...");
      {
        T.mustThrowMatching(()=>w.xWrap('sqlite3_libversion',null,'i32'),
                            /requires 0 arg/).
          assert(w.xWrap.resultAdapter('i32') instanceof Function).
          assert(w.xWrap.argAdapter('i32') instanceof Function);
        let fw = w.xWrap('sqlite3_libversion','utf8');
        T.mustThrowMatching(()=>fw(1), /requires 0 arg/);
        let rc = fw();
        T.assert('string'===typeof rc).assert(rc.length>5);
        rc = w.xCallWrapped('sqlite3_wasm_enum_json','*');
        T.assert(rc>0 && Number.isFinite(rc));
        rc = w.xCallWrapped('sqlite3_wasm_enum_json','utf8');
        T.assert('string'===typeof rc).assert(rc.length>300);


        { // 'string:static' argAdapter() sanity checks...
          let argAd = w.xWrap.argAdapter('string:static');
          let p0 = argAd('foo'), p1 = argAd('bar');
          T.assert(w.isPtr(p0) && w.isPtr(p1))
            .assert(p0 !== p1)
            .assert(p0 === argAd('foo'))
            .assert(p1 === argAd('bar'));
        }

        // 'string:flexible' argAdapter() sanity checks...
        w.scopedAllocCall(()=>{
          const argAd = w.xWrap.argAdapter('string:flexible');
          const cj = (v)=>w.cstrToJs(argAd(v));
          T.assert('Hi' === cj('Hi'))
            .assert('hi' === cj(['h','i']))
            .assert('HI' === cj(new Uint8Array([72, 73])));
        });

        // jsFuncToWasm()
        {
          const fsum3 = (x,y,z)=>x+y+z;
          fw = w.jsFuncToWasm('i(iii)', fsum3);
          T.assert(fw instanceof Function)
            .assert( fsum3 !== fw )
            .assert( 3 === fw.length )
            .assert( 6 === fw(1,2,3) );
          T.mustThrowMatching( ()=>w.jsFuncToWasm('x()', function(){}),
                               'Invalid signature letter: x');
        }

        // xWrap(Function,...)
        {
          let fp;
          try {
            const fmy = function fmy(i,s,d){
              if(fmy.debug) log("fmy(",...arguments,")");
              T.assert( 3 === i )
                .assert( w.isPtr(s) )
                .assert( w.cstrToJs(s) === 'a string' )
                .assert( T.eqApprox(1.2, d) );
              return w.allocCString("hi");
            };
            fmy.debug = false;
            const xwArgs = ['string:dealloc', ['i32', 'string', 'f64']];
            fw = w.xWrap(fmy, ...xwArgs);
            const fmyArgs = [3, 'a string', 1.2];
            let rc = fw(...fmyArgs);
            T.assert( 'hi' === rc );
            if(0){
              /* Retain this as a "reminder to self"...

                 This extra level of indirection does not work: the
                 string argument is ending up as a null in fmy() but
                 the numeric arguments are making their ways through

                 What's happening is: installFunction() is creating a
                 WASM-compatible function instance. When we pass a JS string
                 into there it's getting coerced into `null` before being passed
                 on to the lower-level wrapper.
              */
              fmy.debug = true;
              fp = wasm.installFunction('i(isd)', fw);
              fw = w.functionEntry(fp);
              rc = fw(...fmyArgs);
              log("rc =",rc);
              T.assert( 'hi' === rc );
              // Similarly, this does not work:
              //let fpw = w.xWrap(fp, null, [null,null,null]);
              //rc = fpw(...fmyArgs);
              //log("rc =",rc);
              //T.assert( 'hi' === rc );
            }
          }finally{
            wasm.uninstallFunction(fp);
          }
        }

        if(haveWasmCTests()){
          if(!sqlite3.config.useStdAlloc){
            fw = w.xWrap('sqlite3_wasm_test_str_hello', 'utf8:dealloc',['i32']);
            rc = fw(0);
            T.assert('hello'===rc);
            rc = fw(1);
            T.assert(null===rc);
          }

          if(w.bigIntEnabled){
            w.xWrap.resultAdapter('thrice', (v)=>3n*BigInt(v));
            w.xWrap.argAdapter('twice', (v)=>2n*BigInt(v));
            fw = w.xWrap('sqlite3_wasm_test_int64_times2','thrice','twice');
            rc = fw(1);
            T.assert(12n===rc);

            w.scopedAllocCall(function(){
              const pI1 = w.scopedAlloc(8), pI2 = pI1+4;
              w.pokePtr([pI1, pI2], 0);
              const f = w.xWrap('sqlite3_wasm_test_int64_minmax',undefined,['i64*','i64*']);
              const [r1, r2] = w.peek64([pI1, pI2]);
              T.assert(!Number.isSafeInteger(r1)).assert(!Number.isSafeInteger(r2));
            });
          }
        }
      }/*xWrap()*/
    }/*WhWasmUtil*/)

  ////////////////////////////////////////////////////////////////////
    .t('sqlite3.StructBinder (jaccwabyt🐇)', function(sqlite3){
      const S = sqlite3, W = S.wasm;
      const MyStructDef = {
        sizeof: 16,
        members: {
          p4: {offset: 0, sizeof: 4, signature: "i"},
          pP: {offset: 4, sizeof: 4, signature: "P"},
          ro: {offset: 8, sizeof: 4, signature: "i", readOnly: true},
          cstr: {offset: 12, sizeof: 4, signature: "s"}
        }
      };
      if(W.bigIntEnabled){
        const m = MyStructDef;
        m.members.p8 = {offset: m.sizeof, sizeof: 8, signature: "j"};
        m.sizeof += m.members.p8.sizeof;
      }
      const StructType = S.StructBinder.StructType;
      const K = S.StructBinder('my_struct',MyStructDef);
      T.mustThrowMatching(()=>K(), /via 'new'/).
        mustThrowMatching(()=>new K('hi'), /^Invalid pointer/);
      const k1 = new K(), k2 = new K();
      try {
        T.assert(k1.constructor === K).
          assert(K.isA(k1)).
          assert(k1 instanceof K).
          assert(K.prototype.lookupMember('p4').key === '$p4').
          assert(K.prototype.lookupMember('$p4').name === 'p4').
          mustThrowMatching(()=>K.prototype.lookupMember('nope'), /not a mapped/).
          assert(undefined === K.prototype.lookupMember('nope',false)).
          assert(k1 instanceof StructType).
          assert(StructType.isA(k1)).
          mustThrowMatching(()=>k1.$ro = 1, /read-only/);
        Object.keys(MyStructDef.members).forEach(function(key){
          key = K.memberKey(key);
          T.assert(0 == k1[key],
                   "Expecting allocation to zero the memory "+
                   "for "+key+" but got: "+k1[key]+
                   " from "+k1.memoryDump());
        });
        T.assert('number' === typeof k1.pointer).
          mustThrowMatching(()=>k1.pointer = 1, /pointer/);
        k1.$p4 = 1; k1.$pP = 2;
        T.assert(1 === k1.$p4).assert(2 === k1.$pP);
        if(MyStructDef.members.$p8){
          k1.$p8 = 1/*must not throw despite not being a BigInt*/;
          k1.$p8 = BigInt(Number.MAX_SAFE_INTEGER * 2);
          T.assert(BigInt(2 * Number.MAX_SAFE_INTEGER) === k1.$p8);
        }
        T.assert(!k1.ondispose);
        k1.setMemberCString('cstr', "A C-string.");
        T.assert(Array.isArray(k1.ondispose)).
          assert(k1.ondispose[0] === k1.$cstr).
          assert('number' === typeof k1.$cstr).
          assert('A C-string.' === k1.memberToJsString('cstr'));
        k1.$pP = k2;
        T.assert(k1.$pP === k2.pointer);
        k1.$pP = null/*null is special-cased to 0.*/;
        T.assert(0===k1.$pP);
        let ptr = k1.pointer;
        k1.dispose();
        T.assert(undefined === k1.pointer).
          mustThrowMatching(()=>{k1.$pP=1}, /disposed instance/);
      }finally{
        k1.dispose();
        k2.dispose();
      }

      if(!W.bigIntEnabled){
        log("Skipping WasmTestStruct tests: BigInt not enabled.");
        return;
      }

      const WTStructDesc =
            W.ctype.structs.filter((e)=>'WasmTestStruct'===e.name)[0];
      const autoResolvePtr = true /* EXPERIMENTAL */;
      if(autoResolvePtr){
        WTStructDesc.members.ppV.signature = 'P';
      }
      const WTStruct = S.StructBinder(WTStructDesc);
      //log(WTStruct.structName, WTStruct.structInfo);
      const wts = new WTStruct();
      //log("WTStruct.prototype keys:",Object.keys(WTStruct.prototype));
      try{
        T.assert(wts.constructor === WTStruct).
          assert(WTStruct.memberKeys().indexOf('$ppV')>=0).
          assert(wts.memberKeys().indexOf('$v8')>=0).
          assert(!K.isA(wts)).
          assert(WTStruct.isA(wts)).
          assert(wts instanceof WTStruct).
          assert(wts instanceof StructType).
          assert(StructType.isA(wts)).
          assert(wts.pointer>0).assert(0===wts.$v4).assert(0n===wts.$v8).
          assert(0===wts.$ppV).assert(0===wts.$xFunc);
        const testFunc =
              W.xGet('sqlite3_wasm_test_struct'/*name gets mangled in -O3 builds!*/);
        let counter = 0;
        //log("wts.pointer =",wts.pointer);
        const wtsFunc = function(arg){
          /*log("This from a JS function called from C, "+
              "which itself was called from JS. arg =",arg);*/
          ++counter;
          if(3===counter){
            tossQuietly("Testing exception propagation.");
          }
        }
        wts.$v4 = 10; wts.$v8 = 20;
        wts.$xFunc = W.installFunction(wtsFunc, wts.memberSignature('xFunc'))
        T.assert(0===counter).assert(10 === wts.$v4).assert(20n === wts.$v8)
          .assert(0 === wts.$ppV).assert('number' === typeof wts.$xFunc)
          .assert(0 === wts.$cstr)
          .assert(wts.memberIsString('$cstr'))
          .assert(!wts.memberIsString('$v4'))
          .assert(null === wts.memberToJsString('$cstr'))
          .assert(W.functionEntry(wts.$xFunc) instanceof Function);
        /* It might seem silly to assert that the values match
           what we just set, but recall that all of those property
           reads and writes are, via property interceptors,
           actually marshaling their data to/from a raw memory
           buffer, so merely reading them back is actually part of
           testing the struct-wrapping API. */

        testFunc(wts.pointer);
        //log("wts.pointer, wts.$ppV",wts.pointer, wts.$ppV);
        T.assert(1===counter).assert(20 === wts.$v4).assert(40n === wts.$v8)
          .assert(wts.$ppV === wts.pointer)
          .assert('string' === typeof wts.memberToJsString('cstr'))
          .assert(wts.memberToJsString('cstr') === wts.memberToJsString('$cstr'))
          .mustThrowMatching(()=>wts.memberToJsString('xFunc'),
                             /Invalid member type signature for C-string/)
        ;
        testFunc(wts.pointer);
        T.assert(2===counter).assert(40 === wts.$v4).assert(80n === wts.$v8)
          .assert(wts.$ppV === wts.pointer);
        /** The 3rd call to wtsFunc throw from JS, which is called
            from C, which is called from JS. Let's ensure that
            that exception propagates back here... */
        T.mustThrowMatching(()=>testFunc(wts.pointer),/^Testing/);
        W.uninstallFunction(wts.$xFunc);
        wts.$xFunc = 0;
        wts.$ppV = 0;
        T.assert(!wts.$ppV);
        //WTStruct.debugFlags(0x03);
        wts.$ppV = wts;
        T.assert(wts.pointer === wts.$ppV)
        wts.setMemberCString('cstr', "A C-string.");
        T.assert(Array.isArray(wts.ondispose)).
          assert(wts.ondispose[0] === wts.$cstr).
          assert('A C-string.' === wts.memberToJsString('cstr'));
        const ptr = wts.pointer;
        wts.dispose();
        T.assert(ptr).assert(undefined === wts.pointer);
      }finally{
        wts.dispose();
      }

      if(1){ // ondispose of other struct instances
        const s1 = new WTStruct, s2 = new WTStruct, s3 = new WTStruct;
        T.assert(s1.lookupMember instanceof Function)
          .assert(s1.addOnDispose instanceof Function);
        s1.addOnDispose(s2,"testing variadic args");
        T.assert(2===s1.ondispose.length);
        s2.addOnDispose(s3);
        s1.dispose();
        T.assert(!s2.pointer,"Expecting s2 to be ondispose'd by s1.");
        T.assert(!s3.pointer,"Expecting s3 to be ondispose'd by s2.");
      }
    }/*StructBinder*/)

  ////////////////////////////////////////////////////////////////////
    .t('sqlite3.wasm.pstack', function(sqlite3){
      const P = wasm.pstack;
      const isAllocErr = (e)=>e instanceof sqlite3.WasmAllocError;
      const stack = P.pointer;
      T.assert(0===stack % 8 /* must be 8-byte aligned */);
      try{
        const remaining = P.remaining;
        T.assert(P.quota >= 4096)
          .assert(remaining === P.quota)
          .mustThrowMatching(()=>P.alloc(0), isAllocErr)
          .mustThrowMatching(()=>P.alloc(-1), isAllocErr)
          .mustThrowMatching(
            ()=>P.alloc('i33'),
            (e)=>e instanceof sqlite3.WasmAllocError
          );
        ;
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
        const [p1, p2, p3] = P.allocChunks(3,'i32');
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
  ////////////////////////////////////////////////////////////////////
  ;/*end of C/WASM utils checks*/

  T.g('sqlite3_randomness()')
    .t('To memory buffer', function(sqlite3){
      const stack = wasm.pstack.pointer;
      try{
        const n = 520;
        const p = wasm.pstack.alloc(n);
        T.assert(0===wasm.peek8(p))
          .assert(0===wasm.peek8(p+n-1));
        T.assert(undefined === capi.sqlite3_randomness(n - 10, p));
        let j, check = 0;
        const heap = wasm.heap8u();
        for(j = 0; j < 10 && 0===check; ++j){
          check += heap[p + j];
        }
        T.assert(check > 0);
        check = 0;
        // Ensure that the trailing bytes were not modified...
        for(j = n - 10; j < n && 0===check; ++j){
          check += heap[p + j];
        }
        T.assert(0===check);
      }finally{
        wasm.pstack.restore(stack);
      }
    })
    .t('To byte array', function(sqlite3){
      const ta = new Uint8Array(117);
      let i, n = 0;
      for(i=0; i<ta.byteLength && 0===n; ++i){
        n += ta[i];
      }
      T.assert(0===n)
        .assert(ta === capi.sqlite3_randomness(ta));
      for(i=ta.byteLength-10; i<ta.byteLength && 0===n; ++i){
        n += ta[i];
      }
      T.assert(n>0);
      const t0 = new Uint8Array(0);
      T.assert(t0 === capi.sqlite3_randomness(t0),
               "0-length array is a special case");
    })
  ;/*end sqlite3_randomness() checks*/

  ////////////////////////////////////////////////////////////////////////
  T.g('sqlite3.oo1')
    .t('Create db', function(sqlite3){
      const dbFile = '/tester1.db';
      wasm.sqlite3_wasm_vfs_unlink(0, dbFile);
      const db = this.db = new sqlite3.oo1.DB(dbFile, 0 ? 'ct' : 'c');
      db.onclose = {
        disposeAfter: [],
        disposeBefore: [
          (db)=>{
            //console.debug("db.onclose.before dropping modules");
            //sqlite3.capi.sqlite3_drop_modules(db.pointer, 0);
          }
        ],
        before: function(db){
          while(this.disposeBefore.length){
            const v = this.disposeBefore.shift();
            console.debug("db.onclose.before cleaning up:",v);
            if(wasm.isPtr(v)) wasm.dealloc(v);
            else if(v instanceof sqlite3.StructBinder.StructType){
              v.dispose();
            }else if(v instanceof Function){
              try{ v(db) } catch(e){
                console.warn("beforeDispose() callback threw:",e);
              }
            }
          }
        },
        after: function(){
          while(this.disposeAfter.length){
            const v = this.disposeAfter.shift();
            console.debug("db.onclose.after cleaning up:",v);
            if(wasm.isPtr(v)) wasm.dealloc(v);
            else if(v instanceof sqlite3.StructBinder.StructType){
              v.dispose();
            }else if(v instanceof Function){
              try{v()} catch(e){/*ignored*/}
            }
          }
        }
      };

      T.assert(wasm.isPtr(db.pointer))
        .mustThrowMatching(()=>db.pointer=1, /read-only/)
        .assert(0===sqlite3.capi.sqlite3_extended_result_codes(db.pointer,1))
        .assert('main'===db.dbName(0))
        .assert('string' === typeof db.dbVfsName())
        .assert(db.pointer === wasm.xWrap.testConvertArg('sqlite3*',db));
      // Custom db error message handling via sqlite3_prepare_v2/v3()
      let rc = capi.sqlite3_prepare_v3(db.pointer, {/*invalid*/}, -1, 0, null, null);
      T.assert(capi.SQLITE_MISUSE === rc)
        .assert(0 === capi.sqlite3_errmsg(db.pointer).indexOf("Invalid SQL"))
        .assert(dbFile === db.dbFilename())
        .assert(!db.dbFilename('nope'));
      //Sanity check DB.checkRc()...
      let ex;
      try{db.checkRc(rc)}
      catch(e){ex = e}
      T.assert(ex instanceof sqlite3.SQLite3Error)
        .assert(capi.SQLITE_MISUSE===ex.resultCode)
        .assert(0===ex.message.indexOf("SQLITE_MISUSE: sqlite3 result code"))
        .assert(ex.message.indexOf("Invalid SQL")>0);
      T.assert(db === db.checkRc(0))
        .assert(db === sqlite3.oo1.DB.checkRc(db,0))
        .assert(null === sqlite3.oo1.DB.checkRc(null,0));

      this.progressHandlerCount = 0;
      capi.sqlite3_progress_handler(db, 5, (p)=>{
        ++this.progressHandlerCount;
        return 0;
      }, 0);
    })
  ////////////////////////////////////////////////////////////////////
    .t('sqlite3_db_config() and sqlite3_db_status()', function(sqlite3){
      let rc = capi.sqlite3_db_config(this.db, capi.SQLITE_DBCONFIG_LEGACY_ALTER_TABLE, 0, 0);
      T.assert(0===rc);
      rc = capi.sqlite3_db_config(this.db, capi.SQLITE_DBCONFIG_MAX+1, 0);
      T.assert(capi.SQLITE_MISUSE === rc);
      rc = capi.sqlite3_db_config(this.db, capi.SQLITE_DBCONFIG_MAINDBNAME, "main");
      T.assert(0 === rc);
      const stack = wasm.pstack.pointer;
      try {
        const [pCur, pHi] = wasm.pstack.allocChunks(2,'i64');
        wasm.poke32([pCur, pHi], 0);
        let [vCur, vHi] = wasm.peek32(pCur, pHi);
        T.assert(0===vCur).assert(0===vHi);
        rc = capi.sqlite3_status(capi.SQLITE_STATUS_MEMORY_USED,
                                 pCur, pHi, 0);
        [vCur, vHi] = wasm.peek32(pCur, pHi);
        //console.warn("i32 vCur,vHi",vCur,vHi);
        T.assert(0 === rc).assert(vCur > 0).assert(vHi >= vCur);
        if(wasm.bigIntEnabled){
          // Again in 64-bit. Recall that pCur and pHi are allocated
          // large enough to account for this re-use.
          wasm.poke64([pCur, pHi], 0);
          rc = capi.sqlite3_status64(capi.SQLITE_STATUS_MEMORY_USED,
                                     pCur, pHi, 0);
          [vCur, vHi] = wasm.peek64([pCur, pHi]);
          //console.warn("i64 vCur,vHi",vCur,vHi);
          T.assert(0 === rc).assert(vCur > 0).assert(vHi >= vCur);
        }
      }finally{
        wasm.pstack.restore(stack);
      }
    })

  ////////////////////////////////////////////////////////////////////
    .t('DB.Stmt', function(sqlite3){
      let st = this.db.prepare(
        new TextEncoder('utf-8').encode("select 3 as a")
      );
      //debug("statement =",st);
      this.progressHandlerCount = 0;
      let rc;
      try {
        T.assert(wasm.isPtr(st.pointer))
          .mustThrowMatching(()=>st.pointer=1, /read-only/)
          .assert(1===this.db.openStatementCount())
          .assert(
            capi.sqlite3_stmt_status(
              st, capi.SQLITE_STMTSTATUS_RUN, 0
            ) === 0)
          .assert(!st._mayGet)
          .assert('a' === st.getColumnName(0))
          .mustThrowMatching(()=>st.columnCount=2,
                             /columnCount property is read-only/)
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
          .assert(
            capi.sqlite3_stmt_status(
              st, capi.SQLITE_STMTSTATUS_RUN, 0
            ) > 0);

        T.assert(this.progressHandlerCount > 0,
                 "Expecting progress callback.").
          assert(0===capi.sqlite3_strglob("*.txt", "foo.txt")).
          assert(0!==capi.sqlite3_strglob("*.txt", "foo.xtx")).
          assert(0===capi.sqlite3_strlike("%.txt", "foo.txt", 0)).
          assert(0!==capi.sqlite3_strlike("%.txt", "foo.xtx", 0));
      }finally{
        rc = st.finalize();
      }
      T.assert(!st.pointer)
        .assert(0===this.db.openStatementCount())
        .assert(0===rc);

      T.mustThrowMatching(()=>new sqlite3.oo1.Stmt("hi"), function(err){
        return (err instanceof sqlite3.SQLite3Error)
          && capi.SQLITE_MISUSE === err.resultCode
          && 0 < err.message.indexOf("Do not call the Stmt constructor directly.")
      });
    })

  ////////////////////////////////////////////////////////////////////////
    .t('sqlite3_js_...()', function(){
      const db = this.db;
      if(1){
        const vfsList = capi.sqlite3_js_vfs_list();
        T.assert(vfsList.length>1);
        wasm.scopedAllocCall(()=>{
          const vfsArg = (v)=>wasm.xWrap.testConvertArg('sqlite3_vfs*',v);
          for(const v of vfsList){
            T.assert('string' === typeof v);
            const pVfs = capi.sqlite3_vfs_find(v);
            T.assert(wasm.isPtr(pVfs))
              .assert(pVfs===vfsArg(v));
            const vfs = new capi.sqlite3_vfs(pVfs);
            try { T.assert(vfsArg(vfs)===pVfs) }
            finally{ vfs.dispose() }
          }
        });
      }
      /**
         Trivia: the magic db name ":memory:" does not actually use the
         "memdb" VFS unless "memdb" is _explicitly_ provided as the VFS
         name. Instead, it uses the default VFS with an in-memory btree.
         Thus this.db's VFS may not be memdb even though it's an in-memory
         db.
      */
      const pVfsMem = capi.sqlite3_vfs_find('memdb'),
            pVfsDflt = capi.sqlite3_vfs_find(0),
            pVfsDb = capi.sqlite3_js_db_vfs(db.pointer);
      T.assert(pVfsMem > 0)
        .assert(pVfsDflt > 0)
        .assert(pVfsDb > 0)
        .assert(pVfsMem !== pVfsDflt
                /* memdb lives on top of the default vfs */)
        .assert(pVfsDb === pVfsDflt || pVfsdb === pVfsMem)
      ;
      /*const vMem = new capi.sqlite3_vfs(pVfsMem),
        vDflt = new capi.sqlite3_vfs(pVfsDflt),
        vDb = new capi.sqlite3_vfs(pVfsDb);*/
      const duv = capi.sqlite3_js_db_uses_vfs;
      T.assert(pVfsDflt === duv(db.pointer, 0)
               || pVfsMem === duv(db.pointer,0))
        .assert(!duv(db.pointer, "foo"))
      ;
    }/*sqlite3_js_...()*/)

  ////////////////////////////////////////////////////////////////////
    .t('Table t', function(sqlite3){
      const db = this.db;
      let list = [];
      this.progressHandlerCount = 0;
      let rc = db.exec({
        sql:['CREATE TABLE t(a,b);',
             // ^^^ using TEMP TABLE breaks the db export test
             "INSERT INTO t(a,b) VALUES(1,2),(3,4),",
             "(?,?)"/*intentionally missing semicolon to test for
                      off-by-one bug in string-to-WASM conversion*/],
        saveSql: list,
        bind: [5,6]
      });
      //debug("Exec'd SQL:", list);
      T.assert(rc === db)
        .assert(2 === list.length)
        .assert('string'===typeof list[1])
        .assert(3===db.changes())
        .assert(this.progressHandlerCount > 0,
                "Expecting progress callback.")
      if(wasm.bigIntEnabled){
        T.assert(3n===db.changes(false,true));
      }
      rc = db.exec({
        sql: "INSERT INTO t(a,b) values('blob',X'6869') RETURNING 13",
        rowMode: 0
      });
      T.assert(Array.isArray(rc))
        .assert(1===rc.length)
        .assert(13 === rc[0])
        .assert(1===db.changes());

      let vals = db.selectValues('select a from t order by a limit 2');
      T.assert( 2 === vals.length )
        .assert( 1===vals[0] && 3===vals[1] );
      vals = db.selectValues('select a from t order by a limit $L',
                             {$L:2}, capi.SQLITE_TEXT);
      T.assert( 2 === vals.length )
        .assert( '1'===vals[0] && '3'===vals[1] );
      vals = undefined;

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
        _myState: 3 /* Accessible from the callback */,
        callback: function(row,stmt){
          ++counter;
          T.assert(
            3 === this._myState
            /* Recall that "this" is the options object. */
          ).assert(
            this.columnNames===colNames
          ).assert(
            this.columnNames[0]==='a' && this.columnNames[1]==='b'
          ).assert(
            (row.a%2 && row.a<6) || 'blob'===row.a
          );
        }
      });
      T.assert(2 === colNames.length)
        .assert('a' === colNames[0])
        .assert(4 === counter)
        .assert(4 === list.length);
      colNames = [];
      db.exec({
        /* Ensure that columnNames is populated for empty result sets. */
        sql: "SELECT a a, b B FROM t WHERE 0",
        columnNames: colNames
      });
      T.assert(2===colNames.length)
        .assert('a'===colNames[0] && 'B'===colNames[1]);
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
      counter = 0;
      let rv = db.exec({
        sql: "SELECT a FROM t",
        callback: ()=>(1===++counter),
      });
      T.assert(db === rv)
        .assert(2===counter,
               "Expecting exec step() loop to stop if callback returns false.");
      /** If exec() is passed neither callback nor returnValue but
          is passed an explicit rowMode then the default returnValue
          is the whole result set, as if an empty resultRows option
          had been passed. */
      rv = db.exec({
        sql: "SELECT -1 UNION ALL SELECT -2 UNION ALL SELECT -3 ORDER BY 1 DESC",
        rowMode: 0
      });
      T.assert(Array.isArray(rv)).assert(3===rv.length)
        .assert(-1===rv[0]).assert(-3===rv[2]);
      rv = db.exec("SELECT 1 WHERE 0",{rowMode: 0});
      T.assert(Array.isArray(rv)).assert(0===rv.length);
      if(wasm.bigIntEnabled && haveWasmCTests()){
        const mI = wasm.xCall('sqlite3_wasm_test_int64_max');
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
        T.assert(0===st.columnCount);
        const ndx = st.getParamIndex(':b');
        T.assert(1===ndx);
        st.bindAsBlob(ndx, "ima blob")
          /*step() skipped intentionally*/.reset(true);
      } finally {
        T.assert(0===st.finalize())
          .assert(undefined===st.finalize());        
      }

      try {
        db.prepare("/*empty SQL*/");
        toss("Must not be reached.");
      }catch(e){
        T.assert(e instanceof sqlite3.SQLite3Error)
          .assert(0==e.message.indexOf('Cannot prepare empty'));
      }

      counter = 0;
      db.exec({
        // Check for https://sqlite.org/forum/forumpost/895425b49a
        sql: "pragma table_info('t')",
        rowMode: 'object',
        callback: function(row){
          ++counter;
          T.assert(row.name==='a' || row.name==='b');
        }
      });
      T.assert(2===counter);
    })/*setup table T*/

  ////////////////////////////////////////////////////////////////////
    .t({
      name: "sqlite3_set_authorizer()",
      test:function(sqlite3){
        T.assert(capi.SQLITE_IGNORE>0)
          .assert(capi.SQLITE_DENY>0);
        const db = this.db;
        const ssa = capi.sqlite3_set_authorizer;
        const n = db.selectValue('select count(*) from t');
        T.assert(n>0);
        let authCount = 0;
        let rc = ssa(db, function(pV, iCode, s0, s1, s2, s3){
          ++authCount;
          return capi.SQLITE_IGNORE;
        }, 0);
        T.assert(0===rc)
          .assert(
            undefined === db.selectValue('select count(*) from t')
            /* Note that the count() never runs, so we get undefined
               instead of 0. */
          )
          .assert(authCount>0);
        authCount = 0;
        db.exec("update t set a=-9999");
        T.assert(authCount>0);
        /* Reminder: we don't use DELETE because, from the C API docs:

          "If the action code is [SQLITE_DELETE] and the callback
          returns [SQLITE_IGNORE] then the [DELETE] operation proceeds
          but the [truncate optimization] is disabled and all rows are
          deleted individually."
        */
        rc = ssa(db, null, 0);
        authCount = 0;
        T.assert(-9999 != db.selectValue('select a from t'))
          .assert(0===authCount);
        rc = ssa(db, function(pV, iCode, s0, s1, s2, s3){
          ++authCount;
          return capi.SQLITE_DENY;
        }, 0);
        T.assert(0===rc);
        let err;
        try{ db.exec("select 1 from t") }
        catch(e){ err = e }
        T.assert(err instanceof sqlite3.SQLite3Error)
          .assert(err.message.indexOf('not authorized'>0))
          .assert(1===authCount);
        authCount = 0;
        rc = ssa(db, function(...args){
          ++authCount;
          return capi.SQLITE_OK;
        }, 0);
        T.assert(0===rc);
        T.assert(n === db.selectValue('select count(*) from t'))
          .assert(authCount>0);
        authCount = 0;
        rc = ssa(db, function(pV, iCode, s0, s1, s2, s3){
          ++authCount;
          throw new Error("Testing catching of authorizer.");
        }, 0);
        T.assert(0===rc);
        authCount = 0;
        err = undefined;
        try{ db.exec("select 1 from t") }
        catch(e){err = e}
        T.assert(err instanceof Error)
          .assert(err.message.indexOf('not authorized')>0)
        /* Note that the thrown message is trumped/overwritten
           by the authorizer process. */
          .assert(1===authCount);
        rc = ssa(db, 0, 0);
        authCount = 0;
        T.assert(0===rc);
        T.assert(n === db.selectValue('select count(*) from t'))
          .assert(0===authCount);
      }
    })/*sqlite3_set_authorizer()*/

  ////////////////////////////////////////////////////////////////////////
    .t("sqlite3_table_column_metadata()", function(sqlite3){
      const stack = wasm.pstack.pointer;
      try{
        const [pzDT, pzColl, pNotNull, pPK, pAuto] =
              wasm.pstack.allocPtr(5);
        const rc = capi.sqlite3_table_column_metadata(
          this.db, "main", "t", "rowid",
          pzDT, pzColl, pNotNull, pPK, pAuto
        );
        T.assert(0===rc)
          .assert("INTEGER"===wasm.cstrToJs(wasm.peekPtr(pzDT)))
          .assert("BINARY"===wasm.cstrToJs(wasm.peekPtr(pzColl)))
          .assert(0===wasm.peek32(pNotNull))
          .assert(1===wasm.peek32(pPK))
          .assert(0===wasm.peek32(pAuto))
      }finally{
        wasm.pstack.restore(stack);
      }
    })

  ////////////////////////////////////////////////////////////////////////
    .t('selectArray/Object()', function(sqlite3){
      const db = this.db;
      let rc = db.selectArray('select a, b from t where a=?', 5);
      T.assert(Array.isArray(rc))
        .assert(2===rc.length)
        .assert(5===rc[0] && 6===rc[1]);
      rc = db.selectArray('select a, b from t where b=-1');
      T.assert(undefined === rc);
      rc = db.selectObject('select a A, b b from t where b=?', 6);
      T.assert(rc && 'object'===typeof rc)
        .assert(5===rc.A)
        .assert(6===rc.b);
      rc = db.selectArray('select a, b from t where b=-1');
      T.assert(undefined === rc);
    })
  ////////////////////////////////////////////////////////////////////////
    .t('selectArrays/Objects()', function(sqlite3){
      const db = this.db;
      const sql = 'select a, b from t where a=? or b=? order by a';
      let rc = db.selectArrays(sql, [1, 4]);
      T.assert(Array.isArray(rc))
        .assert(2===rc.length)
        .assert(2===rc[0].length)
        .assert(1===rc[0][0])
        .assert(2===rc[0][1])
        .assert(3===rc[1][0])
        .assert(4===rc[1][1])
      rc = db.selectArrays(sql, [99,99]);
      T.assert(Array.isArray(rc)).assert(0===rc.length);
      rc = db.selectObjects(sql, [1,4]);
      T.assert(Array.isArray(rc))
        .assert(2===rc.length)
        .assert('object' === typeof rc[1])
        .assert(1===rc[0].a)
        .assert(2===rc[0].b)
        .assert(3===rc[1].a)
        .assert(4===rc[1].b);
    })
  ////////////////////////////////////////////////////////////////////////
    .t('selectArray/Object/Values() via INSERT/UPDATE...RETURNING', function(sqlite3){
      let rc = this.db.selectObject("INSERT INTO t(a,b) VALUES(83,84) RETURNING a as AA");
      T.assert(83===rc.AA);
      rc = this.db.selectArray("UPDATE T set a=85 WHERE a=83 RETURNING b as BB");
      T.assert(Array.isArray(rc)).assert(84===rc[0]);
      //log("select * from t:",this.db.selectObjects("select * from t order by a"));
      rc = this.db.selectValues("UPDATE T set a=a*1 RETURNING a");
      T.assert(Array.isArray(rc))
        .assert(5 === rc.length)
        .assert('number'===typeof rc[0])
        .assert(rc[0]|0 === rc[0] /* is small integer */);
    })
    ////////////////////////////////////////////////////////////////////////
    .t({
      name: 'sqlite3_js_db_export()',
      predicate: ()=>true,
      test: function(sqlite3){
        const db = this.db;
        const xp = capi.sqlite3_js_db_export(db.pointer);
        T.assert(xp instanceof Uint8Array)
          .assert(xp.byteLength>0)
          .assert(0 === xp.byteLength % 512);
        this.dbExport = xp;
      }
    }/*sqlite3_js_db_export()*/)
    .t({
      name: 'sqlite3_js_posix_create_file()',
      predicate: ()=>true,
      test: function(sqlite3){
        const db = this.db;
        const filename = "sqlite3_js_posix_create_file.db";
        capi.sqlite3_js_posix_create_file(filename, this.dbExport);
        delete this.dbExport;
        const db2 = new sqlite3.oo1.DB(filename,'r');
        try {
          const sql = "select count(*) from t";
          const n = db.selectValue(sql);
          T.assert(n>0 && db2.selectValue(sql) === n);
        }finally{
          db2.close();
          wasm.sqlite3_wasm_vfs_unlink(0, filename);
        }
      }
    }/*sqlite3_js_vfs_create_file()*/)

  ////////////////////////////////////////////////////////////////////
    .t({
      name:'Scalar UDFs',
      test: function(sqlite3){
        const db = this.db;
        db.createFunction("foo",(pCx,a,b)=>a+b);
        T.assert(7===db.selectValue("select foo(3,4)")).
          assert(5===db.selectValue("select foo(3,?)",2)).
          assert(5===db.selectValue("select foo(?,?2)",[1,4])).
          assert(5===db.selectValue("select foo($a,$b)",{$a:0,$b:5}));
        db.createFunction("bar", {
          arity: -1,
          xFunc: (pCx,...args)=>{
            T.assert(db.pointer === capi.sqlite3_context_db_handle(pCx));
            let rc = 0;
            for(const v of args) rc += v;
            return rc;
          }
        }).createFunction({
          name: "asis",
          xFunc: (pCx,arg)=>arg
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

        let blobArg = new Uint8Array([0x68, 0x69]);
        let blobRc = db.selectValue(
          "select asis(?1)",
          blobArg.buffer/*confirm that ArrayBuffer is handled as a Uint8Array*/
        );
        T.assert(blobRc instanceof Uint8Array).
          assert(2 === blobRc.length).
          assert(0x68==blobRc[0] && 0x69==blobRc[1]);
        blobRc = db.selectValue("select asis(X'6869')");
        T.assert(blobRc instanceof Uint8Array).
          assert(2 === blobRc.length).
          assert(0x68==blobRc[0] && 0x69==blobRc[1]);

        blobArg = new Int8Array([0x68, 0x69]);
        //debug("blobArg=",blobArg);
        blobRc = db.selectValue("select asis(?1)", blobArg);
        T.assert(blobRc instanceof Uint8Array).
          assert(2 === blobRc.length);
        //debug("blobRc=",blobRc);
        T.assert(0x68==blobRc[0] && 0x69==blobRc[1]);

        let rc = sqlite3.capi.sqlite3_create_function_v2(
          this.db, "foo", 0, -1, 0, 0, 0, 0, 0
        );
        T.assert(
          sqlite3.capi.SQLITE_FORMAT === rc,
          "For invalid eTextRep argument."
        );
        rc = sqlite3.capi.sqlite3_create_function_v2(this.db, "foo", 0);
        T.assert(
          sqlite3.capi.SQLITE_MISUSE === rc,
          "For invalid arg count."
        );

        /* Confirm that we can map and unmap the same function with
           multiple arities... */
        const fCounts = [0,0];
        const fArityCheck = function(pCx){
          return ++fCounts[arguments.length-1];
        };
        //wasm.xWrap.FuncPtrAdapter.debugFuncInstall = true;
        rc = capi.sqlite3_create_function_v2(
          db, "nary", 0, capi.SQLITE_UTF8, 0, fArityCheck, 0, 0, 0
        );
        T.assert( 0===rc );
        rc = capi.sqlite3_create_function_v2(
          db, "nary", 1, capi.SQLITE_UTF8, 0, fArityCheck, 0, 0, 0
        );
        T.assert( 0===rc );
        const sqlFArity0 = "select nary()";
        const sqlFArity1 = "select nary(1)";
        T.assert( 1 === db.selectValue(sqlFArity0) )
          .assert( 1 === fCounts[0] ).assert( 0 === fCounts[1] );
        T.assert( 1 === db.selectValue(sqlFArity1) )
          .assert( 1 === fCounts[0] ).assert( 1 === fCounts[1] );
        capi.sqlite3_create_function_v2(
          db, "nary", 0, capi.SQLITE_UTF8, 0, 0, 0, 0, 0
        );
        T.mustThrowMatching((()=>db.selectValue(sqlFArity0)),
                            (e)=>((e instanceof sqlite3.SQLite3Error)
                                  && e.message.indexOf("wrong number of arguments")>0),
                            "0-arity variant was uninstalled.");
        T.assert( 2 === db.selectValue(sqlFArity1) )
          .assert( 1 === fCounts[0] ).assert( 2 === fCounts[1] );
        capi.sqlite3_create_function_v2(
          db, "nary", 1, capi.SQLITE_UTF8, 0, 0, 0, 0, 0
        );
        T.mustThrowMatching((()=>db.selectValue(sqlFArity1)),
                            (e)=>((e instanceof sqlite3.SQLite3Error)
                                  && e.message.indexOf("no such function")>0),
                            "1-arity variant was uninstalled.");
        //wasm.xWrap.FuncPtrAdapter.debugFuncInstall = false;
      }
    })

  ////////////////////////////////////////////////////////////////////
    .t({
      name: 'Aggregate UDFs',
      //predicate: ()=>false,
      test: function(sqlite3){
        const db = this.db;
        const sjac = capi.sqlite3_js_aggregate_context;
        db.createFunction({
          name: 'summer',
          xStep: (pCtx, n)=>{
            const ac = sjac(pCtx, 4);
            wasm.poke32(ac, wasm.peek32(ac) + Number(n));
          },
          xFinal: (pCtx)=>{
            const ac = sjac(pCtx, 0);
            return ac ? wasm.peek32(ac) : 0;
          }
        });
        let v = db.selectValue([
          "with cte(v) as (",
          "select 3 union all select 5 union all select 7",
          ") select summer(v), summer(v+1) from cte"
          /* ------------------^^^^^^^^^^^ ensures that we're handling
              sqlite3_aggregate_context() properly. */
        ]);
        T.assert(15===v);
        T.mustThrowMatching(()=>db.selectValue("select summer(1,2)"),
                            /wrong number of arguments/);

        db.createFunction({
          name: 'summerN',
          arity: -1,
          xStep: (pCtx, ...args)=>{
            const ac = sjac(pCtx, 4);
            let sum = wasm.peek32(ac);
            for(const v of args) sum += Number(v);
            wasm.poke32(ac, sum);
          },
          xFinal: (pCtx)=>{
            const ac = sjac(pCtx, 0);
            capi.sqlite3_result_int( pCtx, ac ? wasm.peek32(ac) : 0 );
            // xFinal() may either return its value directly or call
            // sqlite3_result_xyz() and return undefined. Both are
            // functionally equivalent.
          }
        });
        T.assert(18===db.selectValue('select summerN(1,8,9), summerN(2,3,4)'));
        T.mustThrowMatching(()=>{
          db.createFunction('nope',{
            xFunc: ()=>{}, xStep: ()=>{}
          });
        }, /scalar or aggregate\?/);
        T.mustThrowMatching(()=>{
          db.createFunction('nope',{xStep: ()=>{}});
        }, /Missing xFinal/);
        T.mustThrowMatching(()=>{
          db.createFunction('nope',{xFinal: ()=>{}});
        }, /Missing xStep/);
        T.mustThrowMatching(()=>{
          db.createFunction('nope',{});
        }, /Missing function-type properties/);
        T.mustThrowMatching(()=>{
          db.createFunction('nope',{xFunc:()=>{}, xDestroy:'nope'});
        }, /xDestroy property must be a function/);
        T.mustThrowMatching(()=>{
          db.createFunction('nope',{xFunc:()=>{}, pApp:'nope'});
        }, /Invalid value for pApp/);
     }
    }/*aggregate UDFs*/)

  ////////////////////////////////////////////////////////////////////////
    .t({
      name: 'Aggregate UDFs (64-bit)',
      predicate: ()=>wasm.bigIntEnabled,
      //predicate: ()=>false,
      test: function(sqlite3){
        const db = this.db;
        const sjac = capi.sqlite3_js_aggregate_context;
        db.createFunction({
          name: 'summer64',
          xStep: (pCtx, n)=>{
            const ac = sjac(pCtx, 8);
            wasm.poke64(ac, wasm.peek64(ac) + BigInt(n));
          },
          xFinal: (pCtx)=>{
            const ac = sjac(pCtx, 0);
            return ac ? wasm.peek64(ac) : 0n;
          }
        });
        let v = db.selectValue([
          "with cte(v) as (",
          "select 9007199254740991 union all select 1 union all select 2",
          ") select summer64(v), summer64(v+1) from cte"
        ]);
        T.assert(9007199254740994n===v);
     }
    }/*aggregate UDFs*/)

  ////////////////////////////////////////////////////////////////////
    .t({
      name: 'Window UDFs',
      //predicate: ()=>false,
      test: function(){
        /* Example window function, table, and results taken from:
           https://sqlite.org/windowfunctions.html#udfwinfunc */
        const db = this.db;
        const sjac = (cx,n=4)=>capi.sqlite3_js_aggregate_context(cx,n);
        const xValueFinal = (pCtx)=>{
          const ac = sjac(pCtx, 0);
          return ac ? wasm.peek32(ac) : 0;
        };
        const xStepInverse = (pCtx, n)=>{
          const ac = sjac(pCtx);
          wasm.poke32(ac, wasm.peek32(ac) + Number(n));
        };
        db.createFunction({
          name: 'winsumint',
          xStep: (pCtx, n)=>xStepInverse(pCtx, n),
          xInverse: (pCtx, n)=>xStepInverse(pCtx, -n),
          xFinal: xValueFinal,
          xValue: xValueFinal
        });
        db.exec([
          "CREATE TEMP TABLE twin(x, y); INSERT INTO twin VALUES",
          "('a', 4),('b', 5),('c', 3),('d', 8),('e', 1)"
        ]);
        let rc = db.exec({
          returnValue: 'resultRows',
          sql:[
            "SELECT x, winsumint(y) OVER (",
            "ORDER BY x ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING",
            ") AS sum_y ",
            "FROM twin ORDER BY x;"
          ]
        });
        T.assert(Array.isArray(rc))
          .assert(5 === rc.length);
        let count = 0;
        for(const row of rc){
          switch(++count){
              case 1: T.assert('a'===row[0] && 9===row[1]); break;
              case 2: T.assert('b'===row[0] && 12===row[1]); break;
              case 3: T.assert('c'===row[0] && 16===row[1]); break;
              case 4: T.assert('d'===row[0] && 12===row[1]); break;
              case 5: T.assert('e'===row[0] && 9===row[1]); break;
              default: toss("Too many rows to window function.");
          }
        }
        const resultRows = [];
        rc = db.exec({
          resultRows,
          returnValue: 'resultRows',
          sql:[
            "SELECT x, winsumint(y) OVER (",
            "ORDER BY x ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING",
            ") AS sum_y ",
            "FROM twin ORDER BY x;"
          ]
        });
        T.assert(rc === resultRows)
          .assert(5 === rc.length);

        rc = db.exec({
          returnValue: 'saveSql',
          sql: "select 1; select 2; -- empty\n; select 3"
        });
        T.assert(Array.isArray(rc))
          .assert(3===rc.length)
          .assert('select 1;' === rc[0])
          .assert('select 2;' === rc[1])
          .assert('-- empty\n; select 3' === rc[2]
                  /* Strange but true. */);
        T.mustThrowMatching(()=>{
          db.exec({sql:'', returnValue: 'nope'});
        }, /^Invalid returnValue/);

        db.exec("DROP TABLE twin");
      }
    }/*window UDFs*/)

  ////////////////////////////////////////////////////////////////////
    .t("ATTACH", function(){
      const db = this.db;
      const resultRows = [];
      db.exec({
        sql:new TextEncoder('utf-8').encode([
          // ^^^ testing string-vs-typedarray handling in exec()
          "attach 'session' as foo;",
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

      /** Demonstrate the JS-simplified form of the sqlite3_exec() callback... */
      let colCount = 0, rowCount = 0;
      let rc = capi.sqlite3_exec(
        db, "select a, a*2 from foo.bar", function(aVals, aNames){
          //console.warn("execCallback(",arguments,")");
          colCount = aVals.length;
          ++rowCount;
          T.assert(2===aVals.length)
            .assert(2===aNames.length)
            .assert(+(aVals[1]) === 2 * +(aVals[0]));
        }, 0, 0
      );
      T.assert(0===rc).assert(3===rowCount).assert(2===colCount);
      rc = capi.sqlite3_exec(
        db.pointer, "select a from foo.bar", ()=>{
          tossQuietly("Testing throwing from exec() callback.");
        }, 0, 0
      );
      T.assert(capi.SQLITE_ABORT === rc);

      /* Demonstrate how to get access to the "full" callback
         signature, as opposed to the simplified JS-specific one... */
      rowCount = colCount = 0;
      const pCb = wasm.installFunction('i(pipp)', function(pVoid,nCols,aVals,aCols){
        /* Tip: wasm.cArgvToJs() can be used to convert aVals and
           aCols to arrays: const vals = wasm.cArgvToJs(nCols,
           aVals); */
        ++rowCount;
        colCount = nCols;
        T.assert(2 === nCols)
          .assert(wasm.isPtr(pVoid))
          .assert(wasm.isPtr(aVals))
          .assert(wasm.isPtr(aCols))
          .assert(+wasm.cstrToJs(wasm.peekPtr(aVals + wasm.ptrSizeof))
                  === 2 * +wasm.cstrToJs(wasm.peekPtr(aVals)));
        return 0;
      });
      try {
        T.assert(wasm.isPtr(pCb));
        rc = capi.sqlite3_exec(
          db, new TextEncoder('utf-8').encode("select a, a*2 from foo.bar"),
          pCb, 0, 0
        );
        T.assert(0===rc)
          .assert(3===rowCount)
          .assert(2===colCount);
      }finally{
        wasm.uninstallFunction(pCb);
      }

      // Demonstrate that an OOM result does not propagate through sqlite3_exec()...
      rc = capi.sqlite3_exec(
        db, ["select a,"," a*2 from foo.bar"], (aVals, aNames)=>{
          sqlite3.WasmAllocError.toss("just testing");
        }, 0, 0
      );
      T.assert(capi.SQLITE_ABORT === rc);

      db.exec("detach foo");
      T.mustThrow(()=>db.exec("select * from foo.bar"),
                  "Because foo is no longer attached.");
    })

  ////////////////////////////////////////////////////////////////////
    .t({
      name: 'C-side WASM tests',
      predicate: ()=>(haveWasmCTests() || "Not compiled in."),
      test: function(){
        const w = wasm, db = this.db;
        const stack = w.scopedAllocPush();
        let ptrInt;
        const origValue = 512;
        try{
          ptrInt = w.scopedAlloc(4);
          w.poke32(ptrInt,origValue);
          const cf = w.xGet('sqlite3_wasm_test_intptr');
          const oldPtrInt = ptrInt;
          T.assert(origValue === w.peek32(ptrInt));
          const rc = cf(ptrInt);
          T.assert(2*origValue === rc).
            assert(rc === w.peek32(ptrInt)).
            assert(oldPtrInt === ptrInt);
          const pi64 = w.scopedAlloc(8)/*ptr to 64-bit integer*/;
          const o64 = 0x010203040506/*>32-bit integer*/;
          if(w.bigIntEnabled){
            w.poke64(pi64, o64);
            //log("pi64 =",pi64, "o64 = 0x",o64.toString(16), o64);
            const v64 = ()=>w.peek64(pi64)
            T.assert(v64() == o64);
            //T.assert(o64 === w.peek64(pi64));
            const cf64w = w.xGet('sqlite3_wasm_test_int64ptr');
            cf64w(pi64);
            T.assert(v64() == BigInt(2 * o64));
            cf64w(pi64);
            T.assert(v64() == BigInt(4 * o64));

            const biTimes2 = w.xGet('sqlite3_wasm_test_int64_times2');
            T.assert(BigInt(2 * o64) ===
                     biTimes2(BigInt(o64)/*explicit conv. required to avoid TypeError
                                           in the call :/ */));

            const pMin = w.scopedAlloc(16);
            const pMax = pMin + 8;
            const g64 = (p)=>w.peek64(p);
            w.poke64([pMin, pMax], 0);
            const minMaxI64 = [
              w.xCall('sqlite3_wasm_test_int64_min'),
              w.xCall('sqlite3_wasm_test_int64_max')
            ];
            T.assert(minMaxI64[0] < BigInt(Number.MIN_SAFE_INTEGER)).
              assert(minMaxI64[1] > BigInt(Number.MAX_SAFE_INTEGER));
            //log("int64_min/max() =",minMaxI64, typeof minMaxI64[0]);
            w.xCall('sqlite3_wasm_test_int64_minmax', pMin, pMax);
            T.assert(g64(pMin) === minMaxI64[0], "int64 mismatch").
              assert(g64(pMax) === minMaxI64[1], "int64 mismatch");
            //log("pMin",g64(pMin), "pMax",g64(pMax));
            w.poke64(pMin, minMaxI64[0]);
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

  ////////////////////////////////////////////////////////////////////////
    .t({
      name: 'virtual table #1: eponymous w/ manual exception handling',
      predicate: ()=>!!capi.sqlite3_index_info,
      test: function(sqlite3){
        const VT = sqlite3.vtab;
        const tmplCols = Object.assign(Object.create(null),{
          A: 0, B: 1
        });
        /**
           The vtab demonstrated here is a JS-ification of
           ext/misc/templatevtab.c.
        */
        const tmplMod = new sqlite3.capi.sqlite3_module();
        T.assert(0===tmplMod.$xUpdate);
        tmplMod.setupModule({
          catchExceptions: false,
          methods: {
            xConnect: function(pDb, pAux, argc, argv, ppVtab, pzErr){
              try{
                const args = wasm.cArgvToJs(argc, argv);
                T.assert(args.length>=3)
                  .assert(args[0] === 'testvtab')
                  .assert(args[1] === 'main')
                  .assert(args[2] === 'testvtab');
                //console.debug("xConnect() args =",args);
                const rc = capi.sqlite3_declare_vtab(
                  pDb, "CREATE TABLE ignored(a,b)"
                );
                if(0===rc){
                  const t = VT.xVtab.create(ppVtab);
                  T.assert(t === VT.xVtab.get(wasm.peekPtr(ppVtab)));
                }
                return rc;
              }catch(e){
                if(!(e instanceof sqlite3.WasmAllocError)){
                  wasm.dealloc(wasm.peekPtr, pzErr);
                  wasm.pokePtr(pzErr, wasm.allocCString(e.message));
                }
                return VT.xError('xConnect',e);
              }
            },
            xCreate: true /* just for testing. Will be removed afterwards. */,
            xDisconnect: function(pVtab){
              try {
                VT.xVtab.unget(pVtab).dispose();
                return 0;
              }catch(e){
                return VT.xError('xDisconnect',e);
              }
            },
            xOpen: function(pVtab, ppCursor){
              try{
                const t = VT.xVtab.get(pVtab),
                      c = VT.xCursor.create(ppCursor);
                T.assert(t instanceof capi.sqlite3_vtab)
                  .assert(c instanceof capi.sqlite3_vtab_cursor);
                c._rowId = 0;
                return 0;
              }catch(e){
                return VT.xError('xOpen',e);
              }
            },
            xClose: function(pCursor){
              try{
                const c = VT.xCursor.unget(pCursor);
                T.assert(c instanceof capi.sqlite3_vtab_cursor)
                  .assert(!VT.xCursor.get(pCursor));
                c.dispose();
                return 0;
              }catch(e){
                return VT.xError('xClose',e);
              }
            },
            xNext: function(pCursor){
              try{
                const c = VT.xCursor.get(pCursor);
                ++c._rowId;
                return 0;
              }catch(e){
                return VT.xError('xNext',e);
              }
            },
            xColumn: function(pCursor, pCtx, iCol){
              try{
                const c = VT.xCursor.get(pCursor);
                switch(iCol){
                    case tmplCols.A:
                      capi.sqlite3_result_int(pCtx, 1000 + c._rowId);
                      break;
                    case tmplCols.B:
                      capi.sqlite3_result_int(pCtx, 2000 + c._rowId);
                      break;
                    default: sqlite3.SQLite3Error.toss("Invalid column id",iCol);
                }
                return 0;
              }catch(e){
                return VT.xError('xColumn',e);
              }
            },
            xRowid: function(pCursor, ppRowid64){
              try{
                const c = VT.xCursor.get(pCursor);
                VT.xRowid(ppRowid64, c._rowId);
                return 0;
              }catch(e){
                return VT.xError('xRowid',e);
              }
            },
            xEof: function(pCursor){
              const c = VT.xCursor.get(pCursor),
                    rc = c._rowId>=10;
              return rc;
            },
            xFilter: function(pCursor, idxNum, idxCStr,
                              argc, argv/* [sqlite3_value* ...] */){
              try{
                const c = VT.xCursor.get(pCursor);
                c._rowId = 0;
                const list = capi.sqlite3_values_to_js(argc, argv);
                T.assert(argc === list.length);
                //log(argc,"xFilter value(s):",list);
                return 0;
              }catch(e){
                return VT.xError('xFilter',e);
              }
            },
            xBestIndex: function(pVtab, pIdxInfo){
              try{
                //const t = VT.xVtab.get(pVtab);
                const sii = capi.sqlite3_index_info;
                const pii = new sii(pIdxInfo);
                pii.$estimatedRows = 10;
                pii.$estimatedCost = 10.0;
                //log("xBestIndex $nConstraint =",pii.$nConstraint);
                if(pii.$nConstraint>0){
                  // Validate nthConstraint() and nthConstraintUsage()
                  const max = pii.$nConstraint;
                  for(let i=0; i < max; ++i ){
                    let v = pii.nthConstraint(i,true);
                    T.assert(wasm.isPtr(v));
                    v = pii.nthConstraint(i);
                    T.assert(v instanceof sii.sqlite3_index_constraint)
                      .assert(v.pointer >= pii.$aConstraint);
                    v.dispose();
                    v = pii.nthConstraintUsage(i,true);
                    T.assert(wasm.isPtr(v));
                    v = pii.nthConstraintUsage(i);
                    T.assert(v instanceof sii.sqlite3_index_constraint_usage)
                      .assert(v.pointer >= pii.$aConstraintUsage);
                    v.$argvIndex = i;//just to get some values into xFilter
                    v.dispose();
                  }
                }
                //log("xBestIndex $nOrderBy =",pii.$nOrderBy);
                if(pii.$nOrderBy>0){
                  // Validate nthOrderBy()
                  const max = pii.$nOrderBy;
                  for(let i=0; i < max; ++i ){
                    let v = pii.nthOrderBy(i,true);
                    T.assert(wasm.isPtr(v));
                    v = pii.nthOrderBy(i);
                    T.assert(v instanceof sii.sqlite3_index_orderby)
                      .assert(v.pointer >= pii.$aOrderBy);
                    v.dispose();
                  }
                }
                pii.dispose();
                return 0;
              }catch(e){
                return VT.xError('xBestIndex',e);
              }
            }
          }
        });
        this.db.onclose.disposeAfter.push(tmplMod);
        T.assert(0===tmplMod.$xUpdate)
          .assert(tmplMod.$xCreate)
          .assert(tmplMod.$xCreate === tmplMod.$xConnect,
                  "setup() must make these equivalent and "+
                  "installMethods() must avoid re-compiling identical functions");
        tmplMod.$xCreate = 0 /* make tmplMod eponymous-only */;
        let rc = capi.sqlite3_create_module(
          this.db, "testvtab", tmplMod, 0
        );
        this.db.checkRc(rc);
        const list = this.db.selectArrays(
          "SELECT a,b FROM testvtab where a<9999 and b>1 order by a, b"
          /* Query is shaped so that it will ensure that some constraints
             end up in xBestIndex(). */
        );
        T.assert(10===list.length)
          .assert(1000===list[0][0])
          .assert(2009===list[list.length-1][1]);
      }
    })/*custom vtab #1*/

  ////////////////////////////////////////////////////////////////////////
    .t({
      name: 'virtual table #2: non-eponymous w/ automated exception wrapping',
      predicate: ()=>!!capi.sqlite3_index_info,
      test: function(sqlite3){
        const VT = sqlite3.vtab;
        const tmplCols = Object.assign(Object.create(null),{
          A: 0, B: 1
        });
        /**
           The vtab demonstrated here is a JS-ification of
           ext/misc/templatevtab.c.
        */
        let throwOnCreate = 1 ? 0 : capi.SQLITE_CANTOPEN
        /* ^^^ just for testing exception wrapping. Note that sqlite
           always translates errors from a vtable to a generic
           SQLITE_ERROR unless it's from xConnect()/xCreate() and that
           callback sets an error string. */;
        const vtabTrace = 1
              ? ()=>{}
              : (methodName,...args)=>console.debug('sqlite3_module::'+methodName+'():',...args);
        const modConfig = {
          /* catchExceptions changes how the methods are wrapped */
          catchExceptions: true,
          name: "vtab2test",
          methods:{
            xCreate: function(pDb, pAux, argc, argv, ppVtab, pzErr){
              vtabTrace("xCreate",...arguments);
              if(throwOnCreate){
                sqlite3.SQLite3Error.toss(
                  throwOnCreate,
                  "Throwing a test exception."
                );
              }
              const args = wasm.cArgvToJs(argc, argv);
              vtabTrace("xCreate","argv:",args);
              T.assert(args.length>=3);
              const rc = capi.sqlite3_declare_vtab(
                pDb, "CREATE TABLE ignored(a,b)"
              );
              if(0===rc){
                const t = VT.xVtab.create(ppVtab);
                T.assert(t === VT.xVtab.get(wasm.peekPtr(ppVtab)));
                vtabTrace("xCreate",...arguments," ppVtab =",t.pointer);
              }
              return rc;
            },
            xConnect: true,
            xDestroy: function(pVtab){
              vtabTrace("xDestroy/xDisconnect",pVtab);
              VT.xVtab.dispose(pVtab);
            },
            xDisconnect: true,
            xOpen: function(pVtab, ppCursor){
              const t = VT.xVtab.get(pVtab),
                    c = VT.xCursor.create(ppCursor);
              T.assert(t instanceof capi.sqlite3_vtab)
                .assert(c instanceof capi.sqlite3_vtab_cursor);
              vtabTrace("xOpen",...arguments," cursor =",c.pointer);
              c._rowId = 0;
            },
            xClose: function(pCursor){
              vtabTrace("xClose",...arguments);
              const c = VT.xCursor.unget(pCursor);
              T.assert(c instanceof capi.sqlite3_vtab_cursor)
                .assert(!VT.xCursor.get(pCursor));
              c.dispose();
            },
            xNext: function(pCursor){
              vtabTrace("xNext",...arguments);
              const c = VT.xCursor.get(pCursor);
              ++c._rowId;
            },
            xColumn: function(pCursor, pCtx, iCol){
              vtabTrace("xColumn",...arguments);
              const c = VT.xCursor.get(pCursor);
              switch(iCol){
                  case tmplCols.A:
                    capi.sqlite3_result_int(pCtx, 1000 + c._rowId);
                    break;
                  case tmplCols.B:
                    capi.sqlite3_result_int(pCtx, 2000 + c._rowId);
                    break;
                  default: sqlite3.SQLite3Error.toss("Invalid column id",iCol);
              }
            },
            xRowid: function(pCursor, ppRowid64){
              vtabTrace("xRowid",...arguments);
              const c = VT.xCursor.get(pCursor);
              VT.xRowid(ppRowid64, c._rowId);
            },
            xEof: function(pCursor){
              vtabTrace("xEof",...arguments);
              return VT.xCursor.get(pCursor)._rowId>=10;
            },
            xFilter: function(pCursor, idxNum, idxCStr,
                              argc, argv/* [sqlite3_value* ...] */){
              vtabTrace("xFilter",...arguments);
              const c = VT.xCursor.get(pCursor);
              c._rowId = 0;
              const list = capi.sqlite3_values_to_js(argc, argv);
              T.assert(argc === list.length);
            },
            xBestIndex: function(pVtab, pIdxInfo){
              vtabTrace("xBestIndex",...arguments);
              //const t = VT.xVtab.get(pVtab);
              const pii = VT.xIndexInfo(pIdxInfo);
              pii.$estimatedRows = 10;
              pii.$estimatedCost = 10.0;
              pii.dispose();
            }
          }/*methods*/
        };
        const tmplMod = VT.setupModule(modConfig);
        T.assert(1===tmplMod.$iVersion);
        this.db.onclose.disposeAfter.push(tmplMod);
        this.db.checkRc(capi.sqlite3_create_module(
          this.db.pointer, modConfig.name, tmplMod.pointer, 0
        ));
        this.db.exec([
          "create virtual table testvtab2 using ",
          modConfig.name,
          "(arg1 blah, arg2 bloop)"
        ]);
        if(0){
          /* If we DROP TABLE then xDestroy() is called. If the
             vtab is instead destroyed when the db is closed,
             xDisconnect() is called. */
          this.db.onclose.disposeBefore.push(function(db){
            console.debug("Explicitly dropping testvtab2 via disposeBefore handler...");
            db.exec(
              /** DROP TABLE is the only way to get xDestroy() to be called. */
              "DROP TABLE testvtab2"
            );
          });
        }
        let list = this.db.selectArrays(
          "SELECT a,b FROM testvtab2 where a<9999 and b>1 order by a, b"
          /* Query is shaped so that it will ensure that some
             constraints end up in xBestIndex(). */
        );
        T.assert(10===list.length)
          .assert(1000===list[0][0])
          .assert(2009===list[list.length-1][1]);

        list = this.db.selectArrays(
          "SELECT a,b FROM testvtab2 where a<9999 and b>1 order by b, a limit 5"
        );
        T.assert(5===list.length)
          .assert(1000===list[0][0])
          .assert(2004===list[list.length-1][1]);

        // Call it as a table-valued function...
        list = this.db.selectArrays([
          "SELECT a,b FROM ", modConfig.name,
          " where a<9999 and b>1 order by b, a limit 1"
        ]);
        T.assert(1===list.length)
          .assert(1000===list[0][0])
          .assert(2000===list[0][1]);
      }
    })/*custom vtab #2*/
  ////////////////////////////////////////////////////////////////////////
    .t('Custom collation', function(sqlite3){
      let collationCounter = 0;
      let myCmp = function(pArg,n1,p1,n2,p2){
        //int (*)(void*,int,const void*,int,const void*)
        ++collationCounter;
        const rc = wasm.exports.sqlite3_strnicmp(p1,p2,(n1<n2?n1:n2));
        return rc ? rc : (n1 - n2);
      };
      let rc = capi.sqlite3_create_collation_v2(this.db, "mycollation", capi.SQLITE_UTF8,
                                                0, myCmp, 0);
      this.db.checkRc(rc);
      rc = this.db.selectValue("select 'hi' = 'HI' collate mycollation");
      T.assert(1===rc).assert(1===collationCounter);
      rc = this.db.selectValue("select 'hii' = 'HI' collate mycollation");
      T.assert(0===rc).assert(2===collationCounter);
      rc = this.db.selectValue("select 'hi' = 'HIi' collate mycollation");
      T.assert(0===rc).assert(3===collationCounter);
      rc = capi.sqlite3_create_collation(this.db,"hi",capi.SQLITE_UTF8/*not enough args*/);
      T.assert(capi.SQLITE_MISUSE === rc);
      rc = capi.sqlite3_create_collation_v2(this.db,"hi",capi.SQLITE_UTF8+1/*invalid encoding*/,0,0,0);
      T.assert(capi.SQLITE_FORMAT === rc)
        .mustThrowMatching(()=>this.db.checkRc(rc),
                           /SQLITE_UTF8 is the only supported encoding./);
      /*
        We need to ensure that replacing that collation function does
        the right thing. We don't have a handle to the underlying WASM
        pointer from here, so cannot verify (without digging through
        internal state) that the old one gets uninstalled, but we can
        verify that a new one properly replaces it.  (That said,
        console.warn() output has shown that the uninstallation does
        happen.)
      */
      collationCounter = 0;
      myCmp = function(pArg,n1,p1,n2,p2){
        --collationCounter;
        return 0;
      };
      rc = capi.sqlite3_create_collation_v2(this.db, "MYCOLLATION", capi.SQLITE_UTF8,
                                            0, myCmp, 0);
      this.db.checkRc(rc);
      rc = this.db.selectValue("select 'hi' = 'HI' collate mycollation");
      T.assert(rc>0).assert(-1===collationCounter);
      rc = this.db.selectValue("select 'a' = 'b' collate mycollation");
      T.assert(rc>0).assert(-2===collationCounter);
      rc = capi.sqlite3_create_collation_v2(this.db, "MYCOLLATION", capi.SQLITE_UTF8,
                                            0, null, 0);
      this.db.checkRc(rc);
      rc = 0;
      try {
        this.db.selectValue("select 'a' = 'b' collate mycollation");
      }catch(e){
        /* Why is e.resultCode not automatically an extended result
           code? The DB() class enables those automatically. */
        rc = sqlite3.capi.sqlite3_extended_errcode(this.db);
      }
      T.assert(capi.SQLITE_ERROR_MISSING_COLLSEQ === rc);
    })/*custom collation*/

  ////////////////////////////////////////////////////////////////////////
    .t('Close db', function(){
      T.assert(this.db).assert(wasm.isPtr(this.db.pointer));
      //wasm.sqlite3_wasm_db_reset(this.db); // will leak virtual tables!
      this.db.close();
      T.assert(!this.db.pointer);
    })
  ;/* end of oo1 checks */

  ////////////////////////////////////////////////////////////////////////
  T.g('kvvfs')
    .t({
      name: 'kvvfs is disabled in worker',
      predicate: ()=>(isWorker() || "test is only valid in a Worker"),
      test: function(sqlite3){
        T.assert(
          !capi.sqlite3_vfs_find('kvvfs'),
          "Expecting kvvfs to be unregistered."
        );
      }
    })
    .t({
      name: 'kvvfs in main thread',
      predicate: ()=>(isUIThread()
                      || "local/sessionStorage are unavailable in a Worker"),
      test: function(sqlite3){
        const filename = this.kvvfsDbFile = 'session';
        const pVfs = capi.sqlite3_vfs_find('kvvfs');
        T.assert(pVfs);
        const JDb = this.JDb = sqlite3.oo1.JsStorageDb;
        const unlink = this.kvvfsUnlink = ()=>{JDb.clearStorage(filename)};
        unlink();
        let db = new JDb(filename);
        try {
          db.exec([
            'create table kvvfs(a);',
            'insert into kvvfs(a) values(1),(2),(3)'
          ]);
          T.assert(3 === db.selectValue('select count(*) from kvvfs'));
          db.close();
          db = new JDb(filename);
          db.exec('insert into kvvfs(a) values(4),(5),(6)');
          T.assert(6 === db.selectValue('select count(*) from kvvfs'));
        }finally{
          db.close();
        }
      }
    }/*kvvfs sanity checks*/)
    .t({
      name: 'kvvfs sqlite3_js_vfs_create_file()',
      predicate: ()=>"kvvfs does not currently support this",
      test: function(sqlite3){
        let db;
        try {
          db = new this.JDb(this.kvvfsDbFile);
          const exp = capi.sqlite3_js_db_export(db);
          db.close();
          this.kvvfsUnlink();
          capi.sqlite3_js_vfs_create_file("kvvfs", this.kvvfsDbFile, exp);
          db = new this.JDb(filename);
          T.assert(6 === db.selectValue('select count(*) from kvvfs'));
        }finally{
          db.close();
          this.kvvfsUnlink();
        }
        delete this.kvvfsDbFile;
        delete this.kvvfsUnlink;
        delete this.JDb;
      }
   }/*kvvfs sqlite3_js_vfs_create_file()*/)
  ;/* end kvvfs tests */

  ////////////////////////////////////////////////////////////////////////
  T.g('Hook APIs')
    .t({
      name: "sqlite3_commit/rollback/update_hook()",
      predicate: ()=>wasm.bigIntEnabled || "Update hook requires int64",
      test: function(sqlite3){
        let countCommit = 0, countRollback = 0;;
        const db = new sqlite3.oo1.DB(':memory:',1 ? 'c' : 'ct');
        let rc = capi.sqlite3_commit_hook(db, (p)=>{
          ++countCommit;
          return (1 === p) ? 0 : capi.SQLITE_ERROR;
        }, 1);
        T.assert( 0 === rc /*void pointer*/ );

        // Commit hook...
        db.exec("BEGIN; SELECT 1; COMMIT");
        T.assert(0 === countCommit,
                 "No-op transactions (mostly) do not trigger commit hook.");
        db.exec("BEGIN EXCLUSIVE; SELECT 1; COMMIT");
        T.assert(1 === countCommit,
                 "But EXCLUSIVE transactions do.");
        db.transaction((d)=>{d.exec("create table t(a)");});
        T.assert(2 === countCommit);

        // Rollback hook:
        rc = capi.sqlite3_rollback_hook(db, (p)=>{
          ++countRollback;
          T.assert( 2 === p );
        }, 2);
        T.assert( 0 === rc /*void pointer*/ );
        T.mustThrowMatching(()=>{
          db.transaction('drop table t',()=>{})
        }, (e)=>{
          return (capi.SQLITE_MISUSE === e.resultCode)
            && ( e.message.indexOf('Invalid argument') > 0 );
        });
        T.assert(0 === countRollback, "Transaction was not started.");
        T.mustThrowMatching(()=>{
          db.transaction('immediate', ()=>{
            sqlite3.SQLite3Error.toss(capi.SQLITE_FULL,'testing rollback hook');
          });
        }, (e)=>{
          return capi.SQLITE_FULL === e.resultCode
        });
        T.assert(1 === countRollback);

        // Update hook...
        const countUpdate = Object.create(null);
        capi.sqlite3_update_hook(db, (p,op,dbName,tbl,rowid)=>{
          T.assert('main' === dbName.toLowerCase())
            .assert('t' === tbl.toLowerCase())
            .assert(3===p)
            .assert('bigint' === typeof rowid);
          switch(op){
              case capi.SQLITE_INSERT:
              case capi.SQLITE_UPDATE:
              case capi.SQLITE_DELETE:
                countUpdate[op] = (countUpdate[op]||0) + 1;
                break;
              default: toss("Unexpected hook operator:",op);
          }
        }, 3);
        db.transaction((d)=>{
          d.exec([
            "insert into t(a) values(1);",
            "update t set a=2;",
            "update t set a=3;",
            "delete from t where a=3"
            // update hook is not called for an unqualified DELETE
          ]);
        });
        T.assert(1 === countRollback)
          .assert(3 === countCommit)
          .assert(1 === countUpdate[capi.SQLITE_INSERT])
          .assert(2 === countUpdate[capi.SQLITE_UPDATE])
          .assert(1 === countUpdate[capi.SQLITE_DELETE]);
        //wasm.xWrap.FuncPtrAdapter.debugFuncInstall = true;
        T.assert(1 === capi.sqlite3_commit_hook(db, 0, 0));
        T.assert(2 === capi.sqlite3_rollback_hook(db, 0, 0));
        T.assert(3 === capi.sqlite3_update_hook(db, 0, 0));
        //wasm.xWrap.FuncPtrAdapter.debugFuncInstall = false;
        db.close();
      }
    })/* commit/rollback/update hooks */
    .t({
      name: "sqlite3_preupdate_hook()",
      predicate: ()=>wasm.bigIntEnabled || "Pre-update hook requires int64",
      test: function(sqlite3){
        const db = new sqlite3.oo1.DB(':memory:', 1 ? 'c' : 'ct');
        const countHook = Object.create(null);
        let rc = capi.sqlite3_preupdate_hook(
          db, function(p, pDb, op, zDb, zTbl, iKey1, iKey2){
            T.assert(9 === p)
              .assert(db.pointer === pDb)
              .assert(1 === capi.sqlite3_preupdate_count(pDb))
              .assert( 0 > capi.sqlite3_preupdate_blobwrite(pDb) );
            countHook[op] = (countHook[op]||0) + 1;
            switch(op){
                case capi.SQLITE_INSERT:
                case capi.SQLITE_UPDATE:
                 T.assert('number' === typeof capi.sqlite3_preupdate_new_js(pDb, 0));
                  break;
                case capi.SQLITE_DELETE:
                 T.assert('number' === typeof capi.sqlite3_preupdate_old_js(pDb, 0));
                  break;
                default: toss("Unexpected hook operator:",op);
            }
          },
          9
        );
        db.transaction((d)=>{
          d.exec([
            "create table t(a);",
            "insert into t(a) values(1);",
            "update t set a=2;",
            "update t set a=3;",
            "delete from t where a=3"
          ]);
        });
        T.assert(1 === countHook[capi.SQLITE_INSERT])
          .assert(2 === countHook[capi.SQLITE_UPDATE])
          .assert(1 === countHook[capi.SQLITE_DELETE]);
        //wasm.xWrap.FuncPtrAdapter.debugFuncInstall = true;
        db.close();
        //wasm.xWrap.FuncPtrAdapter.debugFuncInstall = false;
      }
    })/*pre-update hooks*/
  ;/*end hook API tests*/

  ////////////////////////////////////////////////////////////////////////
  T.g('Auto-extension API')
    .t({
      name: "Auto-extension sanity checks.",
      test: function(sqlite3){
        let counter = 0;
        const fp = wasm.installFunction('i(ppp)', function(pDb,pzErr,pApi){
          ++counter;
          return 0;
        });
        (new sqlite3.oo1.DB()).close();
        T.assert( 0===counter );
        capi.sqlite3_auto_extension(fp);
        (new sqlite3.oo1.DB()).close();
        T.assert( 1===counter );
        (new sqlite3.oo1.DB()).close();
        T.assert( 2===counter );
        capi.sqlite3_cancel_auto_extension(fp);
        wasm.uninstallFunction(fp);
        (new sqlite3.oo1.DB()).close();
        T.assert( 2===counter );
      }
    });

  ////////////////////////////////////////////////////////////////////////
  T.g('Session API')
    .t({
      name: 'Session API sanity checks',
      predicate: ()=>!!capi.sqlite3changegroup_add,
      test: function(sqlite3){
        warn("The session API tests could use some expansion.");
        const db1 = new sqlite3.oo1.DB(), db2 = new sqlite3.oo1.DB();
        const sqlInit = [
          "create table t(rowid INTEGER PRIMARY KEY,a,b); ",
          "insert into t(rowid,a,b) values",
          "(1,'a1','b1'),",
          "(2,'a2','b2'),",
          "(3,'a3','b3');"
        ].join('');
        db1.exec(sqlInit);
        db2.exec(sqlInit);
        T.assert(3 === db1.selectValue("select count(*) from t"))
          .assert('b3' === db1.selectValue('select b from t where rowid=3'));
        const stackPtr = wasm.pstack.pointer;
        try{
          let ppOut = wasm.pstack.allocPtr();
          let rc = capi.sqlite3session_create(db1, "main", ppOut);
          T.assert(0===rc);
          let pSession = wasm.peekPtr(ppOut);
          T.assert(pSession && wasm.isPtr(pSession));
          capi.sqlite3session_table_filter(pSession, (pCtx, tbl)=>{
            T.assert('t' === tbl).assert( 99 === pCtx );
            return 1;
          }, 99);
          db1.exec([
            "update t set b='bTwo' where rowid=2;",
            "update t set a='aThree' where rowid=3;",
            "delete from t where rowid=1;",
            "insert into t(rowid,a,b) values(4,'a4','b4')"
          ]);
          T.assert('bTwo' === db1.selectValue("select b from t where rowid=2"))
            .assert(undefined === db1.selectValue('select a from t where rowid=1'))
            .assert('b4' === db1.selectValue('select b from t where rowid=4'))
            .assert(3 === db1.selectValue('select count(*) from t'));

          const testSessionEnable = false;
          if(testSessionEnable){
            rc = capi.sqlite3session_enable(pSession, 0);
            T.assert( 0 === rc )
              .assert( 0 === capi.sqlite3session_enable(pSession, -1) );
            db1.exec("delete from t where rowid=2;");
            rc = capi.sqlite3session_enable(pSession, 1);
            T.assert( rc > 0 )
              .assert( capi.sqlite3session_enable(pSession, -1) > 0 )
              .assert(undefined === db1.selectValue('select a from t where rowid=2'));
          }else{
            warn("sqlite3session_enable() tests are currently disabled.");
          }
          let db1Count = db1.selectValue("select count(*) from t");
          T.assert( db1Count === (testSessionEnable ? 2 : 3) );

          /* Capture changeset and destroy session. */
          let pnChanges = wasm.pstack.alloc('i32'),
              ppChanges = wasm.pstack.allocPtr();
          rc = capi.sqlite3session_changeset(pSession, pnChanges, ppChanges);
          T.assert( 0 === rc );
          capi.sqlite3session_delete(pSession);
          pSession = 0;
          const pChanges = wasm.peekPtr(ppChanges),
                nChanges = wasm.peek32(pnChanges);
          T.assert( pChanges && wasm.isPtr( pChanges ) )
            .assert( nChanges > 0 );

          /* Revert db1 via an inverted changeset, but keep pChanges
             and nChanges for application to db2. */
          rc = capi.sqlite3changeset_invert( nChanges, pChanges, pnChanges, ppChanges );
          T.assert( 0 === rc );
          rc = capi.sqlite3changeset_apply(
            db1, wasm.peek32(pnChanges), wasm.peekPtr(ppChanges), 0, (pCtx, eConflict, pIter)=>{
              return 1;
            }, 0
          );
          T.assert( 0 === rc );
          wasm.dealloc( wasm.peekPtr(ppChanges) );
          pnChanges = ppChanges = 0;
          T.assert('b2' === db1.selectValue("select b from t where rowid=2"))
            .assert('a1' === db1.selectValue('select a from t where rowid=1'))
            .assert(undefined === db1.selectValue('select b from t where rowid=4'));
          db1Count = db1.selectValue("select count(*) from t");
          T.assert(3 === db1Count);

          /* Apply pre-reverted changeset (pChanges, nChanges) to
             db2... */
          rc = capi.sqlite3changeset_apply(
            db2, nChanges, pChanges, 0, (pCtx, eConflict, pIter)=>{
              return pCtx ? 1 : 0
            }, 1
          );
          wasm.dealloc( pChanges );
          T.assert( 0 === rc )
            .assert( 'b4' === db2.selectValue('select b from t where rowid=4') )
            .assert( 'aThree' === db2.selectValue('select a from t where rowid=3') )
            .assert( undefined === db2.selectValue('select b from t where rowid=1') );
          if(testSessionEnable){
            T.assert( (undefined === db2.selectValue('select b from t where rowid=2')),
                      "But... the session was disabled when rowid=2 was deleted?" );
            log("rowids from db2.t:",db2.selectValues('select rowid from t order by rowid'));
            T.assert( 3 === db2.selectValue('select count(*) from t') );
          }else{
            T.assert( 'bTwo' === db2.selectValue('select b from t where rowid=2') )
              .assert( 3 === db2.selectValue('select count(*) from t') );
          }
        }finally{
          wasm.pstack.restore(stackPtr);
          db1.close();
          db2.close();
        }
      }
    })/*session API sanity tests*/
  ;/*end of session API group*/;

  ////////////////////////////////////////////////////////////////////////
  T.g('OPFS: Origin-Private File System',
      (sqlite3)=>(sqlite3.capi.sqlite3_vfs_find("opfs")
                  || 'requires "opfs" VFS'))
    .t({
      name: 'OPFS db sanity checks',
      test: async function(sqlite3){
        const filename = this.opfsDbFile = '/dir/sqlite3-tester1.db';
        const pVfs = this.opfsVfs = capi.sqlite3_vfs_find('opfs');
        T.assert(pVfs);
        const unlink = this.opfsUnlink =
              (fn=filename)=>{wasm.sqlite3_wasm_vfs_unlink(pVfs,fn)};
        unlink();
        let db = new sqlite3.oo1.OpfsDb(filename);
        try {
          db.exec([
            'create table p(a);',
            'insert into p(a) values(1),(2),(3)'
          ]);
          T.assert(3 === db.selectValue('select count(*) from p'));
          db.close();
          db = new sqlite3.oo1.OpfsDb(filename);
          db.exec('insert into p(a) values(4),(5),(6)');
          T.assert(6 === db.selectValue('select count(*) from p'));
          this.opfsDbExport = capi.sqlite3_js_db_export(db);
          T.assert(this.opfsDbExport instanceof Uint8Array)
            .assert(this.opfsDbExport.byteLength>0
                    && 0===this.opfsDbExport.byteLength % 512);
        }finally{
          db.close();
          unlink();
        }
      }
    }/*OPFS db sanity checks*/)
    .t({
      name: 'OPFS import',
      test: async function(sqlite3){
        let db;
        try {
          const exp = this.opfsDbExport;
          const filename = this.opfsDbFile;
          delete this.opfsDbExport;
          this.opfsImportSize = await sqlite3.oo1.OpfsDb.importDb(filename, exp);
          db = new sqlite3.oo1.OpfsDb(this.opfsDbFile);
          T.assert(6 === db.selectValue('select count(*) from p')).
            assert( this.opfsImportSize == exp.byteLength );
          db.close();
          this.opfsUnlink(filename);
          T.assert(!(await sqlite3.opfs.entryExists(filename)));
          // Try again with a function as an input source:
          let cursor = 0;
          const blockSize = 512, end = exp.byteLength;
          const reader = async function(){
            if(cursor >= exp.byteLength){
              return undefined;
            }
            const rv = exp.subarray(cursor, cursor+blockSize>end ? end : cursor+blockSize);
            cursor += blockSize;
            return rv;
          };
          this.opfsImportSize = await sqlite3.oo1.OpfsDb.importDb(filename, reader);
          db = new sqlite3.oo1.OpfsDb(this.opfsDbFile);
          T.assert(6 === db.selectValue('select count(*) from p')).
            assert( this.opfsImportSize == exp.byteLength );
        }finally{
          if(db) db.close();
        }
      }
    }/*OPFS export/import*/)
    .t({
      name: '(Internal-use) OPFS utility APIs',
      test: async function(sqlite3){
        const filename = this.opfsDbFile;
        const pVfs = this.opfsVfs;
        const unlink = this.opfsUnlink;
        T.assert(filename && pVfs && !!unlink);
        delete this.opfsDbFile;
        delete this.opfsVfs;
        delete this.opfsUnlink;
        /**************************************************************
           ATTENTION CLIENT-SIDE USERS: sqlite3.opfs is NOT intended
           for client-side use. It is only for this project's own
           internal use. Its APIs are subject to change or removal at
           any time.
        ***************************************************************/
        const opfs = sqlite3.opfs;
        const fSize = this.opfsImportSize;
        delete this.opfsImportSize;
        let sh;
        try{
          T.assert(await opfs.entryExists(filename));
          const [dirHandle, filenamePart] = await opfs.getDirForFilename(filename, false);
          const fh = await dirHandle.getFileHandle(filenamePart);
          sh = await fh.createSyncAccessHandle();
          T.assert(fSize === await sh.getSize());
          await sh.close();
          sh = undefined;
          unlink();
          T.assert(!(await opfs.entryExists(filename)));
        }finally{
          if(sh) await sh.close();
          unlink();
        }

        // Some sanity checks of the opfs utility functions...
        const testDir = '/sqlite3-opfs-'+opfs.randomFilename(12);
        const aDir = testDir+'/test/dir';
        T.assert(await opfs.mkdir(aDir), "mkdir failed")
          .assert(await opfs.mkdir(aDir), "mkdir must pass if the dir exists")
          .assert(!(await opfs.unlink(testDir+'/test')), "delete 1 should have failed (dir not empty)")
          .assert((await opfs.unlink(testDir+'/test/dir')), "delete 2 failed")
          .assert(!(await opfs.unlink(testDir+'/test/dir')),
                  "delete 2b should have failed (dir already deleted)")
          .assert((await opfs.unlink(testDir, true)), "delete 3 failed")
          .assert(!(await opfs.entryExists(testDir)),
                  "entryExists(",testDir,") should have failed");
      }
    }/*OPFS util sanity checks*/)
  ;/* end OPFS tests */

  ////////////////////////////////////////////////////////////////////////
  T.g('OPFS SyncAccessHandle Pool VFS',
      (sqlite3)=>(hasOpfs() || "requires OPFS APIs"))
    .t({
      name: 'SAH sanity checks',
      test: async function(sqlite3){
        T.assert(!sqlite3.capi.sqlite3_vfs_find(sahPoolConfig.name))
          .assert(sqlite3.capi.sqlite3_js_vfs_list().indexOf(sahPoolConfig.name) < 0)
        const inst = sqlite3.installOpfsSAHPoolVfs,
              catcher = (e)=>{
                error("Cannot load SAH pool VFS.",
                      "This might not be a problem,",
                      "depending on the environment.");
                return false;
              };
        let u1, u2;
        // Ensure that two immediately-consecutive installations
        // resolve to the same Promise instead of triggering
        // a locking error.
        const P1 = inst(sahPoolConfig).then(u=>u1 = u).catch(catcher),
              P2 = inst(sahPoolConfig).then(u=>u2 = u).catch(catcher);
        await Promise.all([P1, P2]);
        if(!(await P1)) return;
        T.assert(u1 === u2)
          .assert(sahPoolConfig.name === u1.vfsName)
          .assert(sqlite3.capi.sqlite3_vfs_find(sahPoolConfig.name))
          .assert(u1.getCapacity() >= sahPoolConfig.initialCapacity
                  /* If a test fails before we get to nuke the VFS, we
                     can have more than the initial capacity on the next
                     run. */)
          .assert(u1.getCapacity() + 2 === (await u2.addCapacity(2)))
          .assert(2 === (await u2.reduceCapacity(2)))
          .assert(sqlite3.capi.sqlite3_js_vfs_list().indexOf(sahPoolConfig.name) >= 0);

        T.assert(0 === u1.getFileCount());
        const dbName = '/foo.db';
        let db = new u1.OpfsSAHPoolDb(dbName);
        T.assert(db instanceof sqlite3.oo1.DB)
          .assert(1 === u1.getFileCount());
        db.exec([
          'create table t(a);',
          'insert into t(a) values(1),(2),(3)'
        ]);
        T.assert(1 === u1.getFileCount());
        T.assert(3 === db.selectValue('select count(*) from t'));
        db.close();
        T.assert(1 === u1.getFileCount());
        db = new u2.OpfsSAHPoolDb(dbName);
        T.assert(1 === u1.getFileCount());
        db.close();
        const fileNames = u1.getFileNames();
        T.assert(1 === fileNames.length)
          .assert(dbName === fileNames[0])
          .assert(1 === u1.getFileCount())

        if(1){ // test exportFile() and importDb()
          const dbytes = u1.exportFile(dbName);
          T.assert(dbytes.length >= 4096);
          const dbName2 = '/exported.db';
          let nWrote = u1.importDb(dbName2, dbytes);
          T.assert( 2 == u1.getFileCount() )
            .assert( dbytes.byteLength == nWrote );
          let db2 = new u1.OpfsSAHPoolDb(dbName2);
          T.assert(db2 instanceof sqlite3.oo1.DB)
            .assert(3 === db2.selectValue('select count(*) from t'));
          db2.close();
          T.assert(true === u1.unlink(dbName2))
            .assert(false === u1.unlink(dbName2))
            .assert(1 === u1.getFileCount())
            .assert(1 === u1.getFileNames().length);
          // Try again with a function as an input source:
          let cursor = 0;
          const blockSize = 1024, end = dbytes.byteLength;
          const reader = async function(){
            if(cursor >= dbytes.byteLength){
              return undefined;
            }
            const rv = dbytes.subarray(cursor, cursor+blockSize>end ? end : cursor+blockSize);
            cursor += blockSize;
            return rv;
          };
          nWrote = await u1.importDb(dbName2, reader);
          T.assert( 2 == u1.getFileCount() );
          db2 = new u1.OpfsSAHPoolDb(dbName2);
          T.assert(db2 instanceof sqlite3.oo1.DB)
            .assert(3 === db2.selectValue('select count(*) from t'));
          db2.close();
          T.assert(true === u1.unlink(dbName2))
            .assert(dbytes.byteLength == nWrote);
        }

        T.assert(true === u1.unlink(dbName))
          .assert(false === u1.unlink(dbName))
          .assert(0 === u1.getFileCount())
          .assert(0 === u1.getFileNames().length);

        // Demonstrate that two SAH pools can coexist so long as
        // they have different names.
        const conf2 = JSON.parse(JSON.stringify(sahPoolConfig));
        conf2.name += '-test2';
        const POther = await inst(conf2);
        //log("Installed second SAH instance as",conf2.name);
        T.assert(0 === POther.getFileCount())
          .assert(true === await POther.removeVfs());

        if(0){
           /* Enable this block to inspect vfs's contents via the dev
              console or OPFS Explorer browser extension.  The
              following bits will remove them. */
          return;
        }
        T.assert(true === await u2.removeVfs())
          .assert(false === await u1.removeVfs())
          .assert(!sqlite3.capi.sqlite3_vfs_find(sahPoolConfig.name));

        let cErr, u3;
        conf2.$testThrowInInit = new Error("Testing throwing during init.");
        conf2.name = sahPoolConfig.name+'-err';
        const P3 = await inst(conf2).then(u=>u3 = u).catch((e)=>cErr=e);
        T.assert(P3 === conf2.$testThrowInInit)
          .assert(cErr === P3)
          .assert(undefined === u3)
          .assert(!sqlite3.capi.sqlite3_vfs_find(conf2.name));
      }
    }/*OPFS SAH Pool sanity checks*/)

  ////////////////////////////////////////////////////////////////////////
  T.g('Bug Reports')
    .t({
      name: 'Delete via bound parameter in subquery',
      test: function(sqlite3){
        // Testing https://sqlite.org/forum/forumpost/40ce55bdf5
        // with the exception that that post uses "external content"
        // for the FTS index.
        const db = new sqlite3.oo1.DB();//(':memory:','wt');
        db.exec([
          "create virtual table f using fts5 (path);",
          "insert into f(path) values('abc'),('def'),('ghi');"
        ]);
        const fetchEm = ()=> db.exec({
          sql: "SELECT * FROM f order by path",
          rowMode: 'array'
        });
        const dump = function(lbl){
          let rc = fetchEm();
          log((lbl ? (lbl+' results') : ''),rc);
        };
        //dump('Full fts table');
        let rc = fetchEm();
        T.assert(3===rc.length);
        db.exec(`
          delete from f where rowid in (
          select rowid from f where path = :path
           )`,
          {bind: {":path": "def"}}
        );
        //dump('After deleting one entry via subquery');
        rc = fetchEm();
        T.assert(2===rc.length)
          .assert('abcghi'===rc.join(''));
        //log('rc =',rc);
        db.close();
      }
    })
  ;/*end of Bug Reports group*/;

  ////////////////////////////////////////////////////////////////////////
  log("Loading and initializing sqlite3 WASM module...");
  if(0){
    globalThis.sqlite3ApiConfig = {
      debug: ()=>{},
      log: ()=>{},
      warn: ()=>{},
      error: ()=>{}
    }
  }
//#ifnot target=es6-module
  if(!globalThis.sqlite3InitModule && !isUIThread()){
    /* Vanilla worker, as opposed to an ES6 module worker */
    /*
      If sqlite3.js is in a directory other than this script, in order
      to get sqlite3.js to resolve sqlite3.wasm properly, we have to
      explicitly tell it where sqlite3.js is being loaded from. We do
      that by passing the `sqlite3.dir=theDirName` URL argument to
      _this_ script. That URL argument will be seen by the JS/WASM
      loader and it will adjust the sqlite3.wasm path accordingly. If
      sqlite3.js/.wasm are in the same directory as this script then
      that's not needed.

      URL arguments passed as part of the filename via importScripts()
      are simply lost, and such scripts see the globalThis.location of
      _this_ script.
    */
    let sqlite3Js = 'sqlite3.js';
    const urlParams = new URL(globalThis.location.href).searchParams;
    if(urlParams.has('sqlite3.dir')){
      sqlite3Js = urlParams.get('sqlite3.dir') + '/' + sqlite3Js;
    }
    importScripts(sqlite3Js);
  }
//#endif
  globalThis.sqlite3InitModule.__isUnderTest =
    true /* disables certain API-internal cleanup so that we can
            test internal APIs from here */;
  globalThis.sqlite3InitModule({
    print: log,
    printErr: error
  }).then(async function(sqlite3){
    log("Done initializing WASM/JS bits. Running tests...");
    sqlite3.config.warn("Installing sqlite3 bits as global S for local dev/test purposes.");
    globalThis.S = sqlite3;
    /*await sqlite3.installOpfsSAHPoolVfs(sahPoolConfig)
      .then((u)=>log("Loaded",u.vfsName,"VFS"))
      .catch(e=>{
        log("Cannot install OpfsSAHPool.",e);
      });*/
    capi = sqlite3.capi;
    wasm = sqlite3.wasm;
    log("sqlite3 version:",capi.sqlite3_libversion(),
        capi.sqlite3_sourceid());
    if(wasm.bigIntEnabled){
      log("BigInt/int64 support is enabled.");
    }else{
      logClass('warning',"BigInt/int64 support is disabled.");
    }
    if(haveWasmCTests()){
      log("sqlite3_wasm_test_...() APIs are available.");
    }else{
      logClass('warning',"sqlite3_wasm_test_...() APIs unavailable.");
    }
    log("registered vfs list =",capi.sqlite3_js_vfs_list().join(', '));
    TestUtil.runTests(sqlite3);
  });
})(self);
