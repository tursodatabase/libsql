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
  const haveWasmCTests = ()=>{
    return !!wasm.exports.sqlite3_wasm_test_int64_max;
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
          log(TestUtil.separator);
          logClass('group-start',"Group #"+this.number+':',this.name);
          const indent = '    ';
          if(this.predicate && !this.predicate(sqlite3)){
            logClass('warning',indent,
                     "SKIPPING group because predicate says to.");
            return;
          }
          const assertCount = TestUtil.counter;
          const groupState = Object.create(null);
          const skipped = [];
          let runtime = 0, i = 0;
          for(const t of this.tests){
            ++i;
            const n = this.number+"."+i;
              log(indent, n+":", t.name);
            if(t.predicate && !t.predicate(sqlite3)){
              logClass('warning', indent, indent,
                       'SKIPPING because predicate says to');
              skipped.push( n+': '+t.name );
            }else{
              const tc = TestUtil.counter, now = performance.now();
              await t.test.call(groupState, sqlite3);
              const then = performance.now();
              runtime += then - now;
              logClass('faded',indent, indent,
                       TestUtil.counter - tc, 'assertion(s) in',
                       roundMs(then-now),'ms');
            }
          }
          logClass('green',
                   "Group #"+this.number+":",(TestUtil.counter - assertCount),
                   "assertion(s) in",roundMs(runtime),"ms");
          if(skipped.length){
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
          let runtime = 0;
          for(let g of this.testGroups){
            const now = performance.now();
            await g.run(sqlite3);
            runtime += performance.now() - now;
          }
          log(TestUtil.separator);
          logClass(['strong','green'],
                   "Done running tests.",TestUtil.counter,"assertions in",
                   roundMs(runtime),'ms');
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

  ////////////////////////////////////////////////////////////////////
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
      try{ throw new sqlite3.SQLite3Error(capi.SQLITE_SCHEMA) }
      catch(e){ T.assert('SQLITE_SCHEMA' === e.message) }
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
    .t('sqlite3.capi.wasm', function(sqlite3){
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
            assert("hello" === w.cstringToJs(cpy)).
            assert(chr('o') === w.getMemValue(cpy+n-1)).
            assert(0 === w.getMemValue(cpy+n));
          let cStr2 = w.scopedAllocCString("HI!!!");
          rc = w.cstrncpy(cpy, cStr2, 3);
          T.assert(3===rc).
            assert("HI!lo" === w.cstringToJs(cpy)).
            assert(chr('!') === w.getMemValue(cpy+2)).
            assert(chr('l') === w.getMemValue(cpy+3));
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
        const cstr = w.allocCString("hällo, world");
        const n = w.cstrlen(cstr);
        T.assert(13 === n)
          .assert(0===w.getMemValue(cstr+n))
          .assert(chr('d')===w.getMemValue(cstr+n-1));
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
            .assert(0===w.getMemValue(z1,'i32'), 'allocPtr() must zero the targets')
            .assert(0===w.getMemValue(z3,'i32'));
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
            .assert(0===w.getMemValue(cstr+n))
            .assert(chr('d')===w.getMemValue(cstr+n-1));
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
        if(haveWasmCTests()){
          fw = w.xWrap('sqlite3_wasm_test_str_hello', 'utf8:free',['i32']);
          rc = fw(0);
          T.assert('hello'===rc);
          rc = fw(1);
          T.assert(null===rc);

          if(w.bigIntEnabled){
            w.xWrap.resultAdapter('thrice', (v)=>3n*BigInt(v));
            w.xWrap.argAdapter('twice', (v)=>2n*BigInt(v));
            fw = w.xWrap('sqlite3_wasm_test_int64_times2','thrice','twice');
            rc = fw(1);
            T.assert(12n===rc);

            w.scopedAllocCall(function(){
              let pI1 = w.scopedAlloc(8), pI2 = pI1+4;
              w.setMemValue(pI1, 0,'*')(pI2, 0, '*');
              let f = w.xWrap('sqlite3_wasm_test_int64_minmax',undefined,['i64*','i64*']);
              let r1 = w.getMemValue(pI1, 'i64'), r2 = w.getMemValue(pI2, 'i64');
              T.assert(!Number.isSafeInteger(r1)).assert(!Number.isSafeInteger(r2));
            });
          }
        }
      }
    }/*WhWasmUtil*/)

  ////////////////////////////////////////////////////////////////////
    .t('sqlite3.StructBinder (jaccwabyt)', function(sqlite3){
      const S = sqlite3, W = S.capi.wasm;
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
          assert(K.resolveToInstance(k1.pointer)===k1).
          mustThrowMatching(()=>K.resolveToInstance(null,true), /is-not-a my_struct/).
          assert(k1 === StructType.instanceForPointer(k1.pointer)).
          mustThrowMatching(()=>k1.$ro = 1, /read-only/);
        Object.keys(MyStructDef.members).forEach(function(key){
          key = K.memberKey(key);
          T.assert(0 == k1[key],
                   "Expecting allocation to zero the memory "+
                   "for "+key+" but got: "+k1[key]+
                   " from "+k1.memoryDump());
        });
        T.assert('number' === typeof k1.pointer).
          mustThrowMatching(()=>k1.pointer = 1, /pointer/).
          assert(K.instanceForPointer(k1.pointer) === k1);
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
        T.assert(k1.$pP === k2);
        k1.$pP = null/*null is special-cased to 0.*/;
        T.assert(0===k1.$pP);
        let ptr = k1.pointer;
        k1.dispose();
        T.assert(undefined === k1.pointer).
          assert(undefined === K.instanceForPointer(ptr)).
          mustThrowMatching(()=>{k1.$pP=1}, /disposed instance/);
        const k3 = new K();
        ptr = k3.pointer;
        T.assert(k3 === K.instanceForPointer(ptr));
        K.disposeAll();
        T.assert(ptr).
          assert(undefined === k2.pointer).
          assert(undefined === k3.pointer).
          assert(undefined === K.instanceForPointer(ptr));
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
          assert(wts === StructType.instanceForPointer(wts.pointer));
        T.assert(wts.pointer>0).assert(0===wts.$v4).assert(0n===wts.$v8).
          assert(0===wts.$ppV).assert(0===wts.$xFunc).
          assert(WTStruct.instanceForPointer(wts.pointer) === wts);
        const testFunc =
              W.xGet('sqlite3_wasm_test_struct'/*name gets mangled in -O3 builds!*/);
        let counter = 0;
        //log("wts.pointer =",wts.pointer);
        const wtsFunc = function(arg){
          /*log("This from a JS function called from C, "+
              "which itself was called from JS. arg =",arg);*/
          ++counter;
          T.assert(WTStruct.instanceForPointer(arg) === wts);
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
          .assert(autoResolvePtr ? (wts.$ppV === wts) : (wts.$ppV === wts.pointer))
          .assert('string' === typeof wts.memberToJsString('cstr'))
          .assert(wts.memberToJsString('cstr') === wts.memberToJsString('$cstr'))
          .mustThrowMatching(()=>wts.memberToJsString('xFunc'),
                             /Invalid member type signature for C-string/)
        ;
        testFunc(wts.pointer);
        T.assert(2===counter).assert(40 === wts.$v4).assert(80n === wts.$v8)
          .assert(autoResolvePtr ? (wts.$ppV === wts) : (wts.$ppV === wts.pointer));
        /** The 3rd call to wtsFunc throw from JS, which is called
            from C, which is called from JS. Let's ensure that
            that exception propagates back here... */
        T.mustThrowMatching(()=>testFunc(wts.pointer),/^Testing/);
        W.uninstallFunction(wts.$xFunc);
        wts.$xFunc = 0;
        if(autoResolvePtr){
          wts.$ppV = 0;
          T.assert(!wts.$ppV);
          //WTStruct.debugFlags(0x03);
          wts.$ppV = wts;
          T.assert(wts === wts.$ppV)
          //WTStruct.debugFlags(0);
        }
        wts.setMemberCString('cstr', "A C-string.");
        T.assert(Array.isArray(wts.ondispose)).
          assert(wts.ondispose[0] === wts.$cstr).
          assert('A C-string.' === wts.memberToJsString('cstr'));
        const ptr = wts.pointer;
        wts.dispose();
        T.assert(ptr).assert(undefined === wts.pointer).
          assert(undefined === WTStruct.instanceForPointer(ptr))
      }finally{
        wts.dispose();
      }
    }/*StructBinder*/)

  ////////////////////////////////////////////////////////////////////
    .t('sqlite3.StructBinder part 2', function(sqlite3){
      // https://www.sqlite.org/c3ref/vfs.html
      // https://www.sqlite.org/c3ref/io_methods.html
      const W = wasm;
      const sqlite3_io_methods = capi.sqlite3_io_methods,
            sqlite3_vfs = capi.sqlite3_vfs,
            sqlite3_file = capi.sqlite3_file;
      //log("struct sqlite3_file", sqlite3_file.memberKeys());
      //log("struct sqlite3_vfs", sqlite3_vfs.memberKeys());
      //log("struct sqlite3_io_methods", sqlite3_io_methods.memberKeys());
      const installMethod = function callee(tgt, name, func){
        if(1===arguments.length){
          return (n,f)=>callee(tgt,n,f);
        }
        if(!callee.argcProxy){
          callee.argcProxy = function(func,sig){
            return function(...args){
              if(func.length!==arguments.length){
                toss("Argument mismatch. Native signature is:",sig);
              }
              return func.apply(this, args);
            }
          };
          callee.ondisposeRemoveFunc = function(){
            if(this.__ondispose){
              const who = this;
              this.__ondispose.forEach(
                (v)=>{
                  if('number'===typeof v){
                    try{capi.wasm.uninstallFunction(v)}
                    catch(e){/*ignore*/}
                  }else{/*wasm function wrapper property*/
                    delete who[v];
                  }
                }
              );
              delete this.__ondispose;
            }
          };
        }/*static init*/
        const sigN = tgt.memberSignature(name),
              memKey = tgt.memberKey(name);
        //log("installMethod",tgt, name, sigN);
        if(!tgt.__ondispose){
          T.assert(undefined === tgt.ondispose);
          tgt.ondispose = [callee.ondisposeRemoveFunc];
          tgt.__ondispose = [];
        }
        const fProxy = callee.argcProxy(func, sigN);
        const pFunc = capi.wasm.installFunction(fProxy, tgt.memberSignature(name, true));
        tgt[memKey] = pFunc;
        /**
           ACHTUNG: function pointer IDs are from a different pool than
           allocation IDs, starting at 1 and incrementing in steps of 1,
           so if we set tgt[memKey] to those values, we'd very likely
           later misinterpret them as plain old pointer addresses unless
           unless we use some silly heuristic like "all values <5k are
           presumably function pointers," or actually perform a function
           lookup on every pointer to first see if it's a function. That
           would likely work just fine, but would be kludgy.

           It turns out that "all values less than X are functions" is
           essentially how it works in wasm: a function pointer is
           reported to the client as its index into the
           __indirect_function_table.

           So... once jaccwabyt can be told how to access the
           function table, it could consider all pointer values less
           than that table's size to be functions.  As "real" pointer
           values start much, much higher than the function table size,
           that would likely work reasonably well. e.g. the object
           pointer address for sqlite3's default VFS is (in this local
           setup) 65104, whereas the function table has fewer than 600
           entries.
        */
        const wrapperKey = '$'+memKey;
        tgt[wrapperKey] = fProxy;
        tgt.__ondispose.push(pFunc, wrapperKey);
        //log("tgt.__ondispose =",tgt.__ondispose);
        return (n,f)=>callee(tgt, n, f);
      }/*installMethod*/;

      const installIOMethods = function instm(iom){
        (iom instanceof capi.sqlite3_io_methods) || toss("Invalid argument type.");
        if(!instm._requireFileArg){
          instm._requireFileArg = function(arg,methodName){
            arg = capi.sqlite3_file.resolveToInstance(arg);
            if(!arg){
              err("sqlite3_io_methods::xClose() was passed a non-sqlite3_file.");
            }
            return arg;
          };
          instm._methods = {
            // https://sqlite.org/c3ref/io_methods.html
            xClose: /*i(P)*/function(f){
              /* int (*xClose)(sqlite3_file*) */
              log("xClose(",f,")");
              if(!(f = instm._requireFileArg(f,'xClose'))) return capi.SQLITE_MISUSE;
              f.dispose(/*noting that f has externally-owned memory*/);
              return 0;
            },
            xRead: /*i(Ppij)*/function(f,dest,n,offset){
              /* int (*xRead)(sqlite3_file*, void*, int iAmt, sqlite3_int64 iOfst) */
              log("xRead(",arguments,")");
              if(!(f = instm._requireFileArg(f))) return capi.SQLITE_MISUSE;
              capi.wasm.heap8().fill(0, dest + offset, n);
              return 0;
            },
            xWrite: /*i(Ppij)*/function(f,dest,n,offset){
              /* int (*xWrite)(sqlite3_file*, const void*, int iAmt, sqlite3_int64 iOfst) */
              log("xWrite(",arguments,")");
              if(!(f=instm._requireFileArg(f,'xWrite'))) return capi.SQLITE_MISUSE;
              return 0;
            },
            xTruncate: /*i(Pj)*/function(f){
              /* int (*xTruncate)(sqlite3_file*, sqlite3_int64 size) */
              log("xTruncate(",arguments,")");
              if(!(f=instm._requireFileArg(f,'xTruncate'))) return capi.SQLITE_MISUSE;
              return 0;
            },
            xSync: /*i(Pi)*/function(f){
              /* int (*xSync)(sqlite3_file*, int flags) */
              log("xSync(",arguments,")");
              if(!(f=instm._requireFileArg(f,'xSync'))) return capi.SQLITE_MISUSE;
              return 0;
            },
            xFileSize: /*i(Pp)*/function(f,pSz){
              /* int (*xFileSize)(sqlite3_file*, sqlite3_int64 *pSize) */
              log("xFileSize(",arguments,")");
              if(!(f=instm._requireFileArg(f,'xFileSize'))) return capi.SQLITE_MISUSE;
              capi.wasm.setMemValue(pSz, 0/*file size*/);
              return 0;
            },
            xLock: /*i(Pi)*/function(f){
              /* int (*xLock)(sqlite3_file*, int) */
              log("xLock(",arguments,")");
              if(!(f=instm._requireFileArg(f,'xLock'))) return capi.SQLITE_MISUSE;
              return 0;
            },
            xUnlock: /*i(Pi)*/function(f){
              /* int (*xUnlock)(sqlite3_file*, int) */
              log("xUnlock(",arguments,")");
              if(!(f=instm._requireFileArg(f,'xUnlock'))) return capi.SQLITE_MISUSE;
              return 0;
            },
            xCheckReservedLock: /*i(Pp)*/function(){
              /* int (*xCheckReservedLock)(sqlite3_file*, int *pResOut) */
              log("xCheckReservedLock(",arguments,")");
              return 0;
            },
            xFileControl: /*i(Pip)*/function(){
              /* int (*xFileControl)(sqlite3_file*, int op, void *pArg) */
              log("xFileControl(",arguments,")");
              return capi.SQLITE_NOTFOUND;
            },
            xSectorSize: /*i(P)*/function(){
              /* int (*xSectorSize)(sqlite3_file*) */
              log("xSectorSize(",arguments,")");
              return 0/*???*/;
            },
            xDeviceCharacteristics:/*i(P)*/function(){
              /* int (*xDeviceCharacteristics)(sqlite3_file*) */
              log("xDeviceCharacteristics(",arguments,")");
              return 0;
            }
          };
        }/*static init*/
        iom.$iVersion = 1;
        Object.keys(instm._methods).forEach(
          (k)=>installMethod(iom, k, instm._methods[k])
        );
      }/*installIOMethods()*/;

      const iom = new sqlite3_io_methods, sfile = new sqlite3_file;
      const err = console.error.bind(console);
      try {
        const IOM = sqlite3_io_methods, S3F = sqlite3_file;
        //log("iom proto",iom,iom.constructor.prototype);
        //log("sfile",sfile,sfile.constructor.prototype);
        T.assert(0===sfile.$pMethods).assert(iom.pointer > 0);
        //log("iom",iom);
        sfile.$pMethods = iom.pointer;
        T.assert(iom.pointer === sfile.$pMethods)
          .assert(IOM.resolveToInstance(iom))
          .assert(undefined ===IOM.resolveToInstance(sfile))
          .mustThrow(()=>IOM.resolveToInstance(0,true))
          .assert(S3F.resolveToInstance(sfile.pointer))
          .assert(undefined===S3F.resolveToInstance(iom))
          .assert(iom===IOM.resolveToInstance(sfile.$pMethods));
        T.assert(0===iom.$iVersion);
        installIOMethods(iom);
        T.assert(1===iom.$iVersion);
        //log("iom.__ondispose",iom.__ondispose);
        T.assert(Array.isArray(iom.__ondispose)).assert(iom.__ondispose.length>10);
      }finally{
        iom.dispose();
        T.assert(undefined === iom.__ondispose);
      }

      const dVfs = new sqlite3_vfs(capi.sqlite3_vfs_find(null));
      try {
        const SB = sqlite3.StructBinder;
        T.assert(dVfs instanceof SB.StructType)
          .assert(dVfs.pointer)
          .assert('sqlite3_vfs' === dVfs.structName)
          .assert(!!dVfs.structInfo)
          .assert(SB.StructType.hasExternalPointer(dVfs))
          .assert(dVfs.$iVersion>0)
          .assert('number'===typeof dVfs.$zName)
          .assert('number'===typeof dVfs.$xSleep)
          .assert(capi.wasm.functionEntry(dVfs.$xOpen))
          .assert(dVfs.memberIsString('zName'))
          .assert(dVfs.memberIsString('$zName'))
          .assert(!dVfs.memberIsString('pAppData'))
          .mustThrowMatching(()=>dVfs.memberToJsString('xSleep'),
                             /Invalid member type signature for C-string/)
          .mustThrowMatching(()=>dVfs.memberSignature('nope'), /nope is not a mapped/)
          .assert('string' === typeof dVfs.memberToJsString('zName'))
          .assert(dVfs.memberToJsString('zName')===dVfs.memberToJsString('$zName'))
        ;
        //log("Default VFS: @",dVfs.pointer);
        Object.keys(sqlite3_vfs.structInfo.members).forEach(function(mname){
          const mk = sqlite3_vfs.memberKey(mname), mbr = sqlite3_vfs.structInfo.members[mname],
                addr = dVfs[mk], prefix = 'defaultVfs.'+mname;
          if(1===mbr.signature.length){
            let sep = '?', val = undefined;
            switch(mbr.signature[0]){
                  // TODO: move this into an accessor, e.g. getPreferredValue(member)
                case 'i': case 'j': case 'f': case 'd': sep = '='; val = dVfs[mk]; break
                case 'p': case 'P': sep = '@'; val = dVfs[mk]; break;
                case 's': sep = '=';
                  val = dVfs.memberToJsString(mname);
                  break;
            }
            //log(prefix, sep, val);
          }else{
            //log(prefix," = funcptr @",addr, capi.wasm.functionEntry(addr));
          }
        });
      }finally{
        dVfs.dispose();
        T.assert(undefined===dVfs.pointer);
      }
    }/*StructBinder part 2*/)
  
  ////////////////////////////////////////////////////////////////////
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

  ////////////////////////////////////////////////////////////////////
  ;/*end of C/WASM utils checks*/

  ////////////////////////////////////////////////////////////////////////
  T.g('sqlite3.oo1')
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

  ////////////////////////////////////////////////////////////////////
    .t('DB.Stmt', function(S){
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

  ////////////////////////////////////////////////////////////////////////
    .t('sqlite3_js_...()', function(){
      const db = this.db;
      if(1){
        const vfsList = capi.sqlite3_js_vfs_list();
        T.assert(vfsList.length>1);
        T.assert('string'===typeof vfsList[0]);
        //log("vfsList =",vfsList);
        for(const v of vfsList){
          T.assert('string' === typeof v)
            .assert(capi.sqlite3_vfs_find(v) > 0);
        }
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
        .assert(':memory:' === db.filename)
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
      if(capi.wasm.bigIntEnabled && haveWasmCTests()){
        const mI = capi.wasm.xCall('sqlite3_wasm_test_int64_max');
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
  ////////////////////////////////////////////////////////////////////////
    .t('sqlite3_js_db_export()', function(){
      const db = this.db;
      const xp = capi.sqlite3_js_db_export(db.pointer);
      T.assert(xp instanceof Uint8Array)
        .assert(xp.byteLength>0)
        .assert(0 === xp.byteLength % 512);
    }/*sqlite3_js_db_export()*/)

  ////////////////////////////////////////////////////////////////////
    .t('Scalar UDFs', function(sqlite3){
      const db = this.db;
      db.createFunction("foo",(pCx,a,b)=>a+b);
      T.assert(7===db.selectValue("select foo(3,4)")).
        assert(5===db.selectValue("select foo(3,?)",2)).
        assert(5===db.selectValue("select foo(?,?2)",[1,4])).
        assert(5===db.selectValue("select foo($a,$b)",{$a:0,$b:5}));
      db.createFunction("bar", {
        arity: -1,
        xFunc: (pCx,...args)=>{
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

  ////////////////////////////////////////////////////////////////////
    .t({
      name: 'Aggregate UDFs',
      test: function(sqlite3){
        const db = this.db;
        const sjac = capi.sqlite3_js_aggregate_context;
        db.createFunction({
          name: 'summer',
          xStep: (pCtx, n)=>{
            const ac = sjac(pCtx, 4);
            wasm.setMemValue(ac, wasm.getMemValue(ac,'i32') + Number(n), 'i32');
          },
          xFinal: (pCtx)=>{
            const ac = sjac(pCtx, 0);
            return ac ? wasm.getMemValue(ac,'i32') : 0;
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
            let sum = wasm.getMemValue(ac, 'i32');
            for(const v of args) sum += Number(v);
            wasm.setMemValue(ac, sum, 'i32');
          },
          xFinal: (pCtx)=>{
            const ac = sjac(pCtx, 0);
            capi.sqlite3_result_int( pCtx, ac ? wasm.getMemValue(ac,'i32') : 0 );
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
      test: function(sqlite3){
        const db = this.db;
        const sjac = capi.sqlite3_js_aggregate_context;
        db.createFunction({
          name: 'summer64',
          xStep: (pCtx, n)=>{
            const ac = sjac(pCtx, 8);
            wasm.setMemValue(ac, wasm.getMemValue(ac,'i64') + BigInt(n), 'i64');
          },
          xFinal: (pCtx)=>{
            const ac = sjac(pCtx, 0);
            return ac ? wasm.getMemValue(ac,'i64') : 0n;
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
      test: function(){
        /* Example window function, table, and results taken from:
           https://sqlite.org/windowfunctions.html#udfwinfunc */
        const db = this.db;
        const sjac = (cx,n=4)=>capi.sqlite3_js_aggregate_context(cx,n);
        const xValueFinal = (pCtx)=>{
          const ac = sjac(pCtx, 0);
          return ac ? wasm.getMemValue(ac,'i32') : 0;
        };
        const xStepInverse = (pCtx, n)=>{
          const ac = sjac(pCtx);
          wasm.setMemValue(ac, wasm.getMemValue(ac,'i32') + Number(n), 'i32');
        };
        db.createFunction({
          name: 'winsumint',
          xStep: (pCtx, n)=>xStepInverse(pCtx, n),
          xInverse: (pCtx, n)=>xStepInverse(pCtx, -n),
          xFinal: xValueFinal,
          xValue: xValueFinal
        });
        db.exec([
          "CREATE TABLE twin(x, y); INSERT INTO twin VALUES",
          "('a', 4),('b', 5),('c', 3),('d', 8),('e', 1)"
        ]);
        let count = 0;
        db.exec({
          sql:[
            "SELECT x, winsumint(y) OVER (",
            "ORDER BY x ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING",
            ") AS sum_y ",
            "FROM twin ORDER BY x;",
            "DROP TABLE twin;"
          ],
          callback: function(row){
            switch(++count){
                case 1: T.assert('a'===row[0] && 9===row[1]); break;
                case 2: T.assert('b'===row[0] && 12===row[1]); break;
                case 3: T.assert('c'===row[0] && 16===row[1]); break;
                case 4: T.assert('d'===row[0] && 12===row[1]); break;
                case 5: T.assert('e'===row[0] && 9===row[1]); break;
                default: toss("Too many rows to window function.");
            }
          }
        });
        T.assert(5 === count);
      }
    }/*window UDFs*/)

  ////////////////////////////////////////////////////////////////////
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

  ////////////////////////////////////////////////////////////////////
    .t({
      name: 'C-side WASM tests (if compiled in)',
      predicate: haveWasmCTests,
      test: function(){
        const w = wasm, db = this.db;
        const stack = w.scopedAllocPush();
        let ptrInt;
        const origValue = 512;
        const ptrValType = 'i32';
        try{
          ptrInt = w.scopedAlloc(4);
          w.setMemValue(ptrInt,origValue, ptrValType);
          const cf = w.xGet('sqlite3_wasm_test_intptr');
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
            const cf64w = w.xGet('sqlite3_wasm_test_int64ptr');
            cf64w(pi64);
            //log("getMemValue(pi64)",v64());
            T.assert(v64() == BigInt(2 * o64));
            cf64w(pi64);
            T.assert(v64() == BigInt(4 * o64));

            const biTimes2 = w.xGet('sqlite3_wasm_test_int64_times2');
            T.assert(BigInt(2 * o64) ===
                     biTimes2(BigInt(o64)/*explicit conv. required to avoid TypeError
                                           in the call :/ */));

            const pMin = w.scopedAlloc(16);
            const pMax = pMin + 8;
            const g64 = (p)=>w.getMemValue(p,ptrType64);
            w.setMemValue(pMin, 0, ptrType64);
            w.setMemValue(pMax, 0, ptrType64);
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
  T.g('kvvfs (Worker thread only)', isWorker)
    .t({
      name: 'kvvfs is disabled',
      test: ()=>{
        T.assert(
          !capi.sqlite3_vfs_find('kvvfs'),
          "Expecting kvvfs to be unregistered."
        );
      }
    });
  T.g('kvvfs (UI thread only)', isUIThread)
    .t({
      name: 'kvvfs sanity checks',
      test: function(sqlite3){
        const filename = 'session';
        const pVfs = capi.sqlite3_vfs_find('kvvfs');
        T.assert(pVfs);
        const JDb = sqlite3.oo1.JsStorageDb;
        const unlink = ()=>JDb.clearStorage(filename);
        unlink();
        let db = new JDb(filename);
        db.exec([
          'create table kvvfs(a);',
          'insert into kvvfs(a) values(1),(2),(3)'
        ]);
        T.assert(3 === db.selectValue('select count(*) from kvvfs'));
        db.close();
        db = new JDb(filename);
        db.exec('insert into kvvfs(a) values(4),(5),(6)');
        T.assert(6 === db.selectValue('select count(*) from kvvfs'));
        db.close();
        unlink();
      }
    }/*kvvfs sanity checks*/)
  ;/* end kvvfs tests */

  ////////////////////////////////////////////////////////////////////////
  T.g('OPFS (Worker thread only and only in supported browsers)',
      (sqlite3)=>{return !!sqlite3.opfs})
    .t({
      name: 'OPFS sanity checks',
      test: function(sqlite3){
        const filename = 'sqlite3-tester1.db';
        const pVfs = capi.sqlite3_vfs_find('opfs');
        T.assert(pVfs);
        const unlink = (fn=filename)=>wasm.sqlite3_wasm_vfs_unlink(pVfs,fn);
        unlink();
        let db = new sqlite3.opfs.OpfsDb(filename);
        db.exec([
          'create table p(a);',
          'insert into p(a) values(1),(2),(3)'
        ]);
        T.assert(3 === db.selectValue('select count(*) from p'));
        db.close();
        db = new sqlite3.opfs.OpfsDb(filename);
        db.exec('insert into p(a) values(4),(5),(6)');
        T.assert(6 === db.selectValue('select count(*) from p'));
        db.close();
        unlink();
      }
    }/*OPFS sanity checks*/)
  ;/* end OPFS tests */

  ////////////////////////////////////////////////////////////////////////
  log("Loading and initializing sqlite3 WASM module...");
  if(!isUIThread()){
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
      are simply lost, and such scripts see the self.location of
      _this_ script.
    */
    let sqlite3Js = 'sqlite3.js';
    const urlParams = new URL(self.location.href).searchParams;
    if(urlParams.has('sqlite3.dir')){
      sqlite3Js = urlParams.get('sqlite3.dir') + '/' + sqlite3Js;
    }
    importScripts(sqlite3Js);
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
    TestUtil.runTests(sqlite3);
  });
})();
