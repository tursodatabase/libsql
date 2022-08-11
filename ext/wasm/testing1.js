/*
  2022-05-22

  The author disclaims copyright to this source code.  In place of a
  legal notice, here is a blessing:

  *   May you do good and not evil.
  *   May you find forgiveness for yourself and forgive others.
  *   May you share freely, never taking more than you give.

  ***********************************************************************

  A basic test script for sqlite3-api.js. This file must be run in
  main JS thread and sqlite3.js must have been loaded before it.
*/
'use strict';
(function(){
  const T = self.SqliteTestUtil;
  const toss = function(...args){throw new Error(args.join(' '))};
  const debug = console.debug.bind(console);
  const eOutput = document.querySelector('#test-output');
  const log = console.log.bind(console)
  const logHtml = function(...args){
    log.apply(this, args);
    const ln = document.createElement('div');
    ln.append(document.createTextNode(args.join(' ')));
    eOutput.append(ln);
  };

  const eqApprox = function(v1,v2,factor=0.05){
    //debug('eqApprox',v1, v2);
    return v1>=(v2-factor) && v1<=(v2+factor);
  };

  const testBasicSanity = function(db,sqlite3){
    const capi = sqlite3.capi;
    log("Basic sanity tests...");
    T.assert(Number.isInteger(db.pointer)).
      mustThrowMatching(()=>db.pointer=1, /read-only/).
      assert(0===capi.sqlite3_extended_result_codes(db.pointer,1)).
      assert('main'===db.dbName(0));
    let pId;
    let st = db.prepare(
      new TextEncoder('utf-8').encode("select 3 as a")
      /* Testing handling of Uint8Array input */
    );
    //debug("statement =",st);
    try {
      T.assert(Number.isInteger(st.pointer))
        .mustThrowMatching(()=>st.pointer=1, /read-only/)
        .assert(1===db.openStatementCount())
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
      .assert(0===db.openStatementCount());
    let list = [];
    db.exec({
      sql:['CREATE TABLE t(a,b);',
           "INSERT INTO t(a,b) VALUES(1,2),(3,4),",
           "(?,?),('blob',X'6869')"/*intentionally missing semicolon to test for
                                     off-by-one bug in string-to-WASM conversion*/],
      multi: true,
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
    if(capi.wasm.bigIntEnabled){
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

    st = db.prepare("update t set b=:b where a='blob'");
    try {
      const ndx = st.getParamIndex(':b');
      T.assert(1===ndx);
      st.bindAsBlob(ndx, "ima blob").reset(true);
    } finally {
      st.finalize();
    }

    try {
      throw new capi.WasmAllocError;
    }catch(e){
      T.assert(e instanceof Error)
        .assert(e instanceof capi.WasmAllocError);
    }

    try {
      db.prepare("/*empty SQL*/");
      toss("Must not be reached.");
    }catch(e){
      T.assert(e instanceof sqlite3.SQLite3Error)
        .assert(0==e.message.indexOf('Cannot prepare empty'));
    }

    T.assert(capi.sqlite3_errstr(capi.SQLITE_IOERR_ACCESS).indexOf("I/O")>=0).
      assert(capi.sqlite3_errstr(capi.SQLITE_CORRUPT).indexOf('malformed')>0).
      assert(capi.sqlite3_errstr(capi.SQLITE_OK) === 'not an error');
    
    // Custom db error message handling via sqlite3_prepare_v2/v3()
    if(capi.wasm.exports.sqlite3_wasm_db_error){
      log("Testing custom error message via prepare_v3()...");
      let rc = capi.sqlite3_prepare_v3(db.pointer, [/*invalid*/], -1, 0, null, null);
      T.assert(capi.SQLITE_MISUSE === rc)
        .assert(0 === capi.sqlite3_errmsg(db.pointer).indexOf("Invalid SQL"));
      log("errmsg =",capi.sqlite3_errmsg(db.pointer));
    }
  }/*testBasicSanity()*/;

  const testUDF = function(db){
    db.createFunction("foo",function(a,b){return a+b});
    T.assert(7===db.selectValue("select foo(3,4)")).
      assert(5===db.selectValue("select foo(3,?)",2)).
      assert(5===db.selectValue("select foo(?,?2)",[1,4])).
      assert(5===db.selectValue("select foo($a,$b)",{$a:0,$b:5}));
    db.createFunction("bar", {
      arity: -1,
      callback: function(){
        var rc = 0;
        for(let i = 0; i < arguments.length; ++i) rc += arguments[i];
        return rc;
      }
    }).createFunction({
      name: "asis",
      callback: (arg)=>arg
    });
    
    //log("Testing DB::selectValue() w/ UDF...");
    T.assert(0===db.selectValue("select bar()")).
      assert(1===db.selectValue("select bar(1)")).
      assert(3===db.selectValue("select bar(1,2)")).
      assert(-1===db.selectValue("select bar(1,2,-4)")).
      assert('hi'===db.selectValue("select asis('hi')"));
    
    T.assert('hi' === db.selectValue("select ?",'hi')).
      assert(null===db.selectValue("select null")).
      assert(null === db.selectValue("select ?",null)).
      assert(null === db.selectValue("select ?",[null])).
      assert(null === db.selectValue("select $a",{$a:null})).
      assert(eqApprox(3.1,db.selectValue("select 3.0 + 0.1"))).
      assert(eqApprox(1.3,db.selectValue("select asis(1 + 0.3)")))
    ;

    //log("Testing binding and UDF propagation of blobs...");
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
  };

  const testAttach = function(db){
    const resultRows = [];
    db.exec({
      sql:new TextEncoder('utf-8').encode([
        // ^^^ testing string-vs-typedarray handling in execMulti()
        "attach 'foo.db' as foo;",
        "create table foo.bar(a);",
        "insert into foo.bar(a) values(1),(2),(3);",
        "select a from foo.bar order by a;"
      ].join('')),
      multi: true,
      rowMode: 0,
      resultRows
    });
    T.assert(3===resultRows.length)
      .assert(2===resultRows[1]);
    T.assert(2===db.selectValue('select a from foo.bar where a>1 order by a'));
    db.exec("detach foo");
    T.mustThrow(()=>db.exec("select * from foo.bar"));
  };

  const testIntPtr = function(db,S,Module){
    const w = S.capi.wasm;
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
        log("BigInt support is enabled...");
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
          assert(g64(pMax) === minMaxI64[1], "int64 mismatch")
        /* ^^^ that will fail, as of this writing, due to
           mismatched getMemValue()/setMemValue() impls in the
           Emscripten-generated glue.  We install a
           replacement getMemValue() in sqlite3-api.js to work
           around that bug:

           https://github.com/emscripten-core/emscripten/issues/17322
        */;
        //log("pMin",g64(pMin), "pMax",g64(pMax));
        w.setMemValue(pMin, minMaxI64[0], ptrType64);
        T.assert(g64(pMin) === minMaxI64[0]).
          assert(minMaxI64[0] === db.selectValue("select ?",g64(pMin))).
          assert(minMaxI64[1] === db.selectValue("select ?",g64(pMax)));
        const rxRange = /out of range for int64/;
        T.mustThrowMatching(()=>{db.prepare("select ?").bind(minMaxI64[0] - BigInt(1))},
                          rxRange).
          mustThrowMatching(()=>{db.prepare("select ?").bind(minMaxI64[1] + BigInt(1))},
                          (e)=>rxRange.test(e.message));
      }else{
        log("No BigInt support. Skipping related tests.");
        log("\"The problem\" here is that we can manipulate, at the byte level,",
            "heap memory to set 64-bit values, but we can't get those values",
            "back into JS because of the lack of 64-bit number support.");
      }
    }finally{
      const x = w.scopedAlloc(1), y = w.scopedAlloc(1), z = w.scopedAlloc(1);
      //log("x=",x,"y=",y,"z=",z); // just looking at the alignment
      w.scopedAllocPop(stack);
    }
  }/*testIntPtr()*/;
  
  const testStructStuff = function(db,S,M){
    const W = S.capi.wasm, C = S;
    /** Maintenance reminder: the rest of this function is copy/pasted
        from the upstream jaccwabyt tests. */
    log("Jaccwabyt tests...");
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
    const StructType = C.StructBinder.StructType;
    const K = C.StructBinder('my_struct',MyStructDef);
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

    const ctype = W.xCallWrapped('jaccwabyt_test_ctype_json', 'json');
    log("Struct descriptions:",ctype.structs);
    const WTStructDesc =
          ctype.structs.filter((e)=>'WasmTestStruct'===e.name)[0];
    const autoResolvePtr = true /* EXPERIMENTAL */;
    if(autoResolvePtr){
      WTStructDesc.members.ppV.signature = 'P';
    }
    const WTStruct = C.StructBinder(WTStructDesc);
    log(WTStruct.structName, WTStruct.structInfo);
    const wts = new WTStruct();
    log("WTStruct.prototype keys:",Object.keys(WTStruct.prototype));
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
            W.xGet('jaccwabyt_test_struct'/*name gets mangled in -O3 builds!*/);
      let counter = 0;
      log("wts.pointer =",wts.pointer);
      const wtsFunc = function(arg){
        log("This from a JS function called from C, "+
            "which itself was called from JS. arg =",arg);
        ++counter;
        T.assert(WTStruct.instanceForPointer(arg) === wts);
        if(3===counter){
          toss("Testing exception propagation.");
        }
      }
      wts.$v4 = 10; wts.$v8 = 20;
      wts.$xFunc = W.installFunction(wtsFunc, wts.memberSignature('xFunc'))
      /* ^^^ compiles wtsFunc to WASM and returns its new function pointer */;
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
      log("wts.pointer, wts.$ppV",wts.pointer, wts.$ppV);
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
        WTStruct.debugFlags(0x03);
        wts.$ppV = wts;
        T.assert(wts === wts.$ppV)
        WTStruct.debugFlags(0);
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
  }/*testStructStuff()*/;

  const testSqliteStructs = function(db,sqlite3,M){
    log("Tinkering with sqlite3_io_methods...");
    // https://www.sqlite.org/c3ref/vfs.html
    // https://www.sqlite.org/c3ref/io_methods.html
    const capi = sqlite3.capi, W = capi.wasm;
    const sqlite3_io_methods = capi.sqlite3_io_methods,
          sqlite3_vfs = capi.sqlite3_vfs,
          sqlite3_file = capi.sqlite3_file;
    log("struct sqlite3_file", sqlite3_file.memberKeys());
    log("struct sqlite3_vfs", sqlite3_vfs.memberKeys());
    log("struct sqlite3_io_methods", sqlite3_io_methods.memberKeys());

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
      /** Some of the following tests require that pMethods has a
          signature of "P", as opposed to "p". */
      sfile.$pMethods = iom;
      T.assert(iom === sfile.$pMethods);
      sfile.$pMethods = iom.pointer;
      T.assert(iom === sfile.$pMethods)
        .assert(IOM.resolveToInstance(iom))
        .assert(undefined ===IOM.resolveToInstance(sfile))
        .mustThrow(()=>IOM.resolveToInstance(0,true))
        .assert(S3F.resolveToInstance(sfile.pointer))
        .assert(undefined===S3F.resolveToInstance(iom));
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
        .assert(3===dVfs.$iVersion)
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
      log("Default VFS: @",dVfs.pointer);
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
                //val = capi.wasm.UTF8ToString(addr);
                val = dVfs.memberToJsString(mname);
                break;
          }
          log(prefix, sep, val);
        }
        else{
          log(prefix," = funcptr @",addr, capi.wasm.functionEntry(addr));
        }
      });
    }finally{
      dVfs.dispose();
      T.assert(undefined===dVfs.pointer);
    }
  }/*testSqliteStructs()*/;

  const testWasmUtil = function(DB,S){
    const w = S.capi.wasm;
    /**
       Maintenance reminder: the rest of this function is part of the
       upstream Jaccwabyt tree.
    */
    const chr = (x)=>x.charCodeAt(0);
    log("heap getters...");
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

    log("jstrlen()...");
    {
      T.assert(3 === w.jstrlen("abc")).assert(4 === w.jstrlen("äbc"));
    }

    log("jstrcpy()...");
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

    log("cstrncpy()...");
    {
      w.scopedAllocPush();
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
        w.scopedAllocPop();
      }
    }

    log("jstrToUintArray()...");
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

    log("allocCString()...");
    {
      const cstr = w.allocCString("hällo, world");
      const n = w.cstrlen(cstr);
      T.assert(13 === n)
        .assert(0===w.getMemValue(cstr+n))
        .assert(chr('d')===w.getMemValue(cstr+n-1));
    }

    log("scopedAlloc() and friends...");
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

    log("xCall()...");
    {
      const pJson = w.xCall('jaccwabyt_test_ctype_json');
      T.assert(Number.isFinite(pJson)).assert(w.cstrlen(pJson)>300);
    }

    log("xWrap()...");
    {
      //int jaccwabyt_test_intptr(int * p);
      //int64_t jaccwabyt_test_int64_max(void)
      //int64_t jaccwabyt_test_int64_min(void)
      //int64_t jaccwabyt_test_int64_times2(int64_t x)
      //void jaccwabyt_test_int64_minmax(int64_t * min, int64_t *max)
      //int64_t jaccwabyt_test_int64ptr(int64_t * p)
      //const char * jaccwabyt_test_ctype_json(void)
      T.mustThrowMatching(()=>w.xWrap('jaccwabyt_test_ctype_json',null,'i32'),
                          /requires 0 arg/).
        assert(w.xWrap.resultAdapter('i32') instanceof Function).
        assert(w.xWrap.argAdapter('i32') instanceof Function);
      let fw = w.xWrap('jaccwabyt_test_ctype_json','string');
      T.mustThrowMatching(()=>fw(1), /requires 0 arg/);
      let rc = fw();
      T.assert('string'===typeof rc).assert(rc.length>300);
      rc = w.xCallWrapped('jaccwabyt_test_ctype_json','*');
      T.assert(rc>0 && Number.isFinite(rc));
      rc = w.xCallWrapped('jaccwabyt_test_ctype_json','string');
      T.assert('string'===typeof rc).assert(rc.length>300);
      fw = w.xWrap('jaccwabyt_test_str_hello', 'string:free',['i32']);
      rc = fw(0);
      T.assert('hello'===rc);
      rc = fw(1);
      T.assert(null===rc);

      w.xWrap.resultAdapter('thrice', (v)=>3n*BigInt(v));
      w.xWrap.argAdapter('twice', (v)=>2n*BigInt(v));
      fw = w.xWrap('jaccwabyt_test_int64_times2','thrice','twice');
      rc = fw(1);
      T.assert(12n===rc);

      w.scopedAllocCall(function(){
        let pI1 = w.scopedAlloc(8), pI2 = pI1+4;
        w.setMemValue(pI1, 0,'*')(pI2, 0, '*');
        let f = w.xWrap('jaccwabyt_test_int64_minmax',undefined,['i64*','i64*']);
        let r1 = w.getMemValue(pI1, 'i64'), r2 = w.getMemValue(pI2, 'i64');
        T.assert(!Number.isSafeInteger(r1)).assert(!Number.isSafeInteger(r2));
      });
    }
  }/*testWasmUtil()*/;

  const runTests = function(Module){
    //log("Module",Module);
    const sqlite3 = Module.sqlite3,
          capi = sqlite3.capi,
          oo = sqlite3.oo1,
          wasm = capi.wasm;
    log("Loaded module:",capi.sqlite3_libversion(), capi.sqlite3_sourceid());
    log("Build options:",wasm.compileOptionUsed());

    if(1){
      /* Let's grab those last few lines of test coverage for
         sqlite3-api.js... */
      const rc = wasm.compileOptionUsed(['COMPILER']);
      T.assert(1 === rc.COMPILER);
      const obj = {COMPILER:undefined};
      wasm.compileOptionUsed(obj);
      T.assert(1 === obj.COMPILER);
    }
    log("WASM heap size =",wasm.heap8().length);
    //log("capi.wasm.exports.__indirect_function_table",capi.wasm.exports.__indirect_function_table);

    const wasmCtypes = wasm.ctype;
    //log("wasmCtypes",wasmCtypes);
    T.assert(wasmCtypes.structs[0].name==='sqlite3_vfs').
      assert(wasmCtypes.structs[0].members.szOsFile.sizeof>=4).
      assert(wasmCtypes.structs[1/*sqlite3_io_methods*/
                             ].members.xFileSize.offset>0);
    //log(wasmCtypes.structs[0].name,"members",wasmCtypes.structs[0].members);
    [ /* Spot-check a handful of constants to make sure they got installed... */
      'SQLITE_SCHEMA','SQLITE_NULL','SQLITE_UTF8',
      'SQLITE_STATIC', 'SQLITE_DIRECTONLY',
      'SQLITE_OPEN_CREATE', 'SQLITE_OPEN_DELETEONCLOSE'
    ].forEach(function(k){
      T.assert('number' === typeof capi[k]);
    });
    [/* Spot-check a few of the WASM API methods. */
      'alloc', 'dealloc', 'installFunction'
    ].forEach(function(k){
      T.assert(capi.wasm[k] instanceof Function);
    });

    const db = new oo.DB(':memory:'), startTime = performance.now();
    try {
      log("DB filename:",db.filename,db.fileName());
      const banner1 = '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>',
            banner2 = '<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<';
      [
        testWasmUtil, testBasicSanity, testUDF,
        testAttach, testIntPtr, testStructStuff,
        testSqliteStructs        
      ].forEach((f)=>{
        const t = T.counter, n = performance.now();
        logHtml(banner1,"Running",f.name+"()...");
        f(db, sqlite3, Module);
        logHtml(banner2,f.name+"():",T.counter - t,'tests in',(performance.now() - n),"ms");
      });
    }finally{
      db.close();
    }
    logHtml("Total Test count:",T.counter,"in",(performance.now() - startTime),"ms");
    log('capi.wasm.exports',capi.wasm.exports);
  };

  sqlite3InitModule(self.sqlite3TestModule).then(function(theModule){
    /** Use a timeout so that we are (hopefully) out from under
        the module init stack when our setup gets run. Just on
        principle, not because we _need_ to be. */
    //console.debug("theModule =",theModule);
    //setTimeout(()=>runTests(theModule), 0);
    // ^^^ Chrome warns: "VIOLATION: setTimeout() handler took A WHOLE 50ms!"
    self._MODULE = theModule /* this is only to facilitate testing from the console */
    runTests(theModule);
  });
})();
