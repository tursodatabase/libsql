/*
  2022-07-22

  The author disclaims copyright to this source code.  In place of a
  legal notice, here is a blessing:

  *   May you do good and not evil.
  *   May you find forgiveness for yourself and forgive others.
  *   May you share freely, never taking more than you give.

  ***********************************************************************

  This file glues together disparate pieces of JS which are loaded in
  previous steps of the sqlite3-api.js bootstrapping process:
  sqlite3-api-prologue.js, whwasmutil.js, and jaccwabyt.js. It
  initializes the main API pieces so that the downstream components
  (e.g. sqlite3-api-oo1.js) have all that they need.
*/
self.sqlite3ApiBootstrap.initializers.push(function(sqlite3){
  'use strict';
  const toss = (...args)=>{throw new Error(args.join(' '))};
  const capi = sqlite3.capi, wasm = capi.wasm, util = capi.util;
  self.WhWasmUtilInstaller(capi.wasm);
  delete self.WhWasmUtilInstaller;

  if(0){
    /*  "The problem" is that the following isn't type-safe.
        OTOH, nothing about WASM pointers is. */
    /**
       Add the `.pointer` xWrap() signature entry to extend the
       `pointer` arg handler to check for a `pointer` property. This
       can be used to permit, e.g., passing an sqlite3.oo1.DB instance
       to a C-style sqlite3_xxx function which takes an `sqlite3*`
       argument.
    */
    const xPointer = wasm.xWrap.argAdapter('pointer');
    const adapter = function(v){
      if(v && v.constructor){
        const x = v.pointer;
        if(Number.isInteger(x)) return x;
        else toss("Invalid (object) type for .pointer-type argument.");
      }
      return xPointer(v);
    };
    wasm.xWrap.argAdapter('.pointer', adapter);
  } /* ".pointer" xWrap() argument adapter */

  if(1){/* Convert Arrays and certain TypedArrays to strings for
           'flexible-string'-type arguments */
    const xString = wasm.xWrap.argAdapter('string');
    wasm.xWrap.argAdapter(
      'flexible-string', (v)=>xString(util.arrayToString(v))
    );
  }
  
  if(1){// WhWasmUtil.xWrap() bindings...
    /**
       Add some descriptive xWrap() aliases for '*' intended to (A)
       initially improve readability/correctness of capi.signatures
       and (B) eventually perhaps provide automatic conversion from
       higher-level representations, e.g. capi.sqlite3_vfs to
       `sqlite3_vfs*` via capi.sqlite3_vfs.pointer.
    */
    const aPtr = wasm.xWrap.argAdapter('*');
    wasm.xWrap.argAdapter('sqlite3*', aPtr)('sqlite3_stmt*', aPtr);
    wasm.xWrap.resultAdapter('sqlite3*', aPtr)('sqlite3_stmt*', aPtr);

    /**
       Populate api object with sqlite3_...() by binding the "raw" wasm
       exports into type-converting proxies using wasm.xWrap().
    */
    for(const e of wasm.bindingSignatures){
      capi[e[0]] = wasm.xWrap.apply(null, e);
    }
    for(const e of wasm.bindingSignatures.wasm){
      capi.wasm[e[0]] = wasm.xWrap.apply(null, e);
    }

    /* For C API functions which cannot work properly unless
       wasm.bigIntEnabled is true, install a bogus impl which
       throws if called when bigIntEnabled is false. */
    const fI64Disabled = function(fname){
      return ()=>toss(fname+"() disabled due to lack",
                      "of BigInt support in this build.");
    };
    for(const e of wasm.bindingSignatures.int64){
      capi[e[0]] = wasm.bigIntEnabled
        ? wasm.xWrap.apply(null, e)
        : fI64Disabled(e[0]);
    }

    if(wasm.exports.sqlite3_wasm_db_error){
      util.sqlite3_wasm_db_error = capi.wasm.xWrap(
        'sqlite3_wasm_db_error', 'int', 'sqlite3*', 'int', 'string'
      );
    }else{
      util.sqlite3_wasm_db_error = function(pDb,errCode,msg){
        console.warn("sqlite3_wasm_db_error() is not exported.",arguments);
        return errCode;
      };
    }

    /**
       When registering a VFS and its related components it may be
       necessary to ensure that JS keeps a reference to them to keep
       them from getting garbage collected. Simply pass each such value
       to this function and a reference will be held to it for the life
       of the app.
    */
    capi.sqlite3_vfs_register.addReference = function f(...args){
      if(!f._) f._ = [];
      f._.push(...args);
    };

  }/*xWrap() bindings*/;

  /**
     Internal helper to assist in validating call argument counts in
     the hand-written sqlite3_xyz() wrappers. We do this only for
     consistency with non-special-case wrappings.
  */
  const __dbArgcMismatch = (pDb,f,n)=>{
    return sqlite3.util.sqlite3_wasm_db_error(pDb, capi.SQLITE_MISUSE,
                                              f+"() requires "+n+" argument"+
                                              (1===n?'':'s')+".");
  };

  /**
     Helper for flexible-string conversions which require a
     byte-length counterpart argument. Passed a value and its
     ostensible length, this function returns [V,N], where V
     is either v or a transformed copy of v and N is either n,
     -1, or the byte length of v (if it's a byte array).
  */
  const __flexiString = function(v,n){
    if('string'===typeof v){
      n = -1;
    }else if(util.isSQLableTypedArray(v)){
      n = v.byteLength;
      v = util.typedArrayToString(v);
    }else if(Array.isArray(v)){
      v = v.join('');
      n = -1;
    }
    return [v, n];
  };

  if(1){/* Special-case handling of sqlite3_exec() */
    const __exec = wasm.xWrap("sqlite3_exec", "int",
                              ["sqlite3*", "flexible-string", "*", "*", "**"]);
    /* Documented in the api object's initializer. */
    capi.sqlite3_exec = function(pDb, sql, callback, pVoid, pErrMsg){
      if(5!==arguments.length){
        return __dbArgcMismatch(pDb,"sqlite3_exec",5);
      }else if('function' !== typeof callback){
        return __exec(pDb, sql, callback, pVoid, pErrMsg);
      }
      /* Wrap the callback in a WASM-bound function and convert the callback's
         `(char**)` arguments to arrays of strings... */
      const wasm = capi.wasm;
      const cbwrap = function(pVoid, nCols, pColVals, pColNames){
        let rc = capi.SQLITE_ERROR;
        try {
          let aVals = [], aNames = [], i = 0, offset = 0;
          for( ; i < nCols; offset += (wasm.ptrSizeof * ++i) ){
            aVals.push( wasm.cstringToJs(wasm.getPtrValue(pColVals + offset)) );
            aNames.push( wasm.cstringToJs(wasm.getPtrValue(pColNames + offset)) );
          }
          rc = callback(pVoid, nCols, aVals, aNames) | 0;
          /* The first 2 args of the callback are useless for JS but
             we want the JS mapping of the C API to be as close to the
             C API as possible. */
        }catch(e){
          /* If we set the db error state here, the higher-level exec() call
             replaces it with its own, so we have no way of reporting the
             exception message except the console. We must not propagate
             exceptions through the C API. */
        }
        return rc;
      };
      let pFunc, rc;
      try{
        pFunc = wasm.installFunction("ipipp", cbwrap);
        rc = __exec(pDb, sql, pFunc, pVoid, pErrMsg);
      }catch(e){
        rc = util.sqlite3_wasm_db_error(pDb, capi.SQLITE_ERROR,
                                        "Error running exec(): "+e.message);
      }finally{
        if(pFunc) wasm.uninstallFunction(pFunc);
      }
      return rc;
    };
  }/*sqlite3_exec() proxy*/;

  if(1){/* Special-case handling of sqlite3_prepare_v2() and
      sqlite3_prepare_v3() */
    /**
       Scope-local holder of the two impls of sqlite3_prepare_v2/v3().
    */
    const __prepare = Object.create(null);
    /**
       This binding expects a JS string as its 2nd argument and
       null as its final argument. In order to compile multiple
       statements from a single string, the "full" impl (see
       below) must be used.
    */
    __prepare.basic = wasm.xWrap('sqlite3_prepare_v3',
                                 "int", ["sqlite3*", "string",
                                         "int"/*ignored for this impl!*/,
                                         "int", "**",
                                         "**"/*MUST be 0 or null or undefined!*/]);
    /**
       Impl which requires that the 2nd argument be a pointer
       to the SQL string, instead of being converted to a
       string. This variant is necessary for cases where we
       require a non-NULL value for the final argument
       (exec()'ing multiple statements from one input
       string). For simpler cases, where only the first
       statement in the SQL string is required, the wrapper
       named sqlite3_prepare_v2() is sufficient and easier to
       use because it doesn't require dealing with pointers.
    */
    __prepare.full = wasm.xWrap('sqlite3_prepare_v3',
                                "int", ["sqlite3*", "*", "int", "int",
                                        "**", "**"]);

    /* Documented in the api object's initializer. */
    capi.sqlite3_prepare_v3 = function f(pDb, sql, sqlLen, prepFlags, ppStmt, pzTail){
      if(6!==arguments.length){
        return __dbArgcMismatch(pDb,"sqlite3_prepare_v3",6);
      }
      const [xSql, xSqlLen] = __flexiString(sql, sqlLen);
      switch(typeof xSql){
          case 'string': return __prepare.basic(pDb, xSql, xSqlLen, prepFlags, ppStmt, null);
          case 'number': return __prepare.full(pDb, xSql, xSqlLen, prepFlags, ppStmt, pzTail);
          default:
            return util.sqlite3_wasm_db_error(
              pDb, capi.SQLITE_MISUSE,
              "Invalid SQL argument type for sqlite3_prepare_v2/v3()."
            );
      }
    };

    /* Documented in the api object's initializer. */
    capi.sqlite3_prepare_v2 = function(pDb, sql, sqlLen, ppStmt, pzTail){
      return (5==arguments.length)
        ? capi.sqlite3_prepare_v3(pDb, sql, sqlLen, 0, ppStmt, pzTail)
        : __dbArgcMismatch(pDb,"sqlite3_prepare_v2",5);
    };
  }/*sqlite3_prepare_v2/v3()*/;

  if(1){// Extend wasm.pstack, now that the wasm utils are installed
    /**
       Allocates n chunks, each sz bytes, as a single memory block and
       returns the addresses as an array of n element, each holding
       the address of one chunk.

       Throws a WasmAllocError if allocation fails.

       Example:

       ```
       const [p1, p2, p3] = wasm.pstack.allocChunks(3,4);
       ```
    */
    wasm.pstack.allocChunks = (n,sz)=>{
      const mem = wasm.pstack.alloc(n * sz);
      const rc = [];
      let i = 0, offset = 0;
      for(; i < n; offset = (sz * ++i)){
        rc.push(mem + offset);
      }
      return rc;
    };

    /**
       A convenience wrapper for allocChunks() which sizes each chunks
       as either 8 bytes (safePtrSize is truthy) or wasm.ptrSizeof (if
       safePtrSize is truthy).

       How it returns its result differs depending on its first
       argument: if it's 1, it returns a single pointer value. If it's
       more than 1, it returns the same as allocChunks().

       When one of the pointers refers to a 64-bit value, e.g. a
       double or int64, and that value must be written or fetch,
       e.g. using wasm.setMemValue() or wasm.getMemValue(), it is
       important that the pointer in question be aligned to an 8-byte
       boundary or else it will not be fetched or written properly and
       will corrupt or read neighboring memory.

       However, when all pointers involved are "small", it is safe to
       pass a falsy value to save to memory.
    */
    wasm.pstack.allocPtr = (n=1,safePtrSize=true) =>{
      return 1===n
        ? wasm.pstack.alloc(safePtrSize ? 8 : wasm.ptrSizeof)
        : wasm.pstack.allocChunks(n, safePtrSize ? 8 : wasm.ptrSizeof);
    };
  }/*wasm.pstack filler*/

  /**
     Install JS<->C struct bindings for the non-opaque struct types we
     need... */
  sqlite3.StructBinder = self.Jaccwabyt({
    heap: 0 ? wasm.memory : wasm.heap8u,
    alloc: wasm.alloc,
    dealloc: wasm.dealloc,
    functionTable: wasm.functionTable,
    bigIntEnabled: wasm.bigIntEnabled,
    memberPrefix: '$'
  });
  delete self.Jaccwabyt;

  {/* Import C-level constants and structs... */
    const cJson = wasm.xCall('sqlite3_wasm_enum_json');
    if(!cJson){
      toss("Maintenance required: increase sqlite3_wasm_enum_json()'s",
           "static buffer size!");
    }
    wasm.ctype = JSON.parse(wasm.cstringToJs(cJson));
    //console.debug('wasm.ctype length =',wasm.cstrlen(cJson));
    for(const t of ['access', 'blobFinalizers', 'dataTypes',
                    'encodings', 'fcntl', 'flock', 'ioCap',
                    'openFlags', 'prepareFlags', 'resultCodes',
                    'serialize', 'syncFlags', 'udfFlags',
                    'version'
                   ]){
      for(const e of Object.entries(wasm.ctype[t])){
        // ^^^ [k,v] there triggers a buggy code transormation via one
        // of the Emscripten-driven optimizers.
        capi[e[0]] = e[1];
      }
    }
    const __rcMap = Object.create(null);
    for(const t of ['resultCodes']){
      for(const e of Object.entries(wasm.ctype[t])){
        __rcMap[e[1]] = e[0];
      }
    }
    /**
       For the given integer, returns the SQLITE_xxx result code as a
       string, or undefined if no such mapping is found.
    */
    capi.sqlite3_web_rc_str = (rc)=>__rcMap[rc];
    /* Bind all registered C-side structs... */
    for(const s of wasm.ctype.structs){
      capi[s.name] = sqlite3.StructBinder(s);
    }
  }/*end C constant imports*/

  sqlite3.version = Object.assign(Object.create(null),{
    library: sqlite3.capi.sqlite3_libversion(),
    sourceId: sqlite3.capi.sqlite3_sourceid()
  });
});

