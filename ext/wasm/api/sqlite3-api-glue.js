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
  const toss3 = sqlite3.SQLite3Error.toss;
  const capi = sqlite3.capi, wasm = sqlite3.wasm, util = sqlite3.util;
  self.WhWasmUtilInstaller(wasm);
  delete self.WhWasmUtilInstaller;

  /**
     Install JS<->C struct bindings for the non-opaque struct types we
     need... */
  sqlite3.StructBinder = self.Jaccwabyt({
    heap: 0 ? wasm.memory : wasm.heap8u,
    alloc: wasm.alloc,
    dealloc: wasm.dealloc,
    bigIntEnabled: wasm.bigIntEnabled,
    memberPrefix: /* Never change this: this prefix is baked into any
                     amount of code and client-facing docs. */ '$'
  });
  delete self.Jaccwabyt;

  {/* Convert Arrays and certain TypedArrays to strings for
      'string:flexible'-type arguments */
    const xString = wasm.xWrap.argAdapter('string');
    wasm.xWrap.argAdapter(
      'string:flexible', (v)=>xString(util.flexibleString(v))
    );

    /**
       The 'string:static' argument adapter treats its argument as
       either...

       - WASM pointer: assumed to be a long-lived C-string which gets
         returned as-is.

       - Anything else: gets coerced to a JS string for use as a map
         key. If a matching entry is found (as described next), it is
         returned, else wasm.allocCString() is used to create a a new
         string, map its pointer to (''+v) for the remainder of the
         application's life, and returns that pointer value for this
         call and all future calls which are passed a
         string-equivalent argument.

       Use case: sqlite3_bind_pointer() and sqlite3_result_pointer()
       call for "a static string and preferably a string
       literal". This converter is used to ensure that the string
       value seen by those functions is long-lived and behaves as they
       need it to.
    */
    wasm.xWrap.argAdapter(
      'string:static',
      function(v){
        if(wasm.isPtr(v)) return v;
        v = ''+v;
        let rc = this[v];
        return rc || (rc = this[v] = wasm.allocCString(v));
      }.bind(Object.create(null))
    );
  }/* special-case string-type argument conversions */
  
  if(1){// WhWasmUtil.xWrap() bindings...
    /**
       Add some descriptive xWrap() aliases for '*' intended to (A)
       initially improve readability/correctness of capi.signatures
       and (B) provide automatic conversion from higher-level
       representations, e.g. capi.sqlite3_vfs to `sqlite3_vfs*` via
       capi.sqlite3_vfs.pointer.
    */
    const aPtr = wasm.xWrap.argAdapter('*');
    const nilType = function(){};
    wasm.xWrap.argAdapter('sqlite3_filename', aPtr)
    ('sqlite3_context*', aPtr)
    ('sqlite3_value*', aPtr)
    ('void*', aPtr)
    ('sqlite3_stmt*', (v)=>
      aPtr((v instanceof (sqlite3?.oo1?.Stmt || nilType))
           ? v.pointer : v))
    ('sqlite3*', (v)=>
      aPtr((v instanceof (sqlite3?.oo1?.DB || nilType))
           ? v.pointer : v))
    ('sqlite3_index_info*', (v)=>
      aPtr((v instanceof (capi.sqlite3_index_info || nilType))
           ? v.pointer : v))
    ('sqlite3_module*', (v)=>
      aPtr((v instanceof (capi.sqlite3_module || nilType))
           ? v.pointer : v))
    /**
       `sqlite3_vfs*`:

       - v is-a string: use the result of sqlite3_vfs_find(v) but
         throw if it returns 0.
       - v is-a capi.sqlite3_vfs: use v.pointer.
       - Else return the same as the `'*'` argument conversion.
    */
    ('sqlite3_vfs*', (v)=>{
      if('string'===typeof v){
        /* A NULL sqlite3_vfs pointer will be treated as the default
           VFS in many contexts. We specifically do not want that
           behavior here. */
        return capi.sqlite3_vfs_find(v)
          || sqlite3.SQLite3Error.toss("Unknown sqlite3_vfs name:",v);
      }
      return aPtr((v instanceof (capi.sqlite3_vfs || nilType))
                  ? v.pointer : v);
    });

    const rPtr = wasm.xWrap.resultAdapter('*');
    wasm.xWrap.resultAdapter('sqlite3*', rPtr)
    ('sqlite3_context*', rPtr)
    ('sqlite3_stmt*', rPtr)
    ('sqlite3_vfs*', rPtr)
    ('void*', rPtr);

    /**
       Populate api object with sqlite3_...() by binding the "raw" wasm
       exports into type-converting proxies using wasm.xWrap().
    */
    for(const e of wasm.bindingSignatures){
      capi[e[0]] = wasm.xWrap.apply(null, e);
    }
    for(const e of wasm.bindingSignatures.wasm){
      wasm[e[0]] = wasm.xWrap.apply(null, e);
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

    /* There's no need to expose bindingSignatures to clients,
       implicitly making it part of the public interface. */
    delete wasm.bindingSignatures;

    if(wasm.exports.sqlite3_wasm_db_error){
      const __db_err = wasm.xWrap(
        'sqlite3_wasm_db_error', 'int', 'sqlite3*', 'int', 'string'
      );
      /**
         Sets the given db's error state. Accepts:

         - (sqlite3*, int code, string msg)
         - (sqlite3*, Error [,string msg])

         If passed a WasmAllocError, the message is ingored and the
         result code is SQLITE_NOMEM. If passed any other Error type,
         the result code defaults to SQLITE_ERROR unless the Error
         object has a resultCode property, in which case that is used
         (e.g. SQLite3Error has that). If passed a non-WasmAllocError
         exception, the message string defaults to theError.message.

         Returns the resulting code. Pass (pDb,0,0) to clear the error
         state.
       */
      util.sqlite3_wasm_db_error = function(pDb, resultCode, message){
        if(resultCode instanceof sqlite3.WasmAllocError){
          resultCode = capi.SQLITE_NOMEM;
          message = 0 /*avoid allocating message string*/;
        }else if(resultCode instanceof Error){
          message = message || resultCode.message;
          resultCode = (resultCode.resultCode || capi.SQLITE_ERROR);
        }
        return __db_err(pDb, resultCode, message);
      };
    }else{
      util.sqlite3_wasm_db_error = function(pDb,errCode,msg){
        console.warn("sqlite3_wasm_db_error() is not exported.",arguments);
        return errCode;
      };
    }
  }/*xWrap() bindings*/;

  /**
     Internal helper to assist in validating call argument counts in
     the hand-written sqlite3_xyz() wrappers. We do this only for
     consistency with non-special-case wrappings.
  */
  const __dbArgcMismatch = (pDb,f,n)=>{
    return sqlite3.util.sqlite3_wasm_db_error(pDb, capi.SQLITE_MISUSE,
                                              f+"() requires "+n+" argument"+
                                              (1===n?"":'s')+".");
  };

  if(1){/* Bindings for sqlite3_create_collation() */

    const __ccv2 = wasm.xWrap(
      'sqlite3_create_collation_v2', 'int',
      'sqlite3*','string','int','*','*','*'
      /* int(*xCompare)(void*,int,const void*,int,const void*) */
      /* void(*xDestroy(void*) */);

    /**
       Works exactly like C's sqlite3_create_collation_v2() except that:

       1) It permits its two function arguments to be JS functions,
          for which it will install WASM-bound proxies.

       2) It returns capi.SQLITE_FORMAT if the 3rd argument is not
          capi.SQLITE_UTF8. No other encodings are supported.

       Returns 0 on success, non-0 on error, in which case the error
       state of pDb (of type `sqlite3*` or argument-convertible to it)
       may contain more information.
    */
    capi.sqlite3_create_collation_v2 = function(pDb,zName,eTextRep,pArg,xCompare,xDestroy){
      if(6!==arguments.length) return __dbArgcMismatch(pDb, 'sqlite3_create_collation_v2', 6);
      else if(capi.SQLITE_UTF8!==eTextRep){
        return util.sqlite3_wasm_db_error(
          pDb, capi.SQLITE_FORMAT, "SQLITE_UTF8 is the only supported encoding."
        );
      }
      let rc, pfCompare, pfDestroy;
      try{
        if(xCompare instanceof Function){
          pfCompare = wasm.installFunction(xCompare, 'i(pipip)');
        }
        if(xDestroy instanceof Function){
          pfDestroy = wasm.installFunction(xDestroy, 'v(p)');
        }
        rc = __ccv2(pDb, zName, eTextRep, pArg,
                    pfCompare || xCompare, pfDestroy || xDestroy);
      }catch(e){
        if(pfCompare) wasm.uninstallFunction(pfCompare);
        if(pfDestroy) wasm.uninstallFunction(pfDestroy);
        rc = util.sqlite3_wasm_db_error(pDb, e);
      }
      return rc;
    };

    capi.sqlite3_create_collation = (pDb,zName,eTextRep,pArg,xCompare)=>{
      return (5===arguments.length)
        ? capi.sqlite3_create_collation_v2(pDb,zName,eTextRep,pArg,xCompare,0)
        : __dbArgcMismatch(pDb, 'sqlite3_create_collation', 5);
    }


  }/*sqlite3_create_collation() and friends*/

  if(1){/* Special-case handling of sqlite3_exec() */
    const __exec = wasm.xWrap("sqlite3_exec", "int",
                              ["sqlite3*", "string:flexible", "*", "*", "**"]);
    /* Documented in the api object's initializer. */
    capi.sqlite3_exec = function f(pDb, sql, callback, pVoid, pErrMsg){
      if(f.length!==arguments.length){
        return __dbArgcMismatch(pDb,"sqlite3_exec",f.length);
      }else if('function' !== typeof callback){
        return __exec(pDb, sql, callback, pVoid, pErrMsg);
      }
      /* Wrap the callback in a WASM-bound function and convert the callback's
         `(char**)` arguments to arrays of strings... */
      const cbwrap = function(pVoid, nCols, pColVals, pColNames){
        let rc = capi.SQLITE_ERROR;
        try {
          let aVals = [], aNames = [], i = 0, offset = 0;
          for( ; i < nCols; offset += (wasm.ptrSizeof * ++i) ){
            aVals.push( wasm.cstrToJs(wasm.peekPtr(pColVals + offset)) );
            aNames.push( wasm.cstrToJs(wasm.peekPtr(pColNames + offset)) );
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

  if(1){/* Special-case handling of sqlite3_create_function_v2()
           and sqlite3_create_window_function() */
    const sqlite3CreateFunction = wasm.xWrap(
      "sqlite3_create_function_v2", "int",
      ["sqlite3*", "string"/*funcName*/, "int"/*nArg*/,
       "int"/*eTextRep*/, "*"/*pApp*/,
       "*"/*xStep*/,"*"/*xFinal*/, "*"/*xValue*/, "*"/*xDestroy*/]
    );
    const sqlite3CreateWindowFunction = wasm.xWrap(
      "sqlite3_create_window_function", "int",
      ["sqlite3*", "string"/*funcName*/, "int"/*nArg*/,
       "int"/*eTextRep*/, "*"/*pApp*/,
       "*"/*xStep*/,"*"/*xFinal*/, "*"/*xValue*/,
       "*"/*xInverse*/, "*"/*xDestroy*/]
    );

    const __udfSetResult = function(pCtx, val){
      //console.warn("udfSetResult",typeof val, val);
      switch(typeof val) {
          case 'undefined':
            /* Assume that the client already called sqlite3_result_xxx(). */
            break;
          case 'boolean':
            capi.sqlite3_result_int(pCtx, val ? 1 : 0);
            break;
          case 'bigint':
            if(wasm.bigIntEnabled){
              if(util.bigIntFits64(val)) capi.sqlite3_result_int64(pCtx, val);
              else toss3("BigInt value",val.toString(),"is too BigInt for int64.");
            }else if(util.bigIntFits32(val)){
              capi.sqlite3_result_int(pCtx, Number(val));
            }else if(util.bigIntFitsDouble(val)){
              capi.sqlite3_result_double(pCtx, Number(val));
            }else{
              toss3("BigInt value",val.toString(),"is too BigInt.");
            }
            break;
          case 'number': {
            (util.isInt32(val)
             ? capi.sqlite3_result_int
             : capi.sqlite3_result_double)(pCtx, val);
            break;
          }
          case 'string':
            capi.sqlite3_result_text(pCtx, val, -1, capi.SQLITE_TRANSIENT);
            break;
          case 'object':
            if(null===val/*yes, typeof null === 'object'*/) {
              capi.sqlite3_result_null(pCtx);
              break;
            }else if(util.isBindableTypedArray(val)){
              const pBlob = wasm.allocFromTypedArray(val);
              capi.sqlite3_result_blob(
                pCtx, pBlob, val.byteLength,
                wasm.exports[sqlite3.config.deallocExportName]
              );
              break;
            }
            // else fall through
          default:
          toss3("Don't not how to handle this UDF result value:",(typeof val), val);
      };
    }/*__udfSetResult()*/;

    const __udfConvertArgs = function(argc, pArgv){
      let i, pVal, valType, arg;
      const tgt = [];
      for(i = 0; i < argc; ++i){
        pVal = wasm.peekPtr(pArgv + (wasm.ptrSizeof * i));
        /**
           Curiously: despite ostensibly requiring 8-byte
           alignment, the pArgv array is parcelled into chunks of
           4 bytes (1 pointer each). The values those point to
           have 8-byte alignment but the individual argv entries
           do not.
        */            
        valType = capi.sqlite3_value_type(pVal);
        switch(valType){
            case capi.SQLITE_INTEGER:
              if(wasm.bigIntEnabled){
                arg = capi.sqlite3_value_int64(pVal);
                if(util.bigIntFitsDouble(arg)) arg = Number(arg);
              }
              else arg = capi.sqlite3_value_double(pVal)/*yes, double, for larger integers*/;
              break;
            case capi.SQLITE_FLOAT:
              arg = capi.sqlite3_value_double(pVal);
              break;
            case capi.SQLITE_TEXT:
              arg = capi.sqlite3_value_text(pVal);
              break;
            case capi.SQLITE_BLOB:{
              const n = capi.sqlite3_value_bytes(pVal);
              const pBlob = capi.sqlite3_value_blob(pVal);
              if(n && !pBlob) sqlite3.WasmAllocError.toss(
                "Cannot allocate memory for blob argument of",n,"byte(s)"
              );
              arg = n ? wasm.heap8u().slice(pBlob, pBlob + Number(n)) : null;
              break;
            }
            case capi.SQLITE_NULL:
              arg = null; break;
            default:
              toss3("Unhandled sqlite3_value_type()",valType,
                    "is possibly indicative of incorrect",
                    "pointer size assumption.");
        }
        tgt.push(arg);
      }
      return tgt;
    }/*__udfConvertArgs()*/;

    const __udfSetError = (pCtx, e)=>{
      if(e instanceof sqlite3.WasmAllocError){
        capi.sqlite3_result_error_nomem(pCtx);
      }else{
        const msg = ('string'===typeof e) ? e : e.message;
        capi.sqlite3_result_error(pCtx, msg, -1);
      }
    };

    const __xFunc = function(callback){
      return function(pCtx, argc, pArgv){
        try{ __udfSetResult(pCtx, callback(pCtx, ...__udfConvertArgs(argc, pArgv))) }
        catch(e){
          //console.error('xFunc() caught:',e);
          __udfSetError(pCtx, e);
        }
      };
    };

    const __xInverseAndStep = function(callback){
      return function(pCtx, argc, pArgv){
        try{ callback(pCtx, ...__udfConvertArgs(argc, pArgv)) }
        catch(e){ __udfSetError(pCtx, e) }
      };
    };

    const __xFinalAndValue = function(callback){
      return function(pCtx){
        try{ __udfSetResult(pCtx, callback(pCtx)) }
        catch(e){ __udfSetError(pCtx, e) }
      };
    };

    const __xDestroy = function(callback){
      return function(pVoid){
        try{ callback(pVoid) }
        catch(e){ console.error("UDF xDestroy method threw:",e) }
      };
    };

    const __xMap = Object.assign(Object.create(null), {
      xFunc:    {sig:'v(pip)', f:__xFunc},
      xStep:    {sig:'v(pip)', f:__xInverseAndStep},
      xInverse: {sig:'v(pip)', f:__xInverseAndStep},
      xFinal:   {sig:'v(p)',   f:__xFinalAndValue},
      xValue:   {sig:'v(p)',   f:__xFinalAndValue},
      xDestroy: {sig:'v(p)',   f:__xDestroy}
    });

    const __xWrapFuncs = function(theFuncs, tgtUninst){
      const rc = []
      let k;
      for(k in theFuncs){
        let fArg = theFuncs[k];
        if('function'===typeof fArg){
          const w = __xMap[k];
          fArg = wasm.installFunction(w.sig, w.f(fArg));
          tgtUninst.push(fArg);
        }
        rc.push(fArg);
      }
      return rc;
    };

    /* Documented in the api object's initializer. */
    capi.sqlite3_create_function_v2 = function f(
      pDb, funcName, nArg, eTextRep, pApp,
      xFunc,   //void (*xFunc)(sqlite3_context*,int,sqlite3_value**)
      xStep,   //void (*xStep)(sqlite3_context*,int,sqlite3_value**)
      xFinal,  //void (*xFinal)(sqlite3_context*)
      xDestroy //void (*xDestroy)(void*)
    ){
      if(f.length!==arguments.length){
        return __dbArgcMismatch(pDb,"sqlite3_create_function_v2",f.length);
      }
      /* Wrap the callbacks in a WASM-bound functions... */
      const uninstall = [/*funcs to uninstall on error*/];
      let rc;
      try{
        const funcArgs =  __xWrapFuncs({xFunc, xStep, xFinal, xDestroy},
                                       uninstall);
        rc = sqlite3CreateFunction(pDb, funcName, nArg, eTextRep,
                                   pApp, ...funcArgs);
      }catch(e){
        console.error("sqlite3_create_function_v2() setup threw:",e);
        for(let v of uninstall){
          wasm.uninstallFunction(v);
        }
        rc = util.sqlite3_wasm_db_error(pDb, capi.SQLITE_ERROR,
                                        "Creation of UDF threw: "+e.message);
      }
      return rc;
    };

    capi.sqlite3_create_function = function f(
      pDb, funcName, nArg, eTextRep, pApp,
      xFunc, xStep, xFinal
    ){
      return (f.length===arguments.length)
        ? capi.sqlite3_create_function_v2(pDb, funcName, nArg, eTextRep,
                                          pApp, xFunc, xStep, xFinal, 0)
        : __dbArgcMismatch(pDb,"sqlite3_create_function",f.length);
    };

    /* Documented in the api object's initializer. */
    capi.sqlite3_create_window_function = function f(
      pDb, funcName, nArg, eTextRep, pApp,
      xStep,   //void (*xStep)(sqlite3_context*,int,sqlite3_value**)
      xFinal,  //void (*xFinal)(sqlite3_context*)
      xValue,  //void (*xFinal)(sqlite3_context*)
      xInverse,//void (*xStep)(sqlite3_context*,int,sqlite3_value**)
      xDestroy //void (*xDestroy)(void*)
    ){
      if(f.length!==arguments.length){
        return __dbArgcMismatch(pDb,"sqlite3_create_window_function",f.length);
      }
      /* Wrap the callbacks in a WASM-bound functions... */
      const uninstall = [/*funcs to uninstall on error*/];
      let rc;
      try{
        const funcArgs = __xWrapFuncs({xStep, xFinal, xValue, xInverse, xDestroy},
                                      uninstall);
        rc = sqlite3CreateWindowFunction(pDb, funcName, nArg, eTextRep,
                                         pApp, ...funcArgs);
      }catch(e){
        console.error("sqlite3_create_window_function() setup threw:",e);
        for(let v of uninstall){
          wasm.uninstallFunction(v);
        }
        rc = util.sqlite3_wasm_db_error(pDb, capi.SQLITE_ERROR,
                                        "Creation of UDF threw: "+e.message);
      }
      return rc;
    };
    /**
       A helper for UDFs implemented in JS and bound to WASM by the
       client. Given a JS value, udfSetResult(pCtx,X) calls one of the
       sqlite3_result_xyz(pCtx,...)  routines, depending on X's data
       type:

       - `null`: sqlite3_result_null()
       - `boolean`: sqlite3_result_int()
       - `number`: sqlite3_result_int() or sqlite3_result_double()
       - `string`: sqlite3_result_text()
       - Uint8Array or Int8Array: sqlite3_result_blob()
       - `undefined`: indicates that the UDF called one of the
         `sqlite3_result_xyz()` routines on its own, making this
         function a no-op. Results are _undefined_ if this function is
         passed the `undefined` value but did _not_ call one of the
         `sqlite3_result_xyz()` routines.

       Anything else triggers sqlite3_result_error().
    */
    capi.sqlite3_create_function_v2.udfSetResult =
      capi.sqlite3_create_function.udfSetResult =
      capi.sqlite3_create_window_function.udfSetResult = __udfSetResult;

    /**
       A helper for UDFs implemented in JS and bound to WASM by the
       client. When passed the
       (argc,argv) values from the UDF-related functions which receive
       them (xFunc, xStep, xInverse), it creates a JS array
       representing those arguments, converting each to JS in a manner
       appropriate to its data type: numeric, text, blob
       (Uint8Array), or null.

       Results are undefined if it's passed anything other than those
       two arguments from those specific contexts.

       Thus an argc of 4 will result in a length-4 array containing
       the converted values from the corresponding argv.

       The conversion will throw only on allocation error or an internal
       error.
    */
    capi.sqlite3_create_function_v2.udfConvertArgs =
      capi.sqlite3_create_function.udfConvertArgs =
      capi.sqlite3_create_window_function.udfConvertArgs = __udfConvertArgs;

    /**
       A helper for UDFs implemented in JS and bound to WASM by the
       client. It expects to be a passed `(sqlite3_context*, Error)`
       (an exception object or message string). And it sets the
       current UDF's result to sqlite3_result_error_nomem() or
       sqlite3_result_error(), depending on whether the 2nd argument
       is a sqlite3.WasmAllocError object or not.
    */
    capi.sqlite3_create_function_v2.udfSetError =
      capi.sqlite3_create_function.udfSetError =
      capi.sqlite3_create_window_function.udfSetError = __udfSetError;

  }/*sqlite3_create_function_v2() and sqlite3_create_window_function() proxies*/;

  if(1){/* Special-case handling of sqlite3_prepare_v2() and
           sqlite3_prepare_v3() */
    /**
       Helper for string:flexible conversions which require a
       byte-length counterpart argument. Passed a value and its
       ostensible length, this function returns [V,N], where V
       is either v or a transformed copy of v and N is either n,
       -1, or the byte length of v (if it's a byte array).
    */
    const __flexiString = (v,n)=>{
      if('string'===typeof v){
        n = -1;
      }else if(util.isSQLableTypedArray(v)){
        n = v.byteLength;
        v = util.typedArrayToString(v);
      }else if(Array.isArray(v)){
        v = v.join("");
        n = -1;
      }
      return [v, n];
    };

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

    /* Documented in the capi object's initializer. */
    capi.sqlite3_prepare_v3 = function f(pDb, sql, sqlLen, prepFlags, ppStmt, pzTail){
      if(f.length!==arguments.length){
        return __dbArgcMismatch(pDb,"sqlite3_prepare_v3",f.length);
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

    /* Documented in the capi object's initializer. */
    capi.sqlite3_prepare_v2 = function f(pDb, sql, sqlLen, ppStmt, pzTail){
      return (f.length===arguments.length)
        ? capi.sqlite3_prepare_v3(pDb, sql, sqlLen, 0, ppStmt, pzTail)
        : __dbArgcMismatch(pDb,"sqlite3_prepare_v2",f.length);
    };
  }/*sqlite3_prepare_v2/v3()*/;

  {/* Import C-level constants and structs... */
    const cJson = wasm.xCall('sqlite3_wasm_enum_json');
    if(!cJson){
      toss("Maintenance required: increase sqlite3_wasm_enum_json()'s",
           "static buffer size!");
    }
    wasm.ctype = JSON.parse(wasm.cstrToJs(cJson));
    //console.debug('wasm.ctype length =',wasm.cstrlen(cJson));
    const defineGroups = ['access', 'authorizer',
                          'blobFinalizers', 'dataTypes',
                          'dbConfig', 'dbStatus',
                          'encodings', 'fcntl', 'flock', 'ioCap',
                          'limits', 'openFlags',
                          'prepareFlags', 'resultCodes',
                          'serialize', 'sqlite3Status',
                          'stmtStatus', 'syncFlags',
                          'trace', 'udfFlags',
                          'version' ];
    if(wasm.bigIntEnabled){
      defineGroups.push('vtab');
    }
    for(const t of defineGroups){
      for(const e of Object.entries(wasm.ctype[t])){
        // ^^^ [k,v] there triggers a buggy code transformation via
        // one of the Emscripten-driven optimizers.
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
    capi.sqlite3_js_rc_str = (rc)=>__rcMap[rc];
    /* Bind all registered C-side structs... */
    const notThese = Object.assign(Object.create(null),{
      // For each struct to NOT register, map its name to true:
      WasmTestStruct: true,
      /* We unregister the kvvfs VFS from Worker threads below. */
      sqlite3_kvvfs_methods: !util.isUIThread(),
      /* sqlite3_index_info and friends require int64: */
      sqlite3_index_info: !wasm.bigIntEnabled,
      sqlite3_index_constraint: !wasm.bigIntEnabled,
      sqlite3_index_orderby: !wasm.bigIntEnabled,
      sqlite3_index_constraint_usage: !wasm.bigIntEnabled
    });
    for(const s of wasm.ctype.structs){
      if(!notThese[s.name]){
        capi[s.name] = sqlite3.StructBinder(s);
      }
    }
    if(capi.sqlite3_index_info){
      /* Move these inner structs into sqlite3_index_info.  Binding
      ** them to WASM requires that we create global-scope structs to
      ** model them with, but those are no longer needed after we've
      ** passed them to StructBinder. */
      for(const k of ['sqlite3_index_constraint',
                      'sqlite3_index_orderby',
                      'sqlite3_index_constraint_usage']){
        capi.sqlite3_index_info[k] = capi[k];
        delete capi[k];
      }
      capi.sqlite3_vtab_config =
        (pDb, op, arg=0)=>wasm.exports.sqlite3_wasm_vtab_config(
          wasm.xWrap.argAdapter('sqlite3*')(pDb), op, arg);
    }/* end vtab-related setup */
  }/*end C constant and struct imports*/

  const pKvvfs = capi.sqlite3_vfs_find("kvvfs");
  if( pKvvfs ){/* kvvfs-specific glue */
    if(util.isUIThread()){
      const kvvfsMethods = new capi.sqlite3_kvvfs_methods(
        wasm.exports.sqlite3_wasm_kvvfs_methods()
      );
      delete capi.sqlite3_kvvfs_methods;

      const kvvfsMakeKey = wasm.exports.sqlite3_wasm_kvvfsMakeKeyOnPstack,
            pstack = wasm.pstack;

      const kvvfsStorage = (zClass)=>
            ((115/*=='s'*/===wasm.peek(zClass))
             ? sessionStorage : localStorage);

      /**
         Implementations for members of the object referred to by
         sqlite3_wasm_kvvfs_methods(). We swap out the native
         implementations with these, which use localStorage or
         sessionStorage for their backing store.
      */
      const kvvfsImpls = {
        xRead: (zClass, zKey, zBuf, nBuf)=>{
          const stack = pstack.pointer,
                astack = wasm.scopedAllocPush();
          try {
            const zXKey = kvvfsMakeKey(zClass,zKey);
            if(!zXKey) return -3/*OOM*/;
            const jKey = wasm.cstrToJs(zXKey);
            const jV = kvvfsStorage(zClass).getItem(jKey);
            if(!jV) return -1;
            const nV = jV.length /* Note that we are relying 100% on v being
                                    ASCII so that jV.length is equal to the
                                    C-string's byte length. */;
            if(nBuf<=0) return nV;
            else if(1===nBuf){
              wasm.poke(zBuf, 0);
              return nV;
            }
            const zV = wasm.scopedAllocCString(jV);
            if(nBuf > nV + 1) nBuf = nV + 1;
            wasm.heap8u().copyWithin(zBuf, zV, zV + nBuf - 1);
            wasm.poke(zBuf + nBuf - 1, 0);
            return nBuf - 1;
          }catch(e){
            console.error("kvstorageRead()",e);
            return -2;
          }finally{
            pstack.restore(stack);
            wasm.scopedAllocPop(astack);
          }
        },
        xWrite: (zClass, zKey, zData)=>{
          const stack = pstack.pointer;
          try {
            const zXKey = kvvfsMakeKey(zClass,zKey);
            if(!zXKey) return 1/*OOM*/;
            const jKey = wasm.cstrToJs(zXKey);
            kvvfsStorage(zClass).setItem(jKey, wasm.cstrToJs(zData));
            return 0;
          }catch(e){
            console.error("kvstorageWrite()",e);
            return capi.SQLITE_IOERR;
          }finally{
            pstack.restore(stack);
          }
        },
        xDelete: (zClass, zKey)=>{
          const stack = pstack.pointer;
          try {
            const zXKey = kvvfsMakeKey(zClass,zKey);
            if(!zXKey) return 1/*OOM*/;
            kvvfsStorage(zClass).removeItem(wasm.cstrToJs(zXKey));
            return 0;
          }catch(e){
            console.error("kvstorageDelete()",e);
            return capi.SQLITE_IOERR;
          }finally{
            pstack.restore(stack);
          }
        }
      }/*kvvfsImpls*/;
      for(const k of Object.keys(kvvfsImpls)){
        kvvfsMethods[kvvfsMethods.memberKey(k)] =
          wasm.installFunction(
            kvvfsMethods.memberSignature(k),
            kvvfsImpls[k]
          );
      }
    }else{
      /* Worker thread: unregister kvvfs to avoid it being used
         for anything other than local/sessionStorage. It "can"
         be used that way but it's not really intended to be. */
      capi.sqlite3_vfs_unregister(pKvvfs);
    }
  }/*pKvvfs*/

});
