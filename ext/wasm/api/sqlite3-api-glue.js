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
     Signatures for the WASM-exported C-side functions. Each entry
     is an array with 2+ elements:

     [ "c-side name",
       "result type" (wasm.xWrap() syntax),
       [arg types in xWrap() syntax]
       // ^^^ this needn't strictly be an array: it can be subsequent
       // elements instead: [x,y,z] is equivalent to x,y,z
     ]

     Note that support for the API-specific data types in the
     result/argument type strings gets plugged in at a later phase in
     the API initialization process.
  */
  wasm.bindingSignatures = [
    // Please keep these sorted by function name!
    ["sqlite3_aggregate_context","void*", "sqlite3_context*", "int"],
    ["sqlite3_bind_blob","int", "sqlite3_stmt*", "int", "*", "int", "*"
     /* TODO: we should arguably write a custom wrapper which knows
        how to handle Blob, TypedArrays, and JS strings. */
    ],
    ["sqlite3_bind_double","int", "sqlite3_stmt*", "int", "f64"],
    ["sqlite3_bind_int","int", "sqlite3_stmt*", "int", "int"],
    ["sqlite3_bind_null",undefined, "sqlite3_stmt*", "int"],
    ["sqlite3_bind_parameter_count", "int", "sqlite3_stmt*"],
    ["sqlite3_bind_parameter_index","int", "sqlite3_stmt*", "string"],
    ["sqlite3_bind_pointer", "int",
     "sqlite3_stmt*", "int", "*", "string:static", "*"],
    ["sqlite3_bind_text","int", "sqlite3_stmt*", "int", "string", "int", "int"
     /* We should arguably create a hand-written binding of
        bind_text() which does more flexible text conversion, along
        the lines of sqlite3_prepare_v3(). The slightly problematic
        part is the final argument (text destructor). */
    ],
    //["sqlite3_busy_handler","int", "sqlite3*", "*", "*"],
    // ^^^^ TODO: custom binding which auto-converts JS function arg
    // to a WASM function, noting that calling it multiple times
    // would introduce a leak.
    ["sqlite3_busy_timeout","int", "sqlite3*", "int"],
    ["sqlite3_close_v2", "int", "sqlite3*"],
    ["sqlite3_changes", "int", "sqlite3*"],
    ["sqlite3_clear_bindings","int", "sqlite3_stmt*"],
    ["sqlite3_collation_needed", "int", "sqlite3*", "*", "*"/*=>v(ppis)*/],
    ["sqlite3_column_blob","*", "sqlite3_stmt*", "int"],
    ["sqlite3_column_bytes","int", "sqlite3_stmt*", "int"],
    ["sqlite3_column_count", "int", "sqlite3_stmt*"],
    ["sqlite3_column_double","f64", "sqlite3_stmt*", "int"],
    ["sqlite3_column_int","int", "sqlite3_stmt*", "int"],
    ["sqlite3_column_name","string", "sqlite3_stmt*", "int"],
    ["sqlite3_column_text","string", "sqlite3_stmt*", "int"],
    ["sqlite3_column_type","int", "sqlite3_stmt*", "int"],
    ["sqlite3_column_value","sqlite3_value*", "sqlite3_stmt*", "int"],
    ["sqlite3_compileoption_get", "string", "int"],
    ["sqlite3_compileoption_used", "int", "string"],
    ["sqlite3_complete", "int", "string:flexible"],
    /* sqlite3_create_function(), sqlite3_create_function_v2(), and
       sqlite3_create_window_function() use hand-written bindings to
       simplify handling of their function-type arguments. */
    /* sqlite3_create_collation() and sqlite3_create_collation_v2()
       use hand-written bindings to simplify passing of the callback
       function.
      ["sqlite3_create_collation", "int",
     "sqlite3*", "string", "int",//SQLITE_UTF8 is the only legal value
     "*", "*"],
    ["sqlite3_create_collation_v2", "int",
     "sqlite3*", "string", "int",//SQLITE_UTF8 is the only legal value
     "*", "*", "*"],
    */
    ["sqlite3_data_count", "int", "sqlite3_stmt*"],
    ["sqlite3_db_filename", "string", "sqlite3*", "string"],
    ["sqlite3_db_handle", "sqlite3*", "sqlite3_stmt*"],
    ["sqlite3_db_name", "string", "sqlite3*", "int"],
    ["sqlite3_db_status", "int", "sqlite3*", "int", "*", "*", "int"],
    ["sqlite3_deserialize", "int", "sqlite3*", "string", "*", "i64", "i64", "int"]
    /* Careful! Short version: de/serialize() are problematic because they
       might use a different allocator than the user for managing the
       deserialized block. de/serialize() are ONLY safe to use with
       sqlite3_malloc(), sqlite3_free(), and its 64-bit variants. */,
    ["sqlite3_errcode", "int", "sqlite3*"],
    ["sqlite3_errmsg", "string", "sqlite3*"],
    ["sqlite3_error_offset", "int", "sqlite3*"],
    ["sqlite3_errstr", "string", "int"],
    /*["sqlite3_exec", "int", "sqlite3*", "string", "*", "*", "**"
      Handled seperately to perform translation of the callback
      into a WASM-usable one. ],*/
    ["sqlite3_expanded_sql", "string", "sqlite3_stmt*"],
    ["sqlite3_extended_errcode", "int", "sqlite3*"],
    ["sqlite3_extended_result_codes", "int", "sqlite3*", "int"],
    ["sqlite3_file_control", "int", "sqlite3*", "string", "int", "*"],
    ["sqlite3_finalize", "int", "sqlite3_stmt*"],
    ["sqlite3_free", undefined,"*"],
    ["sqlite3_get_auxdata", "*", "sqlite3_context*", "int"],
    ["sqlite3_initialize", undefined],
    /*["sqlite3_interrupt", undefined, "sqlite3*"
       ^^^ we cannot actually currently support this because JS is
        single-threaded and we don't have a portable way to access a DB
        from 2 SharedWorkers concurrently. ],*/
    ["sqlite3_keyword_count", "int"],
    ["sqlite3_keyword_name", "int", ["int", "**", "*"]],
    ["sqlite3_keyword_check", "int", ["string", "int"]],
    ["sqlite3_libversion", "string"],
    ["sqlite3_libversion_number", "int"],
    ["sqlite3_limit", "int", ["sqlite3*", "int", "int"]],
    ["sqlite3_malloc", "*","int"],
    ["sqlite3_open", "int", "string", "*"],
    ["sqlite3_open_v2", "int", "string", "*", "int", "string"],
    /* sqlite3_prepare_v2() and sqlite3_prepare_v3() are handled
       separately due to us requiring two different sets of semantics
       for those, depending on how their SQL argument is provided. */
    /* sqlite3_randomness() uses a hand-written wrapper to extend
       the range of supported argument types. */
    [ 
      "sqlite3_progress_handler", undefined, [
        "sqlite3*", "int", new wasm.xWrap.FuncPtrAdapter({
          name: 'xProgressHandler',
          signature: 'i(p)',
          bindScope: 'context',
          contextKey: (argIndex,argv)=>'sqlite3@'+argv[0]
        }), "*"
      ]
    ],
    ["sqlite3_realloc", "*","*","int"],
    ["sqlite3_reset", "int", "sqlite3_stmt*"],
    ["sqlite3_result_blob", undefined, "sqlite3_context*", "*", "int", "*"],
    ["sqlite3_result_double", undefined, "sqlite3_context*", "f64"],
    ["sqlite3_result_error", undefined, "sqlite3_context*", "string", "int"],
    ["sqlite3_result_error_code", undefined, "sqlite3_context*", "int"],
    ["sqlite3_result_error_nomem", undefined, "sqlite3_context*"],
    ["sqlite3_result_error_toobig", undefined, "sqlite3_context*"],
    ["sqlite3_result_int", undefined, "sqlite3_context*", "int"],
    ["sqlite3_result_null", undefined, "sqlite3_context*"],
    ["sqlite3_result_pointer", undefined,
     "sqlite3_context*", "*", "string:static", "*"],
    ["sqlite3_result_subtype", undefined, "sqlite3_value*", "int"],
    ["sqlite3_result_text", undefined, "sqlite3_context*", "string", "int", "*"],
    ["sqlite3_result_zeroblob", undefined, "sqlite3_context*", "int"],
    ["sqlite3_serialize","*", "sqlite3*", "string", "*", "int"],
    ["sqlite3_set_auxdata", undefined, "sqlite3_context*", "int", "*", "*"/* => v(*) */],
    ["sqlite3_shutdown", undefined],
    ["sqlite3_sourceid", "string"],
    ["sqlite3_sql", "string", "sqlite3_stmt*"],
    ["sqlite3_status", "int", "int", "*", "*", "int"],
    ["sqlite3_step", "int", "sqlite3_stmt*"],
    ["sqlite3_stmt_isexplain", "int", ["sqlite3_stmt*"]],
    ["sqlite3_stmt_readonly", "int", ["sqlite3_stmt*"]],
    ["sqlite3_stmt_status", "int", "sqlite3_stmt*", "int", "int"],
    ["sqlite3_strglob", "int", "string","string"],
    ["sqlite3_stricmp", "int", "string", "string"],
    ["sqlite3_strlike", "int", "string", "string","int"],
    ["sqlite3_strnicmp", "int", "string", "string", "int"],
    ["sqlite3_table_column_metadata", "int",
     "sqlite3*", "string", "string", "string",
     "**", "**", "*", "*", "*"],
    ["sqlite3_total_changes", "int", "sqlite3*"],
    ["sqlite3_trace_v2", "int", "sqlite3*", "int",
     new wasm.xWrap.FuncPtrAdapter({
       name: 'sqlite3_trace_v2::callback',
       signature: 'i(ippp)',
       contextKey: (argIndex, argv)=>'sqlite3@'+argv[0]
     }), "*"],
    ["sqlite3_txn_state", "int", ["sqlite3*","string"]],
    /* Note that sqlite3_uri_...() have very specific requirements for
       their first C-string arguments, so we cannot perform any value
       conversion on those. */
    ["sqlite3_uri_boolean", "int", "sqlite3_filename", "string", "int"],
    ["sqlite3_uri_key", "string", "sqlite3_filename", "int"],
    ["sqlite3_uri_parameter", "string", "sqlite3_filename", "string"],
    ["sqlite3_user_data","void*", "sqlite3_context*"],
    ["sqlite3_value_blob", "*", "sqlite3_value*"],
    ["sqlite3_value_bytes","int", "sqlite3_value*"],
    ["sqlite3_value_double","f64", "sqlite3_value*"],
    ["sqlite3_value_dup", "sqlite3_value*", "sqlite3_value*"],
    ["sqlite3_value_free", undefined, "sqlite3_value*"],
    ["sqlite3_value_frombind", "int", "sqlite3_value*"],
    ["sqlite3_value_int","int", "sqlite3_value*"],
    ["sqlite3_value_nochange", "int", "sqlite3_value*"],
    ["sqlite3_value_numeric_type", "int", "sqlite3_value*"],
    ["sqlite3_value_pointer", "*", "sqlite3_value*", "string:static"],
    ["sqlite3_value_subtype", "int", "sqlite3_value*"],
    ["sqlite3_value_text", "string", "sqlite3_value*"],
    ["sqlite3_value_type", "int", "sqlite3_value*"],
    ["sqlite3_vfs_find", "*", "string"],
    ["sqlite3_vfs_register", "int", "sqlite3_vfs*", "int"],
    ["sqlite3_vfs_unregister", "int", "sqlite3_vfs*"]
  ]/*wasm.bindingSignatures*/;

  if(false && wasm.compileOptionUsed('SQLITE_ENABLE_NORMALIZE')){
    /* ^^^ "the problem" is that this is an option feature and the
       build-time function-export list does not currently take
       optional features into account. */
    wasm.bindingSignatures.push(["sqlite3_normalized_sql", "string", "sqlite3_stmt*"]);
  }
  
  /**
     Functions which require BigInt (int64) support are separated from
     the others because we need to conditionally bind them or apply
     dummy impls, depending on the capabilities of the environment.

     Note that not all of these functions directly require int64
     but are only for use with APIs which require int64. For example,
     the vtab-related functions.
  */
  wasm.bindingSignatures.int64 = [
    ["sqlite3_bind_int64","int", ["sqlite3_stmt*", "int", "i64"]],
    ["sqlite3_changes64","i64", ["sqlite3*"]],
    ["sqlite3_column_int64","i64", ["sqlite3_stmt*", "int"]],
    ["sqlite3_create_module", "int",
     ["sqlite3*","string","sqlite3_module*","*"]],
    ["sqlite3_create_module_v2", "int",
     ["sqlite3*","string","sqlite3_module*","*","*"]],
    ["sqlite3_declare_vtab", "int", ["sqlite3*", "string:flexible"]],
    ["sqlite3_drop_modules", "int", ["sqlite3*", "**"]],
    ["sqlite3_last_insert_rowid", "i64", ["sqlite3*"]],
    ["sqlite3_malloc64", "*","i64"],
    ["sqlite3_msize", "i64", "*"],
    ["sqlite3_overload_function", "int", ["sqlite3*","string","int"]],
    ["sqlite3_realloc64", "*","*", "i64"],
    ["sqlite3_result_int64", undefined, "*", "i64"],
    ["sqlite3_result_zeroblob64", "int", "*", "i64"],
    ["sqlite3_set_last_insert_rowid", undefined, ["sqlite3*", "i64"]],
    ["sqlite3_status64", "int", "int", "*", "*", "int"],
    ["sqlite3_total_changes64", "i64", ["sqlite3*"]],
    ["sqlite3_uri_int64", "i64", ["sqlite3_filename", "string", "i64"]],
    ["sqlite3_value_int64","i64", "sqlite3_value*"],
    ["sqlite3_vtab_collation","string","sqlite3_index_info*","int"],
    ["sqlite3_vtab_distinct","int", "sqlite3_index_info*"],
    ["sqlite3_vtab_in","int", "sqlite3_index_info*", "int", "int"],
    ["sqlite3_vtab_in_first", "int", "sqlite3_value*", "**"],
    ["sqlite3_vtab_in_next", "int", "sqlite3_value*", "**"],
    /*["sqlite3_vtab_config" is variadic and requires a hand-written
      proxy.] */
    ["sqlite3_vtab_nochange","int", "sqlite3_context*"],
    ["sqlite3_vtab_on_conflict","int", "sqlite3*"],
    ["sqlite3_vtab_rhs_value","int", "sqlite3_index_info*", "int", "**"]
  ];

  /**
     Functions which are intended solely for API-internal use by the
     WASM components, not client code. These get installed into
     sqlite3.wasm. Some of them get exposed to clients via variants
     named sqlite3_js_...().
  */
  wasm.bindingSignatures.wasm = [
    ["sqlite3_wasm_db_reset", "int", "sqlite3*"],
    ["sqlite3_wasm_db_vfs", "sqlite3_vfs*", "sqlite3*","string"],
    ["sqlite3_wasm_vfs_create_file", "int",
     "sqlite3_vfs*","string","*", "int"],
    ["sqlite3_wasm_vfs_unlink", "int", "sqlite3_vfs*","string"]
  ];

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
    ('sqlite3_value*', rPtr)
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
         - (sqlite3*, Error e [,string msg = ''+e])

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
          message = message || ''+resultCode;
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

    const __collationContextKey = (argIndex,argv)=>{
      return 'argv['+argIndex+']:sqlite3@'+argv[0]+
        ':'+wasm.cstrToJs(argv[1]).toLowerCase()
    };
    const __ccv2 = wasm.xWrap(
      'sqlite3_create_collation_v2', 'int',
      'sqlite3*','string','int','*',
      new wasm.xWrap.FuncPtrAdapter({
        /* int(*xCompare)(void*,int,const void*,int,const void*) */
        name: 'sqlite3_create_collation_v2::xCompare',
        signature: 'i(pipip)',
        bindScope: 'context',
        contextKey: __collationContextKey
      }),
      new wasm.xWrap.FuncPtrAdapter({
        /* void(*xDestroy(void*) */
        name: 'sqlite3_create_collation_v2::xDestroy',
        signature: 'v(p)',
        bindScope: 'context',
        contextKey: __collationContextKey
      })
    );

    /**
       Works exactly like C's sqlite3_create_collation_v2() except that:

       1) It accepts JS functions for its function-pointer arguments,
          for which it will install WASM-bound proxies. The bindings
          are "permanent," in that they will stay in the WASM environment
          until it shuts down unless the client somehow finds and removes
          them.

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
        rc = __ccv2(pDb, zName, eTextRep, pArg, xCompare, xDestroy);
      }catch(e){
        rc = util.sqlite3_wasm_db_error(pDb, e);
      }
      return rc;
    };

    capi.sqlite3_create_collation = (pDb,zName,eTextRep,pArg,xCompare)=>{
      return (5===arguments.length)
        ? capi.sqlite3_create_collation_v2(pDb,zName,eTextRep,pArg,xCompare,0)
        : __dbArgcMismatch(pDb, 'sqlite3_create_collation', 5);
    };

  }/*sqlite3_create_collation() and friends*/

  if(1){/* Special-case handling of sqlite3_exec() */
    const __exec = wasm.xWrap("sqlite3_exec", "int",
                              ["sqlite3*", "string:flexible",
                               new wasm.xWrap.FuncPtrAdapter({
                                 signature: 'i(pipp)',
                                 bindScope: 'transient'
                               }), "*", "**"]);
    /* Documented in the api object's initializer. */
    capi.sqlite3_exec = function f(pDb, sql, callback, pVoid, pErrMsg){
      if(f.length!==arguments.length){
        return __dbArgcMismatch(pDb,"sqlite3_exec",f.length);
      }else if(!(callback instanceof Function)){
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
      let rc;
      try{
        rc = __exec(pDb, sql, cbwrap, pVoid, pErrMsg);
      }catch(e){
        rc = util.sqlite3_wasm_db_error(pDb, capi.SQLITE_ERROR,
                                        "Error running exec(): "+e);
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

    const __xFunc = function(callback){
      return function(pCtx, argc, pArgv){
        try{
          capi.sqlite3_result_js(
            pCtx,
            callback(pCtx, ...capi.sqlite3_values_to_js(argc, pArgv))
          );
        }catch(e){
          //console.error('xFunc() caught:',e);
          capi.sqlite3_result_error_js(pCtx, e);
        }
      };
    };

    const __xInverseAndStep = function(callback){
      return function(pCtx, argc, pArgv){
        try{ callback(pCtx, ...capi.sqlite3_values_to_js(argc, pArgv)) }
        catch(e){ capi.sqlite3_result_error_js(pCtx, e) }
      };
    };

    const __xFinalAndValue = function(callback){
      return function(pCtx){
        try{ capi.sqlite3_result_js(pCtx, callback(pCtx)) }
        catch(e){ capi.sqlite3_result_error_js(pCtx, e) }
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
       A _deprecated_ alias for capi.sqlite3_result_js() which
       predates the addition of that function in the public API.
    */
    capi.sqlite3_create_function_v2.udfSetResult =
      capi.sqlite3_create_function.udfSetResult =
      capi.sqlite3_create_window_function.udfSetResult = capi.sqlite3_result_js;

    /**
       A _deprecated_ alias for capi.sqlite3_values_to_js() which
       predates the addition of that function in the public API.
    */
    capi.sqlite3_create_function_v2.udfConvertArgs =
      capi.sqlite3_create_function.udfConvertArgs =
      capi.sqlite3_create_window_function.udfConvertArgs = capi.sqlite3_values_to_js;

    /**
       A _deprecated_ alias for capi.sqlite3_result_error_js() which
       predates the addition of that function in the public API.
    */
    capi.sqlite3_create_function_v2.udfSetError =
      capi.sqlite3_create_function.udfSetError =
      capi.sqlite3_create_window_function.udfSetError = capi.sqlite3_result_error_js;

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
                          'trace', 'txnState', 'udfFlags',
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
