/*
  2022-05-22

  The author disclaims copyright to this source code.  In place of a
  legal notice, here is a blessing:

  *   May you do good and not evil.
  *   May you find forgiveness for yourself and forgive others.
  *   May you share freely, never taking more than you give.

  ***********************************************************************

  This file is intended to be combined at build-time with other
  related code, most notably a header and footer which wraps this whole
  file into an Emscripten Module.postRun() handler which has a parameter
  named "Module" (the Emscripten Module object). The exact requirements,
  conventions, and build process are very much under construction and
  will be (re)documented once they've stopped fluctuating so much.

  Specific goals of this project:

  - Except where noted in the non-goals, provide a more-or-less
    feature-complete wrapper to the sqlite3 C API, insofar as WASM
    feature parity with C allows for. In fact, provide at least 3
    APIs...

    1) Bind a low-level sqlite3 API which is as close to the native
       one as feasible in terms of usage.

    2) A higher-level API, more akin to sql.js and node.js-style
       implementations. This one speaks directly to the low-level
       API. This API must be used from the same thread as the
       low-level API.

    3) A second higher-level API which speaks to the previous APIs via
       worker messages. This one is intended for use in the main
       thread, with the lower-level APIs installed in a Worker thread,
       and talking to them via Worker messages. Because Workers are
       asynchronouns and have only a single message channel, some
       acrobatics are needed here to feed async work results back to
       the client (as we cannot simply pass around callbacks between
       the main and Worker threads).

  - Insofar as possible, support client-side storage using JS
    filesystem APIs. As of this writing, such things are still very
    much TODO. Initial testing with using IndexedDB as backing storage
    showed it to work reasonably well, but it's also too easy to
    corrupt by using a web page in two browser tabs because IndexedDB
    lacks the locking features needed to support that.

  Specific non-goals of this project:

  - As WASM is a web-centric technology and UTF-8 is the King of
    Encodings in that realm, there are no currently plans to support
    the UTF16-related sqlite3 APIs. They would add a complication to
    the bindings for no appreciable benefit. Though web-related
    implementation details take priority, the lower-level WASM module
    "should" work in non-web WASM environments.

  - Supporting old or niche-market platforms. WASM is built for a
    modern web and requires modern platforms.

  - Though scalar User-Defined Functions (UDFs) may be created in
    JavaScript, there are currently no plans to add support for
    aggregate and window functions.

  Attribution:

  This project is endebted to the work of sql.js:

  https://github.com/sql-js/sql.js

  sql.js was an essential stepping stone in this code's development as
  it demonstrated how to handle some of the WASM-related voodoo (like
  handling pointers-to-pointers and adding JS implementations of
  C-bound callback functions). These APIs have a considerably
  different shape than sql.js's, however.
*/

/**
   This global symbol is is only a temporary measure: the JS-side
   post-processing will remove that object from the global scope when
   setup is complete. We require it there temporarily in order to glue
   disparate parts together during the loading of the API (which spans
   several components).

   This function requires a configuration object intended to abstract
   away details specific to any given WASM environment, primarily so
   that it can be used without any _direct_ dependency on Emscripten.
   (That said, OO API #1 requires, as of this writing, Emscripten's
   virtual filesystem API. Baby steps.)
*/
self.sqlite3ApiBootstrap = function(config){
  'use strict';

  /** Throws a new Error, the message of which is the concatenation
      all args with a space between each. */
  const toss = (...args)=>{throw new Error(args.join(' '))};

  /**
     Returns true if n is a 32-bit (signed) integer, else
     false. This is used for determining when we need to switch to
     double-type DB operations for integer values in order to keep
     more precision.
  */
  const isInt32 = function(n){
    return ('bigint'!==typeof n /*TypeError: can't convert BigInt to number*/)
      && !!(n===(n|0) && n<=2147483647 && n>=-2147483648);
  };

  /** Returns v if v appears to be a TypedArray, else false. */
  const isTypedArray = (v)=>{
    return (v && v.constructor && isInt32(v.constructor.BYTES_PER_ELEMENT)) ? v : false;
  };

  /**
     Returns true if v appears to be one of our bind()-able
     TypedArray types: Uint8Array or Int8Array. Support for
     TypedArrays with element sizes >1 is TODO.
  */
  const isBindableTypedArray = (v)=>{
    return v && v.constructor && (1===v.constructor.BYTES_PER_ELEMENT);
  };

  /**
     Returns true if v appears to be one of the TypedArray types
     which is legal for holding SQL code (as opposed to binary blobs).

     Currently this is the same as isBindableTypedArray() but it
     seems likely that we'll eventually want to add Uint32Array
     and friends to the isBindableTypedArray() list but not to the
     isSQLableTypedArray() list.
  */
  const isSQLableTypedArray = (v)=>{
    return v && v.constructor && (1===v.constructor.BYTES_PER_ELEMENT);
  };

  /** Returns true if isBindableTypedArray(v) does, else throws with a message
      that v is not a supported TypedArray value. */
  const affirmBindableTypedArray = (v)=>{
    return isBindableTypedArray(v)
      || toss("Value is not of a supported TypedArray type.");
  };

  const utf8Decoder = new TextDecoder('utf-8');
  const typedArrayToString = (str)=>utf8Decoder.decode(str);

  /**
     An Error subclass specifically for reporting Wasm-level malloc()
     failure and enabling clients to unambiguously identify such
     exceptions.
  */
  class WasmAllocError extends Error {
    constructor(...args){
      super(...args);
      this.name = 'WasmAllocError';
    }
  };

  /** 
      The main sqlite3 binding API gets installed into this object,
      mimicking the C API as closely as we can. The numerous members
      names with prefixes 'sqlite3_' and 'SQLITE_' behave, insofar as
      possible, identically to the C-native counterparts, as documented at:

      https://www.sqlite.org/c3ref/intro.html

      A very few exceptions require an additional level of proxy
      function or may otherwise require special attention in the WASM
      environment, and all such cases are document here. Those not
      documented here are installed as 1-to-1 proxies for their C-side
      counterparts.
  */
  const capi = {
    /**
       An Error subclass which is thrown by this object's alloc() method
       on OOM.
    */
    WasmAllocError: WasmAllocError,
    /**
       The API's one single point of access to the WASM-side memory
       allocator. Works like malloc(3) (and is likely bound to
       malloc()) but throws an WasmAllocError if allocation fails. It is
       important that any code which might pass through the sqlite3 C
       API NOT throw and must instead return SQLITE_NOMEM (or
       equivalent, depending on the context).

       That said, very few cases in the API can result in
       client-defined functions propagating exceptions via the C-style
       API. Most notably, this applies ot User-defined SQL Functions
       (UDFs) registered via sqlite3_create_function_v2(). For that
       specific case it is recommended that all UDF creation be
       funneled through a utility function and that a wrapper function
       be added around the UDF which catches any exception and sets
       the error state to OOM. (The overall complexity of registering
       UDFs essentially requires a helper for doing so!)
    */
    alloc: undefined/*installed later*/,
    /**
       The API's one single point of access to the WASM-side memory
       deallocator. Works like free(3) (and is likely bound to
       free()).
    */
    dealloc: undefined/*installed later*/,
    /**
       When using sqlite3_open_v2() it is important to keep the following
       in mind:

       https://www.sqlite.org/c3ref/open.html

       - The flags for use with its 3rd argument are installed in this
       object using the C-cide names, e.g. SQLITE_OPEN_CREATE.

       - If the combination of flags passed to it are invalid,
       behavior is undefined. Thus is is never okay to call this
       with fewer than 3 arguments, as JS will default the
       missing arguments to `undefined`, which will result in a
       flag value of 0. Most of the available SQLITE_OPEN_xxx
       flags are meaningless in the WASM build, e.g. the mutext-
       and cache-related flags, but they are retained in this
       API for consistency's sake.

       - The final argument to this function specifies the VFS to
       use, which is largely (but not entirely!) meaningless in
       the WASM environment. It should always be null or
       undefined, and it is safe to elide that argument when
       calling this function.
    */
    sqlite3_open_v2: function(filename,dbPtrPtr,flags,vfsStr){}/*installed later*/,
    /**
       The sqlite3_prepare_v3() binding handles two different uses
       with differing JS/WASM semantics:

       1) sqlite3_prepare_v3(pDb, sqlString, -1, prepFlags, ppStmt [, null])

       2) sqlite3_prepare_v3(pDb, sqlPointer, sqlByteLen, prepFlags, ppStmt, sqlPointerToPointer)

       Note that the SQL length argument (the 3rd argument) must, for
       usage (1), always be negative because it must be a byte length
       and that value is expensive to calculate from JS (where only
       the character length of strings is readily available). It is
       retained in this API's interface for code/documentation
       compatibility reasons but is currently _always_ ignored. With
       usage (2), the 3rd argument is used as-is but is is still
       critical that the C-style input string (2nd argument) be
       terminated with a 0 byte.

       In usage (1), the 2nd argument must be of type string,
       Uint8Array, or Int8Array (either of which is assumed to
       hold SQL). If it is, this function assumes case (1) and
       calls the underyling C function with the equivalent of:

       (pDb, sqlAsString, -1, prepFlags, ppStmt, null)

       The pzTail argument is ignored in this case because its result
       is meaningless when a string-type value is passed through
       (because the string goes through another level of internal
       conversion for WASM's sake and the result pointer would refer
       to that transient conversion's memory, not the passed-in
       string).

       If the sql argument is not a string, it must be a _pointer_ to
       a NUL-terminated string which was allocated in the WASM memory
       (e.g. using cwapi.wasm.alloc() or equivalent). In that case,
       the final argument may be 0/null/undefined or must be a pointer
       to which the "tail" of the compiled SQL is written, as
       documented for the C-side sqlite3_prepare_v3(). In case (2),
       the underlying C function is called with the equivalent of:

       (pDb, sqlAsPointer, (sqlByteLen||-1), prepFlags, ppStmt, pzTail)

       It returns its result and compiled statement as documented in
       the C API. Fetching the output pointers (5th and 6th
       parameters) requires using capi.wasm.getMemValue() (or
       equivalent) and the pzTail will point to an address relative to
       the sqlAsPointer value.

       If passed an invalid 2nd argument type, this function will
       return SQLITE_MISUSE but will unfortunately be able to return
       any additional error information because we have no way to set
       the db's error state such that this function could return a
       non-0 integer and the client could call sqlite3_errcode() or
       sqlite3_errmsg() to fetch it. See the RFE at:

       https://sqlite.org/forum/forumpost/f9eb79b11aefd4fc81d

       The alternative would be to throw an exception for that case,
       but that would be in strong constrast to the rest of the
       C-level API and seems likely to cause more confusion.

       Side-note: in the C API the function does not fail if provided
       an empty string but its result output pointer will be NULL.
    */
    sqlite3_prepare_v3: function(dbPtr, sql, sqlByteLen, prepFlags,
                                 stmtPtrPtr, strPtrPtr){}/*installed later*/,

    /**
       Equivalent to calling sqlite3_prapare_v3() with 0 as its 4th argument.
    */
    sqlite3_prepare_v2: function(dbPtr, sql, sqlByteLen, stmtPtrPtr,
                                 strPtrPtr){}/*installed later*/,

    /**
       Various internal-use utilities are added here as needed. They
       are bound to an object only so that we have access to them in
       the differently-scoped steps of the API bootstrapping
       process. At the end of the API setup process, this object gets
       removed.
    */
    util:{
      isInt32, isTypedArray, isBindableTypedArray, isSQLableTypedArray,
      affirmBindableTypedArray, typedArrayToString
    },
    
    /**
       Holds state which are specific to the WASM-related
       infrastructure and glue code. It is not expected that client
       code will normally need these, but they're exposed here in case
       it does. These APIs are _not_ to be considered an
       official/stable part of the sqlite3 WASM API. They may change
       as the developers' experience suggests appropriate changes.

       Note that a number of members of this object are injected
       dynamically after the api object is fully constructed, so
       not all are documented inline here.
    */
    wasm: {
    //^^^ TODO?: move wasm from sqlite3.capi.wasm to sqlite3.wasm
      /**
         Emscripten APIs have a deep-seated assumption that all pointers
         are 32 bits. We'll remain optimistic that that won't always be
         the case and will use this constant in places where we might
         otherwise use a hard-coded 4.
      */
      ptrSizeof: config.wasmPtrSizeof || 4,
      /**
         The WASM IR (Intermediate Representation) value for
         pointer-type values. It MUST refer to a value type of the
         size described by this.ptrSizeof _or_ it may be any value
         which ends in '*', which Emscripten's glue code internally
         translates to i32.
      */
      ptrIR: config.wasmPtrIR || "i32",
      /**
         True if BigInt support was enabled via (e.g.) the
         Emscripten -sWASM_BIGINT flag, else false. When
         enabled, certain 64-bit sqlite3 APIs are enabled which
         are not otherwise enabled due to JS/WASM int64
         impedence mismatches.
      */
      bigIntEnabled: !!config.bigIntEnabled,
      /**
         The symbols exported by the WASM environment.
      */
      exports: config.exports
        || toss("Missing API config.exports (WASM module exports)."),

      /**
         When Emscripten compiles with `-sIMPORT_MEMORY`, it
         initalizes the heap and imports it into wasm, as opposed to
         the other way around. In this case, the memory is not
         available via this.exports.memory.
      */
      memory: config.memory || config.exports['memory']
        || toss("API config object requires a WebAssembly.Memory object",
                "in either config.exports.memory (exported)",
                "or config.memory (imported)."),
      /* Many more wasm-related APIs get installed later on. */
    }/*wasm*/
  }/*capi*/;

  /**
     capi.wasm.alloc()'s srcTypedArray.byteLength bytes,
     populates them with the values from the source
     TypedArray, and returns the pointer to that memory. The
     returned pointer must eventually be passed to
     capi.wasm.dealloc() to clean it up.

     As a special case, to avoid further special cases where
     this is used, if srcTypedArray.byteLength is 0, it
     allocates a single byte and sets it to the value
     0. Even in such cases, calls must behave as if the
     allocated memory has exactly srcTypedArray.byteLength
     bytes.

     ACHTUNG: this currently only works for Uint8Array and
     Int8Array types and will throw if srcTypedArray is of
     any other type.
  */
  capi.wasm.mallocFromTypedArray = function(srcTypedArray){
    affirmBindableTypedArray(srcTypedArray);
    const pRet = this.alloc(srcTypedArray.byteLength || 1);
    this.heapForSize(srcTypedArray.constructor).set(srcTypedArray.byteLength ? srcTypedArray : [0], pRet);
    return pRet;
  }.bind(capi.wasm);

  const keyAlloc = config.allocExportName || 'malloc',
        keyDealloc =  config.deallocExportName || 'free';
  for(const key of [keyAlloc, keyDealloc]){
    const f = capi.wasm.exports[key];
    if(!(f instanceof Function)) toss("Missing required exports[",key,"] function.");
  }
  capi.wasm.alloc = function(n){
    const m = this.exports[keyAlloc](n);
    if(!m) throw new WasmAllocError("Failed to allocate "+n+" bytes.");
    return m;
  }.bind(capi.wasm)
  capi.wasm.dealloc = (m)=>capi.wasm.exports[keyDealloc](m);

  /**
     Reports info about compile-time options using
     sqlite_compileoption_get() and sqlite3_compileoption_used(). It
     has several distinct uses:

     If optName is an array then it is expected to be a list of
     compilation options and this function returns an object
     which maps each such option to true or false, indicating
     whether or not the given option was included in this
     build. That object is returned.

     If optName is an object, its keys are expected to be compilation
     options and this function sets each entry to true or false,
     indicating whether the compilation option was used or not. That
     object is returned.

     If passed no arguments then it returns an object mapping
     all known compilation options to their compile-time values,
     or boolean true if they are defined with no value. This
     result, which is relatively expensive to compute, is cached
     and returned for future no-argument calls.

     In all other cases it returns true if the given option was
     active when when compiling the sqlite3 module, else false.

     Compile-time option names may optionally include their
     "SQLITE_" prefix. When it returns an object of all options,
     the prefix is elided.
  */
  capi.wasm.compileOptionUsed = function f(optName){
    if(!arguments.length){
      if(f._result) return f._result;
      else if(!f._opt){
        f._rx = /^([^=]+)=(.+)/;
        f._rxInt = /^-?\d+$/;
        f._opt = function(opt, rv){
          const m = f._rx.exec(opt);
          rv[0] = (m ? m[1] : opt);
          rv[1] = m ? (f._rxInt.test(m[2]) ? +m[2] : m[2]) : true;
        };                    
      }
      const rc = {}, ov = [0,0];
      let i = 0, k;
      while((k = capi.sqlite3_compileoption_get(i++))){
        f._opt(k,ov);
        rc[ov[0]] = ov[1];
      }
      return f._result = rc;
    }else if(Array.isArray(optName)){
      const rc = {};
      optName.forEach((v)=>{
        rc[v] = capi.sqlite3_compileoption_used(v);
      });
      return rc;
    }else if('object' === typeof optName){
      Object.keys(optName).forEach((k)=> {
        optName[k] = capi.sqlite3_compileoption_used(k);
      });
      return optName;
    }
    return (
      'string'===typeof optName
    ) ? !!capi.sqlite3_compileoption_used(optName) : false;
  }/*compileOptionUsed()*/;

  capi.wasm.bindingSignatures = [
    /**
       Signatures for the WASM-exported C-side functions. Each entry
       is an array with 2+ elements:

       ["c-side name",
        "result type" (capi.wasm.xWrap() syntax),
         [arg types in xWrap() syntax]
         // ^^^ this needn't strictly be an array: it can be subsequent
         // elements instead: [x,y,z] is equivalent to x,y,z
       ]
    */
    // Please keep these sorted by function name!
    ["sqlite3_bind_blob","int", "sqlite3_stmt*", "int", "*", "int", "*"],
    ["sqlite3_bind_double","int", "sqlite3_stmt*", "int", "f64"],
    ["sqlite3_bind_int","int", "sqlite3_stmt*", "int", "int"],
    ["sqlite3_bind_null",undefined, "sqlite3_stmt*", "int"],
    ["sqlite3_bind_parameter_count", "int", "sqlite3_stmt*"],
    ["sqlite3_bind_parameter_index","int", "sqlite3_stmt*", "string"],
    ["sqlite3_bind_text","int", "sqlite3_stmt*", "int", "string", "int", "int"],
    ["sqlite3_close_v2", "int", "sqlite3*"],
    ["sqlite3_changes", "int", "sqlite3*"],
    ["sqlite3_clear_bindings","int", "sqlite3_stmt*"],
    ["sqlite3_column_blob","*", "sqlite3_stmt*", "int"],
    ["sqlite3_column_bytes","int", "sqlite3_stmt*", "int"],
    ["sqlite3_column_count", "int", "sqlite3_stmt*"],
    ["sqlite3_column_double","f64", "sqlite3_stmt*", "int"],
    ["sqlite3_column_int","int", "sqlite3_stmt*", "int"],
    ["sqlite3_column_name","string", "sqlite3_stmt*", "int"],
    ["sqlite3_column_text","string", "sqlite3_stmt*", "int"],
    ["sqlite3_column_type","int", "sqlite3_stmt*", "int"],
    ["sqlite3_compileoption_get", "string", "int"],
    ["sqlite3_compileoption_used", "int", "string"],
    ["sqlite3_create_function_v2", "int",
     "sqlite3*", "string", "int", "int", "*", "*", "*", "*", "*"],
    ["sqlite3_data_count", "int", "sqlite3_stmt*"],
    ["sqlite3_db_filename", "string", "sqlite3*", "string"],
    ["sqlite3_db_name", "string", "sqlite3*", "int"],
    ["sqlite3_errmsg", "string", "sqlite3*"],
    ["sqlite3_error_offset", "int", "sqlite3*"],
    ["sqlite3_errstr", "string", "int"],
    //["sqlite3_exec", "int", "sqlite3*", "string", "*", "*", "**"],
    // ^^^ TODO: we need a wrapper to support passing a function pointer or a function
    // for the callback.
    ["sqlite3_expanded_sql", "string", "sqlite3_stmt*"],
    ["sqlite3_extended_errcode", "int", "sqlite3*"],
    ["sqlite3_extended_result_codes", "int", "sqlite3*", "int"],
    ["sqlite3_finalize", "int", "sqlite3_stmt*"],
    ["sqlite3_initialize", undefined],
    ["sqlite3_interrupt", undefined, "sqlite3*"
     /* ^^^ we cannot actually currently support this because JS is
        single-threaded and we don't have a portable way to access a DB
        from 2 SharedWorkers concurrently. */],
    ["sqlite3_libversion", "string"],
    ["sqlite3_libversion_number", "int"],
    ["sqlite3_open", "int", "string", "*"],
    ["sqlite3_open_v2", "int", "string", "*", "int", "string"],
    /* sqlite3_prepare_v2() and sqlite3_prepare_v3() are handled
       separately due to us requiring two different sets of semantics
       for those, depending on how their SQL argument is provided. */
    ["sqlite3_reset", "int", "sqlite3_stmt*"],
    ["sqlite3_result_blob",undefined, "*", "*", "int", "*"],
    ["sqlite3_result_double",undefined, "*", "f64"],
    ["sqlite3_result_error",undefined, "*", "string", "int"],
    ["sqlite3_result_error_code", undefined, "*", "int"],
    ["sqlite3_result_error_nomem", undefined, "*"],
    ["sqlite3_result_error_toobig", undefined, "*"],
    ["sqlite3_result_int",undefined, "*", "int"],
    ["sqlite3_result_null",undefined, "*"],
    ["sqlite3_result_text",undefined, "*", "string", "int", "*"],
    ["sqlite3_sourceid", "string"],
    ["sqlite3_sql", "string", "sqlite3_stmt*"],
    ["sqlite3_step", "int", "sqlite3_stmt*"],
    ["sqlite3_strglob", "int", "string","string"],
    ["sqlite3_strlike", "int", "string","string","int"],
    ["sqlite3_total_changes", "int", "sqlite3*"],
    ["sqlite3_value_blob", "*", "*"],
    ["sqlite3_value_bytes","int", "*"],
    ["sqlite3_value_double","f64", "*"],
    ["sqlite3_value_text", "string", "*"],
    ["sqlite3_value_type", "int", "*"],
    ["sqlite3_vfs_find", "*", "string"],
    ["sqlite3_vfs_register", "int", "*", "int"]
  ]/*capi.wasm.bindingSignatures*/;

  if(false && capi.wasm.compileOptionUsed('SQLITE_ENABLE_NORMALIZE')){
    /* ^^^ "the problem" is that this is an option feature and the
       build-time function-export list does not currently take
       optional features into account. */
    capi.wasm.bindingSignatures.push(["sqlite3_normalized_sql", "string", "sqlite3_stmt*"]);
  }
  
  /**
     Functions which require BigInt (int64) support are separated from
     the others because we need to conditionally bind them or apply
     dummy impls, depending on the capabilities of the environment.
  */
  capi.wasm.bindingSignatures.int64 = [
      ["sqlite3_bind_int64","int", ["sqlite3_stmt*", "int", "i64"]],
      ["sqlite3_changes64","i64", ["sqlite3*"]],
      ["sqlite3_column_int64","i64", ["sqlite3_stmt*", "int"]],
      ["sqlite3_total_changes64", "i64", ["sqlite3*"]]
  ];

  /* The remainder of the API will be set up in later steps. */
  return {
    capi,
    postInit: [
      /* some pieces of the API may install functions into this array,
         and each such function will be called, passed (self,sqlite3),
         at the very end of the API load/init process, where self is
         the current global object and sqlite3 is the object returned
         from sqlite3ApiBootstrap(). This array will be removed at the
         end of the API setup process. */],
    /** Config is needed downstream for gluing pieces together. It
        will be removed at the end of the API setup process. */
    config
  };
}/*sqlite3ApiBootstrap()*/;
