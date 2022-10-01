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
    much under development.

  Specific non-goals of this project:

  - As WASM is a web-centric technology and UTF-8 is the King of
    Encodings in that realm, there are no currently plans to support
    the UTF16-related sqlite3 APIs. They would add a complication to
    the bindings for no appreciable benefit. Though web-related
    implementation details take priority, and the JavaScript
    components of the API specifically focus on browser clients, the
    lower-level WASM module "should" work in non-web WASM
    environments.

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
   sqlite3ApiBootstrap() is the only global symbol persistently
   exposed by this API. It is intended to be called one time at the
   end of the API amalgamation process, passed configuration details
   for the current environment, and then optionally be removed from
   the global object using `delete self.sqlite3ApiBootstrap`.

   This function expects a configuration object, intended to abstract
   away details specific to any given WASM environment, primarily so
   that it can be used without any _direct_ dependency on
   Emscripten. (Note the default values for the config object!) The
   config object is only honored the first time this is
   called. Subsequent calls ignore the argument and return the same
   (configured) object which gets initialized by the first call.

   The config object properties include:

   - `exports`[^1]: the "exports" object for the current WASM
     environment. In an Emscripten build, this should be set to
     `Module['asm']`.

   - `memory`[^1]: optional WebAssembly.Memory object, defaulting to
     `exports.memory`. In Emscripten environments this should be set
     to `Module.wasmMemory` if the build uses `-sIMPORT_MEMORY`, or be
     left undefined/falsy to default to `exports.memory` when using
     WASM-exported memory.

   - `bigIntEnabled`: true if BigInt support is enabled. Defaults to
     true if self.BigInt64Array is available, else false. Some APIs
     will throw exceptions if called without BigInt support, as BigInt
     is required for marshalling C-side int64 into and out of JS.

   - `allocExportName`: the name of the function, in `exports`, of the
     `malloc(3)`-compatible routine for the WASM environment. Defaults
     to `"malloc"`.

   - `deallocExportName`: the name of the function, in `exports`, of
     the `free(3)`-compatible routine for the WASM
     environment. Defaults to `"free"`.

   - `wasmfsOpfsDir`[^1]: if the environment supports persistent storage, this
     directory names the "mount point" for that directory. It must be prefixed
     by `/` and may currently contain only a single directory-name part. Using
     the root directory name is not supported by any current persistent backend.


   [^1] = This property may optionally be a function, in which case this
          function re-assigns it to the value returned from that function,
          enabling delayed evaluation.

*/
'use strict';
self.sqlite3ApiBootstrap = function sqlite3ApiBootstrap(
  apiConfig = (self.sqlite3ApiConfig || sqlite3ApiBootstrap.defaultConfig)
){
  if(sqlite3ApiBootstrap.sqlite3){ /* already initalized */
    console.warn("sqlite3ApiBootstrap() called multiple times.",
                 "Config and external initializers are ignored on calls after the first.");
    return sqlite3ApiBootstrap.sqlite3;
  }
  apiConfig = apiConfig || {};
  const config = Object.create(null);
  {
    const configDefaults = {
      exports: undefined,
      memory: undefined,
      bigIntEnabled: (()=>{
        if('undefined'!==typeof Module){
          /* Emscripten module will contain HEAPU64 when built with
             -sWASM_BIGINT=1, else it will not. */
          return !!Module.HEAPU64;
        }
        return !!self.BigInt64Array;
      })(),
      allocExportName: 'malloc',
      deallocExportName: 'free',
      wasmfsOpfsDir: '/opfs'
    };
    Object.keys(configDefaults).forEach(function(k){
      config[k] = Object.getOwnPropertyDescriptor(apiConfig, k)
        ? apiConfig[k] : configDefaults[k];
    });
    // Copy over any properties apiConfig defines but configDefaults does not...
    Object.keys(apiConfig).forEach(function(k){
      if(!Object.getOwnPropertyDescriptor(config, k)){
        config[k] = apiConfig[k];
      }
    });
  }

  [
    // If any of these config options are functions, replace them with
    // the result of calling that function...
    'exports', 'memory', 'wasmfsOpfsDir'
  ].forEach((k)=>{
    if('function' === typeof config[k]){
      config[k] = config[k]();
    }
  });

  /** Throws a new Error, the message of which is the concatenation
      all args with a space between each. */
  const toss = (...args)=>{throw new Error(args.join(' '))};

  if(config.wasmfsOpfsDir && !/^\/[^/]+$/.test(config.wasmfsOpfsDir)){
    toss("config.wasmfsOpfsDir must be falsy or in the form '/dir-name'.");
  }

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

  /** Internal helper to use in operations which need to distinguish
      between SharedArrayBuffer heap memory and non-shared heap. */
  const __SAB = ('undefined'===typeof SharedArrayBuffer)
        ? function(){} : SharedArrayBuffer;
  const typedArrayToString = function(arrayBuffer, begin, end){
    return utf8Decoder.decode(
      (arrayBuffer.buffer instanceof __SAB)
        ? arrayBuffer.slice(begin, end)
        : arrayBuffer.subarray(begin, end)
    );
  };

  /**
     If v is-a Array, its join('') result is returned.  If
     isSQLableTypedArray(v) then typedArrayToString(v) is
     returned. Else v is returned as-is.
  */
  const arrayToString = function(v){
    if(isSQLableTypedArray(v)) return typedArrayToString(v);
    else if(Array.isArray(v)) return v.join('');
    return v;
  };

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
       When using sqlite3_open_v2() it is important to keep the following
       in mind:

       https://www.sqlite.org/c3ref/open.html

       - The flags for use with its 3rd argument are installed in this
       object using their C-side names, e.g. SQLITE_OPEN_CREATE.

       - If the combination of flags passed to it are invalid,
       behavior is undefined. Thus is is never okay to call this
       with fewer than 3 arguments, as JS will default the
       missing arguments to `undefined`, which will result in a
       flag value of 0. Most of the available SQLITE_OPEN_xxx
       flags are meaningless in the WASM build, e.g. the mutext-
       and cache-related flags, but they are retained in this
       API for consistency's sake.

       - The final argument to this function specifies the VFS to use,
       which is largely (but not entirely!) meaningless in the WASM
       environment. It may be null, undefined, or 0 to denote the
       default.
    */
    sqlite3_open_v2: function(filename,dbPtrPtr,flags,vfsStr){}/*installed later*/,
    /**
       The sqlite3_prepare_v3() binding handles two different uses
       with differing JS/WASM semantics:

       1) sqlite3_prepare_v3(pDb, sqlString, -1, prepFlags, ppStmt , null)

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

       The `pzTail` argument is ignored in this case because its
       result is meaningless when a string-type value is passed
       through: the string goes through another level of internal
       conversion for WASM's sake and the result pointer would refer
       to that transient conversion's memory, not the passed-in
       string.

       If the sql argument is not a string, it must be a _pointer_ to
       a NUL-terminated string which was allocated in the WASM memory
       (e.g. using capi.wasm.alloc() or equivalent). In that case,
       the final argument may be 0/null/undefined or must be a pointer
       to which the "tail" of the compiled SQL is written, as
       documented for the C-side sqlite3_prepare_v3(). In case (2),
       the underlying C function is called with the equivalent of:

       (pDb, sqlAsPointer, sqlByteLen, prepFlags, ppStmt, pzTail)

       It returns its result and compiled statement as documented in
       the C API. Fetching the output pointers (5th and 6th
       parameters) requires using `capi.wasm.getMemValue()` (or
       equivalent) and the `pzTail` will point to an address relative to
       the `sqlAsPointer` value.

       If passed an invalid 2nd argument type, this function will
       return SQLITE_MISUSE and sqlite3_errmsg() will contain a string
       describing the problem.

       Side-note: if given an empty string, or one which contains only
       comments or an empty SQL expression, 0 is returned but the result
       output pointer will be NULL.
    */
    sqlite3_prepare_v3: (dbPtr, sql, sqlByteLen, prepFlags,
                         stmtPtrPtr, strPtrPtr)=>{}/*installed later*/,

    /**
       Equivalent to calling sqlite3_prapare_v3() with 0 as its 4th argument.
    */
    sqlite3_prepare_v2: (dbPtr, sql, sqlByteLen,
                         stmtPtrPtr,strPtrPtr)=>{}/*installed later*/,

    /**
       This binding enables the callback argument to be a JavaScript.

       If the callback is a function, then for the duration of the
       sqlite3_exec() call, it installs a WASM-bound function which
       acts as a proxy for the given callback. That proxy will
       also perform a conversion of the callback's arguments from
       `(char**)` to JS arrays of strings. However, for API
       consistency's sake it will still honor the C-level
       callback parameter order and will call it like:

       `callback(pVoid, colCount, listOfValues, listOfColNames)`

       If the callback is not a JS function then this binding performs
       no translation of the callback, but the sql argument is still
       converted to a WASM string for the call using the
       "flexible-string" argument converter.
    */
    sqlite3_exec: (pDb, sql, callback, pVoid, pErrMsg)=>{}/*installed later*/,
    /**
       Various internal-use utilities are added here as needed. They
       are bound to an object only so that we have access to them in
       the differently-scoped steps of the API bootstrapping
       process. At the end of the API setup process, this object gets
       removed.
    */
    util:{
      affirmBindableTypedArray, arrayToString, isBindableTypedArray,
      isInt32, isSQLableTypedArray, isTypedArray, 
      typedArrayToString,
      isMainWindow: ()=>{
        return self.window===self && self.document;
      }
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
      dealloc: undefined/*installed later*/

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
  capi.wasm.allocFromTypedArray = function(srcTypedArray){
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

  /**
     Signatures for the WASM-exported C-side functions. Each entry
     is an array with 2+ elements:

     [ "c-side name",
       "result type" (capi.wasm.xWrap() syntax),
       [arg types in xWrap() syntax]
       // ^^^ this needn't strictly be an array: it can be subsequent
       // elements instead: [x,y,z] is equivalent to x,y,z
     ]

     Note that support for the API-specific data types in the
     result/argument type strings gets plugged in at a later phase in
     the API initialization process.
  */
  capi.wasm.bindingSignatures = [
    // Please keep these sorted by function name!
    ["sqlite3_bind_blob","int", "sqlite3_stmt*", "int", "*", "int", "*"
     /* We should arguably write a custom wrapper which knows how
        to handle Blob, TypedArrays, and JS strings. */
    ],
    ["sqlite3_bind_double","int", "sqlite3_stmt*", "int", "f64"],
    ["sqlite3_bind_int","int", "sqlite3_stmt*", "int", "int"],
    ["sqlite3_bind_null",undefined, "sqlite3_stmt*", "int"],
    ["sqlite3_bind_parameter_count", "int", "sqlite3_stmt*"],
    ["sqlite3_bind_parameter_index","int", "sqlite3_stmt*", "string"],
    ["sqlite3_bind_text","int", "sqlite3_stmt*", "int", "string", "int", "int"
     /* We should arguably create a hand-written binding
        which does more flexible text conversion, along the lines of
        sqlite3_prepare_v3(). The slightly problematic part is the
        final argument (text destructor). */
    ],
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
    ["sqlite3_db_handle", "sqlite3*", "sqlite3_stmt*"],
    ["sqlite3_db_name", "string", "sqlite3*", "int"],
    ["sqlite3_deserialize", "int", "sqlite3*", "string", "*", "i64", "i64", "int"]
    /* Careful! Short version: de/serialize() are problematic because they
       might use a different allocator that the user for managing the
       deserialized block. de/serialize() are ONLY safe to use with
       sqlite3_malloc(), sqlite3_free(), and its 64-bit variants. */,
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
    ["sqlite3_initialize", undefined],
    ["sqlite3_interrupt", undefined, "sqlite3*"
     /* ^^^ we cannot actually currently support this because JS is
        single-threaded and we don't have a portable way to access a DB
        from 2 SharedWorkers concurrently. */],
    ["sqlite3_libversion", "string"],
    ["sqlite3_libversion_number", "int"],
    ["sqlite3_malloc", "*","int"],
    ["sqlite3_open", "int", "string", "*"],
    ["sqlite3_open_v2", "int", "string", "*", "int", "string"],
    /* sqlite3_prepare_v2() and sqlite3_prepare_v3() are handled
       separately due to us requiring two different sets of semantics
       for those, depending on how their SQL argument is provided. */
    ["sqlite3_realloc", "*","*","int"],
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
    ["sqlite3_serialize","*", "sqlite3*", "string", "*", "int"],
    ["sqlite3_shutdown", undefined],
    ["sqlite3_sourceid", "string"],
    ["sqlite3_sql", "string", "sqlite3_stmt*"],
    ["sqlite3_step", "int", "sqlite3_stmt*"],
    ["sqlite3_strglob", "int", "string","string"],
    ["sqlite3_strlike", "int", "string","string","int"],
    ["sqlite3_total_changes", "int", "sqlite3*"],
    ["sqlite3_uri_boolean", "int", "string", "string", "int"],
    ["sqlite3_uri_key", "string", "string", "int"],
    ["sqlite3_uri_parameter", "string", "string", "string"],
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
    ["sqlite3_malloc64", "*","i64"],
    ["sqlite3_msize", "i64", "*"],
    ["sqlite3_realloc64", "*","*", "i64"],
    ["sqlite3_total_changes64", "i64", ["sqlite3*"]],
    ["sqlite3_uri_int64", "i64", ["string", "string", "i64"]]
  ];

  /**
     Functions which are intended solely for API-internal use by the
     WASM components, not client code. These get installed into
     capi.wasm.
  */
  capi.wasm.bindingSignatures.wasm = [
    ["sqlite3_wasm_vfs_unlink", "int", "string"]
  ];


  /**
     sqlite3.capi.wasm.pstack (pseudo-stack) holds a special-case
     stack-style allocator intended only for use with _small_ data of
     not more than (in total) a few kb in size, managed as if it were
     stack-based.

     It has only a single intended usage:

     ```
     const stackPos = pstack.pointer;
     try{
       const ptr = pstack.alloc(8);
       // ==> pstack.pointer === ptr
       const otherPtr = pstack.alloc(8);
       // ==> pstack.pointer === otherPtr
       ...
     }finally{
       pstack.restore(stackPos);
       // ==> pstack.pointer === stackPos
     }
     ```

     This allocator is much faster than a general-purpose one but is
     limited to usage patterns like the one shown above.

     It operates from a static range of memory which lives outside of
     space managed by Emscripten's stack-management, so does not
     collide with Emscripten-provided stack allocation APIs. The
     memory lives in the WASM heap and can be used with routines such
     as wasm.setMemValue() and any wasm.heap8u().slice().
  */
  capi.wasm.pstack = Object.assign(Object.create(null),{
    /**
       Sets the current ppstack position to the given pointer.
       Results are undefined if the passed-in value did not come from
       this.pointer.
    */
    restore: capi.wasm.exports.sqlite3_wasm_pstack_restore,
    /**
       Attempts to allocate the given number of bytes from the
       pstack. On success, it zeroes out a block of memory of the
       given size, adjusts the pstack pointer, and returns a pointer
       to the memory. On error, returns 0. The memory must eventually
       be released using restore().

       This method always adjusts the given value to be a multiple
       of 8 in order to keep alignment guarantees.
    */
    alloc: capi.wasm.exports.sqlite3_wasm_pstack_alloc
  });
  /**
     sqlite3.capi.wasm.pstack.pointer resolves to the current pstack
     position pointer. This value is intended _only_ to be passed to restore().
  */
  Object.defineProperty(capi.wasm.pstack, 'pointer', {
    configurable: false, iterable: true, writeable: false,
    get: capi.wasm.exports.sqlite3_wasm_pstack_ptr
    //Whether or not a setter as an alternative to restore() is
    //clearer or would just lead to confusion is unclear.
    //set: capi.wasm.exports.sqlite3_wasm_pstack_restore
  });
  /**
     sqlite3.capi.wasm.pstack.remaining resolves to the amount of
     space remaining in the pstack.
  */
  Object.defineProperty(capi.wasm.pstack, 'remaining', {
    configurable: false, iterable: true, writeable: false,
    get: capi.wasm.exports.sqlite3_wasm_pstack_remaining
  });


  /** State for sqlite3_wasmfs_opfs_dir(). */
  let __persistentDir = undefined;
  /**
     If the wasm environment has a WASMFS/OPFS-backed persistent
     storage directory, its path is returned by this function. If it
     does not then it returns "" (noting that "" is a falsy value).

     The first time this is called, this function inspects the current
     environment to determine whether persistence support is available
     and, if it is, enables it (if needed).

     This function currently only recognizes the WASMFS/OPFS storage
     combination and its path refers to storage rooted in the
     Emscripten-managed virtual filesystem.
  */
  capi.sqlite3_wasmfs_opfs_dir = function(){
    if(undefined !== __persistentDir) return __persistentDir;
    // If we have no OPFS, there is no persistent dir
    const pdir = config.wasmfsOpfsDir;
    if(!pdir
       || !self.FileSystemHandle
       || !self.FileSystemDirectoryHandle
       || !self.FileSystemFileHandle){
      return __persistentDir = "";
    }
    try{
      if(pdir && 0===capi.wasm.xCallWrapped(
        'sqlite3_wasm_init_wasmfs', 'i32', ['string'], pdir
      )){
        return __persistentDir = pdir;
      }else{
        return __persistentDir = "";
      }
    }catch(e){
      // sqlite3_wasm_init_wasmfs() is not available
      return __persistentDir = "";
    }
  };

  /**
     Experimental and subject to change or removal.

     Returns true if sqlite3.capi.sqlite3_wasmfs_opfs_dir() is a
     non-empty string and the given name starts with (that string +
     '/'), else returns false.

     Potential (but arguable) TODO: return true if the name is one of
     (":localStorage:", "local", ":sessionStorage:", "session") and
     kvvfs is available.
  */
  capi.sqlite3_web_filename_is_persistent = function(name){
    const p = capi.sqlite3_wasmfs_opfs_dir();
    return (p && name) ? name.startsWith(p+'/') : false;
  };

  // This bit is highly arguable and is incompatible with the fiddle shell.
  if(false && 0===capi.wasm.exports.sqlite3_vfs_find(0)){
    /* Assume that sqlite3_initialize() has not yet been called.
       This will be the case in an SQLITE_OS_KV build. */
    capi.wasm.exports.sqlite3_initialize();
  }

  /**
     Given an `sqlite3*`, an sqlite3_vfs name, and an optional db
     name, returns a truthy value (see below) if that db handle uses
     that VFS, else returns false. If pDb is falsy then the 3rd
     argument is ignored and this function returns a truthy value if
     the default VFS name matches that of the 2nd argument. Results
     are undefined if pDb is truthy but refers to an invalid
     pointer. The 3rd argument specifies the database name of the
     given database connection to check, defaulting to the main db.

     The 2nd and 3rd arguments may either be a JS string or a C-string
     allocated from the wasm environment.

     The truthy value it returns is a pointer to the `sqlite3_vfs`
     object.

     To permit safe use of this function from APIs which may be called
     via the C stack (like SQL UDFs), this function does not throw: if
     bad arguments cause a conversion error when passing into
     wasm-space, false is returned.
  */
  capi.sqlite3_web_db_uses_vfs = function(pDb,vfsName,dbName="main"){
    try{
      const pK = capi.sqlite3_vfs_find(vfsName);
      if(!pK) return false;
      else if(!pDb){
        return capi.sqlite3_vfs_find(0)===pK ? pK : false;
      }
      const ppVfs = capi.wasm.allocPtr();
      try{
        return (
          (0===capi.sqlite3_file_control(
            pDb, dbName, capi.SQLITE_FCNTL_VFS_POINTER, ppVfs
          )) && (capi.wasm.getPtrValue(ppVfs) === pK)
        ) ? pK : false;
      }finally{
        capi.wasm.dealloc(ppVfs);
      }
    }catch(e){
      /* Ignore - probably bad args to a wasm-bound function. */
      return false;
    }
  };

  /**
     Returns an array of the names of all currently-registered sqlite3
     VFSes.
  */
  capi.sqlite3_web_vfs_list = function(){
    const rc = [];
    let pVfs = capi.sqlite3_vfs_find(0);
    while(pVfs){
      const oVfs = new capi.sqlite3_vfs(pVfs);
      rc.push(capi.wasm.cstringToJs(oVfs.$zName));
      pVfs = oVfs.$pNext;
      oVfs.dispose();
    }
    return rc;
  };

  /**
     Serializes the given `sqlite3*` pointer to a Uint8Array, as per
     sqlite3_serialize(). On success it returns a Uint8Array. On
     error it throws with a description of the problem.
  */
  capi.sqlite3_web_db_export = function(pDb){
    if(!pDb) toss('Invalid sqlite3* argument.');
    const wasm = capi.wasm;
    if(!wasm.bigIntEnabled) toss('BigInt64 support is not enabled.');
    const stack = wasm.pstack.pointer();
    let pOut;
    try{
      const pSize = wasm.pstack.alloc(8/*i64*/ + wasm.ptrSizeof);
      const ppOut = pSize + 8;
      /**
         Maintenance reminder, since this cost a full hour of grief
         and confusion: if the order of pSize/ppOut are reversed in
         that memory block, fetching the value of pSize after the
         export reads a garbage size because it's not on an 8-byte
         memory boundary!
      */
      let rc = wasm.exports.sqlite3_wasm_db_serialize(
        pDb, ppOut, pSize, 0
      );
      if(rc){
        toss("Database serialization failed with code",
             sqlite3.capi.sqlite3_web_rc_str(rc));
      }
      pOut = wasm.getPtrValue(ppOut);
      const nOut = wasm.getMemValue(pSize, 'i64');
      rc = nOut
        ? wasm.heap8u().slice(pOut, pOut + Number(nOut))
        : new Uint8Array();
      return rc;
    }catch(e){
      console.error('internal error?',e);
      throw w;
    }finally{
      if(pOut) wasm.exports.sqlite3_free(pOut);
      wasm.pstack.restore(stack);
    }
  };
  
  if( capi.util.isMainWindow() ){
    /* Features specific to the main window thread... */

    /**
       Internal helper for sqlite3_web_kvvfs_clear() and friends.
       Its argument should be one of ('local','session','').
    */
    const __kvvfsInfo = function(which){
      const rc = Object.create(null);
      rc.prefix = 'kvvfs-'+which;
      rc.stores = [];
      if('session'===which || ''===which) rc.stores.push(self.sessionStorage);
      if('local'===which || ''===which) rc.stores.push(self.localStorage);
      return rc;
    };

    /**
       Clears all storage used by the kvvfs DB backend, deleting any
       DB(s) stored there. Its argument must be either 'session',
       'local', or ''. In the first two cases, only sessionStorage
       resp. localStorage is cleared. If it's an empty string (the
       default) then both are cleared. Only storage keys which match
       the pattern used by kvvfs are cleared: any other client-side
       data are retained.

       This function is only available in the main window thread.

       Returns the number of entries cleared.
    */
    capi.sqlite3_web_kvvfs_clear = function(which=''){
      let rc = 0;
      const kvinfo = __kvvfsInfo(which);
      kvinfo.stores.forEach((s)=>{
        const toRm = [] /* keys to remove */;
        let i;
        for( i = 0; i < s.length; ++i ){
          const k = s.key(i);
          if(k.startsWith(kvinfo.prefix)) toRm.push(k);
        }
        toRm.forEach((kk)=>s.removeItem(kk));
        rc += toRm.length;
      });
      return rc;
    };

    /**
       This routine guesses the approximate amount of
       window.localStorage and/or window.sessionStorage in use by the
       kvvfs database backend. Its argument must be one of
       ('session', 'local', ''). In the first two cases, only
       sessionStorage resp. localStorage is counted. If it's an empty
       string (the default) then both are counted. Only storage keys
       which match the pattern used by kvvfs are counted. The returned
       value is the "length" value of every matching key and value,
       noting that JavaScript stores each character in 2 bytes.

       Note that the returned size is not authoritative from the
       perspective of how much data can fit into localStorage and
       sessionStorage, as the precise algorithms for determining
       those limits are unspecified and may include per-entry
       overhead invisible to clients.
    */
    capi.sqlite3_web_kvvfs_size = function(which=''){
      let sz = 0;
      const kvinfo = __kvvfsInfo(which);
      kvinfo.stores.forEach((s)=>{
        let i;
        for(i = 0; i < s.length; ++i){
          const k = s.key(i);
          if(k.startsWith(kvinfo.prefix)){
            sz += k.length;
            sz += s.getItem(k).length;
          }
        }
      });
      return sz * 2 /* because JS uses 2-byte char encoding */;
    };

  }/* main-window-only bits */


  /* The remainder of the API will be set up in later steps. */
  const sqlite3 = {
    WasmAllocError: WasmAllocError,
    capi,
    config,
    /**
       Performs any optional asynchronous library-level initialization
       which might be required. This function returns a Promise which
       resolves to the sqlite3 namespace object. It _ignores any
       errors_ in the asynchronous init process, as such components
       are all optional. If called more than once, the second and
       subsequent calls are no-ops which return a pre-resolved
       Promise.

       Ideally this function is called as part of the Promise chain
       which handles the loading and bootstrapping of the API.  If not
       then it must be called by client-level code, which must not use
       the library until the returned promise resolves.

       Bug: if called while a prior call is still resolving, the 2nd
       call will resolve prematurely, before the 1st call has finished
       resolving. The current build setup precludes that possibility,
       so it's only a hypothetical problem if/when this function
       ever needs to be invoked by clients.

       In Emscripten-based builds, this function is called
       automatically and deleted from this object.
    */
    asyncPostInit: async function(){
      let lip = sqlite3ApiBootstrap.initializersAsync;
      delete sqlite3ApiBootstrap.initializersAsync;
      if(!lip || !lip.length) return Promise.resolve(sqlite3);
      // Is it okay to resolve these in parallel or do we need them
      // to resolve in order? We currently only have 1, so it
      // makes no difference.
      lip = lip.map((f)=>f(sqlite3).catch(()=>{}));
      //let p = lip.shift();
      //while(lip.length) p = p.then(lip.shift());
      //return p.then(()=>sqlite3);
      return Promise.all(lip).then(()=>sqlite3);
    }
  };
  sqlite3ApiBootstrap.initializers.forEach((f)=>f(sqlite3));
  delete sqlite3ApiBootstrap.initializers;
  sqlite3ApiBootstrap.sqlite3 = sqlite3;
  return sqlite3;
}/*sqlite3ApiBootstrap()*/;
/**
  self.sqlite3ApiBootstrap.initializers is an internal detail used by
  the various pieces of the sqlite3 API's amalgamation process. It
  must not be modified by client code except when plugging such code
  into the amalgamation process.

  Each component of the amalgamation is expected to append a function
  to this array. When sqlite3ApiBootstrap() is called for the first
  time, each such function will be called (in their appended order)
  and passed the sqlite3 namespace object, into which they can install
  their features (noting that most will also require that certain
  features alread have been installed).  At the end of that process,
  this array is deleted.

  Note that the order of insertion into this array is significant for
  some pieces. e.g. sqlite3.capi.wasm cannot be fully utilized until
  the whwasmutil.js part is plugged in.
*/
self.sqlite3ApiBootstrap.initializers = [];
/**
  self.sqlite3ApiBootstrap.initializersAsync is an internal detail
  used by the sqlite3 API's amalgamation process. It must not be
  modified by client code except when plugging such code into the
  amalgamation process.

  Counterpart of self.sqlite3ApiBootstrap.initializers, specifically
  for initializers which are asynchronous. All functions in this list
  take the sqlite3 object as their argument and MUST return a
  Promise. Both the resolved value and rejection cases are ignored.

  This list is not processed until the client calls
  sqlite3.asyncPostInit(). This means, for example, that intializers
  added to self.sqlite3ApiBootstrap.initializers may push entries to
   this list.
*/
self.sqlite3ApiBootstrap.initializersAsync = [];
/**
   Client code may assign sqlite3ApiBootstrap.defaultConfig an
   object-type value before calling sqlite3ApiBootstrap() (without
   arguments) in order to tell that call to use this object as its
   default config value. The intention of this is to provide
   downstream clients with a reasonably flexible approach for plugging in
   an environment-suitable configuration without having to define a new
   global-scope symbol.
*/
self.sqlite3ApiBootstrap.defaultConfig = Object.create(null);
/**
   Placeholder: gets installed by the first call to
   self.sqlite3ApiBootstrap(). However, it is recommended that the
   caller of sqlite3ApiBootstrap() capture its return value and delete
   self.sqlite3ApiBootstrap after calling it. It returns the same
   value which will be stored here.
*/
self.sqlite3ApiBootstrap.sqlite3 = undefined;
