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

  Project home page: https://sqlite.org

  Documentation home page: https://sqlite.org/wasm

  Specific goals of this subproject:

  - Except where noted in the non-goals, provide a more-or-less
    feature-complete wrapper to the sqlite3 C API, insofar as WASM
    feature parity with C allows for. In fact, provide at least 4
    APIs...

    1) 1-to-1 bindings as exported from WASM, with no automatic
       type conversions between JS and C.
       
    2) A binding of (1) which provides certain JS/C type conversions
       to greatly simplify its use.

    3) A higher-level API, more akin to sql.js and node.js-style
       implementations. This one speaks directly to the low-level
       API. This API must be used from the same thread as the
       low-level API.

    4) A second higher-level API which speaks to the previous APIs via
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
   This function will throw if any of the required config options are
   missing.

   The config object properties include:

   - `exports`[^1]: the "exports" object for the current WASM
     environment. In an Emscripten-based build, this should be set to
     `Module['asm']`.

   - `memory`[^1]: optional WebAssembly.Memory object, defaulting to
     `exports.memory`. In Emscripten environments this should be set
     to `Module.wasmMemory` if the build uses `-sIMPORT_MEMORY`, or be
     left undefined/falsy to default to `exports.memory` when using
     WASM-exported memory.

   - `bigIntEnabled`: true if BigInt support is enabled. Defaults to
     true if `self.BigInt64Array` is available, else false. Some APIs
     will throw exceptions if called without BigInt support, as BigInt
     is required for marshalling C-side int64 into and out of JS.

   - `allocExportName`: the name of the function, in `exports`, of the
     `malloc(3)`-compatible routine for the WASM environment. Defaults
     to `"malloc"`.

   - `deallocExportName`: the name of the function, in `exports`, of
     the `free(3)`-compatible routine for the WASM
     environment. Defaults to `"free"`.

   - `wasmfsOpfsDir`[^1]: if the environment supports persistent
     storage, this directory names the "mount point" for that
     directory. It must be prefixed by `/` and may contain only a
     single directory-name part. Using the root directory name is not
     supported by any current persistent backend.  This setting is
     only used in WASMFS-enabled builds.


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
  const config = Object.assign(Object.create(null),{
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
  }, apiConfig || {});

  [
    // If any of these config options are functions, replace them with
    // the result of calling that function...
    'exports', 'memory', 'wasmfsOpfsDir'
  ].forEach((k)=>{
    if('function' === typeof config[k]){
      config[k] = config[k]();
    }
  });

  /** 
      The main sqlite3 binding API gets installed into this object,
      mimicking the C API as closely as we can. The numerous members
      names with prefixes 'sqlite3_' and 'SQLITE_' behave, insofar as
      possible, identically to the C-native counterparts, as documented at:

      https://www.sqlite.org/c3ref/intro.html

      A very few exceptions require an additional level of proxy
      function or may otherwise require special attention in the WASM
      environment, and all such cases are documented somewhere below
      in this file or in sqlite3-api-glue.js. capi members which are
      not documented are installed as 1-to-1 proxies for their
      C-side counterparts.
  */
  const capi = Object.create(null);
  /**
     Holds state which are specific to the WASM-related
     infrastructure and glue code. It is not expected that client
     code will normally need these, but they're exposed here in case
     it does. These APIs are _not_ to be considered an
     official/stable part of the sqlite3 WASM API. They may change
     as the developers' experience suggests appropriate changes.

     Note that a number of members of this object are injected
     dynamically after the api object is fully constructed, so
     not all are documented in this file.
  */
  const wasm = Object.create(null);

  /** Internal helper for SQLite3Error ctor. */
  const __rcStr = (rc)=>{
    return (capi.sqlite3_js_rc_str && capi.sqlite3_js_rc_str(rc))
           || ("Unknown result code #"+rc);
  };

  /** Internal helper for SQLite3Error ctor. */
  const __isInt = (n)=>'number'===typeof n && n===(n | 0);

  /**
     An Error subclass specifically for reporting DB-level errors and
     enabling clients to unambiguously identify such exceptions.
     The C-level APIs never throw, but some of the higher-level
     C-style APIs do and the object-oriented APIs use exceptions
     exclusively to report errors.
  */
  class SQLite3Error extends Error {
    /**
       Constructs this object with a message depending on its arguments:

       - If it's passed only a single integer argument, it is assumed
       to be an sqlite3 C API result code. The message becomes the
       result of sqlite3.capi.sqlite3_js_rc_str() or (if that returns
       falsy) a synthesized string which contains that integer.

       - If passed 2 arguments and the 2nd is a object, it bevaves
       like the Error(string,object) constructor except that the first
       argument is subject to the is-integer semantics from the
       previous point.

       - Else all arguments are concatenated with a space between each
       one, using args.join(' '), to create the error message.
    */
    constructor(...args){
      if(1===args.length && __isInt(args[0])){
        super(__rcStr(args[0]));
      }else if(2===args.length && 'object'===typeof args){
        if(__isInt(args[0])) super(__rcStr(args[0]), args[1]);
        else super(...args);
      }else{
        super(args.join(' '));
      }
      this.name = 'SQLite3Error';
    }
  };

  /**
     Functionally equivalent to the SQLite3Error constructor but may
     be used as part of an expression, e.g.:

     ```
     return someFunction(x) || SQLite3Error.toss(...);
     ```
  */
  SQLite3Error.toss = (...args)=>{
    throw new SQLite3Error(...args);
  };
  const toss3 = SQLite3Error.toss;

  if(config.wasmfsOpfsDir && !/^\/[^/]+$/.test(config.wasmfsOpfsDir)){
    toss3("config.wasmfsOpfsDir must be falsy or in the form '/dir-name'.");
  }

  /**
     Returns true if n is a 32-bit (signed) integer, else
     false. This is used for determining when we need to switch to
     double-type DB operations for integer values in order to keep
     more precision.
  */
  const isInt32 = (n)=>{
    return ('bigint'!==typeof n /*TypeError: can't convert BigInt to number*/)
      && !!(n===(n|0) && n<=2147483647 && n>=-2147483648);
  };
  /**
     Returns true if the given BigInt value is small enough to fit
     into an int64 value, else false.
  */
  const bigIntFits64 = function f(b){
    if(!f._max){
      f._max = BigInt("0x7fffffffffffffff");
      f._min = ~f._max;
    }
    return b >= f._min && b <= f._max;
  };

  /**
     Returns true if the given BigInt value is small enough to fit
     into an int32, else false.
  */
  const bigIntFits32 = (b)=>(b >= (-0x7fffffffn - 1n) && b <= 0x7fffffffn);

  /**
     Returns true if the given BigInt value is small enough to fit
     into a double value without loss of precision, else false.
  */
  const bigIntFitsDouble = function f(b){
    if(!f._min){
      f._min = Number.MIN_SAFE_INTEGER;
      f._max = Number.MAX_SAFE_INTEGER;
    }
    return b >= f._min && b <= f._max;
  };

  /** Returns v if v appears to be a TypedArray, else false. */
  const isTypedArray = (v)=>{
    return (v && v.constructor && isInt32(v.constructor.BYTES_PER_ELEMENT)) ? v : false;
  };


  /** Internal helper to use in operations which need to distinguish
      between TypedArrays which are backed by a SharedArrayBuffer
      from those which are not. */
  const __SAB = ('undefined'===typeof SharedArrayBuffer)
        ? function(){} : SharedArrayBuffer;
  /** Returns true if the given TypedArray object is backed by a
      SharedArrayBuffer, else false. */
  const isSharedTypedArray = (aTypedArray)=>(aTypedArray.buffer instanceof __SAB);

  /**
     Returns either aTypedArray.slice(begin,end) (if
     aTypedArray.buffer is a SharedArrayBuffer) or
     aTypedArray.subarray(begin,end) (if it's not).

     This distinction is important for APIs which don't like to
     work on SABs, e.g. TextDecoder, and possibly for our
     own APIs which work on memory ranges which "might" be
     modified by other threads while they're working.
  */
  const typedArrayPart = (aTypedArray, begin, end)=>{
    return isSharedTypedArray(aTypedArray)
      ? aTypedArray.slice(begin, end)
      : aTypedArray.subarray(begin, end);
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
      || toss3("Value is not of a supported TypedArray type.");
  };

  const utf8Decoder = new TextDecoder('utf-8');

  /**
     Uses TextDecoder to decode the given half-open range of the
     given TypedArray to a string. This differs from a simple
     call to TextDecoder in that it accounts for whether the
     first argument is backed by a SharedArrayBuffer or not,
     and can work more efficiently if it's not (TextDecoder
     refuses to act upon an SAB).
  */
  const typedArrayToString = function(typedArray, begin, end){
    return utf8Decoder.decode(typedArrayPart(typedArray, begin,end));
  };

  /**
     If v is-a Array, its join("") result is returned.  If
     isSQLableTypedArray(v) is true then typedArrayToString(v) is
     returned. If it looks like a WASM pointer, wasm.cstringToJs(v) is
     returned. Else v is returned as-is.
  */
  const flexibleString = function(v){
    if(isSQLableTypedArray(v)) return typedArrayToString(v);
    else if(Array.isArray(v)) return v.join("");
    else if(wasm.isPtr(v)) v = wasm.cstringToJs(v);
    return v;
  };

  /**
     An Error subclass specifically for reporting Wasm-level malloc()
     failure and enabling clients to unambiguously identify such
     exceptions.
  */
  class WasmAllocError extends Error {
    /**
       If called with 2 arguments and the 2nd one is an object, it
       behaves like the Error constructor, else it concatenates all
       arguments together with a single space between each to
       construct an error message string. As a special case, if
       called with no arguments then it uses a default error
       message.
    */
    constructor(...args){
      if(2===args.length && 'object'===typeof args){
        super(...args);
      }else if(args.length){
        super(args.join(' '));
      }else{
        super("Allocation failed.");
      }
      this.name = 'WasmAllocError';
    }
  };
  /**
     Functionally equivalent to the WasmAllocError constructor but may
     be used as part of an expression, e.g.:

     ```
     return someAllocatingFunction(x) || WasmAllocError.toss(...);
     ```
  */
  WasmAllocError.toss = (...args)=>{
    throw new WasmAllocError(...args);
  };

  Object.assign(capi, {
    /**
       sqlite3_create_function_v2() differs from its native
       counterpart only in the following ways:

       1) The fourth argument (`eTextRep`) argument must not specify
       any encoding other than sqlite3.SQLITE_UTF8. The JS API does not
       currently support any other encoding and likely never
       will. This function does not replace that argument on its own
       because it may contain other flags.

       2) Any of the four final arguments may be either WASM pointers
       (assumed to be function pointers) or JS Functions. In the
       latter case, each gets bound to WASM using
       sqlite3.capi.wasm.installFunction() and that wrapper is passed
       on to the native implementation.

       The semantics of JS functions are:

       xFunc: is passed `(pCtx, ...values)`. Its return value becomes
       the new SQL function's result.

       xStep: is passed `(pCtx, ...values)`. Its return value is
       ignored.

       xFinal: is passed `(pCtx)`. Its return value becomes the new
       aggregate SQL function's result.

       xDestroy: is passed `(void*)`. Its return value is ignored. The
       pointer passed to it is the one from the 5th argument to
       sqlite3_create_function_v2().

       Note that:

       - `pCtx` in the above descriptions is a `sqlite3_context*`. At
         least 99 times out of a hundred, that initial argument will
         be irrelevant for JS UDF bindings, but it needs to be there
         so that the cases where it _is_ relevant, in particular with
         window and aggregate functions, have full access to the
         lower-level sqlite3 APIs.

       - When wrapping JS functions, the remaining arguments are passd
         to them as positional arguments, not as an array of
         arguments, because that allows callback definitions to be
         more JS-idiomatic than C-like. For example `(pCtx,a,b)=>a+b`
         is more intuitive and legible than
         `(pCtx,args)=>args[0]+args[1]`. For cases where an array of
         arguments would be more convenient, the callbacks simply need
         to be declared like `(pCtx,...args)=>{...}`, in which case
         `args` will be an array.

       - If a JS wrapper throws, it gets translated to
         sqlite3_result_error() or sqlite3_result_error_nomem(),
         depending on whether the exception is an
         sqlite3.WasmAllocError object or not.

       - When passing on WASM function pointers, arguments are _not_
         converted or reformulated. They are passed on as-is in raw
         pointer form using their native C signatures. Only JS
         functions passed in to this routine, and thus wrapped by this
         routine, get automatic conversions of arguments and result
         values. The routines which perform those conversions are
         exposed for client-side use as
         sqlite3_create_function_v2.convertUdfArgs() and
         sqlite3_create_function_v2.setUdfResult(). sqlite3_create_function()
         and sqlite3_create_window_function() have those same methods.

       For xFunc(), xStep(), and xFinal():

       - When called from SQL, arguments to the UDF, and its result,
         will be converted between JS and SQL with as much fidelity as
         is feasible, triggering an exception if a type conversion
         cannot be determined. Some freedom is afforded to numeric
         conversions due to friction between the JS and C worlds:
         integers which are larger than 32 bits may be treated as
         doubles or BigInts.

       If any JS-side bound functions throw, those exceptions are
       intercepted and converted to database-side errors with the
       exception of xDestroy(): any exception from it is ignored,
       possibly generating a console.error() message.  Destructors
       must not throw.

       Once installed, there is currently no way to uninstall the
       automatically-converted WASM-bound JS functions from WASM. They
       can be uninstalled from the database as documented in the C
       API, but this wrapper currently has no infrastructure in place
       to also free the WASM-bound JS wrappers, effectively resulting
       in a memory leak if the client uninstalls the UDF. Improving that
       is a potential TODO, but removing client-installed UDFs is rare
       in practice. If this factor is relevant for a given client,
       they can create WASM-bound JS functions themselves, hold on to their
       pointers, and pass the pointers in to here. Later on, they can
       free those pointers (using `wasm.uninstallFunction()` or
       equivalent).

       C reference: https://www.sqlite.org/c3ref/create_function.html

       Maintenance reminder: the ability to add new
       WASM-accessible functions to the runtime requires that the
       WASM build is compiled with emcc's `-sALLOW_TABLE_GROWTH`
       flag.
    */
    sqlite3_create_function_v2: function(
      pDb, funcName, nArg, eTextRep, pApp,
      xFunc, xStep, xFinal, xDestroy
    ){/*installed later*/},
    /**
       Equivalent to passing the same arguments to
       sqlite3_create_function_v2(), with 0 as the final argument.
    */
    sqlite3_create_function:function(
      pDb, funcName, nArg, eTextRep, pApp,
      xFunc, xStep, xFinal
    ){/*installed later*/},
    /**
       The sqlite3_create_window_function() JS wrapper differs from
       its native implementation in the exact same way that
       sqlite3_create_function_v2() does. The additional function,
       xInverse(), is treated identically to xStep() by the wrapping
       layer.
    */
    sqlite3_create_window_function: function(
      pDb, funcName, nArg, eTextRep, pApp,
      xStep, xFinal, xValue, xInverse, xDestroy
    ){/*installed later*/},
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
       acts as a proxy for the given callback. That proxy will also
       perform a conversion of the callback's arguments from
       `(char**)` to JS arrays of strings. However, for API
       consistency's sake it will still honor the C-level callback
       parameter order and will call it like:

       `callback(pVoid, colCount, listOfValues, listOfColNames)`

       If the callback is not a JS function then this binding performs
       no translation of the callback, but the sql argument is still
       converted to a WASM string for the call using the
       "flexible-string" argument converter.
    */
    sqlite3_exec: (pDb, sql, callback, pVoid, pErrMsg)=>{}/*installed later*/,

    /**
       If passed a single argument which appears to be a byte-oriented
       TypedArray (Int8Array or Uint8Array), this function treats that
       TypedArray as an output target, fetches `theArray.byteLength`
       bytes of randomness, and populates the whole array with it. As
       a special case, if the array's length is 0, this function
       behaves as if it were passed (0,0). When called this way, it
       returns its argument, else it returns the `undefined` value.

       If called with any other arguments, they are passed on as-is
       to the C API. Results are undefined if passed any incompatible
       values.
     */
    sqlite3_randomness: (n, outPtr)=>{/*installed later*/},
  }/*capi*/);

  /**
     Various internal-use utilities are added here as needed. They
     are bound to an object only so that we have access to them in
     the differently-scoped steps of the API bootstrapping
     process. At the end of the API setup process, this object gets
     removed. These are NOT part of the public API.
  */
  const util = {
    affirmBindableTypedArray, flexibleString,
    bigIntFits32, bigIntFits64, bigIntFitsDouble,
    isBindableTypedArray,
    isInt32, isSQLableTypedArray, isTypedArray, 
    typedArrayToString,
    isUIThread: ()=>'undefined'===typeof WorkerGlobalScope,
    isSharedTypedArray,
    typedArrayPart
  };
    
  Object.assign(wasm, {
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
      || toss3("Missing API config.exports (WASM module exports)."),

    /**
       When Emscripten compiles with `-sIMPORT_MEMORY`, it
       initalizes the heap and imports it into wasm, as opposed to
       the other way around. In this case, the memory is not
       available via this.exports.memory.
    */
    memory: config.memory || config.exports['memory']
      || toss3("API config object requires a WebAssembly.Memory object",
              "in either config.exports.memory (exported)",
              "or config.memory (imported)."),

    /**
       The API's one single point of access to the WASM-side memory
       allocator. Works like malloc(3) (and is likely bound to
       malloc()) but throws an WasmAllocError if allocation fails. It is
       important that any code which might pass through the sqlite3 C
       API NOT throw and must instead return SQLITE_NOMEM (or
       equivalent, depending on the context).

       Very few cases in the sqlite3 JS APIs can result in
       client-defined functions propagating exceptions via the C-style
       API. Most notably, this applies to WASM-bound JS functions
       which are created directly by clients and passed on _as WASM
       function pointers_ to functions such as
       sqlite3_create_function_v2(). Such bindings created
       transparently by this API will automatically use wrappers which
       catch exceptions and convert them to appropriate error codes.

       For cases where non-throwing allocation is required, use
       sqlite3.wasm.alloc.impl(), which is direct binding of the
       underlying C-level allocator.

       Design note: this function is not named "malloc" primarily
       because Emscripten uses that name and we wanted to avoid any
       confusion early on in this code's development, when it still
       had close ties to Emscripten's glue code.
    */
    alloc: undefined/*installed later*/,

    /**
       The API's one single point of access to the WASM-side memory
       deallocator. Works like free(3) (and is likely bound to
       free()).

       Design note: this function is not named "free" for the same
       reason that this.alloc() is not called this.malloc().
    */
    dealloc: undefined/*installed later*/

    /* Many more wasm-related APIs get installed later on. */
  }/*wasm*/);

  /**
     wasm.alloc()'s srcTypedArray.byteLength bytes,
     populates them with the values from the source
     TypedArray, and returns the pointer to that memory. The
     returned pointer must eventually be passed to
     wasm.dealloc() to clean it up.

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
  wasm.allocFromTypedArray = function(srcTypedArray){
    affirmBindableTypedArray(srcTypedArray);
    const pRet = wasm.alloc(srcTypedArray.byteLength || 1);
    wasm.heapForSize(srcTypedArray.constructor).set(
      srcTypedArray.byteLength ? srcTypedArray : [0], pRet
    );
    return pRet;
  };

  const keyAlloc = config.allocExportName || 'malloc',
        keyDealloc =  config.deallocExportName || 'free';
  for(const key of [keyAlloc, keyDealloc]){
    const f = wasm.exports[key];
    if(!(f instanceof Function)) toss3("Missing required exports[",key,"] function.");
  }

  wasm.alloc = function f(n){
    const m = f.impl(n);
    if(!m) throw new WasmAllocError("Failed to allocate",n," bytes.");
    return m;
  };
  wasm.alloc.impl = wasm.exports[keyAlloc];
  wasm.dealloc = wasm.exports[keyDealloc];

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
  wasm.compileOptionUsed = function f(optName){
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
    ["sqlite3_bind_text","int", "sqlite3_stmt*", "int", "string", "int", "int"
     /* We should arguably create a hand-written binding of
        bind_text() which does more flexible text conversion, along
        the lines of sqlite3_prepare_v3(). The slightly problematic
        part is the final argument (text destructor). */
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
    /* sqlite3_create_function(), sqlite3_create_function_v2(), and
       sqlite3_create_window_function() use hand-written bindings to
       simplify handling of their function-type arguments. */
    ["sqlite3_data_count", "int", "sqlite3_stmt*"],
    ["sqlite3_db_filename", "string", "sqlite3*", "string"],
    ["sqlite3_db_handle", "sqlite3*", "sqlite3_stmt*"],
    ["sqlite3_db_name", "string", "sqlite3*", "int"],
    ["sqlite3_deserialize", "int", "sqlite3*", "string", "*", "i64", "i64", "int"]
    /* Careful! Short version: de/serialize() are problematic because they
       might use a different allocator than the user for managing the
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
    /*["sqlite3_interrupt", undefined, "sqlite3*"
       ^^^ we cannot actually currently support this because JS is
        single-threaded and we don't have a portable way to access a DB
        from 2 SharedWorkers concurrently. ],*/
    ["sqlite3_libversion", "string"],
    ["sqlite3_libversion_number", "int"],
    ["sqlite3_malloc", "*","int"],
    ["sqlite3_open", "int", "string", "*"],
    ["sqlite3_open_v2", "int", "string", "*", "int", "string"],
    /* sqlite3_prepare_v2() and sqlite3_prepare_v3() are handled
       separately due to us requiring two different sets of semantics
       for those, depending on how their SQL argument is provided. */
    /* sqlite3_randomness() uses a hand-written wrapper to extend
       the range of supported argument types. */
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
    ["sqlite3_trace_v2", "int", "sqlite3*", "int", "*", "*"],
    ["sqlite3_total_changes", "int", "sqlite3*"],
    ["sqlite3_uri_boolean", "int", "string", "string", "int"],
    ["sqlite3_uri_key", "string", "string", "int"],
    ["sqlite3_uri_parameter", "string", "string", "string"],
    ["sqlite3_user_data","void*", "sqlite3_context*"],
    ["sqlite3_value_blob", "*", "sqlite3_value*"],
    ["sqlite3_value_bytes","int", "sqlite3_value*"],
    ["sqlite3_value_double","f64", "sqlite3_value*"],
    ["sqlite3_value_int","int", "sqlite3_value*"],
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
  */
  wasm.bindingSignatures.int64 = [
    ["sqlite3_bind_int64","int", ["sqlite3_stmt*", "int", "i64"]],
    ["sqlite3_changes64","i64", ["sqlite3*"]],
    ["sqlite3_column_int64","i64", ["sqlite3_stmt*", "int"]],
    ["sqlite3_malloc64", "*","i64"],
    ["sqlite3_msize", "i64", "*"],
    ["sqlite3_realloc64", "*","*", "i64"],
    ["sqlite3_result_int64",undefined, "*", "i64"],
    ["sqlite3_total_changes64", "i64", ["sqlite3*"]],
    ["sqlite3_uri_int64", "i64", ["string", "string", "i64"]],
    ["sqlite3_value_int64","i64", "sqlite3_value*"],
  ];

  /**
     Functions which are intended solely for API-internal use by the
     WASM components, not client code. These get installed into
     sqlite3.wasm.
  */
  wasm.bindingSignatures.wasm = [
    ["sqlite3_wasm_db_reset", "int", "sqlite3*"],
    ["sqlite3_wasm_db_vfs", "sqlite3_vfs*", "sqlite3*","string"],
    ["sqlite3_wasm_vfs_create_file", "int",
     "sqlite3_vfs*","string","*", "int"],
    ["sqlite3_wasm_vfs_unlink", "int", "sqlite3_vfs*","string"]
  ];


  /**
     sqlite3.wasm.pstack (pseudo-stack) holds a special-case
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
  wasm.pstack = Object.assign(Object.create(null),{
    /**
       Sets the current pstack position to the given pointer. Results
       are undefined if the passed-in value did not come from
       this.pointer.
    */
    restore: wasm.exports.sqlite3_wasm_pstack_restore,
    /**
       Attempts to allocate the given number of bytes from the
       pstack. On success, it zeroes out a block of memory of the
       given size, adjusts the pstack pointer, and returns a pointer
       to the memory. On error, returns throws a WasmAllocError. The
       memory must eventually be released using restore().

       This method always adjusts the given value to be a multiple
       of 8 bytes because failing to do so can lead to incorrect
       results when reading and writing 64-bit values from/to the WASM
       heap. Similarly, the returned address is always 8-byte aligned.
    */
    alloc: (n)=>{
      return wasm.exports.sqlite3_wasm_pstack_alloc(n)
        || WasmAllocError.toss("Could not allocate",n,
                               "bytes from the pstack.");
    },
    /**
       alloc()'s n chunks, each sz bytes, as a single memory block and
       returns the addresses as an array of n element, each holding
       the address of one chunk.

       Throws a WasmAllocError if allocation fails.

       Example:

       ```
       const [p1, p2, p3] = wasm.pstack.allocChunks(3,4);
       ```
    */
    allocChunks: (n,sz)=>{
      const mem = wasm.pstack.alloc(n * sz);
      const rc = [];
      let i = 0, offset = 0;
      for(; i < n; offset = (sz * ++i)){
        rc.push(mem + offset);
      }
      return rc;
    },
    /**
       A convenience wrapper for allocChunks() which sizes each chunk
       as either 8 bytes (safePtrSize is truthy) or wasm.ptrSizeof (if
       safePtrSize is falsy).

       How it returns its result differs depending on its first
       argument: if it's 1, it returns a single pointer value. If it's
       more than 1, it returns the same as allocChunks().

       When a returned pointers will refer to a 64-bit value, e.g. a
       double or int64, and that value must be written or fetched,
       e.g. using wasm.setMemValue() or wasm.getMemValue(), it is
       important that the pointer in question be aligned to an 8-byte
       boundary or else it will not be fetched or written properly and
       will corrupt or read neighboring memory.

       However, when all pointers involved point to "small" data, it
       is safe to pass a falsy value to save a tiny bit of memory.
    */
    allocPtr: (n=1,safePtrSize=true)=>{
      return 1===n
        ? wasm.pstack.alloc(safePtrSize ? 8 : wasm.ptrSizeof)
        : wasm.pstack.allocChunks(n, safePtrSize ? 8 : wasm.ptrSizeof);
    }
  })/*wasm.pstack*/;
  Object.defineProperties(wasm.pstack, {
    /**
       sqlite3.wasm.pstack.pointer resolves to the current pstack
       position pointer. This value is intended _only_ to be saved
       for passing to restore(). Writing to this memory, without
       first reserving it via wasm.pstack.alloc() and friends, leads
       to undefined results.
    */
    pointer: {
      configurable: false, iterable: true, writeable: false,
      get: wasm.exports.sqlite3_wasm_pstack_ptr
      //Whether or not a setter as an alternative to restore() is
      //clearer or would just lead to confusion is unclear.
      //set: wasm.exports.sqlite3_wasm_pstack_restore
    },
    /**
       sqlite3.wasm.pstack.quota to the total number of bytes
       available in the pstack, including any space which is currently
       allocated. This value is a compile-time constant.
    */
    quota: {
      configurable: false, iterable: true, writeable: false,
      get: wasm.exports.sqlite3_wasm_pstack_quota
    },
    /**
       sqlite3.wasm.pstack.remaining resolves to the amount of space
       remaining in the pstack.
    */
    remaining: {
      configurable: false, iterable: true, writeable: false,
      get: wasm.exports.sqlite3_wasm_pstack_remaining
    }
  })/*wasm.pstack properties*/;

  capi.sqlite3_randomness = (...args)=>{
    if(1===args.length && util.isTypedArray(args[0])
      && 1===args[0].BYTES_PER_ELEMENT){
      const ta = args[0];
      if(0===ta.byteLength){
        wasm.exports.sqlite3_randomness(0,0);
        return ta;
      }
      const stack = wasm.pstack.pointer;
      try {
        let n = ta.byteLength, offset = 0;
        const r = wasm.exports.sqlite3_randomness;
        const heap = wasm.heap8u();
        const nAlloc = n < 512 ? n : 512;
        const ptr = wasm.pstack.alloc(nAlloc);
        do{
          const j = (n>nAlloc ? nAlloc : n);
          r(j, ptr);
          ta.set(typedArrayPart(heap, ptr, ptr+j), offset);
          n -= j;
          offset += j;
        } while(n > 0);
      }catch(e){
        console.error("Highly unexpected (and ignored!) "+
                      "exception in sqlite3_randomness():",e);
      }finally{
        wasm.pstack.restore(stack);
      }
      return ta;
    }
    wasm.exports.sqlite3_randomness(...args);
  };

  /** State for sqlite3_wasmfs_opfs_dir(). */
  let __wasmfsOpfsDir = undefined;
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
    if(undefined !== __wasmfsOpfsDir) return __wasmfsOpfsDir;
    // If we have no OPFS, there is no persistent dir
    const pdir = config.wasmfsOpfsDir;
    if(!pdir
       || !self.FileSystemHandle
       || !self.FileSystemDirectoryHandle
       || !self.FileSystemFileHandle){
      return __wasmfsOpfsDir = "";
    }
    try{
      if(pdir && 0===wasm.xCallWrapped(
        'sqlite3_wasm_init_wasmfs', 'i32', ['string'], pdir
      )){
        return __wasmfsOpfsDir = pdir;
      }else{
        return __wasmfsOpfsDir = "";
      }
    }catch(e){
      // sqlite3_wasm_init_wasmfs() is not available
      return __wasmfsOpfsDir = "";
    }
  };

  /**
     Experimental and subject to change or removal.

     Returns true if sqlite3.capi.sqlite3_wasmfs_opfs_dir() is a
     non-empty string and the given name starts with (that string +
     '/'), else returns false.
  */
  capi.sqlite3_wasmfs_filename_is_persistent = function(name){
    const p = capi.sqlite3_wasmfs_opfs_dir();
    return (p && name) ? name.startsWith(p+'/') : false;
  };

  // This bit is highly arguable and is incompatible with the fiddle shell.
  if(false && 0===wasm.exports.sqlite3_vfs_find(0)){
    /* Assume that sqlite3_initialize() has not yet been called.
       This will be the case in an SQLITE_OS_KV build. */
    wasm.exports.sqlite3_initialize();
  }

  /**
     Given an `sqlite3*`, an sqlite3_vfs name, and an optional db name
     (defaulting to "main"), returns a truthy value (see below) if
     that db uses that VFS, else returns false. If pDb is falsy then
     the 3rd argument is ignored and this function returns a truthy
     value if the default VFS name matches that of the 2nd
     argument. Results are undefined if pDb is truthy but refers to an
     invalid pointer. The 3rd argument specifies the database name of
     the given database connection to check, defaulting to the main
     db.

     The 2nd and 3rd arguments may either be a JS string or a WASM
     C-string. If the 2nd argument is a NULL WASM pointer, the default
     VFS is assumed. If the 3rd is a NULL WASM pointer, "main" is
     assumed.

     The truthy value it returns is a pointer to the `sqlite3_vfs`
     object.

     To permit safe use of this function from APIs which may be called
     via the C stack (like SQL UDFs), this function does not throw: if
     bad arguments cause a conversion error when passing into
     wasm-space, false is returned.
  */
  capi.sqlite3_js_db_uses_vfs = function(pDb,vfsName,dbName=0){
    try{
      const pK = capi.sqlite3_vfs_find(vfsName);
      if(!pK) return false;
      else if(!pDb){
        return pK===capi.sqlite3_vfs_find(0) ? pK : false;
      }else{
        return pK===capi.sqlite3_js_db_vfs(pDb,dbName) ? pK : false;
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
  capi.sqlite3_js_vfs_list = function(){
    const rc = [];
    let pVfs = capi.sqlite3_vfs_find(0);
    while(pVfs){
      const oVfs = new capi.sqlite3_vfs(pVfs);
      rc.push(wasm.cstringToJs(oVfs.$zName));
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
  capi.sqlite3_js_db_export = function(pDb){
    if(!pDb) toss3('Invalid sqlite3* argument.');
    if(!wasm.bigIntEnabled) toss3('BigInt64 support is not enabled.');
    const stack = wasm.pstack.pointer;
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
        toss3("Database serialization failed with code",
             sqlite3.capi.sqlite3_js_rc_str(rc));
      }
      pOut = wasm.getPtrValue(ppOut);
      const nOut = wasm.getMemValue(pSize, 'i64');
      rc = nOut
        ? wasm.heap8u().slice(pOut, pOut + Number(nOut))
        : new Uint8Array();
      return rc;
    }finally{
      if(pOut) wasm.exports.sqlite3_free(pOut);
      wasm.pstack.restore(stack);
    }
  };

  /**
     Given a `sqlite3*` and a database name (JS string or WASM
     C-string pointer, which may be 0), returns a pointer to the
     sqlite3_vfs responsible for it. If the given db name is null/0,
     or not provided, then "main" is assumed.
  */
  capi.sqlite3_js_db_vfs =
    (dbPointer, dbName=0)=>wasm.sqlite3_wasm_db_vfs(dbPointer, dbName);

  /**
     A thin wrapper around capi.sqlite3_aggregate_context() which
     behaves the same except that it throws a WasmAllocError if that
     function returns 0. As a special case, if n is falsy it does
     _not_ throw if that function returns 0. That special case is
     intended for use with xFinal() implementations.
  */
  capi.sqlite3_js_aggregate_context = (pCtx, n)=>{
    return capi.sqlite3_aggregate_context(pCtx, n)
      || (n ? WasmAllocError.toss("Cannot allocate",n,
                                  "bytes for sqlite3_aggregate_context()")
          : 0);
  };
  
  if( util.isUIThread() ){
    /* Features specific to the main window thread... */

    /**
       Internal helper for sqlite3_js_kvvfs_clear() and friends.
       Its argument should be one of ('local','session',"").
    */
    const __kvvfsInfo = function(which){
      const rc = Object.create(null);
      rc.prefix = 'kvvfs-'+which;
      rc.stores = [];
      if('session'===which || ""===which) rc.stores.push(self.sessionStorage);
      if('local'===which || ""===which) rc.stores.push(self.localStorage);
      return rc;
    };

    /**
       Clears all storage used by the kvvfs DB backend, deleting any
       DB(s) stored there. Its argument must be either 'session',
       'local', or "". In the first two cases, only sessionStorage
       resp. localStorage is cleared. If it's an empty string (the
       default) then both are cleared. Only storage keys which match
       the pattern used by kvvfs are cleared: any other client-side
       data are retained.

       This function is only available in the main window thread.

       Returns the number of entries cleared.
    */
    capi.sqlite3_js_kvvfs_clear = function(which=""){
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
       ('session', 'local', ""). In the first two cases, only
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
    capi.sqlite3_js_kvvfs_size = function(which=""){
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
    SQLite3Error: SQLite3Error,
    capi,
    util,
    wasm,
    config,
    /**
       Holds the version info of the sqlite3 source tree from which
       the generated sqlite3-api.js gets built. Note that its version
       may well differ from that reported by sqlite3_libversion(), but
       that should be considered a source file mismatch, as the JS and
       WASM files are intended to be built and distributed together.

       This object is initially a placeholder which gets replaced by a
       build-generated object.
    */
    version: Object.create(null),
    /**
       Performs any optional asynchronous library-level initialization
       which might be required. This function returns a Promise which
       resolves to the sqlite3 namespace object. Any error in the
       async init will be fatal to the init as a whole, but init
       routines are themselves welcome to install dummy catch()
       handlers which are not fatal if their failure should be
       considered non-fatal. If called more than once, the second and
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
      lip = lip.map((f)=>{
        const p = (f instanceof Promise) ? f : f(sqlite3);
        return p.catch((e)=>{
          console.error("an async sqlite3 initializer failed:",e);
          throw e;
        });
      });
      //let p = lip.shift();
      //while(lip.length) p = p.then(lip.shift());
      //return p.then(()=>sqlite3);
      return Promise.all(lip).then(()=>sqlite3);
    },
    /**
       scriptInfo ideally gets injected into this object by the
       infrastructure which assembles the JS/WASM module. It contains
       state which must be collected before sqlite3ApiBootstrap() can
       be declared. It is not necessarily available to any
       sqlite3ApiBootstrap.initializers but "should" be in place (if
       it's added at all) by the time that
       sqlite3ApiBootstrap.initializersAsync is processed.

       This state is not part of the public API, only intended for use
       with the sqlite3 API bootstrapping and wasm-loading process.
    */
    scriptInfo: undefined
  };
  try{
    sqlite3ApiBootstrap.initializers.forEach((f)=>{
      f(sqlite3);
    });
  }catch(e){
    /* If we don't report this here, it can get completely swallowed
       up and disappear into the abyss of Promises and Workers. */
    console.error("sqlite3 bootstrap initializer threw:",e);
    throw e;
  }
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
  some pieces. e.g. sqlite3.capi and sqlite3.wasm cannot be fully
  utilized until the whwasmutil.js part is plugged in via
  sqlite3-api-glue.js.
*/
self.sqlite3ApiBootstrap.initializers = [];
/**
  self.sqlite3ApiBootstrap.initializersAsync is an internal detail
  used by the sqlite3 API's amalgamation process. It must not be
  modified by client code except when plugging such code into the
  amalgamation process.

  The counterpart of self.sqlite3ApiBootstrap.initializers,
  specifically for initializers which are asynchronous. All entries in
  this list must be either async functions, non-async functions which
  return a Promise, or a Promise. Each function in the list is called
  with the sqlite3 ojbect as its only argument.

  The resolved value of any Promise is ignored and rejection will kill
  the asyncPostInit() process (at an indeterminate point because all
  of them are run asynchronously in parallel).

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

