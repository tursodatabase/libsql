/*
  2022-05-22

  The author disclaims copyright to this source code.  In place of a
  legal notice, here is a blessing:

  *   May you do good and not evil.
  *   May you find forgiveness for yourself and forgive others.
  *   May you share freely, never taking more than you give.

  ***********************************************************************

  This file is intended to be combined at build-time with other
  related code, most notably a header and footer which wraps this
  whole file into an Emscripten Module.postRun() handler. The sqlite3
  JS API has no hard requirements on Emscripten and does not expose
  any Emscripten APIs to clients. It is structured such that its build
  can be tweaked to include it in arbitrary WASM environments which
  can supply the necessary underlying features (e.g. a POSIX file I/O
  layer).

  Main project home page: https://sqlite.org

  Documentation home page: https://sqlite.org/wasm
*/

/**
   sqlite3ApiBootstrap() is the only global symbol persistently
   exposed by this API. It is intended to be called one time at the
   end of the API amalgamation process, passed configuration details
   for the current environment, and then optionally be removed from
   the global object using `delete globalThis.sqlite3ApiBootstrap`.

   This function is not intended for client-level use. It is intended
   for use in creating bundles configured for specific WASM
   environments.

   This function expects a configuration object, intended to abstract
   away details specific to any given WASM environment, primarily so
   that it can be used without any _direct_ dependency on
   Emscripten. (Note the default values for the config object!) The
   config object is only honored the first time this is
   called. Subsequent calls ignore the argument and return the same
   (configured) object which gets initialized by the first call.  This
   function will throw if any of the required config options are
   missing.

   The config object properties include:

   - `exports`[^1]: the "exports" object for the current WASM
     environment. In an Emscripten-based build, this should be set to
     `Module['asm']`.

   - `memory`[^1]: optional WebAssembly.Memory object, defaulting to
     `exports.memory`. In Emscripten environments this should be set
     to `Module.wasmMemory` if the build uses `-sIMPORTED_MEMORY`, or be
     left undefined/falsy to default to `exports.memory` when using
     WASM-exported memory.

   - `bigIntEnabled`: true if BigInt support is enabled. Defaults to
     true if `globalThis.BigInt64Array` is available, else false. Some APIs
     will throw exceptions if called without BigInt support, as BigInt
     is required for marshalling C-side int64 into and out of JS.
     (Sidebar: it is technically possible to add int64 support via
     marshalling of int32 pairs, but doing so is unduly invasive.)

   - `allocExportName`: the name of the function, in `exports`, of the
     `malloc(3)`-compatible routine for the WASM environment. Defaults
     to `"sqlite3_malloc"`. Beware that using any allocator other than
     sqlite3_malloc() may require care in certain client-side code
     regarding which allocator is uses. Notably, sqlite3_deserialize()
     and sqlite3_serialize() can only safely use memory from different
     allocators under very specific conditions. The canonical builds
     of this API guaranty that `sqlite3_malloc()` is the JS-side
     allocator implementation.

   - `deallocExportName`: the name of the function, in `exports`, of
     the `free(3)`-compatible routine for the WASM
     environment. Defaults to `"sqlite3_free"`.

   - `reallocExportName`: the name of the function, in `exports`, of
     the `realloc(3)`-compatible routine for the WASM
     environment. Defaults to `"sqlite3_realloc"`.

   - `debug`, `log`, `warn`, and `error` may be functions equivalent
     to the like-named methods of the global `console` object. By
     default, these map directly to their `console` counterparts, but
     can be replaced with (e.g.) empty functions to squelch all such
     output.

   - `wasmfsOpfsDir`[^1]: Specifies the "mount point" of the OPFS-backed
     filesystem in WASMFS-capable builds.


   [^1] = This property may optionally be a function, in which case
          this function calls that function to fetch the value,
          enabling delayed evaluation.

   The returned object is the top-level sqlite3 namespace object.

*/
'use strict';
globalThis.sqlite3ApiBootstrap = function sqlite3ApiBootstrap(
  apiConfig = (globalThis.sqlite3ApiConfig || sqlite3ApiBootstrap.defaultConfig)
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
      return !!globalThis.BigInt64Array;
    })(),
    debug: console.debug.bind(console),
    warn: console.warn.bind(console),
    error: console.error.bind(console),
    log: console.log.bind(console),
    wasmfsOpfsDir: '/opfs',
    /**
       useStdAlloc is just for testing allocator discrepancies. The
       docs guarantee that this is false in the canonical builds. For
       99% of purposes it doesn't matter which allocators we use, but
       it becomes significant with, e.g., sqlite3_deserialize() and
       certain wasm.xWrap.resultAdapter()s.
    */
    useStdAlloc: false
  }, apiConfig || {});

  Object.assign(config, {
    allocExportName: config.useStdAlloc ? 'malloc' : 'sqlite3_malloc',
    deallocExportName: config.useStdAlloc ? 'free' : 'sqlite3_free',
    reallocExportName: config.useStdAlloc ? 'realloc' : 'sqlite3_realloc'
  }, config);

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
     infrastructure and glue code.

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

       If its first argument is an integer, it is assumed to be
       an SQLITE_... result code and it is passed to
       sqlite3.capi.sqlite3_js_rc_str() to stringify it.

       If called with exactly 2 arguments and the 2nd is an object,
       that object is treated as the 2nd argument to the parent
       constructor.

       The exception's message is created by concatenating its
       arguments with a space between each, except for the
       two-args-with-an-objec form and that the first argument will
       get coerced to a string, as described above, if it's an
       integer.

       If passed an integer first argument, the error object's
       `resultCode` member will be set to the given integer value,
       else it will be set to capi.SQLITE_ERROR.
    */
    constructor(...args){
      let rc;
      if(args.length){
        if(__isInt(args[0])){
          rc = args[0];
          if(1===args.length){
            super(__rcStr(args[0]));
          }else{
            const rcStr = __rcStr(rc);
            if('object'===typeof args[1]){
              super(rcStr,args[1]);
            }else{
              args[0] = rcStr+':';
              super(args.join(' '));
            }
          }
        }else{
          if(2===args.length && 'object'===typeof args[1]){
            super(...args);
          }else{
            super(args.join(' '));
          }
        }
      }
      this.resultCode = rc || capi.SQLITE_ERROR;
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
     Returns true if v appears to be one of our bind()-able TypedArray
     types: Uint8Array or Int8Array or ArrayBuffer. Support for
     TypedArrays with element sizes >1 is a potential TODO just
     waiting on a use case to justify them. Until then, their `buffer`
     property can be used to pass them as an ArrayBuffer. If it's not
     a bindable array type, a falsy value is returned.
  */
  const isBindableTypedArray = (v)=>{
    return v && (v instanceof Uint8Array
                 || v instanceof Int8Array
                 || v instanceof ArrayBuffer);
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
    return v && (v instanceof Uint8Array
                 || v instanceof Int8Array
                 || v instanceof ArrayBuffer);
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
     returned. If it looks like a WASM pointer, wasm.cstrToJs(v) is
     returned. Else v is returned as-is.
  */
  const flexibleString = function(v){
    if(isSQLableTypedArray(v)){
      return typedArrayToString(
        (v instanceof ArrayBuffer) ? new Uint8Array(v) : v
      );
    }
    else if(Array.isArray(v)) return v.join("");
    else if(wasm.isPtr(v)) v = wasm.cstrToJs(v);
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
      if(2===args.length && 'object'===typeof args[1]){
        super(...args);
      }else if(args.length){
        super(args.join(' '));
      }else{
        super("Allocation failed.");
      }
      this.resultCode = capi.SQLITE_NOMEM;
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
       sqlite3_bind_blob() works exactly like its C counterpart unless
       its 3rd argument is one of:

       - JS string: the 3rd argument is converted to a C string, the
         4th argument is ignored, and the C-string's length is used
         in its place.

       - Array: converted to a string as defined for "flexible
         strings" and then it's treated as a JS string.

       - Int8Array or Uint8Array: wasm.allocFromTypedArray() is used to
         conver the memory to the WASM heap. If the 4th argument is
         0 or greater, it is used as-is, otherwise the array's byteLength
         value is used. This is an exception to the C API's undefined
         behavior for a negative 4th argument, but results are undefined
         if the given 4th argument value is greater than the byteLength
         of the input array.

       - If it's an ArrayBuffer, it gets wrapped in a Uint8Array and
         treated as that type.

       In all of those cases, the final argument (destructor) is
       ignored and capi.SQLITE_WASM_DEALLOC is assumed.

       A 3rd argument of `null` is treated as if it were a WASM pointer
       of 0.

       If the 3rd argument is neither a WASM pointer nor one of the
       above-described types, capi.SQLITE_MISUSE is returned.

       The first argument may be either an `sqlite3_stmt*` WASM
       pointer or an sqlite3.oo1.Stmt instance.

       For consistency with the C API, it requires the same number of
       arguments. It returns capi.SQLITE_MISUSE if passed any other
       argument count.
    */
    sqlite3_bind_blob: undefined/*installed later*/,

    /**
       sqlite3_bind_text() works exactly like its C counterpart unless
       its 3rd argument is one of:

       - JS string: the 3rd argument is converted to a C string, the
         4th argument is ignored, and the C-string's length is used
         in its place.

       - Array: converted to a string as defined for "flexible
         strings". The 4th argument is ignored and a value of -1
         is assumed.

       - Int8Array or Uint8Array: is assumed to contain UTF-8 text, is
         converted to a string. The 4th argument is ignored, replaced
         by the array's byteLength value.

       - If it's an ArrayBuffer, it gets wrapped in a Uint8Array and
         treated as that type.

       In each of those cases, the final argument (text destructor) is
       ignored and capi.SQLITE_WASM_DEALLOC is assumed.

       A 3rd argument of `null` is treated as if it were a WASM pointer
       of 0.

       If the 3rd argument is neither a WASM pointer nor one of the
       above-described types, capi.SQLITE_MISUSE is returned.

       The first argument may be either an `sqlite3_stmt*` WASM
       pointer or an sqlite3.oo1.Stmt instance.

       For consistency with the C API, it requires the same number of
       arguments. It returns capi.SQLITE_MISUSE if passed any other
       argument count.

       If client code needs to bind partial strings, it needs to
       either parcel the string up before passing it in here or it
       must pass in a WASM pointer for the 3rd argument and a valid
       4th-argument value, taking care not to pass a value which
       truncates a multi-byte UTF-8 character. When passing
       WASM-format strings, it is important that the final argument be
       valid or unexpected content can result can result, or even a
       crash if the application reads past the WASM heap bounds.
    */
    sqlite3_bind_text: undefined/*installed later*/,

    /**
       sqlite3_create_function_v2() differs from its native
       counterpart only in the following ways:

       1) The fourth argument (`eTextRep`) argument must not specify
       any encoding other than sqlite3.SQLITE_UTF8. The JS API does not
       currently support any other encoding and likely never
       will. This function does not replace that argument on its own
       because it may contain other flags. As a special case, if
       the bottom 4 bits of that argument are 0, SQLITE_UTF8 is
       assumed.

       2) Any of the four final arguments may be either WASM pointers
       (assumed to be function pointers) or JS Functions. In the
       latter case, each gets bound to WASM using
       sqlite3.capi.wasm.installFunction() and that wrapper is passed
       on to the native implementation.

       For consistency with the C API, it requires the same number of
       arguments. It returns capi.SQLITE_MISUSE if passed any other
       argument count.

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
    sqlite3_create_function_v2: (
      pDb, funcName, nArg, eTextRep, pApp,
      xFunc, xStep, xFinal, xDestroy
    )=>{/*installed later*/},
    /**
       Equivalent to passing the same arguments to
       sqlite3_create_function_v2(), with 0 as the final argument.
    */
    sqlite3_create_function: (
      pDb, funcName, nArg, eTextRep, pApp,
      xFunc, xStep, xFinal
    )=>{/*installed later*/},
    /**
       The sqlite3_create_window_function() JS wrapper differs from
       its native implementation in the exact same way that
       sqlite3_create_function_v2() does. The additional function,
       xInverse(), is treated identically to xStep() by the wrapping
       layer.
    */
    sqlite3_create_window_function: (
      pDb, funcName, nArg, eTextRep, pApp,
      xStep, xFinal, xValue, xInverse, xDestroy
    )=>{/*installed later*/},
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
       Uint8Array, Int8Array, or ArrayBuffer (all of which are assumed
       to hold SQL). If it is, this function assumes case (1) and
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
       parameters) requires using `capi.wasm.peek()` (or
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
       "string:flexible" argument converter.
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
    isUIThread: ()=>(globalThis.window===globalThis && !!globalThis.document),
    // is this true for ESM?: 'undefined'===typeof WorkerGlobalScope
    isSharedTypedArray,
    toss: function(...args){throw new Error(args.join(' '))},
    toss3,
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
       size described by this.ptrSizeof.
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
       When Emscripten compiles with `-sIMPORTED_MEMORY`, it
       initalizes the heap and imports it into wasm, as opposed to
       the other way around. In this case, the memory is not
       available via this.exports.memory.
    */
    memory: config.memory || config.exports['memory']
      || toss3("API config object requires a WebAssembly.Memory object",
              "in either config.exports.memory (exported)",
              "or config.memory (imported)."),

    /**
       The API's primary point of access to the WASM-side memory
       allocator.  Works like sqlite3_malloc() but throws a
       WasmAllocError if allocation fails. It is important that any
       code which might pass through the sqlite3 C API NOT throw and
       must instead return SQLITE_NOMEM (or equivalent, depending on
       the context).

       Very few cases in the sqlite3 JS APIs can result in
       client-defined functions propagating exceptions via the C-style
       API. Most notably, this applies to WASM-bound JS functions
       which are created directly by clients and passed on _as WASM
       function pointers_ to functions such as
       sqlite3_create_function_v2(). Such bindings created
       transparently by this API will automatically use wrappers which
       catch exceptions and convert them to appropriate error codes.

       For cases where non-throwing allocation is required, use
       this.alloc.impl(), which is direct binding of the
       underlying C-level allocator.

       Design note: this function is not named "malloc" primarily
       because Emscripten uses that name and we wanted to avoid any
       confusion early on in this code's development, when it still
       had close ties to Emscripten's glue code.
    */
    alloc: undefined/*installed later*/,

    /**
       Rarely necessary in JS code, this routine works like
       sqlite3_realloc(M,N), where M is either NULL or a pointer
       obtained from this function or this.alloc() and N is the number
       of bytes to reallocate the block to. Returns a pointer to the
       reallocated block or 0 if allocation fails.

       If M is NULL and N is positive, this behaves like
       this.alloc(N). If N is 0, it behaves like this.dealloc().
       Results are undefined if N is negative (sqlite3_realloc()
       treats that as 0, but if this code is built with a different
       allocator it may misbehave with negative values).

       Like this.alloc.impl(), this.realloc.impl() is a direct binding
       to the underlying realloc() implementation which does not throw
       exceptions, instead returning 0 on allocation error.
    */
    realloc: undefined/*installed later*/,

    /**
       The API's primary point of access to the WASM-side memory
       deallocator. Works like sqlite3_free().

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

     The argument may be a Uint8Array, Int8Array, or ArrayBuffer,
     and it throws if passed any other type.

     As a special case, to avoid further special cases where
     this is used, if srcTypedArray.byteLength is 0, it
     allocates a single byte and sets it to the value
     0. Even in such cases, calls must behave as if the
     allocated memory has exactly srcTypedArray.byteLength
     bytes.
  */
  wasm.allocFromTypedArray = function(srcTypedArray){
    if(srcTypedArray instanceof ArrayBuffer){
      srcTypedArray = new Uint8Array(srcTypedArray);
    }
    affirmBindableTypedArray(srcTypedArray);
    const pRet = wasm.alloc(srcTypedArray.byteLength || 1);
    wasm.heapForSize(srcTypedArray.constructor).set(
      srcTypedArray.byteLength ? srcTypedArray : [0], pRet
    );
    return pRet;
  };

  {
    // Set up allocators...
    const keyAlloc = config.allocExportName,
          keyDealloc = config.deallocExportName,
          keyRealloc = config.reallocExportName;
    for(const key of [keyAlloc, keyDealloc, keyRealloc]){
      const f = wasm.exports[key];
      if(!(f instanceof Function)) toss3("Missing required exports[",key,"] function.");
    }

    wasm.alloc = function f(n){
      return f.impl(n) || WasmAllocError.toss("Failed to allocate",n," bytes.");
    };
    wasm.alloc.impl = wasm.exports[keyAlloc];
    wasm.realloc = function f(m,n){
      const m2 = f.impl(m,n);
      return n ? (m2 || WasmAllocError.toss("Failed to reallocate",n," bytes.")) : 0;
    };
    wasm.realloc.impl = wasm.exports[keyRealloc];
    wasm.dealloc = wasm.exports[keyDealloc];
  }

  /**
     Reports info about compile-time options using
     sqlite3_compileoption_get() and sqlite3_compileoption_used(). It
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
     as wasm.poke() and wasm.heap8u().slice().
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
       to the memory. On error, throws a WasmAllocError. The
       memory must eventually be released using restore().

       If n is a string, it must be a WASM "IR" value in the set
       accepted by wasm.sizeofIR(), which is mapped to the size of
       that data type. If passed a string not in that set, it throws a
       WasmAllocError.

       This method always adjusts the given value to be a multiple
       of 8 bytes because failing to do so can lead to incorrect
       results when reading and writing 64-bit values from/to the WASM
       heap. Similarly, the returned address is always 8-byte aligned.
    */
    alloc: function(n){
      if('string'===typeof n && !(n = wasm.sizeofIR(n))){
        WasmAllocError.toss("Invalid value for pstack.alloc(",arguments[0],")");
      }
      return wasm.exports.sqlite3_wasm_pstack_alloc(n)
        || WasmAllocError.toss("Could not allocate",n,
                               "bytes from the pstack.");
    },
    /**
       alloc()'s n chunks, each sz bytes, as a single memory block and
       returns the addresses as an array of n element, each holding
       the address of one chunk.

       sz may optionally be an IR string accepted by wasm.sizeofIR().

       Throws a WasmAllocError if allocation fails.

       Example:

       ```
       const [p1, p2, p3] = wasm.pstack.allocChunks(3,4);
       ```
    */
    allocChunks: function(n,sz){
      if('string'===typeof sz && !(sz = wasm.sizeofIR(sz))){
        WasmAllocError.toss("Invalid size value for allocChunks(",arguments[1],")");
      }
      const mem = wasm.pstack.alloc(n * sz);
      const rc = [];
      let i = 0, offset = 0;
      for(; i < n; ++i, offset += sz) rc.push(mem + offset);
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
       e.g. using wasm.poke() or wasm.peek(), it is
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
     and, if it is, enables it (if needed). After the first call it
     always returns the cached result.

     If the returned string is not empty, any files stored under the
     given path (recursively) are housed in OPFS storage. If the
     returned string is empty, this particular persistent storage
     option is not available on the client.

     Though the mount point name returned by this function is intended
     to remain stable, clients should not hard-coded it anywhere. Always call this function to get the path.

     Note that this function is a no-op in must builds of this
     library, as the WASMFS capability requires a custom
     build.
  */
  capi.sqlite3_wasmfs_opfs_dir = function(){
    if(undefined !== __wasmfsOpfsDir) return __wasmfsOpfsDir;
    // If we have no OPFS, there is no persistent dir
    const pdir = config.wasmfsOpfsDir;
    if(!pdir
       || !globalThis.FileSystemHandle
       || !globalThis.FileSystemDirectoryHandle
       || !globalThis.FileSystemFileHandle){
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
     Returns true if sqlite3.capi.sqlite3_wasmfs_opfs_dir() is a
     non-empty string and the given name starts with (that string +
     '/'), else returns false.
  */
  capi.sqlite3_wasmfs_filename_is_persistent = function(name){
    const p = capi.sqlite3_wasmfs_opfs_dir();
    return (p && name) ? name.startsWith(p+'/') : false;
  };

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
      rc.push(wasm.cstrToJs(oVfs.$zName));
      pVfs = oVfs.$pNext;
      oVfs.dispose();
    }
    return rc;
  };

  /**
     A convenience wrapper around sqlite3_serialize() which serializes
     the given `sqlite3*` pointer to a Uint8Array. The first argument
     may be either an `sqlite3*` or an sqlite3.oo1.DB instance.

     On success it returns a Uint8Array. If the schema is empty, an
     empty array is returned.

     `schema` is the schema to serialize. It may be a WASM C-string
     pointer or a JS string. If it is falsy, it defaults to `"main"`.

     On error it throws with a description of the problem.
  */
  capi.sqlite3_js_db_export = function(pDb, schema=0){
    pDb = wasm.xWrap.testConvertArg('sqlite3*', pDb);
    if(!pDb) toss3('Invalid sqlite3* argument.');
    if(!wasm.bigIntEnabled) toss3('BigInt64 support is not enabled.');
    const scope = wasm.scopedAllocPush();
    let pOut;
    try{
      const pSize = wasm.scopedAlloc(8/*i64*/ + wasm.ptrSizeof);
      const ppOut = pSize + 8;
      /**
         Maintenance reminder, since this cost a full hour of grief
         and confusion: if the order of pSize/ppOut are reversed in
         that memory block, fetching the value of pSize after the
         export reads a garbage size because it's not on an 8-byte
         memory boundary!
      */
      const zSchema = schema
            ? (wasm.isPtr(schema) ? schema : wasm.scopedAllocCString(''+schema))
            : 0;
      let rc = wasm.exports.sqlite3_wasm_db_serialize(
        pDb, zSchema, ppOut, pSize, 0
      );
      if(rc){
        toss3("Database serialization failed with code",
             sqlite3.capi.sqlite3_js_rc_str(rc));
      }
      pOut = wasm.peekPtr(ppOut);
      const nOut = wasm.peek(pSize, 'i64');
      rc = nOut
        ? wasm.heap8u().slice(pOut, pOut + Number(nOut))
        : new Uint8Array();
      return rc;
    }finally{
      if(pOut) wasm.exports.sqlite3_free(pOut);
      wasm.scopedAllocPop(scope);
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

  /**
     If the current environment supports the POSIX file APIs, this routine
     creates (or overwrites) the given file using those APIs. This is
     primarily intended for use in Emscripten-based builds where the POSIX
     APIs are transparently proxied by an in-memory virtual filesystem.
     It may behave diffrently in other environments.

     The first argument must be either a JS string or WASM C-string
     holding the filename. Note that this routine does _not_ create
     intermediary directories if the filename has a directory part.

     The 2nd argument may either a valid WASM memory pointer, an
     ArrayBuffer, or a Uint8Array. The 3rd must be the length, in
     bytes, of the data array to copy. If the 2nd argument is an
     ArrayBuffer or Uint8Array and the 3rd is not a positive integer
     then the 3rd defaults to the array's byteLength value.

     Results are undefined if data is a WASM pointer and dataLen is
     exceeds data's bounds.

     Throws if any arguments are invalid or if creating or writing to
     the file fails.

     Added in 3.43 as an alternative for the deprecated
     sqlite3_js_vfs_create_file().
  */
  capi.sqlite3_js_posix_create_file = function(filename, data, dataLen){
    let pData;
    if(data && wasm.isPtr(data)){
      pData = data;
    }else if(data instanceof ArrayBuffer || data instanceof Uint8Array){
      pData = wasm.allocFromTypedArray(data);
      if(arguments.length<3 || !util.isInt32(dataLen) || dataLen<0){
        dataLen = data.byteLength;
      }
    }else{
      SQLite3Error.toss("Invalid 2nd argument for sqlite3_js_posix_create_file().");
    }
    try{
      if(!util.isInt32(dataLen) || dataLen<0){
        SQLite3Error.toss("Invalid 3rd argument for sqlite3_js_posix_create_file().");
      }
      const rc = wasm.sqlite3_wasm_posix_create_file(filename, pData, dataLen);
      if(rc) SQLite3Error.toss("Creation of file failed with sqlite3 result code",
                               capi.sqlite3_js_rc_str(rc));
    }finally{
       wasm.dealloc(pData);
    }
  };

  /**
     Deprecation warning: this function does not work properly in
     debug builds of sqlite3 because its out-of-scope use of the
     sqlite3_vfs API triggers assertions in the core library.  That
     was unfortunately not discovered until 2023-08-11. This function
     is now deprecated and should not be used in new code.

     Alternative options:

     - "unix" VFS and its variants can get equivalent functionality
       with sqlite3_js_posix_create_file().

     - OPFS: use either sqlite3.oo1.OpfsDb.importDb(), for the "opfs"
       VFS, or the importDb() method of the PoolUtil object provided
       by the "opfs-sahpool" OPFS (noting that its VFS name may differ
       depending on client-side configuration). We cannot proxy those
       from here because the former is necessarily asynchronous and
       the latter requires information not available to this function.

     Creates a file using the storage appropriate for the given
     sqlite3_vfs.  The first argument may be a VFS name (JS string
     only, NOT a WASM C-string), WASM-managed `sqlite3_vfs*`, or
     a capi.sqlite3_vfs instance. Pass 0 (a NULL pointer) to use the
     default VFS. If passed a string which does not resolve using
     sqlite3_vfs_find(), an exception is thrown. (Note that a WASM
     C-string is not accepted because it is impossible to
     distinguish from a C-level `sqlite3_vfs*`.)

     The second argument, the filename, must be a JS or WASM C-string.

     The 3rd may either be falsy, a valid WASM memory pointer, an
     ArrayBuffer, or a Uint8Array. The 4th must be the length, in
     bytes, of the data array to copy. If the 3rd argument is an
     ArrayBuffer or Uint8Array and the 4th is not a positive integer
     then the 4th defaults to the array's byteLength value.

     If data is falsy then a file is created with dataLen bytes filled
     with uninitialized data (whatever truncate() leaves there). If
     data is not falsy then a file is created or truncated and it is
     filled with the first dataLen bytes of the data source.

     Throws if any arguments are invalid or if creating or writing to
     the file fails.

     Note that most VFSes do _not_ automatically create directory
     parts of filenames, nor do all VFSes have a concept of
     directories.  If the given filename is not valid for the given
     VFS, an exception will be thrown. This function exists primarily
     to assist in implementing file-upload capability, with the caveat
     that clients must have some idea of the VFS into which they want
     to upload and that VFS must support the operation.

     VFS-specific notes:

     - "memdb": results are undefined.

     - "kvvfs": will fail with an I/O error due to strict internal
       requirments of that VFS's xTruncate().

     - "unix" and related: will use the WASM build's equivalent of the
       POSIX I/O APIs. This will work so long as neither a specific
       VFS nor the WASM environment imposes requirements which break it.

     - "opfs": uses OPFS storage and creates directory parts of the
       filename. It can only be used to import an SQLite3 database
       file and will fail if given anything else.
  */
  capi.sqlite3_js_vfs_create_file = function(vfs, filename, data, dataLen){
    config.warn("sqlite3_js_vfs_create_file() is deprecated and",
                "should be avoided because it can lead to C-level crashes.",
                "See its documentation for alternative options.");
    let pData;
    if(data){
      if(wasm.isPtr(data)){
        pData = data;
      }else if(data instanceof ArrayBuffer){
        data = new Uint8Array(data);
      }
      if(data instanceof Uint8Array){
        pData = wasm.allocFromTypedArray(data);
        if(arguments.length<4 || !util.isInt32(dataLen) || dataLen<0){
          dataLen = data.byteLength;
        }
      }else{
        SQLite3Error.toss("Invalid 3rd argument type for sqlite3_js_vfs_create_file().");
      }
    }else{
       pData = 0;
    }
    if(!util.isInt32(dataLen) || dataLen<0){
      wasm.dealloc(pData);
      SQLite3Error.toss("Invalid 4th argument for sqlite3_js_vfs_create_file().");
    }
    try{
      const rc = wasm.sqlite3_wasm_vfs_create_file(vfs, filename, pData, dataLen);
      if(rc) SQLite3Error.toss("Creation of file failed with sqlite3 result code",
                               capi.sqlite3_js_rc_str(rc));
    }finally{
       wasm.dealloc(pData);
    }
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
      if('session'===which || ""===which) rc.stores.push(globalThis.sessionStorage);
      if('local'===which || ""===which) rc.stores.push(globalThis.localStorage);
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

  /**
     Wraps all known variants of the C-side variadic
     sqlite3_db_config().

     Full docs: https://sqlite.org/c3ref/db_config.html

     Returns capi.SQLITE_MISUSE if op is not a valid operation ID.

     The variants which take `(int, int*)` arguments treat a
     missing or falsy pointer argument as 0.
  */
  capi.sqlite3_db_config = function(pDb, op, ...args){
    if(!this.s){
      this.s = wasm.xWrap('sqlite3_wasm_db_config_s','int',
                          ['sqlite3*', 'int', 'string:static']
                          /* MAINDBNAME requires a static string */);
      this.pii = wasm.xWrap('sqlite3_wasm_db_config_pii', 'int',
                            ['sqlite3*', 'int', '*','int', 'int']);
      this.ip = wasm.xWrap('sqlite3_wasm_db_config_ip','int',
                           ['sqlite3*', 'int', 'int','*']);
    }
    switch(op){
        case capi.SQLITE_DBCONFIG_ENABLE_FKEY:
        case capi.SQLITE_DBCONFIG_ENABLE_TRIGGER:
        case capi.SQLITE_DBCONFIG_ENABLE_FTS3_TOKENIZER:
        case capi.SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION:
        case capi.SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE:
        case capi.SQLITE_DBCONFIG_ENABLE_QPSG:
        case capi.SQLITE_DBCONFIG_TRIGGER_EQP:
        case capi.SQLITE_DBCONFIG_RESET_DATABASE:
        case capi.SQLITE_DBCONFIG_DEFENSIVE:
        case capi.SQLITE_DBCONFIG_WRITABLE_SCHEMA:
        case capi.SQLITE_DBCONFIG_LEGACY_ALTER_TABLE:
        case capi.SQLITE_DBCONFIG_DQS_DML:
        case capi.SQLITE_DBCONFIG_DQS_DDL:
        case capi.SQLITE_DBCONFIG_ENABLE_VIEW:
        case capi.SQLITE_DBCONFIG_LEGACY_FILE_FORMAT:
        case capi.SQLITE_DBCONFIG_TRUSTED_SCHEMA:
        case capi.SQLITE_DBCONFIG_STMT_SCANSTATUS:
        case capi.SQLITE_DBCONFIG_REVERSE_SCANORDER:
          return this.ip(pDb, op, args[0], args[1] || 0);
        case capi.SQLITE_DBCONFIG_LOOKASIDE:
          return this.pii(pDb, op, args[0], args[1], args[2]);
        case capi.SQLITE_DBCONFIG_MAINDBNAME:
          return this.s(pDb, op, args[0]);
        default:
          return capi.SQLITE_MISUSE;
    }
  }.bind(Object.create(null));

  /**
     Given a (sqlite3_value*), this function attempts to convert it
     to an equivalent JS value with as much fidelity as feasible and
     return it.

     By default it throws if it cannot determine any sensible
     conversion. If passed a falsy second argument, it instead returns
     `undefined` if no suitable conversion is found.  Note that there
     is no conversion from SQL to JS which results in the `undefined`
     value, so `undefined` has an unambiguous meaning here.  It will
     always throw a WasmAllocError if allocating memory for a
     conversion fails.

     Caveats:

     - It does not support sqlite3_value_to_pointer() conversions
       because those require a type name string which this function
       does not have and cannot sensibly be given at the level of the
       API where this is used (e.g. automatically converting UDF
       arguments). Clients using sqlite3_value_to_pointer(), and its
       related APIs, will need to manage those themselves.
  */
  capi.sqlite3_value_to_js = function(pVal,throwIfCannotConvert=true){
    let arg;
    const valType = capi.sqlite3_value_type(pVal);
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
          if(throwIfCannotConvert){
            toss3(capi.SQLITE_MISMATCH,
                  "Unhandled sqlite3_value_type():",valType);
          }
          arg = undefined;
    }
    return arg;
  };

  /**
     Requires a C-style array of `sqlite3_value*` objects and the
     number of entries in that array. Returns a JS array containing
     the results of passing each C array entry to
     sqlite3_value_to_js(). The 3rd argument to this function is
     passed on as the 2nd argument to that one.
  */
  capi.sqlite3_values_to_js = function(argc,pArgv,throwIfCannotConvert=true){
    let i;
    const tgt = [];
    for(i = 0; i < argc; ++i){
      /**
         Curiously: despite ostensibly requiring 8-byte
         alignment, the pArgv array is parcelled into chunks of
         4 bytes (1 pointer each). The values those point to
         have 8-byte alignment but the individual argv entries
         do not.
      */
      tgt.push(capi.sqlite3_value_to_js(
        wasm.peekPtr(pArgv + (wasm.ptrSizeof * i)),
        throwIfCannotConvert
      ));
    }
    return tgt;
  };

  /**
     Calls either sqlite3_result_error_nomem(), if e is-a
     WasmAllocError, or sqlite3_result_error(). In the latter case,
     the second arugment is coerced to a string to create the error
     message.

     The first argument is a (sqlite3_context*). Returns void.
     Does not throw.
  */
  capi.sqlite3_result_error_js = function(pCtx,e){
    if(e instanceof WasmAllocError){
      capi.sqlite3_result_error_nomem(pCtx);
    }else{
      /* Maintenance reminder: ''+e, rather than e.message,
         will prefix e.message with e.name, so it includes
         the exception's type name in the result. */;
      capi.sqlite3_result_error(pCtx, ''+e, -1);
    }
  };

  /**
     This function passes its 2nd argument to one of the
     sqlite3_result_xyz() routines, depending on the type of that
     argument:

     - If (val instanceof Error), this function passes it to
       sqlite3_result_error_js().
     - `null`: `sqlite3_result_null()`
     - `boolean`: `sqlite3_result_int()` with a value of 0 or 1.
     - `number`: `sqlite3_result_int()`, `sqlite3_result_int64()`, or
       `sqlite3_result_double()`, depending on the range of the number
       and whether or not int64 support is enabled.
     - `bigint`: similar to `number` but will trigger an error if the
       value is too big to store in an int64.
     - `string`: `sqlite3_result_text()`
     - Uint8Array or Int8Array or ArrayBuffer: `sqlite3_result_blob()`
     - `undefined`: is a no-op provided to simplify certain use cases.

     Anything else triggers `sqlite3_result_error()` with a
     description of the problem.

     The first argument to this function is a `(sqlite3_context*)`.
     Returns void. Does not throw.
  */
  capi.sqlite3_result_js = function(pCtx,val){
    if(val instanceof Error){
      capi.sqlite3_result_error_js(pCtx, val);
      return;
    }
    try{
      switch(typeof val) {
          case 'undefined':
            /* This is a no-op. This routine originated in the create_function()
               family of APIs and in that context, passing in undefined indicated
               that the caller was responsible for calling sqlite3_result_xxx()
               (if needed). */
            break;
          case 'boolean':
            capi.sqlite3_result_int(pCtx, val ? 1 : 0);
            break;
          case 'bigint':
            if(util.bigIntFits32(val)){
              capi.sqlite3_result_int(pCtx, Number(val));
            }else if(util.bigIntFitsDouble(val)){
              capi.sqlite3_result_double(pCtx, Number(val));
            }else if(wasm.bigIntEnabled){
              if(util.bigIntFits64(val)) capi.sqlite3_result_int64(pCtx, val);
              else toss3("BigInt value",val.toString(),"is too BigInt for int64.");
            }else{
              toss3("BigInt value",val.toString(),"is too BigInt.");
            }
            break;
          case 'number': {
            let f;
            if(util.isInt32(val)){
              f = capi.sqlite3_result_int;
            }else if(wasm.bigIntEnabled
                     && Number.isInteger(val)
                     && util.bigIntFits64(BigInt(val))){
              f = capi.sqlite3_result_int64;
            }else{
              f = capi.sqlite3_result_double;
            }
            f(pCtx, val);
            break;
          }
          case 'string': {
            const [p, n] = wasm.allocCString(val,true);
            capi.sqlite3_result_text(pCtx, p, n, capi.SQLITE_WASM_DEALLOC);
            break;
          }
          case 'object':
            if(null===val/*yes, typeof null === 'object'*/) {
              capi.sqlite3_result_null(pCtx);
              break;
            }else if(util.isBindableTypedArray(val)){
              const pBlob = wasm.allocFromTypedArray(val);
              capi.sqlite3_result_blob(
                pCtx, pBlob, val.byteLength,
                capi.SQLITE_WASM_DEALLOC
              );
              break;
            }
            // else fall through
          default:
            toss3("Don't not how to handle this UDF result value:",(typeof val), val);
      }
    }catch(e){
      capi.sqlite3_result_error_js(pCtx, e);
    }
  };

  /**
     Returns the result sqlite3_column_value(pStmt,iCol) passed to
     sqlite3_value_to_js(). The 3rd argument of this function is
     ignored by this function except to pass it on as the second
     argument of sqlite3_value_to_js(). If the sqlite3_column_value()
     returns NULL (e.g. because the column index is out of range),
     this function returns `undefined`, regardless of the 3rd
     argument. If the 3rd argument is falsy and conversion fails,
     `undefined` will be returned.

     Note that sqlite3_column_value() returns an "unprotected" value
     object, but in a single-threaded environment (like this one)
     there is no distinction between protected and unprotected values.
  */
  capi.sqlite3_column_js = function(pStmt, iCol, throwIfCannotConvert=true){
    const v = capi.sqlite3_column_value(pStmt, iCol);
    return (0===v) ? undefined : capi.sqlite3_value_to_js(v, throwIfCannotConvert);
  };

  /**
     Internal impl of sqlite3_preupdate_new/old_js() and
     sqlite3changeset_new/old_js().
  */
  const __newOldValue = function(pObj, iCol, impl){
    impl = capi[impl];
    if(!this.ptr) this.ptr = wasm.allocPtr();
    else wasm.pokePtr(this.ptr, 0);
    const rc = impl(pObj, iCol, this.ptr);
    if(rc) return SQLite3Error.toss(rc,arguments[2]+"() failed with code "+rc);
    const pv = wasm.peekPtr(this.ptr);
    return pv ? capi.sqlite3_value_to_js( pv, true ) : undefined;
  }.bind(Object.create(null));

  /**
     A wrapper around sqlite3_preupdate_new() which fetches the
     sqlite3_value at the given index and returns the result of
     passing it to sqlite3_value_to_js(). Throws on error.
  */
  capi.sqlite3_preupdate_new_js =
    (pDb, iCol)=>__newOldValue(pDb, iCol, 'sqlite3_preupdate_new');

  /**
     The sqlite3_preupdate_old() counterpart of
     sqlite3_preupdate_new_js(), with an identical interface.
  */
  capi.sqlite3_preupdate_old_js =
    (pDb, iCol)=>__newOldValue(pDb, iCol, 'sqlite3_preupdate_old');

  /**
     A wrapper around sqlite3changeset_new() which fetches the
     sqlite3_value at the given index and returns the result of
     passing it to sqlite3_value_to_js(). Throws on error.

     If sqlite3changeset_new() succeeds but has no value to report,
     this function returns the undefined value, noting that undefined
     is a valid conversion from an `sqlite3_value`, so is unambiguous.
  */
  capi.sqlite3changeset_new_js =
    (pChangesetIter, iCol) => __newOldValue(pChangesetIter, iCol,
                                            'sqlite3changeset_new');

  /**
     The sqlite3changeset_old() counterpart of
     sqlite3changeset_new_js(), with an identical interface.
  */
  capi.sqlite3changeset_old_js =
    (pChangesetIter, iCol)=>__newOldValue(pChangesetIter, iCol,
                                          'sqlite3changeset_old');

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
       The library reserves the 'client' property for client-side use
       and promises to never define a property with this name nor to
       ever rely on specific contents of it. It makes no such guarantees
       for other properties.
    */
    client: undefined,

    /**
       This function is not part of the public interface, but a
       piece of internal bootstrapping infrastructure.

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

       If called multiple times it will return the same promise on
       subsequent calls. The current build setup precludes that
       possibility, so it's only a hypothetical problem if/when this
       function ever needs to be invoked by clients.

       In Emscripten-based builds, this function is called
       automatically and deleted from this object.
    */
    asyncPostInit: async function ff(){
      if(ff.isReady instanceof Promise) return ff.isReady;
      let lia = sqlite3ApiBootstrap.initializersAsync;
      delete sqlite3ApiBootstrap.initializersAsync;
      const postInit = async ()=>{
        if(!sqlite3.__isUnderTest){
          /* Delete references to internal-only APIs which are used by
             some initializers. Retain them when running in test mode
             so that we can add tests for them. */
          delete sqlite3.util;
          /* It's conceivable that we might want to expose
             StructBinder to client-side code, but it's only useful if
             clients build their own sqlite3.wasm which contains their
             own C struct types. */
          delete sqlite3.StructBinder;
        }
        return sqlite3;
      };
      const catcher = (e)=>{
        config.error("an async sqlite3 initializer failed:",e);
        throw e;
      };
      if(!lia || !lia.length){
        return ff.isReady = postInit().catch(catcher);
      }
      lia = lia.map((f)=>{
        return (f instanceof Function) ? async x=>f(sqlite3) : f;
      });
      lia.push(postInit);
      let p = Promise.resolve(sqlite3);
      while(lia.length) p = p.then(lia.shift());
      return ff.isReady = p.catch(catcher);
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
  globalThis.sqlite3ApiBootstrap.initializers is an internal detail used by
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
globalThis.sqlite3ApiBootstrap.initializers = [];
/**
  globalThis.sqlite3ApiBootstrap.initializersAsync is an internal detail
  used by the sqlite3 API's amalgamation process. It must not be
  modified by client code except when plugging such code into the
  amalgamation process.

  The counterpart of globalThis.sqlite3ApiBootstrap.initializers,
  specifically for initializers which are asynchronous. All entries in
  this list must be either async functions, non-async functions which
  return a Promise, or a Promise. Each function in the list is called
  with the sqlite3 object as its only argument.

  The resolved value of any Promise is ignored and rejection will kill
  the asyncPostInit() process (at an indeterminate point because all
  of them are run asynchronously in parallel).

  This list is not processed until the client calls
  sqlite3.asyncPostInit(). This means, for example, that intializers
  added to globalThis.sqlite3ApiBootstrap.initializers may push entries to
  this list.
*/
globalThis.sqlite3ApiBootstrap.initializersAsync = [];
/**
   Client code may assign sqlite3ApiBootstrap.defaultConfig an
   object-type value before calling sqlite3ApiBootstrap() (without
   arguments) in order to tell that call to use this object as its
   default config value. The intention of this is to provide
   downstream clients with a reasonably flexible approach for plugging in
   an environment-suitable configuration without having to define a new
   global-scope symbol.
*/
globalThis.sqlite3ApiBootstrap.defaultConfig = Object.create(null);
/**
   Placeholder: gets installed by the first call to
   globalThis.sqlite3ApiBootstrap(). However, it is recommended that the
   caller of sqlite3ApiBootstrap() capture its return value and delete
   globalThis.sqlite3ApiBootstrap after calling it. It returns the same
   value which will be stored here.
*/
globalThis.sqlite3ApiBootstrap.sqlite3 = undefined;
