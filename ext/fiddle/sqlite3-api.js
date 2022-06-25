/*
  2022-05-22

  The author disclaims copyright to this source code.  In place of a
  legal notice, here is a blessing:

  *   May you do good and not evil.
  *   May you find forgiveness for yourself and forgive others.
  *   May you share freely, never taking more than you give.

  ***********************************************************************

  This file is intended to be appended to the emcc-generated
  sqlite3.js via emcc:

  emcc ... -sMODULARIZE -sEXPORT_NAME=sqlite3InitModule --post-js=THIS_FILE

  It is loaded by importing the emcc-generated sqlite3.js, then:

  sqlite3InitModule({module object}).then(
    function(theModule){
      theModule.sqlite3 == an object containing this file's
      deliverables:
      {
        api: bindings for much of the core sqlite3 APIs,
        SQLite3: high-level OO API wrapper
      }
   });

  It is up to the caller to provide a module object compatible with
  emcc, but it can be a plain empty object. The object passed to
  sqlite3InitModule() will get populated by the emscripten-generated
  bits and, in part, by the code from this file. Specifically, this file
  installs the `theModule.sqlite3` part shown above.

  The resulting sqlite3.api object wraps the standard sqlite3 C API in
  a way as close to its native form as JS allows for. The
  sqlite3.SQLite3 object provides a higher-level wrapper more
  appropriate for general client-side use in JS.

  Because using certain parts of the low-level API properly requires
  some degree of WASM-related magic, it is not recommended that that
  API be used as-is in client-level code. Rather, client code is
  encouraged use the higher-level OO API or write a custom wrapper on
  top of the lower-level API. In short, most of the C-style API is
  used in an intuitive manner from JS but any C-style APIs which take
  pointers-to-pointer arguments require WASM-specific interfaces
  installed by Emscripten-generated code. Those which take or return
  only integers, doubles, strings, or "plain" pointers to db or
  statement objects can be used in "as normal," noting that "pointers"
  in WASM are simply 32-bit integers.


  Specific goals of this project:

  - Except where noted in the non-goals, provide a more-or-less
    complete wrapper to the sqlite3 C API, insofar as WASM feature
    parity with C allows for. In fact, provide at least 3...

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
    much TODO.


  Specific non-goals of this project:

  - As WASM is a web-centric technology and UTF-8 is the King of
    Encodings in that realm, there are no currently plans to support
    the UTF16-related sqlite3 APIs. They would add a complication to
    the bindings for no appreciable benefit.

  - Supporting old or niche-market platforms. WASM is built for a
    modern web and requires modern platforms.


  Attribution:

  Though this code is not a direct copy/paste, much of the
  functionality in this file is strongly influenced by the
  corresponding features in sql.js:

  https://github.com/sql-js/sql.js

  sql.js was an essential stepping stone in this code's development as
  it demonstrated how to handle some of the WASM-related voodoo (like
  handling pointers-to-pointers and adding JS implementations of
  C-bound callback functions). These APIs have a considerably
  different shape than sql.js's, however.
*/
if(!Module.postRun) Module.postRun = [];
/* ^^^^ the name Module is, in this setup, scope-local in the generated
   file sqlite3.js, with which this file gets combined at build-time. */
Module.postRun.push(function(namespace/*the module object, the target for
                                        installed features*/){
    'use strict';
    /**
    */
    const SQM/*interal-use convenience alias*/ = namespace/*the sqlite module object */;

    /** Throws a new Error, the message of which is the concatenation
        all args with a space between each. */
    const toss = function(){
        throw new Error(Array.prototype.join.call(arguments, ' '));
    };
    
    /** Returns true if n is a 32-bit (signed) integer, else false. */
    const isInt32 = function(n){
        return !!(n===(n|0) && n<=2147483647 && n>=-2147483648);
    };

    /** Returns v if v appears to be a TypedArray, else false. */
    const isTypedArray = (v)=>{
        return (v && v.constructor && isInt32(v.constructor.BYTES_PER_ELEMENT)) ? v : false;
    };
    
    /**
       Returns true if v appears to be one of our bind()-able
       TypedArray types: Uint8Array or Int8Array. Support for
       TypedArrays with element sizes >1 is a potential TODO.
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
    const isSQLableTypedArray = isBindableTypedArray;

    /** Returns true if isBindableTypedArray(v) does, else throws with a message
        that v is not a supported TypedArray value. */
    const affirmBindableTypedArray = (v)=>{
        return isBindableTypedArray(v)
            || toss("Value is not of a supported TypedArray type.");
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
    const api = {
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
           The sqlite3_prepare_v2() binding handles two different uses
           with differing JS/WASM semantics:

           1) sqlite3_prepare_v2(pDb, sqlString, -1, ppStmt [, null])

           2) sqlite3_prepare_v2(pDb, sqlPointer, -1, ppStmt, sqlPointerToPointer)

           Note that the SQL length argument (the 3rd argument) must
           always be negative because it must be a byte length and
           that value is expensive to calculate from JS (where only
           the character length of strings is readily available). It
           is retained in this API's interface for code/documentation
           compatibility reasons but is currently _always_
           ignored. When using the 2nd form of this call, it is
           critical that the custom-allocated string be terminated
           with a 0 byte. (Potential TODO: if the 3rd argument is >0,
           assume the caller knows precisely what they're doing, vis a
           vis WASM memory management, and pass it on as-is. That
           approach currently seems fraught with peril.)

           In usage (1), the 2nd argument must be of type string,
           Uint8Array, or Int8Array (either of which is assumed to
           hold SQL). If it is, this function assumes case (1) and
           calls the underyling C function with:

           (pDb, sqlAsString, -1, ppStmt, null)

           The pzTail argument is ignored in this case because its result
           is meaningless when a string-type value is passed through
           (because the string goes through another level of internal
           conversion for WASM's sake and the result pointer would refer
           to that conversion's memory, not the passed-in string).

           If sql is not a string or supported TypedArray, it must be
           a _pointer_ to a string which was allocated via
           api.wasm.allocateUTF8OnStack(), api.wasm._malloc(), or
           equivalent. In that case, the final argument may be
           0/null/undefined or must be a pointer to which the "tail"
           of the compiled SQL is written, as documented for the
           C-side sqlite3_prepare_v2(). In case (2), the underlying C
           function is called with:

           (pDb, sqlAsPointer, -1, ppStmt, pzTail)

           It returns its result and compiled statement as documented
           in the C API. Fetching the output pointers (4th and 5th
           parameters) requires using api.wasm.getValue() and the
           pzTail will point to an address relative to the
           sqlAsPointer value.

           If passed an invalid 2nd argument type, this function will
           throw. That behavior is in strong constrast to all of the
           other C-bound functions (which return a non-0 result code
           on error) but is necessary because we have to way to set
           the db's error state such that this function could return a
           non-0 integer and the client could call sqlite3_errcode()
           or sqlite3_errmsg() to fetch it.
        */
        sqlite3_prepare_v2: function(dbPtr, sql, sqlByteLen, stmtPtrPtr, strPtrPtr){}/*installed later*/,

        /**
           Holds state which are specific to the WASM-related
           infrastructure and glue code. It is not expected that client
           code will normally need these, but they're exposed here in case it
           does.

           Note that a number of members of this object are injected
           dynamically after the api object is fully constructed, so
           not all are documented inline here.
        */
        wasm: {
            /**
               api.wasm._malloc()'s srcTypedArray.byteLength bytes,
               populates them with the values from the source
               TypedArray, and returns the pointer to that memory. The
               returned pointer must eventually be passed to
               api.wasm._free() to clean it up.

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
            mallocFromTypedArray: function(srcTypedArray){
                affirmBindableTypedArray(srcTypedArray);
                const pRet = api.wasm._malloc(srcTypedArray.byteLength || 1);
                this.heapForSize(srcTypedArray).set(srcTypedArray.byteLength ? srcTypedArray : [0], pRet);
                return pRet;
            },
            /** Convenience form of this.heapForSize(8,false). */
            HEAP8: ()=>SQM['HEAP8'],
            /** Convenience form of this.heapForSize(8,true). */
            HEAPU8: ()=>SQM['HEAPU8'],
            /**
               Requires n to be one of (8, 16, 32) or a TypedArray
               instance of Int8Array, Int16Array, Int32Array, or their
               Uint counterparts.

               Returns the current integer-based TypedArray view of
               the WASM heap memory buffer associated with the given
               block size. If unsigned is truthy then the "U"
               (unsigned) variant of that view is returned, else the
               signed variant is returned. If passed a TypedArray
               value and no 2nd argument then the 2nd argument
               defaults to the signedness of that array. Note that
               Float32Array and Float64Array views are not supported
               by this function.

               Note that growth of the heap will invalidate any
               references to this heap, so do not hold a reference
               longer than needed and do not use a reference
               after any operation which may allocate.

               Throws if passed an invalid n
            */
            heapForSize: function(n,unsigned = true){
                if(isTypedArray(n)){
                    if(1===arguments.length){
                        unsigned = n instanceof Uint8Array || n instanceof Uint16Array
                            || n instanceof Uint32Array;
                    }
                    n = n.constructor.BYTES_PER_ELEMENT * 8;
                }
                switch(n){
                    case 8:  return SQM[unsigned ? 'HEAPU8'  : 'HEAP8'];
                    case 16: return SQM[unsigned ? 'HEAPU16' : 'HEAP16'];
                    case 32: return SQM[unsigned ? 'HEAPU32' : 'HEAP32'];
                }
                toss("Invalid heapForSize() size: expecting 8, 16, or 32.");
            }
        }
    };
    [/* C-side functions to bind. Each entry is an array with 3 elements:
        
        ["c-side name",
         "result type" (cwrap() syntax),
         [arg types in cwrap() syntax]
        ]
     */
        ["sqlite3_bind_blob","number",["number", "number", "number", "number", "number"]],
        ["sqlite3_bind_double","number",["number", "number", "number"]],
        ["sqlite3_bind_int","number",["number", "number", "number"]],
        /*Noting that JS/wasm combo does not currently support 64-bit integers:
          ["sqlite3_bind_int64","number",["number", "number", "number"]],*/
        ["sqlite3_bind_null","void",["number"]],
        ["sqlite3_bind_parameter_count", "number", ["number"]],
        ["sqlite3_bind_parameter_index","number",["number", "string"]],
        ["sqlite3_bind_text","number",["number", "number", "number", "number", "number"]],
        ["sqlite3_changes", "number", ["number"]],
        ["sqlite3_clear_bindings","number",["number"]],
        ["sqlite3_close_v2", "number", ["number"]],
        ["sqlite3_column_blob","number", ["number", "number"]],
        ["sqlite3_column_bytes","number",["number", "number"]],
        ["sqlite3_column_count", "number", ["number"]],
        ["sqlite3_column_count","number",["number"]],
        ["sqlite3_column_double","number",["number", "number"]],
        ["sqlite3_column_int","number",["number", "number"]],
        /*Noting that JS/wasm combo does not currently support 64-bit integers:
          ["sqlite3_column_int64","number",["number", "number"]],*/
        ["sqlite3_column_name","string",["number", "number"]],
        ["sqlite3_column_text","string",["number", "number"]],
        ["sqlite3_column_type","number",["number", "number"]],
        ["sqlite3_compileoption_get", "string", ["number"]],
        ["sqlite3_compileoption_used", "number", ["string"]],
        ["sqlite3_create_function_v2", "number",
         ["number", "string", "number", "number","number",
          "number", "number", "number", "number"]],
        ["sqlite3_data_count", "number", ["number"]],
        ["sqlite3_db_filename", "string", ["number", "string"]],
        ["sqlite3_errmsg", "string", ["number"]],
        ["sqlite3_exec", "number", ["number", "string", "number", "number", "number"]],
        ["sqlite3_extended_result_codes", "number", ["number", "number"]],
        ["sqlite3_finalize", "number", ["number"]],
        ["sqlite3_interrupt", "void", ["number"]],
        ["sqlite3_libversion", "string", []],
        ["sqlite3_open", "number", ["string", "number"]],
        ["sqlite3_open_v2", "number", ["string", "number", "number", "string"]],
        /* sqlite3_prepare_v2() is handled separately due to us requiring two
           different sets of semantics for that function. */
        ["sqlite3_reset", "number", ["number"]],
        ["sqlite3_result_blob",null,["number", "number", "number", "number"]],
        ["sqlite3_result_double",null,["number", "number"]],
        ["sqlite3_result_error",null,["number", "string", "number"]],
        ["sqlite3_result_int",null,["number", "number"]],
        ["sqlite3_result_null",null,["number"]],
        ["sqlite3_result_text",null,["number", "string", "number", "number"]],
        ["sqlite3_sourceid", "string", []],
        ["sqlite3_sql", "string", ["number"]],
        ["sqlite3_step", "number", ["number"]],
        ["sqlite3_value_blob", "number", ["number"]],
        ["sqlite3_value_bytes","number",["number"]],
        ["sqlite3_value_double","number",["number"]],
        ["sqlite3_value_text", "string", ["number"]],
        ["sqlite3_value_type", "number", ["number"]]
        //["sqlite3_normalized_sql", "string", ["number"]]
    ].forEach((a)=>api[a[0]] = SQM.cwrap.apply(this, a));

    /**
       Proxies for variants of sqlite3_prepare_v2() which have
       differing JS/WASM binding semantics.
    */
    const prepareMethods = {
        /**
           This binding expects a JS string as its 2nd argument and
           null as its final argument. In order to compile multiple
           statements from a single string, the "full" impl (see
           below) must be used.
        */
        basic: SQM.cwrap('sqlite3_prepare_v2',
                         "number", ["number", "string", "number"/*MUST always be negative*/,
                                    "number", "number"/*MUST be 0 or null or undefined!*/]),
         /* Impl which requires that the 2nd argument be a pointer to
            the SQL string, instead of being converted to a
            string. This variant is necessary for cases where we
            require a non-NULL value for the final argument
            (exec()'ing multiple statements from one input
            string). For simpler cases, where only the first statement
            in the SQL string is required, the wrapper named
            sqlite3_prepare_v2() is sufficient and easier to use
            because it doesn't require dealing with pointers.

            TODO: hide both of these methods behind a single hand-written 
            sqlite3_prepare_v2() wrapper which dispatches to the appropriate impl.
         */
        full: SQM.cwrap('sqlite3_prepare_v2',
                        "number", ["number", "number", "number"/*MUST always be negative*/,
                                   "number", "number"]),
    };

    /* Import C-level constants... */
    //console.log("wasmEnum=",SQM.ccall('sqlite3_wasm_enum_json', 'string', []));
    const wasmEnum = JSON.parse(SQM.ccall('sqlite3_wasm_enum_json', 'string', []));
    ['blobFinalizers', 'dataTypes','encodings',
     'openFlags', 'resultCodes','udfFlags'
    ].forEach(function(t){
        Object.keys(wasmEnum[t]).forEach(function(k){
            api[k] = wasmEnum[t][k];
        });
    });

    const utf8Decoder = new TextDecoder('utf-8');
    const typedArrayToString = (str)=>utf8Decoder.decode(str);
    //const stringToUint8 = (sql)=>new TextEncoder('utf-8').encode(sql);

    /* Documented inline in the api object. */
    api.sqlite3_prepare_v2 = function(pDb, sql, sqlLen, ppStmt, pzTail){
        if(isSQLableTypedArray(sql)) sql = typedArrayToString(sql);
        switch(typeof sql){
            case 'string': return prepareMethods.basic(pDb, sql, -1, ppStmt, null);
            case 'number': return prepareMethods.full(pDb, sql, -1, ppStmt, pzTail);
            default: toss("Invalid SQL argument type for sqlite3_prepare_v2().");
        }
    };

    /**
       Populate api.wasm with several members of the module object. Some of these
       will be required by higher-level code. At a minimum:

       getValue(), setValue(), stackSave(), stackRestore(), stackAlloc(), _malloc(),
       _free(), addFunction(), removeFunction()

       The rest are exposed primarily for internal use in this API but may well
       be useful from higher-level client code.

       All of the functions injected here are part of the
       Emscripten-exposed APIs and are documented "elsewhere". Some
       are documented in the Emscripten-generated `sqlite3.js` and
       some are documented (if at all) in places unknown, possibly
       even inaccessible, to us.
    */
    [
        // Memory management:
        'getValue','setValue', 'stackSave', 'stackRestore', 'stackAlloc',
        'allocateUTF8OnStack', '_malloc', '_free',
        // String utilities:
        'intArrayFromString', 'lengthBytesUTF8', 'stringToUTF8Array',
        // The obligatory "misc" category:
        'addFunction', 'removeFunction'
    ].forEach(function(m){
        if(undefined === (api.wasm[m] = SQM[m])){
            toss("Internal init error: Module."+m+" not found.");
        }
    });

    /* What follows is colloquially known as "OO API #1". It is a
       binding of the sqlite3 API which is designed to be run within
       the same thread (main or worker) as the one in which the
       sqlite3 WASM binding was initialized. This wrapper cannot use
       the sqlite3 binding if, e.g., the wrapper is in the main thread
       and the sqlite3 API is in a worker. */

    /**
       The DB class wraps a sqlite3 db handle.

       It accepts the following argument signatures:

       - ()
       - (undefined) (same effect as ())
       - (filename[,buffer])
       - (buffer)

       Where a buffer indicates a Uint8Array holding an sqlite3 db
       image.

       If the filename is provided, only the last component of the
       path is used - any path prefix is stripped and certain
       "special" characters are replaced with `_`. If no name is
       provided, a random name is generated. The resulting filename is
       the one used for accessing the db file within root directory of
       the emscripten-supplied virtual filesystem, and is set (with no
       path part) as the DB object's `filename` property.

       Note that the special sqlite3 db names ":memory:" and ""
       (temporary db) have no special meanings here. We can apparently
       only export images of DBs which are stored in the
       pseudo-filesystem provided by the JS APIs. Since exporting and
       importing images is an important usability feature for this
       class, ":memory:" DBs are not supported (until/unless we can
       find a way to export those as well). The naming semantics will
       certainly evolve as this API does.

       The db is opened with a fixed set of flags:
       (SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE |
       SQLITE_OPEN_EXRESCODE).  This API may change in the future
       permit the caller to provide those flags via an additional
       argument.
    */
    const DB = function(arg){
        let buffer, fn;
        if(arg instanceof Uint8Array){
            buffer = arg;
            arg = undefined;
        }else if(arguments.length){ /*(filename[,buffer])*/
            if('string'===typeof arg){
                const p = arg.split('/').pop().replace(':','_');
                if(p) fn = p;
                if(arguments.length>1){
                    buffer = arguments[1];
                }
            }else if(undefined!==arg){
                toss("Invalid arguments to DB constructor.",
                     "Expecting (), (undefined), (name,buffer),",
                     "or (buffer), where buffer an sqlite3 db ",
                     "as a Uint8Array.");
            }
        }
        if(!fn){
            fn = "db-"+((Math.random() * 10000000) | 0)+
                "-"+((Math.random() * 10000000) | 0)+".sqlite3";
        }
        if(buffer){
            if(!(buffer instanceof Uint8Array)){
                toss("Expecting Uint8Array image of db contents.");
            }
            FS.createDataFile("/", fn, buffer, true, true);
        }
        const stack = api.wasm.stackSave();
        const ppDb  = api.wasm.stackAlloc(4) /* output (sqlite3**) arg */;
        api.wasm.setValue(ppDb, 0, "i32");
        try {
            this.checkRc(api.sqlite3_open_v2(fn, ppDb, api.SQLITE_OPEN_READWRITE
                                             | api.SQLITE_OPEN_CREATE
                                             | api.SQLITE_OPEN_EXRESCODE, null));
        }
        finally{api.wasm.stackRestore(stack);}
        this._pDb = api.wasm.getValue(ppDb, "i32");
        this.filename = fn;
        this._statements = {/*map of open Stmt _pointers_ to Stmt*/};
        this._udfs = {/*map of UDF names to wasm function _pointers_*/};
    };

    /**
       Internal-use enum for mapping JS types to DB-bindable types.
       These do not (and need not) line up with the SQLITE_type
       values. All values in this enum must be truthy and distinct
       but they need not be numbers.
    */
    const BindTypes = {
        null: 1,
        number: 2,
        string: 3,
        boolean: 4,
        blob: 5
    };
    BindTypes['undefined'] == BindTypes.null;

    /**
       This class wraps sqlite3_stmt. Calling this constructor
       directly will trigger an exception. Use DB.prepare() to create
       new instances.
    */
    const Stmt = function(){
        if(BindTypes!==arguments[2]){
            toss("Do not call the Stmt constructor directly. Use DB.prepare().");
        }
        this.db = arguments[0];
        this._pStmt = arguments[1];
        this.columnCount = api.sqlite3_column_count(this._pStmt);
        this.parameterCount = api.sqlite3_bind_parameter_count(this._pStmt);
    };

    /** Throws if the given DB has been closed, else it is returned. */
    const affirmDbOpen = function(db){
        if(!db._pDb) toss("DB has been closed.");
        return db;
    };

    /**
       Expects to be passed (arguments) from DB.exec() and
       DB.execMulti(). Does the argument processing/validation, throws
       on error, and returns a new object on success:

       { sql: the SQL, opt: optionsObj, cbArg: function}

       cbArg is only set if the opt.callback is set, in which case
       it's a function which expects to be passed the current Stmt
       and returns the callback argument of the type indicated by
       the input arguments.
    */
    const parseExecArgs = function(args){
        const out = {opt:{}};
        switch(args.length){
            case 1:
                if('string'===typeof args[0] || isSQLableTypedArray(args[0])){
                    out.sql = args[0];
                }else if(args[0] && 'object'===typeof args[0]){
                    out.opt = args[0];
                    out.sql = out.opt.sql;
                }
            break;
            case 2:
                out.sql = args[0];
                out.opt = args[1];
            break;
            default: toss("Invalid argument count for exec().");
        };
        if(isSQLableTypedArray(out.sql)){
            out.sql = typedArrayToString(out.sql);
        }else if(Array.isArray(out.sql)){
            out.sql = out.sql.join('');
        }else if('string'!==typeof out.sql){
            toss("Missing SQL argument.");
        }
        if(out.opt.callback || out.opt.resultRows){
            switch((undefined===out.opt.rowMode)
                   ? 'stmt' : out.opt.rowMode) {
                case 'object': out.cbArg = (stmt)=>stmt.get({}); break;
                case 'array': out.cbArg = (stmt)=>stmt.get([]); break;
                case 'stmt': out.cbArg = (stmt)=>stmt; break;
                default: toss("Invalid rowMode:",out.opt.rowMode);
            }
        }
        return out;
    };

    /** If object opts has _its own_ property named p then that
        property's value is returned, else dflt is returned. */
    const getOwnOption = (opts, p, dflt)=>
        opts.hasOwnProperty(p) ? opts[p] : dflt;

    DB.prototype = {
        /**
           Expects to be given an sqlite3 API result code. If it is
           falsy, this function returns this object, else it throws an
           exception with an error message from sqlite3_errmsg(),
           using this object's db handle. Note that if it's passed a
           non-error code like SQLITE_ROW or SQLITE_DONE, it will
           still throw but the error string might be "Not an error."
           The various non-0 non-error codes need to be checked for in
           client code where they are expected.
        */
        checkRc: function(sqliteResultCode){
            if(!sqliteResultCode) return this;
            toss("sqlite result code",sqliteResultCode+":",
                 api.sqlite3_errmsg(this._pDb) || "Unknown db error.");
        },
        /**
           Finalizes all open statements and closes this database
           connection. This is a no-op if the db has already been
           closed. If the db is open and alsoUnlink is truthy then the
           this.filename entry in the pseudo-filesystem will also be
           removed (and any error in that attempt is silently
           ignored).
        */
        close: function(alsoUnlink){
            if(this._pDb){
                let s;
                const that = this;
                Object.keys(this._statements).forEach(function(k,s){
                    delete that._statements[k];
                    if(s && s._pStmt) s.finalize();
                });
                Object.values(this._udfs).forEach(api.wasm.removeFunction);
                delete this._udfs;
                delete this._statements;
                api.sqlite3_close_v2(this._pDb);
                delete this._pDb;
                if(this.filename){
                    if(alsoUnlink){
                        try{SQM.FS.unlink('/'+this.filename);}
                        catch(e){/*ignored*/}
                    }
                    delete this.filename;
                }
            }
        },
        /**
           Similar to this.filename but will return NULL for
           special names like ":memory:". Not of much use until
           we have filesystem support. Throws if the DB has
           been closed. If passed an argument it then it will return
           the filename of the ATTACHEd db with that name, else it assumes
           a name of `main`.
        */
        fileName: function(dbName){
            return api.sqlite3_db_filename(affirmDbOpen(this)._pDb, dbName||"main");
        },
        /**
           Compiles the given SQL and returns a prepared Stmt. This is
           the only way to create new Stmt objects. Throws on error.

           The given SQL must be a string, a Uint8Array holding SQL,
           or a WASM pointer to memory allocated using
           api.wasm.allocateUTF8OnStack() (or equivalent (a term which
           is yet to be defined precisely)).
        */
        prepare: function(sql){
            affirmDbOpen(this);
            const stack = api.wasm.stackSave();
            const ppStmt  = api.wasm.stackAlloc(4)/* output (sqlite3_stmt**) arg */;
            api.wasm.setValue(ppStmt, 0, "i32");
            try {this.checkRc(api.sqlite3_prepare_v2(this._pDb, sql, -1, ppStmt, null));}
            finally {api.wasm.stackRestore(stack);}
            const pStmt = api.wasm.getValue(ppStmt, "i32");
            if(!pStmt) toss("Cannot prepare empty SQL.");
            const stmt = new Stmt(this, pStmt, BindTypes);
            this._statements[pStmt] = stmt;
            return stmt;
        },
        /**
           This function works like execMulti(), and takes most of the
           same arguments, but is more efficient (performs much less
           work) when the input SQL is only a single statement. If
           passed a multi-statement SQL, it only processes the first
           one.

           This function supports the following additional options not
           supported by execMulti():

           - .multi: if true, this function acts as a proxy for
             execMulti() and behaves identically to that function.

           - .resultRows: if this is an array, each row of the result
             set (if any) is appended to it in the format specified
             for the `rowMode` property, with the exception that the
             only legal values for `rowMode` in this case are 'array'
             or 'object', neither of which is the default. It is legal
             to use both `resultRows` and `callback`, but `resultRows`
             is likely much simpler to use for small data sets and can
             be used over a WebWorker-style message interface.

           - .columnNames: if this is an array and the query has
             result columns, the array is passed to
             Stmt.getColumnNames() to append the column names to it
             (regardless of whether the query produces any result
             rows). If the query has no result columns, this value is
             unchanged.

           The following options to execMulti() are _not_ supported by
           this method (they are simply ignored):

          - .saveSql
        */
        exec: function(/*(sql [,optionsObj]) or (optionsObj)*/){
            affirmDbOpen(this);
            const arg = parseExecArgs(arguments);
            if(!arg.sql) return this;
            else if(arg.opt.multi){
                return this.execMulti(arg, undefined, BindTypes);
            }
            const opt = arg.opt;
            let stmt, rowTarget;
            try {
                if(Array.isArray(opt.resultRows)){
                    if(opt.rowMode!=='array' && opt.rowMode!=='object'){
                        toss("Invalid rowMode for resultRows array: must",
                             "be one of 'array' or 'object'.");
                    }
                    rowTarget = opt.resultRows;
                }
                stmt = this.prepare(arg.sql);
                if(stmt.columnCount && Array.isArray(opt.columnNames)){
                    stmt.getColumnNames(opt.columnNames);
                }
                if(opt.bind) stmt.bind(opt.bind);
                if(opt.callback || rowTarget){
                    while(stmt.step()){
                        const row = arg.cbArg(stmt);
                        if(rowTarget) rowTarget.push(row);
                        if(opt.callback){
                            stmt._isLocked = true;
                            opt.callback(row, stmt);
                            stmt._isLocked = false;
                        }
                    }
                }else{
                    stmt.step();
                }
            }finally{
                if(stmt){
                    delete stmt._isLocked;
                    stmt.finalize();
                }
            }
            return this;

        }/*exec()*/,
        /**
           Executes one or more SQL statements in the form of a single
           string. Its arguments must be either (sql,optionsObject) or
           (optionsObject). In the latter case, optionsObject.sql
           must contain the SQL to execute. Returns this
           object. Throws on error.

           If no SQL is provided, or a non-string is provided, an
           exception is triggered. Empty SQL, on the other hand, is
           simply a no-op.

           The optional options object may contain any of the following
           properties:

           - .sql = the SQL to run (unless it's provided as the first
             argument). This must be of type string, Uint8Array, or an
             array of strings (in which case they're concatenated
             together as-is, with no separator between elements,
             before evaluation).

           - .bind = a single value valid as an argument for
             Stmt.bind(). This is ONLY applied to the FIRST non-empty
             statement in the SQL which has any bindable
             parameters. (Empty statements are skipped entirely.)

           - .callback = a function which gets called for each row of
             the FIRST statement in the SQL which has result
             _columns_, but only if that statement has any result
             _rows_. The second argument passed to the callback is
             always the current Stmt object (so that the caller may
             collect column names, or similar). The first argument
             passed to the callback defaults to the current Stmt
             object but may be changed with ...

           - .rowMode = a string describing what type of argument
             should be passed as the first argument to the callback. A
             value of 'object' causes the results of `stmt.get({})` to
             be passed to the object. A value of 'array' causes the
             results of `stmt.get([])` to be passed to the callback.
             A value of 'stmt' is equivalent to the default, passing
             the current Stmt to the callback (noting that it's always
             passed as the 2nd argument). Any other value triggers an
             exception.

           - saveSql = an optional array. If set, the SQL of each
             executed statement is appended to this array before the
             statement is executed (but after it is prepared - we
             don't have the string until after that). Empty SQL
             statements are elided.

           See also the exec() method, which is a close cousin of this
           one.

           ACHTUNG #1: The callback MUST NOT modify the Stmt
           object. Calling any of the Stmt.get() variants,
           Stmt.getColumnName(), or similar, is legal, but calling
           step() or finalize() is not. Routines which are illegal
           in this context will trigger an exception.

           ACHTUNG #2: The semantics of the `bind` and `callback`
           options may well change or those options may be removed
           altogether for this function (but retained for exec()).
           Generally speaking, neither bind parameters nor a callback
           are generically useful when executing multi-statement SQL.
        */
        execMulti: function(/*(sql [,obj]) || (obj)*/){
            affirmDbOpen(this);
            const arg = (BindTypes===arguments[2]
                         /* ^^^ Being passed on from exec() */
                         ? arguments[0] : parseExecArgs(arguments));
            if(!arg.sql) return this;
            const opt = arg.opt;
            const stack = api.wasm.stackSave();
            let stmt;
            let bind = opt.bind;
            let rowMode = (
                (opt.callback && opt.rowMode)
                    ? opt.rowMode : false);
            try{
                const sql = isSQLableTypedArray(arg.sql)
                      ? typedArrayToString(arg.sql)
                      : arg.sql;
                let pSql = api.wasm.allocateUTF8OnStack(sql)
                const ppStmt  = api.wasm.stackAlloc(8) /* output (sqlite3_stmt**) arg */;
                const pzTail = ppStmt + 4 /* final arg to sqlite3_prepare_v2_sqlptr() */;
                while(api.wasm.getValue(pSql, "i8")){
                    api.wasm.setValue(ppStmt, 0, "i32");
                    api.wasm.setValue(pzTail, 0, "i32");
                    this.checkRc(api.sqlite3_prepare_v2(
                        this._pDb, pSql, -1, ppStmt, pzTail
                    ));
                    const pStmt = api.wasm.getValue(ppStmt, "i32");
                    pSql = api.wasm.getValue(pzTail, "i32");
                    if(!pStmt) continue;
                    if(opt.saveSql){
                        opt.saveSql.push(api.sqlite3_sql(pStmt).trim());
                    }
                    stmt = new Stmt(this, pStmt, BindTypes);
                    if(bind && stmt.parameterCount){
                        stmt.bind(bind);
                        bind = null;
                    }
                    if(opt.callback && null!==rowMode && stmt.columnCount){
                        while(stmt.step()){
                            stmt._isLocked = true;
                            callback(arg.cbArg(stmt), stmt);
                            stmt._isLocked = false;
                        }
                        rowMode = null;
                    }else{
                        // Do we need to while(stmt.step()){} here?
                        stmt.step();
                    }
                    stmt.finalize();
                    stmt = null;
                }
            }finally{
                if(stmt){
                    delete stmt._isLocked;
                    stmt.finalize();
                }
                api.wasm.stackRestore(stack);
            }
            return this;
        }/*execMulti()*/,
        /**
           Creates a new scalar UDF (User-Defined Function) which is
           accessible via SQL code. This function may be called in any
           of the following forms:

           - (name, function)
           - (name, function, optionsObject)
           - (name, optionsObject)
           - (optionsObject)

           In the final two cases, the function must be defined as the
           'callback' property of the options object. In the final
           case, the function's name must be the 'name' property.

           This can only be used to create scalar functions, not
           aggregate or window functions. UDFs cannot be removed from
           a DB handle after they're added.

           On success, returns this object. Throws on error.

           When called from SQL, arguments to the UDF, and its result,
           will be converted between JS and SQL with as much fidelity
           as is feasible, triggering an exception if a type
           conversion cannot be determined. Some freedom is afforded
           to numeric conversions due to friction between the JS and C
           worlds: integers which are larger than 32 bits will be
           treated as doubles, as JS does not support 64-bit integers
           and it is (as of this writing) illegal to use WASM
           functions which take or return 64-bit integers from JS.

           The optional options object may contain flags to modify how
           the function is defined:

           - .arity: the number of arguments which SQL calls to this
             function expect or require. The default value is the
             callback's length property (i.e. the number of declared
             parameters it has). A value of -1 means that the function
             is variadic and may accept any number of arguments, up to
             sqlite3's compile-time limits. sqlite3 will enforce the
             argument count if is zero or greater.

           The following properties correspond to flags documented at:

           https://sqlite.org/c3ref/create_function.html

           - .deterministic = SQLITE_DETERMINISTIC
           - .directOnly = SQLITE_DIRECTONLY
           - .innocuous = SQLITE_INNOCUOUS

           Maintenance reminder: the ability to add new
           WASM-accessible functions to the runtime requires that the
           WASM build is compiled with emcc's `-sALLOW_TABLE_GROWTH`
           flag.
        */
        createFunction: function f(name, callback,opt){
            switch(arguments.length){
                case 1: /* (optionsObject) */
                    opt = name;
                    name = opt.name;
                    callback = opt.callback;
                    break;
                case 2: /* (name, callback|optionsObject) */
                    if(!(callback instanceof Function)){
                        opt = callback;
                        callback = opt.callback;
                    }
                    break;
                default: break;
            }
            if(!opt) opt = {};
            if(!(callback instanceof Function)){
                toss("Invalid arguments: expecting a callback function.");
            }else if('string' !== typeof name){
                toss("Invalid arguments: missing function name.");
            }
            if(!f._extractArgs){
                /* Static init */
                f._extractArgs = function(argc, pArgv){
                    let i, pVal, valType, arg;
                    const tgt = [];
                    for(i = 0; i < argc; ++i){
                        pVal = api.wasm.getValue(pArgv + (4 * i), "i32");
                        valType = api.sqlite3_value_type(pVal);
                        switch(valType){
                            case api.SQLITE_INTEGER:
                            case api.SQLITE_FLOAT:
                                arg = api.sqlite3_value_double(pVal);
                                break;
                            case api.SQLITE_TEXT:
                                arg = api.sqlite3_value_text(pVal);
                                break;
                            case api.SQLITE_BLOB:{
                                const n = api.sqlite3_value_bytes(pVal);
                                const pBlob = api.sqlite3_value_blob(pVal);
                                arg = new Uint8Array(n);
                                let i;
                                const heap = n ? api.wasm.HEAP8() : false;
                                for(i = 0; i < n; ++i) arg[i] = heap[pBlob+i];
                                break;
                            }
                            default:
                                arg = null; break;
                        }
                        tgt.push(arg);
                    }
                    return tgt;
                }/*_extractArgs()*/;
                f._setResult = function(pCx, val){
                    switch(typeof val) {
                        case 'boolean':
                            api.sqlite3_result_int(pCx, val ? 1 : 0);
                            break;
                        case 'number': {
                            (isInt32(val)
                             ? api.sqlite3_result_int
                             : api.sqlite3_result_double)(pCx, val);
                            break;
                        }
                        case 'string':
                            api.sqlite3_result_text(pCx, val, -1, api.SQLITE_TRANSIENT);
                            break;
                        case 'object':
                            if(null===val) {
                                api.sqlite3_result_null(pCx);
                                break;
                            }else if(isBindableTypedArray(val)){
                                const pBlob = api.wasm.mallocFromTypedArray(val);
                                api.sqlite3_result_blob(pCx, pBlob, val.byteLength,
                                                        api.SQLITE_TRANSIENT);
                                api.wasm._free(pBlob);
                                break;
                            }
                            // else fall through
                        default:
                            toss("Don't not how to handle this UDF result value:",val);
                    };
                }/*_setResult()*/;
            }/*static init*/
            const wrapper = function(pCx, argc, pArgv){
                try{
                    f._setResult(pCx, callback.apply(null, f._extractArgs(argc, pArgv)));
                }catch(e){
                    api.sqlite3_result_error(pCx, e.message, -1);
                }
            };
            const pUdf = api.wasm.addFunction(wrapper, "viii");
            let fFlags = 0;
            if(getOwnOption(opt, 'deterministic')) fFlags |= api.SQLITE_DETERMINISTIC;
            if(getOwnOption(opt, 'directOnly')) fFlags |= api.SQLITE_DIRECTONLY;
            if(getOwnOption(opt, 'innocuous')) fFlags |= api.SQLITE_INNOCUOUS;
            name = name.toLowerCase();
            try {
                this.checkRc(api.sqlite3_create_function_v2(
                    this._pDb, name,
                    (opt.hasOwnProperty('arity') ? +opt.arity : callback.length),
                    api.SQLITE_UTF8 | fFlags, null/*pApp*/, pUdf,
                    null/*xStep*/, null/*xFinal*/, null/*xDestroy*/));
            }catch(e){
                api.wasm.removeFunction(pUdf);
                throw e;
            }
            if(this._udfs.hasOwnProperty(name)){
                api.wasm.removeFunction(this._udfs[name]);
            }
            this._udfs[name] = pUdf;
            return this;
        }/*createFunction()*/,
        /**
           Prepares the given SQL, step()s it one time, and returns
           the value of the first result column. If it has no results,
           undefined is returned. If passed a second argument, it is
           treated like an argument to Stmt.bind(), so may be any type
           supported by that function. Throws on error (e.g. malformed
           SQL).
        */
        selectValue: function(sql,bind){
            let stmt, rc;
            try {
                stmt = this.prepare(sql).bind(bind);
                if(stmt.step()) rc = stmt.get(0);
            }finally{
                if(stmt) stmt.finalize();
            }
            return rc;
        },

        /**
           Exports a copy of this db's file as a Uint8Array and
           returns it. It is technically not legal to call this while
           any prepared statement are currently active because,
           depending on the platform, it might not be legal to read
           the db while a statement is locking it. Throws if this db
           is not open or has any opened statements.

           The resulting buffer can be passed to this class's
           constructor to restore the DB.

           Maintenance reminder: the corresponding sql.js impl of this
           feature closes the current db, finalizing any active
           statements and (seemingly unnecessarily) destroys any UDFs,
           copies the file, and then re-opens it (without restoring
           the UDFs). Those gymnastics are not necessary on the tested
           platform but might be necessary on others. Because of that
           eventuality, this interface currently enforces that no
           statements are active when this is run. It will throw if
           any are.
        */
        exportBinaryImage: function(){
            affirmDbOpen(this);
            if(Object.keys(this._statements).length){
                toss("Cannot export with prepared statements active!",
                     "finalize() all statements and try again.");
            }
            return FS.readFile(this.filename, {encoding:"binary"});
        }
    }/*DB.prototype*/;


    /** Throws if the given Stmt has been finalized, else stmt is
        returned. */
    const affirmStmtOpen = function(stmt){
        if(!stmt._pStmt) toss("Stmt has been closed.");
        return stmt;
    };

    /** Returns an opaque truthy value from the BindTypes
        enum if v's type is a valid bindable type, else
        returns a falsy value. As a special case, a value of
        undefined is treated as a bind type of null. */
    const isSupportedBindType = function(v){
        let t = BindTypes[(null===v||undefined===v) ? 'null' : typeof v];
        switch(t){
            case BindTypes.boolean:
            case BindTypes.null:
            case BindTypes.number:
            case BindTypes.string:
                return t;
            default:
                //console.log("isSupportedBindType",t,v);
                return isBindableTypedArray(v) ? BindTypes.blob : undefined;
        }
    };

    /**
       If isSupportedBindType(v) returns a truthy value, this
       function returns that value, else it throws.
    */
    const affirmSupportedBindType = function(v){
        //console.log('affirmSupportedBindType',v);
        return isSupportedBindType(v) || toss("Unsupported bind() argument type:",typeof v);
    };

    /**
       If key is a number and within range of stmt's bound parameter
       count, key is returned.

       If key is not a number then it is checked against named
       parameters. If a match is found, its index is returned.

       Else it throws.
    */
    const affirmParamIndex = function(stmt,key){
        const n = ('number'===typeof key)
              ? key : api.sqlite3_bind_parameter_index(stmt._pStmt, key);
        if(0===n || !isInt32(n)){
            toss("Invalid bind() parameter name: "+key);
        }
        else if(n<1 || n>stmt.parameterCount) toss("Bind index",key,"is out of range.");
        return n;
    };

    /** Throws if ndx is not an integer or if it is out of range
        for stmt.columnCount, else returns stmt.

        Reminder: this will also fail after the statement is finalized
        but the resulting error will be about an out-of-bounds column
        index.
    */
    const affirmColIndex = function(stmt,ndx){
        if((ndx !== (ndx|0)) || ndx<0 || ndx>=stmt.columnCount){
            toss("Column index",ndx,"is out of range.");
        }
        return stmt;
    };

    /**
       If stmt._isLocked is truthy, this throws an exception
       complaining that the 2nd argument (an operation name,
       e.g. "bind()") is not legal while the statement is "locked".
       Locking happens before an exec()-like callback is passed a
       statement, to ensure that the callback does not mutate or
       finalize the statement. If it does not throw, it returns stmt.
    */
    const affirmUnlocked = function(stmt,currentOpName){
        if(stmt._isLocked){
            toss("Operation is illegal when statement is locked:",currentOpName);
        }
        return stmt;
    };

    /**
       Binds a single bound parameter value on the given stmt at the
       given index (numeric or named) using the given bindType (see
       the BindTypes enum) and value. Throws on error. Returns stmt on
       success.
    */
    const bindOne = function f(stmt,ndx,bindType,val){
        affirmUnlocked(stmt, 'bind()');
        if(!f._){
            f._ = {
                string: function(stmt, ndx, val, asBlob){
                    if(1){
                        /* _Hypothetically_ more efficient than the impl in the 'else' block. */
                        const stack = api.wasm.stackSave();
                        try{
                            const n = api.wasm.lengthBytesUTF8(val)+1/*required for NUL terminator*/;
                            const pStr = api.wasm.stackAlloc(n);
                            api.wasm.stringToUTF8Array(val, api.wasm.HEAPU8(), pStr, n);
                            const f = asBlob ? api.sqlite3_bind_blob : api.sqlite3_bind_text;
                            return f(stmt._pStmt, ndx, pStr, n-1, api.SQLITE_TRANSIENT);
                        }finally{
                            api.wasm.stackRestore(stack);
                        }
                    }else{
                        const bytes = api.wasm.intArrayFromString(val,true);
                        const pStr = api.wasm._malloc(bytes.length || 1);
                        api.wasm.HEAPU8().set(bytes.length ? bytes : [0], pStr);
                        try{
                            const f = asBlob ? api.sqlite3_bind_blob : api.sqlite3_bind_text;
                            return f(stmt._pStmt, ndx, pStr, bytes.length, api.SQLITE_TRANSIENT);
                        }finally{
                            api.wasm._free(pStr);
                        }
                    }
                }
            };
        }
        affirmSupportedBindType(val);
        ndx = affirmParamIndex(stmt,ndx);
        let rc = 0;
        switch((null===val || undefined===val) ? BindTypes.null : bindType){
            case BindTypes.null:
                rc = api.sqlite3_bind_null(stmt._pStmt, ndx);
                break;
            case BindTypes.string:{
                rc = f._.string(stmt, ndx, val, false);
                break;
            }
            case BindTypes.number: {
                const m = (isInt32(val)
                           ? api.sqlite3_bind_int
                           /*It's illegal to bind a 64-bit int
                             from here*/
                           : api.sqlite3_bind_double);
                rc = m(stmt._pStmt, ndx, val);
                break;
            }
            case BindTypes.boolean:
                rc = api.sqlite3_bind_int(stmt._pStmt, ndx, val ? 1 : 0);
                break;
            case BindTypes.blob: {
                if('string'===typeof val){
                    rc = f._.string(stmt, ndx, val, true);
                }else if(!isBindableTypedArray(val)){
                    toss("Binding a value as a blob requires",
                         "that it be a string, Uint8Array, or Int8Array.");
                }else if(1){
                    /* _Hypothetically_ more efficient than the impl in the 'else' block. */
                    const stack = api.wasm.stackSave();
                    try{
                        const pBlob = api.wasm.stackAlloc(val.byteLength || 1);
                        api.wasm.HEAP8().set(val.byteLength ? val : [0], pBlob)
                        rc = api.sqlite3_bind_blob(stmt._pStmt, ndx, pBlob, val.byteLength,
                                                   api.SQLITE_TRANSIENT);
                    }finally{
                        api.wasm.stackRestore(stack);
                    }
                }else{
                    const pBlob = api.wasm.mallocFromTypedArray(val);
                    try{
                        rc = api.sqlite3_bind_blob(stmt._pStmt, ndx, pBlob, val.byteLength,
                                                   api.SQLITE_TRANSIENT);
                    }finally{
                        api.wasm._free(pBlob);
                    }
                }
                break;
            }
            default:
                console.warn("Unsupported bind() argument type:",val);
                toss("Unsupported bind() argument type.");
        }
        if(rc) stmt.db.checkRc(rc);
        return stmt;
    };
    
    Stmt.prototype = {
        /**
           "Finalizes" this statement. This is a no-op if the
           statement has already been finalizes. Returns
           undefined. Most methods in this class will throw if called
           after this is.
        */
        finalize: function(){
            if(this._pStmt){
                affirmUnlocked(this,'finalize()');
                delete this.db._statements[this._pStmt];
                api.sqlite3_finalize(this._pStmt);
                delete this.columnCount;
                delete this.parameterCount;
                delete this._pStmt;
                delete this.db;
                delete this._isLocked;
            }
        },
        /** Clears all bound values. Returns this object.
            Throws if this statement has been finalized. */
        clearBindings: function(){
            affirmUnlocked(affirmStmtOpen(this), 'clearBindings()')
            api.sqlite3_clear_bindings(this._pStmt);
            this._mayGet = false;
            return this;
        },
        /**
           Resets this statement so that it may be step()ed again
           from the beginning. Returns this object. Throws if this
           statement has been finalized.

           If passed a truthy argument then this.clearBindings() is
           also called, otherwise any existing bindings, along with
           any memory allocated for them, are retained.
        */
        reset: function(alsoClearBinds){
            affirmUnlocked(this,'reset()');
            if(alsoClearBinds) this.clearBindings();
            api.sqlite3_reset(affirmStmtOpen(this)._pStmt);
            this._mayGet = false;
            return this;
        },
        /**
           Binds one or more values to its bindable parameters. It
           accepts 1 or 2 arguments:

           If passed a single argument, it must be either an array, an
           object, or a value of a bindable type (see below).

           If passed 2 arguments, the first one is the 1-based bind
           index or bindable parameter name and the second one must be
           a value of a bindable type.

           Bindable value types:

           - null is bound as NULL.

           - undefined as a standalone value is a no-op intended to
             simplify certain client-side use cases: passing undefined
             as a value to this function will not actually bind
             anything and this function will skip confirmation that
             binding is even legal. (Those semantics simplify certain
             client-side uses.) Conversely, a value of undefined as an
             array or object property when binding an array/object
             (see below) is treated the same as null.

           - Numbers are bound as either doubles or integers: doubles
             if they are larger than 32 bits, else double or int32,
             depending on whether they have a fractional part. (It is,
             as of this writing, illegal to call (from JS) a WASM
             function which either takes or returns an int64.)
             Booleans are bound as integer 0 or 1. It is not expected
             the distinction of binding doubles which have no
             fractional parts is integers is significant for the
             majority of clients due to sqlite3's data typing
             model. This API does not currently support the BigInt
             type.

           - Strings are bound as strings (use bindAsBlob() to force
             blob binding).

           - Uint8Array and Int8Array instances are bound as blobs.
           (TODO: support binding other TypedArray types with larger
           int sizes.)

           If passed an array, each element of the array is bound at
           the parameter index equal to the array index plus 1
           (because arrays are 0-based but binding is 1-based).

           If passed an object, each object key is treated as a
           bindable parameter name. The object keys _must_ match any
           bindable parameter names, including any `$`, `@`, or `:`
           prefix. Because `$` is a legal identifier chararacter in
           JavaScript, that is the suggested prefix for bindable
           parameters: `stmt.bind({$a: 1, $b: 2})`.

           It returns this object on success and throws on
           error. Errors include:

           - Any bind index is out of range, a named bind parameter
             does not match, or this statement has no bindable
             parameters.

           - Any value to bind is of an unsupported type.

           - Passed no arguments or more than two.

           - The statement has been finalized.
        */
        bind: function(/*[ndx,] arg*/){
            affirmStmtOpen(this);
            let ndx, arg;
            switch(arguments.length){
                case 1: ndx = 1; arg = arguments[0]; break;
                case 2: ndx = arguments[0]; arg = arguments[1]; break;
                default: toss("Invalid bind() arguments.");
            }
            if(undefined===arg){
                /* It might seem intuitive to bind undefined as NULL
                   but this approach simplifies certain client-side
                   uses when passing on arguments between 2+ levels of
                   functions. */
                return this;
            }else if(!this.parameterCount){
                toss("This statement has no bindable parameters.");
            }
            this._mayGet = false;
            if(null===arg){
                /* bind NULL */
                return bindOne(this, ndx, BindTypes.null, arg);
            }
            else if(Array.isArray(arg)){
                /* bind each entry by index */
                if(1!==arguments.length){
                    toss("When binding an array, an index argument is not permitted.");
                }
                arg.forEach((v,i)=>bindOne(this, i+1, affirmSupportedBindType(v), v));
                return this;
            }
            else if('object'===typeof arg/*null was checked above*/
                    && !isBindableTypedArray(arg)){
                /* Treat each property of arg as a named bound parameter. */
                if(1!==arguments.length){
                    toss("When binding an object, an index argument is not permitted.");
                }
                Object.keys(arg)
                    .forEach(k=>bindOne(this, k,
                                        affirmSupportedBindType(arg[k]),
                                        arg[k]));
                return this;
            }else{
                return bindOne(this, ndx, affirmSupportedBindType(arg), arg);
            }
            toss("Should not reach this point.");
        },
        /**
           Special case of bind() which binds the given value using
           the BLOB binding mechanism instead of the default selected
           one for the value. The ndx may be a numbered or named bind
           index. The value must be of type string, null/undefined
           (both treated as null), or a TypedArray of a type supported
           by the bind() API.

           If passed a single argument, a bind index of 1 is assumed.
        */
        bindAsBlob: function(ndx,arg){
            affirmStmtOpen(this);
            if(1===arguments.length){
                arg = ndx;
                ndx = 1;
            }
            const t = affirmSupportedBindType(arg);
            if(BindTypes.string !== t && BindTypes.blob !== t
               && BindTypes.null !== t){
                toss("Invalid value type for bindAsBlob()");
            }
            this._mayGet = false;
            return bindOne(this, ndx, BindTypes.blob, arg);
        },
        /**
           Steps the statement one time. If the result indicates that
           a row of data is available, true is returned.  If no row of
           data is available, false is returned.  Throws on error.
        */
        step: function(){
            affirmUnlocked(this, 'step()');
            const rc = api.sqlite3_step(affirmStmtOpen(this)._pStmt);
            switch(rc){
                case api.SQLITE_DONE: return this._mayGet = false;
                case api.SQLITE_ROW: return this._mayGet = true;
                default:
                    this._mayGet = false;
                    console.warn("sqlite3_step() rc=",rc,"SQL =",
                                 api.sqlite3_sql(this._pStmt));
                    this.db.checkRc(rc);
            };
        },
        /**
           Fetches the value from the given 0-based column index of
           the current data row, throwing if index is out of range. 

           Requires that step() has just returned a truthy value, else
           an exception is thrown.

           By default it will determine the data type of the result
           automatically. If passed a second arugment, it must be one
           of the enumeration values for sqlite3 types, which are
           defined as members of the sqlite3 module: SQLITE_INTEGER,
           SQLITE_FLOAT, SQLITE_TEXT, SQLITE_BLOB. Any other value,
           except for undefined, will trigger an exception. Passing
           undefined is the same as not passing a value. It is legal
           to, e.g., fetch an integer value as a string, in which case
           sqlite3 will convert the value to a string.

           If ndx is an array, this function behaves a differently: it
           assigns the indexes of the array, from 0 to the number of
           result columns, to the values of the corresponding column,
           and returns that array.

           If ndx is a plain object, this function behaves even
           differentlier: it assigns the properties of the object to
           the values of their corresponding result columns.

           Blobs are returned as Uint8Array instances.

           Potential TODO: add type ID SQLITE_JSON, which fetches the
           result as a string and passes it (if it's not null) to
           JSON.parse(), returning the result of that. Until then,
           getJSON() can be used for that.
        */
        get: function(ndx,asType){
            if(!affirmStmtOpen(this)._mayGet){
                toss("Stmt.step() has not (recently) returned true.");
            }
            if(Array.isArray(ndx)){
                let i = 0;
                while(i<this.columnCount){
                    ndx[i] = this.get(i++);
                }
                return ndx;
            }else if(ndx && 'object'===typeof ndx){
                let i = 0;
                while(i<this.columnCount){
                    ndx[api.sqlite3_column_name(this._pStmt,i)] = this.get(i++);
                }
                return ndx;
            }
            affirmColIndex(this, ndx);
            switch(undefined===asType
                   ? api.sqlite3_column_type(this._pStmt, ndx)
                   : asType){
                case api.SQLITE_NULL: return null;
                case api.SQLITE_INTEGER:{
                    return 0 | api.sqlite3_column_double(this._pStmt, ndx);
                    /* ^^^^^^^^ strips any fractional part and handles
                       handles >32bits */
                }
                case api.SQLITE_FLOAT:
                    return api.sqlite3_column_double(this._pStmt, ndx);
                case api.SQLITE_TEXT:
                    return api.sqlite3_column_text(this._pStmt, ndx);
                case api.SQLITE_BLOB: {
                    const n = api.sqlite3_column_bytes(this._pStmt, ndx),
                          ptr = api.sqlite3_column_blob(this._pStmt, ndx),
                          rc = new Uint8Array(n),
                          heap = n ? api.wasm.HEAP8() : false;
                    for(let i = 0; i < n; ++i) rc[i] = heap[ptr + i];
                    if(n && this.db._blobXfer instanceof Array){
                        /* This is an optimization soley for the
                           Worker-based API. These values will be
                           transfered to the main thread directly
                           instead of being copied. */
                        this.db._blobXfer.push(rc.buffer);
                    }
                    return rc;
                }
                default: toss("Don't know how to translate",
                              "type of result column #"+ndx+".");
            }
            abort("Not reached.");
        },
        /** Equivalent to get(ndx) but coerces the result to an
            integer. */
        getInt: function(ndx){return this.get(ndx,api.SQLITE_INTEGER)},
        /** Equivalent to get(ndx) but coerces the result to a
            float. */
        getFloat: function(ndx){return this.get(ndx,api.SQLITE_FLOAT)},
        /** Equivalent to get(ndx) but coerces the result to a
            string. */
        getString: function(ndx){return this.get(ndx,api.SQLITE_TEXT)},
        /** Equivalent to get(ndx) but coerces the result to a
            Uint8Array. */
        getBlob: function(ndx){return this.get(ndx,api.SQLITE_BLOB)},
        /**
           A convenience wrapper around get() which fetches the value
           as a string and then, if it is not null, passes it to
           JSON.parse(), returning that result. Throws if parsing
           fails. If the result is null, null is returned. An empty
           string, on the other hand, will trigger an exception.
        */
        getJSON: function(ndx){
            const s = this.get(ndx, api.SQLITE_STRING);
            return null===s ? s : JSON.parse(s);
        },
        /**
           Returns the result column name of the given index, or
           throws if index is out of bounds or this statement has been
           finalized. This can be used without having run step()
           first.
        */
        getColumnName: function(ndx){
            return api.sqlite3_column_name(
                affirmColIndex(affirmStmtOpen(this),ndx)._pStmt, ndx
            );
        },
        /**
           If this statement potentially has result columns, this
           function returns an array of all such names. If passed an
           array, it is used as the target and all names are appended
           to it. Returns the target array. Throws if this statement
           cannot have result columns. This object's columnCount member
           holds the number of columns.
        */
        getColumnNames: function(tgt){
            affirmColIndex(affirmStmtOpen(this),0);
            if(!tgt) tgt = [];
            for(let i = 0; i < this.columnCount; ++i){
                tgt.push(api.sqlite3_column_name(this._pStmt, i));
            }
            return tgt;
        },
        /**
           If this statement has named bindable parameters and the
           given name matches one, its 1-based bind index is
           returned. If no match is found, 0 is returned. If it has no
           bindable parameters, the undefined value is returned.
        */
        getParamIndex: function(name){
            return (affirmStmtOpen(this).parameterCount
                    ? api.sqlite3_bind_parameter_index(this._pStmt, name)
                    : undefined);
        }
    }/*Stmt.prototype*/;

    /** OO binding's namespace. */
    const SQLite3 = {
        version: {
            lib: api.sqlite3_libversion(),
            ooApi: "0.0.1"
        },
        DB,
        Stmt,
        /**
           Reports info about compile-time options. It has several
           distinct uses:

           If optName is an array then it is expected to be a list of
           compilation options and this function returns an object
           which maps each such option to true or false, indicating
           whether or not the given option was included in this
           build. That object is returned.

           If optName is an object, its keys are expected to be
           compilation options and this function sets each entry to
           true or false. That object is returned.

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
        compileOptionUsed: function f(optName){
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
                while((k = api.sqlite3_compileoption_get(i++))){
                    f._opt(k,ov);
                    rc[ov[0]] = ov[1];
                }
                return f._result = rc;
            }else if(Array.isArray(optName)){
                const rc = {};
                optName.forEach((v)=>{
                    rc[v] = api.sqlite3_compileoption_used(v);
                });
                return rc;
            }else if('object' === typeof optName){
                Object.keys(optName).forEach((k)=> {
                    optName[k] = api.sqlite3_compileoption_used(k);
                });
                return optName;
            }
            return (
                'string'===typeof optName
            ) ? !!api.sqlite3_compileoption_used(optName) : false;
        }
    }/*SQLite3 object*/;

    namespace.sqlite3 = {
        api: api,
        SQLite3
    };

    if(self === self.window){
        /* This is running in the main window thread, so we're done. */
        postMessage({type:'sqlite3-api',data:'loaded'});
        return;
    }
    /******************************************************************
     End of main window thread. What follows is only intended for use
     in Worker threads.
    ******************************************************************/

    /**
      UNDER CONSTRUCTION

      We need an API which can proxy the DB API via a Worker message
      interface. The primary quirky factor in such an API is that we
      cannot pass callback functions between the window thread and a
      worker thread, so we have to receive all db results via
      asynchronous message-passing. That requires an asychronous API
      with a distinctly different shape that the main OO API.

      Certain important considerations here include:

      - Support only one db connection or multiple? The former is far
        easier, but there's always going to be a user out there who
        wants to juggle six database handles at once. Do we add that
        complexity or tell such users to write their own code using
        the provided lower-level APIs?

      - Fetching multiple results: do we pass them on as a series of
        messages, with start/end messages on either end, or do we
        collect all results and bundle them back in a single message?
        The former is, generically speaking, more memory-efficient but
        the latter far easier to implement in this environment. The
        latter is untennable for large data sets. Despite a web page
        hypothetically being a relatively limited environment, there
        will always be those users who feel that they should/need to
        be able to work with multi-hundred-meg (or larger) blobs, and
        passing around arrays of those may quickly exhaust the JS
        engine's memory.

      TODOs include, but are not limited to:

      - The ability to manage multiple DB handles. This can
        potentially be done via a simple mapping of DB.filename or
        DB._pDb (`sqlite3*` handle) to DB objects. The open()
        interface would need to provide an ID (probably DB._pDb) back
        to the user which can optionally be passed as an argument to
        the other APIs (they'd default to the first-opened DB, for
        ease of use). Client-side usability of this feature would
        benefit from making another wrapper class (or a singleton)
        available to the main thread, with that object proxying all(?)
        communication with the worker.

      - Revisit how virtual files are managed. We currently delete DBs
        from the virtual filesystem when we close them, for the sake
        of saving memory (the VFS lives in RAM). Supporting multiple
        DBs may require that we give up that habit. Similarly, fully
        supporting ATTACH, where a user can upload multiple DBs and
        ATTACH them, also requires the that we manage the VFS entries
        better. As of this writing, ATTACH will fail fatally in the
        fiddle app (but not the lower-level APIs) because it runs in
        safe mode, where ATTACH is disabled.
    */

    /**
       Helper for managing Worker-level state.
    */
    const wState = {
        db: undefined,
        open: function(arg){
            if(!arg && this.db) return this.db;
            else if(this.db) this.db.close();
            return this.db = (Array.isArray(arg) ? new DB(...arg) : new DB(arg));
        },
        close: function(alsoUnlink){
            if(this.db){
                this.db.close(alsoUnlink);
                this.db = undefined;
            }
        },
        affirmOpen: function(){
            return this.db || toss("DB is not opened.");
        },
        post: function(type,data,xferList){
            if(xferList){
                self.postMessage({type, data},xferList);
                xferList.length = 0;
            }else{
                self.postMessage({type, data});
            }
        }
    };

    /**
       A level of "organizational abstraction" for the Worker
       API. Each method in this object must map directly to a Worker
       message type key. The onmessage() dispatcher attempts to
       dispatch all inbound messages to a method of this object,
       passing it the event.data part of the inbound event object. All
       methods must return a plain Object containing any response
       state, which the dispatcher may amend. All methods must throw
       on error.
    */
    const wMsgHandler = {
        xfer: [/*Temp holder for "transferable" postMessage() state.*/],
        /**
           Proxy for DB.exec() which expects a single argument of type
           string (SQL to execute) or an options object in the form
           expected by exec(). The notable differences from exec()
           include:

           - The default value for options.rowMode is 'array' because
           the normal default cannot cross the window/Worker boundary.

           - A function-type options.callback property cannot cross
           the window/Worker boundary, so is not useful here. If
           options.callback is a string then it is assumed to be a
           message type key, in which case a callback function will be
           applied which posts each row result via:

           postMessage({type: thatKeyType, data: theRow})

           And, at the end of the result set (whether or not any
           result rows were produced), it will post an identical
           message with data:null to alert the caller than the result
           set is completed.

           The callback proxy must not recurse into this interface, or
           results are undefined. (It hypothetically cannot recurse
           because an exec() call will be tying up the Worker thread,
           causing any recursion attempt to wait until the first
           exec() is completed.)

           The response is the input options object (or a synthesized
           one if passed only a string), noting that
           options.resultRows and options.columnNames may be populated
           by the call to exec().

           This opens/creates the Worker's db if needed.
        */
        exec: function(ev){
            const opt = (
                'string'===typeof ev.data
            ) ? {sql: ev.data} : (ev.data || {});
            if(!opt.rowMode){
                /* Since the default rowMode of 'stmt' is not useful
                   for the Worker interface, we'll default to
                   something else. */
                opt.rowMode = 'array';
            }else if('stmt'===opt.rowMode){
                toss("Invalid rowMode for exec(): stmt mode",
                     "does not work in the Worker API.");
            }
            const db = wState.open();
            if(opt.callback || opt.resultRows instanceof Array){
                // Part of a copy-avoidance optimization for blobs
                db._blobXfer = this.xfer;
            }
            const callbackMsgType = opt.callback;
            if('string' === typeof callbackMsgType){
                const that = this;
                opt.callback =
                    (row)=>wState.post(callbackMsgType,row,this.xfer);
            }
            try {
                db.exec(opt);
                if(opt.callback instanceof Function){
                    opt.callback = callbackMsgType;
                    wState.post(callbackMsgType, null);
                }
            }finally{
                delete db._blobXfer;
                if('string'===typeof callbackMsgType){
                    opt.callback = callbackMsgType;
                }
            }
            return opt;
        }/*exec()*/,
        /**
           Proxy for DB.exportBinaryImage(). Throws if the db has not
           been opened. Response is an object:

           {
             buffer: Uint8Array (db file contents),
             filename: the current db filename,
             mimetype: string
           }
        */
        export: function(ev){
            const db = wState.affirmOpen();
            const response = {
                buffer: db.exportBinaryImage(),
                filename: db.filename,
                mimetype: 'application/x-sqlite3'
            };
            this.xfer.push(response.buffer.buffer);
            return response;
        }/*export()*/,
        /**
           Proxy for the DB constructor. Expects to be passed a single
           object or a falsy value to use defaults. The object may
           have a filename property to name the db file (see the DB
           constructor for peculiarities and transformations) and/or a
           buffer property (a Uint8Array holding a complete database
           file's contents). The response is an object:

           {
             filename: db filename (possibly differing from the input)
           }

           If the Worker's db is currently opened, this call closes it
           before proceeding.
        */
        open: function(ev){
            wState.close(/*true???*/);
            const args = [], data = (ev.data || {});
            if(data.filename) args.push(data.filename);
            if(data.buffer){
                args.push(data.buffer);
                this.xfer.push(data.buffer.buffer);
            }
            const db = wState.open(args);
            return {filename: db.filename};
        },
        /**
           Proxy for DB.close(). If ev.data may either be a boolean or
           an object with an `unlink` property. If that value is
           truthy then the db file (if the db is currently open) will
           be unlinked from the virtual filesystem, else it will be
           kept intact. The response object is:

           {filename: db filename _if_ the db is is opened when this
                      is called, else the undefined value
           }
        */
        close: function(ev){
            const response = {
                filename: wState.db && wState.db.filename
            };
            if(wState.db){
                wState.close(!!(ev.data && 'object'===typeof ev.data)
                             ? ev.data.unlink : ev.data);
            }
            return response;
        }
    }/*wMsgHandler*/;

    /**
       UNDER CONSTRUCTION!

       A subset of the DB API is accessible via Worker messages in the form:

       { type: apiCommand,
         data: apiArguments }

       As a rule, these commands respond with a postMessage() of their
       own in the same form, but will, if needed, transform the `data`
       member to an object and may add state to it. The responses
       always have an object-format `data` part. If the inbound `data`
       is an object which has a `messageId` property, that property is
       always mirrored in the result object, for use in client-side
       dispatching of these asynchronous results. Exceptions thrown
       during processing result in an `error`-type event with a
       payload in the form:

       {
         message: error string,
         errorClass: class name of the error type,
         input: ev.data,
         [messageId: if set in the inbound message]
       }

       The individual APIs are documented in the wMsgHandler object.
    */
    self.onmessage = function(ev){
        ev = ev.data;
        let response, evType = ev.type;
        try {
            if(wMsgHandler.hasOwnProperty(evType) &&
               wMsgHandler[evType] instanceof Function){
                response = wMsgHandler[evType](ev);
            }else{
                toss("Unknown db worker message type:",ev.type);
            }
        }catch(err){
            evType = 'error';
            response = {
                message: err.message,
                errorClass: err.name,
                input: ev
            };
        }
        if(!response.messageId && ev.data
           && 'object'===typeof ev.data && ev.data.messageId){
            response.messageId = ev.data.messageId;
        }
        wState.post(evType, response, wMsgHandler.xfer);
    };

    postMessage({type:'sqlite3-api',data:'loaded'});
})/*postRun.push(...)*/;
