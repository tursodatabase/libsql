/**
  2022-07-08

  The author disclaims copyright to this source code.  In place of a
  legal notice, here is a blessing:

  *   May you do good and not evil.
  *   May you find forgiveness for yourself and forgive others.
  *   May you share freely, never taking more than you give.

  ***********************************************************************

  The whwasmutil is developed in conjunction with the Jaccwabyt
  project:

  https://fossil.wanderinghorse.net/r/jaccwabyt

  and sqlite3:

  https://sqlite.org

  This file is kept in sync between both of those trees.

  Maintenance reminder: If you're reading this in a tree other than
  one of those listed above, note that this copy may be replaced with
  upstream copies of that one from time to time. Thus the code
  installed by this function "should not" be edited outside of those
  projects, else it risks getting overwritten.
*/
/**
   This function is intended to simplify porting around various bits
   of WASM-related utility code from project to project.

   The primary goal of this code is to replace, where possible,
   Emscripten-generated glue code with equivalent utility code which
   can be used in arbitrary WASM environments built with toolchains
   other than Emscripten. As of this writing, this code is capable of
   acting as a replacement for Emscripten's generated glue code
   _except_ that the latter installs handlers for Emscripten-provided
   APIs such as its "FS" (virtual filesystem) API. Loading of such
   things still requires using Emscripten's glue, but the post-load
   utility APIs provided by this code are still usable as replacements
   for their sub-optimally-documented Emscripten counterparts.

   Intended usage:

   ```
   globalThis.WhWasmUtilInstaller(appObject);
   delete globalThis.WhWasmUtilInstaller;
   ```

   Its global-scope symbol is intended only to provide an easy way to
   make it available to 3rd-party scripts and "should" be deleted
   after calling it. That symbols is _not_ used within the library.

   Forewarning: this API explicitly targets only browser
   environments. If a given non-browser environment has the
   capabilities needed for a given feature (e.g. TextEncoder), great,
   but it does not go out of its way to account for them and does not
   provide compatibility crutches for them.

   It currently offers alternatives to the following
   Emscripten-generated APIs:

   - OPTIONALLY memory allocation, but how this gets imported is
     environment-specific.  Most of the following features only work
     if allocation is available.

   - WASM-exported "indirect function table" access and
     manipulation. e.g.  creating new WASM-side functions using JS
     functions, analog to Emscripten's addFunction() and
     uninstallFunction() but slightly different.

   - Get/set specific heap memory values, analog to Emscripten's
     getValue() and setValue().

   - String length counting in UTF-8 bytes (C-style and JS strings).

   - JS string to C-string conversion and vice versa, analog to
     Emscripten's stringToUTF8Array() and friends, but with slighter
     different interfaces.

   - JS string to Uint8Array conversion, noting that browsers actually
     already have this built in via TextEncoder.

   - "Scoped" allocation, such that allocations made inside of a given
     explicit scope will be automatically cleaned up when the scope is
     closed. This is fundamentally similar to Emscripten's
     stackAlloc() and friends but uses the heap instead of the stack
     because access to the stack requires C code.

   - Create JS wrappers for WASM functions, analog to Emscripten's
     ccall() and cwrap() functions, except that the automatic
     conversions for function arguments and return values can be
     easily customized by the client by assigning custom function
     signature type names to conversion functions. Essentially,
     it's ccall() and cwrap() on steroids.

   How to install...

   Passing an object to this function will install the functionality
   into that object. Afterwards, client code "should" delete the global
   symbol.

   This code requires that the target object have the following
   properties, noting that they needn't be available until the first
   time one of the installed APIs is used (as opposed to when this
   function is called) except where explicitly noted:

   - `exports` must be a property of the target object OR a property
     of `target.instance` (a WebAssembly.Module instance) and it must
     contain the symbols exported by the WASM module associated with
     this code. In an Enscripten environment it must be set to
     `Module['asm']` (versions <=3.1.43) or `wasmExports` (versions
     >=3.1.44). The exports object must contain a minimum of the
     following symbols:

     - `memory`: a WebAssembly.Memory object representing the WASM
       memory. _Alternately_, the `memory` property can be set as
       `target.memory`, in particular if the WASM heap memory is
       initialized in JS an _imported_ into WASM, as opposed to being
       initialized in WASM and exported to JS.

     - `__indirect_function_table`: the WebAssembly.Table object which
       holds WASM-exported functions. This API does not strictly
       require that the table be able to grow but it will throw if its
       `installFunction()` is called and the table cannot grow.

   In order to simplify downstream usage, if `target.exports` is not
   set when this is called then a property access interceptor
   (read-only, configurable, enumerable) gets installed as `exports`
   which resolves to `target.instance.exports`, noting that the latter
   property need not exist until the first time `target.exports` is
   accessed.

   Some APIs _optionally_ make use of the `bigIntEnabled` property of
   the target object. It "should" be set to true if the WASM
   environment is compiled with BigInt support, else it must be
   false. If it is false, certain BigInt-related features will trigger
   an exception if invoked. This property, if not set when this is
   called, will get a default value of true only if the BigInt64Array
   constructor is available, else it will default to false. Note that
   having the BigInt type is not sufficient for full int64 integration
   with WASM: the target WASM file must also have been built with
   that support. In Emscripten that's done using the `-sWASM_BIGINT`
   flag.

   Some optional APIs require that the target have the following
   methods:

   - 'alloc()` must behave like C's `malloc()`, allocating N bytes of
     memory and returning its pointer. In Emscripten this is
     conventionally made available via `Module['_malloc']`. This API
     requires that the alloc routine throw on allocation error, as
     opposed to returning null or 0.

   - 'dealloc()` must behave like C's `free()`, accepting either a
     pointer returned from its allocation counterpart or the values
     null/0 (for which it must be a no-op). allocating N bytes of
     memory and returning its pointer. In Emscripten this is
     conventionally made available via `Module['_free']`.

   APIs which require allocation routines are explicitly documented as
   such and/or have "alloc" in their names.

   This code is developed and maintained in conjunction with the
   Jaccwabyt project:

   https://fossil.wanderinghorse.net/r/jaccwabbyt

   More specifically:

   https://fossil.wanderinghorse.net/r/jaccwabbyt/file/common/whwasmutil.js
*/
globalThis.WhWasmUtilInstaller = function(target){
  'use strict';
  if(undefined===target.bigIntEnabled){
    target.bigIntEnabled = !!globalThis['BigInt64Array'];
  }

  /** Throws a new Error, the message of which is the concatenation of
      all args with a space between each. */
  const toss = (...args)=>{throw new Error(args.join(' '))};

  if(!target.exports){
    Object.defineProperty(target, 'exports', {
      enumerable: true, configurable: true,
      get: ()=>(target.instance && target.instance.exports)
    });
  }

  /*********
    alloc()/dealloc() auto-install...

    This would be convenient but it can also cause us to pick up
    malloc() even when the client code is using a different exported
    allocator (who, me?), which is bad. malloc() may be exported even
    if we're not explicitly using it and overriding the malloc()
    function, linking ours first, is not always feasible when using a
    malloc() proxy, as it can lead to recursion and stack overflow
    (who, me?). So... we really need the downstream code to set up
    target.alloc/dealloc() itself.
  ******/
  /******
  if(target.exports){
    //Maybe auto-install alloc()/dealloc()...
    if(!target.alloc && target.exports.malloc){
      target.alloc = function(n){
        const m = this(n);
        return m || toss("Allocation of",n,"byte(s) failed.");
      }.bind(target.exports.malloc);
    }

    if(!target.dealloc && target.exports.free){
      target.dealloc = function(ptr){
        if(ptr) this(ptr);
      }.bind(target.exports.free);
    }
  }*******/

  /**
     Pointers in WASM are currently assumed to be 32-bit, but someday
     that will certainly change.
  */
  const ptrIR = target.pointerIR || 'i32';
  const ptrSizeof = target.ptrSizeof =
        ('i32'===ptrIR ? 4
         : ('i64'===ptrIR
            ? 8 : toss("Unhandled ptrSizeof:",ptrIR)));
  /** Stores various cached state. */
  const cache = Object.create(null);
  /** Previously-recorded size of cache.memory.buffer, noted so that
      we can recreate the view objects if the heap grows. */
  cache.heapSize = 0;
  /** WebAssembly.Memory object extracted from target.memory or
      target.exports.memory the first time heapWrappers() is
      called. */
  cache.memory = null;
  /** uninstallFunction() puts table indexes in here for reuse and
      installFunction() extracts them. */
  cache.freeFuncIndexes = [];
  /**
     Used by scopedAlloc() and friends.
  */
  cache.scopedAlloc = [];

  cache.utf8Decoder = new TextDecoder();
  cache.utf8Encoder = new TextEncoder('utf-8');

  /**
     For the given IR-like string in the set ('i8', 'i16', 'i32',
     'f32', 'float', 'i64', 'f64', 'double', '*'), or any string value
     ending in '*', returns the sizeof for that value
     (target.ptrSizeof in the latter case). For any other value, it
     returns the undefined value.
  */
  target.sizeofIR = (n)=>{
    switch(n){
        case 'i8': return 1;
        case 'i16': return 2;
        case 'i32': case 'f32': case 'float': return 4;
        case 'i64': case 'f64': case 'double': return 8;
        case '*': return ptrSizeof;
        default:
          return (''+n).endsWith('*') ? ptrSizeof : undefined;
    }
  };

  /**
     If (cache.heapSize !== cache.memory.buffer.byteLength), i.e. if
     the heap has grown since the last call, updates cache.HEAPxyz.
     Returns the cache object.
  */
  const heapWrappers = function(){
    if(!cache.memory){
      cache.memory = (target.memory instanceof WebAssembly.Memory)
        ? target.memory : target.exports.memory;
    }else if(cache.heapSize === cache.memory.buffer.byteLength){
      return cache;
    }
    // heap is newly-acquired or has been resized....
    const b = cache.memory.buffer;
    cache.HEAP8 = new Int8Array(b); cache.HEAP8U = new Uint8Array(b);
    cache.HEAP16 = new Int16Array(b); cache.HEAP16U = new Uint16Array(b);
    cache.HEAP32 = new Int32Array(b); cache.HEAP32U = new Uint32Array(b);
    if(target.bigIntEnabled){
      cache.HEAP64 = new BigInt64Array(b); cache.HEAP64U = new BigUint64Array(b);
    }
    cache.HEAP32F = new Float32Array(b); cache.HEAP64F = new Float64Array(b);
    cache.heapSize = b.byteLength;
    return cache;
  };

  /** Convenience equivalent of this.heapForSize(8,false). */
  target.heap8 = ()=>heapWrappers().HEAP8;

  /** Convenience equivalent of this.heapForSize(8,true). */
  target.heap8u = ()=>heapWrappers().HEAP8U;

  /** Convenience equivalent of this.heapForSize(16,false). */
  target.heap16 = ()=>heapWrappers().HEAP16;

  /** Convenience equivalent of this.heapForSize(16,true). */
  target.heap16u = ()=>heapWrappers().HEAP16U;

  /** Convenience equivalent of this.heapForSize(32,false). */
  target.heap32 = ()=>heapWrappers().HEAP32;

  /** Convenience equivalent of this.heapForSize(32,true). */
  target.heap32u = ()=>heapWrappers().HEAP32U;

  /**
     Requires n to be one of:

     - integer 8, 16, or 32.
     - A integer-type TypedArray constructor: Int8Array, Int16Array,
     Int32Array, or their Uint counterparts.

     If this.bigIntEnabled is true, it also accepts the value 64 or a
     BigInt64Array/BigUint64Array, else it throws if passed 64 or one
     of those constructors.

     Returns an integer-based TypedArray view of the WASM heap
     memory buffer associated with the given block size. If passed
     an integer as the first argument and unsigned is truthy then
     the "U" (unsigned) variant of that view is returned, else the
     signed variant is returned. If passed a TypedArray value, the
     2nd argument is ignored. Note that Float32Array and
     Float64Array views are not supported by this function.

     Note that growth of the heap will invalidate any references to
     this heap, so do not hold a reference longer than needed and do
     not use a reference after any operation which may
     allocate. Instead, re-fetch the reference by calling this
     function again.

     Throws if passed an invalid n.

     Pedantic side note: the name "heap" is a bit of a misnomer. In a
     WASM environment, the stack and heap memory are all accessed via
     the same view(s) of the memory.
  */
  target.heapForSize = function(n,unsigned = true){
    let ctor;
    const c = (cache.memory && cache.heapSize === cache.memory.buffer.byteLength)
          ? cache : heapWrappers();
    switch(n){
        case Int8Array: return c.HEAP8; case Uint8Array: return c.HEAP8U;
        case Int16Array: return c.HEAP16; case Uint16Array: return c.HEAP16U;
        case Int32Array: return c.HEAP32; case Uint32Array: return c.HEAP32U;
        case 8:  return unsigned ? c.HEAP8U : c.HEAP8;
        case 16: return unsigned ? c.HEAP16U : c.HEAP16;
        case 32: return unsigned ? c.HEAP32U : c.HEAP32;
        case 64:
          if(c.HEAP64) return unsigned ? c.HEAP64U : c.HEAP64;
          break;
        default:
          if(target.bigIntEnabled){
            if(n===globalThis['BigUint64Array']) return c.HEAP64U;
            else if(n===globalThis['BigInt64Array']) return c.HEAP64;
            break;
          }
    }
    toss("Invalid heapForSize() size: expecting 8, 16, 32,",
         "or (if BigInt is enabled) 64.");
  };

  /**
     Returns the WASM-exported "indirect function table."
  */
  target.functionTable = function(){
    return target.exports.__indirect_function_table;
    /** -----------------^^^^^ "seems" to be a standardized export name.
        From Emscripten release notes from 2020-09-10:
        - Use `__indirect_function_table` as the import name for the
        table, which is what LLVM does.
    */
  };

  /**
     Given a function pointer, returns the WASM function table entry
     if found, else returns a falsy value: undefined if fptr is out of
     range or null if it's in range but the table entry is empty.
  */
  target.functionEntry = function(fptr){
    const ft = target.functionTable();
    return fptr < ft.length ? ft.get(fptr) : undefined;
  };

  /**
     Creates a WASM function which wraps the given JS function and
     returns the JS binding of that WASM function. The signature
     string must be the Jaccwabyt-format or Emscripten
     addFunction()-format function signature string. In short: in may
     have one of the following formats:

     - Emscripten: `"x..."`, where the first x is a letter representing
       the result type and subsequent letters represent the argument
       types. Functions with no arguments have only a single
       letter. See below.

     - Jaccwabyt: `"x(...)"` where `x` is the letter representing the
       result type and letters in the parens (if any) represent the
       argument types. Functions with no arguments use `x()`. See
       below.

     Supported letters:

     - `i` = int32
     - `p` = int32 ("pointer")
     - `j` = int64
     - `f` = float32
     - `d` = float64
     - `v` = void, only legal for use as the result type

     It throws if an invalid signature letter is used.

     Jaccwabyt-format signatures support some additional letters which
     have no special meaning here but (in this context) act as aliases
     for other letters:

     - `s`, `P`: same as `p`

     Sidebar: this code is developed together with Jaccwabyt, thus the
     support for its signature format.

     The arguments may be supplied in either order: (func,sig) or
     (sig,func).
  */
  target.jsFuncToWasm = function f(func, sig){
    /** Attribution: adapted up from Emscripten-generated glue code,
        refactored primarily for efficiency's sake, eliminating
        call-local functions and superfluous temporary arrays. */
    if(!f._){/*static init...*/
      f._ = {
        // Map of signature letters to type IR values
        sigTypes: Object.assign(Object.create(null),{
          i: 'i32', p: 'i32', P: 'i32', s: 'i32',
          j: 'i64', f: 'f32', d: 'f64'
        }),
        // Map of type IR values to WASM type code values
        typeCodes: Object.assign(Object.create(null),{
          f64: 0x7c, f32: 0x7d, i64: 0x7e, i32: 0x7f
        }),
        /** Encodes n, which must be <2^14 (16384), into target array
            tgt, as a little-endian value, using the given method
            ('push' or 'unshift'). */
        uleb128Encode: function(tgt, method, n){
          if(n<128) tgt[method](n);
          else tgt[method]( (n % 128) | 128, n>>7);
        },
        /** Intentionally-lax pattern for Jaccwabyt-format function
            pointer signatures, the intent of which is simply to
            distinguish them from Emscripten-format signatures. The
            downstream checks are less lax. */
        rxJSig: /^(\w)\((\w*)\)$/,
        /** Returns the parameter-value part of the given signature
            string. */
        sigParams: function(sig){
          const m = f._.rxJSig.exec(sig);
          return m ? m[2] : sig.substr(1);
        },
        /** Returns the IR value for the given letter or throws
            if the letter is invalid. */
        letterType: (x)=>f._.sigTypes[x] || toss("Invalid signature letter:",x),
        /** Returns an object describing the result type and parameter
            type(s) of the given function signature, or throws if the
            signature is invalid. */
        /******** // only valid for use with the WebAssembly.Function ctor, which
                  // is not yet documented on MDN.
        sigToWasm: function(sig){
          const rc = {parameters:[], results: []};
          if('v'!==sig[0]) rc.results.push(f.sigTypes(sig[0]));
          for(const x of f._.sigParams(sig)){
            rc.parameters.push(f._.typeCodes(x));
          }
          return rc;
        },************/
        /** Pushes the WASM data type code for the given signature
            letter to the given target array. Throws if letter is
            invalid. */
        pushSigType: (dest, letter)=>dest.push(f._.typeCodes[f._.letterType(letter)])
      };
    }/*static init*/
    if('string'===typeof func){
      const x = sig;
      sig = func;
      func = x;
    }
    const sigParams = f._.sigParams(sig);
    const wasmCode = [0x01/*count: 1*/, 0x60/*function*/];
    f._.uleb128Encode(wasmCode, 'push', sigParams.length);
    for(const x of sigParams) f._.pushSigType(wasmCode, x);
    if('v'===sig[0]) wasmCode.push(0);
    else{
      wasmCode.push(1);
      f._.pushSigType(wasmCode, sig[0]);
    }
    f._.uleb128Encode(wasmCode, 'unshift', wasmCode.length)/* type section length */;
    wasmCode.unshift(
      0x00, 0x61, 0x73, 0x6d, /* magic: "\0asm" */
      0x01, 0x00, 0x00, 0x00, /* version: 1 */
      0x01 /* type section code */
    );
    wasmCode.push(
      /* import section: */ 0x02, 0x07,
      /* (import "e" "f" (func 0 (type 0))): */
      0x01, 0x01, 0x65, 0x01, 0x66, 0x00, 0x00,
      /* export section: */ 0x07, 0x05,
      /* (export "f" (func 0 (type 0))): */
      0x01, 0x01, 0x66, 0x00, 0x00
    );
    return (new WebAssembly.Instance(
      new WebAssembly.Module(new Uint8Array(wasmCode)), {
        e: { f: func }
      })).exports['f'];
  }/*jsFuncToWasm()*/;

  /**
     Documented as target.installFunction() except for the 3rd
     argument: if truthy, the newly-created function pointer
     is stashed in the current scoped-alloc scope and will be
     cleaned up at the matching scopedAllocPop(), else it
     is not stashed there.
   */
  const __installFunction = function f(func, sig, scoped){
    if(scoped && !cache.scopedAlloc.length){
      toss("No scopedAllocPush() scope is active.");
    }
    if('string'===typeof func){
      const x = sig;
      sig = func;
      func = x;
    }
    if('string'!==typeof sig || !(func instanceof Function)){
      toss("Invalid arguments: expecting (function,signature) "+
           "or (signature,function).");
    }
    const ft = target.functionTable();
    const oldLen = ft.length;
    let ptr;
    while(cache.freeFuncIndexes.length){
      ptr = cache.freeFuncIndexes.pop();
      if(ft.get(ptr)){ /* Table was modified via a different API */
        ptr = null;
        continue;
      }else{
        break;
      }
    }
    if(!ptr){
      ptr = oldLen;
      ft.grow(1);
    }
    try{
      /*this will only work if func is a WASM-exported function*/
      ft.set(ptr, func);
      if(scoped){
        cache.scopedAlloc[cache.scopedAlloc.length-1].push(ptr);
      }
      return ptr;
    }catch(e){
      if(!(e instanceof TypeError)){
        if(ptr===oldLen) cache.freeFuncIndexes.push(oldLen);
        throw e;
      }
    }
    // It's not a WASM-exported function, so compile one...
    try {
      const fptr = target.jsFuncToWasm(func, sig);
      ft.set(ptr, fptr);
      if(scoped){
        cache.scopedAlloc[cache.scopedAlloc.length-1].push(ptr);
      }
    }catch(e){
      if(ptr===oldLen) cache.freeFuncIndexes.push(oldLen);
      throw e;
    }
    return ptr;
  };

  /**
     Expects a JS function and signature, exactly as for
     this.jsFuncToWasm(). It uses that function to create a
     WASM-exported function, installs that function to the next
     available slot of this.functionTable(), and returns the
     function's index in that table (which acts as a pointer to that
     function). The returned pointer can be passed to
     uninstallFunction() to uninstall it and free up the table slot for
     reuse.

     If passed (string,function) arguments then it treats the first
     argument as the signature and second as the function.

     As a special case, if the passed-in function is a WASM-exported
     function then the signature argument is ignored and func is
     installed as-is, without requiring re-compilation/re-wrapping.

     This function will propagate an exception if
     WebAssembly.Table.grow() throws or this.jsFuncToWasm() throws.
     The former case can happen in an Emscripten-compiled
     environment when building without Emscripten's
     `-sALLOW_TABLE_GROWTH` flag.

     Sidebar: this function differs from Emscripten's addFunction()
     _primarily_ in that it does not share that function's
     undocumented behavior of reusing a function if it's passed to
     addFunction() more than once, which leads to uninstallFunction()
     breaking clients which do not take care to avoid that case:

     https://github.com/emscripten-core/emscripten/issues/17323
  */
  target.installFunction = (func, sig)=>__installFunction(func, sig, false);

  /**
     Works exactly like installFunction() but requires that a
     scopedAllocPush() is active and uninstalls the given function
     when that alloc scope is popped via scopedAllocPop().
     This is used for implementing JS/WASM function bindings which
     should only persist for the life of a call into a single
     C-side function.
  */
  target.scopedInstallFunction = (func, sig)=>__installFunction(func, sig, true);

  /**
     Requires a pointer value previously returned from
     this.installFunction(). Removes that function from the WASM
     function table, marks its table slot as free for re-use, and
     returns that function. It is illegal to call this before
     installFunction() has been called and results are undefined if
     ptr was not returned by that function. The returned function
     may be passed back to installFunction() to reinstall it.

     To simplify certain use cases, if passed a falsy non-0 value
     (noting that 0 is a valid function table index), this function
     has no side effects and returns undefined.
  */
  target.uninstallFunction = function(ptr){
    if(!ptr && 0!==ptr) return undefined;
    const fi = cache.freeFuncIndexes;
    const ft = target.functionTable();
    fi.push(ptr);
    const rc = ft.get(ptr);
    ft.set(ptr, null);
    return rc;
  };

  /**
     Given a WASM heap memory address and a data type name in the form
     (i8, i16, i32, i64, float (or f32), double (or f64)), this
     fetches the numeric value from that address and returns it as a
     number or, for the case of type='i64', a BigInt (noting that that
     type triggers an exception if this.bigIntEnabled is
     falsy). Throws if given an invalid type.

     If the first argument is an array, it is treated as an array of
     addresses and the result is an array of the values from each of
     those address, using the same 2nd argument for determining the
     value type to fetch.

     As a special case, if type ends with a `*`, it is considered to
     be a pointer type and is treated as the WASM numeric type
     appropriate for the pointer size (`i32`).

     While likely not obvious, this routine and its poke()
     counterpart are how pointer-to-value _output_ parameters
     in WASM-compiled C code can be interacted with:

     ```
     const ptr = alloc(4);
     poke(ptr, 0, 'i32'); // clear the ptr's value
     aCFuncWithOutputPtrToInt32Arg( ptr ); // e.g. void foo(int *x);
     const result = peek(ptr, 'i32'); // fetch ptr's value
     dealloc(ptr);
     ```

     scopedAlloc() and friends can be used to make handling of
     `ptr` safe against leaks in the case of an exception:

     ```
     let result;
     const scope = scopedAllocPush();
     try{
       const ptr = scopedAlloc(4);
       poke(ptr, 0, 'i32');
       aCFuncWithOutputPtrArg( ptr );
       result = peek(ptr, 'i32');
     }finally{
       scopedAllocPop(scope);
     }
     ```

     As a rule poke() must be called to set (typically zero
     out) the pointer's value, else it will contain an essentially
     random value.

     ACHTUNG: calling this often, e.g. in a loop, can have a noticably
     painful impact on performance. Rather than doing so, use
     heapForSize() to fetch the heap object and read directly from it.

     See: poke()
  */
  target.peek = function f(ptr, type='i8'){
    if(type.endsWith('*')) type = ptrIR;
    const c = (cache.memory && cache.heapSize === cache.memory.buffer.byteLength)
          ? cache : heapWrappers();
    const list = Array.isArray(ptr) ? [] : undefined;
    let rc;
    do{
      if(list) ptr = arguments[0].shift();
      switch(type){
          case 'i1':
          case 'i8': rc = c.HEAP8[ptr>>0]; break;
          case 'i16': rc = c.HEAP16[ptr>>1]; break;
          case 'i32': rc = c.HEAP32[ptr>>2]; break;
          case 'float': case 'f32': rc = c.HEAP32F[ptr>>2]; break;
          case 'double': case 'f64': rc = Number(c.HEAP64F[ptr>>3]); break;
          case 'i64':
            if(target.bigIntEnabled){
              rc = BigInt(c.HEAP64[ptr>>3]);
              break;
            }
            /* fallthru */
          default:
            toss('Invalid type for peek():',type);
      }
      if(list) list.push(rc);
    }while(list && arguments[0].length);
    return list || rc;
  };

  /**
     The counterpart of peek(), this sets a numeric value at
     the given WASM heap address, using the type to define how many
     bytes are written. Throws if given an invalid type. See
     peek() for details about the type argument. If the 3rd
     argument ends with `*` then it is treated as a pointer type and
     this function behaves as if the 3rd argument were `i32`.

     If the first argument is an array, it is treated like a list
     of pointers and the given value is written to each one.

     Returns `this`. (Prior to 2022-12-09 it returns this function.)

     ACHTUNG: calling this often, e.g. in a loop, can have a noticably
     painful impact on performance. Rather than doing so, use
     heapForSize() to fetch the heap object and assign directly to it
     or use the heap's set() method.
  */
  target.poke = function(ptr, value, type='i8'){
    if (type.endsWith('*')) type = ptrIR;
    const c = (cache.memory && cache.heapSize === cache.memory.buffer.byteLength)
          ? cache : heapWrappers();
    for(const p of (Array.isArray(ptr) ? ptr : [ptr])){
      switch (type) {
          case 'i1':
          case 'i8': c.HEAP8[p>>0] = value; continue;
          case 'i16': c.HEAP16[p>>1] = value; continue;
          case 'i32': c.HEAP32[p>>2] = value; continue;
          case 'float': case 'f32': c.HEAP32F[p>>2] = value; continue;
          case 'double': case 'f64': c.HEAP64F[p>>3] = value; continue;
          case 'i64':
            if(c.HEAP64){
              c.HEAP64[p>>3] = BigInt(value);
              continue;
            }
            /* fallthru */
          default:
            toss('Invalid type for poke(): ' + type);
      }
    }
    return this;
  };

  /**
     Convenience form of peek() intended for fetching
     pointer-to-pointer values. If passed a single non-array argument
     it returns the value of that one pointer address. If passed
     multiple arguments, or a single array of arguments, it returns an
     array of their values.
  */
  target.peekPtr = (...ptr)=>target.peek( (1===ptr.length ? ptr[0] : ptr), ptrIR );

  /**
     A variant of poke() intended for setting pointer-to-pointer
     values. Its differences from poke() are that (1) it defaults to a
     value of 0 and (2) it always writes to the pointer-sized heap
     view.
  */
  target.pokePtr = (ptr, value=0)=>target.poke(ptr, value, ptrIR);

  /**
     Convenience form of peek() intended for fetching i8 values. If
     passed a single non-array argument it returns the value of that
     one pointer address. If passed multiple arguments, or a single
     array of arguments, it returns an array of their values.
  */
  target.peek8 = (...ptr)=>target.peek( (1===ptr.length ? ptr[0] : ptr), 'i8' );
  /**
     Convience form of poke() intended for setting individual bytes.
     Its difference from poke() is that it always writes to the
     i8-sized heap view.
  */
  target.poke8 = (ptr, value)=>target.poke(ptr, value, 'i8');
  /** i16 variant of peek8(). */
  target.peek16 = (...ptr)=>target.peek( (1===ptr.length ? ptr[0] : ptr), 'i16' );
  /** i16 variant of poke8(). */
  target.poke16 = (ptr, value)=>target.poke(ptr, value, 'i16');
  /** i32 variant of peek8(). */
  target.peek32 = (...ptr)=>target.peek( (1===ptr.length ? ptr[0] : ptr), 'i32' );
  /** i32 variant of poke8(). */
  target.poke32 = (ptr, value)=>target.poke(ptr, value, 'i32');
  /** i64 variant of peek8(). Will throw if this build is not
      configured for BigInt support. */
  target.peek64 = (...ptr)=>target.peek( (1===ptr.length ? ptr[0] : ptr), 'i64' );
  /** i64 variant of poke8(). Will throw if this build is not
      configured for BigInt support. Note that this returns
      a BigInt-type value, not a Number-type value. */
  target.poke64 = (ptr, value)=>target.poke(ptr, value, 'i64');
  /** f32 variant of peek8(). */
  target.peek32f = (...ptr)=>target.peek( (1===ptr.length ? ptr[0] : ptr), 'f32' );
  /** f32 variant of poke8(). */
  target.poke32f = (ptr, value)=>target.poke(ptr, value, 'f32');
  /** f64 variant of peek8(). */
  target.peek64f = (...ptr)=>target.peek( (1===ptr.length ? ptr[0] : ptr), 'f64' );
  /** f64 variant of poke8(). */
  target.poke64f = (ptr, value)=>target.poke(ptr, value, 'f64');

  /** Deprecated alias for getMemValue() */
  target.getMemValue = target.peek;
  /** Deprecated alias for peekPtr() */
  target.getPtrValue = target.peekPtr;
  /** Deprecated alias for poke() */
  target.setMemValue = target.poke;
  /** Deprecated alias for pokePtr() */
  target.setPtrValue = target.pokePtr;

  /**
     Returns true if the given value appears to be legal for use as
     a WASM pointer value. Its _range_ of values is not (cannot be)
     validated except to ensure that it is a 32-bit integer with a
     value of 0 or greater. Likewise, it cannot verify whether the
     value actually refers to allocated memory in the WASM heap.
  */
  target.isPtr32 = (ptr)=>('number'===typeof ptr && (ptr===(ptr|0)) && ptr>=0);

  /**
     isPtr() is an alias for isPtr32(). If/when 64-bit WASM pointer
     support becomes widespread, it will become an alias for either
     isPtr32() or the as-yet-hypothetical isPtr64(), depending on a
     configuration option.
  */
  target.isPtr = target.isPtr32;

  /**
     Expects ptr to be a pointer into the WASM heap memory which
     refers to a NUL-terminated C-style string encoded as UTF-8.
     Returns the length, in bytes, of the string, as for `strlen(3)`.
     As a special case, if !ptr or if it's not a pointer then it
     returns `null`. Throws if ptr is out of range for
     target.heap8u().
  */
  target.cstrlen = function(ptr){
    if(!ptr || !target.isPtr(ptr)) return null;
    const h = heapWrappers().HEAP8U;
    let pos = ptr;
    for( ; h[pos] !== 0; ++pos ){}
    return pos - ptr;
  };

  /** Internal helper to use in operations which need to distinguish
      between SharedArrayBuffer heap memory and non-shared heap. */
  const __SAB = ('undefined'===typeof SharedArrayBuffer)
        ? function(){} : SharedArrayBuffer;
  const __utf8Decode = function(arrayBuffer, begin, end){
    return cache.utf8Decoder.decode(
      (arrayBuffer.buffer instanceof __SAB)
        ? arrayBuffer.slice(begin, end)
        : arrayBuffer.subarray(begin, end)
    );
  };

  /**
     Expects ptr to be a pointer into the WASM heap memory which
     refers to a NUL-terminated C-style string encoded as UTF-8. This
     function counts its byte length using cstrlen() then returns a
     JS-format string representing its contents. As a special case, if
     ptr is falsy or not a pointer, `null` is returned.
  */
  target.cstrToJs = function(ptr){
    const n = target.cstrlen(ptr);
    return n ? __utf8Decode(heapWrappers().HEAP8U, ptr, ptr+n) : (null===n ? n : "");
  };

  /**
     Given a JS string, this function returns its UTF-8 length in
     bytes. Returns null if str is not a string.
  */
  target.jstrlen = function(str){
    /** Attribution: derived from Emscripten's lengthBytesUTF8() */
    if('string'!==typeof str) return null;
    const n = str.length;
    let len = 0;
    for(let i = 0; i < n; ++i){
      let u = str.charCodeAt(i);
      if(u>=0xd800 && u<=0xdfff){
        u = 0x10000 + ((u & 0x3FF) << 10) | (str.charCodeAt(++i) & 0x3FF);
      }
      if(u<=0x7f) ++len;
      else if(u<=0x7ff) len += 2;
      else if(u<=0xffff) len += 3;
      else len += 4;
    }
    return len;
  };

  /**
     Encodes the given JS string as UTF8 into the given TypedArray
     tgt, starting at the given offset and writing, at most, maxBytes
     bytes (including the NUL terminator if addNul is true, else no
     NUL is added). If it writes any bytes at all and addNul is true,
     it always NUL-terminates the output, even if doing so means that
     the NUL byte is all that it writes.

     If maxBytes is negative (the default) then it is treated as the
     remaining length of tgt, starting at the given offset.

     If writing the last character would surpass the maxBytes count
     because the character is multi-byte, that character will not be
     written (as opposed to writing a truncated multi-byte character).
     This can lead to it writing as many as 3 fewer bytes than
     maxBytes specifies.

     Returns the number of bytes written to the target, _including_
     the NUL terminator (if any). If it returns 0, it wrote nothing at
     all, which can happen if:

     - str is empty and addNul is false.
     - offset < 0.
     - maxBytes == 0.
     - maxBytes is less than the byte length of a multi-byte str[0].

     Throws if tgt is not an Int8Array or Uint8Array.

     Design notes:

     - In C's strcpy(), the destination pointer is the first
       argument. That is not the case here primarily because the 3rd+
       arguments are all referring to the destination, so it seems to
       make sense to have them grouped with it.

     - Emscripten's counterpart of this function (stringToUTF8Array())
       returns the number of bytes written sans NUL terminator. That
       is, however, ambiguous: str.length===0 or maxBytes===(0 or 1)
       all cause 0 to be returned.
  */
  target.jstrcpy = function(jstr, tgt, offset = 0, maxBytes = -1, addNul = true){
    /** Attribution: the encoding bits are taken from Emscripten's
        stringToUTF8Array(). */
    if(!tgt || (!(tgt instanceof Int8Array) && !(tgt instanceof Uint8Array))){
      toss("jstrcpy() target must be an Int8Array or Uint8Array.");
    }
    if(maxBytes<0) maxBytes = tgt.length - offset;
    if(!(maxBytes>0) || !(offset>=0)) return 0;
    let i = 0, max = jstr.length;
    const begin = offset, end = offset + maxBytes - (addNul ? 1 : 0);
    for(; i < max && offset < end; ++i){
      let u = jstr.charCodeAt(i);
      if(u>=0xd800 && u<=0xdfff){
        u = 0x10000 + ((u & 0x3FF) << 10) | (jstr.charCodeAt(++i) & 0x3FF);
      }
      if(u<=0x7f){
        if(offset >= end) break;
        tgt[offset++] = u;
      }else if(u<=0x7ff){
        if(offset + 1 >= end) break;
        tgt[offset++] = 0xC0 | (u >> 6);
        tgt[offset++] = 0x80 | (u & 0x3f);
      }else if(u<=0xffff){
        if(offset + 2 >= end) break;
        tgt[offset++] = 0xe0 | (u >> 12);
        tgt[offset++] = 0x80 | ((u >> 6) & 0x3f);
        tgt[offset++] = 0x80 | (u & 0x3f);
      }else{
        if(offset + 3 >= end) break;
        tgt[offset++] = 0xf0 | (u >> 18);
        tgt[offset++] = 0x80 | ((u >> 12) & 0x3f);
        tgt[offset++] = 0x80 | ((u >> 6) & 0x3f);
        tgt[offset++] = 0x80 | (u & 0x3f);
      }
    }
    if(addNul) tgt[offset++] = 0;
    return offset - begin;
  };

  /**
     Works similarly to C's strncpy(), copying, at most, n bytes (not
     characters) from srcPtr to tgtPtr. It copies until n bytes have
     been copied or a 0 byte is reached in src. _Unlike_ strncpy(), it
     returns the number of bytes it assigns in tgtPtr, _including_ the
     NUL byte (if any). If n is reached before a NUL byte in srcPtr,
     tgtPtr will _not_ be NULL-terminated. If a NUL byte is reached
     before n bytes are copied, tgtPtr will be NUL-terminated.

     If n is negative, cstrlen(srcPtr)+1 is used to calculate it, the
     +1 being for the NUL byte.

     Throws if tgtPtr or srcPtr are falsy. Results are undefined if:

     - either is not a pointer into the WASM heap or

     - srcPtr is not NUL-terminated AND n is less than srcPtr's
       logical length.

     ACHTUNG: it is possible to copy partial multi-byte characters
     this way, and converting such strings back to JS strings will
     have undefined results.
  */
  target.cstrncpy = function(tgtPtr, srcPtr, n){
    if(!tgtPtr || !srcPtr) toss("cstrncpy() does not accept NULL strings.");
    if(n<0) n = target.cstrlen(strPtr)+1;
    else if(!(n>0)) return 0;
    const heap = target.heap8u();
    let i = 0, ch;
    for(; i < n && (ch = heap[srcPtr+i]); ++i){
      heap[tgtPtr+i] = ch;
    }
    if(i<n) heap[tgtPtr + i++] = 0;
    return i;
  };

  /**
     For the given JS string, returns a Uint8Array of its contents
     encoded as UTF-8. If addNul is true, the returned array will have
     a trailing 0 entry, else it will not.
  */
  target.jstrToUintArray = (str, addNul=false)=>{
    return cache.utf8Encoder.encode(addNul ? (str+"\0") : str);
    // Or the hard way...
    /** Attribution: derived from Emscripten's stringToUTF8Array() */
    //const a = [], max = str.length;
    //let i = 0, pos = 0;
    //for(; i < max; ++i){
    //  let u = str.charCodeAt(i);
    //  if(u>=0xd800 && u<=0xdfff){
    //    u = 0x10000 + ((u & 0x3FF) << 10) | (str.charCodeAt(++i) & 0x3FF);
    //  }
    //  if(u<=0x7f) a[pos++] = u;
    //  else if(u<=0x7ff){
    //    a[pos++] = 0xC0 | (u >> 6);
    //    a[pos++] = 0x80 | (u & 63);
    //  }else if(u<=0xffff){
    //    a[pos++] = 0xe0 | (u >> 12);
    //    a[pos++] = 0x80 | ((u >> 6) & 63);
    //    a[pos++] = 0x80 | (u & 63);
    //  }else{
    //    a[pos++] = 0xf0 | (u >> 18);
    //    a[pos++] = 0x80 | ((u >> 12) & 63);
    //    a[pos++] = 0x80 | ((u >> 6) & 63);
    //    a[pos++] = 0x80 | (u & 63);
    //  }
    // }
    // return new Uint8Array(a);
  };

  const __affirmAlloc = (obj,funcName)=>{
    if(!(obj.alloc instanceof Function) ||
       !(obj.dealloc instanceof Function)){
      toss("Object is missing alloc() and/or dealloc() function(s)",
           "required by",funcName+"().");
    }
  };

  const __allocCStr = function(jstr, returnWithLength, allocator, funcName){
    __affirmAlloc(target, funcName);
    if('string'!==typeof jstr) return null;
    if(0){/* older impl, possibly more widely compatible? */
      const n = target.jstrlen(jstr),
            ptr = allocator(n+1);
      target.jstrcpy(jstr, target.heap8u(), ptr, n+1, true);
      return returnWithLength ? [ptr, n] : ptr;
    }else{/* newer, (probably) faster and (certainly) simpler impl */
      const u = cache.utf8Encoder.encode(jstr),
            ptr = allocator(u.length+1),
            heap = heapWrappers().HEAP8U;
      heap.set(u, ptr);
      heap[ptr + u.length] = 0;
      return returnWithLength ? [ptr, u.length] : ptr;
    }
  };

  /**
     Uses target.alloc() to allocate enough memory for jstrlen(jstr)+1
     bytes of memory, copies jstr to that memory using jstrcpy(),
     NUL-terminates it, and returns the pointer to that C-string.
     Ownership of the pointer is transfered to the caller, who must
     eventually pass the pointer to dealloc() to free it.

     If passed a truthy 2nd argument then its return semantics change:
     it returns [ptr,n], where ptr is the C-string's pointer and n is
     its cstrlen().

     Throws if `target.alloc` or `target.dealloc` are not functions.
  */
  target.allocCString =
    (jstr, returnWithLength=false)=>__allocCStr(jstr, returnWithLength,
                                                target.alloc, 'allocCString()');

  /**
     Starts an "allocation scope." All allocations made using
     scopedAlloc() are recorded in this scope and are freed when the
     value returned from this function is passed to
     scopedAllocPop().

     This family of functions requires that the API's object have both
     `alloc()` and `dealloc()` methods, else this function will throw.

     Intended usage:

     ```
     const scope = scopedAllocPush();
     try {
       const ptr1 = scopedAlloc(100);
       const ptr2 = scopedAlloc(200);
       const ptr3 = scopedAlloc(300);
       ...
       // Note that only allocations made via scopedAlloc()
       // are managed by this allocation scope.
     }finally{
       scopedAllocPop(scope);
     }
     ```

     The value returned by this function must be treated as opaque by
     the caller, suitable _only_ for passing to scopedAllocPop().
     Its type and value are not part of this function's API and may
     change in any given version of this code.

     `scopedAlloc.level` can be used to determine how many scoped
     alloc levels are currently active.
   */
  target.scopedAllocPush = function(){
    __affirmAlloc(target, 'scopedAllocPush');
    const a = [];
    cache.scopedAlloc.push(a);
    return a;
  };

  /**
     Cleans up all allocations made using scopedAlloc() in the context
     of the given opaque state object, which must be a value returned
     by scopedAllocPush(). See that function for an example of how to
     use this function.

     Though scoped allocations are managed like a stack, this API
     behaves properly if allocation scopes are popped in an order
     other than the order they were pushed.

     If called with no arguments, it pops the most recent
     scopedAllocPush() result:

     ```
     scopedAllocPush();
     try{ ... } finally { scopedAllocPop(); }
     ```

     It's generally recommended that it be passed an explicit argument
     to help ensure that push/push are used in matching pairs, but in
     trivial code that may be a non-issue.
  */
  target.scopedAllocPop = function(state){
    __affirmAlloc(target, 'scopedAllocPop');
    const n = arguments.length
          ? cache.scopedAlloc.indexOf(state)
          : cache.scopedAlloc.length-1;
    if(n<0) toss("Invalid state object for scopedAllocPop().");
    if(0===arguments.length) state = cache.scopedAlloc[n];
    cache.scopedAlloc.splice(n,1);
    for(let p; (p = state.pop()); ){
      if(target.functionEntry(p)){
        //console.warn("scopedAllocPop() uninstalling function",p);
        target.uninstallFunction(p);
      }
      else target.dealloc(p);
    }
  };

  /**
     Allocates n bytes of memory using this.alloc() and records that
     fact in the state for the most recent call of scopedAllocPush().
     Ownership of the memory is given to scopedAllocPop(), which
     will clean it up when it is called. The memory _must not_ be
     passed to this.dealloc(). Throws if this API object is missing
     the required `alloc()` or `dealloc()` functions or no scoped
     alloc is active.

     See scopedAllocPush() for an example of how to use this function.

     The `level` property of this function can be queried to query how
     many scoped allocation levels are currently active.

     See also: scopedAllocPtr(), scopedAllocCString()
  */
  target.scopedAlloc = function(n){
    if(!cache.scopedAlloc.length){
      toss("No scopedAllocPush() scope is active.");
    }
    const p = target.alloc(n);
    cache.scopedAlloc[cache.scopedAlloc.length-1].push(p);
    return p;
  };

  Object.defineProperty(target.scopedAlloc, 'level', {
    configurable: false, enumerable: false,
    get: ()=>cache.scopedAlloc.length,
    set: ()=>toss("The 'active' property is read-only.")
  });

  /**
     Works identically to allocCString() except that it allocates the
     memory using scopedAlloc().

     Will throw if no scopedAllocPush() call is active.
  */
  target.scopedAllocCString =
    (jstr, returnWithLength=false)=>__allocCStr(jstr, returnWithLength,
                                                target.scopedAlloc, 'scopedAllocCString()');

  // impl for allocMainArgv() and scopedAllocMainArgv().
  const __allocMainArgv = function(isScoped, list){
    const pList = target[
      isScoped ? 'scopedAlloc' : 'alloc'
    ]((list.length + 1) * target.ptrSizeof);
    let i = 0;
    list.forEach((e)=>{
      target.pokePtr(pList + (target.ptrSizeof * i++),
                         target[
                           isScoped ? 'scopedAllocCString' : 'allocCString'
                         ](""+e));
    });
    target.pokePtr(pList + (target.ptrSizeof * i), 0);
    return pList;
  };

  /**
     Creates an array, using scopedAlloc(), suitable for passing to a
     C-level main() routine. The input is a collection with a length
     property and a forEach() method. A block of memory
     (list.length+1) entries long is allocated and each pointer-sized
     block of that memory is populated with a scopedAllocCString()
     conversion of the (""+value) of each element, with the exception
     that the final entry is a NULL pointer. Returns a pointer to the
     start of the list, suitable for passing as the 2nd argument to a
     C-style main() function.

     Throws if scopedAllocPush() is not active.

     Design note: the returned array is allocated with an extra NULL
     pointer entry to accommodate certain APIs, but client code which
     does not need that functionality should treat the returned array
     as list.length entries long.
  */
  target.scopedAllocMainArgv = (list)=>__allocMainArgv(true, list);

  /**
     Identical to scopedAllocMainArgv() but uses alloc() instead of
     scopedAlloc().
  */
  target.allocMainArgv = (list)=>__allocMainArgv(false, list);

  /**
     Expects to be given a C-style string array and its length. It
     returns a JS array of strings and/or nulls: any entry in the
     pArgv array which is NULL results in a null entry in the result
     array. If argc is 0 then an empty array is returned.

     Results are undefined if any entry in the first argc entries of
     pArgv are neither 0 (NULL) nor legal UTF-format C strings.

     To be clear, the expected C-style arguments to be passed to this
     function are `(int, char **)` (optionally const-qualified).
  */
  target.cArgvToJs = (argc, pArgv)=>{
    const list = [];
    for(let i = 0; i < argc; ++i){
      const arg = target.peekPtr(pArgv + (target.ptrSizeof * i));
      list.push( arg ? target.cstrToJs(arg) : null );
    }
    return list;
  };

  /**
     Wraps function call func() in a scopedAllocPush() and
     scopedAllocPop() block, such that all calls to scopedAlloc() and
     friends from within that call will have their memory freed
     automatically when func() returns. If func throws or propagates
     an exception, the scope is still popped, otherwise it returns the
     result of calling func().
  */
  target.scopedAllocCall = function(func){
    target.scopedAllocPush();
    try{ return func() } finally{ target.scopedAllocPop() }
  };

  /** Internal impl for allocPtr() and scopedAllocPtr(). */
  const __allocPtr = function(howMany, safePtrSize, method){
    __affirmAlloc(target, method);
    const pIr = safePtrSize ? 'i64' : ptrIR;
    let m = target[method](howMany * (safePtrSize ? 8 : ptrSizeof));
    target.poke(m, 0, pIr)
    if(1===howMany){
      return m;
    }
    const a = [m];
    for(let i = 1; i < howMany; ++i){
      m += (safePtrSize ? 8 : ptrSizeof);
      a[i] = m;
      target.poke(m, 0, pIr);
    }
    return a;
  };

  /**
     Allocates one or more pointers as a single chunk of memory and
     zeroes them out.

     The first argument is the number of pointers to allocate. The
     second specifies whether they should use a "safe" pointer size (8
     bytes) or whether they may use the default pointer size
     (typically 4 but also possibly 8).

     How the result is returned depends on its first argument: if
     passed 1, it returns the allocated memory address. If passed more
     than one then an array of pointer addresses is returned, which
     can optionally be used with "destructuring assignment" like this:

     ```
     const [p1, p2, p3] = allocPtr(3);
     ```

     ACHTUNG: when freeing the memory, pass only the _first_ result
     value to dealloc(). The others are part of the same memory chunk
     and must not be freed separately.

     The reason for the 2nd argument is..

     When one of the returned pointers will refer to a 64-bit value,
     e.g. a double or int64, an that value must be written or fetched,
     e.g. using poke() or peek(), it is important that
     the pointer in question be aligned to an 8-byte boundary or else
     it will not be fetched or written properly and will corrupt or
     read neighboring memory. It is only safe to pass false when the
     client code is certain that it will only get/fetch 4-byte values
     (or smaller).
  */
  target.allocPtr =
    (howMany=1, safePtrSize=true)=>__allocPtr(howMany, safePtrSize, 'alloc');

  /**
     Identical to allocPtr() except that it allocates using scopedAlloc()
     instead of alloc().
  */
  target.scopedAllocPtr =
    (howMany=1, safePtrSize=true)=>__allocPtr(howMany, safePtrSize, 'scopedAlloc');

  /**
     If target.exports[name] exists, it is returned, else an
     exception is thrown.
  */
  target.xGet = function(name){
    return target.exports[name] || toss("Cannot find exported symbol:",name);
  };

  const __argcMismatch =
        (f,n)=>toss(f+"() requires",n,"argument(s).");

  /**
     Looks up a WASM-exported function named fname from
     target.exports. If found, it is called, passed all remaining
     arguments, and its return value is returned to xCall's caller. If
     not found, an exception is thrown. This function does no
     conversion of argument or return types, but see xWrap() and
     xCallWrapped() for variants which do.

     As a special case, if passed only 1 argument after the name and
     that argument in an Array, that array's entries become the
     function arguments. (This is not an ambiguous case because it's
     not legal to pass an Array object to a WASM function.)
  */
  target.xCall = function(fname, ...args){
    const f = target.xGet(fname);
    if(!(f instanceof Function)) toss("Exported symbol",fname,"is not a function.");
    if(f.length!==args.length) __argcMismatch(fname,f.length)
    /* This is arguably over-pedantic but we want to help clients keep
       from shooting themselves in the foot when calling C APIs. */;
    return (2===arguments.length && Array.isArray(arguments[1]))
      ? f.apply(null, arguments[1])
      : f.apply(null, args);
  };

  /**
     State for use with xWrap()
  */
  cache.xWrap = Object.create(null);
  cache.xWrap.convert = Object.create(null);
  /** Map of type names to argument conversion functions. */
  cache.xWrap.convert.arg = new Map;
  /** Map of type names to return result conversion functions. */
  cache.xWrap.convert.result = new Map;
  const xArg = cache.xWrap.convert.arg, xResult = cache.xWrap.convert.result;

  if(target.bigIntEnabled){
    xArg.set('i64', (i)=>BigInt(i));
  }
  const __xArgPtr = 'i32' === ptrIR
        ? ((i)=>(i | 0)) : ((i)=>(BigInt(i) | BigInt(0)));
  xArg.set('i32', __xArgPtr )
    .set('i16', (i)=>((i | 0) & 0xFFFF))
    .set('i8', (i)=>((i | 0) & 0xFF))
    .set('f32', (i)=>Number(i).valueOf())
    .set('float', xArg.get('f32'))
    .set('f64', xArg.get('f32'))
    .set('double', xArg.get('f64'))
    .set('int', xArg.get('i32'))
    .set('null', (i)=>i)
    .set(null, xArg.get('null'))
    .set('**', __xArgPtr)
    .set('*', __xArgPtr);
  xResult.set('*', __xArgPtr)
    .set('pointer', __xArgPtr)
    .set('number', (v)=>Number(v))
    .set('void', (v)=>undefined)
    .set('null', (v)=>v)
    .set(null, xResult.get('null'));

  { /* Copy certain xArg[...] handlers to xResult[...] and
       add pointer-style variants of them. */
    const copyToResult = ['i8', 'i16', 'i32', 'int',
                          'f32', 'float', 'f64', 'double'];
    if(target.bigIntEnabled) copyToResult.push('i64');
    const adaptPtr = xArg.get(ptrIR);
    for(const t of copyToResult){
      xArg.set(t+'*', adaptPtr);
      xResult.set(t+'*', adaptPtr);
      xResult.set(t, (xArg.get(t) || toss("Missing arg converter:",t)));
    }
  }

  /**
     In order for args of type string to work in various contexts in
     the sqlite3 API, we need to pass them on as, variably, a C-string
     or a pointer value. Thus for ARGs of type 'string' and
     '*'/'pointer' we behave differently depending on whether the
     argument is a string or not:

     - If v is a string, scopeAlloc() a new C-string from it and return
       that temp string's pointer.

     - Else return the value from the arg adapter defined for ptrIR.

     TODO? Permit an Int8Array/Uint8Array and convert it to a string?
     Would that be too much magic concentrated in one place, ready to
     backfire? We handle that at the client level in sqlite3 with a
     custom argument converter.
  */
  const __xArgString = function(v){
    if('string'===typeof v) return target.scopedAllocCString(v);
    return v ? __xArgPtr(v) : null;
  };
  xArg.set('string', __xArgString)
    .set('utf8', __xArgString)
    .set('pointer', __xArgString);
  //xArg.set('*', __xArgString);

  xResult.set('string', (i)=>target.cstrToJs(i))
    .set('utf8', xResult.get('string'))
    .set('string:dealloc', (i)=>{
      try { return i ? target.cstrToJs(i) : null }
      finally{ target.dealloc(i) }
    })
    .set('utf8:dealloc', xResult.get('string:dealloc'))
    .set('json', (i)=>JSON.parse(target.cstrToJs(i)))
    .set('json:dealloc', (i)=>{
      try{ return i ? JSON.parse(target.cstrToJs(i)) : null }
      finally{ target.dealloc(i) }
    });

  /**
     Internal-use-only base class for FuncPtrAdapter and potentially
     additional stateful argument adapter classes.

     Note that its main interface (convertArg()) is strictly
     internal, not to be exposed to client code, as it may still
     need re-shaping. Only the constructors of concrete subclasses
     should be exposed to clients, and those in such a way that
     does not hinder internal redesign of the convertArg()
     interface.
  */
  const AbstractArgAdapter = class {
    constructor(opt){
      this.name = opt.name || 'unnamed adapter';
    }
    /**
       Gets called via xWrap() to "convert" v to whatever type
       this specific class supports.

       argIndex is the argv index of _this_ argument in the
       being-xWrap()'d call. argv is the current argument list
       undergoing xWrap() argument conversion. argv entries to the
       left of argIndex will have already undergone transformation and
       those to the right will not have (they will have the values the
       client-level code passed in, awaiting conversion). The RHS
       indexes must never be relied upon for anything because their
       types are indeterminate, whereas the LHS values will be
       WASM-compatible values by the time this is called.
    */
    convertArg(v,argv,argIndex){
      toss("AbstractArgAdapter must be subclassed.");
    }
  };

  /**
     An attempt at adding function pointer conversion support to
     xWrap(). This type is recognized by xWrap() as a proxy for
     converting a JS function to a C-side function, either
     permanently, for the duration of a single call into the C layer,
     or semi-contextual, where it may keep track of a single binding
     for a given context and uninstall the binding if it's replaced.

     The constructor requires an options object with these properties:

     - name (optional): string describing the function binding. This
       is solely for debugging and error-reporting purposes. If not
       provided, an empty string is assumed.

     - signature: a function signature string compatible with
       jsFuncToWasm().

     - bindScope (string): one of ('transient', 'context',
       'singleton'). Bind scopes are:

       - 'transient': it will convert JS functions to WASM only for
         the duration of the xWrap()'d function call, using
         scopedInstallFunction(). Before that call returns, the
         WASM-side binding will be uninstalled.

       - 'singleton': holds one function-pointer binding for this
         instance. If it's called with a different function pointer,
         it uninstalls the previous one after converting the new
         value. This is only useful for use with "global" functions
         which do not rely on any state other than this function
         pointer. If the being-converted function pointer is intended
         to be mapped to some sort of state object (e.g. an
         `sqlite3*`) then "context" (see below) is the proper mode.

       - 'context': similar to singleton mode but for a given
         "context", where the context is a key provided by the user
         and possibly dependent on a small amount of call-time
         context. This mode is the default if bindScope is _not_ set
         but a property named contextKey (described below) is.

       - 'permanent': the function is installed and left there
         forever. There is no way to recover its pointer address
         later on.

     - callProxy (function): if set, this must be a function which
       will act as a proxy for any "converted" JS function. It is
       passed the being-converted function value and must return
       either that function or a function which acts on its
       behalf. The returned function will be the one which gets
       installed into the WASM function table. The proxy must perform
       any required argument conversion (noting that it will be called
       from C code, so will receive C-format arguments) before passing
       them on to the being-converted function. Whether or not the
       proxy itself must return a value depends on the context. If it
       does, it must be a WASM-friendly value, as it will be returning
       from a call made from native code.

     - contextKey (function): is only used if bindScope is 'context'
       or if bindScope is not set and this function is, in which case
       'context' is assumed. This function gets bound to this object,
       so its "this" is this object. It gets passed (argv,argIndex),
       where argIndex is the index of _this_ function pointer in its
       _wrapping_ function's arguments and argv is the _current_
       still-being-xWrap()-processed args array. All arguments to the
       left of argIndex will have been processed by xWrap() by the
       time this is called. argv[argIndex] will be the value the user
       passed in to the xWrap()'d function for the argument this
       FuncPtrAdapter is mapped to. Arguments to the right of
       argv[argIndex] will not yet have been converted before this is
       called. The function must return a key which uniquely
       identifies this function mapping context for _this_
       FuncPtrAdapter instance (other instances are not considered),
       taking into account that C functions often take some sort of
       state object as one or more of their arguments. As an example,
       if the xWrap()'d function takes `(int,T*,functionPtr,X*)` and
       this FuncPtrAdapter is the argv[2]nd arg, contextKey(argv,2)
       might return 'T@'+argv[1], or even just argv[1].  Note,
       however, that the (X*) argument will not yet have been
       processed by the time this is called and should not be used as
       part of that key because its pre-conversion data type might be
       unpredictable. Similarly, care must be taken with C-string-type
       arguments: those to the left in argv will, when this is called,
       be WASM pointers, whereas those to the right might (and likely
       do) have another data type. When using C-strings in keys, never
       use their pointers in the key because most C-strings in this
       constellation are transient.

     Yes, that ^^^ is quite awkward, but it's what we have.

     The constructor only saves the above state for later, and does
     not actually bind any functions. Its convertArg() method is
     called via xWrap() to perform any bindings.

     Shortcomings:

     - These "reverse" bindings, i.e. calling into a JS-defined
       function from a WASM-defined function (the generated proxy
       wrapper), lack all type conversion support. That means, for
       example, that...

     - Function pointers which include C-string arguments may still
       need a level of hand-written wrappers around them, depending on
       how they're used, in order to provide the client with JS
       strings. Alternately, clients will need to perform such conversions
       on their own, e.g. using cstrtojs(). Or maybe we can find a way
       to perform such conversions here, via addition of an xWrap()-style
       function signature to the options argument.
  */
  xArg.FuncPtrAdapter = class FuncPtrAdapter extends AbstractArgAdapter {
    constructor(opt) {
      super(opt);
      if(xArg.FuncPtrAdapter.warnOnUse){
        console.warn('xArg.FuncPtrAdapter is an internal-only API',
                     'and is not intended to be invoked from',
                     'client-level code. Invoked with:',opt);
      }
      this.name = opt.name || "unnamed";
      this.signature = opt.signature;
      if(opt.contextKey instanceof Function){
        this.contextKey = opt.contextKey;
        if(!opt.bindScope) opt.bindScope = 'context';
      }
      this.bindScope = opt.bindScope
        || toss("FuncPtrAdapter options requires a bindScope (explicit or implied).");
      if(FuncPtrAdapter.bindScopes.indexOf(opt.bindScope)<0){
        toss("Invalid options.bindScope ("+opt.bindMod+") for FuncPtrAdapter. "+
             "Expecting one of: ("+FuncPtrAdapter.bindScopes.join(', ')+')');
      }
      this.isTransient = 'transient'===this.bindScope;
      this.isContext = 'context'===this.bindScope;
      this.isPermanent = 'permanent'===this.bindScope;
      this.singleton = ('singleton'===this.bindScope) ? [] : undefined;
      //console.warn("FuncPtrAdapter()",opt,this);
      this.callProxy = (opt.callProxy instanceof Function)
        ? opt.callProxy : undefined;
    }

    /**
       Note that static class members are defined outside of the class
       to work around an emcc toolchain build problem: one of the
       tools in emsdk v3.1.42 does not support the static keyword.
    */

    /* Dummy impl. Overwritten per-instance as needed. */
    contextKey(argv,argIndex){
      return this;
    }

    /* Returns this objects mapping for the given context key, in the
       form of an an array, creating the mapping if needed. The key
       may be anything suitable for use in a Map. */
    contextMap(key){
      const cm = (this.__cmap || (this.__cmap = new Map));
      let rc = cm.get(key);
      if(undefined===rc) cm.set(key, (rc = []));
      return rc;
    }

    /**
       Gets called via xWrap() to "convert" v to a WASM-bound function
       pointer. If v is one of (a pointer, null, undefined) then
       (v||0) is returned and any earlier function installed by this
       mapping _might_, depending on how it's bound, be uninstalled.
       If v is not one of those types, it must be a Function, for
       which it creates (if needed) a WASM function binding and
       returns the WASM pointer to that binding. If this instance is
       not in 'transient' mode, it will remember the binding for at
       least the next call, to avoid recreating the function binding
       unnecessarily.

       If it's passed a pointer(ish) value for v, it does _not_
       perform any function binding, so this object's bindMode is
       irrelevant for such cases.

       See the parent class's convertArg() docs for details on what
       exactly the 2nd and 3rd arguments are.
    */
    convertArg(v,argv,argIndex){
      //FuncPtrAdapter.debugOut("FuncPtrAdapter.convertArg()",this.name,this.signature,this.transient,v);
      let pair = this.singleton;
      if(!pair && this.isContext){
        pair = this.contextMap(this.contextKey(argv,argIndex));
        //FuncPtrAdapter.debugOut(this.name, this.signature, "contextKey() =",this.contextKey(argv,argIndex), pair);
      }
      if(pair && pair[0]===v) return pair[1];
      if(v instanceof Function){
        /* Install a WASM binding and return its pointer. */
        //FuncPtrAdapter.debugOut("FuncPtrAdapter.convertArg()",this.name,this.signature,this.transient,v,pair);
        if(this.callProxy) v = this.callProxy(v);
        const fp = __installFunction(v, this.signature, this.isTransient);
        if(FuncPtrAdapter.debugFuncInstall){
          FuncPtrAdapter.debugOut("FuncPtrAdapter installed", this,
                                  this.contextKey(argv,argIndex), '@'+fp, v);
        }
        if(pair){
          /* Replace existing stashed mapping */
          if(pair[1]){
            if(FuncPtrAdapter.debugFuncInstall){
              FuncPtrAdapter.debugOut("FuncPtrAdapter uninstalling", this,
                                      this.contextKey(argv,argIndex), '@'+pair[1], v);
            }
            try{
              /* Because the pending native call might rely on the
                 pointer we're replacing, e.g. as is normally the case
                 with sqlite3's xDestroy() methods, we don't
                 immediately uninstall but instead add its pointer to
                 the scopedAlloc stack, which will be cleared when the
                 xWrap() mechanism is done calling the native
                 function. We're relying very much here on xWrap()
                 having pushed an alloc scope.
              */
              cache.scopedAlloc[cache.scopedAlloc.length-1].push(pair[1]);
            }
            catch(e){/*ignored*/}
          }
          pair[0] = v;
          pair[1] = fp;
        }
        return fp;
      }else if(target.isPtr(v) || null===v || undefined===v){
        //FuncPtrAdapter.debugOut("FuncPtrAdapter.convertArg()",this.name,this.signature,this.transient,v,pair);
        if(pair && pair[1] && pair[1]!==v){
          /* uninstall stashed mapping and replace stashed mapping with v. */
          if(FuncPtrAdapter.debugFuncInstall){
            FuncPtrAdapter.debugOut("FuncPtrAdapter uninstalling", this,
                                    this.contextKey(argv,argIndex), '@'+pair[1], v);
          }
          try{ cache.scopedAlloc[cache.scopedAlloc.length-1].push(pair[1]) }
          catch(e){/*ignored*/}
          pair[0] = pair[1] = (v | 0);
        }
        return v || 0;
      }else{
        throw new TypeError("Invalid FuncPtrAdapter argument type. "+
                            "Expecting a function pointer or a "+
                            (this.name ? this.name+' ' : '')+
                            "function matching signature "+
                            this.signature+".");
      }
    }/*convertArg()*/
  }/*FuncPtrAdapter*/;

  /** If true, the constructor emits a warning. The intent is that
      this be set to true after bootstrapping of the higher-level
      client library is complete, to warn downstream clients that
      they shouldn't be relying on this implemenation detail which
      does not have a stable interface. */
  xArg.FuncPtrAdapter.warnOnUse = false;

  /** If true, convertArg() will FuncPtrAdapter.debugOut() when it
      (un)installs a function binding to/from WASM. Note that
      deinstallation of bindScope=transient bindings happens
      via scopedAllocPop() so will not be output. */
  xArg.FuncPtrAdapter.debugFuncInstall = false;

  /** Function used for debug output. */
  xArg.FuncPtrAdapter.debugOut = console.debug.bind(console);

  xArg.FuncPtrAdapter.bindScopes = [
    'transient', 'context', 'singleton', 'permanent'
  ];

  const __xArgAdapterCheck =
        (t)=>xArg.get(t) || toss("Argument adapter not found:",t);

  const __xResultAdapterCheck =
        (t)=>xResult.get(t) || toss("Result adapter not found:",t);

  cache.xWrap.convertArg = (t,...args)=>__xArgAdapterCheck(t)(...args);
  cache.xWrap.convertArgNoCheck = (t,...args)=>xArg.get(t)(...args);

  cache.xWrap.convertResult =
    (t,v)=>(null===t ? v : (t ? __xResultAdapterCheck(t)(v) : undefined));
  cache.xWrap.convertResultNoCheck =
    (t,v)=>(null===t ? v : (t ? xResult.get(t)(v) : undefined));

  /**
     Creates a wrapper for another function which converts the arguments
     of the wrapper to argument types accepted by the wrapped function,
     then converts the wrapped function's result to another form
     for the wrapper.

     The first argument must be one of:

     - A JavaScript function.
     - The name of a WASM-exported function. In the latter case xGet()
       is used to fetch the exported function, which throws if it's not
       found.
     - A pointer into the indirect function table. e.g. a pointer
       returned from target.installFunction().

     It returns either the passed-in function or a wrapper for that
     function which converts the JS-side argument types into WASM-side
     types and converts the result type.

     The second argument, `resultType`, describes the conversion for
     the wrapped functions result. A literal `null` or the string
     `'null'` both mean to return the original function's value as-is
     (mnemonic: there is "null" conversion going on). Literal
     `undefined` or the string `"void"` both mean to ignore the
     function's result and return `undefined`. Aside from those two
     special cases, the `resultType` value may be one of the values
     described below or any mapping installed by the client using
     xWrap.resultAdapter().

     If passed 3 arguments and the final one is an array, that array
     must contain a list of type names (see below) for adapting the
     arguments from JS to WASM.  If passed 2 arguments, more than 3,
     or the 3rd is not an array, all arguments after the 2nd (if any)
     are treated as type names. i.e.:

     ```
     xWrap('funcname', 'i32', 'string', 'f64');
     // is equivalent to:
     xWrap('funcname', 'i32', ['string', 'f64']);
     ```

     This function enforces that the given list of arguments has the
     same arity as the being-wrapped function (as defined by its
     `length` property) and it will throw if that is not the case.
     Similarly, the created wrapper will throw if passed a differing
     argument count.

     Type names are symbolic names which map the arguments to an
     adapter function to convert, if needed, the value before passing
     it on to WASM or to convert a return result from WASM. The list
     of built-in names:

     - `i8`, `i16`, `i32` (args and results): all integer conversions
       which convert their argument to an integer and truncate it to
       the given bit length.

     - `N*` (args): a type name in the form `N*`, where N is a numeric
       type name, is treated the same as WASM pointer.

     - `*` and `pointer` (args): are assumed to be WASM pointer values
       and are returned coerced to an appropriately-sized pointer
       value (i32 or i64). Non-numeric values will coerce to 0 and
       out-of-range values will have undefined results (just as with
       any pointer misuse).

     - `*` and `pointer` (results): aliases for the current
       WASM pointer numeric type.

     - `**` (args): is simply a descriptive alias for the WASM pointer
       type. It's primarily intended to mark output-pointer arguments.

     - `i64` (args and results): passes the value to BigInt() to
       convert it to an int64. Only available if bigIntEnabled is
       true.

     - `f32` (`float`), `f64` (`double`) (args and results): pass
       their argument to Number(). i.e. the adapter does not currently
       distinguish between the two types of floating-point numbers.

     - `number` (results): converts the result to a JS Number using
       Number(theValue).valueOf(). Note that this is for result
       conversions only, as it's not possible to generically know
       which type of number to convert arguments to.

     Non-numeric conversions include:

     - `null` literal or `"null"` string (args and results): perform
       no translation and pass the arg on as-is. This is primarily
       useful for results but may have a use or two for arguments.

     - `string` or `utf8` (args): has two different semantics in order
       to accommodate various uses of certain C APIs
       (e.g. output-style strings)...

       - If the arg is a string, it creates a _temporary_
         UTF-8-encoded C-string to pass to the exported function,
         cleaning it up before the wrapper returns. If a long-lived
         C-string pointer is required, that requires client-side code
         to create the string, then pass its pointer to the function.

       - Else the arg is assumed to be a pointer to a string the
         client has already allocated and it's passed on as
         a WASM pointer.

     - `string` or `utf8` (results): treats the result value as a
       const C-string, encoded as UTF-8, copies it to a JS string,
       and returns that JS string.

     - `string:dealloc` or `utf8:dealloc) (results): treats the result value
       as a non-const UTF-8 C-string, ownership of which has just been
       transfered to the caller. It copies the C-string to a JS
       string, frees the C-string, and returns the JS string. If such
       a result value is NULL, the JS result is `null`. Achtung: when
       using an API which returns results from a specific allocator,
       e.g. `my_malloc()`, this conversion _is not legal_. Instead, an
       equivalent conversion which uses the appropriate deallocator is
       required. For example:

```js
   target.xWrap.resultAdapter('string:my_free',(i)=>{
      try { return i ? target.cstrToJs(i) : null }
      finally{ target.exports.my_free(i) }
   };
```

     - `json` (results): treats the result as a const C-string and
       returns the result of passing the converted-to-JS string to
       JSON.parse(). Returns `null` if the C-string is a NULL pointer.

     - `json:dealloc` (results): works exactly like `string:dealloc` but
       returns the same thing as the `json` adapter. Note the
       warning in `string:dealloc` regarding maching allocators and
       deallocators.

     The type names for results and arguments are validated when
     xWrap() is called and any unknown names will trigger an
     exception.

     Clients may map their own result and argument adapters using
     xWrap.resultAdapter() and xWrap.argAdapter(), noting that not all
     type conversions are valid for both arguments _and_ result types
     as they often have different memory ownership requirements.

     Design note: the ability to pass in a JS function as the first
     argument is of relatively limited use, primarily for testing
     argument and result converters. JS functions, by and large, will
     not want to deal with C-type arguments.

     TODOs:

     - Figure out how/whether we can (semi-)transparently handle
       pointer-type _output_ arguments. Those currently require
       explicit handling by allocating pointers, assigning them before
       the call using poke(), and fetching them with
       peek() after the call. We may be able to automate some
       or all of that.

     - Figure out whether it makes sense to extend the arg adapter
       interface such that each arg adapter gets an array containing
       the results of the previous arguments in the current call. That
       might allow some interesting type-conversion feature. Use case:
       handling of the final argument to sqlite3_prepare_v2() depends
       on the type (pointer vs JS string) of its 2nd
       argument. Currently that distinction requires hand-writing a
       wrapper for that function. That case is unusual enough that
       abstracting it into this API (and taking on the associated
       costs) may well not make good sense.
  */
  target.xWrap = function(fArg, resultType, ...argTypes){
    if(3===arguments.length && Array.isArray(arguments[2])){
      argTypes = arguments[2];
    }
    if(target.isPtr(fArg)){
      fArg = target.functionEntry(fArg)
        || toss("Function pointer not found in WASM function table.");
    }
    const fIsFunc = (fArg instanceof Function);
    const xf = fIsFunc ? fArg : target.xGet(fArg);
    if(fIsFunc) fArg = xf.name || 'unnamed function';
    if(argTypes.length!==xf.length) __argcMismatch(fArg, xf.length);
    if((null===resultType) && 0===xf.length){
      /* Func taking no args with an as-is return. We don't need a wrapper.
         We forego the argc check here, though. */
      return xf;
    }
    /*Verify the arg type conversions are valid...*/;
    if(undefined!==resultType && null!==resultType) __xResultAdapterCheck(resultType);
    for(const t of argTypes){
      if(t instanceof AbstractArgAdapter) xArg.set(t, (...args)=>t.convertArg(...args));
      else __xArgAdapterCheck(t);
    }
    const cxw = cache.xWrap;
    if(0===xf.length){
      // No args to convert, so we can create a simpler wrapper...
      return (...args)=>(args.length
                         ? __argcMismatch(fArg, xf.length)
                         : cxw.convertResult(resultType, xf.call(null)));
    }
    return function(...args){
      if(args.length!==xf.length) __argcMismatch(fArg, xf.length);
      const scope = target.scopedAllocPush();
      try{
        /*
          Maintenance reminder re. arguments passed to convertArg():
          The public interface of argument adapters is that they take
          ONE argument and return a (possibly) converted result for
          it. The passing-on of arguments after the first is an
          internal implementation detail for the sake of
          AbstractArgAdapter, and not to be relied on or documented
          for other cases. The fact that this is how
          AbstractArgAdapter.convertArgs() gets its 2nd+ arguments,
          and how FuncPtrAdapter.contextKey() gets its args, is also
          an implementation detail and subject to change. i.e. the
          public interface of 1 argument is stable.  The fact that any
          arguments may be passed in after that one, and what those
          arguments are, is _not_ part of the public interface and is
          _not_ stable.
        */
        for(const i in args) args[i] = cxw.convertArgNoCheck(
          argTypes[i], args[i], args, i
        );
        return cxw.convertResultNoCheck(resultType, xf.apply(null,args));
      }finally{
        target.scopedAllocPop(scope);
      }
    };
  }/*xWrap()*/;

  /** Internal impl for xWrap.resultAdapter() and argAdapter(). */
  const __xAdapter = function(func, argc, typeName, adapter, modeName, xcvPart){
    if('string'===typeof typeName){
      if(1===argc) return xcvPart.get(typeName);
      else if(2===argc){
        if(!adapter){
          delete xcvPart.get(typeName);
          return func;
        }else if(!(adapter instanceof Function)){
          toss(modeName,"requires a function argument.");
        }
        xcvPart.set(typeName, adapter);
        return func;
      }
    }
    toss("Invalid arguments to",modeName);
  };

  /**
     Gets, sets, or removes a result value adapter for use with
     xWrap(). If passed only 1 argument, the adapter function for the
     given type name is returned.  If the second argument is explicit
     falsy (as opposed to defaulted), the adapter named by the first
     argument is removed. If the 2nd argument is not falsy, it must be
     a function which takes one value and returns a value appropriate
     for the given type name. The adapter may throw if its argument is
     not of a type it can work with. This function throws for invalid
     arguments.

     Example:

     ```
     xWrap.resultAdapter('twice',(v)=>v+v);
     ```

     xWrap.resultAdapter() MUST NOT use the scopedAlloc() family of
     APIs to allocate a result value. xWrap()-generated wrappers run
     in the context of scopedAllocPush() so that argument adapters can
     easily convert, e.g., to C-strings, and have them cleaned up
     automatically before the wrapper returns to the caller. Likewise,
     if a _result_ adapter uses scoped allocation, the result will be
     freed before because they would be freed before the wrapper
     returns, leading to chaos and undefined behavior.

     Except when called as a getter, this function returns itself.
  */
  target.xWrap.resultAdapter = function f(typeName, adapter){
    return __xAdapter(f, arguments.length, typeName, adapter,
                      'resultAdapter()', xResult);
  };

  /**
     Functions identically to xWrap.resultAdapter() but applies to
     call argument conversions instead of result value conversions.

     xWrap()-generated wrappers perform argument conversion in the
     context of a scopedAllocPush(), so any memory allocation
     performed by argument adapters really, really, really should be
     made using the scopedAlloc() family of functions unless
     specifically necessary. For example:

     ```
     xWrap.argAdapter('my-string', function(v){
       return ('string'===typeof v)
         ? myWasmObj.scopedAllocCString(v) : null;
     };
     ```

     Contrariwise, xWrap.resultAdapter() must _not_ use scopedAlloc()
     to allocate its results because they would be freed before the
     xWrap()-created wrapper returns.

     Note that it is perfectly legitimate to use these adapters to
     perform argument validation, as opposed (or in addition) to
     conversion.
  */
  target.xWrap.argAdapter = function f(typeName, adapter){
    return __xAdapter(f, arguments.length, typeName, adapter,
                      'argAdapter()', xArg);
  };

  target.xWrap.FuncPtrAdapter = xArg.FuncPtrAdapter;

  /**
     Functions like xCall() but performs argument and result type
     conversions as for xWrap(). The first, second, and third
     arguments are as documented for xWrap(), except that the 3rd
     argument may be either a falsy value or empty array to represent
     nullary functions. The 4th+ arguments are arguments for the call,
     with the special case that if the 4th argument is an array, it is
     used as the arguments for the call. Returns the converted result
     of the call.

     This is just a thin wrapper around xWrap(). If the given function
     is to be called more than once, it's more efficient to use
     xWrap() to create a wrapper, then to call that wrapper as many
     times as needed. For one-shot calls, however, this variant is
     arguably more efficient because it will hypothetically free the
     wrapper function quickly.
  */
  target.xCallWrapped = function(fArg, resultType, argTypes, ...args){
    if(Array.isArray(arguments[3])) args = arguments[3];
    return target.xWrap(fArg, resultType, argTypes||[]).apply(null, args||[]);
  };

  /**
     This function is ONLY exposed in the public API to facilitate
     testing. It should not be used in application-level code, only
     in test code.

     Expects to be given (typeName, value) and returns a conversion
     of that value as has been registered using argAdapter().
     It throws if no adapter is found.

     ACHTUNG: the adapter may require that a scopedAllocPush() is
     active and it may allocate memory within that scope. It may also
     require additional arguments, depending on the type of
     conversion.
  */
  target.xWrap.testConvertArg = cache.xWrap.convertArg;

  /**
     This function is ONLY exposed in the public API to facilitate
     testing. It should not be used in application-level code, only
     in test code.

     Expects to be given (typeName, value) and returns a conversion
     of that value as has been registered using resultAdapter().
     It throws if no adapter is found.

     ACHTUNG: the adapter may allocate memory which the caller may need
     to know how to free.
  */
  target.xWrap.testConvertResult = cache.xWrap.convertResult;

  return target;
};

/**
   yawl (Yet Another Wasm Loader) provides very basic wasm loader.
   It requires a config object:

   - `uri`: required URI of the WASM file to load.

   - `onload(loadResult,config)`: optional callback. The first
     argument is the result object from
     WebAssembly.instantiate[Streaming](). The 2nd is the config
     object passed to this function. Described in more detail below.

   - `imports`: optional imports object for
     WebAssembly.instantiate[Streaming](). The default is an empty set
     of imports. If the module requires any imports, this object
     must include them.

   - `wasmUtilTarget`: optional object suitable for passing to
     WhWasmUtilInstaller(). If set, it gets passed to that function
     after the promise resolves. This function sets several properties
     on it before passing it on to that function (which sets many
     more):

     - `module`, `instance`: the properties from the
       instantiate[Streaming]() result.

     - If `instance.exports.memory` is _not_ set then it requires that
       `config.imports.env.memory` be set (else it throws), and
       assigns that to `target.memory`.

     - If `wasmUtilTarget.alloc` is not set and
       `instance.exports.malloc` is, it installs
       `wasmUtilTarget.alloc()` and `wasmUtilTarget.dealloc()`
       wrappers for the exports `malloc` and `free` functions.

   It returns a function which, when called, initiates loading of the
   module and returns a Promise. When that Promise resolves, it calls
   the `config.onload` callback (if set) and passes it
   `(loadResult,config)`, where `loadResult` is the result of
   WebAssembly.instantiate[Streaming](): an object in the form:

   ```
   {
     module: a WebAssembly.Module,
     instance: a WebAssembly.Instance
   }
   ```

   (Note that the initial `then()` attached to the promise gets only
   that object, and not the `config` one.)

   Error handling is up to the caller, who may attach a `catch()` call
   to the promise.
*/
globalThis.WhWasmUtilInstaller.yawl = function(config){
  const wfetch = ()=>fetch(config.uri, {credentials: 'same-origin'});
  const wui = this;
  const finalThen = function(arg){
    //log("finalThen()",arg);
    if(config.wasmUtilTarget){
      const toss = (...args)=>{throw new Error(args.join(' '))};
      const tgt = config.wasmUtilTarget;
      tgt.module = arg.module;
      tgt.instance = arg.instance;
      //tgt.exports = tgt.instance.exports;
      if(!tgt.instance.exports.memory){
        /**
           WhWasmUtilInstaller requires either tgt.exports.memory
           (exported from WASM) or tgt.memory (JS-provided memory
           imported into WASM).
        */
        tgt.memory = (config.imports && config.imports.env
                      && config.imports.env.memory)
          || toss("Missing 'memory' object!");
      }
      if(!tgt.alloc && arg.instance.exports.malloc){
        const exports = arg.instance.exports;
        tgt.alloc = function(n){
          return exports.malloc(n) || toss("Allocation of",n,"bytes failed.");
        };
        tgt.dealloc = function(m){exports.free(m)};
      }
      wui(tgt);
    }
    if(config.onload) config.onload(arg,config);
    return arg /* for any then() handler attached to
                  yetAnotherWasmLoader()'s return value */;
  };
  const loadWasm = WebAssembly.instantiateStreaming
        ? function loadWasmStreaming(){
          return WebAssembly.instantiateStreaming(wfetch(), config.imports||{})
            .then(finalThen);
        }
        : function loadWasmOldSchool(){ // Safari < v15
          return wfetch()
            .then(response => response.arrayBuffer())
            .then(bytes => WebAssembly.instantiate(bytes, config.imports||{}))
            .then(finalThen);
        };
  return loadWasm;
}.bind(globalThis.WhWasmUtilInstaller)/*yawl()*/;
