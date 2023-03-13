/*
** 2022-11-30
**
** The author disclaims copyright to this source code.  In place of a
** legal notice, here is a blessing:
**
** *   May you do good and not evil.
** *   May you find forgiveness for yourself and forgive others.
** *   May you share freely, never taking more than you give.
*/

/**
   This file installs sqlite3.vfs, and object which exists to assist
   in the creation of JavaScript implementations of sqlite3_vfs, along
   with its virtual table counterpart, sqlite3.vtab.
*/
'use strict';
globalThis.sqlite3ApiBootstrap.initializers.push(function(sqlite3){
  const wasm = sqlite3.wasm, capi = sqlite3.capi, toss = sqlite3.util.toss3;
  const vfs = Object.create(null), vtab = Object.create(null);

  const StructBinder = sqlite3.StructBinder
  /* we require a local alias b/c StructBinder is removed from the sqlite3
     object during the final steps of the API cleanup. */;
  sqlite3.vfs = vfs;
  sqlite3.vtab = vtab;

  const sii = capi.sqlite3_index_info;
  /**
     If n is >=0 and less than this.$nConstraint, this function
     returns either a WASM pointer to the 0-based nth entry of
     this.$aConstraint (if passed a truthy 2nd argument) or an
     sqlite3_index_info.sqlite3_index_constraint object wrapping that
     address (if passed a falsy value or no 2nd argument). Returns a
     falsy value if n is out of range.
  */
  sii.prototype.nthConstraint = function(n, asPtr=false){
    if(n<0 || n>=this.$nConstraint) return false;
    const ptr = this.$aConstraint + (
      sii.sqlite3_index_constraint.structInfo.sizeof * n
    );
    return asPtr ? ptr : new sii.sqlite3_index_constraint(ptr);
  };

  /**
     Works identically to nthConstraint() but returns state from
     this.$aConstraintUsage, so returns an
     sqlite3_index_info.sqlite3_index_constraint_usage instance
     if passed no 2nd argument or a falsy 2nd argument.
  */
  sii.prototype.nthConstraintUsage = function(n, asPtr=false){
    if(n<0 || n>=this.$nConstraint) return false;
    const ptr = this.$aConstraintUsage + (
      sii.sqlite3_index_constraint_usage.structInfo.sizeof * n
    );
    return asPtr ? ptr : new sii.sqlite3_index_constraint_usage(ptr);
  };

  /**
     If n is >=0 and less than this.$nOrderBy, this function
     returns either a WASM pointer to the 0-based nth entry of
     this.$aOrderBy (if passed a truthy 2nd argument) or an
     sqlite3_index_info.sqlite3_index_orderby object wrapping that
     address (if passed a falsy value or no 2nd argument). Returns a
     falsy value if n is out of range.
  */
  sii.prototype.nthOrderBy = function(n, asPtr=false){
    if(n<0 || n>=this.$nOrderBy) return false;
    const ptr = this.$aOrderBy + (
      sii.sqlite3_index_orderby.structInfo.sizeof * n
    );
    return asPtr ? ptr : new sii.sqlite3_index_orderby(ptr);
  };

  /**
     Installs a StructBinder-bound function pointer member of the
     given name and function in the given StructType target object.

     It creates a WASM proxy for the given function and arranges for
     that proxy to be cleaned up when tgt.dispose() is called. Throws
     on the slightest hint of error, e.g. tgt is-not-a StructType,
     name does not map to a struct-bound member, etc.

     As a special case, if the given function is a pointer, then
     `wasm.functionEntry()` is used to validate that it is a known
     function. If so, it is used as-is with no extra level of proxying
     or cleanup, else an exception is thrown. It is legal to pass a
     value of 0, indicating a NULL pointer, with the caveat that 0
     _is_ a legal function pointer in WASM but it will not be accepted
     as such _here_. (Justification: the function at address zero must
     be one which initially came from the WASM module, not a method we
     want to bind to a virtual table or VFS.)

     This function returns a proxy for itself which is bound to tgt
     and takes 2 args (name,func). That function returns the same
     thing as this one, permitting calls to be chained.

     If called with only 1 arg, it has no side effects but returns a
     func with the same signature as described above.

     ACHTUNG: because we cannot generically know how to transform JS
     exceptions into result codes, the installed functions do no
     automatic catching of exceptions. It is critical, to avoid 
     undefined behavior in the C layer, that methods mapped via
     this function do not throw. The exception, as it were, to that
     rule is...

     If applyArgcCheck is true then each JS function (as opposed to
     function pointers) gets wrapped in a proxy which asserts that it
     is passed the expected number of arguments, throwing if the
     argument count does not match expectations. That is only intended
     for dev-time usage for sanity checking, and will leave the C
     environment in an undefined state.
  */
  const installMethod = function callee(
    tgt, name, func, applyArgcCheck = callee.installMethodArgcCheck
  ){
    if(!(tgt instanceof StructBinder.StructType)){
      toss("Usage error: target object is-not-a StructType.");
    }else if(!(func instanceof Function) && !wasm.isPtr(func)){
      toss("Usage errror: expecting a Function or WASM pointer to one.");
    }
    if(1===arguments.length){
      return (n,f)=>callee(tgt, n, f, applyArgcCheck);
    }
    if(!callee.argcProxy){
      callee.argcProxy = function(tgt, funcName, func,sig){
        return function(...args){
          if(func.length!==arguments.length){
            toss("Argument mismatch for",
                 tgt.structInfo.name+"::"+funcName
                 +": Native signature is:",sig);
          }
          return func.apply(this, args);
        }
      };
      /* An ondispose() callback for use with
         StructBinder-created types. */
      callee.removeFuncList = function(){
        if(this.ondispose.__removeFuncList){
          this.ondispose.__removeFuncList.forEach(
            (v,ndx)=>{
              if('number'===typeof v){
                try{wasm.uninstallFunction(v)}
                catch(e){/*ignore*/}
              }
              /* else it's a descriptive label for the next number in
                 the list. */
            }
          );
          delete this.ondispose.__removeFuncList;
        }
      };
    }/*static init*/
    const sigN = tgt.memberSignature(name);
    if(sigN.length<2){
      toss("Member",name,"does not have a function pointer signature:",sigN);
    }
    const memKey = tgt.memberKey(name);
    const fProxy = (applyArgcCheck && !wasm.isPtr(func))
    /** This middle-man proxy is only for use during development, to
        confirm that we always pass the proper number of
        arguments. We know that the C-level code will always use the
        correct argument count. */
          ? callee.argcProxy(tgt, memKey, func, sigN)
          : func;
    if(wasm.isPtr(fProxy)){
      if(fProxy && !wasm.functionEntry(fProxy)){
        toss("Pointer",fProxy,"is not a WASM function table entry.");
      }
      tgt[memKey] = fProxy;
    }else{
      const pFunc = wasm.installFunction(fProxy, tgt.memberSignature(name, true));
      tgt[memKey] = pFunc;
      if(!tgt.ondispose || !tgt.ondispose.__removeFuncList){
        tgt.addOnDispose('ondispose.__removeFuncList handler',
                         callee.removeFuncList);
        tgt.ondispose.__removeFuncList = [];
      }
      tgt.ondispose.__removeFuncList.push(memKey, pFunc);
    }
    return (n,f)=>callee(tgt, n, f, applyArgcCheck);
  }/*installMethod*/;
  installMethod.installMethodArgcCheck = false;

  /**
     Installs methods into the given StructType-type instance. Each
     entry in the given methods object must map to a known member of
     the given StructType, else an exception will be triggered.  See
     installMethod() for more details, including the semantics of the
     3rd argument.

     As an exception to the above, if any two or more methods in the
     2nd argument are the exact same function, installMethod() is
     _not_ called for the 2nd and subsequent instances, and instead
     those instances get assigned the same method pointer which is
     created for the first instance. This optimization is primarily to
     accommodate special handling of sqlite3_module::xConnect and
     xCreate methods.

     On success, returns its first argument. Throws on error.
  */
  const installMethods = function(
    structInstance, methods, applyArgcCheck = installMethod.installMethodArgcCheck
  ){
    const seen = new Map /* map of <Function, memberName> */;
    for(const k of Object.keys(methods)){
      const m = methods[k];
      const prior = seen.get(m);
      if(prior){
        const mkey = structInstance.memberKey(k);
        structInstance[mkey] = structInstance[structInstance.memberKey(prior)];
      }else{
        installMethod(structInstance, k, m, applyArgcCheck);
        seen.set(m, k);
      }
    }
    return structInstance;
  };

  /**
     Equivalent to calling installMethod(this,...arguments) with a
     first argument of this object. If called with 1 or 2 arguments
     and the first is an object, it's instead equivalent to calling
     installMethods(this,...arguments).
  */
  StructBinder.StructType.prototype.installMethod = function callee(
    name, func, applyArgcCheck = installMethod.installMethodArgcCheck
  ){
    return (arguments.length < 3 && name && 'object'===typeof name)
      ? installMethods(this, ...arguments)
      : installMethod(this, ...arguments);
  };

  /**
     Equivalent to calling installMethods() with a first argument
     of this object.
  */
  StructBinder.StructType.prototype.installMethods = function(
    methods, applyArgcCheck = installMethod.installMethodArgcCheck
  ){
    return installMethods(this, methods, applyArgcCheck);
  };

  /**
     Uses sqlite3_vfs_register() to register this
     sqlite3.capi.sqlite3_vfs. This object must have already been
     filled out properly. If the first argument is truthy, the VFS is
     registered as the default VFS, else it is not.

     On success, returns this object. Throws on error.
  */
  capi.sqlite3_vfs.prototype.registerVfs = function(asDefault=false){
    if(!(this instanceof sqlite3.capi.sqlite3_vfs)){
      toss("Expecting a sqlite3_vfs-type argument.");
    }
    const rc = capi.sqlite3_vfs_register(this, asDefault ? 1 : 0);
    if(rc){
      toss("sqlite3_vfs_register(",this,") failed with rc",rc);
    }
    if(this.pointer !== capi.sqlite3_vfs_find(this.$zName)){
      toss("BUG: sqlite3_vfs_find(vfs.$zName) failed for just-installed VFS",
           this);
    }
    return this;
  };

  /**
     A wrapper for installMethods() or registerVfs() to reduce
     installation of a VFS and/or its I/O methods to a single
     call.

     Accepts an object which contains the properties "io" and/or
     "vfs", each of which is itself an object with following properties:

     - `struct`: an sqlite3.StructType-type struct. This must be a
       populated (except for the methods) object of type
       sqlite3_io_methods (for the "io" entry) or sqlite3_vfs (for the
       "vfs" entry).

     - `methods`: an object mapping sqlite3_io_methods method names
       (e.g. 'xClose') to JS implementations of those methods. The JS
       implementations must be call-compatible with their native
       counterparts.

     For each of those object, this function passes its (`struct`,
     `methods`, (optional) `applyArgcCheck`) properties to
     installMethods().

     If the `vfs` entry is set then:

     - Its `struct` property's registerVfs() is called. The
       `vfs` entry may optionally have an `asDefault` property, which
       gets passed as the argument to registerVfs().

     - If `struct.$zName` is falsy and the entry has a string-type
       `name` property, `struct.$zName` is set to the C-string form of
       that `name` value before registerVfs() is called.

     On success returns this object. Throws on error.
  */
  vfs.installVfs = function(opt){
    let count = 0;
    const propList = ['io','vfs'];
    for(const key of propList){
      const o = opt[key];
      if(o){
        ++count;
        installMethods(o.struct, o.methods, !!o.applyArgcCheck);
        if('vfs'===key){
          if(!o.struct.$zName && 'string'===typeof o.name){
            o.struct.addOnDispose(
              o.struct.$zName = wasm.allocCString(o.name)
            );
          }
          o.struct.registerVfs(!!o.asDefault);
        }
      }
    }
    if(!count) toss("Misuse: installVfs() options object requires at least",
                    "one of:", propList);
    return this;
  };

  /**
     Internal factory function for xVtab and xCursor impls.
  */
  const __xWrapFactory = function(methodName,StructType){
    return function(ptr,removeMapping=false){
      if(0===arguments.length) ptr = new StructType;
      if(ptr instanceof StructType){
        //T.assert(!this.has(ptr.pointer));
        this.set(ptr.pointer, ptr);
        return ptr;
      }else if(!wasm.isPtr(ptr)){
        sqlite3.SQLite3Error.toss("Invalid argument to",methodName+"()");
      }
      let rc = this.get(ptr);
      if(removeMapping) this.delete(ptr);
      return rc;
    }.bind(new Map);
  };

  /**
     A factory function which implements a simple lifetime manager for
     mappings between C struct pointers and their JS-level wrappers.
     The first argument must be the logical name of the manager
     (e.g. 'xVtab' or 'xCursor'), which is only used for error
     reporting. The second must be the capi.XYZ struct-type value,
     e.g. capi.sqlite3_vtab or capi.sqlite3_vtab_cursor.

     Returns an object with 4 methods: create(), get(), unget(), and
     dispose(), plus a StructType member with the value of the 2nd
     argument. The methods are documented in the body of this
     function.
  */
  const StructPtrMapper = function(name, StructType){
    const __xWrap = __xWrapFactory(name,StructType);
    /**
       This object houses a small API for managing mappings of (`T*`)
       to StructType<T> objects, specifically within the lifetime
       requirements of sqlite3_module methods.
    */
    return Object.assign(Object.create(null),{
      /** The StructType object for this object's API. */
      StructType,
      /**
         Creates a new StructType object, writes its `pointer`
         value to the given output pointer, and returns that
         object. Its intended usage depends on StructType:

         sqlite3_vtab: to be called from sqlite3_module::xConnect()
         or xCreate() implementations.

         sqlite3_vtab_cursor: to be called from xOpen().

         This will throw if allocation of the StructType instance
         fails or if ppOut is not a pointer-type value.
      */
      create: (ppOut)=>{
        const rc = __xWrap();
        wasm.pokePtr(ppOut, rc.pointer);
        return rc;
      },
      /**
         Returns the StructType object previously mapped to the
         given pointer using create(). Its intended usage depends
         on StructType:

         sqlite3_vtab: to be called from sqlite3_module methods which
         take a (sqlite3_vtab*) pointer _except_ for
         xDestroy()/xDisconnect(), in which case unget() or dispose().

         sqlite3_vtab_cursor: to be called from any sqlite3_module methods
         which take a `sqlite3_vtab_cursor*` argument except xClose(),
         in which case use unget() or dispose().

         Rule to remember: _never_ call dispose() on an instance
         returned by this function.
      */
      get: (pCObj)=>__xWrap(pCObj),
      /**
         Identical to get() but also disconnects the mapping between the
         given pointer and the returned StructType object, such that
         future calls to this function or get() with the same pointer
         will return the undefined value. Its intended usage depends
         on StructType:

         sqlite3_vtab: to be called from sqlite3_module::xDisconnect() or
         xDestroy() implementations or in error handling of a failed
         xCreate() or xConnect().

         sqlite3_vtab_cursor: to be called from xClose() or during
         cleanup in a failed xOpen().

         Calling this method obligates the caller to call dispose() on
         the returned object when they're done with it.
      */
      unget: (pCObj)=>__xWrap(pCObj,true),
      /**
         Works like unget() plus it calls dispose() on the
         StructType object.
      */
      dispose: (pCObj)=>{
        const o = __xWrap(pCObj,true);
        if(o) o.dispose();
      }
    });
  };

  /**
     A lifetime-management object for mapping `sqlite3_vtab*`
     instances in sqlite3_module methods to capi.sqlite3_vtab
     objects.

     The API docs are in the API-internal StructPtrMapper().
  */
  vtab.xVtab = StructPtrMapper('xVtab', capi.sqlite3_vtab);

  /**
     A lifetime-management object for mapping `sqlite3_vtab_cursor*`
     instances in sqlite3_module methods to capi.sqlite3_vtab_cursor
     objects.

     The API docs are in the API-internal StructPtrMapper().
  */
  vtab.xCursor = StructPtrMapper('xCursor', capi.sqlite3_vtab_cursor);

  /**
     Convenience form of creating an sqlite3_index_info wrapper,
     intended for use in xBestIndex implementations. Note that the
     caller is expected to call dispose() on the returned object
     before returning. Though not _strictly_ required, as that object
     does not own the pIdxInfo memory, it is nonetheless good form.
  */
  vtab.xIndexInfo = (pIdxInfo)=>new capi.sqlite3_index_info(pIdxInfo);

  /**
     Given an error object, this function returns
     sqlite3.capi.SQLITE_NOMEM if (e instanceof
     sqlite3.WasmAllocError), else it returns its
     second argument. Its intended usage is in the methods
     of a sqlite3_vfs or sqlite3_module:

     ```
     try{
      let rc = ...
      return rc;
     }catch(e){
       return sqlite3.vtab.exceptionToRc(e, sqlite3.capi.SQLITE_XYZ);
       // where SQLITE_XYZ is some call-appropriate result code.
     }
     ```
  */
  /**vfs.exceptionToRc = vtab.exceptionToRc =
    (e, defaultRc=capi.SQLITE_ERROR)=>(
      (e instanceof sqlite3.WasmAllocError)
        ? capi.SQLITE_NOMEM
        : defaultRc
    );*/

  /**
     Given an sqlite3_module method name and error object, this
     function returns sqlite3.capi.SQLITE_NOMEM if (e instanceof
     sqlite3.WasmAllocError), else it returns its second argument. Its
     intended usage is in the methods of a sqlite3_vfs or
     sqlite3_module:

     ```
     try{
      let rc = ...
      return rc;
     }catch(e){
       return sqlite3.vtab.xError(
                'xColumn', e, sqlite3.capi.SQLITE_XYZ);
       // where SQLITE_XYZ is some call-appropriate result code.
     }
     ```

     If no 3rd argument is provided, its default depends on
     the error type:

     - An sqlite3.WasmAllocError always resolves to capi.SQLITE_NOMEM.

     - If err is an SQLite3Error then its `resultCode` property
       is used.

     - If all else fails, capi.SQLITE_ERROR is used.

     If xError.errorReporter is a function, it is called in
     order to report the error, else the error is not reported.
     If that function throws, that exception is ignored.
  */
  vtab.xError = function f(methodName, err, defaultRc){
    if(f.errorReporter instanceof Function){
      try{f.errorReporter("sqlite3_module::"+methodName+"(): "+err.message);}
      catch(e){/*ignored*/}
    }
    let rc;
    if(err instanceof sqlite3.WasmAllocError) rc = capi.SQLITE_NOMEM;
    else if(arguments.length>2) rc = defaultRc;
    else if(err instanceof sqlite3.SQLite3Error) rc = err.resultCode;
    return rc || capi.SQLITE_ERROR;
  };
  vtab.xError.errorReporter = 1 ? console.error.bind(console) : false;

  /**
     "The problem" with this is that it introduces an outer function with
     a different arity than the passed-in method callback. That means we
     cannot do argc validation on these. Additionally, some methods (namely
     xConnect) may have call-specific error handling. It would be a shame to
     hard-coded that per-method support in this function.
  */
  /** vtab.methodCatcher = function(methodName, method, defaultErrRc=capi.SQLITE_ERROR){
    return function(...args){
      try { method(...args); }
      }catch(e){ return vtab.xError(methodName, e, defaultRc) }
  };
  */

  /**
     A helper for sqlite3_vtab::xRowid() and xUpdate()
     implementations. It must be passed the final argument to one of
     those methods (an output pointer to an int64 row ID) and the
     value to store at the output pointer's address. Returns the same
     as wasm.poke() and will throw if the 1st or 2nd arguments
     are invalid for that function.

     Example xRowid impl:

     ```
     const xRowid = (pCursor, ppRowid64)=>{
       const c = vtab.xCursor(pCursor);
       vtab.xRowid(ppRowid64, c.myRowId);
       return 0;
     };
     ```
  */
  vtab.xRowid = (ppRowid64, value)=>wasm.poke(ppRowid64, value, 'i64');

  /**
     A helper to initialize and set up an sqlite3_module object for
     later installation into individual databases using
     sqlite3_create_module(). Requires an object with the following
     properties:

     - `methods`: an object containing a mapping of properties with
       the C-side names of the sqlite3_module methods, e.g. xCreate,
       xBestIndex, etc., to JS implementations for those functions.
       Certain special-case handling is performed, as described below.

     - `catchExceptions` (default=false): if truthy, the given methods
       are not mapped as-is, but are instead wrapped inside wrappers
       which translate exceptions into result codes of SQLITE_ERROR or
       SQLITE_NOMEM, depending on whether the exception is an
       sqlite3.WasmAllocError. In the case of the xConnect and xCreate
       methods, the exception handler also sets the output error
       string to the exception's error string.

     - OPTIONAL `struct`: a sqlite3.capi.sqlite3_module() instance. If
       not set, one will be created automatically. If the current
       "this" is-a sqlite3_module then it is unconditionally used in
       place of `struct`.

     - OPTIONAL `iVersion`: if set, it must be an integer value and it
       gets assigned to the `$iVersion` member of the struct object.
       If it's _not_ set, and the passed-in `struct` object's `$iVersion`
       is 0 (the default) then this function attempts to define a value
       for that property based on the list of methods it has.

     If `catchExceptions` is false, it is up to the client to ensure
     that no exceptions escape the methods, as doing so would move
     them through the C API, leading to undefined
     behavior. (vtab.xError() is intended to assist in reporting
     such exceptions.)

     Certain methods may refer to the same implementation. To simplify
     the definition of such methods:

     - If `methods.xConnect` is `true` then the value of
       `methods.xCreate` is used in its place, and vice versa. sqlite
       treats xConnect/xCreate functions specially if they are exactly
       the same function (same pointer value).

     - If `methods.xDisconnect` is true then the value of
       `methods.xDestroy` is used in its place, and vice versa.

     This is to facilitate creation of those methods inline in the
     passed-in object without requiring the client to explicitly get a
     reference to one of them in order to assign it to the other
     one. 

     The `catchExceptions`-installed handlers will account for
     identical references to the above functions and will install the
     same wrapper function for both.

     The given methods are expected to return integer values, as
     expected by the C API. If `catchExceptions` is truthy, the return
     value of the wrapped function will be used as-is and will be
     translated to 0 if the function returns a falsy value (e.g. if it
     does not have an explicit return). If `catchExceptions` is _not_
     active, the method implementations must explicitly return integer
     values.

     Throws on error. On success, returns the sqlite3_module object
     (`this` or `opt.struct` or a new sqlite3_module instance,
     depending on how it's called).
  */
  vtab.setupModule = function(opt){
    let createdMod = false;
    const mod = (this instanceof capi.sqlite3_module)
          ? this : (opt.struct || (createdMod = new capi.sqlite3_module()));
    try{
      const methods = opt.methods || toss("Missing 'methods' object.");
      for(const e of Object.entries({
        // -----^ ==> [k,v] triggers a broken code transformation in
        // some versions of the emsdk toolchain.
        xConnect: 'xCreate', xDisconnect: 'xDestroy'
      })){
        // Remap X=true to X=Y for certain X/Y combinations
        const k = e[0], v = e[1];
        if(true === methods[k]) methods[k] = methods[v];
        else if(true === methods[v]) methods[v] = methods[k];
      }
      if(opt.catchExceptions){
        const fwrap = function(methodName, func){
          if(['xConnect','xCreate'].indexOf(methodName) >= 0){
            return function(pDb, pAux, argc, argv, ppVtab, pzErr){
              try{return func(...arguments) || 0}
              catch(e){
                if(!(e instanceof sqlite3.WasmAllocError)){
                  wasm.dealloc(wasm.peekPtr(pzErr));
                  wasm.pokePtr(pzErr, wasm.allocCString(e.message));
                }
                return vtab.xError(methodName, e);
              }
            };
          }else{
            return function(...args){
              try{return func(...args) || 0}
              catch(e){
                return vtab.xError(methodName, e);
              }
            };
          }
        };
        const mnames = [
          'xCreate', 'xConnect', 'xBestIndex', 'xDisconnect',
          'xDestroy', 'xOpen', 'xClose', 'xFilter', 'xNext',
          'xEof', 'xColumn', 'xRowid', 'xUpdate',
          'xBegin', 'xSync', 'xCommit', 'xRollback',
          'xFindFunction', 'xRename', 'xSavepoint', 'xRelease',
          'xRollbackTo', 'xShadowName'
        ];
        const remethods = Object.create(null);
        for(const k of mnames){
          const m = methods[k];
          if(!(m instanceof Function)) continue;
          else if('xConnect'===k && methods.xCreate===m){
            remethods[k] = methods.xCreate;
          }else if('xCreate'===k && methods.xConnect===m){
            remethods[k] = methods.xConnect;
          }else{
            remethods[k] = fwrap(k, m);
          }
        }
        installMethods(mod, remethods, false);
      }else{
        // No automatic exception handling. Trust the client
        // to not throw.
        installMethods(
          mod, methods, !!opt.applyArgcCheck/*undocumented option*/
        );
      }
      if(0===mod.$iVersion){
        let v;
        if('number'===typeof opt.iVersion) v = opt.iVersion;
        else if(mod.$xShadowName) v = 3;
        else if(mod.$xSavePoint || mod.$xRelease || mod.$xRollbackTo) v = 2;
        else v = 1;
        mod.$iVersion = v;
      }
    }catch(e){
      if(createdMod) createdMod.dispose();
      throw e;
    }
    return mod;
  }/*setupModule()*/;

  /**
     Equivalent to calling vtab.setupModule() with this sqlite3_module
     object as the call's `this`.
  */
  capi.sqlite3_module.prototype.setupModule = function(opt){
    return vtab.setupModule.call(this, opt);
  };
}/*sqlite3ApiBootstrap.initializers.push()*/);
