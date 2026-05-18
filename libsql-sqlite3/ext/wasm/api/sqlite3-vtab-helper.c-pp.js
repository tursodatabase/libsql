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
   This file installs sqlite3.vtab, a namespace of helpers for use in
   the creation of JavaScript implementations virtual tables. If built
   without virtual table support then this function does nothing.
*/
'use strict';
globalThis.sqlite3ApiBootstrap.initializers.push(function(sqlite3){
  if( !sqlite3.wasm.exports.sqlite3_declare_vtab ){
    return;
  }
  const wasm = sqlite3.wasm, capi = sqlite3.capi, toss = sqlite3.util.toss3;
  const vtab = Object.create(null);
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
        mod.installMethods(remethods, false);
      }else{
        // No automatic exception handling. Trust the client
        // to not throw.
        mod.installMethods(
          methods, !!opt.applyArgcCheck/*undocumented option*/
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
