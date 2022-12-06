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
   This file installs sqlite3.VfsHelper, and object which exists to
   assist in the creation of JavaScript implementations of sqlite3_vfs,
   along with its virtual table counterpart, sqlite3.VtabHelper.
*/
'use strict';
self.sqlite3ApiBootstrap.initializers.push(function(sqlite3){
  const wasm = sqlite3.wasm, capi = sqlite3.capi, toss = sqlite3.util.toss;
  const vh = Object.create(null), vt = Object.create(null);

  sqlite3.VfsHelper = vh;
  sqlite3.VtabHelper = vt;

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
     that proxy to be cleaned up when tgt.dispose() is called.  Throws
     on the slightest hint of error, e.g. tgt is-not-a StructType,
     name does not map to a struct-bound member, etc.

     Returns a proxy for this function which is bound to tgt and takes
     2 args (name,func). That function returns the same thing,
     permitting calls to be chained.

     If called with only 1 arg, it has no side effects but returns a
     func with the same signature as described above.

     If tgt.ondispose is set before this is called then it _must_
     be an array, to which this function will append entries.

     ACHTUNG: because we cannot generically know how to transform JS
     exceptions into result codes, the installed functions do no
     automatic catching of exceptions. It is critical, to avoid 
     undefined behavior in the C layer, that methods mapped via
     this function do not throw. The exception, as it were, to that
     rule is...

     If applyArgcCheck is true then each method gets wrapped in a
     proxy which asserts that it is passed the expected number of
     arguments, throwing if the argument count does not match
     expectations. That is only intended for dev-time usage for sanity
     checking, and will leave the C environment in an undefined
     state. For non-dev-time use, it is a given that the C API will
     never call one of the generated function wrappers with the wrong
     argument count.
  */
  vh.installMethod = vt.installMethod = function callee(
    tgt, name, func, applyArgcCheck = callee.installMethodArgcCheck
  ){
    if(!(tgt instanceof sqlite3.StructBinder.StructType)){
      toss("Usage error: target object is-not-a StructType.");
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
         sqlite3.StructBinder-created types. */
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
      toss("Member",name," is not a function pointer. Signature =",sigN);
    }
    const memKey = tgt.memberKey(name);
    const fProxy = applyArgcCheck
    /** This middle-man proxy is only for use during development, to
        confirm that we always pass the proper number of
        arguments. We know that the C-level code will always use the
        correct argument count. */
          ? callee.argcProxy(tgt, memKey, func, sigN)
          : func;
    const pFunc = wasm.installFunction(fProxy, tgt.memberSignature(name, true));
    tgt[memKey] = pFunc;
    if(!tgt.ondispose) tgt.ondispose = [];
    else if(!Array.isArray(tgt.ondispose)) tgt.ondispose = [tgt.ondispose];
    if(!tgt.ondispose.__removeFuncList){
      tgt.ondispose.push('ondispose.__removeFuncList handler',
                         callee.removeFuncList);
      tgt.ondispose.__removeFuncList = [];
    }
    tgt.ondispose.__removeFuncList.push(memKey, pFunc);
    return (n,f)=>callee(tgt, n, f, applyArgcCheck);
  }/*installMethod*/;
  vh.installMethod.installMethodArgcCheck = false;

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

     On success, returns this object. Throws on error.
  */
  vh.installMethods = vt.installMethods = function(
    structType, methods, applyArgcCheck = vh.installMethod.installMethodArgcCheck
  ){
    const seen = new Map /* map of <Function, memberName> */;
    for(const k of Object.keys(methods)){
      const m = methods[k];
      const prior = seen.get(m);
      if(prior){
        const mkey = structType.memberKey(k);
        structType[mkey] = structType[structType.memberKey(prior)];
      }else{
        vh.installMethod(structType, k, m, applyArgcCheck);
        seen.set(m, k);
      }
    }
    return this;
  };

  /**
     Uses sqlite3_vfs_register() to register the
     sqlite3.capi.sqlite3_vfs-type vfs, which must have already been
     filled out properly. If the 2nd argument is truthy, the VFS is
     registered as the default VFS, else it is not.

     On success, returns this object. Throws on error.
  */
  vh.registerVfs = function(vfs, asDefault=false){
    if(!(vfs instanceof sqlite3.capi.sqlite3_vfs)){
      toss("Expecting a sqlite3_vfs-type argument.");
    }
    const rc = capi.sqlite3_vfs_register(vfs.pointer, asDefault ? 1 : 0);
    if(rc){
      toss("sqlite3_vfs_register(",vfs,") failed with rc",rc);
    }
    if(vfs.pointer !== capi.sqlite3_vfs_find(vfs.$zName)){
      toss("BUG: sqlite3_vfs_find(vfs.$zName) failed for just-installed VFS",
           vfs);
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
     this.installMethods().

     If the `vfs` entry is set then:

     - Its `struct` property is passed to this.registerVfs(). The
       `vfs` entry may optionally have an `asDefault` property, which
       gets passed as the 2nd argument to registerVfs().

     - If `struct.$zName` is falsy and the entry has a string-type
       `name` property, `struct.$zName` is set to the C-string form of
       that `name` value before registerVfs() is called.

     On success returns this object. Throws on error.
  */
  vh.installVfs = function(opt){
    let count = 0;
    const propList = ['io','vfs'];
    for(const key of propList){
      const o = opt[key];
      if(o){
        ++count;
        this.installMethods(o.struct, o.methods, !!o.applyArgcCheck);
        if('vfs'===key){
          if(!o.struct.$zName && 'string'===typeof o.name){
            o.struct.$zName = wasm.allocCString(o.name);
            /* Note that we leak that C-string. */
          }
          this.registerVfs(o.struct, !!o.asDefault);
        }
      }
    }
    if(!count) toss("Misuse: installVfs() options object requires at least",
                    "one of:", propList);
    return this;
  };

  /**
     Expects to be passed the (argc,argv) arguments of
     sqlite3_module::xFilter(), or an equivalent API.  This function
     transforms the arguments (an array of (sqlite3_value*)) into a JS
     array of equivalent JS values. It uses the same type conversions
     as sqlite3_create_function_v2() and friends. Throws on error,
     e.g. if it cannot figure out a sensible data conversion.
  */
  vt.sqlite3ValuesToJs = capi.sqlite3_create_function_v2.udfConvertArgs;

  /**
     Factory function for wrapXyz() impls.
  */
  const __xWrapFactory = function(structType){
    return function(ptr,remove=false){
      if(0===arguments.length) ptr = new structType;
      if(ptr instanceof structType){
        //T.assert(!this.has(ptr.pointer));
        this.set(ptr.pointer, ptr);
        return ptr;
      }else if(!wasm.isPtr(ptr)){
        sqlite3.SQLite3Error.toss("Invalid argument to xWrapFactory");
      }
      let rc = this.get(ptr);
      if(remove) this.delete(ptr);
      /*arguable else if(!rc){
        rc = new structType(ptr);
        this.set(ptr, rc);
      }*/
      return rc;
    }.bind(new Map);
  };
  /**
     EXPERIMENTAL. DO NOT USE IN CLIENT CODE.

     Has 3 distinct uses:

     - wrapVtab() instantiates a new capi.sqlite3_vtab instance, maps
       its pointer for later by-pointer lookup, and returns that
       object. This is intended to be called from
       sqlite3_module::xConnect() or xCreate() implementations.

     - wrapVtab(pVtab) accepts a WASM pointer to a C-level
       (sqlite3_vtab*) instance and returns the capi.sqlite3_vtab
       object created by the first form of this function, or undefined
       if that form has not been used. This is intended to be called
       from sqlite3_module methods which take a (sqlite3_vtab*) pointer
       _except_ for xDisconnect(), in which case use...

     - wrapVtab(pVtab,true) as for the previous form, but removes the
       pointer-to-object mapping before returning.  The caller must
       call dispose() on the returned object. This is intended to be
       called from sqlite3_module::xDisconnect() implementations or
       in error handling of a failed xCreate() or xConnect().
 */
  vt.xWrapVtab = __xWrapFactory(capi.sqlite3_vtab);

  /**
     EXPERIMENTAL. DO NOT USE IN CLIENT CODE.

     Works identically to wrapVtab() except that it deals with
     sqlite3_cursor objects and pointers instead of sqlite3_vtab.

     - wrapCursor() is intended to be called from sqlite3_module::xOpen()

     - wrapCursor(pCursor) is intended to be called from all sqlite3_module
       methods which take a (sqlite3_vtab_cursor*) _except_ for
       xClose(), in which case use...

     - wrapCursor(pCursor, true) will remove the m apping of pCursor to a
       capi.sqlite3_vtab_cursor object and return that object.  The
       caller must call dispose() on the returned object. This is
       intended to be called form xClose() or in error handling of a
       failed xOpen().
 */
  vt.xWrapCursor = __xWrapFactory(capi.sqlite3_vtab_cursor);

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
       return sqlite3.VtabHelper.exceptionToRc(e, sqlite3.capi.SQLITE_XYZ);
       // where SQLITE_XYZ is some call-appropriate result code.
     }
     ```
  */
  /**vh.exceptionToRc = vt.exceptionToRc =
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
       return sqlite3.VtabHelper.xMethodError(
                'xColumn', e, sqlite3.capi.SQLITE_XYZ);
       // where SQLITE_XYZ is some call-appropriate result code
       // defaulting to SQLITE_ERROR.
     }
     ```

     If xMethodError.errorReporter is a function, it is called in
     order to report the error, else the error is not reported.
     If that function throws, that exception is ignored.
  */
  vt.xMethodError = function f(methodName, err, defaultRc=capi.SQLITE_ERROR){
    if(f.errorReporter instanceof Function){
      try{f.errorReporter("sqlite3_module::"+methodName+"(): "+err.message);}
      catch(e){/*ignored*/}
    }
    return (err instanceof sqlite3.WasmAllocError)
      ? capi.SQLITE_NOMEM
      : defaultRc;
  };
  vt.xMethodError.errorReporter = 1 ? console.error.bind(console) : false;

  /**
     "The problem" with this is that it introduces an outer function with
     a different arity than the passed-in method callback. That means we
     cannot do argc validation on these. Additionally, some methods (namely
     xConnect) may have call-specific error handling. It would be a shame to
     hard-coded that per-method support in this function.
  */
  /** vt.methodCatcher = function(methodName, method, defaultErrRc=capi.SQLITE_ERROR){
    return function(...args){
      try { method(...args); }
      }catch(e){ return vt.xMethodError(methodName, e, defaultRc) }
  };
  */

  /**
     A helper for sqlite3_vtab::xRow() implementations. It must be
     passed that function's 2nd argument and the value for that
     pointer.  Returns the same as wasm.setMemValue() and will throw
     if the 1st or 2nd arguments are invalid for that function.
  */
  vt.setRowId = (ppRowid64, value)=>wasm.setMemValue(ppRowid64, value, 'i64');
}/*sqlite3ApiBootstrap.initializers.push()*/);
