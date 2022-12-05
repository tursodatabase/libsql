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
   This file installs sqlite.VfsHelper, an object which exists
   to assist in the creation of JavaScript implementations of
   sqlite3_vfs. It is NOT part of the public API, and is an
   internal implemenation detail for use in this project's
   own development of VFSes. It may be exposed to clients
   at some point, provided there is value in doing so.
*/
'use strict';
self.sqlite3ApiBootstrap.initializers.push(function(sqlite3){
  const wasm = sqlite3.wasm, capi = sqlite3.capi, toss = sqlite3.util.toss;
  const vh = Object.create(null);

  /**
     Does nothing more than holds a permanent reference to each
     argument. This is useful in some cases to ensure that, e.g., a
     custom sqlite3_io_methods instance does not get
     garbage-collected.

     Returns this object.
  */
  vh.holdReference = function(...args){
    for(const v of args) this.refs.add(v);
    return vh;
  }.bind({refs: new Set});
  
  /**
     Installs a StructBinder-bound function pointer member of the
     given name and function in the given StructType target object.
     It creates a WASM proxy for the given function and arranges for
     that proxy to be cleaned up when tgt.dispose() is called.  Throws
     on the slightest hint of error, e.g. tgt is-not-a StructType,
     name does not map to a struct-bound member, etc.

     If applyArgcCheck is true then each method gets wrapped in a
     proxy which asserts that it is passed the expected number of
     arguments, throwing if the argument count does not match
     expectations. That is only recommended for dev-time usage for
     sanity checking. Once a VFS implementation is known to be
     working, it is a given that the C API will never call it with the
     wrong argument count.

     Returns a proxy for this function which is bound to tgt and takes
     2 args (name,func). That function returns the same thing,
     permitting calls to be chained.

     If called with only 1 arg, it has no side effects but returns a
     func with the same signature as described above.

     If tgt.ondispose is set before this is called then it _must_
     be an array, to which this function will append entries.
  */
  vh.installMethod = function callee(tgt, name, func,
                                     applyArgcCheck=callee.installMethodArgcCheck){
    if(!(tgt instanceof sqlite3.StructBinder.StructType)){
      toss("Usage error: target object is-not-a StructType.");
    }
    if(1===arguments.length){
      return (n,f)=>callee(tgt, n, f, applyArgcCheck);
    }
    if(!callee.argcProxy){
      callee.argcProxy = function(func,sig){
        return function(...args){
          if(func.length!==arguments.length){
            toss("Argument mismatch. Native signature is:",sig);
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
          ? callee.argcProxy(func, sigN)
          : func;
    const pFunc = wasm.installFunction(fProxy, tgt.memberSignature(name, true));
    tgt[memKey] = pFunc;
    if(!tgt.ondispose) tgt.ondispose = [];
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
     Installs methods into the given StructType-type object. Each
     entry in the given methods object must map to a known member of
     the given StructType, else an exception will be triggered.
     See installMethod() for more details, including the semantics
     of the 3rd argument.

     On success, passes its first argument to holdRefence() and
     returns this object. Throws on error.
  */
  vh.installMethods = function(structType, methods,
                               applyArgcCheck=vh.installMethod.installMethodArgcCheck){
    for(const k of Object.keys(methods)){
      vh.installMethod(structType, k, methods[k], applyArgcCheck);
    }
    return vh.holdReference(structType);
  };

  /**
     Uses sqlite3_vfs_register() to register the
     sqlite3.capi.sqlite3_vfs-type vfs, which must have already been
     filled out properly. If the 2nd argument is truthy, the VFS is
     registered as the default VFS, else it is not.

     On success, passes its first argument to this.holdReference() and
     returns this object. Throws on error.
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
    return vh.holdReference(vfs);
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
       (e.g. 'xClose') to JS implementations of those methods.

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
  
  sqlite3.VfsHelper = vh;
}/*sqlite3ApiBootstrap.initializers.push()*/);
