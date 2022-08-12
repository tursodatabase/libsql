/*
  2022-07-22

  The author disclaims copyright to this source code.  In place of a
  legal notice, here is a blessing:

  *   May you do good and not evil.
  *   May you find forgiveness for yourself and forgive others.
  *   May you share freely, never taking more than you give.

  ***********************************************************************

  This file contains extensions to the sqlite3 WASM API related to the
  Origin-Private FileSystem (OPFS). It is intended to be appended to
  the main JS deliverable somewhere after sqlite3-api-glue.js and
  before sqlite3-api-cleanup.js.

  Significant notes and limitations:

  - As of this writing, OPFS is still very much in flux and only
    available in bleeding-edge versions of Chrome (v102+, noting that
    that number will increase as the OPFS API matures).

  - The _synchronous_ family of OPFS features (which is what this API
    requires) are only available in non-shared Worker threads. This
    file tries to detect that case and becomes a no-op if those
    features do not seem to be available.
*/

// FileSystemHandle
// FileSystemDirectoryHandle
// FileSystemFileHandle
// FileSystemFileHandle.prototype.createSyncAccessHandle
self.sqlite3.postInit.push(function(self, sqlite3){
  const warn = console.warn.bind(console),
        error = console.error.bind(console);
  if(!self.importScripts || !self.FileSystemFileHandle
     || !self.FileSystemFileHandle.prototype.createSyncAccessHandle){
    warn("OPFS not found or its sync API is not available in this environment.");
    return;
  }else if(!sqlite3.capi.wasm.bigIntEnabled){
    error("OPFS requires BigInt support but sqlite3.capi.wasm.bigIntEnabled is false.");
    return;
  }
  //warn('self.FileSystemFileHandle =',self.FileSystemFileHandle);
  //warn('self.FileSystemFileHandle.prototype =',self.FileSystemFileHandle.prototype);
  const toss = (...args)=>{throw new Error(args.join(' '))};
  const capi = sqlite3.capi,
        wasm = capi.wasm;
  const sqlite3_vfs = capi.sqlite3_vfs
        || toss("Missing sqlite3.capi.sqlite3_vfs object.");
  const sqlite3_file = capi.sqlite3_file
        || toss("Missing sqlite3.capi.sqlite3_file object.");
  const sqlite3_io_methods = capi.sqlite3_io_methods
        || toss("Missing sqlite3.capi.sqlite3_io_methods object.");
  const StructBinder = sqlite3.StructBinder || toss("Missing sqlite3.StructBinder.");
  const debug = console.debug.bind(console),
        log = console.log.bind(console);
  warn("UNDER CONSTRUCTION: setting up OPFS VFS...");

  const pDVfs = capi.sqlite3_vfs_find(null)/*pointer to default VFS*/;
  const dVfs = pDVfs
        ? new sqlite3_vfs(pDVfs)
        : null /* dVfs will be null when sqlite3 is built with
                  SQLITE_OS_OTHER. Though we cannot currently handle
                  that case, the hope is to eventually be able to. */;
  const oVfs = new sqlite3_vfs();
  const oIom = new sqlite3_io_methods();
  oVfs.$iVersion = 2/*yes, two*/;
  oVfs.$szOsFile = capi.sqlite3_file.structInfo.sizeof;
  oVfs.$mxPathname = 1024/*sure, why not?*/;
  oVfs.$zName = wasm.allocCString("opfs");
  oVfs.ondispose = [
    '$zName', oVfs.$zName,
    'cleanup dVfs', ()=>(dVfs ? dVfs.dispose() : null)
  ];
  if(dVfs){
    oVfs.$xSleep = dVfs.$xSleep;
    oVfs.$xRandomness = dVfs.$xRandomness;
  }
  // All C-side memory of oVfs is zeroed out, but just to be explicit:
  oVfs.$xDlOpen = oVfs.$xDlError = oVfs.$xDlSym = oVfs.$xDlClose = null;

  /**
     Pedantic sidebar about oVfs.ondispose: the entries in that array
     are items to clean up when oVfs.dispose() is called, but in this
     environment it will never be called. The VFS instance simply
     hangs around until the WASM module instance is cleaned up. We
     "could" _hypothetically_ clean it up by "importing" an
     sqlite3_os_end() impl into the wasm build, but the shutdown order
     of the wasm engine and the JS one are undefined so there is no
     guaranty that the oVfs instance would be available in one
     environment or the other when sqlite3_os_end() is called (_if_ it
     gets called at all in a wasm build, which is undefined).
  */

  /**
     Installs a StructBinder-bound function pointer member of the
     given name and function in the given StructType target object.
     It creates a WASM proxy for the given function and arranges for
     that proxy to be cleaned up when tgt.dispose() is called.  Throws
     on the slightest hint of error (e.g. tgt is-not-a StructType,
     name does not map to a struct-bound member, etc.).

     Returns a proxy for this function which is bound to tgt and takes
     2 args (name,func). That function returns the same thing,
     permitting calls to be chained.

     If called with only 1 arg, it has no side effects but returns a
     func with the same signature as described above.
  */
  const installMethod = function callee(tgt, name, func){
    if(!(tgt instanceof StructBinder.StructType)){
      toss("Usage error: target object is-not-a StructType.");
    }
    if(1===arguments.length){
      return (n,f)=>callee(tgt,n,f);
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
    //log("installMethod",tgt, name, sigN);
    const fProxy = 1
          // We can remove this proxy middle-man once the VFS is working
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
    return (n,f)=>callee(tgt, n, f);
  }/*installMethod*/;

  /**
     Map of sqlite3_file pointers to OPFS handles.
  */
  const __opfsHandles = Object.create(null);

  const randomFilename = function f(len=16){
    if(!f._chars){
      f._chars = "abcdefghijklmnopqrstuvwxyz"+
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"+
        "012346789";
      f._n = f._chars.length;
    }
    const a = [];
    let i = 0;
    for( ; i < len; ++i){
      const ndx = Math.random() * (f._n * 64) % f._n | 0;
      a[i] = f._chars[ndx];
    }
    return a.join('');
  };

  //const rootDir = await navigator.storage.getDirectory();
  
  ////////////////////////////////////////////////////////////////////////
  // Set up OPFS VFS methods...
  let inst = installMethod(oVfs);
  inst('xOpen', function(pVfs, zName, pFile, flags, pOutFlags){
    const f = new sqlite3_file(pFile);
    f.$pMethods = oIom.pointer;
    __opfsHandles[pFile] = f;
    f.opfsHandle = null /* TODO */;
    if(flags & capi.SQLITE_OPEN_DELETEONCLOSE){
      f.deleteOnClose = true;
    }
    f.filename = zName ? wasm.cstringToJs(zName) : randomFilename();
    error("OPFS sqlite3_vfs::xOpen is not yet full implemented.");
    return capi.SQLITE_IOERR;
  })
  ('xFullPathname', function(pVfs,zName,nOut,pOut){
    /* Until/unless we have some notion of "current dir"
       in OPFS, simply copy zName to pOut... */
    const i = wasm.cstrncpy(pOut, zName, nOut);
    return i<nOut ? 0 : capi.SQLITE_CANTOPEN
    /*CANTOPEN is required by the docs but SQLITE_RANGE would be a closer match*/;
  })
  ('xAccess', function(pVfs,zName,flags,pOut){
    error("OPFS sqlite3_vfs::xAccess is not yet implemented.");
    let fileExists = 0;
    switch(flags){
        case capi.SQLITE_ACCESS_EXISTS: break;
        case capi.SQLITE_ACCESS_READWRITE: break;
        case capi.SQLITE_ACCESS_READ/*docs say this is never used*/:
        default:
          error("Unexpected flags value for sqlite3_vfs::xAccess():",flags);
          return capi.SQLITE_MISUSE;
    }
    wasm.setMemValue(pOut, fileExists, 'i32');
    return 0;
  })
  ('xDelete', function(pVfs, zName, doSyncDir){
    error("OPFS sqlite3_vfs::xDelete is not yet implemented.");
    return capi.SQLITE_IOERR;
  })
  ('xGetLastError', function(pVfs,nOut,pOut){
    debug("OPFS sqlite3_vfs::xGetLastError() has nothing sensible to return.");
    return 0;
  })
  ('xCurrentTime', function(pVfs,pOut){
    /* If it turns out that we need to adjust for timezone, see:
       https://stackoverflow.com/a/11760121/1458521 */
    wasm.setMemValue(pOut, 2440587.5 + (new Date().getTime()/86400000),
                     'double');
    return 0;
  })
  ('xCurrentTimeInt64',function(pVfs,pOut){
    // TODO: confirm that this calculation is correct
    wasm.setMemValue(pOut, (2440587.5 * 86400000) + new Date().getTime(),
                     'i64');
    return 0;
  });
  if(!oVfs.$xSleep){
    inst('xSleep', function(pVfs,ms){
      error("sqlite3_vfs::xSleep(",ms,") cannot be implemented from "+
           "JS and we have no default VFS to copy the impl from.");
      return 0;
    });
  }
  if(!oVfs.$xRandomness){
    inst('xRandomness', function(pVfs, nOut, pOut){
      const heap = wasm.heap8u();
      let i = 0;
      for(; i < nOut; ++i) heap[pOut + i] = (Math.random()*255000) & 0xFF;
      return i;
    });
  }

  ////////////////////////////////////////////////////////////////////////
  // Set up OPFS sqlite3_io_methods...
  inst = installMethod(oIom);
  inst('xClose', async function(pFile){
    warn("xClose(",arguments,") uses await");
    const f = __opfsHandles[pFile];
    delete __opfsHandles[pFile];
    if(f.opfsHandle){
      await f.opfsHandle.close();
      if(f.deleteOnClose){
        // TODO
      }
    }
    f.dispose();
    return 0;
  })
  ('xRead', /*i(ppij)*/function(pFile,pDest,n,offset){
    /* int (*xRead)(sqlite3_file*, void*, int iAmt, sqlite3_int64 iOfst) */
    try {
      const f = __opfsHandles[pFile];
      const heap = wasm.heap8u();
      const b = new Uint8Array(heap.buffer, pDest, n);
      const nRead = f.opfsHandle.read(b, {at: offset});
      if(nRead<n){
        // MUST zero-fill short reads (per the docs)
        heap.fill(0, dest + nRead, n - nRead);
      }
      return 0;
    }catch(e){
      error("xRead(",arguments,") failed:",e);
      return capi.SQLITE_IOERR_READ;
    }
  })
  ('xWrite', /*i(ppij)*/function(pFile,pSrc,n,offset){
    /* int (*xWrite)(sqlite3_file*, const void*, int iAmt, sqlite3_int64 iOfst) */
    try {
      const f = __opfsHandles[pFile];
      const b = new Uint8Array(wasm.heap8u().buffer, pSrc, n);
      const nOut = f.opfsHandle.write(b, {at: offset});
      if(nOut<n){
        error("xWrite(",arguments,") short write!");
        return capi.SQLITE_IOERR_WRITE;
      }
      return 0;
    }catch(e){
      error("xWrite(",arguments,") failed:",e);
      return capi.SQLITE_IOERR_WRITE;
    }
  })
  ('xTruncate', /*i(pj)*/async function(pFile,sz){
    /* int (*xTruncate)(sqlite3_file*, sqlite3_int64 size) */
    try{
      warn("xTruncate(",arguments,") uses await");
      const f = __opfsHandles[pFile];
      await f.opfsHandle.truncate(sz);
      return 0;
    }
    catch(e){
      error("xTruncate(",arguments,") failed:",e);
      return capi.SQLITE_IOERR_TRUNCATE;
    }
  })
  ('xSync', /*i(pi)*/async function(pFile,flags){
    /* int (*xSync)(sqlite3_file*, int flags) */
    try {
      warn("xSync(",arguments,") uses await");
      const f = __opfsHandles[pFile];
      await f.opfsHandle.flush();
      return 0;
    }catch(e){
      error("xSync(",arguments,") failed:",e);
      return capi.SQLITE_IOERR_SYNC;
    }
  })
  ('xFileSize', /*i(pp)*/async function(pFile,pSz){
    /* int (*xFileSize)(sqlite3_file*, sqlite3_int64 *pSize) */
    try {
      warn("xFileSize(",arguments,") uses await");
      const f = __opfsHandles[pFile];
      const fsz = await f.opfsHandle.getSize();
      capi.wasm.setMemValue(pSz, fsz,'i64');
      return 0;
    }catch(e){
      error("xFileSize(",arguments,") failed:",e);
      return capi.SQLITE_IOERR_SEEK;
    }
  })
  ('xLock', /*i(pi)*/function(pFile,lockType){
    /* int (*xLock)(sqlite3_file*, int) */
    // Opening a handle locks it automatically.
    warn("xLock(",arguments,") is a no-op");
    return 0;
  })
  ('xUnlock', /*i(pi)*/function(pFile,lockType){
    /* int (*xUnlock)(sqlite3_file*, int) */
    // Opening a handle locks it automatically.
    warn("xUnlock(",arguments,") is a no-op");
    return 0;
  })
  ('xCheckReservedLock', /*i(pp)*/function(pFile,pOut){
    /* int (*xCheckReservedLock)(sqlite3_file*, int *pResOut) */
    // Exclusive lock is automatically acquired when opened
    warn("xCheckReservedLock(",arguments,") is a no-op");
    wasm.setMemValue(pOut,1,'i32');
    return 0;
  })
  ('xFileControl', /*i(pip)*/function(pFile,op,pArg){
    /* int (*xFileControl)(sqlite3_file*, int op, void *pArg) */
    debug("xFileControl(",arguments,") is a no-op");
    return capi.SQLITE_NOTFOUND;
  })
  ('xDeviceCharacteristics',/*i(p)*/function(pFile){
    /* int (*xDeviceCharacteristics)(sqlite3_file*) */
    debug("xDeviceCharacteristics(",pFile,")");
    return capi.SQLITE_IOCAP_UNDELETABLE_WHEN_OPEN;
  });
  // xSectorSize may be NULL
  //('xSectorSize', function(pFile){
  //  /* int (*xSectorSize)(sqlite3_file*) */
  //  log("xSectorSize(",pFile,")");
  //  return 4096 /* ==> SQLITE_DEFAULT_SECTOR_SIZE */;
  //})

  const rc = capi.sqlite3_vfs_register(oVfs.pointer, 0);
  if(rc){
    oVfs.dispose();
    toss("sqlite3_vfs_register(OPFS) failed with rc",rc);
  }
  capi.sqlite3_vfs_register.addReference(oVfs, oIom);
  warn("End of (very incomplete) OPFS setup.", oVfs);
  //oVfs.dispose()/*only because we can't yet do anything with it*/;
});
