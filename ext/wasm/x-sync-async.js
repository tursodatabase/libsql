/*
  2022-09-17

  The author disclaims copyright to this source code.  In place of a
  legal notice, here is a blessing:

  *   May you do good and not evil.
  *   May you find forgiveness for yourself and forgive others.
  *   May you share freely, never taking more than you give.

  ***********************************************************************

  An INCOMPLETE and UNDER CONSTRUCTION experiment for OPFS.
  This file holds the synchronous half of an sqlite3_vfs
  implementation which proxies, in a synchronous fashion, the
  asynchronous OPFS APIs using a second Worker, implemented
  in sqlite3-opfs-async-proxy.js.

  Summary of how this works:

  This file uses the sqlite3.StructBinder-created struct wrappers for
  sqlite3_vfs, sqlite3_io_methods, ans sqlite3_file to set up a
  conventional sqlite3_vfs (except that it's implemented in JS). The
  methods which require OPFS APIs use a separate worker (hereafter called the
  OPFS worker) to access that functionality. This worker and that one
  use SharedBufferArray
*/
'use strict';
/**
   This function is a placeholder for use in development. When
   working, this will be moved into a file named
   api/sqlite3-api-opfs.js, or similar, and hooked in to the
   sqlite-api build construct.
*/
const initOpfsVfs = function(sqlite3){
  const toss = function(...args){throw new Error(args.join(' '))};
  const logPrefix = "OPFS syncer:";
  const log = (...args)=>{
    console.log(logPrefix,...args);
  };
  const warn =  (...args)=>{
    console.warn(logPrefix,...args);
  };
  const error =  (...args)=>{
    console.error(logPrefix,...args);
  };
  warn("This file is very much experimental and under construction.",self.location.pathname);

  if(self.window===self ||
     !self.SharedBufferArray ||
     !self.FileSystemHandle ||
     !self.FileSystemDirectoryHandle ||
     !self.FileSystemFileHandle ||
     !self.FileSystemFileHandle.prototype.createSyncAccessHandle ||
     !navigator.storage.getDirectory){
    warn("This environment does not have OPFS support.");
  }

  const capi = sqlite3.capi;
  const wasm = capi.wasm;
  const sqlite3_vfs = capi.sqlite3_vfs
        || toss("Missing sqlite3.capi.sqlite3_vfs object.");
  const sqlite3_file = capi.sqlite3_file
        || toss("Missing sqlite3.capi.sqlite3_file object.");
  const sqlite3_io_methods = capi.sqlite3_io_methods
        || toss("Missing sqlite3.capi.sqlite3_io_methods object.");

  const W = new Worker("sqlite3-opfs-async-proxy.js");
  const wMsg = (type,payload)=>W.postMessage({type,payload});

  /**
     State which we send to the async-api Worker or share with it.
     This object must initially contain only cloneable or sharable
     objects. After the worker's "inited" message arrives, other types
     of data may be added to it.
  */
  const state = Object.create(null);
  state.verbose = 3;
  state.fileBufferSize = 1024 * 64 + 8 /* size of fileHandle.sab. 64k = max sqlite3 page size */;
  state.fbInt64Offset = state.fileBufferSize - 8 /*spot in fileHandle.sab to store an int64*/;
  state.opIds = Object.create(null);
  {
    let i = 0;
    state.opIds.xAccess = i++;
    state.opIds.xClose = i++;
    state.opIds.xDelete = i++;
    state.opIds.xFileSize = i++;
    state.opIds.xOpen = i++;
    state.opIds.xRead = i++;
    state.opIds.xSleep = i++;
    state.opIds.xSync = i++;
    state.opIds.xTruncate = i++;
    state.opIds.xWrite = i++;
    state.opSAB = new SharedArrayBuffer(i * 4/*sizeof int32*/);
  }

  state.sq3Codes = Object.create(null);
  state.sq3Codes._reverse = Object.create(null);
  [ // SQLITE_xxx constants to export to the async worker counterpart...
    'SQLITE_ERROR', 'SQLITE_IOERR',
    'SQLITE_NOTFOUND', 'SQLITE_MISUSE',
    'SQLITE_IOERR_READ', 'SQLITE_IOERR_SHORT_READ',
    'SQLITE_IOERR_WRITE', 'SQLITE_IOERR_FSYNC',
    'SQLITE_IOERR_TRUNCATE', 'SQLITE_IOERR_DELETE',
    'SQLITE_IOERR_ACCESS', 'SQLITE_IOERR_CLOSE'
  ].forEach(function(k){
    state.sq3Codes[k] = capi[k] || toss("Maintenance required: not found:",k);
    state.sq3Codes._reverse[capi[k]] = k;
  });

  const isWorkerErrCode = (n)=>!!state.sq3Codes._reverse[n];
  
  const opStore = (op,val=-1)=>Atomics.store(state.opSABView, state.opIds[op], val);
  const opWait = (op,val=-1)=>Atomics.wait(state.opSABView, state.opIds[op], val);

  /**
     Runs the given operation in the async worker counterpart, waits
     for its response, and returns the result which the async worker
     writes to the given op's index in state.opSABView. The 2nd argument
     must be a single object or primitive value, depending on the
     given operation's signature in the async API counterpart.
  */
  const opRun = (op,args)=>{
    opStore(op);
    wMsg(op, args);
    opWait(op);
    return Atomics.load(state.opSABView, state.opIds[op]);
  };

  const wait = (ms,value)=>{
    return new Promise((resolve)=>{
      setTimeout(()=>resolve(value), ms);
    });
  };

  /**
     Generates a random ASCII string len characters long, intended for
     use as a temporary file name.
  */
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

  /**
     Map of sqlite3_file pointers to objects constructed by xOpen().
  */
  const __openFiles = Object.create(null);
  
  const pDVfs = capi.sqlite3_vfs_find(null)/*pointer to default VFS*/;
  const dVfs = pDVfs
        ? new sqlite3_vfs(pDVfs)
        : null /* dVfs will be null when sqlite3 is built with
                  SQLITE_OS_OTHER. Though we cannot currently handle
                  that case, the hope is to eventually be able to. */;
  const opfsVfs = new sqlite3_vfs();
  const opfsIoMethods = new sqlite3_io_methods();
  opfsVfs.$iVersion = 2/*yes, two*/;
  opfsVfs.$szOsFile = capi.sqlite3_file.structInfo.sizeof;
  opfsVfs.$mxPathname = 1024/*sure, why not?*/;
  opfsVfs.$zName = wasm.allocCString("opfs");
  // All C-side memory of opfsVfs is zeroed out, but just to be explicit:
  opfsVfs.$xDlOpen = opfsVfs.$xDlError = opfsVfs.$xDlSym = opfsVfs.$xDlClose = null;
  opfsVfs.ondispose = [
    '$zName', opfsVfs.$zName,
    'cleanup default VFS wrapper', ()=>(dVfs ? dVfs.dispose() : null),
    'cleanup opfsIoMethods', ()=>opfsIoMethods.dispose()
  ];
  if(dVfs){
    opfsVfs.$xSleep = dVfs.$xSleep;
    opfsVfs.$xRandomness = dVfs.$xRandomness;
  }
  /**
     Pedantic sidebar about opfsVfs.ondispose: the entries in that array
     are items to clean up when opfsVfs.dispose() is called, but in this
     environment it will never be called. The VFS instance simply
     hangs around until the WASM module instance is cleaned up. We
     "could" _hypothetically_ clean it up by "importing" an
     sqlite3_os_end() impl into the wasm build, but the shutdown order
     of the wasm engine and the JS one are undefined so there is no
     guaranty that the opfsVfs instance would be available in one
     environment or the other when sqlite3_os_end() is called (_if_ it
     gets called at all in a wasm build, which is undefined).
  */
  
  /**
     Impls for the sqlite3_io_methods methods. Maintenance reminder:
     members are in alphabetical order to simplify finding them.
  */
  const ioSyncWrappers = {
    xCheckReservedLock: function(pFile,pOut){
      // Exclusive lock is automatically acquired when opened
      //warn("xCheckReservedLock(",arguments,") is a no-op");
      wasm.setMemValue(pOut,1,'i32');
      return 0;
    },
    xClose: function(pFile){
      let rc = 0;
      const f = __openFiles[pFile];
      if(f){
        delete __openFiles[pFile];
        rc = opRun('xClose', pFile);
        if(f.sq3File) f.sq3File.dispose();
      }
      return rc;
    },
    xDeviceCharacteristics: function(pFile){
      //debug("xDeviceCharacteristics(",pFile,")");
      return capi.SQLITE_IOCAP_UNDELETABLE_WHEN_OPEN;
    },
    xFileControl: function(pFile,op,pArg){
      //debug("xFileControl(",arguments,") is a no-op");
      return capi.SQLITE_NOTFOUND;
    },
    xFileSize: function(pFile,pSz64){
      const rc = opRun('xFileSize', pFile);
      if(!isWorkerErrCode(rc)){
        const f = __openFiles[pFile];
        wasm.setMemValue(pSz64, f.sabViewFileSize.getBigInt64(0) ,'i64');
      }
      return rc;
    },
    xLock: function(pFile,lockType){
      //2022-09: OPFS handles lock when opened
      //warn("xLock(",arguments,") is a no-op");
      return 0;
    },
    xRead: function(pFile,pDest,n,offset){
      /* int (*xRead)(sqlite3_file*, void*, int iAmt, sqlite3_int64 iOfst) */
      const f = __opfsHandles[pFile];
      try {
        // FIXME(?): block until we finish copying the xRead result buffer. How?
        let rc = opRun('xRead',{fid:pFile, n, offset});
        if(0!==rc) return rc;
        let i = 0;
        for(; i < n; ++i) wasm.setMemValue(pDest + i, f.sabView[i]);
      }catch(e){
        error("xRead(",arguments,") failed:",e,f);
        rc = capi.SQLITE_IOERR_READ;
      }
      return rc;
    },
    xSync: function(pFile,flags){
      return opRun('xSync', {fid:pFile, flags});
    },
    xTruncate: function(pFile,sz64){
      return opRun('xTruncate', {fid:pFile, size: sz64});
    },
    xUnlock: function(pFile,lockType){
      //2022-09: OPFS handles lock when opened
      //warn("xUnlock(",arguments,") is a no-op");
      return 0;
    },
    xWrite: function(pFile,pSrc,n,offset){
    /* int (*xWrite)(sqlite3_file*, const void*, int iAmt, sqlite3_int64 iOfst) */
      const f = __opfsHandles[pFile];
      try {
        let i = 0;
        // FIXME(?): block from here until we finish the xWrite. How?
        for(; i < n; ++i) f.sabView[i] = wasm.getMemValue(pSrc+i);
        return opRun('xWrite',{fid:pFile, n, offset});
      }catch(e){
        error("xWrite(",arguments,") failed:",e,f);
        return capi.SQLITE_IOERR_WRITE;
      }
    }
  }/*ioSyncWrappers*/;
  
  /**
     Impls for the sqlite3_vfs methods. Maintenance reminder: members
     are in alphabetical order to simplify finding them.
  */
  const vfsSyncWrappers = {
    // TODO: xAccess
    xCurrentTime: function(pVfs,pOut){
      /* If it turns out that we need to adjust for timezone, see:
         https://stackoverflow.com/a/11760121/1458521 */
      wasm.setMemValue(pOut, 2440587.5 + (new Date().getTime()/86400000),
                       'double');
      return 0;
    },
    xCurrentTimeInt64: function(pVfs,pOut){
      // TODO: confirm that this calculation is correct
      wasm.setMemValue(pOut, (2440587.5 * 86400000) + new Date().getTime(),
                       'i64');
      return 0;
    },
    xDelete: function(pVfs, zName, doSyncDir){
      return opRun('xDelete', {filename: wasm.cstringToJs(zName), syncDir: doSyncDir});
    },
    xFullPathname: function(pVfs,zName,nOut,pOut){
      /* Until/unless we have some notion of "current dir"
         in OPFS, simply copy zName to pOut... */
      const i = wasm.cstrncpy(pOut, zName, nOut);
      return i<nOut ? 0 : capi.SQLITE_CANTOPEN
      /*CANTOPEN is required by the docs but SQLITE_RANGE would be a closer match*/;
    },
    xGetLastError: function(pVfs,nOut,pOut){
      /* TODO: store exception.message values from the async
         partner in a dedicated SharedArrayBuffer, noting that we'd have
         to encode them... TextEncoder can do that for us. */
      warn("OPFS xGetLastError() has nothing sensible to return.");
      return 0;
    },
    xOpen: function f(pVfs, zName, pFile, flags, pOutFlags){
      if(!f._){
        f._ = {
          fileTypes: {
            SQLITE_OPEN_MAIN_DB: 'mainDb',
            SQLITE_OPEN_MAIN_JOURNAL: 'mainJournal',
            SQLITE_OPEN_TEMP_DB: 'tempDb',
            SQLITE_OPEN_TEMP_JOURNAL: 'tempJournal',
            SQLITE_OPEN_TRANSIENT_DB: 'transientDb',
            SQLITE_OPEN_SUBJOURNAL: 'subjournal',
            SQLITE_OPEN_SUPER_JOURNAL: 'superJournal',
            SQLITE_OPEN_WAL: 'wal'
          },
          getFileType: function(filename,oflags){
            const ft = f._.fileTypes;
            for(let k of Object.keys(ft)){
              if(oflags & capi[k]) return ft[k];
            }
            warn("Cannot determine fileType based on xOpen() flags for file",filename);
            return '???';
          }
        };
      }
      if(0===zName){
        zName = randomFilename();
      }else if('number'===typeof zName){
        zName = wasm.cstringToJs(zName);
      }
      const args = Object.create(null);
      args.fid = pFile;
      args.filename = zName;
      args.sab = new SharedArrayBuffer(state.fileBufferSize);
      args.fileType = f._.getFileType(args.filename, flags);
      args.create = !!(flags & capi.SQLITE_OPEN_CREATE);
      args.deleteOnClose = !!(flags & capi.SQLITE_OPEN_DELETEONCLOSE);
      args.readOnly = !!(flags & capi.SQLITE_OPEN_READONLY);
      const rc = opRun('xOpen', args);
      if(!rc){
        /* Recall that sqlite3_vfs::xClose() will be called, even on
           error, unless pFile->pMethods is NULL. */
        if(args.readOnly){
          wasm.setMemValue(pOutFlags, capi.SQLITE_OPEN_READONLY, 'i32');
        }
        __openFiles[pFile] = args;
        args.sabView = new Uint8Array(args.sab);
        args.sabViewFileSize = new DataView(args.sab, state.fbInt64Offset, 8);
        args.sq3File = new sqlite3_file(pFile);
        args.sq3File.$pMethods = opfsIoMethods.pointer;
        args.ba = new Uint8Array(args.sab);
      }
      return rc;
    }/*xOpen()*/
  }/*vfsSyncWrappers*/;

  if(!opfsVfs.$xRandomness){
    /* If the default VFS has no xRandomness(), add a basic JS impl... */
    vfsSyncWrappers.xRandomness = function(pVfs, nOut, pOut){
      const heap = wasm.heap8u();
      let i = 0;
      for(; i < nOut; ++i) heap[pOut + i] = (Math.random()*255000) & 0xFF;
      return i;
    };
  }
  if(!opfsVfs.$xSleep){
    /* If we can inherit an xSleep() impl from the default VFS then
       use it, otherwise install one which is certainly less accurate
       because it has to go round-trip through the async worker, but
       provides the only option for a synchronous sleep() in JS. */
    vfsSyncWrappers.xSleep = (pVfs,ms)=>opRun('xSleep',ms);
  }

  /*
    TODO: plug in the above functions in to opfsVfs and opfsIoMethods.
    Code for doing so is in api/sqlite3-api-opfs.js.
  */
  
  const sanityCheck = async function(){
    //state.ioBuf = new Uint8Array(state.sabIo);
    const scope = wasm.scopedAllocPush();
    const sq3File = new sqlite3_file();
    try{
      const fid = sq3File.pointer;
      const openFlags = capi.SQLITE_OPEN_CREATE
            | capi.SQLITE_OPEN_READWRITE
            | capi.SQLITE_OPEN_DELETEONCLOSE
            | capi.SQLITE_OPEN_MAIN_DB;
      const pOut = wasm.scopedAlloc(8);
      const dbFile = "/sanity/check/file";
      let rc = vfsSyncWrappers.xOpen(opfsVfs.pointer, dbFile,
                                     fid, openFlags, pOut);
      log("open rc =",rc,"state.opSABView[xOpen] =",state.opSABView[state.opIds.xOpen]);
      if(isWorkerErrCode(rc)){
        error("open failed with code",rc);
        return;
      }
      rc = ioSyncWrappers.xSync(sq3File.pointer, 0);
      if(rc) toss('sync failed w/ rc',rc);
      rc = ioSyncWrappers.xTruncate(sq3File.pointer, 1024);
      if(rc) toss('truncate failed w/ rc',rc);
      wasm.setMemValue(pOut,0,'i64');
      rc = ioSyncWrappers.xFileSize(sq3File.pointer, pOut);
      if(rc) toss('xFileSize failed w/ rc',rc);
      log("xFileSize says:",wasm.getMemValue(pOut, 'i64'));
      log("xSleep()ing before close()ing...");
      opRun('xSleep',1500);
      rc = ioSyncWrappers.xClose(fid);
      log("xClose rc =",rc,"opSABView =",state.opSABView);
      log("Deleting file:",dbFile);
      opRun('xDelete', dbFile);
    }finally{
      sq3File.dispose();
      wasm.scopedAllocPop(scope);
    }
  };

  
  W.onmessage = function({data}){
    log("Worker.onmessage:",data);
    switch(data.type){
        case 'loaded':
          /*Pass our config and shared state on to the async worker.*/
          wMsg('init',state);
          break;
        case 'inited':{
          /*Indicates that the async partner has received the 'init',
            so we now know that the state object is no longer subject to
            being copied by a pending postMessage() call.*/
          try {
            const rc = capi.sqlite3_vfs_register(opfsVfs.pointer, opfsVfs.$zName);
            if(rc){
              opfsVfs.dispose();
              toss("sqlite3_vfs_register(OPFS) failed with rc",rc);
            }
            if(opfsVfs.pointer !== capi.sqlite3_vfs_find("opfs")){
              toss("BUG: sqlite3_vfs_find() failed for just-installed OPFS VFS");
            }
            capi.sqlite3_vfs_register.addReference(opfsVfs, opfsIoMethods);
            state.opSABView = new Int32Array(state.opSAB);
            if(self.location && +self.location.port > 1024){
              log("Running sanity check for dev mode...");
              sanityCheck();
            }
            warn("End of (very incomplete) OPFS setup.", opfsVfs);
          }catch(e){
            error(e);
          }
          break;
        }
        default:
          error("Unexpected message from the async worker:",data);
          break;
    }
  };
}/*initOpfsVfs*/

importScripts('sqlite3.js');
self.sqlite3InitModule().then((EmscriptenModule)=>initOpfsVfs(EmscriptenModule.sqlite3));
