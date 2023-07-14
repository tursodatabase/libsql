/*
  2023-07-14

  The author disclaims copyright to this source code.  In place of a
  legal notice, here is a blessing:

  *   May you do good and not evil.
  *   May you find forgiveness for yourself and forgive others.
  *   May you share freely, never taking more than you give.

  ***********************************************************************

  INCOMPLETE! WORK IN PROGRESS!

  This file holds an experimental sqlite3_vfs backed by OPFS storage
  which uses a different implementation strategy than the "opfs"
  VFS. This one is a port of Roy Hashimoto's OPFS SyncAccessHandle
  pool:

  https://github.com/rhashimoto/wa-sqlite/blob/master/src/examples/AccessHandlePoolVFS.js

  As described at:

  https://github.com/rhashimoto/wa-sqlite/discussions/67

  with Roy's explicit permission to permit us to port his to our
  infrastructure rather than having to clean-room reverse-engineer it:

  https://sqlite.org/forum/forumpost/e140d84e71

  Primary differences from the original "opfs" VFS include:

  - This one avoids the need for a sub-worker to synchronize
  communication between the synchronous C API and the only-partly
  synchronous OPFS API.

  - It does so by opening a fixed number of OPFS files at
  library-level initialization time, obtaining SyncAccessHandles to
  each, and manipulating those handles via the synchronous sqlite3_vfs
  interface.

  - Because of that, this one lacks all library-level concurrency
  support.

  - Also because of that, it does not require the SharedArrayBuffer,
  so can function without the COOP/COEP HTTP response headers.

  - It can hypothetically support Safari 16.4+, whereas the "opfs"
  VFS requires v17 due to a bug in 16.x which makes it incompatible
  with that VFS.

  - This VFS requires the "semi-fully-sync" FileSystemSyncAccessHandle
  (hereafter "SAH") APIs released with Chrome v108. There is
  unfortunately no known programmatic way to determine whether a given
  API is from that release or newer without actually calling it and
  checking whether one of the "fully-sync" functions returns a Promise
  (in which case it's the older version). (Reminder to self: when
  opening up the initial pool of files, we can close() the first one
  we open and see if close() returns a Promise. If it does, it's the
  older version so fail VFS initialization. If it doesn't, re-open it.)

*/
'use strict';
globalThis.sqlite3ApiBootstrap.initializers.push(function(sqlite3){
const installOpfsVfs = async function(sqlite3){
  const pToss = (...args)=>Promise.reject(new Error(args.join(' ')));
  if(!globalThis.FileSystemHandle ||
     !globalThis.FileSystemDirectoryHandle ||
     !globalThis.FileSystemFileHandle ||
     !globalThis.FileSystemFileHandle.prototype.createSyncAccessHandle ||
     !navigator?.storage?.getDirectory){
    return pToss("Missing required OPFS APIs.");
  }
  const thePromise = new Promise(function(promiseResolve, promiseReject_){
    const verbosity = 3;
    const loggers = [
      sqlite3.config.error,
      sqlite3.config.warn,
      sqlite3.config.log
    ];
    const logImpl = (level,...args)=>{
      if(verbosity>level) loggers[level]("opfs-sahpool:",...args);
    };
    const log =    (...args)=>logImpl(2, ...args);
    const warn =   (...args)=>logImpl(1, ...args);
    const error =  (...args)=>logImpl(0, ...args);
    const toss = sqlite3.util.toss;
    const capi = sqlite3.capi;
    const wasm = sqlite3.wasm;
    const opfsIoMethods = new capi.sqlite3_io_methods();
    const opfsVfs = new capi.sqlite3_vfs()
          .addOnDispose(()=>opfsIoMethods.dispose());
    const promiseReject = (err)=>{
      opfsVfs.dispose();
      return promiseReject_(err);
    };

    // Config opts for the VFS...
    const SECTOR_SIZE = 4096;
    const HEADER_MAX_PATH_SIZE = 512;
    const HEADER_FLAGS_SIZE = 4;
    const HEADER_DIGEST_SIZE = 8;
    const HEADER_CORPUS_SIZE = HEADER_MAX_PATH_SIZE + HEADER_FLAGS_SIZE;
    const HEADER_OFFSET_FLAGS = HEADER_MAX_PATH_SIZE;
    const HEADER_OFFSET_DIGEST = HEADER_CORPUS_SIZE;
    const HEADER_OFFSET_DATA = SECTOR_SIZE;
    const DEFAULT_CAPACITY = 6;
    /* Bitmask of file types which may persist across sessions.
       SQLITE_OPEN_xyz types not listed here may be inadvertently
       left in OPFS but are treated as transient by this VFS and
       they will be cleaned up during VFS init. */
    const PERSISTENT_FILE_TYPES =
          capi.SQLITE_OPEN_MAIN_DB |
          capi.SQLITE_OPEN_MAIN_JOURNAL |
          capi.SQLITE_OPEN_SUPER_JOURNAL |
          capi.SQLITE_OPEN_WAL /* noting that WAL support is
                                  unavailable in the WASM build.*/;
    const pDVfs = capi.sqlite3_vfs_find(null)/*default VFS*/;
    const dVfs = pDVfs
          ? new capi.sqlite3_vfs(pDVfs)
          : null /* dVfs will be null when sqlite3 is built with
                    SQLITE_OS_OTHER. */;
    opfsVfs.$iVersion = 2/*yes, two*/;
    opfsVfs.$szOsFile = capi.sqlite3_file.structInfo.sizeof;
    opfsVfs.$mxPathname = HEADER_MAX_PATH_SIZE;
    opfsVfs.$zName = wasm.allocCString("opfs-sahpool");
    log('opfsVfs.$zName =',opfsVfs.$zName);
    opfsVfs.addOnDispose(
      '$zName', opfsVfs.$zName,
      'cleanup default VFS wrapper', ()=>(dVfs ? dVfs.dispose() : null)
    );

    const getFilename = function(ndx){
      return 'sahpool-'+('00'+ndx).substr(-3);
    }

    const SAHPool = Object.assign(Object.create(null),{
      /* OPFS dir in which VFS metadata is stored. */
      vfsDir: ".sqlite3-opfs-sahpool",
      dirHandle: undefined,
      /* Maps OPFS access handles to their opaque file names. */
      mapAH2Name: new Map(),
      mapPath2AH: new Map(),
      availableAH: new Set(),
      mapId2File: new Map(),
      getCapacity: function(){return this.mapAH2Name.size},
      getFileCount: function(){return this.mapPath2AH.size},
      addCapacity: async function(n){
        const cap = this.getCapacity();
        for(let i = cap; i < cap+n; ++i){
          const name = getFilename(i);
          const h = await this.dirHandle.getFileHandle(name, {create:true});
          let ah = await h.createSyncAccessHandle();
          if(0===i){
            /* Ensure that this client has the "all-synchronous"
               OPFS API and fail if they don't. */
            if(undefined !== ah.close()){
              toss("OPFS API is too old for opfs-sahpool:",
                   "it has an async close() method.");
            }
            ah = await h.createSyncAccessHandle();
          }
          this.mapAH2Name.set(ah,name);
          this.setAssociatedPath(ah, '', 0);
        }
      },
      setAssociatedPath: function(accessHandle, path, flags){
        // TODO
      },
      releaseAccessHandles: function(){
        for(const ah of this.mapAH2Name.keys()) ah.close();
        this.mapAH2Name.clear();
        this.mapPath2AH.clear();
        this.availableAH.clear();
      },
      acquireAccessHandles: async function(){
        // TODO
      },
      reset: async function(){
        await this.isReady;
        let h = await navigator.storage.getDirectory();
        for(const d of this.vfsDir.split('/')){
          if(d){
            h = await h.getDirectoryHandle(d,{create:true});
          }
        }
        this.dirHandle = h;
        this.releaseAccessHandles();
        await this.acquireAccessHandles();
      }
      // much more TODO
    })/*SAHPool*/;
    sqlite3.SAHPool = SAHPool/*only for testing*/;
    // Much, much more TODO...
    /**
       Impls for the sqlite3_io_methods methods. Maintenance reminder:
       members are in alphabetical order to simplify finding them.
    */
    const ioSyncWrappers = {
      xCheckReservedLock: function(pFile,pOut){
        return 0;
      },
      xClose: function(pFile){
        let rc = 0;
        return rc;
      },
      xDeviceCharacteristics: function(pFile){
        return capi.SQLITE_IOCAP_UNDELETABLE_WHEN_OPEN;
      },
      xFileControl: function(pFile, opId, pArg){
        return capi.SQLITE_NOTFOUND;
      },
      xFileSize: function(pFile,pSz64){
        let rc = 0;
        return rc;
      },
      xLock: function(pFile,lockType){
        let rc = capi.SQLITE_IOERR_LOCK;
        return rc;
      },
      xRead: function(pFile,pDest,n,offset64){
        let rc = capi.SQLITE_IOERR_READ;
        return rc;
      },
      xSync: function(pFile,flags){
        let rc = capi.SQLITE_IOERR_FSYNC;
        return rc;
      },
      xTruncate: function(pFile,sz64){
        let rc = capi.SQLITE_IOERR_TRUNCATE;
        return rc;
      },
      xUnlock: function(pFile,lockType){
        let rc = capi.SQLITE_IOERR_UNLOCK;
        return rc;
      },
      xWrite: function(pFile,pSrc,n,offset64){
        let rc = capi.SQLITE_IOERR_WRITE;
        return rc;
      }
    }/*ioSyncWrappers*/;

    /**
       Impls for the sqlite3_vfs methods. Maintenance reminder: members
       are in alphabetical order to simplify finding them.
    */
    const vfsSyncWrappers = {
      xAccess: function(pVfs,zName,flags,pOut){
        const rc = capi.SQLITE_ERROR;
        return rc;
      },
      xCurrentTime: function(pVfs,pOut){
        wasm.poke(pOut, 2440587.5 + (new Date().getTime()/86400000),
                  'double');
        return 0;
      },
      xCurrentTimeInt64: function(pVfs,pOut){
        wasm.poke(pOut, (2440587.5 * 86400000) + new Date().getTime(),
                  'i64');
        return 0;
      },
      xDelete: function(pVfs, zName, doSyncDir){
        const rc = capi.SQLITE_ERROR;
        return rc;
      },
      xFullPathname: function(pVfs,zName,nOut,pOut){
        const i = wasm.cstrncpy(pOut, zName, nOut);
        return i<nOut ? 0 : capi.SQLITE_CANTOPEN;
      },
      xGetLastError: function(pVfs,nOut,pOut){
        /* TODO: store exception state somewhere and serve
           it from here. */
        warn("OPFS xGetLastError() has nothing sensible to return.");
        return 0;
      },
      //xSleep is optionally defined below
      xOpen: function f(pVfs, zName, pFile, flags, pOutFlags){
        let rc = capi.SQLITE_ERROR;
        return rc;
      }/*xOpen()*/
    }/*vfsSyncWrappers*/;

    if(dVfs){
      /* Inherit certain VFS members from the default VFS,
         if available. */
      opfsVfs.$xRandomness = dVfs.$xRandomness;
      opfsVfs.$xSleep = dVfs.$xSleep;
    }
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
      vfsSyncWrappers.xSleep = function(pVfs,ms){
        return 0;
      };
    }

    SAHPool.isReady = SAHPool.reset().then(async ()=>{
      if(0===SAHPool.getCapacity()){
        await SAHPool.addCapacity(DEFAULT_CAPACITY);
      }
      log("vfs list:",capi.sqlite3_js_vfs_list());
      sqlite3.vfs.installVfs({
        io: {struct: opfsIoMethods, methods: ioSyncWrappers},
        vfs: {struct: opfsVfs, methods: vfsSyncWrappers}
      });
      log("vfs list:",capi.sqlite3_js_vfs_list());
      log("opfs-sahpool VFS initialized.");
      promiseResolve(sqlite3);
    }).catch(promiseReject);
  })/*thePromise*/;
  return thePromise;
}/*installOpfsVfs()*/;

globalThis.sqlite3ApiBootstrap.initializersAsync.push(async (sqlite3)=>{
  return installOpfsVfs(sqlite3).catch((e)=>{
    sqlite3.config.warn("Ignoring inability to install opfs-sahpool sqlite3_vfs:",
                        e.message);
  });
}/*sqlite3ApiBootstrap.initializersAsync*/);
}/*sqlite3ApiBootstrap.initializers*/);
