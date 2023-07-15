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
  if(!globalThis.FileSystemHandle ||
     !globalThis.FileSystemDirectoryHandle ||
     !globalThis.FileSystemFileHandle ||
     !globalThis.FileSystemFileHandle.prototype.createSyncAccessHandle ||
     !navigator?.storage?.getDirectory){
    return Promise.reject(new Error("Missing required OPFS APIs."));
  }
  return new Promise(async function(promiseResolve, promiseReject_){
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

    const getFilename = false
          ? (ndx)=>'sahpool-'+('00'+ndx).substr(-3)
          : ()=>Math.random().toString(36).slice(2)

    /**
       All state for the VFS.
    */
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
          const name = getFilename(i)
          /* Reminder: because of how removeCapacity() works, we
             really want random names. At this point in the dev
             process that fills up the OPFS with randomly-named files
             each time the page is reloaded, so delay the return to
             random names until we've gotten far enough to eliminate
             that problem. */;
          const h = await this.dirHandle.getFileHandle(name, {create:true});
          const ah = await h.createSyncAccessHandle();
          this.mapAH2Name.set(ah,name);
          this.setAssociatedPath(ah, '', 0);
        }
      },
      removeCapacity: async function(n){
        let nRm = 0;
        for(const ah of Array.from(this.availableAH)){
          if(nRm === n || this.getFileCount() === this.getCapacity()){
            break;
          }
          const name = this.mapAH2Name.get(ah);
          ah.close();
          await this.dirHandle.removeEntry(name);
          this.mapAH2Name.delete(ah);
          this.availableAH.delete(ah);
          ++nRm;
        }
        return nRm;
      },
      releaseAccessHandles: function(){
        for(const ah of this.mapAH2Name.keys()) ah.close();
        this.mapAH2Name.clear();
        this.mapPath2AH.clear();
        this.availableAH.clear();
      },
      acquireAccessHandles: async function(){
        const files = [];
        for await (const [name,h] of this.dirHandle){
          if('file'===h.kind){
            files.push([name,h]);
          }
        }
        await Promise.all(files.map(async ([name,h])=>{
          const ah = await h.createSyncAccessHandle()
            /*TODO: clean up and fail vfs init on error*/;
          this.mapAH2Name.set(ah, name);
          const path = this.getAssociatedPath(ah);
          if(path){
            this.mapPath2AH.set(path, ah);
          }else{
            this.availableAH.add(ah);
          }
        }));
      },
      gapBody: new Uint8Array(HEADER_CORPUS_SIZE),
      textDecoder: new TextDecoder(),
      getAssociatedPath: function(sah){
        const body = this.gapBody;
        sah.read(body, {at: 0});
        // Delete any unexpected files left over by previous
        // untimely errors...
        const dv = new DataView(body.buffer, body.byteOffset);
        const flags = dv.getUint32(HEADER_OFFSET_FLAGS);
        if(body[0] &&
           ((flags & capi.SQLITE_OPEN_DELETEONCLOSE) ||
            (flags & PERSISTENT_FILE_TYPES)===0)){
          warn(`Removing file with unexpected flags ${flags.toString(16)}`);
          this.setAssociatedPath(sah, '', 0);
          return '';
        }

        const fileDigest = new Uint32Array(HEADER_DIGEST_SIZE / 4);
        sah.read(fileDigest, {at: HEADER_OFFSET_DIGEST});
        const compDigest = this.computeDigest(body);
        if(fileDigest.every((v,i) => v===compDigest[i])){
          // Valid digest
          const pathBytes = body.findIndex((v)=>0===v);
          if(0===pathBytes){
            // This file is unassociated, so ensure that it's empty
            // to avoid leaving stale db data laying around.
            sah.truncate(HEADER_OFFSET_DATA);
          }
          return this.textDecoder.decode(body.subarray(0,pathBytes));
        }else{
          // Invalid digest
          warn('Disassociating file with bad digest.');
          this.setAssociatedPath(sah, '', 0);
          return '';
        }
      },
      textEncoder: new TextEncoder(),
      setAssociatedPath: function(sah, path, flags){
        const body = this.gapBody;
        const enc = this.textEncoder.encodeInto(path, body);
        if(HEADER_MAX_PATH_SIZE <= enc.written){
          toss("Path too long:",path);
        }

        const dv = new DataView(body.buffer, body.byteOffset);
        dv.setUint32(HEADER_OFFSET_FLAGS, flags);

        const digest = this.computeDigest(body);
        sah.write(body, {at: 0});
        sah.write(digest, {at: HEADER_OFFSET_DIGEST});
        sah.flush();

        if(path){
          this.mapPath2AH.set(path, sah);
          this.availableAH.delete(sah);
        }else{
          // This is not a persistent file, so eliminate the contents.
          sah.truncate(HEADER_OFFSET_DATA);
          this.mapPath2AH.delete(path);
          this.availableAH.add(sah);
        }
      },
      computeDigest: function(byteArray){
        if(!byteArray[0]){
          // Deleted file
          return new Uint32Array([0xfecc5f80, 0xaccec037]);
        }
        let h1 = 0xdeadbeef;
        let h2 = 0x41c6ce57;
        for(const v of byteArray){
          h1 = 31 * h1 + (v * 307);
          h2 = 31 * h2 + (v * 307);
        }
        return new Uint32Array([h1>>>0, h2>>>0]);
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
      },
      getPath: function(arg) {
        if(wasm.isPtr(arg)) arg = wasm.cstrToJs(arg);
        return ((arg instanceof URL)
                ? arg
                : new URL(arg, 'file://localhost/')).pathname;
      },
      deletePath: function(path) {
        const sah = this.mapPath2AH.get(path);
        if(sah) {
          // Un-associate the SQLite path from the OPFS file.
          this.setAssociatedPath(sah, '', 0);
        }
      }
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
        const file = SAHPool.mapId2File.get(pFile);
        if(file) {
          try{
            log(`xClose ${file.path}`);
            file.sah.flush();
            SAHPool.mapId2File.delete(pFIle);
            if(file.flags & capi.SQLITE_OPEN_DELETEONCLOSE){
              SAHPool.deletePath(file.path);
            }
          }catch(e){
            error("xClose() failed:",e.message);
            return capi.SQLITE_IOERR;
          }
        }
        return 0;
      },
      xDeviceCharacteristics: function(pFile){
        return capi.SQLITE_IOCAP_UNDELETABLE_WHEN_OPEN;
      },
      xFileControl: function(pFile, opId, pArg){
        return capi.SQLITE_NOTFOUND;
      },
      xFileSize: function(pFile,pSz64){
        const file = SAHPool.mapId2File(pFile);
        const size = file.sah.getSize() - HEADER_OFFSET_DATA;
        //log(`xFileSize ${file.path} ${size}`);
        wasm.poke64(pSz64, BigInt(size));
        return 0;
      },
      xLock: function(pFile,lockType){
        let rc = capi.SQLITE_IOERR;
        return rc;
      },
      xRead: function(pFile,pDest,n,offset64){
        const file = SAHPool.mapId2File.get(pFile);
        log(`xRead ${file.path} ${n} ${offset64}`);
        try {
          const nRead = file.sah.read(
            pDest, {at: HEADER_OFFSET_DATA + offset64}
          );
          if(nRead < n){
            wasm.heap8u().fill(0, pDest + nRead, pDest + n);
            return capi.SQLITE_IOERR_SHORT_READ;
          }
          return 0;
        }catch(e){
          error("xRead() failed:",e.message);
          return capi.SQLITE_IOERR;
        }
      },
      xSectorSize: function(pFile){
        return SECTOR_SIZE;
      },
      xSync: function(pFile,flags){
        const file = SAHPool.mapId2File.get(pFile);
        //log(`xSync ${file.path} ${flags}`);
        try{
          file.sah.flush();
          return 0;
        }catch(e){
          error("xSync() failed:",e.message);
          return capi.SQLITE_IOERR;
        }
      },
      xTruncate: function(pFile,sz64){
        const file = SAHPool.mapId2File.get(pFile);
        //log(`xTruncate ${file.path} ${iSize}`);
        try{
          file.sah.truncate(HEADER_OFFSET_DATA + Number(sz64));
          return 0;
        }catch(e){
          error("xTruncate() failed:",e.message);
          return capi.SQLITE_IOERR;
        }
      },
      /**xUnlock: function(pFile,lockType){
        return capi.SQLITE_IOERR;
      },*/
      xWrite: function(pFile,pSrc,n,offset64){
        const file = SAHPool.mapId2File(pFile);
        //log(`xWrite ${file.path} ${n} ${offset64}`);
        try{
          const nBytes = file.sah.write(
            pSrc, { at: HEADER_OFFSET_DATA + Number(offset64) }
          );
          return nBytes === n ? 0 : capi.SQLITE_IOERR;
          return 0;
        }catch(e){
          error("xWrite() failed:",e.message);
          return capi.SQLITE_IOERR;
        }
      }
    }/*ioSyncWrappers*/;

    /**
       Impls for the sqlite3_vfs methods. Maintenance reminder: members
       are in alphabetical order to simplify finding them.
    */
    const vfsSyncWrappers = {
      xAccess: function(pVfs,zName,flags,pOut){
        const name = this.getPath(zName);
        wasm.poke32(pOut, SAHPool.mapPath2AH.has(name) ? 1 : 0);
        return 0;
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
        try{
          SAHPool.deletePath(SAHPool.getPath(zName));
          return 0;
        }catch(e){
          error("Error xDelete()ing file:",e.message);
          return capi.SQLITE_IOERR_DELETE;
        }
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

    /**
       Ensure that the client has a "fully-sync" SAH impl,
       else reject the promise. Returns true on success,
       else false.
    */
    const affirmHasSyncAPI = async function(){
      try {
        const dh = await navigator.storage.getDirectory();
        const fn = '.opfs-sahpool-sync-check-'+Math.random().toString(36).slice(2);
        const fh = await dh.getFileHandle(fn, { create: true });
        const ah = await fh.createSyncAccessHandle();
        const close = ah.close();
        await close;
        await dh.removeEntry(fn);
        if(close?.then){
          toss("The local OPFS API is too old for opfs-sahpool:",
               "it has an async FileSystemSyncAccessHandle.close() method.");
        }
        return true;
      }catch(e){
        promiseReject(e);
        return false;
      }
    };
    if(!(await affirmHasSyncAPI())) return;
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
  })/*return Promise*/;
}/*installOpfsVfs()*/;

globalThis.sqlite3ApiBootstrap.initializersAsync.push(async (sqlite3)=>{
  return installOpfsVfs(sqlite3).catch((e)=>{
    sqlite3.config.warn("Ignoring inability to install opfs-sahpool sqlite3_vfs:",
                        e.message);
  });
}/*sqlite3ApiBootstrap.initializersAsync*/);
}/*sqlite3ApiBootstrap.initializers*/);
