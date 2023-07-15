/*
  2023-07-14

  The author disclaims copyright to this source code.  In place of a
  legal notice, here is a blessing:

  *   May you do good and not evil.
  *   May you find forgiveness for yourself and forgive others.
  *   May you share freely, never taking more than you give.

  ***********************************************************************

  This file holds a sqlite3_vfs backed by OPFS storage which uses a
  different implementation strategy than the "opfs" VFS. This one is a
  port of Roy Hashimoto's OPFS SyncAccessHandle pool:

  https://github.com/rhashimoto/wa-sqlite/blob/master/src/examples/AccessHandlePoolVFS.js

  As described at:

  https://github.com/rhashimoto/wa-sqlite/discussions/67

  with Roy's explicit permission to permit us to port his to our
  infrastructure rather than having to clean-room reverse-engineer it:

  https://sqlite.org/forum/forumpost/e140d84e71

  Primary differences from the "opfs" VFS include:

  - This one avoids the need for a sub-worker to synchronize
  communication between the synchronous C API and the
  only-partly-synchronous OPFS API.

  - It does so by opening a fixed number of OPFS files at
  library-level initialization time, obtaining SyncAccessHandles to
  each, and manipulating those handles via the synchronous sqlite3_vfs
  interface. If it cannot open them (e.g. they are already opened by
  another tab) then the VFS will not be installed.

  - Because of that, this one lacks all library-level concurrency
  support.

  - Also because of that, it does not require the SharedArrayBuffer,
  so can function without the COOP/COEP HTTP response headers.

  - It can hypothetically support Safari 16.4+, whereas the "opfs" VFS
  requires v17 due to a subworker/storage bug in 16.x which makes it
  incompatible with that VFS.

  - This VFS requires the "semi-fully-sync" FileSystemSyncAccessHandle
  (hereafter "SAH") APIs released with Chrome v108. If that API
  is not detected, the VFS is not registered.
*/
'use strict';
globalThis.sqlite3ApiBootstrap.initializers.push(function(sqlite3){
const toss = sqlite3.util.toss;
let vfsRegisterResult = undefined;
/**
   installOpfsSAHPoolVfs() asynchronously initializes the
   OPFS SyncAccessHandle Pool VFS. It returns a Promise
   which either resolves to the sqlite3 object or rejects
   with an Error value.

   Initialization of this VFS is not automatic because its
   registration requires that it lock all resources it
   will potentially use, even if client code does not want
   to use them. That, in turn, can lead to locking errors
   when, for example, one page in a given origin has loaded
   this VFS but does not use it, then another page in that
   origin tries to use the VFS. If the VFS were automatically
   registered, the second page would fail to load the VFS
   due to OPFS locking errors.

   On calls after the first this function immediately returns a
   resolved or rejected Promise. If called while the first call is
   still pending resolution, a rejected promise with a descriptive
   error is returned.
*/
sqlite3.installOpfsSAHPoolVfs = async function(){
  if(sqlite3===vfsRegisterResult) return Promise.resolve(sqlite3);
  else if(undefined!==vfsRegisterResult){
    return Promise.reject(vfsRegisterResult);
  }
  if(!globalThis.FileSystemHandle ||
     !globalThis.FileSystemDirectoryHandle ||
     !globalThis.FileSystemFileHandle ||
     !globalThis.FileSystemFileHandle.prototype.createSyncAccessHandle ||
     !navigator?.storage?.getDirectory){
    return Promise.reject(vfsRegisterResult = new Error("Missing required OPFS APIs."));
  }
  vfsRegisterResult = new Error("VFS initialization still underway.");
  const verbosity = 2 /*3+ == everything*/;
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
  const capi = sqlite3.capi;
  const wasm = sqlite3.wasm;
  const opfsIoMethods = new capi.sqlite3_io_methods();
  const opfsVfs = new capi.sqlite3_vfs()
        .addOnDispose(()=>opfsIoMethods.dispose());
  const promiseReject = (err)=>{
    error("rejecting promise:",err);
    //opfsVfs.dispose();
    vfsRegisterResult = err;
    return Promise.reject(err);
  };
  const promiseResolve =
        ()=>Promise.resolve(vfsRegisterResult = sqlite3);
  // Config opts for the VFS...
  const SECTOR_SIZE = 4096;
  const HEADER_MAX_PATH_SIZE = 512;
  const HEADER_FLAGS_SIZE = 4;
  const HEADER_DIGEST_SIZE = 8;
  const HEADER_CORPUS_SIZE = HEADER_MAX_PATH_SIZE + HEADER_FLAGS_SIZE;
  const HEADER_OFFSET_FLAGS = HEADER_MAX_PATH_SIZE;
  const HEADER_OFFSET_DIGEST = HEADER_CORPUS_SIZE;
  const HEADER_OFFSET_DATA = SECTOR_SIZE;
  const DEFAULT_CAPACITY =
        sqlite3.config['opfs-sahpool.defaultCapacity'] || 6;
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
  /* We fetch the default VFS so that we can inherit some
     methods from it. */
  const pDVfs = capi.sqlite3_vfs_find(null);
  const dVfs = pDVfs
        ? new capi.sqlite3_vfs(pDVfs)
        : null /* dVfs will be null when sqlite3 is built with
                  SQLITE_OS_OTHER. */;
  opfsIoMethods.$iVersion = 1;
  opfsVfs.$iVersion = 2/*yes, two*/;
  opfsVfs.$szOsFile = capi.sqlite3_file.structInfo.sizeof;
  opfsVfs.$mxPathname = HEADER_MAX_PATH_SIZE;
  opfsVfs.addOnDispose(
    opfsVfs.$zName = wasm.allocCString("opfs-sahpool"),
    ()=>(dVfs ? dVfs.dispose() : null)
  );

  /**
     Returns short a string of random alphanumeric characters
     suitable for use as a random filename.
  */
  const getRandomName = ()=>Math.random().toString(36).slice(2);

  /**
     All state for the VFS.
  */
  const SAHPool = Object.assign(Object.create(null),{
    /* OPFS dir in which VFS metadata is stored. */
    vfsDir: sqlite3.config['opfs-sahpool.dir']
      || ".sqlite3-opfs-sahpool",
    /* Directory handle to this.vfsDir. */
    dirHandle: undefined,
    /* Maps SAHs to their opaque file names. */
    mapSAHToName: new Map(),
    /* Maps client-side file names to SAHs. */
    mapPathToSAH: new Map(),
    /* Set of currently-unused SAHs. */
    availableSAH: new Set(),
    /* Maps (sqlite3_file*) to xOpen's file objects. */
    mapIdToFile: new Map(),
    /* Current pool capacity. */
    getCapacity: function(){return this.mapSAHToName.size},
    /* Current number of in-use files from pool. */
    getFileCount: function(){return this.mapPathToSAH.size},
    /**
       Adds n files to the pool's capacity. This change is
       persistent across settings. Returns a Promise which resolves
       to the new capacity.
    */
    addCapacity: async function(n){
      const cap = this.getCapacity();
      for(let i = cap; i < cap+n; ++i){
        const name = getRandomName();
        const h = await this.dirHandle.getFileHandle(name, {create:true});
        const ah = await h.createSyncAccessHandle();
        this.mapSAHToName.set(ah,name);
        this.setAssociatedPath(ah, '', 0);
      }
      return i;
    },
    /**
       Removes n entries from the pool's current capacity
       if possible. It can only remove currently-unallocated
       files. Returns a Promise resolving to the number of
       removed files.
    */
    reduceCapacity: async function(n){
      let nRm = 0;
      for(const ah of Array.from(this.availableSAH)){
        if(nRm === n || this.getFileCount() === this.getCapacity()){
          break;
        }
        const name = this.mapSAHToName.get(ah);
        ah.close();
        await this.dirHandle.removeEntry(name);
        this.mapSAHToName.delete(ah);
        this.availableSAH.delete(ah);
        ++nRm;
      }
      return nRm;
    },
    /**
       Releases all currently-opened SAHs.
    */
    releaseAccessHandles: function(){
      for(const ah of this.mapSAHToName.keys()) ah.close();
      this.mapSAHToName.clear();
      this.mapPathToSAH.clear();
      this.availableSAH.clear();
    },
    /**
       Opens all files under this.vfsDir/this.dirHandle and acquires
       a SAH for each. returns a Promise which resolves to no value
       but completes once all SAHs are acquired. If acquiring an SAH
       throws, SAHPool.$error will contain the corresponding
       exception.
    */
    acquireAccessHandles: async function(){
      const files = [];
      for await (const [name,h] of this.dirHandle){
        if('file'===h.kind){
          files.push([name,h]);
        }
      }
      await Promise.all(files.map(async ([name,h])=>{
        try{
          const ah = await h.createSyncAccessHandle()
          this.mapSAHToName.set(ah, name);
          const path = this.getAssociatedPath(ah);
          if(path){
            this.mapPathToSAH.set(path, ah);
          }else{
            this.availableSAH.add(ah);
          }
        }catch(e){
          SAHPool.storeErr(e);
          this.releaseAccessHandles();
          throw e;
        }
      }));
    },
    /** Buffer used by [sg]etAssociatedPath(). */
    apBody: new Uint8Array(HEADER_CORPUS_SIZE),
    textDecoder: new TextDecoder(),
    textEncoder: new TextEncoder(),
    /**
       Given an SAH, returns the client-specified name of
       that file by extracting it from the SAH's header.

       On error, it disassociates SAH from the pool and
       returns an empty string.
    */
    getAssociatedPath: function(sah){
      const body = this.apBody;
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
          // This file is unassociated, so truncate it to avoid
          // leaving stale db data laying around.
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
    /**
       Stores the given client-defined path and SQLITE_OPEN_xyz
       flags into the given SAH.
    */
    setAssociatedPath: function(sah, path, flags){
      const body = this.apBody;
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
        this.mapPathToSAH.set(path, sah);
        this.availableSAH.delete(sah);
      }else{
        // This is not a persistent file, so eliminate the contents.
        sah.truncate(HEADER_OFFSET_DATA);
        this.mapPathToSAH.delete(path);
        this.availableSAH.add(sah);
      }
    },
    /**
       Computes a digest for the given byte array and
       returns it as a two-element Uint32Array.
    */
    computeDigest: function(byteArray){
      let h1 = 0xdeadbeef;
      let h2 = 0x41c6ce57;
      for(const v of byteArray){
        h1 = 31 * h1 + (v * 307);
        h2 = 31 * h2 + (v * 307);
      }
      return new Uint32Array([h1>>>0, h2>>>0]);
    },
    /**
       Re-initializes the state of the SAH pool,
       releasing and re-acquiring all handles.
    */
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
    /**
       Returns the pathname part of the given argument,
       which may be any of:

       - a URL object
       - A JS string representing a file name
       - Wasm C-string representing a file name
    */
    getPath: function(arg) {
      if(wasm.isPtr(arg)) arg = wasm.cstrToJs(arg);
      return ((arg instanceof URL)
              ? arg
              : new URL(arg, 'file://localhost/')).pathname;
    },
    /**
       Removes the association of the given client-specified file
       name (JS string) from the pool.
    */
    deletePath: function(path) {
      const sah = this.mapPathToSAH.get(path);
      if(sah) {
        // Un-associate the SQLite path from the OPFS file.
        this.setAssociatedPath(sah, '', 0);
      }
    },
    /**
       Sets e as this object's current error. Pass a falsy
       (or no) value to clear it.
    */
    storeErr: function(e){
      if(e) error(e);
      return this.$error = e;
    },
    /**
       Pops this object's Error object and returns
       it (a falsy value if no error is set).
    */
    popErr: function(){
      const rc = this.$error;
      this.$error = undefined;
      return rc;
    }
  })/*SAHPool*/;
  sqlite3.SAHPool = SAHPool/*only for testing*/;
  /**
     Impls for the sqlite3_io_methods methods. Maintenance reminder:
     members are in alphabetical order to simplify finding them.
  */
  const ioSyncWrappers = {
    xCheckReservedLock: function(pFile,pOut){
      log('xCheckReservedLock');
      SAHPool.storeErr();
      wasm.poke32(pOut, 1);
      return 0;
    },
    xClose: function(pFile){
      SAHPool.storeErr();
      const file = SAHPool.mapIdToFile.get(pFile);
      if(file) {
        try{
          log(`xClose ${file}`);
          if(file.sq3File) file.sq3File.dispose();
          file.sah.flush();
          SAHPool.mapIdToFile.delete(pFile);
          if(file.flags & capi.SQLITE_OPEN_DELETEONCLOSE){
            SAHPool.deletePath(file.path);
          }
        }catch(e){
          SAHPool.storeErr(e);
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
      log(`xFileSize`);
      const file = SAHPool.mapIdToFile.get(pFile);
      const size = file.sah.getSize() - HEADER_OFFSET_DATA;
      //log(`xFileSize ${file.path} ${size}`);
      wasm.poke64(pSz64, BigInt(size));
      return 0;
    },
    xLock: function(pFile,lockType){
      log(`xLock ${lockType}`);
      SAHPool.storeErr();
      const file = SAHPool.mapIdToFile.get(pFile);
      file.lockType = lockType;
      return 0;
    },
    xRead: function(pFile,pDest,n,offset64){
      log(`xRead ${n}@${offset64}`);
      SAHPool.storeErr();
      const file = SAHPool.mapIdToFile.get(pFile);
      log(`xRead ${file.path} ${n} ${offset64}`);
      try {
        const nRead = file.sah.read(
          wasm.heap8u().subarray(pDest, pDest+n),
          {at: HEADER_OFFSET_DATA + Number(offset64)}
        );
        if(nRead < n){
          wasm.heap8u().fill(0, pDest + nRead, pDest + n);
          return capi.SQLITE_IOERR_SHORT_READ;
        }
        return 0;
      }catch(e){
        SAHPool.storeErr(e);
        return capi.SQLITE_IOERR;
      }
    },
    xSectorSize: function(pFile){
      return SECTOR_SIZE;
    },
    xSync: function(pFile,flags){
      log(`xSync ${flags}`);
      SAHPool.storeErr();
      const file = SAHPool.mapIdToFile.get(pFile);
      //log(`xSync ${file.path} ${flags}`);
      try{
        file.sah.flush();
        return 0;
      }catch(e){
        SAHPool.storeErr(e);
        return capi.SQLITE_IOERR;
      }
    },
    xTruncate: function(pFile,sz64){
      log(`xTruncate ${sz64}`);
      SAHPool.storeErr();
      const file = SAHPool.mapIdToFile.get(pFile);
      //log(`xTruncate ${file.path} ${iSize}`);
      try{
        file.sah.truncate(HEADER_OFFSET_DATA + Number(sz64));
        return 0;
      }catch(e){
        SAHPool.storeErr(e);
        return capi.SQLITE_IOERR;
      }
    },
    xUnlock: function(pFile,lockType){
      log('xUnlock');
      const file = SAHPool.mapIdToFile.get(pFile);
      file.lockType = lockType;
      return 0;
    },
    xWrite: function(pFile,pSrc,n,offset64){
      SAHPool.storeErr();
      const file = SAHPool.mapIdToFile.get(pFile);
      log(`xWrite ${file.path} ${n} ${offset64}`);
      try{
        const nBytes = file.sah.write(
          wasm.heap8u().subarray(pSrc, pSrc+n),
          { at: HEADER_OFFSET_DATA + Number(offset64) }
        );
        return nBytes === n ? 0 : capi.SQLITE_IOERR;
      }catch(e){
        SAHPool.storeErr(e);
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
      log(`xAccess ${wasm.cstrToJs(zName)}`);
      SAHPool.storeErr();
      try{
        const name = this.getPath(zName);
        wasm.poke32(pOut, SAHPool.mapPathToSAH.has(name) ? 1 : 0);
      }catch(e){
        /*ignored*/;
      }
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
      log(`xDelete ${wasm.cstrToJs(zName)}`);
      SAHPool.storeErr();
      try{
        SAHPool.deletePath(SAHPool.getPath(zName));
        return 0;
      }catch(e){
        SAHPool.storeErr(e);
        return capi.SQLITE_IOERR_DELETE;
      }
    },
    xFullPathname: function(pVfs,zName,nOut,pOut){
      log(`xFullPathname ${wasm.cstrToJs(zName)}`);
      const i = wasm.cstrncpy(pOut, zName, nOut);
      return i<nOut ? 0 : capi.SQLITE_CANTOPEN;
    },
    xGetLastError: function(pVfs,nOut,pOut){
      log(`xGetLastError ${nOut}`);
      const e = SAHPool.popErr();
      if(e){
        const scope = wasm.scopedAllocPush();
        try{
          const [cMsg, n] = wasm.scopedAllocCString(e.message, true);
          wasm.cstrncpy(pOut, cMsg, nOut);
          if(n > nOut) wasm.poke8(pOut + nOut - 1, 0);
        }catch(e){
          return capi.SQLITE_NOMEM;
        }finally{
          wasm.scopedAllocPop(scope);
        }
      }
      return 0;
    },
    //xSleep is optionally defined below
    xOpen: function f(pVfs, zName, pFile, flags, pOutFlags){
      log(`xOpen ${wasm.cstrToJs(zName)} ${flags}`);
      try{
        // First try to open a path that already exists in the file system.
        const path = (zName && wasm.peek8(zName))
              ? SAHPool.getPath(zName)
              : getRandomName();
        let sah = SAHPool.mapPathToSAH.get(path);
        if(!sah && (flags & capi.SQLITE_OPEN_CREATE)) {
          // File not found so try to create it.
          if(SAHPool.getFileCount() < SAHPool.getCapacity()) {
            // Choose an unassociated OPFS file from the pool.
            [sah] = SAHPool.availableSAH.keys();
            SAHPool.setAssociatedPath(sah, path, flags);
          }else{
            // File pool is full.
            toss('SAH pool is full. Cannot create file',path);
          }
        }
        if(!sah){
          toss('file not found:',path);
        }
        // Subsequent methods are only passed the file pointer, so
        // map the relevant info we need to that pointer.
        const file = {path, flags, sah};
        SAHPool.mapIdToFile.set(pFile, file);
        wasm.poke32(pOutFlags, flags);
        file.sq3File = new capi.sqlite3_file(pFile);
        file.sq3File.$pMethods = opfsIoMethods.pointer;
        file.lockType = capi.SQLITE_LOCK_NONE;
        return 0;
      }catch(e){
        SAHPool.storeErr(e);
        return capi.SQLITE_CANTOPEN;
      }
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
    vfsSyncWrappers.xSleep = (pVfs,ms)=>0;
  }

  /**
     Ensure that the client has a "fully-sync" SAH impl,
     else reject the promise. Returns true on success,
     else a value intended to be returned via the containing
     function's Promise result.
  */
  const apiVersionCheck = await (async ()=>{
    try {
      const dh = await navigator.storage.getDirectory();
      const fn = '.opfs-sahpool-sync-check-'+getRandomName();
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
      return e;
    }
  })();
  if(true!==apiVersionCheck){
    return promiseReject(apiVersionCheck);
  }
  return SAHPool.isReady = SAHPool.reset().then(async ()=>{
    if(SAHPool.$error){
      throw SAHPool.$error;
    }
    if(0===SAHPool.getCapacity()){
      await SAHPool.addCapacity(DEFAULT_CAPACITY);
    }
    //log("vfs list:",capi.sqlite3_js_vfs_list());
    sqlite3.vfs.installVfs({
      io: {struct: opfsIoMethods, methods: ioSyncWrappers},
      vfs: {struct: opfsVfs, methods: vfsSyncWrappers},
      applyArgcCheck: true
    });
    log("opfsVfs",opfsVfs,"opfsIoMethods",opfsIoMethods);
    log("vfs list:",capi.sqlite3_js_vfs_list());
    if(sqlite3.oo1){
      const OpfsSAHPoolDb = function(...args){
        const opt = sqlite3.oo1.DB.dbCtorHelper.normalizeArgs(...args);
        opt.vfs = opfsVfs.$zName;
        sqlite3.oo1.DB.dbCtorHelper.call(this, opt);
      };
      OpfsSAHPoolDb.prototype = Object.create(sqlite3.oo1.DB.prototype);
      OpfsSAHPoolDb.addPoolCapacity = async (n)=>SAHPool.addCapacity(n);
      OpfsSAHPoolDb.reducePoolCapacity = async (n)=>SAHPool.reduceCapacity(n);
      OpfsSAHPoolDb.getPoolCapacity = ()=>SAHPool.getCapacity();
      OpfsSAHPoolDb.getPoolUsage = ()=>SAHPool.getFileCount();
      sqlite3.oo1.OpfsSAHPoolDb = OpfsSAHPoolDb;
      sqlite3.oo1.DB.dbCtorHelper.setVfsPostOpenSql(
        opfsVfs.pointer,
        function(oo1Db, sqlite3){
          sqlite3.capi.sqlite3_exec(oo1Db, [
            /* As of July 2023, the PERSIST journal mode on OPFS is
               somewhat slower than DELETE or TRUNCATE (it was faster
               before Chrome version 108 or 109). TRUNCATE and DELETE
               have very similar performance on OPFS.

               Roy Hashimoto notes that TRUNCATE and PERSIST modes may
               decrease OPFS concurrency because multiple connections
               can open the journal file in those modes:

               https://github.com/rhashimoto/wa-sqlite/issues/68

               Given that, and the fact that testing has not revealed
               any appreciable difference between performance of
               TRUNCATE and DELETE modes on OPFS, we currently (as of
               2023-07-13) default to DELETE mode.
            */
            "pragma journal_mode=DELETE;",
            /*
              OPFS benefits hugely from cache on moderate/large
              speedtest1 --size 50 and --size 100 workloads. We
              currently rely on setting a non-default cache size when
              building sqlite3.wasm. If that policy changes, the cache
              can be set here.
            */
            "pragma cache_size=-16384;"
          ], 0, 0, 0);
        }
      );
    }/*extend sqlite3.oo1*/
    log("VFS initialized.");
    return promiseResolve();
  }).catch(promiseReject);
}/*installOpfsSAHPoolVfs()*/;
}/*sqlite3ApiBootstrap.initializers*/);
