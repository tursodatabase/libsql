//#ifnot target=node
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
  (hereafter "SAH") APIs released with Chrome v108 (and all other
  major browsers released since March 2023). If that API is not
  detected, the VFS is not registered.
*/
globalThis.sqlite3ApiBootstrap.initializers.push(function(sqlite3){
  'use strict';
  const toss = sqlite3.util.toss;
  const toss3 = sqlite3.util.toss3;
  const initPromises = Object.create(null);
  const capi = sqlite3.capi;
  const util = sqlite3.util;
  const wasm = sqlite3.wasm;
  // Config opts for the VFS...
  const SECTOR_SIZE = 4096;
  const HEADER_MAX_PATH_SIZE = 512;
  const HEADER_FLAGS_SIZE = 4;
  const HEADER_DIGEST_SIZE = 8;
  const HEADER_CORPUS_SIZE = HEADER_MAX_PATH_SIZE + HEADER_FLAGS_SIZE;
  const HEADER_OFFSET_FLAGS = HEADER_MAX_PATH_SIZE;
  const HEADER_OFFSET_DIGEST = HEADER_CORPUS_SIZE;
  const HEADER_OFFSET_DATA = SECTOR_SIZE;
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

  /** Subdirectory of the VFS's space where "opaque" (randomly-named)
      files are stored. Changing this effectively invalidates the data
      stored under older names (orphaning it), so don't do that. */
  const OPAQUE_DIR_NAME = ".opaque";

  /**
     Returns short a string of random alphanumeric characters
     suitable for use as a random filename.
  */
  const getRandomName = ()=>Math.random().toString(36).slice(2);

  const textDecoder = new TextDecoder();
  const textEncoder = new TextEncoder();

  const optionDefaults = Object.assign(Object.create(null),{
    name: 'opfs-sahpool',
    directory: undefined /* derived from .name */,
    initialCapacity: 6,
    clearOnInit: false,
    /* Logging verbosity 3+ == everything, 2 == warnings+errors, 1 ==
       errors only. */
    verbosity: 2
  });

  /** Logging routines, from most to least serious. */
  const loggers = [
    sqlite3.config.error,
    sqlite3.config.warn,
    sqlite3.config.log
  ];
  const log = sqlite3.config.log;
  const warn = sqlite3.config.warn;
  const error = sqlite3.config.error;

  /* Maps (sqlite3_vfs*) to OpfsSAHPool instances */
  const __mapVfsToPool = new Map();
  const getPoolForVfs = (pVfs)=>__mapVfsToPool.get(pVfs);
  const setPoolForVfs = (pVfs,pool)=>{
    if(pool) __mapVfsToPool.set(pVfs, pool);
    else __mapVfsToPool.delete(pVfs);
  };
  /* Maps (sqlite3_file*) to OpfsSAHPool instances */
  const __mapSqlite3File = new Map();
  const getPoolForPFile = (pFile)=>__mapSqlite3File.get(pFile);
  const setPoolForPFile = (pFile,pool)=>{
    if(pool) __mapSqlite3File.set(pFile, pool);
    else __mapSqlite3File.delete(pFile);
  };

  /**
     Impls for the sqlite3_io_methods methods. Maintenance reminder:
     members are in alphabetical order to simplify finding them.
  */
  const ioMethods = {
    xCheckReservedLock: function(pFile,pOut){
      const pool = getPoolForPFile(pFile);
      pool.log('xCheckReservedLock');
      pool.storeErr();
      wasm.poke32(pOut, 1);
      return 0;
    },
    xClose: function(pFile){
      const pool = getPoolForPFile(pFile);
      pool.storeErr();
      const file = pool.getOFileForS3File(pFile);
      if(file) {
        try{
          pool.log(`xClose ${file.path}`);
          pool.mapS3FileToOFile(pFile, false);
          file.sah.flush();
          if(file.flags & capi.SQLITE_OPEN_DELETEONCLOSE){
            pool.deletePath(file.path);
          }
        }catch(e){
          return pool.storeErr(e, capi.SQLITE_IOERR);
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
      const pool = getPoolForPFile(pFile);
      pool.log(`xFileSize`);
      const file = pool.getOFileForS3File(pFile);
      const size = file.sah.getSize() - HEADER_OFFSET_DATA;
      //log(`xFileSize ${file.path} ${size}`);
      wasm.poke64(pSz64, BigInt(size));
      return 0;
    },
    xLock: function(pFile,lockType){
      const pool = getPoolForPFile(pFile);
      pool.log(`xLock ${lockType}`);
      pool.storeErr();
      const file = pool.getOFileForS3File(pFile);
      file.lockType = lockType;
      return 0;
    },
    xRead: function(pFile,pDest,n,offset64){
      const pool = getPoolForPFile(pFile);
      pool.storeErr();
      const file = pool.getOFileForS3File(pFile);
      pool.log(`xRead ${file.path} ${n} @ ${offset64}`);
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
        return pool.storeErr(e, capi.SQLITE_IOERR);
      }
    },
    xSectorSize: function(pFile){
      return SECTOR_SIZE;
    },
    xSync: function(pFile,flags){
      const pool = getPoolForPFile(pFile);
      pool.log(`xSync ${flags}`);
      pool.storeErr();
      const file = pool.getOFileForS3File(pFile);
      //log(`xSync ${file.path} ${flags}`);
      try{
        file.sah.flush();
        return 0;
      }catch(e){
        return pool.storeErr(e, capi.SQLITE_IOERR);
      }
    },
    xTruncate: function(pFile,sz64){
      const pool = getPoolForPFile(pFile);
      pool.log(`xTruncate ${sz64}`);
      pool.storeErr();
      const file = pool.getOFileForS3File(pFile);
      //log(`xTruncate ${file.path} ${iSize}`);
      try{
        file.sah.truncate(HEADER_OFFSET_DATA + Number(sz64));
        return 0;
      }catch(e){
        return pool.storeErr(e, capi.SQLITE_IOERR);
      }
    },
    xUnlock: function(pFile,lockType){
      const pool = getPoolForPFile(pFile);
      pool.log('xUnlock');
      const file = pool.getOFileForS3File(pFile);
      file.lockType = lockType;
      return 0;
    },
    xWrite: function(pFile,pSrc,n,offset64){
      const pool = getPoolForPFile(pFile);
      pool.storeErr();
      const file = pool.getOFileForS3File(pFile);
      pool.log(`xWrite ${file.path} ${n} ${offset64}`);
      try{
        const nBytes = file.sah.write(
          wasm.heap8u().subarray(pSrc, pSrc+n),
          { at: HEADER_OFFSET_DATA + Number(offset64) }
        );
        return n===nBytes ? 0 : toss("Unknown write() failure.");
      }catch(e){
        return pool.storeErr(e, capi.SQLITE_IOERR);
      }
    }
  }/*ioMethods*/;

  const opfsIoMethods = new capi.sqlite3_io_methods();
  opfsIoMethods.$iVersion = 1;
  sqlite3.vfs.installVfs({
    io: {struct: opfsIoMethods, methods: ioMethods}
  });

  /**
     Impls for the sqlite3_vfs methods. Maintenance reminder: members
     are in alphabetical order to simplify finding them.
  */
  const vfsMethods = {
    xAccess: function(pVfs,zName,flags,pOut){
      //log(`xAccess ${wasm.cstrToJs(zName)}`);
      const pool = getPoolForVfs(pVfs);
      pool.storeErr();
      try{
        const name = pool.getPath(zName);
        wasm.poke32(pOut, pool.hasFilename(name) ? 1 : 0);
      }catch(e){
        /*ignored*/
        wasm.poke32(pOut, 0);
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
      const pool = getPoolForVfs(pVfs);
      pool.log(`xDelete ${wasm.cstrToJs(zName)}`);
      pool.storeErr();
      try{
        pool.deletePath(pool.getPath(zName));
        return 0;
      }catch(e){
        pool.storeErr(e);
        return capi.SQLITE_IOERR_DELETE;
      }
    },
    xFullPathname: function(pVfs,zName,nOut,pOut){
      //const pool = getPoolForVfs(pVfs);
      //pool.log(`xFullPathname ${wasm.cstrToJs(zName)}`);
      const i = wasm.cstrncpy(pOut, zName, nOut);
      return i<nOut ? 0 : capi.SQLITE_CANTOPEN;
    },
    xGetLastError: function(pVfs,nOut,pOut){
      const pool = getPoolForVfs(pVfs);
      const e = pool.popErr();
      pool.log(`xGetLastError ${nOut} e =`,e);
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
      return e ? (e.sqlite3Rc || capi.SQLITE_IOERR) : 0;
    },
    //xSleep is optionally defined below
    xOpen: function f(pVfs, zName, pFile, flags, pOutFlags){
      const pool = getPoolForVfs(pVfs);
      try{
        pool.log(`xOpen ${wasm.cstrToJs(zName)} ${flags}`);
        // First try to open a path that already exists in the file system.
        const path = (zName && wasm.peek8(zName))
              ? pool.getPath(zName)
              : getRandomName();
        let sah = pool.getSAHForPath(path);
        if(!sah && (flags & capi.SQLITE_OPEN_CREATE)) {
          // File not found so try to create it.
          if(pool.getFileCount() < pool.getCapacity()) {
            // Choose an unassociated OPFS file from the pool.
            sah = pool.nextAvailableSAH();
            pool.setAssociatedPath(sah, path, flags);
          }else{
            // File pool is full.
            toss('SAH pool is full. Cannot create file',path);
          }
        }
        if(!sah){
          toss('file not found:',path);
        }
        // Subsequent I/O methods are only passed the sqlite3_file
        // pointer, so map the relevant info we need to that pointer.
        const file = {path, flags, sah};
        pool.mapS3FileToOFile(pFile, file);
        file.lockType = capi.SQLITE_LOCK_NONE;
        const sq3File = new capi.sqlite3_file(pFile);
        sq3File.$pMethods = opfsIoMethods.pointer;
        sq3File.dispose();
        wasm.poke32(pOutFlags, flags);
        return 0;
      }catch(e){
        pool.storeErr(e);
        return capi.SQLITE_CANTOPEN;
      }
    }/*xOpen()*/
  }/*vfsMethods*/;

  /**
     Creates and initializes an sqlite3_vfs instance for an
     OpfsSAHPool. The argument is the VFS's name (JS string).

     Throws if the VFS name is already registered or if something
     goes terribly wrong via sqlite3.vfs.installVfs().

     Maintenance reminder: the only detail about the returned object
     which is specific to any given OpfsSAHPool instance is the $zName
     member. All other state is identical.
  */
  const createOpfsVfs = function(vfsName){
    if( sqlite3.capi.sqlite3_vfs_find(vfsName)){
      toss3("VFS name is already registered:", vfsName);
    }
    const opfsVfs = new capi.sqlite3_vfs();
    /* We fetch the default VFS so that we can inherit some
       methods from it. */
    const pDVfs = capi.sqlite3_vfs_find(null);
    const dVfs = pDVfs
          ? new capi.sqlite3_vfs(pDVfs)
          : null /* dVfs will be null when sqlite3 is built with
                    SQLITE_OS_OTHER. */;
    opfsVfs.$iVersion = 2/*yes, two*/;
    opfsVfs.$szOsFile = capi.sqlite3_file.structInfo.sizeof;
    opfsVfs.$mxPathname = HEADER_MAX_PATH_SIZE;
    opfsVfs.addOnDispose(
      opfsVfs.$zName = wasm.allocCString(vfsName),
      ()=>setPoolForVfs(opfsVfs.pointer, 0)
    );

    if(dVfs){
      /* Inherit certain VFS members from the default VFS,
         if available. */
      opfsVfs.$xRandomness = dVfs.$xRandomness;
      opfsVfs.$xSleep = dVfs.$xSleep;
      dVfs.dispose();
    }
    if(!opfsVfs.$xRandomness && !vfsMethods.xRandomness){
      /* If the default VFS has no xRandomness(), add a basic JS impl... */
      vfsMethods.xRandomness = function(pVfs, nOut, pOut){
        const heap = wasm.heap8u();
        let i = 0;
        for(; i < nOut; ++i) heap[pOut + i] = (Math.random()*255000) & 0xFF;
        return i;
      };
    }
    if(!opfsVfs.$xSleep && !vfsMethods.xSleep){
      vfsMethods.xSleep = (pVfs,ms)=>0;
    }
    sqlite3.vfs.installVfs({
      vfs: {struct: opfsVfs, methods: vfsMethods}
    });
    return opfsVfs;
  };

  /**
     Class for managing OPFS-related state for the
     OPFS SharedAccessHandle Pool sqlite3_vfs.
  */
  class OpfsSAHPool {
    /* OPFS dir in which VFS metadata is stored. */
    vfsDir;
    /* Directory handle to this.vfsDir. */
    #dhVfsRoot;
    /* Directory handle to the subdir of this.#dhVfsRoot which holds
       the randomly-named "opaque" files. This subdir exists in the
       hope that we can eventually support client-created files in
       this.#dhVfsRoot. */
    #dhOpaque;
    /* Directory handle to this.dhVfsRoot's parent dir. Needed
       for a VFS-wipe op. */
    #dhVfsParent;
    /* Maps SAHs to their opaque file names. */
    #mapSAHToName = new Map();
    /* Maps client-side file names to SAHs. */
    #mapFilenameToSAH = new Map();
    /* Set of currently-unused SAHs. */
    #availableSAH = new Set();
    /* Maps (sqlite3_file*) to xOpen's file objects. */
    #mapS3FileToOFile_ = new Map();

    /* Maps SAH to an abstract File Object which contains
       various metadata about that handle. */
    //#mapSAHToMeta = new Map();

    /** Buffer used by [sg]etAssociatedPath(). */
    #apBody = new Uint8Array(HEADER_CORPUS_SIZE);
    // DataView for this.#apBody
    #dvBody;

    // associated sqlite3_vfs instance
    #cVfs;

    // Logging verbosity. See optionDefaults.verbosity.
    #verbosity;

    constructor(options = Object.create(null)){
      this.#verbosity = options.verbosity ?? optionDefaults.verbosity;
      this.vfsName = options.name || optionDefaults.name;
      this.#cVfs = createOpfsVfs(this.vfsName);
      setPoolForVfs(this.#cVfs.pointer, this);
      this.vfsDir = options.directory || ("."+this.vfsName);
      this.#dvBody =
        new DataView(this.#apBody.buffer, this.#apBody.byteOffset);
      this.isReady = this
        .reset(!!(options.clearOnInit ?? optionDefaults.clearOnInit))
        .then(()=>{
          if(this.$error) throw this.$error;
          return this.getCapacity()
            ? Promise.resolve(undefined)
            : this.addCapacity(options.initialCapacity
                               || optionDefaults.initialCapacity);
        });
    }

    #logImpl(level,...args){
      if(this.#verbosity>level) loggers[level](this.vfsName+":",...args);
    };
    log(...args){this.#logImpl(2, ...args)};
    warn(...args){this.#logImpl(1, ...args)};
    error(...args){this.#logImpl(0, ...args)};

    getVfs(){return this.#cVfs}

    /* Current pool capacity. */
    getCapacity(){return this.#mapSAHToName.size}

    /* Current number of in-use files from pool. */
    getFileCount(){return this.#mapFilenameToSAH.size}

    /* Returns an array of the names of all
       currently-opened client-specified filenames. */
    getFileNames(){
      const rc = [];
      const iter = this.#mapFilenameToSAH.keys();
      for(const n of iter) rc.push(n);
      return rc;
    }

//    #createFileObject(sah,clientName,opaqueName){
//      const f = Object.assign(Object.create(null),{
//        clientName, opaqueName
//      });
//      this.#mapSAHToMeta.set(sah, f);
//      return f;
//    }
//    #unmapFileObject(sah){
//      this.#mapSAHToMeta.delete(sah);
//    }

    /**
       Adds n files to the pool's capacity. This change is
       persistent across settings. Returns a Promise which resolves
       to the new capacity.
    */
    async addCapacity(n){
      for(let i = 0; i < n; ++i){
        const name = getRandomName();
        const h = await this.#dhOpaque.getFileHandle(name, {create:true});
        const ah = await h.createSyncAccessHandle();
        this.#mapSAHToName.set(ah,name);
        this.setAssociatedPath(ah, '', 0);
        //this.#createFileObject(ah,undefined,name);
      }
      return this.getCapacity();
    }

    /**
       Reduce capacity by n, but can only reduce up to the limit
       of currently-available SAHs. Returns a Promise which resolves
       to the number of slots really removed.
    */
    async reduceCapacity(n){
      let nRm = 0;
      for(const ah of Array.from(this.#availableSAH)){
        if(nRm === n || this.getFileCount() === this.getCapacity()){
          break;
        }
        const name = this.#mapSAHToName.get(ah);
        //this.#unmapFileObject(ah);
        ah.close();
        await this.#dhOpaque.removeEntry(name);
        this.#mapSAHToName.delete(ah);
        this.#availableSAH.delete(ah);
        ++nRm;
      }
      return nRm;
    }

    /**
       Releases all currently-opened SAHs. The only legal
       operation after this is acquireAccessHandles().
    */
    releaseAccessHandles(){
      for(const ah of this.#mapSAHToName.keys()) ah.close();
      this.#mapSAHToName.clear();
      this.#mapFilenameToSAH.clear();
      this.#availableSAH.clear();
    }

    /**
       Opens all files under this.vfsDir/this.#dhOpaque and acquires
       a SAH for each. returns a Promise which resolves to no value
       but completes once all SAHs are acquired. If acquiring an SAH
       throws, SAHPool.$error will contain the corresponding
       exception.

       If clearFiles is true, the client-stored state of each file is
       cleared when its handle is acquired, including its name, flags,
       and any data stored after the metadata block.
    */
    async acquireAccessHandles(clearFiles){
      const files = [];
      for await (const [name,h] of this.#dhOpaque){
        if('file'===h.kind){
          files.push([name,h]);
        }
      }
      return Promise.all(files.map(async([name,h])=>{
        try{
          const ah = await h.createSyncAccessHandle()
          this.#mapSAHToName.set(ah, name);
          if(clearFiles){
            ah.truncate(HEADER_OFFSET_DATA);
            this.setAssociatedPath(ah, '', 0);
          }else{
            const path = this.getAssociatedPath(ah);
            if(path){
              this.#mapFilenameToSAH.set(path, ah);
            }else{
              this.#availableSAH.add(ah);
            }
          }
        }catch(e){
          this.storeErr(e);
          this.releaseAccessHandles();
          throw e;
        }
      }));
    }

    /**
       Given an SAH, returns the client-specified name of
       that file by extracting it from the SAH's header.

       On error, it disassociates SAH from the pool and
       returns an empty string.
    */
    getAssociatedPath(sah){
      sah.read(this.#apBody, {at: 0});
      // Delete any unexpected files left over by previous
      // untimely errors...
      const flags = this.#dvBody.getUint32(HEADER_OFFSET_FLAGS);
      if(this.#apBody[0] &&
         ((flags & capi.SQLITE_OPEN_DELETEONCLOSE) ||
          (flags & PERSISTENT_FILE_TYPES)===0)){
        warn(`Removing file with unexpected flags ${flags.toString(16)}`,
             this.#apBody);
        this.setAssociatedPath(sah, '', 0);
        return '';
      }

      const fileDigest = new Uint32Array(HEADER_DIGEST_SIZE / 4);
      sah.read(fileDigest, {at: HEADER_OFFSET_DIGEST});
      const compDigest = this.computeDigest(this.#apBody);
      if(fileDigest.every((v,i) => v===compDigest[i])){
        // Valid digest
        const pathBytes = this.#apBody.findIndex((v)=>0===v);
        if(0===pathBytes){
          // This file is unassociated, so truncate it to avoid
          // leaving stale db data laying around.
          sah.truncate(HEADER_OFFSET_DATA);
        }
        return pathBytes
          ? textDecoder.decode(this.#apBody.subarray(0,pathBytes))
          : '';
      }else{
        // Invalid digest
        warn('Disassociating file with bad digest.');
        this.setAssociatedPath(sah, '', 0);
        return '';
      }
    }

    /**
       Stores the given client-defined path and SQLITE_OPEN_xyz flags
       into the given SAH. If path is an empty string then the file is
       disassociated from the pool but its previous name is preserved
       in the metadata.
    */
    setAssociatedPath(sah, path, flags){
      const enc = textEncoder.encodeInto(path, this.#apBody);
      if(HEADER_MAX_PATH_SIZE <= enc.written + 1/*NUL byte*/){
        toss("Path too long:",path);
      }
      this.#apBody.fill(0, enc.written, HEADER_MAX_PATH_SIZE);
      this.#dvBody.setUint32(HEADER_OFFSET_FLAGS, flags);

      const digest = this.computeDigest(this.#apBody);
      sah.write(this.#apBody, {at: 0});
      sah.write(digest, {at: HEADER_OFFSET_DIGEST});
      sah.flush();

      if(path){
        this.#mapFilenameToSAH.set(path, sah);
        this.#availableSAH.delete(sah);
      }else{
        // This is not a persistent file, so eliminate the contents.
        sah.truncate(HEADER_OFFSET_DATA);
        this.#availableSAH.add(sah);
      }
    }

    /**
       Computes a digest for the given byte array and returns it as a
       two-element Uint32Array. This digest gets stored in the
       metadata for each file as a validation check. Changing this
       algorithm invalidates all existing databases for this VFS, so
       don't do that.
    */
    computeDigest(byteArray){
      let h1 = 0xdeadbeef;
      let h2 = 0x41c6ce57;
      for(const v of byteArray){
        h1 = 31 * h1 + (v * 307);
        h2 = 31 * h2 + (v * 307);
      }
      return new Uint32Array([h1>>>0, h2>>>0]);
    }

    /**
       Re-initializes the state of the SAH pool, releasing and
       re-acquiring all handles.

       See acquireAccessHandles() for the specifics of the clearFiles
       argument.
    */
    async reset(clearFiles){
      await this.isReady;
      let h = await navigator.storage.getDirectory();
      let prev, prevName;
      for(const d of this.vfsDir.split('/')){
        if(d){
          prev = h;
          h = await h.getDirectoryHandle(d,{create:true});
        }
      }
      this.#dhVfsRoot = h;
      this.#dhVfsParent = prev;
      this.#dhOpaque = await this.#dhVfsRoot.getDirectoryHandle(
        OPAQUE_DIR_NAME,{create:true}
      );
      this.releaseAccessHandles();
      return this.acquireAccessHandles(clearFiles);
    }

    /**
       Returns the pathname part of the given argument,
       which may be any of:

       - a URL object
       - A JS string representing a file name
       - Wasm C-string representing a file name

       All "../" parts and duplicate slashes are resolve/removed from
       the returned result.
    */
    getPath(arg) {
      if(wasm.isPtr(arg)) arg = wasm.cstrToJs(arg);
      return ((arg instanceof URL)
              ? arg
              : new URL(arg, 'file://localhost/')).pathname;
    }

    /**
       Removes the association of the given client-specified file
       name (JS string) from the pool. Returns true if a mapping
       is found, else false.
    */
    deletePath(path) {
      const sah = this.#mapFilenameToSAH.get(path);
      if(sah) {
        // Un-associate the name from the SAH.
        this.#mapFilenameToSAH.delete(path);
        this.setAssociatedPath(sah, '', 0);
      }
      return !!sah;
    }

    /**
       Sets e (an Error object) as this object's current error. Pass a
       falsy (or no) value to clear it. If code is truthy it is
       assumed to be an SQLITE_xxx result code, defaulting to
       SQLITE_IOERR if code is falsy.

       Returns the 2nd argument.
    */
    storeErr(e,code){
      if(e){
        e.sqlite3Rc = code || capi.SQLITE_IOERR;
        this.error(e);
      }
      this.$error = e;
      return code;
    }
    /**
       Pops this object's Error object and returns
       it (a falsy value if no error is set).
    */
    popErr(){
      const rc = this.$error;
      this.$error = undefined;
      return rc;
    }

    /**
       Returns the next available SAH without removing
       it from the set.
    */
    nextAvailableSAH(){
      const [rc] = this.#availableSAH.keys();
      return rc;
    }

    /**
       Given an (sqlite3_file*), returns the mapped
       xOpen file object.
    */
    getOFileForS3File(pFile){
      return this.#mapS3FileToOFile_.get(pFile);
    }
    /**
       Maps or unmaps (if file is falsy) the given (sqlite3_file*)
       to an xOpen file object and to this pool object.
    */
    mapS3FileToOFile(pFile,file){
      if(file){
        this.#mapS3FileToOFile_.set(pFile, file);
        setPoolForPFile(pFile, this);
      }else{
        this.#mapS3FileToOFile_.delete(pFile);
        setPoolForPFile(pFile, false);
      }
    }

    /**
       Returns true if the given client-defined file name is in this
       object's name-to-SAH map.
    */
    hasFilename(name){
      return this.#mapFilenameToSAH.has(name)
    }

    /**
       Returns the SAH associated with the given
       client-defined file name.
    */
    getSAHForPath(path){
      return this.#mapFilenameToSAH.get(path);
    }

    /**
       Removes this object's sqlite3_vfs registration and shuts down
       this object, releasing all handles, mappings, and whatnot,
       including deleting its data directory. There is currently no
       way to "revive" the object and reaquire its resources.

       This function is intended primarily for testing.

       Resolves to true if it did its job, false if the
       VFS has already been shut down.
    */
    async removeVfs(){
      if(!this.#cVfs.pointer || !this.#dhOpaque) return false;
      capi.sqlite3_vfs_unregister(this.#cVfs.pointer);
      this.#cVfs.dispose();
      try{
        this.releaseAccessHandles();
        await this.#dhVfsRoot.removeEntry(OPAQUE_DIR_NAME, {recursive: true});
        this.#dhOpaque = undefined;
        await this.#dhVfsParent.removeEntry(
          this.#dhVfsRoot.name, {recursive: true}
        );
        this.#dhVfsRoot = this.#dhVfsParent = undefined;
      }catch(e){
        sqlite3.config.error(this.vfsName,"removeVfs() failed:",e);
        /*otherwise ignored - there is no recovery strategy*/
      }
      return true;
    }


    //! Documented elsewhere in this file.
    exportFile(name){
      const sah = this.#mapFilenameToSAH.get(name) || toss("File not found:",name);
      const n = sah.getSize() - HEADER_OFFSET_DATA;
      const b = new Uint8Array(n>0 ? n : 0);
      if(n>0){
        const nRead = sah.read(b, {at: HEADER_OFFSET_DATA});
        if(nRead != n){
          toss("Expected to read "+n+" bytes but read "+nRead+".");
        }
      }
      return b;
    }

    //! Impl for importDb() when its 2nd arg is a function.
    async importDbChunked(name, callback){
      const sah = this.#mapFilenameToSAH.get(name)
            || this.nextAvailableSAH()
            || toss("No available handles to import to.");
      sah.truncate(0);
      let nWrote = 0, chunk, checkedHeader = false, err = false;
      try{
        while( undefined !== (chunk = await callback()) ){
          if(chunk instanceof ArrayBuffer) chunk = new Uint8Array(chunk);
          if( 0===nWrote && chunk.byteLength>=15 ){
            util.affirmDbHeader(chunk);
            checkedHeader = true;
          }
          sah.write(chunk, {at:  HEADER_OFFSET_DATA + nWrote});
          nWrote += chunk.byteLength;
        }
        if( nWrote < 512 || 0!==nWrote % 512 ){
          toss("Input size",nWrote,"is not correct for an SQLite database.");
        }
        if( !checkedHeader ){
          const header = new Uint8Array(20);
          sah.read( header, {at: 0} );
          util.affirmDbHeader( header );
        }
        sah.write(new Uint8Array([1,1]), {
          at: HEADER_OFFSET_DATA + 18
        }/*force db out of WAL mode*/);
      }catch(e){
        this.setAssociatedPath(sah, '', 0);
        throw e;
      }
      this.setAssociatedPath(sah, name, capi.SQLITE_OPEN_MAIN_DB);
      return nWrote;
    }

    //! Documented elsewhere in this file.
    importDb(name, bytes){
      if( bytes instanceof ArrayBuffer ) bytes = new Uint8Array(bytes);
      else if( bytes instanceof Function ) return this.importDbChunked(name, bytes);
      const sah = this.#mapFilenameToSAH.get(name)
            || this.nextAvailableSAH()
            || toss("No available handles to import to.");
      const n = bytes.byteLength;
      if(n<512 || n%512!=0){
        toss("Byte array size is invalid for an SQLite db.");
      }
      const header = "SQLite format 3";
      for(let i = 0; i < header.length; ++i){
        if( header.charCodeAt(i) !== bytes[i] ){
          toss("Input does not contain an SQLite database header.");
        }
      }
      const nWrote = sah.write(bytes, {at: HEADER_OFFSET_DATA});
      if(nWrote != n){
        this.setAssociatedPath(sah, '', 0);
        toss("Expected to write "+n+" bytes but wrote "+nWrote+".");
      }else{
        sah.write(new Uint8Array([1,1]), {at: HEADER_OFFSET_DATA+18}
                   /* force db out of WAL mode */);
        this.setAssociatedPath(sah, name, capi.SQLITE_OPEN_MAIN_DB);
      }
      return nWrote;
    }

  }/*class OpfsSAHPool*/;


  /**
     A OpfsSAHPoolUtil instance is exposed to clients in order to
     manipulate an OpfsSAHPool object without directly exposing that
     object and allowing for some semantic changes compared to that
     class.

     Class docs are in the client-level docs for
     installOpfsSAHPoolVfs().
  */
  class OpfsSAHPoolUtil {
    /* This object's associated OpfsSAHPool. */
    #p;

    constructor(sahPool){
      this.#p = sahPool;
      this.vfsName = sahPool.vfsName;
    }

    async addCapacity(n){ return this.#p.addCapacity(n) }

    async reduceCapacity(n){ return this.#p.reduceCapacity(n) }

    getCapacity(){ return this.#p.getCapacity(this.#p) }

    getFileCount(){ return this.#p.getFileCount() }
    getFileNames(){ return this.#p.getFileNames() }

    async reserveMinimumCapacity(min){
      const c = this.#p.getCapacity();
      return (c < min) ? this.#p.addCapacity(min - c) : c;
    }

    exportFile(name){ return this.#p.exportFile(name) }

    importDb(name, bytes){ return this.#p.importDb(name,bytes) }

    async wipeFiles(){ return this.#p.reset(true) }

    unlink(filename){ return this.#p.deletePath(filename) }

    async removeVfs(){ return this.#p.removeVfs() }

  }/* class OpfsSAHPoolUtil */;

  /**
     Returns a resolved Promise if the current environment
     has a "fully-sync" SAH impl, else a rejected Promise.
  */
  const apiVersionCheck = async ()=>{
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
  };

  /** Only for testing a rejection case. */
  let instanceCounter = 0;

  /**
     installOpfsSAHPoolVfs() asynchronously initializes the OPFS
     SyncAccessHandle (a.k.a. SAH) Pool VFS. It returns a Promise which
     either resolves to a utility object described below or rejects with
     an Error value.

     Initialization of this VFS is not automatic because its
     registration requires that it lock all resources it
     will potentially use, even if client code does not want
     to use them. That, in turn, can lead to locking errors
     when, for example, one page in a given origin has loaded
     this VFS but does not use it, then another page in that
     origin tries to use the VFS. If the VFS were automatically
     registered, the second page would fail to load the VFS
     due to OPFS locking errors.

     If this function is called more than once with a given "name"
     option (see below), it will return the same Promise. Calls for
     different names will return different Promises which resolve to
     independent objects and refer to different VFS registrations.

     On success, the resulting Promise resolves to a utility object
     which can be used to query and manipulate the pool. Its API is
     described at the end of these docs.

     This function accepts an options object to configure certain
     parts but it is only acknowledged for the very first call and
     ignored for all subsequent calls.

     The options, in alphabetical order:

     - `clearOnInit`: (default=false) if truthy, contents and filename
     mapping are removed from each SAH it is acquired during
     initalization of the VFS, leaving the VFS's storage in a pristine
     state. Use this only for databases which need not survive a page
     reload.

     - `initialCapacity`: (default=6) Specifies the default capacity of
     the VFS. This should not be set unduly high because the VFS has
     to open (and keep open) a file for each entry in the pool. This
     setting only has an effect when the pool is initially empty. It
     does not have any effect if a pool already exists.

     - `directory`: (default="."+`name`) Specifies the OPFS directory
     name in which to store metadata for the `"opfs-sahpool"`
     sqlite3_vfs.  Only one instance of this VFS can be installed per
     JavaScript engine, and any two engines with the same storage
     directory name will collide with each other, leading to locking
     errors and the inability to register the VFS in the second and
     subsequent engine. Using a different directory name for each
     application enables different engines in the same HTTP origin to
     co-exist, but their data are invisible to each other. Changing
     this name will effectively orphan any databases stored under
     previous names. The default is unspecified but descriptive.  This
     option may contain multiple path elements, e.g. "foo/bar/baz",
     and they are created automatically.  In practice there should be
     no driving need to change this. ACHTUNG: all files in this
     directory are assumed to be managed by the VFS. Do not place
     other files in that directory, as they may be deleted or
     otherwise modified by the VFS.

     - `name`: (default="opfs-sahpool") sets the name to register this
     VFS under. Normally this should not be changed, but it is
     possible to register this VFS under multiple names so long as
     each has its own separate directory to work from. The storage for
     each is invisible to all others. The name must be a string
     compatible with `sqlite3_vfs_register()` and friends and suitable
     for use in URI-style database file names.

     Achtung: if a custom `name` is provided, a custom `directory`
     must also be provided if any other instance is registered with
     the default directory. If no directory is explicitly provided
     then a directory name is synthesized from the `name` option.

     Peculiarities of this VFS:

     - Paths given to it _must_ be absolute. Relative paths will not
     be properly recognized. This is arguably a bug but correcting it
     requires some hoop-jumping in routines which have no business
     doing tricks.

     - It is possible to install multiple instances under different
     names, each sandboxed from one another inside their own private
     directory.  This feature exists primarily as a way for disparate
     applications within a given HTTP origin to use this VFS without
     introducing locking issues between them.


     The API for the utility object passed on by this function's
     Promise, in alphabetical order...

     - [async] number addCapacity(n)

     Adds `n` entries to the current pool. This change is persistent
     across sessions so should not be called automatically at each app
     startup (but see `reserveMinimumCapacity()`). Its returned Promise
     resolves to the new capacity.  Because this operation is necessarily
     asynchronous, the C-level VFS API cannot call this on its own as
     needed.

     - byteArray exportFile(name)

     Synchronously reads the contents of the given file into a Uint8Array
     and returns it. This will throw if the given name is not currently
     in active use or on I/O error. Note that the given name is _not_
     visible directly in OPFS (or, if it is, it's not from this VFS).

     - number getCapacity()

     Returns the number of files currently contained
     in the SAH pool. The default capacity is only large enough for one
     or two databases and their associated temp files.

     - number getFileCount()

     Returns the number of files from the pool currently allocated to
     slots. This is not the same as the files being "opened".

     - array getFileNames()

     Returns an array of the names of the files currently allocated to
     slots. This list is the same length as getFileCount().

     - void importDb(name, bytes)

     Imports the contents of an SQLite database, provided as a byte
     array or ArrayBuffer, under the given name, overwriting any
     existing content. Throws if the pool has no available file slots,
     on I/O error, or if the input does not appear to be a
     database. In the latter case, only a cursory examination is made.
     Note that this routine is _only_ for importing database files,
     not arbitrary files, the reason being that this VFS will
     automatically clean up any non-database files so importing them
     is pointless.

     If passed a function for its second argument, its behavior
     changes to asynchronous and it imports its data in chunks fed to
     it by the given callback function. It calls the callback (which
     may be async) repeatedly, expecting either a Uint8Array or
     ArrayBuffer (to denote new input) or undefined (to denote
     EOF). For so long as the callback continues to return
     non-undefined, it will append incoming data to the given
     VFS-hosted database file. The result of the resolved Promise when
     called this way is the size of the resulting database.

     On succes this routine rewrites the database header bytes in the
     output file (not the input array) to force disabling of WAL mode.

     On a write error, the handle is removed from the pool and made
     available for re-use.

     - [async] number reduceCapacity(n)

     Removes up to `n` entries from the pool, with the caveat that it can
     only remove currently-unused entries. It returns a Promise which
     resolves to the number of entries actually removed.

     - [async] boolean removeVfs()

     Unregisters the opfs-sahpool VFS and removes its directory from OPFS
     (which means that _all client content_ is removed). After calling
     this, the VFS may no longer be used and there is no way to re-add it
     aside from reloading the current JavaScript context.

     Results are undefined if a database is currently in use with this
     VFS.

     The returned Promise resolves to true if it performed the removal
     and false if the VFS was not installed.

     If the VFS has a multi-level directory, e.g. "/foo/bar/baz", _only_
     the bottom-most directory is removed because this VFS cannot know for
     certain whether the higher-level directories contain data which
     should be removed.

     - [async] number reserveMinimumCapacity(min)

     If the current capacity is less than `min`, the capacity is
     increased to `min`, else this returns with no side effects. The
     resulting Promise resolves to the new capacity.

     - boolean unlink(filename)

     If a virtual file exists with the given name, disassociates it from
     the pool and returns true, else returns false without side
     effects. Results are undefined if the file is currently in active
     use.

     - string vfsName

     The SQLite VFS name under which this pool's VFS is registered.

     - [async] void wipeFiles()

     Clears all client-defined state of all SAHs and makes all of them
     available for re-use by the pool. Results are undefined if any such
     handles are currently in use, e.g. by an sqlite3 db.
  */
  sqlite3.installOpfsSAHPoolVfs = async function(options=Object.create(null)){
    const vfsName = options.name || optionDefaults.name;
    if(0 && 2===++instanceCounter){
      throw new Error("Just testing rejection.");
    }
    if(initPromises[vfsName]){
      //console.warn("Returning same OpfsSAHPool result",options,vfsName,initPromises[vfsName]);
      return initPromises[vfsName];
    }
    if(!globalThis.FileSystemHandle ||
       !globalThis.FileSystemDirectoryHandle ||
       !globalThis.FileSystemFileHandle ||
       !globalThis.FileSystemFileHandle.prototype.createSyncAccessHandle ||
       !navigator?.storage?.getDirectory){
      return (initPromises[vfsName] = Promise.reject(new Error("Missing required OPFS APIs.")));
    }

    /**
       Maintenance reminder: the order of ASYNC ops in this function
       is significant. We need to have them all chained at the very
       end in order to be able to catch a race condition where
       installOpfsSAHPoolVfs() is called twice in rapid succession,
       e.g.:

       installOpfsSAHPoolVfs().then(console.warn.bind(console));
       installOpfsSAHPoolVfs().then(console.warn.bind(console));

       If the timing of the async calls is not "just right" then that
       second call can end up triggering the init a second time and chaos
       ensues.
    */
    return initPromises[vfsName] = apiVersionCheck().then(async function(){
      if(options.$testThrowInInit){
        throw options.$testThrowInInit;
      }
      const thePool = new OpfsSAHPool(options);
      return thePool.isReady.then(async()=>{
        /** The poolUtil object will be the result of the
            resolved Promise. */
        const poolUtil = new OpfsSAHPoolUtil(thePool);
        if(sqlite3.oo1){
          const oo1 = sqlite3.oo1;
          const theVfs = thePool.getVfs();
          const OpfsSAHPoolDb = function(...args){
            const opt = oo1.DB.dbCtorHelper.normalizeArgs(...args);
            opt.vfs = theVfs.$zName;
            oo1.DB.dbCtorHelper.call(this, opt);
          };
          OpfsSAHPoolDb.prototype = Object.create(oo1.DB.prototype);
          // yes or no? OpfsSAHPoolDb.PoolUtil = poolUtil;
          poolUtil.OpfsSAHPoolDb = OpfsSAHPoolDb;
          oo1.DB.dbCtorHelper.setVfsPostOpenSql(
            theVfs.pointer,
            function(oo1Db, sqlite3){
              sqlite3.capi.sqlite3_exec(oo1Db, [
                /* See notes in sqlite3-vfs-opfs.js */
                "pragma journal_mode=DELETE;",
                "pragma cache_size=-16384;"
              ], 0, 0, 0);
            }
          );
        }/*extend sqlite3.oo1*/
        thePool.log("VFS initialized.");
        return poolUtil;
      }).catch(async (e)=>{
        await thePool.removeVfs().catch(()=>{});
        return e;
      });
    }).catch((err)=>{
      //error("rejecting promise:",err);
      return initPromises[vfsName] = Promise.reject(err);
    });
  }/*installOpfsSAHPoolVfs()*/;
}/*sqlite3ApiBootstrap.initializers*/);
//#else
/*
  The OPFS SAH Pool VFS parts are elided from builds targeting
  node.js.
*/
//#endif target=node
