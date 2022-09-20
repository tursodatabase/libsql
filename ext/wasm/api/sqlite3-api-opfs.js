/*
  2022-09-18

  The author disclaims copyright to this source code.  In place of a
  legal notice, here is a blessing:

  *   May you do good and not evil.
  *   May you find forgiveness for yourself and forgive others.
  *   May you share freely, never taking more than you give.

  ***********************************************************************

  This file holds the synchronous half of an sqlite3_vfs
  implementation which proxies, in a synchronous fashion, the
  asynchronous Origin-Private FileSystem (OPFS) APIs using a second
  Worker, implemented in sqlite3-opfs-async-proxy.js.  This file is
  intended to be appended to the main sqlite3 JS deliverable somewhere
  after sqlite3-api-glue.js and before sqlite3-api-cleanup.js.

*/

'use strict';
self.sqlite3ApiBootstrap.initializers.push(function(sqlite3){
/**
   sqlite3.installOpfsVfs() returns a Promise which, on success, installs
   an sqlite3_vfs named "opfs", suitable for use with all sqlite3 APIs
   which accept a VFS. It uses the Origin-Private FileSystem API for
   all file storage. On error it is rejected with an exception
   explaining the problem. Reasons for rejection include, but are
   not limited to:

   - The counterpart Worker (see below) could not be loaded.

   - The environment does not support OPFS. That includes when
     this function is called from the main window thread.


  Significant notes and limitations:

  - As of this writing, OPFS is still very much in flux and only
    available in bleeding-edge versions of Chrome (v102+, noting that
    that number will increase as the OPFS API matures).

  - The OPFS features used here are only available in dedicated Worker
    threads. This file tries to detect that case, resulting in a
    rejected Promise if those features do not seem to be available.

  - It requires the SharedArrayBuffer and Atomics classes, and the
    former is only available if the HTTP server emits the so-called
    COOP and COEP response headers. These features are required for
    proxying OPFS's synchronous API via the synchronous interface
    required by the sqlite3_vfs API.

  - This function may only be called a single time and it must be
    called from the client, as opposed to the library initialization,
    in case the client requires a custom path for this API's
    "counterpart": this function's argument is the relative URI to
    this module's "asynchronous half". When called, this function removes
    itself from the sqlite3 object.

   The argument may optionally be a plain object with the following
   configuration options:

   - proxyUri: as described above

   - verbose (=2): an integer 0-3. 0 disables all logging, 1 enables
     logging of errors. 2 enables logging of warnings and errors. 3
     additionally enables debugging info.

   - sanityChecks (=false): if true, some basic sanity tests are
     run on the OPFS VFS API after it's initialized, before the
     returned Promise resolves.

   On success, the Promise resolves to the top-most sqlite3 namespace
   object and that object gets a new object installed in its
   `opfs` property, containing several OPFS-specific utilities.
*/
sqlite3.installOpfsVfs = function callee(asyncProxyUri = callee.defaultProxyUri){
  delete sqlite3.installOpfsVfs;
  if(self.window===self ||
     !self.SharedArrayBuffer ||
     !self.FileSystemHandle ||
     !self.FileSystemDirectoryHandle ||
     !self.FileSystemFileHandle ||
     !self.FileSystemFileHandle.prototype.createSyncAccessHandle ||
     !navigator.storage.getDirectory){
    return Promise.reject(
      new Error("This environment does not have OPFS support.")
    );
  }
  const options = (asyncProxyUri && 'object'===asyncProxyUri) ? asyncProxyUri : {
    proxyUri: asyncProxyUri
  };
  const urlParams = new URL(self.location.href).searchParams;
  if(undefined===options.verbose){
    options.verbose = urlParams.has('opfs-verbose') ? 3 : 2;
  }
  if(undefined===options.sanityChecks){
    options.sanityChecks = urlParams.has('opfs-sanity-check');
  }
  if(undefined===options.proxyUri){
    options.proxyUri = callee.defaultProxyUri;
  }

  const thePromise = new Promise(function(promiseResolve, promiseReject){
    const loggers = {
      0:console.error.bind(console),
      1:console.warn.bind(console),
      2:console.log.bind(console)
    };
    const logImpl = (level,...args)=>{
      if(options.verbose>level) loggers[level]("OPFS syncer:",...args);
    };
    const log =    (...args)=>logImpl(2, ...args);
    const warn =   (...args)=>logImpl(1, ...args);
    const error =  (...args)=>logImpl(0, ...args);
    warn("The OPFS VFS feature is very much experimental and under construction.");
    const toss = function(...args){throw new Error(args.join(' '))};
    const capi = sqlite3.capi;
    const wasm = capi.wasm;
    const sqlite3_vfs = capi.sqlite3_vfs;
    const sqlite3_file = capi.sqlite3_file;
    const sqlite3_io_methods = capi.sqlite3_io_methods;
    const W = new Worker(options.proxyUri);
    W._originalOnError = W.onerror /* will be restored later */;
    W.onerror = function(err){
      // The error object doesn't contain any useful info when the
      // failure is, e.g., that the remote script is 404.
      promiseReject(new Error("Loading OPFS async Worker failed for unknown reasons."));
    };
    /**
       Generic utilities for working with OPFS. This will get filled out
       by the Promise setup and, on success, installed as sqlite3.opfs.
    */
    const opfsUtil = Object.create(null);
    /**
       Not part of the public API. Solely for internal/development
       use.
    */
    opfsUtil.metrics = {
      dump: function(){
        let k, n = 0, t = 0, w = 0;
        for(k in state.opIds){
          const m = metrics[k];
          n += m.count;
          t += m.time;
          w += m.wait;
          m.avgTime = (m.count && m.time) ? (m.time / m.count) : 0;
          m.avgWait = (m.count && m.wait) ? (m.wait / m.count) : 0;
        }
        console.log(self.location.href,
                    "metrics for",self.location.href,":",metrics,
                    "\nTotal of",n,"op(s) for",t,
                    "ms (incl. "+w+" ms of waiting on the async side)");
      },
      reset: function(){
        let k;
        const r = (m)=>(m.count = m.time = m.wait = 0);
        for(k in state.opIds){
          r(metrics[k] = Object.create(null));
        }
        //[ // timed routines which are not in state.opIds
        //  'xFileControl'
        //].forEach((k)=>r(metrics[k] = Object.create(null)));
      }
    }/*metrics*/;      

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
       State which we send to the async-api Worker or share with it.
       This object must initially contain only cloneable or sharable
       objects. After the worker's "inited" message arrives, other types
       of data may be added to it.

       For purposes of Atomics.wait() and Atomics.notify(), we use a
       SharedArrayBuffer with one slot reserved for each of the API
       proxy's methods. The sync side of the API uses Atomics.wait()
       on the corresponding slot and the async side uses
       Atomics.notify() on that slot.

       The approach of using a single SAB to serialize comms for all
       instances might(?) lead to deadlock situations in multi-db
       cases. We should probably have one SAB here with a single slot
       for locking a per-file initialization step and then allocate a
       separate SAB like the above one for each file. That will
       require a bit of acrobatics but should be feasible.
    */
    const state = Object.create(null);
    state.littleEndian = true;
    state.verbose = options.verbose;
    /* Size of file I/O buffer block. 64k = max sqlite3 page size. */
    state.fileBufferSize =
      1024 * 64;
    state.sabS11nOffset = state.fileBufferSize;
    /**
       The size of the block in our SAB for serializing arguments and
       result values. Need to be large enough to hold serialized
       values of any of the proxied APIs. Filenames are the largest
       part but are limited to opfsVfs.$mxPathname bytes.
    */
    state.sabS11nSize = opfsVfs.$mxPathname * 2;
    /**
       The SAB used for all data I/O (files and arg/result s11n).
    */
    state.sabIO = new SharedArrayBuffer(
      state.fileBufferSize/* file i/o block */
      + state.sabS11nSize/* argument/result serialization block */
    );
    state.opIds = Object.create(null);
    const metrics = Object.create(null);
    {
      /* Indexes for use in our SharedArrayBuffer... */
      let i = 0;
      /* SAB slot used to communicate which operation is desired
         between both workers. This worker writes to it and the other
         listens for changes. */
      state.opIds.whichOp = i++;
      /* Slot for storing return values. This work listens to that
         slot and the other worker writes to it. */
      state.opIds.rc = i++;
      /* Each function gets an ID which this worker writes to
         the whichOp slot. The async-api worker uses Atomic.wait()
         on the whichOp slot to figure out which operation to run
         next. */
      state.opIds.xAccess = i++;
      state.opIds.xClose = i++;
      state.opIds.xDelete = i++;
      state.opIds.xDeleteNoWait = i++;
      state.opIds.xFileSize = i++;
      state.opIds.xOpen = i++;
      state.opIds.xRead = i++;
      state.opIds.xSleep = i++;
      state.opIds.xSync = i++;
      state.opIds.xTruncate = i++;
      state.opIds.xWrite = i++;
      state.opIds.mkdir = i++;
      state.opIds.xFileControl = i++;
      state.sabOP = new SharedArrayBuffer(i * 4/*sizeof int32*/);
      opfsUtil.metrics.reset();
    }

    /**
       SQLITE_xxx constants to export to the async worker
       counterpart...
    */
    state.sq3Codes = Object.create(null);
    state.sq3Codes._reverse = Object.create(null);
    [
      'SQLITE_ERROR', 'SQLITE_IOERR',
      'SQLITE_NOTFOUND', 'SQLITE_MISUSE',
      'SQLITE_IOERR_READ', 'SQLITE_IOERR_SHORT_READ',
      'SQLITE_IOERR_WRITE', 'SQLITE_IOERR_FSYNC',
      'SQLITE_IOERR_TRUNCATE', 'SQLITE_IOERR_DELETE',
      'SQLITE_IOERR_ACCESS', 'SQLITE_IOERR_CLOSE',
      'SQLITE_IOERR_DELETE',
      'SQLITE_OPEN_CREATE', 'SQLITE_OPEN_DELETEONCLOSE',
      'SQLITE_OPEN_READONLY'
    ].forEach(function(k){
      state.sq3Codes[k] = capi[k] || toss("Maintenance required: not found:",k);
      state.sq3Codes._reverse[capi[k]] = k;
    });

    const isWorkerErrCode = (n)=>!!state.sq3Codes._reverse[n];

    /**
       Runs the given operation (by name) in the async worker
       counterpart, waits for its response, and returns the result
       which the async worker writes to SAB[state.opIds.rc]. The
       2nd and subsequent arguments must be the aruguments for the
       async op.
    */
    const opRun = (op,...args)=>{
      const opNdx = state.opIds[op] || toss("Invalid op ID:",op);
      state.s11n.serialize(...args);
      Atomics.store(state.sabOPView, state.opIds.rc, -1);
      Atomics.store(state.sabOPView, state.opIds.whichOp, opNdx);
      Atomics.notify(state.sabOPView, state.opIds.whichOp) /* async thread will take over here */;
      const t = performance.now();
      Atomics.wait(state.sabOPView, state.opIds.rc, -1);
      const rc = Atomics.load(state.sabOPView, state.opIds.rc);
      metrics[op].wait += performance.now() - t;
      return rc;
    };

    const initS11n = ()=>{
      // Achtung: this code is 100% duplicated in the other half of this proxy!
      if(state.s11n) return state.s11n;
      const jsonDecoder = new TextDecoder(),
            jsonEncoder = new TextEncoder('utf-8'),
            viewSz = new DataView(state.sabIO, state.sabS11nOffset, 4),
            viewJson = new Uint8Array(state.sabIO, state.sabS11nOffset+4, state.sabS11nSize-4);
      state.s11n = Object.create(null);
      /**
         Returns an array of the state serialized by the most recent
         serialize() operation (here or in the counterpart thread), or
         null if the serialization buffer is empty.
      */
      state.s11n.deserialize = function(){
        const sz = viewSz.getInt32(0, state.littleEndian);
        const json = sz ? jsonDecoder.decode(
          viewJson.slice(0, sz)
          /* slice() (copy) needed, instead of subarray() (reference),
             because TextDecoder throws if asked to decode from an
             SAB. */
        ) : null;
        return JSON.parse(json);
      }
      /**
         Serializes all arguments to the shared buffer for consumption
         by the counterpart thread. This impl currently uses JSON for
         serialization for simplicy of implementation, but if that
         proves imperformant then a lower-level approach will be
         created.

         If passed "too much data" (more that the shared buffer size
         it will either throw or truncate the data (not certain
         which)). This routine is only intended for serializing OPFS
         VFS arguments and (in at least one special case) result
         values, and the buffer is sized to be able to comfortably
         handle those.

         If passed no arguments then it zeroes out the serialization
         state.
      */
      state.s11n.serialize = function(...args){
        if(args.length){
          const json = jsonEncoder.encode(JSON.stringify(args));
          viewSz.setInt32(0, json.byteLength, state.littleEndian);
          viewJson.set(json);
        }else{
          viewSz.setInt32(0, 0, state.littleEndian);
        }
      };
      return state.s11n;
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
      if(!(tgt instanceof sqlite3.StructBinder.StructType)){
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
      const fProxy = 0
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

    const opTimer = Object.create(null);
    opTimer.op = undefined;
    opTimer.start = undefined;
    const mTimeStart = (op)=>{
      opTimer.start = performance.now();
      opTimer.op = op;
      //metrics[op] || toss("Maintenance required: missing metrics for",op);
      ++metrics[op].count;
    };
    const mTimeEnd = ()=>(
      metrics[opTimer.op].time += performance.now() - opTimer.start
    );

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
        mTimeStart('xClose');
        let rc = 0;
        const f = __openFiles[pFile];
        if(f){
          delete __openFiles[pFile];
          rc = opRun('xClose', pFile);
          if(f.sq3File) f.sq3File.dispose();
        }
        mTimeEnd();
        return rc;
      },
      xDeviceCharacteristics: function(pFile){
        //debug("xDeviceCharacteristics(",pFile,")");
        return capi.SQLITE_IOCAP_UNDELETABLE_WHEN_OPEN;
      },
      xFileControl: function(pFile, opId, pArg){
        mTimeStart('xFileControl');
        const rc = (capi.SQLITE_FCNTL_SYNC===opId)
              ? opRun('xSync', pFile, 0)
              : capi.SQLITE_NOTFOUND;
        mTimeEnd();
        return rc;
      },
      xFileSize: function(pFile,pSz64){
        mTimeStart('xFileSize');
        const rc = opRun('xFileSize', pFile);
        if(!isWorkerErrCode(rc)){
          const sz = state.s11n.deserialize()[0];
          wasm.setMemValue(pSz64, BigInt(sz), 'i64');
        }
        mTimeEnd();
        return rc;
      },
      xLock: function(pFile,lockType){
        //2022-09: OPFS handles lock when opened
        //warn("xLock(",arguments,") is a no-op");
        return 0;
      },
      xRead: function(pFile,pDest,n,offset64){
        /* int (*xRead)(sqlite3_file*, void*, int iAmt, sqlite3_int64 iOfst) */
        mTimeStart('xRead');
        const f = __openFiles[pFile];
        let rc;
        try {
          rc = opRun('xRead',pFile, n, Number(offset64));
          if(0===rc || capi.SQLITE_IOERR_SHORT_READ===rc){
            // set() seems to be the fastest way to copy this...
            wasm.heap8u().set(f.sabView.subarray(0, n), pDest);
          }
        }catch(e){
          error("xRead(",arguments,") failed:",e,f);
          rc = capi.SQLITE_IOERR_READ;
        }
        mTimeEnd();
        return rc;
      },
      xSync: function(pFile,flags){
        ++metrics.xSync.count;
        return 0; // impl'd in xFileControl()
      },
      xTruncate: function(pFile,sz64){
        mTimeStart('xTruncate');
        const rc = opRun('xTruncate', pFile, Number(sz64));
        mTimeEnd();
        return rc;
      },
      xUnlock: function(pFile,lockType){
        //2022-09: OPFS handles lock when opened
        //warn("xUnlock(",arguments,") is a no-op");
        return 0;
      },
      xWrite: function(pFile,pSrc,n,offset64){
        /* int (*xWrite)(sqlite3_file*, const void*, int iAmt, sqlite3_int64 iOfst) */
        mTimeStart('xWrite');
        const f = __openFiles[pFile];
        let rc;
        try {
          f.sabView.set(wasm.heap8u().subarray(pSrc, pSrc+n));
          rc = opRun('xWrite', pFile, n, Number(offset64));
        }catch(e){
          error("xWrite(",arguments,") failed:",e,f);
          rc = capi.SQLITE_IOERR_WRITE;
        }
        mTimeEnd();
        return rc;
      }
    }/*ioSyncWrappers*/;
    
    /**
       Impls for the sqlite3_vfs methods. Maintenance reminder: members
       are in alphabetical order to simplify finding them.
    */
    const vfsSyncWrappers = {
      xAccess: function(pVfs,zName,flags,pOut){
        mTimeStart('xAccess');
        const rc = opRun('xAccess', wasm.cstringToJs(zName));
        wasm.setMemValue( pOut, (rc ? 0 : 1), 'i32' );
        mTimeEnd();
        return 0;
      },
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
        mTimeStart('xDelete');
        opRun('xDelete', wasm.cstringToJs(zName), doSyncDir, false);
        /* We're ignoring errors because we cannot yet differentiate
           between harmless and non-harmless failures. */
        mTimeEnd();
        return 0;
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
      //xSleep is optionally defined below
      xOpen: function f(pVfs, zName, pFile, flags, pOutFlags){
        mTimeStart('xOpen');
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
        const fh = Object.create(null);
        fh.fid = pFile;
        fh.filename = zName;
        fh.sab = new SharedArrayBuffer(state.fileBufferSize);
        fh.flags = flags;
        const rc = opRun('xOpen', pFile, zName, flags);
        if(!rc){
          /* Recall that sqlite3_vfs::xClose() will be called, even on
             error, unless pFile->pMethods is NULL. */
          if(fh.readOnly){
            wasm.setMemValue(pOutFlags, capi.SQLITE_OPEN_READONLY, 'i32');
          }
          __openFiles[pFile] = fh;
          fh.sabView = state.sabFileBufView;
          fh.sq3File = new sqlite3_file(pFile);
          fh.sq3File.$pMethods = opfsIoMethods.pointer;
        }
        mTimeEnd();
        return rc;
      }/*xOpen()*/
    }/*vfsSyncWrappers*/;

    if(dVfs){
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
      /* If we can inherit an xSleep() impl from the default VFS then
         assume it's sane and use it, otherwise install a JS-based
         one. */
      vfsSyncWrappers.xSleep = function(pVfs,ms){
        Atomics.wait(state.sabOPView, state.opIds.xSleep, 0, ms);
        return 0;
      };
    }

    /* Install the vfs/io_methods into their C-level shared instances... */
    let inst = installMethod(opfsIoMethods);
    for(let k of Object.keys(ioSyncWrappers)) inst(k, ioSyncWrappers[k]);
    inst = installMethod(opfsVfs);
    for(let k of Object.keys(vfsSyncWrappers)) inst(k, vfsSyncWrappers[k]);

    /**
       Syncronously deletes the given OPFS filesystem entry, ignoring
       any errors. As this environment has no notion of "current
       directory", the given name must be an absolute path. If the 2nd
       argument is truthy, deletion is recursive (use with caution!).

       Returns true if the deletion succeeded and fails if it fails,
       but cannot report the nature of the failure.
    */
    opfsUtil.deleteEntry = function(fsEntryName,recursive=false){
      return 0===opRun('xDelete', fsEntryName, 0, recursive);
    };
    /**
       Synchronously creates the given directory name, recursively, in
       the OPFS filesystem. Returns true if it succeeds or the
       directory already exists, else false.
    */
    opfsUtil.mkdir = function(absDirName){
      return 0===opRun('mkdir', absDirName);
    };
    /**
       Synchronously checks whether the given OPFS filesystem exists,
       returning true if it does, false if it doesn't.
    */
    opfsUtil.entryExists = function(fsEntryName){
      return 0===opRun('xAccess', fsEntryName);
    };

    /**
       Generates a random ASCII string, intended for use as a
       temporary file name. Its argument is the length of the string,
       defaulting to 16.
    */
    opfsUtil.randomFilename = randomFilename;
    
    if(sqlite3.oo1){
      opfsUtil.OpfsDb = function(...args){
        const opt = sqlite3.oo1.dbCtorHelper.normalizeArgs(...args);
        opt.vfs = opfsVfs.$zName;
        sqlite3.oo1.dbCtorHelper.call(this, opt);
      };
      opfsUtil.OpfsDb.prototype = Object.create(sqlite3.oo1.DB.prototype);
    }
    
    /**
       Potential TODOs:

       - Expose one or both of the Worker objects via opfsUtil and
         publish an interface for proxying the higher-level OPFS
         features like getting a directory listing.
    */
    
    const sanityCheck = function(){
      const scope = wasm.scopedAllocPush();
      const sq3File = new sqlite3_file();
      try{
        const fid = sq3File.pointer;
        const openFlags = capi.SQLITE_OPEN_CREATE
              | capi.SQLITE_OPEN_READWRITE
        //| capi.SQLITE_OPEN_DELETEONCLOSE
              | capi.SQLITE_OPEN_MAIN_DB;
        const pOut = wasm.scopedAlloc(8);
        const dbFile = "/sanity/check/file";
        const zDbFile = wasm.scopedAllocCString(dbFile);
        let rc;
        vfsSyncWrappers.xAccess(opfsVfs.pointer, zDbFile, 0, pOut);
        rc = wasm.getMemValue(pOut,'i32');
        log("xAccess(",dbFile,") exists ?=",rc);
        rc = vfsSyncWrappers.xOpen(opfsVfs.pointer, zDbFile,
                                   fid, openFlags, pOut);
        log("open rc =",rc,"state.sabOPView[xOpen] =",
            state.sabOPView[state.opIds.xOpen]);
        if(isWorkerErrCode(rc)){
          error("open failed with code",rc);
          return;
        }
        vfsSyncWrappers.xAccess(opfsVfs.pointer, zDbFile, 0, pOut);
        rc = wasm.getMemValue(pOut,'i32');
        if(!rc) toss("xAccess() failed to detect file.");
        rc = ioSyncWrappers.xSync(sq3File.pointer, 0);
        if(rc) toss('sync failed w/ rc',rc);
        rc = ioSyncWrappers.xTruncate(sq3File.pointer, 1024);
        if(rc) toss('truncate failed w/ rc',rc);
        wasm.setMemValue(pOut,0,'i64');
        rc = ioSyncWrappers.xFileSize(sq3File.pointer, pOut);
        if(rc) toss('xFileSize failed w/ rc',rc);
        log("xFileSize says:",wasm.getMemValue(pOut, 'i64'));
        rc = ioSyncWrappers.xWrite(sq3File.pointer, zDbFile, 10, 1);
        if(rc) toss("xWrite() failed!");
        const readBuf = wasm.scopedAlloc(16);
        rc = ioSyncWrappers.xRead(sq3File.pointer, readBuf, 6, 2);
        wasm.setMemValue(readBuf+6,0);
        let jRead = wasm.cstringToJs(readBuf);
        log("xRead() got:",jRead);
        if("sanity"!==jRead) toss("Unexpected xRead() value.");
        if(vfsSyncWrappers.xSleep){
          log("xSleep()ing before close()ing...");
          vfsSyncWrappers.xSleep(opfsVfs.pointer,2000);
          log("waking up from xSleep()");
        }
        rc = ioSyncWrappers.xClose(fid);
        log("xClose rc =",rc,"sabOPView =",state.sabOPView);
        log("Deleting file:",dbFile);
        vfsSyncWrappers.xDelete(opfsVfs.pointer, zDbFile, 0x1234);
        vfsSyncWrappers.xAccess(opfsVfs.pointer, zDbFile, 0, pOut);
        rc = wasm.getMemValue(pOut,'i32');
        if(rc) toss("Expecting 0 from xAccess(",dbFile,") after xDelete().");
        warn("End of OPFS sanity checks.");
      }finally{
        sq3File.dispose();
        wasm.scopedAllocPop(scope);
      }
    }/*sanityCheck()*/;

    
    W.onmessage = function({data}){
      //log("Worker.onmessage:",data);
      switch(data.type){
          case 'opfs-async-loaded':
            /*Pass our config and shared state on to the async worker.*/
            W.postMessage({type: 'opfs-async-init',args: state});
            break;
          case 'opfs-async-inited':{
            /*Indicates that the async partner has received the 'init',
              so we now know that the state object is no longer subject to
              being copied by a pending postMessage() call.*/
            try {
              const rc = capi.sqlite3_vfs_register(opfsVfs.pointer, 0);
              if(rc){
                opfsVfs.dispose();
                toss("sqlite3_vfs_register(OPFS) failed with rc",rc);
              }
              if(opfsVfs.pointer !== capi.sqlite3_vfs_find("opfs")){
                toss("BUG: sqlite3_vfs_find() failed for just-installed OPFS VFS");
              }
              capi.sqlite3_vfs_register.addReference(opfsVfs, opfsIoMethods);
              state.sabOPView = new Int32Array(state.sabOP);
              state.sabFileBufView = new Uint8Array(state.sabIO, 0, state.fileBufferSize);
              state.sabS11nView = new Uint8Array(state.sabIO, state.sabS11nOffset, state.sabS11nSize);
              initS11n();
              if(options.sanityChecks){
                warn("Running sanity checks because of opfs-sanity-check URL arg...");
                sanityCheck();
              }
              W.onerror = W._originalOnError;
              delete W._originalOnError;
              sqlite3.opfs = opfsUtil;
              log("End of OPFS sqlite3_vfs setup.", opfsVfs);
              promiseResolve(sqlite3);
            }catch(e){
              error(e);
              promiseReject(e);
            }
            break;
          }
          default:
            promiseReject(e);
            error("Unexpected message from the async worker:",data);
            break;
      }
    };
  })/*thePromise*/;
  return thePromise;
}/*installOpfsVfs()*/;
sqlite3.installOpfsVfs.defaultProxyUri = "sqlite3-opfs-async-proxy.js";
}/*sqlite3ApiBootstrap.initializers.push()*/);
