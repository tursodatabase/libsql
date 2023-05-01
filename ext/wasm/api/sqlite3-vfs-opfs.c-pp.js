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
  after sqlite3-api-oo1.js and before sqlite3-api-cleanup.js.
*/
'use strict';
globalThis.sqlite3ApiBootstrap.initializers.push(function(sqlite3){
/**
   installOpfsVfs() returns a Promise which, on success, installs an
   sqlite3_vfs named "opfs", suitable for use with all sqlite3 APIs
   which accept a VFS. It is intended to be called via
   sqlite3ApiBootstrap.initializersAsync or an equivalent mechanism.

   The installed VFS uses the Origin-Private FileSystem API for
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

  - This function may only be called a single time. When called, this
    function removes itself from the sqlite3 object.

  All arguments to this function are for internal/development purposes
  only. They do not constitute a public API and may change at any
  time.

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
const installOpfsVfs = function callee(options){
  if(!globalThis.SharedArrayBuffer
    || !globalThis.Atomics){
    return Promise.reject(
      new Error("Cannot install OPFS: Missing SharedArrayBuffer and/or Atomics. "+
                "The server must emit the COOP/COEP response headers to enable those. "+
                "See https://sqlite.org/wasm/doc/trunk/persistence.md#coop-coep")
    );
  }else if('undefined'===typeof WorkerGlobalScope){
    return Promise.reject(
      new Error("The OPFS sqlite3_vfs cannot run in the main thread "+
                "because it requires Atomics.wait().")
    );
  }else if(!globalThis.FileSystemHandle ||
           !globalThis.FileSystemDirectoryHandle ||
           !globalThis.FileSystemFileHandle ||
           !globalThis.FileSystemFileHandle.prototype.createSyncAccessHandle ||
           !navigator?.storage?.getDirectory){
    return Promise.reject(
      new Error("Missing required OPFS APIs.")
    );
  }
  if(!options || 'object'!==typeof options){
    options = Object.create(null);
  }
  const urlParams = new URL(globalThis.location.href).searchParams;
  if(undefined===options.verbose){
    options.verbose = urlParams.has('opfs-verbose')
      ? (+urlParams.get('opfs-verbose') || 2) : 1;
  }
  if(undefined===options.sanityChecks){
    options.sanityChecks = urlParams.has('opfs-sanity-check');
  }
  if(undefined===options.proxyUri){
    options.proxyUri = callee.defaultProxyUri;
  }

  //sqlite3.config.warn("OPFS options =",options,globalThis.location);

  if('function' === typeof options.proxyUri){
    options.proxyUri = options.proxyUri();
  }
  const thePromise = new Promise(function(promiseResolve_, promiseReject_){
    const loggers = {
      0:sqlite3.config.error,
      1:sqlite3.config.warn,
      2:sqlite3.config.log
    };
    const logImpl = (level,...args)=>{
      if(options.verbose>level) loggers[level]("OPFS syncer:",...args);
    };
    const log =    (...args)=>logImpl(2, ...args);
    const warn =   (...args)=>logImpl(1, ...args);
    const error =  (...args)=>logImpl(0, ...args);
    const toss = sqlite3.util.toss;
    const capi = sqlite3.capi;
    const wasm = sqlite3.wasm;
    const sqlite3_vfs = capi.sqlite3_vfs;
    const sqlite3_file = capi.sqlite3_file;
    const sqlite3_io_methods = capi.sqlite3_io_methods;
    /**
       Generic utilities for working with OPFS. This will get filled out
       by the Promise setup and, on success, installed as sqlite3.opfs.

       ACHTUNG: do not rely on these APIs in client code. They are
       experimental and subject to change or removal as the
       OPFS-specific sqlite3_vfs evolves.
    */
    const opfsUtil = Object.create(null);

    /**
       Returns true if _this_ thread has access to the OPFS APIs.
    */
    const thisThreadHasOPFS = ()=>{
      return globalThis.FileSystemHandle &&
        globalThis.FileSystemDirectoryHandle &&
        globalThis.FileSystemFileHandle &&
        globalThis.FileSystemFileHandle.prototype.createSyncAccessHandle &&
        navigator?.storage?.getDirectory;
    };

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
        sqlite3.config.log(globalThis.location.href,
                    "metrics for",globalThis.location.href,":",metrics,
                    "\nTotal of",n,"op(s) for",t,
                    "ms (incl. "+w+" ms of waiting on the async side)");
        sqlite3.config.log("Serialization metrics:",metrics.s11n);
        W.postMessage({type:'opfs-async-metrics'});
      },
      reset: function(){
        let k;
        const r = (m)=>(m.count = m.time = m.wait = 0);
        for(k in state.opIds){
          r(metrics[k] = Object.create(null));
        }
        let s = metrics.s11n = Object.create(null);
        s = s.serialize = Object.create(null);
        s.count = s.time = 0;
        s = metrics.s11n.deserialize = Object.create(null);
        s.count = s.time = 0;
      }
    }/*metrics*/;
    const opfsVfs = new sqlite3_vfs();
    const opfsIoMethods = new sqlite3_io_methods();
    let promiseWasRejected = undefined;
    const promiseReject = (err)=>{
      promiseWasRejected = true;
      opfsVfs.dispose();
      return promiseReject_(err);
    };
    const promiseResolve = (value)=>{
      promiseWasRejected = false;
      return promiseResolve_(value);
    };
    const W =
//#if target=es6-bundler-friendly
    new Worker(new URL("sqlite3-opfs-async-proxy.js", import.meta.url));
//#elif target=es6-module
    new Worker(new URL(options.proxyUri, import.meta.url));
//#else
    new Worker(options.proxyUri);
//#endif
    setTimeout(()=>{
      /* At attempt to work around a browser-specific quirk in which
         the Worker load is failing in such a way that we neither
         resolve nor reject it. This workaround gives that resolve/reject
         a time limit and rejects if that timer expires. Discussion:
         https://sqlite.org/forum/forumpost/a708c98dcb3ef */
      if(undefined===promiseWasRejected){
        promiseReject(
          new Error("Timeout while waiting for OPFS async proxy worker.")
        );
      }
    }, 4000);
    W._originalOnError = W.onerror /* will be restored later */;
    W.onerror = function(err){
      // The error object doesn't contain any useful info when the
      // failure is, e.g., that the remote script is 404.
      error("Error initializing OPFS asyncer:",err);
      promiseReject(new Error("Loading OPFS async Worker failed for unknown reasons."));
    };
    const pDVfs = capi.sqlite3_vfs_find(null)/*pointer to default VFS*/;
    const dVfs = pDVfs
          ? new sqlite3_vfs(pDVfs)
          : null /* dVfs will be null when sqlite3 is built with
                    SQLITE_OS_OTHER. */;
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
       require a bit of acrobatics but should be feasible. The most
       problematic part is that xOpen() would have to use
       postMessage() to communicate its SharedArrayBuffer, and mixing
       that approach with Atomics.wait/notify() gets a bit messy.
    */
    const state = Object.create(null);
    state.verbose = options.verbose;
    state.littleEndian = (()=>{
      const buffer = new ArrayBuffer(2);
      new DataView(buffer).setInt16(0, 256, true /* ==>littleEndian */);
      // Int16Array uses the platform's endianness.
      return new Int16Array(buffer)[0] === 256;
    })();
    /**
       asyncIdleWaitTime is how long (ms) to wait, in the async proxy,
       for each Atomics.wait() when waiting on inbound VFS API calls.
       We need to wake up periodically to give the thread a chance to
       do other things. If this is too high (e.g. 500ms) then even two
       workers/tabs can easily run into locking errors. Some multiple
       of this value is also used for determining how long to wait on
       lock contention to free up.
    */
    state.asyncIdleWaitTime = 150;
    /**
       Whether the async counterpart should log exceptions to
       the serialization channel. That produces a great deal of
       noise for seemingly innocuous things like xAccess() checks
       for missing files, so this option may have one of 3 values:

       0 = no exception logging.

       1 = only log exceptions for "significant" ops like xOpen(),
       xRead(), and xWrite().

       2 = log all exceptions.
    */
    state.asyncS11nExceptions = 1;
    /* Size of file I/O buffer block. 64k = max sqlite3 page size, and
       xRead/xWrite() will never deal in blocks larger than that. */
    state.fileBufferSize = 1024 * 64;
    state.sabS11nOffset = state.fileBufferSize;
    /**
       The size of the block in our SAB for serializing arguments and
       result values. Needs to be large enough to hold serialized
       values of any of the proxied APIs. Filenames are the largest
       part but are limited to opfsVfs.$mxPathname bytes. We also
       store exceptions there, so it needs to be long enough to hold
       a reasonably long exception string.
    */
    state.sabS11nSize = opfsVfs.$mxPathname * 2;
    /**
       The SAB used for all data I/O between the synchronous and
       async halves (file i/o and arg/result s11n).
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
      /* Slot for storing return values. This worker listens to that
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
      state.opIds.xLock = i++;
      state.opIds.xOpen = i++;
      state.opIds.xRead = i++;
      state.opIds.xSleep = i++;
      state.opIds.xSync = i++;
      state.opIds.xTruncate = i++;
      state.opIds.xUnlock = i++;
      state.opIds.xWrite = i++;
      state.opIds.mkdir = i++;
      state.opIds['opfs-async-metrics'] = i++;
      state.opIds['opfs-async-shutdown'] = i++;
      /* The retry slot is used by the async part for wait-and-retry
         semantics. Though we could hypothetically use the xSleep slot
         for that, doing so might lead to undesired side effects. */
      state.opIds.retry = i++;
      state.sabOP = new SharedArrayBuffer(
        i * 4/* ==sizeof int32, noting that Atomics.wait() and friends
                can only function on Int32Array views of an SAB. */);
      opfsUtil.metrics.reset();
    }
    /**
       SQLITE_xxx constants to export to the async worker
       counterpart...
    */
    state.sq3Codes = Object.create(null);
    [
      'SQLITE_ACCESS_EXISTS',
      'SQLITE_ACCESS_READWRITE',
      'SQLITE_BUSY',
      'SQLITE_ERROR',
      'SQLITE_IOERR',
      'SQLITE_IOERR_ACCESS',
      'SQLITE_IOERR_CLOSE',
      'SQLITE_IOERR_DELETE',
      'SQLITE_IOERR_FSYNC',
      'SQLITE_IOERR_LOCK',
      'SQLITE_IOERR_READ',
      'SQLITE_IOERR_SHORT_READ',
      'SQLITE_IOERR_TRUNCATE',
      'SQLITE_IOERR_UNLOCK',
      'SQLITE_IOERR_WRITE',
      'SQLITE_LOCK_EXCLUSIVE',
      'SQLITE_LOCK_NONE',
      'SQLITE_LOCK_PENDING',
      'SQLITE_LOCK_RESERVED',
      'SQLITE_LOCK_SHARED',
      'SQLITE_LOCKED',
      'SQLITE_MISUSE',
      'SQLITE_NOTFOUND',
      'SQLITE_OPEN_CREATE',
      'SQLITE_OPEN_DELETEONCLOSE',
      'SQLITE_OPEN_MAIN_DB',
      'SQLITE_OPEN_READONLY'
    ].forEach((k)=>{
      if(undefined === (state.sq3Codes[k] = capi[k])){
        toss("Maintenance required: not found:",k);
      }
    });
    state.opfsFlags = Object.assign(Object.create(null),{
      /**
         Flag for use with xOpen(). "opfs-unlock-asap=1" enables
         this. See defaultUnlockAsap, below.
       */
      OPFS_UNLOCK_ASAP: 0x01,
      /**
         If true, any async routine which implicitly acquires a sync
         access handle (i.e. an OPFS lock) will release that locks at
         the end of the call which acquires it. If false, such
         "autolocks" are not released until the VFS is idle for some
         brief amount of time.

         The benefit of enabling this is much higher concurrency. The
         down-side is much-reduced performance (as much as a 4x decrease
         in speedtest1).
      */
      defaultUnlockAsap: false
    });

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
      Atomics.notify(state.sabOPView, state.opIds.whichOp)
      /* async thread will take over here */;
      const t = performance.now();
      Atomics.wait(state.sabOPView, state.opIds.rc, -1)
      /* When this wait() call returns, the async half will have
         completed the operation and reported its results. */;
      const rc = Atomics.load(state.sabOPView, state.opIds.rc);
      metrics[op].wait += performance.now() - t;
      if(rc && state.asyncS11nExceptions){
        const err = state.s11n.deserialize();
        if(err) error(op+"() async error:",...err);
      }
      return rc;
    };

    /**
       Not part of the public API. Only for test/development use.
    */
    opfsUtil.debug = {
      asyncShutdown: ()=>{
        warn("Shutting down OPFS async listener. The OPFS VFS will no longer work.");
        opRun('opfs-async-shutdown');
      },
      asyncRestart: ()=>{
        warn("Attempting to restart OPFS VFS async listener. Might work, might not.");
        W.postMessage({type: 'opfs-async-restart'});
      }
    };

    const initS11n = ()=>{
      /**
         !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
         ACHTUNG: this code is 100% duplicated in the other half of
         this proxy! The documentation is maintained in the
         "synchronous half".
         !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

         This proxy de/serializes cross-thread function arguments and
         output-pointer values via the state.sabIO SharedArrayBuffer,
         using the region defined by (state.sabS11nOffset,
         state.sabS11nOffset]. Only one dataset is recorded at a time.

         This is not a general-purpose format. It only supports the
         range of operations, and data sizes, needed by the
         sqlite3_vfs and sqlite3_io_methods operations. Serialized
         data are transient and this serialization algorithm may
         change at any time.

         The data format can be succinctly summarized as:

         Nt...Td...D

         Where:

         - N = number of entries (1 byte)

         - t = type ID of first argument (1 byte)

         - ...T = type IDs of the 2nd and subsequent arguments (1 byte
         each).

         - d = raw bytes of first argument (per-type size).

         - ...D = raw bytes of the 2nd and subsequent arguments (per-type
         size).

         All types except strings have fixed sizes. Strings are stored
         using their TextEncoder/TextDecoder representations. It would
         arguably make more sense to store them as Int16Arrays of
         their JS character values, but how best/fastest to get that
         in and out of string form is an open point. Initial
         experimentation with that approach did not gain us any speed.

         Historical note: this impl was initially about 1% this size by
         using using JSON.stringify/parse(), but using fit-to-purpose
         serialization saves considerable runtime.
      */
      if(state.s11n) return state.s11n;
      const textDecoder = new TextDecoder(),
            textEncoder = new TextEncoder('utf-8'),
            viewU8 = new Uint8Array(state.sabIO, state.sabS11nOffset, state.sabS11nSize),
            viewDV = new DataView(state.sabIO, state.sabS11nOffset, state.sabS11nSize);
      state.s11n = Object.create(null);
      /* Only arguments and return values of these types may be
         serialized. This covers the whole range of types needed by the
         sqlite3_vfs API. */
      const TypeIds = Object.create(null);
      TypeIds.number  = { id: 1, size: 8, getter: 'getFloat64', setter: 'setFloat64' };
      TypeIds.bigint  = { id: 2, size: 8, getter: 'getBigInt64', setter: 'setBigInt64' };
      TypeIds.boolean = { id: 3, size: 4, getter: 'getInt32', setter: 'setInt32' };
      TypeIds.string =  { id: 4 };

      const getTypeId = (v)=>(
        TypeIds[typeof v]
          || toss("Maintenance required: this value type cannot be serialized.",v)
      );
      const getTypeIdById = (tid)=>{
        switch(tid){
            case TypeIds.number.id: return TypeIds.number;
            case TypeIds.bigint.id: return TypeIds.bigint;
            case TypeIds.boolean.id: return TypeIds.boolean;
            case TypeIds.string.id: return TypeIds.string;
            default: toss("Invalid type ID:",tid);
        }
      };

      /**
         Returns an array of the deserialized state stored by the most
         recent serialize() operation (from from this thread or the
         counterpart thread), or null if the serialization buffer is
         empty.  If passed a truthy argument, the serialization buffer
         is cleared after deserialization.
      */
      state.s11n.deserialize = function(clear=false){
        ++metrics.s11n.deserialize.count;
        const t = performance.now();
        const argc = viewU8[0];
        const rc = argc ? [] : null;
        if(argc){
          const typeIds = [];
          let offset = 1, i, n, v;
          for(i = 0; i < argc; ++i, ++offset){
            typeIds.push(getTypeIdById(viewU8[offset]));
          }
          for(i = 0; i < argc; ++i){
            const t = typeIds[i];
            if(t.getter){
              v = viewDV[t.getter](offset, state.littleEndian);
              offset += t.size;
            }else{/*String*/
              n = viewDV.getInt32(offset, state.littleEndian);
              offset += 4;
              v = textDecoder.decode(viewU8.slice(offset, offset+n));
              offset += n;
            }
            rc.push(v);
          }
        }
        if(clear) viewU8[0] = 0;
        //log("deserialize:",argc, rc);
        metrics.s11n.deserialize.time += performance.now() - t;
        return rc;
      };

      /**
         Serializes all arguments to the shared buffer for consumption
         by the counterpart thread.

         This routine is only intended for serializing OPFS VFS
         arguments and (in at least one special case) result values,
         and the buffer is sized to be able to comfortably handle
         those.

         If passed no arguments then it zeroes out the serialization
         state.
      */
      state.s11n.serialize = function(...args){
        const t = performance.now();
        ++metrics.s11n.serialize.count;
        if(args.length){
          //log("serialize():",args);
          const typeIds = [];
          let i = 0, offset = 1;
          viewU8[0] = args.length & 0xff /* header = # of args */;
          for(; i < args.length; ++i, ++offset){
            /* Write the TypeIds.id value into the next args.length
               bytes. */
            typeIds.push(getTypeId(args[i]));
            viewU8[offset] = typeIds[i].id;
          }
          for(i = 0; i < args.length; ++i) {
            /* Deserialize the following bytes based on their
               corresponding TypeIds.id from the header. */
            const t = typeIds[i];
            if(t.setter){
              viewDV[t.setter](offset, args[i], state.littleEndian);
              offset += t.size;
            }else{/*String*/
              const s = textEncoder.encode(args[i]);
              viewDV.setInt32(offset, s.byteLength, state.littleEndian);
              offset += 4;
              viewU8.set(s, offset);
              offset += s.byteLength;
            }
          }
          //log("serialize() result:",viewU8.slice(0,offset));
        }else{
          viewU8[0] = 0;
        }
        metrics.s11n.serialize.time += performance.now() - t;
      };
      return state.s11n;
    }/*initS11n()*/;

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
      return a.join("");
      /*
        An alternative impl. with an unpredictable length
        but much simpler:

        Math.floor(Math.random() * Number.MAX_SAFE_INTEGER).toString(36)
      */
    };

    /**
       Map of sqlite3_file pointers to objects constructed by xOpen().
    */
    const __openFiles = Object.create(null);

    const opTimer = Object.create(null);
    opTimer.op = undefined;
    opTimer.start = undefined;
    const mTimeStart = (op)=>{
      opTimer.start = performance.now();
      opTimer.op = op;
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
        /**
           As of late 2022, only a single lock can be held on an OPFS
           file. We have no way of checking whether any _other_ db
           connection has a lock except by trying to obtain and (on
           success) release a sync-handle for it, but doing so would
           involve an inherent race condition. For the time being,
           pending a better solution, we simply report whether the
           given pFile is open.
        */
        const f = __openFiles[pFile];
        wasm.poke(pOut, f.lockType ? 1 : 0, 'i32');
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
        /*mTimeStart('xFileControl');
          mTimeEnd();*/
        return capi.SQLITE_NOTFOUND;
      },
      xFileSize: function(pFile,pSz64){
        mTimeStart('xFileSize');
        let rc = opRun('xFileSize', pFile);
        if(0==rc){
          try {
            const sz = state.s11n.deserialize()[0];
            wasm.poke(pSz64, sz, 'i64');
          }catch(e){
            error("Unexpected error reading xFileSize() result:",e);
            rc = state.sq3Codes.SQLITE_IOERR;
          }
        }
        mTimeEnd();
        return rc;
      },
      xLock: function(pFile,lockType){
        mTimeStart('xLock');
        const f = __openFiles[pFile];
        let rc = 0;
        /* All OPFS locks are exclusive locks. If xLock() has
           previously succeeded, do nothing except record the lock
           type. If no lock is active, have the async counterpart
           lock the file. */
        if( !f.lockType ) {
          rc = opRun('xLock', pFile, lockType);
          if( 0===rc ) f.lockType = lockType;
        }else{
          f.lockType = lockType;
        }
        mTimeEnd();
        return rc;
      },
      xRead: function(pFile,pDest,n,offset64){
        mTimeStart('xRead');
        const f = __openFiles[pFile];
        let rc;
        try {
          rc = opRun('xRead',pFile, n, Number(offset64));
          if(0===rc || capi.SQLITE_IOERR_SHORT_READ===rc){
            /**
               Results get written to the SharedArrayBuffer f.sabView.
               Because the heap is _not_ a SharedArrayBuffer, we have
               to copy the results. TypedArray.set() seems to be the
               fastest way to copy this. */
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
        mTimeStart('xSync');
        ++metrics.xSync.count;
        const rc = opRun('xSync', pFile, flags);
        mTimeEnd();
        return rc;
      },
      xTruncate: function(pFile,sz64){
        mTimeStart('xTruncate');
        const rc = opRun('xTruncate', pFile, Number(sz64));
        mTimeEnd();
        return rc;
      },
      xUnlock: function(pFile,lockType){
        mTimeStart('xUnlock');
        const f = __openFiles[pFile];
        let rc = 0;
        if( capi.SQLITE_LOCK_NONE === lockType
          && f.lockType ){
          rc = opRun('xUnlock', pFile, lockType);
        }
        if( 0===rc ) f.lockType = lockType;
        mTimeEnd();
        return rc;
      },
      xWrite: function(pFile,pSrc,n,offset64){
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
        const rc = opRun('xAccess', wasm.cstrToJs(zName));
        wasm.poke( pOut, (rc ? 0 : 1), 'i32' );
        mTimeEnd();
        return 0;
      },
      xCurrentTime: function(pVfs,pOut){
        /* If it turns out that we need to adjust for timezone, see:
           https://stackoverflow.com/a/11760121/1458521 */
        wasm.poke(pOut, 2440587.5 + (new Date().getTime()/86400000),
                         'double');
        return 0;
      },
      xCurrentTimeInt64: function(pVfs,pOut){
        // TODO: confirm that this calculation is correct
        wasm.poke(pOut, (2440587.5 * 86400000) + new Date().getTime(),
                         'i64');
        return 0;
      },
      xDelete: function(pVfs, zName, doSyncDir){
        mTimeStart('xDelete');
        opRun('xDelete', wasm.cstrToJs(zName), doSyncDir, false);
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
        let opfsFlags = 0;
        if(0===zName){
          zName = randomFilename();
        }else if('number'===typeof zName){
          if(capi.sqlite3_uri_boolean(zName, "opfs-unlock-asap", 0)){
            /* -----------------------^^^^^ MUST pass the untranslated
               C-string here. */
            opfsFlags |= state.opfsFlags.OPFS_UNLOCK_ASAP;
          }
          zName = wasm.cstrToJs(zName);
        }
        const fh = Object.create(null);
        fh.fid = pFile;
        fh.filename = zName;
        fh.sab = new SharedArrayBuffer(state.fileBufferSize);
        fh.flags = flags;
        const rc = opRun('xOpen', pFile, zName, flags, opfsFlags);
        if(!rc){
          /* Recall that sqlite3_vfs::xClose() will be called, even on
             error, unless pFile->pMethods is NULL. */
          if(fh.readOnly){
            wasm.poke(pOutFlags, capi.SQLITE_OPEN_READONLY, 'i32');
          }
          __openFiles[pFile] = fh;
          fh.sabView = state.sabFileBufView;
          fh.sq3File = new sqlite3_file(pFile);
          fh.sq3File.$pMethods = opfsIoMethods.pointer;
          fh.lockType = capi.SQLITE_LOCK_NONE;
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

    /**
       Expects an OPFS file path. It gets resolved, such that ".."
       components are properly expanded, and returned. If the 2nd arg
       is true, the result is returned as an array of path elements,
       else an absolute path string is returned.
    */
    opfsUtil.getResolvedPath = function(filename,splitIt){
      const p = new URL(filename, "file://irrelevant").pathname;
      return splitIt ? p.split('/').filter((v)=>!!v) : p;
    };

    /**
       Takes the absolute path to a filesystem element. Returns an
       array of [handleOfContainingDir, filename]. If the 2nd argument
       is truthy then each directory element leading to the file is
       created along the way. Throws if any creation or resolution
       fails.
    */
    opfsUtil.getDirForFilename = async function f(absFilename, createDirs = false){
      const path = opfsUtil.getResolvedPath(absFilename, true);
      const filename = path.pop();
      let dh = opfsUtil.rootDirectory;
      for(const dirName of path){
        if(dirName){
          dh = await dh.getDirectoryHandle(dirName, {create: !!createDirs});
        }
      }
      return [dh, filename];
    };

    /**
       Creates the given directory name, recursively, in
       the OPFS filesystem. Returns true if it succeeds or the
       directory already exists, else false.
    */
    opfsUtil.mkdir = async function(absDirName){
      try {
        await opfsUtil.getDirForFilename(absDirName+"/filepart", true);
        return true;
      }catch(e){
        //sqlite3.config.warn("mkdir(",absDirName,") failed:",e);
        return false;
      }
    };
    /**
       Checks whether the given OPFS filesystem entry exists,
       returning true if it does, false if it doesn't.
    */
    opfsUtil.entryExists = async function(fsEntryName){
      try {
        const [dh, fn] = await opfsUtil.getDirForFilename(fsEntryName);
        await dh.getFileHandle(fn);
        return true;
      }catch(e){
        return false;
      }
    };

    /**
       Generates a random ASCII string, intended for use as a
       temporary file name. Its argument is the length of the string,
       defaulting to 16.
    */
    opfsUtil.randomFilename = randomFilename;

    /**
       Re-registers the OPFS VFS. This is intended only for odd use
       cases which have to call sqlite3_shutdown() as part of their
       initialization process, which will unregister the VFS
       registered by installOpfsVfs(). If passed a truthy value, the
       OPFS VFS is registered as the default VFS, else it is not made
       the default. Returns the result of the the
       sqlite3_vfs_register() call.

       Design note: the problem of having to re-register things after
       a shutdown/initialize pair is more general. How to best plug
       that in to the library is unclear. In particular, we cannot
       hook in to any C-side calls to sqlite3_initialize(), so we
       cannot add an after-initialize callback mechanism.
    */
    opfsUtil.registerVfs = (asDefault=false)=>{
      return wasm.exports.sqlite3_vfs_register(
        opfsVfs.pointer, asDefault ? 1 : 0
      );
    };

    /**
       Returns a promise which resolves to an object which represents
       all files and directories in the OPFS tree. The top-most object
       has two properties: `dirs` is an array of directory entries
       (described below) and `files` is a list of file names for all
       files in that directory.

       Traversal starts at sqlite3.opfs.rootDirectory.

       Each `dirs` entry is an object in this form:

       ```
       { name: directoryName,
         dirs: [...subdirs],
         files: [...file names]
       }
       ```

       The `files` and `subdirs` entries are always set but may be
       empty arrays.

       The returned object has the same structure but its `name` is
       an empty string. All returned objects are created with
       Object.create(null), so have no prototype.

       Design note: the entries do not contain more information,
       e.g. file sizes, because getting such info is not only
       expensive but is subject to locking-related errors.
    */
    opfsUtil.treeList = async function(){
      const doDir = async function callee(dirHandle,tgt){
        tgt.name = dirHandle.name;
        tgt.dirs = [];
        tgt.files = [];
        for await (const handle of dirHandle.values()){
          if('directory' === handle.kind){
            const subDir = Object.create(null);
            tgt.dirs.push(subDir);
            await callee(handle, subDir);
          }else{
            tgt.files.push(handle.name);
          }
        }
      };
      const root = Object.create(null);
      await doDir(opfsUtil.rootDirectory, root);
      return root;
    };

    /**
       Irrevocably deletes _all_ files in the current origin's OPFS.
       Obviously, this must be used with great caution. It may throw
       an exception if removal of anything fails (e.g. a file is
       locked), but the precise conditions under which the underlying
       APIs will throw are not documented (so we cannot tell you what
       they are).
    */
    opfsUtil.rmfr = async function(){
      const dir = opfsUtil.rootDirectory, opt = {recurse: true};
      for await (const handle of dir.values()){
        dir.removeEntry(handle.name, opt);
      }
    };

    /**
       Deletes the given OPFS filesystem entry.  As this environment
       has no notion of "current directory", the given name must be an
       absolute path. If the 2nd argument is truthy, deletion is
       recursive (use with caution!).

       The returned Promise resolves to true if the deletion was
       successful, else false (but...). The OPFS API reports the
       reason for the failure only in human-readable form, not
       exceptions which can be type-checked to determine the
       failure. Because of that...

       If the final argument is truthy then this function will
       propagate any exception on error, rather than returning false.
    */
    opfsUtil.unlink = async function(fsEntryName, recursive = false,
                                          throwOnError = false){
      try {
        const [hDir, filenamePart] =
              await opfsUtil.getDirForFilename(fsEntryName, false);
        await hDir.removeEntry(filenamePart, {recursive});
        return true;
      }catch(e){
        if(throwOnError){
          throw new Error("unlink(",arguments[0],") failed: "+e.message,{
            cause: e
          });
        }
        return false;
      }
    };

    /**
       Traverses the OPFS filesystem, calling a callback for each one.
       The argument may be either a callback function or an options object
       with any of the following properties:

       - `callback`: function which gets called for each filesystem
         entry.  It gets passed 3 arguments: 1) the
         FileSystemFileHandle or FileSystemDirectoryHandle of each
         entry (noting that both are instanceof FileSystemHandle). 2)
         the FileSystemDirectoryHandle of the parent directory. 3) the
         current depth level, with 0 being at the top of the tree
         relative to the starting directory. If the callback returns a
         literal false, as opposed to any other falsy value, traversal
         stops without an error. Any exceptions it throws are
         propagated. Results are undefined if the callback manipulate
         the filesystem (e.g. removing or adding entries) because the
         how OPFS iterators behave in the face of such changes is
         undocumented.

       - `recursive` [bool=true]: specifies whether to recurse into
         subdirectories or not. Whether recursion is depth-first or
         breadth-first is unspecified!

       - `directory` [FileSystemDirectoryEntry=sqlite3.opfs.rootDirectory]
         specifies the starting directory.

       If this function is passed a function, it is assumed to be the
       callback.

       Returns a promise because it has to (by virtue of being async)
       but that promise has no specific meaning: the traversal it
       performs is synchronous. The promise must be used to catch any
       exceptions propagated by the callback, however.

       TODO: add an option which specifies whether to traverse
       depth-first or breadth-first. We currently do depth-first but
       an incremental file browsing widget would benefit more from
       breadth-first.
    */
    opfsUtil.traverse = async function(opt){
      const defaultOpt = {
        recursive: true,
        directory: opfsUtil.rootDirectory
      };
      if('function'===typeof opt){
        opt = {callback:opt};
      }
      opt = Object.assign(defaultOpt, opt||{});
      const doDir = async function callee(dirHandle, depth){
        for await (const handle of dirHandle.values()){
          if(false === opt.callback(handle, dirHandle, depth)) return false;
          else if(opt.recursive && 'directory' === handle.kind){
            if(false === await callee(handle, depth + 1)) break;
          }
        }
      };
      doDir(opt.directory, 0);
    };

    //TODO to support fiddle and worker1 db upload:
    //opfsUtil.createFile = function(absName, content=undefined){...}
    //We have sqlite3.wasm.sqlite3_wasm_vfs_create_file() for this
    //purpose but its interface and name are still under
    //consideration.

    if(sqlite3.oo1){
      const OpfsDb = function(...args){
        const opt = sqlite3.oo1.DB.dbCtorHelper.normalizeArgs(...args);
        opt.vfs = opfsVfs.$zName;
        sqlite3.oo1.DB.dbCtorHelper.call(this, opt);
      };
      OpfsDb.prototype = Object.create(sqlite3.oo1.DB.prototype);
      sqlite3.oo1.OpfsDb = OpfsDb;
      sqlite3.oo1.DB.dbCtorHelper.setVfsPostOpenSql(
        opfsVfs.pointer,
        function(oo1Db, sqlite3){
          /* Set a relatively high default busy-timeout handler to
             help OPFS dbs deal with multi-tab/multi-worker
             contention. */
          sqlite3.capi.sqlite3_busy_timeout(oo1Db, 10000);
          sqlite3.capi.sqlite3_exec(oo1Db, [
            /* Truncate journal mode is faster than delete for
               this vfs, per speedtest1. That gap seems to have closed with
               Chrome version 108 or 109, but "persist" is very roughly 5-6%
               faster than truncate in initial tests.

               For later analysis: Roy Hashimoto notes that TRUNCATE
               and PERSIST modes may decrease OPFS concurrency because
               multiple connections can open the journal file in those
               modes:

               https://github.com/rhashimoto/wa-sqlite/issues/68
            */
            "pragma journal_mode=persist;",
            /*
              This vfs benefits hugely from cache on moderate/large
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
        const dbFile = "/sanity/check/file"+randomFilename(8);
        const zDbFile = wasm.scopedAllocCString(dbFile);
        let rc;
        state.s11n.serialize("This is ä string.");
        rc = state.s11n.deserialize();
        log("deserialize() says:",rc);
        if("This is ä string."!==rc[0]) toss("String d13n error.");
        vfsSyncWrappers.xAccess(opfsVfs.pointer, zDbFile, 0, pOut);
        rc = wasm.peek(pOut,'i32');
        log("xAccess(",dbFile,") exists ?=",rc);
        rc = vfsSyncWrappers.xOpen(opfsVfs.pointer, zDbFile,
                                   fid, openFlags, pOut);
        log("open rc =",rc,"state.sabOPView[xOpen] =",
            state.sabOPView[state.opIds.xOpen]);
        if(0!==rc){
          error("open failed with code",rc);
          return;
        }
        vfsSyncWrappers.xAccess(opfsVfs.pointer, zDbFile, 0, pOut);
        rc = wasm.peek(pOut,'i32');
        if(!rc) toss("xAccess() failed to detect file.");
        rc = ioSyncWrappers.xSync(sq3File.pointer, 0);
        if(rc) toss('sync failed w/ rc',rc);
        rc = ioSyncWrappers.xTruncate(sq3File.pointer, 1024);
        if(rc) toss('truncate failed w/ rc',rc);
        wasm.poke(pOut,0,'i64');
        rc = ioSyncWrappers.xFileSize(sq3File.pointer, pOut);
        if(rc) toss('xFileSize failed w/ rc',rc);
        log("xFileSize says:",wasm.peek(pOut, 'i64'));
        rc = ioSyncWrappers.xWrite(sq3File.pointer, zDbFile, 10, 1);
        if(rc) toss("xWrite() failed!");
        const readBuf = wasm.scopedAlloc(16);
        rc = ioSyncWrappers.xRead(sq3File.pointer, readBuf, 6, 2);
        wasm.poke(readBuf+6,0);
        let jRead = wasm.cstrToJs(readBuf);
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
        rc = wasm.peek(pOut,'i32');
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
          case 'opfs-unavailable':
            /* Async proxy has determined that OPFS is unavailable. There's
               nothing more for us to do here. */
            promiseReject(new Error(data.payload.join(' ')));
            break;
          case 'opfs-async-loaded':
            /* Arrives as soon as the asyc proxy finishes loading.
               Pass our config and shared state on to the async
               worker. */
            W.postMessage({type: 'opfs-async-init',args: state});
            break;
          case 'opfs-async-inited': {
            /* Indicates that the async partner has received the 'init'
               and has finished initializing, so the real work can
               begin... */
            if(true===promiseWasRejected){
              break /* promise was already rejected via timer */;
            }
            try {
              sqlite3.vfs.installVfs({
                io: {struct: opfsIoMethods, methods: ioSyncWrappers},
                vfs: {struct: opfsVfs, methods: vfsSyncWrappers}
              });
              state.sabOPView = new Int32Array(state.sabOP);
              state.sabFileBufView = new Uint8Array(state.sabIO, 0, state.fileBufferSize);
              state.sabS11nView = new Uint8Array(state.sabIO, state.sabS11nOffset, state.sabS11nSize);
              initS11n();
              if(options.sanityChecks){
                warn("Running sanity checks because of opfs-sanity-check URL arg...");
                sanityCheck();
              }
              if(thisThreadHasOPFS()){
                navigator.storage.getDirectory().then((d)=>{
                  W.onerror = W._originalOnError;
                  delete W._originalOnError;
                  sqlite3.opfs = opfsUtil;
                  opfsUtil.rootDirectory = d;
                  log("End of OPFS sqlite3_vfs setup.", opfsVfs);
                  promiseResolve(sqlite3);
                }).catch(promiseReject);
              }else{
                promiseResolve(sqlite3);
              }
            }catch(e){
              error(e);
              promiseReject(e);
            }
            break;
          }
          default: {
            const errMsg = (
              "Unexpected message from the OPFS async worker: " +
              JSON.stringify(data)
            );
            error(errMsg);
            promiseReject(new Error(errMsg));
            break;
          }
      }/*switch(data.type)*/
    }/*W.onmessage()*/;
  })/*thePromise*/;
  return thePromise;
}/*installOpfsVfs()*/;
installOpfsVfs.defaultProxyUri =
  "sqlite3-opfs-async-proxy.js";
globalThis.sqlite3ApiBootstrap.initializersAsync.push(async (sqlite3)=>{
  try{
    let proxyJs = installOpfsVfs.defaultProxyUri;
    if(sqlite3.scriptInfo.sqlite3Dir){
      installOpfsVfs.defaultProxyUri =
        sqlite3.scriptInfo.sqlite3Dir + proxyJs;
      //sqlite3.config.warn("installOpfsVfs.defaultProxyUri =",installOpfsVfs.defaultProxyUri);
    }
    return installOpfsVfs().catch((e)=>{
      sqlite3.config.warn("Ignoring inability to install OPFS sqlite3_vfs:",e.message);
    });
  }catch(e){
    sqlite3.config.error("installOpfsVfs() exception:",e);
    throw e;
  }
});
}/*sqlite3ApiBootstrap.initializers.push()*/);
