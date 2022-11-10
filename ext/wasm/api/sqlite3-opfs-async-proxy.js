/*
  2022-09-16

  The author disclaims copyright to this source code.  In place of a
  legal notice, here is a blessing:

  *   May you do good and not evil.
  *   May you find forgiveness for yourself and forgive others.
  *   May you share freely, never taking more than you give.

  ***********************************************************************

  A Worker which manages asynchronous OPFS handles on behalf of a
  synchronous API which controls it via a combination of Worker
  messages, SharedArrayBuffer, and Atomics. It is the asynchronous
  counterpart of the API defined in sqlite3-api-opfs.js.

  Highly indebted to:

  https://github.com/rhashimoto/wa-sqlite/blob/master/src/examples/OriginPrivateFileSystemVFS.js

  for demonstrating how to use the OPFS APIs.

  This file is to be loaded as a Worker. It does not have any direct
  access to the sqlite3 JS/WASM bits, so any bits which it needs (most
  notably SQLITE_xxx integer codes) have to be imported into it via an
  initialization process.

  This file represents an implementation detail of a larger piece of
  code, and not a public interface. Its details may change at any time
  and are not intended to be used by any client-level code.
*/
"use strict";
const toss = function(...args){throw new Error(args.join(' '))};
if(self.window === self){
  toss("This code cannot run from the main thread.",
       "Load it as a Worker from a separate Worker.");
}else if(!navigator.storage.getDirectory){
  toss("This API requires navigator.storage.getDirectory.");
}

/**
   Will hold state copied to this object from the syncronous side of
   this API.
*/
const state = Object.create(null);

/**
   verbose:

   0 = no logging output
   1 = only errors
   2 = warnings and errors
   3 = debug, warnings, and errors
*/
state.verbose = 2;

const loggers = {
  0:console.error.bind(console),
  1:console.warn.bind(console),
  2:console.log.bind(console)
};
const logImpl = (level,...args)=>{
  if(state.verbose>level) loggers[level]("OPFS asyncer:",...args);
};
const log =    (...args)=>logImpl(2, ...args);
const warn =   (...args)=>logImpl(1, ...args);
const error =  (...args)=>logImpl(0, ...args);
const metrics = Object.create(null);
metrics.reset = ()=>{
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
};
metrics.dump = ()=>{
  let k, n = 0, t = 0, w = 0;
  for(k in state.opIds){
    const m = metrics[k];
    n += m.count;
    t += m.time;
    w += m.wait;
    m.avgTime = (m.count && m.time) ? (m.time / m.count) : 0;
  }
  console.log(self.location.href,
              "metrics for",self.location.href,":\n",
              metrics,
              "\nTotal of",n,"op(s) for",t,"ms",
              "approx",w,"ms spent waiting on OPFS APIs.");
  console.log("Serialization metrics:",metrics.s11n);
};

/**
   __openFiles is a map of sqlite3_file pointers (integers) to
   metadata related to a given OPFS file handles. The pointers are, in
   this side of the interface, opaque file handle IDs provided by the
   synchronous part of this constellation. Each value is an object
   with a structure demonstrated in the xOpen() impl.
*/
const __openFiles = Object.create(null);
/**
   __autoLocks is a Set of sqlite3_file pointers (integers) which were
   "auto-locked".  i.e. those for which we obtained a sync access
   handle without an explicit xLock() call. Such locks will be
   released during db connection idle time, whereas a sync access
   handle obtained via xLock(), or subsequently xLock()'d after
   auto-acquisition, will not be released until xUnlock() is called.

   Maintenance reminder: if we relinquish auto-locks at the end of the
   operation which acquires them, we pay a massive performance
   penalty: speedtest1 benchmarks take up to 4x as long. By delaying
   the lock release until idle time, the hit is negligible.
*/
const __autoLocks = new Set();

/**
   Expects an OPFS file path. It gets resolved, such that ".."
   components are properly expanded, and returned. If the 2nd arg is
   true, the result is returned as an array of path elements, else an
   absolute path string is returned.
*/
const getResolvedPath = function(filename,splitIt){
  const p = new URL(
    filename, 'file://irrelevant'
  ).pathname;
  return splitIt ? p.split('/').filter((v)=>!!v) : p;
};

/**
   Takes the absolute path to a filesystem element. Returns an array
   of [handleOfContainingDir, filename]. If the 2nd argument is truthy
   then each directory element leading to the file is created along
   the way. Throws if any creation or resolution fails.
*/
const getDirForFilename = async function f(absFilename, createDirs = false){
  const path = getResolvedPath(absFilename, true);
  const filename = path.pop();
  let dh = state.rootDir;
  for(const dirName of path){
    if(dirName){
      dh = await dh.getDirectoryHandle(dirName, {create: !!createDirs});
    }
  }
  return [dh, filename];
};

/**
   An error class specifically for use with getSyncHandle(), the goal
   of which is to eventually be able to distinguish unambiguously
   between locking-related failures and other types, noting that we
   cannot currently do so because createSyncAccessHandle() does not
   define its exceptions in the required level of detail.
*/
class GetSyncHandleError extends Error {
  constructor(errorObject, ...msg){
    super();
    this.error = errorObject;
    this.message = [
      ...msg, ': Original exception ['+errorObject.name+']:',
      errorObject.message
    ].join(' ');
    this.name = 'GetSyncHandleError';
  }
};

/**
   Returns the sync access handle associated with the given file
   handle object (which must be a valid handle object, as created by
   xOpen()), lazily opening it if needed.

   In order to help alleviate cross-tab contention for a dabase,
   if an exception is thrown while acquiring the handle, this routine
   will wait briefly and try again, up to 3 times. If acquisition
   still fails at that point it will give up and propagate the
   exception.
*/
const getSyncHandle = async (fh)=>{
  if(!fh.syncHandle){
    const t = performance.now();
    log("Acquiring sync handle for",fh.filenameAbs);
    const maxTries = 4, msBase = 300;
    let i = 1, ms = msBase;
    for(; true; ms = msBase * ++i){
      try {
        //if(i<3) toss("Just testing getSyncHandle() wait-and-retry.");
        //TODO? A config option which tells it to throw here
        //randomly every now and then, for testing purposes.
        fh.syncHandle = await fh.fileHandle.createSyncAccessHandle();
        break;
      }catch(e){
        if(i === maxTries){
          throw new GetSyncHandleError(
            e, "Error getting sync handle.",maxTries,
            "attempts failed.",fh.filenameAbs
          );
        }
        warn("Error getting sync handle. Waiting",ms,
              "ms and trying again.",fh.filenameAbs,e);
        Atomics.wait(state.sabOPView, state.opIds.retry, 0, ms);
      }
    }
    log("Got sync handle for",fh.filenameAbs,'in',performance.now() - t,'ms');
    if(!fh.xLock){
      __autoLocks.add(fh.fid);
      log("Auto-locked",fh.fid,fh.filenameAbs);
    }
  }
  return fh.syncHandle;
};

/**
   If the given file-holding object has a sync handle attached to it,
   that handle is remove and asynchronously closed. Though it may
   sound sensible to continue work as soon as the close() returns
   (noting that it's asynchronous), doing so can cause operations
   performed soon afterwards, e.g. a call to getSyncHandle() to fail
   because they may happen out of order from the close(). OPFS does
   not guaranty that the actual order of operations is retained in
   such cases. i.e.  always "await" on the result of this function.
*/
const closeSyncHandle = async (fh)=>{
  if(fh.syncHandle){
    log("Closing sync handle for",fh.filenameAbs);
    const h = fh.syncHandle;
    delete fh.syncHandle;
    delete fh.xLock;
    __autoLocks.delete(fh.fid);
    return h.close();
  }
};

/**
   A proxy for closeSyncHandle() which is guaranteed to not throw.

   This function is part of a lock/unlock step in functions which
   require a sync access handle but may be called without xLock()
   having been called first. Such calls need to release that
   handle to avoid locking the file for all of time. This is an
   _attempt_ at reducing cross-tab contention but it may prove
   to be more of a problem than a solution and may need to be
   removed.
*/
const closeSyncHandleNoThrow = async (fh)=>{
  try{await closeSyncHandle(fh)}
  catch(e){
    warn("closeSyncHandleNoThrow() ignoring:",e,fh);
  }
};

/**
   Stores the given value at state.sabOPView[state.opIds.rc] and then
   Atomics.notify()'s it.
*/
const storeAndNotify = (opName, value)=>{
  log(opName+"() => notify(",value,")");
  Atomics.store(state.sabOPView, state.opIds.rc, value);
  Atomics.notify(state.sabOPView, state.opIds.rc);
};

/**
   Throws if fh is a file-holding object which is flagged as read-only.
*/
const affirmNotRO = function(opName,fh){
  if(fh.readOnly) toss(opName+"(): File is read-only: "+fh.filenameAbs);
};
const affirmLocked = function(opName,fh){
  //if(!fh.syncHandle) toss(opName+"(): File does not have a lock: "+fh.filenameAbs);
  /**
     Currently a no-op, as speedtest1 triggers xRead() without a
     lock (that seems like a bug but it's currently uninvestigated).
     This means, however, that some OPFS VFS routines may trigger
     acquisition of a lock but never let it go until xUnlock() is
     called (which it likely won't be if xLock() was not called).
  */
};

/**
   We track 2 different timers: the "metrics" timer records how much
   time we spend performing work. The "wait" timer records how much
   time we spend waiting on the underlying OPFS timer. See the calls
   to mTimeStart(), mTimeEnd(), wTimeStart(), and wTimeEnd()
   throughout this file to see how they're used.
*/
const __mTimer = Object.create(null);
__mTimer.op = undefined;
__mTimer.start = undefined;
const mTimeStart = (op)=>{
  __mTimer.start = performance.now();
  __mTimer.op = op;
  //metrics[op] || toss("Maintenance required: missing metrics for",op);
  ++metrics[op].count;
};
const mTimeEnd = ()=>(
  metrics[__mTimer.op].time += performance.now() - __mTimer.start
);
const __wTimer = Object.create(null);
__wTimer.op = undefined;
__wTimer.start = undefined;
const wTimeStart = (op)=>{
  __wTimer.start = performance.now();
  __wTimer.op = op;
  //metrics[op] || toss("Maintenance required: missing metrics for",op);
};
const wTimeEnd = ()=>(
  metrics[__wTimer.op].wait += performance.now() - __wTimer.start
);

/**
   Gets set to true by the 'opfs-async-shutdown' command to quit the
   wait loop. This is only intended for debugging purposes: we cannot
   inspect this file's state while the tight waitLoop() is running and
   need a way to stop that loop for introspection purposes.
*/
let flagAsyncShutdown = false;


/**
   Asynchronous wrappers for sqlite3_vfs and sqlite3_io_methods
   methods, as well as helpers like mkdir(). Maintenance reminder:
   members are in alphabetical order to simplify finding them.
*/
const vfsAsyncImpls = {
  'opfs-async-metrics': async ()=>{
    mTimeStart('opfs-async-metrics');
    metrics.dump();
    storeAndNotify('opfs-async-metrics', 0);
    mTimeEnd();
  },
  'opfs-async-shutdown': async ()=>{
    flagAsyncShutdown = true;
    storeAndNotify('opfs-async-shutdown', 0);
  },
  mkdir: async (dirname)=>{
    mTimeStart('mkdir');
    let rc = 0;
    wTimeStart('mkdir');
    try {
        await getDirForFilename(dirname+"/filepart", true);
    }catch(e){
      state.s11n.storeException(2,e);
      rc = state.sq3Codes.SQLITE_IOERR;
    }finally{
      wTimeEnd();
    }
    storeAndNotify('mkdir', rc);
    mTimeEnd();
  },
  xAccess: async (filename)=>{
    mTimeStart('xAccess');
    /* OPFS cannot support the full range of xAccess() queries sqlite3
       calls for. We can essentially just tell if the file is
       accessible, but if it is it's automatically writable (unless
       it's locked, which we cannot(?) know without trying to open
       it). OPFS does not have the notion of read-only.

       The return semantics of this function differ from sqlite3's
       xAccess semantics because we are limited in what we can
       communicate back to our synchronous communication partner: 0 =
       accessible, non-0 means not accessible.
    */
    let rc = 0;
    wTimeStart('xAccess');
    try{
      const [dh, fn] = await getDirForFilename(filename);
      await dh.getFileHandle(fn);
    }catch(e){
      state.s11n.storeException(2,e);
      rc = state.sq3Codes.SQLITE_IOERR;
    }finally{
      wTimeEnd();
    }
    storeAndNotify('xAccess', rc);
    mTimeEnd();
  },
  xClose: async function(fid/*sqlite3_file pointer*/){
    const opName = 'xClose';
    mTimeStart(opName);
    __autoLocks.delete(fid);
    const fh = __openFiles[fid];
    let rc = 0;
    wTimeStart(opName);
    if(fh){
      delete __openFiles[fid];
      await closeSyncHandle(fh);
      if(fh.deleteOnClose){
        try{ await fh.dirHandle.removeEntry(fh.filenamePart) }
        catch(e){ warn("Ignoring dirHandle.removeEntry() failure of",fh,e) }
      }
    }else{
      state.s11n.serialize();
      rc = state.sq3Codes.SQLITE_NOTFOUND;
    }
    wTimeEnd();
    storeAndNotify(opName, rc);
    mTimeEnd();
  },
  xDelete: async function(...args){
    mTimeStart('xDelete');
    const rc = await vfsAsyncImpls.xDeleteNoWait(...args);
    storeAndNotify('xDelete', rc);
    mTimeEnd();
  },
  xDeleteNoWait: async function(filename, syncDir = 0, recursive = false){
    /* The syncDir flag is, for purposes of the VFS API's semantics,
       ignored here. However, if it has the value 0x1234 then: after
       deleting the given file, recursively try to delete any empty
       directories left behind in its wake (ignoring any errors and
       stopping at the first failure).

       That said: we don't know for sure that removeEntry() fails if
       the dir is not empty because the API is not documented. It has,
       however, a "recursive" flag which defaults to false, so
       presumably it will fail if the dir is not empty and that flag
       is false.
    */
    let rc = 0;
    wTimeStart('xDelete');
    try {
      while(filename){
        const [hDir, filenamePart] = await getDirForFilename(filename, false);
        if(!filenamePart) break;
        await hDir.removeEntry(filenamePart, {recursive});
        if(0x1234 !== syncDir) break;
        recursive = false;
        filename = getResolvedPath(filename, true);
        filename.pop();
        filename = filename.join('/');
      }
    }catch(e){
      state.s11n.storeException(2,e);
      rc = state.sq3Codes.SQLITE_IOERR_DELETE;
    }
    wTimeEnd();
    return rc;
  },
  xFileSize: async function(fid/*sqlite3_file pointer*/){
    mTimeStart('xFileSize');
    const fh = __openFiles[fid];
    let rc;
    wTimeStart('xFileSize');
    try{
      affirmLocked('xFileSize',fh);
      rc = await (await getSyncHandle(fh)).getSize();
      state.s11n.serialize(Number(rc));
      rc = 0;
    }catch(e){
      state.s11n.storeException(2,e);
      rc = state.sq3Codes.SQLITE_IOERR;
    }
    wTimeEnd();
    storeAndNotify('xFileSize', rc);
    mTimeEnd();
  },
  xLock: async function(fid/*sqlite3_file pointer*/,
                        lockType/*SQLITE_LOCK_...*/){
    mTimeStart('xLock');
    const fh = __openFiles[fid];
    let rc = 0;
    const oldLockType = fh.xLock;
    fh.xLock = lockType;
    if( !fh.syncHandle ){
      wTimeStart('xLock');
      try {
        await getSyncHandle(fh);
        __autoLocks.delete(fid);
      }catch(e){
        state.s11n.storeException(1,e);
        rc = state.sq3Codes.SQLITE_IOERR_LOCK;
        fh.xLock = oldLockType;
      }
      wTimeEnd();
    }
    storeAndNotify('xLock',rc);
    mTimeEnd();
  },
  xOpen: async function(fid/*sqlite3_file pointer*/, filename,
                        flags/*SQLITE_OPEN_...*/){
    const opName = 'xOpen';
    mTimeStart(opName);
    const deleteOnClose = (state.sq3Codes.SQLITE_OPEN_DELETEONCLOSE & flags);
    const create = (state.sq3Codes.SQLITE_OPEN_CREATE & flags);
    wTimeStart('xOpen');
    try{
      let hDir, filenamePart;
      try {
        [hDir, filenamePart] = await getDirForFilename(filename, !!create);
      }catch(e){
        state.s11n.storeException(1,e);
        storeAndNotify(opName, state.sq3Codes.SQLITE_NOTFOUND);
        mTimeEnd();
        wTimeEnd();
        return;
      }
      const hFile = await hDir.getFileHandle(filenamePart, {create});
      /**
         wa-sqlite, at this point, grabs a SyncAccessHandle and
         assigns it to the syncHandle prop of the file state
         object, but only for certain cases and it's unclear why it
         places that limitation on it.
      */
      wTimeEnd();
      __openFiles[fid] = Object.assign(Object.create(null),{
        fid: fid,
        filenameAbs: filename,
        filenamePart: filenamePart,
        dirHandle: hDir,
        fileHandle: hFile,
        sabView: state.sabFileBufView,
        readOnly: create
          ? false : (state.sq3Codes.SQLITE_OPEN_READONLY & flags),
        deleteOnClose: deleteOnClose
      });
      storeAndNotify(opName, 0);
    }catch(e){
      wTimeEnd();
      error(opName,e);
      state.s11n.storeException(1,e);
      storeAndNotify(opName, state.sq3Codes.SQLITE_IOERR);
    }
    mTimeEnd();
  },
  xRead: async function(fid/*sqlite3_file pointer*/,n,offset64){
    mTimeStart('xRead');
    let rc = 0, nRead;
    const fh = __openFiles[fid];
    try{
      affirmLocked('xRead',fh);
      wTimeStart('xRead');
      nRead = (await getSyncHandle(fh)).read(
        fh.sabView.subarray(0, n),
        {at: Number(offset64)}
      );
      wTimeEnd();
      if(nRead < n){/* Zero-fill remaining bytes */
        fh.sabView.fill(0, nRead, n);
        rc = state.sq3Codes.SQLITE_IOERR_SHORT_READ;
      }
    }catch(e){
      if(undefined===nRead) wTimeEnd();
      error("xRead() failed",e,fh);
      state.s11n.storeException(1,e);
      rc = state.sq3Codes.SQLITE_IOERR_READ;
    }
    storeAndNotify('xRead',rc);
    mTimeEnd();
  },
  xSync: async function(fid/*sqlite3_file pointer*/,flags/*ignored*/){
    mTimeStart('xSync');
    const fh = __openFiles[fid];
    let rc = 0;
    if(!fh.readOnly && fh.syncHandle){
      try {
        wTimeStart('xSync');
        await fh.syncHandle.flush();
      }catch(e){
        state.s11n.storeException(2,e);
        rc = state.sq3Codes.SQLITE_IOERR_FSYNC;
      }
      wTimeEnd();
    }
    storeAndNotify('xSync',rc);
    mTimeEnd();
  },
  xTruncate: async function(fid/*sqlite3_file pointer*/,size){
    mTimeStart('xTruncate');
    let rc = 0;
    const fh = __openFiles[fid];
    wTimeStart('xTruncate');
    try{
      affirmLocked('xTruncate',fh);
      affirmNotRO('xTruncate', fh);
      await (await getSyncHandle(fh)).truncate(size);
    }catch(e){
      error("xTruncate():",e,fh);
      state.s11n.storeException(2,e);
      rc = state.sq3Codes.SQLITE_IOERR_TRUNCATE;
    }
    wTimeEnd();
    storeAndNotify('xTruncate',rc);
    mTimeEnd();
  },
  xUnlock: async function(fid/*sqlite3_file pointer*/,
                          lockType/*SQLITE_LOCK_...*/){
    mTimeStart('xUnlock');
    let rc = 0;
    const fh = __openFiles[fid];
    if( state.sq3Codes.SQLITE_LOCK_NONE===lockType
        && fh.syncHandle ){
      wTimeStart('xUnlock');
      try { await closeSyncHandle(fh) }
      catch(e){
        state.s11n.storeException(1,e);
        rc = state.sq3Codes.SQLITE_IOERR_UNLOCK;
      }
      wTimeEnd();
    }
    storeAndNotify('xUnlock',rc);
    mTimeEnd();
  },
  xWrite: async function(fid/*sqlite3_file pointer*/,n,offset64){
    mTimeStart('xWrite');
    let rc;
    const fh = __openFiles[fid];
    wTimeStart('xWrite');
    try{
      affirmLocked('xWrite',fh);
      affirmNotRO('xWrite', fh);
      rc = (
        n === (await getSyncHandle(fh))
          .write(fh.sabView.subarray(0, n),
                 {at: Number(offset64)})
      ) ? 0 : state.sq3Codes.SQLITE_IOERR_WRITE;
    }catch(e){
      error("xWrite():",e,fh);
      state.s11n.storeException(1,e);
      rc = state.sq3Codes.SQLITE_IOERR_WRITE;
    }
    wTimeEnd();
    storeAndNotify('xWrite',rc);
    mTimeEnd();
  }
}/*vfsAsyncImpls*/;

const initS11n = ()=>{
  /**
     ACHTUNG: this code is 100% duplicated in the other half of this
     proxy! The documentation is maintained in the "synchronous half".
  */
  if(state.s11n) return state.s11n;
  const textDecoder = new TextDecoder(),
  textEncoder = new TextEncoder('utf-8'),
  viewU8 = new Uint8Array(state.sabIO, state.sabS11nOffset, state.sabS11nSize),
  viewDV = new DataView(state.sabIO, state.sabS11nOffset, state.sabS11nSize);
  state.s11n = Object.create(null);
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

  state.s11n.storeException = state.asyncS11nExceptions
    ? ((priority,e)=>{
      if(priority<=state.asyncS11nExceptions){
        state.s11n.serialize([e.name,': ',e.message].join(""));
      }
    })
    : ()=>{};

  return state.s11n;
}/*initS11n()*/;

const waitLoop = async function f(){
  const opHandlers = Object.create(null);
  for(let k of Object.keys(state.opIds)){
    const vi = vfsAsyncImpls[k];
    if(!vi) continue;
    const o = Object.create(null);
    opHandlers[state.opIds[k]] = o;
    o.key = k;
    o.f = vi;
  }
  /**
     waitTime is how long (ms) to wait for each Atomics.wait().
     We need to wake up periodically to give the thread a chance
     to do other things.
  */
  const waitTime = 500;
  while(!flagAsyncShutdown){
    try {
      if('timed-out'===Atomics.wait(
        state.sabOPView, state.opIds.whichOp, 0, waitTime
      )){
        if(__autoLocks.size){
          /* Release all auto-locks. */
          for(const fid of __autoLocks){
            const fh = __openFiles[fid];
            await closeSyncHandleNoThrow(fh);
            log("Auto-unlocked",fid,fh.filenameAbs);
          }
        }
        continue;
      }
      const opId = Atomics.load(state.sabOPView, state.opIds.whichOp);
      Atomics.store(state.sabOPView, state.opIds.whichOp, 0);
      const hnd = opHandlers[opId] ?? toss("No waitLoop handler for whichOp #",opId);
      const args = state.s11n.deserialize(
        true /* clear s11n to keep the caller from confusing this with
                an exception string written by the upcoming
                operation */
      ) || [];
      //warn("waitLoop() whichOp =",opId, hnd, args);
      if(hnd.f) await hnd.f(...args);
      else error("Missing callback for opId",opId);
    }catch(e){
      error('in waitLoop():',e);
    }
  }
};

navigator.storage.getDirectory().then(function(d){
  const wMsg = (type)=>postMessage({type});
  state.rootDir = d;
  self.onmessage = function({data}){
    switch(data.type){
        case 'opfs-async-init':{
          /* Receive shared state from synchronous partner */
          const opt = data.args;
          state.littleEndian = opt.littleEndian;
          state.asyncS11nExceptions = opt.asyncS11nExceptions;
          state.verbose = opt.verbose ?? 2;
          state.fileBufferSize = opt.fileBufferSize;
          state.sabS11nOffset = opt.sabS11nOffset;
          state.sabS11nSize = opt.sabS11nSize;
          state.sabOP = opt.sabOP;
          state.sabOPView = new Int32Array(state.sabOP);
          state.sabIO = opt.sabIO;
          state.sabFileBufView = new Uint8Array(state.sabIO, 0, state.fileBufferSize);
          state.sabS11nView = new Uint8Array(state.sabIO, state.sabS11nOffset, state.sabS11nSize);
          state.opIds = opt.opIds;
          state.sq3Codes = opt.sq3Codes;
          Object.keys(vfsAsyncImpls).forEach((k)=>{
            if(!Number.isFinite(state.opIds[k])){
              toss("Maintenance required: missing state.opIds[",k,"]");
            }
          });
          initS11n();
          metrics.reset();
          log("init state",state);
          wMsg('opfs-async-inited');
          waitLoop();
          break;
        }
        case 'opfs-async-restart':
          if(flagAsyncShutdown){
            warn("Restarting after opfs-async-shutdown. Might or might not work.");
            flagAsyncShutdown = false;
            waitLoop();
          }
          break;
        case 'opfs-async-metrics':
          metrics.dump();
          break;
    }
  };
  wMsg('opfs-async-loaded');
}).catch((e)=>error("error initializing OPFS asyncer:",e));
