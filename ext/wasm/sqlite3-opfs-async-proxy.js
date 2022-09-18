/*
  2022-09-16

  The author disclaims copyright to this source code.  In place of a
  legal notice, here is a blessing:

  *   May you do good and not evil.
  *   May you find forgiveness for yourself and forgive others.
  *   May you share freely, never taking more than you give.

  ***********************************************************************

  An INCOMPLETE and UNDER CONSTRUCTION experiment for OPFS: a Worker
  which manages asynchronous OPFS handles on behalf of a synchronous
  API which controls it via a combination of Worker messages,
  SharedArrayBuffer, and Atomics.

  Highly indebted to:

  https://github.com/rhashimoto/wa-sqlite/blob/master/src/examples/OriginPrivateFileSystemVFS.js

  for demonstrating how to use the OPFS APIs.

  This file is to be loaded as a Worker. It does not have any direct
  access to the sqlite3 JS/WASM bits, so any bits which it needs (most
  notably SQLITE_xxx integer codes) have to be imported into it via an
  initialization process.
*/
'use strict';
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

const __logPrefix = "OPFS asyncer:";
const log = (...args)=>{
  if(state.verbose>2) console.log(__logPrefix,...args);
};
const warn =  (...args)=>{
  if(state.verbose>1) console.warn(__logPrefix,...args);
};
const error =  (...args)=>{
  if(state.verbose) console.error(__logPrefix,...args);
};

warn("This file is very much experimental and under construction.",self.location.pathname);

/**
   Map of sqlite3_file pointers (integers) to metadata related to a
   given OPFS file handles. The pointers are, in this side of the
   interface, opaque file handle IDs provided by the synchronous
   part of this constellation. Each value is an object with a structure
   demonstrated in the xOpen() impl.
*/
const __openFiles = Object.create(null);

/**
   Expects an OPFS file path. It gets resolved, such that ".."
   components are properly expanded, and returned. If the 2nd
   are is true, it's returned as an array of path elements,
   else it's returned as an absolute path string.
*/
const getResolvedPath = function(filename,splitIt){
  const p = new URL(
    filename, 'file://irrelevant'
  ).pathname;
  return splitIt ? p.split('/').filter((v)=>!!v) : p;
}

/**
   Takes the absolute path to a filesystem element. Returns an array
   of [handleOfContainingDir, filename]. If the 2nd argument is
   truthy then each directory element leading to the file is created
   along the way. Throws if any creation or resolution fails.
*/
const getDirForPath = async function f(absFilename, createDirs = false){
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
   Stores the given value at the array index reserved for the given op
   and then Atomics.notify()'s it.
*/
const storeAndNotify = (opName, value)=>{
  log(opName+"() is notify()ing w/ value:",value);
  Atomics.store(state.opSABView, state.opIds[opName], value);
  Atomics.notify(state.opSABView, state.opIds[opName]);
};

/**
   Throws if fh is a file-holding object which is flagged as read-only.
*/
const affirmNotRO = function(opName,fh){
  if(fh.readOnly) toss(opName+"(): File is read-only: "+fh.filenameAbs);
};

/**
   Asynchronous wrappers for sqlite3_vfs and sqlite3_io_methods
   methods. Maintenance reminder: members are in alphabetical order
   to simplify finding them.
*/
const vfsAsyncImpls = {
  xAccess: async function(filename){
    log("xAccess(",arguments[0],")");
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
    try{
      const [dh, fn] = await getDirForPath(filename);
      await dh.getFileHandle(fn);
    }catch(e){
      rc = state.sq3Codes.SQLITE_IOERR;
    }
    storeAndNotify('xAccess', rc);
  },
  xClose: async function(fid){
    const opName = 'xClose';
    log(opName+"(",arguments[0],")");
    const fh = __openFiles[fid];
    if(fh){
      delete __openFiles[fid];
      if(fh.accessHandle) await fh.accessHandle.close();
      if(fh.deleteOnClose){
        try{ await fh.dirHandle.removeEntry(fh.filenamePart) }
        catch(e){ warn("Ignoring dirHandle.removeEntry() failure of",fh,e) }
      }
      storeAndNotify(opName, 0);
    }else{
      storeAndNotify(opName, state.sq3Codes.SQLITE_NOFOUND);
    }
  },
  xDelete: async function({filename, syncDir/*ignored*/}){
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
    log("xDelete(",arguments[0],")");
    try {
      while(filename){
        const [hDir, filenamePart] = await getDirForPath(filename, false);
        //log("Removing:",hDir, filenamePart);
        if(!filenamePart) break;
        await hDir.removeEntry(filenamePart);
        if(0x1234 !== syncDir) break;
        filename = getResolvedPath(filename, true);
        filename.pop();
        filename = filename.join('/');
      }
    }catch(e){
      /* Ignoring: _presumably_ the file can't be found or a dir is
         not empty. */
      //error("Delete failed",filename, e.message);
    }
    storeAndNotify('xDelete', 0);
  },
  xFileSize: async function(fid){
    log("xFileSize(",arguments,")");
    const fh = __openFiles[fid];
    let sz;
    try{
      sz = await fh.accessHandle.getSize();
      fh.sabViewFileSize.setBigInt64(0, BigInt(sz));
      sz = 0;
    }catch(e){
      error("xFileSize():",e, fh);
      sz = state.sq3Codes.SQLITE_IOERR;
    }
    storeAndNotify('xFileSize', sz);
  },
  xOpen: async function({
    fid/*sqlite3_file pointer*/,
    sab/*file-specific SharedArrayBuffer*/,
    filename,
    fileType = undefined /*mainDb, mainJournal, etc.*/,
    create = false, readOnly = false, deleteOnClose = false
  }){
    const opName = 'xOpen';
    try{
      if(create) readOnly = false;
      log(opName+"(",arguments[0],")");
      let hDir, filenamePart;
      try {
        [hDir, filenamePart] = await getDirForPath(filename, !!create);
      }catch(e){
        storeAndNotify(opName, state.sql3Codes.SQLITE_NOTFOUND);
        return;
      }
      const hFile = await hDir.getFileHandle(filenamePart, {create: !!create});
      log(opName,"filenamePart =",filenamePart, 'hDir =',hDir);
      const fobj = __openFiles[fid] = Object.create(null);
      fobj.filenameAbs = filename;
      fobj.filenamePart = filenamePart;
      fobj.dirHandle = hDir;
      fobj.fileHandle = hFile;
      fobj.fileType = fileType;
      fobj.sab = sab;
      fobj.sabViewFileSize = new DataView(sab,state.fbInt64Offset,8);
      fobj.create = !!create;
      fobj.readOnly = !!readOnly;
      fobj.deleteOnClose = !!deleteOnClose;
      /**
         wa-sqlite, at this point, grabs a SyncAccessHandle and
         assigns it to the accessHandle prop of the file state
         object, but only for certain cases and it's unclear why it
         places that limitation on it.
      */
      fobj.accessHandle = await hFile.createSyncAccessHandle();
      storeAndNotify(opName, 0);
    }catch(e){
      error(opName,e);
      storeAndNotify(opName, state.sq3Codes.SQLITE_IOERR);
    }
  },
  xRead: async function({fid,n,offset}){
    log("xRead(",arguments[0],")");
    let rc = 0;
    const fh = __openFiles[fid];
    try{
      const aRead = new Uint8Array(fh.sab, 0, n);
      const nRead = fh.accessHandle.read(aRead, {at: Number(offset)});
      if(nRead < n){/* Zero-fill remaining bytes */
        new Uint8Array(fh.sab).fill(0, nRead, n);
        rc = state.sq3Codes.SQLITE_IOERR_SHORT_READ;
      }
    }catch(e){
      error("xRead() failed",e,fh);
      rc = state.sq3Codes.SQLITE_IOERR_READ;
    }
    storeAndNotify('xRead',rc);
  },
  xSleep: async function f(ms){
    log("xSleep(",ms,")");
    await new Promise((resolve)=>{
      setTimeout(()=>resolve(), ms);
    }).finally(()=>storeAndNotify('xSleep',0));
  },
  xSync: async function({fid,flags/*ignored*/}){
    log("xSync(",arguments[0],")");
    const fh = __openFiles[fid];
    if(!fh.readOnly && fh.accessHandle) await fh.accessHandle.flush();
    storeAndNotify('xSync',0);
  },
  xTruncate: async function({fid,size}){
    log("xTruncate(",arguments[0],")");
    let rc = 0;
    const fh = __openFiles[fid];
    try{
      affirmNotRO('xTruncate', fh);
      await fh.accessHandle.truncate(size);
    }catch(e){
      error("xTruncate():",e,fh);
      rc = state.sq3Codes.SQLITE_IOERR_TRUNCATE;
    }
    storeAndNotify('xTruncate',rc);
  },
  xWrite: async function({fid,src,n,offset}){
    log("xWrite(",arguments[0],")");
    let rc;
    const fh = __openFiles[fid];
    try{
      affirmNotRO('xWrite', fh);
      const nOut = fh.accessHandle.write(new Uint8Array(fh.sab, 0, n),
                                         {at: Number(offset)});
      rc = (nOut===n) ? 0 : state.sq3Codes.SQLITE_IOERR_WRITE;
    }catch(e){
      error("xWrite():",e,fh);
      rc = state.sq3Codes.SQLITE_IOERR_WRITE;
    }
    storeAndNotify('xWrite',rc);
  }
};

navigator.storage.getDirectory().then(function(d){
  const wMsg = (type)=>postMessage({type});
  state.rootDir = d;
  log("state.rootDir =",state.rootDir);
  self.onmessage = async function({data}){
    log("self.onmessage()",data);
    switch(data.type){
        case 'init':{
          /* Receive shared state from synchronous partner */
          const opt = data.payload;
          state.verbose = opt.verbose ?? 2;
          state.fileBufferSize = opt.fileBufferSize;
          state.fbInt64Offset = opt.fbInt64Offset;
          state.opSAB = opt.opSAB;
          state.opSABView = new Int32Array(state.opSAB);
          state.opIds = opt.opIds;
          state.sq3Codes = opt.sq3Codes;
          Object.keys(vfsAsyncImpls).forEach((k)=>{
            if(!Number.isFinite(state.opIds[k])){
              toss("Maintenance required: missing state.opIds[",k,"]");
            }
          });
          log("init state",state);
          wMsg('inited');
          break;
        }
        default:{
          let err;
          const m = vfsAsyncImpls[data.type] || toss("Unknown message type:",data.type);
          try {
            await m(data.payload).catch((e)=>err=e);
          }catch(e){
            err = e;
          }
          if(err){
            error("Error handling",data.type+"():",e);
            storeAndNotify(data.type, state.sq3Codes.SQLITE_ERROR);
          }
          break;
        }
    }
  };
  wMsg('loaded');
}).catch((e)=>error(e));
