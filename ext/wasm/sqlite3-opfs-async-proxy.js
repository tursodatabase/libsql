/*
  2022-09-16

  The author disclaims copyright to this source code.  In place of a
  legal notice, here is a blessing:

  *   May you do good and not evil.
  *   May you find forgiveness for yourself and forgive others.
  *   May you share freely, never taking more than you give.

  ***********************************************************************

  A EXTREMELY INCOMPLETE and UNDER CONSTRUCTION experiment for OPFS: a
  Worker which manages asynchronous OPFS handles on behalf of a
  synchronous API which controls it via a combination of Worker
  messages, SharedArrayBuffer, and Atomics.

  Highly indebted to:

  https://github.com/rhashimoto/wa-sqlite/blob/master/src/examples/OriginPrivateFileSystemVFS.js

  for demonstrating how to use the OPFS APIs.
*/
'use strict';
(function(){
  const toss = function(...args){throw new Error(args.join(' '))};
  if(self.window === self){
    toss("This code cannot run from the main thread.",
         "Load it as a Worker from a separate Worker.");
  }else if(!navigator.storage.getDirectory){
    toss("This API requires navigator.storage.getDirectory.");
  }
  const logPrefix = "OPFS worker:";
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
  const wMsg = (type,payload)=>postMessage({type,payload});

  const state = Object.create(null);
  /*state.opSab;
  state.sabIO;
  state.opBuf;
  state.opIds;
  state.rootDir;*/
  /**
     Map of sqlite3_file pointers (integers) to metadata related to a
     given OPFS file handles. The pointers are, in this side of the
     interface, opaque file handle IDs provided by the synchronous
     part of this constellation. Each value is an object with a structure
     demonstrated in the xOpen() impl.
  */
  state.openFiles = Object.create(null);

  /**
     Map of dir names to FileSystemDirectoryHandle objects.
  */
  state.dirCache = new Map;

  const __splitPath = (absFilename)=>{
    const a = absFilename.split('/').filter((v)=>!!v);
    return [a, a.pop()];
  };
  /**
     Takes the absolute path to a filesystem element. Returns an array
     of [handleOfContainingDir, filename]. If the 2nd argument is
     truthy then each directory element leading to the file is created
     along the way. Throws if any creation or resolution fails.
  */
  const getDirForPath = async function f(absFilename, createDirs = false){
    const url = new URL(
      absFilename, 'file://xyz'
    ) /* use URL to resolve path pieces such as a/../b */;
    const [path, filename] = __splitPath(url.pathname);
    const allDirs = path.join('/');
    let dh = state.dirCache.get(allDirs);
    if(!dh){
      dh = state.rootDir;
      for(const dirName of path){
        if(dirName){
          dh = await dh.getDirectoryHandle(dirName, {create: !!createDirs});
        }
      }
      state.dirCache.set(allDirs, dh);
    }
    return [dh, filename];
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

  const storeAndNotify = (opName, value)=>{
    log(opName+"() is notify()ing w/ value:",value);
    Atomics.store(state.opBuf, state.opIds[opName], value);
    Atomics.notify(state.opBuf, state.opIds[opName]);
  };

  const isInt32 = function(n){
    return ('bigint'!==typeof n /*TypeError: can't convert BigInt to number*/)
      && !!(n===(n|0) && n<=2147483647 && n>=-2147483648);
  };
  const affirm32Bits = function(n){
    return isInt32(n) || toss("Number is too large (>31 bits):",n);
  };

  const ioMethods = {
    xAccess: async function({filename, exists, readWrite}){
      log("xAccess(",arguments,")");
      const rc = 1;
      storeAndNotify('xAccess', rc);
    },
    xClose: async function(fid){
      const opName = 'xClose';
      log(opName+"(",arguments[0],")");
      log("state.openFiles",state.openFiles);
      const fh = state.openFiles[fid];
      if(fh){
        delete state.openFiles[fid];
        //await fh.close();
        if(fh.accessHandle) await fh.accessHandle.close();
        if(fh.deleteOnClose){
          try{
            await fh.dirHandle.removeEntry(fh.filenamePart);
          }
          catch(e){
            warn("Ignoring dirHandle.removeEntry() failure of",fh);
          }
        }
        log("state.openFiles",state.openFiles);
        storeAndNotify(opName, 0);
      }else{
        storeAndNotify(opName, state.errCodes.NotFound);
      }
    },
    xDelete: async function(filename){
      log("xDelete(",arguments,")");
      storeAndNotify('xClose', 0);
    },
    xFileSize: async function(fid){
      log("xFileSize(",arguments,")");
      const fh = state.openFiles[fid];
      const sz = await fh.getSize();
      affirm32Bits(sz);
      storeAndNotify('xFileSize', sz | 0);
    },
    xOpen: async function({
      fid/*sqlite3_file pointer*/, sab/*file-specific SharedArrayBuffer*/,
      filename,
      fileType = undefined /*mainDb, mainJournal, etc.*/,
      create = false, readOnly = false, deleteOnClose = false,
    }){
      const opName = 'xOpen';
      try{
        if(create) readOnly = false;
        log(opName+"(",arguments[0],")");

        let hDir, filenamePart, hFile;
        try {
          [hDir, filenamePart] = await getDirForPath(filename, !!create);
        }catch(e){
          storeAndNotify(opName, state.errCodes.NotFound);
          return;
        }
        hFile = await hDir.getFileHandle(filenamePart, {create: !!create});
        log(opName,"filenamePart =",filenamePart, 'hDir =',hDir);
        const fobj = state.openFiles[fid] = Object.create(null);
        fobj.filenameAbs = filename;
        fobj.filenamePart = filenamePart;
        fobj.dirHandle = hDir;
        fobj.fileHandle = hFile;
        fobj.accessHandle = undefined;
        fobj.fileType = fileType;
        fobj.sab = sab;
        fobj.create = !!create;
        fobj.readOnly = !!readOnly;
        fobj.deleteOnClose = !!deleteOnClose;

        /**
           wa-sqlite, at this point, grabs a SyncAccessHandle and
           assigns it to the accessHandle prop of the file state
           object, but it's unclear why it does that.
        */
        storeAndNotify(opName, 0);
      }catch(e){
        error(opName,e);
        storeAndNotify(opName, state.errCodes.IO);
      }
    },
    xRead: async function({fid,n,offset}){
      log("xRead(",arguments,")");
      affirm32Bits(n + offset);
      const fh = state.openFiles[fid];
      storeAndNotify('xRead',fid);
    },
    xSleep: async function f({ms}){
      log("xSleep(",arguments[0],")");
      await new Promise((resolve)=>{
        setTimeout(()=>resolve(), ms);
      }).finally(()=>storeAndNotify('xSleep',0));
    },
    xSync: async function({fid}){
      log("xSync(",arguments,")");
      const fh = state.openFiles[fid];
      await fh.flush();
      storeAndNotify('xSync',fid);
    },
    xTruncate: async function({fid,size}){
      log("xTruncate(",arguments,")");
      affirm32Bits(size);
      const fh = state.openFiles[fid];
      fh.truncate(size);
      storeAndNotify('xTruncate',fid);
    },
    xWrite: async function({fid,src,n,offset}){
      log("xWrite(",arguments,")");
      const fh = state.openFiles[fid];
      storeAndNotify('xWrite',fid);
    }
  };
  
  const onReady = function(){
    self.onmessage = async function({data}){
      log("self.onmessage",data);
      switch(data.type){
          case 'init':{
            const opt = data.payload;
            state.opSab = opt.opSab;
            state.opBuf = new Int32Array(state.opSab);
            state.opIds = opt.opIds;
            state.errCodes = opt.errCodes;
            state.sq3Codes = opt.sq3Codes;
            Object.keys(ioMethods).forEach((k)=>{
              if(!state.opIds[k]){
                toss("Maintenance required: missing state.opIds[",k,"]");
              }
            });
            log("init state",state);
            break;
          }
          default:{
            const m = ioMethods[data.type] || toss("Unknown message type:",data.type);
            try {
              await m(data.payload);
            }catch(e){
              error("Error handling",data.type+"():",e);
              storeAndNotify(data.type, -99);
            }
            break;
          }
      }
    };      
    wMsg('ready');
  };

  navigator.storage.getDirectory().then(function(d){
    state.rootDir = d;
    log("state.rootDir =",state.rootDir);
    onReady();
  });
    
})();
