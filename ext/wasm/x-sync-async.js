'use strict';
const doAtomicsStuff = function(sqlite3){
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
  const W = new Worker("sqlite3-opfs-async-proxy.js");
  const wMsg = (type,payload)=>W.postMessage({type,payload});
  warn("This file is very much experimental and under construction.",self.location.pathname);

  /**
     State which we send to the async-api Worker or share with it.
     This object must initially contain only cloneable or sharable
     objects. After the worker's "ready" message arrives, other types
     of data may be added to it.
  */
  const state = Object.create(null);
  state.opIds = Object.create(null);
  state.opIds.xAccess = 1;
  state.opIds.xClose = 2;
  state.opIds.xDelete = 3;
  state.opIds.xFileSize = 4;
  state.opIds.xOpen = 5;
  state.opIds.xRead = 6;
  state.opIds.xSync = 7;
  state.opIds.xTruncate = 8;
  state.opIds.xWrite = 9;
  state.opIds.xSleep = 10;
  state.opIds.xBlock = 99 /* to block worker while this code is still handling something */;
  state.opSab = new SharedArrayBuffer(64);
  state.fileBufferSize = 1024 * 65 /* 64k = max sqlite3 page size */;
  /* TODO: use SQLITE_xxx err codes. */
  state.errCodes = Object.create(null);
  state.errCodes.Error = -100;
  state.errCodes.IO = -101;
  state.errCodes.NotFound = -102;
  state.errCodes.Misuse = -103;

  // TODO: add any SQLITE_xxx symbols we need here.
  state.sq3Codes = Object.create(null);
  
  const isWorkerErrCode = (n)=>(n<=state.errCodes.Error);
  
  const opStore = (op,val=-1)=>Atomics.store(state.opBuf, state.opIds[op], val);
  const opWait = (op,val=-1)=>Atomics.wait(state.opBuf, state.opIds[op], val);

  const opRun = (op,args)=>{
    opStore(op);
    wMsg(op, args);
    opWait(op);
    return Atomics.load(state.opBuf, state.opIds[op]);
  };

  const wait = (ms,value)=>{
    return new Promise((resolve)=>{
      setTimeout(()=>resolve(value), ms);
    });
  };

  const vfsSyncWrappers = {
    xOpen: function f(pFile, name, flags, outFlags = {}){
      if(!f._){
        f._ = {
          // TODO: map openFlags to args.fileType names.
        };
      }
      const args = Object.create(null);
      args.fid = pFile;
      args.filename = name;
      args.sab = new SharedArrayBuffer(state.fileBufferSize);
      args.fileType = undefined /*TODO: populate based on SQLITE_OPEN_xxx */;
      // TODO: populate args object based on flags:
      // args.create, args.readOnly, args.deleteOnClose
      args.create = true;
      args.deleteOnClose = true;
      const rc = opRun('xOpen', args);
      if(!rc){
        outFlags.readOnly = args.readOnly;
        args.ba = new Uint8Array(args.sab);
        state.openFiles[pFile] = args;
      }
      return rc;
    },
    xClose: function(pFile){
      let rc = 0;
      if(state.openFiles[pFile]){
        delete state.openFiles[pFile];
        rc = opRun('xClose', pFile);
      }
      return rc;
    }
  };


  const doSomething = function(){
    //state.ioBuf = new Uint8Array(state.sabIo);
    const fid = 37;
    let rc = vfsSyncWrappers.xOpen(fid, "/foo/bar/baz.sqlite3",0, {});
    log("open rc =",rc,"state.opBuf[xOpen] =",state.opBuf[state.opIds.xOpen]);
    if(isWorkerErrCode(rc)){
      error("open failed with code",rc);
      return;
    }
    log("xSleep()ing before close()ing...");
    opRun('xSleep',{ms: 1500});
    log("wait()ing before close()ing...");
    wait(1500).then(function(){
      rc = vfsSyncWrappers.xClose(fid);
      log("xClose rc =",rc,"opBuf =",state.opBuf);
    });
  };

  W.onmessage = function({data}){
    log("Worker.onmessage:",data);
    switch(data.type){
        case 'ready':
          wMsg('init',state);
          state.opBuf = new Int32Array(state.opSab);
          state.openFiles = Object.create(null);
          doSomething();
          break;
    }
  };
}/*doAtomicsStuff*/

importScripts('sqlite3.js');
self.sqlite3InitModule().then((EmscriptenModule)=>doAtomicsStuff(EmscriptenModule.sqlite3));
