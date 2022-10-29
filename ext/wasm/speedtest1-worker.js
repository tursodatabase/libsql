'use strict';
(function(){
  let speedtestJs = 'speedtest1.js';
  const urlParams = new URL(self.location.href).searchParams;
  if(urlParams.has('sqlite3.dir')){
    speedtestJs = urlParams.get('sqlite3.dir') + '/' + speedtestJs;
  }
  importScripts('common/whwasmutil.js', speedtestJs);
  /**
     If this environment contains OPFS, this function initializes it and
     returns the name of the dir on which OPFS is mounted, else it returns
     an empty string.
  */
  const wasmfsDir = function f(wasmUtil){
    if(undefined !== f._) return f._;
    const pdir = '/opfs';
    if( !self.FileSystemHandle
        || !self.FileSystemDirectoryHandle
        || !self.FileSystemFileHandle){
      return f._ = "";
    }
    try{
      if(0===wasmUtil.xCallWrapped(
        'sqlite3_wasm_init_wasmfs', 'i32', ['string'], pdir
      )){
        return f._ = pdir;
      }else{
        return f._ = "";
      }
    }catch(e){
      // sqlite3_wasm_init_wasmfs() is not available
      return f._ = "";
    }
  };
  wasmfsDir._ = undefined;

  const mPost = function(msgType,payload){
    postMessage({type: msgType, data: payload});
  };

  const App = Object.create(null);
  App.logBuffer = [];
  const logMsg = (type,msgArgs)=>{
    const msg = msgArgs.join(' ');
    App.logBuffer.push(msg);
    mPost(type,msg);
  };
  const log = (...args)=>logMsg('stdout',args);
  const logErr = (...args)=>logMsg('stderr',args);

  const runSpeedtest = function(cliFlagsArray){
    const scope = App.wasm.scopedAllocPush();
    const dbFile = App.pDir+"/speedtest1.sqlite3";
    try{
      const argv = [
        "speedtest1.wasm", ...cliFlagsArray, dbFile
      ];
      App.logBuffer.length = 0;
      mPost('run-start', [...argv]);
      App.wasm.xCall('wasm_main', argv.length,
                     App.wasm.scopedAllocMainArgv(argv));
    }catch(e){
      mPost('error',e.message);
    }finally{
      App.wasm.scopedAllocPop(scope);
      mPost('run-end', App.logBuffer.join('\n'));
      App.logBuffer.length = 0;
    }
  };

  self.onmessage = function(msg){
    msg = msg.data;
    switch(msg.type){
        case 'run': runSpeedtest(msg.data || []); break;
        default:
          logErr("Unhandled worker message type:",msg.type);
          break;
    }
  };

  const EmscriptenModule = {
    print: log,
    printErr: logErr,
    setStatus: (text)=>mPost('load-status',text)
  };
  self.sqlite3InitModule(EmscriptenModule).then((sqlite3)=>{
    const S = sqlite3;
    App.vfsUnlink = function(pDb, fname){
      const pVfs = S.wasm.sqlite3_wasm_db_vfs(pDb, 0);
      if(pVfs) S.wasm.sqlite3_wasm_vfs_unlink(pVfs, fname||0);
    };
    App.pDir = wasmfsDir(S.wasm);
    App.wasm = S.wasm;
    //if(App.pDir) log("Persistent storage:",pDir);
    //else log("Using transient storage.");
    mPost('ready',true);
    log("Registered VFSes:", ...S.capi.sqlite3_js_vfs_list());
  });
})();
