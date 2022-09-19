'use strict';
(function(){
  importScripts('common/whwasmutil.js','speedtest1.js');
  /**
     If this environment contains OPFS, this function initializes it and
     returns the name of the dir on which OPFS is mounted, else it returns
     an empty string.
  */
  const wasmfsDir = function f(wasmUtil){
    if(undefined !== f._) return f._;
    const pdir = '/persistent';
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
    const dbFile = 0 ? "" : App.pDir+"/speedtest1.db";
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
      App.unlink(dbFile);
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
  self.sqlite3Speedtest1InitModule(EmscriptenModule).then(function(EModule){
    const S = EModule.sqlite3;
    log("Module inited.");
    return S.installOpfsVfs()
      .catch((e)=>console.warn(e.message))
      .then(()=>{
        App.unlink = S.capi.wasm.xWrap("sqlite3_wasm_vfs_unlink", "int", ["string"]);
        App.pDir = wasmfsDir(S.wasm);
        App.wasm = S.capi.wasm;
        //if(App.pDir) log("Persistent storage:",pDir);
        //else log("Using transient storage.");
        mPost('ready',true);
      });
  });
})();
