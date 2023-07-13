import sqlite3InitModule from './jswasm/speedtest1-wasmfs.mjs';
const wMsg = (type,...args)=>{
  postMessage({type, args});
};
wMsg('log',"speedtest1-wasmfs starting...");
/**
   If this environment contains OPFS, this function initializes it and
   returns the name of the dir on which OPFS is mounted, else it returns
   an empty string.
*/
const wasmfsDir = function f(wasmUtil,dirName="/opfs"){
  if(undefined !== f._) return f._;
  if( !self.FileSystemHandle
      || !self.FileSystemDirectoryHandle
      || !self.FileSystemFileHandle){
    return f._ = "";
  }
  try{
    if(0===wasmUtil.xCallWrapped(
      'sqlite3_wasm_init_wasmfs', 'i32', ['string'], dirName
    )){
      return f._ = dirName;
    }else{
      return f._ = "";
    }
  }catch(e){
    // sqlite3_wasm_init_wasmfs() is not available
    return f._ = "";
  }
};
wasmfsDir._ = undefined;

const log = (...args)=>wMsg('log',...args);
const logErr = (...args)=>wMsg('logErr',...args);

const runTests = function(sqlite3){
  console.log("Module inited.",sqlite3);
  const wasm = sqlite3.wasm;
  const __unlink = wasm.xWrap("sqlite3_wasm_vfs_unlink", "int", ["*","string"]);
  const unlink = (fn)=>__unlink(0,fn);
  const pDir = wasmfsDir(wasm);
  if(pDir) log("Persistent storage:",pDir);
  else{
    logErr("Expecting persistent storage in this build.");
    return;
  }
  const scope = wasm.scopedAllocPush();
  const dbFile = pDir+"/speedtest1.db";
  const urlParams = new URL(self.location.href).searchParams;
  const argv = ["speedtest1"];
  if(urlParams.has('flags')){
    argv.push(...(urlParams.get('flags').split(',')));
    let i = argv.indexOf('--vfs');
    if(i>=0) argv.splice(i,2);
  }else{
    argv.push(
      "--singlethread",
      "--nomutex",
      //"--nosync",
      "--nomemstat",
      "--size", "10"
    );
  }

  if(argv.indexOf('--memdb')>=0){
    logErr("WARNING: --memdb flag trumps db filename.");
  }
  argv.push("--big-transactions"/*important for tests 410 and 510!*/,
            dbFile);
  //log("argv =",argv);
  // These log messages are not emitted to the UI until after main() returns. Fixing that
  // requires moving the main() call and related cleanup into a timeout handler.
  if(pDir) unlink(dbFile);
  log("Starting native app:\n ",argv.join(' '));
  log("This will take a while and the browser might warn about the runaway JS.",
      "Give it time...");
  setTimeout(function(){
    if(pDir) unlink(dbFile);
    wasm.xCall('wasm_main', argv.length,
               wasm.scopedAllocMainArgv(argv));
    wasm.scopedAllocPop(scope);
    if(pDir) unlink(dbFile);
    log("Done running native main()");
  }, 25);
}/*runTests()*/;

sqlite3InitModule({
  print: log,
  printErr: logErr
}).then(runTests);
