/*
  2022-05-22

  The author disclaims copyright to this source code.  In place of a
  legal notice, here is a blessing:

  *   May you do good and not evil.
  *   May you find forgiveness for yourself and forgive others.
  *   May you share freely, never taking more than you give.

  ***********************************************************************

  An experiment for wasmfs/opfs. This file MUST be in the same dir as
  the sqlite3.js emscripten module or that module won't be able to
  resolve the relative URIs (importScript()'s relative URI handling
  is, quite frankly, broken).
*/
'use strict';
(function(){
  const toss = function(...args){throw new Error(args.join(' '))};
  importScripts('sqlite3.js');

  /**
     Posts a message in the form {type,data} unless passed more than 2
     args, in which case it posts {type, data:[arg1...argN]}.
  */
  const wMsg = function(type,data){
    postMessage({
      type,
      data: arguments.length<3
        ? data
        : Array.prototype.slice.call(arguments,1)
    });
  };

  const stdout = console.log.bind(console);
  const stderr = console.error.bind(console);//function(...args){wMsg('stderr', args);};

  const test1 = function(db){
    db.execMulti("create table if not exists t(a);")
      .callInTransaction(function(db){
        db.prepare("insert into t(a) values(?)")
          .bind(new Date().getTime())
          .stepFinalize();
        stdout("Number of values in table t:",
            db.selectValue("select count(*) from t"));
      });
  };

  const runTests = function(Module){
    //stdout("Module",Module);
    self._MODULE = Module /* this is only to facilitate testing from the console */;
    const sqlite3 = Module.sqlite3,
          capi = sqlite3.capi,
          oo = sqlite3.oo1,
          wasm = capi.wasm;
    stdout("Loaded sqlite3:",capi.sqlite3_libversion(), capi.sqlite3_sourceid());
    const persistentDir = capi.sqlite3_web_persistent_dir();
    if(persistentDir){
      stderr("Persistent storage dir:",persistentDir);
    }else{
      stderr("No persistent storage available.");
    }
    const startTime = performance.now();
    let db;
    try {
      db = new oo.DB(persistentDir+'/foo.db');
      stdout("DB filename:",db.filename,db.fileName());
      const banner1 = '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>',
            banner2 = '<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<';
      [
        test1
      ].forEach((f)=>{
        const n = performance.now();
        stdout(banner1,"Running",f.name+"()...");
        f(db, sqlite3, Module);
        stdout(banner2,f.name+"() took ",(performance.now() - n),"ms");
      });
    }finally{
      if(db) db.close();
    }
    stdout("Total test time:",(performance.now() - startTime),"ms");
  };

  sqlite3InitModule(self.sqlite3TestModule).then(runTests);
})();
