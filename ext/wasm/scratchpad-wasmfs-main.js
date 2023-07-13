/*
  2022-05-22

  The author disclaims copyright to this source code.  In place of a
  legal notice, here is a blessing:

  *   May you do good and not evil.
  *   May you find forgiveness for yourself and forgive others.
  *   May you share freely, never taking more than you give.

  ***********************************************************************

  A basic test script for sqlite3-api.js. This file must be run in
  main JS thread and sqlite3.js must have been loaded before it.
*/
'use strict';
//importScripts('jswasm/sqlite3-wasmfs.js');
import sqlite3InitModule from './jswasm/sqlite3-wasmfs.mjs';
//console.log('sqlite3InitModule =',sqlite3InitModule);
(function(){
  const toss = function(...args){throw new Error(args.join(' '))};
  const log = console.log.bind(console),
        warn = console.warn.bind(console),
        error = console.error.bind(console);

  const stdout = log;
  const stderr = error;

  const test1 = function(db){
    db.exec("create table if not exists t(a);")
      .transaction(function(db){
        db.prepare("insert into t(a) values(?)")
          .bind(new Date().getTime())
          .stepFinalize();
        stdout("Number of values in table t:",
            db.selectValue("select count(*) from t"));
      });
  };

  const runTests = function(sqlite3){
    const capi = sqlite3.capi,
          oo = sqlite3.oo1,
          wasm = sqlite3.wasm;
    stdout("Loaded module:",sqlite3);
    stdout("Loaded sqlite3:",capi.sqlite3_libversion(), capi.sqlite3_sourceid());
    const persistentDir = capi.sqlite3_wasmfs_opfs_dir();
    if(persistentDir){
      stdout("Persistent storage dir:",persistentDir);
    }else{
      stderr("No persistent storage available.");
    }
    const startTime = performance.now();
    let db;
    try {
      db = new oo.DB(persistentDir+'/foo.db');
      stdout("DB filename:",db.filename);
      const banner1 = '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>',
            banner2 = '<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<';
      [
        test1
      ].forEach((f)=>{
        const n = performance.now();
        stdout(banner1,"Running",f.name+"()...");
        f(db, sqlite3);
        stdout(banner2,f.name+"() took ",(performance.now() - n),"ms");
      });
    }finally{
      if(db) db.close();
    }
    stdout("Total test time:",(performance.now() - startTime),"ms");
  };

  sqlite3InitModule(globalThis.sqlite3TestModule).then((X)=>{
    /*
      2023-07-13: we're passed the Emscripten Module object here
      instead of the sqlite3 object. This differs from the canonical
      build and is a side effect of WASMFS's requirement
      that the module init function (i.e. sqlite3InitModule())
      return its initial arrgument (the Emscripten Module).
    */
    stdout("then() got",X.sqlite3,X);
    runTests(X.sqlite3 || X);
  });
})();
