/*
  2022-09-12

  The author disclaims copyright to this source code.  In place of a
  legal notice, here is a blessing:

  *   May you do good and not evil.
  *   May you find forgiveness for yourself and forgive others.
  *   May you share freely, never taking more than you give.

  ***********************************************************************

  A basic test script for sqlite3.wasm with kvvfs support. This file
  must be run in main JS thread and sqlite3.js must have been loaded
  before it.
*/
'use strict';
(function(){
  const T = self.SqliteTestUtil;
  const toss = function(...args){throw new Error(args.join(' '))};
  const debug = console.debug.bind(console);
  const eOutput = document.querySelector('#test-output');
  const logC = console.log.bind(console)
  const logE = function(domElement){
    eOutput.append(domElement);
  };
  const logHtml = function(cssClass,...args){
    const ln = document.createElement('div');
    if(cssClass) ln.classList.add(cssClass);
    ln.append(document.createTextNode(args.join(' ')));
    logE(ln);
  }
  const log = function(...args){
    logC(...args);
    logHtml('',...args);
  };
  const warn = function(...args){
    logHtml('warning',...args);
  };
  const error = function(...args){
    logHtml('error',...args);
  };
  
  const runTests = function(sqlite3){
    const capi = sqlite3.capi,
          oo = sqlite3.oo1,
          wasm = sqlite3.wasm;
    log("Loaded module:",capi.sqlite3_libversion(), capi.sqlite3_sourceid());
    T.assert( 0 !== capi.sqlite3_vfs_find(null) );
    if(!capi.sqlite3_vfs_find('kvvfs')){
      error("This build is not kvvfs-capable.");
      return;
    }
    
    const dbStorage = 0 ? 'session' : 'local';
    const theStore = 's'===dbStorage[0] ? sessionStorage : localStorage;
    const db = new oo.JsStorageDb( dbStorage );
    // Or: oo.DB(dbStorage, 'c', 'kvvfs')
    log("db.storageSize():",db.storageSize());
    document.querySelector('#btn-clear-storage').addEventListener('click',function(){
      const sz = db.clearStorage();
      log("kvvfs",db.filename+"Storage cleared:",sz,"entries.");
    });
    document.querySelector('#btn-clear-log').addEventListener('click',function(){
      eOutput.innerText = '';
    });
    document.querySelector('#btn-init-db').addEventListener('click',function(){
      try{
        const saveSql = [];
        db.exec({
          sql: ["drop table if exists t;",
                "create table if not exists t(a);",
                "insert into t(a) values(?),(?),(?)"],
          bind: [performance.now() >> 0,
                 (performance.now() * 2) >> 0,
                 (performance.now() / 2) >> 0],
          saveSql
        });
        console.log("saveSql =",saveSql,theStore);
        log("DB (re)initialized.");
      }catch(e){
        error(e.message);
      }
    });
    const btnSelect = document.querySelector('#btn-select1');
    btnSelect.addEventListener('click',function(){
      log("DB rows:");
      try{
        db.exec({
          sql: "select * from t order by a",
          rowMode: 0,
          callback: (v)=>log(v)
        });
      }catch(e){
        error(e.message);
      }
    });
    document.querySelector('#btn-storage-size').addEventListener('click',function(){
      log("size.storageSize(",dbStorage,") says", db.storageSize(),
          "bytes");
    });
    log("Storage backend:",db.filename);
    if(0===db.selectValue('select count(*) from sqlite_master')){
      log("DB is empty. Use the init button to populate it.");
    }else{
      log("DB contains data from a previous session. Use the Clear Storage button to delete it.");
      btnSelect.click();
    }
  };

  sqlite3InitModule(self.sqlite3TestModule).then((sqlite3)=>{
    runTests(sqlite3);
  });
})();
