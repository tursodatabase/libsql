/*
  2022-09-12

  The author disclaims copyright to this source code.  In place of a
  legal notice, here is a blessing:

  *   May you do good and not evil.
  *   May you find forgiveness for yourself and forgive others.
  *   May you share freely, never taking more than you give.

  ***********************************************************************

  A basic test script for sqlite3-kvvfs.wasm. This file must be run in
  main JS thread and sqlite3-kvvfs.js must have been loaded before it.
*/
'use strict';
(function(){
  const T = self.SqliteTestUtil;
  const toss = function(...args){throw new Error(args.join(' '))};
  const debug = console.debug.bind(console);
  const eOutput = document.querySelector('#test-output');
  const log = console.log.bind(console)
  const logHtml = function(...args){
    log.apply(this, args);
    const ln = document.createElement('div');
    ln.append(document.createTextNode(args.join(' ')));
    eOutput.append(ln);
  };

  const runTests = function(Module){
    //log("Module",Module);
    const sqlite3 = Module.sqlite3,
          capi = sqlite3.capi,
          oo = sqlite3.oo1,
          wasm = capi.wasm;
    log("Loaded module:",capi.sqlite3_libversion(), capi.sqlite3_sourceid());
    log("Build options:",wasm.compileOptionUsed());
    self.S = sqlite3;
    T.assert(0 === capi.sqlite3_vfs_find(null));
    S.capi.sqlite3_initialize();
    T.assert( Number.isFinite( capi.sqlite3_vfs_find(null) ) );
    const stores = {
      local: localStorage,
      session: sessionStorage
    };
    const cleanupStore = function(n){
      const s = stores[n];
      const isKv = (key)=>key.startsWith('kvvfs-'+n);
      let i, k, toRemove = [];
      for( i = 0; (k = s.key(i)); ++i) {
        if(isKv(k)) toRemove.push(k);
      }
      toRemove.forEach((k)=>s.removeItem(k));
    };
    const dbStorage = 1 ? 'session' : 'local';
    const db = new oo.DB(dbStorage);
    try {
      db.exec("create table if not exists t(a)");
      if(undefined===db.selectValue("select a from t limit 1")){
        log("New db. Populating..");
        db.exec("insert into t(a) values(1),(2),(3)");
      }else{
        log("Found existing table data:");
        db.exec({
          sql: "select * from t order by a",
          rowMode: 0,
          callback: function(v){log(v)}
        });
      }
    }finally{
      const n = db.filename;
      db.close();
      //cleanupStore(n);
    }
    
    log("Init done. Proceed from the dev console.");
  };

  sqlite3InitModule(self.sqlite3TestModule).then(function(theModule){
    console.warn("Installing Emscripten module as global EM for dev console access.");
    self.EM = theModule;
    runTests(theModule);
  });
})();
