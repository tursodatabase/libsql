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
    T.assert( 0 !== capi.sqlite3_vfs_find(null) );

    const dbStorage = 1 ? ':sessionStorage:' : ':localStorage:';
    /**
       The names ':sessionStorage:' and ':localStorage:' are handled
       via the DB class constructor, not the C level. In the C API,
       the names "local" and "session" are the current (2022-09-12)
       names for those keys, but that is subject to change.
    */
    const db = new oo.DB( dbStorage );
    log("Storage backend:",db.filename /* note that the name was internally translated */);
    try {
      db.exec("create table if not exists t(a)");
      if(undefined===db.selectValue("select a from t limit 1")){
        log("New db. Populating. This DB will persist across page reloads.");
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
      db.close();
    }
    log("End of demo.");
  };

  sqlite3InitModule(self.sqlite3TestModule).then(function(theModule){
    console.warn("Installing Emscripten module as global EM for dev console access.");
    self.EM = theModule;
    runTests(theModule);
  });
})();
