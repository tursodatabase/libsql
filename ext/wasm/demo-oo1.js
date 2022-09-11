/*
  2022-08-16

  The author disclaims copyright to this source code.  In place of a
  legal notice, here is a blessing:

  *   May you do good and not evil.
  *   May you find forgiveness for yourself and forgive others.
  *   May you share freely, never taking more than you give.

  ***********************************************************************

  A basic demonstration of the SQLite3 OO API #1, shorn of assertions
  and the like to improve readability.
*/
'use strict';
(function(){
  const toss = function(...args){throw new Error(args.join(' '))};
  const debug = console.debug.bind(console),
        log = console.log.bind(console),
        warn = console.warn.bind(console),
        error = console.error.bind(console);

  const demo1 = function(sqlite3){
    const capi = sqlite3.capi,
          oo = sqlite3.oo1,
          wasm = capi.wasm;

    const dbName = (
      0 ? "" : capi.sqlite3_web_persistent_dir()
    )+"/mydb.sqlite3"
    if(0 && capi.sqlite3_web_persistent_dir()){
      capi.wasm.sqlite3_wasm_vfs_unlink(dbName);
    }
    const db = new oo.DB(dbName);
    log("db =",db.filename);
    /**
       Never(!) rely on garbage collection to clean up DBs and
       (especially) statements. Always wrap their lifetimes in
       try/finally construct...
    */
    try {
      log("Create a table...");
      db.exec("CREATE TABLE IF NOT EXISTS t(a,b)");
      //Equivalent:
      db.exec({
        sql:"CREATE TABLE IF NOT EXISTS t(a,b)"
        // ... numerous other options ... 
      });
      // SQL can be either a string or a byte array

      log("Insert some data using exec()...");
      let i;
      for( i = 1; i <= 5; ++i ){
        db.exec({
          sql: "insert into t(a,b) values (?,?)",
          // bind by parameter index...
          bind: [i, i*2]
        });
        db.exec({
          sql: "insert into t(a,b) values ($a,$b)",
          // bind by parameter name...
          bind: {$a: i * 3, $b: i * 4}
        });
      }    

      log("Insert using a prepared statement...");
      let q = db.prepare("insert into t(a,b) values(?,?)");
      try {
        for( i = 100; i < 103; ++i ){
          q.bind( [i, i*2] ).step();
          q.reset();
        }
        // Equivalent...
        for( i = 103; i <= 105; ++i ){
          q.bind(1, i).bind(2, i*2).stepReset();
        }
      }finally{
        q.finalize();
      }

      log("Query data with exec() using rowMode 'array'...");
      db.exec({
        sql: "select a from t order by a limit 3",
        rowMode: 'array', // 'array', 'object', or 'stmt' (default)
        callback: function(row){
          log("row ",++this.counter,"=",row);
        }.bind({counter: 0})
      });

      log("Query data with exec() using rowMode 'object'...");
      db.exec({
        sql: "select a as aa, b as bb from t order by aa limit 3",
        rowMode: 'object',
        callback: function(row){
          log("row ",++this.counter,"=",row);
        }.bind({counter: 0})
      });

      log("Query data with exec() using rowMode 'stmt'...");
      db.exec({
        sql: "select a from t order by a limit 3",
        rowMode: 'stmt', // stmt === the default
        callback: function(row){
          log("row ",++this.counter,"get(0) =",row.get(0));
        }.bind({counter: 0})
      });

      log("Query data with exec() using rowMode INTEGER (result column index)...");
      db.exec({
        sql: "select a, b from t order by a limit 3",
        rowMode: 1, // === result column 1
        callback: function(row){
          log("row ",++this.counter,"b =",row);
        }.bind({counter: 0})
      });

      log("Query data with exec() without a callback...");
      let resultRows = [];
      db.exec({
        sql: "select a, b from t order by a limit 3",
        rowMode: 'object',
        resultRows: resultRows
      });
      log("Result rows:",resultRows);

      log("Create a scalar UDF...");
      db.createFunction({
        name: 'twice',
        callback: function(arg){ // note the call arg count
          return arg + arg;
        }
      });
      log("Run scalar UDF and collect result column names...");
      let columnNames = [];
      db.exec({
        sql: "select a, twice(a), twice(''||a) from t order by a desc limit 3",
        columnNames: columnNames,
        rowMode: 'stmt',
        callback: function(row){
          log("a =",row.get(0), "twice(a) =", row.get(1),
              "twice(''||a) =",row.get(2));
        }
      });
      log("Result column names:",columnNames);

      if(0){
        warn("UDF will throw because of incorrect arg count...");
        db.exec("select twice(1,2,3)");
      }

      try {
        db.transaction( function(D) {
          D.exec("delete from t");
          log("In transaction: count(*) from t =",db.selectValue("select count(*) from t"));
          throw new sqlite3.SQLite3Error("Demonstrating transaction() rollback");
        });
      }catch(e){
        if(e instanceof sqlite3.SQLite3Error){
          log("Got expected exception from db.transaction():",e.message);
          log("count(*) from t =",db.selectValue("select count(*) from t"));
        }else{
          throw e;
        }
      }

      try {
        db.savepoint( function(D) {
          D.exec("delete from t");
          log("In savepoint: count(*) from t =",db.selectValue("select count(*) from t"));
          D.savepoint(function(DD){
            const rows = [];
            D.exec({
              sql: ["insert into t(a,b) values(99,100);",
                    "select count(*) from t"],
              rowMode: 0,
              resultRows: rows
            });
            log("In nested savepoint. Row count =",rows[0]);
            throw new sqlite3.SQLite3Error("Demonstrating nested savepoint() rollback");
          })
        });
      }catch(e){
        if(e instanceof sqlite3.SQLite3Error){
          log("Got expected exception from nested db.savepoint():",e.message);
          log("count(*) from t =",db.selectValue("select count(*) from t"));
        }else{
          throw e;
        }
      }

    }finally{
      db.close();
    }

    /**
       Misc. DB features:

       - get change count (total or statement-local, 32- or 64-bit)
       - get its file name
       - selectValue() takes SQL and returns first column of first row.
    
       Misc. Stmt features:

       - Various forms of bind() 
       - clearBindings()
       - reset()
       - Various forms of step()
       - Variants of get() for explicit type treatment/conversion,
         e.g. getInt(), getFloat(), getBlob(), getJSON()
       - getColumnName(ndx), getColumnNames()
       - getParamIndex(name)
    */
  }/*demo1()*/;

  const runDemos = function(Module){
    //log("Module.sqlite3",Module);
    const sqlite3 = Module.sqlite3,
          capi = sqlite3.capi;
    log("Loaded module:",capi.sqlite3_libversion(), capi.sqlite3_sourceid());
    log("sqlite3 namespace:",sqlite3);
    try {
      demo1(sqlite3);
    }catch(e){
      error("Exception:",e.message);
      throw e;
    }
  };

  //self.sqlite3TestModule.sqlite3ApiConfig.persistentDirName = "/hi";
  self.sqlite3TestModule.initSqlite3().then(runDemos);
})();
